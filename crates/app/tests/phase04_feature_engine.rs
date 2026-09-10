//! Exercises `binary-alpha features build` end to end: synthetic tick and bar instruments
//! always, and the governed reference parity proof when `BINARY_ALPHA_TEST_CONFIG` names it.
//!
//! The test owns input adaptation (it reads published objects itself and drives the engine
//! in-process for chunking and stable-prefix checks); `FeatureEngine` owns the formulas and the
//! plan; the application command owns loading, fitting, publication, and reconstruction.

mod common;

use std::borrow::Cow;
use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use binary_alpha_engine::config::Config;
use binary_alpha_engine::dataset::GenerationManifest;
use binary_alpha_engine::features::{
    FeatureEngine, FeatureManifest, FeatureOutput, FeaturePlan, Value, feature_generation_id,
    raw_identity,
};
use binary_alpha_engine::market::{Tick, format_event_time_micros};
use binary_alpha_engine::stream::{Observation, Source, StreamManifest};
use common::*;
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::record::{Field, RowAccessor};

/// The report lines of a successful build, or the diagnostic of a failed one.
fn build(config: &Path) -> Result<Vec<String>, String> {
    let output = binary_alpha(&["features", "build", "--config", config.to_str().unwrap()]);
    let stdout = String::from_utf8(output.stdout).unwrap();
    let stderr = String::from_utf8(output.stderr).unwrap();
    if output.status.success() {
        assert!(stderr.is_empty(), "{stderr}");
        Ok(stdout.lines().map(str::to_string).collect())
    } else {
        assert_eq!(output.status.code(), Some(1));
        assert!(stdout.is_empty(), "{stdout}");
        Err(stderr)
    }
}

fn audit(config: &Path, manifest: &Path) -> String {
    let uri = format!("file://{}", manifest.display());
    let output = binary_alpha(&[
        "data",
        "audit",
        "--config",
        config.to_str().unwrap(),
        "--manifest",
        &uri,
    ]);
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout)
        .unwrap()
        .trim_end()
        .to_string()
}

fn manifest_uri(path: &Path) -> String {
    format!("file://{}", path.display())
}

/// A deterministic tick file: about four ticks per second for `minutes` minutes with repeats,
/// a few multi-second delays, one reopen gap, and occasional larger moves.
fn synthetic_ticks(minutes: i64) -> Vec<String> {
    let mut state: u64 = 0x2545_F491_4F6C_DD1D;
    let mut next = move || {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        state
    };
    let base = 1_774_137_600_000_i64; // 2026-03-22T00:00:00Z in milliseconds
    let mut time = base;
    let mut price: i64 = 1_800_000;
    let mut rows = Vec::new();
    while time < base + minutes * 60_000 {
        let draw = next();
        let repeat = draw % 40 == 1;
        time += match draw % 12_000 {
            0 => 3_500,
            2 if draw % 60_000 == 2 => 65_000,
            _ if repeat => 0,
            _ => 120 + (draw % 200) as i64,
        };
        if !repeat {
            price += match (draw >> 8) % 15_000 {
                0 => 1_100,
                1 => -1_000,
                n if n % 4 == 0 => 0,
                n if n % 2 == 0 => (n % 3) as i64 + 1,
                n => -((n % 3) as i64 + 1),
            };
        }
        let seconds = time.div_euclid(1_000);
        rows.push(format!(
            "{}.{:03}Z,AEDCNY,{}.{:06}",
            format_event_time_micros(seconds * 1_000_000)
                .strip_suffix(".000000Z")
                .unwrap(),
            time.rem_euclid(1_000),
            price / 1_000_000,
            price % 1_000_000
        ));
    }
    rows
}

fn tick_instrument() -> String {
    "\n[[instruments]]\nbroker = \"pocket_option\"\nprovider_symbol = \"AEDCNY_otc\"\nbase_currency = \"AED\"\nquote_currency = \"CNY\"\nprice_scale = 6\nnative_granularity = { kind = \"tick\" }\ngap = { max_seconds = 2, reopen_seconds = 60 }\nfrozen = { min_observations = 10, min_seconds = 5 }\njump = { min_basis_points = 5 }\nspan = { min_percent = 75 }\nsessions = [{ name = \"week\", open_seconds = 0, close_seconds = 604800 }]\ncandles = [{ duration_seconds = 15, offset_seconds = 5, min_observations = 20, hard_min_observations = 10 }, { duration_seconds = 60, offset_seconds = 30, min_observations = 80, hard_min_observations = 40 }]\n".to_string()
}

fn bar_instrument(symbol: &str) -> String {
    format!(
        "\n[[instruments]]\nbroker = \"pocket_option\"\nprovider_symbol = \"{symbol}\"\nquote_currency = \"USD\"\nprice_scale = 3\nnative_granularity = {{ kind = \"bar\", period_seconds = 5 }}\ngap = {{ max_seconds = 2, reopen_seconds = 60 }}\nfrozen = {{ min_observations = 10, min_seconds = 5 }}\njump = {{ min_basis_points = 5 }}\nspan = {{ min_percent = 75 }}\ncandles = [{{ duration_seconds = 60, offset_seconds = 0 }}]\n"
    )
}

const TICK_SETTINGS: &str = "streams = [{ duration_seconds = 15, offset_seconds = 5 }, { duration_seconds = 60, offset_seconds = 30 }]\noutputs = \"all_supported\"\nmoving_average_periods = [20, 50]\nrolling_window = 100\nmin_history = 20\nprice_epsilon = \"0\"\ntick_path_streams = [{ duration_seconds = 15, offset_seconds = 5 }, { duration_seconds = 60, offset_seconds = 30 }]\nstructure = { swing_left = 3, swing_right = 3, rolling_windows = [5, 10, 20], direction_window = 10, trend_efficiency_threshold = 0.35, trend_min_abs_momentum_bps = 3.0, range_efficiency_threshold = 0.25, compression_ratio_threshold = 0.7, expanded_ratio_threshold = 1.3, extreme_ratio_threshold = 1.8, pullback_min_trend_age = 3, trend_reset_sideways_bars = 3, failed_breakout_max_bars = 5 }\nencodings = { max_labels = 32768, outputs = [{ output = \"candle_type\" }, { output = \"is_doji\" }, { output = \"regime_v1\" }, { output = \"body_bps_bucketed\" }, { output = \"tick_volume_dev_quantile\" }, { output = \"return_1_bps\", bins = \"development_fifths\" }, { output = \"range_bps\", bins = [0.0, 0.1, 0.2, 0.3, 0.5, 1.0] }] }\n";

const BAR_SETTINGS: &str = "streams = [{ duration_seconds = 60, offset_seconds = 0 }]\noutputs = \"all_supported\"\nmoving_average_periods = [8, 21]\nrolling_window = 30\nmin_history = 10\nprice_epsilon = \"0.001\"\nstructure = { swing_left = 2, swing_right = 2, rolling_windows = [5, 10, 20], direction_window = 10, trend_efficiency_threshold = 0.35, trend_min_abs_momentum_bps = 3.0, range_efficiency_threshold = 0.25, compression_ratio_threshold = 0.7, expanded_ratio_threshold = 1.3, extreme_ratio_threshold = 1.8, pullback_min_trend_age = 3, trend_reset_sideways_bars = 3, failed_breakout_max_bars = 5 }\nencodings = { max_labels = 64, outputs = [{ output = \"candle_type\" }, { output = \"regime_trend_state\" }, { output = \"body_bps_bucketed\" }] }\n";

fn feature_entry(role: &str, input: &Path, profile: &Path, settings: &str) -> String {
    format!(
        "\n[[features.instruments]]\nrole = \"{role}\"\ninput_manifest = \"{}\"\nprofile_manifest = \"{}\"\n{settings}",
        manifest_uri(input),
        manifest_uri(profile)
    )
}

/// Every row of a published table through the generic row API, as engine values.
fn read_table(path: &Path) -> Table {
    let reader = SerializedFileReader::new(fs::File::open(path).unwrap()).unwrap();
    let names: Vec<String> = reader
        .metadata()
        .file_metadata()
        .schema_descr()
        .columns()
        .iter()
        .map(|column| column.name().to_string())
        .collect();
    let rows = reader
        .get_row_iter(None)
        .unwrap()
        .map(|row| {
            row.unwrap()
                .get_column_iter()
                .map(|(_, field)| match field {
                    Field::Null => None,
                    Field::Long(value) => Some(Value::Int(*value)),
                    Field::Short(value) => Some(Value::Int(i64::from(*value))),
                    Field::Int(value) => Some(Value::Int(i64::from(*value))),
                    Field::Double(value) => Some(Value::Float(*value)),
                    Field::Bool(value) => Some(Value::Bool(*value)),
                    Field::Str(value) => Some(Value::Text(Cow::Owned(value.clone()))),
                    Field::TimestampMicros(value) => Some(Value::Time(*value)),
                    other => panic!("unexpected field {other:?}"),
                })
                .collect()
        })
        .collect();
    (names, rows)
}

/// The published feature generation at `manifest`: manifest, plan, and every stream's tables.
/// One published table: its column names and every row.
type Table = (Vec<String>, Vec<Vec<Option<Value>>>);

struct PublishedFeatures {
    manifest: FeatureManifest,
    plan: FeaturePlan,
    tables: Vec<Vec<Table>>,
}

fn published_features(store: &Path, manifest: &Path) -> PublishedFeatures {
    let manifest = FeatureManifest::from_json(&fs::read(manifest).unwrap()).unwrap();
    let object = |path: &str| {
        store.join(
            &manifest
                .objects
                .iter()
                .find(|object| object.path == path)
                .unwrap_or_else(|| panic!("object {path}"))
                .key,
        )
    };
    let plan = FeaturePlan::from_json(&fs::read(object("plan.json")).unwrap()).unwrap();
    let tables = plan
        .streams
        .iter()
        .map(|stream| {
            stream
                .object_paths()
                .iter()
                .map(|path| read_table(&object(path)))
                .collect()
        })
        .collect();
    PublishedFeatures {
        manifest,
        plan,
        tables,
    }
}

fn feature_manifests(scratch: &Scratch, store: &str) -> Vec<PathBuf> {
    scratch
        .manifests(store)
        .into_iter()
        .filter(|path| {
            fs::read_to_string(path)
                .unwrap()
                .starts_with("{\n  \"kind\": \"feature_generation\"")
        })
        .collect()
}

fn read_normalized_ticks(store: &Path, dataset: &GenerationManifest) -> Vec<Tick> {
    let path = store.join(
        &dataset
            .objects
            .iter()
            .find(|object| object.path == "normalized/ticks.parquet")
            .unwrap()
            .key,
    );
    let reader = SerializedFileReader::new(fs::File::open(path).unwrap()).unwrap();
    reader
        .get_row_iter(None)
        .unwrap()
        .map(|row| {
            let row = row.unwrap();
            Tick {
                event_time_micros: row.get_timestamp_micros(0).unwrap(),
                price_units: row.get_long(1).unwrap(),
            }
        })
        .collect()
}

/// Drives the engine in-process over `ticks` in chunks of `chunk` and returns everything it
/// emitted per stream.
fn feed(
    plan: &FeaturePlan,
    dataset: &GenerationManifest,
    ticks: &[Tick],
    chunk: usize,
) -> FeatureOutput {
    let mut engine = FeatureEngine::new(plan, Source::from_manifest(dataset)).unwrap();
    let mut out = FeatureOutput::default();
    for ticks in ticks.chunks(chunk.max(1)) {
        for tick in ticks {
            engine.push(Observation::Tick(*tick), &mut out).unwrap();
        }
    }
    out
}

/// The prefix length at `percent` of the ticks, extended until the last kept tick's event time
/// is strictly before the next tick's, so the cutoff time names exactly the kept ticks.
fn cutoff(ticks: &[Tick], percent: usize) -> usize {
    let mut cut = ticks.len() * percent / 100;
    while cut < ticks.len() && ticks[cut].event_time_micros == ticks[cut - 1].event_time_micros {
        cut += 1;
    }
    cut
}

/// A feed cut at `cutoff` is exactly the rows and events the full feed made known by then.
fn assert_prefix(prefix: &FeatureOutput, full: &FeatureOutput, cutoff: i64, percent: usize) {
    assert!(
        !prefix.rows.is_empty(),
        "{percent} percent: the prefix emits rows"
    );
    assert_eq!(
        prefix.rows,
        full.rows[..prefix.rows.len()],
        "{percent} percent"
    );
    assert_eq!(
        prefix.structure_events,
        full.structure_events[..prefix.structure_events.len()],
        "{percent} percent"
    );
    assert_eq!(
        prefix.sequence_events,
        full.sequence_events[..prefix.sequence_events.len()],
        "{percent} percent"
    );
    let known = |output: &FeatureOutput| {
        (
            output
                .rows
                .iter()
                .filter(|(_, row)| row.known_at_micros <= cutoff)
                .count(),
            output
                .structure_events
                .iter()
                .filter(|(_, event)| event.known_at_micros <= cutoff)
                .count(),
            output
                .sequence_events
                .iter()
                .filter(|(_, event)| event.known_at_micros <= cutoff)
                .count(),
        )
    };
    let counts = (
        prefix.rows.len(),
        prefix.structure_events.len(),
        prefix.sequence_events.len(),
    );
    assert_eq!(
        known(prefix),
        counts,
        "{percent} percent: nothing in the prefix is known after the cutoff"
    );
    assert_eq!(
        known(full),
        counts,
        "{percent} percent: the prefix holds everything known by the cutoff"
    );
}

fn value_of<'a>(row: &'a [Option<Value>], names: &[String], name: &str) -> Option<&'a Value> {
    row[names.iter().position(|n| n == name).unwrap()].as_ref()
}

#[test]
fn features_build_fits_publishes_reconstructs_freezes_and_isolates() {
    let scratch = Scratch::new("phase04_ticks");
    let rows = synthetic_ticks(480);
    write_ticks(
        &scratch.path("sources/ticks/ticks.csv"),
        &rows.iter().map(String::as_str).collect::<Vec<_>>(),
    );
    let import_config = scratch.config("import.toml", &scratch.tick_source());
    let lines = import(&import_config).unwrap();
    let development = generation(&lines[0]);
    let dataset_manifest = scratch.path(&format!("published/manifests/{development}/ready.json"));
    let audit_config = scratch.config("audit.toml", &tick_instrument());
    let audited = audit(&audit_config, &dataset_manifest);
    let stream_generation = generation(&audited);
    let stream_manifest = scratch.path(&format!(
        "published/manifests/{stream_generation}/ready.json"
    ));

    // A new plan fitted on the development generation.
    let config = scratch.config(
        "features.toml",
        &feature_entry(
            "development",
            &dataset_manifest,
            &stream_manifest,
            TICK_SETTINGS,
        ),
    );
    let lines = build(&config).unwrap();
    assert_eq!(lines.len(), 2, "{lines:?}");
    assert!(
        lines[0].starts_with("features pocket_option:AEDCNY_otc development generation "),
        "{}",
        lines[0]
    );
    assert!(
        lines[0].contains("reused 0 [stream ")
            && lines[0].contains("s fit ")
            && lines[0].contains("s encode ")
            && lines[0].contains("s publish "),
        "{}",
        lines[0]
    );
    let feature_generation = generation(&lines[0]);
    assert_eq!(
        lines[1],
        verify(&scratch.path(&format!(
            "published/manifests/{feature_generation}/ready.json"
        )))
        .unwrap()
    );
    assert!(
        lines[1].starts_with("verified pocket_option:AEDCNY_otc development generation "),
        "{}",
        lines[1]
    );
    let manifest_path = scratch.path(&format!(
        "published/manifests/{feature_generation}/ready.json"
    ));
    assert_eq!(
        feature_manifests(&scratch, "published"),
        std::slice::from_ref(&manifest_path)
    );
    assert_eq!(
        feature_manifests(&scratch, "retained"),
        [scratch.path(&format!(
            "retained/manifests/{feature_generation}/ready.json"
        ))]
    );
    // A manifest listing an object outside the generation's object set is rejected.
    let mut json: serde_json::Value =
        serde_json::from_slice(&fs::read(&manifest_path).unwrap()).unwrap();
    let mut extra = json["objects"][0].clone();
    extra["path"] = "notes.txt".into();
    json["objects"].as_array_mut().unwrap().push(extra);
    assert!(
        FeatureManifest::from_json(&serde_json::to_vec(&json).unwrap())
            .unwrap_err()
            .contains("`notes.txt` is not part of a feature generation")
    );
    let published = published_features(&scratch.path("published"), &manifest_path);
    let (manifest, plan) = (&published.manifest, &published.plan);
    let dataset = GenerationManifest::from_json(&fs::read(&dataset_manifest).unwrap()).unwrap();
    let stream = StreamManifest::from_json(&fs::read(&stream_manifest).unwrap()).unwrap();
    assert_eq!(
        manifest.generation,
        feature_generation_id(&plan.identity(), &development)
    );
    assert_eq!(manifest.plan_identity, plan.identity());
    assert_eq!(manifest.frozen_from, None);
    assert_eq!(manifest.profile_generation, stream_generation);
    assert_eq!(manifest.input_generation, development);
    assert_eq!(manifest.observations, dataset.row_count);
    assert_eq!(
        manifest.config_hash,
        Config::parse(&fs::read_to_string(&config).unwrap())
            .unwrap()
            .content_hash()
    );
    assert_eq!(manifest.code_revision, env!("BINARY_ALPHA_CODE_REVISION"));
    assert_eq!(manifest.objects.len(), 1 + 4 * 2);
    assert_eq!(plan.profile.definition, stream.definition);
    assert_eq!(plan.profile.stream_generation, stream_generation);
    assert!(plan.profile.ticks);
    assert_eq!(plan.development_generation, development);
    assert_eq!(
        plan.raw_identity,
        raw_identity(&plan.profile, &development, &plan.settings)
    );
    assert!(plan.is_fitted());
    assert_eq!(plan.streams.len(), 2);
    for (stream_plan, summary) in plan.streams.iter().zip(&manifest.streams) {
        assert!(
            stream_plan.excluded.is_empty(),
            "{:?}",
            stream_plan.excluded
        );
        assert!(
            stream_plan.tick_path,
            "every configured tick stream carries a path, including sixty seconds"
        );
        assert_eq!(stream_plan.encodings.len(), 7);
        assert_eq!(
            (stream_plan.duration_seconds, stream_plan.offset_seconds),
            (summary.duration_seconds, summary.offset_seconds)
        );
        let fit = plan
            .fit_windows
            .iter()
            .find(|window| window.duration_seconds == summary.duration_seconds)
            .unwrap();
        assert_eq!(
            (fit.rows, &fit.first_decision_time, &fit.last_decision_time),
            (
                summary.rows,
                &summary.first_decision_time,
                &summary.last_decision_time
            )
        );
        assert!(summary.rows > 200, "{summary:?}");
        assert!(
            summary.structure_events > 0 && summary.sequence_events > 0,
            "{summary:?}"
        );
    }
    // Rows equal the clean candle count of the published profile.
    let profile: serde_json::Value = manifest_json(
        &scratch.path("published").join(
            &stream
                .objects
                .iter()
                .find(|o| o.path == "profile.json")
                .unwrap()
                .key,
        ),
    );
    for (index, summary) in manifest.streams.iter().enumerate() {
        assert_eq!(profile["streams"][index]["flagged"]["clean"], summary.rows);
    }

    // The command's tables equal an in-process feed, whole and chunked, and every prefix.
    let ticks = read_normalized_ticks(&scratch.path("published"), &dataset);
    assert_eq!(ticks.len() as u64, dataset.row_count);
    let full = feed(plan, &dataset, &ticks, ticks.len());
    for (index, stream_plan) in plan.streams.iter().enumerate() {
        let [
            (names, rows),
            (_, structure),
            (_, sequence),
            (code_names, codes),
        ] = published.tables[index].as_slice()
        else {
            panic!("four tables");
        };
        assert_eq!(
            names,
            &stream_plan
                .outputs
                .iter()
                .map(|o| o.name.clone())
                .collect::<Vec<_>>()
        );
        let direct: Vec<&Vec<Option<Value>>> = full
            .rows
            .iter()
            .filter(|(s, _)| *s == index)
            .map(|(_, row)| &row.values)
            .collect();
        assert_eq!(rows.len(), direct.len());
        assert_eq!(
            rows.iter().collect::<Vec<_>>(),
            direct,
            "stream {index} rows"
        );
        assert_eq!(
            structure.len() as u64,
            manifest.streams[index].structure_events
        );
        assert_eq!(
            sequence.len() as u64,
            manifest.streams[index].sequence_events
        );
        assert_eq!(
            full.structure_events
                .iter()
                .filter(|(s, _)| *s == index)
                .count(),
            structure.len()
        );
        assert_eq!(
            full.sequence_events
                .iter()
                .filter(|(s, _)| *s == index)
                .count(),
            sequence.len()
        );
        // Encoded codes are the frozen encodings applied to the raw columns.
        assert_eq!(
            code_names,
            &stream_plan
                .encodings
                .iter()
                .map(|e| e.output.clone())
                .collect::<Vec<_>>()
        );
        for (column, encoding) in stream_plan.encodings.iter().enumerate() {
            let input = names.iter().position(|n| *n == encoding.input).unwrap();
            let raw: Vec<Option<Value>> = rows.iter().map(|row| row[input].clone()).collect();
            let expected: Vec<Option<Value>> = encoding
                .encode(&raw)
                .into_iter()
                .map(|code| Some(Value::Int(i64::from(code))))
                .collect();
            assert_eq!(
                codes
                    .iter()
                    .map(|row| row[column].clone())
                    .collect::<Vec<_>>(),
                expected,
                "{}",
                encoding.output
            );
            assert!(!encoding.labels.is_empty(), "{}", encoding.output);
        }
        let fifths = stream_plan
            .encodings
            .iter()
            .find(|e| e.output == "tick_volume_dev_quantile")
            .unwrap();
        assert_eq!(fifths.edges.as_ref().map(Vec::len), Some(4));
        let fixed = stream_plan
            .encodings
            .iter()
            .find(|e| e.output == "range_bps")
            .unwrap();
        assert_eq!(
            fixed.edges.as_deref(),
            Some(&[0.0, 0.1, 0.2, 0.3, 0.5, 1.0][..])
        );
        // Missingness and readiness on the first accepted rows.
        let first = &rows[0];
        assert_eq!(
            value_of(first, names, "is_ema20_ready"),
            Some(&Value::Bool(false))
        );
        assert_eq!(
            value_of(first, names, "ema20_slope_bps"),
            None,
            "the first slope is unavailable"
        );
        assert_eq!(
            value_of(first, names, "close_vs_ema20_state"),
            Some(&Value::Text("not_ready".into()))
        );
        assert!(
            value_of(first, names, "ema20").is_some(),
            "the numeric preview starts at the first close"
        );
        assert_eq!(
            value_of(first, names, "previous_candle_relation"),
            Some(&Value::Text("first_clean_candle".into()))
        );
        assert_eq!(
            value_of(first, names, "range_vs_recent_bucket"),
            Some(&Value::Text("unknown_warmup".into()))
        );
        assert_eq!(
            value_of(first, names, "market_structure_sequence"),
            Some(&Value::Text("unknown".into()))
        );
        assert_eq!(value_of(first, names, "momentum_5_bps"), None);
        assert_eq!(
            value_of(first, names, "compression_state"),
            Some(&Value::Text("unknown".into()))
        );
        assert_eq!(
            value_of(first, names, "quality_tier"),
            Some(&Value::Text("clean_v1".into()))
        );
        let ready = rows
            .iter()
            .find(|row| value_of(row, names, "is_ema50_ready") == Some(&Value::Bool(true)))
            .expect("fifty closes arrive");
        assert!(value_of(ready, names, "ema50_slope_bps").is_some());
        assert!(
            rows.iter()
                .any(|row| value_of(row, names, "tick_path_ready") == Some(&Value::Bool(true)))
        );
        assert!(
            rows.iter()
                .any(|row| value_of(row, names, "newly_confirmed_swing_high")
                    == Some(&Value::Bool(true)))
        );
        assert!(
            rows.iter()
                .all(|row| value_of(row, names, "regime_v1").is_some())
        );
        assert!(
            rows.iter().all(|row| {
                let (Some(Value::Time(close)), Some(Value::Time(known))) = (
                    value_of(row, names, "close_time_micros"),
                    value_of(row, names, "known_at_micros"),
                ) else {
                    panic!("clocks")
                };
                known >= close
            }),
            "availability never precedes the logical close"
        );
    }
    for chunk in [1, 7, 1000] {
        assert_eq!(feed(plan, &dataset, &ticks, chunk), full, "chunk {chunk}");
    }
    for percent in [25, 50, 75] {
        let cut = cutoff(&ticks, percent);
        let prefix = feed(plan, &dataset, &ticks[..cut], usize::MAX);
        assert_prefix(&prefix, &full, ticks[cut - 1].event_time_micros, percent);
    }

    // Repeating the build reuses every immutable object and the manifest.
    let again = build(&config).unwrap();
    assert!(
        again[0].ends_with(&format!(
            "objects {} reused {} (already published)",
            manifest.objects.len(),
            manifest.objects.len()
        )),
        "{}",
        again[0]
    );
    assert_eq!(again[1], lines[1]);

    // A run interrupted before the ready manifest leaves an incomplete generation that a rerun
    // completes through the same immutable writes.
    fs::remove_file(&manifest_path).unwrap();
    assert!(verify(&manifest_path).unwrap_err().contains("cannot open"));
    let resumed = build(&config).unwrap();
    assert!(
        resumed[0].ends_with(&format!(
            "objects {} reused {} [",
            manifest.objects.len(),
            manifest.objects.len()
        )) || resumed[0].contains(&format!("reused {} [", manifest.objects.len())),
        "{}",
        resumed[0]
    );
    assert_eq!(fs::read(&manifest_path).unwrap(), manifest.to_json());

    // Conflicting content under a completed identity fails without replacing anything.
    let rows_object = scratch.path("published").join(&manifest.objects[1].key);
    let original = fs::read(&rows_object).unwrap();
    fs::write(&rows_object, b"tampered").unwrap();
    let error = build(&config).unwrap_err();
    assert!(error.contains("already holds different content"), "{error}");
    assert_eq!(fs::read(&rows_object).unwrap(), b"tampered");
    fs::write(&rows_object, &original).unwrap();

    // Applying the frozen plan to an evaluation generation of the same instrument recomputes
    // rows under the frozen settings and encodings without refitting.
    fs::create_dir_all(scratch.path("sources/eval")).unwrap();
    fs::copy(
        scratch.path("sources/ticks/ticks.csv"),
        scratch.path("sources/eval/ticks.csv"),
    )
    .unwrap();
    let eval_source = scratch
        .tick_source()
        .replace("sources/ticks/ticks.csv", "sources/eval/ticks.csv")
        .replace("\"development\"", "\"evaluation\"");
    let evaluation =
        generation(&import(&scratch.config("import_eval.toml", &eval_source)).unwrap()[0]);
    assert_ne!(evaluation, development);
    let eval_manifest = scratch.path(&format!("published/manifests/{evaluation}/ready.json"));
    let frozen = scratch.config(
        "frozen.toml",
        &format!(
            "\n[[features.instruments]]\nrole = \"evaluation\"\ninput_manifest = \"{}\"\nprofile_manifest = \"{}\"\nfrozen_plan = \"{}\"\n",
            manifest_uri(&eval_manifest),
            manifest_uri(&stream_manifest),
            manifest_uri(&manifest_path)
        ),
    );
    let lines = build(&frozen).unwrap();
    assert!(lines[0].contains(" evaluation generation "), "{}", lines[0]);
    let applied_generation = generation(&lines[0]);
    assert_ne!(applied_generation, feature_generation);
    let applied = published_features(
        &scratch.path("published"),
        &scratch.path(&format!(
            "published/manifests/{applied_generation}/ready.json"
        )),
    );
    assert_eq!(
        applied.manifest.frozen_from.as_deref(),
        Some(feature_generation.as_str())
    );
    assert_eq!(applied.manifest.plan_identity, manifest.plan_identity);
    assert_eq!(applied.plan, *plan, "the frozen plan is applied unchanged");
    assert_eq!(
        applied.manifest.objects[0].sha256, manifest.objects[0].sha256,
        "the plan object is reused"
    );
    assert_eq!(
        applied.manifest.streams, manifest.streams,
        "the same ticks yield the same rows"
    );
    assert_eq!(applied.tables, published.tables);

    // Isolation: a declared holdout input and an evaluation input for a new fit are refused by
    // the configuration before anything is resolved.
    for (role, expected) in [("holdout", "holdout"), ("evaluation", "new plan")] {
        let refused = scratch.config(
            "refused.toml",
            &feature_entry(role, &dataset_manifest, &stream_manifest, TICK_SETTINGS),
        );
        let error = build(&refused).unwrap_err();
        assert!(
            error.contains("features.instruments[0].role") && error.contains(expected),
            "{role}: {error}"
        );
    }
    // A declared role that the input manifest contradicts is refused on the manifest alone.
    let mismatched = scratch.config(
        "mismatched.toml",
        &feature_entry(
            "development",
            &eval_manifest,
            &stream_manifest,
            TICK_SETTINGS,
        ),
    );
    let error = build(&mismatched).unwrap_err();
    assert!(
        error.contains("declared `development`") && error.contains("`evaluation`"),
        "{error}"
    );
    // An evaluation-role profile is refused before any child object is read: a store holding
    // only the manifests cannot even satisfy a child read.
    let eval_audit = audit(&audit_config, &eval_manifest);
    let eval_stream = scratch.path(&format!(
        "published/manifests/{}/ready.json",
        generation(&eval_audit)
    ));
    let bare = scratch.path("bare");
    for source in [&dataset_manifest, &eval_stream] {
        let target = bare.join(source.strip_prefix(scratch.path("published")).unwrap());
        fs::create_dir_all(target.parent().unwrap()).unwrap();
        fs::copy(source, target).unwrap();
    }
    let bare_config = scratch.config(
        "bare.toml",
        &feature_entry(
            "development",
            &bare.join(
                dataset_manifest
                    .strip_prefix(scratch.path("published"))
                    .unwrap(),
            ),
            &bare.join(eval_stream.strip_prefix(scratch.path("published")).unwrap()),
            TICK_SETTINGS,
        ),
    );
    let error = build(&bare_config).unwrap_err();
    assert!(error.contains("development-only"), "{error}");
    assert!(
        !error.contains("is missing"),
        "no child object was read: {error}"
    );
    // A profile of another input generation cannot fit a new plan.
    let other_profile = scratch.config(
        "other.toml",
        &feature_entry(
            "development",
            &dataset_manifest,
            &eval_stream,
            TICK_SETTINGS,
        ),
    );
    let error = build(&other_profile).unwrap_err();
    assert!(error.contains("development-only"), "{error}");
    // One owner per instrument, role, and stream: a second development profile of the same
    // instrument is refused before anything is streamed or published.
    let other_audit = scratch.config(
        "audit_other.toml",
        &tick_instrument().replace("min_observations = 20", "min_observations = 21"),
    );
    let other_stream = scratch.path(&format!(
        "published/manifests/{}/ready.json",
        generation(&audit(&other_audit, &dataset_manifest))
    ));
    assert_ne!(other_stream, stream_manifest);
    let before = feature_manifests(&scratch, "published");
    let duplicate = scratch.config(
        "duplicate.toml",
        &format!(
            "{}{}",
            feature_entry(
                "development",
                &dataset_manifest,
                &stream_manifest,
                TICK_SETTINGS
            ),
            feature_entry(
                "development",
                &dataset_manifest,
                &other_stream,
                TICK_SETTINGS
            )
        ),
    );
    let error = build(&duplicate).unwrap_err();
    assert!(
        error.contains(
            "features.instruments[1]: pocket_option:AEDCNY_otc development 15s/5s is already owned by features.instruments[0]"
        ),
        "{error}"
    );
    assert_eq!(feature_manifests(&scratch, "published"), before);
}

#[test]
fn bars_exclude_tick_outputs_with_their_reason_and_named_tick_requests_fail() {
    let scratch = Scratch::new("phase04_bars");
    let mut rows = Vec::new();
    let mut state: u64 = 0x9E37_79B9_7F4A_7C15;
    let mut price = 100.0_f64;
    let start = 1_747_653_300;
    for index in 0..3_000_i64 {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        let step = ((state >> 20) % 21) as f64 - 10.0;
        let open = price;
        let close = (price + step * 0.001 * 1000.0).round() / 1000.0;
        let high = open.max(close) + ((state >> 8) % 5) as f64 * 0.001;
        let low = open.min(close) - ((state >> 12) % 5) as f64 * 0.001;
        price = close;
        rows.push(bar(
            "AAPL_otc",
            7,
            start + index * 5,
            [
                open,
                (high * 1000.0).round() / 1000.0,
                (low * 1000.0).round() / 1000.0,
                close,
                3.0,
            ],
        ));
    }
    write_collection(
        &scratch.path("sources/bars"),
        &[AssetSpec {
            asset: "AAPL_otc",
            expected_symbol_id: Some(7),
            symbol_id: Some(7),
            files: vec![rows],
            metadata: true,
        }],
    );
    let import_config = scratch.config(
        "import.toml",
        &scratch
            .bar_source()
            .replace("\"evaluation\"", "\"development\""),
    );
    let dataset = generation(&import(&import_config).unwrap()[0]);
    let dataset_manifest = scratch.path(&format!("published/manifests/{dataset}/ready.json"));
    let audit_config = scratch.config("audit.toml", &bar_instrument("AAPL_otc"));
    let stream_manifest = scratch.path(&format!(
        "published/manifests/{}/ready.json",
        generation(&audit(&audit_config, &dataset_manifest))
    ));
    let config = scratch.config(
        "features.toml",
        &feature_entry(
            "development",
            &dataset_manifest,
            &stream_manifest,
            BAR_SETTINGS,
        ),
    );
    let lines = build(&config).unwrap();
    let manifest_path = scratch.path(&format!(
        "published/manifests/{}/ready.json",
        generation(&lines[0])
    ));
    let published = published_features(&scratch.path("published"), &manifest_path);
    let plan = &published.plan;
    assert!(!plan.profile.ticks);
    let stream = &plan.streams[0];
    assert!(!stream.tick_path);
    let excluded: BTreeMap<&str, &str> = stream
        .excluded
        .iter()
        .map(|e| (e.name.as_str(), e.reason.as_str()))
        .collect();
    for name in [
        "tick_volume",
        "complete",
        "has_gap",
        "frozen_price_flag",
        "max_abs_tick_jump_bps",
        "tick_path_ready",
        "tick_path_pressure_bucket",
        "tick_volume_mean_5",
        "tick_volume_vs_recent_ratio",
        "tick_volume_vs_recent_bucket",
        "regime_quality_state",
        "regime_v1",
        "is_regime_clean",
    ] {
        assert!(
            excluded
                .get(name)
                .is_some_and(|reason| reason.contains("individual ticks")),
            "{name}: {:?}",
            excluded.get(name)
        );
    }
    assert!(
        !excluded.contains_key("tick_volume_dev_quantile") && excluded.len() < 70,
        "{excluded:?}"
    );
    let selected: Vec<&str> = stream.outputs.iter().map(|o| o.name.as_str()).collect();
    for name in [
        "regime_trend_state",
        "regime_volatility_state",
        "regime_structure_state",
        "regime_transition_state",
        "regime_directional_bias",
        "is_regime_trending",
        "market_structure_bias",
        "ema8",
        "ema21",
        "is_ema21_ready",
        "candle_type",
        "momentum_10_bps",
        "missing_buckets_since_prev_candle",
        "active_span_micros",
        "swing_high_type",
    ] {
        assert!(selected.contains(&name), "{name} is bar-compatible");
    }
    assert!(!selected.contains(&"ema20_minus_ema50_bps"));
    assert!(
        excluded
            .get("ema20_minus_ema50_bps")
            .is_some_and(|reason| reason.contains("period 20"))
    );
    assert_eq!(
        stream
            .encodings
            .iter()
            .map(|e| e.output.as_str())
            .collect::<Vec<_>>(),
        ["candle_type", "regime_trend_state", "body_bps_bucketed"]
    );
    assert_eq!(
        plan.settings.price_epsilon_units,
        Some(1),
        "0.001 at scale 3"
    );
    let (names, rows) = &published.tables[0][0];
    assert!(rows.len() > 30);
    assert!(
        rows.iter()
            .all(|row| value_of(row, names, "regime_trend_state").is_some())
    );
    assert!(!names.contains(&"tick_volume".to_string()));
    let first = &rows[0];
    assert_eq!(
        value_of(first, names, "is_ema8_ready"),
        Some(&Value::Bool(false))
    );
    let ready = rows
        .iter()
        .find(|row| value_of(row, names, "is_ema8_ready") == Some(&Value::Bool(true)))
        .unwrap();
    assert_ne!(
        value_of(ready, names, "ema8_slope_state"),
        Some(&Value::Text("not_ready".into()))
    );
    for named in [
        "tick_path_pressure_bucket",
        "regime_quality_state",
        "regime_v1",
        "tick_volume",
    ] {
        let request = scratch.config(
            "named.toml",
            &feature_entry(
                "development",
                &dataset_manifest,
                &stream_manifest,
                &BAR_SETTINGS.replace(
                    "outputs = \"all_supported\"",
                    &format!("outputs = [\"{named}\"]"),
                ),
            ),
        );
        let error = build(&request).unwrap_err();
        assert!(
            error.contains(named) && error.contains("individual ticks"),
            "{named}: {error}"
        );
    }
    let unknown = scratch.config(
        "unknown.toml",
        &feature_entry(
            "development",
            &dataset_manifest,
            &stream_manifest,
            &BAR_SETTINGS.replace(
                "outputs = \"all_supported\"",
                "outputs = [\"body_bps\", \"not_an_output\"]",
            ),
        ),
    );
    assert!(build(&unknown).unwrap_err().contains("not_an_output"));
    // Without encodings a stream publishes its three tables and no encoded object.
    let unencoded = scratch.config(
        "unencoded.toml",
        &feature_entry(
            "development",
            &dataset_manifest,
            &stream_manifest,
            &BAR_SETTINGS[..BAR_SETTINGS.find("encodings").unwrap()],
        ),
    );
    let lines = build(&unencoded).unwrap();
    let unencoded_manifest = scratch.path(&format!(
        "published/manifests/{}/ready.json",
        generation(&lines[0])
    ));
    let bare = published_features(&scratch.path("published"), &unencoded_manifest);
    assert!(bare.plan.streams[0].encodings.is_empty());
    assert_eq!(bare.manifest.objects.len(), 4);
    assert_eq!(bare.tables[0].len(), 3);
    assert_eq!(
        bare.tables[0][0], published.tables[0][0],
        "rows do not depend on encodings"
    );
    assert_eq!(lines[1], verify(&unencoded_manifest).unwrap());
    // Any settings change is another raw identity and therefore another plan.
    let mut other = plan.settings.clone();
    other.rolling_window = Some(31);
    assert_ne!(
        plan.raw_identity,
        raw_identity(&plan.profile, &dataset, &other)
    );
}

// ---------------------------------------------------------------------------------------------
// Governed reference parity
// ---------------------------------------------------------------------------------------------

#[derive(serde::Deserialize)]
struct GovernedConfig {
    /// The research configuration `features build` runs: filesystem publication, the Phase 02
    /// input, and the Phase 03 development profile with the five reference streams.
    application_config: PathBuf,
    /// The legacy reference root whose expected files the checked-in allowlist names.
    reference_root: PathBuf,
}

#[derive(serde::Deserialize)]
struct ReferenceFixture {
    legacy_revision: String,
    source_sha256: String,
    source_rows: u64,
    policies: BTreeMap<String, String>,
    reference_files: Vec<ReferenceFile>,
    streams: Vec<ReferenceStream>,
}

#[derive(serde::Deserialize)]
struct ReferenceFile {
    path: String,
    sha256: String,
}

#[derive(serde::Deserialize)]
struct ReferenceStream {
    label: String,
    duration_seconds: u32,
    offset_seconds: u32,
    rows: u64,
    structure_events: u64,
    sequence_events: u64,
}

/// How one legacy column projects from the canonical outputs for exact text comparison.
#[derive(Clone)]
enum Projection {
    /// A constant the reference wrote on every row.
    Const(String),
    /// A microsecond clock rendered as the reference's millisecond text, or empty when absent.
    TimeMs(&'static str),
    /// Integer units at scale six rendered at eight decimal places, or empty when absent.
    Price8(&'static str),
    /// A six-place value rendered at six decimals, or empty when absent.
    Six(&'static str),
    /// An integer, or empty when absent.
    Int(&'static str),
    /// A one-based ordinal rendered as the reference's line number (ordinal plus one).
    IntPlusOne(&'static str),
    /// A boolean rendered as `1` or `0`.
    Bool(&'static str),
    /// Text as is, or empty when absent.
    Text(&'static str),
    /// Microseconds rendered as whole milliseconds.
    Millis(&'static str),
    /// A tick-path output, or the reference's dummy value on a stream without a path.
    TickPath(&'static str, Box<Projection>, &'static str),
    /// A structure-event reference: the literal for swings, else the referenced close.
    EventReference,
}

fn ms_text(micros: i64) -> String {
    assert_eq!(micros % 1_000, 0, "reference clocks are whole milliseconds");
    let text = format_event_time_micros(micros);
    format!("{}Z", &text[..text.len() - 4])
}

fn price8(units: i64) -> String {
    format!("{:.8}", units as f64 / 1_000_000.0)
}

impl Projection {
    fn render(&self, row: &[Option<Value>], names: &[String], tick_path: bool) -> String {
        let get = |name: &str| -> Option<&Value> {
            let index = names
                .iter()
                .position(|n| n == name)
                .unwrap_or_else(|| panic!("column {name} is not published"));
            row[index].as_ref()
        };
        match self {
            Self::Const(text) => text.clone(),
            Self::TimeMs(name) => match get(name) {
                Some(Value::Time(micros)) => ms_text(*micros),
                None => String::new(),
                other => panic!("{name}: {other:?} is not a clock"),
            },
            Self::Price8(name) => match get(name) {
                Some(Value::Int(units)) => price8(*units),
                None => String::new(),
                other => panic!("{name}: {other:?} is not units"),
            },
            Self::Six(name) => match get(name) {
                Some(Value::Float(value)) => format!("{value:.6}"),
                None => String::new(),
                other => panic!("{name}: {other:?} is not a float"),
            },
            Self::Int(name) => match get(name) {
                Some(Value::Int(value)) => value.to_string(),
                None => String::new(),
                other => panic!("{name}: {other:?} is not an integer"),
            },
            Self::IntPlusOne(name) => match get(name) {
                Some(Value::Int(value)) => (value + 1).to_string(),
                other => panic!("{name}: {other:?} is not an ordinal"),
            },
            Self::Bool(name) => match get(name) {
                Some(Value::Bool(value)) => u8::from(*value).to_string(),
                other => panic!("{name}: {other:?} is not a boolean"),
            },
            Self::Text(name) => match get(name) {
                Some(Value::Text(text)) => text.to_string(),
                None => String::new(),
                other => panic!("{name}: {other:?} is not text"),
            },
            Self::Millis(name) => match get(name) {
                Some(Value::Int(micros)) => {
                    assert_eq!(
                        micros % 1_000,
                        0,
                        "{name}: {micros} is not whole milliseconds"
                    );
                    (micros / 1_000).to_string()
                }
                other => panic!("{name}: {other:?} is not a duration"),
            },
            Self::TickPath(name, inner, dummy) => {
                if tick_path {
                    assert!(
                        names.iter().any(|n| n == name),
                        "{name} is selected on a tick-path stream"
                    );
                    inner.render(row, names, tick_path)
                } else {
                    assert!(
                        !names.iter().any(|n| n == name),
                        "{name} is excluded without a path"
                    );
                    (*dummy).to_string()
                }
            }
            Self::EventReference => match (get("reference"), get("reference_close_micros")) {
                (Some(Value::Text(kind)), None) if kind == "strict_left_right" => kind.to_string(),
                (Some(Value::Text(_)), Some(Value::Time(micros))) => ms_text(*micros),
                other => panic!("reference {other:?}"),
            },
        }
    }
}

/// The projection of every physical field of the three legacy row tables.
fn row_projection(
    stream: &ReferenceStream,
    plan: &FeaturePlan,
    regime_policy_hash: &str,
) -> BTreeMap<&'static str, Projection> {
    use Projection::*;
    let spec = plan
        .profile
        .definition
        .candles
        .iter()
        .find(|spec| {
            spec.duration_seconds == stream.duration_seconds
                && spec.offset_seconds == stream.offset_seconds
        })
        .unwrap();
    let mut map: BTreeMap<&'static str, Projection> = BTreeMap::new();
    let mut put = |name: &'static str, projection: Projection| {
        assert!(
            map.insert(name, projection).is_none(),
            "{name} projected twice"
        );
    };
    put(
        "timeframe_seconds",
        Const(stream.duration_seconds.to_string()),
    );
    put("offset_seconds", Const(stream.offset_seconds.to_string()));
    put("candle_set", Const(stream.label.clone()));
    put("source_row_number", IntPlusOne("candle_ordinal"));
    put("open_time_utc", TimeMs("open_time_micros"));
    put("close_time_utc", TimeMs("close_time_micros"));
    put("first_tick_time_utc", TimeMs("first_event_micros"));
    put("last_tick_time_utc", TimeMs("last_event_micros"));
    put("row_decision_time_utc", TimeMs("close_time_micros"));
    for (legacy, target) in [
        ("open", "open_units"),
        ("high", "high_units"),
        ("low", "low_units"),
        ("close", "close_units"),
        ("body", "body_units"),
        ("range", "range_units"),
        ("upper_wick", "upper_wick_units"),
        ("lower_wick", "lower_wick_units"),
        ("last_swing_high_price", "last_swing_high_units"),
        ("last_swing_low_price", "last_swing_low_units"),
        ("last_confirmed_HH_price", "last_confirmed_HH_units"),
        ("last_confirmed_HL_price", "last_confirmed_HL_units"),
        ("last_confirmed_LH_price", "last_confirmed_LH_units"),
        ("last_confirmed_LL_price", "last_confirmed_LL_units"),
    ] {
        put(legacy, Price8(target));
    }
    put("tick_volume", Int("tick_volume"));
    put("active_span_ms", Millis("active_span_micros"));
    for name in [
        "complete",
        "low_tick_volume",
        "hard_low_tick_volume",
        "has_gap",
        "has_internal_gap",
        "starts_after_gap",
        "frozen_price_flag",
        "has_true_tick_jump",
        "has_feed_delay_jump",
        "has_gap_reopen_jump",
        "range_like",
        "pullback_against_trend",
        "newly_confirmed_swing_high",
        "newly_confirmed_swing_low",
        "breakout_up",
        "breakout_down",
        "sweep_reject_high",
        "sweep_reject_low",
        "failed_breakout_up",
        "failed_breakout_down",
        "break_of_structure",
        "change_of_character",
        "newly_confirmed_HH",
        "newly_confirmed_HL",
        "newly_confirmed_LH",
        "newly_confirmed_LL",
        "has_adjacent_previous_clean_candle",
        "is_doji",
        "is_strong_body",
        "is_pin_bar",
        "is_hammer_like",
        "is_shooting_star_like",
        "is_inside_bar",
        "is_outside_bar",
        "is_expansion_candle",
        "is_compression_candle",
        "is_regime_clean",
        "is_regime_trending",
        "is_regime_ranging",
        "is_regime_transition",
    ] {
        put(name, Bool(name));
    }
    put(
        "research_min_ticks",
        Const(spec.min_observations.unwrap().to_string()),
    );
    put(
        "hard_min_ticks",
        Const(spec.hard_min_observations.unwrap().to_string()),
    );
    put("starts_after_gap_ms", Millis("starts_after_gap_micros"));
    put("max_internal_gap_ms", Millis("max_internal_gap_micros"));
    put("max_gap_ms", Millis("max_gap_micros"));
    put("max_same_price_run_ms", Millis("max_same_price_run_micros"));
    for name in [
        "starts_after_gap_class",
        "worst_gap_class",
        "candle_direction",
        "compression_state",
        "directional_state",
        "trend_leg_direction",
        "structure_state",
        "current_event_types",
        "swing_high_type",
        "swing_low_type",
        "last_swing_high_type",
        "last_swing_low_type",
        "market_structure_sequence",
        "market_structure_bias",
        "previous_candle_relation",
        "candle_color",
        "candle_type",
        "range_bps_bucket",
        "body_bps_bucket",
        "range_vs_recent_bucket",
        "body_vs_recent_bucket",
        "tick_volume_vs_recent_bucket",
        "upper_wick_size_bucket",
        "lower_wick_size_bucket",
        "wick_profile",
        "close_location_bucket",
        "body_dominance",
        "regime_trend_state",
        "regime_volatility_state",
        "regime_structure_state",
        "regime_transition_state",
        "regime_quality_state",
        "regime_directional_bias",
        "regime_v1",
        "quality_tier",
    ] {
        put(name, Text(name));
    }
    put("candle_size_bucket", Text("range_vs_recent_bucket"));
    put("body_size_bucket", Text("body_vs_recent_bucket"));
    for name in [
        "missing_buckets_since_prev_candle",
        "max_same_price_run_ticks",
        "trend_leg_age_candles",
        "bars_since_last_swing_high_known",
        "bars_since_last_swing_low_known",
        "bars_since_last_confirmed_HH_known",
        "bars_since_last_confirmed_HL_known",
        "bars_since_last_confirmed_LH_known",
        "bars_since_last_confirmed_LL_known",
        "clean_segment_index",
        "clean_segment_candle_index",
        "prior_clean_history_count",
    ] {
        put(name, Int(name));
    }
    for name in [
        "max_abs_tick_jump_bps",
        "max_true_tick_jump_bps",
        "max_gap_reopen_jump_bps",
        "body_bps",
        "range_bps",
        "upper_wick_bps",
        "lower_wick_bps",
        "close_position",
        "body_to_range",
        "upper_wick_to_range",
        "lower_wick_to_range",
        "return_1_bps",
        "momentum_5_bps",
        "directional_efficiency_5",
        "abs_return_mean_5_bps",
        "range_mean_5_bps",
        "tick_volume_mean_5",
        "momentum_10_bps",
        "directional_efficiency_10",
        "abs_return_mean_10_bps",
        "range_mean_10_bps",
        "tick_volume_mean_10",
        "momentum_20_bps",
        "directional_efficiency_20",
        "abs_return_mean_20_bps",
        "range_mean_20_bps",
        "tick_volume_mean_20",
        "range_to_avg20",
        "distance_to_last_swing_high_bps",
        "distance_to_last_swing_low_bps",
        "range_vs_recent_ratio",
        "body_vs_recent_ratio",
        "tick_volume_vs_recent_ratio",
    ] {
        put(name, Six(name));
    }
    put(
        "tick_path_pressure_policy",
        Const(plan.definitions.tick_path.clone()),
    );
    put(
        "tick_path_pressure_enabled",
        Const(
            u8::from(
                plan.stream(binary_alpha_engine::config::StreamKey {
                    duration_seconds: stream.duration_seconds,
                    offset_seconds: stream.offset_seconds,
                })
                .unwrap()
                .tick_path,
            )
            .to_string(),
        ),
    );
    put(
        "tick_path_ready",
        TickPath("tick_path_ready", Box::new(Bool("tick_path_ready")), "0"),
    );
    for name in [
        "tick_path_directional_move_count",
        "tick_path_uptick_count",
        "tick_path_downtick_count",
        "tick_path_flat_count",
        "tick_path_direction_change_count",
        "tick_path_terminal_move_count",
    ] {
        put(name, TickPath(name, Box::new(Int(name)), "0"));
    }
    for name in [
        "tick_path_signed_imbalance",
        "tick_path_reversal_rate",
        "tick_path_efficiency",
        "tick_path_terminal_signed_imbalance",
    ] {
        put(name, TickPath(name, Box::new(Six(name)), "0.000000"));
    }
    put(
        "tick_path_close_position",
        TickPath(
            "tick_path_close_position",
            Box::new(Six("tick_path_close_position")),
            "0.500000",
        ),
    );
    for name in [
        "tick_path_pressure_bucket",
        "tick_path_shape_bucket",
        "tick_path_terminal_pressure_bucket",
        "tick_path_failed_pressure_direction",
        "tick_path_efficiency_bucket",
        "tick_path_reversal_bucket",
    ] {
        put(
            name,
            TickPath(name, Box::new(Text(name)), "unsupported_timeframe"),
        );
    }
    put(
        "research_policy",
        Const("aedcny_binary_research_v1".to_string()),
    );
    put("research_usable", Const("1".to_string()));
    put(
        "label_policy",
        Const("aedcny_structure_labels_v1".to_string()),
    );
    put("label_knowable_time_utc", TimeMs("close_time_micros"));
    for (legacy, target) in [
        (
            "last_swing_high_event_time_utc",
            "last_swing_high_event_close_micros",
        ),
        (
            "last_swing_high_knowable_time_utc",
            "last_swing_high_confirm_close_micros",
        ),
        (
            "last_swing_low_event_time_utc",
            "last_swing_low_event_close_micros",
        ),
        (
            "last_swing_low_knowable_time_utc",
            "last_swing_low_confirm_close_micros",
        ),
        (
            "current_event_knowable_time_utc",
            "current_event_close_micros",
        ),
        (
            "last_confirmed_HH_event_time_utc",
            "last_confirmed_HH_event_close_micros",
        ),
        (
            "last_confirmed_HH_knowable_time_utc",
            "last_confirmed_HH_confirm_close_micros",
        ),
        (
            "last_confirmed_HL_event_time_utc",
            "last_confirmed_HL_event_close_micros",
        ),
        (
            "last_confirmed_HL_knowable_time_utc",
            "last_confirmed_HL_confirm_close_micros",
        ),
        (
            "last_confirmed_LH_event_time_utc",
            "last_confirmed_LH_event_close_micros",
        ),
        (
            "last_confirmed_LH_knowable_time_utc",
            "last_confirmed_LH_confirm_close_micros",
        ),
        (
            "last_confirmed_LL_event_time_utc",
            "last_confirmed_LL_event_close_micros",
        ),
        (
            "last_confirmed_LL_knowable_time_utc",
            "last_confirmed_LL_confirm_close_micros",
        ),
    ] {
        put(legacy, TimeMs(target));
    }
    for name in [
        "no_lookahead_check_pass",
        "structure_sequence_no_lookahead_check_pass",
        "candle_features_no_lookahead_check_pass",
        "regime_no_lookahead_check_pass",
    ] {
        put(name, Const("1".to_string()));
    }
    put(
        "sequence_policy",
        Const("aedcny_structure_sequence_v1".to_string()),
    );
    put(
        "sequence_label_knowable_time_utc",
        TimeMs("close_time_micros"),
    );
    put(
        "candle_feature_policy",
        Const("aedcny_candle_features_v1".to_string()),
    );
    put(
        "feature_label_knowable_time_utc",
        TimeMs("close_time_micros"),
    );
    put(
        "regime_policy",
        Const("aedcny_regime_features_v1".to_string()),
    );
    put("regime_policy_hash", Const(regime_policy_hash.to_string()));
    put(
        "regime_label_knowable_time_utc",
        TimeMs("close_time_micros"),
    );
    map
}

fn structure_event_projection(stream: &ReferenceStream) -> Vec<(&'static str, Projection)> {
    use Projection::*;
    vec![
        ("event_id", Int("event_id")),
        ("candle_set", Const(stream.label.clone())),
        ("event_type", Text("event_type")),
        ("event_direction", Text("event_direction")),
        ("event_time_utc", TimeMs("event_close_micros")),
        ("knowable_time_utc", TimeMs("confirm_close_micros")),
        ("event_index", Int("event_row")),
        ("knowable_index", Int("confirm_row")),
        ("source_row_number", IntPlusOne("event_candle_ordinal")),
        (
            "knowable_source_row_number",
            IntPlusOne("confirm_candle_ordinal"),
        ),
        ("price", Price8("price_units")),
        ("level", Price8("level_units")),
        ("reference", EventReference),
    ]
}

fn sequence_event_projection(stream: &ReferenceStream) -> Vec<(&'static str, Projection)> {
    use Projection::*;
    vec![
        ("sequence_event_id", Int("event_id")),
        ("candle_set", Const(stream.label.clone())),
        ("row_index", Int("row")),
        ("source_row_number", IntPlusOne("candle_ordinal")),
        ("row_decision_time_utc", TimeMs("decision_close_micros")),
        ("swing_event_type", Text("swing_event_type")),
        ("swing_type", Text("swing_type")),
        ("swing_price", Price8("swing_price_units")),
        ("swing_event_time_utc", TimeMs("swing_event_close_micros")),
        (
            "swing_knowable_time_utc",
            TimeMs("swing_confirm_close_micros"),
        ),
        ("previous_same_side_price", Price8("previous_price_units")),
        (
            "previous_same_side_event_time_utc",
            TimeMs("previous_event_close_micros"),
        ),
        (
            "previous_same_side_knowable_time_utc",
            TimeMs("previous_confirm_close_micros"),
        ),
        ("market_structure_sequence_after", Text("sequence_after")),
        ("market_structure_bias_after", Text("bias_after")),
        ("no_lookahead_check_pass", Const("1".to_string())),
    ]
}

/// A streaming reader of one legacy CSV: the header and one split row at a time.
struct LegacyCsv {
    header: Vec<String>,
    lines: std::io::Lines<std::io::BufReader<fs::File>>,
    path: PathBuf,
}

impl LegacyCsv {
    fn open(path: &Path) -> Self {
        use std::io::BufRead;
        let mut lines =
            std::io::BufReader::with_capacity(1 << 20, fs::File::open(path).unwrap()).lines();
        let header: Vec<String> = lines
            .next()
            .unwrap()
            .unwrap()
            .split(',')
            .map(str::to_string)
            .collect();
        Self {
            header,
            lines,
            path: path.to_path_buf(),
        }
    }

    fn next_row(&mut self) -> Option<Vec<String>> {
        let line = self.lines.next()?.unwrap();
        let row: Vec<String> = line.split(',').map(str::to_string).collect();
        assert_eq!(
            row.len(),
            self.header.len(),
            "{}: ragged row",
            self.path.display()
        );
        Some(row)
    }
}

/// Streams the rows of a published table through the row API.
fn table_rows(
    path: &Path,
) -> (
    Vec<String>,
    impl Iterator<Item = Vec<Option<Value>>> + use<>,
) {
    let reader = SerializedFileReader::new(fs::File::open(path).unwrap()).unwrap();
    let names: Vec<String> = reader
        .metadata()
        .file_metadata()
        .schema_descr()
        .columns()
        .iter()
        .map(|column| column.name().to_string())
        .collect();
    let rows = reader.into_iter().map(|row| {
        row.unwrap()
            .get_column_iter()
            .map(|(_, field)| match field {
                Field::Null => None,
                Field::Long(value) => Some(Value::Int(*value)),
                Field::Short(value) => Some(Value::Int(i64::from(*value))),
                Field::Double(value) => Some(Value::Float(*value)),
                Field::Bool(value) => Some(Value::Bool(*value)),
                Field::Str(value) => Some(Value::Text(Cow::Owned(value.clone()))),
                Field::TimestampMicros(value) => Some(Value::Time(*value)),
                other => panic!("unexpected field {other:?}"),
            })
            .collect()
    });
    (names, rows)
}

fn in_process_peak_kb() -> u64 {
    fs::read_to_string("/proc/self/status")
        .unwrap()
        .lines()
        .find_map(|line| line.strip_prefix("VmHWM:"))
        .unwrap()
        .trim()
        .trim_end_matches(" kB")
        .parse()
        .unwrap()
}

/// Compares a published table against legacy CSV tables row by row under a projection,
/// returning the number of rows compared; the first mismatches fail the test with detail.
fn compare_rows(
    label: &str,
    published: &Path,
    legacy: &mut [LegacyCsv],
    projection: &BTreeMap<&'static str, Projection>,
    tick_path: bool,
) -> u64 {
    let (names, rows) = table_rows(published);
    let mut compared = 0;
    let mut mismatches = Vec::new();
    for row in rows {
        for csv in legacy.iter_mut() {
            let legacy_row = csv.next_row().unwrap_or_else(|| {
                panic!(
                    "{label}: {} ends before the published rows",
                    csv.path.display()
                )
            });
            for (column, expected) in csv.header.iter().zip(&legacy_row) {
                let projected = projection[column.as_str()].render(&row, &names, tick_path);
                if projected != *expected && mismatches.len() < 20 {
                    mismatches.push(format!(
                        "{label} row {compared} {column}: target `{projected}` legacy `{expected}`"
                    ));
                }
            }
        }
        compared += 1;
    }
    assert!(mismatches.is_empty(), "{}", mismatches.join("\n"));
    for csv in legacy {
        assert!(
            csv.next_row().is_none(),
            "{label}: {} has more rows than published",
            csv.path.display()
        );
    }
    compared
}

fn compare_events(
    label: &str,
    published: &Path,
    legacy: &mut LegacyCsv,
    projection: &[(&'static str, Projection)],
) -> u64 {
    assert_eq!(
        legacy.header,
        projection
            .iter()
            .map(|(name, _)| name.to_string())
            .collect::<Vec<_>>(),
        "{label}: event header"
    );
    let (names, rows) = table_rows(published);
    let mut compared = 0;
    let mut mismatches = Vec::new();
    for row in rows {
        let legacy_row = legacy
            .next_row()
            .unwrap_or_else(|| panic!("{label}: legacy events end early"));
        for ((column, projection), expected) in projection.iter().zip(&legacy_row) {
            let projected = projection.render(&row, &names, true);
            if projected != *expected && mismatches.len() < 20 {
                mismatches.push(format!(
                    "{label} event {compared} {column}: target `{projected}` legacy `{expected}`"
                ));
            }
        }
        compared += 1;
    }
    assert!(mismatches.is_empty(), "{}", mismatches.join("\n"));
    assert!(
        legacy.next_row().is_none(),
        "{label}: legacy has more events"
    );
    compared
}

#[test]
#[ignore = "needs BINARY_ALPHA_TEST_CONFIG naming the research configuration and the reference root"]
fn governed_reference_parity() {
    let config_path = std::env::var("BINARY_ALPHA_TEST_CONFIG")
        .expect("BINARY_ALPHA_TEST_CONFIG names the governed test configuration");
    let governed: GovernedConfig =
        serde_json::from_slice(&fs::read(&config_path).unwrap()).unwrap();
    let fixture: ReferenceFixture = serde_json::from_slice(
        &fs::read(
            Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/phase04_reference.json"),
        )
        .unwrap(),
    )
    .unwrap();
    let root = &governed.reference_root;

    // Every allowlisted reference file and policy carries exactly its recorded content hash.
    let started = std::time::Instant::now();
    for (path, expected) in fixture
        .reference_files
        .iter()
        .map(|file| (&file.path, &file.sha256))
        .chain(fixture.policies.iter())
    {
        let path = root.join(path);
        assert_eq!(sha256(&path), *expected, "{}", path.display());
    }
    println!(
        "reference allowlist: {} tables and {} policies hashed in {:.1} s against legacy revision {}",
        fixture.reference_files.len(),
        fixture.policies.len(),
        started.elapsed().as_secs_f64(),
        fixture.legacy_revision
    );

    // The application command over the governed input under GNU time.
    let config = Config::parse(&fs::read_to_string(&governed.application_config).unwrap()).unwrap();
    let entry = &config.features.as_ref().unwrap().instruments[0];
    let input = GenerationManifest::from_json(
        &fs::read(
            entry
                .input_manifest
                .to_string()
                .strip_prefix("file://")
                .unwrap(),
        )
        .unwrap(),
    )
    .unwrap();
    assert_eq!(input.row_count, fixture.source_rows);
    assert_eq!(
        input.inputs[0].sha256, fixture.source_sha256,
        "the input generation was imported from the registered source"
    );
    let started = std::time::Instant::now();
    let output = std::process::Command::new("/usr/bin/time")
        .args([
            "-v",
            env!("CARGO_BIN_EXE_binary-alpha"),
            "features",
            "build",
            "--config",
            governed.application_config.to_str().unwrap(),
        ])
        .output()
        .expect("GNU time runs the build");
    let wall = started.elapsed().as_secs_f64();
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(output.status.success(), "{stderr}");
    let peak: u64 = stderr
        .lines()
        .find_map(|line| {
            line.trim()
                .strip_prefix("Maximum resident set size (kbytes): ")
        })
        .unwrap()
        .parse()
        .unwrap();
    let stdout = String::from_utf8(output.stdout).unwrap();
    let lines: Vec<&str> = stdout.lines().collect();
    println!("build: {}", lines[0]);
    println!(
        "build wall {wall:.3} s, peak resident {peak} kB (streaming, fit, encoding, and publication in one process)"
    );
    println!("reconstruction: {}", lines[1]);
    let generation = generation(lines[0]);
    let published_root = match &config.storage.publication_uri {
        binary_alpha_engine::config::PublicationUri::Filesystem(path) => path.clone(),
        other => panic!("{other} is not the filesystem boundary"),
    };
    let manifest_path = published_root.join(format!("manifests/{generation}/ready.json"));
    println!("verify: {}", verify(&manifest_path).unwrap());
    let published = published_features(&published_root, &manifest_path);
    let (manifest, plan) = (&published.manifest, &published.plan);
    assert_eq!(manifest.code_revision, env!("BINARY_ALPHA_CODE_REVISION"));
    assert!(
        !manifest.code_revision.ends_with("-dirty") && manifest.code_revision != "unavailable",
        "the governed proof binds to a clean commit, not {}",
        manifest.code_revision
    );
    assert_eq!(manifest.config_hash, config.content_hash());
    assert_eq!(manifest.observations, fixture.source_rows);
    println!(
        "feature generation {} plan {} raw identity {} config {} profile {} input {}",
        manifest.generation,
        manifest.plan_identity,
        plan.raw_identity,
        manifest.config_hash,
        manifest.profile_generation,
        manifest.input_generation
    );
    println!("definitions: {:?}", plan.definitions);

    // Breadth: every stream selects every compiled output, and the 201 physical fields of the
    // three legacy row tables plus the derived row identity all project from canonical outputs.
    assert_eq!(plan.streams.len(), fixture.streams.len());
    let mut physical: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
    let mut total_rows = 0;
    let mut total_structure = 0;
    let mut total_sequence = 0;
    let regime_policy_hash = {
        let mut csv = LegacyCsv::open(
            &root.join("regimes/phase4d_regime_features/regime_features_5s_offset0s.csv"),
        );
        let row = csv.next_row().unwrap();
        row[csv
            .header
            .iter()
            .position(|h| h == "regime_policy_hash")
            .unwrap()]
        .clone()
    };
    for (index, reference) in fixture.streams.iter().enumerate() {
        let stream = &plan.streams[index];
        assert_eq!(
            (stream.duration_seconds, stream.offset_seconds),
            (reference.duration_seconds, reference.offset_seconds)
        );
        // The reference configured no moving averages, so only the fixed 20/50 pair outputs are
        // excluded, each with the missing period as its reason.
        let excluded: Vec<&str> = stream
            .excluded
            .iter()
            .map(|exclusion| exclusion.name.as_str())
            .collect();
        assert_eq!(
            excluded,
            [
                "ema20_minus_ema50_bps",
                "is_ema20_above_ema50",
                "ema20_ema50_alignment_state"
            ],
            "{:?}",
            stream.excluded
        );
        assert!(
            stream
                .excluded
                .iter()
                .all(|exclusion| exclusion.reason.contains("moving-average period 20"))
        );
        assert_eq!(
            stream.tick_path,
            reference.duration_seconds <= 30,
            "the reference enabled paths on 5, 15, and 30 seconds"
        );
        let summary = &manifest.streams[index];
        assert_eq!(
            (
                summary.rows,
                summary.structure_events,
                summary.sequence_events
            ),
            (
                reference.rows,
                reference.structure_events,
                reference.sequence_events
            ),
            "{}",
            reference.label
        );
        let object = |path: &str| {
            published_root.join(
                &manifest
                    .objects
                    .iter()
                    .find(|object| object.path == path)
                    .unwrap()
                    .key,
            )
        };
        let paths = stream.object_paths();
        let (rows_path, structure_path, sequence_path) = (&paths[0], &paths[1], &paths[2]);
        let mut legacy = [
            LegacyCsv::open(&root.join(format!(
                "labels/phase4b_structure_sequence_swing_3x3/sequenced_labeled_candles_{}.csv",
                reference.label
            ))),
            LegacyCsv::open(&root.join(format!(
                "features/phase4c_candle_features/candle_features_{}.csv",
                reference.label
            ))),
            LegacyCsv::open(&root.join(format!(
                "regimes/phase4d_regime_features/regime_features_{}.csv",
                reference.label
            ))),
        ];
        for csv in &legacy {
            physical.extend(csv.header.iter().cloned());
        }
        let projection = row_projection(reference, plan, &regime_policy_hash);
        for csv in &legacy {
            for column in &csv.header {
                assert!(
                    projection.contains_key(column.as_str()),
                    "{}: legacy field {column} has no projection",
                    reference.label
                );
            }
        }
        let started = std::time::Instant::now();
        let rows = compare_rows(
            &reference.label,
            &object(rows_path),
            &mut legacy,
            &projection,
            stream.tick_path,
        );
        assert_eq!(rows, reference.rows);
        let structure = compare_events(&reference.label, &object(structure_path), &mut LegacyCsv::open(&root.join(format!("labels/phase4_structure_swing_3x3/aedcny_structure_labels_v1/swing_3x3/offset_mode=tek/structure_events_{}.csv", reference.label))), &structure_event_projection(reference));
        let sequence = compare_events(
            &reference.label,
            &object(sequence_path),
            &mut LegacyCsv::open(&root.join(format!(
                "labels/phase4b_structure_sequence_swing_3x3/structure_sequence_events_{}.csv",
                reference.label
            ))),
            &sequence_event_projection(reference),
        );
        assert_eq!(
            (structure, sequence),
            (reference.structure_events, reference.sequence_events)
        );
        println!(
            "parity {}: {rows} rows across {} legacy fields, {structure} structure events, {sequence} sequence events compared exactly in {:.1} s",
            reference.label,
            legacy.iter().map(|csv| csv.header.len()).sum::<usize>(),
            started.elapsed().as_secs_f64()
        );
        total_rows += rows;
        total_structure += structure;
        total_sequence += sequence;
    }
    assert_eq!(physical.len(), 201, "distinct physical reference fields");
    assert_eq!(
        (total_rows, total_structure, total_sequence),
        (1_515_091, 752_661, 263_240)
    );
    println!(
        "breadth: {} distinct physical fields plus the derived row identity; {total_rows} rows, {total_structure} structure events, {total_sequence} sequence events",
        physical.len()
    );

    // The whole-input in-process feed reproduces the published tables; every prefix is stable.
    let ticks = read_normalized_ticks(&published_root, &input);
    assert_eq!(ticks.len() as u64, input.row_count);
    let started = std::time::Instant::now();
    let full = feed(plan, &input, &ticks, ticks.len());
    println!(
        "in-process whole-input feed of {} ticks: {:.1} s, peak resident {} kB (bounded stream state, no fit)",
        ticks.len(),
        started.elapsed().as_secs_f64(),
        in_process_peak_kb()
    );
    for (index, stream) in plan.streams.iter().enumerate() {
        let paths = stream.object_paths();
        let (rows_path, structure_path, sequence_path) = (&paths[0], &paths[1], &paths[2]);
        let object = |path: &str| {
            published_root.join(
                &manifest
                    .objects
                    .iter()
                    .find(|object| object.path == path)
                    .unwrap()
                    .key,
            )
        };
        let (_, rows) = table_rows(&object(rows_path));
        let mut direct = full
            .rows
            .iter()
            .filter(|(s, _)| *s == index)
            .map(|(_, row)| &row.values);
        let mut count = 0;
        for row in rows {
            assert_eq!(
                Some(&row),
                direct.next(),
                "{} row {count}",
                stream.duration_seconds
            );
            count += 1;
        }
        assert!(direct.next().is_none());
        assert_eq!(count, manifest.streams[index].rows);
        let (_, structure) = table_rows(&object(structure_path));
        assert_eq!(
            structure.count(),
            full.structure_events
                .iter()
                .filter(|(s, _)| *s == index)
                .count()
        );
        let (_, sequence) = table_rows(&object(sequence_path));
        assert_eq!(
            sequence.count(),
            full.sequence_events
                .iter()
                .filter(|(s, _)| *s == index)
                .count()
        );
    }
    for percent in [25, 50, 75] {
        let cut = cutoff(&ticks, percent);
        let prefix = feed(plan, &input, &ticks[..cut], usize::MAX);
        assert_prefix(&prefix, &full, ticks[cut - 1].event_time_micros, percent);
        println!(
            "stable prefix {percent} percent: {} rows and {} events are a prefix of the full output",
            prefix.rows.len(),
            prefix.structure_events.len() + prefix.sequence_events.len()
        );
    }
}

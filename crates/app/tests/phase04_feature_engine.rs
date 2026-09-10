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
fn read_table(path: &Path) -> (Vec<String>, Vec<Vec<Option<Value>>>) {
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
struct PublishedFeatures {
    manifest: FeatureManifest,
    plan: FeaturePlan,
    tables: Vec<[(Vec<String>, Vec<Vec<Option<Value>>>); 4]>,
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
        .map(|stream| stream.object_paths().map(|path| read_table(&object(&path))))
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
        [manifest_path.clone()]
    );
    assert_eq!(
        feature_manifests(&scratch, "retained"),
        [scratch.path(&format!(
            "retained/manifests/{feature_generation}/ready.json"
        ))]
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
        ] = &published.tables[index];
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
        assert_eq!(fixed.edges.as_deref(), Some(&[0.0, 0.1, 0.2, 0.3, 0.5, 1.0][..]));
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
        let prefix = feed(
            plan,
            &dataset,
            &ticks[..ticks.len() * percent / 100],
            usize::MAX,
        );
        assert_eq!(
            prefix.rows,
            full.rows[..prefix.rows.len()],
            "{percent} percent"
        );
        assert_eq!(
            prefix.structure_events,
            full.structure_events[..prefix.structure_events.len()]
        );
        assert_eq!(
            prefix.sequence_events,
            full.sequence_events[..prefix.sequence_events.len()]
        );
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
        excluded.get("tick_volume_dev_quantile").is_none() && excluded.len() < 70,
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
    let [(names, rows), _, _, _] = &published.tables[0];
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
    // Any settings change is another raw identity and therefore another plan.
    let mut other = plan.settings.clone();
    other.rolling_window = Some(31);
    assert_ne!(
        plan.raw_identity,
        raw_identity(&plan.profile, &dataset, &other)
    );
}

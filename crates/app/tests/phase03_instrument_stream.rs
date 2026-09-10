//! Exercises `binary-alpha data audit` end to end: synthetic tick and bar generations always,
//! and the governed development fixture when `BINARY_ALPHA_TEST_CONFIG` names it.
//!
//! The test owns input adaptation (it reads normalized objects and legacy files itself and
//! feeds the engine directly for chunking and stable-prefix checks); `InstrumentStream` owns
//! audit and candles; the application command owns loading and publication.

mod common;

use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Instant;

use binary_alpha_engine::config::Config;
use binary_alpha_engine::dataset::GenerationManifest;
use binary_alpha_engine::market::{PriceScale, Tick, parse_event_time_micros, parse_price_units};
use binary_alpha_engine::stream::{
    Candle, InstrumentProfile, InstrumentStream, Observation, Source, StreamManifest,
    stream_generation_id,
};
use common::*;
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::record::RowAccessor;
use serde_json::Value;

/// The report line of a successful audit, or the diagnostic of a failed one.
fn audit(config: &Path, manifest: &Path) -> Result<String, String> {
    let uri = format!("file://{}", manifest.display());
    let output = binary_alpha(&[
        "data",
        "audit",
        "--config",
        config.to_str().unwrap(),
        "--manifest",
        &uri,
    ]);
    let stdout = String::from_utf8(output.stdout).unwrap();
    let stderr = String::from_utf8(output.stderr).unwrap();
    if output.status.success() {
        assert!(stderr.is_empty(), "{stderr}");
        Ok(stdout.trim_end().to_string())
    } else {
        assert_eq!(output.status.code(), Some(1));
        assert!(stdout.is_empty(), "{stdout}");
        Err(stderr)
    }
}

/// The `[[instruments]]` entry the synthetic tests use, with the legacy-shaped checks.
fn tick_instrument(symbol: &str, scale: u8) -> String {
    format!(
        "\n[[instruments]]\nbroker = \"pocket_option\"\nprovider_symbol = \"{symbol}\"\nbase_currency = \"AED\"\nquote_currency = \"CNY\"\nprice_scale = {scale}\nnative_granularity = {{ kind = \"tick\" }}\ngap = {{ max_seconds = 2 }}\nfrozen = {{ min_observations = 3, min_seconds = 5 }}\njump = {{ min_basis_points = 5 }}\nspan = {{ min_percent = 75 }}\nsessions = [{{ name = \"week\", open_seconds = 0, close_seconds = 604800 }}]\ncandles = [{{ duration_seconds = 5, offset_seconds = 0, min_observations = 3, hard_min_observations = 2 }}, {{ duration_seconds = 15, offset_seconds = 5 }}]\n"
    )
}

fn bar_instrument(symbol: &str, scale: u8, base: Option<&str>) -> String {
    let base = base.map_or(String::new(), |base| {
        format!("base_currency = \"{base}\"\n")
    });
    format!(
        "\n[[instruments]]\nbroker = \"pocket_option\"\nprovider_symbol = \"{symbol}\"\n{base}quote_currency = \"USD\"\nprice_scale = {scale}\nnative_granularity = {{ kind = \"bar\", period_seconds = 5 }}\ngap = {{ max_seconds = 2 }}\nfrozen = {{ min_observations = 3, min_seconds = 5 }}\njump = {{ min_basis_points = 5 }}\ncandles = [{{ duration_seconds = 15, offset_seconds = 5 }}]\n"
    )
}

/// One candle row as the published object records it, read through the generic row API so the
/// comparison does not depend on the application's reader.
#[derive(Debug, Clone, PartialEq)]
struct Row {
    times: [i64; 6],
    prices: [i64; 4],
    counts: [i64; 2],
    volume: Option<f64>,
    gap_before: Option<i64>,
    facts: [i64; 6],
    flags: [bool; 11],
}

impl From<&Candle> for Row {
    fn from(candle: &Candle) -> Self {
        let flags = candle.flags;
        Self {
            times: [
                candle.open_time_micros,
                candle.close_time_micros,
                candle.known_at_micros,
                candle.first_event_micros,
                candle.last_event_micros,
                candle.active_span_micros,
            ],
            prices: [
                candle.open_units,
                candle.high_units,
                candle.low_units,
                candle.close_units,
            ],
            counts: [i64::from(candle.observations), i64::from(candle.duplicates)],
            volume: candle.volume,
            gap_before: candle.gap_before_micros,
            facts: [
                candle.max_gap_inside_micros,
                i64::from(candle.missing_buckets_before),
                i64::from(candle.frozen_observations),
                candle.frozen_micros,
                i64::from(candle.max_jump_basis_points),
                i64::from(candle.max_gap_jump_basis_points),
            ],
            flags: [
                flags.low_activity,
                flags.hard_low_activity,
                flags.gap_before,
                flags.gap_inside,
                flags.missing_before,
                flags.frozen,
                flags.jump,
                flags.gap_jump,
                flags.short_span,
                flags.complete(),
                flags.clean(),
            ],
        }
    }
}

fn read_rows(path: &Path) -> Vec<Row> {
    let reader = SerializedFileReader::new(fs::File::open(path).unwrap()).unwrap();
    reader
        .get_row_iter(None)
        .unwrap()
        .map(|row| {
            let row = row.unwrap();
            let time = |index| row.get_timestamp_micros(index).unwrap();
            let long = |index| row.get_long(index).unwrap();
            Row {
                times: [time(0), time(1), time(2), time(3), time(4), long(5)],
                prices: [long(6), long(7), long(8), long(9)],
                counts: [long(10), long(11)],
                volume: row.get_double(12).ok(),
                gap_before: row.get_long(13).ok(),
                facts: [long(14), long(15), long(16), long(17), long(18), long(19)],
                flags: std::array::from_fn(|index| row.get_bool(20 + index).unwrap()),
            }
        })
        .collect()
}

fn read_normalized_ticks(path: &Path) -> Vec<Tick> {
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

/// The stream manifest published at `manifest`, its profile, and every candle object's rows.
struct Published {
    manifest: StreamManifest,
    profile: InstrumentProfile,
    candles: Vec<Vec<Row>>,
}

fn published_stream(store: &Path, manifest: &Path) -> Published {
    let manifest = StreamManifest::from_json(&fs::read(manifest).unwrap()).unwrap();
    let object = |path: &str| {
        store.join(
            &manifest
                .objects
                .iter()
                .find(|object| object.path == path)
                .unwrap()
                .key,
        )
    };
    let profile = InstrumentProfile::from_json(&fs::read(object("profile.json")).unwrap()).unwrap();
    let candles = manifest
        .streams
        .iter()
        .map(|summary| {
            read_rows(&object(&format!(
                "candles/{}s_{}s.parquet",
                summary.duration_seconds, summary.offset_seconds
            )))
        })
        .collect();
    Published {
        manifest,
        profile,
        candles,
    }
}

fn stream_manifests(scratch: &Scratch, store: &str) -> Vec<PathBuf> {
    scratch
        .manifests(store)
        .into_iter()
        .filter(|path| {
            fs::read_to_string(path)
                .unwrap()
                .starts_with("{\n  \"kind\": \"instrument_stream\"")
        })
        .collect()
}

/// Feeds every observation and returns the finalized candles and the profile.
fn feed(
    instrument: &binary_alpha_engine::config::Instrument,
    manifest: &GenerationManifest,
    ticks: &[Tick],
) -> (Vec<(usize, Candle)>, InstrumentProfile) {
    let mut stream = InstrumentStream::new(instrument, Source::from_manifest(manifest)).unwrap();
    let mut out = Vec::new();
    for tick in ticks {
        stream.push(Observation::Tick(*tick), &mut out).unwrap();
    }
    (out, stream.profile())
}

const SYNTHETIC_TICKS: [&str; 14] = [
    "2026-03-22T06:02:39.312Z,AEDCNY,1.80787",
    "2026-03-22T06:02:39.530Z,AEDCNY,1.80787",
    "2026-03-22T06:02:39.530Z,AEDCNY,1.80787",
    "2026-03-22T06:02:41.001Z,AEDCNY,1.80787",
    "2026-03-22T06:02:41.900Z,AEDCNY,1.80900",
    "2026-03-22T06:02:45.000Z,AEDCNY,1.80900",
    "2026-03-22T06:02:46.000Z,AEDCNY,1.80901",
    "2026-03-22T06:02:47.000Z,AEDCNY,1.80902",
    "2026-03-22T06:02:49.999Z,AEDCNY,1.80902",
    "2026-03-22T06:03:10.000Z,AEDCNY,1.81000",
    "2026-03-22T06:03:11.000Z,AEDCNY,1.81000",
    "2026-03-22T06:03:12.000Z,AEDCNY,1.81000",
    "2026-03-22T06:03:14.000Z,AEDCNY,1.81000",
    "2026-03-22T06:03:20.000Z,AEDCNY,1.80950",
];

#[test]
fn audit_publishes_a_stream_generation_that_verifies_and_matches_a_direct_feed() {
    let scratch = Scratch::new("phase03_ticks");
    write_ticks(&scratch.path("sources/ticks/ticks.csv"), &SYNTHETIC_TICKS);
    let config = scratch.config(
        "audit.toml",
        &format!(
            "{}{}",
            scratch.tick_source(),
            tick_instrument("AEDCNY_otc", 6)
        ),
    );
    let lines = import(&config).unwrap();
    let dataset_manifest = scratch.manifests("published")[0].clone();
    let line = audit(&config, &dataset_manifest).unwrap();
    let dataset = GenerationManifest::from_json(&fs::read(&dataset_manifest).unwrap()).unwrap();
    assert_eq!(generation(&lines[0]), dataset.generation);
    let parsed = Config::parse(&fs::read_to_string(&config).unwrap()).unwrap();
    let instrument = parsed.instruments[0].clone();
    let expected_generation =
        stream_generation_id(&dataset.generation, &instrument.canonical_toml());
    assert!(
        line.starts_with(&format!(
            "audited pocket_option:AEDCNY_otc development generation {expected_generation} from {} observations 14 candles 6 objects 3 reused 0 [stream ",
            dataset.generation
        )),
        "{line}"
    );
    let manifests = stream_manifests(&scratch, "published");
    assert_eq!(manifests.len(), 1);
    let manifest_path = &manifests[0];
    assert!(manifest_path.ends_with(format!("{expected_generation}/ready.json")));
    let mirror = scratch.path("retained").join(
        manifest_path
            .strip_prefix(scratch.path("published"))
            .unwrap(),
    );
    assert_eq!(fs::read(manifest_path).unwrap(), fs::read(&mirror).unwrap());

    let published = published_stream(&scratch.path("published"), manifest_path);
    let manifest = &published.manifest;
    assert_eq!(manifest.definition, instrument);
    assert_eq!(manifest.config_hash, parsed.content_hash());
    assert!(manifest.config_hash.starts_with("v3:sha256:"));
    assert_eq!(manifest.code_revision, dataset.code_revision);
    assert_eq!(manifest.source_generation, dataset.generation);
    assert_eq!(manifest.observations, 14);
    assert_eq!(
        manifest.coverage.as_ref().unwrap().first_event_time,
        "2026-03-22T06:02:39.312000Z"
    );
    let rows: Vec<u64> = manifest
        .streams
        .iter()
        .map(|summary| summary.rows)
        .collect();
    assert_eq!(rows, [4, 2]);
    for object in &manifest.objects {
        let stored = scratch.path("published").join(&object.key);
        assert_eq!(sha256(&stored), object.sha256, "{}", object.path);
        assert_eq!(fs::metadata(&stored).unwrap().len(), object.bytes);
        assert!(scratch.path("retained").join(&object.key).is_file());
    }

    // The published rows equal a direct row-at-a-time feed of the normalized object, and so do
    // chunked and whole-input feeds; the test adapts the input itself.
    let normalized = scratch.path("published").join(
        &dataset
            .objects
            .iter()
            .find(|object| object.path == "normalized/ticks.parquet")
            .unwrap()
            .key,
    );
    let ticks = read_normalized_ticks(&normalized);
    assert_eq!(ticks.len(), 14);
    let (direct, profile) = feed(&instrument, &dataset, &ticks);
    assert_eq!(profile, published.profile);
    let by_stream: Vec<Vec<Row>> = (0..2)
        .map(|stream| {
            direct
                .iter()
                .filter(|(index, _)| *index == stream)
                .map(|(_, candle)| Row::from(candle))
                .collect()
        })
        .collect();
    assert_eq!(by_stream, published.candles);
    for chunk in [1, 3, 5, 14] {
        let mut stream =
            InstrumentStream::new(&instrument, Source::from_manifest(&dataset)).unwrap();
        let mut out = Vec::new();
        for ticks in ticks.chunks(chunk) {
            for tick in ticks {
                stream.push(Observation::Tick(*tick), &mut out).unwrap();
            }
        }
        assert_eq!(out, direct, "chunk size {chunk}");
        assert_eq!(stream.profile(), profile, "chunk size {chunk}");
    }

    // Field-level expectations of the synthetic path.
    let five = &published.candles[0];
    let first = &five[0];
    assert_eq!(
        first.times[0],
        parse_event_time_micros("2026-03-22T06:02:35Z").unwrap()
    );
    assert_eq!(
        first.times[2],
        parse_event_time_micros("2026-03-22T06:02:41.001Z").unwrap()
    );
    assert_eq!(first.prices, [1_807_870; 4]);
    assert_eq!(
        first.counts,
        [3, 1],
        "the identical repeat is counted once more"
    );
    assert_eq!(first.gap_before, None);
    assert_eq!(first.facts[2], 3, "three records at one price");
    assert!(first.flags[5], "frozen at three records");
    assert!(first.flags[8], "0.218 seconds of five is a short span");
    assert!(first.flags[9] && !first.flags[10]);
    let second = &five[1];
    assert_eq!(
        second.times[0],
        parse_event_time_micros("2026-03-22T06:02:40Z").unwrap()
    );
    assert_eq!(second.counts, [2, 0]);
    assert_eq!(second.gap_before, Some(1_471_000));
    assert_eq!(second.prices, [1_807_870, 1_809_000, 1_807_870, 1_809_000]);
    assert_eq!(
        second.facts[4], 6,
        "1.80787 to 1.80900 is 6.25 basis points"
    );
    assert!(second.flags[0] && !second.flags[1] && second.flags[6]);
    assert_eq!(profile.duplicates, 1);
    assert_eq!(profile.streams[0].withheld_observations, 1);
    assert_eq!(profile.streams[1].withheld_observations, 1);
    assert_eq!(profile.gaps.as_ref().unwrap().count, 4);
    let jumps = profile.jumps.as_ref().unwrap();
    assert_eq!(
        (jumps.flagged, jumps.flagged_after_gap),
        (1, 1),
        "one true jump, one reopen jump"
    );
    let reopened = &five[3];
    assert_eq!(reopened.gap_before, Some(20_001_000));
    assert_eq!(
        reopened.facts[5], 5,
        "1.80902 to 1.81000 after a gap is 5.4 basis points"
    );
    assert!(reopened.flags[2] && reopened.flags[7] && !reopened.flags[6]);
    assert!(
        !reopened.flags[9],
        "a gap before the candle makes it incomplete"
    );
    assert_eq!(
        profile.prices.step_units,
        Some(10),
        "five-decimal prices at scale six"
    );
    assert!(
        profile
            .calculations
            .iter()
            .all(|calculation| calculation.supported)
    );

    // Verification reads the generation back from either store; a repeated audit reuses it.
    assert_eq!(
        verify(manifest_path).unwrap(),
        format!(
            "verified pocket_option:AEDCNY_otc development generation {expected_generation} candles 6 objects 3 bytes {}",
            manifest
                .objects
                .iter()
                .map(|object| object.bytes)
                .sum::<u64>()
        )
    );
    assert!(
        verify(&mirror)
            .unwrap()
            .starts_with("verified pocket_option:AEDCNY_otc")
    );
    let again = audit(&config, &dataset_manifest).unwrap();
    assert!(again.ends_with(" reused 3 (already published)"), "{again}");
    assert_eq!(fs::read(manifest_path).unwrap(), manifest.to_json());

    // Tampering with a published candle object is detected on verification.
    let candle_object = scratch.path("published").join(&manifest.objects[1].key);
    let bytes = fs::read(&candle_object).unwrap();
    fs::write(&candle_object, &bytes[..bytes.len() - 1]).unwrap();
    assert!(
        verify(manifest_path)
            .unwrap_err()
            .contains("does not match the recorded size")
    );
    fs::write(&candle_object, &bytes).unwrap();
}

#[test]
fn audit_binds_only_a_configured_matching_instrument() {
    let scratch = Scratch::new("phase03_binding");
    write_ticks(&scratch.path("sources/ticks/ticks.csv"), &SYNTHETIC_TICKS);
    write_collection(
        &scratch.path("sources/bars"),
        &[
            AssetSpec {
                asset: "#AAPL",
                expected_symbol_id: Some(5),
                symbol_id: None,
                files: vec![bars("#AAPL", 5, 1_747_653_305, 6)],
                metadata: true,
            },
            AssetSpec {
                asset: "EURUSD_otc",
                expected_symbol_id: Some(9),
                symbol_id: None,
                files: vec![
                    (0..4)
                        .map(|index| {
                            bar(
                                "EURUSD_otc",
                                9,
                                1_747_653_305 + index * 5,
                                [
                                    1.1,
                                    1.15,
                                    1.05,
                                    (1_100 + index) as f64 / 1_000.0,
                                    index as f64,
                                ],
                            )
                        })
                        .collect(),
                ],
                metadata: true,
            },
        ],
    );
    let sources = format!("{}{}", scratch.tick_source(), scratch.bar_source());
    let import_config = scratch.config("import.toml", &sources);
    import(&import_config).unwrap();
    let manifests = scratch.manifests("published");
    let find = |symbol: &str| {
        manifests
            .iter()
            .find(|path| manifest_json(path)["provider_symbol"].as_str().unwrap() == symbol)
            .unwrap()
            .clone()
    };
    let (ticks, apple, euro) = (find("AEDCNY_otc"), find("#AAPL"), find("EURUSD_otc"));

    // No instrument maps the generation: nothing is defaulted.
    let unmapped = scratch.config("unmapped.toml", &sources);
    assert!(
        audit(&unmapped, &ticks)
            .unwrap_err()
            .contains("no configured instrument maps pocket_option:AEDCNY_otc")
    );

    // A tick instrument on a bar-only generation is refused with the machine-readable reason.
    let mismatched = scratch.config(
        "mismatched.toml",
        &format!(
            "{sources}{}{}{}",
            tick_instrument("AEDCNY_otc", 5),
            tick_instrument("#AAPL", 2),
            bar_instrument("EURUSD_otc", 5, Some("EUR"))
        ),
    );
    let error = audit(&mismatched, &apple).unwrap_err();
    let reason: Value = serde_json::from_str(error.trim_end()).unwrap();
    assert_eq!(reason["required"], "ticks");
    assert_eq!(reason["provided"], serde_json::json!(["bars"]));
    assert_eq!(reason["instrument"], "pocket_option:#AAPL");
    assert_eq!(reason["generation"], generation_of(&apple));
    let error = audit(&mismatched, &ticks).unwrap_err();
    assert!(
        error.contains("price_scale 5") && error.contains("price_scale 6"),
        "{error}"
    );
    assert!(
        stream_manifests(&scratch, "published").is_empty(),
        "nothing was published"
    );

    // A bar instrument audits through its configured scale; its profile refuses every tick
    // calculation and its candles carry volume.
    let line = audit(&mismatched, &euro).unwrap();
    assert!(
        line.contains(" observations 4 candles 1 objects 2 "),
        "{line}"
    );
    let manifest_path = &stream_manifests(&scratch, "published")[0];
    let published = published_stream(&scratch.path("published"), manifest_path);
    assert_eq!(published.manifest.source_kind.as_str(), "bar_parquet");
    assert_eq!(
        published.profile.base_currency.as_ref().unwrap().as_str(),
        "EUR"
    );
    assert!(published.profile.calculations.iter().all(|calculation| {
        !calculation.supported
            && calculation
                .reason
                .as_deref()
                .unwrap()
                .contains("\"required\":\"ticks\"")
    }));
    let candle = &published.candles[0][0];
    assert_eq!(candle.counts, [3, 0]);
    assert_eq!(candle.volume, Some(0.0 + 1.0 + 2.0));
    assert_eq!(candle.prices, [110_000, 115_000, 105_000, 110_200]);
    assert_eq!(
        candle.times[5], 15_000_000,
        "three whole bars span the candle"
    );
    assert!(
        verify(manifest_path)
            .unwrap()
            .contains(" candles 1 objects 2 ")
    );

    // A stream manifest is not an audit input.
    let error = audit(&mismatched, manifest_path).unwrap_err();
    assert!(
        error.contains("`instrument_stream` manifest, not a dataset ready manifest"),
        "{error}"
    );
}

fn generation_of(manifest: &Path) -> String {
    manifest_json(manifest)["generation"]
        .as_str()
        .unwrap()
        .to_string()
}

/// The untracked resolved test configuration of the governed proof.
#[derive(serde::Deserialize)]
struct GovernedConfig {
    /// The Phase 02 ready manifest of the governed open development tick fixture.
    tick_manifest: String,
    /// The pinned resampler's candle files, one per stream, for parity.
    legacy_candles: Vec<LegacyCandles>,
    /// Three Phase 02 bar ready manifests selected from the collection manifest.
    bar_manifests: Vec<BarCase>,
}

#[derive(serde::Deserialize)]
struct LegacyCandles {
    duration_seconds: u32,
    offset_seconds: u32,
    min_observations: u32,
    hard_min_observations: u32,
    path: PathBuf,
}

#[derive(serde::Deserialize)]
struct BarCase {
    manifest: String,
    price_scale: u8,
    base_currency: Option<String>,
    quote_currency: String,
}

/// Runs the audit under GNU `time -v`, returning the report line, wall seconds, and peak
/// resident kilobytes of the child.
fn timed_audit(config: &Path, manifest: &str) -> (String, f64, u64) {
    let started = Instant::now();
    let output = Command::new("/usr/bin/time")
        .args([
            "-v",
            env!("CARGO_BIN_EXE_binary-alpha"),
            "data",
            "audit",
            "--config",
            config.to_str().unwrap(),
            "--manifest",
            manifest,
        ])
        .output()
        .expect("GNU time runs the audit");
    let wall = started.elapsed().as_secs_f64();
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(output.status.success(), "{stderr}");
    let peak = stderr
        .lines()
        .find_map(|line| {
            line.trim()
                .strip_prefix("Maximum resident set size (kbytes): ")
        })
        .expect("GNU time reports the peak")
        .parse()
        .unwrap();
    (
        String::from_utf8(output.stdout)
            .unwrap()
            .trim_end()
            .to_string(),
        wall,
        peak,
    )
}

fn verify_uri(uri: &str) -> String {
    let output = binary_alpha(&["data", "verify", "--manifest", uri]);
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

fn local_path(uri: &str) -> PathBuf {
    PathBuf::from(
        uri.strip_prefix("file://")
            .expect("the governed stores are local"),
    )
}

/// One legacy candle row's fields that this phase reproduces, normalized to the target's
/// representation: microseconds, integer units, and the target's flag vocabulary.
#[derive(Debug, PartialEq)]
struct LegacyRow {
    times: [i64; 5],
    prices: [i64; 4],
    tick_volume: i64,
    starts_after_gap_micros: i64,
    max_internal_gap_micros: i64,
    missing_buckets: i64,
    same_price_run: [i64; 2],
    true_jump_bps: f64,
    flags: [bool; 11],
    eligible: bool,
}

fn legacy_rows(path: &Path, scale: PriceScale) -> Vec<LegacyRow> {
    let text = fs::read_to_string(path).unwrap();
    let mut lines = text.lines();
    let header: Vec<&str> = lines.next().unwrap().split(',').collect();
    let column = |name: &str| header.iter().position(|field| *field == name).unwrap();
    let columns: Vec<usize> = [
        "open_time_utc",
        "close_time_utc",
        "first_tick_time_utc",
        "last_tick_time_utc",
        "active_span_ms",
        "open",
        "high",
        "low",
        "close",
        "tick_volume",
        "starts_after_gap_ms",
        "max_internal_gap_ms",
        "missing_buckets_since_prev_candle",
        "max_same_price_run_ticks",
        "max_same_price_run_ms",
        "max_true_tick_jump_bps",
        "low_tick_volume",
        "hard_low_tick_volume",
        "starts_after_gap",
        "has_internal_gap",
        "frozen_price_flag",
        "has_true_tick_jump",
        "has_feed_delay_jump",
        "has_gap_reopen_jump",
        "complete",
        "has_gap",
    ]
    .iter()
    .map(|name| column(name))
    .collect();
    lines
        .map(|line| {
            let fields: Vec<&str> = line.split(',').collect();
            let field = |index: usize| fields[columns[index]];
            let micros = |index| parse_event_time_micros(field(index)).unwrap();
            // The resampler renders prices with eight decimals; the governed fixture carries at
            // most six, so the trailing digits are zero and the value normalizes exactly.
            let units = |index| {
                let eight = PriceScale::try_from(scale.digits() + 2).unwrap();
                let units = parse_price_units(field(index), eight).unwrap();
                assert_eq!(
                    units % 100,
                    0,
                    "{}: not representable at the scale",
                    field(index)
                );
                units / 100
            };
            let int = |index| field(index).parse::<i64>().unwrap();
            let flag = |index| field(index) == "1";
            let low = flag(16);
            let hard_low = flag(17);
            let gap_before = flag(18);
            let gap_inside = flag(19);
            let missing = int(12) > 0;
            let frozen = flag(20);
            let jump = flag(21);
            let gap_jump = flag(22) || flag(23);
            let short_span = int(4) < (timeframe(field(0), field(1)) * 1_000 * 75) / 100;
            let complete = flag(24);
            let eligible = complete
                && !low
                && !hard_low
                && !flag(25)
                && !gap_inside
                && !gap_before
                && !missing
                && !frozen
                && !jump
                && !gap_jump
                && !short_span;
            LegacyRow {
                times: [micros(0), micros(1), micros(2), micros(3), int(4) * 1_000],
                prices: [units(5), units(6), units(7), units(8)],
                tick_volume: int(9),
                starts_after_gap_micros: int(10) * 1_000,
                max_internal_gap_micros: int(11) * 1_000,
                missing_buckets: int(12),
                same_price_run: [int(13), int(14) * 1_000],
                true_jump_bps: field(15).parse().unwrap(),
                flags: [
                    low, hard_low, gap_before, gap_inside, missing, frozen, jump, gap_jump,
                    short_span, complete, eligible,
                ],
                eligible,
            }
        })
        .collect()
}

/// The candle duration in seconds from a legacy row's open and close times.
fn timeframe(open: &str, close: &str) -> i64 {
    (parse_event_time_micros(close).unwrap() - parse_event_time_micros(open).unwrap()) / 1_000_000
}

/// Compares one stream's published rows against the pinned resampler's rows for every interval
/// the observed input closed. Returns the count of continuous-diagnostic boundary differences.
fn assert_parity(target: &[Row], legacy: &[LegacyRow], label: &str) -> usize {
    assert_eq!(
        legacy.len(),
        target.len() + 1,
        "{label}: the legacy file ends with the unfinished interval the target withholds"
    );
    let last = legacy.last().unwrap();
    assert!(
        last.times[3] < last.times[1],
        "{label}: the legacy tail is unfinished"
    );
    let mut boundary = 0;
    for (index, (row, reference)) in target.iter().zip(legacy).enumerate() {
        let context = format!("{label} row {index}");
        let [open, close, _, first, last, span] = row.times;
        assert_eq!(
            [open, close, first, last, span],
            reference.times,
            "{context}: times"
        );
        assert_eq!(row.prices, reference.prices, "{context}: prices");
        assert_eq!(
            row.counts[0], reference.tick_volume,
            "{context}: tick volume"
        );
        assert_eq!(
            row.gap_before.unwrap_or(0),
            reference.starts_after_gap_micros,
            "{context}: gap before"
        );
        assert_eq!(
            row.facts[0], reference.max_internal_gap_micros,
            "{context}: internal gap"
        );
        assert_eq!(
            row.facts[1], reference.missing_buckets,
            "{context}: missing buckets"
        );
        assert_eq!(
            row.facts[2..4],
            reference.same_price_run,
            "{context}: same-price run"
        );
        assert_eq!(
            row.flags, reference.flags,
            "{context}: flags {:?} vs legacy {:?}",
            row.flags, reference.flags
        );
        assert_eq!(
            row.flags[10], reference.eligible,
            "{context}: strict eligibility"
        );
        // Continuous diagnostic: the legacy value is binary floating point rendered to six
        // decimals; the target floors the exact ratio. A difference of one whole basis point at
        // an integer boundary is the documented tolerance and never changes the flag above.
        let floored = reference.true_jump_bps.floor() as i64;
        if floored != row.facts[4] {
            assert_eq!(
                (floored - row.facts[4]).abs(),
                1,
                "{context}: true jump {} vs {}",
                reference.true_jump_bps,
                row.facts[4]
            );
            boundary += 1;
        }
    }
    boundary
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

#[test]
#[ignore = "needs BINARY_ALPHA_TEST_CONFIG naming the governed Phase 02 generations"]
fn governed_fixture_proof() {
    let config_path = std::env::var("BINARY_ALPHA_TEST_CONFIG")
        .expect("BINARY_ALPHA_TEST_CONFIG names the resolved test configuration");
    let governed: GovernedConfig =
        serde_json::from_slice(&fs::read(&config_path).unwrap()).unwrap();
    let scratch = Scratch::new("phase03_governed");
    let dataset =
        GenerationManifest::from_json(&fs::read(local_path(&governed.tick_manifest)).unwrap())
            .unwrap();
    let candles: String = governed
        .legacy_candles
        .iter()
        .map(|case| {
            format!(
                "{{ duration_seconds = {}, offset_seconds = {}, min_observations = {}, hard_min_observations = {} }}",
                case.duration_seconds, case.offset_seconds, case.min_observations, case.hard_min_observations
            )
        })
        .collect::<Vec<_>>()
        .join(", ");
    let mut instruments = format!(
        "\n[[instruments]]\nbroker = \"{}\"\nprovider_symbol = \"{}\"\nbase_currency = \"AED\"\nquote_currency = \"CNY\"\nprice_scale = 6\nnative_granularity = {{ kind = \"tick\" }}\ngap = {{ max_seconds = 2 }}\nfrozen = {{ min_observations = 10, min_seconds = 5 }}\njump = {{ min_basis_points = 5 }}\nspan = {{ min_percent = 75 }}\nsessions = [{{ name = \"week\", open_seconds = 0, close_seconds = 604800 }}]\ncandles = [{candles}]\n",
        dataset.broker, dataset.provider_symbol
    );
    let bar_datasets: Vec<GenerationManifest> = governed
        .bar_manifests
        .iter()
        .map(|case| {
            GenerationManifest::from_json(&fs::read(local_path(&case.manifest)).unwrap()).unwrap()
        })
        .collect();
    for (case, manifest) in governed.bar_manifests.iter().zip(&bar_datasets) {
        let base = case.base_currency.as_ref().map_or(String::new(), |base| {
            format!("base_currency = \"{base}\"\n")
        });
        instruments.push_str(&format!(
            "\n[[instruments]]\nbroker = \"{}\"\nprovider_symbol = \"{}\"\n{base}quote_currency = \"{}\"\nprice_scale = {}\nnative_granularity = {{ kind = \"bar\", period_seconds = 5 }}\ngap = {{ max_seconds = 2 }}\nfrozen = {{ min_observations = 10, min_seconds = 5 }}\njump = {{ min_basis_points = 5 }}\nspan = {{ min_percent = 75 }}\nsessions = [{{ name = \"week\", open_seconds = 0, close_seconds = 604800 }}]\ncandles = [{}]\n",
            manifest.broker,
            manifest.provider_symbol,
            case.quote_currency,
            case.price_scale,
            governed
                .legacy_candles
                .iter()
                .map(|case| format!("{{ duration_seconds = {}, offset_seconds = {} }}", case.duration_seconds, case.offset_seconds))
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }
    let config = scratch.config("governed.toml", &instruments);
    let parsed = Config::parse(&fs::read_to_string(&config).unwrap()).unwrap();
    let instrument = parsed.instruments[0].clone();

    // The application command over the governed tick generation.
    let (line, wall, peak) = timed_audit(&config, &governed.tick_manifest);
    println!("audit: {line}");
    println!("audit wall {wall:.3} s, peak resident {peak} kB");
    let manifest_path = &stream_manifests(&scratch, "published")[0];
    let uri = format!("file://{}", manifest_path.display());
    println!("verify: {}", verify_uri(&uri));
    let mirror = scratch.path("retained").join(
        manifest_path
            .strip_prefix(scratch.path("published"))
            .unwrap(),
    );
    println!(
        "verify mirror: {}",
        verify_uri(&format!("file://{}", mirror.display()))
    );
    let published = published_stream(&scratch.path("published"), manifest_path);
    let manifest = &published.manifest;
    assert_eq!(manifest.definition, instrument);
    assert_eq!(manifest.config_hash, parsed.content_hash());
    assert_eq!(manifest.code_revision, dataset.code_revision);
    assert_eq!(manifest.source_generation, dataset.generation);
    assert_eq!(manifest.observations, dataset.row_count);
    assert_eq!(manifest.coverage.as_ref().unwrap(), &dataset.coverage);
    println!("stream generation {}", manifest.generation);
    println!(
        "profile:\n{}",
        String::from_utf8(published.profile.to_json()).unwrap()
    );
    for (summary, object) in manifest.streams.iter().zip(manifest.objects.iter().skip(1)) {
        println!(
            "stream {}s/{}s rows {} first {:?} last {:?} object {} sha256 {} bytes {}",
            summary.duration_seconds,
            summary.offset_seconds,
            summary.rows,
            summary.first_open_time,
            summary.last_close_time,
            object.path,
            object.sha256,
            object.bytes
        );
    }

    // Legacy parity per stream, closed intervals only.
    let scale = PriceScale::try_from(6).unwrap();
    for (case, rows) in governed.legacy_candles.iter().zip(&published.candles) {
        let label = format!("{}s/{}s", case.duration_seconds, case.offset_seconds);
        let legacy = legacy_rows(&case.path, scale);
        let boundary = assert_parity(rows, &legacy, &label);
        println!(
            "parity {label}: {} finalized candles match the legacy rows field by field; {boundary} continuous-diagnostic boundary differences",
            rows.len()
        );
    }

    // Row-at-a-time, chunked, and whole-input feeds reproduce the published output, and the
    // stable-prefix property holds at 25, 50, and 75 percent.
    let normalized = local_path(&governed.tick_manifest)
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .join(
            &dataset
                .objects
                .iter()
                .find(|object| object.path == "normalized/ticks.parquet")
                .unwrap()
                .key,
        );
    let ticks = read_normalized_ticks(&normalized);
    assert_eq!(ticks.len() as u64, dataset.row_count);
    let started = Instant::now();
    let (direct, profile) = feed(&instrument, &dataset, &ticks);
    println!(
        "direct row-at-a-time feed of {} ticks: {:.3} s",
        ticks.len(),
        started.elapsed().as_secs_f64()
    );
    assert_eq!(profile, published.profile);
    let by_stream: Vec<Vec<Row>> = (0..manifest.streams.len())
        .map(|stream| {
            direct
                .iter()
                .filter(|(index, _)| *index == stream)
                .map(|(_, candle)| Row::from(candle))
                .collect()
        })
        .collect();
    assert_eq!(by_stream, published.candles);
    for chunk in [1_000, 1 << 20, ticks.len()] {
        let mut stream =
            InstrumentStream::new(&instrument, Source::from_manifest(&dataset)).unwrap();
        let mut out = Vec::new();
        for ticks in ticks.chunks(chunk) {
            for tick in ticks {
                stream.push(Observation::Tick(*tick), &mut out).unwrap();
            }
        }
        assert_eq!(out, direct, "chunk size {chunk}");
        assert_eq!(stream.profile(), profile, "chunk size {chunk}");
    }
    for percent in [25, 50, 75] {
        let cut = ticks.len() * percent / 100;
        let (prefix, prefix_profile) = feed(&instrument, &dataset, &ticks[..cut]);
        assert_eq!(prefix, direct[..prefix.len()], "{percent} percent");
        assert_eq!(
            prefix_profile.coverage.as_ref().unwrap().first_event_time,
            profile.coverage.as_ref().unwrap().first_event_time
        );
        assert!(
            prefix_profile.gaps.as_ref().unwrap().count <= profile.gaps.as_ref().unwrap().count
        );
        assert!(
            prefix_profile.frozen_runs.as_ref().unwrap().count
                <= profile.frozen_runs.as_ref().unwrap().count
        );
        assert!(prefix_profile.duplicates <= profile.duplicates);
        for (short, long) in prefix_profile.streams.iter().zip(&profile.streams) {
            assert!(short.finalized <= long.finalized && short.flagged.clean <= long.flagged.clean);
        }
        println!(
            "stable prefix {percent} percent: {} finalized candles are a prefix of the full output",
            prefix.len()
        );
    }
    println!("in-process peak resident {} kB", in_process_peak_kb());

    // The separate bar acceptance case: three explicitly supplied generations.
    let mut steps = Vec::new();
    for (case, bar_dataset) in governed.bar_manifests.iter().zip(&bar_datasets) {
        let (line, wall, peak) = timed_audit(&config, &case.manifest);
        println!("audit: {line}");
        println!("audit wall {wall:.3} s, peak resident {peak} kB");
        let manifest_path = stream_manifests(&scratch, "published")
            .into_iter()
            .find(|path| {
                fs::read_to_string(path).unwrap().contains(&format!(
                    "\"source_generation\": \"{}\"",
                    bar_dataset.generation
                ))
            })
            .unwrap();
        println!(
            "verify: {}",
            verify_uri(&format!("file://{}", manifest_path.display()))
        );
        let published = published_stream(&scratch.path("published"), &manifest_path);
        let profile = &published.profile;
        assert_eq!(profile.instrument, bar_dataset.instrument);
        assert_eq!(profile.observations, bar_dataset.row_count);
        assert_eq!(
            profile
                .base_currency
                .as_ref()
                .map(|base| base.as_str().to_string()),
            case.base_currency
        );
        assert!(
            profile
                .calculations
                .iter()
                .all(|calculation| !calculation.supported)
        );
        assert!(
            published
                .candles
                .iter()
                .flatten()
                .all(|row| row.volume.is_some())
        );
        println!(
            "bar profile {}: observations {} step_units {:?} min {:?} max {:?} duplicates {} gaps {:?} frozen {:?} streams {:?}",
            profile.instrument,
            profile.observations,
            profile.prices.step_units,
            profile.prices.min_units,
            profile.prices.max_units,
            profile.duplicates,
            profile.gaps,
            profile.frozen_runs,
            profile
                .streams
                .iter()
                .map(|facts| (
                    facts.finalized,
                    facts.withheld_observations,
                    facts.flagged.clean
                ))
                .collect::<Vec<_>>()
        );
        steps.push((
            case.base_currency.is_some(),
            case.price_scale,
            profile.prices.step_units,
        ));
    }
    let currencies: Vec<_> = steps.iter().filter(|(currency, _, _)| *currency).collect();
    assert_eq!(currencies.len(), 2, "two currency instruments");
    assert_ne!(
        currencies[0].1, currencies[1].1,
        "different configured price scales"
    );
    assert_eq!(
        steps.iter().filter(|(currency, _, _)| !*currency).count(),
        1,
        "one non-currency instrument"
    );
}

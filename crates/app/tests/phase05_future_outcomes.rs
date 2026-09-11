//! `binary-alpha outcomes build`: the deterministic publication, readback, invariance, and
//! rejection proofs always, and the governed reference parity proof when
//! `BINARY_ALPHA_TEST_CONFIG` names it.

mod common;

use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File};
use std::io::{BufRead, BufReader, Read};
use std::path::{Path, PathBuf};

use binary_alpha_engine::config::Config;
use binary_alpha_engine::dataset::GenerationManifest;
use binary_alpha_engine::features::{FeatureManifest, feature_generation_id};
use binary_alpha_engine::market::format_event_time_micros;
use binary_alpha_engine::outcomes::{
    InvalidReason, MISSING_INDEX, Outcome, OutcomeBuilder, OutcomeManifest, OutcomeRule,
    TICK_PRICE_OBJECT_PATH, TICK_TIME_OBJECT_PATH, outcome_generation_id, stream_object_paths,
};
use common::*;
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::record::RowAccessor;
use serde_json::json;

fn manifest_uri(path: &Path) -> String {
    format!("file://{}", path.display())
}

/// One tick line at `millis` since the Unix epoch.
fn tick_line(millis: i64, price_units: i64) -> String {
    let seconds = millis.div_euclid(1_000);
    format!(
        "{}.{:03}Z,AEDCNY,{}.{:06}",
        format_event_time_micros(seconds * 1_000_000)
            .strip_suffix(".000000Z")
            .unwrap(),
        millis.rem_euclid(1_000),
        price_units / 1_000_000,
        price_units % 1_000_000
    )
}

/// Thirty minutes of deterministic ticks four per second with, at known offsets, a five-second
/// gap followed by a frozen run, a frozen run followed by a jump, a frozen-by-time run, a jump,
/// a three-second gap, a minute of alternating prices (ties), and two three-second gaps three
/// seconds apart. One-unit moves are a hundredth of a basis point, so only the planted jumps
/// reach five basis points.
fn synthetic_ticks() -> Vec<(i64, i64)> {
    const BASE: i64 = 1_767_571_200_000; // 2026-01-05T00:00:00Z, a Monday
    let mut state: u64 = 0x2545_F491_4F6C_DD1D;
    let mut step = move || -> i64 {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        (state % 5) as i64 - 2
    };
    let mut price = 1_800_000_i64;
    let mut ticks = Vec::new();
    let mut offset = 0;
    while offset < 1_800_000 {
        let second = offset / 1_000;
        match second {
            // Ticks stop at 299.75 s and resume at 305.0 s with twelve identical prices.
            300 if offset == 300_000 => {
                offset = 305_000;
                for tick in 0..12 {
                    ticks.push((BASE + offset + tick * 250, price));
                }
                offset += 12 * 250;
                continue;
            }
            // Twelve identical prices from 600.0 s, then a jump of 1,200 units (6.7 basis
            // points) 250 ms later.
            600 if offset == 600_000 => {
                for tick in 0..12 {
                    ticks.push((BASE + offset + tick * 250, price));
                }
                offset += 12 * 250;
                price += 1_200;
                ticks.push((BASE + offset, price));
                offset += 250;
                continue;
            }
            // Four identical prices spread over 5.7 s: frozen by elapsed time, not by count.
            840 if offset == 840_000 => {
                for tick in 0..4 {
                    ticks.push((BASE + offset + tick * 1_900, price));
                }
                offset += 3 * 1_900 + 300;
                continue;
            }
            // A jump down of 1,300 units within 250 ms.
            900 if offset == 900_000 => {
                ticks.push((BASE + offset, price));
                price -= 1_300;
                ticks.push((BASE + offset + 250, price));
                offset += 500;
                continue;
            }
            // Ticks stop at 1199.75 s and resume 3 s later.
            1_200 if offset == 1_200_000 => {
                offset = 1_202_750;
                continue;
            }
            // A minute of alternating prices: windows of an even tick count tie.
            1_320..=1_379 => {
                price += if (offset / 250) % 2 == 0 { 1 } else { -1 };
                ticks.push((BASE + offset, price));
                offset += 250;
                continue;
            }
            // Ticks stop at 1619.75 s, resume at 1622.75 s for 3 s, stop, resume at 1628.75 s.
            1_620 if offset == 1_620_000 => {
                offset = 1_622_750;
                continue;
            }
            1_626 if offset == 1_626_000 => {
                offset = 1_628_750;
                continue;
            }
            _ => {}
        }
        price += step();
        ticks.push((BASE + offset, price));
        offset += 250;
    }
    ticks
}

fn tick_instrument(symbol: &str, candles: &str) -> String {
    format!(
        "\n[[instruments]]\nbroker = \"pocket_option\"\nprovider_symbol = \"{symbol}\"\nbase_currency = \"AED\"\nquote_currency = \"CNY\"\nprice_scale = 6\nnative_granularity = {{ kind = \"tick\" }}\ngap = {{ max_seconds = 2, reopen_seconds = 60 }}\nfrozen = {{ min_observations = 10, min_seconds = 5 }}\njump = {{ min_basis_points = 5 }}\nspan = {{ min_percent = 75 }}\nsessions = [{{ name = \"week\", open_seconds = 0, close_seconds = 604800 }}]\ncandles = [{candles}]\n"
    )
}

const CANDLES: &str = "{ duration_seconds = 5, offset_seconds = 0, min_observations = 9, hard_min_observations = 5 }, { duration_seconds = 15, offset_seconds = 5, min_observations = 29, hard_min_observations = 15 }";

fn feature_entry(input: &Path, profile: &Path, outputs: &str) -> String {
    format!(
        "\n[[features.instruments]]\nrole = \"development\"\ninput_manifest = \"{}\"\nprofile_manifest = \"{}\"\nstreams = [{{ duration_seconds = 5, offset_seconds = 0 }}, {{ duration_seconds = 15, offset_seconds = 5 }}]\noutputs = [{outputs}]\n",
        manifest_uri(input),
        manifest_uri(profile)
    )
}

const EXPIRIES: [u32; 5] = [5, 10, 20, 60, 600];

fn outcomes_table(role: &str, tick: &str, feature: &str) -> String {
    format!(
        "\n[outcomes]\nrole = \"{role}\"\ntick_manifest = \"{tick}\"\nfeature_manifest = \"{feature}\"\nexpiry_seconds = [5, 10, 20, 60, 600]\nmax_entry_delay_ms = 2000\nmax_settlement_delay_ms = 2000\nmax_tick_gap_ms = 2000\ntrue_jump_max_gap_ms = 2000\ntrue_jump_basis_points = \"5\"\nfrozen_min_ticks = 10\nfrozen_min_ms = 5000\n"
    )
}

fn build(config: &Path) -> Result<Vec<String>, String> {
    command(&["outcomes", "build", "--config", config.to_str().unwrap()])
}

fn object_path(
    store: &Path,
    objects: &[binary_alpha_engine::dataset::ObjectRecord],
    path: &str,
) -> PathBuf {
    store.join(
        &objects
            .iter()
            .find(|object| object.path == path)
            .unwrap()
            .key,
    )
}

/// One published stream's arrays, read from the store's objects alone.
struct PublishedStream {
    references: Vec<i64>,
    entries: Vec<u32>,
    settlements: Vec<u32>,
    reasons: Vec<u8>,
}

/// An outcome generation read back from its manifest and objects alone.
struct Published {
    manifest: OutcomeManifest,
    builder: OutcomeBuilder,
    streams: Vec<PublishedStream>,
}

/// The manifest and the builder over the shared tick arrays of a published outcome generation.
fn published_manifest(store: &Path, manifest_path: &Path) -> (OutcomeManifest, OutcomeBuilder) {
    let manifest = OutcomeManifest::from_json(&fs::read(manifest_path).unwrap()).unwrap();
    let object = |path: &str| object_path(store, &manifest.objects, path);
    let times = read_le(&object(TICK_TIME_OBJECT_PATH), i64::from_le_bytes);
    let prices = read_le(&object(TICK_PRICE_OBJECT_PATH), i64::from_le_bytes);
    let builder = OutcomeBuilder::new(manifest.rule.clone(), times, prices).unwrap();
    (manifest, builder)
}

/// One stream's arrays, read from the store's objects alone.
fn stream_arrays(store: &Path, manifest: &OutcomeManifest, index: usize) -> PublishedStream {
    let summary = &manifest.streams[index];
    let paths = stream_object_paths(summary.duration_seconds, summary.offset_seconds);
    let object = |path: &str| object_path(store, &manifest.objects, path);
    PublishedStream {
        references: read_le(&object(&paths[0]), i64::from_le_bytes),
        entries: read_le(&object(&paths[1]), u32::from_le_bytes),
        settlements: read_le(&object(&paths[2]), u32::from_le_bytes),
        reasons: fs::read(object(&paths[3])).unwrap(),
    }
}

fn published(store: &Path, manifest_path: &Path) -> Published {
    let (manifest, builder) = published_manifest(store, manifest_path);
    let streams = (0..manifest.streams.len())
        .map(|index| stream_arrays(store, &manifest, index))
        .collect();
    Published {
        manifest,
        builder,
        streams,
    }
}

/// One column of a published feature-row table through the generic row API.
fn feature_column(store: &Path, manifest: &FeatureManifest, stream: usize, name: &str) -> Vec<i64> {
    let summary = &manifest.streams[stream];
    let path = object_path(
        store,
        &manifest.objects,
        &format!(
            "rows/{}s_{}s.parquet",
            summary.duration_seconds, summary.offset_seconds
        ),
    );
    let reader = SerializedFileReader::new(File::open(path).unwrap()).unwrap();
    let index = reader
        .metadata()
        .file_metadata()
        .schema_descr()
        .columns()
        .iter()
        .position(|column| column.name() == name)
        .unwrap();
    reader
        .get_row_iter(None)
        .unwrap()
        .map(|row| {
            let row = row.unwrap();
            row.get_timestamp_micros(index)
                .or_else(|_| row.get_long(index))
                .unwrap()
        })
        .collect()
}

/// An independent per-cell oracle over the raw ticks: every reason that applies to a cell, so
/// the stored reason must be the first of them, plus the entry and settlement it expects.
struct Oracle {
    times: Vec<i64>,
    prices: Vec<i64>,
    frozen: Vec<bool>,
}

impl Oracle {
    fn new(ticks: &[(i64, i64)]) -> Self {
        let times: Vec<i64> = ticks.iter().map(|tick| tick.0 * 1_000).collect();
        let prices: Vec<i64> = ticks.iter().map(|tick| tick.1).collect();
        let mut frozen = vec![false; ticks.len()];
        let mut start = 0;
        while start < ticks.len() {
            let mut end = start;
            while end + 1 < ticks.len() && prices[end + 1] == prices[start] {
                end += 1;
            }
            if end - start + 1 >= 10 || times[end] - times[start] >= 5_000_000 {
                frozen[start..=end].fill(true);
            }
            start = end + 1;
        }
        Self {
            times,
            prices,
            frozen,
        }
    }

    fn entry(&self, reference: i64) -> Option<usize> {
        self.times.iter().position(|&time| time >= reference)
    }

    /// `(settlement index, applicable reasons)` of one cell from its entry index.
    fn cell(
        &self,
        reference: i64,
        entry: usize,
        expiry_seconds: u32,
    ) -> (Option<usize>, Vec<InvalidReason>) {
        use InvalidReason::*;
        let due = self.times[entry] + i64::from(expiry_seconds) * 1_000_000;
        let settlement = (entry..self.times.len()).find(|&index| self.times[index] >= due);
        let mut reasons = Vec::new();
        if self.times[entry] - reference > 2_000_000 {
            reasons.push(StaleEntry);
        }
        let Some(settlement) = settlement else {
            reasons.push(NoSettlement);
            return (None, reasons);
        };
        if self.times[settlement] - due > 2_000_000 {
            reasons.push(StaleSettlement);
        }
        if (entry + 1..=settlement)
            .any(|index| self.times[index] - self.times[index - 1] > 2_000_000)
        {
            reasons.push(InternalGap);
        }
        if (entry..=settlement).any(|index| self.frozen[index]) {
            reasons.push(FrozenRun);
        }
        if (entry + 1..=settlement).any(|index| {
            self.times[index] - self.times[index - 1] <= 2_000_000
                && (self.prices[index] - self.prices[index - 1]).abs() * 10_000
                    >= 5 * self.prices[index - 1]
        }) {
            reasons.push(TrueJump);
        }
        (Some(settlement), reasons)
    }
}

#[test]
fn outcomes_build_publishes_reconstructs_and_reuses() {
    let scratch = Scratch::new("phase05_outcomes");
    let ticks = synthetic_ticks();
    write_ticks(
        &scratch.path("sources/ticks/ticks.csv"),
        &ticks
            .iter()
            .map(|&(millis, price)| tick_line(millis, price))
            .collect::<Vec<_>>()
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>(),
    );
    let import_config = scratch.config("import.toml", &scratch.tick_source());
    let tick_manifest = {
        let line = import(&import_config).unwrap().remove(0);
        scratch.path(&format!(
            "published/manifests/{}/ready.json",
            generation(&line)
        ))
    };
    let audit_config = scratch.config("audit.toml", &tick_instrument("AEDCNY_otc", CANDLES));
    let profile_manifest = {
        let line = command(&[
            "data",
            "audit",
            "--config",
            audit_config.to_str().unwrap(),
            "--manifest",
            &manifest_uri(&tick_manifest),
        ])
        .unwrap()
        .remove(0);
        scratch.path(&format!(
            "published/manifests/{}/ready.json",
            generation(&line)
        ))
    };
    let features_config = scratch.config(
        "features.toml",
        &feature_entry(
            &tick_manifest,
            &profile_manifest,
            "\"close_time_micros\", \"known_at_micros\", \"candle_ordinal\"",
        ),
    );
    let feature_manifest = {
        let line = command(&[
            "features",
            "build",
            "--config",
            features_config.to_str().unwrap(),
        ])
        .unwrap()
        .remove(0);
        scratch.path(&format!(
            "published/manifests/{}/ready.json",
            generation(&line)
        ))
    };
    let store = scratch.path("published");
    let feature = FeatureManifest::from_json(&fs::read(&feature_manifest).unwrap()).unwrap();
    assert!(
        feature.streams.iter().all(|stream| stream.rows > 100),
        "{:?}",
        feature.streams
    );

    // The build publishes, reconstructs, and reports; verify re-reads the same generation.
    let config = scratch.config(
        "outcomes.toml",
        &outcomes_table(
            "development",
            &manifest_uri(&tick_manifest),
            &manifest_uri(&feature_manifest),
        ),
    );
    let lines = build(&config).unwrap();
    assert_eq!(lines.len(), 2, "{lines:?}");
    let rows: u64 = feature.streams.iter().map(|stream| stream.rows).sum();
    let cells = rows * EXPIRIES.len() as u64;
    assert!(
        lines[0].starts_with("outcomes pocket_option:AEDCNY_otc development generation ")
            && lines[0].contains(&format!(
                " ticks {} rows {rows} cells {cells} objects 10 reused 0 [load ",
                ticks.len()
            )),
        "{}",
        lines[0]
    );
    let outcome_generation = generation(&lines[0]);
    let manifest_path = scratch.path(&format!(
        "published/manifests/{outcome_generation}/ready.json"
    ));
    assert_eq!(
        lines[1],
        format!(
            "verified pocket_option:AEDCNY_otc development generation {outcome_generation} rows {rows} cells {cells} objects 10 bytes {}",
            fs::read(&manifest_path)
                .map(|bytes| OutcomeManifest::from_json(&bytes).unwrap())
                .unwrap()
                .objects
                .iter()
                .map(|object| object.bytes)
                .sum::<u64>()
        )
    );
    assert_eq!(verify(&manifest_path).unwrap(), lines[1]);
    assert_eq!(
        scratch.manifests("published").len(),
        4,
        "the tick, stream, feature, and outcome generations"
    );
    let retained = scratch
        .path("retained")
        .join(format!("manifests/{outcome_generation}/ready.json"));
    assert_eq!(
        fs::read(&retained).unwrap(),
        fs::read(&manifest_path).unwrap(),
        "mirrored locally"
    );

    // The manifest binds both inputs, the rule, the conventions, and this clean revision.
    let Published {
        manifest,
        builder,
        streams,
    } = published(&store, &manifest_path);
    let tick = GenerationManifest::from_json(&fs::read(&tick_manifest).unwrap()).unwrap();
    assert_eq!(manifest.generation, outcome_generation);
    assert_eq!(manifest.tick_generation, tick.generation);
    assert_eq!(manifest.feature_generation, feature.generation);
    assert_eq!(
        manifest.tick_manifest.to_string(),
        manifest_uri(&tick_manifest)
    );
    assert_eq!(
        manifest.feature_manifest.to_string(),
        manifest_uri(&feature_manifest)
    );
    assert_eq!(manifest.tick_count, ticks.len() as u64);
    assert_eq!(manifest.rule.expiry_seconds, EXPIRIES);
    assert_eq!(manifest.rule.max_entry_delay_micros, 2_000_000);
    assert_eq!(manifest.rule.frozen_min_micros, 5_000_000);
    assert_eq!(manifest.rule.true_jump_basis_points, "5");
    assert_eq!(manifest.code_revision, env!("BINARY_ALPHA_CODE_REVISION"));
    assert_eq!(
        manifest.config_hash,
        Config::parse(&fs::read_to_string(&config).unwrap())
            .unwrap()
            .content_hash()
    );
    assert_eq!(manifest.streams.len(), 2);
    for (summary, stream) in manifest.streams.iter().zip(&feature.streams) {
        assert_eq!(
            (
                summary.duration_seconds,
                summary.offset_seconds,
                summary.rows
            ),
            (stream.duration_seconds, stream.offset_seconds, stream.rows)
        );
        assert_eq!(summary.first_reference_time, stream.first_decision_time);
        assert_eq!(summary.last_reference_time, stream.last_decision_time);
    }
    assert_eq!(
        builder.times(),
        &ticks.iter().map(|tick| tick.0 * 1_000).collect::<Vec<_>>()[..]
    );
    assert_eq!(
        builder.prices(),
        &ticks.iter().map(|tick| tick.1).collect::<Vec<_>>()[..]
    );

    // Every cell reconstructs from the published objects alone and agrees with the independent
    // oracle; the stored reason is the first applicable one; every reason but `no_entry`, and
    // every result, occurs. `no_entry` cannot occur: the tick that finalizes a candle is at or
    // after its close, so it is the entry tick.
    let oracle = Oracle::new(&ticks);
    let mut seen_reasons = BTreeSet::new();
    let mut seen_results = BTreeSet::new();
    let mut combinations: BTreeSet<Vec<InvalidReason>> = BTreeSet::new();
    let mut stale_rows = 0;
    for (index, stream) in streams.iter().enumerate() {
        let closes = feature_column(&store, &feature, index, "close_time_micros");
        let known_at = feature_column(&store, &feature, index, "known_at_micros");
        let ordinals = feature_column(&store, &feature, index, "candle_ordinal");
        assert_eq!(
            stream.references, closes,
            "the ordered row mapping onto the feature rows"
        );
        assert!(ordinals.windows(2).all(|pair| pair[0] < pair[1]));
        for (row, &reference) in stream.references.iter().enumerate() {
            let entry = oracle.entry(reference).unwrap();
            assert_eq!(stream.entries[row], entry as u32);
            // The logical reference clock is the close; the entry tick is the tick that made
            // the row available, so actual availability equals the entry time.
            assert_eq!(builder.times()[entry], known_at[row]);
            assert!(known_at[row] >= reference);
            if known_at[row] - reference > 2_000_000 {
                stale_rows += 1;
            }
            for (column, &expiry) in EXPIRIES.iter().enumerate() {
                let position = row * EXPIRIES.len() + column;
                let (settlement, applicable) = oracle.cell(reference, entry, expiry);
                let stored = stream.settlements[position];
                assert_eq!(
                    settlement.map_or(MISSING_INDEX, |index| index as u32),
                    stored
                );
                let reason = InvalidReason::from_code(stream.reasons[position]).unwrap();
                assert_eq!(
                    reason,
                    applicable.first().copied().unwrap_or(InvalidReason::Valid),
                    "row {row} column {column}: {applicable:?}"
                );
                if applicable.len() > 1 {
                    combinations.insert(applicable.clone());
                }
                seen_reasons.insert(reason);
                let cell = builder
                    .cell(
                        stream.entries[row],
                        column,
                        stored,
                        stream.reasons[position],
                    )
                    .unwrap();
                let entry_tick = cell.entry.unwrap();
                assert_eq!(entry_tick.index as usize, entry);
                assert_eq!(entry_tick.event_time_micros, oracle.times[entry]);
                assert_eq!(entry_tick.price_units, oracle.prices[entry]);
                assert_eq!(
                    cell.due_time_micros,
                    Some(oracle.times[entry] + i64::from(expiry) * 1_000_000)
                );
                assert_eq!(cell.settlement.map(|tick| tick.index as usize), settlement);
                assert_eq!(cell.reason, reason);
                match settlement {
                    Some(settlement) if reason == InvalidReason::Valid => {
                        let expected = match oracle.prices[settlement].cmp(&oracle.prices[entry]) {
                            std::cmp::Ordering::Greater => Outcome::BuyWin,
                            std::cmp::Ordering::Less => Outcome::SellWin,
                            std::cmp::Ordering::Equal => Outcome::Tie,
                        };
                        assert_eq!(cell.outcome, Some(expected));
                        seen_results.insert(expected);
                    }
                    _ => assert_eq!(cell.outcome, None, "an invalid cell is never a result"),
                }
            }
        }
    }
    use InvalidReason::*;
    assert_eq!(
        seen_reasons.into_iter().collect::<Vec<_>>(),
        [
            Valid,
            StaleEntry,
            NoSettlement,
            StaleSettlement,
            InternalGap,
            FrozenRun,
            TrueJump
        ]
    );
    assert_eq!(
        seen_results.into_iter().collect::<Vec<_>>(),
        [Outcome::BuyWin, Outcome::SellWin, Outcome::Tie]
    );
    for expected in [
        vec![StaleSettlement, InternalGap],
        vec![StaleSettlement, InternalGap, FrozenRun],
        vec![InternalGap, FrozenRun],
        vec![FrozenRun, TrueJump],
        vec![StaleEntry, InternalGap],
    ] {
        assert!(
            combinations.contains(&expected),
            "{expected:?} never applied together: {combinations:?}"
        );
    }
    assert!(
        stale_rows >= 3,
        "{stale_rows} rows became available after the entry delay"
    );

    // Chunk sizes and decision-row prefixes over the complete tick generation give identical
    // cells; the labels of a row never depend on other rows.
    for stream in &streams {
        let whole = builder.label(&stream.references).unwrap();
        assert_eq!(whole.entries, stream.entries);
        assert_eq!(whole.settlements, stream.settlements);
        assert_eq!(whole.reasons, stream.reasons);
        for chunk_rows in [1, 7, 64] {
            let (mut entries, mut settlements, mut reasons) = (Vec::new(), Vec::new(), Vec::new());
            for chunk in stream.references.chunks(chunk_rows) {
                let labels = builder.label(chunk).unwrap();
                entries.extend(labels.entries);
                settlements.extend(labels.settlements);
                reasons.extend(labels.reasons);
            }
            assert_eq!(
                (entries, settlements, reasons),
                (
                    whole.entries.clone(),
                    whole.settlements.clone(),
                    whole.reasons.clone()
                ),
                "chunks of {chunk_rows}"
            );
        }
        for prefix in [1, 13, stream.references.len() / 2] {
            let labels = builder.label(&stream.references[..prefix]).unwrap();
            assert_eq!(labels.entries, whole.entries[..prefix]);
            assert_eq!(
                labels.settlements,
                whole.settlements[..prefix * EXPIRIES.len()]
            );
            assert_eq!(labels.reasons, whole.reasons[..prefix * EXPIRIES.len()]);
        }
    }

    // An identical rerun reuses every object and the committed manifest.
    let rerun = build(&config).unwrap();
    assert_eq!(
        rerun[0],
        lines[0]
            .split(" [load ")
            .next()
            .unwrap()
            .replace("reused 0", "reused 10")
            + " (already published)"
    );
    assert_eq!(rerun[1], lines[1]);
    assert_eq!(fs::read(&manifest_path).unwrap(), manifest.to_json());
    // A committed manifest whose invariant metadata differs is never reused: the same
    // generation with another price scale stops the build without replacing anything.
    let mut conflicting = manifest_json(&manifest_path);
    conflicting["price_representation"]["scale"] = json!(5);
    fs::write(
        &manifest_path,
        serde_json::to_vec_pretty(&conflicting).unwrap(),
    )
    .unwrap();
    let error = build(&config).unwrap_err();
    assert!(
        error.contains(
            "records a different generation, instrument, role, row identity, price representation"
        ),
        "{error}"
    );
    fs::write(&manifest_path, manifest.to_json()).unwrap();
    // A run interrupted before the ready manifest leaves an incomplete generation that a rerun
    // completes through the same immutable writes.
    fs::remove_file(&manifest_path).unwrap();
    assert!(verify(&manifest_path).unwrap_err().contains("cannot open"));
    let resumed = build(&config).unwrap();
    assert!(
        resumed[0].contains(" objects 10 reused 10 [load "),
        "{}",
        resumed[0]
    );
    assert_eq!(fs::read(&manifest_path).unwrap(), manifest.to_json());
    // Verification recomputes every label: a reason object whose bytes and hash are internally
    // consistent but whose one changed reason disagrees with the ticks is rejected.
    let reason_object = manifest
        .objects
        .iter()
        .find(|object| object.path == "reason/5s_0s.bin")
        .unwrap();
    let mut reasons = fs::read(store.join(&reason_object.key)).unwrap();
    reasons[0] = if reasons[0] == 0 { 7 } else { 0 };
    let tampered_path = scratch.path("tampered_reason.bin");
    fs::write(&tampered_path, &reasons).unwrap();
    let tampered_sha = sha256(&tampered_path);
    fs::copy(
        &tampered_path,
        store.join(format!("objects/{tampered_sha}")),
    )
    .unwrap();
    let mut tampered = manifest_json(&manifest_path);
    for object in tampered["objects"].as_array_mut().unwrap() {
        if object["path"] == "reason/5s_0s.bin" {
            object["key"] = json!(format!("objects/{tampered_sha}"));
            object["sha256"] = json!(tampered_sha);
        }
    }
    fs::write(
        &manifest_path,
        serde_json::to_vec_pretty(&tampered).unwrap(),
    )
    .unwrap();
    let error = verify(&manifest_path).unwrap_err();
    assert!(
        error.contains("the stored reasons disagree with the labels recomputed from the published ticks and reference times"),
        "{error}"
    );
    fs::write(&manifest_path, manifest.to_json()).unwrap();
    // A manifest whose instrument text contradicts its broker and symbol is rejected on parse.
    let mut tampered = manifest_json(&manifest_path);
    tampered["instrument"] = json!("pocket_option:OTHER_otc");
    let error = OutcomeManifest::from_json(&serde_json::to_vec(&tampered).unwrap()).unwrap_err();
    assert!(
        error.contains("instrument `pocket_option:OTHER_otc` is not `pocket_option:AEDCNY_otc`"),
        "{error}"
    );
    // Inputs and outputs may live in different stores: the same build into another
    // destination publishes the same objects there under the same generation.
    let separate = scratch.path("separate.toml");
    fs::write(
        &separate,
        format!(
            "schema_version = 1\nrun_mode = \"research\"\n\n[storage]\nhistorical_data_dir = \"retained_separate\"\npublication_uri = \"file://{}\"\n{}",
            scratch.path("published_separate").display(),
            outcomes_table(
                "development",
                &manifest_uri(&tick_manifest),
                &manifest_uri(&feature_manifest)
            )
        ),
    )
    .unwrap();
    let separate_lines = build(&separate).unwrap();
    assert_eq!(generation(&separate_lines[0]), outcome_generation);
    assert!(
        separate_lines[0].contains(" objects 10 reused 0 [load "),
        "{}",
        separate_lines[0]
    );
    let separate_manifest = scratch.path(&format!(
        "published_separate/manifests/{outcome_generation}/ready.json"
    ));
    assert_eq!(verify(&separate_manifest).unwrap(), separate_lines[1]);
    assert_eq!(
        OutcomeManifest::from_json(&fs::read(&separate_manifest).unwrap())
            .unwrap()
            .objects,
        manifest.objects
    );

    // Refusals name the field and publish no outcome generation.
    let manifests_before = scratch.manifests("published").len();
    let refusal = |name: &str, table: &str| -> String {
        let config = scratch.config(name, table);
        build(&config).unwrap_err()
    };
    let cases = [
        (
            "role.toml",
            outcomes_table(
                "evaluation",
                &manifest_uri(&tick_manifest),
                &manifest_uri(&feature_manifest),
            ),
            "outcomes: role: declared `evaluation`, but generation",
        ),
        (
            "swapped.toml",
            outcomes_table(
                "development",
                &manifest_uri(&feature_manifest),
                &manifest_uri(&feature_manifest),
            ),
            "is a `feature_generation` manifest, not a dataset ready manifest",
        ),
        (
            "stream.toml",
            outcomes_table(
                "development",
                &manifest_uri(&tick_manifest),
                &manifest_uri(&profile_manifest),
            ),
            "is not a feature generation manifest",
        ),
        (
            "missing.toml",
            String::new(),
            "outcomes: the table is required",
        ),
    ];
    for (name, table, expected) in cases {
        let error = refusal(name, &table);
        assert!(error.contains(expected), "{name}: {error}");
    }
    // A feature manifest naming another tick generation is refused on its bytes alone: this
    // crafted copy has no objects, so fetching its plan first would fail differently.
    let mut crafted = manifest_json(&feature_manifest);
    let other_input = "f".repeat(64);
    crafted["generation"] = json!(feature_generation_id(&feature.plan_identity, &other_input));
    crafted["input_generation"] = json!(other_input);
    let crafted_path = scratch.path(&format!(
        "crafted/manifests/{}/ready.json",
        crafted["generation"].as_str().unwrap()
    ));
    fs::create_dir_all(crafted_path.parent().unwrap()).unwrap();
    fs::write(&crafted_path, serde_json::to_vec_pretty(&crafted).unwrap()).unwrap();
    let error = refusal(
        "crafted.toml",
        &outcomes_table(
            "development",
            &manifest_uri(&tick_manifest),
            &manifest_uri(&crafted_path),
        ),
    );
    assert!(
        error.contains(&format!(
            "was computed from tick generation {other_input}, not {}",
            tick.generation
        )),
        "{error}"
    );
    // A tick manifest declaring more ticks than the missing index admits is refused before
    // anything is allocated or read.
    let mut oversized = manifest_json(&tick_manifest);
    oversized["row_count"] = json!(4_294_967_296u64);
    let oversized_path = scratch.path(&format!(
        "oversized/manifests/{}/ready.json",
        oversized["generation"].as_str().unwrap()
    ));
    fs::create_dir_all(oversized_path.parent().unwrap()).unwrap();
    fs::write(
        &oversized_path,
        serde_json::to_vec_pretty(&oversized).unwrap(),
    )
    .unwrap();
    let error = refusal(
        "oversized.toml",
        &outcomes_table(
            "development",
            &manifest_uri(&oversized_path),
            &manifest_uri(&feature_manifest),
        ),
    );
    assert!(
        error.contains(
            "outcomes: tick_manifest: 4294967296 ticks cannot be indexed below the missing index"
        ),
        "{error}"
    );
    // A manifest whose references or rule disagree with its identity is rejected on its bytes.
    let mut tampered = manifest_json(&manifest_path);
    tampered["tick_manifest"] = json!(manifest_uri(&feature_manifest));
    let error = OutcomeManifest::from_json(&serde_json::to_vec(&tampered).unwrap()).unwrap_err();
    assert!(
        error.contains("do not name the recorded tick and feature generations"),
        "{error}"
    );
    let mut tampered = manifest_json(&manifest_path);
    tampered["rule"]["expiry_seconds"] = json!([10, 5]);
    let rule: OutcomeRule = serde_json::from_value(tampered["rule"].clone()).unwrap();
    tampered["generation"] = json!(outcome_generation_id(
        &manifest.tick_generation,
        &manifest.feature_generation,
        &rule
    ));
    let error = OutcomeManifest::from_json(&serde_json::to_vec(&tampered).unwrap()).unwrap_err();
    assert!(error.contains("expiry_seconds"), "{error}");
    // Every stream plan carries the candle clocks, so a feature generation naming one other
    // output labels the same rows into byte-identical objects under its own manifest.
    let bare_config = scratch.config(
        "features_bare.toml",
        &feature_entry(&tick_manifest, &profile_manifest, "\"candle_ordinal\""),
    );
    let bare_manifest = {
        let line = command(&[
            "features",
            "build",
            "--config",
            bare_config.to_str().unwrap(),
        ])
        .unwrap()
        .remove(0);
        scratch.path(&format!(
            "published/manifests/{}/ready.json",
            generation(&line)
        ))
    };
    assert_ne!(bare_manifest, feature_manifest);
    let bare = build(&scratch.config(
        "bare.toml",
        &outcomes_table(
            "development",
            &manifest_uri(&tick_manifest),
            &manifest_uri(&bare_manifest),
        ),
    ))
    .unwrap();
    assert_ne!(generation(&bare[0]), outcome_generation);
    assert!(
        bare[0].contains(" objects 10 reused 10 [load "),
        "{}",
        bare[0]
    );
    // Another instrument's complete feature generation (its own ticks, stream, plan, and rows),
    // re-labeled as this instrument's by a crafted manifest that keeps that plan readable, is
    // refused because the plan does not describe the manifest's instrument.
    write_ticks(
        &scratch.path("sources/other/ticks.csv"),
        &ticks
            .iter()
            .take(2_400)
            .map(|&(millis, price)| tick_line(millis, price))
            .collect::<Vec<_>>()
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>(),
    );
    let other_config = scratch.config(
        "other.toml",
        &scratch
            .tick_source()
            .replace("sources/ticks/ticks.csv", "sources/other/ticks.csv")
            .replace(
                "provider_symbol = \"AEDCNY_otc\"",
                "provider_symbol = \"OTHER_otc\"",
            ),
    );
    let other_tick = {
        let line = import(&other_config).unwrap().remove(0);
        scratch.path(&format!(
            "published/manifests/{}/ready.json",
            generation(&line)
        ))
    };
    let other_audit = scratch.config("other_audit.toml", &tick_instrument("OTHER_otc", CANDLES));
    let other_profile = {
        let line = command(&[
            "data",
            "audit",
            "--config",
            other_audit.to_str().unwrap(),
            "--manifest",
            &manifest_uri(&other_tick),
        ])
        .unwrap()
        .remove(0);
        scratch.path(&format!(
            "published/manifests/{}/ready.json",
            generation(&line)
        ))
    };
    let other_features = scratch.config(
        "other_features.toml",
        &feature_entry(&other_tick, &other_profile, "\"close_time_micros\""),
    );
    let other_feature = {
        let line = command(&[
            "features",
            "build",
            "--config",
            other_features.to_str().unwrap(),
        ])
        .unwrap()
        .remove(0);
        scratch.path(&format!(
            "published/manifests/{}/ready.json",
            generation(&line)
        ))
    };
    let mut relabeled = manifest_json(&other_feature);
    for field in ["broker", "provider_symbol", "instrument"] {
        relabeled[field] = json!(manifest_json(&feature_manifest)[field]);
    }
    relabeled["input_generation"] = json!(tick.generation);
    relabeled["generation"] = json!(feature_generation_id(
        relabeled["plan_identity"].as_str().unwrap(),
        &tick.generation
    ));
    let relabeled_path = scratch.path(&format!(
        "relabeled/manifests/{}/ready.json",
        relabeled["generation"].as_str().unwrap()
    ));
    fs::create_dir_all(relabeled_path.parent().unwrap()).unwrap();
    fs::write(
        &relabeled_path,
        serde_json::to_vec_pretty(&relabeled).unwrap(),
    )
    .unwrap();
    let plan_key = relabeled["objects"]
        .as_array()
        .unwrap()
        .iter()
        .find(|object| object["path"] == "plan.json")
        .unwrap()["key"]
        .as_str()
        .unwrap()
        .to_string();
    fs::create_dir_all(scratch.path("relabeled/objects")).unwrap();
    fs::copy(
        store.join(&plan_key),
        scratch.path("relabeled").join(&plan_key),
    )
    .unwrap();
    let error = refusal(
        "relabeled.toml",
        &outcomes_table(
            "development",
            &manifest_uri(&tick_manifest),
            &manifest_uri(&relabeled_path),
        ),
    );
    assert!(
        error.contains("outcomes: feature_manifest: the plan does not describe the manifest's plan identity, instrument"),
        "{error}"
    );
    // A bar generation has no ticks; the refusal is the machine-readable capability error.
    let bar_root = scratch.path("sources/bars");
    write_collection(
        &bar_root,
        &[AssetSpec {
            asset: "AEDCNY_otc",
            expected_symbol_id: Some(7),
            symbol_id: Some(7),
            files: vec![bars("AEDCNY_otc", 7, 1_767_571_200, 40)],
            metadata: true,
        }],
    );
    let bar_config = scratch.config("bars.toml", &scratch.bar_source());
    let bar_manifest = {
        let line = import(&bar_config).unwrap().remove(0);
        scratch.path(&format!(
            "published/manifests/{}/ready.json",
            generation(&line)
        ))
    };
    let error = refusal(
        "bar_outcomes.toml",
        &outcomes_table(
            "evaluation",
            &manifest_uri(&bar_manifest),
            &manifest_uri(&feature_manifest),
        ),
    );
    assert!(
        error.contains("outcomes: tick_manifest: {\"required\":\"ticks\",\"provided\":[\"bars\"]"),
        "{error}"
    );
    // Configuration refusals happen before any store is opened.
    let holdout = scratch.config(
        "holdout.toml",
        &outcomes_table(
            "holdout",
            &manifest_uri(&tick_manifest),
            &manifest_uri(&feature_manifest),
        ),
    );
    assert!(
        build(&holdout)
            .unwrap_err()
            .contains("outcomes.role: holdout data never enters an outcome build")
    );
    let replay = scratch.path("replay.toml");
    fs::write(
        &replay,
        format!(
            "schema_version = 1\nrun_mode = \"replay\"\n\n[storage]\nhistorical_data_dir = \"retained\"\npublication_uri = \"gs://example-bucket/historical\"\n{}",
            outcomes_table("development", &manifest_uri(&tick_manifest), &manifest_uri(&feature_manifest))
        ),
    )
    .unwrap();
    assert!(
        build(&replay)
            .unwrap_err()
            .contains("run_mode: an outcome build is research, not `replay`")
    );
    let manifests = scratch.manifests("published");
    assert_eq!(
        manifests.len() - manifests_before,
        6,
        "the bare feature and outcome, the other instrument's tick, stream, and feature, and the bar generations; no refusal published"
    );
    assert_eq!(
        manifests
            .iter()
            .filter(|path| manifest_json(path)["kind"] == "outcome_generation")
            .count(),
        2
    );
}

/// No feature implementation or live feature path names the outcome owner: labels are read
/// only after feature identity is frozen, never inside the causal feature graph.
#[test]
fn features_never_depend_on_outcomes() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).parent().unwrap();
    for path in [
        "engine/src/dataset.rs",
        "engine/src/features.rs",
        "engine/src/market.rs",
        "engine/src/stream.rs",
        "app/src/archive.rs",
        "app/src/audit.rs",
        "app/src/features.rs",
        "app/src/import.rs",
        "app/src/store.rs",
    ] {
        let source = fs::read_to_string(root.join(path)).unwrap();
        assert!(
            !source.contains("outcomes::") && !source.contains("OutcomeBuilder"),
            "{path} references the outcome owner"
        );
    }
}

#[derive(serde::Deserialize)]
struct GovernedConfig {
    /// The research configuration `outcomes build` runs: filesystem publication, the Phase 02
    /// tick generation, and the Phase 04 feature generation with the five reference streams.
    application_config: PathBuf,
    /// The legacy reference root whose expected files the checked-in allowlist names.
    reference_root: PathBuf,
}

#[derive(serde::Deserialize)]
struct ReferenceFixture {
    legacy_revision: String,
    builder_sha256: String,
    source_sha256: String,
    source_rows: u64,
    policies: BTreeMap<String, String>,
    reference_files: Vec<ReferenceFile>,
    expiry_seconds: ExpiryRange,
    streams: Vec<ReferenceStream>,
}

#[derive(serde::Deserialize)]
struct ReferenceFile {
    path: String,
    sha256: String,
}

#[derive(serde::Deserialize)]
struct ExpiryRange {
    min: u32,
    max: u32,
    count: usize,
}

#[derive(serde::Deserialize)]
struct ReferenceStream {
    label: String,
    duration_seconds: u32,
    offset_seconds: u32,
    rows: u64,
}

/// A NumPy `.npy` file opened at its data section after its header was checked.
struct Npy {
    file: File,
    rows: usize,
    columns: usize,
}

impl Npy {
    fn open(path: &Path, dtype: &str, width: usize) -> Self {
        let mut file = File::open(path).unwrap();
        let mut magic = [0u8; 10];
        file.read_exact(&mut magic).unwrap();
        assert_eq!(&magic[..8], b"\x93NUMPY\x01\x00", "{}", path.display());
        let header_len = u16::from_le_bytes([magic[8], magic[9]]) as usize;
        let mut header = vec![0u8; header_len];
        file.read_exact(&mut header).unwrap();
        let header = String::from_utf8(header).unwrap();
        assert!(header.contains(&format!("'descr': '{dtype}'")), "{header}");
        assert!(header.contains("'fortran_order': False"), "{header}");
        let shape = header
            .split("'shape': (")
            .nth(1)
            .unwrap()
            .split(')')
            .next()
            .unwrap();
        let dims: Vec<usize> = shape
            .split(',')
            .map(str::trim)
            .filter(|text| !text.is_empty())
            .map(|text| text.parse().unwrap())
            .collect();
        let (rows, columns) = (dims[0], dims.get(1).copied().unwrap_or(1));
        assert_eq!(
            file.metadata().unwrap().len(),
            (10 + header_len + rows * columns * width) as u64,
            "{}",
            path.display()
        );
        Self {
            file,
            rows,
            columns,
        }
    }

    fn read_all<T, const N: usize>(mut self, decode: fn([u8; N]) -> T) -> Vec<T> {
        let mut bytes = Vec::new();
        self.file.read_to_end(&mut bytes).unwrap();
        bytes
            .as_chunks::<N>()
            .0
            .iter()
            .map(|chunk| decode(*chunk))
            .collect()
    }

    fn read_rows<T, const N: usize>(&mut self, rows: usize, decode: fn([u8; N]) -> T) -> Vec<T> {
        let mut bytes = vec![0u8; rows * self.columns * N];
        self.file.read_exact(&mut bytes).unwrap();
        bytes
            .as_chunks::<N>()
            .0
            .iter()
            .map(|chunk| decode(*chunk))
            .collect()
    }
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
            Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/phase05_reference.json"),
        )
        .unwrap(),
    )
    .unwrap();
    let root = &governed.reference_root;

    // Every allowlisted reference file and policy carries exactly its registered content hash.
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
    let summary_path = fixture
        .policies
        .keys()
        .find(|path| path.ends_with("_summary.json"))
        .unwrap();
    let summary: serde_json::Value =
        serde_json::from_slice(&fs::read(root.join(summary_path)).unwrap()).unwrap();
    assert_eq!(
        summary["script_hash"], fixture.builder_sha256,
        "the summary names the pinned builder"
    );
    println!(
        "reference allowlist: {} files and {} policies hashed in {:.1} s against legacy revision {} builder {}",
        fixture.reference_files.len(),
        fixture.policies.len(),
        started.elapsed().as_secs_f64(),
        fixture.legacy_revision,
        fixture.builder_sha256
    );

    // The application configuration binds the registered source through its tick generation.
    let config = Config::parse(&fs::read_to_string(&governed.application_config).unwrap()).unwrap();
    let settings = config.outcomes.as_ref().unwrap();
    let local = |uri: &str| PathBuf::from(uri.strip_prefix("file://").unwrap());
    let tick = GenerationManifest::from_json(
        &fs::read(local(&settings.tick_manifest.to_string())).unwrap(),
    )
    .unwrap();
    assert_eq!(tick.row_count, fixture.source_rows);
    assert_eq!(
        tick.inputs[0].sha256, fixture.source_sha256,
        "imported from the registered source"
    );
    let expiries: Vec<u32> = (fixture.expiry_seconds.min..=fixture.expiry_seconds.max).collect();
    assert_eq!(expiries.len(), fixture.expiry_seconds.count);
    assert_eq!(
        settings.expiry_seconds, expiries,
        "every second of the reference range"
    );
    let store = match &config.storage.publication_uri {
        binary_alpha_engine::config::PublicationUri::Filesystem(path) => path.clone(),
        other => panic!("{other} is not the filesystem boundary"),
    };
    let (build_lines, build_wall, build_peak) = timed(&[
        "outcomes",
        "build",
        "--config",
        governed.application_config.to_str().unwrap(),
    ]);
    println!("build: {}", build_lines[0]);
    println!("reconstruction: {}", build_lines[1]);
    println!(
        "build wall {build_wall:.3} s, peak resident {build_peak} kB (load, label, publish, and reconstruction in one process)"
    );
    let generation = generation(&build_lines[0]);
    let manifest_path = store.join(format!("manifests/{generation}/ready.json"));
    let (verify_lines, verify_wall, verify_peak) = timed(&[
        "data",
        "verify",
        "--manifest",
        &manifest_uri(&manifest_path),
    ]);
    assert_eq!(verify_lines, build_lines[1..]);
    println!("verify: {}", verify_lines[0]);
    println!("verify wall {verify_wall:.3} s, peak resident {verify_peak} kB");

    let (manifest, builder) = published_manifest(&store, &manifest_path);
    assert_eq!(manifest.code_revision, env!("BINARY_ALPHA_CODE_REVISION"));
    assert!(
        !manifest.code_revision.ends_with("-dirty") && manifest.code_revision != "unavailable",
        "the governed proof binds to a clean commit, not {}",
        manifest.code_revision
    );
    assert_eq!(manifest.config_hash, config.content_hash());
    assert_eq!(manifest.tick_count, fixture.source_rows);
    assert_eq!(manifest.streams.len(), fixture.streams.len());
    println!(
        "outcome generation {} tick {} feature {} raw identity {} config {}",
        manifest.generation,
        manifest.tick_generation,
        manifest.feature_generation,
        manifest.raw_identity,
        manifest.config_hash
    );

    // The shared tick arrays map exactly onto the reference's milliseconds and eight-place
    // scaled prices.
    let outcomes_dir = root.join(Path::new(summary_path).parent().unwrap());
    let legacy_times = Npy::open(&outcomes_dir.join("tick_times_ms_i64.npy"), "<i8", 8)
        .read_all(i64::from_le_bytes);
    let legacy_prices = Npy::open(
        &outcomes_dir.join("tick_prices_scaled_1e8_i64.npy"),
        "<i8",
        8,
    )
    .read_all(i64::from_le_bytes);
    assert_eq!(legacy_times.len(), fixture.source_rows as usize);
    assert!(
        builder
            .times()
            .iter()
            .zip(&legacy_times)
            .all(|(micros, millis)| *micros == millis * 1_000)
    );
    assert!(
        builder
            .prices()
            .iter()
            .zip(&legacy_prices)
            .all(|(units, scaled)| *units * 100 == *scaled)
    );
    let expiry_list: Vec<String> =
        BufReader::new(File::open(outcomes_dir.join("expiry_seconds.csv")).unwrap())
            .lines()
            .map(Result::unwrap)
            .collect();
    assert_eq!(expiry_list[0], "expiry_index,expiry_seconds");
    assert_eq!(
        expiry_list[1..]
            .iter()
            .map(|line| line.split(',').nth(1).unwrap().parse::<u32>().unwrap())
            .collect::<Vec<_>>(),
        expiries
    );
    println!(
        "tick arrays: {} ticks map exactly; {} expiries match the reference list",
        legacy_times.len(),
        expiries.len()
    );

    // Per stream: the row mapping and entry fields once per row against the entry reference,
    // then every cell's settlement index, reason, due time, settlement time and price, and
    // result against the matrices, plus the totals the registered summary records.
    let feature_store = local(&settings.feature_manifest.root.to_string());
    let feature = FeatureManifest::from_json(
        &fs::read(local(&settings.feature_manifest.to_string())).unwrap(),
    )
    .unwrap();
    let mut total_cells = 0u64;
    for (stream_index, (reference, summary_stream)) in
        fixture.streams.iter().zip(&manifest.streams).enumerate()
    {
        let started = std::time::Instant::now();
        assert_eq!(
            (
                summary_stream.duration_seconds,
                summary_stream.offset_seconds,
                summary_stream.rows
            ),
            (
                reference.duration_seconds,
                reference.offset_seconds,
                reference.rows
            ),
            "{}",
            reference.label
        );
        let rows = reference.rows as usize;
        let published_stream = stream_arrays(&store, &manifest, stream_index);
        let ordinals = feature_column(&feature_store, &feature, stream_index, "candle_ordinal");
        let entry_reference = BufReader::new(
            File::open(outcomes_dir.join(format!("entry_reference_{}.csv", reference.label)))
                .unwrap(),
        );
        let mut lines = entry_reference.lines().map(Result::unwrap);
        assert_eq!(
            lines.next().unwrap(),
            "candle_set,source_row_number,open_time_utc,close_time_utc,decision_time_ms,entry_tick_index,entry_tick_time_ms,entry_delay_ms,entry_price_scaled_1e8"
        );
        for (row, line) in lines.enumerate() {
            let fields: Vec<&str> = line.split(',').collect();
            assert_eq!(fields[0], reference.label);
            assert_eq!(
                fields[1].parse::<i64>().unwrap(),
                ordinals[row] + 1,
                "row {row}: source_row_number = candle_ordinal + 1"
            );
            let decision_ms: i64 = fields[4].parse().unwrap();
            assert_eq!(
                published_stream.references[row],
                decision_ms * 1_000,
                "row {row} reference clock"
            );
            let entry = published_stream.entries[row];
            let cell = builder
                .cell(entry, 0, MISSING_INDEX, InvalidReason::NoSettlement.code())
                .unwrap();
            let entry_tick = cell.entry.expect("every reference row has an entry");
            assert_eq!(
                fields[5].parse::<u32>().unwrap(),
                entry_tick.index,
                "row {row} entry index"
            );
            assert_eq!(
                fields[6].parse::<i64>().unwrap() * 1_000,
                entry_tick.event_time_micros,
                "row {row} entry time"
            );
            assert_eq!(
                fields[7].parse::<i64>().unwrap() * 1_000,
                entry_tick.event_time_micros - published_stream.references[row],
                "row {row} entry delay"
            );
            assert_eq!(
                fields[8].parse::<i64>().unwrap(),
                entry_tick.price_units * 100,
                "row {row} entry price"
            );
            assert!(row < rows);
        }
        assert_eq!(ordinals.len(), rows);

        let mut legacy_settlement = Npy::open(
            &outcomes_dir.join(format!("settlement_tick_index_{}.npy", reference.label)),
            "<u4",
            4,
        );
        let mut legacy_reason = Npy::open(
            &outcomes_dir.join(format!("invalid_reason_{}.npy", reference.label)),
            "|u1",
            1,
        );
        assert_eq!(
            (legacy_settlement.rows, legacy_settlement.columns),
            (rows, expiries.len())
        );
        assert_eq!(
            (legacy_reason.rows, legacy_reason.columns),
            (rows, expiries.len())
        );
        let columns = expiries.len();
        let mut counts = [0u64; 8];
        let (mut buy, mut sell, mut tie) = (0u64, 0u64, 0u64);
        const CHUNK: usize = 8_192;
        for start in (0..rows).step_by(CHUNK) {
            let end = (start + CHUNK).min(rows);
            let settlements = legacy_settlement.read_rows(end - start, u32::from_le_bytes);
            let reasons = legacy_reason.read_rows(end - start, |[byte]| byte);
            let cells = start * columns..end * columns;
            assert_eq!(
                published_stream.settlements[cells.clone()],
                settlements[..],
                "{} rows {start}..{end} settlement indices",
                reference.label
            );
            assert_eq!(
                published_stream.reasons[cells.clone()],
                reasons[..],
                "{} rows {start}..{end} reasons",
                reference.label
            );
            for offset in 0..cells.len() {
                let (row, column) = (start + offset / columns, offset % columns);
                let cell = builder
                    .cell(
                        published_stream.entries[row],
                        column,
                        settlements[offset],
                        reasons[offset],
                    )
                    .unwrap();
                counts[usize::from(reasons[offset])] += 1;
                let entry = cell.entry.unwrap();
                assert_eq!(
                    cell.due_time_micros,
                    Some(
                        (legacy_times[entry.index as usize] + i64::from(expiries[column]) * 1_000)
                            * 1_000
                    )
                );
                assert_eq!(cell.reason.code(), reasons[offset]);
                match cell.settlement {
                    Some(settlement) => {
                        let index = settlements[offset] as usize;
                        assert_eq!(settlement.index as usize, index);
                        assert_eq!(settlement.event_time_micros, legacy_times[index] * 1_000);
                        assert_eq!(settlement.price_units * 100, legacy_prices[index]);
                    }
                    None => assert_eq!(settlements[offset], MISSING_INDEX),
                }
                let expected = (reasons[offset] == 0).then(|| {
                    let settlement = settlements[offset] as usize;
                    match legacy_prices[settlement].cmp(&legacy_prices[entry.index as usize]) {
                        std::cmp::Ordering::Greater => Outcome::BuyWin,
                        std::cmp::Ordering::Less => Outcome::SellWin,
                        std::cmp::Ordering::Equal => Outcome::Tie,
                    }
                });
                assert_eq!(cell.outcome, expected);
                match cell.outcome {
                    Some(Outcome::BuyWin) => buy += 1,
                    Some(Outcome::SellWin) => sell += 1,
                    Some(Outcome::Tie) => tie += 1,
                    None => {}
                }
            }
        }
        let totals = &summary["timeframes"][&reference.label];
        assert_eq!(totals["rows"], rows as u64);
        assert_eq!(totals["buy_wins_total"], buy, "{}", reference.label);
        assert_eq!(totals["sell_wins_total"], sell, "{}", reference.label);
        assert_eq!(totals["ties_total"], tie, "{}", reference.label);
        assert_eq!(
            totals["valid_outcomes_total"], counts[0],
            "{}",
            reference.label
        );
        for (name, code) in [
            ("no_entry_tick", 1),
            ("entry_tick_too_stale", 2),
            ("no_settlement_tick", 3),
            ("settlement_tick_too_stale", 4),
            ("bad_tick_gap_inside_window", 5),
            ("frozen_price_run_inside_window", 6),
            ("true_tick_jump_inside_window", 7),
        ] {
            assert_eq!(
                totals["invalid_reason_totals"][name], counts[code],
                "{} {name}",
                reference.label
            );
        }
        total_cells += (rows * columns) as u64;
        println!(
            "{}: {rows} rows and {} cells equal the reference in {:.1} s; valid {} buy {buy} sell {sell} tie {tie} invalid {:?}",
            reference.label,
            rows * columns,
            started.elapsed().as_secs_f64(),
            counts[0],
            &counts[1..]
        );
    }
    assert_eq!(
        total_cells,
        summary["totals"]["outcome_cells"].as_u64().unwrap()
    );
    println!(
        "compared {total_cells} cells across {} streams; test process peak resident {} kB",
        fixture.streams.len(),
        in_process_peak_kb()
    );
}

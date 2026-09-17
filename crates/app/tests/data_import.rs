//! Exercises `binary-alpha data import` and `binary-alpha data verify` end to end against
//! synthetic tick and bar sources, the retained folder, and a `file://` destination.

mod common;

use std::fs::{self, File};
use std::path::{Path, PathBuf};

use common::*;
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::record::RowAccessor;
use serde_json::{Value, json};

const TICK_ROWS: [&str; 4] = [
    "2026-03-22T06:02:39.312Z,AEDCNY,1.80787",
    "2026-03-22T06:02:39.530Z,AEDCNY,1.80787",
    "2026-03-22T06:02:39.530Z,AEDCNY,1.80787",
    "2026-03-22T06:02:40.001Z,AEDCNY,1.914290",
];

/// The exact normalized rows of `TICK_ROWS`: Unix microseconds and units at scale six.
const NORMALIZED_TICKS: [(i64, i64); 4] = [
    (1_774_159_359_312_000, 1_807_870),
    (1_774_159_359_530_000, 1_807_870),
    (1_774_159_359_530_000, 1_807_870),
    (1_774_159_360_001_000, 1_914_290),
];

/// Unix seconds at 2025-08-11T00:00:00Z.
const DAY_2025_08_11: i64 = 1_754_870_400;

fn ns(seconds: i64) -> i64 {
    seconds * 1_000_000_000
}

/// A two-directory daily archive beside entries that are never inspected.
fn daily_sources(scratch: &Scratch) {
    let root = scratch.path("sources/deriv");
    write_daily_directory(
        &root.join("AUDUSD"),
        "AUDUSD",
        "frxAUDUSD",
        &[
            ("2025-08-10", &[]),
            (
                "2025-08-11",
                &[
                    (ns(DAY_2025_08_11), 0.65165),
                    (ns(DAY_2025_08_11 + 1), 0.65166),
                    (ns(DAY_2025_08_11 + 1), 0.65166),
                    (ns(DAY_2025_08_11 + 86_399), 0.65135),
                ],
            ),
            ("2025-08-12", &[(ns(DAY_2025_08_11 + 86_400), 0.6514)]),
        ],
    );
    write_daily_directory(
        &root.join("USDJPY"),
        "USDJPY",
        "frxUSDJPY",
        &[(
            "2025-08-11",
            &[
                (ns(DAY_2025_08_11), 158.424),
                (ns(DAY_2025_08_11 + 2), 157.8),
            ],
        )],
    );
    fs::create_dir_all(root.join("EURUSD")).unwrap();
    fs::write(root.join("EURUSD/junk.txt"), b"not a daily file").unwrap();
    fs::write(root.join("README"), b"not a directory").unwrap();
}

/// The exact normalized rows of the `AUDUSD` directory: microseconds and units at scale five.
const NORMALIZED_DAILY_TICKS: [(i64, i64); 5] = [
    (1_754_870_400_000_000, 65_165),
    (1_754_870_401_000_000, 65_166),
    (1_754_870_401_000_000, 65_166),
    (1_754_956_799_000_000, 65_135),
    (1_754_956_800_000_000, 65_140),
];

fn standard_sources(scratch: &Scratch) {
    write_ticks(&scratch.path("sources/ticks/ticks.csv"), &TICK_ROWS);
    write_ticks(
        &scratch.path("sources/ticks/mixed.csv"),
        &["2026-03-22T06:02:39.312Z,AEDCNY,9.9"],
    );
    write_collection(
        &scratch.path("sources/bars"),
        &[
            AssetSpec {
                asset: "#AAPL",
                expected_symbol_id: Some(5),
                symbol_id: None,
                files: vec![
                    bars("#AAPL", 5, 1_747_653_300, 3),
                    bars("#AAPL", 5, 1_747_653_400, 2),
                ],
                metadata: true,
            },
            AssetSpec {
                asset: "AEDCNY_otc",
                expected_symbol_id: None,
                symbol_id: Some(538),
                files: vec![bars("AEDCNY_otc", 538, 1_747_653_300, 4)],
                metadata: false,
            },
        ],
    );
}

/// A published scratch tree with both sources imported once.
fn published(name: &str) -> (Scratch, PathBuf, Vec<String>) {
    let scratch = Scratch::new(name);
    standard_sources(&scratch);
    let config = scratch.config(
        "import.toml",
        &format!("{}{}", scratch.tick_source(), scratch.bar_source()),
    );
    let lines = import(&config).unwrap();
    (scratch, config, lines)
}

#[test]
fn import_publishes_retains_and_verifies_from_either_copy() {
    let scratch = Scratch::new("publish");
    standard_sources(&scratch);
    let originals: Vec<(PathBuf, String)> = walk(&scratch.path("sources"))
        .into_iter()
        .map(|path| (path.clone(), sha256(&path)))
        .collect();
    let config = scratch.config(
        "import.toml",
        &format!("{}{}", scratch.tick_source(), scratch.bar_source()),
    );

    let lines = import(&config).unwrap();
    assert_eq!(lines.len(), 3, "{lines:?}");
    assert!(
        lines[0].starts_with("published pocket_option:AEDCNY_otc development generation "),
        "{}",
        lines[0]
    );
    assert!(
        lines[0].contains(" rows 4 objects 2 reused 0 "),
        "{}",
        lines[0]
    );
    assert!(
        lines[1].starts_with("published pocket_option:#AAPL evaluation generation "),
        "{}",
        lines[1]
    );
    assert!(
        lines[1].contains(" rows 5 objects 10 reused 0 "),
        "{}",
        lines[1]
    );
    assert!(
        lines[2].contains("pocket_option:AEDCNY_otc evaluation")
            && lines[2].contains(" rows 4 objects 9 reused 3 "),
        "provenance bytes shared with the first asset are reused: {}",
        lines[2]
    );

    for (path, digest) in &originals {
        assert_eq!(&sha256(path), digest, "{} changed", path.display());
    }
    assert_eq!(scratch.objects("retained"), scratch.objects("published"));
    for name in scratch.objects("published") {
        assert_eq!(
            sha256(&scratch.path("published").join("objects").join(&name)),
            name,
            "content-addressed key"
        );
    }
    let manifests = scratch.manifests("published");
    assert_eq!(manifests.len(), 3);
    for manifest in &manifests {
        let mirror = scratch
            .path("retained")
            .join(manifest.strip_prefix(scratch.path("published")).unwrap());
        assert_eq!(
            fs::read(manifest).unwrap(),
            fs::read(&mirror).unwrap(),
            "byte-for-byte mirror"
        );
        let json = manifest_json(manifest);
        for object in json["objects"].as_array().unwrap() {
            assert!(
                scratch
                    .path("published")
                    .join(object["key"].as_str().unwrap())
                    .is_file()
            );
            assert!(object["crc32c"].is_null() && object["generation"].is_null());
        }
        assert_eq!(json["config_hash"].as_str().unwrap()[..10], *"v3:sha256:");
        let objects = json["objects"].as_array().unwrap();
        assert_eq!(
            verify(manifest).unwrap(),
            format!(
                "verified {} {} generation {} rows {} objects {} bytes {}",
                json["instrument"].as_str().unwrap(),
                json["role"].as_str().unwrap(),
                json["generation"].as_str().unwrap(),
                json["row_count"],
                objects.len(),
                objects
                    .iter()
                    .map(|object| object["bytes"].as_u64().unwrap())
                    .sum::<u64>()
            )
        );
        assert!(verify(&mirror).is_ok());
    }
    let ticks = manifests
        .iter()
        .map(|path| manifest_json(path))
        .find(|json| json["source_kind"] == "tick_csv")
        .unwrap();
    assert_eq!(ticks["capabilities"], json!(["ticks"]));
    assert_eq!(
        ticks["price_representation"],
        json!({"kind": "integer_units", "scale": 6})
    );
    assert_eq!(
        ticks["coverage"],
        json!({"first_event_time": "2026-03-22T06:02:39.312000Z", "last_event_time": "2026-03-22T06:02:40.001000Z"})
    );
    let tick_objects: Vec<&str> = ticks["objects"]
        .as_array()
        .unwrap()
        .iter()
        .map(|object| object["path"].as_str().unwrap())
        .collect();
    assert_eq!(tick_objects, ["ticks.csv", "normalized/ticks.parquet"]);
    assert!(
        !ticks.to_string().contains("mixed.csv"),
        "the undeclared sibling was never inventoried"
    );
    let normalized = scratch
        .path("published")
        .join(ticks["objects"][1]["key"].as_str().unwrap());
    let reader = SerializedFileReader::new(File::open(&normalized).unwrap()).unwrap();
    let pairs: Vec<(i64, i64)> = reader
        .get_row_iter(None)
        .unwrap()
        .map(|row| {
            let row = row.unwrap();
            (
                row.get_timestamp_micros(0).unwrap(),
                row.get_long(1).unwrap(),
            )
        })
        .collect();
    assert_eq!(
        pairs, NORMALIZED_TICKS,
        "exact normalized rows, duplicates kept in order"
    );
    let metadata = reader
        .metadata()
        .file_metadata()
        .key_value_metadata()
        .unwrap();
    assert!(
        metadata
            .iter()
            .any(|pair| pair.key == "price_scale" && pair.value.as_deref() == Some("6"))
    );

    let by_symbol = |symbol: &str| {
        manifests
            .iter()
            .map(|path| manifest_json(path))
            .find(|json| json["provider_symbol"] == symbol && json["source_kind"] == "bar_parquet")
            .unwrap()
    };
    let apple = by_symbol("#AAPL");
    assert_eq!(apple["capabilities"], json!(["bars"]));
    assert_eq!(apple["interval"]["provenance"], "parquet_metadata");
    assert_eq!(
        apple["native_granularity"],
        json!({"kind": "bar", "period_seconds": 5})
    );
    assert_eq!(
        apple["coverage"]["first_event_time"],
        "2025-05-19T11:15:00.000000Z"
    );
    let paths: Vec<&str> = apple["objects"]
        .as_array()
        .unwrap()
        .iter()
        .map(|object| object["path"].as_str().unwrap())
        .collect();
    assert_eq!(
        &paths[..2],
        [
            "dataset/parquet/year=2025/month=05/part-00000.parquet",
            "dataset/parquet/year=2025/month=06/part-00000.parquet"
        ],
        "source objects first, in manifest order"
    );
    for provenance in [
        "raw_pages.ndjson",
        "checkpoint.ndjson",
        "download_manifest.json",
        "dataset/_SUCCESS",
        "collection/collection.json",
    ] {
        assert!(paths.contains(&provenance), "{paths:?}");
    }
    assert_eq!(
        by_symbol("AEDCNY_otc")["interval"]["provenance"],
        "legacy_inferred_from_validated_5s_grid"
    );

    let again = import(&config).unwrap();
    assert!(
        again
            .iter()
            .all(|line| line.ends_with("(already published)")),
        "{again:?}"
    );
    assert!(again[1].contains(" objects 10 reused 10 "), "{}", again[1]);
    let mut expected_keys: Vec<String> = manifests
        .iter()
        .flat_map(|path| {
            manifest_json(path)["objects"]
                .as_array()
                .unwrap()
                .iter()
                .map(|object| object["key"].as_str().unwrap()["objects/".len()..].to_string())
                .collect::<Vec<_>>()
        })
        .collect();
    expected_keys.sort();
    expected_keys.dedup();
    assert_eq!(
        scratch.objects("published"),
        expected_keys,
        "identical provenance bytes share one object"
    );
    assert_eq!(expected_keys.len(), 18);

    // Reusing the committed generations from a new historical-data folder retains every child.
    let elsewhere = scratch.config_with(
        "elsewhere.toml",
        "retained2",
        &format!("{}{}", scratch.tick_source(), scratch.bar_source()),
    );
    let reused = import(&elsewhere).unwrap();
    assert!(
        reused
            .iter()
            .all(|line| line.ends_with("(already published)")),
        "{reused:?}"
    );
    assert_eq!(scratch.objects("retained2"), expected_keys);
    for manifest in scratch.manifests("retained2") {
        assert!(verify(&manifest).is_ok());
    }

    fs::remove_dir_all(scratch.path("sources")).unwrap();
    for manifest in scratch.manifests("retained") {
        assert!(
            verify(&manifest).is_ok(),
            "reconstruction from the retained copy alone"
        );
    }
}

#[test]
fn interrupted_runs_resume_without_duplicates_or_early_ready_state() {
    // Each scenario reproduces one interruption state on a freshly published tree, reruns the
    // command, and requires identical objects and manifests plus successful verification of
    // both copies.
    type Snapshot = (Vec<String>, Vec<(PathBuf, Vec<u8>)>);
    let snapshot = |scratch: &Scratch| -> Snapshot {
        let manifests = scratch
            .manifests("published")
            .into_iter()
            .map(|path| {
                let bytes = fs::read(&path).unwrap();
                (path, bytes)
            })
            .collect();
        (scratch.objects("published"), manifests)
    };
    let mirror_of = |scratch: &Scratch, path: &Path| {
        scratch
            .path("retained")
            .join(path.strip_prefix(scratch.path("published")).unwrap())
    };
    let assert_recovered = |scratch: &Scratch, before: &Snapshot| {
        assert_eq!(
            scratch.objects("published"),
            before.0,
            "no duplicate objects"
        );
        assert_eq!(
            scratch.objects("retained"),
            before.0,
            "every child retained"
        );
        assert_eq!(scratch.manifests("published").len(), before.1.len());
        for (path, bytes) in &before.1 {
            assert_eq!(
                &fs::read(path).unwrap(),
                bytes,
                "identical ready manifest after resumption"
            );
            assert_eq!(
                &fs::read(mirror_of(scratch, path)).unwrap(),
                bytes,
                "restored local mirror"
            );
            assert!(verify(path).is_ok(), "{}", path.display());
            assert!(
                verify(&mirror_of(scratch, path)).is_ok(),
                "{}",
                path.display()
            );
        }
    };
    let remove_manifests = |scratch: &Scratch, before: &Snapshot, mirrors_too: bool| {
        for (path, _) in &before.1 {
            fs::remove_file(path).unwrap();
            if mirrors_too {
                fs::remove_file(mirror_of(scratch, path)).unwrap();
            }
        }
    };
    let manifest_for = |before: &Snapshot, symbol: &str| {
        before
            .1
            .iter()
            .map(|(path, _)| manifest_json(path))
            .find(|json| json["provider_symbol"] == symbol)
            .unwrap()
    };

    // 1. Interrupted at local close: the normalized object was never closed, no ready manifest
    //    exists anywhere, and temporary files are left in the retained object directory.
    let (scratch, config, lines) = published("resume_local_close");
    let before = snapshot(&scratch);
    let ticks = before
        .1
        .iter()
        .map(|(path, _)| manifest_json(path))
        .find(|json| json["source_kind"] == "tick_csv")
        .unwrap();
    let normalized_key = ticks["objects"][1]["key"].as_str().unwrap().to_string();
    fs::remove_file(scratch.path("published").join(&normalized_key)).unwrap();
    fs::remove_file(scratch.path("retained").join(&normalized_key)).unwrap();
    remove_manifests(&scratch, &before, true);
    fs::write(
        scratch.path("retained/objects/.tmp-leftover-1-0"),
        b"partial",
    )
    .unwrap();
    fs::write(
        scratch.path(&format!(
            "retained/objects/.tmp-{}-1-1",
            generation(&lines[0])
        )),
        b"partial normalized",
    )
    .unwrap();
    let again = import(&config).unwrap();
    assert!(
        again[0].contains(" objects 2 reused 1 ["),
        "the normalized object is rebuilt from the retained source: {}",
        again[0]
    );
    assert_recovered(&scratch, &before);

    // 2. Interrupted during upload: one destination object is missing and no ready manifest
    //    exists anywhere.
    let (scratch, config, _) = published("resume_upload");
    let before = snapshot(&scratch);
    let apple = manifest_for(&before, "#AAPL");
    let removed = scratch
        .path("published")
        .join(apple["objects"][0]["key"].as_str().unwrap());
    fs::remove_file(&removed).unwrap();
    remove_manifests(&scratch, &before, true);
    let again = import(&config).unwrap();
    assert!(
        again[1].contains(" objects 10 reused 9 ["),
        "one object re-created: {}",
        again[1]
    );
    assert!(again[2].contains(" objects 9 reused 9 ["), "{}", again[2]);
    assert!(removed.is_file());
    assert_recovered(&scratch, &before);

    // 3. Interrupted after object creation: the first half of a dataset's objects exist at the
    //    destination, the rest do not, and no ready manifest exists anywhere.
    let (scratch, config, _) = published("resume_after_object");
    let before = snapshot(&scratch);
    let apple = manifest_for(&before, "#AAPL");
    let objects = apple["objects"].as_array().unwrap();
    for object in &objects[objects.len() / 2..] {
        let _ = fs::remove_file(
            scratch
                .path("published")
                .join(object["key"].as_str().unwrap()),
        );
    }
    remove_manifests(&scratch, &before, true);
    let again = import(&config).unwrap();
    assert!(again[1].contains(" objects 10 reused "), "{}", again[1]);
    assert_recovered(&scratch, &before);

    // 4. Interrupted before ready publication: every object exists at both copies and no ready
    //    manifest exists anywhere.
    let (scratch, config, _) = published("resume_before_ready");
    let before = snapshot(&scratch);
    remove_manifests(&scratch, &before, true);
    let again = import(&config).unwrap();
    assert!(
        again
            .iter()
            .all(|line| line.contains(" reused ") && line.ends_with("s]")),
        "{again:?}"
    );
    assert_recovered(&scratch, &before);

    // 5. Interrupted after destination ready creation and before the local mirror.
    let (scratch, config, _) = published("resume_mirror");
    let before = snapshot(&scratch);
    remove_manifests(&scratch, &before, false);
    for (path, _) in &before.1 {
        fs::write(path, fs::read(mirror_of(&scratch, path)).unwrap()).unwrap();
        fs::remove_file(mirror_of(&scratch, path)).unwrap();
    }
    let again = import(&config).unwrap();
    assert!(
        again
            .iter()
            .all(|line| line.ends_with("(already published)")),
        "{again:?}"
    );
    assert_recovered(&scratch, &before);

    // Conflicting bytes at an existing key fail closed without replacing either copy, whether
    // or not the committed ready manifest is still present.
    let (scratch, config, _) = published("resume_conflict");
    let before = snapshot(&scratch);
    let apple = manifest_for(&before, "#AAPL");
    let victim_key = apple["objects"][1]["key"].as_str().unwrap();
    let victim = scratch.path("published").join(victim_key);
    let victim_bytes = fs::read(&victim).unwrap();
    fs::write(&victim, b"different bytes").unwrap();
    let error = import(&config).unwrap_err();
    assert!(error.contains("already holds different content"), "{error}");
    assert_eq!(fs::read(&victim).unwrap(), b"different bytes");
    assert_eq!(
        fs::read(scratch.path("retained").join(victim_key)).unwrap(),
        victim_bytes
    );
    remove_manifests(&scratch, &before, false);
    let error = import(&config).unwrap_err();
    assert!(error.contains("already holds different content"), "{error}");
    assert_eq!(fs::read(&victim).unwrap(), b"different bytes");
    assert!(
        scratch.manifests("published").len() < before.1.len(),
        "no early ready state after a conflict"
    );

    // A committed ready manifest that misrecords a child's checksum is never reused or mirrored.
    let (scratch, config, _) = published("resume_bad_checksum");
    let before = snapshot(&scratch);
    let (path, bytes) = &before.1[0];
    let text = String::from_utf8(bytes.clone()).unwrap();
    fs::write(
        path,
        text.replacen("\"crc32c\": null", "\"crc32c\": 12345", 1),
    )
    .unwrap();
    fs::remove_file(mirror_of(&scratch, path)).unwrap();
    let error = import(&config).unwrap_err();
    assert!(error.contains("records a different generation"), "{error}");
    assert!(
        !mirror_of(&scratch, path).exists(),
        "the incorrect manifest was not mirrored"
    );
}

#[test]
fn malformed_inputs_and_unsafe_layouts_are_rejected() {
    let scratch = Scratch::new("reject");
    let tick_cases: [(&str, &[&str], &str); 7] = [
        (
            "mixed instruments",
            &[
                "2026-03-22T06:02:39.312Z,AEDCNY,1.8",
                "2026-03-22T06:02:39.400Z,EURUSD,1.8",
            ],
            "not the declared source symbol",
        ),
        (
            "backwards time",
            &[
                "2026-03-22T06:02:39.312Z,AEDCNY,1.8",
                "2026-03-22T06:02:39.311Z,AEDCNY,1.8",
            ],
            "backwards time",
        ),
        (
            "conflicting duplicate",
            &[
                "2026-03-22T06:02:39.312Z,AEDCNY,1.8",
                "2026-03-22T06:02:39.312Z,AEDCNY,1.9",
            ],
            "conflicting prices",
        ),
        (
            "non-finite price",
            &["2026-03-22T06:02:39.312Z,AEDCNY,nan"],
            "invalid decimal price",
        ),
        (
            "too many fraction digits",
            &["2026-03-22T06:02:39.312Z,AEDCNY,1.1234567"],
            "more than the declared price_scale",
        ),
        (
            "malformed row",
            &["2026-03-22T06:02:39.312Z,AEDCNY"],
            "expected three comma-separated fields",
        ),
        (
            "malformed timestamp",
            &["2026-03-22 06:02:39Z,AEDCNY,1.8"],
            "invalid timestamp",
        ),
    ];
    for (name, rows, message) in tick_cases {
        write_ticks(&scratch.path("sources/ticks/ticks.csv"), rows);
        let error = import(&scratch.config("ticks.toml", &scratch.tick_source())).unwrap_err();
        assert!(error.contains(message), "{name}: {error}");
    }
    fs::write(
        scratch.path("sources/ticks/ticks.csv"),
        "time,symbol,price\n",
    )
    .unwrap();
    assert!(
        import(&scratch.config("ticks.toml", &scratch.tick_source()))
            .unwrap_err()
            .contains("header")
    );

    let rows = bars("#AAPL", 5, 1_747_653_300, 3);
    let bar_cases: Vec<(&str, Vec<BarRow>, &str)> = vec![
        (
            "off-grid",
            vec![bar("#AAPL", 5, 1_747_653_301, [1.0, 1.5, 0.5, 1.2, 0.0])],
            "off the 5-second grid",
        ),
        (
            "high below close",
            vec![bar("#AAPL", 5, 1_747_653_300, [1.0, 1.1, 0.5, 1.2, 0.0])],
            "invalid high/low relationship",
        ),
        (
            "non-finite",
            vec![bar(
                "#AAPL",
                5,
                1_747_653_300,
                [1.0, f64::INFINITY, 0.5, 1.2, 0.0],
            )],
            "non-finite value",
        ),
        (
            "negative volume",
            vec![bar("#AAPL", 5, 1_747_653_300, [1.0, 1.5, 0.5, 1.2, -1.0])],
            "negative volume",
        ),
        (
            "mixed instruments",
            vec![
                rows[0].clone(),
                bar("#MSFT", 5, 1_747_653_305, [1.0, 1.5, 0.5, 1.2, 0.0]),
            ],
            "carries symbol `#MSFT`",
        ),
        (
            "mixed identifiers",
            vec![
                rows[0].clone(),
                bar("#AAPL", 6, 1_747_653_305, [1.0, 1.5, 0.5, 1.2, 0.0]),
            ],
            "symbol identifier 6",
        ),
        (
            "backwards time",
            vec![rows[1].clone(), rows[0].clone()],
            "does not follow",
        ),
        (
            "duplicate timestamp",
            vec![rows[0].clone(), rows[0].clone()],
            "does not follow",
        ),
        (
            "wrong period",
            vec![BarRow {
                period: 60,
                ..rows[0].clone()
            }],
            "period 60 seconds",
        ),
        (
            "period overflows its logical unsigned width",
            vec![BarRow {
                period: 65_541,
                ..rows[0].clone()
            }],
            "not an unsigned 16-bit value",
        ),
        (
            "timestamp drift",
            vec![BarRow {
                timestamp: Some(1),
                ..rows[0].clone()
            }],
            "does not equal Unix seconds",
        ),
        (
            "server offset drift",
            vec![BarRow {
                server: Some(1_747_653_300),
                ..rows[0].clone()
            }],
            "not the recorded offset",
        ),
    ];
    let apple = |files: Vec<Vec<BarRow>>| AssetSpec {
        asset: "#AAPL",
        expected_symbol_id: Some(5),
        symbol_id: None,
        files,
        metadata: true,
    };
    for (name, file_rows, message) in bar_cases {
        let _ = fs::remove_dir_all(scratch.path("sources/bars"));
        write_collection(&scratch.path("sources/bars"), &[apple(vec![file_rows])]);
        let error = import(&scratch.config("bars.toml", &scratch.bar_source())).unwrap_err();
        assert!(error.contains(message), "{name}: {error}");
    }

    // Schema drift, interval-contract problems, and manifest tampering.
    let rewrite = |name: &str, change: &dyn Fn(&Path, &Path)| -> String {
        let _ = fs::remove_dir_all(scratch.path("sources/bars"));
        let manifest =
            write_collection(&scratch.path("sources/bars"), &[apple(vec![rows.clone()])]);
        let file = scratch
            .path("sources/bars/#AAPL/dataset/parquet/year=2025/month=05/part-00000.parquet");
        change(&file, &manifest);
        let error = import(&scratch.config("bars.toml", &scratch.bar_source())).unwrap_err();
        assert!(!error.is_empty(), "{name}");
        error
    };
    let relist = |file: &Path, manifest: &Path| {
        let mut json: Value = serde_json::from_slice(&fs::read(manifest).unwrap()).unwrap();
        let entry = &mut json["assets"]["#AAPL"]["parquet_files"][0];
        entry["bytes"] = json!(fs::metadata(file).unwrap().len());
        entry["sha256"] = json!(sha256(file));
        fs::write(manifest, serde_json::to_vec(&json).unwrap()).unwrap();
    };
    let edit_manifest = |manifest: &Path, from: &str, to: &str| {
        let text = fs::read_to_string(manifest).unwrap();
        assert!(text.contains(from), "{from}");
        fs::write(manifest, text.replace(from, to)).unwrap();
    };
    assert!(
        rewrite("schema drift", &|file, manifest| {
            write_bar_file(
                file,
                &rows,
                Some(&INTERVAL),
                &BAR_SCHEMA.replace(
                    "OPTIONAL DOUBLE volume;",
                    "OPTIONAL DOUBLE volume;\n  OPTIONAL INT32 extra;",
                ),
            );
            relist(file, manifest);
        })
        .contains("unexpected schema")
    );
    assert!(
        rewrite("missing metadata", &|file, manifest| {
            write_bar_file(file, &rows, None, BAR_SCHEMA);
            relist(file, manifest);
        })
        .contains("lacks the interval metadata")
    );
    assert!(
        rewrite("contradictory metadata", &|file, manifest| {
            let mut wrong = INTERVAL;
            wrong[1] = ("frequency", "10s");
            write_bar_file(file, &rows, Some(&wrong), BAR_SCHEMA);
            relist(file, manifest);
        })
        .contains("contradicts the declared")
    );
    assert!(
        rewrite("unapproved interval contract", &|_, manifest| {
            edit_manifest(manifest, "\"frequency\": \"5s\"", "\"frequency\": \"10s\"");
        })
        .contains("interval contract `frequency` is `10s`")
    );
    assert!(
        rewrite("missing symbol identifier", &|_, manifest| {
            edit_manifest(
                manifest,
                "\"expected_symbol_id\": 5",
                "\"expected_symbol_id\": null",
            );
        })
        .contains("records no symbol identifier")
    );
    assert!(
        rewrite("hash mismatch", &|file, _| {
            fs::write(file, b"not parquet").unwrap();
        })
        .contains("do not match the listed")
    );
    assert!(
        rewrite("unsafe relative path", &|_, manifest| {
            edit_manifest(manifest, "parquet/year=2025", "../parquet/year=2025");
        })
        .contains("must stay inside its root")
    );
    assert!(
        rewrite("row count mismatch", &|_, manifest| {
            edit_manifest(manifest, "\"rows\": 3", "\"rows\": 4");
        })
        .contains("rows, listed 4")
    );
    assert!(
        rewrite("symbolic link in the asset root", &|file, _| {
            std::os::unix::fs::symlink(file, file.parent().unwrap().join("link.parquet")).unwrap();
        })
        .contains("symbolic link")
    );
    assert!(
        rewrite(
            "collection manifest resolving outside the root",
            &|_, manifest| {
                let outside = scratch.path("outside.json");
                fs::rename(manifest, &outside).unwrap();
                std::os::unix::fs::symlink(&outside, manifest).unwrap();
            }
        )
        .contains("outside the collection root")
    );
    assert!(
        rewrite("control character in a file name", &|file, _| {
            fs::write(file.parent().unwrap().join("bad\nname"), b"x").unwrap();
        })
        .contains("control character")
    );

    // Containment: sources never lie inside a destination, and destinations never lie inside a
    // listed asset root; a destination beside the asset roots is allowed.
    let _ = fs::remove_dir_all(scratch.path("sources/bars"));
    write_collection(&scratch.path("sources/bars"), &[apple(vec![rows.clone()])]);
    let inside_asset = scratch.config_with(
        "inside.toml",
        "sources/bars/#AAPL/retained",
        &scratch.bar_source(),
    );
    assert!(
        import(&inside_asset)
            .unwrap_err()
            .contains("overlaps the historical-data folder")
    );
    assert!(
        !scratch.path("sources/bars/#AAPL/retained").exists(),
        "nothing was created before the check"
    );
    write_ticks(&scratch.path("retained/inner/ticks.csv"), &TICK_ROWS);
    let source_inside = scratch.config(
        "source_inside.toml",
        &scratch
            .tick_source()
            .replace("sources/ticks/ticks.csv", "retained/inner/ticks.csv"),
    );
    assert!(
        import(&source_inside)
            .unwrap_err()
            .contains("lies inside the historical-data folder")
    );
    let beside = scratch.config_with(
        "beside.toml",
        "sources/bars/retained",
        &scratch.bar_source(),
    );
    assert!(
        import(&beside).is_ok(),
        "a destination beside the asset roots is inside no listed inventory"
    );
    let inside_destination = scratch.config_with(
        "inside_destination.toml",
        "sources/bars/retained",
        &scratch
            .bar_source()
            .replace("collection.json", "retained/collection.json"),
    );
    fs::create_dir_all(scratch.path("sources/bars/retained")).unwrap();
    fs::copy(
        scratch.path("sources/bars/collection.json"),
        scratch.path("sources/bars/retained/collection.json"),
    )
    .unwrap();
    assert!(
        import(&inside_destination)
            .unwrap_err()
            .contains("inside the historical-data folder"),
        "a collection manifest inside a destination is never an input"
    );
    let twice = scratch.config(
        "twice.toml",
        &format!(
            "{}{}",
            scratch.bar_source(),
            scratch.bar_source().replace(
                "\"sources/bars\"",
                &format!("\"{}\"", scratch.path("sources/bars").display())
            )
        ),
    );
    assert!(import(&twice).unwrap_err().contains("declared twice"));
    assert!(
        import(&scratch.config("none.toml", ""))
            .unwrap_err()
            .contains("at least one entry")
    );
}

#[test]
fn daily_tick_archives_publish_one_generation_per_listed_directory() {
    let scratch = Scratch::new("daily");
    daily_sources(&scratch);
    let config = scratch.config("daily.toml", &scratch.daily_source());

    let lines = import(&config).unwrap();
    assert_eq!(lines.len(), 2, "{lines:?}");
    assert!(
        lines[0].starts_with("published deriv:frxAUDUSD development generation ")
            && lines[0].contains(" rows 5 objects 5 reused 0 ["),
        "{}",
        lines[0]
    );
    assert!(
        lines[1].starts_with("published deriv:frxUSDJPY development generation ")
            && lines[1].contains(" rows 2 objects 3 reused 0 ["),
        "{}",
        lines[1]
    );
    let manifests = scratch.manifests("published");
    assert_eq!(manifests.len(), 2);
    let audusd = manifests
        .iter()
        .find(|path| manifest_json(path)["provider_symbol"] == "frxAUDUSD")
        .unwrap();
    let json = manifest_json(audusd);
    assert_eq!(json["source_kind"], "tick_parquet_daily");
    assert_eq!(json["instrument"], "deriv:frxAUDUSD");
    assert_eq!(json["capabilities"], json!(["ticks"]));
    assert_eq!(json["native_granularity"], json!({"kind": "tick"}));
    assert_eq!(json["time_unit"], "microsecond");
    assert_eq!(
        json["price_representation"],
        json!({"kind": "integer_units", "scale": 5})
    );
    assert!(json["interval"].is_null());
    assert_eq!(json["row_count"], 5);
    assert_eq!(
        json["coverage"],
        json!({"first_event_time": "2025-08-11T00:00:00.000000Z", "last_event_time": "2025-08-12T00:00:00.000000Z"})
    );
    let objects: Vec<(&str, &str)> = json["objects"]
        .as_array()
        .unwrap()
        .iter()
        .map(|object| {
            (
                object["role"].as_str().unwrap(),
                object["path"].as_str().unwrap(),
            )
        })
        .collect();
    assert_eq!(
        objects,
        [
            ("normalized", "observations/2025-08-10.parquet"),
            ("normalized", "observations/2025-08-11.parquet"),
            ("normalized", "observations/2025-08-12.parquet"),
            ("provenance", "provenance/coverage.json"),
            ("provenance", "provenance/lineage.json"),
        ],
        "daily observations and embedded provenance"
    );
    assert_eq!(json["layout"], "daily-v2");
    let inputs = json["inputs"].as_array().unwrap();
    assert_eq!(inputs.len(), 5);
    let first = scratch.path("sources/deriv/AUDUSD/AUDUSD_2025-08-11_ticks.parquet");
    assert_eq!(inputs[0]["path"], first.to_str().unwrap());
    assert_eq!(inputs[0]["sha256"], sha256(&first));
    let text = fs::read_to_string(audusd).unwrap();
    assert!(
        !text.contains("EURUSD") && !text.contains("README"),
        "unlisted root entries are never inventoried"
    );
    let manifest =
        binary_alpha_engine::dataset::GenerationManifest::from_json(&fs::read(audusd).unwrap())
            .unwrap();
    let pairs: Vec<_> = common::read_normalized_ticks(&scratch.path("published"), &manifest)
        .into_iter()
        .map(|t| (t.event_time_micros, t.price_units))
        .collect();
    let normalized = scratch.path("published").join(
        &manifest
            .objects
            .iter()
            .find(|o| o.path == "observations/2025-08-11.parquet")
            .unwrap()
            .key,
    );
    let reader = SerializedFileReader::new(File::open(&normalized).unwrap()).unwrap();
    assert_eq!(
        pairs, NORMALIZED_DAILY_TICKS,
        "exact rows across days, duplicates kept in order"
    );
    let metadata = reader
        .metadata()
        .file_metadata()
        .key_value_metadata()
        .unwrap();
    for (key, value) in [
        ("broker", "deriv"),
        ("provider_symbol", "frxAUDUSD"),
        ("price_scale", "5"),
    ] {
        assert!(
            metadata
                .iter()
                .any(|pair| pair.key == key && pair.value.as_deref() == Some(value)),
            "{key}"
        );
    }

    let again = import(&config).unwrap();
    assert_eq!(again.len(), lines.len(), "{again:?}");
    assert!(
        again
            .iter()
            .all(|line| line.ends_with("(already published)")),
        "{again:?}"
    );
    // Both copies verify from the manifest and objects alone.
    fs::remove_dir_all(scratch.path("sources")).unwrap();
    for manifest in &manifests {
        verify(manifest).unwrap();
        let mirror = scratch
            .path("retained")
            .join(manifest.strip_prefix(scratch.path("published")).unwrap());
        verify(&mirror).unwrap();
    }
}

#[test]
fn daily_tick_archives_reject_malformed_days_and_layouts() {
    let scratch = Scratch::new("daily_reject");
    let config = scratch.config("daily.toml", &scratch.daily_source());
    // Each case rebuilds the archive, applies one change, and expects the import to stop before
    // any ready manifest exists.
    type Change<'a> = &'a dyn Fn(&Path);
    let rejected = |name: &str, change: Change| -> String {
        let _ = fs::remove_dir_all(scratch.path("sources"));
        daily_sources(&scratch);
        change(&scratch.path("sources/deriv"));
        let error = import(&config).unwrap_err();
        assert!(
            !scratch.path("published/manifests").exists(),
            "{name}: nothing was published: {error}"
        );
        error
    };
    let day = |root: &Path, date: &str| root.join(format!("AUDUSD/AUDUSD_{date}_ticks"));
    let parquet =
        |root: &Path, date: &str| PathBuf::from(format!("{}.parquet", day(root, date).display()));
    let metadata =
        |root: &Path, date: &str| PathBuf::from(format!("{}.meta.json", day(root, date).display()));
    let next_day = ns(DAY_2025_08_11 + 86_400);
    type Rows<'a> = &'a [(i64, f64)];
    let row_cases: [(&str, Rows, &str); 6] = [
        (
            "more fraction digits than the scale",
            &[(next_day, 0.651_401)],
            "more than the declared price_scale",
        ),
        (
            "non-finite price",
            &[(next_day, f64::NAN)],
            "invalid decimal price",
        ),
        (
            "sub-microsecond timestamp",
            &[(next_day + 1, 0.6514)],
            "not on a microsecond boundary",
        ),
        (
            "row outside its calendar day",
            &[(next_day - ns(1), 0.6514)],
            "outside the file's calendar day",
        ),
        (
            "backwards time within a daily file",
            &[(next_day + ns(1), 0.6514), (next_day, 0.6514)],
            "backwards time",
        ),
        (
            "conflicting duplicate timestamps",
            &[(next_day, 0.6514), (next_day, 0.6515)],
            "conflicting prices",
        ),
    ];
    for (name, rows, message) in row_cases {
        let error = rejected(name, &|root| {
            write_daily_file(&parquet(root, "2025-08-12"), rows, None);
            fs::write(
                metadata(root, "2025-08-12"),
                daily_metadata("frxAUDUSD", "2025-08-12", rows.len() as u64),
            )
            .unwrap();
        });
        assert!(error.contains(message), "{name}: {error}");
        assert!(
            error.starts_with("AUDUSD_2025-08-12_ticks.parquet: ")
                || error.starts_with("deriv:frxAUDUSD: "),
            "a rejection names the day file or the instrument: {error}"
        );
    }
    let metadata_cases: [(&str, &str, &str, &str); 5] = [
        (
            "tick count differing from the rows",
            "\"ticks\":1",
            "\"ticks\":2",
            "metadata records 2 ticks",
        ),
        (
            "date differing from the file name",
            "\"date\":\"2025-08-12\"",
            "\"date\":\"2025-08-13\"",
            "expected `UTC` and `2025-08-12`",
        ),
        (
            "symbol differing within the directory",
            "\"symbol\":\"frxAUDUSD\"",
            "\"symbol\":\"frxEURUSD\"",
            "earlier days record `frxAUDUSD`",
        ),
        (
            "calendar other than UTC",
            "\"calendar\":\"UTC\"",
            "\"calendar\":\"Europe/London\"",
            "expected `UTC` and `2025-08-12`",
        ),
        (
            "negative tick count",
            "\"ticks\":1",
            "\"ticks\":-1",
            "is not a daily metadata file",
        ),
    ];
    for (name, from, to, message) in metadata_cases {
        let error = rejected(name, &|root| {
            let path = metadata(root, "2025-08-12");
            let text = fs::read_to_string(&path).unwrap();
            assert!(text.contains(from), "{name}: {text}");
            fs::write(&path, text.replace(from, to)).unwrap();
        });
        assert!(error.contains(message), "{name}: {error}");
    }
    let layout_cases: [(&str, Change, &str); 9] = [
        (
            "null cell",
            &|root| write_daily_file(&parquet(root, "2025-08-12"), &[(next_day, 0.6514)], Some(0)),
            "null value",
        ),
        (
            "metadata-only day recording ticks",
            &|root| {
                fs::write(
                    metadata(root, "2025-08-10"),
                    daily_metadata("frxAUDUSD", "2025-08-10", 3),
                )
                .unwrap();
            },
            "records 3 ticks but the day has no Parquet file",
        ),
        (
            "Parquet day without metadata",
            &|root| fs::remove_file(metadata(root, "2025-08-12")).unwrap(),
            "has no metadata file",
        ),
        (
            "extra file",
            &|root| fs::write(root.join("AUDUSD/notes.txt"), b"x").unwrap(),
            "is not `AUDUSD_YYYY-MM-DD_ticks.parquet`",
        ),
        (
            "invalid calendar date",
            &|root| {
                fs::write(
                    metadata(root, "2025-02-30"),
                    daily_metadata("frxAUDUSD", "2025-02-30", 0),
                )
                .unwrap();
            },
            "`2025-02-30` is not a calendar date",
        ),
        (
            "nested directory",
            &|root| fs::create_dir(root.join("AUDUSD/nested")).unwrap(),
            "is not a regular file",
        ),
        (
            "symbolic link",
            &|root| {
                std::os::unix::fs::symlink(
                    parquet(root, "2025-08-11"),
                    parquet(root, "2025-08-13"),
                )
                .unwrap();
            },
            "symbolic link",
        ),
        (
            "listed directory without a Parquet file",
            &|root| {
                fs::remove_file(root.join("USDJPY/USDJPY_2025-08-11_ticks.parquet")).unwrap();
                fs::write(
                    root.join("USDJPY/USDJPY_2025-08-11_ticks.meta.json"),
                    daily_metadata("frxUSDJPY", "2025-08-11", 0),
                )
                .unwrap();
            },
            "holds no daily Parquet file",
        ),
        (
            "listed directory resolving outside the root",
            &|root| {
                let outside = root.parent().unwrap().join("elsewhere");
                fs::rename(root.join("AUDUSD"), &outside).unwrap();
                std::os::unix::fs::symlink(&outside, root.join("AUDUSD")).unwrap();
            },
            "outside the root",
        ),
    ];
    for (name, change, message) in layout_cases {
        let error = rejected(name, change);
        assert!(error.contains(message), "{name}: {error}");
    }

    // A destination inside a listed directory, and a listed directory that does not exist.
    let _ = fs::remove_dir_all(scratch.path("sources"));
    daily_sources(&scratch);
    let inside = scratch.config_with(
        "inside.toml",
        "sources/deriv/AUDUSD/retained",
        &scratch.daily_source(),
    );
    assert!(
        import(&inside)
            .unwrap_err()
            .contains("overlaps the historical-data folder")
    );
    assert!(
        !scratch.path("sources/deriv/AUDUSD/retained").exists(),
        "nothing was created before the check"
    );
    let missing = scratch.config(
        "missing.toml",
        &scratch
            .daily_source()
            .replace("\"USDJPY\"", "\"USDJPY\", \"GBPUSD\""),
    );
    assert!(import(&missing).unwrap_err().contains("cannot resolve"));
}

#[test]
fn verify_rejects_incomplete_or_tampered_generations() {
    let scratch = Scratch::new("verify");
    write_ticks(&scratch.path("sources/ticks/ticks.csv"), &TICK_ROWS);
    let config = scratch.config("import.toml", &scratch.tick_source());
    import(&config).unwrap();
    let manifest = scratch.manifests("published").remove(0);
    let json = manifest_json(&manifest);
    let objects = json["objects"].as_array().unwrap();

    // A ready manifest whose child is missing was published before all children verified.
    let normalized = scratch
        .path("published")
        .join(objects[1]["key"].as_str().unwrap());
    let bytes = fs::read(&normalized).unwrap();
    fs::remove_file(&normalized).unwrap();
    assert!(verify(&manifest).unwrap_err().contains("is missing"));
    fs::write(&normalized, b"tampered").unwrap();
    assert!(
        verify(&manifest)
            .unwrap_err()
            .contains("does not match the recorded size")
    );
    fs::write(&normalized, &bytes).unwrap();
    assert!(verify(&manifest).is_ok());

    // A manifest that lies about its rows, its objects, or its own generation.
    let text = fs::read_to_string(&manifest).unwrap();
    fs::write(
        &manifest,
        text.replace("\"row_count\": 4", "\"row_count\": 5"),
    )
    .unwrap();
    assert!(
        verify(&manifest)
            .unwrap_err()
            .contains("reconstructed 4 rows")
    );
    fs::write(
        &manifest,
        text.replace("\"key\": \"objects/", "\"key\": \"../objects/"),
    )
    .unwrap();
    assert!(verify(&manifest).unwrap_err().contains("content-addressed"));
    fs::write(&manifest, &text).unwrap();
    let moved = scratch.path(&format!(
        "published/manifests/{}/ready.json",
        "0".repeat(64)
    ));
    fs::create_dir_all(moved.parent().unwrap()).unwrap();
    fs::write(&moved, &text).unwrap();
    assert!(
        verify(&moved)
            .unwrap_err()
            .contains("holds the manifest of generation")
    );
    assert!(
        verify(&scratch.path("published/manifests/0000/ready.json"))
            .unwrap_err()
            .contains("must end with")
    );
    assert!(
        verify(&scratch.path("published/objects/x"))
            .unwrap_err()
            .contains("must end with")
    );

    // A mirrored manifest carrying Google metadata verifies locally without a cloud request
    // when its checksums match the bytes; a wrong checksum is rejected.
    let source_crc = crc32c(
        &scratch
            .path("published")
            .join(objects[0]["key"].as_str().unwrap()),
    );
    let normalized_crc = crc32c(&normalized);
    let cloud_like = text
        .replacen("\"crc32c\": null", &format!("\"crc32c\": {source_crc}"), 1)
        .replacen(
            "\"crc32c\": null",
            &format!("\"crc32c\": {normalized_crc}"),
            1,
        )
        .replace("\"generation\": null", "\"generation\": 1757000000000000");
    assert!(
        !cloud_like.contains("\"crc32c\": null") && !cloud_like.contains("\"generation\": null")
    );
    fs::write(&manifest, &cloud_like).unwrap();
    assert!(
        verify(&manifest).is_ok(),
        "recorded Google generations are provenance for file:// reads"
    );
    fs::write(
        &manifest,
        cloud_like.replacen(&format!("\"crc32c\": {source_crc}"), "\"crc32c\": 12345", 1),
    )
    .unwrap();
    assert!(verify(&manifest).unwrap_err().contains("CRC32C"));

    // A bar manifest whose interval contract was altered after publication is rejected before
    // any object is trusted, even though the generation identity is unchanged.
    let scratch = Scratch::new("verify_interval");
    write_collection(
        &scratch.path("sources/bars"),
        &[AssetSpec {
            asset: "AEDCNY_otc",
            expected_symbol_id: None,
            symbol_id: Some(538),
            files: vec![bars("AEDCNY_otc", 538, 1_747_653_300, 4)],
            metadata: false,
        }],
    );
    import(&scratch.config("bars.toml", &scratch.bar_source())).unwrap();
    let manifest = scratch.manifests("published").remove(0);
    let text = fs::read_to_string(&manifest).unwrap();
    fs::write(
        &manifest,
        text.replace("\"closed\": \"left\"", "\"closed\": \"right\""),
    )
    .unwrap();
    assert!(
        verify(&manifest)
            .unwrap_err()
            .contains("interval contract `closed`")
    );
}

fn walk(root: &Path) -> Vec<PathBuf> {
    let mut files = Vec::new();
    for entry in fs::read_dir(root).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() {
            files.extend(walk(&path));
        } else {
            files.push(path);
        }
    }
    files.sort();
    files
}

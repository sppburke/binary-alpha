//! Exercises `binary-alpha data import` and `binary-alpha data verify` end to end against
//! synthetic tick and bar sources, the retained folder, and a `file://` destination.

use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::sync::Arc;

use parquet::basic::{Compression, ZstdLevel};
use parquet::column::writer::ColumnWriter;
use parquet::data_type::ByteArray;
use parquet::file::metadata::KeyValue;
use parquet::file::properties::WriterProperties;
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::file::writer::SerializedFileWriter;
use parquet::record::RowAccessor;
use parquet::schema::parser::parse_message_type;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

const BAR_SCHEMA: &str = "message duckdb_schema {
  OPTIONAL BYTE_ARRAY symbol (UTF8);
  OPTIONAL INT32 symbol_id (INT_32);
  OPTIONAL INT64 timestamp_utc (TIMESTAMP(MICROS,true));
  OPTIONAL INT64 unix_utc_s (INT_64);
  OPTIONAL INT64 server_time_s (INT_64);
  OPTIONAL DOUBLE open;
  OPTIONAL DOUBLE high;
  OPTIONAL DOUBLE low;
  OPTIONAL DOUBLE close;
  OPTIONAL DOUBLE volume;
  OPTIONAL INT32 period_s (UINT_16);
}";

const INTERVAL: [(&str, &str); 7] = [
    ("closed", "left"),
    ("frequency", "5s"),
    ("interval", "[timestamp,timestamp+5s)"),
    ("label", "left"),
    ("offset_seconds", "0"),
    ("origin", "unix_epoch_utc"),
    ("timestamp_semantics", "bar_start"),
];

const OFFSET: i64 = 7200;

/// One synthetic archive row; `server` defaults to `unix + OFFSET`.
#[derive(Clone)]
struct BarRow {
    symbol: String,
    symbol_id: i32,
    unix: i64,
    server: Option<i64>,
    timestamp: Option<i64>,
    ohlcv: [f64; 5],
    period: i32,
}

fn bar(symbol: &str, symbol_id: i32, unix: i64, ohlcv: [f64; 5]) -> BarRow {
    BarRow {
        symbol: symbol.to_string(),
        symbol_id,
        unix,
        server: None,
        timestamp: None,
        ohlcv,
        period: 5,
    }
}

fn bars(symbol: &str, symbol_id: i32, start: i64, count: i64) -> Vec<BarRow> {
    (0..count)
        .map(|index| {
            bar(
                symbol,
                symbol_id,
                start + index * 5,
                [1.0, 1.5, 0.5, 1.2 + index as f64 * 0.001, index as f64],
            )
        })
        .collect()
}

fn write_bar_file(path: &Path, rows: &[BarRow], metadata: Option<&[(&str, &str)]>, schema: &str) {
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    let schema = Arc::new(parse_message_type(schema).unwrap());
    let mut builder = WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::try_new(1).unwrap()));
    if let Some(pairs) = metadata {
        builder = builder.set_key_value_metadata(Some(
            pairs
                .iter()
                .map(|(key, value)| KeyValue::new(key.to_string(), value.to_string()))
                .collect(),
        ));
    }
    let mut writer = SerializedFileWriter::new(
        File::create(path).unwrap(),
        schema,
        Arc::new(builder.build()),
    )
    .unwrap();
    let mut group = writer.next_row_group().unwrap();
    let levels = vec![1i16; rows.len()];
    let mut index = 0;
    while let Some(mut column) = group.next_column().unwrap() {
        match column.untyped() {
            ColumnWriter::ByteArrayColumnWriter(typed) => {
                let values: Vec<ByteArray> = rows
                    .iter()
                    .map(|row| ByteArray::from(row.symbol.as_str()))
                    .collect();
                typed.write_batch(&values, Some(&levels), None).unwrap();
            }
            ColumnWriter::Int32ColumnWriter(typed) => {
                let values: Vec<i32> = rows
                    .iter()
                    .map(|row| {
                        if index == 1 {
                            row.symbol_id
                        } else {
                            row.period
                        }
                    })
                    .collect();
                typed.write_batch(&values, Some(&levels), None).unwrap();
            }
            ColumnWriter::Int64ColumnWriter(typed) => {
                let values: Vec<i64> = rows
                    .iter()
                    .map(|row| match index {
                        2 => row.timestamp.unwrap_or(row.unix * 1_000_000),
                        3 => row.unix,
                        _ => row.server.unwrap_or(row.unix + OFFSET),
                    })
                    .collect();
                typed.write_batch(&values, Some(&levels), None).unwrap();
            }
            ColumnWriter::DoubleColumnWriter(typed) => {
                let values: Vec<f64> = rows.iter().map(|row| row.ohlcv[index - 5]).collect();
                typed.write_batch(&values, Some(&levels), None).unwrap();
            }
            _ => unreachable!("the synthetic schema has no other column type"),
        }
        column.close().unwrap();
        index += 1;
    }
    group.close().unwrap();
    writer.close().unwrap();
}

fn sha256(path: &Path) -> String {
    let bytes = fs::read(path).unwrap();
    Sha256::digest(&bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

/// CRC32C of a file, computed independently of the application.
fn crc32c(path: &Path) -> u32 {
    let mut crc = !0u32;
    for byte in fs::read(path).unwrap() {
        crc ^= u32::from(byte);
        for _ in 0..8 {
            crc = if crc & 1 == 1 {
                0x82F6_3B78 ^ (crc >> 1)
            } else {
                crc >> 1
            };
        }
    }
    !crc
}

/// One synthetic asset root with the observed layout.
struct AssetSpec {
    asset: &'static str,
    expected_symbol_id: Option<i32>,
    symbol_id: Option<i32>,
    files: Vec<Vec<BarRow>>,
    metadata: bool,
}

fn write_collection(root: &Path, assets: &[AssetSpec]) -> PathBuf {
    let mut entries = serde_json::Map::new();
    for spec in assets {
        let asset_root = root.join(spec.asset);
        let dataset_root = asset_root.join("dataset");
        fs::create_dir_all(dataset_root.join("reports")).unwrap();
        fs::write(
            asset_root.join("download_manifest.json"),
            format!("{{\"asset\": \"{}\"}}\n", spec.asset),
        )
        .unwrap();
        fs::write(asset_root.join("checkpoint.ndjson"), "{\"page\": 1}\n").unwrap();
        fs::write(
            asset_root.join("raw_pages.ndjson"),
            format!("{{\"asset\": \"{}\", \"data\": []}}\n", spec.asset),
        )
        .unwrap();
        fs::write(dataset_root.join("_SUCCESS"), "").unwrap();
        fs::write(dataset_root.join("reports/quality.json"), "{}\n").unwrap();
        let mut listed = Vec::new();
        let mut hashes = String::new();
        let mut rows = 0;
        for (index, file_rows) in spec.files.iter().enumerate() {
            let relative = format!(
                "parquet/year=2025/month={:02}/part-00000.parquet",
                index + 5
            );
            let path = dataset_root.join(&relative);
            write_bar_file(
                &path,
                file_rows,
                spec.metadata.then_some(&INTERVAL[..]),
                BAR_SCHEMA,
            );
            let digest = sha256(&path);
            hashes.push_str(&format!("{digest}  {relative}\n"));
            rows += file_rows.len();
            listed.push(json!({"path": relative, "bytes": fs::metadata(&path).unwrap().len(), "rows": file_rows.len(), "sha256": digest}));
        }
        fs::write(dataset_root.join("hashes.sha256"), hashes).unwrap();
        fs::write(
            dataset_root.join("manifest.json"),
            format!(
                "{{\"asset\": \"{}\", \"canonical_rows\": {rows}}}\n",
                spec.asset
            ),
        )
        .unwrap();
        let interval: serde_json::Map<String, Value> = INTERVAL
            .iter()
            .map(|(key, value)| {
                (
                    key.to_string(),
                    if *key == "offset_seconds" {
                        json!(0)
                    } else {
                        json!(value)
                    },
                )
            })
            .collect();
        entries.insert(
            spec.asset.to_string(),
            json!({
                "asset": spec.asset,
                "asset_root": asset_root,
                "dataset_root": dataset_root,
                "expected_symbol_id": spec.expected_symbol_id,
                "symbol_id": spec.symbol_id,
                "canonical_rows": rows,
                "parquet_files": listed,
                "interval_contract": interval,
                "interval_contract_provenance": if spec.metadata { "manifest_and_parquet_metadata" } else { "legacy_inferred_from_validated_5s_grid" },
            }),
        );
    }
    let manifest = root.join("collection.json");
    fs::write(
        &manifest,
        serde_json::to_string_pretty(&json!({"assets": entries, "server_timestamp_offset_seconds": OFFSET, "status": "complete"})).unwrap(),
    )
    .unwrap();
    manifest
}

fn write_ticks(path: &Path, rows: &[&str]) {
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    let mut text = String::from("time_utc,symbol,price\n");
    for row in rows {
        text.push_str(row);
        text.push('\n');
    }
    fs::write(path, text).unwrap();
}

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

/// A fresh scratch tree for one test.
struct Scratch {
    root: PathBuf,
}

impl Scratch {
    fn new(name: &str) -> Self {
        let root = PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join(format!("data_import_{name}"));
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(&root).unwrap();
        Self { root }
    }

    fn path(&self, relative: &str) -> PathBuf {
        self.root.join(relative)
    }

    /// Writes a research configuration publishing to `published` and retaining in `retained`.
    fn config(&self, name: &str, sources: &str) -> PathBuf {
        self.config_with(name, "retained", sources)
    }

    fn config_with(&self, name: &str, retained: &str, sources: &str) -> PathBuf {
        let path = self.path(name);
        fs::write(
            &path,
            format!(
                "schema_version = 1\nrun_mode = \"research\"\n\n[storage]\nhistorical_data_dir = \"{retained}\"\npublication_uri = \"file://{}\"\n{sources}",
                self.path("published").display()
            ),
        )
        .unwrap();
        path
    }

    fn tick_source(&self) -> String {
        "\n[[import.sources]]\nkind = \"tick_csv\"\npath = \"sources/ticks/ticks.csv\"\nbroker = \"pocket_option\"\nrole = \"development\"\nprovider_symbol = \"AEDCNY_otc\"\nsource_symbol = \"AEDCNY\"\nprice_scale = 6\n".to_string()
    }

    fn bar_source(&self) -> String {
        "\n[[import.sources]]\nkind = \"bar_parquet_collection\"\npath = \"sources/bars\"\nbroker = \"pocket_option\"\nrole = \"evaluation\"\nmanifest = \"collection.json\"\n".to_string()
    }

    fn objects(&self, store: &str) -> Vec<String> {
        let mut names: Vec<String> = fs::read_dir(self.path(store).join("objects"))
            .unwrap()
            .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
            .filter(|name| !name.starts_with('.'))
            .collect();
        names.sort();
        names
    }

    fn manifests(&self, store: &str) -> Vec<PathBuf> {
        let mut paths: Vec<PathBuf> = fs::read_dir(self.path(store).join("manifests"))
            .unwrap()
            .map(|entry| entry.unwrap().path().join("ready.json"))
            .filter(|path| path.is_file())
            .collect();
        paths.sort();
        paths
    }
}

fn binary_alpha(args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_binary-alpha"))
        .args(args)
        .output()
        .expect("binary-alpha runs")
}

fn import(config: &Path) -> Result<Vec<String>, String> {
    let output = binary_alpha(&["data", "import", "--config", config.to_str().unwrap()]);
    let stdout = String::from_utf8(output.stdout).unwrap();
    let stderr = String::from_utf8(output.stderr).unwrap();
    if output.status.success() {
        assert!(stderr.is_empty(), "{stderr}");
        Ok(stdout.lines().map(str::to_string).collect())
    } else {
        assert_eq!(output.status.code(), Some(1));
        Err(stderr)
    }
}

fn verify(manifest: &Path) -> Result<String, String> {
    let uri = format!("file://{}", manifest.display());
    let output = binary_alpha(&["data", "verify", "--manifest", &uri]);
    let stdout = String::from_utf8(output.stdout).unwrap();
    let stderr = String::from_utf8(output.stderr).unwrap();
    if output.status.success() {
        assert!(stderr.is_empty(), "{stderr}");
        Ok(stdout.trim_end().to_string())
    } else {
        assert!(stdout.is_empty(), "{stdout}");
        Err(stderr)
    }
}

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

fn manifest_json(path: &Path) -> Value {
    serde_json::from_slice(&fs::read(path).unwrap()).unwrap()
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

fn generation(line: &str) -> String {
    line.split(" generation ")
        .nth(1)
        .unwrap()
        .split(' ')
        .next()
        .unwrap()
        .to_string()
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
            && lines[2].contains(" rows 4 objects 9 reused 4 "),
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
        assert_eq!(json["config_hash"].as_str().unwrap()[..10], *"v2:sha256:");
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
    assert_eq!(expected_keys.len(), 17);

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
    // Each scenario reproduces one interruption state on a freshly published tree.
    let snapshot = |scratch: &Scratch| -> (Vec<String>, Vec<(PathBuf, Vec<u8>)>) {
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
    let assert_same = |scratch: &Scratch, before: &(Vec<String>, Vec<(PathBuf, Vec<u8>)>)| {
        assert_eq!(
            scratch.objects("published"),
            before.0,
            "no duplicate objects"
        );
        assert_eq!(scratch.manifests("published").len(), before.1.len());
        for (path, bytes) in &before.1 {
            assert_eq!(
                &fs::read(path).unwrap(),
                bytes,
                "identical ready manifest after resumption"
            );
            let mirror = scratch
                .path("retained")
                .join(path.strip_prefix(scratch.path("published")).unwrap());
            assert_eq!(&fs::read(&mirror).unwrap(), bytes, "restored local mirror");
        }
    };

    // Interrupted at local close: the normalized object was never closed, no ready manifest
    // exists, and temporary files are left in the retained object directory.
    let (scratch, config, lines) = published("resume_local_close");
    let before = snapshot(&scratch);
    let ticks = before
        .1
        .iter()
        .map(|(path, _)| manifest_json(path))
        .find(|json| json["source_kind"] == "tick_csv")
        .unwrap();
    let normalized_key = ticks["objects"][1]["key"].as_str().unwrap();
    fs::remove_file(scratch.path("published").join(normalized_key)).unwrap();
    fs::remove_file(scratch.path("retained").join(normalized_key)).unwrap();
    for (path, _) in &before.1 {
        fs::remove_file(path).unwrap();
        fs::remove_file(
            scratch
                .path("retained")
                .join(path.strip_prefix(scratch.path("published")).unwrap()),
        )
        .unwrap();
    }
    fs::write(scratch.path("retained/objects/.tmp-leftover-1"), b"partial").unwrap();
    fs::write(
        scratch.path(&format!(
            "retained/objects/.tmp-{}-1",
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
    assert!(scratch.path("published").join(normalized_key).is_file());
    assert_same(&scratch, &before);

    // Interrupted during upload: one destination object and every ready manifest are missing.
    let (scratch, config, _) = published("resume_upload");
    let before = snapshot(&scratch);
    let apple = before
        .1
        .iter()
        .map(|(path, _)| manifest_json(path))
        .find(|json| json["provider_symbol"] == "#AAPL")
        .unwrap();
    let removed = scratch
        .path("published")
        .join(apple["objects"][0]["key"].as_str().unwrap());
    fs::remove_file(&removed).unwrap();
    for (path, _) in &before.1 {
        fs::remove_file(path).unwrap();
    }
    let again = import(&config).unwrap();
    assert!(
        again[1].contains(" objects 10 reused 9 ["),
        "one object re-created: {}",
        again[1]
    );
    assert!(again[2].contains(" objects 9 reused 9 ["), "{}", again[2]);
    assert!(removed.is_file());
    assert_same(&scratch, &before);

    // Interrupted after object creation and before ready publication: every object exists,
    // no ready manifest does.
    let (scratch, config, _) = published("resume_before_ready");
    let before = snapshot(&scratch);
    for (path, _) in &before.1 {
        fs::remove_file(path).unwrap();
        fs::remove_file(
            scratch
                .path("retained")
                .join(path.strip_prefix(scratch.path("published")).unwrap()),
        )
        .unwrap();
    }
    let again = import(&config).unwrap();
    assert!(
        again
            .iter()
            .all(|line| line.contains(" reused ") && line.ends_with("s]")),
        "{again:?}"
    );
    assert_same(&scratch, &before);

    // Interrupted after destination ready creation and before the local mirror.
    let (scratch, config, _) = published("resume_mirror");
    let before = snapshot(&scratch);
    for (path, _) in &before.1 {
        fs::remove_file(
            scratch
                .path("retained")
                .join(path.strip_prefix(scratch.path("published")).unwrap()),
        )
        .unwrap();
    }
    let again = import(&config).unwrap();
    assert!(
        again
            .iter()
            .all(|line| line.ends_with("(already published)")),
        "{again:?}"
    );
    assert_same(&scratch, &before);

    // Conflicting bytes at an existing key fail closed without replacing either copy, whether
    // or not the committed ready manifest is still present.
    let (scratch, config, _) = published("resume_conflict");
    let before = snapshot(&scratch);
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
    for (path, _) in &before.1 {
        fs::remove_file(path).unwrap();
    }
    let error = import(&config).unwrap_err();
    assert!(error.contains("already holds different content"), "{error}");
    assert_eq!(fs::read(&victim).unwrap(), b"different bytes");
    assert_eq!(
        fs::read(scratch.path("retained").join(victim_key)).unwrap(),
        victim_bytes
    );
    assert!(
        scratch.manifests("published").len() < before.1.len(),
        "no early ready state after a conflict"
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
        .contains("expected the approved five-second contract")
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

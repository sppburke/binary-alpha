//! Fixture writers and command helpers shared by the application's integration tests.
#![allow(dead_code)]

use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::sync::Arc;

use parquet::basic::{Compression, ZstdLevel};
use parquet::column::writer::ColumnWriter;
use parquet::data_type::ByteArray;
use parquet::file::metadata::KeyValue;
use parquet::file::properties::WriterProperties;
use parquet::file::writer::SerializedFileWriter;
use parquet::schema::parser::parse_message_type;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

pub const BAR_SCHEMA: &str = "message duckdb_schema {
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

pub const INTERVAL: [(&str, &str); 7] = [
    ("closed", "left"),
    ("frequency", "5s"),
    ("interval", "[timestamp,timestamp+5s)"),
    ("label", "left"),
    ("offset_seconds", "0"),
    ("origin", "unix_epoch_utc"),
    ("timestamp_semantics", "bar_start"),
];

pub const OFFSET: i64 = 7200;

/// One synthetic archive row; `server` defaults to `unix + OFFSET`.
#[derive(Clone)]
pub struct BarRow {
    pub symbol: String,
    pub symbol_id: i32,
    pub unix: i64,
    pub server: Option<i64>,
    pub timestamp: Option<i64>,
    pub ohlcv: [f64; 5],
    pub period: i32,
}

pub fn bar(symbol: &str, symbol_id: i32, unix: i64, ohlcv: [f64; 5]) -> BarRow {
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

pub fn bars(symbol: &str, symbol_id: i32, start: i64, count: i64) -> Vec<BarRow> {
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

pub fn write_bar_file(
    path: &Path,
    rows: &[BarRow],
    metadata: Option<&[(&str, &str)]>,
    schema: &str,
) {
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

pub fn sha256(path: &Path) -> String {
    let bytes = fs::read(path).unwrap();
    Sha256::digest(&bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

/// CRC32C of a file, computed independently of the application.
pub fn crc32c(path: &Path) -> u32 {
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
pub struct AssetSpec {
    pub asset: &'static str,
    pub expected_symbol_id: Option<i32>,
    pub symbol_id: Option<i32>,
    pub files: Vec<Vec<BarRow>>,
    pub metadata: bool,
}

pub fn write_collection(root: &Path, assets: &[AssetSpec]) -> PathBuf {
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

pub fn write_ticks(path: &Path, rows: &[&str]) {
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    let mut text = String::from("time_utc,symbol,price\n");
    for row in rows {
        text.push_str(row);
        text.push('\n');
    }
    fs::write(path, text).unwrap();
}

pub struct Scratch {
    pub root: PathBuf,
}

impl Scratch {
    pub fn new(name: &str) -> Self {
        let root = PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join(format!("scratch_{name}"));
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(&root).unwrap();
        Self { root }
    }

    pub fn path(&self, relative: &str) -> PathBuf {
        self.root.join(relative)
    }

    /// Writes a research configuration publishing to `published` and retaining in `retained`.
    pub fn config(&self, name: &str, sources: &str) -> PathBuf {
        self.config_with(name, "retained", sources)
    }

    pub fn config_with(&self, name: &str, retained: &str, sources: &str) -> PathBuf {
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

    pub fn tick_source(&self) -> String {
        "\n[[import.sources]]\nkind = \"tick_csv\"\npath = \"sources/ticks/ticks.csv\"\nbroker = \"pocket_option\"\nrole = \"development\"\nprovider_symbol = \"AEDCNY_otc\"\nsource_symbol = \"AEDCNY\"\nprice_scale = 6\n".to_string()
    }

    pub fn bar_source(&self) -> String {
        "\n[[import.sources]]\nkind = \"bar_parquet_collection\"\npath = \"sources/bars\"\nbroker = \"pocket_option\"\nrole = \"evaluation\"\nmanifest = \"collection.json\"\n".to_string()
    }

    pub fn daily_source(&self) -> String {
        "\n[[import.sources]]\nkind = \"tick_parquet_daily\"\npath = \"sources/deriv\"\nbroker = \"deriv\"\nrole = \"development\"\nprice_scale = 5\ninstruments = [\"AUDUSD\", \"USDJPY\"]\n".to_string()
    }

    pub fn objects(&self, store: &str) -> Vec<String> {
        let mut names: Vec<String> = fs::read_dir(self.path(store).join("objects"))
            .unwrap()
            .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
            .filter(|name| !name.starts_with('.'))
            .collect();
        names.sort();
        names
    }

    pub fn manifests(&self, store: &str) -> Vec<PathBuf> {
        let mut paths: Vec<PathBuf> = fs::read_dir(self.path(store).join("manifests"))
            .unwrap()
            .map(|entry| entry.unwrap().path().join("ready.json"))
            .filter(|path| path.is_file())
            .collect();
        paths.sort();
        paths
    }
}

pub fn binary_alpha(args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_binary-alpha"))
        .args(args)
        .output()
        .expect("binary-alpha runs")
}

pub fn import(config: &Path) -> Result<Vec<String>, String> {
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

pub fn verify(manifest: &Path) -> Result<String, String> {
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

pub fn manifest_json(path: &Path) -> Value {
    serde_json::from_slice(&fs::read(path).unwrap()).unwrap()
}

pub fn generation(line: &str) -> String {
    line.split(" generation ")
        .nth(1)
        .unwrap()
        .split(' ')
        .next()
        .unwrap()
        .to_string()
}

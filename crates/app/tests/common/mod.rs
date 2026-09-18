//! Fixture writers and command helpers shared by the application's integration tests.
#![allow(dead_code)]

use std::borrow::Cow;
use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::sync::Arc;

use binary_alpha_engine::dataset::GenerationManifest;
use binary_alpha_engine::market::Tick;
use parquet::basic::{Compression, ZstdLevel};
use parquet::column::writer::ColumnWriter;
use parquet::data_type::ByteArray;
use parquet::file::metadata::KeyValue;
use parquet::file::properties::WriterProperties;
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::file::writer::SerializedFileWriter;
use parquet::record::{Field, RowAccessor};
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

/// Exact regular-file inventory, including names, for refusal/source-preservation oracles.
pub fn snapshot_tree(root: &Path) -> std::collections::BTreeMap<PathBuf, Vec<u8>> {
    fn visit(root: &Path, dir: &Path, files: &mut std::collections::BTreeMap<PathBuf, Vec<u8>>) {
        if !dir.exists() {
            return;
        }
        for entry in fs::read_dir(dir).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                visit(root, &path, files);
            } else {
                files.insert(
                    path.strip_prefix(root).unwrap().to_path_buf(),
                    fs::read(path).unwrap(),
                );
            }
        }
    }
    let mut files = std::collections::BTreeMap::new();
    visit(root, root, &mut files);
    files
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
        let raw_page = format!("{{\"asset\": \"{}\", \"data\": []}}", spec.asset);
        let raw_path = asset_root.join("raw_pages.ndjson");
        fs::write(&raw_path, &raw_page).unwrap();
        let payload_sha256 = sha256(&raw_path);
        fs::write(&raw_path, format!("{raw_page}\n")).unwrap();
        fs::write(
            asset_root.join("checkpoint.ndjson"),
            format!(
                "{}\n",
                json!({"payload_sha256":payload_sha256,"request_token":"1747660500"})
            ),
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

/// The exact schema of one daily tick archive file.
pub const DAILY_SCHEMA: &str = "message schema {
  OPTIONAL INT64 datetime_utc (TIMESTAMP(NANOS,true));
  OPTIONAL DOUBLE price;
}
";

/// Writes one Snappy daily archive file of `(nanoseconds, price)` rows; `null_row` leaves both
/// cells of that row null.
pub fn write_daily_file(path: &Path, rows: &[(i64, f64)], null_row: Option<usize>) {
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    let schema = Arc::new(parse_message_type(DAILY_SCHEMA).unwrap());
    let properties = Arc::new(
        WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build(),
    );
    let mut writer =
        SerializedFileWriter::new(File::create(path).unwrap(), schema, properties).unwrap();
    let mut group = writer.next_row_group().unwrap();
    let present: Vec<&(i64, f64)> = rows
        .iter()
        .enumerate()
        .filter(|(row, _)| Some(*row) != null_row)
        .map(|(_, cells)| cells)
        .collect();
    let levels: Vec<i16> = (0..rows.len())
        .map(|row| i16::from(Some(row) != null_row))
        .collect();
    let times: Vec<i64> = present.iter().map(|(nanos, _)| *nanos).collect();
    let prices: Vec<f64> = present.iter().map(|(_, price)| *price).collect();
    let mut column = group.next_column().unwrap().unwrap();
    if let ColumnWriter::Int64ColumnWriter(typed) = column.untyped() {
        typed.write_batch(&times, Some(&levels), None).unwrap();
    }
    column.close().unwrap();
    let mut column = group.next_column().unwrap().unwrap();
    if let ColumnWriter::DoubleColumnWriter(typed) = column.untyped() {
        typed.write_batch(&prices, Some(&levels), None).unwrap();
    }
    column.close().unwrap();
    group.close().unwrap();
    writer.close().unwrap();
}

/// The metadata document of one archive day, with every field the observed archive records.
pub fn daily_metadata(symbol: &str, date: &str, ticks: u64) -> String {
    json!({
        "symbol": symbol,
        "date": date,
        "calendar": "UTC",
        "ticks": ticks,
        "windows_requested": 96,
        "windows_skipped_closed": 0,
        "gaps": [],
        "clipped_by_retention": false,
        "clipped_by_now": false,
        "market_closed": false,
        "complete": true,
        "written_at": "2026-08-10T07:18:42.882488+00:00"
    })
    .to_string()
}

/// Writes one listed directory: a metadata file per day and a Parquet file for each day with
/// rows.
pub fn write_daily_directory(dir: &Path, name: &str, symbol: &str, days: &[(&str, &[(i64, f64)])]) {
    fs::create_dir_all(dir).unwrap();
    for (date, rows) in days {
        fs::write(
            dir.join(format!("{name}_{date}_ticks.meta.json")),
            daily_metadata(symbol, date, rows.len() as u64),
        )
        .unwrap();
        if !rows.is_empty() {
            write_daily_file(
                &dir.join(format!("{name}_{date}_ticks.parquet")),
                rows,
                None,
            );
        }
    }
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

/// The standard-output lines of a successful command, or the diagnostic of a failed one.
pub fn command(args: &[&str]) -> Result<Vec<String>, String> {
    let output = binary_alpha(args);
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

pub fn import(config: &Path) -> Result<Vec<String>, String> {
    command(&["data", "import", "--config", config.to_str().unwrap()])
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

/// Runs one command under GNU time, returning its standard-output lines, wall seconds, and the
/// child's peak resident kilobytes.
pub fn timed(args: &[&str]) -> (Vec<String>, f64, u64) {
    let started = std::time::Instant::now();
    let output = Command::new("/usr/bin/time")
        .arg("-v")
        .arg(env!("CARGO_BIN_EXE_binary-alpha"))
        .args(args)
        .output()
        .expect("GNU time runs the command");
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
    let stdout = String::from_utf8(output.stdout).unwrap();
    (stdout.lines().map(str::to_string).collect(), wall, peak)
}

/// The peak resident size of this test process, in kilobytes.
pub fn in_process_peak_kb() -> u64 {
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

/// A streaming reader of a legacy comma-separated reference file: the header, then one row at
/// a time. A field may be quoted with double quotes, inside which a doubled quote is one quote
/// and a comma is literal; no field spans a line. Every row must have the header's width.
pub struct LegacyCsv {
    pub header: Vec<String>,
    pub path: PathBuf,
    lines: std::io::Lines<std::io::BufReader<File>>,
}

impl LegacyCsv {
    pub fn open(path: &Path) -> Self {
        use std::io::BufRead;
        let mut lines =
            std::io::BufReader::with_capacity(1 << 20, File::open(path).unwrap()).lines();
        let header = split_csv(&lines.next().unwrap().unwrap());
        Self {
            header,
            path: path.to_path_buf(),
            lines,
        }
    }

    pub fn next_row(&mut self) -> Option<Vec<String>> {
        let row = split_csv(&self.lines.next()?.unwrap());
        assert_eq!(
            row.len(),
            self.header.len(),
            "{}: ragged row",
            self.path.display()
        );
        Some(row)
    }

    /// The position of a header field.
    pub fn column(&self, name: &str) -> usize {
        self.header
            .iter()
            .position(|column| column == name)
            .unwrap_or_else(|| panic!("{}: no column {name}", self.path.display()))
    }
}

/// Splits one line into fields, honoring double-quoted fields with doubled quotes.
pub fn split_csv(line: &str) -> Vec<String> {
    let mut fields = Vec::new();
    let mut field = String::new();
    let mut quoted = false;
    let mut chars = line.chars().peekable();
    while let Some(character) = chars.next() {
        match (quoted, character) {
            (true, '"') if chars.peek() == Some(&'"') => {
                chars.next();
                field.push('"');
            }
            (true, '"') => quoted = false,
            (false, '"') if field.is_empty() => quoted = true,
            (false, ',') => fields.push(std::mem::take(&mut field)),
            (_, character) => field.push(character),
        }
    }
    assert!(!quoted, "unterminated quoted field in {line:?}");
    fields.push(field);
    fields
}

/// One published table: its column names and every row.
pub type Table = (
    Vec<String>,
    Vec<Vec<Option<binary_alpha_engine::features::Value>>>,
);

/// Streams a published table's column names and rows through the generic row API,
/// independently of the application's readers.
pub fn table_rows(
    path: &Path,
) -> (
    Vec<String>,
    impl Iterator<Item = Vec<Option<binary_alpha_engine::features::Value>>> + use<>,
) {
    use binary_alpha_engine::features::Value;
    let reader = SerializedFileReader::new(File::open(path).unwrap()).unwrap();
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
                Field::Int(value) => Some(Value::Int(i64::from(*value))),
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

/// Decodes a whole little-endian array object.
pub fn read_le<T, const N: usize>(path: &Path, decode: fn([u8; N]) -> T) -> Vec<T> {
    let bytes = std::fs::read(path).unwrap();
    let (chunks, remainder) = bytes.as_chunks::<N>();
    assert!(
        remainder.is_empty(),
        "{}: {} trailing bytes do not form an element",
        path.display(),
        remainder.len()
    );
    chunks.iter().map(|chunk| decode(*chunk)).collect()
}

/// Reads a whole published table.
pub fn read_table(path: &Path) -> Table {
    let (names, rows) = table_rows(path);
    (names, rows.collect())
}

/// Every normalized tick of a published dataset generation, through the generic row API.
pub fn read_normalized_ticks(store: &Path, dataset: &GenerationManifest) -> Vec<Tick> {
    binary_alpha_app::daily::observation_partitions(dataset)
        .unwrap()
        .into_iter()
        .flat_map(|(object, _)| {
            let reader =
                SerializedFileReader::new(File::open(store.join(&object.key)).unwrap()).unwrap();
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
                .collect::<Vec<_>>()
        })
        .collect()
}

/// Scripted broker transport and clock shared by Phase 10 proofs.
pub mod broker {
    pub struct HttpCall {
        pub method: String,
        pub url: String,
        pub headers: Vec<(String, String)>,
    }
    pub struct FakeHttp {
        pub responses: VecDeque<Vec<u8>>,
        pub calls: Vec<HttpCall>,
    }
    impl Http for FakeHttp {
        fn get_json(&mut self, url: &str, headers: &[(String, String)]) -> Result<Vec<u8>, String> {
            self.calls.push(HttpCall {
                method: "GET".into(),
                url: url.into(),
                headers: headers.to_vec(),
            });
            self.responses
                .pop_front()
                .ok_or("unexpected HTTP request".into())
        }
        fn post_json(
            &mut self,
            url: &str,
            headers: &[(String, String)],
        ) -> Result<Vec<u8>, String> {
            self.calls.push(HttpCall {
                method: "POST".into(),
                url: url.into(),
                headers: headers.to_vec(),
            });
            self.responses
                .pop_front()
                .ok_or("unexpected HTTP request".into())
        }
    }
    pub fn fixture(name: &str) -> String {
        std::fs::read_to_string(
            std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("tests/fixtures/phase10")
                .join(name),
        )
        .unwrap()
        .trim_end()
        .into()
    }
    pub fn replace(text: &str, key: &str, value: &str) -> String {
        let mut fields: std::collections::BTreeMap<String, Box<serde_json::value::RawValue>> =
            serde_json::from_str(text).unwrap();
        fields.insert(
            key.into(),
            serde_json::value::RawValue::from_string(value.into()).unwrap(),
        );
        serde_json::to_string(&fields).unwrap()
    }
    pub fn correlated(text: &str, req_id: u64) -> String {
        replace(text, "req_id", &req_id.to_string())
    }

    use binary_alpha_app::broker::Clock;
    use binary_alpha_app::broker::transport::{Connector, Frame, Http, Transport};
    use std::collections::VecDeque;
    use std::sync::Arc;
    use std::sync::{
        Mutex,
        atomic::{AtomicI64, Ordering},
    };
    #[derive(Clone, Default)]
    pub struct FakeClock(Arc<AtomicI64>);
    impl FakeClock {
        pub fn at(value: i64) -> Self {
            Self(Arc::new(AtomicI64::new(value)))
        }
    }
    impl Clock for FakeClock {
        fn now_micros(&self) -> i64 {
            self.0.load(Ordering::SeqCst)
        }
        fn sleep(&mut self, micros: i64) {
            self.0.fetch_add(micros, Ordering::SeqCst);
        }
    }
    struct ScriptTransport {
        frames: VecDeque<Frame>,
        sent: Arc<Mutex<Vec<Frame>>>,
        clock: FakeClock,
        fail_on_write: Option<usize>,
    }
    impl Transport for ScriptTransport {
        fn send(&mut self, frame: Frame) -> Result<(), String> {
            self.sent.lock().unwrap().push(frame);
            if self.fail_on_write == Some(self.sent.lock().unwrap().len()) {
                return Err("synthetic socket send failure".into());
            }
            Ok(())
        }
        fn receive(&mut self, timeout: i64) -> Result<Option<Frame>, String> {
            let frame = self.frames.pop_front();
            self.clock.sleep(if frame.is_some() { 10 } else { timeout });
            Ok(frame)
        }
        fn close(&mut self) -> Result<(), String> {
            self.send(Frame::Close)
        }
    }
    struct ScriptConnector {
        sessions: VecDeque<Vec<Frame>>,
        sent: Arc<Mutex<Vec<Frame>>>,
        clock: FakeClock,
        fail_on_write: Option<usize>,
    }
    impl Connector for ScriptConnector {
        fn connect(
            &mut self,
            _: &str,
            _: &[(String, String)],
        ) -> Result<Box<dyn Transport>, String> {
            Ok(Box::new(ScriptTransport {
                frames: self
                    .sessions
                    .pop_front()
                    .ok_or("unexpected connection")?
                    .into(),
                sent: Arc::clone(&self.sent),
                clock: self.clock.clone(),
                fail_on_write: self.fail_on_write,
            }))
        }
    }
    pub fn connector(
        sessions: Vec<Vec<Frame>>,
        clock: &FakeClock,
    ) -> (Box<dyn Connector>, Arc<Mutex<Vec<Frame>>>) {
        scripted(sessions, clock, None)
    }
    pub fn failing_connector(
        sessions: Vec<Vec<Frame>>,
        clock: &FakeClock,
        fail_on_write: usize,
    ) -> (Box<dyn Connector>, Arc<Mutex<Vec<Frame>>>) {
        scripted(sessions, clock, Some(fail_on_write))
    }
    fn scripted(
        sessions: Vec<Vec<Frame>>,
        clock: &FakeClock,
        fail_on_write: Option<usize>,
    ) -> (Box<dyn Connector>, Arc<Mutex<Vec<Frame>>>) {
        let sent = Arc::new(Mutex::new(Vec::new()));
        (
            Box::new(ScriptConnector {
                sessions: sessions.into(),
                sent: Arc::clone(&sent),
                clock: clock.clone(),
                fail_on_write,
            }),
            sent,
        )
    }
}

pub fn logged_output(result: Output) -> Result<String, String> {
    let stdout = String::from_utf8(result.stdout).unwrap();
    let stderr = String::from_utf8(result.stderr).unwrap();
    if result.status.success() {
        assert!(stderr.is_empty(), "{stderr}");
        Ok(stdout)
    } else {
        assert_eq!(result.status.code(), Some(1));
        Err(stderr)
    }
}

pub fn cli(log: &Path, args: &[&str]) -> Result<String, String> {
    fs::write(log, []).unwrap();
    logged_output(
        Command::new(env!("CARGO_BIN_EXE_binary-alpha"))
            .args(args)
            .env("BINARY_ALPHA_STORE_LOG", log)
            .output()
            .unwrap(),
    )
}

/// `cli` under a distinct operator account: the grant command's simulated operator capability.
pub fn cli_as(log: &Path, user: &str, args: &[&str]) -> Result<String, String> {
    fs::write(log, []).unwrap();
    logged_output(
        Command::new(env!("CARGO_BIN_EXE_binary-alpha"))
            .args(args)
            .env("BINARY_ALPHA_STORE_LOG", log)
            .env("USER", user)
            .output()
            .unwrap(),
    )
}

pub mod daily;
pub mod legacy;

pub mod current;

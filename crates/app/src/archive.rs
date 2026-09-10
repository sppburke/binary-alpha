//! Parquet input and output through the direct reader and writer: the normalized tick object,
//! decoding of the observed daily tick archive, and validation of the observed five-second bar
//! archive. Nothing here rewrites archive bytes.

use std::fs::File;
use std::path::Path;
use std::sync::Arc;

use binary_alpha_engine::dataset::{IntervalContract, MANIFEST_SCHEMA_VERSION};
use binary_alpha_engine::market::{
    Bar, BarSequence, InstrumentId, PriceScale, Tick, TickSequence, float_price_units,
    format_event_time_micros,
};
use parquet::basic::{Compression, ZstdLevel};
use parquet::column::reader::{ColumnReader, get_typed_column_reader};
use parquet::column::writer::ColumnWriter;
use parquet::data_type::{ByteArrayType, DoubleType, Int32Type, Int64Type};
use parquet::file::metadata::KeyValue;
use parquet::file::properties::WriterProperties;
use parquet::file::reader::{FileReader, RowGroupReader, SerializedFileReader};
use parquet::file::writer::SerializedFileWriter;
use parquet::schema::parser::parse_message_type;
use parquet::schema::printer::print_schema;

/// The path of the normalized tick object inside a tick generation.
pub const TICK_OBJECT_PATH: &str = "normalized/ticks.parquet";

/// Rows per normalized row group.
const ROW_GROUP_ROWS: usize = 1 << 20;

/// Values read per call from one column.
const BATCH: usize = 1 << 16;

/// The exact schema every archive file must carry, as the direct reader prints it.
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
}
";

const TICK_SCHEMA: &str = "message binary_alpha_ticks {
  REQUIRED INT64 event_time_micros (TIMESTAMP(MICROS,true));
  REQUIRED INT64 price_units;
}
";

/// The exact schema of one daily tick archive file, as the direct reader prints it.
const DAILY_TICK_SCHEMA: &str = "message schema {
  OPTIONAL INT64 datetime_utc (TIMESTAMP(NANOS,true));
  OPTIONAL DOUBLE price;
}
";

/// Microseconds in one calendar day.
const DAY_MICROS: i64 = 86_400_000_000;

/// Row count and provider event-time bounds of one data object.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct DataSummary {
    pub rows: u64,
    pub first_event_micros: Option<i64>,
    pub last_event_micros: Option<i64>,
}

impl DataSummary {
    fn observe(&mut self, event_micros: i64) {
        self.rows += 1;
        self.first_event_micros.get_or_insert(event_micros);
        self.last_event_micros = Some(event_micros);
    }

    /// Folds a later summary into this one; callers enforce ordering between them.
    pub fn extend(&mut self, later: &Self) {
        self.rows += later.rows;
        if self.first_event_micros.is_none() {
            self.first_event_micros = later.first_event_micros;
        }
        if later.last_event_micros.is_some() {
            self.last_event_micros = later.last_event_micros;
        }
    }
}

/// Writes validated ticks as one Zstandard Parquet object and returns what was written. The
/// sequence rules (no backwards time, no conflicting prices at one time) are applied here.
pub fn write_ticks(
    path: &Path,
    instrument: &InstrumentId,
    scale: PriceScale,
    ticks: impl Iterator<Item = Result<Tick, String>>,
) -> Result<DataSummary, String> {
    let schema = Arc::new(parse_message_type(TICK_SCHEMA).map_err(|error| error.to_string())?);
    let metadata = [
        ("broker", instrument.broker.to_string()),
        ("provider_symbol", instrument.provider_symbol.to_string()),
        ("price_scale", scale.digits().to_string()),
        (
            "dataset_schema_version",
            MANIFEST_SCHEMA_VERSION.to_string(),
        ),
    ]
    .into_iter()
    .map(|(key, value)| KeyValue::new(key.to_string(), value))
    .collect();
    let properties = Arc::new(
        WriterProperties::builder()
            .set_compression(Compression::ZSTD(
                ZstdLevel::try_new(3).map_err(|error| error.to_string())?,
            ))
            .set_key_value_metadata(Some(metadata))
            .build(),
    );
    let file =
        File::create(path).map_err(|error| format!("cannot create {}: {error}", path.display()))?;
    let mut writer =
        SerializedFileWriter::new(file, schema, properties).map_err(|error| error.to_string())?;
    let mut sequence = TickSequence::default();
    let mut summary = DataSummary::default();
    let mut times = Vec::with_capacity(ROW_GROUP_ROWS);
    let mut prices = Vec::with_capacity(ROW_GROUP_ROWS);
    let flush = |writer: &mut SerializedFileWriter<File>,
                 times: &mut Vec<i64>,
                 prices: &mut Vec<i64>|
     -> Result<(), String> {
        let mut group = writer.next_row_group().map_err(|error| error.to_string())?;
        for values in [&times, &prices] {
            let mut column = group
                .next_column()
                .map_err(|error| error.to_string())?
                .ok_or("missing tick column")?;
            if let ColumnWriter::Int64ColumnWriter(typed) = column.untyped() {
                typed
                    .write_batch(values, None, None)
                    .map_err(|error| error.to_string())?;
            }
            column.close().map_err(|error| error.to_string())?;
        }
        group.close().map_err(|error| error.to_string())?;
        times.clear();
        prices.clear();
        Ok(())
    };
    for tick in ticks {
        let tick = tick?;
        sequence.accept(tick)?;
        summary.observe(tick.event_time_micros);
        times.push(tick.event_time_micros);
        prices.push(tick.price_units);
        if times.len() == ROW_GROUP_ROWS {
            flush(&mut writer, &mut times, &mut prices)?;
        }
    }
    if !times.is_empty() {
        flush(&mut writer, &mut times, &mut prices)?;
    }
    let file = writer.into_inner().map_err(|error| error.to_string())?;
    file.sync_all()
        .map_err(|error| format!("cannot close {}: {error}", path.display()))?;
    Ok(summary)
}

/// Reads a normalized tick object back, re-applying the schema and sequence rules.
pub fn read_ticks(path: &Path, scale: PriceScale) -> Result<DataSummary, String> {
    let reader = open(path)?;
    let schema = printed_schema(&reader);
    if schema != TICK_SCHEMA {
        return Err(format!(
            "{} has an unexpected schema:\n{schema}",
            path.display()
        ));
    }
    let embedded_scale = reader
        .metadata()
        .file_metadata()
        .key_value_metadata()
        .and_then(|pairs| pairs.iter().find(|pair| pair.key == "price_scale"))
        .and_then(|pair| pair.value.as_deref()?.parse::<u8>().ok());
    if embedded_scale != Some(scale.digits()) {
        return Err(format!(
            "{} declares price_scale {embedded_scale:?}, expected {}",
            path.display(),
            scale.digits()
        ));
    }
    let mut sequence = TickSequence::default();
    let mut summary = DataSummary::default();
    for index in 0..reader.num_row_groups() {
        let group = reader
            .get_row_group(index)
            .map_err(|error| error.to_string())?;
        let times = read_column::<Int64Type>(&*group, 0, None)?;
        let prices = read_column::<Int64Type>(&*group, 1, None)?;
        if times.len() != prices.len() {
            return Err(format!(
                "{} row group {index} has ragged columns",
                path.display()
            ));
        }
        for (event_time_micros, price_units) in times.into_iter().zip(prices) {
            sequence.accept(Tick {
                event_time_micros,
                price_units,
            })?;
            summary.observe(event_time_micros);
        }
    }
    Ok(summary)
}

/// Decodes one daily tick archive file whose calendar day starts at `day_start_micros`: every
/// nanosecond timestamp must fall on a whole microsecond inside that day, and every price is
/// converted exactly to units at `scale`. Nulls are rejected.
pub fn read_daily_ticks(
    path: &Path,
    scale: PriceScale,
    day_start_micros: i64,
) -> Result<Vec<Tick>, String> {
    let reader = open(path)?;
    let schema = printed_schema(&reader);
    if schema != DAILY_TICK_SCHEMA {
        return Err(format!(
            "{} has an unexpected schema:\n{schema}",
            path.display()
        ));
    }
    let mut ticks = Vec::new();
    for index in 0..reader.num_row_groups() {
        let group = reader
            .get_row_group(index)
            .map_err(|error| error.to_string())?;
        let rows = group.metadata().num_rows() as usize;
        let times = read_column::<Int64Type>(&*group, 0, Some(rows))?;
        let prices = read_column::<DoubleType>(&*group, 1, Some(rows))?;
        for (nanos, price) in times.into_iter().zip(prices) {
            let row = ticks.len();
            if nanos % 1_000 != 0 {
                return Err(format!(
                    "{} row {row}: timestamp {nanos} ns is not on a microsecond boundary",
                    path.display()
                ));
            }
            let event_time_micros = nanos / 1_000;
            if !(day_start_micros..day_start_micros + DAY_MICROS).contains(&event_time_micros) {
                return Err(format!(
                    "{} row {row}: {} is outside the file's calendar day",
                    path.display(),
                    format_event_time_micros(event_time_micros)
                ));
            }
            let price_units = float_price_units(price, scale)
                .map_err(|reason| format!("{} row {row}: {reason}", path.display()))?;
            ticks.push(Tick {
                event_time_micros,
                price_units,
            });
        }
    }
    Ok(ticks)
}

/// What one archive file must satisfy.
#[derive(Debug, Clone)]
pub struct BarExpectation {
    pub symbol: String,
    /// The recorded identifier; `None` requires one constant identifier across rows.
    pub symbol_id: Option<i32>,
    pub period_s: u16,
    /// Server seconds minus Unix seconds, when the source manifest records it.
    pub server_offset_s: Option<i64>,
    pub interval: IntervalContract,
    /// Whether every file must embed the interval contract.
    pub metadata_required: bool,
}

/// What one validated archive file contained.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BarFileSummary {
    pub data: DataSummary,
    pub embedded_interval: bool,
}

/// Validates one archive file row by row without rewriting it.
pub fn validate_bar_file(
    path: &Path,
    expectation: &BarExpectation,
) -> Result<BarFileSummary, String> {
    let reader = open(path)?;
    let schema = printed_schema(&reader);
    if schema != BAR_SCHEMA {
        return Err(format!(
            "{} has an unexpected schema:\n{schema}",
            path.display()
        ));
    }
    let metadata = reader.metadata();
    for group in metadata.row_groups() {
        for column in group.columns() {
            if !matches!(column.compression(), Compression::ZSTD(_)) {
                return Err(format!(
                    "{} column {} uses {:?}, expected Zstandard",
                    path.display(),
                    column.column_path(),
                    column.compression()
                ));
            }
        }
    }
    let embedded_interval = match metadata.file_metadata().key_value_metadata() {
        Some(pairs) => {
            let embedded = embedded_interval(pairs, &expectation.interval.provenance)
                .map_err(|reason| format!("{}: {reason}", path.display()))?;
            if embedded != expectation.interval {
                return Err(format!(
                    "{} embeds an interval contract that contradicts the declared one",
                    path.display()
                ));
            }
            true
        }
        None if expectation.metadata_required => {
            return Err(format!(
                "{} lacks the interval metadata its collection requires",
                path.display()
            ));
        }
        None => false,
    };
    let mut sequence = BarSequence::default();
    let mut summary = DataSummary::default();
    let mut symbol_id = expectation.symbol_id;
    for index in 0..reader.num_row_groups() {
        let group = reader
            .get_row_group(index)
            .map_err(|error| error.to_string())?;
        let rows = group.metadata().num_rows() as usize;
        let symbols = read_column::<ByteArrayType>(&*group, 0, Some(rows))?;
        let ids = read_column::<Int32Type>(&*group, 1, Some(rows))?;
        let timestamps = read_column::<Int64Type>(&*group, 2, Some(rows))?;
        let unix = read_column::<Int64Type>(&*group, 3, Some(rows))?;
        let server = read_column::<Int64Type>(&*group, 4, Some(rows))?;
        let prices: Vec<Vec<f64>> = (5..10)
            .map(|column| read_column::<DoubleType>(&*group, column, Some(rows)))
            .collect::<Result<_, _>>()?;
        let periods = read_column::<Int32Type>(&*group, 10, Some(rows))?;
        for row in 0..rows {
            let symbol = symbols[row].as_utf8().map_err(|error| error.to_string())?;
            if symbol != expectation.symbol {
                return Err(format!(
                    "{} row {row} carries symbol `{symbol}`, expected `{}`",
                    path.display(),
                    expectation.symbol
                ));
            }
            match symbol_id {
                Some(expected) if ids[row] != expected => {
                    return Err(format!(
                        "{} row {row} carries symbol identifier {}, expected {expected}",
                        path.display(),
                        ids[row]
                    ));
                }
                Some(_) => {}
                None => symbol_id = Some(ids[row]),
            }
            if timestamps[row]
                != unix[row]
                    .checked_mul(1_000_000)
                    .ok_or("timestamp overflow")?
            {
                return Err(format!(
                    "{} row {row}: timestamp {} does not equal Unix seconds {}",
                    path.display(),
                    timestamps[row],
                    unix[row]
                ));
            }
            if let Some(offset) = expectation.server_offset_s
                && server[row].checked_sub(unix[row]) != Some(offset)
            {
                return Err(format!(
                    "{} row {row}: server seconds {} minus Unix seconds {} is not the recorded offset {offset}",
                    path.display(),
                    server[row],
                    unix[row]
                ));
            }
            let period_s = u16::try_from(periods[row]).map_err(|_| {
                format!(
                    "{} row {row}: period {} is not an unsigned 16-bit value",
                    path.display(),
                    periods[row]
                )
            })?;
            let bar = Bar {
                start_unix_s: unix[row],
                open: prices[0][row],
                high: prices[1][row],
                low: prices[2][row],
                close: prices[3][row],
                volume: prices[4][row],
                period_s,
            };
            bar.validate(expectation.period_s)
                .map_err(|reason| format!("{}: {reason}", path.display()))?;
            sequence
                .accept(bar.start_unix_s)
                .map_err(|reason| format!("{}: {reason}", path.display()))?;
            summary.observe(bar.start_unix_s * 1_000_000);
        }
    }
    Ok(BarFileSummary {
        data: summary,
        embedded_interval,
    })
}

/// The interval contract a file embeds as key-value metadata.
fn embedded_interval(pairs: &[KeyValue], provenance: &str) -> Result<IntervalContract, String> {
    let get = |key: &str| -> Result<String, String> {
        pairs
            .iter()
            .find(|pair| pair.key == key)
            .and_then(|pair| pair.value.clone())
            .ok_or_else(|| format!("interval metadata lacks `{key}`"))
    };
    Ok(IntervalContract {
        closed: get("closed")?,
        frequency: get("frequency")?,
        interval: get("interval")?,
        label: get("label")?,
        offset_seconds: get("offset_seconds")?
            .parse()
            .map_err(|_| "interval metadata `offset_seconds` is not an integer".to_string())?,
        origin: get("origin")?,
        timestamp_semantics: get("timestamp_semantics")?,
        provenance: provenance.to_string(),
    })
}

fn open(path: &Path) -> Result<SerializedFileReader<File>, String> {
    let file =
        File::open(path).map_err(|error| format!("cannot open {}: {error}", path.display()))?;
    SerializedFileReader::new(file)
        .map_err(|error| format!("{} is not a readable Parquet file: {error}", path.display()))
}

fn printed_schema(reader: &SerializedFileReader<File>) -> String {
    let mut text = Vec::new();
    print_schema(&mut text, reader.metadata().file_metadata().schema());
    String::from_utf8_lossy(&text).into_owned()
}

/// Reads every value of one column of a row group, rejecting nulls.
fn read_column<T: parquet::data_type::DataType>(
    group: &dyn RowGroupReader,
    column: usize,
    expected_rows: Option<usize>,
) -> Result<Vec<T::T>, String> {
    let descriptor = group.metadata().column(column).column_descr_ptr();
    let reader: ColumnReader = group
        .get_column_reader(column)
        .map_err(|error| error.to_string())?;
    let mut reader = get_typed_column_reader::<T>(reader);
    let nullable = descriptor.max_def_level() > 0;
    let mut values = Vec::with_capacity(expected_rows.unwrap_or(BATCH));
    let mut levels = Vec::with_capacity(if nullable { BATCH } else { 0 });
    loop {
        levels.clear();
        let (records, read, level_count) = reader
            .read_records(BATCH, nullable.then_some(&mut levels), None, &mut values)
            .map_err(|error| error.to_string())?;
        if records == 0 {
            break;
        }
        if nullable && (read != level_count || levels.contains(&0)) {
            return Err(format!(
                "column {} contains a null value",
                descriptor.name()
            ));
        }
    }
    if let Some(expected) = expected_rows
        && values.len() != expected
    {
        return Err(format!(
            "column {} has {} values, expected {expected}",
            descriptor.name(),
            values.len()
        ));
    }
    Ok(values)
}

/// Renders a summary's coverage bounds for a manifest.
pub fn coverage(summary: &DataSummary) -> Result<(String, String), String> {
    match (summary.first_event_micros, summary.last_event_micros) {
        (Some(first), Some(last)) => Ok((
            format_event_time_micros(first),
            format_event_time_micros(last),
        )),
        _ => Err("dataset has no rows".to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use binary_alpha_engine::market::{BrokerId, ProviderSymbol};
    use parquet::record::RowAccessor;

    #[test]
    fn ticks_round_trip_through_parquet() {
        let dir = std::env::temp_dir().join(format!("binary-alpha-archive-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("ticks.parquet");
        let instrument = InstrumentId {
            broker: BrokerId::try_from("b".to_string()).unwrap(),
            provider_symbol: ProviderSymbol::try_from("S".to_string()).unwrap(),
        };
        let scale = PriceScale::try_from(6).unwrap();
        let ticks = (0..(ROW_GROUP_ROWS as i64 + 10)).map(|i| {
            Ok(Tick {
                event_time_micros: i * 1_000,
                price_units: 1_000_000 + i,
            })
        });
        let written = write_ticks(&path, &instrument, scale, ticks).unwrap();
        let read = read_ticks(&path, scale).unwrap();
        assert_eq!(written, read);
        let reader = open(&path).unwrap();
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
        assert_eq!(pairs.len(), ROW_GROUP_ROWS + 10);
        assert!(
            pairs
                .iter()
                .enumerate()
                .all(|(i, pair)| *pair == (i as i64 * 1_000, 1_000_000 + i as i64))
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
        assert!(
            metadata
                .iter()
                .any(|pair| pair.key == "provider_symbol" && pair.value.as_deref() == Some("S"))
        );
        assert_eq!(read.rows, ROW_GROUP_ROWS as u64 + 10);
        assert_eq!(read.first_event_micros, Some(0));
        assert_eq!(
            read.last_event_micros,
            Some((ROW_GROUP_ROWS as i64 + 9) * 1_000)
        );
        assert!(
            read_ticks(&path, PriceScale::try_from(5).unwrap())
                .unwrap_err()
                .contains("price_scale")
        );
        let backwards = [
            Ok(Tick {
                event_time_micros: 5,
                price_units: 1,
            }),
            Ok(Tick {
                event_time_micros: 4,
                price_units: 1,
            }),
        ];
        assert!(
            write_ticks(&path, &instrument, scale, backwards.into_iter())
                .unwrap_err()
                .contains("backwards")
        );
        std::fs::remove_dir_all(&dir).unwrap();
    }
}

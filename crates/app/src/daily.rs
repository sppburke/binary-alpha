//! Opt-in daily-v2 codecs. Legacy archive writers and command routing remain unchanged.
//! A call buffers one day, validates it before creating a file, and segments each column
//! independently of upstream batches. This module never reads or deletes source objects.

use std::{fs::File, path::Path, sync::Arc};

use binary_alpha_engine::{
    dataset::{
        IntervalContract, MANIFEST_SCHEMA_VERSION,
        daily::{DAILY_PARQUET_PROFILE, day_bounds},
    },
    market::{Bar, BarProviderColumns, InstrumentId, PriceScale, Tick},
    stream::{Candle, Flags},
};
use parquet::{
    basic::{Compression, Encoding, Repetition, ZstdLevel},
    column::writer::ColumnWriter,
    data_type::ByteArray,
    file::{
        metadata::KeyValue,
        properties::{EnabledStatistics, WriterProperties, WriterVersion},
        reader::{FileReader, SerializedFileReader},
        writer::SerializedFileWriter,
    },
    record::{Field, Row, RowAccessor},
    schema::parser::parse_message_type,
};
use sha2::{Digest, Sha256};

use crate::archive::{self, DataSummary};

pub const COLUMN_SEGMENT_VALUES: usize = 8_192;
pub const DATA_PAGE_ROW_LIMIT: usize = 8_192;
pub const DATA_PAGE_BYTE_LIMIT: usize = 1_048_576;

/// All page times are Unix microseconds; binary columns carry no text annotation.
const PAGE_SCHEMA: &str = "message binary_alpha_pages {
  REQUIRED BYTE_ARRAY acquisition_id (UTF8);
  OPTIONAL BYTE_ARRAY intent (UTF8);
  REQUIRED INT64 ordinal (UINT_64);
  OPTIONAL INT64 checkpoint_ordinal (UINT_64);
  REQUIRED BYTE_ARRAY order_kind (UTF8);
  REQUIRED BYTE_ARRAY payload_sha256 (UTF8);
  REQUIRED BYTE_ARRAY payload;
  OPTIONAL BYTE_ARRAY request_token (UTF8);
  OPTIONAL INT64 request_anchor_utc (TIMESTAMP(MICROS,true));
  OPTIONAL INT64 receipt_time_utc (TIMESTAMP(MICROS,true));
  REQUIRED BYTE_ARRAY receipt_state (UTF8);
  OPTIONAL INT64 first_event_time (TIMESTAMP(MICROS,true));
  OPTIONAL INT64 last_event_time (TIMESTAMP(MICROS,true));
  REQUIRED INT64 rows (UINT_64);
  OPTIONAL BYTE_ARRAY checkpoint;
  REQUIRED BYTE_ARRAY disposition (UTF8);
}
";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PageOrderKind {
    RequestOrder,
    SourceFileOrder,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReceiptState {
    Recorded,
    NotRecordedBySource,
    AbsentInLegacyRecord,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PageDisposition {
    Indexed,
    Diagnostic,
}

/// One response occurrence, including its own request and independent checkpoint position.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PageOccurrence {
    pub acquisition_id: String,
    pub intent: Option<String>,
    pub ordinal: u64,
    pub checkpoint_ordinal: Option<u64>,
    pub order_kind: PageOrderKind,
    pub payload_sha256: String,
    pub payload: Vec<u8>,
    pub request_token: Option<String>,
    pub request_anchor_utc: Option<i64>,
    pub receipt_time_utc: Option<i64>,
    pub receipt_state: ReceiptState,
    pub first_event_time: Option<i64>,
    pub last_event_time: Option<i64>,
    pub rows: u64,
    pub checkpoint: Option<Vec<u8>>,
    pub disposition: PageDisposition,
}

impl PageOccurrence {
    /// A nonempty response belongs to its last-event day; an empty response uses the historical
    /// anchor, then receipt time. Failure means unresolved evidence that the caller must retain.
    pub fn partition_time(&self) -> Result<i64, String> {
        if self.rows > 0 {
            self.last_event_time
        } else {
            self.request_anchor_utc.or(self.receipt_time_utc)
        }
        .ok_or_else(|| {
            "unresolved page: no assignable event, request anchor, or receipt time; retain source"
                .into()
        })
    }

    fn validate(&self) -> Result<(), String> {
        if self.checkpoint.is_some() != self.checkpoint_ordinal.is_some() {
            return Err("page checkpoint and checkpoint_ordinal must occur together".into());
        }
        if self.acquisition_id.is_empty() {
            return Err("page acquisition_id is empty".into());
        }
        if self.payload_sha256 != binary_alpha_engine::hex(&Sha256::digest(&self.payload)) {
            return Err("page payload_sha256 does not match payload".into());
        }
        if (self.receipt_state == ReceiptState::Recorded) != self.receipt_time_utc.is_some() {
            return Err("page receipt_state disagrees with receipt_time_utc".into());
        }
        match (self.rows, self.first_event_time, self.last_event_time) {
            (0, None, None) => {}
            (n, Some(first), Some(last)) if n > 0 && first <= last => {}
            _ => return Err("page event bounds disagree with rows".into()),
        }
        Ok(())
    }
}

fn properties(metadata: Vec<KeyValue>) -> Result<WriterProperties, String> {
    Ok(WriterProperties::builder()
        .set_writer_version(WriterVersion::PARQUET_1_0)
        .set_created_by(DAILY_PARQUET_PROFILE.into())
        .set_compression(Compression::ZSTD(ZstdLevel::try_new(3).map_err(err)?))
        .set_dictionary_enabled(false)
        .set_encoding(Encoding::PLAIN)
        .set_statistics_enabled(EnabledStatistics::None)
        .set_data_page_row_count_limit(DATA_PAGE_ROW_LIMIT)
        .set_data_page_size_limit(DATA_PAGE_BYTE_LIMIT)
        .set_write_batch_size(COLUMN_SEGMENT_VALUES)
        .set_key_value_metadata(Some(metadata))
        .build())
}

fn err(error: impl std::fmt::Display) -> String {
    error.to_string()
}

fn tick_metadata(instrument: &InstrumentId, scale: PriceScale) -> Vec<KeyValue> {
    [
        ("broker", instrument.broker.to_string()),
        ("provider_symbol", instrument.provider_symbol.to_string()),
        ("price_scale", scale.digits().to_string()),
        (
            "dataset_schema_version",
            MANIFEST_SCHEMA_VERSION.to_string(),
        ),
    ]
    .into_iter()
    .map(|(k, v)| KeyValue::new(k.into(), v))
    .collect()
}

fn bar_metadata() -> Vec<KeyValue> {
    IntervalContract::five_second("parquet_metadata")
        .metadata()
        .into_iter()
        .map(|(k, v)| KeyValue::new(k.into(), v))
        .collect()
}

fn page_metadata() -> Vec<KeyValue> {
    vec![KeyValue::new("page_schema_version".into(), "1".to_string())]
}

/// Values are assembled only for the current column's canonical segment. Null positions
/// count towards 8,192 and are represented in definition levels, never dropped from batching.
enum Value {
    Null,
    Int64(i64),
    Int32(i32),
    Double(f64),
    Bool(bool),
    Bytes(ByteArray),
}

fn write_file<R>(
    path: &Path,
    schema: &str,
    metadata: Vec<KeyValue>,
    rows: &[R],
    field: impl Fn(&R, usize) -> Value,
) -> Result<(), String> {
    let schema = Arc::new(parse_message_type(schema).map_err(err)?);
    let properties = Arc::new(properties(metadata)?);
    let file = File::create(path).map_err(err)?;
    let mut writer = SerializedFileWriter::new(file, schema.clone(), properties).map_err(err)?;
    let mut group = writer.next_row_group().map_err(err)?;
    let mut index = 0;
    while let Some(mut column) = group.next_column().map_err(err)? {
        let nullable =
            schema.get_fields()[index].get_basic_info().repetition() == Repetition::OPTIONAL;
        for segment in rows.chunks(COLUMN_SEGMENT_VALUES) {
            let values: Vec<_> = segment.iter().map(|row| field(row, index)).collect();
            let levels: Vec<_> = values
                .iter()
                .map(|v| i16::from(!matches!(v, Value::Null)))
                .collect();
            macro_rules! write {
                ($writer:expr, $variant:ident) => {{
                    let present: Vec<_> = values
                        .into_iter()
                        .filter_map(|v| match v {
                            Value::$variant(value) => Some(value),
                            Value::Null => None,
                            _ => unreachable!("schema and field mapping disagree"),
                        })
                        .collect();
                    $writer
                        .write_batch(&present, nullable.then_some(levels.as_slice()), None)
                        .map_err(err)?;
                }};
            }
            match column.untyped() {
                ColumnWriter::Int64ColumnWriter(w) => write!(w, Int64),
                ColumnWriter::Int32ColumnWriter(w) => write!(w, Int32),
                ColumnWriter::DoubleColumnWriter(w) => write!(w, Double),
                ColumnWriter::BoolColumnWriter(w) => write!(w, Bool),
                ColumnWriter::ByteArrayColumnWriter(w) => write!(w, Bytes),
                _ => unreachable!("daily schemas use no other physical type"),
            }
        }
        column.close().map_err(err)?;
        index += 1;
    }
    group.close().map_err(err)?;
    writer.into_inner().map_err(err)?.sync_all().map_err(err)
}

fn read_file<R>(
    path: &Path,
    schema: &str,
    metadata: Vec<KeyValue>,
    decode: impl Fn(Row) -> Result<R, String>,
) -> Result<Vec<R>, String> {
    let reader = SerializedFileReader::new(File::open(path).map_err(err)?).map_err(err)?;
    let file = reader.metadata().file_metadata();
    if file.schema() != &parse_message_type(schema).map_err(err)?
        || file.key_value_metadata() != Some(&metadata)
        || file.created_by() != Some(DAILY_PARQUET_PROFILE)
        || file.version() != 1
        || reader.num_row_groups() != 1
    {
        return Err(
            "daily file schema, semantic metadata, profile, or row-group count mismatch".into(),
        );
    }
    // Compression level and write-call segmentation are not encoded in the footer. Check
    // every profile property the footer does expose; writer-property and byte tests prove
    // the remaining choices at this pinned crate version.
    for column in reader.metadata().row_group(0).columns() {
        if !matches!(column.compression(), Compression::ZSTD(_))
            || column.dictionary_page_offset().is_some()
            || column.statistics().is_some()
            || column
                .encodings()
                .any(|e| !matches!(e, Encoding::PLAIN | Encoding::RLE))
        {
            return Err("daily file column encoding does not match daily-parquet-v1".into());
        }
    }
    reader
        .get_row_iter(None)
        .map_err(err)?
        .map(|row| decode(row.map_err(err)?))
        .collect()
}

fn summary<R>(
    date: &str,
    rows: &[R],
    time: impl Fn(&R) -> Result<i64, String>,
    ordered: bool,
) -> Result<DataSummary, String> {
    let (start, end) = day_bounds(date)?;
    let mut summary = DataSummary::default();
    let mut previous = None;
    for row in rows {
        let time = time(row)?;
        if !(start..end).contains(&time) {
            return Err(format!("row outside UTC day {date}"));
        }
        if ordered && previous.is_some_and(|p| p > time) {
            return Err("rows are not in canonical time order".into());
        }
        previous = Some(time);
        summary.rows += 1;
        summary.first_event_micros = Some(summary.first_event_micros.map_or(time, |t| t.min(time)));
        summary.last_event_micros = Some(summary.last_event_micros.map_or(time, |t| t.max(time)));
    }
    Ok(summary)
}

pub fn write_ticks<B: IntoIterator<Item = Tick>>(
    path: &Path,
    date: &str,
    instrument: &InstrumentId,
    scale: PriceScale,
    batches: impl IntoIterator<Item = B>,
) -> Result<DataSummary, String> {
    let rows: Vec<_> = batches.into_iter().flatten().collect();
    let summary = summary(date, &rows, |t| Ok(t.event_time_micros), true)?;
    write_file(
        path,
        archive::TICK_SCHEMA,
        tick_metadata(instrument, scale),
        &rows,
        |t, c| {
            Value::Int64(if c == 0 {
                t.event_time_micros
            } else {
                t.price_units
            })
        },
    )?;
    Ok(summary)
}

pub fn read_ticks(
    path: &Path,
    date: &str,
    instrument: &InstrumentId,
    scale: PriceScale,
) -> Result<Vec<Tick>, String> {
    let rows = read_file(
        path,
        archive::TICK_SCHEMA,
        tick_metadata(instrument, scale),
        |r| {
            Ok(Tick {
                event_time_micros: r.get_timestamp_micros(0).map_err(err)?,
                price_units: r.get_long(1).map_err(err)?,
            })
        },
    )?;
    summary(date, &rows, |t| Ok(t.event_time_micros), true)?;
    Ok(rows)
}

fn bar_time(bar: &Bar<BarProviderColumns>) -> Result<i64, String> {
    bar.validate(5)?;
    let time = bar
        .start_unix_s
        .checked_mul(1_000_000)
        .ok_or("bar timestamp overflow")?;
    if bar.provider.timestamp_utc != time {
        return Err("bar timestamp_utc disagrees with unix_utc_s".into());
    }
    Ok(time)
}

pub fn write_bars<B: IntoIterator<Item = Bar<BarProviderColumns>>>(
    path: &Path,
    date: &str,
    batches: impl IntoIterator<Item = B>,
) -> Result<DataSummary, String> {
    let rows: Vec<_> = batches.into_iter().flatten().collect();
    let summary = summary(date, &rows, bar_time, true)?;
    write_file(
        path,
        archive::BAR_SCHEMA,
        bar_metadata(),
        &rows,
        |b, c| match c {
            0 => Value::Bytes(ByteArray::from(b.provider.symbol.as_str())),
            1 => Value::Int32(b.provider.symbol_id),
            2 => Value::Int64(b.provider.timestamp_utc),
            3 => Value::Int64(b.start_unix_s),
            4 => Value::Int64(b.provider.server_time_s),
            5..=9 => Value::Double([b.open, b.high, b.low, b.close, b.volume][c - 5]),
            10 => Value::Int32(i32::from(b.period_s)),
            _ => unreachable!(),
        },
    )?;
    Ok(summary)
}

pub fn read_bars(path: &Path, date: &str) -> Result<Vec<Bar<BarProviderColumns>>, String> {
    let rows = read_file(path, archive::BAR_SCHEMA, bar_metadata(), |r| {
        Ok(Bar {
            provider: BarProviderColumns {
                symbol: r.get_string(0).map_err(err)?.clone(),
                symbol_id: r.get_int(1).map_err(err)?,
                timestamp_utc: r.get_timestamp_micros(2).map_err(err)?,
                server_time_s: r.get_long(4).map_err(err)?,
            },
            start_unix_s: r.get_long(3).map_err(err)?,
            open: r.get_double(5).map_err(err)?,
            high: r.get_double(6).map_err(err)?,
            low: r.get_double(7).map_err(err)?,
            close: r.get_double(8).map_err(err)?,
            volume: r.get_double(9).map_err(err)?,
            period_s: r.get_ushort(10).map_err(err)?,
        })
    })?;
    summary(date, &rows, bar_time, true)?;
    Ok(rows)
}

fn candle_time(candle: &Candle) -> Result<i64, String> {
    for count in [
        candle.observations,
        candle.duplicates,
        candle.missing_buckets_before,
        candle.frozen_observations,
        candle.max_jump_basis_points,
        candle.max_delayed_jump_basis_points,
        candle.max_reopen_jump_basis_points,
    ] {
        i64::try_from(count).map_err(|_| "candle count overflows INT64")?;
    }
    if candle.volume.is_some_and(|v| !v.is_finite()) {
        return Err("candle volume is not finite".into());
    }
    Ok(candle.open_time_micros)
}

fn candle_spec(duration: u32, offset: u32) -> Result<(), String> {
    if duration == 0 || offset >= duration {
        return Err("invalid candle duration/offset".into());
    }
    Ok(())
}

pub fn write_candles<B: IntoIterator<Item = Candle>>(
    path: &Path,
    date: &str,
    instrument: &InstrumentId,
    scale: PriceScale,
    duration: u32,
    offset: u32,
    batches: impl IntoIterator<Item = B>,
) -> Result<DataSummary, String> {
    candle_spec(duration, offset)?;
    let rows: Vec<_> = batches.into_iter().flatten().collect();
    let summary = summary(date, &rows, candle_time, true)?;
    write_file(
        path,
        archive::CANDLE_SCHEMA,
        archive::candle_metadata(instrument, scale, duration, offset),
        &rows,
        |c, i| match i {
            12 => c.volume.map_or(Value::Null, Value::Double),
            13 => c.gap_before_micros.map_or(Value::Null, Value::Int64),
            21..=32 => Value::Bool(archive::flag_field(c, i)),
            _ => Value::Int64(archive::int_field(c, i)),
        },
    )?;
    Ok(summary)
}

fn optional<T>(
    row: &Row,
    index: usize,
    get: impl Fn(&Row, usize) -> parquet::errors::Result<T>,
) -> Result<Option<T>, String> {
    if matches!(
        row.get_column_iter().nth(index).map(|(_, f)| f),
        Some(Field::Null)
    ) {
        Ok(None)
    } else {
        get(row, index).map(Some).map_err(err)
    }
}

pub fn read_candles(
    path: &Path,
    date: &str,
    instrument: &InstrumentId,
    scale: PriceScale,
    duration: u32,
    offset: u32,
) -> Result<Vec<Candle>, String> {
    candle_spec(duration, offset)?;
    let rows = read_file(
        path,
        archive::CANDLE_SCHEMA,
        archive::candle_metadata(instrument, scale, duration, offset),
        |r| {
            let time = |c| r.get_timestamp_micros(c).map_err(err);
            let int = |c| r.get_long(c).map_err(err);
            let count = |c| u64::try_from(int(c)?).map_err(err);
            let flag = |c| r.get_bool(c).map_err(err);
            let flags = Flags {
                low_activity: flag(21)?,
                hard_low_activity: flag(22)?,
                gap_before: flag(23)?,
                gap_inside: flag(24)?,
                missing_before: flag(25)?,
                frozen: flag(26)?,
                jump: flag(27)?,
                delayed_jump: flag(28)?,
                reopen_jump: flag(29)?,
                short_span: flag(30)?,
            };
            if flags.complete() != flag(31)? || flags.clean() != flag(32)? {
                return Err("candle complete/clean flags disagree".into());
            }
            Ok(Candle {
                open_time_micros: time(0)?,
                close_time_micros: time(1)?,
                known_at_micros: time(2)?,
                first_event_micros: time(3)?,
                last_event_micros: time(4)?,
                active_span_micros: int(5)?,
                open_units: int(6)?,
                high_units: int(7)?,
                low_units: int(8)?,
                close_units: int(9)?,
                observations: count(10)?,
                duplicates: count(11)?,
                volume: optional(&r, 12, RowAccessor::get_double)?,
                gap_before_micros: optional(&r, 13, RowAccessor::get_long)?,
                max_gap_inside_micros: int(14)?,
                missing_buckets_before: count(15)?,
                frozen_observations: count(16)?,
                frozen_micros: int(17)?,
                max_jump_basis_points: count(18)?,
                max_delayed_jump_basis_points: count(19)?,
                max_reopen_jump_basis_points: count(20)?,
                flags,
            })
        },
    )?;
    summary(date, &rows, candle_time, true)?;
    Ok(rows)
}

fn page_summary(date: &str, pages: &[PageOccurrence]) -> Result<DataSummary, String> {
    for pair in pages.windows(2) {
        if (&pair[0].acquisition_id, pair[0].ordinal) >= (&pair[1].acquisition_id, pair[1].ordinal)
        {
            return Err("pages are not in canonical acquisition_id/ordinal order".into());
        }
    }
    summary(
        date,
        pages,
        |p| {
            p.validate()?;
            p.partition_time()
        },
        false,
    )
}

pub fn write_pages<B: IntoIterator<Item = PageOccurrence>>(
    path: &Path,
    date: &str,
    batches: impl IntoIterator<Item = B>,
) -> Result<DataSummary, String> {
    let rows: Vec<_> = batches.into_iter().flatten().collect();
    let summary = page_summary(date, &rows)?;
    write_file(path, PAGE_SCHEMA, page_metadata(), &rows, |p, c| {
        let text = |s: &str| Value::Bytes(ByteArray::from(s));
        match c {
            0 => text(&p.acquisition_id),
            1 => p.intent.as_deref().map_or(Value::Null, text),
            2 => Value::Int64(p.ordinal as i64),
            3 => p
                .checkpoint_ordinal
                .map_or(Value::Null, |n| Value::Int64(n as i64)),
            4 => text(match p.order_kind {
                PageOrderKind::RequestOrder => "request_order",
                PageOrderKind::SourceFileOrder => "source_file_order",
            }),
            5 => text(&p.payload_sha256),
            6 => Value::Bytes(ByteArray::from(p.payload.clone())),
            7 => p.request_token.as_deref().map_or(Value::Null, text),
            8 => p.request_anchor_utc.map_or(Value::Null, Value::Int64),
            9 => p.receipt_time_utc.map_or(Value::Null, Value::Int64),
            10 => text(match p.receipt_state {
                ReceiptState::Recorded => "recorded",
                ReceiptState::NotRecordedBySource => "not_recorded_by_source",
                ReceiptState::AbsentInLegacyRecord => "absent_in_legacy_record",
            }),
            11 => p.first_event_time.map_or(Value::Null, Value::Int64),
            12 => p.last_event_time.map_or(Value::Null, Value::Int64),
            13 => Value::Int64(p.rows as i64),
            14 => p
                .checkpoint
                .as_ref()
                .map_or(Value::Null, |v| Value::Bytes(ByteArray::from(v.clone()))),
            15 => text(match p.disposition {
                PageDisposition::Indexed => "indexed",
                PageDisposition::Diagnostic => "diagnostic",
            }),
            _ => unreachable!(),
        }
    })?;
    Ok(summary)
}

pub fn read_pages(path: &Path, date: &str) -> Result<Vec<PageOccurrence>, String> {
    let rows = read_file(path, PAGE_SCHEMA, page_metadata(), |r| {
        let text = |c| r.get_string(c).cloned().map_err(err);
        let optional_text = |c| optional(&r, c, |r, c| r.get_string(c).cloned());
        let time = |c| optional(&r, c, RowAccessor::get_timestamp_micros);
        Ok(PageOccurrence {
            acquisition_id: text(0)?,
            intent: optional_text(1)?,
            ordinal: r.get_ulong(2).map_err(err)?,
            checkpoint_ordinal: optional(&r, 3, RowAccessor::get_ulong)?,
            order_kind: match text(4)?.as_str() {
                "request_order" => PageOrderKind::RequestOrder,
                "source_file_order" => PageOrderKind::SourceFileOrder,
                _ => return Err("invalid page order_kind".into()),
            },
            payload_sha256: text(5)?,
            payload: r.get_bytes(6).map_err(err)?.data().to_vec(),
            request_token: optional_text(7)?,
            request_anchor_utc: time(8)?,
            receipt_time_utc: time(9)?,
            receipt_state: match text(10)?.as_str() {
                "recorded" => ReceiptState::Recorded,
                "not_recorded_by_source" => ReceiptState::NotRecordedBySource,
                "absent_in_legacy_record" => ReceiptState::AbsentInLegacyRecord,
                _ => return Err("invalid page receipt_state".into()),
            },
            first_event_time: time(11)?,
            last_event_time: time(12)?,
            rows: r.get_ulong(13).map_err(err)?,
            checkpoint: optional(&r, 14, |r, c| r.get_bytes(c).map(|b| b.data().to_vec()))?,
            disposition: match text(15)?.as_str() {
                "indexed" => PageDisposition::Indexed,
                "diagnostic" => PageDisposition::Diagnostic,
                _ => return Err("invalid page disposition".into()),
            },
        })
    })?;
    page_summary(date, &rows)?;
    Ok(rows)
}

#[cfg(test)]
mod tests {
    use super::*;
    use binary_alpha_engine::{
        dataset::daily::DAY_MICROS,
        market::{BrokerId, ProviderSymbol},
    };
    use std::{
        fs,
        path::PathBuf,
        sync::atomic::{AtomicU64, Ordering},
    };

    const DATE: &str = "2026-09-17";
    const NEXT: &str = "2026-09-18";
    static NEXT_DIR: AtomicU64 = AtomicU64::new(0);

    struct Fixture(PathBuf);
    impl Fixture {
        fn new() -> Self {
            let path = std::env::temp_dir().join(format!(
                "binary-alpha-daily-{}-{}",
                std::process::id(),
                NEXT_DIR.fetch_add(1, Ordering::Relaxed)
            ));
            fs::create_dir(&path).unwrap();
            Self(path)
        }
        fn path(&self, name: &str) -> PathBuf {
            self.0.join(name)
        }
    }
    impl Drop for Fixture {
        fn drop(&mut self) {
            fs::remove_dir_all(&self.0).unwrap();
        }
    }

    fn instrument() -> InstrumentId {
        InstrumentId {
            broker: BrokerId::try_from("fixture".to_string()).unwrap(),
            provider_symbol: ProviderSymbol::try_from("S".to_string()).unwrap(),
        }
    }
    fn scale() -> PriceScale {
        PriceScale::try_from(4).unwrap()
    }
    fn start() -> i64 {
        day_bounds(DATE).unwrap().0
    }

    fn bar(index: i64) -> Bar<BarProviderColumns> {
        let start_unix_s = start() / 1_000_000 + index * 5;
        Bar {
            provider: BarProviderColumns {
                symbol: format!("S{}", index % 2),
                symbol_id: 100 + (index % 2) as i32,
                timestamp_utc: start_unix_s * 1_000_000,
                server_time_s: start_unix_s + 7200 + index % 3,
            },
            start_unix_s,
            open: 1.01,
            high: 2.0,
            low: 0.5,
            close: 1.02,
            volume: if index % 2 == 0 { -0.0 } else { 0.125 },
            period_s: 5,
        }
    }

    fn candle(index: i64) -> Candle {
        let open = start() + index * 1_000_000;
        Candle {
            open_time_micros: open,
            close_time_micros: open + 1_000_000,
            known_at_micros: open + DAY_MICROS,
            first_event_micros: open,
            last_event_micros: open + 999_999,
            active_span_micros: 999_999,
            open_units: 101,
            high_units: 103,
            low_units: 100,
            close_units: 102,
            observations: 4,
            duplicates: 1,
            volume: (index >= 8_192 && index % 2 == 0).then_some(0.25),
            gap_before_micros: (index % 3 == 0).then_some(2),
            max_gap_inside_micros: 3,
            missing_buckets_before: 4,
            frozen_observations: 5,
            frozen_micros: 6,
            max_jump_basis_points: 7,
            max_delayed_jump_basis_points: 8,
            max_reopen_jump_basis_points: 9,
            flags: Flags {
                low_activity: index % 2 == 0,
                hard_low_activity: index % 3 == 0,
                gap_before: index % 5 == 0,
                gap_inside: index % 7 == 0,
                missing_before: index % 11 == 0,
                frozen: index % 13 == 0,
                jump: index % 17 == 0,
                delayed_jump: index % 19 == 0,
                reopen_jump: index % 23 == 0,
                short_span: index % 29 == 0,
            },
        }
    }

    fn page(index: u64) -> PageOccurrence {
        let payload = vec![0, 255, b'\n', b'\r', 128, (index % 2) as u8];
        let empty = index.is_multiple_of(3);
        let recorded = index.is_multiple_of(2);
        PageOccurrence {
            acquisition_id: format!("acquisition-{:05}", index / 10_000),
            intent: (index.is_multiple_of(2)).then(|| "intent-record".into()),
            ordinal: index % 10_000,
            checkpoint_ordinal: (index >= 8_192 && index.is_multiple_of(2))
                .then_some(u64::MAX - index),
            order_kind: if index.is_multiple_of(2) {
                PageOrderKind::RequestOrder
            } else {
                PageOrderKind::SourceFileOrder
            },
            payload_sha256: binary_alpha_engine::hex(&Sha256::digest(&payload)),
            payload,
            request_token: (index.is_multiple_of(2)).then(|| "opaque+anchor".into()),
            request_anchor_utc: if empty && !recorded {
                Some(start() + 20)
            } else {
                None
            },
            receipt_time_utc: recorded.then_some(start() + 100),
            receipt_state: if recorded {
                ReceiptState::Recorded
            } else if index.is_multiple_of(3) {
                ReceiptState::NotRecordedBySource
            } else {
                ReceiptState::AbsentInLegacyRecord
            },
            first_event_time: (!empty).then_some(start() - 1),
            last_event_time: (!empty).then_some(start() + 10_000 - index as i64 % 10_000),
            rows: if empty { 0 } else { 2 },
            checkpoint: (index >= 8_192 && index.is_multiple_of(2))
                .then(|| vec![b'\n', 255, 0, b'\r']),
            disposition: if index.is_multiple_of(3) {
                PageDisposition::Diagnostic
            } else {
                PageDisposition::Indexed
            },
        }
    }

    fn deterministic<R: Clone + PartialEq + std::fmt::Debug>(
        rows: &[R],
        write: impl Fn(&Path, Vec<Vec<R>>) -> Result<DataSummary, String>,
        read: impl Fn(&Path) -> Result<Vec<R>, String>,
    ) {
        let fixture = Fixture::new();
        let mut expected = None;
        for batch in [777, 1_024, 65_536] {
            for repeat in 0..2 {
                let path = fixture.path(&format!("{batch}-{repeat}.parquet"));
                let batches = rows.chunks(batch).map(|s| s.to_vec()).collect();
                assert_eq!(write(&path, batches).unwrap().rows, rows.len() as u64);
                assert_eq!(read(&path).unwrap(), rows);
                let bytes = fs::read(&path).unwrap();
                if let Some(prior) = &expected {
                    assert_eq!(&bytes, prior, "batch={batch}, repeat={repeat}");
                } else {
                    expected = Some(bytes);
                }
                let reader = SerializedFileReader::new(File::open(path).unwrap()).unwrap();
                assert_eq!(reader.num_row_groups(), 1);
                assert_eq!(
                    reader.metadata().file_metadata().created_by(),
                    Some(DAILY_PARQUET_PROFILE)
                );
                for column in reader.metadata().row_group(0).columns() {
                    assert!(column.statistics().is_none());
                    assert!(column.dictionary_page_offset().is_none());
                    // The footer records only the codec, not the compression level.
                    assert!(matches!(column.compression(), Compression::ZSTD(_)));
                    assert!(
                        column
                            .encodings()
                            .all(|e| matches!(e, Encoding::PLAIN | Encoding::RLE))
                    );
                }
            }
        }
    }

    #[test]
    fn ticks_round_trip_with_identical_bytes_across_batches_and_repeated_writes() {
        let rows: Vec<_> = (0..70_003)
            .map(|i| Tick {
                event_time_micros: start() + i / 2,
                price_units: i / 2,
            })
            .collect();
        deterministic(
            &rows,
            |p, batches| write_ticks(p, DATE, &instrument(), scale(), batches),
            |p| read_ticks(p, DATE, &instrument(), scale()),
        );
    }

    #[test]
    fn all_eleven_bar_columns_round_trip_with_identical_bytes_across_batches() {
        let rows: Vec<_> = (0..17_003).map(bar).collect();
        deterministic(
            &rows,
            |p, batches| write_bars(p, DATE, batches),
            |p| {
                let decoded = read_bars(p, DATE)?;
                for (actual, expected) in decoded.iter().zip(&rows) {
                    assert_eq!(actual.volume.to_bits(), expected.volume.to_bits());
                    assert_eq!(actual.open.to_bits(), expected.open.to_bits());
                    assert_eq!(actual.high.to_bits(), expected.high.to_bits());
                    assert_eq!(actual.low.to_bits(), expected.low.to_bits());
                    assert_eq!(actual.close.to_bits(), expected.close.to_bits());
                }
                Ok(decoded)
            },
        );
    }

    #[test]
    fn candles_round_trip_all_columns_with_identical_bytes_across_batches() {
        let rows: Vec<_> = (0..17_003).map(candle).collect();
        deterministic(
            &rows,
            |p, batches| write_candles(p, DATE, &instrument(), scale(), 1, 0, batches),
            |p| read_candles(p, DATE, &instrument(), scale(), 1, 0),
        );
    }

    #[test]
    fn pages_round_trip_nulls_binary_and_occurrences_with_identical_bytes_across_batches() {
        let rows: Vec<_> = (0..17_003).map(page).collect();
        deterministic(
            &rows,
            |p, batches| write_pages(p, DATE, batches),
            |p| read_pages(p, DATE),
        );
    }

    #[test]
    fn midnight_is_half_open_and_repeated_ticks_keep_multiplicity_and_order() {
        let fixture = Fixture::new();
        let path = fixture.path("ticks.parquet");
        let midnight = start() + DAY_MICROS;
        let before = Tick {
            event_time_micros: midnight - 1,
            price_units: 10,
        };
        let after = Tick {
            event_time_micros: midnight,
            price_units: 11,
        };
        assert!(
            write_ticks(&path, DATE, &instrument(), scale(), [vec![before, after]])
                .unwrap_err()
                .contains("outside UTC day")
        );
        assert!(!path.exists());
        write_ticks(
            &path,
            DATE,
            &instrument(),
            scale(),
            [vec![before], vec![before, before]],
        )
        .unwrap();
        assert_eq!(
            read_ticks(&path, DATE, &instrument(), scale()).unwrap(),
            vec![before; 3]
        );
        assert!(read_ticks(&path, NEXT, &instrument(), scale()).is_err());
        let rows = vec![
            after,
            after,
            Tick {
                price_units: 12,
                ..after
            },
        ];
        write_ticks(&path, NEXT, &instrument(), scale(), [rows.clone()]).unwrap();
        assert_eq!(
            read_ticks(&path, NEXT, &instrument(), scale()).unwrap(),
            rows
        );
        assert!(
            write_ticks(
                &path,
                DATE,
                &instrument(),
                scale(),
                [vec![
                    before,
                    Tick {
                        event_time_micros: midnight - 2,
                        ..before
                    }
                ]]
            )
            .unwrap_err()
            .contains("order")
        );
    }

    #[test]
    fn pages_use_last_event_then_empty_anchor_then_receipt_and_never_deduplicate() {
        let fixture = Fixture::new();
        let path = fixture.path("pages.parquet");
        let mut a = page(1);
        a.first_event_time = Some(start() - 10);
        a.last_event_time = Some(start());
        a.request_anchor_utc = Some(start() - DAY_MICROS);
        assert_eq!(a.partition_time().unwrap(), start());
        let mut b = a.clone();
        b.ordinal += 1;
        b.receipt_time_utc = Some(start() + 123);
        b.receipt_state = ReceiptState::Recorded;
        write_pages(&path, DATE, [vec![a.clone(), b.clone()]]).unwrap();
        assert_eq!(read_pages(&path, DATE).unwrap(), vec![a.clone(), b.clone()]);
        assert!(write_pages(&path, DATE, [vec![b.clone(), a.clone()]]).is_err());
        assert!(write_pages(&path, DATE, [vec![a.clone(), a]]).is_err());
        let mut empty = page(3);
        empty.request_anchor_utc = Some(start() + 1);
        empty.receipt_time_utc = Some(start() + DAY_MICROS);
        empty.receipt_state = ReceiptState::Recorded;
        assert_eq!(empty.partition_time().unwrap(), start() + 1);
        write_pages(&path, DATE, [vec![empty.clone()]]).unwrap();
        empty.request_anchor_utc = None;
        assert!(write_pages(&path, DATE, [vec![empty.clone()]]).is_err());
        write_pages(&path, NEXT, [vec![empty.clone()]]).unwrap();
        empty.receipt_time_utc = None;
        empty.receipt_state = ReceiptState::NotRecordedBySource;
        let retained = fs::read(&path).unwrap();
        assert!(
            write_pages(&path, DATE, [vec![empty]])
                .unwrap_err()
                .contains("unresolved")
        );
        assert_eq!(fs::read(&path).unwrap(), retained);
        assert!(read_pages(&path, DATE).is_err());
    }

    #[test]
    fn page_hash_event_bounds_and_receipt_state_are_checked() {
        let fixture = Fixture::new();
        let path = fixture.path("pages.parquet");
        let base = page(1);
        let mut bad = base.clone();
        bad.payload.push(1);
        assert!(
            write_pages(&path, DATE, [vec![bad]])
                .unwrap_err()
                .contains("sha256")
        );
        let mut bad = base.clone();
        bad.checkpoint = Some(vec![0, 255]);
        assert!(
            write_pages(&path, DATE, [vec![bad]])
                .unwrap_err()
                .contains("checkpoint_ordinal")
        );
        let mut bad = base.clone();
        bad.checkpoint_ordinal = Some(99);
        assert!(
            write_pages(&path, DATE, [vec![bad]])
                .unwrap_err()
                .contains("checkpoint_ordinal")
        );
        let mut bad = base.clone();
        bad.receipt_state = ReceiptState::Recorded;
        assert!(
            write_pages(&path, DATE, [vec![bad]])
                .unwrap_err()
                .contains("receipt_state")
        );
        let mut bad = base.clone();
        bad.receipt_time_utc = Some(start());
        assert!(write_pages(&path, DATE, [vec![bad]]).is_err());
        let mut bad = base.clone();
        bad.rows = 0;
        assert!(write_pages(&path, DATE, [vec![bad]]).is_err());
        let mut bad = base.clone();
        bad.first_event_time = None;
        assert!(write_pages(&path, DATE, [vec![bad]]).is_err());
        let mut bad = base.clone();
        bad.first_event_time = Some(start() + DAY_MICROS);
        assert!(write_pages(&path, DATE, [vec![bad]]).is_err());
        let mut bad = base;
        bad.acquisition_id.clear();
        assert!(write_pages(&path, DATE, [vec![bad]]).is_err());
        assert!(!path.exists());
    }

    #[test]
    fn bars_and_candles_check_membership_order_and_representability() {
        let fixture = Fixture::new();
        let path = fixture.path("data.parquet");
        assert!(write_bars(&path, DATE, [vec![bar(1), bar(0)]]).is_err());
        assert!(write_bars(&path, NEXT, [vec![bar(0)]]).is_err());
        let mut bad = bar(0);
        bad.provider.timestamp_utc += 1;
        assert!(write_bars(&path, DATE, [vec![bad]]).is_err());
        let mut bad = bar(0);
        bad.start_unix_s = i64::MAX - i64::MAX % 5;
        assert!(
            write_bars(&path, DATE, [vec![bad]])
                .unwrap_err()
                .contains("overflow")
        );
        write_bars(&path, DATE, [vec![bar(0)]]).unwrap();
        assert!(read_bars(&path, NEXT).is_err());
        assert!(
            write_candles(
                &path,
                DATE,
                &instrument(),
                scale(),
                1,
                0,
                [vec![candle(1), candle(0)]]
            )
            .is_err()
        );
        assert!(
            write_candles(&path, NEXT, &instrument(), scale(), 1, 0, [vec![candle(0)]]).is_err()
        );
        let mut bad = candle(0);
        bad.observations = u64::MAX;
        assert!(write_candles(&path, DATE, &instrument(), scale(), 1, 0, [vec![bad]]).is_err());
        let mut bad = candle(0);
        bad.volume = Some(f64::NAN);
        assert!(write_candles(&path, DATE, &instrument(), scale(), 1, 0, [vec![bad]]).is_err());
        for (n, o) in [(0, 0), (5, 5)] {
            assert!(
                write_candles(&path, DATE, &instrument(), scale(), n, o, [vec![candle(0)]])
                    .is_err()
            );
        }
        // Only open time assigns the day: close and finalization can occur on later days.
        write_candles(
            &path,
            DATE,
            &instrument(),
            scale(),
            1,
            0,
            [vec![candle(86_399)]],
        )
        .unwrap();
        assert!(read_candles(&path, NEXT, &instrument(), scale(), 1, 0).is_err());
        assert_eq!(
            read_candles(&path, DATE, &instrument(), scale(), 1, 0).unwrap(),
            vec![candle(86_399)]
        );
    }

    #[test]
    fn empty_files_have_one_group_and_profile_properties_are_fixed() {
        let fixture = Fixture::new();
        let path = fixture.path("empty.parquet");
        write_ticks(&path, DATE, &instrument(), scale(), [Vec::new()]).unwrap();
        assert!(
            read_ticks(&path, DATE, &instrument(), scale())
                .unwrap()
                .is_empty()
        );
        write_bars(&path, DATE, [Vec::new()]).unwrap();
        assert!(read_bars(&path, DATE).unwrap().is_empty());
        write_candles(&path, DATE, &instrument(), scale(), 1, 0, [Vec::new()]).unwrap();
        assert!(
            read_candles(&path, DATE, &instrument(), scale(), 1, 0)
                .unwrap()
                .is_empty()
        );
        write_pages(&path, DATE, [Vec::new()]).unwrap();
        assert!(read_pages(&path, DATE).unwrap().is_empty());
        let props = properties(page_metadata()).unwrap();
        assert_eq!(props.data_page_row_count_limit(), 8_192);
        assert_eq!(props.data_page_size_limit(), 1_048_576);
        assert_eq!(props.write_batch_size(), 8_192);
        assert_eq!(props.writer_version(), WriterVersion::PARQUET_1_0);
        assert_eq!(
            props.compression(&"payload".into()),
            Compression::ZSTD(ZstdLevel::try_new(3).unwrap())
        );
        assert_eq!(
            props.statistics_enabled(&"payload".into()),
            EnabledStatistics::None
        );
        assert!(!props.dictionary_enabled(&"payload".into()));
    }

    #[test]
    fn readers_reject_wrong_schema_metadata_profile_and_order() {
        let fixture = Fixture::new();
        let path = fixture.path("ticks.parquet");
        let rows = [
            Tick {
                event_time_micros: start() + 2,
                price_units: 1,
            },
            Tick {
                event_time_micros: start(),
                price_units: 1,
            },
        ];
        write_file(
            &path,
            archive::TICK_SCHEMA,
            tick_metadata(&instrument(), scale()),
            &rows,
            |t, c| {
                Value::Int64(if c == 0 {
                    t.event_time_micros
                } else {
                    t.price_units
                })
            },
        )
        .unwrap();
        assert!(
            read_ticks(&path, DATE, &instrument(), scale())
                .unwrap_err()
                .contains("order")
        );
        assert!(
            read_ticks(&path, DATE, &instrument(), PriceScale::try_from(5).unwrap())
                .unwrap_err()
                .contains("metadata")
        );
        assert!(read_bars(&path, DATE).unwrap_err().contains("schema"));
        archive::write_ticks(
            &path,
            &instrument(),
            scale(),
            rows[..1].iter().copied().map(Ok),
        )
        .unwrap();
        assert!(
            read_ticks(&path, DATE, &instrument(), scale())
                .unwrap_err()
                .contains("profile")
        );
    }

    #[test]
    fn required_column_pages_follow_canonical_segments() {
        let fixture = Fixture::new();
        let path = fixture.path("ticks.parquet");
        let rows: Vec<_> = (0..20_000)
            .map(|i| Tick {
                event_time_micros: start() + i,
                price_units: i,
            })
            .collect();
        write_ticks(
            &path,
            DATE,
            &instrument(),
            scale(),
            rows.chunks(777).map(|r| r.to_vec()),
        )
        .unwrap();
        let reader = SerializedFileReader::new(File::open(path).unwrap()).unwrap();
        let group = reader.get_row_group(0).unwrap();
        for column in 0..2 {
            let mut pages = group.get_column_page_reader(column).unwrap();
            let mut counts = Vec::new();
            while let Some(page) = pages.get_next_page().unwrap() {
                counts.push(page.num_values());
            }
            assert_eq!(counts, [8_192, 8_192, 3_616]);
        }
    }
}

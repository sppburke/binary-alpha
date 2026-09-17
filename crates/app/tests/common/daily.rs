//! Small synthetic generations built directly with codecs; no migration or external service.
use super::Scratch;
use binary_alpha_app::{archive, daily, store};
use binary_alpha_engine::dataset::daily::{DAY_MICROS, day_bounds};
use binary_alpha_engine::{dataset::*, market::*};
use sha2::{Digest, Sha256};
use std::{
    fs,
    path::{Path, PathBuf},
};

pub const FIRST: &str = "2026-09-17";
pub const SECOND: &str = "2026-09-18";
pub const LAST: &str = "2026-09-21";
pub fn micros(date: &str) -> i64 {
    day_bounds(date).unwrap().0
}
pub fn uri(path: &Path) -> String {
    format!("file://{}", path.display())
}
pub fn scale() -> PriceScale {
    4.try_into().unwrap()
}
pub fn date(time: i64) -> String {
    format_event_time_micros(time)[..10].to_string()
}

pub struct Pair {
    pub v1: GenerationManifest,
    pub v2: GenerationManifest,
    pub ticks: Vec<Tick>,
    pub bars: Vec<Bar<BarProviderColumns>>,
    pub pages: Vec<daily::PageOccurrence>,
}
impl Pair {
    pub fn path(&self, scratch: &Scratch, daily: bool) -> PathBuf {
        scratch
            .path("published")
            .join(if daily { self.v2.key() } else { self.v1.key() })
    }
    pub fn instrument(&self) -> String {
        let native = if self.bars.is_empty() {
            "{ kind = \"tick\" }"
        } else {
            "{ kind = \"bar\", period_seconds = 5 }"
        };
        format!(
            "\n[[instruments]]\nbroker = \"{}\"\nprovider_symbol = \"{}\"\nquote_currency = \"USD\"\nprice_scale = 4\nnative_granularity = {native}\ngap = {{ max_seconds = 2, reopen_seconds = 60 }}\ncandles = [{{ duration_seconds = 5, offset_seconds = 0 }}, {{ duration_seconds = 15, offset_seconds = 5 }}]\n",
            self.v1.broker, self.v1.provider_symbol
        )
    }
}

pub fn object(root: &Path, path: &str, role: ObjectRole, file: &Path) -> ObjectRecord {
    let id = store::identify(file).unwrap();
    let key = object_key(&id.sha256);
    fs::create_dir_all(root.join("objects")).unwrap();
    fs::copy(file, root.join(&key)).unwrap();
    ObjectRecord {
        role,
        path: path.into(),
        key,
        sha256: id.sha256,
        bytes: id.bytes,
        crc32c: Some(id.crc32c),
        generation: None,
    }
}
pub fn publish(root: &Path, manifest: &mut GenerationManifest) -> PathBuf {
    let id = InstrumentId {
        broker: manifest.broker.clone(),
        provider_symbol: manifest.provider_symbol.clone(),
    };
    let scale = match manifest.price_representation {
        PriceRepresentation::IntegerUnits { scale } => Some(scale),
        _ => None,
    };
    manifest.generation = generation_id_with_layout(
        &id,
        manifest.source_kind,
        manifest.role,
        scale,
        &manifest.objects,
        manifest.layout,
    );
    let path = root.join(manifest.key());
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    fs::write(&path, manifest.to_json()).unwrap();
    path
}
fn entry(
    date: &str,
    family: DayFamily,
    object: Option<String>,
    data: archive::DataSummary,
    state: DayState,
) -> DayInventoryEntry {
    let (start, end) = day_bounds(date).unwrap();
    DayInventoryEntry {
        date: date.into(),
        family,
        duration: None,
        offset: None,
        object,
        rows: data.rows,
        first_time: data.first_event_micros.map(format_event_time_micros),
        last_time: data.last_event_micros.map(format_event_time_micros),
        state,
        reason: matches!(state, DayState::Partial | DayState::Unknown)
            .then(|| "synthetic cutoff or historical gap".into()),
        unresolved: if state == DayState::Partial {
            vec![UnresolvedInterval {
                start: format_event_time_micros(data.last_event_micros.unwrap_or(start) + 1),
                end: format_event_time_micros(end),
            }]
        } else {
            vec![]
        },
    }
}
pub fn pair(scratch: &Scratch, pocket: bool) -> Pair {
    let root = scratch.path("published");
    fs::create_dir_all(&root).unwrap();
    let id = InstrumentId {
        broker: if pocket { "pocket_option" } else { "deriv" }
            .to_string()
            .try_into()
            .unwrap(),
        provider_symbol: if pocket { "AEDCNY_otc" } else { "R_50" }
            .to_string()
            .try_into()
            .unwrap(),
    };
    let times: Vec<_> = if pocket {
        (micros(SECOND) - 120_000_000..micros(SECOND) + 120_000_000)
            .step_by(5_000_000)
            .chain((micros(LAST)..micros(LAST) + 135_000_000).step_by(5_000_000))
            .collect()
    } else {
        (micros(SECOND) - 120_000_000..micros(SECOND))
            .step_by(1_000_000)
            .chain([micros(SECOND), micros(SECOND)])
            .chain((micros(SECOND) + 1_000_000..micros(SECOND) + 120_000_000).step_by(1_000_000))
            .chain([micros(SECOND) + DAY_MICROS - 1_000_000])
            .chain((micros(LAST)..micros(LAST) + 132_000_000).step_by(1_000_000))
            .collect()
    };
    let ticks: Vec<_> = if pocket {
        vec![]
    } else {
        times
            .iter()
            .map(|&t| Tick {
                event_time_micros: t,
                price_units: 18_000 + (t / 1_000_000).rem_euclid(47),
            })
            .collect()
    };
    let bars: Vec<_> = if !pocket {
        vec![]
    } else {
        times
            .iter()
            .map(|&t| {
                let units = 18_000 + (t / 1_000_000).rem_euclid(47);
                let open = units as f64 / 10_000.;
                Bar {
                    provider: BarProviderColumns {
                        symbol: id.provider_symbol.to_string(),
                        symbol_id: 538,
                        timestamp_utc: t,
                        server_time_s: t / 1_000_000 + 7200,
                    },
                    start_unix_s: t / 1_000_000,
                    open,
                    high: (units + 10) as f64 / 10_000.,
                    low: (units - 10) as f64 / 10_000.,
                    close: (units + 2) as f64 / 10_000.,
                    volume: 1.,
                    period_s: 5,
                }
            })
            .collect()
    };
    let payload = b"{\"fixture\":\"cross-midnight\"}".to_vec();
    let cross = daily::PageOccurrence {
        acquisition_id: "fixture-acquisition".into(),
        intent: None,
        ordinal: 0,
        checkpoint_ordinal: None,
        order_kind: daily::PageOrderKind::SourceFileOrder,
        payload_sha256: binary_alpha_engine::hex(&Sha256::digest(&payload)),
        payload,
        request_token: Some("opaque".into()),
        request_anchor_utc: Some(micros(SECOND) + 120_000_000),
        receipt_time_utc: None,
        receipt_state: daily::ReceiptState::AbsentInLegacyRecord,
        first_event_time: Some(times[0]),
        last_event_time: Some(micros(SECOND) + 115_000_000),
        rows: 2,
        checkpoint: None,
        disposition: daily::PageDisposition::Indexed,
    };
    let mut empty = cross.clone();
    empty.ordinal = 1;
    empty.rows = 0;
    empty.first_event_time = None;
    empty.last_event_time = None;
    empty.request_anchor_utc = Some(micros(LAST));
    empty.payload = b"[]".to_vec();
    empty.payload_sha256 = binary_alpha_engine::hex(&Sha256::digest(&empty.payload));
    let pages = vec![cross, empty];
    let file = scratch.path("daily-fixture.parquet");
    let provenance = scratch.path("coverage-fixture.json");
    fs::write(
        &provenance,
        b"{\"fixture\":true,\"complete\":false,\"unresolved\":[]}",
    )
    .unwrap();
    let coverage = object(
        &root,
        "provenance/coverage.json",
        ObjectRole::Provenance,
        &provenance,
    );
    let mut v1 = GenerationManifest {
        layout: None,
        day_inventory: vec![],
        schema_version: 1,
        generation: String::new(),
        broker: id.broker.clone(),
        provider_symbol: id.provider_symbol.clone(),
        instrument: id.to_string(),
        role: DatasetRole::Development,
        source_kind: if pocket {
            SourceKind::BarParquet
        } else {
            SourceKind::TickParquetDaily
        },
        native_granularity: if pocket {
            NativeGranularity::Bar { period_seconds: 5 }
        } else {
            NativeGranularity::Tick
        },
        time_unit: if pocket {
            TimeUnit::Second
        } else {
            TimeUnit::Microsecond
        },
        price_representation: if pocket {
            PriceRepresentation::BinaryFloat64
        } else {
            PriceRepresentation::IntegerUnits { scale: scale() }
        },
        coverage: Coverage {
            first_event_time: format_event_time_micros(times[0]),
            last_event_time: format_event_time_micros(*times.last().unwrap()),
        },
        row_count: times.len() as u64,
        capabilities: vec![if pocket {
            Capability::Bars
        } else {
            Capability::Ticks
        }],
        config_hash: "a".repeat(64),
        code_revision: "synthetic-fixture".into(),
        inputs: vec![],
        interval: pocket.then(|| IntervalContract::five_second("parquet_metadata")),
        objects: vec![coverage.clone()],
    };
    if pocket {
        archive::write_bars(
            &file,
            id.provider_symbol.as_str(),
            538,
            7200,
            bars.iter()
                .map(|b| Ok(daily::DailyBar::from(b.clone()).bar().unwrap())),
        )
        .unwrap();
    } else {
        archive::write_ticks(&file, &id, scale(), ticks.iter().copied().map(Ok)).unwrap();
    }
    v1.objects.push(object(
        &root,
        if pocket {
            "source/bars.parquet"
        } else {
            "normalized/ticks.parquet"
        },
        if pocket {
            ObjectRole::Source
        } else {
            ObjectRole::Normalized
        },
        &file,
    ));
    publish(&root, &mut v1);
    GenerationManifest::from_json(&v1.to_json()).unwrap();
    let mut v2 = v1.clone();
    v2.layout = Some(Layout::DailyV2);
    v2.objects = vec![coverage];
    for d in [FIRST, SECOND, "2026-09-19", "2026-09-20", LAST] {
        let (start, end) = day_bounds(d).unwrap();
        if d == "2026-09-19" || d == "2026-09-20" {
            v2.day_inventory.push(entry(
                d,
                DayFamily::Observations,
                None,
                archive::DataSummary::default(),
                DayState::EmptyKnown,
            ));
            continue;
        }
        let summary = if pocket {
            daily::write_bars(
                &file,
                d,
                [bars
                    .iter()
                    .filter(|b| (start..end).contains(&(b.start_unix_s * 1_000_000)))
                    .cloned()
                    .collect::<Vec<_>>()],
            )
            .unwrap()
        } else {
            daily::write_ticks(
                &file,
                d,
                &id,
                scale(),
                [ticks
                    .iter()
                    .filter(|t| (start..end).contains(&t.event_time_micros))
                    .copied()
                    .collect::<Vec<_>>()],
            )
            .unwrap()
        };
        let o = object(
            &root,
            &format!("observations/{d}.parquet"),
            ObjectRole::Normalized,
            &file,
        );
        v2.day_inventory.push(entry(
            d,
            DayFamily::Observations,
            Some(o.key.clone()),
            summary,
            if d == LAST {
                DayState::Partial
            } else if d == FIRST {
                DayState::Unknown
            } else {
                DayState::Complete
            },
        ));
        v2.objects.push(o);
    }
    for page in &pages {
        let d = date(page.partition_time().unwrap());
        let summary = daily::write_pages(&file, &d, [vec![page.clone()]]).unwrap();
        let o = object(
            &root,
            &format!("pages/{d}.parquet"),
            ObjectRole::Source,
            &file,
        );
        v2.day_inventory.push(entry(
            &d,
            DayFamily::Pages,
            Some(o.key.clone()),
            summary,
            DayState::Unknown,
        ));
        v2.objects.push(o);
    }
    write_coverage(scratch, &mut v2);
    // Object listing order is not chronological authority; the inventory is.
    v2.objects.reverse();
    publish(&root, &mut v2);
    GenerationManifest::from_json(&v2.to_json()).unwrap();
    Pair {
        v1,
        v2,
        ticks,
        bars,
        pages,
    }
}

/// Produce a valid daily Parquet envelope with one changed payload byte and its OLD per-page
/// digest. Re-hashing the outer object lets verification reach the independent payload check.
pub fn flip_page_payload(source: &Path, target: &Path) {
    use parquet::{
        basic::{Compression, Encoding, Repetition, ZstdLevel},
        column::writer::ColumnWriter,
        data_type::ByteArray,
        file::{
            properties::{EnabledStatistics, WriterProperties},
            reader::{FileReader, SerializedFileReader},
            writer::SerializedFileWriter,
        },
        record::Field,
    };
    use std::{fs::File, sync::Arc};
    let reader = SerializedFileReader::new(File::open(source).unwrap()).unwrap();
    let metadata = reader.metadata().file_metadata();
    let schema = metadata.schema_descr().root_schema_ptr();
    let props = WriterProperties::builder()
        .set_created_by("daily-parquet-v1".into())
        .set_compression(Compression::ZSTD(ZstdLevel::try_new(3).unwrap()))
        .set_dictionary_enabled(false)
        .set_encoding(Encoding::PLAIN)
        .set_statistics_enabled(EnabledStatistics::None)
        .set_key_value_metadata(metadata.key_value_metadata().cloned())
        .build();
    let mut rows: Vec<_> = reader
        .get_row_iter(None)
        .unwrap()
        .map(|r| r.unwrap().into_columns())
        .collect();
    let Field::Bytes(payload) = &mut rows[0][6].1 else {
        panic!("payload binary")
    };
    let mut bytes = payload.data().to_vec();
    bytes[0] ^= 1;
    *payload = ByteArray::from(bytes);
    let mut writer = SerializedFileWriter::new(
        File::create(target).unwrap(),
        schema.clone(),
        Arc::new(props),
    )
    .unwrap();
    let mut group = writer.next_row_group().unwrap();
    let mut index = 0;
    while let Some(mut column) = group.next_column().unwrap() {
        let fields: Vec<_> = rows.iter().map(|r| &r[index].1).collect();
        let levels: Vec<i16> = fields
            .iter()
            .map(|f| i16::from(!matches!(f, Field::Null)))
            .collect();
        let optional =
            schema.get_fields()[index].get_basic_info().repetition() == Repetition::OPTIONAL;
        match column.untyped() {
            ColumnWriter::Int64ColumnWriter(w) => {
                let values: Vec<_> = fields
                    .iter()
                    .filter_map(|f| match f {
                        Field::Null => None,
                        Field::Long(n) | Field::TimestampMicros(n) => Some(*n),
                        Field::ULong(n) => Some(*n as i64),
                        _ => panic!("int64"),
                    })
                    .collect();
                w.write_batch(&values, optional.then_some(&levels), None)
                    .unwrap();
            }
            ColumnWriter::ByteArrayColumnWriter(w) => {
                let values: Vec<_> = fields
                    .iter()
                    .filter_map(|f| match f {
                        Field::Null => None,
                        Field::Str(s) => Some(ByteArray::from(s.as_str())),
                        Field::Bytes(b) => Some(b.clone()),
                        _ => panic!("bytes"),
                    })
                    .collect();
                w.write_batch(&values, optional.then_some(&levels), None)
                    .unwrap();
            }
            _ => panic!("page physical type"),
        }
        column.close().unwrap();
        index += 1;
    }
    group.close().unwrap();
    writer.close().unwrap();
}

/// Synthetic acquisition claims for fixtures; production never infers proof from inventory.
pub fn write_coverage(scratch: &Scratch, manifest: &mut GenerationManifest) {
    use binary_alpha_engine::dataset::coverage::*;
    let mut acquisitions = Vec::new();
    let mut days = Vec::new();
    for day in &mut manifest.day_inventory {
        let (start, end) = day_bounds(&day.date).unwrap();
        if day.state == DayState::Unknown {
            day.unresolved = vec![UnresolvedInterval {
                start: format_event_time_micros(start),
                end: format_event_time_micros(end),
            }];
        }
        let unresolved: Vec<_> = day
            .unresolved
            .iter()
            .map(|i| CoverageRange {
                start: i.start.clone(),
                end: i.end.clone(),
            })
            .collect();
        let mut verified = Vec::new();
        let mut cursor = start;
        for interval in &unresolved {
            let (from, to) = interval.bounds().unwrap();
            if cursor < from {
                verified.push(CoverageRange::new(cursor, from));
            }
            cursor = to;
        }
        if cursor < end {
            verified.push(CoverageRange::new(cursor, end));
        }
        let id = format!("fixture-{}-{}", day.family, day.date);
        acquisitions.push(AcquisitionCoverage {
            acquisition_id: id.clone(),
            source_identity: "synthetic-day-evidence".into(),
            requested: vec![CoverageRange::new(start, end)],
            verified: verified.clone(),
            shortfalls: unresolved
                .iter()
                .map(|range| CoverageShortfall {
                    reason: day.reason.clone().unwrap(),
                    unresolved: range.clone(),
                })
                .collect(),
            unresolved: unresolved.clone(),
        });
        days.push(DayCoverage {
            date: day.date.clone(),
            family: day.family,
            acquisition_ids: vec![id],
            basis: "synthetic fixture acquisition claim; no external source".into(),
            verified,
            unresolved,
            reason: day.reason.clone(),
        });
    }
    let coverage = DailyCoverage {
        schema_version: 2,
        broker: manifest.broker.clone(),
        provider_symbol: manifest.provider_symbol.clone(),
        role: manifest.role,
        native_granularity: manifest.native_granularity,
        acquisitions,
        days,
    };
    coverage.check_manifest(manifest).unwrap();
    let file = scratch.path("typed-coverage.json");
    fs::write(&file, coverage.to_json()).unwrap();
    *manifest
        .objects
        .iter_mut()
        .find(|o| o.path == "provenance/coverage.json")
        .unwrap() = object(
        &scratch.path("published"),
        "provenance/coverage.json",
        ObjectRole::Provenance,
        &file,
    );
}

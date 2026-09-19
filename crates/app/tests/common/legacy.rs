//! Explicit historical fixtures. These writers are compiled only into integration tests;
//! production commands never gain a legacy publication capability.
use binary_alpha_app::{archive, daily, store::Store};
use binary_alpha_engine::{config::Config, dataset::*, market::*, stream::*};
use std::{fs, path::Path};

pub fn stream(config: &Path, root: &Path, manifest: &GenerationManifest) -> StreamManifest {
    assert!(manifest.layout.is_none());
    let config = Config::parse(&fs::read_to_string(config).unwrap()).unwrap();
    let id = InstrumentId {
        broker: manifest.broker.clone(),
        provider_symbol: manifest.provider_symbol.clone(),
    };
    let definition = config.instrument(&id, manifest.native_granularity).unwrap();
    let mut engine = InstrumentStream::new(definition, Source::from_manifest(manifest)).unwrap();
    let mut candles: Vec<Vec<Candle>> = definition.candles.iter().map(|_| vec![]).collect();
    let mut output = Vec::new();
    daily::read_generation(&Store::filesystem(root), manifest, |row| {
        engine
            .push(row.observation(definition.price_scale)?, &mut output)
            .map_err(|e| e.to_string())?;
        for (index, candle) in output.drain(..) {
            candles[index].push(candle);
        }
        Ok(())
    })
    .unwrap();
    let profile = engine.profile();
    assert_eq!(profile.observations, manifest.row_count);
    assert_eq!(profile.coverage.as_ref(), Some(&manifest.coverage));
    let temporary = root.join("fixture-stream.parquet");
    let mut objects = Vec::new();
    let mut summaries = Vec::new();
    for (spec, rows) in definition.candles.iter().zip(candles) {
        let mut writer = archive::CandleWriter::create(
            &temporary,
            &id,
            definition.price_scale,
            spec.duration_seconds,
            spec.offset_seconds,
        )
        .unwrap();
        for candle in rows {
            writer.push(&candle).unwrap();
        }
        let (rows, first, last) = writer.finish().unwrap();
        objects.push(super::daily::object(
            root,
            &StreamSummary::object_path(spec.duration_seconds, spec.offset_seconds),
            ObjectRole::Normalized,
            &temporary,
        ));
        summaries.push(StreamSummary {
            duration_seconds: spec.duration_seconds,
            offset_seconds: spec.offset_seconds,
            rows,
            first_open_time: first.map(format_event_time_micros),
            last_close_time: last.map(format_event_time_micros),
        });
    }
    fs::write(&temporary, profile.to_json()).unwrap();
    objects.insert(
        0,
        super::daily::object(
            root,
            PROFILE_OBJECT_PATH,
            ObjectRole::Normalized,
            &temporary,
        ),
    );
    fs::remove_file(temporary).unwrap();
    let stream = StreamManifest {
        layout: None,
        day_inventory: vec![],
        kind: STREAM_MANIFEST_KIND.into(),
        schema_version: STREAM_SCHEMA_VERSION,
        generation: stream_generation_id_with_layout(
            &manifest.generation,
            &definition.canonical_toml(),
            None,
        ),
        broker: id.broker.clone(),
        provider_symbol: id.provider_symbol.clone(),
        instrument: id.to_string(),
        role: manifest.role,
        source_generation: manifest.generation.clone(),
        source_manifest_uri: None,
        source_kind: manifest.source_kind,
        definition: definition.clone(),
        config_hash: config.content_hash(),
        code_revision: "synthetic-legacy-fixture".into(),
        observations: profile.observations,
        coverage: profile.coverage.clone(),
        streams: summaries,
        objects,
    };
    StreamManifest::from_json(&stream.to_json()).unwrap();
    let path = root.join(manifest_key(&stream.generation));
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    fs::write(path, stream.to_json()).unwrap();
    stream
}

/// Materialize an historical dataset from a synthetic current fixture. Original import
/// bytes come from its generated source inventory; history pages come from decoded exact
/// response occurrences. No production writer is asked to publish v1.
pub fn dataset(root: &Path, current: &GenerationManifest) -> GenerationManifest {
    use binary_alpha_app::fetch::{
        HistoryCoverage, ObjectIdentitySummary, OccurrenceIdentity, PageCoverage,
    };
    use serde_json::{Value, json};
    let value = |path: &str| -> Value {
        let o = current.objects.iter().find(|o| o.path == path).unwrap();
        serde_json::from_slice(&fs::read(root.join(&o.key)).unwrap()).unwrap()
    };
    assert_eq!(current.layout, Some(Layout::DailyV2));
    let lineage = value("provenance/lineage.json");
    let mut manifest = current.clone();
    manifest.layout = None;
    manifest.day_inventory.clear();
    manifest.objects.clear();
    if current.source_kind != SourceKind::BrokerHistory {
        for original in lineage["original_objects"].as_array().unwrap() {
            let object: ObjectRecord = serde_json::from_value(original["object"].clone()).unwrap();
            let input = current
                .inputs
                .iter()
                .find(|i| i.sha256 == object.sha256)
                .unwrap();
            fs::copy(&input.path, root.join(&object.key)).unwrap();
            manifest.objects.push(object);
        }
    }
    let temporary = root.join("fixture-history-object");
    if current.source_kind == SourceKind::BrokerHistory
        || current.native_granularity == NativeGranularity::Tick
    {
        let mut ticks = vec![];
        let mut bars = vec![];
        daily::read_generation_lossless(&Store::filesystem(root), current, |row| {
            match row {
                daily::LosslessRow::Tick(t) => ticks.push(t),
                daily::LosslessRow::Bar(b) => bars.push(b),
            }
            Ok(())
        })
        .unwrap();
        let path = if let PriceRepresentation::IntegerUnits { scale } = current.price_representation
        {
            archive::write_ticks(
                &temporary,
                &InstrumentId {
                    broker: current.broker.clone(),
                    provider_symbol: current.provider_symbol.clone(),
                },
                scale,
                ticks.into_iter().map(Ok),
            )
            .unwrap();
            "normalized/ticks.parquet"
        } else {
            // The legacy native archive has the same eleven nullable provider columns.
            let rows: Vec<_> = bars
                .iter()
                .map(|b| super::BarRow {
                    symbol: b.symbol.clone().unwrap(),
                    symbol_id: b.symbol_id.unwrap(),
                    unix: b.unix_utc_s.unwrap(),
                    server: b.server_time_s,
                    timestamp: b.timestamp_utc,
                    ohlcv: [
                        b.open.unwrap(),
                        b.high.unwrap(),
                        b.low.unwrap(),
                        b.close.unwrap(),
                        b.volume.unwrap(),
                    ],
                    period: i32::from(b.period_s.unwrap()),
                })
                .collect();
            super::write_bar_file(&temporary, &rows, Some(&super::INTERVAL), super::BAR_SCHEMA);
            "normalized/bars.parquet"
        };
        manifest.objects.push(super::daily::object(
            root,
            path,
            ObjectRole::Normalized,
            &temporary,
        ));
    }
    if current.source_kind == SourceKind::BrokerHistory {
        let doc = value("provenance/coverage.json");
        let acquisition = doc["acquisitions"]
            .as_array()
            .unwrap()
            .iter()
            .find(|a| a["acquisition_id"] == lineage["continuation"]["acquisition_id"])
            .unwrap();
        let mut coverage: HistoryCoverage = serde_json::from_value(json!({
            "schema_version":1,"source_identity":acquisition["source_identity"],"broker":current.broker,
            "provider_symbol":current.provider_symbol,"role":current.role,
            "requested":acquisition["requested"][0],"verified":acquisition["verified"].as_array().unwrap().first(),
            "actual":{"first":current.coverage.first_event_time,"last":current.coverage.last_event_time},
            "rows":current.row_count,"pages":[],"shortfall":acquisition["shortfalls"].as_array().unwrap().first(),
            "tail_shortfall":acquisition["shortfalls"].as_array().unwrap().get(1),
            "native_granularity":current.native_granularity,"seed":lineage["continuation"]["seed"]
        })).unwrap();
        let mut pages: Vec<_> = current
            .day_inventory
            .iter()
            .filter(|d| d.family == DayFamily::Pages)
            .flat_map(|d| {
                daily::read_pages(&root.join(d.object.as_ref().unwrap()), &d.date).unwrap()
            })
            .filter(|p| {
                p.order_kind == daily::PageOrderKind::RequestOrder
                    && p.disposition == daily::PageDisposition::Indexed
            })
            .collect();
        pages.sort_by(|a, b| (&a.acquisition_id, a.ordinal).cmp(&(&b.acquisition_id, b.ordinal)));
        let mut bundle = vec![];
        for p in pages {
            fs::write(root.join(object_key(&p.payload_sha256)), &p.payload).unwrap();
            coverage.pages.push(PageCoverage {
                occurrence: Some(OccurrenceIdentity {
                    acquisition_id: p.acquisition_id,
                    intent: p.intent,
                    ordinal: p.ordinal,
                }),
                path: "raw/pages.bin".into(),
                sha256: p.payload_sha256,
                bytes: p.payload.len() as u64,
                offset: Some(bundle.len() as u64),
                anchor: p.request_token,
                rows: p.rows,
                first: p.first_event_time.map(format_event_time_micros),
                last: p.last_event_time.map(format_event_time_micros),
                receipt_time: p.receipt_time_utc.map(format_event_time_micros),
            });
            bundle.extend(p.payload);
        }
        fs::write(&temporary, bundle).unwrap();
        let object = super::daily::object(root, "raw/pages.bin", ObjectRole::Source, &temporary);
        coverage.bundle = Some(ObjectIdentitySummary {
            sha256: object.sha256.clone(),
            bytes: object.bytes,
        });
        manifest.objects.push(object);
        fs::write(&temporary, serde_json::to_vec(&coverage).unwrap()).unwrap();
        manifest.objects.push(super::daily::object(
            root,
            "provenance/coverage.json",
            ObjectRole::Provenance,
            &temporary,
        ));
    }
    if temporary.exists() {
        fs::remove_file(temporary).unwrap();
    }
    super::daily::publish(root, &mut manifest);
    GenerationManifest::from_json(&manifest.to_json()).unwrap();
    manifest
}

pub fn import(config: &Path) -> Result<Vec<String>, String> {
    use std::collections::BTreeSet;
    let core = Config::parse(&fs::read_to_string(config).unwrap()).map_err(|e| e.to_string())?;
    let root = config
        .parent()
        .unwrap()
        .join(core.storage.historical_data_dir.as_path());
    let existing: BTreeSet<_> = fs::read_dir(root.join("objects"))
        .into_iter()
        .flatten()
        .map(|e| format!("objects/{}", e.unwrap().file_name().to_string_lossy()))
        .collect();
    let report = super::command(&["data", "import", "--config", config.to_str().unwrap()])?;
    let mut result = Vec::new();
    let mut temporary_keys = BTreeSet::new();
    for line in report {
        let generation = super::generation(&line);
        let current =
            GenerationManifest::from_json(&fs::read(root.join(manifest_key(&generation))).unwrap())
                .unwrap();
        let old = dataset(&root, &current);
        temporary_keys.extend(current.objects.into_iter().map(|o| o.key));
        fs::remove_dir_all(root.join(manifest_key(&generation)).parent().unwrap()).unwrap();
        result.push(line.replace(&generation, &old.generation));
    }
    let retained: BTreeSet<_> = Store::filesystem(&root)
        .list_manifests()
        .unwrap()
        .into_iter()
        .flat_map(|id| {
            let value: serde_json::Value =
                serde_json::from_slice(&fs::read(root.join(manifest_key(&id))).unwrap()).unwrap();
            value["objects"]
                .as_array()
                .unwrap()
                .iter()
                .map(|o| o["key"].as_str().unwrap().to_string())
                .collect::<Vec<_>>()
        })
        .collect();
    for key in temporary_keys
        .difference(&retained)
        .filter(|key| !existing.contains(*key))
    {
        fs::remove_file(root.join(key)).unwrap();
    }
    Ok(result)
}

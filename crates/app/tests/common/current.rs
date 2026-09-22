//! Synthetic datasets for consumer tests. Development fixtures use the current daily codecs;
//! evaluation and holdout remain direct legacy fixtures because daily-v2 is development-only.
//! CLI import/refusal tests continue to use `common::import`, without this adaptation.
use std::{collections::BTreeMap, fs, path::Path};

use binary_alpha_app::{archive, store};
use binary_alpha_engine::{
    config::{Config, PublicationUri, Source},
    dataset::*,
    market::*,
};

/// Write invented rows directly, preserving sequence and multiplicity. Non-development roles
/// are reader fixtures; no production command is asked to publish a legacy generation.
pub fn ticks(
    root: &Path,
    id: &InstrumentId,
    role: DatasetRole,
    scale: PriceScale,
    rows: &[Tick],
) -> Result<GenerationManifest, String> {
    ticks_with_source(root, id, role, scale, rows, None)
}

/// Bind original synthetic source bytes as well as observations. Equivalent numeric rows in
/// byte-distinct source files remain different generations, including holdout alias fixtures.
pub fn ticks_with_source(
    root: &Path,
    id: &InstrumentId,
    role: DatasetRole,
    scale: PriceScale,
    rows: &[Tick],
    source: Option<&Path>,
) -> Result<GenerationManifest, String> {
    let first = rows.first().ok_or("tick fixture is empty")?;
    let last = rows.last().unwrap();
    let mut sequence = TickSequence::default();
    for tick in rows {
        sequence.accept(*tick)?;
    }
    fs::create_dir_all(root).map_err(|e| e.to_string())?;
    let mut manifest = GenerationManifest {
        layout: (role == DatasetRole::Development).then_some(Layout::DailyV2),
        day_inventory: vec![],
        schema_version: 1,
        generation: String::new(),
        broker: id.broker.clone(),
        provider_symbol: id.provider_symbol.clone(),
        instrument: id.to_string(),
        role,
        source_kind: SourceKind::TickParquetDaily,
        native_granularity: NativeGranularity::Tick,
        time_unit: TimeUnit::Microsecond,
        price_representation: PriceRepresentation::IntegerUnits { scale },
        coverage: Coverage {
            first_event_time: format_event_time_micros(first.event_time_micros),
            last_event_time: format_event_time_micros(last.event_time_micros),
        },
        row_count: rows.len() as u64,
        capabilities: vec![Capability::Ticks],
        config_hash: "a".repeat(64),
        code_revision: "synthetic-consumer-fixture".into(),
        inputs: vec![],
        interval: None,
        objects: vec![],
    };
    let temporary = root.join(".fixture-ticks.parquet");
    if manifest.layout.is_some() {
        let mut days: BTreeMap<String, Vec<Tick>> = BTreeMap::new();
        for tick in rows {
            days.entry(super::daily::date(tick.event_time_micros))
                .or_default()
                .push(*tick);
        }
        for (date, rows) in days {
            let summary =
                binary_alpha_app::daily::write_ticks(&temporary, &date, id, scale, [rows])?;
            let object = super::daily::object(
                root,
                &format!("observations/{date}.parquet"),
                ObjectRole::Normalized,
                &temporary,
            );
            manifest.day_inventory.push(DayInventoryEntry {
                date,
                family: DayFamily::Observations,
                duration: None,
                offset: None,
                object: Some(object.key.clone()),
                rows: summary.rows,
                first_time: summary.first_event_micros.map(format_event_time_micros),
                last_time: summary.last_event_micros.map(format_event_time_micros),
                state: DayState::Unknown,
                reason: Some("invented consumer fixture; no acquisition completeness claim".into()),
                unresolved: vec![],
            });
            manifest.objects.push(object);
        }
        super::daily::write_coverage_at(root, &mut manifest);
    } else {
        archive::write_ticks(&temporary, id, scale, rows.iter().copied().map(Ok))?;
        manifest.objects.push(super::daily::object(
            root,
            "normalized/ticks.parquet",
            ObjectRole::Normalized,
            &temporary,
        ));
    }
    if let Some(source) = source {
        let identity = store::identify(source)?;
        manifest.inputs.push(Input {
            path: source.to_string_lossy().into_owned(),
            bytes: identity.bytes,
            sha256: identity.sha256.clone(),
        });
        if manifest.layout.is_some() {
            fs::write(
                &temporary,
                serde_json::to_vec(&serde_json::json!({
                    "schema_version": 1,
                    "kind": "synthetic_fixture",
                    "source": { "sha256": identity.sha256, "bytes": identity.bytes }
                }))
                .map_err(|e| e.to_string())?,
            )
            .map_err(|e| e.to_string())?;
            manifest.objects.push(super::daily::object(
                root,
                "provenance/lineage.json",
                ObjectRole::Provenance,
                &temporary,
            ));
        } else {
            manifest.objects.push(super::daily::object(
                root,
                "ticks.csv",
                ObjectRole::Source,
                source,
            ));
        }
    } else if manifest.layout.is_none() {
        // Legacy manifests require retained source/provenance in addition to normalization.
        fs::write(&temporary, b"{\"synthetic_rows\":true}").map_err(|e| e.to_string())?;
        manifest.objects.push(super::daily::object(
            root,
            "provenance/source.json",
            ObjectRole::Provenance,
            &temporary,
        ));
    }
    fs::remove_file(temporary).map_err(|e| e.to_string())?;
    super::daily::publish(root, &mut manifest);
    GenerationManifest::from_json(&manifest.to_json())?;
    Ok(manifest)
}

/// Adapts only downstream tests' invented CSV/non-development sources. Supported development
/// archive inputs still exercise the real importer. This helper is explicitly imported by
/// consumer suites, and never changes the CLI helper used by importer tests.
pub fn import(path: &Path) -> Result<Vec<String>, String> {
    let config = Config::parse(&fs::read_to_string(path).map_err(|e| e.to_string())?)
        .map_err(|e| e.to_string())?;
    let sources = &config
        .import
        .as_ref()
        .ok_or("fixture has no import sources")?
        .sources;
    if sources.iter().all(|source| {
        !matches!(source, Source::TickCsv { .. }) && source.role() == DatasetRole::Development
    }) {
        return super::import(path);
    }
    import_config(&config, path.parent().unwrap())
}

/// Publishes invented consumer inputs from typed settings, including synthetic holdout bars.
pub fn import_config(config: &Config, base: &Path) -> Result<Vec<String>, String> {
    let sources = &config
        .import
        .as_ref()
        .ok_or("fixture has no import sources")?
        .sources;
    let PublicationUri::Filesystem(root) = &config.storage.publication_uri else {
        return Err("consumer fixtures require a local publication directory".into());
    };
    let retained = base.join(config.storage.historical_data_dir.as_path());
    fs::create_dir_all(root).map_err(|e| e.to_string())?;
    let mut manifests = Vec::new();
    for source in sources {
        match source {
            Source::TickCsv {
                path,
                broker,
                role,
                provider_symbol,
                source_symbol,
                price_scale,
            } => {
                let source_path = base.join(path.as_path());
                let text = fs::read_to_string(&source_path).map_err(|e| e.to_string())?;
                let mut lines = text.lines();
                if lines.next() != Some(TICK_HEADER) {
                    return Err("fixture CSV header differs from the native tick header".into());
                }
                let rows = lines
                    .map(|line| parse_tick_line(line, source_symbol, *price_scale))
                    .collect::<Result<Vec<_>, _>>()?;
                let id = InstrumentId {
                    broker: broker.clone(),
                    provider_symbol: provider_symbol.clone(),
                };
                manifests.push(ticks_with_source(
                    root,
                    &id,
                    *role,
                    *price_scale,
                    &rows,
                    Some(&source_path),
                )?);
            }
            Source::BarParquetCollection {
                path,
                broker,
                role,
                manifest,
                instruments,
                ..
            } if *role != DatasetRole::Development => {
                let source_root = base.join(path.as_path());
                let collection: serde_json::Value = serde_json::from_slice(
                    &fs::read(source_root.join(manifest)).map_err(|e| e.to_string())?,
                )
                .map_err(|e| e.to_string())?;
                let assets = collection["assets"]
                    .as_object()
                    .ok_or("fixture collection assets absent")?;
                for (symbol, asset) in assets {
                    if instruments
                        .as_ref()
                        .is_some_and(|selected| !selected.contains(symbol))
                    {
                        continue;
                    }
                    let id = InstrumentId {
                        broker: broker.clone(),
                        provider_symbol: symbol.clone().try_into()?,
                    };
                    let expectation = archive::BarExpectation {
                        symbol: symbol.clone(),
                        symbol_id: asset["symbol_id"]
                            .as_i64()
                            .map(i32::try_from)
                            .transpose()
                            .map_err(|e| e.to_string())?,
                        period_s: 5,
                        server_offset_s: collection["server_timestamp_offset_seconds"].as_i64(),
                        interval: IntervalContract::five_second("parquet_metadata"),
                        metadata_required: true,
                    };
                    let mut objects = vec![];
                    let mut inputs = vec![];
                    let mut summary = archive::DataSummary::default();
                    for entry in asset["parquet_files"]
                        .as_array()
                        .ok_or("fixture files absent")?
                    {
                        let relative = entry["path"].as_str().ok_or("fixture file path absent")?;
                        let file = source_root.join(symbol).join("dataset").join(relative);
                        let decoded = archive::validate_bar_file(&file, &expectation)?;
                        summary.extend(&decoded.data);
                        let identity = store::identify(&file)?;
                        inputs.push(Input {
                            path: file.to_string_lossy().into_owned(),
                            bytes: identity.bytes,
                            sha256: identity.sha256,
                        });
                        objects.push(super::daily::object(
                            root,
                            &format!("source/{relative}"),
                            ObjectRole::Source,
                            &file,
                        ));
                    }
                    let (first, last) = archive::coverage(&summary)?;
                    let mut manifest = GenerationManifest {
                        layout: None,
                        day_inventory: vec![],
                        schema_version: 1,
                        generation: String::new(),
                        broker: id.broker.clone(),
                        provider_symbol: id.provider_symbol.clone(),
                        instrument: id.to_string(),
                        role: *role,
                        source_kind: SourceKind::BarParquet,
                        native_granularity: NativeGranularity::Bar { period_seconds: 5 },
                        time_unit: TimeUnit::Second,
                        price_representation: PriceRepresentation::BinaryFloat64,
                        coverage: Coverage {
                            first_event_time: first,
                            last_event_time: last,
                        },
                        row_count: summary.rows,
                        capabilities: vec![Capability::Bars],
                        config_hash: config.content_hash(),
                        code_revision: "synthetic-consumer-fixture".into(),
                        inputs,
                        interval: Some(expectation.interval),
                        objects,
                    };
                    super::daily::publish(root, &mut manifest);
                    GenerationManifest::from_json(&manifest.to_json())?;
                    manifests.push(manifest);
                }
            }
            _ => return Err("unsupported mixed consumer-fixture source configuration".into()),
        }
    }
    let mut report = vec![];
    for manifest in manifests {
        if &retained != root {
            fs::create_dir_all(retained.join("objects")).map_err(|e| e.to_string())?;
            for object in &manifest.objects {
                fs::copy(root.join(&object.key), retained.join(&object.key))
                    .map_err(|e| e.to_string())?;
            }
            let target = retained.join(manifest.key());
            fs::create_dir_all(target.parent().unwrap()).map_err(|e| e.to_string())?;
            fs::copy(root.join(manifest.key()), target).map_err(|e| e.to_string())?;
        }
        report.push(format!(
            "published {} {} generation {} rows {}",
            manifest.instrument, manifest.role, manifest.generation, manifest.row_count
        ));
    }
    Ok(report)
}

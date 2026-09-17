//! Daily continuation selection and publication. The ready manifest owns the complete closure;
//! a descendant carries existing day keys and changes only partitions with additional rows.

use crate::{
    archive::DataSummary,
    daily::{self, DailyBar, PageOccurrence},
    fetch::{HistoryCoverage, PageCoverage},
    import,
    store::{self, Store},
    verify,
};
use binary_alpha_engine::{
    dataset::{daily::day_bounds, *},
    market::{
        InstrumentId, Tick, format_event_time_micros as text, parse_event_time_micros as time,
    },
    research::Access,
};
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::Path,
};

pub const LINEAGE_PATH: &str = "provenance/lineage.json";
fn err(e: impl std::fmt::Display) -> String {
    e.to_string()
}
fn date(t: i64) -> String {
    text(t)[..10].into()
}

/// Select the unique daily root, including migrated broker-history roots. A lineage's root
/// remains the effective seed even when its newest descendant has a later coverage end.
pub(crate) fn root(
    local: &Store,
    instrument: &str,
    role: DatasetRole,
    access: Access<'_>,
) -> Result<Option<String>, String> {
    let candidates = match access.declaration {
        Some(d) => d
            .populations
            .iter()
            .filter(|p| p.instrument == instrument && p.role == role)
            .flat_map(|p| p.generations.clone())
            .collect(),
        None => local.list_manifests()?,
    };
    let mut roots = BTreeSet::new();
    for generation in candidates {
        let key = manifest_key(&generation);
        if local.head(&key)?.is_none() {
            continue;
        }
        let mut bytes = Vec::new();
        local.read_to(&key, None, &mut bytes)?;
        if verify::manifest_kind(&bytes)?.is_some() {
            continue;
        }
        let manifest = GenerationManifest::from_json(&bytes)?;
        if manifest.layout != Some(Layout::DailyV2)
            || manifest.instrument != instrument
            || manifest.role != role
        {
            continue;
        }
        access.permit(Some(role), &generation)?;
        let mut parent = false;
        if let Some(o) = manifest.objects.iter().find(|o| o.path == LINEAGE_PATH) {
            let (_, file) = verify::fetch(local, o, true)?;
            let lineage: Value =
                serde_json::from_slice(&fs::read(&file.expect("lineage").path).map_err(err)?)
                    .map_err(err)?;
            parent = lineage
                .get("parent_generation")
                .and_then(Value::as_str)
                .is_some();
            if let Some(generation) = lineage.get("parent_generation").and_then(Value::as_str) {
                let key = manifest_key(generation);
                if local.head(&key)?.is_some() {
                    let mut bytes = Vec::new();
                    local.read_to(&key, None, &mut bytes)?;
                    parent = GenerationManifest::from_json(&bytes)?.layout == Some(Layout::DailyV2);
                } else if lineage
                    .get("replaced_generations")
                    .and_then(Value::as_array)
                    .is_some_and(|old| old.iter().any(|v| v.as_str() == Some(generation)))
                {
                    parent = false;
                }
            }
            if lineage
                .get("root_generation")
                .and_then(Value::as_str)
                .is_some_and(|r| r != manifest.generation)
            {
                parent = true;
            }
        }
        if !parent && manifest.source_kind == SourceKind::BrokerHistory {
            let o = manifest
                .objects
                .iter()
                .find(|o| o.path == crate::fetch::COVERAGE_PATH)
                .ok_or("missing daily coverage")?;
            let (_, file) = verify::fetch(local, o, true)?;
            let coverage: Value =
                serde_json::from_slice(&fs::read(&file.expect("coverage").path).map_err(err)?)
                    .map_err(err)?;
            // A migration root may retain the legacy seed in its historical coverage. Only a
            // daily seed makes this a descendant; no legacy manifest need remain readable.
            if let Some(seed) = coverage.pointer("/seed/generation").and_then(Value::as_str) {
                let key = manifest_key(seed);
                if local.head(&key)?.is_some() {
                    let mut bytes = Vec::new();
                    local.read_to(&key, None, &mut bytes)?;
                    parent = GenerationManifest::from_json(&bytes)?.layout == Some(Layout::DailyV2);
                }
            }
        }
        if !parent {
            roots.insert(generation);
        }
    }
    match roots.len() {
        0 => Ok(None),
        1 => Ok(roots.into_iter().next()),
        _ => Err(format!(
            "{instrument}: multiple daily continuation roots; an unambiguous migration binding is required"
        )),
    }
}

pub(crate) enum Observation {
    Tick(Tick),
    Bar(DailyBar),
}
impl Observation {
    fn time(&self) -> Result<i64, String> {
        match self {
            Self::Tick(t) => Ok(t.event_time_micros),
            Self::Bar(b) => b
                .unix_utc_s
                .and_then(|s| s.checked_mul(1_000_000))
                .ok_or_else(|| "bar time overflow".into()),
        }
    }
}
fn retain_file(
    local: &Store,
    path: &Path,
    logical: &str,
    role: ObjectRole,
) -> Result<ObjectRecord, String> {
    let id = store::identify(path)?;
    local.put_new(&object_key(&id.sha256), path, &id)?;
    fs::remove_file(path).map_err(err)?;
    Ok(import::record(role, logical, &id))
}
fn metadata(
    local: &Store,
    logical: &str,
    value: &impl serde::Serialize,
) -> Result<ObjectRecord, String> {
    let id = import::retain_bytes(local, &crate::fetch::json_bytes(value)?, "daily-metadata")?;
    Ok(import::record(ObjectRole::Provenance, logical, &id))
}
fn entry(
    date: &str,
    family: DayFamily,
    object: &ObjectRecord,
    data: DataSummary,
) -> DayInventoryEntry {
    DayInventoryEntry {
        date: date.into(),
        family,
        duration: None,
        offset: None,
        object: Some(object.key.clone()),
        rows: data.rows,
        first_time: data.first_event_micros.map(text),
        last_time: data.last_event_micros.map(text),
        state: DayState::Unknown,
        reason: Some("source evidence without whole-day completeness".into()),
        unresolved: vec![],
    }
}
fn old_object<'a>(
    manifest: &'a GenerationManifest,
    day: &DayInventoryEntry,
) -> Result<&'a ObjectRecord, String> {
    manifest
        .objects
        .iter()
        .find(|o| {
            Some(&o.key) == day.object.as_ref() && day.logical_path().is_ok_and(|p| p == o.path)
        })
        .ok_or_else(|| "daily inventory object absent".into())
}
fn observations(
    local: &Store,
    manifest: &mut GenerationManifest,
    baseline: Option<&GenerationManifest>,
    rows: Vec<Observation>,
) -> Result<(), String> {
    let id = InstrumentId {
        broker: manifest.broker.clone(),
        provider_symbol: manifest.provider_symbol.clone(),
    };
    let mut grouped: BTreeMap<String, Vec<Observation>> = BTreeMap::new();
    for row in rows {
        grouped.entry(date(row.time()?)).or_default().push(row);
    }
    if let Some(old) = baseline {
        for d in old
            .day_inventory
            .iter()
            .filter(|d| d.family == DayFamily::Observations)
        {
            if !grouped.contains_key(&d.date) {
                if d.object.is_some() {
                    manifest.objects.push(old_object(old, d)?.clone());
                }
                manifest.day_inventory.push(d.clone());
            }
        }
    }
    for (date, rows) in grouped {
        let previous = baseline.and_then(|m| {
            m.day_inventory
                .iter()
                .find(|d| d.family == DayFamily::Observations && d.date == date)
        });
        let first = rows.first().expect("nonempty day").time()?;
        let last = rows.last().expect("nonempty day").time()?;
        if let Some(d) = previous.filter(|d| {
            d.rows == rows.len() as u64
                && d.first_time.as_deref() == Some(text(first).as_str())
                && d.last_time.as_deref() == Some(text(last).as_str())
        }) {
            // Acquisition has already proved overlap equality including repeated-tick order.
            manifest
                .objects
                .push(old_object(baseline.expect("old"), d)?.clone());
            manifest.day_inventory.push(d.clone());
            continue;
        }
        let path = import::temporary_path(local, "daily-observations")?;
        let summary = match manifest.price_representation {
            PriceRepresentation::IntegerUnits { scale } => {
                let ticks = rows
                    .into_iter()
                    .map(|r| match r {
                        Observation::Tick(t) => Ok(t),
                        _ => Err("expected ticks".into()),
                    })
                    .collect::<Result<Vec<_>, String>>()?;
                let data = daily::write_ticks(&path, &date, &id, scale, [ticks.clone()])?;
                if daily::read_ticks(&path, &date, &id, scale)? != ticks {
                    return Err("daily tick row equality proof failed".into());
                }
                data
            }
            _ => {
                let mut bars = rows
                    .into_iter()
                    .map(|r| match r {
                        Observation::Bar(b) => Ok(b),
                        _ => Err("expected bars".into()),
                    })
                    .collect::<Result<Vec<_>, String>>()?;
                // Keep all eleven original provider columns for previously imported rows.
                if let Some(d) = previous.filter(|d| d.object.is_some()) {
                    let o = old_object(baseline.expect("old"), d)?;
                    let (_, file) = verify::fetch(local, o, true)?;
                    let old = daily::read_bars(&file.expect("day").path, &date)?;
                    let mut cursor = 0;
                    for row in &mut bars {
                        if old
                            .get(cursor)
                            .is_some_and(|b| b.unix_utc_s == row.unix_utc_s)
                        {
                            *row = old[cursor].clone();
                            cursor += 1;
                        }
                    }
                    if cursor != old.len() {
                        return Err("daily update lost prior provider rows".into());
                    }
                }
                let data = daily::write_bars(&path, &date, [bars.clone()])?;
                if daily::read_bars(&path, &date)? != bars {
                    return Err("daily provider row equality proof failed".into());
                }
                data
            }
        };
        let o = retain_file(
            local,
            &path,
            &format!("observations/{date}.parquet"),
            ObjectRole::Normalized,
        )?;
        manifest
            .day_inventory
            .push(entry(&date, DayFamily::Observations, &o, summary));
        manifest.objects.push(o);
    }
    manifest
        .day_inventory
        .sort_by(|a, b| (a.family, &a.date).cmp(&(b.family, &b.date)));
    Ok(())
}
fn pages(
    local: &Store,
    manifest: &mut GenerationManifest,
    baseline: Option<&GenerationManifest>,
    additions: Vec<PageOccurrence>,
) -> Result<(), String> {
    let mut grouped: BTreeMap<String, Vec<PageOccurrence>> = BTreeMap::new();
    for page in additions {
        grouped
            .entry(date(page.partition_time()?))
            .or_default()
            .push(page);
    }
    if let Some(old) = baseline {
        for d in old
            .day_inventory
            .iter()
            .filter(|d| d.family == DayFamily::Pages)
        {
            if let Some(new) = grouped.get_mut(&d.date) {
                if d.object.is_some() {
                    let (_, file) = verify::fetch(local, old_object(old, d)?, true)?;
                    let existing = daily::read_pages(&file.expect("pages").path, &d.date)?;
                    for p in existing {
                        if let Some(alias) = new.iter().find(|n| {
                            n.acquisition_id == p.acquisition_id && n.ordinal == p.ordinal
                        }) {
                            if alias != &p {
                                return Err("page occurrence identity collision".into());
                            }
                        } else {
                            new.push(p);
                        }
                    }
                }
            } else {
                if d.object.is_some() {
                    manifest.objects.push(old_object(old, d)?.clone());
                }
                manifest.day_inventory.push(d.clone());
            }
        }
    }
    for (date, mut rows) in grouped {
        rows.sort_by(|a, b| (&a.acquisition_id, a.ordinal).cmp(&(&b.acquisition_id, b.ordinal)));
        let path = import::temporary_path(local, "daily-pages")?;
        let data = daily::write_pages(&path, &date, [rows])?;
        let o = retain_file(
            local,
            &path,
            &format!("pages/{date}.parquet"),
            ObjectRole::Source,
        )?;
        manifest
            .day_inventory
            .push(entry(&date, DayFamily::Pages, &o, data));
        manifest.objects.push(o);
    }
    manifest
        .day_inventory
        .sort_by(|a, b| (a.family, &a.date).cmp(&(b.family, &b.date)));
    Ok(())
}
fn identify(manifest: &mut GenerationManifest) {
    manifest.layout = Some(Layout::DailyV2);
    let scale = match manifest.price_representation {
        PriceRepresentation::IntegerUnits { scale } => Some(scale),
        _ => None,
    };
    manifest.objects.sort_by(|a, b| a.path.cmp(&b.path));
    manifest.generation = generation_id_with_layout(
        &InstrumentId {
            broker: manifest.broker.clone(),
            provider_symbol: manifest.provider_symbol.clone(),
        },
        manifest.source_kind,
        manifest.role,
        scale,
        &manifest.objects,
        manifest.layout,
    );
}

/// Publish a cumulative history closure with daily occurrences and metadata-only coverage.
#[allow(clippy::too_many_arguments)]
pub(crate) fn descendant(
    local: &Store,
    destination: &Store,
    baseline: &GenerationManifest,
    mut manifest: GenerationManifest,
    rows: Vec<Observation>,
    coverage: &HistoryCoverage,
    acquired: &[PageCoverage],
    diagnostics: &[PageCoverage],
    server_offset_s: i64,
) -> Result<import::Publication, String> {
    manifest.objects.clear();
    manifest.day_inventory.clear();
    observations(local, &mut manifest, Some(baseline), rows)?;
    let mut additions = Vec::new();
    for (page, diagnostic) in acquired
        .iter()
        .map(|p| (p, false))
        .chain(diagnostics.iter().map(|p| (p, true)))
    {
        let occurrence = page
            .occurrence
            .as_ref()
            .ok_or("daily acquisition has no durable occurrence identity")?;
        let mut payload = Vec::new();
        local.read_to(&object_key(&page.sha256), None, &mut payload)?;
        let anchor = page
            .anchor
            .as_deref()
            .map(|a| seconds(&json!(a), server_offset_s))
            .transpose()?;
        additions.push(PageOccurrence {
            acquisition_id: occurrence.acquisition_id.clone(),
            intent: occurrence.intent.clone(),
            ordinal: occurrence.ordinal,
            checkpoint_ordinal: None,
            order_kind: daily::PageOrderKind::RequestOrder,
            payload_sha256: page.sha256.clone(),
            payload,
            request_token: page.anchor.clone(),
            request_anchor_utc: anchor,
            receipt_time_utc: page.receipt_time.as_deref().map(time).transpose()?,
            receipt_state: if page.receipt_time.is_some() {
                daily::ReceiptState::Recorded
            } else {
                daily::ReceiptState::AbsentInLegacyRecord
            },
            first_event_time: page.first.as_deref().map(time).transpose()?,
            last_event_time: page.last.as_deref().map(time).transpose()?,
            rows: page.rows,
            checkpoint: None,
            disposition: if diagnostic {
                daily::PageDisposition::Diagnostic
            } else {
                daily::PageDisposition::Indexed
            },
        });
    }
    pages(local, &mut manifest, Some(baseline), additions)?;
    observation_coverage(&mut manifest, baseline, coverage)?;

    let mut value = serde_json::to_value(coverage).map_err(err)?;
    value
        .as_object_mut()
        .expect("coverage object")
        .remove("pages");
    value
        .as_object_mut()
        .expect("coverage object")
        .remove("bundle");
    let old_coverage = baseline
        .objects
        .iter()
        .find(|o| o.path == crate::fetch::COVERAGE_PATH)
        .ok_or("prior coverage absent")?;
    let (_, file) = verify::fetch(local, old_coverage, true)?;
    let mut prior: Value =
        serde_json::from_slice(&fs::read(&file.expect("coverage").path).map_err(err)?)
            .map_err(err)?;
    let mut history = prior
        .as_object_mut()
        .and_then(|p| p.remove("prior_acquisitions"))
        .and_then(|v| v.as_array().cloned())
        .unwrap_or_default();
    history.push(prior);
    value["prior_acquisitions"] = json!(history);
    manifest
        .objects
        .push(metadata(local, crate::fetch::COVERAGE_PATH, &value)?);
    let inherited = if let Some(o) = baseline.objects.iter().find(|o| o.path == LINEAGE_PATH) {
        let (_, file) = verify::fetch(local, o, true)?;
        Some(
            serde_json::from_slice::<Value>(&fs::read(&file.expect("lineage").path).map_err(err)?)
                .map_err(err)?,
        )
    } else {
        None
    };
    // Flatten inherited provenance rather than recursively duplicating the ancestor chain.
    let mut lineage = inherited.unwrap_or_else(|| json!({"schema_version":1}));
    lineage["parent_generation"] = json!(baseline.generation);
    lineage["root_generation"] = json!(coverage.seed.as_ref().map(|s| &s.generation));
    manifest
        .objects
        .push(metadata(local, LINEAGE_PATH, &lineage)?);
    identify(&mut manifest);
    let identities = manifest
        .objects
        .iter()
        .map(|o| store::identify(&local.local_path(&o.key).expect("local")))
        .collect::<Result<Vec<_>, _>>()?;
    import::publish_generation(manifest, &identities, local, destination)
}

/// Convert validated import inputs directly into a daily root. No legacy ready manifest is
/// committed. Provenance embeds original text bytes and binds all original object identities.
pub(crate) fn import_root(
    local: &Store,
    mut manifest: GenerationManifest,
) -> Result<GenerationManifest, String> {
    use parquet::{
        file::reader::{FileReader, SerializedFileReader},
        record::RowAccessor,
    };
    let original = manifest.objects.clone();
    manifest.objects.clear();
    manifest.day_inventory.clear();
    let mut provenance = Vec::new();
    let mut source_rows = BTreeMap::new();
    let mut metadata_days = BTreeMap::new();
    let mut offset = None;
    for object in &original {
        if object.role != ObjectRole::Provenance {
            continue;
        }
        let path = local.local_path(&object.key).expect("retained source");
        let content = fs::read(&path).map_err(err)?;
        if object.path.ends_with(".meta.json") {
            let value: Value = serde_json::from_slice(&content).map_err(err)?;
            let d = value["date"].as_str().ok_or("daily metadata date absent")?;
            metadata_days.insert(d.to_string(), value);
        }
        if object.path.starts_with("collection/") && object.path.ends_with(".json") {
            let value: Value = serde_json::from_slice(&content).map_err(err)?;
            offset = value["server_timestamp_offset_seconds"].as_i64().or(offset);
            // The shared collection binds only this instrument's own entry.
            if let Some(asset) = value.pointer(&format!("/assets/{}", manifest.provider_symbol)) {
                provenance.push(json!({"object": object, "instrument_entry": asset}));
                continue;
            }
        }
        if object.path != "raw_pages.ndjson" && object.path != "checkpoint.ndjson" {
            let marker = object.path.ends_with("_SUCCESS")
                || object.path.ends_with(".conversion.lock")
                || (object.path.ends_with("gaps.parquet")
                    && SerializedFileReader::new(fs::File::open(&path).map_err(err)?)
                        .map_err(err)?
                        .metadata()
                        .file_metadata()
                        .num_rows()
                        == 0);
            provenance
                .push(json!({"object": object, "bytes_verbatim": (!marker).then_some(content)}));
        }
    }
    let mut buffered = Vec::new();
    let mut buffered_date: Option<String> = None;
    let price_representation = manifest.price_representation;
    let mut push = |row: Observation| -> Result<(), String> {
        let d = date(row.time()?);
        if buffered_date.as_ref().is_some_and(|old| old != &d) {
            observations(local, &mut manifest, None, std::mem::take(&mut buffered))?;
        }
        buffered_date = Some(d);
        buffered.push(row);
        Ok(())
    };
    for object in original.iter().filter(|o| o.role == ObjectRole::Source) {
        let path = local.local_path(&object.key).expect("retained source");
        let mut count = 0u64;
        if let PriceRepresentation::IntegerUnits { scale } = price_representation {
            let date = object
                .path
                .rsplit_once('_')
                .and_then(|(p, _)| p.rsplit_once('_').map(|(_, d)| d))
                .ok_or("daily import path has no date")?;
            for tick in crate::archive::read_daily_ticks(&path, scale, day_bounds(date)?.0)? {
                push(Observation::Tick(tick))?;
                count += 1;
            }
        } else {
            let reader =
                SerializedFileReader::new(fs::File::open(&path).map_err(err)?).map_err(err)?;
            for row in reader.get_row_iter(None).map_err(err)? {
                let r = row.map_err(err)?;
                push(Observation::Bar(DailyBar {
                    symbol: Some(r.get_string(0).map_err(err)?.clone()),
                    symbol_id: Some(r.get_int(1).map_err(err)?),
                    timestamp_utc: Some(r.get_timestamp_micros(2).map_err(err)?),
                    unix_utc_s: Some(r.get_long(3).map_err(err)?),
                    server_time_s: Some(r.get_long(4).map_err(err)?),
                    open: Some(r.get_double(5).map_err(err)?),
                    high: Some(r.get_double(6).map_err(err)?),
                    low: Some(r.get_double(7).map_err(err)?),
                    close: Some(r.get_double(8).map_err(err)?),
                    volume: Some(r.get_double(9).map_err(err)?),
                    period_s: Some(r.get_ushort(10).map_err(err)?),
                }))?;
                count += 1;
            }
        }
        source_rows.insert(object.path.clone(), count);
    }
    if !buffered.is_empty() {
        observations(local, &mut manifest, None, buffered)?;
    }
    for (date, value) in &metadata_days {
        let existing = manifest
            .day_inventory
            .iter_mut()
            .find(|d| d.family == DayFamily::Observations && d.date == *date);
        let complete = value["complete"] == true
            && value["clipped_by_retention"] != true
            && value["clipped_by_now"] != true;
        match existing {
            Some(d) if complete => {
                d.state = DayState::Complete;
                d.reason = None;
            }
            Some(d) if value["clipped_by_retention"] == true || value["clipped_by_now"] == true => {
                // These source flags identify clipping, but do not identify a verified boundary.
                // Preserve the uncertainty explicitly instead of inferring coverage from ticks.
                let (start, end) = day_bounds(date)?;
                d.state = DayState::Partial;
                d.reason = Some("source day clipped by retention or collection cutoff; exact covered boundary unavailable".into());
                d.unresolved = vec![UnresolvedInterval {
                    start: text(start),
                    end: text(end),
                }];
            }
            Some(_) => {}
            None => {
                let known = complete && value["market_closed"] == true;
                if known {
                    manifest.day_inventory.push(DayInventoryEntry {
                        date: date.clone(),
                        family: DayFamily::Observations,
                        duration: None,
                        offset: None,
                        object: None,
                        rows: 0,
                        first_time: None,
                        last_time: None,
                        state: DayState::EmptyKnown,
                        reason: None,
                        unresolved: vec![],
                    });
                } else {
                    let file = import::temporary_path(local, "empty-import-day")?;
                    let PriceRepresentation::IntegerUnits { scale } = manifest.price_representation
                    else {
                        return Err("tick metadata on bars".into());
                    };
                    let id = InstrumentId {
                        broker: manifest.broker.clone(),
                        provider_symbol: manifest.provider_symbol.clone(),
                    };
                    let data = daily::write_ticks(&file, date, &id, scale, [Vec::new()])?;
                    let object = retain_file(
                        local,
                        &file,
                        &format!("observations/{date}.parquet"),
                        ObjectRole::Normalized,
                    )?;
                    manifest.day_inventory.push(entry(
                        date,
                        DayFamily::Observations,
                        &object,
                        data,
                    ));
                    manifest.objects.push(object);
                }
            }
        }
    }
    let framing = import_pages(local, &mut manifest, &original, offset)?;
    manifest
        .day_inventory
        .sort_by(|a, b| (a.family, &a.date).cmp(&(b.family, &b.date)));
    let objects: Vec<_> = original
        .iter()
        .map(|o| json!({"object": o, "rows": source_rows.get(&o.path)}))
        .collect();
    let observation_objects: Vec<_> = manifest
        .objects
        .iter()
        .filter(|o| o.role == ObjectRole::Normalized)
        .cloned()
        .collect();
    manifest.objects.push(metadata(local, LINEAGE_PATH, &json!({"schema_version":1, "kind":"import", "replaced_generations":[], "original_objects": objects, "provenance": provenance, "framing":framing, "observation_objects": observation_objects, "proofs":{"observation_rows": manifest.row_count, "source_validation":"ordered rows and provider columns", "ndjson_reconstruction":"sha256 and byte count verified"}}))?);
    manifest.objects.push(metadata(local, crate::fetch::COVERAGE_PATH, &json!({"schema_version":1,"kind":"import","rows":manifest.row_count,"actual":{"first":manifest.coverage.first_event_time,"last":manifest.coverage.last_event_time},"unresolved":manifest.day_inventory.iter().filter(|d| matches!(d.state,DayState::Unknown|DayState::Partial)).collect::<Vec<_>>()}))?);
    identify(&mut manifest);
    let summary = daily::read_generation(local, &manifest, |_| Ok(()))?.data;
    if summary.rows != manifest.row_count
        || crate::archive::coverage(&summary)?
            != (
                manifest.coverage.first_event_time.clone(),
                manifest.coverage.last_event_time.clone(),
            )
    {
        return Err("daily import does not equal validated input coverage/rows".into());
    }
    Ok(manifest)
}

/// Index line positions, keeping payload bytes on disk until their assigned day is encoded.
#[derive(Clone)]
struct Line {
    offset: u64,
    bytes: usize,
    ordinal: u64,
    terminator: String,
}
fn lines(path: &Path) -> Result<Vec<Line>, String> {
    use std::io::BufRead;
    let mut reader = std::io::BufReader::new(fs::File::open(path).map_err(err)?);
    let mut result = Vec::new();
    let mut offset = 0;
    loop {
        let mut bytes = Vec::new();
        if reader.read_until(b'\n', &mut bytes).map_err(err)? == 0 {
            break;
        }
        let terminator = if bytes.ends_with(b"\r\n") {
            "\r\n"
        } else if bytes.ends_with(b"\n") {
            "\n"
        } else {
            ""
        };
        result.push(Line {
            offset,
            bytes: bytes.len() - terminator.len(),
            ordinal: result.len() as u64,
            terminator: terminator.into(),
        });
        offset += bytes.len() as u64;
    }
    Ok(result)
}
fn line(path: &Path, line: &Line) -> Result<Vec<u8>, String> {
    use std::io::{Read, Seek};
    let mut file = fs::File::open(path).map_err(err)?;
    file.seek(std::io::SeekFrom::Start(line.offset))
        .map_err(err)?;
    let mut bytes = vec![0; line.bytes];
    file.read_exact(&mut bytes).map_err(err)?;
    Ok(bytes)
}
fn seconds(value: &Value, offset: i64) -> Result<i64, String> {
    let token = value
        .as_str()
        .map(str::to_string)
        .unwrap_or_else(|| value.to_string());
    binary_alpha_engine::market::parse_price_units(&token, 6.try_into().expect("scale"))?
        .checked_sub(offset.checked_mul(1_000_000).ok_or("offset overflow")?)
        .ok_or_else(|| "timestamp overflow".into())
}
fn digest(bytes: &[u8]) -> String {
    use sha2::Digest;
    binary_alpha_engine::hex(&sha2::Sha256::digest(bytes))
}
fn import_page(
    payload: Vec<u8>,
    acquisition: &str,
    ordinal: u64,
    checkpoint: Option<(u64, Vec<u8>)>,
    offset: i64,
) -> Result<PageOccurrence, String> {
    let value: Value = serde_json::from_slice(&payload).map_err(err)?;
    let check: Value = checkpoint
        .as_ref()
        .map(|(_, b)| serde_json::from_slice(b).map_err(err))
        .transpose()?
        .unwrap_or(Value::Null);
    let data = value
        .get("data")
        .and_then(Value::as_array)
        .ok_or("import page has no data array; retain unresolved source")?;
    let mut times = Vec::new();
    for row in data {
        let t = row
            .get("time")
            .or_else(|| row.get(0))
            .ok_or("import page row has no time")?;
        times.push(seconds(t, offset)?);
    }
    times.sort();
    let anchor = check
        .get("request_token")
        .or_else(|| check.get("request_anchor"))
        .or_else(|| value.get("request_token"));
    let receipt = check
        .get("receipt_time_utc")
        .and_then(Value::as_str)
        .map(time)
        .transpose()?;
    let p = PageOccurrence {
        acquisition_id: acquisition.into(),
        intent: None,
        ordinal,
        checkpoint_ordinal: checkpoint.as_ref().map(|(n, _)| *n),
        order_kind: daily::PageOrderKind::SourceFileOrder,
        payload_sha256: digest(&payload),
        payload,
        request_token: anchor.map(|a| {
            a.as_str()
                .map(str::to_string)
                .unwrap_or_else(|| a.to_string())
        }),
        request_anchor_utc: anchor.map(|a| seconds(a, offset)).transpose()?,
        receipt_time_utc: receipt,
        receipt_state: if receipt.is_some() {
            daily::ReceiptState::Recorded
        } else if checkpoint.is_some() {
            daily::ReceiptState::NotRecordedBySource
        } else {
            daily::ReceiptState::AbsentInLegacyRecord
        },
        first_event_time: times.first().copied(),
        last_event_time: times.last().copied(),
        rows: times.len() as u64,
        checkpoint: checkpoint.map(|(_, b)| b),
        disposition: daily::PageDisposition::Indexed,
    };
    p.partition_time()?;
    Ok(p)
}
fn import_pages(
    local: &Store,
    manifest: &mut GenerationManifest,
    original: &[ObjectRecord],
    offset: Option<i64>,
) -> Result<Value, String> {
    use std::collections::VecDeque;
    use std::io::Write;
    let raw = original.iter().find(|o| o.path == "raw_pages.ndjson");
    let checkpoint = original.iter().find(|o| o.path == "checkpoint.ndjson");
    let Some(raw) = raw else {
        if checkpoint.is_some() {
            return Err("checkpoint without raw source; retain unresolved source".into());
        }
        return Ok(Value::Null);
    };
    let offset = offset.ok_or("import pages have no recorded provider offset")?;
    let raw_path = local.local_path(&raw.key).expect("local");
    let raw_lines = lines(&raw_path)?;
    let check_path = checkpoint.map(|o| local.local_path(&o.key).expect("local"));
    let check_lines = check_path
        .as_ref()
        .map(|p| lines(p))
        .transpose()?
        .unwrap_or_default();
    let mut checks: BTreeMap<String, VecDeque<Line>> = BTreeMap::new();
    for l in &check_lines {
        let bytes = line(check_path.as_ref().expect("path"), l)?;
        let v: Value = serde_json::from_slice(&bytes).map_err(err)?;
        let hash = v["payload_sha256"]
            .as_str()
            .ok_or("checkpoint lacks payload_sha256; retain unresolved source")?;
        checks.entry(hash.into()).or_default().push_back(l.clone());
    }
    let mut days: BTreeMap<String, Vec<(Line, Option<Line>)>> = BTreeMap::new();
    for l in &raw_lines {
        let payload = line(&raw_path, l)?;
        let check = checks
            .get_mut(&digest(&payload))
            .and_then(VecDeque::pop_front);
        let bytes = check
            .as_ref()
            .map(|l| line(check_path.as_ref().expect("path"), l).map(|b| (l.ordinal, b)))
            .transpose()?;
        let p = import_page(payload, &raw.sha256, l.ordinal, bytes, offset)?;
        days.entry(date(p.partition_time()?))
            .or_default()
            .push((l.clone(), check));
    }
    if checks.values().any(|q| !q.is_empty()) {
        return Err(
            "checkpoint occurrence has no matching raw line; retain unresolved source".into(),
        );
    }
    // Hash reconstruction in each original order, including independent newline framing.
    for (object, path, framing) in std::iter::once((raw, &raw_path, &raw_lines)).chain(
        checkpoint
            .zip(check_path.as_ref())
            .map(|(o, p)| (o, p, &check_lines)),
    ) {
        let mut h = store::Hasher::default();
        for l in framing {
            h.write_all(&line(path, l)?).map_err(err)?;
            h.write_all(l.terminator.as_bytes()).map_err(err)?;
        }
        let proof = h.finish();
        if proof.sha256 != object.sha256 || proof.bytes != object.bytes {
            return Err("NDJSON framing reconstruction failed".into());
        }
    }
    let mut raw_positions = BTreeMap::new();
    let mut check_positions = BTreeMap::new();
    for (date, indexes) in days {
        let mut occurrences = Vec::new();
        for (l, c) in indexes {
            raw_positions.insert(l.ordinal, (date.clone(), l.ordinal));
            if let Some(c) = &c {
                check_positions.insert(c.ordinal, (date.clone(), l.ordinal));
            }
            let check = c
                .as_ref()
                .map(|l| line(check_path.as_ref().expect("path"), l).map(|b| (l.ordinal, b)))
                .transpose()?;
            occurrences.push(import_page(
                line(&raw_path, &l)?,
                &raw.sha256,
                l.ordinal,
                check,
                offset,
            )?);
        }
        pages(local, manifest, None, occurrences)?;
    }
    for (object, framing, positions, is_checkpoint) in
        std::iter::once((raw, &raw_lines, &raw_positions, false))
            .chain(checkpoint.map(|o| (o, &check_lines, &check_positions, true)))
    {
        let mut hash = store::Hasher::default();
        let mut cached_date = String::new();
        let mut cached = Vec::new();
        for l in framing {
            let (date, ordinal) = positions
                .get(&l.ordinal)
                .ok_or("source occurrence missing from daily inventory")?;
            if &cached_date != date {
                let object = manifest
                    .objects
                    .iter()
                    .find(|o| o.path == format!("pages/{date}.parquet"))
                    .ok_or("daily page object absent")?;
                cached = daily::read_pages(&local.local_path(&object.key).expect("local"), date)?;
                cached_date = date.clone();
            }
            let page = cached
                .iter()
                .find(|p| p.ordinal == *ordinal && p.acquisition_id == raw.sha256)
                .ok_or("encoded occurrence absent")?;
            let bytes = if is_checkpoint {
                page.checkpoint
                    .as_ref()
                    .ok_or("encoded checkpoint absent")?
            } else {
                &page.payload
            };
            hash.write_all(bytes).map_err(err)?;
            hash.write_all(l.terminator.as_bytes()).map_err(err)?;
        }
        let proof = hash.finish();
        if proof.sha256 != object.sha256 || proof.bytes != object.bytes {
            return Err("encoded daily NDJSON reconstruction failed".into());
        }
    }
    let frame = |ls: &[Line]| json!({"line_terminators":ls.iter().map(|l| &l.terminator).collect::<Vec<_>>(),"final_newline":ls.last().is_some_and(|l| !l.terminator.is_empty())});
    Ok(
        json!({"acquisition_id":raw.sha256,"raw_pages":{"object":raw,"framing":frame(&raw_lines)},"checkpoint":{"object":checkpoint,"framing":frame(&check_lines)}}),
    )
}

/// A closed acquisition's deletion inventory. It is written before removing its progress
/// header. Replaying this journal after interruption treats an already missing key as done.
#[derive(serde::Serialize, serde::Deserialize)]
struct Reclamation {
    intent: String,
    receipt: String,
    generation: String,
    pending_header_sha256: Option<String>,
    #[serde(default)]
    acquisitions: BTreeSet<String>,
    candidates: BTreeSet<String>,
    reclaimed: BTreeSet<String>,
    protected: BTreeSet<String>,
}

const RECEIVED: &str = "progress.received.jsonl";

/// The raw response is journaled before decode or overlap validation. Only indexed progress
/// is replayed; unmatched received responses become diagnostic occurrences on resumption.
pub(crate) fn received(state: &Path, page: &PageCoverage) -> Result<(), String> {
    use std::io::Write;
    let mut file = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(state.join(RECEIVED))
        .map_err(err)?;
    serde_json::to_writer(&mut file, page).map_err(err)?;
    file.write_all(b"\n").map_err(err)?;
    file.sync_all().map_err(err)
}
pub(crate) fn clear_received(state: &Path) -> Result<(), String> {
    match fs::remove_file(state.join(RECEIVED)) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(err(e)),
    }
}
pub(crate) fn clear_pending(state: &Path) -> Result<(), String> {
    for name in ["progress.json", "progress.pages.jsonl", RECEIVED] {
        match fs::remove_file(state.join(name)) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(err(e)),
        }
    }
    Ok(())
}
pub(crate) fn diagnostics(
    state: &Path,
    indexed: Option<&[PageCoverage]>,
) -> Result<(Vec<PageCoverage>, bool), String> {
    let Some(indexed) = indexed else {
        return Ok((Vec::new(), false));
    };
    let path = state.join(RECEIVED);
    if !path.exists() {
        return Ok((Vec::new(), false));
    }
    let bytes = fs::read(&path).map_err(err)?;
    let complete = bytes.iter().rposition(|b| *b == b'\n').map_or(0, |p| p + 1);
    let partial = complete < bytes.len();
    if partial {
        // Unknown response references are retained, never used as deletion authority. Keep
        // the exact fragment and allow indexed progress to resume from its durable boundary.
        let fragment = &bytes[complete..];
        atomic(
            &state.join(format!("unresolved-received-{}.json", digest(fragment))),
            &json!({"reason":"interrupted response journal append", "fragment":fragment}),
        )?;
        let file = fs::OpenOptions::new()
            .write(true)
            .open(&path)
            .map_err(err)?;
        file.set_len(complete as u64).map_err(err)?;
        file.sync_all().map_err(err)?;
    }
    let mut pages = BTreeMap::new();
    for line in bytes[..complete].split_inclusive(|b| *b == b'\n') {
        let page: PageCoverage = serde_json::from_slice(line).map_err(err)?;
        let identity = page
            .occurrence
            .as_ref()
            .ok_or("received response lacks occurrence identity")?;
        if page.receipt_time.is_none() {
            return Err("received response lacks receipt time".into());
        }
        if !indexed.iter().any(|p| p.occurrence == page.occurrence) {
            pages.insert((identity.acquisition_id.clone(), identity.ordinal), page);
        }
    }
    Ok((pages.into_values().collect(), partial))
}
fn atomic(path: &Path, value: &impl serde::Serialize) -> Result<(), String> {
    use std::io::Write;
    let temporary = path.with_extension("tmp");
    let mut file = fs::File::create(&temporary).map_err(err)?;
    file.write_all(&crate::fetch::json_bytes(value)?)
        .map_err(err)?;
    file.sync_all().map_err(err)?;
    fs::rename(&temporary, path).map_err(err)?;
    fs::File::open(path.parent().ok_or("state parent absent")?)
        .map_err(err)?
        .sync_all()
        .map_err(err)
}
pub(crate) fn schedule_reclamation(
    state: &Path,
    intent: &str,
    receipt: &str,
    generation: &str,
    requests: &[crate::fetch::PageReceipt],
) -> Result<(), String> {
    let plan = Reclamation {
        intent: intent.into(),
        receipt: receipt.into(),
        generation: generation.into(),
        pending_header_sha256: if state.join("progress.json").exists() {
            Some(digest(&fs::read(state.join("progress.json")).map_err(err)?))
        } else {
            None
        },
        acquisitions: requests
            .iter()
            .filter_map(|r| r.occurrence.as_ref().map(|o| o.acquisition_id.clone()))
            .collect(),
        candidates: requests.iter().map(|r| object_key(&r.sha256)).collect(),
        reclaimed: BTreeSet::new(),
        protected: BTreeSet::new(),
    };
    let path = state.join(format!("reclaim-{}.json", digest(receipt.as_bytes())));
    if !path.exists() {
        atomic(&path, &plan)?;
    }
    Ok(())
}
fn references(value: &Value, keys: &mut BTreeSet<String>) {
    match value {
        Value::Object(map) => {
            if let Some(hash) = map
                .get("sha256")
                .and_then(Value::as_str)
                .filter(|s| s.len() == 64)
            {
                keys.insert(object_key(hash));
            }
            if let Some(key) = map.get("key").and_then(Value::as_str) {
                keys.insert(key.into());
            }
            for child in map.values() {
                references(child, keys);
            }
        }
        Value::Array(items) => {
            for child in items {
                references(child, keys);
            }
        }
        _ => {}
    }
}
/// Caller holds the pipeline writer lock. The closed receipt and verified daily generation
/// authorize precisely this acquisition's singles; manifests and other pending logs pin keys.
pub(crate) fn reclaim(
    local: &Store,
    state: &Path,
    state_root: &Path,
    records: &Store,
    access: Access<'_>,
) -> Result<(), String> {
    for entry in fs::read_dir(state).map_err(err)? {
        let path = entry.map_err(err)?.path();
        if !path
            .file_name()
            .is_some_and(|s| s.to_string_lossy().starts_with("reclaim-"))
            || path.extension().is_none_or(|e| e != "json")
        {
            continue;
        }
        let mut plan: Reclamation =
            serde_json::from_slice(&fs::read(&path).map_err(err)?).map_err(err)?;
        if plan.candidates.iter().all(|k| plan.reclaimed.contains(k)) {
            continue;
        }
        let mut receipt = Vec::new();
        records.read_to(&plan.receipt, None, &mut receipt)?;
        let receipt: Value = serde_json::from_slice(&receipt).map_err(err)?;
        if receipt["pending"] != false
            || receipt["intent"] != plan.intent
            || receipt["dataset_generation"] != plan.generation
        {
            return Err("reclamation receipt does not close this acquisition".into());
        }
        verify::run_with(&local.uri(&manifest_key(&plan.generation)), access)?;
        let pending = state.join("progress.json");
        if pending.exists() {
            let bytes = fs::read(&pending).map_err(err)?;
            let value: Value = serde_json::from_slice(&bytes).map_err(err)?;
            if value["intent"] == plan.intent
                && plan.pending_header_sha256.as_deref() == Some(digest(&bytes).as_str())
            {
                fs::remove_file(&pending).map_err(err)?;
                clear_received(state)?;
                match fs::remove_file(state.join("progress.pages.jsonl")) {
                    Ok(()) => {}
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                    Err(e) => return Err(err(e)),
                }
            }
        }
        let mut protected = BTreeSet::new();
        plan.protected.clear();
        for generation in local.list_manifests()? {
            let mut bytes = Vec::new();
            local.read_to(&manifest_key(&generation), None, &mut bytes)?;
            let value: Value = serde_json::from_slice(&bytes).map_err(err)?;
            references(&value["objects"], &mut protected);
        }
        for job in fs::read_dir(state_root).map_err(err)? {
            let dir = job.map_err(err)?.path();
            if !dir.is_dir() {
                continue;
            }
            if fs::read_dir(&dir).map_err(err)?.any(|entry| {
                entry.is_ok_and(|e| {
                    e.file_name()
                        .to_string_lossy()
                        .starts_with("unresolved-received-")
                })
            }) {
                return Ok(());
            }
            if !dir.join("progress.json").exists() {
                continue;
            }
            let value: Value =
                serde_json::from_slice(&fs::read(dir.join("progress.json")).map_err(err)?)
                    .map_err(err)?;
            references(&value, &mut protected);
            for log in [dir.join("progress.pages.jsonl"), dir.join(RECEIVED)] {
                if log.exists() {
                    use std::io::BufRead;
                    for line in std::io::BufReader::new(fs::File::open(log).map_err(err)?).lines() {
                        // An unresolved/torn reference is not permission to delete anything.
                        let Ok(value) = serde_json::from_str::<Value>(&line.map_err(err)?) else {
                            // A producer repairs its torn tail on resume; reclamation must not
                            // block that recovery or delete an unresolved reference meanwhile.
                            return Ok(());
                        };
                        references(&value, &mut protected);
                    }
                }
            }
        }
        // Another acquisition's receipt can still rely on its single-page representation.
        let record_root = records.local_path("").ok_or("records must be local")?;
        for record in fs::read_dir(record_root).map_err(err)? {
            let p = record.map_err(err)?.path();
            if !p.is_file() || p.extension().is_none_or(|e| e != "json") {
                continue;
            }
            let value: Value = serde_json::from_slice(&fs::read(&p).map_err(err)?).map_err(err)?;
            if let Some(requests) = value["requests"].as_array() {
                for request in requests {
                    let acquisition = request
                        .pointer("/occurrence/acquisition_id")
                        .and_then(Value::as_str);
                    if acquisition.is_none_or(|id| !plan.acquisitions.contains(id)) {
                        references(request, &mut protected);
                    }
                }
            }
        }
        for key in plan.candidates.clone() {
            if plan.reclaimed.contains(&key) {
                continue;
            }
            if protected.contains(&key) {
                plan.protected.insert(key);
            } else {
                let target = local
                    .local_path(&key)
                    .ok_or("reclamation requires local store")?;
                match fs::remove_file(target) {
                    Ok(()) => {}
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                    Err(e) => return Err(err(e)),
                }
                plan.reclaimed.insert(key);
            }
            atomic(&path, &plan)?;
        }
    }
    Ok(())
}

fn observation_coverage(
    manifest: &mut GenerationManifest,
    baseline: &GenerationManifest,
    coverage: &HistoryCoverage,
) -> Result<(), String> {
    let verified = coverage
        .verified
        .as_ref()
        .map(|r| Ok::<_, String>((time(&r.start)?, time(&r.end)?)))
        .transpose()?;
    for day in manifest
        .day_inventory
        .iter_mut()
        .filter(|d| d.family == DayFamily::Observations)
    {
        let (start, end) = day_bounds(&day.date)?;
        let old = baseline
            .day_inventory
            .iter()
            .find(|d| d.family == DayFamily::Observations && d.date == day.date);
        let Some((from, to)) = verified.filter(|(from, to)| *from < end && *to > start) else {
            continue;
        };
        if old.is_some_and(|d| matches!(d.state, DayState::Complete | DayState::EmptyKnown)) {
            day.state = if day.object.is_none() {
                DayState::EmptyKnown
            } else {
                DayState::Complete
            };
            day.reason = None;
            day.unresolved.clear();
            continue;
        }
        if old.is_some_and(|d| d.state == DayState::Unknown) && !(from <= start && to >= end) {
            day.state = DayState::Unknown;
            day.reason = old.and_then(|d| d.reason.clone());
            day.unresolved = old.map_or_else(Vec::new, |d| d.unresolved.clone());
            continue;
        }
        let ranges = if let Some(old) = old.filter(|d| d.state == DayState::Partial) {
            old.unresolved
                .iter()
                .map(|r| Ok((time(&r.start)?, time(&r.end)?)))
                .collect::<Result<Vec<_>, String>>()?
        } else {
            vec![(start, end)]
        };
        let mut unresolved = Vec::new();
        for (a, b) in ranges {
            if to <= a || from >= b {
                unresolved.push((a, b));
            } else {
                if a < from {
                    unresolved.push((a, from.min(b)));
                }
                if b > to {
                    unresolved.push((to.max(a), b));
                }
            }
        }
        day.unresolved = unresolved
            .into_iter()
            .map(|(a, b)| UnresolvedInterval {
                start: text(a),
                end: text(b),
            })
            .collect();
        day.state = if day.unresolved.is_empty() {
            DayState::Complete
        } else {
            DayState::Partial
        };
        day.reason = (!day.unresolved.is_empty())
            .then(|| "acquisition covers only part of this UTC day".into());
    }
    Ok(())
}

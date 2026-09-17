//! Daily continuation selection and publication. The ready manifest owns the complete closure;
//! a descendant carries existing day keys and changes only partitions with additional rows.

use crate::drive::Drive;
use crate::store::ObjectIdentity;
use crate::{
    archive::DataSummary,
    daily::{self, DailyBar, PageOccurrence},
    fetch::{HistoryCoverage, PageCoverage},
    import,
    store::{self, Store},
    verify,
};
use binary_alpha_engine::{
    dataset::{coverage::*, daily::day_bounds, *},
    market::{
        InstrumentId, Tick, format_event_time_micros as text, parse_event_time_micros as time,
    },
    research::Access,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::Path,
};

pub const LINEAGE_PATH: &str = "provenance/lineage.json";

/// Immutable legacy closure replaced by one daily continuation root. Dataset and stream
/// identities remain distinct; every historical stream is included, not only the newest.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MigrationMapping {
    pub v1_generations: BTreeSet<String>,
    pub v1_stream: Option<String>,
    #[serde(default)]
    pub v1_streams: BTreeSet<String>,
}
impl MigrationMapping {
    pub fn streams(&self) -> BTreeSet<String> {
        self.v1_streams
            .iter()
            .chain(self.v1_stream.iter())
            .cloned()
            .collect()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MigrationEquality {
    pub observations: bool,
    pub pages: bool,
    pub source_files: bool,
    /// Legacy candle evidence remains exactly reproducible. Session migrations record
    /// reconstruction under the legacy definition separately from the new product proof.
    /// None means there was no legacy stream to compare, not a measured equality.
    pub candles: Option<bool>,
}

/// The migration writer, archive transport, and retirement gate share this exact record.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MigrationRecord {
    pub schema_version: u32,
    pub job: String,
    pub phase: String,
    #[serde(flatten)]
    pub mapping: MigrationMapping,
    pub v2_root: String,
    pub v2_stream: String,
    pub equality: MigrationEquality,
    #[serde(flatten)]
    pub evidence: BTreeMap<String, Value>,
    /// Earlier job identifiers whose generations, records, catalogs, and transfers belong to this job's
    /// instrument and source identity (verified during migration); retirement treats them as this job's.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub predecessor_jobs: Vec<String>,
    /// Physical store keys holding a byte-identical standalone copy of a migrated page payload
    /// (`objects/<payload_sha256>`); they are storage aliases of existing occurrences, not occurrences.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub storage_aliases: Vec<String>,
}
impl MigrationRecord {
    fn session_proof_consistent(&self) -> bool {
        let Some(candles) = self.evidence.get("proofs").and_then(|p| p.get("candles")) else {
            return true; // Legacy records predate the session-product proof.
        };
        match candles.get("basis").and_then(Value::as_str) {
            Some("legacy_definition_reconstruction") => {}
            Some("direct_product_equality") => {
                return candles["equal"] == true
                    && candles["profile_equal"] == true
                    && candles["session_product_verified"] == false
                    && candles["product_definition"].is_object()
                    && candles["product_definition"]["session"].is_null()
                    && candles["legacy_definition"] == candles["product_definition"]
                    && candles["legacy_streams"].is_array()
                    && candles["legacy_streams"] == candles["product_streams"];
            }
            Some(_) => return false,
            None => {
                return [
                    "basis",
                    "session_product_verified",
                    "legacy_definition",
                    "product_definition",
                    "legacy_streams",
                    "product_streams",
                ]
                .iter()
                .all(|field| candles.get(field).is_none());
            }
        }
        candles["equal"] == true
            && candles["profile_equal"] == true
            && candles["session_product_verified"] == true
            && candles["legacy_definition"].is_object()
            && candles["product_definition"]["session"].is_object()
            && candles["legacy_streams"].is_array()
            && candles["product_streams"].is_array()
    }
    pub fn verified(&self) -> bool {
        self.schema_version == 1
            && self.phase == "verified"
            && self.equality.observations
            && self.equality.pages
            && self.equality.source_files
            && self.session_proof_consistent()
            && if self.mapping.streams().is_empty() {
                self.equality.candles.is_none()
            } else {
                self.mapping.v1_stream.is_some() && self.equality.candles == Some(true)
            }
    }
}

pub(crate) fn record_name(key: &str) -> Result<&str, String> {
    let name = key
        .strip_prefix("records/")
        .ok_or("migration record key must start with records/")?;
    if name.is_empty() || name == "." || name == ".." || name.contains(['/', '\\']) {
        return Err("migration record must have one safe filename".into());
    }
    Ok(name)
}

/// Select the verified receipt by its exact root mapping and confirm every immutable record
/// named by that root. Descendants inherit this provenance, never a second migration format.
#[derive(Default)]
pub(crate) struct MigrationRecords {
    pub files: BTreeMap<String, ObjectIdentity>,
    pub streams: BTreeMap<String, binary_alpha_engine::stream::StreamManifest>,
}

pub(crate) fn migration_records(
    local: &Store,
    records: &Path,
    manifest: &GenerationManifest,
    job: &str,
    access: Access<'_>,
) -> Result<MigrationRecords, String> {
    let lineage = read_lineage(local, manifest)?;
    if lineage.get("v1_generations").is_none() {
        return Ok(MigrationRecords::default());
    }
    let mapping: MigrationMapping = serde_json::from_value(lineage.clone()).map_err(err)?;
    let root = lineage["root_generation"]
        .as_str()
        .unwrap_or(&manifest.generation);
    let mut selected = BTreeMap::new();
    let mut streams = BTreeMap::new();
    if records.is_dir() {
        for entry in fs::read_dir(records).map_err(err)? {
            let path = entry.map_err(err)?.path();
            if path.extension().is_none_or(|e| e != "json") {
                continue;
            }
            let bytes = fs::read(&path).map_err(err)?;
            if let Ok(record) = serde_json::from_slice::<MigrationRecord>(&bytes)
                && record.verified()
                && record.job == job
                && record.v2_root == root
                && record.mapping == mapping
            {
                if record.v2_stream.len() != 64
                    || !record.v2_stream.bytes().all(|b| b.is_ascii_hexdigit())
                {
                    return Err("invalid migration stream identity".into());
                }
                access.lookup(&record.v2_stream)?;
                access.permit(Some(manifest.role), root)?;
                let key = manifest_key(&record.v2_stream);
                let mut bytes = Vec::new();
                local.read_to(&key, None, &mut bytes)?;
                let stream = binary_alpha_engine::stream::StreamManifest::from_json(&bytes)?;
                if stream.generation != record.v2_stream
                    || stream.source_generation != root
                    || stream.instrument != manifest.instrument
                    || stream.role != manifest.role
                    || stream.layout != Some(Layout::DailyV2)
                {
                    return Err("migration stream does not bind its daily continuation root".into());
                }
                verify::run_with(&local.uri(&key), access)?;
                streams.insert(record.v2_stream.clone(), stream);
                selected.insert(
                    format!("records/{}", path.file_name().unwrap().to_string_lossy()),
                    store::identify(&path)?,
                );
            }
        }
    }
    if selected.is_empty() {
        return Err(format!("missing verified migration evidence for {root}"));
    }
    for binding in lineage["records"]
        .as_array()
        .into_iter()
        .flatten()
        .filter(|v| v.get("name").is_some())
        .chain(lineage.get("alias_table"))
    {
        let name = binding["name"]
            .as_str()
            .or_else(|| binding["record"].as_str())
            .ok_or("migration record name absent")?;
        let key = format!("records/{name}");
        let identity = store::identify(&records.join(record_name(&key)?))?;
        if binding["bytes"] != identity.bytes || binding["sha256"] != identity.sha256 {
            return Err(format!("migration record identity mismatch: {name}"));
        }
        selected.insert(key, identity);
    }
    Ok(MigrationRecords {
        files: selected,
        streams,
    })
}
fn err(e: impl std::fmt::Display) -> String {
    e.to_string()
}
fn date(t: i64) -> String {
    text(t)[..10].into()
}

/// Immutable receipt names, rather than proof versions or mutable checkpoints, order
/// migration roots. The same resolver consumes local records and pinned archive records.
fn current_migration<'a>(
    records: &'a BTreeMap<String, MigrationRecord>,
    job: &str,
) -> Result<Option<&'a MigrationRecord>, String> {
    let owned: BTreeMap<_, _> = records
        .iter()
        .filter(|(_, r)| r.job == job)
        .map(|(name, record)| (name.as_str(), record))
        .collect();
    if owned.is_empty() {
        return Ok(None);
    }
    let mut superseded = BTreeSet::new();
    for (name, record) in &owned {
        if !record.verified() {
            return Err(format!("missing verified migration evidence: {name}"));
        }
        let mut seen = BTreeSet::from([*name]);
        let mut cursor = *record;
        while let Some(previous) = cursor.evidence.get("supersedes").filter(|v| !v.is_null()) {
            let previous = previous
                .as_str()
                .ok_or("invalid migration supersession name")?;
            record_name(&format!("records/{previous}"))?;
            if !seen.insert(previous) {
                return Err("cyclic migration supersession".into());
            }
            let predecessor = owned.get(previous).ok_or_else(|| {
                format!("missing verified superseded migration record {previous}")
            })?;
            if !predecessor.verified() {
                return Err(format!("missing verified migration evidence: {previous}"));
            }
            superseded.insert(previous);
            cursor = predecessor;
        }
    }
    let mut newest = owned
        .keys()
        .copied()
        .filter(|name| !superseded.contains(*name));
    let name = newest.next().ok_or("cyclic migration supersession")?;
    if newest.next().is_some() {
        return Err("competing verified migration records; no unique superseding record".into());
    }
    Ok(Some(owned[name]))
}

fn insert_migration(
    records: &mut BTreeMap<String, MigrationRecord>,
    name: &str,
    bytes: &[u8],
) -> Result<(), String> {
    let value: Value = serde_json::from_slice(bytes).map_err(err)?;
    if value.get("v2_root").is_some() {
        let record: MigrationRecord = serde_json::from_value(value).map_err(err)?;
        if let Some(existing) = records.get(name)
            && serde_json::to_value(existing).map_err(err)?
                != serde_json::to_value(&record).map_err(err)?
        {
            return Err(format!("conflicting migration record bytes: {name}"));
        }
        records.insert(name.into(), record);
    }
    Ok(())
}

fn migration_family(
    manifest: &GenerationManifest,
    lineage: &Value,
    record: &MigrationRecord,
) -> Result<bool, String> {
    let logical_root = lineage["root_generation"]
        .as_str()
        .unwrap_or(&manifest.generation);
    if logical_root != record.v2_root {
        return Ok(false);
    }
    if lineage.get("v1_generations").is_some() {
        let mapping: MigrationMapping = serde_json::from_value(lineage.clone()).map_err(err)?;
        if mapping != record.mapping {
            return Err("selected migration root disagrees with verified mapping".into());
        }
    } else if manifest.generation == record.v2_root {
        return Err("verified migration root is missing its mapping".into());
    }
    Ok(true)
}

fn selected_daily(
    local: &Store,
    records_dir: &Path,
    job: &str,
    candidates: Vec<GenerationManifest>,
    access: Access<'_>,
) -> Result<Vec<GenerationManifest>, String> {
    let mut records = BTreeMap::new();
    if records_dir.is_dir() {
        for entry in fs::read_dir(records_dir).map_err(err)? {
            let entry = entry.map_err(err)?;
            if entry.path().extension().is_some_and(|e| e == "json") {
                insert_migration(
                    &mut records,
                    &entry.file_name().to_string_lossy(),
                    &fs::read(entry.path()).map_err(err)?,
                )?;
            }
        }
    }
    let current = current_migration(&records, job)?;
    let mut selected = Vec::new();
    for manifest in candidates {
        let lineage = read_lineage(local, &manifest)?;
        if let Some(record) = current {
            if migration_family(&manifest, &lineage, record)? {
                selected.push(manifest);
            }
        } else if lineage.get("v1_generations").is_some() {
            return Err(format!(
                "missing verified migration evidence for {}",
                manifest.generation
            ));
        } else {
            selected.push(manifest);
        }
    }
    if current.is_some() {
        let first = selected
            .iter()
            .find(|m| current.is_some_and(|r| m.generation == r.v2_root))
            .or_else(|| {
                selected.iter().find(|m| {
                    read_lineage(local, m).is_ok_and(|v| v.get("v1_generations").is_some())
                })
            })
            .ok_or("verified migration continuation root and descendants are absent")?;
        // Reuse the existing mapping, stream, and immutable source-record verifier.
        migration_records(local, records_dir, first, job, access)?;
    }
    Ok(selected)
}

/// Select a stable readable seed. A restored descendant is a self-contained baseline;
/// its immutable logical root remains in lineage even if ancestor manifests are absent.
pub(crate) fn root(
    local: &Store,
    records: &Path,
    job: &str,
    instrument: &str,
    role: DatasetRole,
    access: Access<'_>,
) -> Result<Option<String>, String> {
    let candidates = selected_daily(
        local,
        records,
        job,
        daily_candidates(local, instrument, role, access)?,
        access,
    )?;
    if candidates.is_empty() {
        return Ok(None);
    }
    let ids: BTreeSet<_> = candidates.iter().map(|m| m.generation.clone()).collect();
    let mut roots = Vec::new();
    let mut logical = BTreeSet::new();
    for manifest in &candidates {
        let value = read_lineage(local, manifest)?;
        if let Some(root) = value.get("root_generation").and_then(Value::as_str) {
            logical.insert(root.to_string());
        }
        if lineage_references(
            &serde_json::to_vec(&value).map_err(err)?,
            &ids,
            &manifest.generation,
        )?
        .is_empty()
        {
            roots.push(manifest.generation.clone());
        }
    }
    if roots.len() != 1 || logical.len() > 1 {
        return Err(format!(
            "{instrument}: multiple daily continuation roots; an unambiguous migration binding is required"
        ));
    }
    Ok(roots.pop())
}
fn daily_candidates(
    local: &Store,
    instrument: &str,
    role: DatasetRole,
    access: Access<'_>,
) -> Result<Vec<GenerationManifest>, String> {
    let generations = match access.declaration {
        Some(d) => d
            .populations
            .iter()
            .filter(|p| p.instrument == instrument && p.role == role)
            .flat_map(|p| p.generations.clone())
            .collect(),
        None => local.list_manifests()?,
    };
    let mut candidates = Vec::new();
    for generation in generations {
        access.lookup(&generation)?;
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
        if manifest.layout == Some(Layout::DailyV2)
            && manifest.instrument == instrument
            && manifest.role == role
        {
            access.permit(Some(role), &generation)?;
            candidates.push(manifest);
        }
    }
    Ok(candidates)
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
    acquisition: &crate::fetch::OccurrenceIdentity,
    acquired: &[PageCoverage],
    diagnostics: &[PageCoverage],
    server_offset_s: i64,
    access: Access<'_>,
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
    let superseded = superseded_snapshots(local, &manifest, acquisition, access)?;
    let mut evidence = read_coverage(local, baseline)?;
    for old in &superseded {
        for claim in read_coverage(local, old)?.acquisitions {
            if let Some(existing) = evidence
                .acquisitions
                .iter()
                .find(|a| a.acquisition_id == claim.acquisition_id)
            {
                if existing != &claim {
                    return Err("conflicting acquisition evidence in resumed snapshots".into());
                }
            } else {
                evidence.acquisitions.push(claim);
            }
        }
    }
    let requested = CoverageRange {
        start: coverage.requested.start.clone(),
        end: coverage.requested.end.clone(),
    };
    let verified: Vec<_> = coverage
        .verified
        .iter()
        .map(|r| CoverageRange {
            start: r.start.clone(),
            end: r.end.clone(),
        })
        .collect();
    let unresolved = complement(requested.bounds()?, &verified)?;
    let shortfalls = coverage
        .shortfall
        .iter()
        .chain(coverage.tail_shortfall.iter())
        .map(|s| CoverageShortfall {
            reason: s.reason.clone(),
            unresolved: CoverageRange {
                start: s.unresolved.start.clone(),
                end: s.unresolved.end.clone(),
            },
        })
        .collect();
    let claim = AcquisitionCoverage {
        acquisition_id: acquisition.acquisition_id.clone(),
        source_identity: coverage.source_identity.clone(),
        requested: vec![requested],
        verified,
        shortfalls,
        unresolved,
    };
    if evidence
        .acquisitions
        .iter()
        .any(|a| a.acquisition_id == claim.acquisition_id)
    {
        return Err("daily acquisition identity was already published".into());
    }
    evidence.acquisitions.push(claim);
    for page in acquired.iter().chain(diagnostics) {
        let id = &page
            .occurrence
            .as_ref()
            .ok_or("missing occurrence identity")?
            .acquisition_id;
        if !evidence
            .acquisitions
            .iter()
            .any(|a| &a.acquisition_id == id)
        {
            evidence.acquisitions.push(AcquisitionCoverage {
                acquisition_id: id.clone(),
                source_identity: coverage.source_identity.clone(),
                requested: vec![],
                verified: vec![],
                shortfalls: vec![],
                unresolved: vec![],
            });
        }
    }
    descendant_days(&mut evidence, &mut manifest, &acquisition.acquisition_id)?;
    manifest
        .objects
        .push(metadata(local, crate::fetch::COVERAGE_PATH, &evidence)?);
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
    let mut ancestors: BTreeSet<String> = lineage
        .get("ancestors")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .map(str::to_string)
        .collect();
    ancestors.insert(baseline.generation.clone());
    ancestors.extend(superseded.iter().map(|m| m.generation.clone()));
    if let Some(parent) = lineage.get("parent_generation").and_then(Value::as_str) {
        ancestors.insert(parent.into());
    }
    lineage["ancestors"] = json!(ancestors);
    lineage["parent_generation"] = json!(baseline.generation);
    if lineage
        .get("root_generation")
        .and_then(Value::as_str)
        .is_none()
    {
        lineage["root_generation"] = json!(baseline.generation);
    }
    lineage["continuation"] = json!({"acquisition_id": acquisition.acquisition_id, "intent": acquisition.intent, "seed": coverage.seed});
    manifest
        .objects
        .push(metadata(local, LINEAGE_PATH, &lineage)?);
    identify(&mut manifest);
    evidence.check_manifest(&manifest)?;
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
    let evidence = import_coverage(&mut manifest, &original)?;
    manifest
        .objects
        .push(metadata(local, crate::fetch::COVERAGE_PATH, &evidence)?);
    identify(&mut manifest);
    evidence.check_manifest(&manifest)?;
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
pub(crate) fn receipt_time(value: &str) -> Result<i64, String> {
    if !value.is_ascii() {
        return Err("receipt timestamp must be ASCII".into());
    }
    if value.ends_with('Z') {
        return time(value);
    }
    if value.len() >= 6 {
        let i = value.len() - 6;
        let zone = &value[i..];
        if (zone.starts_with('+') || zone.starts_with('-')) && zone.as_bytes()[3] == b':' {
            let h: i64 = zone[1..3].parse().map_err(err)?;
            let m: i64 = zone[4..].parse().map_err(err)?;
            if h > 23 || m > 59 {
                return Err("receipt timezone offset is invalid".into());
            }
            let offset =
                (h * 3600 + m * 60) * 1_000_000 * if zone.starts_with('-') { -1 } else { 1 };
            return time(&format!("{}Z", &value[..i]))?
                .checked_sub(offset)
                .ok_or("receipt time overflow".into());
        }
    }
    time(value)
}

pub(crate) fn import_page(
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
    let anchor = [
        check.get("request_token"),
        check.get("request_anchor"),
        check.get("target_server_s"),
        value.get("request_token"),
    ]
    .into_iter()
    .flatten()
    .find(|value| !value.is_null());
    let receipt = [check.get("receipt_time_utc"), check.get("received_at")]
        .into_iter()
        .flatten()
        .find_map(Value::as_str)
        .map(receipt_time)
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
        } else if check["recovered_from_raw"] == true {
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
/// A retired publication can be replaced as deletion proof only by a permitted, verified
/// closure in the same lineage that still carries every exact recorded response occurrence.
fn reclamation_proven(
    local: &Store,
    plan: &Reclamation,
    receipt: &Value,
    access: Access<'_>,
) -> Result<bool, String> {
    if verify::run_with(&local.uri(&manifest_key(&plan.generation)), access).is_ok() {
        return Ok(true);
    }
    let coverage: HistoryCoverage =
        serde_json::from_value(receipt["coverage"].clone()).map_err(err)?;
    let Some(seed) = &coverage.seed else {
        return Ok(false);
    };
    let requests: Vec<crate::fetch::PageReceipt> =
        serde_json::from_value(receipt["requests"].clone()).map_err(err)?;
    if requests.iter().any(|r| r.occurrence.is_none()) {
        return Ok(false);
    }
    for generation in local.list_manifests()? {
        let proves = || -> Result<bool, String> {
            access.lookup(&generation)?;
            access.permit(Some(coverage.role), &generation)?;
            let mut bytes = Vec::new();
            local.read_to(&manifest_key(&generation), None, &mut bytes)?;
            if verify::manifest_kind(&bytes)?.is_some() {
                return Ok(false);
            }
            let manifest = GenerationManifest::from_json(&bytes)?;
            if manifest.layout != Some(Layout::DailyV2)
                || manifest.broker.as_str() != coverage.broker
                || manifest.provider_symbol.as_str() != coverage.provider_symbol
                || manifest.role != coverage.role
            {
                return Ok(false);
            }
            access.permit(Some(manifest.role), &generation)?;
            let lineage = manifest
                .objects
                .iter()
                .find(|o| o.path == LINEAGE_PATH)
                .ok_or("replacement lineage absent")?;
            let (_, file) = verify::fetch(local, lineage, true)?;
            let lineage: Value =
                serde_json::from_slice(&fs::read(&file.expect("lineage").path).map_err(err)?)
                    .map_err(err)?;
            let descendant = lineage["ancestors"].as_array().is_some_and(|ancestors| {
                ancestors.iter().any(|ancestor| {
                    ancestor.as_str() == Some(plan.generation.as_str())
                        || ancestor.as_str() == Some(seed.generation.as_str())
                })
            });
            // A self-contained restored descendant may be the operational seed while
            // root_generation still names the original, unavailable logical root.
            if lineage["root_generation"].as_str() != Some(seed.generation.as_str())
                && lineage["continuation"]["seed"]["generation"].as_str()
                    != Some(seed.generation.as_str())
                && !descendant
            {
                return Ok(false);
            }
            let mut remaining: BTreeMap<_, _> = requests
                .iter()
                .map(|request| {
                    let occurrence = request.occurrence.as_ref().expect("checked occurrence");
                    (
                        (occurrence.acquisition_id.clone(), occurrence.ordinal),
                        request,
                    )
                })
                .collect();
            for day in manifest
                .day_inventory
                .iter()
                .filter(|d| d.family == DayFamily::Pages && d.object.is_some())
            {
                let (_, file) = verify::fetch(local, old_object(&manifest, day)?, true)?;
                for page in daily::read_pages(&file.expect("pages").path, &day.date)? {
                    let key = (page.acquisition_id.clone(), page.ordinal);
                    if let Some(request) = remaining.get(&key)
                        && page.intent == request.occurrence.as_ref().expect("checked").intent
                        && page.payload_sha256 == request.sha256
                        && page.payload.len() as u64 == request.bytes
                        && page.rows == request.rows
                        && page.request_token == request.anchor
                        && page.receipt_time_utc == Some(time(&request.receipt_time)?)
                        && page.receipt_state == daily::ReceiptState::Recorded
                    {
                        remaining.remove(&key);
                    }
                }
            }
            Ok(remaining.is_empty()
                && verify::run_with(&local.uri(&manifest_key(&generation)), access).is_ok())
        };
        // Missing, incomplete, inaccessible, or mismatched replacement evidence defers
        // this cleanup; it must neither authorize deletion nor block other update jobs.
        if proves().unwrap_or(false) {
            return Ok(true);
        }
    }
    Ok(false)
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
        if !reclamation_proven(local, &plan, &receipt, access)? {
            continue;
        }
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

pub(crate) fn reclamation_roots(path: &Path) -> Result<BTreeSet<String>, String> {
    let plan: Reclamation = serde_json::from_slice(&fs::read(path).map_err(err)?).map_err(err)?;
    let mut roots: BTreeSet<_> = plan
        .candidates
        .difference(&plan.reclaimed)
        .cloned()
        .collect();
    if !roots.is_empty() {
        roots.insert(plan.generation);
    }
    Ok(roots)
}

/// Read the single authenticated acquisition-coverage contract used by writers and fetch.
fn read_coverage(local: &Store, manifest: &GenerationManifest) -> Result<DailyCoverage, String> {
    let object = manifest
        .objects
        .iter()
        .find(|o| o.path == crate::fetch::COVERAGE_PATH)
        .ok_or("daily coverage absent")?;
    let (_, file) = verify::fetch(local, object, true)?;
    let coverage =
        DailyCoverage::from_json(&fs::read(&file.expect("coverage").path).map_err(err)?)?;
    coverage.check_manifest(manifest)?;
    Ok(coverage)
}

fn merge_ranges(ranges: Vec<CoverageRange>) -> Result<Vec<CoverageRange>, String> {
    let mut spans = ranges
        .iter()
        .map(CoverageRange::bounds)
        .collect::<Result<Vec<_>, _>>()?;
    spans.sort_unstable();
    let mut merged: Vec<(i64, i64)> = Vec::new();
    for (start, end) in spans {
        if let Some(last) = merged.last_mut()
            && start <= last.1
        {
            last.1 = last.1.max(end);
        } else {
            merged.push((start, end));
        }
    }
    Ok(merged
        .into_iter()
        .map(|(a, b)| CoverageRange::new(a, b))
        .collect())
}
fn clip(bounds: (i64, i64), ranges: &[CoverageRange]) -> Result<Vec<CoverageRange>, String> {
    let mut clipped = Vec::new();
    for range in ranges {
        let (a, b) = range.bounds()?;
        let (a, b) = (a.max(bounds.0), b.min(bounds.1));
        if a < b {
            clipped.push(CoverageRange::new(a, b));
        }
    }
    merge_ranges(clipped)
}
fn complement(
    bounds: (i64, i64),
    verified: &[CoverageRange],
) -> Result<Vec<CoverageRange>, String> {
    let mut cursor = bounds.0;
    let mut missing = Vec::new();
    for range in clip(bounds, verified)? {
        let (a, b) = range.bounds()?;
        if cursor < a {
            missing.push(CoverageRange::new(cursor, a));
        }
        cursor = b;
    }
    if cursor < bounds.1 {
        missing.push(CoverageRange::new(cursor, bounds.1));
    }
    Ok(missing)
}
fn apply_day(day: &mut DayInventoryEntry, evidence: &DayCoverage) -> Result<(), String> {
    day.state = evidence.state(day.rows)?;
    day.reason = evidence.reason.clone();
    day.unresolved = evidence
        .unresolved
        .iter()
        .map(|r| UnresolvedInterval {
            start: r.start.clone(),
            end: r.end.clone(),
        })
        .collect();
    Ok(())
}
fn import_coverage(
    manifest: &mut GenerationManifest,
    original: &[ObjectRecord],
) -> Result<DailyCoverage, String> {
    let id = original
        .iter()
        .find(|o| o.path == "raw_pages.ndjson")
        .map_or_else(|| manifest.generation.clone(), |o| o.sha256.clone());
    let mut coverage = DailyCoverage {
        schema_version: 2,
        broker: manifest.broker.clone(),
        provider_symbol: manifest.provider_symbol.clone(),
        role: manifest.role,
        native_granularity: manifest.native_granularity,
        acquisitions: vec![],
        days: vec![],
    };
    let mut verified = Vec::new();
    let mut requested = Vec::new();
    for day in &mut manifest.day_inventory {
        let bounds = day_bounds(&day.date)?;
        let complete = matches!(day.state, DayState::Complete | DayState::EmptyKnown);
        let spans = if complete {
            vec![CoverageRange::new(bounds.0, bounds.1)]
        } else {
            vec![]
        };
        if day.family == DayFamily::Observations {
            verified.extend(spans.clone());
            requested.push(CoverageRange::new(bounds.0, bounds.1));
        }
        let evidence = DayCoverage {
            date: day.date.clone(),
            family: day.family,
            acquisition_ids: vec![id.clone()],
            basis: if day.family == DayFamily::Observations {
                "retained import source metadata"
            } else {
                "retained import response occurrences; no whole-day occurrence claim"
            }
            .into(),
            unresolved: complement(bounds, &spans)?,
            verified: spans,
            reason: (!complete).then(|| {
                day.reason
                    .clone()
                    .unwrap_or_else(|| "source has no verified whole-day coverage".into())
            }),
        };
        apply_day(day, &evidence)?;
        coverage.days.push(evidence);
    }
    let requested = merge_ranges(requested)?;
    let verified = merge_ranges(verified)?;
    let mut unresolved = Vec::new();
    for range in &requested {
        unresolved.extend(complement(range.bounds()?, &verified)?);
    }
    coverage.acquisitions.push(AcquisitionCoverage {
        acquisition_id: id,
        source_identity: format!("import:{}", manifest.source_kind),
        requested,
        verified,
        shortfalls: vec![],
        unresolved,
    });
    coverage.validate()?;
    Ok(coverage)
}

/// Translate retained v1 claims into the same typed coverage consumed by daily update and
/// verification. Observation endpoints never create a completeness claim.
pub(crate) fn migration_coverage(
    manifest: &mut GenerationManifest,
    histories: &[(String, Value)],
    source_identity: &str,
) -> Result<DailyCoverage, String> {
    let id = format!("migration-source:{}", manifest.generation);
    let mut evidence = DailyCoverage {
        schema_version: 2,
        broker: manifest.broker.clone(),
        provider_symbol: manifest.provider_symbol.clone(),
        role: manifest.role,
        native_granularity: manifest.native_granularity,
        acquisitions: vec![],
        days: vec![],
    };
    let mut requested = Vec::new();
    let mut verified = Vec::new();
    for day in &mut manifest.day_inventory {
        let bounds = day_bounds(&day.date)?;
        let unresolved = day
            .unresolved
            .iter()
            .map(|r| CoverageRange {
                start: r.start.clone(),
                end: r.end.clone(),
            })
            .collect::<Vec<_>>();
        let spans = match day.state {
            DayState::Complete | DayState::EmptyKnown => {
                vec![CoverageRange::new(bounds.0, bounds.1)]
            }
            DayState::Partial => complement(bounds, &unresolved)?,
            DayState::Unknown => vec![],
        };
        if day.family == DayFamily::Observations {
            requested.push(CoverageRange::new(bounds.0, bounds.1));
            verified.extend(spans.clone());
        }
        let missing = complement(bounds, &spans)?;
        let coverage = DayCoverage {
            date: day.date.clone(),
            family: day.family,
            acquisition_ids: vec![id.clone()],
            basis: if day.family == DayFamily::Observations {
                "retained v1 source metadata and acquisition coverage, bound by migration lineage"
            } else {
                "migration census of retained response occurrences; no whole-day occurrence claim"
            }
            .into(),
            reason: (!missing.is_empty()).then(|| {
                day.reason
                    .clone()
                    .unwrap_or_else(|| "retained source has no verified whole-day coverage".into())
            }),
            unresolved: missing,
            verified: spans,
        };
        apply_day(day, &coverage)?;
        evidence.days.push(coverage);
    }
    let requested = merge_ranges(requested)?;
    let verified = merge_ranges(verified)?;
    let mut unresolved = Vec::new();
    for range in &requested {
        unresolved.extend(complement(range.bounds()?, &verified)?);
    }
    evidence.acquisitions.push(AcquisitionCoverage {
        acquisition_id: id,
        source_identity: source_identity.into(),
        requested,
        verified,
        shortfalls: vec![],
        unresolved,
    });
    for (generation, value) in histories {
        let history: HistoryCoverage = serde_json::from_value(value.clone()).map_err(err)?;
        let range = |r: &crate::fetch::Range| CoverageRange {
            start: r.start.clone(),
            end: r.end.clone(),
        };
        let requested = vec![range(&history.requested)];
        let verified: Vec<_> = history.verified.iter().map(range).collect();
        let unresolved = complement(requested[0].bounds()?, &verified)?;
        evidence.acquisitions.push(AcquisitionCoverage {
            acquisition_id: format!("v1-history:{generation}"),
            source_identity: history.source_identity,
            requested,
            verified,
            unresolved,
            shortfalls: history
                .shortfall
                .iter()
                .chain(history.tail_shortfall.iter())
                .map(|s| CoverageShortfall {
                    reason: s.reason.clone(),
                    unresolved: range(&s.unresolved),
                })
                .collect(),
        });
    }
    evidence.validate()?;
    Ok(evidence)
}
fn descendant_days(
    coverage: &mut DailyCoverage,
    manifest: &mut GenerationManifest,
    acquisition: &str,
) -> Result<(), String> {
    let previous = std::mem::take(&mut coverage.days);
    let acquired = &coverage
        .acquisitions
        .iter()
        .find(|a| a.acquisition_id == acquisition)
        .ok_or("missing acquisition claim")?
        .verified;
    for day in &mut manifest.day_inventory {
        let old = previous
            .iter()
            .find(|d| d.date == day.date && d.family == day.family);
        let bounds = day_bounds(&day.date)?;
        let mut verified = old.map_or_else(Vec::new, |d| d.verified.clone());
        let mut ids = old.map_or_else(Vec::new, |d| d.acquisition_ids.clone());
        if day.family == DayFamily::Observations {
            verified.extend(clip(bounds, acquired)?);
        }
        ids.push(acquisition.into());
        let verified = merge_ranges(verified)?;
        let unresolved = complement(bounds, &verified)?;
        let evidence = DayCoverage {
            date:day.date.clone(), family:day.family, acquisition_ids:ids,
            basis: if day.family == DayFamily::Observations { "cumulative verified acquisition ranges and retained import evidence" } else { "retained response occurrences; market coverage does not prove occurrence completeness" }.into(),
            reason: (!unresolved.is_empty()).then(|| "acquisition evidence does not cover the whole UTC day".into()),
            verified, unresolved,
        };
        apply_day(day, &evidence)?;
        coverage.days.push(evidence);
    }
    let empty: BTreeSet<_> = manifest
        .day_inventory
        .iter_mut()
        .filter(|d| d.state == DayState::EmptyKnown)
        .filter_map(|d| d.object.take())
        .collect();
    manifest.objects.retain(|o| !empty.contains(&o.key));
    coverage.validate()
}

/// V1 retains its original format. V2 continuation reads its ranges only from typed coverage;
/// lineage binds the active acquisition and seed, without a second copy of coverage facts.
pub(crate) fn history_coverage(
    local: &Store,
    manifest: &GenerationManifest,
) -> Result<Option<HistoryCoverage>, String> {
    if manifest.layout != Some(Layout::DailyV2) {
        let object = manifest
            .objects
            .iter()
            .find(|o| o.path == crate::fetch::COVERAGE_PATH)
            .ok_or("history coverage absent")?;
        let (_, file) = verify::fetch(local, object, true)?;
        return serde_json::from_slice(&fs::read(&file.expect("coverage").path).map_err(err)?)
            .map(Some)
            .map_err(err);
    }
    let coverage = read_coverage(local, manifest)?;
    let value = read_lineage(local, manifest)?;
    let Some(continuation) = value.get("continuation") else {
        return Ok(None);
    };
    let id = continuation["acquisition_id"]
        .as_str()
        .ok_or("continuation acquisition absent")?;
    let acquisition = coverage
        .acquisitions
        .iter()
        .find(|a| a.acquisition_id == id)
        .ok_or("continuation acquisition has no coverage")?;
    let [requested] = acquisition.requested.as_slice() else {
        return Err("continuation needs one requested range".into());
    };
    if acquisition.verified.len() > 1 {
        return Err("continuation needs one verified range".into());
    }
    let range = |r: &CoverageRange| crate::fetch::Range {
        start: r.start.clone(),
        end: r.end.clone(),
    };
    let shortfall = |s: &CoverageShortfall| crate::fetch::Shortfall {
        reason: s.reason.clone(),
        unresolved: range(&s.unresolved),
    };
    Ok(Some(HistoryCoverage {
        schema_version: 1,
        source_identity: acquisition.source_identity.clone(),
        broker: manifest.broker.to_string(),
        provider_symbol: manifest.provider_symbol.to_string(),
        role: manifest.role,
        requested: range(requested),
        verified: acquisition.verified.first().map(range),
        actual: Some(crate::fetch::Actual {
            first: manifest.coverage.first_event_time.clone(),
            last: manifest.coverage.last_event_time.clone(),
        }),
        rows: manifest.row_count,
        pages: vec![],
        bundle: None,
        shortfall: acquisition.shortfalls.first().map(shortfall),
        tail_shortfall: acquisition.shortfalls.get(1).map(shortfall),
        native_granularity: manifest.native_granularity,
        seed: serde_json::from_value(continuation["seed"].clone()).map_err(err)?,
    }))
}
pub(crate) fn read_lineage(local: &Store, manifest: &GenerationManifest) -> Result<Value, String> {
    let Some(object) = manifest.objects.iter().find(|o| o.path == LINEAGE_PATH) else {
        return Ok(json!({}));
    };
    let (_, file) = verify::fetch(local, object, true)?;
    serde_json::from_slice(&fs::read(&file.expect("lineage").path).map_err(err)?).map_err(err)
}

/// Select by manifest evidence, independent of migration/update implementation. A restored
/// descendant is sufficient: no removed v1 generation or local parent closure is required.
pub fn newest_daily(
    local: &store::Store,
    instrument: &str,
    role: binary_alpha_engine::dataset::DatasetRole,
    access: binary_alpha_engine::research::Access<'_>,
) -> Result<String, String> {
    let candidates = daily_candidates(local, instrument, role, access)?;
    newest_from(local, candidates.iter().collect())
}
pub(crate) fn newest_daily_for_job(
    local: &Store,
    records: &Path,
    job: &str,
    instrument: &str,
    role: DatasetRole,
    access: Access<'_>,
) -> Result<String, String> {
    let candidates = selected_daily(
        local,
        records,
        job,
        daily_candidates(local, instrument, role, access)?,
        access,
    )?;
    newest_from(local, candidates.iter().collect())
}
pub(crate) fn newest_from(
    local: &Store,
    manifests: Vec<&GenerationManifest>,
) -> Result<String, String> {
    let mut candidates = manifests
        .into_iter()
        .map(|manifest| Ok((time(&manifest.coverage.last_event_time)?, manifest)))
        .collect::<Result<Vec<_>, String>>()?;
    let end = candidates
        .iter()
        .map(|(end, _)| *end)
        .max()
        .ok_or("pipeline: no daily-v2 dataset")?;
    candidates.retain(|(candidate_end, _)| *candidate_end == end);
    let generations: BTreeSet<_> = candidates
        .iter()
        .map(|(_, m)| m.generation.clone())
        .collect();
    let mut ancestors = BTreeSet::new();
    if generations.len() > 1 {
        for (_, manifest) in &candidates {
            if let Some(object) = manifest
                .objects
                .iter()
                .find(|o| o.path == "provenance/lineage.json")
            {
                let mut bytes = Vec::new();
                local.read_to(&object.key, None, &mut bytes)?;
                confirm_bytes(&bytes, object.bytes, &object.sha256)?;
                ancestors.extend(lineage_references(
                    &bytes,
                    &generations,
                    &manifest.generation,
                )?);
            }
        }
    }
    terminal_generation(&generations, &ancestors)
}

fn terminal_generation(
    generations: &BTreeSet<String>,
    ancestors: &BTreeSet<String>,
) -> Result<String, String> {
    let mut terminals = generations.difference(ancestors);
    let newest = terminals
        .next()
        .ok_or("archive: cyclic daily lineage at newest coverage")?;
    if terminals.next().is_some() {
        return Err(
            "archive: ambiguous daily lineage at equal coverage; no unique descendant is proved"
                .into(),
        );
    }
    Ok(newest.clone())
}

fn confirm_bytes(bytes: &[u8], expected_bytes: u64, expected_sha256: &str) -> Result<(), String> {
    use sha2::{Digest, Sha256};
    if bytes.len() as u64 != expected_bytes
        || binary_alpha_engine::hex(&Sha256::digest(bytes)) != expected_sha256
    {
        return Err("archive: lineage bytes disagree with the pinned manifest".into());
    }
    Ok(())
}

/// Lineage is owned by migration/acquisition. Its generation references name predecessors;
/// intersecting those references with ready candidates avoids coupling to either writer's
/// JSON field names or needing removed ancestor manifests to restore a descendant.
fn lineage_references(
    bytes: &[u8],
    candidates: &BTreeSet<String>,
    current: &str,
) -> Result<BTreeSet<String>, String> {
    fn visit(
        value: &serde_json::Value,
        candidates: &BTreeSet<String>,
        found: &mut BTreeSet<String>,
    ) {
        match value {
            serde_json::Value::String(value) if candidates.contains(value) => {
                found.insert(value.clone());
            }
            serde_json::Value::Array(values) => {
                values.iter().for_each(|v| visit(v, candidates, found))
            }
            serde_json::Value::Object(values) => {
                values.values().for_each(|v| visit(v, candidates, found))
            }
            _ => {}
        }
    }
    let value = serde_json::from_slice(bytes).map_err(|e| format!("archive lineage: {e}"))?;
    let mut found = BTreeSet::new();
    visit(&value, candidates, &mut found);
    found.remove(current);
    Ok(found)
}

/// Resolve the active verified migration family before comparing coverage. All remote
/// evidence is read through the catalog's immutable byte/hash/file-id bindings.
pub fn newest_catalog(
    catalogs: &[(String, String, crate::data_pipeline::Catalog)],
    drive: &mut Drive,
    scratch: &Path,
    access: Access<'_>,
) -> Result<usize, String> {
    fn read(
        drive: &mut Drive,
        scratch: &Path,
        id: &str,
        bytes: u64,
        sha256: &str,
    ) -> Result<Vec<u8>, String> {
        let path = scratch.join(format!("{sha256}.lineage"));
        drive.download(
            id,
            &path,
            &ObjectIdentity {
                bytes,
                sha256: sha256.into(),
                crc32c: 0,
            },
        )?;
        let bytes = fs::read(&path).map_err(err)?;
        fs::remove_file(path).map_err(err)?;
        Ok(bytes)
    }
    let mut records = BTreeMap::new();
    let mut record_bytes = BTreeMap::<String, Vec<u8>>::new();
    let mut daily = BTreeMap::new();
    let mut denied = BTreeMap::new();
    for (index, (_, _, catalog)) in catalogs.iter().enumerate() {
        if catalog.layout != Some(Layout::DailyV2) {
            continue;
        }
        if let Err(reason) = access.permit(Some(catalog.role), &catalog.dataset.generation) {
            // Catalog discovery is allowed; opening an unpermitted target is not. Delay
            // rejection only while permitted lineage may prove that target superseded.
            denied.insert(index, reason);
            continue;
        }
        let entry = &catalog.dataset;
        let manifest = GenerationManifest::from_json(&read(
            drive,
            scratch,
            &entry.file_id,
            entry.bytes,
            &entry.sha256,
        )?)?;
        if manifest.generation != entry.generation
            || manifest.layout != catalog.layout
            || manifest.role != catalog.role
            || manifest.instrument != catalog.instrument
        {
            return Err("archive: catalog disagrees with lineage manifest".into());
        }
        let lineage = if let Some(object) = manifest.objects.iter().find(|o| o.path == LINEAGE_PATH)
        {
            let entry = catalog
                .objects
                .iter()
                .find(|e| {
                    e.key == object.key && e.bytes == object.bytes && e.sha256 == object.sha256
                })
                .ok_or("archive: lineage lies outside pinned catalog closure")?;
            serde_json::from_slice(&read(
                drive,
                scratch,
                &entry.file_id,
                entry.bytes,
                &entry.sha256,
            )?)
            .map_err(err)?
        } else {
            json!({})
        };
        for entry in &catalog.records {
            let name = record_name(&entry.key)?;
            if !name.ends_with(".json") {
                continue;
            }
            let bytes = if let Some(bytes) = record_bytes.get(name) {
                confirm_bytes(bytes, entry.bytes, &entry.sha256)?;
                bytes.clone()
            } else {
                let bytes = read(drive, scratch, &entry.file_id, entry.bytes, &entry.sha256)?;
                record_bytes.insert(name.into(), bytes.clone());
                bytes
            };
            insert_migration(&mut records, name, &bytes)?;
        }
        daily.insert(index, (manifest, lineage));
    }
    let mut candidates = Vec::new();
    for (index, (_, _, catalog)) in catalogs.iter().enumerate() {
        if denied.contains_key(&index) {
            continue;
        }
        if let Some((manifest, lineage)) = daily.get(&index) {
            if let Some(record) = current_migration(&records, &catalog.job)? {
                if !migration_family(manifest, lineage, record)? {
                    continue;
                }
                if lineage.get("v1_generations").is_none() {
                    // Some historical descendants bind their root without repeating its
                    // migration map. Verify that map from the exact pinned root closure.
                    let root = catalog
                        .lineage_manifests
                        .iter()
                        .find(|e| e.generation == record.v2_root)
                        .ok_or("catalog omits the verified migration root")?;
                    access.permit(Some(catalog.role), &root.generation)?;
                    let root_manifest = GenerationManifest::from_json(&read(
                        drive,
                        scratch,
                        &root.file_id,
                        root.bytes,
                        &root.sha256,
                    )?)?;
                    if root_manifest.generation != record.v2_root
                        || root_manifest.instrument != manifest.instrument
                        || root_manifest.role != manifest.role
                        || root_manifest.layout != Some(Layout::DailyV2)
                    {
                        return Err("catalog migration root binding mismatch".into());
                    }
                    let object = root_manifest
                        .objects
                        .iter()
                        .find(|o| o.path == LINEAGE_PATH)
                        .ok_or("verified migration root is missing its mapping")?;
                    let entry = catalog
                        .objects
                        .iter()
                        .find(|e| {
                            e.key == object.key
                                && e.bytes == object.bytes
                                && e.sha256 == object.sha256
                        })
                        .ok_or("archive: lineage lies outside pinned catalog closure")?;
                    let root_lineage: Value = serde_json::from_slice(&read(
                        drive,
                        scratch,
                        &entry.file_id,
                        entry.bytes,
                        &entry.sha256,
                    )?)
                    .map_err(err)?;
                    if !migration_family(&root_manifest, &root_lineage, record)? {
                        return Err("catalog migration root binding mismatch".into());
                    }
                }
                // A superseded catalog for the same root may predate the newest receipt.
                // Only a cumulative closure can restore the complete verified chain.
                if records
                    .iter()
                    .filter(|(_, r)| r.job == catalog.job)
                    .any(|(name, _)| {
                        !catalog
                            .records
                            .iter()
                            .any(|entry| entry.key == format!("records/{name}"))
                    })
                {
                    continue;
                }
                let entry = std::iter::once(&catalog.stream)
                    .chain(&catalog.lineage_manifests)
                    .find(|entry| entry.generation == record.v2_stream)
                    .ok_or("catalog omits the verified migration stream")?;
                access.lookup(&entry.generation)?;
                access.permit(Some(catalog.role), &record.v2_root)?;
                let stream = binary_alpha_engine::stream::StreamManifest::from_json(&read(
                    drive,
                    scratch,
                    &entry.file_id,
                    entry.bytes,
                    &entry.sha256,
                )?)?;
                if stream.generation != record.v2_stream
                    || stream.source_generation != record.v2_root
                    || stream.instrument != manifest.instrument
                    || stream.role != manifest.role
                    || stream.layout != Some(Layout::DailyV2)
                {
                    return Err("migration stream does not bind its daily continuation root".into());
                }
            } else if lineage.get("v1_generations").is_some() {
                return Err(format!(
                    "verified migration evidence is not archived for {}",
                    manifest.generation
                ));
            }
        }
        candidates.push((
            (
                catalog.layout.is_some(),
                time(&catalog.coverage.last_event_time)?,
            ),
            index,
        ));
    }
    if candidates.is_empty()
        && let Some(reason) = denied.values().next()
    {
        return Err(reason.clone());
    }
    if !daily.is_empty() && !candidates.iter().any(|(key, _)| key.0) {
        return Err("drive: no archived catalog for the verified migration continuation".into());
    }
    let newest = candidates
        .iter()
        .map(|(key, _)| *key)
        .max()
        .ok_or("drive: no archived catalog for the verified migration continuation")?;
    candidates.retain(|(key, _)| *key == newest);
    let generations: BTreeSet<_> = candidates
        .iter()
        .map(|(_, i)| catalogs[*i].2.dataset.generation.clone())
        .collect();
    let mut ancestors = BTreeSet::new();
    if newest.0 && generations.len() > 1 {
        for (_, index) in &candidates {
            let (manifest, lineage) = &daily[index];
            ancestors.extend(lineage_references(
                &serde_json::to_vec(lineage).map_err(err)?,
                &generations,
                &manifest.generation,
            )?);
        }
    }
    let terminal = newest
        .0
        .then(|| terminal_generation(&generations, &ancestors))
        .transpose()?;
    let selected = candidates
        .iter()
        .map(|(_, i)| *i)
        .filter(|i| {
            terminal
                .as_ref()
                .is_none_or(|generation| *generation == catalogs[*i].2.dataset.generation)
        })
        .max_by_key(|i| &catalogs[*i].2.dataset.generation)
        .ok_or("archive: cyclic daily catalog lineage")?;
    if !denied.is_empty() {
        let generations = denied
            .keys()
            .map(|i| catalogs[*i].2.dataset.generation.clone())
            .collect();
        let superseded = if let Some((manifest, lineage)) = daily.get(&selected) {
            lineage_references(
                &serde_json::to_vec(lineage).map_err(err)?,
                &generations,
                &manifest.generation,
            )?
        } else {
            BTreeSet::new()
        };
        for (index, reason) in denied {
            // Lower coverage alone cannot prove obsolescence: a proof upgrade may
            // publish a new root. Unknown denied candidates must never cause fallback.
            if !superseded.contains(&catalogs[index].2.dataset.generation) {
                return Err(reason);
            }
        }
    }
    Ok(selected)
}

/// Archive consumes an already published stream. Computing its identity from the configured
/// definition avoids regenerating daily files and racing parallel jobs' audit temporaries.
pub fn verified_daily_stream(
    local: &store::Store,
    dataset: &str,
    config: &binary_alpha_engine::config::Config,
    access: binary_alpha_engine::research::Access<'_>,
) -> Result<String, String> {
    use binary_alpha_engine::{
        dataset::{GenerationManifest, manifest_key},
        market::InstrumentId,
        stream::stream_generation_id_with_layout,
    };
    access.permit(None, dataset)?;
    let key = manifest_key(dataset);
    let mut bytes = Vec::new();
    local.read_to(&key, None, &mut bytes)?;
    let manifest = GenerationManifest::from_json(&bytes)?;
    access.permit(Some(manifest.role), dataset)?;
    let instrument = InstrumentId {
        broker: manifest.broker.clone(),
        provider_symbol: manifest.provider_symbol.clone(),
    };
    let definition = config
        .instrument(&instrument, manifest.native_granularity)
        .ok_or_else(|| format!("no configured instrument maps {instrument}"))?;
    let stream =
        stream_generation_id_with_layout(dataset, &definition.canonical_toml(), manifest.layout);
    crate::verify::run_with(&local.uri(&key), access)?;
    crate::verify::run_with(&local.uri(&manifest_key(&stream)), access)?;
    Ok(stream)
}

/// Resumption keeps the original baseline, so its published snapshots are siblings. A later
/// snapshot supersedes one only for the same intent and with every response occurrence retained.
fn superseded_snapshots(
    local: &Store,
    manifest: &GenerationManifest,
    acquisition: &crate::fetch::OccurrenceIdentity,
    access: Access<'_>,
) -> Result<Vec<GenerationManifest>, String> {
    let Some(intent) = acquisition.intent.as_deref() else {
        return Ok(vec![]);
    };
    let mut result = Vec::new();
    for candidate in daily_candidates(local, &manifest.instrument, manifest.role, access)? {
        let lineage = read_lineage(local, &candidate)?;
        if lineage
            .pointer("/continuation/intent")
            .and_then(Value::as_str)
            != Some(intent)
        {
            continue;
        }
        if candidate.row_count > manifest.row_count
            || candidate.coverage.first_event_time < manifest.coverage.first_event_time
            || candidate.coverage.last_event_time > manifest.coverage.last_event_time
        {
            continue;
        }
        let mut retained = true;
        for old_day in candidate
            .day_inventory
            .iter()
            .filter(|d| d.family == DayFamily::Pages && d.object.is_some())
        {
            let Some(day) = manifest.day_inventory.iter().find(|d| {
                d.family == DayFamily::Pages && d.date == old_day.date && d.object.is_some()
            }) else {
                retained = false;
                break;
            };
            if old_day.object == day.object {
                continue;
            }
            let (_, old_file) = verify::fetch(local, old_object(&candidate, old_day)?, true)?;
            let (_, new_file) = verify::fetch(local, old_object(manifest, day)?, true)?;
            let new_pages = daily::read_pages(&new_file.expect("pages").path, &day.date)?;
            for old in daily::read_pages(&old_file.expect("pages").path, &old_day.date)? {
                if !new_pages.iter().any(|new| {
                    new.acquisition_id == old.acquisition_id
                        && new.ordinal == old.ordinal
                        && new.payload_sha256 == old.payload_sha256
                }) {
                    retained = false;
                    break;
                }
            }
            if !retained {
                break;
            }
        }
        if retained {
            result.push(candidate);
        }
    }
    Ok(result)
}

#[cfg(test)]
mod migration_selection_tests {
    use super::*;

    fn receipt(root: char, supersedes: Option<&str>) -> MigrationRecord {
        serde_json::from_value(json!({
            "schema_version": 1, "job": "fixture", "phase": "verified",
            "v1_generations": ["a".repeat(64)], "v1_stream": null,
            "v2_root": root.to_string().repeat(64), "v2_stream": "d".repeat(64),
            "equality": {"observations": true, "pages": true, "source_files": true, "candles": null},
            "supersedes": supersedes,
        })).unwrap()
    }

    #[test]
    fn migration_selection_follows_exact_verified_supersession() {
        // Lexical name ordering and proof version are deliberately irrelevant.
        let records = BTreeMap::from([
            ("z-old.json".into(), receipt('b', None)),
            ("a-new.json".into(), receipt('c', Some("z-old.json"))),
        ]);
        assert_eq!(
            current_migration(&records, "fixture")
                .unwrap()
                .unwrap()
                .v2_root,
            "c".repeat(64)
        );
        assert!(current_migration(&records, "unrelated").unwrap().is_none());
    }

    #[test]
    fn migration_selection_refuses_unverified_new_receipt() {
        let mut new = receipt('c', Some("old.json"));
        new.equality.pages = false;
        let records = BTreeMap::from([
            ("old.json".into(), receipt('b', None)),
            ("new.json".into(), new),
        ]);
        assert!(
            current_migration(&records, "fixture")
                .unwrap_err()
                .contains("missing verified migration evidence")
        );
    }

    #[test]
    fn migration_selection_refuses_competing_roots_and_missing_receipts() {
        let records = BTreeMap::from([
            ("first.json".into(), receipt('b', None)),
            ("second.json".into(), receipt('c', None)),
        ]);
        assert!(
            current_migration(&records, "fixture")
                .unwrap_err()
                .contains("competing verified migration records")
        );
        let records = BTreeMap::from([("new.json".into(), receipt('c', Some("absent.json")))]);
        assert!(
            current_migration(&records, "fixture")
                .unwrap_err()
                .contains("missing verified superseded migration record")
        );
    }

    #[test]
    fn migration_selection_refuses_cycles_even_with_another_terminal() {
        let records = BTreeMap::from([
            ("first.json".into(), receipt('b', Some("second.json"))),
            ("second.json".into(), receipt('c', Some("first.json"))),
            ("independent.json".into(), receipt('e', None)),
        ]);
        assert!(
            current_migration(&records, "fixture")
                .unwrap_err()
                .contains("cyclic migration supersession")
        );
    }
}

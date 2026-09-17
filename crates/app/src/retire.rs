//! Reachability retirement of ordinary pipeline data. Plans, completed records and progress
//! frames are immutable; the only removed paths are the exact inventory in a sealed plan.

use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

use binary_alpha_engine::dataset::{
    DatasetRole, GenerationManifest, Layout as DataLayout, manifest_key,
};
use binary_alpha_engine::research::Access;
use binary_alpha_engine::stream::StreamManifest;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::data_pipeline::{self, Layout, PipelineConfig};
use crate::drive::Drive;
use crate::store::{self, ObjectIdentity, Store};
use crate::verify;

// Schema 1 seals predate verified migration and reliable pending-acquisition authorization.
const SCHEMA: u32 = 2;
const BATCH: usize = 32;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Identity {
    pub bytes: u64,
    pub sha256: String,
}
impl Identity {
    fn of(path: &Path) -> Result<Self, String> {
        let id = store::identify(path)?;
        Ok(Self {
            bytes: id.bytes,
            sha256: id.sha256,
        })
    }
    fn remote(&self) -> ObjectIdentity {
        ObjectIdentity {
            bytes: self.bytes,
            sha256: self.sha256.clone(),
            crc32c: 0,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Remote {
    pub file_id: String,
    pub key: String,
    pub name: String,
    pub identity: Identity,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Local {
    /// Store-relative content key or manifest directory. No recursive, unlisted deletion.
    pub path: String,
    pub files: BTreeMap<String, Identity>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Status {
    Protected,
    Retired,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Reference {
    pub source: String,
    pub closure: String,
    pub status: Status,
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Totals {
    pub manifest_directories: usize,
    pub objects: usize,
    pub local_bytes: u64,
    pub drive_files: usize,
    pub drive_bytes: u64,
}

/// Version 2 plan: the exact pre-state, reachability decisions, retained verification roots,
/// and ordered deletion inventory. Its SHA-256 is the plan filename and progress binding.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Plan {
    pub schema_version: u32,
    pub store: PathBuf,
    pub archive_root: String,
    pub jobs: Vec<String>,
    pub state: BTreeMap<String, Identity>,
    pub remote_state: BTreeMap<String, Value>,
    pub references: Vec<Reference>,
    pub retained_manifests: Vec<String>,
    pub retained_objects: BTreeMap<String, Identity>,
    pub retained_drive: Vec<Remote>,
    pub delete_drive: Vec<Remote>,
    pub delete_local: Vec<Local>,
    pub totals: Totals,
}

struct Manifest {
    value: Value,
    instrument: String,
    daily: bool,
    dataset: bool,
    ordinary: bool,
    objects: BTreeSet<String>,
}
struct Archive {
    value: Value,
    file: Remote,
    entries: Vec<Remote>,
    dataset: String,
    stream: String,
    job: String,
}

/// Completed migration evidence in pipeline_state/records. All equalities refer to this
/// exact mapping; layout/coverage and a converted phase alone never authorize retirement.
#[derive(Deserialize)]
struct MigrationEvidence {
    schema_version: u32,
    job: String,
    phase: String,
    v1_generations: BTreeSet<String>,
    v1_stream: String,
    v2_root: String,
    v2_stream: String,
    equality: MigrationEquality,
}

#[derive(Deserialize)]
struct MigrationEquality {
    observations: bool,
    pages: bool,
    source_files: bool,
    candles: bool,
}

#[derive(Deserialize)]
struct MigrationLineage {
    v1_generations: BTreeSet<String>,
    v1_stream: String,
}

fn verified_replacements(
    records: &[MigrationEvidence],
    job: &str,
    root: &str,
    lineage: &Value,
    manifests: &BTreeMap<String, Manifest>,
    retired: &BTreeSet<String>,
) -> Result<BTreeSet<String>, String> {
    let mapping: MigrationLineage = serde_json::from_value(lineage.clone())
        .map_err(|_| format!("retire: unresolved migration mapping for {root}"))?;
    let instrument = &manifests[root].instrument;
    let valid_old = |id: &String, dataset: bool| {
        hex_id(id)
            && manifests.get(id).map_or_else(
                || retired.contains(id),
                |m| m.ordinary && !m.daily && m.dataset == dataset && &m.instrument == instrument,
            )
    };
    if mapping.v1_generations.is_empty()
        || !mapping.v1_generations.iter().all(|id| valid_old(id, true))
        || !valid_old(&mapping.v1_stream, false)
        || manifests.get(&mapping.v1_stream).is_some_and(|m| {
            !m.value["source_generation"]
                .as_str()
                .is_some_and(|id| mapping.v1_generations.contains(id))
        })
        || !records.iter().any(|record| {
            record.schema_version == 1
                && record.phase == "verified"
                && record.job == job
                && record.v1_generations == mapping.v1_generations
                && record.v1_stream == mapping.v1_stream
                && record.v2_root == root
                && record.equality.observations
                && record.equality.pages
                && record.equality.source_files
                && record.equality.candles
                && manifests.get(&record.v2_stream).is_some_and(|m| {
                    m.ordinary
                        && m.daily
                        && !m.dataset
                        && &m.instrument == instrument
                        && m.value["source_generation"] == root
                })
        })
    {
        return Err(format!(
            "retire: missing verified migration evidence for {root}"
        ));
    }
    let mut replaced = mapping.v1_generations;
    replaced.insert(mapping.v1_stream);
    Ok(replaced)
}

fn hash(bytes: &[u8]) -> String {
    binary_alpha_engine::hex(&Sha256::digest(bytes))
}
fn json(bytes: &[u8]) -> Result<Value, String> {
    serde_json::from_slice(bytes).map_err(|e| e.to_string())
}
fn string<'a>(value: &'a Value, field: &str) -> Result<&'a str, String> {
    value
        .get(field)
        .and_then(Value::as_str)
        .ok_or_else(|| format!("retire: unresolved {field}"))
}
fn hex_id(text: &str) -> bool {
    text.len() == 64 && text.bytes().all(|b| b.is_ascii_hexdigit())
}
fn valid_key(key: &str) -> bool {
    key.strip_prefix("objects/").is_some_and(hex_id)
        || key
            .strip_prefix("manifests/")
            .and_then(|s| s.strip_suffix("/ready.json"))
            .is_some_and(hex_id)
}
fn name(key: &str) -> Result<String, String> {
    if let Some(id) = key.strip_prefix("objects/").filter(|s| hex_id(s)) {
        return Ok(format!("object-{id}"));
    }
    if let Some(id) = key
        .strip_prefix("manifests/")
        .and_then(|s| s.strip_suffix("/ready.json"))
        .filter(|s| hex_id(s))
    {
        return Ok(format!("manifest-{id}.json"));
    }
    if let Some(ids) = key.strip_prefix("catalog/") {
        let parts: Vec<_> = ids.split('/').collect();
        if parts.len() == 2 && parts.iter().all(|s| hex_id(s)) {
            return Ok(format!(
                "catalog-{}-{}.json",
                &parts[0][..16],
                &parts[1][..16]
            ));
        }
    }
    Err(format!("retire: unresolved archive key {key}"))
}

/// Sorted, symlink-free traversal; never follows a store/configuration link into other data.
fn files(root: &Path, out: &mut Vec<PathBuf>) -> Result<(), String> {
    if !root.exists() {
        return Ok(());
    }
    let metadata = fs::symlink_metadata(root).map_err(|e| e.to_string())?;
    if metadata.file_type().is_symlink() {
        return Err(format!("retire: unresolved symlink {}", root.display()));
    }
    if metadata.is_file() {
        out.push(root.to_path_buf());
        return Ok(());
    }
    for entry in fs::read_dir(root).map_err(|e| e.to_string())? {
        files(&entry.map_err(|e| e.to_string())?.path(), out)?;
    }
    out.sort();
    Ok(())
}

fn config_files(
    config_path: &Path,
    config: &PipelineConfig,
    layout: &Layout,
) -> Result<Vec<PathBuf>, String> {
    fn visit(dir: &Path, managed: &Path, out: &mut Vec<PathBuf>) -> Result<(), String> {
        for entry in fs::read_dir(dir).map_err(|e| e.to_string())? {
            let entry = entry.map_err(|e| e.to_string())?;
            let path = entry.path();
            if path == managed {
                continue;
            }
            let ty = entry.file_type().map_err(|e| e.to_string())?;
            if ty.is_symlink() {
                return Err(format!(
                    "retire: unresolved configuration symlink {}",
                    path.display()
                ));
            }
            if ty.is_dir() {
                visit(&path, managed, out)?;
            } else if path.extension().is_some_and(|e| e == "toml") {
                out.push(path);
            }
        }
        Ok(())
    }
    let mut result = vec![config_path.canonicalize().map_err(|e| e.to_string())?];
    visit(
        &layout.base.canonicalize().map_err(|e| e.to_string())?,
        layout.store.parent().expect("managed root"),
        &mut result,
    )?;
    for job in &config.jobs {
        result.push(
            layout
                .base
                .join(&job.config)
                .canonicalize()
                .map_err(|e| e.to_string())?,
        );
    }
    result.sort();
    result.dedup();
    Ok(result)
}

fn state(
    config_path: &Path,
    config: &PipelineConfig,
    layout: &Layout,
) -> Result<BTreeMap<String, Identity>, String> {
    let mut paths = Vec::new();
    files(&layout.store.join("manifests"), &mut paths)?;
    for entry in fs::read_dir(&layout.state).map_err(|e| e.to_string())? {
        let entry = entry.map_err(|e| e.to_string())?;
        let n = entry.file_name();
        if n == "retirement"
            || n == "writer.lock"
            || n == "downloads"
            || n.to_string_lossy().starts_with('.')
        {
            continue;
        }
        files(&entry.path(), &mut paths)?;
    }
    paths.extend(config_files(config_path, config, layout)?);
    if let Some(uri) = &config.governance_manifest {
        let path = uri
            .strip_prefix("file://")
            .ok_or("retire: governance must be locally pinned for retirement")?;
        paths.push(PathBuf::from(path));
    }
    paths.sort();
    paths.dedup();
    paths
        .into_iter()
        .map(|p| Ok((p.to_string_lossy().into_owned(), Identity::of(&p)?)))
        .collect()
}

fn remote_state(drive: &mut Drive) -> Result<BTreeMap<String, Value>, String> {
    let mut state = BTreeMap::new();
    for file in drive.list("")? {
        let id = file.id.clone();
        let value = serde_json::json!({"name":file.name,"bytes":file.size,"sha256":file.sha256,"trashed":file.trashed});
        if state.insert(id, value).is_some() {
            return Err("retire: duplicate Drive listing identity".into());
        }
    }
    Ok(state)
}

fn objects(value: &Value) -> Result<BTreeSet<String>, String> {
    let mut result = BTreeSet::new();
    if let Some(entries) = value.get("objects").and_then(Value::as_array) {
        for entry in entries {
            let key = string(entry, "key")?;
            if !valid_key(key) {
                return Err(format!("retire: invalid object key {key}"));
            }
            result.insert(key.to_string());
        }
    }
    Ok(result)
}

fn manifests(layout: &Layout, access: Access<'_>) -> Result<BTreeMap<String, Manifest>, String> {
    let local = Store::filesystem(&layout.store);
    let mut result = BTreeMap::new();
    for generation in local.list_manifests()? {
        access.lookup(&generation)?;
        let bytes =
            fs::read(layout.store.join(manifest_key(&generation))).map_err(|e| e.to_string())?;
        let value = json(&bytes)?;
        if value.get("role").and_then(Value::as_str) == Some("holdout") {
            return Err("retire: holdout data is protected".into());
        }
        let kind = value.get("kind").and_then(Value::as_str);
        let (instrument, daily, dataset, ordinary) = match kind {
            None => {
                let m = GenerationManifest::from_json(&bytes)?;
                access.permit(Some(m.role), &generation)?;
                if m.generation != generation {
                    return Err("retire: dataset directory identity mismatch".into());
                }
                (
                    m.instrument,
                    m.layout == Some(DataLayout::DailyV2),
                    true,
                    m.role == DatasetRole::Development,
                )
            }
            Some("instrument_stream") => {
                let m = StreamManifest::from_json(&bytes)?;
                access.permit(Some(m.role), &m.source_generation)?;
                if m.generation != generation {
                    return Err("retire: stream directory identity mismatch".into());
                }
                (
                    m.instrument,
                    m.layout == Some(DataLayout::DailyV2),
                    false,
                    m.role == DatasetRole::Development,
                )
            }
            _ => (String::new(), false, false, false),
        };
        result.insert(
            generation,
            Manifest {
                objects: objects(&value)?,
                value,
                instrument,
                daily,
                dataset,
                ordinary,
            },
        );
    }
    Ok(result)
}

fn archive_entries(
    value: &Value,
    remote: &BTreeMap<String, Value>,
    result: &mut Vec<Remote>,
) -> Result<(), String> {
    match value {
        Value::Object(map) => {
            if map.contains_key("key") && map.contains_key("file_id") {
                let key = string(value, "key")?.to_string();
                let file_id = string(value, "file_id")?;
                let metadata = remote.get(file_id).ok_or_else(|| {
                    format!("retire: unresolved archive binding outside root listing: {file_id}")
                })?;
                // Immutable migration records have archive-owned names. Keep their exact
                // bindings but never turn an unfamiliar key into a deletion candidate.
                let expected_name = if key.starts_with("records/") {
                    string(metadata, "name")?.to_string()
                } else {
                    name(&key)?
                };
                let identity: Identity = serde_json::from_value(
                    serde_json::json!({"bytes":value["bytes"],"sha256":value["sha256"]}),
                )
                .map_err(|e| e.to_string())?;
                if !hex_id(&identity.sha256)
                    || key
                        .strip_prefix("objects/")
                        .is_some_and(|sha| sha != identity.sha256)
                    || metadata["name"] != expected_name
                    || metadata["bytes"].as_u64() != Some(identity.bytes)
                    || metadata["sha256"]
                        .as_str()
                        .is_some_and(|sha| !sha.eq_ignore_ascii_case(&identity.sha256))
                {
                    return Err(format!("retire: archive binding mismatch: {file_id}"));
                }
                result.push(Remote {
                    name: expected_name,
                    key,
                    file_id: file_id.into(),
                    identity,
                });
            } else {
                for v in map.values() {
                    archive_entries(v, remote, result)?;
                }
            }
        }
        Value::Array(array) => {
            for v in array {
                archive_entries(v, remote, result)?;
            }
        }
        _ => (),
    }
    Ok(())
}

fn archives(
    drive: &mut Drive,
    layout: &Layout,
    remote: &BTreeMap<String, Value>,
    access: Access<'_>,
) -> Result<Vec<Archive>, String> {
    let mut result = Vec::new();
    for (id, metadata) in remote {
        if !string(metadata, "name")?.starts_with("catalog-") {
            continue;
        }
        let confirmed = drive.listed_identity(&crate::drive::RemoteFile {
            id: id.clone(),
            name: string(metadata, "name")?.into(),
            size: metadata["bytes"].as_u64(),
            sha256: metadata["sha256"].as_str().map(str::to_string),
            trashed: metadata["trashed"].as_bool().unwrap_or(false),
        })?;
        let identity = Identity {
            bytes: confirmed.bytes,
            sha256: confirmed.sha256,
        };
        // Do not use a remote identifier as a filesystem component.
        let scratch = layout
            .state
            .join("retirement/downloads")
            .join(hash(id.as_bytes()));
        drive.download(id, &scratch, &identity.remote())?;
        let bytes = fs::read(&scratch).map_err(|e| e.to_string())?;
        fs::remove_file(&scratch).map_err(|e| e.to_string())?;
        let value = json(&bytes)?;
        let dataset = string(&value["dataset"], "generation")?.to_string();
        let stream = string(&value["stream"], "generation")?.to_string();
        if !hex_id(&dataset) || !hex_id(&stream) {
            return Err("retire: invalid catalog generation".into());
        }
        access.lookup(&dataset)?;
        access.lookup(&stream)?;
        let key = format!("catalog/{dataset}/{stream}");
        if string(metadata, "name")? != name(&key)? {
            return Err("retire: catalog name mismatch".into());
        }
        let mut entries = Vec::new();
        archive_entries(&value, remote, &mut entries)?;
        result.push(Archive {
            job: string(&value, "job")?.into(),
            file: Remote {
                key: key.clone(),
                name: name(&key)?,
                file_id: id.clone(),
                identity,
            },
            value,
            entries,
            dataset,
            stream,
        });
    }
    Ok(result)
}

/// Extract actual dependency names, preserving historical references in record inventories.
/// Lineage hashes in a daily manifest are never followed as runtime roots.
fn references(value: &Value, known: &BTreeSet<String>, out: &mut BTreeSet<String>) {
    match value {
        Value::String(s) => {
            if known.contains(s) {
                out.insert(s.clone());
            }
            if hex_id(s) && known.contains(&format!("objects/{s}")) {
                out.insert(format!("objects/{s}"));
            }
            if let Some((_, tail)) = s.rsplit_once("/manifests/")
                && let Some(id) = tail.strip_suffix("/ready.json")
            {
                out.insert(id.into());
            }
            if valid_key(s) {
                if let Some(id) = s
                    .strip_prefix("manifests/")
                    .and_then(|s| s.strip_suffix("/ready.json"))
                {
                    out.insert(id.into());
                } else {
                    out.insert(s.clone());
                }
            }
        }
        Value::Array(array) => {
            for v in array {
                references(v, known, out);
            }
        }
        Value::Object(map) => {
            for (field, v) in map {
                references(v, known, out);
                if (field.contains("generation")
                    || field == "baseline"
                    || field == "catalog"
                    || field == "file_id"
                    || field == "intent")
                    && let Some(s) = v.as_str()
                {
                    out.insert(s.into());
                }
            }
        }
        _ => (),
    }
}

fn expand(roots: &mut BTreeSet<String>, graph: &BTreeMap<String, BTreeSet<String>>) {
    loop {
        let more: Vec<_> = roots
            .iter()
            .filter_map(|r| graph.get(r))
            .flat_map(|r| r.iter().cloned())
            .collect();
        let before = roots.len();
        roots.extend(more);
        if before == roots.len() {
            break;
        }
    }
}

fn transfer_entries(
    value: &Value,
    context: Option<&str>,
    entries: &mut BTreeMap<String, Value>,
) -> Result<(), String> {
    match value {
        Value::Object(map) => {
            if (map.contains_key("done") || map.contains_key("file_id"))
                && !(map.contains_key("done") && map.contains_key("file_id"))
            {
                return Err("retire: unresolved transfer entry".into());
            }
            if map.contains_key("done") && map.contains_key("file_id") {
                let key = map
                    .get("key")
                    .and_then(Value::as_str)
                    .or(context)
                    .ok_or("retire: unresolved transfer key")?;
                map["done"]
                    .as_bool()
                    .ok_or("retire: unresolved transfer status")?;
                string(value, "file_id")?;
                if !key.starts_with("records/") {
                    name(key)?;
                }
                entries.insert(key.to_string(), value.clone());
            }
            for (key, v) in map {
                let next = map.get("key").and_then(Value::as_str).or_else(|| {
                    if key.starts_with("objects/")
                        || key.starts_with("manifests/")
                        || key.starts_with("catalog/")
                        || key.starts_with("records/")
                    {
                        Some(key.as_str())
                    } else {
                        context
                    }
                });
                transfer_entries(v, next, entries)?;
            }
        }
        Value::Array(array) => {
            for v in array {
                transfer_entries(v, context, entries)?;
            }
        }
        _ => (),
    }
    Ok(())
}

fn transfer_roots(entries: BTreeMap<String, Value>) -> BTreeSet<String> {
    let mut roots = BTreeSet::new();
    for (key, entry) in entries {
        if entry["done"] == false {
            roots.insert(key.clone());
            roots.insert(
                entry["file_id"]
                    .as_str()
                    .expect("validated transfer")
                    .to_string(),
            );
            if let Some(id) = key
                .strip_prefix("manifests/")
                .and_then(|s| s.strip_suffix("/ready.json"))
            {
                roots.insert(id.to_string());
            }
            if let Some(pair) = key.strip_prefix("catalog/") {
                roots.extend(pair.split('/').map(str::to_string));
            }
        }
    }
    roots
}

fn check_parents(value: &Value, known: &BTreeSet<String>) -> Result<(), String> {
    fn parent(value: &Value, known: &BTreeSet<String>) -> Result<(), String> {
        match value {
            Value::String(id) if hex_id(id) && !known.contains(id) => {
                return Err(format!("retire: unresolved lineage parent {id}"));
            }
            Value::Array(values) => {
                for value in values {
                    parent(value, known)?;
                }
            }
            Value::Object(fields) => {
                for (field, value) in fields {
                    if field.contains("generation") || field == "id" {
                        parent(value, known)?;
                    }
                }
            }
            _ => (),
        }
        Ok(())
    }
    match value {
        Value::Object(map) => {
            for (field, value) in map {
                if field.contains("parent")
                    || field == "prior"
                    || field == "prior_generation"
                    || field == "root_generation"
                    || field == "continuation_root"
                {
                    parent(value, known)?;
                }
                check_parents(value, known)?;
            }
        }
        Value::Array(values) => {
            for value in values {
                check_parents(value, known)?;
            }
        }
        _ => (),
    }
    Ok(())
}

fn registry_document(path: &Path, state: &Path) -> bool {
    let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
        return false;
    };
    if path.parent() == Some(state) {
        return matches!(
            name,
            "registry.json"
                | "registry.jsonl"
                | "registry.ndjson"
                | "registry.snapshot.json"
                | "registry.log.jsonl"
                | "registry.log"
                | "transfers.json"
        );
    }
    path.parent().and_then(Path::parent) == Some(state) && name == "transfers.json"
}

fn documents(path: &Path) -> Result<Vec<Value>, String> {
    let bytes = fs::read(path).map_err(|e| e.to_string())?;
    if bytes.is_empty() {
        return Ok(vec![]);
    }
    if let Ok(value) = json(&bytes) {
        return Ok(vec![value]);
    }
    if !bytes.ends_with(b"\n") {
        return Err(format!("retire: unresolved torn log {}", path.display()));
    }
    bytes
        .split(|b| *b == b'\n')
        .filter(|b| !b.is_empty())
        .map(json)
        .collect()
}

/// Historical references may already have been removed by a completed, sealed plan. This
/// affects record classification only; a live configuration or pending acquisition still pins
/// its dependencies and fails if one is absent.
pub(crate) fn retired_closures(state: &Path) -> Result<BTreeSet<String>, String> {
    let mut paths = Vec::new();
    files(&state.join("retirement"), &mut paths)?;
    let mut retired = BTreeSet::new();
    for path in paths.into_iter().filter(|p| {
        p.file_name()
            .is_some_and(|n| n.to_string_lossy().ends_with(".retired.json"))
    }) {
        let record = json(&fs::read(&path).map_err(|e| e.to_string())?)?;
        let digest = string(&record, "plan_sha256")?;
        if !hex_id(digest) {
            return Err("retire: invalid completed retirement identity".into());
        }
        let bytes = fs::read(state.join("retirement").join(format!("plan-{digest}.json")))
            .map_err(|e| e.to_string())?;
        let plan: Plan = serde_json::from_slice(&bytes).map_err(|e| e.to_string())?;
        if hash(&bytes) != digest
            || record["removed_local"]
                != serde_json::to_value(&plan.delete_local).map_err(|e| e.to_string())?
            || record["removed_drive"]
                != serde_json::to_value(&plan.delete_drive).map_err(|e| e.to_string())?
        {
            return Err("retire: completed retirement does not match sealed plan".into());
        }
        for local in plan.delete_local {
            retired.insert(local.path.clone());
            if let Some(id) = local.path.strip_prefix("manifests/") {
                retired.insert(id.to_string());
            }
            retired.extend(local.files.into_keys());
        }
        for remote in plan.delete_drive {
            retired.insert(remote.key);
            retired.insert(remote.file_id);
        }
    }
    Ok(retired)
}

/// Default is plan-only. Apply accepts only an unchanged plan previously sealed in this store.
pub fn run(
    config_path: &Path,
    job: Option<&str>,
    apply: Option<&Path>,
    out: &mut dyn Write,
) -> Result<(), String> {
    let (config, layout, _) = data_pipeline::load(config_path)?;
    let _lock = data_pipeline::writer_lock(&layout)?;
    let _archive_lock = data_pipeline::archive_lock(&config)?;
    let declaration = data_pipeline::declaration(&config)?;
    let access = Access {
        declaration: declaration.as_ref(),
        certification: None,
    };
    let mut drive = Drive::open(&config.drive)?;
    if let Some(path) = apply {
        return apply_plan(
            config_path,
            &config,
            &layout,
            &mut drive,
            access,
            job,
            path,
            out,
        );
    }
    let plan = plan(config_path, &config, &layout, &mut drive, access, job)?;
    let bytes = crate::fetch::json_bytes(&plan)?;
    let path = layout
        .state
        .join("retirement")
        .join(format!("plan-{}.json", hash(&bytes)));
    seal(&path, &bytes)?;
    writeln!(
        out,
        "retirement plan {} manifests {} objects {} local_bytes {} drive_files {} drive_bytes {}",
        path.display(),
        plan.totals.manifest_directories,
        plan.totals.objects,
        plan.totals.local_bytes,
        plan.totals.drive_files,
        plan.totals.drive_bytes
    )
    .map_err(|e| e.to_string())
}

fn plan(
    config_path: &Path,
    config: &PipelineConfig,
    layout: &Layout,
    drive: &mut Drive,
    access: Access<'_>,
    job_filter: Option<&str>,
) -> Result<Plan, String> {
    if job_filter.is_some_and(|id| !config.jobs.iter().any(|job| job.id == id)) {
        return Err("retire: unknown job".into());
    }
    let before = state(config_path, config, layout)?;
    let already_retired = retired_closures(&layout.state)?;
    let remote = remote_state(drive)?;
    let manifests = manifests(layout, access)?;
    let archives = archives(drive, layout, &remote, access)?;
    let mut record_paths = Vec::new();
    files(&layout.state.join("records"), &mut record_paths)?;
    let mut migrations = Vec::new();
    for path in &record_paths {
        for value in documents(path)? {
            if let Ok(record) = serde_json::from_value::<MigrationEvidence>(value) {
                migrations.push(record);
            }
        }
    }
    let mut selected = BTreeMap::new();
    for job in &config.jobs {
        if job_filter.is_some_and(|id| id != job.id) {
            continue;
        }
        let core = crate::load_config(&layout.base.join(&job.config))?;
        let history = core
            .history
            .as_ref()
            .ok_or("retire: job requires history binding")?;
        let [symbol] = history.instruments.as_slice() else {
            return Err("retire: job requires one instrument".into());
        };
        let instrument = format!("{}:{symbol}", history.broker);
        let candidates: Vec<_> = archives
            .iter()
            .filter(|a| {
                a.job == job.id
                    && a.value["layout"] == "daily-v2"
                    && a.value["instrument"] == instrument
                    && a.value["role"] == "development"
            })
            .map(|a| {
                Ok((
                    a.file.file_id.clone(),
                    a.file.identity.sha256.clone(),
                    data_pipeline::Catalog::from_json(
                        &serde_json::to_vec(&a.value).map_err(|e| e.to_string())?,
                    )
                    .map_err(|e| {
                        format!(
                            "retire: catalog lacks manifest binding or has invalid envelope: {e}"
                        )
                    })?,
                ))
            })
            .collect::<Result<Vec<_>, String>>()?;
        if !candidates.is_empty() {
            let index = crate::lineage::newest_catalog(
                &candidates,
                drive,
                &layout.state.join("downloads"),
                access,
            )?;
            let catalog = &candidates[index].2;
            if !manifests.contains_key(&catalog.dataset.generation)
                || !manifests.contains_key(&catalog.stream.generation)
            {
                return Err(
                    "retire: newest archived closure must be restored locally before planning"
                        .into(),
                );
            }
            selected.insert(job.id.clone(), (instrument, candidates[index].0.clone()));
        }
    }
    // A daily layout alone does not prove that old history was replaced. Require the root's
    // explicit provenance mapping and retain every legacy generation outside that mapping.
    let generation_names: BTreeSet<_> = manifests.keys().cloned().collect();
    let resolved_lineage_names: BTreeSet<_> = generation_names
        .iter()
        .chain(already_retired.iter())
        .cloned()
        .collect();
    let mut replaced = BTreeSet::new();
    let mut continuation_seeds = BTreeSet::new();
    for (job, (instrument, catalog_id)) in &selected {
        let archive = archives
            .iter()
            .find(|a| &a.file.file_id == catalog_id)
            .expect("selected catalog");
        let newest = &manifests[&archive.dataset];
        let mut lineage = BTreeMap::new();
        let mut parents = BTreeMap::new();
        for (id, m) in &manifests {
            if m.daily && m.dataset && &m.instrument == instrument {
                verify::run_with(
                    &Store::filesystem(&layout.store).uri(&manifest_key(id)),
                    access,
                )?;
                let mut refs = BTreeSet::new();
                check_parents(&m.value, &resolved_lineage_names)?;
                references(&m.value, &generation_names, &mut refs);
                for object in m.value["objects"]
                    .as_array()
                    .ok_or("retire: missing daily objects")?
                {
                    if object["path"] == "provenance/lineage.json" {
                        let value = json(
                            &fs::read(layout.store.join(string(object, "key")?))
                                .map_err(|e| e.to_string())?,
                        )?;
                        let mut resolved = resolved_lineage_names.clone();
                        if value.get("continuation").is_some() {
                            if let Some(ids) = value.get("ancestors").and_then(Value::as_array) {
                                resolved.extend(
                                    ids.iter()
                                        .filter_map(Value::as_str)
                                        .filter(|s| hex_id(s))
                                        .map(str::to_string),
                                );
                            }
                            if let Some(root) = value
                                .get("root_generation")
                                .and_then(Value::as_str)
                                .filter(|s| hex_id(s))
                            {
                                resolved.insert(root.into());
                            }
                        }
                        check_parents(&value, &resolved)?;
                        references(&value, &generation_names, &mut refs);
                        lineage.insert(id.clone(), value);
                    }
                }
                refs.remove(id);
                refs.retain(|r| {
                    manifests.get(r).is_some_and(|parent| {
                        parent.daily && parent.dataset && &parent.instrument == instrument
                    })
                });
                parents.insert(id.clone(), refs);
            }
        }
        let seed = crate::lineage::root(
            &Store::filesystem(&layout.store),
            instrument,
            DatasetRole::Development,
            access,
        )?
        .ok_or_else(|| format!("retire: unresolved continuation lineage for {instrument}"))?;
        continuation_seeds.insert(seed.clone());
        let mut ancestors = BTreeSet::from([archive.dataset.clone()]);
        expand(&mut ancestors, &parents);
        if !ancestors.contains(&seed) || !lineage.contains_key(&seed) {
            return Err(format!(
                "retire: unresolved continuation lineage for {instrument}"
            ));
        }
        replaced.extend(
            ancestors
                .iter()
                .filter(|id| **id != seed && **id != archive.dataset)
                .cloned(),
        );
        if let Some(value) = lineage.get(&archive.dataset) {
            let logical_root = value
                .get("root_generation")
                .and_then(Value::as_str)
                .unwrap_or(&seed);
            for id in value
                .get("ancestors")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
                .filter_map(Value::as_str)
                .filter(|id| *id != logical_root && *id != seed && *id != archive.dataset)
            {
                // An ancestry claim never grants v1 deletion authority. Local daily
                // ancestors were verified above; remote-only ones need a pinned manifest.
                if let Some(manifest) = manifests.get(id) {
                    if manifest.daily
                        && manifest.dataset
                        && manifest.ordinary
                        && &manifest.instrument == instrument
                    {
                        replaced.insert(id.to_string());
                    }
                    continue;
                }
                if let Some(prior) = archives.iter().find(|a| {
                    a.dataset == id
                        && a.value["layout"] == "daily-v2"
                        && a.value["role"] == "development"
                        && a.value["instrument"] == *instrument
                }) {
                    let catalog = data_pipeline::Catalog::from_json(
                        &serde_json::to_vec(&prior.value).map_err(|e| e.to_string())?,
                    )?;
                    access.permit(Some(catalog.role), id)?;
                    let entry = &catalog.dataset;
                    let scratch = layout
                        .state
                        .join("retirement/downloads")
                        .join(format!("{}.manifest", entry.sha256));
                    drive.download(
                        &entry.file_id,
                        &scratch,
                        &ObjectIdentity {
                            bytes: entry.bytes,
                            sha256: entry.sha256.clone(),
                            crc32c: 0,
                        },
                    )?;
                    let bytes = fs::read(&scratch).map_err(|e| e.to_string())?;
                    fs::remove_file(scratch).map_err(|e| e.to_string())?;
                    let manifest = GenerationManifest::from_json(&bytes)?;
                    if manifest.generation != id
                        || manifest.layout != Some(DataLayout::DailyV2)
                        || manifest.role != DatasetRole::Development
                        || &manifest.instrument != instrument
                    {
                        return Err(
                            "retire: archive ancestor is not the claimed daily dataset".into()
                        );
                    }
                    replaced.insert(id.to_string());
                }
            }
        }
        // Native daily roots need no migration authorization. If legacy manifests are
        // present, only the root's verified migration mapping can authorize their removal.
        let mut legacy_refs = BTreeSet::new();
        references(&lineage[&seed], &generation_names, &mut legacy_refs);
        if legacy_refs
            .iter()
            .any(|id| manifests.get(id).is_some_and(|m| !m.daily))
        {
            let verified = verified_replacements(
                &migrations,
                job,
                &seed,
                &lineage[&seed],
                &manifests,
                &already_retired,
            )?;
            for id in &verified {
                if let Some(old) = manifests.get(id)
                    && old.dataset
                    && newest.value["coverage"]["first_event_time"].as_str()
                        <= old.value["coverage"]["first_event_time"].as_str()
                    && newest.value["coverage"]["last_event_time"].as_str()
                        >= old.value["coverage"]["last_event_time"].as_str()
                {
                    replaced.insert(id.clone());
                }
            }
            for id in verified {
                if let Some(old) = manifests.get(&id)
                    && !old.dataset
                    && old.value["source_generation"]
                        .as_str()
                        .is_some_and(|source| replaced.contains(source))
                {
                    replaced.insert(id);
                }
            }
        }
    }
    for (id, m) in &manifests {
        if m.daily
            && !m.dataset
            && m.ordinary
            && m.value["source_generation"]
                .as_str()
                .is_some_and(|source| replaced.contains(source))
        {
            replaced.insert(id.clone());
        }
    }
    let mut known: BTreeSet<String> = manifests.keys().cloned().collect();
    let mut graph = BTreeMap::new();
    let mut roots = BTreeSet::new();
    let mut candidates = BTreeSet::new();
    let mut inventory = Vec::new();
    for (generation, m) in &manifests {
        known.extend(m.objects.iter().cloned());
        known.insert(manifest_key(generation));
        let mut closure = m.objects.clone();
        closure.insert(manifest_key(generation));
        if !m.dataset
            && let Some(source) = m.value.get("source_generation").and_then(Value::as_str)
        {
            closure.insert(source.into());
        }
        graph.insert(generation.clone(), closure);
        graph.insert(
            manifest_key(generation),
            BTreeSet::from([generation.clone()]),
        );
        if m.ordinary && replaced.contains(generation) && !continuation_seeds.contains(generation) {
            candidates.insert(generation.clone());
        } else {
            roots.insert(generation.clone());
        }
    }
    for a in &archives {
        known.insert(a.dataset.clone());
        known.insert(a.stream.clone());
        known.insert(a.file.file_id.clone());
        known.insert(a.file.key.clone());
        let mut closure: BTreeSet<_> =
            [a.dataset.clone(), a.stream.clone(), a.file.key.clone()].into();
        for e in &a.entries {
            known.insert(e.key.clone());
            known.insert(e.file_id.clone());
            closure.insert(e.key.clone());
            closure.insert(e.file_id.clone());
            if e.key.starts_with("records/") {
                roots.insert(e.file_id.clone());
                roots.insert(e.key.clone());
            }
        }
        graph.insert(a.file.file_id.clone(), closure.clone());
        graph.insert(a.file.key.clone(), closure);
        if selected
            .get(&a.job)
            .is_some_and(|(_, id)| id != &a.file.file_id)
            && a.value["role"] == "development"
            && (replaced.contains(&a.dataset) || manifests.get(&a.dataset).is_some_and(|m| m.daily))
        {
            candidates.insert(a.file.file_id.clone());
        } else {
            roots.insert(a.file.file_id.clone());
        }
    }
    // Inventory all immutable records, including migration records regardless of their prefix.
    let mut records = Vec::new();
    for path in record_paths {
        let id = path
            .file_name()
            .ok_or("retire: record name unresolved")?
            .to_string_lossy()
            .into_owned();
        known.insert(id.clone());
        records.push((path, id));
    }
    for (path, id) in &records {
        let values = documents(path)?;
        let mut refs = BTreeSet::new();
        for value in &values {
            references(value, &known, &mut refs);
        }
        graph.insert(id.clone(), refs);
    }
    // Research and other non-pipeline manifests retain their complete explicit dependencies.
    for (id, m) in &manifests {
        if !m.ordinary {
            let mut refs = BTreeSet::new();
            references(&m.value, &known, &mut refs);
            graph.entry(id.clone()).or_default().extend(refs);
        }
    }
    // Configuration references are live roots, never retired by interpreting an old receipt.
    for path in config_files(config_path, config, layout)? {
        let text = fs::read_to_string(&path).map_err(|e| e.to_string())?;
        let value: toml::Value =
            toml::from_str(&text).map_err(|e| format!("retire: {}: {e}", path.display()))?;
        let mut refs = BTreeSet::new();
        references(
            &serde_json::to_value(value).map_err(|e| e.to_string())?,
            &known,
            &mut refs,
        );
        for r in refs {
            roots.insert(r.clone());
            inventory.push((path.display().to_string(), r, "configuration reference"));
        }
    }
    let (mut transfer_reservations, unbound) =
        crate::registry::in_flight(&layout.state, &config.drive)?;
    roots.extend(transfer_reservations.iter().cloned());
    if unbound {
        roots.extend(manifests.keys().cloned());
        roots.extend(archives.iter().map(|a| a.file.file_id.clone()));
    }
    for root in &transfer_reservations {
        inventory.push((
            layout.state.join("registry").display().to_string(),
            root.clone(),
            "in-flight transfer",
        ));
    }
    let mut registries: BTreeMap<PathBuf, BTreeMap<String, Value>> = BTreeMap::new();
    // Mutable state is a pin. Registry entries have explicit completion status; only unfinished
    // transfers pin data. A torn/unrecognized state document blocks planning instead of deletion.
    let mut mutable_paths: Vec<_> = before
        .keys()
        .map(PathBuf::from)
        .filter(|p| p.starts_with(&layout.state) && !p.starts_with(layout.state.join("records")))
        .collect();
    // Replay the compact snapshot before the append-only tail, then keep the latest entry
    // per content key. Completed transfers must supersede their old in-flight reservations.
    mutable_paths.sort_by_key(|p| {
        (
            p.extension().is_some_and(|e| e == "jsonl" || e == "ndjson")
                || p.file_name()
                    .is_some_and(|n| n.to_string_lossy().contains("log")),
            p.clone(),
        )
    });
    for path in mutable_paths {
        if path
            .file_name()
            .is_some_and(|n| n.to_string_lossy().starts_with("reclaim-"))
        {
            roots.extend(crate::lineage::reclamation_roots(&path)?);
            continue;
        }
        if path.starts_with(layout.state.join("registry"))
            || (path.parent().and_then(Path::parent) == Some(layout.state.as_path())
                && path.file_name().is_some_and(|n| n == "transfers.json"))
        {
            continue;
        }
        // Generated execution configs are fingerprints, not operator pins after completion.
        if path.extension().is_some_and(|e| e == "toml") {
            if path
                .parent()
                .is_some_and(|p| p.join("progress.json").exists())
            {
                let source = fs::read_to_string(&path).map_err(|e| e.to_string())?;
                let value: toml::Value = toml::from_str(&source).map_err(|e| e.to_string())?;
                let mut refs = BTreeSet::new();
                references(
                    &serde_json::to_value(value).map_err(|e| e.to_string())?,
                    &known,
                    &mut refs,
                );
                roots.extend(refs);
            }
            continue;
        }
        if path.extension().is_some_and(|e| e == "lock") {
            continue;
        }
        // Acquisition references are inventoried independently below, regardless of any
        // ancestor directory names or registry formats.
        if path
            .file_name()
            .is_some_and(|n| n.to_string_lossy().starts_with("progress"))
        {
            continue;
        }
        let values = documents(&path)?;
        let registry = registry_document(&path, &layout.state);
        for value in values {
            let mut refs = BTreeSet::new();
            if registry {
                transfer_entries(
                    &value,
                    None,
                    registries
                        .entry(path.parent().expect("registry parent").to_path_buf())
                        .or_default(),
                )?;
            } else {
                references(&value, &known, &mut refs);
            }
            for r in refs {
                roots.insert(r.clone());
                inventory.push((
                    path.display().to_string(),
                    r,
                    "pending acquisition or in-flight transfer",
                ));
            }
        }
    }
    for (path, entries) in registries {
        for r in transfer_roots(entries) {
            transfer_reservations.insert(r.clone());
            roots.insert(r.clone());
            inventory.push((path.display().to_string(), r, "in-flight transfer"));
        }
    }
    // Pending page hashes may not be referenced by a published manifest yet. Resolve each
    // hash from its page record to the retained standalone content key.
    for path in before.keys().map(PathBuf::from).filter(|p| {
        p.starts_with(&layout.state)
            && p.file_name()
                .is_some_and(|n| n.to_string_lossy().starts_with("progress"))
    }) {
        fn page_keys(v: &Value, keys: &mut BTreeSet<String>) {
            if let Value::Object(map) = v {
                if map.contains_key("rows")
                    && let Some(sha) = map
                        .get("sha256")
                        .and_then(Value::as_str)
                        .filter(|s| hex_id(s))
                {
                    keys.insert(format!("objects/{sha}"));
                }
                for v in map.values() {
                    page_keys(v, keys);
                }
            } else if let Value::Array(array) = v {
                for v in array {
                    page_keys(v, keys);
                }
            }
        }
        for value in documents(&path)? {
            let mut refs = BTreeSet::new();
            references(&value, &known, &mut refs);
            for r in refs {
                roots.insert(r.clone());
                inventory.push((path.display().to_string(), r, "pending acquisition"));
            }
            page_keys(&value, &mut roots);
        }
    }
    // Completed records remain immutable evidence, not authority to collect arbitrary objects.
    // Only mapped manifest/catalog closures are candidates. Receipt-only pages, unresolved
    // records, and records belonging to another job pin their resolved dependencies.
    let mut eligible = candidates.clone();
    expand(&mut eligible, &graph);
    for (path, id) in &records {
        let mut selected_record = false;
        for value in documents(path)? {
            selected_record |= value
                .get("job")
                .and_then(Value::as_str)
                .is_some_and(|j| selected.contains_key(j));
            fn retained_pages(v: &Value, out: &mut BTreeSet<String>, store: &Path) {
                match v {
                    Value::Object(map) => {
                        if let Some(sha) = map
                            .get("sha256")
                            .and_then(Value::as_str)
                            .filter(|s| hex_id(s))
                        {
                            let key = format!("objects/{sha}");
                            if store.join(&key).is_file() {
                                out.insert(key);
                            }
                        }
                        for v in map.values() {
                            retained_pages(v, out, store);
                        }
                    }
                    Value::Array(a) => {
                        for v in a {
                            retained_pages(v, out, store);
                        }
                    }
                    _ => (),
                }
            }
            retained_pages(&value, graph.entry(id.clone()).or_default(), &layout.store);
        }
        let mut closure = BTreeSet::from([id.clone()]);
        expand(&mut closure, &graph);
        // Catalog receipts have no job field; their exact catalog identifies the job.
        selected_record |= archives
            .iter()
            .any(|a| selected.contains_key(&a.job) && closure.contains(&a.file.file_id));
        let resolved = |dependency: &String| {
            graph.contains_key(dependency)
                || known.contains(dependency)
                || remote.contains_key(dependency)
                || (valid_key(dependency) && layout.store.join(dependency).is_file())
        };
        let unresolved = closure
            .iter()
            .any(|dependency| !resolved(dependency) && !already_retired.contains(dependency));
        if unresolved || !selected_record {
            // Unknown evidence cannot authorize partial retirement of a record's closure.
            roots.extend(
                closure
                    .iter()
                    .filter(|d| resolved(d) && !records.iter().any(|(_, id)| id == *d))
                    .cloned(),
            );
        }
        for dependency in &closure {
            if dependency.starts_with("objects/") && !eligible.contains(dependency) {
                roots.insert(dependency.clone());
            }
        }
    }
    expand(&mut roots, &graph);
    expand(&mut candidates, &graph);
    // A missing closure is unresolved, including a pinned seed or dangling non-pipeline input.
    // In-flight reservations are the only roots permitted to precede publication.
    for root in &roots {
        if hex_id(root) && !manifests.contains_key(root) && !transfer_reservations.contains(root) {
            return Err(format!("retire: unresolved retained generation {root}"));
        }
        if root.starts_with("objects/")
            && !layout.store.join(root).is_file()
            && !transfer_reservations.contains(root)
        {
            return Err(format!("retire: unresolved retained object {root}"));
        }
    }
    for (id, m) in &manifests {
        if candidates.contains(id) {
            for key in &m.objects {
                if !layout.store.join(key).is_file() {
                    return Err(format!("retire: unresolved candidate dependency {key}"));
                }
            }
        }
    }
    let mut retained_manifests = Vec::new();
    for (id, m) in &manifests {
        if roots.contains(id) && m.ordinary {
            verify::run_with(
                &Store::filesystem(&layout.store).uri(&manifest_key(id)),
                access,
            )?;
            retained_manifests.push(manifest_key(id));
        }
    }
    let mut retained_objects = BTreeMap::new();
    for key in roots.iter().filter(|r| r.starts_with("objects/")) {
        if layout.store.join(key).is_file() {
            retained_objects.insert(key.clone(), Identity::of(&layout.store.join(key))?);
        }
    }
    let mut retained_drive = BTreeMap::new();
    let mut delete_drive = BTreeMap::new();
    for a in &archives {
        // Eligible retained catalogs must bind exactly the verified local manifests and every
        // object in them. Extra v2 closure entries (lineage manifests/records) remain pinned.
        if selected.values().any(|(_, id)| id == &a.file.file_id) {
            let dataset = &manifests[&a.dataset].value;
            let stream = &manifests[&a.stream].value;
            if stream["source_generation"] != a.dataset
                || a.value["role"] != dataset["role"]
                || a.value["instrument"] != dataset["instrument"]
                || a.value["broker"] != dataset["broker"]
                || a.value["provider_symbol"] != dataset["provider_symbol"]
                || a.value["source_kind"] != dataset["source_kind"]
                || a.value["native_granularity"] != dataset["native_granularity"]
                || a.value["coverage"] != dataset["coverage"]
                || a.value["row_count"] != dataset["row_count"]
                || a.value["dataset"]["key"] != manifest_key(&a.dataset)
                || a.value["stream"]["key"] != manifest_key(&a.stream)
            {
                return Err("retire: retained catalog does not bind its dataset and stream".into());
            }
            for id in [&a.dataset, &a.stream] {
                if !a.entries.iter().any(|e| e.key == manifest_key(id)) {
                    return Err("retire: catalog lacks manifest binding".into());
                }
            }
            for entry in a.entries.iter().filter(|e| e.key.starts_with("manifests/")) {
                let key = &entry.key;
                let id = key
                    .strip_prefix("manifests/")
                    .and_then(|s| s.strip_suffix("/ready.json"))
                    .ok_or("retire: invalid archived manifest key")?;
                let manifest = manifests
                    .get(id)
                    .ok_or("retire: unresolved catalog lineage manifest")?;
                if Identity::of(&layout.store.join(key))? != entry.identity {
                    return Err("retire: catalog/local manifest mismatch".into());
                }
                for key in &manifest.objects {
                    let e = a
                        .entries
                        .iter()
                        .find(|e| &e.key == key)
                        .ok_or("retire: catalog lacks retained object")?;
                    if Identity::of(&layout.store.join(key))? != e.identity {
                        return Err("retire: catalog/local object mismatch".into());
                    }
                }
            }
        }
        for entry in std::iter::once(&a.file).chain(&a.entries) {
            if roots.contains(&entry.file_id) || roots.contains(&entry.key) {
                retained_drive.insert(entry.file_id.clone(), entry.clone());
            } else if candidates.contains(&entry.file_id) || candidates.contains(&entry.key) {
                delete_drive.insert(entry.file_id.clone(), entry.clone());
            }
        }
    }
    for id in retained_drive.keys() {
        delete_drive.remove(id);
    }
    let mut delete_local = Vec::new();
    for id in manifests
        .keys()
        .filter(|id| candidates.contains(*id) && !roots.contains(*id))
    {
        let relative = format!("manifests/{id}");
        let mut paths = Vec::new();
        files(&layout.store.join(&relative), &mut paths)?;
        let mut members = BTreeMap::new();
        for path in paths {
            members.insert(
                path.strip_prefix(&layout.store)
                    .map_err(|e| e.to_string())?
                    .to_string_lossy()
                    .into_owned(),
                Identity::of(&path)?,
            );
        }
        delete_local.push(Local {
            path: relative,
            files: members,
        });
    }
    for key in candidates
        .iter()
        .filter(|r| r.starts_with("objects/") && !roots.contains(*r))
    {
        if !valid_key(key) {
            return Err("retire: unresolved candidate key".into());
        }
        let path = layout.store.join(key);
        if path.is_file() {
            delete_local.push(Local {
                path: key.clone(),
                files: BTreeMap::from([(key.clone(), Identity::of(&path)?)]),
            });
        }
    }
    let mut refs = Vec::new();
    for (source, id, why) in inventory {
        refs.push(Reference {
            source,
            closure: id,
            status: Status::Protected,
            reason: why.into(),
        });
    }
    for (path, id) in records {
        let mut closure = BTreeSet::from([id.clone()]);
        expand(&mut closure, &graph);
        let any_retired = closure
            .iter()
            .any(|r| !roots.contains(r) && (candidates.contains(r) || already_retired.contains(r)));
        refs.push(Reference {
            source: path.display().to_string(),
            closure: id.clone(),
            status: if any_retired {
                Status::Retired
            } else {
                Status::Protected
            },
            reason: if any_retired {
                "historical record preserved; superseded closure retired"
            } else {
                "immutable record preserved; retained or unresolved closure"
            }
            .into(),
        });
        for r in closure.into_iter().filter(|r| r != &id) {
            let retired =
                !roots.contains(&r) && (candidates.contains(&r) || already_retired.contains(&r));
            refs.push(Reference {
                source: path.display().to_string(),
                closure: r,
                status: if retired {
                    Status::Retired
                } else {
                    Status::Protected
                },
                reason: if retired {
                    "unreachable from retained roots"
                } else {
                    "retained root, shared content, or unresolved dependency"
                }
                .into(),
            });
        }
    }
    for (id, m) in &manifests {
        refs.push(Reference {
            source: manifest_key(id),
            closure: id.clone(),
            status: if roots.contains(id) {
                Status::Protected
            } else {
                Status::Retired
            },
            reason: if m.daily {
                "daily-v2 lineage"
            } else if roots.contains(id) {
                "configuration, pending, unselected job, or non-pipeline dependency"
            } else {
                "superseded by verified archived daily-v2 lineage"
            }
            .into(),
        });
    }
    let delete_drive: Vec<_> = delete_drive.into_values().collect();
    let totals = Totals {
        manifest_directories: delete_local
            .iter()
            .filter(|e| e.path.starts_with("manifests/"))
            .count(),
        objects: delete_local
            .iter()
            .filter(|e| e.path.starts_with("objects/"))
            .count(),
        local_bytes: delete_local
            .iter()
            .flat_map(|e| e.files.values())
            .map(|e| e.bytes)
            .sum(),
        drive_files: delete_drive.len(),
        drive_bytes: delete_drive.iter().map(|e| e.identity.bytes).sum(),
    };
    let plan = Plan {
        schema_version: SCHEMA,
        store: layout.store.clone(),
        archive_root: config.drive.root_folder_id.clone(),
        jobs: selected.into_keys().collect(),
        state: before.clone(),
        remote_state: remote,
        references: refs,
        retained_manifests,
        retained_objects,
        retained_drive: retained_drive.into_values().collect(),
        delete_drive,
        delete_local,
        totals,
    };
    verify_retained(&plan, drive, access)?;
    if before != state(config_path, config, layout)? || plan.remote_state != remote_state(drive)? {
        return Err("retire: store changed during planning".into());
    }
    Ok(plan)
}

fn verify_retained(plan: &Plan, drive: &mut Drive, access: Access<'_>) -> Result<(), String> {
    let local = Store::filesystem(&plan.store);
    for key in &plan.retained_manifests {
        verify::run_with(&local.uri(key), access)?;
    }
    for (key, identity) in &plan.retained_objects {
        if Identity::of(&plan.store.join(key))? != *identity {
            return Err(format!("retire: retained object changed {key}"));
        }
    }
    for e in &plan.retained_drive {
        let file = drive.verify(&e.file_id, &e.identity.remote())?;
        if file.name != e.name {
            return Err(format!(
                "retire: retained remote name changed {}",
                e.file_id
            ));
        }
    }
    Ok(())
}

/// Deleting exact unreachable leaves cannot touch retained closures. Include directory
/// descendants and remote identities in this check; sharing a content digest never permits
/// removing the retained binding. This avoids decoding the whole archive after every batch.
fn touches_retained(plan: &Plan, start: usize, end: usize) -> bool {
    (start..end).any(|index| {
        if index < plan.delete_drive.len() {
            let removed = &plan.delete_drive[index];
            plan.retained_drive
                .iter()
                .any(|kept| kept.file_id == removed.file_id)
        } else {
            let removed = &plan.delete_local[index - plan.delete_drive.len()];
            plan.retained_manifests
                .iter()
                .chain(plan.retained_objects.keys())
                .any(|key| {
                    Path::new(key).starts_with(&removed.path) || removed.files.contains_key(key)
                })
        }
    })
}

fn seal(path: &Path, bytes: &[u8]) -> Result<(), String> {
    let parent = path.parent().ok_or("retire: missing record parent")?;
    fs::create_dir_all(parent).map_err(|e| e.to_string())?;
    // Publish via hard link: a crash during the scratch write can never leave a partial sealed
    // plan/record, and publication never replaces an existing immutable identity.
    let mut collision = 0_u64;
    let (scratch, mut file) = loop {
        let scratch = parent.join(format!(".seal-{}-{collision}", std::process::id()));
        match OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&scratch)
        {
            Ok(file) => break (scratch, file),
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
                collision = collision
                    .checked_add(1)
                    .ok_or("retire: seal names exhausted")?;
            }
            Err(e) => return Err(e.to_string()),
        }
    };
    file.write_all(bytes)
        .and_then(|()| file.sync_all())
        .map_err(|e| e.to_string())?;
    match fs::hard_link(&scratch, path) {
        Ok(()) => (),
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
            if fs::read(path).map_err(|e| e.to_string())? != bytes {
                return Err("retire: immutable record conflict".into());
            }
        }
        Err(e) => return Err(e.to_string()),
    }
    fs::remove_file(&scratch).map_err(|e| e.to_string())?;
    File::open(parent)
        .and_then(|f| f.sync_all())
        .map_err(|e| e.to_string())
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Event {
    plan: String,
    index: usize,
    phase: String,
}

/// Record-separator framing lets a torn final append remain untouched. A subsequent complete
/// frame resumes the same operation; all durable frames must still form begin/done pairs.
fn progress(path: &Path, digest: &str) -> Result<(usize, bool), String> {
    let bytes = match fs::read(path) {
        Ok(b) => b,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok((0, false)),
        Err(e) => return Err(e.to_string()),
    };
    let mut index = 0;
    let mut active = false;
    for frame in bytes.split(|b| *b == 0x1e).filter(|b| !b.is_empty()) {
        if !frame.ends_with(b"\n") {
            continue;
        }
        let event: Event =
            serde_json::from_slice(frame).map_err(|e| format!("retire: invalid progress: {e}"))?;
        if event.plan != digest || event.index != index {
            return Err("retire: progress identity/order mismatch".into());
        }
        match (event.phase.as_str(), active) {
            ("begin", false) => active = true,
            ("done", true) => {
                active = false;
                index += 1;
            }
            _ => return Err("retire: progress phase mismatch".into()),
        }
    }
    Ok((index, active))
}
fn append(path: &Path, digest: &str, index: usize, phase: &str) -> Result<(), String> {
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .map_err(|e| e.to_string())?;
    let event = Event {
        plan: digest.into(),
        index,
        phase: phase.into(),
    };
    let mut bytes = vec![0x1e];
    bytes.extend(serde_json::to_vec(&event).map_err(|e| e.to_string())?);
    bytes.push(b'\n');
    file.write_all(&bytes)
        .and_then(|()| file.sync_all())
        .map_err(|e| e.to_string())?;
    File::open(path.parent().expect("progress parent"))
        .and_then(|f| f.sync_all())
        .map_err(|e| e.to_string())
}

#[allow(clippy::too_many_arguments)]
fn apply_plan(
    config_path: &Path,
    config: &PipelineConfig,
    layout: &Layout,
    drive: &mut Drive,
    access: Access<'_>,
    job: Option<&str>,
    path: &Path,
    out: &mut dyn Write,
) -> Result<(), String> {
    let bytes = fs::read(path).map_err(|e| e.to_string())?;
    let digest = hash(&bytes);
    let sealed = layout
        .state
        .join("retirement")
        .join(format!("plan-{digest}.json"));
    if fs::read(&sealed).map_err(|_| "retire: plan is not sealed in this store")? != bytes {
        return Err("retire: altered plan".into());
    }
    let plan: Plan = serde_json::from_slice(&bytes).map_err(|e| e.to_string())?;
    if plan.schema_version != SCHEMA {
        return Err("retire: unsupported plan schema; a new retirement plan is required".into());
    }
    if plan.store != layout.store
        || plan.archive_root != config.drive.root_folder_id
        || job.is_some_and(|j| plan.jobs != [j])
    {
        return Err("retire: plan scope mismatch".into());
    }
    let log = sealed.with_extension("progress.jsonseq");
    let (mut index, mut active) = progress(&log, &digest)?;
    let count = plan.delete_drive.len() + plan.delete_local.len();
    if touches_retained(&plan, 0, count) {
        return Err("retire: deletion inventory overlaps a retained closure".into());
    }
    if index > count || (index == count && active) {
        return Err("retire: invalid progress length".into());
    }
    // Only a write-ahead authorized item may be absent on resumption. Every other state byte
    // must still equal the original plan, including records and transfer-registry checkpoints.
    let mut expected = plan.state.clone();
    let mut actual = state(config_path, config, layout)?;
    for (i, item) in plan.delete_local.iter().enumerate() {
        let op = plan.delete_drive.len() + i;
        if op < index || (op == index && active) {
            for key in item.files.keys() {
                let p = plan.store.join(key).to_string_lossy().into_owned();
                if op < index || !actual.contains_key(&p) {
                    expected.remove(&p);
                }
            }
        }
    }
    if expected != actual {
        return Err(
            "retire: stale plan: manifest, configuration, record, or registry changed".into(),
        );
    }
    let mut expected_remote = plan.remote_state.clone();
    let actual_remote = remote_state(drive)?;
    for (i, item) in plan.delete_drive.iter().enumerate() {
        if i < index || (i == index && active && !actual_remote.contains_key(&item.file_id)) {
            expected_remote.remove(&item.file_id);
        }
    }
    if expected_remote != actual_remote {
        return Err("retire: stale plan: archive state changed".into());
    }
    // Check every surviving local deletion target before any further destructive operation.
    for (i, item) in plan.delete_local.iter().enumerate() {
        let op = plan.delete_drive.len() + i;
        for (key, identity) in &item.files {
            let p = plan.store.join(key);
            if !p.exists() && (op < index || (op == index && active)) {
                continue;
            }
            if op < index || Identity::of(&p)? != *identity {
                return Err(format!("retire: stale plan: local target changed {key}"));
            }
        }
    }
    verify_retained(&plan, drive, access)?;
    while index < count {
        let start = index;
        let end = (index + BATCH)
            .min(count)
            .min(if index < plan.delete_drive.len() {
                plan.delete_drive.len()
            } else {
                count
            });
        while index < end {
            if !active {
                append(&log, &digest, index, "begin")?;
            }
            if index < plan.delete_drive.len() {
                let item = &plan.delete_drive[index];
                drive.delete_named(&item.file_id, &item.name, &item.identity.remote())?;
            } else {
                let item = &plan.delete_local[index - plan.delete_drive.len()];
                for (key, identity) in &item.files {
                    let p = plan.store.join(key);
                    if p.exists() {
                        if Identity::of(&p)? != *identity {
                            return Err(format!("retire: local target changed {key}"));
                        }
                        fs::remove_file(&p).map_err(|e| e.to_string())?;
                        File::open(p.parent().expect("target parent"))
                            .and_then(|f| f.sync_all())
                            .map_err(|e| e.to_string())?;
                    }
                    actual.remove(&p.to_string_lossy().into_owned());
                }
                if item.path.starts_with("manifests/") {
                    match fs::remove_dir(plan.store.join(&item.path)) {
                        Ok(()) => (),
                        Err(e) if e.kind() == std::io::ErrorKind::NotFound => (),
                        Err(e) => return Err(e.to_string()),
                    }
                    File::open(plan.store.join("manifests"))
                        .and_then(|f| f.sync_all())
                        .map_err(|e| e.to_string())?;
                }
            }
            append(&log, &digest, index, "done")?;
            active = false;
            index += 1;
        }
        if touches_retained(&plan, start, index) {
            verify_retained(&plan, drive, access)?;
        }
        writeln!(out, "retirement progress {index}/{count}").map_err(|e| e.to_string())?;
    }
    verify_retained(&plan, drive, access)?;
    let record = serde_json::json!({"schema_version":SCHEMA,"plan_sha256":digest,"removed_drive":plan.delete_drive,"removed_local":plan.delete_local,"retained_verified":true});
    let record_path = sealed.with_extension("retired.json");
    seal(&record_path, &crate::fetch::json_bytes(&record)?)?;
    writeln!(out, "retirement complete {}", record_path.display()).map_err(|e| e.to_string())
}

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
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub whole_job: bool,
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

use crate::lineage::{MigrationMapping, MigrationRecord};

fn verified_replacements(
    records: &[MigrationRecord],
    job: &str,
    root: &str,
    lineage: &Value,
    manifests: &BTreeMap<String, Manifest>,
) -> Result<BTreeSet<String>, String> {
    let mapping: MigrationMapping = serde_json::from_value(lineage.clone())
        .map_err(|_| format!("retire: unresolved migration mapping for {root}"))?;
    let instrument = &manifests[root].instrument;
    let valid_old = |id: &String, dataset: bool| {
        hex_id(id)
            // A fresh restore need not contain any historical v1 closure. Missing mapped
            // identities confer no deletion authority; only present manifests become candidates.
            && manifests.get(id).is_none_or(|m|
                m.ordinary && !m.daily && m.dataset == dataset && &m.instrument == instrument)
    };
    let streams = mapping.streams();
    if mapping.v1_generations.is_empty()
        || !mapping.v1_generations.iter().all(|id| valid_old(id, true))
        || !streams.iter().all(|id| valid_old(id, false))
        || streams.iter().any(|id| {
            manifests.get(id).is_some_and(|m| {
                !m.value["source_generation"]
                    .as_str()
                    .is_some_and(|source| mapping.v1_generations.contains(source))
            })
        })
        || !records.iter().any(|record| {
            record.verified()
                && record.job == job
                && record.mapping.v1_generations == mapping.v1_generations
                && record.mapping.v1_stream == mapping.v1_stream
                && record.mapping.streams() == streams
                && record.v2_root == root
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
    replaced.extend(streams);
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
            || path.file_name().and_then(|n| n.to_str())
                != Some(format!("plan-{digest}.retired.json").as_str())
            || record["retained_verified"] != true
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
        // Fresh-store plans can retire historical references whose byte copies were already
        // omitted by a verified migration restore. Keep that sealed decision for later jobs.
        retired.extend(
            plan.references
                .into_iter()
                .filter(|reference| reference.status == Status::Retired)
                .map(|reference| reference.closure),
        );
    }
    Ok(retired)
}

/// Completed whole-job plans are tombstones, preventing a timer from recreating retired
/// data while the operator finishes removing the document entry.
pub(crate) fn retired_jobs(state: &Path) -> Result<BTreeSet<String>, String> {
    retired_closures(state)?;
    let mut paths = Vec::new();
    files(&state.join("retirement"), &mut paths)?;
    let mut jobs = BTreeSet::new();
    for path in paths
        .iter()
        .filter(|p| p.to_string_lossy().ends_with(".retired.json"))
    {
        let completed = json(&fs::read(path).map_err(|e| e.to_string())?)?;
        let digest = string(&completed, "plan_sha256")?;
        let plan: Plan = serde_json::from_slice(
            &fs::read(state.join("retirement").join(format!("plan-{digest}.json")))
                .map_err(|e| e.to_string())?,
        )
        .map_err(|e| e.to_string())?;
        if plan.whole_job {
            jobs.extend(plan.jobs);
        }
    }
    Ok(jobs)
}

/// Called under the managed-store writer lock, before any producer can change sealed state.
/// Journal presence is the durable fence, even when no complete progress frame exists yet.
pub(crate) fn check_writer(state: &Path, resume: Option<&Path>) -> Result<(), String> {
    let mut paths = Vec::new();
    files(&state.join("retirement"), &mut paths)?;
    let mut resume_digest = None;
    let mut checked_completions = false;
    for path in paths {
        let Some(digest) = path
            .file_name()
            .and_then(|n| n.to_str())
            .and_then(|n| n.strip_prefix("plan-"))
            .and_then(|n| n.strip_suffix(".progress.jsonseq"))
        else {
            continue;
        };
        if !hex_id(digest) {
            return Err(format!(
                "retire: unresolved retirement journal {}",
                path.display()
            ));
        }
        let sealed = path.with_file_name(format!("plan-{digest}.json"));
        if sealed.with_extension("retired.json").exists() {
            if !checked_completions {
                retired_closures(state)?;
                checked_completions = true;
            }
            continue;
        }
        if let Some(resume) = resume {
            if resume_digest.is_none() {
                resume_digest = Some(hash(&fs::read(resume).map_err(|e| e.to_string())?));
            }
            if resume_digest.as_deref() == Some(digest) {
                continue;
            }
        }
        return Err(format!(
            "pipeline: unfinished retirement {}; resume this exact plan",
            sealed.display()
        ));
    }
    Ok(())
}

/// Default is plan-only. Apply accepts only an unchanged plan previously sealed in this store.
pub fn run(
    config_path: &Path,
    job: Option<&str>,
    apply: Option<&Path>,
    out: &mut dyn Write,
) -> Result<(), String> {
    run_scoped(config_path, job, apply, false, out)
}

pub fn run_scoped(
    config_path: &Path,
    job: Option<&str>,
    apply: Option<&Path>,
    whole_job: bool,
    out: &mut dyn Write,
) -> Result<(), String> {
    if whole_job && job.is_none() {
        return Err("retire: --whole-job requires one --job".into());
    }
    let (config, layout, _) = data_pipeline::load(config_path)?;
    let _lock = data_pipeline::retirement_writer_lock(&layout, apply)?;
    let _archive_lock = data_pipeline::archive_lock(&config)?;
    let declaration = data_pipeline::declaration(&config)?;
    let access = Access {
        declaration: declaration.as_ref(),
        certification: None,
    };
    let mut drive = Drive::open(&config.drive)?;
    if let Some(path) = apply {
        if whole_job {
            let plan: Plan = serde_json::from_slice(&fs::read(path).map_err(|e| e.to_string())?)
                .map_err(|e| e.to_string())?;
            if !plan.whole_job {
                return Err("retire: plan is not a whole-job retirement".into());
            }
        }
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
    let plan = plan(
        config_path,
        &config,
        &layout,
        &mut drive,
        access,
        job,
        whole_job,
    )?;
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

#[allow(clippy::too_many_arguments)]
fn plan(
    config_path: &Path,
    config: &PipelineConfig,
    layout: &Layout,
    drive: &mut Drive,
    access: Access<'_>,
    job_filter: Option<&str>,
    whole_job: bool,
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
            if let Ok(record) = serde_json::from_value::<MigrationRecord>(value) {
                migrations.push(record);
            }
        }
    }
    let mut selected = BTreeMap::new();
    for job in &config.jobs {
        if job_filter.is_some_and(|id| id != job.id) {
            continue;
        }
        let core = data_pipeline::add_job::load_core(&layout.base.join(&job.config), false)?;
        let history = core
            .history
            .as_ref()
            .ok_or("retire: job requires history binding")?;
        let [symbol] = history.instruments.as_slice() else {
            return Err("retire: job requires one instrument".into());
        };
        let instrument = format!("{}:{symbol}", history.broker);
        if whole_job {
            if history.role != DatasetRole::Development {
                return Err("retire: whole-job retirement requires development data".into());
            }
            for other in config.jobs.iter().filter(|other| other.id != job.id) {
                let core =
                    data_pipeline::add_job::load_core(&layout.base.join(&other.config), false)?;
                if core
                    .history
                    .as_ref()
                    .is_some_and(|h| h.broker == history.broker && h.instruments.contains(symbol))
                {
                    return Err("retire: instrument is also owned by another configured job".into());
                }
            }
            if layout.state.join(&job.id).join("progress.json").exists() {
                return Err(
                    "retire: complete the pending acquisition before whole-job retirement".into(),
                );
            }
            selected.insert(job.id.clone(), (instrument, String::new()));
            continue;
        }
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
        if candidates.is_empty() {
            return Err(format!(
                "retire: job {} has no archived daily-v2 catalog",
                job.id
            ));
        } else {
            let index = data_pipeline::newest_catalog(
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
    let mut completed_records = BTreeSet::new();
    let mut owned_records = BTreeSet::new();
    let mut migrated_objects = BTreeSet::new();
    let mut declared_history = BTreeSet::new();
    let mut job_owners: BTreeMap<_, _> = selected
        .keys()
        .map(|job| (job.clone(), job.clone()))
        .collect();
    for (job, (instrument, catalog_id)) in &selected {
        if whole_job {
            replaced.extend(
                manifests
                    .iter()
                    .filter(|(_, m)| m.ordinary && &m.instrument == instrument)
                    .map(|(id, _)| id.clone()),
            );
            // Current descendant catalogs retain the cumulative migration evidence even
            // after ordinary retirement removes the original root catalog and older roots.
            let mut authenticated_records = BTreeSet::new();
            for archive in archives
                .iter()
                .filter(|a| a.job == *job && a.value["instrument"] == *instrument)
            {
                let Some(source) = manifests
                    .get(&archive.dataset)
                    .filter(|m| m.ordinary && m.daily && m.dataset && &m.instrument == instrument)
                else {
                    continue;
                };
                let catalog = data_pipeline::Catalog::from_json(
                    &serde_json::to_vec(&archive.value).map_err(|e| e.to_string())?,
                )?;
                let dataset = GenerationManifest::from_json(
                    &serde_json::to_vec(&source.value).map_err(|e| e.to_string())?,
                )?;
                let Ok(bindings) = catalog.check_migration_records(layout, &dataset, access) else {
                    continue;
                };
                for (key, identity) in data_pipeline::evidence_records(layout, job, &bindings)? {
                    if catalog.records.iter().any(|r| {
                        r.key == key && r.bytes == identity.bytes && r.sha256 == identity.sha256
                    }) {
                        authenticated_records.insert(layout.state.join(key));
                    }
                }
            }
            for path in &authenticated_records {
                owned_records.insert(
                    path.file_name()
                        .ok_or("retire: record filename absent")?
                        .to_string_lossy()
                        .into_owned(),
                );
                // Verified migration alias tables name the legacy byte sources represented
                // losslessly in daily pages. Fresh restores intentionally omit those copies.
                if path.extension().is_some_and(|e| e == "jsonl") {
                    for alias in documents(path)? {
                        for field in ["source", "checkpoint"] {
                            if let Some(key) = alias[field]["key"].as_str() {
                                if !valid_key(key) || !key.starts_with("objects/") {
                                    return Err("retire: invalid migrated source key".into());
                                }
                                migrated_objects.insert(key.to_string());
                            }
                        }
                    }
                }
            }
            for record in migrations.iter().filter(|r| r.job == *job && r.verified()) {
                let mut archived = false;
                let mut authenticated = false;
                for path in &record_paths {
                    let bytes = fs::read(path).map_err(|e| e.to_string())?;
                    if serde_json::from_slice::<MigrationRecord>(&bytes).is_ok_and(|r| {
                        serde_json::to_value(r).ok() == serde_json::to_value(record).ok()
                    }) {
                        authenticated |= authenticated_records.contains(path);
                        let key = format!(
                            "records/{}",
                            path.file_name()
                                .ok_or("retire: invalid record path")?
                                .to_string_lossy()
                        );
                        archived |= archives.iter().any(|a| {
                            a.job == *job
                                && a.value["instrument"] == *instrument
                                && a.value["role"] == "development"
                                && a.entries
                                    .iter()
                                    .any(|e| e.key == manifest_key(&record.v2_root))
                                && a.entries
                                    .iter()
                                    .any(|e| e.key == manifest_key(&record.v2_stream))
                                && a.entries.iter().any(|e| {
                                    e.key == key
                                        && e.identity.bytes == bytes.len() as u64
                                        && e.identity.sha256 == hash(&bytes)
                                })
                        });
                    }
                }
                if archived || authenticated {
                    // Absent historical identities are catalog-bound, not unknown dependencies;
                    // this grants no authority over another live root.
                    declared_history.extend(record.mapping.v1_generations.iter().cloned());
                    declared_history.extend(record.mapping.streams());
                    declared_history.insert(record.v2_root.clone());
                    declared_history.insert(record.v2_stream.clone());
                }
                if record.predecessor_jobs.is_empty() && record.storage_aliases.is_empty() {
                    continue;
                }
                if !authenticated {
                    return Err("retire: migration ownership is not bound to verified archived evidence; restore the current catalog first".into());
                }
                for predecessor in &record.predecessor_jobs {
                    if config
                        .jobs
                        .iter()
                        .any(|j| &j.id == predecessor && j.id != *job)
                    {
                        return Err("retire: predecessor job is still configured".into());
                    }
                    job_owners.insert(predecessor.clone(), job.clone());
                }
                for key in &record.storage_aliases {
                    if !valid_key(key)
                        || !key.starts_with("objects/")
                        || (layout.store.join(key).exists()
                            && Identity::of(&layout.store.join(key))?.sha256
                                != key["objects/".len()..])
                    {
                        return Err("retire: invalid migrated storage alias identity".into());
                    }
                    migrated_objects.insert(key.clone());
                }
            }
            // A self-contained descendant explicitly names absent older daily datasets.
            // Its archived operation receipt supplies the corresponding historical stream.
            for m in manifests
                .values()
                .filter(|m| m.ordinary && m.daily && m.dataset && &m.instrument == instrument)
            {
                for object in m.value["objects"]
                    .as_array()
                    .ok_or("retire: missing daily objects")?
                {
                    if object["path"] != "provenance/lineage.json" {
                        continue;
                    }
                    let path = layout.store.join(string(object, "key")?);
                    let identity = Identity::of(&path)?;
                    if object["sha256"] != identity.sha256 || object["bytes"] != identity.bytes {
                        return Err("retire: lineage object identity mismatch".into());
                    }
                    let lineage = json(&fs::read(path).map_err(|e| e.to_string())?)?;
                    for ancestor in lineage["ancestors"].as_array().into_iter().flatten() {
                        let id = ancestor
                            .as_str()
                            .filter(|id| hex_id(id))
                            .ok_or("retire: invalid daily ancestor")?;
                        declared_history.insert(id.to_string());
                    }
                }
            }
            for path in &authenticated_records {
                for receipt in documents(path)? {
                    if receipt["command"] == "update"
                        && receipt["dataset_generation"]
                            .as_str()
                            .is_some_and(|id| declared_history.contains(id))
                        && let Some(stream) = receipt["stream_generation"]
                            .as_str()
                            .filter(|id| hex_id(id))
                    {
                        declared_history.insert(stream.to_string());
                    }
                }
            }
            continue;
        }
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
            &layout.state.join("records"),
            job,
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
                .chain(
                    migrations
                        .iter()
                        .filter(|r| r.job == *job && r.verified())
                        .map(|r| r.v2_root.as_str()),
                )
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
        if lineage[&seed].get("v1_generations").is_some()
            || legacy_refs
                .iter()
                .any(|id| manifests.get(id).is_some_and(|m| !m.daily))
        {
            let verified =
                verified_replacements(&migrations, job, &seed, &lineage[&seed], &manifests)?;
            let root_manifest = GenerationManifest::from_json(
                &fs::read(layout.store.join(manifest_key(&seed))).map_err(|e| e.to_string())?,
            )?;
            let catalog = data_pipeline::Catalog::from_json(
                &serde_json::to_vec(&archive.value).map_err(|e| e.to_string())?,
            )?;
            let bindings = catalog.check_migration_records(layout, &root_manifest, access)
                .map_err(|e| format!("retire: verified migration evidence is not archived with the retained v2 catalog: {e}"))?;
            // The job's cumulative evidence closure (superseded migration receipts, their alias
            // tables, predecessor jobs' records) is its own immutable history, archived with the
            // catalog. Owned records never pin the closures a verified superseding record replaced.
            for key in data_pipeline::evidence_records(layout, job, &bindings)?.keys() {
                owned_records.insert(crate::lineage::record_name(key)?.to_string());
            }
            let mapping: MigrationMapping =
                serde_json::from_value(lineage[&seed].clone()).map_err(|e| e.to_string())?;
            for key in bindings.files.keys() {
                completed_records.insert(crate::lineage::record_name(key)?.to_string());
                for value in documents(&layout.state.join(key))? {
                    if let Ok(record) = serde_json::from_value::<MigrationRecord>(value)
                        && record.verified()
                        && record.job == *job
                        && record.v2_root == seed
                        && record.mapping == mapping
                        && bindings.streams.contains_key(&record.v2_stream)
                    {
                        for predecessor in record.predecessor_jobs {
                            if predecessor.is_empty()
                                || !predecessor
                                    .bytes()
                                    .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
                            {
                                return Err("retire: invalid predecessor job identity".into());
                            }
                            if job_owners
                                .insert(predecessor, job.clone())
                                .is_some_and(|owner| owner != *job)
                            {
                                return Err("retire: conflicting predecessor job ownership".into());
                            }
                        }
                        for alias in record.storage_aliases {
                            if !alias.starts_with("objects/") || !valid_key(&alias) {
                                return Err("retire: invalid migrated storage alias key".into());
                            }
                            if layout.store.join(&alias).exists()
                                && Identity::of(&layout.store.join(&alias))?.sha256
                                    != alias["objects/".len()..]
                            {
                                return Err(
                                    "retire: migrated storage alias content mismatch".into()
                                );
                            }
                            migrated_objects.insert(alias);
                        }
                    }
                }
                if lineage[&seed]["alias_table"]["record"].as_str()
                    == Some(crate::lineage::record_name(key)?)
                {
                    for alias in documents(&layout.state.join(key))? {
                        for source in ["source", "checkpoint"] {
                            if let Some(key) = alias[source]["key"].as_str() {
                                if !valid_key(key) || !key.starts_with("objects/") {
                                    return Err(
                                        "retire: invalid migrated occurrence source key".into()
                                    );
                                }
                                migrated_objects.insert(key.to_string());
                            }
                        }
                    }
                }
            }
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
    known.extend(declared_history);
    let mut graph = BTreeMap::new();
    let mut roots = BTreeSet::new();
    let mut candidates = BTreeSet::new();
    let completed_transfers = if whole_job {
        crate::registry::completed_aliases(&layout.state, &config.drive)?
    } else {
        BTreeMap::new()
    };
    for (id, aliases) in &completed_transfers {
        if already_retired.contains(id) && !remote.contains_key(id) {
            continue;
        }
        for (owner, key) in aliases.iter().filter_map(|alias| alias.split_once('/')) {
            if key.starts_with("objects/") {
                known.insert(key.to_string());
                if job_owners.contains_key(owner) {
                    candidates.insert(key.to_string());
                } else {
                    roots.insert(key.to_string());
                    roots.insert(id.clone());
                }
            }
        }
    }
    known.extend(migrated_objects.iter().cloned());
    // Keys a verified migration record declares as storage aliases or migrated byte sources
    // are represented by v2 pages. They are retirement candidates by declaration and never
    // become retained roots through a record that merely mentions them; after retirement, or
    // in a fresh restore, their absence is the expected state rather than a lost dependency.
    let migrated_keys = migrated_objects.clone();
    candidates.extend(migrated_objects);
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
            if e.key.starts_with("records/") {
                roots.insert(e.file_id.clone());
                roots.insert(e.key.clone());
            }
        }
        // A content-key reference pins local content, never another catalog's remote copies.
        graph
            .entry(a.file.key.clone())
            .or_insert_with(BTreeSet::new)
            .extend(closure.clone());
        closure.remove(&a.file.key);
        closure.extend(a.entries.iter().map(|e| e.file_id.clone()));
        graph.insert(a.file.file_id.clone(), closure);
        let whole_owned = whole_job
            && job_owners
                .get(&a.job)
                .and_then(|owner| selected.get(owner))
                .is_some_and(|(instrument, _)| a.value["instrument"] == *instrument)
            && a.value["role"] == "development";
        if whole_owned
            || (job_owners
                .get(&a.job)
                .and_then(|owner| selected.get(owner))
                .is_some_and(|(instrument, id)| {
                    id != &a.file.file_id && a.value["instrument"] == *instrument
                })
                && a.value["role"] == "development"
                && (replaced.contains(&a.dataset)
                    || manifests.get(&a.dataset).is_some_and(|m| m.daily)))
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
    // A fresh restore carries immutable records for retained jobs but deliberately omits
    // their legacy byte copies. Only a verified, archived migration can classify those
    // absent record references as history. Live configuration and pending pins stay strict.
    let mut historical_record_objects: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    if whole_job {
        for archive in &archives {
            let Some(source) = manifests.get(&archive.dataset).filter(|m| {
                m.ordinary && m.daily && m.dataset && archive.value["instrument"] == m.instrument
            }) else {
                continue;
            };
            let catalog = data_pipeline::Catalog::from_json(
                &serde_json::to_vec(&archive.value).map_err(|e| e.to_string())?,
            )?;
            let dataset = GenerationManifest::from_json(
                &serde_json::to_vec(&source.value).map_err(|e| e.to_string())?,
            )?;
            let Ok(bindings) = catalog.check_migration_records(layout, &dataset, access) else {
                continue;
            };
            let mut historical = BTreeSet::new();
            for key in bindings.files.keys() {
                for value in documents(&layout.state.join(key))? {
                    for field in ["source", "checkpoint"] {
                        if let Some(key) = value[field]["key"].as_str() {
                            if !valid_key(key) || !key.starts_with("objects/") {
                                return Err("retire: invalid historical source key".into());
                            }
                            historical.insert(key.to_string());
                        }
                    }
                    if let Ok(record) = serde_json::from_value::<MigrationRecord>(value)
                        && record.verified()
                    {
                        if record
                            .storage_aliases
                            .iter()
                            .any(|key| !valid_key(key) || !key.starts_with("objects/"))
                        {
                            return Err("retire: invalid historical storage alias".into());
                        }
                        historical.extend(record.storage_aliases);
                    }
                }
            }
            historical.retain(|key| !layout.store.join(key).exists());
            for (key, identity) in data_pipeline::evidence_records(layout, &archive.job, &bindings)?
            {
                if catalog.records.iter().any(|r| {
                    r.key == key && r.bytes == identity.bytes && r.sha256 == identity.sha256
                }) {
                    historical_record_objects
                        .entry(crate::lineage::record_name(&key)?.to_string())
                        .or_default()
                        .extend(historical.iter().cloned());
                }
            }
        }
    }
    let mut record_history_decisions: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    for (path, id) in &records {
        let values = documents(path)?;
        let mut refs = BTreeSet::new();
        for value in &values {
            references(value, &known, &mut refs);
        }
        refs.retain(|key| {
            let historical = historical_record_objects
                .get(id)
                .is_some_and(|history| history.contains(key))
                || (key.starts_with("objects/")
                    && already_retired.contains(key)
                    && !layout.store.join(key).exists());
            if historical {
                record_history_decisions
                    .entry(id.clone())
                    .or_default()
                    .insert(key.clone());
            }
            !historical
        });
        graph.insert(id.clone(), refs);
    }
    if whole_job {
        // Acquisition records reference their job's intent; catalog receipts bind its exact
        // catalog. Resolve ownership through immutable records, not filename prefixes.
        for (path, id) in &records {
            if documents(path)?.iter().any(|v| {
                v["job"]
                    .as_str()
                    .is_some_and(|j| job_owners.contains_key(j))
            }) {
                owned_records.insert(id.clone());
            }
        }
        loop {
            let previous = owned_records.len();
            for (path, id) in &records {
                let values = documents(path)?;
                if values.iter().any(|v| {
                    v.get("job")
                        .is_some_and(|j| !j.as_str().is_some_and(|j| job_owners.contains_key(j)))
                }) {
                    continue;
                }
                if values.iter().any(|v| {
                    ["intent", "acquisition_id"]
                        .iter()
                        .any(|field| v[field].as_str().is_some_and(|r| owned_records.contains(r)))
                        || v["file_id"].as_str().is_some_and(|id| {
                            archives
                                .iter()
                                .any(|a| job_owners.contains_key(&a.job) && a.file.file_id == id)
                        })
                }) {
                    owned_records.insert(id.clone());
                }
            }
            if previous == owned_records.len() {
                break;
            }
        }
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
        if whole_job
            && path.file_name().is_some_and(|n| n == "migration.json")
            && path
                .parent()
                .and_then(Path::file_name)
                .and_then(|n| n.to_str())
                .is_some_and(|j| job_owners.contains_key(j))
        {
            let value = json(&fs::read(&path).map_err(|e| e.to_string())?)?;
            if value["phase"] != "verified" {
                return Err("retire: finish pending migration before whole-job retirement".into());
            }
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
            // A completed checkpoint is a cache of its archived immutable migration receipt.
            // Converted, unknown, or mismatched checkpoints continue to pin their source data.
            if path
                .file_name()
                .is_some_and(|name| name == "migration.json")
                && value["phase"] == "verified"
                && let Some(name) = value["record"]
                    .as_str()
                    .filter(|name| completed_records.contains(*name))
            {
                let record: MigrationRecord = serde_json::from_slice(
                    &fs::read(layout.state.join("records").join(name))
                        .map_err(|e| e.to_string())?,
                )
                .map_err(|e| e.to_string())?;
                if record.verified()
                    && value["dataset"] == record.v2_root
                    && value["stream"] == record.v2_stream
                    && record
                        .evidence
                        .get("binding")
                        .is_some_and(|binding| value.get("binding") == Some(binding))
                {
                    continue;
                }
            }
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
        let mut selected_record = completed_records.contains(id) || owned_records.contains(id);
        let mut foreign_job = false;
        for value in documents(path)? {
            foreign_job |= value
                .get("job")
                .is_some_and(|j| !j.as_str().is_some_and(|j| job_owners.contains_key(j)));
            selected_record |= value
                .get("job")
                .and_then(Value::as_str)
                .is_some_and(|j| job_owners.contains_key(j));
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
            .any(|a| job_owners.contains_key(&a.job) && closure.contains(&a.file.file_id));
        if whole_job && foreign_job {
            selected_record = false;
        }
        if whole_job && selected_record {
            // Completed standalone responses are job data too. Other records and live pins
            // still retain shared objects when reachability is expanded below.
            for key in closure.iter().filter(|key| key.starts_with("objects/")) {
                candidates.insert(key.clone());
                eligible.insert(key.clone());
            }
        }
        let resolved = |dependency: &String| {
            graph.contains_key(dependency)
                || known.contains(dependency)
                || remote.contains_key(dependency)
                || (valid_key(dependency) && layout.store.join(dependency).is_file())
        };
        let unresolved = closure
            .iter()
            .any(|dependency| !resolved(dependency) && !already_retired.contains(dependency));
        if (unresolved && !completed_records.contains(id)) || !selected_record {
            // Unknown evidence cannot authorize partial retirement of a record's closure.
            roots.extend(
                closure
                    .iter()
                    .filter(|d| {
                        resolved(d)
                            && !migrated_keys.contains(*d)
                            && !records.iter().any(|(_, id)| id == *d)
                    })
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
    if whole_job {
        // A configuration/unknown record pin must not leave owned standalone data behind.
        // Shared bytes are allowed only when another retained manifest/catalog needs them.
        for key in candidates
            .iter()
            .filter(|k| k.starts_with("objects/") && roots.contains(*k))
        {
            let shared = manifests
                .iter()
                .any(|(id, m)| roots.contains(id) && m.objects.contains(key))
                || archives.iter().any(|a| {
                    roots.contains(&a.file.file_id) && a.entries.iter().any(|e| &e.key == key)
                });
            if !shared {
                return Err(format!("retire: whole-job data remains pinned: {key}"));
            }
        }
    }
    if whole_job
        && (manifests.iter().any(|(id, m)| {
            roots.contains(id)
                && selected
                    .values()
                    .any(|(instrument, _)| m.ordinary && &m.instrument == instrument)
        }) || archives
            .iter()
            .any(|a| roots.contains(&a.file.file_id) && job_owners.contains_key(&a.job)))
    {
        return Err("retire: whole job remains referenced by a configuration, pending transfer, or another retained closure".into());
    }
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
            if roots.contains(&entry.file_id) {
                retained_drive.insert(entry.file_id.clone(), entry.clone());
            } else if candidates.contains(&entry.file_id) {
                delete_drive.insert(entry.file_id.clone(), entry.clone());
            }
        }
    }
    for id in retained_drive.keys() {
        delete_drive.remove(id);
    }
    if whole_job {
        // Completed uploads can precede catalog publication. Their durable registry aliases
        // are part of job ownership; inventory them rather than leaving remote orphan data.
        for (id, aliases) in completed_transfers {
            let owned: Vec<_> = aliases
                .iter()
                .filter_map(|a| a.split_once('/'))
                .filter(|(job, _)| job_owners.contains_key(*job))
                .collect();
            if owned.is_empty()
                || retained_drive.contains_key(&id)
                || delete_drive.contains_key(&id)
            {
                continue;
            }
            if owned.iter().any(|(_, key)| key.starts_with("records/")) {
                continue;
            }
            let Some(metadata) = remote.get(&id) else {
                if already_retired.contains(&id) {
                    continue;
                }
                return Err(
                    "retire: completed transfer is missing without retirement proof".into(),
                );
            };
            if aliases.len() != owned.len() {
                return Err(
                    "retire: completed unarchived transfer has shared or unresolved job ownership"
                        .into(),
                );
            }
            if roots.contains(&id) {
                return Err("retire: whole-job remote data remains pinned".into());
            }
            let key = owned[0].1;
            let checked_name = name(key)?;
            if string(metadata, "name")? != checked_name {
                return Err("retire: completed transfer name mismatch".into());
            }
            let identity = drive.listed_identity(&crate::drive::RemoteFile {
                id: id.clone(),
                name: checked_name.clone(),
                size: metadata["bytes"].as_u64(),
                sha256: metadata["sha256"].as_str().map(str::to_string),
                trashed: metadata["trashed"].as_bool().unwrap_or(false),
            })?;
            if key.starts_with("objects/") && key != format!("objects/{}", identity.sha256) {
                return Err("retire: completed transfer content mismatch".into());
            }
            delete_drive.insert(
                id.clone(),
                Remote {
                    file_id: id,
                    key: key.into(),
                    name: checked_name,
                    identity: Identity {
                        bytes: identity.bytes,
                        sha256: identity.sha256,
                    },
                },
            );
        }
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
        let historical = record_history_decisions.remove(&id).unwrap_or_default();
        let any_retired = !historical.is_empty()
            || closure.iter().any(|r| {
                !roots.contains(r) && (candidates.contains(r) || already_retired.contains(r))
            });
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
        for dependency in historical {
            refs.push(Reference {
                source: path.display().to_string(),
                closure: dependency,
                status: Status::Retired,
                reason:
                    "absent historical bytes bound to verified migration or completed retirement"
                        .into(),
            });
        }
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
        whole_job,
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
    // Install the fence before verification/first mutation; an empty or torn journal still
    // reserves this store for the identical plan until its verified completion is sealed.
    OpenOptions::new()
        .create(true)
        .append(true)
        .open(&log)
        .and_then(|f| f.sync_all())
        .map_err(|e| e.to_string())?;
    File::open(log.parent().expect("progress parent"))
        .and_then(|f| f.sync_all())
        .map_err(|e| e.to_string())?;
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

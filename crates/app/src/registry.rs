//! Archive-root transfer ownership. A synced JSON-lines journal records individual changes;
//! a versioned snapshot compacts every 256 changes. Content keys include manifests/catalogs,
//! so names and job-local aliases never establish content identity.
use crate::drive::{Drive, DriveSettings};
use crate::store::{self, ObjectIdentity};
use binary_alpha_engine::dataset::object_key;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, RwLock};

const COMPACT_EVERY: u64 = 256;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Entry {
    pub file_id: String,
    pub session: Option<String>,
    pub done: bool,
    pub bytes: u64,
    pub sha256: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct LegacyEntry {
    file_id: String,
    #[serde(default)]
    session: Option<String>,
    done: bool,
}

#[derive(Default, Serialize, Deserialize)]
struct LegacyTransfers {
    files: BTreeMap<String, LegacyEntry>,
}

#[derive(Default, Serialize, Deserialize)]
struct State {
    version: u32,
    archive_root: String,
    endpoint: Option<String>,
    sequence: u64,
    rebuilt: bool,
    files: BTreeMap<String, Entry>,
    legacy: BTreeMap<String, LegacyEntry>,
    imported: BTreeSet<String>,
}

#[derive(Serialize, Deserialize)]
struct Record {
    sequence: u64,
    change: Change,
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum Change {
    Put { key: String, entry: Entry },
    Legacy { alias: String, entry: LegacyEntry },
    Imported { job: String },
    Rebuilt,
    Remove { key: String },
}

impl State {
    fn apply(&mut self, change: Change) {
        match change {
            Change::Put { key, entry } => {
                self.files.insert(key, entry);
            }
            Change::Legacy { alias, entry } => {
                self.legacy.insert(alias, entry);
            }
            Change::Imported { job } => {
                self.imported.insert(job);
            }
            Change::Rebuilt => self.rebuilt = true,
            Change::Remove { key } => {
                self.files.remove(&key);
            }
        }
    }
}

/// One handle is shared by every job in a producer invocation. The process writer lock
/// excludes other producers; per-key mutexes exclude overlapping uploads inside this process.
pub struct Registry {
    directory: PathBuf,
    state: Mutex<State>,
    leases: Mutex<BTreeMap<String, Arc<Mutex<()>>>>,
    operations: RwLock<()>,
    failed: AtomicBool,
}

impl Registry {
    pub fn open(
        state_dir: &Path,
        settings: &DriveSettings,
        drive: &mut Drive,
    ) -> Result<Self, String> {
        let directory = state_dir.join("registry");
        fs::create_dir_all(&directory).map_err(|e| e.to_string())?;
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&directory, fs::Permissions::from_mode(0o700))
            .map_err(|e| e.to_string())?;
        File::open(state_dir)
            .and_then(|f| f.sync_all())
            .map_err(|e| e.to_string())?;
        let snapshot = directory.join("snapshot.json");
        let mut state: State = match fs::read(&snapshot) {
            Ok(bytes) => {
                serde_json::from_slice(&bytes).map_err(|e| format!("registry snapshot: {e}"))?
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => State {
                version: 1,
                archive_root: settings.root_folder_id.clone(),
                endpoint: settings.loopback_endpoint.clone(),
                ..State::default()
            },
            Err(e) => return Err(e.to_string()),
        };
        if state.version != 1
            || state.archive_root != settings.root_folder_id
            || state.endpoint != settings.loopback_endpoint
        {
            return Err(
                "registry: archive root, endpoint, or version conflicts with snapshot".into(),
            );
        }
        let log = directory.join("events.ndjson");
        if let Ok(file) = File::open(&log) {
            let mut reader = BufReader::new(file);
            let mut valid_bytes = 0;
            loop {
                let mut line = Vec::new();
                let length = reader
                    .read_until(b'\n', &mut line)
                    .map_err(|e| e.to_string())?;
                if length == 0 {
                    break;
                }
                if line.last() != Some(&b'\n') {
                    // The only discardable record is an uncommitted trailing partial line.
                    OpenOptions::new()
                        .write(true)
                        .open(&log)
                        .and_then(|f| {
                            f.set_len(valid_bytes)?;
                            f.sync_all()
                        })
                        .map_err(|e| e.to_string())?;
                    break;
                }
                valid_bytes += length as u64;
                let record: Record =
                    serde_json::from_slice(&line).map_err(|e| format!("registry journal: {e}"))?;
                if record.sequence <= state.sequence {
                    continue;
                }
                if record.sequence != state.sequence + 1 {
                    return Err("registry: journal sequence gap".into());
                }
                state.sequence = record.sequence;
                state.apply(record.change);
            }
        } else if log.exists() {
            return Err("registry: cannot read journal".into());
        }
        let registry = Self {
            directory,
            state: Mutex::new(state),
            leases: Mutex::new(BTreeMap::new()),
            operations: RwLock::new(()),
            failed: AtomicBool::new(false),
        };
        if !snapshot.exists() {
            registry.snapshot(
                &*registry
                    .state
                    .lock()
                    .map_err(|_| "registry lock poisoned")?,
            )?;
        }
        registry.import_legacy(state_dir, drive)?;
        if !registry
            .state
            .lock()
            .map_err(|_| "registry lock poisoned")?
            .rebuilt
        {
            registry.rebuild(drive)?;
        }
        Ok(registry)
    }

    fn snapshot(&self, state: &State) -> Result<(), String> {
        let temporary = self.directory.join("snapshot.tmp");
        let mut file = File::create(&temporary).map_err(|e| e.to_string())?;
        serde_json::to_writer(&mut file, state).map_err(|e| e.to_string())?;
        file.sync_all().map_err(|e| e.to_string())?;
        fs::rename(temporary, self.directory.join("snapshot.json")).map_err(|e| e.to_string())?;
        File::open(&self.directory)
            .and_then(|f| f.sync_all())
            .map_err(|e| e.to_string())?;
        // Snapshot is durable before truncating. Replay skips records below its watermark.
        File::create(self.directory.join("events.ndjson"))
            .and_then(|f| f.sync_all())
            .and_then(|()| File::open(&self.directory)?.sync_all())
            .map_err(|e| e.to_string())
    }

    fn record(&self, state: &mut State, change: Change) -> Result<(), String> {
        self.healthy()?;
        // An uncertain append or snapshot cannot be followed by another sequence or upload.
        // Reopening replays durable records and repairs a torn trailing append.
        let result = self.append(state, change);
        if result.is_err() {
            self.failed.store(true, Ordering::SeqCst);
        }
        result
    }

    fn healthy(&self) -> Result<(), String> {
        if self.failed.load(Ordering::SeqCst) {
            return Err("registry: persistence failed; reopen before continuing transfers".into());
        }
        Ok(())
    }

    fn append(&self, state: &mut State, change: Change) -> Result<(), String> {
        let sequence = state
            .sequence
            .checked_add(1)
            .ok_or("registry sequence overflow")?;
        let record = Record { sequence, change };
        let mut bytes = serde_json::to_vec(&record).map_err(|e| e.to_string())?;
        bytes.push(b'\n');
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.directory.join("events.ndjson"))
            .map_err(|e| e.to_string())?;
        file.write_all(&bytes)
            .and_then(|()| file.sync_all())
            .map_err(|e| e.to_string())?;
        state.sequence = sequence;
        state.apply(record.change);
        if sequence.is_multiple_of(COMPACT_EVERY) {
            self.snapshot(state)?;
        }
        Ok(())
    }

    fn lease(&self, key: &str) -> Result<Arc<Mutex<()>>, String> {
        Ok(self
            .leases
            .lock()
            .map_err(|_| "registry lease lock poisoned")?
            .entry(key.into())
            .or_default()
            .clone())
    }

    fn import_legacy(&self, directory: &Path, drive: &mut Drive) -> Result<(), String> {
        for dir in fs::read_dir(directory).map_err(|e| e.to_string())? {
            let dir = dir.map_err(|e| e.to_string())?;
            let path = dir.path().join("transfers.json");
            if !path.is_file() {
                continue;
            }
            let job = dir.file_name().to_string_lossy().into_owned();
            if self
                .state
                .lock()
                .map_err(|_| "registry lock poisoned")?
                .imported
                .contains(&job)
            {
                continue;
            }
            let transfers: LegacyTransfers =
                serde_json::from_slice(&fs::read(path).map_err(|e| e.to_string())?)
                    .map_err(|e| format!("legacy transfers: {e}"))?;
            for (key, entry) in transfers.files {
                // Keep every old binding: a catalog session may already contain these exact ids.
                {
                    let mut state = self.state.lock().map_err(|_| "registry lock poisoned")?;
                    self.record(
                        &mut state,
                        Change::Legacy {
                            alias: format!("{job}/{key}"),
                            entry: entry.clone(),
                        },
                    )?;
                }
                if entry.done
                    && let Some(remote) = drive.metadata(&entry.file_id)?.filter(|r| !r.trashed)
                {
                    let identity = drive.listed_identity(&remote)?;
                    // Object keys are authoritative hashes; a mismatched old entry is not reusable.
                    if !key.starts_with("objects/") || key == object_key(&identity.sha256) {
                        self.merge_completed(&identity, entry.file_id)?;
                    }
                }
            }
            let mut state = self.state.lock().map_err(|_| "registry lock poisoned")?;
            self.record(&mut state, Change::Imported { job })?;
        }
        Ok(())
    }

    fn merge_completed(&self, identity: &ObjectIdentity, file_id: String) -> Result<(), String> {
        let key = object_key(&identity.sha256);
        let lease = self.lease(&key)?;
        let _lease = lease.lock().map_err(|_| "registry lease poisoned")?;
        let mut state = self.state.lock().map_err(|_| "registry lock poisoned")?;
        // Never displace a reservation, session, or existing catalog's selected identity.
        if !state.files.contains_key(&key) {
            self.record(
                &mut state,
                Change::Put {
                    key,
                    entry: Entry {
                        file_id,
                        session: None,
                        done: true,
                        bytes: identity.bytes,
                        sha256: identity.sha256.clone(),
                    },
                },
            )?;
        }
        Ok(())
    }

    /// A started legacy catalog already binds its job's original object and manifest ids.
    /// Returning those aliases lets the archive owner reconstruct exactly the session bytes.
    pub fn legacy_catalog_bindings(
        &self,
        drive: &mut Drive,
        job: &str,
        dataset: &str,
        stream: &str,
    ) -> Result<Option<BTreeMap<String, String>>, String> {
        let alias = format!("{job}/catalog/{dataset}/{stream}");
        let state = self.state.lock().map_err(|_| "registry lock poisoned")?;
        let Some(entry) = state.legacy.get(&alias) else {
            return Ok(None);
        };
        if !entry.done && entry.session.is_none() && drive.metadata(&entry.file_id)?.is_none() {
            return Ok(None);
        }
        let prefix = format!("{job}/");
        Ok(Some(
            state
                .legacy
                .iter()
                .filter_map(|(alias, entry)| {
                    alias
                        .strip_prefix(&prefix)
                        .map(|key| (key.to_string(), entry.file_id.clone()))
                })
                .collect(),
        ))
    }

    /// Rebuild only from a complete listing. Names narrow candidates, never prove identity.
    /// In-flight/reserved entries retain their original ids and session capabilities.
    pub fn rebuild(&self, drive: &mut Drive) -> Result<(), String> {
        let _operation = self
            .operations
            .write()
            .map_err(|_| "registry operation lock poisoned")?;
        self.healthy()?;
        let files = drive.list("")?;
        let mut confirmed: BTreeMap<String, Vec<Entry>> = BTreeMap::new();
        for remote in files {
            if !(remote.name.starts_with("object-")
                || remote.name.starts_with("manifest-")
                || remote.name.starts_with("catalog-"))
            {
                continue;
            }
            let identity = drive.listed_identity(&remote)?;
            if let Some(expected) = remote.name.strip_prefix("object-")
                && expected != identity.sha256
            {
                continue;
            }
            confirmed
                .entry(object_key(&identity.sha256))
                .or_default()
                .push(Entry {
                    file_id: remote.id,
                    session: None,
                    done: true,
                    bytes: identity.bytes,
                    sha256: identity.sha256,
                });
        }
        let keys: BTreeSet<_> = self
            .state
            .lock()
            .map_err(|_| "registry lock poisoned")?
            .files
            .keys()
            .chain(confirmed.keys())
            .cloned()
            .collect();
        for key in keys {
            let lease = self.lease(&key)?;
            let _lease = lease.lock().map_err(|_| "registry lease poisoned")?;
            let mut state = self.state.lock().map_err(|_| "registry lock poisoned")?;
            if state.files.get(&key).is_some_and(|entry| !entry.done) {
                continue;
            }
            if let Some(candidates) = confirmed.get(&key) {
                let entry = state
                    .files
                    .get(&key)
                    .and_then(|prior| {
                        candidates
                            .iter()
                            .find(|entry| entry.file_id == prior.file_id)
                    })
                    .unwrap_or(&candidates[0])
                    .clone();
                self.record(&mut state, Change::Put { key, entry })?;
            } else {
                self.record(&mut state, Change::Remove { key })?;
            }
        }
        let mut state = self.state.lock().map_err(|_| "registry lock poisoned")?;
        self.record(&mut state, Change::Rebuilt)
    }

    /// Reserves an id durably before upload. The caller holds the content lease until done.
    fn reserve(
        &self,
        drive: &mut Drive,
        identity: &ObjectIdentity,
        alias: &str,
    ) -> Result<Entry, String> {
        self.healthy()?;
        let key = object_key(&identity.sha256);
        let mut state = self.state.lock().map_err(|_| "registry lock poisoned")?;
        if let Some(entry) = state.files.get(&key) {
            if entry.bytes != identity.bytes || entry.sha256 != identity.sha256 {
                return Err("registry: content identity conflict".into());
            }
            return Ok(entry.clone());
        }
        let legacy = state.legacy.get(alias).or_else(|| {
            let (_, key) = alias.split_once('/')?;
            if !(key.starts_with("objects/") || key.starts_with("manifests/")) {
                return None;
            }
            state.legacy.iter().find_map(|(other, entry)| {
                (other.split_once('/').map(|(_, suffix)| suffix) == Some(key)).then_some(entry)
            })
        });
        let (file_id, session) = if let Some(entry) = legacy {
            (entry.file_id.clone(), entry.session.clone())
        } else {
            (drive.generate_ids(1)?.remove(0), None)
        };
        let entry = Entry {
            file_id,
            session,
            done: false,
            bytes: identity.bytes,
            sha256: identity.sha256.clone(),
        };
        self.record(
            &mut state,
            Change::Put {
                key,
                entry: entry.clone(),
            },
        )?;
        Ok(entry)
    }

    /// Different keys transfer concurrently; identical bytes across jobs have one uploader.
    pub fn transfer(
        &self,
        drive: &mut Drive,
        alias: &str,
        name: &str,
        path: &Path,
        identity: &ObjectIdentity,
    ) -> Result<String, String> {
        let _operation = self
            .operations
            .read()
            .map_err(|_| "registry operation lock poisoned")?;
        self.healthy()?;
        let key = object_key(&identity.sha256);
        let lease = self.lease(&key)?;
        let _lease = lease.lock().map_err(|_| "registry lease poisoned")?;
        let mut entry = self.reserve(drive, identity, alias)?;
        if entry.done {
            drive.verify(&entry.file_id, identity)?;
            return Ok(entry.file_id);
        }
        let file_id = entry.file_id.clone();
        let session = entry.session.clone();
        drive.upload(&file_id, name, path, identity, session, &mut |session| {
            entry.session = session.map(str::to_string);
            let mut state = self.state.lock().map_err(|_| "registry lock poisoned")?;
            self.record(
                &mut state,
                Change::Put {
                    key: key.clone(),
                    entry: entry.clone(),
                },
            )
        })?;
        entry.done = true;
        entry.session = None;
        let mut state = self.state.lock().map_err(|_| "registry lock poisoned")?;
        self.record(&mut state, Change::Put { key, entry })?;
        Ok(file_id)
    }
}

/// Select by manifest evidence, independent of migration/update implementation. A restored
/// descendant is sufficient: no removed v1 generation or local parent closure is required.
pub fn newest_daily(
    local: &store::Store,
    instrument: &str,
    role: binary_alpha_engine::dataset::DatasetRole,
    access: binary_alpha_engine::research::Access<'_>,
) -> Result<String, String> {
    use binary_alpha_engine::dataset::{GenerationManifest, Layout, manifest_key};
    let mut candidates = Vec::new();
    for generation in local.list_manifests()? {
        access.lookup(&generation)?;
        let mut bytes = Vec::new();
        local.read_to(&manifest_key(&generation), None, &mut bytes)?;
        if crate::verify::manifest_kind(&bytes)?.is_some() {
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
        let end = binary_alpha_engine::market::parse_event_time_micros(
            &manifest.coverage.last_event_time,
        )?;
        candidates.push((end, manifest));
    }
    let end = candidates
        .iter()
        .map(|(end, _)| *end)
        .max()
        .ok_or_else(|| format!("pipeline: no daily-v2 dataset for {instrument}"))?;
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

/// Resolve equal-coverage daily catalogs through their pinned lineage metadata. Catalog
/// discovery itself still reads catalogs only; acquisition and migration remain separate owners.
pub fn newest_catalog(
    catalogs: &[(String, String, crate::data_pipeline::Catalog)],
    drive: &mut Drive,
    scratch: &Path,
    access: binary_alpha_engine::research::Access<'_>,
) -> Result<usize, String> {
    use binary_alpha_engine::{dataset::GenerationManifest, market::parse_event_time_micros};
    let mut candidates = Vec::new();
    for (index, (_, _, catalog)) in catalogs.iter().enumerate() {
        candidates.push((
            (
                parse_event_time_micros(&catalog.coverage.last_event_time)?,
                catalog.layout.is_some(),
            ),
            index,
        ));
    }
    let newest = candidates
        .iter()
        .map(|(key, _)| *key)
        .max()
        .ok_or("drive: no archived catalog")?;
    candidates.retain(|(key, _)| *key == newest);
    let generations: BTreeSet<_> = candidates
        .iter()
        .map(|(_, i)| catalogs[*i].2.dataset.generation.clone())
        .collect();
    let mut ancestors = BTreeSet::new();
    if newest.1 && generations.len() > 1 {
        for (_, index) in &candidates {
            let catalog = &catalogs[*index].2;
            access.permit(Some(catalog.role), &catalog.dataset.generation)?;
            let read = |drive: &mut Drive,
                        id: &str,
                        bytes: u64,
                        sha256: &str|
             -> Result<Vec<u8>, String> {
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
                let bytes = fs::read(&path).map_err(|e| e.to_string())?;
                fs::remove_file(path).map_err(|e| e.to_string())?;
                Ok(bytes)
            };
            let entry = &catalog.dataset;
            let manifest = GenerationManifest::from_json(&read(
                drive,
                &entry.file_id,
                entry.bytes,
                &entry.sha256,
            )?)?;
            if manifest.generation != entry.generation
                || manifest.layout != catalog.layout
                || manifest.role != catalog.role
            {
                return Err("archive: catalog disagrees with lineage manifest".into());
            }
            if let Some(object) = manifest
                .objects
                .iter()
                .find(|o| o.path == "provenance/lineage.json")
            {
                let entry = catalog
                    .objects
                    .iter()
                    .find(|e| {
                        e.key == object.key && e.bytes == object.bytes && e.sha256 == object.sha256
                    })
                    .ok_or("archive: lineage lies outside pinned catalog closure")?;
                let bytes = read(drive, &entry.file_id, entry.bytes, &entry.sha256)?;
                ancestors.extend(lineage_references(
                    &bytes,
                    &generations,
                    &manifest.generation,
                )?);
            }
        }
    }
    let terminal = newest
        .1
        .then(|| terminal_generation(&generations, &ancestors))
        .transpose()?;
    candidates
        .iter()
        .map(|(_, i)| *i)
        .filter(|i| {
            terminal
                .as_ref()
                .is_none_or(|generation| *generation == catalogs[*i].2.dataset.generation)
        })
        .max_by_key(|i| &catalogs[*i].2.dataset.generation)
        .ok_or_else(|| "archive: cyclic daily catalog lineage".into())
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

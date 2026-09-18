//! Archive-root transfer ownership. A synced JSON-lines journal records individual changes;
//! a versioned snapshot compacts every 256 changes. Content keys include manifests/catalogs,
//! so names and job-local aliases never establish content identity.
use crate::drive::{Drive, DriveSettings, RemoteFile};
use crate::store::ObjectIdentity;
use binary_alpha_engine::dataset::object_key;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, RwLock};

const COMPACT_EVERY: u64 = 256;
/// Ids generated per `generateIds` request when the pool runs dry.
const ID_POOL: usize = 100;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Entry {
    pub file_id: String,
    pub session: Option<String>,
    pub done: bool,
    pub bytes: u64,
    pub sha256: String,
    #[serde(default)]
    pub aliases: BTreeSet<String>,
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
    /// The archive root as listed when this registry opened: file id to the size and SHA-256
    /// Drive reports for every untrashed file that reports one. A completed entry whose listed
    /// identity equals the local identity is confirmed without another request; anything
    /// else keeps the per-file confirmation path.
    listed: BTreeMap<String, ObjectIdentity>,
    /// Generated Drive ids not yet bound: one `generateIds` request serves many reservations.
    ids: Mutex<Vec<String>>,
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
        let state = load_state(&directory, settings, true)?;
        let snapshot = directory.join("snapshot.json");
        let mut registry = Self {
            directory,
            state: Mutex::new(state),
            leases: Mutex::new(BTreeMap::new()),
            operations: RwLock::new(()),
            failed: AtomicBool::new(false),
            listed: BTreeMap::new(),
            ids: Mutex::new(Vec::new()),
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
        // One complete listing per run confirms completed bindings and feeds the rebuild.
        let files = drive.list("")?;
        registry.listed = files
            .iter()
            .filter(|file| !file.trashed)
            .filter_map(|file| {
                Some((
                    file.id.clone(),
                    ObjectIdentity {
                        bytes: file.size?,
                        sha256: file.sha256.as_ref()?.to_ascii_lowercase(),
                        crc32c: 0,
                    },
                ))
            })
            .collect();
        if !registry
            .state
            .lock()
            .map_err(|_| "registry lock poisoned")?
            .rebuilt
        {
            registry.rebuild_from(files, drive)?;
        }
        Ok(registry)
    }

    /// A remote file the index claims complete still carries exactly the local identity: by
    /// the opening listing when it agrees, otherwise by Drive's per-file confirmation.
    pub fn confirm(
        &self,
        drive: &mut Drive,
        file_id: &str,
        identity: &ObjectIdentity,
    ) -> Result<(), String> {
        if self.listed_matches(file_id, identity) {
            return Ok(());
        }
        drive.verify(file_id, identity).map(|_| ())
    }

    /// One unbound generated id, refilling the pool `ID_POOL` at a time. An unused id costs
    /// nothing: Drive ids bind only when a file is created under them.
    fn next_id(&self, drive: &mut Drive) -> Result<String, String> {
        let mut ids = self.ids.lock().map_err(|_| "registry id pool poisoned")?;
        if ids.is_empty() {
            *ids = drive.generate_ids(ID_POOL)?;
        }
        ids.pop()
            .ok_or_else(|| "drive generateIds: empty pool".into())
    }

    /// The listed identity confirms a completed binding when it equals the local identity.
    fn listed_matches(&self, file_id: &str, identity: &ObjectIdentity) -> bool {
        self.listed.get(file_id).is_some_and(|listed| {
            listed.bytes == identity.bytes && listed.sha256 == identity.sha256
        })
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
                        aliases: BTreeSet::new(),
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
        let files = drive.list("")?;
        self.rebuild_from(files, drive)
    }

    fn rebuild_from(&self, files: Vec<RemoteFile>, drive: &mut Drive) -> Result<(), String> {
        let _operation = self
            .operations
            .write()
            .map_err(|_| "registry operation lock poisoned")?;
        self.healthy()?;
        let mut confirmed: BTreeMap<String, Vec<Entry>> = BTreeMap::new();
        for remote in files {
            if !(remote.name.starts_with("object-")
                || remote.name.starts_with("manifest-")
                || remote.name.starts_with("catalog-")
                || remote.name.starts_with("record-"))
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
                    aliases: BTreeSet::new(),
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
        if let Some(mut entry) = state.files.get(&key).cloned() {
            if entry.bytes != identity.bytes || entry.sha256 != identity.sha256 {
                return Err("registry: content identity conflict".into());
            }
            if entry.aliases.insert(alias.into()) {
                self.record(
                    &mut state,
                    Change::Put {
                        key,
                        entry: entry.clone(),
                    },
                )?;
            }
            return Ok(entry);
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
        let legacy = match legacy {
            Some(entry)
                if entry.done
                    && crate::retire::retired_closures(
                        self.directory.parent().expect("state directory"),
                    )?
                    .contains(&entry.file_id) =>
            {
                None
            }
            entry => entry,
        };
        let (file_id, session, done) = if let Some(entry) = legacy {
            // A completed legacy receipt is still binding even when its remote file is
            // missing or trashed. Verify it before recording or allocating anything.
            if entry.done && !self.listed_matches(&entry.file_id, identity) {
                drive.verify(&entry.file_id, identity)?;
            }
            (entry.file_id.clone(), entry.session.clone(), entry.done)
        } else {
            (self.next_id(drive)?, None, false)
        };
        let entry = Entry {
            file_id,
            session,
            done,
            bytes: identity.bytes,
            sha256: identity.sha256.clone(),
            aliases: BTreeSet::from([alias.into()]),
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
            if self.listed_matches(&entry.file_id, identity) {
                return Ok(entry.file_id);
            }
            if drive
                .metadata(&entry.file_id)?
                .is_some_and(|file| !file.trashed)
            {
                drive.verify(&entry.file_id, identity)?;
                return Ok(entry.file_id);
            }
            // Only this store's completed retirement authorizes releasing a missing binding.
            // Arbitrary remote loss still fails closed, preserving pinned catalog receipts.
            let retired =
                crate::retire::retired_closures(self.directory.parent().expect("state directory"))?
                    .contains(&entry.file_id);
            if !retired {
                drive.verify(&entry.file_id, identity)?;
            }
            {
                let mut state = self.state.lock().map_err(|_| "registry lock poisoned")?;
                self.record(&mut state, Change::Remove { key: key.clone() })?;
            }
            entry = self.reserve(drive, identity, alias)?;
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

fn load_state(directory: &Path, settings: &DriveSettings, repair: bool) -> Result<State, String> {
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
        return Err("registry: archive root, endpoint, or version conflicts with snapshot".into());
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
                if !repair {
                    return Err(
                        "registry: incomplete journal tail; resume producer before retirement"
                            .into(),
                    );
                }
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
    Ok(state)
}

/// Read-only replay for retirement: the registry owns its snapshot watermark, journal changes,
/// logical aliases and legacy imports. No registry state changes while a plan is fingerprinted.
pub(crate) fn completed_aliases(
    state_dir: &Path,
    settings: &DriveSettings,
) -> Result<BTreeMap<String, BTreeSet<String>>, String> {
    let state = load_state(&state_dir.join("registry"), settings, false)?;
    let mut completed: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    for entry in state.files.values().filter(|e| e.done) {
        completed
            .entry(entry.file_id.clone())
            .or_default()
            .extend(entry.aliases.clone());
    }
    for (alias, entry) in state.legacy.iter().filter(|(_, e)| e.done) {
        completed
            .entry(entry.file_id.clone())
            .or_default()
            .insert(alias.clone());
    }
    for job in fs::read_dir(state_dir).map_err(|e| e.to_string())? {
        let job = job.map_err(|e| e.to_string())?;
        let path = job.path().join("transfers.json");
        if !path.is_file() {
            continue;
        }
        let legacy: LegacyTransfers =
            serde_json::from_slice(&fs::read(path).map_err(|e| e.to_string())?)
                .map_err(|e| e.to_string())?;
        for (key, entry) in legacy.files.into_iter().filter(|(_, e)| e.done) {
            completed
                .entry(entry.file_id)
                .or_default()
                .insert(format!("{}/{key}", job.file_name().to_string_lossy()));
        }
    }
    for aliases in completed.values() {
        for alias in aliases {
            pin_alias(alias, &mut BTreeSet::new())?;
        }
    }
    Ok(completed)
}

pub(crate) fn in_flight(
    state_dir: &Path,
    settings: &DriveSettings,
) -> Result<(BTreeSet<String>, bool), String> {
    let directory = state_dir.join("registry");
    let state = load_state(&directory, settings, false)?;
    let mut roots = BTreeSet::new();
    let mut unbound = false;
    let completed: BTreeSet<_> = state
        .files
        .values()
        .filter(|e| e.done)
        .map(|e| e.file_id.as_str())
        .collect();
    for (key, entry) in &state.files {
        if entry.done {
            continue;
        }
        roots.insert(key.clone());
        roots.insert(entry.file_id.clone());
        if entry.aliases.is_empty() {
            unbound = true;
        }
        for alias in &entry.aliases {
            pin_alias(alias, &mut roots)?;
        }
    }
    for (alias, entry) in &state.legacy {
        if !entry.done && !completed.contains(entry.file_id.as_str()) {
            roots.insert(entry.file_id.clone());
            pin_alias(alias, &mut roots)?;
        }
    }
    for job in fs::read_dir(state_dir).map_err(|e| e.to_string())? {
        let job = job.map_err(|e| e.to_string())?;
        let path = job.path().join("transfers.json");
        if !path.is_file() {
            continue;
        }
        let legacy: LegacyTransfers =
            serde_json::from_slice(&fs::read(path).map_err(|e| e.to_string())?)
                .map_err(|e| e.to_string())?;
        for (key, entry) in legacy.files {
            if !entry.done && !completed.contains(entry.file_id.as_str()) {
                roots.insert(entry.file_id);
                pin_key(&key, &mut roots)?;
            }
        }
    }
    Ok((roots, unbound))
}
fn pin_alias(alias: &str, roots: &mut BTreeSet<String>) -> Result<(), String> {
    let (_, key) = alias
        .split_once('/')
        .ok_or("registry: malformed logical alias")?;
    pin_key(key, roots)
}
fn pin_key(key: &str, roots: &mut BTreeSet<String>) -> Result<(), String> {
    roots.insert(key.into());
    if let Some(id) = key
        .strip_prefix("manifests/")
        .and_then(|s| s.strip_suffix("/ready.json"))
    {
        roots.insert(id.into());
    } else if let Some(pair) = key.strip_prefix("catalog/") {
        // Evidence revisions share market generations; the trailing evidence digest names
        // a catalog revision, not another dataset or stream dependency.
        roots.extend(pair.split('/').take(2).map(str::to_string));
    } else if !key.starts_with("objects/") && !key.starts_with("records/") {
        return Err("registry: unresolved logical transfer key".into());
    }
    Ok(())
}

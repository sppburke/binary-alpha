//! `binary-alpha data pipeline`: the research market-data coordinator. It stages selected
//! archives into an intake, imports and audits them through the existing owners into one
//! managed local store, extends them by bounded native-history acquisition from explicitly
//! bound seeds, archives each dataset and stream closure privately in Google Drive under an
//! immutable catalog published last, and restores one exact catalog into a fresh store. Every
//! mutable step is resumable from `pipeline_state/`; every completed record is immutable.

use std::collections::VecDeque;
use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, OnceLock};

use binary_alpha_engine::config::{
    Config, ConfigPath, ManifestUri, PublicationUri, RunMode, Seed, relative_path,
};
use binary_alpha_engine::dataset::{
    Coverage, DatasetRole, GenerationManifest, NativeGranularity, ObjectRecord, SourceKind,
    manifest_key,
};
use binary_alpha_engine::market::{
    BrokerId, format_event_time_micros as time_text, parse_event_time_micros as time,
};
use binary_alpha_engine::research::{Access, Declaration};
use binary_alpha_engine::stream::StreamManifest;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::audit;
use crate::broker::{self, Clock, SystemClock};
use crate::drive::{Drive, DriveSettings};
use crate::fetch::{self, Bounds, PageReceipt, Progress, ProgressEvent, Requested};
use crate::research;
use crate::store::{self, ObjectIdentity, Store};
use crate::verify;

pub const PIPELINE_SCHEMA_VERSION: u32 = 1;
pub const CATALOG_SCHEMA_VERSION: u32 = 1;
const STORE_DIR: &str = "store";
const STATE_DIR: &str = "pipeline_state";
const CATALOG_PREFIX: &str = "catalog-";

// ----------------------------------------------------------------------------------------------
// Configuration
// ----------------------------------------------------------------------------------------------

/// The application-owned pipeline document: one managed local root, one private archive, and
/// the jobs whose core configurations name their sources and history.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PipelineConfig {
    pub schema_version: u32,
    /// The managed root holding `store/`, `raw_sources/`, and `pipeline_state/`; a relative
    /// path resolves against this document's directory.
    pub local_root: PathBuf,
    /// An existing governance declaration every read applies; a standalone catalog never
    /// overrides its denial.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub governance_manifest: Option<String>,
    pub drive: DriveSettings,
    /// How many jobs one producer run works on at a time (default 1). Each job keeps its own
    /// broker connection and Drive session; the writer lock still admits one producer process.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parallel_jobs: Option<u32>,
    /// How many object uploads or downloads one job runs at a time (default 8), each worker
    /// with its own Drive session.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parallel_transfers: Option<u32>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub jobs: Vec<Job>,
}

/// One producer job: a core configuration declaring one history instrument whose imported
/// generation in the managed store is the seed, and the non-secret evidence binding that
/// archive to the configured broker context.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Job {
    pub id: String,
    pub config: PathBuf,
    pub evidence: PathBuf,
}

impl PipelineConfig {
    pub fn parse(text: &str) -> Result<Self, String> {
        let config: Self = toml::from_str(text).map_err(|error| error.to_string())?;
        config.validate()?;
        Ok(config)
    }

    fn validate(&self) -> Result<(), String> {
        if self.parallel_jobs == Some(0) {
            return Err("parallel_jobs must be positive".into());
        }
        if self.parallel_transfers == Some(0) {
            return Err("parallel_transfers must be positive".into());
        }
        if self.schema_version != PIPELINE_SCHEMA_VERSION {
            return Err(format!(
                "unsupported schema_version {}, expected {PIPELINE_SCHEMA_VERSION}",
                self.schema_version
            ));
        }
        if self.local_root.as_os_str().is_empty() {
            return Err("local_root: expected a non-empty path".into());
        }
        self.drive
            .validate()
            .map_err(|reason| format!("drive.{reason}"))?;
        for (index, job) in self.jobs.iter().enumerate() {
            let field = |name: &str| format!("jobs[{index}].{name}");
            if job.id.is_empty()
                || !job
                    .id
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'-')
            {
                return Err(format!(
                    "{}: must be letters, digits, `_`, or `-`",
                    field("id")
                ));
            }
            if self.jobs[..index]
                .iter()
                .any(|earlier| earlier.id == job.id)
            {
                return Err(format!("{}: {} is listed twice", field("id"), job.id));
            }
            for (name, path) in [("config", &job.config), ("evidence", &job.evidence)] {
                relative_path(&path.to_string_lossy())
                    .map_err(|reason| format!("{}: {reason}", field(name)))?;
            }
        }
        Ok(())
    }
}

/// The resolved managed root and its fixed children.
pub(crate) struct Layout {
    pub(crate) base: PathBuf,
    pub(crate) store: PathBuf,
    pub(crate) state: PathBuf,
    registry: OnceLock<Result<crate::registry::Registry, String>>,
}

impl Layout {
    fn open(config_path: &Path, config: &PipelineConfig) -> Result<Self, String> {
        let base = config_path.parent().unwrap_or(Path::new(".")).to_path_buf();
        let root = absolute(&base.join(&config.local_root))?;
        let store = root.join(STORE_DIR);
        let state = root.join(STATE_DIR);
        for dir in [&store, &state] {
            fs::create_dir_all(dir)
                .map_err(|error| format!("cannot create {}: {error}", dir.display()))?;
        }
        private(&state)?;
        Ok(Self {
            base,
            store,
            state,
            registry: OnceLock::new(),
        })
    }

    fn store(&self) -> Store {
        Store::filesystem(&self.store)
    }

    /// Immutable intents, receipts, and catalog receipts live in their own record store.
    fn records(&self) -> Store {
        Store::filesystem(self.state.join("records"))
    }

    fn job_state(&self, job: &str) -> Result<PathBuf, String> {
        let dir = self.state.join(job);
        fs::create_dir_all(&dir)
            .map_err(|error| format!("cannot create {}: {error}", dir.display()))?;
        private(&dir)?;
        Ok(dir)
    }

    fn manifest_uri(&self, generation: &str) -> String {
        self.store().uri(&manifest_key(generation))
    }
}

/// Creates the directory chain and canonicalizes the result.
fn absolute(path: &Path) -> Result<PathBuf, String> {
    fs::create_dir_all(path)
        .map_err(|error| format!("cannot create {}: {error}", path.display()))?;
    path.canonicalize()
        .map_err(|error| format!("cannot resolve {}: {error}", path.display()))
}

fn private(dir: &Path) -> Result<(), String> {
    use std::os::unix::fs::PermissionsExt;
    fs::set_permissions(dir, fs::Permissions::from_mode(0o700))
        .map_err(|error| format!("cannot restrict {}: {error}", dir.display()))
}

/// Replaces a mutable checkpoint atomically: written and flushed beside its target, then
/// renamed over it.
fn write_atomic(path: &Path, bytes: &[u8]) -> Result<(), String> {
    let temporary = path.with_extension("tmp");
    let mut file = File::create(&temporary)
        .map_err(|error| format!("cannot create {}: {error}", temporary.display()))?;
    file.write_all(bytes)
        .and_then(|()| file.sync_all())
        .map_err(|error| format!("cannot write {}: {error}", temporary.display()))?;
    fs::rename(&temporary, path)
        .map_err(|error| format!("cannot replace {}: {error}", path.display()))
}

fn read_json<T: for<'de> Deserialize<'de>>(path: &Path) -> Result<Option<T>, String> {
    match fs::read(path) {
        Ok(bytes) => serde_json::from_slice(&bytes)
            .map(Some)
            .map_err(|error| format!("{} is malformed: {error}", path.display())),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(format!("cannot read {}: {error}", path.display())),
    }
}

fn json_bytes(value: &impl Serialize) -> Result<Vec<u8>, String> {
    fetch::json_bytes(value)
}

fn sha256_hex(bytes: &[u8]) -> String {
    binary_alpha_engine::hex(&Sha256::digest(bytes))
}

/// The one local writer of an archive root: producer commands hold this nonblocking lock for
/// their whole run; installing consumers also hold it to exclude retirement.
pub(crate) fn writer_lock(layout: &Layout) -> Result<File, String> {
    let path = layout.state.join("writer.lock");
    let file = File::create(&path)
        .map_err(|error| format!("cannot create {}: {error}", path.display()))?;
    match file.try_lock() {
        Ok(()) => Ok(file),
        Err(std::fs::TryLockError::WouldBlock) => Err(format!(
            "pipeline: another producer holds {}",
            path.display()
        )),
        Err(std::fs::TryLockError::Error(error)) => {
            Err(format!("cannot lock {}: {error}", path.display()))
        }
    }
}

/// An immutable record beneath the record store, named by its own content hash; an identical
/// record is reused and different content at the same name is a conflict.
fn publish(records: &Store, prefix: &str, value: &impl Serialize) -> Result<String, String> {
    let bytes = json_bytes(value)?;
    let name = format!("{prefix}-{}.json", &sha256_hex(&bytes)[..32]);
    research::publish_record(records, records, &name, &bytes)?;
    Ok(name)
}

// ----------------------------------------------------------------------------------------------
// Job binding and the effective configuration
// ----------------------------------------------------------------------------------------------

/// The machine-checked part of the operator's source-binding evidence: the source identity the
/// archive was collected under (see `broker::source_identity`). Other keys are free-form notes.
#[derive(Deserialize)]
struct EvidenceBinding {
    source_identity: String,
}

/// The facts a job's core configuration must state before anything runs.
struct Bound {
    core: Config,
    /// The one history instrument the job extends.
    symbol: String,
    evidence_sha256: String,
}

fn bind(job: &Job, layout: &Layout) -> Result<Bound, String> {
    let core = crate::load_config(&layout.base.join(&job.config))
        .map_err(|reason| format!("job {}: {reason}", job.id))?;
    let field = |reason: String| format!("job {}: {reason}", job.id);
    if core.run_mode != RunMode::Research {
        return Err(field("run_mode must be research".into()));
    }
    if core.research.is_some() {
        return Err(field(
            "a job core configuration declares no research study; supply governance_manifest"
                .into(),
        ));
    }
    let history = core
        .history
        .as_ref()
        .ok_or_else(|| field("history is required".into()))?;
    let [symbol] = history.instruments.as_slice() else {
        return Err(field(
            "history.instruments must name exactly one instrument".into(),
        ));
    };
    let symbol = symbol.to_string();
    if history.refresh_interval_seconds.is_some() {
        return Err(field(
            "history.refresh_interval_seconds must be absent".into(),
        ));
    }
    if history.overlap_seconds.is_none()
        || history.max_pages.is_none()
        || history.max_elapsed_seconds.is_none()
    {
        return Err(field(
            "history.overlap_seconds, max_pages, and max_elapsed_seconds are required".into(),
        ));
    }
    let settings = core
        .brokers
        .iter()
        .find(|broker| broker.id() == &history.broker)
        .ok_or_else(|| field("history.broker is not declared".into()))?;
    let required = match settings.kind() {
        broker::BrokerKind::Deriv => NativeGranularity::Tick,
        broker::BrokerKind::PocketOption => NativeGranularity::Bar { period_seconds: 5 },
    };
    if history.native_granularity != required {
        return Err(field(format!(
            "history.native_granularity must be {required} for a {} broker",
            settings.kind()
        )));
    }
    let evidence = layout.base.join(&job.evidence);
    let evidence_bytes = fs::read(&evidence).map_err(|error| {
        field(format!(
            "evidence {} is unreadable: {error}",
            evidence.display()
        ))
    })?;
    // The operator's evidence binds the archive to one source context; the configured broker
    // must be that context, so a demo/real or endpoint switch is refused before admission.
    let declared: EvidenceBinding = serde_json::from_slice(&evidence_bytes)
        .map_err(|error| field(format!("evidence {}: {error}", evidence.display())))?;
    let configured = broker::source_identity(settings);
    if declared.source_identity != configured {
        return Err(field(format!(
            "evidence {} binds source identity {}, but the configured broker is {configured}",
            evidence.display(),
            declared.source_identity
        )));
    }
    let evidence_sha256 = sha256_hex(&evidence_bytes);
    Ok(Bound {
        core,
        symbol,
        evidence_sha256,
    })
}

/// What a pending intent binds: the effective configuration without its per-invocation page
/// and time budgets, which every resumption may set afresh.
fn binding_hash(config: &Config) -> String {
    let mut binding = config.clone();
    if let Some(history) = binding.history.as_mut() {
        history.max_pages = None;
        history.max_elapsed_seconds = None;
    }
    binding.content_hash()
}

/// The effective core configuration of a job: the managed store as both retained folder and
/// publication root, the seed binding, and the pinned cutoff.
fn effective(
    bound: &Bound,
    layout: &Layout,
    cutoff: i64,
    seeds: Vec<Seed>,
) -> Result<Config, String> {
    let mut config = bound.core.clone();
    config.storage.historical_data_dir =
        ConfigPath::try_from(layout.store.clone()).expect("an absolute store path");
    config.storage.publication_uri = PublicationUri::Filesystem(layout.store.clone());
    let history = config
        .history
        .as_mut()
        .expect("bound configuration has history");
    history.end = time_text(cutoff);
    history.seeds = seeds;
    // The effective document is parsed again so every cross-field rule applies to it.
    Config::parse(&config.canonical_toml())
        .map_err(|error| format!("effective configuration: {error}"))
}

// ----------------------------------------------------------------------------------------------
// Records
// ----------------------------------------------------------------------------------------------

/// The immutable binding of one invocation before any external request.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct Intent {
    schema_version: u32,
    command: String,
    job: String,
    pipeline_config_hash: String,
    base_config_hash: String,
    effective_config_hash: String,
    evidence_sha256: String,
    archive_root: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    cutoff: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    seeds: Vec<Seed>,
}

/// The pending acquisition of one job.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct Pending {
    intent: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    acquisition_id: Option<String>,
    effective_config_hash: String,
    progress: Progress,
}

/// Read legacy inline pages first, followed by complete append-only page records. Discard a
/// torn final append before future writes, so a replacement page starts on a clean line.
fn read_pending(path: &Path, pages_path: &Path) -> Result<(Option<Pending>, bool), String> {
    let Some(mut pending) = read_json::<Pending>(path)? else {
        return Ok((None, false));
    };
    let bytes = match fs::read(pages_path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Vec::new(),
        Err(error) => return Err(format!("cannot read {}: {error}", pages_path.display())),
    };
    let complete = bytes
        .iter()
        .rposition(|byte| *byte == b'\n')
        .map_or(0, |position| position + 1);
    for (index, line) in bytes[..complete]
        .split_inclusive(|byte| *byte == b'\n')
        .enumerate()
    {
        pending
            .progress
            .pages
            .push(serde_json::from_slice(line).map_err(|error| {
                format!("{} line {}: {error}", pages_path.display(), index + 1)
            })?);
    }
    let partial = complete != bytes.len();
    if partial {
        fs::OpenOptions::new()
            .write(true)
            .open(pages_path)
            .and_then(|file| file.set_len(complete as u64))
            .map_err(|error| format!("cannot truncate {}: {error}", pages_path.display()))?;
    }
    Ok((Some(pending), partial))
}

/// One finished invocation of one job.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct Receipt {
    schema_version: u32,
    command: String,
    job: String,
    intent: String,
    status: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    acquisition_id: Option<String>,
    dataset_generation: Option<String>,
    stream_generation: Option<String>,
    coverage: Option<fetch::HistoryCoverage>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    requests: Vec<PageReceipt>,
    catalog: Option<CatalogReceipt>,
    pending: bool,
}

/// The local record of one published catalog.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CatalogReceipt {
    pub file_id: String,
    pub sha256: String,
    pub bytes: u64,
}

// ----------------------------------------------------------------------------------------------
// Catalog and archive
// ----------------------------------------------------------------------------------------------

/// One archived ready manifest.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ManifestEntry {
    pub generation: String,
    pub key: String,
    pub sha256: String,
    pub bytes: u64,
    pub file_id: String,
}

/// One archived object of the closure.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObjectEntry {
    pub key: String,
    pub sha256: String,
    pub bytes: u64,
    pub file_id: String,
}

/// The immutable description of one dataset generation and its matching stream generation in
/// the archive: derived from the two validated ready manifests, published only after every
/// listed file was confirmed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Catalog {
    pub schema_version: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub layout: Option<binary_alpha_engine::dataset::Layout>,
    pub job: String,
    pub broker: String,
    pub provider_symbol: String,
    pub instrument: String,
    pub role: DatasetRole,
    pub source_kind: SourceKind,
    pub native_granularity: NativeGranularity,
    pub coverage: Coverage,
    pub row_count: u64,
    pub dataset: ManifestEntry,
    pub stream: ManifestEntry,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub lineage_manifests: Vec<ManifestEntry>,
    pub objects: Vec<ObjectEntry>,
}

impl Catalog {
    pub fn from_json(bytes: &[u8]) -> Result<Self, String> {
        let catalog: Self = serde_json::from_slice(bytes).map_err(|error| error.to_string())?;
        if catalog.schema_version != CATALOG_SCHEMA_VERSION {
            return Err(format!(
                "unsupported catalog schema_version {}, expected {CATALOG_SCHEMA_VERSION}",
                catalog.schema_version
            ));
        }
        let mut generations = std::collections::BTreeSet::new();
        for entry in [&catalog.dataset, &catalog.stream]
            .into_iter()
            .chain(&catalog.lineage_manifests)
        {
            if entry.generation.len() != 64
                || !entry.generation.bytes().all(|b| b.is_ascii_hexdigit())
                || entry.key != manifest_key(&entry.generation)
                || !generations.insert(&entry.generation)
            {
                return Err("catalog manifest keys must name unique generation identities".into());
            }
        }
        for (index, object) in catalog.objects.iter().enumerate() {
            if object.key != binary_alpha_engine::dataset::object_key(&object.sha256)
                || catalog.objects[..index]
                    .iter()
                    .any(|earlier| earlier.key == object.key)
            {
                return Err(format!(
                    "catalog object `{}` is not content-addressed once",
                    object.key
                ));
            }
        }
        Ok(catalog)
    }
}

/// Archives one dataset generation and its stream generation from the managed store: every
/// object once, then both manifests, then the catalog. A catalog receipt already recorded for
/// the pair is reused after its remote file is confirmed.
fn archive_generation(
    config: &PipelineConfig,
    drive: &mut Drive,
    layout: &Layout,
    job: &str,
    dataset: &str,
    stream: &str,
    access: Access<'_>,
) -> Result<CatalogReceipt, String> {
    let local = layout.store();
    let records = layout.records();
    let receipt_name = format!("{job}-catalog-{}-{}.json", &dataset[..16], &stream[..16]);
    let (dataset_manifest, dataset_bytes) = read_manifest(&local, dataset)?;
    let mut stream_bytes = Vec::new();
    local.read_to(&manifest_key(stream), None, &mut stream_bytes)?;
    let stream_manifest =
        StreamManifest::from_json(&stream_bytes).map_err(|error| format!("{stream}: {error}"))?;
    if stream_manifest.source_generation != dataset_manifest.generation
        || stream_manifest.instrument != dataset_manifest.instrument
        || stream_manifest.role != dataset_manifest.role
        || stream_manifest.layout != dataset_manifest.layout
        || dataset_manifest.role == DatasetRole::Holdout
    {
        return Err(format!(
            "pipeline {job}: stream {stream} does not derive from ordinary dataset {dataset}"
        ));
    }
    let state = layout.job_state(job)?;
    let registry = layout
        .registry
        .get_or_init(|| crate::registry::Registry::open(&layout.state, &config.drive, drive))
        .as_ref()
        .map_err(Clone::clone)?;
    // Immutable receipts pin exact file ids even if a later rebuild finds duplicate bytes.
    if let Some(receipt) =
        read_json::<CatalogReceipt>(&records.local_path(&receipt_name).expect("local records"))?
    {
        confirm_remote(drive, &receipt.file_id, receipt.bytes, &receipt.sha256)?;
        let scratch = state.join(".existing-catalog");
        drive.download(
            &receipt.file_id,
            &scratch,
            &ObjectIdentity {
                bytes: receipt.bytes,
                sha256: receipt.sha256.clone(),
                crc32c: 0,
            },
        )?;
        let catalog = Catalog::from_json(&fs::read(&scratch).map_err(|e| e.to_string())?)?;
        fs::remove_file(scratch).map_err(|e| e.to_string())?;
        if catalog.job != job
            || catalog.dataset.generation != dataset
            || catalog.stream.generation != stream
        {
            return Err("catalog receipt does not bind the requested job and generations".into());
        }
        for entry in &catalog.objects {
            confirm_remote(drive, &entry.file_id, entry.bytes, &entry.sha256)?;
        }
        for entry in [&catalog.dataset, &catalog.stream]
            .into_iter()
            .chain(&catalog.lineage_manifests)
        {
            confirm_remote(drive, &entry.file_id, entry.bytes, &entry.sha256)?;
        }
        return Ok(receipt);
    }
    let legacy = registry.legacy_catalog_bindings(drive, job, dataset, stream)?;
    let transfer =
        |drive: &mut Drive, key: &str, name: &str, path: &Path, identity: &ObjectIdentity| {
            if let Some(bindings) = &legacy {
                let file_id = bindings
                    .get(key)
                    .ok_or_else(|| format!("legacy catalog: missing binding for {key}"))?;
                confirm_remote(drive, file_id, identity.bytes, &identity.sha256)?;
                return Ok(file_id.clone());
            }
            registry.transfer(drive, &format!("{job}/{key}"), name, path, identity)
        };
    // Every descendant catalog also owns the continuation root and its configured stream.
    // This makes fresh-store restore preserve seed identity and retirement's retained root.
    let mut lineage_manifests = Vec::new();
    let mut lineage_objects = Vec::new();
    if dataset_manifest.layout == Some(binary_alpha_engine::dataset::Layout::DailyV2)
        && let Some(root) = crate::lineage::root(
            &local,
            &dataset_manifest.instrument,
            dataset_manifest.role,
            access,
        )?
        && root != dataset
    {
        let (root_manifest, bytes) = read_manifest(&local, &root)?;
        verify::run_with(&local.uri(&root_manifest.key()), access)?;
        lineage_objects.extend(root_manifest.objects);
        lineage_manifests.push((root.clone(), manifest_key(&root), bytes));
        let root_stream = binary_alpha_engine::stream::stream_generation_id_with_layout(
            &root,
            &stream_manifest.definition.canonical_toml(),
            dataset_manifest.layout,
        );
        let root_key = manifest_key(&root_stream);
        if local.head(&root_key)?.is_none() {
            // The root may have been imported without an audit. Use the same semantic audit
            // owner and definition as this descendant, before publishing either catalog.
            let core = crate::load_config(
                &layout.base.join(
                    &config
                        .jobs
                        .iter()
                        .find(|j| j.id == job)
                        .ok_or("archive job absent")?
                        .config,
                ),
            )?;
            let mut core = core;
            core.storage.historical_data_dir = ConfigPath::try_from(layout.store.clone())?;
            core.storage.publication_uri = PublicationUri::Filesystem(layout.store.clone());
            finish(&core, layout, &local, &root, access, &mut std::io::sink())?;
        }
        verify::run_with(&local.uri(&root_key), access)?;
        let mut bytes = Vec::new();
        local.read_to(&root_key, None, &mut bytes)?;
        let root_manifest = StreamManifest::from_json(&bytes)?;
        lineage_objects.extend(root_manifest.objects);
        lineage_manifests.push((root_stream, root_key, bytes));
    }
    let mut closure: Vec<&ObjectRecord> = Vec::new();
    for object in dataset_manifest
        .objects
        .iter()
        .chain(stream_manifest.objects.iter())
        .chain(lineage_objects.iter())
    {
        if !closure.iter().any(|known| known.key == object.key) {
            closure.push(object);
        }
    }
    let objects = run_pool(config, &closure, |object, drive| {
        let path = local
            .local_path(&object.key)
            .expect("the managed store is local");
        let identity = store::identify(&path)?;
        if identity.bytes != object.bytes || identity.sha256 != object.sha256 {
            return Err(format!(
                "pipeline {job}: {} does not carry its recorded identity",
                local.uri(&object.key)
            ));
        }
        let file_id = transfer(
            drive,
            &object.key,
            &format!("object-{}", object.sha256),
            &path,
            &identity,
        )?;
        Ok(ObjectEntry {
            key: object.key.clone(),
            sha256: object.sha256.clone(),
            bytes: object.bytes,
            file_id,
        })
    })?;
    let mut manifests = Vec::with_capacity(2 + lineage_manifests.len());
    for (generation, key, bytes) in [
        (dataset, dataset_manifest.key(), &dataset_bytes),
        (stream, stream_manifest.key(), &stream_bytes),
    ]
    .into_iter()
    .chain(
        lineage_manifests
            .iter()
            .map(|(g, k, b)| (g.as_str(), k.clone(), b)),
    ) {
        let scratch = state.join(format!(".manifest-{generation}"));
        fs::write(&scratch, bytes)
            .map_err(|error| format!("cannot write {}: {error}", scratch.display()))?;
        let identity = store::identify(&scratch)?;
        let file_id = transfer(
            drive,
            &key,
            &format!("manifest-{generation}.json"),
            &scratch,
            &identity,
        )?;
        fs::remove_file(&scratch)
            .map_err(|error| format!("cannot remove {}: {error}", scratch.display()))?;
        manifests.push(ManifestEntry {
            generation: generation.to_string(),
            key,
            sha256: identity.sha256,
            bytes: identity.bytes,
            file_id,
        });
    }
    let lineage_manifests = manifests.split_off(2);
    let stream_entry = manifests.pop().expect("stream entry");
    let dataset_entry = manifests.pop().expect("dataset entry");
    let catalog = Catalog {
        schema_version: CATALOG_SCHEMA_VERSION,
        layout: dataset_manifest.layout,
        job: job.to_string(),
        broker: dataset_manifest.broker.to_string(),
        provider_symbol: dataset_manifest.provider_symbol.to_string(),
        instrument: dataset_manifest.instrument.clone(),
        role: dataset_manifest.role,
        source_kind: dataset_manifest.source_kind,
        native_granularity: dataset_manifest.native_granularity,
        coverage: dataset_manifest.coverage.clone(),
        row_count: dataset_manifest.row_count,
        dataset: dataset_entry,
        stream: stream_entry,
        lineage_manifests,
        objects,
    };
    let bytes = json_bytes(&catalog)?;
    let scratch = state.join(format!(".catalog-{}", &dataset[..16]));
    fs::write(&scratch, &bytes)
        .map_err(|error| format!("cannot write {}: {error}", scratch.display()))?;
    let identity = store::identify(&scratch)?;
    let file_id = registry.transfer(
        drive,
        &format!("{job}/catalog/{dataset}/{stream}"),
        &format!("{CATALOG_PREFIX}{}-{}.json", &dataset[..16], &stream[..16]),
        &scratch,
        &identity,
    )?;
    fs::remove_file(&scratch)
        .map_err(|error| format!("cannot remove {}: {error}", scratch.display()))?;
    let receipt = CatalogReceipt {
        file_id,
        sha256: identity.sha256,
        bytes: identity.bytes,
    };
    research::publish_record(&records, &records, &receipt_name, &json_bytes(&receipt)?)?;
    Ok(receipt)
}

/// Transfers objects with one Drive session per worker. A failure stops new work; in-flight
/// transfers finish and checkpoint before results (including failures) return in input order.
fn run_pool<T: Sync, R: Send>(
    config: &PipelineConfig,
    items: &[T],
    work: impl Fn(&T, &mut Drive) -> Result<R, String> + Sync,
) -> Result<Vec<R>, String> {
    let workers = usize::try_from(config.parallel_transfers.unwrap_or(8))
        .unwrap_or(8)
        .min(items.len());
    let queue = Mutex::new(items.iter().enumerate().collect::<VecDeque<_>>());
    std::thread::scope(|scope| {
        let handles: Vec<_> = (0..workers)
            .map(|_| {
                scope.spawn(|| {
                    let mut drive = None;
                    let mut results = Vec::new();
                    loop {
                        let Some((index, item)) =
                            queue.lock().expect("transfer queue lock").pop_front()
                        else {
                            break;
                        };
                        let result = (|| {
                            // Opening after reservation binds any session failure to this item.
                            if drive.is_none() {
                                drive = Some(Drive::open(&config.drive)?);
                            }
                            work(item, drive.as_mut().expect("worker Drive session"))
                        })();
                        let failed = result.is_err();
                        if failed {
                            queue.lock().expect("transfer queue lock").clear();
                        }
                        results.push((index, result));
                        if failed {
                            break;
                        }
                    }
                    results
                })
            })
            .collect();
        let mut results = Vec::with_capacity(items.len());
        for handle in handles {
            results.extend(
                handle
                    .join()
                    .map_err(|_| "pipeline: transfer worker panicked")?,
            );
        }
        results.sort_unstable_by_key(|(index, _)| *index);
        results.into_iter().map(|(_, result)| result).collect()
    })
}

/// A remote file the index claims complete still carries exactly the local identity.
fn confirm_remote(
    drive: &mut Drive,
    file_id: &str,
    bytes: u64,
    sha256: &str,
) -> Result<(), String> {
    drive
        .verify(
            file_id,
            &ObjectIdentity {
                bytes,
                sha256: sha256.to_string(),
                crc32c: 0,
            },
        )
        .map(|_| ())
}

/// The newest imported dataset generation of one instrument in the store: the seed every
/// update extends. Broker-history descendants are found from it by the fetch owner.
fn imported_seed(
    local: &Store,
    broker: &BrokerId,
    symbol: &str,
    role: DatasetRole,
    access: Access<'_>,
) -> Result<Option<String>, String> {
    let instrument = format!("{broker}:{symbol}");
    if let Some(root) = crate::lineage::root(local, &instrument, role, access)? {
        return Ok(Some(root));
    }
    let mut newest: Option<(String, String)> = None;
    for generation in local.list_manifests()? {
        let mut bytes = Vec::new();
        local.read_to(&manifest_key(&generation), None, &mut bytes)?;
        if verify::manifest_kind(&bytes)?.is_some() {
            continue;
        }
        let manifest = GenerationManifest::from_json(&bytes)
            .map_err(|error| format!("{generation}: {error}"))?;
        if manifest.instrument != instrument
            || manifest.role != role
            || manifest.source_kind == SourceKind::BrokerHistory
        {
            continue;
        }
        let last = manifest.coverage.last_event_time.clone();
        if newest.as_ref().is_none_or(|(_, known)| last > *known) {
            newest = Some((manifest.generation.clone(), last));
        }
    }
    Ok(newest.map(|(generation, _)| generation))
}

fn read_manifest(local: &Store, generation: &str) -> Result<(GenerationManifest, Vec<u8>), String> {
    let mut bytes = Vec::new();
    local.read_to(&manifest_key(generation), None, &mut bytes)?;
    let manifest =
        GenerationManifest::from_json(&bytes).map_err(|error| format!("{generation}: {error}"))?;
    Ok((manifest, bytes))
}

// ----------------------------------------------------------------------------------------------
// Producer commands
// ----------------------------------------------------------------------------------------------

/// The declaration a pipeline applies to every read, when it names one.
pub(crate) fn declaration(config: &PipelineConfig) -> Result<Option<Declaration>, String> {
    config
        .governance_manifest
        .as_deref()
        .map(research::load_declaration)
        .transpose()
        .map_err(|reason| format!("governance_manifest: {reason}"))
}

pub(crate) fn load(config_path: &Path) -> Result<(PipelineConfig, Layout, String), String> {
    let text = fs::read_to_string(config_path)
        .map_err(|error| format!("cannot read {}: {error}", config_path.display()))?;
    let config = PipelineConfig::parse(&text)?;
    let layout = Layout::open(config_path, &config)?;
    let hash = sha256_hex(text.as_bytes());
    Ok((config, layout, hash))
}

/// Archive existing v2 history without acquisition. Verification uses the same
/// owner as update; all jobs share the archive-root registry and writer lock.
pub fn archive(config_path: &Path, job: Option<&str>, out: &mut dyn Write) -> Result<(), String> {
    let (mut config, layout, _) = load(config_path)?;
    if let Some(id) = job {
        config.jobs.retain(|job| job.id == id);
        if config.jobs.is_empty() {
            return Err(format!("pipeline: unknown job {id}"));
        }
    }
    run_jobs(&config, &layout, out, &|job, bound, drive, access, _out| {
        let local = layout.store();
        let history = bound.core.history.as_ref().expect("bound history");
        let dataset = crate::lineage::newest_daily(
            &local,
            &format!("{}:{}", history.broker, bound.symbol),
            history.role,
            access,
        )?;
        let stream = crate::lineage::verified_daily_stream(&local, &dataset, &bound.core, access)?;
        let receipt =
            archive_generation(&config, drive, &layout, &job.id, &dataset, &stream, access)?;
        Ok(format!(
            "pipeline archive {} dataset {dataset} stream {stream} catalog {} sha256 {}",
            job.id, receipt.file_id, receipt.sha256
        ))
    })
}

/// `data pipeline update`: extend every job's imported generation from its frontier to one
/// pinned cutoff within its budget, then audit, verify, and archive the result.
pub fn update(config_path: &Path, end: Option<&str>, out: &mut dyn Write) -> Result<(), String> {
    update_with(config_path, end, &SystemClock, out)
}

/// `update` under an explicit clock: the cutoff and the invocation deadline come from it.
pub fn update_with(
    config_path: &Path,
    end: Option<&str>,
    clock: &dyn Clock,
    out: &mut dyn Write,
) -> Result<(), String> {
    let (config, layout, hash) = load(config_path)?;
    let end = end.map(time).transpose()?;
    run_jobs(&config, &layout, out, &|job, bound, drive, access, out| {
        update_job(
            &config, job, bound, &layout, &hash, drive, access, end, clock, out,
        )
    })
}

/// One job's work: the bound job, its own Drive session, the access rule, and its report sink.
type JobRun<'a> = dyn Fn(&Job, Bound, &mut Drive, Access<'_>, &mut dyn Write) -> Result<String, String>
    + Sync
    + 'a;

/// Runs every job independently under the writer lock, up to `parallel_jobs` at a time, each
/// with its own Drive session; one failed job never masks another, each job's report lines stay
/// contiguous, and the command fails when any job did.
fn run_jobs(
    config: &PipelineConfig,
    layout: &Layout,
    out: &mut dyn Write,
    run: &JobRun<'_>,
) -> Result<(), String> {
    if config.jobs.is_empty() {
        return Err("pipeline: the configuration declares no jobs".into());
    }
    let _lock = writer_lock(layout)?;
    let _archive_lock = archive_lock(config)?;
    let declaration = declaration(config)?;
    let access = Access {
        declaration: declaration.as_ref(),
        certification: None,
    };
    // No acquisition or publication may race reachability checks and single-page deletion.
    let reclaim = || -> Result<(), String> {
        for job in &config.jobs {
            crate::lineage::reclaim(
                &layout.store(),
                &layout.job_state(&job.id)?,
                &layout.state,
                &layout.records(),
                access,
            )?;
        }
        Ok(())
    };
    reclaim()?;
    let workers = usize::try_from(config.parallel_jobs.unwrap_or(1))
        .unwrap_or(1)
        .min(config.jobs.len())
        .max(1);
    let queue = std::sync::Mutex::new(
        config
            .jobs
            .iter()
            .enumerate()
            .collect::<std::collections::VecDeque<_>>(),
    );
    let (reports, finished) = std::sync::mpsc::channel();
    let one = |job: &Job| {
        let mut lines = Vec::new();
        let result = Drive::open(&config.drive).and_then(|mut drive| {
            bind(job, layout).and_then(|bound| run(job, bound, &mut drive, access, &mut lines))
        });
        match &result {
            Ok(line) => writeln!(lines, "{line}").expect("writing to a Vec cannot fail"),
            Err(reason) => {
                writeln!(lines, "pipeline job {} failed: {reason}", job.id)
                    .expect("writing to a Vec cannot fail");
            }
        }
        (lines, result.is_err())
    };
    let mut failed = std::thread::scope(|scope| {
        for _ in 0..workers {
            let reports = reports.clone();
            let queue = &queue;
            let one = &one;
            scope.spawn(move || {
                loop {
                    let job = match queue.lock() {
                        Ok(mut queue) => queue.pop_front(),
                        Err(_) => None,
                    };
                    let Some((index, job)) = job else { break };
                    let (lines, failed) = one(job);
                    if reports.send((index, lines, failed)).is_err() {
                        break;
                    }
                }
            });
        }
        drop(reports);
        let mut failed = Vec::new();
        for (index, lines, job_failed) in finished {
            out.write_all(&lines)
                .and_then(|()| out.flush())
                .map_err(|error| format!("cannot write the report: {error}"))?;
            if job_failed {
                failed.push(index);
            }
        }
        Ok::<_, String>(failed)
    })?;
    reclaim()?;
    failed.sort_unstable();
    if failed.is_empty() {
        Ok(())
    } else {
        Err(format!(
            "pipeline: {} job(s) failed: {}",
            failed.len(),
            failed
                .iter()
                .map(|index| config.jobs[*index].id.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        ))
    }
}

/// Audits and verifies one dataset generation in the managed store, returning its stream
/// generation.
fn finish(
    config: &Config,
    layout: &Layout,
    local: &Store,
    dataset: &str,
    access: Access<'_>,
    out: &mut dyn Write,
) -> Result<String, String> {
    let uri = layout.manifest_uri(dataset);
    let audited = audit::audit(config, &uri, local, local, access)?;
    writeln!(out, "{}", audited.report)
        .map_err(|error| format!("cannot write the report: {error}"))?;
    verify::run_with(&uri, access)?;
    verify::run_with(&layout.manifest_uri(&audited.generation), access)?;
    Ok(audited.generation)
}

#[allow(clippy::too_many_arguments)]
fn update_job(
    pipeline: &PipelineConfig,
    job: &Job,
    bound: Bound,
    layout: &Layout,
    pipeline_hash: &str,
    drive: &mut Drive,
    access: Access<'_>,
    end: Option<i64>,
    clock: &dyn Clock,
    out: &mut dyn Write,
) -> Result<String, String> {
    let state = layout.job_state(&job.id)?;
    let local = layout.store();
    let history = bound.core.history.as_ref().expect("bound history");
    let settings = bound
        .core
        .brokers
        .iter()
        .find(|broker| broker.id() == &history.broker)
        .expect("bound broker");
    let imported = imported_seed(&local, &history.broker, &bound.symbol, history.role, access)?
        .ok_or_else(|| {
            format!(
                "job {}: the store holds no imported generation for {}:{}; run `data import` first",
                job.id, history.broker, bound.symbol
            )
        })?;
    let pending_path = state.join("progress.json");
    let pages_path = state.join("progress.pages.jsonl");
    let (pending, partial) = read_pending(&pending_path, &pages_path)?;
    let (diagnostics, received_partial) = crate::lineage::diagnostics(
        &state,
        pending.as_ref().map(|p| p.progress.pages.as_slice()),
    )?;
    if received_partial {
        writeln!(out, "pipeline job {} received log: incomplete response metadata retained as unresolved; single-page reclamation deferred", job.id).map_err(|e| e.to_string())?;
    }
    if partial {
        writeln!(
            out,
            "pipeline job {} progress log: 1 partial line ignored",
            job.id
        )
        .map_err(|error| format!("cannot write the report: {error}"))?;
    }
    let cutoff = match (&pending, end) {
        (Some(pending), Some(end)) if time(&pending.progress.cutoff)? != end => {
            return Err(format!(
                "job {}: intent {} is pending at cutoff {}; the requested cutoff {} conflicts",
                job.id,
                pending.intent,
                pending.progress.cutoff,
                time_text(end)
            ));
        }
        (Some(pending), _) => time(&pending.progress.cutoff)?,
        (None, Some(end)) => end,
        (None, None) => clock.now_micros(),
    };
    let seed = Seed {
        provider_symbol: bound
            .symbol
            .clone()
            .try_into()
            .expect("a bound symbol is a provider symbol"),
        manifest: layout.manifest_uri(&imported).parse::<ManifestUri>()?,
        source_identity: broker::source_identity(settings),
    };
    let config = effective(&bound, layout, cutoff, vec![seed.clone()])?;
    let binding = binding_hash(&config);
    let records = layout.records();
    if let Some(pending) = &pending {
        if pending.effective_config_hash != binding {
            return Err(format!(
                "job {}: intent {} is pending under configuration {}; the current effective configuration {binding} conflicts",
                job.id, pending.intent, pending.effective_config_hash
            ));
        }
        let mut bytes = Vec::new();
        records.read_to(&pending.intent, None, &mut bytes)?;
        let opened: Intent = serde_json::from_slice(&bytes)
            .map_err(|error| format!("{}: {error}", records.uri(&pending.intent)))?;
        if opened.archive_root != drive.root() || opened.evidence_sha256 != bound.evidence_sha256 {
            return Err(format!(
                "job {}: intent {} is pending under archive root {} and evidence {}; the current archive root and evidence conflict",
                job.id, pending.intent, opened.archive_root, opened.evidence_sha256
            ));
        }
    }
    // Seed binding and source context are checked before any credential is resolved.
    fetch::prepare(&config, &local, access.declaration)?;
    let intent = match &pending {
        Some(pending) => pending.intent.clone(),
        None => publish(
            &records,
            &format!("{}-intent", job.id),
            &Intent {
                schema_version: PIPELINE_SCHEMA_VERSION,
                command: "update".into(),
                job: job.id.clone(),
                pipeline_config_hash: pipeline_hash.to_string(),
                base_config_hash: bound.core.content_hash(),
                effective_config_hash: config.content_hash(),
                evidence_sha256: bound.evidence_sha256.clone(),
                archive_root: drive.root().to_string(),
                cutoff: Some(time_text(cutoff)),
                seeds: vec![seed],
            },
        )?,
    };
    write_atomic(
        &state.join("update.toml"),
        config.canonical_toml().as_bytes(),
    )?;
    let history = config.history.as_ref().expect("bound history");
    let deadline = clock
        .now_micros()
        .saturating_add(i64::from(history.max_elapsed_seconds.expect("bound")) * 1_000_000);
    let token = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|e| e.to_string())?
        .as_nanos()
        .to_string();
    let acquisition_id = publish(
        &records,
        &format!("{}-acquisition", job.id),
        &serde_json::json!({"schema_version": 1, "intent": intent, "invocation": token, "process": std::process::id()}),
    )?;
    let intent_name = intent.clone();
    let mut persist = |event: ProgressEvent<'_>| -> Result<(), String> {
        match event {
            ProgressEvent::Started(progress) => {
                crate::lineage::clear_received(&state)?;
                // A crash after removing a completed header can leave its old log behind.
                File::create(&pages_path)
                    .map_err(|error| format!("cannot create {}: {error}", pages_path.display()))?;
                write_atomic(
                    &pending_path,
                    &json_bytes(&Pending {
                        intent: intent_name.clone(),
                        acquisition_id: Some(acquisition_id.clone()),
                        effective_config_hash: binding.clone(),
                        progress: progress.clone(),
                    })?,
                )
            }
            ProgressEvent::Received(page) => crate::lineage::received(&state, page),
            ProgressEvent::Invalidate => {
                if let Some(mut pending) = read_json::<Pending>(&pending_path)? {
                    pending.progress.pages.clear();
                    write_atomic(&pending_path, &json_bytes(&pending)?)?;
                }
                File::create(&pages_path)
                    .and_then(|f| f.sync_all())
                    .map_err(|e| format!("cannot invalidate {}: {e}", pages_path.display()))
            }
            ProgressEvent::Page(page) => {
                let mut line = serde_json::to_vec(page).map_err(|error| error.to_string())?;
                line.push(b'\n');
                let mut file = fs::OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(&pages_path)
                    .map_err(|error| format!("cannot open {}: {error}", pages_path.display()))?;
                let written = file
                    .write(&line)
                    .map_err(|error| format!("cannot append {}: {error}", pages_path.display()))?;
                if written != line.len() {
                    return Err(format!(
                        "short append to {}: {written} of {} bytes",
                        pages_path.display(),
                        line.len()
                    ));
                }
                file.sync_all()
                    .map_err(|error| format!("cannot sync {}: {error}", pages_path.display()))
            }
        }
    };
    let mut adapter = broker::connect(&config)?;
    let outcomes = {
        let mut bounds = Bounds {
            diagnostics,
            acquisition: Some(fetch::OccurrenceIdentity {
                acquisition_id: acquisition_id.clone(),
                intent: Some(intent.clone()),
                ordinal: 0,
            }),
            max_pages: history.max_pages,
            deadline_micros: Some(deadline),
            clock,
            declaration: access.declaration,
            resume: pending.map(|pending| pending.progress),
            persist: Some(&mut persist),
        };
        fetch::acquire(
            &config,
            adapter.market(),
            &local,
            &local,
            Requested::Advance { cutoff },
            &mut bounds,
            out,
        )?
    };
    let [outcome] = outcomes.as_slice() else {
        return Err(format!("job {}: expected one acquisition outcome", job.id));
    };
    let daily = outcome
        .generation
        .as_deref()
        .map(|g| {
            read_manifest(&local, g)
                .map(|(m, _)| m.layout == Some(binary_alpha_engine::dataset::Layout::DailyV2))
        })
        .transpose()?
        .unwrap_or(false);
    if !outcome.pending && !daily {
        crate::lineage::clear_pending(&state)?;
    }
    let (stream, catalog) = match &outcome.generation {
        Some(dataset) => {
            let stream = finish(&config, layout, &local, dataset, access, out)?;
            let catalog =
                archive_generation(pipeline, drive, layout, &job.id, dataset, &stream, access)?;
            (Some(stream), Some(catalog))
        }
        None => (None, None),
    };
    // A provider tail before the cutoff is ordinary; a shortfall on the start side of the
    // acquisition leaves a gap the archive must report.
    let gaps = outcome
        .coverage
        .shortfall
        .as_ref()
        .is_some_and(|shortfall| shortfall.reason != fetch::TAIL_SHORTFALL);
    let status = match (outcome.pending, gaps, &catalog) {
        (true, _, _) => "pending",
        (false, _, None) => "no_data",
        (false, true, Some(_)) => "archived_with_gaps",
        (false, false, Some(_)) => "archived",
    };
    let receipt = publish(
        &records,
        &format!("{}-receipt", job.id),
        &Receipt {
            schema_version: PIPELINE_SCHEMA_VERSION,
            command: "update".into(),
            job: job.id.clone(),
            intent: intent.clone(),
            status: status.into(),
            acquisition_id: Some(acquisition_id),
            dataset_generation: outcome.generation.clone(),
            stream_generation: stream.clone(),
            coverage: Some(outcome.coverage.clone()),
            requests: outcome.receipts.clone(),
            catalog: catalog.clone(),
            pending: outcome.pending,
        },
    )?;
    if !outcome.pending
        && let Some(generation) = &outcome.generation
    {
        let (manifest, _) = read_manifest(&local, generation)?;
        if manifest.layout == Some(binary_alpha_engine::dataset::Layout::DailyV2) {
            crate::lineage::schedule_reclamation(
                &state,
                &intent,
                &receipt,
                generation,
                &outcome.receipts,
            )?;
        }
    }
    if !outcome.pending {
        crate::lineage::clear_pending(&state)?;
    }
    let line = format!(
        "pipeline update {} {} cutoff {} status {status} requested {} {} verified {} shortfall {} dataset {} stream {} catalog {} sha256 {}",
        job.id,
        outcome.instrument,
        time_text(cutoff),
        outcome.coverage.requested.start,
        outcome.coverage.requested.end,
        outcome
            .coverage
            .verified
            .as_ref()
            .map_or("none none".to_string(), |range| format!(
                "{} {}",
                range.start, range.end
            )),
        outcome
            .coverage
            .shortfall
            .as_ref()
            .map_or("none", |shortfall| shortfall.reason.as_str()),
        outcome.generation.as_deref().unwrap_or("none"),
        stream.as_deref().unwrap_or("none"),
        catalog
            .as_ref()
            .map_or("none", |catalog| catalog.file_id.as_str()),
        catalog
            .as_ref()
            .map_or("none", |catalog| catalog.sha256.as_str()),
    );
    if status == "archived" || status == "no_data" && !outcome.pending {
        Ok(line)
    } else {
        // A pending acquisition or remaining gaps are reported, never passed silently.
        Err(line)
    }
}

// ----------------------------------------------------------------------------------------------
// Consumer commands
// ----------------------------------------------------------------------------------------------

/// `data pipeline list`: every catalog of one instrument in the archive root, read from the
/// catalog files alone.
pub fn list(
    config_path: &Path,
    broker: &str,
    symbol: &str,
    out: &mut dyn Write,
) -> Result<(), String> {
    let (config, layout, _) = load(config_path)?;
    let mut drive = Drive::open(&config.drive)?;
    let mut lines = Vec::new();
    for (file_id, sha256, catalog) in catalogs(&mut drive, &layout, broker, symbol)? {
        let transfer: u64 = catalog
            .objects
            .iter()
            .map(|object| object.bytes)
            .sum::<u64>()
            + catalog.dataset.bytes
            + catalog.stream.bytes
            + catalog
                .lineage_manifests
                .iter()
                .map(|entry| entry.bytes)
                .sum::<u64>();
        lines.push(format!(
            "catalog {file_id} sha256 {sha256} {} {} {} layout {} dataset {} stream {} coverage {} {} rows {} bytes {transfer}",
            catalog.instrument,
            catalog.role,
            catalog.native_granularity,
            catalog.layout.map_or("v1".to_string(), |layout| layout.to_string()),
            catalog.dataset.generation,
            catalog.stream.generation,
            catalog.coverage.first_event_time,
            catalog.coverage.last_event_time,
            catalog.row_count
        ));
    }
    lines.sort();
    for line in lines {
        writeln!(out, "{line}").map_err(|error| format!("cannot write the report: {error}"))?;
    }
    Ok(())
}

/// Every archived catalog of one instrument: the archive-root listing followed to completion,
/// each catalog downloaded and validated against its reported size and checksum.
fn catalogs(
    drive: &mut Drive,
    layout: &Layout,
    broker: &str,
    symbol: &str,
) -> Result<Vec<(String, String, Catalog)>, String> {
    let scratch = layout.state.join("downloads");
    let mut found = Vec::new();
    for file in drive.list(CATALOG_PREFIX)? {
        let identity = drive.listed_identity(&file)?;
        let size = identity.bytes;
        let sha256 = identity.sha256;
        let partial = scratch.join(format!("{}.catalog", file.id));
        drive.download(
            &file.id,
            &partial,
            &ObjectIdentity {
                bytes: size,
                sha256: sha256.clone(),
                crc32c: 0,
            },
        )?;
        let bytes = fs::read(&partial).map_err(|error| error.to_string())?;
        fs::remove_file(&partial).map_err(|error| error.to_string())?;
        let catalog =
            Catalog::from_json(&bytes).map_err(|error| format!("{}: {error}", file.id))?;
        if catalog.broker == broker && catalog.provider_symbol == symbol {
            found.push((file.id, sha256, catalog));
        }
    }
    Ok(found)
}

/// `data pipeline pull`: the consumer's one step. Select the newest archived catalog of one
/// instrument, preferring daily lineage and resolving equal coverage by ancestry. Restore any
/// missing pinned manifest, verify the full closure, and print the ready-manifest locations.
pub fn pull(
    config_path: &Path,
    broker: &str,
    symbol: &str,
    out: &mut dyn Write,
) -> Result<(), String> {
    let (config, layout, _) = load(config_path)?;
    let _lock = writer_lock(&layout)?;
    let _archive_lock = archive_lock(&config)?;
    let mut drive = Drive::open(&config.drive)?;
    let mut found = catalogs(&mut drive, &layout, broker, symbol)?;
    if found.is_empty() {
        return Err(format!("drive: no archived catalog for {broker}:{symbol}"));
    }
    let declaration = declaration(&config)?;
    let access = Access {
        declaration: declaration.as_ref(),
        certification: None,
    };
    let selected = crate::lineage::newest_catalog(
        &found,
        &mut drive,
        &layout.state.join("downloads"),
        access,
    )?;
    let (file_id, sha256, catalog) = found.swap_remove(selected);
    let local = layout.store();
    let entries: Vec<_> = [&catalog.dataset, &catalog.stream]
        .into_iter()
        .chain(&catalog.lineage_manifests)
        .collect();
    if entries
        .iter()
        .map(|entry| local.head(&entry.key).map(|m| m.is_some()))
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .all(|present| present)
    {
        for entry in entries {
            verify::run_with(&local.uri(&entry.key), access)?;
        }
        writeln!(
            out,
            "pulled {} {} dataset {} stream {} catalog {file_id} (already local)",
            catalog.instrument,
            catalog.role,
            local.uri(&catalog.dataset.key),
            local.uri(&catalog.stream.key)
        )
        .map_err(|error| format!("cannot write the report: {error}"))?;
        return Ok(());
    }
    restore_locked(&config, &layout, &file_id, &sha256, broker, symbol, out)
}

/// `data pipeline restore`: install exactly one catalog's dataset and stream closure into this
/// configuration's managed store, verify both, and print their local ready-manifest locations.
pub fn restore(
    config_path: &Path,
    catalog_id: &str,
    sha256: &str,
    broker: &str,
    symbol: &str,
    out: &mut dyn Write,
) -> Result<(), String> {
    let (config, layout, _) = load(config_path)?;
    let _lock = writer_lock(&layout)?;
    let _archive_lock = archive_lock(&config)?;
    restore_locked(&config, &layout, catalog_id, sha256, broker, symbol, out)
}

#[allow(clippy::too_many_arguments)]
fn restore_locked(
    config: &PipelineConfig,
    layout: &Layout,
    catalog_id: &str,
    sha256: &str,
    broker: &str,
    symbol: &str,
    out: &mut dyn Write,
) -> Result<(), String> {
    let declaration = declaration(config)?;
    let access = Access {
        declaration: declaration.as_ref(),
        certification: None,
    };
    let mut drive = Drive::open(&config.drive)?;
    let downloads = layout.state.join("downloads");
    let expected = sha256.to_ascii_lowercase();
    let remote = drive
        .metadata(catalog_id)?
        .filter(|remote| !remote.trashed)
        .ok_or_else(|| format!("drive: catalog {catalog_id} is missing or trashed"))?;
    let size = remote
        .size
        .ok_or_else(|| format!("drive: catalog {catalog_id} reports no size"))?;
    let partial = downloads.join(format!("{catalog_id}.catalog"));
    drive.download(
        catalog_id,
        &partial,
        &ObjectIdentity {
            bytes: size,
            sha256: expected.clone(),
            crc32c: 0,
        },
    )?;
    let bytes = fs::read(&partial).map_err(|error| error.to_string())?;
    fs::remove_file(&partial).map_err(|error| error.to_string())?;
    let catalog =
        Catalog::from_json(&bytes).map_err(|error| format!("catalog {catalog_id}: {error}"))?;
    if catalog.broker != broker || catalog.provider_symbol != symbol {
        return Err(format!(
            "catalog {catalog_id} describes {}, not {broker}:{symbol}",
            catalog.instrument
        ));
    }
    // The pinned catalog is the finite allowset: a declared target is permitted before its
    // manifest is read, and nothing outside the closure is ever fetched.
    access.permit(None, &catalog.dataset.generation)?;
    access.lookup(&catalog.stream.generation)?;
    let local = layout.store();
    let fetch_entry = |drive: &mut Drive, entry: &ManifestEntry| -> Result<Vec<u8>, String> {
        let partial = downloads.join(format!("{}.manifest", entry.generation));
        drive.download(
            &entry.file_id,
            &partial,
            &ObjectIdentity {
                bytes: entry.bytes,
                sha256: entry.sha256.clone(),
                crc32c: 0,
            },
        )?;
        let bytes = fs::read(&partial).map_err(|error| error.to_string())?;
        fs::remove_file(&partial).map_err(|error| error.to_string())?;
        Ok(bytes)
    };
    let dataset_bytes = fetch_entry(&mut drive, &catalog.dataset)?;
    let dataset = GenerationManifest::from_json(&dataset_bytes)
        .map_err(|error| format!("catalog {catalog_id} dataset manifest: {error}"))?;
    if dataset.key() != catalog.dataset.key
        || dataset.instrument != catalog.instrument
        || dataset.role != catalog.role
        || dataset.layout != catalog.layout
        || dataset.coverage != catalog.coverage
        || dataset.row_count != catalog.row_count
        || dataset.broker.to_string() != catalog.broker
        || dataset.provider_symbol.to_string() != catalog.provider_symbol
        || dataset.source_kind != catalog.source_kind
        || dataset.native_granularity != catalog.native_granularity
        || dataset.role == DatasetRole::Holdout
    {
        return Err(format!(
            "catalog {catalog_id}: the dataset manifest does not describe ordinary generation {}",
            catalog.dataset.generation
        ));
    }
    access.permit(Some(dataset.role), &dataset.generation)?;
    let stream_bytes = fetch_entry(&mut drive, &catalog.stream)?;
    let stream = StreamManifest::from_json(&stream_bytes)
        .map_err(|error| format!("catalog {catalog_id} stream manifest: {error}"))?;
    if stream.key() != catalog.stream.key
        || stream.source_generation != dataset.generation
        || stream.role != dataset.role
        || stream.layout != dataset.layout
        || stream.instrument != dataset.instrument
    {
        return Err(format!(
            "catalog {catalog_id}: the stream manifest does not derive from dataset {}",
            dataset.generation
        ));
    }
    let mut lineage = Vec::new();
    let mut lineage_objects = Vec::new();
    for entry in &catalog.lineage_manifests {
        access.lookup(&entry.generation)?;
        let bytes = fetch_entry(&mut drive, entry)?;
        let (key, instrument, role, objects) = if verify::manifest_kind(&bytes)?.is_none() {
            let m = GenerationManifest::from_json(&bytes)?;
            access.permit(Some(m.role), &m.generation)?;
            (m.key(), m.instrument, m.role, m.objects)
        } else {
            let m = StreamManifest::from_json(&bytes)?;
            access.permit(Some(m.role), &m.source_generation)?;
            (m.key(), m.instrument, m.role, m.objects)
        };
        if key != entry.key || role != catalog.role || instrument != catalog.instrument {
            return Err("catalog lineage manifest identity mismatch".into());
        }
        lineage_objects.extend(objects);
        lineage.push((key, bytes));
    }
    let allowed = |object: &ObjectRecord| {
        catalog.objects.iter().any(|entry| {
            entry.key == object.key && entry.sha256 == object.sha256 && entry.bytes == object.bytes
        })
    };
    for object in dataset
        .objects
        .iter()
        .chain(stream.objects.iter())
        .chain(&lineage_objects)
    {
        if !allowed(object) {
            return Err(format!(
                "catalog {catalog_id}: object {} lies outside the pinned catalog closure",
                object.key
            ));
        }
    }
    if let Some(extra) = catalog.objects.iter().find(|entry| {
        !dataset
            .objects
            .iter()
            .chain(stream.objects.iter())
            .chain(&lineage_objects)
            .any(|object| object.key == entry.key)
    }) {
        return Err(format!(
            "catalog {catalog_id}: object {} is named by neither pinned manifest",
            extra.key
        ));
    }
    let installed = run_pool(config, &catalog.objects, |entry, drive| {
        let identity = ObjectIdentity {
            bytes: entry.bytes,
            sha256: entry.sha256.clone(),
            crc32c: 0,
        };
        if local.head(&entry.key)?.is_some() {
            let path = local.local_path(&entry.key).expect("local store");
            let existing = store::identify(&path)?;
            if existing.bytes != entry.bytes || existing.sha256 != entry.sha256 {
                return Err(format!(
                    "{} already holds different content; nothing was replaced",
                    local.uri(&entry.key)
                ));
            }
            return Ok(false);
        }
        let partial = downloads.join(format!("{}.partial", entry.sha256));
        drive.download(&entry.file_id, &partial, &identity)?;
        let identity = store::identify(&partial)?;
        local.put_new(&entry.key, &partial, &identity)?;
        fs::remove_file(&partial).map_err(|error| error.to_string())?;
        Ok(true)
    })?
    .into_iter()
    .filter(|installed| *installed)
    .count();
    let reused = catalog.objects.len() - installed;
    // Manifests last, through the same create-once owner.
    for (key, bytes) in [
        (dataset.key(), &dataset_bytes),
        (stream.key(), &stream_bytes),
    ]
    .into_iter()
    .chain(lineage.iter().map(|(k, b)| (k.clone(), b)))
    {
        research::publish_record(&local, &local, &key, bytes)?;
    }
    let dataset_uri = layout.manifest_uri(&dataset.generation);
    let stream_uri = layout.manifest_uri(&stream.generation);
    verify::run_with(&dataset_uri, access)?;
    verify::run_with(&stream_uri, access)?;
    for (key, _) in &lineage {
        verify::run_with(&local.uri(key), access)?;
    }
    writeln!(
        out,
        "restored {} {} dataset {dataset_uri} stream {stream_uri} objects {} installed {installed} reused {reused}",
        dataset.instrument,
        dataset.role,
        catalog.objects.len()
    )
    .map_err(|error| format!("cannot write the report: {error}"))
}

/// The archive fence also excludes restores into another managed store on this host.
pub(crate) fn archive_lock(config: &PipelineConfig) -> Result<File, String> {
    use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
    let binding = format!(
        "{}\n{}",
        config
            .drive
            .loopback_endpoint
            .as_deref()
            .unwrap_or("https://www.googleapis.com"),
        config.drive.root_folder_id
    );
    let path = std::env::temp_dir().join(format!(
        "binary-alpha-archive-{}.lock",
        binary_alpha_engine::hex(&Sha256::digest(binding.as_bytes()))
    ));
    let file = match fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(&path)
    {
        Ok(file) => file,
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
            let metadata = fs::symlink_metadata(&path).map_err(|e| e.to_string())?;
            if !metadata.is_file() || metadata.permissions().mode() & 0o077 != 0 {
                return Err("retire: archive lock is not private regular storage".into());
            }
            fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(&path)
                .map_err(|e| e.to_string())?
        }
        Err(e) => return Err(e.to_string()),
    };
    file.try_lock().map_err(|_| {
        "pipeline: another operation holds this archive root (including a restore)".to_string()
    })?;
    Ok(file)
}

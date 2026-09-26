//! `binary-alpha features build`: resolve or apply one feature plan per configured instrument,
//! feed the input generation through the feature engine, fit encodings on development rows,
//! and publish the plan, rows, events, and encoded rows as one feature generation.
//!
//! The engine owns every formula, the plan, and the encoder; this module owns reading the
//! manifests and objects, temporary files, one-column-at-a-time encoding, publication through
//! the same store as every other generation, and the reconstruction proof.

use std::collections::HashMap;
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use binary_alpha_engine::config::{Config, FeatureInstrument, StreamKey};
use binary_alpha_engine::dataset::{DatasetRole, GenerationManifest, ObjectRecord, ObjectRole};
use binary_alpha_engine::execution::reusable_revision;
use binary_alpha_engine::features::{
    FEATURE_MANIFEST_KIND, FEATURE_SCHEMA_VERSION, FeatureEngine, FeatureManifest, FeatureOutput,
    FeaturePlan, FeatureStreamSummary, FitWindow, FittedEncoding, PLAN_OBJECT_PATH, Readiness,
    SequenceEvent, StreamPlan, StructureEvent, Value, feature_generation_id, profile_reference,
};
use binary_alpha_engine::market::format_event_time_micros;
use binary_alpha_engine::stream::{
    InstrumentProfile, PROFILE_OBJECT_PATH, STREAM_MANIFEST_KIND, Source, StreamManifest,
};

use crate::archive::{ColumnType, TABLE_ROW_GROUP_ROWS, TableColumn, TableReader, TableWriter};
use crate::audit::feed_generation;
use crate::import::{self, CODE_REVISION};
use crate::store::{self, ObjectIdentity, Put, Store};
use crate::verify;
use binary_alpha_engine::research::Access;

/// The schema message names of the four table objects of every stream.
pub const ROWS_MESSAGE: &str = "binary_alpha_feature_rows";
pub const STRUCTURE_MESSAGE: &str = "binary_alpha_structure_events";
pub const SEQUENCE_MESSAGE: &str = "binary_alpha_sequence_events";
pub const ENCODED_MESSAGE: &str = "binary_alpha_encoded_rows";

/// The fixed columns of the structure-event table.
pub fn structure_columns() -> Vec<TableColumn> {
    use ColumnType::{Int64, Text, Time};
    [
        ("event_id", Int64),
        ("event_type", Text),
        ("event_direction", Text),
        ("event_close_micros", Time),
        ("confirm_close_micros", Time),
        ("known_at_micros", Time),
        ("event_row", Int64),
        ("confirm_row", Int64),
        ("event_candle_ordinal", Int64),
        ("confirm_candle_ordinal", Int64),
        ("price_units", Int64),
        ("level_units", Int64),
        ("reference", Text),
        ("reference_close_micros", Time),
    ]
    .into_iter()
    .map(|(name, ty)| TableColumn::new(name, ty))
    .collect()
}

/// The fixed columns of the sequence-event table.
pub fn sequence_columns() -> Vec<TableColumn> {
    use ColumnType::{Int64, Text, Time};
    [
        ("event_id", Int64),
        ("row", Int64),
        ("candle_ordinal", Int64),
        ("decision_close_micros", Time),
        ("known_at_micros", Time),
        ("swing_event_type", Text),
        ("swing_type", Text),
        ("swing_price_units", Int64),
        ("swing_event_close_micros", Time),
        ("swing_confirm_close_micros", Time),
        ("previous_price_units", Int64),
        ("previous_event_close_micros", Time),
        ("previous_confirm_close_micros", Time),
        ("sequence_after", Text),
        ("bias_after", Text),
    ]
    .into_iter()
    .map(|(name, ty)| TableColumn::new(name, ty))
    .collect()
}

fn structure_values(event: &StructureEvent) -> Vec<Option<Value>> {
    let count = |value: u64| Some(Value::Int(value as i64));
    let text = |value: &'static str| Some(Value::Text(value.into()));
    vec![
        count(event.event_id),
        text(event.event_type),
        text(event.direction),
        Some(Value::Time(event.event_close_micros)),
        Some(Value::Time(event.confirm_close_micros)),
        Some(Value::Time(event.known_at_micros)),
        count(event.event_row),
        count(event.confirm_row),
        count(event.event_candle_ordinal),
        count(event.confirm_candle_ordinal),
        Some(Value::Int(event.price_units)),
        Some(Value::Int(event.level_units)),
        text(event.reference),
        event.reference_close_micros.map(Value::Time),
    ]
}

fn sequence_values(event: &SequenceEvent) -> Vec<Option<Value>> {
    let count = |value: u64| Some(Value::Int(value as i64));
    let text = |value: &'static str| Some(Value::Text(value.into()));
    vec![
        count(event.event_id),
        count(event.row),
        count(event.candle_ordinal),
        Some(Value::Time(event.decision_close_micros)),
        Some(Value::Time(event.known_at_micros)),
        text(event.swing_event_type),
        text(event.swing_type),
        Some(Value::Int(event.swing_price_units)),
        Some(Value::Time(event.swing_event_close_micros)),
        Some(Value::Time(event.swing_confirm_close_micros)),
        event.previous_price_units.map(Value::Int),
        event.previous_event_close_micros.map(Value::Time),
        event.previous_confirm_close_micros.map(Value::Time),
        text(event.sequence_after),
        text(event.bias_after),
    ]
}

/// The footer metadata of every table of one stream of one plan.
pub(crate) fn table_metadata(
    plan: &FeaturePlan,
    stream: &StreamPlan,
    identity: (&'static str, &str),
) -> Vec<(&'static str, String)> {
    vec![
        ("broker", plan.broker.to_string()),
        ("provider_symbol", plan.provider_symbol.to_string()),
        ("price_scale", plan.price_scale.digits().to_string()),
        ("duration_seconds", stream.duration_seconds.to_string()),
        ("offset_seconds", stream.offset_seconds.to_string()),
        (identity.0, identity.1.to_string()),
        ("feature_schema_version", FEATURE_SCHEMA_VERSION.to_string()),
    ]
}

/// Runs every configured feature build, writing one report line per instrument to `out`.
pub fn run(config_path: &Path, out: &mut dyn Write) -> Result<(), String> {
    let config = crate::load_config(config_path)?;
    let entries = config
        .features
        .as_ref()
        .filter(|features| !features.instruments.is_empty())
        .ok_or("features.instruments: at least one entry is required")?;
    let base = config_path.parent().unwrap_or(Path::new("."));
    let historical_dir = base.join(config.storage.historical_data_dir.as_path());
    fs::create_dir_all(&historical_dir)
        .map_err(|error| format!("cannot create {}: {error}", historical_dir.display()))?;
    let local = Store::filesystem(&historical_dir);
    let destination = Store::open(&config.storage.publication_uri)?;
    let declaration = crate::research::declaration(&config)?;
    let access = Access {
        declaration: declaration.as_ref(),
        certification: None,
        verified: None,
    };
    // Every entry resolves, and every resolved instrument and stream has one owner in one role,
    // before anything is streamed or published.
    let mut resolved = Vec::with_capacity(entries.instruments.len());
    let mut owners: HashMap<String, usize> = HashMap::new();
    for (index, entry) in entries.instruments.iter().enumerate() {
        let item = resolve(entry, access)
            .map_err(|reason| format!("features.instruments[{index}]: {reason}"))?;
        for stream in &item.plan.streams {
            let owned = format!(
                "{} {} {}s/{}s",
                item.plan.instrument,
                item.bound.input.role,
                stream.duration_seconds,
                stream.offset_seconds
            );
            if let Some(owner) = owners.insert(owned.clone(), index) {
                return Err(format!(
                    "features.instruments[{index}]: {owned} is already owned by features.instruments[{owner}]"
                ));
            }
        }
        resolved.push(item);
    }
    for (index, item) in resolved.into_iter().enumerate() {
        let built = build(item, &config, &local, &destination, access)
            .map_err(|reason| format!("features.instruments[{index}]: {reason}"))?;
        writeln!(out, "{}", built.report)
            .and_then(|()| out.flush())
            .map_err(|error| format!("cannot write the report: {error}"))?;
    }
    Ok(())
}

/// The bound inputs of one build, resolved from permitted manifest metadata before any child
/// object is read.
struct Bound {
    input: GenerationManifest,
    input_store: Store,
    stream_manifest: StreamManifest,
    profile: InstrumentProfile,
    profile_sha256: String,
}

/// Reads and checks the input and profile manifests. The input target needs a read permit
/// before it is opened; role mismatches are refused on the manifest bytes alone; the profile
/// object is the first child read.
fn bind(entry: &FeatureInstrument, access: Access<'_>) -> Result<Bound, String> {
    access
        .permit(Some(entry.role), entry.input_manifest.generation())
        .map_err(|reason| format!("input_manifest: {reason}"))?;
    let (input_store, input_key) = verify::open(&entry.input_manifest.to_string())?;
    let mut bytes = Vec::new();
    input_store.read_to(&input_key, None, &mut bytes)?;
    if let Some(kind) = verify::manifest_kind(&bytes)? {
        return Err(format!(
            "input_manifest: {} is a `{kind}` manifest, not a dataset ready manifest",
            entry.input_manifest
        ));
    }
    let input = GenerationManifest::from_json(&bytes)
        .map_err(|error| format!("input_manifest: {}: {error}", entry.input_manifest))?;
    if input.key() != input_key {
        return Err(format!(
            "input_manifest: {} holds the manifest of generation {}",
            entry.input_manifest, input.generation
        ));
    }
    if input.role == DatasetRole::Holdout {
        access
            .protected(std::iter::once(input.generation.as_str()))
            .map_err(|reason| {
                format!("input_manifest: holdout data never enters a feature build; {reason}")
            })?;
    }
    if input.role != entry.role {
        return Err(format!(
            "role: declared `{}`, but generation {} is `{}`",
            entry.role, input.generation, input.role
        ));
    }
    let (profile_store, profile_key) = verify::open(&entry.profile_manifest.to_string())?;
    let mut bytes = Vec::new();
    profile_store.read_to(&profile_key, None, &mut bytes)?;
    if verify::manifest_kind(&bytes)?.as_deref() != Some(STREAM_MANIFEST_KIND) {
        return Err(format!(
            "profile_manifest: {} is not an instrument stream manifest",
            entry.profile_manifest
        ));
    }
    let stream_manifest = StreamManifest::from_json(&bytes)
        .map_err(|error| format!("profile_manifest: {}: {error}", entry.profile_manifest))?;
    if stream_manifest.key() != profile_key {
        return Err(format!(
            "profile_manifest: {} holds the manifest of generation {}",
            entry.profile_manifest, stream_manifest.generation
        ));
    }
    if stream_manifest.role != DatasetRole::Development {
        return Err(format!(
            "profile_manifest: the profile reference is development-only, but generation {} is `{}`",
            stream_manifest.generation, stream_manifest.role
        ));
    }
    // The profile's source is protected by its declared role whatever the profile's own label
    // says: refused before the profile object is opened.
    access
        .lookup(&stream_manifest.source_generation)
        .map_err(|reason| format!("profile_manifest: {reason}"))?;
    let definition = &stream_manifest.definition;
    if definition.broker != input.broker
        || definition.provider_symbol != input.provider_symbol
        || definition.native_granularity != input.native_granularity
    {
        return Err(format!(
            "profile_manifest: the profile describes {} at {} granularity, but the input is {} at {} granularity",
            stream_manifest.instrument,
            definition.native_granularity,
            input.instrument,
            input.native_granularity
        ));
    }
    let object = stream_manifest
        .objects
        .iter()
        .find(|object| object.path == PROFILE_OBJECT_PATH)
        .ok_or("profile_manifest: no profile object")?;
    let (_, fetched) = verify::fetch(&profile_store, object, true)?;
    let profile = fs::read(&fetched.expect("decoded objects have a local path").path)
        .map_err(|error| format!("cannot read the profile: {error}"))
        .and_then(|bytes| InstrumentProfile::from_json(&bytes))
        .map_err(|reason| format!("profile_manifest: {reason}"))?;
    if profile.source.generation != stream_manifest.source_generation {
        return Err(
            "profile_manifest: the profile does not describe the manifest's source".to_string(),
        );
    }
    let profile_sha256 = object.sha256.clone();
    Ok(Bound {
        input,
        input_store,
        stream_manifest,
        profile,
        profile_sha256,
    })
}

/// Reads the ready manifest of a completed feature generation, naming `field`, the
/// configuration field that referenced it, in every refusal. A generation of holdout data
/// resolves only within the certification context naming its input generation; nothing else
/// of it is read first.
pub(crate) fn feature_manifest(
    field: &str,
    uri: &str,
    access: Access<'_>,
) -> Result<(Store, FeatureManifest), String> {
    let (store, key) = verify::open(uri)?;
    let mut bytes = Vec::new();
    store.read_to(&key, None, &mut bytes)?;
    if verify::manifest_kind(&bytes)?.as_deref() != Some(FEATURE_MANIFEST_KIND) {
        return Err(format!(
            "{field}: {uri} is not a feature generation manifest"
        ));
    }
    let manifest =
        FeatureManifest::from_json(&bytes).map_err(|error| format!("{field}: {uri}: {error}"))?;
    if manifest.key() != key {
        return Err(format!(
            "{field}: {uri} holds the manifest of generation {}",
            manifest.generation
        ));
    }
    if manifest.role == DatasetRole::Holdout
        || access.lookup(&manifest.input_generation)? == Some(DatasetRole::Holdout)
    {
        access
            .protected(std::iter::once(manifest.input_generation.as_str()))
            .map_err(|reason| format!("{field}: {uri}: {reason}"))?;
    }
    Ok((store, manifest))
}

/// Reads the plan a feature manifest names and checks that it is fitted and carries the
/// manifest's plan identity and streams.
pub(crate) fn fitted_plan(
    field: &str,
    store: &Store,
    manifest: &FeatureManifest,
) -> Result<FeaturePlan, String> {
    let object = manifest
        .objects
        .iter()
        .find(|object| object.path == PLAN_OBJECT_PATH)
        .expect("a validated manifest lists its plan");
    let (_, fetched) = verify::fetch(store, object, true)?;
    let plan = fs::read(&fetched.expect("decoded objects have a local path").path)
        .map_err(|error| format!("cannot read the plan: {error}"))
        .and_then(|bytes| FeaturePlan::from_json(&bytes))
        .map_err(|reason| format!("{field}: {reason}"))?;
    plan_describes(&plan, manifest).map_err(|reason| format!("{field}: {reason}"))?;
    Ok(plan)
}

/// The consistency every consumer relies on between a plan and the manifest that names it: the
/// plan identity, instrument, profile generation, fit, and the same streams in order.
pub(crate) fn plan_describes(plan: &FeaturePlan, manifest: &FeatureManifest) -> Result<(), String> {
    if plan.identity() != manifest.plan_identity
        || plan.instrument != manifest.instrument
        || plan.broker != manifest.broker
        || plan.provider_symbol != manifest.provider_symbol
        || plan.profile.stream_generation != manifest.profile_generation
        || !plan.is_fitted()
        || plan.streams.len() != manifest.streams.len()
        || plan
            .streams
            .iter()
            .zip(&manifest.streams)
            .any(|(stream, summary)| {
                stream.key()
                    != StreamKey {
                        duration_seconds: summary.duration_seconds,
                        offset_seconds: summary.offset_seconds,
                    }
            })
    {
        return Err(
            "the plan does not describe the manifest's plan identity, instrument, profile, fit, and streams"
                .to_string(),
        );
    }
    Ok(())
}

/// One stream's temporary tables and running summary while the input streams through.
struct StreamOutput {
    rows: TableWriter,
    structure: TableWriter,
    sequence: TableWriter,
    summary: FeatureStreamSummary,
    first_decision: Option<i64>,
    last_decision: Option<i64>,
}

/// One entry's bound inputs and its resolved or frozen plan.
pub(crate) struct Resolved {
    bound: Bound,
    plan: FeaturePlan,
    frozen_from: Option<String>,
}

impl Resolved {
    /// The bound input generation.
    pub(crate) fn input(&self) -> &GenerationManifest {
        &self.bound.input
    }

    /// The resolved plan before any fit: its profile, input, settings, definitions, label
    /// limit, and compiled outputs and encodings.
    pub(crate) fn plan(&self) -> &FeaturePlan {
        &self.plan
    }
}

/// One published feature generation: its committed ready manifest, the plan it applies, and
/// the report and reconstruction lines of the command.
pub(crate) struct Built {
    pub(crate) manifest: FeatureManifest,
    pub(crate) plan: FeaturePlan,
    pub(crate) report: String,
}

#[derive(Serialize)]
struct FitRequest<'a> {
    code_revision: &'a str,
    unfitted_plan_identity: &'a str,
    input_generation: &'a str,
    profile_generation: &'a str,
    role: DatasetRole,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct FitReceipt {
    request_digest: String,
    code_revision: String,
    generation: String,
}

fn fit_request_digest(request: &FitRequest<'_>) -> String {
    let bytes = serde_json::to_vec(request).expect("a fit request serializes");
    binary_alpha_engine::hex(&Sha256::digest(bytes))
}

fn fit_receipt_key(digest: &str) -> String {
    format!("features/fits/{digest}")
}

fn write_fit_receipt(
    local: &Store,
    destination: &Store,
    key: &str,
    receipt: &FitReceipt,
) -> Result<(), String> {
    let temporary = import::temporary_path(local, "feature-fit-receipt")?;
    let mut bytes = serde_json::to_vec_pretty(receipt).expect("a fit receipt serializes");
    bytes.push(b'\n');
    fs::write(&temporary, bytes)
        .map_err(|error| format!("cannot write {}: {error}", temporary.display()))?;
    let identity = store::identify(&temporary)?;
    let result = destination.put_new(key, &temporary, &identity);
    fs::remove_file(&temporary)
        .map_err(|error| format!("cannot remove {}: {error}", temporary.display()))?;
    result.map(|_| ())
}

fn immutable_conflict(reason: impl std::fmt::Display) -> String {
    format!("immutable feature generation conflict: {reason}")
}

/// Reads only a ready manifest, after the target generation's declaration check. The
/// manifest's own role and input are checked before any child object can be opened.
fn ready_feature(
    store: &Store,
    generation: &str,
    access: Access<'_>,
) -> Result<(Vec<u8>, FeatureManifest), String> {
    access.lookup(generation)?;
    let key = binary_alpha_engine::dataset::manifest_key(generation);
    let mut bytes = Vec::new();
    store.read_to(&key, None, &mut bytes)?;
    let manifest = FeatureManifest::from_json(&bytes)
        .map_err(|reason| format!("{}: {reason}", store.uri(&key)))?;
    if manifest.key() != key {
        return Err(format!("{} records another generation", store.uri(&key)));
    }
    if manifest.role == DatasetRole::Holdout
        || access.lookup(&manifest.input_generation)? == Some(DatasetRole::Holdout)
    {
        access.protected(std::iter::once(manifest.input_generation.as_str()))?;
    }
    Ok((bytes, manifest))
}

fn reused_feature(manifest: FeatureManifest, plan: FeaturePlan, verified: String) -> Built {
    let rows: u64 = manifest.streams.iter().map(|stream| stream.rows).sum();
    let events: u64 = manifest
        .streams
        .iter()
        .map(|stream| stream.structure_events + stream.sequence_events)
        .sum();
    let count = manifest.objects.len();
    let report = format!(
        "features {} {} generation {} plan {} input {} observations {} rows {rows} events {events} objects {count} reused {count} (already published)\n{verified}",
        manifest.instrument,
        manifest.role,
        manifest.generation,
        manifest.plan_identity,
        manifest.input_generation,
        manifest.observations,
    );
    Built {
        manifest,
        plan,
        report,
    }
}

fn verify_frozen_before_revision(
    destination: &Store,
    key: &str,
    bytes: &[u8],
    manifest: &FeatureManifest,
    revision: &str,
) -> Result<Option<String>, String> {
    let verified = verify_feature(&destination.uri(key), destination, key, bytes)
        .map_err(immutable_conflict)?;
    Ok((manifest.code_revision == revision).then_some(verified))
}

struct FrozenRequest<'a> {
    generation: &'a str,
    role: DatasetRole,
    input_generation: &'a str,
    profile_generation: &'a str,
    frozen_from: &'a str,
    plan_identity: &'a str,
    code_revision: &'a str,
}

fn check_frozen_ready(
    destination: &Store,
    request: FrozenRequest<'_>,
    access: Access<'_>,
) -> Result<Option<(FeatureManifest, String)>, String> {
    let key = binary_alpha_engine::dataset::manifest_key(request.generation);
    if destination.head(&key)?.is_none() {
        return Ok(None);
    }
    let (bytes, manifest) =
        ready_feature(destination, request.generation, access).map_err(immutable_conflict)?;
    if manifest.role != request.role
        || manifest.input_generation != request.input_generation
        || manifest.profile_generation != request.profile_generation
        || manifest.frozen_from.as_deref() != Some(request.frozen_from)
        || manifest.plan_identity != request.plan_identity
    {
        return Err(immutable_conflict(format!(
            "{} does not match the frozen application request",
            destination.uri(&key)
        )));
    }
    let verified =
        verify_frozen_before_revision(destination, &key, &bytes, &manifest, request.code_revision)?;
    Ok(verified
        .filter(|_| reusable_revision(request.code_revision))
        .map(|verified| (manifest, verified)))
}

/// One cap is shared by outer stream work and its nested column work. Reserve at most a
/// quarter of currently available memory for simultaneous tasks. The fallback is conservative
/// on systems without Linux MemAvailable.
struct FeatureParallelism {
    limit: usize,
    active: AtomicUsize,
}

impl FeatureParallelism {
    fn for_fitting(max_fit_rows: u64, live_fit_columns: usize) -> Self {
        // A fit holds whole decoded input and readiness columns plus fitting scratch.
        Self::new(Self::fit_bytes(max_fit_rows, live_fit_columns))
    }

    fn for_encoding(max_columns: usize) -> Self {
        // A stream holds one decoded row-group column per worker, all completed i16
        // columns, and the writer's row-group values and flush buffers.
        Self::new(Self::encoding_bytes(max_columns))
    }

    fn fit_bytes(max_fit_rows: u64, live_fit_columns: usize) -> u64 {
        max_fit_rows
            .saturating_mul(128)
            .saturating_mul(live_fit_columns as u64)
    }

    fn encoding_bytes(max_columns: usize) -> u64 {
        (TABLE_ROW_GROUP_ROWS as u64)
            .saturating_mul(256)
            .saturating_mul(max_columns as u64)
    }

    fn memory_limit(available: u64, cores: usize, bytes_per_task: u64) -> usize {
        let memory = (available / 4 / bytes_per_task.max(1)).max(1);
        cores
            .min(usize::try_from(memory).unwrap_or(usize::MAX))
            .max(1)
    }

    fn new(bytes_per_task: u64) -> Self {
        let cores = std::thread::available_parallelism().map_or(1, |count| count.get());
        let available = fs::read_to_string("/proc/meminfo")
            .ok()
            .and_then(|contents| {
                contents.lines().find_map(|line| {
                    line.strip_prefix("MemAvailable:")?
                        .split_whitespace()
                        .next()?
                        .parse::<u64>()
                        .ok()
                        .and_then(|kib| kib.checked_mul(1024))
                })
            })
            .unwrap_or(1 << 30);
        Self {
            limit: Self::memory_limit(available, cores, bytes_per_task),
            active: AtomicUsize::new(1),
        }
    }

    fn try_acquire(&self) -> bool {
        self.active
            .fetch_update(Ordering::AcqRel, Ordering::Relaxed, |count| {
                (count < self.limit).then_some(count + 1)
            })
            .is_ok()
    }

    fn map<T: Sync, R: Send>(&self, items: &[T], worker: impl Fn(&T) -> R + Sync) -> Vec<R> {
        self.map_limited(items, self.limit, worker)
    }

    fn map_streams<T: Sync, R: Send>(
        &self,
        items: &[T],
        worker: impl Fn(&T) -> R + Sync,
    ) -> Vec<R> {
        // Leave slots for the row group's independent columns while streams overlap.
        let streams = self.limit.min(self.limit.div_ceil(2).max(2));
        self.map_limited(items, streams, worker)
    }

    fn map_limited<T: Sync, R: Send>(
        &self,
        items: &[T],
        tasks: usize,
        worker: impl Fn(&T) -> R + Sync,
    ) -> Vec<R> {
        let next = AtomicUsize::new(0);
        let results: Mutex<Vec<Option<R>>> = Mutex::new((0..items.len()).map(|_| None).collect());
        std::thread::scope(|scope| {
            for _ in 1..items.len().min(tasks) {
                if !self.try_acquire() {
                    break;
                }
                let worker = &worker;
                let next = &next;
                let results = &results;
                scope.spawn(move || {
                    loop {
                        let index = next.fetch_add(1, Ordering::Relaxed);
                        let Some(item) = items.get(index) else { break };
                        let value = worker(item);
                        results.lock().expect("worker did not panic")[index] = Some(value);
                    }
                    self.active.fetch_sub(1, Ordering::Release);
                });
            }
            loop {
                let index = next.fetch_add(1, Ordering::Relaxed);
                if index >= items.len() {
                    break;
                }
                let value = worker(&items[index]);
                results.lock().expect("worker did not panic")[index] = Some(value);
            }
        });
        results
            .into_inner()
            .expect("worker did not panic")
            .into_iter()
            .map(|value| value.expect("every item was processed"))
            .collect()
    }
}

struct FitJob {
    stream: usize,
    column: usize,
    rows_path: PathBuf,
    encoding: FittedEncoding,
    readiness: Readiness,
}

fn fit_column(job: &FitJob, max_labels: u32) -> Result<FittedEncoding, String> {
    let reader = TableReader::open(&job.rows_path, ROWS_MESSAGE)?;
    let index = reader.column_index(&job.encoding.input).ok_or_else(|| {
        format!(
            "encoding input `{}` is not a row column",
            job.encoding.input
        )
    })?;
    let mut column = reader.whole_column(index)?;
    if job.encoding.automatic {
        let flags: Vec<_> = job
            .readiness
            .flags
            .iter()
            .map(|flag| {
                let index = reader
                    .column_index(flag)
                    .ok_or_else(|| format!("readiness flag `{flag}` is not a row column"))?;
                reader.whole_column(index)
            })
            .collect::<Result<_, String>>()?;
        for row in 0..column.len() {
            if !binary_alpha_engine::execution::value_ready(
                column[row].as_ref(),
                &job.readiness.unready,
                flags
                    .iter()
                    .map(|flag: &Vec<Option<Value>>| flag[row] == Some(Value::Bool(true))),
            ) {
                column[row] = None;
            }
        }
    }
    let mut encoding = job.encoding.clone();
    encoding.fit(&column, max_labels)?;
    Ok(encoding)
}

fn encode_stream(
    stream: &StreamPlan,
    rows_path: &Path,
    path: &Path,
    plan: &FeaturePlan,
    plan_identity: &str,
    parallelism: &FeatureParallelism,
) -> Result<(), String> {
    let columns = stream
        .encodings
        .iter()
        .map(|encoding| TableColumn::new(&encoding.output, ColumnType::Int16))
        .collect();
    let mut writer = TableWriter::create(
        path,
        ENCODED_MESSAGE,
        columns,
        &table_metadata(plan, stream, ("plan_identity", plan_identity)),
    )?;
    // Parquet row-group readers share seek state. A bounded pool gives concurrent columns
    // independent file descriptors without reopening them for every row group.
    let readers: Vec<Mutex<TableReader>> = (0..stream.encodings.len().min(parallelism.limit))
        .map(|_| TableReader::open(rows_path, ROWS_MESSAGE))
        .map(|reader| reader.map(Mutex::new))
        .collect::<Result<_, _>>()?;
    let columns: Vec<_> = stream.encodings.iter().enumerate().collect();
    let groups = readers[0]
        .lock()
        .expect("reader did not panic")
        .row_groups();
    for group in 0..groups {
        let codes = parallelism.map(&columns, |(column, encoding)| {
            let values = {
                let reader = readers[*column % readers.len()]
                    .lock()
                    .expect("reader did not panic");
                let index = reader.column_index(&encoding.input).ok_or_else(|| {
                    format!("encoding input `{}` is not a row column", encoding.input)
                })?;
                reader.column(group, index)?
            };
            Ok::<_, String>(encoding.encode(&values))
        });
        let codes: Vec<Vec<i16>> = codes.into_iter().collect::<Result<_, _>>()?;
        let rows = codes.first().map_or(0, Vec::len);
        for row in 0..rows {
            writer.push(
                codes
                    .iter()
                    .map(|column| Some(Value::Int(i64::from(column[row]))))
                    .collect(),
            )?;
        }
    }
    writer.finish()?;
    Ok(())
}

/// Binds one entry's inputs and resolves its new plan or reads its frozen one; nothing is
/// streamed or published.
pub(crate) fn resolve(entry: &FeatureInstrument, access: Access<'_>) -> Result<Resolved, String> {
    let bound = bind(entry, access)?;
    let reference = profile_reference(
        &bound.stream_manifest,
        &bound.profile,
        &bound.profile_sha256,
    );
    let (plan, frozen_from) = match &entry.frozen_plan {
        None => {
            if bound.stream_manifest.source_generation != bound.input.generation {
                return Err(format!(
                    "input_manifest: a new plan fits on the generation its profile was built from ({}), not {}",
                    bound.stream_manifest.source_generation, bound.input.generation
                ));
            }
            (
                FeaturePlan::resolve(entry, reference, &bound.input.generation)?,
                None,
            )
        }
        Some(uri) => {
            let (store, manifest) = feature_manifest("frozen_plan", &uri.to_string(), access)?;
            let plan = fitted_plan("frozen_plan", &store, &manifest)?;
            if plan.profile != reference {
                return Err(
                    "frozen_plan: the plan was frozen under another profile reference".to_string(),
                );
            }
            (plan, Some(manifest.generation))
        }
    };
    Ok(Resolved {
        bound,
        plan,
        frozen_from,
    })
}

/// Streams, fits or applies, encodes, publishes, and reconstructs one resolved entry as a
/// feature generation; a completed identical generation is reused.
pub(crate) fn build(
    resolved: Resolved,
    config: &Config,
    local: &Store,
    destination: &Store,
    access: Access<'_>,
) -> Result<Built, String> {
    let Resolved {
        bound,
        mut plan,
        frozen_from,
    } = resolved;
    let id = bound.stream_manifest.definition.id();
    let generation_of =
        |plan: &FeaturePlan| feature_generation_id(&plan.identity(), &bound.input.generation);

    // Binding has already permitted the input and frozen plan. The fast path checks the same
    // target context before touching its receipt or any feature child object.
    access.permit(Some(bound.input.role), &bound.input.generation)?;
    let unfitted_identity = (frozen_from.is_none()).then(|| plan.unfitted().identity());
    let request = unfitted_identity
        .as_ref()
        .filter(|_| reusable_revision(CODE_REVISION))
        .map(|identity| FitRequest {
            code_revision: CODE_REVISION,
            unfitted_plan_identity: identity,
            input_generation: &bound.input.generation,
            profile_generation: &bound.stream_manifest.generation,
            role: bound.input.role,
        });
    let receipt_key = request
        .as_ref()
        .map(|request| fit_receipt_key(&fit_request_digest(request)));
    if let Some(frozen_from) = &frozen_from {
        let generation = generation_of(&plan);
        if let Some((manifest, verified)) = check_frozen_ready(
            destination,
            FrozenRequest {
                generation: &generation,
                role: bound.input.role,
                input_generation: &bound.input.generation,
                profile_generation: &bound.stream_manifest.generation,
                frozen_from,
                plan_identity: &plan.identity(),
                code_revision: CODE_REVISION,
            },
            access,
        )? {
            return Ok(reused_feature(manifest, plan, verified));
        }
    } else if let (Some(request), Some(receipt_key)) = (&request, &receipt_key)
        && destination.head(receipt_key)?.is_some()
    {
        let mut bytes = Vec::new();
        destination.read_to(receipt_key, None, &mut bytes)?;
        let receipt: FitReceipt = serde_json::from_slice(&bytes).map_err(|reason| {
            immutable_conflict(format!("{}: {reason}", destination.uri(receipt_key)))
        })?;
        if receipt.request_digest != fit_request_digest(request)
            || receipt.code_revision != CODE_REVISION
            || receipt.generation.len() != 64
            || !receipt
                .generation
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit())
        {
            return Err(immutable_conflict(format!(
                "{} does not match the fit request",
                destination.uri(receipt_key)
            )));
        }
        let key = binary_alpha_engine::dataset::manifest_key(&receipt.generation);
        let (manifest_bytes, manifest) =
            ready_feature(destination, &receipt.generation, access).map_err(immutable_conflict)?;
        if manifest.role != request.role
            || manifest.input_generation != request.input_generation
            || manifest.profile_generation != request.profile_generation
            || manifest.code_revision != request.code_revision
            || manifest.frozen_from.is_some()
        {
            return Err(immutable_conflict(format!(
                "{} does not match the fit request",
                destination.uri(&key)
            )));
        }
        let verified = verify_feature(&destination.uri(&key), destination, &key, &manifest_bytes)
            .map_err(immutable_conflict)?;
        let fitted = fitted_plan(&destination.uri(&key), destination, &manifest)
            .map_err(immutable_conflict)?;
        if fitted.unfitted().identity() != request.unfitted_plan_identity {
            return Err(immutable_conflict(format!(
                "{} has a different unfitted plan",
                destination.uri(&key)
            )));
        }
        return Ok(reused_feature(manifest, fitted, verified));
    }

    // Stream the input through the engine into temporary tables.
    let streaming = Instant::now();
    let mut engine = FeatureEngine::new(&plan, Source::from_manifest(&bound.input))?;
    let raw = plan.raw_identity.clone();
    let mut temporaries = Vec::new();
    let mut outputs: Vec<StreamOutput> = Vec::with_capacity(plan.streams.len());
    for stream in &plan.streams {
        let stem = format!(
            "features-{raw}-{}s-{}s",
            stream.duration_seconds, stream.offset_seconds
        );
        let metadata = table_metadata(&plan, stream, ("raw_identity", &raw));
        let mut open = |suffix: &str,
                        message: &str,
                        columns: Vec<TableColumn>|
         -> Result<TableWriter, String> {
            let path = import::temporary_path(local, &format!("{stem}-{suffix}"))?;
            let writer = TableWriter::create(&path, message, columns, &metadata)?;
            temporaries.push(path);
            Ok(writer)
        };
        outputs.push(StreamOutput {
            rows: open(
                "rows",
                ROWS_MESSAGE,
                stream.outputs.iter().map(TableColumn::of_output).collect(),
            )?,
            structure: open("structure", STRUCTURE_MESSAGE, structure_columns())?,
            sequence: open("sequence", SEQUENCE_MESSAGE, sequence_columns())?,
            summary: FeatureStreamSummary {
                duration_seconds: stream.duration_seconds,
                offset_seconds: stream.offset_seconds,
                rows: 0,
                structure_events: 0,
                sequence_events: 0,
                first_decision_time: None,
                last_decision_time: None,
            },
            first_decision: None,
            last_decision: None,
        });
    }
    let mut produced = FeatureOutput::default();
    let mut push = |observation| -> Result<(), String> {
        engine
            .push(observation, &mut produced)
            .map_err(|rejection| format!("{id}: {rejection}"))?;
        for (index, row) in produced.rows.drain(..) {
            let output = &mut outputs[index];
            output.first_decision.get_or_insert(row.close_time_micros);
            output.last_decision = Some(row.close_time_micros);
            output.summary.rows += 1;
            output.rows.push(row.values)?;
        }
        for (index, event) in produced.structure_events.drain(..) {
            outputs[index].summary.structure_events += 1;
            outputs[index].structure.push(structure_values(&event))?;
        }
        for (index, event) in produced.sequence_events.drain(..) {
            outputs[index].summary.sequence_events += 1;
            outputs[index].sequence.push(sequence_values(&event))?;
        }
        Ok(())
    };
    feed_generation(
        &bound.input_store,
        &bound.input,
        plan.price_scale,
        &mut push,
    )?;
    let profile = engine.profile();
    if profile.observations != bound.input.row_count
        || profile.coverage.as_ref() != Some(&bound.input.coverage)
    {
        return Err(format!(
            "input_manifest: observed {} records from {:?}, but the manifest records {} rows from {} to {}; nothing was published",
            profile.observations,
            profile.coverage,
            bound.input.row_count,
            bound.input.coverage.first_event_time,
            bound.input.coverage.last_event_time
        ));
    }
    if frozen_from.is_none() && profile != bound.profile {
        return Err("profile_manifest: the published profile is not the profile of this input under the recorded definition".to_string());
    }
    let mut summaries = Vec::with_capacity(outputs.len());
    for output in outputs {
        let StreamOutput {
            rows,
            structure,
            sequence,
            mut summary,
            first_decision,
            last_decision,
        } = output;
        let written = rows.finish()?;
        assert_eq!(written, summary.rows, "every emitted row is written");
        structure.finish()?;
        sequence.finish()?;
        summary.first_decision_time = first_decision.map(format_event_time_micros);
        summary.last_decision_time = last_decision.map(format_event_time_micros);
        summaries.push(summary);
    }
    let streamed = streaming.elapsed();
    let max_flags = plan
        .streams
        .iter()
        .flat_map(|stream| stream.encodings.iter())
        .map(|encoding| plan.readiness_of(&encoding.input).flags.len())
        .max()
        .unwrap_or(0);
    // One fit keeps the input, its readiness flags, and fitting scratch live together.
    let parallelism = FeatureParallelism::for_fitting(
        summaries
            .iter()
            .map(|stream| stream.rows)
            .max()
            .unwrap_or(0),
        max_flags + 2,
    );

    // Each fit owns its decoded column and returns an encoding to the original plan position.
    let fitting = Instant::now();
    if frozen_from.is_none() {
        plan.fit_windows = summaries
            .iter()
            .map(|summary| FitWindow {
                duration_seconds: summary.duration_seconds,
                offset_seconds: summary.offset_seconds,
                rows: summary.rows,
                first_decision_time: summary.first_decision_time.clone(),
                last_decision_time: summary.last_decision_time.clone(),
            })
            .collect();
        let mut jobs = Vec::new();
        for (stream_index, stream) in plan.streams.iter().enumerate() {
            for (column, encoding) in stream.encodings.iter().enumerate() {
                jobs.push(FitJob {
                    stream: stream_index,
                    column,
                    rows_path: temporaries[stream_index * 3].clone(),
                    encoding: encoding.clone(),
                    readiness: plan.readiness_of(&encoding.input),
                });
            }
        }
        for (job, result) in jobs
            .iter()
            .zip(parallelism.map(&jobs, |job| fit_column(job, plan.max_labels)))
        {
            plan.streams[job.stream].encodings[job.column] = result?;
        }
    }
    let plan_identity = plan.identity();
    let generation = generation_of(&plan);
    let key = binary_alpha_engine::dataset::manifest_key(&generation);
    let fitted = fitting.elapsed();

    // Each stream writes its own table. Within a row group, columns are decoded and encoded
    // independently, then returned in plan order to the serial row and metadata writer.
    let encoding = Instant::now();
    let encoded_paths: Vec<Option<PathBuf>> = plan
        .streams
        .iter()
        .map(|stream| {
            (!stream.encodings.is_empty())
                .then(|| {
                    import::temporary_path(
                        local,
                        &format!(
                            "features-{raw}-{}s-{}s-encoded",
                            stream.duration_seconds, stream.offset_seconds
                        ),
                    )
                })
                .transpose()
        })
        .collect::<Result<_, String>>()?;
    let stream_jobs: Vec<_> = plan
        .streams
        .iter()
        .enumerate()
        .filter(|(_, stream)| !stream.encodings.is_empty())
        .map(|(index, stream)| {
            (
                stream,
                temporaries[index * 3].as_path(),
                encoded_paths[index]
                    .as_ref()
                    .expect("encoded path")
                    .as_path(),
            )
        })
        .collect();
    let encoding_parallelism = FeatureParallelism::for_encoding(
        plan.streams
            .iter()
            .map(|stream| stream.encodings.len())
            .max()
            .unwrap_or(0),
    );
    for result in encoding_parallelism.map_streams(&stream_jobs, |(stream, rows, path)| {
        encode_stream(
            stream,
            rows,
            path,
            &plan,
            &plan_identity,
            &encoding_parallelism,
        )
    }) {
        result?;
    }
    let plan_path = import::temporary_path(local, &format!("features-{generation}-plan"))?;
    fs::write(&plan_path, plan.to_json())
        .map_err(|error| format!("cannot write {}: {error}", plan_path.display()))?;
    let encoded = encoding.elapsed();

    // Publish every object, then the manifest last, and mirror it locally.
    let publishing = Instant::now();
    let mut paths = vec![PLAN_OBJECT_PATH.to_string()];
    let mut files = vec![plan_path];
    for ((stream, temporary), encoded_path) in plan
        .streams
        .iter()
        .zip(temporaries.chunks(3))
        .zip(encoded_paths)
    {
        paths.extend(stream.object_paths());
        files.extend(temporary.iter().cloned());
        files.extend(encoded_path);
    }
    assert_eq!(paths.len(), files.len(), "one file per object path");
    let identities: Vec<ObjectIdentity> = files
        .iter()
        .map(|path| store::identify(path))
        .collect::<Result<_, _>>()?;
    let mut objects: Vec<ObjectRecord> = paths
        .iter()
        .zip(&identities)
        .map(|(path, identity)| import::record(ObjectRole::Normalized, path, identity))
        .collect();
    let mut reused = 0;
    for ((object, identity), file) in objects.iter_mut().zip(&identities).zip(&files) {
        local.put_new(&object.key, file, identity)?;
        let put = destination.put_new(&object.key, file, identity)?;
        if let Put::Reused(_) = put {
            reused += 1;
        }
        object.crc32c = put.object().crc32c;
        object.generation = put.object().generation;
        fs::remove_file(file)
            .map_err(|error| format!("cannot remove {}: {error}", file.display()))?;
    }
    let rows: u64 = summaries.iter().map(|summary| summary.rows).sum();
    let events: u64 = summaries
        .iter()
        .map(|summary| summary.structure_events + summary.sequence_events)
        .sum();
    let manifest = FeatureManifest {
        kind: FEATURE_MANIFEST_KIND.to_string(),
        schema_version: FEATURE_SCHEMA_VERSION,
        generation: generation.clone(),
        broker: plan.broker.clone(),
        provider_symbol: plan.provider_symbol.clone(),
        instrument: plan.instrument.clone(),
        role: bound.input.role,
        input_generation: bound.input.generation.clone(),
        plan_identity: plan_identity.clone(),
        frozen_from,
        profile_generation: bound.stream_manifest.generation.clone(),
        config_hash: config.content_hash(),
        code_revision: CODE_REVISION.to_string(),
        observations: profile.observations,
        streams: summaries,
        objects,
    };
    let report = format!(
        "features {id} {} generation {generation} plan {plan_identity} input {} observations {} rows {rows} events {events} objects {} reused {reused}",
        bound.input.role,
        bound.input.generation,
        profile.observations,
        manifest.objects.len()
    );
    let committed = match destination.head(&key)? {
        Some(_) => {
            let mut bytes = Vec::new();
            destination.read_to(&key, None, &mut bytes)?;
            let committed = FeatureManifest::from_json(&bytes)
                .map_err(|error| format!("{}: {error}", destination.uri(&key)))?;
            if !(committed.generation == manifest.generation
                && committed.role == manifest.role
                && committed.observations == manifest.observations
                && committed.streams == manifest.streams
                && import::same_objects(&committed.objects, &manifest.objects, &identities))
            {
                return Err(format!(
                    "{} records a different generation, object set, observation count, or streams than this build produced",
                    destination.uri(&key)
                ));
            }
            bytes
        }
        None => manifest.to_json(),
    };
    // Reconstruct from the published objects under the manifest bytes about to become ready;
    // a generation its own verifier rejects is never marked ready.
    let uri = destination.uri(&key);
    let verified = verify_feature(&uri, destination, &key, &committed)?;
    let temporary = import::temporary_path(local, &format!("manifest-{generation}"))?;
    fs::write(&temporary, &committed)
        .map_err(|error| format!("cannot write {}: {error}", temporary.display()))?;
    let identity = store::identify(&temporary)?;
    let put = destination.put_new(&key, &temporary, &identity)?;
    local.put_new(&key, &temporary, &identity)?;
    fs::remove_file(&temporary)
        .map_err(|error| format!("cannot remove {}: {error}", temporary.display()))?;
    let manifest = FeatureManifest::from_json(&committed).expect("the committed manifest parsed");
    if let (Some(request), Some(receipt_key)) = (&request, &receipt_key)
        && manifest.role == request.role
        && manifest.input_generation == request.input_generation
        && manifest.profile_generation == request.profile_generation
        && manifest.code_revision == request.code_revision
        && manifest.frozen_from.is_none()
        && plan.unfitted().identity() == request.unfitted_plan_identity
    {
        write_fit_receipt(
            local,
            destination,
            receipt_key,
            &FitReceipt {
                request_digest: fit_request_digest(request),
                code_revision: CODE_REVISION.to_string(),
                generation: manifest.generation.clone(),
            },
        )?;
    }
    let published = publishing.elapsed();
    let line = match put {
        Put::Reused(_) => format!("{report} (already published)"),
        Put::Created(_) => format!(
            "{report} [stream {:.3}s fit {:.3}s encode {:.3}s publish {:.3}s]",
            streamed.as_secs_f64(),
            fitted.as_secs_f64(),
            encoded.as_secs_f64(),
            published.as_secs_f64()
        ),
    };
    Ok(Built {
        manifest,
        plan,
        report: format!("{line}\n{verified}"),
    })
}

/// Verifies a feature generation: every object's bytes and hashes, the plan's identity, and
/// every stream's tables against the manifest and the plan.
pub fn verify_feature(uri: &str, store: &Store, key: &str, bytes: &[u8]) -> Result<String, String> {
    let manifest = FeatureManifest::from_json(bytes).map_err(|error| format!("{uri}: {error}"))?;
    if manifest.key() != key {
        return Err(format!(
            "{uri} holds the manifest of generation {}",
            manifest.generation
        ));
    }
    let plan_object = manifest
        .objects
        .iter()
        .find(|object| object.path == PLAN_OBJECT_PATH)
        .expect("a validated manifest lists its plan");
    let plan = fitted_plan(uri, store, &manifest)?;
    let mut bytes_verified = plan_object.bytes;
    let mut expected_paths = vec![PLAN_OBJECT_PATH.to_string()];
    for stream in &plan.streams {
        expected_paths.extend(stream.object_paths());
    }
    expected_paths.sort_unstable();
    let mut recorded: Vec<&str> = manifest
        .objects
        .iter()
        .map(|object| object.path.as_str())
        .collect();
    recorded.sort_unstable();
    if recorded
        != expected_paths
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>()
    {
        return Err(format!(
            "{uri}: the manifest's objects are not the plan's object set"
        ));
    }
    let mut rows = 0;
    let mut events = 0;
    for (stream, summary) in plan.streams.iter().zip(&manifest.streams) {
        let expected = [
            (
                ROWS_MESSAGE,
                summary.rows,
                stream
                    .outputs
                    .iter()
                    .map(TableColumn::of_output)
                    .collect::<Vec<_>>(),
                ("raw_identity", plan.raw_identity.as_str()),
            ),
            (
                STRUCTURE_MESSAGE,
                summary.structure_events,
                structure_columns(),
                ("raw_identity", plan.raw_identity.as_str()),
            ),
            (
                SEQUENCE_MESSAGE,
                summary.sequence_events,
                sequence_columns(),
                ("raw_identity", plan.raw_identity.as_str()),
            ),
            (
                ENCODED_MESSAGE,
                summary.rows,
                stream
                    .encodings
                    .iter()
                    .map(|encoding| TableColumn::new(&encoding.output, ColumnType::Int16))
                    .collect(),
                ("plan_identity", manifest.plan_identity.as_str()),
            ),
        ];
        let mut decision: (Option<i64>, Option<i64>) = (None, None);
        for (path, (message, count, columns, identity)) in
            stream.object_paths().iter().zip(expected)
        {
            let object = manifest
                .objects
                .iter()
                .find(|object| object.path == *path)
                .expect("a validated manifest lists every stream object");
            let (verified, local) = verify::fetch(store, object, true)?;
            bytes_verified += verified;
            let location = store.uri(&object.key);
            let reader = TableReader::open(
                &local.expect("decoded objects have a local path").path,
                message,
            )
            .map_err(|reason| format!("{location}: {reason}"))?;
            let metadata: Vec<(String, String)> = table_metadata(&plan, stream, identity)
                .into_iter()
                .map(|(key, value)| (key.to_string(), value))
                .collect();
            if reader.columns() != columns.as_slice()
                || reader.metadata() != metadata
                || reader.rows() != count
            {
                return Err(format!(
                    "{location}: columns, metadata, or row count disagree with the plan and manifest"
                ));
            }
            if message == ROWS_MESSAGE
                && let Some(index) = reader.column_index("close_time_micros")
            {
                {
                    for group in 0..reader.row_groups() {
                        for value in reader.column(group, index)?.into_iter().flatten() {
                            if let Value::Time(micros) = value {
                                decision.0.get_or_insert(micros);
                                decision.1 = Some(micros);
                            }
                        }
                    }
                    if decision.0.map(format_event_time_micros) != summary.first_decision_time
                        || decision.1.map(format_event_time_micros) != summary.last_decision_time
                    {
                        return Err(format!(
                            "{location}: decision-time bounds disagree with the manifest"
                        ));
                    }
                }
            }
        }
        rows += summary.rows;
        events += summary.structure_events + summary.sequence_events;
    }
    Ok(format!(
        "verified {} {} generation {} rows {rows} events {events} objects {} bytes {bytes_verified}",
        manifest.instrument,
        manifest.role,
        manifest.generation,
        manifest.objects.len()
    ))
}

#[cfg(test)]
mod tests {
    use super::{
        FeatureParallelism, FitReceipt, FrozenRequest, check_frozen_ready,
        verify_frozen_before_revision, write_fit_receipt,
    };
    use crate::store::Store;
    use binary_alpha_engine::dataset::GenerationManifest;
    use binary_alpha_engine::features::FeatureManifest;
    use binary_alpha_engine::research::Access;

    #[test]
    fn dirty_revision_checks_existing_frozen_request_and_objects() {
        let fixture = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/legacy_schema1/published");
        let bytes = std::fs::read(fixture.join(
            "manifests/24409612301a6ee325fcdac35b37641c65cca1475a72f93b244f7df2efe79cc1/ready.json",
        ))
        .unwrap();
        let mut manifest = FeatureManifest::from_json(&bytes).unwrap();
        manifest.frozen_from = Some("original-frozen-plan".into());
        let root = std::env::temp_dir().join(format!(
            "binary-alpha-dirty-frozen-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let path = root.join(manifest.key());
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, manifest.to_json()).unwrap();
        let destination = Store::filesystem(&root);
        let request = |frozen_from| FrozenRequest {
            generation: &manifest.generation,
            role: manifest.role,
            input_generation: &manifest.input_generation,
            profile_generation: &manifest.profile_generation,
            frozen_from,
            plan_identity: &manifest.plan_identity,
            code_revision: "test-dirty",
        };
        let mismatch = check_frozen_ready(
            &destination,
            request("another-frozen-plan"),
            Access::ORDINARY,
        )
        .unwrap_err();
        assert!(mismatch.contains("immutable feature generation conflict"));
        assert!(mismatch.contains("does not match the frozen application request"));
        let corrupt = check_frozen_ready(
            &destination,
            request("original-frozen-plan"),
            Access::ORDINARY,
        )
        .unwrap_err();
        assert!(corrupt.contains("immutable feature generation conflict"));
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn fitting_and_encoding_use_their_own_live_memory() {
        let available = 1 << 30;
        let fit = FeatureParallelism::fit_bytes(1_000_000, 3);
        let encode = FeatureParallelism::encoding_bytes(4);
        assert_eq!(FeatureParallelism::memory_limit(available, 8, fit), 1);
        assert_eq!(FeatureParallelism::memory_limit(available, 8, encode), 8);
        assert_eq!(FeatureParallelism::memory_limit(available, 2, encode), 2);
        assert_eq!(FeatureParallelism::memory_limit(0, 8, encode), 1);
        assert_eq!(FeatureParallelism::encoding_bytes(8), encode * 2);
        let two = FeatureParallelism {
            limit: 2,
            active: std::sync::atomic::AtomicUsize::new(1),
        };
        let (sender, receiver) = std::sync::mpsc::channel();
        let receiver = std::sync::Mutex::new(receiver);
        let concurrent = two.map_streams(&[0, 1], |stream| {
            if *stream == 0 {
                receiver
                    .lock()
                    .unwrap()
                    .recv_timeout(std::time::Duration::from_secs(5))
                    .is_ok()
            } else {
                sender.send(()).unwrap();
                true
            }
        });
        assert_eq!(concurrent, [true, true]);
    }

    #[test]
    fn different_revision_verifies_corrupt_frozen_object_before_input() {
        let fixture = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/legacy_schema1/published");
        let manifest_path = fixture.join(
            "manifests/24409612301a6ee325fcdac35b37641c65cca1475a72f93b244f7df2efe79cc1/ready.json",
        );
        let bytes = std::fs::read(manifest_path).unwrap();
        let manifest = FeatureManifest::from_json(&bytes).unwrap();
        if let Some(root) = std::env::var_os("BINARY_ALPHA_TEST_FROZEN_ROOT") {
            let store = Store::filesystem(root);
            let error = verify_frozen_before_revision(
                &store,
                &manifest.key(),
                &bytes,
                &manifest,
                "another-unique-revision",
            )
            .unwrap_err();
            assert!(
                error.contains("immutable feature generation conflict"),
                "{error}"
            );
            return;
        }
        let input = GenerationManifest::from_json(
            &std::fs::read(fixture.join(format!(
                "manifests/{}/ready.json",
                manifest.input_generation
            )))
            .unwrap(),
        )
        .unwrap();
        let root = std::env::temp_dir().join(format!(
            "binary-alpha-frozen-revision-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let plan = &manifest.objects[0];
        let rows = &manifest.objects[1];
        for (object, source) in [(plan, Some(fixture.join(&plan.key))), (rows, None)] {
            let path = root.join(&object.key);
            std::fs::create_dir_all(path.parent().unwrap()).unwrap();
            if let Some(source) = source {
                std::fs::copy(source, path).unwrap();
            } else {
                std::fs::write(path, b"corrupt row object").unwrap();
            }
        }
        let log = root.join("access.log");
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "features::tests::different_revision_verifies_corrupt_frozen_object_before_input",
            ])
            .env("BINARY_ALPHA_TEST_FROZEN_ROOT", &root)
            .env("BINARY_ALPHA_STORE_LOG", &log)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stdout)
        );
        let access = std::fs::read_to_string(log).unwrap();
        assert!(access.contains(&format!("head {}", rows.key)), "{access}");
        for object in &input.objects {
            assert!(!access.contains(&format!("read_to {}", object.key)));
        }
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn fit_receipt_accepts_identical_content_and_rejects_a_conflict() {
        let root = std::env::temp_dir().join(format!(
            "binary-alpha-fit-receipt-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let local = Store::filesystem(root.join("local"));
        let destination = Store::filesystem(root.join("published"));
        let key = "features/fits/receipt-test";
        let receipt = FitReceipt {
            request_digest: "request".into(),
            code_revision: "revision".into(),
            generation: "first".into(),
        };
        write_fit_receipt(&local, &destination, key, &receipt).unwrap();
        write_fit_receipt(&local, &destination, key, &receipt).unwrap();
        let conflict = FitReceipt {
            generation: "second".into(),
            ..receipt
        };
        assert!(
            write_fit_receipt(&local, &destination, key, &conflict)
                .unwrap_err()
                .contains("already holds different content")
        );
        std::fs::remove_dir_all(root).unwrap();
    }
}

//! `binary-alpha outcomes build`: label every decision row of one feature generation against the
//! tick generation it was computed from, and publish the labels as one outcome generation.
//!
//! The engine owns the label rule, the reader, the identities, and the manifest; this module owns
//! reading the bound generations, the little-endian object layout, publication through the same
//! store as every other generation, and the reconstruction proof.

use std::fs::{self, File};
use std::io::{BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::time::Instant;

use binary_alpha_engine::config::{Config, ManifestUri, Outcomes, RunMode};
use binary_alpha_engine::dataset::{
    Capability, DatasetRole, GenerationManifest, ObjectRecord, ObjectRole, PriceRepresentation,
    TimeUnit, manifest_key,
};
use binary_alpha_engine::features::{
    FeatureManifest, FeaturePlan, FeatureStreamSummary, StreamPlan, Value,
};
use binary_alpha_engine::market::{PriceScale, format_event_time_micros};
use binary_alpha_engine::outcomes::{
    Labels, MISSING_INDEX, OUTCOME_MANIFEST_KIND, OUTCOME_SCHEMA_VERSION, OutcomeBuilder,
    OutcomeManifest, OutcomeRule, OutcomeStreamSummary, REFERENCE_CLOCK, TICK_PRICE_OBJECT_PATH,
    TICK_TIME_OBJECT_PATH, outcome_generation_id, stream_object_paths,
};
use binary_alpha_engine::research::Access;
use binary_alpha_engine::stream::Observation;

use crate::archive::TableReader;
use crate::audit::feed_generation;
use crate::features::{self, ROWS_MESSAGE};
use crate::import::{self, CODE_REVISION};
use crate::parallel;
use crate::store::{self, ObjectIdentity, Put, Store};
use crate::verify::{self, LocalObject};

/// The reference-clock column of every labeled feature-row table.
const REFERENCE_COLUMN: &str = "close_time_micros";

/// Decision rows labeled per unit of parallel work; a row's labels never depend on its chunk.
const CHUNK_ROWS: usize = 1 << 12;

/// Runs the configured outcome build, writing its report and reconstruction lines to `out`.
pub fn run(config_path: &Path, out: &mut dyn Write) -> Result<(), String> {
    let config = crate::load_config(config_path)?;
    if config.run_mode != RunMode::Research {
        return Err(format!(
            "run_mode: an outcome build is research, not `{}`",
            config.run_mode
        ));
    }
    let settings = config
        .outcomes
        .as_ref()
        .ok_or("outcomes: the table is required")?;
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
    };
    let line = build(settings, &config, &local, &destination, access)
        .map_err(|reason| format!("outcomes: {reason}"))?
        .report;
    writeln!(out, "{line}")
        .and_then(|()| out.flush())
        .map_err(|error| format!("cannot write the report: {error}"))
}

/// The bound tick and feature inputs of one build, resolved from permitted manifest metadata
/// before any child object other than the feature plan is read.
pub(crate) struct Bound {
    pub(crate) tick: GenerationManifest,
    pub(crate) tick_store: Store,
    pub(crate) scale: PriceScale,
    pub(crate) feature: FeatureManifest,
    pub(crate) feature_store: Store,
    pub(crate) plan: FeaturePlan,
}

/// Reads and checks a tick ready manifest and its feature generation, then the feature plan,
/// for a build of `role`; `field` names the input in errors and `what` names the build. A
/// declared role, a holdout generation, a source without ticks, and a feature generation of
/// another role, instrument, or tick generation are refused on the manifest bytes alone.
pub(crate) fn bind_inputs(
    field: &dyn Fn(&str) -> String,
    role: DatasetRole,
    tick_manifest: &ManifestUri,
    feature_manifest: &ManifestUri,
    what: &str,
    access: Access<'_>,
) -> Result<Bound, String> {
    bind_source_inputs(
        field,
        role,
        tick_manifest,
        feature_manifest,
        what,
        access,
        false,
    )
}

/// A live source may use a separately fitted plan; all scope and scale checks are shared.
#[allow(clippy::too_many_arguments)]
pub(crate) fn bind_source_inputs(
    field: &dyn Fn(&str) -> String,
    role: DatasetRole,
    tick_manifest: &ManifestUri,
    feature_manifest: &ManifestUri,
    what: &str,
    access: Access<'_>,
    refit: bool,
) -> Result<Bound, String> {
    let (tick_store, tick, scale) =
        bind_tick(&field("tick_manifest"), role, tick_manifest, what, access)?;
    let (feature_store, feature) = features::feature_manifest(
        &field("feature_manifest"),
        &feature_manifest.to_string(),
        access,
    )?;
    if !refit && feature.input_generation != tick.generation {
        return Err(format!(
            "{}: feature generation {} was computed from tick generation {}, not {}",
            field("feature_manifest"),
            feature.generation,
            feature.input_generation,
            tick.generation
        ));
    }
    if feature.role != tick.role
        || feature.broker != tick.broker
        || feature.provider_symbol != tick.provider_symbol
    {
        return Err(format!(
            "{}: feature generation {} describes {} `{}`, not {} `{}`",
            field("feature_manifest"),
            feature.generation,
            feature.instrument,
            feature.role,
            tick.instrument,
            tick.role
        ));
    }
    // The plan is the only child object read before the rows; it describes the manifest's
    // instrument and streams, so a build may zip the streams, and it must carry the ticks'
    // price scale.
    let plan = features::fitted_plan(&field("feature_manifest"), &feature_store, &feature)?;
    if plan.price_scale != scale {
        return Err(format!(
            "{}: the plan carries price scale {}, but the ticks carry {}",
            field("feature_manifest"),
            plan.price_scale.digits(),
            scale.digits()
        ));
    }
    Ok(Bound {
        tick,
        tick_store,
        scale,
        feature,
        feature_store,
        plan,
    })
}

/// The bound inputs of an outcome build, whose ticks must be indexable below the missing
/// index.
fn bind(settings: &Outcomes, access: Access<'_>) -> Result<Bound, String> {
    let bound = bind_inputs(
        &|name| name.to_string(),
        settings.role,
        &settings.tick_manifest,
        &settings.feature_manifest,
        "an outcome build",
        access,
    )?;
    if u32::try_from(bound.tick.row_count).is_err() {
        return Err(format!(
            "tick_manifest: {} ticks cannot be indexed below the missing index",
            bound.tick.row_count
        ));
    }
    Ok(bound)
}

/// Reads and checks one tick ready manifest for a build of `role` on its bytes alone: another
/// kind, another generation, holdout data, another role, and a source without ticks are refused
/// before any object is read. `field` names the input in errors and `what` names the build.
pub(crate) fn bind_tick(
    field: &str,
    role: DatasetRole,
    tick_manifest: &ManifestUri,
    what: &str,
    access: Access<'_>,
) -> Result<(Store, GenerationManifest, PriceScale), String> {
    let uri = tick_manifest.to_string();
    access
        .permit(Some(role), tick_manifest.generation())
        .map_err(|reason| format!("{field}: {reason}"))?;
    let (tick_store, tick_key) = verify::open(&uri)?;
    let mut bytes = Vec::new();
    tick_store.read_to(&tick_key, None, &mut bytes)?;
    if let Some(kind) = verify::manifest_kind(&bytes)? {
        return Err(format!(
            "{field}: {uri} is a `{kind}` manifest, not a dataset ready manifest"
        ));
    }
    let tick = GenerationManifest::from_json(&bytes)
        .map_err(|error| format!("{field}: {uri}: {error}"))?;
    if tick.key() != tick_key {
        return Err(format!(
            "{field}: {uri} holds the manifest of generation {}",
            tick.generation
        ));
    }
    if tick.role == DatasetRole::Holdout {
        access
            .protected(std::iter::once(tick.generation.as_str()))
            .map_err(|reason| format!("{field}: holdout data never enters {what}; {reason}"))?;
    }
    if tick.role != role {
        return Err(format!(
            "role: declared `{role}`, but generation {} is `{}`",
            tick.generation, tick.role
        ));
    }
    tick.require(Capability::Ticks)
        .map_err(|error| format!("{field}: {error}"))?;
    let PriceRepresentation::IntegerUnits { scale } = tick.price_representation else {
        unreachable!("a validated tick generation carries integer units at microsecond times")
    };
    Ok((tick_store, tick, scale))
}

/// Every tick of one generation, in order, through the Phase 02 readers.
pub(crate) fn load_ticks(
    store: &Store,
    manifest: &GenerationManifest,
    scale: PriceScale,
) -> Result<(Vec<i64>, Vec<i64>), String> {
    let count = usize::try_from(manifest.row_count).map_err(|_| {
        format!(
            "tick_manifest: {} ticks cannot be loaded",
            manifest.row_count
        )
    })?;
    let (mut times, mut prices) = (Vec::with_capacity(count), Vec::with_capacity(count));
    feed_generation(store, manifest, scale, &mut |observation| {
        if let Observation::Tick(tick) = observation {
            times.push(tick.event_time_micros);
            prices.push(tick.price_units);
        }
        Ok(())
    })?;
    if times.len() as u64 != manifest.row_count {
        return Err(format!(
            "tick_manifest: observed {} ticks, but the manifest records {} rows; nothing was published",
            times.len(),
            manifest.row_count
        ));
    }
    Ok((times, prices))
}

/// The reference clock of every decision row of one stream, checked against the plan's frozen
/// row identity and the feature manifest's ordered row mapping.
fn read_references(
    bound: &Bound,
    stream: &StreamPlan,
    summary: &FeatureStreamSummary,
) -> Result<Vec<i64>, String> {
    let path = &stream.object_paths()[0];
    let object = bound
        .feature
        .objects
        .iter()
        .find(|object| object.path == *path)
        .expect("a validated feature manifest lists every stream's rows");
    let (_, local) = verify::fetch(&bound.feature_store, object, true)?;
    let location = bound.feature_store.uri(&object.key);
    let reader = TableReader::open(
        &local.expect("decoded objects have a local path").path,
        ROWS_MESSAGE,
    )
    .map_err(|reason| format!("{location}: {reason}"))?;
    let expected: Vec<(String, String)> = features::table_metadata(
        &bound.plan,
        stream,
        ("raw_identity", &bound.plan.raw_identity),
    )
    .into_iter()
    .map(|(key, value)| (key.to_string(), value))
    .collect();
    if reader.metadata() != expected {
        return Err(format!(
            "{location}: the rows table does not carry the plan's frozen row identity"
        ));
    }
    let index = reader
        .column_index(REFERENCE_COLUMN)
        .ok_or_else(|| format!("{location}: no `{REFERENCE_COLUMN}` column"))?;
    let mut references = Vec::with_capacity(reader.rows() as usize);
    for group in 0..reader.row_groups() {
        for value in reader.column(group, index)? {
            match value {
                Some(Value::Time(micros)) => references.push(micros),
                _ => {
                    return Err(format!(
                        "{location}: a decision row has no {REFERENCE_COLUMN}"
                    ));
                }
            }
        }
    }
    if references.len() as u64 != summary.rows
        || bounds(&references)
            != (
                summary.first_decision_time.clone(),
                summary.last_decision_time.clone(),
            )
    {
        return Err(format!(
            "{location}: {} rows do not map onto the feature manifest's {} rows from {:?} to {:?}",
            references.len(),
            summary.rows,
            summary.first_decision_time,
            summary.last_decision_time
        ));
    }
    Ok(references)
}

/// The first and last reference time as a manifest records them.
fn bounds(references: &[i64]) -> (Option<String>, Option<String>) {
    (
        references.first().copied().map(format_event_time_micros),
        references.last().copied().map(format_event_time_micros),
    )
}

/// The little-endian bytes of an array.
fn le_bytes<T: Copy, const N: usize>(values: &[T], encode: fn(T) -> [u8; N]) -> Vec<u8> {
    values.iter().flat_map(|&value| encode(value)).collect()
}

/// Decodes a little-endian array, rejecting a byte count that is not whole values.
pub(crate) fn from_le_bytes<T, const N: usize>(
    bytes: &[u8],
    decode: fn([u8; N]) -> T,
) -> Result<Vec<T>, String> {
    let (values, remainder) = bytes.as_chunks::<N>();
    if !remainder.is_empty() {
        return Err(format!(
            "{} bytes are not whole {N}-byte values",
            bytes.len()
        ));
    }
    Ok(values.iter().map(|chunk| decode(*chunk)).collect())
}

/// Labels `references` in parallel chunks and hands every chunk's labels to `sink` in row order.
fn label_chunks(
    builder: &OutcomeBuilder,
    references: &[i64],
    sink: &mut dyn FnMut(Labels) -> Result<(), String>,
) -> Result<(), String> {
    let threads = std::thread::available_parallelism().map_or(1, |count| count.get());
    for batch in references.chunks(CHUNK_ROWS * threads) {
        let chunks: Vec<&[i64]> = batch.chunks(CHUNK_ROWS).collect();
        for labels in parallel::map(&chunks, |chunk| builder.label(chunk)) {
            sink(labels?)?;
        }
    }
    Ok(())
}

/// One temporary object written in order and closed before it is identified.
pub(crate) struct Temporary {
    path: PathBuf,
    writer: BufWriter<File>,
}

impl Temporary {
    pub(crate) fn create(local: &Store, name: &str) -> Result<Self, String> {
        let path = import::temporary_path(local, name)?;
        let file = File::create(&path)
            .map_err(|error| format!("cannot create {}: {error}", path.display()))?;
        Ok(Self {
            path,
            writer: BufWriter::new(file),
        })
    }

    pub(crate) fn write(&mut self, bytes: &[u8]) -> Result<(), String> {
        self.writer
            .write_all(bytes)
            .map_err(|error| format!("cannot write {}: {error}", self.path.display()))
    }

    pub(crate) fn finish(self) -> Result<PathBuf, String> {
        self.writer
            .into_inner()
            .map_err(|error| error.to_string())
            .and_then(|file| file.sync_all().map_err(|error| error.to_string()))
            .map_err(|error| format!("cannot close {}: {error}", self.path.display()))?;
        Ok(self.path)
    }
}

/// One published outcome generation and the report and verification lines of the command.
pub(crate) struct Built {
    pub(crate) generation: String,
    pub(crate) report: String,
}

/// Binds, labels, publishes, and reconstructs one outcome generation; a completed identical
/// generation is reused.
pub(crate) fn build(
    settings: &Outcomes,
    config: &Config,
    local: &Store,
    destination: &Store,
    access: Access<'_>,
) -> Result<Built, String> {
    let bound = bind(settings, access)?;
    let rule = OutcomeRule::resolve(settings)?;
    let generation =
        outcome_generation_id(&bound.tick.generation, &bound.feature.generation, &rule);
    let key = manifest_key(&generation);

    // Load the ticks, fold their quality flags, and write the shared arrays.
    let loading = Instant::now();
    let (times, prices) = load_ticks(&bound.tick_store, &bound.tick, bound.scale)?;
    let builder = OutcomeBuilder::new(rule, times, prices)
        .map_err(|reason| format!("tick_manifest: {reason}"))?;
    let mut paths = vec![
        TICK_TIME_OBJECT_PATH.to_string(),
        TICK_PRICE_OBJECT_PATH.to_string(),
    ];
    let mut files = Vec::new();
    for (name, values) in [("times", builder.times()), ("prices", builder.prices())] {
        let mut temporary = Temporary::create(local, &format!("outcomes-{generation}-{name}"))?;
        temporary.write(&le_bytes(values, i64::to_le_bytes))?;
        files.push(temporary.finish()?);
    }
    let loaded = loading.elapsed();

    // Label every decision row of every stream against the reference clock its rows carry.
    let labeling = Instant::now();
    let columns = builder.rule().expiry_seconds.len() as u64;
    let mut summaries = Vec::with_capacity(bound.plan.streams.len());
    for (stream, summary) in bound.plan.streams.iter().zip(&bound.feature.streams) {
        let references = read_references(&bound, stream, summary)?;
        let stem = format!(
            "outcomes-{generation}-{}s-{}s",
            stream.duration_seconds, stream.offset_seconds
        );
        let mut temporaries = Vec::with_capacity(4);
        for kind in ["reference", "entry", "settlement", "reason"] {
            temporaries.push(Temporary::create(local, &format!("{stem}-{kind}"))?);
        }
        temporaries[0].write(&le_bytes(&references, i64::to_le_bytes))?;
        label_chunks(&builder, &references, &mut |labels| {
            temporaries[1].write(&le_bytes(&labels.entries, u32::to_le_bytes))?;
            temporaries[2].write(&le_bytes(&labels.settlements, u32::to_le_bytes))?;
            temporaries[3].write(&labels.reasons)
        })?;
        paths.extend(stream_object_paths(
            stream.duration_seconds,
            stream.offset_seconds,
        ));
        for temporary in temporaries {
            files.push(temporary.finish()?);
        }
        summaries.push(OutcomeStreamSummary {
            duration_seconds: stream.duration_seconds,
            offset_seconds: stream.offset_seconds,
            rows: summary.rows,
            first_reference_time: summary.first_decision_time.clone(),
            last_reference_time: summary.last_decision_time.clone(),
        });
    }
    let labeled = labeling.elapsed();

    // Publish every object, then the manifest last, and mirror it locally.
    let publishing = Instant::now();
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
    let manifest = OutcomeManifest {
        kind: OUTCOME_MANIFEST_KIND.to_string(),
        schema_version: OUTCOME_SCHEMA_VERSION,
        generation: generation.clone(),
        broker: bound.tick.broker.clone(),
        provider_symbol: bound.tick.provider_symbol.clone(),
        instrument: bound.tick.instrument.clone(),
        role: bound.tick.role,
        tick_generation: bound.tick.generation.clone(),
        tick_manifest: settings.tick_manifest.clone(),
        feature_generation: bound.feature.generation.clone(),
        feature_manifest: settings.feature_manifest.clone(),
        raw_identity: bound.plan.raw_identity.clone(),
        config_hash: config.content_hash(),
        code_revision: CODE_REVISION.to_string(),
        rule: builder.rule().clone(),
        reference_clock: REFERENCE_CLOCK.to_string(),
        time_unit: TimeUnit::Microsecond,
        price_representation: PriceRepresentation::IntegerUnits { scale: bound.scale },
        missing_index: MISSING_INDEX,
        tick_count: bound.tick.row_count,
        streams: summaries,
        objects,
    };
    let report = format!(
        "outcomes {} {} generation {generation} tick {} feature {} ticks {} rows {rows} cells {} objects {} reused {reused}",
        manifest.instrument,
        manifest.role,
        manifest.tick_generation,
        manifest.feature_generation,
        manifest.tick_count,
        rows * columns,
        manifest.objects.len()
    );
    let committed = match destination.head(&key)? {
        Some(_) => {
            let mut bytes = Vec::new();
            destination.read_to(&key, None, &mut bytes)?;
            let committed = OutcomeManifest::from_json(&bytes)
                .map_err(|error| format!("{}: {error}", destination.uri(&key)))?;
            if !(committed.generation == manifest.generation
                && committed.broker == manifest.broker
                && committed.provider_symbol == manifest.provider_symbol
                && committed.instrument == manifest.instrument
                && committed.role == manifest.role
                && committed.raw_identity == manifest.raw_identity
                && committed.price_representation == manifest.price_representation
                && committed.tick_count == manifest.tick_count
                && committed.streams == manifest.streams
                && import::same_objects(&committed.objects, &manifest.objects, &identities))
            {
                return Err(format!(
                    "{} records a different generation, instrument, role, row identity, price representation, tick count, streams, or object set than this build produced",
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
    let verified = verify_outcome(&uri, destination, &key, &committed)?;
    let temporary = import::temporary_path(local, &format!("manifest-{generation}"))?;
    fs::write(&temporary, &committed)
        .map_err(|error| format!("cannot write {}: {error}", temporary.display()))?;
    let identity = store::identify(&temporary)?;
    let put = destination.put_new(&key, &temporary, &identity)?;
    local.put_new(&key, &temporary, &identity)?;
    fs::remove_file(&temporary)
        .map_err(|error| format!("cannot remove {}: {error}", temporary.display()))?;
    let published = publishing.elapsed();
    let line = match put {
        Put::Reused(_) => format!("{report} (already published)"),
        Put::Created(_) => format!(
            "{report} [load {:.3}s label {:.3}s publish {:.3}s]",
            loaded.as_secs_f64(),
            labeled.as_secs_f64(),
            published.as_secs_f64()
        ),
    };
    Ok(Built {
        generation,
        report: format!("{line}\n{verified}"),
    })
}

/// One verified object read back in order and compared with the bytes recomputed for it.
struct Published {
    location: String,
    file: File,
    _local: LocalObject,
}

impl Published {
    fn open(location: String, local: LocalObject) -> Result<Self, String> {
        let file =
            File::open(&local.path).map_err(|error| format!("cannot open {location}: {error}"))?;
        Ok(Self {
            location,
            file,
            _local: local,
        })
    }

    fn read_all(mut self) -> Result<Vec<u8>, String> {
        let mut bytes = Vec::new();
        self.file
            .read_to_end(&mut bytes)
            .map_err(|error| format!("cannot read {}: {error}", self.location))?;
        Ok(bytes)
    }

    fn expect(&mut self, recomputed: &[u8], what: &str) -> Result<(), String> {
        let mut stored = vec![0; recomputed.len()];
        self.file
            .read_exact(&mut stored)
            .map_err(|error| format!("cannot read {}: {error}", self.location))?;
        if stored != recomputed {
            return Err(format!(
                "{}: the stored {what} disagree with the labels recomputed from the published ticks and reference times",
                self.location
            ));
        }
        Ok(())
    }
}

/// Verifies an outcome generation: every object's bytes and hashes, the tick arrays, and every
/// stream's reference times, entry indices, settlement indices, and reasons against the labels
/// recomputed from the published ticks and reference times under the manifest's rule.
pub fn verify_outcome(uri: &str, store: &Store, key: &str, bytes: &[u8]) -> Result<String, String> {
    let manifest = OutcomeManifest::from_json(bytes).map_err(|error| format!("{uri}: {error}"))?;
    if manifest.key() != key {
        return Err(format!(
            "{uri} holds the manifest of generation {}",
            manifest.generation
        ));
    }
    let dimension = |count: u64, width: u64| {
        count
            .checked_mul(width)
            .ok_or_else(|| format!("{uri}: the declared dimensions overflow"))
    };
    let mut bytes_verified = 0;
    let mut fetch = |path: &str, expected_bytes: u64| -> Result<Published, String> {
        let object = manifest
            .objects
            .iter()
            .find(|object| object.path == path)
            .expect("a validated manifest lists every object");
        let location = store.uri(&object.key);
        if object.bytes != expected_bytes {
            return Err(format!(
                "{location}: {} bytes, expected {expected_bytes} for the declared dimensions",
                object.bytes
            ));
        }
        let (verified, local) = verify::fetch(store, object, true)?;
        bytes_verified += verified;
        Published::open(location, local.expect("decoded objects have a local path"))
    };
    let times = from_le_bytes(
        &fetch(TICK_TIME_OBJECT_PATH, dimension(manifest.tick_count, 8)?)?.read_all()?,
        i64::from_le_bytes,
    )?;
    let prices = from_le_bytes(
        &fetch(TICK_PRICE_OBJECT_PATH, dimension(manifest.tick_count, 8)?)?.read_all()?,
        i64::from_le_bytes,
    )?;
    let builder = OutcomeBuilder::new(manifest.rule.clone(), times, prices)
        .map_err(|reason| format!("{uri}: {reason}"))?;
    let columns = manifest.rule.expiry_seconds.len() as u64;
    let (mut rows, mut cells) = (0, 0);
    for summary in &manifest.streams {
        let paths = stream_object_paths(summary.duration_seconds, summary.offset_seconds);
        let cells_of_stream = dimension(summary.rows, columns)?;
        let published = fetch(&paths[0], dimension(summary.rows, 8)?)?;
        let location = published.location.clone();
        let references = from_le_bytes(&published.read_all()?, i64::from_le_bytes)?;
        if bounds(&references)
            != (
                summary.first_reference_time.clone(),
                summary.last_reference_time.clone(),
            )
        {
            return Err(format!(
                "{location}: reference-time bounds disagree with the manifest"
            ));
        }
        let mut entries = fetch(&paths[1], dimension(summary.rows, 4)?)?;
        let mut settlements = fetch(&paths[2], dimension(cells_of_stream, 4)?)?;
        let mut reasons = fetch(&paths[3], cells_of_stream)?;
        label_chunks(&builder, &references, &mut |labels| {
            entries.expect(
                &le_bytes(&labels.entries, u32::to_le_bytes),
                "entry indices",
            )?;
            settlements.expect(
                &le_bytes(&labels.settlements, u32::to_le_bytes),
                "settlement indices",
            )?;
            reasons.expect(&labels.reasons, "reasons")
        })?;
        rows += summary.rows;
        cells += cells_of_stream;
    }
    Ok(format!(
        "verified {} {} generation {} rows {rows} cells {cells} objects {} bytes {bytes_verified}",
        manifest.instrument,
        manifest.role,
        manifest.generation,
        manifest.objects.len()
    ))
}

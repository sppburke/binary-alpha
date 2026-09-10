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
use std::time::Instant;

use binary_alpha_engine::config::{Config, FeatureInstrument, StreamKey};
use binary_alpha_engine::dataset::{DatasetRole, GenerationManifest, ObjectRecord, ObjectRole};
use binary_alpha_engine::features::{
    FEATURE_MANIFEST_KIND, FEATURE_SCHEMA_VERSION, FeatureEngine, FeatureManifest, FeatureOutput,
    FeaturePlan, FeatureStreamSummary, FitWindow, PLAN_OBJECT_PATH, SequenceEvent, StreamPlan,
    StructureEvent, Value, feature_generation_id, profile_reference,
};
use binary_alpha_engine::market::format_event_time_micros;
use binary_alpha_engine::stream::{
    InstrumentProfile, PROFILE_OBJECT_PATH, STREAM_MANIFEST_KIND, Source, StreamManifest,
};

use crate::archive::{ColumnType, TableColumn, TableReader, TableWriter};
use crate::audit::feed_generation;
use crate::import::{self, CODE_REVISION};
use crate::store::{self, ObjectIdentity, Put, Store};
use crate::verify;

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
fn table_metadata(
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
    let text = fs::read_to_string(config_path)
        .map_err(|error| format!("cannot read {}: {error}", config_path.display()))?;
    let config = Config::parse(&text).map_err(|error| error.to_string())?;
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
    // Every entry resolves, and every resolved instrument and stream has one owner in one role,
    // before anything is streamed or published.
    let mut resolved = Vec::with_capacity(entries.instruments.len());
    let mut owners: HashMap<String, usize> = HashMap::new();
    for (index, entry) in entries.instruments.iter().enumerate() {
        let item =
            resolve(entry).map_err(|reason| format!("features.instruments[{index}]: {reason}"))?;
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
        let line = build(item, &config, &local, &destination)
            .map_err(|reason| format!("features.instruments[{index}]: {reason}"))?;
        writeln!(out, "{line}")
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

/// Reads and checks the input and profile manifests. Role mismatches are refused on the
/// manifest bytes alone; the profile object is the first child read.
fn bind(entry: &FeatureInstrument) -> Result<Bound, String> {
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
        return Err("input_manifest: holdout data never enters a feature build".to_string());
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

/// Loads a frozen plan from a completed feature generation's ready manifest.
fn frozen_plan(uri: &str) -> Result<(FeaturePlan, FeatureManifest), String> {
    let (store, key) = verify::open(uri)?;
    let mut bytes = Vec::new();
    store.read_to(&key, None, &mut bytes)?;
    if verify::manifest_kind(&bytes)?.as_deref() != Some(FEATURE_MANIFEST_KIND) {
        return Err(format!(
            "frozen_plan: {uri} is not a feature generation manifest"
        ));
    }
    let manifest = FeatureManifest::from_json(&bytes)
        .map_err(|error| format!("frozen_plan: {uri}: {error}"))?;
    if manifest.key() != key {
        return Err(format!(
            "frozen_plan: {uri} holds the manifest of generation {}",
            manifest.generation
        ));
    }
    let object = manifest
        .objects
        .iter()
        .find(|object| object.path == PLAN_OBJECT_PATH)
        .ok_or("frozen_plan: no plan object")?;
    let (_, fetched) = verify::fetch(&store, object, true)?;
    let plan = fs::read(&fetched.expect("decoded objects have a local path").path)
        .map_err(|error| format!("cannot read the plan: {error}"))
        .and_then(|bytes| FeaturePlan::from_json(&bytes))
        .map_err(|reason| format!("frozen_plan: {reason}"))?;
    if plan.identity() != manifest.plan_identity {
        return Err(
            "frozen_plan: the plan object does not carry the manifest's plan identity".to_string(),
        );
    }
    if !plan.is_fitted() {
        return Err("frozen_plan: the plan was never fitted".to_string());
    }
    Ok((plan, manifest))
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
struct Resolved {
    bound: Bound,
    plan: FeaturePlan,
    frozen_from: Option<String>,
}

fn resolve(entry: &FeatureInstrument) -> Result<Resolved, String> {
    let bound = bind(entry)?;
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
            let (plan, manifest) = frozen_plan(&uri.to_string())?;
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

fn build(
    resolved: Resolved,
    config: &Config,
    local: &Store,
    destination: &Store,
) -> Result<String, String> {
    let Resolved {
        bound,
        mut plan,
        frozen_from,
    } = resolved;
    let id = bound.stream_manifest.definition.id();
    let generation_of =
        |plan: &FeaturePlan| feature_generation_id(&plan.identity(), &bound.input.generation);

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

    // Fit encodings on the development rows, one column at a time, and freeze the plan.
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
        let max_labels = plan.max_labels;
        for (stream, temporary) in plan.streams.iter_mut().zip(temporaries.chunks(3)) {
            let reader = TableReader::open(&temporary[0], ROWS_MESSAGE)?;
            for encoding in &mut stream.encodings {
                let index = reader.column_index(&encoding.input).ok_or_else(|| {
                    format!("encoding input `{}` is not a row column", encoding.input)
                })?;
                let column = reader.whole_column(index)?;
                encoding.fit(&column, max_labels)?;
            }
        }
    }
    let plan_identity = plan.identity();
    let generation = generation_of(&plan);
    let key = binary_alpha_engine::dataset::manifest_key(&generation);
    let fitted = fitting.elapsed();

    // Encode every stream under the frozen encodings, row group by row group.
    let encoding = Instant::now();
    let mut encoded_paths: Vec<Option<PathBuf>> = Vec::with_capacity(plan.streams.len());
    for (stream, temporary) in plan.streams.iter().zip(temporaries.chunks(3)) {
        if stream.encodings.is_empty() {
            encoded_paths.push(None);
            continue;
        }
        let path = import::temporary_path(
            local,
            &format!(
                "features-{raw}-{}s-{}s-encoded",
                stream.duration_seconds, stream.offset_seconds
            ),
        )?;
        let columns = stream
            .encodings
            .iter()
            .map(|encoding| TableColumn::new(&encoding.output, ColumnType::Int16))
            .collect();
        let mut writer = TableWriter::create(
            &path,
            ENCODED_MESSAGE,
            columns,
            &table_metadata(&plan, stream, ("plan_identity", &plan_identity)),
        )?;
        let reader = TableReader::open(&temporary[0], ROWS_MESSAGE)?;
        for group in 0..reader.row_groups() {
            let mut codes: Vec<Vec<i16>> = Vec::with_capacity(stream.encodings.len());
            for encoding in &stream.encodings {
                let index = reader.column_index(&encoding.input).ok_or_else(|| {
                    format!("encoding input `{}` is not a row column", encoding.input)
                })?;
                codes.push(encoding.encode(&reader.column(group, index)?));
            }
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
        encoded_paths.push(Some(path));
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
    Ok(format!("{line}\n{verified}"))
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
    let (mut bytes_verified, plan_local) = verify::fetch(store, plan_object, true)?;
    let plan = fs::read(&plan_local.expect("decoded objects have a local path").path)
        .map_err(|error| error.to_string())
        .and_then(|bytes| FeaturePlan::from_json(&bytes))
        .map_err(|reason| format!("{}: {reason}", store.uri(&plan_object.key)))?;
    if plan.identity() != manifest.plan_identity
        || plan.instrument != manifest.instrument
        || plan.profile.stream_generation != manifest.profile_generation
        || !plan.is_fitted()
        || plan.streams.len() != manifest.streams.len()
    {
        return Err(format!(
            "{}: the plan does not describe the manifest's plan identity, instrument, profile, and streams",
            store.uri(&plan_object.key)
        ));
    }
    let mut rows = 0;
    let mut events = 0;
    for (stream, summary) in plan.streams.iter().zip(&manifest.streams) {
        let key = StreamKey {
            duration_seconds: summary.duration_seconds,
            offset_seconds: summary.offset_seconds,
        };
        if stream.key() != key {
            return Err(format!(
                "{uri}: the plan's streams do not match the manifest's streams"
            ));
        }
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

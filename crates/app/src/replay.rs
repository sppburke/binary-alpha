//! `binary-alpha replay`: bind the verified tick, feature, and optional outcome generations of
//! every configured instrument, feed their ticks and feature rows through the one `Engine` in
//! availability order with the configured historical simulation, and publish the canonical
//! ledger and its summary as one replay generation that its own verifier reconstructs.
//!
//! The engine owns every decision, posting, identity, and projection; this module owns reading
//! the manifests and objects, the availability merge, temporary files, publication through the
//! same store as every other generation, and the reconstruction proof.

use std::fs;
use std::io::{BufRead, BufReader, Write};
use std::path::Path;
use std::time::Instant;

use binary_alpha_engine::config::{Config, Replay, RunMode};
use binary_alpha_engine::dataset::{ObjectRecord, ObjectRole, manifest_key};
use binary_alpha_engine::execution::{
    ColumnSpec, EVENTS_OBJECT_PATH, Engine, EventKind, EventSource, FinancialEvent,
    HISTORICAL_AVAILABILITY, InstrumentBinding, Observation, REPLAY_MANIFEST_KIND,
    REPLAY_SCHEMA_VERSION, ReplayManifest, RunDefinition, SUMMARY_OBJECT_PATH, StreamColumns,
    replay_generation_id,
};
use binary_alpha_engine::features::{FeaturePlan, StreamPlan, Value};
use binary_alpha_engine::market::parse_event_time_micros;
use binary_alpha_engine::outcomes::{OUTCOME_MANIFEST_KIND, OutcomeManifest};

use crate::archive::TableReader;
use crate::features::{self, ROWS_MESSAGE};
use crate::import::{self, CODE_REVISION};
use crate::outcomes::{Bound, Temporary, bind_inputs, load_ticks};
use crate::store::{self, ObjectIdentity, Put, Store};
use crate::verify;

/// Runs the configured replay, writing its report and reconstruction lines to `out`.
pub fn run(config_path: &Path, out: &mut dyn Write) -> Result<(), String> {
    let text = fs::read_to_string(config_path)
        .map_err(|error| format!("cannot read {}: {error}", config_path.display()))?;
    let config = Config::parse(&text).map_err(|error| error.to_string())?;
    if config.run_mode != RunMode::Research {
        return Err(format!(
            "run_mode: a replay is research, not `{}`",
            config.run_mode
        ));
    }
    let base = config_path.parent().unwrap_or(Path::new("."));
    let historical_dir = base.join(config.storage.historical_data_dir.as_path());
    fs::create_dir_all(&historical_dir)
        .map_err(|error| format!("cannot create {}: {error}", historical_dir.display()))?;
    let local = Store::filesystem(&historical_dir);
    let destination = Store::open(&config.storage.publication_uri)?;
    let line =
        replay(&config, &local, &destination).map_err(|reason| format!("replay: {reason}"))?;
    writeln!(out, "{line}")
        .and_then(|()| out.flush())
        .map_err(|error| format!("cannot write the report: {error}"))
}

/// One instrument's bound inputs and its frozen binding.
struct BoundInstrument {
    inputs: Bound,
    binding: InstrumentBinding,
}

/// Binds one input through the shared tick and feature binder, then refuses an outcome
/// generation of other inputs and decision times outside the declared window on the manifest
/// bytes alone.
fn bind_instrument(settings: &Replay, index: usize) -> Result<BoundInstrument, String> {
    let input = &settings.inputs[index];
    let field = |name: &str| format!("inputs[{index}].{name}");
    let inputs = bind_inputs(
        &field,
        settings.role,
        &input.tick_manifest,
        &input.feature_manifest,
        "a replay",
    )?;
    let Bound {
        tick,
        scale,
        feature,
        plan,
        ..
    } = &inputs;
    let start = parse_event_time_micros(&settings.decision_start)?;
    let end = parse_event_time_micros(&settings.decision_end)?;
    for summary in &feature.streams {
        for (what, time) in [
            ("first", &summary.first_decision_time),
            ("last", &summary.last_decision_time),
        ] {
            let Some(time) = time else { continue };
            let micros = parse_event_time_micros(time)?;
            if micros < start || micros >= end {
                return Err(format!(
                    "{}: the {what} decision time {time} of stream {}s/{}s lies outside the declared decision window",
                    field("feature_manifest"),
                    summary.duration_seconds,
                    summary.offset_seconds
                ));
            }
        }
    }
    let outcome_generation = match &input.outcome_manifest {
        None => None,
        Some(uri) => {
            let uri = uri.to_string();
            let (outcome_store, outcome_key) = verify::open(&uri)?;
            let mut bytes = Vec::new();
            outcome_store.read_to(&outcome_key, None, &mut bytes)?;
            if verify::manifest_kind(&bytes)?.as_deref() != Some(OUTCOME_MANIFEST_KIND) {
                return Err(format!(
                    "{}: {uri} is not an outcome generation manifest",
                    field("outcome_manifest")
                ));
            }
            let outcome = OutcomeManifest::from_json(&bytes)
                .map_err(|error| format!("{}: {uri}: {error}", field("outcome_manifest")))?;
            if outcome.key() != outcome_key {
                return Err(format!(
                    "{}: {uri} holds the manifest of generation {}",
                    field("outcome_manifest"),
                    outcome.generation
                ));
            }
            if outcome.role != settings.role
                || outcome.tick_generation != tick.generation
                || outcome.feature_generation != feature.generation
                || outcome.raw_identity != plan.raw_identity
            {
                return Err(format!(
                    "{}: outcome generation {} labels role {}, tick generation {}, feature generation {}, and raw rows {}, not this input's {}, {}, {}, and {}",
                    field("outcome_manifest"),
                    outcome.generation,
                    outcome.role,
                    outcome.tick_generation,
                    outcome.feature_generation,
                    outcome.raw_identity,
                    settings.role,
                    tick.generation,
                    feature.generation,
                    plan.raw_identity
                ));
            }
            Some(outcome.generation)
        }
    };
    let plan_identity = plan.identity();
    let binding = InstrumentBinding {
        instrument: tick.instrument.clone(),
        broker: tick.broker.clone(),
        provider_symbol: tick.provider_symbol.clone(),
        price_scale: scale.digits(),
        tick_generation: tick.generation.clone(),
        feature_generation: feature.generation.clone(),
        plan_identity: plan_identity.clone(),
        raw_identity: plan.raw_identity.clone(),
        outcome_generation,
        streams: stream_columns(settings, plan, &plan_identity)?,
    };
    Ok(BoundInstrument { inputs, binding })
}

/// The streams and columns the strategies frozen on `plan` name, in frozen-plan order: every
/// base stream and condition stream, each with the compiled outputs and fitted encodings its
/// conditions read. A name that is neither is refused here with its strategy and condition.
fn stream_columns(
    settings: &Replay,
    plan: &FeaturePlan,
    plan_identity: &str,
) -> Result<Vec<StreamColumns>, String> {
    let mut streams: Vec<StreamColumns> = Vec::new();
    let stream_of = |streams: &mut Vec<StreamColumns>, key| -> Result<usize, String> {
        let stream = plan
            .stream(key)
            .ok_or_else(|| format!("stream {key} is not a stream of the frozen plan"))?;
        Ok(match streams.iter().position(|bound| bound.stream == key) {
            Some(index) => index,
            None => {
                streams.push(StreamColumns {
                    stream: stream.key(),
                    columns: Vec::new(),
                });
                streams.len() - 1
            }
        })
    };
    for (index, strategy) in settings
        .strategies
        .iter()
        .enumerate()
        .filter(|(_, strategy)| strategy.plan_identity == plan_identity)
    {
        stream_of(&mut streams, strategy.base_stream)
            .map_err(|reason| format!("strategies[{index}].base_stream: {reason}"))?;
        for (name, conditions) in [
            ("conditions", &strategy.conditions),
            ("repair", &strategy.repair),
        ] {
            for (position, condition) in conditions.iter().enumerate() {
                let field = |part: &str| format!("strategies[{index}].{name}[{position}].{part}");
                let stream = stream_of(&mut streams, condition.stream)
                    .map_err(|reason| format!("{}: {reason}", field("stream")))?;
                if streams[stream]
                    .columns
                    .iter()
                    .any(|column| column.name == condition.output)
                {
                    continue;
                }
                let plan_stream = plan.stream(condition.stream).expect("bound");
                let mut spec = column_spec(plan_stream, &condition.output).ok_or_else(|| {
                    format!(
                        "{}: `{}` is not a compiled output or fitted encoding of stream {}",
                        field("output"),
                        condition.output,
                        condition.stream
                    )
                })?;
                // The feature owner's readiness flags of the value are read beside it.
                let readiness = plan.readiness_of(&spec.source);
                spec.readiness = readiness.flags;
                spec.unready = readiness.unready;
                for flag in spec.readiness.clone() {
                    if streams[stream]
                        .columns
                        .iter()
                        .any(|column| column.name == flag)
                    {
                        continue;
                    }
                    let flag = column_spec(plan_stream, &flag).ok_or_else(|| {
                        format!(
                            "{}: readiness flag `{flag}` of `{}` is not an output of stream {}",
                            field("output"),
                            condition.output,
                            condition.stream
                        )
                    })?;
                    streams[stream].columns.push(flag);
                }
                streams[stream].columns.push(spec);
            }
        }
    }
    // Frozen-plan order, then column names in first-use order within each stream.
    streams.sort_by_key(|bound| {
        plan.streams
            .iter()
            .position(|stream| stream.key() == bound.stream)
    });
    Ok(streams)
}

/// The column a condition name reads: the compiled output itself, or the fitted encoding's input
/// with the encoding that labels it.
fn column_spec(stream: &StreamPlan, name: &str) -> Option<ColumnSpec> {
    if let Some(output) = stream.outputs.iter().find(|output| output.name == name) {
        return Some(ColumnSpec {
            name: name.to_string(),
            source: name.to_string(),
            kind: output.kind,
            encoding: None,
            readiness: Vec::new(),
            unready: Vec::new(),
        });
    }
    let encoding = stream
        .encodings
        .iter()
        .find(|encoding| encoding.output == name)?;
    let input = stream
        .outputs
        .iter()
        .find(|output| output.name == encoding.input)?;
    Some(ColumnSpec {
        name: name.to_string(),
        source: encoding.input.clone(),
        kind: input.kind,
        encoding: Some(encoding.clone()),
        readiness: Vec::new(),
        unready: Vec::new(),
    })
}

/// The rows of one stream's published table, one row group at a time, with only the clocks and
/// the bound columns held in memory.
struct RowCursor {
    reader: TableReader,
    _local: verify::LocalObject,
    location: String,
    close: usize,
    known: usize,
    sources: Vec<usize>,
    group: usize,
    offset: usize,
    clocks: Vec<(i64, i64)>,
    columns: Vec<Vec<Option<Value>>>,
    rows: u64,
}

impl RowCursor {
    fn open(bound: &BoundInstrument, stream: &StreamColumns) -> Result<Self, String> {
        let plan_stream = bound
            .inputs
            .plan
            .stream(stream.stream)
            .expect("bound streams are plan streams");
        let path = &plan_stream.object_paths()[0];
        let object = bound
            .inputs
            .feature
            .objects
            .iter()
            .find(|object| object.path == *path)
            .expect("a validated feature manifest lists every stream's rows");
        let (_, local) = verify::fetch(&bound.inputs.feature_store, object, true)?;
        let local = local.expect("decoded objects have a local path");
        let location = bound.inputs.feature_store.uri(&object.key);
        let reader = TableReader::open(&local.path, ROWS_MESSAGE)
            .map_err(|reason| format!("{location}: {reason}"))?;
        let expected: Vec<(String, String)> = features::table_metadata(
            &bound.inputs.plan,
            plan_stream,
            ("raw_identity", &bound.inputs.plan.raw_identity),
        )
        .into_iter()
        .map(|(key, value)| (key.to_string(), value))
        .collect();
        if reader.metadata() != expected {
            return Err(format!(
                "{location}: the rows table does not carry the plan's frozen row identity"
            ));
        }
        let index = |name: &str| {
            reader
                .column_index(name)
                .ok_or_else(|| format!("{location}: no `{name}` column"))
        };
        let close = index("close_time_micros")?;
        let known = index("known_at_micros")?;
        let sources = stream
            .columns
            .iter()
            .map(|column| index(&column.source))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self {
            reader,
            _local: local,
            location,
            close,
            known,
            sources,
            group: 0,
            offset: 0,
            clocks: Vec::new(),
            columns: Vec::new(),
            rows: 0,
        })
    }

    /// Loads the next row group when the buffered one is exhausted.
    fn fill(&mut self) -> Result<(), String> {
        while self.offset == self.clocks.len() && self.group < self.reader.row_groups() {
            let clock = |values: Vec<Option<Value>>| -> Result<Vec<i64>, String> {
                values
                    .into_iter()
                    .map(|value| match value {
                        Some(Value::Time(micros)) => Ok(micros),
                        _ => Err(format!("{}: a decision row has no clock", self.location)),
                    })
                    .collect()
            };
            let close = clock(self.reader.column(self.group, self.close)?)?;
            let known = clock(self.reader.column(self.group, self.known)?)?;
            self.clocks = close.into_iter().zip(known).collect();
            self.columns = self
                .sources
                .iter()
                .map(|&index| self.reader.column(self.group, index))
                .collect::<Result<_, _>>()?;
            self.offset = 0;
            self.group += 1;
        }
        Ok(())
    }

    /// The availability of the next row, if any.
    fn peek(&mut self) -> Result<Option<i64>, String> {
        self.fill()?;
        Ok(self.clocks.get(self.offset).map(|&(_, known)| known))
    }

    /// The next row: its clocks and its bound column values.
    fn next(&mut self) -> Result<(i64, i64, Vec<Option<Value>>), String> {
        self.fill()?;
        let (close, known) = self.clocks[self.offset];
        let values = self
            .columns
            .iter_mut()
            .map(|column| std::mem::take(&mut column[self.offset]))
            .collect();
        self.offset += 1;
        self.rows += 1;
        Ok((close, known, values))
    }
}

/// One instrument's inputs positioned for the availability merge.
struct Inputs {
    times: Vec<i64>,
    prices: Vec<i64>,
    next_tick: usize,
    cursors: Vec<RowCursor>,
}

impl Inputs {
    /// The earliest availability among the next tick and the next row of every stream.
    fn peek(&mut self) -> Result<Option<i64>, String> {
        let mut next = self.times.get(self.next_tick).copied();
        for cursor in &mut self.cursors {
            if let Some(known) = cursor.peek()? {
                next = Some(next.map_or(known, |time| time.min(known)));
            }
        }
        Ok(next)
    }
}

/// Feeds every instrument's observations available at each time, in input order with ticks
/// before rows and rows in frozen-plan order, through the engine; answers each admitted signal
/// with the configured simulated acceptance at the same time; and closes the window.
fn simulate(
    definition: RunDefinition,
    inputs: &mut [Inputs],
    sink: &mut dyn FnMut(&FinancialEvent) -> Result<(), String>,
) -> Result<Engine, String> {
    let mut engine = Engine::new(definition)?;
    for event in engine.drain() {
        sink(&event)?;
    }
    loop {
        let mut time: Option<i64> = None;
        for input in inputs.iter_mut() {
            if let Some(next) = input.peek()? {
                time = Some(time.map_or(next, |time| time.min(next)));
            }
        }
        let Some(time) = time else { break };
        let mut observations = Vec::new();
        for (instrument, input) in inputs.iter_mut().enumerate() {
            while input.times.get(input.next_tick) == Some(&time) {
                observations.push(Observation::Tick {
                    instrument,
                    provider_time_micros: time,
                    price_units: input.prices[input.next_tick],
                });
                input.next_tick += 1;
            }
            for (stream, cursor) in input.cursors.iter_mut().enumerate() {
                while cursor.peek()? == Some(time) {
                    let (close_time_micros, known_at_micros, values) = cursor.next()?;
                    observations.push(Observation::Row {
                        instrument,
                        stream,
                        close_time_micros,
                        known_at_micros,
                        values,
                    });
                }
            }
        }
        engine.step(time, observations)?;
        let mut acceptances = Vec::new();
        for event in engine.drain() {
            if let EventKind::Signal {
                command: Some(command),
                quote_price_units: Some(entry_price_units),
                quote_time_micros: Some(price_time_micros),
                ..
            } = &event.kind
            {
                acceptances.push(Observation::Accepted {
                    command: command.clone(),
                    source: EventSource {
                        id: format!("{HISTORICAL_AVAILABILITY}:{command}"),
                        provider_time_micros: time,
                        available_at_micros: time,
                        simulated: true,
                    },
                    entry_time_micros: time,
                    entry_price_units: *entry_price_units,
                    price_time_micros: *price_time_micros,
                });
            }
            sink(&event)?;
        }
        if !acceptances.is_empty() {
            engine.step(time, acceptances)?;
            for event in engine.drain() {
                sink(&event)?;
            }
        }
    }
    engine.finish()?;
    for event in engine.drain() {
        sink(&event)?;
    }
    Ok(engine)
}

/// The typed replay every caller uses: bind, simulate, publish, and reconstruct one replay
/// generation of the configuration's `replay` table, returning its report and reconstruction
/// lines.
pub fn replay(config: &Config, local: &Store, destination: &Store) -> Result<String, String> {
    let settings = config
        .replay
        .as_ref()
        .ok_or("replay: the table is required")?;
    let loading = Instant::now();
    let bound = (0..settings.inputs.len())
        .map(|index| bind_instrument(settings, index))
        .collect::<Result<Vec<_>, _>>()?;
    let definition = RunDefinition {
        schema_version: REPLAY_SCHEMA_VERSION,
        config_hash: config.content_hash(),
        code_revision: CODE_REVISION.to_string(),
        availability: HISTORICAL_AVAILABILITY.to_string(),
        replay: settings.clone(),
        instruments: bound.iter().map(|bound| bound.binding.clone()).collect(),
    };
    let generation = replay_generation_id(&definition.config_hash, &definition.instruments);
    let key = manifest_key(&generation);
    let mut inputs = Vec::with_capacity(bound.len());
    for instrument in &bound {
        let (times, prices) = load_ticks(
            &instrument.inputs.tick_store,
            &instrument.inputs.tick,
            instrument.inputs.scale,
        )?;
        let cursors = instrument
            .binding
            .streams
            .iter()
            .map(|stream| RowCursor::open(instrument, stream))
            .collect::<Result<Vec<_>, _>>()?;
        inputs.push(Inputs {
            times,
            prices,
            next_tick: 0,
            cursors,
        });
    }
    let loaded = loading.elapsed();

    let simulating = Instant::now();
    let mut ledger = Temporary::create(local, &format!("replay-{generation}-events"))?;
    let engine = simulate(definition, &mut inputs, &mut |event| {
        ledger.write(&event.to_line())
    })?;
    for (index, (instrument, input)) in bound.iter().zip(&inputs).enumerate() {
        for (cursor, stream) in input.cursors.iter().zip(&instrument.binding.streams) {
            let summary = instrument
                .inputs
                .feature
                .streams
                .iter()
                .find(|summary| {
                    summary.duration_seconds == stream.stream.duration_seconds
                        && summary.offset_seconds == stream.stream.offset_seconds
                })
                .expect("bound streams are manifest streams");
            if cursor.rows != summary.rows {
                return Err(format!(
                    "inputs[{index}].feature_manifest: stream {} supplied {} rows, but the manifest records {}",
                    stream.stream, cursor.rows, summary.rows
                ));
            }
        }
    }
    let events_file = ledger.finish()?;
    let summary = engine.summary();
    let mut summary_file = Temporary::create(local, &format!("replay-{generation}-summary"))?;
    summary_file.write(&summary.to_json())?;
    let files = [events_file, summary_file.finish()?];
    let simulated = simulating.elapsed();

    // Publish both objects, then the manifest last, and mirror it locally.
    let publishing = Instant::now();
    let identities: Vec<ObjectIdentity> = files
        .iter()
        .map(|path| store::identify(path))
        .collect::<Result<_, _>>()?;
    let mut objects: Vec<ObjectRecord> = [EVENTS_OBJECT_PATH, SUMMARY_OBJECT_PATH]
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
    let manifest = ReplayManifest {
        kind: REPLAY_MANIFEST_KIND.to_string(),
        schema_version: REPLAY_SCHEMA_VERSION,
        generation: generation.clone(),
        role: settings.role,
        config_hash: config.content_hash(),
        code_revision: CODE_REVISION.to_string(),
        availability: HISTORICAL_AVAILABILITY.to_string(),
        decision_start: settings.decision_start.clone(),
        decision_end: settings.decision_end.clone(),
        instruments: engine.definition().instruments.clone(),
        events: engine.sequence(),
        final_state_identity: engine.state_identity(),
        summary_identity: summary.identity(),
        objects,
    };
    let portfolio = &summary.portfolio;
    let report = format!(
        "replay {} generation {generation} instruments {} events {} signals {} accepted {} settled {} unresolved {} objects {} reused {reused}",
        manifest.role,
        manifest.instruments.len(),
        manifest.events,
        portfolio.signals,
        portfolio.accepted,
        portfolio.settled,
        portfolio.unresolved,
        manifest.objects.len()
    );
    let committed = match destination.head(&key)? {
        Some(_) => {
            let mut bytes = Vec::new();
            destination.read_to(&key, None, &mut bytes)?;
            let committed = ReplayManifest::from_json(&bytes)
                .map_err(|error| format!("{}: {error}", destination.uri(&key)))?;
            if !(committed.generation == manifest.generation
                && committed.role == manifest.role
                && committed.instruments == manifest.instruments
                && committed.events == manifest.events
                && committed.final_state_identity == manifest.final_state_identity
                && committed.summary_identity == manifest.summary_identity
                && import::same_objects(&committed.objects, &manifest.objects, &identities))
            {
                return Err(format!(
                    "{} records a different generation, role, instruments, event count, final state, summary, or object set than this replay produced",
                    destination.uri(&key)
                ));
            }
            bytes
        }
        None => manifest.to_json(),
    };
    // Reconstruct from the published ledger under the manifest bytes about to become ready; a
    // generation its own verifier rejects is never marked ready.
    let uri = destination.uri(&key);
    let verified = verify_replay(&uri, destination, &key, &committed)?;
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
            "{report} [load {:.3}s simulate {:.3}s publish {:.3}s]",
            loaded.as_secs_f64(),
            simulated.as_secs_f64(),
            published.as_secs_f64()
        ),
    };
    Ok(format!("{line}\n{verified}"))
}

/// Verifies a replay generation: every object's bytes and hashes, the ledger restored record by
/// record through the engine's one event-application function, and the restored sequence,
/// final state, and summary against the manifest and the published summary bytes.
pub fn verify_replay(uri: &str, store: &Store, key: &str, bytes: &[u8]) -> Result<String, String> {
    let manifest = ReplayManifest::from_json(bytes).map_err(|error| format!("{uri}: {error}"))?;
    if manifest.key() != key {
        return Err(format!(
            "{uri} holds the manifest of generation {}",
            manifest.generation
        ));
    }
    let mut bytes_verified = 0;
    let mut fetch = |path: &str| -> Result<(String, verify::LocalObject), String> {
        let object = manifest
            .objects
            .iter()
            .find(|object| object.path == path)
            .expect("a validated manifest lists both objects");
        let (verified, local) = verify::fetch(store, object, true)?;
        bytes_verified += verified;
        Ok((
            store.uri(&object.key),
            local.expect("decoded objects have a local path"),
        ))
    };
    let (ledger_location, ledger) = fetch(EVENTS_OBJECT_PATH)?;
    let (summary_location, summary) = fetch(SUMMARY_OBJECT_PATH)?;
    let file = fs::File::open(&ledger.path)
        .map_err(|error| format!("cannot open {ledger_location}: {error}"))?;
    let lines = BufReader::with_capacity(1 << 20, file)
        .split(b'\n')
        .map(|line| line.map_err(|error| format!("cannot read {ledger_location}: {error}")));
    let engine = Engine::restore(lines).map_err(|reason| format!("{ledger_location}: {reason}"))?;
    let definition = engine.definition();
    if engine.sequence() != manifest.events
        || engine.state_identity() != manifest.final_state_identity
        || engine.summary().identity() != manifest.summary_identity
        || definition.instruments != manifest.instruments
        || definition.config_hash != manifest.config_hash
        || definition.code_revision != manifest.code_revision
        || definition.availability != manifest.availability
        || definition.replay.role != manifest.role
        || definition.replay.decision_start != manifest.decision_start
        || definition.replay.decision_end != manifest.decision_end
    {
        return Err(format!(
            "{ledger_location}: the restored ledger does not reproduce the manifest's definition, event count, final state, and summary"
        ));
    }
    let published = fs::read(&summary.path)
        .map_err(|error| format!("cannot read {summary_location}: {error}"))?;
    if published != engine.summary().to_json() {
        return Err(format!(
            "{summary_location}: the published summary disagrees with the projection restored from the ledger"
        ));
    }
    let portfolio = &engine.summary().portfolio;
    Ok(format!(
        "verified {} generation {} events {} signals {} accepted {} settled {} unresolved {} objects {} bytes {bytes_verified}",
        manifest.role,
        manifest.generation,
        manifest.events,
        portfolio.signals,
        portfolio.accepted,
        portfolio.settled,
        portfolio.unresolved,
        manifest.objects.len()
    ))
}

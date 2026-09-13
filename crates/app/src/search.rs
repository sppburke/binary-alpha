//! `binary-alpha search`: enumerate one typed candidate family, lower every distinct condition
//! through the engine's own signal records, score the complete family with the retained
//! capacity-one kernel, replay the survivors through the engine in chunks, evaluate the frozen
//! development ranking, resample settlement paths through the retained bootstrap primitive, and
//! publish the family as one immutable generation that `data verify` re-derives.
//!
//! The engine module owns every pure rule; this module owns binding, the replay batches, the
//! device calls, temporary files, publication, and the verifier.

use std::collections::BTreeMap;
use std::fs;
use std::io::Write;
use std::path::Path;
use std::time::{Duration, Instant};

use binary_alpha_accelerator::{Backend, KERNEL_SOURCES, Timings, bootstrap, search as kernels};
use binary_alpha_engine::config::{Backend as Selected, Config, RunMode, Search};
use binary_alpha_engine::dataset::{DatasetRole, ObjectRecord, ObjectRole, manifest_key};
use binary_alpha_engine::execution::{
    EVENTS_OBJECT_PATH, EventKind, FinancialEvent, ReplayManifest, SUMMARY_OBJECT_PATH,
    StrategySpec, Summary,
};
use binary_alpha_engine::outcomes::{
    InvalidReason, OUTCOME_MANIFEST_KIND, Outcome as Label, OutcomeBuilder, OutcomeManifest,
    TICK_PRICE_OBJECT_PATH, TICK_TIME_OBJECT_PATH, stream_object_paths,
};
use binary_alpha_engine::search::{
    self, ChunkRef, FAMILY_OBJECT_PATH, Family, FamilyInput, FamilyManifest, Member, RawCounts,
    SAMPLER_VERSION, StabilityOutcome, family_generation_id,
};
use sha2::{Digest, Sha256};

use crate::import::{self, CODE_REVISION};
use crate::outcomes::{bind_inputs, from_le_bytes};
use crate::replay;
use crate::store::{self, Put, Store};
use crate::verify;

/// Runs the configured search, writing its report and verification lines to `out`.
pub fn run(config_path: &Path, out: &mut dyn Write) -> Result<(), String> {
    let config = crate::load_config(config_path)?;
    if config.run_mode != RunMode::Research {
        return Err(format!(
            "run_mode: a search is research, not `{}`",
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
        search(&config, &local, &destination).map_err(|reason| format!("search: {reason}"))?;
    writeln!(out, "{line}")
        .and_then(|()| out.flush())
        .map_err(|error| format!("cannot write the report: {error}"))
}

/// One member's executable identity inside a chunk.
type ChunkMember = (String, StrategySpec, usize);

/// The bound development input: plan, instrument, and the outcome generation's stored rows.
struct Development {
    plan_identity: String,
    instrument: String,
    base_stream: binary_alpha_engine::config::StreamKey,
    input: FamilyInput,
    outcome: OutcomeManifest,
    outcome_store: Store,
}

/// The device rows of one contract duration over the base stream's reference rows.
struct DeviceRows {
    decision_ms: Vec<i64>,
    release_ms: Vec<i64>,
    valid: Vec<u8>,
    buy_win: Vec<u8>,
    sell_win: Vec<u8>,
    tie: Vec<u8>,
}

/// Wall-clock stages of one run, outside every identity.
#[derive(Default)]
struct Clock {
    load: Duration,
    lowering: Duration,
    device: Timings,
    replay: Duration,
    stability: Duration,
    publish: Duration,
}

fn add(total: &mut Timings, measured: Timings) {
    total.upload += measured.upload;
    total.execute += measured.execute;
    total.download += measured.download;
    total.allocated_bytes = total.allocated_bytes.max(measured.allocated_bytes);
}

/// The typed search every caller uses: bind, lower, score, replay, evaluate, resample, publish,
/// and verify one family generation of the configuration's `search` table.
pub fn search(config: &Config, local: &Store, destination: &Store) -> Result<String, String> {
    let settings = config
        .search
        .as_ref()
        .ok_or("search: the table is required")?;
    let backend = match config.accelerator.as_ref().map(|section| section.backend) {
        Some(Selected::Cuda) => Backend::cuda(0)?,
        _ => Backend::Cpu,
    };
    let mut clock = Clock::default();
    let started = Instant::now();

    // 1. Bind the development input and enumerate the family before any allocation.
    let development = bind_development(settings)?;
    let conditions = search::conditions(&settings.conditions);
    let candidates = search::candidates(
        &development.plan_identity,
        settings.base_stream,
        &conditions,
        settings.min_conditions as usize,
        settings.max_conditions as usize,
    );
    let expiries = expiry_columns(settings, &development)?;
    let mut members: Vec<Member> = candidates
        .iter()
        .flat_map(|candidate| {
            settings.contracts.iter().map(|contract| Member {
                logic_identity: candidate.logic_identity.clone(),
                conditions: candidate
                    .conditions
                    .iter()
                    .map(|&index| conditions[index].clone())
                    .collect(),
                contract: contract.id.clone(),
                raw: RawCounts::default(),
                null: None,
                inapplicable: None,
                score: None,
                adjusted: None,
                screened: None,
                development: None,
                rejected: None,
                rank: None,
                evaluation: None,
                evaluation_splits: BTreeMap::new(),
                stability: BTreeMap::new(),
            })
        })
        .collect();
    clock.load = started.elapsed();

    // 2. Lower every distinct condition through the engine's own signal records.
    let lowering_started = Instant::now();
    let lowering = search::lowering_replay(
        settings,
        &development.plan_identity,
        &development.instrument,
    );
    let lowering_bindings: Vec<String> = lowering.strategies.iter().map(|s| s.id.clone()).collect();
    let lowered = replay::publish(&chunk_config(config, lowering), local, destination, true)?;
    let references = read_references(&development)?;
    let codes = lowering_codes(
        &chunk_events(destination, &lowered.manifest)?,
        &references,
        conditions.len(),
    )?;
    let lowering_ref = ChunkRef {
        role: DatasetRole::Development.to_string(),
        generation: lowered.manifest.generation.clone(),
        summary_identity: lowered.manifest.summary_identity.clone(),
        bindings: lowering_bindings,
    };
    clock.lowering = lowering_started.elapsed();

    // 3. Score the complete family once per contract duration.
    let raw = score_members(
        &backend,
        settings,
        &development,
        &candidates,
        &expiries,
        &codes,
        &references,
        &mut clock,
    )?;
    let contracts = settings.contracts.len();
    for (index, member) in members.iter_mut().enumerate() {
        let contract = &settings.contracts[index % contracts];
        member.raw = raw[index].clone();
        match search::null_rate(contract) {
            Ok(null) => {
                member.score = Some(search::upper_tail(
                    member.raw.wins as u64,
                    member.raw.losses as u64,
                    null.break_even,
                ));
                member.null = Some(null);
            }
            Err(reason) => member.inapplicable = Some(reason),
        }
    }
    let applicable: Vec<usize> = (0..members.len())
        .filter(|&index| members[index].score.is_some())
        .collect();
    let adjusted = search::benjamini_hochberg(
        &applicable
            .iter()
            .map(|&index| members[index].score.expect("applicable"))
            .collect::<Vec<_>>(),
    );
    for (&index, &value) in applicable.iter().zip(&adjusted) {
        members[index].adjusted = Some(value);
    }

    // 4. Screen under heuristic scope; exhaustive scope replays every member.
    if let Some(screen) = &settings.screen {
        let mut order: Vec<usize> = applicable.clone();
        order.sort_by(|&a, &b| {
            members[a]
                .adjusted
                .partial_cmp(&members[b].adjusted)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then(a.cmp(&b))
        });
        for (position, &index) in order.iter().enumerate() {
            let adjusted = members[index].adjusted.expect("applicable");
            if adjusted > screen.max_adjusted_score {
                members[index].screened = Some(format!(
                    "adjusted score {adjusted} above the maximum {}",
                    screen.max_adjusted_score
                ));
            } else if screen.top.is_some_and(|top| position >= top as usize) {
                members[index].screened = Some(format!(
                    "beyond the first {} members by adjusted score",
                    screen.top.expect("set")
                ));
            }
        }
        for member in members.iter_mut() {
            if let Some(reason) = &member.inapplicable {
                member.screened = Some(format!("inapplicable: {reason}"));
            }
        }
    }

    // 5. Replay survivors through the engine in canonical chunks; gate and rank.
    let currency = settings.account.currency.to_string();
    let replay_started = Instant::now();
    let survivors: Vec<usize> = (0..members.len())
        .filter(|&index| members[index].screened.is_none())
        .collect();
    let mut chunks = Vec::new();
    let mut profits: BTreeMap<(String, String), Vec<f64>> = BTreeMap::new();
    let run_chunks = |role: DatasetRole,
                      window: &binary_alpha_engine::config::SearchWindow,
                      selected: &[usize],
                      members: &mut Vec<Member>,
                      chunks: &mut Vec<ChunkRef>,
                      profits: &mut BTreeMap<(String, String), Vec<f64>>|
     -> Result<(), String> {
        for chunk in selected.chunks(settings.chunk_size as usize) {
            let ids: Vec<String> = chunk.iter().map(|index| format!("m{index}")).collect();
            let chunk_members = chunk_members(
                &ids,
                settings,
                &development.plan_identity,
                &conditions,
                &candidates,
            )?;
            let table = search::replay_table(
                settings,
                role,
                window,
                &development.instrument,
                &chunk_members,
                settings.account.initial_cash,
            );
            let published =
                replay::publish(&chunk_config(config, table), local, destination, true)?;
            let summary = published.summary;
            let events = chunk_events(destination, &published.manifest)?;
            let splits = search::project_splits(events.iter().cloned(), &currency);
            for (id, _, _) in &chunk_members {
                let index: usize = id[1..].parse().expect("member id");
                let group = summary.strategies.get(id).cloned().unwrap_or_default();
                let series = settled_profits(&events, id, settings.account.scale);
                profits.insert((id.clone(), role.to_string()), series);
                match role {
                    DatasetRole::Development => {
                        members[index].rejected =
                            search::gate(&group, &currency, &settings.gates).err();
                        members[index].development = Some(group);
                    }
                    _ => {
                        members[index].evaluation_splits =
                            splits.get(id).cloned().unwrap_or_default();
                        members[index].evaluation = Some(group);
                    }
                }
            }
            chunks.push(ChunkRef {
                role: role.to_string(),
                generation: published.manifest.generation.clone(),
                summary_identity: published.manifest.summary_identity.clone(),
                bindings: chunk_members.iter().map(|(id, _, _)| id.clone()).collect(),
            });
        }
        Ok(())
    };
    run_chunks(
        DatasetRole::Development,
        &settings.development,
        &survivors,
        &mut members,
        &mut chunks,
        &mut profits,
    )?;
    let mut passed: Vec<usize> = survivors
        .iter()
        .copied()
        .filter(|&index| members[index].rejected.is_none())
        .collect();
    passed.sort_by(|&a, &b| {
        search::rank_order(
            (
                members[a].development.as_ref().expect("replayed"),
                &members[a].logic_identity,
                &members[a].contract,
            ),
            (
                members[b].development.as_ref().expect("replayed"),
                &members[b].logic_identity,
                &members[b].contract,
            ),
            &currency,
        )
    });
    for (rank, &index) in passed.iter().enumerate() {
        members[index].rank = Some(rank as u32 + 1);
    }
    // The development result is complete here; only now may evaluation objects be read.
    let mut inputs = vec![development.input.clone()];
    let evaluated: Vec<usize> = passed.to_vec();
    if let Some(window) = &settings.evaluation {
        inputs.push(bind_evaluation(settings, &development)?);
        let mut ordered = evaluated.clone();
        ordered.sort_unstable();
        run_chunks(
            DatasetRole::Evaluation,
            window,
            &ordered,
            &mut members,
            &mut chunks,
            &mut profits,
        )?;
    }
    clock.replay = replay_started.elapsed();

    // 6. Resample every passing member's settlement paths through the retained primitive.
    let stability_started = Instant::now();
    for &index in &passed {
        for role in [DatasetRole::Development, DatasetRole::Evaluation] {
            let Some(series) = profits.get(&(format!("m{index}"), role.to_string())) else {
                continue;
            };
            let outcome = resample(
                &backend,
                settings,
                &members[index],
                role,
                series,
                &mut clock,
            )?;
            members[index].stability.insert(role.to_string(), outcome);
        }
    }
    clock.stability = stability_started.elapsed();

    // 7. Publish the family, then its manifest, and verify before it becomes ready.
    let publishing = Instant::now();
    let family = Family {
        search: settings.clone(),
        plan_identity: development.plan_identity.clone(),
        base_stream: settings.base_stream,
        kernel_module: kernel_identity(),
        sampler: SAMPLER_VERSION.to_string(),
        applicable: applicable.len() as u64,
        members,
        lowering: lowering_ref,
        chunks,
    };
    let generation = family_generation_id(&config.content_hash(), CODE_REVISION, &inputs);
    let key = manifest_key(&generation);
    let temporary = import::temporary_path(local, &format!("family-{generation}"))?;
    fs::write(&temporary, family.to_json())
        .map_err(|error| format!("cannot write {}: {error}", temporary.display()))?;
    let identity = store::identify(&temporary)?;
    let mut object = import::record(ObjectRole::Normalized, FAMILY_OBJECT_PATH, &identity);
    local.put_new(&object.key, &temporary, &identity)?;
    let put = destination.put_new(&object.key, &temporary, &identity)?;
    object.crc32c = put.object().crc32c;
    object.generation = put.object().generation;
    fs::remove_file(&temporary)
        .map_err(|error| format!("cannot remove {}: {error}", temporary.display()))?;
    let manifest = FamilyManifest {
        kind: search::FAMILY_MANIFEST_KIND.to_string(),
        schema_version: search::FAMILY_SCHEMA_VERSION,
        generation: generation.clone(),
        config_hash: config.content_hash(),
        code_revision: CODE_REVISION.to_string(),
        inputs,
        members: family.members.len() as u64,
        objects: vec![object],
    };
    let committed = match destination.head(&key)? {
        Some(_) => {
            let mut bytes = Vec::new();
            destination.read_to(&key, None, &mut bytes)?;
            let committed = FamilyManifest::from_json(&bytes)
                .map_err(|error| format!("{}: {error}", destination.uri(&key)))?;
            if !(committed.generation == manifest.generation
                && committed.inputs == manifest.inputs
                && committed.members == manifest.members
                && import::same_objects(&committed.objects, &manifest.objects, &[identity]))
            {
                return Err(format!(
                    "{} records a different generation, inputs, member count, or family object than this search produced",
                    destination.uri(&key)
                ));
            }
            bytes
        }
        None => manifest.to_json(),
    };
    let uri = destination.uri(&key);
    let verified = verify_family(&uri, destination, &key, &committed)?;
    let temporary = import::temporary_path(local, &format!("manifest-{generation}"))?;
    fs::write(&temporary, &committed)
        .map_err(|error| format!("cannot write {}: {error}", temporary.display()))?;
    let identity = store::identify(&temporary)?;
    let put = destination.put_new(&key, &temporary, &identity)?;
    local.put_new(&key, &temporary, &identity)?;
    fs::remove_file(&temporary)
        .map_err(|error| format!("cannot remove {}: {error}", temporary.display()))?;
    clock.publish = publishing.elapsed();
    let screened = family
        .members
        .iter()
        .filter(|member| member.screened.is_some())
        .count();
    let report = format!(
        "search {} generation {generation} members {} applicable {} screened {screened} replayed {} passed {} evaluated {} objects 1",
        settings.scope,
        family.members.len(),
        family.applicable,
        survivors.len(),
        passed.len(),
        if settings.evaluation.is_some() {
            evaluated.len()
        } else {
            0
        }
    );
    let line = match put {
        Put::Reused(_) => format!("{report} (already published)"),
        Put::Created(_) => format!(
            "{report} [load {:.3}s lowering {:.3}s device upload {:.3}s execute {:.3}s download {:.3}s bytes {} replay {:.3}s stability {:.3}s publish {:.3}s] peak_rss_kb {}",
            clock.load.as_secs_f64(),
            clock.lowering.as_secs_f64(),
            clock.device.upload.as_secs_f64(),
            clock.device.execute.as_secs_f64(),
            clock.device.download.as_secs_f64(),
            clock.device.allocated_bytes,
            clock.replay.as_secs_f64(),
            clock.stability.as_secs_f64(),
            clock.publish.as_secs_f64(),
            peak_rss_kb()
        ),
    };
    Ok(format!("{line}\n{verified}"))
}

/// The configuration of one synthesized replay: only the schema, run mode, storage, and that
/// table, so its hash and generation depend on nothing else.
fn chunk_config(config: &Config, table: binary_alpha_engine::config::Replay) -> Config {
    Config {
        import: None,
        instruments: Vec::new(),
        features: None,
        outcomes: None,
        replay: Some(table),
        accelerator: None,
        search: None,
        ..config.clone()
    }
}

/// Binds the development input: the plan identity, instrument, and the outcome generation.
fn bind_development(settings: &Search) -> Result<Development, String> {
    let input = &settings.development.inputs[0];
    let field = |name: &str| format!("development.inputs[0].{name}");
    let bound = bind_inputs(
        &field,
        DatasetRole::Development,
        &input.tick_manifest,
        &input.feature_manifest,
        "a search",
    )?;
    let uri = input
        .outcome_manifest
        .as_ref()
        .ok_or_else(|| {
            format!(
                "{}: a search requires the outcome generation",
                field("outcome_manifest")
            )
        })?
        .to_string();
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
    if outcome.key() != outcome_key
        || outcome.role != DatasetRole::Development
        || outcome.tick_generation != bound.tick.generation
        || outcome.feature_generation != bound.feature.generation
    {
        return Err(format!(
            "{}: {uri} does not label this input's development tick and feature generations",
            field("outcome_manifest")
        ));
    }
    Ok(Development {
        plan_identity: bound.plan.identity(),
        instrument: bound.tick.instrument.clone(),
        base_stream: settings.base_stream,
        input: FamilyInput {
            role: DatasetRole::Development.to_string(),
            instrument: bound.tick.instrument.clone(),
            tick_generation: bound.tick.generation.clone(),
            feature_generation: bound.feature.generation.clone(),
            plan_identity: bound.plan.identity(),
            outcome_generation: Some(outcome.generation.clone()),
        },
        outcome,
        outcome_store,
    })
}

/// Binds the evaluation input after the development result is frozen: the same instrument
/// applying the development plan unchanged.
fn bind_evaluation(settings: &Search, development: &Development) -> Result<FamilyInput, String> {
    let window = settings.evaluation.as_ref().expect("configured");
    let input = &window.inputs[0];
    let field = |name: &str| format!("evaluation.inputs[0].{name}");
    let bound = bind_inputs(
        &field,
        DatasetRole::Evaluation,
        &input.tick_manifest,
        &input.feature_manifest,
        "a search",
    )?;
    if bound.tick.instrument != development.instrument {
        return Err(format!(
            "{}: instrument {} is not the development instrument {}",
            field("tick_manifest"),
            bound.tick.instrument,
            development.instrument
        ));
    }
    if bound.feature.frozen_from.as_deref() != Some(development.input.feature_generation.as_str())
        || bound.feature.plan_identity != development.plan_identity
    {
        return Err(format!(
            "{}: feature generation {} does not apply the development plan {} frozen by generation {}",
            field("feature_manifest"),
            bound.feature.generation,
            development.plan_identity,
            development.input.feature_generation
        ));
    }
    Ok(FamilyInput {
        role: DatasetRole::Evaluation.to_string(),
        instrument: bound.tick.instrument.clone(),
        tick_generation: bound.tick.generation.clone(),
        feature_generation: bound.feature.generation.clone(),
        plan_identity: bound.plan.identity(),
        outcome_generation: None,
    })
}

/// The bytes of one stored object of a generation, verified against its record.
fn read_object(store: &Store, objects: &[ObjectRecord], path: &str) -> Result<Vec<u8>, String> {
    let object = objects
        .iter()
        .find(|object| object.path == path)
        .ok_or_else(|| format!("the generation lists no `{path}`"))?;
    let (_, local) = verify::fetch(store, object, true)?;
    let local = local.expect("decoded objects have a local path");
    fs::read(&local.path).map_err(|error| format!("cannot read {}: {error}", local.path.display()))
}

/// The base stream's reference rows of the outcome generation.
fn read_references(development: &Development) -> Result<Vec<i64>, String> {
    let base = development.base_stream();
    development
        .outcome
        .streams
        .iter()
        .find(|stream| {
            stream.duration_seconds == base.duration_seconds
                && stream.offset_seconds == base.offset_seconds
        })
        .ok_or_else(|| {
            format!(
                "base_stream: outcome generation {} labels no stream {base}",
                development.outcome.generation
            )
        })?;
    let paths = stream_object_paths(base.duration_seconds, base.offset_seconds);
    from_le_bytes(
        &read_object(
            &development.outcome_store,
            &development.outcome.objects,
            &paths[0],
        )?,
        i64::from_le_bytes,
    )
}

impl Development {
    fn base_stream(&self) -> binary_alpha_engine::config::StreamKey {
        self.base_stream
    }
}

/// The outcome builder over the outcome generation's stored tick arrays.
fn outcome_builder(development: &Development) -> Result<OutcomeBuilder, String> {
    let objects = &development.outcome.objects;
    let times = from_le_bytes(
        &read_object(&development.outcome_store, objects, TICK_TIME_OBJECT_PATH)?,
        i64::from_le_bytes,
    )?;
    let prices = from_le_bytes(
        &read_object(&development.outcome_store, objects, TICK_PRICE_OBJECT_PATH)?,
        i64::from_le_bytes,
    )?;
    OutcomeBuilder::new(development.outcome.rule.clone(), times, prices)
}

/// The device rows of one expiry column: a row without an entry tick is masked out
/// (`decision_ms = i64::MIN`); otherwise the entry tick time, the settlement tick time when
/// valid and the nominal due time otherwise, and the outcome flags. The kernel only compares
/// its clock arguments, so they carry the original microsecond times under their retained
/// `_ms` ABI names.
fn device_rows(
    development: &Development,
    builder: &OutcomeBuilder,
    references: &[i64],
    column: usize,
) -> Result<DeviceRows, String> {
    let objects = &development.outcome.objects;
    let paths = stream_object_paths(
        development.base_stream().duration_seconds,
        development.base_stream().offset_seconds,
    );
    let entries = from_le_bytes(
        &read_object(&development.outcome_store, objects, &paths[1])?,
        u32::from_le_bytes,
    )?;
    let settlements = from_le_bytes(
        &read_object(&development.outcome_store, objects, &paths[2])?,
        u32::from_le_bytes,
    )?;
    let reasons = read_object(&development.outcome_store, objects, &paths[3])?;
    let expiries = development.outcome.rule.expiry_seconds.len();
    let n = references.len();
    if entries.len() != n || settlements.len() != n * expiries || reasons.len() != n * expiries {
        return Err(format!(
            "outcome generation {} stores {} entries, {} settlements, and {} reasons for {n} rows and {expiries} expiries",
            development.outcome.generation,
            entries.len(),
            settlements.len(),
            reasons.len()
        ));
    }
    let mut rows = DeviceRows {
        decision_ms: vec![i64::MIN; n],
        release_ms: vec![0; n],
        valid: vec![0; n],
        buy_win: vec![0; n],
        sell_win: vec![0; n],
        tie: vec![0; n],
    };
    for row in 0..n {
        let cell = builder.cell(
            entries[row],
            column,
            settlements[row * expiries + column],
            reasons[row * expiries + column],
        )?;
        let Some(entry) = cell.entry else { continue };
        rows.decision_ms[row] = entry.event_time_micros;
        let due = cell.due_time_micros.expect("an entry has a due time");
        rows.release_ms[row] = match (cell.reason, cell.settlement) {
            (InvalidReason::Valid, Some(settlement)) => settlement.event_time_micros,
            _ => due,
        };
        if cell.reason == InvalidReason::Valid {
            rows.valid[row] = 1;
            match cell.outcome.expect("a valid cell has an outcome") {
                Label::BuyWin => rows.buy_win[row] = 1,
                Label::SellWin => rows.sell_win[row] = 1,
                Label::Tie => rows.tie[row] = 1,
            }
        }
    }
    Ok(rows)
}

/// The ledger records of one published replay generation.
fn chunk_events(store: &Store, manifest: &ReplayManifest) -> Result<Vec<FinancialEvent>, String> {
    read_object(store, &manifest.objects, EVENTS_OBJECT_PATH)?
        .split(|&byte| byte == b'\n')
        .filter(|line| !line.is_empty())
        .map(FinancialEvent::from_line)
        .collect()
}

/// One binding's settlement profits in ledger order as account units.
fn settled_profits(events: &[FinancialEvent], binding: &str, scale: u8) -> Vec<f64> {
    let prefix = format!("{binding}/");
    events
        .iter()
        .filter_map(|event| match &event.kind {
            EventKind::Settled {
                command, profit, ..
            } if command.starts_with(&prefix) => Some(
                profit
                    .rescale(scale)
                    .map(|profit| profit.coefficient() as f64 / 10_f64.powi(i32::from(scale))),
            ),
            _ => None,
        })
        .map(|profit| profit.expect("a posted profit rescales to its account scale"))
        .collect()
}

/// Draws the member's paths and evaluates them through the retained primitive.
fn resample(
    backend: &Backend,
    settings: &Search,
    member: &Member,
    role: DatasetRole,
    series: &[f64],
    clock: &mut Clock,
) -> Result<StabilityOutcome, String> {
    let n = series.len();
    let horizon = settings.stability.rolling_horizon;
    if n < 2 {
        return Ok(StabilityOutcome::Unavailable {
            reason: format!("{n} settlements; at least two are required"),
        });
    }
    if horizon as usize > n {
        return Ok(StabilityOutcome::Unavailable {
            reason: format!("rolling horizon {horizon} exceeds {n} settlements"),
        });
    }
    if series.iter().any(|value| !value.is_finite()) {
        return Ok(StabilityOutcome::Unavailable {
            reason: "a settlement profit is not finite".to_string(),
        });
    }
    let stratum = format!("{}/{}/{role}", member.logic_identity, member.contract);
    let simulations = settings.stability.simulations;
    let mut paths = Vec::with_capacity(simulations as usize * n);
    for replicate in 0..simulations {
        for index in search::block_path(
            settings.seed,
            &stratum,
            replicate,
            n,
            settings.stability.block_length,
        ) {
            paths.push(series[index]);
        }
    }
    let measured = bootstrap::bootstrap_path_metrics(
        backend,
        &paths,
        simulations as i32,
        n as i32,
        horizon as i32,
    )?;
    add(&mut clock.device, measured.timings);
    Ok(StabilityOutcome::Available(search::stability(
        n as u64,
        settings.stability.block_length,
        horizon,
        &measured.output.max_drawdowns,
        &measured.output.longest_underwater,
        &measured.output.negative_rolling,
    )))
}

/// The identity of the retained kernel sources this binary carries, on every backend.
fn kernel_identity() -> String {
    let mut hasher = Sha256::new();
    for (name, source) in KERNEL_SOURCES {
        hasher.update(name.as_bytes());
        hasher.update(b"\n");
        hasher.update(source.as_bytes());
        hasher.update(b"\n");
    }
    binary_alpha_engine::hex(&hasher.finalize())
}

fn peak_rss_kb() -> u64 {
    fs::read_to_string("/proc/self/status")
        .ok()
        .and_then(|status| {
            status
                .lines()
                .find_map(|line| line.strip_prefix("VmHWM:"))
                .and_then(|value| value.trim().trim_end_matches(" kB").parse().ok())
        })
        .unwrap_or(0)
}

/// The 0/1 code of every distinct condition at every reference row, from the lowering
/// replay's `signal` records: binding `c{condition}` at a base close marks that row.
fn lowering_codes(
    events: &[FinancialEvent],
    references: &[i64],
    conditions: usize,
) -> Result<Vec<i16>, String> {
    let row_of: BTreeMap<i64, usize> = references
        .iter()
        .enumerate()
        .map(|(row, &close)| (close, row))
        .collect();
    let mut codes = vec![0_i16; conditions * references.len()];
    for event in events {
        if let EventKind::Signal {
            binding,
            close_time_micros,
            ..
        } = &event.kind
        {
            let condition: usize = binding
                .strip_prefix('c')
                .and_then(|index| index.parse().ok())
                .filter(|&index| index < conditions)
                .ok_or_else(|| format!("lowering binding `{binding}` is not a condition"))?;
            let row = *row_of.get(close_time_micros).ok_or_else(|| {
                format!(
                    "lowering signal at {close_time_micros} is not a reference row of the outcome generation"
                )
            })?;
            codes[condition * references.len() + row] = 1;
        }
    }
    Ok(codes)
}

/// The raw diagnostic counts of every member in canonical order: one basic dual kernel launch
/// per distinct contract duration over every candidate.
#[allow(clippy::too_many_arguments)]
fn score_members(
    backend: &Backend,
    settings: &Search,
    development: &Development,
    candidates: &[search::Candidate],
    expiries: &[usize],
    codes: &[i16],
    references: &[i64],
    clock: &mut Clock,
) -> Result<Vec<RawCounts>, String> {
    let (window_start, window_end) = (
        binary_alpha_engine::market::parse_event_time_micros(&settings.development.decision_start)?,
        binary_alpha_engine::market::parse_event_time_micros(&settings.development.decision_end)?,
    );
    let mask: Vec<u8> = references
        .iter()
        .map(|&close| u8::from(window_start <= close && close < window_end))
        .collect();
    let builder = outcome_builder(development)?;
    let mut condition_feature = Vec::new();
    let mut offsets = vec![0_i32];
    for candidate in candidates {
        condition_feature.extend(candidate.conditions.iter().map(|&index| index as i32));
        offsets.push(condition_feature.len() as i32);
    }
    let condition_bucket = vec![1_i16; condition_feature.len()];
    let mut scored: BTreeMap<usize, kernels::DualScores> = BTreeMap::new();
    for (contract_index, &column) in expiries.iter().enumerate() {
        if scored.contains_key(&column) {
            continue;
        }
        let rows = device_rows(development, &builder, references, column)?;
        let mut split_mask = mask.clone();
        for (row, &decision) in rows.decision_ms.iter().enumerate() {
            if decision == i64::MIN {
                split_mask[row] = 0;
            }
        }
        let mut ordered: Vec<i64> = (0..references.len() as i64).collect();
        ordered.sort_by_key(|&row| (rows.decision_ms[row as usize], row));
        let measured = kernels::score_bucket_plans_cap1_basic_dual(
            backend,
            codes,
            &condition_feature,
            &condition_bucket,
            &offsets,
            &split_mask,
            &ordered,
            &rows.decision_ms,
            &rows.release_ms,
            &rows.valid,
            &rows.buy_win,
            &rows.sell_win,
            &rows.tie,
            candidates.len() as i32,
            references.len() as i32,
            settings.contracts[contract_index].duration_micros,
            0,
        )?;
        add(&mut clock.device, measured.timings);
        scored.insert(column, measured.output);
    }
    let contracts = settings.contracts.len();
    Ok((0..candidates.len() * contracts)
        .map(|index| {
            let (candidate, contract_index) = (index / contracts, index % contracts);
            let output = &scored[&expiries[contract_index]];
            let columns = match settings.contracts[contract_index].direction {
                binary_alpha_engine::execution::Direction::Buy => &output.buy_output,
                binary_alpha_engine::execution::Direction::Sell => &output.sell_output,
            };
            let at = |column: usize| columns[candidate * 8 + column];
            RawCounts {
                total: at(0),
                wins: at(1),
                losses: at(2),
                ties: at(3),
                invalid: at(4),
            }
        })
        .collect())
}

/// The expiry column of every configured contract in the bound outcome generation.
fn expiry_columns(settings: &Search, development: &Development) -> Result<Vec<usize>, String> {
    settings
        .contracts
        .iter()
        .enumerate()
        .map(|(index, contract)| {
            let seconds = contract.duration_micros / 1_000_000;
            if contract.duration_micros % 1_000_000 != 0 {
                return Err(format!(
                    "contracts[{index}].duration_micros: {} is not a whole number of seconds",
                    contract.duration_micros
                ));
            }
            development
                .outcome
                .rule
                .expiry_seconds
                .iter()
                .position(|&expiry| i64::from(expiry) == seconds)
                .ok_or_else(|| {
                    format!(
                        "contracts[{index}].duration_micros: {seconds} s is not an expiry of outcome generation {}",
                        development.outcome.generation
                    )
                })
        })
        .collect()
}

/// The chunk members `(id, strategy, contract index)` of the recorded binding ids.
fn chunk_members(
    bindings: &[String],
    settings: &Search,
    plan_identity: &str,
    conditions: &[binary_alpha_engine::execution::Condition],
    candidates: &[search::Candidate],
) -> Result<Vec<ChunkMember>, String> {
    let contracts = settings.contracts.len();
    bindings
        .iter()
        .map(|id| {
            let index: usize = id
                .strip_prefix('m')
                .and_then(|index| index.parse().ok())
                .filter(|&index| index < candidates.len() * contracts)
                .ok_or_else(|| format!("chunk binding `{id}` is not a member"))?;
            Ok((
                id.clone(),
                search::strategy(
                    id,
                    plan_identity,
                    settings.base_stream,
                    conditions,
                    &candidates[index / contracts].conditions,
                ),
                index % contracts,
            ))
        })
        .collect()
}

/// The run definition a published replay generation restored from: its first ledger record.
fn restored_definition(
    events: &[FinancialEvent],
) -> Result<binary_alpha_engine::execution::RunDefinition, String> {
    match events.first().map(|event| &event.kind) {
        Some(EventKind::RunDefinition { definition }) => Ok((**definition).clone()),
        _ => Err("the ledger does not begin with its run definition".to_string()),
    }
}

/// Verifies a family generation: the manifest and its content-addressed object, the complete
/// member set re-enumerated from the recorded search table, every referenced replay through the
/// replay verifier, each member's groups against those verified summaries and the shared
/// ledger projection, and every recomputed score, adjustment, screen decision, gate, and rank.
pub fn verify_family(uri: &str, store: &Store, key: &str, bytes: &[u8]) -> Result<String, String> {
    let manifest = FamilyManifest::from_json(bytes).map_err(|error| format!("{uri}: {error}"))?;
    if manifest.key() != key {
        return Err(format!(
            "{uri} holds the manifest of generation {}",
            manifest.generation
        ));
    }
    let (bytes_read, _) = verify::fetch(store, &manifest.objects[0], false)?;
    let family = Family::from_json(&read_object(store, &manifest.objects, FAMILY_OBJECT_PATH)?)
        .map_err(|error| format!("{uri}: {FAMILY_OBJECT_PATH}: {error}"))?;
    if family.members.len() as u64 != manifest.members {
        return Err(format!(
            "{uri}: the manifest records {} members but the family holds {}",
            manifest.members,
            family.members.len()
        ));
    }
    let settings = &family.search;
    settings
        .validate()
        .map_err(|reason| format!("{uri}: search.{reason}"))?;
    let conditions = search::conditions(&settings.conditions);
    let candidates = search::candidates(
        &family.plan_identity,
        family.base_stream,
        &conditions,
        settings.min_conditions as usize,
        settings.max_conditions as usize,
    );
    let contracts = settings.contracts.len();
    if family.members.len() != candidates.len() * contracts {
        return Err(format!(
            "{uri}: the family holds {} members, but the search table enumerates {}",
            family.members.len(),
            candidates.len() * contracts
        ));
    }
    let scores: Vec<Option<f64>> = family
        .members
        .iter()
        .enumerate()
        .map(|(index, member)| {
            let candidate = &candidates[index / contracts];
            let contract = &settings.contracts[index % contracts];
            let expected: Vec<_> = candidate
                .conditions
                .iter()
                .map(|&i| conditions[i].clone())
                .collect();
            if member.logic_identity != candidate.logic_identity
                || member.conditions != expected
                || member.contract != contract.id
            {
                return Err(format!(
                    "{uri}: member {index} is not the enumerated member"
                ));
            }
            let (null, score) = match search::null_rate(contract) {
                Ok(null) => {
                    let score = search::upper_tail(
                        member.raw.wins as u64,
                        member.raw.losses as u64,
                        null.break_even,
                    );
                    (Some(null), Some(score))
                }
                Err(_) => (None, None),
            };
            if member.null != null
                || member.score != score
                || member.inapplicable.is_some() != score.is_none()
            {
                return Err(format!(
                    "{uri}: member {index} records a score its contract and counts do not produce"
                ));
            }
            Ok(score)
        })
        .collect::<Result<_, _>>()?;
    let applicable: Vec<usize> = (0..scores.len()).filter(|&i| scores[i].is_some()).collect();
    if family.applicable != applicable.len() as u64 {
        return Err(format!(
            "{uri}: the applicable count {} is not {}",
            family.applicable,
            applicable.len()
        ));
    }
    let adjusted = search::benjamini_hochberg(
        &applicable
            .iter()
            .map(|&i| scores[i].expect("applicable"))
            .collect::<Vec<_>>(),
    );
    for (&index, &value) in applicable.iter().zip(&adjusted) {
        if family.members[index].adjusted != Some(value) {
            return Err(format!(
                "{uri}: member {index} records an adjusted score the family does not produce"
            ));
        }
    }
    // Screen decisions, gates, and ranks follow from the recorded groups and settings.
    let currency = settings.account.currency.to_string();
    let mut screened = 0;
    if let Some(screen) = &settings.screen {
        let mut order = applicable.clone();
        order.sort_by(|&a, &b| {
            family.members[a]
                .adjusted
                .partial_cmp(&family.members[b].adjusted)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then(a.cmp(&b))
        });
        for (index, member) in family.members.iter().enumerate() {
            let position = order.iter().position(|&i| i == index);
            let expected = member.inapplicable.is_some()
                || member
                    .adjusted
                    .is_some_and(|value| value > screen.max_adjusted_score)
                || position
                    .is_some_and(|position| screen.top.is_some_and(|top| position >= top as usize));
            if member.screened.is_some() != expected {
                return Err(format!(
                    "{uri}: member {index} records a screen decision the settings do not produce"
                ));
            }
        }
        screened = family
            .members
            .iter()
            .filter(|m| m.screened.is_some())
            .count();
    } else if family
        .members
        .iter()
        .any(|member| member.screened.is_some())
    {
        return Err(format!("{uri}: exhaustive scope screens nothing"));
    }
    let mut passed = Vec::new();
    for (index, member) in family.members.iter().enumerate() {
        match (&member.screened, &member.development) {
            (Some(_), None) => {}
            (None, Some(group)) => {
                let rejected = search::gate(group, &currency, &settings.gates).err();
                if member.rejected != rejected {
                    return Err(format!(
                        "{uri}: member {index} records a gate outcome its group does not produce"
                    ));
                }
                if rejected.is_none() {
                    passed.push(index);
                }
            }
            _ => {
                return Err(format!(
                    "{uri}: member {index} is both screened and replayed, or neither"
                ));
            }
        }
    }
    passed.sort_by(|&a, &b| {
        let (x, y) = (&family.members[a], &family.members[b]);
        search::rank_order(
            (
                x.development.as_ref().expect("replayed"),
                &x.logic_identity,
                &x.contract,
            ),
            (
                y.development.as_ref().expect("replayed"),
                &y.logic_identity,
                &y.contract,
            ),
            &currency,
        )
    });
    for (index, member) in family.members.iter().enumerate() {
        let rank = passed
            .iter()
            .position(|&i| i == index)
            .map(|rank| rank as u32 + 1);
        if member.rank != rank {
            return Err(format!(
                "{uri}: member {index} records a rank the ranking does not produce"
            ));
        }
    }
    // Every referenced replay restores through the replay verifier, its restored definition is
    // the table synthesized for the recorded members, and every group, split group, raw count,
    // and stability result is recomputed from the verified records.
    let development = bind_development(settings)?;
    if development.plan_identity != family.plan_identity {
        return Err(format!(
            "{uri}: the bound development plan {} is not the recorded {}",
            development.plan_identity, family.plan_identity
        ));
    }
    let mut clock = Clock::default();
    let read_chunk = |chunk: &ChunkRef| -> Result<(ReplayManifest, Vec<FinancialEvent>), String> {
        let chunk_key = manifest_key(&chunk.generation);
        let mut bytes = Vec::new();
        store.read_to(&chunk_key, None, &mut bytes)?;
        let chunk_uri = store.uri(&chunk_key);
        replay::verify_replay(&chunk_uri, store, &chunk_key, &bytes)?;
        let manifest =
            ReplayManifest::from_json(&bytes).map_err(|error| format!("{chunk_uri}: {error}"))?;
        if manifest.summary_identity != chunk.summary_identity
            || manifest.role.to_string() != chunk.role
        {
            return Err(format!("{uri}: {chunk_uri} is not the recorded chunk"));
        }
        let events = chunk_events(store, &manifest)?;
        Ok((manifest, events))
    };
    let (_, lowering_events) = read_chunk(&family.lowering)?;
    let expected_lowering =
        search::lowering_replay(settings, &family.plan_identity, &development.instrument);
    if restored_definition(&lowering_events)?.replay != expected_lowering
        || family.lowering.bindings
            != expected_lowering
                .strategies
                .iter()
                .map(|s| s.id.clone())
                .collect::<Vec<_>>()
    {
        return Err(format!(
            "{uri}: the lowering replay is not the table synthesized for this family"
        ));
    }
    let references = read_references(&development)?;
    let codes = lowering_codes(&lowering_events, &references, conditions.len())?;
    let expiries = expiry_columns(settings, &development)?;
    let raw = score_members(
        &Backend::Cpu,
        settings,
        &development,
        &candidates,
        &expiries,
        &codes,
        &references,
        &mut clock,
    )?;
    for (index, member) in family.members.iter().enumerate() {
        if member.raw != raw[index] {
            return Err(format!(
                "{uri}: member {index} records raw counts the lowering records and outcomes do not produce"
            ));
        }
    }
    let mut replayed = 0;
    let mut seen: BTreeMap<(String, String), ()> = BTreeMap::new();
    for chunk in &family.chunks {
        let (manifest, events) = read_chunk(chunk)?;
        let members = chunk_members(
            &chunk.bindings,
            settings,
            &family.plan_identity,
            &conditions,
            &candidates,
        )?;
        let (role, window) = match chunk.role.as_str() {
            "development" => (DatasetRole::Development, &settings.development),
            "evaluation" => (
                DatasetRole::Evaluation,
                settings.evaluation.as_ref().ok_or_else(|| {
                    format!("{uri}: an evaluation chunk without an evaluation window")
                })?,
            ),
            other => return Err(format!("{uri}: chunk role `{other}`")),
        };
        let expected = search::replay_table(
            settings,
            role,
            window,
            &development.instrument,
            &members,
            settings.account.initial_cash,
        );
        let definition = restored_definition(&events)?;
        if definition.replay != expected {
            return Err(format!(
                "{uri}: chunk {} is not the table synthesized for its recorded members",
                chunk.generation
            ));
        }
        let summary =
            Summary::from_json(&read_object(store, &manifest.objects, SUMMARY_OBJECT_PATH)?)
                .map_err(|error| format!("{uri}: {}: {error}", chunk.generation))?;
        let splits = search::project_splits(events.iter().cloned(), &currency);
        for (id, _, _) in &members {
            let index: usize = id[1..].parse().expect("checked");
            if seen.insert((id.clone(), chunk.role.clone()), ()).is_some() {
                return Err(format!(
                    "{uri}: member {index} is replayed twice for {}",
                    chunk.role
                ));
            }
            let member = &family.members[index];
            let group = summary.strategies.get(id).cloned().unwrap_or_default();
            let matches = match role {
                DatasetRole::Development => {
                    replayed += 1;
                    member.screened.is_none() && member.development.as_ref() == Some(&group)
                }
                _ => {
                    member.rejected.is_none()
                        && member.evaluation.as_ref() == Some(&group)
                        && member.evaluation_splits == splits.get(id).cloned().unwrap_or_default()
                }
            };
            if !matches {
                return Err(format!(
                    "{uri}: member {index} records {} groups its replay {} does not hold",
                    chunk.role, chunk.generation
                ));
            }
            if member.rejected.is_none() && member.screened.is_none() {
                let series = settled_profits(&events, id, settings.account.scale);
                let expected =
                    resample(&Backend::Cpu, settings, member, role, &series, &mut clock)?;
                if member.stability.get(&chunk.role) != Some(&expected) {
                    return Err(format!(
                        "{uri}: member {index} records {} stability its settlements do not produce",
                        chunk.role
                    ));
                }
            }
        }
    }
    for (index, member) in family.members.iter().enumerate() {
        let expected_roles: Vec<String> = match (&member.screened, &member.rejected) {
            (None, None) => {
                let mut roles = vec!["development".to_string()];
                if settings.evaluation.is_some() {
                    roles.push("evaluation".to_string());
                }
                roles
            }
            _ => Vec::new(),
        };
        if member.stability.keys().cloned().collect::<Vec<_>>() != expected_roles
            || (member.screened.is_some() && member.development.is_some())
            || (member.rejected.is_some() && member.evaluation.is_some())
            || expected_roles
                .iter()
                .any(|role| !seen.contains_key(&(format!("m{index}"), role.clone())))
        {
            return Err(format!(
                "{uri}: member {index} is not replayed and resampled exactly as its status requires"
            ));
        }
    }
    let _ = screened;
    Ok(format!(
        "verified search generation {} members {} applicable {} replayed {replayed} passed {} objects 1 bytes {bytes_read}",
        manifest.generation,
        family.members.len(),
        family.applicable,
        passed.len()
    ))
}

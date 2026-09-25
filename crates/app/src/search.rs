//! `binary-alpha search`: resolve one typed candidate family, project fitted labels and lower
//! fallback conditions, stream sparse screening over the complete family, replay retained
//! survivors through the engine in chunks, evaluate the frozen
//! development ranking, resample settlement paths through the retained bootstrap primitive, and
//! publish the family as one immutable generation that `data verify` re-derives.
//!
//! The engine module owns every pure rule; this module owns binding, the replay batches, the
//! device calls, temporary files, publication, and the verifier.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io::Write;
use std::path::Path;
use std::time::{Duration, Instant};

use binary_alpha_accelerator::{
    Backend, KERNEL_SOURCES, SCREEN_KERNEL_SOURCE, Timings, bootstrap, search as kernels,
};
use binary_alpha_engine::config::{Backend as Selected, Config, RunMode, Search};
use binary_alpha_engine::dataset::{DatasetRole, ObjectRecord, ObjectRole, manifest_key};
use binary_alpha_engine::execution::{
    ColumnSpec, EVENTS_OBJECT_PATH, EventKind, FinancialEvent, ReplayManifest, Resolution,
    SUMMARY_OBJECT_PATH, StrategySpec, StreamColumns, Summary, project_fitted_label,
    signal_logic_identity,
};
use binary_alpha_engine::features::Value;
use binary_alpha_engine::outcomes::{
    InvalidReason, MISSING_INDEX, OUTCOME_MANIFEST_KIND, Outcome as Label, OutcomeBuilder,
    OutcomeManifest, TICK_PRICE_OBJECT_PATH, TICK_TIME_OBJECT_PATH, stream_object_paths,
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
use binary_alpha_engine::research::Access;

/// One immutable CPU scoring batch. Global indices bind results independently of worker order.
#[derive(Clone, Copy)]
pub struct CpuBatch<'a> {
    pub global_indices: &'a [u64],
    pub candidates: kernels::CandidateConditions<'a>,
    pub driver_keys: &'a [i32],
}

/// Score independent batches with the existing CPU sparse dual reference, preserving global order.
/// The caller accumulates expiry counts into its compact whole-family records.
pub fn score_cpu_batches(
    tuple: &kernels::CpuSparseTuple<'_>,
    batches: &[CpuBatch<'_>],
    split: usize,
    expiry_ms: i64,
    payout_basis: i64,
) -> Result<Vec<(u64, RawCounts, RawCounts)>, String> {
    let scored = crate::parallel::map(batches, |batch| {
        if batch.global_indices.len() != batch.candidates.candidate_count as usize {
            return Err("CPU batch global index count differs from candidate count".to_string());
        }
        let _ = payout_basis;
        let output = tuple
            .score_screen_batch(split, batch.candidates, batch.driver_keys, expiry_ms)?
            .output;
        Ok(batch
            .global_indices
            .iter()
            .enumerate()
            .map(|(position, &global)| {
                (
                    global,
                    raw_at_basic(&output.buy_output, position),
                    raw_at_basic(&output.sell_output, position),
                )
            })
            .collect::<Vec<_>>())
    });
    let mut merged = Vec::new();
    for batch in scored {
        merged.extend(batch?);
    }
    merged.sort_by_key(|item| item.0);
    if merged.windows(2).any(|pair| pair[0].0 == pair[1].0) {
        return Err("CPU batches repeat a global member index".into());
    }
    Ok(merged)
}

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
type ProjectedRows = BTreeMap<(u32, u32, String), Vec<i16>>;

/// The bound development input: plan, instrument, and the outcome generation's stored rows.
struct Development {
    bound: crate::outcomes::Bound,
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

#[cfg(feature = "cuda")]
struct PackedTile {
    expiries: Vec<usize>,
    bytes: Vec<u8>,
    stride: usize,
}

#[cfg(feature = "cuda")]
fn packed_tiles(
    outcomes: &BTreeMap<usize, DeviceRows>,
    rows: usize,
) -> Result<Vec<PackedTile>, String> {
    for outcome in outcomes.values() {
        if [
            outcome.release_ms.len(),
            outcome.valid.len(),
            outcome.buy_win.len(),
            outcome.sell_win.len(),
            outcome.tie.len(),
        ]
        .into_iter()
        .any(|len| len != rows)
        {
            return Err("packed outcome row count differs".into());
        }
    }
    let distinct: Vec<_> = outcomes.iter().collect();
    distinct
        .chunks(8)
        .map(|chunk| {
            let active = chunk.len();
            let stride = (active * 9).div_ceil(8) * 8;
            let mut bytes = vec![0_u8; rows.checked_mul(stride).ok_or("packed outcomes overflow")?];
            for row in 0..rows {
                for (expiry, (_, outcome)) in chunk.iter().enumerate() {
                    let release = outcome
                        .release_ms
                        .get(row)
                        .ok_or("packed outcome row count differs")?;
                    let at = row * stride + expiry * 8;
                    bytes[at..at + 8].copy_from_slice(&release.to_le_bytes());
                    bytes[row * stride + active * 8 + expiry] = outcome.valid[row]
                        | (outcome.tie[row] << 1)
                        | (outcome.buy_win[row] << 2)
                        | (outcome.sell_win[row] << 3);
                }
            }
            Ok(PackedTile {
                expiries: chunk.iter().map(|(column, _)| **column).collect(),
                bytes,
                stride,
            })
        })
        .collect()
}

#[derive(Default)]
struct SparseBatch {
    globals: Vec<u64>,
    features: Vec<i32>,
    buckets: Vec<i16>,
    offsets: Vec<i32>,
    drivers: Vec<i32>,
}

impl SparseBatch {
    fn candidates(&self) -> kernels::CandidateConditions<'_> {
        kernels::CandidateConditions {
            condition_feature: &self.features,
            condition_bucket: &self.buckets,
            candidate_offsets: &self.offsets,
            candidate_count: self.globals.len() as i32,
        }
    }

    fn sort_by_driver_rank(&mut self) {
        let mut positions: Vec<usize> = (0..self.globals.len()).collect();
        positions
            .sort_unstable_by_key(|&position| (self.drivers[position], self.globals[position]));
        let mut sorted = Self {
            offsets: vec![0],
            ..Self::default()
        };
        for position in positions {
            sorted.globals.push(self.globals[position]);
            sorted.drivers.push(self.drivers[position]);
            let range = self.offsets[position] as usize..self.offsets[position + 1] as usize;
            sorted
                .features
                .extend_from_slice(&self.features[range.clone()]);
            sorted.buckets.extend_from_slice(&self.buckets[range]);
            sorted.offsets.push(sorted.features.len() as i32);
        }
        *self = sorted;
    }
}

fn visit_block_tuples(
    blocks: &[kernels::ColumnBlock],
    suffix_capacity: &[usize],
    remaining: usize,
    first: usize,
    tuple: &mut Vec<usize>,
    visit: &mut impl FnMut(&[usize]) -> Result<(), String>,
) -> Result<(), String> {
    if remaining == 0 {
        return visit(tuple);
    }
    for block in first..blocks.len() {
        let used = tuple.iter().filter(|&&current| current == block).count();
        if used == blocks[block].columns.len() || suffix_capacity[block] - used < remaining {
            continue;
        }
        tuple.push(block);
        visit_block_tuples(blocks, suffix_capacity, remaining - 1, block, tuple, visit)?;
        tuple.pop();
    }
    Ok(())
}

fn visit_tuple_conditions(
    blocks: &[kernels::ColumnBlock],
    tuple: &[usize],
    position: usize,
    first: usize,
    chosen: &mut Vec<usize>,
    visit: &mut impl FnMut(&[usize]) -> Result<(), String>,
) -> Result<(), String> {
    if position == tuple.len() {
        return visit(chosen);
    }
    for condition in blocks[tuple[position]].columns.clone() {
        if condition >= first {
            chosen.push(condition);
            visit_tuple_conditions(blocks, tuple, position + 1, condition + 1, chosen, visit)?;
            chosen.pop();
        }
    }
    Ok(())
}

fn tuple_candidate_index(
    chosen: &[usize],
    condition_count: usize,
    settings: &Search,
) -> Result<usize, String> {
    let global = search::member_rank(
        condition_count,
        settings.min_conditions as usize,
        settings.max_conditions as usize,
        settings.contracts.len(),
        chosen,
        0,
    )
    .ok_or("cannot rank streamed member")?;
    Ok(global as usize / settings.contracts.len())
}

#[allow(clippy::too_many_arguments)]
fn tuple_batches(
    blocks: &[kernels::ColumnBlock],
    tuple: &[usize],
    requested: &[usize],
    buckets: &[i16],
    offsets: &[i32],
    count: usize,
    settings: &Search,
    batch_size: usize,
    expiry_multiplier: usize,
    completed: &[bool],
    driver_visits: &mut usize,
    mut consume: impl FnMut(&[SparseBatch]) -> Result<(), String>,
) -> Result<(), String> {
    const GROUP: usize = 16;
    let mut group = Vec::new();
    let mut batch = SparseBatch {
        offsets: vec![0],
        ..SparseBatch::default()
    };
    visit_tuple_conditions(blocks, tuple, 0, 0, &mut Vec::new(), &mut |chosen| {
        let global = search::member_rank(
            count,
            settings.min_conditions as usize,
            settings.max_conditions as usize,
            settings.contracts.len(),
            chosen,
            0,
        )
        .ok_or("cannot rank streamed member")?;
        if completed[global as usize / settings.contracts.len()] {
            return Ok(());
        }
        batch.globals.push(global);
        let mut driver = None;
        for &condition in chosen {
            let local = requested.binary_search(&condition).expect("tuple column");
            batch.features.push(local as i32);
            batch.buckets.push(buckets[local]);
            let frequency = offsets[local + 1] - offsets[local];
            if driver.is_none_or(|(_, shortest)| frequency < shortest) {
                driver = Some((local as i32, frequency));
            }
        }
        let driver = driver.expect("nonempty member").0;
        *driver_visits +=
            (offsets[driver as usize + 1] - offsets[driver as usize]) as usize * expiry_multiplier;
        batch.drivers.push(driver);
        batch.offsets.push(batch.features.len() as i32);
        if batch.globals.len() == batch_size {
            batch.sort_by_driver_rank();
            group.push(std::mem::replace(
                &mut batch,
                SparseBatch {
                    offsets: vec![0],
                    ..SparseBatch::default()
                },
            ));
            if group.len() == GROUP {
                consume(&group)?;
                group.clear();
            }
        }
        Ok(())
    })?;
    if !batch.globals.is_empty() {
        batch.sort_by_driver_rank();
        group.push(batch);
    }
    if !group.is_empty() {
        consume(&group)?;
    }
    Ok(())
}

/// Process-scoped screening tuning, outside every configuration identity.
fn test_screen_limit(name: &str) -> Result<Option<usize>, String> {
    std::env::var_os(name)
        .map(|value| {
            value
                .into_string()
                .map_err(|_| format!("{name}: expected a positive integer"))?
                .parse::<usize>()
                .ok()
                .filter(|&limit| limit > 0)
                .ok_or_else(|| format!("{name}: expected a positive integer"))
        })
        .transpose()
}

/// Test-only digest of the complete screening population; the published schema-2 object keeps
/// only survivors. Separate raw, BH-order, and survivor digests make CUDA/CPU parity explicit
/// without allocating a large JSON snapshot in the release scale gate.
fn test_screen_digest(compact: &[search::CompactMember], survivors: &[u64]) -> Result<(), String> {
    let Ok(path) = std::env::var("BINARY_ALPHA_TEST_SCREEN_DIGEST") else {
        return Ok(());
    };
    let mut raw = Sha256::new();
    for member in compact {
        for count in [
            member.raw.total,
            member.raw.wins,
            member.raw.losses,
            member.raw.ties,
            member.raw.invalid,
        ] {
            raw.update(count.to_le_bytes());
        }
        raw.update(
            member
                .score
                .map(f64::to_bits)
                .unwrap_or(u64::MAX)
                .to_le_bytes(),
        );
        raw.update(
            member
                .adjusted
                .map(f64::to_bits)
                .unwrap_or(u64::MAX)
                .to_le_bytes(),
        );
    }
    let mut order: Vec<usize> = (0..compact.len())
        .filter(|&index| compact[index].score.is_some())
        .collect();
    order.sort_by(|&left, &right| {
        compact[left]
            .score
            .unwrap()
            .partial_cmp(&compact[right].score.unwrap())
            .unwrap_or(std::cmp::Ordering::Equal)
            .then(left.cmp(&right))
    });
    let mut bh = Sha256::new();
    for index in order {
        bh.update((index as u64).to_le_bytes());
    }
    let mut retained = Sha256::new();
    for &index in survivors {
        retained.update(index.to_le_bytes());
    }
    let digest = serde_json::json!({
        "members": compact.len(),
        "raw_and_adjusted": binary_alpha_engine::hex(&raw.finalize()),
        "bh_order": binary_alpha_engine::hex(&bh.finalize()),
        "survivors": binary_alpha_engine::hex(&retained.finalize()),
        "survivor_count": survivors.len(),
    });
    fs::write(
        &path,
        serde_json::to_vec(&digest).expect("test digest serializes"),
    )
    .map_err(|error| format!("cannot write test screen digest {path}: {error}"))
}

fn screen_compact(
    compact: &mut [search::CompactMember],
    settings: &Search,
) -> Result<(u64, Vec<u64>), String> {
    let result = search::screen_compact(compact, &settings.contracts, settings.screen.as_ref());
    test_screen_digest(compact, &result.1)?;
    Ok(result)
}

/// Wall-clock stages of one run, outside every identity.
#[derive(Default)]
struct Clock {
    #[cfg(feature = "cuda")]
    started: Option<Instant>,
    setup_before_gpu: Duration,
    load: Duration,
    lowering: Duration,
    device: Timings,
    replay: Duration,
    stability: Duration,
    publish: Duration,
    columns: usize,
    blocks: usize,
    tuples: usize,
    list_entries: usize,
    construction_visits: usize,
    validation_visits: usize,
    driver_visits: usize,
    transfer_bytes: usize,
    tuning: String,
    memory: String,
    #[cfg(feature = "cuda")]
    memory_peak_bytes: usize,
}

fn add(total: &mut Timings, measured: Timings) {
    total.upload += measured.upload;
    total.execute += measured.execute;
    total.download += measured.download;
    total.allocated_bytes = total.allocated_bytes.max(measured.allocated_bytes);
}

fn lowered_conditions(
    plan: &binary_alpha_engine::features::FeaturePlan,
    conditions: &[binary_alpha_engine::execution::Condition],
) -> (
    Vec<binary_alpha_engine::execution::Condition>,
    BTreeMap<usize, String>,
) {
    let indices: Vec<usize> = conditions
        .iter()
        .enumerate()
        .filter_map(|(index, condition)| {
            projected_bucket(plan, condition).is_none().then_some(index)
        })
        .collect();
    (
        indices
            .iter()
            .map(|&index| conditions[index].clone())
            .collect(),
        indices
            .iter()
            .enumerate()
            .map(|(local, &global)| (global, format!("c{local}")))
            .collect(),
    )
}

/// One published family generation and the report and verification lines of the command.
pub struct Searched {
    pub generation: String,
    pub(crate) report: String,
}

/// The typed search every caller uses: bind, lower, score, replay, evaluate, resample, publish,
/// and verify one family generation of the configuration's `search` table.
pub fn search(config: &Config, local: &Store, destination: &Store) -> Result<String, String> {
    let declaration = crate::research::declaration(config)?;
    let verified = crate::verification_cache(Some(config));
    family(
        config,
        local,
        destination,
        Access {
            declaration: declaration.as_ref(),
            certification: None,
            verified: Some(&verified),
        },
    )
    .map(|searched| searched.report)
}

/// `search` with its typed result, under the caller's read permit.
pub fn family(
    config: &Config,
    local: &Store,
    destination: &Store,
    access: Access<'_>,
) -> Result<Searched, String> {
    #[cfg(feature = "cuda")]
    let command_started = Instant::now();
    let settings = config
        .search
        .as_ref()
        .ok_or("search: the table is required")?;
    let backends: Vec<Backend> = match config.accelerator.as_ref() {
        Some(section) if section.backend == Selected::Cuda => section
            .devices
            .iter()
            .map(|&device| Backend::cuda(device))
            .collect::<Result<_, _>>()?,
        _ => vec![Backend::Cpu],
    };
    let backend = &backends[0];
    #[cfg(feature = "cuda")]
    let mut clock = Clock {
        started: Some(command_started),
        ..Clock::default()
    };
    #[cfg(not(feature = "cuda"))]
    let mut clock = Clock::default();
    let started = Instant::now();

    // Resolve against the bound fitted plan before allocating a family-sized buffer.
    let development = bind_development(settings, access)?;
    let resolved = search::resolve_conditions(settings, &development.bound.plan)?;
    let conditions = &resolved.conditions;
    clock.columns = conditions.len();
    let total = resolved.members;
    clock.load = started.elapsed();

    let lowering_started = Instant::now();
    let (lowered_conditions, lowered_bindings) =
        lowered_conditions(&development.bound.plan, conditions);
    let (lowering_ref, lowering_events) = if total > 0 && !lowered_conditions.is_empty() {
        let lowering = search::lowering_replay_for(
            settings,
            &development.plan_identity,
            &development.instrument,
            &lowered_conditions,
        );
        let bindings = lowering
            .strategies
            .iter()
            .map(|strategy| strategy.id.clone())
            .collect();
        let published = replay::publish(
            &chunk_config(config, lowering),
            local,
            destination,
            true,
            access,
        )?;
        let events = chunk_events(destination, &published.manifest)?;
        (
            Some(ChunkRef {
                role: DatasetRole::Development.to_string(),
                generation: published.manifest.generation.clone(),
                summary_identity: published.manifest.summary_identity.clone(),
                bindings,
            }),
            events,
        )
    } else {
        (None, Vec::new())
    };
    clock.lowering = lowering_started.elapsed();

    let mut members = Vec::new();
    let applicable = if total == 0 {
        0
    } else {
        let mut compact = score_streamed(
            settings,
            &development,
            conditions,
            &lowering_events,
            &lowered_bindings,
            &backends,
            config
                .accelerator
                .as_ref()
                .filter(|section| section.backend == Selected::Cuda)
                .map(|section| section.devices.as_slice()),
            &mut clock,
        )?;
        let (applicable, survivors) = screen_compact(&mut compact, settings)?;
        for global in survivors {
            let mut member =
                streamed_member(global, settings, &development.plan_identity, conditions)?;
            let contract = &settings.contracts[global as usize % settings.contracts.len()];
            compact[global as usize].apply(&mut member, contract, settings.screen.as_ref());
            members.push(member);
        }
        applicable
    };

    // 5. Replay survivors through the engine in canonical chunks; gate and rank.
    let currency = settings.account.currency.to_string();
    let replay_started = Instant::now();
    let survivors: Vec<usize> = (0..members.len()).collect();
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
            let ids: Vec<String> = chunk
                .iter()
                .map(|&index| format!("m{}", members[index].global_index.expect("schema-2 member")))
                .collect();
            let chunk_members =
                chunk_members_streamed(&ids, settings, &development.plan_identity, conditions)?;
            let table = search::replay_table(
                settings,
                role,
                window,
                &development.instrument,
                &chunk_members,
                settings.account.initial_cash,
            );
            let published = replay::publish(
                &chunk_config(config, table),
                local,
                destination,
                true,
                access,
            )?;
            let summary = published.engine.summary();
            let events = chunk_events(destination, &published.manifest)?;
            let mut splits = if role == DatasetRole::Evaluation {
                search::project_splits(events.iter().cloned(), &currency)
            } else {
                BTreeMap::new()
            };
            for (id, _, _) in &chunk_members {
                let global: u64 = id[1..].parse().expect("member id");
                let index = members
                    .binary_search_by_key(&global, |member| {
                        member.global_index.expect("schema-2 member")
                    })
                    .expect("retained member binding");
                let group = summary.strategies.get(id).cloned().unwrap_or_default();
                profits.insert(
                    (id.clone(), role.to_string()),
                    settled_profits(&events, id, settings.account.scale),
                );
                if role == DatasetRole::Development {
                    members[index].development = Some(group);
                } else {
                    members[index].evaluation_splits = splits.remove(id).unwrap_or_default();
                    members[index].evaluation = Some(group);
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
    let passed = search::rank(&mut members, &settings.gates, &currency);
    // The development result is complete here; only now may evaluation objects be read.
    let mut inputs = vec![development.input.clone()];
    if let Some(window) = settings.evaluation.as_ref().filter(|_| total > 0) {
        inputs.push(bind_evaluation(settings, &development, access)?);
        let mut ordered = passed.clone();
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
            let Some(series) = profits.get(&(
                format!("m{}", members[index].global_index.expect("schema-2 member")),
                role.to_string(),
            )) else {
                continue;
            };
            let outcome = resample(backend, settings, &members[index], role, series, &mut clock)?;
            members[index].stability.insert(role.to_string(), outcome);
        }
    }
    clock.stability = stability_started.elapsed();

    // 7. Publish the family, then its manifest, and verify before it becomes ready.
    let publishing = Instant::now();
    let family = Family {
        schema_version: search::STREAMED_FAMILY_SCHEMA_VERSION,
        search: settings.clone(),
        resolved_conditions: Some(conditions.clone()),
        resolved_hash: Some(resolved.hash),
        plan_identity: development.plan_identity.clone(),
        base_stream: settings.base_stream,
        kernel_module: kernel_identity(),
        sampler: SAMPLER_VERSION.to_string(),
        applicable,
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
        schema_version: search::STREAMED_FAMILY_SCHEMA_VERSION,
        generation: generation.clone(),
        config_hash: config.content_hash(),
        code_revision: CODE_REVISION.to_string(),
        inputs,
        members: total,
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
    let verified = verify::memo(access, &uri, || {
        verify_family(&uri, destination, &key, &committed, access)
    })?;
    let temporary = import::temporary_path(local, &format!("manifest-{generation}"))?;
    fs::write(&temporary, &committed)
        .map_err(|error| format!("cannot write {}: {error}", temporary.display()))?;
    let identity = store::identify(&temporary)?;
    let put = destination.put_new(&key, &temporary, &identity)?;
    local.put_new(&key, &temporary, &identity)?;
    fs::remove_file(&temporary)
        .map_err(|error| format!("cannot remove {}: {error}", temporary.display()))?;
    clock.publish = publishing.elapsed();
    let screened = total - family.members.len() as u64;
    let report = format!(
        "search {} generation {generation} members {} applicable {} screened {screened} replayed {} passed {} evaluated {} objects 1",
        settings.scope,
        total,
        family.applicable,
        survivors.len(),
        passed.len(),
        if settings.evaluation.is_some() {
            passed.len()
        } else {
            0
        }
    );
    let line = match put {
        Put::Reused(_) => format!(
            "{report} columns {} blocks {} tuples {} list_entries {} construction_visits {} validation_visits {} driver_visits {} transfer_bytes {} peak_rss_kb {} {} {} (already published)",
            clock.columns,
            clock.blocks,
            clock.tuples,
            clock.list_entries,
            clock.construction_visits,
            clock.validation_visits,
            clock.driver_visits,
            clock.transfer_bytes,
            peak_rss_kb(),
            clock.tuning,
            clock.memory,
        ),
        Put::Created(_) => format!(
            "{report} [load {:.3}s lowering {:.3}s setup_before_gpu {:.3}s device upload {:.3}s execute {:.3}s download {:.3}s bytes {} replay {:.3}s stability {:.3}s publish {:.3}s] columns {} blocks {} tuples {} list_entries {} construction_visits {} validation_visits {} driver_visits {} transfer_bytes {} peak_rss_kb {} {} {}",
            clock.load.as_secs_f64(),
            clock.lowering.as_secs_f64(),
            clock.setup_before_gpu.as_secs_f64(),
            clock.device.upload.as_secs_f64(),
            clock.device.execute.as_secs_f64(),
            clock.device.download.as_secs_f64(),
            clock.device.allocated_bytes,
            clock.replay.as_secs_f64(),
            clock.stability.as_secs_f64(),
            clock.publish.as_secs_f64(),
            clock.columns,
            clock.blocks,
            clock.tuples,
            clock.list_entries,
            clock.construction_visits,
            clock.validation_visits,
            clock.driver_visits,
            clock.transfer_bytes,
            peak_rss_kb(),
            clock.tuning,
            clock.memory,
        ),
    };
    Ok(Searched {
        generation,
        report: format!("{line}\n{verified}"),
    })
}

/// The configuration of one synthesized replay: only the schema, run mode, storage, and that
/// table, so its hash and generation depend on nothing else.
fn chunk_config(config: &Config, table: binary_alpha_engine::config::Replay) -> Config {
    Config {
        replay: Some(table),
        ..crate::skeleton(config)
    }
}

/// Binds the development input: the plan identity, instrument, and the outcome generation.
fn bind_development(settings: &Search, access: Access<'_>) -> Result<Development, String> {
    let input = &settings.development.inputs[0];
    let field = |name: &str| format!("development.inputs[0].{name}");
    let bound = bind_inputs(
        &field,
        DatasetRole::Development,
        &input.tick_manifest,
        &input.feature_manifest,
        "a search",
        access,
        crate::outcomes::BindingMode::Historical,
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
        bound,
    })
}

/// Binds the evaluation input after the development result is frozen: the same instrument
/// applying the development plan unchanged.
fn bind_evaluation(
    settings: &Search,
    development: &Development,
    access: Access<'_>,
) -> Result<FamilyInput, String> {
    let window = settings.evaluation.as_ref().expect("configured");
    let input = &window.inputs[0];
    let field = |name: &str| format!("evaluation.inputs[0].{name}");
    let bound = bind_inputs(
        &field,
        DatasetRole::Evaluation,
        &input.tick_manifest,
        &input.feature_manifest,
        "a search",
        access,
        crate::outcomes::BindingMode::Historical,
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
pub(crate) fn read_object(
    store: &Store,
    objects: &[ObjectRecord],
    path: &str,
) -> Result<Vec<u8>, String> {
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
    let base = development.base_stream;
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

/// One base decision row. `entry` remains the stored close-referenced outcome entry tick.
#[allow(dead_code)] // Consumed by schema-2 screening in the next step.
struct ProjectionBaseRow {
    close: i64,
    installed: i64,
    entry: i64, // i64::MIN means the stored cell has no entry.
}

/// The availability join shared by every projected column block. `latest` is row-major, one
/// u32 per (base row, plan stream), with MISSING_INDEX for an absent installed row. Storage per
/// base row is 24 + 4 * stream_count bytes, apart from Vec capacity and stream clock arrays.
#[allow(dead_code)]
struct ProjectionIndex {
    streams: Vec<binary_alpha_engine::config::StreamKey>,
    closes: Vec<Vec<i64>>,
    base: Vec<ProjectionBaseRow>,
    latest: Vec<u32>,
}

#[allow(dead_code)]
impl ProjectionIndex {
    /// Install all rows at a time before recording the latest row for that base decision.
    fn from_clocks(
        streams: Vec<binary_alpha_engine::config::StreamKey>,
        clocks: Vec<Vec<(i64, i64)>>,
        base_stream: binary_alpha_engine::config::StreamKey,
        entries: Vec<i64>,
    ) -> Result<Self, String> {
        let base_stream_index = streams
            .iter()
            .position(|stream| *stream == base_stream)
            .ok_or_else(|| format!("base stream {base_stream} is not in the plan"))?;
        let base_clocks = &clocks[base_stream_index];
        if entries.len() != base_clocks.len() {
            return Err(format!(
                "{} stored entries for {} base rows",
                entries.len(),
                base_clocks.len()
            ));
        }
        let mut cursors = vec![0; streams.len()];
        let mut base = Vec::with_capacity(base_clocks.len());
        let mut latest = Vec::with_capacity(base_clocks.len() * streams.len());
        for ((close, installed), entry) in base_clocks.iter().copied().zip(entries) {
            for (stream, rows) in clocks.iter().enumerate() {
                while cursors[stream] < rows.len() && rows[cursors[stream]].1 <= installed {
                    cursors[stream] += 1;
                }
                latest.push(if cursors[stream] == 0 {
                    MISSING_INDEX
                } else {
                    u32::try_from(cursors[stream] - 1).map_err(|_| {
                        format!("stream {} has too many indexed rows", streams[stream])
                    })?
                });
            }
            base.push(ProjectionBaseRow {
                close,
                installed,
                entry,
            });
        }
        Ok(Self {
            streams,
            closes: clocks
                .into_iter()
                .map(|rows| rows.into_iter().map(|row| row.0).collect())
                .collect(),
            base,
            latest,
        })
    }

    /// Schema-2 kernel slot eligibility uses installation time and the stored entry tick.
    fn slot_mask(&self, start: i64, end: i64) -> Vec<u8> {
        self.base
            .iter()
            .map(|row| {
                u8::from(
                    start <= row.installed
                        && row.installed < end
                        && row.entry != i64::MIN
                        && row.entry >= row.installed,
                )
            })
            .collect()
    }

    fn latest(&self, base: usize, stream: usize) -> Option<usize> {
        let index = self.latest[base * self.streams.len() + stream];
        (index != MISSING_INDEX).then_some(index as usize)
    }
}

/// Read each stream's clocks and every required fitted encoding in one verified cursor pass;
/// derive stored entries from published outcome indices rather than recomputing outcomes.
fn projection_index(
    development: &Development,
    conditions: &[binary_alpha_engine::execution::Condition],
) -> Result<(ProjectionIndex, ProjectedRows), String> {
    let streams: Vec<_> = development
        .bound
        .plan
        .streams
        .iter()
        .map(|stream| stream.key())
        .collect();
    let mut projected = BTreeMap::new();
    let clocks: Vec<Vec<(i64, i64)>> = streams
        .iter()
        .map(|&stream| {
            let plan_stream = development
                .bound
                .plan
                .stream(stream)
                .ok_or("projected stream is absent from the plan")?;
            let mut columns = Vec::new();
            let mut outputs = Vec::new();
            for condition in conditions
                .iter()
                .filter(|condition| condition.stream == stream)
            {
                if projected_bucket(&development.bound.plan, condition).is_none()
                    || outputs.iter().any(
                        |(name, _, _, _): &(String, ColumnSpec, usize, Vec<usize>)| {
                            name == &condition.output
                        },
                    )
                {
                    continue;
                }
                let mut spec = replay::column_spec(plan_stream, &condition.output)
                    .ok_or("projected encoding is absent from the plan")?;
                let readiness = development.bound.plan.readiness_of(&spec.source);
                spec.readiness = readiness.flags;
                spec.unready = readiness.unready;
                let mut readiness_indices = Vec::new();
                for flag in &spec.readiness {
                    let flag_spec = replay::column_spec(plan_stream, flag)
                        .ok_or_else(|| format!("readiness flag `{flag}` is absent"))?;
                    readiness_indices.push(column_index(&mut columns, flag_spec));
                }
                let value_index = column_index(&mut columns, spec.clone());
                outputs.push((
                    condition.output.clone(),
                    spec,
                    value_index,
                    readiness_indices,
                ));
            }
            let mut cursor =
                replay::RowCursor::open(&development.bound, &StreamColumns { stream, columns })?;
            let mut rows = Vec::new();
            let mut codes = vec![Vec::new(); outputs.len()];
            while cursor.peek()?.is_some() {
                let (close, known, values) = cursor.next()?;
                rows.push((close, known));
                for ((_, spec, value_index, readiness_indices), row_codes) in
                    outputs.iter().zip(&mut codes)
                {
                    row_codes.push(project_fitted_label(
                        i64::MAX,
                        Some((close, values[*value_index].as_ref())),
                        spec,
                        readiness_indices
                            .iter()
                            .map(|&index| values[index] == Some(Value::Bool(true))),
                    ));
                }
            }
            for ((output, _, _, _), row_codes) in outputs.into_iter().zip(codes) {
                projected.insert(
                    (stream.duration_seconds, stream.offset_seconds, output),
                    row_codes,
                );
            }
            Ok(rows)
        })
        .collect::<Result<_, String>>()?;
    let references = read_references(development)?;
    let base = streams
        .iter()
        .position(|&stream| stream == development.base_stream)
        .ok_or("base stream is absent from plan")?;
    if references != clocks[base].iter().map(|row| row.0).collect::<Vec<_>>() {
        return Err("outcome references differ from bound base feature rows".into());
    }
    let paths = stream_object_paths(
        development.base_stream.duration_seconds,
        development.base_stream.offset_seconds,
    );
    let entries = from_le_bytes(
        &read_object(
            &development.outcome_store,
            &development.outcome.objects,
            &paths[1],
        )?,
        u32::from_le_bytes,
    )?;
    let builder = outcome_builder(development)?;
    let entry_times: Vec<i64> = entries
        .into_iter()
        .map(|index| {
            if index == MISSING_INDEX {
                Ok(i64::MIN)
            } else {
                builder.times().get(index as usize).copied().ok_or_else(|| {
                    format!("stored entry index {index} is beyond the tick generation")
                })
            }
        })
        .collect::<Result<_, _>>()?;
    Ok((
        ProjectionIndex::from_clocks(streams, clocks, development.base_stream, entry_times)?,
        projected,
    ))
}

fn column_index(columns: &mut Vec<ColumnSpec>, spec: ColumnSpec) -> usize {
    if let Some(index) = columns
        .iter()
        .position(|column| column.source == spec.source)
    {
        index
    } else {
        columns.push(spec);
        columns.len() - 1
    }
}

/// The retained code of an equality condition, or None when it must use engine lowering.
#[allow(dead_code)]
fn projected_bucket(
    plan: &binary_alpha_engine::features::FeaturePlan,
    condition: &binary_alpha_engine::execution::Condition,
) -> Option<i16> {
    use binary_alpha_engine::execution::{Comparator, Threshold};
    let Threshold::Text(label) = &condition.threshold else {
        return None;
    };
    if condition.comparator != Comparator::Eq {
        return None;
    }
    let stream = plan.stream(condition.stream)?;
    if stream
        .outputs
        .iter()
        .any(|output| output.name == condition.output)
    {
        return None;
    }
    stream
        .encodings
        .iter()
        .find(|encoding| encoding.output == condition.output)?
        .labels
        .iter()
        .position(|retained| retained == label)
        .and_then(|code| i16::try_from(code).ok())
}

fn projected_code_at(
    index: &ProjectionIndex,
    base_row: usize,
    stream: usize,
    codes: &[i16],
) -> i16 {
    let base = &index.base[base_row];
    index
        .latest(base_row, stream)
        .filter(|&row| index.closes[stream][row] <= base.close)
        .map_or(-1, |row| codes[row])
}

fn projection_block_from_development(
    development: &Development,
    index: &ProjectionIndex,
    conditions: &[binary_alpha_engine::execution::Condition],
    requested: &[usize],
    projected: &ProjectedRows,
    signals_by_binding: &BTreeMap<&str, Vec<usize>>,
    lowered_bindings: &BTreeMap<usize, String>,
) -> Result<(Vec<i16>, Vec<i16>), String> {
    projection_block(
        &development.bound.plan,
        index,
        conditions,
        requested,
        signals_by_binding,
        lowered_bindings,
        |condition| {
            projected
                .get(&(
                    condition.stream.duration_seconds,
                    condition.stream.offset_seconds,
                    condition.output.clone(),
                ))
                .map(Vec::as_slice)
                .ok_or_else(|| "projected encoding has no row codes".to_string())
        },
    )
}

/// Bind lowering signals to base rows once per scoring call. The BTreeMap preserves the
/// previous duplicate-close rule: a signal at a repeated close selects the last base row.
fn lowering_rows_by_binding<'a>(
    index: &ProjectionIndex,
    signals: &'a [FinancialEvent],
    lowered_bindings: &BTreeMap<usize, String>,
) -> Result<BTreeMap<&'a str, Vec<usize>>, String> {
    if signals.is_empty() || lowered_bindings.is_empty() {
        return Ok(BTreeMap::new());
    }
    let row_of: BTreeMap<i64, usize> = index
        .base
        .iter()
        .enumerate()
        .map(|(row, base)| (base.close, row))
        .collect();
    let bindings: BTreeSet<&str> = lowered_bindings.values().map(String::as_str).collect();
    let mut rows = BTreeMap::<&str, Vec<usize>>::new();
    for event in signals {
        if let EventKind::Signal {
            binding,
            close_time_micros,
            ..
        } = &event.kind
            && bindings.contains(binding.as_str())
        {
            rows.entry(binding.as_str()).or_default().push(
                *row_of
                    .get(close_time_micros)
                    .ok_or("lowering signal has no base row")?,
            );
        }
    }
    Ok(rows)
}

/// Build only the requested condition columns. Fitted equalities carry retained label codes;
/// all other columns carry 0/1 from the engine's own lowering Signal records.
fn projection_block<'a>(
    plan: &binary_alpha_engine::features::FeaturePlan,
    index: &ProjectionIndex,
    conditions: &[binary_alpha_engine::execution::Condition],
    requested: &[usize],
    signals_by_binding: &BTreeMap<&str, Vec<usize>>,
    lowered_bindings: &BTreeMap<usize, String>,
    mut projected_rows: impl FnMut(
        &binary_alpha_engine::execution::Condition,
    ) -> Result<&'a [i16], String>,
) -> Result<(Vec<i16>, Vec<i16>), String> {
    let mut codes = Vec::with_capacity(requested.len() * index.base.len());
    let mut buckets = Vec::with_capacity(requested.len());
    for &condition_index in requested {
        let condition = conditions
            .get(condition_index)
            .ok_or("condition block index is out of bounds")?;
        if let Some(bucket) = projected_bucket(plan, condition) {
            let stream_index = index
                .streams
                .iter()
                .position(|&stream| stream == condition.stream)
                .ok_or("condition stream is absent from plan")?;
            let row_codes = projected_rows(condition)?;
            if row_codes.len() != index.closes[stream_index].len() {
                return Err("projection row count changed after clock indexing".into());
            }
            for base_row in 0..index.base.len() {
                codes.push(projected_code_at(index, base_row, stream_index, row_codes));
            }
            buckets.push(bucket);
        } else {
            let mut column = vec![0_i16; index.base.len()];
            let binding_id = lowered_bindings
                .get(&condition_index)
                .ok_or_else(|| format!("condition {condition_index} has no lowering binding"))?;
            if let Some(rows) = signals_by_binding.get(binding_id.as_str()) {
                for &row in rows {
                    column[row] = 1;
                }
            }
            codes.extend(column);
            buckets.push(1);
        }
    }
    Ok((codes, buckets))
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
        development.base_stream.duration_seconds,
        development.base_stream.offset_seconds,
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
            }
            | EventKind::Reconciled {
                command,
                resolution: Resolution::ExternallyClosed { .. } | Resolution::Settled { .. },
                profit: Some(profit),
                ..
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
fn kernel_identity_for_schema(schema_version: u32) -> String {
    let mut hasher = Sha256::new();
    let sources = KERNEL_SOURCES.iter().chain(
        (schema_version == search::STREAMED_FAMILY_SCHEMA_VERSION).then_some(&SCREEN_KERNEL_SOURCE),
    );
    for (name, source) in sources {
        hasher.update(name.as_bytes());
        hasher.update(b"\n");
        hasher.update(source.as_bytes());
        hasher.update(b"\n");
    }
    binary_alpha_engine::hex(&hasher.finalize())
}

fn kernel_identity() -> String {
    kernel_identity_for_schema(search::STREAMED_FAMILY_SCHEMA_VERSION)
}

pub(crate) fn peak_rss_kb() -> u64 {
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

fn raw_at_basic(values: &[i64], position: usize) -> RawCounts {
    let at = position * 8;
    RawCounts {
        total: values[at],
        wins: values[at + 1],
        losses: values[at + 2],
        ties: values[at + 3],
        invalid: values[at + 4],
    }
}

/// Stream block tuples, keeping only compact whole-family counts. Each tuple's sparse index is
/// scoped to its columns and built once; batches carry only conjunctions and driver IDs.
fn condition_slots(max_conditions: u32, resolved: usize) -> usize {
    (max_conditions as usize).min(resolved)
}

fn tuple_buffers<'a>(
    codes: &'a [i16],
    feature_count: i32,
    rows: i32,
    ordered: &'a [i64],
    entry_times: &'a [i64],
    outcome: &'a DeviceRows,
) -> kernels::SearchBuffers<'a> {
    kernels::SearchBuffers {
        feature_codes: codes,
        feature_count,
        row_count: rows,
        ordered_rows: ordered,
        decision_time_ms: entry_times,
        release_time_ms: &outcome.release_ms,
        settlement_time_ms: &outcome.release_ms,
        valid: &outcome.valid,
        buy_win: &outcome.buy_win,
        sell_win: &outcome.sell_win,
        tie: &outcome.tie,
    }
}

#[allow(clippy::too_many_arguments)]
fn parallel_projection_block(
    development: &Development,
    index: &ProjectionIndex,
    conditions: &[binary_alpha_engine::execution::Condition],
    requested: &[usize],
    projected: &ProjectedRows,
    signals_by_binding: &BTreeMap<&str, Vec<usize>>,
    lowered_bindings: &BTreeMap<usize, String>,
) -> Result<(Vec<i16>, Vec<i16>), String> {
    let columns = crate::parallel::map(requested, |&column| {
        projection_block_from_development(
            development,
            index,
            conditions,
            &[column],
            projected,
            signals_by_binding,
            lowered_bindings,
        )
    });
    let mut codes = Vec::with_capacity(requested.len() * index.base.len());
    let mut buckets = Vec::with_capacity(requested.len());
    for column in columns {
        let (one, bucket) = column?;
        codes.extend(one);
        buckets.extend(bucket);
    }
    Ok((codes, buckets))
}

fn parallel_sparse_lists(
    codes: &[i16],
    buckets: &[i16],
    ordered: &[i64],
    rows: usize,
) -> Result<(Vec<i32>, Vec<i32>), String> {
    let columns: Vec<usize> = (0..buckets.len()).collect();
    let lists = crate::parallel::map(&columns, |&column| {
        ordered
            .iter()
            .filter_map(|&row| {
                (codes[column * rows + row as usize] == buckets[column]).then_some(row as i32)
            })
            .collect::<Vec<_>>()
    });
    let mut offsets = vec![0_i32];
    let mut sparse_rows = Vec::new();
    for list in lists {
        let next = sparse_rows
            .len()
            .checked_add(list.len())
            .ok_or("sparse list length overflows")?;
        offsets.push(i32::try_from(next).map_err(|_| "sparse row list exceeds i32")?);
        sparse_rows.extend(list);
    }
    Ok((offsets, sparse_rows))
}

fn with_screen_plan<T>(
    lengths: &[usize],
    shape: &kernels::ScreenShape,
    budget: usize,
    unit_hint: Option<usize>,
    mut attempt: impl FnMut(&kernels::ColumnBlockPlan) -> Result<T, String>,
) -> Result<(kernels::ColumnBlockPlan, T), String> {
    let mut width = lengths.len().max(1);
    loop {
        let plan = kernels::plan_screen_blocks(lengths, shape, budget, unit_hint, width)?;
        match attempt(&plan) {
            Ok(value) => return Ok((plan, value)),
            Err(error)
                if (error.starts_with("screen preallocation failed:")
                    || error.starts_with("screen tuple budget exceeded:"))
                    && width > 1 =>
            {
                eprintln!("{error}; re-planning at half block width");
                width = (width / 2).max(1);
            }
            Err(error) => return Err(error),
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn score_streamed(
    settings: &Search,
    development: &Development,
    conditions: &[binary_alpha_engine::execution::Condition],
    signals: &[FinancialEvent],
    lowered_bindings: &BTreeMap<usize, String>,
    backends: &[Backend],
    device_ordinals: Option<&[usize]>,
    clock: &mut Clock,
) -> Result<Vec<search::CompactMember>, String> {
    #[cfg(feature = "cuda")]
    let upload_start: Vec<usize> = backends
        .iter()
        .map(|backend| match backend {
            Backend::Cuda(device) => device.uploaded_bytes(),
            Backend::Cpu => 0,
        })
        .collect();
    let total = search::family_size(
        conditions.len(),
        settings.min_conditions as usize,
        settings.max_conditions as usize,
        settings.contracts.len(),
    )
    .ok_or("resolved family size overflows")?;
    let total = usize::try_from(total).map_err(|_| "family is too large for this host")?;
    let (index, projected) = projection_index(development, conditions)?;
    let signals_by_binding = lowering_rows_by_binding(&index, signals, lowered_bindings)?;
    let rows = index.base.len();
    if rows > i32::MAX as usize {
        return Err("search base rows exceed i32".into());
    }
    let expiries = expiry_columns(settings, development)?;
    let builder = outcome_builder(development)?;
    let references = read_references(development)?;
    let mut outcome_rows = BTreeMap::new();
    for &expiry in &expiries {
        if let std::collections::btree_map::Entry::Vacant(entry) = outcome_rows.entry(expiry) {
            entry.insert(device_rows(development, &builder, &references, expiry)?);
        }
    }
    let entry_times = &outcome_rows
        .first_key_value()
        .ok_or("search has no expiry outcomes")?
        .1
        .decision_ms;
    if outcome_rows
        .values()
        .any(|outcome| outcome.decision_ms != *entry_times)
    {
        return Err("expiry outcomes disagree on entry times".into());
    }
    let start =
        binary_alpha_engine::market::parse_event_time_micros(&settings.development.decision_start)?;
    let end =
        binary_alpha_engine::market::parse_event_time_micros(&settings.development.decision_end)?;
    let mask = index.slot_mask(start, end);
    let mut split_mask = mask.clone();
    for (flag, &entry) in split_mask.iter_mut().zip(entry_times) {
        if entry == i64::MIN {
            *flag = 0;
        }
    }
    let mut ordered: Vec<i64> = (0..rows as i64).collect();
    ordered.sort_by_key(|&row| (entry_times[row as usize], row));
    let columns: Vec<usize> = (0..conditions.len()).collect();
    let lengths = crate::parallel::map(&columns, |&column| {
        let (codes, buckets) = projection_block_from_development(
            development,
            &index,
            conditions,
            &[column],
            &projected,
            &signals_by_binding,
            lowered_bindings,
        )?;
        Ok::<usize, String>(codes.iter().filter(|&&code| code == buckets[0]).count())
    })
    .into_iter()
    .collect::<Result<Vec<_>, _>>()?;
    clock.construction_visits += rows * conditions.len();
    #[cfg(feature = "cuda")]
    let packed = packed_tiles(&outcome_rows, rows)?;
    let slots = condition_slots(settings.max_conditions, conditions.len());
    let forced_budget = test_screen_limit("BINARY_ALPHA_SCREEN_MEMORY_BUDGET_BYTES")?;
    let forced_unit = test_screen_limit("BINARY_ALPHA_CUDA_RESERVATION_UNIT_BYTES")?;
    let forced_batch = test_screen_limit("BINARY_ALPHA_SCREEN_BATCH_SIZE")?;
    let _forced_threads = test_screen_limit("BINARY_ALPHA_SCREEN_THREADS_PER_BLOCK")?;
    #[cfg(feature = "cuda")]
    let mut budget = usize::MAX;
    #[cfg(not(feature = "cuda"))]
    let mut budget = usize::MAX;
    #[cfg(feature = "cuda")]
    let mut derived_batch = if backends
        .iter()
        .any(|backend| matches!(backend, Backend::Cuda(_)))
    {
        usize::MAX
    } else {
        1_024
    };
    #[cfg(not(feature = "cuda"))]
    let derived_batch = 1_024;
    #[cfg(feature = "cuda")]
    let mut measured_unit = None;
    #[cfg(feature = "cuda")]
    let mut local_hint_bytes = 0;
    #[cfg(not(feature = "cuda"))]
    let local_hint_bytes = 0;
    #[cfg(feature = "cuda")]
    for (position, backend) in backends.iter().enumerate() {
        if let Backend::Cuda(device) = backend {
            if clock.setup_before_gpu.is_zero() {
                clock.setup_before_gpu = clock
                    .started
                    .map_or(Duration::ZERO, |start| start.elapsed());
            }
            let ordinals = device_ordinals.ok_or("CUDA screening has no device ordinals")?;
            let duplicate_workers = ordinals
                .iter()
                .filter(|&&ordinal| ordinal == ordinals[position])
                .count();
            budget = budget.min(device.screen_warmup_free_bytes()? / duplicate_workers);
            derived_batch = derived_batch.min(device.screening_batch_capacity()?);
            local_hint_bytes = local_hint_bytes.max(device.screening_local_hint_bytes()?);
            measured_unit = match (measured_unit, device.reservation_unit()) {
                (Some(left), Some(right)) if left == right => Some(left),
                (None, value) => value,
                _ => None,
            };
        }
    }
    #[cfg(not(feature = "cuda"))]
    let _ = device_ordinals;
    #[cfg(not(feature = "cuda"))]
    let measured_unit: Option<usize> = None;
    let budget_derived = budget;
    if let Some(forced) = forced_budget {
        budget = budget.min(forced);
    }
    let mut batch_size = forced_batch.unwrap_or(derived_batch.min(i32::MAX as usize / slots));
    let mut shape = kernels::ScreenShape {
        rows,
        slots,
        batch: batch_size,
        tile_strides: (0..outcome_rows.len())
            .collect::<Vec<_>>()
            .chunks(8)
            .map(|chunk| (chunk.len() * 9).div_ceil(8) * 8)
            .collect(),
        largest_tile: outcome_rows.len().min(8),
        local_hint_bytes,
    };
    if forced_batch.is_none() && budget != usize::MAX {
        let largest_list = lengths.iter().copied().max().unwrap_or(0);
        while batch_size > 1
            && shape
                .logical_bytes(1, largest_list)
                .map_or(true, |n| n > budget)
        {
            batch_size = (batch_size / 2).max(1);
            shape.batch = batch_size;
        }
    }
    let unit_hint = forced_unit.or(measured_unit);
    clock.tuning = format!(
        "screen_batch {}({}) screen_budget {}({}) reservation_unit {}({}) local_hint_bytes {}(derived)",
        batch_size,
        if forced_batch.is_some() {
            "override"
        } else {
            "derived"
        },
        budget,
        if forced_budget.is_some_and(|forced| forced < budget_derived) {
            "override"
        } else {
            "derived"
        },
        unit_hint.map_or("none".to_string(), |n| n.to_string()),
        if forced_unit.is_some() {
            "override"
        } else if measured_unit.is_some() {
            "measured"
        } else {
            "unavailable"
        },
        local_hint_bytes
    );
    #[cfg(feature = "cuda")]
    for backend in backends {
        if let Backend::Cuda(device) = backend {
            let info = device.info();
            let (threads, source) = device.screening_threads()?;
            clock.tuning.push_str(&format!(
                " device={}({}) sm_{}{}({}) build_target={}({}) screen_threads={}({})",
                info.name.replace(' ', "_"),
                "device_query",
                info.compute_capability.0,
                info.compute_capability.1,
                "device_query",
                info.build_target,
                info.build_target_source,
                threads,
                source
            ));
        }
    }
    let mut records = vec![search::CompactMember::new(RawCounts::default()); total];
    let mut completed = vec![false; total / settings.contracts.len()];
    let mut attempts = 0_usize;
    with_screen_plan(&lengths, &shape, budget, unit_hint, |plan| {
        attempts += 1;
        clock.blocks = plan.blocks.len();
        let mut suffix_capacity = vec![0; plan.blocks.len() + 1];
        for block in (0..plan.blocks.len()).rev() {
            suffix_capacity[block] = suffix_capacity[block + 1] + plan.blocks[block].columns.len();
        }
        #[cfg(feature = "cuda")]
        let mut next_device = 0_usize;
        for size in settings.min_conditions as usize
            ..=settings.max_conditions.min(conditions.len() as u32) as usize
        {
            visit_block_tuples(
                &plan.blocks,
                &suffix_capacity,
                size,
                0,
                &mut Vec::new(),
                &mut |tuple| {
                    if attempts > 1 {
                        let mut pending = false;
                        visit_tuple_conditions(
                            &plan.blocks,
                            tuple,
                            0,
                            0,
                            &mut Vec::new(),
                            &mut |chosen| {
                                pending |= !completed
                                    [tuple_candidate_index(chosen, conditions.len(), settings)?];
                                Ok(())
                            },
                        )?;
                        if !pending {
                            return Ok(());
                        }
                    }
                    clock.tuples += 1;
                    let mut requested = Vec::new();
                    for &block in tuple {
                        requested.extend(plan.blocks[block].columns.clone());
                    }
                    requested.sort_unstable();
                    requested.dedup();
                    let (codes, buckets) = parallel_projection_block(
                        development,
                        &index,
                        conditions,
                        &requested,
                        &projected,
                        &signals_by_binding,
                        lowered_bindings,
                    )?;
                    let (offsets, sparse_rows) =
                        parallel_sparse_lists(&codes, &buckets, &ordered, rows)?;
                    let tuple_bytes = shape.exact_bytes(requested.len(), sparse_rows.len())?;
                    if tuple_bytes > budget {
                        return Err(format!(
                            "screen tuple budget exceeded: planned {tuple_bytes} logical bytes, free budget {budget} bytes"
                        ));
                    }
                    clock.construction_visits += buckets.len() * ordered.len();
                    clock.list_entries += sparse_rows.len();
                    let keys = kernels::SparseKeys {
                        key_chrono_offsets: &offsets,
                        key_chrono_rows: &sparse_rows,
                    };
                    let first = outcome_rows.first_key_value().expect("nonempty outcomes").1;
                    let cpu = backends.len() == 1 && matches!(backends[0], Backend::Cpu);
                    let mut cpu_workspace = if cpu {
                        let workspace = kernels::CpuSparseTuple::new(
                            tuple_buffers(
                                &codes,
                                requested.len() as i32,
                                rows as i32,
                                &ordered,
                                entry_times,
                                first,
                            ),
                            &[&split_mask],
                            keys,
                        )?;
                        clock.validation_visits += offsets.len() + sparse_rows.len();
                        Some(workspace)
                    } else {
                        None
                    };
                    #[cfg(feature = "cuda")]
                    let mut workspaces = if cpu {
                        Vec::new()
                    } else {
                        let tiles: Vec<_> = packed
                            .iter()
                            .map(|tile| binary_alpha_accelerator::cuda::ScreenTile {
                                bytes: &tile.bytes,
                                active: tile.expiries.len() as i32,
                                stride: tile.stride as i32,
                            })
                            .collect();
                        let workspaces: Vec<_> = backends
                        .iter()
                        .map(|backend| match backend {
                            Backend::Cuda(device) => {
                                let free = device.memory_info()?.0;
                                clock.validation_visits += offsets.len() + sparse_rows.len();
                                device.screen_tuple_workspace(
                                tuple_buffers(
                                    &codes,
                                    requested.len() as i32,
                                    rows as i32,
                                    &ordered,
                                    entry_times,
                                    first,
                                ),
                                &split_mask,
                                keys,
                                &tiles,
                                batch_size,
                                slots,
                                ).map_err(|reason| {
                                    if reason.contains("CUDA_ERROR_OUT_OF_MEMORY") {
                                        format!("screen preallocation failed: planned {tuple_bytes} logical bytes, free {free} bytes: {reason}")
                                    } else { reason }
                                })
                            },
                            Backend::Cpu => Err("mixed CPU and CUDA search devices".into()),
                        })
                        .collect::<Result<_, String>>()?;
                        for workspace in &workspaces {
                            add(&mut clock.device, workspace.timings);
                        }
                        for workspace in &workspaces {
                            if workspace.allocated_bytes() >= clock.memory_peak_bytes {
                                clock.memory_peak_bytes = workspace.allocated_bytes();
                                clock.memory = format!(
                                    "screen_preallocated {} screen_batch {} screen_free_before {} screen_free_prelaunch {} screen_free_after {} pool_before {:?} pool_prelaunch {:?} pool_after {:?}",
                                    workspace.allocated_bytes(),
                                    batch_size,
                                    workspace.free_before,
                                    workspace.free_prelaunch,
                                    workspace.free_after,
                                    workspace.pool_before,
                                    workspace.pool_prelaunch,
                                    workspace.pool_after
                                );
                            }
                        }
                        workspaces
                    };
                    if let Some(workspace) = &mut cpu_workspace {
                        for (&expiry, outcome) in &outcome_rows {
                            let duration =
                                i64::from(development.outcome.rule.expiry_seconds[expiry])
                                    * 1_000_000;
                            let buffers = tuple_buffers(
                                &codes,
                                requested.len() as i32,
                                rows as i32,
                                &ordered,
                                entry_times,
                                outcome,
                            );
                            if !std::ptr::eq(outcome, first) {
                                workspace.set_outcome(buffers, &split_mask)?;
                            }
                            let apply =
                        |scored: Vec<(u64, RawCounts, RawCounts)>,
                         records: &mut [search::CompactMember]| {
                            for (global, buy, sell) in scored {
                                for (contract, &column) in expiries.iter().enumerate() {
                                    if column == expiry {
                                        let at = usize::try_from(global).expect("bounded rank")
                                            + contract;
                                        records[at].raw =
                                            match settings.contracts[contract].direction {
                                                binary_alpha_engine::execution::Direction::Buy => {
                                                    buy.clone()
                                                }
                                                binary_alpha_engine::execution::Direction::Sell => {
                                                    sell.clone()
                                                }
                                            };
                                    }
                                }
                            }
                        };
                            tuple_batches(
                                &plan.blocks,
                                tuple,
                                &requested,
                                &buckets,
                                &offsets,
                                conditions.len(),
                                settings,
                                batch_size,
                                1,
                                &completed,
                                &mut clock.driver_visits,
                                |group| {
                                    let batches: Vec<_> = group
                                        .iter()
                                        .map(|batch| CpuBatch {
                                            global_indices: &batch.globals,
                                            candidates: batch.candidates(),
                                            driver_keys: &batch.drivers,
                                        })
                                        .collect();
                                    let scored =
                                        score_cpu_batches(workspace, &batches, 0, duration, 0)?;
                                    apply(scored, &mut records);
                                    Ok(())
                                },
                            )?;
                        }
                    } else {
                        #[cfg(feature = "cuda")]
                        tuple_batches(
                            &plan.blocks,
                            tuple,
                            &requested,
                            &buckets,
                            &offsets,
                            conditions.len(),
                            settings,
                            batch_size,
                            outcome_rows.len(),
                            &completed,
                            &mut clock.driver_visits,
                            |group| {
                                for (i, batch) in group.iter().enumerate() {
                                    let device = (next_device + i) % workspaces.len();
                                    for (tile_index, tile) in packed.iter().enumerate() {
                                        let output = workspaces[device].score_batch(
                                            batch.candidates(),
                                            &batch.drivers,
                                            tile_index,
                                        )?;
                                        add(&mut clock.device, output.timings);
                                        for (position, &global) in batch.globals.iter().enumerate()
                                        {
                                            for (local_expiry, &expiry) in
                                                tile.expiries.iter().enumerate()
                                            {
                                                let at = (position * tile.expiries.len()
                                                    + local_expiry)
                                                    * 5;
                                                let counts = &output.output[at..at + 5];
                                                for (contract, &column) in
                                                    expiries.iter().enumerate()
                                                {
                                                    if column == expiry {
                                                        let raw = RawCounts {
                                                        total: i64::from(counts[0]),
                                                        wins: i64::from(counts[if matches!(settings.contracts[contract].direction, binary_alpha_engine::execution::Direction::Buy) { 1 } else { 2 }]),
                                                        losses: i64::from(counts[if matches!(settings.contracts[contract].direction, binary_alpha_engine::execution::Direction::Buy) { 2 } else { 1 }]),
                                                        ties: i64::from(counts[3]),
                                                        invalid: i64::from(counts[4]),
                                                    };
                                                        records[global as usize + contract].raw =
                                                            raw;
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                                next_device = (next_device + group.len()) % workspaces.len();
                                Ok(())
                            },
                        )?;
                        #[cfg(not(feature = "cuda"))]
                        return Err("CUDA screening requires the cuda feature".into());
                    }
                    visit_tuple_conditions(
                        &plan.blocks,
                        tuple,
                        0,
                        0,
                        &mut Vec::new(),
                        &mut |chosen| {
                            completed[tuple_candidate_index(chosen, conditions.len(), settings)?] =
                                true;
                            Ok(())
                        },
                    )
                },
            )?;
        }
        Ok(())
    })?;
    if completed.iter().any(|&done| !done) {
        return Err("screen blocks: re-plan left candidate ranks unscored".into());
    }
    #[cfg(feature = "cuda")]
    {
        clock.transfer_bytes = backends
            .iter()
            .zip(upload_start)
            .map(|(backend, start)| match backend {
                Backend::Cuda(device) => device.uploaded_bytes() - start,
                Backend::Cpu => 0,
            })
            .sum();
    }
    Ok(records)
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

fn streamed_member(
    global: u64,
    settings: &Search,
    plan_identity: &str,
    conditions: &[binary_alpha_engine::execution::Condition],
) -> Result<Member, String> {
    let (selected, contract) = search::member_unrank(
        conditions.len(),
        settings.min_conditions as usize,
        settings.max_conditions as usize,
        settings.contracts.len(),
        global,
    )
    .ok_or_else(|| format!("global member {global} is outside the resolved family"))?;
    let strategy = search::strategy(
        "",
        plan_identity,
        settings.base_stream,
        conditions,
        &selected,
    );
    Ok(Member {
        global_index: Some(global),
        logic_identity: signal_logic_identity(&strategy),
        conditions: strategy.conditions,
        contract: settings.contracts[contract].id.clone(),
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
}

fn chunk_members_streamed(
    bindings: &[String],
    settings: &Search,
    plan_identity: &str,
    conditions: &[binary_alpha_engine::execution::Condition],
) -> Result<Vec<ChunkMember>, String> {
    bindings
        .iter()
        .map(|id| {
            let global: u64 = id
                .strip_prefix('m')
                .and_then(|index| index.parse().ok())
                .ok_or_else(|| format!("chunk binding `{id}` is not a member"))?;
            let (selected, contract) = search::member_unrank(
                conditions.len(),
                settings.min_conditions as usize,
                settings.max_conditions as usize,
                settings.contracts.len(),
                global,
            )
            .ok_or_else(|| format!("chunk binding `{id}` is outside the resolved family"))?;
            Ok((
                id.clone(),
                search::strategy(
                    id,
                    plan_identity,
                    settings.base_stream,
                    conditions,
                    &selected,
                ),
                contract,
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

/// Verifies a family generation: the manifest and its content-addressed object, the member set
/// re-enumerated from the recorded search table, the bound inputs against the manifest, every
/// referenced replay through the replay verifier and its restored definition against the table
/// synthesized for its recorded members, the raw counts recomputed through the CPU kernel from
/// the verified lowering records and outcome objects, every score, adjustment, screen decision,
/// gate and rank recomputed by the engine owner, every group and split group against the
/// verified summaries and the shared ledger projection, and every stability result recomputed
/// from the verified settlement profits.
pub fn verify_family(
    uri: &str,
    store: &Store,
    key: &str,
    bytes: &[u8],
    access: Access<'_>,
) -> Result<String, String> {
    let manifest = family_manifest(uri, key, bytes)?;
    let (family, family_bytes) = read_family(uri, store, &manifest)?;
    let replayed = verify_read_family(uri, store, &manifest, &family, access)?;
    Ok(format!(
        "verified search generation {} members {} applicable {} replayed {replayed} passed {} objects 1 bytes {family_bytes}",
        manifest.generation,
        manifest.members,
        family.applicable,
        family
            .members
            .iter()
            .filter(|member| member.rank.is_some())
            .count()
    ))
}

/// The typed development-only reader of a verified family: every manifest input is development
/// before `family.json` is opened; the family carries no evaluation window, lowering or chunk
/// of another role, or member evaluation evidence before any referenced generation is followed;
/// then the whole family verifies exactly as `data verify` does. Nothing is stripped to make an
/// input acceptable.
pub(crate) fn development_family(
    uri: &str,
    access: Access<'_>,
) -> Result<(FamilyManifest, Family), String> {
    let (store, key) = verify::open(uri)?;
    let mut bytes = Vec::new();
    store.read_to(&key, None, &mut bytes)?;
    if verify::manifest_kind(&bytes)?.as_deref() != Some(search::FAMILY_MANIFEST_KIND) {
        return Err(format!("{uri} is not a search family manifest"));
    }
    let manifest = family_manifest(uri, &key, &bytes)?;
    let development = DatasetRole::Development.to_string();
    if let Some(input) = manifest
        .inputs
        .iter()
        .find(|input| input.role != development)
    {
        return Err(format!(
            "{uri}: input {} of {} is `{}`; a portfolio universe reads development-only families",
            input.tick_generation, input.instrument, input.role
        ));
    }
    // A declared holdout input is protected whatever the family's own labels say.
    for input in &manifest.inputs {
        access.lookup(&input.tick_generation).map_err(|reason| {
            format!(
                "{uri}: input {} of {}: {reason}",
                input.tick_generation, input.instrument
            )
        })?;
    }
    let (family, _) = read_family(uri, &store, &manifest)?;
    let later = if family.search.evaluation.is_some() {
        Some("an evaluation window")
    } else if family
        .lowering
        .as_ref()
        .is_some_and(|lowering| lowering.role != development)
    {
        Some("a lowering replay of another role")
    } else if family.chunks.iter().any(|chunk| chunk.role != development) {
        Some("a replay chunk of another role")
    } else if family.members.iter().any(|member| {
        member.evaluation.is_some()
            || !member.evaluation_splits.is_empty()
            || member.stability.keys().any(|role| *role != development)
    }) {
        Some("member evaluation evidence")
    } else {
        None
    };
    if let Some(what) = later {
        return Err(format!(
            "{uri}: the family carries {what}; a portfolio universe reads development-only families"
        ));
    }
    verify::run_with(uri, access)?;
    Ok((manifest, family))
}

/// The manifest of the family generation at `key`.
fn family_manifest(uri: &str, key: &str, bytes: &[u8]) -> Result<FamilyManifest, String> {
    let manifest = FamilyManifest::from_json(bytes).map_err(|error| format!("{uri}: {error}"))?;
    if manifest.key() != key {
        return Err(format!(
            "{uri} holds the manifest of generation {}",
            manifest.generation
        ));
    }
    Ok(manifest)
}

/// The family object of a manifest and its byte count, checked against the recorded member
/// count and the kernel sources and sampler this binary carries.
fn read_family(
    uri: &str,
    store: &Store,
    manifest: &FamilyManifest,
) -> Result<(Family, usize), String> {
    let family_bytes = read_object(store, &manifest.objects, FAMILY_OBJECT_PATH)?;
    let family = Family::from_json(&family_bytes)
        .map_err(|error| format!("{uri}: {FAMILY_OBJECT_PATH}: {error}"))?;
    if family.schema_version != manifest.schema_version {
        return Err(format!("{uri}: family and manifest schema versions differ"));
    }
    if manifest.schema_version == search::FAMILY_SCHEMA_VERSION
        && family.members.len() as u64 != manifest.members
    {
        return Err(format!(
            "{uri}: the manifest records {} members but the family holds {}",
            manifest.members,
            family.members.len()
        ));
    }
    if family.kernel_module != kernel_identity_for_schema(family.schema_version)
        || family.sampler != SAMPLER_VERSION
    {
        return Err(format!(
            "{uri}: the family records kernel sources or a sampler this binary does not carry"
        ));
    }
    Ok((family, family_bytes.len()))
}

fn read_verified_chunk(
    uri: &str,
    store: &Store,
    chunk: &ChunkRef,
    access: Access<'_>,
) -> Result<(ReplayManifest, Vec<FinancialEvent>), String> {
    let chunk_key = manifest_key(&chunk.generation);
    let mut bytes = Vec::new();
    store.read_to(&chunk_key, None, &mut bytes)?;
    let chunk_uri = store.uri(&chunk_key);
    let chunk_manifest =
        ReplayManifest::from_json(&bytes).map_err(|error| format!("{chunk_uri}: {error}"))?;
    if chunk_manifest.summary_identity != chunk.summary_identity
        || chunk_manifest.role.to_string() != chunk.role
    {
        return Err(format!("{uri}: {chunk_uri} is not the recorded chunk"));
    }
    replay::verify_replay(&chunk_uri, store, &chunk_key, &bytes, access)?;
    let events = chunk_events(store, &chunk_manifest)?;
    Ok((chunk_manifest, events))
}

/// Verifies a read family against its bound generations and every referenced replay, returning
/// the number of members replayed on development data.
fn verify_read_family(
    uri: &str,
    store: &Store,
    manifest: &FamilyManifest,
    family: &Family,
    access: Access<'_>,
) -> Result<usize, String> {
    if manifest.schema_version == search::STREAMED_FAMILY_SCHEMA_VERSION {
        return verify_read_family_streamed(uri, store, manifest, family, access);
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
    for (index, member) in family.members.iter().enumerate() {
        let candidate = &candidates[index / contracts];
        let expected: Vec<_> = candidate
            .conditions
            .iter()
            .map(|&i| conditions[i].clone())
            .collect();
        if member.logic_identity != candidate.logic_identity
            || member.conditions != expected
            || member.contract != settings.contracts[index % contracts].id
        {
            return Err(format!(
                "{uri}: member {index} is not the enumerated member"
            ));
        }
    }
    // The bound development (and evaluation) generations are the manifest's inputs.
    let development = bind_development(settings, access)?;
    if development.plan_identity != family.plan_identity
        || family.base_stream != settings.base_stream
    {
        return Err(format!(
            "{uri}: the bound development plan is not the recorded plan and base stream"
        ));
    }
    let mut inputs = vec![development.input.clone()];
    if settings.evaluation.is_some() {
        inputs.push(bind_evaluation(settings, &development, access)?);
    }
    if manifest.inputs != inputs {
        return Err(format!(
            "{uri}: the manifest inputs are not the bound generations"
        ));
    }
    // Every referenced replay restores through its verifier.
    let mut clock = Clock::default();
    let read_chunk = |chunk: &ChunkRef| read_verified_chunk(uri, store, chunk, access);
    let lowering = family
        .lowering
        .as_ref()
        .ok_or_else(|| format!("{uri}: schema-1 family has no lowering"))?;
    let (_, lowering_events) = read_chunk(lowering)?;
    let expected_lowering =
        search::lowering_replay(settings, &family.plan_identity, &development.instrument);
    if restored_definition(&lowering_events)?.replay != expected_lowering
        || lowering.bindings
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
    // Raw counts, scores, adjustments, screen decisions, gates and ranks are recomputed by the
    // owners that published them and compared field by field.
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
    let currency = settings.account.currency.to_string();
    let mut expected = family.members.clone();
    for (member, raw) in expected.iter_mut().zip(raw) {
        member.raw = raw;
    }
    let applicable = search::score(&mut expected, &settings.contracts, settings.screen.as_ref());
    search::rank(&mut expected, &settings.gates, &currency);
    if family.applicable != applicable {
        return Err(format!(
            "{uri}: the applicable count {} is not {applicable}",
            family.applicable
        ));
    }
    for (index, (member, expected)) in family.members.iter().zip(&expected).enumerate() {
        if member != expected {
            return Err(format!(
                "{uri}: member {index} records counts, scores, screen, gate, or rank the family does not produce"
            ));
        }
    }
    // Every chunk is the table synthesized for its members; groups, split groups and stability
    // agree with the verified records; every member is replayed exactly as its status requires.
    let mut seen: BTreeMap<(String, String), ()> = BTreeMap::new();
    let mut replayed = 0;
    for chunk in &family.chunks {
        let (chunk_manifest, events) = read_chunk(chunk)?;
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
        let table = search::replay_table(
            settings,
            role,
            window,
            &development.instrument,
            &members,
            settings.account.initial_cash,
        );
        if restored_definition(&events)?.replay != table {
            return Err(format!(
                "{uri}: chunk {} is not the table synthesized for its recorded members",
                chunk.generation
            ));
        }
        let summary = Summary::from_json(&read_object(
            store,
            &chunk_manifest.objects,
            SUMMARY_OBJECT_PATH,
        )?)
        .map_err(|error| format!("{uri}: {}: {error}", chunk.generation))?;
        let splits = if role == DatasetRole::Evaluation {
            search::project_splits(events.iter().cloned(), &currency)
        } else {
            BTreeMap::new()
        };
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
            let matches = if role == DatasetRole::Development {
                replayed += 1;
                member.development.as_ref() == Some(&group)
            } else {
                member.evaluation.as_ref() == Some(&group)
                    && member.evaluation_splits == splits.get(id).cloned().unwrap_or_default()
            };
            if !matches {
                return Err(format!(
                    "{uri}: member {index} records {} groups its replay {} does not hold",
                    chunk.role, chunk.generation
                ));
            }
            if member.rank.is_some() {
                let series = settled_profits(&events, id, settings.account.scale);
                let stability =
                    resample(&Backend::Cpu, settings, member, role, &series, &mut clock)?;
                if member.stability.get(&chunk.role) != Some(&stability) {
                    return Err(format!(
                        "{uri}: member {index} records {} stability its settlements do not produce",
                        chunk.role
                    ));
                }
            }
        }
    }
    for (index, member) in family.members.iter().enumerate() {
        let id = format!("m{index}");
        let replayed_development = seen.contains_key(&(id.clone(), "development".into()));
        let replayed_evaluation = seen.contains_key(&(id, "evaluation".into()));
        let expect_evaluation = member.rank.is_some() && settings.evaluation.is_some();
        let roles: Vec<&str> = member.stability.keys().map(String::as_str).collect();
        let expected_roles: &[&str] = match (member.rank.is_some(), expect_evaluation) {
            (true, true) => &["development", "evaluation"],
            (true, false) => &["development"],
            _ => &[],
        };
        if replayed_development != member.screened.is_none()
            || member.development.is_some() != replayed_development
            || replayed_evaluation != expect_evaluation
            || member.evaluation.is_some() != expect_evaluation
            || (!member.evaluation_splits.is_empty() && !expect_evaluation)
            || roles != expected_roles
        {
            return Err(format!(
                "{uri}: member {index} is not replayed and resampled exactly as its status requires"
            ));
        }
    }
    Ok(replayed)
}

fn verify_read_family_streamed(
    uri: &str,
    store: &Store,
    manifest: &FamilyManifest,
    family: &Family,
    access: Access<'_>,
) -> Result<usize, String> {
    let settings = &family.search;
    settings
        .validate()
        .map_err(|reason| format!("{uri}: search.{reason}"))?;
    let development = bind_development(settings, access)?;
    if family.plan_identity != development.plan_identity
        || family.base_stream != settings.base_stream
    {
        return Err(format!(
            "{uri}: bound development plan differs from the family"
        ));
    }
    let resolved = search::resolve_conditions(settings, &development.bound.plan)?;
    if family.resolved_conditions.as_ref() != Some(&resolved.conditions)
        || family.resolved_hash.as_ref() != Some(&resolved.hash)
        || manifest.members != resolved.members
    {
        return Err(format!(
            "{uri}: resolved rules, hash, or enumerated count differs from the fitted development plan"
        ));
    }
    let conditions = &resolved.conditions;
    let (lowered_conditions, lowered_bindings) =
        lowered_conditions(&development.bound.plan, conditions);
    let lowering_events = match (
        &family.lowering,
        lowered_conditions.is_empty() || resolved.members == 0,
    ) {
        (None, true) => Vec::new(),
        (Some(_), true) | (None, false) => {
            return Err(format!(
                "{uri}: lowering reference does not match the lowered conditions"
            ));
        }
        (Some(lowering), false) => {
            if lowering.role != "development" {
                return Err(format!("{uri}: lowering replay must have development role"));
            }
            let (_, events) = read_verified_chunk(uri, store, lowering, access)?;
            let table = search::lowering_replay_for(
                settings,
                &family.plan_identity,
                &development.instrument,
                &lowered_conditions,
            );
            if restored_definition(&events)?.replay != table
                || lowering.bindings
                    != table
                        .strategies
                        .iter()
                        .map(|strategy| strategy.id.clone())
                        .collect::<Vec<_>>()
            {
                return Err(format!(
                    "{uri}: lowering replay is not the table for its fallback conditions"
                ));
            }
            events
        }
    };
    let backends: Vec<Backend> = match access.verified.and_then(|cache| cache.devices()) {
        Some(devices) => devices
            .iter()
            .map(|&ordinal| Backend::cuda(ordinal))
            .collect::<Result<_, _>>()?,
        None => vec![Backend::Cpu],
    };
    let mut clock = Clock::default();
    let mut compact = if resolved.members == 0 {
        Vec::new()
    } else {
        let scored = score_streamed(
            settings,
            &development,
            conditions,
            &lowering_events,
            &lowered_bindings,
            &backends,
            access.verified.and_then(|cache| cache.devices()),
            &mut clock,
        )?;
        if let Some(cache) = access.verified {
            cache.note_family_rescore();
        }
        scored
    };
    let (applicable, survivors) = screen_compact(&mut compact, settings)?;
    if applicable != family.applicable
        || survivors
            != family
                .members
                .iter()
                .map(|member| member.global_index.expect("schema-2 parsed member"))
                .collect::<Vec<_>>()
    {
        return Err(format!(
            "{uri}: applicable count or indexed survivors differ from full-family screening"
        ));
    }
    let mut expected = family.members.clone();
    for (member, &global) in expected.iter_mut().zip(&survivors) {
        let identity = streamed_member(global, settings, &family.plan_identity, conditions)?;
        if member.global_index != identity.global_index
            || member.logic_identity != identity.logic_identity
            || member.conditions != identity.conditions
            || member.contract != identity.contract
        {
            return Err(format!(
                "{uri}: member {global} is not the enumerated member"
            ));
        }
        compact[global as usize].apply(
            member,
            &settings.contracts[global as usize % settings.contracts.len()],
            settings.screen.as_ref(),
        );
    }
    let currency = settings.account.currency.to_string();
    search::rank(&mut expected, &settings.gates, &currency);
    for (member, expected) in family.members.iter().zip(&expected) {
        if member != expected {
            return Err(format!(
                "{uri}: member {} records counts, scores, screen, gate, or rank the family does not produce",
                member.global_index.expect("schema-2 parsed member")
            ));
        }
    }
    let expected_chunks: Vec<(String, Vec<String>)> = [
        ("development", family.members.iter().collect::<Vec<_>>()),
        (
            "evaluation",
            if settings.evaluation.is_some() {
                family
                    .members
                    .iter()
                    .filter(|member| member.rank.is_some())
                    .collect()
            } else {
                Vec::new()
            },
        ),
    ]
    .into_iter()
    .flat_map(|(role, members)| {
        members
            .chunks(settings.chunk_size as usize)
            .map(move |chunk| {
                (
                    role.to_string(),
                    chunk
                        .iter()
                        .map(|member| format!("m{}", member.global_index.expect("schema-2 member")))
                        .collect(),
                )
            })
            .collect::<Vec<_>>()
    })
    .collect();
    if family
        .chunks
        .iter()
        .map(|chunk| (chunk.role.clone(), chunk.bindings.clone()))
        .collect::<Vec<_>>()
        != expected_chunks
    {
        return Err(format!(
            "{uri}: chunk bindings are not canonical survivor partitions"
        ));
    }
    let mut seen: BTreeMap<(String, String), ()> = BTreeMap::new();
    let mut replayed = 0;
    for phase in ["development", "evaluation"] {
        if phase == "evaluation" {
            for member in &family.members {
                let global = member.global_index.expect("schema-2 parsed member");
                if !seen.contains_key(&(format!("m{global}"), "development".into()))
                    || member.development.is_none()
                    || member.stability.contains_key("development") != member.rank.is_some()
                {
                    return Err(format!(
                        "{uri}: member {global} is not replayed and resampled exactly as its status requires"
                    ));
                }
            }
            let mut inputs = vec![development.input.clone()];
            if resolved.members > 0 && settings.evaluation.is_some() {
                inputs.push(bind_evaluation(settings, &development, access)?);
            }
            if manifest.inputs != inputs {
                return Err(format!(
                    "{uri}: manifest inputs are not the bound generations"
                ));
            }
        }
        for chunk in family.chunks.iter().filter(|chunk| chunk.role == phase) {
            let (chunk_manifest, events) = read_verified_chunk(uri, store, chunk, access)?;
            let members = chunk_members_streamed(
                &chunk.bindings,
                settings,
                &family.plan_identity,
                conditions,
            )?;
            let (role, window) = match chunk.role.as_str() {
                "development" => (DatasetRole::Development, &settings.development),
                "evaluation" => (
                    DatasetRole::Evaluation,
                    settings
                        .evaluation
                        .as_ref()
                        .ok_or_else(|| format!("{uri}: evaluation chunk without window"))?,
                ),
                other => return Err(format!("{uri}: chunk role `{other}`")),
            };
            let table = search::replay_table(
                settings,
                role,
                window,
                &development.instrument,
                &members,
                settings.account.initial_cash,
            );
            if restored_definition(&events)?.replay != table {
                return Err(format!(
                    "{uri}: chunk {} is not the table synthesized for its recorded members",
                    chunk.generation
                ));
            }
            let summary = Summary::from_json(&read_object(
                store,
                &chunk_manifest.objects,
                SUMMARY_OBJECT_PATH,
            )?)
            .map_err(|error| format!("{uri}: {}: {error}", chunk.generation))?;
            let splits = if role == DatasetRole::Evaluation {
                search::project_splits(events.iter().cloned(), &currency)
            } else {
                BTreeMap::new()
            };
            for (id, _, _) in &members {
                let global: u64 = id[1..].parse().expect("checked");
                let position = survivors
                    .binary_search(&global)
                    .map_err(|_| format!("{uri}: chunk replays screened member {global}"))?;
                if seen.insert((id.clone(), chunk.role.clone()), ()).is_some() {
                    return Err(format!(
                        "{uri}: member {global} is replayed twice for {}",
                        chunk.role
                    ));
                }
                let member = &family.members[position];
                let group = summary.strategies.get(id).cloned().unwrap_or_default();
                let matches = if role == DatasetRole::Development {
                    replayed += 1;
                    member.development.as_ref() == Some(&group)
                } else {
                    member.evaluation.as_ref() == Some(&group)
                        && member.evaluation_splits == splits.get(id).cloned().unwrap_or_default()
                };
                if !matches {
                    return Err(format!(
                        "{uri}: member {global} records groups its replay does not hold"
                    ));
                }
                if member.rank.is_some() {
                    let series = settled_profits(&events, id, settings.account.scale);
                    let stability =
                        resample(&Backend::Cpu, settings, member, role, &series, &mut clock)?;
                    if member.stability.get(&chunk.role) != Some(&stability) {
                        return Err(format!(
                            "{uri}: member {global} records stability its settlements do not produce"
                        ));
                    }
                }
            }
        }
    }
    for member in &family.members {
        let global = member.global_index.expect("schema-2 parsed member");
        let id = format!("m{global}");
        let development_seen = seen.contains_key(&(id.clone(), "development".into()));
        let evaluation_seen = seen.contains_key(&(id, "evaluation".into()));
        let evaluation_expected = member.rank.is_some() && settings.evaluation.is_some();
        let roles: Vec<&str> = member.stability.keys().map(String::as_str).collect();
        let expected_roles: &[&str] = match (member.rank.is_some(), evaluation_expected) {
            (true, true) => &["development", "evaluation"],
            (true, false) => &["development"],
            _ => &[],
        };
        if !development_seen
            || member.development.is_none()
            || evaluation_seen != evaluation_expected
            || member.evaluation.is_some() != evaluation_expected
            || (!member.evaluation_splits.is_empty() && !evaluation_expected)
            || roles != expected_roles
        {
            return Err(format!(
                "{uri}: member {global} is not replayed and resampled exactly as its status requires"
            ));
        }
    }
    Ok(replayed)
}

#[cfg(test)]
mod projection_tests {
    use super::*;
    use binary_alpha_engine::config::StreamKey;
    use binary_alpha_engine::execution::{Comparator, Condition, Threshold};
    use binary_alpha_engine::features::{FittedEncoding, ProjectionKind};

    #[test]
    fn preallocation_failure_halves_width_and_keeps_exact_column_coverage() {
        let lengths = [2, 0, 3, 1, 4];
        let shape = kernels::ScreenShape {
            rows: 8,
            slots: 2,
            batch: 3,
            tile_strides: vec![72],
            largest_tile: 8,
            local_hint_bytes: 0,
        };
        let score = |plan: &kernels::ColumnBlockPlan| {
            plan.blocks
                .iter()
                .flat_map(|block| block.columns.clone())
                .map(|column| lengths[column] * (column + 1))
                .sum::<usize>()
        };
        let expected =
            score(&kernels::plan_screen_blocks(&lengths, &shape, 1_000_000, None, 5).unwrap());
        let mut attempts = 0;
        let mut completed = [false; 5];
        let mut visits = [0_u8; 5];
        let mut counts = 0;
        let (plan, ()) = with_screen_plan(&lengths, &shape, 1_000_000, None, |plan| {
            attempts += 1;
            for column in plan.blocks.iter().flat_map(|block| block.columns.clone()) {
                if completed[column] {
                    continue;
                }
                if attempts == 1 && column == 2 {
                    return Err("screen preallocation failed: injected".into());
                }
                completed[column] = true;
                visits[column] += 1;
                counts += lengths[column] * (column + 1);
            }
            Ok(())
        })
        .unwrap();
        assert_eq!(attempts, 2);
        assert_eq!(
            plan.blocks
                .iter()
                .map(|block| block.columns.len())
                .collect::<Vec<_>>(),
            [2, 2, 1]
        );
        assert_eq!(
            plan.blocks
                .iter()
                .flat_map(|block| block.columns.clone())
                .collect::<Vec<_>>(),
            [0, 1, 2, 3, 4]
        );
        assert_eq!(counts, expected);
        assert_eq!(visits, [1; 5], "completed columns are skipped on re-plan");
    }

    #[test]
    fn wide_search_budget_uses_resolved_condition_slots() {
        let rows = 4_440_960;
        let slots = condition_slots(1_000, 1);
        assert_eq!(slots, 1);
        let plan =
            kernels::plan_column_blocks(&[rows], rows, slots, 1_024, 1, 1, 8_151 * 1024 * 1024)
                .unwrap();
        assert_eq!(plan.blocks.len(), 1);
        assert_eq!(plan.blocks[0].columns, 0..1);
    }

    #[test]
    fn wide_block_tuples_visit_only_feasible_combinations() {
        let blocks: Vec<_> = (0..100)
            .map(|column| kernels::ColumnBlock {
                columns: column..column + 1,
            })
            .collect();
        let suffix_capacity: Vec<_> = (0..=100).map(|block| 100 - block).collect();
        let mut visited = 0;
        let mut ranks = Vec::new();
        visit_block_tuples(
            &blocks,
            &suffix_capacity,
            99,
            0,
            &mut Vec::new(),
            &mut |tuple| {
                assert_eq!(tuple.len(), 99);
                assert!(tuple.windows(2).all(|pair| pair[0] < pair[1]));
                visited += 1;
                visit_tuple_conditions(&blocks, tuple, 0, 0, &mut Vec::new(), &mut |chosen| {
                    ranks.push(search::member_rank(100, 99, 99, 1, chosen, 0).unwrap());
                    Ok(())
                })?;
                Ok(())
            },
        )
        .unwrap();
        assert_eq!(visited, 100);
        assert_eq!(search::family_size(100, 99, 99, 1), Some(100));
        ranks.sort_unstable();
        assert_eq!(ranks, (0..100).collect::<Vec<_>>());
    }

    fn legacy_plan() -> binary_alpha_engine::features::FeaturePlan {
        let root =
            Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/legacy_schema1/published");
        let manifest: binary_alpha_engine::features::FeatureManifest = serde_json::from_slice(
            &fs::read(root.join("manifests/a7ccab4e17b84ad665de5b29c9ccbca10df2b926d7dfe7a3c4987267092f8f29/ready.json")).unwrap(),
        ).unwrap();
        let object = manifest
            .objects
            .iter()
            .find(|object| object.path == "plan.json")
            .unwrap();
        binary_alpha_engine::features::FeaturePlan::from_json(
            &fs::read(root.join(&object.key)).unwrap(),
        )
        .unwrap()
    }

    #[test]
    fn cpu_batches_merge_by_global_member_index() {
        let codes = [0_i16; 3];
        let ordered = [0_i64, 1, 2];
        let entry = [1_i64, 2, 3];
        let release = [2_i64, 3, 4];
        let valid = [1_u8; 3];
        let buy = [1_u8, 0, 1];
        let sell = [0_u8, 1, 0];
        let tie = [0_u8; 3];
        let mask = [1_u8; 3];
        let rows = [0_i32, 1, 2];
        let offsets = [0_i32, 3];
        let tuple = kernels::CpuSparseTuple::new(
            kernels::SearchBuffers {
                feature_codes: &codes,
                feature_count: 1,
                row_count: 3,
                ordered_rows: &ordered,
                decision_time_ms: &entry,
                release_time_ms: &release,
                settlement_time_ms: &release,
                valid: &valid,
                buy_win: &buy,
                sell_win: &sell,
                tie: &tie,
            },
            &[&mask],
            kernels::SparseKeys {
                key_chrono_offsets: &offsets,
                key_chrono_rows: &rows,
            },
        )
        .unwrap();
        let features = [0_i32];
        let buckets = [0_i16];
        let candidate_offsets = [0_i32, 1];
        let drivers = [0_i32];
        let candidates = kernels::CandidateConditions {
            condition_feature: &features,
            condition_bucket: &buckets,
            candidate_offsets: &candidate_offsets,
            candidate_count: 1,
        };
        let batches = [
            CpuBatch {
                global_indices: &[7],
                candidates,
                driver_keys: &drivers,
            },
            CpuBatch {
                global_indices: &[2],
                candidates,
                driver_keys: &drivers,
            },
        ];
        let scored = score_cpu_batches(&tuple, &batches, 0, 1, 92).unwrap();
        assert_eq!(scored.iter().map(|item| item.0).collect::<Vec<_>>(), [2, 7]);
        assert_eq!(scored[0].1, scored[1].1);
        assert!(
            score_cpu_batches(&tuple, &[batches[0], batches[0]], 0, 1, 92)
                .unwrap_err()
                .contains("repeat")
        );
    }

    fn stream(duration_seconds: u32, offset_seconds: u32) -> StreamKey {
        StreamKey {
            duration_seconds,
            offset_seconds,
        }
    }

    #[test]
    fn availability_join_and_stored_entry_mask_match_replay_order() {
        let base = stream(5, 0);
        let offset = stream(15, 5);
        let index = ProjectionIndex::from_clocks(
            vec![base, offset],
            vec![
                vec![(15, 20), (25, 30), (35, 40)],
                vec![(5, 10), (20, 20), (25, 30), (35, 40)],
            ],
            base,
            vec![20, 30, 38],
        )
        .unwrap();
        assert_eq!(index.latest(0, 1), Some(1)); // Same-time close 20 replaces close 5.
        assert!(index.closes[1][index.latest(0, 1).unwrap()] > index.base[0].close);
        assert_eq!(index.latest(1, 1), Some(2));
        assert_eq!(index.slot_mask(20, 50), [1, 1, 0]); // Close 35 has entry 38 before install 40.
        assert_eq!(index.slot_mask(30, 40), [0, 1, 0]); // Windows use installation, not close.
        assert_eq!(index.base[0].entry, index.base[0].installed); // Tick-finalized row keeps its stored cell.
        let scored = kernels::score_bucket_plans_cap1_basic_dual(
            &Backend::Cpu,
            &[1, 1, 1],
            &[0],
            &[1],
            &[0, 1],
            &index.slot_mask(0, 50),
            &[0, 1, 2],
            &[20, 30, 38], // Stored entry ticks, not close or installation clocks.
            &[21, 31, 39],
            &[1, 1, 1],
            &[1, 0, 1],
            &[0, 1, 0],
            &[0, 0, 0],
            1,
            3,
            1,
            0,
        )
        .unwrap();
        assert_eq!(scored.output.buy_output[0], 2);
        assert_eq!(scored.output.buy_output[1], 1); // The tick-finalized row's stored win.
        assert_eq!(scored.output.buy_output[2], 1);
    }

    #[test]
    fn projection_block_uses_latest_same_time_condition_row() {
        let base = stream(5, 0);
        let condition = stream(15, 5);
        let mut plan = legacy_plan();
        let mut condition_plan = plan.streams[0].clone();
        condition_plan.duration_seconds = condition.duration_seconds;
        condition_plan.offset_seconds = condition.offset_seconds;
        condition_plan.encodings.push(FittedEncoding {
            output: "direction_encoded".into(),
            input: "candle_direction".into(),
            automatic: true,
            encoding: ProjectionKind::Category,
            edges: None,
            input_divisor: 1.0,
            labels: vec!["up".into(), "down".into()],
        });
        plan.streams.push(condition_plan);
        let index = ProjectionIndex::from_clocks(
            vec![base, condition],
            vec![vec![(15, 20)], vec![(5, 5), (20, 20)]],
            base,
            vec![20],
        )
        .unwrap();
        // Both condition rows carry the retained label. The paired engine parity case
        // `later close replaces earlier` proves replay rejects this same condition and clock.
        let condition = Condition {
            stream: condition,
            output: "direction_encoded".into(),
            comparator: Comparator::Eq,
            threshold: Threshold::Text("up".into()),
        };
        let row_codes = [0, 0];
        let (codes, buckets) = projection_block(
            &plan,
            &index,
            &[condition],
            &[0],
            &BTreeMap::new(),
            &BTreeMap::new(),
            |_| Ok(&row_codes),
        )
        .unwrap();
        assert_eq!(index.latest(0, 1), Some(1));
        assert_eq!(codes, [-1]);
        assert_eq!(buckets, [0]);
        assert_eq!(index.slot_mask(20, 21), [1]);
    }

    #[test]
    fn retained_fitted_label_projects_and_dropped_label_uses_lowering() {
        let mut plan = legacy_plan();
        let stream = plan.streams[0].key();
        plan.streams[0].encodings.push(FittedEncoding {
            output: "direction_encoded".into(),
            input: "candle_direction".into(),
            automatic: true,
            encoding: ProjectionKind::Category,
            edges: None,
            input_divisor: 1.0,
            labels: vec!["up".into()],
        });
        let condition = |label: &str| Condition {
            stream,
            output: "direction_encoded".into(),
            comparator: Comparator::Eq,
            threshold: Threshold::Text(label.into()),
        };
        assert_eq!(projected_bucket(&plan, &condition("up")), Some(0));
        assert_eq!(projected_bucket(&plan, &condition("down")), None);
        assert_eq!(
            projected_bucket(
                &plan,
                &Condition {
                    output: "candle_direction".into(),
                    ..condition("up")
                }
            ),
            None
        );
    }

    #[test]
    fn requested_block_matches_immutable_engine_lowering_signals() {
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/legacy_schema1/published");
        let manifest = |generation: &str| {
            fs::read(root.join("manifests").join(generation).join("ready.json")).unwrap()
        };
        let tick: binary_alpha_engine::dataset::GenerationManifest = serde_json::from_slice(
            &manifest("f2f1b9cd5e4ca1db4e4afcfec207f63eb81320019fbf379056e7fdee2d1a7d33"),
        )
        .unwrap();
        let feature: binary_alpha_engine::features::FeatureManifest = serde_json::from_slice(
            &manifest("bc01ba9344f078a18659c982b05c74801d43ae44e2eaff729e8e7defa60bf05c"),
        )
        .unwrap();
        let outcome: OutcomeManifest = serde_json::from_slice(&manifest(
            "a377abdc0fdc92a8c71d2e98163cad588803d64c256d145cfbf2bfe8b2fc993f",
        ))
        .unwrap();
        let family_manifest: FamilyManifest = serde_json::from_slice(&manifest(
            "d7924115f6ec1c2219d5239081221020315db2bf998d950c10cb60d194c53f67",
        ))
        .unwrap();
        let plan_object = feature
            .objects
            .iter()
            .find(|object| object.path == "plan.json")
            .unwrap();
        let mut plan = binary_alpha_engine::features::FeaturePlan::from_json(
            &fs::read(root.join(&plan_object.key)).unwrap(),
        )
        .unwrap();
        let stream = plan.streams[0].key();
        plan.streams[0].encodings.push(FittedEncoding {
            output: "direction_encoded".into(),
            input: "candle_direction".into(),
            automatic: true,
            encoding: ProjectionKind::Category,
            edges: None,
            input_divisor: 1.0,
            labels: vec!["up".into(), "down".into()],
        });
        let development = Development {
            plan_identity: family_manifest.inputs[0].plan_identity.clone(),
            instrument: tick.instrument.clone(),
            base_stream: stream,
            input: family_manifest.inputs[0].clone(),
            outcome,
            outcome_store: Store::filesystem(&root),
            bound: crate::outcomes::Bound {
                scale: plan.price_scale,
                tick,
                tick_store: Store::filesystem(&root),
                feature,
                feature_store: Store::filesystem(&root),
                plan,
            },
        };
        let conditions = vec![
            Condition {
                stream,
                output: "direction_encoded".into(),
                comparator: Comparator::Eq,
                threshold: Threshold::Text("up".into()),
            },
            Condition {
                stream,
                output: "candle_direction".into(),
                comparator: Comparator::Eq,
                threshold: Threshold::Text("down".into()),
            },
        ];
        let (index, projected) = projection_index(&development, &conditions).unwrap();
        let family =
            Family::from_json(&fs::read(root.join(&family_manifest.objects[0].key)).unwrap())
                .unwrap();
        let lowering = family.lowering.as_ref().unwrap();
        let lowering_manifest: ReplayManifest =
            serde_json::from_slice(&manifest(&lowering.generation)).unwrap();
        let events_object = lowering_manifest
            .objects
            .iter()
            .find(|object| object.path == EVENTS_OBJECT_PATH)
            .unwrap();
        let event_bytes = fs::read(root.join(&events_object.key)).unwrap();
        let signals: Vec<FinancialEvent> = event_bytes
            .split(|&byte| byte == b'\n')
            .filter(|line| !line.is_empty())
            .map(|line| FinancialEvent::from_line(line).unwrap())
            .collect();
        let mut bindings = BTreeMap::new();
        bindings.insert(1, "c1".into());
        let signals_by_binding = lowering_rows_by_binding(&index, &signals, &bindings).unwrap();
        let (codes, buckets) = projection_block_from_development(
            &development,
            &index,
            &conditions,
            &[0, 1],
            &projected,
            &signals_by_binding,
            &bindings,
        )
        .unwrap();
        let references: Vec<i64> = index.base.iter().map(|row| row.close).collect();
        let lowered = lowering_codes(&signals, &references, 2).unwrap();
        let n = references.len();
        assert_eq!(buckets, [0, 1]);
        assert!(lowered[..n].contains(&1));
        for row in 0..n {
            assert_eq!(
                i16::from(codes[row] == 0),
                lowered[row],
                "projected row {row}"
            );
            assert_eq!(codes[n + row], lowered[n + row], "fallback row {row}");
        }
        crate::replay::ROW_CURSOR_OPENS.with(|count| count.set(0));
        let mut clock = Clock::default();
        score_streamed(
            &family.search,
            &development,
            &conditions,
            &signals,
            &bindings,
            &[Backend::Cpu],
            None,
            &mut clock,
        )
        .unwrap();
        crate::replay::ROW_CURSOR_OPENS.with(|count| {
            assert_eq!(count.get(), development.bound.plan.streams.len());
        });
    }
}

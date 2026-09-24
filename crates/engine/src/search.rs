//! Candidate search: deterministic enumeration over a typed condition menu, complete-family
//! identity, the one-sided exact binomial model score with Benjamini-Hochberg adjustment, the
//! deterministic circular stationary-block sampler, and the development gates and ranking.
//! Pure logic only: no device, storage, or engine calls.
//!
//! The scores are model-based diagnostics. Independent fixed-probability trials and independence
//! across hypotheses are not established for this workload, so nothing here claims a controlled
//! false-discovery rate, an interval guarantee, or positive expected return.

use std::cmp::Ordering;
use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::config::{Replay, Scope, Screen, Search, SearchCondition, SearchWindow, StreamKey};
use crate::dataset::{DatasetRole, ObjectRecord};
use crate::execution::{
    AccountSpec, Condition, ContractTerms, Decimal, DeploymentBinding, Disposition, EventKind,
    FinancialEvent, Group, Resolution, SettlementRule, StrategySpec, signal_logic_identity,
};
use crate::features::{FeaturePlan, Kind};
use crate::market::parse_event_time_micros;

/// The manifest kind of a published search family.
pub const FAMILY_MANIFEST_KIND: &str = "search_family";
pub const FAMILY_SCHEMA_VERSION: u32 = 1;
/// Survivor-only family format written by search; schema 1 remains readable.
pub const STREAMED_FAMILY_SCHEMA_VERSION: u32 = 2;
/// The one object of a family generation.
pub const FAMILY_OBJECT_PATH: &str = "family.json";
/// The frozen sampler identity recorded with every stability result.
pub const SAMPLER_VERSION: &str = "stationary_block_sha256_v1";
const FAMILY_DOMAIN_V1: &[u8] = b"binary-alpha search family v1\n";
const SAMPLER_DOMAIN_V1: &[u8] = b"binary-alpha search sampler v1\n";

// ----------------------------------------------------------------------------------------------
// Configuration validation and replay tables
// ----------------------------------------------------------------------------------------------

/// The rules of the `search` table a single field's deserializer cannot see; an error names the
/// field. The synthesized replay tables are validated by the engine's own rules.
pub fn validate(search: &Search) -> Result<(), String> {
    if search.envelope.settlement_rule == SettlementRule::BrokerAuthoritativeV1
        || search
            .contracts
            .iter()
            .any(|contract| contract.settlement.rule == SettlementRule::BrokerAuthoritativeV1)
    {
        return Err("contracts/envelope: broker_authoritative_v1 settlement needs a broker; research and historical replay use price_at_due_v1".into());
    }
    if search.chunk_size == 0 {
        return Err("chunk_size: must be positive".to_string());
    }
    if search.max_candidates == 0 {
        return Err("max_candidates: must be positive".to_string());
    }
    if search.min_conditions == 0 || search.min_conditions > search.max_conditions {
        return Err(format!(
            "min_conditions: {} must be at least one and at most max_conditions {}",
            search.min_conditions, search.max_conditions
        ));
    }
    if search.conditions.is_empty() {
        return Err("conditions: at least one menu entry is required".to_string());
    }
    for (index, entry) in search.conditions.iter().enumerate() {
        match entry {
            SearchCondition::Named(entry) if entry.thresholds.is_empty() => {
                return Err(format!(
                    "conditions[{index}].thresholds: at least one threshold is required"
                ));
            }
            SearchCondition::Named(entry) if entry.output == "*" => {
                return Err(format!(
                    "conditions[{index}].output: `*` requires a generation rule without thresholds"
                ));
            }
            SearchCondition::Generate(entry)
                if entry.output != "*" || entry.comparator != crate::execution::Comparator::Eq =>
            {
                return Err(format!(
                    "conditions[{index}]: a generation rule requires output `*` and comparator `eq`"
                ));
            }
            _ => {}
        }
    }
    if search.contracts.is_empty() {
        return Err("contracts: at least one contract is required".to_string());
    }
    for (index, contract) in search.contracts.iter().enumerate() {
        let same_terms = |other: &ContractTerms| {
            let mut renamed = normalized_terms(other);
            renamed.id = contract.id.clone();
            renamed == normalized_terms(contract)
        };
        if let Some(earlier) = search.contracts[..index].iter().position(same_terms) {
            return Err(format!(
                "contracts[{index}]: repeats every term of contracts[{earlier}]; one hypothesis per contract"
            ));
        }
    }
    for (name, limit) in [
        (
            "max_open_per_duration",
            search.risk_policy.max_open_per_duration,
        ),
        (
            "max_open_per_instrument",
            search.risk_policy.max_open_per_instrument,
        ),
        ("max_open_total", search.risk_policy.max_open_total),
    ] {
        if limit.is_some() {
            return Err(format!(
                "risk_policy.{name}: a cross-account scope would let members interact; leave it absent"
            ));
        }
    }
    if search.risk_policy.max_unresolved_loss_total.is_some() {
        return Err("risk_policy.max_unresolved_loss_total: a cross-account scope would let members interact; leave it absent".to_string());
    }
    for (name, window) in [
        ("development", Some(&search.development)),
        ("evaluation", search.evaluation.as_ref()),
    ] {
        if window.is_some_and(|window| window.inputs.len() != 1) {
            return Err(format!(
                "{name}.inputs: exactly one instrument input is required"
            ));
        }
    }
    match (search.scope, &search.screen) {
        (Scope::Heuristic, None) => {
            return Err("screen: heuristic scope requires the screen table".to_string());
        }
        (Scope::Exhaustive, Some(_)) => {
            return Err("screen: exhaustive scope does not screen".to_string());
        }
        (_, Some(screen)) if !(0.0..=1.0).contains(&screen.max_adjusted_score) => {
            return Err(format!(
                "screen.max_adjusted_score: {} must lie in [0, 1]",
                screen.max_adjusted_score
            ));
        }
        (_, Some(Screen { top: Some(0), .. })) => {
            return Err("screen.top: must be positive".to_string());
        }
        _ => {}
    }
    for (name, value) in [
        ("block_length", search.stability.block_length),
        ("simulations", search.stability.simulations),
        ("rolling_horizon", search.stability.rolling_horizon),
    ] {
        if value == 0 {
            return Err(format!("stability.{name}: must be positive"));
        }
    }
    let generated = search
        .conditions
        .iter()
        .any(|entry| matches!(entry, SearchCondition::Generate(_)));
    if !generated {
        validate_size(search, conditions(&search.conditions).len(), false)?;
    }
    if search.embargo_micros < 0 {
        return Err("embargo_micros: must not be negative".to_string());
    }
    for (index, contract) in search.contracts.iter().enumerate() {
        let required = contract
            .settlement_horizon()
            .map_err(|reason| format!("contracts[{index}].{reason}"))?;
        if search.embargo_micros < required {
            return Err(format!(
                "embargo_micros: {} is shorter than contracts[{index}]'s duration plus settlement delay {required}",
                search.embargo_micros
            ));
        }
    }
    if let Some(window) = &search.evaluation
        && window
            .splits
            .iter()
            .flatten()
            .any(|split| split.name == "none")
    {
        return Err("evaluation.splits: `none` names the undeclared split".to_string());
    }
    let placeholder_instrument = format!("{}:validation", search.account.broker);
    let placeholder = [Condition {
        stream: search.base_stream,
        output: "validation_only".into(),
        comparator: crate::execution::Comparator::Eq,
        threshold: crate::execution::Threshold::Bool(true),
    }];
    let named = conditions(&search.conditions);
    let lowering = if generated && named.is_empty() {
        lowering_replay_for(search, "validation", &placeholder_instrument, &placeholder)
    } else {
        lowering_replay_for(search, "validation", &placeholder_instrument, &named)
    };
    crate::execution::validate(&lowering)?;
    if let Some(evaluation) = &search.evaluation {
        let development_end = parse_event_time_micros(&search.development.decision_end)?;
        let evaluation_start = parse_event_time_micros(&evaluation.decision_start)?;
        if evaluation_start - development_end < search.embargo_micros {
            return Err(format!(
                "evaluation.decision_start: {} begins less than the embargo after development.decision_end {}",
                evaluation.decision_start, search.development.decision_end
            ));
        }
        let members: Vec<(String, StrategySpec, usize)> = lowering
            .strategies
            .into_iter()
            .map(|strategy| (strategy.id.clone(), strategy, 0))
            .collect();
        crate::execution::validate(&replay_table(
            search,
            DatasetRole::Evaluation,
            evaluation,
            &placeholder_instrument,
            &members,
            search.account.initial_cash,
        ))
        .map_err(|reason| format!("evaluation.{reason}"))?;
    }
    Ok(())
}

fn validate_size(search: &Search, count: usize, allow_short: bool) -> Result<u64, String> {
    if !allow_short && search.max_conditions as usize > count {
        return Err(format!(
            "max_conditions: {} exceeds the {} distinct conditions of the menu",
            search.max_conditions, count
        ));
    }
    let size = family_size(
        count,
        search.min_conditions as usize,
        search.max_conditions as usize,
        search.contracts.len(),
    )
    .ok_or("conditions: the family size overflows")?;
    if size > search.max_candidates {
        return Err(format!(
            "max_candidates: the menu enumerates {size} members, above the maximum {}",
            search.max_candidates
        ));
    }
    Ok(size)
}

/// The ordered concrete table and its canonical identity, available to search and verification.
#[derive(Debug)]
pub struct ResolvedConditions {
    pub conditions: Vec<Condition>,
    pub hash: String,
    pub members: u64,
}

/// Resolve all generation rules against the frozen development plan, then validate the complete
/// lowering table and bound family size before any candidate allocation.
pub fn resolve_conditions(
    search: &Search,
    plan: &FeaturePlan,
) -> Result<ResolvedConditions, String> {
    validate(search)?;
    let mut resolved = Vec::new();
    for (index, entry) in search.conditions.iter().enumerate() {
        match entry {
            SearchCondition::Named(entry) => {
                for threshold in &entry.thresholds {
                    let condition = Condition {
                        stream: entry.stream,
                        output: entry.output.clone(),
                        comparator: entry.comparator,
                        threshold: threshold.clone(),
                    };
                    if !resolved.contains(&condition) {
                        resolved.push(condition);
                    }
                }
            }
            SearchCondition::Generate(rule) => {
                let stream = plan.stream(rule.stream).ok_or_else(|| {
                    format!(
                        "conditions[{index}].stream: {} is not in the development plan",
                        rule.stream
                    )
                })?;
                for encoding in &stream.encodings {
                    if encoding.labels.len() <= 1
                        || stream
                            .outputs
                            .iter()
                            .any(|output| output.name == encoding.output)
                    {
                        continue;
                    }
                    let unready = &plan.readiness_of(&encoding.input).unready;
                    for label in &encoding.labels {
                        if !crate::features::coded_label(label) || unready.contains(label) {
                            continue;
                        }
                        let condition = Condition {
                            stream: rule.stream,
                            output: encoding.output.clone(),
                            comparator: crate::execution::Comparator::Eq,
                            threshold: crate::execution::Threshold::Text(label.clone()),
                        };
                        if !resolved.contains(&condition) {
                            resolved.push(condition);
                        }
                    }
                }
            }
        }
    }
    for (index, condition) in resolved.iter().enumerate() {
        let stream = plan.stream(condition.stream).ok_or_else(|| {
            format!(
                "resolved conditions[{index}].stream: {} is not in the development plan",
                condition.stream
            )
        })?;
        let (kind, source) = if let Some(output) = stream
            .outputs
            .iter()
            .find(|output| output.name == condition.output)
        {
            (output.kind, output.name.as_str())
        } else if let Some(encoding) = stream
            .encodings
            .iter()
            .find(|encoding| encoding.output == condition.output)
        {
            if !stream
                .outputs
                .iter()
                .any(|output| output.name == encoding.input)
            {
                return Err(format!(
                    "resolved conditions[{index}].output: encoding `{}` has no compiled input `{}`",
                    encoding.output, encoding.input
                ));
            }
            (Kind::Text, encoding.input.as_str())
        } else {
            return Err(format!(
                "resolved conditions[{index}].output: `{}` is not a compiled output or fitted encoding of stream {}",
                condition.output, condition.stream
            ));
        };
        if !matches!(
            (&condition.threshold, kind),
            (crate::execution::Threshold::Text(_), Kind::Text)
                | (crate::execution::Threshold::Bool(_), Kind::Bool)
                | (
                    crate::execution::Threshold::Number(_),
                    Kind::Int | Kind::Float | Kind::Time
                )
        ) {
            return Err(format!(
                "resolved conditions[{index}]: `{}` is a {kind} output; the threshold type does not match",
                condition.output
            ));
        }
        for flag in plan.readiness_of(source).flags {
            if !stream.outputs.iter().any(|output| output.name == flag)
                && !stream
                    .encodings
                    .iter()
                    .any(|encoding| encoding.output == flag)
            {
                return Err(format!(
                    "resolved conditions[{index}].output: readiness flag `{flag}` of `{}` is absent from stream {}",
                    condition.output, condition.stream
                ));
            }
        }
    }
    let members = validate_size(search, resolved.len(), true)?;
    let placeholder = format!("{}:validation", search.account.broker);
    let lowering = lowering_replay_for(search, "validation", &placeholder, &resolved);
    // An empty resolved table has no replay strategies; the next stage publishes an empty family.
    if !resolved.is_empty() {
        crate::execution::validate(&lowering)?;
    }
    if let Some(evaluation) = &search.evaluation {
        let members: Vec<_> = lowering
            .strategies
            .into_iter()
            .map(|strategy| (strategy.id.clone(), strategy, 0))
            .collect();
        if !members.is_empty() {
            crate::execution::validate(&replay_table(
                search,
                DatasetRole::Evaluation,
                evaluation,
                &placeholder,
                &members,
                search.account.initial_cash,
            ))
            .map_err(|reason| format!("evaluation.{reason}"))?;
        }
    }
    let mut hasher = Sha256::new();
    hasher.update(b"binary-alpha resolved search conditions v1\n");
    hasher.update(serde_json::to_vec(&resolved).expect("conditions serialize"));
    Ok(ResolvedConditions {
        conditions: resolved,
        hash: crate::hex(&hasher.finalize()),
        members,
    })
}

/// The contract with every amount normalized, so equal money written at different scales
/// compares equal.
fn normalized_terms(contract: &ContractTerms) -> ContractTerms {
    let cashflow = |cashflow: crate::execution::Cashflow| crate::execution::Cashflow {
        gross_return: cashflow.gross_return.normalized(),
        terminal_fee: cashflow.terminal_fee.normalized(),
    };
    ContractTerms {
        stake: contract.stake.normalized(),
        quoted_cost: contract.quoted_cost.normalized(),
        entry_fee: contract.entry_fee.normalized(),
        win: cashflow(contract.win),
        loss: cashflow(contract.loss),
        tie: cashflow(contract.tie),
        ..contract.clone()
    }
}

/// The development replay that lowers every distinct condition of the menu: one unfunded
/// account and one single-condition strategy per condition, so every base row where a
/// condition holds is a `signal` record and nothing is ever funded.
pub fn lowering_replay(search: &Search, plan_identity: &str, instrument: &str) -> Replay {
    lowering_replay_for(
        search,
        plan_identity,
        instrument,
        &conditions(&search.conditions),
    )
}

/// Lower an already resolved table; unlike menu lowering, this includes generated conditions.
pub fn lowering_replay_for(
    search: &Search,
    plan_identity: &str,
    instrument: &str,
    conditions: &[Condition],
) -> Replay {
    let members: Vec<(String, StrategySpec, usize)> = (0..conditions.len())
        .map(|index| {
            let id = format!("c{index}");
            (
                id.clone(),
                strategy(&id, plan_identity, search.base_stream, conditions, &[index]),
                0,
            )
        })
        .collect();
    replay_table(
        search,
        DatasetRole::Development,
        &search.development,
        instrument,
        &members,
        Decimal::zero(0),
    )
}

/// One replay table of `members`, each `(id, strategy, contract index)` on its own account
/// funded with `initial_cash`, under the search's single risk policy and envelope.
pub fn replay_table(
    search: &Search,
    role: DatasetRole,
    window: &SearchWindow,
    instrument: &str,
    members: &[(String, StrategySpec, usize)],
    initial_cash: Decimal,
) -> Replay {
    Replay {
        role,
        decision_start: window.decision_start.clone(),
        decision_end: window.decision_end.clone(),
        inputs: window.inputs.clone(),
        splits: window.splits.clone(),
        accounts: members
            .iter()
            .map(|(id, _, _)| AccountSpec {
                id: id.clone(),
                broker: search.account.broker.clone(),
                currency: search.account.currency.clone(),
                scale: search.account.scale,
                initial_cash,
            })
            .collect(),
        strategies: members
            .iter()
            .map(|(_, strategy, _)| strategy.clone())
            .collect(),
        bindings: members
            .iter()
            .map(|(id, _, contract)| DeploymentBinding {
                id: id.clone(),
                strategy: id.clone(),
                account: id.clone(),
                instrument: instrument.to_string(),
                contract: search.contracts[*contract].id.clone(),
                risk_policy: search.risk_policy.id.clone(),
                envelope: search.envelope.clone(),
            })
            .collect(),
        contracts: search.contracts.clone(),
        risk_policies: vec![search.risk_policy.clone()],
        reporting_currency: search.account.currency.clone(),
        reporting_scale: search.account.scale,
        max_rate_age_micros: 0,
        rates: None,
        scenario: None,
    }
}

// ----------------------------------------------------------------------------------------------
// Enumeration
// ----------------------------------------------------------------------------------------------

/// Expands the menu into distinct conditions in menu order.
pub fn conditions(menu: &[SearchCondition]) -> Vec<Condition> {
    let mut conditions: Vec<Condition> = Vec::new();
    for entry in menu {
        let SearchCondition::Named(entry) = entry else {
            continue;
        };
        for threshold in &entry.thresholds {
            let condition = Condition {
                stream: entry.stream,
                output: entry.output.clone(),
                comparator: entry.comparator,
                threshold: threshold.clone(),
            };
            if !conditions.contains(&condition) {
                conditions.push(condition);
            }
        }
    }
    conditions
}

/// The number of members `count` conditions produce with `min..=max` conditions per candidate
/// and `contracts` contracts each, before identity deduplication; `None` on overflow.
pub fn family_size(count: usize, min: usize, max: usize, contracts: usize) -> Option<u64> {
    let mut total = 0_u64;
    for k in min..=max.min(count) {
        if k == 0 {
            continue;
        }
        let choose = binomial(count, k)?;
        total = total.checked_add(choose)?;
    }
    total.checked_mul(u64::try_from(contracts).ok()?)
}

/// Exact coefficient while it fits a family index. With k <= n/2 coefficients increase at
/// every step, so an intermediate coefficient above u64 cannot later become representable.
fn binomial(n: usize, k: usize) -> Option<u64> {
    if k > n {
        return Some(0);
    }
    let k = k.min(n - k);
    let mut value = 1_u128;
    for i in 1..=k {
        value = value.checked_mul((n - k + i) as u128)? / i as u128;
        if value > u64::MAX as u128 {
            return None;
        }
    }
    u64::try_from(value).ok()
}

/// Global member index in size/lexicographic order, with contracts fastest.
pub fn member_rank(
    count: usize,
    min: usize,
    max: usize,
    contracts: usize,
    indices: &[usize],
    contract: usize,
) -> Option<u64> {
    if indices.is_empty()
        || indices.len() < min
        || indices.len() > max.min(count)
        || contracts == 0
        || contract >= contracts
    {
        return None;
    }
    let mut rank = 0_u64;
    for size in min.max(1)..indices.len() {
        rank = rank.checked_add(binomial(count, size)?)?;
    }
    let mut first = 0;
    for (position, &index) in indices.iter().enumerate() {
        if index < first || index >= count {
            return None;
        }
        for skipped in first..index {
            rank =
                rank.checked_add(binomial(count - skipped - 1, indices.len() - position - 1)?)?;
        }
        first = index + 1;
    }
    rank.checked_mul(u64::try_from(contracts).ok()?)?
        .checked_add(u64::try_from(contract).ok()?)
}

/// Inverse of `member_rank` for a bounded family.
pub fn member_unrank(
    count: usize,
    min: usize,
    max: usize,
    contracts: usize,
    global: u64,
) -> Option<(Vec<usize>, usize)> {
    let total = family_size(count, min, max, contracts)?;
    if contracts == 0 || global >= total {
        return None;
    }
    let contracts = u64::try_from(contracts).ok()?;
    let contract = (global % contracts) as usize;
    let mut ordinal = global / contracts;
    for size in min.max(1)..=max.min(count) {
        let group = binomial(count, size)?;
        if ordinal >= group {
            ordinal -= group;
            continue;
        }
        let mut indices = Vec::with_capacity(size);
        let mut first = 0;
        for position in 0..size {
            let mut selected = None;
            for index in first..count {
                let following = binomial(count - index - 1, size - position - 1)?;
                if ordinal < following {
                    selected = Some(index);
                    break;
                }
                ordinal -= following;
            }
            let index = selected?;
            indices.push(index);
            first = index + 1;
        }
        return Some((indices, contract));
    }
    None
}

/// Streams bounded members without allocating the family or its candidate conjunctions.
pub fn stream_members(
    count: usize,
    min: usize,
    max: usize,
    contracts: usize,
) -> Option<MemberStream> {
    let total = family_size(count, min, max, contracts)?;
    let size = min.max(1);
    Some(MemberStream {
        count,
        max: max.min(count),
        contracts,
        total,
        global: 0,
        contract: 0,
        indices: if total == 0 {
            Vec::new()
        } else {
            (0..size).collect()
        },
    })
}

/// Current conjunction and contract are the only retained enumeration state.
pub struct MemberStream {
    count: usize,
    max: usize,
    contracts: usize,
    total: u64,
    global: u64,
    contract: usize,
    indices: Vec<usize>,
}

impl Iterator for MemberStream {
    type Item = (u64, Vec<usize>, usize);

    fn next(&mut self) -> Option<Self::Item> {
        if self.global == self.total {
            return None;
        }
        let item = (self.global, self.indices.clone(), self.contract);
        self.global += 1;
        self.contract += 1;
        if self.contract == self.contracts {
            self.contract = 0;
            let size = self.indices.len();
            let mut position = size;
            while position > 0 && self.indices[position - 1] == self.count - size + position - 1 {
                position -= 1;
            }
            if position > 0 {
                self.indices[position - 1] += 1;
                for later in position..size {
                    self.indices[later] = self.indices[later - 1] + 1;
                }
            } else if size < self.max {
                self.indices = (0..size + 1).collect();
            }
        }
        Some(item)
    }
}

/// Every combination of `min..=max` distinct indices below `count`, in lexicographic order.
pub fn combinations(count: usize, min: usize, max: usize) -> Vec<Vec<usize>> {
    let mut all = Vec::new();
    for k in min..=max.min(count) {
        if k == 0 {
            continue;
        }
        let mut current: Vec<usize> = (0..k).collect();
        loop {
            all.push(current.clone());
            let mut position = k;
            while position > 0 && current[position - 1] == count - k + position - 1 {
                position -= 1;
            }
            if position == 0 {
                break;
            }
            current[position - 1] += 1;
            for later in position..k {
                current[later] = current[later - 1] + 1;
            }
        }
    }
    all
}

/// One candidate conjunction and its signal-logic identity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Candidate {
    pub conditions: Vec<usize>,
    pub logic_identity: String,
}

/// The candidates of the menu in enumeration order, deduplicated by logic identity: the first
/// combination owning an identity keeps it.
pub fn candidates(
    plan_identity: &str,
    base_stream: StreamKey,
    conditions: &[Condition],
    min: usize,
    max: usize,
) -> Vec<Candidate> {
    let mut seen = std::collections::HashSet::new();
    let mut candidates = Vec::new();
    for combination in combinations(conditions.len(), min, max) {
        let logic_identity = signal_logic_identity(&strategy(
            "",
            plan_identity,
            base_stream,
            conditions,
            &combination,
        ));
        if seen.insert(logic_identity.clone()) {
            candidates.push(Candidate {
                conditions: combination,
                logic_identity,
            });
        }
    }
    candidates
}

/// The executable strategy of one candidate.
pub fn strategy(
    id: &str,
    plan_identity: &str,
    base_stream: StreamKey,
    conditions: &[Condition],
    selected: &[usize],
) -> StrategySpec {
    StrategySpec {
        id: id.to_string(),
        plan_identity: plan_identity.to_string(),
        base_stream,
        conditions: selected.iter().map(|&i| conditions[i].clone()).collect(),
        repair: Vec::new(),
    }
}

// ----------------------------------------------------------------------------------------------
// Model score
// ----------------------------------------------------------------------------------------------

/// The decisive-count null of one contract: fixed net win `W`, positive net loss `L`, and the
/// break-even win rate `L / (W + L)`.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
pub struct Null {
    pub net_win: Decimal,
    pub net_loss: Decimal,
    pub break_even: f64,
}

/// Derives the null from the contract's exact terms, including every fee. The statistic applies
/// only when `W >= 0`, `L > 0`, and a tie nets exactly zero; `W = 0` gives a break-even of one.
pub fn null_rate(contract: &ContractTerms) -> Result<Null, String> {
    let purchase = contract.purchase()?;
    let net_win = contract.winning_net()?;
    let net_loss = purchase
        .checked_add(contract.loss.terminal_fee)?
        .checked_sub(contract.loss.gross_return)?;
    let tie_net = contract
        .tie
        .gross_return
        .checked_sub(purchase)?
        .checked_sub(contract.tie.terminal_fee)?;
    if net_win.is_negative() {
        return Err(format!("net win {net_win} is negative"));
    }
    if !net_loss.is_negative() && net_loss.is_zero() || net_loss.is_negative() {
        return Err(format!("net loss {net_loss} is not positive"));
    }
    if !tie_net.is_zero() {
        return Err(format!("a tie nets {tie_net}, not zero"));
    }
    let scale = net_win.scale().max(net_loss.scale());
    let win = net_win.rescale(scale)?.coefficient() as f64;
    let loss = net_loss.rescale(scale)?.coefficient() as f64;
    Ok(Null {
        net_win,
        net_loss,
        break_even: loss / (win + loss),
    })
}

fn ln_factorial(n: u64) -> f64 {
    if n < 2 {
        return 0.0;
    }
    if n < 256 {
        return (2..=n).map(|i| (i as f64).ln()).sum();
    }
    let x = n as f64;
    (x + 0.5) * x.ln() - x + 0.5 * (2.0 * std::f64::consts::PI).ln() + 1.0 / (12.0 * x)
        - 1.0 / (360.0 * x.powi(3))
        + 1.0 / (1260.0 * x.powi(5))
}

fn ln_add(left: f64, right: f64) -> f64 {
    if left == f64::NEG_INFINITY {
        return right;
    }
    if right == f64::NEG_INFINITY {
        return left;
    }
    let larger = left.max(right);
    larger + (left.min(right) - larger).exp().ln_1p()
}

/// The one-sided exact binomial upper tail `P(X >= wins)` over `wins + losses` decisive trials
/// at the null win rate, summed in log space away from the mode until the remaining terms are
/// below the representable precision of the sum.
pub fn upper_tail(wins: u64, losses: u64, break_even: f64) -> f64 {
    let n = wins + losses;
    if n == 0 || wins == 0 || break_even >= 1.0 {
        return 1.0;
    }
    if break_even <= 0.0 {
        return 0.0;
    }
    let (lp, lq) = (break_even.ln(), (-break_even).ln_1p());
    let ln_term = |k: u64| -> f64 {
        ln_factorial(n) - ln_factorial(k) - ln_factorial(n - k)
            + k as f64 * lp
            + (n - k) as f64 * lq
    };
    let negligible = |term: f64, total: f64| term < total - 40.0;
    let value = if (wins as f64) <= n as f64 * break_even {
        // Below the mode: one minus the lower tail, summed downward from `wins - 1`.
        let mut k = wins - 1;
        let mut term = ln_term(k);
        let mut total = term;
        while k > 0 {
            term += (k as f64).ln() - ((n - k + 1) as f64).ln() - lp + lq;
            k -= 1;
            total = ln_add(total, term);
            if negligible(term, total) {
                break;
            }
        }
        1.0 - total.exp()
    } else {
        let mut k = wins;
        let mut term = ln_term(k);
        let mut total = term;
        while k < n {
            term += ((n - k) as f64).ln() - ((k + 1) as f64).ln() + lp - lq;
            k += 1;
            total = ln_add(total, term);
            if negligible(term, total) {
                break;
            }
        }
        total.exp()
    };
    value.clamp(0.0, 1.0)
}

/// Benjamini-Hochberg adjusted values over one complete family: ranks by value then position,
/// scales each by `m / rank`, takes the running minimum from the largest rank, and clips to one.
pub fn benjamini_hochberg(values: &[f64]) -> Vec<f64> {
    let m = values.len();
    let mut order: Vec<usize> = (0..m).collect();
    order.sort_by(|&a, &b| {
        values[a]
            .partial_cmp(&values[b])
            .unwrap_or(Ordering::Equal)
            .then(a.cmp(&b))
    });
    let mut adjusted = vec![0.0; m];
    let mut running = 1.0_f64;
    for (rank, &index) in order.iter().enumerate().rev() {
        running = running.min(values[index] * m as f64 / (rank + 1) as f64);
        adjusted[index] = running.clamp(0.0, 1.0);
    }
    adjusted
}

// ----------------------------------------------------------------------------------------------
// Sampler and stability
// ----------------------------------------------------------------------------------------------

/// Rejection-sampled uniform draws from SHA-256 in counter mode over the sampler domain, the
/// seed, the stratum, and the replicate.
struct Draws {
    prefix: Vec<u8>,
    counter: u64,
    buffer: Vec<u64>,
}

impl Draws {
    fn new(seed: u64, stratum: &str, replicate: u32) -> Self {
        let mut prefix = SAMPLER_DOMAIN_V1.to_vec();
        prefix.extend_from_slice(&seed.to_le_bytes());
        prefix.extend_from_slice(stratum.as_bytes());
        prefix.push(0);
        prefix.extend_from_slice(&replicate.to_le_bytes());
        Self {
            prefix,
            counter: 0,
            buffer: Vec::new(),
        }
    }

    fn next(&mut self) -> u64 {
        if self.buffer.is_empty() {
            let mut hasher = Sha256::new();
            hasher.update(&self.prefix);
            hasher.update(self.counter.to_le_bytes());
            self.counter += 1;
            let digest = hasher.finalize();
            self.buffer = digest
                .chunks(8)
                .rev()
                .map(|chunk| u64::from_le_bytes(chunk.try_into().expect("eight bytes")))
                .collect();
        }
        // Words are consumed in digest order; the buffer holds them reversed.
        self.buffer.pop().expect("refilled")
    }

    /// A uniform integer below `modulus`, rejecting the biased top of the 64-bit range.
    fn uniform(&mut self, modulus: u64) -> u64 {
        let limit = (u64::MAX / modulus) * modulus;
        loop {
            let draw = self.next();
            if draw < limit {
                return draw % modulus;
            }
        }
    }
}

/// One circular stationary-block index path of length `n`: the first index is uniform, then
/// each step restarts uniformly with probability `1 / block_length` or advances circularly.
pub fn block_path(
    seed: u64,
    stratum: &str,
    replicate: u32,
    n: usize,
    block_length: u32,
) -> Vec<usize> {
    let mut draws = Draws::new(seed, stratum, replicate);
    let mut path = Vec::with_capacity(n);
    if n == 0 {
        return path;
    }
    let mut index = draws.uniform(n as u64) as usize;
    path.push(index);
    for _ in 1..n {
        index = if draws.uniform(u64::from(block_length)) == 0 {
            draws.uniform(n as u64) as usize
        } else {
            (index + 1) % n
        };
        path.push(index);
    }
    path
}

/// The `q` quantile of ascending `sorted` by linear interpolation between order statistics.
pub fn quantile(sorted: &[f64], q: f64) -> f64 {
    let position = q * (sorted.len() - 1) as f64;
    let lower = position.floor() as usize;
    let upper = position.ceil() as usize;
    sorted[lower] + (sorted[upper] - sorted[lower]) * (position - lower as f64)
}

/// The stability summary of one stratum's resampled paths.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
pub struct Stability {
    pub sampler: String,
    pub trade_count: u64,
    pub simulations: u32,
    pub block_length: u32,
    pub rolling_horizon: u32,
    pub median_max_drawdown: f64,
    pub p95_max_drawdown: f64,
    pub p95_longest_underwater: f64,
    /// Negative rolling windows over all complete windows of every path.
    pub negative_rolling_share: f64,
}

/// Summarizes raw path metrics; the caller drew the paths and ran the primitive.
pub fn stability(
    trade_count: u64,
    block_length: u32,
    rolling_horizon: u32,
    max_drawdowns: &[f64],
    longest_underwater: &[i64],
    negative_rolling: &[i64],
) -> Stability {
    let mut drawdowns = max_drawdowns.to_vec();
    drawdowns.sort_by(|a, b| a.partial_cmp(b).unwrap_or(Ordering::Equal));
    let mut underwater: Vec<f64> = longest_underwater.iter().map(|&v| v as f64).collect();
    underwater.sort_by(|a, b| a.partial_cmp(b).unwrap_or(Ordering::Equal));
    let windows =
        max_drawdowns.len() as f64 * (trade_count - u64::from(rolling_horizon) + 1) as f64;
    Stability {
        sampler: SAMPLER_VERSION.to_string(),
        trade_count,
        simulations: max_drawdowns.len() as u32,
        block_length,
        rolling_horizon,
        median_max_drawdown: quantile(&drawdowns, 0.5),
        p95_max_drawdown: quantile(&drawdowns, 0.95),
        p95_longest_underwater: quantile(&underwater, 0.95),
        negative_rolling_share: negative_rolling.iter().sum::<i64>() as f64 / windows,
    }
}

/// A stability result or the exact reason none is available.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum StabilityOutcome {
    Available(Stability),
    Unavailable { reason: String },
}

// ----------------------------------------------------------------------------------------------
// Gates and ranking
// ----------------------------------------------------------------------------------------------

/// The development gates every replayed member must pass to rank.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Gates {
    pub min_settled: u64,
    pub max_unresolved: u64,
    pub min_net_profit: Decimal,
}

/// The completed profit of one group in `currency`: zero when the group never settled and
/// recorded no total, and unavailable when the engine recorded an unrepresentable total.
pub fn profit(group: &Group, currency: &str) -> Option<Decimal> {
    match group.profit.get(currency) {
        Some(total) => *total,
        None if group.settled == 0 => Some(Decimal::zero(0)),
        None => None,
    }
}

/// The per-binding, per-admission-split groups of one ledger: `signal` records carry the
/// binding and split, `accepted`, `released`, `settled` and `unresolved` records are attributed
/// through their command's signal. Decisions outside every declared split fall under `none`.
pub fn project_splits(
    events: impl IntoIterator<Item = FinancialEvent>,
    currency: &str,
) -> BTreeMap<String, BTreeMap<String, Group>> {
    let mut groups: BTreeMap<String, BTreeMap<String, Group>> = BTreeMap::new();
    let mut commands: BTreeMap<String, (String, String)> = BTreeMap::new();
    let mut open = std::collections::BTreeSet::new();
    let mut accepted = std::collections::BTreeSet::new();
    let mut unresolved = std::collections::BTreeSet::new();
    for event in events {
        match event.kind {
            EventKind::Signal {
                binding,
                split,
                disposition,
                command,
                ..
            } => {
                let split = split.unwrap_or_else(|| "none".to_string());
                let group = groups
                    .entry(binding.clone())
                    .or_default()
                    .entry(split.clone())
                    .or_default();
                group.signals += 1;
                *group
                    .dispositions
                    .entry(disposition.to_string())
                    .or_default() += 1;
                if disposition == Disposition::Admitted
                    && let Some(command) = command
                {
                    group.open += 1;
                    open.insert(command.clone());
                    commands.insert(command, (binding, split));
                }
            }
            EventKind::Accepted { command, .. }
            | EventKind::Reconciled {
                command,
                resolution: Resolution::Purchased { .. } | Resolution::Accepted { .. },
                ..
            } => {
                if open.contains(&command)
                    && accepted.insert(command.clone())
                    && let Some(group) = group_of(&mut groups, &commands, &command)
                {
                    group.accepted += 1;
                    group.unresolved -= u64::from(unresolved.remove(&command));
                }
            }
            EventKind::Released { command, .. }
            | EventKind::Reconciled {
                command,
                resolution: Resolution::NotSent,
                ..
            } => {
                if open.remove(&command)
                    && let Some(group) = group_of(&mut groups, &commands, &command)
                {
                    group.close(unresolved.remove(&command));
                    group.released += 1;
                }
            }
            EventKind::Settled {
                command,
                outcome,
                profit,
                ..
            }
            | EventKind::Reconciled {
                command,
                resolution: Resolution::Settled { outcome, .. },
                profit: Some(profit),
                ..
            } => {
                if open.remove(&command)
                    && let Some(group) = group_of(&mut groups, &commands, &command)
                {
                    group.close(unresolved.remove(&command));
                    group.outcome(outcome);
                    group.add_profit(currency, profit);
                }
            }
            EventKind::Reconciled {
                command,
                resolution: Resolution::ExternallyClosed { .. },
                profit: Some(profit),
                ..
            } => {
                if open.remove(&command)
                    && let Some(group) = group_of(&mut groups, &commands, &command)
                {
                    group.externally_closed += 1;
                    group.close(unresolved.remove(&command));
                    group.add_profit(currency, profit);
                }
            }
            EventKind::Unresolved { command, .. } | EventKind::PossiblySent { command, .. } => {
                if open.contains(&command)
                    && let Some(group) = group_of(&mut groups, &commands, &command)
                {
                    group.unresolved += u64::from(unresolved.insert(command));
                }
            }
            _ => {}
        }
    }
    groups
}

fn group_of<'a>(
    groups: &'a mut BTreeMap<String, BTreeMap<String, Group>>,
    commands: &BTreeMap<String, (String, String)>,
    command: &str,
) -> Option<&'a mut Group> {
    let (binding, split) = commands.get(command)?;
    groups.get_mut(binding)?.get_mut(split)
}

/// Why a development group fails the gates, or `Ok` when it passes.
pub fn gate(group: &Group, currency: &str, gates: &Gates) -> Result<(), String> {
    if group.settled < gates.min_settled {
        return Err(format!(
            "settled {} below the minimum {}",
            group.settled, gates.min_settled
        ));
    }
    if group.unresolved > gates.max_unresolved {
        return Err(format!(
            "unresolved {} above the maximum {}",
            group.unresolved, gates.max_unresolved
        ));
    }
    let Some(profit) = profit(group, currency) else {
        return Err("net profit is unavailable".to_string());
    };
    if profit.compare(gates.min_net_profit)? == Ordering::Less {
        return Err(format!(
            "net profit {profit} below the minimum {}",
            gates.min_net_profit
        ));
    }
    Ok(())
}

/// Scores every member from its raw counts and contract, adjusts over the applicable subset
/// of the complete family, and applies the heuristic screen when one is configured. Returns
/// the applicable count. Members are in canonical order, contracts cycling fastest.
pub fn score(members: &mut [Member], contracts: &[ContractTerms], screen: Option<&Screen>) -> u64 {
    for (index, member) in members.iter_mut().enumerate() {
        let contract = &contracts[index % contracts.len()];
        (
            member.null,
            member.inapplicable,
            member.score,
            member.adjusted,
            member.screened,
        ) = match null_rate(contract) {
            Ok(null) => {
                let score = upper_tail(
                    member.raw.wins.max(0) as u64,
                    member.raw.losses.max(0) as u64,
                    null.break_even,
                );
                (Some(null), None, Some(score), None, None)
            }
            Err(reason) => (
                None,
                Some(reason.clone()),
                None,
                None,
                screen.map(|_| format!("inapplicable: {reason}")),
            ),
        };
    }
    let applicable: Vec<usize> = (0..members.len())
        .filter(|&index| members[index].score.is_some())
        .collect();
    let adjusted = benjamini_hochberg(
        &applicable
            .iter()
            .map(|&index| members[index].score.expect("applicable"))
            .collect::<Vec<_>>(),
    );
    for (&index, &value) in applicable.iter().zip(&adjusted) {
        members[index].adjusted = Some(value);
    }
    if let Some(screen) = screen {
        let mut order = applicable.clone();
        order.sort_by(|&a, &b| {
            members[a]
                .adjusted
                .partial_cmp(&members[b].adjusted)
                .unwrap_or(Ordering::Equal)
                .then(a.cmp(&b))
        });
        for (position, &index) in order.iter().enumerate() {
            let adjusted = members[index].adjusted.expect("applicable");
            members[index].screened = if adjusted > screen.max_adjusted_score {
                Some(format!(
                    "adjusted score {adjusted} above the maximum {}",
                    screen.max_adjusted_score
                ))
            } else if screen.top.is_some_and(|top| position >= top as usize) {
                Some(format!(
                    "beyond the first {} members by adjusted score",
                    screen.top.expect("set")
                ))
            } else {
                None
            };
        }
    }
    applicable.len() as u64
}

/// Minimal whole-family screening state; contract and identity are recovered from the global
/// member index only for retained records. Input records must be in global index order.
#[derive(Debug, Clone, PartialEq)]
pub struct CompactMember {
    pub raw: RawCounts,
    pub score: Option<f64>,
    pub adjusted: Option<f64>,
}

impl CompactMember {
    pub fn new(raw: RawCounts) -> Self {
        Self {
            raw,
            score: None,
            adjusted: None,
        }
    }

    /// Populate the existing member fields after the whole-family screen.
    pub fn apply(&self, member: &mut Member, contract: &ContractTerms, screen: Option<&Screen>) {
        member.raw = self.raw.clone();
        match null_rate(contract) {
            Ok(null) => {
                member.null = Some(null);
                member.inapplicable = None;
                member.score = self.score;
                member.adjusted = self.adjusted;
                member.screened = None;
            }
            Err(reason) => {
                member.null = None;
                member.inapplicable = Some(reason.clone());
                member.score = None;
                member.adjusted = None;
                member.screened = screen.map(|_| format!("inapplicable: {reason}"));
            }
        }
    }
}

/// Returns the applicable count and exactly the unscreened global indices, in family order.
/// The BH calculation and both tie breaks match `score` over the complete family.
pub fn screen_compact(
    records: &mut [CompactMember],
    contracts: &[ContractTerms],
    screen: Option<&Screen>,
) -> (u64, Vec<u64>) {
    assert!(!contracts.is_empty(), "a family has at least one contract");
    let nulls: Vec<_> = contracts.iter().map(null_rate).collect();
    let mut applicable = Vec::new();
    let mut scores = Vec::new();
    for (index, record) in records.iter_mut().enumerate() {
        record.adjusted = None;
        record.score = nulls[index % contracts.len()].as_ref().ok().map(|null| {
            upper_tail(
                record.raw.wins.max(0) as u64,
                record.raw.losses.max(0) as u64,
                null.break_even,
            )
        });
        if let Some(score) = record.score {
            applicable.push(index);
            scores.push(score);
        }
    }
    for (&index, adjusted) in applicable.iter().zip(benjamini_hochberg(&scores)) {
        records[index].adjusted = Some(adjusted);
    }
    let mut retained = vec![screen.is_none(); records.len()];
    if let Some(screen) = screen {
        let mut order = applicable.clone();
        order.sort_by(|&a, &b| {
            records[a]
                .adjusted
                .partial_cmp(&records[b].adjusted)
                .unwrap_or(Ordering::Equal)
                .then(a.cmp(&b))
        });
        for (position, index) in order.into_iter().enumerate() {
            retained[index] = records[index].adjusted.is_some_and(|adjusted| {
                adjusted <= screen.max_adjusted_score
                    && screen.top.is_none_or(|top| position < top as usize)
            });
        }
    }
    (
        applicable.len() as u64,
        retained
            .into_iter()
            .enumerate()
            .filter_map(|(i, keep)| keep.then_some(i as u64))
            .collect(),
    )
}

/// Gates every replayed member on its development group and ranks the passing members: net
/// profit descending, settled count descending, then member order. Returns the passing members
/// in rank order.
pub fn rank(members: &mut [Member], gates: &Gates, currency: &str) -> Vec<usize> {
    let mut passed = Vec::new();
    for (index, member) in members.iter_mut().enumerate() {
        member.rank = None;
        member.rejected = match (&member.screened, &member.development) {
            (None, Some(group)) => {
                let rejected = gate(group, currency, gates).err();
                if rejected.is_none() {
                    passed.push(index);
                }
                rejected
            }
            _ => None,
        };
    }
    passed.sort_by(|&a, &b| {
        let (x, y) = (
            members[a].development.as_ref().expect("replayed"),
            members[b].development.as_ref().expect("replayed"),
        );
        match (profit(x, currency), profit(y, currency)) {
            (Some(p), Some(q)) => q.compare(p).unwrap_or(Ordering::Equal),
            _ => Ordering::Equal,
        }
        .then(y.settled.cmp(&x.settled))
    });
    for (rank, &index) in passed.iter().enumerate() {
        members[index].rank = Some(rank as u32 + 1);
    }
    passed
}

// ----------------------------------------------------------------------------------------------
// Published family
// ----------------------------------------------------------------------------------------------

/// The raw device counts of one member: diagnostic, never a financial result.
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize, Serialize)]
pub struct RawCounts {
    pub total: i64,
    pub wins: i64,
    pub losses: i64,
    pub ties: i64,
    pub invalid: i64,
}

/// One published member and everything computed for it.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
pub struct Member {
    /// Schema-2 global family index; absent from schema-1 bytes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub global_index: Option<u64>,
    pub logic_identity: String,
    pub conditions: Vec<Condition>,
    pub contract: String,
    pub raw: RawCounts,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub null: Option<Null>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub inapplicable: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub score: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub adjusted: Option<f64>,
    /// The heuristic elimination reason; an eliminated member is never replayed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub screened: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub development: Option<Group>,
    /// The development gate failure; a rejected member is not ranked or evaluated.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rejected: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rank: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub evaluation: Option<Group>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub evaluation_splits: BTreeMap<String, Group>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub stability: BTreeMap<String, StabilityOutcome>,
}

/// One replay generation the family produced.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct ChunkRef {
    pub role: String,
    pub generation: String,
    pub summary_identity: String,
    pub bindings: Vec<String>,
}

/// A published family: declared rules, the bound plan, and its member records. Schema 2 stores
/// only retained members; backend and timings stay outside.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
pub struct Family {
    /// Schema-2 marker; omitted from schema-1 JSON.
    #[serde(default = "schema_one", skip_serializing_if = "is_schema_one")]
    pub schema_version: u32,
    pub search: Search,
    /// The resolved concrete table and hash retained beside schema-2 search rules.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resolved_conditions: Option<Vec<Condition>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resolved_hash: Option<String>,
    pub plan_identity: String,
    pub base_stream: StreamKey,
    pub kernel_module: String,
    pub sampler: String,
    pub applicable: u64,
    pub members: Vec<Member>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lowering: Option<ChunkRef>,
    pub chunks: Vec<ChunkRef>,
}

impl Family {
    pub fn to_json(&self) -> Vec<u8> {
        let mut bytes = serde_json::to_vec_pretty(self).expect("a family serializes");
        bytes.push(b'\n');
        bytes
    }

    pub fn from_json(bytes: &[u8]) -> Result<Self, String> {
        let family: Self = serde_json::from_slice(bytes).map_err(|error| error.to_string())?;
        if !matches!(family.schema_version, 1 | 2) {
            return Err(format!(
                "unsupported family schema_version {}",
                family.schema_version
            ));
        }
        if family.schema_version == STREAMED_FAMILY_SCHEMA_VERSION {
            let resolved = family
                .resolved_conditions
                .as_ref()
                .ok_or("schema-2 family has no resolved_conditions")?;
            family
                .resolved_hash
                .as_ref()
                .ok_or("schema-2 family has no resolved_hash")?;
            let mut previous = None;
            for member in &family.members {
                let index = member
                    .global_index
                    .ok_or("schema-2 member has no global_index")?;
                if previous.is_some_and(|old| index <= old) || member.screened.is_some() {
                    return Err(
                        "schema-2 members must be unscreened and strictly ordered by global_index"
                            .into(),
                    );
                }
                previous = Some(index);
            }
            if resolved.len() < family.search.min_conditions as usize
                && (family.applicable != 0
                    || !family.members.is_empty()
                    || family.lowering.is_some()
                    || !family.chunks.is_empty())
            {
                return Err(
                    "schema-2 empty family has counts, members, lowering, or chunks".into(),
                );
            }
        }
        Ok(family)
    }
}

fn schema_one() -> u32 {
    1
}
fn is_schema_one(version: &u32) -> bool {
    *version == 1
}

/// One bound input generation of a family.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct FamilyInput {
    pub role: String,
    pub instrument: String,
    pub tick_generation: String,
    pub feature_generation: String,
    pub plan_identity: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub outcome_generation: Option<String>,
}

/// The ready manifest of a family generation.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct FamilyManifest {
    pub kind: String,
    pub schema_version: u32,
    pub generation: String,
    pub config_hash: String,
    pub code_revision: String,
    pub inputs: Vec<FamilyInput>,
    pub members: u64,
    pub objects: Vec<ObjectRecord>,
}

impl FamilyManifest {
    pub fn to_json(&self) -> Vec<u8> {
        let mut bytes = serde_json::to_vec_pretty(self).expect("a manifest serializes");
        bytes.push(b'\n');
        bytes
    }

    pub fn from_json(bytes: &[u8]) -> Result<Self, String> {
        let manifest: Self = serde_json::from_slice(bytes).map_err(|error| error.to_string())?;
        if manifest.kind != FAMILY_MANIFEST_KIND {
            return Err(format!(
                "kind `{}` is not `{FAMILY_MANIFEST_KIND}`",
                manifest.kind
            ));
        }
        if !matches!(
            manifest.schema_version,
            FAMILY_SCHEMA_VERSION | STREAMED_FAMILY_SCHEMA_VERSION
        ) {
            return Err(format!(
                "unsupported schema_version {}, expected 1 or 2",
                manifest.schema_version
            ));
        }
        if manifest.generation
            != family_generation_id(
                &manifest.config_hash,
                &manifest.code_revision,
                &manifest.inputs,
            )
        {
            return Err(
                "generation is not the identity of its configuration, revision, and inputs"
                    .to_string(),
            );
        }
        crate::dataset::validate_objects(&manifest.objects)?;
        if manifest.objects.len() != 1 || manifest.objects[0].path != FAMILY_OBJECT_PATH {
            return Err(format!(
                "a family generation publishes exactly `{FAMILY_OBJECT_PATH}`"
            ));
        }
        Ok(manifest)
    }

    pub fn key(&self) -> String {
        crate::dataset::manifest_key(&self.generation)
    }
}

/// The family generation identity: the domain, the configuration hash, the code revision, and
/// every bound input, one per line.
pub fn family_generation_id(
    config_hash: &str,
    code_revision: &str,
    inputs: &[FamilyInput],
) -> String {
    let mut hasher = Sha256::new();
    hasher.update(FAMILY_DOMAIN_V1);
    for line in [config_hash, code_revision] {
        hasher.update(line.as_bytes());
        hasher.update(b"\n");
    }
    for input in inputs {
        hasher.update(
            format!(
                "{} {} {} {} {} {}\n",
                input.role,
                input.instrument,
                input.tick_generation,
                input.feature_generation,
                input.plan_identity,
                input.outcome_generation.as_deref().unwrap_or("-")
            )
            .as_bytes(),
        );
    }
    crate::hex(&hasher.finalize())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::execution::{
        Cashflow, Comparator, Direction, Settlement, SettlementRule, Threshold,
    };

    fn legacy_manifest(generation: &str) -> Vec<u8> {
        std::fs::read(
            std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("../app/tests/fixtures/legacy_schema1/published/manifests")
                .join(generation)
                .join("ready.json"),
        )
        .unwrap()
    }

    fn legacy_object(generation: &str, path: &str) -> Vec<u8> {
        let manifest: serde_json::Value =
            serde_json::from_slice(&legacy_manifest(generation)).unwrap();
        let key = manifest["objects"]
            .as_array()
            .unwrap()
            .iter()
            .find(|object| object["path"] == path)
            .unwrap()["key"]
            .as_str()
            .unwrap();
        std::fs::read(
            std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("../app/tests/fixtures/legacy_schema1/published")
                .join(key),
        )
        .unwrap()
    }

    fn decimal(text: &str) -> Decimal {
        Decimal::parse(text).unwrap()
    }

    fn contract(quoted_cost: &str, entry_fee: &str, win: &str, tie: &str) -> ContractTerms {
        ContractTerms {
            id: "c".into(),
            direction: Direction::Buy,
            duration_micros: 5_000_000,
            currency: "unit".to_string().try_into().unwrap(),
            stake: decimal("1"),
            quoted_cost: decimal(quoted_cost),
            entry_fee: decimal(entry_fee),
            win: Cashflow {
                gross_return: decimal(win),
                terminal_fee: decimal("0"),
            },
            loss: Cashflow {
                gross_return: decimal("0"),
                terminal_fee: decimal("0"),
            },
            tie: Cashflow {
                gross_return: decimal(tie),
                terminal_fee: decimal("0"),
            },
            semantics: None,
            settlement: Settlement {
                rule: SettlementRule::PriceAtDueV1,
                max_settlement_delay_micros: 0,
                max_tick_gap_micros: 0,
            },
        }
    }

    #[test]
    fn enumeration_is_lexicographic_and_deduplicated() {
        assert_eq!(
            combinations(3, 1, 2),
            vec![
                vec![0],
                vec![1],
                vec![2],
                vec![0, 1],
                vec![0, 2],
                vec![1, 2]
            ]
        );
        assert_eq!(combinations(3, 4, 5), Vec::<Vec<usize>>::new());
        assert_eq!(family_size(3, 1, 2, 2), Some(12));
        assert_eq!(family_size(3, 1, 3, 1), Some(7));
        assert_eq!(family_size(100, 1, 40, 1), None);
        let stream = StreamKey {
            duration_seconds: 5,
            offset_seconds: 0,
        };
        let menu = vec![
            SearchCondition::Named(crate::config::NamedSearchCondition {
                stream,
                output: "candle_direction".into(),
                comparator: Comparator::Eq,
                thresholds: vec![Threshold::Text("up".into()), Threshold::Text("up".into())],
            }),
            SearchCondition::Named(crate::config::NamedSearchCondition {
                stream,
                output: "range_bps".into(),
                comparator: Comparator::Gt,
                thresholds: vec![Threshold::Number(0.08)],
            }),
        ];
        let conditions = conditions(&menu);
        assert_eq!(conditions.len(), 2);
        let candidates = candidates("plan", stream, &conditions, 1, 2);
        assert_eq!(candidates.len(), 3);
        assert_eq!(candidates[2].conditions, vec![0, 1]);
        assert_eq!(
            candidates[2].logic_identity,
            signal_logic_identity(&strategy("x", "plan", stream, &conditions, &[1, 0]))
        );
    }

    #[test]
    fn streamed_ranks_match_exhaustive_order_and_large_edge() {
        for count in 1..=8 {
            for min in 1..=count + 1 {
                for max in min..=count + 1 {
                    let expected = combinations(count, min, max);
                    let streamed: Vec<_> = stream_members(count, min, max, 3).unwrap().collect();
                    assert_eq!(streamed.len(), expected.len() * 3);
                    for (ordinal, indices) in expected.iter().enumerate() {
                        for contract in 0..3 {
                            let global = (ordinal * 3 + contract) as u64;
                            assert_eq!(
                                streamed[global as usize],
                                (global, indices.clone(), contract)
                            );
                            assert_eq!(
                                member_rank(count, min, max, 3, indices, contract),
                                Some(global)
                            );
                            assert_eq!(
                                member_unrank(count, min, max, 3, global),
                                Some((indices.clone(), contract))
                            );
                        }
                    }
                }
            }
        }
        let max_candidates = 100;
        assert_eq!(family_size(100, 99, 99, 1), Some(max_candidates));
        let expected = combinations(100, 99, 99);
        for (global, indices, contract) in stream_members(100, 99, 99, 1).unwrap() {
            assert_eq!(indices, expected[global as usize]);
            assert_eq!(contract, 0);
            assert_eq!(member_rank(100, 99, 99, 1, &indices, 0), Some(global));
        }
    }

    #[test]
    fn compact_screen_matches_member_scoring_on_random_families() {
        let contracts = [
            contract("1", "0", "1.80", "1"),
            contract("1", "0", "1.80", "0.95"),
        ];
        let screens = [
            None,
            Some(Screen {
                max_adjusted_score: 0.5,
                top: Some(4),
            }),
            Some(Screen {
                max_adjusted_score: 1.0,
                top: Some(2),
            }),
        ];
        let mut state = 0x1234_5678_9abc_def0_u64;
        for size in [0, 1, 2, 7, 32, 101] {
            for screen in &screens {
                let mut members: Vec<Member> = (0..size)
                    .map(|_| {
                        state ^= state << 13;
                        state ^= state >> 7;
                        state ^= state << 17;
                        let wins = (state % 32) as i64;
                        let losses = ((state >> 8) % 32) as i64;
                        Member {
                            global_index: None,
                            logic_identity: String::new(),
                            conditions: Vec::new(),
                            contract: String::new(),
                            raw: RawCounts {
                                total: wins + losses,
                                wins,
                                losses,
                                ties: 0,
                                invalid: 0,
                            },
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
                        }
                    })
                    .collect();
                let mut compact: Vec<_> = members
                    .iter()
                    .map(|m| CompactMember::new(m.raw.clone()))
                    .collect();
                let applicable = score(&mut members, &contracts, screen.as_ref());
                let (actual_applicable, survivors) =
                    screen_compact(&mut compact, &contracts, screen.as_ref());
                assert_eq!(actual_applicable, applicable);
                let expected: Vec<_> = members
                    .iter()
                    .enumerate()
                    .filter_map(|(i, m)| m.screened.is_none().then_some(i as u64))
                    .collect();
                assert_eq!(survivors, expected);
                for &global in &survivors {
                    let index = global as usize;
                    let mut rebuilt = members[index].clone();
                    rebuilt.raw = RawCounts::default();
                    rebuilt.score = None;
                    rebuilt.adjusted = None;
                    rebuilt.null = None;
                    compact[index].apply(
                        &mut rebuilt,
                        &contracts[index % contracts.len()],
                        screen.as_ref(),
                    );
                    assert_eq!(rebuilt, members[index]);
                }
                let gates = Gates {
                    min_settled: 1,
                    max_unresolved: 0,
                    min_net_profit: decimal("0"),
                };
                for (index, member) in members.iter_mut().enumerate() {
                    member.development = Some(Group {
                        settled: 2,
                        profit: BTreeMap::from([(
                            "unit".into(),
                            Some(decimal(if index % 3 == 0 { "2" } else { "1" })),
                        )]),
                        ..Group::default()
                    });
                }
                let mut retained: Vec<Member> = survivors
                    .iter()
                    .map(|&global| members[global as usize].clone())
                    .collect();
                rank(&mut members, &gates, "unit");
                rank(&mut retained, &gates, "unit");
                for (global, member) in survivors.iter().zip(&retained) {
                    assert_eq!(member.rank, members[*global as usize].rank);
                    assert_eq!(member.rejected, members[*global as usize].rejected);
                }
            }
        }
    }

    #[test]
    fn schema_two_empty_family_round_trips_without_changing_schema_one_bytes() {
        let bytes = legacy_object(
            "d7924115f6ec1c2219d5239081221020315db2bf998d950c10cb60d194c53f67",
            "family.json",
        );
        let original = Family::from_json(&bytes).unwrap();
        assert_eq!(original.schema_version, 1);
        assert_eq!(original.to_json(), bytes);
        let mut empty = original;
        empty.schema_version = STREAMED_FAMILY_SCHEMA_VERSION;
        empty.resolved_conditions = Some(Vec::new());
        empty.resolved_hash = Some("v1:sha256:empty".into());
        empty.applicable = 0;
        empty.members.clear();
        empty.lowering = None;
        empty.chunks.clear();
        let serialized = empty.to_json();
        assert_eq!(Family::from_json(&serialized).unwrap(), empty);
        let document: serde_json::Value = serde_json::from_slice(&serialized).unwrap();
        assert!(document.get("lowering").is_none());
        assert_eq!(document["members"].as_array().unwrap().len(), 0);
        let mut malformed = empty.clone();
        malformed.applicable = 1;
        assert!(
            Family::from_json(&malformed.to_json())
                .unwrap_err()
                .contains("empty family")
        );
        let manifest_bytes =
            legacy_manifest("73486072a7f59ab1df2e6f2f2b704454759ec5c23ff977b874b58c0f3fd180d7");
        let mut manifest = FamilyManifest::from_json(&manifest_bytes).unwrap();
        assert_eq!(manifest.to_json(), manifest_bytes);
        manifest.schema_version = STREAMED_FAMILY_SCHEMA_VERSION;
        manifest.members = 0;
        assert_eq!(
            FamilyManifest::from_json(&manifest.to_json()).unwrap(),
            manifest
        );
    }

    #[test]
    fn generated_rules_resolve_fitted_labels_only_after_binding() {
        use crate::config::{GeneratedSearchCondition, NamedSearchCondition};
        use crate::features::{FittedEncoding, OutputSpec, ProjectionKind, Value};
        let family: Family = serde_json::from_slice(&legacy_object(
            "d7924115f6ec1c2219d5239081221020315db2bf998d950c10cb60d194c53f67",
            "family.json",
        ))
        .unwrap();
        let mut search = family.search;
        let mut plan = FeaturePlan::from_json(&legacy_object(
            "a7ccab4e17b84ad665de5b29c9ccbca10df2b926d7dfe7a3c4987267092f8f29",
            "plan.json",
        ))
        .unwrap();
        let stream = plan.streams[0].key();
        let mut boolean: OutputSpec = plan.streams[0]
            .outputs
            .iter()
            .find(|output| output.name == "candle_direction")
            .unwrap()
            .clone();
        boolean.name = "is_bullish".into();
        boolean.kind = Kind::Bool;
        plan.streams[0].outputs.push(boolean);
        let mut sequence = plan.streams[0]
            .outputs
            .iter()
            .find(|output| output.name == "candle_direction")
            .unwrap()
            .clone();
        sequence.name = "market_structure_sequence".into();
        plan.streams[0].outputs.push(sequence);
        let mut numeric = FittedEncoding {
            output: "range_bps__dev_fifths".into(),
            input: "range_bps".into(),
            automatic: true,
            encoding: ProjectionKind::DevelopmentFifths,
            edges: None,
            input_divisor: 1.0,
            labels: vec![],
        };
        numeric
            .fit(
                &(0..100)
                    .map(|x| Some(Value::Float(x as f64)))
                    .collect::<Vec<_>>(),
                5,
            )
            .unwrap();
        assert_eq!(numeric.labels.len(), 5);
        plan.streams[0].encodings = vec![
            numeric.clone(),
            FittedEncoding {
                output: "is_bullish__category".into(),
                input: "is_bullish".into(),
                automatic: true,
                encoding: ProjectionKind::Category,
                edges: None,
                input_divisor: 1.0,
                labels: vec!["true".into(), "false".into()],
            },
            FittedEncoding {
                output: "constant".into(),
                input: "candle_direction".into(),
                automatic: true,
                encoding: ProjectionKind::Category,
                edges: None,
                input_divisor: 1.0,
                labels: vec!["up".into()],
            },
            FittedEncoding {
                output: "collided".into(),
                input: "range_bps".into(),
                automatic: true,
                encoding: ProjectionKind::DevelopmentFifths,
                edges: None,
                input_divisor: 1.0,
                labels: vec![],
            },
            FittedEncoding {
                output: "range_bps".into(),
                input: "range_bps".into(),
                automatic: false,
                encoding: ProjectionKind::Category,
                edges: None,
                input_divisor: 1.0,
                labels: vec!["a".into(), "b".into()],
            },
            FittedEncoding {
                output: "uncoded".into(),
                input: "candle_direction".into(),
                automatic: true,
                encoding: ProjectionKind::Category,
                edges: None,
                input_divisor: 1.0,
                labels: vec!["missing".into(), "none".into()],
            },
            FittedEncoding {
                output: "unready_dominant".into(),
                input: "market_structure_sequence".into(),
                automatic: true,
                encoding: ProjectionKind::Category,
                edges: None,
                input_divisor: 1.0,
                labels: vec!["unknown".into(), "warming_up".into()],
            },
        ];
        search.conditions = vec![
            SearchCondition::Named(NamedSearchCondition {
                stream,
                output: numeric.output.clone(),
                comparator: Comparator::Eq,
                thresholds: vec![Threshold::Text(numeric.labels[0].clone())],
            }),
            SearchCondition::Generate(GeneratedSearchCondition {
                stream,
                output: "*".into(),
                comparator: Comparator::Eq,
            }),
        ];
        search.min_conditions = 1;
        search.max_conditions = 2;
        search.max_candidates = 1000;
        validate(&search).unwrap(); // Syntax requires no plan or resolved family count.
        let resolved = resolve_conditions(&search, &plan).unwrap();
        assert_eq!(resolved.conditions.len(), 7);
        assert_eq!(
            resolved.conditions[0].threshold,
            Threshold::Text(numeric.labels[0].clone())
        );
        assert_eq!(
            resolved.conditions[1].threshold,
            Threshold::Text(numeric.labels[1].clone())
        );
        assert_eq!(
            resolved.conditions[5].threshold,
            Threshold::Text("true".into())
        );
        assert_eq!(
            resolved.conditions[6].threshold,
            Threshold::Text("false".into())
        );
        assert_eq!(
            resolved.members,
            family_size(7, 1, 2, search.contracts.len()).unwrap()
        );
        assert_eq!(
            resolve_conditions(&search, &plan).unwrap().hash,
            resolved.hash
        );
        plan.streams[0].encodings[0].input = "absent".into();
        assert!(
            resolve_conditions(&search, &plan)
                .unwrap_err()
                .contains("has no compiled input")
        );
        plan.streams[0].encodings[0].input = "range_bps".into();
        search.max_candidates = 1;
        assert!(validate(&search).is_ok());
        assert!(
            resolve_conditions(&search, &plan)
                .unwrap_err()
                .contains("max_candidates")
        );
        search.max_candidates = 1000;
        search.conditions.remove(0);
        plan.streams[0].encodings.truncate(1);
        plan.streams[0].encodings[0].labels.truncate(1);
        search.min_conditions = 2;
        let short = resolve_conditions(&search, &plan).unwrap();
        assert!(short.conditions.is_empty());
        assert_eq!(short.members, 0);
        search.conditions.insert(
            0,
            SearchCondition::Named(NamedSearchCondition {
                stream,
                output: "candle_direction".into(),
                comparator: Comparator::Eq,
                thresholds: vec![Threshold::Text("up".into())],
            }),
        );
        search.min_conditions = 1;
        search.max_conditions = 3;
        let one = resolve_conditions(&search, &plan).unwrap();
        assert_eq!(one.conditions.len(), 1);
        assert_eq!(one.members, search.contracts.len() as u64);
    }

    #[test]
    fn the_null_follows_the_exact_terms() {
        let null = null_rate(&contract("1", "0.10", "1.80", "1.10")).unwrap();
        assert_eq!(null.net_win.to_string(), "0.70");
        assert_eq!(null.net_loss.to_string(), "1.10");
        assert!((null.break_even - 11.0 / 18.0).abs() < 1e-15);
        assert_eq!(
            null_rate(&contract("1", "0", "1.80", "1"))
                .unwrap()
                .break_even,
            1.0 / 1.8
        );
        let zero_win = null_rate(&contract("1", "0", "1", "1")).unwrap();
        assert_eq!(zero_win.break_even, 1.0);
        assert_eq!(upper_tail(5, 5, zero_win.break_even), 1.0);
        assert_eq!(
            null_rate(&contract("1", "0", "0.90", "1")).unwrap_err(),
            "net win -0.10 is negative"
        );
        assert_eq!(
            null_rate(&contract("1", "0", "1.80", "0.95")).unwrap_err(),
            "a tie nets -0.05, not zero"
        );
    }

    #[test]
    fn upper_tails_match_exact_arithmetic() {
        assert!((upper_tail(6, 4, 0.5) - 0.376953125).abs() < 1e-12);
        assert!((upper_tail(60, 40, 0.5) - 0.028443966820490395).abs() < 1e-12);
        assert!((upper_tail(3, 7, 0.5) - 0.9453125).abs() < 1e-12);
        assert_eq!(upper_tail(0, 10, 0.5), 1.0);
        assert_eq!(upper_tail(0, 0, 0.5), 1.0);
        assert_eq!(upper_tail(10, 0, 0.0), 0.0);
        assert!((upper_tail(10, 0, 0.5) - 0.0009765625).abs() < 1e-15);
        // A large family member: the truncated sum agrees with the complementary sum.
        let far = upper_tail(60_000, 40_000, 0.5);
        assert!(far < 1e-300 || far == 0.0, "{far}");
        let near = upper_tail(50_100, 49_900, 0.5);
        assert!((0.26..0.27).contains(&near), "{near}");
    }

    #[test]
    fn benjamini_hochberg_matches_hand_calculation() {
        assert_eq!(benjamini_hochberg(&[]), Vec::<f64>::new());
        assert_eq!(benjamini_hochberg(&[0.2]), vec![0.2]);
        let adjusted = benjamini_hochberg(&[0.01, 0.04, 0.03]);
        assert!((adjusted[0] - 0.03).abs() < 1e-15);
        assert!((adjusted[1] - 0.04).abs() < 1e-15);
        assert!((adjusted[2] - 0.04).abs() < 1e-15);
        assert_eq!(benjamini_hochberg(&[0.5, 0.5, 1.0]), vec![0.75, 0.75, 1.0]);
    }

    #[test]
    fn block_paths_are_deterministic_circular_and_bounded() {
        let path = block_path(7, "member/role", 0, 10, 3);
        assert_eq!(path.len(), 10);
        assert!(path.iter().all(|&i| i < 10));
        assert_eq!(path, block_path(7, "member/role", 0, 10, 3));
        assert_ne!(path, block_path(7, "member/role", 1, 10, 3));
        assert_ne!(path, block_path(8, "member/role", 0, 10, 3));
        for pair in path.windows(2) {
            assert!(pair[1] == (pair[0] + 1) % 10 || pair[1] < 10);
        }
        // A block length of one restarts every step; a huge block length never restarts.
        let never = block_path(1, "s", 0, 6, u32::MAX);
        assert!(never.windows(2).all(|pair| pair[1] == (pair[0] + 1) % 6));
        assert!(block_path(1, "s", 0, 0, 1).is_empty());
    }

    #[test]
    fn sampler_v1_index_vectors_are_frozen() {
        // Seed 0, stratum `m/c/development`, twelve trades: block length one restarts every
        // step; block length three restarts exactly when the uniform draw on [0, 3) is zero.
        assert_eq!(
            block_path(0, "m/c/development", 0, 12, 1),
            [0, 3, 8, 10, 7, 8, 7, 2, 8, 3, 4, 0]
        );
        assert_eq!(
            block_path(0, "m/c/development", 0, 12, 3),
            [0, 3, 8, 9, 10, 11, 0, 8, 9, 10, 2, 3]
        );
        assert_eq!(
            block_path(0, "m/c/development", 1, 12, 3),
            [1, 2, 0, 1, 2, 3, 4, 5, 6, 7, 10, 11]
        );
        // A draw on [0, 1) still consumes a word, so unit ranges advance the stream.
        let mut draws = Draws::new(0, "s", 0);
        assert_eq!(draws.uniform(1), 0);
        let after_unit = draws.next();
        let mut fresh = Draws::new(0, "s", 0);
        assert_ne!(fresh.next(), after_unit);
        assert_eq!(fresh.next(), after_unit);
    }

    #[test]
    fn stability_summarizes_quantiles_and_windows() {
        assert_eq!(quantile(&[1.0, 2.0, 3.0, 4.0], 0.5), 2.5);
        assert_eq!(quantile(&[1.0, 2.0, 3.0, 4.0], 0.0), 1.0);
        assert_eq!(quantile(&[5.0], 0.95), 5.0);
        let result = stability(4, 2, 2, &[3.0, 1.0, 2.0, 4.0], &[1, 3, 2, 2], &[1, 0, 2, 0]);
        assert_eq!(result.median_max_drawdown, 2.5);
        assert!((result.p95_max_drawdown - 3.85).abs() < 1e-12);
        assert!((result.p95_longest_underwater - 2.85).abs() < 1e-12);
        assert_eq!(result.negative_rolling_share, 3.0 / 12.0);
        assert_eq!(result.sampler, SAMPLER_VERSION);
    }

    #[test]
    fn gates_and_ranking_are_exact() {
        let gates = Gates {
            min_settled: 2,
            max_unresolved: 0,
            min_net_profit: decimal("0"),
        };
        let mut group = Group {
            settled: 2,
            ..Group::default()
        };
        assert_eq!(
            gate(&group, "unit", &gates).unwrap_err(),
            "net profit is unavailable"
        );
        group.profit.insert("unit".into(), Some(decimal("-0.01")));
        assert_eq!(
            gate(&group, "unit", &gates).unwrap_err(),
            "net profit -0.01 below the minimum 0"
        );
        group.profit.insert("unit".into(), Some(decimal("0.00")));
        assert_eq!(gate(&group, "unit", &gates), Ok(()));
        group.unresolved = 1;
        assert_eq!(
            gate(&group, "unit", &gates).unwrap_err(),
            "unresolved 1 above the maximum 0"
        );
        let richer = Group {
            settled: 2,
            profit: BTreeMap::from([("unit".to_string(), Some(decimal("5")))]),
            ..Group::default()
        };
        let member = |development: Option<Group>, screened: Option<&str>| Member {
            global_index: None,
            logic_identity: String::new(),
            conditions: Vec::new(),
            contract: "c".into(),
            raw: RawCounts::default(),
            null: None,
            inapplicable: None,
            score: None,
            adjusted: None,
            screened: screened.map(str::to_string),
            development,
            rejected: None,
            rank: None,
            evaluation: None,
            evaluation_splits: BTreeMap::new(),
            stability: BTreeMap::new(),
        };
        group.unresolved = 0;
        let mut members = vec![
            member(Some(group.clone()), None),
            member(Some(richer.clone()), None),
            member(Some(richer.clone()), None),
            member(None, Some("screened")),
            member(Some(Group::default()), None),
        ];
        assert_eq!(rank(&mut members, &gates, "unit"), vec![1, 2, 0]);
        assert_eq!(
            members.iter().map(|m| m.rank).collect::<Vec<_>>(),
            [Some(3), Some(1), Some(2), None, None],
            "ties keep member order"
        );
        assert_eq!(
            members[4].rejected.as_deref(),
            Some("settled 0 below the minimum 2")
        );
        assert_eq!(members[3].rejected, None);
    }

    #[test]
    fn scores_adjust_over_the_applicable_subset_and_screen_in_order() {
        let mut members: Vec<Member> = [(16, 16), (12, 4), (0, 0), (16, 16), (12, 4), (0, 0)]
            .into_iter()
            .map(|(wins, losses)| Member {
                global_index: None,
                logic_identity: String::new(),
                conditions: Vec::new(),
                contract: String::new(),
                raw: RawCounts {
                    total: wins + losses,
                    wins,
                    losses,
                    ties: 0,
                    invalid: 0,
                },
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
            .collect();
        let contracts = [
            contract("1", "0", "1.80", "1"),
            contract("1", "0", "1.80", "0.95"),
        ];
        let screen = Screen {
            max_adjusted_score: 0.5,
            top: Some(1),
        };
        // Contracts cycle fastest: members 0, 2, 4 use the neutral tie, 1, 3, 5 the odd tie.
        assert_eq!(score(&mut members, &contracts, Some(&screen)), 3);
        assert!(members[4].screened.is_none(), "{:?}", members[4]);
        assert_eq!(
            members[4].adjusted,
            Some(upper_tail(12, 4, 1.0 / 1.8) * 3.0 / 1.0)
        );
        assert!(
            members[1]
                .screened
                .as_deref()
                .unwrap()
                .starts_with("inapplicable: ")
        );
        assert_eq!(members[2].adjusted, Some(1.0));
        assert!(
            members[2]
                .screened
                .as_deref()
                .unwrap()
                .contains("above the maximum")
        );
        assert!(
            members[0]
                .screened
                .as_deref()
                .unwrap()
                .contains("above the maximum")
        );
        assert_eq!(score(&mut members, &contracts, None), 3);
        assert!(members.iter().all(|m| m.screened.is_none()));
    }

    #[test]
    fn family_identity_binds_inputs_in_order() {
        let input = FamilyInput {
            role: "development".into(),
            instrument: "pocket_option:AEDCNY_otc".into(),
            tick_generation: "t".into(),
            feature_generation: "f".into(),
            plan_identity: "p".into(),
            outcome_generation: Some("o".into()),
        };
        let one = family_generation_id("c", "r", std::slice::from_ref(&input));
        let mut other = input.clone();
        other.outcome_generation = None;
        assert_ne!(one, family_generation_id("c", "r", &[other]));
        assert_ne!(one, family_generation_id("d", "r", &[input]));
        assert_eq!(one.len(), 64);
    }
}

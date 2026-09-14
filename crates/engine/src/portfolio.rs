//! Portfolio selection: the finite enumeration of complete joint policies over development-only
//! search families, the logical and fold-resolved form of every choice, the settlement, profit,
//! and drawdown projection of a verified restored engine, the frozen objective, gates, tie
//! breaks, and ranking, and the published selection records. Pure logic only: no storage,
//! feature build, or engine execution.
//!
//! Selection compares separately funded folds; it claims no continuous equity path, no
//! certification, and no evidence of trading profitability.

use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::config::{Config, FeatureInstrument, Portfolio, Replay, StreamKey};
use crate::dataset::{DatasetRole, ObjectRecord};
use crate::execution::{
    AccountSpec, Comparator, Condition, ContractTerms, Decimal, DeploymentBinding, Engine, Group,
    ReplayInput, RiskPolicy, SettlementRule, Split, StrategySpec, Threshold,
};
use crate::features::{FeaturePlan, FittedEncoding, ProjectionKind};
use crate::market::{format_event_time_micros, parse_event_time_micros};
use crate::search::Family;

/// The manifest kind of a published selection.
pub const SELECTION_MANIFEST_KIND: &str = "portfolio_selection";
pub const SELECTION_SCHEMA_VERSION: u32 = 1;
/// The one object of a selection generation.
pub const SELECTION_OBJECT_PATH: &str = "selection.json";
/// The plan identity prefix of a strategy's logical form, before a fold resolves it: the
/// prefix and the deployment's instrument, since every instrument carries its own fitted plan.
pub const LOGICAL_PLAN_IDENTITY: &str = "logical";
const SELECTION_DOMAIN_V1: &[u8] = b"binary-alpha portfolio selection v1\n";
const CHOICE_DOMAIN_V1: &[u8] = b"binary-alpha portfolio choice v1\n";

crate::string_enum! {
    /// The frozen order of the two financial values: the first decides, the second breaks ties.
    Objective "objective" {
        ProfitThenDrawdown => "profit_then_drawdown",
        DrawdownThenProfit => "drawdown_then_profit",
    }
}

/// The exact gates every required fold of a passing choice satisfies, in the reporting currency.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Gates {
    pub min_settled: u64,
    pub max_unresolved: u64,
    pub min_profit: Decimal,
    pub max_drawdown: Decimal,
}

// ----------------------------------------------------------------------------------------------
// Configuration validation and the declared count
// ----------------------------------------------------------------------------------------------

fn time(field: &str, text: &str) -> Result<i64, String> {
    parse_event_time_micros(text).map_err(|reason| format!("{field}: {reason}"))
}

fn unique<'a>(field: &str, ids: impl Iterator<Item = &'a str>) -> Result<(), String> {
    let mut seen = BTreeSet::new();
    for (index, id) in ids.enumerate() {
        if id.is_empty() || id.bytes().any(|byte| byte.is_ascii_control()) {
            return Err(format!(
                "{field}[{index}].id: must be a non-empty identifier without a control character"
            ));
        }
        if !seen.insert(id) {
            return Err(format!("{field}[{index}].id: `{id}` is listed twice"));
        }
    }
    Ok(())
}

/// One development fit entry: a new plan on development data, never a frozen one.
fn fit(field: &str, entry: &FeatureInstrument) -> Result<(), String> {
    entry
        .validate()
        .map_err(|reason| format!("{field}.{reason}"))?;
    if entry.frozen_plan.is_some() {
        return Err(format!(
            "{field}.frozen_plan: a fit resolves a new plan; the assessment applies it"
        ));
    }
    if entry.role != DatasetRole::Development {
        return Err(format!("{field}.role: a fit is development-only"));
    }
    Ok(())
}

/// A window that starts at least the embargo after its cutoff, in event-time microseconds.
fn embargoed(
    field: &str,
    cutoff: i64,
    start: &str,
    end: &str,
    embargo_micros: i64,
) -> Result<(), String> {
    let start_micros = time(&format!("{field}.decision_start"), start)?;
    let end_micros = time(&format!("{field}.decision_end"), end)?;
    if start_micros >= end_micros {
        return Err(format!(
            "{field}.decision_end: {end} must be after decision_start {start}"
        ));
    }
    if start_micros
        .checked_sub(cutoff)
        .is_none_or(|gap| gap < embargo_micros)
    {
        return Err(format!(
            "{field}.decision_start: {start} begins less than the embargo after the cutoff"
        ));
    }
    Ok(())
}

/// The rules of the `portfolio` table a single field's deserializer cannot see; an error names
/// the field. Accounts, every contract alternative, every risk policy, the rates, the reporting
/// contract, and the first fold's window are validated by the execution rules through the
/// structure of one policy per risk policy binding every alternative once; a choice's own
/// duplicate-deployment and shared-policy rules apply when it is enumerated.
pub fn validate(portfolio: &Portfolio) -> Result<(), String> {
    if portfolio.families.is_empty() {
        return Err("families: at least one family is required".to_string());
    }
    for (index, uri) in portfolio.families.iter().enumerate() {
        if portfolio.families[..index].contains(uri) {
            return Err(format!("families[{index}]: {uri} is listed twice"));
        }
    }
    if portfolio.max_policies == 0 {
        return Err("max_policies: must be positive".to_string());
    }
    if portfolio.embargo_micros <= 0 {
        return Err("embargo_micros: must be positive".to_string());
    }
    for (index, binding) in portfolio.bindings.iter().enumerate() {
        for (position, alternative) in binding.alternatives.iter().enumerate() {
            let field = format!("bindings[{index}].alternatives[{position}].contract");
            if alternative.contract.settlement.rule == SettlementRule::BrokerAuthoritativeV1
                || alternative.envelope.settlement_rule == SettlementRule::BrokerAuthoritativeV1
            {
                return Err(format!(
                    "{field}: broker_authoritative_v1 settlement needs a broker; research and historical replay use price_at_due_v1"
                ));
            }
            let required = alternative
                .contract
                .settlement_horizon()
                .map_err(|reason| format!("{field}.{reason}"))?;
            if portfolio.embargo_micros < required {
                return Err(format!(
                    "embargo_micros: {} is shorter than {field}'s duration plus settlement delay {required}",
                    portfolio.embargo_micros
                ));
            }
        }
    }
    if portfolio.gates.min_settled == 0 {
        return Err("gates.min_settled: must be positive".to_string());
    }
    if portfolio.gates.max_drawdown.is_negative() {
        return Err("gates.max_drawdown: must not be negative".to_string());
    }
    if portfolio.members.is_empty() {
        return Err("members: at least one base member is required".to_string());
    }
    for (index, member) in portfolio.members.iter().enumerate() {
        if member.family >= portfolio.families.len() {
            return Err(format!(
                "members[{index}].family: {} is not a listed family",
                member.family
            ));
        }
        for (position, ordinal) in member.ordinals.iter().enumerate() {
            let field = |name: &str| format!("members[{index}].ordinals[{position}].{name}");
            if ordinal.ordinal > 4 {
                return Err(format!("{}: must lie in 0..=4", field("ordinal")));
            }
            if member.ordinals[..position]
                .iter()
                .any(|earlier| earlier.condition == ordinal.condition)
            {
                return Err(format!(
                    "{}: condition {} is listed twice",
                    field("condition"),
                    ordinal.condition
                ));
            }
        }
    }
    if portfolio.repairs.is_empty() {
        return Err(
            "repairs: at least one alternative is required; an empty conjunction is no repair"
                .to_string(),
        );
    }
    unique("repairs", portfolio.repairs.iter().map(|r| r.id.as_str()))?;
    if portfolio.bindings.is_empty() {
        return Err("bindings: at least one binding is required".to_string());
    }
    unique("bindings", portfolio.bindings.iter().map(|b| b.id.as_str()))?;
    let mut contracts: Vec<ContractTerms> = Vec::new();
    for (index, binding) in portfolio.bindings.iter().enumerate() {
        if binding.alternatives.is_empty() {
            return Err(format!(
                "bindings[{index}].alternatives: at least one contract and envelope pair is required"
            ));
        }
        for (position, alternative) in binding.alternatives.iter().enumerate() {
            let field = format!("bindings[{index}].alternatives[{position}]");
            match contracts
                .iter()
                .find(|contract| contract.id == alternative.contract.id)
            {
                Some(existing) if *existing != alternative.contract => {
                    return Err(format!(
                        "{field}.contract.id: `{}` names different terms elsewhere; one identity is one contract",
                        alternative.contract.id
                    ));
                }
                Some(_) => {}
                None => contracts.push(alternative.contract.clone()),
            }
            match alternative.envelope.admits(&alternative.contract) {
                Ok(true) => {}
                Ok(false) => {
                    return Err(format!(
                        "{field}.envelope: does not admit its own contract `{}`",
                        alternative.contract.id
                    ));
                }
                Err(reason) => return Err(format!("{field}.contract: {reason}")),
            }
        }
    }
    if portfolio.subsets.is_empty() {
        return Err("subsets: at least one ordered subset is required".to_string());
    }
    for (index, subset) in portfolio.subsets.iter().enumerate() {
        if subset.deployments.is_empty() {
            return Err(format!(
                "subsets[{index}].deployments: at least one deployment is required"
            ));
        }
        for (position, deployment) in subset.deployments.iter().enumerate() {
            let field = |name: &str| format!("subsets[{index}].deployments[{position}].{name}");
            if deployment.member >= portfolio.members.len() {
                return Err(format!(
                    "{}: {} is not a base member",
                    field("member"),
                    deployment.member
                ));
            }
            if deployment.repair >= portfolio.repairs.len() {
                return Err(format!(
                    "{}: {} is not a repair alternative",
                    field("repair"),
                    deployment.repair
                ));
            }
            if deployment.binding >= portfolio.bindings.len() {
                return Err(format!(
                    "{}: {} is not a binding",
                    field("binding"),
                    deployment.binding
                ));
            }
        }
    }
    if portfolio.risk_policies.is_empty() {
        return Err("risk_policies: at least one risk policy is required".to_string());
    }
    unique(
        "risk_policies",
        portfolio.risk_policies.iter().map(|p| p.id.as_str()),
    )?;
    let declared = declared_count(portfolio)?;
    if declared > portfolio.max_policies {
        return Err(format!(
            "max_policies: the grid declares {declared} policies, above the maximum {}",
            portfolio.max_policies
        ));
    }
    if portfolio.folds.is_empty() {
        return Err("folds: at least one inner fit and assessment pair is required".to_string());
    }
    for (index, fold) in portfolio.folds.iter().enumerate() {
        let field = format!("folds[{index}]");
        let cutoff = time(&format!("{field}.cutoff"), &fold.cutoff)?;
        embargoed(
            &field,
            cutoff,
            &fold.decision_start,
            &fold.decision_end,
            portfolio.embargo_micros,
        )?;
        if fold.inputs.is_empty() {
            return Err(format!(
                "{field}.inputs: at least one instrument input is required"
            ));
        }
        for (position, input) in fold.inputs.iter().enumerate() {
            fit(&format!("{field}.inputs[{position}].fit"), &input.fit)?;
        }
    }
    let refit_cutoff = time("refit.cutoff", &portfolio.refit.cutoff)?;
    if portfolio.refit.fits.is_empty() {
        return Err("refit.fits: at least one instrument fit is required".to_string());
    }
    for (position, entry) in portfolio.refit.fits.iter().enumerate() {
        fit(&format!("refit.fits[{position}]"), entry)?;
    }
    if let Some(evaluation) = &portfolio.evaluation {
        embargoed(
            "evaluation",
            refit_cutoff,
            &evaluation.decision_start,
            &evaluation.decision_end,
            portfolio.embargo_micros,
        )?;
        if evaluation.inputs.is_empty() {
            return Err("evaluation.inputs: at least one instrument input is required".to_string());
        }
    }
    // Every account, contract, policy, rate, and the reporting contract under the execution
    // rules: one policy per risk policy binds every alternative once, each through its own
    // placeholder strategy.
    for (index, risk_policy) in portfolio.risk_policies.iter().enumerate() {
        let mut policy = Policy {
            strategies: Vec::new(),
            bindings: Vec::new(),
            contracts: contracts.clone(),
            risk_policies: vec![risk_policy.clone()],
        };
        for binding in &portfolio.bindings {
            for alternative in &binding.alternatives {
                let id = format!("validation-{}", policy.strategies.len());
                let stream = StreamKey {
                    duration_seconds: 1,
                    offset_seconds: 0,
                };
                policy.strategies.push(StrategySpec {
                    id: id.clone(),
                    plan_identity: "validation".to_string(),
                    base_stream: stream,
                    conditions: vec![Condition {
                        stream,
                        output: "validation".to_string(),
                        comparator: Comparator::Eq,
                        threshold: Threshold::Number(policy.strategies.len() as f64),
                    }],
                    repair: Vec::new(),
                });
                policy.bindings.push(DeploymentBinding {
                    id: id.clone(),
                    strategy: id,
                    account: binding.account.clone(),
                    instrument: binding.instrument.clone(),
                    contract: alternative.contract.id.clone(),
                    risk_policy: risk_policy.id.clone(),
                    envelope: alternative.envelope.clone(),
                });
            }
        }
        structure(portfolio, &policy)
            .map_err(|reason| format!("under risk_policies[{index}]: {reason}"))?;
    }
    Ok(())
}

/// The number of complete choices the table declares, with checked arithmetic: for every
/// subset the product of its deployments' alternative counts, summed, times the risk policies.
pub fn declared_count(portfolio: &Portfolio) -> Result<u64, String> {
    let mut total: u64 = 0;
    for (index, subset) in portfolio.subsets.iter().enumerate() {
        let overflow = || format!("subsets[{index}]: the declared policy count overflows");
        let mut product: u64 = 1;
        for deployment in &subset.deployments {
            let alternatives = portfolio.bindings[deployment.binding].alternatives.len() as u64;
            product = product.checked_mul(alternatives).ok_or_else(overflow)?;
        }
        total = product
            .checked_mul(portfolio.risk_policies.len() as u64)
            .and_then(|policies| total.checked_add(policies))
            .ok_or_else(overflow)?;
    }
    Ok(total)
}

// ----------------------------------------------------------------------------------------------
// The logical universe and its enumeration
// ----------------------------------------------------------------------------------------------

/// One member condition in logical form: literal, or resolved per fold from the declared
/// interval ordinal of a development-fifths encoding.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
pub struct LogicalCondition {
    pub condition: Condition,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ordinal: Option<u8>,
}

/// One base of the frozen logical universe: a family member's conditions with their ordinals.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
pub struct LogicalMember {
    pub family: usize,
    pub member: usize,
    pub base_stream: StreamKey,
    pub conditions: Vec<LogicalCondition>,
}

/// The logical universe of the configured base members over the verified families, in
/// declared order. Every declared member and ordinal must exist; an ordinal condition compares
/// text with `eq` or `ne`.
pub fn logical_members(
    portfolio: &Portfolio,
    families: &[Family],
) -> Result<Vec<LogicalMember>, String> {
    portfolio
        .members
        .iter()
        .enumerate()
        .map(|(index, base)| {
            let family = &families[base.family];
            let member = family.members.get(base.member).ok_or_else(|| {
                format!(
                    "members[{index}].member: {} is not a member of family {}, which holds {}",
                    base.member,
                    base.family,
                    family.members.len()
                )
            })?;
            let mut conditions: Vec<LogicalCondition> = member
                .conditions
                .iter()
                .map(|condition| LogicalCondition {
                    condition: condition.clone(),
                    ordinal: None,
                })
                .collect();
            for (position, ordinal) in base.ordinals.iter().enumerate() {
                let field = format!("members[{index}].ordinals[{position}]");
                let logical = conditions.get_mut(ordinal.condition).ok_or_else(|| {
                    format!(
                        "{field}.condition: {} is not a condition of member {}, which holds {}",
                        ordinal.condition,
                        base.member,
                        member.conditions.len()
                    )
                })?;
                if !matches!(
                    logical.condition.comparator,
                    Comparator::Eq | Comparator::Ne
                ) {
                    return Err(format!(
                        "{field}.condition: interval ordinals resolve text equality or inequality, not `{}`",
                        logical.condition.comparator
                    ));
                }
                logical.ordinal = Some(ordinal.ordinal);
            }
            Ok(LogicalMember {
                family: base.family,
                member: base.member,
                base_stream: family.base_stream,
                conditions,
            })
        })
        .collect()
}

/// One declared complete choice: the subset, one alternative per deployment, and the policy.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct ChoiceKey {
    pub subset: usize,
    pub alternatives: Vec<usize>,
    pub risk_policy: usize,
}

/// Advances a mixed-radix counter with the last digit cycling fastest; `false` after the last.
fn advance(digits: &mut [usize], radices: &[usize]) -> bool {
    for position in (0..digits.len()).rev() {
        digits[position] += 1;
        if digits[position] < radices[position] {
            return true;
        }
        digits[position] = 0;
    }
    false
}

/// Every declared choice in declared order: subsets, then the alternatives of each deployment
/// with the last deployment cycling fastest, then risk policies. The caller has bounded the
/// count through [`declared_count`].
pub fn enumerate(portfolio: &Portfolio) -> Vec<ChoiceKey> {
    let mut keys = Vec::new();
    for (subset, deployments) in portfolio.subsets.iter().enumerate() {
        let radices: Vec<usize> = deployments
            .deployments
            .iter()
            .map(|deployment| portfolio.bindings[deployment.binding].alternatives.len())
            .collect();
        let mut alternatives = vec![0; radices.len()];
        loop {
            for risk_policy in 0..portfolio.risk_policies.len() {
                keys.push(ChoiceKey {
                    subset,
                    alternatives: alternatives.clone(),
                    risk_policy,
                });
            }
            if !advance(&mut alternatives, &radices) {
                break;
            }
        }
    }
    keys
}

// ----------------------------------------------------------------------------------------------
// The complete policy of one choice, logical or resolved
// ----------------------------------------------------------------------------------------------

/// The complete deployments of one choice with their exact terms, in binding priority order:
/// what a replay table carries beyond its window, inputs, accounts, and reporting contract.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
pub struct Policy {
    pub strategies: Vec<StrategySpec>,
    pub bindings: Vec<DeploymentBinding>,
    pub contracts: Vec<ContractTerms>,
    pub risk_policies: Vec<RiskPolicy>,
}

impl Policy {
    /// The canonical complete-choice identity: SHA-256 over the choice domain and the JSON of
    /// the logical form.
    pub fn identity(&self) -> String {
        let mut hasher = Sha256::new();
        hasher.update(CHOICE_DOMAIN_V1);
        hasher.update(serde_json::to_vec(self).expect("a policy serializes"));
        crate::hex(&hasher.finalize())
    }
}

/// Which form a choice's strategies take.
#[derive(Clone, Copy)]
pub enum Form<'a> {
    /// The frozen logical form: the plan identity `logical:INSTRUMENT` and every ordinal
    /// rendered as the text `interval ORDINAL`, so identity and structure depend on nothing a
    /// fold fits.
    Logical,
    /// Resolved under one fitted plan per instrument: the plan identity of the deployment's
    /// instrument and every ordinal replaced by that plan's fitted interval label.
    Resolved(&'a BTreeMap<String, FeaturePlan>),
}

/// Why a choice has no resolved policy: inapplicable in this fold, or an input error.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Failure {
    Inapplicable(String),
    Error(String),
}

/// The fitted encoding a condition reads, when its output is one.
fn encoding_of<'a>(plan: &'a FeaturePlan, condition: &Condition) -> Option<&'a FittedEncoding> {
    plan.stream(condition.stream)?
        .encodings
        .iter()
        .find(|encoding| encoding.output == condition.output)
}

fn resolve_condition(
    field: &str,
    logical: &LogicalCondition,
    plan: &FeaturePlan,
) -> Result<Condition, Failure> {
    let encoding = encoding_of(plan, &logical.condition);
    let fifths =
        encoding.is_some_and(|encoding| encoding.encoding == ProjectionKind::DevelopmentFifths);
    match (logical.ordinal, fifths) {
        (None, false) => Ok(logical.condition.clone()),
        (None, true) => Err(Failure::Error(format!(
            "{field}: `{}` is a development-fifths encoding and declares no interval ordinal",
            logical.condition.output
        ))),
        (Some(_), false) => Err(Failure::Error(format!(
            "{field}: `{}` is not a development-fifths encoding of stream {}",
            logical.condition.output, logical.condition.stream
        ))),
        (Some(ordinal), true) => {
            let label = encoding
                .expect("fifths")
                .interval_label(ordinal)
                .map_err(|reason| Failure::Inapplicable(format!("{field}: {reason}")))?;
            Ok(Condition {
                threshold: Threshold::Text(label),
                ..logical.condition.clone()
            })
        }
    }
}

/// The complete policy of one choice in `form`. Deployments keep their subset position as
/// `d{position}`; every deployment's contract is listed once by identity; the risk policy is
/// the one chosen for the whole choice.
pub fn policy(
    portfolio: &Portfolio,
    members: &[LogicalMember],
    key: &ChoiceKey,
    form: Form<'_>,
) -> Result<Policy, Failure> {
    let subset = &portfolio.subsets[key.subset];
    let mut strategies = Vec::with_capacity(subset.deployments.len());
    let mut bindings = Vec::with_capacity(subset.deployments.len());
    let mut contracts: Vec<ContractTerms> = Vec::new();
    for (position, (deployment, &alternative)) in
        subset.deployments.iter().zip(&key.alternatives).enumerate()
    {
        let id = format!("d{position}");
        let member = &members[deployment.member];
        let binding = &portfolio.bindings[deployment.binding];
        let field = |index: usize| format!("{id} (member {}) condition {index}", deployment.member);
        let (plan_identity, conditions) = match form {
            Form::Logical => (
                format!("{LOGICAL_PLAN_IDENTITY}:{}", binding.instrument),
                member
                    .conditions
                    .iter()
                    .map(|logical| match logical.ordinal {
                        None => logical.condition.clone(),
                        Some(ordinal) => Condition {
                            threshold: Threshold::Text(format!("interval {ordinal}")),
                            ..logical.condition.clone()
                        },
                    })
                    .collect(),
            ),
            Form::Resolved(plans) => {
                let plan = plans.get(&binding.instrument).ok_or_else(|| {
                    Failure::Error(format!(
                        "{id}: no fitted plan for instrument {}",
                        binding.instrument
                    ))
                })?;
                (
                    plan.identity(),
                    member
                        .conditions
                        .iter()
                        .enumerate()
                        .map(|(index, logical)| resolve_condition(&field(index), logical, plan))
                        .collect::<Result<Vec<_>, _>>()?,
                )
            }
        };
        strategies.push(StrategySpec {
            id: id.clone(),
            plan_identity,
            base_stream: member.base_stream,
            conditions,
            repair: portfolio.repairs[deployment.repair].conditions.clone(),
        });
        let chosen = &binding.alternatives[alternative];
        if !contracts
            .iter()
            .any(|contract| contract.id == chosen.contract.id)
        {
            contracts.push(chosen.contract.clone());
        }
        bindings.push(DeploymentBinding {
            id: id.clone(),
            strategy: id,
            account: binding.account.clone(),
            instrument: binding.instrument.clone(),
            contract: chosen.contract.id.clone(),
            risk_policy: portfolio.risk_policies[key.risk_policy].id.clone(),
            envelope: chosen.envelope.clone(),
        });
    }
    Ok(Policy {
        strategies,
        bindings,
        contracts,
        risk_policies: vec![portfolio.risk_policies[key.risk_policy].clone()],
    })
}

/// The joint replay table of one policy: the shared funded accounts, the reporting contract and
/// rates of the portfolio, and the given role, window, inputs, and splits.
pub fn replay_table(
    portfolio: &Portfolio,
    policy: &Policy,
    role: DatasetRole,
    decision_start: &str,
    decision_end: &str,
    inputs: Vec<ReplayInput>,
    splits: Option<Vec<Split>>,
) -> Replay {
    let accounts: Vec<AccountSpec> = portfolio.accounts.clone();
    Replay {
        role,
        decision_start: decision_start.to_string(),
        decision_end: decision_end.to_string(),
        inputs,
        splits,
        accounts,
        strategies: policy.strategies.clone(),
        bindings: policy.bindings.clone(),
        contracts: policy.contracts.clone(),
        risk_policies: policy.risk_policies.clone(),
        reporting_currency: portfolio.reporting_currency.clone(),
        reporting_scale: portfolio.reporting_scale,
        max_rate_age_micros: portfolio.max_rate_age_micros,
        rates: portfolio.rates.clone(),
        scenario: None,
    }
}

/// The structural verdict of one choice: the execution rules applied to its logical table with
/// the first fold's window and inputs, which they never open.
pub fn structure(portfolio: &Portfolio, policy: &Policy) -> Result<(), String> {
    let fold = &portfolio.folds[0];
    let table = replay_table(
        portfolio,
        policy,
        DatasetRole::Development,
        &fold.decision_start,
        &fold.decision_end,
        fold.inputs
            .iter()
            .map(|input| ReplayInput {
                tick_manifest: input.assessment_manifest.clone(),
                feature_manifest: input.assessment_manifest.clone(),
                outcome_manifest: None,
            })
            .collect(),
        None,
    );
    crate::execution::validate(&table)
}

// ----------------------------------------------------------------------------------------------
// Projection, gates, aggregation, and ranking
// ----------------------------------------------------------------------------------------------

/// The projection of one verified restored engine and the first gate it fails, if any.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct Projection {
    pub settled: u64,
    pub unresolved: u64,
    /// The restored ledger's final event time, at which every account is valued.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub valued_at: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub profit: Option<Decimal>,
    #[serde(default, skip_serializing_if = "BTreeSet::is_empty")]
    pub rates: BTreeSet<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub drawdown: Option<Decimal>,
    pub unavailable_observations: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failure: Option<String>,
}

/// Projects and gates one verified restored engine: settlement support first; only then every
/// account's native completed profit converted by the engine at the restored ledger's final
/// event time and summed with checked arithmetic; then the engine's reporting drawdown, which
/// passes only when every reporting observation was available. A missing rate is a failed
/// gate; an arithmetic error stops.
pub fn project(engine: &Engine, gates: &Gates) -> Result<Projection, String> {
    let summary = engine.summary();
    let mut projection = Projection {
        settled: summary.portfolio.settled,
        unresolved: summary.portfolio.unresolved,
        valued_at: summary.last_time_micros.map(format_event_time_micros),
        profit: None,
        rates: BTreeSet::new(),
        drawdown: summary.reporting.max_drawdown,
        unavailable_observations: summary.reporting.unavailable_observations,
        failure: None,
    };
    if projection.settled < gates.min_settled {
        projection.failure = Some(format!(
            "settled {} below the minimum {}",
            projection.settled, gates.min_settled
        ));
        return Ok(projection);
    }
    if projection.unresolved > gates.max_unresolved {
        projection.failure = Some(format!(
            "unresolved {} above the maximum {}",
            projection.unresolved, gates.max_unresolved
        ));
        return Ok(projection);
    }
    let replay = &engine.definition().replay;
    let mut total = Decimal::zero(replay.reporting_scale);
    for account in engine.accounts() {
        match engine.convert(account.completed_profit, &account.currency)? {
            Some(converted) => {
                total = total.checked_add(converted.amount)?;
                projection.rates.extend(converted.rate);
            }
            None => {
                projection.failure = Some(format!(
                    "the completed profit of account `{}` is unavailable: no {} to {} rate is available at {} within {} microseconds",
                    account.id,
                    account.currency,
                    replay.reporting_currency,
                    summary
                        .last_time_micros
                        .map_or_else(|| "the start".to_string(), format_event_time_micros),
                    replay.max_rate_age_micros
                ));
                return Ok(projection);
            }
        }
    }
    projection.profit = Some(total);
    if total.compare(gates.min_profit)? == Ordering::Less {
        projection.failure = Some(format!(
            "net profit {total} below the minimum {}",
            gates.min_profit
        ));
        return Ok(projection);
    }
    match (projection.drawdown, projection.unavailable_observations) {
        (Some(drawdown), 0) => {
            if drawdown.compare(gates.max_drawdown)? == Ordering::Greater {
                projection.failure = Some(format!(
                    "drawdown {drawdown} above the maximum {}",
                    gates.max_drawdown
                ));
            }
        }
        (_, unavailable) => {
            projection.failure = Some(format!(
                "drawdown is unavailable: {unavailable} reporting observations were unavailable"
            ));
        }
    }
    Ok(projection)
}

/// One replay generation a choice or the outer evaluation produced.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct ReplayRef {
    pub generation: String,
    pub summary_identity: String,
}

/// One required fold of one structurally valid choice.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
pub struct FoldResult {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub inapplicable: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub replay: Option<ReplayRef>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub projection: Option<Projection>,
}

impl FoldResult {
    /// Why the fold is not passing, if it is not.
    pub fn failure(&self) -> Option<&str> {
        self.inapplicable.as_deref().or_else(|| {
            self.projection
                .as_ref()
                .and_then(|projection| projection.failure.as_deref())
        })
    }
}

/// One declared complete choice and everything computed for it.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
pub struct Choice {
    pub subset: usize,
    pub alternatives: Vec<usize>,
    pub risk_policy: usize,
    pub identity: String,
    /// The structural rejection; a rejected choice is never replayed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rejection: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub folds: Vec<FoldResult>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub profit: Option<Decimal>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub drawdown: Option<Decimal>,
    /// The first fold that is not passing.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failure: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rank: Option<u32>,
}

impl Choice {
    pub fn key(&self) -> ChoiceKey {
        ChoiceKey {
            subset: self.subset,
            alternatives: self.alternatives.clone(),
            risk_policy: self.risk_policy,
        }
    }

    /// Aggregates the folds of a structurally valid choice: the sum of their completed profits
    /// and the largest fold drawdown when every fold passes, else the first failure.
    pub fn aggregate(&mut self) -> Result<(), String> {
        self.profit = None;
        self.drawdown = None;
        self.failure = None;
        if self.rejection.is_some() {
            return Ok(());
        }
        if let Some((index, fold)) = self
            .folds
            .iter()
            .enumerate()
            .find(|(_, fold)| fold.failure().is_some())
        {
            self.failure = Some(format!(
                "fold {index}: {}",
                fold.failure().expect("failing")
            ));
            return Ok(());
        }
        let mut profit: Option<Decimal> = None;
        let mut drawdown: Option<Decimal> = None;
        for fold in &self.folds {
            let projection = fold
                .projection
                .as_ref()
                .expect("a passing fold is projected");
            let (fold_profit, fold_drawdown) = (
                projection.profit.expect("a passing fold has a profit"),
                projection.drawdown.expect("a passing fold has a drawdown"),
            );
            profit = Some(match profit {
                None => fold_profit,
                Some(total) => total.checked_add(fold_profit)?,
            });
            drawdown = Some(match drawdown {
                None => fold_drawdown,
                Some(worst) => worst.max(fold_drawdown)?,
            });
        }
        self.profit = profit;
        self.drawdown = drawdown;
        Ok(())
    }

    /// Whether every required fold passed.
    pub fn passing(&self) -> bool {
        self.rejection.is_none() && self.failure.is_none() && !self.folds.is_empty()
    }
}

/// The frozen order: the objective's first value, its second value, fewer deployments, then the
/// canonical complete-choice identity.
fn compare(
    objective: Objective,
    (a, deployments_a): (&Choice, usize),
    (b, deployments_b): (&Choice, usize),
) -> Result<Ordering, String> {
    let profit = b
        .profit
        .expect("passing")
        .compare(a.profit.expect("passing"))?;
    let drawdown = a
        .drawdown
        .expect("passing")
        .compare(b.drawdown.expect("passing"))?;
    let objective = match objective {
        Objective::ProfitThenDrawdown => profit.then(drawdown),
        Objective::DrawdownThenProfit => drawdown.then(profit),
    };
    Ok(objective
        .then(deployments_a.cmp(&deployments_b))
        .then_with(|| a.identity.cmp(&b.identity)))
}

/// Ranks every passing choice under the frozen objective and tie breaks, writing one-based
/// ranks, and returns the selected choice: the first in rank order, or none when no choice
/// passes.
pub fn rank(
    portfolio: &Portfolio,
    choices: &mut [Choice],
    objective: Objective,
) -> Result<Option<usize>, String> {
    let mut passed: Vec<usize> = Vec::new();
    for (index, choice) in choices.iter_mut().enumerate() {
        choice.rank = None;
        if choice.passing() {
            passed.push(index);
        }
    }
    let deployments = |index: usize| portfolio.subsets[choices[index].subset].deployments.len();
    let mut error = None;
    passed.sort_by(|&a, &b| {
        match compare(
            objective,
            (&choices[a], deployments(a)),
            (&choices[b], deployments(b)),
        ) {
            Ok(ordering) => ordering,
            Err(reason) => {
                error.get_or_insert(reason);
                Ordering::Equal
            }
        }
    });
    if let Some(reason) = error {
        return Err(reason);
    }
    for (rank, &index) in passed.iter().enumerate() {
        choices[index].rank = Some(rank as u32 + 1);
    }
    Ok(passed.first().copied())
}

// ----------------------------------------------------------------------------------------------
// Published selection
// ----------------------------------------------------------------------------------------------

/// One member of a source family and every base that declares it; a member no base declares
/// is excluded from the universe by configuration, never by a search rank, score, or screen.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct SourceMember {
    pub logic_identity: String,
    pub contract: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub bases: Vec<usize>,
}

/// One verified development-only family the universe draws from, with every source member.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct FamilyRecord {
    pub generation: String,
    pub plan_identity: String,
    pub base_stream: StreamKey,
    pub members: Vec<SourceMember>,
}

/// One feature generation the selection built or applied: a fit, an assessment, a refit, or an
/// outer application.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct FeatureRef {
    pub instrument: String,
    pub input_generation: String,
    pub generation: String,
    pub plan_identity: String,
}

/// One inner fold's fitted plans and assessment generations, per instrument.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct FoldRecord {
    pub fits: Vec<FeatureRef>,
    pub assessments: Vec<FeatureRef>,
}

/// The outer evaluation of the frozen choice: one continuous joint replay whose reporting
/// splits attribute records without resetting cash, exposure, or unresolved obligations.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct Outer {
    pub features: Vec<FeatureRef>,
    pub replay: ReplayRef,
    pub projection: Projection,
    pub splits: BTreeMap<String, Group>,
}

/// The terminal state of a selection.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum State {
    /// The frozen choice is the deployable candidate; never a certification.
    Selected,
    NoFeasiblePolicy,
    RefitInapplicable {
        reason: String,
    },
    OuterRejected {
        reason: String,
    },
}

impl State {
    pub fn status(&self) -> &'static str {
        match self {
            Self::Selected => "selected",
            Self::NoFeasiblePolicy => "no_feasible_policy",
            Self::RefitInapplicable { .. } => "refit_inapplicable",
            Self::OuterRejected { .. } => "outer_rejected",
        }
    }
}

/// The complete selection as published in `selection.json`: the resolved configuration whose
/// hash the manifest binds, then everything the procedure computed.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
pub struct Selection {
    pub config: Config,
    pub families: Vec<FamilyRecord>,
    pub members: Vec<LogicalMember>,
    pub declared: u64,
    pub rejected: u64,
    pub valid: u64,
    pub passing: u64,
    pub folds: Vec<FoldRecord>,
    pub choices: Vec<Choice>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub selected: Option<usize>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub refit: Vec<FeatureRef>,
    /// The frozen policy resolved under the refit plans.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub frozen: Option<Policy>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub outer: Option<Outer>,
    pub state: State,
}

impl Selection {
    pub fn to_json(&self) -> Vec<u8> {
        let mut bytes = serde_json::to_vec_pretty(self).expect("a selection serializes");
        bytes.push(b'\n');
        bytes
    }

    pub fn from_json(bytes: &[u8]) -> Result<Self, String> {
        serde_json::from_slice(bytes).map_err(|error| error.to_string())
    }
}

/// The ready manifest of a selection generation.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct SelectionManifest {
    pub kind: String,
    pub schema_version: u32,
    pub generation: String,
    pub config_hash: String,
    pub code_revision: String,
    pub families: Vec<String>,
    pub state: String,
    pub objects: Vec<ObjectRecord>,
}

impl SelectionManifest {
    pub fn to_json(&self) -> Vec<u8> {
        let mut bytes = serde_json::to_vec_pretty(self).expect("a manifest serializes");
        bytes.push(b'\n');
        bytes
    }

    pub fn from_json(bytes: &[u8]) -> Result<Self, String> {
        let manifest: Self = serde_json::from_slice(bytes).map_err(|error| error.to_string())?;
        if manifest.kind != SELECTION_MANIFEST_KIND {
            return Err(format!(
                "kind `{}` is not `{SELECTION_MANIFEST_KIND}`",
                manifest.kind
            ));
        }
        if manifest.schema_version != SELECTION_SCHEMA_VERSION {
            return Err(format!(
                "unsupported schema_version {}, expected {SELECTION_SCHEMA_VERSION}",
                manifest.schema_version
            ));
        }
        if manifest.generation
            != selection_generation_id(
                &manifest.config_hash,
                &manifest.code_revision,
                &manifest.families,
            )
        {
            return Err(
                "generation is not the identity of its configuration, revision, and families"
                    .to_string(),
            );
        }
        crate::dataset::validate_objects(&manifest.objects)?;
        if manifest.objects.len() != 1 || manifest.objects[0].path != SELECTION_OBJECT_PATH {
            return Err(format!(
                "a selection generation publishes exactly `{SELECTION_OBJECT_PATH}`"
            ));
        }
        Ok(manifest)
    }

    pub fn key(&self) -> String {
        crate::dataset::manifest_key(&self.generation)
    }
}

/// The selection generation identity: the domain, the configuration hash, the code revision,
/// and every family generation, one per line.
pub fn selection_generation_id(
    config_hash: &str,
    code_revision: &str,
    families: &[String],
) -> String {
    let mut hasher = Sha256::new();
    hasher.update(SELECTION_DOMAIN_V1);
    for line in [config_hash, code_revision]
        .into_iter()
        .chain(families.iter().map(String::as_str))
    {
        hasher.update(line.as_bytes());
        hasher.update(b"\n");
    }
    crate::hex(&hasher.finalize())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mixed_radix_enumeration_cycles_the_last_deployment_fastest() {
        let mut digits = vec![0, 0];
        let radices = [2, 3];
        let mut seen = vec![digits.clone()];
        while advance(&mut digits, &radices) {
            seen.push(digits.clone());
        }
        assert_eq!(
            seen,
            vec![
                vec![0, 0],
                vec![0, 1],
                vec![0, 2],
                vec![1, 0],
                vec![1, 1],
                vec![1, 2]
            ]
        );
        let mut single = vec![0];
        assert!(!advance(&mut single, &[1]));
    }

    fn choice(identity: &str, profit: &str, drawdown: &str) -> Choice {
        Choice {
            subset: 0,
            alternatives: Vec::new(),
            risk_policy: 0,
            identity: identity.to_string(),
            rejection: None,
            folds: vec![FoldResult {
                inapplicable: None,
                replay: None,
                projection: Some(Projection {
                    settled: 1,
                    unresolved: 0,
                    valued_at: None,
                    profit: Some(Decimal::parse(profit).unwrap()),
                    rates: BTreeSet::new(),
                    drawdown: Some(Decimal::parse(drawdown).unwrap()),
                    unavailable_observations: 0,
                    failure: None,
                }),
            }],
            profit: Some(Decimal::parse(profit).unwrap()),
            drawdown: Some(Decimal::parse(drawdown).unwrap()),
            failure: None,
            rank: None,
        }
    }

    #[test]
    fn the_objective_orders_then_fewer_deployments_then_identity() {
        let a = choice("b", "5.00", "2.00");
        let b = choice("a", "5.00", "2.00");
        let c = choice("c", "6.00", "3.00");
        let profit_first = |x: &Choice, y: &Choice, dx, dy| {
            compare(Objective::ProfitThenDrawdown, (x, dx), (y, dy)).unwrap()
        };
        let drawdown_first = |x: &Choice, y: &Choice, dx, dy| {
            compare(Objective::DrawdownThenProfit, (x, dx), (y, dy)).unwrap()
        };
        assert_eq!(profit_first(&c, &a, 1, 1), Ordering::Less);
        assert_eq!(drawdown_first(&c, &a, 1, 1), Ordering::Greater);
        assert_eq!(profit_first(&a, &b, 2, 1), Ordering::Greater);
        assert_eq!(profit_first(&a, &b, 1, 1), Ordering::Greater);
        assert_eq!(profit_first(&b, &a, 1, 1), Ordering::Less);
    }

    #[test]
    fn aggregation_sums_profits_and_keeps_the_largest_drawdown() {
        let mut choice = choice("a", "1.50", "0.20");
        choice.folds.push(FoldResult {
            inapplicable: None,
            replay: None,
            projection: Some(Projection {
                settled: 1,
                unresolved: 0,
                valued_at: None,
                profit: Some(Decimal::parse("-0.25").unwrap()),
                rates: BTreeSet::new(),
                drawdown: Some(Decimal::parse("0.75").unwrap()),
                unavailable_observations: 0,
                failure: None,
            }),
        });
        choice.aggregate().unwrap();
        assert_eq!(choice.profit, Some(Decimal::parse("1.25").unwrap()));
        assert_eq!(choice.drawdown, Some(Decimal::parse("0.75").unwrap()));
        assert!(choice.passing());
        choice.folds.push(FoldResult {
            inapplicable: Some("collapsed".into()),
            replay: None,
            projection: None,
        });
        choice.aggregate().unwrap();
        assert_eq!(choice.failure.as_deref(), Some("fold 2: collapsed"));
        assert_eq!(choice.profit, None);
        assert!(!choice.passing());
    }

    #[test]
    fn the_manifest_binds_its_generation() {
        let families = vec!["f".to_string()];
        let generation = selection_generation_id("v3:sha256:x", "rev", &families);
        assert_eq!(generation.len(), 64);
        assert_ne!(
            generation,
            selection_generation_id("v3:sha256:y", "rev", &families)
        );
        assert_ne!(
            generation,
            selection_generation_id("v3:sha256:x", "rev", &[])
        );
    }
}

//! Pure compatibility measurement of the frozen baseline against journaled financial facts.

use std::collections::BTreeMap;

use binary_alpha_engine::config::AccountClass;
use binary_alpha_engine::execution::{
    CashAction, ContractTerms, Decimal, EventKind, FinancialEvent, Outcome, Resolution,
    RunDefinition,
};
use binary_alpha_engine::market::parse_event_time_micros;
use binary_alpha_engine::research::{EXECUTION_CONTRACT_V1, LiveSource, Window, digest};
use serde::Serialize;

use super::journal::{Record, RecordKind};

pub const RECEIPT_SCHEMA_VERSION: u32 = 1;
pub const CLOCK_BASIS: &str = "unix_epoch_micros; provider seconds × 1_000_000";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Status {
    Matched,
    OutsideEnvelope,
    Unavailable,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Dimension {
    pub name: &'static str,
    pub status: Status,
    pub samples: u32,
    pub required: u32,
    pub bound: String,
    pub reason: Option<String>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Promotion {
    pub eligible: bool,
    pub reasons: Vec<String>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, serde::Deserialize)]
pub struct Segment {
    pub key: String,
    pub sha256: String,
    pub bytes: u64,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Receipt {
    pub schema_version: u32,
    pub deployment: String,
    pub definition: String,
    pub bundle_sha256: String,
    pub research: String,
    pub frozen: String,
    pub certification: String,
    pub execution_contract: String,
    pub broker: String,
    pub account_class: AccountClass,
    pub required_account_class: AccountClass,
    pub instruments: Vec<String>,
    pub contracts: Vec<String>,
    pub currency: String,
    pub observation: Window,
    pub scenarios: Vec<String>,
    pub clock_basis: String,
    pub min_samples: u32,
    pub ledger: String,
    pub dimensions: Vec<Dimension>,
    pub promotion: Promotion,
}
impl Receipt {
    pub fn to_json(&self) -> Vec<u8> {
        serde_json::to_vec(self).expect("receipt serializes")
    }
    pub fn key(&self) -> String {
        key(&self.deployment, &self.to_json())
    }
}
pub fn key(deployment: &str, bytes: &[u8]) -> String {
    format!("live/{deployment}/receipts/{}.json", digest(b"", bytes))
}

pub struct Inputs<'a> {
    pub deployment: &'a str,
    pub definition: &'a RunDefinition,
    pub baseline: &'a [ContractTerms],
    pub source: &'a LiveSource,
    pub account_class: AccountClass,
    pub required_account_class: AccountClass,
    pub observation: &'a Window,
    pub min_samples: u32,
    pub scenarios: &'a [String],
    pub ledger: &'a str,
    pub events: &'a [FinancialEvent],
    pub refusals: &'a [Record],
}

#[derive(Default)]
struct Facts<'a> {
    signal: Option<&'a FinancialEvent>,
    accepted: Option<&'a FinancialEvent>,
    entry_price: Option<i64>,
    entry: Option<i64>,
    start: Option<i64>,
    exit_price: Option<i64>,
    due_tick: Option<(i64, i64)>,
    expiry: Option<i64>,
    exit: Option<i64>,
    terminal_at: Option<i64>,
    cash_at: Option<i64>,
    release: Option<(i64, Option<i64>, Option<i64>)>,
    transaction: Option<String>,
    contract: Option<String>,
}
impl Dimension {
    fn new(name: &'static str, bound: String, required: u32) -> Self {
        Self {
            name,
            status: Status::Matched,
            samples: 0,
            required,
            bound,
            reason: None,
        }
    }
    fn outside(&mut self, reason: String) {
        self.status = Status::OutsideEnvelope;
        self.detail(reason);
    }
    fn unavailable(&mut self, reason: impl Into<String>) {
        if self.status != Status::OutsideEnvelope {
            self.status = Status::Unavailable;
        }
        self.detail(reason.into());
    }
    fn detail(&mut self, reason: String) {
        if let Some(existing) = &mut self.reason {
            existing.push_str("; ");
            existing.push_str(&reason);
        } else {
            self.reason = Some(reason);
        }
    }
}
fn same(a: Decimal, b: Decimal) -> bool {
    a.compare(b) == Ok(std::cmp::Ordering::Equal)
}
fn baseline<'a>(inputs: &'a Inputs<'_>, signal: &FinancialEvent) -> Option<&'a ContractTerms> {
    let EventKind::Signal { binding, .. } = &signal.kind else {
        return None;
    };
    let binding = inputs
        .definition
        .replay
        .bindings
        .iter()
        .find(|b| b.id == *binding)?;
    inputs
        .baseline
        .iter()
        .find(|terms| terms.id == binding.contract)
}

/// Computes every mandatory dimension in fixed order; missing facts never imply zero delay.
pub fn compute(inputs: &Inputs<'_>) -> Receipt {
    let replay = &inputs.definition.replay;
    let bounds = inputs
        .baseline
        .iter()
        .map(|terms| terms.id.clone())
        .collect::<Vec<_>>()
        .join(",");
    let mut dimensions = vec![
        Dimension::new("economics_scope", bounds, inputs.min_samples),
        Dimension::new(
            "offer_availability_rejection",
            "every admitted command accepted at the assessed terms, no rejections".into(),
            inputs.min_samples,
        ),
        Dimension::new(
            "quote_age_entry_price",
            replay
                .risk_policies
                .iter()
                .map(|risk| {
                    format!(
                        "{}: max_quote_age_micros={}; entry_price_units=signal.quote_price_units",
                        risk.id, risk.max_quote_age_micros
                    )
                })
                .collect::<Vec<_>>()
                .join(","),
            inputs.min_samples,
        ),
        Dimension::new(
            "acceptance_delay",
            "0 microseconds (whole-second resolution)".into(),
            inputs.min_samples,
        ),
        Dimension::new(
            "contract_timing",
            "expiry=entry_time+duration; exit_time=expiry; start=entry_time; exit_price=first_due_tick_price".into(),
            inputs.min_samples,
        ),
        Dimension::new(
            "funds_release",
            "expiry_to_evidence=0; evidence_to_application=0; total=0 microseconds".into(),
            inputs.min_samples,
        ),
    ];
    let start = parse_event_time_micros(&inputs.observation.decision_start).ok();
    let end = parse_event_time_micros(&inputs.observation.decision_end).ok();
    let within = |time| {
        start
            .zip(end)
            .is_some_and(|(start, end)| start <= time && time < end)
    };
    let mut commands: BTreeMap<&str, Facts<'_>> = BTreeMap::new();
    let mut cash: BTreeMap<String, (&str, &str, i64)> = BTreeMap::new();
    let events: Vec<_> = inputs
        .events
        .iter()
        .filter(|event| within(event.time_micros))
        .collect();
    for event in events {
        match &event.kind {
            EventKind::Signal {
                command: Some(command),
                ..
            } => {
                commands.entry(command).or_default().signal = Some(event);
                dimensions[1].samples += 1;
            }
            EventKind::Accepted {
                command,
                liability,
                discrepancy,
                deficit,
                ..
            } => {
                let facts = commands.entry(command).or_default();
                facts.accepted = Some(event);
                if let Some(liability) = liability {
                    dimensions[0].samples += 1;
                    facts.contract = Some(liability.contract_ref.clone());
                }
                if *discrepancy || deficit.is_some() {
                    dimensions[0].outside(format!(
                        "{command}: accepted economics discrepancy or deficit"
                    ));
                }
                if let Some(signal) = facts.signal {
                    if let (
                        Some(terms),
                        EventKind::Signal {
                            proposal: Some(proposal),
                            ..
                        },
                    ) = (baseline(inputs, signal), &signal.kind)
                        && proposal.terms.same_economics(terms) != Ok(true)
                    {
                        dimensions[0].outside(format!(
                            "{command}: accepted terms differ from baseline {}",
                            terms.id
                        ));
                    }
                } else {
                    dimensions[0].unavailable(format!("{command}: missing signal baseline"));
                }
            }
            EventKind::Reconciled {
                command,
                resolution: Resolution::Purchased { debit, liability },
                ..
            } => {
                let facts = commands.entry(command).or_default();
                facts.accepted = Some(event);
                facts.contract = Some(liability.contract_ref.clone());
                dimensions[0].samples += 1;
                if let Some(signal) = facts.signal {
                    if let Some(terms) = baseline(inputs, signal) {
                        let economics = terms.purchase().and_then(|expected| {
                            Ok(debit.compare(expected)? == std::cmp::Ordering::Equal
                                && liability.payout.compare(terms.win.gross_return)?
                                    == std::cmp::Ordering::Equal)
                        });
                        match economics {
                            Ok(true) => {}
                            Ok(false) => dimensions[0].outside(format!("{command}: recovered actual debit or payout differs from baseline {}", terms.id)),
                            Err(error) => dimensions[0].unavailable(format!("{command}: recovered economics cannot be compared: {error}")),
                        }
                    } else {
                        dimensions[0].unavailable(format!("{command}: missing signal baseline"));
                    }
                    if let (
                        Some(terms),
                        EventKind::Signal {
                            proposal: Some(proposal),
                            ..
                        },
                    ) = (baseline(inputs, signal), &signal.kind)
                        && proposal.terms.same_economics(terms) != Ok(true)
                    {
                        dimensions[0].outside(format!(
                            "{command}: accepted terms differ from baseline {}",
                            terms.id
                        ));
                    }
                } else {
                    dimensions[0].unavailable(format!("{command}: missing signal baseline"));
                }
            }
            EventKind::Confirmed {
                command,
                entry_price_units,
                entry_time_micros,
                expiry_micros,
                start_micros,
                ..
            } => {
                let facts = commands.entry(command).or_default();
                facts.entry_price = facts.entry_price.or(*entry_price_units);
                facts.entry = facts.entry.or(*entry_time_micros);
                facts.start = facts.start.or(*start_micros);
                facts.expiry = facts.expiry.or(*expiry_micros);
            }
            EventKind::Unresolved {
                command,
                terminal: Some(terminal),
                source: Some(source),
                ..
            } => {
                let facts = commands.entry(command).or_default();
                if facts.release.is_none() {
                    facts.exit = terminal.exit_time_micros.or(facts.exit);
                    facts.exit_price = terminal.exit_price_units.or(facts.exit_price);
                    facts.terminal_at =
                        Some(facts.terminal_at.map_or(source.available_at_micros, |old| {
                            old.max(source.available_at_micros)
                        }));
                    facts.transaction = terminal
                        .transaction_ref
                        .clone()
                        .or(facts.transaction.clone());
                }
            }
            EventKind::CashObserved { source, fact, .. } if fact.action == CashAction::Sell => {
                cash.entry(format!("{}:{}", fact.account, fact.transaction_ref))
                    .or_insert((
                        fact.account.as_str(),
                        fact.contract_ref.as_deref().unwrap_or(""),
                        source.available_at_micros,
                    ));
                for facts in commands.values_mut().filter(|f| {
                    f.contract.as_deref() == fact.contract_ref.as_deref() && f.release.is_none()
                }) {
                    if facts
                        .transaction
                        .as_deref()
                        .is_none_or(|id| id == fact.transaction_ref)
                    {
                        facts.cash_at = Some(source.available_at_micros);
                    }
                }
            }
            EventKind::Released {
                command,
                rejected: true,
                source,
                ..
            } => dimensions[1].outside(format!("{command}: broker rejected ({})", source.id)),
            EventKind::Settled {
                command,
                gross_return,
                terminal_fee,
                outcome,
                discrepancy,
                deficit,
                transaction_ref,
                ..
            } => {
                let facts = commands.entry(command).or_default();
                if *discrepancy || deficit.is_some() {
                    dimensions[0].outside(format!(
                        "{command}: settled economics discrepancy or deficit"
                    ));
                }
                if let Some(terms) = facts.signal.and_then(|signal| baseline(inputs, signal)) {
                    let expected = match outcome {
                        Outcome::Win => terms.win,
                        Outcome::Loss => terms.loss,
                        Outcome::Tie => terms.tie,
                    };
                    if !same(*gross_return, expected.gross_return)
                        || !same(*terminal_fee, expected.terminal_fee)
                    {
                        dimensions[0].outside(format!(
                            "{command}: settled cashflow differs from baseline {} {outcome}",
                            terms.id
                        ));
                    }
                } else {
                    dimensions[0].unavailable(format!("{command}: missing settlement baseline"));
                }
                if let Some(id) = transaction_ref {
                    facts.cash_at = cash
                        .get(&format!("{}:{id}", replay.accounts[0].id))
                        .map(|(_, _, at)| *at);
                }
                facts
                    .release
                    .get_or_insert((event.time_micros, facts.terminal_at, facts.cash_at));
            }
            EventKind::Reconciled {
                command,
                source,
                resolution,
                profit: Some(_),
                ..
            } if matches!(
                resolution,
                Resolution::Settled { .. } | Resolution::ExternallyClosed { .. }
            ) =>
            {
                let facts = commands.entry(command).or_default();
                match resolution {
                    Resolution::Settled {
                        outcome,
                        gross_return,
                        terminal_fee,
                    } => {
                        if let Some(terms) =
                            facts.signal.and_then(|signal| baseline(inputs, signal))
                        {
                            let expected = match outcome {
                                Outcome::Win => terms.win,
                                Outcome::Loss => terms.loss,
                                Outcome::Tie => terms.tie,
                            };
                            if !same(*gross_return, expected.gross_return)
                                || !same(*terminal_fee, expected.terminal_fee)
                            {
                                dimensions[0].outside(format!("{command}: reconciled cashflow differs from baseline {} {outcome}",terms.id));
                            }
                        } else {
                            dimensions[0]
                                .unavailable(format!("{command}: missing reconciliation baseline"));
                        }
                        // An explicit zero-credit reconciliation supplies the otherwise absent
                        // cash evidence; its source availability is distinct from application.
                        if gross_return.is_zero()
                            && terminal_fee.is_zero()
                            && facts.cash_at.is_none()
                        {
                            facts.cash_at = Some(source.available_at_micros);
                        }
                    }
                    Resolution::ExternallyClosed { .. } => dimensions[0].outside(format!(
                        "{command}: external closure is outside the baseline outcome model"
                    )),
                    _ => unreachable!("guarded resolution"),
                }
                facts
                    .release
                    .get_or_insert((event.time_micros, facts.terminal_at, facts.cash_at));
            }
            _ => {}
        }
    }
    for record in inputs
        .refusals
        .iter()
        .filter(|record| within(record.time_micros))
    {
        if let RecordKind::DueTick {
            command,
            provider_time_micros,
            price_units,
        } = &record.kind
        {
            commands
                .entry(command)
                .or_default()
                .due_tick
                .get_or_insert((*provider_time_micros, *price_units));
        }
        if let RecordKind::Refused {
            binding, reason, ..
        } = &record.kind
        {
            dimensions[1].samples += 1;
            dimensions[1].outside(format!("{binding}: {reason}"));
        }
    }
    for (command, facts) in &commands {
        let Some(signal) = facts.signal else {
            continue;
        };
        let EventKind::Signal {
            quote_price_units,
            quote_time_micros,
            binding,
            ..
        } = &signal.kind
        else {
            continue;
        };
        let risk = replay
            .bindings
            .iter()
            .find(|b| b.id == *binding)
            .and_then(|b| {
                replay
                    .risk_policies
                    .iter()
                    .find(|risk| risk.id == b.risk_policy)
            });
        if facts.entry_price.is_some() {
            dimensions[2].samples += 1;
        }
        match (
            facts.entry_price,
            quote_price_units,
            quote_time_micros,
            risk,
        ) {
            (Some(price), Some(quote), Some(time), Some(risk)) => {
                if price != *quote
                    || signal
                        .time_micros
                        .checked_sub(*time)
                        .is_none_or(|age| age < 0 || age > risk.max_quote_age_micros)
                {
                    dimensions[2].outside(format!("{command}: entry_price_units={price}, quote_price_units={quote}, quote_age_micros={}", signal.time_micros.saturating_sub(*time)));
                }
            }
            _ => dimensions[2].unavailable(format!(
                "{command}: missing confirmed entry price or signal quote"
            )),
        }
        match facts.accepted.map(|event| &event.kind) {
            Some(EventKind::Accepted {
                liability: Some(liability),
                ..
            })
            | Some(EventKind::Reconciled {
                resolution: Resolution::Purchased { liability, .. },
                ..
            }) => {
                dimensions[3].samples += 1;
                let second = signal.time_micros - signal.time_micros.rem_euclid(1_000_000);
                let delay = liability.purchase_time_micros.checked_sub(second);
                if delay != Some(0) {
                    dimensions[3].outside(format!("{command}: purchase_time_micros - decision_second = {}; assessed 0 microseconds", delay.map_or_else(|| "overflow".into(), |d| d.to_string())));
                }
            }
            _ => dimensions[3].unavailable(format!("{command}: missing purchase time")),
        }
        if facts.entry.is_some() && facts.expiry.is_some() {
            dimensions[4].samples += 1;
        }
        match (
            facts.entry,
            facts.expiry,
            facts.exit,
            facts.start,
            facts.exit_price,
            facts.due_tick,
            baseline(inputs, signal),
        ) {
            (Some(entry), Some(expiry), Some(exit), Some(start), Some(exit_price), Some((tick_time, tick_price)), Some(terms)) => {
                if entry.checked_add(terms.duration_micros) != Some(expiry) || exit != expiry || start != entry || exit_price != tick_price || tick_time < expiry {
                    dimensions[4].outside(format!("{command}: entry_time={entry}, duration={}, expiry={expiry}, exit_time={exit}, start={start}, exit_price={exit_price}, due_tick={tick_price}@{tick_time}", terms.duration_micros));
                }
            }
            _ => dimensions[4].unavailable(format!(
                "{command}: missing confirmed entry, start, expiry, terminal exit price/time, or due tick"
            )),
        }
        match (facts.expiry, facts.release) {
            (Some(expiry), Some((application, Some(terminal), Some(cash)))) => {
                dimensions[5].samples += 1;
                let evidence = terminal.max(cash);
                match (evidence.checked_sub(expiry), application.checked_sub(evidence), application.checked_sub(expiry)) {
                    (Some(first), Some(second), Some(total)) => {
                        let detail = format!("{command}: expiry_to_evidence={first}; evidence_to_application={second}; total={total} microseconds");
                        if first != 0 || second != 0 { dimensions[5].outside(detail); } else { dimensions[5].detail(detail); }
                    }
                    _ => dimensions[5].unavailable(format!("{command}: release clock arithmetic overflow")),
                }
            }
            _ => dimensions[5].unavailable(format!("{command}: missing confirmed expiry, unresolved liability, or missing matched cash/zero-credit reconciliation")),
        }
    }
    for dimension in &mut dimensions {
        if dimension.samples == 0 && dimension.status == Status::Matched {
            dimension.unavailable(if dimension.name == "offer_availability_rejection" {
                "no rejection-model evidence"
            } else {
                "no measured samples"
            });
        }
        if dimension.samples < dimension.required && dimension.status == Status::Matched {
            dimension.status = Status::Unavailable;
            dimension.reason = Some(format!(
                "{} of {} required samples",
                dimension.samples, dimension.required
            ));
        }
    }
    let mut reasons: Vec<String> = dimensions
        .iter()
        .filter(|d| d.status != Status::Matched || d.samples < d.required)
        .map(|d| d.name.into())
        .collect();
    if inputs.account_class != inputs.required_account_class {
        reasons.push("account_class".into());
    }
    Receipt {
        schema_version: RECEIPT_SCHEMA_VERSION,
        deployment: inputs.deployment.into(),
        definition: binary_alpha_engine::execution::replay_generation_id(
            &inputs.definition.config_hash,
            &inputs.definition.instruments,
        ),
        bundle_sha256: inputs.source.bundle_sha256.clone(),
        research: inputs.source.research.clone(),
        frozen: inputs.source.frozen.clone(),
        certification: inputs.source.certification.clone(),
        execution_contract: EXECUTION_CONTRACT_V1.into(),
        broker: replay
            .accounts
            .first()
            .map_or_else(String::new, |a| a.broker.to_string()),
        account_class: inputs.account_class,
        required_account_class: inputs.required_account_class,
        instruments: inputs
            .definition
            .instruments
            .iter()
            .map(|i| i.instrument.clone())
            .collect(),
        contracts: inputs.baseline.iter().map(|c| c.id.clone()).collect(),
        currency: replay.reporting_currency.to_string(),
        observation: inputs.observation.clone(),
        scenarios: inputs.scenarios.into(),
        clock_basis: CLOCK_BASIS.into(),
        min_samples: inputs.min_samples,
        ledger: inputs.ledger.into(),
        dimensions,
        promotion: Promotion {
            eligible: reasons.is_empty(),
            reasons,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use binary_alpha_engine::config::Config;
    use serde_json::{Value, json};
    const T: i64 = 1_789_347_035_000_000;
    const DUE: i64 = T + 15_000_000;

    fn definition() -> RunDefinition {
        let config = Config::parse(include_str!(
            "../../tests/fixtures/phase10/execution-definition.toml"
        ))
        .unwrap();
        let mut replay = config.replay.unwrap();
        replay.contracts[0].win.gross_return = Decimal::parse("18.83").unwrap();
        RunDefinition {
            schema_version: 2,
            config_hash: "a".repeat(64),
            code_revision: "synthetic".into(),
            availability: "synthetic".into(),
            replay,
            instruments: vec![],
        }
    }
    fn events() -> Vec<FinancialEvent> {
        let source = |id: &str, at| json!({"id":id,"provider_time_micros":at,"available_at_micros":at,"simulated":true});
        vec![
            json!({"kind":"signal","instrument":"deriv:R_50","binding":"call","deployment_identity":"deployment","signal_logic_identity":"logic","stream":{"duration_seconds":5,"offset_seconds":0},"close_time_micros":T,"known_at_micros":T,"disposition":"admitted","quote_price_units":100,"quote_time_micros":T,"command":"c","reservation":"10"}),
            json!({"kind":"accepted","command":"c","source":source("buy",T),"debit":"10","reservation":"0","liability":{"contract_ref":"contract","transaction_ref":"buy","purchase_time_micros":T,"expected_start_micros":T,"payout":"18.83"}}),
            json!({"kind":"confirmed","command":"c","source":source("entry",T),"entry_price_units":100,"entry_time_micros":T,"start_micros":T,"expiry_micros":DUE}),
            json!({"kind":"unresolved","command":"c","reason":"awaiting_cash","evidence":"terminal","terminal":{"status":"won","exit_price_units":101,"exit_time_micros":DUE,"transaction_ref":"sell"},"source":source("terminal",DUE)}),
            json!({"kind":"cash_observed","source":source("sell",DUE),"fact":{"account":"a","transaction_ref":"sell","contract_ref":"contract","action":"sell","amount":"18.83","time_micros":DUE},"matched":"c"}),
            json!({"kind":"settled","command":"c","source":source("settle",DUE),"settlement_time_micros":DUE,"settlement_price_units":101,"outcome":"win","gross_return":"18.83","terminal_fee":"0","credit":"18.83","profit":"8.83","release":"0","discrepancy":false,"transaction_ref":"sell"}),
        ].into_iter().enumerate().map(|(i,mut value)| {value["sequence"]=json!(i);value["time_micros"]=json!(if i<3 {T}else{DUE});serde_json::from_value(value).unwrap()}).collect()
    }
    fn measured(events: &[FinancialEvent], min_samples: u32) -> Receipt {
        measured_with_due_tick(events, min_samples, Some(101))
    }
    fn measured_with_due_tick(
        events: &[FinancialEvent],
        min_samples: u32,
        due_tick: Option<i64>,
    ) -> Receipt {
        let definition = definition();
        let source = LiveSource {
            research: "research".into(),
            bundle_sha256: "bundle".into(),
            frozen: "frozen".into(),
            selection: "selection".into(),
            policy: "policy".into(),
            certification: "certification".into(),
        };
        let due_records = [Record {
            sequence: 1,
            previous_sha256: "0".repeat(64),
            time_micros: DUE,
            deployment: "deployment".into(),
            kind: RecordKind::DueTick {
                command: "c".into(),
                provider_time_micros: DUE,
                price_units: due_tick.unwrap_or(101),
            },
        }];
        compute(&Inputs {
            deployment: "deployment",
            definition: &definition,
            baseline: &definition.replay.contracts,
            source: &source,
            account_class: AccountClass::Demo,
            required_account_class: AccountClass::Demo,
            observation: &Window {
                decision_start: definition.replay.decision_start.clone(),
                decision_end: definition.replay.decision_end.clone(),
            },
            min_samples,
            scenarios: &[],
            ledger: "ledger",
            events,
            refusals: if due_tick.is_some() {
                &due_records
            } else {
                &[]
            },
        })
    }
    fn edit(events: &mut [FinancialEvent], i: usize, field: &str, value: Value) {
        let mut json = serde_json::to_value(&events[i]).unwrap();
        json[field] = value;
        events[i] = serde_json::from_value(json).unwrap();
    }
    fn outside(events: &[FinancialEvent], i: usize, bound: &str, reason: &str) {
        let receipt = measured(events, 1);
        let d = &receipt.dimensions[i];
        assert_eq!(d.status, Status::OutsideEnvelope);
        assert_eq!(d.bound, bound);
        assert_eq!(d.reason.as_deref(), Some(reason));
        assert!(!receipt.promotion.eligible);
    }
    #[test]
    fn contract_timing_bound_declares_all_four_requirements() {
        let receipt = measured(&events(), 1);
        assert_eq!(
            receipt.dimensions[4].bound,
            "expiry=entry_time+duration; exit_time=expiry; start=entry_time; exit_price=first_due_tick_price"
        );
    }
    #[test]
    fn all_dimensions_matched_and_sample_support_is_mandatory() {
        let events = events();
        let receipt = measured(&events, 1);
        assert!(receipt.promotion.eligible);
        assert_eq!(receipt.clock_basis, CLOCK_BASIS);
        assert_eq!(
            receipt
                .dimensions
                .iter()
                .map(|d| d.name)
                .collect::<Vec<_>>(),
            [
                "economics_scope",
                "offer_availability_rejection",
                "quote_age_entry_price",
                "acceptance_delay",
                "contract_timing",
                "funds_release"
            ]
        );
        for d in &receipt.dimensions {
            assert_eq!(d.status, Status::Matched);
            assert_eq!(d.samples, 1);
        }
        let insufficient = measured(&events, 2);
        assert!(!insufficient.promotion.eligible);
        assert_eq!(insufficient.promotion.reasons.len(), 6);
        for dimension in &insufficient.dimensions {
            assert_eq!(dimension.status, Status::Unavailable);
            assert_eq!((dimension.samples, dimension.required), (1, 2));
            assert_eq!(dimension.reason.as_deref(), Some("1 of 2 required samples"));
        }
        assert_eq!(receipt.to_json(), measured(&events, 1).to_json());
    }
    #[test]
    fn economics_scope_outside_and_unavailable() {
        let mut events = events();
        edit(&mut events, 5, "gross_return", json!("18.82"));
        outside(
            &events,
            0,
            "call",
            "c: settled cashflow differs from baseline call win",
        );
        events.remove(1);
        assert_eq!(
            measured(&events, 1).dimensions[0].status,
            Status::OutsideEnvelope
        );
        assert_eq!(measured(&[], 1).dimensions[0].status, Status::Unavailable);
    }
    #[test]
    fn offer_availability_rejection_outside_and_unavailable() {
        let mut events = events();
        events.push(FinancialEvent {
            sequence: 6,
            time_micros: DUE,
            kind: EventKind::Released {
                command: "refused".into(),
                source: binary_alpha_engine::execution::EventSource {
                    id: "broker-rejected".into(),
                    provider_time_micros: DUE,
                    available_at_micros: DUE,
                    simulated: true,
                },
                rejected: true,
                release: Decimal::zero(0),
            },
        });
        outside(
            &events,
            1,
            "every admitted command accepted at the assessed terms, no rejections",
            "refused: broker rejected (broker-rejected)",
        );
        let receipt = measured(&[], 1);
        assert_eq!(receipt.dimensions[1].status, Status::Unavailable);
        assert_eq!(
            receipt.dimensions[1].reason.as_deref(),
            Some("no rejection-model evidence")
        );
    }
    #[test]
    fn quote_age_entry_price_outside_and_unavailable() {
        let mut events = events();
        edit(&mut events, 2, "entry_price_units", json!(101));
        outside(
            &events,
            2,
            "p: max_quote_age_micros=120000000; entry_price_units=signal.quote_price_units",
            "c: entry_price_units=101, quote_price_units=100, quote_age_micros=0",
        );
        edit(&mut events, 2, "entry_price_units", Value::Null);
        assert_eq!(
            measured(&events, 1).dimensions[2].status,
            Status::Unavailable
        );
    }
    #[test]
    fn acceptance_delay_outside_and_unavailable() {
        let mut events = events();
        if let EventKind::Accepted {
            liability: Some(liability),
            ..
        } = &mut events[1].kind
        {
            liability.purchase_time_micros += 1_000_000;
        }
        outside(
            &events,
            3,
            "0 microseconds (whole-second resolution)",
            "c: purchase_time_micros - decision_second = 1000000; assessed 0 microseconds",
        );
        events.remove(1);
        assert_eq!(
            measured(&events, 1).dimensions[3].status,
            Status::Unavailable
        );
    }
    #[test]
    fn contract_timing_outside_and_unavailable() {
        let mut events = events();
        edit(&mut events, 2, "entry_time_micros", json!(T + 2_000_000));
        outside(
            &events,
            4,
            "expiry=entry_time+duration; exit_time=expiry; start=entry_time; exit_price=first_due_tick_price",
            &format!(
                "c: entry_time={}, duration=15000000, expiry={DUE}, exit_time={DUE}, start={T}, exit_price=101, due_tick=101@{DUE}",
                T + 2_000_000
            ),
        );
        edit(&mut events, 2, "expiry_micros", Value::Null);
        assert_eq!(
            measured(&events, 1).dimensions[4].status,
            Status::Unavailable
        );
    }
    #[test]
    fn contract_timing_requires_start_exit_price_and_due_tick() {
        let mut missing_start = events();
        edit(&mut missing_start, 2, "start_micros", Value::Null);
        assert_eq!(
            measured(&missing_start, 1).dimensions[4].status,
            Status::Unavailable
        );
        let mut missing_exit = events();
        let mut terminal = serde_json::to_value(&missing_exit[3]).unwrap();
        terminal["terminal"]["exit_price_units"] = Value::Null;
        missing_exit[3] = serde_json::from_value(terminal).unwrap();
        assert_eq!(
            measured(&missing_exit, 1).dimensions[4].status,
            Status::Unavailable
        );
        assert_eq!(
            measured_with_due_tick(&events(), 1, None).dimensions[4].status,
            Status::Unavailable
        );
    }
    #[test]
    fn funds_release_outside_and_unavailable() {
        let mut events = events();
        if let EventKind::CashObserved { source, .. } = &mut events[4].kind {
            source.available_at_micros += 2;
        }
        events[4].time_micros += 2;
        events[5].time_micros += 3;
        outside(
            &events,
            5,
            "expiry_to_evidence=0; evidence_to_application=0; total=0 microseconds",
            "c: expiry_to_evidence=2; evidence_to_application=1; total=3 microseconds",
        );
        events.retain(|e| !matches!(e.kind, EventKind::CashObserved { .. }));
        assert_eq!(
            measured(&events, 1).dimensions[5].status,
            Status::Unavailable
        );
    }
    #[test]
    fn zero_credit_needs_explicit_reconciliation_and_its_receipt_clock() {
        let mut events = events();
        events.retain(|event| {
            !matches!(
                event.kind,
                EventKind::CashObserved { .. } | EventKind::Settled { .. }
            )
        });
        if let EventKind::Unresolved {
            terminal: Some(terminal),
            ..
        } = &mut events[3].kind
        {
            terminal.status = binary_alpha_engine::execution::TerminalStatus::Lost;
            terminal.exit_price_units = Some(99);
        }
        assert_eq!(
            measured(&events, 1).dimensions[5].status,
            Status::Unavailable
        );
        events.push(FinancialEvent {
            sequence: 5,
            time_micros: DUE,
            kind: EventKind::Reconciled {
                command: "c".into(),
                source: binary_alpha_engine::execution::EventSource {
                    id: "zero-proof".into(),
                    provider_time_micros: DUE,
                    available_at_micros: DUE,
                    simulated: true,
                },
                resolution: Resolution::Settled {
                    outcome: Outcome::Loss,
                    gross_return: Decimal::zero(0),
                    terminal_fee: Decimal::zero(0),
                },
                release: Decimal::zero(0),
                debit: Decimal::zero(0),
                credit: Decimal::zero(0),
                profit: Some(Decimal::parse("-10").unwrap()),
            },
        });
        let receipt = measured_with_due_tick(&events, 1, Some(99));
        assert_eq!(receipt.dimensions[5].status, Status::Matched);
        assert!(receipt.promotion.eligible);
        if let EventKind::Reconciled { source, .. } = &mut events.last_mut().unwrap().kind {
            source.available_at_micros += 2;
        }
        events.last_mut().unwrap().time_micros += 3;
        outside(
            &events,
            5,
            "expiry_to_evidence=0; evidence_to_application=0; total=0 microseconds",
            "c: expiry_to_evidence=2; evidence_to_application=1; total=3 microseconds",
        );
    }
}

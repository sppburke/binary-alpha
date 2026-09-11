//! The one chronological owner of strategy evaluation, admission, settlement, accounting, and
//! risk: `Engine`. Historical replay, research, and live adapters hand it observations in
//! availability order and it returns canonical `FinancialEvent`s; applying those records back
//! through the same function restores every financial state and summary projection.
//! `docs/contracts.md`, section "Execution", is the normative description.
//!
//! Money is exact: a checked signed 128-bit coefficient at a declared decimal scale. Features and
//! path movements stay in their native units; only nonfinancial projections divide.

use std::borrow::Cow;
use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::fmt;

use serde::de::Error as _;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use sha2::{Digest, Sha256};

use crate::config::{ManifestUri, Replay, StreamKey};
use crate::dataset::{DatasetRole, ObjectRecord, ObjectRole, manifest_key, validate_objects};
use crate::features::{FittedEncoding, Kind, Value};
use crate::market::{
    BrokerId, Currency, ProviderSymbol, format_event_time_micros, parse_event_time_micros,
    split_decimal,
};

/// The ledger and manifest schema written and accepted by this checkout.
pub const REPLAY_SCHEMA_VERSION: u32 = 1;

/// The `kind` an engine replay ready manifest declares.
pub const REPLAY_MANIFEST_KIND: &str = "engine_replay";

/// The two objects of a replay generation: the canonical ledger and its summary projection.
pub const EVENTS_OBJECT_PATH: &str = "ledger/events.jsonl";
pub const SUMMARY_OBJECT_PATH: &str = "summary.json";

/// The availability assumption every historical replay tags: source files carry no local
/// receipts, so configured availability follows provider order.
pub const HISTORICAL_AVAILABILITY: &str = "provider_order_simulation";

/// The largest decimal scale of any money value.
pub const MAX_SCALE: u8 = 18;

const REPLAY_GENERATION_DOMAIN_V1: &[u8] = b"binary-alpha engine replay v1\n";
const SIGNAL_LOGIC_DOMAIN_V1: &[u8] = b"binary-alpha signal logic v1\n";
const DEPLOYMENT_DOMAIN_V1: &[u8] = b"binary-alpha deployment strategy v1\n";
const STATE_DOMAIN_V1: &[u8] = b"binary-alpha engine state v1\n";
const SUMMARY_DOMAIN_V1: &[u8] = b"binary-alpha engine summary v1\n";

// ---------------------------------------------------------------------------------------------
// Exact money
// ---------------------------------------------------------------------------------------------

/// An exact decimal: `coefficient / 10^scale`, with `scale` at most [`MAX_SCALE`]. Values keep the
/// scale they were declared or posted at; identity compares the normalized value.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Decimal {
    coefficient: i128,
    scale: u8,
}

impl Decimal {
    pub const fn zero(scale: u8) -> Self {
        Self {
            coefficient: 0,
            scale,
        }
    }

    pub const fn coefficient(self) -> i128 {
        self.coefficient
    }

    pub const fn scale(self) -> u8 {
        self.scale
    }

    /// Parses plain decimal text such as `-9.50` exactly; the scale is the fraction length and
    /// the whole signed 128-bit coefficient range round-trips.
    pub fn parse(text: &str) -> Result<Self, String> {
        let (negative, whole, fraction) = split_decimal(text)?;
        if fraction.len() > usize::from(MAX_SCALE) {
            return Err(format!(
                "`{text}` has {} fraction digits, more than {MAX_SCALE}",
                fraction.len()
            ));
        }
        let sign = if negative { "-" } else { "" };
        let coefficient = format!("{sign}{whole}{fraction}")
            .parse::<i128>()
            .map_err(|_| format!("`{text}` overflows the exact money representation"))?;
        Ok(Self {
            coefficient,
            scale: fraction.len() as u8,
        })
    }

    pub const fn is_negative(self) -> bool {
        self.coefficient < 0
    }

    pub const fn is_zero(self) -> bool {
        self.coefficient == 0
    }

    /// The same value with trailing fraction zeros removed: the identity form.
    pub fn normalized(mut self) -> Self {
        while self.scale > 0 && self.coefficient % 10 == 0 {
            self.coefficient /= 10;
            self.scale -= 1;
        }
        self
    }

    /// The same value at another scale, rejecting overflow and any lost precision.
    pub fn rescale(self, scale: u8) -> Result<Self, String> {
        if scale > MAX_SCALE {
            return Err(format!("scale {scale} exceeds {MAX_SCALE}"));
        }
        let coefficient = match scale.cmp(&self.scale) {
            Ordering::Equal => self.coefficient,
            Ordering::Greater => self
                .coefficient
                .checked_mul(pow10(scale - self.scale))
                .ok_or_else(|| format!("{self} overflows at scale {scale}"))?,
            Ordering::Less => {
                let unit = pow10(self.scale - scale);
                if self.coefficient % unit != 0 {
                    return Err(format!("{self} loses precision at scale {scale}"));
                }
                self.coefficient / unit
            }
        };
        Ok(Self { coefficient, scale })
    }

    fn aligned(self, other: Self) -> Result<(i128, i128, u8), String> {
        let scale = self.scale.max(other.scale);
        Ok((
            self.rescale(scale)?.coefficient,
            other.rescale(scale)?.coefficient,
            scale,
        ))
    }

    pub fn checked_add(self, other: Self) -> Result<Self, String> {
        let (a, b, scale) = self.aligned(other)?;
        a.checked_add(b)
            .map(|coefficient| Self { coefficient, scale })
            .ok_or_else(|| format!("{self} + {other} overflows"))
    }

    pub fn checked_sub(self, other: Self) -> Result<Self, String> {
        let (a, b, scale) = self.aligned(other)?;
        a.checked_sub(b)
            .map(|coefficient| Self { coefficient, scale })
            .ok_or_else(|| format!("{self} - {other} overflows"))
    }

    /// The exact product, normalized so that only genuinely needed fraction digits remain, and
    /// rejected when they exceed [`MAX_SCALE`].
    pub fn checked_mul(self, other: Self) -> Result<Self, String> {
        let coefficient = self
            .coefficient
            .checked_mul(other.coefficient)
            .ok_or_else(|| format!("{self} × {other} overflows"))?;
        let product = Self {
            coefficient,
            scale: self.scale + other.scale,
        }
        .normalized();
        if product.scale > MAX_SCALE {
            return Err(format!(
                "{self} × {other} needs {} fraction digits, more than {MAX_SCALE}",
                product.scale
            ));
        }
        Ok(product)
    }

    pub fn compare(self, other: Self) -> Result<Ordering, String> {
        let (a, b, _) = self.aligned(other)?;
        Ok(a.cmp(&b))
    }

    pub fn max(self, other: Self) -> Result<Self, String> {
        Ok(if self.compare(other)? == Ordering::Less {
            other
        } else {
            self
        })
    }
}

fn pow10(digits: u8) -> i128 {
    10_i128.pow(u32::from(digits))
}

impl fmt::Display for Decimal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let magnitude = self.coefficient.unsigned_abs().to_string();
        let sign = if self.coefficient < 0 { "-" } else { "" };
        let scale = usize::from(self.scale);
        if scale == 0 {
            return write!(f, "{sign}{magnitude}");
        }
        let padded = format!("{magnitude:0>width$}", width = scale + 1);
        let (whole, fraction) = padded.split_at(padded.len() - scale);
        write!(f, "{sign}{whole}.{fraction}")
    }
}

impl Serialize for Decimal {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(self)
    }
}

impl<'de> Deserialize<'de> for Decimal {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        Self::parse(&String::deserialize(deserializer)?).map_err(D::Error::custom)
    }
}

/// The nonfinancial basis-point projection of a price movement: `move / |entry| × 10000` to ten
/// decimal places, round to nearest, ties to even; absent with its reason at a zero entry price.
pub fn basis_points_text(move_units: i64, entry_units: i64) -> Result<String, String> {
    if entry_units == 0 {
        return Err("zero entry price, no denominator".to_string());
    }
    let numerator = u128::from(move_units.unsigned_abs()) * 10_u128.pow(14);
    let denominator = u128::from(entry_units.unsigned_abs());
    let (mut quotient, remainder) = (numerator / denominator, numerator % denominator);
    match (remainder * 2).cmp(&denominator) {
        Ordering::Greater => quotient += 1,
        Ordering::Equal if quotient % 2 == 1 => quotient += 1,
        _ => {}
    }
    let magnitude = i128::try_from(quotient).map_err(|_| "projection overflows".to_string())?;
    Ok(Decimal {
        coefficient: if move_units < 0 {
            -magnitude
        } else {
            magnitude
        },
        scale: 10,
    }
    .to_string())
}

// ---------------------------------------------------------------------------------------------
// Version-one neutral records
// ---------------------------------------------------------------------------------------------

/// One funded account: a typed identity at one broker, its currency, its posting scale, and the
/// cash it starts with. Runs start with no obligations.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AccountSpec {
    pub id: String,
    pub broker: BrokerId,
    pub currency: Currency,
    pub scale: u8,
    pub initial_cash: Decimal,
}

crate::string_enum! {
    /// How a feature value is compared with its threshold.
    Comparator "comparator" {
        Eq => "eq",
        Ne => "ne",
        Lt => "lt",
        Le => "le",
        Gt => "gt",
        Ge => "ge",
    }
}

/// A canonical typed threshold: text for text outputs, a finite number for numeric outputs, or a
/// boolean. An integer literal is a number.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
#[serde(untagged)]
pub enum Threshold {
    Text(String),
    Number(f64),
    Bool(bool),
}

impl Threshold {
    /// The exact text hashed into identities.
    fn canonical(&self) -> String {
        match self {
            Self::Text(text) => format!("text:{text}"),
            Self::Number(value) => format!("number:{value:?}"),
            Self::Bool(value) => format!("bool:{value}"),
        }
    }
}

/// One typed feature condition on one stream's compiled output or fitted encoding.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Condition {
    pub stream: StreamKey,
    pub output: String,
    pub comparator: Comparator,
    pub threshold: Threshold,
}

impl Condition {
    fn canonical(&self) -> String {
        format!(
            "{} {} {} {}",
            self.stream,
            self.output,
            self.comparator,
            self.threshold.canonical()
        )
    }
}

/// The frozen-plan identity, base candle stream, and flat conjunction of conditions a signal
/// requires; `repair` is an additional conjunction checked after selection and deduplication.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct StrategySpec {
    pub id: String,
    pub plan_identity: String,
    pub base_stream: StreamKey,
    pub conditions: Vec<Condition>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub repair: Vec<Condition>,
}

/// The signal-logic identity: the frozen plan, base stream, and conditions in canonical order
/// with exact duplicates removed, excluding contract direction, duration, and economics.
pub fn signal_logic_identity(strategy: &StrategySpec) -> String {
    let mut canonical: Vec<String> = strategy
        .conditions
        .iter()
        .map(Condition::canonical)
        .collect();
    canonical.sort_unstable();
    canonical.dedup();
    let mut hasher = Sha256::new();
    hasher.update(SIGNAL_LOGIC_DOMAIN_V1);
    for line in [
        strategy.plan_identity.as_str(),
        &strategy.base_stream.to_string(),
    ]
    .into_iter()
    .chain(canonical.iter().map(String::as_str))
    {
        hasher.update(line.as_bytes());
        hasher.update(b"\n");
    }
    crate::hex(&hasher.finalize())
}

crate::string_enum! {
    /// The contract direction: a buy pays when the settlement price is above the entry price, a
    /// sell when it is below.
    Direction "direction" {
        Buy => "buy",
        Sell => "sell",
    }
}

impl Direction {
    const fn sign(self) -> i64 {
        match self {
            Self::Buy => 1,
            Self::Sell => -1,
        }
    }
}

crate::string_enum! {
    /// The declared settlement-evidence rule of a quote. `price_at_due_v1` settles on the first
    /// observed tick at or after the due time when no gap larger than the maximum intersects the
    /// contract window and the tick is not later than the maximum delay.
    SettlementRule "settlement_rule" {
        PriceAtDueV1 => "price_at_due_v1",
    }
}

/// The cash returned and the fee charged when one outcome settles, in the account currency.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Cashflow {
    pub gross_return: Decimal,
    pub terminal_fee: Decimal,
}

/// The settlement rule and its evidence limits.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Settlement {
    pub rule: SettlementRule,
    pub max_settlement_delay_micros: i64,
    pub max_tick_gap_micros: i64,
}

/// One quoted contract's explicit terms: direction, duration, currency, stake, purchase cost,
/// entry fee, the exhaustive win, loss, and tie cashflows, and the settlement rule. In a
/// historical replay the configured template is the declared quote.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ContractTerms {
    pub id: String,
    pub direction: Direction,
    pub duration_micros: i64,
    pub currency: Currency,
    pub stake: Decimal,
    pub quoted_cost: Decimal,
    pub entry_fee: Decimal,
    pub win: Cashflow,
    pub loss: Cashflow,
    pub tie: Cashflow,
    pub settlement: Settlement,
}

crate::string_enum! {
    /// The settled result of one contract.
    Outcome "outcome" {
        Win => "win",
        Loss => "loss",
        Tie => "tie",
    }
}

impl ContractTerms {
    fn cashflow(&self, outcome: Outcome) -> Cashflow {
        match outcome {
            Outcome::Win => self.win,
            Outcome::Loss => self.loss,
            Outcome::Tie => self.tie,
        }
    }

    /// `A = quoted_cost + entry_fee`, the paid purchase basis.
    pub fn purchase(&self) -> Result<Decimal, String> {
        self.quoted_cost.checked_add(self.entry_fee)
    }

    /// `max_outcomes(terminal_fee - gross_return)`, the worst terminal cashflow.
    fn worst_terminal(&self) -> Result<Decimal, String> {
        let mut worst = self.win.terminal_fee.checked_sub(self.win.gross_return)?;
        for cashflow in [self.loss, self.tie] {
            worst = worst.max(cashflow.terminal_fee.checked_sub(cashflow.gross_return)?)?;
        }
        Ok(worst)
    }

    /// `F = max(0, worst terminal)`, reserved beyond the purchase.
    fn terminal_reserve(&self) -> Result<Decimal, String> {
        self.worst_terminal()?.max(Decimal::zero(0))
    }

    /// `A + F`, reserved before dispatch.
    pub fn reservation(&self) -> Result<Decimal, String> {
        self.purchase()?.checked_add(self.terminal_reserve()?)
    }

    /// `max(0, A + worst terminal)`, the worst unresolved loss.
    pub fn worst_loss(&self) -> Result<Decimal, String> {
        self.purchase()?
            .checked_add(self.worst_terminal()?)?
            .max(Decimal::zero(0))
    }

    /// `gross_payout - quoted_cost - entry_fee - win_terminal_fee`.
    pub fn winning_net(&self) -> Result<Decimal, String> {
        self.win
            .gross_return
            .checked_sub(self.purchase()?)?
            .checked_sub(self.win.terminal_fee)
    }
}

/// The frozen economic envelope a binding accepts: maximum costs and fees, the minimum winning net
/// return, and the settlement rule the quote must declare.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Envelope {
    pub max_purchase_cost: Decimal,
    pub max_entry_fee: Decimal,
    pub max_win_terminal_fee: Decimal,
    pub max_loss_terminal_fee: Decimal,
    pub max_tie_terminal_fee: Decimal,
    pub min_winning_net_return: Decimal,
    pub settlement_rule: SettlementRule,
}

impl Envelope {
    fn admits(&self, terms: &ContractTerms) -> Result<bool, String> {
        let within = |value: Decimal, maximum: Decimal| -> Result<bool, String> {
            Ok(value.compare(maximum)? != Ordering::Greater)
        };
        Ok(terms.settlement.rule == self.settlement_rule
            && within(terms.quoted_cost, self.max_purchase_cost)?
            && within(terms.entry_fee, self.max_entry_fee)?
            && within(terms.win.terminal_fee, self.max_win_terminal_fee)?
            && within(terms.loss.terminal_fee, self.max_loss_terminal_fee)?
            && within(terms.tie.terminal_fee, self.max_tie_terminal_fee)?
            && terms.winning_net()?.compare(self.min_winning_net_return)? != Ordering::Less)
    }

    /// The identity form: every amount normalized.
    fn normalized(&self) -> Self {
        Self {
            max_purchase_cost: self.max_purchase_cost.normalized(),
            max_entry_fee: self.max_entry_fee.normalized(),
            max_win_terminal_fee: self.max_win_terminal_fee.normalized(),
            max_loss_terminal_fee: self.max_loss_terminal_fee.normalized(),
            max_tie_terminal_fee: self.max_tie_terminal_fee.normalized(),
            min_winning_net_return: self.min_winning_net_return.normalized(),
            settlement_rule: self.settlement_rule,
        }
    }
}

/// One deployment: an ordered strategy, account, instrument, contract template, and risk policy
/// reference plus the frozen envelope. Binding list order is candidate priority.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DeploymentBinding {
    pub id: String,
    pub strategy: String,
    pub account: String,
    /// `BROKER:PROVIDER_SYMBOL`, the neutral instrument identity of one replay input.
    pub instrument: String,
    pub contract: String,
    pub risk_policy: String,
    pub envelope: Envelope,
}

/// The deployment-strategy identity: the signal logic plus direction, duration, currency, and the
/// normalized envelope. Actual quotes belong to events, never to identities.
pub fn deployment_identity(logic: &str, contract: &ContractTerms, envelope: &Envelope) -> String {
    let mut hasher = Sha256::new();
    hasher.update(DEPLOYMENT_DOMAIN_V1);
    for line in [
        logic,
        contract.direction.as_str(),
        &contract.duration_micros.to_string(),
        contract.currency.as_str(),
    ] {
        hasher.update(line.as_bytes());
        hasher.update(b"\n");
    }
    hasher.update(serde_json::to_vec(&envelope.normalized()).expect("an envelope serializes"));
    crate::hex(&hasher.finalize())
}

crate::string_enum! {
    /// Which matching signals of one account, instrument, contract duration, and entry event
    /// proceed to admission.
    SameEntry "same_entry" {
        All => "all",
        First => "first",
    }
}

/// An account-native drawdown pause: triggered when the epoch drawdown reaches the threshold,
/// lasting the duration.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Pause {
    pub drawdown: Decimal,
    pub duration_micros: i64,
}

/// Explicit capacity maxima, selection and deduplication, freshness limits, unresolved-loss
/// limits, and the drawdown pause. An absent limit is no limit for that scope.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RiskPolicy {
    pub id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_open_per_strategy: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_open_per_duration: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_open_per_instrument: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_open_per_account: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_open_total: Option<u32>,
    pub same_entry: SameEntry,
    pub deduplicate_signal_logic: bool,
    pub max_feature_age_micros: i64,
    pub max_quote_age_micros: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_unresolved_loss_per_account: Option<Decimal>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_unresolved_loss_total: Option<Decimal>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pause: Option<Pause>,
}

/// One supplied immutable conversion rate: reporting-currency units per source-currency unit.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RateEvent {
    pub id: String,
    pub source_currency: Currency,
    pub reporting_currency: Currency,
    pub provider: String,
    pub provider_time: String,
    pub available_at: String,
    pub rate: Decimal,
}

/// One reporting label over a half-open decision-time interval.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Split {
    pub name: String,
    pub start: String,
    pub end: String,
}

/// The verified manifests of one instrument's inputs.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ReplayInput {
    pub tick_manifest: ManifestUri,
    pub feature_manifest: ManifestUri,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub outcome_manifest: Option<ManifestUri>,
}

fn identifier(field: &str, text: &str) -> Result<(), String> {
    if text.is_empty() || text.bytes().any(|byte| byte.is_ascii_control()) {
        return Err(format!(
            "{field}: must be a non-empty identifier without a control character"
        ));
    }
    Ok(())
}

fn unique<'a>(field: &str, ids: impl Iterator<Item = &'a str>) -> Result<(), String> {
    let mut seen = HashSet::new();
    for (index, id) in ids.enumerate() {
        identifier(&format!("{field}[{index}].id"), id)?;
        if !seen.insert(id) {
            return Err(format!("{field}[{index}].id: `{id}` is listed twice"));
        }
    }
    Ok(())
}

fn non_negative(field: &str, value: Decimal) -> Result<(), String> {
    if value.is_negative() {
        return Err(format!("{field}: {value} must not be negative"));
    }
    Ok(())
}

fn positive(field: &str, value: Decimal) -> Result<(), String> {
    if value.is_negative() || value.is_zero() {
        return Err(format!("{field}: {value} must be positive"));
    }
    Ok(())
}

fn time(field: &str, text: &str) -> Result<i64, String> {
    parse_event_time_micros(text).map_err(|reason| format!("{field}: {reason}"))
}

/// The rules of the `replay` table a single field's deserializer cannot see: unique identities,
/// resolved references, exact and representable money, positive durations and thresholds,
/// non-negative ages, agreeing splits, and one immutable shared policy per scope.
pub fn validate(replay: &Replay) -> Result<(), String> {
    if replay.role == DatasetRole::Holdout {
        return Err("role: holdout data never enters a replay".to_string());
    }
    let start = time("decision_start", &replay.decision_start)?;
    let end = time("decision_end", &replay.decision_end)?;
    if start >= end {
        return Err(format!(
            "decision_end: {} must be after decision_start {}",
            replay.decision_end, replay.decision_start
        ));
    }
    if replay.inputs.is_empty() {
        return Err("inputs: at least one instrument input is required".to_string());
    }
    for (index, input) in replay.inputs.iter().enumerate() {
        if replay.inputs[..index]
            .iter()
            .any(|earlier| earlier.tick_manifest == input.tick_manifest)
        {
            return Err(format!(
                "inputs[{index}].tick_manifest: {} is listed twice",
                input.tick_manifest
            ));
        }
    }
    let mut intervals: Vec<(&str, i64, i64)> = Vec::new();
    for (index, split) in replay.splits.iter().flatten().enumerate() {
        let field = |name: &str| format!("splits[{index}].{name}");
        identifier(&field("name"), &split.name)?;
        let split_start = time(&field("start"), &split.start)?;
        let split_end = time(&field("end"), &split.end)?;
        if split_start >= split_end || split_start < start || split_end > end {
            return Err(format!(
                "{}: {}..{} must be a nonempty interval inside the decision window",
                field("start"),
                split.start,
                split.end
            ));
        }
        if let Some((earlier, _, _)) =
            intervals.iter().find(|(name, earlier_start, earlier_end)| {
                *name == split.name || (*earlier_start < split_end && split_start < *earlier_end)
            })
        {
            return Err(format!(
                "{}: `{}` repeats or overlaps `{earlier}`",
                field("name"),
                split.name
            ));
        }
        intervals.push((&split.name, split_start, split_end));
    }
    if replay.accounts.is_empty() {
        return Err("accounts: at least one account is required".to_string());
    }
    unique("accounts", replay.accounts.iter().map(|a| a.id.as_str()))?;
    for (index, account) in replay.accounts.iter().enumerate() {
        let field = |name: &str| format!("accounts[{index}].{name}");
        if account.scale > MAX_SCALE {
            return Err(format!(
                "{}: {} exceeds {MAX_SCALE}",
                field("scale"),
                account.scale
            ));
        }
        non_negative(&field("initial_cash"), account.initial_cash)?;
        account
            .initial_cash
            .rescale(account.scale)
            .map_err(|reason| format!("{}: {reason}", field("initial_cash")))?;
    }
    if replay.strategies.is_empty() {
        return Err("strategies: at least one strategy is required".to_string());
    }
    unique(
        "strategies",
        replay.strategies.iter().map(|s| s.id.as_str()),
    )?;
    for (index, strategy) in replay.strategies.iter().enumerate() {
        let field = |name: &str| format!("strategies[{index}].{name}");
        identifier(&field("plan_identity"), &strategy.plan_identity)?;
        if strategy.conditions.is_empty() {
            return Err(format!(
                "{}: at least one condition is required",
                field("conditions")
            ));
        }
        for (name, conditions) in [
            ("conditions", &strategy.conditions),
            ("repair", &strategy.repair),
        ] {
            for (position, condition) in conditions.iter().enumerate() {
                let field = |part: &str| format!("{}[{position}].{part}", field(name));
                identifier(&field("output"), &condition.output)?;
                match (&condition.threshold, condition.comparator) {
                    (Threshold::Number(value), _) if !value.is_finite() => {
                        return Err(format!("{}: {value} must be finite", field("threshold")));
                    }
                    (Threshold::Text(_) | Threshold::Bool(_), comparator)
                        if !matches!(comparator, Comparator::Eq | Comparator::Ne) =>
                    {
                        return Err(format!(
                            "{}: text and boolean thresholds compare only with `eq` or `ne`",
                            field("comparator")
                        ));
                    }
                    _ => {}
                }
            }
        }
    }
    unique("contracts", replay.contracts.iter().map(|c| c.id.as_str()))?;
    for (index, contract) in replay.contracts.iter().enumerate() {
        let field = |name: &str| format!("contracts[{index}].{name}");
        if contract.duration_micros <= 0 {
            return Err(format!("{}: must be positive", field("duration_micros")));
        }
        positive(&field("stake"), contract.stake)?;
        positive(&field("quoted_cost"), contract.quoted_cost)?;
        non_negative(&field("entry_fee"), contract.entry_fee)?;
        for (name, cashflow) in [
            ("win", contract.win),
            ("loss", contract.loss),
            ("tie", contract.tie),
        ] {
            non_negative(
                &field(&format!("{name}.gross_return")),
                cashflow.gross_return,
            )?;
            non_negative(
                &field(&format!("{name}.terminal_fee")),
                cashflow.terminal_fee,
            )?;
        }
        if contract.settlement.max_settlement_delay_micros < 0
            || contract.settlement.max_tick_gap_micros < 0
        {
            return Err(format!(
                "{}: delay and gap limits must not be negative",
                field("settlement")
            ));
        }
        contract
            .worst_loss()
            .map_err(|reason| format!("{}: {reason}", field("quoted_cost")))?;
    }
    unique(
        "risk_policies",
        replay.risk_policies.iter().map(|p| p.id.as_str()),
    )?;
    for (index, policy) in replay.risk_policies.iter().enumerate() {
        let field = |name: &str| format!("risk_policies[{index}].{name}");
        for (name, limit) in [
            ("max_open_per_strategy", policy.max_open_per_strategy),
            ("max_open_per_duration", policy.max_open_per_duration),
            ("max_open_per_instrument", policy.max_open_per_instrument),
            ("max_open_per_account", policy.max_open_per_account),
            ("max_open_total", policy.max_open_total),
        ] {
            if limit == Some(0) {
                return Err(format!("{}: must be positive when present", field(name)));
            }
        }
        if policy.max_feature_age_micros < 0 || policy.max_quote_age_micros < 0 {
            return Err(format!(
                "{}: ages must not be negative",
                field("max_feature_age_micros")
            ));
        }
        for (name, limit) in [
            (
                "max_unresolved_loss_per_account",
                policy.max_unresolved_loss_per_account,
            ),
            (
                "max_unresolved_loss_total",
                policy.max_unresolved_loss_total,
            ),
        ] {
            if let Some(limit) = limit {
                positive(&field(name), limit)?;
            }
        }
        if let Some(pause) = policy.pause {
            positive(&field("pause.drawdown"), pause.drawdown)?;
            if pause.duration_micros <= 0 {
                return Err(format!(
                    "{}: must be positive",
                    field("pause.duration_micros")
                ));
            }
        }
    }
    if replay.bindings.is_empty() {
        return Err("bindings: at least one binding is required".to_string());
    }
    unique("bindings", replay.bindings.iter().map(|b| b.id.as_str()))?;
    let mut identities: HashMap<(String, String), usize> = HashMap::new();
    let mut shared: HashMap<String, (usize, String)> = HashMap::new();
    for (index, binding) in replay.bindings.iter().enumerate() {
        let field = |name: &str| format!("bindings[{index}].{name}");
        let strategy = replay
            .strategies
            .iter()
            .find(|s| s.id == binding.strategy)
            .ok_or_else(|| {
                format!(
                    "{}: unknown strategy `{}`",
                    field("strategy"),
                    binding.strategy
                )
            })?;
        let account = replay
            .accounts
            .iter()
            .find(|a| a.id == binding.account)
            .ok_or_else(|| {
                format!(
                    "{}: unknown account `{}`",
                    field("account"),
                    binding.account
                )
            })?;
        let contract = replay
            .contracts
            .iter()
            .find(|c| c.id == binding.contract)
            .ok_or_else(|| {
                format!(
                    "{}: unknown contract `{}`",
                    field("contract"),
                    binding.contract
                )
            })?;
        let policy = replay
            .risk_policies
            .iter()
            .find(|p| p.id == binding.risk_policy)
            .ok_or_else(|| {
                format!(
                    "{}: unknown risk policy `{}`",
                    field("risk_policy"),
                    binding.risk_policy
                )
            })?;
        let (broker, _) = binding
            .instrument
            .split_once(':')
            .filter(|(broker, symbol)| !broker.is_empty() && !symbol.is_empty())
            .ok_or_else(|| {
                format!(
                    "{}: `{}` must be BROKER:PROVIDER_SYMBOL",
                    field("instrument"),
                    binding.instrument
                )
            })?;
        if broker != account.broker.as_str() {
            return Err(format!(
                "{}: instrument `{}` does not belong to account `{}` at broker `{}`",
                field("instrument"),
                binding.instrument,
                account.id,
                account.broker
            ));
        }
        if contract.currency != account.currency {
            return Err(format!(
                "{}: contract `{}` is quoted in {} but account `{}` holds {}",
                field("contract"),
                contract.id,
                contract.currency,
                account.id,
                account.currency
            ));
        }
        for (name, value) in [
            ("stake", contract.stake),
            ("quoted_cost", contract.quoted_cost),
            ("entry_fee", contract.entry_fee),
            ("win.gross_return", contract.win.gross_return),
            ("win.terminal_fee", contract.win.terminal_fee),
            ("loss.gross_return", contract.loss.gross_return),
            ("loss.terminal_fee", contract.loss.terminal_fee),
            ("tie.gross_return", contract.tie.gross_return),
            ("tie.terminal_fee", contract.tie.terminal_fee),
        ] {
            value.rescale(account.scale).map_err(|reason| {
                format!(
                    "{}: contract `{}` {name} {reason} of account `{}`",
                    field("contract"),
                    contract.id,
                    account.id
                )
            })?;
        }
        for (name, value) in [
            (
                "max_unresolved_loss_per_account",
                policy.max_unresolved_loss_per_account,
            ),
            ("pause.drawdown", policy.pause.map(|pause| pause.drawdown)),
        ] {
            if let Some(value) = value {
                value.rescale(account.scale).map_err(|reason| {
                    format!(
                        "{}: {name} {reason} of account `{}`",
                        field("risk_policy"),
                        account.id
                    )
                })?;
            }
        }
        let identity = deployment_identity(
            &signal_logic_identity(strategy),
            contract,
            &binding.envelope,
        );
        if let Some(earlier) = identities.insert((binding.account.clone(), identity), index) {
            return Err(format!(
                "{}: the same deployment strategy on account `{}` is already bound by bindings[{earlier}]",
                field("id"),
                binding.account
            ));
        }
        let scopes = [
            (
                format!("max_open_per_account of account {}", binding.account),
                format!("{:?}", policy.max_open_per_account),
            ),
            (
                format!(
                    "max_unresolved_loss_per_account of account {}",
                    binding.account
                ),
                format!(
                    "{:?}",
                    policy
                        .max_unresolved_loss_per_account
                        .map(Decimal::normalized)
                ),
            ),
            (
                format!("pause of account {}", binding.account),
                format!(
                    "{:?}",
                    policy
                        .pause
                        .map(|pause| (pause.drawdown.normalized(), pause.duration_micros))
                ),
            ),
            (
                format!(
                    "max_open_per_instrument of instrument {}",
                    binding.instrument
                ),
                format!("{:?}", policy.max_open_per_instrument),
            ),
            (
                format!(
                    "max_open_per_duration of duration {}",
                    contract.duration_micros
                ),
                format!("{:?}", policy.max_open_per_duration),
            ),
            (
                format!(
                    "same_entry of account {}, instrument {}, duration {}",
                    binding.account, binding.instrument, contract.duration_micros
                ),
                policy.same_entry.to_string(),
            ),
            (
                format!(
                    "deduplicate_signal_logic of account {}, instrument {}",
                    binding.account, binding.instrument
                ),
                policy.deduplicate_signal_logic.to_string(),
            ),
            (
                "max_open_total of the portfolio".to_string(),
                format!("{:?}", policy.max_open_total),
            ),
            (
                "max_unresolved_loss_total of the portfolio".to_string(),
                format!(
                    "{:?}",
                    policy.max_unresolved_loss_total.map(Decimal::normalized)
                ),
            ),
        ];
        for (scope, value) in scopes {
            match shared.get(&scope) {
                Some((owner, earlier)) if *earlier != value => {
                    return Err(format!(
                        "{}: {scope} is `{value}` here but `{earlier}` in bindings[{owner}]; a shared policy must be declared identically",
                        field("risk_policy")
                    ));
                }
                Some(_) => {}
                None => {
                    shared.insert(scope, (index, value));
                }
            }
        }
    }
    if replay.reporting_scale > MAX_SCALE {
        return Err(format!(
            "reporting_scale: {} exceeds {MAX_SCALE}",
            replay.reporting_scale
        ));
    }
    if replay.max_rate_age_micros < 0 {
        return Err("max_rate_age_micros: must not be negative".to_string());
    }
    if let Some(rates) = &replay.rates {
        unique("rates", rates.iter().map(|r| r.id.as_str()))?;
        for (index, rate) in rates.iter().enumerate() {
            let field = |name: &str| format!("rates[{index}].{name}");
            identifier(&field("provider"), &rate.provider)?;
            let provider_time = time(&field("provider_time"), &rate.provider_time)?;
            let available_at = time(&field("available_at"), &rate.available_at)?;
            if available_at < provider_time {
                return Err(format!(
                    "{}: availability precedes the provider time",
                    field("available_at")
                ));
            }
            positive(&field("rate"), rate.rate)?;
            if rate.source_currency == rate.reporting_currency {
                return Err(format!(
                    "{}: a same-currency rate is never supplied; same-currency amounts rescale",
                    field("source_currency")
                ));
            }
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------------------------
// Resolved run definition
// ---------------------------------------------------------------------------------------------

/// One column the adapter reads for a stream: the condition name it serves, the plan output it
/// is read from, the output's kind, and the fitted encoding when the name is a projection.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
pub struct ColumnSpec {
    pub name: String,
    pub source: String,
    pub kind: Kind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub encoding: Option<FittedEncoding>,
    /// The boolean columns of the same stream that must be true for the value to be ready.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub readiness: Vec<String>,
    /// The text values that mean the value is not ready.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub unready: Vec<String>,
}

/// The columns one stream must supply, in the order a row's values are indexed.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
pub struct StreamColumns {
    pub stream: StreamKey,
    pub columns: Vec<ColumnSpec>,
}

/// One instrument's bound inputs and frozen identities.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
pub struct InstrumentBinding {
    pub instrument: String,
    pub broker: BrokerId,
    pub provider_symbol: ProviderSymbol,
    pub price_scale: u8,
    pub tick_generation: String,
    pub feature_generation: String,
    pub plan_identity: String,
    pub raw_identity: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub outcome_generation: Option<String>,
    /// Streams in frozen-plan order, only those a strategy names.
    pub streams: Vec<StreamColumns>,
}

/// The complete resolved run: the validated table, its identities, and every instrument's bound
/// inputs. The first ledger record carries it, so restoration needs nothing else.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
pub struct RunDefinition {
    pub schema_version: u32,
    pub config_hash: String,
    pub code_revision: String,
    pub availability: String,
    pub replay: Replay,
    pub instruments: Vec<InstrumentBinding>,
}

/// The identity of a replay generation: the configuration identity and every bound input and
/// plan identity, under the engine definition's domain.
pub fn replay_generation_id(config_hash: &str, instruments: &[InstrumentBinding]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(REPLAY_GENERATION_DOMAIN_V1);
    hasher.update(config_hash.as_bytes());
    hasher.update(b"\n");
    for instrument in instruments {
        for line in [
            &instrument.instrument,
            &instrument.tick_generation,
            &instrument.feature_generation,
            &instrument.plan_identity,
            instrument.outcome_generation.as_deref().unwrap_or(""),
        ] {
            hasher.update(line.as_bytes());
            hasher.update(b"\n");
        }
    }
    crate::hex(&hasher.finalize())
}

// ---------------------------------------------------------------------------------------------
// Observations
// ---------------------------------------------------------------------------------------------

/// The provenance of one external event: its source identity, provider and availability clocks,
/// and whether it is a configured simulation rather than an observed broker message.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct EventSource {
    pub id: String,
    pub provider_time_micros: i64,
    pub available_at_micros: i64,
    pub simulated: bool,
}

/// What a reconciliation proves about a command.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(tag = "resolution", rename_all = "snake_case")]
pub enum Resolution {
    /// The broker never received the command: release everything.
    NotSent,
    /// The broker accepted it at these terms.
    Accepted {
        entry_time_micros: i64,
        entry_price_units: i64,
        price_time_micros: i64,
    },
    /// The broker settled it with this actual cashflow.
    Settled {
        outcome: Outcome,
        gross_return: Decimal,
        terminal_fee: Decimal,
    },
}

/// One observation in availability order.
#[derive(Debug, Clone, PartialEq)]
pub enum Observation {
    /// A provider tick of one instrument.
    Tick {
        instrument: usize,
        provider_time_micros: i64,
        price_units: i64,
    },
    /// One feature row of one instrument stream, indexed by the definition's stream order, with
    /// values in the stream's column order.
    Row {
        instrument: usize,
        stream: usize,
        close_time_micros: i64,
        known_at_micros: i64,
        values: Vec<Option<Value>>,
    },
    /// The broker acknowledged receipt of a sent command.
    Acknowledged {
        command: String,
        source: EventSource,
    },
    /// The broker (or the configured simulation) accepted a sent command.
    Accepted {
        command: String,
        source: EventSource,
        entry_time_micros: i64,
        entry_price_units: i64,
        price_time_micros: i64,
    },
    /// The broker rejected a sent command.
    Rejected {
        command: String,
        source: EventSource,
    },
    /// The adapter proved a command never left.
    NotSent {
        command: String,
        source: EventSource,
    },
    /// The adapter cannot say whether the command was sent.
    PossiblySent {
        command: String,
        source: EventSource,
    },
    /// An authoritative settlement with its actual cashflow.
    Settlement {
        command: String,
        source: EventSource,
        outcome: Outcome,
        gross_return: Decimal,
        terminal_fee: Decimal,
        settlement_price_units: i64,
    },
    /// An authoritative reconciliation.
    Reconciliation {
        command: String,
        source: EventSource,
        resolution: Resolution,
    },
}

/// The external identity of an event: its command and source identity, and the payload text
/// (the transition's fields and the source's clocks and simulation flag) that must repeat
/// exactly for a redelivery to be a no-op.
fn external_identity(command: &str, source: &EventSource, payload: String) -> (String, String) {
    (
        format!("{command}\n{}", source.id),
        format!(
            "{payload} {} {} {}",
            source.provider_time_micros, source.available_at_micros, source.simulated
        ),
    )
}

impl Observation {
    fn external(&self) -> Option<(String, String)> {
        match self {
            Self::Tick { .. } | Self::Row { .. } => None,
            Self::Acknowledged { command, source } => {
                Some(external_identity(command, source, "acknowledged".into()))
            }
            Self::Accepted {
                command,
                source,
                entry_time_micros,
                entry_price_units,
                price_time_micros,
            } => Some(external_identity(
                command,
                source,
                format!("accepted {entry_time_micros} {entry_price_units} {price_time_micros}"),
            )),
            Self::Rejected { command, source } => {
                Some(external_identity(command, source, "rejected".into()))
            }
            Self::NotSent { command, source } => {
                Some(external_identity(command, source, "not_sent".into()))
            }
            Self::PossiblySent { command, source } => {
                Some(external_identity(command, source, "possibly_sent".into()))
            }
            Self::Settlement {
                command,
                source,
                outcome,
                gross_return,
                terminal_fee,
                settlement_price_units,
            } => Some(external_identity(
                command,
                source,
                format!(
                    "settlement {outcome} {gross_return} {terminal_fee} {settlement_price_units}"
                ),
            )),
            Self::Reconciliation {
                command,
                source,
                resolution,
            } => Some(external_identity(
                command,
                source,
                format!("reconciliation {resolution:?}"),
            )),
        }
    }
}

// ---------------------------------------------------------------------------------------------
// Ledger events
// ---------------------------------------------------------------------------------------------

crate::string_enum! {
    /// Why a matching signal did or did not become a sent command, in check order.
    Disposition "disposition" {
        Admitted => "admitted",
        SameEntryDuplicate => "same_entry_duplicate",
        DuplicateLogic => "duplicate_logic",
        RepairBlocked => "repair_blocked",
        NoQuote => "no_quote",
        StaleFeature => "stale_feature",
        StaleQuote => "stale_quote",
        GapAtEntry => "gap_at_entry",
        AccountPaused => "account_paused",
        AccountBlocked => "account_blocked",
        QuoteRejected => "quote_rejected",
        CapacityStrategy => "capacity_strategy",
        CapacityDuration => "capacity_duration",
        CapacityInstrument => "capacity_instrument",
        CapacityAccount => "capacity_account",
        CapacityTotal => "capacity_total",
        InsufficientCash => "insufficient_cash",
        UnresolvedLossAccount => "unresolved_loss_account",
        UnresolvedLossTotal => "unresolved_loss_total",
        ConversionUnavailable => "conversion_unavailable",
    }
}

crate::string_enum! {
    /// Why the declared rule could not settle an obligation.
    UnresolvedReason "unresolved_reason" {
        Gap => "gap",
        LateSettlement => "late_settlement",
        WindowExhausted => "window_exhausted",
        PossiblySent => "possibly_sent",
    }
}

/// The directional path of one contract in integer price units: signed movement is
/// `(current - entry) × direction`; extrema keep the earliest event; extreme times start at entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
pub struct PathMetrics {
    pub final_move_units: i64,
    pub max_favorable_units: i64,
    pub max_adverse_units: i64,
    pub max_favorable_time_micros: i64,
    pub max_adverse_time_micros: i64,
    pub first_favorable_time_micros: Option<i64>,
    pub first_adverse_time_micros: Option<i64>,
    pub favorable_before_adverse: bool,
    pub adverse_before_favorable: bool,
}

impl PathMetrics {
    pub fn new(entry_time_micros: i64) -> Self {
        Self {
            final_move_units: 0,
            max_favorable_units: 0,
            max_adverse_units: 0,
            max_favorable_time_micros: entry_time_micros,
            max_adverse_time_micros: entry_time_micros,
            first_favorable_time_micros: None,
            first_adverse_time_micros: None,
            favorable_before_adverse: false,
            adverse_before_favorable: false,
        }
    }

    /// Observes one signed movement whose magnitude fits the signed range.
    pub fn observe(&mut self, time_micros: i64, move_units: i64) {
        self.final_move_units = move_units;
        if move_units > self.max_favorable_units {
            self.max_favorable_units = move_units;
            self.max_favorable_time_micros = time_micros;
        }
        if move_units < 0 && move_units.saturating_neg() > self.max_adverse_units {
            self.max_adverse_units = move_units.saturating_neg();
            self.max_adverse_time_micros = time_micros;
        }
        if move_units > 0 && self.first_favorable_time_micros.is_none() {
            self.first_favorable_time_micros = Some(time_micros);
        }
        if move_units < 0 && self.first_adverse_time_micros.is_none() {
            self.first_adverse_time_micros = Some(time_micros);
        }
        if let (Some(favorable), Some(adverse)) = (
            self.first_favorable_time_micros,
            self.first_adverse_time_micros,
        ) {
            self.favorable_before_adverse = favorable < adverse;
            self.adverse_before_favorable = adverse < favorable;
        }
    }
}

/// The signed movement of a price against an entry for a direction, rejecting a magnitude
/// outside the signed range.
fn signed_move(entry_price: i64, price_units: i64, direction: Direction) -> Result<i64, String> {
    price_units
        .checked_sub(entry_price)
        .and_then(|delta| delta.checked_mul(direction.sign()))
        .filter(|units| units.checked_neg().is_some())
        .ok_or_else(|| format!("the move from {entry_price} to {price_units} overflows"))
}

/// One canonical ledger record. `sequence` is contiguous from zero; `time_micros` is the engine
/// decision time the record was applied at.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
pub struct FinancialEvent {
    pub sequence: u64,
    pub time_micros: i64,
    #[serde(flatten)]
    pub kind: EventKind,
}

/// The tagged transition of one ledger record.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum EventKind {
    /// The complete resolved run, frozen identities, and initial state.
    RunDefinition { definition: Box<RunDefinition> },
    /// One matching signal and its disposition; an admitted signal reserves and sends.
    Signal {
        instrument: String,
        binding: String,
        deployment_identity: String,
        signal_logic_identity: String,
        stream: StreamKey,
        close_time_micros: i64,
        known_at_micros: i64,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        split: Option<String>,
        disposition: Disposition,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        quote_price_units: Option<i64>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        quote_time_micros: Option<i64>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        command: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        reservation: Option<Decimal>,
        /// The rate identities a conversion used during admission.
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        rates: Vec<String>,
    },
    /// The broker acknowledged receipt of a sent command; nothing is posted.
    Acknowledged {
        command: String,
        source: EventSource,
    },
    /// A sent command was accepted: the purchase is debited once and only the terminal reserve
    /// stays reserved.
    Accepted {
        command: String,
        source: EventSource,
        entry_time_micros: i64,
        entry_price_units: i64,
        price_time_micros: i64,
        due_time_micros: i64,
        debit: Decimal,
        reservation: Decimal,
    },
    /// A sent command was refused or proved never sent: everything is released without debit.
    Released {
        command: String,
        source: EventSource,
        rejected: bool,
        release: Decimal,
    },
    /// The command may or may not have been sent: the reservation stays and the account blocks.
    PossiblySent {
        command: String,
        source: EventSource,
    },
    /// A confirmed terminal settlement with its actual cashflow and path.
    Settled {
        command: String,
        source: EventSource,
        settlement_time_micros: i64,
        settlement_price_units: i64,
        outcome: Outcome,
        gross_return: Decimal,
        terminal_fee: Decimal,
        credit: Decimal,
        profit: Decimal,
        release: Decimal,
        discrepancy: bool,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        deficit: Option<Decimal>,
        path: PathMetrics,
    },
    /// The declared rule could not settle the obligation; it stays open with its evidence.
    Unresolved {
        command: String,
        reason: UnresolvedReason,
        evidence: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        path: Option<PathMetrics>,
    },
    /// An authoritative reconciliation resolved an open command, or lifted the block a settled
    /// discrepancy left on its account.
    Reconciled {
        command: String,
        source: EventSource,
        resolution: Resolution,
        release: Decimal,
        debit: Decimal,
        credit: Decimal,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        profit: Option<Decimal>,
    },
    /// A supplied conversion rate became available: the portfolio projection is observed so a
    /// rate change is visible without an account posting.
    RateAvailable { rate: String },
    /// An account pause began at the epoch drawdown.
    PauseStarted {
        account: String,
        until_micros: i64,
        drawdown: Decimal,
    },
    /// An account pause ended and the epoch peak reset.
    PauseEnded { account: String },
}

impl EventKind {
    /// The external identity a record establishes, so a redelivered observation is a no-op after
    /// generation and after restoration alike.
    fn external(&self) -> Option<(String, String)> {
        match self {
            Self::Acknowledged { command, source } => {
                Some(external_identity(command, source, "acknowledged".into()))
            }
            Self::Accepted {
                command,
                source,
                entry_time_micros,
                entry_price_units,
                price_time_micros,
                ..
            } => Some(external_identity(
                command,
                source,
                format!("accepted {entry_time_micros} {entry_price_units} {price_time_micros}"),
            )),
            Self::Released {
                command,
                source,
                rejected,
                ..
            } => Some(external_identity(
                command,
                source,
                if *rejected { "rejected" } else { "not_sent" }.into(),
            )),
            Self::PossiblySent { command, source } => {
                Some(external_identity(command, source, "possibly_sent".into()))
            }
            Self::Settled {
                command,
                source,
                outcome,
                gross_return,
                terminal_fee,
                settlement_price_units,
                ..
            } => Some(external_identity(
                command,
                source,
                format!(
                    "settlement {outcome} {gross_return} {terminal_fee} {settlement_price_units}"
                ),
            )),
            Self::Reconciled {
                command,
                source,
                resolution,
                ..
            } => Some(external_identity(
                command,
                source,
                format!("reconciliation {resolution:?}"),
            )),
            _ => None,
        }
    }

    /// The external source a record carries.
    fn source(&self) -> Option<&EventSource> {
        match self {
            Self::Acknowledged { source, .. }
            | Self::Accepted { source, .. }
            | Self::Released { source, .. }
            | Self::PossiblySent { source, .. }
            | Self::Settled { source, .. }
            | Self::Reconciled { source, .. } => Some(source),
            _ => None,
        }
    }

    /// Whether the record changes an account's postings or a rate, so the portfolio is
    /// observed.
    fn observes_portfolio(&self) -> bool {
        match self {
            Self::RunDefinition { .. }
            | Self::Accepted { .. }
            | Self::Released { .. }
            | Self::Settled { .. }
            | Self::Reconciled { .. }
            | Self::RateAvailable { .. } => true,
            Self::Signal { disposition, .. } => *disposition == Disposition::Admitted,
            _ => false,
        }
    }
}

impl FinancialEvent {
    /// One canonical line: compact JSON and one line feed.
    pub fn to_line(&self) -> Vec<u8> {
        let mut bytes = serde_json::to_vec(self).expect("an event serializes");
        bytes.push(b'\n');
        bytes
    }

    pub fn from_line(line: &[u8]) -> Result<Self, String> {
        serde_json::from_slice(line).map_err(|error| error.to_string())
    }
}

// ---------------------------------------------------------------------------------------------
// Engine state
// ---------------------------------------------------------------------------------------------

#[derive(Debug, Clone)]
struct CompiledCondition {
    stream: usize,
    column: usize,
    /// Readiness flag columns of the stream; a false or unavailable flag fails the condition.
    readiness: Vec<usize>,
    comparator: Comparator,
    threshold: Threshold,
}

#[derive(Debug, Clone)]
struct CompiledStrategy {
    instrument: usize,
    base_stream: usize,
    conditions: Vec<CompiledCondition>,
    repair: Vec<CompiledCondition>,
    logic: String,
}

#[derive(Debug, Clone)]
struct CompiledBinding {
    id: String,
    strategy: usize,
    account: usize,
    instrument: usize,
    contract: usize,
    policy: usize,
    identity: String,
}

#[derive(Debug, Clone, Copy)]
struct TickState {
    provider_time_micros: i64,
    price_units: i64,
    /// The inter-arrival from the previous tick, when there was one.
    gap_micros: Option<i64>,
}

#[derive(Debug, Clone, PartialEq)]
struct RowState {
    close_time_micros: i64,
    known_at_micros: i64,
    values: Vec<Option<Value>>,
}

#[derive(Debug, Default)]
struct InstrumentState {
    tick: Option<TickState>,
    rows: Vec<Option<RowState>>,
    /// Open accepted obligations whose path and settlement this instrument's ticks drive, in
    /// acceptance order.
    tracked: Vec<String>,
}

/// The financial state of one account, every value at the account scale.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct AccountState {
    pub id: String,
    pub currency: Currency,
    pub scale: u8,
    pub cash: Decimal,
    pub reserved: Decimal,
    pub paid_basis: Decimal,
    pub unresolved_loss: Decimal,
    pub completed_profit: Decimal,
    pub epoch_peak: Decimal,
    pub lifetime_peak: Decimal,
    pub max_drawdown: Decimal,
    pub open: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub paused_until_micros: Option<i64>,
    /// The commands whose reconciliation the account waits for, with why: possibly sent, or
    /// settled with a discrepancy or deficit at the booked cashflow. New entries are blocked
    /// while any remains.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub blocked: BTreeMap<String, Block>,
}

/// Why an account waits for a command's reconciliation.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(tag = "reason", rename_all = "snake_case")]
pub enum Block {
    PossiblySent,
    /// The terminal cashflow booked at settlement; only a reconciliation that states the same
    /// settlement lifts the block, since no corrective posting exists.
    Settled {
        outcome: Outcome,
        gross_return: Decimal,
        terminal_fee: Decimal,
    },
}

impl AccountState {
    /// Native cash plus the paid basis of open contracts.
    pub fn settled_equity(&self) -> Result<Decimal, String> {
        self.cash.checked_add(self.paid_basis)
    }

    fn available(&self) -> Result<Decimal, String> {
        self.cash.checked_sub(self.reserved)
    }

    /// Posts a completed profit and advances the peaks and lifetime drawdown.
    fn complete(&mut self, profit: Decimal) -> Result<(), String> {
        self.completed_profit = self.completed_profit.checked_add(profit)?;
        self.epoch_peak = self.epoch_peak.max(self.completed_profit)?;
        self.lifetime_peak = self.lifetime_peak.max(self.completed_profit)?;
        self.max_drawdown = self
            .max_drawdown
            .max(self.lifetime_peak.checked_sub(self.completed_profit)?)?;
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
enum ObligationState {
    Sent,
    Acknowledged,
    Accepted,
    PossiblySent,
}

impl ObligationState {
    /// Whether the command is still with the broker without a proved acceptance.
    fn unaccepted(self) -> bool {
        matches!(self, Self::Sent | Self::Acknowledged)
    }
}

/// One open obligation's financial record.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
struct Obligation {
    binding: usize,
    state: ObligationState,
    /// Unpaid reservation.
    reservation: Decimal,
    paid_basis: Decimal,
    worst_loss: Decimal,
    split: Option<String>,
    /// The decision time the command was dispatched at.
    sent_micros: i64,
    entry_time_micros: Option<i64>,
    entry_price_units: Option<i64>,
    due_time_micros: Option<i64>,
    unresolved: Option<UnresolvedReason>,
    #[serde(skip)]
    path: Option<PathMetrics>,
    /// The path the ledger last recorded for the obligation (empty at acceptance, then its
    /// unresolved record), the base of an authoritative settlement's path in the live and the
    /// restored engine alike.
    #[serde(skip)]
    recorded_path: Option<PathMetrics>,
    /// The latest tick time the obligation has continuity evidence from: the quote tick at
    /// acceptance, then every tick it observed. Not ledger state, so a restored engine starts
    /// again from the quote tick.
    #[serde(skip)]
    continuity_micros: Option<i64>,
}

/// The exact postings of one terminal settlement.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SettlementPostings {
    credit: Decimal,
    profit: Decimal,
    release: Decimal,
    discrepancy: bool,
    deficit: Option<Decimal>,
}

/// The exact postings of one reconciliation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ReconciliationPostings {
    release: Decimal,
    debit: Decimal,
    credit: Decimal,
    profit: Option<Decimal>,
}

/// Signal dispositions, outcomes, open obligations, and completed profit by currency of one
/// group; a currency's profit is unavailable once its total exceeds the representable range.
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize, Serialize)]
pub struct Group {
    pub signals: u64,
    pub dispositions: BTreeMap<String, u64>,
    pub accepted: u64,
    pub released: u64,
    pub settled: u64,
    pub wins: u64,
    pub losses: u64,
    pub ties: u64,
    pub unresolved: u64,
    pub open: u64,
    pub profit: BTreeMap<String, Option<Decimal>>,
}

impl Group {
    fn add_profit(&mut self, currency: &Currency, profit: Decimal) {
        let entry = self
            .profit
            .entry(currency.to_string())
            .or_insert_with(|| Some(Decimal::zero(profit.scale())));
        *entry = entry.and_then(|total| total.checked_add(profit).ok());
    }

    fn close(&mut self, unresolved: bool) {
        self.open -= 1;
        if unresolved {
            self.unresolved -= 1;
        }
    }

    fn outcome(&mut self, outcome: Outcome) {
        self.settled += 1;
        match outcome {
            Outcome::Win => self.wins += 1,
            Outcome::Loss => self.losses += 1,
            Outcome::Tie => self.ties += 1,
        }
    }
}

/// The reporting-currency projection of the portfolio and its converted drawdown, observed at
/// the start and after every record that changes an account.
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize, Serialize)]
pub struct Reporting {
    pub currency: String,
    pub scale: u8,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub settled_equity: Option<Decimal>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub unresolved_loss: Option<Decimal>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub peak_equity: Option<Decimal>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_drawdown: Option<Decimal>,
    pub observations: u64,
    pub unavailable_observations: u64,
    pub used_rates: BTreeSet<String>,
}

/// Every summary projection, grouped only by portfolio, strategy, duration, instrument, and
/// declared split. Reports are projections; the ledger is the owner.
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize, Serialize)]
pub struct Summary {
    pub events: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_time_micros: Option<i64>,
    pub accounts: Vec<AccountState>,
    pub portfolio: Group,
    pub strategies: BTreeMap<String, Group>,
    pub durations: BTreeMap<String, Group>,
    pub instruments: BTreeMap<String, Group>,
    pub splits: BTreeMap<String, Group>,
    pub reporting: Reporting,
}

impl Summary {
    /// The exact bytes published as `summary.json`.
    pub fn to_json(&self) -> Vec<u8> {
        let mut bytes = serde_json::to_vec_pretty(self).expect("a summary serializes");
        bytes.push(b'\n');
        bytes
    }

    pub fn from_json(bytes: &[u8]) -> Result<Self, String> {
        serde_json::from_slice(bytes).map_err(|error| error.to_string())
    }

    pub fn identity(&self) -> String {
        let mut hasher = Sha256::new();
        hasher.update(SUMMARY_DOMAIN_V1);
        hasher.update(self.to_json());
        crate::hex(&hasher.finalize())
    }
}

/// The result of one conversion.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Converted {
    pub amount: Decimal,
    pub rate: Option<String>,
}

#[derive(Debug, Clone)]
struct Rate {
    id: String,
    source: Currency,
    reporting: Currency,
    provider_time_micros: i64,
    available_at_micros: i64,
    rate: Decimal,
}

/// Converts `amount` from `source` into `reporting` units at `scale` as of `at`: same-currency
/// amounts rescale exactly; otherwise the latest supplied rate whose provider and availability
/// times are no later than `at` and whose provider age is at most `max_age` multiplies once.
fn convert(
    amount: Decimal,
    source: &Currency,
    reporting: &Currency,
    scale: u8,
    at: i64,
    max_age: i64,
    rates: &[Rate],
) -> Result<Converted, String> {
    if source == reporting {
        return Ok(Converted {
            amount: amount.rescale(scale)?,
            rate: None,
        });
    }
    let rate = rates
        .iter()
        .filter(|rate| {
            rate.source == *source
                && rate.reporting == *reporting
                && rate.provider_time_micros <= at
                && rate.available_at_micros <= at
                && at - rate.provider_time_micros <= max_age
        })
        .max_by_key(|rate| rate.provider_time_micros)
        .ok_or_else(|| {
            format!(
                "no {source} to {reporting} rate is available at {} within {max_age} microseconds",
                format_event_time_micros(at)
            )
        })?;
    Ok(Converted {
        amount: amount.checked_mul(rate.rate)?.rescale(scale)?,
        rate: Some(rate.id.clone()),
    })
}

/// The group keys of one binding's records.
#[derive(Debug, Clone)]
struct GroupKey {
    binding: String,
    duration: i64,
    instrument: String,
    split: Option<String>,
}

/// The one chronological and financial owner.
pub struct Engine {
    definition: RunDefinition,
    decision_start: i64,
    decision_end: i64,
    splits: Vec<(String, i64, i64)>,
    /// Supplied rates in availability order; `next_rate` is the first not yet observed.
    rates: Vec<Rate>,
    next_rate: usize,
    strategies: Vec<CompiledStrategy>,
    bindings: Vec<CompiledBinding>,
    /// Binding indices per instrument and base stream, in configured order.
    by_base: HashMap<(usize, usize), Vec<usize>>,
    binding_index: HashMap<String, usize>,
    instruments: Vec<InstrumentState>,
    accounts: Vec<AccountState>,
    obligations: BTreeMap<String, Obligation>,
    open_by_binding: HashMap<usize, u32>,
    open_by_duration: HashMap<i64, u32>,
    open_by_instrument: HashMap<usize, u32>,
    open_total: u32,
    /// External identities already applied, with their payload text.
    externals: HashMap<String, String>,
    sequence: u64,
    now: i64,
    /// The selection and deduplication slots claimed at `slot_time`, rebuilt from signal
    /// records so a restored engine keeps them.
    same_entry: HashSet<(usize, usize, i64)>,
    logic_seen: HashSet<(usize, usize, String)>,
    slot_time: i64,
    /// The latest base close time each binding decided, so a row redelivered after
    /// restoration is not decided twice.
    decided: Vec<Option<i64>>,
    /// The account whose closure just made its pause due; the next record must start it.
    pause_pending: Option<(usize, i64)>,
    summary: Summary,
    events: Vec<FinancialEvent>,
    /// Set by a failed step: the engine's state is no longer known to match its ledger.
    failed: bool,
}

impl Engine {
    /// Compiles the definition against its own column lists and starts from the accounts'
    /// initial cash with no obligations. The definition record is the first ledger record.
    pub fn new(definition: RunDefinition) -> Result<Self, String> {
        let replay = &definition.replay;
        validate(replay)?;
        if definition.schema_version != REPLAY_SCHEMA_VERSION {
            return Err(format!(
                "unsupported definition schema_version {}, expected {REPLAY_SCHEMA_VERSION}",
                definition.schema_version
            ));
        }
        if definition.instruments.len() != replay.inputs.len() {
            return Err(
                "the definition binds a different number of instruments than inputs".to_string(),
            );
        }
        let decision_start = parse_event_time_micros(&replay.decision_start)?;
        let decision_end = parse_event_time_micros(&replay.decision_end)?;
        let splits = replay
            .splits
            .iter()
            .flatten()
            .map(|split| {
                Ok((
                    split.name.clone(),
                    parse_event_time_micros(&split.start)?,
                    parse_event_time_micros(&split.end)?,
                ))
            })
            .collect::<Result<Vec<_>, String>>()?;
        let mut rates = replay
            .rates
            .iter()
            .flatten()
            .map(|rate| {
                Ok(Rate {
                    id: rate.id.clone(),
                    source: rate.source_currency.clone(),
                    reporting: rate.reporting_currency.clone(),
                    provider_time_micros: parse_event_time_micros(&rate.provider_time)?,
                    available_at_micros: parse_event_time_micros(&rate.available_at)?,
                    rate: rate.rate,
                })
            })
            .collect::<Result<Vec<_>, String>>()?;
        rates.sort_by(|a, b| (a.available_at_micros, &a.id).cmp(&(b.available_at_micros, &b.id)));
        // Strategies bind through their plan identity to exactly one instrument input.
        let mut strategies = Vec::with_capacity(replay.strategies.len());
        for (index, strategy) in replay.strategies.iter().enumerate() {
            let field = |name: &str| format!("strategies[{index}].{name}");
            let instrument = definition
                .instruments
                .iter()
                .position(|instrument| instrument.plan_identity == strategy.plan_identity)
                .ok_or_else(|| {
                    format!(
                        "{}: no replay input carries frozen plan {}",
                        field("plan_identity"),
                        strategy.plan_identity
                    )
                })?;
            let bound = &definition.instruments[instrument];
            let stream_index = |key: StreamKey, what: &str| -> Result<usize, String> {
                bound
                    .streams
                    .iter()
                    .position(|stream| stream.stream == key)
                    .ok_or_else(|| {
                        format!("{what}: stream {key} is not bound for {}", bound.instrument)
                    })
            };
            let base_stream = stream_index(strategy.base_stream, &field("base_stream"))?;
            let compile = |name: &str,
                           conditions: &[Condition]|
             -> Result<Vec<CompiledCondition>, String> {
                conditions
                    .iter()
                    .enumerate()
                    .map(|(position, condition)| {
                        let field = |part: &str| format!("{}[{position}].{part}", field(name));
                        let stream = stream_index(condition.stream, &field("stream"))?;
                        let columns = &bound.streams[stream].columns;
                        let column = columns
                            .iter()
                            .position(|column| column.name == condition.output)
                            .ok_or_else(|| {
                                format!(
                                    "{}: `{}` is not a compiled output or fitted encoding of stream {}",
                                    field("output"),
                                    condition.output,
                                    condition.stream
                                )
                            })?;
                        let spec = &columns[column];
                        let kind = if spec.encoding.is_some() {
                            Kind::Text
                        } else {
                            spec.kind
                        };
                        let compatible = matches!(
                            (&condition.threshold, kind),
                            (Threshold::Text(_), Kind::Text)
                                | (Threshold::Bool(_), Kind::Bool)
                                | (Threshold::Number(_), Kind::Int | Kind::Float | Kind::Time)
                        );
                        if !compatible {
                            return Err(format!(
                                "{}: `{}` is a {kind} output; the threshold type does not match",
                                field("threshold"),
                                condition.output
                            ));
                        }
                        let readiness = spec
                            .readiness
                            .iter()
                            .map(|flag| {
                                columns
                                    .iter()
                                    .position(|column| {
                                        column.name == *flag && column.kind == Kind::Bool
                                    })
                                    .ok_or_else(|| {
                                        format!(
                                            "{}: readiness flag `{flag}` of `{}` is not a boolean column of stream {}",
                                            field("output"),
                                            condition.output,
                                            condition.stream
                                        )
                                    })
                            })
                            .collect::<Result<Vec<_>, String>>()?;
                        Ok(CompiledCondition {
                            stream,
                            column,
                            readiness,
                            comparator: condition.comparator,
                            threshold: condition.threshold.clone(),
                        })
                    })
                    .collect()
            };
            strategies.push(CompiledStrategy {
                instrument,
                base_stream,
                conditions: compile("conditions", &strategy.conditions)?,
                repair: compile("repair", &strategy.repair)?,
                logic: signal_logic_identity(strategy),
            });
        }
        let mut bindings = Vec::with_capacity(replay.bindings.len());
        let mut by_base: HashMap<(usize, usize), Vec<usize>> = HashMap::new();
        let mut binding_index = HashMap::new();
        for (index, binding) in replay.bindings.iter().enumerate() {
            let strategy = replay
                .strategies
                .iter()
                .position(|s| s.id == binding.strategy)
                .expect("validated");
            let instrument = definition
                .instruments
                .iter()
                .position(|instrument| instrument.instrument == binding.instrument)
                .ok_or_else(|| {
                    format!(
                        "bindings[{index}].instrument: `{}` is not a replay input",
                        binding.instrument
                    )
                })?;
            if strategies[strategy].instrument != instrument {
                return Err(format!(
                    "bindings[{index}].strategy: strategy `{}` is frozen on plan {}, which is not the plan of instrument `{}`",
                    binding.strategy, replay.strategies[strategy].plan_identity, binding.instrument
                ));
            }
            let contract = replay
                .contracts
                .iter()
                .position(|c| c.id == binding.contract)
                .expect("validated");
            by_base
                .entry((instrument, strategies[strategy].base_stream))
                .or_default()
                .push(index);
            binding_index.insert(binding.id.clone(), index);
            bindings.push(CompiledBinding {
                id: binding.id.clone(),
                strategy,
                account: replay
                    .accounts
                    .iter()
                    .position(|a| a.id == binding.account)
                    .expect("validated"),
                instrument,
                contract,
                policy: replay
                    .risk_policies
                    .iter()
                    .position(|p| p.id == binding.risk_policy)
                    .expect("validated"),
                identity: deployment_identity(
                    &strategies[strategy].logic,
                    &replay.contracts[contract],
                    &binding.envelope,
                ),
            });
        }
        let accounts = replay
            .accounts
            .iter()
            .map(|account| {
                let zero = Decimal::zero(account.scale);
                Ok(AccountState {
                    id: account.id.clone(),
                    currency: account.currency.clone(),
                    scale: account.scale,
                    cash: account.initial_cash.rescale(account.scale)?,
                    reserved: zero,
                    paid_basis: zero,
                    unresolved_loss: zero,
                    completed_profit: zero,
                    epoch_peak: zero,
                    lifetime_peak: zero,
                    max_drawdown: zero,
                    open: 0,
                    paused_until_micros: None,
                    blocked: BTreeMap::new(),
                })
            })
            .collect::<Result<Vec<_>, String>>()?;
        let instruments = definition
            .instruments
            .iter()
            .map(|instrument| InstrumentState {
                tick: None,
                rows: vec![None; instrument.streams.len()],
                tracked: Vec::new(),
            })
            .collect();
        let summary = Summary {
            reporting: Reporting {
                currency: replay.reporting_currency.to_string(),
                scale: replay.reporting_scale,
                ..Reporting::default()
            },
            ..Summary::default()
        };
        let mut engine = Self {
            decision_start,
            decision_end,
            splits,
            rates,
            next_rate: 0,
            strategies,
            bindings,
            by_base,
            binding_index,
            instruments,
            accounts,
            obligations: BTreeMap::new(),
            open_by_binding: HashMap::new(),
            open_by_duration: HashMap::new(),
            open_by_instrument: HashMap::new(),
            open_total: 0,
            externals: HashMap::new(),
            sequence: 0,
            now: decision_start,
            same_entry: HashSet::new(),
            logic_seen: HashSet::new(),
            slot_time: i64::MIN,
            decided: vec![None; replay.bindings.len()],
            pause_pending: None,
            summary,
            events: Vec::new(),
            failed: false,
            definition,
        };
        let definition = Box::new(engine.definition.clone());
        engine.emit(decision_start, EventKind::RunDefinition { definition })?;
        engine.now = i64::MIN;
        Ok(engine)
    }

    /// Restores an engine from a complete ledger, applying every record through the same
    /// function that generated it. Sequences must be contiguous from zero and the first record
    /// must be the definition.
    pub fn restore(lines: impl Iterator<Item = Result<Vec<u8>, String>>) -> Result<Self, String> {
        let mut engine: Option<Self> = None;
        for (expected, line) in lines.enumerate() {
            let event = FinancialEvent::from_line(&line?)
                .map_err(|reason| format!("ledger record {expected}: {reason}"))?;
            if event.sequence != expected as u64 {
                return Err(format!(
                    "ledger record {expected} carries sequence {}",
                    event.sequence
                ));
            }
            match (&mut engine, event.kind) {
                (None, EventKind::RunDefinition { definition }) => {
                    let mut restored = Self::new(*definition)?;
                    if restored.events[0].time_micros != event.time_micros {
                        return Err("the definition record does not reproduce".to_string());
                    }
                    restored.events.clear();
                    engine = Some(restored);
                }
                (None, _) => {
                    return Err("the first ledger record is not the definition".to_string());
                }
                (Some(_), EventKind::RunDefinition { .. }) => {
                    return Err(format!("ledger record {expected} repeats the definition"));
                }
                (Some(engine), kind) => {
                    if event.time_micros < engine.now {
                        return Err(format!("ledger record {expected} goes back in time"));
                    }
                    engine.now = event.time_micros;
                    engine.emit(event.time_micros, kind)?;
                    engine.events.clear();
                }
            }
        }
        let engine = engine.ok_or_else(|| "the ledger is empty".to_string())?;
        if let Some((pending, _)) = engine.pause_pending {
            return Err(format!(
                "the ledger ends while account `{}` still requires its pause record",
                engine.accounts[pending].id
            ));
        }
        if let Some(account) = engine.accounts.iter().find(|account| {
            account
                .paused_until_micros
                .is_some_and(|until| until <= engine.now)
        }) {
            return Err(format!(
                "the ledger ends while account `{}` has an expired pause",
                account.id
            ));
        }
        Ok(engine)
    }

    pub fn definition(&self) -> &RunDefinition {
        &self.definition
    }

    /// The records generated since the last drain, in sequence order.
    pub fn drain(&mut self) -> Vec<FinancialEvent> {
        std::mem::take(&mut self.events)
    }

    pub fn sequence(&self) -> u64 {
        self.sequence
    }

    pub fn summary(&self) -> &Summary {
        &self.summary
    }

    pub fn accounts(&self) -> &[AccountState] {
        &self.accounts
    }

    /// The identity of every financial state a ledger reconstructs: accounts, open obligations,
    /// capacity, and the sequence.
    pub fn state_identity(&self) -> String {
        #[derive(Serialize)]
        struct State<'a> {
            sequence: u64,
            accounts: &'a [AccountState],
            obligations: &'a BTreeMap<String, Obligation>,
            open_total: u32,
        }
        let state = State {
            sequence: self.sequence,
            accounts: &self.accounts,
            obligations: &self.obligations,
            open_total: self.open_total,
        };
        let mut hasher = Sha256::new();
        hasher.update(STATE_DOMAIN_V1);
        hasher.update(serde_json::to_vec(&state).expect("state serializes"));
        crate::hex(&hasher.finalize())
    }

    /// Converts one amount for reporting or portfolio risk as of the current decision time.
    pub fn convert(&self, amount: Decimal, source: &Currency) -> Result<Converted, String> {
        convert(
            amount,
            source,
            &self.definition.replay.reporting_currency,
            self.definition.replay.reporting_scale,
            self.now,
            self.definition.replay.max_rate_age_micros,
            &self.rates,
        )
    }

    fn split_of(&self, time_micros: i64) -> Option<String> {
        self.splits
            .iter()
            .find(|(_, start, end)| *start <= time_micros && time_micros < *end)
            .map(|(name, _, _)| name.clone())
    }

    fn keys(&self, binding: usize, split: Option<&str>) -> GroupKey {
        let compiled = &self.bindings[binding];
        GroupKey {
            binding: compiled.id.clone(),
            duration: self.definition.replay.contracts[compiled.contract].duration_micros,
            instrument: self.definition.instruments[compiled.instrument]
                .instrument
                .clone(),
            split: split.map(str::to_string),
        }
    }

    /// The contract, account scale, and account of an open obligation.
    fn terms(&self, command: &str) -> Result<(&ContractTerms, u8, usize), String> {
        let obligation = self
            .obligations
            .get(command)
            .ok_or_else(|| format!("{command} is not an open obligation"))?;
        let binding = &self.bindings[obligation.binding];
        Ok((
            &self.definition.replay.contracts[binding.contract],
            self.accounts[binding.account].scale,
            binding.account,
        ))
    }

    // ------------------------------------------------------------------------------------------
    // Stepping: observations, then decisions, at one availability time
    // ------------------------------------------------------------------------------------------

    /// Applies every observation available at `time` in source order, then evaluates the base
    /// rows installed by them at decision time `time`. Times never decrease. A failed step
    /// leaves the engine unusable: its state may hold observations the ledger does not, so the
    /// caller restores from the ledger instead of retrying.
    pub fn step(&mut self, time: i64, observations: Vec<Observation>) -> Result<(), String> {
        if self.failed {
            return Err(
                "the engine failed an earlier step; restore it from its ledger".to_string(),
            );
        }
        let result = self.apply_step(time, observations);
        if result.is_err() {
            self.failed = true;
        }
        result
    }

    fn apply_step(&mut self, time: i64, observations: Vec<Observation>) -> Result<(), String> {
        if time < self.now {
            return Err(format!(
                "observations at {} arrive after the engine reached {}",
                format_event_time_micros(time),
                format_event_time_micros(self.now)
            ));
        }
        self.observe_rates(time)?;
        self.now = time;
        self.advance_pauses()?;
        let mut installed: Vec<(usize, usize)> = Vec::new();
        for observation in observations {
            if let Some((key, payload)) = observation.external() {
                match self.externals.get(&key) {
                    Some(seen) if *seen == payload => continue,
                    Some(_) => {
                        return Err(format!(
                            "external event `{key}` arrived again with a different payload; reconciliation failed"
                        ));
                    }
                    None => {}
                }
            }
            match observation {
                Observation::Tick {
                    instrument,
                    provider_time_micros,
                    price_units,
                } => self.observe_tick(instrument, provider_time_micros, price_units)?,
                Observation::Row {
                    instrument,
                    stream,
                    close_time_micros,
                    known_at_micros,
                    values,
                } => {
                    if known_at_micros > time || close_time_micros > known_at_micros {
                        return Err(format!(
                            "a row closing at {} known at {} is not available at {}",
                            format_event_time_micros(close_time_micros),
                            format_event_time_micros(known_at_micros),
                            format_event_time_micros(time)
                        ));
                    }
                    let row = RowState {
                        close_time_micros,
                        known_at_micros,
                        values,
                    };
                    let state = self
                        .instruments
                        .get_mut(instrument)
                        .and_then(|state| state.rows.get_mut(stream))
                        .ok_or_else(|| {
                            format!("no bound stream {stream} of instrument {instrument}")
                        })?;
                    match state {
                        Some(current) if *current == row => continue,
                        Some(current) if current.close_time_micros >= row.close_time_micros => {
                            return Err(format!(
                                "a row of stream {stream} closing at {} arrives after the row closing at {}",
                                format_event_time_micros(row.close_time_micros),
                                format_event_time_micros(current.close_time_micros)
                            ));
                        }
                        _ if installed.contains(&(instrument, stream)) => {
                            return Err(format!(
                                "two rows of stream {stream} of instrument {instrument} at one availability time"
                            ));
                        }
                        _ => {}
                    }
                    *state = Some(row);
                    installed.push((instrument, stream));
                }
                Observation::Acknowledged { command, source } => {
                    self.emit(time, EventKind::Acknowledged { command, source })?;
                }
                Observation::Accepted {
                    command,
                    source,
                    entry_time_micros,
                    entry_price_units,
                    price_time_micros,
                } => self.observe_accepted(
                    command,
                    source,
                    entry_time_micros,
                    entry_price_units,
                    price_time_micros,
                )?,
                Observation::Rejected { command, source } => {
                    self.observe_release(command, source, true)?;
                }
                Observation::NotSent { command, source } => {
                    self.observe_release(command, source, false)?;
                }
                Observation::PossiblySent { command, source } => {
                    self.emit(time, EventKind::PossiblySent { command, source })?;
                }
                Observation::Settlement {
                    command,
                    source,
                    outcome,
                    gross_return,
                    terminal_fee,
                    settlement_price_units,
                } => {
                    self.require(&command, ObligationState::Accepted)?;
                    let recorded = self.obligations[&command]
                        .recorded_path
                        .ok_or_else(|| format!("{command} has no recorded path"))?;
                    self.settle(
                        &command,
                        source,
                        settlement_price_units,
                        outcome,
                        gross_return,
                        terminal_fee,
                        recorded,
                    )?;
                }
                Observation::Reconciliation {
                    command,
                    source,
                    resolution,
                } => self.observe_reconciliation(command, source, resolution)?,
            }
        }
        // Installed base streams in frozen-plan order, then their bindings in configured order.
        installed.sort_unstable();
        for (instrument, stream) in installed {
            let Some(bindings) = self.by_base.get(&(instrument, stream)).cloned() else {
                continue;
            };
            for binding in bindings {
                self.evaluate(binding)?;
            }
        }
        Ok(())
    }

    /// Observes each rate available by `until` at its own availability time, so every valuation
    /// between market observations is recorded.
    fn observe_rates(&mut self, until: i64) -> Result<(), String> {
        while let Some(rate) = self.rates.get(self.next_rate)
            && rate.available_at_micros <= until
        {
            let at = rate
                .available_at_micros
                .max(self.decision_start)
                .max(self.now);
            let rate = rate.id.clone();
            self.now = at;
            self.advance_pauses()?;
            self.emit(at, EventKind::RateAvailable { rate })?;
        }
        Ok(())
    }

    /// Marks every obligation still open after the last permitted observation as unresolved
    /// with its path so far, then observes the rates available by the decision end.
    pub fn finish(&mut self) -> Result<(), String> {
        let commands: Vec<String> = self
            .obligations
            .iter()
            .filter(|(_, obligation)| obligation.unresolved.is_none())
            .map(|(command, _)| command.clone())
            .collect();
        for command in commands {
            let path = self.obligations[&command].path;
            self.emit(
                self.now,
                EventKind::Unresolved {
                    command,
                    reason: UnresolvedReason::WindowExhausted,
                    evidence: format!(
                        "no permitted observation after {}",
                        format_event_time_micros(self.now)
                    ),
                    path,
                },
            )?;
        }
        self.observe_rates(self.decision_end)
    }

    fn advance_pauses(&mut self) -> Result<(), String> {
        for index in 0..self.accounts.len() {
            if self.accounts[index]
                .paused_until_micros
                .is_some_and(|until| until <= self.now)
            {
                let account = self.accounts[index].id.clone();
                self.emit(self.now, EventKind::PauseEnded { account })?;
            }
        }
        Ok(())
    }

    fn observe_tick(
        &mut self,
        instrument: usize,
        time: i64,
        price_units: i64,
    ) -> Result<(), String> {
        let state = self
            .instruments
            .get_mut(instrument)
            .ok_or_else(|| format!("no instrument {instrument}"))?;
        if time > self.now {
            return Err(format!(
                "a tick at {} is not available at {}",
                format_event_time_micros(time),
                format_event_time_micros(self.now)
            ));
        }
        let previous = state.tick;
        if let Some(previous) = previous
            && time < previous.provider_time_micros
        {
            return Err(format!(
                "backwards tick time {} after {}",
                format_event_time_micros(time),
                format_event_time_micros(previous.provider_time_micros)
            ));
        }
        if let Some(previous) = previous
            && time == previous.provider_time_micros
            && price_units != previous.price_units
        {
            return Err(format!(
                "conflicting tick at {}: {} after {}",
                format_event_time_micros(time),
                price_units,
                previous.price_units
            ));
        }
        state.tick = Some(TickState {
            provider_time_micros: time,
            price_units,
            // A repeated tick at the same time keeps the gap into that time.
            gap_micros: previous.and_then(|previous| {
                if time == previous.provider_time_micros {
                    previous.gap_micros
                } else {
                    Some(time - previous.provider_time_micros)
                }
            }),
        });
        let tracked = std::mem::take(&mut state.tracked);
        let mut kept = Vec::with_capacity(tracked.len());
        for command in tracked {
            if self.drive(&command, time, price_units)? {
                kept.push(command);
            }
        }
        self.instruments[instrument].tracked = kept;
        Ok(())
    }

    /// Updates one accepted obligation's path with a tick and settles or leaves it unresolved
    /// under `price_at_due_v1`. Returns whether the tick stream still drives it.
    fn drive(&mut self, command: &str, time: i64, price_units: i64) -> Result<bool, String> {
        let Some(obligation) = self.obligations.get(command) else {
            return Ok(false);
        };
        let (Some(entry_time), Some(entry_price), Some(due), Some(mut path), Some(anchor), None) = (
            obligation.entry_time_micros,
            obligation.entry_price_units,
            obligation.due_time_micros,
            obligation.path,
            obligation.continuity_micros,
            obligation.unresolved,
        ) else {
            return Ok(false);
        };
        let contract =
            &self.definition.replay.contracts[self.bindings[obligation.binding].contract];
        let settlement = contract.settlement;
        let direction = contract.direction;
        // Ticks at or before the entry are continuity evidence, not path evidence.
        if time <= entry_time {
            self.obligations
                .get_mut(command)
                .expect("present")
                .continuity_micros = Some(time.max(anchor));
            return Ok(true);
        }
        // Continuity is measured from the obligation's own evidence: the quote tick at
        // acceptance and the ticks it observed since. Ticks the instrument saw while the command
        // was unaccepted, or before an engine was restored, are not assumed: a gap into this
        // tick that exceeds the maximum is not settlement evidence, and the obligation stays
        // open with the path observed before it.
        if time - anchor > settlement.max_tick_gap_micros {
            self.emit(
                self.now,
                EventKind::Unresolved {
                    command: command.to_string(),
                    reason: UnresolvedReason::Gap,
                    evidence: format!(
                        "a gap of {} microseconds from {} to {} exceeds {} inside the contract window",
                        time - anchor,
                        format_event_time_micros(anchor),
                        format_event_time_micros(time),
                        settlement.max_tick_gap_micros
                    ),
                    path: Some(path),
                },
            )?;
            return Ok(false);
        }
        // A tick too late to settle is not path evidence either.
        if time >= due && time - due > settlement.max_settlement_delay_micros {
            self.emit(
                self.now,
                EventKind::Unresolved {
                    command: command.to_string(),
                    reason: UnresolvedReason::LateSettlement,
                    evidence: format!(
                        "the first tick at or after {} arrived {} microseconds later, more than {}",
                        format_event_time_micros(due),
                        time - due,
                        settlement.max_settlement_delay_micros
                    ),
                    path: Some(path),
                },
            )?;
            return Ok(false);
        }
        if time < due {
            let move_units = signed_move(entry_price, price_units, direction)
                .map_err(|reason| format!("{command}: {reason}"))?;
            path.observe(time, move_units);
            let obligation = self.obligations.get_mut(command).expect("present");
            obligation.path = Some(path);
            obligation.continuity_micros = Some(time);
            return Ok(true);
        }
        let outcome = match price_units.cmp(&entry_price) {
            Ordering::Equal => Outcome::Tie,
            Ordering::Greater if direction == Direction::Buy => Outcome::Win,
            Ordering::Less if direction == Direction::Sell => Outcome::Win,
            _ => Outcome::Loss,
        };
        let cashflow = contract.cashflow(outcome);
        let source = EventSource {
            id: format!("{}:{command}", SettlementRule::PriceAtDueV1),
            provider_time_micros: time,
            available_at_micros: self.now,
            simulated: true,
        };
        self.settle(
            command,
            source,
            price_units,
            outcome,
            cashflow.gross_return,
            cashflow.terminal_fee,
            path,
        )?;
        Ok(false)
    }

    fn require(&self, command: &str, state: ObligationState) -> Result<(), String> {
        match self.obligations.get(command) {
            Some(obligation) if obligation.state == state => Ok(()),
            Some(obligation) => Err(format!(
                "{command} is {:?}, not {state:?}",
                obligation.state
            )),
            None => Err(format!("{command} is not an open obligation")),
        }
    }

    fn require_unaccepted(&self, command: &str) -> Result<(), String> {
        match self.obligations.get(command) {
            Some(obligation) if obligation.state.unaccepted() => Ok(()),
            Some(obligation) => Err(format!(
                "{command} is {:?}, not sent or acknowledged",
                obligation.state
            )),
            None => Err(format!("{command} is not an open obligation")),
        }
    }

    /// The exact postings of accepting an unaccepted command: the debited purchase, the
    /// remaining terminal reserve, and the due time.
    fn acceptance_postings(
        &self,
        command: &str,
        entry_time_micros: i64,
    ) -> Result<(Decimal, Decimal, i64), String> {
        let (contract, scale, _) = self.terms(command)?;
        Ok((
            contract.purchase()?.rescale(scale)?,
            contract.terminal_reserve()?.rescale(scale)?,
            entry_time_micros
                .checked_add(contract.duration_micros)
                .ok_or("the due time overflows microseconds")?,
        ))
    }

    fn observe_accepted(
        &mut self,
        command: String,
        source: EventSource,
        entry_time_micros: i64,
        entry_price_units: i64,
        price_time_micros: i64,
    ) -> Result<(), String> {
        let (debit, reservation, due_time_micros) =
            self.acceptance_postings(&command, entry_time_micros)?;
        self.emit(
            self.now,
            EventKind::Accepted {
                command,
                source,
                entry_time_micros,
                entry_price_units,
                price_time_micros,
                due_time_micros,
                debit,
                reservation,
            },
        )
    }

    fn observe_release(
        &mut self,
        command: String,
        source: EventSource,
        rejected: bool,
    ) -> Result<(), String> {
        self.require_unaccepted(&command)?;
        let release = self.obligations[&command].reservation;
        self.emit(
            self.now,
            EventKind::Released {
                command,
                source,
                rejected,
                release,
            },
        )
    }

    /// The exact postings of settling an accepted obligation with an actual cashflow: the
    /// credit `gross_return - terminal_fee`, the completed profit, the released reservation, a
    /// discrepancy when the cashflow contradicts the frozen terms, and a deficit when the net
    /// terminal debit exceeds the remaining reservation.
    fn settlement_postings(
        &self,
        command: &str,
        outcome: Outcome,
        gross_return: Decimal,
        terminal_fee: Decimal,
    ) -> Result<SettlementPostings, String> {
        let (contract, scale, _) = self.terms(command)?;
        let obligation = &self.obligations[command];
        let expected = contract.cashflow(outcome);
        let discrepancy = gross_return.compare(expected.gross_return)? != Ordering::Equal
            || terminal_fee.compare(expected.terminal_fee)? != Ordering::Equal;
        let credit = gross_return.checked_sub(terminal_fee)?.rescale(scale)?;
        let release = obligation.reservation;
        let shortfall = credit.checked_add(release)?;
        Ok(SettlementPostings {
            credit,
            profit: credit.checked_sub(obligation.paid_basis)?,
            release,
            discrepancy,
            deficit: shortfall
                .is_negative()
                .then(|| Decimal::zero(scale).checked_sub(shortfall))
                .transpose()?,
        })
    }

    #[allow(clippy::too_many_arguments)]
    /// Emits the settlement record of an accepted obligation: `path` is the evidence base
    /// (the ticks observed so far for the rule's own settlement, the ledger-recorded path for
    /// an authoritative one) and the settlement price is observed onto it at its provider
    /// time. A configured pause may follow.
    fn settle(
        &mut self,
        command: &str,
        source: EventSource,
        settlement_price_units: i64,
        outcome: Outcome,
        gross_return: Decimal,
        terminal_fee: Decimal,
        mut path: PathMetrics,
    ) -> Result<(), String> {
        let settlement_time_micros = source.provider_time_micros;
        let (contract, _, account) = self.terms(command)?;
        let direction = contract.direction;
        let postings = self.settlement_postings(command, outcome, gross_return, terminal_fee)?;
        let Some(entry_price) = self.obligations[command].entry_price_units else {
            return Err(format!("{command} has no entry to settle against"));
        };
        path.observe(
            settlement_time_micros,
            signed_move(entry_price, settlement_price_units, direction)
                .map_err(|reason| format!("{command}: {reason}"))?,
        );
        self.emit(
            self.now,
            EventKind::Settled {
                command: command.to_string(),
                source,
                settlement_time_micros,
                settlement_price_units,
                outcome,
                gross_return,
                terminal_fee,
                credit: postings.credit,
                profit: postings.profit,
                release: postings.release,
                discrepancy: postings.discrepancy,
                deficit: postings.deficit,
                path,
            },
        )?;
        self.maybe_pause(account)
    }

    /// The pause an account's bindings declare; validation makes every binding of one account
    /// declare it identically.
    fn pause_of(&self, account: usize) -> Option<Pause> {
        self.bindings
            .iter()
            .find(|binding| binding.account == account)
            .and_then(|binding| self.definition.replay.risk_policies[binding.policy].pause)
    }

    /// Whether an account's epoch drawdown has reached its configured pause threshold.
    fn pause_due(&self, account: usize) -> Result<bool, String> {
        let Some(pause) = self.pause_of(account) else {
            return Ok(false);
        };
        let state = &self.accounts[account];
        let drawdown = state.epoch_peak.checked_sub(state.completed_profit)?;
        Ok(drawdown.compare(pause.drawdown)? != Ordering::Less)
    }

    /// Starts the configured pause of an account whose epoch drawdown reached its threshold.
    fn maybe_pause(&mut self, account: usize) -> Result<(), String> {
        let state = &self.accounts[account];
        if state.paused_until_micros.is_some() || !self.pause_due(account)? {
            return Ok(());
        }
        let pause = self.pause_of(account).expect("due");
        let drawdown = state.epoch_peak.checked_sub(state.completed_profit)?;
        let until_micros = self
            .now
            .checked_add(pause.duration_micros)
            .ok_or("the pause deadline overflows microseconds")?;
        let account = state.id.clone();
        self.emit(
            self.now,
            EventKind::PauseStarted {
                account,
                until_micros,
                drawdown,
            },
        )
    }

    /// The exact postings of reconciling an open command: what is released, debited, credited,
    /// and completed under the resolution.
    fn reconciliation_postings(
        &self,
        command: &str,
        resolution: &Resolution,
    ) -> Result<ReconciliationPostings, String> {
        let (contract, scale, _) = self.terms(command)?;
        let obligation = &self.obligations[command];
        let purchase = contract.purchase()?.rescale(scale)?;
        let zero = Decimal::zero(scale);
        let accepted = obligation.state == ObligationState::Accepted;
        Ok(match resolution {
            Resolution::NotSent if accepted => {
                return Err(format!("{command} was accepted and cannot be not sent"));
            }
            Resolution::NotSent => ReconciliationPostings {
                release: obligation.reservation,
                debit: zero,
                credit: zero,
                profit: None,
            },
            Resolution::Accepted { .. } if accepted => {
                return Err(format!("{command} is already accepted"));
            }
            Resolution::Accepted {
                entry_time_micros, ..
            } => {
                let (debit, reservation, _) =
                    self.acceptance_postings(command, *entry_time_micros)?;
                ReconciliationPostings {
                    release: obligation.reservation.checked_sub(reservation)?,
                    debit,
                    credit: zero,
                    profit: None,
                }
            }
            Resolution::Settled {
                gross_return,
                terminal_fee,
                ..
            } => {
                let credit = gross_return.checked_sub(*terminal_fee)?.rescale(scale)?;
                let (debit, basis) = if accepted {
                    (zero, obligation.paid_basis)
                } else {
                    (purchase, purchase)
                };
                ReconciliationPostings {
                    release: obligation.reservation,
                    debit,
                    credit,
                    profit: Some(credit.checked_sub(basis)?),
                }
            }
        })
    }

    /// The account a settled discrepancy or deficit left blocked on `command`, when the
    /// command is no longer open.
    fn blocked_by(&self, command: &str) -> Option<usize> {
        self.accounts
            .iter()
            .position(|account| account.blocked.contains_key(command))
    }

    /// The postings of reconciling `command`: those of its resolution while it is open, or
    /// nothing when only a settled discrepancy's block remains.
    fn reconciliation_of(
        &self,
        command: &str,
        resolution: &Resolution,
    ) -> Result<(ReconciliationPostings, usize), String> {
        if let Some(obligation) = self.obligations.get(command) {
            let account = self.bindings[obligation.binding].account;
            return Ok((self.reconciliation_postings(command, resolution)?, account));
        }
        let account = self.blocked_by(command).ok_or_else(|| {
            format!("{command} is neither an open obligation nor a settled discrepancy")
        })?;
        // A settled discrepancy has booked its cashflow; only the same settlement lifts the
        // block, because no corrective posting exists.
        let matches = match (&self.accounts[account].blocked[command], resolution) {
            (
                Block::Settled {
                    outcome,
                    gross_return,
                    terminal_fee,
                },
                Resolution::Settled {
                    outcome: stated,
                    gross_return: stated_return,
                    terminal_fee: stated_fee,
                },
            ) => {
                outcome == stated
                    && gross_return.compare(*stated_return)? == Ordering::Equal
                    && terminal_fee.compare(*stated_fee)? == Ordering::Equal
            }
            _ => false,
        };
        if !matches {
            return Err(format!(
                "{command} reconciliation contradicts the settlement already booked; reconciliation failed"
            ));
        }
        let zero = Decimal::zero(self.accounts[account].scale);
        Ok((
            ReconciliationPostings {
                release: zero,
                debit: zero,
                credit: zero,
                profit: None,
            },
            account,
        ))
    }

    fn observe_reconciliation(
        &mut self,
        command: String,
        source: EventSource,
        resolution: Resolution,
    ) -> Result<(), String> {
        let (postings, account) = self.reconciliation_of(&command, &resolution)?;
        self.emit(
            self.now,
            EventKind::Reconciled {
                command,
                source,
                resolution,
                release: postings.release,
                debit: postings.debit,
                credit: postings.credit,
                profit: postings.profit,
            },
        )?;
        self.maybe_pause(account)
    }

    // ------------------------------------------------------------------------------------------
    // Decisions
    // ------------------------------------------------------------------------------------------

    /// Whether one condition holds against the instrument's latest rows for a base row closing at
    /// `base_close`: a required latest row that is missing, that closes after the base row,
    /// whose value is unavailable, or whose readiness flags are not all true fails the condition.
    fn holds(&self, instrument: usize, base_close: i64, condition: &CompiledCondition) -> bool {
        let Some(row) = &self.instruments[instrument].rows[condition.stream] else {
            return false;
        };
        if row.close_time_micros > base_close {
            return false;
        }
        if condition
            .readiness
            .iter()
            .any(|flag| row.values[*flag] != Some(Value::Bool(true)))
        {
            return false;
        }
        let Some(value) = &row.values[condition.column] else {
            return false;
        };
        let spec = &self.definition.instruments[instrument].streams[condition.stream].columns
            [condition.column];
        if let Value::Text(text) = value
            && spec.unready.iter().any(|unready| unready == text.as_ref())
        {
            return false;
        }
        let label: Option<Cow<'_, str>> = spec
            .encoding
            .as_ref()
            .and_then(|encoding| encoding.label(Some(value)));
        let ordering = match (&condition.threshold, label.as_deref(), value) {
            (Threshold::Text(threshold), Some(label), _) => Some(label.cmp(threshold.as_str())),
            (Threshold::Text(threshold), None, Value::Text(text)) => {
                Some(text.as_ref().cmp(threshold.as_str()))
            }
            (Threshold::Bool(threshold), None, Value::Bool(value)) => Some(value.cmp(threshold)),
            (Threshold::Number(threshold), None, Value::Int(v) | Value::Time(v)) => {
                (*v as f64).partial_cmp(threshold)
            }
            (Threshold::Number(threshold), None, Value::Float(v)) => v.partial_cmp(threshold),
            _ => None,
        };
        let Some(ordering) = ordering else {
            return false;
        };
        match condition.comparator {
            Comparator::Eq => ordering == Ordering::Equal,
            Comparator::Ne => ordering != Ordering::Equal,
            Comparator::Lt => ordering == Ordering::Less,
            Comparator::Le => ordering != Ordering::Greater,
            Comparator::Gt => ordering == Ordering::Greater,
            Comparator::Ge => ordering != Ordering::Less,
        }
    }

    /// Evaluates one binding against its base stream's latest row at the current decision time:
    /// base conditions, then same-entry selection and deduplication, then repair and the
    /// remaining admission checks. A matching signal is always a ledger record.
    fn evaluate(&mut self, binding_index: usize) -> Result<(), String> {
        if self.now < self.decision_start || self.now >= self.decision_end {
            return Ok(());
        }
        let binding = self.bindings[binding_index].clone();
        let strategy = &self.strategies[binding.strategy];
        let Some(row) = &self.instruments[binding.instrument].rows[strategy.base_stream] else {
            return Ok(());
        };
        let (close, known_at) = (row.close_time_micros, row.known_at_micros);
        if self.decided[binding_index].is_some_and(|last| close <= last) {
            return Ok(());
        }
        if !strategy
            .conditions
            .iter()
            .all(|condition| self.holds(binding.instrument, close, condition))
        {
            return Ok(());
        }
        let contract = &self.definition.replay.contracts[binding.contract];
        let policy = &self.definition.replay.risk_policies[binding.policy];
        let same_entry = (
            binding.account,
            binding.instrument,
            contract.duration_micros,
        );
        let logic = (binding.account, binding.instrument, strategy.logic.clone());
        let first_only = policy.same_entry == SameEntry::First;
        let deduplicate = policy.deduplicate_signal_logic;
        let scale = self.accounts[binding.account].scale;
        let reservation = contract.reservation()?.rescale(scale)?;
        let stream =
            self.definition.instruments[binding.instrument].streams[strategy.base_stream].stream;
        let logic_identity = strategy.logic.clone();
        let slots_live = self.slot_time == self.now;
        let (disposition, rates) =
            if first_only && slots_live && self.same_entry.contains(&same_entry) {
                (Disposition::SameEntryDuplicate, Vec::new())
            } else if deduplicate && slots_live && self.logic_seen.contains(&logic) {
                (Disposition::DuplicateLogic, Vec::new())
            } else if !self.strategies[binding.strategy]
                .repair
                .iter()
                .all(|condition| self.holds(binding.instrument, close, condition))
            {
                (Disposition::RepairBlocked, Vec::new())
            } else {
                self.admit(binding_index, close, reservation)?
            };
        let admitted = disposition == Disposition::Admitted;
        let quote = self.instruments[binding.instrument].tick;
        let event = EventKind::Signal {
            instrument: self.definition.instruments[binding.instrument]
                .instrument
                .clone(),
            binding: binding.id.clone(),
            deployment_identity: binding.identity.clone(),
            signal_logic_identity: logic_identity,
            stream,
            close_time_micros: close,
            known_at_micros: known_at,
            split: self.split_of(self.now),
            disposition,
            quote_price_units: quote.map(|tick| tick.price_units),
            quote_time_micros: quote.map(|tick| tick.provider_time_micros),
            command: admitted.then(|| format!("{}/{close}", binding.id)),
            reservation: admitted.then_some(reservation),
            rates,
        };
        self.emit(self.now, event)
    }

    /// The remaining admission checks in order: quote presence and freshness, entry continuity,
    /// then the state-based checks of `financial_admission`. Equality with a bound or a maximum
    /// is permitted. Returns the disposition and the rate identities any conversion used.
    fn admit(
        &self,
        binding_index: usize,
        close: i64,
        reservation: Decimal,
    ) -> Result<(Disposition, Vec<String>), String> {
        let binding = &self.bindings[binding_index];
        let contract = &self.definition.replay.contracts[binding.contract];
        let policy = &self.definition.replay.risk_policies[binding.policy];
        let blocked = |disposition| Ok((disposition, Vec::new()));
        let Some(quote) = self.instruments[binding.instrument].tick else {
            return blocked(Disposition::NoQuote);
        };
        if self.now - close > policy.max_feature_age_micros {
            return blocked(Disposition::StaleFeature);
        }
        if self.now - quote.provider_time_micros > policy.max_quote_age_micros {
            return blocked(Disposition::StaleQuote);
        }
        if quote
            .gap_micros
            .is_some_and(|gap| gap > contract.settlement.max_tick_gap_micros)
        {
            return blocked(Disposition::GapAtEntry);
        }
        self.financial_admission(binding_index, reservation)
    }

    /// The state-based admission checks, in order: account pause and block, the envelope, every
    /// capacity scope, cash, and unresolved-loss limits. Generation runs them after the quote
    /// checks; restoration runs them again on every admitted signal record, so an admission the
    /// restored state cannot fund or hold is an illegal transition.
    fn financial_admission(
        &self,
        binding_index: usize,
        reservation: Decimal,
    ) -> Result<(Disposition, Vec<String>), String> {
        let binding = &self.bindings[binding_index];
        let contract = &self.definition.replay.contracts[binding.contract];
        let policy = &self.definition.replay.risk_policies[binding.policy];
        let account = &self.accounts[binding.account];
        let blocked = |disposition| Ok((disposition, Vec::new()));
        if account.paused_until_micros.is_some() || self.pause_due(binding.account)? {
            return blocked(Disposition::AccountPaused);
        }
        if !account.blocked.is_empty() {
            return blocked(Disposition::AccountBlocked);
        }
        if !self.definition.replay.bindings[binding_index]
            .envelope
            .admits(contract)?
        {
            return blocked(Disposition::QuoteRejected);
        }
        let over = |count: u32, limit: Option<u32>| limit.is_some_and(|limit| count + 1 > limit);
        let count = |map: &HashMap<usize, u32>, key: &usize| map.get(key).copied().unwrap_or(0);
        if over(
            count(&self.open_by_binding, &binding_index),
            policy.max_open_per_strategy,
        ) {
            return blocked(Disposition::CapacityStrategy);
        }
        if over(
            self.open_by_duration
                .get(&contract.duration_micros)
                .copied()
                .unwrap_or(0),
            policy.max_open_per_duration,
        ) {
            return blocked(Disposition::CapacityDuration);
        }
        if over(
            count(&self.open_by_instrument, &binding.instrument),
            policy.max_open_per_instrument,
        ) {
            return blocked(Disposition::CapacityInstrument);
        }
        if over(account.open, policy.max_open_per_account) {
            return blocked(Disposition::CapacityAccount);
        }
        if over(self.open_total, policy.max_open_total) {
            return blocked(Disposition::CapacityTotal);
        }
        if account.available()?.compare(reservation)? == Ordering::Less {
            return blocked(Disposition::InsufficientCash);
        }
        let worst = contract.worst_loss()?.rescale(account.scale)?;
        if let Some(limit) = policy.max_unresolved_loss_per_account
            && account
                .unresolved_loss
                .checked_add(worst)?
                .compare(limit.rescale(account.scale)?)?
                == Ordering::Greater
        {
            return blocked(Disposition::UnresolvedLossAccount);
        }
        let mut rates = Vec::new();
        if let Some(limit) = policy.max_unresolved_loss_total {
            let mut total = Decimal::zero(self.definition.replay.reporting_scale);
            for (amount, currency) in std::iter::once((worst, &account.currency)).chain(
                self.accounts
                    .iter()
                    .map(|other| (other.unresolved_loss, &other.currency)),
            ) {
                match self.convert(amount, currency) {
                    Ok(converted) => {
                        total = total.checked_add(converted.amount)?;
                        rates.extend(converted.rate);
                    }
                    Err(_) => return blocked(Disposition::ConversionUnavailable),
                }
            }
            rates.sort_unstable();
            rates.dedup();
            if total.compare(limit)? == Ordering::Greater {
                return Ok((Disposition::UnresolvedLossTotal, rates));
            }
        }
        Ok((Disposition::Admitted, rates))
    }

    // ------------------------------------------------------------------------------------------
    // The one event-application function
    // ------------------------------------------------------------------------------------------

    /// Sequences, applies, and records one transition, then observes the portfolio when the
    /// record changed an account. Generation and restoration both come through here, so an
    /// illegal transition or a posting that disagrees with the obligation fails identically in
    /// both.
    fn emit(&mut self, time_micros: i64, kind: EventKind) -> Result<(), String> {
        let event = FinancialEvent {
            sequence: self.sequence,
            time_micros,
            kind,
        };
        if let Some(source) = event.kind.source()
            && (source.provider_time_micros > source.available_at_micros
                || source.available_at_micros > time_micros)
        {
            return Err(format!(
                "external event `{}` is not available at {}",
                source.id,
                format_event_time_micros(time_micros)
            ));
        }
        let external = event.kind.external();
        if let Some((key, _)) = &external
            && self.externals.contains_key(key)
        {
            return Err(format!("external event `{key}` is already applied"));
        }
        if let Some((pending, due_micros)) = self.pause_pending
            && !matches!(&event.kind, EventKind::PauseStarted { account, .. } if *account == self.accounts[pending].id && time_micros == due_micros)
        {
            return Err(format!(
                "account `{}` reached its pause threshold; its pause record is required next at {}",
                self.accounts[pending].id,
                format_event_time_micros(due_micros)
            ));
        }
        let expired = |account: &AccountState| {
            account
                .paused_until_micros
                .is_some_and(|until| until <= time_micros)
        };
        if !matches!(event.kind, EventKind::PauseEnded { .. })
            && let Some(account) = self.accounts.iter().find(|account| expired(account))
        {
            return Err(format!(
                "account `{}` has an expired pause; its end record is required before {}",
                account.id,
                format_event_time_micros(time_micros)
            ));
        }
        self.apply(&event)?;
        if let Some((key, payload)) = external {
            self.externals.insert(key, payload);
        }
        if event.kind.observes_portfolio() {
            self.observe_portfolio(time_micros);
        }
        self.sequence += 1;
        self.summary.events = self.sequence;
        self.summary.last_time_micros = Some(time_micros);
        self.summary.accounts = self.accounts.clone();
        self.events.push(event);
        Ok(())
    }

    fn groups(&mut self, key: &GroupKey) -> Vec<&mut Group> {
        let summary = &mut self.summary;
        let mut groups = vec![&mut summary.portfolio];
        groups.push(summary.strategies.entry(key.binding.clone()).or_default());
        groups.push(
            summary
                .durations
                .entry(key.duration.to_string())
                .or_default(),
        );
        groups.push(
            summary
                .instruments
                .entry(key.instrument.clone())
                .or_default(),
        );
        if let Some(split) = &key.split {
            groups.push(summary.splits.entry(split.clone()).or_default());
        }
        groups
    }

    fn open_delta(&mut self, binding: usize, delta: i32) {
        let compiled = &self.bindings[binding];
        let duration = self.definition.replay.contracts[compiled.contract].duration_micros;
        let (account, instrument) = (compiled.account, compiled.instrument);
        let bump = |count: &mut u32| *count = (i64::from(*count) + i64::from(delta)) as u32;
        bump(self.open_by_binding.entry(binding).or_default());
        bump(self.open_by_duration.entry(duration).or_default());
        bump(self.open_by_instrument.entry(instrument).or_default());
        bump(&mut self.accounts[account].open);
        bump(&mut self.open_total);
    }

    fn open_obligation(&mut self, command: &str) -> Result<&mut Obligation, String> {
        self.obligations
            .get_mut(command)
            .ok_or_else(|| format!("{command} is not an open obligation"))
    }

    /// Applies an acceptance: the obligation moves to accepted with its entry, the purchase is
    /// debited once, only `reservation` (the terminal reserve) stays reserved, and the
    /// instrument's ticks start driving it. A reconciled acceptance of a possibly sent command
    /// also ends its unresolved state.
    #[allow(clippy::too_many_arguments)]
    fn apply_acceptance(
        &mut self,
        command: &str,
        entry_time_micros: i64,
        entry_price_units: i64,
        price_time_micros: i64,
        due_time_micros: i64,
        debit: Decimal,
        reservation: Decimal,
    ) -> Result<(), String> {
        let sent = self.open_obligation(command)?.sent_micros;
        if price_time_micros > entry_time_micros
            || entry_time_micros < sent
            || entry_time_micros > self.now
        {
            return Err(format!(
                "{command} acceptance clocks are inconsistent: quote {}, dispatch {}, entry {}, decision {}",
                format_event_time_micros(price_time_micros),
                format_event_time_micros(sent),
                format_event_time_micros(entry_time_micros),
                format_event_time_micros(self.now)
            ));
        }
        let obligation = self.open_obligation(command)?;
        let binding = obligation.binding;
        let release = obligation.reservation.checked_sub(reservation)?;
        let unresolved = obligation.unresolved.take().is_some();
        obligation.state = ObligationState::Accepted;
        obligation.reservation = reservation;
        obligation.paid_basis = debit;
        obligation.entry_time_micros = Some(entry_time_micros);
        obligation.entry_price_units = Some(entry_price_units);
        obligation.due_time_micros = Some(due_time_micros);
        obligation.path = Some(PathMetrics::new(entry_time_micros));
        obligation.recorded_path = obligation.path;
        obligation.continuity_micros = Some(price_time_micros);
        let split = obligation.split.clone();
        let account = &mut self.accounts[self.bindings[binding].account];
        account.reserved = account.reserved.checked_sub(release)?;
        account.cash = account.cash.checked_sub(debit)?;
        account.paid_basis = account.paid_basis.checked_add(debit)?;
        let instrument = self.bindings[binding].instrument;
        self.instruments[instrument]
            .tracked
            .push(command.to_string());
        let key = self.keys(binding, split.as_deref());
        for group in self.groups(&key) {
            group.accepted += 1;
            if unresolved {
                group.unresolved -= 1;
            }
        }
        Ok(())
    }

    /// Applies a closure: the obligation leaves the books, its remaining reservation, paid
    /// basis, worst loss, and capacity are released, `credit` is posted to cash, and a settled
    /// contract completes its outcome and profit; otherwise the command was released.
    fn apply_closure(
        &mut self,
        command: &str,
        credit: Decimal,
        settled: Option<(Outcome, Decimal)>,
    ) -> Result<(), String> {
        let obligation = self
            .obligations
            .remove(command)
            .ok_or_else(|| format!("{command} is not an open obligation"))?;
        let binding = obligation.binding;
        let account = &mut self.accounts[self.bindings[binding].account];
        let currency = account.currency.clone();
        account.reserved = account.reserved.checked_sub(obligation.reservation)?;
        account.paid_basis = account.paid_basis.checked_sub(obligation.paid_basis)?;
        account.unresolved_loss = account.unresolved_loss.checked_sub(obligation.worst_loss)?;
        account.cash = account.cash.checked_add(credit)?;
        if let Some((_, profit)) = settled {
            account.complete(profit)?;
        }
        let account_index = self.bindings[binding].account;
        if settled.is_some()
            && self.accounts[account_index].paused_until_micros.is_none()
            && self.pause_due(account_index)?
        {
            self.pause_pending = Some((account_index, self.now));
        }
        self.open_delta(binding, -1);
        let key = self.keys(binding, obligation.split.as_deref());
        for group in self.groups(&key) {
            group.close(obligation.unresolved.is_some());
            match settled {
                Some((outcome, profit)) => {
                    group.outcome(outcome);
                    group.add_profit(&currency, profit);
                }
                None => group.released += 1,
            }
        }
        Ok(())
    }

    fn apply(&mut self, event: &FinancialEvent) -> Result<(), String> {
        match &event.kind {
            EventKind::RunDefinition { .. } => {}
            EventKind::Signal {
                instrument,
                binding,
                deployment_identity,
                signal_logic_identity,
                stream,
                close_time_micros,
                known_at_micros,
                split,
                disposition,
                quote_price_units,
                quote_time_micros,
                command,
                reservation,
                rates,
            } => {
                let index = *self
                    .binding_index
                    .get(binding)
                    .ok_or_else(|| format!("unknown binding `{binding}`"))?;
                let compiled = self.bindings[index].clone();
                let strategy = &self.strategies[compiled.strategy];
                let bound = &self.definition.instruments[compiled.instrument];
                let contract = &self.definition.replay.contracts[compiled.contract];
                let policy = &self.definition.replay.risk_policies[compiled.policy];
                let scale = self.accounts[compiled.account].scale;
                let admitted = *disposition == Disposition::Admitted;
                let time = event.time_micros;
                if *instrument != bound.instrument
                    || *stream != bound.streams[strategy.base_stream].stream
                    || *deployment_identity != compiled.identity
                    || *signal_logic_identity != strategy.logic
                    || *split != self.split_of(time)
                {
                    return Err(format!(
                        "the signal of `{binding}` at {} disagrees with its binding's definition",
                        format_event_time_micros(*close_time_micros)
                    ));
                }
                if close_time_micros > known_at_micros
                    || *known_at_micros > time
                    || quote_time_micros.is_some_and(|quote| quote > time)
                    || quote_price_units.is_some() != quote_time_micros.is_some()
                    || time < self.decision_start
                    || time >= self.decision_end
                    || admitted
                        && (quote_time_micros.is_none()
                            || time - close_time_micros > policy.max_feature_age_micros
                            || quote_time_micros
                                .is_some_and(|quote| time - quote > policy.max_quote_age_micros))
                {
                    return Err(format!(
                        "the signal of `{binding}` at {} carries clocks its decision could not have seen",
                        format_event_time_micros(*close_time_micros)
                    ));
                }
                if self.decided[index].is_some_and(|last| *close_time_micros <= last) {
                    return Err(format!(
                        "binding `{binding}` already decided its signal at or after {}",
                        format_event_time_micros(*close_time_micros)
                    ));
                }
                self.decided[index] = Some(*close_time_micros);
                if time != self.slot_time {
                    self.same_entry.clear();
                    self.logic_seen.clear();
                    self.slot_time = time;
                }
                let entry_key = (
                    compiled.account,
                    compiled.instrument,
                    contract.duration_micros,
                );
                let logic_key = (
                    compiled.account,
                    compiled.instrument,
                    strategy.logic.clone(),
                );
                let slot = if policy.same_entry == SameEntry::First
                    && self.same_entry.contains(&entry_key)
                {
                    Some(Disposition::SameEntryDuplicate)
                } else if policy.deduplicate_signal_logic && self.logic_seen.contains(&logic_key) {
                    Some(Disposition::DuplicateLogic)
                } else {
                    None
                };
                let recorded = matches!(
                    disposition,
                    Disposition::SameEntryDuplicate | Disposition::DuplicateLogic
                )
                .then_some(*disposition);
                if slot != recorded {
                    return Err(format!(
                        "the signal of `{binding}` at {} disagrees with the selection and deduplication slots of its instant",
                        format_event_time_micros(*close_time_micros)
                    ));
                }
                // Selection precedes deduplication: a deduplicated candidate keeps the
                // selection slot it already passed.
                if *disposition != Disposition::SameEntryDuplicate {
                    self.same_entry.insert(entry_key);
                }
                if slot.is_none() {
                    self.logic_seen.insert(logic_key);
                }
                if admitted {
                    let (Some(command), Some(reservation)) = (command, reservation) else {
                        return Err(
                            "an admitted signal names its command and reservation".to_string()
                        );
                    };
                    if *command != format!("{}/{close_time_micros}", compiled.id)
                        || *reservation != contract.reservation()?.rescale(scale)?
                    {
                        return Err(format!(
                            "{command} is not the command and reservation of its signal"
                        ));
                    }
                    let (admissible, used) = self.financial_admission(index, *reservation)?;
                    if admissible != Disposition::Admitted || used != *rates {
                        return Err(format!(
                            "{command} is not admissible at this state: {admissible}"
                        ));
                    }
                    let worst = contract.worst_loss()?.rescale(scale)?;
                    let account = &mut self.accounts[compiled.account];
                    account.reserved = account.reserved.checked_add(*reservation)?;
                    account.unresolved_loss = account.unresolved_loss.checked_add(worst)?;
                    self.obligations.insert(
                        command.clone(),
                        Obligation {
                            binding: index,
                            state: ObligationState::Sent,
                            reservation: *reservation,
                            paid_basis: Decimal::zero(scale),
                            worst_loss: worst,
                            split: split.clone(),
                            entry_time_micros: None,
                            entry_price_units: None,
                            due_time_micros: None,
                            sent_micros: time,
                            unresolved: None,
                            path: None,
                            recorded_path: None,
                            continuity_micros: None,
                        },
                    );
                    self.open_delta(index, 1);
                }
                let key = self.keys(index, split.as_deref());
                for group in self.groups(&key) {
                    group.signals += 1;
                    *group
                        .dispositions
                        .entry(disposition.to_string())
                        .or_default() += 1;
                    if admitted {
                        group.open += 1;
                    }
                }
            }
            EventKind::Acknowledged { command, .. } => {
                self.require(command, ObligationState::Sent)?;
                self.open_obligation(command)?.state = ObligationState::Acknowledged;
            }
            EventKind::Accepted {
                command,
                entry_time_micros,
                entry_price_units,
                price_time_micros,
                due_time_micros,
                debit,
                reservation,
                ..
            } => {
                self.require_unaccepted(command)?;
                if (*debit, *reservation, *due_time_micros)
                    != self.acceptance_postings(command, *entry_time_micros)?
                {
                    return Err(format!(
                        "{command} acceptance postings disagree with its contract"
                    ));
                }
                self.apply_acceptance(
                    command,
                    *entry_time_micros,
                    *entry_price_units,
                    *price_time_micros,
                    *due_time_micros,
                    *debit,
                    *reservation,
                )?;
            }
            EventKind::Released {
                command, release, ..
            } => {
                self.require_unaccepted(command)?;
                if self.obligations[command].reservation != *release {
                    return Err(format!("{command} cannot release {release}"));
                }
                self.apply_closure(command, Decimal::zero(release.scale()), None)?;
            }
            EventKind::PossiblySent { command, .. } => {
                self.require_unaccepted(command)?;
                let obligation = self.open_obligation(command)?;
                obligation.state = ObligationState::PossiblySent;
                let newly_unresolved = obligation.unresolved.is_none();
                obligation.unresolved = Some(UnresolvedReason::PossiblySent);
                let binding = obligation.binding;
                let split = obligation.split.clone();
                self.accounts[self.bindings[binding].account]
                    .blocked
                    .insert(command.clone(), Block::PossiblySent);
                if newly_unresolved {
                    let key = self.keys(binding, split.as_deref());
                    for group in self.groups(&key) {
                        group.unresolved += 1;
                    }
                }
            }
            EventKind::Settled {
                command,
                source,
                settlement_time_micros,
                outcome,
                gross_return,
                terminal_fee,
                credit,
                profit,
                release,
                discrepancy,
                deficit,
                ..
            } => {
                self.require(command, ObligationState::Accepted)?;
                if *settlement_time_micros != source.provider_time_micros
                    || Some(*settlement_time_micros) < self.obligations[command].entry_time_micros
                {
                    return Err(format!(
                        "{command} settlement time disagrees with its source and entry"
                    ));
                }
                let postings =
                    self.settlement_postings(command, *outcome, *gross_return, *terminal_fee)?;
                if postings
                    != (SettlementPostings {
                        credit: *credit,
                        profit: *profit,
                        release: *release,
                        discrepancy: *discrepancy,
                        deficit: *deficit,
                    })
                {
                    return Err(format!(
                        "{command} settlement postings disagree with its obligation and terms"
                    ));
                }
                let account = self.bindings[self.obligations[command].binding].account;
                self.apply_closure(command, *credit, Some((*outcome, *profit)))?;
                if *discrepancy || deficit.is_some() {
                    self.accounts[account].blocked.insert(
                        command.clone(),
                        Block::Settled {
                            outcome: *outcome,
                            gross_return: *gross_return,
                            terminal_fee: *terminal_fee,
                        },
                    );
                }
            }
            EventKind::Unresolved {
                command,
                reason,
                path,
                ..
            } => {
                let obligation = self.open_obligation(command)?;
                if obligation.unresolved.is_some() {
                    return Err(format!("{command} is already unresolved"));
                }
                obligation.unresolved = Some(*reason);
                obligation.path = *path;
                obligation.recorded_path = *path;
                let binding = obligation.binding;
                let split = obligation.split.clone();
                let instrument = self.bindings[binding].instrument;
                self.instruments[instrument]
                    .tracked
                    .retain(|tracked| tracked != command);
                let key = self.keys(binding, split.as_deref());
                for group in self.groups(&key) {
                    group.unresolved += 1;
                }
            }
            EventKind::Reconciled {
                command,
                source,
                resolution,
                release,
                debit,
                credit,
                profit,
            } => {
                let (postings, account) = self.reconciliation_of(command, resolution)?;
                if postings
                    != (ReconciliationPostings {
                        release: *release,
                        debit: *debit,
                        credit: *credit,
                        profit: *profit,
                    })
                {
                    return Err(format!(
                        "{command} reconciliation postings disagree with its resolution"
                    ));
                }
                if self.obligations.contains_key(command) {
                    match resolution {
                        Resolution::NotSent => {
                            self.apply_closure(command, Decimal::zero(credit.scale()), None)?;
                        }
                        Resolution::Accepted {
                            entry_time_micros,
                            entry_price_units,
                            price_time_micros,
                        } => {
                            let (_, reservation, due_time_micros) =
                                self.acceptance_postings(command, *entry_time_micros)?;
                            self.apply_acceptance(
                                command,
                                *entry_time_micros,
                                *entry_price_units,
                                *price_time_micros,
                                due_time_micros,
                                *debit,
                                reservation,
                            )?;
                        }
                        Resolution::Settled { outcome, .. } => {
                            let obligation = &self.obligations[command];
                            if source.provider_time_micros
                                < obligation
                                    .entry_time_micros
                                    .unwrap_or(obligation.sent_micros)
                            {
                                return Err(format!(
                                    "{command} settlement evidence precedes its entry or dispatch"
                                ));
                            }
                            let profit = profit.expect("checked against the postings");
                            let cash = &mut self.accounts[account].cash;
                            *cash = cash.checked_sub(*debit)?;
                            self.apply_closure(command, *credit, Some((*outcome, profit)))?;
                        }
                    }
                }
                self.accounts[account].blocked.remove(command);
            }
            EventKind::RateAvailable { rate } => {
                let next = self.rates.get(self.next_rate);
                if next.is_none_or(|next| {
                    next.id != *rate || next.available_at_micros > event.time_micros
                }) {
                    return Err(format!(
                        "rate `{rate}` is not the next available rate at {}",
                        format_event_time_micros(event.time_micros)
                    ));
                }
                self.next_rate += 1;
            }
            EventKind::PauseStarted {
                account,
                until_micros,
                drawdown,
            } => {
                let index = self
                    .accounts
                    .iter()
                    .position(|state| state.id == *account)
                    .ok_or_else(|| format!("unknown account `{account}`"))?;
                let state = &self.accounts[index];
                let pause = self
                    .pause_of(index)
                    .ok_or_else(|| format!("account `{account}` declares no pause"))?;
                if state.paused_until_micros.is_some() {
                    return Err(format!("account `{account}` is already paused"));
                }
                if *drawdown != state.epoch_peak.checked_sub(state.completed_profit)?
                    || !self.pause_due(index)?
                    || Some(*until_micros) != event.time_micros.checked_add(pause.duration_micros)
                {
                    return Err(format!(
                        "account `{account}` pause disagrees with its drawdown and policy"
                    ));
                }
                self.accounts[index].paused_until_micros = Some(*until_micros);
                self.pause_pending = None;
            }
            EventKind::PauseEnded { account } => {
                let state = self
                    .accounts
                    .iter_mut()
                    .find(|state| state.id == *account)
                    .ok_or_else(|| format!("unknown account `{account}`"))?;
                if state
                    .paused_until_micros
                    .is_none_or(|until| until > event.time_micros)
                {
                    return Err(format!("account `{account}` has no expired pause"));
                }
                state.paused_until_micros = None;
                state.epoch_peak = state.completed_profit;
            }
        }
        Ok(())
    }

    /// Projects converted settled equity and unresolved loss into the reporting currency at one
    /// account-changing record; an unavailable rate leaves the observation unavailable, never
    /// native history.
    fn observe_portfolio(&mut self, at: i64) {
        let replay = &self.definition.replay;
        let scale = replay.reporting_scale;
        let reporting = &self.summary.reporting;
        // A projection that cannot be made (a missing or stale rate, or an aggregate the
        // reporting scale cannot hold) is unavailable; the native records it follows stand.
        let observed = (|| -> Result<(Decimal, Decimal, Vec<String>, Decimal, Decimal), String> {
            let mut equity = Decimal::zero(scale);
            let mut loss = Decimal::zero(scale);
            let mut used = Vec::new();
            for account in &self.accounts {
                let convert = |amount: Decimal| {
                    convert(
                        amount,
                        &account.currency,
                        &replay.reporting_currency,
                        scale,
                        at,
                        replay.max_rate_age_micros,
                        &self.rates,
                    )
                };
                let converted_equity = convert(account.settled_equity()?)?;
                let converted_loss = convert(account.unresolved_loss)?;
                equity = equity.checked_add(converted_equity.amount)?;
                loss = loss.checked_add(converted_loss.amount)?;
                used.extend(converted_equity.rate);
                used.extend(converted_loss.rate);
            }
            let peak = match reporting.peak_equity {
                Some(peak) => peak.max(equity)?,
                None => equity,
            };
            let drawdown = peak.checked_sub(equity)?;
            let drawdown = match reporting.max_drawdown {
                Some(current) => current.max(drawdown)?,
                None => drawdown,
            };
            Ok((equity, loss, used, peak, drawdown))
        })();
        let reporting = &mut self.summary.reporting;
        match observed {
            Ok((equity, loss, used, peak, drawdown)) => {
                reporting.observations += 1;
                reporting.settled_equity = Some(equity);
                reporting.unresolved_loss = Some(loss);
                reporting.used_rates.extend(used);
                reporting.peak_equity = Some(peak);
                reporting.max_drawdown = Some(drawdown);
            }
            Err(_) => {
                reporting.unavailable_observations += 1;
                reporting.settled_equity = None;
                reporting.unresolved_loss = None;
            }
        }
    }
}

// ---------------------------------------------------------------------------------------------
// Ready manifest
// ---------------------------------------------------------------------------------------------

/// The ready manifest of one replay generation. Field order is the serialization order.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ReplayManifest {
    pub kind: String,
    pub schema_version: u32,
    pub generation: String,
    pub role: DatasetRole,
    pub config_hash: String,
    pub code_revision: String,
    pub availability: String,
    pub decision_start: String,
    pub decision_end: String,
    pub instruments: Vec<InstrumentBinding>,
    pub events: u64,
    pub final_state_identity: String,
    pub summary_identity: String,
    pub objects: Vec<ObjectRecord>,
}

impl ReplayManifest {
    pub fn to_json(&self) -> Vec<u8> {
        let mut bytes = serde_json::to_vec_pretty(self).expect("a manifest serializes");
        bytes.push(b'\n');
        bytes
    }

    /// Parses a replay manifest and checks what every consumer relies on: the kind and schema,
    /// a permitted role, a generation that matches the configuration and inputs, exactly the
    /// ledger and summary objects, and content-addressed objects with unique paths.
    pub fn from_json(bytes: &[u8]) -> Result<Self, String> {
        let manifest: Self = serde_json::from_slice(bytes).map_err(|error| error.to_string())?;
        if manifest.kind != REPLAY_MANIFEST_KIND {
            return Err(format!(
                "manifest kind `{}` is not `{REPLAY_MANIFEST_KIND}`",
                manifest.kind
            ));
        }
        if manifest.schema_version != REPLAY_SCHEMA_VERSION {
            return Err(format!(
                "unsupported manifest schema_version {}, expected {REPLAY_SCHEMA_VERSION}",
                manifest.schema_version
            ));
        }
        if manifest.role == DatasetRole::Holdout {
            return Err("a replay generation never carries holdout data".to_string());
        }
        if manifest.generation != replay_generation_id(&manifest.config_hash, &manifest.instruments)
        {
            return Err(format!(
                "generation `{}` does not match the configuration hash and bound inputs",
                manifest.generation
            ));
        }
        validate_objects(&manifest.objects)?;
        let mut recorded: Vec<&str> = manifest
            .objects
            .iter()
            .map(|object| object.path.as_str())
            .collect();
        recorded.sort_unstable();
        if recorded != [EVENTS_OBJECT_PATH, SUMMARY_OBJECT_PATH] {
            return Err("the objects are not exactly the ledger and its summary".to_string());
        }
        if manifest
            .objects
            .iter()
            .any(|object| object.role != ObjectRole::Normalized)
        {
            return Err("every replay object is normalized output".to_string());
        }
        Ok(manifest)
    }

    pub fn key(&self) -> String {
        manifest_key(&self.generation)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn decimal(text: &str) -> Decimal {
        Decimal::parse(text).unwrap()
    }

    #[test]
    fn decimals_parse_render_and_compare_exactly() {
        for (text, coefficient, scale, rendered) in [
            ("0", 0, 0, "0"),
            ("1000", 1000, 0, "1000"),
            ("9.50", 950, 2, "9.50"),
            ("-0.10", -10, 2, "-0.10"),
            ("0.000000000000000001", 1, 18, "0.000000000000000001"),
            ("-5280.72", -528072, 2, "-5280.72"),
        ] {
            let value = decimal(text);
            assert_eq!(
                (value.coefficient(), value.scale()),
                (coefficient, scale),
                "{text}"
            );
            assert_eq!(value.to_string(), rendered);
            assert_eq!(
                serde_json::to_string(&value).unwrap(),
                format!("\"{rendered}\"")
            );
            assert_eq!(
                serde_json::from_str::<Decimal>(&format!("\"{rendered}\"")).unwrap(),
                value
            );
        }
        assert_eq!(decimal("1.50").normalized(), decimal("1.5"));
        assert_eq!(
            decimal("1.5").compare(decimal("1.50")).unwrap(),
            Ordering::Equal
        );
        assert_eq!(
            decimal("1.5").compare(decimal("1.51")).unwrap(),
            Ordering::Less
        );
        assert_eq!(decimal("1.5").rescale(2).unwrap().to_string(), "1.50");
        assert_eq!(
            decimal("9.60")
                .checked_add(decimal("0.4"))
                .unwrap()
                .to_string(),
            "10.00"
        );
        assert_eq!(
            decimal("19")
                .checked_sub(decimal("9.60"))
                .unwrap()
                .to_string(),
            "9.40"
        );
        assert_eq!(
            decimal("2.5")
                .checked_mul(decimal("0.20"))
                .unwrap()
                .to_string(),
            "0.5"
        );
        for text in ["1.2.3", "abc", "0.0000000000000000001", "", "-"] {
            assert!(Decimal::parse(text).is_err(), "{text}");
        }
        assert!(
            decimal("1.005")
                .rescale(2)
                .unwrap_err()
                .contains("loses precision")
        );
        let max = decimal("170141183460469231731687303715884105727");
        assert!(
            max.checked_add(decimal("1"))
                .unwrap_err()
                .contains("overflows")
        );
        assert!(max.rescale(1).unwrap_err().contains("overflows"));
        let min = Decimal::zero(0)
            .checked_sub(max)
            .unwrap()
            .checked_sub(decimal("1"))
            .unwrap();
        assert_eq!(min.coefficient(), i128::MIN);
        assert_eq!(
            Decimal::parse(&min.to_string()).unwrap(),
            min,
            "the whole range round-trips"
        );
        assert!(
            decimal("0.000000000001")
                .checked_mul(decimal("0.0000000001"))
                .unwrap_err()
                .contains("fraction digits")
        );
    }

    #[test]
    fn basis_points_project_to_ten_places_with_ties_to_even() {
        assert_eq!(basis_points_text(1070, 1_806_690).unwrap(), "5.9224327361");
        assert_eq!(
            basis_points_text(-2_690, 1_860_940).unwrap(),
            "-14.4550603458"
        );
        assert_eq!(basis_points_text(0, 5).unwrap(), "0.0000000000");
        assert_eq!(basis_points_text(1, 8).unwrap(), "1250.0000000000");
        assert_eq!(
            basis_points_text(1, 80_000_000_000_000).unwrap(),
            "0.0000000001"
        );
        assert_eq!(
            basis_points_text(3, 80_000_000_000_000).unwrap(),
            "0.0000000004"
        );
        assert_eq!(
            basis_points_text(5, 80_000_000_000_000).unwrap(),
            "0.0000000006"
        );
        assert!(
            basis_points_text(1, 0)
                .unwrap_err()
                .contains("zero entry price")
        );
    }

    fn contract() -> ContractTerms {
        ContractTerms {
            id: "c".into(),
            direction: Direction::Buy,
            duration_micros: 1,
            currency: Currency::try_from("u".to_string()).unwrap(),
            stake: decimal("10"),
            quoted_cost: decimal("9.50"),
            entry_fee: decimal("0.10"),
            win: Cashflow {
                gross_return: decimal("19"),
                terminal_fee: decimal("0.20"),
            },
            loss: Cashflow {
                gross_return: decimal("0"),
                terminal_fee: decimal("0.30"),
            },
            tie: Cashflow {
                gross_return: decimal("9.50"),
                terminal_fee: decimal("0"),
            },
            settlement: Settlement {
                rule: SettlementRule::PriceAtDueV1,
                max_settlement_delay_micros: 0,
                max_tick_gap_micros: 0,
            },
        }
    }

    #[test]
    fn contract_reserves_and_worst_losses_follow_the_cashflow_table() {
        let contract = contract();
        assert_eq!(contract.purchase().unwrap().to_string(), "9.60");
        assert_eq!(contract.terminal_reserve().unwrap().to_string(), "0.30");
        assert_eq!(contract.reservation().unwrap().to_string(), "9.90");
        assert_eq!(contract.worst_loss().unwrap().to_string(), "9.90");
        assert_eq!(contract.winning_net().unwrap().to_string(), "9.20");
        let envelope = Envelope {
            max_purchase_cost: decimal("9.5"),
            max_entry_fee: decimal("0.1"),
            max_win_terminal_fee: decimal("0.2"),
            max_loss_terminal_fee: decimal("0.3"),
            max_tie_terminal_fee: decimal("0"),
            min_winning_net_return: decimal("9.2"),
            settlement_rule: SettlementRule::PriceAtDueV1,
        };
        assert!(envelope.admits(&contract).unwrap(), "equality is allowed");
        let mut worse = contract.clone();
        worse.win.gross_return = decimal("18.99");
        assert!(!envelope.admits(&worse).unwrap());
        let mut rescaled = envelope.clone();
        rescaled.max_purchase_cost = decimal("9.500");
        assert_eq!(
            deployment_identity("logic", &contract, &envelope),
            deployment_identity("logic", &contract, &rescaled),
            "equivalent money values share one deployment identity"
        );
        let mut other = envelope.clone();
        other.max_purchase_cost = decimal("9.51");
        assert_ne!(
            deployment_identity("logic", &contract, &envelope),
            deployment_identity("logic", &contract, &other)
        );
    }

    #[test]
    fn path_metrics_keep_the_earliest_extremum_and_order_first_events() {
        let mut path = PathMetrics::new(100);
        for (time, movement) in [(101, 5), (102, 5), (103, -2), (104, -2), (105, 3)] {
            path.observe(time, movement);
        }
        assert_eq!(
            path,
            PathMetrics {
                final_move_units: 3,
                max_favorable_units: 5,
                max_adverse_units: 2,
                max_favorable_time_micros: 101,
                max_adverse_time_micros: 103,
                first_favorable_time_micros: Some(101),
                first_adverse_time_micros: Some(103),
                favorable_before_adverse: true,
                adverse_before_favorable: false,
            }
        );
        let mut flat = PathMetrics::new(100);
        flat.observe(101, 0);
        assert_eq!(flat.max_favorable_time_micros, 100);
        assert_eq!(flat.first_favorable_time_micros, None);
        assert!(!flat.favorable_before_adverse && !flat.adverse_before_favorable);
        assert_eq!(signed_move(10, 4, Direction::Sell).unwrap(), 6);
        assert!(signed_move(0, i64::MIN, Direction::Buy).is_err());
        assert!(signed_move(i64::MAX, -1, Direction::Buy).is_err());
    }

    #[test]
    fn identities_canonicalize_conditions_and_exclude_economics() {
        let stream = StreamKey {
            duration_seconds: 30,
            offset_seconds: 15,
        };
        let condition = |output: &str, threshold: Threshold| Condition {
            stream,
            output: output.to_string(),
            comparator: Comparator::Eq,
            threshold,
        };
        let a = StrategySpec {
            id: "a".into(),
            plan_identity: "plan".into(),
            base_stream: stream,
            conditions: vec![
                condition("x", Threshold::Text("1".into())),
                condition("y", Threshold::Number(2.0)),
                condition("x", Threshold::Text("1".into())),
            ],
            repair: Vec::new(),
        };
        let b = StrategySpec {
            id: "b".into(),
            conditions: vec![
                condition("y", Threshold::Number(2.0)),
                condition("x", Threshold::Text("1".into())),
            ],
            ..a.clone()
        };
        assert_eq!(signal_logic_identity(&a), signal_logic_identity(&b));
        let mut c = b.clone();
        c.conditions[0].threshold = Threshold::Number(2.5);
        assert_ne!(signal_logic_identity(&a), signal_logic_identity(&c));
        assert_eq!(
            serde_json::from_str::<Threshold>("60").unwrap(),
            Threshold::Number(60.0),
            "an integer literal is a number"
        );
    }
}

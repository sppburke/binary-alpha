//! The one chronological owner of strategy evaluation, admission, settlement, accounting, and
//! risk: `Engine`. Historical replay, research, and live adapters hand it observations in
//! availability order and it returns canonical `FinancialEvent`s; applying those events back
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

    /// Parses plain decimal text such as `-9.50` exactly; the scale is the fraction length.
    pub fn parse(text: &str) -> Result<Self, String> {
        let (negative, whole, fraction) = split_decimal(text)?;
        if fraction.len() > usize::from(MAX_SCALE) {
            return Err(format!(
                "`{text}` has {} fraction digits, more than {MAX_SCALE}",
                fraction.len()
            ));
        }
        let magnitude = format!("{whole}{fraction}")
            .parse::<i128>()
            .map_err(|_| format!("`{text}` overflows the exact money representation"))?;
        Ok(Self {
            coefficient: if negative { -magnitude } else { magnitude },
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
/// boolean.
#[derive(Debug, Clone, PartialEq)]
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

impl Serialize for Threshold {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            Self::Text(text) => serializer.serialize_str(text),
            Self::Number(value) => serializer.serialize_f64(*value),
            Self::Bool(value) => serializer.serialize_bool(*value),
        }
    }
}

impl<'de> Deserialize<'de> for Threshold {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum Raw {
            Bool(bool),
            Int(i64),
            Float(f64),
            Text(String),
        }
        Ok(match Raw::deserialize(deserializer)? {
            Raw::Bool(value) => Self::Bool(value),
            Raw::Int(value) => Self::Number(value as f64),
            Raw::Float(value) => Self::Number(value),
            Raw::Text(text) => Self::Text(text),
        })
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
/// entry fee, the exhaustive win, loss, and tie cashflows, and the settlement rule.
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
    fn purchase(&self) -> Result<Decimal, String> {
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
    fn reservation(&self) -> Result<Decimal, String> {
        self.purchase()?.checked_add(self.terminal_reserve()?)
    }

    /// `max(0, A + worst terminal)`, the worst unresolved loss.
    fn worst_loss(&self) -> Result<Decimal, String> {
        self.purchase()?
            .checked_add(self.worst_terminal()?)?
            .max(Decimal::zero(0))
    }

    /// `gross_payout - quoted_cost - entry_fee - win_terminal_fee`.
    fn winning_net(&self) -> Result<Decimal, String> {
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
/// envelope. Actual quotes belong to events, never to identities.
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
    hasher.update(serde_json::to_vec(envelope).expect("an envelope serializes"));
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
/// inputs. The first ledger event carries it, so restoration needs nothing else.
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
pub fn replay_generation_id(definition: &RunDefinition) -> String {
    let mut hasher = Sha256::new();
    hasher.update(REPLAY_GENERATION_DOMAIN_V1);
    hasher.update(definition.config_hash.as_bytes());
    hasher.update(b"\n");
    for instrument in &definition.instruments {
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

/// What a reconciliation proves about an open command.
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

impl Observation {
    fn external(&self) -> Option<(&str, &EventSource)> {
        match self {
            Self::Tick { .. } | Self::Row { .. } => None,
            Self::Accepted {
                command, source, ..
            }
            | Self::Rejected { command, source }
            | Self::NotSent { command, source }
            | Self::PossiblySent { command, source }
            | Self::Settlement {
                command, source, ..
            }
            | Self::Reconciliation {
                command, source, ..
            } => Some((command, source)),
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

    pub fn observe(&mut self, time_micros: i64, move_units: i64) {
        self.final_move_units = move_units;
        if move_units > self.max_favorable_units {
            self.max_favorable_units = move_units;
            self.max_favorable_time_micros = time_micros;
        }
        if move_units < 0 && -move_units > self.max_adverse_units {
            self.max_adverse_units = -move_units;
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
    /// An authoritative reconciliation resolved an open command; `account_blocked` is the block
    /// that remains on its account afterwards, if any.
    Reconciled {
        command: String,
        source: EventSource,
        resolution: Resolution,
        release: Decimal,
        debit: Decimal,
        credit: Decimal,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        profit: Option<Decimal>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        account_blocked: Option<String>,
    },
    /// An account pause began at the epoch drawdown.
    PauseStarted {
        account: String,
        until_micros: i64,
        drawdown: Decimal,
    },
    /// An account pause ended and the epoch peak reset.
    PauseEnded { account: String },
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

#[derive(Debug, Clone)]
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub blocked: Option<String>,
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
    Accepted,
    PossiblySent,
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
    entry_time_micros: Option<i64>,
    entry_price_units: Option<i64>,
    due_time_micros: Option<i64>,
    unresolved: Option<UnresolvedReason>,
    #[serde(skip)]
    path: Option<PathMetrics>,
}

/// Signal dispositions, outcomes, open obligations, and completed profit of one group.
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
    pub profit: BTreeMap<String, Decimal>,
}

impl Group {
    fn add_profit(&mut self, currency: &Currency, profit: Decimal) -> Result<(), String> {
        let entry = self
            .profit
            .entry(currency.to_string())
            .or_insert_with(|| Decimal::zero(profit.scale()));
        *entry = entry.checked_add(profit)?;
        Ok(())
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

/// The reporting-currency projection of the portfolio and its converted drawdown.
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
    rates: Vec<Rate>,
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
    /// External identities already applied, with their payload rendering.
    externals: HashMap<String, String>,
    sequence: u64,
    now: i64,
    same_entry: HashSet<(usize, usize, i64)>,
    logic_seen: HashSet<(usize, usize, String)>,
    summary: Summary,
    events: Vec<FinancialEvent>,
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
        let rates = replay
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
                        Ok(CompiledCondition {
                            stream,
                            column,
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
                    blocked: None,
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
            now: i64::MIN,
            same_entry: HashSet::new(),
            logic_seen: HashSet::new(),
            summary,
            events: Vec::new(),
            definition,
        };
        let definition = Box::new(engine.definition.clone());
        engine.emit(decision_start, EventKind::RunDefinition { definition })?;
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
        engine.ok_or_else(|| "the ledger is empty".to_string())
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

    // ------------------------------------------------------------------------------------------
    // Stepping: observations, then decisions, at one availability time
    // ------------------------------------------------------------------------------------------

    /// Applies every observation available at `time` in source order, then evaluates the base
    /// rows installed by them at decision time `time`. Times never decrease.
    pub fn step(&mut self, time: i64, observations: Vec<Observation>) -> Result<(), String> {
        if time < self.now {
            return Err(format!(
                "observations at {} arrive after the engine reached {}",
                format_event_time_micros(time),
                format_event_time_micros(self.now)
            ));
        }
        if time != self.now {
            self.same_entry.clear();
            self.logic_seen.clear();
        }
        self.now = time;
        self.advance_pauses()?;
        let mut emitted: Vec<(usize, usize)> = Vec::new();
        for observation in observations {
            if let Some((command, source)) = observation.external() {
                if source.available_at_micros > time
                    || source.provider_time_micros > source.available_at_micros
                {
                    return Err(format!(
                        "external event `{}` for {command} is not available at {}",
                        source.id,
                        format_event_time_micros(time)
                    ));
                }
                let key = format!("{command}\n{}", source.id);
                let payload = format!("{observation:?}");
                match self.externals.get(&key) {
                    Some(seen) if *seen == payload => continue,
                    Some(_) => {
                        return Err(format!(
                            "external event `{}` for {command} arrived again with a different payload; reconciliation failed",
                            source.id
                        ));
                    }
                    None => {
                        self.externals.insert(key, payload);
                    }
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
                    let state = self
                        .instruments
                        .get_mut(instrument)
                        .and_then(|state| state.rows.get_mut(stream))
                        .ok_or_else(|| {
                            format!("no bound stream {stream} of instrument {instrument}")
                        })?;
                    *state = Some(RowState {
                        close_time_micros,
                        known_at_micros,
                        values,
                    });
                    if self.by_base.contains_key(&(instrument, stream)) {
                        emitted.push((instrument, stream));
                    }
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
                    self.require(&command, ObligationState::Sent)?;
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
                    let settlement_time_micros = source.provider_time_micros;
                    self.settle(
                        &command,
                        source,
                        settlement_time_micros,
                        settlement_price_units,
                        outcome,
                        gross_return,
                        terminal_fee,
                    )?;
                }
                Observation::Reconciliation {
                    command,
                    source,
                    resolution,
                } => self.observe_reconciliation(command, source, resolution)?,
            }
        }
        // Emitted base streams in frozen-plan order, then their bindings in configured order.
        emitted.sort_unstable();
        emitted.dedup();
        for (instrument, stream) in emitted {
            for binding in self.by_base[&(instrument, stream)].clone() {
                self.evaluate(binding)?;
            }
        }
        Ok(())
    }

    /// Marks every obligation still open after the last permitted observation as unresolved
    /// with its path so far.
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
        Ok(())
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
        state.tick = Some(TickState {
            provider_time_micros: time,
            price_units,
            gap_micros: previous.map(|previous| time - previous.provider_time_micros),
        });
        let tracked = std::mem::take(&mut state.tracked);
        let mut kept = Vec::with_capacity(tracked.len());
        for command in tracked {
            if self.drive(&command, previous, time, price_units)? {
                kept.push(command);
            }
        }
        self.instruments[instrument].tracked = kept;
        Ok(())
    }

    /// Updates one accepted obligation's path with a tick and settles or leaves it unresolved
    /// under `price_at_due_v1`. Returns whether the tick stream still drives it.
    fn drive(
        &mut self,
        command: &str,
        previous: Option<TickState>,
        time: i64,
        price_units: i64,
    ) -> Result<bool, String> {
        let Some(obligation) = self.obligations.get(command) else {
            return Ok(false);
        };
        let (Some(entry_time), Some(entry_price), Some(due), Some(mut path)) = (
            obligation.entry_time_micros,
            obligation.entry_price_units,
            obligation.due_time_micros,
            obligation.path,
        ) else {
            return Ok(false);
        };
        let contract =
            &self.definition.replay.contracts[self.bindings[obligation.binding].contract];
        let settlement = contract.settlement;
        let direction = contract.direction;
        // A gap into this tick that intersects the contract window is not settlement evidence;
        // the obligation stays open with the path observed before the gap.
        if let Some(previous) = previous
            && previous.provider_time_micros < due
            && time > entry_time
            && time - previous.provider_time_micros > settlement.max_tick_gap_micros
        {
            self.emit(
                self.now,
                EventKind::Unresolved {
                    command: command.to_string(),
                    reason: UnresolvedReason::Gap,
                    evidence: format!(
                        "a gap of {} microseconds from {} to {} exceeds {} inside the contract window",
                        time - previous.provider_time_micros,
                        format_event_time_micros(previous.provider_time_micros),
                        format_event_time_micros(time),
                        settlement.max_tick_gap_micros
                    ),
                    path: Some(path),
                },
            )?;
            return Ok(false);
        }
        let move_units = price_units
            .checked_sub(entry_price)
            .and_then(|delta| delta.checked_mul(direction.sign()))
            .ok_or_else(|| {
                format!("{command}: the move from {entry_price} to {price_units} overflows")
            })?;
        path.observe(time, move_units);
        self.obligations.get_mut(command).expect("present").path = Some(path);
        if time < due {
            return Ok(true);
        }
        if time - due > settlement.max_settlement_delay_micros {
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
            time,
            price_units,
            outcome,
            cashflow.gross_return,
            cashflow.terminal_fee,
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

    fn observe_accepted(
        &mut self,
        command: String,
        source: EventSource,
        entry_time_micros: i64,
        entry_price_units: i64,
        price_time_micros: i64,
    ) -> Result<(), String> {
        self.require(&command, ObligationState::Sent)?;
        let binding = &self.bindings[self.obligations[&command].binding];
        let contract = &self.definition.replay.contracts[binding.contract];
        let scale = self.accounts[binding.account].scale;
        let debit = contract.purchase()?.rescale(scale)?;
        let reservation = contract.terminal_reserve()?.rescale(scale)?;
        let due_time_micros = entry_time_micros
            .checked_add(contract.duration_micros)
            .ok_or("the due time overflows microseconds")?;
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
        self.require(&command, ObligationState::Sent)?;
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

    /// Emits the settlement record of an accepted obligation: the credit is the actual cashflow,
    /// a cashflow contradicting the frozen terms is a discrepancy, and a net terminal debit
    /// beyond the remaining reservation is a deficit. A configured pause may follow.
    #[allow(clippy::too_many_arguments)]
    fn settle(
        &mut self,
        command: &str,
        source: EventSource,
        settlement_time_micros: i64,
        settlement_price_units: i64,
        outcome: Outcome,
        gross_return: Decimal,
        terminal_fee: Decimal,
    ) -> Result<(), String> {
        let obligation = &self.obligations[command];
        let binding = obligation.binding;
        let account = self.bindings[binding].account;
        let contract = &self.definition.replay.contracts[self.bindings[binding].contract];
        let scale = self.accounts[account].scale;
        let expected = contract.cashflow(outcome);
        let discrepancy = gross_return.compare(expected.gross_return)? != Ordering::Equal
            || terminal_fee.compare(expected.terminal_fee)? != Ordering::Equal;
        let credit = gross_return.checked_sub(terminal_fee)?.rescale(scale)?;
        let profit = credit.checked_sub(obligation.paid_basis)?;
        let release = obligation.reservation;
        // The net terminal debit is `-credit`; beyond the remaining reservation it is a deficit.
        let shortfall = credit.checked_add(release)?;
        let deficit = shortfall
            .is_negative()
            .then(|| Decimal::zero(scale).checked_sub(shortfall))
            .transpose()?;
        let path = obligation.path.unwrap_or_else(|| {
            PathMetrics::new(
                obligation
                    .entry_time_micros
                    .unwrap_or(settlement_time_micros),
            )
        });
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
                credit,
                profit,
                release,
                discrepancy,
                deficit,
                path,
            },
        )?;
        self.maybe_pause(account, binding)
    }

    /// Starts the configured pause of an account whose epoch drawdown reached its threshold.
    fn maybe_pause(&mut self, account: usize, binding: usize) -> Result<(), String> {
        let Some(pause) = self.definition.replay.risk_policies[self.bindings[binding].policy].pause
        else {
            return Ok(());
        };
        let state = &self.accounts[account];
        if state.paused_until_micros.is_some() {
            return Ok(());
        }
        let drawdown = state.epoch_peak.checked_sub(state.completed_profit)?;
        if drawdown.compare(pause.drawdown)? == Ordering::Less {
            return Ok(());
        }
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

    fn observe_reconciliation(
        &mut self,
        command: String,
        source: EventSource,
        resolution: Resolution,
    ) -> Result<(), String> {
        let obligation = self
            .obligations
            .get(&command)
            .ok_or_else(|| format!("{command} is not an open obligation"))?;
        let binding = obligation.binding;
        let account = self.bindings[binding].account;
        let contract = &self.definition.replay.contracts[self.bindings[binding].contract];
        let scale = self.accounts[account].scale;
        let purchase = contract.purchase()?.rescale(scale)?;
        let zero = Decimal::zero(scale);
        let (release, debit, credit, profit) = match &resolution {
            Resolution::NotSent => {
                if obligation.state == ObligationState::Accepted {
                    return Err(format!("{command} was accepted and cannot be not sent"));
                }
                (obligation.reservation, zero, zero, None)
            }
            Resolution::Accepted { .. } => {
                if obligation.state == ObligationState::Accepted {
                    return Err(format!("{command} is already accepted"));
                }
                (
                    obligation
                        .reservation
                        .checked_sub(contract.terminal_reserve()?.rescale(scale)?)?,
                    purchase,
                    zero,
                    None,
                )
            }
            Resolution::Settled {
                gross_return,
                terminal_fee,
                ..
            } => {
                let credit = gross_return.checked_sub(*terminal_fee)?.rescale(scale)?;
                let (debit, basis) = if obligation.state == ObligationState::Accepted {
                    (zero, obligation.paid_basis)
                } else {
                    (purchase, purchase)
                };
                (
                    obligation.reservation,
                    debit,
                    credit,
                    Some(credit.checked_sub(basis)?),
                )
            }
        };
        let account_blocked = self
            .obligations
            .iter()
            .find(|(other, obligation)| {
                **other != command
                    && self.bindings[obligation.binding].account == account
                    && obligation.state == ObligationState::PossiblySent
            })
            .map(|(other, _)| format!("{other} possibly sent; reconciliation required"))
            .or_else(|| {
                self.accounts[account]
                    .blocked
                    .clone()
                    .filter(|reason| !reason.starts_with(&format!("{command} ")))
            });
        self.emit(
            self.now,
            EventKind::Reconciled {
                command,
                source,
                resolution,
                release,
                debit,
                credit,
                profit,
                account_blocked,
            },
        )?;
        self.maybe_pause(account, binding)
    }

    // ------------------------------------------------------------------------------------------
    // Decisions
    // ------------------------------------------------------------------------------------------

    /// Whether one condition holds against the instrument's latest rows for a base row closing at
    /// `base_close`: a required latest row that is missing, that closes after the base row, or
    /// whose value is unavailable fails the condition.
    fn holds(&self, instrument: usize, base_close: i64, condition: &CompiledCondition) -> bool {
        let Some(row) = &self.instruments[instrument].rows[condition.stream] else {
            return false;
        };
        if row.close_time_micros > base_close {
            return false;
        }
        let Some(value) = &row.values[condition.column] else {
            return false;
        };
        let spec = &self.definition.instruments[instrument].streams[condition.stream].columns
            [condition.column];
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
        let disposition = if first_only && !self.same_entry.insert(same_entry) {
            Disposition::SameEntryDuplicate
        } else if deduplicate && !self.logic_seen.insert(logic) {
            Disposition::DuplicateLogic
        } else if !self.strategies[binding.strategy]
            .repair
            .iter()
            .all(|condition| self.holds(binding.instrument, close, condition))
        {
            Disposition::RepairBlocked
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
        };
        self.emit(self.now, event)
    }

    /// The remaining admission checks in order: quote presence and freshness, entry continuity,
    /// account pause and block, the envelope, every capacity scope, cash, and unresolved-loss
    /// limits. Equality with a bound or a maximum is permitted.
    fn admit(
        &self,
        binding_index: usize,
        close: i64,
        reservation: Decimal,
    ) -> Result<Disposition, String> {
        let binding = &self.bindings[binding_index];
        let contract = &self.definition.replay.contracts[binding.contract];
        let policy = &self.definition.replay.risk_policies[binding.policy];
        let account = &self.accounts[binding.account];
        let Some(quote) = self.instruments[binding.instrument].tick else {
            return Ok(Disposition::NoQuote);
        };
        if self.now - close > policy.max_feature_age_micros {
            return Ok(Disposition::StaleFeature);
        }
        if self.now - quote.provider_time_micros > policy.max_quote_age_micros {
            return Ok(Disposition::StaleQuote);
        }
        if quote
            .gap_micros
            .is_some_and(|gap| gap > contract.settlement.max_tick_gap_micros)
        {
            return Ok(Disposition::GapAtEntry);
        }
        if account.paused_until_micros.is_some() {
            return Ok(Disposition::AccountPaused);
        }
        if account.blocked.is_some() {
            return Ok(Disposition::AccountBlocked);
        }
        if !self.definition.replay.bindings[binding_index]
            .envelope
            .admits(contract)?
        {
            return Ok(Disposition::QuoteRejected);
        }
        let over = |count: u32, limit: Option<u32>| limit.is_some_and(|limit| count + 1 > limit);
        let count = |map: &HashMap<usize, u32>, key: &usize| map.get(key).copied().unwrap_or(0);
        if over(
            count(&self.open_by_binding, &binding_index),
            policy.max_open_per_strategy,
        ) {
            return Ok(Disposition::CapacityStrategy);
        }
        if over(
            self.open_by_duration
                .get(&contract.duration_micros)
                .copied()
                .unwrap_or(0),
            policy.max_open_per_duration,
        ) {
            return Ok(Disposition::CapacityDuration);
        }
        if over(
            count(&self.open_by_instrument, &binding.instrument),
            policy.max_open_per_instrument,
        ) {
            return Ok(Disposition::CapacityInstrument);
        }
        if over(account.open, policy.max_open_per_account) {
            return Ok(Disposition::CapacityAccount);
        }
        if over(self.open_total, policy.max_open_total) {
            return Ok(Disposition::CapacityTotal);
        }
        if account.available()?.compare(reservation)? == Ordering::Less {
            return Ok(Disposition::InsufficientCash);
        }
        let worst = contract.worst_loss()?.rescale(account.scale)?;
        if let Some(limit) = policy.max_unresolved_loss_per_account
            && account
                .unresolved_loss
                .checked_add(worst)?
                .compare(limit.rescale(account.scale)?)?
                == Ordering::Greater
        {
            return Ok(Disposition::UnresolvedLossAccount);
        }
        if let Some(limit) = policy.max_unresolved_loss_total {
            let mut total = match self.convert(worst, &account.currency) {
                Ok(converted) => converted.amount,
                Err(_) => return Ok(Disposition::ConversionUnavailable),
            };
            for other in &self.accounts {
                match self.convert(other.unresolved_loss, &other.currency) {
                    Ok(converted) => total = total.checked_add(converted.amount)?,
                    Err(_) => return Ok(Disposition::ConversionUnavailable),
                }
            }
            if total.compare(limit)? == Ordering::Greater {
                return Ok(Disposition::UnresolvedLossTotal);
            }
        }
        Ok(Disposition::Admitted)
    }

    // ------------------------------------------------------------------------------------------
    // The one event-application function
    // ------------------------------------------------------------------------------------------

    /// Sequences, applies, and records one transition. Generation and restoration both come
    /// through here, so an illegal transition fails identically in both.
    fn emit(&mut self, time_micros: i64, kind: EventKind) -> Result<(), String> {
        let event = FinancialEvent {
            sequence: self.sequence,
            time_micros,
            kind,
        };
        self.apply(&event)?;
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

    fn apply(&mut self, event: &FinancialEvent) -> Result<(), String> {
        if event.sequence != self.sequence {
            return Err(format!(
                "record {} applied out of sequence at {}",
                event.sequence, self.sequence
            ));
        }
        match &event.kind {
            EventKind::RunDefinition { definition } => {
                if event.sequence != 0 || **definition != self.definition {
                    return Err("the definition record does not describe this engine".to_string());
                }
            }
            EventKind::Signal {
                binding,
                disposition,
                command,
                reservation,
                split,
                close_time_micros,
                ..
            } => {
                let index = *self
                    .binding_index
                    .get(binding)
                    .ok_or_else(|| format!("unknown binding `{binding}`"))?;
                let compiled = self.bindings[index].clone();
                let contract = &self.definition.replay.contracts[compiled.contract];
                let scale = self.accounts[compiled.account].scale;
                let admitted = *disposition == Disposition::Admitted;
                if admitted {
                    let (Some(command), Some(reservation)) = (command, reservation) else {
                        return Err(
                            "an admitted signal names its command and reservation".to_string()
                        );
                    };
                    if self.obligations.contains_key(command) {
                        return Err(format!("{command} is already open"));
                    }
                    if *command != format!("{}/{close_time_micros}", compiled.id)
                        || reservation.compare(contract.reservation()?.rescale(scale)?)?
                            != Ordering::Equal
                    {
                        return Err(format!(
                            "{command} is not the command and reservation of its signal"
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
                            unresolved: None,
                            path: None,
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
            EventKind::Accepted {
                command,
                entry_time_micros,
                entry_price_units,
                due_time_micros,
                debit,
                reservation,
                ..
            } => {
                let obligation = self.open_obligation(command)?;
                if obligation.state != ObligationState::Sent {
                    return Err(format!("{command} is {:?}, not sent", obligation.state));
                }
                let binding = obligation.binding;
                let released = obligation.reservation;
                obligation.state = ObligationState::Accepted;
                obligation.reservation = *reservation;
                obligation.paid_basis = *debit;
                obligation.entry_time_micros = Some(*entry_time_micros);
                obligation.entry_price_units = Some(*entry_price_units);
                obligation.due_time_micros = Some(*due_time_micros);
                obligation.path = Some(PathMetrics::new(*entry_time_micros));
                let split = obligation.split.clone();
                let account = &mut self.accounts[self.bindings[binding].account];
                account.reserved = account
                    .reserved
                    .checked_sub(released)?
                    .checked_add(*reservation)?;
                account.cash = account.cash.checked_sub(*debit)?;
                account.paid_basis = account.paid_basis.checked_add(*debit)?;
                let instrument = self.bindings[binding].instrument;
                self.instruments[instrument].tracked.push(command.clone());
                let key = self.keys(binding, split.as_deref());
                for group in self.groups(&key) {
                    group.accepted += 1;
                }
            }
            EventKind::Released {
                command, release, ..
            } => {
                let obligation = self
                    .obligations
                    .remove(command)
                    .ok_or_else(|| format!("{command} is not an open obligation"))?;
                if obligation.state != ObligationState::Sent || obligation.reservation != *release {
                    return Err(format!("{command} cannot release {release}"));
                }
                let binding = obligation.binding;
                let account = &mut self.accounts[self.bindings[binding].account];
                account.reserved = account.reserved.checked_sub(*release)?;
                account.unresolved_loss =
                    account.unresolved_loss.checked_sub(obligation.worst_loss)?;
                self.open_delta(binding, -1);
                let key = self.keys(binding, obligation.split.as_deref());
                for group in self.groups(&key) {
                    group.released += 1;
                    group.close(obligation.unresolved.is_some());
                }
            }
            EventKind::PossiblySent { command, .. } => {
                let obligation = self.open_obligation(command)?;
                if obligation.state != ObligationState::Sent {
                    return Err(format!("{command} is {:?}, not sent", obligation.state));
                }
                obligation.state = ObligationState::PossiblySent;
                obligation.unresolved = Some(UnresolvedReason::PossiblySent);
                let binding = obligation.binding;
                let split = obligation.split.clone();
                self.accounts[self.bindings[binding].account].blocked =
                    Some(format!("{command} possibly sent; reconciliation required"));
                let key = self.keys(binding, split.as_deref());
                for group in self.groups(&key) {
                    group.unresolved += 1;
                }
            }
            EventKind::Settled {
                command,
                credit,
                profit,
                release,
                discrepancy,
                deficit,
                outcome,
                ..
            } => {
                let obligation = self
                    .obligations
                    .remove(command)
                    .ok_or_else(|| format!("{command} is not an open obligation"))?;
                if obligation.state != ObligationState::Accepted
                    || obligation.reservation != *release
                    || credit
                        .checked_sub(obligation.paid_basis)?
                        .compare(*profit)?
                        != Ordering::Equal
                {
                    return Err(format!(
                        "{command} settlement postings disagree with its obligation"
                    ));
                }
                let binding = obligation.binding;
                let account = &mut self.accounts[self.bindings[binding].account];
                let currency = account.currency.clone();
                account.reserved = account.reserved.checked_sub(*release)?;
                account.paid_basis = account.paid_basis.checked_sub(obligation.paid_basis)?;
                account.unresolved_loss =
                    account.unresolved_loss.checked_sub(obligation.worst_loss)?;
                account.cash = account.cash.checked_add(*credit)?;
                account.complete(*profit)?;
                if *discrepancy || deficit.is_some() {
                    account.blocked = Some(format!(
                        "{command} settled with a cashflow contradicting its frozen terms; reconciliation required"
                    ));
                }
                self.open_delta(binding, -1);
                let key = self.keys(binding, obligation.split.as_deref());
                for group in self.groups(&key) {
                    group.close(obligation.unresolved.is_some());
                    group.outcome(*outcome);
                    group.add_profit(&currency, *profit)?;
                }
                self.observe_portfolio(event.time_micros)?;
            }
            EventKind::Unresolved {
                command, reason, ..
            } => {
                let obligation = self.open_obligation(command)?;
                if obligation.unresolved.is_some() {
                    return Err(format!("{command} is already unresolved"));
                }
                obligation.unresolved = Some(*reason);
                let binding = obligation.binding;
                let split = obligation.split.clone();
                let key = self.keys(binding, split.as_deref());
                for group in self.groups(&key) {
                    group.unresolved += 1;
                }
            }
            EventKind::Reconciled {
                command,
                resolution,
                release,
                debit,
                credit,
                profit,
                account_blocked,
                ..
            } => {
                let obligation = self
                    .obligations
                    .remove(command)
                    .ok_or_else(|| format!("{command} is not an open obligation"))?;
                let binding = obligation.binding;
                let account_index = self.bindings[binding].account;
                let unresolved = obligation.unresolved.is_some();
                let key = self.keys(binding, obligation.split.as_deref());
                let account = &mut self.accounts[account_index];
                let currency = account.currency.clone();
                account.reserved = account.reserved.checked_sub(*release)?;
                account.cash = account.cash.checked_sub(*debit)?.checked_add(*credit)?;
                match resolution {
                    Resolution::NotSent => {
                        if obligation.state == ObligationState::Accepted {
                            return Err(format!("{command} was accepted and cannot be not sent"));
                        }
                        account.unresolved_loss =
                            account.unresolved_loss.checked_sub(obligation.worst_loss)?;
                        self.open_delta(binding, -1);
                        for group in self.groups(&key) {
                            group.released += 1;
                            group.close(unresolved);
                        }
                    }
                    Resolution::Accepted {
                        entry_time_micros,
                        entry_price_units,
                        ..
                    } => {
                        if obligation.state == ObligationState::Accepted {
                            return Err(format!("{command} is already accepted"));
                        }
                        account.paid_basis = account.paid_basis.checked_add(*debit)?;
                        let duration = self.definition.replay.contracts
                            [self.bindings[binding].contract]
                            .duration_micros;
                        let mut restored = obligation;
                        restored.state = ObligationState::Accepted;
                        restored.reservation = restored.reservation.checked_sub(*release)?;
                        restored.paid_basis = *debit;
                        restored.entry_time_micros = Some(*entry_time_micros);
                        restored.entry_price_units = Some(*entry_price_units);
                        restored.due_time_micros = Some(
                            entry_time_micros
                                .checked_add(duration)
                                .ok_or("the due time overflows microseconds")?,
                        );
                        restored.unresolved = None;
                        restored.path = Some(PathMetrics::new(*entry_time_micros));
                        let instrument = self.bindings[binding].instrument;
                        self.obligations.insert(command.clone(), restored);
                        self.instruments[instrument].tracked.push(command.clone());
                        for group in self.groups(&key) {
                            group.accepted += 1;
                            if unresolved {
                                group.unresolved -= 1;
                            }
                        }
                    }
                    Resolution::Settled { outcome, .. } => {
                        let Some(profit) = profit else {
                            return Err(format!(
                                "{command} reconciled settlement names its profit"
                            ));
                        };
                        account.paid_basis =
                            account.paid_basis.checked_sub(obligation.paid_basis)?;
                        account.unresolved_loss =
                            account.unresolved_loss.checked_sub(obligation.worst_loss)?;
                        account.complete(*profit)?;
                        self.open_delta(binding, -1);
                        for group in self.groups(&key) {
                            group.close(unresolved);
                            group.outcome(*outcome);
                            group.add_profit(&currency, *profit)?;
                        }
                        self.observe_portfolio(event.time_micros)?;
                    }
                }
                self.accounts[account_index].blocked = account_blocked.clone();
            }
            EventKind::PauseStarted {
                account,
                until_micros,
                ..
            } => {
                let state = self
                    .accounts
                    .iter_mut()
                    .find(|state| state.id == *account)
                    .ok_or_else(|| format!("unknown account `{account}`"))?;
                if state.paused_until_micros.is_some() {
                    return Err(format!("account `{account}` is already paused"));
                }
                state.paused_until_micros = Some(*until_micros);
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
    /// settlement; an unavailable rate leaves the observation unavailable, never native history.
    fn observe_portfolio(&mut self, at: i64) -> Result<(), String> {
        let replay = &self.definition.replay;
        let scale = replay.reporting_scale;
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
            let (Ok(converted_equity), Ok(converted_loss)) = (
                convert(account.settled_equity()?),
                convert(account.unresolved_loss),
            ) else {
                let reporting = &mut self.summary.reporting;
                reporting.unavailable_observations += 1;
                reporting.settled_equity = None;
                reporting.unresolved_loss = None;
                return Ok(());
            };
            equity = equity.checked_add(converted_equity.amount)?;
            loss = loss.checked_add(converted_loss.amount)?;
            used.extend(converted_equity.rate);
            used.extend(converted_loss.rate);
        }
        let reporting = &mut self.summary.reporting;
        reporting.observations += 1;
        reporting.settled_equity = Some(equity);
        reporting.unresolved_loss = Some(loss);
        reporting.used_rates.extend(used);
        let peak = match reporting.peak_equity {
            Some(peak) => peak.max(equity)?,
            None => equity,
        };
        let drawdown = peak.checked_sub(equity)?;
        reporting.peak_equity = Some(peak);
        reporting.max_drawdown = Some(match reporting.max_drawdown {
            Some(current) => current.max(drawdown)?,
            None => drawdown,
        });
        Ok(())
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
    /// a permitted role, exactly the ledger and summary objects, and content-addressed objects
    /// with unique paths.
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
        assert!(
            decimal("170141183460469231731687303715884105727")
                .checked_add(decimal("1"))
                .unwrap_err()
                .contains("overflows")
        );
        assert!(
            decimal("170141183460469231731687303715884105727")
                .rescale(1)
                .unwrap_err()
                .contains("overflows")
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
        // 1 / 8 × 10000 = 1250 exactly; 1 / 16 = 625 exactly; a half unit at the eleventh
        // place rounds to the even tenth digit in both directions.
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

    #[test]
    fn contract_reserves_and_worst_losses_follow_the_cashflow_table() {
        let contract = ContractTerms {
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
        };
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
    }

    #[test]
    fn path_metrics_keep_the_earliest_extremum_and_order_first_events() {
        let mut path = PathMetrics::new(100);
        path.observe(101, 5);
        path.observe(102, 5);
        path.observe(103, -2);
        path.observe(104, -2);
        path.observe(105, 3);
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
    }
}

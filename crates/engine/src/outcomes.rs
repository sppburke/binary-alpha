//! Future-only binary-expiry outcomes: the historical labels of one feature generation's decision
//! rows against the tick generation it was computed from.
//!
//! An outcome is a research label, never a decision-time feature. For each decision row and each
//! configured expiry the label names the entry tick (the first tick at or after the row's logical
//! reference time, its candle close), the due time (the entry tick's time plus the expiry), the
//! settlement tick (the first tick at or after the due time), the first reason in precedence order
//! that makes the cell invalid, and, for a valid cell, which direction won or that the prices
//! tied. `docs/contracts.md`, section "Outcomes", is the normative description.

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::config::{ManifestUri, Outcomes};
use crate::dataset::{
    DatasetRole, ObjectRecord, ObjectRole, PriceRepresentation, TimeUnit, manifest_key,
    validate_objects,
};
use crate::market::{
    BrokerId, InstrumentId, PriceScale, ProviderSymbol, Tick, TickSequence,
    format_event_time_micros, parse_price_units, split_decimal,
};

/// The manifest schema written and accepted by this checkout.
pub const OUTCOME_SCHEMA_VERSION: u32 = 1;

/// The `kind` an outcome ready manifest declares.
pub const OUTCOME_MANIFEST_KIND: &str = "outcome_generation";

/// The stored entry or settlement index of a cell whose tick does not exist.
pub const MISSING_INDEX: u32 = u32::MAX;

/// The clock every stored reference time carries: the decision row's candle close, the logical
/// decision clock of Phase 04, which is distinct from the row's actual availability.
pub const REFERENCE_CLOCK: &str = "feature_row_close_time";

/// The shared tick arrays of a generation: every provider event time and every price, in order.
pub const TICK_TIME_OBJECT_PATH: &str = "ticks/event_time_micros.bin";
pub const TICK_PRICE_OBJECT_PATH: &str = "ticks/price_units.bin";

/// Domain separator hashed before the generation identity; it names the outcome definition, so a
/// change to the label rule changes every identity.
const OUTCOME_GENERATION_DOMAIN_V1: &[u8] = b"binary-alpha outcome generation v1\n";

const MICROS_PER_MILLI: i64 = 1_000;
const MICROS_PER_SECOND: i64 = 1_000_000;

crate::string_enum! {
    /// Why a cell carries no result, in precedence order; an earlier reason is never overwritten by
    /// a later one. The stored code is the position in this order.
    InvalidReason "invalid_reason" {
        Valid => "valid",
        NoEntry => "no_entry",
        StaleEntry => "stale_entry",
        NoSettlement => "no_settlement",
        StaleSettlement => "stale_settlement",
        InternalGap => "internal_gap",
        FrozenRun => "frozen_run",
        TrueJump => "true_jump",
    }
}

impl InvalidReason {
    pub const fn code(self) -> u8 {
        self as u8
    }

    pub fn from_code(code: u8) -> Option<Self> {
        Self::ALL.get(usize::from(code)).copied()
    }
}

crate::string_enum! {
    /// The result of a valid cell: the settlement price above the entry price pays a buy, below it
    /// pays a sell, and an equal price is a tie, never an assumed loss.
    Outcome "outcome" {
        BuyWin => "buy_win",
        SellWin => "sell_win",
        Tie => "tie",
    }
}

/// The label rule in the units the builder compares: expiries in seconds, delay and gap
/// thresholds in microseconds, the frozen-run thresholds, and the exact jump threshold. Its
/// serialization is part of the generation identity.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct OutcomeRule {
    pub expiry_seconds: Vec<u32>,
    pub max_entry_delay_micros: i64,
    pub max_settlement_delay_micros: i64,
    pub max_tick_gap_micros: i64,
    pub true_jump_max_gap_micros: i64,
    /// Positive decimal basis points, compared exactly.
    pub true_jump_basis_points: String,
    pub frozen_min_ticks: u32,
    pub frozen_min_micros: i64,
}

impl OutcomeRule {
    /// Resolves the configured table, rejecting a millisecond threshold that overflows
    /// microseconds, and validates the rule.
    pub fn resolve(settings: &Outcomes) -> Result<Self, String> {
        if settings.frozen_min_ms == 0 {
            return Err("frozen_min_ms: must be positive".to_string());
        }
        let micros = |name: &str, millis: u64| {
            i64::try_from(millis)
                .ok()
                .and_then(|value| value.checked_mul(MICROS_PER_MILLI))
                .ok_or_else(|| format!("{name}: {millis} milliseconds overflow microseconds"))
        };
        let rule = Self {
            expiry_seconds: settings.expiry_seconds.clone(),
            max_entry_delay_micros: micros("max_entry_delay_ms", settings.max_entry_delay_ms)?,
            max_settlement_delay_micros: micros(
                "max_settlement_delay_ms",
                settings.max_settlement_delay_ms,
            )?,
            max_tick_gap_micros: micros("max_tick_gap_ms", settings.max_tick_gap_ms)?,
            true_jump_max_gap_micros: micros(
                "true_jump_max_gap_ms",
                settings.true_jump_max_gap_ms,
            )?,
            true_jump_basis_points: settings.true_jump_basis_points.clone(),
            frozen_min_ticks: settings.frozen_min_ticks,
            frozen_min_micros: micros("frozen_min_ms", settings.frozen_min_ms)?,
        };
        rule.validate()?;
        Ok(rule)
    }

    /// What every consumer relies on, whether the rule came from a configuration or from a
    /// published manifest: positive sorted unique expiries (the settlement search resumes
    /// across columns), non-negative delay and gap thresholds, positive frozen thresholds, and
    /// positive decimal basis points.
    pub fn validate(&self) -> Result<(), String> {
        if self.expiry_seconds.first().is_none_or(|&first| first == 0)
            || !self.expiry_seconds.windows(2).all(|pair| pair[0] < pair[1])
        {
            return Err(
                "expiry_seconds: at least one positive second, sorted and unique".to_string(),
            );
        }
        if [
            self.max_entry_delay_micros,
            self.max_settlement_delay_micros,
            self.max_tick_gap_micros,
            self.true_jump_max_gap_micros,
        ]
        .iter()
        .any(|&threshold| threshold < 0)
        {
            return Err("a delay or gap threshold must not be negative".to_string());
        }
        if self.frozen_min_ticks == 0 || self.frozen_min_micros <= 0 {
            return Err("frozen_min_ticks and frozen_min_micros must be positive".to_string());
        }
        basis_points(&self.true_jump_basis_points)
            .map(drop)
            .map_err(|reason| format!("true_jump_basis_points: {reason}"))
    }
}

/// Parses decimal basis-point text through the exact price boundary into `(coefficient, unit)`
/// with `coefficient / unit` the threshold, rejecting a value that is not positive.
fn basis_points(text: &str) -> Result<(i128, i128), String> {
    let (negative, _, fraction) = split_decimal(text)?;
    let scale = u8::try_from(fraction.len())
        .ok()
        .and_then(|digits| PriceScale::try_from(digits).ok())
        .ok_or_else(|| {
            format!(
                "`{text}` has {} fraction digits, more than {}",
                fraction.len(),
                PriceScale::MAX
            )
        })?;
    let coefficient = parse_price_units(text, scale)?;
    if negative || coefficient == 0 {
        return Err(format!("`{text}` must be positive"));
    }
    Ok((i128::from(coefficient), i128::from(scale.unit())))
}

/// Whether the move from `prior` to `current` reaches the threshold:
/// `10000 · |move| ≥ threshold · |prior|`, compared exactly and undefined after a zero price.
fn reaches(prior: i64, current: i64, (coefficient, unit): (i128, i128)) -> Result<bool, String> {
    if prior == 0 {
        return Ok(false);
    }
    let moved = (i128::from(current) - i128::from(prior)).abs();
    let left = moved
        .checked_mul(10_000)
        .and_then(|value| value.checked_mul(unit));
    let right = coefficient.checked_mul(i128::from(prior).abs());
    match (left, right) {
        (Some(left), Some(right)) => Ok(left >= right),
        _ => Err(format!(
            "the move from {prior} to {current} overflows the basis-point comparison"
        )),
    }
}

/// One tick generation with its quality flags folded into prefix counts, ready to label any
/// decision row under one rule. A row's labels depend only on the complete immutable tick
/// generation and the row's reference time, never on other rows or on how rows are chunked.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutcomeBuilder {
    rule: OutcomeRule,
    expiries_micros: Vec<i64>,
    times: Vec<i64>,
    prices: Vec<i64>,
    /// Prefix counts, one longer than the ticks: `gaps[i]` counts the flagged transitions into
    /// ticks before index `i`; likewise for jumps; `frozen[i]` counts flagged ticks before `i`.
    gaps: Vec<u32>,
    jumps: Vec<u32>,
    frozen: Vec<u32>,
}

/// The labels of a slice of decision rows: one entry index per row and, in row-major order with
/// one column per expiry, the settlement index and reason code of every cell.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Labels {
    pub entries: Vec<u32>,
    pub settlements: Vec<u32>,
    pub reasons: Vec<u8>,
}

/// One tick a cell refers to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TickRef {
    pub index: u32,
    pub event_time_micros: i64,
    pub price_units: i64,
}

/// The logical fields of one cell, reconstructed from its stored entry index, settlement index,
/// and reason. A missing entry or settlement leaves its dependent fields unavailable, and only a
/// valid cell carries an outcome.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Cell {
    pub entry: Option<TickRef>,
    pub due_time_micros: Option<i64>,
    pub settlement: Option<TickRef>,
    pub reason: InvalidReason,
    pub outcome: Option<Outcome>,
}

impl OutcomeBuilder {
    /// Folds the whole tick generation, in order, under a valid `rule`. Ticks are rejected when
    /// the arrays disagree in length, when they break the tick sequence rules, when a time span
    /// overflows, or when there are so many that an index would collide with [`MISSING_INDEX`].
    pub fn new(rule: OutcomeRule, times: Vec<i64>, prices: Vec<i64>) -> Result<Self, String> {
        rule.validate()?;
        if times.len() != prices.len() {
            return Err(format!(
                "{} tick times but {} prices",
                times.len(),
                prices.len()
            ));
        }
        if u32::try_from(times.len()).is_err() {
            return Err(format!(
                "{} ticks cannot be indexed in thirty-two bits without colliding with the missing index",
                times.len()
            ));
        }
        let threshold = basis_points(&rule.true_jump_basis_points)?;
        let count = times.len();
        let mut sequence = TickSequence::default();
        for (&event_time_micros, &price_units) in times.iter().zip(&prices) {
            sequence.accept(Tick {
                event_time_micros,
                price_units,
            })?;
        }
        let overflow = |index: usize| {
            format!(
                "the time span ending at tick {index} ({}) overflows microseconds",
                format_event_time_micros(times[index])
            )
        };
        let mut gaps = vec![0u32; count + 1];
        let mut jumps = vec![0u32; count + 1];
        let mut frozen = vec![0u32; count + 1];
        let mut run_start = 0;
        for index in 1..=count {
            if index < count {
                let delta = times[index]
                    .checked_sub(times[index - 1])
                    .ok_or_else(|| overflow(index))?;
                let gap = delta > rule.max_tick_gap_micros;
                let jump = delta <= rule.true_jump_max_gap_micros
                    && reaches(prices[index - 1], prices[index], threshold)?;
                gaps[index + 1] = gaps[index] + u32::from(gap);
                jumps[index + 1] = jumps[index] + u32::from(jump);
            }
            // A run of one unchanged price ends before `index`; the count or elapsed-time
            // threshold marks every member of the run.
            if index == count || prices[index] != prices[run_start] {
                let run_end = index - 1;
                let elapsed = times[run_end]
                    .checked_sub(times[run_start])
                    .ok_or_else(|| overflow(run_end))?;
                let flagged = run_end - run_start + 1 >= rule.frozen_min_ticks as usize
                    || elapsed >= rule.frozen_min_micros;
                for tick in run_start..index {
                    frozen[tick + 1] = frozen[tick] + u32::from(flagged);
                }
                run_start = index;
            }
        }
        Ok(Self {
            expiries_micros: rule
                .expiry_seconds
                .iter()
                .map(|&seconds| i64::from(seconds) * MICROS_PER_SECOND)
                .collect(),
            rule,
            times,
            prices,
            gaps,
            jumps,
            frozen,
        })
    }

    pub fn rule(&self) -> &OutcomeRule {
        &self.rule
    }

    pub fn times(&self) -> &[i64] {
        &self.times
    }

    pub fn prices(&self) -> &[i64] {
        &self.prices
    }

    /// Labels every reference time of `references`, one decision row each.
    pub fn label(&self, references: &[i64]) -> Result<Labels, String> {
        let count = self.times.len();
        let columns = self.expiries_micros.len();
        let mut labels = Labels {
            entries: Vec::with_capacity(references.len()),
            settlements: vec![MISSING_INDEX; references.len() * columns],
            reasons: vec![InvalidReason::Valid.code(); references.len() * columns],
        };
        for (row, &reference) in references.iter().enumerate() {
            let cells = row * columns..(row + 1) * columns;
            let entry = self.times.partition_point(|&time| time < reference);
            if entry == count {
                labels.entries.push(MISSING_INDEX);
                labels.reasons[cells].fill(InvalidReason::NoEntry.code());
                continue;
            }
            labels.entries.push(entry as u32);
            let entry_time = self.times[entry];
            let stale_entry = entry_time.checked_sub(reference).ok_or_else(|| {
                format!(
                    "the entry delay of the row at {} overflows microseconds",
                    format_event_time_micros(reference)
                )
            })? > self.rule.max_entry_delay_micros;
            // Expiries ascend, so every column's settlement search resumes where the last ended.
            let mut settlement = entry;
            for (column, &expiry) in self.expiries_micros.iter().enumerate() {
                let due = entry_time.checked_add(expiry).ok_or_else(|| {
                    format!(
                        "the due time after {} overflows microseconds",
                        format_event_time_micros(entry_time)
                    )
                })?;
                settlement += self.times[settlement..].partition_point(|&time| time < due);
                let reason = if stale_entry {
                    InvalidReason::StaleEntry
                } else if settlement == count {
                    InvalidReason::NoSettlement
                } else if self.times[settlement].checked_sub(due).ok_or_else(|| {
                    format!(
                        "the settlement delay after {} overflows microseconds",
                        format_event_time_micros(due)
                    )
                })? > self.rule.max_settlement_delay_micros
                {
                    InvalidReason::StaleSettlement
                } else if self.gaps[settlement + 1] > self.gaps[entry + 1] {
                    InvalidReason::InternalGap
                } else if self.frozen[settlement + 1] > self.frozen[entry] {
                    InvalidReason::FrozenRun
                } else if self.jumps[settlement + 1] > self.jumps[entry + 1] {
                    InvalidReason::TrueJump
                } else {
                    InvalidReason::Valid
                };
                if settlement < count {
                    labels.settlements[cells.start + column] = settlement as u32;
                }
                labels.reasons[cells.start + column] = reason.code();
            }
        }
        Ok(labels)
    }

    /// Reconstructs one cell of expiry column `column` from its stored values.
    pub fn cell(
        &self,
        entry: u32,
        column: usize,
        settlement: u32,
        reason: u8,
    ) -> Result<Cell, String> {
        let reason = InvalidReason::from_code(reason)
            .ok_or_else(|| format!("invalid reason code {reason}"))?;
        let expiry = *self
            .expiries_micros
            .get(column)
            .ok_or_else(|| format!("no expiry column {column}"))?;
        let tick = |index: u32| -> Result<Option<TickRef>, String> {
            if index == MISSING_INDEX {
                return Ok(None);
            }
            let position = index as usize;
            if position >= self.times.len() {
                return Err(format!(
                    "tick index {index} is beyond the {} ticks",
                    self.times.len()
                ));
            }
            Ok(Some(TickRef {
                index,
                event_time_micros: self.times[position],
                price_units: self.prices[position],
            }))
        };
        let entry = tick(entry)?;
        let settlement = tick(settlement)?;
        let due_time_micros = entry
            .map(|entry| {
                entry
                    .event_time_micros
                    .checked_add(expiry)
                    .ok_or_else(|| "the due time overflows microseconds".to_string())
            })
            .transpose()?;
        let outcome = match (reason, entry, settlement) {
            (InvalidReason::Valid, Some(entry), Some(settlement)) => {
                Some(match settlement.price_units.cmp(&entry.price_units) {
                    std::cmp::Ordering::Greater => Outcome::BuyWin,
                    std::cmp::Ordering::Less => Outcome::SellWin,
                    std::cmp::Ordering::Equal => Outcome::Tie,
                })
            }
            (InvalidReason::Valid, _, _) => {
                return Err("a valid cell names its entry and settlement ticks".to_string());
            }
            _ => None,
        };
        Ok(Cell {
            entry,
            due_time_micros,
            settlement,
            reason,
            outcome,
        })
    }
}

/// The identity of an outcome generation: the tick and feature generations it labels and the
/// resolved rule, under the outcome definition's domain.
pub fn outcome_generation_id(
    tick_generation: &str,
    feature_generation: &str,
    rule: &OutcomeRule,
) -> String {
    let mut hasher = Sha256::new();
    hasher.update(OUTCOME_GENERATION_DOMAIN_V1);
    for line in [tick_generation, feature_generation] {
        hasher.update(line.as_bytes());
        hasher.update(b"\n");
    }
    hasher.update(serde_json::to_vec(rule).expect("a rule serializes"));
    crate::hex(&hasher.finalize())
}

/// The four objects of one decision stream, in order: reference times, entry indices, the
/// settlement-index matrix, and the reason matrix.
pub fn stream_object_paths(duration_seconds: u32, offset_seconds: u32) -> [String; 4] {
    ["reference", "entry", "settlement", "reason"]
        .map(|kind| format!("{kind}/{duration_seconds}s_{offset_seconds}s.bin"))
}

/// Summary of one decision stream's labels and its ordered row mapping onto the feature rows.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct OutcomeStreamSummary {
    pub duration_seconds: u32,
    pub offset_seconds: u32,
    pub rows: u64,
    pub first_reference_time: Option<String>,
    pub last_reference_time: Option<String>,
}

/// The ready manifest of one outcome generation. Field order is the serialization order.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct OutcomeManifest {
    pub kind: String,
    pub schema_version: u32,
    pub generation: String,
    pub broker: BrokerId,
    pub provider_symbol: ProviderSymbol,
    pub instrument: String,
    pub role: DatasetRole,
    pub tick_generation: String,
    pub tick_manifest: ManifestUri,
    pub feature_generation: String,
    pub feature_manifest: ManifestUri,
    /// The frozen identity every labeled feature-row table carries.
    pub raw_identity: String,
    pub config_hash: String,
    pub code_revision: String,
    pub rule: OutcomeRule,
    pub reference_clock: String,
    pub time_unit: TimeUnit,
    pub price_representation: PriceRepresentation,
    pub missing_index: u32,
    pub tick_count: u64,
    pub streams: Vec<OutcomeStreamSummary>,
    pub objects: Vec<ObjectRecord>,
}

impl OutcomeManifest {
    pub fn to_json(&self) -> Vec<u8> {
        let mut bytes = serde_json::to_vec_pretty(self).expect("a manifest serializes");
        bytes.push(b'\n');
        bytes
    }

    /// Parses an outcome manifest and checks what every consumer relies on before it trusts a
    /// key: the kind, a generation that matches the inputs and rule, the declared conventions,
    /// exactly the two tick objects and four objects per stream, and content-addressed objects
    /// with unique clean paths.
    pub fn from_json(bytes: &[u8]) -> Result<Self, String> {
        let manifest: Self = serde_json::from_slice(bytes).map_err(|error| error.to_string())?;
        if manifest.kind != OUTCOME_MANIFEST_KIND {
            return Err(format!(
                "manifest kind `{}` is not `{OUTCOME_MANIFEST_KIND}`",
                manifest.kind
            ));
        }
        if manifest.schema_version != OUTCOME_SCHEMA_VERSION {
            return Err(format!(
                "unsupported manifest schema_version {}, expected {OUTCOME_SCHEMA_VERSION}",
                manifest.schema_version
            ));
        }
        let instrument = InstrumentId {
            broker: manifest.broker.clone(),
            provider_symbol: manifest.provider_symbol.clone(),
        };
        if manifest.instrument != instrument.to_string() {
            return Err(format!(
                "instrument `{}` is not `{instrument}`",
                manifest.instrument
            ));
        }
        if manifest.generation
            != outcome_generation_id(
                &manifest.tick_generation,
                &manifest.feature_generation,
                &manifest.rule,
            )
        {
            return Err(format!(
                "generation `{}` does not match the tick generation, feature generation, and rule",
                manifest.generation
            ));
        }
        if manifest.tick_manifest.generation() != manifest.tick_generation
            || manifest.feature_manifest.generation() != manifest.feature_generation
        {
            return Err(
                "the manifest references do not name the recorded tick and feature generations"
                    .to_string(),
            );
        }
        manifest.rule.validate()?;
        if u32::try_from(manifest.tick_count).is_err() {
            return Err(format!(
                "{} ticks cannot be indexed below the missing index",
                manifest.tick_count
            ));
        }
        if manifest.reference_clock != REFERENCE_CLOCK
            || manifest.time_unit != TimeUnit::Microsecond
            || !matches!(
                manifest.price_representation,
                PriceRepresentation::IntegerUnits { .. }
            )
            || manifest.missing_index != MISSING_INDEX
        {
            return Err(
                "the manifest does not declare the reference clock, microsecond times, integer price units, and missing index this checkout reads"
                    .to_string(),
            );
        }
        validate_objects(&manifest.objects)?;
        let mut expected = vec![
            TICK_TIME_OBJECT_PATH.to_string(),
            TICK_PRICE_OBJECT_PATH.to_string(),
        ];
        for summary in &manifest.streams {
            expected.extend(stream_object_paths(
                summary.duration_seconds,
                summary.offset_seconds,
            ));
        }
        let mut recorded: Vec<&str> = manifest
            .objects
            .iter()
            .map(|object| object.path.as_str())
            .collect();
        recorded.sort_unstable();
        expected.sort_unstable();
        if recorded != expected.iter().map(String::as_str).collect::<Vec<_>>() {
            return Err(
                "the objects are not exactly the two tick arrays and four objects per stream"
                    .to_string(),
            );
        }
        if manifest
            .objects
            .iter()
            .any(|object| object.role != ObjectRole::Normalized)
        {
            return Err("every outcome object is normalized output".to_string());
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

    fn rule(expiries: &[u32], basis_points: &str) -> OutcomeRule {
        OutcomeRule {
            expiry_seconds: expiries.to_vec(),
            max_entry_delay_micros: 2_000_000,
            max_settlement_delay_micros: 2_000_000,
            max_tick_gap_micros: 2_000_000,
            true_jump_max_gap_micros: 2_000_000,
            true_jump_basis_points: basis_points.to_string(),
            frozen_min_ticks: 3,
            frozen_min_micros: 5_000_000,
        }
    }

    fn seconds(values: &[f64]) -> Vec<i64> {
        values
            .iter()
            .map(|value| (value * 1_000_000.0).round() as i64)
            .collect()
    }

    fn reasons(builder: &OutcomeBuilder, reference: f64) -> Vec<InvalidReason> {
        let labels = builder.label(&seconds(&[reference])).unwrap();
        labels
            .reasons
            .iter()
            .map(|&code| InvalidReason::from_code(code).unwrap())
            .collect()
    }

    #[test]
    fn codes_follow_precedence_order() {
        use InvalidReason::*;
        let table = [
            (0, Valid),
            (1, NoEntry),
            (2, StaleEntry),
            (3, NoSettlement),
            (4, StaleSettlement),
            (5, InternalGap),
            (6, FrozenRun),
            (7, TrueJump),
        ];
        for (code, reason) in table {
            assert_eq!(reason.code(), code);
            assert_eq!(InvalidReason::from_code(code), Some(reason));
        }
        assert_eq!(InvalidReason::from_code(8), None);
    }

    #[test]
    fn entry_due_settlement_and_results_follow_the_rule() {
        // Ticks every second from 0 to 12 seconds; the price rises by one unit (a hundredth of
        // a basis point) per tick, then ties, then falls.
        let times = seconds(&[
            0.0, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 11.0, 12.0,
        ]);
        let prices: Vec<i64> = [0, 1, 2, 3, 4, 4, 4, 4, 3, 2, 1, 0, -1]
            .iter()
            .map(|step| 1_000_000 + step)
            .collect();
        let mut rule = rule(&[2, 3, 5, 9], "5");
        rule.frozen_min_ticks = 100;
        let builder = OutcomeBuilder::new(rule, times, prices).unwrap();
        // The reference 2.5 s enters at the first tick at or after it, 3 s, and every due time
        // is measured from that tick.
        let labels = builder.label(&seconds(&[2.5])).unwrap();
        assert_eq!(labels.entries, [3]);
        assert_eq!(labels.settlements, [5, 6, 8, 12]);
        assert_eq!(labels.reasons, [0, 0, 0, 0]);
        let cell = builder.cell(3, 0, 5, 0).unwrap();
        assert_eq!(cell.entry.unwrap().event_time_micros, 3_000_000);
        assert_eq!(cell.due_time_micros, Some(5_000_000));
        assert_eq!(cell.settlement.unwrap().price_units, 1_000_004);
        assert_eq!(cell.outcome, Some(Outcome::BuyWin));
        assert_eq!(
            builder.cell(3, 1, 6, 0).unwrap().outcome,
            Some(Outcome::BuyWin)
        );
        assert_eq!(
            builder.cell(3, 2, 8, 0).unwrap().outcome,
            Some(Outcome::Tie),
            "an equal price is a tie"
        );
        assert_eq!(
            builder.cell(3, 3, 12, 0).unwrap().outcome,
            Some(Outcome::SellWin)
        );
        // A reference on a tick enters at that tick.
        assert_eq!(builder.label(&seconds(&[4.0])).unwrap().entries, [4]);
        // Past the last tick there is no entry and every dependent field is unavailable.
        let labels = builder.label(&seconds(&[12.5])).unwrap();
        assert_eq!(labels.entries, [MISSING_INDEX]);
        assert_eq!(labels.settlements, [MISSING_INDEX; 4]);
        assert!(labels.reasons.iter().all(|&code| code == 1));
        let cell = builder.cell(MISSING_INDEX, 0, MISSING_INDEX, 1).unwrap();
        assert_eq!(
            (
                cell.entry,
                cell.due_time_micros,
                cell.settlement,
                cell.outcome
            ),
            (None, None, None, None)
        );
        assert_eq!(cell.reason, InvalidReason::NoEntry);
        // The reference 11 s settles only the shortest expiry; the settlement index of an
        // invalid cell is missing, and an invalid cell never carries an outcome.
        let labels = builder.label(&seconds(&[11.0])).unwrap();
        assert_eq!(labels.settlements, [MISSING_INDEX; 4]);
        assert_eq!(labels.reasons, [3, 3, 3, 3]);
        assert_eq!(builder.cell(11, 0, MISSING_INDEX, 3).unwrap().outcome, None);
        assert!(builder.cell(11, 0, MISSING_INDEX, 0).is_err());
        assert!(builder.cell(11, 4, 12, 0).is_err());
        assert!(builder.cell(13, 0, 12, 0).is_err());
        assert!(builder.cell(0, 0, 0, 8).is_err());
    }

    #[test]
    fn every_reason_applies_in_precedence_order() {
        // 0..5 s dense, a 4 s gap to 9 s, dense to 14 s, a frozen run 14..18 s, a jump at 20 s.
        let times = seconds(&[
            0.0, 1.0, 2.0, 3.0, 4.0, 5.0, 9.0, 10.0, 11.0, 12.0, 13.0, 14.0, 15.0, 16.0, 17.0,
            18.0, 19.0, 20.0, 21.0, 22.0,
        ]);
        let prices = vec![
            10_000, 10_001, 10_002, 10_003, 10_004, 10_005, 10_006, 10_007, 10_008, 10_009, 10_010,
            10_011, 10_011, 10_011, 10_011, 10_012, 10_013, 10_020, 10_021, 10_022,
        ];
        let builder = OutcomeBuilder::new(rule(&[1, 2], "5"), times, prices).unwrap();
        use InvalidReason::*;
        // Entry 3 s: due 4 s and 5 s settle on time, nothing flagged.
        assert_eq!(reasons(&builder, 3.0), [Valid, Valid]);
        // Entry 4 s: due 5 s valid; due 6 s settles at 9 s, later than the delay allows.
        assert_eq!(reasons(&builder, 4.0), [Valid, StaleSettlement]);
        // Reference 5.5 s: the entry at 9 s is 3.5 s late, before the gap into it counts.
        assert_eq!(reasons(&builder, 5.5), [StaleEntry, StaleEntry]);
        // Entry 5 s: due 6 s settles at 9 s (stale); due 7 s settles at 9 s, within the delay,
        // but the gap into the settlement tick is inside the window.
        assert_eq!(reasons(&builder, 5.0), [StaleSettlement, InternalGap]);
        // Entry 9 s: the gap into the entry tick is not inside the window.
        assert_eq!(reasons(&builder, 9.0), [Valid, Valid]);
        // The run 14..17 s (four ticks) is frozen; a window touching any member is frozen.
        assert_eq!(reasons(&builder, 12.0), [Valid, FrozenRun]);
        assert_eq!(reasons(&builder, 17.0), [FrozenRun, FrozenRun]);
        // The move 10_013 to 10_020 at 20 s is 6.99 basis points within the jump gap; the
        // transition into the entry tick is excluded, the transition into settlement included.
        assert_eq!(reasons(&builder, 18.0), [Valid, TrueJump]);
        assert_eq!(reasons(&builder, 19.0), [TrueJump, TrueJump]);
        assert_eq!(reasons(&builder, 20.0), [Valid, Valid]);
        // Past the end: no settlement.
        assert_eq!(reasons(&builder, 21.5), [NoSettlement, NoSettlement]);
        assert_eq!(reasons(&builder, 22.5), [NoEntry, NoEntry]);
    }

    #[test]
    fn frozen_runs_mark_every_member_by_count_or_elapsed_time() {
        let times = seconds(&[0.0, 1.0, 2.0, 8.0, 9.0, 10.0, 11.0]);
        let prices = vec![5, 5, 6, 6, 7, 7, 7];
        let mut rule = rule(&[1], "5");
        rule.max_tick_gap_micros = 10_000_000;
        rule.max_settlement_delay_micros = 10_000_000;
        let builder = OutcomeBuilder::new(rule, times, prices).unwrap();
        // 5,5 lasts 1 s with two ticks: not frozen. 6,6 lasts 6 s: frozen by time. 7,7,7 has
        // three ticks: frozen by count.
        assert_eq!(builder.frozen, [0, 0, 0, 1, 2, 3, 4, 5]);
        assert_eq!(reasons(&builder, 0.0), [InvalidReason::Valid]);
        assert_eq!(reasons(&builder, 1.0), [InvalidReason::FrozenRun]);
    }

    #[test]
    fn jump_thresholds_compare_exactly_below_at_and_above_the_boundary() {
        // 10_000 to 10_002 is exactly two basis points.
        let times = seconds(&[0.0, 1.0]);
        let build = |text: &str| {
            OutcomeBuilder::new(rule(&[1], text), times.clone(), vec![10_000, 10_002]).unwrap()
        };
        assert_eq!(build("1.5").jumps, [0, 0, 1]);
        assert_eq!(build("2").jumps, [0, 0, 1]);
        assert_eq!(build("2.0").jumps, [0, 0, 1]);
        assert_eq!(build("2.000000000000000001").jumps, [0, 0, 0]);
        assert_eq!(build("2.5").jumps, [0, 0, 0]);
        // A zero previous price never jumps; a negative previous price uses its magnitude.
        let zero = OutcomeBuilder::new(rule(&[1], "5"), times.clone(), vec![0, 10]).unwrap();
        assert_eq!(zero.jumps, [0, 0, 0]);
        let negative = OutcomeBuilder::new(rule(&[1], "5"), times, vec![-10_000, -9_990]).unwrap();
        assert_eq!(negative.jumps, [0, 0, 1]);
        assert_eq!(
            basis_points(".5").unwrap(),
            (5, 10),
            "the price boundary reads `.5`"
        );
        for text in ["0", "-1", "1e3", "", "1.", "2.0000000000000000001"] {
            assert!(basis_points(text).is_err(), "{text}");
        }
    }

    #[test]
    fn overflowing_and_unindexable_inputs_are_rejected() {
        let times = seconds(&[0.0, 1.0]);
        let error = OutcomeBuilder::new(
            rule(&[1], "1.000000000000000001"),
            times.clone(),
            vec![i64::MIN + 1, i64::MAX],
        )
        .unwrap_err();
        assert!(
            error.contains("overflows the basis-point comparison"),
            "{error}"
        );
        let mut wide = rule(&[u32::MAX], "5");
        wide.frozen_min_ticks = 100;
        let builder = OutcomeBuilder::new(wide, vec![i64::MAX - 1, i64::MAX], vec![1, 2]).unwrap();
        let error = builder.label(&[i64::MAX - 1]).unwrap_err();
        assert!(error.contains("overflows microseconds"), "{error}");
        let mut widest = rule_default();
        widest.max_entry_delay_micros = i64::MAX;
        let late = OutcomeBuilder::new(widest, vec![0, 1_000_000], vec![1, 1]).unwrap();
        let error = late.label(&[i64::MIN]).unwrap_err();
        assert!(
            error.contains("entry delay") && error.contains("overflows"),
            "{error}"
        );
        assert!(builder.cell(0, 0, MISSING_INDEX, 3).is_err());
        assert!(OutcomeBuilder::new(rule_default(), seconds(&[1.0, 0.0]), vec![1, 1]).is_err());
        assert!(OutcomeBuilder::new(rule_default(), times.clone(), vec![1]).is_err());
        // The canonical sequence rules apply: conflicting prices at one time are rejected.
        let error = OutcomeBuilder::new(rule_default(), vec![0, 0], vec![1, 2]).unwrap_err();
        assert!(error.contains("conflicting prices"), "{error}");
        // A span between ticks or across a run that overflows microseconds is rejected.
        let error = OutcomeBuilder::new(rule_default(), vec![i64::MIN + 1, i64::MAX], vec![1, 2])
            .unwrap_err();
        assert!(error.contains("overflows microseconds"), "{error}");
        // The rule is validated wherever it enters: unsorted expiries never reach the search.
        for expiries in [&[][..], &[0][..], &[2, 1][..], &[1, 1][..]] {
            let error =
                OutcomeBuilder::new(rule(expiries, "5"), times.clone(), vec![1, 1]).unwrap_err();
            assert!(error.starts_with("expiry_seconds:"), "{error}");
        }
        let mut negative = rule_default();
        negative.max_tick_gap_micros = -1;
        assert!(OutcomeBuilder::new(negative, times, vec![1, 1]).is_err());
    }

    #[test]
    fn sub_millisecond_clocks_select_and_compare_exactly() {
        let mut rule = rule(&[1], "5");
        rule.frozen_min_ticks = 100;
        // A reference one microsecond after a tick enters at the next tick, which a millisecond
        // clock would have selected one tick early; the due time keeps the microsecond.
        let builder = OutcomeBuilder::new(
            rule.clone(),
            vec![0, 1_000_001, 2_000_000, 4_000_000],
            vec![1_000_000; 4],
        )
        .unwrap();
        let labels = builder.label(&[1_000_002]).unwrap();
        assert_eq!(labels.entries, [2]);
        assert_eq!(labels.settlements, [3]);
        assert_eq!(labels.reasons, [0]);
        assert_eq!(
            builder.cell(2, 0, 3, 0).unwrap().due_time_micros,
            Some(3_000_000)
        );
        // A due time one microsecond past a tick settles at the next tick, which a millisecond
        // due time would have missed.
        let builder = OutcomeBuilder::new(
            rule.clone(),
            vec![0, 1_000_001, 2_000_000, 2_000_001, 3_000_000],
            vec![1_000_000; 5],
        )
        .unwrap();
        let labels = builder.label(&[1_000_001]).unwrap();
        assert_eq!((labels.entries[0], labels.settlements[0]), (1, 3));
        assert_eq!(
            builder.cell(1, 0, 3, 0).unwrap().due_time_micros,
            Some(2_000_001)
        );
        // Freshness compares at the microsecond: a delay of exactly the threshold is fresh and
        // one microsecond more is stale.
        let delay = |gap: i64| {
            let builder =
                OutcomeBuilder::new(rule.clone(), vec![0, gap, gap + 1_000_000], vec![1, 1, 1])
                    .unwrap();
            InvalidReason::from_code(builder.label(&[1]).unwrap().reasons[0]).unwrap()
        };
        assert_eq!(delay(2_000_001), InvalidReason::Valid);
        assert_eq!(delay(2_000_002), InvalidReason::StaleEntry);
        // Elapsed-time freezing compares at the microsecond.
        let frozen = |elapsed: i64| {
            OutcomeBuilder::new(rule.clone(), vec![0, elapsed], vec![7, 7])
                .unwrap()
                .frozen
        };
        assert_eq!(frozen(4_999_999), [0, 0, 0]);
        assert_eq!(frozen(5_000_000), [0, 1, 2]);
    }

    fn rule_default() -> OutcomeRule {
        rule(&[1], "5")
    }

    #[test]
    fn identity_binds_inputs_rule_and_definition() {
        let base = outcome_generation_id("tick", "feature", &rule(&[30, 60], "5"));
        assert_eq!(base.len(), 64);
        assert_ne!(
            base,
            outcome_generation_id("tick", "other", &rule(&[30, 60], "5"))
        );
        assert_ne!(
            base,
            outcome_generation_id("tick", "feature", &rule(&[30, 61], "5"))
        );
        assert_ne!(
            base,
            outcome_generation_id("tick", "feature", &rule(&[30, 60], "5.0"))
        );
        assert_eq!(
            base,
            outcome_generation_id("tick", "feature", &rule(&[30, 60], "5"))
        );
        assert_eq!(
            stream_object_paths(300, 150),
            [
                "reference/300s_150s.bin",
                "entry/300s_150s.bin",
                "settlement/300s_150s.bin",
                "reason/300s_150s.bin"
            ]
        );
    }
}

//! Per-instrument ordered market state: the audit profile and finalized causal candles.
//!
//! One `InstrumentStream` consumes the records of one instrument in provider event-time order,
//! one record at a time, and emits a candle only once a later record proves its interval has
//! closed. Historical batch, replay, and live feeds call the same `push`. Nothing here fills a
//! missing observation, interpolates a price, or infers anything from a symbol.
//! `docs/contracts.md`, section "Instrument streams", is the normative description.

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::config::{CandleSpec, Instrument};
use crate::dataset::{
    Capability, CapabilityError, Coverage, DatasetRole, DayInventoryEntry, GenerationManifest,
    Layout, NativeGranularity, ObjectRecord, ObjectRole, PriceRepresentation, SourceKind, daily,
    manifest_key,
};
use crate::market::{
    Bar, BrokerId, Currency, PriceScale, ProviderSymbol, Tick, float_price_units,
    format_event_time_micros,
};

/// The profile and stream-manifest schema written and accepted by this checkout.
pub const STREAM_SCHEMA_VERSION: u32 = 1;

/// Domain separator hashed before a stream generation's identity text.
const STREAM_GENERATION_DOMAIN_V1: &[u8] = b"binary-alpha instrument stream generation v1\n";

/// The manifest `kind` that distinguishes a stream generation from a dataset generation.
pub const STREAM_MANIFEST_KIND: &str = "instrument_stream";

/// The object path of the profile inside a stream generation.
pub const PROFILE_OBJECT_PATH: &str = "profile.json";

const MICROS_PER_SECOND: i64 = 1_000_000;
/// The largest event or known-at time a record may carry, in microseconds either side of the
/// epoch (about 73,000 years): every interval boundary and every difference then fits in `i64`.
pub const MAX_EVENT_MICROS: i64 = i64::MAX / 4;
/// The largest relative move a candle records, in whole basis points: the candle column's
/// limit, and far past the point where the reference's floating-point value stops being exact
/// (2^53 basis points).
pub const MAX_BASIS_POINTS: u64 = i64::MAX as u64;
const SECONDS_PER_WEEK: i64 = 7 * 86_400;
/// 1970-01-01 was a Thursday; adding three days makes Monday 00:00 the week origin.
const WEEK_ORIGIN_SHIFT_SECONDS: i64 = 3 * 86_400;

/// One ordered input record with prices in integer units at the instrument's scale.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Observation {
    Tick(Tick),
    Bar(BarUnits),
}

/// One native bar in integer units: it starts at `start_micros` and is known at
/// `start_micros + period_micros`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct BarUnits {
    pub start_micros: i64,
    pub period_micros: i64,
    pub open: i64,
    pub high: i64,
    pub low: i64,
    pub close: i64,
    pub volume: f64,
}

impl Observation {
    /// Converts an archive bar exactly to units at `scale`; a price whose shortest decimal
    /// rendering needs more fraction digits is rejected, never rounded.
    pub fn from_bar<P>(bar: &Bar<P>, scale: PriceScale) -> Result<Self, String> {
        bar.validate(bar.period_s)?;
        let units = |value: f64| {
            float_price_units(value, scale)
                .map_err(|reason| format!("bar at {}: {reason}", bar.start_unix_s))
        };
        let start_micros = bar
            .start_unix_s
            .checked_mul(MICROS_PER_SECOND)
            .ok_or_else(|| {
                format!(
                    "bar at {} lies outside the representable time range",
                    bar.start_unix_s
                )
            })?;
        Ok(Self::Bar(BarUnits {
            start_micros,
            period_micros: i64::from(bar.period_s) * MICROS_PER_SECOND,
            open: units(bar.open)?,
            high: units(bar.high)?,
            low: units(bar.low)?,
            close: units(bar.close)?,
            volume: bar.volume,
        }))
    }
}

/// The normalized view of one accepted record.
#[derive(Debug, Clone, Copy, PartialEq)]
struct Record {
    /// Provider event time: the tick time or the bar start.
    event: i64,
    /// The earliest time the record is fully known: the tick time or the bar end.
    known_at: i64,
    /// Open, high, low, and close; a tick resolves only its one price.
    prices: [i64; 4],
    resolved: usize,
    volume: Option<f64>,
}

impl Record {
    fn tick(tick: Tick) -> Self {
        Self {
            event: tick.event_time_micros,
            known_at: tick.event_time_micros,
            prices: [tick.price_units; 4],
            resolved: 1,
            volume: None,
        }
    }

    fn open(&self) -> i64 {
        self.prices[0]
    }

    fn high(&self) -> i64 {
        self.prices[1]
    }

    fn low(&self) -> i64 {
        self.prices[2]
    }

    fn close(&self) -> i64 {
        self.prices[3]
    }

    /// Every price the source resolves, in the order it resolves them.
    fn resolved_prices(&self) -> &[i64] {
        &self.prices[4 - self.resolved..]
    }

    /// Whether the record shows exactly one price.
    fn flat(&self) -> bool {
        self.prices.iter().all(|price| *price == self.close())
    }
}

crate::string_enum! {
    /// Why a record was refused; the stream state is unchanged by a refusal.
    RejectionReason "rejection" {
        BackwardsTime => "backwards_time",
        ConflictingDuplicate => "conflicting_duplicate",
        WrongGranularity => "wrong_granularity",
        OffGrid => "off_grid",
        NonFinite => "non_finite",
        OutOfRange => "out_of_range",
    }
}

/// A refused record with the clocks a consumer needs to place it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Rejection {
    pub reason: RejectionReason,
    pub event_micros: i64,
    pub known_at_micros: i64,
    /// The source generation the record was offered as part of.
    pub source: String,
    pub detail: String,
}

impl std::fmt::Display for Rejection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} at {} (known at {}) in generation {}: {}",
            self.reason,
            format_event_time_micros(self.event_micros),
            format_event_time_micros(self.known_at_micros),
            self.source,
            self.detail
        )
    }
}

impl std::error::Error for Rejection {}

/// One finalized candle of one configured duration and offset stream.
#[derive(Debug, Clone, PartialEq)]
pub struct Candle {
    pub open_time_micros: i64,
    pub close_time_micros: i64,
    /// The known-at time of the record that proved the interval closed.
    pub known_at_micros: i64,
    pub first_event_micros: i64,
    pub last_event_micros: i64,
    /// Last known-at time minus first event time of the records inside the candle.
    pub active_span_micros: i64,
    pub open_units: i64,
    pub high_units: i64,
    pub low_units: i64,
    pub close_units: i64,
    pub observations: u64,
    /// Identical repeats of the previous record that were accepted inside the candle.
    pub duplicates: u64,
    /// The summed source volume, only when the source provides one.
    pub volume: Option<f64>,
    /// Time from the previous record to the first record of this candle; `None` for the first
    /// record of the stream.
    pub gap_before_micros: Option<i64>,
    pub max_gap_inside_micros: i64,
    pub missing_buckets_before: u64,
    /// The longest run of consecutive records showing one unchanged price inside the candle.
    pub frozen_observations: u64,
    pub frozen_micros: i64,
    /// `floor(10000 * |move| / |previous price|)` over every move between consecutive records
    /// that enters or lies inside the candle, saturated at `MAX_BASIS_POINTS`, by the inter-arrival
    /// context of the move: contiguous, delayed (over the gap but under the reopen threshold),
    /// and reopen. A move after a zero price is undefined and skipped.
    pub max_jump_basis_points: u64,
    pub max_delayed_jump_basis_points: u64,
    pub max_reopen_jump_basis_points: u64,
    pub flags: Flags,
}

/// The configured quality checks a candle failed. `complete` means the feed showed no gap and
/// not hard low activity; `clean` is the absence of every reason and is the strict eligibility
/// verdict.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Flags {
    pub low_activity: bool,
    pub hard_low_activity: bool,
    pub gap_before: bool,
    pub gap_inside: bool,
    pub missing_before: bool,
    pub frozen: bool,
    pub jump: bool,
    pub delayed_jump: bool,
    pub reopen_jump: bool,
    pub short_span: bool,
}

impl Flags {
    pub fn complete(self) -> bool {
        !(self.hard_low_activity || self.gap_before || self.gap_inside || self.missing_before)
    }

    pub fn clean(self) -> bool {
        self.complete()
            && !(self.low_activity
                || self.frozen
                || self.jump
                || self.delayed_jump
                || self.reopen_jump
                || self.short_span)
    }
}

/// Counts of values by bit length: index `0` holds zeros, index `k` holds values in
/// `[2^(k-1), 2^k)`. Trailing zero buckets are never stored.
#[derive(Debug, Clone, PartialEq, Eq, Default, Deserialize, Serialize)]
#[serde(transparent)]
pub struct Histogram(Vec<u64>);

impl Histogram {
    pub fn observe(&mut self, value: u64) {
        let bucket = (u64::BITS - value.leading_zeros()) as usize;
        if self.0.len() <= bucket {
            self.0.resize(bucket + 1, 0);
        }
        self.0[bucket] += 1;
    }

    pub fn buckets(&self) -> &[u64] {
        &self.0
    }

    pub fn total(&self) -> u64 {
        self.0.iter().sum()
    }
}

/// What the stream knows about its source generation.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct Source {
    pub generation: String,
    pub source_kind: SourceKind,
    pub role: DatasetRole,
    pub native_granularity: NativeGranularity,
    /// The source's own scale for integer-unit sources; bar archives carry none.
    pub price_scale: Option<PriceScale>,
    pub capabilities: Vec<Capability>,
}

impl Source {
    pub fn from_manifest(manifest: &GenerationManifest) -> Self {
        Self {
            generation: manifest.generation.clone(),
            source_kind: manifest.source_kind,
            role: manifest.role,
            native_granularity: manifest.native_granularity,
            price_scale: match manifest.price_representation {
                PriceRepresentation::IntegerUnits { scale } => Some(scale),
                PriceRepresentation::BinaryFloat64 => None,
            },
            capabilities: manifest.capabilities.clone(),
        }
    }
}

crate::string_enum! {
    /// A calculation a later consumer may request; a source either supports it or the profile
    /// records why not.
    Calculation "calculation" {
        TickCount => "tick_count",
        TickPath => "tick_path",
        TickGaps => "tick_gaps",
        EntryTick => "entry_tick",
        TickSettlement => "tick_settlement",
    }
}

/// Whether one calculation is available on the bound source.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct CalculationSupport {
    pub calculation: Calculation,
    pub supported: bool,
    pub reason: Option<String>,
}

/// Facts about prices across every accepted record.
#[derive(Debug, Clone, PartialEq, Eq, Default, Deserialize, Serialize)]
pub struct PriceFacts {
    pub min_units: Option<i64>,
    pub max_units: Option<i64>,
    /// The greatest common divisor of every nonzero move between consecutive prices: the
    /// observed price step, never inferred from a symbol.
    pub step_units: Option<u64>,
    pub moves: u64,
}

/// Inter-arrival times above the configured gap.
#[derive(Debug, Clone, PartialEq, Eq, Default, Deserialize, Serialize)]
pub struct GapFacts {
    pub count: u64,
    pub max_micros: i64,
    pub total_micros: i64,
}

/// Closed runs of one unchanged price that met the configured frozen thresholds.
#[derive(Debug, Clone, PartialEq, Eq, Default, Deserialize, Serialize)]
pub struct FrozenFacts {
    pub count: u64,
    pub max_observations: u64,
    pub max_micros: i64,
}

/// Relative moves between consecutive records, in whole basis points by bit length, and how
/// many reached the configured jump in each inter-arrival context.
#[derive(Debug, Clone, PartialEq, Eq, Default, Deserialize, Serialize)]
pub struct JumpFacts {
    pub basis_points: Histogram,
    pub flagged: u64,
    pub flagged_delayed: u64,
    pub flagged_reopen: u64,
}

/// Records inside each configured weekly window and outside every window.
#[derive(Debug, Clone, PartialEq, Eq, Default, Deserialize, Serialize)]
pub struct SessionFacts {
    pub windows: Vec<SessionCount>,
    pub outside: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct SessionCount {
    pub name: String,
    pub observations: u64,
}

/// How many finalized candles failed each check.
#[derive(Debug, Clone, PartialEq, Eq, Default, Deserialize, Serialize)]
pub struct FlagCounts {
    pub low_activity: u64,
    pub hard_low_activity: u64,
    pub gap_before: u64,
    pub gap_inside: u64,
    pub missing_before: u64,
    pub frozen: u64,
    pub jump: u64,
    pub delayed_jump: u64,
    pub reopen_jump: u64,
    pub short_span: u64,
    pub complete: u64,
    pub clean: u64,
}

/// Facts about one configured candle stream.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct StreamFacts {
    pub duration_seconds: u32,
    pub offset_seconds: u32,
    pub finalized: u64,
    /// Records inside the unfinished last candle, which is withheld.
    pub withheld_observations: u64,
    /// Records per finalized candle, by bit length.
    pub activity: Histogram,
    pub flagged: FlagCounts,
}

/// The audit profile: only facts a validator or a feature-compatibility check consumes. It is
/// evidence about the source, never self-modifying configuration.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct InstrumentProfile {
    pub schema_version: u32,
    pub instrument: String,
    pub broker: BrokerId,
    pub provider_symbol: ProviderSymbol,
    pub base_currency: Option<Currency>,
    pub quote_currency: Currency,
    pub price_scale: PriceScale,
    pub source: Source,
    pub observations: u64,
    pub duplicates: u64,
    pub coverage: Option<Coverage>,
    /// Event-time micros between consecutive records, by bit length.
    pub cadence: Histogram,
    pub prices: PriceFacts,
    pub gaps: Option<GapFacts>,
    pub frozen_runs: Option<FrozenFacts>,
    pub jumps: Option<JumpFacts>,
    pub sessions: Option<SessionFacts>,
    pub streams: Vec<StreamFacts>,
    pub calculations: Vec<CalculationSupport>,
}

impl InstrumentProfile {
    /// The exact bytes published as `profile.json`: pretty JSON in field order and one trailing
    /// line feed.
    pub fn to_json(&self) -> Vec<u8> {
        let mut bytes = serde_json::to_vec_pretty(self).expect("a profile serializes");
        bytes.push(b'\n');
        bytes
    }

    pub fn from_json(bytes: &[u8]) -> Result<Self, String> {
        let profile: Self = serde_json::from_slice(bytes).map_err(|error| error.to_string())?;
        if profile.schema_version != STREAM_SCHEMA_VERSION {
            return Err(format!(
                "unsupported profile schema_version {}, expected {STREAM_SCHEMA_VERSION}",
                profile.schema_version
            ));
        }
        Ok(profile)
    }
}

/// A run of consecutive records showing one unchanged price.
#[derive(Debug, Clone, Copy)]
struct Run {
    price: i64,
    start_event: i64,
    last_known_at: i64,
    observations: u64,
}

impl Run {
    fn micros(&self) -> i64 {
        self.last_known_at - self.start_event
    }

    /// Extends the run with a flat record at its price, or restarts it; a record showing more
    /// than one price ends every run.
    fn fold(run: &mut Option<Self>, record: &Record) {
        match run {
            Some(current) if record.flat() && record.close() == current.price => {
                current.observations += 1;
                current.last_known_at = record.known_at;
            }
            _ if record.flat() => {
                *run = Some(Self {
                    price: record.close(),
                    start_event: record.event,
                    last_known_at: record.known_at,
                    observations: 1,
                });
            }
            _ => *run = None,
        }
    }
}

/// The candle being built for one stream.
#[derive(Debug, Clone)]
struct Working {
    open_time: i64,
    close_time: i64,
    first_event: i64,
    last_event: i64,
    last_known_at: i64,
    open: i64,
    high: i64,
    low: i64,
    close: i64,
    observations: u64,
    volume: Option<f64>,
    gap_before: Option<i64>,
    max_gap_inside: i64,
    missing_before: u64,
    run: Option<Run>,
    frozen_observations: u64,
    frozen_micros: i64,
    max_jump: [u64; 3],
    duplicates: u64,
}

impl Working {
    fn observe_run(&mut self, record: &Record) {
        Run::fold(&mut self.run, record);
        if let Some(run) = &self.run {
            self.frozen_observations = self.frozen_observations.max(run.observations);
            self.frozen_micros = self.frozen_micros.max(run.micros());
        }
    }
}

/// One configured duration and offset stream.
#[derive(Debug, Clone)]
struct CandleStream {
    duration: i64,
    offset: i64,
    min_observations: Option<u32>,
    hard_min_observations: Option<u32>,
    working: Option<Working>,
    previous_open_time: Option<i64>,
    facts: StreamFacts,
}

impl CandleStream {
    fn open_time(&self, event: i64) -> i64 {
        interval_open(event, self.duration, self.offset)
    }
}

/// The open time of the left-closed interval of `duration` micros, offset `offset` micros
/// from the Unix-epoch grid, that contains `event`.
pub fn interval_open(event: i64, duration: i64, offset: i64) -> i64 {
    offset + (event - offset).div_euclid(duration) * duration
}

/// The thresholds of the enabled checks, in the stream's units.
#[derive(Debug, Clone, Copy)]
struct Checks {
    /// The gap and reopen thresholds.
    gap_micros: Option<(i64, i64)>,
    frozen: Option<(u32, i64)>,
    jump_basis_points: Option<u32>,
    span_percent: Option<u8>,
}

/// One instrument's ordered state: the running profile and every configured candle stream.
#[derive(Debug, Clone)]
pub struct InstrumentStream {
    instrument: Instrument,
    source: Source,
    checks: Checks,
    sessions: Option<Vec<(String, i64, i64)>>,
    last: Option<Record>,
    run: Option<Run>,
    streams: Vec<CandleStream>,
    observations: u64,
    duplicates: u64,
    first_event: Option<i64>,
    cadence: Histogram,
    prices: PriceFacts,
    gaps: GapFacts,
    frozen_runs: FrozenFacts,
    jumps: JumpFacts,
    session_counts: Vec<u64>,
    outside_sessions: u64,
    calculations: Vec<CalculationSupport>,
}

impl InstrumentStream {
    /// Binds a configured instrument to a source generation. The source must provide the
    /// instrument's native granularity and, for an integer-unit source, the instrument's price
    /// scale; a bar-only source refuses a tick instrument with the machine-readable capability
    /// error, and the profile of a bar instrument records every tick calculation as unsupported
    /// with that same reason.
    pub fn new(instrument: &Instrument, source: Source) -> Result<Self, String> {
        instrument.validate()?;
        let required = match instrument.native_granularity {
            NativeGranularity::Tick => Capability::Ticks,
            NativeGranularity::Bar { .. } => Capability::Bars,
        };
        let capability_error = |required| CapabilityError {
            required,
            provided: source.capabilities.clone(),
            instrument: instrument.id().to_string(),
            generation: source.generation.clone(),
        };
        if !source.capabilities.contains(&required) {
            return Err(capability_error(required).to_string());
        }
        if source.native_granularity != instrument.native_granularity {
            return Err(format!(
                "instrument {} declares {} granularity, but generation {} provides {}",
                instrument.id(),
                instrument.native_granularity,
                source.generation,
                source.native_granularity
            ));
        }
        if let Some(scale) = source.price_scale
            && scale != instrument.price_scale
        {
            return Err(format!(
                "instrument {} declares price_scale {}, but generation {} carries units at price_scale {}",
                instrument.id(),
                instrument.price_scale.digits(),
                source.generation,
                scale.digits()
            ));
        }
        // Support follows the verified granularity, never a capability list on its own.
        let ticks = required == Capability::Ticks;
        let calculations = Calculation::ALL
            .iter()
            .map(|&calculation| CalculationSupport {
                calculation,
                supported: ticks,
                reason: (!ticks).then(|| capability_error(Capability::Ticks).to_string()),
            })
            .collect();
        let seconds = |value: u32| i64::from(value) * MICROS_PER_SECOND;
        let checks = Checks {
            gap_micros: instrument
                .gap
                .as_ref()
                .map(|gap| (seconds(gap.max_seconds), seconds(gap.reopen_seconds))),
            frozen: instrument
                .frozen
                .as_ref()
                .map(|frozen| (frozen.min_observations, seconds(frozen.min_seconds))),
            jump_basis_points: instrument.jump.as_ref().map(|jump| jump.min_basis_points),
            span_percent: instrument.span.as_ref().map(|span| span.min_percent),
        };
        let sessions = instrument.sessions.as_ref().map(|sessions| {
            sessions
                .iter()
                .map(|session| {
                    (
                        session.name.clone(),
                        i64::from(session.open_seconds),
                        i64::from(session.close_seconds),
                    )
                })
                .collect::<Vec<_>>()
        });
        let streams = instrument
            .candles
            .iter()
            .map(|spec: &CandleSpec| CandleStream {
                duration: seconds(spec.duration_seconds),
                offset: seconds(spec.offset_seconds),
                min_observations: spec.min_observations,
                hard_min_observations: spec.hard_min_observations,
                working: None,
                previous_open_time: None,
                facts: StreamFacts {
                    duration_seconds: spec.duration_seconds,
                    offset_seconds: spec.offset_seconds,
                    finalized: 0,
                    withheld_observations: 0,
                    activity: Histogram::default(),
                    flagged: FlagCounts::default(),
                },
            })
            .collect();
        Ok(Self {
            session_counts: vec![0; sessions.as_ref().map_or(0, Vec::len)],
            instrument: instrument.clone(),
            source,
            checks,
            sessions,
            last: None,
            run: None,
            streams,
            observations: 0,
            duplicates: 0,
            first_event: None,
            cadence: Histogram::default(),
            prices: PriceFacts::default(),
            gaps: GapFacts::default(),
            frozen_runs: FrozenFacts::default(),
            jumps: JumpFacts::default(),
            outside_sessions: 0,
            calculations,
        })
    }

    /// Accepts the next record in event-time order and appends every candle it finalizes, as
    /// `(stream index, candle)` in configuration order, to `out`. A refused record leaves the
    /// state unchanged.
    pub fn push(
        &mut self,
        observation: Observation,
        out: &mut Vec<(usize, Candle)>,
    ) -> Result<(), Rejection> {
        let record = self.record(observation)?;
        let reject = |reason, detail: String| Rejection {
            reason,
            event_micros: record.event,
            known_at_micros: record.known_at,
            source: self.source.generation.clone(),
            detail,
        };
        let mut delta = None;
        let mut previous_close = None;
        let mut duplicate = false;
        if let Some(last) = self.last {
            // A bar must strictly follow the previous bar; a tick may repeat the previous tick's
            // event time only with the same price, and such an identical repeat is accepted
            // and counted like the source retained it.
            if record.event < last.event || (record.event == last.event && record.volume.is_some())
            {
                return Err(reject(
                    RejectionReason::BackwardsTime,
                    format!("does not follow {}", format_event_time_micros(last.event)),
                ));
            }
            if record.event == last.event {
                if record != last {
                    return Err(reject(
                        RejectionReason::ConflictingDuplicate,
                        format!(
                            "price {} differs from {} at one event time",
                            record.close(),
                            last.close()
                        ),
                    ));
                }
                self.duplicates += 1;
                duplicate = true;
            }
            delta = Some(record.event - last.known_at);
            previous_close = Some(last.close());
        }
        if let Some(volume) = record.volume
            && self.streams.iter().any(|stream| {
                stream.working.as_ref().is_some_and(|working| {
                    stream.open_time(record.event) == working.open_time
                        && working
                            .volume
                            .is_some_and(|total| !(total + volume).is_finite())
                })
            })
        {
            return Err(reject(
                RejectionReason::NonFinite,
                "the candle's summed volume would not be finite".to_string(),
            ));
        }
        self.observations += 1;
        self.first_event.get_or_insert(record.event);
        if let Some(last) = self.last {
            self.cadence.observe((record.event - last.event) as u64);
        }
        let mut context = GapContext::Contiguous;
        if let (Some(delta), Some((max, reopen))) = (delta, self.checks.gap_micros)
            && delta > max
        {
            self.gaps.count += 1;
            self.gaps.max_micros = self.gaps.max_micros.max(delta);
            self.gaps.total_micros += delta;
            context = if delta >= reopen {
                GapContext::Reopen
            } else {
                GapContext::Delayed
            };
        }
        let jump = self.observe_prices(previous_close, context, &record);
        self.observe_run(&record);
        self.observe_session(record.event);
        for index in 0..self.streams.len() {
            self.fold(index, &record, delta, jump, duplicate, out);
        }
        self.last = Some(record);
        Ok(())
    }

    /// Validates the record against the bound granularity and normalizes it.
    fn record(&self, observation: Observation) -> Result<Record, Rejection> {
        let reject = |reason, event, known_at, detail: String| Rejection {
            reason,
            event_micros: event,
            known_at_micros: known_at,
            source: self.source.generation.clone(),
            detail,
        };
        // Both clocks are bounded before anything else looks at them, so every later sum and
        // difference is representable.
        let (event, known_at) = match &observation {
            Observation::Tick(tick) => (tick.event_time_micros, tick.event_time_micros),
            Observation::Bar(bar) => (
                bar.start_micros,
                bar.start_micros.saturating_add(bar.period_micros),
            ),
        };
        if event.unsigned_abs() > MAX_EVENT_MICROS as u64
            || known_at.unsigned_abs() > MAX_EVENT_MICROS as u64
        {
            return Err(reject(
                RejectionReason::OutOfRange,
                event,
                known_at,
                format!("event time beyond {MAX_EVENT_MICROS} micros either side of the epoch"),
            ));
        }
        match (observation, self.source.native_granularity) {
            (Observation::Tick(tick), NativeGranularity::Tick) => Ok(Record::tick(tick)),
            (Observation::Bar(bar), NativeGranularity::Bar { period_seconds }) => {
                let period = i64::from(period_seconds) * MICROS_PER_SECOND;
                if bar.period_micros != period {
                    return Err(reject(
                        RejectionReason::WrongGranularity,
                        bar.start_micros,
                        known_at,
                        format!("bar period {} micros, expected {period}", bar.period_micros),
                    ));
                }
                if bar.start_micros.rem_euclid(period) != 0 {
                    return Err(reject(
                        RejectionReason::OffGrid,
                        bar.start_micros,
                        known_at,
                        format!("bar start is off the {period_seconds}-second grid"),
                    ));
                }
                if !bar.volume.is_finite()
                    || bar.volume < 0.0
                    || bar.high < bar.open.max(bar.close)
                    || bar.low > bar.open.min(bar.close)
                {
                    return Err(reject(
                        RejectionReason::NonFinite,
                        bar.start_micros,
                        known_at,
                        "bar volume is not finite and non-negative, or the high/low relationship is contradicted"
                            .to_string(),
                    ));
                }
                Ok(Record {
                    event: bar.start_micros,
                    known_at,
                    prices: [bar.open, bar.high, bar.low, bar.close],
                    resolved: 4,
                    volume: Some(bar.volume),
                })
            }
            (Observation::Tick(tick), NativeGranularity::Bar { .. }) => Err(reject(
                RejectionReason::WrongGranularity,
                tick.event_time_micros,
                tick.event_time_micros,
                "a tick was offered to a bar-granularity stream".to_string(),
            )),
            (Observation::Bar(bar), NativeGranularity::Tick) => Err(reject(
                RejectionReason::WrongGranularity,
                bar.start_micros,
                bar.start_micros + bar.period_micros,
                "a bar was offered to a tick-granularity stream".to_string(),
            )),
        }
    }

    /// Folds the record's prices into the price facts and, for the one observed move between
    /// consecutive records (the previous close to this open), the jump distribution; returns
    /// that move in whole basis points under its inter-arrival context. A bar's high and low
    /// bound its prices and its step, never a path: their order inside the bar is unobserved.
    /// The step anchors every resolved price to the previous close, or to the record's own open
    /// for the first record.
    fn observe_prices(
        &mut self,
        previous_close: Option<i64>,
        context: GapContext,
        record: &Record,
    ) -> Jump {
        let mut jump = Jump::default();
        let anchor = previous_close.unwrap_or(record.open());
        for &price in record.resolved_prices() {
            self.prices.min_units = Some(self.prices.min_units.map_or(price, |min| min.min(price)));
            self.prices.max_units = Some(self.prices.max_units.map_or(price, |max| max.max(price)));
            if anchor != price {
                let step = anchor.abs_diff(price);
                self.prices.step_units = Some(
                    self.prices
                        .step_units
                        .map_or(step, |current| gcd(current, step)),
                );
            }
        }
        if let Some(previous) = previous_close {
            let (basis_points, moved) = relative_move(previous, record.open());
            self.prices.moves += u64::from(moved);
            if let Some(basis_points) = basis_points {
                jump.0[context as usize] = basis_points;
                if let Some(min) = self.checks.jump_basis_points {
                    self.jumps.basis_points.observe(basis_points);
                    if basis_points >= u64::from(min) {
                        *match context {
                            GapContext::Contiguous => &mut self.jumps.flagged,
                            GapContext::Delayed => &mut self.jumps.flagged_delayed,
                            GapContext::Reopen => &mut self.jumps.flagged_reopen,
                        } += 1;
                    }
                }
            }
        }
        jump
    }

    /// Extends or closes the instrument-wide run of one unchanged price.
    fn observe_run(&mut self, record: &Record) {
        let before = self.run;
        Run::fold(&mut self.run, record);
        let closed = match (before, self.run) {
            (Some(before), Some(after)) if after.observations > 1 => {
                debug_assert_eq!(before.price, after.price);
                None
            }
            (Some(before), _) => Some(before),
            (None, _) => None,
        };
        if let (Some(run), Some((min_observations, min_micros))) = (closed, self.checks.frozen)
            && (run.observations >= u64::from(min_observations) || run.micros() >= min_micros)
        {
            self.frozen_runs.count += 1;
            self.frozen_runs.max_observations =
                self.frozen_runs.max_observations.max(run.observations);
            self.frozen_runs.max_micros = self.frozen_runs.max_micros.max(run.micros());
        }
    }

    fn observe_session(&mut self, event: i64) {
        let Some(sessions) = &self.sessions else {
            return;
        };
        let week_second = (event.div_euclid(MICROS_PER_SECOND) + WEEK_ORIGIN_SHIFT_SECONDS)
            .rem_euclid(SECONDS_PER_WEEK);
        match sessions
            .iter()
            .position(|(_, open, close)| (*open..*close).contains(&week_second))
        {
            Some(index) => self.session_counts[index] += 1,
            None => self.outside_sessions += 1,
        }
    }

    /// Folds the record into stream `index`, appending every candle it finalizes to `out`: a
    /// stale working candle the record's interval leaves behind, and the record's own interval
    /// when its known-at time already reaches that close.
    fn fold(
        &mut self,
        index: usize,
        record: &Record,
        delta: Option<i64>,
        jump: Jump,
        duplicate: bool,
        out: &mut Vec<(usize, Candle)>,
    ) {
        let stream = &mut self.streams[index];
        let open_time = stream.open_time(record.event);
        if let Some(working) = &mut stream.working {
            if open_time == working.open_time {
                working.last_event = record.event;
                working.last_known_at = record.known_at;
                working.high = working.high.max(record.high());
                working.low = working.low.min(record.low());
                working.close = record.close();
                working.observations += 1;
                working.duplicates += u64::from(duplicate);
                if let (Some(total), Some(volume)) = (&mut working.volume, record.volume) {
                    *total += volume;
                }
                if let Some(delta) = delta {
                    working.max_gap_inside = working.max_gap_inside.max(delta);
                }
                for (current, incoming) in working.max_jump.iter_mut().zip(jump.0) {
                    *current = (*current).max(incoming);
                }
                working.observe_run(record);
                if working.last_known_at >= working.close_time {
                    out.push((index, Self::finalize(stream, &self.checks, record.known_at)));
                }
                return;
            }
            out.push((index, Self::finalize(stream, &self.checks, record.known_at)));
        }
        let close_time = open_time + stream.duration;
        // The previous open lies at least one duration earlier, so the count is never negative.
        let missing_before = stream.previous_open_time.map_or(0, |previous| {
            ((open_time - previous) / stream.duration - 1) as u64
        });
        let mut working = Working {
            open_time,
            close_time,
            first_event: record.event,
            last_event: record.event,
            last_known_at: record.known_at,
            open: record.open(),
            high: record.high(),
            low: record.low(),
            close: record.close(),
            observations: 1,
            volume: record.volume,
            gap_before: delta,
            max_gap_inside: 0,
            missing_before,
            run: None,
            frozen_observations: 0,
            frozen_micros: 0,
            max_jump: jump.0,
            duplicates: u64::from(duplicate),
        };
        working.observe_run(record);
        stream.working = Some(working);
        if record.known_at >= close_time {
            out.push((index, Self::finalize(stream, &self.checks, record.known_at)));
        }
    }

    /// Closes the stream's working candle, evaluates its checks, and records its facts.
    fn finalize(stream: &mut CandleStream, checks: &Checks, known_at: i64) -> Candle {
        let working = stream.working.take().expect("a working candle");
        stream.previous_open_time = Some(working.open_time);
        let active_span = working.last_known_at - working.first_event;
        let flags = Flags {
            low_activity: stream
                .min_observations
                .is_some_and(|min| working.observations < u64::from(min)),
            hard_low_activity: stream
                .hard_min_observations
                .is_some_and(|min| working.observations < u64::from(min)),
            gap_before: checks
                .gap_micros
                .is_some_and(|(max, _)| working.gap_before.is_some_and(|gap| gap > max)),
            gap_inside: checks
                .gap_micros
                .is_some_and(|(max, _)| working.max_gap_inside > max),
            missing_before: working.missing_before > 0,
            frozen: checks.frozen.is_some_and(|(observations, micros)| {
                working.frozen_observations >= u64::from(observations)
                    || working.frozen_micros >= micros
            }),
            jump: checks
                .jump_basis_points
                .is_some_and(|min| working.max_jump[0] >= u64::from(min)),
            delayed_jump: checks
                .jump_basis_points
                .is_some_and(|min| working.max_jump[1] >= u64::from(min)),
            reopen_jump: checks
                .jump_basis_points
                .is_some_and(|min| working.max_jump[2] >= u64::from(min)),
            short_span: checks
                .span_percent
                .is_some_and(|percent| active_span < stream.duration * i64::from(percent) / 100),
        };
        let facts = &mut stream.facts;
        facts.finalized += 1;
        facts.activity.observe(working.observations);
        let counts = &mut facts.flagged;
        for (flag, count) in [
            (flags.low_activity, &mut counts.low_activity),
            (flags.hard_low_activity, &mut counts.hard_low_activity),
            (flags.gap_before, &mut counts.gap_before),
            (flags.gap_inside, &mut counts.gap_inside),
            (flags.missing_before, &mut counts.missing_before),
            (flags.frozen, &mut counts.frozen),
            (flags.jump, &mut counts.jump),
            (flags.delayed_jump, &mut counts.delayed_jump),
            (flags.reopen_jump, &mut counts.reopen_jump),
            (flags.short_span, &mut counts.short_span),
            (flags.complete(), &mut counts.complete),
            (flags.clean(), &mut counts.clean),
        ] {
            *count += u64::from(flag);
        }
        Candle {
            open_time_micros: working.open_time,
            close_time_micros: working.close_time,
            known_at_micros: known_at,
            first_event_micros: working.first_event,
            last_event_micros: working.last_event,
            active_span_micros: active_span,
            open_units: working.open,
            high_units: working.high,
            low_units: working.low,
            close_units: working.close,
            observations: working.observations,
            duplicates: working.duplicates,
            volume: working.volume,
            gap_before_micros: working.gap_before,
            max_gap_inside_micros: working.max_gap_inside,
            missing_buckets_before: working.missing_before,
            frozen_observations: working.frozen_observations,
            frozen_micros: working.frozen_micros,
            max_jump_basis_points: working.max_jump[0],
            max_delayed_jump_basis_points: working.max_jump[1],
            max_reopen_jump_basis_points: working.max_jump[2],
            flags,
        }
    }

    /// The profile of everything accepted so far; every count covers only closed windows, so a
    /// longer input can extend but never revise it.
    pub fn profile(&self) -> InstrumentProfile {
        let coverage = match (self.first_event, &self.last) {
            (Some(first), Some(last)) => Some(Coverage {
                first_event_time: format_event_time_micros(first),
                last_event_time: format_event_time_micros(last.event),
            }),
            _ => None,
        };
        InstrumentProfile {
            schema_version: STREAM_SCHEMA_VERSION,
            instrument: self.instrument.id().to_string(),
            broker: self.instrument.broker.clone(),
            provider_symbol: self.instrument.provider_symbol.clone(),
            base_currency: self.instrument.base_currency.clone(),
            quote_currency: self.instrument.quote_currency.clone(),
            price_scale: self.instrument.price_scale,
            source: self.source.clone(),
            observations: self.observations,
            duplicates: self.duplicates,
            coverage,
            cadence: self.cadence.clone(),
            prices: self.prices.clone(),
            gaps: self.checks.gap_micros.map(|_| self.gaps.clone()),
            frozen_runs: self.checks.frozen.map(|_| self.frozen_runs.clone()),
            jumps: self.checks.jump_basis_points.map(|_| self.jumps.clone()),
            sessions: self.sessions.as_ref().map(|sessions| SessionFacts {
                windows: sessions
                    .iter()
                    .zip(&self.session_counts)
                    .map(|((name, _, _), observations)| SessionCount {
                        name: name.clone(),
                        observations: *observations,
                    })
                    .collect(),
                outside: self.outside_sessions,
            }),
            streams: self
                .streams
                .iter()
                .map(|stream| StreamFacts {
                    withheld_observations: stream
                        .working
                        .as_ref()
                        .map_or(0, |working| working.observations),
                    ..stream.facts.clone()
                })
                .collect(),
            calculations: self.calculations.clone(),
        }
    }
}

/// The inter-arrival context of the move entering a record.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum GapContext {
    Contiguous = 0,
    Delayed = 1,
    Reopen = 2,
}

/// The relative move entering one record, in whole basis points, in the slot of its context.
#[derive(Debug, Clone, Copy, Default)]
struct Jump([u64; 3]);

/// `floor(10000 * |to - from| / |from|)` saturated at `MAX_BASIS_POINTS`, or `None` when `from` is zero,
/// and whether the price moved at all.
fn relative_move(from: i64, to: i64) -> (Option<u64>, bool) {
    let moved = from != to;
    if from == 0 {
        return (None, moved);
    }
    let basis_points = (i128::from(to) - i128::from(from)).abs() * 10_000 / i128::from(from).abs();
    (
        Some(
            u64::try_from(basis_points)
                .map_or(MAX_BASIS_POINTS, |value| value.min(MAX_BASIS_POINTS)),
        ),
        moved,
    )
}

fn gcd(mut a: u64, mut b: u64) -> u64 {
    while b != 0 {
        (a, b) = (b, a % b);
    }
    a
}

/// Summary of one candle stream's published rows.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct StreamSummary {
    pub duration_seconds: u32,
    pub offset_seconds: u32,
    pub rows: u64,
    pub first_open_time: Option<String>,
    pub last_close_time: Option<String>,
}

impl StreamSummary {
    /// The object path of the stream's candle rows inside a stream generation.
    pub fn object_path(duration_seconds: u32, offset_seconds: u32) -> String {
        format!("candles/{duration_seconds}s_{offset_seconds}s.parquet")
    }
}

/// The identity of a stream generation: the source generation and the instrument's canonical
/// definition, so the same source audited under the same definition names the same generation.
pub fn stream_generation_id(source_generation: &str, definition: &str) -> String {
    stream_generation_id_with_layout(source_generation, definition, None)
}

/// Layout-aware stream identity, preserving the legacy recipe for an absent marker.
pub fn stream_generation_id_with_layout(
    source_generation: &str,
    definition: &str,
    layout: Option<Layout>,
) -> String {
    let mut hasher = Sha256::new();
    hasher.update(STREAM_GENERATION_DOMAIN_V1);
    if let Some(layout) = layout {
        hasher.update(format!("layout {layout}\n").as_bytes());
    }
    hasher.update(source_generation.as_bytes());
    hasher.update(b"\n");
    hasher.update(definition.as_bytes());
    crate::hex(&hasher.finalize())
}

/// The ready manifest of one stream generation. Field order is the serialization order.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct StreamManifest {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub layout: Option<Layout>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub day_inventory: Vec<DayInventoryEntry>,
    pub kind: String,
    pub schema_version: u32,
    pub generation: String,
    pub broker: BrokerId,
    pub provider_symbol: ProviderSymbol,
    pub instrument: String,
    pub role: DatasetRole,
    pub source_generation: String,
    pub source_kind: SourceKind,
    pub definition: Instrument,
    pub config_hash: String,
    pub code_revision: String,
    pub observations: u64,
    pub coverage: Option<Coverage>,
    pub streams: Vec<StreamSummary>,
    pub objects: Vec<ObjectRecord>,
}

impl StreamManifest {
    /// The exact bytes published as `ready.json`: pretty JSON in field order and one trailing
    /// line feed.
    pub fn to_json(&self) -> Vec<u8> {
        let mut bytes = serde_json::to_vec_pretty(self).expect("a manifest serializes");
        bytes.push(b'\n');
        bytes
    }

    /// Parses a stream manifest and checks what every consumer relies on before it trusts a key:
    /// the kind, a generation that matches the source and definition, consistent instrument
    /// fields, one candle object per stream, a profile object, and content-addressed objects
    /// with unique clean paths.
    pub fn from_json(bytes: &[u8]) -> Result<Self, String> {
        let manifest: Self = serde_json::from_slice(bytes).map_err(|error| error.to_string())?;
        if manifest.kind != STREAM_MANIFEST_KIND {
            return Err(format!(
                "manifest kind `{}` is not `{STREAM_MANIFEST_KIND}`",
                manifest.kind
            ));
        }
        if manifest.schema_version != STREAM_SCHEMA_VERSION {
            return Err(format!(
                "unsupported manifest schema_version {}, expected {STREAM_SCHEMA_VERSION}",
                manifest.schema_version
            ));
        }
        manifest.validate()?;
        Ok(manifest)
    }

    fn validate(&self) -> Result<(), String> {
        let definition = &self.definition;
        if self.broker != definition.broker
            || self.provider_symbol != definition.provider_symbol
            || self.instrument != definition.id().to_string()
        {
            return Err(format!(
                "instrument `{}` does not match the recorded definition",
                self.instrument
            ));
        }
        definition
            .validate()
            .map_err(|reason| format!("definition: {reason}"))?;
        if self.generation
            != stream_generation_id_with_layout(
                &self.source_generation,
                &definition.canonical_toml(),
                self.layout,
            )
        {
            return Err(format!(
                "generation `{}` does not match the source generation and definition",
                self.generation
            ));
        }
        crate::dataset::validate_objects(&self.objects)?;
        if self
            .objects
            .iter()
            .filter(|object| object.path == PROFILE_OBJECT_PATH)
            .count()
            != 1
        {
            return Err(format!("expected one `{PROFILE_OBJECT_PATH}` object"));
        }
        if self.streams.len() != definition.candles.len()
            || self
                .streams
                .iter()
                .zip(&definition.candles)
                .any(|(summary, spec)| {
                    summary.duration_seconds != spec.duration_seconds
                        || summary.offset_seconds != spec.offset_seconds
                })
        {
            return Err("streams do not match the definition's candle list".to_string());
        }
        if self.layout == Some(Layout::DailyV2) {
            if self.role != DatasetRole::Development || self.source_kind == SourceKind::TickCsv {
                return Err("daily-v2 requires a development daily/archive/history source".into());
            }
            let specs: Vec<_> = self
                .streams
                .iter()
                .map(|s| (s.duration_seconds, s.offset_seconds))
                .collect();
            daily::validate_inventory(
                &self.day_inventory,
                &self.objects,
                daily::DailyOwner::Stream(&specs),
            )?;
            for summary in &self.streams {
                if daily::inventory_rows(self.day_inventory.iter().filter(|day| {
                    day.duration == Some(summary.duration_seconds)
                        && day.offset == Some(summary.offset_seconds)
                }))? != summary.rows
                {
                    return Err("candle inventory rows disagree with stream summary".into());
                }
            }
        } else {
            if !self.day_inventory.is_empty() {
                return Err("day_inventory requires layout daily-v2".into());
            }
            for summary in &self.streams {
                let path =
                    StreamSummary::object_path(summary.duration_seconds, summary.offset_seconds);
                if !self.objects.iter().any(|object| object.path == path) {
                    return Err(format!("expected a `{path}` object"));
                }
            }
        }
        if self
            .objects
            .iter()
            .any(|object| object.role != ObjectRole::Normalized)
        {
            return Err("every stream object is normalized output".to_string());
        }
        Ok(())
    }

    /// The key this manifest is published at.
    pub fn key(&self) -> String {
        manifest_key(&self.generation)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{CandleSpec, FrozenCheck, GapCheck, JumpCheck, Session, SpanCheck};
    use crate::dataset::object_key;

    const SECOND: i64 = MICROS_PER_SECOND;

    fn scale(digits: u8) -> PriceScale {
        PriceScale::try_from(digits).unwrap()
    }

    fn instrument(granularity: NativeGranularity, candles: &[(u32, u32)]) -> Instrument {
        Instrument {
            broker: BrokerId::try_from("b".to_string()).unwrap(),
            provider_symbol: ProviderSymbol::try_from("S".to_string()).unwrap(),
            base_currency: None,
            quote_currency: Currency::try_from("USD".to_string()).unwrap(),
            price_scale: scale(6),
            native_granularity: granularity,
            gap: Some(GapCheck {
                max_seconds: 2,
                reopen_seconds: 60,
            }),
            frozen: Some(FrozenCheck {
                min_observations: 3,
                min_seconds: 5,
            }),
            jump: Some(JumpCheck {
                min_basis_points: 5,
            }),
            span: Some(SpanCheck { min_percent: 75 }),
            sessions: Some(vec![Session {
                name: "week".to_string(),
                open_seconds: 0,
                close_seconds: 604_800,
            }]),
            candles: candles
                .iter()
                .map(|&(duration_seconds, offset_seconds)| CandleSpec {
                    duration_seconds,
                    offset_seconds,
                    min_observations: None,
                    hard_min_observations: None,
                })
                .collect(),
        }
    }

    fn source(granularity: NativeGranularity, scale: Option<PriceScale>) -> Source {
        Source {
            generation: "source-generation".to_string(),
            source_kind: if scale.is_some() {
                SourceKind::TickCsv
            } else {
                SourceKind::BarParquet
            },
            role: DatasetRole::Development,
            native_granularity: granularity,
            price_scale: scale,
            capabilities: vec![if scale.is_some() {
                Capability::Ticks
            } else {
                Capability::Bars
            }],
        }
    }

    fn tick_stream(candles: &[(u32, u32)]) -> InstrumentStream {
        InstrumentStream::new(
            &instrument(NativeGranularity::Tick, candles),
            source(NativeGranularity::Tick, Some(scale(6))),
        )
        .unwrap()
    }

    fn tick(millis: i64, price_units: i64) -> Observation {
        Observation::Tick(Tick {
            event_time_micros: millis * 1_000,
            price_units,
        })
    }

    fn bar(start_s: i64, prices: [i64; 4], volume: f64) -> Observation {
        Observation::Bar(BarUnits {
            start_micros: start_s * SECOND,
            period_micros: 5 * SECOND,
            open: prices[0],
            high: prices[1],
            low: prices[2],
            close: prices[3],
            volume,
        })
    }

    fn feed(stream: &mut InstrumentStream, observations: &[Observation]) -> Vec<(usize, Candle)> {
        let mut out = Vec::new();
        for observation in observations {
            stream.push(*observation, &mut out).unwrap();
        }
        out
    }

    #[test]
    fn candles_finalize_only_when_a_later_record_closes_the_interval() {
        let mut stream = tick_stream(&[(5, 0)]);
        let out = feed(
            &mut stream,
            &[
                tick(0, 100),
                tick(1_000, 101),
                tick(4_900, 102),
                tick(5_000, 103),
                tick(12_000, 104),
            ],
        );
        assert_eq!(out.len(), 2, "the candle holding the last tick is withheld");
        let first = &out[0].1;
        assert_eq!(
            (
                first.open_time_micros,
                first.close_time_micros,
                first.known_at_micros
            ),
            (0, 5 * SECOND, 5 * SECOND)
        );
        assert_eq!(
            (
                first.open_units,
                first.high_units,
                first.low_units,
                first.close_units
            ),
            (100, 102, 100, 102)
        );
        assert_eq!(first.observations, 3);
        assert_eq!(first.first_event_micros, 0);
        assert_eq!(first.last_event_micros, 4_900_000);
        assert_eq!(first.active_span_micros, 4_900_000);
        assert_eq!(first.gap_before_micros, None);
        assert_eq!(first.max_gap_inside_micros, 3_900_000);
        assert!(first.flags.gap_inside && !first.flags.gap_before && !first.flags.complete());
        assert_eq!(first.volume, None);
        let second = &out[1].1;
        assert_eq!(second.open_time_micros, 5 * SECOND);
        assert_eq!(second.known_at_micros, 12 * SECOND);
        assert_eq!(second.observations, 1);
        assert_eq!(second.gap_before_micros, Some(100_000));
        assert!(second.flags.short_span && second.flags.complete() && !second.flags.clean());
        let profile = stream.profile();
        assert_eq!(profile.observations, 5);
        assert_eq!(profile.streams[0].finalized, 2);
        assert_eq!(profile.streams[0].withheld_observations, 1);
        assert_eq!(profile.streams[0].flagged.complete, 1);
        assert_eq!(profile.cadence.total(), 4);
        assert_eq!(profile.cadence.buckets()[20], 1, "one second");
        assert_eq!(profile.cadence.buckets()[17], 1, "a tenth of a second");
        let gaps = profile.gaps.unwrap();
        assert_eq!(
            (gaps.count, gaps.max_micros, gaps.total_micros),
            (2, 7 * SECOND, 10_900_000)
        );
        assert_eq!(
            profile.coverage.unwrap().last_event_time,
            "1970-01-01T00:00:12.000000Z"
        );
        assert_eq!(profile.prices.step_units, Some(1));
        assert_eq!(profile.prices.moves, 4);
        assert_eq!(profile.sessions.unwrap().windows[0].observations, 5);
        assert!(profile.calculations.iter().all(|c| c.supported));
    }

    #[test]
    fn identical_repeats_fold_while_conflicts_and_backwards_time_leave_state_unchanged() {
        let mut stream = tick_stream(&[(5, 0)]);
        let mut out = Vec::new();
        stream.push(tick(0, 100), &mut out).unwrap();
        stream.push(tick(0, 100), &mut out).unwrap();
        let conflict = stream.push(tick(0, 101), &mut out).unwrap_err();
        assert_eq!(conflict.reason, RejectionReason::ConflictingDuplicate);
        assert_eq!(conflict.source, "source-generation");
        assert!(
            conflict
                .to_string()
                .contains("conflicting_duplicate at 1970-01-01T00:00:00.000000Z")
        );
        let backwards = stream.push(tick(-1, 100), &mut out).unwrap_err();
        assert_eq!(backwards.reason, RejectionReason::BackwardsTime);
        assert_eq!(stream.profile().observations, 2);
        stream.push(tick(5_000, 100), &mut out).unwrap();
        let candle = &out[0].1;
        assert_eq!((candle.observations, candle.duplicates), (2, 1));
        assert_eq!(candle.frozen_observations, 2);
        assert_eq!(stream.profile().duplicates, 1);
    }

    #[test]
    fn activity_missing_buckets_and_span_follow_the_configured_thresholds() {
        let mut definition = instrument(NativeGranularity::Tick, &[(5, 0)]);
        definition.candles[0].min_observations = Some(3);
        definition.candles[0].hard_min_observations = Some(2);
        let mut stream =
            InstrumentStream::new(&definition, source(NativeGranularity::Tick, Some(scale(6))))
                .unwrap();
        let out = feed(
            &mut stream,
            &[tick(0, 1), tick(1_000, 1), tick(20_000, 1), tick(26_000, 1)],
        );
        let first = &out[0].1;
        assert!(first.flags.low_activity && !first.flags.hard_low_activity);
        assert!(first.flags.short_span, "1 s of 5 s is under 75 percent");
        assert!(first.flags.complete() && !first.flags.clean());
        let second = &out[1].1;
        assert_eq!(second.missing_buckets_before, 3);
        assert_eq!(second.gap_before_micros, Some(19 * SECOND));
        assert!(second.flags.low_activity && second.flags.hard_low_activity);
        assert!(second.flags.missing_before && second.flags.gap_before);
        assert!(!second.flags.complete());
        let facts = &stream.profile().streams[0];
        assert_eq!(facts.flagged.low_activity, 2);
        assert_eq!(facts.flagged.hard_low_activity, 1);
        assert_eq!(facts.flagged.missing_before, 1);
        assert_eq!(facts.flagged.clean, 0);
        assert_eq!(facts.activity.buckets(), &[0, 1, 1]);
    }

    #[test]
    fn a_run_starting_at_the_unix_epoch_keeps_its_span() {
        // The pinned resampler treats a run start of zero milliseconds as absent and reports a
        // zero span; the target keeps the epoch as a timestamp.
        let mut stream = tick_stream(&[(15, 0)]);
        let out = feed(&mut stream, &[tick(0, 1), tick(6_000, 1), tick(15_000, 2)]);
        assert_eq!(
            (out[0].1.frozen_observations, out[0].1.frozen_micros),
            (2, 6 * SECOND)
        );
        assert!(
            out[0].1.flags.frozen,
            "six seconds at one price reaches the five-second rule"
        );
    }

    #[test]
    fn frozen_runs_are_measured_inside_candles_and_across_the_stream() {
        let mut stream = tick_stream(&[(5, 0)]);
        let out = feed(
            &mut stream,
            &[
                tick(0, 1),
                tick(1_000, 1),
                tick(2_000, 1),
                tick(3_000, 2),
                tick(5_000, 2),
                tick(20_000, 2),
                tick(21_000, 3),
            ],
        );
        let first = &out[0].1;
        assert_eq!(
            (first.frozen_observations, first.frozen_micros),
            (3, 2 * SECOND)
        );
        assert!(first.flags.frozen);
        let second = &out[1].1;
        assert_eq!((second.frozen_observations, second.frozen_micros), (1, 0));
        assert!(!second.flags.frozen);
        let runs = stream.profile().frozen_runs.unwrap();
        assert_eq!(runs.count, 2, "three ticks at 1, then 17 seconds at 2");
        assert_eq!(runs.max_observations, 3);
        assert_eq!(runs.max_micros, 17 * SECOND);
    }

    #[test]
    fn jumps_split_by_gap_context_and_thresholds_compare_exactly() {
        let mut stream = tick_stream(&[(60, 0)]);
        let out = feed(
            &mut stream,
            &[
                tick(0, 1_000_000),
                tick(1_000, 1_000_500),
                tick(2_000, 1_000_999),
                tick(5_000, 1_001_600),
                tick(60_000, 1_001_600),
            ],
        );
        let candle = &out[0].1;
        assert_eq!(candle.max_jump_basis_points, 5, "exactly five basis points");
        assert_eq!(
            candle.max_delayed_jump_basis_points, 6,
            "three seconds is a delay"
        );
        assert_eq!(candle.max_reopen_jump_basis_points, 0);
        assert!(candle.flags.jump && candle.flags.delayed_jump && !candle.flags.reopen_jump);
        let jumps = stream.profile().jumps.unwrap();
        assert_eq!(
            (jumps.flagged, jumps.flagged_delayed, jumps.flagged_reopen),
            (1, 1, 0)
        );
        assert_eq!(jumps.basis_points.total(), 4);
        let mut reopened = tick_stream(&[(60, 0)]);
        let out = feed(
            &mut reopened,
            &[
                tick(0, 1_000_000),
                tick(60_000, 1_000_600),
                tick(120_000, 1_000_600),
            ],
        );
        assert!(
            !out[0].1.flags.reopen_jump,
            "the move enters the next candle"
        );
        assert!(out[1].1.flags.reopen_jump && !out[1].1.flags.delayed_jump);
        assert_eq!(out[1].1.max_reopen_jump_basis_points, 6);
        assert_eq!(out[1].1.gap_before_micros, Some(60 * SECOND));
        assert_eq!(reopened.profile().jumps.unwrap().flagged_reopen, 1);
        assert_eq!(relative_move(1_000_000, 1_000_500), (Some(5), true));
        assert_eq!(relative_move(0, 5), (None, true));
        assert_eq!(relative_move(-100, -110), (Some(1_000), true));
        assert_eq!(relative_move(7, 7), (Some(0), false));
        assert_eq!(relative_move(1, i64::MAX), (Some(MAX_BASIS_POINTS), true));
        assert_eq!(relative_move(i64::MIN, i64::MAX), (Some(19_999), true));
        assert_eq!(relative_move(1, i64::MIN), (Some(MAX_BASIS_POINTS), true));
        // The pinned resampler evaluates the same move in binary floating point and lands just
        // under the threshold; exact units decide the boundary case deterministically.
        let (from, to) = (
            std::hint::black_box(1.0_f64),
            std::hint::black_box(1.0005_f64),
        );
        assert!((to - from) / from * 10_000.0 < 5.0);
    }

    #[test]
    fn bars_aggregate_and_close_at_their_own_end() {
        let granularity = NativeGranularity::Bar { period_seconds: 5 };
        let mut stream = InstrumentStream::new(
            &instrument(granularity, &[(15, 5)]),
            source(granularity, None),
        )
        .unwrap();
        let out = feed(
            &mut stream,
            &[
                bar(5, [100_000, 100_002, 99_999, 100_001], 1.0),
                bar(10, [100_001, 100_003, 100_000, 100_002], 2.0),
                bar(15, [100_002, 100_002, 100_001, 100_001], 0.5),
            ],
        );
        assert_eq!(
            out.len(),
            1,
            "the bar ending at the close finalizes the candle at once"
        );
        let candle = &out[0].1;
        assert_eq!(
            (candle.open_time_micros, candle.close_time_micros),
            (5 * SECOND, 20 * SECOND)
        );
        assert_eq!(candle.known_at_micros, 20 * SECOND);
        assert_eq!(
            (
                candle.open_units,
                candle.high_units,
                candle.low_units,
                candle.close_units
            ),
            (100_000, 100_003, 99_999, 100_001)
        );
        assert_eq!(
            candle.max_jump_basis_points, 0,
            "two units of a hundred thousand"
        );
        assert_eq!((candle.observations, candle.volume), (3, Some(3.5)));
        assert_eq!(candle.last_event_micros, 15 * SECOND);
        assert_eq!(candle.active_span_micros, 15 * SECOND);
        assert_eq!(candle.max_gap_inside_micros, 0);
        assert!(candle.flags.clean());
        let profile = stream.profile();
        assert_eq!(profile.streams[0].withheld_observations, 0);
        assert_eq!(
            profile.cadence.buckets()[23],
            2,
            "five seconds start to start"
        );
        assert_eq!(
            profile.prices.moves, 0,
            "each bar opens at the previous close"
        );
        assert_eq!(profile.prices.step_units, Some(1));
        let flat = [100_001; 4];
        let out = feed(&mut stream, &[bar(20, flat, 0.0), bar(30, flat, 0.0)]);
        let candle = &out[0].1;
        assert_eq!(candle.open_time_micros, 20 * SECOND);
        assert_eq!(
            candle.known_at_micros,
            35 * SECOND,
            "the bar ending at the close is the witness"
        );
        assert_eq!(candle.max_gap_inside_micros, 5 * SECOND);
        assert!(candle.flags.gap_inside);
        assert_eq!(candle.frozen_observations, 2);
        assert_eq!(
            candle.frozen_micros,
            15 * SECOND,
            "two flat bars from 20 to 35"
        );
        assert!(candle.flags.frozen);
        let out = feed(
            &mut stream,
            &[bar(35, flat, 0.0), bar(40, flat, 0.0), bar(50, flat, 0.0)],
        );
        let candle = &out[0].1;
        assert_eq!(
            (candle.open_time_micros, candle.close_time_micros),
            (35 * SECOND, 50 * SECOND)
        );
        assert_eq!(
            candle.known_at_micros,
            55 * SECOND,
            "the next bar's end witnessed the close"
        );
        assert_eq!(candle.observations, 2);
        assert!(
            !candle.flags.gap_inside,
            "the missing bar at 45 precedes the next candle"
        );
        assert_eq!(stream.profile().streams[0].withheld_observations, 1);
        let mut out = Vec::new();
        let reasons: Vec<RejectionReason> = [
            bar(50, flat, 0.0),
            bar(57, flat, 0.0),
            Observation::Bar(BarUnits {
                period_micros: 10 * SECOND,
                ..match bar(60, flat, 0.0) {
                    Observation::Bar(bar) => bar,
                    Observation::Tick(_) => unreachable!(),
                }
            }),
            bar(60, flat, -1.0),
            bar(60, [100_001, 100_000, 100_001, 100_001], 0.0),
            tick(60_000, 100_001),
        ]
        .into_iter()
        .map(|observation| stream.push(observation, &mut out).unwrap_err().reason)
        .collect();
        assert_eq!(
            reasons,
            [
                RejectionReason::BackwardsTime,
                RejectionReason::OffGrid,
                RejectionReason::WrongGranularity,
                RejectionReason::NonFinite,
                RejectionReason::NonFinite,
                RejectionReason::WrongGranularity,
            ]
        );
        assert!(out.is_empty());
        assert_eq!(stream.profile().observations, 8);
        assert!(stream.profile().calculations.iter().all(|c| {
            !c.supported
                && c.reason
                    .as_deref()
                    .unwrap()
                    .contains("\"required\":\"ticks\"")
        }));
        let archive = Bar {
            provider: (),
            start_unix_s: 65,
            open: 1.25,
            high: 1.3,
            low: 1.2,
            close: 1.26,
            volume: 2.0,
            period_s: 5,
        };
        assert_eq!(
            Observation::from_bar(&archive, scale(2)).unwrap(),
            bar(65, [125, 130, 120, 126], 2.0)
        );
        assert!(Observation::from_bar(&archive, scale(1)).is_err());
        for invalid in [
            Bar {
                high: 1.0,
                ..archive
            },
            Bar {
                period_s: 0,
                ..archive
            },
        ] {
            assert!(Observation::from_bar(&invalid, scale(2)).is_err());
        }
        let mut tick_stream = tick_stream(&[(5, 0)]);
        assert_eq!(
            tick_stream
                .push(bar(0, [1, 1, 1, 1], 0.0), &mut out)
                .unwrap_err()
                .reason,
            RejectionReason::WrongGranularity
        );
    }

    #[test]
    fn the_first_bar_anchors_its_step_to_its_own_open_and_volume_sums_stay_finite() {
        let granularity = NativeGranularity::Bar { period_seconds: 5 };
        let mut stream = InstrumentStream::new(
            &instrument(granularity, &[(10, 0)]),
            source(granularity, None),
        )
        .unwrap();
        let mut out = Vec::new();
        stream
            .push(bar(0, [100, 101, 99, 100], f64::MAX), &mut out)
            .unwrap();
        assert_eq!(
            stream.profile().prices.step_units,
            Some(1),
            "high and low against the open"
        );
        let overflow = stream
            .push(bar(5, [102, 102, 102, 102], f64::MAX), &mut out)
            .unwrap_err();
        assert_eq!(overflow.reason, RejectionReason::NonFinite);
        assert_eq!(
            stream.profile().observations,
            1,
            "the refused bar changed nothing"
        );
        stream
            .push(bar(5, [102, 102, 102, 102], 1.0), &mut out)
            .unwrap();
        assert_eq!(out[0].1.volume, Some(f64::MAX + 1.0));
        assert_eq!(stream.profile().prices.step_units, Some(1));
        let mut lone = InstrumentStream::new(
            &instrument(granularity, &[(10, 0)]),
            source(granularity, None),
        )
        .unwrap();
        lone.push(bar(0, [100; 4], 0.0), &mut out).unwrap();
        assert_eq!(
            lone.profile().prices.step_units,
            None,
            "one flat bar shows no step"
        );
    }

    #[test]
    fn construction_applies_the_instrument_rules() {
        let mut zero_jump = instrument(NativeGranularity::Tick, &[(5, 0)]);
        zero_jump.jump = Some(JumpCheck {
            min_basis_points: 0,
        });
        let error =
            InstrumentStream::new(&zero_jump, source(NativeGranularity::Tick, Some(scale(6))))
                .unwrap_err();
        assert!(error.contains("jump.min_basis_points"), "{error}");
        let error = InstrumentStream::new(
            &instrument(NativeGranularity::Tick, &[(0, 0)]),
            source(NativeGranularity::Tick, Some(scale(6))),
        )
        .unwrap_err();
        assert!(error.contains("candles[0].duration_seconds"), "{error}");
    }

    #[test]
    fn the_price_step_is_an_unsigned_magnitude() {
        let mut stream = tick_stream(&[(5, 0)]);
        let mut out = Vec::new();
        for (index, price) in [i64::MIN, i64::MAX, i64::MAX - 3].into_iter().enumerate() {
            stream
                .push(tick(index as i64 * 1_000, price), &mut out)
                .unwrap();
        }
        assert_eq!(
            stream.profile().prices.step_units,
            Some(3),
            "the greatest common divisor of u64::MAX and 3"
        );
    }

    #[test]
    fn a_bar_source_never_supports_tick_calculations() {
        let granularity = NativeGranularity::Bar { period_seconds: 5 };
        let mut contradictory = source(granularity, None);
        contradictory.capabilities = vec![Capability::Bars, Capability::Ticks];
        let stream =
            InstrumentStream::new(&instrument(granularity, &[(10, 0)]), contradictory).unwrap();
        assert!(
            stream
                .profile()
                .calculations
                .iter()
                .all(|support| !support.supported && support.reason.is_some()),
            "every tick calculation stays unsupported on bars"
        );
    }

    #[test]
    fn prices_that_share_one_binary_float_stay_distinct() {
        // 100000000000.000000 and 100000000000.000001 are one `f64`; the pinned resampler
        // would see ten unchanged prices and a frozen candle. Exact units keep them apart.
        let mut stream = tick_stream(&[(5, 0)]);
        let mut out = Vec::new();
        for index in 0..10_i64 {
            let price = 100_000_000_000_000_000 + index % 2;
            stream
                .push(tick(10_100 + index * 500, price), &mut out)
                .unwrap();
        }
        stream
            .push(tick(15_100, 100_000_000_000_000_000), &mut out)
            .unwrap();
        let (_, candle) = &out[0];
        assert_eq!(candle.observations, 10);
        assert_eq!((candle.frozen_observations, candle.frozen_micros), (1, 0));
        assert!(!candle.flags.frozen);
        assert_eq!(stream.profile().prices.moves, 10);
        assert_eq!(stream.profile().prices.step_units, Some(1));
    }

    #[test]
    fn times_beyond_the_representable_range_are_refused() {
        let mut out = Vec::new();
        let mut ticks = tick_stream(&[(5, 0)]);
        let rejection = ticks
            .push(
                Observation::Tick(Tick {
                    event_time_micros: i64::MAX,
                    price_units: 1,
                }),
                &mut out,
            )
            .unwrap_err();
        assert_eq!(rejection.reason, RejectionReason::OutOfRange);
        assert!(rejection.to_string().contains("out_of_range"));
        assert_eq!(ticks.profile().observations, 0);
        let granularity = NativeGranularity::Bar { period_seconds: 5 };
        let mut bars = InstrumentStream::new(
            &instrument(granularity, &[(10, 0)]),
            source(granularity, None),
        )
        .unwrap();
        // The start converts to micros, but its end would not fit; a tick stream refuses the
        // same bar for its range before its granularity.
        let far = Bar {
            provider: (),
            start_unix_s: 9_223_372_036_850,
            open: 1.0,
            high: 1.0,
            low: 1.0,
            close: 1.0,
            volume: 0.0,
            period_s: 5,
        };
        for stream in [&mut bars, &mut ticks] {
            let rejection = stream
                .push(Observation::from_bar(&far, scale(2)).unwrap(), &mut out)
                .unwrap_err();
            assert_eq!(rejection.reason, RejectionReason::OutOfRange);
        }
        // A start inside the bound whose end lies beyond it is refused on its known-at time.
        let edge = Bar {
            start_unix_s: MAX_EVENT_MICROS / MICROS_PER_SECOND / 5 * 5,
            ..far
        };
        let rejection = bars
            .push(Observation::from_bar(&edge, scale(2)).unwrap(), &mut out)
            .unwrap_err();
        assert_eq!(rejection.reason, RejectionReason::OutOfRange);
        assert!(rejection.known_at_micros > MAX_EVENT_MICROS);
        assert_eq!(bars.profile().observations, 0);
        assert!(
            Observation::from_bar(
                &Bar {
                    start_unix_s: i64::MAX,
                    ..far
                },
                scale(2)
            )
            .is_err(),
            "a start that does not convert is an error, not a panic"
        );
        assert!(out.is_empty());
    }

    #[test]
    fn missing_buckets_keep_their_exact_count() {
        // 4,294,967,296 five-second intervals lie between the first two ticks: one more than a
        // 32-bit count can hold, and exactly what the pinned resampler reports.
        let mut stream = tick_stream(&[(5, 0)]);
        let mut out = Vec::new();
        for (index, seconds) in [10, 21_474_836_495, 21_474_836_500].into_iter().enumerate() {
            stream
                .push(tick(seconds * 1_000, 1_000_000 + index as i64), &mut out)
                .unwrap();
        }
        assert_eq!(out.len(), 2);
        assert_eq!(out[1].1.missing_buckets_before, 4_294_967_296);
        assert!(out[1].1.flags.missing_before);
    }

    #[test]
    fn a_wrong_period_rejection_keeps_the_bars_own_clocks() {
        let granularity = NativeGranularity::Bar { period_seconds: 5 };
        let mut stream = InstrumentStream::new(
            &instrument(granularity, &[(10, 0)]),
            source(granularity, None),
        )
        .unwrap();
        let mut out = Vec::new();
        let ten_second = Observation::Bar(BarUnits {
            start_micros: 60 * SECOND,
            period_micros: 10 * SECOND,
            open: 1,
            high: 1,
            low: 1,
            close: 1,
            volume: 0.0,
        });
        let rejection = stream.push(ten_second, &mut out).unwrap_err();
        assert_eq!(rejection.reason, RejectionReason::WrongGranularity);
        assert_eq!(
            (rejection.event_micros, rejection.known_at_micros),
            (60 * SECOND, 70 * SECOND)
        );
        assert_eq!(stream.profile().observations, 0);
    }

    #[test]
    fn counts_and_basis_points_do_not_wrap_at_thirty_two_bits() {
        // Seeded state stands in for 4,294,967,295 identical ticks inside one candle and one run.
        let mut stream = tick_stream(&[(5, 0)]);
        let mut out = Vec::new();
        stream.push(tick(10_000, 1_000_000), &mut out).unwrap();
        let working = stream.streams[0].working.as_mut().unwrap();
        working.observations = u64::from(u32::MAX);
        working.run.as_mut().unwrap().observations = u64::from(u32::MAX);
        stream.run.as_mut().unwrap().observations = u64::from(u32::MAX);
        stream.push(tick(11_000, 1_000_000), &mut out).unwrap();
        stream.push(tick(15_000, 1_000_001), &mut out).unwrap();
        let (_, candle) = &out[0];
        assert_eq!(candle.observations, u64::from(u32::MAX) + 1);
        assert_eq!(candle.frozen_observations, u64::from(u32::MAX) + 1);
        assert!(candle.flags.complete() && !candle.flags.low_activity);
        assert_eq!(
            stream.profile().frozen_runs.unwrap().max_observations,
            u64::from(u32::MAX) + 1
        );
        // A move from one unit to a million units is 9,999,990,000 basis points, as the
        // reference reports it.
        let mut stream = tick_stream(&[(5, 0)]);
        for (millis, price) in [(10_000, 1), (11_000, 1_000_000), (15_000, 1_000_001)] {
            stream.push(tick(millis, price), &mut out).unwrap();
        }
        assert_eq!(out[1].1.max_jump_basis_points, 9_999_990_000);
        assert_eq!(
            relative_move(1, i64::MAX).0,
            Some(MAX_BASIS_POINTS),
            "saturated at the column's limit"
        );
    }

    #[test]
    fn gaps_keep_their_exact_microseconds() {
        // The pinned resampler parses milliseconds, so it would see a two-second delay here
        // and no gap; the target keeps the extra microsecond and reports a gap. Parity inputs
        // must therefore carry whole-millisecond timestamps.
        let mut stream = tick_stream(&[(5, 0)]);
        let out = feed(
            &mut stream,
            &[
                Observation::Tick(Tick {
                    event_time_micros: 100_000,
                    price_units: 1,
                }),
                Observation::Tick(Tick {
                    event_time_micros: 2_100_001,
                    price_units: 1,
                }),
                tick(5_100, 1),
            ],
        );
        assert_eq!(out[0].1.max_gap_inside_micros, 2_000_001);
        assert!(out[0].1.flags.gap_inside && !out[0].1.flags.complete());
    }

    #[test]
    fn one_bar_can_close_a_stale_candle_and_its_own_interval() {
        let granularity = NativeGranularity::Bar { period_seconds: 5 };
        let mut stream = InstrumentStream::new(
            &instrument(granularity, &[(15, 5)]),
            source(granularity, None),
        )
        .unwrap();
        let flat = [100_001; 4];
        let out = feed(
            &mut stream,
            &[bar(20, flat, 0.0), bar(25, flat, 0.0), bar(45, flat, 0.0)],
        );
        assert_eq!(
            out.len(),
            2,
            "the stale candle and the bar's own candle both finalize"
        );
        assert_eq!(
            (
                out[0].1.open_time_micros,
                out[0].1.known_at_micros,
                out[0].1.observations
            ),
            (20 * SECOND, 50 * SECOND, 2)
        );
        assert_eq!(
            (
                out[1].1.open_time_micros,
                out[1].1.known_at_micros,
                out[1].1.observations
            ),
            (35 * SECOND, 50 * SECOND, 1)
        );
        assert_eq!(out[1].1.gap_before_micros, Some(15 * SECOND));
        assert_eq!(out[1].1.missing_buckets_before, 0);
        assert_eq!(stream.profile().streams[0].withheld_observations, 0);
    }

    #[test]
    fn binding_checks_capabilities_granularity_and_scale() {
        let bars = NativeGranularity::Bar { period_seconds: 5 };
        let error = InstrumentStream::new(
            &instrument(NativeGranularity::Tick, &[(5, 0)]),
            source(bars, None),
        )
        .unwrap_err();
        let rendered: serde_json::Value = serde_json::from_str(&error).unwrap();
        assert_eq!(rendered["required"], "ticks");
        assert_eq!(rendered["provided"], serde_json::json!(["bars"]));
        assert_eq!(rendered["instrument"], "b:S");
        assert_eq!(rendered["generation"], "source-generation");
        let error = InstrumentStream::new(
            &instrument(bars, &[(15, 5)]),
            source(NativeGranularity::Tick, Some(scale(6))),
        )
        .unwrap_err();
        assert!(error.contains("\"required\":\"bars\""));
        let error = InstrumentStream::new(
            &instrument(NativeGranularity::Tick, &[(5, 0)]),
            source(NativeGranularity::Tick, Some(scale(5))),
        )
        .unwrap_err();
        assert!(error.contains("price_scale 6") && error.contains("price_scale 5"));
        let mut mismatched = source(bars, None);
        mismatched.native_granularity = NativeGranularity::Bar { period_seconds: 10 };
        assert!(
            InstrumentStream::new(&instrument(bars, &[(15, 5)]), mismatched)
                .unwrap_err()
                .contains("declares 5-second bar granularity")
        );
    }

    /// A deterministic synthetic tick path with gaps, repeats, and jumps.
    fn synthetic_ticks(count: usize) -> Vec<Observation> {
        let mut state: u64 = 0x9E37_79B9_7F4A_7C15;
        let mut next = move || {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state
        };
        let mut time = 0;
        let mut price = 1_000_000;
        (0..count)
            .map(|_| {
                let draw = next();
                time += match draw % 50 {
                    0 => 9_000,
                    1 => 0,
                    2 => 70_000,
                    _ => 100 + (draw % 900) as i64,
                };
                if draw % 50 != 1 {
                    price += match (draw >> 8) % 40 {
                        0 => 700,
                        1 => -600,
                        2..=10 => 0,
                        n if n % 2 == 0 => 1,
                        _ => -1,
                    };
                }
                tick(time, price)
            })
            .collect()
    }

    #[test]
    fn truncating_later_input_cannot_change_finalized_output() {
        let ticks = synthetic_ticks(4_000);
        let streams = [(5, 0), (15, 5), (60, 30)];
        let mut full = tick_stream(&streams);
        let full_out = feed(&mut full, &ticks);
        let full_profile = full.profile();
        assert!(full_out.len() > 100);
        for percent in [25, 50, 75] {
            let cut = ticks.len() * percent / 100;
            let mut prefix = tick_stream(&streams);
            let prefix_out = feed(&mut prefix, &ticks[..cut]);
            assert_eq!(
                prefix_out,
                full_out[..prefix_out.len()],
                "{percent} percent"
            );
            let profile = prefix.profile();
            assert_eq!(
                profile.coverage.as_ref().unwrap().first_event_time,
                full_profile.coverage.as_ref().unwrap().first_event_time
            );
            assert_eq!(profile.observations, cut as u64);
            let pairs: Vec<(Observation, Observation)> = ticks[..cut]
                .windows(2)
                .map(|pair| (pair[0], pair[1]))
                .collect();
            assert_eq!(
                profile.duplicates,
                pairs.iter().filter(|(a, b)| a == b).count() as u64
            );
            let gaps = pairs
                .iter()
                .filter(|(a, b)| match (a, b) {
                    (Observation::Tick(a), Observation::Tick(b)) => {
                        b.event_time_micros - a.event_time_micros > 2 * SECOND
                    }
                    _ => unreachable!(),
                })
                .count() as u64;
            assert_eq!(profile.gaps.as_ref().unwrap().count, gaps);
            assert!(
                profile.frozen_runs.as_ref().unwrap().count
                    <= full_profile.frozen_runs.as_ref().unwrap().count
            );
            for (short, long) in profile.streams.iter().zip(&full_profile.streams) {
                assert!(short.finalized <= long.finalized);
                assert!(short.flagged.clean <= long.flagged.clean);
            }
        }
        let mut rebuilt = tick_stream(&streams);
        let mut out = Vec::new();
        for chunk in ticks.chunks(7) {
            for observation in chunk {
                rebuilt.push(*observation, &mut out).unwrap();
            }
        }
        assert_eq!(out, full_out);
        assert_eq!(rebuilt.profile(), full_profile);
    }

    #[test]
    fn sessions_count_records_by_weekly_window() {
        let mut definition = instrument(NativeGranularity::Tick, &[(5, 0)]);
        definition.sessions = Some(vec![Session {
            name: "monday".to_string(),
            open_seconds: 0,
            close_seconds: 86_400,
        }]);
        let mut stream =
            InstrumentStream::new(&definition, source(NativeGranularity::Tick, Some(scale(6))))
                .unwrap();
        let monday = 1_774_224_000_000; // 2026-03-23T00:00:00Z in milliseconds
        feed(
            &mut stream,
            &[
                tick(monday, 1),
                tick(monday + 86_399_999, 1),
                tick(monday + 86_400_000, 1),
                tick(monday + 7 * 86_400_000, 1),
            ],
        );
        let sessions = stream.profile().sessions.unwrap();
        assert_eq!(sessions.windows[0].observations, 3);
        assert_eq!(sessions.outside, 1);
    }

    #[test]
    fn histograms_bucket_by_bit_length() {
        let mut histogram = Histogram::default();
        for value in [0, 1, 2, 3, 4, 1_000_000] {
            histogram.observe(value);
        }
        assert_eq!(
            histogram.buckets(),
            &[
                1, 1, 2, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1
            ]
        );
        assert_eq!(histogram.total(), 6);
    }

    fn object(path: &str, sha256: &str) -> ObjectRecord {
        let sha256 = format!("{sha256:0>64}");
        ObjectRecord {
            role: ObjectRole::Normalized,
            path: path.to_string(),
            key: object_key(&sha256),
            bytes: 1,
            sha256,
            crc32c: None,
            generation: None,
        }
    }

    #[test]
    fn daily_stream_manifest_owns_daily_candles_and_profile() {
        let definition = instrument(NativeGranularity::Tick, &[(5, 0)]);
        let day = daily::tests::entry(crate::dataset::DayFamily::Candles);
        let mut manifest = StreamManifest {
            layout: Some(Layout::DailyV2),
            day_inventory: vec![day.clone()],
            kind: STREAM_MANIFEST_KIND.into(),
            schema_version: STREAM_SCHEMA_VERSION,
            generation: stream_generation_id_with_layout(
                "source",
                &definition.canonical_toml(),
                Some(Layout::DailyV2),
            ),
            broker: definition.broker.clone(),
            provider_symbol: definition.provider_symbol.clone(),
            instrument: definition.id().to_string(),
            role: DatasetRole::Development,
            source_generation: "source".into(),
            source_kind: SourceKind::TickParquetDaily,
            definition: definition.clone(),
            config_hash: "config".into(),
            code_revision: "fixture".into(),
            observations: 2,
            coverage: None,
            streams: vec![StreamSummary {
                duration_seconds: 5,
                offset_seconds: 0,
                rows: 2,
                first_open_time: day.first_time.clone(),
                last_close_time: Some("2026-09-18T00:00:00Z".into()),
            }],
            objects: vec![
                daily::tests::object(&day.logical_path().unwrap(), ObjectRole::Normalized),
                daily::tests::object("profile.json", ObjectRole::Normalized),
            ],
        };
        assert_eq!(
            StreamManifest::from_json(&manifest.to_json()).unwrap(),
            manifest
        );
        let legacy_id = stream_generation_id("source", &definition.canonical_toml());
        assert_ne!(manifest.generation, legacy_id);
        let mut bad = manifest.clone();
        bad.streams[0].rows += 1;
        assert!(
            StreamManifest::from_json(&bad.to_json())
                .unwrap_err()
                .contains("inventory rows")
        );
        let mut bad = manifest.clone();
        bad.day_inventory[0].duration = Some(10);
        assert!(StreamManifest::from_json(&bad.to_json()).is_err());
        let mut bad = manifest.clone();
        bad.layout = None;
        bad.generation = legacy_id;
        assert!(
            StreamManifest::from_json(&bad.to_json())
                .unwrap_err()
                .contains("requires layout")
        );
        let mut bad = manifest.clone();
        bad.objects.pop();
        assert!(
            StreamManifest::from_json(&bad.to_json())
                .unwrap_err()
                .contains("profile.json")
        );
        manifest.objects.push(daily::tests::object(
            "provenance/lineage.json",
            ObjectRole::Provenance,
        ));
        assert!(StreamManifest::from_json(&manifest.to_json()).is_err());
    }

    #[test]
    fn legacy_stream_identity_is_pinned() {
        assert_eq!(
            stream_generation_id("source", "definition"),
            "cbcfcf95ac592ee678061c858cbc3236be8d410335d971441960f16751cb18ea"
        );
    }

    #[test]
    fn stream_manifests_bind_identity_and_round_trip() {
        let definition = instrument(NativeGranularity::Tick, &[(5, 0), (15, 5)]);
        let manifest = StreamManifest {
            layout: None,
            day_inventory: Vec::new(),
            kind: STREAM_MANIFEST_KIND.to_string(),
            schema_version: STREAM_SCHEMA_VERSION,
            generation: stream_generation_id("source-generation", &definition.canonical_toml()),
            broker: definition.broker.clone(),
            provider_symbol: definition.provider_symbol.clone(),
            instrument: "b:S".to_string(),
            role: DatasetRole::Development,
            source_generation: "source-generation".to_string(),
            source_kind: SourceKind::TickCsv,
            definition: definition.clone(),
            config_hash: "v3:sha256:0".to_string(),
            code_revision: "unavailable".to_string(),
            observations: 3,
            coverage: None,
            streams: vec![
                StreamSummary {
                    duration_seconds: 5,
                    offset_seconds: 0,
                    rows: 1,
                    first_open_time: Some("1970-01-01T00:00:00.000000Z".to_string()),
                    last_close_time: Some("1970-01-01T00:00:05.000000Z".to_string()),
                },
                StreamSummary {
                    duration_seconds: 15,
                    offset_seconds: 5,
                    rows: 0,
                    first_open_time: None,
                    last_close_time: None,
                },
            ],
            objects: vec![
                object(PROFILE_OBJECT_PATH, "aa"),
                object("candles/5s_0s.parquet", "bb"),
                object("candles/15s_5s.parquet", "cc"),
            ],
        };
        let bytes = manifest.to_json();
        assert!(bytes.starts_with(b"{\n  \"kind\": \"instrument_stream\""));
        let parsed = StreamManifest::from_json(&bytes).unwrap();
        assert_eq!(parsed, manifest);
        assert_eq!(parsed.to_json(), bytes);
        assert_eq!(
            parsed.key(),
            format!("manifests/{}/ready.json", manifest.generation)
        );
        let mut renamed = manifest.clone();
        renamed.generation = "0".repeat(64);
        assert!(
            StreamManifest::from_json(&renamed.to_json())
                .unwrap_err()
                .contains("does not match")
        );
        let mut other_source = manifest.clone();
        other_source.source_generation = "other".to_string();
        assert!(StreamManifest::from_json(&other_source.to_json()).is_err());
        let mut missing = manifest.clone();
        missing.objects.pop();
        assert!(
            StreamManifest::from_json(&missing.to_json())
                .unwrap_err()
                .contains("candles/15s_5s.parquet")
        );
        let mut kind = manifest.clone();
        kind.kind = "dataset".to_string();
        assert!(
            StreamManifest::from_json(&kind.to_json())
                .unwrap_err()
                .contains("kind")
        );
        let mut mismatched = manifest.clone();
        mismatched.instrument = "b:T".to_string();
        assert!(
            StreamManifest::from_json(&mismatched.to_json())
                .unwrap_err()
                .contains("definition")
        );
        let profile = tick_stream(&[(5, 0)]).profile();
        assert_eq!(
            InstrumentProfile::from_json(&profile.to_json()).unwrap(),
            profile
        );
        assert!(profile.coverage.is_none() && profile.observations == 0);
    }
}

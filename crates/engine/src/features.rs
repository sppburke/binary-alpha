//! Causal features and regimes over the finalized candles of one instrument stream.
//!
//! The finite compiled output table, the immutable [`FeaturePlan`] that freezes membership,
//! parameters, and encodings, the ordered per-stream feature state that consumes accepted
//! candles and tick paths, and the pure encoder live here. The engine performs no file, network,
//! or cloud operation. `docs/contracts.md`, section "Feature plans", is the normative description.
//!
//! Every formula reproduces the pinned reference at its evidenced precision: floating-point
//! feature arithmetic follows the reference's operation order on prices converted from canonical
//! integer units, and the six-place normalized values the reference wrote to text before a
//! downstream stage read them are rounded at exactly those points. Canonical prices stay exact
//! integer units throughout.

use std::borrow::Cow;
use std::cmp::Ordering;
use std::collections::{HashMap, VecDeque};
use std::fmt;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::config::{
    Bins, EncodingSpec, Encodings, FeatureInstrument, Instrument, Outputs, StreamKey,
    StructureSettings,
};
use crate::dataset::{DatasetRole, ObjectRecord, ObjectRole, manifest_key, validate_objects};
use crate::market::{BrokerId, InstrumentId, PriceScale, ProviderSymbol, parse_price_units};
use crate::stream::{
    Candle, Flags, InstrumentProfile, InstrumentStream, Observation, Rejection, Source,
    StreamManifest, interval_open,
};

/// The plan and feature-manifest schema written and accepted by this checkout.
pub const FEATURE_SCHEMA_VERSION: u32 = 1;
/// The manifest `kind` of a feature generation.
pub const FEATURE_MANIFEST_KIND: &str = "feature_generation";
/// The object path of the plan inside a feature generation.
pub const PLAN_OBJECT_PATH: &str = "plan.json";

const RAW_IDENTITY_DOMAIN_V1: &[u8] = b"binary-alpha feature raw identity v1\n";
const PLAN_IDENTITY_DOMAIN_V1: &[u8] = b"binary-alpha feature plan v1\n";
const FEATURE_GENERATION_DOMAIN_V1: &[u8] = b"binary-alpha feature generation v1\n";

const MICROS_PER_SECOND: i64 = 1_000_000;

/// Names of the compiled policies a plan was resolved under; their literals are not
/// configuration.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Definitions {
    pub tick_path: String,
    pub gap_class: String,
    pub structure: String,
    pub sequence: String,
    pub candle_shape: String,
    pub moving_average: String,
    pub regime: String,
    pub eligibility: String,
    pub encoder: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub statistics: Option<String>,
}

/// The eligibility verdict every accepted row carries: the Phase 03 strict `clean` verdict.
const ELIGIBILITY_VERSION: &str = "clean_v1";

impl Definitions {
    /// The versions this checkout compiles.
    pub fn current() -> Self {
        Self {
            tick_path: "tick_path_pressure_v1".to_string(),
            gap_class: "gap_class_v1".to_string(),
            structure: "structure_labels_v1".to_string(),
            sequence: "structure_sequence_v1".to_string(),
            candle_shape: "candle_features_v1".to_string(),
            moving_average: "moving_average_v1".to_string(),
            regime: "regime_v1".to_string(),
            eligibility: ELIGIBILITY_VERSION.to_string(),
            encoder: "encoder_v1".to_string(),
            statistics: Some("rolling_statistics_v1".to_string()),
        }
    }
}

// Fixed compiled constants of the tick-path pressure policy.
const TICK_PATH_MIN_DIRECTIONAL_MOVES: u64 = 3;
const TICK_PATH_PRESSURE_THRESHOLD: f64 = 0.25;
const TICK_PATH_TERMINAL_PRESSURE_THRESHOLD: f64 = 0.34;
const TICK_PATH_MEDIUM_EFFICIENCY: f64 = 0.35;
const TICK_PATH_HIGH_EFFICIENCY: f64 = 0.65;
const TICK_PATH_CLEAN_PUSH_MAX_REVERSAL_RATE: f64 = 0.45;
const TICK_PATH_CHURN_REVERSAL_RATE: f64 = 0.60;
const TICK_PATH_CHURN_EFFICIENCY: f64 = 0.25;
const TICK_PATH_UP_CLOSE_POSITION: f64 = 0.60;
const TICK_PATH_DOWN_CLOSE_POSITION: f64 = 0.40;
const TICK_PATH_FAILED_UP_CLOSE_POSITION: f64 = 0.45;
const TICK_PATH_FAILED_DOWN_CLOSE_POSITION: f64 = 0.55;

/// The gap-class ladder over an inter-arrival time in microseconds, in worsening order.
const GAP_CLASSES: [(i64, &str); 7] = [
    (0, "duplicate_or_backwards"),
    (2_000_000, "normal_small_tick_delay"),
    (15_000_000, "medium_feed_delay"),
    (60_000_000, "large_feed_delay"),
    (300_000_000, "short_data_gap"),
    (1_800_000_000, "session_or_collection_gap"),
    (i64::MAX, "major_data_outage_or_session_break"),
];

fn gap_class(micros: i64) -> &'static str {
    GAP_CLASSES
        .iter()
        .find(|(limit, _)| micros <= *limit)
        .map_or(GAP_CLASSES[6].1, |(_, class)| class)
}

/// One typed feature value.
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Int(i64),
    Float(f64),
    Bool(bool),
    Text(Cow<'static, str>),
    /// Unix microseconds.
    Time(i64),
}

impl Value {
    fn text(text: &'static str) -> Self {
        Self::Text(Cow::Borrowed(text))
    }

    pub fn as_f64(&self) -> Option<f64> {
        match self {
            Self::Int(value) => Some(*value as f64),
            Self::Float(value) => Some(*value),
            _ => None,
        }
    }

    /// The label the encoder groups a category or boolean value under.
    pub fn as_label(&self) -> Option<Cow<'_, str>> {
        match self {
            Self::Text(text) => Some(Cow::Borrowed(text.as_ref())),
            Self::Bool(true) => Some(Cow::Borrowed("true")),
            Self::Bool(false) => Some(Cow::Borrowed("false")),
            _ => None,
        }
    }
}

crate::string_enum! {
    /// The physical type of one output.
    Kind "kind" {
        Int => "int",
        Float => "float",
        Bool => "bool",
        Text => "text",
        Time => "time",
    }
}

crate::string_enum! {
    /// The computation stage that owns an output, in chain order.
    Stage "stage" {
        Candle => "candle",
        TickPath => "tick_path",
        Anatomy => "anatomy",
        Rolling => "rolling",
        Structure => "structure",
        Sequence => "sequence",
        Shape => "shape",
        MovingAverage => "moving_average",
        Regime => "regime",
        Calendar => "calendar",
    }
}

/// The four confirmed-swing sequence types.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SwingType {
    Hh,
    Hl,
    Lh,
    Ll,
}

impl SwingType {
    const ALL: [Self; 4] = [Self::Hh, Self::Hl, Self::Lh, Self::Ll];

    fn as_str(self) -> &'static str {
        match self {
            Self::Hh => "HH",
            Self::Hl => "HL",
            Self::Lh => "LH",
            Self::Ll => "LL",
        }
    }
}

/// Which computed quantity an output reads.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Field {
    OpenTime,
    CloseTime,
    KnownAt,
    FirstEvent,
    LastEvent,
    CandleOrdinal,
    OpenUnits,
    HighUnits,
    LowUnits,
    CloseUnits,
    TickVolume,
    ActiveSpan,
    Complete,
    LowTickVolume,
    HardLowTickVolume,
    HasGap,
    HasInternalGap,
    StartsAfterGap,
    StartsAfterGapMicros,
    StartsAfterGapClass,
    MissingBuckets,
    MaxInternalGap,
    MaxGap,
    WorstGapClass,
    FrozenPriceFlag,
    MaxSamePriceRunTicks,
    MaxSamePriceRunMicros,
    HasTrueTickJump,
    HasFeedDelayJump,
    HasGapReopenJump,
    MaxAbsTickJumpBps,
    MaxTrueTickJumpBps,
    MaxGapReopenJumpBps,
    QualityTier,
    TickPathReady,
    TickPathDirectionalMoves,
    TickPathUpticks,
    TickPathDownticks,
    TickPathFlats,
    TickPathDirectionChanges,
    TickPathSignedImbalance,
    TickPathReversalRate,
    TickPathEfficiency,
    TickPathTerminalMoves,
    TickPathTerminalSignedImbalance,
    TickPathClosePosition,
    TickPathPressureBucket,
    TickPathShapeBucket,
    TickPathTerminalPressureBucket,
    TickPathFailedPressureDirection,
    TickPathEfficiencyBucket,
    TickPathReversalBucket,
    CandleDirection,
    BodyUnits,
    RangeUnits,
    UpperWickUnits,
    LowerWickUnits,
    BodyBps,
    RangeBps,
    UpperWickBps,
    LowerWickBps,
    ClosePosition,
    BodyToRange,
    UpperWickToRange,
    LowerWickToRange,
    Return1Bps,
    ReturnStd(u32),
    ReturnSkew(u32),
    ReturnKurtosis(u32),
    ReturnAutocorr(u32),
    SignReversalRate(u32),
    UpMoveRatio(u32),
    TrendR2(u32),
    TrendResidual(u32),
    RangePosition(u32),
    RangeOverlap,
    CandlePattern,
    Momentum(u32),
    Efficiency(u32),
    AbsReturnMean(u32),
    RangeMean(u32),
    TickVolumeMean(u32),
    RangeToAvg20,
    CompressionState,
    DirectionalState,
    RangeLike,
    TrendLegDirection,
    TrendLegAge,
    PullbackAgainstTrend,
    LastSwingHighUnits,
    LastSwingHighEventClose,
    LastSwingHighConfirmClose,
    BarsSinceSwingHigh,
    DistanceToSwingHighBps,
    LastSwingLowUnits,
    LastSwingLowEventClose,
    LastSwingLowConfirmClose,
    BarsSinceSwingLow,
    DistanceToSwingLowBps,
    NewlyConfirmedSwingHigh,
    NewlyConfirmedSwingLow,
    BreakoutUp,
    BreakoutDown,
    SweepRejectHigh,
    SweepRejectLow,
    FailedBreakoutUp,
    FailedBreakoutDown,
    BreakOfStructure,
    ChangeOfCharacter,
    StructureState,
    CurrentEventTypes,
    CurrentEventClose,
    SwingHighType,
    SwingLowType,
    LastSwingHighType,
    LastSwingLowType,
    NewlyConfirmed(SwingType),
    MarketStructureSequence,
    MarketStructureBias,
    LastConfirmedUnits(SwingType),
    LastConfirmedEventClose(SwingType),
    LastConfirmedConfirmClose(SwingType),
    BarsSinceConfirmed(SwingType),
    CleanSegmentIndex,
    CleanSegmentCandleIndex,
    PriorCleanHistoryCount,
    HasAdjacentPrevious,
    PreviousCandleRelation,
    CandleColor,
    CandleType,
    RangeBpsBucket,
    BodyBpsBucket,
    RangeVsRecentRatio,
    BodyVsRecentRatio,
    TickVolumeVsRecentRatio,
    RangeVsRecentBucket,
    BodyVsRecentBucket,
    TickVolumeVsRecentBucket,
    UpperWickSizeBucket,
    LowerWickSizeBucket,
    WickProfile,
    CloseLocationBucket,
    BodyDominance,
    IsDoji,
    IsStrongBody,
    IsPinBar,
    IsHammerLike,
    IsShootingStarLike,
    IsInsideBar,
    IsOutsideBar,
    IsExpansionCandle,
    IsCompressionCandle,
    Ema(u32),
    EmaReady(u32),
    CloseVsEmaBps(u32),
    CloseAboveEma(u32),
    EmaSlopeBps(u32),
    EmaSlopeState(u32),
    CloseVsEmaState(u32),
    Ema20MinusEma50Bps,
    Ema20AboveEma50,
    Ema20Ema50AlignmentState,
    RegimeTrendState,
    RegimeVolatilityState,
    RegimeStructureState,
    RegimeTransitionState,
    RegimeQualityState,
    RegimeDirectionalBias,
    RegimeComposite,
    IsRegimeClean,
    IsRegimeTrending,
    IsRegimeRanging,
    IsRegimeTransition,
    UtcDayOfWeek,
    UtcHour,
    UtcSession6h,
}

/// A prerequisite an output needs before it can be selected.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Req {
    Ticks,
    TickPath,
    GapCheck,
    FrozenCheck,
    JumpCheck,
    MinObservations,
    HardMinObservations,
    Structure,
    Window(u32),
    Epsilon,
    Relative,
    Period(u32),
}

/// One compiled output definition.
#[derive(Debug, Clone)]
struct OutputDef {
    name: String,
    field: Field,
    kind: Kind,
    stage: Stage,
    predictive: bool,
    requires: Vec<Req>,
}

/// When an output is available and what an unavailable value means, per field group.
fn readiness(field: Field) -> &'static str {
    use Field as F;
    match field {
        F::Return1Bps => {
            "unavailable on the first accepted candle; state advances across every accepted candle"
        }
        F::ReturnStd(_) => {
            "unavailable until the window has at least two unrounded returns and a prior candle; zero variance yields zero; non-finite results are unavailable"
        }
        F::ReturnSkew(_) => {
            "unavailable until the window has at least three unrounded returns and a prior candle; zero return variance or a non-finite result is unavailable"
        }
        F::ReturnKurtosis(_) => {
            "unavailable until the window has at least four unrounded returns and a prior candle; zero return variance or a non-finite result is unavailable"
        }
        F::ReturnAutocorr(_) => {
            "unavailable until the window has at least three unrounded returns and a prior candle; zero variance in either adjacent series or a non-finite result is unavailable"
        }
        F::SignReversalRate(_) => {
            "unavailable until the window has at least two adjacent unrounded return pairs and a prior candle; zero pairs with both returns nonzero or a non-finite result is unavailable"
        }
        F::UpMoveRatio(_) => {
            "unavailable until the window has at least two unrounded returns and a prior candle; zero absolute-return sum or a non-finite result is unavailable"
        }
        F::TrendR2(_) => {
            "unavailable until the window has at least three closes; zero close variance or a non-finite result is unavailable"
        }
        F::TrendResidual(_) => {
            "unavailable until the window has at least three closes; zero last close or a non-finite result is unavailable"
        }
        F::RangePosition(_) => {
            "unavailable until the window has at least two candles; zero high-low span or a non-finite result is unavailable"
        }
        F::RangeOverlap => {
            "unavailable unless the prior accepted candle is adjacent with no skipped or rejected stream interval, or when the current high-low range is zero; non-finite results are unavailable"
        }
        F::CandlePattern => {
            "unavailable unless the prior accepted candle is adjacent with no skipped or rejected stream interval; `none` for doji bodies or when neither opposite-body pattern holds"
        }
        F::Momentum(_) => {
            "unavailable until the window count of previous accepted candles; state advances across every accepted candle"
        }
        F::RangeMean(_) | F::TickVolumeMean(_) => {
            "unavailable until the window count of accepted candles; state advances across every accepted candle"
        }
        F::Efficiency(_) | F::AbsReturnMean(_) => {
            "unavailable until the window count of returns; efficiency also needs a positive absolute-return sum"
        }
        F::RangeToAvg20 => "unavailable until windows 5 and 20 fill with a positive 20-window mean",
        F::CompressionState => {
            "`unknown` until windows 5 and 20 fill with a positive 20-window mean"
        }
        F::DirectionalState => {
            "`unknown` while the direction window's momentum or efficiency is unavailable (the window unfilled, or a zero absolute-return sum); then `sideways`, `up`, or `down`"
        }
        F::RangeLike | F::PullbackAgainstTrend => "false until its inputs are available",
        F::TrendLegDirection => "`none` until a directional state holds",
        F::TrendLegAge => "zero until a directional state holds",
        F::LastSwingHighUnits
        | F::LastSwingHighEventClose
        | F::LastSwingHighConfirmClose
        | F::BarsSinceSwingHigh
        | F::DistanceToSwingHighBps => {
            "unavailable until a swing high is confirmed after the right-side candles close"
        }
        F::LastSwingLowUnits
        | F::LastSwingLowEventClose
        | F::LastSwingLowConfirmClose
        | F::BarsSinceSwingLow
        | F::DistanceToSwingLowBps => {
            "unavailable until a swing low is confirmed after the right-side candles close"
        }
        F::CurrentEventTypes => "empty text on a row without an event",
        F::CurrentEventClose => "unavailable on a row without an event",
        F::SwingHighType | F::SwingLowType => {
            "empty text on a row that confirms no swing of that side"
        }
        F::LastSwingHighType | F::LastSwingLowType => {
            "empty text until a swing of that side is confirmed"
        }
        F::MarketStructureSequence => {
            "`unknown` until both sides have a confirmed swing; `warming_up` while either is the first"
        }
        F::MarketStructureBias => {
            "`unknown` until both sides have a confirmed swing beyond their first"
        }
        F::LastConfirmedUnits(_)
        | F::LastConfirmedEventClose(_)
        | F::LastConfirmedConfirmClose(_)
        | F::BarsSinceConfirmed(_) => {
            "unavailable until a swing of that sequence type is confirmed"
        }
        F::PriorCleanHistoryCount => {
            "the count of adjacent accepted candles before this one; resets when the accepted ordinal is not adjacent"
        }
        F::RangeVsRecentRatio | F::BodyVsRecentRatio | F::TickVolumeVsRecentRatio => {
            "unavailable until `min_history` adjacent accepted candles with a positive mean; resets when the accepted ordinal is not adjacent"
        }
        F::RangeVsRecentBucket | F::BodyVsRecentBucket | F::TickVolumeVsRecentBucket => {
            "`unknown_warmup` while the ratio is unavailable"
        }
        F::IsExpansionCandle | F::IsCompressionCandle => {
            "false while the range ratio is unavailable"
        }
        F::PreviousCandleRelation => {
            "`first_clean_candle` on the first accepted candle and `no_adjacent_previous_clean_candle` after a non-adjacent ordinal"
        }
        F::IsInsideBar | F::IsOutsideBar => "false without an adjacent previous accepted candle",
        F::Ema(_) => {
            "numeric preview from the first close of a segment; resets when the accepted ordinal is not adjacent"
        }
        F::EmaReady(_) => "true once the period count of adjacent accepted candles has been seen",
        F::CloseVsEmaBps(_) | F::Ema20MinusEma50Bps => {
            "available from the first close of a segment unless the dividing average is zero; readiness is separate"
        }
        F::CloseAboveEma(_) | F::Ema20AboveEma50 => {
            "available from the first close of a segment; readiness is separate"
        }
        F::EmaSlopeBps(_) => {
            "unavailable on the first candle of a segment or when the previous average is zero"
        }
        F::EmaSlopeState(_) | F::CloseVsEmaState(_) | F::Ema20Ema50AlignmentState => {
            "`not_ready` until ready or while the value is unavailable"
        }
        F::TickPathReady => "true once the interval saw three directional moves",
        F::TickPathPressureBucket
        | F::TickPathShapeBucket
        | F::TickPathTerminalPressureBucket
        | F::TickPathFailedPressureDirection
        | F::TickPathEfficiencyBucket
        | F::TickPathReversalBucket => "`insufficient_tick_path` until three directional moves",
        F::TickPathDirectionalMoves
        | F::TickPathUpticks
        | F::TickPathDownticks
        | F::TickPathFlats
        | F::TickPathDirectionChanges
        | F::TickPathSignedImbalance
        | F::TickPathReversalRate
        | F::TickPathEfficiency
        | F::TickPathTerminalMoves
        | F::TickPathTerminalSignedImbalance
        | F::TickPathClosePosition => "available on every accepted candle of a tick-path stream",
        F::BodyBps | F::RangeBps | F::UpperWickBps | F::LowerWickBps => {
            "unavailable when the open is zero"
        }
        F::BodyUnits | F::RangeUnits | F::UpperWickUnits | F::LowerWickUnits => {
            "unavailable when the difference exceeds signed 64-bit units"
        }
        F::MaxAbsTickJumpBps | F::MaxTrueTickJumpBps | F::MaxGapReopenJumpBps => {
            "zero on a candle without a qualifying move"
        }
        F::RegimeTrendState
        | F::RegimeVolatilityState
        | F::RegimeStructureState
        | F::RegimeTransitionState
        | F::RegimeQualityState
        | F::RegimeDirectionalBias
        | F::RegimeComposite
        | F::IsRegimeClean
        | F::IsRegimeTrending
        | F::IsRegimeRanging
        | F::IsRegimeTransition => {
            "available on every accepted candle; an unavailable input reads as its component's `unknown` or `neutral`"
        }
        _ => "available on every accepted candle",
    }
}

/// The text values an output reads while it is not ready or its input is unavailable, per
/// field group; they complement `readiness`.
fn unready(field: Field) -> &'static [&'static str] {
    use Field as F;
    match field {
        F::CompressionState
        | F::DirectionalState
        | F::MarketStructureBias
        | F::RegimeTrendState
        | F::RegimeVolatilityState
        | F::RegimeStructureState
        | F::RegimeQualityState
        | F::RegimeDirectionalBias => &["unknown"],
        F::MarketStructureSequence => &["unknown", "warming_up"],
        F::RangeVsRecentBucket | F::BodyVsRecentBucket | F::TickVolumeVsRecentBucket => {
            &["unknown_warmup"]
        }
        F::EmaSlopeState(_) | F::CloseVsEmaState(_) | F::Ema20Ema50AlignmentState => &["not_ready"],
        F::TickPathPressureBucket
        | F::TickPathShapeBucket
        | F::TickPathTerminalPressureBucket
        | F::TickPathFailedPressureDirection
        | F::TickPathEfficiencyBucket
        | F::TickPathReversalBucket => &["insufficient_tick_path"],
        _ => &[],
    }
}

/// The readiness of one output: the boolean outputs that must be true and the text values
/// that mean not ready.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Readiness {
    pub flags: Vec<String>,
    pub unready: Vec<String>,
}

fn def(
    name: impl Into<String>,
    field: Field,
    kind: Kind,
    stage: Stage,
    predictive: bool,
    requires: &[Req],
) -> OutputDef {
    OutputDef {
        name: name.into(),
        field,
        kind,
        stage,
        predictive,
        requires: requires.to_vec(),
    }
}

/// The finite compiled output table for one set of formula settings, in chain order. The
/// configured moving-average periods and structure windows generate their members.
fn catalog(settings: &FormulaSettings) -> Vec<OutputDef> {
    use Field as F;
    use Kind::{Bool, Float, Int, Text, Time};
    use Req::*;
    use Stage as S;
    let t = |f, kind, stage, reqs: &[Req]| def(field_name(f), f, kind, stage, true, reqs);
    let m = |f, kind, stage, reqs: &[Req]| def(field_name(f), f, kind, stage, false, reqs);
    let mut table = vec![
        m(F::OpenTime, Time, S::Candle, &[]),
        m(F::CloseTime, Time, S::Candle, &[]),
        m(F::KnownAt, Time, S::Candle, &[]),
        m(F::FirstEvent, Time, S::Candle, &[]),
        m(F::LastEvent, Time, S::Candle, &[]),
        m(F::CandleOrdinal, Int, S::Candle, &[]),
        m(F::OpenUnits, Int, S::Candle, &[]),
        m(F::HighUnits, Int, S::Candle, &[]),
        m(F::LowUnits, Int, S::Candle, &[]),
        m(F::CloseUnits, Int, S::Candle, &[]),
        t(F::TickVolume, Int, S::Candle, &[Ticks]),
        t(F::ActiveSpan, Int, S::Candle, &[]),
        t(
            F::Complete,
            Bool,
            S::Candle,
            &[Ticks, GapCheck, HardMinObservations],
        ),
        t(F::LowTickVolume, Bool, S::Candle, &[Ticks, MinObservations]),
        t(
            F::HardLowTickVolume,
            Bool,
            S::Candle,
            &[Ticks, HardMinObservations],
        ),
        t(F::HasGap, Bool, S::Candle, &[Ticks, GapCheck]),
        t(F::HasInternalGap, Bool, S::Candle, &[Ticks, GapCheck]),
        t(F::StartsAfterGap, Bool, S::Candle, &[Ticks, GapCheck]),
        t(F::StartsAfterGapMicros, Int, S::Candle, &[Ticks]),
        t(F::StartsAfterGapClass, Text, S::Candle, &[Ticks]),
        t(F::MissingBuckets, Int, S::Candle, &[]),
        t(F::MaxInternalGap, Int, S::Candle, &[Ticks]),
        t(F::MaxGap, Int, S::Candle, &[Ticks]),
        t(F::WorstGapClass, Text, S::Candle, &[Ticks]),
        t(F::FrozenPriceFlag, Bool, S::Candle, &[Ticks, FrozenCheck]),
        t(F::MaxSamePriceRunTicks, Int, S::Candle, &[Ticks]),
        t(F::MaxSamePriceRunMicros, Int, S::Candle, &[Ticks]),
        t(F::HasTrueTickJump, Bool, S::Candle, &[Ticks, JumpCheck]),
        t(
            F::HasFeedDelayJump,
            Bool,
            S::Candle,
            &[Ticks, JumpCheck, GapCheck],
        ),
        t(
            F::HasGapReopenJump,
            Bool,
            S::Candle,
            &[Ticks, JumpCheck, GapCheck],
        ),
        t(F::MaxAbsTickJumpBps, Float, S::Candle, &[Ticks]),
        t(F::MaxTrueTickJumpBps, Float, S::Candle, &[Ticks, GapCheck]),
        t(F::MaxGapReopenJumpBps, Float, S::Candle, &[Ticks, GapCheck]),
        t(F::QualityTier, Text, S::Candle, &[]),
        t(F::TickPathReady, Bool, S::TickPath, &[TickPath]),
        t(F::TickPathDirectionalMoves, Int, S::TickPath, &[TickPath]),
        t(F::TickPathUpticks, Int, S::TickPath, &[TickPath]),
        t(F::TickPathDownticks, Int, S::TickPath, &[TickPath]),
        t(F::TickPathFlats, Int, S::TickPath, &[TickPath]),
        t(F::TickPathDirectionChanges, Int, S::TickPath, &[TickPath]),
        t(F::TickPathSignedImbalance, Float, S::TickPath, &[TickPath]),
        t(F::TickPathReversalRate, Float, S::TickPath, &[TickPath]),
        t(F::TickPathEfficiency, Float, S::TickPath, &[TickPath]),
        t(F::TickPathTerminalMoves, Int, S::TickPath, &[TickPath]),
        t(
            F::TickPathTerminalSignedImbalance,
            Float,
            S::TickPath,
            &[TickPath],
        ),
        t(F::TickPathClosePosition, Float, S::TickPath, &[TickPath]),
        t(F::TickPathPressureBucket, Text, S::TickPath, &[TickPath]),
        t(F::TickPathShapeBucket, Text, S::TickPath, &[TickPath]),
        t(
            F::TickPathTerminalPressureBucket,
            Text,
            S::TickPath,
            &[TickPath],
        ),
        t(
            F::TickPathFailedPressureDirection,
            Text,
            S::TickPath,
            &[TickPath],
        ),
        t(F::TickPathEfficiencyBucket, Text, S::TickPath, &[TickPath]),
        t(F::TickPathReversalBucket, Text, S::TickPath, &[TickPath]),
        t(F::CandleDirection, Text, S::Anatomy, &[]),
        m(F::BodyUnits, Int, S::Anatomy, &[]),
        m(F::RangeUnits, Int, S::Anatomy, &[]),
        m(F::UpperWickUnits, Int, S::Anatomy, &[]),
        m(F::LowerWickUnits, Int, S::Anatomy, &[]),
        t(F::BodyBps, Float, S::Anatomy, &[]),
        t(F::RangeBps, Float, S::Anatomy, &[]),
        t(F::UpperWickBps, Float, S::Anatomy, &[]),
        t(F::LowerWickBps, Float, S::Anatomy, &[]),
        t(F::ClosePosition, Float, S::Anatomy, &[]),
        t(F::BodyToRange, Float, S::Anatomy, &[]),
        t(F::UpperWickToRange, Float, S::Anatomy, &[]),
        t(F::LowerWickToRange, Float, S::Anatomy, &[]),
        t(F::RangeOverlap, Float, S::Anatomy, &[]),
        t(F::CandlePattern, Text, S::Anatomy, &[]),
        t(F::Return1Bps, Float, S::Rolling, &[Structure]),
    ];
    if let Some(structure) = &settings.structure {
        for &w in &structure.rolling_windows {
            let req = &[Structure, Window(w)];
            if w >= 2 {
                for field in [F::ReturnStd(w), F::UpMoveRatio(w), F::RangePosition(w)] {
                    table.push(t(field, Float, S::Rolling, req));
                }
            }
            if w >= 3 {
                for field in [F::ReturnSkew(w), F::TrendR2(w), F::TrendResidual(w)] {
                    table.push(t(field, Float, S::Rolling, req));
                }
            }
            if w >= 4 {
                for field in [
                    F::ReturnKurtosis(w),
                    F::ReturnAutocorr(w),
                    F::SignReversalRate(w),
                ] {
                    table.push(t(field, Float, S::Rolling, req));
                }
            }
            table.push(t(
                F::Momentum(w),
                Float,
                S::Rolling,
                &[Structure, Window(w)],
            ));
            table.push(t(
                F::Efficiency(w),
                Float,
                S::Rolling,
                &[Structure, Window(w)],
            ));
            table.push(t(
                F::AbsReturnMean(w),
                Float,
                S::Rolling,
                &[Structure, Window(w)],
            ));
            table.push(t(
                F::RangeMean(w),
                Float,
                S::Rolling,
                &[Structure, Window(w)],
            ));
            table.push(t(
                F::TickVolumeMean(w),
                Float,
                S::Rolling,
                &[Structure, Window(w), Ticks],
            ));
        }
    }
    let compression = &[Structure, Window(5), Window(20)];
    let direction = &[Structure];
    table.extend([
        t(F::RangeToAvg20, Float, S::Rolling, compression),
        t(F::CompressionState, Text, S::Rolling, compression),
        t(F::DirectionalState, Text, S::Rolling, direction),
        t(F::RangeLike, Bool, S::Rolling, compression),
        t(F::TrendLegDirection, Text, S::Rolling, direction),
        t(F::TrendLegAge, Int, S::Rolling, direction),
        t(F::PullbackAgainstTrend, Bool, S::Rolling, direction),
        m(F::LastSwingHighUnits, Int, S::Structure, direction),
        m(F::LastSwingHighEventClose, Time, S::Structure, direction),
        m(F::LastSwingHighConfirmClose, Time, S::Structure, direction),
        t(F::BarsSinceSwingHigh, Int, S::Structure, direction),
        t(F::DistanceToSwingHighBps, Float, S::Structure, direction),
        m(F::LastSwingLowUnits, Int, S::Structure, direction),
        m(F::LastSwingLowEventClose, Time, S::Structure, direction),
        m(F::LastSwingLowConfirmClose, Time, S::Structure, direction),
        t(F::BarsSinceSwingLow, Int, S::Structure, direction),
        t(F::DistanceToSwingLowBps, Float, S::Structure, direction),
        t(F::NewlyConfirmedSwingHigh, Bool, S::Structure, direction),
        t(F::NewlyConfirmedSwingLow, Bool, S::Structure, direction),
        t(F::BreakoutUp, Bool, S::Structure, direction),
        t(F::BreakoutDown, Bool, S::Structure, direction),
        t(F::SweepRejectHigh, Bool, S::Structure, direction),
        t(F::SweepRejectLow, Bool, S::Structure, direction),
        t(F::FailedBreakoutUp, Bool, S::Structure, direction),
        t(F::FailedBreakoutDown, Bool, S::Structure, direction),
        t(F::BreakOfStructure, Bool, S::Structure, direction),
        t(F::ChangeOfCharacter, Bool, S::Structure, direction),
        t(F::StructureState, Text, S::Structure, compression),
        t(F::CurrentEventTypes, Text, S::Structure, direction),
        m(F::CurrentEventClose, Time, S::Structure, direction),
    ]);
    let sequence = &[Structure, Epsilon];
    table.extend([
        t(F::SwingHighType, Text, S::Sequence, sequence),
        t(F::SwingLowType, Text, S::Sequence, sequence),
        t(F::LastSwingHighType, Text, S::Sequence, sequence),
        t(F::LastSwingLowType, Text, S::Sequence, sequence),
    ]);
    for swing in SwingType::ALL {
        table.push(t(F::NewlyConfirmed(swing), Bool, S::Sequence, sequence));
    }
    table.extend([
        t(F::MarketStructureSequence, Text, S::Sequence, sequence),
        t(F::MarketStructureBias, Text, S::Sequence, sequence),
    ]);
    for swing in SwingType::ALL {
        table.push(m(F::LastConfirmedUnits(swing), Int, S::Sequence, sequence));
        table.push(m(
            F::LastConfirmedEventClose(swing),
            Time,
            S::Sequence,
            sequence,
        ));
        table.push(m(
            F::LastConfirmedConfirmClose(swing),
            Time,
            S::Sequence,
            sequence,
        ));
        table.push(t(F::BarsSinceConfirmed(swing), Int, S::Sequence, sequence));
    }
    table.extend([
        m(F::CleanSegmentIndex, Int, S::Shape, &[]),
        m(F::CleanSegmentCandleIndex, Int, S::Shape, &[]),
        m(F::PriorCleanHistoryCount, Int, S::Shape, &[Relative]),
        t(F::HasAdjacentPrevious, Bool, S::Shape, &[]),
        t(F::PreviousCandleRelation, Text, S::Shape, &[]),
        t(F::CandleColor, Text, S::Shape, &[]),
        t(F::CandleType, Text, S::Shape, &[]),
        t(F::RangeBpsBucket, Text, S::Shape, &[]),
        t(F::BodyBpsBucket, Text, S::Shape, &[]),
        t(F::RangeVsRecentRatio, Float, S::Shape, &[Relative]),
        t(F::BodyVsRecentRatio, Float, S::Shape, &[Relative]),
        t(
            F::TickVolumeVsRecentRatio,
            Float,
            S::Shape,
            &[Relative, Ticks],
        ),
        t(F::RangeVsRecentBucket, Text, S::Shape, &[Relative]),
        t(F::BodyVsRecentBucket, Text, S::Shape, &[Relative]),
        t(
            F::TickVolumeVsRecentBucket,
            Text,
            S::Shape,
            &[Relative, Ticks],
        ),
        t(F::UpperWickSizeBucket, Text, S::Shape, &[]),
        t(F::LowerWickSizeBucket, Text, S::Shape, &[]),
        t(F::WickProfile, Text, S::Shape, &[]),
        t(F::CloseLocationBucket, Text, S::Shape, &[]),
        t(F::BodyDominance, Text, S::Shape, &[]),
        t(F::IsDoji, Bool, S::Shape, &[]),
        t(F::IsStrongBody, Bool, S::Shape, &[]),
        t(F::IsPinBar, Bool, S::Shape, &[]),
        t(F::IsHammerLike, Bool, S::Shape, &[]),
        t(F::IsShootingStarLike, Bool, S::Shape, &[]),
        t(F::IsInsideBar, Bool, S::Shape, &[]),
        t(F::IsOutsideBar, Bool, S::Shape, &[]),
        t(F::IsExpansionCandle, Bool, S::Shape, &[Relative]),
        t(F::IsCompressionCandle, Bool, S::Shape, &[Relative]),
    ]);
    for &p in &settings.moving_average_periods {
        let period = &[Period(p)];
        table.push(t(F::Ema(p), Float, S::MovingAverage, period));
        table.push(t(F::EmaReady(p), Bool, S::MovingAverage, period));
        table.push(t(F::CloseVsEmaBps(p), Float, S::MovingAverage, period));
        table.push(t(F::CloseAboveEma(p), Bool, S::MovingAverage, period));
        table.push(t(F::EmaSlopeBps(p), Float, S::MovingAverage, period));
        table.push(t(F::EmaSlopeState(p), Text, S::MovingAverage, period));
        table.push(t(F::CloseVsEmaState(p), Text, S::MovingAverage, period));
    }
    let pair = &[Period(20), Period(50)];
    table.extend([
        t(F::Ema20MinusEma50Bps, Float, S::MovingAverage, pair),
        t(F::Ema20AboveEma50, Bool, S::MovingAverage, pair),
        t(F::Ema20Ema50AlignmentState, Text, S::MovingAverage, pair),
    ]);
    let trend = &[Structure, Window(5), Window(10), Window(20), Epsilon];
    let quality = &[
        Ticks,
        GapCheck,
        FrozenCheck,
        JumpCheck,
        MinObservations,
        HardMinObservations,
    ];
    let composite = &[
        Structure,
        Window(5),
        Window(10),
        Window(20),
        Epsilon,
        Ticks,
        GapCheck,
        FrozenCheck,
        JumpCheck,
        MinObservations,
        HardMinObservations,
    ];
    table.extend([
        t(F::RegimeTrendState, Text, S::Regime, trend),
        t(F::RegimeVolatilityState, Text, S::Regime, compression),
        t(F::RegimeStructureState, Text, S::Regime, sequence),
        t(F::RegimeTransitionState, Text, S::Regime, direction),
        t(F::RegimeQualityState, Text, S::Regime, quality),
        t(F::RegimeDirectionalBias, Text, S::Regime, trend),
        t(F::RegimeComposite, Text, S::Regime, composite),
        t(F::IsRegimeClean, Bool, S::Regime, quality),
        t(F::IsRegimeTrending, Bool, S::Regime, trend),
        t(F::IsRegimeRanging, Bool, S::Regime, trend),
        t(F::IsRegimeTransition, Bool, S::Regime, direction),
        m(F::UtcDayOfWeek, Text, S::Calendar, &[]),
        m(F::UtcHour, Int, S::Calendar, &[]),
        m(F::UtcSession6h, Text, S::Calendar, &[]),
    ]);
    table
}

/// The canonical output identifier of a field.
fn field_name(field: Field) -> String {
    use Field as F;
    let fixed = match field {
        F::OpenTime => "open_time_micros",
        F::CloseTime => "close_time_micros",
        F::KnownAt => "known_at_micros",
        F::FirstEvent => "first_event_micros",
        F::LastEvent => "last_event_micros",
        F::CandleOrdinal => "candle_ordinal",
        F::OpenUnits => "open_units",
        F::HighUnits => "high_units",
        F::LowUnits => "low_units",
        F::CloseUnits => "close_units",
        F::TickVolume => "tick_volume",
        F::ActiveSpan => "active_span_micros",
        F::Complete => "complete",
        F::LowTickVolume => "low_tick_volume",
        F::HardLowTickVolume => "hard_low_tick_volume",
        F::HasGap => "has_gap",
        F::HasInternalGap => "has_internal_gap",
        F::StartsAfterGap => "starts_after_gap",
        F::StartsAfterGapMicros => "starts_after_gap_micros",
        F::StartsAfterGapClass => "starts_after_gap_class",
        F::MissingBuckets => "missing_buckets_since_prev_candle",
        F::MaxInternalGap => "max_internal_gap_micros",
        F::MaxGap => "max_gap_micros",
        F::WorstGapClass => "worst_gap_class",
        F::FrozenPriceFlag => "frozen_price_flag",
        F::MaxSamePriceRunTicks => "max_same_price_run_ticks",
        F::MaxSamePriceRunMicros => "max_same_price_run_micros",
        F::HasTrueTickJump => "has_true_tick_jump",
        F::HasFeedDelayJump => "has_feed_delay_jump",
        F::HasGapReopenJump => "has_gap_reopen_jump",
        F::MaxAbsTickJumpBps => "max_abs_tick_jump_bps",
        F::MaxTrueTickJumpBps => "max_true_tick_jump_bps",
        F::MaxGapReopenJumpBps => "max_gap_reopen_jump_bps",
        F::QualityTier => "quality_tier",
        F::TickPathReady => "tick_path_ready",
        F::TickPathDirectionalMoves => "tick_path_directional_move_count",
        F::TickPathUpticks => "tick_path_uptick_count",
        F::TickPathDownticks => "tick_path_downtick_count",
        F::TickPathFlats => "tick_path_flat_count",
        F::TickPathDirectionChanges => "tick_path_direction_change_count",
        F::TickPathSignedImbalance => "tick_path_signed_imbalance",
        F::TickPathReversalRate => "tick_path_reversal_rate",
        F::TickPathEfficiency => "tick_path_efficiency",
        F::TickPathTerminalMoves => "tick_path_terminal_move_count",
        F::TickPathTerminalSignedImbalance => "tick_path_terminal_signed_imbalance",
        F::TickPathClosePosition => "tick_path_close_position",
        F::TickPathPressureBucket => "tick_path_pressure_bucket",
        F::TickPathShapeBucket => "tick_path_shape_bucket",
        F::TickPathTerminalPressureBucket => "tick_path_terminal_pressure_bucket",
        F::TickPathFailedPressureDirection => "tick_path_failed_pressure_direction",
        F::TickPathEfficiencyBucket => "tick_path_efficiency_bucket",
        F::TickPathReversalBucket => "tick_path_reversal_bucket",
        F::CandleDirection => "candle_direction",
        F::BodyUnits => "body_units",
        F::RangeUnits => "range_units",
        F::UpperWickUnits => "upper_wick_units",
        F::LowerWickUnits => "lower_wick_units",
        F::BodyBps => "body_bps",
        F::RangeBps => "range_bps",
        F::UpperWickBps => "upper_wick_bps",
        F::LowerWickBps => "lower_wick_bps",
        F::ClosePosition => "close_position",
        F::BodyToRange => "body_to_range",
        F::UpperWickToRange => "upper_wick_to_range",
        F::LowerWickToRange => "lower_wick_to_range",
        F::Return1Bps => "return_1_bps",
        F::ReturnStd(w) => return format!("return_std_{w}_bps"),
        F::ReturnSkew(w) => return format!("return_skew_{w}"),
        F::ReturnKurtosis(w) => return format!("return_kurtosis_{w}"),
        F::ReturnAutocorr(w) => return format!("return_autocorr_{w}"),
        F::SignReversalRate(w) => return format!("sign_reversal_rate_{w}"),
        F::UpMoveRatio(w) => return format!("up_move_ratio_{w}"),
        F::TrendR2(w) => return format!("trend_r2_{w}"),
        F::TrendResidual(w) => return format!("trend_residual_{w}_bps"),
        F::RangePosition(w) => return format!("range_position_{w}"),
        F::RangeOverlap => "range_overlap",
        F::CandlePattern => "candle_pattern",
        F::Momentum(w) => return format!("momentum_{w}_bps"),
        F::Efficiency(w) => return format!("directional_efficiency_{w}"),
        F::AbsReturnMean(w) => return format!("abs_return_mean_{w}_bps"),
        F::RangeMean(w) => return format!("range_mean_{w}_bps"),
        F::TickVolumeMean(w) => return format!("tick_volume_mean_{w}"),
        F::RangeToAvg20 => "range_to_avg20",
        F::CompressionState => "compression_state",
        F::DirectionalState => "directional_state",
        F::RangeLike => "range_like",
        F::TrendLegDirection => "trend_leg_direction",
        F::TrendLegAge => "trend_leg_age_candles",
        F::PullbackAgainstTrend => "pullback_against_trend",
        F::LastSwingHighUnits => "last_swing_high_units",
        F::LastSwingHighEventClose => "last_swing_high_event_close_micros",
        F::LastSwingHighConfirmClose => "last_swing_high_confirm_close_micros",
        F::BarsSinceSwingHigh => "bars_since_last_swing_high_known",
        F::DistanceToSwingHighBps => "distance_to_last_swing_high_bps",
        F::LastSwingLowUnits => "last_swing_low_units",
        F::LastSwingLowEventClose => "last_swing_low_event_close_micros",
        F::LastSwingLowConfirmClose => "last_swing_low_confirm_close_micros",
        F::BarsSinceSwingLow => "bars_since_last_swing_low_known",
        F::DistanceToSwingLowBps => "distance_to_last_swing_low_bps",
        F::NewlyConfirmedSwingHigh => "newly_confirmed_swing_high",
        F::NewlyConfirmedSwingLow => "newly_confirmed_swing_low",
        F::BreakoutUp => "breakout_up",
        F::BreakoutDown => "breakout_down",
        F::SweepRejectHigh => "sweep_reject_high",
        F::SweepRejectLow => "sweep_reject_low",
        F::FailedBreakoutUp => "failed_breakout_up",
        F::FailedBreakoutDown => "failed_breakout_down",
        F::BreakOfStructure => "break_of_structure",
        F::ChangeOfCharacter => "change_of_character",
        F::StructureState => "structure_state",
        F::CurrentEventTypes => "current_event_types",
        F::CurrentEventClose => "current_event_close_micros",
        F::SwingHighType => "swing_high_type",
        F::SwingLowType => "swing_low_type",
        F::LastSwingHighType => "last_swing_high_type",
        F::LastSwingLowType => "last_swing_low_type",
        F::NewlyConfirmed(swing) => return format!("newly_confirmed_{}", swing.as_str()),
        F::MarketStructureSequence => "market_structure_sequence",
        F::MarketStructureBias => "market_structure_bias",
        F::LastConfirmedUnits(swing) => {
            return format!("last_confirmed_{}_units", swing.as_str());
        }
        F::LastConfirmedEventClose(swing) => {
            return format!("last_confirmed_{}_event_close_micros", swing.as_str());
        }
        F::LastConfirmedConfirmClose(swing) => {
            return format!("last_confirmed_{}_confirm_close_micros", swing.as_str());
        }
        F::BarsSinceConfirmed(swing) => {
            return format!("bars_since_last_confirmed_{}_known", swing.as_str());
        }
        F::CleanSegmentIndex => "clean_segment_index",
        F::CleanSegmentCandleIndex => "clean_segment_candle_index",
        F::PriorCleanHistoryCount => "prior_clean_history_count",
        F::HasAdjacentPrevious => "has_adjacent_previous_clean_candle",
        F::PreviousCandleRelation => "previous_candle_relation",
        F::CandleColor => "candle_color",
        F::CandleType => "candle_type",
        F::RangeBpsBucket => "range_bps_bucket",
        F::BodyBpsBucket => "body_bps_bucket",
        F::RangeVsRecentRatio => "range_vs_recent_ratio",
        F::BodyVsRecentRatio => "body_vs_recent_ratio",
        F::TickVolumeVsRecentRatio => "tick_volume_vs_recent_ratio",
        F::RangeVsRecentBucket => "range_vs_recent_bucket",
        F::BodyVsRecentBucket => "body_vs_recent_bucket",
        F::TickVolumeVsRecentBucket => "tick_volume_vs_recent_bucket",
        F::UpperWickSizeBucket => "upper_wick_size_bucket",
        F::LowerWickSizeBucket => "lower_wick_size_bucket",
        F::WickProfile => "wick_profile",
        F::CloseLocationBucket => "close_location_bucket",
        F::BodyDominance => "body_dominance",
        F::IsDoji => "is_doji",
        F::IsStrongBody => "is_strong_body",
        F::IsPinBar => "is_pin_bar",
        F::IsHammerLike => "is_hammer_like",
        F::IsShootingStarLike => "is_shooting_star_like",
        F::IsInsideBar => "is_inside_bar",
        F::IsOutsideBar => "is_outside_bar",
        F::IsExpansionCandle => "is_expansion_candle",
        F::IsCompressionCandle => "is_compression_candle",
        F::Ema(p) => return format!("ema{p}"),
        F::EmaReady(p) => return format!("is_ema{p}_ready"),
        F::CloseVsEmaBps(p) => return format!("close_vs_ema{p}_bps"),
        F::CloseAboveEma(p) => return format!("is_close_above_ema{p}"),
        F::EmaSlopeBps(p) => return format!("ema{p}_slope_bps"),
        F::EmaSlopeState(p) => return format!("ema{p}_slope_state"),
        F::CloseVsEmaState(p) => return format!("close_vs_ema{p}_state"),
        F::Ema20MinusEma50Bps => "ema20_minus_ema50_bps",
        F::Ema20AboveEma50 => "is_ema20_above_ema50",
        F::Ema20Ema50AlignmentState => "ema20_ema50_alignment_state",
        F::RegimeTrendState => "regime_trend_state",
        F::RegimeVolatilityState => "regime_volatility_state",
        F::RegimeStructureState => "regime_structure_state",
        F::RegimeTransitionState => "regime_transition_state",
        F::RegimeQualityState => "regime_quality_state",
        F::RegimeDirectionalBias => "regime_directional_bias",
        F::RegimeComposite => "regime_v1",
        F::IsRegimeClean => "is_regime_clean",
        F::IsRegimeTrending => "is_regime_trending",
        F::IsRegimeRanging => "is_regime_ranging",
        F::IsRegimeTransition => "is_regime_transition",
        F::UtcDayOfWeek => "utc_day_of_week",
        F::UtcHour => "utc_hour",
        F::UtcSession6h => "utc_session_6h",
    };
    fixed.to_string()
}

fn statistical(field: Field) -> bool {
    matches!(
        field,
        Field::ReturnStd(_)
            | Field::ReturnSkew(_)
            | Field::ReturnKurtosis(_)
            | Field::ReturnAutocorr(_)
            | Field::SignReversalRate(_)
            | Field::UpMoveRatio(_)
            | Field::TrendR2(_)
            | Field::TrendResidual(_)
            | Field::RangePosition(_)
            | Field::RangeOverlap
            | Field::CandlePattern
    )
}

/// The source-defined fixed right-closed bins of the named numeric projections, keyed by the
/// canonical input output. Time-valued members keep the reference's millisecond edges and
/// labels; their microsecond inputs are divided by [`DURATION_DIVISOR`] before bucketing.
const FIXED_BINS: &[(&str, &[f64])] = &[
    ("close_position", &[0.0, 0.2, 0.4, 0.6, 0.8, 1.0]),
    ("body_to_range", &[0.0, 0.1, 0.25, 0.45, 0.65, 0.85, 1.0]),
    (
        "upper_wick_to_range",
        &[0.0, 0.1, 0.25, 0.45, 0.65, 0.85, 1.0],
    ),
    (
        "lower_wick_to_range",
        &[0.0, 0.1, 0.25, 0.45, 0.65, 0.85, 1.0],
    ),
    (
        "range_to_avg20",
        &[0.0, 0.5, 0.75, 1.0, 1.25, 1.5, 2.0, 3.0, f64::INFINITY],
    ),
    (
        "range_vs_recent_ratio",
        &[0.0, 0.5, 0.75, 1.0, 1.25, 1.5, 2.0, 3.0, f64::INFINITY],
    ),
    (
        "body_vs_recent_ratio",
        &[0.0, 0.5, 0.75, 1.0, 1.25, 1.5, 2.0, 3.0, f64::INFINITY],
    ),
    (
        "tick_volume_vs_recent_ratio",
        &[0.0, 0.5, 0.75, 1.0, 1.25, 1.5, 2.0, 3.0, f64::INFINITY],
    ),
    (
        "directional_efficiency_5",
        &[
            f64::NEG_INFINITY,
            -0.6,
            -0.3,
            -0.1,
            0.1,
            0.3,
            0.6,
            f64::INFINITY,
        ],
    ),
    (
        "directional_efficiency_10",
        &[
            f64::NEG_INFINITY,
            -0.6,
            -0.3,
            -0.1,
            0.1,
            0.3,
            0.6,
            f64::INFINITY,
        ],
    ),
    (
        "directional_efficiency_20",
        &[
            f64::NEG_INFINITY,
            -0.6,
            -0.3,
            -0.1,
            0.1,
            0.3,
            0.6,
            f64::INFINITY,
        ],
    ),
    (
        "trend_leg_age_candles",
        &[
            f64::NEG_INFINITY,
            0.0,
            1.0,
            2.0,
            3.0,
            5.0,
            8.0,
            13.0,
            21.0,
            f64::INFINITY,
        ],
    ),
    (
        "bars_since_last_swing_high_known",
        &[
            f64::NEG_INFINITY,
            0.0,
            1.0,
            2.0,
            3.0,
            5.0,
            8.0,
            13.0,
            21.0,
            34.0,
            f64::INFINITY,
        ],
    ),
    (
        "bars_since_last_swing_low_known",
        &[
            f64::NEG_INFINITY,
            0.0,
            1.0,
            2.0,
            3.0,
            5.0,
            8.0,
            13.0,
            21.0,
            34.0,
            f64::INFINITY,
        ],
    ),
    (
        "bars_since_last_confirmed_HH_known",
        &[
            f64::NEG_INFINITY,
            0.0,
            1.0,
            2.0,
            3.0,
            5.0,
            8.0,
            13.0,
            21.0,
            34.0,
            f64::INFINITY,
        ],
    ),
    (
        "bars_since_last_confirmed_HL_known",
        &[
            f64::NEG_INFINITY,
            0.0,
            1.0,
            2.0,
            3.0,
            5.0,
            8.0,
            13.0,
            21.0,
            34.0,
            f64::INFINITY,
        ],
    ),
    (
        "bars_since_last_confirmed_LH_known",
        &[
            f64::NEG_INFINITY,
            0.0,
            1.0,
            2.0,
            3.0,
            5.0,
            8.0,
            13.0,
            21.0,
            34.0,
            f64::INFINITY,
        ],
    ),
    (
        "bars_since_last_confirmed_LL_known",
        &[
            f64::NEG_INFINITY,
            0.0,
            1.0,
            2.0,
            3.0,
            5.0,
            8.0,
            13.0,
            21.0,
            34.0,
            f64::INFINITY,
        ],
    ),
    (
        "return_1_bps",
        &[
            f64::NEG_INFINITY,
            -10.0,
            -5.0,
            -2.0,
            -1.0,
            1.0,
            2.0,
            5.0,
            10.0,
            f64::INFINITY,
        ],
    ),
    (
        "momentum_5_bps",
        &[
            f64::NEG_INFINITY,
            -20.0,
            -10.0,
            -5.0,
            -2.0,
            2.0,
            5.0,
            10.0,
            20.0,
            f64::INFINITY,
        ],
    ),
    (
        "momentum_10_bps",
        &[
            f64::NEG_INFINITY,
            -20.0,
            -10.0,
            -5.0,
            -2.0,
            2.0,
            5.0,
            10.0,
            20.0,
            f64::INFINITY,
        ],
    ),
    (
        "momentum_20_bps",
        &[
            f64::NEG_INFINITY,
            -30.0,
            -15.0,
            -8.0,
            -3.0,
            3.0,
            8.0,
            15.0,
            30.0,
            f64::INFINITY,
        ],
    ),
    (
        "range_bps",
        &[0.0, 1.0, 2.0, 3.0, 5.0, 8.0, 13.0, 21.0, f64::INFINITY],
    ),
    (
        "body_bps",
        &[0.0, 0.5, 1.0, 2.0, 3.0, 5.0, 8.0, 13.0, f64::INFINITY],
    ),
    (
        "upper_wick_bps",
        &[0.0, 0.5, 1.0, 2.0, 3.0, 5.0, 8.0, 13.0, f64::INFINITY],
    ),
    (
        "lower_wick_bps",
        &[0.0, 0.5, 1.0, 2.0, 3.0, 5.0, 8.0, 13.0, f64::INFINITY],
    ),
    (
        "abs_return_mean_5_bps",
        &[0.0, 0.5, 1.0, 2.0, 3.0, 5.0, 8.0, 13.0, 21.0, f64::INFINITY],
    ),
    (
        "abs_return_mean_10_bps",
        &[0.0, 0.5, 1.0, 2.0, 3.0, 5.0, 8.0, 13.0, 21.0, f64::INFINITY],
    ),
    (
        "abs_return_mean_20_bps",
        &[0.0, 0.5, 1.0, 2.0, 3.0, 5.0, 8.0, 13.0, 21.0, f64::INFINITY],
    ),
    (
        "range_mean_5_bps",
        &[0.0, 1.0, 2.0, 3.0, 5.0, 8.0, 13.0, 21.0, f64::INFINITY],
    ),
    (
        "range_mean_10_bps",
        &[0.0, 1.0, 2.0, 3.0, 5.0, 8.0, 13.0, 21.0, f64::INFINITY],
    ),
    (
        "range_mean_20_bps",
        &[0.0, 1.0, 2.0, 3.0, 5.0, 8.0, 13.0, 21.0, f64::INFINITY],
    ),
    (
        "max_abs_tick_jump_bps",
        &[
            0.0,
            0.5,
            1.0,
            2.0,
            3.0,
            5.0,
            8.0,
            13.0,
            21.0,
            34.0,
            f64::INFINITY,
        ],
    ),
    (
        "max_true_tick_jump_bps",
        &[
            0.0,
            0.5,
            1.0,
            2.0,
            3.0,
            5.0,
            8.0,
            13.0,
            21.0,
            34.0,
            f64::INFINITY,
        ],
    ),
    (
        "max_gap_reopen_jump_bps",
        &[
            0.0,
            0.5,
            1.0,
            2.0,
            3.0,
            5.0,
            8.0,
            13.0,
            21.0,
            34.0,
            f64::INFINITY,
        ],
    ),
    (
        "max_gap_micros",
        &[
            f64::NEG_INFINITY,
            0.0,
            250.0,
            500.0,
            1_000.0,
            2_000.0,
            5_000.0,
            10_000.0,
            30_000.0,
            60_000.0,
            f64::INFINITY,
        ],
    ),
    (
        "max_internal_gap_micros",
        &[
            f64::NEG_INFINITY,
            0.0,
            250.0,
            500.0,
            1_000.0,
            2_000.0,
            5_000.0,
            10_000.0,
            30_000.0,
            60_000.0,
            f64::INFINITY,
        ],
    ),
    (
        "starts_after_gap_micros",
        &[
            f64::NEG_INFINITY,
            0.0,
            250.0,
            500.0,
            1_000.0,
            2_000.0,
            5_000.0,
            10_000.0,
            30_000.0,
            60_000.0,
            f64::INFINITY,
        ],
    ),
    (
        "max_same_price_run_micros",
        &[
            f64::NEG_INFINITY,
            0.0,
            250.0,
            500.0,
            1_000.0,
            2_000.0,
            5_000.0,
            10_000.0,
            30_000.0,
            60_000.0,
            f64::INFINITY,
        ],
    ),
    (
        "max_same_price_run_ticks",
        &[
            f64::NEG_INFINITY,
            0.0,
            1.0,
            2.0,
            3.0,
            5.0,
            8.0,
            13.0,
            21.0,
            34.0,
            f64::INFINITY,
        ],
    ),
    (
        "missing_buckets_since_prev_candle",
        &[
            f64::NEG_INFINITY,
            0.0,
            1.0,
            2.0,
            3.0,
            5.0,
            8.0,
            13.0,
            21.0,
            34.0,
            f64::INFINITY,
        ],
    ),
    (
        "tick_path_signed_imbalance",
        &[
            f64::NEG_INFINITY,
            -0.6,
            -0.25,
            -0.1,
            0.1,
            0.25,
            0.6,
            f64::INFINITY,
        ],
    ),
    (
        "tick_path_terminal_signed_imbalance",
        &[
            f64::NEG_INFINITY,
            -0.6,
            -0.34,
            -0.1,
            0.1,
            0.34,
            0.6,
            f64::INFINITY,
        ],
    ),
    ("tick_path_reversal_rate", &[0.0, 0.25, 0.45, 0.6, 0.8, 1.0]),
    ("tick_path_efficiency", &[0.0, 0.25, 0.35, 0.65, 1.0]),
    ("tick_path_close_position", &[0.0, 0.2, 0.4, 0.6, 0.8, 1.0]),
    (
        "close_vs_ema20_bps",
        &[
            f64::NEG_INFINITY,
            -20.0,
            -10.0,
            -5.0,
            -2.0,
            -1.0,
            0.0,
            1.0,
            2.0,
            5.0,
            10.0,
            20.0,
            f64::INFINITY,
        ],
    ),
    (
        "close_vs_ema50_bps",
        &[
            f64::NEG_INFINITY,
            -20.0,
            -10.0,
            -5.0,
            -2.0,
            -1.0,
            0.0,
            1.0,
            2.0,
            5.0,
            10.0,
            20.0,
            f64::INFINITY,
        ],
    ),
    (
        "ema20_slope_bps",
        &[
            f64::NEG_INFINITY,
            -5.0,
            -2.0,
            -1.0,
            -0.25,
            0.25,
            1.0,
            2.0,
            5.0,
            f64::INFINITY,
        ],
    ),
    (
        "ema50_slope_bps",
        &[
            f64::NEG_INFINITY,
            -5.0,
            -2.0,
            -1.0,
            -0.25,
            0.25,
            1.0,
            2.0,
            5.0,
            f64::INFINITY,
        ],
    ),
    (
        "ema20_minus_ema50_bps",
        &[
            f64::NEG_INFINITY,
            -20.0,
            -10.0,
            -5.0,
            -2.0,
            -1.0,
            0.0,
            1.0,
            2.0,
            5.0,
            10.0,
            20.0,
            f64::INFINITY,
        ],
    ),
    (
        "distance_to_last_swing_high_bps",
        &[
            f64::NEG_INFINITY,
            -20.0,
            -10.0,
            -5.0,
            -2.0,
            0.0,
            2.0,
            5.0,
            10.0,
            20.0,
            f64::INFINITY,
        ],
    ),
    (
        "distance_to_last_swing_low_bps",
        &[
            f64::NEG_INFINITY,
            -20.0,
            -10.0,
            -5.0,
            -2.0,
            0.0,
            2.0,
            5.0,
            10.0,
            20.0,
            f64::INFINITY,
        ],
    ),
];

/// Microseconds per millisecond: the divisor applied to a duration input before its
/// millisecond-defined fixed bins.
const DURATION_DIVISOR: f64 = 1_000.0;

/// The microsecond inputs whose compiled bins or development quantiles the reference defined
/// over millisecond values.
const DURATION_INPUTS: [&str; 5] = [
    "max_gap_micros",
    "max_internal_gap_micros",
    "starts_after_gap_micros",
    "max_same_price_run_micros",
    "active_span_micros",
];

fn input_divisor(input: &str) -> f64 {
    if DURATION_INPUTS.contains(&input) {
        DURATION_DIVISOR
    } else {
        1.0
    }
}

fn is_one(value: &f64) -> bool {
    *value == 1.0
}

/// The named development-quantile projections, keyed by the canonical input output.
const QUANTILE_PROJECTIONS: &[&str] = &[
    "tick_volume",
    "tick_volume_mean_5",
    "tick_volume_mean_10",
    "tick_volume_mean_20",
    "active_span_micros",
    "tick_path_directional_move_count",
    "tick_path_uptick_count",
    "tick_path_downtick_count",
    "tick_path_flat_count",
    "tick_path_direction_change_count",
    "tick_path_terminal_move_count",
];

/// Whether `name` is a compiled projection, and of which input.
fn projection_input(name: &str) -> Option<(&str, ProjectionKind)> {
    if let Some(input) = name.strip_suffix("_bucketed")
        && FIXED_BINS.iter().any(|(candidate, _)| *candidate == input)
    {
        return Some((input, ProjectionKind::Fixed));
    }
    if let Some(input) = name.strip_suffix("_dev_quantile")
        && QUANTILE_PROJECTIONS.contains(&input)
    {
        return Some((input, ProjectionKind::DevelopmentFifths));
    }
    None
}

crate::string_enum! {
    /// How an encoding maps values to labels.
    ProjectionKind "encoding" {
        Category => "category",
        Fixed => "fixed",
        DevelopmentFifths => "development_fifths",
    }
}

/// The formula settings a plan freezes before computation. Every value comes from the
/// configuration entry; the resolver never derives one.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct FormulaSettings {
    pub streams: Vec<StreamKey>,
    pub outputs: Outputs,
    pub moving_average_periods: Vec<u32>,
    pub rolling_window: Option<u32>,
    pub min_history: Option<u32>,
    pub structure: Option<StructureSettings>,
    /// The configured text and its exact units at the instrument's scale.
    pub price_epsilon: Option<String>,
    pub price_epsilon_units: Option<i64>,
    pub tick_path_streams: Vec<StreamKey>,
}

impl FormulaSettings {
    /// Reads the entry's new-plan settings, converting the price epsilon at the bound scale.
    fn from_entry(entry: &FeatureInstrument, scale: PriceScale) -> Result<Self, String> {
        let epsilon_units = entry
            .price_epsilon
            .as_deref()
            .map(|text| {
                parse_price_units(text, scale).map_err(|reason| format!("price_epsilon: {reason}"))
            })
            .transpose()?;
        Ok(Self {
            streams: entry
                .streams
                .clone()
                .ok_or("streams: a new plan names its streams")?,
            outputs: entry
                .outputs
                .clone()
                .ok_or("outputs: a new plan names its outputs")?,
            moving_average_periods: entry.moving_average_periods.clone().unwrap_or_default(),
            rolling_window: entry.rolling_window,
            min_history: entry.min_history,
            structure: entry.structure.clone(),
            price_epsilon: entry.price_epsilon.clone(),
            price_epsilon_units: epsilon_units,
            tick_path_streams: entry.tick_path_streams.clone().unwrap_or_default(),
        })
    }
}

/// What the bound Phase 03 stream generation established about the source and instrument.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ProfileReference {
    pub stream_generation: String,
    pub profile_sha256: String,
    pub source_generation: String,
    pub role: DatasetRole,
    pub definition: Instrument,
    /// Whether the profile records individual ticks, so tick calculations are supported.
    pub ticks: bool,
}

/// One selected output of one stream.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct OutputSpec {
    pub name: String,
    pub kind: Kind,
    pub stage: Stage,
    pub predictive: bool,
    /// When the output is available and what an unavailable value means.
    pub readiness: String,
}

/// One output a stream cannot provide and the exact missing prerequisite.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct Exclusion {
    pub name: String,
    pub reason: String,
}

/// One encoding of one stream, frozen after the development fit.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
pub struct FittedEncoding {
    pub output: String,
    /// The row output the labels are computed from.
    pub input: String,
    /// Automatically selected encodings fit only ready rows and may retain no numeric bins.
    #[serde(default, skip_serializing_if = "is_false")]
    pub automatic: bool,
    pub encoding: ProjectionKind,
    /// Right-closed bin edges for `fixed` and `development_fifths`; the first edge is included.
    /// Serialized as exact decimal text so infinite tails and every binary value round-trip.
    #[serde(with = "edge_text")]
    pub edges: Option<Vec<f64>>,
    /// The input is divided by this before bucketing: 1000 for a microsecond duration whose
    /// bins and labels the reference defined in milliseconds, otherwise 1.
    #[serde(default = "one", skip_serializing_if = "is_one")]
    pub input_divisor: f64,
    /// Labels in code order; a value outside the list encodes as `-1`.
    pub labels: Vec<String>,
}

fn one() -> f64 {
    1.0
}

fn is_false(value: &bool) -> bool {
    !value
}

/// Bin edges as text: Rust's shortest round-trip rendering, which also spells `inf` and `-inf`.
mod edge_text {
    use serde::de::Error as _;
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    pub fn serialize<S: Serializer>(
        edges: &Option<Vec<f64>>,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        edges
            .as_ref()
            .map(|edges| {
                edges
                    .iter()
                    .map(|edge| format!("{edge:?}"))
                    .collect::<Vec<_>>()
            })
            .serialize(serializer)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Option<Vec<f64>>, D::Error> {
        Option::<Vec<String>>::deserialize(deserializer)?
            .map(|edges| {
                edges
                    .iter()
                    .map(|edge| {
                        edge.parse::<f64>().map_err(|_| {
                            D::Error::custom(format!("bin edge `{edge}` is not a number"))
                        })
                    })
                    .collect()
            })
            .transpose()
    }
}

/// The frozen plan of one stream.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
pub struct StreamPlan {
    pub duration_seconds: u32,
    pub offset_seconds: u32,
    pub tick_path: bool,
    pub outputs: Vec<OutputSpec>,
    pub excluded: Vec<Exclusion>,
    pub encodings: Vec<FittedEncoding>,
}

impl StreamPlan {
    pub fn key(&self) -> StreamKey {
        StreamKey {
            duration_seconds: self.duration_seconds,
            offset_seconds: self.offset_seconds,
        }
    }

    /// The object paths of this stream's rows, structure events, sequence events, and, when
    /// the stream has encodings, encoded rows inside a feature generation.
    pub fn object_paths(&self) -> Vec<String> {
        let mut paths = stream_object_paths(self.duration_seconds, self.offset_seconds);
        if self.encodings.is_empty() {
            paths.pop();
        }
        paths
    }

    /// The position of an output in this stream's rows.
    pub fn output_index(&self, name: &str) -> Option<usize> {
        self.outputs.iter().position(|output| output.name == name)
    }
}

/// The four object paths a stream may own: rows, structure events, sequence events, and
/// encoded rows.
pub fn stream_object_paths(duration_seconds: u32, offset_seconds: u32) -> Vec<String> {
    let stem = format!("{duration_seconds}s_{offset_seconds}s");
    vec![
        format!("rows/{stem}.parquet"),
        format!("events/structure_{stem}.parquet"),
        format!("events/sequence_{stem}.parquet"),
        format!("encoded/{stem}.parquet"),
    ]
}

/// The development fit window: the decision-time span of the rows the encodings were fitted
/// on, per stream, rendered as `YYYY-MM-DDTHH:MM:SS.ffffffZ`.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct FitWindow {
    pub duration_seconds: u32,
    pub offset_seconds: u32,
    pub rows: u64,
    pub first_decision_time: Option<String>,
    pub last_decision_time: Option<String>,
}

/// The immutable plan: identities, capabilities, every parameter value with its source, the
/// selected and excluded outputs per stream, definition versions, and the fitted encodings.
/// Field order is the serialization order; the JSON bytes carry no incidental timing.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct FeaturePlan {
    pub schema_version: u32,
    pub kind: String,
    pub instrument: String,
    pub broker: BrokerId,
    pub provider_symbol: ProviderSymbol,
    pub price_scale: PriceScale,
    /// Parameters the bound stream generation supplies.
    pub profile: ProfileReference,
    /// The development generation the plan was fitted on.
    pub development_generation: String,
    /// Parameters the configuration entry supplies.
    pub settings: FormulaSettings,
    /// The compiled policies with fixed literals.
    pub definitions: Definitions,
    pub max_labels: u32,
    /// Identity of the raw rows: profile, development input, and formula settings.
    pub raw_identity: String,
    pub fit_windows: Vec<FitWindow>,
    pub streams: Vec<StreamPlan>,
}

impl FeaturePlan {
    /// The exact bytes published as `plan.json`.
    pub fn to_json(&self) -> Vec<u8> {
        let mut bytes = serde_json::to_vec_pretty(self).expect("a plan serializes");
        bytes.push(b'\n');
        bytes
    }

    pub fn from_json(bytes: &[u8]) -> Result<Self, String> {
        let plan: Self = serde_json::from_slice(bytes).map_err(|error| error.to_string())?;
        if plan.schema_version != FEATURE_SCHEMA_VERSION || plan.kind != "feature_plan" {
            return Err(format!(
                "unsupported plan schema_version {} kind `{}`",
                plan.schema_version, plan.kind
            ));
        }
        if plan.raw_identity
            != raw_identity(
                &plan.profile,
                &plan.development_generation,
                &plan.settings,
                &plan.definitions,
            )
        {
            return Err("plan raw identity does not match its profile, input, and settings".into());
        }
        Ok(plan)
    }

    /// The plan identity: SHA-256 over the plan domain and the canonical JSON bytes.
    pub fn identity(&self) -> String {
        let mut hasher = Sha256::new();
        hasher.update(PLAN_IDENTITY_DOMAIN_V1);
        hasher.update(self.to_json());
        crate::hex(&hasher.finalize())
    }

    /// When `output` is ready, as the feature owner declares it: the boolean outputs that must
    /// be true where readiness is separate from the value (`is_ema{p}_ready` for every
    /// moving-average period the output requires, `tick_path_ready` for the tick-path buckets),
    /// and the text values that read as not ready or unavailable. Readiness outputs
    /// themselves, and outputs whose unavailability is their own absence, declare neither.
    pub fn readiness_of(&self, output: &str) -> Readiness {
        use Field as F;
        let Some(definition) = catalog(&self.settings)
            .into_iter()
            .find(|definition| definition.name == output)
        else {
            return Readiness::default();
        };
        let flags = match definition.field {
            F::TickPathPressureBucket
            | F::TickPathShapeBucket
            | F::TickPathTerminalPressureBucket
            | F::TickPathFailedPressureDirection
            | F::TickPathEfficiencyBucket
            | F::TickPathReversalBucket => vec![field_name(F::TickPathReady)],
            F::EmaReady(_) => Vec::new(),
            _ => definition
                .requires
                .iter()
                .filter_map(|requirement| match requirement {
                    Req::Period(period) => Some(field_name(F::EmaReady(*period))),
                    _ => None,
                })
                .collect(),
        };
        Readiness {
            flags,
            unready: unready(definition.field)
                .iter()
                .map(|value| value.to_string())
                .collect(),
        }
    }

    pub fn stream(&self, key: StreamKey) -> Option<&StreamPlan> {
        self.streams.iter().find(|stream| stream.key() == key)
    }

    /// Resolves a new plan from an entry's settings against the bound profile reference. The
    /// encodings are compiled but unfitted (no labels or edges) until [`FeaturePlan::fit`].
    pub fn resolve(
        entry: &FeatureInstrument,
        profile: ProfileReference,
        development_generation: &str,
    ) -> Result<Self, String> {
        Self::resolve_with_definitions(
            entry,
            profile,
            development_generation,
            Definitions::current(),
        )
    }

    /// Re-resolves a configured fit using the definitions recorded by its plan.
    pub fn resolve_with_definitions(
        entry: &FeatureInstrument,
        profile: ProfileReference,
        development_generation: &str,
        definitions: Definitions,
    ) -> Result<Self, String> {
        if profile.role != DatasetRole::Development {
            return Err(format!(
                "profile_manifest: a plan fits only on a development profile, not `{}`",
                profile.role
            ));
        }
        let definition = &profile.definition;
        let settings = FormulaSettings::from_entry(entry, definition.price_scale)?;
        let mut streams = Vec::with_capacity(settings.streams.len());
        for key in &settings.streams {
            let spec = definition
                .candles
                .iter()
                .find(|spec| {
                    spec.duration_seconds == key.duration_seconds
                        && spec.offset_seconds == key.offset_seconds
                })
                .ok_or_else(|| {
                    format!("streams: stream {key} is not a candle stream of the bound definition")
                })?;
            let tick_path = settings.tick_path_streams.contains(key);
            let available = |req: Req| -> Result<(), String> {
                let missing = |what: &str| Err(format!("requires {what}"));
                match req {
                    Req::Ticks if !profile.ticks => {
                        missing("individual ticks, which the bound generation does not provide")
                    }
                    Req::TickPath if !profile.ticks => {
                        missing("individual ticks, which the bound generation does not provide")
                    }
                    Req::TickPath if !tick_path => {
                        missing(&format!("stream {key} in tick_path_streams"))
                    }
                    Req::GapCheck if definition.gap.is_none() => {
                        missing("the `gap` check of the bound definition")
                    }
                    Req::FrozenCheck if definition.frozen.is_none() => {
                        missing("the `frozen` check of the bound definition")
                    }
                    Req::JumpCheck if definition.jump.is_none() => {
                        missing("the `jump` check of the bound definition")
                    }
                    Req::MinObservations if spec.min_observations.is_none() => missing(&format!(
                        "`min_observations` of stream {key} in the bound definition"
                    )),
                    Req::HardMinObservations if spec.hard_min_observations.is_none() => missing(
                        &format!("`hard_min_observations` of stream {key} in the bound definition"),
                    ),
                    Req::Structure if settings.structure.is_none() => {
                        missing("the `structure` settings")
                    }
                    Req::Window(w)
                        if !settings
                            .structure
                            .as_ref()
                            .is_some_and(|structure| structure.rolling_windows.contains(&w)) =>
                    {
                        missing(&format!("structure rolling window {w}"))
                    }
                    Req::Epsilon if settings.price_epsilon_units.is_none() => {
                        missing("`price_epsilon`")
                    }
                    Req::Relative
                        if settings.rolling_window.is_none() || settings.min_history.is_none() =>
                    {
                        missing("`rolling_window` and `min_history`")
                    }
                    Req::Period(p) if !settings.moving_average_periods.contains(&p) => {
                        missing(&format!("moving-average period {p}"))
                    }
                    _ => Ok(()),
                }
            };
            let table = catalog(&settings);
            let mut outputs = Vec::new();
            let mut excluded = Vec::new();
            let mut supported: HashMap<&str, Result<(), String>> = HashMap::new();
            for output in &table {
                if statistical(output.field)
                    && definitions.statistics.as_deref() != Some("rolling_statistics_v1")
                {
                    continue;
                }
                let verdict = output.requires.iter().try_for_each(|req| available(*req));
                supported.insert(&output.name, verdict.clone());
                let spec = OutputSpec {
                    name: output.name.clone(),
                    kind: output.kind,
                    stage: output.stage,
                    predictive: output.predictive,
                    readiness: readiness(output.field).to_string(),
                };
                // Row identity and clocks are always persisted, whatever a named list selects.
                let identity = output.stage == Stage::Candle && !output.predictive;
                match (&settings.outputs, verdict) {
                    (Outputs::AllSupported, Ok(())) => outputs.push(spec),
                    (Outputs::AllSupported, Err(reason)) => excluded.push(Exclusion {
                        name: output.name.clone(),
                        reason,
                    }),
                    (Outputs::Named(names), Ok(())) if identity || names.contains(&output.name) => {
                        outputs.push(spec);
                    }
                    (Outputs::Named(names), Err(reason)) if names.contains(&output.name) => {
                        return Err(format!(
                            "outputs: `{}` on stream {key} {reason}",
                            output.name
                        ));
                    }
                    (Outputs::Named(_), _) => {}
                }
            }
            if let Outputs::Named(names) = &settings.outputs
                && let Some(unknown) = names
                    .iter()
                    .find(|name| !supported.contains_key(name.as_str()))
            {
                return Err(format!(
                    "outputs: `{unknown}` is not a compiled output; a projection is encoded, not selected"
                ));
            }
            let mut encodings = Vec::new();
            if let Some(Encodings { outputs: specs, .. }) = &entry.encodings {
                if specs.len() == 1 && specs[0].output == "all_supported" && specs[0].bins.is_none()
                {
                    for output in outputs
                        .iter()
                        .filter(|output| output.predictive && output.kind != Kind::Time)
                    {
                        let mut name = format!("{}_auto_encoded", output.name);
                        let mut suffix = 1;
                        while table.iter().any(|raw| raw.name == name)
                            || encodings
                                .iter()
                                .any(|encoded: &FittedEncoding| encoded.output == name)
                        {
                            name = format!("{}_auto_encoded_{suffix}", output.name);
                            suffix += 1;
                        }
                        encodings.push(FittedEncoding {
                            output: name,
                            input: output.name.clone(),
                            automatic: true,
                            encoding: if matches!(output.kind, Kind::Int | Kind::Float) {
                                ProjectionKind::DevelopmentFifths
                            } else {
                                ProjectionKind::Category
                            },
                            edges: None,
                            input_divisor: 1.0,
                            labels: Vec::new(),
                        });
                    }
                } else {
                    for spec in specs {
                        if let Some(encoding) =
                            compile_encoding(spec, &outputs, &mut excluded, key)?
                        {
                            encodings.push(encoding);
                        }
                    }
                }
            }
            streams.push(StreamPlan {
                duration_seconds: key.duration_seconds,
                offset_seconds: key.offset_seconds,
                tick_path,
                outputs,
                excluded,
                encodings,
            });
        }
        let raw_identity = raw_identity(&profile, development_generation, &settings, &definitions);
        Ok(Self {
            schema_version: FEATURE_SCHEMA_VERSION,
            kind: "feature_plan".to_string(),
            instrument: definition.id().to_string(),
            broker: definition.broker.clone(),
            provider_symbol: definition.provider_symbol.clone(),
            price_scale: definition.price_scale,
            profile,
            development_generation: development_generation.to_string(),
            settings,
            definitions,
            max_labels: entry
                .encodings
                .as_ref()
                .map_or(crate::config::MAX_ENCODING_LABELS, |encodings| {
                    encodings.max_labels
                }),
            raw_identity,
            fit_windows: Vec::new(),
            streams,
        })
    }

    /// Whether every encoding carries its fitted labels and the fit windows are recorded.
    pub fn is_fitted(&self) -> bool {
        self.fit_windows.len() == self.streams.len()
    }

    /// The plan as resolved before its development fit: no fit windows, no labels, and no
    /// development-fitted edges, so a fitted plan compares with the plan its entry resolves.
    pub fn unfitted(&self) -> Self {
        let mut plan = self.clone();
        plan.fit_windows.clear();
        for stream in &mut plan.streams {
            for encoding in &mut stream.encodings {
                encoding.labels.clear();
                if encoding.encoding == ProjectionKind::DevelopmentFifths {
                    encoding.edges = None;
                }
            }
        }
        plan
    }
}

/// Compiles one configured encoding against a stream's selected outputs. An encoding whose
/// input the stream excluded is excluded with the input's reason; an input that is no compiled
/// output at all, or bins that contradict the output's kind, are errors.
fn compile_encoding(
    spec: &EncodingSpec,
    outputs: &[OutputSpec],
    excluded: &mut Vec<Exclusion>,
    key: &StreamKey,
) -> Result<Option<FittedEncoding>, String> {
    let field = format!("encodings.outputs (`{}` on stream {key})", spec.output);
    let (input, kind) = match projection_input(&spec.output) {
        Some((input, kind)) => {
            if spec.bins.is_some() {
                return Err(format!(
                    "{field}: a compiled projection defines its own bins"
                ));
            }
            (input, Some(kind))
        }
        None => (spec.output.as_str(), None),
    };
    let Some(output) = outputs.iter().find(|output| output.name == input) else {
        return match excluded.iter().find(|exclusion| exclusion.name == input) {
            Some(exclusion) => {
                excluded.push(Exclusion {
                    name: spec.output.clone(),
                    reason: format!("its input `{input}` is excluded: {}", exclusion.reason),
                });
                Ok(None)
            }
            None => Err(format!(
                "{field}: `{input}` is not a selected output or compiled projection"
            )),
        };
    };
    let fixed_edges = FIXED_BINS
        .iter()
        .find(|(candidate, _)| *candidate == input)
        .map(|(_, edges)| edges.to_vec());
    let (encoding, edges) = match (kind, output.kind, &spec.bins) {
        (Some(ProjectionKind::Fixed), _, _) => (ProjectionKind::Fixed, fixed_edges),
        (Some(kind), _, _) => (kind, None),
        (None, Kind::Text | Kind::Bool, None) => (ProjectionKind::Category, None),
        (None, Kind::Text | Kind::Bool, Some(_)) => {
            return Err(format!("{field}: a category output takes no bins"));
        }
        (None, Kind::Int | Kind::Float, Some(Bins::Fixed(edges))) => {
            (ProjectionKind::Fixed, Some(edges.clone()))
        }
        (None, Kind::Int | Kind::Float, Some(Bins::DevelopmentFifths)) => {
            (ProjectionKind::DevelopmentFifths, None)
        }
        (None, Kind::Int | Kind::Float, None) => {
            return Err(format!("{field}: a numeric output needs `bins`"));
        }
        (None, Kind::Time, _) => return Err(format!("{field}: a clock is never encoded")),
    };
    Ok(Some(FittedEncoding {
        output: spec.output.clone(),
        input: input.to_string(),
        automatic: false,
        encoding,
        edges,
        input_divisor: match kind {
            Some(_) => input_divisor(input),
            None => 1.0,
        },
        labels: Vec::new(),
    }))
}

/// The raw-row identity: the profile reference, the development input, and the canonical
/// formula settings, before any encoding exists.
pub fn raw_identity(
    profile: &ProfileReference,
    development_generation: &str,
    settings: &FormulaSettings,
    definitions: &Definitions,
) -> String {
    let mut hasher = Sha256::new();
    hasher.update(RAW_IDENTITY_DOMAIN_V1);
    for line in [
        profile.stream_generation.as_str(),
        profile.profile_sha256.as_str(),
        development_generation,
    ] {
        hasher.update(line.as_bytes());
        hasher.update(b"\n");
    }
    hasher.update(serde_json::to_vec(settings).expect("settings serialize"));
    hasher.update(serde_json::to_vec(definitions).expect("definitions serialize"));
    crate::hex(&hasher.finalize())
}

/// The identity of a feature generation: the plan identity applied to one input generation.
pub fn feature_generation_id(plan_identity: &str, input_generation: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(FEATURE_GENERATION_DOMAIN_V1);
    hasher.update(plan_identity.as_bytes());
    hasher.update(b"\n");
    hasher.update(input_generation.as_bytes());
    crate::hex(&hasher.finalize())
}

// ---------------------------------------------------------------------------------------------
// Feature computation
// ---------------------------------------------------------------------------------------------

/// The reference's six-decimal text rounding of a value, applied where the reference wrote a
/// value to text before a later stage read it back.
fn six(value: f64) -> f64 {
    format!("{value:.6}")
        .parse()
        .expect("a formatted float parses")
}

/// `(value - reference) / |reference| * 10000`, or `None` for a zero reference.
fn bps_change(value: f64, reference: f64) -> Option<f64> {
    (reference != 0.0).then(|| (value - reference) / reference.abs() * 10_000.0)
}

/// `value / |reference| * 10000`, or `None` for a zero reference.
fn bps_size(value: f64, reference: f64) -> Option<f64> {
    (reference != 0.0).then(|| value / reference.abs() * 10_000.0)
}

/// One accepted tick as the path folds it: its event time, floating price, and exact units.
#[derive(Debug, Clone, Copy, PartialEq)]
struct TickSeen {
    event: i64,
    price: f64,
    units: i64,
}

/// The within-candle tick path and the tick-to-tick jump magnitudes of one stream's working
/// interval, folded from ordered accepted ticks before the candle finalizes.
#[derive(Debug, Clone)]
struct TickPath {
    open_time: i64,
    close_time: i64,
    up: u64,
    down: u64,
    flat: u64,
    changes: u64,
    abs_move_sum: f64,
    last_direction: i8,
    /// The last `ceil(directional moves / 3)` nonzero signs.
    signs: VecDeque<i8>,
    max_abs_bps: f64,
    max_true_bps: f64,
    max_reopen_bps: f64,
}

impl TickPath {
    fn new(open_time: i64, close_time: i64) -> Self {
        Self {
            open_time,
            close_time,
            up: 0,
            down: 0,
            flat: 0,
            changes: 0,
            abs_move_sum: 0.0,
            last_direction: 0,
            signs: VecDeque::new(),
            max_abs_bps: 0.0,
            max_true_bps: 0.0,
            max_reopen_bps: 0.0,
        }
    }

    fn directional(&self) -> u64 {
        self.up + self.down
    }

    /// Folds one accepted tick given the previous tick of the stream, in the reference's
    /// order: the jump entering or inside the interval, then the path move when the previous
    /// tick lies inside this interval.
    fn fold(&mut self, previous: Option<TickSeen>, tick: TickSeen, gap: Option<(i64, i64)>) {
        let Some(TickSeen {
            event: previous_event,
            price: previous_price,
            units: previous_units,
        }) = previous
        else {
            return;
        };
        let TickSeen {
            event,
            price,
            units,
        } = tick;
        let delta = event - previous_event;
        if previous_price != 0.0 {
            let bps = (price - previous_price).abs() / previous_price.abs() * 10_000.0;
            self.max_abs_bps = self.max_abs_bps.max(bps);
            if let Some((max, reopen)) = gap {
                if delta <= max {
                    self.max_true_bps = self.max_true_bps.max(bps);
                } else if delta >= reopen {
                    self.max_reopen_bps = self.max_reopen_bps.max(bps);
                }
            }
        }
        if !(self.open_time..self.close_time).contains(&previous_event) {
            return;
        }
        // The sign and flatness of a move are exact unit decisions; its size feeds the
        // reference's floating sum.
        let direction: i8 = match units.cmp(&previous_units) {
            std::cmp::Ordering::Equal => {
                self.flat += 1;
                return;
            }
            std::cmp::Ordering::Greater => 1,
            std::cmp::Ordering::Less => -1,
        };
        let move_ = price - previous_price;
        if direction > 0 {
            self.up += 1;
        } else {
            self.down += 1;
        }
        self.abs_move_sum += move_.abs();
        self.signs.push_back(direction);
        let keep = self.directional().div_ceil(3) as usize;
        while self.signs.len() > keep {
            self.signs.pop_front();
        }
        if self.last_direction != 0 && direction != self.last_direction {
            self.changes += 1;
        }
        self.last_direction = direction;
    }

    /// The tick-path outputs of the closed interval; categories read the unrounded ratios.
    fn summary(&self, open: f64, high: f64, low: f64, close: f64) -> TickPathSummary {
        let directional = self.directional();
        let (signed_imbalance, reversal_rate) = if directional > 0 {
            (
                (self.up as f64 - self.down as f64) / directional as f64,
                self.changes as f64 / (directional.saturating_sub(1)).max(1) as f64,
            )
        } else {
            (0.0, 0.0)
        };
        let efficiency = if self.abs_move_sum > 0.0 {
            (close - open).abs() / self.abs_move_sum
        } else {
            0.0
        };
        let range = high - low;
        let close_position = if range == 0.0 {
            0.5
        } else {
            ((close - low) / range).clamp(0.0, 1.0)
        };
        let terminal: i64 = self.signs.iter().map(|sign| i64::from(*sign)).sum();
        let terminal_moves = self.signs.len() as u64;
        let terminal_signed_imbalance = if terminal_moves > 0 {
            terminal as f64 / terminal_moves as f64
        } else {
            0.0
        };
        let ready = directional >= TICK_PATH_MIN_DIRECTIONAL_MOVES;
        let (pressure, terminal_bucket, failed, shape) = if !ready {
            let n = "insufficient_tick_path";
            (n, n, n, n)
        } else {
            let pressure = if signed_imbalance >= TICK_PATH_PRESSURE_THRESHOLD {
                "pressure_up"
            } else if signed_imbalance <= -TICK_PATH_PRESSURE_THRESHOLD {
                "pressure_down"
            } else {
                "pressure_neutral"
            };
            let terminal_bucket =
                if terminal_signed_imbalance >= TICK_PATH_TERMINAL_PRESSURE_THRESHOLD {
                    "terminal_up"
                } else if terminal_signed_imbalance <= -TICK_PATH_TERMINAL_PRESSURE_THRESHOLD {
                    "terminal_down"
                } else {
                    "terminal_neutral"
                };
            let failed = if pressure == "pressure_up"
                && close_position <= TICK_PATH_FAILED_UP_CLOSE_POSITION
            {
                "failed_up"
            } else if pressure == "pressure_down"
                && close_position >= TICK_PATH_FAILED_DOWN_CLOSE_POSITION
            {
                "failed_down"
            } else {
                "none"
            };
            let clean_push = |direction: &str, position_ok: bool| {
                pressure == direction
                    && efficiency >= TICK_PATH_MEDIUM_EFFICIENCY
                    && reversal_rate <= TICK_PATH_CLEAN_PUSH_MAX_REVERSAL_RATE
                    && position_ok
            };
            let shape = if clean_push("pressure_up", close_position >= TICK_PATH_UP_CLOSE_POSITION)
                || clean_push(
                    "pressure_down",
                    close_position <= TICK_PATH_DOWN_CLOSE_POSITION,
                ) {
                "clean_push"
            } else if failed != "none" {
                "failed_push"
            } else if reversal_rate >= TICK_PATH_CHURN_REVERSAL_RATE
                || efficiency <= TICK_PATH_CHURN_EFFICIENCY
            {
                "churn"
            } else {
                "mixed"
            };
            (pressure, terminal_bucket, failed, shape)
        };
        let (efficiency_bucket, reversal_bucket) = if !ready {
            ("insufficient_tick_path", "insufficient_tick_path")
        } else {
            (
                if efficiency >= TICK_PATH_HIGH_EFFICIENCY {
                    "high_efficiency"
                } else if efficiency >= TICK_PATH_MEDIUM_EFFICIENCY {
                    "medium_efficiency"
                } else {
                    "low_efficiency"
                },
                if reversal_rate >= TICK_PATH_CHURN_REVERSAL_RATE {
                    "high_reversal"
                } else if reversal_rate <= TICK_PATH_CLEAN_PUSH_MAX_REVERSAL_RATE {
                    "low_reversal"
                } else {
                    "medium_reversal"
                },
            )
        };
        TickPathSummary {
            ready,
            directional,
            up: self.up,
            down: self.down,
            flat: self.flat,
            changes: self.changes,
            signed_imbalance: six(signed_imbalance),
            reversal_rate: six(reversal_rate),
            efficiency: six(efficiency),
            terminal_moves,
            terminal_signed_imbalance: six(terminal_signed_imbalance),
            close_position: six(close_position),
            pressure,
            shape,
            terminal_bucket,
            failed,
            efficiency_bucket,
            reversal_bucket,
        }
    }
}

/// The eighteen tick-path outputs of one candle.
#[derive(Debug, Clone, PartialEq)]
pub struct TickPathSummary {
    pub ready: bool,
    pub directional: u64,
    pub up: u64,
    pub down: u64,
    pub flat: u64,
    pub changes: u64,
    pub signed_imbalance: f64,
    pub reversal_rate: f64,
    pub efficiency: f64,
    pub terminal_moves: u64,
    pub terminal_signed_imbalance: f64,
    pub close_position: f64,
    pub pressure: &'static str,
    pub shape: &'static str,
    pub terminal_bucket: &'static str,
    pub failed: &'static str,
    pub efficiency_bucket: &'static str,
    pub reversal_bucket: &'static str,
}

/// The jump magnitudes a stream's tick fold records for one candle, in whole floating basis
/// points as the reference emits them.
#[derive(Debug, Clone, Copy, PartialEq)]
struct JumpMagnitudes {
    abs: f64,
    true_: f64,
    reopen: f64,
}

/// Candle anatomy from canonical prices converted to binary floating point.
#[derive(Debug, Clone, Copy)]
struct Anatomy {
    unit: f64,
    close_units: i64,
    open: f64,
    high: f64,
    low: f64,
    close: f64,
    direction: &'static str,
    /// Exact unit differences, unavailable only when they exceed signed 64-bit units.
    body_units: Option<i64>,
    range_units: Option<i64>,
    upper_wick_units: Option<i64>,
    lower_wick_units: Option<i64>,
    /// Six-place normalized values, as every downstream reference stage read them.
    body_bps: Option<f64>,
    range_bps: Option<f64>,
    upper_wick_bps: Option<f64>,
    lower_wick_bps: Option<f64>,
    close_position: f64,
    body_to_range: f64,
    upper_wick_to_range: f64,
    lower_wick_to_range: f64,
}

impl Anatomy {
    fn new(candle: &Candle, unit: f64) -> Self {
        let price = |units: i64| units as f64 / unit;
        let (open, high, low, close) = (
            price(candle.open_units),
            price(candle.high_units),
            price(candle.low_units),
            price(candle.close_units),
        );
        let range = (high - low).max(0.0);
        let body = (close - open).abs();
        let upper_wick = (high - open.max(close)).max(0.0);
        let lower_wick = (open.min(close) - low).max(0.0);
        let ratio = |value: f64, zero: f64| if range > 0.0 { value / range } else { zero };
        let (open_units, high_units, low_units, close_units) = (
            i128::from(candle.open_units),
            i128::from(candle.high_units),
            i128::from(candle.low_units),
            i128::from(candle.close_units),
        );
        let units = |difference: i128| i64::try_from(difference).ok();
        Self {
            unit,
            close_units: candle.close_units,
            open,
            high,
            low,
            close,
            direction: match close_units.cmp(&open_units) {
                std::cmp::Ordering::Greater => "up",
                std::cmp::Ordering::Less => "down",
                std::cmp::Ordering::Equal => "flat",
            },
            body_units: units((close_units - open_units).abs()),
            range_units: units(high_units - low_units),
            upper_wick_units: units(high_units - open_units.max(close_units)),
            lower_wick_units: units(open_units.min(close_units) - low_units),
            body_bps: bps_size(body, open).map(six),
            range_bps: bps_size(range, open).map(six),
            upper_wick_bps: bps_size(upper_wick, open).map(six),
            lower_wick_bps: bps_size(lower_wick, open).map(six),
            close_position: six(ratio(close - low, 0.5)),
            body_to_range: six(ratio(body, 0.0)),
            upper_wick_to_range: six(ratio(upper_wick, 0.0)),
            lower_wick_to_range: six(ratio(lower_wick, 0.0)),
        }
    }
}

/// A bounded history that drops its oldest value past `max_len`.
#[derive(Debug, Clone)]
struct History<T> {
    values: VecDeque<T>,
    max_len: usize,
}

impl<T: Copy> History<T> {
    fn new(max_len: usize) -> Self {
        Self {
            values: VecDeque::with_capacity(max_len),
            max_len,
        }
    }

    fn push(&mut self, value: T) {
        if self.values.len() == self.max_len {
            self.values.pop_front();
        }
        self.values.push_back(value);
    }

    fn len(&self) -> usize {
        self.values.len()
    }

    fn last(&self) -> Option<T> {
        self.values.back().copied()
    }

    /// The last `count` values in order.
    fn tail(&self, count: usize) -> impl Iterator<Item = T> + '_ {
        self.values.iter().skip(self.values.len() - count).copied()
    }

    fn nth_from_end(&self, count: usize) -> T {
        self.values[self.values.len() - count]
    }
}

/// Whether `point` lies above, below, or within `epsilon` units of `before`, decided on exact
/// units without overflow.
fn beyond(point: i64, before: i64, epsilon: i64) -> Ordering {
    let (point, before, epsilon) = (i128::from(point), i128::from(before), i128::from(epsilon));
    if point > before + epsilon {
        Ordering::Greater
    } else if point < before - epsilon {
        Ordering::Less
    } else {
        Ordering::Equal
    }
}

/// The reference's `sum()` over floats: CPython 3.12's left-to-right Neumaier-compensated
/// addition, which differs from a plain fold in the last bits and therefore, on some windows,
/// in the sixth decimal place of the emitted mean.
fn sum(values: impl Iterator<Item = f64>) -> f64 {
    let (mut total, mut compensation) = (0.0_f64, 0.0_f64);
    for value in values {
        let next = total + value;
        compensation += if total.abs() >= value.abs() {
            (total - next) + value
        } else {
            (value - next) + total
        };
        total = next;
    }
    if compensation != 0.0 && compensation.is_finite() {
        total + compensation
    } else {
        total
    }
}

/// One window's rolling outputs.
#[derive(Debug, Clone, Copy, Default)]
struct WindowRow {
    momentum: Option<f64>,
    efficiency: Option<f64>,
    abs_return_mean: Option<f64>,
    range_mean: Option<f64>,
    tick_volume_mean: Option<f64>,
}

/// The rolling structure state that advances across every accepted candle.
#[derive(Debug, Clone)]
struct Rolling {
    settings: StructureSettings,
    closes: History<f64>,
    abs_returns: History<f64>,
    ranges: History<f64>,
    tick_volumes: History<u64>,
    trend_direction: &'static str,
    trend_age: u64,
    sideways: u64,
}

/// The rolling outputs of one accepted candle.
#[derive(Debug, Clone)]
struct RollingRow {
    return_1_unrounded: Option<f64>,
    return_1_bps: Option<f64>,
    windows: Vec<(u32, WindowRow)>,
    range_to_avg20: Option<f64>,
    compression_state: &'static str,
    directional_state: &'static str,
    range_like: bool,
    trend_direction: &'static str,
    trend_age: u64,
    pullback: bool,
}

#[derive(Debug, Clone, Copy)]
struct StatisticalCandle {
    open: i64,
    high: i64,
    low: i64,
    close: i64,
    doji: bool,
}

#[derive(Debug, Clone, Copy, Default)]
struct StatisticalWindow {
    std: Option<f64>,
    skew: Option<f64>,
    kurtosis: Option<f64>,
    autocorr: Option<f64>,
    reversal: Option<f64>,
    up_ratio: Option<f64>,
    r2: Option<f64>,
    residual: Option<f64>,
    position: Option<f64>,
}

struct StatisticalRow {
    windows: Vec<(u32, StatisticalWindow)>,
    overlap: Option<f64>,
    pattern: Option<&'static str>,
}

struct Statistics {
    candles: History<StatisticalCandle>,
    returns: History<Option<f64>>,
    windows: Vec<u32>,
}

fn finite_six(value: f64) -> Option<f64> {
    value
        .is_finite()
        .then(|| six(value))
        .filter(|value| value.is_finite())
}

fn pearson(left: &[f64], right: &[f64]) -> Option<f64> {
    let n = left.len() as f64;
    let left_mean = sum(left.iter().copied()) / n;
    let right_mean = sum(right.iter().copied()) / n;
    let cross = sum(left
        .iter()
        .zip(right)
        .map(|(a, b)| (a - left_mean) * (b - right_mean)));
    let left_var = sum(left.iter().map(|a| (a - left_mean).powi(2)));
    let right_var = sum(right.iter().map(|b| (b - right_mean).powi(2)));
    (left_var > 0.0 && right_var > 0.0)
        .then(|| cross / (left_var * right_var).sqrt())
        .and_then(finite_six)
}

fn candle_pattern(previous: StatisticalCandle, current: StatisticalCandle) -> &'static str {
    if previous.doji || current.doji {
        return "none";
    }
    let bullish = previous.close < previous.open && current.close > current.open;
    let bearish = previous.close > previous.open && current.close < current.open;
    if bullish && current.open <= previous.close && current.close >= previous.open {
        "bullish_engulfing"
    } else if bearish && current.open >= previous.close && current.close <= previous.open {
        "bearish_engulfing"
    } else if bullish && current.open >= previous.close && current.close <= previous.open {
        "bullish_harami"
    } else if bearish && current.open <= previous.close && current.close >= previous.open {
        "bearish_harami"
    } else {
        "none"
    }
}

impl Statistics {
    fn new(windows: Vec<u32>) -> Self {
        let capacity = windows.last().copied().unwrap_or(1) as usize + 1;
        Self {
            candles: History::new(capacity),
            returns: History::new(capacity),
            windows,
        }
    }

    fn update(
        &mut self,
        candle: &Candle,
        adjacent: bool,
        doji: bool,
        return_1: Option<f64>,
    ) -> StatisticalRow {
        let current = StatisticalCandle {
            open: candle.open_units,
            high: candle.high_units,
            low: candle.low_units,
            close: candle.close_units,
            doji,
        };
        let previous = self.candles.last();
        let overlap = previous
            .filter(|_| adjacent)
            .and_then(|prior| {
                let width = i128::from(current.high) - i128::from(current.low);
                (width > 0).then(|| {
                    let shared = (i128::from(current.high.min(prior.high))
                        - i128::from(current.low.max(prior.low)))
                    .max(0);
                    shared as f64 / width as f64
                })
            })
            .and_then(finite_six);
        let pattern = previous
            .filter(|_| adjacent)
            .map(|prior| candle_pattern(prior, current));
        self.candles.push(current);
        self.returns.push(return_1);
        let mut windows = Vec::with_capacity(self.windows.len());
        for &w in &self.windows {
            let n = w as usize;
            let mut row = StatisticalWindow::default();
            if self.candles.len() >= n {
                let candles: Vec<_> = self.candles.tail(n).collect();
                let min_low = candles.iter().map(|c| c.low).min().unwrap();
                let max_high = candles.iter().map(|c| c.high).max().unwrap();
                if max_high > min_low {
                    row.position = finite_six(
                        (i128::from(current.close) - i128::from(min_low)) as f64
                            / (i128::from(max_high) - i128::from(min_low)) as f64,
                    );
                }
                if n >= 3 {
                    let reference = i128::from(candles[0].close);
                    let closes: Vec<f64> = candles
                        .iter()
                        .map(|c| (i128::from(c.close) - reference) as f64)
                        .collect();
                    let mean_x = (n - 1) as f64 / 2.0;
                    let mean_y = sum(closes.iter().copied()) / n as f64;
                    let sxx = sum((0..n).map(|i| (i as f64 - mean_x).powi(2)));
                    let slope = sum(closes
                        .iter()
                        .enumerate()
                        .map(|(i, y)| (i as f64 - mean_x) * (y - mean_y)))
                        / sxx;
                    let fitted_last = mean_y + slope * ((n - 1) as f64 - mean_x);
                    let sse = sum(closes
                        .iter()
                        .enumerate()
                        .map(|(i, y)| (y - (mean_y + slope * (i as f64 - mean_x))).powi(2)));
                    let sst = sum(closes.iter().map(|y| (y - mean_y).powi(2)));
                    if sst > 0.0 {
                        row.r2 = finite_six(1.0 - sse / sst);
                    }
                    if current.close != 0 {
                        row.residual = finite_six(
                            10_000.0 * (closes[n - 1] - fitted_last) / current.close as f64,
                        );
                    }
                }
            }
            if self.returns.len() >= n {
                let returns: Option<Vec<f64>> = self.returns.tail(n).collect();
                if let Some(values) = returns {
                    let mean = sum(values.iter().copied()) / n as f64;
                    let variance = sum(values.iter().map(|v| (v - mean).powi(2))) / n as f64;
                    row.std = finite_six(variance.sqrt());
                    let absolute = sum(values.iter().map(|v| v.abs()));
                    if absolute > 0.0 {
                        row.up_ratio =
                            finite_six(sum(values.iter().copied().filter(|v| *v > 0.0)) / absolute);
                    }
                    if variance > 0.0 && n >= 3 {
                        row.skew = finite_six(
                            sum(values.iter().map(|v| (v - mean).powi(3)))
                                / n as f64
                                / variance.powf(1.5),
                        );
                    }
                    if variance > 0.0 && n >= 4 {
                        row.kurtosis = finite_six(
                            sum(values.iter().map(|v| (v - mean).powi(4)))
                                / n as f64
                                / variance.powi(2)
                                - 3.0,
                        );
                    }
                    if n >= 4 {
                        row.autocorr = pearson(&values[..n - 1], &values[1..]);
                        let pairs: Vec<_> = values
                            .windows(2)
                            .filter(|pair| pair[0] != 0.0 && pair[1] != 0.0)
                            .collect();
                        if !pairs.is_empty() {
                            row.reversal = finite_six(
                                pairs
                                    .iter()
                                    .filter(|pair| pair[0].signum() != pair[1].signum())
                                    .count() as f64
                                    / pairs.len() as f64,
                            );
                        }
                    }
                }
            }
            windows.push((w, row));
        }
        StatisticalRow {
            windows,
            overlap,
            pattern,
        }
    }

    #[cfg(test)]
    fn update_test(&mut self, candle: &Candle, adjacent: bool, doji: bool) -> StatisticalRow {
        let previous = self.candles.last();
        let return_1 = previous.and_then(|prior| {
            bps_size(
                (i128::from(candle.close_units) - i128::from(prior.close)) as f64,
                prior.close as f64,
            )
        });
        self.update(candle, adjacent, doji, return_1)
    }
}

impl Rolling {
    fn new(settings: &StructureSettings) -> Self {
        let max_len = *settings
            .rolling_windows
            .last()
            .expect("windows are non-empty") as usize
            + 1;
        Self {
            settings: settings.clone(),
            closes: History::new(max_len),
            abs_returns: History::new(max_len),
            ranges: History::new(max_len),
            tick_volumes: History::new(max_len),
            trend_direction: "none",
            trend_age: 0,
            sideways: 0,
        }
    }

    fn update(&mut self, anatomy: &Anatomy, tick_volume: Option<u64>) -> RollingRow {
        let close = anatomy.close;
        let previous_close = self.closes.last();
        let return_1 = previous_close.and_then(|previous| bps_change(close, previous));
        if let Some(value) = return_1 {
            self.abs_returns.push(value.abs());
        }
        self.ranges.push(anatomy.range_bps.unwrap_or(0.0));
        if let Some(volume) = tick_volume {
            self.tick_volumes.push(volume);
        }
        let mut windows = Vec::with_capacity(self.settings.rolling_windows.len());
        for &w in &self.settings.rolling_windows {
            let window = w as usize;
            let mut row = WindowRow::default();
            if self.closes.len() >= window {
                let reference = self.closes.nth_from_end(window);
                row.momentum = bps_change(close, reference);
            }
            if self.abs_returns.len() >= window {
                let total = sum(self.abs_returns.tail(window));
                row.abs_return_mean = Some(total / w as f64);
                if let Some(momentum) = row.momentum
                    && total > 0.0
                {
                    row.efficiency = Some((momentum.abs() / total).min(1.0));
                }
            }
            if self.ranges.len() >= window {
                row.range_mean = Some(sum(self.ranges.tail(window)) / w as f64);
            }
            if tick_volume.is_some() && self.tick_volumes.len() >= window {
                let total: u64 = self.tick_volumes.tail(window).sum();
                row.tick_volume_mean = Some(total as f64 / w as f64);
            }
            row.momentum = row.momentum.map(six);
            row.efficiency = row.efficiency.map(six);
            row.abs_return_mean = row.abs_return_mean.map(six);
            row.range_mean = row.range_mean.map(six);
            row.tick_volume_mean = row.tick_volume_mean.map(six);
            windows.push((w, row));
        }
        let find = |w: u32| {
            windows
                .iter()
                .find(|(window, _)| *window == w)
                .map(|(_, row)| *row)
        };
        let settings = &self.settings;
        let short_mean = find(5).and_then(|row| row.range_mean);
        let long_mean = find(20).and_then(|row| row.range_mean);
        let range_to_avg20 = match (short_mean, long_mean) {
            (Some(short), Some(long)) if long > 0.0 => Some(short / long),
            _ => None,
        };
        let compression_state = match range_to_avg20 {
            None => "unknown",
            Some(ratio) if ratio < settings.compression_ratio_threshold => "compressed",
            Some(ratio) if ratio >= settings.extreme_ratio_threshold => "extreme",
            Some(ratio) if ratio >= settings.expanded_ratio_threshold => "expanded",
            Some(_) => "normal",
        };
        let direction = find(settings.direction_window);
        let direction_momentum = direction.and_then(|row| row.momentum);
        let direction_efficiency = direction.and_then(|row| row.efficiency);
        let directional_state = match (direction_momentum, direction_efficiency) {
            (Some(momentum), Some(efficiency)) => {
                if momentum.abs() < settings.trend_min_abs_momentum_bps
                    || efficiency < settings.trend_efficiency_threshold
                {
                    "sideways"
                } else if momentum > 0.0 {
                    "up"
                } else {
                    "down"
                }
            }
            _ => "unknown",
        };
        let range_like = direction_efficiency
            .is_some_and(|efficiency| efficiency <= settings.range_efficiency_threshold)
            && matches!(compression_state, "compressed" | "normal");
        let pullback = matches!(self.trend_direction, "up" | "down")
            && self.trend_age >= u64::from(settings.pullback_min_trend_age)
            && matches!(anatomy.direction, "up" | "down")
            && anatomy.direction != self.trend_direction;
        if matches!(directional_state, "up" | "down") {
            if directional_state == self.trend_direction {
                self.trend_age += 1;
            } else {
                self.trend_direction = directional_state;
                self.trend_age = 1;
            }
            self.sideways = 0;
        } else {
            self.sideways += 1;
            if self.sideways >= u64::from(settings.trend_reset_sideways_bars) {
                self.trend_direction = "none";
                self.trend_age = 0;
            }
        }
        self.closes.push(close);
        RollingRow {
            return_1_unrounded: return_1,
            return_1_bps: return_1.map(six),
            windows,
            range_to_avg20: range_to_avg20.map(six),
            compression_state,
            directional_state,
            range_like,
            trend_direction: self.trend_direction,
            trend_age: self.trend_age,
            pullback,
        }
    }
}

/// One accepted candle as the swing buffer holds it.
#[derive(Debug, Clone, Copy)]
struct SwingCandle {
    row: u64,
    ordinal: u64,
    close_time: i64,
    /// The candle's actual availability: the known-at time of the record that finalized it.
    known_at: i64,
    high_units: i64,
    low_units: i64,
}

/// The last confirmed swing of one side.
#[derive(Debug, Clone, Copy)]
struct SwingPoint {
    price_units: i64,
    event_close: i64,
    confirm_close: i64,
    confirm_row: u64,
}

/// The level a row event was measured against and the event close it refers to.
#[derive(Debug, Clone, Copy)]
struct EventReference {
    kind: &'static str,
    level_units: i64,
    close: i64,
}

/// A breakout whose failure window is still open.
#[derive(Debug, Clone, Copy)]
struct ActiveBreakout {
    up: bool,
    level_units: i64,
    start_row: u64,
    event_close: i64,
}

/// One structure event: a confirmed swing or a close-time event of the accepted row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StructureEvent {
    pub event_id: u64,
    pub event_type: &'static str,
    pub direction: &'static str,
    /// The close time of the event candle: the swing center or the event row.
    pub event_close_micros: i64,
    /// The close time of the candle that made the event known.
    pub confirm_close_micros: i64,
    /// The actual availability of the event: the confirming candle's known-at time.
    pub known_at_micros: i64,
    pub event_row: u64,
    pub confirm_row: u64,
    pub event_candle_ordinal: u64,
    pub confirm_candle_ordinal: u64,
    pub price_units: i64,
    pub level_units: i64,
    /// `strict_left_right` for a swing, or the referenced swing or breakout's event close.
    pub reference: &'static str,
    pub reference_close_micros: Option<i64>,
}

/// The swing, event, and structure-state machine that advances across accepted candles.
#[derive(Debug, Clone)]
struct Structure {
    left: usize,
    right: usize,
    failed_breakout_max_bars: u64,
    buffer: History<SwingCandle>,
    last_high: Option<SwingPoint>,
    last_low: Option<SwingPoint>,
    active_breakout: Option<ActiveBreakout>,
    last_direction: &'static str,
    previous_close_units: Option<i64>,
    event_counter: u64,
}

/// The structure outputs of one accepted candle.
#[derive(Debug, Clone)]
struct StructureRow {
    last_high: Option<SwingPoint>,
    last_low: Option<SwingPoint>,
    bars_since_high: Option<u64>,
    bars_since_low: Option<u64>,
    distance_high_bps: Option<f64>,
    distance_low_bps: Option<f64>,
    new_high: bool,
    new_low: bool,
    breakout_up: bool,
    breakout_down: bool,
    sweep_high: bool,
    sweep_low: bool,
    failed_up: bool,
    failed_down: bool,
    break_of_structure: bool,
    change_of_character: bool,
    state: &'static str,
    event_types: String,
    event_close: Option<i64>,
}

impl Structure {
    fn new(settings: &StructureSettings) -> Self {
        let (left, right) = (settings.swing_left as usize, settings.swing_right as usize);
        Self {
            left,
            right,
            failed_breakout_max_bars: u64::from(settings.failed_breakout_max_bars),
            buffer: History::new(left + right + 1),
            last_high: None,
            last_low: None,
            active_breakout: None,
            last_direction: "none",
            previous_close_units: None,
            event_counter: 0,
        }
    }

    fn event(
        &mut self,
        event_type: &'static str,
        direction: &'static str,
        candle: SwingCandle,
        price_units: i64,
        reference: EventReference,
    ) -> StructureEvent {
        self.event_counter += 1;
        StructureEvent {
            event_id: self.event_counter,
            event_type,
            direction,
            event_close_micros: candle.close_time,
            confirm_close_micros: candle.close_time,
            known_at_micros: candle.known_at,
            event_row: candle.row,
            confirm_row: candle.row,
            event_candle_ordinal: candle.ordinal,
            confirm_candle_ordinal: candle.ordinal,
            price_units,
            level_units: reference.level_units,
            reference: reference.kind,
            reference_close_micros: Some(reference.close),
        }
    }

    /// Advances on one accepted candle: confirms swings, detects the row's events, and returns
    /// the row's structure outputs plus every event in reference order.
    fn update(
        &mut self,
        candle: SwingCandle,
        anatomy: &Anatomy,
        rolling: &RollingRow,
        events: &mut Vec<StructureEvent>,
    ) -> StructureRow {
        self.buffer.push(candle);
        let mut new_high = false;
        let mut new_low = false;
        if self.buffer.len() == self.left + self.right + 1 {
            let rows: Vec<SwingCandle> = self.buffer.values.iter().copied().collect();
            let center_index = rows.len() - self.right - 1;
            let center = rows[center_index];
            let others = rows[center_index - self.left..center_index]
                .iter()
                .chain(&rows[center_index + 1..]);
            let is_high = others
                .clone()
                .all(|other| center.high_units > other.high_units);
            let is_low = others
                .clone()
                .all(|other| center.low_units < other.low_units);
            for (is_swing, event_type, direction, price, point, flag) in [
                (
                    is_high,
                    "swing_high",
                    "up",
                    center.high_units,
                    &mut self.last_high,
                    &mut new_high,
                ),
                (
                    is_low,
                    "swing_low",
                    "down",
                    center.low_units,
                    &mut self.last_low,
                    &mut new_low,
                ),
            ] {
                if !is_swing {
                    continue;
                }
                *flag = true;
                *point = Some(SwingPoint {
                    price_units: price,
                    event_close: center.close_time,
                    confirm_close: candle.close_time,
                    confirm_row: candle.row,
                });
                self.event_counter += 1;
                events.push(StructureEvent {
                    event_id: self.event_counter,
                    event_type,
                    direction,
                    event_close_micros: center.close_time,
                    confirm_close_micros: candle.close_time,
                    known_at_micros: candle.known_at,
                    event_row: center.row,
                    confirm_row: candle.row,
                    event_candle_ordinal: center.ordinal,
                    confirm_candle_ordinal: candle.ordinal,
                    price_units: price,
                    level_units: price,
                    reference: "strict_left_right",
                    reference_close_micros: None,
                });
            }
        }
        let close = anatomy.close_units;
        let mut tokens: Vec<&'static str> = Vec::new();
        if new_high {
            tokens.push("swing_high:up");
        }
        if new_low {
            tokens.push("swing_low:down");
        }
        let (mut failed_up, mut failed_down) = (false, false);
        if let Some(active) = self.active_breakout {
            let age = candle.row - active.start_row;
            let failed = candle.row > active.start_row
                && if active.up {
                    close < active.level_units
                } else {
                    close > active.level_units
                };
            if age > self.failed_breakout_max_bars {
                self.active_breakout = None;
            } else if failed {
                let direction = if active.up { "up" } else { "down" };
                events.push(self.event(
                    "failed_breakout",
                    direction,
                    candle,
                    close,
                    EventReference {
                        kind: "active_breakout",
                        level_units: active.level_units,
                        close: active.event_close,
                    },
                ));
                tokens.push(if active.up {
                    failed_up = true;
                    "failed_breakout:up"
                } else {
                    failed_down = true;
                    "failed_breakout:down"
                });
                self.active_breakout = None;
            }
        }
        let (mut sweep_high, mut sweep_low) = (false, false);
        let (mut breakout_up, mut breakout_down) = (false, false);
        let (mut break_of_structure, mut change_of_character) = (false, false);
        if let Some(high) = self.last_high {
            let level = high.price_units;
            if candle.high_units > level && close < level {
                events.push(self.event(
                    "sweep_reject_high",
                    "down",
                    candle,
                    close,
                    EventReference {
                        kind: "last_swing_high",
                        level_units: level,
                        close: high.event_close,
                    },
                ));
                sweep_high = true;
                tokens.push("sweep_reject_high:down");
            }
            if close > level
                && self
                    .previous_close_units
                    .is_none_or(|previous| previous <= level)
            {
                let kind = if matches!(self.last_direction, "none" | "up") {
                    break_of_structure = true;
                    "break_of_structure"
                } else {
                    change_of_character = true;
                    "change_of_character"
                };
                for event_type in ["breakout", kind] {
                    events.push(self.event(
                        event_type,
                        "up",
                        candle,
                        close,
                        EventReference {
                            kind: "last_swing_high",
                            level_units: level,
                            close: high.event_close,
                        },
                    ));
                }
                breakout_up = true;
                tokens.push("breakout:up");
                tokens.push(if break_of_structure {
                    "break_of_structure:up"
                } else {
                    "change_of_character:up"
                });
                self.active_breakout = Some(ActiveBreakout {
                    up: true,
                    level_units: level,
                    start_row: candle.row,
                    event_close: candle.close_time,
                });
                self.last_direction = "up";
            }
        }
        if let Some(low) = self.last_low {
            let level = low.price_units;
            if candle.low_units < level && close > level {
                events.push(self.event(
                    "sweep_reject_low",
                    "up",
                    candle,
                    close,
                    EventReference {
                        kind: "last_swing_low",
                        level_units: level,
                        close: low.event_close,
                    },
                ));
                sweep_low = true;
                tokens.push("sweep_reject_low:up");
            }
            if close < level
                && self
                    .previous_close_units
                    .is_none_or(|previous| previous >= level)
            {
                let bos = matches!(self.last_direction, "none" | "down");
                let kind = if bos {
                    break_of_structure = true;
                    "break_of_structure"
                } else {
                    change_of_character = true;
                    "change_of_character"
                };
                for event_type in ["breakout", kind] {
                    events.push(self.event(
                        event_type,
                        "down",
                        candle,
                        close,
                        EventReference {
                            kind: "last_swing_low",
                            level_units: level,
                            close: low.event_close,
                        },
                    ));
                }
                breakout_down = true;
                tokens.push("breakout:down");
                tokens.push(if bos {
                    "break_of_structure:down"
                } else {
                    "change_of_character:down"
                });
                self.active_breakout = Some(ActiveBreakout {
                    up: false,
                    level_units: level,
                    start_row: candle.row,
                    event_close: candle.close_time,
                });
                self.last_direction = "down";
            }
        }
        let state = if failed_up || failed_down {
            "failed_breakout"
        } else if sweep_high || sweep_low {
            "sweep_reject"
        } else if breakout_up || breakout_down {
            "breakout"
        } else if rolling.range_like {
            "range"
        } else if rolling.compression_state == "compressed" {
            "compression"
        } else if rolling.directional_state == "up" {
            "trend_up"
        } else if rolling.directional_state == "down" {
            "trend_down"
        } else {
            "neutral"
        };
        let distance = |point: Option<SwingPoint>| {
            point.and_then(|point| {
                bps_change(anatomy.close, point.price_units as f64 / anatomy.unit)
            })
        };
        let row = StructureRow {
            last_high: self.last_high,
            last_low: self.last_low,
            bars_since_high: self.last_high.map(|point| candle.row - point.confirm_row),
            bars_since_low: self.last_low.map(|point| candle.row - point.confirm_row),
            distance_high_bps: distance(self.last_high).map(six),
            distance_low_bps: distance(self.last_low).map(six),
            new_high,
            new_low,
            breakout_up,
            breakout_down,
            sweep_high,
            sweep_low,
            failed_up,
            failed_down,
            break_of_structure,
            change_of_character,
            state,
            event_close: (!tokens.is_empty()).then_some(candle.close_time),
            event_types: tokens.join("|"),
        };
        self.previous_close_units = Some(close);
        row
    }
}

/// One confirmed swing as the sequence stage classified it.
#[derive(Debug, Clone, Copy)]
struct SequencePoint {
    price_units: i64,
    event_close: i64,
    confirm_close: i64,
    confirm_row: u64,
    swing_type: &'static str,
}

/// One sequence event: a newly confirmed swing with its type against the previous same-side
/// swing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SequenceEvent {
    pub event_id: u64,
    pub row: u64,
    pub candle_ordinal: u64,
    pub decision_close_micros: i64,
    /// The actual availability of the event: the confirming candle's known-at time.
    pub known_at_micros: i64,
    pub swing_event_type: &'static str,
    pub swing_type: &'static str,
    pub swing_price_units: i64,
    pub swing_event_close_micros: i64,
    pub swing_confirm_close_micros: i64,
    pub previous_price_units: Option<i64>,
    pub previous_event_close_micros: Option<i64>,
    pub previous_confirm_close_micros: Option<i64>,
    pub sequence_after: &'static str,
    pub bias_after: &'static str,
}

/// The confirmed-swing sequence state that advances across accepted candles.
#[derive(Debug, Clone)]
struct Sequence {
    epsilon_units: i64,
    previous_high: Option<SequencePoint>,
    previous_low: Option<SequencePoint>,
    last_confirmed: [Option<SequencePoint>; 4],
    event_counter: u64,
}

/// The sequence outputs of one accepted candle.
#[derive(Debug, Clone)]
struct SequenceRow {
    high_type: &'static str,
    low_type: &'static str,
    last_high_type: &'static str,
    last_low_type: &'static str,
    newly: [bool; 4],
    sequence: &'static str,
    bias: &'static str,
    last_confirmed: [Option<SequencePoint>; 4],
    bars_since: [Option<u64>; 4],
}

fn classify_sequence(high: &'static str, low: &'static str) -> (&'static str, &'static str) {
    match (high, low) {
        ("HH", "HL") => ("bullish_hh_hl", "bullish"),
        ("LH", "LL") => ("bearish_lh_ll", "bearish"),
        ("HH", "LL") => ("expanding_hh_ll", "volatile_expansion"),
        ("LH", "HL") => ("contracting_lh_hl", "compression"),
        ("", _) | (_, "") => ("unknown", "unknown"),
        _ if high.starts_with("first") || low.starts_with("first") => ("warming_up", "unknown"),
        ("EH" | "EL", _) | (_, "EH" | "EL") => ("equal_level_mixed", "mixed"),
        _ => ("mixed", "mixed"),
    }
}

impl Sequence {
    fn new(epsilon_units: i64) -> Self {
        Self {
            epsilon_units,
            previous_high: None,
            previous_low: None,
            last_confirmed: [None; 4],
            event_counter: 0,
        }
    }

    fn update(
        &mut self,
        candle: SwingCandle,
        structure: &StructureRow,
        events: &mut Vec<SequenceEvent>,
    ) -> SequenceRow {
        let mut high_type = "";
        let mut low_type = "";
        let mut pending: Vec<(&'static str, SequencePoint, Option<SequencePoint>)> = Vec::new();
        for (is_new, point, previous, first, higher, lower, equal, is_high) in [
            (
                structure.new_high,
                structure.last_high,
                &mut self.previous_high,
                "first_high",
                "HH",
                "LH",
                "EH",
                true,
            ),
            (
                structure.new_low,
                structure.last_low,
                &mut self.previous_low,
                "first_low",
                "HL",
                "LL",
                "EL",
                false,
            ),
        ] {
            if !is_new {
                continue;
            }
            let point = point.expect("a newly confirmed swing has a point");
            let swing_type = match *previous {
                None => first,
                Some(before) => {
                    match beyond(point.price_units, before.price_units, self.epsilon_units) {
                        Ordering::Greater => higher,
                        Ordering::Less => lower,
                        Ordering::Equal => equal,
                    }
                }
            };
            let classified = SequencePoint {
                price_units: point.price_units,
                event_close: point.event_close,
                confirm_close: point.confirm_close,
                confirm_row: candle.row,
                swing_type,
            };
            let before = *previous;
            *previous = Some(classified);
            if let Some(index) = SwingType::ALL
                .iter()
                .position(|swing| swing.as_str() == swing_type)
            {
                self.last_confirmed[index] = Some(classified);
            }
            if is_high {
                high_type = swing_type;
            } else {
                low_type = swing_type;
            }
            pending.push((
                if is_high { "swing_high" } else { "swing_low" },
                classified,
                before,
            ));
        }
        let last_high_type = self.previous_high.map_or("", |point| point.swing_type);
        let last_low_type = self.previous_low.map_or("", |point| point.swing_type);
        let (sequence, bias) = classify_sequence(last_high_type, last_low_type);
        for (swing_event_type, point, before) in pending {
            self.event_counter += 1;
            events.push(SequenceEvent {
                event_id: self.event_counter,
                row: candle.row,
                candle_ordinal: candle.ordinal,
                decision_close_micros: candle.close_time,
                known_at_micros: candle.known_at,
                swing_event_type,
                swing_type: point.swing_type,
                swing_price_units: point.price_units,
                swing_event_close_micros: point.event_close,
                swing_confirm_close_micros: point.confirm_close,
                previous_price_units: before.map(|point| point.price_units),
                previous_event_close_micros: before.map(|point| point.event_close),
                previous_confirm_close_micros: before.map(|point| point.confirm_close),
                sequence_after: sequence,
                bias_after: bias,
            });
        }
        SequenceRow {
            high_type,
            low_type,
            last_high_type,
            last_low_type,
            newly: [
                high_type == "HH",
                low_type == "HL",
                high_type == "LH",
                low_type == "LL",
            ],
            sequence,
            bias,
            last_confirmed: self.last_confirmed,
            bars_since: self
                .last_confirmed
                .map(|point| point.map(|point| candle.row - point.confirm_row)),
        }
    }
}

/// A running mean whose total is kept by addition and subtraction, as the reference keeps it.
#[derive(Debug, Clone)]
struct RunningMean {
    max_len: usize,
    values: VecDeque<f64>,
    total: f64,
}

impl RunningMean {
    fn new(max_len: usize) -> Self {
        Self {
            max_len,
            values: VecDeque::with_capacity(max_len),
            total: 0.0,
        }
    }

    fn clear(&mut self) {
        self.values.clear();
        self.total = 0.0;
    }

    fn add(&mut self, value: f64) {
        self.values.push_back(value);
        self.total += value;
        if self.values.len() > self.max_len {
            self.total -= self.values.pop_front().expect("a value");
        }
    }

    fn mean(&self) -> Option<f64> {
        (!self.values.is_empty()).then(|| self.total / self.values.len() as f64)
    }
}

/// One exponential moving average with the reference's seeding and readiness.
#[derive(Debug, Clone)]
struct Ema {
    period: u32,
    value: Option<f64>,
    count: u64,
}

/// The moving-average outputs of one period on one candle.
#[derive(Debug, Clone, Copy)]
struct EmaRow {
    period: u32,
    value: f64,
    ready: bool,
    close_vs_bps: Option<f64>,
    close_above: bool,
    slope_bps: Option<f64>,
    slope_state: &'static str,
    close_vs_state: &'static str,
}

fn signed_bps_state(value: Option<f64>, ready: bool, prefix: &str) -> &'static str {
    let Some(value) = value.filter(|_| ready) else {
        return "not_ready";
    };
    let suffix = if value >= 5.0 {
        "strong_up"
    } else if value >= 1.0 {
        "mild_up"
    } else if value <= -5.0 {
        "strong_down"
    } else if value <= -1.0 {
        "mild_down"
    } else {
        "flat"
    };
    match (prefix, suffix) {
        ("ema_slope", "strong_up") => "ema_slope_strong_up",
        ("ema_slope", "mild_up") => "ema_slope_mild_up",
        ("ema_slope", "strong_down") => "ema_slope_strong_down",
        ("ema_slope", "mild_down") => "ema_slope_mild_down",
        ("ema_slope", _) => "ema_slope_flat",
        ("close_vs_ema", "strong_up") => "close_vs_ema_strong_up",
        ("close_vs_ema", "mild_up") => "close_vs_ema_mild_up",
        ("close_vs_ema", "strong_down") => "close_vs_ema_strong_down",
        ("close_vs_ema", "mild_down") => "close_vs_ema_mild_down",
        ("close_vs_ema", _) => "close_vs_ema_flat",
        (_, "strong_up") => "ema20_vs_ema50_strong_up",
        (_, "mild_up") => "ema20_vs_ema50_mild_up",
        (_, "strong_down") => "ema20_vs_ema50_strong_down",
        (_, "mild_down") => "ema20_vs_ema50_mild_down",
        _ => "ema20_vs_ema50_flat",
    }
}

impl Ema {
    fn preview(&self, close: f64) -> (f64, Option<f64>, u64) {
        let alpha = 2.0 / (f64::from(self.period) + 1.0);
        let value = match self.value {
            None => close,
            Some(previous) => (alpha * close) + ((1.0 - alpha) * previous),
        };
        (value, self.value, self.count + 1)
    }

    fn row(&self, close: f64) -> EmaRow {
        let (value, previous, count) = self.preview(close);
        let ready = count >= u64::from(self.period);
        let close_vs_bps = (value != 0.0).then(|| (close - value) / value * 10_000.0);
        let slope_bps = previous
            .filter(|previous| *previous != 0.0)
            .map(|previous| (value - previous) / previous * 10_000.0);
        EmaRow {
            period: self.period,
            value,
            ready,
            close_vs_bps: close_vs_bps.map(six),
            close_above: close >= value,
            slope_bps: slope_bps.map(six),
            slope_state: signed_bps_state(slope_bps, ready, "ema_slope"),
            close_vs_state: signed_bps_state(close_vs_bps, ready, "close_vs_ema"),
        }
    }

    fn add(&mut self, close: f64) {
        let (value, _, count) = self.preview(close);
        self.value = Some(value);
        self.count = count;
    }
}

/// The candle-shape and moving-average state that resets when accepted candles are not
/// adjacent.
#[derive(Debug, Clone)]
struct Shape {
    relative: Option<(u32, RunningMean, RunningMean, RunningMean)>,
    emas: Vec<Ema>,
    previous: Option<(u64, i64, i64)>,
    segment: u64,
    segment_candle: u64,
}

/// The shape outputs of one accepted candle.
#[derive(Debug, Clone)]
struct ShapeRow {
    segment: u64,
    segment_candle: u64,
    history: Option<u64>,
    adjacent: bool,
    relation: &'static str,
    color: &'static str,
    candle_type: String,
    range_bps_bucket: &'static str,
    body_bps_bucket: &'static str,
    range_ratio: Option<f64>,
    body_ratio: Option<f64>,
    volume_ratio: Option<f64>,
    range_bucket: &'static str,
    body_bucket: &'static str,
    volume_bucket: &'static str,
    upper_bucket: &'static str,
    lower_bucket: &'static str,
    wick_profile: &'static str,
    close_bucket: &'static str,
    dominance: &'static str,
    doji: bool,
    strong: bool,
    pin: bool,
    hammer: bool,
    shooting_star: bool,
    inside: bool,
    outside: bool,
    expansion: bool,
    compression: bool,
    emas: Vec<EmaRow>,
    pair: Option<(Option<f64>, bool, &'static str)>,
}

fn relative_bucket(ratio: Option<f64>) -> &'static str {
    match ratio {
        None => "unknown_warmup",
        Some(ratio) if ratio <= 0.35 => "tiny",
        Some(ratio) if ratio <= 0.70 => "small",
        Some(ratio) if ratio <= 1.35 => "normal",
        Some(ratio) if ratio <= 2.25 => "large",
        Some(_) => "extreme",
    }
}

fn bps_bucket(value: f64) -> &'static str {
    if value <= 0.0 {
        "flat"
    } else if value <= 1.0 {
        "tiny"
    } else if value <= 3.0 {
        "small"
    } else if value <= 8.0 {
        "normal"
    } else if value <= 18.0 {
        "large"
    } else {
        "extreme"
    }
}

fn wick_size_bucket(ratio: f64) -> &'static str {
    if ratio <= 0.03 {
        "none"
    } else if ratio <= 0.12 {
        "tiny"
    } else if ratio <= 0.25 {
        "small"
    } else if ratio <= 0.45 {
        "normal"
    } else if ratio <= 0.65 {
        "large"
    } else {
        "extreme"
    }
}

fn close_location_bucket(position: f64) -> &'static str {
    if position >= 0.85 {
        "close_near_high"
    } else if position >= 0.65 {
        "upper_mid_close"
    } else if position >= 0.35 {
        "middle_close"
    } else if position >= 0.15 {
        "lower_mid_close"
    } else {
        "close_near_low"
    }
}

fn body_dominance_bucket(ratio: f64) -> &'static str {
    if ratio <= 0.10 {
        "doji_body"
    } else if ratio <= 0.25 {
        "small_body"
    } else if ratio <= 0.55 {
        "balanced_body"
    } else if ratio <= 0.80 {
        "body_dominant"
    } else {
        "marubozu_like"
    }
}

fn wick_profile(upper: f64, lower: f64, body: f64) -> &'static str {
    if upper >= 0.45 && lower >= 0.45 {
        "long_wicks_both"
    } else if upper >= 0.50 && upper >= lower * 1.75 {
        "upper_rejection"
    } else if lower >= 0.50 && lower >= upper * 1.75 {
        "lower_rejection"
    } else if upper <= 0.10 && lower <= 0.10 && body >= 0.75 {
        "clean_body"
    } else if upper >= 0.25 || lower >= 0.25 {
        "wicked"
    } else {
        "balanced_wicks"
    }
}

impl Shape {
    fn new(relative: Option<(u32, u32)>, periods: &[u32]) -> Self {
        Self {
            relative: relative.map(|(window, min_history)| {
                let window = window as usize;
                (
                    min_history,
                    RunningMean::new(window),
                    RunningMean::new(window),
                    RunningMean::new(window),
                )
            }),
            emas: periods
                .iter()
                .map(|&period| Ema {
                    period,
                    value: None,
                    count: 0,
                })
                .collect(),
            previous: None,
            segment: 0,
            segment_candle: 0,
        }
    }

    fn update(
        &mut self,
        candle: SwingCandle,
        anatomy: &Anatomy,
        tick_volume: Option<u64>,
    ) -> ShapeRow {
        let adjacent = self
            .previous
            .is_some_and(|(ordinal, _, _)| candle.ordinal == ordinal + 1);
        match self.previous {
            None => {
                self.segment = 1;
                self.segment_candle = 0;
            }
            Some(_) if !adjacent => {
                self.segment += 1;
                self.segment_candle = 0;
                if let Some((_, ranges, bodies, volumes)) = &mut self.relative {
                    ranges.clear();
                    bodies.clear();
                    volumes.clear();
                }
                for ema in &mut self.emas {
                    ema.value = None;
                    ema.count = 0;
                }
            }
            Some(_) => self.segment_candle += 1,
        }
        let body_bps = anatomy.body_bps.unwrap_or(0.0).abs();
        let range_bps = anatomy.range_bps.unwrap_or(0.0);
        let (upper, lower, body, position) = (
            anatomy.upper_wick_to_range,
            anatomy.lower_wick_to_range,
            anatomy.body_to_range,
            anatomy.close_position,
        );
        let color = match anatomy.direction {
            "up" => "bullish",
            "down" => "bearish",
            _ => "neutral",
        };
        let (history, range_ratio, body_ratio, volume_ratio) = match &self.relative {
            None => (None, None, None, None),
            Some((min_history, ranges, bodies, volumes)) => {
                let count = ranges.values.len().min(bodies.values.len());
                let count = if tick_volume.is_some() {
                    count.min(volumes.values.len())
                } else {
                    count
                };
                let ratio = |value: f64, mean: Option<f64>| {
                    if count < *min_history as usize {
                        return None;
                    }
                    mean.filter(|mean| *mean > 0.0).map(|mean| value / mean)
                };
                (
                    Some(count as u64),
                    ratio(range_bps, ranges.mean()),
                    ratio(body_bps, bodies.mean()),
                    tick_volume.and_then(|volume| ratio(volume as f64, volumes.mean())),
                )
            }
        };
        let doji = body <= 0.10;
        let strong = body >= 0.65
            && ((color == "bullish" && position >= 0.70)
                || (color == "bearish" && position <= 0.30));
        let hammer = lower >= 0.55 && upper <= 0.25 && body <= 0.35 && position >= 0.55;
        let shooting_star = upper >= 0.55 && lower <= 0.25 && body <= 0.35 && position <= 0.45;
        let pin = hammer
            || shooting_star
            || (body <= 0.35 && (upper >= 0.60 || lower >= 0.60) && (upper - lower).abs() >= 0.30);
        let (mut inside, mut outside) = (false, false);
        let relation = match self.previous {
            None => "first_clean_candle",
            Some(_) if !adjacent => "no_adjacent_previous_clean_candle",
            Some((_, previous_high, previous_low)) => {
                let (high, low) = (candle.high_units, candle.low_units);
                inside = high < previous_high && low > previous_low;
                outside = high > previous_high && low < previous_low;
                if inside {
                    "inside_previous"
                } else if outside {
                    "outside_previous"
                } else if high > previous_high && low >= previous_low {
                    "higher_range"
                } else if high <= previous_high && low < previous_low {
                    "lower_range"
                } else {
                    "overlap_previous"
                }
            }
        };
        let expansion = range_ratio.is_some_and(|ratio| ratio >= 1.50) && body >= 0.40;
        let compression = range_ratio.is_some_and(|ratio| ratio <= 0.70);
        let candle_type = if doji {
            if upper >= 0.25 && lower >= 0.25 {
                "long_legged_doji".to_string()
            } else {
                "doji".to_string()
            }
        } else if hammer {
            format!("hammer_like_{color}")
        } else if shooting_star {
            format!("shooting_star_like_{color}")
        } else if pin {
            format!("pin_bar_{color}")
        } else if strong {
            format!("strong_{color}")
        } else if body <= 0.25 && upper >= 0.20 && lower >= 0.20 {
            format!("spinning_top_{color}")
        } else if position >= 0.75 {
            format!("high_close_{color}")
        } else if position <= 0.25 {
            format!("low_close_{color}")
        } else {
            format!("normal_{color}")
        };
        let emas: Vec<EmaRow> = self.emas.iter().map(|ema| ema.row(anatomy.close)).collect();
        let pair = match (
            emas.iter().find(|row| row.period == 20),
            emas.iter().find(|row| row.period == 50),
        ) {
            (Some(fast), Some(slow)) => {
                let ready = fast.ready && slow.ready;
                let spread =
                    (slow.value != 0.0).then(|| (fast.value - slow.value) / slow.value * 10_000.0);
                Some((
                    spread.map(six),
                    fast.value >= slow.value,
                    signed_bps_state(spread, ready, "ema20_vs_ema50"),
                ))
            }
            _ => None,
        };
        let row = ShapeRow {
            segment: self.segment,
            segment_candle: self.segment_candle,
            history,
            adjacent,
            relation,
            color,
            candle_type,
            range_bps_bucket: bps_bucket(range_bps),
            body_bps_bucket: bps_bucket(body_bps),
            range_ratio: range_ratio.map(six),
            body_ratio: body_ratio.map(six),
            volume_ratio: volume_ratio.map(six),
            range_bucket: relative_bucket(range_ratio),
            body_bucket: relative_bucket(body_ratio),
            volume_bucket: relative_bucket(volume_ratio),
            upper_bucket: wick_size_bucket(upper),
            lower_bucket: wick_size_bucket(lower),
            wick_profile: wick_profile(upper, lower, body),
            close_bucket: close_location_bucket(position),
            dominance: body_dominance_bucket(body),
            doji,
            strong,
            pin,
            hammer,
            shooting_star,
            inside,
            outside,
            expansion,
            compression,
            emas,
            pair,
        };
        if let Some((_, ranges, bodies, volumes)) = &mut self.relative {
            ranges.add(range_bps);
            bodies.add(body_bps);
            if let Some(volume) = tick_volume {
                volumes.add(volume as f64);
            }
        }
        for ema in &mut self.emas {
            ema.add(anatomy.close);
        }
        self.previous = Some((candle.ordinal, candle.high_units, candle.low_units));
        row
    }
}

/// The candle-quality facts the regime and the row read from the finalized candle.
#[derive(Debug, Clone, Copy)]
struct Quality {
    complete: bool,
    low_volume: bool,
    hard_low_volume: bool,
    has_gap: bool,
    has_internal_gap: bool,
    starts_after_gap: bool,
    frozen: bool,
    true_jump: bool,
    delay_jump: bool,
    reopen_jump: bool,
    max_abs_bps: f64,
}

/// The regime outputs of one accepted candle.
#[derive(Debug, Clone)]
struct RegimeRow {
    trend: &'static str,
    volatility: &'static str,
    structure: &'static str,
    transition: &'static str,
    quality: Option<&'static str>,
    bias: &'static str,
}

fn regime(
    rolling: Option<&RollingRow>,
    structure: Option<&StructureRow>,
    sequence: Option<&SequenceRow>,
    quality: Option<Quality>,
) -> RegimeRow {
    let compression = rolling.map_or("", |row| row.compression_state);
    let directional = rolling.map_or("", |row| row.directional_state);
    let trend_leg = rolling.map_or("", |row| row.trend_direction);
    let structure_bias = sequence.map_or("", |row| row.bias);
    let window = |w: u32| {
        rolling.and_then(|row| {
            row.windows
                .iter()
                .find(|(window, _)| *window == w)
                .and_then(|(_, row)| row.efficiency)
        })
    };
    let enough_efficiency = [window(10), window(20)]
        .into_iter()
        .flatten()
        .fold(0.0_f64, f64::max)
        >= 0.35;
    let trend = if matches!(compression, "compression" | "compressed") {
        "compression"
    } else if rolling.is_some_and(|row| row.range_like) || directional == "sideways" {
        "range"
    } else {
        let vote = |up: &str, down: &str, value: &str| -> (u32, u32) {
            (u32::from(value == up), u32::from(value == down))
        };
        let votes = [
            vote("up", "down", directional),
            vote("up", "down", trend_leg),
            vote("bullish", "bearish", structure_bias),
        ];
        let up: u32 = votes.iter().map(|(up, _)| up).sum();
        let down: u32 = votes.iter().map(|(_, down)| down).sum();
        if up == 0 && down == 0 {
            if compression.is_empty()
                && directional.is_empty()
                && trend_leg.is_empty()
                && structure_bias.is_empty()
            {
                "unknown"
            } else {
                "neutral"
            }
        } else if up > down && (enough_efficiency || up >= 2) {
            "trend_up"
        } else if down > up && (enough_efficiency || down >= 2) {
            "trend_down"
        } else {
            "neutral"
        }
    };
    let volatility = match rolling.and_then(|row| row.range_to_avg20) {
        None => "unknown",
        Some(value) if value < 0.60 => "quiet",
        Some(value) if value <= 1.80 => "normal",
        Some(value) if value <= 2.50 => "expanded",
        Some(_) => "extreme",
    };
    let structure_state = match structure_bias {
        "bullish" | "bearish" | "compression" | "volatile_expansion" | "mixed" | "neutral"
        | "unknown" => structure_bias,
        "" => "unknown",
        _ => "mixed",
    };
    let transition = match structure {
        None => "none",
        Some(row) if row.failed_up => "failed_breakout_up",
        Some(row) if row.failed_down => "failed_breakout_down",
        Some(row) if row.sweep_high => "sweep_high",
        Some(row) if row.sweep_low => "sweep_low",
        Some(row) if row.change_of_character => {
            if row.breakout_up {
                "coch_up"
            } else if row.breakout_down {
                "coch_down"
            } else {
                "coch"
            }
        }
        Some(row) if row.break_of_structure => {
            if row.breakout_up {
                "bos_up"
            } else if row.breakout_down {
                "bos_down"
            } else {
                "bos"
            }
        }
        Some(_) => "none",
    };
    let quality = quality.map(|q| {
        if q.frozen || q.true_jump || q.reopen_jump {
            "frozen_or_dirty"
        } else if q.has_gap
            || q.has_internal_gap
            || q.starts_after_gap
            || q.delay_jump
            || q.max_abs_bps > 5.0
        {
            "gap_or_jump"
        } else if q.low_volume || q.hard_low_volume {
            "low_volume"
        } else {
            "clean"
        }
    });
    let bias = if trend == "trend_up"
        || structure_state == "bullish"
        || matches!(
            transition,
            "bos_up" | "coch_up" | "sweep_low" | "failed_breakout_down"
        ) {
        "buy_bias"
    } else if trend == "trend_down"
        || structure_state == "bearish"
        || matches!(
            transition,
            "bos_down" | "coch_down" | "sweep_high" | "failed_breakout_up"
        )
    {
        "sell_bias"
    } else if matches!(trend, "range" | "compression")
        || matches!(
            structure_state,
            "compression" | "mixed" | "volatile_expansion"
        )
    {
        "two_sided_or_no_bias"
    } else {
        "neutral"
    };
    RegimeRow {
        trend,
        volatility,
        structure: structure_state,
        transition,
        quality,
        bias,
    }
}

/// Everything computed for one accepted candle, from which the selected outputs are read.
struct Scratch<'a> {
    candle: &'a Candle,
    ordinal: u64,
    quality: Option<Quality>,
    tick_path: Option<TickPathSummary>,
    jumps: Option<JumpMagnitudes>,
    anatomy: Anatomy,
    rolling: Option<RollingRow>,
    statistics: Option<StatisticalRow>,
    structure: Option<StructureRow>,
    sequence: Option<SequenceRow>,
    shape: ShapeRow,
    regime: RegimeRow,
}

impl Scratch<'_> {
    fn value(&self, field: Field) -> Option<Value> {
        use Field as F;
        use Value::{Bool, Float, Int, Time};
        let candle = self.candle;
        let text = Value::text;
        let swing = |index: usize| {
            self.sequence
                .as_ref()
                .and_then(|row| row.last_confirmed[index])
        };
        let window = |w: u32| {
            self.rolling.as_ref().and_then(|row| {
                row.windows
                    .iter()
                    .find(|(window, _)| *window == w)
                    .map(|(_, row)| *row)
            })
        };
        let ema = |p: u32| self.shape.emas.iter().find(|row| row.period == p);
        let tick = self.tick_path.as_ref();
        let quality = self.quality;
        let structure = self.structure.as_ref();
        let sequence = self.sequence.as_ref();
        let rolling = self.rolling.as_ref();
        let statistics = self.statistics.as_ref();
        let stat_window = |w: u32| {
            statistics?
                .windows
                .iter()
                .find(|(key, _)| *key == w)
                .map(|(_, row)| *row)
        };
        let count = |value: u64| Int(value as i64);
        Some(match field {
            F::OpenTime => Time(candle.open_time_micros),
            F::CloseTime => Time(candle.close_time_micros),
            F::KnownAt => Time(candle.known_at_micros),
            F::FirstEvent => Time(candle.first_event_micros),
            F::LastEvent => Time(candle.last_event_micros),
            F::CandleOrdinal => count(self.ordinal),
            F::OpenUnits => Int(candle.open_units),
            F::HighUnits => Int(candle.high_units),
            F::LowUnits => Int(candle.low_units),
            F::CloseUnits => Int(candle.close_units),
            F::TickVolume => count(candle.observations),
            F::ActiveSpan => Int(candle.active_span_micros),
            F::Complete => Bool(quality?.complete),
            F::LowTickVolume => Bool(quality?.low_volume),
            F::HardLowTickVolume => Bool(quality?.hard_low_volume),
            F::HasGap => Bool(quality?.has_gap),
            F::HasInternalGap => Bool(quality?.has_internal_gap),
            F::StartsAfterGap => Bool(quality?.starts_after_gap),
            F::StartsAfterGapMicros => Int(candle.gap_before_micros.unwrap_or(0).max(0)),
            F::StartsAfterGapClass => text(candle.gap_before_micros.map_or("none", gap_class)),
            F::MissingBuckets => count(candle.missing_buckets_before),
            F::MaxInternalGap => Int(candle.max_gap_inside_micros),
            F::MaxGap => Int(max_gap(candle)),
            F::WorstGapClass => text(worst_gap_class(candle)),
            F::FrozenPriceFlag => Bool(quality?.frozen),
            F::MaxSamePriceRunTicks => count(candle.frozen_observations),
            F::MaxSamePriceRunMicros => Int(candle.frozen_micros),
            F::HasTrueTickJump => Bool(quality?.true_jump),
            F::HasFeedDelayJump => Bool(quality?.delay_jump),
            F::HasGapReopenJump => Bool(quality?.reopen_jump),
            F::MaxAbsTickJumpBps => Float(six(self.jumps?.abs)),
            F::MaxTrueTickJumpBps => Float(six(self.jumps?.true_)),
            F::MaxGapReopenJumpBps => Float(six(self.jumps?.reopen)),
            F::QualityTier => text(ELIGIBILITY_VERSION),
            F::TickPathReady => Bool(tick?.ready),
            F::TickPathDirectionalMoves => count(tick?.directional),
            F::TickPathUpticks => count(tick?.up),
            F::TickPathDownticks => count(tick?.down),
            F::TickPathFlats => count(tick?.flat),
            F::TickPathDirectionChanges => count(tick?.changes),
            F::TickPathSignedImbalance => Float(tick?.signed_imbalance),
            F::TickPathReversalRate => Float(tick?.reversal_rate),
            F::TickPathEfficiency => Float(tick?.efficiency),
            F::TickPathTerminalMoves => count(tick?.terminal_moves),
            F::TickPathTerminalSignedImbalance => Float(tick?.terminal_signed_imbalance),
            F::TickPathClosePosition => Float(tick?.close_position),
            F::TickPathPressureBucket => text(tick?.pressure),
            F::TickPathShapeBucket => text(tick?.shape),
            F::TickPathTerminalPressureBucket => text(tick?.terminal_bucket),
            F::TickPathFailedPressureDirection => text(tick?.failed),
            F::TickPathEfficiencyBucket => text(tick?.efficiency_bucket),
            F::TickPathReversalBucket => text(tick?.reversal_bucket),
            F::CandleDirection => text(self.anatomy.direction),
            F::BodyUnits => Int(self.anatomy.body_units?),
            F::RangeUnits => Int(self.anatomy.range_units?),
            F::UpperWickUnits => Int(self.anatomy.upper_wick_units?),
            F::LowerWickUnits => Int(self.anatomy.lower_wick_units?),
            F::BodyBps => Float(self.anatomy.body_bps?),
            F::RangeBps => Float(self.anatomy.range_bps?),
            F::UpperWickBps => Float(self.anatomy.upper_wick_bps?),
            F::LowerWickBps => Float(self.anatomy.lower_wick_bps?),
            F::ClosePosition => Float(self.anatomy.close_position),
            F::BodyToRange => Float(self.anatomy.body_to_range),
            F::UpperWickToRange => Float(self.anatomy.upper_wick_to_range),
            F::LowerWickToRange => Float(self.anatomy.lower_wick_to_range),
            F::Return1Bps => Float(rolling?.return_1_bps?),
            F::ReturnStd(w) => Float(stat_window(w)?.std?),
            F::ReturnSkew(w) => Float(stat_window(w)?.skew?),
            F::ReturnKurtosis(w) => Float(stat_window(w)?.kurtosis?),
            F::ReturnAutocorr(w) => Float(stat_window(w)?.autocorr?),
            F::SignReversalRate(w) => Float(stat_window(w)?.reversal?),
            F::UpMoveRatio(w) => Float(stat_window(w)?.up_ratio?),
            F::TrendR2(w) => Float(stat_window(w)?.r2?),
            F::TrendResidual(w) => Float(stat_window(w)?.residual?),
            F::RangePosition(w) => Float(stat_window(w)?.position?),
            F::RangeOverlap => Float(statistics?.overlap?),
            F::CandlePattern => text(statistics?.pattern?),
            F::Momentum(w) => Float(window(w)?.momentum?),
            F::Efficiency(w) => Float(window(w)?.efficiency?),
            F::AbsReturnMean(w) => Float(window(w)?.abs_return_mean?),
            F::RangeMean(w) => Float(window(w)?.range_mean?),
            F::TickVolumeMean(w) => Float(window(w)?.tick_volume_mean?),
            F::RangeToAvg20 => Float(rolling?.range_to_avg20?),
            F::CompressionState => text(rolling?.compression_state),
            F::DirectionalState => text(rolling?.directional_state),
            F::RangeLike => Bool(rolling?.range_like),
            F::TrendLegDirection => text(rolling?.trend_direction),
            F::TrendLegAge => count(rolling?.trend_age),
            F::PullbackAgainstTrend => Bool(rolling?.pullback),
            F::LastSwingHighUnits => Int(structure?.last_high?.price_units),
            F::LastSwingHighEventClose => Time(structure?.last_high?.event_close),
            F::LastSwingHighConfirmClose => Time(structure?.last_high?.confirm_close),
            F::BarsSinceSwingHigh => count(structure?.bars_since_high?),
            F::DistanceToSwingHighBps => Float(structure?.distance_high_bps?),
            F::LastSwingLowUnits => Int(structure?.last_low?.price_units),
            F::LastSwingLowEventClose => Time(structure?.last_low?.event_close),
            F::LastSwingLowConfirmClose => Time(structure?.last_low?.confirm_close),
            F::BarsSinceSwingLow => count(structure?.bars_since_low?),
            F::DistanceToSwingLowBps => Float(structure?.distance_low_bps?),
            F::NewlyConfirmedSwingHigh => Bool(structure?.new_high),
            F::NewlyConfirmedSwingLow => Bool(structure?.new_low),
            F::BreakoutUp => Bool(structure?.breakout_up),
            F::BreakoutDown => Bool(structure?.breakout_down),
            F::SweepRejectHigh => Bool(structure?.sweep_high),
            F::SweepRejectLow => Bool(structure?.sweep_low),
            F::FailedBreakoutUp => Bool(structure?.failed_up),
            F::FailedBreakoutDown => Bool(structure?.failed_down),
            F::BreakOfStructure => Bool(structure?.break_of_structure),
            F::ChangeOfCharacter => Bool(structure?.change_of_character),
            F::StructureState => text(structure?.state),
            F::CurrentEventTypes => Value::Text(Cow::Owned(structure?.event_types.clone())),
            F::CurrentEventClose => Time(structure?.event_close?),
            F::SwingHighType => text(sequence?.high_type),
            F::SwingLowType => text(sequence?.low_type),
            F::LastSwingHighType => text(sequence?.last_high_type),
            F::LastSwingLowType => text(sequence?.last_low_type),
            F::NewlyConfirmed(kind) => Bool(sequence?.newly[kind as usize]),
            F::MarketStructureSequence => text(sequence?.sequence),
            F::MarketStructureBias => text(sequence?.bias),
            F::LastConfirmedUnits(kind) => Int(swing(kind as usize)?.price_units),
            F::LastConfirmedEventClose(kind) => Time(swing(kind as usize)?.event_close),
            F::LastConfirmedConfirmClose(kind) => Time(swing(kind as usize)?.confirm_close),
            F::BarsSinceConfirmed(kind) => count(sequence?.bars_since[kind as usize]?),
            F::CleanSegmentIndex => count(self.shape.segment),
            F::CleanSegmentCandleIndex => count(self.shape.segment_candle),
            F::PriorCleanHistoryCount => count(self.shape.history?),
            F::HasAdjacentPrevious => Bool(self.shape.adjacent),
            F::PreviousCandleRelation => text(self.shape.relation),
            F::CandleColor => text(self.shape.color),
            F::CandleType => Value::Text(Cow::Owned(self.shape.candle_type.clone())),
            F::RangeBpsBucket => text(self.shape.range_bps_bucket),
            F::BodyBpsBucket => text(self.shape.body_bps_bucket),
            F::RangeVsRecentRatio => Float(self.shape.range_ratio?),
            F::BodyVsRecentRatio => Float(self.shape.body_ratio?),
            F::TickVolumeVsRecentRatio => Float(self.shape.volume_ratio?),
            F::RangeVsRecentBucket => text(self.shape.range_bucket),
            F::BodyVsRecentBucket => text(self.shape.body_bucket),
            F::TickVolumeVsRecentBucket => text(self.shape.volume_bucket),
            F::UpperWickSizeBucket => text(self.shape.upper_bucket),
            F::LowerWickSizeBucket => text(self.shape.lower_bucket),
            F::WickProfile => text(self.shape.wick_profile),
            F::CloseLocationBucket => text(self.shape.close_bucket),
            F::BodyDominance => text(self.shape.dominance),
            F::IsDoji => Bool(self.shape.doji),
            F::IsStrongBody => Bool(self.shape.strong),
            F::IsPinBar => Bool(self.shape.pin),
            F::IsHammerLike => Bool(self.shape.hammer),
            F::IsShootingStarLike => Bool(self.shape.shooting_star),
            F::IsInsideBar => Bool(self.shape.inside),
            F::IsOutsideBar => Bool(self.shape.outside),
            F::IsExpansionCandle => Bool(self.shape.expansion),
            F::IsCompressionCandle => Bool(self.shape.compression),
            F::Ema(p) => Float(ema(p)?.value),
            F::EmaReady(p) => Bool(ema(p)?.ready),
            F::CloseVsEmaBps(p) => Float(ema(p)?.close_vs_bps?),
            F::CloseAboveEma(p) => Bool(ema(p)?.close_above),
            F::EmaSlopeBps(p) => Float(ema(p)?.slope_bps?),
            F::EmaSlopeState(p) => text(ema(p)?.slope_state),
            F::CloseVsEmaState(p) => text(ema(p)?.close_vs_state),
            F::Ema20MinusEma50Bps => Float(self.shape.pair?.0?),
            F::Ema20AboveEma50 => Bool(self.shape.pair?.1),
            F::Ema20Ema50AlignmentState => text(self.shape.pair?.2),
            F::RegimeTrendState => text(self.regime.trend),
            F::RegimeVolatilityState => text(self.regime.volatility),
            F::RegimeStructureState => text(self.regime.structure),
            F::RegimeTransitionState => text(self.regime.transition),
            F::RegimeQualityState => text(self.regime.quality?),
            F::RegimeDirectionalBias => text(self.regime.bias),
            F::RegimeComposite => Value::Text(Cow::Owned(format!(
                "{}|{}|{}|{}|{}",
                self.regime.trend,
                self.regime.volatility,
                self.regime.structure,
                self.regime.transition,
                self.regime.quality?
            ))),
            F::IsRegimeClean => Bool(self.regime.quality? == "clean"),
            F::IsRegimeTrending => Bool(matches!(self.regime.trend, "trend_up" | "trend_down")),
            F::IsRegimeRanging => Bool(self.regime.trend == "range"),
            F::IsRegimeTransition => Bool(self.regime.transition != "none"),
            F::UtcDayOfWeek => text(day_of_week(candle.close_time_micros)),
            F::UtcHour => Int(hour_of_day(candle.close_time_micros)),
            F::UtcSession6h => {
                let start = hour_of_day(candle.close_time_micros) / 6 * 6;
                Value::Text(Cow::Owned(format!("{start:02}-{:02}", start + 6)))
            }
        })
    }
}

/// The largest inter-arrival time entering or inside the candle.
fn max_gap(candle: &Candle) -> i64 {
    candle
        .gap_before_micros
        .unwrap_or(0)
        .max(0)
        .max(candle.max_gap_inside_micros)
}

/// The worst gap class over the entering gap (when a previous record exists) and every
/// inter-arrival inside the candle, starting from the reference's `normal_small_tick_delay`.
fn worst_gap_class(candle: &Candle) -> &'static str {
    let order = |class: &str| {
        GAP_CLASSES
            .iter()
            .position(|(_, name)| *name == class)
            .unwrap_or(0)
            + 1
    };
    let mut worst = "normal_small_tick_delay";
    for class in candle
        .gap_before_micros
        .map(gap_class)
        .into_iter()
        .chain((candle.observations > 1).then(|| gap_class(candle.max_gap_inside_micros)))
    {
        if order(class) > order(worst) {
            worst = class;
        }
    }
    worst
}

fn hour_of_day(micros: i64) -> i64 {
    micros.div_euclid(MICROS_PER_SECOND).rem_euclid(86_400) / 3_600
}

fn day_of_week(micros: i64) -> &'static str {
    // 1970-01-01 was a Thursday.
    const NAMES: [&str; 7] = [
        "Thursday",
        "Friday",
        "Saturday",
        "Sunday",
        "Monday",
        "Tuesday",
        "Wednesday",
    ];
    NAMES[micros.div_euclid(MICROS_PER_SECOND * 86_400).rem_euclid(7) as usize]
}

/// One accepted candle's selected outputs, aligned with the stream plan's output list, with
/// the candle's logical decision clock (its close) and its actual availability clock.
#[derive(Debug, Clone, PartialEq)]
pub struct FeatureRow {
    pub close_time_micros: i64,
    pub known_at_micros: i64,
    pub values: Vec<Option<Value>>,
}

/// Everything one `push` produced, tagged by plan stream index.
#[derive(Debug, Default, Clone, PartialEq)]
pub struct FeatureOutput {
    pub rows: Vec<(usize, FeatureRow)>,
    pub structure_events: Vec<(usize, StructureEvent)>,
    pub sequence_events: Vec<(usize, SequenceEvent)>,
}

impl FeatureOutput {
    pub fn is_empty(&self) -> bool {
        self.rows.is_empty() && self.structure_events.is_empty() && self.sequence_events.is_empty()
    }

    pub fn clear(&mut self) {
        self.rows.clear();
        self.structure_events.clear();
        self.sequence_events.clear();
    }
}

/// The ordered state of one planned stream.
struct StreamState {
    /// The index of this stream in the bound definition's candle list.
    definition_index: usize,
    duration_micros: i64,
    offset_micros: i64,
    fields: Vec<Field>,
    tick_path: Option<TickPath>,
    /// Whether any selected output reads the path summary, and whether any reads the jump
    /// magnitudes; ticks are folded only when one of them does.
    tick_path_enabled: bool,
    jumps_needed: bool,
    ordinal: u64,
    accepted: u64,
    rolling: Option<Rolling>,
    statistics: Option<Statistics>,
    structure: Option<Structure>,
    sequence: Option<Sequence>,
    shape: Shape,
}

/// The feature engine: the bound instrument stream plus every planned stream's feature state.
/// Historical batch, replay, and live feeds call the same `push`; only accepted, finalized
/// candles enter the feature chain, and a tick path is folded from ordered ticks before its
/// candle finalizes.
pub struct FeatureEngine {
    session: Option<crate::session::Calendar>,
    stream: InstrumentStream,
    states: Vec<StreamState>,
    unit: f64,
    ticks: bool,
    gap: Option<(i64, i64)>,
    previous_tick: Option<TickSeen>,
    finalized: Vec<(usize, Candle)>,
}

/// Which calculations the selected outputs of one stream need; unselected stages hold no state.
#[derive(Debug, Clone, Copy, Default)]
struct Needs {
    tick_path: bool,
    jumps: bool,
    rolling: bool,
    structure: bool,
    sequence: bool,
    relative: bool,
}

impl Needs {
    fn of(fields: &[Field], stages: impl Fn(Field) -> Stage) -> Self {
        use Field as F;
        let mut needs = Self::default();
        for &field in fields {
            match stages(field) {
                Stage::TickPath => needs.tick_path = true,
                Stage::Rolling => needs.rolling = true,
                Stage::Structure => needs.structure = true,
                Stage::Sequence => needs.sequence = true,
                _ => {}
            }
            match field {
                F::MaxAbsTickJumpBps
                | F::MaxTrueTickJumpBps
                | F::MaxGapReopenJumpBps
                | F::RegimeQualityState
                | F::IsRegimeClean => needs.jumps = true,
                // A regime component reads only the state its rule reads.
                F::RegimeComposite => {
                    needs.jumps = true;
                    needs.sequence = true;
                }
                F::RegimeVolatilityState => needs.rolling = true,
                F::RegimeTransitionState | F::IsRegimeTransition => needs.structure = true,
                F::RegimeTrendState
                | F::RegimeStructureState
                | F::RegimeDirectionalBias
                | F::IsRegimeTrending
                | F::IsRegimeRanging => needs.sequence = true,
                F::PriorCleanHistoryCount
                | F::RangeVsRecentRatio
                | F::BodyVsRecentRatio
                | F::TickVolumeVsRecentRatio
                | F::RangeVsRecentBucket
                | F::BodyVsRecentBucket
                | F::TickVolumeVsRecentBucket
                | F::IsExpansionCandle
                | F::IsCompressionCandle => needs.relative = true,
                _ => {}
            }
        }
        // Later stages read earlier ones.
        needs.structure |= needs.sequence;
        needs.rolling |= needs.structure;
        needs
    }
}

impl FeatureEngine {
    /// Binds a plan to its definition and a source generation through the Phase 03 owner.
    pub fn new(plan: &FeaturePlan, source: Source) -> Result<Self, String> {
        let definition = &plan.profile.definition;
        let stream = InstrumentStream::new(definition, source)?;
        let ticks = plan.profile.ticks;
        let settings = &plan.settings;
        let mut states = Vec::with_capacity(plan.streams.len());
        for stream_plan in &plan.streams {
            let key = stream_plan.key();
            let definition_index = definition
                .candles
                .iter()
                .position(|spec| {
                    spec.duration_seconds == key.duration_seconds
                        && spec.offset_seconds == key.offset_seconds
                })
                .ok_or_else(|| format!("plan stream {key} is not a stream of the definition"))?;
            let spec = &definition.candles[definition_index];
            let table = catalog(settings);
            let defs = stream_plan
                .outputs
                .iter()
                .map(|output| {
                    table
                        .iter()
                        .find(|def| def.name == output.name)
                        .ok_or_else(|| format!("plan output `{}` is not compiled", output.name))
                })
                .collect::<Result<Vec<_>, _>>()?;
            let fields: Vec<Field> = defs.iter().map(|def| def.field).collect();
            let needs = Needs::of(&fields, |field| {
                table
                    .iter()
                    .find(|def| def.field == field)
                    .map_or(Stage::Candle, |def| def.stage)
            });
            let periods: Vec<u32> = settings
                .moving_average_periods
                .iter()
                .copied()
                .filter(|period| {
                    fields.iter().any(|field| match field {
                        Field::Ema(p)
                        | Field::EmaReady(p)
                        | Field::CloseVsEmaBps(p)
                        | Field::CloseAboveEma(p)
                        | Field::EmaSlopeBps(p)
                        | Field::EmaSlopeState(p)
                        | Field::CloseVsEmaState(p) => p == period,
                        Field::Ema20MinusEma50Bps
                        | Field::Ema20AboveEma50
                        | Field::Ema20Ema50AlignmentState => matches!(period, 20 | 50),
                        _ => false,
                    })
                })
                .collect();
            let statistics = fields.iter().any(|field| statistical(*field)).then(|| {
                Statistics::new(
                    settings
                        .structure
                        .as_ref()
                        .map_or(Vec::new(), |s| s.rolling_windows.clone()),
                )
            });
            states.push(StreamState {
                definition_index,
                duration_micros: i64::from(spec.duration_seconds) * MICROS_PER_SECOND,
                offset_micros: i64::from(spec.offset_seconds) * MICROS_PER_SECOND,
                fields,
                statistics,
                tick_path: None,
                tick_path_enabled: stream_plan.tick_path && ticks && needs.tick_path,
                jumps_needed: ticks && needs.jumps,
                ordinal: 0,
                accepted: 0,
                rolling: settings
                    .structure
                    .as_ref()
                    .filter(|_| needs.rolling)
                    .map(Rolling::new),
                structure: settings
                    .structure
                    .as_ref()
                    .filter(|_| needs.structure)
                    .map(Structure::new),
                sequence: settings
                    .structure
                    .as_ref()
                    .filter(|_| needs.sequence)
                    .and(settings.price_epsilon_units)
                    .map(Sequence::new),
                shape: Shape::new(
                    settings
                        .rolling_window
                        .zip(settings.min_history)
                        .filter(|_| needs.relative),
                    &periods,
                ),
            });
        }
        let seconds = |value: u32| i64::from(value) * MICROS_PER_SECOND;
        Ok(Self {
            session: definition
                .session
                .as_ref()
                .map(crate::session::Session::calendar)
                .transpose()?,
            stream,
            states,
            unit: definition.price_scale.unit() as f64,
            ticks,
            gap: definition
                .gap
                .as_ref()
                .map(|gap| (seconds(gap.max_seconds), seconds(gap.reopen_seconds))),
            previous_tick: None,
            finalized: Vec::new(),
        })
    }

    /// Accepts the next record through the instrument stream, then folds every candle it
    /// finalized into the feature chain and, for a tick, the tick into its interval's path.
    pub fn push(
        &mut self,
        observation: Observation,
        out: &mut FeatureOutput,
    ) -> Result<(), Rejection> {
        self.finalized.clear();
        self.stream.push(observation, &mut self.finalized)?;
        for (definition_index, candle) in self.finalized.drain(..) {
            // Session-excluded feed bars remain in the profile, never feature evidence.
            let included = if let Some(calendar) = &self.session {
                calendar
                    .contains(candle.open_time_micros, candle.close_time_micros)
                    .map_err(|detail| Rejection {
                        reason: crate::stream::RejectionReason::SessionCalendar,
                        event_micros: candle.open_time_micros,
                        known_at_micros: candle.known_at_micros,
                        source: self.stream.profile().source.generation,
                        detail,
                    })?
            } else {
                true
            };
            if let Some(index) = self
                .states
                .iter()
                .position(|state| state.definition_index == definition_index)
            {
                let state = &mut self.states[index];
                state.accept(&candle, included, self.unit, self.ticks, index, out);
            }
        }
        if let Observation::Tick(tick) = observation
            && self.ticks
        {
            let seen = TickSeen {
                event: tick.event_time_micros,
                price: tick.price_units as f64 / self.unit,
                units: tick.price_units,
            };
            for state in &mut self.states {
                if state.tick_path_enabled || state.jumps_needed {
                    state.fold_tick(self.previous_tick, seen, self.gap);
                }
            }
            self.previous_tick = Some(seen);
        }
        Ok(())
    }

    /// The Phase 03 profile of everything accepted so far.
    pub fn profile(&self) -> InstrumentProfile {
        self.stream.profile()
    }
}

impl StreamState {
    fn fold_tick(&mut self, previous: Option<TickSeen>, tick: TickSeen, gap: Option<(i64, i64)>) {
        let open = interval_open(tick.event, self.duration_micros, self.offset_micros);
        let path = match &mut self.tick_path {
            Some(path) if path.open_time == open => path,
            slot => slot.insert(TickPath::new(open, open + self.duration_micros)),
        };
        path.fold(previous, tick, gap);
    }

    /// Folds one finalized candle: consumes its tick path, applies strict eligibility, and, for
    /// an accepted candle, advances every stage in chain order and emits the row and events.
    fn accept(
        &mut self,
        candle: &Candle,
        included: bool,
        unit: f64,
        ticks: bool,
        stream_index: usize,
        out: &mut FeatureOutput,
    ) {
        self.ordinal += 1;
        let path = match self.tick_path.take() {
            Some(path) if path.open_time == candle.open_time_micros => Some(path),
            other => {
                self.tick_path = other;
                None
            }
        };
        if !included || !candle.flags.clean() {
            return;
        }
        let anatomy = Anatomy::new(candle, unit);
        let jumps = path.as_ref().map(|path| JumpMagnitudes {
            abs: path.max_abs_bps,
            true_: path.max_true_bps,
            reopen: path.max_reopen_bps,
        });
        let tick_path = path
            .filter(|_| self.tick_path_enabled)
            .map(|path| path.summary(anatomy.open, anatomy.high, anatomy.low, anatomy.close));
        let quality = ticks.then(|| {
            let flags: Flags = candle.flags;
            Quality {
                complete: flags.complete(),
                low_volume: flags.low_activity,
                hard_low_volume: flags.hard_low_activity,
                has_gap: flags.gap_before || flags.gap_inside || flags.missing_before,
                has_internal_gap: flags.gap_inside,
                starts_after_gap: flags.gap_before,
                frozen: flags.frozen,
                true_jump: flags.jump,
                delay_jump: flags.delayed_jump,
                reopen_jump: flags.reopen_jump,
                max_abs_bps: jumps.map_or(0.0, |jumps| six(jumps.abs)),
            }
        });
        let tick_volume = ticks.then_some(candle.observations);
        let swing_candle = SwingCandle {
            row: self.accepted,
            ordinal: self.ordinal,
            close_time: candle.close_time_micros,
            known_at: candle.known_at_micros,
            high_units: candle.high_units,
            low_units: candle.low_units,
        };
        let rolling = self
            .rolling
            .as_mut()
            .map(|rolling| rolling.update(&anatomy, tick_volume));
        let mut structure_events = Vec::new();
        let structure = match (&mut self.structure, &rolling) {
            (Some(structure), Some(rolling)) => {
                Some(structure.update(swing_candle, &anatomy, rolling, &mut structure_events))
            }
            _ => None,
        };
        let mut sequence_events = Vec::new();
        let sequence = match (&mut self.sequence, &structure) {
            (Some(sequence), Some(structure)) => {
                Some(sequence.update(swing_candle, structure, &mut sequence_events))
            }
            _ => None,
        };
        let shape = self.shape.update(swing_candle, &anatomy, tick_volume);
        let statistics = self.statistics.as_mut().map(|statistics| {
            statistics.update(
                candle,
                shape.adjacent,
                shape.doji,
                rolling.as_ref().and_then(|row| row.return_1_unrounded),
            )
        });
        let regime = regime(
            rolling.as_ref(),
            structure.as_ref(),
            sequence.as_ref(),
            quality,
        );
        let scratch = Scratch {
            candle,
            ordinal: self.ordinal,
            quality,
            tick_path,
            jumps,
            anatomy,
            rolling,
            statistics,
            structure,
            sequence,
            shape,
            regime,
        };
        out.rows.push((
            stream_index,
            FeatureRow {
                close_time_micros: candle.close_time_micros,
                known_at_micros: candle.known_at_micros,
                values: self
                    .fields
                    .iter()
                    .map(|field| scratch.value(*field))
                    .collect(),
            },
        ));
        out.structure_events.extend(
            structure_events
                .into_iter()
                .map(|event| (stream_index, event)),
        );
        out.sequence_events.extend(
            sequence_events
                .into_iter()
                .map(|event| (stream_index, event)),
        );
        self.accepted += 1;
    }
}

// ---------------------------------------------------------------------------------------------
// Encoding
// ---------------------------------------------------------------------------------------------

/// Labels the reference never assigns a code: empty text, missing, and the sentinel spellings of
/// missing values.
const UNCODED_LABELS: [&str; 6] = ["", "missing", "none", "<NA>", "nan", "NaT"];

/// Whether a label can receive a fitted code and therefore a generated condition.
pub fn coded_label(label: &str) -> bool {
    !UNCODED_LABELS.contains(&label)
}

/// The reference's general number format: six significant digits, trailing zeros removed, and
/// an exponent below `1e-4` or at `1e6` and above.
pub fn format_general(value: f64) -> String {
    if value.is_nan() {
        return "nan".to_string();
    }
    if value.is_infinite() {
        return if value > 0.0 { "inf" } else { "-inf" }.to_string();
    }
    if value == 0.0 {
        return if value.is_sign_negative() { "-0" } else { "0" }.to_string();
    }
    let scientific = format!("{value:.5e}");
    let (mantissa, exponent) = scientific
        .split_once('e')
        .expect("scientific notation carries an exponent");
    let exponent: i32 = exponent.parse().expect("an integer exponent");
    if (-4..6).contains(&exponent) {
        let decimals = (5 - exponent).max(0) as usize;
        let fixed = format!("{value:.decimals$}");
        return trim_fraction(&fixed).to_string();
    }
    format!(
        "{}e{}{:02}",
        trim_fraction(mantissa),
        if exponent < 0 { '-' } else { '+' },
        exponent.abs()
    )
}

fn trim_fraction(text: &str) -> &str {
    if text.contains('.') {
        text.trim_end_matches('0').trim_end_matches('.')
    } else {
        text
    }
}

fn edge_label(left: f64, right: f64) -> String {
    format!("{}_to_{}", format_general(left), format_general(right))
}

/// The right-closed bin of `value` over `edges`, with the first left edge included; `None`
/// outside the edges.
fn bucket(value: f64, edges: &[f64]) -> Option<usize> {
    if value.is_nan() || edges.len() < 2 || value < edges[0] || value > edges[edges.len() - 1] {
        return None;
    }
    let position = edges.partition_point(|edge| *edge < value);
    Some(position.saturating_sub(1))
}

/// The development fifths: linear-interpolated quantiles at 0.2, 0.4, 0.6, and 0.8 in the
/// reference's computation order, with duplicate cuts removed; `None` below four distinct values.
pub fn development_fifths(values: &mut Vec<f64>) -> Option<Vec<f64>> {
    values.retain(|value| !value.is_nan());
    values.sort_by(f64::total_cmp);
    let distinct = values.windows(2).filter(|pair| pair[0] != pair[1]).count()
        + usize::from(!values.is_empty());
    if distinct < 4 {
        return None;
    }
    let n = values.len() as f64;
    let mut edges: Vec<f64> = Vec::with_capacity(4);
    for q in [0.2, 0.4, 0.6, 0.8] {
        let virtual_index = (n - 1.0) * q;
        let below = virtual_index.floor();
        let t = virtual_index - below;
        let a = values[below as usize];
        let b = values[(below as usize + 1).min(values.len() - 1)];
        let difference = b - a;
        let edge = if t >= 0.5 {
            b - difference * (1.0 - t)
        } else {
            a + difference * t
        };
        if !edges.contains(&edge) {
            edges.push(edge);
        }
    }
    Some(edges)
}

impl FittedEncoding {
    /// The bin edges including the unbounded tails of a development fit.
    fn full_edges(&self) -> Option<Vec<f64>> {
        let edges = self.edges.as_ref()?;
        Some(match self.encoding {
            ProjectionKind::DevelopmentFifths => {
                let mut full = Vec::with_capacity(edges.len() + 2);
                full.push(f64::NEG_INFINITY);
                full.extend(edges);
                full.push(f64::INFINITY);
                full
            }
            _ => edges.clone(),
        })
    }

    /// The label of one development-fifths interval by its zero-based low-to-high ordinal, `0`
    /// to `4`: the right-closed bin between the fitted cuts with the unbounded tails, labelled
    /// exactly as the fit labelled it. Requires a development-fifths encoding fitted with four
    /// distinct cuts and a label retained under the label limit; the reason names anything
    /// else. Frequency-ranked label codes are never interval ordinals.
    pub fn interval_label(&self, ordinal: u8) -> Result<String, String> {
        if self.encoding != ProjectionKind::DevelopmentFifths {
            return Err(format!(
                "encoding `{}` is `{}`, not `development_fifths`",
                self.output, self.encoding
            ));
        }
        if ordinal > 4 {
            return Err(format!("interval ordinal {ordinal} is not within 0 to 4"));
        }
        let cuts = self.edges.as_ref().map_or(0, Vec::len);
        if cuts != 4 {
            return Err(format!(
                "encoding `{}` fitted {cuts} distinct cuts, not four",
                self.output
            ));
        }
        let edges = self.full_edges().expect("four cuts");
        let label = edge_label(edges[usize::from(ordinal)], edges[usize::from(ordinal) + 1]);
        if !self.labels.contains(&label) {
            return Err(format!(
                "encoding `{}` did not retain interval {ordinal} `{label}` under its label limit",
                self.output
            ));
        }
        Ok(label)
    }

    /// The label of one value under this encoding, or `None` for a missing value, an
    /// out-of-range number, or an unfitted development quantile.
    pub fn label(&self, value: Option<&Value>) -> Option<Cow<'_, str>> {
        let value = value?;
        match self.encoding {
            ProjectionKind::Category => value.as_label().map(|label| {
                if label.is_empty() {
                    Cow::Borrowed("none")
                } else {
                    Cow::Owned(label.into_owned())
                }
            }),
            ProjectionKind::Fixed | ProjectionKind::DevelopmentFifths => {
                let edges = self.full_edges()?;
                let index = bucket(value.as_f64()? / self.input_divisor, &edges)?;
                Some(Cow::Owned(edge_label(edges[index], edges[index + 1])))
            }
        }
    }

    /// Fits the encoding on the development column: development fifths for a quantile
    /// encoding, then the labels ranked by count descending and text ascending, limited to
    /// `max_labels`. Duplicate formatted bin labels are an error, never merged.
    pub fn fit(&mut self, column: &[Option<Value>], max_labels: u32) -> Result<(), String> {
        if self.encoding == ProjectionKind::DevelopmentFifths {
            let mut values: Vec<f64> = column
                .iter()
                .filter_map(|value| value.as_ref()?.as_f64())
                .map(|value| value / self.input_divisor)
                .collect();
            self.edges = development_fifths(&mut values);
        }
        if let Some(edges) = self.full_edges() {
            let labels: Vec<String> = edges
                .windows(2)
                .map(|pair| edge_label(pair[0], pair[1]))
                .collect();
            if let Some(duplicate) = labels
                .iter()
                .enumerate()
                .find(|(index, label)| labels[..*index].contains(label))
            {
                if self.automatic && self.encoding == ProjectionKind::DevelopmentFifths {
                    self.edges = None;
                    self.labels.clear();
                    return Ok(());
                }
                return Err(format!(
                    "encoding `{}`: bin label `{}` is not unique at six significant digits",
                    self.output, duplicate.1
                ));
            }
        }
        let mut counts: HashMap<String, u64> = HashMap::new();
        for value in column {
            if let Some(label) = self.label(value.as_ref()) {
                *counts.entry(label.into_owned()).or_insert(0) += 1;
            }
        }
        let mut ranked: Vec<(u64, String)> = counts
            .into_iter()
            .filter(|(label, count)| *count > 0 && coded_label(label))
            .map(|(label, count)| (count, label))
            .collect();
        ranked.sort_by(|a, b| b.0.cmp(&a.0).then_with(|| a.1.cmp(&b.1)));
        self.labels = ranked
            .into_iter()
            .take(max_labels.max(1) as usize)
            .map(|(_, label)| label)
            .collect();
        Ok(())
    }

    /// Encodes a column under the frozen labels: the zero-based code of the value's label, or
    /// `-1` for a missing, unseen, or uncoded label.
    pub fn encode(&self, column: &[Option<Value>]) -> Vec<i16> {
        let codes: HashMap<&str, i16> = self
            .labels
            .iter()
            .enumerate()
            .map(|(code, label)| (label.as_str(), code as i16))
            .collect();
        column
            .iter()
            .map(|value| {
                self.label(value.as_ref())
                    .and_then(|label| codes.get(label.as_ref()).copied())
                    .unwrap_or(-1)
            })
            .collect()
    }
}

// ---------------------------------------------------------------------------------------------
// Feature generations
// ---------------------------------------------------------------------------------------------

/// Summary of one stream's published rows and events.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct FeatureStreamSummary {
    pub duration_seconds: u32,
    pub offset_seconds: u32,
    pub rows: u64,
    pub structure_events: u64,
    pub sequence_events: u64,
    pub first_decision_time: Option<String>,
    pub last_decision_time: Option<String>,
}

/// The ready manifest of one feature generation. Field order is the serialization order.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct FeatureManifest {
    pub kind: String,
    pub schema_version: u32,
    pub generation: String,
    pub broker: BrokerId,
    pub provider_symbol: ProviderSymbol,
    pub instrument: String,
    /// The role of the input generation the plan was applied to.
    pub role: DatasetRole,
    pub input_generation: String,
    pub plan_identity: String,
    /// The feature generation the plan was frozen by, when this generation applies a frozen
    /// plan rather than fitting one.
    pub frozen_from: Option<String>,
    pub profile_generation: String,
    pub config_hash: String,
    pub code_revision: String,
    pub observations: u64,
    pub streams: Vec<FeatureStreamSummary>,
    pub objects: Vec<ObjectRecord>,
}

impl FeatureManifest {
    pub fn to_json(&self) -> Vec<u8> {
        let mut bytes = serde_json::to_vec_pretty(self).expect("a manifest serializes");
        bytes.push(b'\n');
        bytes
    }

    /// Parses a feature manifest and checks what every consumer relies on before it trusts a
    /// key: the kind, a generation that matches the plan and input, a plan object, the four
    /// objects of every stream, and content-addressed objects with unique clean paths.
    pub fn from_json(bytes: &[u8]) -> Result<Self, String> {
        let manifest: Self = serde_json::from_slice(bytes).map_err(|error| error.to_string())?;
        if manifest.kind != FEATURE_MANIFEST_KIND {
            return Err(format!(
                "manifest kind `{}` is not `{FEATURE_MANIFEST_KIND}`",
                manifest.kind
            ));
        }
        if manifest.schema_version != FEATURE_SCHEMA_VERSION {
            return Err(format!(
                "unsupported manifest schema_version {}, expected {FEATURE_SCHEMA_VERSION}",
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
            != feature_generation_id(&manifest.plan_identity, &manifest.input_generation)
        {
            return Err(format!(
                "generation `{}` does not match the plan identity and input generation",
                manifest.generation
            ));
        }
        validate_objects(&manifest.objects)?;
        if manifest
            .objects
            .iter()
            .filter(|object| object.path == PLAN_OBJECT_PATH)
            .count()
            != 1
        {
            return Err(format!("expected one `{PLAN_OBJECT_PATH}` object"));
        }
        let mut allowed = vec![PLAN_OBJECT_PATH.to_string()];
        for summary in &manifest.streams {
            let paths = stream_object_paths(summary.duration_seconds, summary.offset_seconds);
            for path in &paths[..3] {
                if !manifest.objects.iter().any(|object| object.path == *path) {
                    return Err(format!("expected a `{path}` object"));
                }
            }
            allowed.extend(paths);
        }
        if let Some(object) = manifest
            .objects
            .iter()
            .find(|object| !allowed.contains(&object.path))
        {
            return Err(format!(
                "object `{}` is not part of a feature generation",
                object.path
            ));
        }
        if manifest
            .objects
            .iter()
            .any(|object| object.role != ObjectRole::Normalized)
        {
            return Err("every feature object is normalized output".to_string());
        }
        Ok(manifest)
    }

    pub fn key(&self) -> String {
        manifest_key(&self.generation)
    }
}

/// Reads the profile reference out of a stream manifest and its profile object.
pub fn profile_reference(
    manifest: &StreamManifest,
    profile: &InstrumentProfile,
    profile_sha256: &str,
) -> ProfileReference {
    ProfileReference {
        stream_generation: manifest.generation.clone(),
        profile_sha256: profile_sha256.to_string(),
        source_generation: manifest.source_generation.clone(),
        role: manifest.role,
        definition: manifest.definition.clone(),
        ticks: profile.calculations.iter().all(|support| support.supported),
    }
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Int(value) | Self::Time(value) => write!(f, "{value}"),
            Self::Float(value) => write!(f, "{value:?}"),
            Self::Bool(value) => write!(f, "{value}"),
            Self::Text(text) => f.write_str(text),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{CandleSpec, FrozenCheck, GapCheck, JumpCheck, SpanCheck};
    use crate::dataset::{Capability, NativeGranularity, SourceKind};
    use crate::market::{Currency, Tick};

    fn scale(digits: u8) -> PriceScale {
        PriceScale::try_from(digits).unwrap()
    }

    fn definition(granularity: NativeGranularity, candles: &[(u32, u32)]) -> Instrument {
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
                min_observations: 10,
                min_seconds: 5,
            }),
            jump: Some(JumpCheck {
                min_basis_points: 5,
            }),
            span: Some(SpanCheck { min_percent: 75 }),
            sessions: None,
            session: None,
            candles: candles
                .iter()
                .map(|&(duration_seconds, offset_seconds)| CandleSpec {
                    duration_seconds,
                    offset_seconds,
                    min_observations: Some(2),
                    hard_min_observations: Some(1),
                })
                .collect(),
        }
    }

    fn source(granularity: NativeGranularity, ticks: bool) -> Source {
        Source {
            generation: "input".to_string(),
            source_kind: if ticks {
                SourceKind::TickCsv
            } else {
                SourceKind::BarParquet
            },
            role: DatasetRole::Development,
            native_granularity: granularity,
            price_scale: ticks.then(|| scale(6)),
            capabilities: vec![if ticks {
                Capability::Ticks
            } else {
                Capability::Bars
            }],
        }
    }

    fn profile(
        granularity: NativeGranularity,
        ticks: bool,
        candles: &[(u32, u32)],
    ) -> ProfileReference {
        ProfileReference {
            stream_generation: "stream".to_string(),
            profile_sha256: "0".repeat(64),
            source_generation: "input".to_string(),
            role: DatasetRole::Development,
            definition: definition(granularity, candles),
            ticks,
        }
    }

    fn structure() -> StructureSettings {
        StructureSettings {
            swing_left: 3,
            swing_right: 3,
            rolling_windows: vec![5, 10, 20],
            direction_window: 10,
            trend_efficiency_threshold: 0.35,
            trend_min_abs_momentum_bps: 3.0,
            range_efficiency_threshold: 0.25,
            compression_ratio_threshold: 0.7,
            expanded_ratio_threshold: 1.3,
            extreme_ratio_threshold: 1.8,
            pullback_min_trend_age: 3,
            trend_reset_sideways_bars: 3,
            failed_breakout_max_bars: 5,
        }
    }

    fn entry(streams: &[(u32, u32)], outputs: Outputs) -> FeatureInstrument {
        let keys: Vec<StreamKey> = streams
            .iter()
            .map(|&(duration_seconds, offset_seconds)| StreamKey {
                duration_seconds,
                offset_seconds,
            })
            .collect();
        FeatureInstrument {
            role: DatasetRole::Development,
            input_manifest: "file:///p/manifests/1111111111111111111111111111111111111111111111111111111111111111/ready.json".parse().unwrap(),
            profile_manifest: "file:///p/manifests/2222222222222222222222222222222222222222222222222222222222222222/ready.json".parse().unwrap(),
            frozen_plan: None,
            streams: Some(keys.clone()),
            outputs: Some(outputs),
            moving_average_periods: Some(vec![20, 50]),
            rolling_window: Some(100),
            min_history: Some(20),
            structure: Some(structure()),
            price_epsilon: Some("0".to_string()),
            tick_path_streams: Some(keys),
            encodings: None,
        }
    }

    #[test]
    fn all_supported_fitted_numeric_and_boolean_labels_generate_conditions() {
        use crate::config::{EncodingSpec, Encodings, GeneratedSearchCondition, SearchCondition};
        let mut request = entry(&[(5, 0)], Outputs::AllSupported);
        request.encodings = Some(Encodings {
            max_labels: 8,
            outputs: vec![EncodingSpec {
                output: "all_supported".into(),
                bins: None,
            }],
        });
        let mut plan = FeaturePlan::resolve(
            &request,
            profile(NativeGranularity::Tick, true, &[(5, 0)]),
            "input",
        )
        .unwrap();
        let stream = plan.streams[0].key();
        let numeric = plan.streams[0]
            .encodings
            .iter_mut()
            .find(|encoding| encoding.input == "range_bps")
            .unwrap();
        assert!(numeric.automatic && numeric.output != numeric.input);
        numeric
            .fit(&numbers(&(0..100).map(f64::from).collect::<Vec<_>>()), 8)
            .unwrap();
        let numeric_output = numeric.output.clone();
        let boolean_input = plan.streams[0]
            .outputs
            .iter()
            .find(|output| output.predictive && output.kind == Kind::Bool)
            .unwrap()
            .name
            .clone();
        let boolean = plan.streams[0]
            .encodings
            .iter_mut()
            .find(|encoding| encoding.input == boolean_input)
            .unwrap();
        assert!(boolean.automatic && boolean.output != boolean.input);
        boolean
            .fit(&[Some(Value::Bool(true)), Some(Value::Bool(false))], 8)
            .unwrap();
        let boolean_output = boolean.output.clone();
        let family: crate::search::Family = serde_json::from_slice(include_bytes!("../../app/tests/fixtures/legacy_schema1/published/objects/24e476f4f6bb8abbd2211c19ba7b0659762b682b299cb240ec6d40a694d51327")).unwrap();
        let mut search = family.search;
        search.base_stream = stream;
        search.conditions = vec![SearchCondition::Generate(GeneratedSearchCondition {
            stream,
            output: "*".into(),
            comparator: crate::execution::Comparator::Eq,
        })];
        search.max_conditions = 2;
        search.max_candidates = 1000;
        let resolved = crate::search::resolve_conditions(&search, &plan).unwrap();
        assert_eq!(resolved.conditions.len(), 7);
        assert_eq!(
            resolved
                .conditions
                .iter()
                .filter(|condition| condition.output == numeric_output)
                .count(),
            5
        );
        let boolean_labels: Vec<_> = resolved
            .conditions
            .iter()
            .filter(|condition| condition.output == boolean_output)
            .map(|condition| &condition.threshold)
            .collect();
        assert_eq!(
            boolean_labels,
            [
                &crate::execution::Threshold::Text("false".into()),
                &crate::execution::Threshold::Text("true".into())
            ]
        );
    }

    fn category(values: &[&str]) -> Vec<Option<Value>> {
        values
            .iter()
            .map(|value| Some(Value::Text(Cow::Owned((*value).to_string()))))
            .collect()
    }

    fn numbers(values: &[f64]) -> Vec<Option<Value>> {
        values
            .iter()
            .map(|value| Some(Value::Float(*value)))
            .collect()
    }

    fn encoding(kind: ProjectionKind, edges: Option<Vec<f64>>) -> FittedEncoding {
        FittedEncoding {
            output: "x".to_string(),
            input: "x".to_string(),
            automatic: false,
            encoding: kind,
            edges,
            input_divisor: 1.0,
            labels: Vec::new(),
        }
    }

    fn statistical_candle(open: i64, close: i64) -> Candle {
        Candle {
            open_time_micros: 0,
            close_time_micros: 1,
            known_at_micros: 1,
            first_event_micros: 0,
            last_event_micros: 1,
            active_span_micros: 1,
            open_units: open,
            high_units: open.max(close) + 10,
            low_units: open.min(close) - 10,
            close_units: close,
            observations: 1,
            duplicates: 0,
            volume: None,
            gap_before_micros: None,
            max_gap_inside_micros: 0,
            missing_buckets_before: 0,
            frozen_observations: 0,
            frozen_micros: 0,
            max_jump_basis_points: 0,
            max_delayed_jump_basis_points: 0,
            max_reopen_jump_basis_points: 0,
            flags: Flags::default(),
        }
    }

    #[test]
    fn rolling_statistics_pin_windows_formulas_and_degenerate_cases() {
        let mut statistics = Statistics::new(vec![2, 3, 4]);
        let first = statistics.update_test(&statistical_candle(100, 100), false, true);
        assert_eq!(first.pattern, None);
        assert_eq!(first.overlap, None);
        assert!(
            first
                .windows
                .iter()
                .all(|(_, row)| row.std.is_none() && row.position.is_none())
        );
        for (open, close) in [(100, 110), (110, 90), (90, 120), (120, 80)] {
            statistics.update_test(&statistical_candle(open, close), true, false);
        }
        let last = statistics.update_test(&statistical_candle(80, 130), true, false);
        let w4 = last.windows.iter().find(|(w, _)| *w == 4).unwrap().1;
        assert_eq!(w4.std, Some(3862.649811));
        assert_eq!(w4.skew, Some(0.148881));
        assert_eq!(w4.kurtosis, Some(-1.668039));
        assert_eq!(w4.autocorr, Some(-0.996566));
        assert_eq!(w4.reversal, Some(1.0));
        assert_eq!(w4.up_ratio, Some(0.650386));
        assert_eq!(w4.r2, Some(0.188235));
        assert_eq!(w4.residual, Some(1000.0));
        assert_eq!(w4.position, Some(0.857143));
        assert_eq!(last.overlap, Some(0.857143));
        let flat = statistical_candle(100, 100);
        let mut zero = Statistics::new(vec![2, 3, 4]);
        for _ in 0..5 {
            zero.update_test(&flat, true, true);
        }
        let row = zero.update_test(&flat, true, true).windows[2].1;
        assert_eq!(row.std, Some(0.0));
        assert_eq!(row.skew, None);
        assert_eq!(row.kurtosis, None);
        assert_eq!(row.autocorr, None);
        assert_eq!(row.reversal, None);
        assert_eq!(row.up_ratio, None);
        assert_eq!(row.r2, None);
        assert_eq!(row.residual, Some(0.0));
        let mut degenerate = statistical_candle(100, 100);
        degenerate.high_units = 100;
        degenerate.low_units = 100;
        assert_eq!(zero.update_test(&degenerate, true, true).overlap, None);
        let mut zero_range = Statistics::new(vec![2]);
        zero_range.update_test(&degenerate, false, true);
        assert_eq!(
            zero_range.update_test(&degenerate, true, true).windows[0]
                .1
                .position,
            None
        );
        let mut zero_close = statistical_candle(100, 0);
        zero_close.close_units = 0;
        let mut zero = Statistics::new(vec![3]);
        for _ in 0..2 {
            zero.update_test(&flat, true, true);
        }
        assert_eq!(
            zero.update_test(&zero_close, true, false).windows[0]
                .1
                .residual,
            None
        );
        let mut missing_return = Statistics::new(vec![2]);
        missing_return.update_test(&statistical_candle(0, 0), false, true);
        missing_return.update_test(&statistical_candle(0, 1), true, false);
        assert_eq!(
            missing_return
                .update_test(&statistical_candle(1, 2), true, false)
                .windows[0]
                .1
                .std,
            None
        );
        let mut one_pair = Statistics::new(vec![4]);
        for close in [100, 110, 120, 120, 110] {
            let row = one_pair.update_test(&statistical_candle(close, close), true, true);
            if close == 110 && one_pair.candles.len() == 5 {
                assert_eq!(row.windows[0].1.reversal, Some(0.0));
            }
        }
    }

    #[test]
    fn statistics_keep_unit_differences_above_f64_integer_precision() {
        let base = 1_i64 << 53;
        let mut position = Statistics::new(vec![2]);
        for close in [base, base + 1] {
            let mut candle = statistical_candle(close, close);
            candle.low_units = base;
            candle.high_units = base + 2;
            let row = position.update_test(&candle, true, false);
            if close == base + 1 {
                assert_eq!(row.windows[0].1.position, Some(0.5));
            }
        }

        let mut trend = Statistics::new(vec![3]);
        for close in [base, base + 1, base + 2] {
            let row = trend.update_test(&statistical_candle(close, close), true, false);
            if close == base + 2 {
                assert_eq!(row.windows[0].1.r2, Some(1.0));
                assert_eq!(row.windows[0].1.residual, Some(0.0));
            }
        }
        assert!(trend.returns.last().flatten().unwrap() > 0.0);
    }

    #[test]
    fn disjoint_adjacent_ranges_have_zero_overlap() {
        let mut statistics = Statistics::new(vec![]);
        let mut prior = statistical_candle(0, 1);
        prior.low_units = 0;
        prior.high_units = 1;
        statistics.update_test(&prior, false, false);
        let mut current = statistical_candle(2, 3);
        current.low_units = 2;
        current.high_units = 3;
        assert_eq!(
            statistics.update_test(&current, true, false).overlap,
            Some(0.0)
        );
    }

    #[test]
    fn candle_patterns_pin_equality_priority_and_doji() {
        let candle = |open, close| StatisticalCandle {
            open,
            close,
            high: open.max(close),
            low: open.min(close),
            doji: false,
        };
        assert_eq!(
            candle_pattern(candle(12, 10), candle(10, 12)),
            "bullish_engulfing"
        );
        assert_eq!(
            candle_pattern(candle(10, 12), candle(12, 10)),
            "bearish_engulfing"
        );
        assert_eq!(
            candle_pattern(candle(14, 10), candle(11, 13)),
            "bullish_harami"
        );
        assert_eq!(
            candle_pattern(candle(10, 14), candle(13, 11)),
            "bearish_harami"
        );
        assert_eq!(candle_pattern(candle(10, 10), candle(9, 12)), "none");
        let mut doji = candle(12, 10);
        doji.doji = true;
        assert_eq!(candle_pattern(doji, candle(10, 12)), "none");
        let mut doji = candle(10, 12);
        doji.doji = true;
        assert_eq!(candle_pattern(candle(12, 10), doji), "none");
    }

    #[test]
    fn candle_pattern_reuses_the_six_rounded_shape_doji_decision() {
        let base = 10_000_000;
        let mut prior = statistical_candle(base + 1_000_004, base);
        prior.high_units = base + 5_000_000;
        prior.low_units = base - 5_000_000;
        let current = statistical_candle(base, base + 1_000_004);
        let mut shape = Shape::new(None, &[]);
        let mut statistics = Statistics::new(vec![]);
        for (ordinal, candle) in [(1, &prior), (2, &current)] {
            let anatomy = Anatomy::new(candle, 1.0);
            let row = shape.update(
                SwingCandle {
                    row: ordinal,
                    ordinal,
                    close_time: ordinal as i64,
                    known_at: ordinal as i64,
                    high_units: candle.high_units,
                    low_units: candle.low_units,
                },
                &anatomy,
                None,
            );
            if ordinal == 1 {
                assert_eq!(anatomy.body_to_range, 0.1);
                assert!(row.doji);
            } else {
                assert!(row.adjacent);
                assert!(!row.doji);
                assert_eq!(
                    statistics
                        .update_test(candle, row.adjacent, row.doji)
                        .pattern,
                    Some("none")
                );
                break;
            }
            statistics.update_test(candle, row.adjacent, row.doji);
        }
    }

    #[test]
    fn statistics_catalog_starts_each_formula_at_its_minimum_window() {
        let plan = FeaturePlan::resolve(
            &entry(&[(5, 0)], Outputs::AllSupported),
            profile(
                NativeGranularity::Bar { period_seconds: 5 },
                false,
                &[(5, 0)],
            ),
            "input",
        )
        .unwrap();
        let mut settings = plan.settings;
        settings.structure.as_mut().unwrap().rolling_windows = vec![1, 2, 3, 4];
        let names: Vec<_> = catalog(&settings)
            .into_iter()
            .map(|output| output.name)
            .collect();
        for (stem, min) in [
            ("return_std_", 2),
            ("up_move_ratio_", 2),
            ("range_position_", 2),
            ("return_skew_", 3),
            ("trend_r2_", 3),
            ("trend_residual_", 3),
            ("return_kurtosis_", 4),
            ("return_autocorr_", 4),
            ("sign_reversal_rate_", 4),
        ] {
            for w in 1..=4 {
                assert_eq!(
                    names
                        .iter()
                        .any(|name| name.starts_with(stem) && name.contains(&format!("_{w}"))),
                    w >= min,
                    "{stem}{w}"
                );
            }
        }
    }

    /// A tick as the path folds it, with exact units at scale six.
    fn seen(event: i64, price: f64) -> TickSeen {
        TickSeen {
            event,
            price,
            units: (price * 1e6).round() as i64,
        }
    }

    #[test]
    fn category_encodings_rank_by_count_then_text_and_honor_the_limit() {
        let development = category(&["z", "a", "z", "a"]);
        let later = category(&["m"]);
        let mut fitted = encoding(ProjectionKind::Category, None);
        fitted.fit(&development, 32_768).unwrap();
        assert_eq!(fitted.labels, ["a", "z"]);
        let all: Vec<Option<Value>> = development.iter().chain(&later).cloned().collect();
        assert_eq!(fitted.encode(&all), [1, 0, 1, 0, -1]);
        let mut limited = encoding(ProjectionKind::Category, None);
        limited.fit(&development, 1).unwrap();
        assert_eq!(limited.labels, ["a"]);
        assert_eq!(limited.encode(&all), [-1, 0, -1, 0, -1]);
        // Empty text is the reference's `none` and never receives a code; booleans label as text.
        let mut mixed = encoding(ProjectionKind::Category, None);
        let values = vec![
            Some(Value::Text(Cow::Borrowed(""))),
            Some(Value::Bool(true)),
            Some(Value::Bool(false)),
            None,
            Some(Value::Text(Cow::Borrowed("none"))),
            Some(Value::Text(Cow::Borrowed("missing"))),
        ];
        mixed.fit(&values, 8).unwrap();
        assert_eq!(mixed.labels, ["false", "true"]);
        assert_eq!(mixed.encode(&values), [-1, 1, 0, -1, -1, -1]);
    }

    #[test]
    fn development_fifths_follow_the_pinned_computation_order() {
        assert_eq!(
            development_fifths(&mut vec![1.0, 2.0, 3.0, 4.0]).unwrap(),
            [1.6, 2.2, 2.8, 3.4000000000000004]
        );
        assert_eq!(
            development_fifths(&mut vec![5.0, 1.0, 3.0, 3.0, 7.0, 2.0, 9.0]).unwrap(),
            [2.2, 3.0, 4.199999999999999, 6.600000000000001]
        );
        assert_eq!(
            development_fifths(&mut vec![1.0, 1.0, 2.0, 3.0, 4.0, 4.0, 4.0]).unwrap(),
            [
                1.2000000000000002,
                2.4000000000000004,
                3.5999999999999996,
                4.0
            ]
        );
        assert_eq!(development_fifths(&mut vec![1.0, 2.0, 3.0, 3.0]), None);
        let mut fitted = encoding(ProjectionKind::DevelopmentFifths, None);
        fitted.fit(&numbers(&[1.0, 2.0, 3.0, 4.0]), 32_768).unwrap();
        assert_eq!(
            fitted.edges.as_deref().unwrap(),
            [1.6, 2.2, 2.8, 3.4000000000000004]
        );
        assert_eq!(
            fitted.labels,
            ["-inf_to_1.6", "1.6_to_2.2", "2.8_to_3.4", "3.4_to_inf"],
            "a bin no development value fell in has no label"
        );
        assert_eq!(
            fitted.encode(&numbers(&[1.0, 2.0, 3.0, 4.0, -1000.0])),
            [0, 1, 2, 3, 0]
        );
        assert_eq!(
            fitted.encode(&numbers(&[1.0, 2.0, 3.0, 4.0, 1000.0])),
            [0, 1, 2, 3, 3]
        );
        assert_eq!(
            fitted.encode(&numbers(&[2.5])),
            [-1],
            "an unseen bin has no code"
        );
        let mut frozen = fitted.clone();
        frozen
            .fit(&numbers(&[1.0, 2.0, 3.0, 4.0, -1000.0]), 32_768)
            .unwrap();
        assert_ne!(
            frozen.edges, fitted.edges,
            "fitting a different development sample is a different plan"
        );
        let mut sparse = encoding(ProjectionKind::DevelopmentFifths, None);
        sparse.fit(&numbers(&[1.0, 2.0, 3.0, 3.0]), 32_768).unwrap();
        assert_eq!(sparse.edges, None);
        assert!(sparse.labels.is_empty());
        assert_eq!(
            sparse.encode(&numbers(&[1.0, 2.0, 3.0, 3.0])),
            [-1, -1, -1, -1]
        );
        let mut duplicate = encoding(ProjectionKind::DevelopmentFifths, None);
        let values: Vec<f64> = (100_000_000..100_000_010)
            .map(|value| value as f64)
            .collect();
        let error = duplicate.fit(&numbers(&values), 32_768).unwrap_err();
        assert!(error.contains("1e+08_to_1e+08"), "{error}");
        duplicate.automatic = true;
        duplicate.fit(&numbers(&values), 32_768).unwrap();
        assert_eq!(duplicate.edges, None);
        assert!(duplicate.labels.is_empty());
        duplicate
            .fit(&numbers(&[1.0, 1.0, 2.0, 3.0]), 32_768)
            .unwrap();
        assert_eq!(duplicate.edges, None);
        assert!(duplicate.labels.is_empty());
        duplicate.fit(&[], 32_768).unwrap();
        assert_eq!(duplicate.edges, None);
        assert!(duplicate.labels.is_empty());
    }

    #[test]
    fn fixed_bins_are_right_closed_with_the_first_edge_included() {
        let mut fitted = encoding(ProjectionKind::Fixed, Some(vec![0.0, 1.0, 2.0]));
        let values = numbers(&[0.0, 1.0, 1.0000001, 2.0, 2.5, -0.5]);
        fitted.fit(&values, 32_768).unwrap();
        assert_eq!(fitted.labels, ["0_to_1", "1_to_2"]);
        assert_eq!(
            values
                .iter()
                .map(|value| fitted.label(value.as_ref()).map(Cow::into_owned))
                .collect::<Vec<_>>(),
            [
                Some("0_to_1".to_string()),
                Some("0_to_1".to_string()),
                Some("1_to_2".to_string()),
                Some("1_to_2".to_string()),
                None,
                None
            ]
        );
        assert_eq!(fitted.encode(&values), [0, 0, 1, 1, -1, -1]);
        assert_eq!(fitted.encode(&[None]), [-1]);
    }

    #[test]
    fn general_formatting_matches_the_reference() {
        for (value, text) in [
            (250_000.0, "250000"),
            (1e6, "1e+06"),
            (0.0001, "0.0001"),
            (0.00001, "1e-05"),
            (3.4000000000000004, "3.4"),
            (1_234_567.0, "1.23457e+06"),
            (-0.25, "-0.25"),
            (-0.0, "-0"),
            (f64::INFINITY, "inf"),
            (f64::NEG_INFINITY, "-inf"),
            (0.0, "0"),
            (100_000_001.8, "1e+08"),
            (12_345.678, "12345.7"),
            (0.5, "0.5"),
            (1.0, "1"),
            (13.0, "13"),
            (60_000_000.0, "6e+07"),
            (1_800_000_000.0, "1.8e+09"),
            (999_999.5, "1e+06"),
            (999_999.4, "999999"),
        ] {
            assert_eq!(format_general(value), text, "{value}");
        }
    }

    #[test]
    fn duration_projections_bucket_and_label_in_milliseconds() {
        let mut request = entry(&[(5, 0)], Outputs::AllSupported);
        request.encodings = Some(Encodings {
            max_labels: 32_768,
            outputs: vec![EncodingSpec {
                output: "max_gap_micros_bucketed".to_string(),
                bins: None,
            }],
        });
        let plan = FeaturePlan::resolve(
            &request,
            profile(NativeGranularity::Tick, true, &[(5, 0)]),
            "input",
        )
        .unwrap();
        let mut fitted = plan.streams[0].encodings[0].clone();
        assert_eq!(
            (fitted.input.as_str(), fitted.input_divisor),
            ("max_gap_micros", 1_000.0)
        );
        let values = numbers(&[0.0, 300_000.0, 750_000.0, 2_000_000.0]);
        fitted.fit(&values, 32_768).unwrap();
        assert_eq!(
            fitted.labels,
            ["-inf_to_0", "1000_to_2000", "250_to_500", "500_to_1000"]
        );
        assert_eq!(fitted.encode(&values), [0, 2, 3, 1]);
        let json = serde_json::to_string(&fitted).unwrap();
        assert!(json.contains("\"input_divisor\":1000.0"), "{json}");
        assert_eq!(
            serde_json::from_str::<FittedEncoding>(&json).unwrap(),
            fitted
        );
    }

    #[test]
    fn window_sums_are_compensated_like_the_reference() {
        // Ten consecutive five-second range values of the governed reference: the plain fold
        // rounds their mean to 1.804135, the reference's compensated sum to 1.804134.
        let window = [
            0.885_514, 2.767_17, 1.272_715, 2.656_425, 1.549_307, 2.379_582, 0.719_556, 1.715_807,
            1.881_79, 2.213_479,
        ];
        let plain = window.iter().fold(0.0, |total, value| total + value) / 10.0;
        assert_eq!(six(plain), 1.804_135);
        assert_eq!(six(sum(window.iter().copied()) / 10.0), 1.804_134);
        assert_eq!(sum(window.iter().copied()).to_bits(), 0x4032_0A95_95FE_DA66);
    }

    #[test]
    fn the_active_span_quantile_fits_in_milliseconds() {
        let mut request = entry(&[(5, 0)], Outputs::AllSupported);
        request.encodings = Some(Encodings {
            max_labels: 32_768,
            outputs: vec![EncodingSpec {
                output: "active_span_micros_dev_quantile".to_string(),
                bins: None,
            }],
        });
        let plan = FeaturePlan::resolve(
            &request,
            profile(NativeGranularity::Tick, true, &[(5, 0)]),
            "input",
        )
        .unwrap();
        let mut fitted = plan.streams[0].encodings[0].clone();
        assert_eq!(fitted.input_divisor, 1_000.0);
        // The pinned quantile witness [1, 2, 3, 4], supplied in microseconds.
        let development = numbers(&[1_000.0, 2_000.0, 3_000.0, 4_000.0]);
        fitted.fit(&development, 32_768).unwrap();
        assert_eq!(fitted.edges, Some(vec![1.6, 2.2, 2.8, 3.4000000000000004]));
        assert_eq!(
            fitted.labels,
            ["-inf_to_1.6", "1.6_to_2.2", "2.8_to_3.4", "3.4_to_inf"]
        );
        assert_eq!(fitted.encode(&development), [0, 1, 2, 3]);
        assert_eq!(fitted.encode(&numbers(&[2_500.0])), [-1]);
        let reloaded: FittedEncoding =
            serde_json::from_str(&serde_json::to_string(&fitted).unwrap()).unwrap();
        assert_eq!(reloaded.encode(&development), [0, 1, 2, 3]);
    }

    #[test]
    fn epsilon_comparisons_do_not_overflow() {
        assert_eq!(beyond(i64::MAX, i64::MIN, i64::MAX), Ordering::Greater);
        assert_eq!(beyond(i64::MIN, i64::MAX, i64::MAX), Ordering::Less);
        assert_eq!(beyond(i64::MAX, i64::MAX - 1, 0), Ordering::Greater);
        assert_eq!(beyond(6, 3, 2), Ordering::Greater);
        assert_eq!(beyond(5, 3, 2), Ordering::Equal);
        assert_eq!(beyond(1, 3, 2), Ordering::Equal);
        assert_eq!(beyond(0, 3, 2), Ordering::Less);
    }

    #[test]
    fn gap_classes_follow_the_ladder() {
        assert_eq!(gap_class(0), "duplicate_or_backwards");
        assert_eq!(gap_class(2_000_000), "normal_small_tick_delay");
        assert_eq!(gap_class(2_000_001), "medium_feed_delay");
        assert_eq!(gap_class(60_000_000), "large_feed_delay");
        assert_eq!(
            gap_class(1_800_000_001),
            "major_data_outage_or_session_break"
        );
    }

    /// The reference's whole-list terminal formula, kept literal.
    #[allow(clippy::manual_div_ceil)]
    fn terminal_from_list(signs: &[i8]) -> (usize, i64) {
        let n = signs.len();
        let count = n.min(1.max((n + 2) / 3));
        let tail = &signs[n - count..];
        (tail.len(), tail.iter().map(|sign| i64::from(*sign)).sum())
    }

    #[test]
    fn tick_path_deque_matches_the_whole_list_formula_at_every_prefix() {
        let mut path = TickPath::new(0, 5_000_000);
        let mut previous = Some(seen(0, 1.0));
        let mut signs: Vec<i8> = Vec::new();
        let mut price = 1.0;
        let mut state = 7u64;
        for step in 1..400 {
            state = state
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1);
            let draw = (state >> 33) % 3;
            let delta = [0.0, 0.000_01, -0.000_01][draw as usize];
            price += delta;
            path.fold(
                previous,
                seen(step * 10_000, price),
                Some((2_000_000, 60_000_000)),
            );
            previous = Some(seen(step * 10_000, price));
            if delta != 0.0 {
                signs.push(if delta > 0.0 { 1 } else { -1 });
            }
            let (count, total) = terminal_from_list(&signs);
            assert_eq!(path.signs.len(), count, "step {step}");
            assert_eq!(path.signs.iter().map(|s| i64::from(*s)).sum::<i64>(), total);
            assert_eq!(path.signs.len(), path.directional().div_ceil(3) as usize);
        }
    }

    #[test]
    fn tick_path_cases_cover_ready_pressure_shape_and_terminal_buckets() {
        let gap = Some((2_000_000, 60_000_000));
        let feed = |prices: &[f64]| {
            let mut path = TickPath::new(0, 5_000_000);
            let mut previous = None;
            for (index, price) in prices.iter().enumerate() {
                let event = index as i64 * 100_000;
                path.fold(previous, seen(event, *price), gap);
                previous = Some(seen(event, *price));
            }
            let (high, low) = prices.iter().fold((f64::MIN, f64::MAX), |(high, low), p| {
                (high.max(*p), low.min(*p))
            });
            path.summary(prices[0], high, low, prices[prices.len() - 1])
        };
        let flat = feed(&[1.0, 1.0, 1.0, 1.0]);
        assert_eq!((flat.flat, flat.directional, flat.ready), (3, 0, false));
        assert_eq!(flat.pressure, "insufficient_tick_path");
        assert_eq!(flat.close_position, 0.5);
        assert_eq!(flat.terminal_moves, 0);
        let alternating = feed(&[1.0, 1.1, 1.0, 1.1, 1.0, 1.1]);
        assert_eq!(
            (alternating.up, alternating.down, alternating.changes),
            (3, 2, 4)
        );
        assert_eq!(alternating.reversal_rate, 1.0);
        assert_eq!(alternating.shape, "churn");
        assert_eq!(alternating.reversal_bucket, "high_reversal");
        let directional = feed(&[1.0, 1.1, 1.2, 1.3, 1.4]);
        assert_eq!(directional.efficiency, 1.0);
        assert_eq!(
            (
                directional.pressure,
                directional.shape,
                directional.efficiency_bucket
            ),
            ("pressure_up", "clean_push", "high_efficiency")
        );
        assert_eq!(directional.terminal_moves, 2, "ceil(4 / 3)");
        assert_eq!(directional.terminal_bucket, "terminal_up");
        // Four ups then a drop to the low: pressure up, close at the low, failed push.
        let failed = feed(&[1.0, 1.1, 1.2, 1.3, 1.4, 0.9]);
        assert_eq!(failed.failed, "failed_up");
        assert_eq!(failed.shape, "failed_push");
        assert_eq!(failed.close_position, 0.0);
        for n in [3u64, 4, 5, 6, 7] {
            let prices: Vec<f64> = (0..=n).map(|i| 1.0 + i as f64 * 0.001).collect();
            assert_eq!(feed(&prices).terminal_moves, n.div_ceil(3));
        }
        // Failed pressure reads the unrounded close position: 0.45000049 lies above the 0.45
        // threshold, while its six-place rendering would not.
        let mut path = TickPath::new(0, 5_000_000);
        let prices = [1.0, 1.1, 1.2, 1.3];
        let mut previous = None;
        for (index, price) in prices.iter().enumerate() {
            path.fold(previous, seen(index as i64 * 1_000, *price), gap);
            previous = Some(seen(index as i64 * 1_000, *price));
        }
        let unrounded = path.summary(1.0, 2.0, 1.0, 1.450_000_49);
        assert_eq!(unrounded.close_position, 0.45);
        assert_eq!(unrounded.failed, "none");
        let rounded = path.summary(1.0, 2.0, 1.0, 1.45);
        assert_eq!(rounded.failed, "failed_up");
    }

    #[test]
    fn tick_path_ignores_the_transition_from_the_previous_interval_and_duplicates_are_flat() {
        let gap = Some((2_000_000, 60_000_000));
        let mut path = TickPath::new(5_000_000, 10_000_000);
        path.fold(Some(seen(4_900_000, 1.0)), seen(5_000_000, 1.5), gap);
        assert_eq!((path.up, path.down, path.flat), (0, 0, 0));
        assert!(path.max_abs_bps > 4_999.0, "the entering jump still counts");
        path.fold(Some(seen(5_000_000, 1.5)), seen(5_000_000, 1.5), gap);
        assert_eq!(path.flat, 1, "an identical repeat is a flat move");
        path.fold(Some(seen(5_000_000, 1.5)), seen(5_100_000, 1.6), gap);
        assert_eq!(path.up, 1);
        // Distinct units that collapse to one binary float are still distinct moves: the sign
        // is an exact-unit decision, unlike the reference's floating subtraction.
        let unit = TickSeen {
            event: 5_100_000,
            price: 1.0,
            units: 1_000_000_000_000_000_000,
        };
        let next = TickSeen {
            units: unit.units + 1,
            event: 5_200_000,
            ..unit
        };
        assert_eq!(next.price - unit.price, 0.0);
        path.fold(Some(unit), next, gap);
        assert_eq!((path.up, path.flat), (2, 1));
    }

    fn tick(millis: i64, price_units: i64) -> Observation {
        Observation::Tick(Tick {
            event_time_micros: millis * 1_000,
            price_units,
        })
    }

    #[test]
    fn plans_resolve_capabilities_and_named_requests_exactly() {
        let ticks = profile(NativeGranularity::Tick, true, &[(5, 0), (60, 30)]);
        let plan = FeaturePlan::resolve(
            &entry(&[(5, 0), (60, 30)], Outputs::AllSupported),
            ticks.clone(),
            "input",
        )
        .unwrap();
        assert_eq!(plan.streams.len(), 2);
        let stream = &plan.streams[0];
        assert!(stream.excluded.is_empty(), "{:?}", stream.excluded);
        assert!(
            stream
                .output_index("tick_path_terminal_pressure_bucket")
                .is_some()
        );
        assert!(stream.output_index("regime_v1").is_some());
        assert!(
            plan.streams[1].tick_path,
            "a sixty-second tick stream carries a path when configured"
        );
        let bars = NativeGranularity::Bar { period_seconds: 5 };
        let bar_profile = profile(bars, false, &[(60, 30)]);
        let bar_plan = FeaturePlan::resolve(
            &entry(&[(60, 30)], Outputs::AllSupported),
            bar_profile.clone(),
            "input",
        )
        .unwrap();
        let stream = &bar_plan.streams[0];
        for name in [
            "tick_volume",
            "tick_path_ready",
            "tick_volume_mean_5",
            "tick_volume_vs_recent_ratio",
            "regime_quality_state",
            "regime_v1",
            "is_regime_clean",
            "has_gap",
            "frozen_price_flag",
        ] {
            let excluded = stream.excluded.iter().find(|e| e.name == name);
            assert!(
                excluded.is_some_and(|e| e.reason.contains("individual ticks")),
                "{name}: {excluded:?}"
            );
        }
        for name in [
            "regime_trend_state",
            "market_structure_bias",
            "ema20",
            "candle_type",
            "momentum_10_bps",
            "missing_buckets_since_prev_candle",
            "active_span_micros",
        ] {
            assert!(
                stream.output_index(name).is_some(),
                "{name} is bar-compatible"
            );
        }
        let error = FeaturePlan::resolve(
            &entry(
                &[(60, 30)],
                Outputs::Named(vec!["regime_quality_state".to_string()]),
            ),
            bar_profile,
            "input",
        )
        .unwrap_err();
        assert!(
            error.contains("regime_quality_state") && error.contains("individual ticks"),
            "{error}"
        );
        let mut no_windows = entry(
            &[(5, 0)],
            Outputs::Named(vec!["compression_state".to_string()]),
        );
        no_windows.structure.as_mut().unwrap().rolling_windows = vec![4, 8];
        no_windows.structure.as_mut().unwrap().direction_window = 8;
        let error = FeaturePlan::resolve(
            &no_windows,
            profile(NativeGranularity::Tick, true, &[(5, 0)]),
            "input",
        )
        .unwrap_err();
        assert!(error.contains("rolling window 5"), "{error}");
        let mut all = entry(&[(5, 0)], Outputs::AllSupported);
        all.structure.as_mut().unwrap().rolling_windows = vec![4, 8];
        all.structure.as_mut().unwrap().direction_window = 8;
        all.moving_average_periods = Some(vec![8, 21]);
        let plan = FeaturePlan::resolve(
            &all,
            profile(NativeGranularity::Tick, true, &[(5, 0)]),
            "input",
        )
        .unwrap();
        let stream = &plan.streams[0];
        assert!(
            stream
                .excluded
                .iter()
                .any(|e| e.name == "compression_state" && e.reason.contains("rolling window 5"))
        );
        assert!(
            stream
                .excluded
                .iter()
                .any(|e| e.name == "ema20_minus_ema50_bps" && e.reason.contains("period 20"))
        );
        assert!(
            stream.output_index("momentum_8_bps").is_some()
                && stream.output_index("ema21").is_some()
        );
        let error = FeaturePlan::resolve(
            &entry(
                &[(5, 0)],
                Outputs::Named(vec!["body_bps_bucketed".to_string()]),
            ),
            ticks.clone(),
            "input",
        )
        .unwrap_err();
        assert!(error.contains("not a compiled output"), "{error}");
        // An encoding whose input a stream excludes is excluded on that stream with the input's
        // reason; an encoding of nothing compiled is an error.
        let encodings = |outputs: &[(&str, Option<Bins>)]| {
            Some(crate::config::Encodings {
                max_labels: 8,
                outputs: outputs
                    .iter()
                    .map(|(output, bins)| EncodingSpec {
                        output: (*output).to_string(),
                        bins: bins.clone(),
                    })
                    .collect(),
            })
        };
        let mut encoded = entry(&[(5, 0), (60, 30)], Outputs::AllSupported);
        encoded.tick_path_streams = Some(vec![StreamKey {
            duration_seconds: 5,
            offset_seconds: 0,
        }]);
        encoded.encodings = encodings(&[
            ("tick_path_signed_imbalance_bucketed", None),
            ("candle_type", None),
            ("body_bps", Some(Bins::DevelopmentFifths)),
        ]);
        let plan = FeaturePlan::resolve(&encoded, ticks.clone(), "input").unwrap();
        assert_eq!(plan.streams[0].encodings.len(), 3);
        assert_eq!(
            plan.streams[1]
                .encodings
                .iter()
                .map(|encoding| encoding.output.as_str())
                .collect::<Vec<_>>(),
            ["candle_type", "body_bps"]
        );
        assert!(plan.streams[1].excluded.iter().any(|exclusion| {
            exclusion.name == "tick_path_signed_imbalance_bucketed"
                && exclusion.reason.contains("60s/30s in tick_path_streams")
        }));
        for (outputs, expected) in [
            (&[("nothing", None)][..], "not a selected output"),
            (&[("close_time_micros", None)][..], "never encoded"),
            (&[("body_bps", None)][..], "needs `bins`"),
            (
                &[("candle_type", Some(Bins::DevelopmentFifths))][..],
                "takes no bins",
            ),
            (
                &[("body_bps_bucketed", Some(Bins::DevelopmentFifths))][..],
                "its own bins",
            ),
        ] {
            let mut bad = entry(&[(5, 0)], Outputs::AllSupported);
            bad.encodings = encodings(outputs);
            let error = FeaturePlan::resolve(&bad, ticks.clone(), "input").unwrap_err();
            assert!(error.contains(expected), "{outputs:?}: {error}");
        }
        let error = FeaturePlan::resolve(&entry(&[(15, 5)], Outputs::AllSupported), ticks, "input")
            .unwrap_err();
        assert!(error.contains("15s/5s"), "{error}");
    }

    #[test]
    fn a_valid_scale_nine_candle_keeps_its_direction_and_geometry() {
        let mut instrument = definition(NativeGranularity::Tick, &[(5, 0)]);
        instrument.price_scale = scale(9);
        let candle = Candle {
            open_time_micros: 0,
            close_time_micros: 5_000_000,
            known_at_micros: 5_000_000,
            first_event_micros: 0,
            last_event_micros: 4_000_000,
            active_span_micros: 4_000_000,
            open_units: 1,
            high_units: 3,
            low_units: 1,
            close_units: 2,
            observations: 3,
            duplicates: 0,
            volume: None,
            gap_before_micros: None,
            max_gap_inside_micros: 2_000_000,
            missing_buckets_before: 0,
            frozen_observations: 1,
            frozen_micros: 0,
            max_jump_basis_points: 0,
            max_delayed_jump_basis_points: 0,
            max_reopen_jump_basis_points: 0,
            flags: Flags::default(),
        };
        let anatomy = Anatomy::new(&candle, instrument.price_scale.unit() as f64);
        assert_eq!(anatomy.direction, "up");
        assert_eq!(anatomy.body_to_range, 0.5);
        assert_eq!(
            (anatomy.body_units, anatomy.range_units),
            (Some(1), Some(2))
        );
        assert_eq!(
            format!("{:.8}", anatomy.close),
            "0.00000000",
            "the eight-place projection alone would erase the move"
        );
        // Unit differences beyond signed 64 bits are unavailable, never wrapped, and the
        // direction is still decided on units.
        let wide = Candle {
            open_units: i64::MAX,
            high_units: i64::MAX,
            low_units: -1,
            close_units: i64::MAX - 1,
            ..candle
        };
        let anatomy = Anatomy::new(&wide, instrument.price_scale.unit() as f64);
        assert_eq!(anatomy.direction, "down");
        assert_eq!(
            (
                anatomy.body_units,
                anatomy.range_units,
                anatomy.upper_wick_units,
                anatomy.lower_wick_units
            ),
            (Some(1), None, Some(0), Some(i64::MAX))
        );
    }

    #[test]
    fn session_exclusion_breaks_clean_feature_adjacency() {
        use crate::session::{Boundary, Session};
        let native = NativeGranularity::Bar { period_seconds: 5 };
        let mut reference = profile(native, false, &[(5, 0)]);
        reference.definition.candles[0].min_observations = None;
        reference.definition.candles[0].hard_min_observations = None;
        reference.definition.session = Some(Session::Weekly {
            timezone: "UTC".into(),
            open: Boundary {
                day: "monday".into(),
                // Inclusive close keeps 00:00:05; leave 00:00:10 closed to test adjacency.
                time: "00:00:15".into(),
            },
            close: Boundary {
                day: "monday".into(),
                time: "00:00:05".into(),
            },
            closed_dates: vec![],
            early_closes: vec![],
        });
        let mut request = entry(
            &[(5, 0)],
            Outputs::Named(vec![
                "clean_segment_index".into(),
                "range_overlap".into(),
                "candle_pattern".into(),
                "previous_candle_relation".into(),
            ]),
        );
        request.tick_path_streams = None;
        let plan = FeaturePlan::resolve(&request, reference, "input").unwrap();
        let mut engine = FeatureEngine::new(&plan, source(native, false)).unwrap();
        let start = crate::market::parse_event_time_micros("2026-09-07T00:00:00Z").unwrap();
        let mut out = FeatureOutput::default();
        for seconds in [0, 5, 10, 15, 20] {
            engine
                .push(
                    Observation::Bar(crate::stream::BarUnits {
                        start_micros: start + seconds * MICROS_PER_SECOND,
                        period_micros: 5 * MICROS_PER_SECOND,
                        open: 1_000_000,
                        high: 1_000_002,
                        low: 999_999,
                        close: 1_000_001,
                        volume: 1.0,
                    }),
                    &mut out,
                )
                .unwrap();
        }
        // The inclusive closing bar adds a row to the first segment; the closed bucket
        // still breaks adjacency, and both bars after reopening share the next segment.
        assert_eq!(out.rows.len(), 4);
        let column = plan.streams[0].output_index("clean_segment_index").unwrap();
        assert_eq!(
            out.rows
                .iter()
                .map(|(_, row)| row.values[column].clone())
                .collect::<Vec<_>>(),
            vec![
                Some(Value::Int(1)),
                Some(Value::Int(1)),
                Some(Value::Int(2)),
                Some(Value::Int(2))
            ]
        );
        for name in ["range_overlap", "candle_pattern"] {
            let column = plan.streams[0].output_index(name).unwrap();
            assert_eq!(out.rows[0].1.values[column], None, "{name}: first row");
            assert_eq!(
                out.rows[2].1.values[column], None,
                "{name}: skipped interval"
            );
            assert!(
                out.rows[1].1.values[column].is_some(),
                "{name}: adjacent row"
            );
            assert!(
                out.rows[3].1.values[column].is_some(),
                "{name}: recovered row"
            );
        }
        let relation = plan.streams[0]
            .output_index("previous_candle_relation")
            .unwrap();
        assert_eq!(
            out.rows[2].1.values[relation],
            Some(Value::Text("no_adjacent_previous_clean_candle".into()))
        );
        let pattern = plan.streams[0].output_index("candle_pattern").unwrap();
        assert_eq!(
            out.rows[3].1.values[pattern],
            Some(Value::Text("none".into()))
        );
    }

    #[test]
    fn tiny_returns_publish_zero_but_remain_eligible_for_reversal() {
        let native = NativeGranularity::Bar { period_seconds: 5 };
        let mut request = entry(
            &[(5, 0)],
            Outputs::Named(vec!["return_1_bps".into(), "sign_reversal_rate_5".into()]),
        );
        request.tick_path_streams = None;
        let mut reference = profile(native, false, &[(5, 0)]);
        reference.definition.candles[0].min_observations = None;
        reference.definition.candles[0].hard_min_observations = None;
        let plan = FeaturePlan::resolve(&request, reference, "input").unwrap();
        let mut engine = FeatureEngine::new(&plan, source(native, false)).unwrap();
        let mut out = FeatureOutput::default();
        let base = 1_000_000_000_000_i64;
        for (index, close) in [base, base + 1, base, base + 1, base, base + 1]
            .into_iter()
            .enumerate()
        {
            let open = if index == 0 {
                base
            } else if close == base {
                base + 1
            } else {
                base
            };
            engine
                .push(
                    Observation::Bar(crate::stream::BarUnits {
                        start_micros: index as i64 * 5 * MICROS_PER_SECOND,
                        period_micros: 5 * MICROS_PER_SECOND,
                        open,
                        high: open.max(close) + 10,
                        low: open.min(close) - 10,
                        close,
                        volume: 1.0,
                    }),
                    &mut out,
                )
                .unwrap();
        }
        assert_eq!(out.rows.len(), 6);
        let returns = plan.streams[0].output_index("return_1_bps").unwrap();
        assert_eq!(out.rows[0].1.values[returns], None);
        for (_, row) in &out.rows[1..] {
            assert_eq!(row.values[returns], Some(Value::Float(0.0)));
        }
        let reversal = plan.streams[0]
            .output_index("sign_reversal_rate_5")
            .unwrap();
        assert_eq!(out.rows[5].1.values[reversal], Some(Value::Float(1.0)));
    }

    #[test]
    fn the_engine_emits_rows_only_for_accepted_candles_and_stays_prefix_stable() {
        let plan = FeaturePlan::resolve(
            &entry(&[(5, 0)], Outputs::AllSupported),
            profile(NativeGranularity::Tick, true, &[(5, 0)]),
            "input",
        )
        .unwrap();
        let named = FeaturePlan::resolve(
            &entry(
                &[(5, 0)],
                Outputs::Named(vec!["body_bps".to_string(), "ema20".to_string()]),
            ),
            profile(NativeGranularity::Tick, true, &[(5, 0)]),
            "input",
        )
        .unwrap();
        let mut ticks = Vec::new();
        let mut price = 1_000_000;
        let mut state = 3u64;
        for second in 0..600 {
            for sub in 0..6 {
                state = state
                    .wrapping_mul(6_364_136_223_846_793_005)
                    .wrapping_add(1);
                price += ((state >> 40) % 5) as i64 - 2;
                ticks.push(tick(second * 1_000 + sub * 150, price));
            }
        }
        let run = |plan: &FeaturePlan, ticks: &[Observation]| {
            let mut engine =
                FeatureEngine::new(plan, source(NativeGranularity::Tick, true)).unwrap();
            let mut out = FeatureOutput::default();
            for observation in ticks {
                engine.push(*observation, &mut out).unwrap();
            }
            (out, engine.profile())
        };
        let (full, profile) = run(&plan, &ticks);
        assert_eq!(full.rows.len() as u64, profile.streams[0].flagged.clean);
        assert!(
            full.rows.len() > 100
                && !full.structure_events.is_empty()
                && !full.sequence_events.is_empty()
        );
        let stream = &plan.streams[0];
        let close = stream.output_index("close_units").unwrap();
        let regime = stream.output_index("regime_v1").unwrap();
        assert!(
            full.rows
                .iter()
                .all(|(_, row)| row.values[close].is_some() && row.values[regime].is_some())
        );
        for percent in [25, 50, 75] {
            let (prefix, _) = run(&plan, &ticks[..ticks.len() * percent / 100]);
            assert_eq!(prefix.rows, full.rows[..prefix.rows.len()]);
            assert_eq!(
                prefix.structure_events,
                full.structure_events[..prefix.structure_events.len()]
            );
            assert_eq!(
                prefix.sequence_events,
                full.sequence_events[..prefix.sequence_events.len()]
            );
        }
        // A named selection keeps the row identity and clocks, reproduces the selected values
        // exactly, and holds no state for the stages it does not read.
        let identity: Vec<&str> = stream.outputs[..10]
            .iter()
            .map(|output| output.name.as_str())
            .collect();
        assert!(identity.contains(&"known_at_micros") && identity.contains(&"close_units"));
        let names: Vec<&str> = named.streams[0]
            .outputs
            .iter()
            .map(|output| output.name.as_str())
            .collect();
        assert_eq!(
            names,
            [identity.as_slice(), &["body_bps", "ema20"]].concat()
        );
        assert!(
            stream
                .outputs
                .iter()
                .all(|output| !output.readiness.is_empty())
        );
        let (selected, _) = run(&named, &ticks);
        assert_eq!(selected.rows.len(), full.rows.len());
        assert!(selected.structure_events.is_empty() && selected.sequence_events.is_empty());
        for ((_, row), (_, all)) in selected.rows.iter().zip(&full.rows) {
            for (index, name) in names.iter().enumerate() {
                assert_eq!(
                    row.values[index],
                    all.values[stream.output_index(name).unwrap()],
                    "{name}"
                );
            }
        }
        let plan_bytes = plan.to_json();
        assert_eq!(FeaturePlan::from_json(&plan_bytes).unwrap(), plan);
    }
}

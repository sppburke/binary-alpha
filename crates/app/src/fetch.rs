//! `binary-alpha data fetch` and the one bounded native-history acquisition owner: backward
//! paging from a fixed end, explicit imported seeds, overlap comparison, coverage, durable page
//! progress, and manifest-last publication of one cumulative generation per instrument. Ticks
//! and five-second bars share every rule; only the row type differs.

use crate::archive::{self, BAR_OBJECT_PATH, DataSummary, TICK_OBJECT_PATH};
use crate::broker::{self, Clock, HistoryRows, MarketDataBroker, SystemClock};
use crate::import::{self, CODE_REVISION};
use crate::store::{self, ObjectIdentity, Store};
use crate::verify;
use binary_alpha_engine::config::{Broker, Config, History, Seed};
use binary_alpha_engine::dataset::{
    Capability, Coverage, DatasetRole, GenerationManifest, Input, IntervalContract,
    MANIFEST_SCHEMA_VERSION, NativeGranularity, ObjectRecord, ObjectRole, PriceRepresentation,
    SourceKind, TimeUnit, generation_id, manifest_key, object_key,
};
use binary_alpha_engine::market::{
    Bar, BarSequence, InstrumentId, PriceScale, Tick, TickSequence,
    format_event_time_micros as time_text, parse_event_time_micros as time,
};
use binary_alpha_engine::research::{Access, Declaration};
use serde::{Deserialize, Serialize};
use std::fs;
use std::io::Write;
use std::path::Path;
use std::time::Instant;

pub const COVERAGE_PATH: &str = "provenance/coverage.json";
/// Object paths beneath which a seeded generation retains its seed's manifest and objects.
const SEED_PREFIX: &str = "seed/";
/// The acquisition outcome that leaves the intent pending for the next invocation.
pub const BUDGET_SHORTFALL: &str = "budget";
/// The provider's latest observation lies before the requested end: not a gap in what was
/// acquired.
pub const TAIL_SHORTFALL: &str = "unresolved_tail";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Range {
    pub start: String,
    pub end: String,
}
impl Range {
    fn new(start: i64, end: i64) -> Self {
        Self {
            start: time_text(start),
            end: time_text(end),
        }
    }
    fn bounds(&self) -> Result<(i64, i64), String> {
        Ok((time(&self.start)?, time(&self.end)?))
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Actual {
    pub first: String,
    pub last: String,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PageCoverage {
    pub path: String,
    pub sha256: String,
    pub bytes: u64,
    pub anchor: Option<String>,
    pub rows: u64,
    pub first: Option<String>,
    pub last: Option<String>,
    /// Local receipt time of the request that produced the page; absent in records written
    /// before it was retained.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub receipt_time: Option<String>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Shortfall {
    pub reason: String,
    pub unresolved: Range,
}
/// The imported generation a lineage extends and the source context it was bound to.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SeedLineage {
    pub generation: String,
    pub source_identity: String,
}
/// Requested, directly observed, and verified coverage are separate immutable facts.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HistoryCoverage {
    pub schema_version: u32,
    pub source_identity: String,
    pub broker: String,
    pub provider_symbol: String,
    pub role: DatasetRole,
    pub requested: Range,
    pub verified: Option<Range>,
    pub actual: Option<Actual>,
    pub rows: u64,
    pub pages: Vec<PageCoverage>,
    pub shortfall: Option<Shortfall>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tail_shortfall: Option<Shortfall>,
    #[serde(default, skip_serializing_if = "NativeGranularity::is_tick")]
    pub native_granularity: NativeGranularity,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub seed: Option<SeedLineage>,
}
#[derive(Debug, Clone, Copy)]
pub enum PassLimit {
    Unbounded,
    Exactly(usize),
}

/// One request made in this invocation, recorded whether or not its bytes were new.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PageReceipt {
    pub anchor: Option<String>,
    pub sha256: String,
    pub bytes: u64,
    pub rows: u64,
    pub receipt_time: String,
}

/// Durable progress of one bounded acquisition: the pinned baseline and range, and every
/// retained page in request order. Resuming replays the pages from their retained bytes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Progress {
    pub baseline: Option<String>,
    pub start: String,
    pub cutoff: String,
    pub pages: Vec<PageCoverage>,
}

/// The range one pass acquires: an explicit range, or everything from the acquisition frontier
/// (less the configured overlap) up to a pinned cutoff.
#[derive(Debug, Clone, Copy)]
pub enum Requested {
    Explicit { start: i64, end: i64 },
    Advance { cutoff: i64 },
}

/// Receives the intent before the first request and the progress after every retained page.
pub type Persist<'a> = &'a mut dyn FnMut(&Progress) -> Result<(), String>;

/// Per-invocation limits and durable progress of a pipeline acquisition.
pub struct Bounds<'a> {
    pub max_pages: Option<u32>,
    pub deadline_micros: Option<i64>,
    pub clock: &'a dyn Clock,
    /// The governance declaration that permits every baseline before it is opened; absent
    /// means the configuration's own declaration, if any.
    pub declaration: Option<&'a Declaration>,
    /// The pending intent to resume, if any; its baseline, range, and pages are authoritative.
    pub resume: Option<Progress>,
    pub persist: Option<Persist<'a>>,
}
impl<'a> Bounds<'a> {
    /// A standalone pass: no page or time limit and no durable progress.
    pub fn none(clock: &'a dyn Clock) -> Self {
        Self {
            max_pages: None,
            deadline_micros: None,
            clock,
            declaration: None,
            resume: None,
            persist: None,
        }
    }
}

/// The result of one instrument's pass.
#[derive(Debug, Clone)]
pub struct Outcome {
    pub instrument: InstrumentId,
    /// The ready generation this pass published or reused, if any.
    pub generation: Option<String>,
    pub coverage: HistoryCoverage,
    /// The acquisition stopped on its page or time budget before reaching its start.
    pub pending: bool,
    pub receipts: Vec<PageReceipt>,
}

pub fn run(config_path: &Path, out: &mut dyn Write) -> Result<(), String> {
    let config = crate::load_config(config_path)?;
    config
        .history
        .as_ref()
        .ok_or("fetch: the configuration declares no history table")?;
    let root = config_path.parent().unwrap_or(Path::new("."));
    let local = Store::filesystem(root.join(config.storage.historical_data_dir.as_path()));
    let destination = Store::open(&config.storage.publication_uri)?;
    // Every seed binding and source context is checked before any credential is resolved.
    prepare(&config, &local, None)?;
    let mut adapter = broker::connect(&config)?;
    passes(
        &config,
        adapter.market(),
        &local,
        &destination,
        &mut SystemClock,
        PassLimit::Unbounded,
        out,
    )
}

pub fn passes(
    config: &Config,
    broker: &mut dyn MarketDataBroker,
    local: &Store,
    destination: &Store,
    clock: &mut dyn Clock,
    limit: PassLimit,
    out: &mut dyn Write,
) -> Result<(), String> {
    let history = config
        .history
        .as_ref()
        .ok_or("fetch: the configuration declares no history table")?;
    let start = time(&history.start)?;
    let mut index = 0;
    loop {
        if matches!(limit, PassLimit::Exactly(count) if index >= count) {
            return Ok(());
        }
        let end = if index == 0 {
            time(&history.end)?
        } else {
            clock.now_micros()
        };
        acquire(
            config,
            broker,
            local,
            destination,
            Requested::Explicit { start, end },
            &mut Bounds::none(&*clock),
            out,
        )?;
        index += 1;
        let Some(interval) = history.refresh_interval_seconds else {
            return Ok(());
        };
        if matches!(limit, PassLimit::Exactly(count) if index >= count) {
            return Ok(());
        }
        clock.sleep(i64::from(interval) * 1_000_000);
    }
}

/// The row type one acquisition handles; ticks and bars share every paging, overlap, and
/// coverage rule through this boundary.
pub trait Row: Copy + PartialEq + std::fmt::Debug + Send {
    type Sequence: Default;
    /// The provider event time: the tick time or the bar start.
    fn time(&self) -> i64;
    /// The first instant after the row is complete: the tick time plus one microsecond or the
    /// bar end.
    fn end(&self) -> i64;
    /// Two rows at one event time that disagree.
    fn conflicts(&self, other: &Self) -> bool;
    fn accept(sequence: &mut Self::Sequence, row: Self) -> Result<(), String>;
    fn unpack(rows: HistoryRows) -> Result<Vec<Self>, String>;
    /// Decodes one retained data object of `manifest` in order, returning the provider
    /// identifier the object carried when its rows carry one.
    fn read(
        path: &Path,
        manifest: &GenerationManifest,
        scale: PriceScale,
        sink: &mut dyn FnMut(Self),
    ) -> Result<Option<i32>, String>;
    fn write(
        path: &Path,
        instrument: &InstrumentId,
        native: &Native,
        rows: Vec<Self>,
    ) -> Result<DataSummary, String>;
}

impl Row for Tick {
    type Sequence = TickSequence;
    fn time(&self) -> i64 {
        self.event_time_micros
    }
    fn end(&self) -> i64 {
        self.event_time_micros.saturating_add(1)
    }
    fn conflicts(&self, other: &Self) -> bool {
        self.event_time_micros == other.event_time_micros && self.price_units != other.price_units
    }
    fn accept(sequence: &mut TickSequence, row: Self) -> Result<(), String> {
        sequence.accept(row)
    }
    fn unpack(rows: HistoryRows) -> Result<Vec<Self>, String> {
        match rows {
            HistoryRows::Ticks(rows) => Ok(rows),
            HistoryRows::Bars(_) => {
                Err("fetch: the adapter returned bars for a tick request".into())
            }
        }
    }
    fn read(
        path: &Path,
        _: &GenerationManifest,
        scale: PriceScale,
        sink: &mut dyn FnMut(Self),
    ) -> Result<Option<i32>, String> {
        archive::read_ticks_with(path, scale, |row| {
            sink(row);
            Ok(())
        })?;
        Ok(None)
    }
    fn write(
        path: &Path,
        instrument: &InstrumentId,
        native: &Native,
        rows: Vec<Self>,
    ) -> Result<DataSummary, String> {
        archive::write_ticks(path, instrument, native.scale, rows.into_iter().map(Ok))
    }
}

impl Row for Bar {
    type Sequence = BarSequence;
    fn time(&self) -> i64 {
        self.start_unix_s * 1_000_000
    }
    fn end(&self) -> i64 {
        (self.start_unix_s + i64::from(self.period_s)) * 1_000_000
    }
    fn conflicts(&self, other: &Self) -> bool {
        self.start_unix_s == other.start_unix_s && self != other
    }
    fn accept(sequence: &mut BarSequence, row: Self) -> Result<(), String> {
        row.validate(row.period_s)?;
        sequence.accept(row.start_unix_s)
    }
    fn unpack(rows: HistoryRows) -> Result<Vec<Self>, String> {
        match rows {
            HistoryRows::Bars(rows) => Ok(rows),
            HistoryRows::Ticks(_) => {
                Err("fetch: the adapter returned ticks for a bar request".into())
            }
        }
    }
    fn read(
        path: &Path,
        manifest: &GenerationManifest,
        _: PriceScale,
        sink: &mut dyn FnMut(Self),
    ) -> Result<Option<i32>, String> {
        let summary =
            archive::validate_bar_file_with(path, &verify::bar_expectation(manifest)?, |bar| {
                sink(bar);
                Ok(())
            })?;
        Ok(summary.symbol_id)
    }
    fn write(
        path: &Path,
        instrument: &InstrumentId,
        native: &Native,
        rows: Vec<Self>,
    ) -> Result<DataSummary, String> {
        let symbol_id = native
            .symbol_id
            .ok_or("fetch: bar rows carry no provider identifier")?;
        archive::write_bars(
            path,
            instrument.provider_symbol.as_str(),
            symbol_id,
            native.server_offset_s,
            rows.into_iter().map(Ok),
        )
    }
}

/// What the normalized object of one acquisition records beyond its rows.
pub struct Native {
    pub granularity: NativeGranularity,
    pub scale: PriceScale,
    /// The provider's numeric identifier every bar row carried; absent for ticks.
    pub symbol_id: Option<i32>,
    /// The declared provider clock offset, recorded beside every bar start.
    pub server_offset_s: i64,
}

/// The generation an acquisition extends: a prior broker-history descendant with its coverage,
/// or an explicitly bound imported seed.
struct Baseline {
    manifest: GenerationManifest,
    coverage: Option<HistoryCoverage>,
    /// The seed lineage the descendant carries forward.
    seed: Option<SeedLineage>,
}
impl Baseline {
    fn first_time(&self) -> Result<i64, String> {
        time(&self.manifest.coverage.first_event_time)
    }
    fn last_time(&self) -> Result<i64, String> {
        time(&self.manifest.coverage.last_event_time)
    }
    fn verified(&self) -> Result<Option<(i64, i64)>, String> {
        self.coverage
            .as_ref()
            .and_then(|coverage| coverage.verified.as_ref())
            .map(Range::bounds)
            .transpose()
    }
}

/// Everything one instrument's pass fixes before the first request: the definition, the
/// baseline, and the acquisition range.
struct Plan<'a> {
    instrument: InstrumentId,
    definition: &'a binary_alpha_engine::config::Instrument,
    baseline: Option<Baseline>,
    requested: (i64, i64),
}

/// The prior broker-history generation of this instrument, role, granularity, source, and seed
/// lineage in the local mirror, if any: under a declaration, only its declared generations are
/// candidates and the mirror is never listed; otherwise every mirrored ready manifest is one.
/// With `only`, exactly that generation is the candidate.
#[allow(clippy::too_many_arguments)]
fn prior(
    local: &Store,
    instrument: &InstrumentId,
    history: &History,
    scale: PriceScale,
    source_identity: &str,
    seed_generation: Option<&str>,
    only: Option<&str>,
    declaration: Option<&Declaration>,
) -> Result<Option<Baseline>, String> {
    let candidates: Vec<String> = match (only, declaration) {
        (Some(generation), _) => vec![manifest_key(generation)],
        (None, Some(declaration)) => declaration
            .populations
            .iter()
            .filter(|population| {
                population.role == history.role && population.instrument == instrument.to_string()
            })
            .flat_map(|population| population.generations.iter())
            .map(|generation| manifest_key(generation))
            .collect(),
        (None, None) => local
            .list_manifests()
            .map_err(|reason| format!("fetch: {reason}"))?
            .iter()
            .map(|generation| manifest_key(generation))
            .collect(),
    };
    let expected_representation = match history.native_granularity {
        NativeGranularity::Tick => PriceRepresentation::IntegerUnits { scale },
        NativeGranularity::Bar { .. } => PriceRepresentation::BinaryFloat64,
    };
    let mut selected: Option<Baseline> = None;
    for key in candidates {
        if local.head(&key)?.is_none() {
            continue;
        }
        let mut bytes = Vec::new();
        local.read_to(&key, None, &mut bytes)?;
        if verify::manifest_kind(&bytes)?.is_some() {
            continue;
        }
        let manifest = GenerationManifest::from_json(&bytes)?;
        if manifest.key() != key {
            return Err(format!(
                "fetch: {} holds the manifest of generation {}",
                local.uri(&key),
                manifest.generation
            ));
        }
        if manifest.source_kind != SourceKind::BrokerHistory
            || manifest.instrument != instrument.to_string()
            || manifest.role != history.role
            || manifest.native_granularity != history.native_granularity
        {
            continue;
        }
        if manifest.price_representation != expected_representation {
            return Err("fetch: prior history price scale differs from configuration".into());
        }
        let object = manifest
            .objects
            .iter()
            .find(|object| object.path == COVERAGE_PATH)
            .ok_or("fetch: prior coverage object missing")?;
        let (_, fetched) = verify::fetch(local, object, true)?;
        let coverage: HistoryCoverage = serde_json::from_slice(
            &fs::read(&fetched.expect("decoded").path).map_err(|error| error.to_string())?,
        )
        .map_err(|error| format!("fetch: malformed prior coverage: {error}"))?;
        if coverage.source_identity != source_identity
            || coverage.seed.as_ref().map(|seed| seed.generation.as_str()) != seed_generation
        {
            continue;
        }
        if coverage.schema_version != 1
            || coverage.broker != instrument.broker.as_str()
            || coverage.provider_symbol != instrument.provider_symbol.as_str()
            || coverage.role != history.role
            || coverage.rows != manifest.row_count
        {
            return Err("fetch: prior coverage identity mismatch".into());
        }
        // The widest verified range wins; among equals, a closed acquisition outranks a partial
        // snapshot left by an exhausted budget, and more retained pages outrank fewer.
        let rank = |coverage: &HistoryCoverage| -> Result<(i64, std::cmp::Reverse<i64>, bool, usize), String> {
            let (start, end) = coverage
                .verified
                .as_ref()
                .map(Range::bounds)
                .transpose()?
                .unwrap_or((i64::MAX, i64::MIN));
            let closed = coverage
                .shortfall
                .as_ref()
                .is_none_or(|shortfall| shortfall.reason != BUDGET_SHORTFALL);
            Ok((end, std::cmp::Reverse(start), closed, coverage.pages.len()))
        };
        if selected
            .as_ref()
            .map(|prior| rank(prior.coverage.as_ref().expect("descendant coverage")))
            .transpose()?
            .is_none_or(|old| rank(&coverage).is_ok_and(|new| new > old))
        {
            selected = Some(Baseline {
                seed: coverage.seed.clone(),
                manifest,
                coverage: Some(coverage),
            });
        }
    }
    if let Some(prior) = &selected {
        verify::run_with(
            &local.uri(&prior.manifest.key()),
            Access {
                declaration,
                certification: None,
            },
        )?;
    }
    Ok(selected)
}

/// The explicitly bound seed of `instrument`: verified and permitted before any observation is
/// read, matching the history's broker, symbol, role, granularity, and price interpretation,
/// and collected under exactly the configured broker's source identity.
fn seed_baseline(
    seed: &Seed,
    instrument: &InstrumentId,
    history: &History,
    scale: PriceScale,
    source_identity: &str,
    declaration: Option<&Declaration>,
) -> Result<Baseline, String> {
    if seed.source_identity != source_identity {
        return Err(format!(
            "fetch {instrument}: seed {} was collected under source identity {}, not the configured broker's {source_identity}",
            seed.manifest.generation(),
            seed.source_identity
        ));
    }
    let uri = seed.manifest.to_string();
    verify::run_with(
        &uri,
        Access {
            declaration,
            certification: None,
        },
    )
    .map_err(|reason| format!("fetch {instrument}: seed: {reason}"))?;
    let (store, key) = verify::open(&uri)?;
    let mut bytes = Vec::new();
    store.read_to(&key, None, &mut bytes)?;
    let manifest = GenerationManifest::from_json(&bytes)?;
    let expected_representation = match history.native_granularity {
        NativeGranularity::Tick => PriceRepresentation::IntegerUnits { scale },
        NativeGranularity::Bar { .. } => PriceRepresentation::BinaryFloat64,
    };
    if manifest.instrument != instrument.to_string()
        || manifest.role != history.role
        || manifest.native_granularity != history.native_granularity
        || manifest.price_representation != expected_representation
    {
        return Err(format!(
            "fetch {instrument}: seed {} is {} {} {} data, not {} {} {} data at the configured price scale",
            manifest.generation,
            manifest.instrument,
            manifest.role,
            manifest.native_granularity,
            instrument,
            history.role,
            history.native_granularity
        ));
    }
    Ok(Baseline {
        seed: Some(SeedLineage {
            generation: manifest.generation.clone(),
            source_identity: seed.source_identity.clone(),
        }),
        manifest,
        coverage: None,
    })
}

/// The acquisition range and the retained baseline of every history instrument, fixed before
/// any credential is resolved or connection opened. A pending intent's baseline and range are
/// authoritative when `resume` names them.
fn plan<'a>(
    config: &'a Config,
    local: &Store,
    requested: Requested,
    resume: Option<&Progress>,
    declaration: Option<&Declaration>,
) -> Result<(Vec<Plan<'a>>, &'a History, &'a Broker, String), String> {
    let history = config
        .history
        .as_ref()
        .ok_or("fetch: the configuration declares no history table")?;
    history.validate()?;
    if let NativeGranularity::Bar { period_seconds } = history.native_granularity
        && period_seconds != 5
    {
        return Err(format!(
            "fetch: {} history is not supported; only ticks and 5-second bars are",
            history.native_granularity
        ));
    }
    let settings = config
        .brokers
        .iter()
        .find(|broker| broker.id() == &history.broker)
        .ok_or("fetch: broker is not declared")?;
    let source_identity = broker::source_identity(settings);
    let own = match declaration {
        Some(_) => None,
        None => crate::research::declaration(config)?,
    };
    let declaration = declaration.or(own.as_ref());
    let mut plans = Vec::with_capacity(history.instruments.len());
    for symbol in &history.instruments {
        let instrument = InstrumentId {
            broker: history.broker.clone(),
            provider_symbol: symbol.clone(),
        };
        let definition = config
            .instrument(&instrument, history.native_granularity)
            .filter(|definition| definition.native_granularity == history.native_granularity)
            .ok_or_else(|| {
                format!(
                    "fetch: instrument {instrument} is not declared at {} granularity",
                    history.native_granularity
                )
            })?;
        let seed = history
            .seeds
            .iter()
            .find(|seed| seed.provider_symbol == *symbol);
        let seed_generation = seed.map(|seed| seed.manifest.generation());
        let pinned = resume.and_then(|progress| progress.baseline.as_deref());
        let mut baseline = prior(
            local,
            &instrument,
            history,
            definition.price_scale,
            &source_identity,
            seed_generation,
            pinned,
            declaration,
        )?;
        if baseline.is_none()
            && let Some(seed) = seed
            && pinned.is_none_or(|pinned| pinned == seed.manifest.generation())
        {
            baseline = Some(seed_baseline(
                seed,
                &instrument,
                history,
                definition.price_scale,
                &source_identity,
                declaration,
            )?);
        }
        if pinned.is_some() && baseline.is_none() {
            return Err(format!(
                "fetch {instrument}: the pending intent's baseline {} is no longer readable",
                pinned.unwrap_or_default()
            ));
        }
        let overlap = i64::from(history.overlap_seconds.unwrap_or(0)) * 1_000_000;
        let history_start = time(&history.start)?;
        let (start, end) = match (resume, requested) {
            (Some(progress), _) => (time(&progress.start)?, time(&progress.cutoff)?),
            (None, Requested::Explicit { start, end }) => (start, end),
            (None, Requested::Advance { cutoff }) => {
                let frontier = match &baseline {
                    Some(baseline) => frontier(history.native_granularity, baseline.last_time()?),
                    None => history_start,
                };
                (history_start.max(frontier.saturating_sub(overlap)), cutoff)
            }
        };
        // A seeded lineage is extended, never narrowed: the declared start covers every retained
        // row and the end lies beyond the retained frontier.
        if let Some(baseline) = baseline.as_ref().filter(|baseline| baseline.seed.is_some()) {
            let frontier = frontier(history.native_granularity, baseline.last_time()?);
            if history_start > baseline.first_time()? || end < frontier {
                return Err(format!(
                    "fetch {instrument}: the requested range {} to {} narrows the retained lineage {} to {}; retained rows are never clipped",
                    time_text(history_start),
                    time_text(end),
                    baseline.manifest.coverage.first_event_time,
                    time_text(frontier)
                ));
            }
        }
        if start >= end {
            return Err("fetch: requested start must precede end".into());
        }
        plans.push(Plan {
            instrument,
            definition,
            baseline,
            requested: (start, end),
        });
    }
    Ok((plans, history, settings, source_identity))
}

/// The acquisition frontier a retained last row establishes: the row's time for ticks, the
/// bar's end for bars.
fn frontier(granularity: NativeGranularity, last_time: i64) -> i64 {
    match granularity {
        NativeGranularity::Tick => last_time,
        NativeGranularity::Bar { period_seconds } => {
            last_time.saturating_add(i64::from(period_seconds) * 1_000_000)
        }
    }
}

/// Validates every seed binding and prior lineage of the configured history without a broker,
/// so a mismatched source context is refused before any credential is resolved.
pub fn prepare(
    config: &Config,
    local: &Store,
    declaration: Option<&Declaration>,
) -> Result<(), String> {
    let history = config
        .history
        .as_ref()
        .ok_or("fetch: the configuration declares no history table")?;
    plan(
        config,
        local,
        Requested::Explicit {
            start: time(&history.start)?,
            end: time(&history.end)?,
        },
        None,
        declaration,
    )
    .map(|_| ())
}

/// Re-received rows must equal the retained rows wherever both are verified: from `floor`,
/// the start of the baseline's verified coverage or the seed's requested overlap, so a seed's
/// own unknown gaps are never mistaken for provider conflicts.
fn check_verified_overlap<R: Row>(
    instrument: &InstrumentId,
    previous: &[R],
    received: &[R],
    floor: i64,
) -> Result<(), String> {
    if let (Some(first), Some(last), Some(old_first), Some(old_last)) = (
        received.first(),
        received.last(),
        previous.first(),
        previous.last(),
    ) {
        let start = first.time().max(old_first.time()).max(floor);
        let end = last.time().min(old_last.time());
        let overlap = |row: &&R| row.time() >= start && row.time() <= end;
        if !received
            .iter()
            .filter(overlap)
            .eq(previous.iter().filter(overlap))
        {
            return Err(format!(
                "fetch {instrument}: conflicting or inconsistent reread of verified observations"
            ));
        }
    }
    Ok(())
}

/// Removes only a proven page-boundary overlap; within-page repeated observations stay intact.
pub fn prepend_page<R: PartialEq>(mut older: Vec<R>, newer: Vec<R>) -> Vec<R> {
    let overlap = (1..=older.len().min(newer.len()))
        .rev()
        .find(|count| older[older.len() - count..] == newer[..*count])
        .unwrap_or(0);
    older.truncate(older.len() - overlap);
    older.extend(newer);
    older
}

fn publish_retained(
    manifest: &GenerationManifest,
    local: &Store,
    destination: &Store,
) -> Result<(), String> {
    let identities = manifest
        .objects
        .iter()
        .map(|object| store::identify(&local.local_path(&object.key).expect("local mirror")))
        .collect::<Result<Vec<_>, _>>()?;
    import::publish_generation(manifest.clone(), &identities, local, destination)?;
    Ok(())
}

/// The retained raw page a coverage entry describes, as a source object.
fn page_object(page: &PageCoverage) -> ObjectRecord {
    ObjectRecord {
        role: ObjectRole::Source,
        path: page.path.clone(),
        key: object_key(&page.sha256),
        bytes: page.bytes,
        sha256: page.sha256.clone(),
        crc32c: None,
        generation: None,
    }
}

/// The data objects of a generation in manifest order: its normalized object, or the listed
/// archive files of an imported bar collection.
fn data_objects(manifest: &GenerationManifest) -> Vec<&ObjectRecord> {
    let role = match manifest.source_kind {
        SourceKind::BarParquet => ObjectRole::Source,
        _ => ObjectRole::Normalized,
    };
    manifest
        .objects
        .iter()
        .filter(|object| object.role == role)
        .collect()
}

/// Every retained row of the baseline and, for bars, the one provider identifier they carried.
fn baseline_rows<R: Row>(
    baseline: &Baseline,
    local: &Store,
    scale: PriceScale,
) -> Result<(Vec<R>, Option<i32>), String> {
    let mut rows = Vec::new();
    let mut symbol_id = None;
    for object in data_objects(&baseline.manifest) {
        let (_, fetched) = verify::fetch(local, object, true)?;
        let observed = R::read(
            &fetched.expect("decoded").path,
            &baseline.manifest,
            scale,
            &mut |row| rows.push(row),
        )?;
        if let Some(observed) = observed {
            if symbol_id.is_some_and(|known| known != observed) {
                return Err(format!(
                    "fetch: {} carries provider identifiers {symbol_id:?} and {observed}",
                    baseline.manifest.generation
                ));
            }
            symbol_id = Some(observed);
        }
    }
    Ok((rows, symbol_id))
}

/// The objects a descendant carries forward: a descendant's own raw pages and provenance, or a
/// seed's manifest and every seed object retained beneath `seed/`, mirrored into the local
/// store so the descendant closure stands alone.
fn carried_objects(baseline: &Baseline, local: &Store) -> Result<Vec<ObjectRecord>, String> {
    if baseline.coverage.is_some() {
        return Ok(baseline
            .manifest
            .objects
            .iter()
            .filter(|object| object.role != ObjectRole::Normalized && object.path != COVERAGE_PATH)
            .cloned()
            .collect());
    }
    let mut carried = Vec::with_capacity(baseline.manifest.objects.len() + 1);
    for object in &baseline.manifest.objects {
        if object.role == ObjectRole::Normalized {
            continue;
        }
        let (_, fetched) = verify::fetch(local, object, true)?;
        let fetched = fetched.expect("decoded objects have a local path");
        let identity = store::identify(&fetched.path)?;
        local.put_new(&object.key, &fetched.path, &identity)?;
        carried.push(import::record(
            ObjectRole::Provenance,
            &format!("{SEED_PREFIX}{}", object.path),
            &identity,
        ));
    }
    let identity = import::retain_bytes(local, &baseline.manifest.to_json(), "seed-manifest")?;
    carried.push(import::record(
        ObjectRole::Provenance,
        &format!("{SEED_PREFIX}ready.json"),
        &identity,
    ));
    Ok(carried)
}

/// One pass over an explicit range without a budget or durable progress: the standalone
/// command's step.
pub fn pass(
    config: &Config,
    broker: &mut dyn MarketDataBroker,
    local: &Store,
    destination: &Store,
    requested: (i64, i64),
    out: &mut dyn Write,
) -> Result<(), String> {
    acquire(
        config,
        broker,
        local,
        destination,
        Requested::Explicit {
            start: requested.0,
            end: requested.1,
        },
        &mut Bounds::none(&SystemClock),
        out,
    )
    .map(|_| ())
}

/// Acquires, verifies, and publishes every history instrument once over `requested` within
/// `bounds`, writing one report line per instrument and returning the typed outcome of each.
pub fn acquire(
    config: &Config,
    broker: &mut dyn MarketDataBroker,
    local: &Store,
    destination: &Store,
    requested: Requested,
    bounds: &mut Bounds<'_>,
    out: &mut dyn Write,
) -> Result<Vec<Outcome>, String> {
    let (plans, history, settings, source_identity) = plan(
        config,
        local,
        requested,
        bounds.resume.as_ref(),
        bounds.declaration,
    )?;
    let server_offset_s = match settings {
        Broker::PocketOption(settings) => i64::from(settings.server_offset_minutes) * 60,
        Broker::Deriv(_) => 0,
    };
    let mut outcomes = Vec::with_capacity(plans.len());
    for plan in plans {
        let native = Native {
            granularity: history.native_granularity,
            scale: plan.definition.price_scale,
            symbol_id: None,
            server_offset_s,
        };
        let outcome = match history.native_granularity {
            NativeGranularity::Tick => acquire_one::<Tick>(
                config,
                history,
                &plan,
                native,
                broker,
                local,
                destination,
                &source_identity,
                bounds,
                out,
            )?,
            NativeGranularity::Bar { .. } => acquire_one::<Bar>(
                config,
                history,
                &plan,
                native,
                broker,
                local,
                destination,
                &source_identity,
                bounds,
                out,
            )?,
        };
        outcomes.push(outcome);
    }
    Ok(outcomes)
}

#[allow(clippy::too_many_arguments)]
fn acquire_one<R: Row>(
    config: &Config,
    history: &History,
    plan: &Plan<'_>,
    mut native: Native,
    broker: &mut dyn MarketDataBroker,
    local: &Store,
    destination: &Store,
    source_identity: &str,
    bounds: &mut Bounds<'_>,
    out: &mut dyn Write,
) -> Result<Outcome, String> {
    let instrument = &plan.instrument;
    let requested = plan.requested;
    let overlap = i64::from(history.overlap_seconds.unwrap_or(0)) * 1_000_000;
    let baseline = plan.baseline.as_ref();
    let seed_lineage = baseline.and_then(|baseline| baseline.seed.clone());
    let previous_verified = baseline.map(Baseline::verified).transpose()?.flatten();
    // A verified suffix after shortfall must not skip the still-unfetched leading interval; a
    // seed contributes its frontier instead of a verified range.
    let baseline_end = match baseline {
        Some(baseline) if baseline.coverage.is_none() => {
            Some(frontier(native.granularity, baseline.last_time()?))
        }
        _ => previous_verified
            .filter(|(start, _)| *start <= requested.0)
            .map(|(_, end)| end),
    };
    let fetch_start = baseline_end.map_or(requested.0, |end| {
        requested.0.max(end.saturating_sub(overlap))
    });
    let coverage_of = |baseline: &Baseline| baseline.coverage.clone().expect("descendant coverage");
    if fetch_start >= requested.1 {
        let prior = baseline
            .filter(|baseline| baseline.coverage.is_some())
            .expect("only prior coverage can exhaust the range");
        publish_retained(&prior.manifest, local, destination)?;
        let coverage = coverage_of(prior);
        let line = report(
            instrument,
            history.role,
            &prior.manifest.generation,
            &coverage,
            prior.manifest.objects.len(),
            0,
            (0.0, 0.0),
        );
        writeln!(out, "{line} no new range (already published)")
            .map_err(|error| format!("cannot write fetch report: {error}"))?;
        return Ok(Outcome {
            instrument: instrument.clone(),
            generation: Some(prior.manifest.generation.clone()),
            coverage,
            pending: false,
            receipts: Vec::new(),
        });
    }
    let started = Instant::now();
    let mut rows: Vec<R> = Vec::new();
    let mut objects = baseline
        .map(|baseline| carried_objects(baseline, local))
        .transpose()?
        .unwrap_or_default();
    let mut pages = baseline
        .and_then(|baseline| baseline.coverage.as_ref())
        .map(|coverage| coverage.pages.clone())
        .unwrap_or_default();
    let mut previous_rows: Vec<R> = Vec::new();
    if let Some(baseline) = baseline {
        let (retained, symbol_id) = baseline_rows::<R>(baseline, local, native.scale)?;
        previous_rows = retained;
        native.symbol_id = symbol_id;
    }
    let mut progress = Progress {
        baseline: baseline.map(|baseline| baseline.manifest.generation.clone()),
        start: time_text(requested.0),
        cutoff: time_text(requested.1),
        pages: Vec::new(),
    };
    let mut retained_pages = bounds
        .resume
        .as_ref()
        .map(|progress| progress.pages.clone())
        .unwrap_or_default()
        .into_iter();
    if bounds.resume.is_none()
        && let Some(persist) = bounds.persist.as_mut()
    {
        persist(&progress)?;
    }
    let mut receipts = Vec::new();
    let mut earliest = None;
    let floor = previous_verified.map_or(fetch_start, |(start, _)| start);
    let mut anchor = Some(requested.1);
    let mut received_end = None;
    let mut requests: u32 = 0;
    // Every received row, assembled across page boundaries before the resume filter, so a
    // repeated observation split between two pages still matches the verified multiplicity.
    let mut received_all: Vec<R> = Vec::new();
    let shortfall = loop {
        // A retained page of the pending intent replays from its bytes; then live requests
        // continue from the durable cursor within this invocation's budget.
        let (page_rows, symbol_id, coverage_page) = if let Some(retained) = retained_pages.next() {
            let key = object_key(&retained.sha256);
            let raw = fs::read(local.local_path(&key).expect("local mirror"))
                .map_err(|error| format!("cannot read {}: {error}", local.uri(&key)))?;
            let (symbol_id, decoded) =
                broker.decode_history(instrument, &raw, native.scale, native.granularity)?;
            if let Some(receipt_time) = &retained.receipt_time {
                receipts.push(PageReceipt {
                    anchor: retained.anchor.clone(),
                    sha256: retained.sha256.clone(),
                    bytes: retained.bytes,
                    rows: retained.rows,
                    receipt_time: receipt_time.clone(),
                });
            }
            (R::unpack(decoded)?, symbol_id, retained)
        } else {
            if bounds.max_pages.is_some_and(|limit| requests >= limit)
                || bounds
                    .deadline_micros
                    .is_some_and(|deadline| bounds.clock.now_micros() >= deadline)
            {
                break Some(Shortfall {
                    reason: BUDGET_SHORTFALL.into(),
                    unresolved: Range::new(
                        fetch_start,
                        earliest.unwrap_or(requested.1).min(requested.1),
                    ),
                });
            }
            requests += 1;
            let page = broker.history_page(instrument, native.scale, anchor, native.granularity)?;
            // The matched response is retained before its rows can be rejected, so a malformed
            // page stays inspectable as a diagnostic object that no manifest names.
            let identity = import::retain_bytes(local, &page.raw, "history-page")?;
            let (symbol_id, decoded) = broker
                .decode_history(instrument, &page.raw, native.scale, native.granularity)
                .map_err(|reason| {
                    format!(
                        "fetch {instrument}: retained page {}: {reason}",
                        identity.sha256
                    )
                })?;
            let page_rows = R::unpack(decoded)?;
            receipts.push(PageReceipt {
                anchor: page.anchor_token.clone(),
                sha256: identity.sha256.clone(),
                bytes: identity.bytes,
                rows: page_rows.len() as u64,
                receipt_time: time_text(page.receipt_micros),
            });
            let coverage_page = PageCoverage {
                path: format!("raw/{}.json", identity.sha256),
                sha256: identity.sha256,
                bytes: identity.bytes,
                anchor: page.anchor_token,
                rows: page_rows.len() as u64,
                first: page_rows.first().map(|row| time_text(row.time())),
                last: page_rows.last().map(|row| time_text(row.time())),
                receipt_time: Some(time_text(page.receipt_micros)),
            };
            (page_rows, symbol_id, coverage_page)
        };
        let mut sequence = R::Sequence::default();
        for row in &page_rows {
            R::accept(&mut sequence, *row)
                .map_err(|error| format!("fetch {instrument}: {error}"))?;
        }
        if let Some(symbol_id) = symbol_id {
            if native.symbol_id.is_some_and(|known| known != symbol_id) {
                return Err(format!(
                    "fetch {instrument}: the provider identifies this instrument as {symbol_id}, but the retained lineage carries {}",
                    native.symbol_id.unwrap_or_default()
                ));
            }
            native.symbol_id = Some(symbol_id);
        }
        received_all = prepend_page(page_rows.clone(), received_all);
        // A page contradicting the retained rows is never checkpointed. The oldest received
        // time is excluded until the next page can complete its multiplicity.
        let boundary = received_all
            .first()
            .map_or(floor, |row| row.time().saturating_add(1).max(floor));
        check_verified_overlap(instrument, &previous_rows, &received_all, boundary)?;
        let first = page_rows.first().map(Row::time);
        if let Some(last_row) = page_rows.last().filter(|row| row.time() >= fetch_start) {
            // Coverage ends at the last complete row, or where a row straddles the cutoff: a
            // bar reaching past the cutoff proves nothing after its start.
            let end = match page_rows.iter().find(|row| row.end() > requested.1) {
                Some(beyond) => beyond.time().min(requested.1),
                None => last_row.end(),
            };
            received_end = Some(received_end.unwrap_or(i64::MIN).max(end));
        }
        if !objects
            .iter()
            .any(|object| object.path == coverage_page.path)
        {
            objects.push(page_object(&coverage_page));
            pages.push(coverage_page.clone());
        }
        if !progress
            .pages
            .iter()
            .any(|page| page.sha256 == coverage_page.sha256)
        {
            progress.pages.push(coverage_page);
            if let Some(persist) = bounds.persist.as_mut() {
                persist(&progress)?;
            }
        }
        let Some(first) = first else {
            break Some(Shortfall {
                reason: "empty_page".into(),
                unresolved: Range::new(
                    fetch_start,
                    earliest.unwrap_or(requested.1).min(requested.1),
                ),
            });
        };
        if earliest.is_some_and(|previous| first >= previous) {
            // Contradicting prices are still errors, even in a non-progressing response.
            for new in &page_rows {
                if rows.iter().any(|old: &R| old.conflicts(new)) {
                    return Err(format!(
                        "fetch {instrument}: conflicting prices at one time"
                    ));
                }
            }
            break Some(Shortfall {
                reason: "no_progress".into(),
                unresolved: Range::new(
                    fetch_start,
                    earliest.unwrap_or(requested.1).min(requested.1),
                ),
            });
        }
        earliest = Some(first);
        let kept = page_rows
            .into_iter()
            .filter(|row| {
                row.time() >= fetch_start && row.time() < requested.1 && row.end() <= requested.1
            })
            .collect();
        rows = prepend_page(kept, rows);
        if first <= fetch_start {
            break None;
        }
        anchor = Some(first);
    };
    check_verified_overlap(instrument, &previous_rows, &received_all, floor)?;
    let new_count = rows.len();
    let pending = shortfall
        .as_ref()
        .is_some_and(|shortfall| shortfall.reason == BUDGET_SHORTFALL);
    if pending && progress.pages.is_empty() {
        // The budget expired before the first page: nothing was acquired, so nothing is
        // published; the intent stays pending for the next invocation.
        return Ok(Outcome {
            instrument: instrument.clone(),
            generation: None,
            coverage: HistoryCoverage {
                schema_version: 1,
                source_identity: source_identity.to_string(),
                broker: instrument.broker.to_string(),
                provider_symbol: instrument.provider_symbol.to_string(),
                role: history.role,
                requested: Range::new(requested.0, requested.1),
                verified: None,
                actual: None,
                rows: 0,
                pages: Vec::new(),
                shortfall,
                tail_shortfall: None,
                native_granularity: native.granularity,
                seed: seed_lineage,
            },
            pending,
            receipts,
        });
    }

    // Rows outside the retained span are the only additions a consistent reread can carry: the
    // rows inside it were just proven identical to the retained ones.
    let extends_prior = match (previous_rows.first(), previous_rows.last()) {
        (Some(old_first), Some(old_last)) => rows
            .iter()
            .any(|row| row.time() < old_first.time() || row.time() > old_last.time()),
        _ => !rows.is_empty(),
    };
    if let Some(last) = previous_rows.last() {
        let suffix = rows.split_off(rows.partition_point(|row| row.time() <= last.time()));
        rows = if rows
            .first()
            .zip(previous_rows.first())
            .is_some_and(|(new, old)| new.time() <= old.time())
        {
            prepend_page(rows, previous_rows)
        } else {
            previous_rows
        };
        rows = prepend_page(rows, suffix);
    }
    let newly_verified = (new_count > 0).then(|| {
        (
            if shortfall.is_none() {
                fetch_start
            } else {
                earliest
                    .unwrap_or(requested.1)
                    .max(fetch_start)
                    .min(requested.1)
            },
            received_end.expect("received rows"),
        )
    });
    let verified_bounds = match (previous_verified, newly_verified) {
        (Some((old_start, old_end)), Some((start, end)))
            if start <= old_end && old_start <= end =>
        {
            Some((old_start.min(start), old_end.max(end)))
        }
        (Some(prior), _) => Some(prior),
        (None, new) => new,
    };
    let verified = verified_bounds.map(|(start, end)| Range::new(start, end));
    let tail = verified_bounds
        .map(|(_, end)| end)
        .filter(|end| *end < requested.1)
        .map(|end| Shortfall {
            reason: TAIL_SHORTFALL.into(),
            unresolved: Range::new(end.max(fetch_start), requested.1),
        });
    let tail_shortfall = shortfall.as_ref().and(tail.clone());
    let shortfall = shortfall.or(tail);
    let mut coverage = HistoryCoverage {
        schema_version: 1,
        source_identity: source_identity.to_string(),
        broker: instrument.broker.to_string(),
        provider_symbol: instrument.provider_symbol.to_string(),
        role: history.role,
        requested: Range::new(requested.0, requested.1),
        verified,
        actual: rows.first().zip(rows.last()).map(|(first, last)| Actual {
            first: time_text(first.time()),
            last: time_text(last.time()),
        }),
        rows: rows.len() as u64,
        pages,
        shortfall,
        tail_shortfall,
        native_granularity: native.granularity,
        seed: seed_lineage,
    };
    let prior = baseline.filter(|baseline| baseline.coverage.is_some());
    let no_change = !extends_prior
        && prior.is_some_and(|prior| {
            let previous = coverage_of(prior);
            previous.shortfall == coverage.shortfall
                && previous.tail_shortfall == coverage.tail_shortfall
                && previous.verified == coverage.verified
        });
    if no_change {
        let prior = prior.expect("no change has a prior generation");
        coverage = coverage_of(prior);
        coverage.requested = Range::new(requested.0, requested.1);
        publish_retained(&prior.manifest, local, destination)?;
        let line = report(
            instrument,
            history.role,
            &prior.manifest.generation,
            &coverage,
            prior.manifest.objects.len(),
            0,
            (started.elapsed().as_secs_f64(), 0.0),
        );
        writeln!(out, "{line} (no new data)").map_err(|error| error.to_string())?;
        return Ok(Outcome {
            instrument: instrument.clone(),
            generation: Some(prior.manifest.generation.clone()),
            coverage,
            pending,
            receipts,
        });
    }
    let coverage_bytes = json_bytes(&coverage)?;
    let coverage_identity = import::retain_bytes(local, &coverage_bytes, "history-coverage")?;
    objects.push(import::record(
        ObjectRole::Provenance,
        COVERAGE_PATH,
        &coverage_identity,
    ));
    if rows.is_empty() {
        // Existing dataset manifests require actual first/last events; retain the unresolved receipt without a ready manifest.
        writeln!(
            out,
            "{} (no new data)",
            report(
                instrument,
                history.role,
                "none",
                &coverage,
                objects.len(),
                0,
                (started.elapsed().as_secs_f64(), 0.0)
            )
        )
        .map_err(|error| error.to_string())?;
        return Ok(Outcome {
            instrument: instrument.clone(),
            generation: None,
            coverage,
            pending,
            receipts,
        });
    }
    let (price_representation, time_unit, capability, interval, normalized_path, scale) =
        match native.granularity {
            NativeGranularity::Tick => (
                PriceRepresentation::IntegerUnits {
                    scale: native.scale,
                },
                TimeUnit::Microsecond,
                Capability::Ticks,
                None,
                TICK_OBJECT_PATH,
                Some(native.scale),
            ),
            NativeGranularity::Bar { .. } => (
                PriceRepresentation::BinaryFloat64,
                TimeUnit::Second,
                Capability::Bars,
                Some(IntervalContract::five_second("parquet_metadata")),
                BAR_OBJECT_PATH,
                None,
            ),
        };
    let generation = generation_id(
        instrument,
        SourceKind::BrokerHistory,
        history.role,
        scale,
        &objects,
    );
    let temporary = import::temporary_path(local, &generation)?;
    let summary = R::write(&temporary, instrument, &native, rows)?;
    let normalized = store::identify(&temporary)?;
    local.put_new(&object_key(&normalized.sha256), &temporary, &normalized)?;
    fs::remove_file(&temporary)
        .map_err(|error| format!("cannot remove {}: {error}", temporary.display()))?;
    objects.push(import::record(
        ObjectRole::Normalized,
        normalized_path,
        &normalized,
    ));
    let identities: Vec<ObjectIdentity> = objects
        .iter()
        .map(|object| store::identify(&local.local_path(&object.key).expect("local mirror")))
        .collect::<Result<_, _>>()?;
    let (first_event_time, last_event_time) = archive::coverage(&summary)?;
    let manifest = GenerationManifest {
        schema_version: MANIFEST_SCHEMA_VERSION,
        generation: generation.clone(),
        broker: instrument.broker.clone(),
        provider_symbol: instrument.provider_symbol.clone(),
        instrument: instrument.to_string(),
        role: history.role,
        source_kind: SourceKind::BrokerHistory,
        native_granularity: native.granularity,
        time_unit,
        price_representation,
        coverage: Coverage {
            first_event_time,
            last_event_time,
        },
        row_count: summary.rows,
        capabilities: vec![capability],
        config_hash: config.content_hash(),
        code_revision: CODE_REVISION.into(),
        inputs: objects
            .iter()
            .filter(|object| object.role == ObjectRole::Source)
            .map(|object| Input {
                path: object.path.clone(),
                bytes: object.bytes,
                sha256: object.sha256.clone(),
            })
            .collect(),
        interval,
        objects,
    };
    let fetched = started.elapsed();
    let publishing = Instant::now();
    let published = import::publish_generation(manifest, &identities, local, destination)?;
    let line = report(
        instrument,
        history.role,
        &generation,
        &coverage,
        published.manifest.objects.len(),
        published.reused,
        (fetched.as_secs_f64(), publishing.elapsed().as_secs_f64()),
    );
    writeln!(
        out,
        "{line}{}",
        if published.already_published {
            " (already published)"
        } else {
            ""
        }
    )
    .map_err(|error| format!("cannot write fetch report: {error}"))?;
    out.flush()
        .map_err(|error| format!("cannot flush fetch report: {error}"))?;
    Ok(Outcome {
        instrument: instrument.clone(),
        generation: Some(generation),
        coverage,
        pending,
        receipts,
    })
}
fn report(
    instrument: &InstrumentId,
    role: DatasetRole,
    generation: &str,
    coverage: &HistoryCoverage,
    objects: usize,
    reused: usize,
    elapsed: (f64, f64),
) -> String {
    let (fetch, publish) = elapsed;
    let verified = coverage
        .verified
        .as_ref()
        .map(|r| format!("{} {}", r.start, r.end))
        .unwrap_or("none none".into());
    format!(
        "fetch {role} {instrument} generation {generation} requested {} {} verified {verified} rows {} pages {} objects {objects} reused {reused} shortfall {} [fetch {fetch:.3} publish {publish:.3}]",
        coverage.requested.start,
        coverage.requested.end,
        coverage.rows,
        coverage.pages.len(),
        coverage
            .shortfall
            .as_ref()
            .map_or("none", |s| s.reason.as_str())
    )
}
pub(crate) fn json_bytes(value: &impl Serialize) -> Result<Vec<u8>, String> {
    let mut bytes = serde_json::to_vec_pretty(value).map_err(|error| error.to_string())?;
    bytes.push(b'\n');
    Ok(bytes)
}

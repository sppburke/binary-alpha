use crate::archive::{self, TICK_OBJECT_PATH};
use crate::broker::{self, Clock, MarketDataBroker, SystemClock};
use crate::import::{self, CODE_REVISION};
use crate::store::{self, ObjectIdentity, Store};
use crate::verify;
use binary_alpha_engine::config::Config;
use binary_alpha_engine::dataset::{
    Capability, Coverage, DatasetRole, GenerationManifest, Input, MANIFEST_SCHEMA_VERSION,
    NativeGranularity, ObjectRole, PriceRepresentation, SourceKind, TimeUnit, generation_id,
    object_key,
};
use binary_alpha_engine::market::{
    InstrumentId, PriceScale, Tick, TickSequence, format_event_time_micros as time_text,
    parse_event_time_micros as time,
};
use serde::{Deserialize, Serialize};
use std::fs;
use std::io::Write;
use std::path::Path;
use std::time::Instant;

pub const COVERAGE_PATH: &str = "provenance/coverage.json";
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
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Shortfall {
    pub reason: String,
    pub unresolved: Range,
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
}
#[derive(Debug, Clone, Copy)]
pub enum PassLimit {
    Unbounded,
    Exactly(usize),
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
        pass(config, broker, local, destination, (start, end), out)?;
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

struct Prior {
    manifest: GenerationManifest,
    coverage: HistoryCoverage,
}
fn prior(
    local: &Store,
    instrument: &InstrumentId,
    role: DatasetRole,
    scale: PriceScale,
    source_identity: &str,
) -> Result<Option<Prior>, String> {
    let root = local
        .local_path("manifests")
        .ok_or("fetch: local mirror must be a filesystem store")?;
    let entries = match fs::read_dir(&root) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(format!("cannot inspect {}: {error}", root.display())),
    };
    let mut selected: Option<Prior> = None;
    for entry in entries {
        let path = entry
            .map_err(|error| format!("cannot inspect manifests: {error}"))?
            .path()
            .join("ready.json");
        if !path.is_file() {
            continue;
        }
        let bytes =
            fs::read(&path).map_err(|error| format!("cannot read {}: {error}", path.display()))?;
        if verify::manifest_kind(&bytes)?.is_some() {
            continue;
        }
        let manifest = GenerationManifest::from_json(&bytes)?;
        if manifest.source_kind != SourceKind::BrokerHistory
            || manifest.instrument != instrument.to_string()
            || manifest.role != role
        {
            continue;
        }
        if manifest.price_representation != (PriceRepresentation::IntegerUnits { scale }) {
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
        if coverage.source_identity != source_identity {
            continue;
        }
        if coverage.schema_version != 1
            || coverage.broker != instrument.broker.as_str()
            || coverage.provider_symbol != instrument.provider_symbol.as_str()
            || coverage.role != role
            || coverage.rows != manifest.row_count
        {
            return Err("fetch: prior coverage identity mismatch".into());
        }
        let rank = |coverage: &HistoryCoverage| -> Result<(i64, std::cmp::Reverse<i64>), String> {
            let (start, end) = coverage
                .verified
                .as_ref()
                .map(Range::bounds)
                .transpose()?
                .unwrap_or((i64::MAX, i64::MIN));
            Ok((end, std::cmp::Reverse(start)))
        };
        if selected
            .as_ref()
            .map(|prior| rank(&prior.coverage))
            .transpose()?
            .is_none_or(|old| rank(&coverage).is_ok_and(|new| new > old))
        {
            selected = Some(Prior { manifest, coverage });
        }
    }
    if let Some(prior) = &selected {
        verify::run(&local.uri(&prior.manifest.key()))?;
    }
    Ok(selected)
}

fn check_verified_overlap(
    instrument: &InstrumentId,
    previous: &[Tick],
    received: &[Tick],
) -> Result<(), String> {
    if let (Some(first), Some(last), Some(old_first), Some(old_last)) = (
        received.first(),
        received.last(),
        previous.first(),
        previous.last(),
    ) {
        let start = first.event_time_micros.max(old_first.event_time_micros);
        let end = last.event_time_micros.min(old_last.event_time_micros);
        let overlap = |row: &&Tick| row.event_time_micros >= start && row.event_time_micros <= end;
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
pub fn prepend_page(mut older: Vec<Tick>, newer: Vec<Tick>) -> Vec<Tick> {
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

pub fn pass(
    config: &Config,
    broker: &mut dyn MarketDataBroker,
    local: &Store,
    destination: &Store,
    requested: (i64, i64),
    out: &mut dyn Write,
) -> Result<(), String> {
    let history = config
        .history
        .as_ref()
        .ok_or("fetch: the configuration declares no history table")?;
    history.validate()?;
    let settings = config
        .brokers
        .iter()
        .find(|broker| broker.id() == &history.broker)
        .ok_or("fetch: broker is not declared")?;
    let source_identity = broker::source_identity(settings);
    if requested.0 >= requested.1 {
        return Err("fetch: requested start must precede end".into());
    }
    for symbol in &history.instruments {
        let instrument = InstrumentId {
            broker: history.broker.clone(),
            provider_symbol: symbol.clone(),
        };
        let definition = config
            .instrument(&instrument, NativeGranularity::Tick)
            .ok_or("fetch: instrument is not declared")?;
        let prior = prior(
            local,
            &instrument,
            history.role,
            definition.price_scale,
            &source_identity,
        )?;
        let previous_verified = prior
            .as_ref()
            .and_then(|prior| prior.coverage.verified.as_ref())
            .map(Range::bounds)
            .transpose()?;
        // A verified suffix after shortfall must not skip the still-unfetched leading interval.
        let fetch_start = previous_verified
            .filter(|(start, _)| *start <= requested.0)
            .map_or(requested.0, |(_, end)| requested.0.max(end));
        if fetch_start >= requested.1 {
            let prior = prior
                .as_ref()
                .expect("only prior coverage can exhaust the range");
            publish_retained(&prior.manifest, local, destination)?;
            let line = report(
                &instrument,
                history.role,
                &prior.manifest.generation,
                &prior.coverage,
                prior.manifest.objects.len(),
                0,
                (0.0, 0.0),
            );
            writeln!(out, "{line} no new range (already published)")
                .map_err(|error| format!("cannot write fetch report: {error}"))?;
            continue;
        }
        let started = Instant::now();
        let mut rows = Vec::new();
        let mut objects = prior
            .as_ref()
            .map(|prior| {
                prior
                    .manifest
                    .objects
                    .iter()
                    .filter(|o| o.role == ObjectRole::Source)
                    .cloned()
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        let mut pages = prior
            .as_ref()
            .map(|prior| prior.coverage.pages.clone())
            .unwrap_or_default();
        let mut previous_rows = Vec::new();
        if let Some(prior) = &prior {
            let object = prior
                .manifest
                .objects
                .iter()
                .find(|o| o.role == ObjectRole::Normalized)
                .expect("validated normalized object");
            let (_, fetched) = verify::fetch(local, object, true)?;
            archive::read_ticks_with(
                &fetched.expect("decoded").path,
                definition.price_scale,
                |row| {
                    previous_rows.push(row);
                    Ok(())
                },
            )?;
        }
        let mut earliest = None;
        let mut anchor = Some(requested.1);
        let mut received_end = None;
        // Every received row, assembled across page boundaries before the resume filter, so a
        // repeated observation split between two pages still matches the verified multiplicity.
        let mut received_all: Vec<Tick> = Vec::new();
        let shortfall = loop {
            let page = broker.history_page(&instrument, definition.price_scale, anchor)?;
            let mut sequence = TickSequence::default();
            for row in &page.rows {
                sequence
                    .accept(*row)
                    .map_err(|error| format!("fetch {instrument}: {error}"))?;
            }
            received_all = prepend_page(page.rows.clone(), received_all);
            let first = page.rows.first().map(|row| row.event_time_micros);
            let last = page.rows.last().map(|row| row.event_time_micros);
            if let Some(last) = last.filter(|last| *last >= fetch_start) {
                received_end = Some(
                    received_end
                        .unwrap_or(i64::MIN)
                        .max(last.saturating_add(1).min(requested.1)),
                );
            }
            let identity = import::retain_bytes(local, &page.raw, "history-page")?;
            let path = format!("raw/{}.json", identity.sha256);
            if !objects.iter().any(|object| object.path == path) {
                objects.push(import::record(ObjectRole::Source, &path, &identity));
                pages.push(PageCoverage {
                    path,
                    sha256: identity.sha256,
                    bytes: identity.bytes,
                    anchor: page.anchor_token,
                    rows: page.rows.len() as u64,
                    first: first.map(time_text),
                    last: last.map(time_text),
                });
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
                for new in &page.rows {
                    if rows.iter().any(|old: &Tick| {
                        old.event_time_micros == new.event_time_micros
                            && old.price_units != new.price_units
                    }) {
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
            let kept = page
                .rows
                .into_iter()
                .filter(|row| {
                    row.event_time_micros >= fetch_start && row.event_time_micros < requested.1
                })
                .collect();
            rows = prepend_page(kept, rows);
            if first <= fetch_start {
                break None;
            }
            anchor = Some(first);
        };
        check_verified_overlap(&instrument, &previous_rows, &received_all)?;
        let new_count = rows.len();

        let repeats_prior = rows == previous_rows;
        if let Some(last) = previous_rows.last() {
            let suffix = rows.split_off(
                rows.partition_point(|row| row.event_time_micros <= last.event_time_micros),
            );
            rows = if rows
                .first()
                .zip(previous_rows.first())
                .is_some_and(|(new, old)| new.event_time_micros <= old.event_time_micros)
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
                reason: "unresolved_tail".into(),
                unresolved: Range::new(end.max(fetch_start), requested.1),
            });
        let tail_shortfall = shortfall.as_ref().and(tail.clone());
        let shortfall = shortfall.or(tail);
        let mut coverage = HistoryCoverage {
            schema_version: 1,
            source_identity: source_identity.clone(),
            broker: instrument.broker.to_string(),
            provider_symbol: symbol.to_string(),
            role: history.role,
            requested: Range::new(requested.0, requested.1),
            verified,
            actual: rows.first().zip(rows.last()).map(|(first, last)| Actual {
                first: time_text(first.event_time_micros),
                last: time_text(last.event_time_micros),
            }),
            rows: rows.len() as u64,
            pages,
            shortfall,
            tail_shortfall,
        };
        let no_change = (new_count == 0 || repeats_prior)
            && prior.as_ref().is_some_and(|prior| {
                prior.coverage.shortfall == coverage.shortfall
                    && prior.coverage.tail_shortfall == coverage.tail_shortfall
                    && prior.coverage.verified == coverage.verified
            });
        if no_change {
            let prior = prior.as_ref().expect("no change has a prior generation");
            coverage = prior.coverage.clone();
            coverage.requested = Range::new(requested.0, requested.1);
            publish_retained(&prior.manifest, local, destination)?;
            let line = report(
                &instrument,
                history.role,
                &prior.manifest.generation,
                &coverage,
                prior.manifest.objects.len(),
                0,
                (started.elapsed().as_secs_f64(), 0.0),
            );
            writeln!(out, "{line} (no new data)").map_err(|error| error.to_string())?;
            continue;
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
                    &instrument,
                    history.role,
                    "none",
                    &coverage,
                    objects.len(),
                    0,
                    (started.elapsed().as_secs_f64(), 0.0)
                )
            )
            .map_err(|error| error.to_string())?;
            continue;
        }
        let generation = generation_id(
            &instrument,
            SourceKind::BrokerHistory,
            history.role,
            Some(definition.price_scale),
            &objects,
        );
        let temporary = import::temporary_path(local, &generation)?;
        let summary = archive::write_ticks(
            &temporary,
            &instrument,
            definition.price_scale,
            rows.into_iter().map(Ok),
        )?;
        let normalized = store::identify(&temporary)?;
        local.put_new(&object_key(&normalized.sha256), &temporary, &normalized)?;
        fs::remove_file(&temporary)
            .map_err(|error| format!("cannot remove {}: {error}", temporary.display()))?;
        objects.push(import::record(
            ObjectRole::Normalized,
            TICK_OBJECT_PATH,
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
            provider_symbol: symbol.clone(),
            instrument: instrument.to_string(),
            role: history.role,
            source_kind: SourceKind::BrokerHistory,
            native_granularity: NativeGranularity::Tick,
            time_unit: TimeUnit::Microsecond,
            price_representation: PriceRepresentation::IntegerUnits {
                scale: definition.price_scale,
            },
            coverage: Coverage {
                first_event_time,
                last_event_time,
            },
            row_count: summary.rows,
            capabilities: vec![Capability::Ticks],
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
            interval: None,
            objects,
        };
        let fetched = started.elapsed();
        let publishing = Instant::now();
        let published = import::publish_generation(manifest, &identities, local, destination)?;
        let line = report(
            &instrument,
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
    }
    Ok(())
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

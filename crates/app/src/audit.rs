//! `binary-alpha data audit`: feed one published generation, in order, through the
//! `InstrumentStream` its configuration maps it to, then retain and publish the profile, one
//! candle object per configured stream, and the stream manifest last.

use std::fs;
use std::io::Write;
use std::path::Path;
use std::time::Instant;

use binary_alpha_engine::dataset::{
    DatasetRole, GenerationManifest, ObjectRecord, ObjectRole, manifest_key,
};
use binary_alpha_engine::market::{InstrumentId, format_event_time_micros};
use binary_alpha_engine::stream::{
    InstrumentStream, Observation, PROFILE_OBJECT_PATH, STREAM_MANIFEST_KIND,
    STREAM_SCHEMA_VERSION, Source, StreamManifest, StreamSummary, stream_generation_id_with_layout,
};

use crate::archive::CandleWriter;
use crate::import::{self, CODE_REVISION};
use crate::store::{self, ObjectIdentity, Put, Store};
use crate::verify;
use binary_alpha_engine::config::{Config, ManifestUri};
use binary_alpha_engine::research::Access;

/// Runs the audit of the generation whose ready manifest is at `uri` under the configuration at
/// `config_path`, writing one report line to `out`.
pub fn run(config_path: &Path, uri: &str, out: &mut dyn Write) -> Result<(), String> {
    let config = crate::load_config(config_path)?;
    let base = config_path.parent().unwrap_or(Path::new("."));
    let historical_dir = base.join(config.storage.historical_data_dir.as_path());
    fs::create_dir_all(&historical_dir)
        .map_err(|error| format!("cannot create {}: {error}", historical_dir.display()))?;
    let local = Store::filesystem(&historical_dir);
    let destination = Store::open(&config.storage.publication_uri)?;
    let declaration = crate::research::declaration(&config)?;
    let access = Access {
        declaration: declaration.as_ref(),
        certification: None,
    };
    let line = audit(&config, uri, &local, &destination, access)?.report;
    writeln!(out, "{line}")
        .and_then(|()| out.flush())
        .map_err(|error| format!("cannot write the report: {error}"))
}

/// One published stream generation and the report line of the command.
pub(crate) struct Audited {
    pub(crate) generation: String,
    pub(crate) report: String,
}

/// The typed audit every caller uses: the target needs a read permit before it is opened; then
/// its generation streams through the configured instrument and publishes its profile,
/// candles, and stream manifest; a completed identical generation is reused.
pub(crate) fn audit(
    config: &Config,
    uri: &str,
    local: &Store,
    destination: &Store,
    access: Access<'_>,
) -> Result<Audited, String> {
    let target: ManifestUri = uri.parse()?;
    access.permit(None, target.generation())?;
    let (source_store, source_key) = verify::open(uri)?;
    let mut bytes = Vec::new();
    source_store.read_to(&source_key, None, &mut bytes)?;
    if let Some(kind) = verify::manifest_kind(&bytes)? {
        return Err(format!(
            "{uri} is a `{kind}` manifest, not a dataset ready manifest"
        ));
    }
    let manifest =
        GenerationManifest::from_json(&bytes).map_err(|error| format!("{uri}: {error}"))?;
    if manifest.key() != source_key {
        return Err(format!(
            "{uri} holds the manifest of generation {}",
            manifest.generation
        ));
    }
    if manifest.role == DatasetRole::Holdout {
        return Err(format!(
            "{uri} is a holdout generation; research never audits holdout data, and certification is a separate authorization"
        ));
    }
    let id = InstrumentId {
        broker: manifest.broker.clone(),
        provider_symbol: manifest.provider_symbol.clone(),
    };
    let instrument = config
        .instrument(&id, manifest.native_granularity)
        .ok_or_else(|| {
            format!("no configured instrument maps {id}; an instrument is never defaulted")
        })?;
    let mut stream = InstrumentStream::new(instrument, Source::from_manifest(&manifest))?;
    let generation = stream_generation_id_with_layout(
        &manifest.generation,
        &instrument.canonical_toml(),
        manifest.layout,
    );
    let key = manifest_key(&generation);

    let streaming = Instant::now();
    let mut writers = Vec::with_capacity(instrument.candles.len());
    let mut temporaries = Vec::with_capacity(instrument.candles.len() + 1);
    for (index, spec) in instrument
        .candles
        .iter()
        .enumerate()
        .filter(|_| manifest.layout.is_none())
    {
        let path = import::temporary_path(local, &format!("audit-{generation}-{index}"))?;
        writers.push(CandleWriter::create(
            &path,
            &id,
            instrument.price_scale,
            spec.duration_seconds,
            spec.offset_seconds,
        )?);
        temporaries.push(path);
    }
    let mut daily_writers: Vec<_> = instrument
        .candles
        .iter()
        .map(|_| DailyCandles::default())
        .collect();
    let mut finalized = Vec::new();
    let mut push = |observation: Observation| -> Result<(), String> {
        stream
            .push(observation, &mut finalized)
            .map_err(|rejection| format!("{id}: {rejection}"))?;
        for (index, candle) in finalized.drain(..) {
            if manifest.layout.is_some() {
                daily_writers[index].push(
                    candle,
                    local,
                    &generation,
                    &id,
                    instrument.price_scale,
                    &instrument.candles[index],
                )?;
            } else {
                writers[index].push(&candle)?;
            }
        }
        Ok(())
    };
    feed_generation(&source_store, &manifest, instrument.price_scale, &mut push)?;
    let profile = stream.profile();
    if profile.observations != manifest.row_count
        || profile.coverage.as_ref() != Some(&manifest.coverage)
    {
        return Err(format!(
            "{uri}: observed {} records from {:?}, but the manifest records {} rows from {} to {}; nothing was published",
            profile.observations,
            profile.coverage,
            manifest.row_count,
            manifest.coverage.first_event_time,
            manifest.coverage.last_event_time
        ));
    }
    let mut streams = Vec::with_capacity(writers.len());
    for (writer, spec) in writers.into_iter().zip(&instrument.candles) {
        let (rows, first_open, last_close) = writer.finish()?;
        streams.push(StreamSummary {
            duration_seconds: spec.duration_seconds,
            offset_seconds: spec.offset_seconds,
            rows,
            first_open_time: first_open.map(format_event_time_micros),
            last_close_time: last_close.map(format_event_time_micros),
        });
    }
    let mut day_inventory = Vec::new();
    let mut paths: Vec<_> = streams
        .iter()
        .map(|summary| StreamSummary::object_path(summary.duration_seconds, summary.offset_seconds))
        .collect();
    if manifest.layout.is_some() {
        for (index, mut writer) in daily_writers.into_iter().enumerate() {
            let spec = &instrument.candles[index];
            writer.flush(local, &generation, &id, instrument.price_scale, spec)?;
            let pending = pending_open(&profile, index)?;
            let mut dates: std::collections::BTreeSet<_> = manifest
                .day_inventory
                .iter()
                .filter(|d| d.family == DayFamily::Observations)
                .map(|d| d.date.clone())
                .collect();
            dates.extend(writer.days.keys().cloned());
            if let Some(open) = pending {
                dates.insert(date(open));
            }
            streams.push(writer.summary(spec));
            for date in dates {
                let finalized_at = writer.days.get(&date).map(|(_, _, at)| *at);
                let mut day =
                    candle_day(&date, spec, &manifest.day_inventory, pending, finalized_at)?;
                let (temporary, data) = match writer.days.remove(&date) {
                    Some((path, data, _)) => (path, data),
                    None if day.state == DayState::EmptyKnown => {
                        day_inventory.push(day);
                        continue;
                    }
                    None => {
                        let path = import::temporary_path(
                            local,
                            &format!(
                                "audit-{generation}-{}-{}-{date}",
                                spec.duration_seconds, spec.offset_seconds
                            ),
                        )?;
                        let summary = crate::daily::write_candles(
                            &path,
                            &date,
                            &id,
                            instrument.price_scale,
                            spec.duration_seconds,
                            spec.offset_seconds,
                            [Vec::<Candle>::new()],
                        )?;
                        (path, summary)
                    }
                };
                day.rows = data.rows;
                day.first_time = data.first_event_micros.map(format_event_time_micros);
                day.last_time = data.last_event_micros.map(format_event_time_micros);
                if day.state == DayState::EmptyKnown && day.rows > 0 {
                    day.state = DayState::Complete;
                }
                day.object = Some(binary_alpha_engine::dataset::object_key(
                    &store::identify(&temporary)?.sha256,
                ));
                paths.push(day.logical_path()?);
                temporaries.push(temporary);
                day_inventory.push(day);
            }
        }
    }
    let profile_path = import::temporary_path(local, &format!("audit-{generation}-profile"))?;
    fs::write(&profile_path, profile.to_json())
        .map_err(|error| format!("cannot write {}: {error}", profile_path.display()))?;
    temporaries.insert(0, profile_path);
    let streamed = streaming.elapsed();

    let publishing = Instant::now();
    let identities: Vec<ObjectIdentity> = temporaries
        .iter()
        .map(|path| store::identify(path))
        .collect::<Result<_, _>>()?;
    paths.insert(0, PROFILE_OBJECT_PATH.to_string());
    let mut objects: Vec<ObjectRecord> = paths
        .into_iter()
        .zip(&identities)
        .map(|(path, identity)| import::record(ObjectRole::Normalized, &path, identity))
        .collect();
    let mut reused = 0;
    for ((object, identity), temporary) in objects.iter_mut().zip(&identities).zip(&temporaries) {
        local.put_new(&object.key, temporary, identity)?;
        let put = destination.put_new(&object.key, temporary, identity)?;
        if let Put::Reused(_) = put {
            reused += 1;
        }
        object.crc32c = put.object().crc32c;
        object.generation = put.object().generation;
        fs::remove_file(temporary)
            .map_err(|error| format!("cannot remove {}: {error}", temporary.display()))?;
    }
    let candles: u64 = streams.iter().map(|summary| summary.rows).sum();
    let stream_manifest = StreamManifest {
        layout: manifest.layout,
        day_inventory,
        kind: STREAM_MANIFEST_KIND.to_string(),
        schema_version: STREAM_SCHEMA_VERSION,
        generation: generation.clone(),
        broker: id.broker.clone(),
        provider_symbol: id.provider_symbol.clone(),
        instrument: id.to_string(),
        role: manifest.role,
        source_generation: manifest.generation.clone(),
        source_kind: manifest.source_kind,
        definition: instrument.clone(),
        config_hash: config.content_hash(),
        code_revision: CODE_REVISION.to_string(),
        observations: profile.observations,
        coverage: profile.coverage.clone(),
        streams,
        objects,
    };
    StreamManifest::from_json(&stream_manifest.to_json())?;
    let report = format!(
        "audited {id} {} generation {generation} from {} observations {} candles {candles} objects {} reused {reused}",
        manifest.role,
        manifest.generation,
        profile.observations,
        stream_manifest.objects.len()
    );
    let committed = match destination.head(&key)? {
        Some(_) => {
            let mut bytes = Vec::new();
            destination.read_to(&key, None, &mut bytes)?;
            let committed = StreamManifest::from_json(&bytes)
                .map_err(|error| format!("{}: {error}", destination.uri(&key)))?;
            if !same_result(&committed, &stream_manifest, &identities) {
                return Err(format!(
                    "{} records a different generation, object set, observation count, coverage, or streams than this audit produced",
                    destination.uri(&key)
                ));
            }
            bytes
        }
        None => stream_manifest.to_json(),
    };
    let temporary = import::temporary_path(local, &format!("manifest-{generation}"))?;
    fs::write(&temporary, &committed)
        .map_err(|error| format!("cannot write {}: {error}", temporary.display()))?;
    let identity = store::identify(&temporary)?;
    let put = destination.put_new(&key, &temporary, &identity)?;
    local.put_new(&key, &temporary, &identity)?;
    fs::remove_file(&temporary)
        .map_err(|error| format!("cannot remove {}: {error}", temporary.display()))?;
    let published = publishing.elapsed();
    let line = match put {
        Put::Reused(_) => format!("{report} (already published)"),
        Put::Created(_) => format!(
            "{report} [stream {:.3}s publish {:.3}s]",
            streamed.as_secs_f64(),
            published.as_secs_f64()
        ),
    };
    Ok(Audited {
        generation,
        report: line,
    })
}

/// Verifies and decodes every data object of a published generation in manifest order through
/// the Phase 02 readers, handing every record to `push` as a stream observation.
pub(crate) fn feed_generation(
    source_store: &Store,
    manifest: &GenerationManifest,
    price_scale: binary_alpha_engine::market::PriceScale,
    push: &mut dyn FnMut(Observation) -> Result<(), String>,
) -> Result<(), String> {
    crate::daily::read_generation(source_store, manifest, |row| {
        push(row.observation(price_scale)?)
    })?;
    Ok(())
}

/// A committed stream manifest describes this audit's result when it names the same
/// generation, role, observations, coverage, and streams, and the same objects under the
/// rule shared with dataset generations.
fn same_result(
    committed: &StreamManifest,
    fresh: &StreamManifest,
    identities: &[ObjectIdentity],
) -> bool {
    committed.generation == fresh.generation
        && committed.role == fresh.role
        && committed.observations == fresh.observations
        && committed.coverage == fresh.coverage
        && committed.streams == fresh.streams
        && committed.layout == fresh.layout
        && committed.day_inventory == fresh.day_inventory
        && import::same_objects(&committed.objects, &fresh.objects, identities)
}

use binary_alpha_engine::config::CandleSpec;
use binary_alpha_engine::dataset::daily::{
    DayFamily, DayInventoryEntry, DayState, UnresolvedInterval, day_bounds,
};
use binary_alpha_engine::market::{PriceScale, parse_event_time_micros};
use binary_alpha_engine::stream::{Candle, InstrumentProfile};

fn date(time: i64) -> String {
    format_event_time_micros(time)[..10].to_string()
}

pub(crate) fn pending_open(
    profile: &InstrumentProfile,
    index: usize,
) -> Result<Option<i64>, String> {
    let spec = &profile.streams[index];
    if spec.withheld_observations == 0 {
        return Ok(None);
    }
    let last = parse_event_time_micros(
        &profile
            .coverage
            .as_ref()
            .ok_or("withheld candle has no coverage")?
            .last_event_time,
    )?;
    Ok(Some(binary_alpha_engine::stream::interval_open(
        last,
        i64::from(spec.duration_seconds) * 1_000_000,
        i64::from(spec.offset_seconds) * 1_000_000,
    )))
}

fn candle_day(
    date: &str,
    spec: &CandleSpec,
    inventory: &[DayInventoryEntry],
    pending: Option<i64>,
    finalized_at: Option<i64>,
) -> Result<DayInventoryEntry, String> {
    let (start, end) = day_bounds(date)?;
    let source = inventory
        .iter()
        .find(|d| d.family == DayFamily::Observations && d.date == date);
    let mut day = DayInventoryEntry {
        date: date.into(),
        family: DayFamily::Candles,
        duration: Some(spec.duration_seconds),
        offset: Some(spec.offset_seconds),
        object: None,
        rows: 0,
        first_time: None,
        last_time: None,
        state: source.map_or(DayState::Unknown, |s| match s.state {
            DayState::Complete => DayState::EmptyKnown,
            state => state,
        }),
        reason: source.and_then(|s| s.reason.clone()).or_else(|| {
            source
                .is_none()
                .then(|| "candle opens outside the source day inventory".into())
        }),
        unresolved: source.map_or_else(Vec::new, |s| s.unresolved.clone()),
    };
    // A candle opening near midnight can contain next-day observations. Completeness of
    // the open day alone cannot establish completeness of that candle's output.
    if day.state == DayState::EmptyKnown {
        let duration = i64::from(spec.duration_seconds) * 1_000_000;
        let last_open = binary_alpha_engine::stream::interval_open(
            end - 1,
            duration,
            i64::from(spec.offset_seconds) * 1_000_000,
        );
        if last_open >= start {
            // Finalization time is part of the candle too: a missing observation between
            // close and known_at could have finalized it earlier.
            let close = (last_open + duration).max(finalized_at.map_or(end, |at| at + 1));
            let mut cursor = end;
            while cursor < close {
                let next_end =
                    (cursor + binary_alpha_engine::dataset::daily::DAY_MICROS).min(close);
                let next_date = self::date(cursor);
                let next = inventory
                    .iter()
                    .find(|d| d.family == DayFamily::Observations && d.date == next_date);
                let covered = match next {
                    Some(d) if matches!(d.state, DayState::Complete | DayState::EmptyKnown) => true,
                    Some(d) if d.state == DayState::Partial => {
                        let mut covered = true;
                        for interval in &d.unresolved {
                            if parse_event_time_micros(&interval.start)? < next_end
                                && parse_event_time_micros(&interval.end)? > cursor
                            {
                                covered = false;
                            }
                        }
                        covered
                    }
                    _ => false,
                };
                if !covered {
                    day.state = DayState::Unknown;
                    day.reason = Some(format!(
                        "candle intervals extend into source day {next_date} without verified coverage"
                    ));
                    break;
                }
                cursor = next_end;
            }
        }
    }
    if let Some(open) = pending.filter(|t| (start..end).contains(t)) {
        day.state = DayState::Partial;
        let pending_reason = "last candle may be finalized by later input";
        day.reason = Some(day.reason.map_or_else(
            || pending_reason.into(),
            |reason| format!("{reason}; {pending_reason}"),
        ));
        // Union the pending tail with existing gaps without erasing known-covered spans.
        let mut from = open;
        let mut unresolved = Vec::new();
        for interval in day.unresolved.into_iter().rev() {
            if parse_event_time_micros(&interval.end)? >= from {
                from = from.min(parse_event_time_micros(&interval.start)?);
            } else {
                unresolved.push(interval);
            }
        }
        unresolved.reverse();
        unresolved.push(UnresolvedInterval {
            start: format_event_time_micros(from),
            end: format_event_time_micros(end),
        });
        day.unresolved = unresolved;
    }
    Ok(day)
}

/// Only the current candle-open day is buffered; finalized older days are spooled once.
#[derive(Default)]
struct DailyCandles {
    current: Vec<Candle>,
    days:
        std::collections::BTreeMap<String, (std::path::PathBuf, crate::archive::DataSummary, i64)>,
    rows: u64,
    first: Option<i64>,
    last: Option<i64>,
}
impl DailyCandles {
    fn push(
        &mut self,
        candle: Candle,
        local: &Store,
        generation: &str,
        id: &InstrumentId,
        scale: PriceScale,
        spec: &CandleSpec,
    ) -> Result<(), String> {
        if self
            .current
            .first()
            .is_some_and(|c| date(c.open_time_micros) != date(candle.open_time_micros))
        {
            self.flush(local, generation, id, scale, spec)?;
        }
        self.rows += 1;
        self.first.get_or_insert(candle.open_time_micros);
        self.last = Some(candle.close_time_micros);
        self.current.push(candle);
        Ok(())
    }
    fn flush(
        &mut self,
        local: &Store,
        generation: &str,
        id: &InstrumentId,
        scale: PriceScale,
        spec: &CandleSpec,
    ) -> Result<(), String> {
        let Some(first) = self.current.first() else {
            return Ok(());
        };
        let date = date(first.open_time_micros);
        let path = import::temporary_path(
            local,
            &format!(
                "audit-{generation}-{}-{}-{date}",
                spec.duration_seconds, spec.offset_seconds
            ),
        )?;
        let finalized_at = self
            .current
            .iter()
            .map(|c| c.known_at_micros)
            .max()
            .expect("nonempty day");
        let summary = crate::daily::write_candles(
            &path,
            &date,
            id,
            scale,
            spec.duration_seconds,
            spec.offset_seconds,
            [std::mem::take(&mut self.current)],
        )?;
        self.days.insert(date, (path, summary, finalized_at));
        Ok(())
    }
    fn summary(&self, spec: &CandleSpec) -> StreamSummary {
        StreamSummary {
            duration_seconds: spec.duration_seconds,
            offset_seconds: spec.offset_seconds,
            rows: self.rows,
            first_open_time: self.first.map(format_event_time_micros),
            last_close_time: self.last.map(format_event_time_micros),
        }
    }
}

#[cfg(test)]
mod daily_inventory_tests {
    use super::*;

    #[test]
    fn pending_tail_unions_gaps_without_erasing_resolved_spans_or_reasons() {
        let date = "2026-09-17";
        let (start, end) = day_bounds(date).unwrap();
        let hour = 3_600_000_000;
        let interval = |from, to| UnresolvedInterval {
            start: format_event_time_micros(start + from),
            end: format_event_time_micros(start + to),
        };
        let source = DayInventoryEntry {
            date: date.into(),
            family: DayFamily::Observations,
            duration: None,
            offset: None,
            object: Some("fixture".into()),
            rows: 0,
            first_time: None,
            last_time: None,
            state: DayState::Partial,
            reason: Some("two acquisition gaps".into()),
            unresolved: vec![interval(hour, 2 * hour), interval(3 * hour, 4 * hour)],
        };
        let spec = CandleSpec {
            duration_seconds: 15,
            offset_seconds: 5,
            min_observations: None,
            hard_min_observations: None,
        };
        let day = candle_day(
            date,
            &spec,
            std::slice::from_ref(&source),
            Some(start + 23 * hour),
            None,
        )
        .unwrap();
        assert_eq!(
            day.unresolved,
            vec![
                interval(hour, 2 * hour),
                interval(3 * hour, 4 * hour),
                interval(23 * hour, end - start)
            ]
        );
        assert!(
            day.reason
                .unwrap()
                .contains("two acquisition gaps; last candle")
        );
        let day = candle_day(date, &spec, &[source], Some(start + 4 * hour), None).unwrap();
        assert_eq!(
            day.unresolved,
            vec![interval(hour, 2 * hour), interval(3 * hour, end - start)]
        );
    }
}

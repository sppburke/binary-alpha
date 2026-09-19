//! The single audit feed seam for session-aware daily candles. V1 retains the raw stream.
use crate::{audit, store::Store, verify};
use binary_alpha_engine::{
    config::Instrument,
    continuous::Continuous,
    dataset::{GenerationManifest, NativeGranularity, coverage::DailyCoverage, daily::DayFamily},
    stream::{Candle, InstrumentStream, Source, StreamManifest},
};

pub(crate) fn feed(
    source: &Store,
    manifest: &GenerationManifest,
    instrument: &Instrument,
    stream: &mut InstrumentStream,
    mut emit: impl FnMut(usize, Candle) -> Result<(), String>,
) -> Result<(), String> {
    let mut transforms = Vec::new();
    if manifest.layout.is_some() {
        let calendar=instrument.session.as_ref().ok_or_else(|| format!(
            "{}: daily-v2 candle writing requires an explicit instruments.session table (kind = always or weekly); no session is defaulted", instrument.id()
        ))?.calendar()?;
        let object = manifest
            .objects
            .iter()
            .find(|o| o.path == "provenance/coverage.json")
            .ok_or("daily dataset lacks verified coverage")?;
        let (_, file) = verify::fetch(source, object, true)?;
        let evidence = DailyCoverage::from_json(
            &std::fs::read(&file.expect("requested coverage file").path)
                .map_err(|e| e.to_string())?,
        )?;
        evidence.check_manifest(manifest)?;
        let mut verified = evidence
            .days
            .iter()
            .filter(|d| d.family == DayFamily::Observations)
            .flat_map(|d| d.verified.iter())
            .map(|r| r.bounds())
            .collect::<Result<Vec<_>, _>>()?;
        verified.sort_unstable();
        for spec in &instrument.candles {
            transforms.push(Continuous::new(
                calendar.clone(),
                spec,
                verified.clone(),
                matches!(manifest.native_granularity, NativeGranularity::Bar { .. }),
            )?);
        }
    }
    let mut finalized = Vec::new();
    audit::feed_generation(
        source,
        manifest,
        instrument.price_scale,
        &mut |observation| {
            stream
                .push(observation, &mut finalized)
                .map_err(|e| e.to_string())?;
            for (index, candle) in finalized.drain(..) {
                if let Some(transform) = transforms.get_mut(index) {
                    transform.push(candle, &mut |c| emit(index, c))?;
                } else {
                    emit(index, candle)?;
                }
            }
            Ok(())
        },
    )?;
    let profile = stream.profile();
    for (index, transform) in transforms.iter_mut().enumerate() {
        transform.finish(
            audit::pending_open(&profile, index, instrument.session.as_ref())?,
            &mut |c| emit(index, c),
        )?;
    }
    Ok(())
}

/// Reproduce the expected session sequence from authenticated observations and coverage,
/// compare every row and each inventory day in bounded memory. This proves leading/trailing
/// limits, all internal buckets, prices and diagnostics, not merely equal aggregate counts.
/// Shared bucket-open membership includes weekly and dated close instants. Omitting a closing
/// bucket must fail the same row comparison as an interior gap, even with repaired file hashes.
pub(crate) fn verify_continuity(
    store: &Store,
    manifest: &StreamManifest,
    source: &GenerationManifest,
    profile: &binary_alpha_engine::stream::InstrumentProfile,
) -> Result<(), String> {
    if manifest.definition.session.is_none() {
        return Ok(());
    } // immutable legacy v2
    let fallback;
    let source_store = if store.head(&source.key())?.is_some() {
        store
    } else {
        fallback = verify::open(
            manifest
                .source_manifest_uri
                .as_ref()
                .ok_or("session source unavailable")?,
        )?
        .0;
        &fallback
    };
    let mut readers: Vec<_> = manifest
        .definition
        .candles
        .iter()
        .map(|spec| {
            let days = manifest
                .day_inventory
                .iter()
                .filter(|d| {
                    d.duration == Some(spec.duration_seconds)
                        && d.offset == Some(spec.offset_seconds)
                })
                .collect();
            Rows {
                store,
                manifest,
                days,
                next_day: 0,
                current: Vec::new().into_iter(),
            }
        })
        .collect();
    let mut stream = InstrumentStream::new(&manifest.definition, Source::from_manifest(source))?;
    feed(
        source_store,
        source,
        &manifest.definition,
        &mut stream,
        |index, expected| {
            let actual = readers[index].next()?;
            if actual.as_ref() != Some(&expected) {
                return Err(format!(
                    "session continuity mismatch in stream {index} at {}: expected {:?}, found {:?}",
                    expected.open_time_micros, expected, actual
                ));
            }
            Ok(())
        },
    )?;
    if &stream.profile() != profile {
        return Err(
            "session continuity: reconstructed feed profile differs from profile.json".into(),
        );
    }
    for reader in &mut readers {
        if reader.next()?.is_some() {
            return Err(
                "session continuity: extra candle after verified coverage/pending boundary".into(),
            );
        }
    }
    Ok(())
}
struct Rows<'a> {
    store: &'a Store,
    manifest: &'a StreamManifest,
    days: Vec<&'a binary_alpha_engine::dataset::DayInventoryEntry>,
    next_day: usize,
    current: std::vec::IntoIter<Candle>,
}
impl Rows<'_> {
    fn next(&mut self) -> Result<Option<Candle>, String> {
        loop {
            if let Some(row) = self.current.next() {
                return Ok(Some(row));
            }
            let Some(day) = self.days.get(self.next_day) else {
                return Ok(None);
            };
            self.next_day += 1;
            if let Some(key) = &day.object {
                let path = day.logical_path()?;
                let object = self
                    .manifest
                    .objects
                    .iter()
                    .find(|o| &o.key == key && o.path == path)
                    .ok_or("missing session candle object")?;
                let (_, local) = verify::fetch(self.store, object, true)?;
                let local = local.expect("requested candle file");
                crate::session_candles::require_schema(&local.path)?;
                self.current = crate::daily::read_candles(
                    &local.path,
                    &day.date,
                    &self.manifest.definition.id(),
                    self.manifest.definition.price_scale,
                    day.duration.unwrap(),
                    day.offset.unwrap(),
                )?
                .into_iter();
            }
        }
    }
}

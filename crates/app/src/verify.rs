//! `binary-alpha data verify`: re-read one published generation from its ready manifest and
//! store objects alone, and reconstruct what the manifest asserts. A dataset manifest has no
//! top-level `kind`; a stream manifest carries `kind = "instrument_stream"`, a feature
//! generation `kind = "feature_generation"`, an outcome generation
//! `kind = "outcome_generation"`, an engine replay `kind = "engine_replay"`, a search family
//! `kind = "search_family"`, and a portfolio selection `kind = "portfolio_selection"`.

use std::fs::{self, File};
use std::path::PathBuf;

use binary_alpha_engine::config::ManifestUri;
use binary_alpha_engine::dataset::{
    GenerationManifest, NativeGranularity, ObjectRecord, ObjectRole, SourceKind,
};
use binary_alpha_engine::execution::REPLAY_MANIFEST_KIND;
use binary_alpha_engine::features::FEATURE_MANIFEST_KIND;
use binary_alpha_engine::market::format_event_time_micros;
use binary_alpha_engine::outcomes::OUTCOME_MANIFEST_KIND;
use binary_alpha_engine::portfolio::SELECTION_MANIFEST_KIND;
use binary_alpha_engine::research::{Access, CERTIFICATION_MANIFEST_KIND, RUN_MANIFEST_KIND};
use binary_alpha_engine::search::FAMILY_MANIFEST_KIND;
use binary_alpha_engine::stream::{
    InstrumentProfile, PROFILE_OBJECT_PATH, STREAM_MANIFEST_KIND, StreamManifest, StreamSummary,
};
use serde::Deserialize;
use sha2::{Digest, Sha256};

use crate::archive::{self, BarExpectation, DataSummary};
use crate::store::{Hasher, Store, Tee};

/// Verifies the generation whose ready manifest is at `uri` and returns its report line.
pub fn run(uri: &str) -> Result<String, String> {
    run_with(uri, Access::ORDINARY)
}

/// `run` with the declaration of the configuration at `config`, when one is given: a declared
/// target is permitted before it is opened, and an undeclared dataset is refused.
pub fn run_configured(config: Option<&std::path::Path>, uri: &str) -> Result<String, String> {
    let config = config.map(crate::load_config).transpose()?;
    let declaration = config
        .as_ref()
        .map(crate::research::declaration)
        .transpose()?
        .flatten();
    run_with(
        uri,
        Access {
            declaration: declaration.as_ref(),
            certification: None,
        },
    )
}

/// `run` under an explicit read permit: a dataset generation needs the permit of its role;
/// derived generations keep their own role checks; protected research evidence is verified
/// only within the matching certification context.
pub fn run_with(uri: &str, access: Access<'_>) -> Result<String, String> {
    let target: ManifestUri = uri.parse()?;
    access.lookup(target.generation())?;
    let (store, manifest_key) = open(uri)?;
    let mut bytes = Vec::new();
    store.read_to(&manifest_key, None, &mut bytes)?;
    protected_envelope(uri, &bytes, access)?;
    match manifest_kind(&bytes)?.as_deref() {
        None => {
            let manifest =
                GenerationManifest::from_json(&bytes).map_err(|error| format!("{uri}: {error}"))?;
            access
                .permit(Some(manifest.role), &manifest.generation)
                .map_err(|reason| format!("{uri}: {reason}"))?;
            verify_dataset(uri, &store, &manifest_key, &bytes)
        }
        Some(STREAM_MANIFEST_KIND) => verify_stream(uri, &store, &manifest_key, &bytes, access),
        Some(FEATURE_MANIFEST_KIND) => {
            crate::features::verify_feature(uri, &store, &manifest_key, &bytes)
        }
        Some(OUTCOME_MANIFEST_KIND) => {
            crate::outcomes::verify_outcome(uri, &store, &manifest_key, &bytes)
        }
        Some(REPLAY_MANIFEST_KIND) => {
            crate::replay::verify_replay(uri, &store, &manifest_key, &bytes, access)
        }
        Some(FAMILY_MANIFEST_KIND) => {
            crate::search::verify_family(uri, &store, &manifest_key, &bytes, access)
        }
        Some(SELECTION_MANIFEST_KIND) => {
            crate::portfolio::verify_selection(uri, &store, &manifest_key, &bytes, access)
        }
        Some(RUN_MANIFEST_KIND) => {
            crate::research::verify_run(uri, &store, &manifest_key, &bytes, access)
        }
        Some(CERTIFICATION_MANIFEST_KIND) => {
            crate::research::verify_certification(uri, &store, &manifest_key, &bytes, access)
        }
        Some(kind) => Err(format!("{uri}: unsupported manifest kind `{kind}`")),
    }
}

/// A derived generation of holdout data (a stream, feature, outcome, or replay manifest whose
/// `role` is holdout) resolves only within the certification context that names the dataset
/// generations it was computed from; its children are never opened publicly.
fn protected_envelope(uri: &str, bytes: &[u8], access: Access<'_>) -> Result<(), String> {
    use binary_alpha_engine::dataset::DatasetRole;
    #[derive(Deserialize)]
    struct Envelope {
        role: Option<DatasetRole>,
        source_generation: Option<String>,
        input_generation: Option<String>,
        tick_generation: Option<String>,
        #[serde(default)]
        instruments: Vec<BoundInstrument>,
        #[serde(default)]
        inputs: Vec<BoundInput>,
    }
    #[derive(Deserialize)]
    struct BoundInstrument {
        tick_generation: Option<String>,
    }
    #[derive(Deserialize)]
    struct BoundInput {
        role: Option<String>,
        tick_generation: Option<String>,
    }
    let envelope: Envelope =
        serde_json::from_slice(bytes).map_err(|error| format!("{uri}: {error}"))?;
    let bound: Vec<&str> = envelope
        .source_generation
        .iter()
        .chain(envelope.input_generation.iter())
        .chain(envelope.tick_generation.iter())
        .chain(
            envelope
                .instruments
                .iter()
                .filter_map(|instrument| instrument.tick_generation.as_ref()),
        )
        .chain(
            envelope
                .inputs
                .iter()
                .filter_map(|input| input.tick_generation.as_ref()),
        )
        .map(String::as_str)
        .collect();
    // A declared holdout source is protected whatever the derived manifest's own label says.
    for generation in &bound {
        access
            .lookup(generation)
            .map_err(|reason| format!("{uri}: {reason}"))?;
    }
    let labelled = envelope.role == Some(DatasetRole::Holdout)
        || envelope
            .inputs
            .iter()
            .any(|input| input.role.as_deref() == Some(DatasetRole::Holdout.as_str()));
    if labelled {
        access
            .protected(bound.iter().copied())
            .map_err(|reason| format!("{uri}: {reason}"))?;
    }
    Ok(())
}

/// The top-level `kind` a manifest declares; a dataset ready manifest declares none.
pub fn manifest_kind(bytes: &[u8]) -> Result<Option<String>, String> {
    #[derive(Deserialize)]
    struct Kind {
        kind: Option<String>,
    }
    serde_json::from_slice::<Kind>(bytes)
        .map(|manifest| manifest.kind)
        .map_err(|error| error.to_string())
}

/// Splits `URI` into the store root and the manifest key, accepting only the documented
/// `manifests/GENERATION/ready.json` grammar.
pub fn open(uri: &str) -> Result<(Store, String), String> {
    let uri: ManifestUri = uri.parse()?;
    Ok((Store::open(&uri.root)?, uri.key))
}

/// A store object readable at a local path: the filesystem store's own file, or a scratch copy
/// of a remote object that is removed when dropped.
pub struct LocalObject {
    pub path: PathBuf,
    scratch: bool,
}

impl Drop for LocalObject {
    fn drop(&mut self) {
        if self.scratch {
            let _ = fs::remove_file(&self.path);
        }
    }
}

/// Reads one object from the store and asserts its size, SHA-256, and any recorded CRC32C
/// against the bytes read, plus the store's own checksum and generation when it reports them.
/// Returns the verified byte count and, when `decode` is set, a local path for a decoder.
pub fn fetch(
    store: &Store,
    object: &ObjectRecord,
    decode: bool,
) -> Result<(u64, Option<LocalObject>), String> {
    let location = store.uri(&object.key);
    let stored = store
        .head(&object.key)?
        .ok_or_else(|| format!("{location} is missing"))?;
    // A store that reports checksums or generations must agree with the record; the
    // filesystem implementation reports neither, so recorded Google values stay provenance.
    if stored.bytes != object.bytes
        || (stored.crc32c.is_some() && stored.crc32c != object.crc32c)
        || (stored.generation.is_some() && stored.generation != object.generation)
    {
        return Err(format!(
            "{location} does not match the recorded size, generation, and checksum"
        ));
    }
    let local = match (decode, store.local_path(&object.key)) {
        (false, _) => None,
        (true, Some(path)) => Some(LocalObject {
            path,
            scratch: false,
        }),
        (true, None) => Some(LocalObject {
            path: std::env::temp_dir().join(format!(
                "binary-alpha-verify-{}-{}",
                std::process::id(),
                object.sha256
            )),
            scratch: true,
        }),
    };
    let mut hasher = Hasher::default();
    match &local {
        Some(local) if local.scratch => {
            let mut file = File::create(&local.path)
                .map_err(|error| format!("cannot create {}: {error}", local.path.display()))?;
            store.read_to(
                &object.key,
                object.generation,
                &mut Tee(&mut hasher, &mut file),
            )?;
        }
        _ => {
            store.read_to(&object.key, object.generation, &mut hasher)?;
        }
    }
    let identity = hasher.finish();
    if identity.bytes != object.bytes
        || identity.sha256 != object.sha256
        || object
            .crc32c
            .is_some_and(|crc32c| crc32c != identity.crc32c)
    {
        return Err(format!(
            "{location} has {} bytes, SHA-256 {}, and CRC32C {}, recorded {} bytes, {}, and {:?}",
            identity.bytes,
            identity.sha256,
            identity.crc32c,
            object.bytes,
            object.sha256,
            object.crc32c
        ));
    }
    Ok((identity.bytes, local))
}

fn verify_dataset(uri: &str, store: &Store, key: &str, bytes: &[u8]) -> Result<String, String> {
    let manifest =
        GenerationManifest::from_json(bytes).map_err(|error| format!("{uri}: {error}"))?;
    if manifest.key() != key {
        return Err(format!(
            "{uri} holds the manifest of generation {}",
            manifest.generation
        ));
    }
    // Inspect authenticated coverage and page slices before the whole-object digest check so
    // damaged bundles identify the exact page. Every object still receives `fetch` below.
    let history_report =
        if manifest.layout.is_none() && manifest.source_kind == SourceKind::BrokerHistory {
            verify_history_bundles(store, &manifest)?
        } else {
            String::new()
        };
    let read = crate::daily::read_generation(store, &manifest, |_| Ok(()))?;
    let summary = read.data;
    let mut bytes_verified = read.bytes;
    let partitions = crate::daily::observation_partitions(&manifest)?;
    let mut occurrences = std::collections::BTreeSet::new();
    for object in &manifest.objects {
        if partitions.iter().any(|(o, _)| o.path == object.path) {
            continue;
        }
        let page_day = manifest.day_inventory.iter().find(|d| {
            d.family == binary_alpha_engine::dataset::daily::DayFamily::Pages
                && d.object.as_ref() == Some(&object.key)
                && d.logical_path().is_ok_and(|p| p == object.path)
        });
        let coverage = manifest.layout.is_some() && object.path == "provenance/coverage.json";
        let (verified, local) = fetch(store, object, page_day.is_some() || coverage)?;
        bytes_verified += verified;
        if coverage {
            binary_alpha_engine::dataset::coverage::DailyCoverage::from_json(
                &fs::read(&local.expect("coverage object").path).map_err(|e| e.to_string())?,
            )?
            .check_manifest(&manifest)?;
        } else if let Some(day) = page_day {
            let pages = crate::daily::read_pages(&local.expect("page partition").path, &day.date)
                .map_err(|e| format!("{}: {e}", object.path))?;
            let mut data = DataSummary::default();
            for page in pages {
                if !occurrences.insert((page.acquisition_id.clone(), page.ordinal)) {
                    return Err(format!(
                        "{}: repeated page occurrence {}/{}",
                        object.path, page.acquisition_id, page.ordinal
                    ));
                }
                let time = page.partition_time()?;
                data.rows += 1;
                data.first_event_micros =
                    Some(data.first_event_micros.map_or(time, |t| t.min(time)));
                data.last_event_micros = Some(data.last_event_micros.map_or(time, |t| t.max(time)));
            }
            crate::daily::check_inventory(day, &data)?;
        }
    }
    let (first_event_time, last_event_time) = archive::coverage(&summary)?;
    if summary.rows != manifest.row_count
        || first_event_time != manifest.coverage.first_event_time
        || last_event_time != manifest.coverage.last_event_time
    {
        return Err(format!(
            "{uri}: reconstructed {} rows from {first_event_time} to {last_event_time}, recorded {} rows from {} to {}",
            summary.rows,
            manifest.row_count,
            manifest.coverage.first_event_time,
            manifest.coverage.last_event_time
        ));
    }
    Ok(format!(
        "verified {} {} generation {} rows {} objects {} bytes {bytes_verified}{history_report}",
        manifest.instrument,
        manifest.role,
        manifest.generation,
        manifest.row_count,
        manifest.objects.len()
    ))
}

/// Assert the recorded offsets as well as the lengths, then authenticate every page slice.
pub(crate) fn verify_bundle_pages(
    path: &str,
    bytes: &[u8],
    pages: &[&crate::fetch::PageCoverage],
) -> Result<(), String> {
    let mut end = 0_u64;
    if pages.is_empty() {
        return Err(format!(
            "pages do not tile the bundle {path}: no page entries"
        ));
    }
    for page in pages {
        if page.offset != Some(end) {
            return Err(format!(
                "pages do not tile the bundle {path}: expected offset {end}, recorded {:?}",
                page.offset
            ));
        }
        end = end
            .checked_add(page.bytes)
            .ok_or_else(|| format!("pages do not tile the bundle {path}: length overflow"))?;
    }
    if end != bytes.len() as u64 {
        return Err(format!(
            "pages do not tile the bundle {path}: indexed {end} bytes, observed {}",
            bytes.len()
        ));
    }
    for (index, page) in pages.iter().enumerate() {
        let start = page.offset.expect("checked offset") as usize;
        let slice = &bytes[start..start + page.bytes as usize];
        if binary_alpha_engine::hex(&Sha256::digest(slice)) != page.sha256 {
            return Err(format!(
                "page {} of {path} does not carry its recorded digest",
                index + 1
            ));
        }
    }
    Ok(())
}

fn verify_history_bundles(store: &Store, manifest: &GenerationManifest) -> Result<String, String> {
    use crate::fetch::{BUNDLE_PATH, COVERAGE_PATH, HistoryCoverage};
    let bundle_objects: Vec<_> = manifest
        .objects
        .iter()
        .filter(|object| {
            object.role == ObjectRole::Source
                && (object.path == BUNDLE_PATH
                    || (object.path.starts_with("raw/") && object.path.ends_with("/pages.bin")))
        })
        .collect();
    let Some(record) = manifest
        .objects
        .iter()
        .find(|object| object.path == COVERAGE_PATH)
    else {
        if bundle_objects.is_empty() {
            return Ok(String::new()); // Legacy generations retain individual page objects.
        }
        return Err(format!("history bundles require {COVERAGE_PATH}"));
    };
    let (_, local) = fetch(store, record, true)?;
    let coverage: HistoryCoverage = serde_json::from_slice(
        &fs::read(&local.expect("decoded coverage").path).map_err(|error| error.to_string())?,
    )
    .map_err(|error| format!("{COVERAGE_PATH}: {error}"))?;
    let current = bundle_objects
        .iter()
        .find(|object| object.path == BUNDLE_PATH);
    match (&coverage.bundle, current) {
        (Some(bundle), Some(object))
            if bundle.sha256 == object.sha256 && bundle.bytes == object.bytes => {}
        (None, None) => {}
        _ => {
            return Err(format!(
                "{COVERAGE_PATH}: bundle identity does not match {BUNDLE_PATH}"
            ));
        }
    }
    for page in coverage.pages.iter().filter(|page| page.offset.is_some()) {
        if !bundle_objects.iter().any(|object| object.path == page.path) {
            return Err(format!(
                "page index names missing source bundle {}",
                page.path
            ));
        }
    }
    let mut page_count = 0;
    for object in &bundle_objects {
        let pages: Vec<_> = coverage
            .pages
            .iter()
            .filter(|page| page.path == object.path)
            .collect();
        let mut bytes = Vec::new();
        store.read_to(&object.key, object.generation, &mut bytes)?;
        verify_bundle_pages(&object.path, &bytes, &pages)?;
        if bytes.len() as u64 != object.bytes {
            return Err(format!(
                "pages do not tile the bundle {}: recorded {} bytes, observed {}",
                object.path,
                object.bytes,
                bytes.len()
            ));
        }
        page_count += pages.len();
    }
    if bundle_objects.is_empty() {
        Ok(String::new())
    } else {
        Ok(format!(
            " history bundles {} pages {page_count} verified",
            bundle_objects.len()
        ))
    }
}

/// What every listed bar file of a published generation must satisfy when it is read back.
pub fn bar_expectation(manifest: &GenerationManifest) -> Result<BarExpectation, String> {
    let NativeGranularity::Bar { period_seconds } = manifest.native_granularity else {
        return Err("manifest is not a bar generation".to_string());
    };
    let interval = manifest
        .interval
        .clone()
        .ok_or("bar manifest lacks its interval contract")?;
    Ok(BarExpectation {
        symbol: manifest.provider_symbol.to_string(),
        symbol_id: None,
        period_s: period_seconds,
        server_offset_s: None,
        metadata_required: interval.provenance == "parquet_metadata",
        interval,
    })
}

/// Verifies a stream generation: every object's bytes and hashes, the profile's consistency
/// with the manifest, and every candle object's rows and bounds.
fn verify_stream(
    uri: &str,
    store: &Store,
    key: &str,
    bytes: &[u8],
    access: Access<'_>,
) -> Result<String, String> {
    let manifest = StreamManifest::from_json(bytes).map_err(|error| format!("{uri}: {error}"))?;
    if manifest.key() != key {
        return Err(format!(
            "{uri} holds the manifest of generation {}",
            manifest.generation
        ));
    }
    let mut bytes_verified = 0;
    let mut candles = 0;
    let mut daily_totals =
        std::collections::BTreeMap::<(u32, u32), (u64, Option<i64>, Option<i64>)>::new();
    let mut observed_profile = None;
    let mut finalized_days = std::collections::BTreeMap::<
        (u32, u32),
        std::collections::BTreeMap<String, (u64, i64)>,
    >::new();
    let ordered_objects = if manifest.layout.is_some() {
        let mut objects: Vec<_> = manifest
            .objects
            .iter()
            .filter(|o| o.path == PROFILE_OBJECT_PATH)
            .collect();
        for day in &manifest.day_inventory {
            if let Some(key) = &day.object {
                let path = day.logical_path()?;
                objects.push(
                    manifest
                        .objects
                        .iter()
                        .find(|o| o.key == *key && o.path == path)
                        .ok_or_else(|| format!("missing candle day object {path}"))?,
                );
            }
        }
        objects
    } else {
        manifest.objects.iter().collect()
    };
    for object in ordered_objects {
        let (verified, local) = fetch(store, object, true)?;
        bytes_verified += verified;
        let local = local.expect("decoded objects have a local path");
        let location = store.uri(&object.key);
        if object.path == PROFILE_OBJECT_PATH {
            let profile = fs::read(&local.path)
                .map_err(|error| format!("cannot read {location}: {error}"))
                .and_then(|bytes| InstrumentProfile::from_json(&bytes))
                .map_err(|reason| format!("{location}: {reason}"))?;
            let definition = &manifest.definition;
            let consistent = profile.instrument == manifest.instrument
                && profile.broker == definition.broker
                && profile.provider_symbol == definition.provider_symbol
                && profile.base_currency == definition.base_currency
                && profile.quote_currency == definition.quote_currency
                && profile.source.native_granularity == definition.native_granularity
                && profile.source.generation == manifest.source_generation
                && profile.source.source_kind == manifest.source_kind
                && profile.source.role == manifest.role
                && profile.price_scale == manifest.definition.price_scale
                && profile.observations == manifest.observations
                && profile.coverage == manifest.coverage
                && profile.streams.len() == manifest.streams.len()
                && profile
                    .streams
                    .iter()
                    .zip(&manifest.streams)
                    .all(|(facts, summary)| {
                        facts.duration_seconds == summary.duration_seconds
                            && facts.offset_seconds == summary.offset_seconds
                            && (manifest.layout.is_some() && definition.session.is_some()
                                || facts.finalized == summary.rows)
                    });
            if !consistent {
                return Err(format!(
                    "{location} does not describe the manifest's instrument, source, observations, coverage, and streams"
                ));
            }
            observed_profile = Some(profile);
            continue;
        }
        if manifest.layout.is_some() {
            let day = manifest
                .day_inventory
                .iter()
                .find(|d| {
                    d.object.as_ref() == Some(&object.key)
                        && d.logical_path().is_ok_and(|p| p == object.path)
                })
                .ok_or_else(|| format!("{}: missing candle inventory", object.path))?;
            let spec = (
                day.duration.expect("validated spec"),
                day.offset.expect("validated spec"),
            );
            let rows = crate::daily::read_candles(
                &local.path,
                &day.date,
                &manifest.definition.id(),
                manifest.definition.price_scale,
                spec.0,
                spec.1,
            )
            .map_err(|e| format!("{}: {e}", object.path))?;
            let data = DataSummary {
                rows: rows.len() as u64,
                first_event_micros: rows.first().map(|c| c.open_time_micros),
                last_event_micros: rows.last().map(|c| c.open_time_micros),
            };
            crate::daily::check_inventory(day, &data)?;
            if let Some(at) = rows
                .iter()
                .map(crate::session_candles::inventory_finalizer)
                .max()
            {
                finalized_days
                    .entry(spec)
                    .or_default()
                    .insert(day.date.clone(), (data.rows, at));
            }
            let total = daily_totals.entry(spec).or_default();
            for candle in rows {
                archive::check_candle_order(total.2, &candle)
                    .map_err(|e| format!("{}: {e}", object.path))?;
                total.0 += 1;
                total.1.get_or_insert(candle.open_time_micros);
                total.2 = Some(candle.close_time_micros);
                candles += 1;
            }
            continue;
        }
        let summary = manifest
            .streams
            .iter()
            .find(|summary| {
                StreamSummary::object_path(summary.duration_seconds, summary.offset_seconds)
                    == object.path
            })
            .ok_or_else(|| format!("{location} belongs to no stream"))?;
        let (rows, first_open, last_close) = archive::read_candles(
            &local.path,
            &manifest.definition.id(),
            manifest.definition.price_scale,
            summary.duration_seconds,
            summary.offset_seconds,
        )
        .map_err(|reason| format!("{location}: {reason}"))?;
        if rows != summary.rows
            || first_open.map(format_event_time_micros) != summary.first_open_time
            || last_close.map(format_event_time_micros) != summary.last_close_time
        {
            return Err(format!(
                "{location}: reconstructed {rows} candles, recorded {} from {:?} to {:?}",
                summary.rows, summary.first_open_time, summary.last_close_time
            ));
        }
        candles += rows;
    }
    if manifest.layout.is_some() {
        let profile = observed_profile.ok_or("daily stream lacks profile")?;
        let source = stream_source(store, &manifest, access)?;
        crate::session_audit::verify_continuity(store, &manifest, &source, &profile)?;
        for (index, summary) in manifest.streams.iter().enumerate() {
            let total = daily_totals
                .get(&(summary.duration_seconds, summary.offset_seconds))
                .copied()
                .unwrap_or_default();
            if total.0 != summary.rows
                || total.1.map(format_event_time_micros) != summary.first_open_time
                || total.2.map(format_event_time_micros) != summary.last_close_time
            {
                return Err(format!(
                    "daily stream {}s_{}s aggregate summary mismatch",
                    summary.duration_seconds, summary.offset_seconds
                ));
            }
            let expected = crate::audit::candle_inventory(
                &manifest.definition.candles[index],
                &source.day_inventory,
                crate::audit::pending_open(&profile, index, manifest.definition.session.as_ref())?,
                finalized_days
                    .get(&(summary.duration_seconds, summary.offset_seconds))
                    .unwrap_or(&std::collections::BTreeMap::new()),
                source.native_granularity,
                manifest.definition.session.as_ref(),
            )?;
            let actual: Vec<_> = manifest
                .day_inventory
                .iter()
                .filter(|d| {
                    d.duration == Some(summary.duration_seconds)
                        && d.offset == Some(summary.offset_seconds)
                })
                .collect();
            if actual.len() != expected.len() {
                return Err("candle inventory does not contain every expected source day".into());
            }
            for (actual, expected) in actual.iter().zip(expected) {
                if actual.date != expected.date
                    || actual.state != expected.state
                    || actual.reason != expected.reason
                    || actual.unresolved != expected.unresolved
                {
                    return Err(format!(
                        "candle day {} must be {} with the derived source coverage and unresolved intervals",
                        expected.date, expected.state
                    ));
                }
            }
        }
    }
    Ok(format!(
        "verified {} {} generation {} candles {candles} objects {} bytes {bytes_verified}",
        manifest.instrument,
        manifest.role,
        manifest.generation,
        manifest.objects.len()
    ))
}

/// Prefer a restored closure, otherwise resolve the writer's explicit source location.
/// Permission and source identity checks precede any source object reads.
fn stream_source(
    store: &Store,
    stream: &StreamManifest,
    access: Access<'_>,
) -> Result<GenerationManifest, String> {
    access.permit(Some(stream.role), &stream.source_generation)?;
    let key = binary_alpha_engine::dataset::manifest_key(&stream.source_generation);
    let fallback;
    let source_store = if store.head(&key)?.is_some() {
        store
    } else {
        let uri = stream
            .source_manifest_uri
            .as_ref()
            .ok_or("stream source evidence unavailable: no local dataset or source reference")?;
        fallback = open(uri)?.0;
        &fallback
    };
    let mut bytes = Vec::new();
    source_store
        .read_to(&key, None, &mut bytes)
        .map_err(|e| format!("stream source evidence unavailable: {e}"))?;
    let source = GenerationManifest::from_json(&bytes)?;
    access.permit(Some(source.role), &source.generation)?;
    if source.key() != key
        || source.layout != stream.layout
        || source.role != stream.role
        || source.instrument != stream.instrument
        || source.source_kind != stream.source_kind
        || source.native_granularity != stream.definition.native_granularity
        || source.row_count != stream.observations
        || Some(&source.coverage) != stream.coverage.as_ref()
    {
        return Err("stream source dataset identity or summary mismatch".into());
    }
    verify_dataset(&source_store.uri(&key), source_store, &key, &bytes)?;
    Ok(source)
}

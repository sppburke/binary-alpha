//! `binary-alpha data verify`: re-read one published generation from its ready manifest and
//! store objects alone, and reconstruct what the manifest asserts. A dataset manifest has no
//! top-level `kind`; a stream manifest carries `kind = "instrument_stream"`.

use std::fs::{self, File};
use std::path::PathBuf;

use binary_alpha_engine::config::PublicationUri;
use binary_alpha_engine::dataset::{
    GenerationManifest, NativeGranularity, ObjectRecord, ObjectRole, PriceRepresentation,
    SourceKind,
};
use binary_alpha_engine::market::format_event_time_micros;
use binary_alpha_engine::stream::{
    InstrumentProfile, PROFILE_OBJECT_PATH, STREAM_MANIFEST_KIND, StreamManifest, StreamSummary,
};
use serde::Deserialize;

use crate::archive::{self, BarExpectation, DataSummary};
use crate::store::{Hasher, Store, Tee};

/// Verifies the generation whose ready manifest is at `uri` and returns its report line.
pub fn run(uri: &str) -> Result<String, String> {
    let (store, manifest_key) = open(uri)?;
    let mut bytes = Vec::new();
    store.read_to(&manifest_key, None, &mut bytes)?;
    match manifest_kind(&bytes)?.as_deref() {
        None => verify_dataset(uri, &store, &manifest_key, &bytes),
        Some(STREAM_MANIFEST_KIND) => verify_stream(uri, &store, &manifest_key, &bytes),
        Some(kind) => Err(format!("{uri}: unsupported manifest kind `{kind}`")),
    }
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
    let (root, key) = uri
        .rsplit_once("/manifests/")
        .filter(|(_, key)| {
            key.strip_suffix("/ready.json").is_some_and(|generation| {
                generation.len() == 64
                    && generation
                        .bytes()
                        .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
            })
        })
        .ok_or_else(|| {
            format!(
                "{uri} must end with manifests/GENERATION/ready.json, where GENERATION is sixty-four lowercase hexadecimal digits"
            )
        })?;
    let publication: PublicationUri = root
        .parse()
        .map_err(|error: String| format!("{uri}: {error}"))?;
    Ok((Store::open(&publication)?, format!("manifests/{key}")))
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
    let mut summary = DataSummary::default();
    let mut bytes_verified = 0;
    for object in &manifest.objects {
        let decode = matches!(
            (object.role, manifest.source_kind),
            (
                ObjectRole::Normalized,
                SourceKind::TickCsv | SourceKind::TickParquetDaily
            ) | (ObjectRole::Source, SourceKind::BarParquet)
        );
        let (verified, local) = fetch(store, object, decode)?;
        bytes_verified += verified;
        if let Some(local) = local {
            reconstruct(&manifest, &local.path, &mut summary)
                .map_err(|reason| format!("{}: {reason}", store.uri(&object.key)))?;
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
        "verified {} {} generation {} rows {} objects {} bytes {bytes_verified}",
        manifest.instrument,
        manifest.role,
        manifest.generation,
        manifest.row_count,
        manifest.objects.len()
    ))
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

/// Decodes one data object and folds its rows into the running summary.
fn reconstruct(
    manifest: &GenerationManifest,
    path: &std::path::Path,
    summary: &mut DataSummary,
) -> Result<(), String> {
    let data = match (manifest.source_kind, manifest.price_representation) {
        (
            SourceKind::TickCsv | SourceKind::TickParquetDaily,
            PriceRepresentation::IntegerUnits { scale },
        ) => archive::read_ticks(path, scale)?,
        (SourceKind::BarParquet, PriceRepresentation::BinaryFloat64) => {
            archive::validate_bar_file(path, &bar_expectation(manifest)?)?.data
        }
        _ => {
            return Err(
                "manifest combines a source kind, price representation, and granularity this checkout cannot decode"
                    .to_string(),
            );
        }
    };
    if let (Some(last), Some(first)) = (summary.last_event_micros, data.first_event_micros)
        && first <= last
    {
        return Err("starts at or before the previous object's last event".to_string());
    }
    summary.extend(&data);
    Ok(())
}

/// Verifies a stream generation: every object's bytes and hashes, the profile's consistency
/// with the manifest, and every candle object's rows and bounds.
fn verify_stream(uri: &str, store: &Store, key: &str, bytes: &[u8]) -> Result<String, String> {
    let manifest = StreamManifest::from_json(bytes).map_err(|error| format!("{uri}: {error}"))?;
    if manifest.key() != key {
        return Err(format!(
            "{uri} holds the manifest of generation {}",
            manifest.generation
        ));
    }
    let mut bytes_verified = 0;
    let mut candles = 0;
    for object in &manifest.objects {
        let (verified, local) = fetch(store, object, true)?;
        bytes_verified += verified;
        let local = local.expect("decoded objects have a local path");
        let location = store.uri(&object.key);
        if object.path == PROFILE_OBJECT_PATH {
            let profile = fs::read(&local.path)
                .map_err(|error| format!("cannot read {location}: {error}"))
                .and_then(|bytes| InstrumentProfile::from_json(&bytes))
                .map_err(|reason| format!("{location}: {reason}"))?;
            let consistent = profile.instrument == manifest.instrument
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
                            && facts.finalized == summary.rows
                    });
            if !consistent {
                return Err(format!(
                    "{location} does not describe the manifest's instrument, source, observations, coverage, and streams"
                ));
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
        let (rows, first_open, last_close) =
            archive::read_candles(&local.path, manifest.definition.price_scale)
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
    Ok(format!(
        "verified {} {} generation {} candles {candles} objects {} bytes {bytes_verified}",
        manifest.instrument,
        manifest.role,
        manifest.generation,
        manifest.objects.len()
    ))
}

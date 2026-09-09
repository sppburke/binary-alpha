//! `binary-alpha data verify`: re-read one published generation from its ready manifest and
//! store objects alone, and reconstruct what the manifest asserts.

use std::fs::{self, File};
use std::path::PathBuf;

use binary_alpha_engine::config::PublicationUri;
use binary_alpha_engine::dataset::{
    GenerationManifest, NativeGranularity, ObjectRole, PriceRepresentation, SourceKind,
};

use crate::archive::{self, BarExpectation, DataSummary};
use crate::store::{Hasher, Store, Tee};

/// Verifies the generation whose ready manifest is at `uri` and returns its report line.
pub fn run(uri: &str) -> Result<String, String> {
    let (store, manifest_key) = open(uri)?;
    let mut bytes = Vec::new();
    store.read_to(&manifest_key, None, &mut bytes)?;
    let manifest =
        GenerationManifest::from_json(&bytes).map_err(|error| format!("{uri}: {error}"))?;
    if manifest.key() != manifest_key {
        return Err(format!(
            "{uri} holds the manifest of generation {}",
            manifest.generation
        ));
    }
    let mut summary = DataSummary::default();
    let mut bytes_verified = 0;
    for object in &manifest.objects {
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
        let decode = matches!(
            (object.role, manifest.source_kind),
            (ObjectRole::Normalized, SourceKind::TickCsv)
                | (ObjectRole::Source, SourceKind::BarParquet)
        );
        let scratch = match (decode, store.local_path(&object.key)) {
            (true, None) => Some(std::env::temp_dir().join(format!(
                "binary-alpha-verify-{}-{}",
                std::process::id(),
                object.sha256
            ))),
            _ => None,
        };
        let mut hasher = Hasher::default();
        match &scratch {
            Some(path) => {
                let mut file = File::create(path)
                    .map_err(|error| format!("cannot create {}: {error}", path.display()))?;
                store.read_to(
                    &object.key,
                    object.generation,
                    &mut Tee(&mut hasher, &mut file),
                )?;
            }
            None => {
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
        bytes_verified += identity.bytes;
        if decode {
            let path: PathBuf = scratch
                .clone()
                .or_else(|| store.local_path(&object.key))
                .expect("a decodable path");
            let result = reconstruct(&manifest, &path, &mut summary);
            if let Some(path) = &scratch {
                let _ = fs::remove_file(path);
            }
            result.map_err(|reason| format!("{location}: {reason}"))?;
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

/// Splits `URI` into the store root and the manifest key, accepting only the documented
/// `manifests/GENERATION/ready.json` grammar.
fn open(uri: &str) -> Result<(Store, String), String> {
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

/// Decodes one data object and folds its rows into the running summary.
fn reconstruct(
    manifest: &GenerationManifest,
    path: &std::path::Path,
    summary: &mut DataSummary,
) -> Result<(), String> {
    let data = match (
        manifest.source_kind,
        manifest.price_representation,
        manifest.native_granularity,
    ) {
        (SourceKind::TickCsv, PriceRepresentation::IntegerUnits { scale }, _) => {
            archive::read_ticks(path, scale)?
        }
        (
            SourceKind::BarParquet,
            PriceRepresentation::BinaryFloat64,
            NativeGranularity::Bar { period_seconds },
        ) => {
            let interval = manifest
                .interval
                .clone()
                .ok_or("bar manifest lacks its interval contract")?;
            let expectation = BarExpectation {
                symbol: manifest.provider_symbol.to_string(),
                symbol_id: None,
                period_s: period_seconds,
                server_offset_s: None,
                metadata_required: interval.provenance == "parquet_metadata",
                interval,
            };
            archive::validate_bar_file(path, &expectation)?.data
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

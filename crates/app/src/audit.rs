//! `binary-alpha data audit`: feed one published generation, in order, through the
//! `InstrumentStream` its configuration maps it to, then retain and publish the profile, one
//! candle object per configured stream, and the stream manifest last.

use std::fs;
use std::io::Write;
use std::path::Path;
use std::time::Instant;

use binary_alpha_engine::config::Config;
use binary_alpha_engine::dataset::{
    DatasetRole, GenerationManifest, ObjectRecord, ObjectRole, PriceRepresentation, SourceKind,
    manifest_key,
};
use binary_alpha_engine::market::{InstrumentId, format_event_time_micros};
use binary_alpha_engine::stream::{
    InstrumentStream, Observation, PROFILE_OBJECT_PATH, STREAM_MANIFEST_KIND,
    STREAM_SCHEMA_VERSION, Source, StreamManifest, StreamSummary, stream_generation_id,
};

use crate::archive::{self, CandleWriter};
use crate::import::{self, CODE_REVISION};
use crate::store::{self, ObjectIdentity, Put, Store};
use crate::verify;

/// Runs the audit of the generation whose ready manifest is at `uri` under the configuration at
/// `config_path`, writing one report line to `out`.
pub fn run(config_path: &Path, uri: &str, out: &mut dyn Write) -> Result<(), String> {
    let text = fs::read_to_string(config_path)
        .map_err(|error| format!("cannot read {}: {error}", config_path.display()))?;
    let config = Config::parse(&text).map_err(|error| error.to_string())?;
    let base = config_path.parent().unwrap_or(Path::new("."));
    let historical_dir = base.join(config.storage.historical_data_dir.as_path());
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
    fs::create_dir_all(&historical_dir)
        .map_err(|error| format!("cannot create {}: {error}", historical_dir.display()))?;
    let local = Store::filesystem(&historical_dir);
    let destination = Store::open(&config.storage.publication_uri)?;
    let generation = stream_generation_id(&manifest.generation, &instrument.canonical_toml());
    let key = manifest_key(&generation);

    let streaming = Instant::now();
    let mut writers = Vec::with_capacity(instrument.candles.len());
    let mut temporaries = Vec::with_capacity(instrument.candles.len() + 1);
    for (index, spec) in instrument.candles.iter().enumerate() {
        let path = import::temporary_path(&local, &format!("audit-{generation}-{index}"))?;
        writers.push(CandleWriter::create(
            &path,
            &id,
            instrument.price_scale,
            spec.duration_seconds,
            spec.offset_seconds,
        )?);
        temporaries.push(path);
    }
    let mut finalized = Vec::new();
    let mut push = |observation: Observation| -> Result<(), String> {
        stream
            .push(observation, &mut finalized)
            .map_err(|rejection| format!("{id}: {rejection}"))?;
        for (index, candle) in finalized.drain(..) {
            writers[index].push(&candle)?;
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
    let profile_path = import::temporary_path(&local, &format!("audit-{generation}-profile"))?;
    fs::write(&profile_path, profile.to_json())
        .map_err(|error| format!("cannot write {}: {error}", profile_path.display()))?;
    temporaries.insert(0, profile_path);
    let streamed = streaming.elapsed();

    let publishing = Instant::now();
    let identities: Vec<ObjectIdentity> = temporaries
        .iter()
        .map(|path| store::identify(path))
        .collect::<Result<_, _>>()?;
    let paths =
        std::iter::once(PROFILE_OBJECT_PATH.to_string()).chain(streams.iter().map(|summary| {
            StreamSummary::object_path(summary.duration_seconds, summary.offset_seconds)
        }));
    let mut objects: Vec<ObjectRecord> = paths
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
    let temporary = import::temporary_path(&local, &format!("manifest-{generation}"))?;
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
    writeln!(out, "{line}")
        .and_then(|()| out.flush())
        .map_err(|error| format!("cannot write the report: {error}"))
}

/// Verifies and decodes every data object of a published generation in manifest order through
/// the Phase 02 readers, handing every record to `push` as a stream observation.
pub(crate) fn feed_generation(
    source_store: &Store,
    manifest: &GenerationManifest,
    price_scale: binary_alpha_engine::market::PriceScale,
    push: &mut dyn FnMut(Observation) -> Result<(), String>,
) -> Result<(), String> {
    let data_role = match manifest.source_kind {
        SourceKind::TickCsv | SourceKind::TickParquetDaily => ObjectRole::Normalized,
        SourceKind::BarParquet => ObjectRole::Source,
    };
    for object in manifest
        .objects
        .iter()
        .filter(|object| object.role == data_role)
    {
        let (_, fetched) = verify::fetch(source_store, object, true)?;
        let path = &fetched.expect("decoded objects have a local path").path;
        let location = source_store.uri(&object.key);
        match manifest.price_representation {
            PriceRepresentation::IntegerUnits { scale } => {
                archive::read_ticks_with(path, scale, |tick| push(Observation::Tick(tick)))
                    .map(|_| ())
            }
            PriceRepresentation::BinaryFloat64 => {
                let expectation = verify::bar_expectation(manifest)?;
                archive::validate_bar_file_with(path, &expectation, |bar| {
                    push(Observation::from_bar(&bar, price_scale)?)
                })
                .map(|_| ())
            }
        }
        .map_err(|reason| format!("{location}: {reason}"))?;
    }
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
        && import::same_objects(&committed.objects, &fresh.objects, identities)
}

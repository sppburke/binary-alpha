//! Preserve the exact legacy candle proof when daily publication adds a session product.
use crate::{archive::CandleWriter, audit, store::Store};
use binary_alpha_engine::{
    config::Instrument,
    dataset::GenerationManifest,
    market::format_event_time_micros,
    stream::{InstrumentProfile, InstrumentStream, Source, StreamSummary},
};
use parquet::file::reader::{FileReader, SerializedFileReader};
use sha2::{Digest, Sha256};
use std::{fs::File, path::Path};

pub(crate) fn compatible(before: &Instrument, after: &Instrument) -> bool {
    let mut candidate = after.clone();
    if before.session.is_none() {
        candidate.session = None;
    }
    before == &candidate
}

pub(crate) struct Reconstruction {
    pub streams: Vec<StreamSummary>,
    pub digest: String,
    pub profile: InstrumentProfile,
}

/// Scratch files use the existing legacy codec, preserving the same row digest and summary
/// checks as migration. They are never published as a second candle product.
pub(crate) fn reconstruct(
    store: &Store,
    source: &GenerationManifest,
    definition: &Instrument,
    scratch: &Path,
) -> Result<Reconstruction, String> {
    reconstruct_from(store, source, source, definition, scratch)
}

/// Replay a source's exact observation interval from its replacement. The caller proves
/// all lossless rows in that interval equal before using this as retirement authority.
pub(crate) fn reconstruct_from(
    store: &Store,
    replacement: &GenerationManifest,
    source: &GenerationManifest,
    definition: &Instrument,
    scratch: &Path,
) -> Result<Reconstruction, String> {
    let first =
        binary_alpha_engine::market::parse_event_time_micros(&source.coverage.first_event_time)?;
    let last =
        binary_alpha_engine::market::parse_event_time_micros(&source.coverage.last_event_time)?;
    let mut writers = Vec::new();
    let mut paths = Vec::new();
    for (index, spec) in definition.candles.iter().enumerate() {
        let path = scratch.join(format!("legacy-candles-{index}.parquet"));
        writers.push(CandleWriter::create(
            &path,
            &definition.id(),
            definition.price_scale,
            spec.duration_seconds,
            spec.offset_seconds,
        )?);
        paths.push(path);
    }
    let mut stream = InstrumentStream::new(definition, Source::from_manifest(source))?;
    let mut finalized = Vec::new();
    audit::feed_generation(
        store,
        replacement,
        definition.price_scale,
        &mut |observation| {
            let time = match observation {
                binary_alpha_engine::stream::Observation::Tick(t) => t.event_time_micros,
                binary_alpha_engine::stream::Observation::Bar(b) => b.start_micros,
            };
            if time < first || time > last {
                return Ok(());
            }
            stream
                .push(observation, &mut finalized)
                .map_err(|e| e.to_string())?;
            for (index, candle) in finalized.drain(..) {
                writers[index].push(&candle)?;
            }
            Ok(())
        },
    )?;
    let mut streams = Vec::new();
    let mut hash = Sha256::new();
    for ((writer, path), spec) in writers.into_iter().zip(paths).zip(&definition.candles) {
        let (rows, first, last) = writer.finish()?;
        streams.push(StreamSummary {
            duration_seconds: spec.duration_seconds,
            offset_seconds: spec.offset_seconds,
            rows,
            first_open_time: first.map(format_event_time_micros),
            last_close_time: last.map(format_event_time_micros),
        });
        hash.update(spec.duration_seconds.to_le_bytes());
        hash.update(spec.offset_seconds.to_le_bytes());
        let reader = SerializedFileReader::new(File::open(&path).map_err(|e| e.to_string())?)
            .map_err(|e| e.to_string())?;
        for row in reader.get_row_iter(None).map_err(|e| e.to_string())? {
            hash.update(format!("{:?}\n", row.map_err(|e| e.to_string())?).as_bytes());
        }
    }
    Ok(Reconstruction {
        streams,
        digest: binary_alpha_engine::hex(&hash.finalize()),
        profile: stream.profile(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn compatibility_allows_only_calendar_addition() {
        let old: Instrument = toml::from_str(
            r#"broker="fixture"
provider_symbol="S"
quote_currency="USD"
price_scale=4
native_granularity={kind="tick"}
candles=[{duration_seconds=5,offset_seconds=0}]
"#,
        )
        .unwrap();
        let mut new = old.clone();
        new.session = Some(binary_alpha_engine::session::Session::Always);
        assert!(compatible(&old, &new));
        new.candles[0].duration_seconds = 10;
        assert!(!compatible(&old, &new));
        new.candles[0].duration_seconds = 5;
        assert!(
            !compatible(&new, &old),
            "existing calendar cannot be removed"
        );
    }
}

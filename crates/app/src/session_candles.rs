//! The daily candle schema adds explicit fill provenance while the legacy schema stays readable.
use binary_alpha_engine::{
    continuous::{Fill, fill},
    stream::Candle,
};

pub(crate) fn schema() -> String {
    let base = crate::archive::CANDLE_SCHEMA
        .trim_end()
        .strip_suffix('}')
        .expect("candle schema closing brace");
    format!("{base}  REQUIRED BYTE_ARRAY fill (UTF8);\n}}\n")
}
pub(crate) fn check(c: &Candle, recorded: &str) -> Result<(), String> {
    if recorded != fill(c).as_str() {
        return Err("candle fill column disagrees with observation/price/volume facts".into());
    }
    if fill(c) != Fill::None && c.flags.clean() {
        return Err("filled candle must never be clean".into());
    }
    if fill(c) == Fill::Engine
        && (c.flags.complete()
            || c.volume.is_some_and(|v| v != 0.0)
            || c.open_units != c.high_units
            || c.open_units != c.low_units
            || c.open_units != c.close_units)
    {
        return Err("engine fill must be flat, incomplete, and have zero source volume".into());
    }
    Ok(())
}

/// Inventory asks whether later *observations* could alter a candle. A synthetic candle's
/// absence is proven through its close (exclusive), not by a fictitious event at known_at.
pub(crate) fn inventory_finalizer(c: &Candle) -> i64 {
    if fill(c) == Fill::Engine {
        c.close_time_micros - 1
    } else {
        c.known_at_micros
    }
}
pub(crate) fn require_schema(path: &std::path::Path) -> Result<(), String> {
    use parquet::file::reader::{FileReader, SerializedFileReader};
    let reader = SerializedFileReader::new(std::fs::File::open(path).map_err(|e| e.to_string())?)
        .map_err(|e| e.to_string())?;
    let expected =
        parquet::schema::parser::parse_message_type(&schema()).map_err(|e| e.to_string())?;
    if reader.metadata().file_metadata().schema() != &expected {
        return Err("session candle product requires explicit fill column".into());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use binary_alpha_engine::{market::InstrumentId, stream::Flags};
    #[test]
    fn filled_columns_are_explicit_and_bytes_ignore_input_batch_boundaries() {
        let root =
            std::env::temp_dir().join(format!("binary-alpha-session-codec-{}", std::process::id()));
        std::fs::create_dir_all(&root).unwrap();
        let id = InstrumentId {
            broker: "fixture".to_string().try_into().unwrap(),
            provider_symbol: "S".to_string().try_into().unwrap(),
        };
        let scale = 4.try_into().unwrap();
        let rows: Vec<_> = (0..8201)
            .map(|i| Candle {
                open_time_micros: i * 1_000_000,
                close_time_micros: (i + 1) * 1_000_000,
                known_at_micros: (i + 1) * 1_000_000,
                first_event_micros: 0,
                last_event_micros: 0,
                active_span_micros: 0,
                open_units: 100,
                high_units: 100,
                low_units: 100,
                close_units: 100,
                observations: if i % 2 == 0 { 0 } else { 1 },
                duplicates: 0,
                volume: Some(0.0),
                gap_before_micros: None,
                max_gap_inside_micros: 0,
                missing_buckets_before: 0,
                frozen_observations: 0,
                frozen_micros: 0,
                max_jump_basis_points: 0,
                max_delayed_jump_basis_points: 0,
                max_reopen_jump_basis_points: 0,
                flags: Flags {
                    hard_low_activity: i % 2 == 0,
                    frozen: true,
                    ..Flags::default()
                },
            })
            .collect();
        let mut expected = None;
        for batch in [777, 1024, 65536] {
            let path = root.join(format!("{batch}.parquet"));
            crate::daily::write_candles(
                &path,
                "1970-01-01",
                &id,
                scale,
                1,
                0,
                rows.chunks(batch).map(|chunk| chunk.to_vec()),
            )
            .unwrap();
            require_schema(&path).unwrap();
            let bytes = std::fs::read(&path).unwrap();
            if let Some(expected) = &expected {
                assert_eq!(&bytes, expected);
            } else {
                expected = Some(bytes);
            }
            assert_eq!(
                crate::daily::read_candles(&path, "1970-01-01", &id, scale, 1, 0).unwrap(),
                rows
            );
            std::fs::remove_file(path).unwrap();
        }
        let path = root.join("legacy.parquet");
        let mut legacy = crate::archive::CandleWriter::create(&path, &id, scale, 1, 0).unwrap();
        legacy.push(&rows[0]).unwrap();
        legacy.finish().unwrap();
        assert!(require_schema(&path).unwrap_err().contains("explicit fill"));
        std::fs::remove_file(path).unwrap();
        std::fs::remove_dir(root).unwrap();
    }
}

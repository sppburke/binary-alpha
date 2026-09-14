//! Immutable dataset generations: roles, capabilities, object records, and ready manifests.
//!
//! A generation is identified by its inputs; its ready manifest is the sole publication record.
//! `docs/contracts.md`, section "Historical datasets", is the normative description.

use std::fmt;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::market::{BrokerId, InstrumentId, PriceScale, ProviderSymbol};

/// The manifest schema written and accepted by this checkout.
pub const MANIFEST_SCHEMA_VERSION: u32 = 1;

/// Domain separator hashed before the generation identity text.
const GENERATION_DOMAIN_V1: &[u8] = b"binary-alpha dataset generation v1\n";

crate::string_enum! {
    /// The one role a generation carries; holdout is never an import input.
    DatasetRole "role" {
        Development => "development",
        Evaluation => "evaluation",
        Holdout => "holdout",
    }
}

crate::string_enum! {
    /// The observed input a generation was built from.
    SourceKind "source_kind" {
        TickCsv => "tick_csv",
        BrokerHistory => "broker_history",
        TickParquetDaily => "tick_parquet_daily",
        BarParquet => "bar_parquet",
    }
}

crate::string_enum! {
    /// What a source can provide; consumers derive every rejection from this list.
    Capability "capability" {
        Ticks => "ticks",
        Bars => "bars",
    }
}

crate::string_enum! {
    /// Why an object is in a generation.
    ObjectRole "object_role" {
        Source => "source",
        Provenance => "provenance",
        Normalized => "normalized",
    }
}

crate::string_enum! {
    /// The unit of every event time recorded for the generation.
    TimeUnit "time_unit" {
        Microsecond => "microsecond",
        Second => "second",
    }
}

/// The native granularity of the source.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum NativeGranularity {
    Tick,
    Bar { period_seconds: u16 },
}

impl<'de> Deserialize<'de> for NativeGranularity {
    /// Accepts exactly `{ kind = "tick" }` or `{ kind = "bar", period_seconds = N }`; a derived
    /// internally tagged deserializer would ignore any other key.
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Raw {
            kind: String,
            #[serde(default)]
            period_seconds: Option<u16>,
        }
        let raw = Raw::deserialize(deserializer)?;
        match (raw.kind.as_str(), raw.period_seconds) {
            ("tick", None) => Ok(Self::Tick),
            ("bar", Some(period_seconds)) => Ok(Self::Bar { period_seconds }),
            ("tick", Some(_)) => Err(serde::de::Error::custom(
                "period_seconds belongs to kind `bar`, not `tick`",
            )),
            ("bar", None) => Err(serde::de::Error::missing_field("period_seconds")),
            (kind, _) => Err(serde::de::Error::custom(format!(
                "unknown kind `{kind}`, expected one of `tick`, `bar`"
            ))),
        }
    }
}

impl fmt::Display for NativeGranularity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Tick => f.write_str("tick"),
            Self::Bar { period_seconds } => write!(f, "{period_seconds}-second bar"),
        }
    }
}

/// How prices are represented in the generation's data objects.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum PriceRepresentation {
    /// Signed 64-bit integer units with this many decimal fraction digits.
    IntegerUnits { scale: PriceScale },
    /// The archive's binary floating point, preserved byte-for-byte and never used as money.
    BinaryFloat64,
}

/// First and last provider event time, rendered as `YYYY-MM-DDTHH:MM:SS.ffffffZ`.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct Coverage {
    pub first_event_time: String,
    pub last_event_time: String,
}

/// An original input location with its identity.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct Input {
    pub path: String,
    pub bytes: u64,
    pub sha256: String,
}

/// The bar archive's interval contract and where it was observed.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct IntervalContract {
    pub closed: String,
    pub frequency: String,
    pub interval: String,
    pub label: String,
    pub offset_seconds: i64,
    pub origin: String,
    pub timestamp_semantics: String,
    /// `parquet_metadata` when every file embeds the contract, otherwise the source manifest's
    /// recorded provenance text.
    pub provenance: String,
}

impl IntervalContract {
    /// The exact contract this checkout admits: left-closed five-second bars whose timestamp is
    /// the bar start on the Unix epoch grid, with a non-empty provenance.
    pub fn validate(&self) -> Result<(), String> {
        let expected = [
            ("closed", self.closed.as_str(), "left"),
            ("frequency", self.frequency.as_str(), "5s"),
            (
                "interval",
                self.interval.as_str(),
                "[timestamp,timestamp+5s)",
            ),
            ("label", self.label.as_str(), "left"),
            ("origin", self.origin.as_str(), "unix_epoch_utc"),
            (
                "timestamp_semantics",
                self.timestamp_semantics.as_str(),
                "bar_start",
            ),
        ];
        for (key, actual, approved) in expected {
            if actual != approved {
                return Err(format!(
                    "interval contract `{key}` is `{actual}`, expected `{approved}`"
                ));
            }
        }
        if self.offset_seconds != 0 {
            return Err(format!(
                "interval contract `offset_seconds` is {}, expected 0",
                self.offset_seconds
            ));
        }
        if self.provenance.is_empty() || self.provenance.bytes().any(|byte| byte.is_ascii_control())
        {
            return Err("interval contract provenance must be non-empty plain text".to_string());
        }
        Ok(())
    }
}

/// One retained or published object.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct ObjectRecord {
    pub role: ObjectRole,
    /// Path relative to the dataset root, or the normalized object's name.
    pub path: String,
    /// `objects/SHA256HEX`, relative to the store root.
    pub key: String,
    pub bytes: u64,
    pub sha256: String,
    /// The destination's CRC32C when the destination reports one.
    pub crc32c: Option<u32>,
    /// The destination's object generation when the destination reports one.
    pub generation: Option<i64>,
}

fn is_hex64(text: &str) -> bool {
    text.len() == 64
        && text
            .bytes()
            .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
}

/// The content-addressed key of an object with this SHA-256.
pub fn object_key(sha256: &str) -> String {
    format!("objects/{sha256}")
}

/// The ready-manifest key of a generation.
pub fn manifest_key(generation: &str) -> String {
    format!("manifests/{generation}/ready.json")
}

/// The identity of a generation: the dataset identity and every input object, so equal inputs
/// always name the same generation regardless of the producing code.
pub fn generation_id(
    instrument: &InstrumentId,
    source_kind: SourceKind,
    role: DatasetRole,
    price_scale: Option<PriceScale>,
    inputs: &[ObjectRecord],
) -> String {
    let mut hasher = Sha256::new();
    hasher.update(GENERATION_DOMAIN_V1);
    for line in [
        instrument.broker.as_str(),
        instrument.provider_symbol.as_str(),
        source_kind.as_str(),
        role.as_str(),
    ] {
        hasher.update(line.as_bytes());
        hasher.update(b"\n");
    }
    if let Some(scale) = price_scale {
        hasher.update(scale.digits().to_string().as_bytes());
        hasher.update(b"\n");
    }
    let mut sorted: Vec<&ObjectRecord> = inputs
        .iter()
        .filter(|object| object.role != ObjectRole::Normalized)
        .collect();
    sorted.sort_by(|a, b| a.path.cmp(&b.path));
    for object in sorted {
        hasher.update(
            format!(
                "{}\t{}\t{}\t{}\n",
                object.role, object.path, object.sha256, object.bytes
            )
            .as_bytes(),
        );
    }
    crate::hex(&hasher.finalize())
}

/// The ready manifest of one dataset generation. Field order is the serialization order.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct GenerationManifest {
    pub schema_version: u32,
    pub generation: String,
    pub broker: BrokerId,
    pub provider_symbol: ProviderSymbol,
    pub instrument: String,
    pub role: DatasetRole,
    pub source_kind: SourceKind,
    pub native_granularity: NativeGranularity,
    pub time_unit: TimeUnit,
    pub price_representation: PriceRepresentation,
    pub coverage: Coverage,
    pub row_count: u64,
    pub capabilities: Vec<Capability>,
    pub config_hash: String,
    pub code_revision: String,
    pub inputs: Vec<Input>,
    pub interval: Option<IntervalContract>,
    pub objects: Vec<ObjectRecord>,
}

impl GenerationManifest {
    /// The exact bytes published as `ready.json`: pretty JSON in field order and one trailing
    /// line feed.
    pub fn to_json(&self) -> Vec<u8> {
        let mut bytes = serde_json::to_vec_pretty(self).expect("a manifest serializes");
        bytes.push(b'\n');
        bytes
    }

    /// Parses a ready manifest and checks the invariants every consumer relies on before it
    /// trusts a key: a hexadecimal generation that matches the recorded inputs, consistent
    /// dataset fields, and content-addressed objects with unique clean paths.
    pub fn from_json(bytes: &[u8]) -> Result<Self, String> {
        let manifest: Self = serde_json::from_slice(bytes).map_err(|error| error.to_string())?;
        if manifest.schema_version != MANIFEST_SCHEMA_VERSION {
            return Err(format!(
                "unsupported manifest schema_version {}, expected {MANIFEST_SCHEMA_VERSION}",
                manifest.schema_version
            ));
        }
        manifest.validate()?;
        Ok(manifest)
    }

    fn validate(&self) -> Result<(), String> {
        if !is_hex64(&self.generation) {
            return Err(format!(
                "generation `{}` is not sixty-four lowercase hexadecimal digits",
                self.generation
            ));
        }
        let instrument = InstrumentId {
            broker: self.broker.clone(),
            provider_symbol: self.provider_symbol.clone(),
        };
        if self.instrument != instrument.to_string() {
            return Err(format!(
                "instrument `{}` is not `{instrument}`",
                self.instrument
            ));
        }
        let scale = match (
            self.source_kind,
            self.price_representation,
            self.native_granularity,
            self.time_unit,
            self.interval.is_some(),
            self.capabilities.as_slice(),
        ) {
            (
                SourceKind::TickCsv | SourceKind::TickParquetDaily | SourceKind::BrokerHistory,
                PriceRepresentation::IntegerUnits { scale },
                NativeGranularity::Tick,
                TimeUnit::Microsecond,
                false,
                [Capability::Ticks],
            ) => Some(scale),
            (
                SourceKind::BarParquet,
                PriceRepresentation::BinaryFloat64,
                NativeGranularity::Bar { period_seconds: 5 },
                TimeUnit::Second,
                true,
                [Capability::Bars],
            ) => None,
            _ => {
                return Err(format!(
                    "source kind {}, price representation, granularity, time unit, interval, and capabilities disagree",
                    self.source_kind
                ));
            }
        };
        if let Some(interval) = &self.interval {
            interval.validate()?;
        }
        validate_objects(&self.objects)?;
        let normalized = self
            .objects
            .iter()
            .filter(|object| object.role == ObjectRole::Normalized)
            .count();
        let expected = usize::from(scale.is_some());
        if normalized != expected || self.objects.len() == normalized {
            return Err(format!(
                "expected {expected} normalized object among {} objects, found {normalized}",
                self.objects.len()
            ));
        }
        if self.source_kind == SourceKind::BrokerHistory
            && (!self.objects.iter().any(|o| o.role == ObjectRole::Source)
                || !self.objects.iter().any(|o| {
                    o.role == ObjectRole::Provenance && o.path == "provenance/coverage.json"
                })
                || !self.objects.iter().any(|o| {
                    o.role == ObjectRole::Normalized && o.path == "normalized/ticks.parquet"
                }))
        {
            return Err("broker_history requires raw source pages, provenance/coverage.json, and normalized/ticks.parquet".into());
        }
        if generation_id(
            &instrument,
            self.source_kind,
            self.role,
            scale,
            &self.objects,
        ) != self.generation
        {
            return Err(format!(
                "generation `{}` does not match the recorded inputs",
                self.generation
            ));
        }
        Ok(())
    }

    /// The key this manifest is published at.
    pub fn key(&self) -> String {
        manifest_key(&self.generation)
    }

    /// Rejects a consumer request the source cannot provide, with a machine-readable reason.
    pub fn require(&self, capability: Capability) -> Result<(), CapabilityError> {
        if self.capabilities.contains(&capability) {
            return Ok(());
        }
        Err(CapabilityError {
            required: capability,
            provided: self.capabilities.clone(),
            instrument: self.instrument.clone(),
            generation: self.generation.clone(),
        })
    }
}

/// The object invariants every consumer relies on before it trusts a key: content-addressed
/// keys and unique clean paths without a control character.
pub fn validate_objects(objects: &[ObjectRecord]) -> Result<(), String> {
    for (index, object) in objects.iter().enumerate() {
        if !is_hex64(&object.sha256) || object.key != object_key(&object.sha256) {
            return Err(format!(
                "object `{}` is not content-addressed by its SHA-256",
                object.path
            ));
        }
        if object.path.bytes().any(|byte| byte.is_ascii_control()) {
            return Err(format!(
                "object path `{}` contains a control character",
                object.path.escape_default()
            ));
        }
        crate::config::relative_path(&object.path)
            .map_err(|reason| format!("object path `{}`: {reason}", object.path))?;
        if objects[..index]
            .iter()
            .any(|earlier| earlier.path == object.path)
        {
            return Err(format!("object path `{}` is listed twice", object.path));
        }
    }
    Ok(())
}

/// A consumer asked a generation for a capability its source does not provide.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct CapabilityError {
    pub required: Capability,
    pub provided: Vec<Capability>,
    pub instrument: String,
    pub generation: String,
}

impl fmt::Display for CapabilityError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}",
            serde_json::to_string(self).expect("a capability error serializes")
        )
    }
}

impl std::error::Error for CapabilityError {}

#[cfg(test)]
mod tests {
    use super::*;

    fn object(role: ObjectRole, path: &str, sha256: &str) -> ObjectRecord {
        let sha256 = format!("{sha256:0>64}");
        ObjectRecord {
            role,
            path: path.to_string(),
            key: object_key(&sha256),
            bytes: 1,
            sha256,
            crc32c: None,
            generation: None,
        }
    }

    fn manifest() -> GenerationManifest {
        let instrument = InstrumentId {
            broker: BrokerId::try_from("pocket_option".to_string()).unwrap(),
            provider_symbol: ProviderSymbol::try_from("#AAPL".to_string()).unwrap(),
        };
        let objects = vec![
            object(ObjectRole::Provenance, "download_manifest.json", "bb"),
            object(
                ObjectRole::Source,
                "dataset/parquet/year=2025/month=05/part-00000.parquet",
                "aa",
            ),
        ];
        GenerationManifest {
            schema_version: MANIFEST_SCHEMA_VERSION,
            generation: generation_id(
                &instrument,
                SourceKind::BarParquet,
                DatasetRole::Development,
                None,
                &objects,
            ),
            broker: instrument.broker.clone(),
            provider_symbol: instrument.provider_symbol.clone(),
            instrument: instrument.to_string(),
            role: DatasetRole::Development,
            source_kind: SourceKind::BarParquet,
            native_granularity: NativeGranularity::Bar { period_seconds: 5 },
            time_unit: TimeUnit::Second,
            price_representation: PriceRepresentation::BinaryFloat64,
            coverage: Coverage {
                first_event_time: "2025-05-19T11:15:00.000000Z".to_string(),
                last_event_time: "2026-08-24T23:50:55.000000Z".to_string(),
            },
            row_count: 7_992_432,
            capabilities: vec![Capability::Bars],
            config_hash: "v2:sha256:0".to_string(),
            code_revision: "unavailable".to_string(),
            inputs: vec![],
            interval: Some(IntervalContract {
                closed: "left".to_string(),
                frequency: "5s".to_string(),
                interval: "[timestamp,timestamp+5s)".to_string(),
                label: "left".to_string(),
                offset_seconds: 0,
                origin: "unix_epoch_utc".to_string(),
                timestamp_semantics: "bar_start".to_string(),
                provenance: "parquet_metadata".to_string(),
            }),
            objects,
        }
    }

    #[test]
    fn generation_identity_ignores_object_order_and_normalized_outputs() {
        let manifest = manifest();
        let instrument = InstrumentId {
            broker: manifest.broker.clone(),
            provider_symbol: manifest.provider_symbol.clone(),
        };
        let mut reordered = manifest.objects.clone();
        reordered.reverse();
        reordered.push(object(ObjectRole::Normalized, "ticks.parquet", "cc"));
        assert_eq!(
            generation_id(
                &instrument,
                SourceKind::BarParquet,
                DatasetRole::Development,
                None,
                &reordered
            ),
            manifest.generation
        );
        assert_ne!(
            generation_id(
                &instrument,
                SourceKind::BarParquet,
                DatasetRole::Evaluation,
                None,
                &reordered
            ),
            manifest.generation
        );
    }

    #[test]
    fn generation_identity_is_fixed_by_its_inputs() {
        assert_eq!(
            manifest().generation,
            "c81fe40d17a4bd2914e0b941bdc895147ef9f21cc713e7e1d910e652dbab867e"
        );
    }

    #[test]
    fn manifests_reject_untrusted_keys_and_inconsistent_fields() {
        let mut moved = manifest();
        moved.objects[0].key = "../../etc/passwd".to_string();
        assert!(
            GenerationManifest::from_json(&moved.to_json())
                .unwrap_err()
                .contains("content-addressed")
        );
        let mut renamed = manifest();
        renamed.generation = "0".repeat(64);
        assert!(
            GenerationManifest::from_json(&renamed.to_json())
                .unwrap_err()
                .contains("does not match the recorded inputs")
        );
        let mut duplicated = manifest();
        let copy = duplicated.objects[0].clone();
        duplicated.objects.push(copy);
        assert!(
            GenerationManifest::from_json(&duplicated.to_json())
                .unwrap_err()
                .contains("listed twice")
        );
        let mut period = manifest();
        period.native_granularity = NativeGranularity::Bar { period_seconds: 10 };
        assert!(
            GenerationManifest::from_json(&period.to_json())
                .unwrap_err()
                .contains("disagree")
        );
        let mut ticks = manifest();
        ticks.capabilities = vec![Capability::Ticks];
        assert!(
            GenerationManifest::from_json(&ticks.to_json())
                .unwrap_err()
                .contains("disagree")
        );
        let mut daily = manifest();
        daily.source_kind = SourceKind::TickParquetDaily;
        assert!(
            GenerationManifest::from_json(&daily.to_json())
                .unwrap_err()
                .contains("disagree"),
            "a daily tick archive never carries the bar contract"
        );
        let mut instrument = manifest();
        instrument.instrument = "other".to_string();
        assert!(
            GenerationManifest::from_json(&instrument.to_json())
                .unwrap_err()
                .contains("is not `pocket_option:#AAPL`")
        );
    }

    #[test]
    fn manifests_round_trip_byte_for_byte_and_reject_drift() {
        let manifest = manifest();
        let bytes = manifest.to_json();
        assert!(bytes.ends_with(b"}\n"));
        let parsed = GenerationManifest::from_json(&bytes).unwrap();
        assert_eq!(parsed, manifest);
        assert_eq!(parsed.to_json(), bytes);
        assert_eq!(
            parsed.key(),
            format!("manifests/{}/ready.json", manifest.generation)
        );
        let drifted = String::from_utf8(bytes.clone())
            .unwrap()
            .replace("\"row_count\"", "\"rows\"");
        assert!(GenerationManifest::from_json(drifted.as_bytes()).is_err());
        let unsupported = String::from_utf8(bytes)
            .unwrap()
            .replace("\"schema_version\": 1", "\"schema_version\": 2");
        assert!(
            GenerationManifest::from_json(unsupported.as_bytes())
                .unwrap_err()
                .contains("schema_version 2")
        );
    }

    #[test]
    fn tick_consumers_reject_bar_manifests_with_a_machine_readable_reason() {
        let manifest = manifest();
        manifest.require(Capability::Bars).unwrap();
        let error = manifest.require(Capability::Ticks).unwrap_err();
        let rendered: serde_json::Value = serde_json::from_str(&error.to_string()).unwrap();
        assert_eq!(rendered["required"], "ticks");
        assert_eq!(rendered["provided"], serde_json::json!(["bars"]));
        assert_eq!(rendered["instrument"], "pocket_option:#AAPL");
    }
}

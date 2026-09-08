//! The version-one configuration envelope: validation, canonical form, and content hash.
//!
//! `docs/contracts.md`, section "Configuration", is the normative description of every rule here.

use serde::de::Error as _;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use sha2::{Digest, Sha256};

/// Domain separator hashed before the canonical document; changing it or the canonical form
/// increments the `v1` prefix rendered by [`Config::content_hash`].
const HASH_DOMAIN_V1: &[u8] = b"binary-alpha config hash v1\n";

/// A validated configuration document.
///
/// Field order is the canonical serialization order.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub schema_version: SchemaVersion,
    pub run_mode: RunMode,
}

impl Config {
    /// Parses and validates a TOML document, rejecting unknown fields and unsupported values
    /// with an error that names the field and its position.
    ///
    /// ```
    /// use binary_alpha_engine::config::{Config, RunMode};
    ///
    /// let config = Config::parse("schema_version = 1\nrun_mode = \"research\"\n").unwrap();
    /// assert_eq!(config.run_mode, RunMode::Research);
    /// assert!(Config::parse("schema_version = 1\nrun_mode = \"research\"\napi_token = \"x\"\n").is_err());
    /// ```
    pub fn parse(source: &str) -> Result<Self, toml::de::Error> {
        toml::from_str(source)
    }

    /// The canonical TOML document: schema field order, standard formatting, no comments.
    pub fn canonical_toml(&self) -> String {
        toml::to_string(self).expect("a validated configuration serializes")
    }

    /// The version-one content hash of the canonical document, rendered as `v1:sha256:`
    /// followed by sixty-four lowercase hexadecimal digits.
    pub fn content_hash(&self) -> String {
        let mut hasher = Sha256::new();
        hasher.update(HASH_DOMAIN_V1);
        hasher.update(self.canonical_toml().as_bytes());
        let digits: String = hasher
            .finalize()
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect();
        format!("v1:sha256:{digits}")
    }
}

/// The configuration schema version; only version 1 exists.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(try_from = "u32", into = "u32")]
pub struct SchemaVersion(u32);

impl TryFrom<u32> for SchemaVersion {
    type Error = String;

    fn try_from(version: u32) -> Result<Self, Self::Error> {
        match version {
            1 => Ok(Self(version)),
            other => Err(format!("unsupported schema_version {other}, expected 1")),
        }
    }
}

impl From<SchemaVersion> for u32 {
    fn from(version: SchemaVersion) -> Self {
        version.0
    }
}

/// The run mode a configuration is written for; it selects capabilities, never semantics.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunMode {
    Research,
    Replay,
    Paper,
    Live,
}

impl RunMode {
    const ALL: [Self; 4] = [Self::Research, Self::Replay, Self::Paper, Self::Live];

    /// The spelling accepted and emitted in a configuration document.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Research => "research",
            Self::Replay => "replay",
            Self::Paper => "paper",
            Self::Live => "live",
        }
    }
}

impl Serialize for RunMode {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(self.as_str())
    }
}

/// Accepts only a string; a derived enum deserializer would also accept a single-key table.
impl<'de> Deserialize<'de> for RunMode {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let name = String::deserialize(deserializer)?;
        Self::ALL
            .into_iter()
            .find(|mode| mode.as_str() == name)
            .ok_or_else(|| {
                let expected = Self::ALL
                    .map(|mode| format!("`{}`", mode.as_str()))
                    .join(", ");
                D::Error::custom(format!(
                    "unknown run_mode `{name}`, expected one of {expected}"
                ))
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_run_mode_has_one_canonical_form_and_the_example_hash_is_fixed() {
        for mode in RunMode::ALL {
            let name = mode.as_str();
            let source =
                format!("# comment\nrun_mode = \"{name}\" # trailing\n\nschema_version=1\n");
            let canonical = format!("schema_version = 1\nrun_mode = \"{name}\"\n");
            assert_eq!(Config::parse(&source).unwrap().canonical_toml(), canonical);
        }
        let research = Config::parse("schema_version = 1\nrun_mode = \"research\"\n").unwrap();
        assert_eq!(
            research.content_hash(),
            "v1:sha256:c62f3b3e1a61e1897c2c08f5d39db1e2b7aa8e96229623c73affb9a1862b7e2d"
        );
    }
}

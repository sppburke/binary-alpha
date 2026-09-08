//! The version-one configuration envelope: validation, canonical form, and content hash.
//!
//! `docs/contracts.md`, section "Configuration", is the normative description of every rule here.

use std::fmt;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

/// Domain separator hashed before the canonical document; changing it or the canonical form
/// increments the version rendered by [`ContentHash`].
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
    pub fn parse(source: &str) -> Result<Self, Error> {
        toml::from_str(source).map_err(Error)
    }

    /// The canonical TOML document: schema field order, standard formatting, no comments.
    pub fn canonical_toml(&self) -> String {
        toml::to_string(self).expect("a validated configuration serializes")
    }

    /// The version-one content hash of the canonical document.
    pub fn content_hash(&self) -> ContentHash {
        let mut hasher = Sha256::new();
        hasher.update(HASH_DOMAIN_V1);
        hasher.update(self.canonical_toml().as_bytes());
        ContentHash(hasher.finalize().into())
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
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RunMode {
    Research,
    Replay,
    Paper,
    Live,
}

/// A version-one content hash, rendered as `v1:sha256:` followed by sixty-four hexadecimal digits.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ContentHash([u8; 32]);

impl fmt::Display for ContentHash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("v1:sha256:")?;
        self.0.iter().try_for_each(|byte| write!(f, "{byte:02x}"))
    }
}

/// A field-specific validation error carrying the offending line and column.
#[derive(Debug)]
pub struct Error(toml::de::Error);

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl std::error::Error for Error {}

#[cfg(test)]
mod tests {
    use super::*;

    const CANONICAL: &str = "schema_version = 1\nrun_mode = \"research\"\n";
    const HASH: &str = "v1:sha256:c62f3b3e1a61e1897c2c08f5d39db1e2b7aa8e96229623c73affb9a1862b7e2d";

    #[test]
    fn equivalent_documents_share_canonical_form_and_hash() {
        let reordered = "# comment\nrun_mode = \"research\" # trailing\n\nschema_version=1\n";
        for source in [CANONICAL, reordered] {
            let config = Config::parse(source).unwrap();
            assert_eq!(config.canonical_toml(), CANONICAL);
            assert_eq!(config.content_hash().to_string(), HASH);
        }
    }

    #[test]
    fn unsupported_schema_version_is_rejected_with_its_value() {
        let error = Config::parse("schema_version = 2\nrun_mode = \"research\"\n").unwrap_err();
        assert!(
            error
                .to_string()
                .contains("unsupported schema_version 2, expected 1"),
            "{error}"
        );
    }
}

//! The version-one configuration envelope: validation, canonical form, and content hash.
//!
//! `docs/contracts.md`, section "Configuration", is the normative description of every rule here.

use std::fmt;
use std::path::{Path, PathBuf};

use serde::de::Error as _;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use sha2::{Digest, Sha256};

use crate::dataset::DatasetRole;
use crate::market::{BrokerId, PriceScale, ProviderSymbol};

/// Domain separator hashed before the canonical document; changing it or the canonical form
/// increments the `v2` prefix rendered by [`Config::content_hash`].
const HASH_DOMAIN_V2: &[u8] = b"binary-alpha config hash v2\n";

/// A validated configuration document.
///
/// Field order is the canonical serialization order.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub schema_version: SchemaVersion,
    pub run_mode: RunMode,
    pub storage: Storage,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub import: Option<Import>,
}

impl Config {
    /// Parses and validates a TOML document, rejecting unknown fields and unsupported values
    /// with an error that names the field and, for a present value, its position.
    ///
    /// ```
    /// use binary_alpha_engine::config::{Config, RunMode};
    ///
    /// let source = "schema_version = 1\nrun_mode = \"research\"\n\n[storage]\nhistorical_data_dir = \"../historical_data\"\npublication_uri = \"gs://example-bucket/historical\"\n";
    /// let config = Config::parse(source).unwrap();
    /// assert_eq!(config.run_mode, RunMode::Research);
    /// assert!(Config::parse(&format!("{source}api_token = \"x\"\n")).is_err());
    /// ```
    pub fn parse(source: &str) -> Result<Self, toml::de::Error> {
        let config: Self = toml::from_str(source)?;
        config.validate().map_err(toml::de::Error::custom)?;
        Ok(config)
    }

    /// Cross-field rules that a single field's deserializer cannot see.
    fn validate(&self) -> Result<(), String> {
        if self.run_mode != RunMode::Research
            && matches!(self.storage.publication_uri, PublicationUri::Filesystem(_))
        {
            return Err(format!(
                "storage.publication_uri: a `file://` destination is the non-live test boundary and requires run_mode `research`, not `{}`",
                self.run_mode.as_str()
            ));
        }
        let Some(import) = &self.import else {
            return Ok(());
        };
        for (index, source) in import.sources.iter().enumerate() {
            let field = |name: &str| format!("import.sources[{index}].{name}");
            if source.role() == DatasetRole::Holdout {
                return Err(format!(
                    "{}: holdout data is never an import input",
                    field("role")
                ));
            }
            if let Source::BarParquetCollection {
                manifest,
                provenance,
                ..
            } = source
            {
                relative_path(&manifest.to_string_lossy())
                    .map_err(|reason| format!("{}: {reason}", field("manifest")))?;
                let provenance = provenance.as_deref().unwrap_or(&[]);
                for (position, entry) in provenance.iter().enumerate() {
                    relative_path(&entry.to_string_lossy())
                        .map_err(|reason| format!("{}: {reason}", field("provenance")))?;
                    if entry == manifest || provenance[..position].contains(entry) {
                        return Err(format!(
                            "{}: {} is listed twice",
                            field("provenance"),
                            entry.display()
                        ));
                    }
                }
            }
            if import.sources[..index]
                .iter()
                .any(|earlier| earlier.path() == source.path())
            {
                return Err(format!(
                    "{}: duplicate source path {}",
                    field("path"),
                    source.path().display()
                ));
            }
        }
        Ok(())
    }

    /// The canonical TOML document: schema field order, standard formatting, no comments.
    pub fn canonical_toml(&self) -> String {
        toml::to_string(self).expect("a validated configuration serializes")
    }

    /// The version-two content hash of the canonical document, rendered as `v2:sha256:`
    /// followed by sixty-four lowercase hexadecimal digits.
    pub fn content_hash(&self) -> String {
        let mut hasher = Sha256::new();
        hasher.update(HASH_DOMAIN_V2);
        hasher.update(self.canonical_toml().as_bytes());
        format!("v2:sha256:{}", crate::hex(&hasher.finalize()))
    }
}

/// A relative path exactly as written, which stays inside its root without normalization: not
/// empty, no leading slash, and no empty, `.`, or `..` segment.
pub fn relative_path(text: &str) -> Result<&Path, String> {
    if text.is_empty()
        || text.starts_with('/')
        || text
            .split('/')
            .any(|segment| matches!(segment, "" | "." | ".."))
    {
        return Err(format!(
            "path `{text}` must stay inside its root: no leading slash and no empty, `.`, or `..` segment"
        ));
    }
    Ok(Path::new(text))
}

/// A required non-empty path.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(try_from = "PathBuf", into = "PathBuf")]
pub struct ConfigPath(PathBuf);

impl ConfigPath {
    /// The path as written; the application resolves a relative path against the
    /// configuration file's directory.
    pub fn as_path(&self) -> &Path {
        &self.0
    }
}

impl TryFrom<PathBuf> for ConfigPath {
    type Error = String;

    fn try_from(path: PathBuf) -> Result<Self, Self::Error> {
        if path.as_os_str().is_empty() {
            return Err("expected a non-empty path".to_string());
        }
        Ok(Self(path))
    }
}

impl From<ConfigPath> for PathBuf {
    fn from(path: ConfigPath) -> Self {
        path.0
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

crate::string_enum! {
    /// The run mode a configuration is written for; it selects capabilities, never semantics.
    RunMode "run_mode" {
        Research => "research",
        Replay => "replay",
        Paper => "paper",
        Live => "live",
    }
}

/// The retained historical-data folder and the durable publication destination.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Storage {
    pub historical_data_dir: ConfigPath,
    pub publication_uri: PublicationUri,
}

/// Where ready manifests and objects are published: Google Cloud Storage in every run mode, or
/// the filesystem implementation as the non-live test boundary under `research`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PublicationUri {
    /// `gs://BUCKET/PREFIX`; the prefix may be empty and never starts or ends with `/`.
    GoogleCloudStorage { bucket: String, prefix: String },
    /// `file:///ABSOLUTE/DIR`.
    Filesystem(PathBuf),
}

impl fmt::Display for PublicationUri {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::GoogleCloudStorage { bucket, prefix } if prefix.is_empty() => {
                write!(f, "gs://{bucket}")
            }
            Self::GoogleCloudStorage { bucket, prefix } => write!(f, "gs://{bucket}/{prefix}"),
            Self::Filesystem(path) => write!(f, "file://{}", path.display()),
        }
    }
}

impl std::str::FromStr for PublicationUri {
    type Err = String;

    fn from_str(uri: &str) -> Result<Self, Self::Err> {
        if let Some(rest) = uri.strip_prefix("gs://") {
            let (bucket, prefix) = rest.split_once('/').unwrap_or((rest, ""));
            if bucket.is_empty() {
                return Err(format!("`{uri}` names no bucket"));
            }
            let prefix = prefix.trim_matches('/');
            if prefix.split('/').any(|segment| segment.is_empty()) && !prefix.is_empty() {
                return Err(format!("`{uri}` has an empty prefix segment"));
            }
            return Ok(Self::GoogleCloudStorage {
                bucket: bucket.to_string(),
                prefix: prefix.to_string(),
            });
        }
        if let Some(path) = uri.strip_prefix("file://") {
            if !path.starts_with('/') {
                return Err(format!("`{uri}` must be an absolute `file:///` path"));
            }
            return Ok(Self::Filesystem(PathBuf::from(path)));
        }
        Err(format!("`{uri}` must start with `gs://` or `file:///`"))
    }
}

impl Serialize for PublicationUri {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(self)
    }
}

impl<'de> Deserialize<'de> for PublicationUri {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        String::deserialize(deserializer)?
            .parse()
            .map_err(D::Error::custom)
    }
}

/// The explicit source inventory consumed only by `data import`.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Import {
    pub sources: Vec<Source>,
}

/// One declared source; every entry publishes one or more instrument datasets.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(tag = "kind", deny_unknown_fields)]
pub enum Source {
    /// One three-column native tick file.
    #[serde(rename = "tick_csv")]
    TickCsv {
        path: ConfigPath,
        broker: BrokerId,
        role: DatasetRole,
        provider_symbol: ProviderSymbol,
        source_symbol: ProviderSymbol,
        price_scale: PriceScale,
    },
    /// One collection root holding monthly five-second Parquet bars per asset.
    #[serde(rename = "bar_parquet_collection")]
    BarParquetCollection {
        path: ConfigPath,
        broker: BrokerId,
        role: DatasetRole,
        manifest: PathBuf,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        provenance: Option<Vec<PathBuf>>,
    },
}

impl Source {
    /// The declared file or collection root as written.
    pub fn path(&self) -> &Path {
        match self {
            Self::TickCsv { path, .. } | Self::BarParquetCollection { path, .. } => path.as_path(),
        }
    }

    pub fn broker(&self) -> &BrokerId {
        match self {
            Self::TickCsv { broker, .. } | Self::BarParquetCollection { broker, .. } => broker,
        }
    }

    pub fn role(&self) -> DatasetRole {
        match self {
            Self::TickCsv { role, .. } | Self::BarParquetCollection { role, .. } => *role,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const STORAGE: &str = "\n[storage]\nhistorical_data_dir = \"../historical_data\"\npublication_uri = \"gs://example-bucket/historical\"\n";

    #[test]
    fn every_run_mode_has_one_canonical_form() {
        for name in ["research", "replay", "paper", "live"] {
            let source = format!(
                "# comment\nrun_mode = \"{name}\" # trailing\n\nschema_version=1\n{STORAGE}"
            );
            let canonical = format!("schema_version = 1\nrun_mode = \"{name}\"\n{STORAGE}");
            assert_eq!(Config::parse(&source).unwrap().canonical_toml(), canonical);
        }
    }

    #[test]
    fn sources_round_trip_through_the_canonical_form() {
        let source = "schema_version = 1\nrun_mode = \"research\"\n\n[storage]\nhistorical_data_dir = \"/data/historical\"\npublication_uri = \"file:///data/published\"\n\n[[import.sources]]\nkind = \"tick_csv\"\npath = \"ticks.csv\"\nbroker = \"pocket_option\"\nrole = \"development\"\nprovider_symbol = \"AEDCNY_otc\"\nsource_symbol = \"AEDCNY\"\nprice_scale = 6\n\n[[import.sources]]\nkind = \"bar_parquet_collection\"\npath = \"/data/bars\"\nbroker = \"pocket_option\"\nrole = \"evaluation\"\nmanifest = \"collection.json\"\nprovenance = [\"batch.json\"]\n".to_string();
        let config = Config::parse(&source).unwrap();
        assert_eq!(config.canonical_toml(), source);
        assert_eq!(
            Config::parse(&config.canonical_toml())
                .unwrap()
                .content_hash(),
            config.content_hash()
        );
    }

    #[test]
    fn cross_field_rules_reject_with_the_field_name() {
        let base = "schema_version = 1\nrun_mode = \"replay\"\n\n[storage]\nhistorical_data_dir = \"h\"\npublication_uri = \"file:///p\"\n";
        let error = Config::parse(base).unwrap_err().to_string();
        assert!(error.contains("storage.publication_uri") && error.contains("`replay`"));

        let research = base.replace("replay", "research");
        let tick = "\n[[import.sources]]\nkind = \"tick_csv\"\npath = \"t.csv\"\nbroker = \"b\"\nrole = \"ROLE\"\nprovider_symbol = \"S\"\nsource_symbol = \"S\"\nprice_scale = 3\n";
        let holdout = format!("{research}{}", tick.replace("ROLE", "holdout"));
        assert!(
            Config::parse(&holdout)
                .unwrap_err()
                .to_string()
                .contains("import.sources[0].role")
        );
        let duplicate = format!(
            "{research}{}{}",
            tick.replace("ROLE", "development"),
            tick.replace("ROLE", "development")
        );
        assert!(
            Config::parse(&duplicate)
                .unwrap_err()
                .to_string()
                .contains("import.sources[1].path")
        );
        let collection = |manifest: &str, provenance: &str| {
            format!(
                "{research}\n[[import.sources]]\nkind = \"bar_parquet_collection\"\npath = \"/bars\"\nbroker = \"b\"\nrole = \"development\"\nmanifest = \"{manifest}\"\n{provenance}"
            )
        };
        for manifest in [
            "../collection.json",
            "meta//collection.json",
            "meta/./collection.json",
            "/collection.json",
            "",
        ] {
            let error = Config::parse(&collection(manifest, ""))
                .unwrap_err()
                .to_string();
            assert!(
                error.contains("import.sources[0].manifest"),
                "{manifest}: {error}"
            );
        }
        for provenance in [
            "provenance = [\"collection.json\"]\n",
            "provenance = [\"a.json\", \"a.json\"]\n",
            "provenance = [\"b/../a.json\"]\n",
        ] {
            let error = Config::parse(&collection("collection.json", provenance))
                .unwrap_err()
                .to_string();
            assert!(
                error.contains("import.sources[0].provenance"),
                "{provenance}: {error}"
            );
        }
        assert!(
            Config::parse(&collection(
                "collection.json",
                "provenance = [\"a.json\", \"meta/b.json\"]\n"
            ))
            .is_ok()
        );
    }

    #[test]
    fn publication_uris_parse_exactly() {
        assert_eq!(
            "gs://bucket".parse::<PublicationUri>().unwrap(),
            PublicationUri::GoogleCloudStorage {
                bucket: "bucket".into(),
                prefix: String::new()
            }
        );
        assert_eq!(
            "gs://bucket/a/b/"
                .parse::<PublicationUri>()
                .unwrap()
                .to_string(),
            "gs://bucket/a/b"
        );
        assert!("gs://".parse::<PublicationUri>().is_err());
        assert!("gs://bucket/a//b".parse::<PublicationUri>().is_err());
        assert!("file://relative".parse::<PublicationUri>().is_err());
        assert!("s3://bucket".parse::<PublicationUri>().is_err());
    }
}

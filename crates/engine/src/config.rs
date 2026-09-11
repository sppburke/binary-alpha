//! The version-one configuration envelope: validation, canonical form, and content hash.
//!
//! `docs/contracts.md`, section "Configuration", is the normative description of every rule here.

use std::fmt;
use std::path::{Path, PathBuf};

use serde::de::Error as _;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use sha2::{Digest, Sha256};

use crate::dataset::{DatasetRole, NativeGranularity};
use crate::market::{BrokerId, Currency, InstrumentId, PriceScale, ProviderSymbol};

/// Domain separator hashed before the canonical document; changing it or the canonical form
/// increments the `v3` prefix rendered by [`Config::content_hash`].
const HASH_DOMAIN_V3: &[u8] = b"binary-alpha config hash v3\n";

/// A validated configuration document.
///
/// Field order is the canonical serialization order.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub schema_version: SchemaVersion,
    pub run_mode: RunMode,
    pub storage: Storage,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub import: Option<Import>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub instruments: Vec<Instrument>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub features: Option<Features>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub outcomes: Option<Outcomes>,
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
        for (index, instrument) in self.instruments.iter().enumerate() {
            instrument
                .validate()
                .map_err(|reason| format!("instruments[{index}].{reason}"))?;
            if let Some(earlier) = self.instruments[..index].iter().position(|earlier| {
                earlier.broker == instrument.broker
                    && earlier.provider_symbol == instrument.provider_symbol
                    && earlier.native_granularity == instrument.native_granularity
            }) {
                return Err(format!(
                    "instruments[{index}].provider_symbol: {} with {} granularity is already mapped by instruments[{earlier}]",
                    instrument.id(),
                    instrument.native_granularity
                ));
            }
        }
        if let Some(features) = &self.features {
            for (index, entry) in features.instruments.iter().enumerate() {
                entry
                    .validate()
                    .map_err(|reason| format!("features.instruments[{index}].{reason}"))?;
            }
        }
        if let Some(outcomes) = &self.outcomes {
            outcomes
                .validate()
                .map_err(|reason| format!("outcomes.{reason}"))?;
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
            match source {
                Source::TickCsv { .. } => {}
                Source::BarParquetCollection {
                    manifest,
                    provenance,
                    ..
                } => {
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
                Source::TickParquetDaily { instruments, .. } => {
                    if instruments.is_empty() {
                        return Err(format!(
                            "{}: at least one directory name is required",
                            field("instruments")
                        ));
                    }
                    for (position, name) in instruments.iter().enumerate() {
                        relative_path(name)
                            .map_err(|reason| format!("{}: {reason}", field("instruments")))?;
                        if name.contains('/') || name.bytes().any(|byte| byte.is_ascii_control()) {
                            return Err(format!(
                                "{}: `{}` must be one path component without a control character",
                                field("instruments"),
                                name.escape_default()
                            ));
                        }
                        if instruments[..position].contains(name) {
                            return Err(format!(
                                "{}: {name} is listed twice",
                                field("instruments")
                            ));
                        }
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

    /// The version-three content hash of the canonical document, rendered as `v3:sha256:`
    /// followed by sixty-four lowercase hexadecimal digits.
    pub fn content_hash(&self) -> String {
        let mut hasher = Sha256::new();
        hasher.update(HASH_DOMAIN_V3);
        hasher.update(self.canonical_toml().as_bytes());
        format!("v3:sha256:{}", crate::hex(&hasher.finalize()))
    }

    /// The configured instrument a generation's identity and native granularity map to, or,
    /// when the identity is mapped only at another granularity, that definition so that binding
    /// reports the capability the source lacks. Nothing is ever defaulted.
    pub fn instrument(
        &self,
        id: &InstrumentId,
        native_granularity: NativeGranularity,
    ) -> Option<&Instrument> {
        let mapped = |instrument: &&Instrument| {
            instrument.broker == id.broker && instrument.provider_symbol == id.provider_symbol
        };
        self.instruments
            .iter()
            .filter(mapped)
            .find(|instrument| instrument.native_granularity == native_granularity)
            .or_else(|| self.instruments.iter().find(mapped))
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
    /// One archive root holding, per listed directory, daily Parquet tick files and their
    /// metadata.
    #[serde(rename = "tick_parquet_daily")]
    TickParquetDaily {
        path: ConfigPath,
        broker: BrokerId,
        role: DatasetRole,
        price_scale: PriceScale,
        /// The directory names to import, each one path component.
        instruments: Vec<String>,
    },
}

impl Source {
    /// The declared file or collection root as written.
    pub fn path(&self) -> &Path {
        match self {
            Self::TickCsv { path, .. }
            | Self::BarParquetCollection { path, .. }
            | Self::TickParquetDaily { path, .. } => path.as_path(),
        }
    }

    pub fn broker(&self) -> &BrokerId {
        match self {
            Self::TickCsv { broker, .. }
            | Self::BarParquetCollection { broker, .. }
            | Self::TickParquetDaily { broker, .. } => broker,
        }
    }

    pub fn role(&self) -> DatasetRole {
        match self {
            Self::TickCsv { role, .. }
            | Self::BarParquetCollection { role, .. }
            | Self::TickParquetDaily { role, .. } => *role,
        }
    }
}

/// A ready-manifest location: a `file://` or `gs://` store root followed by
/// `manifests/GENERATION/ready.json`, where the generation is sixty-four lowercase hexadecimal
/// digits.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManifestUri {
    pub root: PublicationUri,
    /// `manifests/GENERATION/ready.json`, the key inside the root.
    pub key: String,
}

impl ManifestUri {
    /// The generation the manifest belongs to.
    pub fn generation(&self) -> &str {
        &self.key["manifests/".len().."manifests/".len() + 64]
    }
}

impl fmt::Display for ManifestUri {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}/{}", self.root, self.key)
    }
}

impl std::str::FromStr for ManifestUri {
    type Err = String;

    fn from_str(uri: &str) -> Result<Self, Self::Err> {
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
        let root: PublicationUri = root
            .parse()
            .map_err(|error: String| format!("{uri}: {error}"))?;
        Ok(Self {
            root,
            key: format!("manifests/{key}"),
        })
    }
}

impl Serialize for ManifestUri {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(self)
    }
}

impl<'de> Deserialize<'de> for ManifestUri {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        String::deserialize(deserializer)?
            .parse()
            .map_err(D::Error::custom)
    }
}

/// The feature-engine inventory consumed only by `features build`.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Features {
    pub instruments: Vec<FeatureInstrument>,
}

/// One instrument's feature build: the declared input role, the Phase 02 input generation, the
/// Phase 03 stream generation whose definition and profile bind the streams, and either a
/// frozen plan to apply or the settings of a new plan. Settings are required only for the
/// outputs that need them; the resolver names a missing one.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct FeatureInstrument {
    pub role: DatasetRole,
    pub input_manifest: ManifestUri,
    pub profile_manifest: ManifestUri,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub frozen_plan: Option<ManifestUri>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub streams: Option<Vec<StreamKey>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub outputs: Option<Outputs>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub moving_average_periods: Option<Vec<u32>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rolling_window: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub min_history: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub structure: Option<StructureSettings>,
    /// Sequence equality tolerance as decimal price text at the instrument's price scale;
    /// `"0"` means strict equality.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub price_epsilon: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tick_path_streams: Option<Vec<StreamKey>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub encodings: Option<Encodings>,
}

/// One configured duration and offset pair naming a stream of the bound definition.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct StreamKey {
    pub duration_seconds: u32,
    pub offset_seconds: u32,
}

impl fmt::Display for StreamKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}s/{}s", self.duration_seconds, self.offset_seconds)
    }
}

/// Which compiled outputs a new plan selects: every supported one, or an explicit list.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outputs {
    AllSupported,
    Named(Vec<String>),
}

impl Serialize for Outputs {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            Self::AllSupported => serializer.serialize_str("all_supported"),
            Self::Named(names) => names.serialize(serializer),
        }
    }
}

impl<'de> Deserialize<'de> for Outputs {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum Raw {
            Text(String),
            Names(Vec<String>),
        }
        match Raw::deserialize(deserializer)? {
            Raw::Text(text) if text == "all_supported" => Ok(Self::AllSupported),
            Raw::Text(text) => Err(D::Error::custom(format!(
                "unknown outputs `{text}`, expected `all_supported` or a list of output identifiers"
            ))),
            Raw::Names(names) => Ok(Self::Named(names)),
        }
    }
}

/// The structure-label policy: swing confirmation, rolling windows, and the classification
/// thresholds of the pinned reference, all explicit.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct StructureSettings {
    pub swing_left: u32,
    pub swing_right: u32,
    pub rolling_windows: Vec<u32>,
    pub direction_window: u32,
    pub trend_efficiency_threshold: f64,
    pub trend_min_abs_momentum_bps: f64,
    pub range_efficiency_threshold: f64,
    pub compression_ratio_threshold: f64,
    pub expanded_ratio_threshold: f64,
    pub extreme_ratio_threshold: f64,
    pub pullback_min_trend_age: u32,
    pub trend_reset_sideways_bars: u32,
    pub failed_breakout_max_bars: u32,
}

/// The encodings a new plan fits: the selected outputs and projections, and the label limit of
/// the signed 16-bit code space.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Encodings {
    /// `1` to `32768`.
    pub max_labels: u32,
    pub outputs: Vec<EncodingSpec>,
}

/// One encoded output: a category output or a compiled projection needs no bins; a numeric
/// output names fixed right-closed bin edges or `development_fifths`.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct EncodingSpec {
    pub output: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bins: Option<Bins>,
}

/// Numeric bin edges: fixed right-closed edges, or the development-fitted fifths.
#[derive(Debug, Clone, PartialEq)]
pub enum Bins {
    DevelopmentFifths,
    Fixed(Vec<f64>),
}

impl Serialize for Bins {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            Self::DevelopmentFifths => serializer.serialize_str("development_fifths"),
            Self::Fixed(edges) => edges.serialize(serializer),
        }
    }
}

impl<'de> Deserialize<'de> for Bins {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum Raw {
            Text(String),
            Edges(Vec<f64>),
        }
        match Raw::deserialize(deserializer)? {
            Raw::Text(text) if text == "development_fifths" => Ok(Self::DevelopmentFifths),
            Raw::Text(text) => Err(D::Error::custom(format!(
                "unknown bins `{text}`, expected `development_fifths` or a list of edges"
            ))),
            Raw::Edges(edges) => Ok(Self::Fixed(edges)),
        }
    }
}

/// The largest label count a signed 16-bit code space holds.
pub const MAX_ENCODING_LABELS: u32 = 32_768;

fn sorted_unique(values: &[u32]) -> bool {
    values.windows(2).all(|pair| pair[0] < pair[1])
}

fn unique_streams(streams: &[StreamKey]) -> Result<(), String> {
    for (index, stream) in streams.iter().enumerate() {
        if stream.duration_seconds == 0 || stream.offset_seconds >= stream.duration_seconds {
            return Err(format!(
                "[{index}]: stream {stream} needs a positive duration and a smaller offset"
            ));
        }
        if streams[..index].contains(stream) {
            return Err(format!("[{index}]: stream {stream} is listed twice"));
        }
    }
    Ok(())
}

impl FeatureInstrument {
    /// The rules a single field's deserializer cannot see; an error names the field. Whether a
    /// setting is required depends on the selected outputs, which the resolver decides.
    pub fn validate(&self) -> Result<(), String> {
        if self.role == DatasetRole::Holdout {
            return Err("role: holdout data is never a feature-build input".to_string());
        }
        let settings = [
            ("streams", self.streams.is_some()),
            ("outputs", self.outputs.is_some()),
            (
                "moving_average_periods",
                self.moving_average_periods.is_some(),
            ),
            ("rolling_window", self.rolling_window.is_some()),
            ("min_history", self.min_history.is_some()),
            ("structure", self.structure.is_some()),
            ("price_epsilon", self.price_epsilon.is_some()),
            ("tick_path_streams", self.tick_path_streams.is_some()),
            ("encodings", self.encodings.is_some()),
        ];
        if self.frozen_plan.is_some() {
            if let Some((name, _)) = settings.iter().find(|(_, present)| *present) {
                return Err(format!(
                    "{name}: a frozen plan fixes membership, parameters, and encodings; no new-plan setting may accompany it"
                ));
            }
            return Ok(());
        }
        if self.role != DatasetRole::Development {
            return Err(format!(
                "role: a new plan fits on development data, not `{}`",
                self.role
            ));
        }
        if let Some(streams) = &self.streams {
            if streams.is_empty() {
                return Err("streams: at least one stream is required when present".to_string());
            }
            unique_streams(streams).map_err(|reason| format!("streams{reason}"))?;
        }
        if let Some(Outputs::Named(names)) = &self.outputs {
            for (index, name) in names.iter().enumerate() {
                if name.is_empty() || name.bytes().any(|byte| byte.is_ascii_control()) {
                    return Err(format!(
                        "outputs[{index}]: must be a non-empty identifier without a control character"
                    ));
                }
                if names[..index].contains(name) {
                    return Err(format!("outputs[{index}]: `{name}` is listed twice"));
                }
            }
        }
        if let Some(periods) = &self.moving_average_periods
            && (!sorted_unique(periods) || periods.iter().any(|period| *period < 2))
        {
            return Err(
                "moving_average_periods: periods must be sorted, unique, and greater than one"
                    .to_string(),
            );
        }
        match (self.rolling_window, self.min_history) {
            (None, None) => {}
            (Some(window), Some(history)) if 1 <= history && history <= window => {}
            (Some(_), Some(_)) => {
                return Err("min_history: requires 1 <= min_history <= rolling_window".to_string());
            }
            _ => {
                return Err(
                    "rolling_window: rolling_window and min_history are declared together"
                        .to_string(),
                );
            }
        }
        if let Some(structure) = &self.structure {
            structure
                .validate()
                .map_err(|reason| format!("structure.{reason}"))?;
        }
        // Units resolve under the profile's price scale at plan time; here only the syntax.
        if let Some(text) = &self.price_epsilon {
            let (negative, _, _) = crate::market::split_decimal(text)
                .map_err(|reason| format!("price_epsilon: {reason}"))?;
            if negative {
                return Err(format!("price_epsilon: `{text}` must not be negative"));
            }
        }
        if let Some(streams) = &self.tick_path_streams {
            unique_streams(streams).map_err(|reason| format!("tick_path_streams{reason}"))?;
            if let Some(selected) = &self.streams
                && let Some(stream) = streams.iter().find(|stream| !selected.contains(stream))
            {
                return Err(format!(
                    "tick_path_streams: stream {stream} is not a selected stream"
                ));
            }
        }
        if let Some(encodings) = &self.encodings {
            encodings
                .validate()
                .map_err(|reason| format!("encodings.{reason}"))?;
        }
        Ok(())
    }
}

impl StructureSettings {
    pub fn validate(&self) -> Result<(), String> {
        let counts = [
            ("swing_left", self.swing_left),
            ("swing_right", self.swing_right),
            ("direction_window", self.direction_window),
            ("pullback_min_trend_age", self.pullback_min_trend_age),
            ("trend_reset_sideways_bars", self.trend_reset_sideways_bars),
            ("failed_breakout_max_bars", self.failed_breakout_max_bars),
        ];
        if let Some((name, _)) = counts.iter().find(|(_, count)| *count == 0) {
            return Err(format!("{name}: must be positive"));
        }
        if self.rolling_windows.is_empty()
            || !sorted_unique(&self.rolling_windows)
            || self.rolling_windows[0] == 0
        {
            return Err(
                "rolling_windows: windows must be positive, sorted, and unique".to_string(),
            );
        }
        if !self.rolling_windows.contains(&self.direction_window) {
            return Err(format!(
                "direction_window: {} is not one of the rolling windows",
                self.direction_window
            ));
        }
        for (name, value) in [
            (
                "trend_efficiency_threshold",
                self.trend_efficiency_threshold,
            ),
            (
                "range_efficiency_threshold",
                self.range_efficiency_threshold,
            ),
        ] {
            if !(0.0..=1.0).contains(&value) {
                return Err(format!("{name}: {value} must lie in [0, 1]"));
            }
        }
        if !self.trend_min_abs_momentum_bps.is_finite() || self.trend_min_abs_momentum_bps < 0.0 {
            return Err(format!(
                "trend_min_abs_momentum_bps: {} must be finite and non-negative",
                self.trend_min_abs_momentum_bps
            ));
        }
        let ratios = [
            self.compression_ratio_threshold,
            self.expanded_ratio_threshold,
            self.extreme_ratio_threshold,
        ];
        if ratios
            .iter()
            .any(|ratio| !ratio.is_finite() || *ratio <= 0.0)
            || !(ratios[0] < ratios[1] && ratios[1] < ratios[2])
        {
            return Err(
                "compression_ratio_threshold: compression, expanded, and extreme ratio thresholds must be finite, positive, and strictly increasing"
                    .to_string(),
            );
        }
        Ok(())
    }
}

impl Encodings {
    pub fn validate(&self) -> Result<(), String> {
        if !(1..=MAX_ENCODING_LABELS).contains(&self.max_labels) {
            return Err(format!(
                "max_labels: {} must lie in 1..={MAX_ENCODING_LABELS}",
                self.max_labels
            ));
        }
        for (index, spec) in self.outputs.iter().enumerate() {
            if spec.output.is_empty() || spec.output.bytes().any(|byte| byte.is_ascii_control()) {
                return Err(format!(
                    "outputs[{index}].output: must be a non-empty identifier without a control character"
                ));
            }
            if self.outputs[..index]
                .iter()
                .any(|earlier| earlier.output == spec.output)
            {
                return Err(format!(
                    "outputs[{index}].output: `{}` is listed twice",
                    spec.output
                ));
            }
            if let Some(Bins::Fixed(edges)) = &spec.bins
                && (edges.is_empty()
                    || edges.iter().any(|edge| !edge.is_finite())
                    || edges.windows(2).any(|pair| pair[0] >= pair[1]))
            {
                return Err(format!(
                    "outputs[{index}].bins: fixed edges must be finite and strictly increasing"
                ));
            }
        }
        Ok(())
    }
}

/// The outcome build consumed only by `outcomes build`: the declared role, the Phase 02 tick
/// generation and the Phase 04 feature generation computed from it, the expiries, and the label
/// thresholds in the units their names declare.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Outcomes {
    pub role: DatasetRole,
    pub tick_manifest: ManifestUri,
    pub feature_manifest: ManifestUri,
    pub expiry_seconds: Vec<u32>,
    pub max_entry_delay_ms: u64,
    pub max_settlement_delay_ms: u64,
    pub max_tick_gap_ms: u64,
    pub true_jump_max_gap_ms: u64,
    /// Positive decimal basis points such as `"5"` or `"2.5"`, compared exactly.
    pub true_jump_basis_points: String,
    pub frozen_min_ticks: u32,
    pub frozen_min_ms: u64,
}

impl Outcomes {
    /// The rules a single field's deserializer cannot see; an error names the field.
    pub fn validate(&self) -> Result<(), String> {
        if self.role == DatasetRole::Holdout {
            return Err("role: holdout data never enters an outcome build".to_string());
        }
        crate::outcomes::OutcomeRule::resolve(self).map(drop)
    }
}

/// One configured instrument: its neutral identity, provider mapping, currencies, native
/// granularity, price scale, candle streams, and the enabled quality checks. A check is enabled
/// by the presence of its table; ordering, causality, interval boundaries, finite values, and
/// source capability are stream invariants that no field can relax.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Instrument {
    pub broker: BrokerId,
    pub provider_symbol: ProviderSymbol,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub base_currency: Option<Currency>,
    pub quote_currency: Currency,
    pub price_scale: PriceScale,
    pub native_granularity: NativeGranularity,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub gap: Option<GapCheck>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub frozen: Option<FrozenCheck>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub jump: Option<JumpCheck>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub span: Option<SpanCheck>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sessions: Option<Vec<Session>>,
    pub candles: Vec<CandleSpec>,
}

/// Inter-arrival times longer than `max_seconds` are gaps; a move after a gap of at least
/// `reopen_seconds` is a reopen move rather than a delayed one.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct GapCheck {
    pub max_seconds: u32,
    pub reopen_seconds: u32,
}

/// A run of one unchanged price is frozen at `min_observations` records or `min_seconds`.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct FrozenCheck {
    pub min_observations: u32,
    pub min_seconds: u32,
}

/// A relative move of at least `min_basis_points` between consecutive prices is a jump.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct JumpCheck {
    pub min_basis_points: u32,
}

/// A candle whose records span less than `min_percent` of its duration is short.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SpanCheck {
    pub min_percent: u8,
}

/// One weekly window in seconds since Monday 00:00 Coordinated Universal Time, left-closed.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Session {
    pub name: String,
    pub open_seconds: u32,
    pub close_seconds: u32,
}

/// One candle stream: left-closed intervals of `duration_seconds` whose boundaries sit
/// `offset_seconds` after the Unix-epoch grid.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CandleSpec {
    pub duration_seconds: u32,
    pub offset_seconds: u32,
    /// Fewer records than this flag the candle as low activity.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub min_observations: Option<u32>,
    /// Fewer records than this flag the candle as hard low activity and incomplete.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hard_min_observations: Option<u32>,
}

/// Seconds in one week, the extent of a session window.
pub const SECONDS_PER_WEEK: u32 = 7 * 86_400;

impl Instrument {
    /// The rules a single field's deserializer cannot see; an error names the field.
    pub fn validate(&self) -> Result<(), String> {
        if self.candles.is_empty() {
            return Err("candles: at least one candle stream is required".to_string());
        }
        let period = match self.native_granularity {
            NativeGranularity::Tick => None,
            NativeGranularity::Bar { period_seconds: 0 } => {
                return Err("native_granularity.period_seconds: must be positive".to_string());
            }
            NativeGranularity::Bar { period_seconds } => Some(u32::from(period_seconds)),
        };
        for (index, candle) in self.candles.iter().enumerate() {
            let field = |name: &str| format!("candles[{index}].{name}");
            if candle.duration_seconds == 0 {
                return Err(format!("{}: must be positive", field("duration_seconds")));
            }
            if candle.offset_seconds >= candle.duration_seconds {
                return Err(format!(
                    "{}: {} must be less than the duration {}",
                    field("offset_seconds"),
                    candle.offset_seconds,
                    candle.duration_seconds
                ));
            }
            if let Some(period) = period
                && (candle.duration_seconds % period != 0 || candle.offset_seconds % period != 0)
            {
                return Err(format!(
                    "{}: duration {} and offset {} must be multiples of the {period}-second bar",
                    field("duration_seconds"),
                    candle.duration_seconds,
                    candle.offset_seconds
                ));
            }
            if self.candles[..index].iter().any(|earlier| {
                earlier.duration_seconds == candle.duration_seconds
                    && earlier.offset_seconds == candle.offset_seconds
            }) {
                return Err(format!(
                    "{}: stream {}s/{}s is listed twice",
                    field("duration_seconds"),
                    candle.duration_seconds,
                    candle.offset_seconds
                ));
            }
        }
        if let Some(frozen) = &self.frozen
            && (frozen.min_observations == 0 || frozen.min_seconds == 0)
        {
            return Err(
                "frozen.min_observations: both frozen thresholds must be positive".to_string(),
            );
        }
        if let Some(jump) = &self.jump
            && jump.min_basis_points == 0
        {
            return Err("jump.min_basis_points: must be positive".to_string());
        }
        if let Some(gap) = &self.gap
            && gap.reopen_seconds <= gap.max_seconds
        {
            return Err(format!(
                "gap.reopen_seconds: {} must exceed max_seconds {}",
                gap.reopen_seconds, gap.max_seconds
            ));
        }
        if let Some(span) = &self.span
            && span.min_percent > 100
        {
            return Err(format!(
                "span.min_percent: {} exceeds 100",
                span.min_percent
            ));
        }
        if let Some(sessions) = &self.sessions {
            if sessions.is_empty() {
                return Err("sessions: at least one window is required when present".to_string());
            }
            for (index, session) in sessions.iter().enumerate() {
                let field = |name: &str| format!("sessions[{index}].{name}");
                if session.name.is_empty()
                    || session.name.bytes().any(|byte| byte.is_ascii_control())
                {
                    return Err(format!(
                        "{}: must be non-empty without a control character",
                        field("name")
                    ));
                }
                if session.open_seconds >= session.close_seconds
                    || session.close_seconds > SECONDS_PER_WEEK
                {
                    return Err(format!(
                        "{}: window {}..{} must lie inside one week of {SECONDS_PER_WEEK} seconds",
                        field("open_seconds"),
                        session.open_seconds,
                        session.close_seconds
                    ));
                }
                if let Some(earlier) = sessions[..index].iter().find(|earlier| {
                    earlier.name == session.name
                        || (earlier.open_seconds < session.close_seconds
                            && session.open_seconds < earlier.close_seconds)
                }) {
                    return Err(format!(
                        "{}: `{}` repeats or overlaps `{}`",
                        field("name"),
                        session.name,
                        earlier.name
                    ));
                }
            }
        }
        Ok(())
    }

    /// The neutral instrument identity this definition maps to.
    pub fn id(&self) -> InstrumentId {
        InstrumentId {
            broker: self.broker.clone(),
            provider_symbol: self.provider_symbol.clone(),
        }
    }

    /// The canonical TOML rendering of this entry alone, in schema order: the text a stream
    /// generation's identity hashes.
    pub fn canonical_toml(&self) -> String {
        toml::to_string(self).expect("a validated instrument serializes")
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
        let source = "schema_version = 1\nrun_mode = \"research\"\n\n[storage]\nhistorical_data_dir = \"/data/historical\"\npublication_uri = \"file:///data/published\"\n\n[[import.sources]]\nkind = \"tick_csv\"\npath = \"ticks.csv\"\nbroker = \"pocket_option\"\nrole = \"development\"\nprovider_symbol = \"AEDCNY_otc\"\nsource_symbol = \"AEDCNY\"\nprice_scale = 6\n\n[[import.sources]]\nkind = \"bar_parquet_collection\"\npath = \"/data/bars\"\nbroker = \"pocket_option\"\nrole = \"evaluation\"\nmanifest = \"collection.json\"\nprovenance = [\"batch.json\"]\n\n[[import.sources]]\nkind = \"tick_parquet_daily\"\npath = \"/data/deriv/ticks\"\nbroker = \"deriv\"\nrole = \"development\"\nprice_scale = 5\ninstruments = [\"AUDUSD\", \"USDJPY\"]\n".to_string();
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
        let daily = |instruments: &str| {
            format!(
                "{research}\n[[import.sources]]\nkind = \"tick_parquet_daily\"\npath = \"/ticks\"\nbroker = \"deriv\"\nrole = \"development\"\nprice_scale = 5\ninstruments = {instruments}\n"
            )
        };
        for instruments in [
            "[]",
            "[\"\"]",
            "[\"..\"]",
            "[\"a/b\"]",
            "[\"/a\"]",
            "[\"a\\tb\"]",
            "[\"a\", \"a\"]",
        ] {
            let error = Config::parse(&daily(instruments)).unwrap_err().to_string();
            assert!(
                error.contains("import.sources[0].instruments"),
                "{instruments}: {error}"
            );
        }
        assert!(Config::parse(&daily("[\"AUDUSD\", \"USDJPY\"]")).is_ok());
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

#[cfg(test)]
mod feature_tests {
    use super::*;

    const HEAD: &str = "schema_version = 1\nrun_mode = \"research\"\n\n[storage]\nhistorical_data_dir = \"h\"\npublication_uri = \"file:///p\"\n";
    const INPUT: &str = "file:///p/manifests/1111111111111111111111111111111111111111111111111111111111111111/ready.json";
    const PROFILE: &str = "file:///p/manifests/2222222222222222222222222222222222222222222222222222222222222222/ready.json";
    const PLAN: &str = "gs://bucket/prefix/manifests/3333333333333333333333333333333333333333333333333333333333333333/ready.json";

    fn entry(rest: &str) -> String {
        format!(
            "{HEAD}\n[[features.instruments]]\nrole = \"development\"\ninput_manifest = \"{INPUT}\"\nprofile_manifest = \"{PROFILE}\"\n{rest}"
        )
    }

    #[test]
    fn feature_entries_round_trip_through_the_canonical_form() {
        let source = entry(
            "outputs = \"all_supported\"\nmoving_average_periods = [20, 50]\nrolling_window = 100\nmin_history = 20\nprice_epsilon = \"0\"\n\n[[features.instruments.streams]]\nduration_seconds = 5\noffset_seconds = 0\n\n[[features.instruments.streams]]\nduration_seconds = 15\noffset_seconds = 5\n\n[features.instruments.structure]\nswing_left = 3\nswing_right = 3\nrolling_windows = [5, 10, 20]\ndirection_window = 10\ntrend_efficiency_threshold = 0.35\ntrend_min_abs_momentum_bps = 3.0\nrange_efficiency_threshold = 0.25\ncompression_ratio_threshold = 0.7\nexpanded_ratio_threshold = 1.3\nextreme_ratio_threshold = 1.8\npullback_min_trend_age = 3\ntrend_reset_sideways_bars = 3\nfailed_breakout_max_bars = 5\n\n[[features.instruments.tick_path_streams]]\nduration_seconds = 5\noffset_seconds = 0\n\n[features.instruments.encodings]\nmax_labels = 32768\n\n[[features.instruments.encodings.outputs]]\noutput = \"candle_type\"\n\n[[features.instruments.encodings.outputs]]\noutput = \"body_bps\"\nbins = \"development_fifths\"\n\n[[features.instruments.encodings.outputs]]\noutput = \"range_bps\"\nbins = [0.5, 1.0, 2.0]\n",
        );
        let config = Config::parse(&source).unwrap();
        assert_eq!(config.canonical_toml(), source);
        assert_eq!(
            Config::parse(&config.canonical_toml()).unwrap(),
            config,
            "the canonical form parses to the same values"
        );
        let entry = &config.features.as_ref().unwrap().instruments[0];
        assert_eq!(entry.outputs, Some(Outputs::AllSupported));
        assert_eq!(
            entry.input_manifest.generation(),
            "1".repeat(64),
            "the generation is the sixty-four digits of the key"
        );
        assert_eq!(entry.input_manifest.to_string(), INPUT);
        assert_eq!(
            entry.encodings.as_ref().unwrap().outputs[2].bins,
            Some(Bins::Fixed(vec![0.5, 1.0, 2.0]))
        );
        let named = entry_named("outputs = [\"body_bps\", \"candle_type\"]\n");
        assert_eq!(
            Config::parse(&named).unwrap().features.unwrap().instruments[0].outputs,
            Some(Outputs::Named(vec![
                "body_bps".to_string(),
                "candle_type".to_string()
            ]))
        );
        let frozen = format!(
            "{HEAD}\n[[features.instruments]]\nrole = \"evaluation\"\ninput_manifest = \"{INPUT}\"\nprofile_manifest = \"{PROFILE}\"\nfrozen_plan = \"{PLAN}\"\n"
        );
        let config = Config::parse(&frozen).unwrap();
        assert_eq!(config.canonical_toml(), frozen);
        assert_eq!(
            config.features.unwrap().instruments[0]
                .frozen_plan
                .as_ref()
                .unwrap()
                .root,
            PublicationUri::GoogleCloudStorage {
                bucket: "bucket".to_string(),
                prefix: "prefix".to_string()
            }
        );
        assert_eq!(
            Config::parse(HEAD).unwrap().content_hash(),
            "v3:sha256:d7be0fdf6fb030fdfaa543417aad386f84bdb7e06ff06a61ae5646ca8e7c1256",
            "a document that omits `features` keeps the identity the previous checkout gave it"
        );
    }

    fn entry_named(rest: &str) -> String {
        entry(rest)
    }

    #[test]
    fn price_epsilon_is_checked_for_syntax_only() {
        // Units resolve under the profile's price scale at plan time; no scale bounds the text.
        for text in ["10", "99999999999999999999", "0.000000000000000001"] {
            Config::parse(&entry(&format!("price_epsilon = \"{text}\"\n"))).unwrap();
        }
    }

    #[test]
    fn feature_rules_reject_with_the_field_name() {
        let structure = |body: &str| format!("[features.instruments.structure]\n{body}");
        let full_structure = "swing_left = 3\nswing_right = 3\nrolling_windows = [5, 10, 20]\ndirection_window = 10\ntrend_efficiency_threshold = 0.35\ntrend_min_abs_momentum_bps = 3.0\nrange_efficiency_threshold = 0.25\ncompression_ratio_threshold = 0.7\nexpanded_ratio_threshold = 1.3\nextreme_ratio_threshold = 1.8\npullback_min_trend_age = 3\ntrend_reset_sideways_bars = 3\nfailed_breakout_max_bars = 5\n";
        let cases = [
            (
                format!("frozen_plan = \"{PLAN}\"\noutputs = \"all_supported\"\n"),
                "features.instruments[0].outputs: a frozen plan",
            ),
            (
                "outputs = \"everything\"\n".to_string(),
                "unknown outputs `everything`",
            ),
            (
                "outputs = []\nmoving_average_periods = [50, 20]\n".to_string(),
                "features.instruments[0].moving_average_periods",
            ),
            (
                "moving_average_periods = [1]\n".to_string(),
                "features.instruments[0].moving_average_periods",
            ),
            (
                "rolling_window = 10\n".to_string(),
                "features.instruments[0].rolling_window",
            ),
            (
                "rolling_window = 10\nmin_history = 11\n".to_string(),
                "features.instruments[0].min_history",
            ),
            (
                "rolling_window = 10\nmin_history = 0\n".to_string(),
                "features.instruments[0].min_history",
            ),
            (
                "outputs = [\"a\", \"a\"]\n".to_string(),
                "features.instruments[0].outputs[1]",
            ),
            (
                "outputs = [\"\"]\n".to_string(),
                "features.instruments[0].outputs[0]",
            ),
            (
                "price_epsilon = \"-0.1\"\n".to_string(),
                "features.instruments[0].price_epsilon",
            ),
            (
                "price_epsilon = \"1e-3\"\n".to_string(),
                "features.instruments[0].price_epsilon",
            ),
            (
                "streams = []\n".to_string(),
                "features.instruments[0].streams",
            ),
            (
                "streams = [{ duration_seconds = 5, offset_seconds = 5 }]\n".to_string(),
                "features.instruments[0].streams[0]",
            ),
            (
                "streams = [{ duration_seconds = 5, offset_seconds = 0 }, { duration_seconds = 5, offset_seconds = 0 }]\n".to_string(),
                "features.instruments[0].streams[1]",
            ),
            (
                "streams = [{ duration_seconds = 5, offset_seconds = 0 }]\ntick_path_streams = [{ duration_seconds = 15, offset_seconds = 5 }]\n".to_string(),
                "features.instruments[0].tick_path_streams: stream 15s/5s",
            ),
            (
                "tick_path_streams = [{ duration_seconds = 5, offset_seconds = 0 }, { duration_seconds = 5, offset_seconds = 0 }]\n".to_string(),
                "features.instruments[0].tick_path_streams[1]",
            ),
            (
                structure(&full_structure.replace("swing_left = 3", "swing_left = 0")),
                "features.instruments[0].structure.swing_left",
            ),
            (
                structure(&full_structure.replace("[5, 10, 20]", "[5, 20, 10]")),
                "features.instruments[0].structure.rolling_windows",
            ),
            (
                structure(&full_structure.replace("direction_window = 10", "direction_window = 7")),
                "features.instruments[0].structure.direction_window",
            ),
            (
                structure(&full_structure.replace("trend_efficiency_threshold = 0.35", "trend_efficiency_threshold = 1.5")),
                "features.instruments[0].structure.trend_efficiency_threshold",
            ),
            (
                structure(&full_structure.replace("trend_min_abs_momentum_bps = 3.0", "trend_min_abs_momentum_bps = -1.0")),
                "features.instruments[0].structure.trend_min_abs_momentum_bps",
            ),
            (
                structure(&full_structure.replace("expanded_ratio_threshold = 1.3", "expanded_ratio_threshold = 0.7")),
                "features.instruments[0].structure.compression_ratio_threshold",
            ),
            (
                structure(&full_structure.replace("failed_breakout_max_bars = 5\n", "")),
                "failed_breakout_max_bars",
            ),
            (
                "encodings = { max_labels = 0, outputs = [] }\n".to_string(),
                "features.instruments[0].encodings.max_labels",
            ),
            (
                "encodings = { max_labels = 32769, outputs = [] }\n".to_string(),
                "features.instruments[0].encodings.max_labels",
            ),
            (
                "encodings = { max_labels = 8, outputs = [{ output = \"a\" }, { output = \"a\" }] }\n".to_string(),
                "features.instruments[0].encodings.outputs[1].output",
            ),
            (
                "encodings = { max_labels = 8, outputs = [{ output = \"a\", bins = [] }] }\n".to_string(),
                "features.instruments[0].encodings.outputs[0].bins",
            ),
            (
                "encodings = { max_labels = 8, outputs = [{ output = \"a\", bins = [2.0, 1.0] }] }\n".to_string(),
                "features.instruments[0].encodings.outputs[0].bins",
            ),
            (
                "encodings = { max_labels = 8, outputs = [{ output = \"a\", bins = \"tenths\" }] }\n".to_string(),
                "unknown bins `tenths`",
            ),
            ("retry = 1\n".to_string(), "retry"),
        ];
        for (rest, key) in cases {
            let error = Config::parse(&entry(&rest)).unwrap_err().to_string();
            assert!(error.contains(key), "{rest}: {error}");
        }
        let holdout = entry("").replace("\"development\"", "\"holdout\"");
        let error = Config::parse(&holdout).unwrap_err().to_string();
        assert!(error.contains("features.instruments[0].role"), "{error}");
        let evaluation = entry("").replace("\"development\"", "\"evaluation\"");
        let error = Config::parse(&evaluation).unwrap_err().to_string();
        assert!(
            error.contains("features.instruments[0].role") && error.contains("new plan"),
            "{error}"
        );
        let twice = format!(
            "{}\n[[features.instruments]]\nrole = \"development\"\ninput_manifest = \"{INPUT}\"\nprofile_manifest = \"{PROFILE}\"\n",
            entry("")
        );
        // One profile may serve several entries (a fit and its frozen applications); the
        // resolved instrument, role, and streams have one owner, checked at build.
        assert_eq!(
            Config::parse(&twice)
                .unwrap()
                .features
                .unwrap()
                .instruments
                .len(),
            2
        );
        for uri in [
            "file:///p/manifests/abc/ready.json",
            "file:///p/manifests/1111111111111111111111111111111111111111111111111111111111111111/other.json",
            "s3://p/manifests/1111111111111111111111111111111111111111111111111111111111111111/ready.json",
            "file://p/manifests/1111111111111111111111111111111111111111111111111111111111111111/ready.json",
        ] {
            assert!(uri.parse::<ManifestUri>().is_err(), "{uri}");
            let error = Config::parse(&entry("").replace(INPUT, uri))
                .unwrap_err()
                .to_string();
            assert!(error.contains("input_manifest"), "{uri}: {error}");
        }
    }
}

#[cfg(test)]
mod instrument_tests {
    use super::*;

    const HEAD: &str = "schema_version = 1\nrun_mode = \"research\"\n\n[storage]\nhistorical_data_dir = \"h\"\npublication_uri = \"file:///p\"\n";

    fn instrument(rest: &str) -> String {
        format!(
            "{HEAD}\n[[instruments]]\nbroker = \"pocket_option\"\nprovider_symbol = \"AEDCNY_otc\"\nquote_currency = \"CNY\"\nprice_scale = 6\n{rest}"
        )
    }

    #[test]
    fn instruments_round_trip_through_the_canonical_form() {
        let source = format!(
            "{HEAD}\n[[instruments]]\nbroker = \"pocket_option\"\nprovider_symbol = \"AEDCNY_otc\"\nbase_currency = \"AED\"\nquote_currency = \"CNY\"\nprice_scale = 6\n\n[instruments.native_granularity]\nkind = \"tick\"\n\n[instruments.gap]\nmax_seconds = 2\nreopen_seconds = 60\n\n[instruments.frozen]\nmin_observations = 10\nmin_seconds = 5\n\n[instruments.jump]\nmin_basis_points = 5\n\n[instruments.span]\nmin_percent = 75\n\n[[instruments.sessions]]\nname = \"week\"\nopen_seconds = 0\nclose_seconds = 604800\n\n[[instruments.candles]]\nduration_seconds = 5\noffset_seconds = 0\nmin_observations = 9\nhard_min_observations = 5\n\n[[instruments.candles]]\nduration_seconds = 15\noffset_seconds = 5\n\n[[instruments]]\nbroker = \"pocket_option\"\nprovider_symbol = \"#AAPL\"\nquote_currency = \"USD\"\nprice_scale = 2\n\n[instruments.native_granularity]\nkind = \"bar\"\nperiod_seconds = 5\n\n[[instruments.candles]]\nduration_seconds = 15\noffset_seconds = 5\n"
        );
        let config = Config::parse(&source).unwrap();
        assert_eq!(config.canonical_toml(), source);
        assert!(config.content_hash().starts_with("v3:sha256:"));
        let inline = source
            .replace(
                "\n[instruments.native_granularity]\nkind = \"tick\"\n",
                "native_granularity = { kind = \"tick\" }\n",
            )
            .replace(
                "\n[instruments.gap]\nmax_seconds = 2\nreopen_seconds = 60\n",
                "gap = { max_seconds = 2, reopen_seconds = 60 }\n",
            );
        assert_eq!(Config::parse(&inline).unwrap(), config);
        let id = InstrumentId {
            broker: BrokerId::try_from("pocket_option".to_string()).unwrap(),
            provider_symbol: ProviderSymbol::try_from("#AAPL".to_string()).unwrap(),
        };
        let bars = NativeGranularity::Bar { period_seconds: 5 };
        assert_eq!(
            config.instrument(&id, bars).unwrap().price_scale.digits(),
            2
        );
        assert_eq!(
            config
                .instrument(&id, NativeGranularity::Tick)
                .unwrap()
                .price_scale
                .digits(),
            2,
            "the only mapping is offered so binding can name the missing capability"
        );
        assert!(
            config
                .instrument(
                    &InstrumentId {
                        provider_symbol: ProviderSymbol::try_from("MSFT".to_string()).unwrap(),
                        ..id
                    },
                    bars
                )
                .is_none()
        );
        let definition = config.instruments[0].canonical_toml();
        assert!(definition.starts_with("broker = \"pocket_option\"\n"));
        assert!(definition.contains("\n[[candles]]\nduration_seconds = 5\n"));
    }

    #[test]
    fn instrument_rules_reject_with_the_field_name() {
        let tick = "native_granularity = { kind = \"tick\" }\n";
        let cases = [
            (format!("{tick}candles = []\n"), "instruments[0].candles"),
            (
                format!("{tick}candles = [{{ duration_seconds = 0, offset_seconds = 0 }}]\n"),
                "instruments[0].candles[0].duration_seconds",
            ),
            (
                format!("{tick}candles = [{{ duration_seconds = 5, offset_seconds = 5 }}]\n"),
                "instruments[0].candles[0].offset_seconds",
            ),
            (
                format!(
                    "{tick}candles = [{{ duration_seconds = 5, offset_seconds = 0 }}, {{ duration_seconds = 5, offset_seconds = 0 }}]\n"
                ),
                "instruments[0].candles[1].duration_seconds",
            ),
            (
                "native_granularity = { kind = \"bar\", period_seconds = 5 }\ncandles = [{ duration_seconds = 12, offset_seconds = 0 }]\n".to_string(),
                "instruments[0].candles[0].duration_seconds",
            ),
            (
                "native_granularity = { kind = \"bar\", period_seconds = 0 }\ncandles = [{ duration_seconds = 5, offset_seconds = 0 }]\n".to_string(),
                "instruments[0].native_granularity.period_seconds",
            ),
            (
                format!("{tick}span = {{ min_percent = 101 }}\ncandles = [{{ duration_seconds = 5, offset_seconds = 0 }}]\n"),
                "instruments[0].span.min_percent",
            ),
            (
                format!("{tick}gap = {{ max_seconds = 60, reopen_seconds = 60 }}\ncandles = [{{ duration_seconds = 5, offset_seconds = 0 }}]\n"),
                "instruments[0].gap.reopen_seconds",
            ),
            (
                format!("{tick}jump = {{ min_basis_points = 0 }}\ncandles = [{{ duration_seconds = 5, offset_seconds = 0 }}]\n"),
                "instruments[0].jump.min_basis_points",
            ),
            (
                format!("{tick}frozen = {{ min_observations = 10, min_seconds = 0 }}\ncandles = [{{ duration_seconds = 5, offset_seconds = 0 }}]\n"),
                "instruments[0].frozen.min_observations",
            ),
            (
                "native_granularity = { kind = \"tick\", unexpected = 1 }\ncandles = [{ duration_seconds = 5, offset_seconds = 0 }]\n".to_string(),
                "unexpected",
            ),
            (
                "native_granularity = { kind = \"bar\", period_seconds = 5, unexpected = 1 }\ncandles = [{ duration_seconds = 5, offset_seconds = 0 }]\n".to_string(),
                "unexpected",
            ),
            (
                format!("{tick}sessions = []\ncandles = [{{ duration_seconds = 5, offset_seconds = 0 }}]\n"),
                "instruments[0].sessions",
            ),
            (
                format!("{tick}sessions = [{{ name = \"a\", open_seconds = 5, close_seconds = 5 }}]\ncandles = [{{ duration_seconds = 5, offset_seconds = 0 }}]\n"),
                "instruments[0].sessions[0].open_seconds",
            ),
            (
                format!("{tick}sessions = [{{ name = \"a\", open_seconds = 0, close_seconds = 604801 }}]\ncandles = [{{ duration_seconds = 5, offset_seconds = 0 }}]\n"),
                "instruments[0].sessions[0].open_seconds",
            ),
            (
                format!("{tick}sessions = [{{ name = \"a\", open_seconds = 0, close_seconds = 10 }}, {{ name = \"b\", open_seconds = 9, close_seconds = 20 }}]\ncandles = [{{ duration_seconds = 5, offset_seconds = 0 }}]\n"),
                "instruments[0].sessions[1].name",
            ),
            (
                format!("{tick}sessions = [{{ name = \"\", open_seconds = 0, close_seconds = 10 }}]\ncandles = [{{ duration_seconds = 5, offset_seconds = 0 }}]\n"),
                "instruments[0].sessions[0].name",
            ),
            (
                format!("{tick}candles = [{{ duration_seconds = 5, offset_seconds = 0 }}]\nretry = 1\n"),
                "retry",
            ),
        ];
        for (rest, key) in cases {
            let error = Config::parse(&instrument(&rest)).unwrap_err().to_string();
            assert!(error.contains(key), "{rest}: {error}");
        }
        let valid = instrument(&format!(
            "{tick}sessions = [{{ name = \"a\", open_seconds = 0, close_seconds = 10 }}, {{ name = \"b\", open_seconds = 10, close_seconds = 20 }}]\ncandles = [{{ duration_seconds = 5, offset_seconds = 0 }}]\n"
        ));
        assert!(
            Config::parse(&valid).is_ok(),
            "adjacent windows do not overlap"
        );
        let twice = format!(
            "{}\n[[instruments]]\nbroker = \"pocket_option\"\nprovider_symbol = \"AEDCNY_otc\"\nquote_currency = \"CNY\"\nprice_scale = 5\n{tick}candles = [{{ duration_seconds = 5, offset_seconds = 0 }}]\n",
            instrument(&format!(
                "{tick}candles = [{{ duration_seconds = 5, offset_seconds = 0 }}]\n"
            ))
        );
        assert!(
            Config::parse(&twice)
                .unwrap_err()
                .to_string()
                .contains("instruments[1].provider_symbol")
        );
    }
}

#[cfg(test)]
mod outcome_tests {
    use super::*;

    const HEAD: &str = "schema_version = 1\nrun_mode = \"research\"\n\n[storage]\nhistorical_data_dir = \"h\"\npublication_uri = \"file:///p\"\n";
    const TICK: &str = "file:///p/manifests/1111111111111111111111111111111111111111111111111111111111111111/ready.json";
    const FEATURE: &str = "gs://bucket/prefix/manifests/2222222222222222222222222222222222222222222222222222222222222222/ready.json";
    const TABLE: &str = "role = \"development\"\ntick_manifest = \"TICK\"\nfeature_manifest = \"FEATURE\"\nexpiry_seconds = [30, 31, 300]\nmax_entry_delay_ms = 2000\nmax_settlement_delay_ms = 2000\nmax_tick_gap_ms = 2000\ntrue_jump_max_gap_ms = 2000\ntrue_jump_basis_points = \"5\"\nfrozen_min_ticks = 10\nfrozen_min_ms = 5000\n";

    fn table(edit: impl Fn(&str) -> String) -> String {
        format!(
            "{HEAD}\n[outcomes]\n{}",
            edit(&TABLE.replace("TICK", TICK).replace("FEATURE", FEATURE))
        )
    }

    #[test]
    fn outcomes_round_trip_through_the_canonical_form() {
        let source = table(str::to_string);
        let config = Config::parse(&source).unwrap();
        assert_eq!(config.canonical_toml(), source);
        assert_eq!(Config::parse(&config.canonical_toml()).unwrap(), config);
        let outcomes = config.outcomes.as_ref().unwrap();
        assert_eq!(outcomes.expiry_seconds, [30, 31, 300]);
        assert_eq!(outcomes.tick_manifest.generation(), "1".repeat(64));
        assert_eq!(outcomes.feature_manifest.to_string(), FEATURE);
        assert_eq!(
            Config::parse(HEAD).unwrap().content_hash(),
            "v3:sha256:d7be0fdf6fb030fdfaa543417aad386f84bdb7e06ff06a61ae5646ca8e7c1256",
            "a document that omits `outcomes` keeps the identity the previous checkout gave it"
        );
        for text in ["2.5", "0.000000000000000001", "99999999999"] {
            let source = table(|body| {
                body.replace(
                    "true_jump_basis_points = \"5\"",
                    &format!("true_jump_basis_points = \"{text}\""),
                )
            });
            Config::parse(&source).unwrap();
        }
    }

    #[test]
    fn outcome_rules_reject_with_the_field_name() {
        let cases = [
            (
                "role = \"development\"",
                "role = \"holdout\"",
                "outcomes.role",
            ),
            ("[30, 31, 300]", "[]", "outcomes.expiry_seconds"),
            ("[30, 31, 300]", "[0, 30]", "outcomes.expiry_seconds"),
            ("[30, 31, 300]", "[30, 30]", "outcomes.expiry_seconds"),
            ("[30, 31, 300]", "[31, 30]", "outcomes.expiry_seconds"),
            (
                "frozen_min_ticks = 10",
                "frozen_min_ticks = 0",
                "outcomes.frozen_min_ticks",
            ),
            (
                "frozen_min_ms = 5000",
                "frozen_min_ms = 0",
                "outcomes.frozen_min_ms",
            ),
            (
                "max_entry_delay_ms = 2000",
                "max_entry_delay_ms = 18446744073709551615",
                "outcomes.max_entry_delay_ms",
            ),
            (
                "frozen_min_ms = 5000",
                "frozen_min_ms = 9223372036854775808",
                "outcomes.frozen_min_ms",
            ),
            (
                "true_jump_basis_points = \"5\"",
                "true_jump_basis_points = \"0\"",
                "outcomes.true_jump_basis_points",
            ),
            (
                "true_jump_basis_points = \"5\"",
                "true_jump_basis_points = \"-1\"",
                "outcomes.true_jump_basis_points",
            ),
            (
                "true_jump_basis_points = \"5\"",
                "true_jump_basis_points = \"5e0\"",
                "outcomes.true_jump_basis_points",
            ),
            (
                "true_jump_basis_points = \"5\"",
                "true_jump_basis_points = \"2.0000000000000000001\"",
                "outcomes.true_jump_basis_points",
            ),
            (
                "true_jump_basis_points = \"5\"",
                "true_jump_basis_points = 5",
                "true_jump_basis_points",
            ),
            (
                "tick_manifest = ",
                "tick_manifest = \"file:///p/objects/x\"\nz = ",
                "tick_manifest",
            ),
            ("frozen_min_ms = 5000\n", "", "frozen_min_ms"),
            (
                "frozen_min_ms = 5000\n",
                "frozen_min_ms = 5000\nsecret = \"x\"\n",
                "secret",
            ),
        ];
        for (from, to, field) in cases {
            let source = table(|body| body.replace(from, to));
            let error = Config::parse(&source).unwrap_err().to_string();
            assert!(error.contains(field), "{from} -> {to}: {error}");
        }
    }
}

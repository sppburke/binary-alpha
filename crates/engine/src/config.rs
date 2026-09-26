//! The version-one configuration envelope: validation, canonical form, and content hash.
//!
//! `docs/contracts.md`, section "Configuration", is the normative description of every rule here.

use std::fmt;
use std::path::{Path, PathBuf};

use serde::de::Error as _;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use sha2::{Digest, Sha256};

use crate::dataset::{DatasetRole, NativeGranularity, coverage::CoverageRange, daily::DAY_MICROS};
use crate::execution::{
    AccountSpec, Comparator, Condition, ContractTerms, Decimal, DeploymentBinding, Envelope,
    RateEvent, ReplayInput, RiskPolicy, Split, StrategySpec, Threshold,
};
use crate::market::{BrokerId, Currency, InstrumentId, PriceScale, ProviderSymbol};
use crate::research::EXECUTION_CONTRACT_V1;

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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub split: Option<DataSplit>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub instruments: Vec<Instrument>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub features: Option<Features>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub outcomes: Option<Outcomes>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub replay: Option<Replay>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub accelerator: Option<Accelerator>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub search: Option<Search>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub portfolio: Option<Portfolio>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub research: Option<Research>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub brokers: Vec<Broker>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub history: Option<History>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub inspect: Option<Inspect>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub live: Option<Live>,
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
        if self
            .accelerator
            .as_ref()
            .is_some_and(|accelerator| accelerator.devices.is_empty())
        {
            return Err("accelerator.devices: at least one device is required".into());
        }
        if self.run_mode != RunMode::Research
            && matches!(self.storage.publication_uri, PublicationUri::Filesystem(_))
        {
            return Err(format!(
                "storage.publication_uri: a `file://` destination requires run_mode `research`, not `{}`",
                self.run_mode.as_str()
            ));
        }
        if let Some(split) = &self.split {
            split
                .validate()
                .map_err(|reason| format!("split.{reason}"))?;
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
        if self.live.is_some() && self.replay.is_some() {
            return Err("live: cannot be combined with [replay]".into());
        }
        if let Some(live) = &self.live {
            live.validate().map_err(|reason| format!("live.{reason}"))?;
            let broker = self
                .brokers
                .iter()
                .find(|broker| broker.id() == &live.broker)
                .ok_or("live.broker: broker is not declared under [[brokers]]")?;
            match self.run_mode {
                RunMode::Paper | RunMode::Live => {
                    if live.replay.is_some() {
                        return Err("live.replay: must be absent for run_mode paper or live".into());
                    }
                    if broker.credential().is_none() {
                        return Err(
                            "live.broker: credential is required for run_mode paper or live".into(),
                        );
                    }
                }
                RunMode::Research | RunMode::Replay => {
                    if live.replay.is_none() {
                        return Err(
                            "live.replay: is required for run_mode research or replay".into()
                        );
                    }
                }
            }
        }
        if let Some(replay) = &self.replay {
            replay
                .validate()
                .map_err(|reason| format!("replay.{reason}"))?;
        }
        if let Some(search) = &self.search {
            search
                .validate()
                .map_err(|reason| format!("search.{reason}"))?;
        }
        if let Some(portfolio) = &self.portfolio {
            if portfolio.generate.is_some() {
                return Err(
                    "portfolio.generate: only a research portfolio may generate members".into(),
                );
            }
            portfolio
                .validate()
                .map_err(|reason| format!("portfolio.{reason}"))?;
        }
        if let Some(research) = &self.research {
            research
                .validate()
                .map_err(|reason| format!("research.{reason}"))?;
            for (index, instrument) in research.instruments.iter().enumerate() {
                if !self
                    .instruments
                    .iter()
                    .any(|configured| configured.id().to_string() == instrument.instrument)
                {
                    return Err(format!(
                        "research.instruments[{index}].instrument: {} maps no configured instrument",
                        instrument.instrument
                    ));
                }
            }
        }
        self.validate_brokers()?;
        if self.live.is_none() && matches!(self.run_mode, RunMode::Paper | RunMode::Live) {
            return Err("live: is required for run_mode paper or live".into());
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
                    instruments,
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
                    if let Some(instruments) = instruments {
                        if instruments.is_empty() {
                            return Err(format!(
                                "{}: at least one asset name is required",
                                field("instruments")
                            ));
                        }
                        for (position, name) in instruments.iter().enumerate() {
                            ProviderSymbol::try_from(name.clone())
                                .map_err(|reason| format!("{}: {reason}", field("instruments")))?;
                            if instruments[..position].contains(name) {
                                return Err(format!(
                                    "{}: {name} is listed twice",
                                    field("instruments")
                                ));
                            }
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

impl Config {
    fn validate_brokers(&self) -> Result<(), String> {
        for (index, broker) in self.brokers.iter().enumerate() {
            broker
                .validate(self.run_mode)
                .map_err(|reason| format!("brokers[{index}]: {reason}"))?;
            if self.brokers[..index]
                .iter()
                .any(|prior| prior.id() == broker.id())
            {
                return Err(format!(
                    "brokers[{index}]: duplicate broker id {}",
                    broker.id()
                ));
            }
        }
        if let Some(history) = &self.history {
            history
                .validate()
                .map_err(|reason| format!("history: {reason}"))?;
            let broker = self
                .brokers
                .iter()
                .find(|broker| broker.id() == &history.broker)
                .ok_or("history: broker is not declared under brokers")?;
            if !broker.kind().capabilities().history {
                return Err("history: broker has no history capability".into());
            }
            for (index, symbol) in history.instruments.iter().enumerate() {
                if history.instruments[..index].contains(symbol) {
                    return Err(format!("history: instruments[{index}] is listed twice"));
                }
                if !self.instruments.iter().any(|instrument| {
                    instrument.broker == history.broker
                        && instrument.provider_symbol == *symbol
                        && instrument.native_granularity == history.native_granularity
                }) {
                    return Err(format!(
                        "history: instruments[{index}] must name a declared {} instrument",
                        history.native_granularity
                    ));
                }
            }
        }
        if let Some(inspect) = &self.inspect {
            inspect
                .validate()
                .map_err(|reason| format!("inspect: {reason}"))?;
            let history = self.history.as_ref().ok_or("inspect: requires history")?;
            let broker = self
                .brokers
                .iter()
                .find(|broker| broker.id() == &history.broker)
                .ok_or("inspect: history broker is not declared")?;
            if !broker.kind().capabilities().live {
                return Err("inspect: broker has no live capability".into());
            }
            if inspect.proposal.is_some()
                && (!broker.kind().capabilities().execution || broker.credential().is_none())
            {
                return Err(
                    "inspect: proposal requires a Deriv broker with a credential reference".into(),
                );
            }
        }
        Ok(())
    }
}

crate::string_enum! {
    /// A statically compiled provider adapter.
    BrokerKind "broker kind" { Deriv => "deriv", PocketOption => "pocket_option" }
}

/// Capabilities of each compiled adapter, checked before connection.
#[derive(Debug, Clone, Copy)]
pub struct Capabilities {
    pub history: bool,
    pub live: bool,
    pub execution: bool,
}
impl BrokerKind {
    pub fn capabilities(self) -> Capabilities {
        Capabilities {
            history: true,
            live: true,
            execution: self == Self::Deriv,
        }
    }
}

crate::string_enum! {
    /// The account class the server must confirm.
    AccountClass "account_class" { Demo => "demo", Real => "real" }
}

/// One provider connection, selected by its declared kind.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Broker {
    Deriv(DerivSettings),
    PocketOption(PocketSettings),
}

/// Whether `reference` is an environment variable name a credential may be resolved from.
pub fn credential_name(reference: &str) -> bool {
    !reference.is_empty()
        && reference
            .bytes()
            .enumerate()
            .all(|(i, b)| b == b'_' || b.is_ascii_alphabetic() || (i > 0 && b.is_ascii_digit()))
}

/// Public and authenticated Deriv connection settings; credentials are environment names.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DerivSettings {
    pub id: BrokerId,
    pub public_endpoint: String,
    pub bootstrap_endpoint: String,
    pub app_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub credential: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub account_class: Option<AccountClass>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub budgets: Option<RateBudgets>,
}

/// Pocket Option's observed direct market interface and its declared source clock.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PocketSettings {
    pub id: BrokerId,
    pub endpoint: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub origin: Option<String>,
    pub credential: String,
    /// A program and its arguments that print a fresh authentication object to standard
    /// output; run when the referenced variable is unset and again, once, after the provider
    /// rejects a session. The program is operator tooling outside this repository.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub credential_command: Option<Vec<String>>,
    pub account_class: AccountClass,
    pub server_offset_minutes: i32,
    /// Maximum unconsumed candle pages, including outstanding requests; defaults to eight.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub history_pages_in_flight: Option<u16>,
}

impl Broker {
    pub fn id(&self) -> &BrokerId {
        match self {
            Self::Deriv(settings) => &settings.id,
            Self::PocketOption(settings) => &settings.id,
        }
    }
    pub fn kind(&self) -> BrokerKind {
        match self {
            Self::Deriv(_) => BrokerKind::Deriv,
            Self::PocketOption(_) => BrokerKind::PocketOption,
        }
    }
    pub fn credential(&self) -> Option<&str> {
        match self {
            Self::Deriv(settings) => settings.credential.as_deref(),
            Self::PocketOption(settings) => Some(&settings.credential),
        }
    }
    pub fn endpoint(&self) -> &str {
        match self {
            Self::Deriv(settings) => &settings.public_endpoint,
            Self::PocketOption(settings) => &settings.endpoint,
        }
    }
    pub fn validate(&self, mode: RunMode) -> Result<(), String> {
        fn endpoint(text: &str, secure: &str, plain: &str, mode: RunMode) -> Result<(), String> {
            let rest = text
                .strip_prefix(secure)
                .or_else(|| {
                    (mode == RunMode::Research)
                        .then(|| text.strip_prefix(plain))
                        .flatten()
                })
                .ok_or_else(|| {
                    format!("endpoint must use {secure}; {plain} requires run_mode research")
                })?;
            let authority = rest.split(['/', '?', '#']).next().unwrap_or_default();
            if authority.is_empty()
                || authority.contains('@')
                || text.chars().any(char::is_whitespace)
                || text.contains('#')
            {
                return Err(
                    "endpoint must have a host and no user information, whitespace, or fragment"
                        .into(),
                );
            }
            Ok(())
        }
        endpoint(self.endpoint(), "wss://", "ws://", mode)?;
        if let Some(reference) = self.credential()
            && !credential_name(reference)
        {
            return Err("credential must be an environment variable name".into());
        }
        match self {
            Self::Deriv(settings) => {
                endpoint(&settings.bootstrap_endpoint, "https://", "http://", mode)?;
                if settings.app_id.is_empty()
                    || settings.app_id.bytes().any(|b| b.is_ascii_control())
                {
                    return Err("app_id must be non-empty header text".into());
                }
                if settings.credential.is_some() && settings.account_class.is_none() {
                    return Err("account_class is required with credential".into());
                }
                if let Some(budgets) = &settings.budgets {
                    budgets.validate()?;
                }
            }
            Self::PocketOption(settings) => {
                if settings.history_pages_in_flight == Some(0) {
                    return Err("history_pages_in_flight must be positive".into());
                }
                if let Some(command) = &settings.credential_command
                    && command.first().is_none_or(String::is_empty)
                {
                    return Err("credential_command must name a program".into());
                }
                if settings.origin.as_ref().is_some_and(|origin| {
                    origin.is_empty() || origin.bytes().any(|b| b.is_ascii_control())
                }) {
                    return Err("origin must be non-empty header text".into());
                }
            }
        }
        Ok(())
    }
}

/// Both documented sliding windows for one Deriv request group.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RateLimit {
    pub per_minute: u32,
    pub per_hour: u32,
}

/// Request-group limits may be reduced from the documented maxima.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RateBudgets {
    pub trade: RateLimit,
    pub account: RateLimit,
    pub portfolio: RateLimit,
    pub other: RateLimit,
}
impl Default for RateBudgets {
    fn default() -> Self {
        Self {
            trade: RateLimit {
                per_minute: 360,
                per_hour: 14_400,
            },
            account: RateLimit {
                per_minute: 100,
                per_hour: 2_000,
            },
            portfolio: RateLimit {
                per_minute: 30,
                per_hour: 1_500,
            },
            other: RateLimit {
                per_minute: 220,
                per_hour: 14_400,
            },
        }
    }
}
impl RateBudgets {
    pub fn validate(&self) -> Result<(), String> {
        let maxima = Self::default();
        for (name, limit, maximum) in [
            ("trade", self.trade, maxima.trade),
            ("account", self.account, maxima.account),
            ("portfolio", self.portfolio, maxima.portfolio),
            ("other", self.other, maxima.other),
        ] {
            if limit.per_minute == 0
                || limit.per_hour == 0
                || limit.per_minute > maximum.per_minute
                || limit.per_hour > maximum.per_hour
            {
                return Err(format!(
                    "budgets.{name}: limits must be positive and no greater than {} per minute and {} per hour",
                    maximum.per_minute, maximum.per_hour
                ));
            }
        }
        Ok(())
    }
}

/// Bounded native-history acquisition, optionally repeated toward a new fixed end time each
/// pass, and optionally extended from explicitly bound imported seed generations.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct History {
    pub broker: BrokerId,
    pub instruments: Vec<ProviderSymbol>,
    pub role: DatasetRole,
    pub start: String,
    pub end: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub refresh_interval_seconds: Option<u32>,
    /// The native representation requested from the provider; an absent field is ticks.
    #[serde(default, skip_serializing_if = "NativeGranularity::is_tick")]
    pub native_granularity: NativeGranularity,
    /// Imported generations an acquisition extends, each bound to its provider symbol, ready
    /// manifest, and the source identity it was collected under.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub seeds: Vec<Seed>,
    /// Seconds before the acquisition frontier that a new acquisition re-reads and compares.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub overlap_seconds: Option<u32>,
    /// Pages one invocation may request before the acquisition stays pending.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_pages: Option<u32>,
    /// Seconds one invocation may spend paging before the acquisition stays pending.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_elapsed_seconds: Option<u32>,
}

/// One imported generation an acquisition extends, bound to the source context it came from.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Seed {
    pub provider_symbol: ProviderSymbol,
    pub manifest: ManifestUri,
    /// The broker source identity the seed was collected under; acquisition refuses a
    /// configured broker whose identity differs.
    pub source_identity: String,
}

impl History {
    pub fn validate(&self) -> Result<(), String> {
        if self.role == DatasetRole::Holdout {
            return Err("role: holdout data is never a history input".into());
        }
        if self.instruments.is_empty() {
            return Err("instruments: at least one instrument is required".into());
        }
        let start = crate::market::parse_event_time_micros(&self.start)
            .map_err(|e| format!("start: {e}"))?;
        let end =
            crate::market::parse_event_time_micros(&self.end).map_err(|e| format!("end: {e}"))?;
        if start >= end {
            return Err("start must precede end".into());
        }
        for (name, value) in [
            ("refresh_interval_seconds", self.refresh_interval_seconds),
            ("overlap_seconds", self.overlap_seconds),
            ("max_pages", self.max_pages),
            ("max_elapsed_seconds", self.max_elapsed_seconds),
        ] {
            if value == Some(0) {
                return Err(format!("{name} must be positive"));
            }
        }
        if self.native_granularity == (NativeGranularity::Bar { period_seconds: 0 }) {
            return Err("native_granularity.period_seconds: must be positive".into());
        }
        for (index, seed) in self.seeds.iter().enumerate() {
            let field = |name: &str| format!("seeds[{index}].{name}");
            if !self.instruments.contains(&seed.provider_symbol) {
                return Err(format!(
                    "{}: {} is not a history instrument",
                    field("provider_symbol"),
                    seed.provider_symbol
                ));
            }
            if self.seeds[..index]
                .iter()
                .any(|earlier| earlier.provider_symbol == seed.provider_symbol)
            {
                return Err(format!(
                    "{}: {} is listed twice",
                    field("provider_symbol"),
                    seed.provider_symbol
                ));
            }
            if !crate::dataset::is_hex64(&seed.source_identity) {
                return Err(format!(
                    "{}: must be sixty-four lowercase hexadecimal digits",
                    field("source_identity")
                ));
            }
        }
        Ok(())
    }
}

/// Finite market observations and optional non-purchasing proposal checks.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Inspect {
    pub live_observations: u32,
    pub live_seconds: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub proposal: Option<InspectProposal>,
}
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct InspectProposal {
    pub stake: Decimal,
    pub duration_seconds: u32,
}
impl Inspect {
    pub fn validate(&self) -> Result<(), String> {
        if self.live_observations == 0 || self.live_seconds == 0 {
            return Err("live_observations and live_seconds must be positive".into());
        }
        if let Some(proposal) = &self.proposal
            && (proposal.stake.is_zero()
                || proposal.stake.is_negative()
                || proposal.duration_seconds == 0)
        {
            return Err("proposal: stake and duration_seconds must be positive".into());
        }
        Ok(())
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
/// the filesystem implementation under `research`.
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
        /// The asset names to import; an absent list imports every asset the manifest lists,
        /// and a present list opens no other asset tree.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        instruments: Option<Vec<String>>,
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
    #[serde(with = "encoding_outputs")]
    pub outputs: Vec<EncodingSpec>,
}

mod encoding_outputs {
    use super::EncodingSpec;
    use serde::{
        Deserializer, Serialize, Serializer,
        de::{Error, SeqAccess, Visitor},
    };
    use std::fmt;

    pub fn serialize<S: Serializer>(
        outputs: &[EncodingSpec],
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        if outputs.len() == 1 && outputs[0].output == "all_supported" && outputs[0].bins.is_none() {
            serializer.serialize_str("all_supported")
        } else {
            outputs.serialize(serializer)
        }
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Vec<EncodingSpec>, D::Error> {
        struct OutputsVisitor;
        impl<'de> Visitor<'de> for OutputsVisitor {
            type Value = Vec<EncodingSpec>;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("`all_supported` or an encoding list")
            }

            fn visit_str<E: Error>(self, text: &str) -> Result<Self::Value, E> {
                if text == "all_supported" {
                    Ok(vec![EncodingSpec {
                        output: text.to_string(),
                        bins: None,
                    }])
                } else {
                    Err(E::custom(format!("unknown encodings outputs `{text}`")))
                }
            }

            fn visit_string<E: Error>(self, text: String) -> Result<Self::Value, E> {
                self.visit_str(&text)
            }

            fn visit_seq<A: SeqAccess<'de>>(
                self,
                mut sequence: A,
            ) -> Result<Self::Value, A::Error> {
                let mut outputs = Vec::new();
                while let Some(output) = sequence.next_element::<EncodingSpec>()? {
                    if output.output == "all_supported" {
                        return Err(A::Error::custom(
                            "`all_supported` is a string mode, not an encoding output",
                        ));
                    }
                    outputs.push(output);
                }
                Ok(outputs)
            }
        }
        deserializer.deserialize_any(OutputsVisitor)
    }
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

/// The optional live-runtime table. Absence preserves every existing configuration identity.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Live {
    /// Exactly `historical_baseline_to_broker_v1`.
    pub execution_contract: String,
    /// Ready manifest of the awaiting research run carrying the DeploymentBundle.
    pub bundle_manifest: ManifestUri,
    /// Ready manifest of the certification generation; only its public envelope is read.
    pub certification_manifest: ManifestUri,
    /// The configured broker that executes the frozen policy's one logical account.
    pub broker: BrokerId,
    /// The frozen policy's logical account id.
    pub account: String,
    /// One verified tick ready manifest per bundle instrument, in bundle instrument order.
    pub warmup: Vec<ManifestUri>,
    /// Measurement requirements frozen before observations.
    pub compatibility: Compatibility,
    /// Local journal segment and spool bounds.
    pub journal: JournalSettings,
    /// Cooperative account ownership and pre-dispatch durability.
    pub control: ControlSettings,
    /// Present only for `live replay`: the recorded broker-event log to drive.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub replay: Option<LiveReplay>,
}

/// The frozen observation window, required account-class evidence, and sample support.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Compatibility {
    /// Inclusive event-time start, parsed by the shared market clock owner.
    pub observation_start: String,
    /// Exclusive event-time end.
    pub observation_end: String,
    /// The account-class evidence required for real promotion.
    pub required_account_class: AccountClass,
    /// Positive sample support required for every measured dimension.
    pub min_samples: u32,
}

/// The local append-only journal's location and measured bounds.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct JournalSettings {
    /// Relative path under the configuration directory.
    pub dir: String,
    /// Positive number of records per closed segment.
    pub segment_records: u32,
    /// Positive spool bound: open plus verified-but-unuploaded closed segment bytes.
    pub max_spool_bytes: u64,
}

/// The encrypted control connection and cooperative lease timing contract.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ControlSettings {
    /// Endpoint hostname, also used for certificate verification.
    pub host: String,
    /// Endpoint port.
    pub port: u16,
    /// Database name.
    pub database: String,
    /// Database user.
    pub user: String,
    /// Environment variable name holding the password, never the password itself.
    pub credential: String,
    /// Relative path of the endpoint's supplied trusted root certificate PEM file.
    pub root_certificate: String,
    /// This runtime's owner identity in the lease row.
    pub owner: String,
    /// Positive lease lifetime.
    pub lease_ttl_micros: i64,
    /// Positive renewal interval strictly below the lease lifetime.
    pub renewal_interval_micros: i64,
    /// Non-negative measured margin; interval plus margin must be below the lifetime.
    pub safety_margin_micros: i64,
}

/// The recorded broker-event input for `live replay`.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct LiveReplay {
    /// Relative path of the recorded broker-event log.
    pub broker_log: String,
}

impl Live {
    /// The rules a single field's deserializer cannot see; an error names the field.
    pub fn validate(&self) -> Result<(), String> {
        if self.execution_contract != EXECUTION_CONTRACT_V1 {
            return Err(format!(
                "execution_contract: `{}` is not `{EXECUTION_CONTRACT_V1}`",
                self.execution_contract
            ));
        }
        if self.account.is_empty() {
            return Err("account: must be non-empty".into());
        }
        if self.warmup.is_empty() {
            return Err("warmup: at least one manifest is required".into());
        }
        let start = crate::market::parse_event_time_micros(&self.compatibility.observation_start)
            .map_err(|reason| format!("compatibility.observation_start: {reason}"))?;
        let end = crate::market::parse_event_time_micros(&self.compatibility.observation_end)
            .map_err(|reason| format!("compatibility.observation_end: {reason}"))?;
        if start >= end {
            return Err("compatibility.observation_end: must be after observation_start".into());
        }
        if self.compatibility.min_samples == 0 {
            return Err("compatibility.min_samples: must be positive".into());
        }
        relative_path(&self.journal.dir).map_err(|reason| format!("journal.dir: {reason}"))?;
        if self.journal.segment_records == 0 {
            return Err("journal.segment_records: must be positive".into());
        }
        if self.journal.max_spool_bytes == 0 {
            return Err("journal.max_spool_bytes: must be positive".into());
        }
        if !credential_name(&self.control.credential) {
            return Err("control.credential: must be an environment variable name".into());
        }
        relative_path(&self.control.root_certificate)
            .map_err(|reason| format!("control.root_certificate: {reason}"))?;
        if self.control.lease_ttl_micros <= 0 {
            return Err("control.lease_ttl_micros: must be positive".into());
        }
        if self.control.renewal_interval_micros <= 0
            || self.control.renewal_interval_micros >= self.control.lease_ttl_micros
        {
            return Err(
                "control.renewal_interval_micros: must be positive and below lease_ttl_micros"
                    .into(),
            );
        }
        if self.control.safety_margin_micros < 0
            || self
                .control
                .renewal_interval_micros
                .checked_add(self.control.safety_margin_micros)
                .is_none_or(|total| total >= self.control.lease_ttl_micros)
        {
            return Err("control.safety_margin_micros: must be non-negative and renewal_interval_micros plus margin must be below lease_ttl_micros".into());
        }
        if let Some(replay) = &self.replay {
            relative_path(&replay.broker_log)
                .map_err(|reason| format!("replay.broker_log: {reason}"))?;
        }
        Ok(())
    }
}

/// The historical replay consumed only by `binary-alpha replay`: the declared role and half-open
/// decision window, the verified inputs per instrument in tie-breaking order, optional reporting
/// splits, the funded accounts, the strategies, the ordered deployment bindings, the contract
/// templates, the risk policies, and the exact currency-aggregation contract. The engine module
/// owns every record and every rule.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Replay {
    pub role: DatasetRole,
    pub decision_start: String,
    pub decision_end: String,
    pub inputs: Vec<ReplayInput>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub splits: Option<Vec<Split>>,
    pub accounts: Vec<AccountSpec>,
    pub strategies: Vec<StrategySpec>,
    pub bindings: Vec<DeploymentBinding>,
    pub contracts: Vec<ContractTerms>,
    pub risk_policies: Vec<RiskPolicy>,
    pub reporting_currency: Currency,
    pub reporting_scale: u8,
    pub max_rate_age_micros: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rates: Option<Vec<RateEvent>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scenario: Option<ReplayScenario>,
}

impl Replay {
    /// The rules a single field's deserializer cannot see; an error names the field.
    pub fn validate(&self) -> Result<(), String> {
        crate::execution::validate(self)
    }
}

/// The optional `search` table: one typed candidate search over a development input set and an
/// optional evaluation input set. Omitting the table preserves every existing identity.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Search {
    pub scope: Scope,
    pub seed: u64,
    pub chunk_size: u32,
    pub max_candidates: u64,
    pub min_conditions: u32,
    pub max_conditions: u32,
    pub embargo_micros: i64,
    pub base_stream: StreamKey,
    pub development: SearchWindow,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub evaluation: Option<SearchWindow>,
    pub conditions: Vec<SearchCondition>,
    pub contracts: Vec<ContractTerms>,
    pub account: SearchAccount,
    pub risk_policy: RiskPolicy,
    pub envelope: Envelope,
    pub gates: crate::search::Gates,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub screen: Option<Screen>,
    pub stability: StabilitySettings,
}

impl Search {
    /// The rules a single field's deserializer cannot see; an error names the field.
    pub fn validate(&self) -> Result<(), String> {
        crate::search::validate(self)
    }
}

crate::string_enum! {
    /// Whether every structurally valid member is replayed, or the model score prunes first.
    Scope "search scope" {
        Exhaustive => "exhaustive",
        Heuristic => "heuristic",
    }
}

/// One role's decision window, bound inputs, and optional reporting splits.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SearchWindow {
    pub decision_start: String,
    pub decision_end: String,
    pub inputs: Vec<ReplayInput>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub splits: Option<Vec<Split>>,
}

/// A named menu entry or a plan-bound rule. The rule has no threshold field, so it cannot
/// accidentally be treated as a named condition before the development plan is bound.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
#[serde(untagged)]
pub enum SearchCondition {
    Named(NamedSearchCondition),
    Generate(GeneratedSearchCondition),
}

#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct NamedSearchCondition {
    pub stream: StreamKey,
    pub output: String,
    pub comparator: Comparator,
    pub thresholds: Vec<Threshold>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct GeneratedSearchCondition {
    pub stream: StreamKey,
    pub output: String,
    pub comparator: Comparator,
}

/// The account every member is funded from, one account per member.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SearchAccount {
    pub broker: BrokerId,
    pub currency: Currency,
    pub scale: u8,
    pub initial_cash: Decimal,
}

/// Heuristic screening: members above the adjusted score, or beyond the first `top`, are
/// eliminated before any engine replay.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Screen {
    pub max_adjusted_score: f64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub top: Option<u32>,
}

/// The frozen stationary-block resampling settings.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct StabilitySettings {
    pub block_length: u32,
    pub simulations: u32,
    pub rolling_horizon: u32,
}

/// The optional `portfolio` table: one finite joint selection over development-only families
/// through the chronological engine. Omitting the table preserves every existing configuration
/// identity. The engine module `portfolio` owns every rule and record.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Portfolio {
    pub families: Vec<ManifestUri>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub generate: Option<PortfolioGenerate>,
    pub max_policies: u64,
    pub embargo_micros: i64,
    pub objective: crate::portfolio::Objective,
    pub gates: crate::portfolio::Gates,
    pub accounts: Vec<AccountSpec>,
    pub reporting_currency: Currency,
    pub reporting_scale: u8,
    pub max_rate_age_micros: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rates: Option<Vec<RateEvent>>,
    pub members: Vec<PortfolioMember>,
    pub repairs: Vec<Repair>,
    pub bindings: Vec<PortfolioBinding>,
    pub subsets: Vec<Subset>,
    pub risk_policies: Vec<RiskPolicy>,
    pub folds: Vec<Fold>,
    pub refit: Refit,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub evaluation: Option<Evaluation>,
}

/// Research-only rule resolved against verified, ranked development families. `nested` deploys
/// each family's first `k` generated members together, for every `k` up to `top`.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PortfolioGenerate {
    pub top: u32,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub nested: bool,
}

impl Portfolio {
    /// The rules a single field's deserializer cannot see; an error names the field.
    pub fn validate(&self) -> Result<(), String> {
        crate::portfolio::validate(self)
    }
}

/// One base of the universe: a member of a listed family, with the interval ordinal every
/// development-fifths condition resolves to per fold.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PortfolioMember {
    pub family: usize,
    pub member: usize,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub ordinals: Vec<Ordinal>,
}

/// The zero-based low-to-high interval ordinal, `0` to `4`, of one member condition on a
/// development-fifths encoding.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Ordinal {
    pub condition: usize,
    pub ordinal: u8,
}

/// One protective-condition alternative; an empty conjunction is no repair.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Repair {
    pub id: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub conditions: Vec<Condition>,
}

/// One account and instrument a deployment binds to, with its complete contract and envelope
/// alternatives.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PortfolioBinding {
    pub id: String,
    pub account: String,
    /// `BROKER:PROVIDER_SYMBOL`, the neutral instrument identity of one fold input.
    pub instrument: String,
    pub alternatives: Vec<Alternative>,
}

/// One complete exact contract and its matching envelope.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Alternative {
    pub contract: ContractTerms,
    pub envelope: Envelope,
}

/// One allowed ordered subset: the deployments in binding priority order.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Subset {
    pub deployments: Vec<Deployment>,
}

/// One deployment of a subset: the base member with one repair on one binding.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Deployment {
    pub member: usize,
    pub repair: usize,
    pub binding: usize,
}

/// One inner fold: the cutoff every fit ends before, the assessment window that starts at
/// least the embargo after it, and one fit and assessment input per instrument.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Fold {
    pub cutoff: String,
    pub decision_start: String,
    pub decision_end: String,
    pub inputs: Vec<FoldInput>,
}

/// One instrument's fold inputs: the development fit entry (a new plan on its own profile's
/// source generation) and the development tick generation the fitted plan is applied to.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct FoldInput {
    pub fit: FeatureInstrument,
    pub assessment_manifest: ManifestUri,
}

/// The final refit of the selected choice: the cutoff and one development fit per instrument
/// on the full permitted development generation.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Refit {
    pub cutoff: String,
    pub fits: Vec<FeatureInstrument>,
}

/// The optional outer evaluation of the frozen choice: one continuous joint replay over the
/// evaluation tick generations with the declared reporting splits.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Evaluation {
    pub decision_start: String,
    pub decision_end: String,
    pub inputs: Vec<ManifestUri>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub splits: Option<Vec<Split>>,
}

/// The optional `research` table: the one-command study that prepares, searches, selects,
/// assesses, and, under separate authorization, certifies one policy over the declared
/// historical generations. Omitting the table preserves every existing configuration identity.
/// The engine module `research` owns every rule, record, and lowering into the existing tables.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Research {
    pub study: Study,
    pub instruments: Vec<ResearchInstrument>,
    pub folds: Vec<ResearchFold>,
    pub refit: ResearchRefit,
    /// The outer evaluation: its window, one evaluation tick generation per instrument, and
    /// optional reporting splits.
    pub evaluation: Evaluation,
    /// The holdout window and one holdout tick generation reference per instrument, validated
    /// for syntax only; the run never opens them before certification.
    pub holdout: Evaluation,
    pub portfolio: ResearchPortfolio,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub scenarios: Vec<ResearchScenario>,
    pub qualification: Qualification,
}

impl Research {
    /// The rules a single field's deserializer cannot see; an error names the field.
    pub fn validate(&self) -> Result<(), String> {
        crate::research::validate(self)
    }
}

/// The study and attempt identities, the operator-declared non-sensitive governance
/// declaration, the predecessor attempts, and the declared manual or procedure changes.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Study {
    pub study: String,
    pub attempt: String,
    /// `file:///DIR/FILE.json` or `gs://BUCKET/KEY`: the declaration object.
    pub governance_manifest: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub predecessors: Vec<String>,
    pub changes: String,
}

/// One instrument of the study: the development family-source generation and the feature,
/// outcome, and search settings the run applies to it.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ResearchInstrument {
    /// `BROKER:PROVIDER_SYMBOL`, the neutral identity of one configured instrument.
    pub instrument: String,
    pub source_manifest: ManifestUri,
    pub features: FeatureSettings,
    pub outcomes: OutcomeSettings,
    pub search: ResearchSearch,
}

/// The new-plan settings of a feature fit, exactly the optional settings of a
/// `features.instruments` entry.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct FeatureSettings {
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub price_epsilon: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tick_path_streams: Option<Vec<StreamKey>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub encodings: Option<Encodings>,
}

/// The outcome-build settings, exactly the `outcomes` table without its role and manifests.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct OutcomeSettings {
    pub expiry_seconds: Vec<u32>,
    pub max_entry_delay_ms: u64,
    pub max_settlement_delay_ms: u64,
    pub max_tick_gap_ms: u64,
    pub true_jump_max_gap_ms: u64,
    pub true_jump_basis_points: String,
    pub frozen_min_ticks: u32,
    pub frozen_min_ms: u64,
}

/// The development-only search of one instrument: the `search` table's settings with its
/// development window and no inputs or evaluation; the run binds the inputs.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ResearchSearch {
    pub decision_start: String,
    pub decision_end: String,
    pub scope: Scope,
    pub seed: u64,
    pub chunk_size: u32,
    pub max_candidates: u64,
    pub min_conditions: u32,
    pub max_conditions: u32,
    pub embargo_micros: i64,
    pub base_stream: StreamKey,
    pub conditions: Vec<SearchCondition>,
    pub contracts: Vec<ContractTerms>,
    pub account: SearchAccount,
    pub risk_policy: RiskPolicy,
    pub envelope: Envelope,
    pub gates: crate::search::Gates,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub screen: Option<Screen>,
    pub stability: StabilitySettings,
}

/// One inner fold: the cutoff, the assessment window, and per instrument the development fit
/// and assessment tick generations, in instrument order.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ResearchFold {
    pub cutoff: String,
    pub decision_start: String,
    pub decision_end: String,
    pub inputs: Vec<ResearchFoldInput>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ResearchFoldInput {
    pub fit_manifest: ManifestUri,
    pub assessment_manifest: ManifestUri,
}

/// The final refit: the cutoff and one development fit tick generation per instrument.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ResearchRefit {
    pub cutoff: String,
    pub fits: Vec<ManifestUri>,
}

/// The portfolio selection settings, exactly the `portfolio` table without its families, folds,
/// refit, and evaluation; the run lowers those from the declarations above. Family index `i`
/// of a member is instrument `i`.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ResearchPortfolio {
    pub max_policies: u64,
    pub embargo_micros: i64,
    pub objective: crate::portfolio::Objective,
    pub gates: crate::portfolio::Gates,
    pub accounts: Vec<AccountSpec>,
    pub reporting_currency: Currency,
    pub reporting_scale: u8,
    pub max_rate_age_micros: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rates: Option<Vec<RateEvent>>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub members: Vec<PortfolioMember>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub generate: Option<PortfolioGenerate>,
    pub repairs: Vec<Repair>,
    pub bindings: Vec<PortfolioBinding>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub subsets: Vec<Subset>,
    pub risk_policies: Vec<RiskPolicy>,
}

/// One required assessment scenario: a checked non-negative synthetic acceptance delay and the
/// complete per-binding exact contract and envelope map the frozen policy is replayed under.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ResearchScenario {
    pub id: String,
    pub acceptance_delay_micros: i64,
    pub alternatives: Vec<ScenarioAlternative>,
}

/// The exact terms one portfolio binding deploys under in a scenario.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ScenarioAlternative {
    pub binding: String,
    pub contract: ContractTerms,
    pub envelope: Envelope,
}

/// The empirical qualification: the version-one claim and the exact gates the frozen policy
/// must satisfy on every scenario, against the analytic zero-profit benchmark on the same
/// initial capital; `min_profit` is the minimum economically useful improvement.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Qualification {
    pub claim: String,
    pub gates: crate::portfolio::Gates,
}

/// The optional acceptance-delay scenario of a replay, version one: every admitted command's
/// synthetic acceptance is scheduled at its decision time plus the delay. Omission is immediate
/// acceptance with every existing identity preserved.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ReplayScenario {
    /// `1`: acceptance at the checked decision time plus the delay, at the instrument's latest
    /// causally available tick, drained through that instrument's own evidence horizon.
    pub schema_version: u32,
    pub id: String,
    pub acceptance_delay_micros: i64,
}

/// Explicit offline accelerator selection. Absence preserves existing configuration identity.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Accelerator {
    /// The requested execution backend; application loading checks build availability.
    pub backend: Backend,
    /// Device ordinals used by offline CUDA screening. The default preserves old config bytes.
    #[serde(
        default = "default_accelerator_devices",
        skip_serializing_if = "default_devices"
    )]
    pub devices: Vec<usize>,
}

fn default_accelerator_devices() -> Vec<usize> {
    vec![0]
}
fn default_devices(devices: &Vec<usize>) -> bool {
    devices.as_slice() == [0]
}

crate::string_enum! {
    /// Offline accelerator backend.
    Backend "accelerator backend" {
        Cpu => "cpu",
        Cuda => "cuda",
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
    /// Trading calendar for the daily continuous candle product (not profile windows).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session: Option<crate::session::Session>,
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
        if let Some(session) = &self.session {
            session.calendar()?;
        }
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

/// Whole-day research populations cut from development daily roots.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DataSplit {
    pub namespace: String,
    pub sources: Vec<ManifestUri>,
    pub development: Vec<CoverageRange>,
    pub evaluation: Vec<CoverageRange>,
    pub holdout: Vec<CoverageRange>,
}

impl DataSplit {
    fn validate(&self) -> Result<(), String> {
        crate::research::identifier("namespace", &self.namespace)?;
        if self.sources.is_empty() {
            return Err("sources: at least one source is required".into());
        }
        for (index, source) in self.sources.iter().enumerate() {
            if self.sources[..index].contains(source) {
                return Err("sources: a source is declared twice".into());
            }
        }
        self.windows().map(|_| ())
    }

    /// Validated, canonically spelled windows, coalescing identical development uses.
    pub fn windows(&self) -> Result<Vec<(DatasetRole, CoverageRange)>, String> {
        let mut windows: Vec<(DatasetRole, CoverageRange)> = Vec::new();
        for (role, ranges) in [
            (DatasetRole::Development, &self.development),
            (DatasetRole::Evaluation, &self.evaluation),
            (DatasetRole::Holdout, &self.holdout),
        ] {
            if ranges.is_empty() && role != DatasetRole::Holdout {
                return Err(format!("{role}: at least one window is required"));
            }
            for range in ranges {
                let (start, end) = range.bounds().map_err(|e| format!("{role}: {e}"))?;
                if start.rem_euclid(DAY_MICROS) != 0 || end.rem_euclid(DAY_MICROS) != 0 {
                    return Err(format!("{role}: window bounds must be UTC midnights"));
                }
                // Development is listed first, so its windows overlap only one another.
                for (earlier_role, earlier) in &windows {
                    let (a, b) = earlier.bounds()?;
                    if start < b && a < end && role != DatasetRole::Development {
                        return Err(format!("{role}: window overlaps {earlier_role}"));
                    }
                }
                let range = CoverageRange::new(start, end);
                if !windows.iter().any(|(r, w)| *r == role && *w == range) {
                    windows.push((role, range));
                }
            }
        }
        Ok(windows)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const STORAGE: &str = "\n[storage]\nhistorical_data_dir = \"../historical_data\"\npublication_uri = \"gs://example-bucket/historical\"\n";

    #[test]
    fn offline_run_modes_have_one_canonical_form() {
        for name in ["research", "replay"] {
            let source = format!(
                "# comment\nrun_mode = \"{name}\" # trailing\n\nschema_version=1\n{STORAGE}"
            );
            let canonical = format!("schema_version = 1\nrun_mode = \"{name}\"\n{STORAGE}");
            assert_eq!(Config::parse(&source).unwrap().canonical_toml(), canonical);
        }
    }

    #[test]
    fn omitted_live_preserves_canonical_bytes_and_fixed_hash() {
        // The expected hash is reproducible by hashing base commit 0ed8326's canonical bytes.
        let canonical = format!("schema_version = 1\nrun_mode = \"research\"\n{STORAGE}");
        let config = Config::parse(&canonical).unwrap();
        assert!(config.live.is_none());
        assert_eq!(config.canonical_toml(), canonical);
        assert_eq!(
            config.content_hash(),
            "v3:sha256:b294efbf3afc5e2a6fcd779c2cd59b126f48a9bb6dedefeae3d5e7c3d4fe085e"
        );
    }

    #[test]
    fn sources_round_trip_through_the_canonical_form() {
        let source = "schema_version = 1\nrun_mode = \"research\"\n\n[storage]\nhistorical_data_dir = \"/data/historical\"\npublication_uri = \"file:///data/published\"\n\n[[import.sources]]\nkind = \"tick_csv\"\npath = \"ticks.csv\"\nbroker = \"pocket_option\"\nrole = \"development\"\nprovider_symbol = \"AEDCNY_otc\"\nsource_symbol = \"AEDCNY\"\nprice_scale = 6\n\n[[import.sources]]\nkind = \"bar_parquet_collection\"\npath = \"/data/bars\"\nbroker = \"pocket_option\"\nrole = \"evaluation\"\nmanifest = \"collection.json\"\nprovenance = [\"batch.json\"]\n\n[[import.sources]]\nkind = \"tick_parquet_daily\"\npath = \"/data/deriv/ticks\"\nbroker = \"deriv\"\nrole = \"development\"\nprice_scale = 5\ninstruments = [\"AUDUSD\", \"USDJPY\"]\n".to_string();
        let config = Config::parse(&source).unwrap();
        assert_eq!(config.canonical_toml(), source);
        assert!(matches!(
            &config.import.as_ref().unwrap().sources[1],
            Source::BarParquetCollection {
                instruments: None,
                ..
            }
        ));
        assert_eq!(
            config.content_hash(),
            "v3:sha256:c0b29aea4a1cc52b5341e7c7dd887af171ed72aa7a424f95917a6c572d86326e"
        );
        assert_eq!(
            Config::parse(&config.canonical_toml())
                .unwrap()
                .content_hash(),
            config.content_hash()
        );
    }

    #[test]
    fn pocket_settings_history_prefetch_round_trip_and_validation() {
        let source = "kind = \"pocket_option\"\nid = \"pocket_option\"\nendpoint = \"wss://example.invalid\"\norigin = \"https://example.invalid\"\ncredential = \"POCKET_AUTH\"\ncredential_command = [\"auth-helper\"]\naccount_class = \"demo\"\nserver_offset_minutes = 120\n";
        let broker: Broker = toml::from_str(source).unwrap();
        broker.validate(RunMode::Research).unwrap();
        let Broker::PocketOption(settings) = &broker else {
            panic!("expected Pocket settings")
        };
        assert_eq!(settings.history_pages_in_flight, None);
        assert_eq!(toml::to_string(&broker).unwrap(), source);
        for count in [1, 3, 8, u16::MAX] {
            let explicit = format!("{source}history_pages_in_flight = {count}\n");
            let broker: Broker = toml::from_str(&explicit).unwrap();
            broker.validate(RunMode::Research).unwrap();
            let Broker::PocketOption(settings) = &broker else {
                panic!("expected Pocket settings")
            };
            assert_eq!(settings.history_pages_in_flight, Some(count));
            assert_eq!(toml::to_string(&broker).unwrap(), explicit);
        }
        let broker: Broker =
            toml::from_str(&format!("{source}history_pages_in_flight = 0\n")).unwrap();
        assert_eq!(
            broker.validate(RunMode::Research).unwrap_err(),
            "history_pages_in_flight must be positive"
        );
    }

    fn history_config() -> Config {
        Config::parse(&format!(
            "schema_version = 1\nrun_mode = \"research\"\n{STORAGE}
[[instruments]]
broker = \"b\"
provider_symbol = \"S\"
quote_currency = \"USD\"
price_scale = 6
native_granularity = {{ kind = \"tick\" }}
candles = [{{ duration_seconds = 5, offset_seconds = 0 }}]

[[brokers]]
kind = \"deriv\"
id = \"b\"
public_endpoint = \"wss://example.invalid\"
bootstrap_endpoint = \"https://example.invalid\"
app_id = \"test\"

[history]
broker = \"b\"
instruments = [\"S\"]
role = \"development\"
start = \"2025-05-19T11:15:00Z\"
end = \"2025-05-19T11:15:10Z\"
"
        ))
        .unwrap()
    }

    fn history_seed() -> Seed {
        Seed {
            provider_symbol: ProviderSymbol::try_from("S".to_string()).unwrap(),
            manifest: format!("file:///seeds/manifests/{}/ready.json", "1".repeat(64))
                .parse()
                .unwrap(),
            source_identity: "a".repeat(64),
        }
    }

    #[test]
    fn history_options_round_trip_and_defaults_are_omitted() {
        let mut config = history_config();
        let canonical = config.canonical_toml();
        let defaults = canonical.split_once("\n[history]\n").unwrap().1;
        assert_eq!(
            defaults,
            "broker = \"b\"\ninstruments = [\"S\"]\nrole = \"development\"\nstart = \"2025-05-19T11:15:00Z\"\nend = \"2025-05-19T11:15:10Z\"\n"
        );
        let explicit = canonical.replace(
            "[history]\n",
            "[history]\nnative_granularity = { kind = \"tick\" }\nseeds = []\n",
        );
        assert_eq!(
            Config::parse(&explicit).unwrap().canonical_toml(),
            canonical
        );

        let seed = history_seed();
        let history = config.history.as_mut().unwrap();
        history.native_granularity = NativeGranularity::Bar { period_seconds: 5 };
        history.seeds = vec![seed.clone()];
        history.overlap_seconds = Some(5);
        history.max_pages = Some(2);
        history.max_elapsed_seconds = Some(30);
        config.instruments[0].native_granularity = NativeGranularity::Bar { period_seconds: 5 };
        let canonical = config.canonical_toml();
        assert_eq!(
            canonical.split_once("\n[history]\n").unwrap().1,
            format!(
                "{defaults}overlap_seconds = 5\nmax_pages = 2\nmax_elapsed_seconds = 30\n\n[history.native_granularity]\nkind = \"bar\"\nperiod_seconds = 5\n\n[[history.seeds]]\nprovider_symbol = \"S\"\nmanifest = \"{}\"\nsource_identity = \"{}\"\n",
                seed.manifest, seed.source_identity
            )
        );
        assert_eq!(Config::parse(&canonical).unwrap(), config);
    }

    #[test]
    fn history_limits_reject_zero_with_the_field_name() {
        for field in ["max_pages", "max_elapsed_seconds", "overlap_seconds"] {
            let config = history_config();
            let source = config
                .canonical_toml()
                .replace("[history]\n", &format!("[history]\n{field} = 0\n"));
            let config: Config = toml::from_str(&source).unwrap();
            assert_eq!(
                config.validate().unwrap_err(),
                format!("history: {field} must be positive")
            );
        }
    }

    #[test]
    fn history_seeds_reject_unknown_and_duplicate_instruments() {
        let seed = history_seed();
        let mut unknown = seed.clone();
        unknown.provider_symbol = ProviderSymbol::try_from("other".to_string()).unwrap();
        for (seeds, message) in [
            (
                vec![unknown],
                "history: seeds[0].provider_symbol: other is not a history instrument",
            ),
            (
                vec![seed.clone(), seed],
                "history: seeds[1].provider_symbol: S is listed twice",
            ),
        ] {
            let mut config = history_config();
            config.history.as_mut().unwrap().seeds = seeds;
            assert_eq!(config.validate().unwrap_err(), message);
        }
    }

    #[test]
    fn history_seed_source_identity_requires_lowercase_hex64() {
        let mut config = history_config();
        config.history.as_mut().unwrap().seeds = vec![history_seed()];
        config.validate().unwrap();
        for identity in [
            "a".repeat(63),
            "a".repeat(65),
            "A".repeat(64),
            "g".repeat(64),
        ] {
            config.history.as_mut().unwrap().seeds[0].source_identity = identity;
            assert_eq!(
                config.validate().unwrap_err(),
                "history: seeds[0].source_identity: must be sixty-four lowercase hexadecimal digits"
            );
        }
    }

    #[test]
    fn history_bars_require_a_matching_declared_granularity() {
        let mut config = history_config();
        config.history.as_mut().unwrap().native_granularity =
            NativeGranularity::Bar { period_seconds: 5 };
        assert_eq!(
            config.validate().unwrap_err(),
            "history: instruments[0] must name a declared 5-second bar instrument"
        );
        config.instruments[0].native_granularity = NativeGranularity::Bar { period_seconds: 5 };
        config.validate().unwrap();
    }

    #[test]
    fn bar_collection_instruments_reject_empty_duplicate_and_invalid_names() {
        let source = format!(
            "schema_version = 1\nrun_mode = \"research\"\n{STORAGE}\n[[import.sources]]\nkind = \"bar_parquet_collection\"\npath = \"/bars\"\nbroker = \"b\"\nrole = \"development\"\nmanifest = \"collection.json\"\n"
        );
        for (instruments, message) in [
            ("[]", "at least one asset name is required"),
            ("[\"S\", \"S\"]", "S is listed twice"),
            ("[\"\"]", "ProviderSymbol must not be empty"),
            (
                "[\"S\\t\"]",
                "ProviderSymbol `S\\t` contains a control character",
            ),
        ] {
            let config: Config =
                toml::from_str(&format!("{source}instruments = {instruments}\n")).unwrap();
            assert_eq!(
                config.validate().unwrap_err(),
                format!("import.sources[0].instruments: {message}")
            );
        }
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

#[cfg(test)]
mod replay_tests {
    use super::*;

    const HEAD: &str = "schema_version = 1\nrun_mode = \"research\"\n\n[storage]\nhistorical_data_dir = \"h\"\npublication_uri = \"file:///p\"\n";
    const TICK: &str = "file:///p/manifests/1111111111111111111111111111111111111111111111111111111111111111/ready.json";
    const FEATURE: &str = "file:///p/manifests/2222222222222222222222222222222222222222222222222222222222222222/ready.json";
    /// The canonical rendering: scalars, then each list as an array of tables.
    const TABLE: &str = "role = \"development\"\ndecision_start = \"2026-01-05T00:00:00Z\"\ndecision_end = \"2026-01-06T00:00:00Z\"\nreporting_currency = \"unit\"\nreporting_scale = 2\nmax_rate_age_micros = 0\n\n[[replay.inputs]]\ntick_manifest = \"TICK\"\nfeature_manifest = \"FEATURE\"\n\n[[replay.splits]]\nname = \"a\"\nstart = \"2026-01-05T00:00:00Z\"\nend = \"2026-01-05T12:00:00Z\"\n\n[[replay.splits]]\nname = \"b\"\nstart = \"2026-01-05T12:00:00Z\"\nend = \"2026-01-06T00:00:00Z\"\n\n[[replay.accounts]]\nid = \"acct\"\nbroker = \"pocket_option\"\ncurrency = \"unit\"\nscale = 2\ninitial_cash = \"1000\"\n\n[[replay.strategies]]\nid = \"s\"\nplan_identity = \"plan\"\n\n[replay.strategies.base_stream]\nduration_seconds = 30\noffset_seconds = 15\n\n[[replay.strategies.conditions]]\noutput = \"wick_profile\"\ncomparator = \"eq\"\nthreshold = \"clean_body\"\n\n[replay.strategies.conditions.stream]\nduration_seconds = 30\noffset_seconds = 15\n\n[[replay.bindings]]\nid = \"b\"\nstrategy = \"s\"\naccount = \"acct\"\ninstrument = \"pocket_option:AEDCNY_otc\"\ncontract = \"c\"\nrisk_policy = \"p\"\n\n[replay.bindings.envelope]\nmax_purchase_cost = \"1\"\nmax_entry_fee = \"0\"\nmax_win_terminal_fee = \"0\"\nmax_loss_terminal_fee = \"0\"\nmax_tie_terminal_fee = \"0\"\nmin_winning_net_return = \"0.92\"\nsettlement_rule = \"price_at_due_v1\"\n\n[[replay.contracts]]\nid = \"c\"\ndirection = \"sell\"\nduration_micros = 60000000\ncurrency = \"unit\"\nstake = \"1\"\nquoted_cost = \"1\"\nentry_fee = \"0\"\n\n[replay.contracts.win]\ngross_return = \"1.92\"\nterminal_fee = \"0\"\n\n[replay.contracts.loss]\ngross_return = \"0\"\nterminal_fee = \"0\"\n\n[replay.contracts.tie]\ngross_return = \"1\"\nterminal_fee = \"0\"\n\n[replay.contracts.settlement]\nrule = \"price_at_due_v1\"\nmax_settlement_delay_micros = 60000000\nmax_tick_gap_micros = 60000000\n\n[[replay.risk_policies]]\nid = \"p\"\nmax_open_per_strategy = 1\nsame_entry = \"all\"\ndeduplicate_signal_logic = false\nmax_feature_age_micros = 60000000\nmax_quote_age_micros = 0\n";

    fn table(edit: impl Fn(&str) -> String) -> String {
        format!(
            "{HEAD}\n[replay]\n{}",
            edit(&TABLE.replace("TICK", TICK).replace("FEATURE", FEATURE))
        )
    }

    #[test]
    fn replays_round_trip_through_the_canonical_form() {
        let source = table(str::to_string);
        let config = Config::parse(&source).unwrap();
        assert_eq!(config.canonical_toml(), source);
        assert_eq!(Config::parse(&config.canonical_toml()).unwrap(), config);
        let replay = config.replay.as_ref().unwrap();
        assert_eq!(replay.inputs[0].tick_manifest.generation(), "1".repeat(64));
        assert_eq!(replay.bindings[0].contract, "c");
        assert_eq!(
            Config::parse(HEAD).unwrap().content_hash(),
            "v3:sha256:d7be0fdf6fb030fdfaa543417aad386f84bdb7e06ff06a61ae5646ca8e7c1256",
            "a document that omits `replay` keeps the identity the previous checkout gave it"
        );
        let numeric = table(|body| {
            body.replace(
                "output = \"wick_profile\"\ncomparator = \"eq\"\nthreshold = \"clean_body\"",
                "output = \"close_units\"\ncomparator = \"ge\"\nthreshold = 1.5",
            )
        });
        assert!(Config::parse(&numeric).is_ok());
    }

    #[test]
    fn replay_rules_reject_with_the_field_name() {
        let cases = [
            (
                "role = \"development\"",
                "role = \"holdout\"",
                "replay.role",
            ),
            (
                "decision_end = \"2026-01-06T00:00:00Z\"",
                "decision_end = \"2026-01-05T00:00:00Z\"",
                "replay.decision_end",
            ),
            (
                "name = \"b\"\nstart = \"2026-01-05T12:00:00Z\"",
                "name = \"b\"\nstart = \"2026-01-05T11:00:00Z\"",
                "replay.splits[1].name",
            ),
            (
                "end = \"2026-01-06T00:00:00Z\"\n\n[[replay.accounts]]",
                "end = \"2026-01-07T00:00:00Z\"\n\n[[replay.accounts]]",
                "replay.splits[1].start",
            ),
            (
                "initial_cash = \"1000\"",
                "initial_cash = \"-1\"",
                "replay.accounts[0].initial_cash",
            ),
            (
                "initial_cash = \"1000\"",
                "initial_cash = \"0.001\"",
                "replay.accounts[0].initial_cash",
            ),
            (
                "\nscale = 2\n",
                "\nscale = 19\n",
                "replay.accounts[0].scale",
            ),
            (
                "comparator = \"eq\"\nthreshold = \"clean_body\"",
                "comparator = \"lt\"\nthreshold = \"clean_body\"",
                "replay.strategies[0].conditions[0].comparator",
            ),
            (
                "comparator = \"eq\"\nthreshold = \"clean_body\"",
                "comparator = \"eq\"\nthreshold = nan",
                "replay.strategies[0].conditions[0].threshold",
            ),
            (
                "duration_micros = 60000000",
                "duration_micros = 0",
                "replay.contracts[0].duration_micros",
            ),
            (
                "stake = \"1\"",
                "stake = \"0\"",
                "replay.contracts[0].stake",
            ),
            (
                "\nentry_fee = \"0\"",
                "\nentry_fee = \"-0.1\"",
                "replay.contracts[0].entry_fee",
            ),
            (
                "quoted_cost = \"1\"",
                "quoted_cost = \"0.001\"",
                "replay.bindings[0].contract",
            ),
            (
                "max_open_per_strategy = 1",
                "max_open_per_strategy = 0",
                "replay.risk_policies[0].max_open_per_strategy",
            ),
            (
                "max_feature_age_micros = 60000000",
                "max_feature_age_micros = -1",
                "replay.risk_policies[0].max_feature_age_micros",
            ),
            (
                "max_quote_age_micros = 0\n",
                "max_quote_age_micros = 0\npause = { drawdown = \"0\", duration_micros = 1 }\n",
                "replay.risk_policies[0].pause.drawdown",
            ),
            (
                "strategy = \"s\"",
                "strategy = \"t\"",
                "replay.bindings[0].strategy",
            ),
            (
                "instrument = \"pocket_option:AEDCNY_otc\"",
                "instrument = \"deriv:AEDCNY_otc\"",
                "replay.bindings[0].instrument",
            ),
            (
                "instrument = \"pocket_option:AEDCNY_otc\"",
                "instrument = \"AEDCNY_otc\"",
                "replay.bindings[0].instrument",
            ),
            (
                "currency = \"unit\"\nstake",
                "currency = \"other\"\nstake",
                "replay.bindings[0].contract",
            ),
            (
                "reporting_scale = 2",
                "reporting_scale = 19",
                "replay.reporting_scale",
            ),
            (
                "max_rate_age_micros = 0",
                "max_rate_age_micros = -1",
                "replay.max_rate_age_micros",
            ),
            (
                "max_rate_age_micros = 0\n",
                "max_rate_age_micros = 0\nrates = [{ id = \"r\", source_currency = \"unit\", reporting_currency = \"unit\", provider = \"x\", provider_time = \"2026-01-05T00:00:00Z\", available_at = \"2026-01-05T00:00:00Z\", rate = \"1\" }]\n",
                "replay.rates[0].source_currency",
            ),
            (
                "max_quote_age_micros = 0\n",
                "max_quote_age_micros = 0\nretry = true\n",
                "retry",
            ),
        ];
        for (from, to, key) in cases {
            let source = table(|body| body.replacen(from, to, 1));
            assert_ne!(source, table(str::to_string), "{from}: the edit applies");
            let error = match Config::parse(&source) {
                Ok(_) => panic!("{to}: accepted"),
                Err(error) => error.to_string(),
            };
            assert!(error.contains(key), "{to}: {error}");
        }
        let empty = table(|body| {
            body.replacen("plan_identity = \"plan\"\n", "plan_identity = \"plan\"\nconditions = []\n", 1)
                .replacen(
                    "\n[[replay.strategies.conditions]]\noutput = \"wick_profile\"\ncomparator = \"eq\"\nthreshold = \"clean_body\"\n\n[replay.strategies.conditions.stream]\nduration_seconds = 30\noffset_seconds = 15\n",
                    "",
                    1,
                )
        });
        let error = Config::parse(&empty).unwrap_err().to_string();
        assert!(error.contains("replay.strategies[0].conditions"), "{error}");
    }

    #[test]
    fn shared_policies_and_duplicate_deployments_reject() {
        let second_policy = "\n[[replay.risk_policies]]\nid = \"q\"\nmax_open_per_strategy = 1\nmax_open_total = 5\nsame_entry = \"all\"\ndeduplicate_signal_logic = false\nmax_feature_age_micros = 60000000\nmax_quote_age_micros = 0\n";
        let second_binding = |policy: &str, envelope: &str| {
            format!(
                "\n[[replay.bindings]]\nid = \"b2\"\nstrategy = \"s\"\naccount = \"acct\"\ninstrument = \"pocket_option:AEDCNY_otc\"\ncontract = \"c\"\nrisk_policy = \"{policy}\"\nenvelope = {{ max_purchase_cost = \"{envelope}\", max_entry_fee = \"0\", max_win_terminal_fee = \"0\", max_loss_terminal_fee = \"0\", max_tie_terminal_fee = \"0\", min_winning_net_return = \"0.92\", settlement_rule = \"price_at_due_v1\" }}\n"
            )
        };
        let conflicting = format!(
            "{}{second_policy}{}",
            table(str::to_string),
            second_binding("q", "2")
        );
        let error = Config::parse(&conflicting).unwrap_err().to_string();
        assert!(
            error.contains("replay.bindings[1].risk_policy") && error.contains("max_open_total"),
            "{error}"
        );
        let duplicate = format!("{}{}", table(str::to_string), second_binding("p", "1"));
        let error = Config::parse(&duplicate).unwrap_err().to_string();
        assert!(error.contains("replay.bindings[1].id"), "{error}");
        let distinct = format!("{}{}", table(str::to_string), second_binding("p", "2"));
        assert!(
            Config::parse(&distinct).is_ok(),
            "another envelope is another deployment strategy"
        );
    }
}

#[cfg(test)]
mod search_tests {
    use super::*;

    const HEAD: &str = "schema_version = 1\nrun_mode = \"research\"\n\n[storage]\nhistorical_data_dir = \"h\"\npublication_uri = \"file:///p\"\n";
    const TABLE: &str = "\n[search]\nscope = \"exhaustive\"\nseed = 7\nchunk_size = 8\nmax_candidates = 100\nmin_conditions = 1\nmax_conditions = 2\nembargo_micros = 3600000000\nbase_stream = { duration_seconds = 20, offset_seconds = 0 }\n\n[search.development]\ndecision_start = \"2026-01-05T00:00:20Z\"\ndecision_end = \"2026-01-05T00:21:40Z\"\ninputs = [{ tick_manifest = \"file:///p/manifests/1111111111111111111111111111111111111111111111111111111111111111/ready.json\", feature_manifest = \"file:///p/manifests/2222222222222222222222222222222222222222222222222222222222222222/ready.json\", outcome_manifest = \"file:///p/manifests/3333333333333333333333333333333333333333333333333333333333333333/ready.json\" }]\n\n[search.evaluation]\ndecision_start = \"2026-01-05T02:00:20Z\"\ndecision_end = \"2026-01-05T02:21:40Z\"\ninputs = [{ tick_manifest = \"file:///p/manifests/4444444444444444444444444444444444444444444444444444444444444444/ready.json\", feature_manifest = \"file:///p/manifests/5555555555555555555555555555555555555555555555555555555555555555/ready.json\" }]\nsplits = [{ name = \"a\", start = \"2026-01-05T02:00:20Z\", end = \"2026-01-05T02:11:00Z\" }, { name = \"b\", start = \"2026-01-05T02:11:00Z\", end = \"2026-01-05T02:21:40Z\" }]\n\n[[search.conditions]]\nstream = { duration_seconds = 20, offset_seconds = 0 }\noutput = \"candle_direction\"\ncomparator = \"eq\"\nthresholds = [\"up\", \"down\"]\n\n[[search.conditions]]\nstream = { duration_seconds = 20, offset_seconds = 0 }\noutput = \"range_bps\"\ncomparator = \"gt\"\nthresholds = [0.08]\n\n[[search.contracts]]\nid = \"buy\"\ndirection = \"buy\"\nduration_micros = 5000000\ncurrency = \"unit\"\nstake = \"1\"\nquoted_cost = \"1\"\nentry_fee = \"0\"\nwin = { gross_return = \"1.80\", terminal_fee = \"0\" }\nloss = { gross_return = \"0\", terminal_fee = \"0\" }\ntie = { gross_return = \"1\", terminal_fee = \"0\" }\nsettlement = { rule = \"price_at_due_v1\", max_settlement_delay_micros = 2000000, max_tick_gap_micros = 2000000 }\n\n[[search.contracts]]\nid = \"sell\"\ndirection = \"sell\"\nduration_micros = 5000000\ncurrency = \"unit\"\nstake = \"1\"\nquoted_cost = \"1\"\nentry_fee = \"0\"\nwin = { gross_return = \"1.80\", terminal_fee = \"0\" }\nloss = { gross_return = \"0\", terminal_fee = \"0\" }\ntie = { gross_return = \"1\", terminal_fee = \"0\" }\nsettlement = { rule = \"price_at_due_v1\", max_settlement_delay_micros = 2000000, max_tick_gap_micros = 2000000 }\n\n[search.account]\nbroker = \"pocket_option\"\ncurrency = \"unit\"\nscale = 2\ninitial_cash = \"1000\"\n\n[search.risk_policy]\nid = \"one\"\nmax_open_per_strategy = 1\nsame_entry = \"all\"\ndeduplicate_signal_logic = false\nmax_feature_age_micros = 60000000\nmax_quote_age_micros = 0\n\n[search.envelope]\nmax_purchase_cost = \"1\"\nmax_entry_fee = \"0\"\nmax_win_terminal_fee = \"0\"\nmax_loss_terminal_fee = \"0\"\nmax_tie_terminal_fee = \"0\"\nmin_winning_net_return = \"0.80\"\nsettlement_rule = \"price_at_due_v1\"\n\n[search.gates]\nmin_settled = 1\nmax_unresolved = 0\nmin_net_profit = \"0\"\n\n[search.stability]\nblock_length = 4\nsimulations = 64\nrolling_horizon = 4\n";

    fn table(edit: impl Fn(&str) -> String) -> String {
        format!("{HEAD}{}", edit(TABLE))
    }

    #[test]
    fn searches_round_trip_through_the_canonical_form() {
        let config = Config::parse(&table(|t| t.to_string())).unwrap();
        let search = config.search.as_ref().unwrap();
        assert_eq!(search.scope, Scope::Exhaustive);
        assert_eq!(search.conditions.len(), 2);
        assert_eq!(
            search
                .evaluation
                .as_ref()
                .unwrap()
                .splits
                .as_ref()
                .unwrap()
                .len(),
            2
        );
        let canonical = config.canonical_toml();
        let reparsed = Config::parse(&canonical).unwrap();
        assert_eq!(reparsed, config);
        assert_eq!(reparsed.canonical_toml(), canonical);
        assert_ne!(
            config.content_hash(),
            Config::parse(HEAD).unwrap().content_hash(),
            "the table changes the identity"
        );
        assert_eq!(
            Config::parse(&table(|t| format!(
                "{}\n[search.screen]\nmax_adjusted_score = 0.5\ntop = 3\n",
                t.replace("scope = \"exhaustive\"", "scope = \"heuristic\"")
            )))
            .unwrap()
            .search
            .unwrap()
            .screen
            .unwrap()
            .top,
            Some(3)
        );
    }

    #[test]
    fn search_rules_reject_with_the_field_name() {
        for (edit, expected) in [
            (
                "scope = \"heuristic\"",
                "search.screen: heuristic scope requires the screen table",
            ),
            ("chunk_size = 0", "search.chunk_size: must be positive"),
            (
                "min_conditions = 3",
                "search.min_conditions: 3 must be at least one and at most max_conditions 2",
            ),
            (
                "max_conditions = 4",
                "search.max_conditions: 4 exceeds the 3 distinct conditions of the menu",
            ),
            (
                "max_candidates = 5",
                "search.max_candidates: the menu enumerates 12 members, above the maximum 5",
            ),
            (
                "embargo_micros = 6000000",
                "search.embargo_micros: 6000000 is shorter than contracts[0]'s duration plus settlement delay 7000000",
            ),
            (
                "max_quote_age_micros = 0",
                "search.risk_policy.max_open_total: a cross-account scope would let members interact; leave it absent",
            ),
            (
                "name = \"a\"",
                "search.evaluation.splits: `none` names the undeclared split",
            ),
            (
                "decision_start = \"2026-01-05T02:00:20Z\"",
                "search.evaluation.decision_start: 2026-01-05T00:30:00Z begins less than the embargo after development.decision_end 2026-01-05T00:21:40Z",
            ),
            (
                "id = \"sell\"\ndirection = \"sell\"",
                "search.contracts[1]: repeats every term of contracts[0]; one hypothesis per contract",
            ),
            (
                "rolling_horizon = 4",
                "search.stability.rolling_horizon: must be positive",
            ),
        ] {
            let edited = match edit {
                "max_quote_age_micros = 0" => {
                    table(|t| t.replace(edit, "max_quote_age_micros = 0\nmax_open_total = 5"))
                }
                "name = \"a\"" => table(|t| t.replace(edit, "name = \"none\"")),
                "decision_start = \"2026-01-05T02:00:20Z\"" => table(|t| {
                    t.replace(edit, "decision_start = \"2026-01-05T00:30:00Z\"")
                        .replace(
                            "name = \"a\", start = \"2026-01-05T02:00:20Z\"",
                            "name = \"a\", start = \"2026-01-05T00:30:00Z\"",
                        )
                }),
                "id = \"sell\"\ndirection = \"sell\"" => {
                    table(|t| t.replace(edit, "id = \"sell\"\ndirection = \"buy\""))
                }
                "rolling_horizon = 4" => table(|t| t.replace(edit, "rolling_horizon = 0")),
                _ => table(|t| {
                    t.replace(
                        match edit {
                            "scope = \"heuristic\"" => "scope = \"exhaustive\"",
                            "chunk_size = 0" => "chunk_size = 8",
                            "min_conditions = 3" => "min_conditions = 1",
                            "max_conditions = 4" => "max_conditions = 2",
                            "max_candidates = 5" => "max_candidates = 100",
                            "embargo_micros = 6000000" => "embargo_micros = 3600000000",
                            other => other,
                        },
                        edit,
                    )
                }),
            };
            let error = Config::parse(&edited).unwrap_err().to_string();
            assert!(error.contains(expected), "{edit}: {error}");
        }
    }

    #[test]
    fn generation_rule_is_typed_and_checked_before_plan_binding() {
        let source = table(|text| {
            text.replace(
            "output = \"candle_direction\"\ncomparator = \"eq\"\nthresholds = [\"up\", \"down\"]",
            "output = \"*\"\ncomparator = \"eq\"",
        )
        });
        let config = Config::parse(&source).unwrap();
        assert!(matches!(
            config.search.as_ref().unwrap().conditions[0],
            SearchCondition::Generate(_)
        ));
        assert_eq!(Config::parse(&config.canonical_toml()).unwrap(), config);
        for (edited, expected) in [
            (
                source.replace("output = \"*\"", "output = \"candle_direction\""),
                "generation rule requires output `*`",
            ),
            (
                source.replace(
                    "output = \"*\"\ncomparator = \"eq\"",
                    "output = \"*\"\ncomparator = \"ne\"",
                ),
                "generation rule requires output `*` and comparator `eq`",
            ),
            (
                source.replace(
                    "output = \"*\"\ncomparator = \"eq\"",
                    "output = \"*\"\ncomparator = \"eq\"\nthresholds = [\"up\"]",
                ),
                "`*` requires a generation rule",
            ),
        ] {
            assert!(
                Config::parse(&edited)
                    .unwrap_err()
                    .to_string()
                    .contains(expected)
            );
        }
    }
}

#[cfg(test)]
mod accelerator_tests {
    use super::*;

    const HEAD: &str = "schema_version = 1\nrun_mode = \"research\"\n\n[storage]\nhistorical_data_dir = \"h\"\npublication_uri = \"file:///p\"\n";

    #[test]
    fn accelerators_round_trip_and_absence_preserves_the_previous_hash() {
        for backend in ["cpu", "cuda"] {
            let source = format!("{HEAD}\n[accelerator]\nbackend = \"{backend}\"\n");
            let config = Config::parse(&source).unwrap();
            assert_eq!(config.canonical_toml(), source);
            assert_eq!(Config::parse(&config.canonical_toml()).unwrap(), config);
        }
        assert_eq!(
            Config::parse(HEAD).unwrap().content_hash(),
            "v3:sha256:d7be0fdf6fb030fdfaa543417aad386f84bdb7e06ff06a61ae5646ca8e7c1256",
            "a document omitting accelerator keeps the previous identity"
        );
        for section in ["backend = \"automatic\"", "backend = \"cpu\"\nordinal = 0"] {
            assert!(Config::parse(&format!("{HEAD}\n[accelerator]\n{section}\n")).is_err());
        }
        let source = format!("{HEAD}\n[accelerator]\nbackend = \"cuda\"\ndevices = [0, 1]\n");
        let config = Config::parse(&source).unwrap();
        assert_eq!(config.accelerator.unwrap().devices, [0, 1]);
        assert!(
            Config::parse(&format!(
                "{HEAD}\n[accelerator]\nbackend = \"cuda\"\ndevices = []\n"
            ))
            .unwrap_err()
            .to_string()
            .contains("accelerator.devices")
        );
    }
}

//! Research: the one-command study over the existing owners' records. The governance
//! declaration and its read permits, the attempt intent and population claims, the frozen
//! research stage, the run record with its qualification descriptor, the holdout grant, the
//! consumption receipt, the certification context, the certification result, and every identity
//! and state among them. Pure records and rules only: no storage, feature build, or engine
//! execution.
//!
//! `docs/contracts.md`, section "Research", is the normative description of every rule here.

use std::collections::{BTreeMap, BTreeSet};

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::config::{
    Alternative, Config, Evaluation, FeatureInstrument, ManifestUri, Outcomes, Portfolio,
    PublicationUri, Replay, Research, ResearchInstrument, ResearchScenario, Search, SearchWindow,
};
use crate::dataset::{Coverage, DatasetRole, ObjectRecord, validate_objects};
use crate::execution::{
    AccountSpec, Cashflow, ContractSemantics, ContractTerms, Decimal, ReplayInput, Settlement,
    SettlementRule,
};
use crate::market::{BrokerId, Currency};
use crate::portfolio::{
    ChoiceKey, FeatureRef, Gates, Objective, Outer, Policy, Projection, Selection,
};

pub const RUN_MANIFEST_KIND: &str = "research_run";
pub const RUN_SCHEMA_VERSION: u32 = 1;
/// The one object of a research run generation: the run record, which is the DeploymentBundle
/// once the state is `awaiting_holdout_authorization`.
pub const RUN_OBJECT_PATH: &str = "research.json";
/// The frozen research stage, published beside the run's ready manifest before any outer claim.
pub const FROZEN_OBJECT_NAME: &str = "frozen.json";
pub const CERTIFICATION_MANIFEST_KIND: &str = "research_certification";
pub const CERTIFICATION_SCHEMA_VERSION: u32 = 1;
pub const CERTIFICATION_OBJECT_PATH: &str = "certification.json";
/// The schema of every governance record: declaration, intent, claim, grant, and receipt.
pub const RECORD_SCHEMA_VERSION: u32 = 1;
/// The only qualification claim version one accepts.
pub const QUALIFICATION_CLAIM_V1: &str = "empirical_policy_qualification_v1";
pub const QUALIFICATION_LOOK: &str = "fixed_horizon_once";
pub const BENCHMARK: &str = "analytic_zero_profit";
pub const EVIDENCE_HORIZON: &str = "last_input_tick_per_instrument";
pub const MARKET_INFERENCE: &str = "unavailable";
pub const INFERENCE_JUSTIFICATION: &str = "no observation model or uncertainty justification is established; support counts, adjusted scores, and synthetic resampling are not market inference";
/// The scenario under the frozen policy's own terms and immediate acceptance.
pub const BASELINE_SCENARIO: &str = "baseline";
/// The immutable historical-baseline projection accepted by the live runtime.
pub const EXECUTION_CONTRACT_V1: &str = "historical_baseline_to_broker_v1";
const DECLARATION_DOMAIN_V1: &[u8] = b"binary-alpha governance declaration v1\n";
const GRANT_DOMAIN_V1: &[u8] = b"binary-alpha holdout grant v1\n";
const RUN_DOMAIN_V1: &[u8] = b"binary-alpha research run v1\n";
const CERTIFICATION_DOMAIN_V1: &[u8] = b"binary-alpha research certification v1\n";

// ----------------------------------------------------------------------------------------------
// Shared record helpers
// ----------------------------------------------------------------------------------------------

/// The exact bytes of a published record: pretty JSON in field order and one trailing line feed.
pub fn to_json<T: Serialize>(value: &T) -> Vec<u8> {
    let mut bytes = serde_json::to_vec_pretty(value).expect("a record serializes");
    bytes.push(b'\n');
    bytes
}

fn parse<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, String> {
    serde_json::from_slice(bytes).map_err(|error| error.to_string())
}

/// The SHA-256 of `bytes` under `domain`.
pub fn digest(domain: &[u8], bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(domain);
    hasher.update(bytes);
    crate::hex(&hasher.finalize())
}

fn lines_id(domain: &[u8], lines: &[&str]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(domain);
    for line in lines {
        hasher.update(line.as_bytes());
        hasher.update(b"\n");
    }
    crate::hex(&hasher.finalize())
}

fn identifier(field: &str, text: &str) -> Result<(), String> {
    if text.is_empty()
        || text
            .bytes()
            .any(|byte| byte.is_ascii_control() || byte == b'/' || byte == b' ')
    {
        return Err(format!(
            "{field}: must be a non-empty identifier without a slash, space, or control character"
        ));
    }
    Ok(())
}

fn schema(field: &str, version: u32) -> Result<(), String> {
    if version != RECORD_SCHEMA_VERSION {
        return Err(format!(
            "{field}: unsupported schema_version {version}, expected {RECORD_SCHEMA_VERSION}"
        ));
    }
    Ok(())
}

fn sorted_unique(field: &str, values: &[String]) -> Result<(), String> {
    if values.is_empty() {
        return Err(format!("{field}: at least one entry is required"));
    }
    for (index, value) in values.iter().enumerate() {
        identifier(&format!("{field}[{index}]"), value)?;
        if index > 0 && values[index - 1] >= *value {
            return Err(format!(
                "{field}[{index}]: `{value}` is not in strictly increasing order"
            ));
        }
    }
    Ok(())
}

fn is_hex64(text: &str) -> bool {
    text.len() == 64
        && text
            .bytes()
            .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
}

// ----------------------------------------------------------------------------------------------
// Governance declaration and read permits
// ----------------------------------------------------------------------------------------------

/// One declared source population: its immutable role, instrument, source provenance and
/// coverage, every dataset generation (alias) that carries it, its complete sorted stable
/// conflict-token set, and its exposure history. Overlapping populations share a token.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Population {
    pub id: String,
    pub role: DatasetRole,
    pub instrument: String,
    pub source: String,
    pub coverage: Coverage,
    pub generations: Vec<String>,
    pub tokens: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub exposure: Vec<Exposure>,
}

/// One recorded prior use of a population by an attempt in a role.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Exposure {
    pub study: String,
    pub attempt: String,
    pub role: DatasetRole,
}

/// The operator-declared non-sensitive governance record: operator provenance, the
/// authoritative store root and namespace of every claim, and every declared population.
/// Research reads it and creates claims beneath it; it never creates or replaces the record.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Declaration {
    pub schema_version: u32,
    pub operator: String,
    pub root: PublicationUri,
    pub namespace: String,
    pub populations: Vec<Population>,
}

impl Declaration {
    /// Parses a declaration and checks what every permit relies on: unique population and
    /// generation identities, sorted non-empty token sets, and no token or exposure that places
    /// one population on both sides of the protected boundary.
    pub fn from_json(bytes: &[u8]) -> Result<Self, String> {
        let declaration: Self = parse(bytes)?;
        schema("schema_version", declaration.schema_version)?;
        identifier("operator", &declaration.operator)?;
        identifier("namespace", &declaration.namespace)?;
        let mut generations: BTreeSet<&str> = BTreeSet::new();
        let mut protected: BTreeMap<&str, &str> = BTreeMap::new();
        let mut ordinary: BTreeMap<&str, &str> = BTreeMap::new();
        for (index, population) in declaration.populations.iter().enumerate() {
            let field = |name: &str| format!("populations[{index}].{name}");
            identifier(&field("id"), &population.id)?;
            if declaration.populations[..index]
                .iter()
                .any(|earlier| earlier.id == population.id)
            {
                return Err(format!(
                    "{}: `{}` is declared twice",
                    field("id"),
                    population.id
                ));
            }
            identifier(&field("instrument"), &population.instrument)?;
            if population.source.is_empty() {
                return Err(format!(
                    "{}: the source provenance is required",
                    field("source")
                ));
            }
            for (name, time) in [
                ("first_event_time", &population.coverage.first_event_time),
                ("last_event_time", &population.coverage.last_event_time),
            ] {
                crate::market::parse_event_time_micros(time)
                    .map_err(|reason| format!("{}.{name}: {reason}", field("coverage")))?;
            }
            if population.generations.is_empty() {
                return Err(format!(
                    "{}: at least one generation is required",
                    field("generations")
                ));
            }
            for generation in &population.generations {
                if !is_hex64(generation) {
                    return Err(format!(
                        "{}: `{generation}` is not a generation identity",
                        field("generations")
                    ));
                }
                if !generations.insert(generation) {
                    return Err(format!(
                        "{}: generation {generation} belongs to another population",
                        field("generations")
                    ));
                }
            }
            sorted_unique(&field("tokens"), &population.tokens)?;
            let side = if population.role == DatasetRole::Holdout {
                &mut protected
            } else {
                &mut ordinary
            };
            for token in &population.tokens {
                side.entry(token).or_insert(&population.id);
            }
            for (position, exposure) in population.exposure.iter().enumerate() {
                let field = format!("{}[{position}]", field("exposure"));
                identifier(&format!("{field}.study"), &exposure.study)?;
                identifier(&format!("{field}.attempt"), &exposure.attempt)?;
                if (exposure.role == DatasetRole::Holdout)
                    != (population.role == DatasetRole::Holdout)
                {
                    return Err(format!(
                        "{field}.role: population `{}` is `{}` but was exposed as `{}`; a token never changes side of the protected boundary",
                        population.id, population.role, exposure.role
                    ));
                }
            }
        }
        if let Some((token, id)) = protected
            .iter()
            .find(|(token, _)| ordinary.contains_key(*token))
        {
            return Err(format!(
                "populations: token `{token}` of holdout population `{id}` is also declared on `{}`; a protected token is never exposed to development or evaluation",
                ordinary[token]
            ));
        }
        Ok(declaration)
    }

    /// The identity every intent, claim, grant, and run binds.
    pub fn identity(&self) -> String {
        digest(DECLARATION_DOMAIN_V1, &to_json(self))
    }

    /// The population a dataset generation belongs to.
    pub fn population(&self, generation: &str) -> Option<&Population> {
        self.populations.iter().find(|population| {
            population
                .generations
                .iter()
                .any(|alias| alias == generation)
        })
    }

    /// The complete token set of the populations these generations belong to; an undeclared
    /// generation is unavailable, never an empty conflict set.
    pub fn tokens<'a>(
        &self,
        generations: impl Iterator<Item = &'a str>,
    ) -> Result<BTreeSet<String>, String> {
        let mut tokens = BTreeSet::new();
        for generation in generations {
            let population = self.population(generation).ok_or_else(|| {
                format!("generation {generation} is not declared by the governance declaration")
            })?;
            tokens.extend(population.tokens.iter().cloned());
        }
        Ok(tokens)
    }

    /// A key beneath the authoritative namespace.
    pub fn key(&self, path: &str) -> String {
        format!("{}/{path}", self.namespace)
    }
}

/// The internal certification context: created only by the application research owner from
/// the exact grant, the consumption receipt it created or resumed, and the run manifest they
/// bind; it alone permits the holdout generations they name.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Certification {
    run: String,
    bundle_sha256: String,
    grant: String,
    receipt: String,
    holdout: BTreeSet<String>,
}

impl Certification {
    /// The context of `run` under `grant` and `receipt`, refused unless every binding agrees:
    /// the grant names this run and its bundle, the receipt names this grant, run, bundle, and
    /// holdout references, and the holdout references are the grant's.
    pub fn authorize(
        run: &RunManifest,
        grant: &Grant,
        receipt: &Receipt,
        receipt_key: &str,
    ) -> Result<Self, String> {
        if grant.research != run.generation || grant.bundle_sha256 != run.bundle_sha256() {
            return Err("the grant does not name this run and its frozen bundle".to_string());
        }
        if receipt.grant != grant.hash
            || receipt.research != run.generation
            || receipt.bundle_sha256 != grant.bundle_sha256
            || receipt.holdout != grant.holdout
        {
            return Err(
                "the receipt does not name this grant, run, bundle, and holdout".to_string(),
            );
        }
        Ok(Self {
            run: run.generation.clone(),
            bundle_sha256: grant.bundle_sha256.clone(),
            grant: grant.hash.clone(),
            receipt: receipt_key.to_string(),
            holdout: grant
                .holdout
                .iter()
                .map(|reference| reference.manifest.generation().to_string())
                .collect(),
        })
    }

    pub fn run(&self) -> &str {
        &self.run
    }

    pub fn bundle_sha256(&self) -> &str {
        &self.bundle_sha256
    }

    pub fn grant(&self) -> &str {
        &self.grant
    }

    pub fn receipt(&self) -> &str {
        &self.receipt
    }

    /// Whether every one of these generations is a holdout generation of this context.
    pub fn covers<'a>(&self, mut generations: impl Iterator<Item = &'a str>) -> bool {
        generations.all(|generation| self.holdout.contains(generation))
    }
}

/// What a reader may open, decided before the target is touched: the declaration that names
/// every permitted population, and the certification context that alone permits holdout.
#[derive(Debug, Clone, Copy)]
pub struct Access<'a> {
    pub declaration: Option<&'a Declaration>,
    pub certification: Option<&'a Certification>,
    /// Manifests this phase has already verified, by URI, with each verifier summary. A phase
    /// that holds the store's writer lock verifies a manifest once however many closures
    /// share it; a phase that must observe fresh state starts an empty memo.
    pub verified: Option<&'a Verified>,
}

/// The memo behind [`Access::verified`].
pub type Verified = std::sync::Mutex<std::collections::BTreeMap<String, String>>;

impl Access<'_> {
    /// An ordinary reader: development and evaluation only, checked on the manifest after it
    /// is read when no declaration names the target first.
    pub const ORDINARY: Access<'static> = Access {
        declaration: None,
        certification: None,
        verified: None,
    };

    /// The permit to open the ready manifest of dataset `generation` as `role` (`None` when the
    /// reader learns the role from the manifest). A declared target must carry the expected
    /// role; a holdout target needs the certification context that names it.
    pub fn permit(self, role: Option<DatasetRole>, generation: &str) -> Result<(), String> {
        let declared = match self.declaration {
            Some(declaration) => {
                let population = declaration.population(generation).ok_or_else(|| {
                    format!("generation {generation} is not declared by the governance declaration")
                })?;
                if let Some(role) = role
                    && population.role != role
                {
                    return Err(format!(
                        "generation {generation} is declared `{}`, not `{role}`",
                        population.role
                    ));
                }
                Some(population.role)
            }
            None => None,
        };
        if declared == Some(DatasetRole::Holdout) || role == Some(DatasetRole::Holdout) {
            return self.protected(std::iter::once(generation));
        }
        Ok(())
    }

    /// The declared role of `generation`, checked for protection, or `None` when no declaration
    /// names it: the pre-read check of a reader that learns the kind from the manifest.
    pub fn lookup(self, generation: &str) -> Result<Option<DatasetRole>, String> {
        match self
            .declaration
            .and_then(|declaration| declaration.population(generation))
        {
            Some(population) => {
                if population.role == DatasetRole::Holdout {
                    self.protected(std::iter::once(generation))?;
                }
                Ok(Some(population.role))
            }
            None => Ok(None),
        }
    }

    /// The permit to open holdout evidence of these generations: only the certification
    /// context that names every one of them.
    pub fn protected<'a>(self, generations: impl Iterator<Item = &'a str>) -> Result<(), String> {
        match self.certification {
            Some(certification) if certification.covers(generations) => Ok(()),
            _ => Err(
                "holdout data is protected; only the matching authorized certification context may open it"
                    .to_string(),
            ),
        }
    }
}

// ----------------------------------------------------------------------------------------------
// Governance records: intent, claims, grant, receipt
// ----------------------------------------------------------------------------------------------

/// One declared population this attempt uses, with the role it uses it in.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct PopulationUse {
    pub id: String,
    pub role: DatasetRole,
    pub tokens: Vec<String>,
}

/// The immutable attempt intent, created before any development computation.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Intent {
    pub schema_version: u32,
    pub study: String,
    pub attempt: String,
    pub config_hash: String,
    pub code_revision: String,
    pub declaration: String,
    pub root: PublicationUri,
    pub namespace: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub predecessors: Vec<String>,
    pub changes: String,
    pub populations: Vec<PopulationUse>,
}

impl Intent {
    pub fn from_json(bytes: &[u8]) -> Result<Self, String> {
        let intent: Self = parse(bytes)?;
        schema("schema_version", intent.schema_version)?;
        Ok(intent)
    }
}

crate::string_enum! {
    /// Which population boundary a claim consumes.
    ClaimKind "claim kind" {
        AssessmentUse => "assessment_use",
        HoldoutUse => "holdout_use",
    }
}

/// One immutable population claim: the token, the attempt and frozen assessment that consume
/// it, the declaration, and the entire token set claimed with it. It carries no result.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Claim {
    pub schema_version: u32,
    pub kind: ClaimKind,
    pub token: String,
    pub study: String,
    pub attempt: String,
    pub research: String,
    pub frozen: String,
    pub declaration: String,
    pub tokens: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub grant: Option<String>,
}

/// One declared holdout tick generation of an instrument: its exact ready-manifest location.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct HoldoutRef {
    pub instrument: String,
    pub manifest: ManifestUri,
}

/// The operator-created one-use authorization for exactly one frozen bundle over exactly the
/// declared holdout generations and their complete protected token set.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Grant {
    pub schema_version: u32,
    pub research: String,
    pub bundle_sha256: String,
    pub holdout: Vec<HoldoutRef>,
    pub declaration: String,
    pub root: PublicationUri,
    pub namespace: String,
    pub tokens: Vec<String>,
    pub operator: String,
    pub reason: String,
    pub created_at: String,
    pub hash: String,
}

impl Grant {
    /// The content hash: the digest of the record with an empty `hash` field.
    pub fn content_hash(&self) -> String {
        let unhashed = Self {
            hash: String::new(),
            ..self.clone()
        };
        digest(GRANT_DOMAIN_V1, &to_json(&unhashed))
    }

    /// Parses a grant and checks its schema and that it carries its own content hash.
    pub fn from_json(bytes: &[u8]) -> Result<Self, String> {
        let grant: Self = parse(bytes)?;
        schema("schema_version", grant.schema_version)?;
        if grant.hash != grant.content_hash() {
            return Err("hash: the grant does not carry its own content hash".to_string());
        }
        Ok(grant)
    }
}

/// The immutable consumption receipt: the grant, bundle, run, and holdout generations it
/// consumed, and the protected claims it references. Only its run may resume from it.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Receipt {
    pub schema_version: u32,
    pub grant: String,
    pub research: String,
    pub bundle_sha256: String,
    pub holdout: Vec<HoldoutRef>,
    pub claims: Vec<String>,
    pub declaration: String,
}

impl Receipt {
    pub fn from_json(bytes: &[u8]) -> Result<Self, String> {
        let receipt: Self = parse(bytes)?;
        schema("schema_version", receipt.schema_version)?;
        Ok(receipt)
    }
}

/// Governance keys beneath the declaration's namespace.
pub fn intent_key(study: &str, attempt: &str) -> String {
    format!("attempts/{study}/{attempt}/intent.json")
}

pub fn claim_key(kind: ClaimKind, token: &str) -> String {
    match kind {
        ClaimKind::AssessmentUse => format!("assessment-use/{token}"),
        ClaimKind::HoldoutUse => format!("holdout-use/{token}"),
    }
}

pub fn grant_key(research: &str) -> String {
    format!("grants/{research}.json")
}

pub fn receipt_key(grant_hash: &str) -> String {
    format!("receipts/{grant_hash}.json")
}

/// The frozen stage object beside a run's ready manifest.
pub fn frozen_key(research: &str) -> String {
    format!("manifests/{research}/{FROZEN_OBJECT_NAME}")
}

// ----------------------------------------------------------------------------------------------
// Configuration validation and lowering into the existing tables
// ----------------------------------------------------------------------------------------------

/// The rules of the `research` table a single field's deserializer cannot see; an error names
/// the field. Every lowered child table is validated by its own owner with the declared source
/// manifests standing in for the generations the run publishes: the feature, outcome, and search
/// settings per instrument; the portfolio settings under the evaluation window and under the
/// holdout window; the qualification gates; and every scenario's exact alternatives.
pub fn validate(research: &Research) -> Result<(), String> {
    identifier("study.study", &research.study.study)?;
    identifier("study.attempt", &research.study.attempt)?;
    if research.study.changes.is_empty() {
        return Err("study.changes: the declared changes are required; `initial attempt` names a first attempt".to_string());
    }
    for (index, predecessor) in research.study.predecessors.iter().enumerate() {
        identifier(&format!("study.predecessors[{index}]"), predecessor)?;
        if *predecessor == research.study.attempt {
            return Err(format!(
                "study.predecessors[{index}]: an attempt is not its own predecessor"
            ));
        }
        if research.study.predecessors[..index].contains(predecessor) {
            return Err(format!(
                "study.predecessors[{index}]: `{predecessor}` is listed twice"
            ));
        }
    }
    if research.instruments.is_empty() {
        return Err("instruments: at least one instrument is required".to_string());
    }
    for (index, instrument) in research.instruments.iter().enumerate() {
        let field = |name: &str| format!("instruments[{index}].{name}");
        identifier(&field("instrument"), &instrument.instrument)?;
        if research.instruments[..index]
            .iter()
            .any(|earlier| earlier.instrument == instrument.instrument)
        {
            return Err(format!(
                "{}: {} is listed twice",
                field("instrument"),
                instrument.instrument
            ));
        }
        let source = &instrument.source_manifest;
        fit_entry(instrument, source, source.clone())
            .validate()
            .map_err(|reason| format!("{}.{reason}", field("features")))?;
        outcomes_table(instrument, source, source)
            .validate()
            .map_err(|reason| format!("{}.{reason}", field("outcomes")))?;
        search_table(
            instrument,
            ReplayInput {
                tick_manifest: source.clone(),
                feature_manifest: source.clone(),
                outcome_manifest: Some(source.clone()),
            },
        )
        .validate()
        .map_err(|reason| format!("{}.{reason}", field("search")))?;
    }
    let count = research.instruments.len();
    let per_instrument = |field: &str, len: usize| {
        if len != count {
            return Err(format!(
                "{field}: {len} entries for {count} instruments; one entry per instrument in instrument order is required"
            ));
        }
        Ok(())
    };
    if research.folds.is_empty() {
        return Err("folds: at least one fold is required".to_string());
    }
    for (index, fold) in research.folds.iter().enumerate() {
        per_instrument(&format!("folds[{index}].inputs"), fold.inputs.len())?;
    }
    per_instrument("refit.fits", research.refit.fits.len())?;
    per_instrument("evaluation.inputs", research.evaluation.inputs.len())?;
    per_instrument("holdout.inputs", research.holdout.inputs.len())?;
    let bindings: BTreeSet<&str> = research
        .portfolio
        .bindings
        .iter()
        .map(|binding| binding.id.as_str())
        .collect();
    for (index, scenario) in research.scenarios.iter().enumerate() {
        let field = |name: &str| format!("scenarios[{index}].{name}");
        identifier(&field("id"), &scenario.id)?;
        if scenario.id == BASELINE_SCENARIO
            || research.scenarios[..index]
                .iter()
                .any(|earlier| earlier.id == scenario.id)
        {
            return Err(format!(
                "{}: `{}` is the baseline or is listed twice",
                field("id"),
                scenario.id
            ));
        }
        if scenario.acceptance_delay_micros < 0 {
            return Err(format!(
                "{}: must be non-negative",
                field("acceptance_delay_micros")
            ));
        }
        let mut covered: BTreeSet<&str> = BTreeSet::new();
        let mut contracts: BTreeMap<&str, &ContractTerms> = BTreeMap::new();
        for (position, alternative) in scenario.alternatives.iter().enumerate() {
            let field = format!("{}[{position}]", field("alternatives"));
            if !bindings.contains(alternative.binding.as_str()) {
                return Err(format!(
                    "{field}.binding: `{}` is not a portfolio binding",
                    alternative.binding
                ));
            }
            if !covered.insert(&alternative.binding) {
                return Err(format!(
                    "{field}.binding: `{}` is listed twice",
                    alternative.binding
                ));
            }
            if let Some(earlier) = contracts.insert(&alternative.contract.id, &alternative.contract)
                && *earlier != alternative.contract
            {
                return Err(format!(
                    "{field}.contract: id `{}` names different terms within one scenario",
                    alternative.contract.id
                ));
            }
        }
        if covered.len() != bindings.len() {
            return Err(format!(
                "{}: every portfolio binding needs exactly one alternative",
                field("alternatives")
            ));
        }
    }
    if research.qualification.claim != QUALIFICATION_CLAIM_V1 {
        return Err(format!(
            "qualification.claim: `{}` is not the supported claim `{QUALIFICATION_CLAIM_V1}`",
            research.qualification.claim
        ));
    }
    let sources: Vec<ManifestUri> = research
        .instruments
        .iter()
        .map(|instrument| instrument.source_manifest.clone())
        .collect();
    let profiles = research
        .instruments
        .iter()
        .flat_map(|instrument| {
            std::iter::once(&instrument.source_manifest)
                .chain(
                    research
                        .folds
                        .iter()
                        .flat_map(|fold| fold.inputs.iter().map(|input| &input.fit_manifest)),
                )
                .chain(research.refit.fits.iter())
        })
        .map(|uri| (uri.generation().to_string(), uri.clone()))
        .collect();
    let base = portfolio_table(research, sources, &profiles, &research.evaluation)?;
    base.validate()
        .map_err(|reason| format!("portfolio.{reason}"))?;
    Portfolio {
        evaluation: Some(research.holdout.clone()),
        ..base.clone()
    }
    .validate()
    .map_err(|reason| format!("holdout.{reason}"))?;
    Portfolio {
        gates: research.qualification.gates.clone(),
        ..base.clone()
    }
    .validate()
    .map_err(|reason| format!("qualification.{reason}"))?;
    for (index, scenario) in research.scenarios.iter().enumerate() {
        let mut table = base.clone();
        for binding in &mut table.bindings {
            let alternative = scenario
                .alternatives
                .iter()
                .find(|alternative| alternative.binding == binding.id)
                .expect("every binding is covered");
            binding.alternatives = vec![Alternative {
                contract: alternative.contract.clone(),
                envelope: alternative.envelope.clone(),
            }];
        }
        table
            .validate()
            .map_err(|reason| format!("scenarios[{index}]: portfolio.{reason}"))?;
    }
    // Each later window's own splits under the execution rules, before any claim is consumed.
    for (name, window) in [
        ("evaluation", &research.evaluation),
        ("holdout", &research.holdout),
    ] {
        let start = crate::market::parse_event_time_micros(&window.decision_start)
            .map_err(|reason| format!("{name}.decision_start: {reason}"))?;
        let end = crate::market::parse_event_time_micros(&window.decision_end)
            .map_err(|reason| format!("{name}.decision_end: {reason}"))?;
        crate::execution::validate_splits(window.splits.as_deref().unwrap_or(&[]), start, end)
            .map_err(|reason| format!("{name}.{reason}"))?;
    }
    Ok(())
}

/// The development fit entry of one instrument on `input` under `profile`: the declared feature
/// settings, a new plan, never a frozen one.
pub fn fit_entry(
    instrument: &ResearchInstrument,
    input: &ManifestUri,
    profile: ManifestUri,
) -> FeatureInstrument {
    let settings = &instrument.features;
    FeatureInstrument {
        role: DatasetRole::Development,
        input_manifest: input.clone(),
        profile_manifest: profile,
        frozen_plan: None,
        streams: settings.streams.clone(),
        outputs: settings.outputs.clone(),
        moving_average_periods: settings.moving_average_periods.clone(),
        rolling_window: settings.rolling_window,
        min_history: settings.min_history,
        structure: settings.structure.clone(),
        price_epsilon: settings.price_epsilon.clone(),
        tick_path_streams: settings.tick_path_streams.clone(),
        encodings: settings.encodings.clone(),
    }
}

/// The development outcome build of one instrument over its family-source generations.
pub fn outcomes_table(
    instrument: &ResearchInstrument,
    tick: &ManifestUri,
    feature: &ManifestUri,
) -> Outcomes {
    let settings = &instrument.outcomes;
    Outcomes {
        role: DatasetRole::Development,
        tick_manifest: tick.clone(),
        feature_manifest: feature.clone(),
        expiry_seconds: settings.expiry_seconds.clone(),
        max_entry_delay_ms: settings.max_entry_delay_ms,
        max_settlement_delay_ms: settings.max_settlement_delay_ms,
        max_tick_gap_ms: settings.max_tick_gap_ms,
        true_jump_max_gap_ms: settings.true_jump_max_gap_ms,
        true_jump_basis_points: settings.true_jump_basis_points.clone(),
        frozen_min_ticks: settings.frozen_min_ticks,
        frozen_min_ms: settings.frozen_min_ms,
    }
}

/// The development-only search of one instrument: its declared settings over one development
/// input and no evaluation window.
pub fn search_table(instrument: &ResearchInstrument, input: ReplayInput) -> Search {
    let settings = &instrument.search;
    Search {
        scope: settings.scope,
        seed: settings.seed,
        chunk_size: settings.chunk_size,
        max_candidates: settings.max_candidates,
        min_conditions: settings.min_conditions,
        max_conditions: settings.max_conditions,
        embargo_micros: settings.embargo_micros,
        base_stream: settings.base_stream,
        development: SearchWindow {
            decision_start: settings.decision_start.clone(),
            decision_end: settings.decision_end.clone(),
            inputs: vec![input],
            splits: None,
        },
        evaluation: None,
        conditions: settings.conditions.clone(),
        contracts: settings.contracts.clone(),
        account: settings.account.clone(),
        risk_policy: settings.risk_policy.clone(),
        envelope: settings.envelope.clone(),
        gates: settings.gates.clone(),
        screen: settings.screen.clone(),
        stability: settings.stability.clone(),
    }
}

/// The portfolio selection: the verified families in instrument order, every fold and the
/// refit lowered from the declared generations under their published profiles, and the given
/// later-role window (the run selects with `evaluation: None`; validation and verification
/// lower each declared window).
pub fn portfolio_table(
    research: &Research,
    families: Vec<ManifestUri>,
    profiles: &BTreeMap<String, ManifestUri>,
    evaluation: &Evaluation,
) -> Result<Portfolio, String> {
    let profile = |uri: &ManifestUri| {
        profiles.get(uri.generation()).cloned().ok_or_else(|| {
            format!(
                "no profile was published for generation {}",
                uri.generation()
            )
        })
    };
    let settings = &research.portfolio;
    let mut folds = Vec::with_capacity(research.folds.len());
    for fold in &research.folds {
        let mut inputs = Vec::with_capacity(fold.inputs.len());
        for (instrument, input) in research.instruments.iter().zip(&fold.inputs) {
            inputs.push(crate::config::FoldInput {
                fit: fit_entry(
                    instrument,
                    &input.fit_manifest,
                    profile(&input.fit_manifest)?,
                ),
                assessment_manifest: input.assessment_manifest.clone(),
            });
        }
        folds.push(crate::config::Fold {
            cutoff: fold.cutoff.clone(),
            decision_start: fold.decision_start.clone(),
            decision_end: fold.decision_end.clone(),
            inputs,
        });
    }
    let mut fits = Vec::with_capacity(research.refit.fits.len());
    for (instrument, fit) in research.instruments.iter().zip(&research.refit.fits) {
        fits.push(fit_entry(instrument, fit, profile(fit)?));
    }
    Ok(Portfolio {
        families,
        max_policies: settings.max_policies,
        embargo_micros: settings.embargo_micros,
        objective: settings.objective,
        gates: settings.gates.clone(),
        accounts: settings.accounts.clone(),
        reporting_currency: settings.reporting_currency.clone(),
        reporting_scale: settings.reporting_scale,
        max_rate_age_micros: settings.max_rate_age_micros,
        rates: settings.rates.clone(),
        members: settings.members.clone(),
        repairs: settings.repairs.clone(),
        bindings: settings.bindings.clone(),
        subsets: settings.subsets.clone(),
        risk_policies: settings.risk_policies.clone(),
        folds,
        refit: crate::config::Refit {
            cutoff: research.refit.cutoff.clone(),
            fits,
        },
        evaluation: Some(evaluation.clone()),
    })
}

/// The frozen policy under one scenario's complete per-binding alternatives: deployment `d{p}`
/// keeps its strategy and account, and takes the contract and envelope declared for the
/// portfolio binding the selected subset deploys at position `p`.
pub fn scenario_policy(
    portfolio: &Portfolio,
    key: &ChoiceKey,
    policy: &Policy,
    scenario: &ResearchScenario,
) -> Result<Policy, String> {
    let mut scenario_policy = policy.clone();
    let mut contracts: Vec<ContractTerms> = Vec::new();
    let deployments = &portfolio.subsets[key.subset].deployments;
    if deployments.len() != policy.bindings.len() {
        return Err(format!(
            "scenario {}: the frozen policy has {} deployments but the selected subset declares {}",
            scenario.id,
            policy.bindings.len(),
            deployments.len()
        ));
    }
    for (deployment, binding) in deployments.iter().zip(&mut scenario_policy.bindings) {
        let id = &portfolio.bindings[deployment.binding].id;
        let alternative = scenario
            .alternatives
            .iter()
            .find(|alternative| alternative.binding == *id)
            .ok_or_else(|| {
                format!(
                    "scenario {}: no alternative for binding `{id}`",
                    scenario.id
                )
            })?;
        binding.contract = alternative.contract.id.clone();
        binding.envelope = alternative.envelope.clone();
        if !contracts
            .iter()
            .any(|contract| contract.id == alternative.contract.id)
        {
            contracts.push(alternative.contract.clone());
        }
    }
    scenario_policy.contracts = contracts;
    Ok(scenario_policy)
}

/// The verified sources one live definition derives from.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct LiveSource {
    pub research: String,
    pub bundle_sha256: String,
    pub frozen: String,
    pub selection: String,
    pub policy: String,
    pub certification: String,
}

/// Broker request templates derived from one frozen baseline, its exact assessed economics,
/// and the refit references the application binds through the existing feature owner.
#[derive(Debug, Clone, PartialEq)]
pub struct LivePolicy {
    pub source: LiveSource,
    pub replay: Replay,
    pub baseline: Vec<ContractTerms>,
    pub refit: Vec<FeatureRef>,
}

/// Projects verified public bundle records into broker templates without changing the frozen
/// policy or opening certification children. Inputs are resolved by the application in frozen
/// instrument order; observation supplies the new half-open decision window.
#[allow(clippy::too_many_arguments)]
pub fn live_policy(
    manifest: &RunManifest,
    run: &Run,
    frozen: &Frozen,
    selection: &Selection,
    certification: &CertificationManifest,
    broker: &BrokerId,
    account: &str,
    observation: (&str, &str),
    inputs: Vec<ReplayInput>,
) -> Result<LivePolicy, String> {
    run.complete_bundle()
        .map_err(|reason| format!("live policy rule 1: {reason}"))?;
    let frozen_identity = digest(b"", &frozen.to_json());
    if run.frozen.as_deref() != Some(frozen_identity.as_str())
        || manifest.state != RunState::AwaitingHoldoutAuthorization.status()
    {
        return Err(
            "live policy rule 1: manifest must be awaiting and the frozen stage must match the run"
                .into(),
        );
    }
    if certification.research != manifest.generation
        || certification.bundle_sha256 != manifest.bundle_sha256()
        || certification.state != "certified"
    {
        return Err("live policy rule 2: certification must be certified and match the research generation and bundle_sha256".into());
    }
    let policy = selection
        .frozen
        .as_ref()
        .ok_or("live policy rule 3: selection carries no frozen policy")?;
    let research = run
        .config
        .research
        .as_ref()
        .ok_or("live policy rule 3: run has no research portfolio")?;
    let portfolio = &research.portfolio;
    if portfolio.accounts.len() != 1 {
        return Err("live policy rule 3: refuse a multiaccount portfolio rather than pruning; exactly one account is required".into());
    }
    let assessed_account = &portfolio.accounts[0];
    if assessed_account.id != account || &assessed_account.broker != broker {
        return Err(
            "live policy rule 3: account and broker must match the frozen portfolio account".into(),
        );
    }
    if policy
        .bindings
        .iter()
        .any(|binding| binding.account != account)
    {
        return Err(
            "live policy rule 3: every frozen binding must reference the one account".into(),
        );
    }
    for risk in &policy.risk_policies {
        if risk.max_proposal_age_micros.is_none() {
            return Err(format!(
                "live policy rule 4: risk policy `{}` requires a predeclared max_proposal_age_micros",
                risk.id
            ));
        }
    }
    for contract in &policy.contracts {
        if contract.settlement.rule != SettlementRule::PriceAtDueV1
            || [
                contract.loss.gross_return,
                contract.tie.gross_return,
                contract.loss.terminal_fee,
                contract.tie.terminal_fee,
            ]
            .iter()
            .any(|amount| !amount.is_zero())
        {
            return Err(format!(
                "live policy rule 5: contract `{}` requires price_at_due_v1 and zero loss/tie gross_return and terminal_fee for rise_fall_strict_v1",
                contract.id
            ));
        }
    }
    let zero = Cashflow {
        gross_return: Decimal::zero(0),
        terminal_fee: Decimal::zero(0),
    };
    let contracts = policy
        .contracts
        .iter()
        .map(|contract| ContractTerms {
            quoted_cost: contract.stake,
            entry_fee: Decimal::zero(0),
            win: zero,
            loss: zero,
            tie: zero,
            settlement: Settlement {
                rule: SettlementRule::BrokerAuthoritativeV1,
                ..contract.settlement
            },
            semantics: Some(ContractSemantics::RiseFallStrictV1),
            ..contract.clone()
        })
        .collect();
    let mut bindings = policy.bindings.clone();
    for binding in &mut bindings {
        binding.envelope.settlement_rule = SettlementRule::BrokerAuthoritativeV1;
        binding.envelope.semantics = Some(ContractSemantics::RiseFallStrictV1);
    }
    if inputs.len() != frozen.instruments.len() {
        return Err(format!(
            "live policy rule 7: inputs has {} entries for {} frozen instruments; one per instrument is required",
            inputs.len(),
            frozen.instruments.len()
        ));
    }
    let replay = Replay {
        role: DatasetRole::Development,
        decision_start: observation.0.into(),
        decision_end: observation.1.into(),
        inputs,
        splits: None,
        accounts: vec![assessed_account.clone()],
        strategies: policy.strategies.clone(),
        bindings,
        contracts,
        risk_policies: policy.risk_policies.clone(),
        reporting_currency: frozen.descriptor.reporting_currency.clone(),
        reporting_scale: frozen.descriptor.reporting_scale,
        max_rate_age_micros: portfolio.max_rate_age_micros,
        rates: portfolio.rates.clone(),
        scenario: None,
    };
    replay
        .validate()
        .map_err(|reason| format!("live policy rule 7: {reason}"))?;
    Ok(LivePolicy {
        source: LiveSource {
            research: manifest.generation.clone(),
            bundle_sha256: manifest.bundle_sha256().into(),
            frozen: frozen_identity,
            selection: run.selection.clone(),
            policy: policy.identity(),
            certification: certification.generation.clone(),
        },
        replay,
        baseline: policy.contracts.clone(),
        refit: selection.refit.clone(),
    })
}

// ----------------------------------------------------------------------------------------------
// Qualification
// ----------------------------------------------------------------------------------------------

/// One decision window.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct Window {
    pub decision_start: String,
    pub decision_end: String,
}

/// The frozen qualification descriptor: what passing means for this one jointly executed policy
/// on the named historical populations. It claims no expected future profit, market error
/// control, posterior probability, or execution proof.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct Descriptor {
    pub claim: String,
    pub look: String,
    pub benchmark: String,
    pub objective: Objective,
    pub gates: Gates,
    pub reporting_currency: Currency,
    pub reporting_scale: u8,
    pub accounts: Vec<AccountSpec>,
    pub evaluation: Window,
    pub holdout: Window,
    pub horizon: String,
    pub scenarios: Vec<String>,
    pub market_inference: String,
    pub inference_justification: String,
}

/// The descriptor of a research configuration, frozen before outer access.
pub fn descriptor(research: &Research) -> Descriptor {
    Descriptor {
        claim: research.qualification.claim.clone(),
        look: QUALIFICATION_LOOK.to_string(),
        benchmark: BENCHMARK.to_string(),
        objective: research.portfolio.objective,
        gates: research.qualification.gates.clone(),
        reporting_currency: research.portfolio.reporting_currency.clone(),
        reporting_scale: research.portfolio.reporting_scale,
        accounts: research.portfolio.accounts.clone(),
        evaluation: Window {
            decision_start: research.evaluation.decision_start.clone(),
            decision_end: research.evaluation.decision_end.clone(),
        },
        holdout: Window {
            decision_start: research.holdout.decision_start.clone(),
            decision_end: research.holdout.decision_end.clone(),
        },
        horizon: EVIDENCE_HORIZON.to_string(),
        scenarios: std::iter::once(BASELINE_SCENARIO.to_string())
            .chain(
                research
                    .scenarios
                    .iter()
                    .map(|scenario| scenario.id.clone()),
            )
            .collect(),
        market_inference: MARKET_INFERENCE.to_string(),
        inference_justification: INFERENCE_JUSTIFICATION.to_string(),
    }
}

/// The qualification of one scenario or of the whole finite set: passing, an observed economic
/// failure, or insufficient evidence. The last two are distinct non-passing results.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(tag = "outcome", rename_all = "snake_case")]
pub enum Verdict {
    Pass,
    EconomicFailure { reason: String },
    InsufficientEvidence { reason: String },
}

impl Verdict {
    pub fn passing(&self) -> bool {
        matches!(self, Self::Pass)
    }

    /// The non-passing reason class.
    pub fn reason(&self) -> Option<&'static str> {
        match self {
            Self::Pass => None,
            Self::EconomicFailure { .. } => Some("economic_failure"),
            Self::InsufficientEvidence { .. } => Some("insufficient_evidence"),
        }
    }
}

/// Qualifies one restored-engine projection made under the frozen gates: missing support,
/// an unavailable conversion, or an unavailable drawdown observation is insufficient evidence;
/// any remaining gate failure is the projection's own economic failure against the analytic
/// zero-profit benchmark on the same capital; otherwise the scenario passes.
pub fn verdict(projection: &Projection, gates: &Gates) -> Verdict {
    let insufficient = |reason: String| Verdict::InsufficientEvidence { reason };
    if projection.settled < gates.min_settled {
        return insufficient(format!(
            "settled {} below the minimum {}",
            projection.settled, gates.min_settled
        ));
    }
    if projection.unresolved > gates.max_unresolved {
        return insufficient(format!(
            "unresolved {} above the maximum {}",
            projection.unresolved, gates.max_unresolved
        ));
    }
    if projection.profit.is_none() {
        return insufficient(
            projection
                .failure
                .clone()
                .unwrap_or_else(|| "the completed profit is unavailable".to_string()),
        );
    }
    if projection.drawdown.is_none() || projection.unavailable_observations > 0 {
        return insufficient(format!(
            "drawdown is unavailable: {} reporting observations were unavailable",
            projection.unavailable_observations
        ));
    }
    match &projection.failure {
        Some(reason) => Verdict::EconomicFailure {
            reason: reason.clone(),
        },
        None => Verdict::Pass,
    }
}

/// The complete frozen scenario set aggregated once: any insufficient scenario makes the
/// evidence insufficient; otherwise any economic failure rejects; otherwise every scenario
/// passed. A failing scenario never removes a scenario or chooses a replacement.
pub fn aggregate(results: &[ScenarioResult]) -> Verdict {
    let reasons = |predicate: fn(&Verdict) -> bool| -> Vec<String> {
        results
            .iter()
            .filter(|result| predicate(&result.verdict))
            .map(|result| match &result.verdict {
                Verdict::EconomicFailure { reason } | Verdict::InsufficientEvidence { reason } => {
                    format!("scenario {}: {reason}", result.scenario)
                }
                Verdict::Pass => unreachable!("filtered"),
            })
            .collect()
    };
    let insufficient = reasons(|verdict| matches!(verdict, Verdict::InsufficientEvidence { .. }));
    if !insufficient.is_empty() {
        return Verdict::InsufficientEvidence {
            reason: insufficient.join("; "),
        };
    }
    let economic = reasons(|verdict| matches!(verdict, Verdict::EconomicFailure { .. }));
    if !economic.is_empty() {
        return Verdict::EconomicFailure {
            reason: economic.join("; "),
        };
    }
    Verdict::Pass
}

/// One scenario's evidence: the applied feature generations, the continuous joint replay, the
/// exact restored-engine projection under the frozen gates and its splits, and the verdict.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct ScenarioResult {
    pub scenario: String,
    pub outer: Outer,
    pub verdict: Verdict,
}

// ----------------------------------------------------------------------------------------------
// The frozen stage, the run record, and its manifest
// ----------------------------------------------------------------------------------------------

/// One instrument's published development chain.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct InstrumentRecord {
    pub instrument: String,
    pub source: String,
    pub profile: String,
    pub feature: String,
    pub outcome: String,
    pub family: String,
}

/// The immutable research stage published before any outer claim or access: the run identity,
/// the intent and declaration, every verified development child, the selection, the frozen
/// scenario definitions, and the qualification descriptor. Its content identity is the frozen
/// assessment every claim binds.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
pub struct Frozen {
    pub research: String,
    pub intent: String,
    pub declaration: String,
    pub instruments: Vec<InstrumentRecord>,
    pub selection: String,
    pub scenarios: Vec<ResearchScenario>,
    pub descriptor: Descriptor,
}

impl Frozen {
    pub fn to_json(&self) -> Vec<u8> {
        to_json(self)
    }

    pub fn from_json(bytes: &[u8]) -> Result<Self, String> {
        parse(bytes)
    }
}

/// The state of a research run.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum RunState {
    NoFeasiblePolicy,
    RefitInapplicable { reason: String },
    OuterRejected { verdict: Verdict },
    AwaitingHoldoutAuthorization,
}

impl RunState {
    pub fn status(&self) -> &'static str {
        match self {
            Self::NoFeasiblePolicy => "no_feasible_policy",
            Self::RefitInapplicable { .. } => "refit_inapplicable",
            Self::OuterRejected { .. } => "outer_rejected",
            Self::AwaitingHoldoutAuthorization => "awaiting_holdout_authorization",
        }
    }
}

/// The complete research run as published in `research.json`: the resolved configuration, the
/// declaration and intent it ran under, the frozen stage identity, every published child, the
/// selection, the descriptor carried unchanged from the frozen stage, the outer claims and
/// evidence, and the state. In the awaiting state this record is the DeploymentBundle; it
/// references immutable evidence rather than duplicating it.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
pub struct Run {
    pub config: Config,
    pub declaration: String,
    pub intent: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub frozen: Option<String>,
    pub instruments: Vec<InstrumentRecord>,
    pub selection: String,
    pub descriptor: Descriptor,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub claims: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub outer: Vec<ScenarioResult>,
    pub state: RunState,
}

impl Run {
    pub fn to_json(&self) -> Vec<u8> {
        to_json(self)
    }

    pub fn from_json(bytes: &[u8]) -> Result<Self, String> {
        parse(bytes)
    }

    /// The frozen bundle an awaiting run carries: its frozen stage, the version-one claim, and
    /// one passing result per frozen scenario in order. Completeness alone authorizes nothing:
    /// live consumption additionally requires this run's verified `certified` certification
    /// manifest, and the same immutable run stays uncertified or rejected without one.
    pub fn complete_bundle(&self) -> Result<(), String> {
        if self.state != RunState::AwaitingHoldoutAuthorization {
            return Err(format!(
                "state `{}` carries no frozen bundle",
                self.state.status()
            ));
        }
        if self.descriptor.claim != QUALIFICATION_CLAIM_V1 {
            return Err(format!(
                "descriptor claim `{}` is not `{QUALIFICATION_CLAIM_V1}`",
                self.descriptor.claim
            ));
        }
        if self.frozen.is_none() {
            return Err("the bundle records no frozen stage".to_string());
        }
        let recorded: Vec<&str> = self
            .outer
            .iter()
            .map(|result| result.scenario.as_str())
            .collect();
        let frozen: Vec<&str> = self
            .descriptor
            .scenarios
            .iter()
            .map(String::as_str)
            .collect();
        if recorded != frozen || self.outer.iter().any(|result| !result.verdict.passing()) {
            return Err(
                "the bundle does not carry one passing result per frozen scenario".to_string(),
            );
        }
        Ok(())
    }
}

/// The ready manifest of a research run generation.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct RunManifest {
    pub kind: String,
    pub schema_version: u32,
    pub generation: String,
    pub config_hash: String,
    pub code_revision: String,
    pub declaration: String,
    pub selection: String,
    pub state: String,
    pub objects: Vec<ObjectRecord>,
}

impl RunManifest {
    pub fn to_json(&self) -> Vec<u8> {
        to_json(self)
    }

    pub fn from_json(bytes: &[u8]) -> Result<Self, String> {
        let manifest: Self = parse(bytes)?;
        if manifest.kind != RUN_MANIFEST_KIND {
            return Err(format!(
                "kind `{}` is not `{RUN_MANIFEST_KIND}`",
                manifest.kind
            ));
        }
        if manifest.schema_version != RUN_SCHEMA_VERSION {
            return Err(format!(
                "unsupported schema_version {}, expected {RUN_SCHEMA_VERSION}",
                manifest.schema_version
            ));
        }
        if manifest.generation
            != run_generation_id(
                &manifest.config_hash,
                &manifest.code_revision,
                &manifest.declaration,
            )
        {
            return Err(
                "generation is not the identity of its configuration, revision, and declaration"
                    .to_string(),
            );
        }
        validate_objects(&manifest.objects)?;
        if manifest.objects.len() != 1 || manifest.objects[0].path != RUN_OBJECT_PATH {
            return Err(format!(
                "a research run generation publishes exactly `{RUN_OBJECT_PATH}`"
            ));
        }
        Ok(manifest)
    }

    pub fn key(&self) -> String {
        crate::dataset::manifest_key(&self.generation)
    }

    /// The bundle hash a grant binds: the content identity of the run record.
    pub fn bundle_sha256(&self) -> &str {
        &self.objects[0].sha256
    }
}

/// The research run identity: the domain, the configuration hash, the code revision, and the
/// declaration identity, one per line.
pub fn run_generation_id(config_hash: &str, code_revision: &str, declaration: &str) -> String {
    lines_id(RUN_DOMAIN_V1, &[config_hash, code_revision, declaration])
}

// ----------------------------------------------------------------------------------------------
// Certification
// ----------------------------------------------------------------------------------------------

/// The protected certification evidence as published in `certification.json`: every
/// authorization reference, each scenario's holdout evidence, and the verdict with its reason.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct CertificationRecord {
    pub research: String,
    pub bundle_sha256: String,
    pub frozen: String,
    pub grant: String,
    pub receipt: String,
    pub claims: Vec<String>,
    pub holdout: Vec<HoldoutRef>,
    pub scenarios: Vec<ScenarioResult>,
    pub verdict: Verdict,
}

impl CertificationRecord {
    pub fn to_json(&self) -> Vec<u8> {
        to_json(self)
    }

    pub fn from_json(bytes: &[u8]) -> Result<Self, String> {
        parse(bytes)
    }
}

/// The public envelope of a certification generation: the terminal state and every
/// authorization reference, never a holdout observation, metric, or failure reason.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct CertificationManifest {
    pub kind: String,
    pub schema_version: u32,
    pub generation: String,
    pub research: String,
    pub bundle_sha256: String,
    pub grant: String,
    pub receipt: String,
    pub state: String,
    pub objects: Vec<ObjectRecord>,
}

impl CertificationManifest {
    pub fn to_json(&self) -> Vec<u8> {
        to_json(self)
    }

    pub fn from_json(bytes: &[u8]) -> Result<Self, String> {
        let manifest: Self = parse(bytes)?;
        if manifest.kind != CERTIFICATION_MANIFEST_KIND {
            return Err(format!(
                "kind `{}` is not `{CERTIFICATION_MANIFEST_KIND}`",
                manifest.kind
            ));
        }
        if manifest.schema_version != CERTIFICATION_SCHEMA_VERSION {
            return Err(format!(
                "unsupported schema_version {}, expected {CERTIFICATION_SCHEMA_VERSION}",
                manifest.schema_version
            ));
        }
        if manifest.generation != certification_generation_id(&manifest.research, &manifest.grant) {
            return Err("generation is not the identity of its run and grant".to_string());
        }
        if !matches!(manifest.state.as_str(), "certified" | "rejected") {
            return Err(format!(
                "state `{}` is not a certification result",
                manifest.state
            ));
        }
        validate_objects(&manifest.objects)?;
        if manifest.objects.len() != 1 || manifest.objects[0].path != CERTIFICATION_OBJECT_PATH {
            return Err(format!(
                "a certification generation publishes exactly `{CERTIFICATION_OBJECT_PATH}`"
            ));
        }
        Ok(manifest)
    }

    pub fn key(&self) -> String {
        crate::dataset::manifest_key(&self.generation)
    }
}

/// The certification identity: the domain, the research run generation, and the grant hash.
pub fn certification_generation_id(research: &str, grant_hash: &str) -> String {
    lines_id(CERTIFICATION_DOMAIN_V1, &[research, grant_hash])
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dataset::ObjectRole;
    use crate::execution::Decimal;
    use crate::portfolio::ReplayRef;

    fn decimal(text: &str) -> Decimal {
        Decimal::parse(text).unwrap()
    }

    fn generation(byte: u8) -> String {
        format!("{byte:02x}").repeat(32)
    }

    fn declaration_json(holdout_tokens: &str, exposure: &str) -> String {
        let coverage = r#""source":"ledger","coverage":{"first_event_time":"2026-01-05T00:00:00Z","last_event_time":"2026-01-05T01:00:00Z"}"#;
        format!(
            r#"{{"schema_version":1,"operator":"op","root":"file:///governance","namespace":"study","populations":[
{{"id":"dev","role":"development","instrument":"b:s",{coverage},"generations":["{}"],"tokens":["a"]}},
{{"id":"eval","role":"evaluation","instrument":"b:s",{coverage},"generations":["{}"],"tokens":["b"]}},
{{"id":"hold","role":"holdout","instrument":"b:s",{coverage},"generations":["{}","{}"],"tokens":[{holdout_tokens}]{exposure}}}]}}"#,
            generation(1),
            generation(2),
            generation(3),
            generation(4)
        )
    }

    fn manifest_uri(byte: u8) -> ManifestUri {
        format!("file:///store/manifests/{}/ready.json", generation(byte))
            .parse()
            .unwrap()
    }

    struct LiveFixture {
        manifest: RunManifest,
        run: Run,
        frozen: Frozen,
        selection: Selection,
        certification: CertificationManifest,
        inputs: Vec<ReplayInput>,
    }

    impl LiveFixture {
        fn project(&self) -> Result<LivePolicy, String> {
            live_policy(
                &self.manifest,
                &self.run,
                &self.frozen,
                &self.selection,
                &self.certification,
                &"deriv".to_string().try_into().unwrap(),
                "one",
                ("2026-01-06T00:00:00Z", "2026-01-06T01:00:00Z"),
                self.inputs.clone(),
            )
        }

        fn policy(&mut self) -> &mut Policy {
            self.selection.frozen.as_mut().unwrap()
        }
    }

    fn live_fixture() -> LiveFixture {
        use serde_json::json;
        let config = Config::parse("schema_version = 1\nrun_mode = \"research\"\n[storage]\nhistorical_data_dir = \"historical\"\npublication_uri = \"file:///synthetic\"\n").unwrap();
        let account: AccountSpec = serde_json::from_value(json!({
            "id":"one", "broker":"deriv", "currency":"USD", "scale":2, "initial_cash":"100.00"
        }))
        .unwrap();
        let contract: ContractTerms = serde_json::from_value(json!({
            "id":"rise", "direction":"buy", "duration_micros":5_000_000, "currency":"USD",
            "stake":"1.00", "quoted_cost":"1.00", "entry_fee":"0.01",
            "win":{"gross_return":"1.80", "terminal_fee":"0.02"},
            "loss":{"gross_return":"0", "terminal_fee":"0"},
            "tie":{"gross_return":"0", "terminal_fee":"0"},
            "settlement":{"rule":"price_at_due_v1", "max_settlement_delay_micros":1_000_000,
                "max_tick_gap_micros":2_000_000}
        }))
        .unwrap();
        let mut second = contract.clone();
        second.id = "fall".into();
        second.direction = crate::execution::Direction::Sell;
        let envelope = json!({"max_purchase_cost":"1", "max_entry_fee":"0.01",
            "max_win_terminal_fee":"0.02", "max_loss_terminal_fee":"0", "max_tie_terminal_fee":"0",
            "min_winning_net_return":"0.77", "settlement_rule":"price_at_due_v1"});
        let risk = json!({"id":"risk", "max_open_total":1, "same_entry":"all",
            "deduplicate_signal_logic":false, "max_feature_age_micros":60_000_000,
            "max_quote_age_micros":1_000_000, "max_proposal_age_micros":2_000_000});
        let stream = json!({"duration_seconds":5,"offset_seconds":0});
        let strategies: Vec<_> = [("second", "up"), ("first", "down")].into_iter().map(|(id, threshold)| {
            json!({"id":id,"plan_identity":generation(9),"base_stream":stream,
                "conditions":[{"stream":stream,"output":"candle_direction","comparator":"eq","threshold":threshold}]})
        }).collect();
        let policy: Policy = serde_json::from_value(json!({
            "strategies":strategies,
            "bindings":[
                {"id":"z","strategy":"second","account":"one","instrument":"deriv:R_50","contract":"rise","risk_policy":"risk","envelope":envelope},
                {"id":"a","strategy":"first","account":"one","instrument":"deriv:R_50","contract":"fall","risk_policy":"risk","envelope":envelope}],
            "contracts":[contract,second],"risk_policies":[risk]
        })).unwrap();
        let selection = Selection {
            config: config.clone(),
            families: Vec::new(),
            members: Vec::new(),
            declared: 1,
            rejected: 0,
            valid: 1,
            passing: 1,
            folds: Vec::new(),
            choices: Vec::new(),
            selected: Some(0),
            refit: vec![FeatureRef {
                instrument: "deriv:R_50".into(),
                input_generation: generation(1),
                generation: generation(8),
                plan_identity: generation(9),
            }],
            frozen: Some(policy),
            outer: None,
            state: crate::portfolio::State::Selected,
        };
        let mut config = config;
        // Only the already-verified records consumed by the pure projection are needed here;
        // the application integration fixture owns research construction and verification.
        config.research = Some(serde_json::from_value(json!({
            "study":{"study":"synthetic","attempt":"one","governance_manifest":"file:///synthetic/declaration.json","changes":"initial"},
            "instruments":[],"folds":[],"refit":{"cutoff":"2026-01-05T00:00:00Z","fits":[]},
            "evaluation":{"decision_start":"2026-01-05T00:00:00Z","decision_end":"2026-01-05T01:00:00Z","inputs":[]},
            "holdout":{"decision_start":"2026-01-05T02:00:00Z","decision_end":"2026-01-05T03:00:00Z","inputs":[]},
            "portfolio":{"max_policies":1,"embargo_micros":0,"objective":"profit_then_drawdown","gates":gates(),
                "accounts":[account],"reporting_currency":"USD","reporting_scale":2,"max_rate_age_micros":60_000_000,
                "members":[],"repairs":[],"bindings":[],"subsets":[],"risk_policies":[risk]},
            "qualification":{"claim":QUALIFICATION_CLAIM_V1,"gates":gates()}
        })).unwrap());
        let descriptor = descriptor(config.research.as_ref().unwrap());
        let mut manifest = run_manifest();
        manifest.config_hash = config.content_hash();
        manifest.generation = run_generation_id(
            &manifest.config_hash,
            &manifest.code_revision,
            &manifest.declaration,
        );
        manifest.selection = crate::portfolio::selection_generation_id(
            &selection.config.content_hash(),
            &manifest.code_revision,
            &[],
        );
        let frozen = Frozen {
            research: manifest.generation.clone(),
            intent: "intent".into(),
            declaration: manifest.declaration.clone(),
            instruments: vec![InstrumentRecord {
                instrument: "deriv:R_50".into(),
                source: generation(1),
                profile: generation(2),
                feature: generation(3),
                outcome: generation(4),
                family: generation(5),
            }],
            selection: manifest.selection.clone(),
            scenarios: Vec::new(),
            descriptor: descriptor.clone(),
        };
        let run = Run {
            config,
            declaration: frozen.declaration.clone(),
            intent: frozen.intent.clone(),
            frozen: Some(digest(b"", &frozen.to_json())),
            instruments: frozen.instruments.clone(),
            selection: frozen.selection.clone(),
            descriptor,
            claims: vec!["synthetic-outer-claim".into()],
            outer: vec![ScenarioResult {
                scenario: BASELINE_SCENARIO.into(),
                outer: Outer {
                    features: selection.refit.clone(),
                    replay: ReplayRef {
                        generation: generation(10),
                        summary_identity: generation(11),
                    },
                    projection: projection(2, Some("1"), Some("0"), 0, None),
                    splits: BTreeMap::new(),
                },
                verdict: Verdict::Pass,
            }],
            state: RunState::AwaitingHoldoutAuthorization,
        };
        manifest.objects[0].sha256 = digest(b"", &run.to_json());
        let certification = CertificationManifest {
            kind: CERTIFICATION_MANIFEST_KIND.into(),
            schema_version: CERTIFICATION_SCHEMA_VERSION,
            generation: certification_generation_id(&manifest.generation, "grant"),
            research: manifest.generation.clone(),
            bundle_sha256: manifest.bundle_sha256().into(),
            grant: "grant".into(),
            receipt: "receipt".into(),
            state: "certified".into(),
            objects: Vec::new(),
        };
        LiveFixture {
            manifest,
            run,
            frozen,
            selection,
            certification,
            inputs: vec![ReplayInput {
                tick_manifest: manifest_uri(1),
                feature_manifest: manifest_uri(8),
                outcome_manifest: None,
            }],
        }
    }

    #[test]
    fn live_policy_preserves_the_frozen_baseline_and_derives_broker_templates() {
        let fixture = live_fixture();
        let live = fixture.project().unwrap();
        let policy = fixture.selection.frozen.as_ref().unwrap();
        assert_eq!(live.baseline, policy.contracts);
        assert_eq!(live.replay.strategies, policy.strategies);
        assert_eq!(live.replay.risk_policies, policy.risk_policies);
        assert_eq!(live.replay.accounts, fixture.frozen.descriptor.accounts);
        assert_eq!(
            live.replay.reporting_currency,
            fixture.frozen.descriptor.reporting_currency
        );
        assert_eq!(
            live.replay.reporting_scale,
            fixture.frozen.descriptor.reporting_scale
        );
        let portfolio = &fixture.run.config.research.as_ref().unwrap().portfolio;
        assert_eq!(
            live.replay.max_rate_age_micros,
            portfolio.max_rate_age_micros
        );
        assert_eq!(live.replay.rates, portfolio.rates);
        assert_eq!(live.replay.inputs, fixture.inputs);
        assert_eq!(live.replay.role, DatasetRole::Development);
        assert_eq!(live.replay.decision_start, "2026-01-06T00:00:00Z");
        assert_eq!(live.replay.decision_end, "2026-01-06T01:00:00Z");
        assert_eq!(live.replay.splits, None);
        assert_eq!(live.replay.scenario, None);
        assert_eq!(live.refit, fixture.selection.refit);
        assert_eq!(
            live.replay
                .bindings
                .iter()
                .map(|binding| binding.id.as_str())
                .collect::<Vec<_>>(),
            ["z", "a"]
        );
        for (template, baseline) in live.replay.contracts.iter().zip(&policy.contracts) {
            let zero = Cashflow {
                gross_return: Decimal::zero(0),
                terminal_fee: Decimal::zero(0),
            };
            assert_eq!(
                template,
                &ContractTerms {
                    quoted_cost: baseline.stake,
                    entry_fee: Decimal::zero(0),
                    win: zero,
                    loss: zero,
                    tie: zero,
                    settlement: Settlement {
                        rule: SettlementRule::BrokerAuthoritativeV1,
                        ..baseline.settlement
                    },
                    semantics: Some(ContractSemantics::RiseFallStrictV1),
                    ..baseline.clone()
                }
            );
            assert!(!template.same_economics(baseline).unwrap());
        }
        for (binding, baseline) in live.replay.bindings.iter().zip(&policy.bindings) {
            let mut expected = baseline.clone();
            expected.envelope.settlement_rule = SettlementRule::BrokerAuthoritativeV1;
            expected.envelope.semantics = Some(ContractSemantics::RiseFallStrictV1);
            assert_eq!(binding, &expected);
        }
        assert_eq!(
            live.source,
            LiveSource {
                research: fixture.manifest.generation.clone(),
                bundle_sha256: fixture.manifest.bundle_sha256().into(),
                frozen: fixture.run.frozen.clone().unwrap(),
                selection: fixture.run.selection.clone(),
                policy: policy.identity(),
                certification: fixture.certification.generation.clone(),
            }
        );
    }

    #[test]
    fn live_policy_refuses_not_awaiting_or_mismatched_sources() {
        for field in ["run", "manifest", "frozen"] {
            let mut fixture = live_fixture();
            match field {
                "run" => fixture.run.state = RunState::NoFeasiblePolicy,
                "manifest" => fixture.manifest.state = "no_feasible_policy".into(),
                "frozen" => fixture.run.frozen = Some("wrong".into()),
                _ => unreachable!(),
            }
            let error = fixture.project().unwrap_err();
            assert!(error.starts_with("live policy rule 1:"), "{field}: {error}");
        }
    }

    #[test]
    fn live_policy_refuses_uncertified_or_mismatched_certification() {
        for field in ["state", "research", "bundle"] {
            let mut fixture = live_fixture();
            match field {
                "state" => fixture.certification.state = "rejected".into(),
                "research" => fixture.certification.research = "wrong".into(),
                "bundle" => fixture.certification.bundle_sha256 = "wrong".into(),
                _ => unreachable!(),
            }
            assert!(
                fixture
                    .project()
                    .unwrap_err()
                    .starts_with("live policy rule 2:"),
                "{field}"
            );
        }
    }

    #[test]
    fn live_policy_refuses_multiaccount_portfolios_including_unused_accounts() {
        for used in [false, true] {
            let mut fixture = live_fixture();
            let accounts = &mut fixture
                .run
                .config
                .research
                .as_mut()
                .unwrap()
                .portfolio
                .accounts;
            let mut second = accounts[0].clone();
            second.id = "two".into();
            accounts.push(second);
            if used {
                fixture.policy().bindings[1].account = "two".into();
            }
            assert!(
                fixture
                    .project()
                    .unwrap_err()
                    .contains("rule 3: refuse a multiaccount portfolio")
            );
        }
    }

    #[test]
    fn live_policy_refuses_binding_on_another_account() {
        let mut fixture = live_fixture();
        fixture.policy().bindings[1].account = "two".into();
        assert!(
            fixture
                .project()
                .unwrap_err()
                .contains("rule 3: every frozen binding")
        );
    }

    #[test]
    fn live_policy_refuses_missing_frozen_policy() {
        let mut fixture = live_fixture();
        fixture.selection.frozen = None;
        assert_eq!(
            fixture.project().unwrap_err(),
            "live policy rule 3: selection carries no frozen policy"
        );
    }

    #[test]
    fn live_policy_refuses_wrong_requested_account() {
        let fixture = live_fixture();
        let error = live_policy(
            &fixture.manifest,
            &fixture.run,
            &fixture.frozen,
            &fixture.selection,
            &fixture.certification,
            &"deriv".to_string().try_into().unwrap(),
            "another-account",
            ("2026-01-06T00:00:00Z", "2026-01-06T01:00:00Z"),
            fixture.inputs.clone(),
        )
        .unwrap_err();
        assert_eq!(
            error,
            "live policy rule 3: account and broker must match the frozen portfolio account"
        );
    }

    #[test]
    fn live_policy_refuses_wrong_broker() {
        let mut fixture = live_fixture();
        fixture
            .run
            .config
            .research
            .as_mut()
            .unwrap()
            .portfolio
            .accounts[0]
            .broker = "other".to_string().try_into().unwrap();
        assert!(
            fixture
                .project()
                .unwrap_err()
                .contains("rule 3: account and broker")
        );
    }

    #[test]
    fn live_policy_refuses_missing_proposal_age() {
        let mut fixture = live_fixture();
        fixture.policy().risk_policies[0].max_proposal_age_micros = None;
        let error = fixture.project().unwrap_err();
        assert!(
            error.starts_with("live policy rule 4:") && error.contains("max_proposal_age_micros")
        );
    }

    #[test]
    fn live_policy_refuses_refund_on_tie() {
        let mut fixture = live_fixture();
        fixture.policy().contracts[0].tie.gross_return = decimal("1");
        assert!(
            fixture
                .project()
                .unwrap_err()
                .starts_with("live policy rule 5:")
        );
    }

    #[test]
    fn live_policy_refuses_nonzero_loss_or_tie_fee() {
        for tie in [false, true] {
            let mut fixture = live_fixture();
            let contract = &mut fixture.policy().contracts[0];
            if tie {
                contract.tie.terminal_fee = decimal("0.01");
            } else {
                contract.loss.terminal_fee = decimal("0.01");
            }
            assert!(
                fixture
                    .project()
                    .unwrap_err()
                    .starts_with("live policy rule 5:")
            );
        }
    }

    #[test]
    fn live_policy_refuses_nonzero_loss_return_or_wrong_settlement() {
        for settlement in [false, true] {
            let mut fixture = live_fixture();
            let contract = &mut fixture.policy().contracts[0];
            if settlement {
                contract.settlement.rule = SettlementRule::BrokerAuthoritativeV1;
            } else {
                contract.loss.gross_return = decimal("0.01");
            }
            assert!(
                fixture
                    .project()
                    .unwrap_err()
                    .starts_with("live policy rule 5:")
            );
        }
    }

    #[test]
    fn live_policy_refuses_wrong_input_count() {
        for count in [0, 2] {
            let mut fixture = live_fixture();
            fixture.inputs.resize(count, fixture.inputs[0].clone());
            assert!(
                fixture
                    .project()
                    .unwrap_err()
                    .starts_with("live policy rule 7: inputs")
            );
        }
    }

    fn grant(hash: bool) -> Grant {
        let mut grant = Grant {
            schema_version: 1,
            research: "r".into(),
            bundle_sha256: "b".into(),
            holdout: vec![HoldoutRef {
                instrument: "b:s".into(),
                manifest: manifest_uri(3),
            }],
            declaration: "d".into(),
            root: "file:///governance".parse().unwrap(),
            namespace: "study".into(),
            tokens: vec!["c".into()],
            operator: "op".into(),
            reason: "why".into(),
            created_at: "2026-09-14T00:00:00Z".into(),
            hash: String::new(),
        };
        if hash {
            grant.hash = grant.content_hash();
        }
        grant
    }

    fn run_manifest() -> RunManifest {
        RunManifest {
            kind: RUN_MANIFEST_KIND.into(),
            schema_version: RUN_SCHEMA_VERSION,
            generation: "r".into(),
            config_hash: "c".into(),
            code_revision: "v".into(),
            declaration: "d".into(),
            selection: "s".into(),
            state: "awaiting_holdout_authorization".into(),
            objects: vec![ObjectRecord {
                role: ObjectRole::Normalized,
                path: RUN_OBJECT_PATH.into(),
                key: "objects/b".into(),
                bytes: 1,
                sha256: "b".into(),
                crc32c: None,
                generation: None,
            }],
        }
    }

    fn receipt(grant: &Grant) -> Receipt {
        Receipt {
            schema_version: 1,
            grant: grant.hash.clone(),
            research: "r".into(),
            bundle_sha256: "b".into(),
            holdout: grant.holdout.clone(),
            claims: vec!["study/holdout-use/c".into()],
            declaration: "d".into(),
        }
    }

    #[test]
    fn declarations_map_aliases_and_refuse_cross_role_tokens() {
        let declaration =
            Declaration::from_json(declaration_json("\"c\",\"d\"", "").as_bytes()).unwrap();
        assert_eq!(declaration.population(&generation(4)).unwrap().id, "hold");
        assert_eq!(
            declaration
                .tokens([generation(1), generation(3)].iter().map(String::as_str))
                .unwrap(),
            BTreeSet::from(["a".to_string(), "c".to_string(), "d".to_string()])
        );
        assert!(
            declaration
                .tokens(std::iter::once(generation(9).as_str()))
                .is_err()
        );
        assert_eq!(declaration.key("grants/x"), "study/grants/x");
        let shared = Declaration::from_json(declaration_json("\"a\"", "").as_bytes()).unwrap_err();
        assert!(shared.contains("never exposed to development"), "{shared}");
        let exposed = Declaration::from_json(
            declaration_json(
                "\"c\"",
                r#","exposure":[{"study":"s","attempt":"t","role":"development"}]"#,
            )
            .as_bytes(),
        )
        .unwrap_err();
        assert!(exposed.contains("never changes side"), "{exposed}");
        let unsorted =
            Declaration::from_json(declaration_json("\"d\",\"c\"", "").as_bytes()).unwrap_err();
        assert!(unsorted.contains("strictly increasing"), "{unsorted}");
    }

    #[test]
    fn permits_are_decided_before_any_read() {
        let declaration = Declaration::from_json(declaration_json("\"c\"", "").as_bytes()).unwrap();
        let access = Access {
            declaration: Some(&declaration),
            certification: None,
            verified: None,
        };
        assert!(
            access
                .permit(Some(DatasetRole::Development), &generation(1))
                .is_ok()
        );
        assert_eq!(
            access.lookup(&generation(2)).unwrap(),
            Some(DatasetRole::Evaluation)
        );
        assert!(
            access
                .permit(Some(DatasetRole::Evaluation), &generation(1))
                .unwrap_err()
                .contains("declared `development`")
        );
        assert!(
            access
                .permit(None, &generation(3))
                .unwrap_err()
                .contains("protected")
        );
        assert!(access.lookup(&generation(3)).is_err());
        assert!(
            access
                .permit(None, &generation(7))
                .unwrap_err()
                .contains("not declared")
        );
        assert_eq!(Access::ORDINARY.lookup(&generation(7)).unwrap(), None);
        assert!(Access::ORDINARY.permit(None, &generation(7)).is_ok());
        assert!(
            Access::ORDINARY
                .permit(Some(DatasetRole::Holdout), &generation(3))
                .is_err()
        );
        let grant = grant(true);
        let receipt = receipt(&grant);
        let certification =
            Certification::authorize(&run_manifest(), &grant, &receipt, "study/receipts/x")
                .unwrap();
        let certified = Access {
            declaration: Some(&declaration),
            certification: Some(&certification),
            verified: None,
        };
        assert!(
            certified
                .permit(Some(DatasetRole::Holdout), &generation(3))
                .is_ok()
        );
        assert!(
            certified
                .permit(Some(DatasetRole::Holdout), &generation(4))
                .is_err()
        );
        let mut other = receipt.clone();
        other.research = "other".into();
        assert!(
            Certification::authorize(&run_manifest(), &grant, &other, "k")
                .unwrap_err()
                .contains("receipt")
        );
        let mut foreign = grant.clone();
        foreign.bundle_sha256 = "x".into();
        assert!(
            Certification::authorize(&run_manifest(), &foreign, &receipt, "k")
                .unwrap_err()
                .contains("grant")
        );
    }

    fn projection(
        settled: u64,
        profit: Option<&str>,
        drawdown: Option<&str>,
        unavailable: u64,
        failure: Option<&str>,
    ) -> Projection {
        Projection {
            settled,
            unresolved: 0,
            valued_at: None,
            profit: profit.map(decimal),
            rates: BTreeSet::new(),
            drawdown: drawdown.map(decimal),
            unavailable_observations: unavailable,
            failure: failure.map(str::to_string),
        }
    }

    fn gates() -> Gates {
        Gates {
            min_settled: 2,
            max_unresolved: 0,
            min_profit: decimal("1"),
            max_drawdown: decimal("5"),
        }
    }

    #[test]
    fn verdicts_separate_insufficient_evidence_from_economic_failure() {
        let pass = verdict(&projection(2, Some("1"), Some("5"), 0, None), &gates());
        assert_eq!(pass, Verdict::Pass);
        let support = verdict(
            &projection(1, None, Some("0"), 0, Some("settled 1 below the minimum 2")),
            &gates(),
        );
        assert!(matches!(support, Verdict::InsufficientEvidence { .. }));
        let missing_rate = verdict(
            &projection(
                2,
                None,
                Some("0"),
                0,
                Some("the completed profit of account `e` is unavailable"),
            ),
            &gates(),
        );
        assert!(
            matches!(missing_rate, Verdict::InsufficientEvidence { ref reason } if reason.contains("unavailable"))
        );
        // Missing reporting observations precede a poor profit.
        let unavailable = verdict(
            &projection(
                2,
                Some("-9"),
                Some("0"),
                1,
                Some("net profit -9 below the minimum 1"),
            ),
            &gates(),
        );
        assert!(matches!(unavailable, Verdict::InsufficientEvidence { .. }));
        let loss = verdict(
            &projection(
                2,
                Some("0.99"),
                Some("0"),
                0,
                Some("net profit 0.99 below the minimum 1"),
            ),
            &gates(),
        );
        assert!(
            matches!(loss, Verdict::EconomicFailure { ref reason } if reason.contains("net profit"))
        );
        let result = |scenario: &str, verdict: Verdict| ScenarioResult {
            scenario: scenario.into(),
            outer: Outer {
                features: Vec::new(),
                replay: ReplayRef {
                    generation: String::new(),
                    summary_identity: String::new(),
                },
                projection: projection(2, None, None, 0, None),
                splits: BTreeMap::new(),
            },
            verdict,
        };
        assert_eq!(
            aggregate(&[result("baseline", Verdict::Pass)]),
            Verdict::Pass
        );
        let mixed = aggregate(&[
            result("baseline", Verdict::EconomicFailure { reason: "x".into() }),
            result(
                "delayed",
                Verdict::InsufficientEvidence { reason: "y".into() },
            ),
        ]);
        assert_eq!(
            mixed,
            Verdict::InsufficientEvidence {
                reason: "scenario delayed: y".into()
            }
        );
        assert_eq!(mixed.reason(), Some("insufficient_evidence"));
    }

    #[test]
    fn grants_carry_their_content_hash_and_manifests_bind_identities() {
        let unhashed = grant(false);
        assert!(
            Grant::from_json(&to_json(&unhashed))
                .unwrap_err()
                .contains("content hash")
        );
        let grant = grant(true);
        assert_eq!(Grant::from_json(&to_json(&grant)).unwrap(), grant);
        let manifest = CertificationManifest {
            kind: CERTIFICATION_MANIFEST_KIND.into(),
            schema_version: CERTIFICATION_SCHEMA_VERSION,
            generation: certification_generation_id("r", &grant.hash),
            research: "r".into(),
            bundle_sha256: "b".into(),
            grant: grant.hash.clone(),
            receipt: "k".into(),
            state: "rejected".into(),
            objects: vec![ObjectRecord {
                role: ObjectRole::Normalized,
                path: CERTIFICATION_OBJECT_PATH.into(),
                key: format!("objects/{}", generation(5)),
                bytes: 1,
                sha256: generation(5),
                crc32c: None,
                generation: None,
            }],
        };
        assert_eq!(
            CertificationManifest::from_json(&manifest.to_json()).unwrap(),
            manifest
        );
        let mut wrong = manifest.clone();
        wrong.state = "passed".into();
        assert!(CertificationManifest::from_json(&wrong.to_json()).is_err());
        assert!(RunManifest::from_json(&run_manifest().to_json()).is_err());
        assert_eq!(claim_key(ClaimKind::HoldoutUse, "c"), "holdout-use/c");
        assert_eq!(intent_key("s", "a"), "attempts/s/a/intent.json");
        assert_eq!(frozen_key("r"), "manifests/r/frozen.json");
    }
}

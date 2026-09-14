//! `binary-alpha research run` and `binary-alpha holdout grant create`: the one-command study.
//!
//! The run validates the governance declaration and permits every declared input before any
//! read, publishes its attempt intent, prepares every instrument through the existing audit,
//! feature, outcome, and search owners, selects through the existing portfolio owner with
//! evaluation disabled, publishes the immutable frozen stage, claims the outer populations,
//! assesses the frozen policy under every scenario through the existing frozen-plan application
//! and replay owners, publishes the run record (the DeploymentBundle when it passes), and exits
//! awaiting authorization. The same command resumes: an exact grant, every protected claim, and
//! the consumption receipt create the certification context that alone opens holdout, and one
//! certified or rejected result is published. Every child is the existing owner's generation;
//! this module owns only the fixed sequence, the governance effects, and the verifiers.
//!
//! The engine module `research` owns every record, identity, permit rule, and qualification.

use std::collections::BTreeMap;
use std::fs;
use std::io::Write;
use std::path::Path;
use std::time::Instant;

use binary_alpha_engine::config::{
    Config, Evaluation, ManifestUri, Portfolio, PublicationUri, Research, RunMode, Search,
    Study as StudyTable,
};
use binary_alpha_engine::dataset::{DatasetRole, ObjectRecord, ObjectRole, manifest_key};
use binary_alpha_engine::execution::ReplayInput;
use binary_alpha_engine::market::format_event_time_micros;
use binary_alpha_engine::outcomes::{
    OUTCOME_MANIFEST_KIND, OutcomeManifest, OutcomeRule, outcome_generation_id,
};
use binary_alpha_engine::portfolio::{self as portfolio_engine, Outer, State};
use binary_alpha_engine::research::{
    self as engine, Access, BASELINE_SCENARIO, CERTIFICATION_MANIFEST_KIND,
    CERTIFICATION_OBJECT_PATH, CERTIFICATION_SCHEMA_VERSION, Certification, CertificationManifest,
    CertificationRecord, Claim, ClaimKind, Declaration, Frozen, Grant, HoldoutRef,
    InstrumentRecord, Intent, PopulationUse, RECORD_SCHEMA_VERSION, RUN_MANIFEST_KIND,
    RUN_OBJECT_PATH, RUN_SCHEMA_VERSION, Receipt, Run, RunManifest, RunState, ScenarioResult,
    Verdict, certification_generation_id, run_generation_id,
};
use binary_alpha_engine::stream::{STREAM_MANIFEST_KIND, StreamManifest};

use crate::audit;
use crate::features;
use crate::import::{self, CODE_REVISION};
use crate::outcomes;
use crate::portfolio;
use crate::search;
use crate::store::{self, Put, Store};
use crate::verify;

// ----------------------------------------------------------------------------------------------
// Declarations and object locations
// ----------------------------------------------------------------------------------------------

/// Splits an object location into its store root and key: `gs://BUCKET/KEY` or
/// `file:///DIR/FILE`.
pub(crate) fn open_object(uri: &str) -> Result<(Store, String), String> {
    if let Some(rest) = uri.strip_prefix("gs://") {
        let (bucket, key) = rest
            .split_once('/')
            .filter(|(bucket, key)| !bucket.is_empty() && !key.is_empty())
            .ok_or_else(|| format!("{uri} must name a bucket and an object key"))?;
        let root: PublicationUri = format!("gs://{bucket}").parse()?;
        return Ok((Store::open(&root)?, key.to_string()));
    }
    if let Some(path) = uri.strip_prefix("file://") {
        let path = Path::new(path);
        let (parent, name) = path
            .parent()
            .zip(path.file_name())
            .filter(|(parent, _)| parent.is_absolute())
            .ok_or_else(|| format!("{uri} must be an absolute `file:///DIR/FILE` object"))?;
        return Ok((
            Store::filesystem(parent),
            name.to_string_lossy().into_owned(),
        ));
    }
    Err(format!("{uri} must start with `gs://` or `file:///`"))
}

/// The governance declaration a configuration's `research.study` names, read and validated
/// before any other target is opened; `None` when the configuration declares no study.
pub fn declaration(config: &Config) -> Result<Option<Declaration>, String> {
    let Some(research) = &config.research else {
        return Ok(None);
    };
    let uri = &research.study.governance_manifest;
    let (store, key) = open_object(uri)
        .map_err(|reason| format!("research.study.governance_manifest: {reason}"))?;
    let mut bytes = Vec::new();
    store.read_to(&key, None, &mut bytes)?;
    Declaration::from_json(&bytes)
        .map(Some)
        .map_err(|reason| format!("research.study.governance_manifest: {uri}: {reason}"))
}

/// One record published beneath a store by conditional creation and confirmed by exact
/// readback: an existing identical record is reused, any other content is a conflict that
/// replaces nothing.
pub(crate) fn publish_record(local: &Store, store: &Store, key: &str, bytes: &[u8]) -> Result<Put, String> {
    let name = key.replace('/', "-");
    let temporary = import::temporary_path(local, &name)?;
    fs::write(&temporary, bytes)
        .map_err(|error| format!("cannot write {}: {error}", temporary.display()))?;
    let identity = store::identify(&temporary)?;
    let put = store
        .put_new(key, &temporary, &identity)
        .map_err(|reason| format!("{}: {reason}", store.uri(key)))?;
    fs::remove_file(&temporary)
        .map_err(|error| format!("cannot remove {}: {error}", temporary.display()))?;
    let mut committed = Vec::new();
    store.read_to(key, None, &mut committed)?;
    if committed != bytes {
        return Err(format!(
            "{} holds a different record than this run produced",
            store.uri(key)
        ));
    }
    Ok(put)
}

/// One object of a generation published to the retained folder and the destination.
fn publish_object(
    local: &Store,
    destination: &Store,
    name: &str,
    path: &str,
    bytes: &[u8],
) -> Result<(ObjectRecord, store::ObjectIdentity), String> {
    let temporary = import::temporary_path(local, name)?;
    fs::write(&temporary, bytes)
        .map_err(|error| format!("cannot write {}: {error}", temporary.display()))?;
    let identity = store::identify(&temporary)?;
    let mut object = import::record(ObjectRole::Normalized, path, &identity);
    local.put_new(&object.key, &temporary, &identity)?;
    let put = destination.put_new(&object.key, &temporary, &identity)?;
    object.crc32c = put.object().crc32c;
    object.generation = put.object().generation;
    fs::remove_file(&temporary)
        .map_err(|error| format!("cannot remove {}: {error}", temporary.display()))?;
    Ok((object, identity))
}

/// The ready manifest of a generation: the committed bytes when the destination already holds
/// one that `same` accepts, else `fresh`; verified by `verify` on those exact bytes before
/// anything becomes ready, then published to the destination and mirrored locally.
fn publish_manifest(
    local: &Store,
    destination: &Store,
    key: &str,
    fresh: Vec<u8>,
    same: impl FnOnce(&[u8]) -> Result<bool, String>,
    verify: impl FnOnce(&[u8]) -> Result<String, String>,
) -> Result<(String, Put), String> {
    let committed = match destination.head(key)? {
        Some(_) => {
            let mut bytes = Vec::new();
            destination.read_to(key, None, &mut bytes)?;
            if !same(&bytes)? {
                return Err(format!(
                    "{} records a different result than this run produced",
                    destination.uri(key)
                ));
            }
            bytes
        }
        None => fresh,
    };
    let verified = verify(&committed)?;
    let temporary = import::temporary_path(local, &key.replace('/', "-"))?;
    fs::write(&temporary, &committed)
        .map_err(|error| format!("cannot write {}: {error}", temporary.display()))?;
    let identity = store::identify(&temporary)?;
    let put = destination.put_new(key, &temporary, &identity)?;
    local.put_new(key, &temporary, &identity)?;
    fs::remove_file(&temporary)
        .map_err(|error| format!("cannot remove {}: {error}", temporary.display()))?;
    Ok((verified, put))
}

fn read_key(store: &Store, key: &str) -> Result<Vec<u8>, String> {
    let mut bytes = Vec::new();
    store.read_to(key, None, &mut bytes)?;
    Ok(bytes)
}

fn ready_uri(store: &Store, generation: &str) -> Result<ManifestUri, String> {
    store.uri(&manifest_key(generation)).parse()
}

fn now_text() -> String {
    let micros = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| elapsed.as_micros() as i64)
        .unwrap_or(0);
    format_event_time_micros(micros)
}

// ----------------------------------------------------------------------------------------------
// The study: configuration, declaration, and governance store bound together
// ----------------------------------------------------------------------------------------------

/// The bound study of one run: the configuration, its declaration and identity, the
/// governance store the claims live in, and the run identity.
struct Study<'a> {
    config: &'a Config,
    research: &'a Research,
    declaration: Declaration,
    identity: String,
    governance: Store,
    generation: String,
}

impl Study<'_> {
    fn access(&self) -> Access<'_> {
        Access {
            declaration: Some(&self.declaration),
            certification: None,
        }
    }

    fn key(&self, path: &str) -> String {
        self.declaration.key(path)
    }

    /// The declared holdout references, in instrument order.
    fn holdout(&self) -> Vec<HoldoutRef> {
        holdout_refs(self.research)
    }

    /// The complete sorted protected token set of the declared holdout populations.
    fn protected_tokens(&self) -> Result<Vec<String>, String> {
        protected_tokens(self.research, &self.declaration)
    }

    /// Every declared input in its declared role, checked as a permit before any read.
    fn permit_inputs(&self) -> Result<Vec<PopulationUse>, String> {
        population_uses(self.research, &self.declaration)
    }

    /// Every predecessor attempt's intent exists under this study, root, and namespace, and
    /// every population it used keeps its side of the protected boundary in this declaration:
    /// a changed governance root fails freshness rather than creating new authority, and an
    /// exposed token never becomes protected again.
    fn check_predecessors(&self) -> Result<(), String> {
        let study = &self.research.study;
        for predecessor in &study.predecessors {
            let key = self.key(&engine::intent_key(&study.study, predecessor));
            let bytes = match self.governance.head(&key)? {
                Some(_) => read_key(&self.governance, &key)?,
                None => {
                    return Err(format!(
                        "study.predecessors: attempt `{predecessor}` has no intent at {}",
                        self.governance.uri(&key)
                    ));
                }
            };
            let intent = Intent::from_json(&bytes)
                .map_err(|reason| format!("{}: {reason}", self.governance.uri(&key)))?;
            if intent.study != study.study
                || intent.root != self.declaration.root
                || intent.namespace != self.declaration.namespace
            {
                return Err(format!(
                    "study.predecessors: attempt `{predecessor}` ran under another study, governance root, or namespace; prior claims must be migrated before a fresh assessment"
                ));
            }
            for used in &intent.populations {
                let exposed = used.role != DatasetRole::Holdout;
                if let Some(population) = self.declaration.populations.iter().find(|population| {
                    (population.id == used.id
                        || population
                            .tokens
                            .iter()
                            .any(|token| used.tokens.contains(token)))
                        && (population.role != DatasetRole::Holdout) != exposed
                }) {
                    return Err(format!(
                        "study.predecessors: attempt `{predecessor}` used population `{}` as `{}`, but the declaration now places `{}` on the other side of the protected boundary",
                        used.id, used.role, population.id
                    ));
                }
            }
        }
        Ok(())
    }

    /// Claims every token of `kind` in canonical order by conditional creation and exact
    /// readback, then reads the complete set back once more. A conflict stops before any
    /// later read and leaves every earlier claim in place.
    fn claim(
        &self,
        local: &Store,
        kind: ClaimKind,
        tokens: &[String],
        frozen: &str,
        grant: Option<&str>,
    ) -> Result<Vec<String>, String> {
        let claims = Claims {
            declaration: &self.declaration,
            identity: &self.identity,
            study: &self.research.study,
            kind,
            tokens,
            research: &self.generation,
            frozen,
            grant,
        };
        for ((key, bytes), token) in claims.records().iter().zip(tokens) {
            publish_record(local, &self.governance, key, bytes).map_err(|reason| {
                format!("population token `{token}` is claimed by another assessment: {reason}")
            })?;
        }
        claims.verify(&self.governance)
    }
}

/// Every declared input of a research table in its declared role and instrument, checked as a
/// permit before any read: the development sources, fits, and assessments, the evaluation
/// inputs, and the holdout references (role and instrument only; never opened here). The
/// result is the intent's population record.
fn population_uses(
    research: &Research,
    declaration: &Declaration,
) -> Result<Vec<PopulationUse>, String> {
    let access = Access {
        declaration: Some(declaration),
        certification: None,
    };
    let mut used: BTreeMap<&str, PopulationUse> = BTreeMap::new();
    let mut note = |role: DatasetRole, instrument: &str, uri: &ManifestUri, field: String| {
        let generation = uri.generation();
        let population = declaration.population(generation).ok_or_else(|| {
            format!(
                "{field}: generation {generation} is not declared by the governance declaration"
            )
        })?;
        if population.role != role {
            return Err(format!(
                "{field}: generation {generation} is declared `{}`, not `{role}`",
                population.role
            ));
        }
        if population.instrument != instrument {
            return Err(format!(
                "{field}: generation {generation} is declared for instrument `{}`, not `{instrument}`",
                population.instrument
            ));
        }
        if role != DatasetRole::Holdout {
            access
                .permit(Some(role), generation)
                .map_err(|reason| format!("{field}: {reason}"))?;
        }
        used.entry(population.id.as_str())
            .or_insert_with(|| PopulationUse {
                id: population.id.clone(),
                role,
                tokens: population.tokens.clone(),
            });
        Ok(())
    };
    let instruments = || {
        research
            .instruments
            .iter()
            .map(|instrument| instrument.instrument.as_str())
            .enumerate()
    };
    for (index, instrument) in instruments() {
        note(
            DatasetRole::Development,
            instrument,
            &research.instruments[index].source_manifest,
            format!("instruments[{index}].source_manifest"),
        )?;
    }
    for (index, fold) in research.folds.iter().enumerate() {
        for ((position, instrument), input) in instruments().zip(&fold.inputs) {
            note(
                DatasetRole::Development,
                instrument,
                &input.fit_manifest,
                format!("folds[{index}].inputs[{position}].fit_manifest"),
            )?;
            note(
                DatasetRole::Development,
                instrument,
                &input.assessment_manifest,
                format!("folds[{index}].inputs[{position}].assessment_manifest"),
            )?;
        }
    }
    for ((position, instrument), fit) in instruments().zip(&research.refit.fits) {
        note(
            DatasetRole::Development,
            instrument,
            fit,
            format!("refit.fits[{position}]"),
        )?;
    }
    for ((position, instrument), input) in instruments().zip(&research.evaluation.inputs) {
        note(
            DatasetRole::Evaluation,
            instrument,
            input,
            format!("evaluation.inputs[{position}]"),
        )?;
    }
    for ((position, instrument), input) in instruments().zip(&research.holdout.inputs) {
        note(
            DatasetRole::Holdout,
            instrument,
            input,
            format!("holdout.inputs[{position}]"),
        )?;
    }
    Ok(used.into_values().collect())
}

/// The declared holdout references of a research table, in instrument order.
fn holdout_refs(research: &Research) -> Vec<HoldoutRef> {
    research
        .instruments
        .iter()
        .zip(&research.holdout.inputs)
        .map(|(instrument, manifest)| HoldoutRef {
            instrument: instrument.instrument.clone(),
            manifest: manifest.clone(),
        })
        .collect()
}

/// The complete sorted protected token set of the declared holdout populations.
fn protected_tokens(research: &Research, declaration: &Declaration) -> Result<Vec<String>, String> {
    Ok(declaration
        .tokens(research.holdout.inputs.iter().map(ManifestUri::generation))?
        .into_iter()
        .collect())
}

/// Whether `grant` names exactly this run's frozen bundle, the declared holdout references, and
/// the declaration identity, root, namespace, and complete protected token set the run was
/// frozen under; the same comparison authorizes a run and verifies a certification.
fn grant_binds(
    grant: &Grant,
    manifest: &RunManifest,
    research: &Research,
    declaration: &Declaration,
    identity: &str,
) -> Result<bool, String> {
    Ok(grant.research == manifest.generation
        && grant.bundle_sha256 == manifest.bundle_sha256()
        && grant.holdout == holdout_refs(research)
        && grant.declaration == identity
        && grant.root == declaration.root
        && grant.namespace == declaration.namespace
        && grant.tokens == protected_tokens(research, declaration)?)
}

/// The claims one run creates over a token set, as created and as verified: one record per
/// token in canonical order, every record naming the complete set.
struct Claims<'a> {
    declaration: &'a Declaration,
    identity: &'a str,
    study: &'a StudyTable,
    kind: ClaimKind,
    tokens: &'a [String],
    research: &'a str,
    frozen: &'a str,
    grant: Option<&'a str>,
}

impl Claims<'_> {
    /// Every claim's key and exact bytes, in token order.
    fn records(&self) -> Vec<(String, Vec<u8>)> {
        self.tokens
            .iter()
            .map(|token| {
                let claim = Claim {
                    schema_version: RECORD_SCHEMA_VERSION,
                    kind: self.kind,
                    token: token.clone(),
                    study: self.study.study.clone(),
                    attempt: self.study.attempt.clone(),
                    research: self.research.to_string(),
                    frozen: self.frozen.to_string(),
                    declaration: self.identity.to_string(),
                    tokens: self.tokens.to_vec(),
                    grant: self.grant.map(str::to_string),
                };
                (
                    self.declaration.key(&engine::claim_key(self.kind, token)),
                    engine::to_json(&claim),
                )
            })
            .collect()
    }

    /// Every claim exists beneath `governance` with exactly these bytes; the keys in order.
    fn verify(&self, governance: &Store) -> Result<Vec<String>, String> {
        let mut keys = Vec::with_capacity(self.tokens.len());
        for (key, bytes) in self.records() {
            if read_key(governance, &key)? != bytes {
                return Err(format!(
                    "{} is not this run's claim of the token",
                    governance.uri(&key)
                ));
            }
            keys.push(key);
        }
        Ok(keys)
    }
}

/// Binds the configuration at `config_path` and its declaration into a study.
fn bind(config: &Config) -> Result<Study<'_>, String> {
    if config.run_mode != RunMode::Research {
        return Err(format!(
            "run_mode: research is research, not `{}`",
            config.run_mode
        ));
    }
    let research = config
        .research
        .as_ref()
        .ok_or("research: the table is required")?;
    let declaration = declaration(config)?.expect("a research table names its declaration");
    let identity = declaration.identity();
    let governance = Store::open(&declaration.root)?;
    let generation = run_generation_id(&config.content_hash(), CODE_REVISION, &identity);
    Ok(Study {
        config,
        research,
        declaration,
        identity,
        governance,
        generation,
    })
}

fn stores(config_path: &Path, config: &Config) -> Result<(Store, Store), String> {
    let base = config_path.parent().unwrap_or(Path::new("."));
    let historical_dir = base.join(config.storage.historical_data_dir.as_path());
    fs::create_dir_all(&historical_dir)
        .map_err(|error| format!("cannot create {}: {error}", historical_dir.display()))?;
    Ok((
        Store::filesystem(&historical_dir),
        Store::open(&config.storage.publication_uri)?,
    ))
}

// ----------------------------------------------------------------------------------------------
// The run
// ----------------------------------------------------------------------------------------------

/// Runs the configured study, writing every child report and the run's own lines to `out`.
pub fn run(config_path: &Path, out: &mut dyn Write) -> Result<(), String> {
    let config = crate::load_config(config_path)?;
    let (local, destination) = stores(config_path, &config)?;
    let study = bind(&config).map_err(|reason| format!("research: {reason}"))?;
    let mut report = |line: &str| {
        writeln!(out, "{line}")
            .and_then(|()| out.flush())
            .map_err(|error| format!("cannot write the report: {error}"))
    };
    let (manifest, run) = match existing_run(&study, &destination)? {
        Some(existing) => existing,
        None => {
            let (manifest, run) = develop(&study, &local, &destination, &mut report)
                .map_err(|reason| format!("research: {reason}"))?;
            (manifest, run)
        }
    };
    report(&format!(
        "research generation {} state {} selection {}",
        manifest.generation, manifest.state, run.selection
    ))?;
    if run.state != RunState::AwaitingHoldoutAuthorization {
        return Ok(());
    }
    certify(&study, &local, &destination, &manifest, &run, &mut report)
        .map_err(|reason| format!("research: {reason}"))
}

/// The run manifest and record already published for this identity, if any, checked against
/// this configuration, revision, and declaration.
fn existing_run(
    study: &Study<'_>,
    destination: &Store,
) -> Result<Option<(RunManifest, Run)>, String> {
    let key = manifest_key(&study.generation);
    if destination.head(&key)?.is_none() {
        return Ok(None);
    }
    let uri = destination.uri(&key);
    let bytes = read_key(destination, &key)?;
    let manifest = RunManifest::from_json(&bytes).map_err(|error| format!("{uri}: {error}"))?;
    if manifest.config_hash != study.config.content_hash()
        || manifest.code_revision != CODE_REVISION
        || manifest.declaration != study.identity
    {
        return Err(format!(
            "{uri} does not record this configuration, revision, and declaration"
        ));
    }
    // The complete verifier before anything resumes on this run: a partial or altered bundle
    // never reaches authorization.
    verify_run(&uri, destination, &key, &bytes, study.access())?;
    let run = Run::from_json(&search::read_object(
        destination,
        &manifest.objects,
        RUN_OBJECT_PATH,
    )?)
    .map_err(|error| format!("{uri}: {RUN_OBJECT_PATH}: {error}"))?;
    Ok(Some((manifest, run)))
}

/// The search child configuration: the skeleton, the declared accelerator, and one table.
fn search_config(config: &Config, table: Search) -> Config {
    Config {
        search: Some(table),
        accelerator: config.accelerator.clone(),
        ..crate::skeleton(config)
    }
}

fn portfolio_config(config: &Config, table: Portfolio) -> Config {
    Config {
        portfolio: Some(table),
        ..crate::skeleton(config)
    }
}

/// The lowered selection table of a run: families in instrument order, every fit under its
/// published profile, and no evaluation.
fn selection_table(
    research: &Research,
    families: Vec<ManifestUri>,
    profiles: &BTreeMap<String, ManifestUri>,
) -> Result<Portfolio, String> {
    let mut table = engine::portfolio_table(research, families, profiles, &research.evaluation)?;
    table.evaluation = None;
    Ok(table)
}

/// The profile of one development source generation through the existing audit owner, once
/// per generation.
fn profile(
    config: &Config,
    local: &Store,
    destination: &Store,
    access: Access<'_>,
    profiles: &mut BTreeMap<String, ManifestUri>,
    uri: &ManifestUri,
    report: &mut dyn FnMut(&str) -> Result<(), String>,
) -> Result<ManifestUri, String> {
    if let Some(profile) = profiles.get(uri.generation()) {
        return Ok(profile.clone());
    }
    // The profile's identity carries the instrument table only: the research table names later
    // observations that never enter a development identity.
    let profile_config = Config {
        instruments: config.instruments.clone(),
        ..crate::skeleton(config)
    };
    let audited = audit::audit(
        &profile_config,
        &uri.to_string(),
        local,
        destination,
        access,
    )?;
    report(&audited.report)?;
    let profile = ready_uri(destination, &audited.generation)?;
    profiles.insert(uri.generation().to_string(), profile.clone());
    Ok(profile)
}

/// Wall-clock stages of one run, outside every identity.
#[derive(Default)]
struct Clock {
    bind: f64,
    development: f64,
    selection: f64,
    outer: f64,
    publish: f64,
}

/// Development, selection, the frozen stage, the outer assessment, and the run publication.
fn develop(
    study: &Study<'_>,
    local: &Store,
    destination: &Store,
    report: &mut dyn FnMut(&str) -> Result<(), String>,
) -> Result<(RunManifest, Run), String> {
    let config = study.config;
    let research = study.research;
    let access = study.access();
    let mut clock = Clock::default();
    let started = Instant::now();

    // 1. Every declared input is permitted in its declared role before any read; the
    //    predecessors exist under this governance root; the attempt intent is published.
    let populations = study.permit_inputs()?;
    study.check_predecessors()?;
    let intent = Intent {
        schema_version: RECORD_SCHEMA_VERSION,
        study: research.study.study.clone(),
        attempt: research.study.attempt.clone(),
        config_hash: config.content_hash(),
        code_revision: CODE_REVISION.to_string(),
        declaration: study.identity.clone(),
        root: study.declaration.root.clone(),
        namespace: study.declaration.namespace.clone(),
        predecessors: research.study.predecessors.clone(),
        changes: research.study.changes.clone(),
        populations,
    };
    let intent_key = study.key(&engine::intent_key(
        &research.study.study,
        &research.study.attempt,
    ));
    publish_record(local, &study.governance, &intent_key, &engine::to_json(&intent)).map_err(
        |reason| {
            format!(
                "study.attempt: `{}` already ran with another configuration; declare a new attempt with this one as its predecessor: {reason}",
                research.study.attempt
            )
        },
    )?;
    clock.bind = started.elapsed().as_secs_f64();

    // 2. The frozen stage this run already published, restored through every child's verifier
    //    instead of recomputed; else every instrument's development chain through the existing
    //    owners: the profile of every development source, the family-source features and
    //    outcomes, and the family.
    let developing = Instant::now();
    let descriptor = engine::descriptor(research);
    let frozen_key = engine::frozen_key(&study.generation);
    if destination.head(&frozen_key)?.is_some() {
        let uri = destination.uri(&frozen_key);
        let frozen_bytes = read_key(destination, &frozen_key)?;
        let stage = Frozen::from_json(&frozen_bytes).map_err(|error| format!("{uri}: {error}"))?;
        if stage.research != study.generation
            || stage.intent != intent_key
            || stage.declaration != study.identity
            || stage.scenarios != research.scenarios
            || stage.descriptor != descriptor
        {
            return Err(format!(
                "{uri} does not bind this run's identity, intent, declaration, scenarios, and descriptor"
            ));
        }
        let selection = verified_children(
            &uri,
            destination,
            config,
            &stage.instruments,
            &stage.selection,
            access,
        )?;
        report(&format!(
            "research generation {} frozen stage restored (already published)",
            study.generation
        ))?;
        clock.development = developing.elapsed().as_secs_f64();
        return finish(
            study,
            local,
            destination,
            stage,
            frozen_bytes,
            selection,
            clock,
            report,
        );
    }
    let mut profiles: BTreeMap<String, ManifestUri> = BTreeMap::new();
    let mut instruments = Vec::with_capacity(research.instruments.len());
    let mut families = Vec::with_capacity(research.instruments.len());
    for instrument in &research.instruments {
        let source = &instrument.source_manifest;
        let profile_uri = profile(
            config,
            local,
            destination,
            access,
            &mut profiles,
            source,
            report,
        )?;
        let fit = engine::fit_entry(instrument, source, profile_uri.clone());
        let built = features::build(
            features::resolve(&fit, access)?,
            &portfolio::features_config(config, &fit),
            local,
            destination,
        )?;
        report(&built.report)?;
        let feature = ready_uri(destination, &built.manifest.generation)?;
        let outcomes = engine::outcomes_table(instrument, source, &feature);
        let outcome = outcomes::build(
            &outcomes,
            &Config {
                outcomes: Some(outcomes.clone()),
                ..crate::skeleton(config)
            },
            local,
            destination,
            access,
        )?;
        report(&outcome.report)?;
        let searched = search::family(
            &search_config(
                config,
                engine::search_table(
                    instrument,
                    ReplayInput {
                        tick_manifest: source.clone(),
                        feature_manifest: feature,
                        outcome_manifest: Some(ready_uri(destination, &outcome.generation)?),
                    },
                ),
            ),
            local,
            destination,
            access,
        )?;
        report(&searched.report)?;
        families.push(ready_uri(destination, &searched.generation)?);
        instruments.push(InstrumentRecord {
            instrument: instrument.instrument.clone(),
            source: source.generation().to_string(),
            profile: profile_uri.generation().to_string(),
            feature: built.manifest.generation.clone(),
            outcome: outcome.generation,
            family: searched.generation,
        });
    }
    for fit in research
        .folds
        .iter()
        .flat_map(|fold| fold.inputs.iter().map(|input| &input.fit_manifest))
        .chain(research.refit.fits.iter())
    {
        profile(
            config,
            local,
            destination,
            access,
            &mut profiles,
            fit,
            report,
        )?;
    }
    clock.development = developing.elapsed().as_secs_f64();

    // 3. The existing portfolio owner selects with evaluation disabled.
    let selecting = Instant::now();
    let table = selection_table(research, families, &profiles)?;
    let selected = portfolio::select(&portfolio_config(config, table), local, destination, access)?;
    report(&selected.report)?;
    clock.selection = selecting.elapsed().as_secs_f64();

    // 4. The frozen stage: published before any outer claim or read.
    let publishing = Instant::now();
    let stage = Frozen {
        research: study.generation.clone(),
        intent: intent_key,
        declaration: study.identity.clone(),
        instruments,
        selection: selected.manifest.generation.clone(),
        scenarios: research.scenarios.clone(),
        descriptor,
    };
    let frozen_bytes = stage.to_json();
    publish_record(local, destination, &frozen_key, &frozen_bytes)?;
    clock.publish = publishing.elapsed().as_secs_f64();
    finish(
        study,
        local,
        destination,
        stage,
        frozen_bytes,
        selected.selection,
        clock,
        report,
    )
}

/// The outer assessment and the run publication over one frozen stage: the outer claims, the
/// refit plans applied to the evaluation generations, every scenario replayed once, and the
/// run record and its ready manifest, verified before it becomes ready.
#[allow(clippy::too_many_arguments)]
fn finish(
    study: &Study<'_>,
    local: &Store,
    destination: &Store,
    stage: Frozen,
    frozen_bytes: Vec<u8>,
    selection: portfolio_engine::Selection,
    mut clock: Clock,
    report: &mut dyn FnMut(&str) -> Result<(), String>,
) -> Result<(RunManifest, Run), String> {
    let config = study.config;
    let research = study.research;
    let access = study.access();
    let frozen = engine::digest(b"", &frozen_bytes);
    let descriptor = stage.descriptor;
    let mut run = Run {
        config: config.clone(),
        declaration: study.identity.clone(),
        intent: stage.intent,
        frozen: Some(frozen.clone()),
        instruments: stage.instruments,
        selection: stage.selection,
        descriptor: descriptor.clone(),
        claims: Vec::new(),
        outer: Vec::new(),
        state: match &selection.state {
            State::NoFeasiblePolicy => RunState::NoFeasiblePolicy,
            State::RefitInapplicable { reason } => RunState::RefitInapplicable {
                reason: reason.clone(),
            },
            State::OuterRejected { .. } => {
                return Err("the selection carries an outer result; research selects with evaluation disabled".to_string());
            }
            State::Selected => RunState::AwaitingHoldoutAuthorization,
        },
    };

    // 5. A selected policy: claim the outer populations, apply the refit plans to the
    //    evaluation generations, and replay every scenario once.
    if run.state == RunState::AwaitingHoldoutAuthorization {
        let assessing = Instant::now();
        let tokens: Vec<String> = study
            .declaration
            .tokens(
                research
                    .evaluation
                    .inputs
                    .iter()
                    .map(ManifestUri::generation),
            )?
            .into_iter()
            .collect();
        run.claims = study.claim(local, ClaimKind::AssessmentUse, &tokens, &frozen, None)?;
        let assessment = assess(
            config,
            local,
            destination,
            &selection,
            &research.evaluation,
            DatasetRole::Evaluation,
            &descriptor.gates,
            &research.scenarios,
            access,
            report,
        )?;
        let verdict = engine::aggregate(&assessment);
        run.outer = assessment;
        if !verdict.passing() {
            run.state = RunState::OuterRejected { verdict };
        }
        clock.outer = assessing.elapsed().as_secs_f64();
    }

    // 6. The run record and its ready manifest, verified before it becomes ready.
    let publishing = Instant::now();
    let generation = study.generation.clone();
    let (object, identity) = publish_object(
        local,
        destination,
        &format!("research-{generation}"),
        RUN_OBJECT_PATH,
        &run.to_json(),
    )?;
    let manifest = RunManifest {
        kind: RUN_MANIFEST_KIND.to_string(),
        schema_version: RUN_SCHEMA_VERSION,
        generation: generation.clone(),
        config_hash: config.content_hash(),
        code_revision: CODE_REVISION.to_string(),
        declaration: study.identity.clone(),
        selection: run.selection.clone(),
        state: run.state.status().to_string(),
        objects: vec![object],
    };
    let key = manifest_key(&generation);
    let uri = destination.uri(&key);
    let fresh = manifest.to_json();
    let (verified, put) = publish_manifest(
        local,
        destination,
        &key,
        fresh,
        |bytes| {
            let committed =
                RunManifest::from_json(bytes).map_err(|error| format!("{uri}: {error}"))?;
            Ok(committed.generation == manifest.generation
                && committed.selection == manifest.selection
                && committed.state == manifest.state
                && import::same_objects(
                    &committed.objects,
                    &manifest.objects,
                    std::slice::from_ref(&identity),
                ))
        },
        |bytes| verify_run(&uri, destination, &key, bytes, access),
    )?;
    clock.publish += publishing.elapsed().as_secs_f64();
    let scenarios = run.outer.len();
    let line = match put {
        Put::Reused(_) => {
            format!("research generation {generation} scenarios {scenarios} (already published)")
        }
        Put::Created(_) => format!(
            "research generation {generation} scenarios {scenarios} [bind {:.3}s development {:.3}s selection {:.3}s outer {:.3}s publish {:.3}s] peak_rss_kb {}",
            clock.bind,
            clock.development,
            clock.selection,
            clock.outer,
            clock.publish,
            search::peak_rss_kb()
        ),
    };
    report(&format!("{line}\n{verified}"))?;
    Ok((manifest, run))
}

/// The frozen policy under every scenario over one later-role window: baseline first under the
/// policy's own terms and immediate acceptance, then each declared scenario under its exact
/// alternatives and delay; each applies the refit plans through the feature owner and replays
/// once through the replay owner, projected and qualified under the frozen gates.
#[allow(clippy::too_many_arguments)]
fn assess(
    config: &Config,
    local: &Store,
    destination: &Store,
    selection: &portfolio_engine::Selection,
    window: &Evaluation,
    role: DatasetRole,
    gates: &portfolio_engine::Gates,
    scenarios: &[binary_alpha_engine::config::ResearchScenario],
    access: Access<'_>,
    report: &mut dyn FnMut(&str) -> Result<(), String>,
) -> Result<Vec<ScenarioResult>, String> {
    let settings = selection
        .config
        .portfolio
        .as_ref()
        .ok_or("the selection records no portfolio table")?;
    let policy = selection
        .frozen
        .as_ref()
        .ok_or("the selection records no frozen policy")?;
    let choice = selection
        .selected
        .map(|index| selection.choices[index].key())
        .ok_or("the selection records no selected choice")?;
    let mut features = Vec::with_capacity(window.inputs.len());
    let mut inputs = Vec::with_capacity(window.inputs.len());
    for (input, fit) in window.inputs.iter().zip(&selection.refit) {
        let entry = settings
            .refit
            .fits
            .iter()
            .find(|entry| entry.input_manifest.generation() == fit.input_generation)
            .ok_or_else(|| format!("no refit entry fitted generation {}", fit.generation))?;
        let (applied, replay_input) = portfolio::apply(
            config,
            local,
            destination,
            role,
            input,
            entry,
            &fit.generation,
            access,
        )?;
        features.push(applied);
        inputs.push(replay_input);
    }
    let mut results = Vec::with_capacity(scenarios.len() + 1);
    let mut plans = vec![(BASELINE_SCENARIO.to_string(), policy.clone(), None)];
    for scenario in scenarios {
        plans.push((
            scenario.id.clone(),
            engine::scenario_policy(settings, &choice, policy, scenario)?,
            Some(binary_alpha_engine::config::ReplayScenario {
                schema_version: 1,
                id: scenario.id.clone(),
                acceptance_delay_micros: scenario.acceptance_delay_micros,
            }),
        ));
    }
    for (id, policy, replay_scenario) in plans {
        let (published, projection) = portfolio::replay_policy(
            config,
            local,
            destination,
            settings,
            &policy,
            role,
            (&window.decision_start, &window.decision_end),
            inputs.clone(),
            window.splits.clone(),
            replay_scenario,
            gates,
            access,
        )?;
        if role == DatasetRole::Holdout {
            // Protected support observations stay in the evidence objects the certification
            // context alone opens; the command prints only the generation it created.
            report(&format!(
                "research scenario {id} replay {}",
                published.manifest.generation
            ))?;
        } else {
            report(&published.report)?;
        }
        let verdict = engine::verdict(&projection, gates);
        results.push(ScenarioResult {
            scenario: id,
            outer: Outer {
                features: features.clone(),
                replay: portfolio::replay_ref(&published),
                splits: published.engine.summary().splits.clone(),
                projection,
            },
            verdict,
        });
    }
    Ok(results)
}

// ----------------------------------------------------------------------------------------------
// Certification
// ----------------------------------------------------------------------------------------------

/// The grant this run awaits, validated against the run, the declared holdout references, and
/// the declaration before any claim; `None` leaves the run awaiting authorization.
fn validated_grant(study: &Study<'_>, manifest: &RunManifest) -> Result<Option<Grant>, String> {
    let key = study.key(&engine::grant_key(&study.generation));
    if study.governance.head(&key)?.is_none() {
        return Ok(None);
    }
    let uri = study.governance.uri(&key);
    let grant = Grant::from_json(&read_key(&study.governance, &key)?)
        .map_err(|reason| format!("{uri}: {reason}"))?;
    if !grant_binds(
        &grant,
        manifest,
        study.research,
        &study.declaration,
        &study.identity,
    )? {
        return Err(format!(
            "{uri} does not authorize this run's frozen bundle over the declared holdout population; no holdout was opened"
        ));
    }
    Ok(Some(grant))
}

/// Claims the protected population, creates or resumes the consumption receipt, creates the
/// certification context, applies the frozen plans and scenarios to the holdout generations,
/// and publishes the one certified or rejected result.
fn certify(
    study: &Study<'_>,
    local: &Store,
    destination: &Store,
    manifest: &RunManifest,
    run: &Run,
    report: &mut dyn FnMut(&str) -> Result<(), String>,
) -> Result<(), String> {
    let Some(grant) = validated_grant(study, manifest)? else {
        report(&format!(
            "research generation {} awaiting holdout authorization: no grant at {}",
            study.generation,
            study
                .governance
                .uri(&study.key(&engine::grant_key(&study.generation)))
        ))?;
        return Ok(());
    };
    let certifying = Instant::now();
    let frozen = run
        .frozen
        .as_deref()
        .ok_or("the run records no frozen stage")?;
    let claims = study.claim(
        local,
        ClaimKind::HoldoutUse,
        &grant.tokens,
        frozen,
        Some(&grant.hash),
    )?;
    let receipt = Receipt {
        schema_version: RECORD_SCHEMA_VERSION,
        grant: grant.hash.clone(),
        research: study.generation.clone(),
        bundle_sha256: grant.bundle_sha256.clone(),
        holdout: grant.holdout.clone(),
        claims: claims.clone(),
        declaration: study.identity.clone(),
    };
    let receipt_key = study.key(&engine::receipt_key(&grant.hash));
    publish_record(
        local,
        &study.governance,
        &receipt_key,
        &engine::to_json(&receipt),
    )
    .map_err(|reason| format!("the grant is consumed by another run: {reason}"))?;
    let certification = Certification::authorize(manifest, &grant, &receipt, &receipt_key)?;
    let access = Access {
        declaration: Some(&study.declaration),
        certification: Some(&certification),
    };

    // A completed result under this grant is terminal: verify it in context and return.
    let generation = certification_generation_id(&study.generation, &grant.hash);
    let key = manifest_key(&generation);
    if destination.head(&key)?.is_some() {
        let uri = destination.uri(&key);
        let bytes = read_key(destination, &key)?;
        let verified = verify_certification(&uri, destination, &key, &bytes, access)?;
        report(&format!(
            "research certification {generation} (already published)\n{verified}"
        ))?;
        return Ok(());
    }

    let selection_uri = destination.uri(&manifest_key(&run.selection));
    let (_, selection, _) = portfolio::verified_selection(
        &selection_uri,
        destination,
        &manifest_key(&run.selection),
        &read_key(destination, &manifest_key(&run.selection))?,
        access,
    )?;
    let scenarios = assess(
        study.config,
        local,
        destination,
        &selection,
        &study.research.holdout,
        DatasetRole::Holdout,
        &run.descriptor.gates,
        &study.research.scenarios,
        access,
        report,
    )?;
    let verdict = engine::aggregate(&scenarios);
    let record = CertificationRecord {
        research: study.generation.clone(),
        bundle_sha256: grant.bundle_sha256.clone(),
        frozen: frozen.to_string(),
        grant: grant.hash.clone(),
        receipt: receipt_key.clone(),
        claims,
        holdout: grant.holdout.clone(),
        scenarios,
        verdict: verdict.clone(),
    };
    let (object, identity) = publish_object(
        local,
        destination,
        &format!("certification-{generation}"),
        CERTIFICATION_OBJECT_PATH,
        &record.to_json(),
    )?;
    let certification_manifest = CertificationManifest {
        kind: CERTIFICATION_MANIFEST_KIND.to_string(),
        schema_version: CERTIFICATION_SCHEMA_VERSION,
        generation: generation.clone(),
        research: study.generation.clone(),
        bundle_sha256: grant.bundle_sha256.clone(),
        grant: grant.hash.clone(),
        receipt: receipt_key,
        state: if verdict.passing() {
            "certified"
        } else {
            "rejected"
        }
        .to_string(),
        objects: vec![object],
    };
    let fresh = certification_manifest.to_json();
    let uri = destination.uri(&key);
    let (verified, put) = publish_manifest(
        local,
        destination,
        &key,
        fresh,
        |bytes| {
            let committed = CertificationManifest::from_json(bytes)
                .map_err(|error| format!("{uri}: {error}"))?;
            Ok(committed.generation == certification_manifest.generation
                && committed.state == certification_manifest.state
                && import::same_objects(
                    &committed.objects,
                    &certification_manifest.objects,
                    std::slice::from_ref(&identity),
                ))
        },
        |bytes| verify_certification(&uri, destination, &key, bytes, access),
    )?;
    let line = match put {
        Put::Reused(_) => format!(
            "research certification {generation} state {} (already published)",
            certification_manifest.state
        ),
        Put::Created(_) => format!(
            "research certification {generation} state {} [certify {:.3}s] peak_rss_kb {}",
            certification_manifest.state,
            certifying.elapsed().as_secs_f64(),
            search::peak_rss_kb()
        ),
    };
    report(&format!("{line}\n{verified}"))
}

// ----------------------------------------------------------------------------------------------
// The operator grant
// ----------------------------------------------------------------------------------------------

/// Creates the one-use grant for the frozen bundle at `bundle_manifest` over exactly the declared
/// holdout references, never opening a holdout object; an identical existing grant is reported.
pub fn grant(
    config_path: &Path,
    bundle_manifest: &str,
    holdout_manifests: &[String],
    reason: &str,
    out: &mut dyn Write,
) -> Result<(), String> {
    let config = crate::load_config(config_path)?;
    let study = bind(&config).map_err(|reason| format!("holdout grant: {reason}"))?;
    let target: ManifestUri = bundle_manifest.parse()?;
    if target.generation() != study.generation {
        return Err(format!(
            "holdout grant: {bundle_manifest} is not the research run of this configuration and declaration ({})",
            study.generation
        ));
    }
    let (store, key) = verify::open(bundle_manifest)?;
    let bytes = read_key(&store, &key)?;
    let manifest = RunManifest::from_json(&bytes)
        .map_err(|error| format!("holdout grant: {bundle_manifest}: {error}"))?;
    if manifest.declaration != study.identity || manifest.config_hash != config.content_hash() {
        return Err(format!(
            "holdout grant: {bundle_manifest} was frozen under another configuration or declaration"
        ));
    }
    if manifest.state != RunState::AwaitingHoldoutAuthorization.status() {
        return Err(format!(
            "holdout grant: {bundle_manifest} is `{}`, not awaiting holdout authorization",
            manifest.state
        ));
    }
    // The complete verifier before any authority is created over the bundle.
    verify_run(bundle_manifest, &store, &key, &bytes, study.access())
        .map_err(|reason| format!("holdout grant: {reason}"))?;
    let holdout = study.holdout();
    let declared: Vec<String> = holdout
        .iter()
        .map(|reference| reference.manifest.to_string())
        .collect();
    if holdout_manifests != declared.as_slice() {
        return Err(format!(
            "holdout grant: --holdout-manifest must name the declared holdout references in instrument order: {}",
            declared.join(" ")
        ));
    }
    let mut grant = Grant {
        schema_version: RECORD_SCHEMA_VERSION,
        research: study.generation.clone(),
        bundle_sha256: manifest.bundle_sha256().to_string(),
        holdout,
        declaration: study.identity.clone(),
        root: study.declaration.root.clone(),
        namespace: study.declaration.namespace.clone(),
        tokens: study.protected_tokens()?,
        operator: std::env::var("USER").unwrap_or_else(|_| "unavailable".to_string()),
        reason: reason.to_string(),
        created_at: now_text(),
        hash: String::new(),
    };
    grant.hash = grant.content_hash();
    let key = study.key(&engine::grant_key(&study.generation));
    let uri = study.governance.uri(&key);
    if study.governance.head(&key)?.is_some() {
        let existing = Grant::from_json(&read_key(&study.governance, &key)?)
            .map_err(|reason| format!("{uri}: {reason}"))?;
        if (
            &existing.research,
            &existing.bundle_sha256,
            &existing.holdout,
            &existing.declaration,
            &existing.tokens,
        ) != (
            &grant.research,
            &grant.bundle_sha256,
            &grant.holdout,
            &grant.declaration,
            &grant.tokens,
        ) {
            return Err(format!(
                "{uri} already authorizes another bundle or population; grants are never overwritten"
            ));
        }
        return writeln!(
            out,
            "holdout grant {} research {} at {uri} (already created {})",
            existing.hash, existing.research, existing.created_at
        )
        .map_err(|error| format!("cannot write the report: {error}"));
    }
    let (local, _) = stores(config_path, &config)?;
    publish_record(&local, &study.governance, &key, &engine::to_json(&grant))?;
    writeln!(
        out,
        "holdout grant {} research {} at {uri}",
        grant.hash, grant.research
    )
    .map_err(|error| format!("cannot write the report: {error}"))
}

// ----------------------------------------------------------------------------------------------
// Verification
// ----------------------------------------------------------------------------------------------

/// Re-lowers the recorded research configuration and checks every published child against it:
/// each instrument's profile, features, outcomes, and family; the selection; the frozen stage;
/// and, for a selected policy, the outer claims and every scenario's applied features, replay,
/// projection, and verdict, recomputed and compared field by field.
pub fn verify_run(
    uri: &str,
    store: &Store,
    key: &str,
    bytes: &[u8],
    access: Access<'_>,
) -> Result<String, String> {
    let manifest = RunManifest::from_json(bytes).map_err(|error| format!("{uri}: {error}"))?;
    if manifest.key() != key {
        return Err(format!(
            "{uri} holds the manifest of generation {}",
            manifest.generation
        ));
    }
    let run_bytes = search::read_object(store, &manifest.objects, RUN_OBJECT_PATH)?;
    let run =
        Run::from_json(&run_bytes).map_err(|error| format!("{uri}: {RUN_OBJECT_PATH}: {error}"))?;
    let config = &run.config;
    let research = config
        .research
        .as_ref()
        .ok_or_else(|| format!("{uri}: the recorded configuration has no research table"))?;
    if config.content_hash() != manifest.config_hash
        || run.declaration != manifest.declaration
        || run.selection != manifest.selection
        || run.state.status() != manifest.state
        || run.descriptor != engine::descriptor(research)
        || manifest.generation
            != run_generation_id(
                &manifest.config_hash,
                &manifest.code_revision,
                &run.declaration,
            )
    {
        return Err(format!(
            "{uri}: the run record does not carry the manifest's configuration, declaration, selection, state, and descriptor"
        ));
    }
    // The frozen stage: published before any outer claim, and carried unchanged into the run.
    let frozen_key = engine::frozen_key(&manifest.generation);
    let frozen_bytes = read_key(store, &frozen_key)?;
    let frozen = Frozen::from_json(&frozen_bytes)
        .map_err(|error| format!("{}: {error}", store.uri(&frozen_key)))?;
    if run.frozen.as_deref() != Some(engine::digest(b"", &frozen_bytes).as_str())
        || frozen.research != manifest.generation
        || frozen.intent != run.intent
        || frozen.declaration != run.declaration
        || frozen.instruments != run.instruments
        || frozen.selection != run.selection
        || frozen.scenarios != research.scenarios
        || frozen.descriptor != run.descriptor
    {
        return Err(format!(
            "{uri}: the frozen stage does not bind the run's identity, intent, children, selection, scenarios, and descriptor"
        ));
    }
    // Under the declaration this run was frozen under: the attempt intent exists with exactly
    // the populations, predecessors, and changes this configuration declares.
    let governance = match access.declaration {
        Some(declaration) if declaration.identity() != run.declaration => {
            return Err(format!(
                "{uri}: the configured declaration is not the one this run was frozen under"
            ));
        }
        Some(declaration) => {
            let governance = Store::open(&declaration.root)?;
            let intent_key = declaration.key(&engine::intent_key(
                &research.study.study,
                &research.study.attempt,
            ));
            let intent = Intent {
                schema_version: RECORD_SCHEMA_VERSION,
                study: research.study.study.clone(),
                attempt: research.study.attempt.clone(),
                config_hash: manifest.config_hash.clone(),
                code_revision: manifest.code_revision.clone(),
                declaration: run.declaration.clone(),
                root: declaration.root.clone(),
                namespace: declaration.namespace.clone(),
                predecessors: research.study.predecessors.clone(),
                changes: research.study.changes.clone(),
                populations: population_uses(research, declaration)?,
            };
            if run.intent != intent_key
                || read_key(&governance, &intent_key)? != engine::to_json(&intent)
            {
                return Err(format!(
                    "{uri}: {} is not the intent of this attempt",
                    governance.uri(&intent_key)
                ));
            }
            Some((declaration, governance))
        }
        None => None,
    };
    let selection =
        verified_children(uri, store, config, &run.instruments, &run.selection, access)?;
    // The state and, for a selected policy, the outer claims and every scenario.
    let expected_state = match &selection.state {
        State::NoFeasiblePolicy => RunState::NoFeasiblePolicy,
        State::RefitInapplicable { reason } => RunState::RefitInapplicable {
            reason: reason.clone(),
        },
        State::OuterRejected { .. } => {
            return Err(format!("{uri}: the selection carries an outer result"));
        }
        State::Selected => {
            if let Some((declaration, governance)) = &governance {
                let tokens: Vec<String> = declaration
                    .tokens(
                        research
                            .evaluation
                            .inputs
                            .iter()
                            .map(ManifestUri::generation),
                    )?
                    .into_iter()
                    .collect();
                let claims = Claims {
                    declaration,
                    identity: &run.declaration,
                    study: &research.study,
                    kind: ClaimKind::AssessmentUse,
                    tokens: &tokens,
                    research: &manifest.generation,
                    frozen: run.frozen.as_deref().unwrap_or_default(),
                    grant: None,
                };
                if claims.verify(governance)? != run.claims {
                    return Err(format!(
                        "{uri}: the outer claims are not this run's claims of the declared evaluation tokens"
                    ));
                }
            }
            let verdict = verify_scenarios(
                uri,
                store,
                &selection,
                &research.evaluation,
                DatasetRole::Evaluation,
                &run.descriptor.gates,
                &run.descriptor.scenarios,
                &research.scenarios,
                &run.outer,
                access,
            )?;
            if verdict.passing() {
                RunState::AwaitingHoldoutAuthorization
            } else {
                RunState::OuterRejected { verdict }
            }
        }
    };
    if run.state != expected_state {
        return Err(format!(
            "{uri}: the recorded state `{}` is not the procedure's `{}`",
            run.state.status(),
            expected_state.status()
        ));
    }
    if run.state == RunState::AwaitingHoldoutAuthorization {
        run.complete_bundle()
            .map_err(|reason| format!("{uri}: {reason}"))?;
    }
    Ok(format!(
        "verified research generation {} state {} instruments {} scenarios {} objects 1 bytes {}",
        manifest.generation,
        manifest.state,
        run.instruments.len(),
        run.outer.len(),
        run_bytes.len()
    ))
}

/// Every published child of one frozen stage re-derived from the recorded configuration: each
/// instrument's profile, features (the configured fit before its cutoff), outcomes (the
/// configured rule over the recorded source and features), and family, every one for the
/// configured instrument; then the selection, whose configuration must equal the lowered
/// table. Every child restores through its own verifier.
fn verified_children(
    uri: &str,
    store: &Store,
    config: &Config,
    instruments: &[InstrumentRecord],
    selection: &str,
    access: Access<'_>,
) -> Result<portfolio_engine::Selection, String> {
    let research = config
        .research
        .as_ref()
        .ok_or_else(|| format!("{uri}: the recorded configuration has no research table"))?;
    if instruments.len() != research.instruments.len() {
        return Err(format!(
            "{uri}: {} instrument records for {} configured instruments",
            instruments.len(),
            research.instruments.len()
        ));
    }
    let mut profiles: BTreeMap<String, ManifestUri> = BTreeMap::new();
    let mut families = Vec::with_capacity(instruments.len());
    for (instrument, record) in research.instruments.iter().zip(instruments) {
        let source = &instrument.source_manifest;
        if record.instrument != instrument.instrument || record.source != source.generation() {
            return Err(format!(
                "{uri}: instrument record {} is not the configured instrument and source",
                record.instrument
            ));
        }
        // Every child restores through its own verifier before anything reuses it.
        for generation in [&record.profile, &record.feature, &record.outcome] {
            verify::run_with(&store.uri(&manifest_key(generation)), access)?;
        }
        let profile = verified_profile(uri, store, &record.profile, &record.source)?;
        profiles.insert(record.source.clone(), profile.clone());
        let (feature_store, feature) =
            features::feature_manifest(uri, &store.uri(&manifest_key(&record.feature)), access)?;
        if feature.role != DatasetRole::Development
            || feature.instrument != instrument.instrument
            || feature.input_generation != record.source
            || feature.profile_generation != record.profile
            || feature.frozen_from.is_some()
        {
            return Err(format!(
                "{uri}: feature generation {} is not the development fit of {} under its profile",
                record.feature, record.source
            ));
        }
        let plan = features::fitted_plan(uri, &feature_store, &feature)?;
        let fit = engine::fit_entry(instrument, source, profile);
        if *features::resolve(&fit, access)?.plan() != plan.unfitted() {
            return Err(format!(
                "{uri}: feature generation {} is not the configured fit of {}",
                record.feature, record.instrument
            ));
        }
        let outcome_key = manifest_key(&record.outcome);
        let outcome_bytes = read_key(store, &outcome_key)?;
        if verify::manifest_kind(&outcome_bytes)?.as_deref() != Some(OUTCOME_MANIFEST_KIND) {
            return Err(format!(
                "{uri}: {} is not an outcome generation",
                record.outcome
            ));
        }
        let outcome = OutcomeManifest::from_json(&outcome_bytes)
            .map_err(|error| format!("{uri}: {error}"))?;
        let rule = OutcomeRule::resolve(&engine::outcomes_table(
            instrument,
            source,
            &ready_uri(store, &record.feature)?,
        ))?;
        if outcome.generation != record.outcome
            || outcome.role != DatasetRole::Development
            || outcome.tick_generation != record.source
            || outcome.feature_generation != record.feature
            || record.outcome != outcome_generation_id(&record.source, &record.feature, &rule)
        {
            return Err(format!(
                "{uri}: outcome generation {} is not the configured rule over the recorded source and features",
                record.outcome
            ));
        }
        let family_uri = store.uri(&manifest_key(&record.family));
        let (family_manifest, family) = search::development_family(&family_uri, access)?;
        let expected = search_config(
            config,
            engine::search_table(
                instrument,
                ReplayInput {
                    tick_manifest: source.clone(),
                    feature_manifest: ready_uri(store, &record.feature)?,
                    outcome_manifest: Some(ready_uri(store, &record.outcome)?),
                },
            ),
        );
        if family_manifest.config_hash != expected.content_hash()
            || family.search != *expected.search.as_ref().expect("set")
            || family_manifest
                .inputs
                .iter()
                .any(|input| input.instrument != instrument.instrument)
        {
            return Err(format!(
                "{uri}: family generation {} is not the search this configuration lowers for {}",
                record.family, record.instrument
            ));
        }
        families.push(ready_uri(store, &record.family)?);
    }
    let selection_key = manifest_key(selection);
    let selection_uri = store.uri(&selection_key);
    for fit in research
        .folds
        .iter()
        .flat_map(|fold| fold.inputs.iter().map(|input| &input.fit_manifest))
        .chain(research.refit.fits.iter())
    {
        if !profiles.contains_key(fit.generation()) {
            let profile = recorded_fit_profile(&selection_uri, store, selection, fit)?;
            let profile = verified_profile(uri, store, &profile, fit.generation())?;
            profiles.insert(fit.generation().to_string(), profile);
        }
    }
    // The selection: the existing verifier, then its configuration equals the lowered table.
    let (_, selected, _) = portfolio::verified_selection(
        &selection_uri,
        store,
        &selection_key,
        &read_key(store, &selection_key)?,
        access,
    )?;
    let expected = portfolio_config(config, selection_table(research, families, &profiles)?);
    if selected.config != expected {
        return Err(format!(
            "{uri}: selection generation {selection} is not the selection this configuration lowers"
        ));
    }
    Ok(selected)
}

/// The stream manifest of `profile` describes `source` on development data.
fn verified_profile(
    uri: &str,
    store: &Store,
    profile: &str,
    source: &str,
) -> Result<ManifestUri, String> {
    let key = manifest_key(profile);
    let bytes = read_key(store, &key)?;
    if verify::manifest_kind(&bytes)?.as_deref() != Some(STREAM_MANIFEST_KIND) {
        return Err(format!(
            "{uri}: {profile} is not an instrument stream generation"
        ));
    }
    let manifest = StreamManifest::from_json(&bytes).map_err(|error| format!("{uri}: {error}"))?;
    if manifest.source_generation != source || manifest.role != DatasetRole::Development {
        return Err(format!(
            "{uri}: stream generation {profile} is not the development profile of {source}"
        ));
    }
    store.uri(&key).parse()
}

/// The profile the recorded selection fitted `fit` under.
fn recorded_fit_profile(
    selection_uri: &str,
    store: &Store,
    selection: &str,
    fit: &ManifestUri,
) -> Result<String, String> {
    let key = manifest_key(selection);
    let manifest = portfolio_engine::SelectionManifest::from_json(&read_key(store, &key)?)
        .map_err(|error| format!("{selection_uri}: {error}"))?;
    let recorded = portfolio_engine::Selection::from_json(&search::read_object(
        store,
        &manifest.objects,
        portfolio_engine::SELECTION_OBJECT_PATH,
    )?)
    .map_err(|error| format!("{selection_uri}: {error}"))?;
    recorded
        .config
        .portfolio
        .as_ref()
        .into_iter()
        .flat_map(|table| {
            table
                .folds
                .iter()
                .flat_map(|fold| fold.inputs.iter().map(|input| &input.fit))
                .chain(table.refit.fits.iter())
        })
        .find(|entry| entry.input_manifest == *fit)
        .map(|entry| entry.profile_manifest.generation().to_string())
        .ok_or_else(|| {
            format!(
                "{selection_uri}: no fit records generation {}",
                fit.generation()
            )
        })
}

/// Every recorded scenario result of one later-role window re-derived: the applied features
/// through the recorded refit, the replay restored and checked against the exact lowered
/// table, the projection under the frozen gates, and the verdict; then the aggregate.
#[allow(clippy::too_many_arguments)]
fn verify_scenarios(
    uri: &str,
    store: &Store,
    selection: &portfolio_engine::Selection,
    window: &Evaluation,
    role: DatasetRole,
    gates: &portfolio_engine::Gates,
    order: &[String],
    scenarios: &[binary_alpha_engine::config::ResearchScenario],
    results: &[ScenarioResult],
    access: Access<'_>,
) -> Result<Verdict, String> {
    let settings = selection
        .config
        .portfolio
        .as_ref()
        .ok_or_else(|| format!("{uri}: the selection records no portfolio table"))?;
    let policy = selection
        .frozen
        .as_ref()
        .ok_or_else(|| format!("{uri}: the selection records no frozen policy"))?;
    let choice = selection
        .selected
        .map(|index| selection.choices[index].key())
        .ok_or_else(|| format!("{uri}: the selection records no selected choice"))?;
    let recorded: Vec<&str> = results
        .iter()
        .map(|result| result.scenario.as_str())
        .collect();
    if recorded != order.iter().map(String::as_str).collect::<Vec<_>>() {
        return Err(format!(
            "{uri}: the recorded scenarios are not the frozen scenario set in order"
        ));
    }
    let mut expected: Vec<ScenarioResult> = Vec::with_capacity(results.len());
    for result in results {
        if result.outer.features.len() != window.inputs.len() {
            return Err(format!(
                "{uri}: scenario {} records {} feature generations for {} inputs",
                result.scenario,
                result.outer.features.len(),
                window.inputs.len()
            ));
        }
        let mut inputs = Vec::with_capacity(window.inputs.len());
        for (input, applied) in window.inputs.iter().zip(&result.outer.features) {
            let fit = selection
                .refit
                .iter()
                .find(|fit| fit.instrument == applied.instrument)
                .ok_or_else(|| {
                    format!(
                        "{uri}: feature generation {} applies no refit",
                        applied.generation
                    )
                })?;
            let entry = settings
                .refit
                .fits
                .iter()
                .zip(&selection.refit)
                .find(|(_, record)| record.generation == fit.generation)
                .map(|(entry, _)| entry)
                .ok_or_else(|| format!("{uri}: no refit entry for {}", fit.generation))?;
            let (_, _, replay_input) = portfolio::recorded_input(
                uri,
                store,
                fit,
                &entry.input_manifest,
                applied,
                role,
                input,
                access,
            )?;
            inputs.push(replay_input);
        }
        let (scenario_policy, replay_scenario) = if result.scenario == BASELINE_SCENARIO {
            (policy.clone(), None)
        } else {
            let scenario = scenarios
                .iter()
                .find(|scenario| scenario.id == result.scenario)
                .ok_or_else(|| format!("{uri}: scenario {} is not configured", result.scenario))?;
            (
                engine::scenario_policy(settings, &choice, policy, scenario)?,
                Some(binary_alpha_engine::config::ReplayScenario {
                    schema_version: 1,
                    id: scenario.id.clone(),
                    acceptance_delay_micros: scenario.acceptance_delay_micros,
                }),
            )
        };
        let mut table = portfolio_engine::replay_table(
            settings,
            &scenario_policy,
            role,
            &window.decision_start,
            &window.decision_end,
            inputs,
            window.splits.clone(),
        );
        table.scenario = replay_scenario;
        let restored =
            portfolio::recorded_replay(uri, store, &result.outer.replay, &table, access)?;
        let projection = portfolio_engine::project(&restored.engine, gates)?;
        let verdict = engine::verdict(&projection, gates);
        expected.push(ScenarioResult {
            scenario: result.scenario.clone(),
            outer: Outer {
                features: result.outer.features.clone(),
                replay: result.outer.replay.clone(),
                splits: restored.engine.summary().splits.clone(),
                projection,
            },
            verdict,
        });
    }
    if expected != results {
        return Err(format!(
            "{uri}: a recorded scenario result is not the projection and verdict of its replay"
        ));
    }
    Ok(engine::aggregate(results))
}

/// Verifies a certification generation: publicly, its envelope and authorization references
/// without resolving protected children; within the matching certification context, every
/// scenario's holdout evidence, projection, and verdict.
pub fn verify_certification(
    uri: &str,
    store: &Store,
    key: &str,
    bytes: &[u8],
    access: Access<'_>,
) -> Result<String, String> {
    let manifest =
        CertificationManifest::from_json(bytes).map_err(|error| format!("{uri}: {error}"))?;
    if manifest.key() != key {
        return Err(format!(
            "{uri} holds the manifest of generation {}",
            manifest.generation
        ));
    }
    let run_key = manifest_key(&manifest.research);
    let run_manifest = RunManifest::from_json(&read_key(store, &run_key)?)
        .map_err(|error| format!("{}: {error}", store.uri(&run_key)))?;
    if run_manifest.bundle_sha256() != manifest.bundle_sha256
        || run_manifest.state != RunState::AwaitingHoldoutAuthorization.status()
    {
        return Err(format!(
            "{uri}: the research run does not carry the frozen bundle this certification names"
        ));
    }
    let run = Run::from_json(&search::read_object(
        store,
        &run_manifest.objects,
        RUN_OBJECT_PATH,
    )?)
    .map_err(|error| format!("{uri}: {RUN_OBJECT_PATH}: {error}"))?;
    let research = run
        .config
        .research
        .as_ref()
        .ok_or_else(|| format!("{uri}: the run records no research table"))?;
    // Under a declaration: the grant names this bundle, every protected claim is this run's,
    // and the receipt records exactly their consumption of the grant.
    let receipt = match access.declaration {
        Some(declaration) if declaration.identity() != run.declaration => {
            return Err(format!(
                "{uri}: the configured declaration is not the one this run was frozen under"
            ));
        }
        Some(declaration) => {
            let grant_key = declaration.key(&engine::grant_key(&manifest.research));
            let governance = Store::open(&declaration.root)?;
            let grant = Grant::from_json(&read_key(&governance, &grant_key)?)
                .map_err(|reason| format!("{}: {reason}", governance.uri(&grant_key)))?;
            let receipt_key = declaration.key(&engine::receipt_key(&grant.hash));
            if grant.hash != manifest.grant
                || !grant_binds(
                    &grant,
                    &run_manifest,
                    research,
                    declaration,
                    &run.declaration,
                )?
                || manifest.receipt != receipt_key
            {
                return Err(format!(
                    "{uri}: the grant does not authorize this certification"
                ));
            }
            let claims = Claims {
                declaration,
                identity: &grant.declaration,
                study: &research.study,
                kind: ClaimKind::HoldoutUse,
                tokens: &grant.tokens,
                research: &manifest.research,
                frozen: run.frozen.as_deref().unwrap_or_default(),
                grant: Some(&grant.hash),
            };
            let keys = claims.verify(&governance)?;
            let receipt = Receipt::from_json(&read_key(&governance, &receipt_key)?)
                .map_err(|reason| format!("{}: {reason}", governance.uri(&receipt_key)))?;
            if receipt.grant != grant.hash
                || receipt.research != manifest.research
                || receipt.bundle_sha256 != grant.bundle_sha256
                || receipt.holdout != grant.holdout
                || receipt.claims != keys
                || receipt.declaration != grant.declaration
            {
                return Err(format!(
                    "{uri}: the receipt does not record this run's consumption of the grant"
                ));
            }
            Some(receipt)
        }
        None => None,
    };
    let Some(certification) = access.certification.filter(|certification| {
        certification.run() == manifest.research
            && certification.grant() == manifest.grant
            && certification.receipt() == manifest.receipt
            && certification.bundle_sha256() == manifest.bundle_sha256
    }) else {
        return Ok(format!(
            "verified research certification {} state {} envelope only: protected evidence is verified within the authorized certification run",
            manifest.generation, manifest.state
        ));
    };
    let record_bytes = search::read_object(store, &manifest.objects, CERTIFICATION_OBJECT_PATH)?;
    let record = CertificationRecord::from_json(&record_bytes)
        .map_err(|error| format!("{uri}: {CERTIFICATION_OBJECT_PATH}: {error}"))?;
    if record.research != manifest.research
        || record.bundle_sha256 != manifest.bundle_sha256
        || record.grant != manifest.grant
        || record.receipt != manifest.receipt
        || receipt.is_some_and(|receipt| receipt.claims != record.claims)
        || run.frozen.as_deref() != Some(record.frozen.as_str())
        || record.verdict.passing() != (manifest.state == "certified")
        || !certification.covers(
            record
                .holdout
                .iter()
                .map(|reference| reference.manifest.generation()),
        )
    {
        return Err(format!(
            "{uri}: the certification record does not carry the manifest's references and state"
        ));
    }
    let selection_key = manifest_key(&run.selection);
    let (_, selection, _) = portfolio::verified_selection(
        &store.uri(&selection_key),
        store,
        &selection_key,
        &read_key(store, &selection_key)?,
        access,
    )?;
    let verdict = verify_scenarios(
        uri,
        store,
        &selection,
        &research.holdout,
        DatasetRole::Holdout,
        &run.descriptor.gates,
        &run.descriptor.scenarios,
        &research.scenarios,
        &record.scenarios,
        access,
    )?;
    if verdict != record.verdict {
        return Err(format!(
            "{uri}: the recorded verdict is not the aggregate of its scenarios"
        ));
    }
    Ok(format!(
        "verified research certification {} state {} scenarios {} objects 1 bytes {}",
        manifest.generation,
        manifest.state,
        record.scenarios.len(),
        record_bytes.len()
    ))
}

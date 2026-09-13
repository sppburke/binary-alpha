//! `binary-alpha portfolio optimize`: bind the development-only families and every declared
//! input on their manifest bytes, freeze the logical universe and enumerate every complete
//! choice, fit and apply one plan per instrument and inner fold through the feature owner,
//! replay every structurally valid choice jointly per fold through the replay owner, select
//! under the frozen objective, refit the selected choice, optionally evaluate it once, and
//! publish one immutable selection that `data verify` re-derives.
//!
//! The engine module `portfolio` owns every pure rule and record; this module owns binding, the
//! feature builds, the replay publications, the restored-engine projection, temporary files,
//! publication, and the verifier.

use std::collections::BTreeMap;
use std::fs;
use std::io::Write;
use std::path::Path;
use std::time::Instant;

use binary_alpha_engine::config::{
    Config, FeatureInstrument, Features, ManifestUri, Portfolio, Replay, RunMode,
};
use binary_alpha_engine::dataset::{DatasetRole, GenerationManifest, ObjectRole, manifest_key};
use binary_alpha_engine::execution::{ReplayInput, Split};
use binary_alpha_engine::features::{FeatureManifest, FeaturePlan};
use binary_alpha_engine::market::parse_event_time_micros;
use binary_alpha_engine::portfolio::{
    self as engine, Choice, Failure, FamilyRecord, FeatureRef, FoldRecord, FoldResult, Form,
    LogicalMember, Outer, Policy, ReplayRef, SELECTION_MANIFEST_KIND, SELECTION_OBJECT_PATH,
    SELECTION_SCHEMA_VERSION, Selection, SelectionManifest, SourceMember, State,
    selection_generation_id,
};
use binary_alpha_engine::search::Family;

use crate::features;
use crate::import::{self, CODE_REVISION};
use crate::replay;
use crate::search;
use crate::store::{self, Put, Store};
use crate::verify;

/// Runs the configured selection, writing its report and verification lines to `out`.
pub fn run(config_path: &Path, out: &mut dyn Write) -> Result<(), String> {
    let config = crate::load_config(config_path)?;
    if config.run_mode != RunMode::Research {
        return Err(format!(
            "run_mode: a portfolio selection is research, not `{}`",
            config.run_mode
        ));
    }
    let base = config_path.parent().unwrap_or(Path::new("."));
    let historical_dir = base.join(config.storage.historical_data_dir.as_path());
    fs::create_dir_all(&historical_dir)
        .map_err(|error| format!("cannot create {}: {error}", historical_dir.display()))?;
    let local = Store::filesystem(&historical_dir);
    let destination = Store::open(&config.storage.publication_uri)?;
    let line =
        optimize(&config, &local, &destination).map_err(|reason| format!("portfolio: {reason}"))?;
    writeln!(out, "{line}")
        .and_then(|()| out.flush())
        .map_err(|error| format!("cannot write the report: {error}"))
}

// ----------------------------------------------------------------------------------------------
// Binding on manifest bytes
// ----------------------------------------------------------------------------------------------

/// Reads one tick generation's ready manifest and refuses, on its bytes alone, another kind,
/// another generation, holdout data, and another role.
fn tick_manifest(
    field: &str,
    uri: &ManifestUri,
    role: DatasetRole,
) -> Result<GenerationManifest, String> {
    let text = uri.to_string();
    let (store, key) = verify::open(&text)?;
    let mut bytes = Vec::new();
    store.read_to(&key, None, &mut bytes)?;
    if let Some(kind) = verify::manifest_kind(&bytes)? {
        return Err(format!(
            "{field}: {text} is a `{kind}` manifest, not a dataset ready manifest"
        ));
    }
    let manifest = GenerationManifest::from_json(&bytes)
        .map_err(|error| format!("{field}: {text}: {error}"))?;
    if manifest.key() != key {
        return Err(format!(
            "{field}: {text} holds the manifest of generation {}",
            manifest.generation
        ));
    }
    if manifest.role == DatasetRole::Holdout {
        return Err(format!(
            "{field}: holdout data never enters a portfolio selection"
        ));
    }
    if manifest.role != role {
        return Err(format!(
            "{field}: generation {} is `{}`, not `{role}`",
            manifest.generation, manifest.role
        ));
    }
    Ok(manifest)
}

/// One resolved fit whose whole fitting coverage ends before `cutoff`.
fn bind_fit(
    field: &str,
    entry: &FeatureInstrument,
    cutoff: i64,
) -> Result<features::Resolved, String> {
    let resolved = features::resolve(entry).map_err(|reason| format!("{field}: {reason}"))?;
    let coverage = &resolved.input().coverage.last_event_time;
    if parse_event_time_micros(coverage)? >= cutoff {
        return Err(format!(
            "{field}.input_manifest: the fitting coverage of generation {} ends at {coverage}, not before the cutoff",
            resolved.input().generation
        ));
    }
    Ok(resolved)
}

/// One instrument's bound fold inputs.
struct BoundInput {
    instrument: String,
    entry: FeatureInstrument,
    fit: features::Resolved,
}

/// Every instrument of `inputs` once, and every binding's instrument among them.
fn instruments_cover(field: &str, settings: &Portfolio, inputs: &[&str]) -> Result<(), String> {
    for (index, instrument) in inputs.iter().enumerate() {
        if inputs[..index].contains(instrument) {
            return Err(format!(
                "{field}[{index}]: instrument {instrument} is listed twice"
            ));
        }
    }
    for (index, binding) in settings.bindings.iter().enumerate() {
        if !inputs.contains(&binding.instrument.as_str()) {
            return Err(format!(
                "{field}: no input supplies instrument {} of bindings[{index}]",
                binding.instrument
            ));
        }
    }
    Ok(())
}

/// The bound declared inputs: every fold's fits and assessments, the refit fits, and the
/// evaluation tick manifests, all checked on their manifest bytes before any output exists.
struct Bound {
    folds: Vec<Vec<BoundInput>>,
    refit: Vec<BoundInput>,
    evaluation: Vec<GenerationManifest>,
}

fn bind(settings: &Portfolio) -> Result<Bound, String> {
    let mut folds = Vec::with_capacity(settings.folds.len());
    for (index, fold) in settings.folds.iter().enumerate() {
        let cutoff = parse_event_time_micros(&fold.cutoff)?;
        let mut inputs = Vec::with_capacity(fold.inputs.len());
        for (position, input) in fold.inputs.iter().enumerate() {
            let field = format!("folds[{index}].inputs[{position}]");
            let fit = bind_fit(&format!("{field}.fit"), &input.fit, cutoff)?;
            let assessment = tick_manifest(
                &format!("{field}.assessment_manifest"),
                &input.assessment_manifest,
                DatasetRole::Development,
            )?;
            if assessment.instrument != fit.input().instrument {
                return Err(format!(
                    "{field}.assessment_manifest: generation {} is {}, not the fit's instrument {}",
                    assessment.generation,
                    assessment.instrument,
                    fit.input().instrument
                ));
            }
            inputs.push(BoundInput {
                instrument: assessment.instrument,
                entry: input.fit.clone(),
                fit,
            });
        }
        instruments_cover(
            &format!("folds[{index}].inputs"),
            settings,
            &inputs
                .iter()
                .map(|input| input.instrument.as_str())
                .collect::<Vec<_>>(),
        )?;
        folds.push(inputs);
    }
    let cutoff = parse_event_time_micros(&settings.refit.cutoff)?;
    let refit = settings
        .refit
        .fits
        .iter()
        .enumerate()
        .map(|(position, entry)| {
            let fit = bind_fit(&format!("refit.fits[{position}]"), entry, cutoff)?;
            Ok(BoundInput {
                instrument: fit.input().instrument.clone(),
                entry: entry.clone(),
                fit,
            })
        })
        .collect::<Result<Vec<_>, String>>()?;
    instruments_cover(
        "refit.fits",
        settings,
        &refit
            .iter()
            .map(|input| input.instrument.as_str())
            .collect::<Vec<_>>(),
    )?;
    // The evaluation tick manifests are read for their role and instrument alone; their objects
    // are opened only after selection and refit.
    let evaluation = match &settings.evaluation {
        None => Vec::new(),
        Some(evaluation) => {
            let manifests = evaluation
                .inputs
                .iter()
                .enumerate()
                .map(|(position, uri)| {
                    tick_manifest(
                        &format!("evaluation.inputs[{position}]"),
                        uri,
                        DatasetRole::Evaluation,
                    )
                })
                .collect::<Result<Vec<_>, String>>()?;
            instruments_cover(
                "evaluation.inputs",
                settings,
                &manifests
                    .iter()
                    .map(|manifest| manifest.instrument.as_str())
                    .collect::<Vec<_>>(),
            )?;
            manifests
        }
    };
    Ok(Bound {
        folds,
        refit,
        evaluation,
    })
}

// ----------------------------------------------------------------------------------------------
// Feature builds and replay publication
// ----------------------------------------------------------------------------------------------

/// The configuration of one synthesized feature build or replay: the skeleton and that table.
fn features_config(config: &Config, entry: &FeatureInstrument) -> Config {
    Config {
        features: Some(Features {
            instruments: vec![entry.clone()],
        }),
        ..crate::skeleton(config)
    }
}

fn replay_config(config: &Config, table: Replay) -> Config {
    Config {
        replay: Some(table),
        ..crate::skeleton(config)
    }
}

/// The ready-manifest location of a published generation in `destination`.
fn published_uri(destination: &Store, generation: &str) -> Result<ManifestUri, String> {
    destination.uri(&manifest_key(generation)).parse()
}

fn feature_ref(manifest: &FeatureManifest) -> FeatureRef {
    FeatureRef {
        instrument: manifest.instrument.clone(),
        input_generation: manifest.input_generation.clone(),
        generation: manifest.generation.clone(),
        plan_identity: manifest.plan_identity.clone(),
    }
}

/// The entry that applies a fitted plan to another generation of the same instrument under the
/// fit's own profile reference.
fn application(
    role: DatasetRole,
    input_manifest: &ManifestUri,
    fit: &FeatureInstrument,
    frozen_plan: ManifestUri,
) -> FeatureInstrument {
    FeatureInstrument {
        role,
        input_manifest: input_manifest.clone(),
        profile_manifest: fit.profile_manifest.clone(),
        frozen_plan: Some(frozen_plan),
        streams: None,
        outputs: None,
        moving_average_periods: None,
        rolling_window: None,
        min_history: None,
        structure: None,
        price_epsilon: None,
        tick_path_streams: None,
        encodings: None,
    }
}

/// One instrument's fitted plan and the generation it was applied to: the replay input of one
/// fold or the outer evaluation.
struct Applied {
    plan: FeaturePlan,
    fit: FeatureRef,
    applied: FeatureRef,
    input: ReplayInput,
}

/// Builds one instrument's fit and applies it to `input_manifest` of `role`.
fn fit_and_apply(
    config: &Config,
    local: &Store,
    destination: &Store,
    bound: BoundInput,
    role: DatasetRole,
    input_manifest: &ManifestUri,
) -> Result<Applied, String> {
    let fitted = features::build(
        bound.fit,
        &features_config(config, &bound.entry),
        local,
        destination,
    )?;
    let entry = application(
        role,
        input_manifest,
        &bound.entry,
        published_uri(destination, &fitted.manifest.generation)?,
    );
    let applied = features::build(
        features::resolve(&entry)?,
        &features_config(config, &entry),
        local,
        destination,
    )?;
    Ok(Applied {
        input: ReplayInput {
            tick_manifest: input_manifest.clone(),
            feature_manifest: published_uri(destination, &applied.manifest.generation)?,
            outcome_manifest: None,
        },
        plan: fitted.plan,
        fit: feature_ref(&fitted.manifest),
        applied: feature_ref(&applied.manifest),
    })
}

/// The plans of applied inputs by instrument.
fn plans_of(applied: &[Applied]) -> BTreeMap<String, FeaturePlan> {
    applied
        .iter()
        .map(|applied| (applied.fit.instrument.clone(), applied.plan.clone()))
        .collect()
}

/// Publishes (or resumes) one joint replay of `policy` and projects its verified restored
/// engine under the gates.
#[allow(clippy::too_many_arguments)]
fn replay_policy(
    config: &Config,
    local: &Store,
    destination: &Store,
    settings: &Portfolio,
    policy: &Policy,
    role: DatasetRole,
    window: (&str, &str),
    inputs: Vec<ReplayInput>,
    splits: Option<Vec<Split>>,
) -> Result<(replay::Published, engine::Projection), String> {
    let table = engine::replay_table(settings, policy, role, window.0, window.1, inputs, splits);
    let published = replay::publish(&replay_config(config, table), local, destination, true)?;
    let projection = engine::project(&published.engine, &settings.gates)?;
    Ok((published, projection))
}

fn replay_ref(published: &replay::Published) -> ReplayRef {
    ReplayRef {
        generation: published.manifest.generation.clone(),
        summary_identity: published.manifest.summary_identity.clone(),
    }
}

// ----------------------------------------------------------------------------------------------
// The selection
// ----------------------------------------------------------------------------------------------

/// The verified families in declared order with their source-member records.
fn families(settings: &Portfolio) -> Result<(Vec<Family>, Vec<FamilyRecord>), String> {
    let mut families = Vec::with_capacity(settings.families.len());
    let mut records = Vec::with_capacity(settings.families.len());
    for (index, uri) in settings.families.iter().enumerate() {
        let (manifest, family) = search::development_family(&uri.to_string())
            .map_err(|reason| format!("families[{index}]: {reason}"))?;
        records.push(FamilyRecord {
            generation: manifest.generation,
            plan_identity: family.plan_identity.clone(),
            base_stream: family.base_stream,
            members: family
                .members
                .iter()
                .enumerate()
                .map(|(member, source)| SourceMember {
                    logic_identity: source.logic_identity.clone(),
                    contract: source.contract.clone(),
                    bases: settings
                        .members
                        .iter()
                        .enumerate()
                        .filter(|(_, base)| base.family == index && base.member == member)
                        .map(|(base, _)| base)
                        .collect(),
                })
                .collect(),
        });
        families.push(family);
    }
    Ok((families, records))
}

/// Every declared choice with its identity and structural verdict, before any fold is read.
fn choices(settings: &Portfolio, members: &[LogicalMember]) -> Result<Vec<Choice>, String> {
    engine::enumerate(settings)
        .into_iter()
        .map(|key| {
            let logical = match engine::policy(settings, members, &key, Form::Logical) {
                Ok(policy) => policy,
                Err(Failure::Error(reason) | Failure::Inapplicable(reason)) => return Err(reason),
            };
            Ok(Choice {
                subset: key.subset,
                alternatives: key.alternatives,
                risk_policy: key.risk_policy,
                identity: logical.identity(),
                rejection: engine::structure(settings, &logical).err(),
                folds: Vec::new(),
                profit: None,
                drawdown: None,
                failure: None,
                rank: None,
            })
        })
        .collect()
}

/// The resolved policy of one choice under `plans`, or why the fold does not apply.
fn resolve(
    settings: &Portfolio,
    members: &[LogicalMember],
    choice: &Choice,
    plans: &BTreeMap<String, FeaturePlan>,
) -> Result<Result<Policy, String>, String> {
    match engine::policy(settings, members, &choice.key(), Form::Resolved(plans)) {
        Ok(policy) => Ok(Ok(policy)),
        Err(Failure::Inapplicable(reason)) => Ok(Err(reason)),
        Err(Failure::Error(reason)) => Err(reason),
    }
}

/// Wall-clock stages of one run, outside every identity.
#[derive(Default)]
struct Clock {
    bind: f64,
    folds: f64,
    refit: f64,
    publish: f64,
}

/// The typed selection every caller uses: bind, enumerate, fit, replay, select, refit,
/// evaluate, publish, and verify one selection generation of the configuration's `portfolio`
/// table, returning its report and verification lines.
pub fn optimize(config: &Config, local: &Store, destination: &Store) -> Result<String, String> {
    let settings = config
        .portfolio
        .as_ref()
        .ok_or("portfolio: the table is required")?;
    let mut clock = Clock::default();
    let started = Instant::now();

    // 1. Every declared input on its manifest bytes, then the verified development-only
    //    families, the frozen logical universe, and every declared choice.
    let bound = bind(settings)?;
    let (families, family_records) = families(settings)?;
    let members = engine::logical_members(settings, &families)?;
    let mut choices = choices(settings, &members)?;
    let declared = engine::declared_count(settings)?;
    let rejected = choices
        .iter()
        .filter(|choice| choice.rejection.is_some())
        .count() as u64;
    clock.bind = started.elapsed().as_secs_f64();

    // 2. Every structurally valid choice, jointly, per inner fold.
    let folding = Instant::now();
    let mut fold_records = Vec::with_capacity(settings.folds.len());
    for (index, inputs) in bound.folds.into_iter().enumerate() {
        let fold = &settings.folds[index];
        let applied = inputs
            .into_iter()
            .zip(&fold.inputs)
            .map(|(bound, input)| {
                fit_and_apply(
                    config,
                    local,
                    destination,
                    bound,
                    DatasetRole::Development,
                    &input.assessment_manifest,
                )
            })
            .collect::<Result<Vec<_>, _>>()?;
        let plans = plans_of(&applied);
        fold_records.push(FoldRecord {
            fits: applied.iter().map(|applied| applied.fit.clone()).collect(),
            assessments: applied
                .iter()
                .map(|applied| applied.applied.clone())
                .collect(),
        });
        for choice in choices
            .iter_mut()
            .filter(|choice| choice.rejection.is_none())
        {
            let result = match resolve(settings, &members, choice, &plans)? {
                Err(reason) => FoldResult {
                    inapplicable: Some(reason),
                    replay: None,
                    projection: None,
                },
                Ok(policy) => {
                    let (published, projection) = replay_policy(
                        config,
                        local,
                        destination,
                        settings,
                        &policy,
                        DatasetRole::Development,
                        (&fold.decision_start, &fold.decision_end),
                        applied
                            .iter()
                            .map(|applied| applied.input.clone())
                            .collect(),
                        None,
                    )?;
                    FoldResult {
                        inapplicable: None,
                        replay: Some(replay_ref(&published)),
                        projection: Some(projection),
                    }
                }
            };
            choice.folds.push(result);
        }
    }
    for choice in &mut choices {
        choice.aggregate()?;
    }
    let selected = engine::rank(settings, &mut choices, settings.objective)?;
    let passing = choices.iter().filter(|choice| choice.passing()).count() as u64;
    clock.folds = folding.elapsed().as_secs_f64();

    // 3. Only a selected choice is refitted, and only a refitted choice is evaluated.
    let refitting = Instant::now();
    let mut refit = Vec::new();
    let mut frozen = None;
    let mut outer = None;
    let state = match selected {
        None => State::NoFeasiblePolicy,
        Some(index) => {
            let mut plans = BTreeMap::new();
            let mut fits: Vec<(FeatureInstrument, FeatureRef)> = Vec::new();
            for bound in bound.refit {
                let entry = bound.entry.clone();
                let built = features::build(
                    bound.fit,
                    &features_config(config, &entry),
                    local,
                    destination,
                )?;
                refit.push(feature_ref(&built.manifest));
                plans.insert(bound.instrument, built.plan);
                fits.push((entry, feature_ref(&built.manifest)));
            }
            match resolve(settings, &members, &choices[index], &plans)? {
                Err(reason) => State::RefitInapplicable { reason },
                Ok(policy) => {
                    let mut state = State::Selected;
                    if let Some(evaluation) = &settings.evaluation {
                        let mut features = Vec::new();
                        let mut inputs = Vec::new();
                        for (position, manifest) in bound.evaluation.iter().enumerate() {
                            let (entry, fitted) = fits
                                .iter()
                                .find(|(_, fitted)| fitted.instrument == manifest.instrument)
                                .expect("every evaluation instrument has a refit");
                            let uri = &evaluation.inputs[position];
                            let entry = application(
                                DatasetRole::Evaluation,
                                uri,
                                entry,
                                published_uri(destination, &fitted.generation)?,
                            );
                            let applied = features::build(
                                features::resolve(&entry)?,
                                &features_config(config, &entry),
                                local,
                                destination,
                            )?;
                            features.push(feature_ref(&applied.manifest));
                            inputs.push(ReplayInput {
                                tick_manifest: uri.clone(),
                                feature_manifest: published_uri(
                                    destination,
                                    &applied.manifest.generation,
                                )?,
                                outcome_manifest: None,
                            });
                        }
                        let (published, projection) = replay_policy(
                            config,
                            local,
                            destination,
                            settings,
                            &policy,
                            DatasetRole::Evaluation,
                            (&evaluation.decision_start, &evaluation.decision_end),
                            inputs,
                            evaluation.splits.clone(),
                        )?;
                        if let Some(reason) = &projection.failure {
                            state = State::OuterRejected {
                                reason: reason.clone(),
                            };
                        }
                        outer = Some(Outer {
                            features,
                            replay: replay_ref(&published),
                            splits: published.engine.summary().splits.clone(),
                            projection,
                        });
                    }
                    frozen = Some(policy);
                    state
                }
            }
        }
    };
    clock.refit = refitting.elapsed().as_secs_f64();

    // 4. Publish the selection, then its manifest, and verify before it becomes ready.
    let publishing = Instant::now();
    let selection = Selection {
        portfolio: settings.clone(),
        families: family_records,
        members,
        declared,
        rejected,
        valid: declared - rejected,
        passing,
        folds: fold_records,
        choices,
        selected,
        refit,
        frozen,
        outer,
        state,
    };
    let family_generations: Vec<String> = selection
        .families
        .iter()
        .map(|family| family.generation.clone())
        .collect();
    let generation =
        selection_generation_id(&config.content_hash(), CODE_REVISION, &family_generations);
    let key = manifest_key(&generation);
    let temporary = import::temporary_path(local, &format!("selection-{generation}"))?;
    fs::write(&temporary, selection.to_json())
        .map_err(|error| format!("cannot write {}: {error}", temporary.display()))?;
    let identity = store::identify(&temporary)?;
    let mut object = import::record(ObjectRole::Normalized, SELECTION_OBJECT_PATH, &identity);
    local.put_new(&object.key, &temporary, &identity)?;
    let put = destination.put_new(&object.key, &temporary, &identity)?;
    object.crc32c = put.object().crc32c;
    object.generation = put.object().generation;
    fs::remove_file(&temporary)
        .map_err(|error| format!("cannot remove {}: {error}", temporary.display()))?;
    let manifest = SelectionManifest {
        kind: SELECTION_MANIFEST_KIND.to_string(),
        schema_version: SELECTION_SCHEMA_VERSION,
        generation: generation.clone(),
        config_hash: config.content_hash(),
        code_revision: CODE_REVISION.to_string(),
        families: family_generations,
        state: selection.state.status().to_string(),
        objects: vec![object],
    };
    let committed = match destination.head(&key)? {
        Some(_) => {
            let mut bytes = Vec::new();
            destination.read_to(&key, None, &mut bytes)?;
            let committed = SelectionManifest::from_json(&bytes)
                .map_err(|error| format!("{}: {error}", destination.uri(&key)))?;
            if !(committed.generation == manifest.generation
                && committed.families == manifest.families
                && committed.state == manifest.state
                && import::same_objects(&committed.objects, &manifest.objects, &[identity]))
            {
                return Err(format!(
                    "{} records a different generation, families, state, or selection object than this selection produced",
                    destination.uri(&key)
                ));
            }
            bytes
        }
        None => manifest.to_json(),
    };
    let uri = destination.uri(&key);
    let verified = verify_selection(&uri, destination, &key, &committed)?;
    let temporary = import::temporary_path(local, &format!("manifest-{generation}"))?;
    fs::write(&temporary, &committed)
        .map_err(|error| format!("cannot write {}: {error}", temporary.display()))?;
    let identity = store::identify(&temporary)?;
    let put = destination.put_new(&key, &temporary, &identity)?;
    local.put_new(&key, &temporary, &identity)?;
    fs::remove_file(&temporary)
        .map_err(|error| format!("cannot remove {}: {error}", temporary.display()))?;
    clock.publish = publishing.elapsed().as_secs_f64();
    let report = format!(
        "portfolio generation {generation} declared {} rejected {} valid {} passing {} state {} objects 1",
        selection.declared, selection.rejected, selection.valid, selection.passing, manifest.state
    );
    let line = match put {
        Put::Reused(_) => format!("{report} (already published)"),
        Put::Created(_) => format!(
            "{report} [bind {:.3}s folds {:.3}s refit {:.3}s publish {:.3}s]",
            clock.bind, clock.folds, clock.refit, clock.publish
        ),
    };
    Ok(format!("{line}\n{verified}"))
}

// ----------------------------------------------------------------------------------------------
// Verification and the typed reader
// ----------------------------------------------------------------------------------------------

/// One recorded feature generation re-read from the store and checked against the record and
/// the generation it was fitted on or applied to.
fn recorded_feature(
    uri: &str,
    store: &Store,
    record: &FeatureRef,
    role: DatasetRole,
    input: &ManifestUri,
    frozen_from: Option<&str>,
) -> Result<(FeatureManifest, FeaturePlan), String> {
    let location = store.uri(&manifest_key(&record.generation));
    let (feature_store, manifest) = features::feature_manifest(uri, &location)?;
    if manifest.role != role
        || manifest.instrument != record.instrument
        || manifest.input_generation != record.input_generation
        || manifest.input_generation != input.generation()
        || manifest.plan_identity != record.plan_identity
        || manifest.frozen_from.as_deref() != frozen_from
    {
        return Err(format!(
            "{uri}: feature generation {} is not the recorded {role} generation of {} applied to {}",
            record.generation,
            record.instrument,
            input.generation()
        ));
    }
    let plan = features::fitted_plan(uri, &feature_store, &manifest)?;
    Ok((manifest, plan))
}

/// The recorded fit and application of one instrument, as replay input and plan.
fn recorded_input(
    uri: &str,
    store: &Store,
    fit: &FeatureRef,
    fit_input: &ManifestUri,
    applied: &FeatureRef,
    applied_role: DatasetRole,
    applied_input: &ManifestUri,
) -> Result<(String, FeaturePlan, ReplayInput), String> {
    let (_, plan) = recorded_feature(uri, store, fit, DatasetRole::Development, fit_input, None)?;
    if applied.plan_identity != fit.plan_identity || applied.instrument != fit.instrument {
        return Err(format!(
            "{uri}: feature generation {} does not apply the fitted plan {} of {}",
            applied.generation, fit.plan_identity, fit.instrument
        ));
    }
    recorded_feature(
        uri,
        store,
        applied,
        applied_role,
        applied_input,
        Some(&fit.generation),
    )?;
    Ok((
        fit.instrument.clone(),
        plan,
        ReplayInput {
            tick_manifest: applied_input.clone(),
            feature_manifest: store.uri(&manifest_key(&applied.generation)).parse()?,
            outcome_manifest: None,
        },
    ))
}

/// Restores one recorded replay through its verifier and checks that it ran exactly `table`.
fn recorded_replay(
    uri: &str,
    store: &Store,
    record: &ReplayRef,
    table: &Replay,
) -> Result<replay::Restored, String> {
    let key = manifest_key(&record.generation);
    let mut bytes = Vec::new();
    store.read_to(&key, None, &mut bytes)?;
    let location = store.uri(&key);
    let restored = replay::restore_verified(&location, store, &key, &bytes)?;
    if restored.manifest.summary_identity != record.summary_identity
        || restored.engine.definition().replay != *table
    {
        return Err(format!(
            "{uri}: {location} is not the joint replay of the recorded policy and fold"
        ));
    }
    Ok(restored)
}

/// The verified selection: the manifest, the selection re-derived from its recorded procedure,
/// and the selection object's byte count. Every referenced family, feature generation, and
/// replay is restored through its own verifier; enumeration, structure, resolution,
/// projections, gates, ranking, the frozen policy, and the terminal state are recomputed and
/// compared field by field.
pub(crate) fn verified_selection(
    uri: &str,
    store: &Store,
    key: &str,
    bytes: &[u8],
) -> Result<(SelectionManifest, Selection, usize), String> {
    let manifest =
        SelectionManifest::from_json(bytes).map_err(|error| format!("{uri}: {error}"))?;
    if manifest.key() != key {
        return Err(format!(
            "{uri} holds the manifest of generation {}",
            manifest.generation
        ));
    }
    let selection_bytes = search::read_object(store, &manifest.objects, SELECTION_OBJECT_PATH)?;
    let selection = Selection::from_json(&selection_bytes)
        .map_err(|error| format!("{uri}: {SELECTION_OBJECT_PATH}: {error}"))?;
    let settings = &selection.portfolio;
    settings
        .validate()
        .map_err(|reason| format!("{uri}: portfolio.{reason}"))?;
    if manifest.state != selection.state.status() {
        return Err(format!(
            "{uri}: the manifest records state `{}` but the selection is `{}`",
            manifest.state,
            selection.state.status()
        ));
    }
    // The universe: verified families, their records, the logical members, and every choice.
    let (families, family_records) = families(settings)?;
    if family_records != selection.families
        || manifest.families
            != family_records
                .iter()
                .map(|family| family.generation.clone())
                .collect::<Vec<_>>()
    {
        return Err(format!(
            "{uri}: the recorded families are not the bound development-only families"
        ));
    }
    let members = engine::logical_members(settings, &families)?;
    if members != selection.members {
        return Err(format!(
            "{uri}: the recorded members are not the logical universe of the configuration"
        ));
    }
    let mut expected = choices(settings, &members)?;
    let declared = engine::declared_count(settings)?;
    let rejected = expected
        .iter()
        .filter(|choice| choice.rejection.is_some())
        .count() as u64;
    if expected.len() != selection.choices.len()
        || selection.declared != declared
        || selection.rejected != rejected
        || selection.valid != declared - rejected
    {
        return Err(format!(
            "{uri}: the recorded {} choices ({} declared, {} rejected, {} valid) are not the enumeration ({declared} declared, {rejected} rejected)",
            selection.choices.len(),
            selection.declared,
            selection.rejected,
            selection.valid
        ));
    }
    // Every fold: the recorded fits and assessments re-read, every valid choice re-resolved,
    // and every recorded replay restored and projected again.
    if selection.folds.len() != settings.folds.len() {
        return Err(format!(
            "{uri}: {} fold records for {} configured folds",
            selection.folds.len(),
            settings.folds.len()
        ));
    }
    for (index, (fold, record)) in settings.folds.iter().zip(&selection.folds).enumerate() {
        if record.fits.len() != fold.inputs.len() || record.assessments.len() != fold.inputs.len() {
            return Err(format!(
                "{uri}: fold {index} records {} fits and {} assessments for {} inputs",
                record.fits.len(),
                record.assessments.len(),
                fold.inputs.len()
            ));
        }
        let mut plans = BTreeMap::new();
        let mut inputs = Vec::with_capacity(fold.inputs.len());
        for ((input, fit), applied) in fold
            .inputs
            .iter()
            .zip(&record.fits)
            .zip(&record.assessments)
        {
            let (instrument, plan, replay_input) = recorded_input(
                uri,
                store,
                fit,
                &input.fit.input_manifest,
                applied,
                DatasetRole::Development,
                &input.assessment_manifest,
            )?;
            plans.insert(instrument, plan);
            inputs.push(replay_input);
        }
        for (position, (choice, recorded)) in
            expected.iter_mut().zip(&selection.choices).enumerate()
        {
            let mismatch = || {
                format!(
                    "{uri}: choice {position} records a fold {index} result its resolution and replay do not produce"
                )
            };
            if choice.rejection.is_some() {
                if !recorded.folds.is_empty() {
                    return Err(mismatch());
                }
                continue;
            }
            let Some(fold_result) = recorded.folds.get(index) else {
                return Err(mismatch());
            };
            let result = match resolve(settings, &members, choice, &plans)? {
                Err(reason) => FoldResult {
                    inapplicable: Some(reason),
                    replay: None,
                    projection: None,
                },
                Ok(policy) => {
                    let Some(replay) = &fold_result.replay else {
                        return Err(mismatch());
                    };
                    let table = engine::replay_table(
                        settings,
                        &policy,
                        DatasetRole::Development,
                        &fold.decision_start,
                        &fold.decision_end,
                        inputs.clone(),
                        None,
                    );
                    let restored = recorded_replay(uri, store, replay, &table)?;
                    FoldResult {
                        inapplicable: None,
                        replay: Some(replay.clone()),
                        projection: Some(engine::project(&restored.engine, &settings.gates)?),
                    }
                }
            };
            if result != *fold_result {
                return Err(mismatch());
            }
            choice.folds.push(result);
        }
    }
    for choice in &mut expected {
        choice.aggregate()?;
    }
    let selected = engine::rank(settings, &mut expected, settings.objective)?;
    let passing = expected.iter().filter(|choice| choice.passing()).count() as u64;
    for (position, (choice, recorded)) in expected.iter().zip(&selection.choices).enumerate() {
        if choice != recorded {
            return Err(format!(
                "{uri}: choice {position} records an identity, structure, folds, aggregate, or rank the procedure does not produce"
            ));
        }
    }
    if selected != selection.selected || passing != selection.passing {
        return Err(format!(
            "{uri}: the recorded selection is not the ranking's"
        ));
    }
    // The refit, the frozen policy, the outer evaluation, and the terminal state.
    let state = match selected {
        None => {
            if !selection.refit.is_empty()
                || selection.frozen.is_some()
                || selection.outer.is_some()
            {
                return Err(format!(
                    "{uri}: a selection without a feasible policy records a refit, a frozen policy, or an outer result"
                ));
            }
            State::NoFeasiblePolicy
        }
        Some(index) => {
            if selection.refit.len() != settings.refit.fits.len() {
                return Err(format!(
                    "{uri}: {} refit records for {} configured fits",
                    selection.refit.len(),
                    settings.refit.fits.len()
                ));
            }
            let mut plans = BTreeMap::new();
            for (entry, record) in settings.refit.fits.iter().zip(&selection.refit) {
                let (_, plan) = recorded_feature(
                    uri,
                    store,
                    record,
                    DatasetRole::Development,
                    &entry.input_manifest,
                    None,
                )?;
                plans.insert(record.instrument.clone(), plan);
            }
            match resolve(settings, &members, &expected[index], &plans)? {
                Err(reason) => {
                    if selection.frozen.is_some() || selection.outer.is_some() {
                        return Err(format!(
                            "{uri}: an inapplicable refit records a frozen policy or an outer result"
                        ));
                    }
                    State::RefitInapplicable { reason }
                }
                Ok(policy) => {
                    if selection.frozen.as_ref() != Some(&policy) {
                        return Err(format!(
                            "{uri}: the frozen policy is not the selected choice resolved under the refit"
                        ));
                    }
                    match (&settings.evaluation, &selection.outer) {
                        (None, None) => State::Selected,
                        (Some(evaluation), Some(outer)) => {
                            if outer.features.len() != evaluation.inputs.len() {
                                return Err(format!(
                                    "{uri}: {} outer feature records for {} evaluation inputs",
                                    outer.features.len(),
                                    evaluation.inputs.len()
                                ));
                            }
                            let mut inputs = Vec::with_capacity(evaluation.inputs.len());
                            for (input, applied) in evaluation.inputs.iter().zip(&outer.features) {
                                let fit = selection
                                    .refit
                                    .iter()
                                    .find(|fit| fit.instrument == applied.instrument)
                                    .ok_or_else(|| {
                                        format!(
                                            "{uri}: outer feature generation {} applies no refit",
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
                                    .expect("a recorded refit has its entry");
                                let (_, _, replay_input) = recorded_input(
                                    uri,
                                    store,
                                    fit,
                                    &entry.input_manifest,
                                    applied,
                                    DatasetRole::Evaluation,
                                    input,
                                )?;
                                inputs.push(replay_input);
                            }
                            let table = engine::replay_table(
                                settings,
                                &policy,
                                DatasetRole::Evaluation,
                                &evaluation.decision_start,
                                &evaluation.decision_end,
                                inputs,
                                evaluation.splits.clone(),
                            );
                            let restored = recorded_replay(uri, store, &outer.replay, &table)?;
                            let projection = engine::project(&restored.engine, &settings.gates)?;
                            if projection != outer.projection
                                || restored.engine.summary().splits != outer.splits
                            {
                                return Err(format!(
                                    "{uri}: the outer result is not the projection of its replay"
                                ));
                            }
                            match &projection.failure {
                                Some(reason) => State::OuterRejected {
                                    reason: reason.clone(),
                                },
                                None => State::Selected,
                            }
                        }
                        _ => {
                            return Err(format!(
                                "{uri}: the outer result does not match the configured evaluation"
                            ));
                        }
                    }
                }
            }
        }
    };
    if state != selection.state {
        return Err(format!(
            "{uri}: the recorded state `{}` is not the procedure's `{}`",
            selection.state.status(),
            state.status()
        ));
    }
    Ok((manifest, selection, selection_bytes.len()))
}

/// Verifies a selection generation and writes its report line.
pub fn verify_selection(
    uri: &str,
    store: &Store,
    key: &str,
    bytes: &[u8],
) -> Result<String, String> {
    let (manifest, selection, bytes) = verified_selection(uri, store, key, bytes)?;
    Ok(format!(
        "verified portfolio generation {} declared {} rejected {} valid {} passing {} state {} objects 1 bytes {bytes}",
        manifest.generation,
        selection.declared,
        selection.rejected,
        selection.valid,
        selection.passing,
        manifest.state
    ))
}

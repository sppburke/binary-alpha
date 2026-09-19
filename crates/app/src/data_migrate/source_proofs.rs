//! Per-source retirement authority. An interval is deliberately exact, so it remains a
//! reconstruction recipe after the legacy objects have been removed. Divergent sources
//! remain ordinary retained and archived closures, never casualties of newest selection.
use super::*;

pub(super) fn observation(
    local: &Store,
    source: &GenerationManifest,
    target: &GenerationManifest,
) -> Result<Value, String> {
    let range = (
        time(&source.coverage.first_event_time)?,
        time(&source.coverage.last_event_time)?,
    );
    let original = observation_proof(local, source)?;
    let replacement = observation_proof_range(local, target, Some(range))?;
    Ok(json!({
        "equal": original == replacement,
        "interval_inclusive": [range.0, range.1],
        "original": original, "replacement": replacement,
    }))
}

pub(super) fn stream(
    layout: &Layout,
    source: &GenerationManifest,
    target: &GenerationManifest,
    prior: &StreamManifest,
    work: &Path,
) -> Result<Value, String> {
    let reconstruction = crate::session_migration::reconstruct_from(
        &layout.store(),
        target,
        source,
        &prior.definition,
        work,
    )?;
    let profile = prior
        .objects
        .iter()
        .find(|o| o.path == "profile.json")
        .ok_or("legacy stream lacks profile")?;
    let original_profile: Value =
        serde_json::from_slice(&fs::read(object_path(layout, profile)?).map_err(err)?)
            .map_err(err)?;
    let original = json!({
        "sha256": candle_digest(layout, prior)?, "streams": prior.streams,
        "profile": normalize_profile(original_profile),
    });
    let reconstructed = json!({
        "sha256": reconstruction.digest, "streams": reconstruction.streams,
        "profile": normalize_profile(serde_json::to_value(reconstruction.profile).map_err(err)?),
    });
    Ok(json!({
        "equal": original == reconstructed, "source_generation": source.generation,
        "definition": prior.definition, "original": original, "reconstructed": reconstructed,
    }))
}

fn normalize_profile(mut profile: Value) -> Value {
    if let Some(calculations) = profile["calculations"].as_array_mut() {
        for calculation in calculations {
            if let Some(reason) = calculation["reason"].as_str()
                && let Ok(value) = serde_json::from_str::<Value>(reason)
            {
                calculation["reason"] = value;
            }
        }
    }
    profile
}

pub(super) fn prove(
    layout: &Layout,
    state: &State,
    access: Access<'_>,
    work: &Path,
) -> Result<Value, String> {
    let local = layout.store();
    let target = read_manifest(&local, &state.dataset)?.0;
    let mapping: lineage::MigrationMapping =
        serde_json::from_value(lineage::read_lineage(&local, &target)?).map_err(err)?;
    let mut datasets = BTreeMap::new();
    for generation in &mapping.v1_generations {
        verify::run_with(&layout.manifest_uri(generation), access)?;
        let source = read_manifest(&local, generation)?.0;
        datasets.insert(generation.clone(), observation(&local, &source, &target)?);
    }
    let mut streams = BTreeMap::new();
    for generation in mapping.streams() {
        verify::run_with(&layout.manifest_uri(&generation), access)?;
        let prior = read_stream(layout, &generation)?;
        let proof = if datasets
            .get(&prior.source_generation)
            .is_some_and(|proof| proof["equal"] == true)
        {
            let source = read_manifest(&local, &prior.source_generation)?.0;
            stream(layout, &source, &target, &prior, work)?
        } else {
            json!({"equal":false,"source_generation":prior.source_generation,
                "reason":"source observation interval is not exactly reconstructible"})
        };
        streams.insert(generation, proof);
    }
    Ok(
        json!({"target_root":state.dataset,"target_stream":state.stream,
        "datasets":datasets,"streams":streams}),
    )
}

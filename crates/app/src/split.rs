//! Whole-day observation subsets and their research governance declaration.

use std::collections::BTreeSet;
use std::io::Write;
use std::path::Path;

use binary_alpha_engine::config::{ManifestUri, PublicationUri};
use binary_alpha_engine::dataset::coverage::{CoverageRange, DailyCoverage};
use binary_alpha_engine::dataset::daily::{DAY_MICROS, DayFamily, Layout, day_bounds};
use binary_alpha_engine::dataset::{Coverage, DatasetRole, GenerationManifest, Input};
use binary_alpha_engine::market::format_event_time_micros as time;
use binary_alpha_engine::research::{Declaration, Population};
use serde_json::json;
use sha2::{Digest, Sha256};

use crate::store::{self, Store};
use crate::{daily, data_pipeline, import, lineage, research, verify};

struct Planned {
    source: ManifestUri,
    manifest: GenerationManifest,
    coverage: DailyCoverage,
    window: CoverageRange,
}

/// Plans every source and window before retaining or publishing any slice.
pub fn run(config_path: &Path, out: &mut dyn Write) -> Result<(), String> {
    let config = crate::load_config(config_path)?;
    let split = config.split.as_ref().ok_or("split table is required")?;
    let windows = split.windows()?;
    let base = config_path.parent().unwrap_or(Path::new("."));
    let retained = base.join(config.storage.historical_data_dir.as_path());
    let retained = resolve(&PublicationUri::Filesystem(retained))?;
    let destination_uri = resolve(&config.storage.publication_uri)?;
    let sources = split
        .sources
        .iter()
        .map(|source| resolve(&source.root))
        .collect::<Result<Vec<_>, _>>()?;
    // The declaration lands beneath the namespace, so that location is checked as well.
    let namespace_uri = resolve(&match &destination_uri {
        PublicationUri::Filesystem(path) => PublicationUri::Filesystem(path.join(&split.namespace)),
        PublicationUri::GoogleCloudStorage { bucket, prefix } => {
            PublicationUri::GoogleCloudStorage {
                bucket: bucket.clone(),
                prefix: if prefix.is_empty() {
                    split.namespace.clone()
                } else {
                    format!("{prefix}/{}", split.namespace)
                },
            }
        }
    })?;
    for target in [&retained, &destination_uri, &namespace_uri] {
        if let PublicationUri::Filesystem(path) = target
            && path.ancestors().any(data_pipeline::is_managed_store)
        {
            return Err(
                "split: retained folder or destination lies at or below a managed pipeline store"
                    .into(),
            );
        }
        if sources
            .iter()
            .any(|source| within(target, source) || within(source, target))
        {
            return Err("split: retained folder or destination overlaps a source store".into());
        }
    }
    let mut planned = Vec::new();
    let mut instruments = BTreeSet::new();
    for source in &split.sources {
        let (store, key) = verify::open(&source.to_string())?;
        let bytes = research::read_key(&store, &key)?;
        let root = GenerationManifest::from_json(&bytes)?;
        if root.key() != key {
            return Err("split: source manifest key disagrees with its generation".into());
        }
        if root.role != DatasetRole::Development || root.layout != Some(Layout::DailyV2) {
            return Err("split: sources must be development daily-v2 roots".into());
        }
        if !instruments.insert(root.instrument.clone()) {
            return Err("split: sources must have distinct instruments".into());
        }
        let coverage = lineage::read_coverage(&store, &root)?;
        let partitions = daily::observation_partitions(&root)?;
        for (role, window) in &windows {
            let (start, end) = window.bounds()?;
            let mut manifest = root.clone();
            manifest.role = *role;
            manifest.day_inventory.retain(|day| {
                day.family == DayFamily::Observations
                    && day_bounds(&day.date).is_ok_and(|(day, _)| (start..end).contains(&day))
            });
            manifest.row_count = manifest.day_inventory.iter().try_fold(0_u64, |rows, day| {
                rows.checked_add(day.rows)
                    .ok_or("split: row count overflow")
            })?;
            if manifest.row_count == 0 {
                return Err(format!(
                    "split: {} {role} window {} to {} has no rows",
                    root.instrument, window.start, window.end
                ));
            }
            manifest.coverage = Coverage {
                first_event_time: manifest
                    .day_inventory
                    .iter()
                    .find_map(|day| day.first_time.clone())
                    .ok_or("split: first event absent")?,
                last_event_time: manifest
                    .day_inventory
                    .iter()
                    .rev()
                    .find_map(|day| day.last_time.clone())
                    .ok_or("split: last event absent")?,
            };
            let dates: BTreeSet<_> = manifest
                .day_inventory
                .iter()
                .map(|day| day.date.as_str())
                .collect();
            manifest.objects = partitions
                .iter()
                .filter(|(_, day)| day.is_some_and(|day| dates.contains(day.date.as_str())))
                .map(|(object, _)| (*object).clone())
                .collect();
            let mut coverage = coverage.clone();
            coverage.role = *role;
            coverage.days.retain(|day| {
                day.family == DayFamily::Observations && dates.contains(day.date.as_str())
            });
            let acquisitions: BTreeSet<_> = coverage
                .days
                .iter()
                .flat_map(|day| &day.acquisition_ids)
                .collect();
            coverage
                .acquisitions
                .retain(|a| acquisitions.contains(&a.acquisition_id));
            manifest.config_hash = config.content_hash();
            manifest.code_revision = import::CODE_REVISION.into();
            manifest.inputs = vec![Input {
                path: source.to_string(),
                bytes: bytes.len() as u64,
                sha256: binary_alpha_engine::hex(&Sha256::digest(&bytes)),
            }];
            planned.push(Planned {
                source: source.clone(),
                manifest,
                coverage,
                window: window.clone(),
            });
        }
    }
    let PublicationUri::Filesystem(retained) = retained else {
        unreachable!()
    };
    let local = Store::filesystem(retained);
    let destination = Store::open(&destination_uri)?;
    let mut populations = Vec::new();
    for Planned {
        source,
        mut manifest,
        coverage,
        window,
    } in planned
    {
        let (source_store, _) = verify::open(&source.to_string())?;
        for object in &manifest.objects {
            let (_, file) = verify::fetch(&source_store, object, true)?;
            let file = file.expect("retained observation");
            local.put_new(&object.key, &file.path, &store::identify(&file.path)?)?;
        }
        manifest.objects.push(lineage::metadata(
            &local,
            "provenance/coverage.json",
            &coverage,
        )?);
        manifest.objects.push(lineage::metadata(&local, "provenance/lineage.json", &json!({
            "schema_version": 1, "kind": "split",
            "source": {"uri": source, "generation": source.generation(), "manifest_sha256": manifest.inputs[0].sha256},
            "role": manifest.role, "window": window,
        }))?);
        lineage::identify(&mut manifest);
        manifest = GenerationManifest::from_json(&manifest.to_json())?;
        coverage.check_manifest(&manifest)?;
        let read = daily::read_generation_lossless(&local, &manifest, |_| Ok(()))?;
        if read.data.rows != manifest.row_count
            || read.data.first_event_micros.map(time).as_ref()
                != Some(&manifest.coverage.first_event_time)
            || read.data.last_event_micros.map(time).as_ref()
                != Some(&manifest.coverage.last_event_time)
        {
            return Err("split: retained observations disagree with rows or coverage".into());
        }
        let identities = manifest
            .objects
            .iter()
            .map(|object| store::identify(&local.local_path(&object.key).expect("retained folder")))
            .collect::<Result<Vec<_>, _>>()?;
        let publication = import::publish_generation(manifest, &identities, &local, &destination)?;
        let manifest = publication.manifest;
        writeln!(
            out,
            "published {} {} generation {} rows {} objects {} reused {}",
            manifest.instrument,
            manifest.role,
            manifest.generation,
            manifest.row_count,
            manifest.objects.len(),
            publication.reused
        )
        .and_then(|()| out.flush())
        .map_err(|e| e.to_string())?;
        let (start, end) = window.bounds()?;
        let tokens = (start..end)
            .step_by(DAY_MICROS as usize)
            .map(|day| format!("{}:{}", manifest.instrument, &time(day)[..10]))
            .collect();
        populations.push(Population {
            id: manifest.generation.clone(),
            role: manifest.role,
            instrument: manifest.instrument,
            source: source.generation().into(),
            coverage: manifest.coverage,
            generations: vec![manifest.generation],
            tokens,
            exposure: vec![],
        });
    }
    let declaration = Declaration {
        schema_version: 1,
        operator: std::env::var("USER").unwrap_or_else(|_| "unavailable".into()),
        root: config.storage.publication_uri,
        namespace: split.namespace.clone(),
        populations,
    };
    let bytes = binary_alpha_engine::research::to_json(&declaration);
    Declaration::from_json(&bytes)?;
    let key = declaration.key(&format!("declaration-{}.json", declaration.identity()));
    research::publish_record(&local, &destination, &key, &bytes)?;
    writeln!(out, "declaration {}", destination.uri(&key)).map_err(|e| e.to_string())
}

fn resolve(uri: &PublicationUri) -> Result<PublicationUri, String> {
    match uri {
        PublicationUri::Filesystem(path) => Ok(PublicationUri::Filesystem(
            data_pipeline::import_destination(
                std::env::current_dir()
                    .map_err(|e| e.to_string())?
                    .join(path),
            )?,
        )),
        _ => Ok(uri.clone()),
    }
}

fn within(target: &PublicationUri, source: &PublicationUri) -> bool {
    match (target, source) {
        (PublicationUri::Filesystem(target), PublicationUri::Filesystem(source)) => {
            target.starts_with(source)
        }
        (
            PublicationUri::GoogleCloudStorage {
                bucket: a,
                prefix: target,
            },
            PublicationUri::GoogleCloudStorage {
                bucket: b,
                prefix: source,
            },
        ) => {
            a == b
                && (source.is_empty()
                    || target == source
                    || target.starts_with(&format!("{source}/")))
        }
        _ => false,
    }
}

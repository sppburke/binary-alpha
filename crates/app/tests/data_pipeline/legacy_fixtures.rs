//! One-time construction of historical migration inputs from synthetic acquisition fixtures.
//! Only these test helpers write legacy manifests or mutate the invented archive inventory.
use super::*;
use binary_alpha_engine::dataset::{ObjectRole, manifest_key};
use std::collections::BTreeSet;

pub(super) fn freeze(f: &Fixture) -> BTreeMap<String, String> {
    let root = f.scratch.path("producer/store");
    let state = f.scratch.path("producer/pipeline_state");
    fs::create_dir_all(state.join("records")).unwrap();
    let store = binary_alpha_app::store::Store::filesystem(&root);
    let generations = store.list_manifests().unwrap();
    let mut mapping = BTreeMap::new();
    let mut legacy = BTreeMap::new();
    let mut current = vec![];
    let mut streams = vec![];
    let mut original_keys = BTreeSet::new();
    for id in &generations {
        let bytes = fs::read(root.join(manifest_key(id))).unwrap();
        let value: Value = serde_json::from_slice(&bytes).unwrap();
        for o in value["objects"].as_array().unwrap() {
            original_keys.insert(o["key"].as_str().unwrap().to_string());
        }
        if verify::manifest_kind(&bytes).unwrap().is_some() {
            streams.push(StreamManifest::from_json(&bytes).unwrap());
        } else {
            current.push(GenerationManifest::from_json(&bytes).unwrap());
        }
    }
    // Imports establish seed identities before histories bind them.
    current
        .sort_by_key(|m| m.source_kind == binary_alpha_engine::dataset::SourceKind::BrokerHistory);
    for m in &current {
        if m.layout.is_none() {
            continue;
        }
        let mut old = common::legacy::dataset(&root, m);
        if old.source_kind == binary_alpha_engine::dataset::SourceKind::BrokerHistory {
            let first_key = old.key();
            let mut cov = coverage(&root, &old);
            if let Some(seed) = &mut cov.seed {
                seed.generation = mapping
                    .get(&seed.generation)
                    .cloned()
                    .unwrap_or(seed.generation.clone());
            }
            let tmp = root.join("fixture-coverage.json");
            fs::write(&tmp, serde_json::to_vec(&cov).unwrap()).unwrap();
            let replaced: Vec<String> = old
                .objects
                .iter()
                .filter(|o| o.path == "provenance/coverage.json")
                .map(|o| o.key.clone())
                .collect();
            old.objects.retain(|o| o.path != "provenance/coverage.json");
            old.objects.push(common::daily::object(
                &root,
                "provenance/coverage.json",
                ObjectRole::Provenance,
                &tmp,
            ));
            fs::remove_file(tmp).unwrap();
            common::daily::publish(&root, &mut old);
            if first_key != old.key() {
                fs::remove_dir_all(root.join(first_key).parent().unwrap()).unwrap();
            }
            // The remapped coverage replaces the intermediate object; a fixture leaves no
            // unreferenced physical object behind for the census or retirement to explain.
            for key in replaced {
                if !old.objects.iter().any(|o| o.key == key) {
                    fs::remove_file(root.join(&key)).unwrap();
                }
            }
        }
        mapping.insert(m.generation.clone(), old.generation.clone());
        legacy.insert(old.generation.clone(), old);
    }
    for s in streams {
        let Some(id) = mapping.get(&s.source_generation) else {
            continue;
        };
        let config = f.scratch.path(if s.broker.as_str() == "deriv" {
            "deriv-import.toml"
        } else {
            "pocket-import.toml"
        });
        let old = common::legacy::stream(&config, &root, &legacy[id]);
        mapping.insert(s.generation, old.generation);
    }
    fn replace(v: &mut Value, mapping: &BTreeMap<String, String>) {
        match v {
            Value::String(s) => {
                for (a, b) in mapping {
                    *s = s.replace(a, b);
                }
            }
            Value::Array(a) => a.iter_mut().for_each(|v| replace(v, mapping)),
            Value::Object(o) => o.values_mut().for_each(|v| replace(v, mapping)),
            _ => (),
        }
    }
    let mut records = vec![];
    if let Ok(entries) = fs::read_dir(state.join("records")) {
        for e in entries {
            let path = e.unwrap().path();
            if !path.is_file() || path.extension().is_none_or(|e| e != "json") {
                continue;
            }
            let mut v = read_json(&path);
            replace(&mut v, &mapping);
            if let Some(m) = v["dataset_generation"]
                .as_str()
                .and_then(|id| legacy.get(id))
            {
                v["coverage"] = serde_json::to_value(coverage(&root, m)).unwrap();
            }
            records.push((path, v));
        }
    }
    let mut remote = f.drive.state.lock().unwrap();
    let catalogs: Vec<_> = remote
        .files
        .iter()
        .filter_map(|(id, e)| Catalog::from_json(&e.bytes).ok().map(|c| (id.clone(), c)))
        .collect();
    remote.files.clear();
    remote.sessions.clear();
    let mut receipts = BTreeMap::new();
    let mut transfers: BTreeMap<String, BTreeMap<String, Value>> = BTreeMap::new();
    for (id, mut catalog) in catalogs {
        let old = &legacy[&mapping[&catalog.dataset.generation]];
        let s = stream(&root, &mapping[&catalog.stream.generation]);
        catalog.layout = None;
        catalog.records.clear();
        catalog.lineage_manifests.clear();
        catalog.objects.clear();
        for o in old.objects.iter().chain(&s.objects) {
            if catalog.objects.iter().any(|x| x.key == o.key) {
                continue;
            }
            let file_id = format!("fixture-object-{}", o.sha256);
            remote.files.insert(
                file_id.clone(),
                RemoteEntry {
                    name: format!("object-{}", o.sha256),
                    bytes: fs::read(root.join(&o.key)).unwrap(),
                    trashed: false,
                },
            );
            catalog.objects.push(data_pipeline::ObjectEntry {
                key: o.key.clone(),
                sha256: o.sha256.clone(),
                bytes: o.bytes,
                file_id,
            });
        }
        for (g, entry) in [
            (&old.generation, &mut catalog.dataset),
            (&s.generation, &mut catalog.stream),
        ] {
            let key = manifest_key(g);
            let bytes = fs::read(root.join(&key)).unwrap();
            let hash = binary_alpha_engine::hex(&Sha256::digest(&bytes));
            let file_id = format!("fixture-manifest-{g}");
            *entry = data_pipeline::ManifestEntry {
                generation: g.clone(),
                key,
                sha256: hash,
                bytes: bytes.len() as u64,
                file_id: file_id.clone(),
            };
            remote.files.insert(
                file_id,
                RemoteEntry {
                    name: format!("manifest-{g}.json"),
                    bytes,
                    trashed: false,
                },
            );
        }
        let bytes = serde_json::to_vec(&catalog).unwrap();
        receipts.insert(id.clone(),json!({"file_id":id,"bytes":bytes.len(),"sha256":binary_alpha_engine::hex(&Sha256::digest(&bytes))}));
        remote.files.insert(
            id,
            RemoteEntry {
                name: format!(
                    "catalog-{}-{}.json",
                    &old.generation[..16],
                    &s.generation[..16]
                ),
                bytes,
                trashed: false,
            },
        );
        let transfer = transfers.entry(catalog.job).or_default();
        for e in &catalog.objects {
            transfer.insert(
                e.key.clone(),
                json!({"file_id":e.file_id,"done":true,"session":null}),
            );
        }
        for e in [&catalog.dataset, &catalog.stream] {
            transfer.insert(
                e.key.clone(),
                json!({"file_id":e.file_id,"done":true,"session":null}),
            );
        }
    }
    drop(remote);
    for (path, mut v) in records {
        if let Some(receipt) = v["file_id"].as_str().and_then(|id| receipts.get(id)) {
            v = receipt.clone();
        }
        let mut name = path.file_name().unwrap().to_string_lossy().to_string();
        if name.contains("-catalog-") {
            for (a, b) in &mapping {
                name = name.replace(&a[..16], &b[..16]);
            }
        }
        let target = path.with_file_name(name);
        fs::write(&target, serde_json::to_vec(&v).unwrap()).unwrap();
        if target != path {
            fs::remove_file(path).unwrap();
        }
    }
    if state.join("registry").exists() {
        fs::remove_dir_all(state.join("registry")).unwrap();
    }
    for (job, files) in transfers {
        fs::write(
            state.join(job).join("transfers.json"),
            json!({"files":files}).to_string(),
        )
        .unwrap();
    }
    // Remove current fixture payloads only after their complete legacy replacements exist.
    for id in mapping.keys() {
        fs::remove_dir_all(root.join(manifest_key(id)).parent().unwrap()).unwrap();
    }
    let mut retained = BTreeSet::new();
    for g in store.list_manifests().unwrap() {
        let v = read_json(&root.join(manifest_key(&g)));
        for o in v["objects"].as_array().unwrap() {
            retained.insert(o["key"].as_str().unwrap().to_string());
        }
    }
    for m in legacy
        .values()
        .filter(|m| m.source_kind == binary_alpha_engine::dataset::SourceKind::BrokerHistory)
    {
        for p in coverage(&root, m).pages {
            retained.insert(binary_alpha_engine::dataset::object_key(&p.sha256));
        }
    }
    for key in original_keys {
        if !retained.contains(&key) && root.join(&key).exists() {
            fs::remove_file(root.join(key)).unwrap();
        }
    }
    // Update configurations and pending baselines remain historical evidence, never CLI inputs.
    for entry in fs::read_dir(&state).into_iter().flatten().flatten() {
        if !entry.path().is_dir() {
            continue;
        }
        for file in fs::read_dir(entry.path()).unwrap().flatten() {
            if file.file_name().to_string_lossy().starts_with("reclaim-") {
                fs::remove_file(file.path()).unwrap();
            }
        }
        let p = entry.path().join("progress.json");
        if p.exists() {
            let mut v = read_json(&p);
            replace(&mut v, &mapping);
            fs::write(p, v.to_string()).unwrap();
        }
        let p = entry.path().join("update.toml");
        if p.exists() {
            let mut text = fs::read_to_string(&p).unwrap();
            for (a, b) in &mapping {
                text = text.replace(a, b);
            }
            fs::write(p, text).unwrap();
        }
    }
    mapping
}

/// A legacy archive reader fixture with its own manifest/object bindings. Current pipeline
/// tests may call this after their v2 assertions to exercise historical archive corruption.
pub(super) fn catalog(f: &Fixture, catalog_id: &str) {
    let files = f.drive.files();
    let mut catalog = Catalog::from_json(&files[catalog_id].bytes).unwrap();
    let root = f.scratch.path("legacy-reader-store");
    fs::create_dir_all(root.join("objects")).unwrap();
    for o in &catalog.objects {
        fs::write(root.join(&o.key), &files[&o.file_id].bytes).unwrap();
    }
    let current = GenerationManifest::from_json(&files[&catalog.dataset.file_id].bytes).unwrap();
    let current_stream = StreamManifest::from_json(&files[&catalog.stream.file_id].bytes).unwrap();
    let legacy = common::legacy::dataset(&root, &current);
    let config = f.scratch.path("legacy-reader.toml");
    fs::write(&config,toml::to_string(&json!({"schema_version":1,"run_mode":"research",
        "storage":{"historical_data_dir":root,"publication_uri":format!("file://{}",root.display())},
        "instruments":[current_stream.definition]})).unwrap()).unwrap();
    let stream = common::legacy::stream(&config, &root, &legacy);
    catalog.layout = None;
    catalog.lineage_manifests.clear();
    catalog.records.clear();
    catalog.objects.clear();
    let mut remote = f.drive.state.lock().unwrap();
    for o in legacy.objects.iter().chain(&stream.objects) {
        let id = format!("legacy-reader-object-{}", o.sha256);
        remote.files.insert(
            id.clone(),
            RemoteEntry {
                name: format!("object-{}", o.sha256),
                bytes: fs::read(root.join(&o.key)).unwrap(),
                trashed: false,
            },
        );
        catalog.objects.push(data_pipeline::ObjectEntry {
            key: o.key.clone(),
            file_id: id,
            bytes: o.bytes,
            sha256: o.sha256.clone(),
        });
    }
    for (generation, e) in [
        (&legacy.generation, &mut catalog.dataset),
        (&stream.generation, &mut catalog.stream),
    ] {
        let key = manifest_key(generation);
        let bytes = fs::read(root.join(&key)).unwrap();
        let id = format!("legacy-reader-manifest-{generation}");
        *e = data_pipeline::ManifestEntry {
            generation: generation.clone(),
            key,
            bytes: bytes.len() as u64,
            sha256: binary_alpha_engine::hex(&Sha256::digest(&bytes)),
            file_id: id.clone(),
        };
        remote.files.insert(
            id,
            RemoteEntry {
                name: format!("manifest-{generation}.json"),
                bytes,
                trashed: false,
            },
        );
    }
    remote.files.get_mut(catalog_id).unwrap().bytes = serde_json::to_vec(&catalog).unwrap();
}

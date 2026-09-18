//! One non-live sequence crosses every daily owner, including recovery and reachability.
use super::*;
use binary_alpha_app::retire::Plan;
use binary_alpha_engine::dataset::Layout;
use std::collections::{BTreeMap, BTreeSet};

fn uploaded_once(drive: &FakeDrive) {
    let state = drive.state.lock().unwrap();
    let mut counts = BTreeMap::new();
    let mut hashes = BTreeSet::new();
    for session in state.sessions.values() {
        assert!(session.completed);
        assert!(
            hashes.insert(binary_alpha_engine::hex(&Sha256::digest(&session.received))),
            "identical bytes uploaded under more than one content binding"
        );
        *counts.entry(&session.name).or_insert(0usize) += 1;
    }
    assert!(!counts.is_empty());
    assert!(
        counts.values().all(|n| *n == 1),
        "content re-uploaded: {counts:?}"
    );
}
fn verify_closure(store: &Path, dataset: &str, stream: &str) {
    for generation in [dataset, stream] {
        run(&[
            "data",
            "verify",
            "--manifest",
            &format!(
                "file://{}/manifests/{generation}/ready.json",
                store.display()
            ),
        ])
        .unwrap();
    }
}

#[test]
fn root_updates_archive_restore_update_retire_preserve_shared_daily_content() {
    let f = fixture("daily_end_to_end");
    let producer = f.scratch.path("producer");
    let store = producer.join("store");
    let config = f.scratch.path("daily-producer.toml");
    fs::write(
        &config,
        pipeline_toml(
            &producer,
            &f.drive.base,
            &[("deriv", "deriv.toml")],
            None,
            3,
        ),
    )
    .unwrap();
    // This is the real v2 import path, using only generated deterministic source fixtures.
    let report = run(&[
        "data",
        "import",
        "--config",
        f.scratch.path("deriv-import.toml").to_str().unwrap(),
    ])
    .unwrap();
    let root = dataset(&store, imported_generation(&report, "deriv:frxEURUSD"));
    assert_eq!(root.layout, Some(Layout::DailyV2));
    let unchanged = root
        .objects
        .iter()
        .find(|o| o.path == "observations/2025-08-11.parquet")
        .unwrap()
        .key
        .clone();
    let mut generations = Vec::new();
    for seconds in [300, 600] {
        let report = pipeline(
            "update",
            &config,
            &["--end", &time_text((DERIV_SEED_END + seconds) * 1_000_000)],
        )
        .unwrap();
        let line = job_line(&report, "deriv");
        let generation = field(line, "dataset").to_string();
        let stream = field(line, "stream").to_string();
        let manifest = dataset(&store, &generation);
        assert!(manifest.objects.iter().any(|o| o.key == unchanged));
        verify_closure(&store, &generation, &stream);
        generations.push((generation, stream));
        uploaded_once(&f.drive);
    }
    let uploads = f.drive.state.lock().unwrap().sessions.len();
    pipeline("archive", &config, &[]).unwrap();
    assert_eq!(
        uploads,
        f.drive.state.lock().unwrap().sessions.len(),
        "standalone archive must reuse update's registry and receipt"
    );
    let listing = pipeline(
        "list",
        &config,
        &["--broker", "deriv", "--symbol", "frxEURUSD"],
    )
    .unwrap();
    let last = listing
        .lines()
        .find(|line| field(line, "dataset") == generations[1].0)
        .unwrap();
    let catalog = field(last, "catalog").to_string();
    let digest = field(last, "sha256").to_string();
    let fresh = f.scratch.path("fresh");
    let restored = f.scratch.path("daily-restored.toml");
    fs::write(
        &restored,
        pipeline_toml(&fresh, &f.drive.base, &[("deriv", "deriv.toml")], None, 3),
    )
    .unwrap();
    // Remove even read access through the original manifest's source URI.
    fs::rename(&store, producer.join("store.saved")).unwrap();
    pipeline(
        "restore",
        &restored,
        &[
            "--catalog",
            &catalog,
            "--sha256",
            &digest,
            "--broker",
            "deriv",
            "--symbol",
            "frxEURUSD",
        ],
    )
    .unwrap();
    let store = fresh.join("store");
    // A retry through pull must repair missing lineage manifests, even with top manifests present.
    let catalog_bytes = f.drive.state.lock().unwrap().files[&catalog].bytes.clone();
    let archived: data_pipeline::Catalog = serde_json::from_slice(&catalog_bytes).unwrap();
    let root_stream = archived
        .lineage_manifests
        .iter()
        .find(|m| m.generation != root.generation)
        .unwrap();
    fs::remove_dir_all(store.join(&root_stream.key).parent().unwrap()).unwrap();
    let declaration = f.scratch.path("daily-declaration.json");
    fs::write(&declaration,json!({
        "schema_version":1,"operator":"fixture","root":format!("file://{}",store.display()),"namespace":"fixture",
        "populations":[{"id":"development","role":"development","instrument":"deriv:frxEURUSD","source":"synthetic",
        "coverage":archived.coverage,"generations":[root.generation,generations[1].0],"tokens":["fixture"]}]
    }).to_string()).unwrap();
    let governed = f.scratch.path("daily-governed.toml");
    fs::write(
        &governed,
        pipeline_toml(
            &fresh,
            &f.drive.base,
            &[],
            Some(&format!("file://{}", declaration.display())),
            3,
        ),
    )
    .unwrap();
    pipeline(
        "pull",
        &governed,
        &["--broker", "deriv", "--symbol", "frxEURUSD"],
    )
    .unwrap();
    assert!(store.join(&root_stream.key).is_file());

    verify_closure(&store, &generations[1].0, &generations[1].1);
    assert!(
        store.join(root.key()).is_file(),
        "catalog carries its continuation root"
    );
    let report = pipeline(
        "update",
        &restored,
        &["--end", &time_text((DERIV_SEED_END + 900) * 1_000_000)],
    )
    .unwrap();
    let line = job_line(&report, "deriv");
    let newest = field(line, "dataset");
    let newest_stream = field(line, "stream");
    let manifest = dataset(&store, newest);
    assert!(manifest.objects.iter().any(|o| o.key == unchanged));
    assert_eq!(
        ticks(&store, &manifest),
        expected_ticks(
            DERIV_SEED_END,
            DERIV_SEED_END - 2 - 60,
            DERIV_SEED_END + 900
        )
    );
    uploaded_once(&f.drive);
    let newest_listing = pipeline(
        "list",
        &restored,
        &["--broker", "deriv", "--symbol", "frxEURUSD"],
    )
    .unwrap();
    let newest_catalog = field(
        newest_listing
            .lines()
            .find(|l| field(l, "dataset") == newest)
            .unwrap(),
        "catalog",
    );
    let pinned: data_pipeline::Catalog =
        serde_json::from_slice(&f.drive.state.lock().unwrap().files[newest_catalog].bytes).unwrap();
    let needed_keys: BTreeSet<_> = pinned
        .objects
        .iter()
        .map(|o| o.key.clone())
        .chain([pinned.dataset.key.clone(), pinned.stream.key.clone()])
        .chain(pinned.lineage_manifests.iter().map(|m| m.key.clone()))
        .collect();
    let needed_ids: BTreeSet<_> = pinned
        .objects
        .iter()
        .map(|o| o.file_id.clone())
        .chain([
            newest_catalog.to_string(),
            pinned.dataset.file_id.clone(),
            pinned.stream.file_id.clone(),
        ])
        .chain(pinned.lineage_manifests.iter().map(|m| m.file_id.clone()))
        .collect();
    let planned = pipeline("retire", &restored, &["--plan"]).unwrap();
    let plan_path = PathBuf::from(field(&planned, "plan"));
    let plan: Plan = serde_json::from_slice(&fs::read(&plan_path).unwrap()).unwrap();
    assert!(!plan.delete_drive.is_empty());
    assert!(
        plan.delete_drive.iter().any(|d| d.file_id == catalog),
        "superseded v2 catalog retires"
    );
    assert!(
        plan.delete_local
            .iter()
            .any(|d| d.path == format!("manifests/{}", generations[1].0)),
        "superseded local v2 descendant retires"
    );
    for deletion in &plan.delete_drive {
        assert!(!needed_ids.contains(&deletion.file_id));
        assert!(!needed_keys.contains(&deletion.key));
    }
    for deletion in &plan.delete_local {
        assert!(deletion.files.keys().all(|key| !needed_keys.contains(key)));
    }
    pipeline(
        "retire",
        &restored,
        &["--apply", plan_path.to_str().unwrap()],
    )
    .unwrap();
    verify_closure(&store, newest, newest_stream);
    assert!(store.join(&unchanged).is_file());
    assert!(
        needed_ids
            .iter()
            .all(|id| f.drive.state.lock().unwrap().files.contains_key(id))
    );
    uploaded_once(&f.drive);
    assert!(!fresh.join("pipeline_state/deriv/transfers.json").exists());
    // Replanning after completed retirement must understand the retired ancestry and receipts.
    pipeline("retire", &restored, &["--plan"]).unwrap();
}

fn catalog_for(config: &Path, broker: &str, symbol: &str, generation: &str) -> (String, String) {
    let listing = pipeline("list", config, &["--broker", broker, "--symbol", symbol]).unwrap();
    let line = listing
        .lines()
        .find(|line| field(line, "dataset") == generation)
        .unwrap();
    (field(line, "catalog").into(), field(line, "sha256").into())
}
fn restore_catalog(config: &Path, catalog: &(String, String), broker: &str, symbol: &str) {
    pipeline(
        "restore",
        config,
        &[
            "--catalog",
            &catalog.0,
            "--sha256",
            &catalog.1,
            "--broker",
            broker,
            "--symbol",
            symbol,
        ],
    )
    .unwrap();
}
fn daily_paths(store: &Path) -> BTreeSet<String> {
    let mut keys = BTreeSet::new();
    for generation in binary_alpha_app::store::Store::filesystem(store)
        .list_manifests()
        .unwrap()
    {
        let value = read_json(&store.join(binary_alpha_engine::dataset::manifest_key(&generation)));
        assert_eq!(
            value["layout"], "daily-v2",
            "v1 manifest remains: {generation}"
        );
        for object in value["objects"].as_array().unwrap() {
            let path = object["path"].as_str().unwrap();
            assert!(
                path.starts_with("observations/") && path.ends_with(".parquet")
                    || path.starts_with("pages/") && path.ends_with(".parquet")
                    || path.starts_with("candles/") && path.ends_with(".parquet")
                    || matches!(
                        path,
                        "provenance/coverage.json" | "provenance/lineage.json" | "profile.json"
                    ),
                "non-v2 object path remains: {path}"
            );
            keys.insert(object["key"].as_str().unwrap().to_string());
        }
    }
    keys
}

#[test]
fn migration_archive_restores_the_proved_stream_after_configuration_changes() {
    let f = fixture("migration_changed_stream");
    let producer = f.scratch.path("producer");
    let store = producer.join("store");
    let config = f.scratch.path("deriv-pipeline.toml");
    fs::write(
        &config,
        pipeline_toml(
            &producer,
            &f.drive.base,
            &[("deriv", "deriv.toml")],
            None,
            3,
        ),
    )
    .unwrap();
    import(&f.scratch.path("deriv-import.toml")).unwrap();
    let session_config = fs::read_to_string(f.scratch.path("deriv.toml")).unwrap();
    let legacy_config = session_config
        .lines()
        .filter(|line| !line.starts_with("session ="))
        .collect::<Vec<_>>()
        .join("\n");
    pipeline(
        "update",
        &config,
        &["--end", &time_text((DERIV_SEED_END + 120) * 1_000_000)],
    )
    .unwrap();
    // Current daily writes require a calendar. Only the reconstructed historical fixture
    // uses the sessionless definition; freeze reads deriv-import.toml.
    import_config(&f.scratch, "deriv", &legacy_config);
    legacy_fixtures::freeze(&f);
    // A calendar may be added to a legacy definition; every other field stays exact.
    import_config(&f.scratch, "deriv", &session_config);
    pipeline("migrate", &config, &[]).unwrap();
    let state = read_json(&producer.join("pipeline_state/deriv/migration.json"));
    let record_path = producer
        .join("pipeline_state/records")
        .join(state["record"].as_str().unwrap());
    let mut record: binary_alpha_app::lineage::MigrationRecord =
        serde_json::from_value(read_json(&record_path)).unwrap();
    assert!(record.verified());
    let candle_proof = &record.evidence["proofs"]["candles"];
    assert_eq!(candle_proof["basis"], "legacy_definition_reconstruction");
    assert_eq!(candle_proof["equal"], true);
    assert_eq!(candle_proof["profile_equal"], true);
    assert!(candle_proof["legacy_definition"].get("session").is_none());
    assert_ne!(
        candle_proof["legacy_streams"], candle_proof["product_streams"],
        "session grid fills the fixture's missing feed buckets; equality proves legacy reconstruction"
    );
    record.evidence.get_mut("proofs").unwrap()["candles"]["session_product_verified"] =
        serde_json::json!(false);
    assert!(
        !record.verified(),
        "a failed session proof cannot authorize retirement"
    );
    for basis in [Some("unknown"), Some("direct_product_equality"), None] {
        let proof = &mut record.evidence.get_mut("proofs").unwrap()["candles"];
        if let Some(basis) = basis {
            proof["basis"] = serde_json::json!(basis);
        } else {
            proof.as_object_mut().unwrap().remove("basis");
        }
        assert!(
            !record.verified(),
            "a malformed basis cannot bypass session proof"
        );
    }
    let root = state["dataset"].as_str().unwrap();
    let proved_stream = state["stream"].as_str().unwrap();
    let core = fs::read_to_string(f.scratch.path("deriv.toml"))
        .unwrap()
        .replace("duration_seconds = 10", "duration_seconds = 20");
    fs::write(f.scratch.path("deriv.toml"), &core).unwrap();
    write_evidence(&f.scratch, "deriv", &core);
    import_config(&f.scratch, "deriv", &core);
    run(&[
        "data",
        "audit",
        "--config",
        f.scratch.path("deriv-import.toml").to_str().unwrap(),
        "--manifest",
        &format!(
            "file://{}",
            store
                .join(binary_alpha_engine::dataset::manifest_key(root))
                .display()
        ),
    ])
    .unwrap();
    pipeline("archive", &config, &[]).unwrap();
    let catalog = catalog_for(&config, "deriv", "frxEURUSD", root);
    let archived: Catalog =
        serde_json::from_slice(&f.drive.state.lock().unwrap().files[&catalog.0].bytes).unwrap();
    assert_ne!(archived.stream.generation, proved_stream);
    assert!(
        archived
            .lineage_manifests
            .iter()
            .any(|m| m.generation == proved_stream)
    );
    let original = {
        let mut drive = f.drive.state.lock().unwrap();
        let file = drive.files.get_mut(&catalog.0).unwrap();
        let original = file.bytes.clone();
        let mut incomplete = archived.clone();
        incomplete
            .lineage_manifests
            .retain(|m| m.generation != proved_stream);
        file.bytes = serde_json::to_vec(&incomplete).unwrap();
        original
    };
    let error = pipeline("retire", &config, &["--plan"]).unwrap_err();
    assert!(
        error.contains("catalog omits the verified migration stream"),
        "{error}"
    );
    assert!(!f.drive.log().iter().any(|line| line.starts_with("DELETE")));
    f.drive
        .state
        .lock()
        .unwrap()
        .files
        .get_mut(&catalog.0)
        .unwrap()
        .bytes = original;
    let planned = pipeline("retire", &config, &["--plan"]).unwrap();
    pipeline("retire", &config, &["--apply", field(&planned, "plan")]).unwrap();

    let fresh = Scratch::new("migration_changed_stream_restore");
    let fresh_config = fresh.path("pipeline.toml");
    fs::write(fresh.path("deriv.toml"), core).unwrap();
    fs::write(
        &fresh_config,
        pipeline_toml(
            &fresh.path("managed"),
            &f.drive.base,
            &[("deriv", "deriv.toml")],
            None,
            3,
        ),
    )
    .unwrap();
    fs::rename(&store, producer.join("store.saved")).unwrap();
    restore_catalog(&fresh_config, &catalog, "deriv", "frxEURUSD");
    verify_closure(&fresh.path("managed/store"), root, proved_stream);
    verify_closure(
        &fresh.path("managed/store"),
        root,
        &archived.stream.generation,
    );
    pipeline("retire", &fresh_config, &["--plan"]).unwrap();
    uploaded_once(&f.drive);
}

#[test]
fn v1_migrate_archive_restore_update_twice_retire_and_restore_both_brokers() {
    use binary_alpha_engine::dataset::daily::{DayFamily, day_bounds};
    let f = fixture("migration_daily_end_to_end");
    let producer = f.scratch.path("producer");
    let store = producer.join("store");
    // Both jobs span multiple UTC days; every source byte is generated by this test.
    let midnight = day_bounds(&time_text(POCKET_START * 1_000_000)[..10])
        .unwrap()
        .0
        / 1_000_000;
    write_collection(
        &f.scratch.path("sources/pocket"),
        &[AssetSpec {
            asset: "AEDCNY_otc",
            expected_symbol_id: None,
            symbol_id: Some(POCKET_SYMBOL_ID),
            files: vec![
                bar_rows(midnight - 10, midnight),
                bar_rows(POCKET_START, POCKET_SEED_END),
            ],
            metadata: true,
        }],
    );
    fs::write(
        f.scratch
            .path("sources/pocket/AEDCNY_otc/download_manifest.json"),
        json!({"asset":"AEDCNY_otc","server_timestamp_offset_seconds":7200}).to_string(),
    )
    .unwrap();
    let pocket_core = fs::read_to_string(f.scratch.path("pocket.toml"))
        .unwrap()
        .replace(
            "2025-05-19T11:15:00Z",
            &time_text((midnight - 10) * 1_000_000),
        );
    fs::write(f.scratch.path("pocket.toml"), &pocket_core).unwrap();
    import_config(&f.scratch, "pocket", &pocket_core);
    write_evidence(&f.scratch, "pocket", &pocket_core);
    let jobs = [
        ("deriv", "deriv", "frxEURUSD", DERIV_SEED_END),
        ("pocket", "pocket_option", "AEDCNY_otc", POCKET_SEED_END),
    ];
    let mut configs = BTreeMap::new();
    fs::copy(
        f.scratch.path("evidence/deriv.json"),
        f.scratch.path("evidence/deriv-old.json"),
    )
    .unwrap();
    for (job, _, _, end) in jobs {
        import(&f.scratch.path(&format!("{job}-import.toml"))).unwrap();
        let config = f.scratch.path(&format!("{job}-pipeline.toml"));
        fs::write(
            &config,
            pipeline_toml(
                &producer,
                &f.drive.base,
                &[(
                    if job == "deriv" { "deriv-old" } else { job },
                    &format!("{job}.toml"),
                )],
                None,
                3,
            ),
        )
        .unwrap();
        // Actual legacy history, streams, per-page/bundle objects, and remote catalogs.
        pipeline(
            "update",
            &config,
            &["--end", &time_text((end + 120) * 1_000_000)],
        )
        .unwrap();
        configs.insert(job, config);
    }
    pipeline(
        "update",
        &configs["deriv"],
        &["--end", &time_text((DERIV_SEED_END + 180) * 1_000_000)],
    )
    .unwrap();
    legacy_fixtures::freeze(&f);
    fs::write(
        &configs["deriv"],
        pipeline_toml(
            &producer,
            &f.drive.base,
            &[("deriv", "deriv.toml")],
            None,
            3,
        ),
    )
    .unwrap();
    let diagnostic = json!({"echo_req":{"ticks_history":"frxEURUSD"},
        "history":{"times":[DAY2-1,DAY2],"prices":["invalid","1.2"]}})
    .to_string()
    .into_bytes();
    let diagnostic_key = binary_alpha_engine::dataset::object_key(&binary_alpha_engine::hex(
        &Sha256::digest(&diagnostic),
    ));
    fs::write(store.join(&diagnostic_key), &diagnostic).unwrap();
    let v1_generations = binary_alpha_app::store::Store::filesystem(&store)
        .list_manifests()
        .unwrap();
    let mut v1_keys = BTreeSet::new();
    for generation in &v1_generations {
        let manifest =
            read_json(&store.join(binary_alpha_engine::dataset::manifest_key(generation)));
        assert!(manifest.get("layout").is_none_or(Value::is_null));
        v1_keys.extend(
            manifest["objects"]
                .as_array()
                .unwrap()
                .iter()
                .map(|o| o["key"].as_str().unwrap().to_string()),
        );
    }
    let v1_remote: BTreeSet<_> = f
        .drive
        .state
        .lock()
        .unwrap()
        .files
        .keys()
        .cloned()
        .collect();
    let requests = (
        f.deriv.requests().len(),
        f.pocket.requests().len(),
        f.drive.log().len(),
    );
    pipeline("migrate", &f.pipeline, &[]).unwrap();
    assert_eq!(
        requests,
        (
            f.deriv.requests().len(),
            f.pocket.requests().len(),
            f.drive.log().len()
        ),
        "migration is offline"
    );
    // A verified local conversion alone cannot authorize deletion before archive publication.
    let unarchived = pipeline("retire", &f.pipeline, &["--plan"]);
    assert!(
        unarchived.is_err(),
        "retirement requires an archived v2 catalog"
    );
    pipeline("archive", &f.pipeline, &[]).unwrap();
    uploaded_once(&f.drive);
    let mut old_roots = BTreeMap::new();
    for (job, broker, symbol, _) in jobs {
        let path = producer.join(format!("pipeline_state/{job}/migration.json"));
        let mut state = read_json(&path);
        let record_path = producer
            .join("pipeline_state/records")
            .join(state["record"].as_str().unwrap());
        let record = fs::read(&record_path).unwrap();
        let proof: Value = serde_json::from_slice(&record).unwrap();
        if job == "deriv" {
            assert_eq!(proof["predecessor_jobs"], json!(["deriv-old"]));
        }
        if job == "pocket" {
            assert!(!proof["storage_aliases"].as_array().unwrap().is_empty());
        }
        old_roots.insert(
            job,
            (
                state["dataset"].as_str().unwrap().to_string(),
                state["stream"].as_str().unwrap().to_string(),
                catalog_for(
                    &f.pipeline,
                    broker,
                    symbol,
                    state["dataset"].as_str().unwrap(),
                ),
                record_path,
                record,
            ),
        );
        // Model the checkpoint left by an older proof executable, retaining its immutable receipt.
        state["proof_version"] = json!(0);
        fs::write(path, state.to_string()).unwrap();
    }
    let recovered_diagnostic = json!({"echo_req":{"ticks_history":"frxEURUSD"},
        "history":{"times":[DAY2],"prices":["newly-recovered"]}})
    .to_string()
    .into_bytes();
    let recovered_key = binary_alpha_engine::dataset::object_key(&binary_alpha_engine::hex(
        &Sha256::digest(&recovered_diagnostic),
    ));
    fs::write(store.join(&recovered_key), &recovered_diagnostic).unwrap();
    pipeline("migrate", &f.pipeline, &[]).unwrap();
    for (job, _, _, _) in jobs {
        let state = read_json(&producer.join(format!("pipeline_state/{job}/migration.json")));
        let (_, _, _, path, bytes) = &old_roots[job];
        assert_eq!(
            fs::read(path).unwrap(),
            *bytes,
            "supersession preserves completed evidence"
        );
        assert_ne!(state["dataset"], old_roots[job].0);
        let record = read_json(
            &producer
                .join("pipeline_state/records")
                .join(state["record"].as_str().unwrap()),
        );
        assert_eq!(
            record["supersedes"],
            path.file_name().unwrap().to_str().unwrap()
        );
    }
    pipeline("archive", &f.pipeline, &[]).unwrap();
    uploaded_once(&f.drive);
    let mut roots = BTreeMap::new();
    for (job, broker, symbol, _) in jobs {
        let state = read_json(&producer.join(format!("pipeline_state/{job}/migration.json")));
        let generation = state["dataset"].as_str().unwrap().to_string();
        let stream = state["stream"].as_str().unwrap().to_string();
        verify_closure(&store, &generation, &stream);
        let manifest = dataset(&store, &generation);
        assert!(
            manifest
                .day_inventory
                .iter()
                .filter(|d| d.family == DayFamily::Observations)
                .count()
                >= 2
        );
        let coverage_object = manifest
            .objects
            .iter()
            .find(|o| o.path == "provenance/coverage.json")
            .unwrap();
        binary_alpha_engine::dataset::coverage::DailyCoverage::from_json(
            &fs::read(store.join(&coverage_object.key)).unwrap(),
        )
        .unwrap()
        .check_manifest(&manifest)
        .unwrap();
        roots.insert(
            job,
            (
                generation.clone(),
                stream,
                catalog_for(&f.pipeline, broker, symbol, &generation),
            ),
        );
    }
    uploaded_once(&f.drive);
    let uploads = f.drive.state.lock().unwrap().sessions.len();
    pipeline("archive", &f.pipeline, &[]).unwrap();
    assert_eq!(uploads, f.drive.state.lock().unwrap().sessions.len());

    let fresh = Scratch::new("migration_daily_restored");
    fs::create_dir_all(fresh.path("evidence")).unwrap();
    for (job, _, _, _) in jobs {
        fs::copy(
            f.scratch.path(&format!("{job}.toml")),
            fresh.path(&format!("{job}.toml")),
        )
        .unwrap();
        fs::copy(
            f.scratch.path(&format!("evidence/{job}.json")),
            fresh.path(&format!("evidence/{job}.json")),
        )
        .unwrap();
    }
    let fresh_config = fresh.path("pipeline.toml");
    fs::write(
        &fresh_config,
        pipeline_toml(
            &fresh.path("managed"),
            &f.drive.base,
            &[("deriv", "deriv.toml"), ("pocket", "pocket.toml")],
            None,
            3,
        ),
    )
    .unwrap();
    // Remove the original source URI from reach throughout restore and continuation.
    fs::rename(&store, producer.join("store.saved")).unwrap();
    let fresh_store = fresh.path("managed/store");
    let mut descendants = BTreeMap::new();
    let mut superseded = BTreeSet::new();
    superseded.extend(
        old_roots
            .values()
            .map(|(_, _, catalog, _, _)| catalog.0.clone()),
    );
    superseded.extend(roots.values().map(|(_, _, catalog)| catalog.0.clone()));
    for (job, broker, symbol, end) in jobs {
        let (root, stream, catalog) = &roots[job];
        restore_catalog(&fresh_config, catalog, broker, symbol);
        verify_closure(&fresh_store, root, stream);
        let archived: Catalog =
            serde_json::from_slice(&f.drive.state.lock().unwrap().files[&catalog.0].bytes).unwrap();
        assert!(!archived.records.is_empty());
        for record in &archived.records {
            assert_eq!(
                fs::read(fresh.path("managed/pipeline_state").join(&record.key)).unwrap(),
                fs::read(producer.join("pipeline_state").join(&record.key)).unwrap(),
                "restored immutable record {}",
                record.key
            );
        }
        let missing_record = fresh
            .path("managed/pipeline_state")
            .join(&archived.records[0].key);
        let expected_record = fs::read(&missing_record).unwrap();
        fs::remove_file(&missing_record).unwrap();
        pipeline(
            "pull",
            &fresh_config,
            &["--broker", broker, "--symbol", symbol],
        )
        .unwrap();
        assert_eq!(fs::read(missing_record).unwrap(), expected_record);
        let config = fresh.path(&format!("{job}-pipeline.toml"));
        fs::write(
            &config,
            pipeline_toml(
                &fresh.path("managed"),
                &f.drive.base,
                &[(job, &format!("{job}.toml"))],
                None,
                3,
            ),
        )
        .unwrap();
        let mut prior_objects = None;
        let mut versions = Vec::new();
        for _ in 0..2 {
            let report = pipeline(
                "update",
                &config,
                &["--end", &time_text((end + 360) * 1_000_000)],
            )
            .unwrap();
            let line = job_line(&report, job);
            let generation = field(line, "dataset").to_string();
            let stream = field(line, "stream").to_string();
            let manifest = dataset(&fresh_store, &generation);
            let lineage = manifest
                .objects
                .iter()
                .find(|o| o.path == "provenance/lineage.json")
                .unwrap();
            assert_eq!(
                read_json(&fresh_store.join(&lineage.key))["root_generation"],
                *root
            );
            if job == "deriv" {
                let pages: Vec<_> = manifest
                    .day_inventory
                    .iter()
                    .filter(|d| d.family == DayFamily::Pages)
                    .flat_map(|d| {
                        binary_alpha_app::daily::read_pages(
                            &fresh_store.join(d.object.as_ref().unwrap()),
                            &d.date,
                        )
                        .unwrap()
                    })
                    .collect();
                for bytes in [&diagnostic, &recovered_diagnostic] {
                    assert!(pages.iter().any(|p| &p.payload == bytes
                        && p.disposition == binary_alpha_app::daily::PageDisposition::Diagnostic));
                }
            }

            let candles =
                read_json(&fresh_store.join(binary_alpha_engine::dataset::manifest_key(&stream)));
            let observations_and_candles: BTreeSet<_> = manifest
                .objects
                .iter()
                .filter(|o| o.path.starts_with("observations/"))
                .map(|o| o.key.clone())
                .chain(
                    candles["objects"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .filter(|o| o["path"].as_str().unwrap().starts_with("candles/"))
                        .map(|o| o["key"].as_str().unwrap().to_string()),
                )
                .collect();
            if let Some(prior) = prior_objects {
                assert_eq!(
                    observations_and_candles, prior,
                    "no-new-data update rewrote market partitions"
                );
            }
            prior_objects = Some(observations_and_candles);
            verify_closure(&fresh_store, &generation, &stream);
            let catalog = catalog_for(&config, broker, symbol, &generation);
            versions.push((generation, stream, catalog));
            uploaded_once(&f.drive);
        }
        assert_ne!(
            versions[0].0, versions[1].0,
            "the second invocation retains its distinct response occurrences"
        );
        superseded.insert(versions[0].2.0.clone());
        pipeline("archive", &config, &[]).unwrap();
        descendants.insert(job, versions);
    }
    for entry in fs::read_dir(producer.join("pipeline_state/records"))
        .unwrap()
        .flatten()
    {
        if !entry.file_type().unwrap().is_file() {
            continue;
        }
        let bytes = fs::read(entry.path()).unwrap();
        if serde_json::from_slice::<Value>(&bytes)
            .is_ok_and(|v| roots.values().any(|(_, _, c)| v["file_id"] == c.0))
        {
            continue;
        }
        assert_eq!(
            fs::read(
                fresh
                    .path("managed/pipeline_state/records")
                    .join(entry.file_name())
            )
            .unwrap(),
            bytes,
            "all cumulative records, including superseded alias tables and predecessor records, restore exactly"
        );
    }
    fs::rename(producer.join("store.saved"), &store).unwrap();
    // Install both daily descendants into the producer that still owns every v1 closure.
    for (job, broker, symbol, _) in jobs {
        for (_, _, catalog) in &descendants[job] {
            restore_catalog(&f.pipeline, catalog, broker, symbol);
        }
    }
    let mut retained_ids = BTreeSet::new();
    for versions in descendants.values() {
        let newest = &versions[1].2.0;
        retained_ids.insert(newest.clone());
        let catalog: Catalog =
            serde_json::from_slice(&f.drive.state.lock().unwrap().files[newest].bytes).unwrap();
        retained_ids.extend(
            catalog
                .objects
                .iter()
                .chain(&catalog.records)
                .map(|e| e.file_id.clone()),
        );
        retained_ids.extend(
            [&catalog.dataset, &catalog.stream]
                .into_iter()
                .chain(&catalog.lineage_manifests)
                .map(|e| e.file_id.clone()),
        );
    }
    let planned = pipeline("retire", &f.pipeline, &["--plan"]).unwrap();
    let path = PathBuf::from(field(&planned, "plan"));
    let plan: Plan = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
    for (root, stream, _, _, _) in old_roots.values() {
        for generation in [root, stream] {
            assert!(
                plan.delete_local
                    .iter()
                    .any(|d| d.path == format!("manifests/{generation}")),
                "superseded migration closure must retire: {generation}"
            );
        }
    }
    for generation in &v1_generations {
        assert!(
            plan.delete_local
                .iter()
                .any(|d| d.path == format!("manifests/{generation}")),
            "v1 manifest not planned: {generation}"
        );
    }
    for versions in descendants.values() {
        for generation in [&versions[0].0, &versions[0].1] {
            assert!(
                plan.delete_local
                    .iter()
                    .any(|d| d.path == format!("manifests/{generation}")),
                "superseded v2 manifest not planned: {generation}"
            );
        }
    }
    for id in &superseded {
        assert!(plan.delete_drive.iter().any(|d| &d.file_id == id));
    }
    assert!(
        plan.delete_drive
            .iter()
            .all(|d| !retained_ids.contains(&d.file_id))
    );
    pipeline("retire", &f.pipeline, &["--apply", path.to_str().unwrap()]).unwrap();
    for versions in descendants.values() {
        for generation in [&versions[0].0, &versions[0].1] {
            assert!(
                !store
                    .join(binary_alpha_engine::dataset::manifest_key(generation))
                    .exists(),
                "superseded v2 manifest remains: {generation}"
            );
        }
    }
    for generation in &v1_generations {
        assert!(
            !store
                .join(binary_alpha_engine::dataset::manifest_key(generation))
                .exists()
        );
    }
    let remaining = daily_paths(&store);
    let physical: BTreeSet<_> = fs::read_dir(store.join("objects"))
        .unwrap()
        .map(|e| format!("objects/{}", e.unwrap().file_name().to_string_lossy()))
        .collect();
    assert_eq!(
        physical, remaining,
        "every remaining physical object belongs to a v2 family or its metadata"
    );
    for key in v1_keys.difference(&remaining) {
        assert!(!store.join(key).exists(), "legacy object remains: {key}");
    }
    for id in v1_remote.difference(&retained_ids) {
        assert!(
            !f.drive.state.lock().unwrap().files.contains_key(id),
            "v1 remote file remains: {id}"
        );
    }
    for id in &superseded {
        assert!(!f.drive.state.lock().unwrap().files.contains_key(id));
    }
    assert!(
        retained_ids
            .iter()
            .all(|id| f.drive.state.lock().unwrap().files.contains_key(id))
    );
    assert_eq!(
        f.drive
            .state
            .lock()
            .unwrap()
            .files
            .keys()
            .cloned()
            .collect::<BTreeSet<_>>(),
        retained_ids,
        "remote inventory contains only exact retained closure file ids"
    );
    uploaded_once(&f.drive);
    pipeline("retire", &f.pipeline, &["--plan"]).unwrap();

    let recovered = Scratch::new("migration_daily_after_retirement");
    fs::create_dir_all(recovered.path("evidence")).unwrap();
    for (job, _, _, _) in jobs {
        fs::copy(
            f.scratch.path(&format!("{job}.toml")),
            recovered.path(&format!("{job}.toml")),
        )
        .unwrap();
        fs::copy(
            f.scratch.path(&format!("evidence/{job}.json")),
            recovered.path(&format!("evidence/{job}.json")),
        )
        .unwrap();
    }
    let recovered_config = recovered.path("pipeline.toml");
    fs::write(
        &recovered_config,
        pipeline_toml(
            &recovered.path("managed"),
            &f.drive.base,
            &[("deriv", "deriv.toml"), ("pocket", "pocket.toml")],
            None,
            3,
        ),
    )
    .unwrap();
    fs::rename(&store, producer.join("retired-store.saved")).unwrap();
    fs::rename(&fresh_store, fresh.path("retired-store.saved")).unwrap();
    for (job, broker, symbol, _) in jobs {
        let (generation, stream, catalog) = &descendants[job][1];
        verify_closure(&producer.join("retired-store.saved"), generation, stream);
        restore_catalog(&recovered_config, catalog, broker, symbol);
        verify_closure(&recovered.path("managed/store"), generation, stream);
    }
    daily_paths(&recovered.path("managed/store"));
    pipeline("retire", &recovered_config, &["--plan"]).unwrap();
    // A later instrument removal uses the descendant catalog's cumulative migration
    // evidence; the original root catalogs and superseded roots have already been retired.
    for (job, _, _, _) in jobs {
        let report = pipeline(
            "retire",
            &recovered_config,
            &["--job", job, "--whole-job", "--plan"],
        )
        .unwrap();
        let path = report.split_whitespace().nth(2).unwrap();
        pipeline("retire", &recovered_config, &["--apply", path]).unwrap();
        pipeline("remove-job", &recovered_config, &["--job", job]).unwrap();
    }
    assert!(
        binary_alpha_app::store::Store::filesystem(recovered.path("managed/store"))
            .list_manifests()
            .unwrap()
            .is_empty()
    );
    assert!(
        f.drive
            .state
            .lock()
            .unwrap()
            .files
            .values()
            .all(|entry| entry.name.starts_with("record-"))
    );
}

//! Regressions from the archive/evidence integration review; synthetic fixtures only.
use super::*;

#[test]
fn review_standalone_acquisition_record_restores() {
    let f = fixture("review_standalone_acquisition");
    let producer = f.scratch.path("producer");
    let config = f.scratch.path("standalone.toml");
    let core = fs::read_to_string(f.scratch.path("deriv-import.toml"))
        .unwrap()
        .replace("2025-08-13T00:00:00Z", "2025-08-11T00:01:00Z");
    fs::write(&config, core).unwrap();
    let report = run(&["data", "fetch", "--config", config.to_str().unwrap()]).unwrap();
    let generation = field(&report, "generation");
    let manifest = dataset(&producer.join("store"), generation);
    let lineage = manifest
        .objects
        .iter()
        .find(|o| o.path == "provenance/lineage.json")
        .unwrap();
    let lineage = read_json(&producer.join("store").join(&lineage.key));
    let acquisition = lineage["continuation"]["acquisition_id"]
        .as_str()
        .unwrap()
        .to_owned();
    assert!(acquisition.starts_with("objects/"));
    let original = fs::read(producer.join("store").join(&acquisition)).unwrap();
    run(&[
        "data",
        "audit",
        "--config",
        config.to_str().unwrap(),
        "--manifest",
        &format!(
            "file://{}",
            producer.join("store").join(manifest.key()).display()
        ),
    ])
    .unwrap();
    let archived = pipeline("archive", &f.pipeline, &["--job", "deriv"]).unwrap();
    let catalog_id = field(job_line(&archived, "deriv"), "catalog");
    let sha = field(job_line(&archived, "deriv"), "sha256");
    let fresh = f.scratch.path("fresh");
    fs::write(&config, pipeline_toml(&fresh, &f.drive.base, &[], None, 3)).unwrap();
    fs::rename(&producer, f.scratch.path("producer.saved")).unwrap();
    let restored = pipeline(
        "restore",
        &config,
        &[
            "--catalog",
            catalog_id,
            "--sha256",
            sha,
            "--broker",
            "deriv",
            "--symbol",
            "frxEURUSD",
        ],
    )
    .unwrap();
    println!(
        "{restored}\nacquisition key {acquisition}, {} original bytes",
        original.len()
    );
    assert!(
        fresh.join("store").join(&acquisition).is_file(),
        "successful archive/restore omitted standalone acquisition record {acquisition}"
    );
    assert_eq!(
        fs::read(fresh.join("store").join(&acquisition)).unwrap(),
        original
    );
}

#[test]
fn review_standalone_descendant_acquisitions_restore() {
    standalone_pocket_acquisitions(false);
}

#[test]
fn review_legacy_standalone_acquisitions_migrate_and_restore() {
    standalone_pocket_acquisitions(true);
}

fn standalone_pocket_acquisitions(migrate: bool) {
    use binary_alpha_engine::{
        config::{Config, Seed},
        dataset::manifest_key,
    };
    let f = fixture(&format!("review_pocket_acquisitions_{migrate}"));
    let producer = f.scratch.path("producer");
    let store = producer.join("store");
    let config_path = f.scratch.path("pocket-import.toml");
    let imported = import(&config_path).unwrap();
    let seed = imported_generation(&imported, "pocket_option:AEDCNY_otc");
    let mut config = Config::parse(&fs::read_to_string(&config_path).unwrap()).unwrap();
    let source_identity = binary_alpha_app::broker::source_identity(&config.brokers[0]);
    config.history.as_mut().unwrap().seeds = vec![Seed {
        provider_symbol: "AEDCNY_otc".to_string().try_into().unwrap(),
        manifest: format!("file://{}", store.join(manifest_key(seed)).display())
            .parse()
            .unwrap(),
        source_identity,
    }];
    let mut acquisitions = BTreeMap::new();
    let mut current = None;
    for extra in [600, 660] {
        config.history.as_mut().unwrap().end = time_text((POCKET_SEED_END + extra) * 1_000_000);
        fs::write(&config_path, config.canonical_toml()).unwrap();
        let report = run(&["data", "fetch", "--config", config_path.to_str().unwrap()]).unwrap();
        let manifest = dataset(&store, field(&report, "generation"));
        let lineage = manifest
            .objects
            .iter()
            .find(|o| o.path == "provenance/lineage.json")
            .unwrap();
        let value = read_json(&store.join(&lineage.key));
        let key = value["continuation"]["acquisition_id"]
            .as_str()
            .unwrap()
            .to_string();
        assert!(key.starts_with("objects/"));
        acquisitions.insert(key.clone(), fs::read(store.join(key)).unwrap());
        if !migrate {
            assert_acquisition_closure(&manifest, &acquisitions);
        }
        current = Some(manifest);
    }
    assert_eq!(
        acquisitions.len(),
        2,
        "each invocation has its own immutable record"
    );
    if migrate {
        // Model historical v1 coverage-only references. The fixture converter deliberately
        // removes daily-only objects, so restore the original invocation bytes it discards.
        legacy_fixtures::freeze(&f);
        for (key, bytes) in &acquisitions {
            fs::write(store.join(key), bytes).unwrap();
        }
        pipeline("migrate", &f.pipeline, &["--job", "pocket"]).unwrap();
        let state = read_json(&producer.join("pipeline_state/pocket/migration.json"));
        let record = read_json(
            &producer
                .join("pipeline_state/records")
                .join(state["record"].as_str().unwrap()),
        );
        let census = &record["proofs"]["pages"]["source_census"]["physical"];
        assert_eq!(
            census["acquisition_records"], 2,
            "legacy invocations must be attributed as evidence: {census}"
        );
        current = Some(dataset(&store, state["dataset"].as_str().unwrap()));
        assert_acquisition_closure(current.as_ref().unwrap(), &acquisitions);
        // The merged proof replaces both former version-2 proofs. Re-proving must retain
        // standalone acquisition evidence as well as its daily response occurrences.
        let state_path = producer.join("pipeline_state/pocket/migration.json");
        let original_record = fs::read(
            producer
                .join("pipeline_state/records")
                .join(state["record"].as_str().unwrap()),
        )
        .unwrap();
        let mut prior = state.clone();
        prior["proof_version"] = json!(2);
        fs::write(&state_path, serde_json::to_vec_pretty(&prior).unwrap()).unwrap();
        pipeline("migrate", &f.pipeline, &["--job", "pocket"]).unwrap();
        let upgraded = read_json(&state_path);
        assert_eq!(upgraded["proof_version"], 4);
        assert_ne!(upgraded["record"], state["record"]);
        assert_eq!(
            fs::read(
                producer
                    .join("pipeline_state/records")
                    .join(state["record"].as_str().unwrap())
            )
            .unwrap(),
            original_record,
        );
        current = Some(dataset(&store, upgraded["dataset"].as_str().unwrap()));
        assert_acquisition_closure(current.as_ref().unwrap(), &acquisitions);
        // The migrated root is also a standalone continuation seed; its descendant must
        // retain the v1 invocation evidence despite its migration coverage identities.
        config.history.as_mut().unwrap().seeds[0].manifest = format!(
            "file://{}",
            store.join(current.as_ref().unwrap().key()).display()
        )
        .parse()
        .unwrap();
        config.history.as_mut().unwrap().end = time_text((POCKET_SEED_END + 720) * 1_000_000);
        fs::write(&config_path, config.canonical_toml()).unwrap();
        let report = run(&["data", "fetch", "--config", config_path.to_str().unwrap()]).unwrap();
        current = Some(dataset(&store, field(&report, "generation")));
        assert_acquisition_closure(current.as_ref().unwrap(), &acquisitions);
    }
    let manifest = current.unwrap();
    run(&[
        "data",
        "audit",
        "--config",
        config_path.to_str().unwrap(),
        "--manifest",
        &format!("file://{}", store.join(manifest.key()).display()),
    ])
    .unwrap();
    let archived = pipeline("archive", &f.pipeline, &["--job", "pocket"]).unwrap();
    let line = job_line(&archived, "pocket");
    let fresh = f.scratch.path("fresh");
    let restore_config = f.scratch.path("restore.toml");
    fs::write(
        &restore_config,
        pipeline_toml(&fresh, &f.drive.base, &[], None, 3),
    )
    .unwrap();
    fs::rename(&producer, f.scratch.path("producer.saved")).unwrap();
    pipeline(
        "restore",
        &restore_config,
        &[
            "--catalog",
            field(line, "catalog"),
            "--sha256",
            field(line, "sha256"),
            "--broker",
            "pocket_option",
            "--symbol",
            "AEDCNY_otc",
        ],
    )
    .unwrap();
    for (key, bytes) in acquisitions {
        assert_eq!(fs::read(fresh.join("store").join(key)).unwrap(), bytes);
    }
    verify::run(&format!(
        "file://{}",
        fresh.join("store").join(manifest.key()).display()
    ))
    .unwrap();
}

fn assert_acquisition_closure(
    manifest: &GenerationManifest,
    acquisitions: &BTreeMap<String, Vec<u8>>,
) {
    for (key, bytes) in acquisitions {
        assert!(
            manifest.objects.iter().any(|o| &o.key == key
                && o.role == binary_alpha_engine::dataset::ObjectRole::Provenance
                && o.bytes == bytes.len() as u64
                && o.sha256 == binary_alpha_engine::hex(&Sha256::digest(bytes))),
            "ready manifest must hash-bind invocation {key}"
        );
    }
}

#[test]
fn review_superseded_pending_log_restores() {
    let f = fixture("review_superseded_pending");
    import(&f.scratch.path("deriv-import.toml")).unwrap();
    let core = deriv_core(&f.deriv.url, 60, 1, 60);
    fs::write(f.scratch.path("deriv.toml"), &core).unwrap();
    write_evidence(&f.scratch, "deriv", &core);
    let config = f.scratch.path("pending-only.toml");
    let producer = f.scratch.path("producer");
    fs::write(
        &config,
        pipeline_toml(
            &producer,
            &f.drive.base,
            &[("deriv", "deriv.toml")],
            None,
            2,
        ),
    )
    .unwrap();
    let pending = pipeline(
        "update",
        &config,
        &["--end", &time_text((DERIV_SEED_END + 1000) * 1_000_000)],
    )
    .unwrap_err();
    assert!(pending.contains("status pending"), "{pending}");
    legacy_fixtures::freeze(&f);
    pipeline("migrate", &config, &[]).unwrap();
    let state_dir = producer.join("pipeline_state/deriv");
    let records = producer.join("pipeline_state/records");
    let originals: BTreeMap<_, _> = fs::read_dir(&records)
        .unwrap()
        .map(|e| e.unwrap().path())
        .filter(|p| p.extension().is_some_and(|e| e == "jsonl"))
        .map(|p| {
            (
                p.file_name().unwrap().to_str().unwrap().to_owned(),
                fs::read(&p).unwrap(),
            )
        })
        .collect();
    assert!(
        !originals.is_empty(),
        "expected migrated pending progress log"
    );
    let first = pipeline("archive", &config, &[]).unwrap();
    let first_id = field(job_line(&first, "deriv"), "catalog");
    let catalog = Catalog::from_json(&f.drive.state.lock().unwrap().files[first_id].bytes).unwrap();
    for name in originals.keys() {
        assert!(
            catalog
                .records
                .iter()
                .any(|e| e.key == format!("records/{name}"))
        );
    }
    // Fixture models pending acquisition abandonment after its bytes have been migrated.
    for file in [
        "progress.json",
        "progress.pages.jsonl",
        "progress.received.jsonl",
    ] {
        let p = state_dir.join(file);
        if p.exists() {
            fs::remove_file(p).unwrap();
        }
    }
    let state_path = state_dir.join("migration.json");
    let mut superseded_roots = Vec::new();
    for _ in 0..2 {
        let mut state = read_json(&state_path);
        superseded_roots.push(state["dataset"].as_str().unwrap().to_string());
        state.as_object_mut().unwrap().remove("proof_version");
        fs::write(&state_path, serde_json::to_vec_pretty(&state).unwrap()).unwrap();
        pipeline("migrate", &config, &[]).unwrap();
    }
    let second = pipeline("archive", &config, &[]).unwrap();
    let second_id = field(job_line(&second, "deriv"), "catalog");
    let sha = field(job_line(&second, "deriv"), "sha256");
    let inventory = Catalog::from_json(&f.drive.files()[second_id].bytes).unwrap();
    for (name, bytes) in &originals {
        assert!(
            inventory
                .records
                .iter()
                .any(|r| r.key == format!("records/{name}")
                    && r.bytes == bytes.len() as u64
                    && r.sha256 == binary_alpha_engine::hex(&Sha256::digest(bytes)))
        );
    }
    let planned = pipeline("retire", &config, &["--plan"]).unwrap();
    let plan_path = field(&planned, "plan");
    let plan: binary_alpha_app::retire::Plan =
        serde_json::from_slice(&fs::read(plan_path).unwrap()).unwrap();
    for root in &superseded_roots {
        assert!(
            plan.delete_local
                .iter()
                .any(|item| item.path == format!("manifests/{root}")),
            "superseded market closure must remain retireable"
        );
    }
    pipeline("retire", &config, &["--apply", plan_path]).unwrap();
    pipeline("retire", &config, &["--plan"]).unwrap();
    for (name, bytes) in &originals {
        assert_eq!(&fs::read(records.join(name)).unwrap(), bytes);
    }
    let fresh = f.scratch.path("fresh");
    // A fresh operator configuration has no references into the preserved producer backup.
    let config = f.scratch.path("restore-config/pipeline.toml");
    fs::create_dir_all(config.parent().unwrap().join("evidence")).unwrap();
    for file in ["deriv.toml", "evidence/deriv.json"] {
        fs::copy(f.scratch.path(file), config.parent().unwrap().join(file)).unwrap();
    }
    fs::write(
        &config,
        pipeline_toml(&fresh, &f.drive.base, &[("deriv", "deriv.toml")], None, 2),
    )
    .unwrap();
    fs::rename(&producer, f.scratch.path("producer.saved")).unwrap();
    let restored = pipeline(
        "restore",
        &config,
        &[
            "--catalog",
            second_id,
            "--sha256",
            sha,
            "--broker",
            "deriv",
            "--symbol",
            "frxEURUSD",
        ],
    )
    .unwrap();
    println!("{restored}");
    for (name, bytes) in originals {
        let path = fresh.join("pipeline_state/records").join(&name);
        assert!(
            path.exists(),
            "superseding archive omitted old pending evidence {name}"
        );
        assert_eq!(fs::read(path).unwrap(), bytes);
    }
    for root in &superseded_roots {
        assert!(
            !fresh
                .join("store")
                .join(binary_alpha_engine::dataset::manifest_key(root))
                .exists(),
            "superseded market roots are not a restore dependency"
        );
    }
    pipeline("archive", &config, &[]).unwrap();
    let planned = pipeline("retire", &config, &["--plan"]).unwrap();
    let plan: binary_alpha_app::retire::Plan =
        serde_json::from_slice(&fs::read(field(&planned, "plan")).unwrap()).unwrap();
    for root in superseded_roots {
        assert!(
            plan.references
                .iter()
                .any(|reference| reference.source.contains("-migration-root-")
                    && reference.closure == root
                    && reference.status == binary_alpha_app::retire::Status::Retired),
            "retirement still inventories superseded snapshot references"
        );
    }
    let planned = pipeline(
        "retire",
        &config,
        &["--job", "deriv", "--whole-job", "--plan"],
    )
    .unwrap();
    pipeline("retire", &config, &["--apply", field(&planned, "plan")]).unwrap();
    assert!(
        binary_alpha_app::store::Store::filesystem(fresh.join("store"))
            .list_manifests()
            .unwrap()
            .is_empty()
    );
}

#[test]
fn review_restored_evidence_revision_archives_after_retirement() {
    let f = fixture("review_restore_retired_catalog");
    let producer = f.scratch.path("producer");
    let config = f.scratch.path("one-job.toml");
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
    let imported = run(&[
        "data",
        "import",
        "--config",
        f.scratch.path("deriv-import.toml").to_str().unwrap(),
    ])
    .unwrap();
    let generation = imported_generation(&imported, "deriv:frxEURUSD");
    run(&[
        "data",
        "audit",
        "--config",
        f.scratch.path("deriv-import.toml").to_str().unwrap(),
        "--manifest",
        &format!(
            "file://{}",
            producer
                .join("store/manifests")
                .join(generation)
                .join("ready.json")
                .display()
        ),
    ])
    .unwrap();
    let first = pipeline("archive", &config, &[]).unwrap();
    let first_id = field(job_line(&first, "deriv"), "catalog").to_owned();
    fs::write(
        producer.join("pipeline_state/records/deriv-intent-added.json"),
        br#"{"job":"deriv","command":"update"}"#,
    )
    .unwrap();
    let second = pipeline("archive", &config, &[]).unwrap();
    let second_id = field(job_line(&second, "deriv"), "catalog").to_owned();
    let sha = field(job_line(&second, "deriv"), "sha256").to_owned();
    assert_ne!(first_id, second_id);
    let planned = pipeline("retire", &config, &["--plan"]).unwrap();
    let plan_path = field(&planned, "plan");
    let plan: binary_alpha_app::retire::Plan =
        serde_json::from_slice(&fs::read(plan_path).unwrap()).unwrap();
    assert!(plan.delete_drive.iter().any(|d| d.file_id == first_id));
    pipeline("retire", &config, &["--apply", plan_path]).unwrap();
    assert!(!f.drive.files().contains_key(&first_id));
    let fresh = f.scratch.path("fresh");
    fs::write(
        &config,
        pipeline_toml(&fresh, &f.drive.base, &[("deriv", "deriv.toml")], None, 3),
    )
    .unwrap();
    fs::rename(&producer, f.scratch.path("producer.saved")).unwrap();
    pipeline(
        "restore",
        &config,
        &[
            "--catalog",
            &second_id,
            "--sha256",
            &sha,
            "--broker",
            "deriv",
            "--symbol",
            "frxEURUSD",
        ],
    )
    .unwrap();
    let result = pipeline("archive", &config, &[]);
    println!("retired old catalog {first_id}; retained {second_id}; fresh archive: {result:?}");
    assert!(
        result.is_ok(),
        "fresh archive depends on deleted superseded catalog: {result:?}"
    );
}

#[test]
fn review_symlink_store_import_must_obey_writer_lock() {
    use std::fs::File;
    use std::os::unix::fs::symlink;
    let scratch = Scratch::new("review_symlink_store_lock");
    common::write_daily_directory(
        &scratch.path("sources/deriv/AUDUSD"),
        "AUDUSD",
        "AUDUSD",
        &[("2025-08-11", &[(1_754_870_400_000_000_000, 1.23456)])],
    );
    let managed = scratch.path("managed");
    let state = managed.join("pipeline_state");
    let physical = scratch.path("physical_objects");
    fs::create_dir_all(&state).unwrap();
    fs::create_dir_all(&physical).unwrap();
    fs::create_dir(managed.join("store")).unwrap();
    let config = scratch.path("import.toml");
    fs::write(&config, format!("schema_version=1\nrun_mode=\"research\"\n[storage]\nhistorical_data_dir=\"{}\"\npublication_uri=\"file://{}\"\n[[import.sources]]\nkind=\"tick_parquet_daily\"\npath=\"sources/deriv\"\nbroker=\"deriv\"\nrole=\"development\"\nprice_scale=5\ninstruments=[\"AUDUSD\"]\n", managed.join("store").display(), managed.join("store").display())).unwrap();
    let lock = File::create(state.join("writer.lock")).unwrap();
    lock.try_lock().unwrap();
    let normal = binary_alpha_app::import::run(&config, &mut Vec::new());
    assert!(
        normal
            .as_ref()
            .is_err_and(|e| e.contains("another producer")),
        "normal store locks: {normal:?}"
    );
    fs::remove_dir(managed.join("store")).unwrap();
    symlink(&physical, managed.join("store")).unwrap();
    let mut report = Vec::new();
    let outcome = binary_alpha_app::import::run(&config, &mut report);
    eprintln!(
        "import while writer locked: {outcome:?}; report: {}",
        String::from_utf8_lossy(&report)
    );
    assert!(
        outcome
            .as_ref()
            .is_err_and(|e| e.contains("symlinked managed store")),
        "import must reject the redirected managed store: {outcome:?}"
    );
    let alias = scratch.path("store-alias");
    symlink("managed/store", &alias).unwrap();
    let text = fs::read_to_string(&config).unwrap();
    fs::write(
        &config,
        text.replace(
            managed.join("store").to_str().unwrap(),
            alias.to_str().unwrap(),
        ),
    )
    .unwrap();
    let outcome = binary_alpha_app::import::run(&config, &mut Vec::new());
    assert!(
        outcome.unwrap_err().contains("symlinked managed store"),
        "an intermediate alias must not hide the managed store"
    );
    drop(lock);
    fs::set_permissions(&state, fs::Permissions::from_mode(0o755)).unwrap();
    let outcome = binary_alpha_app::import::run(&config, &mut Vec::new());
    assert!(outcome.unwrap_err().contains("symlinked managed store"));
    fs::write(
        &config,
        pipeline_toml(&managed, "http://127.0.0.1:1", &[], None, 1),
    )
    .unwrap();
    for command in ["migrate", "archive", "update", "retire"] {
        let args: &[&str] = if command == "retire" {
            &["--plan"]
        } else {
            &[]
        };
        let error = pipeline(command, &config, args).unwrap_err();
        assert!(
            error.contains("symlinked managed store"),
            "{command}: {error}"
        );
        assert_eq!(
            fs::read_dir(&physical).unwrap().count(),
            0,
            "{command} mutated the store"
        );
        assert_eq!(
            fs::read_dir(&state).unwrap().count(),
            1,
            "{command} mutated state"
        );
        assert_eq!(
            fs::metadata(&state).unwrap().permissions().mode() & 0o777,
            0o755
        );
    }
}

#[test]
fn review_budget_split_boundary_must_remain_resumable() {
    use binary_alpha_app::{fetch, store::Store};
    use binary_alpha_engine::config::Config;
    for conflict in [false, true] {
        let scratch = Scratch::new(&format!("review_budget_boundary_{conflict}"));
        let mut config = Config::parse(&deriv_core("ws://127.0.0.1/", 1, 1, 60)).unwrap();
        let history = config.history.as_mut().unwrap();
        history.overlap_seconds = None;
        history.start = time_text(0);
        history.end = time_text(10_000_000);
        let local = Store::filesystem(scratch.path("retained"));
        let destination = Store::filesystem(scratch.path("published"));
        let repeated = [(5, 100_005), (5, 100_005), (6, 100_006), (7, 100_007)];
        fetch::pass(
            &config,
            &mut ReviewPages::new(vec![review_page(&repeated), review_page(&[])]),
            &local,
            &destination,
            (0, 10_000_000),
            &mut Vec::new(),
        )
        .unwrap();
        let mut progress: Option<fetch::Progress> = None;
        let mut invalidated = false;
        let clock = FakeClock::default();
        let first = {
            let mut persist = |event: fetch::ProgressEvent<'_>| {
                match event {
                    fetch::ProgressEvent::Started(p) => progress = Some(p.clone()),
                    fetch::ProgressEvent::Page(p) => {
                        progress.as_mut().unwrap().pages.push(p.clone())
                    }
                    fetch::ProgressEvent::Invalidate => invalidated = true,
                    fetch::ProgressEvent::Received(_) => (),
                }
                Ok(())
            };
            let mut bounds = fetch::Bounds::none(&clock);
            bounds.max_pages = Some(1);
            bounds.persist = Some(&mut persist);
            fetch::acquire(
                &config,
                &mut ReviewPages::new(vec![review_page(&[
                    (5, 100_005),
                    (6, 100_006),
                    (7, 100_007),
                    (8, 100_008),
                    (9, 100_009),
                ])]),
                &local,
                &destination,
                fetch::Requested::Explicit {
                    start: 0,
                    end: 10_000_000,
                },
                &mut bounds,
                &mut Vec::new(),
            )
        };
        assert!(
            first.is_ok(),
            "consistent boundary split must checkpoint as pending: {first:?}"
        );
        let pending = first.unwrap().remove(0);
        assert!(pending.pending);
        let manifest = dataset(
            &scratch.path("published"),
            pending.generation.as_ref().unwrap(),
        );
        let expected: Vec<_> = [
            (5, 100_005),
            (5, 100_005),
            (6, 100_006),
            (7, 100_007),
            (8, 100_008),
            (9, 100_009),
        ]
        .into_iter()
        .map(|(t, p)| Tick {
            event_time_micros: t * 1_000_000,
            price_units: p,
        })
        .collect();
        assert_eq!(
            common::read_normalized_ticks(&scratch.path("published"), &manifest),
            expected
        );
        assert!(!invalidated);
        assert_eq!(progress.as_ref().unwrap().pages.len(), 1);
        let mut older = vec![
            (0, 100_000),
            (1, 100_001),
            (2, 100_002),
            (3, 100_003),
            (4, 100_004),
            (5, 100_005),
        ];
        if !conflict {
            older.push((5, 100_005));
        }
        let mut broker = ReviewPages::new(vec![review_page(&older)]);
        let mut persist = |event: fetch::ProgressEvent<'_>| {
            if matches!(event, fetch::ProgressEvent::Invalidate) {
                invalidated = true;
            }
            Ok(())
        };
        let mut bounds = fetch::Bounds::none(&clock);
        bounds.max_pages = Some(1);
        bounds.resume = progress;
        bounds.persist = Some(&mut persist);
        let resumed = fetch::acquire(
            &config,
            &mut broker,
            &local,
            &destination,
            fetch::Requested::Explicit {
                start: 0,
                end: 10_000_000,
            },
            &mut bounds,
            &mut Vec::new(),
        );
        assert_eq!(
            broker.anchors,
            vec![Some(5_000_000)],
            "same budget must advance beyond replay"
        );
        if conflict {
            assert!(resumed.unwrap_err().contains("inconsistent reread"));
            assert!(
                invalidated,
                "a completed boundary still rejects missing multiplicity"
            );
        } else {
            let outcome = resumed.unwrap().remove(0);
            assert!(!outcome.pending);
            assert!(!invalidated);
            let manifest = dataset(
                &scratch.path("published"),
                outcome.generation.as_ref().unwrap(),
            );
            let expected: Vec<_> = (0..10)
                .flat_map(|t| {
                    std::iter::repeat_n(
                        Tick {
                            event_time_micros: t * 1_000_000,
                            price_units: 100_000 + t,
                        },
                        if t == 5 { 2 } else { 1 },
                    )
                })
                .collect();
            assert_eq!(
                common::read_normalized_ticks(&scratch.path("published"), &manifest),
                expected
            );
        }
    }
}

use binary_alpha_app::broker::{
    self, Cancellation, Continuity, HistoryPage, HistoryRows, LiveEvent, MarketDataBroker,
};
use binary_alpha_engine::dataset::NativeGranularity;
use binary_alpha_engine::market::{InstrumentId, PriceScale};
use std::collections::VecDeque;
struct ReviewPages {
    pages: VecDeque<Result<HistoryPage, String>>,
    anchors: Vec<Option<i64>>,
    continuity: Continuity,
}
impl ReviewPages {
    fn new(pages: Vec<HistoryPage>) -> Self {
        Self {
            pages: pages.into_iter().map(Ok).collect(),
            anchors: Vec::new(),
            continuity: Continuity::default(),
        }
    }
}
impl MarketDataBroker for ReviewPages {
    fn discover(&mut self) -> Result<Vec<broker::DiscoveredInstrument>, String> {
        Ok(Vec::new())
    }
    fn history_page(
        &mut self,
        _: &InstrumentId,
        _: PriceScale,
        before: Option<i64>,
        _: NativeGranularity,
    ) -> Result<HistoryPage, String> {
        self.anchors.push(before);
        self.pages.pop_front().ok_or("unexpected page request")?
    }
    fn decode_history(
        &self,
        _: &InstrumentId,
        raw: &[u8],
        scale: PriceScale,
        native: NativeGranularity,
    ) -> Result<(Option<i32>, HistoryRows), String> {
        let rows: Vec<(i64, i64)> = serde_json::from_slice(raw).map_err(|e| e.to_string())?;
        if let NativeGranularity::Bar { period_seconds } = native {
            return Ok((
                Some(538),
                HistoryRows::Bars(
                    rows.iter()
                        .map(|&(t, p)| {
                            let price = p as f64 / scale.unit() as f64;
                            binary_alpha_engine::market::Bar {
                                provider: (),
                                start_unix_s: t / 1_000_000,
                                open: price,
                                high: price,
                                low: price,
                                close: price,
                                volume: 1.,
                                period_s: period_seconds,
                            }
                        })
                        .collect(),
                ),
            ));
        }
        Ok((
            None,
            HistoryRows::Ticks(
                rows.iter()
                    .map(|&(t, p)| Tick {
                        event_time_micros: t,
                        price_units: p,
                    })
                    .collect(),
            ),
        ))
    }
    fn subscribe(&mut self, _: &InstrumentId, _: PriceScale) -> Result<(), String> {
        Err("unused subscribe".into())
    }
    fn next_live(&mut self, _: i64) -> Result<Option<LiveEvent>, String> {
        Ok(None)
    }
    fn unsubscribe(&mut self, _: &InstrumentId) -> Result<Cancellation, String> {
        Err("unused unsubscribe".into())
    }
    fn reconnect(&mut self) -> Result<(), String> {
        Err("unused reconnect".into())
    }
    fn continuity(&self) -> &Continuity {
        &self.continuity
    }
}
/// A synthetic page of `(seconds, units)` rows; the fake adapter decodes it back exactly.
fn review_page(rows: &[(i64, i64)]) -> HistoryPage {
    review_page_micros(
        &rows
            .iter()
            .map(|&(t, p)| (t * 1_000_000, p))
            .collect::<Vec<_>>(),
    )
}
/// A synthetic page of `(microseconds, units)` rows.
fn review_page_micros(rows: &[(i64, i64)]) -> HistoryPage {
    HistoryPage {
        raw: serde_json::to_vec(&rows).unwrap(),
        anchor_token: None,
        receipt_micros: 0,
    }
}

use super::*;
use binary_alpha_app::daily::PageOccurrence;
use binary_alpha_engine::dataset::{daily::DayFamily, object_key};

fn migrated(f: &Fixture, job: &str) -> (Value, Value, Vec<PageOccurrence>) {
    let state = read_json(
        &f.scratch
            .path(&format!("producer/pipeline_state/{job}/migration.json")),
    );
    let record = read_json(
        &f.scratch
            .path("producer/pipeline_state/records")
            .join(state["record"].as_str().unwrap()),
    );
    let store = f.scratch.path("producer/store");
    let m = dataset(&store, state["dataset"].as_str().unwrap());
    let pages = m
        .day_inventory
        .iter()
        .filter(|d| d.family == DayFamily::Pages)
        .flat_map(|d| {
            binary_alpha_app::daily::read_pages(&store.join(d.object.as_ref().unwrap()), &d.date)
                .unwrap()
        })
        .collect();
    (state, record, pages)
}

fn assert_preserved(
    store: &Path,
    before: &GenerationManifest,
    after: &GenerationManifest,
    old_stream: &StreamManifest,
    new_stream: &StreamManifest,
) {
    assert_eq!(
        ticks(store, before),
        ticks(store, after),
        "all observation rows and repeated ticks survive"
    );
    for day in &before.day_inventory {
        let new = after
            .day_inventory
            .iter()
            .find(|d| d.family == day.family && d.date == day.date)
            .unwrap();
        assert_eq!(
            day.object, new.object,
            "unchanged day must retain its key: {} {:?}",
            day.date, day.family
        );
        if day.family == DayFamily::Pages
            && let Some(key) = &day.object
        {
            assert_eq!(
                binary_alpha_app::daily::read_pages(&store.join(key), &day.date).unwrap(),
                binary_alpha_app::daily::read_pages(
                    &store.join(new.object.as_ref().unwrap()),
                    &new.date
                )
                .unwrap()
            );
        }
    }
    assert_eq!(old_stream.streams, new_stream.streams);
    for day in &old_stream.day_inventory {
        assert!(
            new_stream.day_inventory.iter().any(|d| d == day),
            "candle day must remain exact: {}",
            day.date
        );
    }
}

fn updated(name: &str) -> Fixture {
    let f = fixture(name);
    import(&f.scratch.path("deriv-import.toml")).unwrap();
    fs::write(
        &f.pipeline,
        pipeline_toml(
            &f.scratch.path("producer"),
            &f.drive.base,
            &[("deriv", "deriv.toml")],
            None,
            2,
        ),
    )
    .unwrap();
    pipeline(
        "update",
        &f.pipeline,
        &["--end", &time_text((DERIV_SEED_END + 200) * 1_000_000)],
    )
    .unwrap();
    legacy_fixtures::freeze(&f);
    f
}

#[test]
fn review_other_store_update_honors_unfinished_retirement() {
    let f = fixture("review_other_store_fence");
    lineage::legacy_import(&f.scratch.path("deriv-import.toml")).unwrap();
    fs::write(
        &f.pipeline,
        pipeline_toml(
            &f.scratch.path("producer"),
            &f.drive.base,
            &[("deriv", "deriv.toml")],
            None,
            3,
        ),
    )
    .unwrap();
    pipeline("migrate", &f.pipeline, &[]).unwrap();
    let archived = pipeline("archive", &f.pipeline, &[]).unwrap();
    let line = job_line(&archived, "deriv");
    let other = f.scratch.path("other-producer.toml");
    let other_root = f.scratch.path("other-producer");
    fs::write(
        &other,
        pipeline_toml(
            &other_root,
            &f.drive.base,
            &[("deriv", "deriv.toml")],
            None,
            3,
        ),
    )
    .unwrap();
    pipeline(
        "restore",
        &other,
        &[
            "--catalog",
            field(line, "catalog"),
            "--sha256",
            field(line, "sha256"),
            "--broker",
            "deriv",
            "--symbol",
            "frxEURUSD",
        ],
    )
    .unwrap();
    let report = pipeline("retire", &f.pipeline, &["--plan"]).unwrap();
    let plan = PathBuf::from(report.split_whitespace().nth(2).unwrap());
    struct StopAfterProgress;
    impl Write for StopAfterProgress {
        fn write(&mut self, _: &[u8]) -> std::io::Result<usize> {
            Err(std::io::Error::other(
                "fixture interruption after deletion batch",
            ))
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    assert!(
        binary_alpha_app::retire::run(&f.pipeline, None, Some(&plan), &mut StopAfterProgress)
            .is_err()
    );
    assert!(plan.with_extension("progress.jsonseq").exists());
    let first = pipeline(
        "update",
        &other,
        &["--end", &time_text((DERIV_SEED_END + 120) * 1_000_000)],
    );
    eprintln!("archive from second store during unfinished retirement: {first:?}");
    if first.is_ok() {
        eprintln!("second archive: {:?}", pipeline("archive", &other, &[]));
    }
    for command in ["archive", "pull", "retire"] {
        let args: &[&str] = if command == "pull" {
            &["--broker", "deriv", "--symbol", "frxEURUSD"]
        } else {
            &[]
        };
        let blocked = pipeline(command, &other, args).unwrap_err();
        assert!(blocked.contains("unfinished retirement"), "{blocked}");
    }
    let resumed = pipeline("retire", &f.pipeline, &["--apply", plan.to_str().unwrap()]);
    eprintln!("resume after second producer: {resumed:?}");
    assert!(
        first.is_err(),
        "another store wrote through unfinished archive-root retirement"
    );
    assert!(
        resumed.is_ok(),
        "owner must resume after rejected foreign write: {resumed:?}"
    );
}

#[test]
fn review_upgrade_after_daily_update() {
    let f = fixture("review_upgrade_after_daily_update");
    lineage::legacy_import(&f.scratch.path("deriv-import.toml")).unwrap();
    let config = f.scratch.path("single.toml");
    fs::write(
        &config,
        pipeline_toml(
            &f.scratch.path("producer"),
            &f.drive.base,
            &[("deriv", "deriv.toml")],
            None,
            3,
        ),
    )
    .unwrap();
    pipeline("migrate", &config, &[]).unwrap();
    pipeline("archive", &config, &[]).unwrap();
    let report = pipeline(
        "update",
        &config,
        &["--end", &time_text((DERIV_SEED_END + 120) * 1_000_000)],
    )
    .unwrap();
    let original = field(job_line(&report, "deriv"), "dataset").to_string();
    let store = f.scratch.path("producer/store");
    let manifest = dataset(&store, &original);
    let prior_stream = stream(&store, field(job_line(&report, "deriv"), "stream"));
    eprintln!(
        "before upgrade dataset={} rows={} coverage={:?}",
        original, manifest.row_count, manifest.coverage
    );
    let state_path = f
        .scratch
        .path("producer/pipeline_state/deriv/migration.json");
    let mut state: Value = serde_json::from_slice(&fs::read(&state_path).unwrap()).unwrap();
    state["proof_version"] = json!(0);
    fs::write(&state_path, state.to_string()).unwrap();
    let result = pipeline("migrate", &config, &[]);
    eprintln!("upgrade result: {result:?}");
    assert!(
        result.is_ok(),
        "upgrade must preserve prior daily updates: {result:?}"
    );
    let result = pipeline("archive", &config, &[]).unwrap();
    let chosen = field(job_line(&result, "deriv"), "dataset").to_string();
    let upgraded = dataset(&store, &chosen);
    let upgraded_stream = stream(&store, field(job_line(&result, "deriv"), "stream"));
    assert_preserved(
        &store,
        &manifest,
        &upgraded,
        &prior_stream,
        &upgraded_stream,
    );
    eprintln!(
        "after upgrade dataset={} rows={} coverage={:?}",
        chosen, upgraded.row_count, upgraded.coverage
    );
    assert_eq!(
        upgraded.row_count, manifest.row_count,
        "proof upgrade lost prior daily observation rows"
    );

    let (_, record, pages) = migrated(&f, "deriv");
    assert_eq!(record["continuation_preservation"]["to_root"], chosen);
    for entry in fs::read_dir(f.scratch.path("producer/pipeline_state/records")).unwrap() {
        let path = entry.unwrap().path();
        if !path
            .file_name()
            .unwrap()
            .to_string_lossy()
            .contains("-receipt-")
        {
            continue;
        }
        for request in read_json(&path)["requests"].as_array().unwrap() {
            let occurrence = &request["occurrence"];
            if occurrence.is_null() {
                continue;
            }
            assert!(pages.iter().any(|p| p.acquisition_id
                == occurrence["acquisition_id"].as_str().unwrap()
                && p.ordinal == occurrence["ordinal"].as_u64().unwrap()
                && p.payload_sha256 == request["sha256"].as_str().unwrap()));
            assert!(
                !store
                    .join(object_key(request["sha256"].as_str().unwrap()))
                    .exists(),
                "upgrade must not recreate reclaimed standalone payloads"
            );
        }
    }
    let later = pipeline(
        "update",
        &config,
        &["--end", &time_text((DERIV_SEED_END + 240) * 1_000_000)],
    )
    .unwrap();
    let later = dataset(&store, field(job_line(&later, "deriv"), "dataset"));
    assert!(later.row_count > upgraded.row_count);
    assert!(ticks(&store, &later).starts_with(&ticks(&store, &upgraded)));
}
#[test]
fn review_upgrade_after_pending_daily_update() {
    let f = fixture("review_upgrade_after_pending_daily_update");
    let core = deriv_core(&f.deriv.url, 60, 1, 60);
    fs::write(f.scratch.path("deriv.toml"), &core).unwrap();
    write_evidence(&f.scratch, "deriv", &core);
    lineage::legacy_import(&f.scratch.path("deriv-import.toml")).unwrap();
    let config = f.scratch.path("single.toml");
    fs::write(
        &config,
        pipeline_toml(
            &f.scratch.path("producer"),
            &f.drive.base,
            &[("deriv", "deriv.toml")],
            None,
            3,
        ),
    )
    .unwrap();
    pipeline("migrate", &config, &[]).unwrap();
    pipeline("archive", &config, &[]).unwrap();
    let report = pipeline(
        "update",
        &config,
        &["--end", &time_text((DERIV_SEED_END + 1000) * 1_000_000)],
    )
    .unwrap_err();
    let original = field(job_line(&report, "deriv"), "dataset").to_string();
    let store = f.scratch.path("producer/store");
    let manifest = dataset(&store, &original);
    let prior_stream = stream(&store, field(job_line(&report, "deriv"), "stream"));
    eprintln!(
        "before upgrade dataset={} rows={} coverage={:?}",
        original, manifest.row_count, manifest.coverage
    );
    let state_path = f
        .scratch
        .path("producer/pipeline_state/deriv/migration.json");
    let mut state: Value = serde_json::from_slice(&fs::read(&state_path).unwrap()).unwrap();
    state["proof_version"] = json!(0);
    fs::write(&state_path, state.to_string()).unwrap();
    let result = pipeline("migrate", &config, &[]);
    eprintln!("upgrade result: {result:?}");
    assert!(
        result.is_ok(),
        "upgrade must preserve prior daily updates: {result:?}"
    );
    let result = pipeline("archive", &config, &[]).unwrap();
    let chosen = field(job_line(&result, "deriv"), "dataset").to_string();
    let upgraded = dataset(&store, &chosen);
    let upgraded_stream = stream(&store, field(job_line(&result, "deriv"), "stream"));
    assert_preserved(
        &store,
        &manifest,
        &upgraded,
        &prior_stream,
        &upgraded_stream,
    );
    eprintln!(
        "after upgrade dataset={} rows={} coverage={:?}",
        chosen, upgraded.row_count, upgraded.coverage
    );

    let consumer_root = f.scratch.path("fresh-consumer");
    let consumer_config = f.scratch.path("fresh-consumer.toml");
    fs::write(
        &consumer_config,
        pipeline_toml(&consumer_root, &f.drive.base, &[], None, 3),
    )
    .unwrap();
    let pulled = pipeline(
        "pull",
        &consumer_config,
        &["--broker", "deriv", "--symbol", "frxEURUSD"],
    )
    .unwrap();
    eprintln!("fresh pull result: {pulled}");
    let restored = dataset(&consumer_root.join("store"), &chosen);
    eprintln!("fresh restored rows={}", restored.row_count);
    let planned = pipeline("retire", &config, &["--plan"]).unwrap();
    let plan_path = PathBuf::from(planned.split_whitespace().nth(2).unwrap());
    let plan: binary_alpha_app::retire::Plan =
        serde_json::from_slice(&fs::read(&plan_path).unwrap()).unwrap();
    for day in &manifest.day_inventory {
        if let Some(key) = &day.object {
            assert!(!plan.delete_local.iter().any(|o| &o.path == key));
            assert!(plan.retained_drive.iter().any(|o| &o.key == key));
        }
    }
    pipeline("retire", &config, &["--apply", plan_path.to_str().unwrap()]).unwrap();
    let repulled = pipeline(
        "pull",
        &consumer_config,
        &["--broker", "deriv", "--symbol", "frxEURUSD"],
    )
    .unwrap();
    assert!(repulled.contains(&chosen));
    assert_eq!(
        ticks(&consumer_root.join("store"), &restored),
        ticks(&store, &upgraded)
    );
    assert_eq!(
        upgraded.row_count, manifest.row_count,
        "proof upgrade lost prior daily observation rows"
    );
}

#[test]
fn review_actual_migrated_alias_still_needed_by_foreign_receipt() {
    let f = updated("review_foreign_actual_alias");
    let records = f.scratch.path("producer/pipeline_state/records");
    let original = fs::read_dir(&records)
        .unwrap()
        .map(|p| p.unwrap().path())
        .find(|p| {
            p.file_name()
                .unwrap()
                .to_string_lossy()
                .starts_with("deriv-receipt-")
        })
        .unwrap();
    let mut receipt = read_json(&original);
    let request = receipt["requests"][0].clone();
    let key = object_key(request["sha256"].as_str().unwrap());

    let other_core = deriv_core(&f.deriv.url, 60, 50, 60)
        .replace("sources/deriv", "sources/other")
        .replace("EURUSD", "USDJPY");
    common::write_daily_directory(
        &f.scratch.path("sources/other/USDJPY"),
        "USDJPY",
        "frxUSDJPY",
        &[(
            "2025-08-12",
            &[
                (DAY2 * 1_000_000_000, 1.1),
                ((DAY2 + 2) * 1_000_000_000, 1.2),
            ],
        )],
    );
    let other_import = import_config(&f.scratch, "other", &other_core);
    let imported = lineage::legacy_import(&other_import).unwrap();
    let generation = imported_generation(&imported, "deriv:frxUSDJPY").to_string();
    fs::write(f.scratch.path("other.toml"), &other_core).unwrap();
    write_evidence(&f.scratch, "other", &other_core);
    let intent_name = "other-intent-shared-payload.json";
    let mut intent = read_json(&records.join(receipt["intent"].as_str().unwrap()));
    intent["job"] = json!("other");
    for seed in intent["seeds"].as_array_mut().unwrap() {
        seed["provider_symbol"] = json!("frxUSDJPY");
        seed["manifest"] = json!(format!(
            "file://{}/manifests/{generation}/ready.json",
            f.scratch.path("producer/store").display()
        ));
    }
    fs::write(records.join(intent_name), intent.to_string()).unwrap();
    receipt["job"] = json!("other");
    receipt["intent"] = json!(intent_name);
    receipt["acquisition_id"] = Value::Null;
    receipt["dataset_generation"] = json!(generation);
    receipt["stream_generation"] = Value::Null;
    receipt["catalog"] = Value::Null;
    receipt["coverage"]["provider_symbol"] = json!("frxUSDJPY");
    let mut request = request;
    request["receipt_time"] = json!("2026-01-01T00:00:00Z");
    request["occurrence"] = Value::Null;
    receipt["requests"] = json!([request]);
    fs::write(
        records.join("other-receipt-shared-payload.json"),
        receipt.to_string(),
    )
    .unwrap();
    pipeline("migrate", &f.pipeline, &["--job", "deriv"]).unwrap();
    let (_, record, pages) = migrated(&f, "deriv");
    assert!(
        record["storage_aliases"]
            .as_array()
            .unwrap()
            .contains(&json!(key))
    );
    assert!(pages.iter().any(|p| object_key(&p.payload_sha256) == key));
    pipeline("archive", &f.pipeline, &["--job", "deriv"]).unwrap();
    let report = pipeline("retire", &f.pipeline, &["--job", "deriv", "--plan"]).unwrap();
    let path = PathBuf::from(report.split_whitespace().nth(2).unwrap());
    let plan: binary_alpha_app::retire::Plan =
        serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
    eprintln!(
        "foreign payload candidate = {}",
        plan.delete_local.iter().any(|e| e.path == key)
    );
    pipeline("retire", &f.pipeline, &["--apply", path.to_str().unwrap()]).unwrap();
    let other_pipeline = f.scratch.path("other-pipeline.toml");
    fs::write(
        &other_pipeline,
        pipeline_toml(
            &f.scratch.path("producer"),
            &f.drive.base,
            &[("other", "other.toml")],
            None,
            2,
        ),
    )
    .unwrap();
    let result = pipeline("migrate", &other_pipeline, &[]);
    eprintln!("foreign migration result: {result:?}");
    assert!(f.scratch.path("producer/store").join(&key).exists());
    assert!(
        result.is_ok(),
        "retirement removed another instrument's receipt source: {result:?}"
    );
}

#[test]
fn review_unproved_legacy_supersession_requires_full_continuation_repair() {
    let f = fixture("review_legacy_supersession_repair");
    lineage::legacy_import(&f.scratch.path("deriv-import.toml")).unwrap();
    fs::write(
        &f.pipeline,
        pipeline_toml(
            &f.scratch.path("producer"),
            &f.drive.base,
            &[("deriv", "deriv.toml")],
            None,
            3,
        ),
    )
    .unwrap();
    pipeline("migrate", &f.pipeline, &[]).unwrap();
    let (mut state, original, _) = migrated(&f, "deriv");
    let update = pipeline(
        "update",
        &f.pipeline,
        &["--end", &time_text(DERIV_SEED_END * 1_000_000)],
    )
    .unwrap();
    let store = f.scratch.path("producer/store");
    let before = dataset(&store, field(job_line(&update, "deriv"), "dataset"));
    // Model the immutable receipt of a pre-fix executable: its strict v1 equality was
    // measured, but it did not measure the continuation before declaring supersession.
    let mut legacy_root = dataset(&store, state["dataset"].as_str().unwrap());
    let old_root = legacy_root.generation.clone();
    let position = legacy_root
        .objects
        .iter()
        .position(|o| o.path == "provenance/lineage.json")
        .unwrap();
    let mut provenance = read_json(&store.join(&legacy_root.objects[position].key));
    provenance["supersedes_generation"] = json!(old_root);
    let lineage_file = f.scratch.path("legacy-upgrade-lineage.json");
    fs::write(&lineage_file, provenance.to_string()).unwrap();
    legacy_root.objects[position] = common::daily::object(
        &store,
        "provenance/lineage.json",
        binary_alpha_engine::dataset::ObjectRole::Provenance,
        &lineage_file,
    );
    let legacy_manifest = common::daily::publish(&store, &mut legacy_root);
    let mut audit_report = Vec::new();
    binary_alpha_app::audit::run(
        &f.scratch.path("deriv-import.toml"),
        &format!("file://{}", legacy_manifest.display()),
        &mut audit_report,
    )
    .unwrap();
    let audit_report = String::from_utf8(audit_report).unwrap();
    let mut legacy = original.clone();
    legacy["v2_root"] = json!(legacy_root.generation);
    legacy["v2_stream"] = json!(field(&audit_report, "generation"));
    state["dataset"] = legacy["v2_root"].clone();
    state["stream"] = legacy["v2_stream"].clone();
    legacy["supersedes"] = state["record"].clone();
    legacy["proof_version"] = json!(1);
    legacy
        .as_object_mut()
        .unwrap()
        .remove("continuation_preservation");
    let name = "deriv-migration-legacy-upgrade.json";
    let path = f.scratch.path("producer/pipeline_state/records").join(name);
    let bytes = serde_json::to_vec(&legacy).unwrap();
    fs::write(&path, &bytes).unwrap();
    state["record"] = json!(name);
    state["proof_version"] = json!(1);
    fs::write(
        f.scratch
            .path("producer/pipeline_state/deriv/migration.json"),
        state.to_string(),
    )
    .unwrap();
    assert!(
        pipeline("archive", &f.pipeline, &[])
            .unwrap_err()
            .contains("missing verified migration evidence")
    );
    pipeline("migrate", &f.pipeline, &[]).unwrap();
    assert_eq!(
        fs::read(&path).unwrap(),
        bytes,
        "repair preserves immutable old receipt"
    );
    let (_, repaired, _) = migrated(&f, "deriv");
    assert_eq!(repaired["supersedes"], name);
    let archived = pipeline("archive", &f.pipeline, &[]).unwrap();
    let after = dataset(&store, field(job_line(&archived, "deriv"), "dataset"));
    assert_eq!(ticks(&store, &before), ticks(&store, &after));
}

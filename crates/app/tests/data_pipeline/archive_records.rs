//! Archived evidence survives successive acquisitions independently of the producer store.
use super::*;

fn cumulative_records(job: &str, broker: &str, symbol: &str, end: i64) {
    let f = fixture(&format!("archive_records_{job}"));
    let producer = f.scratch.path("producer");
    let config = f.scratch.path("records-pipeline.toml");
    fs::write(
        &config,
        pipeline_toml(
            &producer,
            &f.drive.base,
            &[(job, &format!("{job}.toml"))],
            None,
            3,
        ),
    )
    .unwrap();
    lineage::legacy_import(&f.scratch.path(&format!("{job}-import.toml"))).unwrap();
    pipeline("migrate", &config, &[]).unwrap();
    // Model the shared migration writer's verified predecessor ownership without changing
    // lineage.rs, which belongs to the other work groups.
    let record_dir = producer.join("pipeline_state/records");
    let migration = fs::read_dir(&record_dir)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .find(|path| {
            path.extension().is_some_and(|e| e == "json") && read_json(path)["phase"] == "verified"
        })
        .unwrap();
    let predecessor = format!("old-{job}");
    let mut proof = read_json(&migration);
    proof["predecessor_jobs"] = json!([predecessor]);
    fs::write(migration, serde_json::to_vec_pretty(&proof).unwrap()).unwrap();
    for (name, value) in [
        (
            "prior-intent.json",
            json!({"job": predecessor, "command": "update"}),
        ),
        (
            "prior-acquisition.json",
            json!({"intent": "prior-intent.json", "invocation": "fixture"}),
        ),
        (
            "prior-receipt.json",
            json!({"job": predecessor, "intent": "prior-intent.json", "acquisition_id": "prior-acquisition.json"}),
        ),
        (
            "unrelated.json",
            json!({"job": format!("{job}-other"), "command": "update"}),
        ),
    ] {
        fs::write(
            record_dir.join(name),
            serde_json::to_vec_pretty(&value).unwrap(),
        )
        .unwrap();
    }
    pipeline("archive", &config, &[]).unwrap();
    let mut final_catalog = String::new();
    let mut final_digest = String::new();
    for seconds in [120, 240] {
        pipeline(
            "update",
            &config,
            &["--end", &time_text((end + seconds) * 1_000_000)],
        )
        .unwrap();
        let report = pipeline("archive", &config, &[]).unwrap();
        final_catalog = field(job_line(&report, job), "catalog").into();
        final_digest = field(job_line(&report, job), "sha256").into();
    }
    let archived =
        Catalog::from_json(&f.drive.state.lock().unwrap().files[&final_catalog].bytes).unwrap();
    let originals: BTreeMap<String, Vec<u8>> =
        fs::read_dir(producer.join("pipeline_state/records"))
            .unwrap()
            .filter(|entry| entry.as_ref().unwrap().file_type().unwrap().is_file())
            .map(|entry| {
                let path = entry.unwrap().path();
                (
                    path.file_name().unwrap().to_str().unwrap().to_string(),
                    fs::read(path).unwrap(),
                )
            })
            .filter(|(name, _)| name != "unrelated.json")
            .collect();
    assert!(originals.keys().any(|name| name.contains("-migration-")));
    assert!(originals.keys().any(|name| name.contains("-intent-")));
    assert!(originals.keys().any(|name| name.contains("-acquisition-")));
    assert!(originals.keys().any(|name| name.contains("-receipt-")));
    let fresh = f.scratch.path("fresh-records");
    let fresh_config = f.scratch.path("fresh-records.toml");
    fs::write(
        &fresh_config,
        pipeline_toml(&fresh, &f.drive.base, &[], None, 3),
    )
    .unwrap();
    // Every original store/record URI is unavailable during recovery.
    fs::rename(&producer, f.scratch.path("producer.saved")).unwrap();
    let args = [
        "--catalog",
        &final_catalog,
        "--sha256",
        &final_digest,
        "--broker",
        broker,
        "--symbol",
        symbol,
    ];
    pipeline("restore", &fresh_config, &args).unwrap();
    assert!(!fresh.join("pipeline_state/records/unrelated.json").exists());
    for (name, bytes) in &originals {
        let restored = fresh.join("pipeline_state/records").join(name);
        assert!(restored.is_file(), "archive omitted evidence record {name}");
        assert_eq!(&fs::read(&restored).unwrap(), bytes, "record {name}");
        if serde_json::from_slice::<Value>(bytes).is_ok_and(|v| v["file_id"] == final_catalog) {
            assert_eq!(read_json(&restored)["sha256"], final_digest);
            assert_eq!(
                read_json(&restored)["bytes"],
                f.drive.files()[&final_catalog].bytes.len()
            );
            continue; // This receipt is derived from the pinned catalog, outside its inventory.
        }
        let entry = archived
            .records
            .iter()
            .find(|entry| entry.key == format!("records/{name}"))
            .unwrap();
        assert_eq!(entry.bytes, bytes.len() as u64);
        assert_eq!(
            entry.sha256,
            binary_alpha_engine::hex(&Sha256::digest(bytes))
        );
    }
    let acquisition_result = originals
        .iter()
        .find(|(_, bytes)| {
            serde_json::from_slice::<Value>(bytes).is_ok_and(|v| {
                v["dataset_generation"] == archived.dataset.generation
                    && v["catalog"].is_null()
                    && v["requests"].as_array().is_some_and(|r| !r.is_empty())
            })
        })
        .expect("the current acquisition result must precede catalog publication");
    let missing = fresh
        .join("pipeline_state/records")
        .join(acquisition_result.0);
    fs::remove_file(&missing).unwrap();
    pipeline(
        "pull",
        &fresh_config,
        &["--broker", broker, "--symbol", symbol],
    )
    .unwrap();
    assert_eq!(&fs::read(missing).unwrap(), acquisition_result.1);
    for entry in [&archived.dataset, &archived.stream] {
        run(&[
            "data",
            "verify",
            "--manifest",
            &format!("file://{}", fresh.join("store").join(&entry.key).display()),
        ])
        .unwrap();
    }

    // A same-length mutation must fail the pinned record hash, before ready publication.
    let entry = archived
        .records
        .iter()
        .find(|entry| entry.key == format!("records/{}", acquisition_result.0))
        .unwrap();
    f.drive
        .state
        .lock()
        .unwrap()
        .files
        .get_mut(&entry.file_id)
        .unwrap()
        .bytes[0] ^= 1;
    let damaged = f.scratch.path("damaged-records");
    fs::write(
        &fresh_config,
        pipeline_toml(&damaged, &f.drive.base, &[], None, 3),
    )
    .unwrap();
    let error = pipeline("restore", &fresh_config, &args).unwrap_err();
    assert!(
        error.contains(&entry.key) && error.contains("SHA-256"),
        "{error}"
    );
    assert!(!damaged.join("store").join(&archived.dataset.key).exists());
}

#[test]
fn deriv_migrate_update_twice_restores_every_record_and_rejects_tampering() {
    cumulative_records("deriv", "deriv", "frxEURUSD", DERIV_SEED_END);
}

#[test]
fn pocket_migrate_update_twice_restores_every_record_and_rejects_tampering() {
    cumulative_records("pocket", "pocket_option", "AEDCNY_otc", POCKET_SEED_END);
}

#[test]
fn archive_new_evidence_for_same_generation_and_pull_latest_closure() {
    let f = fixture("archive_records_same_generation");
    let producer = f.scratch.path("producer");
    let config = f.scratch.path("records-pipeline.toml");
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
    lineage::legacy_import(&f.scratch.path("deriv-import.toml")).unwrap();
    pipeline("migrate", &config, &[]).unwrap();
    let first = pipeline("archive", &config, &[]).unwrap();
    let first_id = field(job_line(&first, "deriv"), "catalog");
    let original_catalog = f.drive.state.lock().unwrap().files[first_id].bytes.clone();
    // Model a receipt created by the pre-closure implementation. A subsequent archive
    // carries it as evidence even if its old remote catalog is no longer available.
    let catalog = Catalog::from_json(&original_catalog).unwrap();
    let records = producer.join("pipeline_state/records");
    let receipt = fs::read_dir(&records)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .find(|path| {
            path.extension().is_some_and(|e| e == "json") && read_json(path)["file_id"] == first_id
        })
        .unwrap();
    fs::rename(
        receipt,
        records.join(format!(
            "deriv-catalog-{}-{}.json",
            &catalog.dataset.generation[..16],
            &catalog.stream.generation[..16]
        )),
    )
    .unwrap();
    // An invocation can publish an intent without changing market generations (including
    // no-new-range acquisitions). It must not be hidden by a cached catalog receipt.
    let name = "deriv-intent-new-invocation.json";
    let bytes = br#"{"schema_version":1,"job":"deriv","command":"update"}"#;
    fs::write(producer.join("pipeline_state/records").join(name), bytes).unwrap();
    let second = pipeline("archive", &config, &[]).unwrap();
    let second_id = field(job_line(&second, "deriv"), "catalog");
    assert_ne!(
        first_id, second_id,
        "new evidence requires a new immutable catalog"
    );
    assert_eq!(
        f.drive.state.lock().unwrap().files[first_id].bytes,
        original_catalog
    );
    let old = f
        .drive
        .state
        .lock()
        .unwrap()
        .files
        .remove(first_id)
        .unwrap();
    let count = f.drive.state.lock().unwrap().sessions.len();
    let retry = pipeline("archive", &config, &[]).unwrap();
    assert_eq!(field(job_line(&retry, "deriv"), "catalog"), second_id);
    assert_eq!(f.drive.state.lock().unwrap().sessions.len(), count);
    // Discovery order and remote duplicate IDs cannot determine which evidence is newest.
    {
        let mut remote = f.drive.state.lock().unwrap();
        let duplicate = RemoteEntry {
            name: old.name.clone(),
            bytes: old.bytes.clone(),
            trashed: false,
        };
        remote.files.insert("zzz-older-catalog".into(), duplicate);
    }
    let fresh = f.scratch.path("fresh");
    fs::write(&config, pipeline_toml(&fresh, &f.drive.base, &[], None, 3)).unwrap();
    pipeline(
        "pull",
        &config,
        &["--broker", "deriv", "--symbol", "frxEURUSD"],
    )
    .unwrap();
    assert_eq!(
        fs::read(fresh.join("pipeline_state/records").join(name)).unwrap(),
        bytes
    );
}

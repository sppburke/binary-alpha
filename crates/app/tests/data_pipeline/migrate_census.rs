use super::*;
use binary_alpha_app::daily::{PageDisposition, PageOccurrence, ReceiptState};
use binary_alpha_engine::dataset::{daily::DayFamily, object_key};
use std::fs::OpenOptions;

fn put(f: &Fixture, value: Value) -> (String, Vec<u8>) {
    let bytes = serde_json::to_vec(&value).unwrap();
    let key = object_key(&binary_alpha_engine::hex(&Sha256::digest(&bytes)));
    fs::write(f.scratch.path("producer/store").join(&key), &bytes).unwrap();
    (key, bytes)
}

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

fn diagnostic(job: &str) -> Value {
    if job == "deriv" {
        json!({"echo_req":{"ticks_history":"frxEURUSD"},"history":{"times":[DAY2-1,DAY2],"prices":["bad","1.2"]}})
    } else {
        json!({"asset":"AEDCNY_otc","data":[{"time":POCKET_START+POCKET_OFFSET_S}]})
    }
}

#[test]
fn physical_responses_are_preserved_and_unresolved_are_explicit() {
    for job in ["deriv", "pocket"] {
        let f = fixture("physical_census");
        lineage::legacy_import(&f.scratch.path(&format!("{job}-import.toml"))).unwrap();
        let (key, bytes) = put(&f, diagnostic(job));
        let (unknown, _) = put(&f, json!({"history":{"times":[DAY2],"prices":["1.2"]}}));
        pipeline("migrate", &f.pipeline, &["--job", job]).unwrap();
        let (state, record, pages) = migrated(&f, job);
        let page = pages
            .iter()
            .find(|p| p.payload == bytes)
            .expect("physical diagnostic must enter daily pages");
        assert_eq!(page.disposition, PageDisposition::Diagnostic);
        assert_eq!(page.receipt_state, ReceiptState::AbsentInLegacyRecord);
        assert_eq!(page.request_anchor_utc, None);
        assert_eq!(page.receipt_time_utc, None);
        assert_eq!(
            page.last_event_time,
            Some(if job == "deriv" { DAY2 } else { POCKET_START } * 1_000_000)
        );
        assert_eq!(
            record["proofs"]["pages"]["source_census"]["physical"]["diagnostic_objects"],
            1
        );
        assert!(
            record["unresolved_objects"]
                .as_array()
                .unwrap()
                .iter()
                .any(|v| v["key"] == unknown)
        );
        assert!(
            !record["storage_aliases"]
                .as_array()
                .is_some_and(|v| v.contains(&json!(unknown)))
        );
        assert!(f.scratch.path("producer/store").join(key).exists());
        assert!(f.deriv.requests().is_empty());
        assert!(f.pocket.requests().is_empty());
        assert!(f.drive.log().is_empty());
        let archive = pipeline("archive", &f.pipeline, &["--job", job]).unwrap();
        let line = job_line(&archive, job);
        let consumer = f.scratch.path("consumer.toml");
        let consumer_root = f.scratch.path("consumer");
        fs::write(
            &consumer,
            pipeline_toml(&consumer_root, &f.drive.base, &[], None, 2),
        )
        .unwrap();
        pipeline(
            "restore",
            &consumer,
            &[
                "--catalog",
                field(line, "catalog"),
                "--sha256",
                field(line, "sha256"),
                "--broker",
                if job == "deriv" {
                    "deriv"
                } else {
                    "pocket_option"
                },
                "--symbol",
                if job == "deriv" {
                    "frxEURUSD"
                } else {
                    "AEDCNY_otc"
                },
            ],
        )
        .unwrap();
        let restored = dataset(
            &consumer_root.join("store"),
            state["dataset"].as_str().unwrap(),
        );
        assert!(
            restored
                .day_inventory
                .iter()
                .filter(|d| d.family == DayFamily::Pages)
                .flat_map(|d| {
                    binary_alpha_app::daily::read_pages(
                        &consumer_root.join("store").join(d.object.as_ref().unwrap()),
                        &d.date,
                    )
                    .unwrap()
                })
                .any(|p| p.payload == bytes && p.disposition == PageDisposition::Diagnostic)
        );
        assert!(
            !consumer_root.join("store").join(unknown).exists(),
            "unresolved bytes must not gain retirement authority from an archive"
        );
    }
}

#[test]
fn attributable_physical_response_added_after_conversion_blocks_verified() {
    let f = fixture("physical_census_late");
    lineage::legacy_import(&f.scratch.path("deriv-import.toml")).unwrap();
    data_pipeline::migrate_with(
        &f.pipeline,
        Some("deriv"),
        &|_| Err("fixture stop".into()),
        &mut Vec::new(),
    )
    .unwrap_err();
    put(&f, diagnostic("deriv"));
    let error = pipeline("migrate", &f.pipeline, &["--job", "deriv"])
        .expect_err("physical census must independently reject omitted response");
    assert!(error.contains("physical"), "{error}");
}

#[test]
fn physical_response_without_day_is_unresolved_and_cannot_verify() {
    let f = fixture("physical_census_empty");
    lineage::legacy_import(&f.scratch.path("deriv-import.toml")).unwrap();
    let (key, _) = put(
        &f,
        json!({"echo_req":{"ticks_history":"frxEURUSD"},"history":{"times":[],"prices":[]}}),
    );
    let error = pipeline("migrate", &f.pipeline, &["--job", "deriv"])
        .expect_err("unknown day must not authorize retirement");
    assert!(error.contains("unresolved"), "{error}");
    let state = read_json(
        &f.scratch
            .path("producer/pipeline_state/deriv/migration.json"),
    );
    assert_ne!(state["phase"], "verified");
    assert!(
        state["unresolved_objects"]
            .as_array()
            .unwrap()
            .iter()
            .any(|v| v["key"] == key)
    );
}

#[test]
fn physical_error_response_is_unresolved_instead_of_another_source() {
    let f = fixture("physical_error_response");
    lineage::legacy_import(&f.scratch.path("deriv-import.toml")).unwrap();
    let (key, _) = put(
        &f,
        json!({"echo_req":{"ticks_history":"frxEURUSD"},"error":{"code":"InvalidRequest"}}),
    );
    let error = pipeline("migrate", &f.pipeline, &["--job", "deriv"]).unwrap_err();
    assert!(error.contains("unresolved attributable"), "{error}");
    let state = read_json(
        &f.scratch
            .path("producer/pipeline_state/deriv/migration.json"),
    );
    assert!(
        state["unresolved_objects"]
            .as_array()
            .unwrap()
            .iter()
            .any(|v| v["key"] == key && v["attributable"] == true)
    );
}

#[test]
fn physical_payload_cannot_choose_between_retained_source_contexts() {
    let f = fixture("physical_source_contexts");
    lineage::legacy_import(&f.scratch.path("deriv-import.toml")).unwrap();
    let records = f.scratch.path("producer/pipeline_state/records");
    fs::create_dir_all(&records).unwrap();
    fs::write(
        records.join("context.json"),
        json!({"seeds":[{"provider_symbol":"frxEURUSD","source_identity":"another-context"}]})
            .to_string(),
    )
    .unwrap();
    let (key, _) = put(&f, diagnostic("deriv"));
    pipeline("migrate", &f.pipeline, &["--job", "deriv"]).unwrap();
    let (_, record, pages) = migrated(&f, "deriv");
    assert!(pages.is_empty());
    assert!(
        record["unresolved_objects"]
            .as_array()
            .unwrap()
            .iter()
            .any(|v| v["key"] == key && v["reason"].as_str().unwrap().contains("source identity"))
    );
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
fn indexed_standalone_copies_are_storage_aliases_without_extra_occurrences() {
    let f = updated("physical_census_aliases");
    let records = f.scratch.path("producer/pipeline_state/records");
    let receipt = fs::read_dir(&records)
        .unwrap()
        .map(|p| p.unwrap().path())
        .find(|p| {
            p.file_name()
                .unwrap()
                .to_string_lossy()
                .starts_with("deriv-receipt-")
        })
        .unwrap();
    let mut receipt = read_json(&receipt);
    let mut request = receipt["requests"][0].clone();
    let received = binary_alpha_engine::market::parse_event_time_micros(
        request["receipt_time"].as_str().unwrap(),
    )
    .unwrap();
    request["receipt_time"] = json!(time_text(received + 1_000_000));
    receipt["requests"] = json!([request]);
    fs::write(
        records.join("deriv-receipt-distinct-occurrence.json"),
        receipt.to_string(),
    )
    .unwrap();
    pipeline("migrate", &f.pipeline, &[]).unwrap();
    let (_, record, pages) = migrated(&f, "deriv");
    let aliases = record["storage_aliases"]
        .as_array()
        .expect("standalone page keys must be recorded");
    assert!(!aliases.is_empty());
    assert_eq!(
        aliases.len() + 1,
        pages.len(),
        "same payload at two receipt times remains two occurrences"
    );
    for p in &pages {
        let key = object_key(&p.payload_sha256);
        assert!(aliases.contains(&json!(key)), "missing {key}");
        assert_eq!(
            fs::read(f.scratch.path("producer/store").join(key)).unwrap(),
            p.payload
        );
    }
    assert_eq!(
        record["proofs"]["pages"]["source_census"]["physical"]["diagnostic_objects"],
        0
    );
    assert_eq!(
        record["proofs"]["pages"]["source_census"]["receipt_requests"],
        pages.len()
    );
}

#[test]
fn predecessor_job_ownership_and_receipts_are_carried_to_current_job() {
    let f = updated("physical_census_predecessor");
    fs::copy(
        f.scratch.path("evidence/deriv.json"),
        f.scratch.path("evidence/deriv-frxeurusd.json"),
    )
    .unwrap();
    fs::write(
        &f.pipeline,
        pipeline_toml(
            &f.scratch.path("producer"),
            &f.drive.base,
            &[("deriv-frxeurusd", "deriv.toml")],
            None,
            2,
        ),
    )
    .unwrap();
    pipeline("migrate", &f.pipeline, &[]).unwrap();
    let (_, record, pages) = migrated(&f, "deriv-frxeurusd");
    assert_eq!(record["predecessor_jobs"], json!(["deriv"]));
    assert!(!pages.is_empty());
    assert!(
        pages
            .iter()
            .all(|p| p.intent.as_ref().unwrap().starts_with("deriv-"))
    );
}

#[test]
fn predecessor_with_foreign_generation_is_not_authorized() {
    let f = updated("physical_census_foreign_predecessor");
    let other = lineage::legacy_import(&f.scratch.path("pocket-import.toml")).unwrap();
    let other = imported_generation(&other, "pocket_option:AEDCNY_otc");
    let transfers = f
        .scratch
        .path("producer/pipeline_state/deriv/transfers.json");
    fs::write(&transfers, json!({"files":{format!("manifests/{other}/ready.json"):{"file_id":"foreign","done":true}}}).to_string()).unwrap();
    fs::copy(
        f.scratch.path("evidence/deriv.json"),
        f.scratch.path("evidence/current.json"),
    )
    .unwrap();
    fs::write(
        &f.pipeline,
        pipeline_toml(
            &f.scratch.path("producer"),
            &f.drive.base,
            &[("current", "deriv.toml")],
            None,
            2,
        ),
    )
    .unwrap();
    pipeline("migrate", &f.pipeline, &[]).unwrap();
    let (_, record, _) = migrated(&f, "current");
    assert!(
        !record["predecessor_jobs"]
            .as_array()
            .is_some_and(|v| v.contains(&json!("deriv")))
    );
    assert!(
        record["unresolved_predecessor_jobs"]
            .as_array()
            .unwrap()
            .iter()
            .any(|v| v["job"] == "deriv")
    );
}

#[test]
fn corrupt_standalone_copy_cannot_be_verified_from_bundle_prefix() {
    let f = updated("physical_census_corrupt_alias");
    let receipt = fs::read_dir(f.scratch.path("producer/pipeline_state/records"))
        .unwrap()
        .map(|p| p.unwrap().path())
        .find(|p| {
            p.file_name()
                .unwrap()
                .to_string_lossy()
                .starts_with("deriv-receipt-")
        })
        .unwrap();
    let receipt = read_json(&receipt);
    let key = object_key(receipt["requests"][0]["sha256"].as_str().unwrap());
    let path = f.scratch.path("producer/store").join(key);
    OpenOptions::new()
        .append(true)
        .open(path)
        .unwrap()
        .write_all(b"trailing corrupt bytes")
        .unwrap();
    let error = pipeline("migrate", &f.pipeline, &[])
        .expect_err("bundle proof must not hide corrupt standalone copy");
    assert!(
        error.contains("physical storage alias") && error.contains("whole-file bytes mismatch"),
        "{error}"
    );
}

#[test]
fn older_verified_proof_is_superseded_without_rewriting_record() {
    let f = fixture("physical_census_version");
    lineage::legacy_import(&f.scratch.path("deriv-import.toml")).unwrap();
    pipeline("migrate", &f.pipeline, &["--job", "deriv"]).unwrap();
    let (mut state, _, _) = migrated(&f, "deriv");
    let old_path = f
        .scratch
        .path("producer/pipeline_state/records")
        .join(state["record"].as_str().unwrap());
    let old = fs::read(&old_path).unwrap();
    state.as_object_mut().unwrap().remove("proof_version");
    fs::write(
        f.scratch
            .path("producer/pipeline_state/deriv/migration.json"),
        state.to_string(),
    )
    .unwrap();
    let (_, bytes) = put(&f, diagnostic("deriv"));
    let report = pipeline("migrate", &f.pipeline, &["--job", "deriv"]).unwrap();
    assert!(
        !report.contains("already_verified"),
        "older proof must run a new census"
    );
    let (new, record, pages) = migrated(&f, "deriv");
    assert!(new["proof_version"].as_u64().unwrap() > 0);
    assert_ne!(new["record"], state["record"]);
    assert_eq!(record["supersedes"], state["record"]);
    assert_eq!(fs::read(old_path).unwrap(), old);
    assert!(pages.iter().any(|p| p.payload == bytes));
    let report = pipeline("archive", &f.pipeline, &["--job", "deriv"]).unwrap();
    assert!(
        report.contains(new["dataset"].as_str().unwrap()),
        "superseding root must remain selectable: {report}"
    );
    let config = f.scratch.path("upgrade-update.toml");
    fs::write(
        &config,
        pipeline_toml(
            &f.scratch.path("producer"),
            &f.drive.base,
            &[("deriv", "deriv.toml")],
            None,
            2,
        ),
    )
    .unwrap();
    let update = pipeline(
        "update",
        &config,
        &["--end", &time_text((DERIV_SEED_END + 120) * 1_000_000)],
    )
    .unwrap();
    let m = dataset(
        &f.scratch.path("producer/store"),
        field(job_line(&update, "deriv"), "dataset"),
    );
    let lineage = m
        .objects
        .iter()
        .find(|o| o.path == "provenance/lineage.json")
        .unwrap();
    assert_eq!(
        read_json(&f.scratch.path("producer/store").join(&lineage.key))["root_generation"],
        new["dataset"]
    );
    assert!(
        m.day_inventory
            .iter()
            .filter(|d| d.family == DayFamily::Pages)
            .flat_map(|d| binary_alpha_app::daily::read_pages(
                &f.scratch
                    .path("producer/store")
                    .join(d.object.as_ref().unwrap()),
                &d.date
            )
            .unwrap())
            .any(|p| p.payload == bytes),
        "post-upgrade continuation lost recovered diagnostics"
    );
}

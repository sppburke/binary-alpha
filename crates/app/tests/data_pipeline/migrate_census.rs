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

// ----------------------------------------------------------------------------------------------
// Whole-day native-bar completeness on a first-time migration (#35)
// ----------------------------------------------------------------------------------------------

use binary_alpha_app::retire::Plan;
use binary_alpha_engine::dataset::coverage::{CoverageRange, DailyCoverage, DayCoverage};
use binary_alpha_engine::dataset::daily::{DayInventoryEntry, DayState};
use binary_alpha_engine::dataset::manifest_key;

/// 2025-05-17T00:00:00Z: two full UTC bar days precede the seed's first historical Pocket bar.
const GRID_START: i64 = POCKET_START / 86_400 * 86_400 - 2 * 86_400;
/// 2025-05-19T00:00:00Z, the day the fake broker's history reaches into.
const GRID_END: i64 = GRID_START + 2 * 86_400;
const DAY_A: &str = "2025-05-17";
const DAY_B: &str = "2025-05-18";
const DAY_C: &str = "2025-05-19";
const GRID_BASIS: &str = "validated complete native-bar grid";
const CUTOFF_REASON: &str =
    "verified acquisition covers only part of this day; cutoff day may receive later input";

fn range(from: i64, to: i64) -> CoverageRange {
    CoverageRange::new(from * 1_000_000, to * 1_000_000)
}

/// Pocket sources whose imported seed holds two full UTC bar days and stops ten minutes before
/// the fake broker's history begins, so the newest v1 baseline is `broker_history`, its verified
/// acquisition range covers only the tail of the second day, and the first day has no claim.
fn grid_fixture(name: &str, edit: impl FnOnce(&mut Vec<BarRow>)) -> (Fixture, Vec<String>) {
    let mut f = fixture(name);
    f.pocket = serve_broker(Kind::Pocket {
        from: GRID_END - 3_600,
        to: GRID_END + 7_200,
    });
    let core = pocket_core(&f.pocket.url, "demo", BAR_GRANULARITY, 60, 50, 60).replace(
        "start = \"2025-05-19T11:15:00Z\"",
        "start = \"2025-05-17T00:00:00Z\"",
    );
    assert!(core.contains("2025-05-17T00:00:00Z"));
    fs::write(f.scratch.path("pocket.toml"), &core).unwrap();
    import_config(&f.scratch, "pocket", &core);
    write_evidence(&f.scratch, "pocket", &core);
    let mut rows = bar_rows(GRID_START, GRID_END - 600);
    edit(&mut rows);
    write_collection(
        &f.scratch.path("sources/pocket"),
        &[AssetSpec {
            asset: "AEDCNY_otc",
            expected_symbol_id: None,
            symbol_id: Some(POCKET_SYMBOL_ID),
            files: vec![rows],
            metadata: true,
        }],
    );
    import(&f.scratch.path("pocket-import.toml")).unwrap();
    fs::write(
        &f.pipeline,
        pipeline_toml(
            &f.scratch.path("producer"),
            &f.drive.base,
            &[("pocket", "pocket.toml")],
            None,
            2,
        ),
    )
    .unwrap();
    pipeline(
        "update",
        &f.pipeline,
        &["--end", &time_text((GRID_END + 3_600) * 1_000_000)],
    )
    .unwrap();
    let legacy = legacy_fixtures::freeze(&f).into_values().collect();
    (f, legacy)
}

fn typed(store: &Path, generation: &str) -> (GenerationManifest, DailyCoverage) {
    let m = dataset(store, generation);
    let object = m
        .objects
        .iter()
        .find(|o| o.path == "provenance/coverage.json")
        .unwrap();
    let coverage = DailyCoverage::from_json(&fs::read(store.join(&object.key)).unwrap()).unwrap();
    coverage.check_manifest(&m).unwrap();
    (m, coverage)
}

fn observation<'a>(m: &'a GenerationManifest, date: &str) -> &'a DayInventoryEntry {
    m.day_inventory
        .iter()
        .find(|d| d.family == DayFamily::Observations && d.date == date)
        .unwrap()
}

fn evidence<'a>(c: &'a DailyCoverage, date: &str) -> &'a DayCoverage {
    c.days
        .iter()
        .find(|d| d.family == DayFamily::Observations && d.date == date)
        .unwrap()
}

fn observations(m: &GenerationManifest) -> Vec<&DayInventoryEntry> {
    m.day_inventory
        .iter()
        .filter(|d| d.family == DayFamily::Observations)
        .collect()
}

/// Requires `--nocapture` to appear: the compact inventory the issue asks for.
fn print_inventory(m: &GenerationManifest, c: &DailyCoverage) {
    println!("date family rows first last state reason verified unresolved");
    for d in observations(m) {
        let e = evidence(c, &d.date);
        println!(
            "{} {} {} {} {} {} {:?} {:?} {:?}",
            d.date,
            d.family,
            d.rows,
            d.first_time.as_deref().unwrap_or("-"),
            d.last_time.as_deref().unwrap_or("-"),
            d.state,
            d.reason,
            e.verified
                .iter()
                .map(|r| format!("{}..{}", r.start, r.end))
                .collect::<Vec<_>>(),
            e.unresolved
                .iter()
                .map(|r| format!("{}..{}", r.start, r.end))
                .collect::<Vec<_>>()
        );
    }
}

fn assert_complete_grid(
    m: &GenerationManifest,
    c: &DailyCoverage,
    date: &str,
    from: i64,
    basis: &str,
) {
    let d = observation(m, date);
    assert_eq!(
        (d.rows, d.state, &d.reason),
        (17_280, DayState::Complete, &None),
        "{date}"
    );
    assert!(d.unresolved.is_empty(), "{date}: {:?}", d.unresolved);
    let e = evidence(c, date);
    assert_eq!(e.verified, vec![range(from, from + 86_400)], "{date}");
    assert!(e.unresolved.is_empty() && e.reason.is_none(), "{date}");
    assert!(e.basis.contains(basis), "{}", e.basis);
}

/// Day C is the cutoff day: verified from midnight to the requested end, unresolved after it.
fn assert_cutoff_day(m: &GenerationManifest, c: &DailyCoverage, end: i64, reason: &str) {
    let d = observation(m, DAY_C);
    assert_eq!(
        (d.rows, d.state),
        ((end - GRID_END) as u64 / 5, DayState::Partial)
    );
    assert!(
        d.reason.as_deref().is_some_and(|r| r.contains(reason)),
        "{:?}",
        d.reason
    );
    let e = evidence(c, DAY_C);
    assert_eq!(e.verified, vec![range(GRID_END, end)]);
    assert_eq!(e.unresolved, vec![range(end, GRID_END + 86_400)]);
}

#[test]
fn whole_day_bar_completeness() {
    let (f, _) = grid_fixture("grid_completeness", |_| ());
    let store = f.scratch.path("producer/store");
    let (broker_requests, drive_requests) = (f.pocket.requests().len(), f.drive.log().len());
    let report = pipeline("migrate", &f.pipeline, &["--job", "pocket"]).unwrap();
    assert!(report.contains(" status verified "), "{report}");
    assert_eq!(
        f.pocket.requests().len(),
        broker_requests,
        "migration contacted the broker"
    );
    assert_eq!(
        f.drive.log().len(),
        drive_requests,
        "migration contacted Drive"
    );
    let (state, _, _) = migrated(&f, "pocket");
    let root = state["dataset"].as_str().unwrap().to_string();
    let (m, c) = typed(&store, &root);
    print_inventory(&m, &c);
    assert_eq!(
        observations(&m)
            .iter()
            .map(|d| d.date.as_str())
            .collect::<Vec<_>>(),
        [DAY_A, DAY_B, DAY_C]
    );
    // Day A has no acquisition claim at all; day B's claim covers only its last eleven minutes.
    assert_complete_grid(&m, &c, DAY_A, GRID_START, GRID_BASIS);
    assert_complete_grid(&m, &c, DAY_B, GRID_START + 86_400, GRID_BASIS);
    assert_cutoff_day(&m, &c, GRID_END + 3_600, CUTOFF_REASON);
    let [migration] = c
        .acquisitions
        .iter()
        .filter(|a| a.acquisition_id.starts_with("migration-source:"))
        .collect::<Vec<_>>()[..]
    else {
        panic!("one migration-source claim: {:?}", c.acquisitions);
    };
    assert_eq!(
        migration.requested,
        vec![range(GRID_START, GRID_END + 86_400)]
    );
    assert_eq!(
        migration.verified,
        vec![range(GRID_START, GRID_END + 3_600)]
    );
    assert_eq!(
        migration.unresolved,
        vec![range(GRID_END + 3_600, GRID_END + 86_400)]
    );
    // The retained v1 history claim is exactly the legacy record's own request.
    let newest = state["newest"].as_str().unwrap();
    let legacy = coverage(&store, &dataset(&store, newest));
    let history = c
        .acquisitions
        .iter()
        .find(|a| a.acquisition_id == format!("v1-history:{newest}"))
        .unwrap();
    assert_eq!(history.requested[0].start, legacy.requested.start);
    assert_eq!(history.requested[0].end, legacy.requested.end);
    assert_eq!(
        history.verified[0].start,
        legacy.verified.as_ref().unwrap().start
    );
    assert_eq!(
        history.verified[0].end,
        legacy.verified.as_ref().unwrap().end
    );
    assert!(history.verified[0].bounds().unwrap().0 > (GRID_START + 86_400) * 1_000_000);
    // Candle days close only through a later observation: both full days finalize, day C stays open.
    let candles = stream(&store, state["stream"].as_str().unwrap());
    let candle_state = |date: &str| {
        candles
            .day_inventory
            .iter()
            .find(|d| d.date == date)
            .map(|d| d.state)
            .unwrap()
    };
    assert_eq!(candle_state(DAY_A), DayState::Complete);
    assert_eq!(candle_state(DAY_B), DayState::Complete);
    assert_eq!(candle_state(DAY_C), DayState::Partial);
    // A verified checkpoint of the current proof is reused byte for byte.
    let checkpoint = f
        .scratch
        .path("producer/pipeline_state/pocket/migration.json");
    let record_path = f
        .scratch
        .path("producer/pipeline_state/records")
        .join(state["record"].as_str().unwrap());
    let (checkpoint_bytes, record_bytes) = (
        fs::read(&checkpoint).unwrap(),
        fs::read(&record_path).unwrap(),
    );
    let report = pipeline("migrate", &f.pipeline, &["--job", "pocket"]).unwrap();
    assert!(report.contains(" status already_verified "), "{report}");
    assert_eq!(fs::read(&checkpoint).unwrap(), checkpoint_bytes);
    assert_eq!(fs::read(&record_path).unwrap(), record_bytes);
    // Superseding an older proof re-derives the baseline under the rule the predecessor's basis
    // recorded, reproducing its inventory and coverage: the shared-identity claim cannot
    // conflict and no root is relabelled through supersession.
    let mut older = state.clone();
    older["proof_version"] = json!(0);
    fs::write(&checkpoint, older.to_string()).unwrap();
    let report = pipeline("migrate", &f.pipeline, &["--job", "pocket"]).unwrap();
    assert!(report.contains(" status verified "), "{report}");
    let (superseding, new_record, _) = migrated(&f, "pocket");
    assert_ne!(superseding["dataset"], root);
    assert_eq!(new_record["supersedes"], state["record"]);
    assert_eq!(fs::read(&record_path).unwrap(), record_bytes);
    // The baseline differs from the predecessor only by its lineage's `supersedes_generation`.
    let baseline = new_record["continuation_preservation"]["closures"]
        .as_array()
        .unwrap()
        .iter()
        .map(|c| c["dataset"].as_str().unwrap())
        .find(|g| *g != root)
        .expect("superseding baseline closure");
    let (b, bc) = typed(&store, baseline);
    assert_eq!(observations(&b), observations(&m));
    assert_eq!(
        bc, c,
        "baseline re-derives the predecessor's coverage byte for byte"
    );
    let (final_root, final_coverage) = typed(&store, superseding["dataset"].as_str().unwrap());
    assert_eq!(observations(&final_root), observations(&m));
    assert_eq!(
        final_coverage
            .acquisitions
            .iter()
            .find(|a| a.acquisition_id == migration.acquisition_id),
        Some(migration)
    );
}

#[test]
fn whole_day_bar_completeness_boundaries() {
    // One missing interior slot on day A; day B starts one slot late. Endpoints and counts alone
    // never promote a day.
    let (f, _) = grid_fixture("grid_boundaries", |rows| {
        rows.retain(|r| r.unix != GRID_START + 43_200 && r.unix != GRID_START + 86_400);
    });
    let store = f.scratch.path("producer/store");
    pipeline("migrate", &f.pipeline, &["--job", "pocket"]).unwrap();
    let (state, _, _) = migrated(&f, "pocket");
    let (m, c) = typed(&store, state["dataset"].as_str().unwrap());
    print_inventory(&m, &c);
    let a = observation(&m, DAY_A);
    assert_eq!(
        (a.rows, a.state, a.reason.as_deref()),
        (
            17_279,
            DayState::Unknown,
            Some("source has no whole-day completeness claim")
        )
    );
    assert_eq!(
        evidence(&c, DAY_A).unresolved,
        vec![range(GRID_START, GRID_START + 86_400)]
    );
    let b = observation(&m, DAY_B);
    assert_eq!(
        (b.rows, b.state, b.reason.as_deref()),
        (
            17_279,
            DayState::Partial,
            Some("verified acquisition covers only part of this day")
        )
    );
    assert_eq!(
        b.first_time.as_deref(),
        Some(time_text((GRID_START + 86_400 + 5) * 1_000_000).as_str())
    );
    let newest = state["newest"].as_str().unwrap();
    let verified_start = coverage(&store, &dataset(&store, newest))
        .verified
        .unwrap()
        .start;
    assert_eq!(
        evidence(&c, DAY_B).unresolved,
        vec![CoverageRange {
            start: time_text((GRID_START + 86_400) * 1_000_000),
            end: verified_start,
        }]
    );
    assert_cutoff_day(&m, &c, GRID_END + 3_600, CUTOFF_REASON);
}

#[test]
fn whole_day_bar_completeness_lifecycle() {
    let (f, legacy) = grid_fixture("grid_lifecycle", |_| ());
    let store = f.scratch.path("producer/store");
    pipeline("migrate", &f.pipeline, &["--job", "pocket"]).unwrap();
    let (state, _, _) = migrated(&f, "pocket");
    let root = state["dataset"].as_str().unwrap().to_string();
    let (m, c) = typed(&store, &root);
    // Archive, then restore the pinned catalog into a fresh store and verify it independently.
    let archive = pipeline("archive", &f.pipeline, &["--job", "pocket"]).unwrap();
    let line = job_line(&archive, "pocket");
    let consumer_root = f.scratch.path("consumer");
    let consumer = f.scratch.path("consumer.toml");
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
            "pocket_option",
            "--symbol",
            "AEDCNY_otc",
        ],
    )
    .unwrap();
    let restored_store = consumer_root.join("store");
    for generation in [&root, state["stream"].as_str().unwrap()] {
        run(&[
            "data",
            "verify",
            "--manifest",
            &format!(
                "file://{}/manifests/{generation}/ready.json",
                restored_store.display()
            ),
        ])
        .unwrap();
    }
    let (restored, restored_coverage) = typed(&restored_store, &root);
    assert_eq!((restored, restored_coverage), (m.clone(), c.clone()));
    // A later update carries the whole-day verified spans and every retained claim forward.
    let update = pipeline(
        "update",
        &f.pipeline,
        &["--end", &time_text((GRID_END + 5_400) * 1_000_000)],
    )
    .unwrap();
    let descendant = field(job_line(&update, "pocket"), "dataset").to_string();
    assert_ne!(descendant, root);
    let (d, dc) = typed(&store, &descendant);
    print_inventory(&d, &dc);
    let cumulative = "cumulative verified acquisition ranges";
    assert_complete_grid(&d, &dc, DAY_A, GRID_START, cumulative);
    assert_complete_grid(&d, &dc, DAY_B, GRID_START + 86_400, cumulative);
    assert_cutoff_day(
        &d,
        &dc,
        GRID_END + 5_400,
        "acquisition evidence does not cover the whole UTC day",
    );
    for claim in &c.acquisitions {
        assert_eq!(
            dc.acquisitions
                .iter()
                .find(|a| a.acquisition_id == claim.acquisition_id),
            Some(claim),
            "{}",
            claim.acquisition_id
        );
    }
    // Ordinary retirement removes the proved v1 closure and keeps the continuation verifiable.
    pipeline("archive", &f.pipeline, &["--job", "pocket"]).unwrap();
    let planned = pipeline("retire", &f.pipeline, &["--plan"]).unwrap();
    let path = PathBuf::from(field(&planned, "plan"));
    let plan: Plan = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
    for generation in &legacy {
        assert!(
            plan.delete_local
                .iter()
                .any(|d| d.path == format!("manifests/{generation}")),
            "v1 manifest not planned: {generation}"
        );
    }
    pipeline("retire", &f.pipeline, &["--apply", path.to_str().unwrap()]).unwrap();
    for generation in &legacy {
        assert!(!store.join(manifest_key(generation)).exists());
    }
    assert!(store.join(manifest_key(&root)).exists());
    for generation in [&descendant, field(job_line(&update, "pocket"), "stream")] {
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

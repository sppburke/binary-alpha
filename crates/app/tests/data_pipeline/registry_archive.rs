//! Archive-root journal and daily closure proof, using only the loopback Drive and fixtures.
use super::*;
use binary_alpha_app::{
    daily,
    drive::{Drive, DriveSettings},
    registry::Registry,
    store,
};
use binary_alpha_engine::{
    dataset::{self, Layout, ObjectRole},
    research::Access,
};
use common::daily as fixture;

/// Replay the public on-disk format for both new tests and existing recovery assertions.
pub(super) fn registry_state(directory: &Path) -> Value {
    let mut state = read_json(&directory.join("snapshot.json"));
    let watermark = state["sequence"].as_u64().unwrap();
    for line in fs::read_to_string(directory.join("events.ndjson"))
        .unwrap()
        .lines()
    {
        let record: Value = serde_json::from_str(line).unwrap();
        if record["sequence"].as_u64().unwrap() <= watermark {
            continue;
        }
        let change = &record["change"];
        if change["kind"] == "put" {
            state["files"][change["key"].as_str().unwrap()] = change["entry"].clone();
        }
        if change["kind"] == "remove" {
            state["files"]
                .as_object_mut()
                .unwrap()
                .remove(change["key"].as_str().unwrap());
        }
        if change["kind"] == "legacy" {
            state["legacy"][change["alias"].as_str().unwrap()] = change["entry"].clone();
        }
        state["sequence"] = record["sequence"].clone();
    }
    state
}

fn settings(fake: &FakeDrive) -> DriveSettings {
    DriveSettings {
        root_folder_id: "fixture-root".into(),
        credential: None,
        chunk_bytes: 262144,
        request_timeout_seconds: 2,
        max_attempts: 3,
        retry_seconds: Some(1),
        loopback_endpoint: Some(fake.base.clone()),
    }
}
fn bytes_fixture(scratch: &Scratch, name: &str) -> (PathBuf, store::ObjectIdentity) {
    let path = scratch.path(name);
    fs::write(&path, vec![42; 300_000]).unwrap();
    let identity = store::identify(&path).unwrap();
    (path, identity)
}
fn transfer(
    registry: &Registry,
    drive: &mut Drive,
    path: &Path,
    identity: &store::ObjectIdentity,
) -> Result<String, String> {
    registry.transfer(
        drive,
        &format!("job/{}", dataset::object_key(&identity.sha256)),
        &format!("object-{}", identity.sha256),
        path,
        identity,
    )
}
fn uploads(fake: &FakeDrive) -> usize {
    fake.state.lock().unwrap().files.len()
}

#[test]
fn registry_reservation_and_session_survive_restart_and_rebuild() {
    for after_session in [false, true] {
        let scratch = Scratch::new(if after_session {
            "registry_session"
        } else {
            "registry_reservation"
        });
        let fake = serve_drive();
        let settings = settings(&fake);
        let mut drive = Drive::open(&settings).unwrap();
        let state = scratch.path("pipeline_state");
        let registry = Registry::open(&state, &settings, &mut drive).unwrap();
        let (path, identity) = bytes_fixture(&scratch, "content");
        let key = dataset::object_key(&identity.sha256);
        fake.set(if after_session {
            DriveFaults {
                unavailable_uploads: usize::MAX,
                ..Default::default()
            }
        } else {
            DriveFaults {
                forbidden_begins: Some(("forbidden", usize::MAX)),
                ..Default::default()
            }
        });
        assert!(transfer(&registry, &mut drive, &path, &identity).is_err());
        let before = registry_state(&state.join("registry"));
        let entry = &before["files"][&key];
        assert_eq!(entry["done"], false);
        assert_eq!(entry["session"].is_string(), after_session);
        assert_eq!(uploads(&fake), 0);
        drop(registry);
        fake.set(DriveFaults::default());
        let registry = Registry::open(&state, &settings, &mut drive).unwrap();
        registry.rebuild(&mut drive).unwrap();
        assert_eq!(
            registry_state(&state.join("registry"))["files"][&key],
            *entry
        );
        let id = transfer(&registry, &mut drive, &path, &identity).unwrap();
        assert_eq!(id, entry["file_id"]);
        assert_eq!(uploads(&fake), 1);
        assert_eq!(fake.state.lock().unwrap().sessions.len(), 1);
        assert_eq!(
            registry_state(&state.join("registry"))["files"][&key]["done"],
            true
        );
    }
}

#[test]
fn registry_complete_paginated_rebuild_confirms_duplicate_names_and_readback() {
    let scratch = Scratch::new("registry_rebuild");
    let fake = serve_drive();
    let settings = settings(&fake);
    let mut drive = Drive::open(&settings).unwrap();
    let state = scratch.path("pipeline_state");
    let registry = Registry::open(&state, &settings, &mut drive).unwrap();
    let (path, identity) = bytes_fixture(&scratch, "content");
    let id = transfer(&registry, &mut drive, &path, &identity).unwrap();
    let name = format!("object-{}", identity.sha256);
    {
        let mut remote = fake.state.lock().unwrap();
        for (id, bytes) in [
            ("a-wrong", vec![1; identity.bytes as usize]),
            ("b-other", vec![2]),
            ("c-other", vec![3]),
        ] {
            remote.files.insert(
                id.into(),
                RemoteEntry {
                    name: name.clone(),
                    bytes,
                    trashed: false,
                },
            );
        }
    }
    drop(registry);
    fs::remove_dir_all(state.join("registry")).unwrap();
    fake.set(DriveFaults {
        incomplete_listing: true,
        ..Default::default()
    });
    assert!(
        Registry::open(&state, &settings, &mut drive)
            .err()
            .unwrap()
            .contains("incompleteSearch")
    );
    assert!(
        registry_state(&state.join("registry"))["files"]
            .as_object()
            .unwrap()
            .is_empty()
    );
    fake.set(DriveFaults {
        omit_sha256: true,
        reject_page_token_once: true,
        ..Default::default()
    });
    let registry = Registry::open(&state, &settings, &mut drive).unwrap();
    let before = uploads(&fake);
    assert_eq!(
        transfer(&registry, &mut drive, &path, &identity).unwrap(),
        id
    );
    assert_eq!(uploads(&fake), before);
    let entries = registry_state(&state.join("registry"));
    assert_eq!(entries["files"].as_object().unwrap().len(), 1);
    assert_eq!(
        entries["files"][dataset::object_key(&identity.sha256)]["file_id"],
        id
    );
    assert!(!fake.state.lock().unwrap().faults.reject_page_token_once);
}

#[test]
fn registry_imports_legacy_completed_and_open_sessions_once() {
    let scratch = Scratch::new("registry_legacy");
    let fake = serve_drive();
    let settings = settings(&fake);
    let mut drive = Drive::open(&settings).unwrap();
    let state = scratch.path("pipeline_state");
    fs::create_dir_all(state.join("job")).unwrap();
    let (path, identity) = bytes_fixture(&scratch, "content");
    let ids = drive.generate_ids(2).unwrap();
    drive
        .upload(
            &ids[0],
            &format!("object-{}", identity.sha256),
            &path,
            &identity,
            None,
            &mut |_| Ok(()),
        )
        .unwrap();
    let other = scratch.path("other");
    fs::write(&other, b"a pending legacy object").unwrap();
    let other_identity = store::identify(&other).unwrap();
    let mut session = None;
    assert!(
        drive
            .upload(
                &ids[1],
                &format!("object-{}", other_identity.sha256),
                &other,
                &other_identity,
                None,
                &mut |value| {
                    session = value.map(str::to_string);
                    Err("fixture crash after session creation".into())
                }
            )
            .is_err()
    );
    let legacy = json!({ "files": {
        dataset::object_key(&identity.sha256): {"file_id":ids[0],"done":true},
        dataset::object_key(&other_identity.sha256): {"file_id":ids[1],"done":false,"session":session}
    }});
    fs::write(state.join("job/transfers.json"), legacy.to_string()).unwrap();
    let registry = Registry::open(&state, &settings, &mut drive).unwrap();
    assert_eq!(
        transfer(&registry, &mut drive, &path, &identity).unwrap(),
        ids[0]
    );
    assert_eq!(
        registry
            .transfer(
                &mut drive,
                &format!(
                    "different-job/{}",
                    dataset::object_key(&other_identity.sha256)
                ),
                &format!("object-{}", other_identity.sha256),
                &other,
                &other_identity
            )
            .unwrap(),
        ids[1]
    );
    assert_eq!(uploads(&fake), 2);
    assert_eq!(fake.state.lock().unwrap().sessions.len(), 2);
    drop(registry);
    // A completed import is not reparsed or allowed to regress a now-completed session.
    fs::write(
        state.join("job/transfers.json"),
        "invalid obsolete checkpoint",
    )
    .unwrap();
    Registry::open(&state, &settings, &mut drive).unwrap();
}

fn archive_fixture(
    name: &str,
    pocket: bool,
    jobs: bool,
) -> (Scratch, FakeDrive, fixture::Pair, PathBuf) {
    let scratch = Scratch::new(name);
    let pair = fixture::pair(&scratch, pocket);
    let fake = serve_drive();
    let mut core = if pocket {
        pocket_core(
            "ws://127.0.0.1:9/socket.io/",
            "demo",
            BAR_GRANULARITY,
            60,
            50,
            60,
        )
    } else {
        synthetic_deriv(deriv_core("ws://127.0.0.1:9", 60, 50, 60))
    };
    let start = core.find("[[instruments]]").unwrap();
    let end = core.find("[[brokers]]").unwrap();
    core.replace_range(start..end, &format!("{}\n", pair.instrument()));
    fs::write(scratch.path("core.toml"), &core).unwrap();
    fs::create_dir_all(scratch.path("evidence")).unwrap();
    write_evidence(&scratch, "first", &core);
    write_evidence(&scratch, "second", &core);
    fs::create_dir_all(scratch.path("producer")).unwrap();
    fs::rename(scratch.path("published"), scratch.path("producer/store")).unwrap();
    let config = scratch.path("pipeline.toml");
    fs::write(
        &config,
        pipeline_toml(
            &scratch.path("producer"),
            &fake.base,
            if jobs {
                &[("first", "core.toml"), ("second", "core.toml")]
            } else {
                &[("first", "core.toml")]
            },
            None,
            3,
        ),
    )
    .unwrap();
    audit_local(&scratch, &pair.v2.generation);
    (scratch, fake, pair, config)
}
fn remote_catalogs(fake: &FakeDrive) -> Vec<(String, Catalog)> {
    fake.state
        .lock()
        .unwrap()
        .files
        .iter()
        .filter(|(_, e)| e.name.starts_with("catalog-"))
        .map(|(id, e)| (id.clone(), Catalog::from_json(&e.bytes).unwrap()))
        .collect()
}

#[test]
fn archive_parallel_jobs_share_daily_objects_and_descendant_uploads_only_changes() {
    let (scratch, fake, pair, config) = archive_fixture("registry_parallel_archive", false, true);
    let report = pipeline("archive", &config, &[]).unwrap();
    assert!(report.contains("pipeline archive first"));
    assert!(report.contains("pipeline archive second"));
    let catalogs = remote_catalogs(&fake);
    assert_eq!(catalogs.len(), 2);
    assert_eq!(catalogs[0].1.objects, catalogs[1].1.objects);
    assert_eq!(catalogs[0].1.dataset, catalogs[1].1.dataset);
    assert_eq!(catalogs[0].1.stream, catalogs[1].1.stream);
    assert_eq!(uploads(&fake), catalogs[0].1.objects.len() + 4);
    let state = fake.state.lock().unwrap();
    for entry in &catalogs[0].1.objects {
        assert_eq!(
            state
                .sessions
                .values()
                .filter(|s| s.id == entry.file_id)
                .count(),
            1
        );
    }
    drop(state);
    let before = uploads(&fake);
    pipeline("archive", &config, &[]).unwrap();
    assert_eq!(uploads(&fake), before);
    let root = scratch.path("producer/store");
    let mut descendant = pair.v2.clone();
    let mut changed = Vec::new();
    for (index, page) in pair.pages.iter().enumerate() {
        let original = page.clone();
        let mut page = original.clone();
        page.ordinal += 2;
        page.payload = format!("{{\"descendant_page\":{index}}}").into_bytes();
        page.payload_sha256 = binary_alpha_engine::hex(&Sha256::digest(&page.payload));
        let date = fixture::date(page.partition_time().unwrap());
        let path = scratch.path("changed.parquet");
        daily::write_pages(&path, &date, [vec![original, page]]).unwrap();
        let object = fixture::object(
            &root,
            &format!("pages/{date}.parquet"),
            ObjectRole::Source,
            &path,
        );
        descendant
            .objects
            .iter_mut()
            .find(|o| o.path == object.path)
            .map(|o| *o = object.clone())
            .unwrap();
        let day = descendant
            .day_inventory
            .iter_mut()
            .find(|d| d.family == dataset::DayFamily::Pages && d.date == date)
            .unwrap();
        day.object = Some(object.key.clone());
        day.rows += 1;
        changed.push(object.key);
    }
    assert_eq!(changed.len(), 2);
    // New requests can leave observation coverage unchanged. The lineage, not lexical
    // generation order, identifies the descendant; retain its predecessor throughout.
    for nonce in 0..256 {
        let path = scratch.path("lineage.json");
        fs::write(
            &path,
            json!({"parent": pair.v2.generation, "fixture_nonce": nonce}).to_string(),
        )
        .unwrap();
        let object = fixture::object(
            &root,
            "provenance/lineage.json",
            ObjectRole::Provenance,
            &path,
        );
        descendant.objects.retain(|o| o.path != object.path);
        descendant.objects.push(object);
        fixture::publish(&root, &mut descendant);
        if descendant.generation < pair.v2.generation {
            break;
        }
        fs::remove_file(root.join(descendant.key())).unwrap();
    }
    assert!(descendant.generation < pair.v2.generation);
    changed.push(
        descendant
            .objects
            .iter()
            .find(|o| o.path == "provenance/lineage.json")
            .unwrap()
            .key
            .clone(),
    );
    audit_local(&scratch, &descendant.generation);
    pipeline("archive", &config, &["--job", "first"]).unwrap();
    let catalogs = remote_catalogs(&fake);
    let latest = &catalogs
        .iter()
        .find(|(_, c)| c.dataset.generation == descendant.generation)
        .unwrap()
        .1;
    let old = &catalogs
        .iter()
        .find(|(_, c)| c.dataset.generation == pair.v2.generation)
        .unwrap()
        .1;
    let new_keys: Vec<_> = latest
        .objects
        .iter()
        .filter(|o| !old.objects.iter().any(|prior| prior.key == o.key))
        .map(|o| o.key.clone())
        .collect();
    let stream = stream(&root, &latest.stream.generation);
    let profile = stream
        .objects
        .iter()
        .find(|o| o.path == "profile.json")
        .unwrap();
    changed.push(profile.key.clone()); // Per-generation metadata names the new dataset.
    changed.sort();
    let mut actual = new_keys.clone();
    actual.sort();
    assert_eq!(actual, changed);
    assert_eq!(uploads(&fake) - before, new_keys.len() + 3);
    let after = uploads(&fake);
    pipeline("archive", &config, &["--job", "first"]).unwrap();
    assert_eq!(uploads(&fake), after);
    // Deleting the registry rebuilds from all files without changing an immutable receipt.
    fs::remove_dir_all(scratch.path("producer/pipeline_state/registry")).unwrap();
    pipeline("archive", &config, &["--job", "first"]).unwrap();
    assert_eq!(uploads(&fake), after);
    let consumer = scratch.path("descendant-consumer.toml");
    fs::write(
        &consumer,
        pipeline_toml(
            &scratch.path("descendant-consumer"),
            &fake.base,
            &[],
            None,
            3,
        ),
    )
    .unwrap();
    let pulled = pipeline(
        "pull",
        &consumer,
        &["--broker", "deriv", "--symbol", "R_50"],
    )
    .unwrap();
    assert!(pulled.contains(&descendant.generation), "{pulled}");
    assert!(
        scratch
            .path("descendant-consumer/store")
            .join(pair.v2.key())
            .exists()
    );
}

#[test]
fn archive_daily_fresh_restore_verifies_every_partition_for_both_brokers() {
    for pocket in [false, true] {
        let (scratch, fake, pair, config) = archive_fixture(
            if pocket {
                "registry_restore_pocket"
            } else {
                "registry_restore_deriv"
            },
            pocket,
            false,
        );
        pipeline("archive", &config, &[]).unwrap();
        let catalogs = remote_catalogs(&fake);
        let (id, catalog) = &catalogs[0];
        assert_eq!(catalog.layout, Some(Layout::DailyV2));
        let broker = pair.v2.broker.to_string();
        let symbol = pair.v2.provider_symbol.to_string();
        let listing =
            pipeline("list", &config, &["--broker", &broker, "--symbol", &symbol]).unwrap();
        assert!(listing.contains("layout daily-v2"));
        let consumer = scratch.path("consumer.toml");
        fs::write(
            &consumer,
            pipeline_toml(&scratch.path("consumer"), &fake.base, &[], None, 3),
        )
        .unwrap();
        let sha256 = field(&listing, "sha256");
        pipeline(
            "restore",
            &consumer,
            &[
                "--catalog",
                id,
                "--sha256",
                sha256,
                "--broker",
                &broker,
                "--symbol",
                &symbol,
            ],
        )
        .unwrap();
        let restored = scratch.path("consumer/store");
        for entry in &catalog.objects {
            let identity = store::identify(&restored.join(&entry.key)).unwrap();
            assert_eq!(
                (identity.bytes, identity.sha256),
                (entry.bytes, entry.sha256.clone())
            );
        }
        for entry in [&catalog.dataset, &catalog.stream] {
            common::verify(&restored.join(&entry.key)).unwrap();
        }
        assert!(!restored.join(pair.v1.key()).exists());
        assert_eq!(
            binary_alpha_app::lineage::newest_daily(
                &store::Store::filesystem(&restored),
                &pair.v2.instrument,
                pair.v2.role,
                Access::ORDINARY
            )
            .unwrap(),
            pair.v2.generation
        );
        let pulled = pipeline(
            "pull",
            &consumer,
            &["--broker", &broker, "--symbol", &symbol],
        )
        .unwrap();
        assert!(pulled.contains("already local"));
        // A bad daily partition must be observed by full closure verification.
        let daily = catalog
            .objects
            .iter()
            .find(|o| {
                pair.v2
                    .day_inventory
                    .iter()
                    .any(|d| d.object.as_ref() == Some(&o.key))
            })
            .unwrap();
        fs::write(restored.join(&daily.key), b"corrupted daily fixture").unwrap();
        assert!(
            pipeline(
                "pull",
                &consumer,
                &["--broker", &broker, "--symbol", &symbol]
            )
            .is_err()
        );
    }
}

#[test]
fn restored_daily_seed_updates_and_archives_without_legacy_data() {
    let (scratch, fake, pair, config) = archive_fixture("restore_update", true, false);
    pipeline("archive", &config, &[]).unwrap();
    let (id, original) = remote_catalogs(&fake).remove(0);
    let producer = scratch.path("producer/store");
    fs::remove_file(producer.join(pair.v1.key())).unwrap();
    for object in &pair.v1.objects {
        if !pair
            .v2
            .objects
            .iter()
            .any(|retained| retained.key == object.key)
        {
            fs::remove_file(producer.join(&object.key)).unwrap();
        }
    }
    let from = pair.bars.last().unwrap().start_unix_s + 5;
    let to = from + 30;
    let broker = serve_broker(Kind::Pocket { from: from - 5, to });
    let core = fs::read_to_string(scratch.path("core.toml"))
        .unwrap()
        .replace("ws://127.0.0.1:9/socket.io/", &broker.url)
        .replace("overlap_seconds = 60", "overlap_seconds = 1");
    fs::write(scratch.path("core.toml"), &core).unwrap();
    write_evidence(&scratch, "first", &core);
    let consumer = scratch.path("consumer.toml");
    fs::write(
        &consumer,
        pipeline_toml(
            &scratch.path("consumer"),
            &fake.base,
            &[("first", "core.toml")],
            None,
            3,
        ),
    )
    .unwrap();
    let hash = binary_alpha_engine::hex(&Sha256::digest(&fake.files()[&id].bytes));
    pipeline(
        "restore",
        &consumer,
        &[
            "--catalog",
            &id,
            "--sha256",
            &hash,
            "--broker",
            "pocket_option",
            "--symbol",
            "AEDCNY_otc",
        ],
    )
    .unwrap();
    let root = scratch.path("consumer/store");
    assert!(!root.join(pair.v1.key()).exists());
    let result = pipeline("update", &consumer, &["--end", &time_text(to * 1_000_000)]).unwrap();
    let line = job_line(&result, "first");
    assert_eq!(field(line, "status"), "archived");
    let generation = field(line, "dataset");
    let updated = dataset(&root, generation);
    let mut expected: Vec<Bar> = pair
        .bars
        .iter()
        .map(|bar| Bar {
            start_unix_s: bar.start_unix_s,
            open: bar.open,
            high: bar.high,
            low: bar.low,
            close: bar.close,
            volume: bar.volume,
            period_s: bar.period_s,
            provider: (),
        })
        .collect();
    expected.extend((from..to).step_by(5).map(|start_unix_s| {
        let [open, high, low, close, volume] = synthetic_bar(start_unix_s);
        Bar {
            start_unix_s,
            open,
            high,
            low,
            close,
            volume,
            period_s: 5,
            provider: (),
        }
    }));
    assert_eq!(
        bars(&root, &updated),
        expected,
        "every restored partition and appended provider row must survive update"
    );
    let lineage = updated
        .objects
        .iter()
        .find(|object| object.path == binary_alpha_app::lineage::LINEAGE_PATH)
        .unwrap();
    assert_eq!(
        read_json(&root.join(&lineage.key))["continuation"]["seed"]["generation"],
        pair.v2.generation
    );
    common::verify(&root.join(updated.key())).unwrap();
    assert!(!root.join(pair.v1.key()).exists());
    assert!(!broker.requests().is_empty());
    assert!(broker.forbidden().is_empty());
    let catalogs = remote_catalogs(&fake);
    let (catalog_id, archived) = catalogs
        .iter()
        .find(|(_, c)| c.dataset.generation == generation)
        .unwrap();
    assert_eq!(catalog_id, field(line, "catalog"));
    assert_eq!(archived.stream.generation, field(line, "stream"));
    let remote = fake.files();
    for object in &archived.objects {
        let bytes = fs::read(root.join(&object.key)).unwrap();
        assert_eq!(remote[&object.file_id].bytes, bytes);
        assert_eq!(
            binary_alpha_engine::hex(&Sha256::digest(&bytes)),
            object.sha256
        );
        assert_eq!(bytes.len() as u64, object.bytes);
        if let Some(prior) = original
            .objects
            .iter()
            .find(|prior| prior.key == object.key)
        {
            assert_eq!(
                object.file_id, prior.file_id,
                "retained objects reuse their original archive binding"
            );
        }
    }
    // Assert a specific retained page, so the reuse oracle cannot pass on an empty intersection.
    let page_key = pair
        .v2
        .day_inventory
        .iter()
        .find(|day| day.family == dataset::DayFamily::Pages)
        .unwrap()
        .object
        .as_ref()
        .unwrap();
    assert_eq!(
        archived
            .objects
            .iter()
            .find(|o| &o.key == page_key)
            .unwrap(),
        original
            .objects
            .iter()
            .find(|o| &o.key == page_key)
            .unwrap(),
    );
}

#[test]
fn restore_rejects_hash_consistent_later_dataset_and_stream_inventory() {
    for pocket in [false, true] {
        let (scratch, fake, pair, config) = archive_fixture(
            if pocket {
                "restore_invalid_pocket"
            } else {
                "restore_invalid_deriv"
            },
            pocket,
            false,
        );
        pipeline("archive", &config, &[]).unwrap();
        let original = remote_catalogs(&fake)[0].1.clone();
        for is_stream in [false, true] {
            let kind = if is_stream { "stream" } else { "dataset" };
            let mut catalog = original.clone();
            let entry = if is_stream {
                &mut catalog.stream
            } else {
                &mut catalog.dataset
            };
            let mut remote = fake.state.lock().unwrap();
            let mut manifest: Value =
                serde_json::from_slice(&remote.files[&entry.file_id].bytes).unwrap();
            let day = manifest["day_inventory"]
                .as_array_mut()
                .unwrap()
                .iter_mut()
                .rev()
                .find(|day| day["date"] == fixture::LAST && day["rows"].as_u64().unwrap() > 1)
                .unwrap();
            let expected_day: dataset::DayInventoryEntry =
                serde_json::from_value(day.clone()).unwrap();
            let first = binary_alpha_engine::market::parse_event_time_micros(
                day["first_time"].as_str().unwrap(),
            )
            .unwrap();
            // Keep every object and aggregate row count intact. Only the later partition's
            // declared first time lies: byte hashes and manifest validation still succeed.
            day["first_time"] = json!(time_text(first + 1));
            let bytes = serde_json::to_vec(&manifest).unwrap();
            if is_stream {
                StreamManifest::from_json(&bytes).unwrap();
            } else {
                GenerationManifest::from_json(&bytes).unwrap();
            }
            entry.file_id = format!("invalid-{kind}-manifest");
            entry.bytes = bytes.len() as u64;
            entry.sha256 = binary_alpha_engine::hex(&Sha256::digest(&bytes));
            remote.files.insert(
                entry.file_id.clone(),
                RemoteEntry {
                    name: format!("manifest-{}.json", entry.generation),
                    bytes,
                    trashed: false,
                },
            );
            let bytes = serde_json::to_vec(&catalog).unwrap();
            Catalog::from_json(&bytes).unwrap();
            let hash = binary_alpha_engine::hex(&Sha256::digest(&bytes));
            let id = format!("invalid-{kind}-catalog");
            remote.files.insert(
                id.clone(),
                RemoteEntry {
                    name: format!("catalog-{id}.json"),
                    bytes,
                    trashed: false,
                },
            );
            drop(remote);
            let consumer = scratch.path(&format!("consumer-{kind}.toml"));
            let root = scratch.path(&format!("consumer-{kind}"));
            fs::write(&consumer, pipeline_toml(&root, &fake.base, &[], None, 3)).unwrap();
            let error = pipeline(
                "restore",
                &consumer,
                &[
                    "--catalog",
                    &id,
                    "--sha256",
                    &hash,
                    "--broker",
                    pair.v2.broker.as_str(),
                    "--symbol",
                    pair.v2.provider_symbol.as_str(),
                ],
            )
            .unwrap_err();
            assert!(
                error.contains(&format!(
                    "{}: day inventory mismatch",
                    expected_day.logical_path().unwrap()
                )),
                "{kind}: {error}"
            );
            // All bytes arrived with the exact pinned identities: rejection is semantic.
            for object in &catalog.objects {
                let identity = store::identify(&root.join("store").join(&object.key)).unwrap();
                assert_eq!(
                    (identity.bytes, identity.sha256),
                    (object.bytes, object.sha256.clone())
                );
            }
        }
    }
}

#[test]
fn pull_prefers_daily_layout_over_legacy_at_equal_coverage() {
    let (scratch, fake, pair, config) = archive_fixture("registry_pull_tie", false, false);
    pipeline("archive", &config, &[]).unwrap();
    let root = scratch.path("producer/store");
    let mut legacy = pair.v1.clone();
    // Make the legacy generation sort higher, so generation-only ordering provably fails.
    for nonce in 0..100 {
        let path = scratch.path("legacy-note.json");
        fs::write(&path, nonce.to_string()).unwrap();
        let note = fixture::object(&root, "provenance/note.json", ObjectRole::Provenance, &path);
        legacy.objects.retain(|o| o.path != note.path);
        legacy.objects.push(note);
        fixture::publish(&root, &mut legacy);
        if legacy.generation > pair.v2.generation {
            break;
        }
    }
    assert!(legacy.generation > pair.v2.generation);
    common::verify(&root.join(legacy.key())).unwrap();
    let legacy_stream = stream(&root, &audit_local(&scratch, &legacy.generation));
    let mut catalog = remote_catalogs(&fake)[0].1.clone();
    catalog.layout = None;
    catalog.job = "legacy".into();
    let mut remote = fake.state.lock().unwrap();
    catalog.objects.clear();
    for object in legacy.objects.iter().chain(&legacy_stream.objects) {
        if catalog.objects.iter().any(|o| o.key == object.key) {
            continue;
        }
        let id = format!("legacy-object-{}", object.sha256);
        remote.files.insert(
            id.clone(),
            RemoteEntry {
                name: format!("object-{}", object.sha256),
                bytes: fs::read(root.join(&object.key)).unwrap(),
                trashed: false,
            },
        );
        catalog.objects.push(data_pipeline::ObjectEntry {
            key: object.key.clone(),
            sha256: object.sha256.clone(),
            bytes: object.bytes,
            file_id: id,
        });
    }
    for (generation, entry) in [
        (&legacy.generation, &mut catalog.dataset),
        (&legacy_stream.generation, &mut catalog.stream),
    ] {
        let key = dataset::manifest_key(generation);
        let identity = store::identify(&root.join(&key)).unwrap();
        let id = format!("legacy-manifest-{generation}");
        remote.files.insert(
            id.clone(),
            RemoteEntry {
                name: format!("manifest-{generation}.json"),
                bytes: fs::read(root.join(&key)).unwrap(),
                trashed: false,
            },
        );
        *entry = data_pipeline::ManifestEntry {
            generation: generation.clone(),
            key,
            sha256: identity.sha256,
            bytes: identity.bytes,
            file_id: id,
        };
    }
    remote.files.insert(
        "legacy-catalog".into(),
        RemoteEntry {
            name: "catalog-legacy.json".into(),
            bytes: serde_json::to_vec(&catalog).unwrap(),
            trashed: false,
        },
    );
    drop(remote);
    fake.set(DriveFaults {
        omit_sha256: true,
        ..Default::default()
    });
    let consumer = scratch.path("consumer.toml");
    fs::write(
        &consumer,
        pipeline_toml(&scratch.path("consumer"), &fake.base, &[], None, 3),
    )
    .unwrap();
    let report = pipeline(
        "pull",
        &consumer,
        &["--broker", "deriv", "--symbol", "R_50"],
    )
    .unwrap();
    assert!(report.contains(&pair.v2.generation), "{report}");
    assert!(!scratch.path("consumer/store").join(legacy.key()).exists());
}

#[test]
fn registry_compaction_replays_watermark_and_discards_only_partial_tail() {
    let scratch = Scratch::new("registry_compaction");
    let fake = serve_drive();
    let settings = settings(&fake);
    let mut drive = Drive::open(&settings).unwrap();
    let state = scratch.path("pipeline_state");
    let registry = Registry::open(&state, &settings, &mut drive).unwrap();
    let path = scratch.path("content");
    for n in 0u32..90 {
        fs::write(&path, n.to_le_bytes()).unwrap();
        transfer(
            &registry,
            &mut drive,
            &path,
            &store::identify(&path).unwrap(),
        )
        .unwrap();
    }
    let directory = state.join("registry");
    let snapshot = read_json(&directory.join("snapshot.json"));
    assert_eq!(snapshot["sequence"], 256);
    let expected = registry_state(&directory);
    let log = directory.join("events.ndjson");
    let tail = fs::read(&log).unwrap();
    // A crash after snapshot rename but before log truncation leaves older records behind.
    let stale = json!({"sequence":1,"change":{"kind":"rebuilt"}});
    let mut file = File::create(&log).unwrap();
    writeln!(file, "{stale}").unwrap();
    file.write_all(&tail).unwrap();
    file.write_all(b"{\"sequence\":").unwrap();
    drop(file);
    drop(registry);
    let registry = Registry::open(&state, &settings, &mut drive).unwrap();
    assert_eq!(registry_state(&directory)["files"], expected["files"]);
    transfer(
        &registry,
        &mut drive,
        &path,
        &store::identify(&path).unwrap(),
    )
    .unwrap();
    assert_eq!(uploads(&fake), 90);
    let mut other = settings.clone();
    other.root_folder_id = "another-root".into();
    assert!(
        Registry::open(&state, &other, &mut drive)
            .err()
            .unwrap()
            .contains("archive root")
    );
}

fn audit_local(scratch: &Scratch, generation: &str) -> String {
    let root = scratch.path("producer/store");
    let config = scratch.path("audit-local.toml");
    let core = fs::read_to_string(scratch.path("core.toml"))
        .unwrap()
        .replace(
            "historical_data_dir = \"unused\"",
            "historical_data_dir = \"producer/store\"",
        )
        .replace("file:///unused", &fixture::uri(&root));
    fs::write(&config, core).unwrap();
    let mut report = Vec::new();
    binary_alpha_app::audit::run(
        &config,
        &fixture::uri(&root.join(dataset::manifest_key(generation))),
        &mut report,
    )
    .unwrap();
    common::generation(&String::from_utf8(report).unwrap())
}

#[test]
fn registry_rebuild_repairs_completed_entries_without_rebinding_catalog_receipts() {
    let scratch = Scratch::new("registry_repair");
    let fake = serve_drive();
    let settings = settings(&fake);
    let mut drive = Drive::open(&settings).unwrap();
    let state = scratch.path("pipeline_state");
    let registry = Registry::open(&state, &settings, &mut drive).unwrap();
    let (path, identity) = bytes_fixture(&scratch, "content");
    let old = transfer(&registry, &mut drive, &path, &identity).unwrap();
    {
        let mut state = fake.state.lock().unwrap();
        let entry = state.files.remove(&old).unwrap();
        state.files.insert("confirmed-replacement".into(), entry);
    }
    registry.rebuild(&mut drive).unwrap();
    assert_eq!(
        transfer(&registry, &mut drive, &path, &identity).unwrap(),
        "confirmed-replacement"
    );
    fake.state.lock().unwrap().files.clear();
    registry.rebuild(&mut drive).unwrap();
    assert!(
        registry_state(&state.join("registry"))["files"]
            .as_object()
            .unwrap()
            .is_empty()
    );
    assert_ne!(
        transfer(&registry, &mut drive, &path, &identity).unwrap(),
        old
    );

    let (_scratch, fake, _, config) = archive_fixture("registry_trashed_receipt", false, false);
    pipeline("archive", &config, &[]).unwrap();
    let id = remote_catalogs(&fake)[0].0.clone();
    fake.state
        .lock()
        .unwrap()
        .files
        .get_mut(&id)
        .unwrap()
        .trashed = true;
    let error = pipeline("archive", &config, &[]).unwrap_err();
    assert!(error.contains("missing or trashed"), "{error}");
}

#[test]
fn registry_persistence_failure_stops_other_jobs_until_reopened() {
    let scratch = Scratch::new("registry_io_failure");
    let fake = serve_drive();
    let settings = settings(&fake);
    let mut drive = Drive::open(&settings).unwrap();
    let state = scratch.path("pipeline_state");
    let registry = Registry::open(&state, &settings, &mut drive).unwrap();
    let (path, identity) = bytes_fixture(&scratch, "content");
    let log = state.join("registry/events.ndjson");
    let saved = state.join("registry/saved-events");
    fs::rename(&log, &saved).unwrap();
    fs::create_dir(&log).unwrap();
    assert!(transfer(&registry, &mut drive, &path, &identity).is_err());
    fs::remove_dir(&log).unwrap();
    fs::rename(saved, log).unwrap();
    assert!(
        transfer(&registry, &mut drive, &path, &identity)
            .unwrap_err()
            .contains("reopen")
    );
    assert_eq!(uploads(&fake), 0);
    assert!(fake.state.lock().unwrap().sessions.is_empty());
    drop(registry);
    let registry = Registry::open(&state, &settings, &mut drive).unwrap();
    transfer(&registry, &mut drive, &path, &identity).unwrap();
    assert_eq!(uploads(&fake), 1);
}

#[test]
fn archive_refuses_equal_coverage_without_proved_lineage() {
    let (scratch, fake, pair, config) = archive_fixture("registry_ambiguous_lineage", false, false);
    let root = scratch.path("producer/store");
    let mut other = pair.v2.clone();
    let path = scratch.path("ambiguous-lineage.json");
    // A missing intermediate parent does not establish precedence over the other ready root.
    fs::write(&path, json!({"parent": "a".repeat(64)}).to_string()).unwrap();
    other.objects.push(fixture::object(
        &root,
        "provenance/lineage.json",
        ObjectRole::Provenance,
        &path,
    ));
    fixture::publish(&root, &mut other);
    let error = pipeline("archive", &config, &[]).unwrap_err();
    assert!(error.contains("ambiguous daily lineage"), "{error}");
    assert_eq!(uploads(&fake), 0);
}

#[test]
fn legacy_catalog_sessions_keep_original_ids_despite_shared_registry_duplicates() {
    for completed in [false, true] {
        let (scratch, fake, _, config) = archive_fixture(
            if completed {
                "legacy_catalog_completed"
            } else {
                "legacy_catalog_session"
            },
            false,
            false,
        );
        pipeline("archive", &config, &[]).unwrap();
        let (_, mut catalog) = remote_catalogs(&fake).pop().unwrap();
        let mut legacy = json!({"files": {}});
        {
            let mut remote = fake.state.lock().unwrap();
            let bindings = catalog
                .objects
                .iter_mut()
                .map(|e| (&e.key, &mut e.file_id))
                .chain(
                    [&mut catalog.dataset, &mut catalog.stream]
                        .into_iter()
                        .map(|e| (&e.key, &mut e.file_id)),
                );
            for (key, id) in bindings {
                let old = remote.files.get(id).unwrap();
                let copy = RemoteEntry {
                    name: old.name.clone(),
                    bytes: old.bytes.clone(),
                    trashed: false,
                };
                *id = format!("duplicate-{id}");
                remote.files.insert(id.clone(), copy);
                legacy["files"][key] = json!({"file_id": id, "done": true});
            }
        }
        let settings = settings(&fake);
        let mut drive = Drive::open(&settings).unwrap();
        let id = drive.generate_ids(1).unwrap().remove(0);
        let path = scratch.path("legacy-catalog.json");
        let mut bytes = serde_json::to_vec_pretty(&catalog).unwrap();
        bytes.push(b'\n');
        fs::write(&path, bytes).unwrap();
        let identity = store::identify(&path).unwrap();
        let mut session = None;
        let uploaded = drive.upload(
            &id,
            "catalog-legacy-resume.json",
            &path,
            &identity,
            None,
            &mut |value| {
                session = value.map(str::to_string);
                if completed {
                    Ok(())
                } else {
                    Err("fixture stopped after session checkpoint".into())
                }
            },
        );
        assert_eq!(uploaded.is_ok(), completed);
        let key = format!(
            "catalog/{}/{}",
            catalog.dataset.generation, catalog.stream.generation
        );
        legacy["files"][key] = json!({"file_id": id, "done": false, "session": session});
        let state = scratch.path("producer/pipeline_state");
        fs::write(state.join("first/transfers.json"), legacy.to_string()).unwrap();
        for entry in fs::read_dir(state.join("records")).unwrap() {
            let path = entry.unwrap().path();
            if path
                .file_name()
                .unwrap()
                .to_string_lossy()
                .contains("-catalog-")
            {
                fs::remove_file(path).unwrap();
            }
        }
        let before = uploads(&fake);
        let report = pipeline("archive", &config, &[]).unwrap();
        assert!(
            report.contains(&format!("catalog {id} sha256 {}", identity.sha256)),
            "{report}"
        );
        assert_eq!(uploads(&fake), before + usize::from(!completed));
        let remote = fake.state.lock().unwrap();
        assert_eq!(
            Catalog::from_json(&remote.files[&id].bytes).unwrap(),
            catalog
        );
        assert_eq!(remote.sessions.values().filter(|s| s.id == id).count(), 1);
    }
}

// Permanent adversarial regressions; every fixture is local or loopback.
#[test]
fn archive_rejects_trashed_completed_legacy_object() {
    let (scratch, fake, pair, config) = archive_fixture("review_legacy_trashed", false, false);
    let object = &pair.v2.objects[0];
    let id = "trashed-legacy-object";
    fake.state.lock().unwrap().files.insert(
        id.into(),
        RemoteEntry {
            name: format!("object-{}", object.sha256),
            bytes: fs::read(scratch.path("producer/store").join(&object.key)).unwrap(),
            trashed: true,
        },
    );
    let state = scratch.path("producer/pipeline_state/first");
    fs::create_dir_all(&state).unwrap();
    fs::write(
        state.join("transfers.json"),
        json!({"files": {&object.key: {"file_id":id, "done":true}}}).to_string(),
    )
    .unwrap();
    let before = fs::read(state.join("transfers.json")).unwrap();
    let result = pipeline("archive", &config, &[]);
    let catalogs = remote_catalogs(&fake);
    assert_eq!(fs::read(state.join("transfers.json")).unwrap(), before);
    let error = result
        .expect_err("must reject a trashed completed legacy entry before publishing a catalog");
    assert!(error.contains("trashed"), "{error}");
    assert!(catalogs.is_empty());
    let registry = registry_state(&scratch.path("producer/pipeline_state/registry"));
    assert!(registry["files"][&object.key].is_null());
    assert_eq!(
        registry["legacy"][format!("first/{}", object.key)]["file_id"],
        id
    );
    // Reopening must keep rejecting the receipt, including after the trashed file is gone.
    fake.state.lock().unwrap().files.remove(id);
    let error = pipeline("archive", &config, &[]).unwrap_err();
    assert!(error.contains("missing or trashed"), "{error}");
    assert!(remote_catalogs(&fake).is_empty());
    assert_eq!(fs::read(state.join("transfers.json")).unwrap(), before);
}

#[test]
fn newest_daily_skips_unrelated_declared_holdout() {
    let (scratch, _fake, pair, _config) = archive_fixture("review_unrelated_holdout", false, false);
    let root = scratch.path("producer/store");
    let protected = "f".repeat(64);
    let protected_path = root.join(dataset::manifest_key(&protected));
    fs::create_dir_all(protected_path.parent().unwrap()).unwrap();
    fs::write(&protected_path, b"fixture must not be opened").unwrap();
    let declaration = binary_alpha_engine::research::Declaration::from_json(&serde_json::to_vec(&json!({
        "schema_version": 1, "operator": "fixture", "root": fixture::uri(&root), "namespace": "fixture",
        "populations": [
            {"id":"allowed", "role":"development", "instrument":pair.v2.instrument, "source":"synthetic", "coverage":pair.v2.coverage, "generations":[pair.v2.generation], "tokens":["allowed"]},
            {"id":"unrelated", "role":"holdout", "instrument":"deriv:R_100", "source":"synthetic", "coverage":pair.v2.coverage, "generations":[protected], "tokens":["protected"]}
        ]
    })).unwrap()).unwrap();
    let result = binary_alpha_app::lineage::newest_daily(
        &store::Store::filesystem(&root),
        &pair.v2.instrument,
        pair.v2.role,
        Access {
            declaration: Some(&declaration),
            certification: None,
        },
    );
    assert_eq!(result.unwrap(), pair.v2.generation);
    let denied = binary_alpha_app::lineage::newest_daily(
        &store::Store::filesystem(&root),
        "deriv:R_100",
        dataset::DatasetRole::Holdout,
        Access {
            declaration: Some(&declaration),
            certification: None,
        },
    )
    .unwrap_err();
    assert!(denied.contains("holdout data is protected"), "{denied}");
}

#[test]
fn rebuild_checksumless_uses_one_full_readback() {
    let scratch = Scratch::new("review_checksumless_rebuild");
    let fake = serve_drive();
    let settings = settings(&fake);
    let mut drive = Drive::open(&settings).unwrap();
    let state = scratch.path("pipeline_state");
    let registry = Registry::open(&state, &settings, &mut drive).unwrap();
    let (path, identity) = bytes_fixture(&scratch, "content");
    let id = transfer(&registry, &mut drive, &path, &identity).unwrap();
    fake.set(DriveFaults {
        omit_sha256: true,
        ..Default::default()
    });
    fake.state.lock().unwrap().media_requests.clear();
    registry.rebuild(&mut drive).unwrap();
    assert_eq!(
        fake.state.lock().unwrap().media_requests,
        vec![(id.clone(), None)],
        "one complete readback suffices to confirm size and SHA-256"
    );
    assert_eq!(
        registry_state(&state.join("registry"))["files"][dataset::object_key(&identity.sha256)],
        json!({"file_id":id,"session":null,"done":true,"bytes":identity.bytes,"sha256":identity.sha256,"aliases":[]})
    );
}

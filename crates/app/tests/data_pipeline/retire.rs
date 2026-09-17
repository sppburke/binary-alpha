//! Retirement owns its deletion-capable loopback fake; the other pipeline fixtures are unchanged.
use super::*;
use binary_alpha_app::retire::{Plan, Status};
use binary_alpha_engine::dataset::{ObjectRole, SourceKind};
use common::daily;
use std::collections::BTreeSet;

struct DeleteFault {
    after: Option<usize>,
    deleted: usize,
    lose_reply: bool,
    incomplete: bool,
    no_checksum: bool,
    hidden: BTreeSet<String>,
}
struct RetireDrive {
    drive: FakeDrive,
    faults: Arc<Mutex<DeleteFault>>,
}
fn serve() -> RetireDrive {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let state = Arc::new(Mutex::new(DriveState::default()));
    let faults = Arc::new(Mutex::new(DeleteFault {
        after: None,
        deleted: 0,
        lose_reply: false,
        incomplete: false,
        no_checksum: false,
        hidden: BTreeSet::new(),
    }));
    let stop = Arc::new(AtomicBool::new(false));
    let (shared, fault, stopped) = (Arc::clone(&state), Arc::clone(&faults), Arc::clone(&stop));
    let thread = std::thread::spawn(move || {
        while !stopped.load(Ordering::SeqCst) {
            let Ok((mut stream, _)) = listener.accept() else {
                std::thread::sleep(Duration::from_millis(2));
                continue;
            };
            let Some(request) = read_request(&mut stream) else {
                continue;
            };
            let mut state = shared.lock().unwrap();
            let mut faults = fault.lock().unwrap();
            state
                .log
                .push(format!("{} {}", request.method, request.path));
            if request.path == "/token" {
                respond(
                    &mut stream,
                    200,
                    &[],
                    br#"{"access_token":"fixture-token","expires_in":3600}"#,
                );
                continue;
            }
            if request.path == "/drive/v3/files" {
                let offset = request
                    .query
                    .get("pageToken")
                    .map_or(0, |v| v.parse::<usize>().unwrap());
                let all: Vec<_> = state
                    .files
                    .iter()
                    .filter(|(id, e)| !e.trashed && !faults.hidden.contains(*id))
                    .collect();
                let entries: Vec<Value> = all
                    .iter()
                    .skip(offset)
                    .take(3)
                    .map(|(id, e)| {
                        serde_json::from_slice(&file_json(id, e, faults.no_checksum)).unwrap()
                    })
                    .collect();
                let mut body = json!({"files":entries,"incompleteSearch":faults.incomplete});
                if offset + 3 < all.len() {
                    body["nextPageToken"] = json!((offset + 3).to_string());
                }
                respond(&mut stream, 200, &[], body.to_string().as_bytes());
                continue;
            }
            let id = request.path.trim_start_matches("/drive/v3/files/");
            if request.method == "DELETE" {
                if faults.after.is_some_and(|n| faults.deleted >= n) {
                    respond(&mut stream, 403, &[], b"{}");
                    continue;
                }
                let found = state.files.remove(id).is_some();
                if found {
                    faults.deleted += 1;
                }
                if faults.lose_reply {
                    faults.lose_reply = false;
                    continue;
                }
                respond(&mut stream, if found { 204 } else { 404 }, &[], b"");
                continue;
            }
            let Some(entry) = state.files.get(id) else {
                respond(&mut stream, 404, &[], b"{}");
                continue;
            };
            if request.query.get("alt").is_some_and(|v| v == "media") {
                let from = request.headers.get("range").map_or(0, |s| {
                    s.trim_start_matches("bytes=")
                        .trim_end_matches('-')
                        .parse::<usize>()
                        .unwrap()
                });
                respond(
                    &mut stream,
                    if from == 0 { 200 } else { 206 },
                    &[],
                    &entry.bytes[from..],
                );
            } else {
                respond(
                    &mut stream,
                    200,
                    &[],
                    &file_json(id, entry, faults.no_checksum),
                );
            }
        }
    });
    RetireDrive {
        drive: FakeDrive {
            base,
            state,
            activity: Arc::new(DriveActivity::default()),
            stop,
            thread: Some(thread),
        },
        faults,
    }
}
fn remote(drive: &RetireDrive, key: &str, name: &str, bytes: Vec<u8>) -> String {
    let id = format!("remote-{}", sha256(key.as_bytes()));
    drive.drive.state.lock().unwrap().files.insert(
        id.clone(),
        RemoteEntry {
            name: name.into(),
            bytes,
            trashed: false,
        },
    );
    id
}
fn sha256(bytes: &[u8]) -> String {
    binary_alpha_engine::hex(&Sha256::digest(bytes))
}
fn entry(drive: &RetireDrive, store: &Path, key: &str, generation: Option<&str>) -> Value {
    let bytes = fs::read(store.join(key)).unwrap();
    let sha = sha256(&bytes);
    let name = if let Some(g) = generation {
        format!("manifest-{g}.json")
    } else {
        format!("object-{sha}")
    };
    let id = remote(drive, key, &name, bytes.clone());
    let mut e = json!({"key":key,"file_id":id,"bytes":bytes.len(),"sha256":sha});
    if let Some(g) = generation {
        e["generation"] = json!(g);
    }
    e
}
fn archive_fixture(
    drive: &RetireDrive,
    root: &Path,
    dataset: &GenerationManifest,
    stream: &StreamManifest,
) -> (String, String) {
    let mut objects = BTreeMap::new();
    for o in dataset.objects.iter().chain(&stream.objects) {
        objects
            .entry(o.key.clone())
            .or_insert_with(|| entry(drive, root, &o.key, None));
    }
    let catalog = json!({"schema_version":1,"layout":dataset.layout,"job":"deriv","broker":dataset.broker,"provider_symbol":dataset.provider_symbol,"instrument":dataset.instrument,"role":dataset.role,"source_kind":dataset.source_kind,"native_granularity":dataset.native_granularity,"coverage":dataset.coverage,"row_count":dataset.row_count,"dataset":entry(drive,root,&dataset.key(),Some(&dataset.generation)),"stream":entry(drive,root,&stream.key(),Some(&stream.generation)),"objects":objects.into_values().collect::<Vec<_>>()});
    let bytes = serde_json::to_vec_pretty(&catalog).unwrap();
    let sha = sha256(&bytes);
    let id = remote(
        drive,
        &format!("catalog/{}/{}", dataset.generation, stream.generation),
        &format!(
            "catalog-{}-{}.json",
            &dataset.generation[..16],
            &stream.generation[..16]
        ),
        bytes,
    );
    (id, sha)
}
fn audit_fixture(
    scratch: &Scratch,
    pair: &daily::Pair,
    manifest: &GenerationManifest,
) -> StreamManifest {
    let config = scratch.config("audit.toml", &pair.instrument());
    let report = common::command(&[
        "data",
        "audit",
        "--config",
        config.to_str().unwrap(),
        "--manifest",
        &daily::uri(&scratch.path("published").join(manifest.key())),
    ])
    .unwrap();
    fs::remove_file(config).unwrap();
    let path = scratch
        .path("published")
        .join(binary_alpha_engine::dataset::manifest_key(
            &common::generation(&report[0]),
        ));
    StreamManifest::from_json(&fs::read(path).unwrap()).unwrap()
}
struct Fixture {
    scratch: Scratch,
    drive: RetireDrive,
    config: PathBuf,
    root: PathBuf,
    old: Vec<String>,
    old_catalog: String,
    new_catalog: String,
    new_sha: String,
    new_dataset: GenerationManifest,
    new_stream: StreamManifest,
    shared: String,
    pending: String,
    original_records: BTreeMap<PathBuf, Vec<u8>>,
}
fn fixture() -> Fixture {
    static NEXT: AtomicUsize = AtomicUsize::new(0);
    let scratch = Scratch::new(&format!(
        "pipeline_retirement_{}",
        NEXT.fetch_add(1, Ordering::SeqCst)
    ));
    let drive = serve();
    let mut pair = daily::pair(&scratch, false);
    let published = scratch.path("published");
    let obsolete_coverage = pair
        .v1
        .objects
        .iter()
        .find(|o| o.path == "provenance/coverage.json")
        .unwrap()
        .key
        .clone();
    fs::remove_file(published.join(obsolete_coverage)).unwrap();
    let old_import = published.join(pair.v1.key());
    fs::remove_dir_all(old_import.parent().unwrap()).unwrap();
    let shared = pair
        .v2
        .objects
        .iter()
        .find(|o| o.path == "provenance/coverage.json")
        .unwrap()
        .clone();
    *pair
        .v1
        .objects
        .iter_mut()
        .find(|o| o.path == "provenance/coverage.json")
        .unwrap() = shared;
    daily::publish(&published, &mut pair.v1);

    let mut history = pair.v1.clone();
    history.source_kind = SourceKind::BrokerHistory;
    let coverage = scratch.path("history-coverage.json");
    fs::write(
        &coverage,
        json!({"schema_version":1,"source_identity":"fixture","broker":history.broker,
            "provider_symbol":history.provider_symbol,"role":history.role,
            "requested":{"start":history.coverage.first_event_time,"end":history.coverage.last_event_time},
            "verified":null,"actual":null,"rows":history.row_count,"pages":[],"shortfall":null}).to_string(),
    ).unwrap();
    history
        .objects
        .retain(|o| o.path != "provenance/coverage.json");
    history.objects.push(daily::object(
        &published,
        "provenance/coverage.json",
        ObjectRole::Provenance,
        &coverage,
    ));
    let page = scratch.path("legacy-page.json");
    fs::write(&page, b"{\"synthetic\":\"legacy response\"}").unwrap();
    history.objects.push(daily::object(
        &published,
        "raw/pages/0.json",
        ObjectRole::Source,
        &page,
    ));
    daily::publish(&published, &mut history);
    let old_stream = audit_fixture(&scratch, &pair, &history);
    let old_catalog = archive_fixture(&drive, &published, &history, &old_stream).0;
    let old_v2 = published.join(pair.v2.key());
    fs::remove_dir_all(old_v2.parent().unwrap()).unwrap();
    let lineage = scratch.path("lineage.json");
    fs::write(&lineage,json!({"v1_generations":[pair.v1.generation,history.generation],"v1_stream":old_stream.generation,"equality":"verified"}).to_string()).unwrap();
    pair.v2.objects.push(daily::object(
        &published,
        "provenance/lineage.json",
        ObjectRole::Provenance,
        &lineage,
    ));
    daily::publish(&published, &mut pair.v2);
    let new_stream = audit_fixture(&scratch, &pair, &pair.v2);
    let (new_catalog, new_sha) = archive_fixture(&drive, &published, &pair.v2, &new_stream);
    let root = scratch.path("managed");
    fs::create_dir_all(&root).unwrap();
    fs::rename(&published, root.join("store")).unwrap();
    let state = root.join("pipeline_state");
    fs::create_dir_all(state.join("records")).unwrap();
    fs::create_dir_all(state.join("deriv")).unwrap();
    // Pending diagnostic response intentionally also belongs to the retired history closure.
    let pending = history.objects.last().unwrap().key.clone();
    let digest = pending.strip_prefix("objects/").unwrap();
    fs::write(state.join("deriv/progress.json"),json!({"intent":"pending-intent.json","progress":{"baseline":pair.v2.generation,"pages":[]}}).to_string()).unwrap();
    fs::write(state.join("deriv/progress.pages.jsonl"),format!("{}\n",json!({"path":"raw/pages/0.json","sha256":digest,"bytes":fs::metadata(root.join("store").join(&pending)).unwrap().len(),"rows":0,"receipt_time":null}))).unwrap();
    let values = [
        (
            "old-intent.json",
            json!({"job":"deriv","seeds":[{"manifest":daily::uri(&root.join("store").join(pair.v1.key()))}]}),
        ),
        (
            "old-receipt.json",
            json!({"job":"deriv","intent":"old-intent.json","dataset_generation":history.generation,"stream_generation":old_stream.generation,"catalog":{"file_id":old_catalog}}),
        ),
        ("old-catalog.json", json!({"file_id":old_catalog})),
        (
            "migration.json",
            json!({"job":"deriv","phase":"verified","old_generation":history.generation,"new_generation":pair.v2.generation}),
        ),
        (
            "pending-intent.json",
            json!({"job":"deriv","seeds":[{"manifest":daily::uri(&root.join("store").join(pair.v2.key()))}]}),
        ),
    ];
    let mut original_records = BTreeMap::new();
    for (name, value) in values {
        let path = state.join("records").join(name);
        let bytes = value.to_string().into_bytes();
        fs::write(&path, &bytes).unwrap();
        original_records.insert(path, bytes);
    }
    fs::write(state.join("deriv/update.toml"), "schema_version = 1\n").unwrap();
    let config = scratch.path("pipeline.toml");
    fs::write(
        &config,
        pipeline_toml(&root, &drive.drive.base, &[("deriv", "job.toml")], None, 1),
    )
    .unwrap();
    let core = deriv_core("ws://127.0.0.1:9/", 60, 50, 60);
    fs::write(scratch.path("job.toml"), core.replace("frxEURUSD", "R_50")).unwrap();
    let shared = pair
        .v1
        .objects
        .iter()
        .find(|o| o.path == "provenance/coverage.json")
        .unwrap()
        .key
        .clone();
    Fixture {
        scratch,
        drive,
        config,
        root,
        old: vec![
            pair.v1.generation,
            history.generation,
            old_stream.generation,
        ],
        old_catalog,
        new_catalog,
        new_sha,
        new_dataset: pair.v2,
        new_stream,
        shared,
        pending,
        original_records,
    }
}
fn plan(f: &Fixture) -> (PathBuf, Plan) {
    let report = pipeline("retire", &f.config, &["--job", "deriv", "--plan"]).unwrap();
    let path = PathBuf::from(report.split_whitespace().nth(2).unwrap());
    let p = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
    (path, p)
}
fn apply(f: &Fixture, path: &Path) -> Result<String, String> {
    pipeline("retire", &f.config, &["--apply", path.to_str().unwrap()])
}

#[test]
fn retirement_reachability_resume_and_fresh_restore() {
    let f = fixture();
    let local = f.root.join("store");
    let before_local: BTreeSet<_> = fs::read_dir(local.join("objects"))
        .unwrap()
        .map(|e| format!("objects/{}", e.unwrap().file_name().to_string_lossy()))
        .collect();
    let before_remote = f.drive.drive.files();
    let (path, plan) = plan(&f);
    assert_eq!(plan.jobs, ["deriv"]);
    assert_eq!(plan.totals.manifest_directories, 3);
    assert!(
        !plan
            .delete_local
            .iter()
            .any(|e| e.path == f.shared || e.path == f.pending)
    );
    let kept: BTreeSet<_> = f
        .new_dataset
        .objects
        .iter()
        .chain(&f.new_stream.objects)
        .map(|o| o.key.clone())
        .chain([f.pending.clone()])
        .collect();
    let expected: BTreeSet<_> = before_local.difference(&kept).cloned().collect();
    let planned: BTreeSet<_> = plan
        .delete_local
        .iter()
        .filter(|e| e.path.starts_with("objects/"))
        .map(|e| e.path.clone())
        .collect();
    assert_eq!(planned, expected);
    let old_catalog: Value = serde_json::from_slice(&before_remote[&f.old_catalog].bytes).unwrap();
    let expected_remote: BTreeSet<_> = std::iter::once(f.old_catalog.clone())
        .chain(
            [&old_catalog["dataset"], &old_catalog["stream"]]
                .into_iter()
                .chain(old_catalog["objects"].as_array().unwrap())
                .filter(|e| !kept.contains(e["key"].as_str().unwrap()))
                .map(|e| e["file_id"].as_str().unwrap().to_string()),
        )
        .collect();
    assert_eq!(
        plan.delete_drive
            .iter()
            .map(|e| e.file_id.clone())
            .collect::<BTreeSet<_>>(),
        expected_remote
    );
    assert!(plan.delete_drive.iter().any(|e| e.file_id == f.old_catalog));
    assert!(
        !plan
            .delete_drive
            .iter()
            .any(|e| e.file_id == f.new_catalog || e.key == f.shared || e.key == f.pending)
    );
    assert!(
        plan.references
            .iter()
            .any(|r| r.source.ends_with("old-receipt.json")
                && r.closure == "old-receipt.json"
                && r.status == Status::Retired)
    );
    // Interrupt after one remote deletion; no local file may have been removed yet.
    f.drive.faults.lock().unwrap().after = Some(1);
    assert!(apply(&f, &path).unwrap_err().contains("403"));
    for id in &f.old {
        assert!(
            local
                .join(binary_alpha_engine::dataset::manifest_key(id))
                .exists()
        );
    }
    f.drive.faults.lock().unwrap().after = None;
    apply(&f, &path).unwrap();
    apply(&f, &path).unwrap();
    let after = f.drive.drive.files();
    let deleted: BTreeSet<_> = before_remote
        .keys()
        .filter(|id| !after.contains_key(*id))
        .cloned()
        .collect();
    assert_eq!(
        deleted,
        plan.delete_drive
            .iter()
            .map(|e| e.file_id.clone())
            .collect()
    );
    for e in &plan.delete_local {
        assert!(!local.join(&e.path).exists());
    }
    for key in kept {
        assert!(local.join(key).exists());
    }
    for (p, b) in &f.original_records {
        assert_eq!(fs::read(p).unwrap(), *b);
    }
    common::verify(&local.join(f.new_dataset.key())).unwrap();
    common::verify(&local.join(f.new_stream.key())).unwrap();
    let consumer = f.scratch.path("consumer.toml");
    fs::write(
        &consumer,
        pipeline_toml(
            &f.scratch.path("restored"),
            &f.drive.drive.base,
            &[],
            None,
            1,
        ),
    )
    .unwrap();
    pipeline(
        "restore",
        &consumer,
        &[
            "--catalog",
            &f.new_catalog,
            "--sha256",
            &f.new_sha,
            "--broker",
            "deriv",
            "--symbol",
            "R_50",
        ],
    )
    .unwrap();
    common::verify(&f.scratch.path("restored/store").join(f.new_dataset.key())).unwrap();
    common::verify(&f.scratch.path("restored/store").join(f.new_stream.key())).unwrap();
}

#[test]
fn retirement_refuses_stale_state_and_preserves_configuration_and_transfer_pins() {
    let f = fixture();
    let (path, _) = plan(&f);
    let registry = f.root.join("pipeline_state/registry.json");
    fs::write(&registry, "{\"files\":{}}").unwrap();
    assert!(apply(&f, &path).unwrap_err().contains("stale plan"));
    assert!(
        !f.drive
            .drive
            .log()
            .iter()
            .any(|line| line.starts_with("DELETE"))
    );
    let pinned = f.scratch.path("nested/pinned.toml");
    fs::create_dir_all(pinned.parent().unwrap()).unwrap();
    fs::write(
        &pinned,
        format!(
            "manifest=\"{}\"\n",
            daily::uri(
                &f.root
                    .join("store")
                    .join(binary_alpha_engine::dataset::manifest_key(&f.old[1]))
            )
        ),
    )
    .unwrap();
    let (_, protected) = plan(&f);
    assert!(
        !protected
            .delete_local
            .iter()
            .any(|e| e.path == format!("manifests/{}", f.old[1]))
    );
    fs::remove_file(pinned).unwrap();
    let key = f
        .root
        .join("store")
        .join(binary_alpha_engine::dataset::manifest_key(&f.old[0]));
    fs::write(&registry,json!({"files":{binary_alpha_engine::dataset::manifest_key(&f.old[0]):{"file_id":"in-flight","done":false,"session":"fixture"}}}).to_string()).unwrap();
    let (_, protected) = plan(&f);
    assert!(
        !protected
            .delete_local
            .iter()
            .any(|e| e.path == format!("manifests/{}", f.old[0]))
    );
    assert!(key.exists());
}

#[test]
fn retirement_preserves_unmapped_receipt_pages_and_unselected_jobs() {
    let f = fixture();
    let bytes = b"unmapped diagnostic response";
    let key = format!("objects/{}", sha256(bytes));
    fs::write(f.root.join("store").join(&key), bytes).unwrap();
    let records = f.root.join("pipeline_state/records");
    fs::write(
        records.join("diagnostic.json"),
        json!({"job":"deriv","requests":[{"sha256":sha256(bytes),"rows":0}]}).to_string(),
    )
    .unwrap();
    let old = dataset(&f.root.join("store"), &f.old[0]);
    let shared = old
        .objects
        .iter()
        .find(|o| o.role == ObjectRole::Normalized)
        .unwrap();
    fs::write(
        records.join("other-job.json"),
        json!({"job":"other","requests":[{"sha256":shared.sha256,"rows":1}]}).to_string(),
    )
    .unwrap();
    let (path, planned) = plan(&f);
    for k in [&key, &shared.key] {
        assert!(planned.retained_objects.contains_key(k));
        assert!(!planned.delete_local.iter().any(|e| &e.path == k));
    }
    apply(&f, &path).unwrap();
    assert_eq!(fs::read(f.root.join("store").join(key)).unwrap(), bytes);
}

#[test]
fn retirement_pins_unpublished_catalog_transfer_and_accepts_registry_log() {
    let f = fixture();
    f.drive
        .drive
        .state
        .lock()
        .unwrap()
        .files
        .remove(&f.old_catalog);
    // The catalog is still uploading; completed constituent transfers alone do not pin it.
    let key = format!("catalog/{}/{}", f.old[1], f.old[2]);
    fs::write(
        f.root.join("pipeline_state/deriv/transfers.json"),
        json!({"files":{key:{"file_id":"reserved","done":false,"session":"fixture"}}}).to_string(),
    )
    .unwrap();
    let object = dataset(&f.root.join("store"), &f.old[0]).objects[0]
        .key
        .clone();
    fs::write(
        f.root.join("pipeline_state/registry.jsonl"),
        format!(
            "{}\n",
            json!({"key":object,"file_id":"reserved-object","done":false,"session":null})
        ),
    )
    .unwrap();
    let (_, p) = plan(&f);
    for id in [&f.old[1], &f.old[2]] {
        assert!(
            !p.delete_local
                .iter()
                .any(|e| e.path == format!("manifests/{id}"))
        );
    }
    assert!(p.retained_objects.contains_key(&object));
}

#[test]
fn retirement_refuses_incomplete_or_outside_root_catalog_bindings() {
    let f = fixture();
    f.drive.faults.lock().unwrap().incomplete = true;
    assert!(
        pipeline("retire", &f.config, &[])
            .unwrap_err()
            .contains("incomplete search")
    );
    f.drive.faults.lock().unwrap().incomplete = false;
    let catalog: Value =
        serde_json::from_slice(&f.drive.drive.files()[&f.old_catalog].bytes).unwrap();
    let outside = catalog["dataset"]["file_id"].as_str().unwrap();
    f.drive
        .faults
        .lock()
        .unwrap()
        .hidden
        .insert(outside.to_string());
    assert!(
        pipeline("retire", &f.config, &[])
            .unwrap_err()
            .contains("outside root listing")
    );
    assert!(!f.drive.drive.log().iter().any(|s| s.starts_with("DELETE")));
}

#[test]
fn retirement_checks_manifest_and_remote_state_before_deletion() {
    let f = fixture();
    let (path, _) = plan(&f);
    let manifest = f.root.join("store").join(f.new_dataset.key());
    let original = fs::read(&manifest).unwrap();
    fs::write(&manifest, b"changed manifest").unwrap();
    assert!(apply(&f, &path).unwrap_err().contains("stale plan"));
    fs::write(&manifest, &original).unwrap();
    f.drive
        .drive
        .state
        .lock()
        .unwrap()
        .files
        .get_mut(&f.old_catalog)
        .unwrap()
        .name = "renamed.json".into();
    assert!(apply(&f, &path).unwrap_err().contains("stale plan"));
    assert!(!f.drive.drive.log().iter().any(|s| s.starts_with("DELETE")));
}

#[test]
fn retirement_resumes_lost_delete_reply_and_checksum_free_catalogs() {
    let f = fixture();
    f.drive.faults.lock().unwrap().no_checksum = true;
    let (path, p) = plan(&f);
    {
        let mut faults = f.drive.faults.lock().unwrap();
        faults.lose_reply = true;
        faults.after = Some(1);
    }
    assert!(apply(&f, &path).is_err());
    assert!(
        !f.drive
            .drive
            .files()
            .contains_key(&p.delete_drive[0].file_id)
    );
    f.drive.faults.lock().unwrap().after = None;
    apply(&f, &path).unwrap();
    for e in p.delete_drive {
        assert!(!f.drive.drive.files().contains_key(&e.file_id));
    }
}

#[test]
fn retirement_resumes_torn_local_progress_and_can_plan_again() {
    struct StopAtBatch;
    impl Write for StopAtBatch {
        fn write(&mut self, _: &[u8]) -> std::io::Result<usize> {
            Err(std::io::Error::other("fixture interruption"))
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let f = fixture();
    let (path, p) = plan(&f);
    assert!(
        binary_alpha_app::retire::run(&f.config, None, Some(&path), &mut StopAtBatch)
            .unwrap_err()
            .contains("fixture interruption")
    );
    let log = path.with_extension("progress.jsonseq");
    let prefix = fs::read(&log).unwrap();
    let digest = sha256(&fs::read(&path).unwrap());
    let mut file = fs::OpenOptions::new().append(true).open(&log).unwrap();
    writeln!(
        file,
        "\u{1e}{}",
        json!({"plan":digest,"index":p.delete_drive.len(),"phase":"begin"})
    )
    .unwrap();
    // Simulate a killed process after unlinking one local manifest, before its done frame.
    let first = &p.delete_local[0];
    fs::remove_file(p.store.join(first.files.keys().next().unwrap())).unwrap();
    file.write_all(b"\x1e{\"plan\":").unwrap();
    file.sync_all().unwrap();
    drop(file);
    apply(&f, &path).unwrap();
    assert!(fs::read(&log).unwrap().starts_with(&prefix));
    let (_, next) = plan(&f);
    assert!(next.delete_local.is_empty());
    assert!(next.delete_drive.is_empty());
    assert!(
        next.references
            .iter()
            .any(|r| r.closure == "old-receipt.json" && r.status == Status::Retired)
    );
}

#[test]
fn retirement_preserves_unresolved_record_closures() {
    let f = fixture();
    fs::write(
        f.root.join("pipeline_state/records/unresolved.json"),
        json!({"job":"deriv","generation":"f".repeat(64),"other_generation":f.old[0]}).to_string(),
    )
    .unwrap();
    let (_, p) = plan(&f);
    assert!(
        !p.delete_local
            .iter()
            .any(|e| e.path == format!("manifests/{}", f.old[0]))
    );
    assert!(
        p.references
            .iter()
            .any(|r| r.source.ends_with("unresolved.json")
                && r.closure == "f".repeat(64)
                && r.status == Status::Protected)
    );
}

#[test]
fn retirement_replays_registry_completion_instead_of_pinning_old_log_frames() {
    let f = fixture();
    let key = binary_alpha_engine::dataset::manifest_key(&f.old[0]);
    fs::write(
        f.root.join("pipeline_state/registry.snapshot.json"),
        json!({"files":{key.clone():{"file_id":"reserved","done":false},"records/migration.json":{"file_id":"migration-record","done":true}}}).to_string(),
    )
    .unwrap();
    let log = [false, true]
        .into_iter()
        .map(|done| json!({"key":key,"file_id":"reserved","done":done}).to_string())
        .collect::<Vec<_>>()
        .join("\n");
    fs::write(
        f.root.join("pipeline_state/registry.log.jsonl"),
        format!("{log}\n"),
    )
    .unwrap();
    let (_, p) = plan(&f);
    assert!(
        p.delete_local
            .iter()
            .any(|e| e.path == format!("manifests/{}", f.old[0]))
    );
}

#[test]
fn retirement_rejects_missing_lineage_parent_and_retained_corruption() {
    let f = fixture();
    let object = f
        .new_dataset
        .objects
        .iter()
        .find(|o| o.path.starts_with("observations/"))
        .unwrap();
    let path = f.root.join("store").join(&object.key);
    let original = fs::read(&path).unwrap();
    fs::write(&path, b"broken retained partition").unwrap();
    assert!(pipeline("retire", &f.config, &[]).is_err());
    fs::write(&path, original).unwrap();
    let root = f.root.join("store");
    let mut changed = f.new_dataset.clone();
    let lineage = f.scratch.path("missing-parent.json");
    fs::write(
        &lineage,
        json!({"parent_generation":"e".repeat(64),"v1_generations":f.old[..2]}).to_string(),
    )
    .unwrap();
    changed
        .objects
        .retain(|o| o.path != "provenance/lineage.json");
    changed.objects.push(daily::object(
        &root,
        "provenance/lineage.json",
        ObjectRole::Provenance,
        &lineage,
    ));
    daily::publish(&root, &mut changed);
    assert!(
        pipeline("retire", &f.config, &[])
            .unwrap_err()
            .contains("unresolved lineage parent")
    );
}

#[test]
fn retirement_excludes_an_active_restore_in_another_store() {
    let f = fixture();
    let _ = plan(&f);
    let binding = format!("{}\nfixture-root", f.drive.drive.base);
    let path = std::env::temp_dir().join(format!(
        "binary-alpha-archive-{}.lock",
        sha256(binding.as_bytes())
    ));
    let lock = File::open(path).unwrap();
    lock.try_lock().unwrap();
    let error = pipeline("retire", &f.config, &[]).unwrap_err();
    assert!(error.contains("another operation holds this archive root"));
    let restore = f.scratch.path("restore.toml");
    fs::write(
        &restore,
        pipeline_toml(&f.scratch.path("fresh"), &f.drive.drive.base, &[], None, 1),
    )
    .unwrap();
    let error = pipeline(
        "restore",
        &restore,
        &[
            "--catalog",
            &f.new_catalog,
            "--sha256",
            &f.new_sha,
            "--broker",
            "deriv",
            "--symbol",
            "R_50",
        ],
    )
    .unwrap_err();
    assert!(error.contains("another operation holds this archive root"));
}

#[test]
fn retirement_requires_complete_catalog_bindings_and_keeps_extra_manifests() {
    let f = fixture();
    let original = f.drive.drive.files()[&f.new_catalog].bytes.clone();
    let mut catalog: Value = serde_json::from_slice(&original).unwrap();
    catalog["dataset"]
        .as_object_mut()
        .unwrap()
        .remove("file_id");
    f.drive
        .drive
        .state
        .lock()
        .unwrap()
        .files
        .get_mut(&f.new_catalog)
        .unwrap()
        .bytes = serde_json::to_vec(&catalog).unwrap();
    assert!(
        pipeline("retire", &f.config, &[])
            .unwrap_err()
            .contains("catalog lacks manifest binding")
    );
    let mut catalog: Value = serde_json::from_slice(&original).unwrap();
    let root = f.root.join("store");
    let old = dataset(&root, &f.old[0]);
    // The archive owns the catalog envelope. Additional manifest entries pin their full
    // closure even when they are not the top-level dataset/stream pair.
    catalog["lineage_manifests"] =
        json!([entry(&f.drive, &root, &old.key(), Some(&old.generation))]);
    for object in &old.objects {
        if !catalog["objects"]
            .as_array()
            .unwrap()
            .iter()
            .any(|e| e["key"] == object.key)
        {
            catalog["objects"].as_array_mut().unwrap().push(entry(
                &f.drive,
                &root,
                &object.key,
                None,
            ));
        }
    }
    f.drive
        .drive
        .state
        .lock()
        .unwrap()
        .files
        .get_mut(&f.new_catalog)
        .unwrap()
        .bytes = serde_json::to_vec(&catalog).unwrap();
    let (_, p) = plan(&f);
    assert!(
        !p.delete_local
            .iter()
            .any(|e| e.path == format!("manifests/{}", old.generation))
    );
    assert!(
        old.objects
            .iter()
            .all(|e| p.retained_objects.contains_key(&e.key))
    );
}

#[test]
fn retirement_replays_actual_registry_aliases_watermark_and_removals() {
    let f = fixture();
    let config: data_pipeline::PipelineConfig =
        toml::from_str(&fs::read_to_string(&f.config).unwrap()).unwrap();
    let state = f.root.join("pipeline_state");
    let mut drive = binary_alpha_app::drive::Drive::open(&config.drive).unwrap();
    drop(binary_alpha_app::registry::Registry::open(&state, &config.drive, &mut drive).unwrap());
    let directory = state.join("registry");
    let mut snapshot = super::registry_archive::registry_state(&directory);
    // Compact the actual schema, leaving older journal records to be skipped at its watermark.
    fs::write(directory.join("snapshot.json"), snapshot.to_string()).unwrap();
    let mut seq = snapshot["sequence"].as_u64().unwrap();
    let mut append = |change: Value| {
        seq += 1;
        let mut log = std::fs::OpenOptions::new()
            .append(true)
            .open(directory.join("events.ndjson"))
            .unwrap();
        writeln!(log, "{}", json!({"sequence":seq,"change":change})).unwrap();
    };
    let key = format!("objects/{}", "a".repeat(64));
    let legacy_key = binary_alpha_engine::dataset::manifest_key(&f.old[0]);
    let entry = json!({"file_id":"reserved-catalog","session":null,"done":false,"bytes":12,"sha256":"a".repeat(64),"aliases":[format!("deriv/catalog/{}/{}",f.old[1],f.old[2])]});
    append(json!({"kind":"put","key":key,"entry":entry}));
    append(
        json!({"kind":"legacy","alias":format!("deriv/{legacy_key}"),"entry":{"file_id":"reserved-manifest","done":false,"session":null}}),
    );
    let (_, p) = plan(&f);
    assert!(
        !p.delete_local
            .iter()
            .any(|d| f.old.iter().any(|id| d.path == format!("manifests/{id}")))
    );
    // Completion supersedes the imported in-flight alias, and Remove supersedes reservation.
    append(
        json!({"kind":"put","key":format!("objects/{}","b".repeat(64)),"entry":{"file_id":"reserved-manifest","session":null,"done":true,"bytes":12,"sha256":"b".repeat(64),"aliases":[format!("deriv/{legacy_key}")]}}),
    );
    append(json!({"kind":"remove","key":key}));
    let (_, p) = plan(&f);
    assert_eq!(p.totals.manifest_directories, 3);
    // A replayed pre-watermark reservation must not resurrect the removed transfer.
    snapshot = super::registry_archive::registry_state(&directory);
    fs::write(directory.join("snapshot.json"), snapshot.to_string()).unwrap();
    let (_, p) = plan(&f);
    assert_eq!(p.totals.manifest_directories, 3);
}

//! Retirement owns its deletion-capable loopback fake; the other pipeline fixtures are unchanged.
use super::*;
use binary_alpha_app::retire::{Plan, Status};
use binary_alpha_engine::dataset::{ObjectRole, SourceKind};
use common::daily;
use std::collections::BTreeSet;

enum DeleteChange {
    Rename,
    Content,
    Trash,
    None,
}
struct DeleteFault {
    after: Option<usize>,
    deleted: usize,
    lose_reply: bool,
    retry_fault: Option<(u16, DeleteChange)>,
    rename_on_metadata: Option<(String, usize)>,
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
        retry_fault: None,
        rename_on_metadata: None,
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
                if let Some((status, change)) = faults.retry_fault.take() {
                    let file = state.files.get_mut(id).unwrap();
                    match change {
                        DeleteChange::Rename => file.name = "renamed-after-delete-attempt".into(),
                        DeleteChange::Content => file.bytes.push(b'!'),
                        DeleteChange::Trash => file.trashed = true,
                        DeleteChange::None => (),
                    }
                    if status != 0 {
                        respond(&mut stream, status, &[], b"{}");
                    }
                    continue;
                }
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
            if !request.query.contains_key("alt")
                && let Some((target, remaining)) = &mut faults.rename_on_metadata
                && target == id
            {
                *remaining -= 1;
                if *remaining == 0 {
                    state.files.get_mut(id).unwrap().name = "renamed-during-confirmation".into();
                    faults.rename_on_metadata = None;
                }
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
    let mut catalog = json!({"schema_version":1,"layout":dataset.layout,"job":"deriv","broker":dataset.broker,"provider_symbol":dataset.provider_symbol,"instrument":dataset.instrument,"role":dataset.role,"source_kind":dataset.source_kind,"native_granularity":dataset.native_granularity,"coverage":dataset.coverage,"row_count":dataset.row_count,"dataset":entry(drive,root,&dataset.key(),Some(&dataset.generation)),"stream":entry(drive,root,&stream.key(),Some(&stream.generation)),"objects":objects.into_values().collect::<Vec<_>>()});
    let record = root
        .parent()
        .unwrap()
        .join("pipeline_state/records/migration.json");
    if dataset.layout.is_some() && record.is_file() {
        let bytes = fs::read(record).unwrap();
        let sha = sha256(&bytes);
        let id = remote(
            drive,
            "records/migration.json",
            &format!("record-{sha}"),
            bytes.clone(),
        );
        catalog["records"] =
            json!([{"key":"records/migration.json","file_id":id,"sha256":sha,"bytes":bytes.len()}]);
        let migration: Value = serde_json::from_slice(&bytes).unwrap();
        let mut lineage_manifests = Vec::new();
        for generation in ["v2_root", "v2_stream"].map(|field| migration[field].as_str().unwrap()) {
            if generation == dataset.generation || generation == stream.generation {
                continue;
            }
            let key = binary_alpha_engine::dataset::manifest_key(generation);
            let manifest = read_json(&root.join(&key));
            for object in manifest["objects"].as_array().unwrap() {
                let key = object["key"].as_str().unwrap();
                let objects = catalog["objects"].as_array_mut().unwrap();
                if !objects.iter().any(|object| object["key"] == key) {
                    objects.push(entry(drive, root, key, None));
                }
            }
            lineage_manifests.push(entry(drive, root, &key, Some(generation)));
        }
        catalog["lineage_manifests"] = json!(lineage_manifests);
    }
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
    let (new_catalog, _) = archive_fixture(&drive, &published, &pair.v2, &new_stream);
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
            json!({"schema_version":1,"job":"deriv","phase":"verified",
                "v1_generations":[pair.v1.generation,history.generation],
                "v1_stream":old_stream.generation,"v2_root":pair.v2.generation,
                "v2_stream":new_stream.generation,
                "equality":{"observations":true,"pages":true,"source_files":true,"candles":true}}),
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
    let migration_bytes = fs::read(state.join("records/migration.json")).unwrap();
    let migration_sha = sha256(&migration_bytes);
    let migration_file = remote(
        &drive,
        "records/migration.json",
        &format!("record-{migration_sha}"),
        migration_bytes.clone(),
    );
    let new_sha = {
        let mut archive = drive.drive.state.lock().unwrap();
        let file = archive.files.get_mut(&new_catalog).unwrap();
        let mut catalog: Value = serde_json::from_slice(&file.bytes).unwrap();
        catalog["records"] = json!([{"key":"records/migration.json","file_id":migration_file,"sha256":migration_sha,"bytes":migration_bytes.len()}]);
        file.bytes = serde_json::to_vec_pretty(&catalog).unwrap();
        sha256(&file.bytes)
    };
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

fn amend_migration(f: &mut Fixture, field: &str, value: Value) {
    let path = f.root.join("pipeline_state/records/migration.json");
    let mut record = read_json(&path);
    record[field] = value;
    let bytes = serde_json::to_vec(&record).unwrap();
    fs::write(&path, &bytes).unwrap();
    f.original_records.insert(path, bytes);
    (f.new_catalog, f.new_sha) = archive_fixture(
        &f.drive,
        &f.root.join("store"),
        &f.new_dataset,
        &f.new_stream,
    );
}

#[test]
fn retirement_unfinished_plan_fences_every_writer_and_resumes() {
    let f = fixture();
    common::write_ticks(
        &f.scratch.path("sources/ticks/ticks.csv"),
        &["2026-03-22T06:02:39.312Z,AEDCNY,1.80787"],
    );
    let import = f
        .scratch
        .config("fenced-import.toml", &f.scratch.tick_source());
    let source = fs::read_to_string(&import).unwrap();
    fs::write(
        &import,
        source.replace(
            &format!("file://{}", f.scratch.path("published").display()),
            &format!("file://{}", f.root.join("store").display()),
        ),
    )
    .unwrap();
    let (path, mut alternative) = plan(&f);
    alternative.references.reverse();
    let bytes = serde_json::to_vec(&alternative).unwrap();
    let other = path.with_file_name(format!("plan-{}.json", sha256(&bytes)));
    fs::write(&other, bytes).unwrap();
    f.drive.faults.lock().unwrap().after = Some(1);
    assert!(apply(&f, &path).unwrap_err().contains("403"));
    let error = apply(&f, &other).unwrap_err();
    assert!(error.contains("unfinished retirement"), "{error}");
    assert!(
        error.contains(path.file_name().unwrap().to_str().unwrap()),
        "{error}"
    );
    let requests = f.drive.drive.log().len();
    for command in ["archive", "update", "migrate", "retire"] {
        let error = pipeline(command, &f.config, &[]).unwrap_err();
        assert!(
            error.contains("unfinished retirement"),
            "{command}: {error}"
        );
        assert!(
            error.contains(path.file_name().unwrap().to_str().unwrap()),
            "{error}"
        );
    }
    let error =
        common::command(&["data", "import", "--config", import.to_str().unwrap()]).unwrap_err();
    assert!(error.contains("unfinished retirement"), "import: {error}");
    assert!(
        error.contains(path.file_name().unwrap().to_str().unwrap()),
        "{error}"
    );
    assert_eq!(f.drive.drive.log().len(), requests);
    f.drive.faults.lock().unwrap().after = None;
    apply(&f, &path).unwrap();
    assert!(path.with_extension("retired.json").is_file());
    plan(&f);
}

#[test]
fn retirement_fence_precedes_the_first_deletion_and_survives_torn_progress() {
    let f = fixture();
    let (path, _) = plan(&f);
    let object = f
        .new_dataset
        .objects
        .iter()
        .find(|o| o.role == ObjectRole::Normalized)
        .unwrap();
    let local = f.root.join("store").join(&object.key);
    let original = fs::read(&local).unwrap();
    fs::write(&local, b"corrupt retained fixture bytes").unwrap();
    assert!(apply(&f, &path).is_err());
    let log = path.with_extension("progress.jsonseq");
    assert_eq!(fs::read(&log).unwrap(), b"");
    assert!(
        !f.drive
            .drive
            .log()
            .iter()
            .any(|line| line.starts_with("DELETE"))
    );
    fs::write(&log, b"\x1e{\"torn\":").unwrap();
    let error = pipeline("archive", &f.config, &[]).unwrap_err();
    assert!(error.contains("unfinished retirement"), "{error}");
    fs::write(local, original).unwrap();
    apply(&f, &path).unwrap();
    assert!(fs::read(log).unwrap().starts_with(b"\x1e{\"torn\":"));
    plan(&f);
}

#[test]
fn retirement_duplicate_remote_content_uses_exact_file_ids() {
    let f = fixture();
    let original = f.drive.drive.files()[&f.old_catalog].clone();
    let mut catalog: Value = serde_json::from_slice(&original.bytes).unwrap();
    let retained: Value =
        serde_json::from_slice(&f.drive.drive.files()[&f.new_catalog].bytes).unwrap();
    catalog["objects"].as_array_mut().unwrap().push(
        retained["objects"]
            .as_array()
            .unwrap()
            .iter()
            .find(|e| e["key"] == f.shared)
            .unwrap()
            .clone(),
    );
    let object = catalog["objects"]
        .as_array_mut()
        .unwrap()
        .iter_mut()
        .find(|e| e["key"] == f.shared)
        .unwrap();
    let retained_id = object["file_id"].as_str().unwrap().to_string();
    let duplicate = remote(
        &f.drive,
        "obsolete-duplicate",
        &format!("object-{}", object["sha256"].as_str().unwrap()),
        fs::read(f.root.join("store").join(&f.shared)).unwrap(),
    );
    object["file_id"] = json!(duplicate);
    f.drive
        .drive
        .state
        .lock()
        .unwrap()
        .files
        .get_mut(&f.old_catalog)
        .unwrap()
        .bytes = serde_json::to_vec(&catalog).unwrap();
    let (_, planned) = plan(&f);
    assert!(planned.delete_drive.iter().any(|e| e.file_id == duplicate));
    assert!(
        planned
            .retained_drive
            .iter()
            .any(|e| e.file_id == retained_id)
    );
    assert!(!planned.delete_local.iter().any(|e| e.path == f.shared));
    let transfers = f.root.join("pipeline_state/deriv/transfers.json");
    fs::write(
        &transfers,
        json!({"files":{&f.shared:{"file_id":duplicate,"done":false}}}).to_string(),
    )
    .unwrap();
    let (_, pinned) = plan(&f);
    assert!(pinned.retained_drive.iter().any(|e| e.file_id == duplicate));
    assert!(!pinned.delete_drive.iter().any(|e| e.file_id == duplicate));
    fs::write(
        &transfers,
        json!({"files":{&f.shared:{"file_id":duplicate,"done":true}}}).to_string(),
    )
    .unwrap();
    let (path, _) = plan(&f);
    apply(&f, &path).unwrap();
    assert!(!f.drive.drive.files().contains_key(&duplicate));
    assert!(f.drive.drive.files().contains_key(&retained_id));
    common::verify(&f.root.join("store").join(f.new_dataset.key())).unwrap();
}

#[test]
fn retirement_duplicate_catalog_does_not_pin_obsolete_only_content() {
    let f = fixture();
    fs::remove_file(f.root.join("pipeline_state/deriv/progress.json")).unwrap();
    fs::remove_file(f.root.join("pipeline_state/deriv/progress.pages.jsonl")).unwrap();
    let files = f.drive.drive.files();
    let mut duplicate = files[&f.new_catalog].clone();
    let mut catalog: Value = serde_json::from_slice(&duplicate.bytes).unwrap();
    catalog["objects"].as_array_mut().unwrap().push(entry(
        &f.drive,
        &f.root.join("store"),
        &f.pending,
        None,
    ));
    duplicate.bytes = serde_json::to_vec(&catalog).unwrap();
    let duplicate_id = "aaa-obsolete-catalog";
    f.drive
        .drive
        .state
        .lock()
        .unwrap()
        .files
        .insert(duplicate_id.into(), duplicate);
    let (path, planned) = plan(&f);
    assert!(
        planned
            .retained_drive
            .iter()
            .any(|e| e.file_id == f.new_catalog)
    );
    assert!(
        planned
            .delete_drive
            .iter()
            .any(|e| e.file_id == duplicate_id)
    );
    assert!(planned.delete_local.iter().any(|e| e.path == f.pending));
    apply(&f, &path).unwrap();
    assert!(!f.root.join("store").join(&f.pending).exists());
    assert!(!f.drive.drive.files().contains_key(duplicate_id));
}

#[test]
fn retirement_verified_predecessor_jobs_retire_their_closures() {
    let mut f = fixture();
    let state = f.root.join("pipeline_state");
    for name in ["old-intent.json", "old-receipt.json"] {
        let path = state.join("records").join(name);
        let mut record = read_json(&path);
        record["job"] = json!("legacy-deriv");
        let bytes = serde_json::to_vec(&record).unwrap();
        fs::write(&path, &bytes).unwrap();
        f.original_records.insert(path, bytes);
    }
    {
        let mut archive = f.drive.drive.state.lock().unwrap();
        let file = archive.files.get_mut(&f.old_catalog).unwrap();
        let mut catalog: Value = serde_json::from_slice(&file.bytes).unwrap();
        catalog["job"] = json!("legacy-deriv");
        file.bytes = serde_json::to_vec(&catalog).unwrap();
    }
    fs::create_dir_all(state.join("legacy-deriv")).unwrap();
    fs::write(
        state.join("legacy-deriv/transfers.json"),
        json!({"files":{
            format!("catalog/{}/{}", f.old[1], f.old[2]): {"file_id":f.old_catalog,"done":true}
        }})
        .to_string(),
    )
    .unwrap();
    let (_, unowned) = plan(&f);
    assert!(
        !unowned
            .delete_drive
            .iter()
            .any(|e| e.file_id == f.old_catalog)
    );
    amend_migration(&mut f, "predecessor_jobs", json!(["legacy-deriv"]));
    fs::remove_file(state.join("deriv/progress.json")).unwrap();
    fs::remove_file(state.join("deriv/progress.pages.jsonl")).unwrap();
    let alias_bytes = b"standalone migrated payload with no pending acquisition";
    let alias = format!("objects/{}", sha256(alias_bytes));
    fs::write(f.root.join("store").join(&alias), alias_bytes).unwrap();
    amend_migration(&mut f, "storage_aliases", json!([alias]));
    let (path, planned) = plan(&f);
    assert!(
        planned
            .delete_drive
            .iter()
            .any(|e| e.file_id == f.old_catalog)
    );
    for id in &f.old {
        assert!(
            planned
                .delete_local
                .iter()
                .any(|e| e.path == format!("manifests/{id}"))
        );
    }
    for name in ["old-intent.json", "old-receipt.json", "old-catalog.json"] {
        assert!(
            planned
                .references
                .iter()
                .any(|r| r.closure == name && r.status == Status::Retired)
        );
    }
    apply(&f, &path).unwrap();
    for (path, bytes) in &f.original_records {
        assert_eq!(fs::read(path).unwrap(), *bytes);
    }
    // Complete fixture inventory: only the selected daily families and manifest pair survive.
    let expected_objects: BTreeSet<_> = f
        .new_dataset
        .objects
        .iter()
        .chain(&f.new_stream.objects)
        .map(|o| o.key.clone())
        .collect();
    let actual_objects: BTreeSet<_> = fs::read_dir(f.root.join("store/objects"))
        .unwrap()
        .map(|e| format!("objects/{}", e.unwrap().file_name().to_string_lossy()))
        .collect();
    assert_eq!(actual_objects, expected_objects);
    let manifests: BTreeSet<_> = fs::read_dir(f.root.join("store/manifests"))
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .collect();
    assert_eq!(
        manifests,
        BTreeSet::from([
            f.new_dataset.generation.clone(),
            f.new_stream.generation.clone()
        ])
    );
    let remaining = f.drive.drive.files();
    let catalog: Value = serde_json::from_slice(&remaining[&f.new_catalog].bytes).unwrap();
    let expected_remote: BTreeSet<_> = std::iter::once(f.new_catalog.clone())
        .chain(
            [&catalog["dataset"], &catalog["stream"]]
                .into_iter()
                .chain(catalog["objects"].as_array().unwrap())
                .chain(catalog["records"].as_array().unwrap())
                .map(|e| e["file_id"].as_str().unwrap().to_string()),
        )
        .collect();
    assert_eq!(
        remaining.keys().cloned().collect::<BTreeSet<_>>(),
        expected_remote
    );
    common::verify(&f.root.join("store").join(f.new_dataset.key())).unwrap();
    common::verify(&f.root.join("store").join(f.new_stream.key())).unwrap();
}

#[test]
fn retirement_verified_storage_aliases_wait_for_pending_acquisition() {
    let mut f = fixture();
    let bytes = b"standalone migrated page payload";
    let key = format!("objects/{}", sha256(bytes));
    fs::write(f.root.join("store").join(&key), bytes).unwrap();
    let (_, unmapped) = plan(&f);
    assert!(!unmapped.delete_local.iter().any(|e| e.path == key));
    amend_migration(&mut f, "storage_aliases", json!([key]));
    let progress = f.root.join("pipeline_state/deriv/progress.pages.jsonl");
    fs::write(
        &progress,
        format!("{}\n", json!({"sha256":sha256(bytes),"rows":0})),
    )
    .unwrap();
    let (_, pinned) = plan(&f);
    assert!(pinned.retained_objects.contains_key(&key));
    assert!(!pinned.delete_local.iter().any(|e| e.path == key));
    fs::remove_file(progress).unwrap();
    let (path, planned) = plan(&f);
    assert!(planned.delete_local.iter().any(|e| e.path == key));
    apply(&f, &path).unwrap();
    assert!(!f.root.join("store").join(key).exists());
}

#[test]
fn retirement_requires_migration_evidence_in_the_retained_catalog() {
    let f = fixture();
    {
        let mut state = f.drive.drive.state.lock().unwrap();
        let file = state.files.get_mut(&f.new_catalog).unwrap();
        let mut catalog: Value = serde_json::from_slice(&file.bytes).unwrap();
        catalog.as_object_mut().unwrap().remove("records");
        file.bytes = serde_json::to_vec(&catalog).unwrap();
    }
    let error = pipeline("retire", &f.config, &["--plan"]).unwrap_err();
    assert!(
        error.contains("verified migration evidence is not archived"),
        "{error}"
    );
    assert!(
        !f.drive
            .drive
            .log()
            .iter()
            .any(|line| line.starts_with("DELETE"))
    );
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
    let new_catalog: Value = serde_json::from_slice(&before_remote[&f.new_catalog].bytes).unwrap();
    let retained_remote: BTreeSet<_> = [&new_catalog["dataset"], &new_catalog["stream"]]
        .into_iter()
        .chain(new_catalog["objects"].as_array().unwrap())
        .map(|e| e["file_id"].as_str().unwrap())
        .collect();
    let expected_remote: BTreeSet<_> = std::iter::once(f.old_catalog.clone())
        .chain(
            [&old_catalog["dataset"], &old_catalog["stream"]]
                .into_iter()
                .chain(old_catalog["objects"].as_array().unwrap())
                .filter(|e| !retained_remote.contains(e["file_id"].as_str().unwrap()))
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
            .any(|e| e.file_id == f.new_catalog || e.key == f.shared)
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
    // An unpublished catalog has no completed receipt. Dangling fixture receipts would
    // independently pin its closure and make the transfer assertions vacuous.
    for name in ["old-receipt.json", "old-catalog.json"] {
        fs::remove_file(f.root.join("pipeline_state/records").join(name)).unwrap();
    }
    let object = dataset(&f.root.join("store"), &f.old[0])
        .objects
        .into_iter()
        .find(|o| o.role == ObjectRole::Normalized)
        .unwrap()
        .key;
    assert_ne!(object, f.shared);
    assert_ne!(object, f.pending);
    assert!(
        !f.new_dataset
            .objects
            .iter()
            .chain(&f.new_stream.objects)
            .any(|o| o.key == object)
    );
    let original_object = fs::read(f.root.join("store").join(&object)).unwrap();
    let manifests: BTreeMap<_, _> = [&f.old[1], &f.old[2]]
        .into_iter()
        .map(|id| {
            let key = binary_alpha_engine::dataset::manifest_key(id);
            (
                key.clone(),
                fs::read(f.root.join("store").join(key)).unwrap(),
            )
        })
        .collect();
    let (_, unpinned) = plan(&f);
    for id in [&f.old[1], &f.old[2]] {
        assert!(
            unpinned
                .delete_local
                .iter()
                .any(|e| e.path == format!("manifests/{id}")),
            "the no-transfer control must allow deletion of {id}"
        );
    }
    assert!(unpinned.delete_local.iter().any(|e| e.path == object));
    assert!(!unpinned.retained_objects.contains_key(&object));

    let transfer = f.root.join("pipeline_state/deriv/transfers.json");
    let key = format!("catalog/{}/{}", f.old[1], f.old[2]);
    let transfer_bytes =
        json!({"files":{key:{"file_id":"reserved","done":false,"session":"fixture"}}}).to_string();
    fs::write(&transfer, &transfer_bytes).unwrap();
    let (_, catalog_pinned) = plan(&f);
    for id in [&f.old[1], &f.old[2]] {
        assert!(
            !catalog_pinned
                .delete_local
                .iter()
                .any(|e| e.path == format!("manifests/{id}"))
        );
    }

    // Exercise the registry pin independently: no catalog transfer may protect this object.
    fs::remove_file(&transfer).unwrap();
    fs::write(
        f.root.join("pipeline_state/registry.jsonl"),
        format!(
            "{}\n",
            json!({"key":object,"file_id":"reserved-object","done":false,"session":null})
        ),
    )
    .unwrap();
    let (_, object_pinned) = plan(&f);
    assert!(object_pinned.retained_objects.contains_key(&object));
    assert!(!object_pinned.delete_local.iter().any(|e| e.path == object));
    for id in [&f.old[1], &f.old[2]] {
        assert!(
            object_pinned
                .delete_local
                .iter()
                .any(|e| e.path == format!("manifests/{id}"))
        );
    }
    fs::write(&transfer, transfer_bytes).unwrap();
    let (path, _) = plan(&f);
    apply(&f, &path).unwrap();
    assert_eq!(
        fs::read(f.root.join("store").join(&object)).unwrap(),
        original_object
    );
    for (key, bytes) in manifests {
        assert_eq!(fs::read(f.root.join("store").join(key)).unwrap(), bytes);
    }
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

#[test]
fn retirement_pending_baseline_survives_transfer_named_ancestors() {
    for name in ["managed-transfers", "managed-registry"] {
        let mut f = fixture();
        let moved = f.scratch.path(name);
        fs::rename(&f.root, &moved).unwrap();
        let config = fs::read_to_string(&f.config)
            .unwrap()
            .replace(f.root.to_str().unwrap(), moved.to_str().unwrap());
        fs::write(&f.config, config).unwrap();
        f.root = moved;
        let state = f.root.join("pipeline_state/deriv");
        fs::write(
            state.join("progress.json"),
            json!({
                "intent":"pending-intent.json",
                "progress":{"baseline":f.old[1],"pages":[]}
            })
            .to_string(),
        )
        .unwrap();
        fs::write(
            state.join("update.toml"),
            format!(
                "seed_manifest = {:?}\n",
                daily::uri(
                    &f.root
                        .join("store")
                        .join(binary_alpha_engine::dataset::manifest_key(&f.old[0]))
                )
            ),
        )
        .unwrap();
        let baseline = f
            .root
            .join("store")
            .join(binary_alpha_engine::dataset::manifest_key(&f.old[1]));
        let original = fs::read(&baseline).unwrap();
        let (path, p) = plan(&f);
        assert!(
            !p.delete_local
                .iter()
                .any(|e| e.path == format!("manifests/{}", f.old[1])),
            "pending baseline is a deletion candidate under {name}"
        );
        assert!(
            p.references
                .iter()
                .any(|r| r.source.ends_with("progress.json")
                    && r.closure == "pending-intent.json"
                    && r.status == Status::Protected)
        );
        apply(&f, &path).unwrap();
        assert_eq!(fs::read(&baseline).unwrap(), original);
    }
}

#[test]
fn retirement_leftover_seal_links_preserve_completed_record() {
    let f = fixture();
    let (path, _) = plan(&f);
    apply(&f, &path).unwrap();
    let completed = path.with_extension("retired.json");
    let original = fs::read(&completed).unwrap();
    // Model a crash after publication, before scratch unlink, followed by PID reuse.
    let parent = completed.parent().unwrap();
    for suffix in [String::new(), "-0".into(), "-1".into()] {
        let alias = parent.join(format!(".seal-{}{suffix}", std::process::id()));
        fs::hard_link(&completed, alias).unwrap();
    }
    let mut report = Vec::new();
    binary_alpha_app::retire::run(&f.config, None, None, &mut report).unwrap();
    assert!(
        fs::read(&completed).unwrap() == original,
        "allocating a new seal must never truncate a published record through its crash alias"
    );
}

#[test]
fn retirement_refuses_unverified_or_mismatched_migration() {
    let f = fixture();
    let path = f.root.join("pipeline_state/records/migration.json");
    let original = fs::read(&path).unwrap();
    let verified: Value = serde_json::from_slice(&original).unwrap();
    let (_, eligible) = plan(&f);
    for id in &f.old {
        assert!(
            eligible
                .delete_local
                .iter()
                .any(|e| e.path == format!("manifests/{id}"))
        );
    }
    let mut invalid = Vec::new();
    for (pointer, replacement) in [
        ("/phase", json!("converted")),
        ("/equality", Value::Null),
        ("/equality/observations", json!(false)),
        ("/equality/pages", json!(false)),
        ("/equality/source_files", json!(false)),
        ("/equality/candles", json!(false)),
        ("/v1_generations/0", json!("e".repeat(64))),
        ("/v1_stream", json!("e".repeat(64))),
        ("/v2_root", json!("e".repeat(64))),
        ("/v2_stream", json!("e".repeat(64))),
    ] {
        let mut value = verified.clone();
        *value.pointer_mut(pointer).unwrap() = replacement;
        invalid.push((pointer.to_string(), Some(value)));
    }
    invalid.push(("missing immutable record".into(), None));
    for (case, evidence) in invalid {
        if let Some(value) = evidence {
            fs::write(&path, value.to_string()).unwrap();
        } else {
            fs::remove_file(&path).unwrap();
        }
        let result = pipeline("retire", &f.config, &["--job", "deriv", "--plan"]);
        assert!(
            result.is_err(),
            "invalid migration evidence {case} authorized retirement: {result:?}"
        );
        for id in &f.old {
            assert!(
                f.root
                    .join("store")
                    .join(binary_alpha_engine::dataset::manifest_key(id))
                    .is_file()
            );
        }
        assert!(
            !f.drive
                .drive
                .log()
                .iter()
                .any(|line| line.starts_with("DELETE"))
        );
    }
    fs::write(&path, original).unwrap();
    let (path, _) = plan(&f);
    apply(&f, &path).unwrap();
    for id in &f.old {
        assert!(
            !f.root
                .join("store")
                .join(binary_alpha_engine::dataset::manifest_key(id))
                .exists()
        );
    }
}

#[test]
fn retirement_rechecks_name_before_every_delete_retry() {
    // 0 drops the connection without replying; the other cases exercise HTTP retries.
    for status in [503, 401, 0] {
        let f = fixture();
        fs::write(
            &f.config,
            pipeline_toml(
                &f.root,
                &f.drive.drive.base,
                &[("deriv", "job.toml")],
                None,
                2,
            ),
        )
        .unwrap();
        let (path, p) = plan(&f);
        let first = &p.delete_drive[0].file_id;
        f.drive.faults.lock().unwrap().retry_fault = Some((status, DeleteChange::Rename));
        let result = apply(&f, &path);
        assert!(
            result
                .as_ref()
                .is_err_and(|e| e.contains("recorded name mismatch")),
            "name-changed target must refuse deletion after {status}: {result:?}"
        );
        assert_eq!(
            f.drive.drive.files()[first].name,
            "renamed-after-delete-attempt"
        );
        let log = f.drive.drive.log();
        let delete = format!("DELETE /drive/v3/files/{first}");
        let attempt = log.iter().position(|line| line == &delete).unwrap();
        assert!(
            log[attempt + 1..]
                .iter()
                .any(|line| line == &format!("GET /drive/v3/files/{first}"))
        );
        assert!(
            !log[attempt + 1..]
                .iter()
                .any(|line| line.starts_with("DELETE"))
        );
    }
}

#[test]
fn retirement_refuses_plans_sealed_before_verified_migration_gate() {
    let f = fixture();
    let (path, mut p) = plan(&f);
    // A schema-1 seal predates migration verification and reliable acquisition pinning.
    p.schema_version = 1;
    let bytes = serde_json::to_vec_pretty(&p).unwrap();
    let legacy = path
        .parent()
        .unwrap()
        .join(format!("plan-{}.json", sha256(&bytes)));
    fs::write(&legacy, bytes).unwrap();
    let before = f.drive.drive.files();
    let result = apply(&f, &legacy);
    assert!(
        result.as_ref().is_err_and(|e| e.contains("schema")),
        "an old seal cannot bypass current retirement prerequisites: {result:?}"
    );
    for target in &p.delete_local {
        for (key, expected) in &target.files {
            let bytes = fs::read(f.root.join("store").join(key)).unwrap();
            assert_eq!(sha256(&bytes), expected.sha256);
        }
    }
    let after = f.drive.drive.files();
    for (id, file) in before {
        assert_eq!(after[&id].bytes, file.bytes);
        assert_eq!(after[&id].name, file.name);
    }
    assert!(
        !f.drive
            .drive
            .log()
            .iter()
            .any(|line| line.starts_with("DELETE"))
    );
    assert!(!legacy.with_extension("progress.jsonseq").exists());
}

#[test]
fn retirement_rechecks_name_after_checksum_free_confirmation() {
    let f = fixture();
    f.drive.faults.lock().unwrap().no_checksum = true;
    let (path, p) = plan(&f);
    let first = &p.delete_drive[0].file_id;
    f.drive.faults.lock().unwrap().rename_on_metadata = Some((first.clone(), 2));
    let result = apply(&f, &path);
    assert!(
        result
            .as_ref()
            .is_err_and(|e| e.contains("recorded name mismatch")),
        "confirmation observed a rename and must refuse DELETE: {result:?}"
    );
    assert_eq!(
        f.drive.drive.files()[first].name,
        "renamed-during-confirmation"
    );
    assert!(
        !f.drive
            .drive
            .log()
            .iter()
            .any(|line| line.starts_with("DELETE"))
    );
}

#[test]
fn retirement_rechecks_content_and_trash_state_before_delete_retry() {
    for change in [DeleteChange::Trash, DeleteChange::Content] {
        let f = fixture();
        let (path, p) = plan(&f);
        let first = &p.delete_drive[0].file_id;
        f.drive.faults.lock().unwrap().retry_fault = Some((503, change));
        let result = apply(&f, &path);
        assert!(
            result.is_err(),
            "changed target must survive the retry: {result:?}"
        );
        assert!(f.drive.drive.files().contains_key(first));
        let log = f.drive.drive.log();
        let attempt = log
            .iter()
            .position(|line| line == &format!("DELETE /drive/v3/files/{first}"))
            .unwrap();
        assert!(
            !log[attempt + 1..]
                .iter()
                .any(|line| line.starts_with("DELETE"))
        );
    }
}

#[test]
fn retirement_retries_unchanged_delete_after_fresh_metadata() {
    let f = fixture();
    let (path, p) = plan(&f);
    let first = &p.delete_drive[0].file_id;
    f.drive.faults.lock().unwrap().retry_fault = Some((503, DeleteChange::None));
    apply(&f, &path).unwrap();
    assert!(!f.drive.drive.files().contains_key(first));
    let log = f.drive.drive.log();
    let delete = format!("DELETE /drive/v3/files/{first}");
    let first_attempt = log.iter().position(|line| line == &delete).unwrap();
    let last_attempt = log.iter().rposition(|line| line == &delete).unwrap();
    assert!(
        log[first_attempt + 1..last_attempt]
            .iter()
            .any(|line| line == &format!("GET /drive/v3/files/{first}"))
    );
}

#[test]
fn retirement_descendant_ancestry_cannot_authorize_unmapped_legacy_deletion() {
    for local_legacy in [true, false] {
        let f = fixture();
        let root = f.root.join("store");
        let audit_config = f.scratch.path("ancestry-audit.toml");
        fs::write(
            &audit_config,
            toml::to_string(&json!({
                "schema_version":1,"run_mode":"research",
                "storage":{"historical_data_dir":root,"publication_uri":daily::uri(&root)},
                "instruments":[f.new_stream.definition]
            }))
            .unwrap(),
        )
        .unwrap();
        let audit = |manifest: &GenerationManifest| {
            let mut report = Vec::new();
            binary_alpha_app::audit::run(
                &audit_config,
                &daily::uri(&root.join(manifest.key())),
                &mut report,
            )
            .unwrap();
            stream(
                &root,
                &common::generation(&String::from_utf8(report).unwrap()),
            )
        };
        let extra = f.scratch.path("outside-migration.json");
        fs::write(&extra, b"{\"not_verified_by_migration\":true}").unwrap();
        let mut legacy = dataset(&root, &f.old[0]);
        let unique = daily::object(
            &root,
            "provenance/outside-migration.json",
            ObjectRole::Provenance,
            &extra,
        );
        legacy.objects.push(unique.clone());
        daily::publish(&root, &mut legacy);
        let legacy_stream = audit(&legacy);
        let legacy_catalog = archive_fixture(&f.drive, &root, &legacy, &legacy_stream).0;

        // A later valid v2 manifest names an ordinary v1 generation outside the verified
        // root mapping. Its ancestry claim grants no authority to delete that legacy closure.
        let mut descendant = f.new_dataset.clone();
        let lineage_path = f.scratch.path("claimed-ancestry.json");
        fs::write(
            &lineage_path,
            json!({"root_generation":f.new_dataset.generation,
                "parent_generation":f.new_dataset.generation,
                "ancestors":[f.new_dataset.generation,legacy.generation],
                "continuation":{}})
            .to_string(),
        )
        .unwrap();
        descendant
            .objects
            .retain(|o| o.path != "provenance/lineage.json");
        descendant.objects.push(daily::object(
            &root,
            "provenance/lineage.json",
            ObjectRole::Provenance,
            &lineage_path,
        ));
        daily::publish(&root, &mut descendant);
        let descendant_stream = audit(&descendant);
        archive_fixture(&f.drive, &root, &descendant, &descendant_stream);
        if !local_legacy {
            for generation in [&legacy.generation, &legacy_stream.generation] {
                fs::remove_dir_all(root.join("manifests").join(generation)).unwrap();
            }
        }
        let original_remote = f.drive.drive.files()[&legacy_catalog].bytes.clone();
        if !local_legacy {
            let error = pipeline("retire", &f.config, &["--plan"]).unwrap_err();
            assert!(error.contains("unresolved retained generation"), "{error}");
            assert_eq!(
                f.drive.drive.files()[&legacy_catalog].bytes,
                original_remote
            );
            assert!(
                !f.drive
                    .drive
                    .log()
                    .iter()
                    .any(|line| line.starts_with("DELETE"))
            );
            continue;
        }
        let (path, plan) = plan(&f);
        assert!(
            !plan
                .delete_drive
                .iter()
                .any(|d| d.file_id == legacy_catalog),
            "unverified v1 catalog is protected even when named by a v2 descendant"
        );
        assert!(plan.retained_objects.contains_key(&unique.key));
        if local_legacy {
            assert!(plan.retained_manifests.contains(&legacy.key()));
            assert!(
                plan.retained_manifests
                    .contains(&binary_alpha_engine::dataset::manifest_key(
                        &legacy_stream.generation
                    ))
            );
        }
        apply(&f, &path).unwrap();
        assert_eq!(
            f.drive.drive.files()[&legacy_catalog].bytes,
            original_remote
        );
        assert!(root.join(unique.key).exists());
        common::verify(&root.join(descendant.key())).unwrap();
    }
}

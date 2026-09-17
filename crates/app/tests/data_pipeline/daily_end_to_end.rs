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

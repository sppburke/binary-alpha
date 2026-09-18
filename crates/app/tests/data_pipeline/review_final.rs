//! Final review regressions: preservation authority, authentic pre-session upgrade, and
//! complete record recovery. All market data, transports, and deletions are fixtures.
use super::*;

/// The older retirement fixtures hand-write migration records. Measure their new source
/// authority independently, keeping all existing deletion assertions and fake faults intact.
pub(super) fn fixture_source_preservation(
    root: &Path,
    sources: &[&GenerationManifest],
    legacy: &StreamManifest,
    target: &GenerationManifest,
    product: &StreamManifest,
    scratch: &Path,
) -> Value {
    use binary_alpha_app::{archive::CandleWriter, daily, store::Store};
    use binary_alpha_engine::stream::{InstrumentStream, Source, StreamSummary};
    use parquet::file::reader::{FileReader, SerializedFileReader};
    let store = Store::filesystem(root);
    let observation = |source: &GenerationManifest, range: (i64, i64)| {
        let mut hash = Sha256::new();
        let mut times = Vec::new();
        daily::read_generation_lossless(&store, source, |row| {
            let time = row.time()?;
            if time < range.0 || time > range.1 {
                return Ok(());
            }
            times.push(time);
            match row {
                daily::LosslessRow::Tick(tick) => {
                    hash.update(b"tick");
                    hash.update(tick.event_time_micros.to_le_bytes());
                    hash.update(tick.price_units.to_le_bytes());
                }
                daily::LosslessRow::Bar(bar) => {
                    hash.update(b"bar");
                    hash.update(serde_json::to_vec(&bar).unwrap());
                }
            }
            Ok(())
        })
        .unwrap();
        json!({"rows":times.len(),"first":times.first(),"last":times.last(),
            "sha256":binary_alpha_engine::hex(&hash.finalize()),"instrument":source.instrument,
            "role":source.role,"native_granularity":source.native_granularity,
            "price_representation":source.price_representation})
    };
    let mut datasets = BTreeMap::new();
    for source in sources {
        let range = (
            binary_alpha_engine::market::parse_event_time_micros(&source.coverage.first_event_time)
                .unwrap(),
            binary_alpha_engine::market::parse_event_time_micros(&source.coverage.last_event_time)
                .unwrap(),
        );
        let original = observation(source, range);
        let replacement = observation(target, range);
        assert_eq!(
            original, replacement,
            "fixture cannot claim unmeasured source preservation"
        );
        datasets.insert(
            source.generation.clone(),
            json!({"equal":true,"interval_inclusive":[range.0,range.1],
            "original":original,"replacement":replacement}),
        );
    }
    let source = sources
        .iter()
        .find(|s| s.generation == legacy.source_generation)
        .unwrap();
    let mut engine =
        InstrumentStream::new(&legacy.definition, Source::from_manifest(source)).unwrap();
    let mut rows = vec![Vec::new(); legacy.streams.len()];
    let mut finalized = Vec::new();
    daily::read_generation(&store, target, |row| {
        engine
            .push(
                row.observation(legacy.definition.price_scale)?,
                &mut finalized,
            )
            .map_err(|e| e.to_string())?;
        for (index, candle) in finalized.drain(..) {
            rows[index].push(candle);
        }
        Ok(())
    })
    .unwrap();
    let mut original_hash = Sha256::new();
    let mut replacement_hash = Sha256::new();
    let mut summaries = Vec::new();
    for (spec, candles) in legacy.streams.iter().zip(rows) {
        let path = scratch.join("reconstructed-candles.parquet");
        let mut writer = CandleWriter::create(
            &path,
            &legacy.definition.id(),
            legacy.definition.price_scale,
            spec.duration_seconds,
            spec.offset_seconds,
        )
        .unwrap();
        for candle in candles {
            writer.push(&candle).unwrap();
        }
        let (rows, first, last) = writer.finish().unwrap();
        summaries.push(StreamSummary {
            duration_seconds: spec.duration_seconds,
            offset_seconds: spec.offset_seconds,
            rows,
            first_open_time: first.map(time_text),
            last_close_time: last.map(time_text),
        });
        let logical = StreamSummary::object_path(spec.duration_seconds, spec.offset_seconds);
        let object = legacy.objects.iter().find(|o| o.path == logical).unwrap();
        for (hash, path) in [
            (&mut original_hash, root.join(&object.key)),
            (&mut replacement_hash, path),
        ] {
            hash.update(spec.duration_seconds.to_le_bytes());
            hash.update(spec.offset_seconds.to_le_bytes());
            let reader = SerializedFileReader::new(File::open(path).unwrap()).unwrap();
            for row in reader.get_row_iter(None).unwrap() {
                hash.update(format!("{:?}\n", row.unwrap()).as_bytes());
            }
        }
    }
    let profile = legacy
        .objects
        .iter()
        .find(|o| o.path == "profile.json")
        .unwrap();
    let original = json!({"sha256":binary_alpha_engine::hex(&original_hash.finalize()),"streams":legacy.streams,
        "profile":read_json(&root.join(&profile.key))});
    let reconstructed = json!({"sha256":binary_alpha_engine::hex(&replacement_hash.finalize()),"streams":summaries,
        "profile":engine.profile()});
    assert_eq!(
        original, reconstructed,
        "every legacy candle/profile must reconstruct"
    );
    json!({"target_root":target.generation,"target_stream":product.generation,"datasets":datasets,
        "streams":{legacy.generation.clone():{"equal":true,"source_generation":source.generation,
            "definition":legacy.definition,"original":original,"reconstructed":reconstructed}}})
}

fn records(root: &Path) -> BTreeMap<String, Vec<u8>> {
    fs::read_dir(root.join("pipeline_state/records"))
        .unwrap()
        .map(Result::unwrap)
        .filter(|entry| entry.file_type().unwrap().is_file())
        .map(|entry| {
            (
                entry.file_name().to_str().unwrap().to_string(),
                fs::read(entry.path()).unwrap(),
            )
        })
        .collect()
}

fn only_deriv(f: &Fixture) {
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
}

fn restore_deriv(f: &Fixture, report: &str) -> PathBuf {
    let fresh = f.scratch.path("fresh");
    let config = f.scratch.path("fresh.toml");
    fs::write(&config, pipeline_toml(&fresh, &f.drive.base, &[], None, 3)).unwrap();
    let line = job_line(report, "deriv");
    pipeline(
        "restore",
        &config,
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
    fresh
}

#[test]
fn adversarial_older_v1_observation_loss() {
    for remove_duplicate in [false, true] {
        let f = fixture(&format!("final_older_v1_{remove_duplicate}"));
        only_deriv(&f);
        let store = f.scratch.path("producer/store");
        if remove_duplicate {
            let rows = |start, end| {
                deriv_ticks(start, end)
                    .into_iter()
                    .map(|(t, p)| (t * 1_000_000_000, p.parse::<f64>().unwrap()))
                    .collect::<Vec<_>>()
            };
            let mut day1 = rows(DAY1, DAY1 + 600);
            day1.insert(1, day1[0]);
            let day2 = rows(DAY2, DERIV_SEED_END);
            write_daily_directory(
                &f.scratch.path("sources/deriv/EURUSD"),
                "EURUSD",
                "frxEURUSD",
                &[("2025-08-11", &day1), ("2025-08-12", &day2)],
            );
        }
        let first = lineage::legacy_import(&f.scratch.path("deriv-import.toml")).unwrap();
        let old = dataset(&store, imported_generation(&first, "deriv:frxEURUSD"));
        let old_rows = ticks(&store, &old);
        let old_stream = common::legacy::stream(&f.scratch.path("deriv.toml"), &store, &old);
        let mut day1: Vec<_> = old_rows
            .iter()
            .filter(|t| t.event_time_micros < DAY2 * 1_000_000)
            .map(|t| (t.event_time_micros * 1000, t.price_units as f64 / 100_000.0))
            .collect();
        if remove_duplicate {
            let index = day1
                .windows(2)
                .position(|w| w[0] == w[1])
                .expect("fixture repeated tick");
            day1.remove(index);
        } else {
            day1[0].1 = 9.87654;
        }
        let mut day2: Vec<_> = old_rows
            .iter()
            .filter(|t| t.event_time_micros >= DAY2 * 1_000_000)
            .map(|t| (t.event_time_micros * 1000, t.price_units as f64 / 100_000.0))
            .collect();
        day2.push((
            DERIV_SEED_END * 1_000_000_000,
            deriv_price(DERIV_SEED_END).parse().unwrap(),
        ));
        write_daily_directory(
            &f.scratch.path("sources/deriv/EURUSD"),
            "EURUSD",
            "frxEURUSD",
            &[("2025-08-11", &day1), ("2025-08-12", &day2)],
        );
        let second = lineage::legacy_import(&f.scratch.path("deriv-import.toml")).unwrap();
        let newer = dataset(&store, imported_generation(&second, "deriv:frxEURUSD"));
        common::legacy::stream(&f.scratch.path("deriv.toml"), &store, &newer);
        assert_ne!(old.generation, newer.generation);
        pipeline("migrate", &f.pipeline, &[]).unwrap();
        let state = read_json(
            &f.scratch
                .path("producer/pipeline_state/deriv/migration.json"),
        );
        let record = read_json(
            &f.scratch
                .path("producer/pipeline_state/records")
                .join(state["record"].as_str().unwrap()),
        );
        assert_eq!(
            record["source_preservation"]["datasets"][&old.generation]["equal"],
            false
        );
        assert_eq!(
            record["source_preservation"]["streams"][&old_stream.generation]["equal"],
            false
        );
        assert_eq!(
            record["source_preservation"]["datasets"][&newer.generation]["equal"],
            true
        );
        let archive = pipeline("archive", &f.pipeline, &[]).unwrap();
        let original_records = records(&f.scratch.path("producer"));
        let fresh = restore_deriv(&f, &archive);
        assert_eq!(records(&fresh), original_records);
        assert_eq!(
            ticks(&fresh.join("store"), &old),
            old_rows,
            "every old row and duplicate restored"
        );
        for object in old.objects.iter().chain(&old_stream.objects) {
            assert_eq!(
                fs::read(store.join(&object.key)).unwrap(),
                fs::read(fresh.join("store").join(&object.key)).unwrap()
            );
        }
        let planned = pipeline("retire", &f.pipeline, &["--plan"]).unwrap();
        let path = PathBuf::from(planned.split_whitespace().nth(2).unwrap());
        let plan = read_json(&path);
        for generation in [&old.generation, &old_stream.generation] {
            assert!(
                plan["references"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .any(|r| r["closure"] == *generation
                        && r["status"] == "protected"
                        && r["reason"].as_str().unwrap().contains("per-source"))
            );
            assert!(!plan["delete_local"].as_array().unwrap().iter().any(|d| {
                d["files"]
                    .get(binary_alpha_engine::dataset::manifest_key(generation))
                    .is_some()
            }));
        }
        pipeline("retire", &f.pipeline, &["--apply", path.to_str().unwrap()]).unwrap();
        assert_eq!(
            ticks(&store, &old),
            old_rows,
            "retirement loses no older observation"
        );
        common::verify(&store.join(old_stream.key())).unwrap();
    }
}

#[test]
fn final_archive_latest_receipt_restores_every_record() {
    let f = fixture("final_latest_receipt");
    only_deriv(&f);
    lineage::legacy_import(&f.scratch.path("deriv-import.toml")).unwrap();
    pipeline("migrate", &f.pipeline, &[]).unwrap();
    let report = pipeline("archive", &f.pipeline, &[]).unwrap();
    let before = f.drive.files().len();
    assert_eq!(pipeline("archive", &f.pipeline, &[]).unwrap(), report);
    assert_eq!(f.drive.files().len(), before);
    let original = records(&f.scratch.path("producer"));
    fs::rename(
        f.scratch.path("producer"),
        f.scratch.path("producer.hidden"),
    )
    .unwrap();
    let fresh = restore_deriv(&f, &report);
    assert_eq!(records(&fresh), original);
    restore_deriv(&f, &report);
    assert_eq!(
        records(&fresh),
        original,
        "receipt publication is idempotent"
    );
}

#[test]
fn per_source_proofs_bind_tick_scale_and_every_legacy_stream_definition() {
    let f = fixture("final_source_scale");
    only_deriv(&f);
    let store = f.scratch.path("producer/store");
    let import_path = f.scratch.path("deriv-import.toml");
    let core = fs::read_to_string(&import_path).unwrap();
    let write = |price, end| {
        let rows = (DAY1..end)
            .step_by(2)
            .map(|t| (t * 1_000_000_000, price))
            .collect::<Vec<_>>();
        // Replace only this fixture's original source directory, preserving its immutable store.
        fs::remove_dir_all(f.scratch.path("sources/deriv/EURUSD")).unwrap();
        write_daily_directory(
            &f.scratch.path("sources/deriv/EURUSD"),
            "EURUSD",
            "frxEURUSD",
            &[("2025-08-11", &rows)],
        );
    };
    write(1.2345, DAY1 + 60);
    fs::write(
        &import_path,
        core.replace("price_scale = 5", "price_scale = 4"),
    )
    .unwrap();
    let first = lineage::legacy_import(&import_path).unwrap();
    let old = dataset(&store, imported_generation(&first, "deriv:frxEURUSD"));
    write(0.12345, DAY1 + 60);
    fs::write(&import_path, &core).unwrap();
    let second = lineage::legacy_import(&import_path).unwrap();
    let middle = dataset(&store, imported_generation(&second, "deriv:frxEURUSD"));
    let alternate = f.scratch.path("alternate-stream.toml");
    fs::write(
        &alternate,
        fs::read_to_string(f.scratch.path("deriv.toml"))
            .unwrap()
            .replace("duration_seconds = 10", "duration_seconds = 20"),
    )
    .unwrap();
    let prior_stream = common::legacy::stream(&alternate, &store, &middle);
    assert_eq!(
        ticks(&store, &old),
        ticks(&store, &middle),
        "same integers at different scales"
    );
    write(0.12345, DAY1 + 80);
    let third = lineage::legacy_import(&import_path).unwrap();
    let newest = dataset(&store, imported_generation(&third, "deriv:frxEURUSD"));
    common::legacy::stream(&f.scratch.path("deriv.toml"), &store, &newest);
    pipeline("migrate", &f.pipeline, &[]).unwrap();
    let state = read_json(
        &f.scratch
            .path("producer/pipeline_state/deriv/migration.json"),
    );
    let record = read_json(
        &f.scratch
            .path("producer/pipeline_state/records")
            .join(state["record"].as_str().unwrap()),
    );
    let proofs = &record["source_preservation"];
    assert_eq!(proofs["datasets"][&old.generation]["equal"], false);
    assert_eq!(proofs["datasets"][&middle.generation]["equal"], true);
    assert_eq!(proofs["streams"][&prior_stream.generation]["equal"], true);
    assert_eq!(
        proofs["streams"][&prior_stream.generation]["definition"]["candles"][0]["duration_seconds"],
        20
    );
    let archive = pipeline("archive", &f.pipeline, &[]).unwrap();
    let fresh = restore_deriv(&f, &archive);
    assert_eq!(
        dataset(&fresh.join("store"), &old.generation).price_representation,
        old.price_representation
    );
    let planned = pipeline("retire", &f.pipeline, &["--plan"]).unwrap();
    let plan = read_json(Path::new(planned.split_whitespace().nth(2).unwrap()));
    assert!(
        plan["references"]
            .as_array()
            .unwrap()
            .iter()
            .any(|r| r["closure"] == old.generation && r["status"] == "protected")
    );
    assert!(
        plan["references"]
            .as_array()
            .unwrap()
            .iter()
            .any(|r| r["closure"] == prior_stream.generation && r["status"] == "retired")
    );
}

#[test]
fn older_pocket_provider_columns_remain_in_restored_original_closure() {
    let f = fixture("final_provider_columns");
    let producer = f.scratch.path("producer");
    let store = producer.join("store");
    fs::write(
        &f.pipeline,
        pipeline_toml(
            &producer,
            &f.drive.base,
            &[("pocket", "pocket.toml")],
            None,
            3,
        ),
    )
    .unwrap();
    let first = lineage::legacy_import(&f.scratch.path("pocket-import.toml")).unwrap();
    let old = dataset(
        &store,
        imported_generation(&first, "pocket_option:AEDCNY_otc"),
    );
    let mut newer_rows = bar_rows(POCKET_START, POCKET_SEED_END + 5);
    for row in &mut newer_rows {
        row.symbol_id += 1;
    }
    write_collection(
        &f.scratch.path("sources/pocket"),
        &[AssetSpec {
            asset: "AEDCNY_otc",
            expected_symbol_id: None,
            symbol_id: Some(POCKET_SYMBOL_ID + 1),
            files: vec![newer_rows],
            metadata: true,
        }],
    );
    lineage::legacy_import(&f.scratch.path("pocket-import.toml")).unwrap();
    pipeline("migrate", &f.pipeline, &[]).unwrap();
    let state = read_json(&producer.join("pipeline_state/pocket/migration.json"));
    let record = read_json(
        &producer
            .join("pipeline_state/records")
            .join(state["record"].as_str().unwrap()),
    );
    assert_eq!(
        record["source_preservation"]["datasets"][&old.generation]["equal"],
        false
    );
    let report = pipeline("archive", &f.pipeline, &[]).unwrap();
    let fresh = f.scratch.path("fresh");
    let config = f.scratch.path("fresh.toml");
    fs::write(&config, pipeline_toml(&fresh, &f.drive.base, &[], None, 3)).unwrap();
    let line = job_line(&report, "pocket");
    pipeline(
        "restore",
        &config,
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
    for object in &old.objects {
        assert_eq!(
            fs::read(store.join(&object.key)).unwrap(),
            fs::read(fresh.join("store").join(&object.key)).unwrap()
        );
    }
    let planned = pipeline("retire", &f.pipeline, &["--plan"]).unwrap();
    let plan = read_json(Path::new(planned.split_whitespace().nth(2).unwrap()));
    assert!(
        plan["references"]
            .as_array()
            .unwrap()
            .iter()
            .any(|r| r["closure"] == old.generation && r["status"] == "protected")
    );
}

#[test]
fn missing_per_source_proof_retains_and_archives_mapped_v1_closure() {
    let f = fixture("final_missing_source_proof");
    only_deriv(&f);
    let store = f.scratch.path("producer/store");
    let report = lineage::legacy_import(&f.scratch.path("deriv-import.toml")).unwrap();
    let legacy = dataset(&store, imported_generation(&report, "deriv:frxEURUSD"));
    pipeline("migrate", &f.pipeline, &[]).unwrap();
    let state = read_json(
        &f.scratch
            .path("producer/pipeline_state/deriv/migration.json"),
    );
    let path = f
        .scratch
        .path("producer/pipeline_state/records")
        .join(state["record"].as_str().unwrap());
    let mut record = read_json(&path);
    record["source_preservation"]["datasets"]
        .as_object_mut()
        .unwrap()
        .remove(&legacy.generation);
    fs::write(&path, serde_json::to_vec_pretty(&record).unwrap()).unwrap();
    let archived = pipeline("archive", &f.pipeline, &[]).unwrap();
    let fresh = restore_deriv(&f, &archived);
    assert_eq!(ticks(&fresh.join("store"), &legacy), ticks(&store, &legacy));
    let planned = pipeline("retire", &f.pipeline, &["--plan"]).unwrap();
    let plan = read_json(Path::new(planned.split_whitespace().nth(2).unwrap()));
    assert!(
        plan["references"]
            .as_array()
            .unwrap()
            .iter()
            .any(|r| r["closure"] == legacy.generation
                && r["status"] == "protected"
                && r["reason"].as_str().unwrap().contains("per-source"))
    );
    assert!(
        !plan["delete_local"]
            .as_array()
            .unwrap()
            .iter()
            .any(|d| d["files"].get(legacy.key()).is_some())
    );
}

/// Execute the actual pre-session strict schema and proof-v1 writer. This finite fixture
/// uses only tracked source from the pinned commit, the unchanged lockfile, and an isolated
/// build directory. It never checks out or changes another worktree or fetches Git history.
fn proof_v1_executable() -> &'static Path {
    static EXECUTABLE: std::sync::OnceLock<PathBuf> = std::sync::OnceLock::new();
    EXECUTABLE.get_or_init(|| {
        const REVISION: &str = "89b10358d17fbb2371a3dc8406f3b85f01fab3ff";
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
        let snapshot = root.join("target/review-final-proof-v1");
        fs::create_dir_all(&snapshot).unwrap();
        let archive = Command::new("git")
            .current_dir(&root)
            .args([
                "archive",
                REVISION,
                "Cargo.toml",
                "Cargo.lock",
                "rust-toolchain.toml",
                "crates",
            ])
            .output()
            .unwrap();
        assert!(
            archive.status.success(),
            "pinned historical source required: {}",
            String::from_utf8_lossy(&archive.stderr)
        );
        let tar = snapshot.join("source.tar");
        fs::write(&tar, archive.stdout).unwrap();
        assert!(
            Command::new("tar")
                .args(["-xf"])
                .arg(&tar)
                .arg("-C")
                .arg(&snapshot)
                .status()
                .unwrap()
                .success()
        );
        assert_eq!(
            fs::read(root.join("Cargo.lock")).unwrap(),
            fs::read(snapshot.join("Cargo.lock")).unwrap()
        );
        // Re-run build.rs with Git discovery fenced, including when a prior fixture build
        // used this cache. Source bytes stay exactly those of the pinned revision.
        File::options()
            .write(true)
            .open(snapshot.join("crates/app/build.rs"))
            .unwrap()
            .set_modified(std::time::SystemTime::now())
            .unwrap();
        let output = Command::new(env!("CARGO"))
            .current_dir(&snapshot)
            .args([
                "build",
                "-p",
                "binary-alpha-app",
                "--bin",
                "binary-alpha",
                "--locked",
                "--offline",
            ])
            .env("CARGO_TARGET_DIR", snapshot.join("build"))
            .env("GIT_CEILING_DIRECTORIES", &snapshot)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "historical fixture build: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        snapshot.join("build/debug/binary-alpha")
    })
}

#[test]
#[ignore = "builds the historical executable from commit 89b1035, which a shallow hosted checkout does not hold; run locally with --ignored (the probe rehearsal exercises the same upgrade with the real old binary)"]
fn genuine_proof_v1_without_session_upgrades_only_calendar_addition() {
    let old_exe = proof_v1_executable();
    let f = fixture("final_genuine_v1_session_upgrade");
    only_deriv(&f);
    let core_path = f.scratch.path("deriv.toml");
    let new_core = fs::read_to_string(&core_path).unwrap();
    let old_core = new_core
        .lines()
        .filter(|line| !line.starts_with("session ="))
        .collect::<Vec<_>>()
        .join("\n")
        + "\n";
    let command = || {
        Command::new(old_exe)
            .args(["data", "pipeline", "migrate", "--config"])
            .arg(&f.pipeline)
            .output()
            .unwrap()
    };
    let refused = command();
    assert!(!refused.status.success());
    let rejection = format!(
        "{}{}",
        String::from_utf8_lossy(&refused.stdout),
        String::from_utf8_lossy(&refused.stderr)
    );
    assert!(rejection.contains("unknown field `session`"), "{rejection}");
    fs::write(&core_path, &old_core).unwrap();
    let import_core = f.scratch.path("deriv-import.toml");
    let text = fs::read_to_string(&import_core).unwrap();
    fs::write(
        &import_core,
        text.lines()
            .filter(|line| !line.starts_with("session ="))
            .collect::<Vec<_>>()
            .join("\n")
            + "\n",
    )
    .unwrap();
    let report = lineage::legacy_import(&import_core).unwrap();
    let store = f.scratch.path("producer/store");
    let legacy = dataset(&store, imported_generation(&report, "deriv:frxEURUSD"));
    common::legacy::stream(&core_path, &store, &legacy);
    let produced = command();
    assert!(
        produced.status.success(),
        "{}",
        String::from_utf8_lossy(&produced.stderr)
    );
    let checkpoint = f
        .scratch
        .path("producer/pipeline_state/deriv/migration.json");
    let prior = read_json(&checkpoint);
    assert_eq!(prior["proof_version"], 1);
    let prior_record_path = f
        .scratch
        .path("producer/pipeline_state/records")
        .join(prior["record"].as_str().unwrap());
    let prior_bytes = fs::read(&prior_record_path).unwrap();
    let old_stream = stream(&store, prior["stream"].as_str().unwrap());
    assert!(old_stream.definition.session.is_none());
    assert_eq!(
        old_stream.code_revision, "unavailable",
        "source snapshot must not claim the enclosing checkout revision"
    );
    fs::write(
        &core_path,
        new_core.replace("max_pages = 50", "max_pages = 51"),
    )
    .unwrap();
    assert!(
        pipeline("migrate", &f.pipeline, &[])
            .unwrap_err()
            .contains("configuration/evidence changed")
    );
    assert_eq!(read_json(&checkpoint), prior);
    fs::write(&core_path, &new_core).unwrap();
    let interrupted = data_pipeline::migrate_with(
        &f.pipeline,
        None,
        &|_| Err("fixture interrupted after converted".into()),
        &mut Vec::new(),
    )
    .unwrap_err();
    assert!(interrupted.contains("failed"));
    let converted = read_json(&checkpoint);
    assert_eq!(converted["phase"], "converted");
    assert_eq!(
        converted["configuration_upgrade"]["old_binding"],
        prior["binding"]
    );
    pipeline("migrate", &f.pipeline, &[]).unwrap();
    let current = read_json(&checkpoint);
    assert_eq!(current["proof_version"], 4);
    assert_ne!(current["stream"], prior["stream"]);
    let upgraded = read_json(
        &f.scratch
            .path("producer/pipeline_state/records")
            .join(current["record"].as_str().unwrap()),
    );
    assert!(
        serde_json::from_value::<binary_alpha_app::lineage::MigrationRecord>(upgraded.clone())
            .unwrap()
            .verified()
    );
    assert_eq!(upgraded["supersedes"], prior["record"]);
    let binding = &upgraded["configuration_upgrade"];
    assert_eq!(binding["old_binding"], prior["binding"]);
    assert_eq!(binding["new_binding"], current["binding"]);
    let parse = |text: &str| binary_alpha_engine::config::Config::parse(text).unwrap();
    assert_eq!(binding["old_config_hash"], parse(&old_core).content_hash());
    assert_eq!(binding["new_config_hash"], parse(&new_core).content_hash());
    assert_eq!(
        binding["calendars"][0]["session"],
        serde_json::to_value(&parse(&new_core).instruments[0].session).unwrap()
    );
    let proof =
        &upgraded["continuation_preservation"]["reconstructed_streams"][&old_stream.generation];
    assert_eq!(proof["observations"]["equal"], true);
    assert_eq!(proof["candles"]["equal"], true);
    assert_eq!(proof["session_product_verified"], true);
    for field in ["session_product_verified", "observations", "candles"] {
        let mut contradictory = upgraded.clone();
        let proof = &mut contradictory["continuation_preservation"]["reconstructed_streams"]
            [&old_stream.generation];
        if field == "session_product_verified" {
            proof[field] = json!(false);
        } else {
            proof[field]["equal"] = json!(false);
        }
        assert!(
            !serde_json::from_value::<binary_alpha_app::lineage::MigrationRecord>(contradictory)
                .unwrap()
                .verified()
        );
    }
    assert_eq!(fs::read(&prior_record_path).unwrap(), prior_bytes);
    let archived = pipeline("archive", &f.pipeline, &[]).unwrap();
    let originals = records(&f.scratch.path("producer"));
    let fresh = restore_deriv(&f, &archived);
    assert_eq!(records(&fresh), originals);
    let planned = pipeline("retire", &f.pipeline, &["--plan"]).unwrap();
    let plan = read_json(Path::new(planned.split_whitespace().nth(2).unwrap()));
    let retained = stream(&store, current["stream"].as_str().unwrap());
    for object in &retained.objects {
        assert!(
            !plan["delete_local"]
                .as_array()
                .unwrap()
                .iter()
                .any(|d| d["path"] == object.key)
        );
    }
    let old_candles: Vec<_> = old_stream
        .objects
        .iter()
        .filter(|o| {
            o.path.starts_with("candles/") && !retained.objects.iter().any(|new| new.key == o.key)
        })
        .collect();
    assert!(!old_candles.is_empty());
    for object in old_candles {
        assert!(
            plan["delete_local"]
                .as_array()
                .unwrap()
                .iter()
                .any(|d| d["path"] == object.key),
            "unreachable superseded candle is eligible: {}",
            object.key
        );
    }
    pipeline("migrate", &f.pipeline, &[]).unwrap();
    assert_eq!(records(&f.scratch.path("producer")), originals);
    assert!(f.deriv.requests().is_empty());
}

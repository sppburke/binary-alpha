//! Supported first-instrument lifecycle: no import, no source archive, loopback services only.
use super::*;
use binary_alpha_app::retire::Plan;
use std::collections::BTreeSet;

struct NewJobs {
    scratch: Scratch,
    deriv: FakeBroker,
    _pocket: FakeBroker,
    drive: FakeDrive,
    config: PathBuf,
    jobs: Vec<data_pipeline::Job>,
}

fn new_jobs(name: &str, pages: u32, pocket_price: Option<&str>) -> NewJobs {
    let scratch = Scratch::new(name);
    let start = DAY2 - 600;
    let deriv = serve_broker(Kind::Deriv(Arc::new(deriv_ticks(start, DAY2 + 1000))));
    let pocket = serve_broker(Kind::Pocket {
        from: start,
        to: DAY2 + 1000,
    });
    pocket.set(BrokerFaults {
        pocket_price: pocket_price.map(str::to_string),
        ..BrokerFaults::default()
    });
    let drive = serve_drive();
    let config = scratch.path("pipeline.toml");
    fs::write(
        &config,
        pipeline_toml(&scratch.path("producer"), &drive.base, &[], None, 3),
    )
    .unwrap();
    let session = scratch.path("always.toml");
    fs::write(&session, "kind = \"always\"\n").unwrap();
    for (broker, symbol, core) in [
        ("deriv", "frxEURUSD", deriv_core(&deriv.url, 60, pages, 600)),
        (
            "pocket_option",
            "AEDCNY_otc",
            pocket_core(&pocket.url, "demo", BAR_GRANULARITY, 60, pages, 600),
        ),
    ] {
        let mut core: toml::Value = toml::from_str(&core).unwrap();
        core["history"]["start"] = time_text(start * 1_000_000).into();
        core["history"]["end"] = time_text((DAY2 + 100) * 1_000_000).into();
        // Registration must replace the policy's symbol; it cannot require a hand-generated
        // template for the target instrument. Discovery proves the requested target exists.
        core["instruments"][0]["provider_symbol"] = "TEMPLATE".into();
        core["history"]["instruments"] = toml::Value::Array(vec!["TEMPLATE".into()]);
        let template = scratch.path(&format!("{broker}-template.toml"));
        fs::write(&template, toml::to_string(&core).unwrap()).unwrap();
        let missing = pipeline(
            "add-job",
            &config,
            &[
                "--broker",
                broker,
                "--symbol",
                symbol,
                "--template",
                template.to_str().unwrap(),
            ],
        )
        .unwrap_err();
        assert!(
            missing.contains("explicit [instruments.session]"),
            "{missing}"
        );
        pipeline(
            "add-job",
            &config,
            &[
                "--broker",
                broker,
                "--symbol",
                symbol,
                "--template",
                template.to_str().unwrap(),
                "--session",
                session.to_str().unwrap(),
            ],
        )
        .unwrap();
    }
    let parsed =
        data_pipeline::PipelineConfig::parse(&fs::read_to_string(&config).unwrap()).unwrap();
    let jobs = parsed.jobs;
    assert_eq!(jobs.len(), 2);
    for (job, currency, digits) in [
        (&jobs[0], "USD", 5),
        (&jobs[1], "CNY", if pocket_price.is_some() { 11 } else { 4 }),
    ] {
        let core: toml::Value = toml::from_str(
            &fs::read_to_string(scratch.path(job.config.to_str().unwrap())).unwrap(),
        )
        .unwrap();
        assert_eq!(
            core["instruments"][0]["quote_currency"].as_str(),
            Some(currency)
        );
        assert_eq!(
            core["instruments"][0]["price_scale"].as_integer(),
            Some(digits)
        );
        assert_eq!(
            core["instruments"][0]["session"]["kind"].as_str(),
            Some("always")
        );
        assert!(core.get("import").is_none());
    }
    assert!(!scratch.path("sources").exists());
    assert_eq!(
        fs::read_dir(scratch.path("producer/store"))
            .unwrap()
            .count(),
        0
    );
    NewJobs {
        scratch,
        deriv,
        _pocket: pocket,
        drive,
        config,
        jobs,
    }
}

fn daily_only(store: &Path) -> BTreeSet<String> {
    let local = binary_alpha_app::store::Store::filesystem(store);
    let mut keys = BTreeSet::new();
    for id in local.list_manifests().unwrap() {
        let path = store.join(format!("manifests/{id}/ready.json"));
        let value = read_json(&path);
        assert_eq!(value["layout"], "daily-v2");
        run(&[
            "data",
            "verify",
            "--manifest",
            &format!("file://{}", path.display()),
        ])
        .unwrap();
        for object in value["objects"].as_array().unwrap() {
            let path = object["path"].as_str().unwrap();
            assert!(
                path.starts_with("observations/") && path.ends_with(".parquet")
                    || path.starts_with("pages/") && path.ends_with(".parquet")
                    || path.starts_with("candles/") && path.ends_with(".parquet")
                    || [
                        "provenance/coverage.json",
                        "provenance/lineage.json",
                        "profile.json"
                    ]
                    .contains(&path),
                "{path}"
            );
            keys.insert(object["key"].as_str().unwrap().to_string());
        }
    }
    if store.join("objects").exists() {
        for object in fs::read_dir(store.join("objects")).unwrap() {
            let object = object.unwrap();
            assert!(
                keys.contains(&format!("objects/{}", object.file_name().to_str().unwrap())),
                "unowned or v1 single-page data retained: {}",
                object.path().display()
            );
        }
    }
    keys
}

fn uploaded_hashes(drive: &FakeDrive) -> BTreeSet<String> {
    let state = drive.state.lock().unwrap();
    let mut hashes = BTreeSet::new();
    for session in state.sessions.values() {
        assert!(session.completed);
        assert!(
            hashes.insert(binary_alpha_engine::hex(&Sha256::digest(&session.received))),
            "unchanged bytes uploaded twice"
        );
    }
    hashes
}

#[test]
fn empty_store_add_update_archive_pull_and_whole_job_retire() {
    let f = new_jobs("new_instrument_lifecycle", 100, None);
    let store = f.scratch.path("producer/store");
    let mut first_days = BTreeMap::new();
    let mut latest = Vec::new();
    for cutoff in [DAY2 + 100, DAY2 + 600] {
        let before = uploaded_hashes(&f.drive);
        let report = pipeline(
            "update",
            &f.config,
            &["--end", &time_text(cutoff * 1_000_000)],
        )
        .unwrap();
        daily_only(&store);
        let after = uploaded_hashes(&f.drive);
        assert!(after.len() > before.len());
        latest.clear();
        for job in &f.jobs {
            let line = job_line(&report, &job.id);
            let m = dataset(&store, field(line, "dataset"));
            assert_eq!(
                m.coverage.first_event_time,
                time_text((DAY2 - 600) * 1_000_000)
            );
            let stream =
                read_json(&store.join(format!("manifests/{}/ready.json", field(line, "stream"))));
            let mut days: BTreeMap<String, String> = m
                .objects
                .iter()
                .filter(|o| o.path.ends_with(".parquet"))
                .map(|o| (o.path.clone(), o.key.clone()))
                .collect();
            for o in stream["objects"]
                .as_array()
                .unwrap()
                .iter()
                .filter(|o| o["path"].as_str().unwrap().ends_with(".parquet"))
            {
                days.insert(
                    o["path"].as_str().unwrap().into(),
                    o["key"].as_str().unwrap().into(),
                );
            }
            if cutoff == DAY2 + 100 {
                first_days.insert(job.id.clone(), days);
            } else {
                for (path, key) in &first_days[&job.id] {
                    if path.ends_with("2025-08-11.parquet") {
                        assert_eq!(
                            days.get(path),
                            Some(key),
                            "unchanged day {path} must reuse its key"
                        );
                        assert!(!after.difference(&before).any(|hash| key.ends_with(hash)));
                    }
                }
                assert!(
                    days.iter()
                        .any(|(path, key)| first_days[&job.id].get(path) != Some(key))
                );
            }
            let bytes = f.drive.state.lock().unwrap().files[field(line, "catalog")]
                .bytes
                .clone();
            latest.push(Catalog::from_json(&bytes).unwrap());
        }
    }
    let before = uploaded_hashes(&f.drive);
    pipeline("archive", &f.config, &[]).unwrap();
    assert_eq!(before, uploaded_hashes(&f.drive));
    let consumer = f.scratch.path("consumer.toml");
    fs::write(
        &consumer,
        pipeline_toml(&f.scratch.path("consumer"), &f.drive.base, &[], None, 3),
    )
    .unwrap();
    for catalog in &latest {
        assert!(!catalog.records.is_empty());
        pipeline(
            "pull",
            &consumer,
            &[
                "--broker",
                &catalog.broker,
                "--symbol",
                &catalog.provider_symbol,
            ],
        )
        .unwrap();
        for record in &catalog.records {
            assert_eq!(
                fs::read(f.scratch.path("producer/pipeline_state").join(&record.key)).unwrap(),
                fs::read(f.scratch.path("consumer/pipeline_state").join(&record.key)).unwrap()
            );
        }
    }
    daily_only(&f.scratch.path("consumer/store"));
    let job = &f.jobs[0].id;
    assert!(
        pipeline("remove-job", &f.config, &["--job", job])
            .unwrap_err()
            .contains("completed")
    );
    let report = pipeline(
        "retire",
        &f.config,
        &["--job", job, "--whole-job", "--plan"],
    )
    .unwrap();
    let path = report
        .lines()
        .find(|s| s.starts_with("retirement plan "))
        .unwrap()
        .split_whitespace()
        .nth(2)
        .unwrap();
    let plan: Plan = serde_json::from_slice(&fs::read(path).unwrap()).unwrap();
    assert!(plan.whole_job);
    assert!(plan.totals.manifest_directories >= 4);
    assert!(plan.references.iter().any(|r| matches!(
        r.status,
        binary_alpha_app::retire::Status::Retired
    ) && r.source.contains("/records/")));
    pipeline(
        "retire",
        &f.config,
        &["--job", job, "--whole-job", "--apply", path],
    )
    .unwrap();
    for item in &plan.delete_local {
        for key in item.files.keys() {
            assert!(!store.join(key).exists(), "{key}");
        }
    }
    for item in &plan.delete_drive {
        assert!(
            !f.drive
                .state
                .lock()
                .unwrap()
                .files
                .contains_key(&item.file_id)
        );
    }
    for id in binary_alpha_app::store::Store::filesystem(&store)
        .list_manifests()
        .unwrap()
    {
        assert_eq!(
            read_json(&store.join(format!("manifests/{id}/ready.json")))["instrument"],
            "pocket_option:AEDCNY_otc"
        );
    }
    for file in f.drive.state.lock().unwrap().files.values() {
        if let Ok(catalog) = Catalog::from_json(&file.bytes) {
            assert_ne!(catalog.job, *job);
        }
    }
    assert!(f.scratch.path("producer/pipeline_state/records").is_dir());
    pipeline("remove-job", &f.config, &["--job", job]).unwrap();
    pipeline("remove-job", &f.config, &["--job", job]).unwrap();
    assert_eq!(
        data_pipeline::PipelineConfig::parse(&fs::read_to_string(&f.config).unwrap())
            .unwrap()
            .jobs,
        vec![f.jobs[1].clone()]
    );
    daily_only(&store);
    // Preserved registry/receipt evidence from the first removal must not pin its deleted
    // objects when another instrument is retired later.
    let second = &f.jobs[1].id;
    let report = pipeline(
        "retire",
        &f.config,
        &["--job", second, "--whole-job", "--plan"],
    )
    .unwrap();
    let path = report.split_whitespace().nth(2).unwrap();
    pipeline("retire", &f.config, &["--apply", path]).unwrap();
    pipeline("remove-job", &f.config, &["--job", second]).unwrap();
    assert!(
        binary_alpha_app::store::Store::filesystem(&store)
            .list_manifests()
            .unwrap()
            .is_empty()
    );
    assert!(
        fs::read_dir(store.join("objects"))
            .unwrap()
            .next()
            .is_none()
    );
    assert!(
        f.drive
            .state
            .lock()
            .unwrap()
            .files
            .values()
            .all(|e| e.name.starts_with("record-"))
    );
    assert!(f.deriv.forbidden().is_empty());
    assert!(f._pocket.forbidden().is_empty());
}

#[test]
fn interrupted_empty_store_keeps_original_seed_binding() {
    let f = new_jobs("new_instrument_resume", 1, None);
    let cutoff = time_text((DAY2 + 100) * 1_000_000);
    let mut completed = None;
    for _ in 0..10 {
        match pipeline("update", &f.config, &["--end", &cutoff]) {
            Ok(report) => {
                completed = Some(report);
                break;
            }
            Err(reason) => {
                assert!(reason.contains("status pending"), "{reason}");
                assert!(
                    !reason.contains("configuration") || !reason.contains("conflicts"),
                    "{reason}"
                );
            }
        }
    }
    let report = completed.expect("bounded first acquisition eventually completes");
    let store = f.scratch.path("producer/store");
    daily_only(&store);
    for job in &f.jobs {
        let manifest = dataset(&store, field(job_line(&report, &job.id), "dataset"));
        assert_eq!(
            manifest.coverage.first_event_time,
            time_text((DAY2 - 600) * 1_000_000)
        );
        if manifest.broker.as_str() == "deriv" {
            assert_eq!(
                ticks(&store, &manifest),
                deriv_ticks(DAY2 - 600, DAY2 + 100)
                    .into_iter()
                    .map(|(time, price)| Tick {
                        event_time_micros: time * 1_000_000,
                        price_units: units5(&price)
                    })
                    .collect::<Vec<_>>()
            );
        } else {
            assert_eq!(
                bars(&store, &manifest),
                (DAY2 - 600..DAY2 + 100)
                    .step_by(5)
                    .map(|start| {
                        let [open, high, low, close, volume] = synthetic_bar(start);
                        Bar {
                            provider: (),
                            start_unix_s: start,
                            open,
                            high,
                            low,
                            close,
                            volume,
                            period_s: 5,
                        }
                    })
                    .collect::<Vec<_>>()
            );
        }
    }
}

#[test]
fn registration_recovers_from_checkpoint_without_rediscovery() {
    let f = new_jobs("new_instrument_registration_resume", 100, None);
    let job = &f.jobs[1];
    let core = fs::read(f.scratch.path(job.config.to_str().unwrap())).unwrap();
    let evidence_path = f.scratch.path(job.evidence.to_str().unwrap());
    let evidence = fs::read(&evidence_path).unwrap();
    // Simulate death after checkpoint/core publication and before evidence/document publication.
    let mut document =
        data_pipeline::PipelineConfig::parse(&fs::read_to_string(&f.config).unwrap()).unwrap();
    document.jobs.pop();
    fs::write(&f.config, toml::to_string(&document).unwrap()).unwrap();
    fs::remove_file(&evidence_path).unwrap();
    f._pocket.set(BrokerFaults {
        reject_initial_session: true,
        ..BrokerFaults::default()
    });
    let requests = f._pocket.requests().len();
    pipeline(
        "add-job",
        &f.config,
        &[
            "--broker",
            "pocket_option",
            "--symbol",
            "AEDCNY_otc",
            "--template",
            f.scratch
                .path("pocket_option-template.toml")
                .to_str()
                .unwrap(),
            "--session",
            f.scratch.path("always.toml").to_str().unwrap(),
        ],
    )
    .unwrap();
    assert_eq!(f._pocket.requests().len(), requests);
    assert_eq!(f._pocket.rejected_auths.load(Ordering::SeqCst), 0);
    assert_eq!(
        fs::read(f.scratch.path(job.config.to_str().unwrap())).unwrap(),
        core
    );
    assert_eq!(fs::read(evidence_path).unwrap(), evidence);
    assert_eq!(
        data_pipeline::PipelineConfig::parse(&fs::read_to_string(&f.config).unwrap())
            .unwrap()
            .jobs,
        f.jobs
    );
}

#[test]
fn pocket_registration_measures_eleven_digit_prices_and_updates_without_rounding() {
    let f = new_jobs("new_instrument_eleven_digits", 100, Some("0.00002412345"));
    let report = pipeline(
        "update",
        &f.config,
        &["--end", &time_text((DAY2 + 100) * 1_000_000)],
    )
    .unwrap();
    let store = f.scratch.path("producer/store");
    let manifest = dataset(&store, field(job_line(&report, &f.jobs[1].id), "dataset"));
    assert!(
        bars(&store, &manifest)
            .iter()
            .all(|bar| bar.close == 0.00002412345)
    );
    daily_only(&store);
}

#[test]
fn invalid_scale_is_rejected_before_configuration_or_broker_access() {
    let error = run(&[
        "data",
        "pipeline",
        "add-job",
        "--config",
        "absent-fixture.toml",
        "--template",
        "absent-template.toml",
        "--broker",
        "deriv",
        "--symbol",
        "frxEURUSD",
        "--price-scale",
        "19",
    ])
    .unwrap_err();
    assert!(
        error.contains("price_scale") && error.contains("18"),
        "{error}"
    );
}

#[test]
fn price_scale_failure_reports_required_digits() {
    let price: binary_alpha_app::broker::wire::WireDecimal =
        serde_json::from_str("0.00002412345").unwrap();
    let error = price.price_units(6.try_into().unwrap()).unwrap_err();
    assert!(
        error.contains("11 fraction digits") && error.contains("price_scale 6"),
        "{error}"
    );
    assert_eq!(
        price.price_units(11.try_into().unwrap()).unwrap(),
        2_412_345
    );
}

#[test]
#[ignore = "WT-SESSIONS integration: enable after the singular session field and calendar owner merge"]
fn native_session_contract() {
    let core = deriv_core("ws://127.0.0.1:1/", 60, 6000, 3600);
    let mut doc: toml::Value = toml::from_str(&core).unwrap();
    doc["instruments"][0].as_table_mut().unwrap().insert("session".into(), toml::from_str(
        "kind='weekly'\ntimezone='America/New_York'\nopen={day='sunday',time='17:00:00'}\nclose={day='friday',time='17:00:00'}\nclosed_dates=['2026-12-25']\nearly_closes=[{date='2026-11-27',time='13:00:00'}]"
    ).unwrap());
    let core = binary_alpha_engine::config::Config::parse(&toml::to_string(&doc).unwrap()).unwrap();
    let roundtrip: toml::Value = toml::from_str(&core.canonical_toml()).unwrap();
    assert_eq!(
        roundtrip["instruments"][0]["session"],
        doc["instruments"][0]["session"]
    );
}

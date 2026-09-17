//! Daily lineage integration and explicit legacy roots for the pre-existing v1 regression set.
use super::*;
use binary_alpha_app::{daily, store::Store};
use binary_alpha_engine::dataset::{
    self as ds, DayFamily, Layout, ObjectRole, PriceRepresentation,
};
use std::collections::BTreeSet;

/// Keep the original bundle/legacy-seed regression cases on explicit v1 fixtures. The command
/// under test now imports daily roots; new tests below exercise that real output directly.
/// This helper rebuilds an old root from its synthetic source inventory, never production data.
pub(super) fn legacy_import(config: &Path) -> Result<String, String> {
    let report = run(&["data", "import", "--config", config.to_str().unwrap()])?;
    let core =
        binary_alpha_engine::config::Config::parse(&fs::read_to_string(config).unwrap()).unwrap();
    let root = PathBuf::from(core.storage.historical_data_dir.as_path());
    let mut result = report.clone();
    for line in report.lines().filter(|l| l.starts_with("published ")) {
        let generation = field(line, "generation");
        let mut m = dataset(&root, generation);
        if m.layout.is_none() {
            continue;
        }
        let lineage = m
            .objects
            .iter()
            .find(|o| o.path == "provenance/lineage.json")
            .unwrap();
        let lineage = read_json(&root.join(&lineage.key));
        let mut original = Vec::new();
        for value in lineage["original_objects"].as_array().unwrap() {
            let object: ds::ObjectRecord = serde_json::from_value(value["object"].clone()).unwrap();
            let source = m.inputs.iter().find(|i| i.sha256 == object.sha256).unwrap();
            fs::copy(&source.path, root.join(&object.key)).unwrap();
            original.push(object);
        }
        if let PriceRepresentation::IntegerUnits { scale } = m.price_representation {
            let rows = legacy_tick_rows(&m);
            let tmp = root.join("legacy-ticks.parquet");
            binary_alpha_app::archive::write_ticks(
                &tmp,
                &binary_alpha_engine::market::InstrumentId {
                    broker: m.broker.clone(),
                    provider_symbol: m.provider_symbol.clone(),
                },
                scale,
                rows.into_iter().map(Ok),
            )
            .unwrap();
            original.push(common::daily::object(
                &root,
                "normalized/ticks.parquet",
                ObjectRole::Normalized,
                &tmp,
            ));
            fs::remove_file(tmp).unwrap();
        }
        m.objects = original;
        m.layout = None;
        m.day_inventory.clear();
        let id = binary_alpha_engine::market::InstrumentId {
            broker: m.broker.clone(),
            provider_symbol: m.provider_symbol.clone(),
        };
        let scale = match m.price_representation {
            PriceRepresentation::IntegerUnits { scale } => Some(scale),
            _ => None,
        };
        m.generation = ds::generation_id(&id, m.source_kind, m.role, scale, &m.objects);
        let old = root.join(m.key());
        let existed = old.exists();
        fs::create_dir_all(old.parent().unwrap()).unwrap();
        fs::write(old, m.to_json()).unwrap();
        fs::remove_file(root.join(ds::manifest_key(generation))).unwrap();
        fs::remove_dir(root.join(ds::manifest_key(generation)).parent().unwrap()).unwrap();
        result = result.replace(generation, &m.generation);
        if existed && !result.contains("(already published)") {
            result = format!("{} (already published)\n", result.trim_end());
        }
    }
    Ok(result)
}

fn legacy_tick_rows(manifest: &GenerationManifest) -> Vec<Tick> {
    let PriceRepresentation::IntegerUnits { scale } = manifest.price_representation else {
        panic!("expected tick fixture");
    };
    let mut sources: Vec<_> = manifest
        .inputs
        .iter()
        .filter(|i| i.path.ends_with("_ticks.parquet"))
        .collect();
    sources.sort_by_key(|i| &i.path);
    assert!(
        !sources.is_empty(),
        "legacy oracle requires original daily source files"
    );
    sources
        .into_iter()
        .flat_map(|input| {
            let name = Path::new(&input.path)
                .file_name()
                .unwrap()
                .to_str()
                .unwrap();
            let date = &name[name.len() - "YYYY-MM-DD_ticks.parquet".len()..][..10];
            binary_alpha_app::archive::read_daily_ticks(
                Path::new(&input.path),
                scale,
                ds::daily::day_bounds(date).unwrap().0,
            )
            .unwrap()
        })
        .collect()
}

fn assert_exact_reclamation(
    store: &Path,
    inventory: &BTreeSet<String>,
    expected: &BTreeSet<String>,
) {
    let deleted: BTreeSet<_> = inventory
        .iter()
        .filter(|key| !store.join(key).exists())
        .cloned()
        .collect();
    assert_eq!(
        &deleted, expected,
        "every eligible single and no protected object must be reclaimed"
    );
}

#[test]
fn reclamation_oracle_rejects_any_retained_eligible_single() {
    let scratch = Scratch::new("reclamation_oracle");
    let keys = BTreeSet::from(["deleted".to_string(), "accidentally-retained".to_string()]);
    fs::write(scratch.path("accidentally-retained"), b"single page").unwrap();
    assert!(
        std::panic::catch_unwind(|| assert_exact_reclamation(&scratch.path(""), &keys, &keys))
            .is_err(),
        "the deletion oracle must reject retention of any eligible key"
    );
}

#[test]
fn legacy_tick_oracle_ignores_corrupted_daily_output() {
    let f = fixture("legacy_tick_oracle");
    let report = run(&[
        "data",
        "import",
        "--config",
        f.scratch.path("deriv-import.toml").to_str().unwrap(),
    ])
    .unwrap();
    let store = f.scratch.path("producer/store");
    let mut manifest = dataset(&store, imported_generation(&report, "deriv:frxEURUSD"));
    let instrument = binary_alpha_engine::market::InstrumentId {
        broker: manifest.broker.clone(),
        provider_symbol: manifest.provider_symbol.clone(),
    };
    for day in manifest
        .day_inventory
        .iter_mut()
        .filter(|d| d.family == DayFamily::Observations)
    {
        let Some(key) = &day.object else {
            continue;
        };
        let scale = 5.try_into().unwrap();
        let mut rows = daily::read_ticks(&store.join(key), &day.date, &instrument, scale).unwrap();
        for row in &mut rows {
            row.price_units += 1;
        }
        let path = f.scratch.path("corrupted-daily.parquet");
        daily::write_ticks(&path, &day.date, &instrument, scale, [rows]).unwrap();
        let object = common::daily::object(
            &store,
            &day.logical_path().unwrap(),
            ObjectRole::Normalized,
            &path,
        );
        manifest.objects.retain(|o| &o.key != key);
        day.object = Some(object.key.clone());
        manifest.objects.push(object);
    }
    let expected = expected_ticks(DERIV_SEED_END, DERIV_SEED_END, DERIV_SEED_END);
    assert_ne!(
        ticks(&store, &manifest),
        expected,
        "fault injection must change the v2 prices"
    );
    assert_eq!(
        legacy_tick_rows(&manifest),
        expected,
        "legacy rows must come independently from the source fixture"
    );
}

fn sparse_pocket(times: Vec<i64>) -> FakeBroker {
    use futures_util::{SinkExt, StreamExt};
    use tokio_tungstenite::tungstenite::Message;
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let url = format!(
        "ws://{}/socket.io/?EIO=4&transport=websocket",
        listener.local_addr().unwrap()
    );
    let stop = Arc::new(AtomicBool::new(false));
    let requests = Arc::new(Mutex::new(Vec::new()));
    let stop_ = stop.clone();
    let requests_ = requests.clone();
    let thread = std::thread::spawn(move || {
        runtime().block_on(async move {
        let listener = tokio::net::TcpListener::from_std(listener).unwrap();
        while !stop_.load(Ordering::SeqCst) {
            let Ok(Ok((stream,_))) = tokio::time::timeout(Duration::from_millis(100),listener.accept()).await else { continue; };
            let Ok(mut socket) = tokio_tungstenite::accept_async(stream).await else { continue; };
            if socket.send(Message::Text(r#"0{"synthetic":true,"pingInterval":25000,"pingTimeout":20000}"#.into())).await.is_err() { continue; }
            while let Ok(Some(Ok(message))) = tokio::time::timeout(Duration::from_secs(5),socket.next()).await {
                let Message::Text(text) = message else { break; };
                if text == "40" { if socket.send(Message::Text(r#"40{"synthetic":true}"#.into())).await.is_err() { break; } continue; }
                if text == "2" || text == "3" { continue; }
                let socket_io::Packet::Event { name,argument } = socket_io::decode(&text).unwrap() else { panic!("{text}") };
                let replies = match name.as_str() {
                    "auth" => {
                        let mut row = vec![Value::Null;19]; row[0]=json!("synthetic");row[1]=json!("AEDCNY_otc");
                        let mut r = vec![Message::Text(r#"42["successauth",{"synthetic":true}]"#.into()),Message::Text(r#"42["successupdateBalance",{"isDemo":1,"synthetic":true}]"#.into())];
                        r.extend(attachment("updateAssets",json!([row]).to_string()));r
                    },
                    "loadHistoryPeriod" => {
                        let request: Value = serde_json::from_slice(&argument).unwrap();
                        requests_.lock().unwrap().push(String::from_utf8(argument).unwrap());
                        let anchor = request["time"].as_i64().unwrap()-POCKET_OFFSET_S;
                        let end = times.partition_point(|t| *t <= anchor);
                        let rows: Vec<_> = times[end.saturating_sub(20)..end].iter().map(|&t| {
                            let [open,high,low,close,volume]=synthetic_bar(t);
                            json!({"symbol_id":POCKET_SYMBOL_ID,"time":t+POCKET_OFFSET_S,"open":open,"high":high,"low":low,"close":close,"volume":volume})
                        }).collect();
                        attachment("loadHistoryPeriodFast",json!({"asset":"AEDCNY_otc","index":request["index"],"period":5,"data":rows}).to_string())
                    }, other => panic!("unexpected request {other}"),
                };
                continue_send(&mut socket,replies).await;
            }
        }
    })
    });
    FakeBroker {
        url,
        requests,
        forbidden: Arc::new(Mutex::new(Vec::new())),
        faults: Arc::new(Mutex::new(BrokerFaults::default())),
        rejected_auths: Arc::new(AtomicUsize::new(0)),
        stop,
        thread: Some(thread),
    }
}

fn all_pages(store: &Path, m: &GenerationManifest) -> Vec<daily::PageOccurrence> {
    let mut rows = Vec::new();
    for d in m
        .day_inventory
        .iter()
        .filter(|d| d.family == DayFamily::Pages)
    {
        if let Some(key) = &d.object {
            rows.extend(daily::read_pages(&store.join(key), &d.date).unwrap());
        }
    }
    rows
}
fn keys(m: &GenerationManifest, prefix: &str) -> BTreeMap<String, String> {
    m.objects
        .iter()
        .filter(|o| o.path.starts_with(prefix))
        .map(|o| (o.path.clone(), o.key.clone()))
        .collect()
}
fn rewrite_rows(
    scratch: &Scratch,
    m: &mut GenerationManifest,
    ticks: &[Tick],
    bars: &[binary_alpha_engine::market::Bar<binary_alpha_engine::market::BarProviderColumns>],
) {
    let root = scratch.path("published");
    let empty_keys = m
        .day_inventory
        .iter()
        .filter(|d| d.family == DayFamily::Observations && d.rows == 0)
        .filter_map(|d| d.object.as_ref())
        .cloned()
        .collect::<std::collections::BTreeSet<_>>();
    m.objects
        .retain(|o| !o.path.starts_with("observations/") || empty_keys.contains(&o.key));
    m.day_inventory
        .retain(|d| d.family != DayFamily::Observations || d.rows == 0);
    let times: Vec<_> = if bars.is_empty() {
        ticks.iter().map(|t| t.event_time_micros).collect()
    } else {
        bars.iter().map(|b| b.start_unix_s * 1_000_000).collect()
    };
    let dates: std::collections::BTreeSet<_> =
        times.iter().map(|&t| common::daily::date(t)).collect();
    let id = binary_alpha_engine::market::InstrumentId {
        broker: m.broker.clone(),
        provider_symbol: m.provider_symbol.clone(),
    };
    for date in dates {
        let (start, end) = ds::daily::day_bounds(&date).unwrap();
        let tmp = scratch.path("rows.parquet");
        let data = if bars.is_empty() {
            daily::write_ticks(
                &tmp,
                &date,
                &id,
                match m.price_representation {
                    PriceRepresentation::IntegerUnits { scale } => scale,
                    _ => unreachable!(),
                },
                [ticks
                    .iter()
                    .copied()
                    .filter(|t| (start..end).contains(&t.event_time_micros))
                    .collect::<Vec<_>>()],
            )
            .unwrap()
        } else {
            daily::write_bars(
                &tmp,
                &date,
                [bars
                    .iter()
                    .filter(|b| (start..end).contains(&(b.start_unix_s * 1_000_000)))
                    .cloned()
                    .collect::<Vec<_>>()],
            )
            .unwrap()
        };
        let object = common::daily::object(
            &root,
            &format!("observations/{date}.parquet"),
            ObjectRole::Normalized,
            &tmp,
        );
        m.day_inventory.push(ds::DayInventoryEntry {
            date,
            family: DayFamily::Observations,
            duration: None,
            offset: None,
            object: Some(object.key.clone()),
            rows: data.rows,
            first_time: data.first_event_micros.map(time_text),
            last_time: data.last_event_micros.map(time_text),
            state: ds::DayState::Unknown,
            reason: Some("synthetic source with historical gaps".into()),
            unresolved: vec![],
        });
        m.objects.push(object);
    }
    m.day_inventory
        .sort_by(|a, b| (a.family, &a.date).cmp(&(b.family, &b.date)));
    m.coverage.first_event_time = time_text(times[0]);
    m.coverage.last_event_time = time_text(*times.last().unwrap());
    m.row_count = times.len() as u64;
    let coverage = scratch.path("rewritten-coverage.json");
    fs::write(
        &coverage,
        serde_json::to_vec(&json!({"fixture":true,"rows":m.row_count,"actual":m.coverage}))
            .unwrap(),
    )
    .unwrap();
    m.objects.retain(|o| o.path != "provenance/coverage.json");
    m.objects.push(common::daily::object(
        &root,
        "provenance/coverage.json",
        ObjectRole::Provenance,
        &coverage,
    ));
}
fn audit_snapshot(scratch: &Scratch, config: &Path, m: &GenerationManifest) -> StreamManifest {
    let mut report = Vec::new();
    binary_alpha_app::audit::run(
        config,
        &format!(
            "file://{}",
            scratch.path("published").join(m.key()).display()
        ),
        &mut report,
    )
    .unwrap();
    let text = String::from_utf8(report).unwrap();
    stream(&scratch.path("published"), field(&text, "generation"))
}
fn normalized_profile(store: &Path, m: &StreamManifest) -> Value {
    let o = m.objects.iter().find(|o| o.path == "profile.json").unwrap();
    let mut value = read_json(&store.join(&o.key));
    value["source"]["generation"] = Value::Null;
    for calculation in value["calculations"].as_array_mut().unwrap() {
        if let Some(reason) = calculation["reason"].as_str() {
            let mut reason: Value = serde_json::from_str(reason).unwrap();
            assert_eq!(reason["generation"], m.source_generation);
            reason["generation"] = Value::Null;
            calculation["reason"] = serde_json::to_value(reason).unwrap();
        }
    }
    value
}

#[test]
fn daily_updates_preserve_days_occurrences_resume_and_reclaim_for_both_brokers() {
    for pocket in [false, true] {
        let scratch = Scratch::new(if pocket {
            "daily_pocket_lineage"
        } else {
            "daily_deriv_lineage"
        });
        let mut pair = common::daily::pair(&scratch, pocket);
        let monday = common::daily::micros(common::daily::LAST);
        let tuesday = common::daily::micros("2026-09-22");
        if pocket {
            pair.bars.retain(|b| {
                b.start_unix_s * 1_000_000
                    != common::daily::micros(common::daily::SECOND) + 115_000_000
            });
            for b in &mut pair.bars {
                let [open, high, low, close, volume] = synthetic_bar(b.start_unix_s);
                b.open = open;
                b.high = high;
                b.low = low;
                b.close = close;
                b.volume = volume;
            }
            let mut extra = pair
                .bars
                .iter()
                .filter(|b| b.start_unix_s * 1_000_000 >= monday)
                .cloned()
                .collect::<Vec<_>>();
            for b in &mut extra {
                b.start_unix_s += 86400;
                b.provider.timestamp_utc += ds::daily::DAY_MICROS;
                b.provider.server_time_s += 86400;
                let [open, high, low, close, volume] = synthetic_bar(b.start_unix_s);
                b.open = open;
                b.high = high;
                b.low = low;
                b.close = close;
                b.volume = volume;
            }
            pair.bars.extend(extra);
        } else {
            pair.ticks.extend(
                (tuesday..tuesday + 132_000_000)
                    .step_by(1_000_000)
                    .map(|t| Tick {
                        event_time_micros: t,
                        price_units: 18000 + (t / 1_000_000).rem_euclid(47),
                    }),
            );
            for tick in &mut pair.ticks {
                tick.price_units *= 10;
            }
            pair.v2.price_representation = PriceRepresentation::IntegerUnits {
                scale: 5.try_into().unwrap(),
            };
        }
        let broker = if pocket {
            sparse_pocket(pair.bars.iter().map(|b| b.start_unix_s).collect())
        } else {
            serve_broker(Kind::Deriv(Arc::new(
                pair.ticks
                    .iter()
                    .map(|t| {
                        (
                            t.event_time_micros / 1_000_000,
                            format!("{:.5}", t.price_units as f64 / 100000.),
                        )
                    })
                    .collect(),
            )))
        };
        let drive = serve_drive();
        let mut core = if pocket {
            pocket_core(&broker.url, "demo", BAR_GRANULARITY, 60, 1, 60)
                .replace("price_scale = 6", "price_scale = 4")
                .replace(
                    &format!("history_pages_in_flight = {POCKET_HISTORY_PAGES_IN_FLIGHT}"),
                    "history_pages_in_flight = 1",
                )
        } else {
            deriv_core(&broker.url, 60, 1, 60).replace("frxEURUSD", "R_50")
        };
        core = core
            .replace(
                if pocket {
                    "2025-05-19T11:15:00Z"
                } else {
                    "2025-08-11T00:00:00Z"
                },
                "2026-09-17T00:00:00Z",
            )
            .replace(
                if pocket {
                    "2025-05-20T00:00:00Z"
                } else {
                    "2025-08-13T00:00:00Z"
                },
                "2026-09-23T00:00:00Z",
            );
        let root = scratch.path("published");
        let seed_ticks = pair
            .ticks
            .iter()
            .copied()
            .filter(|t| t.event_time_micros < monday)
            .collect::<Vec<_>>();
        let seed_bars = pair
            .bars
            .iter()
            .filter(|b| b.start_unix_s * 1_000_000 < monday)
            .cloned()
            .collect::<Vec<_>>();
        let original_key = pair.v2.key();
        rewrite_rows(&scratch, &mut pair.v2, &seed_ticks, &seed_bars);
        // Last source day is partial, so a Monday observation finalizes its withheld candle.
        let last = pair
            .v2
            .day_inventory
            .iter_mut()
            .rfind(|d| d.family == DayFamily::Observations && d.rows > 0)
            .unwrap();
        last.state = ds::DayState::Partial;
        last.reason = Some("fixture cutoff".into());
        last.unresolved = vec![ds::UnresolvedInterval {
            start: time_text(
                binary_alpha_engine::market::parse_event_time_micros(
                    last.last_time.as_ref().unwrap(),
                )
                .unwrap()
                    + 1,
            ),
            end: time_text(ds::daily::day_bounds(&last.date).unwrap().1),
        }];
        // A source-evidenced empty day without a completeness claim still owns a day file.
        let empty_path = scratch.path("unknown-empty.parquet");
        let empty_date = "2026-09-19";
        if pocket {
            daily::write_bars(&empty_path, empty_date, [Vec::<daily::DailyBar>::new()]).unwrap();
        } else {
            daily::write_ticks(
                &empty_path,
                empty_date,
                &binary_alpha_engine::market::InstrumentId {
                    broker: pair.v2.broker.clone(),
                    provider_symbol: pair.v2.provider_symbol.clone(),
                },
                5.try_into().unwrap(),
                [Vec::<Tick>::new()],
            )
            .unwrap();
        }
        let empty_object = common::daily::object(
            &root,
            &format!("observations/{empty_date}.parquet"),
            ObjectRole::Normalized,
            &empty_path,
        );
        let empty_day = pair
            .v2
            .day_inventory
            .iter_mut()
            .find(|d| d.family == DayFamily::Observations && d.date == empty_date)
            .unwrap();
        empty_day.object = Some(empty_object.key.clone());
        empty_day.state = ds::DayState::Unknown;
        empty_day.reason = Some("empty source without completeness evidence".into());
        pair.v2.objects.push(empty_object);
        if !pocket {
            // A migration-style root keeps its old history coverage but binds future seeds to
            // itself. Its legacy parent need not remain readable after continuation begins.
            let parsed = binary_alpha_engine::config::Config::parse(&core).unwrap();
            let source = binary_alpha_app::broker::source_identity(&parsed.brokers[0]);
            let proof = scratch.path("migration-root-coverage.json");
            fs::write(&proof, json!({
                "schema_version":1,"source_identity":source,"broker":"deriv","provider_symbol":"R_50","role":"development",
                "requested":{"start":"2026-09-17T00:00:00Z","end":"2026-09-19T00:00:00Z"},
                "verified":{"start":"2026-09-18T00:00:00Z","end":time_text(seed_ticks.last().unwrap().event_time_micros + 1)},
                "actual":{"first":pair.v2.coverage.first_event_time,"last":pair.v2.coverage.last_event_time},"rows":pair.v2.row_count,
                "shortfall":{"reason":"historical_gap","unresolved":{"start":"2026-09-17T00:00:00Z","end":"2026-09-18T00:00:00Z"}},
                "seed":{"generation":pair.v1.generation,"source_identity":source}
            }).to_string()).unwrap();
            pair.v2
                .objects
                .retain(|o| o.path != "provenance/coverage.json");
            pair.v2.objects.push(common::daily::object(
                &root,
                "provenance/coverage.json",
                ObjectRole::Provenance,
                &proof,
            ));
            fs::write(&proof, json!({"schema_version":1,"parent_generation":pair.v1.generation,"replaced_generations":[pair.v1.generation]}).to_string()).unwrap();
            pair.v2.objects.push(common::daily::object(
                &root,
                "provenance/lineage.json",
                ObjectRole::Provenance,
                &proof,
            ));
            pair.v2.source_kind = ds::SourceKind::BrokerHistory;
        }
        common::daily::publish(&root, &mut pair.v2);
        fs::remove_file(root.join(original_key)).unwrap();
        // Place the managed store beneath its normal pipeline root; codecs created it directly.
        fs::create_dir_all(scratch.path("producer")).unwrap();
        fs::rename(&root, scratch.path("producer/store")).unwrap();
        let store = scratch.path("producer/store");
        fs::create_dir_all(scratch.path("evidence")).unwrap();
        write_evidence(&scratch, "daily", &core);
        fs::write(scratch.path("daily.toml"), &core).unwrap();
        let config = scratch.path("daily-pipeline.toml");
        fs::write(
            &config,
            pipeline_toml(
                &scratch.path("producer"),
                &drive.base,
                &[("daily", "daily.toml")],
                None,
                1,
            ),
        )
        .unwrap();
        let audit_config = scratch.path("audit.toml");
        fs::write(
            &audit_config,
            core.replace(
                "historical_data_dir = \"unused\"",
                &format!("historical_data_dir = \"{}\"", store.display()),
            )
            .replace(
                "publication_uri = \"file:///unused\"",
                &format!("publication_uri = \"file://{}\"", store.display()),
            ),
        )
        .unwrap();
        let mut output = Vec::new();
        binary_alpha_app::audit::run(
            &audit_config,
            &format!("file://{}", store.join(pair.v2.key()).display()),
            &mut output,
        )
        .unwrap();
        let root_stream = stream(
            &store,
            field(&String::from_utf8(output).unwrap(), "generation"),
        );
        let cutoff1 = monday + 120_000_000;
        let pending = pipeline("update", &config, &["--end", &time_text(cutoff1)]).unwrap_err();
        assert!(pending.contains("status pending"), "{pending}");
        let progress = read_progress(&scratch.path("producer/pipeline_state/daily/progress.json"));
        assert_eq!(progress["progress"]["pages"].as_array().unwrap().len(), 1);
        let first_hash = progress["progress"]["pages"][0]["sha256"]
            .as_str()
            .unwrap()
            .to_string();
        assert!(store.join(ds::object_key(&first_hash)).exists());
        let first_occurrence = progress["progress"]["pages"][0]["occurrence"].clone();
        let foreign_state = scratch.path("producer/pipeline_state/another-job");
        fs::create_dir_all(&foreign_state).unwrap();
        fs::write(
            foreign_state.join("progress.json"),
            json!({"page":{"sha256":first_hash}}).to_string(),
        )
        .unwrap();
        let before_close = std::fs::read_dir(store.join("objects"))
            .unwrap()
            .map(|p| p.unwrap().path())
            .collect::<Vec<_>>();
        fs::write(
            scratch.path("daily.toml"),
            core.replace("max_pages = 1", "max_pages = 50"),
        )
        .unwrap();
        // Daily selection survives removal of EVERY legacy ready manifest, even mid-acquisition.
        for generation in Store::filesystem(&store).list_manifests().unwrap() {
            let path = store.join(ds::manifest_key(&generation));
            let v = read_json(&path);
            if v.get("kind").is_none() && v.get("layout").is_none() {
                fs::remove_file(path).unwrap();
            }
        }
        if !pocket {
            broker.set(BrokerFaults {
                conflict_before: Some(cutoff1 / 1_000_000),
                ..Default::default()
            });
            let rejected =
                pipeline("update", &config, &["--end", &time_text(cutoff1)]).unwrap_err();
            assert!(
                rejected.contains("conflicting or inconsistent reread"),
                "{rejected}"
            );
            broker.set(BrokerFaults::default());
        }
        let first = pipeline("update", &config, &["--end", &time_text(cutoff1)]).unwrap();
        let first_m = dataset(&store, field(job_line(&first, "daily"), "dataset"));
        let first_stream = stream(&store, field(job_line(&first, "daily"), "stream"));
        assert_eq!(first_m.layout, Some(Layout::DailyV2));
        if !pocket {
            let coverage = read_json(
                &store.join(
                    &first_m
                        .objects
                        .iter()
                        .find(|o| o.path == "provenance/coverage.json")
                        .unwrap()
                        .key,
                ),
            );
            assert_eq!(coverage["verified"]["start"], "2026-09-18T00:00:00.000000Z");
            assert_eq!(
                coverage["prior_acquisitions"][0]["shortfall"]["reason"],
                "historical_gap"
            );
            assert!(
                all_pages(&store, &first_m)
                    .iter()
                    .any(|p| p.disposition == daily::PageDisposition::Diagnostic)
            );
        }
        assert!(
            store.join(ds::object_key(&first_hash)).exists(),
            "another pending log protects a shared single"
        );
        let state = scratch.path("producer/pipeline_state/daily");
        let plan = fs::read_dir(&state)
            .unwrap()
            .map(|p| p.unwrap().path())
            .find(|p| {
                p.file_name()
                    .unwrap()
                    .to_str()
                    .unwrap()
                    .starts_with("reclaim-")
            })
            .unwrap();
        let plan = read_json(&plan);
        assert!(
            plan["protected"]
                .as_array()
                .unwrap()
                .contains(&json!(ds::object_key(&first_hash)))
        );
        let receipt = read_json(
            &scratch
                .path("producer/pipeline_state/records")
                .join(plan["receipt"].as_str().unwrap()),
        );
        let candidates: BTreeSet<_> = receipt["requests"]
            .as_array()
            .unwrap()
            .iter()
            .map(|r| ds::object_key(r["sha256"].as_str().unwrap()))
            .collect();
        let expected: BTreeSet<_> = candidates
            .iter()
            .filter(|k| **k != ds::object_key(&first_hash))
            .cloned()
            .collect();
        assert!(
            !expected.is_empty(),
            "fixture must have unpinned singles eligible for reclamation"
        );
        let inventory = before_close
            .iter()
            .map(|p| format!("objects/{}", p.file_name().unwrap().to_str().unwrap()))
            .chain(candidates.iter().cloned())
            .collect();
        assert_exact_reclamation(&store, &inventory, &expected);
        assert_eq!(
            serde_json::from_value::<BTreeSet<String>>(plan["reclaimed"].clone()).unwrap(),
            expected
        );
        fs::remove_file(foreign_state.join("progress.json")).unwrap();
        assert_eq!(
            all_pages(&store, &first_m)
                .iter()
                .filter(|p| p.acquisition_id
                    == first_occurrence["acquisition_id"].as_str().unwrap()
                    && p.ordinal == 0)
                .count(),
            1
        );
        assert_eq!(
            keys(&pair.v2, "observations/"),
            keys(&first_m, "observations/")
                .into_iter()
                .filter(|(p, _)| !p.contains(common::daily::LAST))
                .collect()
        );
        let friday = format!("candles/10s_0s/{}.parquet", common::daily::SECOND);
        let candle_key = |s: &StreamManifest, p: &str| {
            s.objects.iter().find(|o| o.path == p).unwrap().key.clone()
        };
        assert_ne!(
            candle_key(&root_stream, &friday),
            candle_key(&first_stream, &friday),
            "weekend finalization rewrites Friday candles"
        );
        let thursday = format!("candles/10s_0s/{}.parquet", common::daily::FIRST);
        assert_eq!(
            candle_key(&root_stream, &thursday),
            candle_key(&first_stream, &thursday)
        );
        let cutoff2 = tuesday + 135_000_000;
        let second = pipeline("update", &config, &["--end", &time_text(cutoff2)]).unwrap();
        let second_m = dataset(&store, field(job_line(&second, "daily"), "dataset"));
        let second_stream = stream(&store, field(job_line(&second, "daily"), "stream"));
        assert!(
            !store.join(ds::object_key(&first_hash)).exists(),
            "reclamation resumes when a pending pin is released"
        );
        for (path, key) in keys(&pair.v2, "observations/") {
            assert_eq!(keys(&second_m, "observations/")[&path], key);
        }
        let expected_ticks = pair
            .ticks
            .iter()
            .copied()
            .filter(|t| t.event_time_micros < cutoff2)
            .collect::<Vec<_>>();
        let expected_bars = pair
            .bars
            .iter()
            .filter(|b| (b.start_unix_s + 5) * 1_000_000 <= cutoff2)
            .cloned()
            .collect::<Vec<_>>();
        if pocket {
            assert_eq!(
                bars(&store, &second_m),
                expected_bars
                    .iter()
                    .cloned()
                    .map(daily::DailyBar::from)
                    .map(|b| b.bar().unwrap())
                    .collect::<Vec<_>>()
            );
        } else {
            assert_eq!(ticks(&store, &second_m), expected_ticks);
        }
        let pages_before = all_pages(&store, &second_m);
        let repeat = pipeline("update", &config, &["--end", &time_text(cutoff2)]).unwrap();
        let repeated = dataset(&store, field(job_line(&repeat, "daily"), "dataset"));
        assert_ne!(repeated.generation, second_m.generation);
        assert_eq!(
            keys(&repeated, "observations/"),
            keys(&second_m, "observations/")
        );
        let pages_after = all_pages(&store, &repeated);
        let mut expected = BTreeMap::new();
        for entry in fs::read_dir(scratch.path("producer/pipeline_state/records")).unwrap() {
            let path = entry.unwrap().path();
            if !path
                .file_name()
                .unwrap()
                .to_str()
                .unwrap()
                .starts_with("daily-receipt-")
            {
                continue;
            }
            let receipt = read_json(&path);
            assert!(receipt["acquisition_id"].is_string());
            for request in receipt["requests"].as_array().unwrap() {
                let identity = &request["occurrence"];
                let key = (
                    identity["acquisition_id"].as_str().unwrap().to_string(),
                    identity["ordinal"].as_u64().unwrap(),
                );
                if let Some(old) = expected.insert(key, request.clone()) {
                    assert_eq!(&old, request, "replayed receipts are one occurrence");
                }
            }
        }
        assert_eq!(pages_after.len(), pair.pages.len() + expected.len());
        for page in &pages_after {
            if page.acquisition_id == "fixture-acquisition" {
                assert!(pair.pages.contains(page));
                continue;
            }
            let request = &expected[&(page.acquisition_id.clone(), page.ordinal)];
            assert_eq!(page.payload_sha256, request["sha256"]);
            assert_eq!(
                page.payload.len() as u64,
                request["bytes"].as_u64().unwrap()
            );
            assert_eq!(page.rows, request["rows"].as_u64().unwrap());
            assert_eq!(
                serde_json::to_value(&page.request_token).unwrap(),
                request["anchor"]
            );
            assert_eq!(
                time_text(page.receipt_time_utc.unwrap()),
                request["receipt_time"]
            );
            assert_eq!(
                page.intent.as_deref(),
                request["occurrence"]["intent"].as_str()
            );
        }
        assert!(pages_after.len() > pages_before.len());
        for p in &pages_before {
            assert!(pages_after.contains(p));
        }
        let changed = keys(&repeated, "pages/")
            .into_iter()
            .filter(|(p, k)| keys(&second_m, "pages/").get(p) != Some(k))
            .map(|(p, _)| p)
            .collect::<Vec<_>>();
        assert_eq!(changed, ["pages/2026-09-22.parquet"]);
        // Compare to an independent uninterrupted daily encoding and audit of all input rows.
        fs::rename(&store, &root).unwrap();
        let mut whole = pair.v2.clone();
        whole.source_kind = ds::SourceKind::BrokerHistory;
        rewrite_rows(&scratch, &mut whole, &expected_ticks, &expected_bars);
        for day in &mut whole.day_inventory {
            if day.family == DayFamily::Observations {
                let evidence = second_m
                    .day_inventory
                    .iter()
                    .find(|d| d.family == day.family && d.date == day.date)
                    .unwrap();
                day.state = evidence.state;
                day.reason = evidence.reason.clone();
                day.unresolved = evidence.unresolved.clone();
            }
        }
        whole.config_hash = "b".repeat(64);
        let proof = scratch.path("whole-proof.json");
        fs::write(&proof, b"{\"uninterrupted\":true}").unwrap();
        whole
            .objects
            .retain(|o| o.path != "provenance/coverage.json");
        whole.objects.push(common::daily::object(
            &root,
            "provenance/coverage.json",
            ObjectRole::Provenance,
            &proof,
        ));
        common::daily::publish(&root, &mut whole);
        fs::write(
            &audit_config,
            core.replace(
                "historical_data_dir = \"unused\"",
                &format!("historical_data_dir = \"{}\"", root.display()),
            )
            .replace(
                "publication_uri = \"file:///unused\"",
                &format!("publication_uri = \"file://{}\"", root.display()),
            ),
        )
        .unwrap();
        let whole_stream = audit_snapshot(&scratch, &audit_config, &whole);
        assert_eq!(
            keys(&whole, "observations/"),
            keys(&second_m, "observations/")
        );
        for object in &whole_stream.objects {
            if object.path != "profile.json" {
                assert_eq!(candle_key(&second_stream, &object.path), object.key);
            }
        }
        assert_eq!(
            normalized_profile(&root, &whole_stream),
            normalized_profile(&root, &second_stream)
        );
        verify::run(&format!("file://{}", root.join(repeated.key()).display())).unwrap();
        let effective =
            fs::read_to_string(scratch.path("producer/pipeline_state/daily/update.toml")).unwrap();
        assert!(effective.contains(&pair.v2.generation));
    }
}

#[test]
fn fresh_imports_are_daily_roots_with_lossless_provider_rows_and_source_framing() {
    use sha2::{Digest, Sha256};
    let fixture = fixture("daily_import_roundtrip");
    let scratch = &fixture.scratch;
    let asset = scratch.path("sources/pocket/AEDCNY_otc");
    let payloads = [
        json!({"asset":"AEDCNY_otc","data":[{"time":POCKET_START + POCKET_OFFSET_S}]}).to_string(),
        json!({"asset":"AEDCNY_otc","data":[]}).to_string(),
    ];
    let hash = |s: &str| binary_alpha_engine::hex(&Sha256::digest(s.as_bytes()));
    let checkpoint = |index: usize, payload: usize| {
        json!({"payload_sha256":hash(&payloads[payload]),"request_token":(POCKET_START+POCKET_OFFSET_S).to_string(),"index":index}).to_string()
    };
    let raw = format!("{}\r\n{}\n{}", payloads[0], payloads[1], payloads[0]).into_bytes();
    let checks = format!(
        "{}\n{}\r\n{}\n",
        checkpoint(9, 1),
        checkpoint(7, 0),
        checkpoint(1, 0)
    )
    .into_bytes();
    fs::write(asset.join("raw_pages.ndjson"), &raw).unwrap();
    fs::write(asset.join("checkpoint.ndjson"), &checks).unwrap();
    let extra = b"{\"additional_provenance\":true}\r\n";
    fs::write(asset.join("notes.ndjson"), extra).unwrap();
    let metadata_path = scratch.path("sources/deriv/EURUSD/EURUSD_2025-08-12_ticks.meta.json");
    let mut meta = read_json(&metadata_path);
    meta["clipped_by_now"] = json!(true);
    fs::write(&metadata_path, meta.to_string()).unwrap();
    let store = scratch.path("producer/store");
    for (job, instrument) in [
        ("deriv", "deriv:frxEURUSD"),
        ("pocket", "pocket_option:AEDCNY_otc"),
    ] {
        let config = scratch.path(&format!("{job}-import.toml"));
        let report = run(&["data", "import", "--config", config.to_str().unwrap()]).unwrap();
        let m = dataset(&store, imported_generation(&report, instrument));
        assert_eq!(m.layout, Some(Layout::DailyV2));
        verify::run(&format!("file://{}", store.join(m.key()).display())).unwrap();
        assert!(
            !Store::filesystem(&store)
                .list_manifests()
                .unwrap()
                .iter()
                .any(|g| {
                    let value = read_json(&store.join(ds::manifest_key(g)));
                    value["instrument"] == instrument && value.get("layout").is_none()
                })
        );
        let lineage = read_json(
            &store.join(
                &m.objects
                    .iter()
                    .find(|o| o.path == "provenance/lineage.json")
                    .unwrap()
                    .key,
            ),
        );
        if job == "pocket" {
            let pages = all_pages(&store, &m);
            assert_eq!(pages.len(), 3);
            assert_eq!(
                pages
                    .iter()
                    .map(|p| p.checkpoint_ordinal.unwrap())
                    .collect::<Vec<_>>(),
                [1, 0, 2]
            );
            let reconstruct = |checkpoint: bool| {
                let mut ordered = pages.iter().collect::<Vec<_>>();
                ordered.sort_by_key(|p| {
                    if checkpoint {
                        p.checkpoint_ordinal.unwrap()
                    } else {
                        p.ordinal
                    }
                });
                let name = if checkpoint {
                    "checkpoint"
                } else {
                    "raw_pages"
                };
                let terminators = lineage["framing"][name]["framing"]["line_terminators"]
                    .as_array()
                    .unwrap();
                let mut bytes: Vec<u8> = Vec::new();
                for (page, terminator) in ordered.iter().zip(terminators) {
                    bytes.extend(if checkpoint {
                        page.checkpoint.as_ref().unwrap()
                    } else {
                        &page.payload
                    });
                    bytes.extend(terminator.as_str().unwrap().as_bytes());
                }
                bytes
            };
            assert_eq!(reconstruct(false), raw);
            assert_eq!(reconstruct(true), checks);
            let notes = lineage["provenance"]
                .as_array()
                .unwrap()
                .iter()
                .find(|v| v["object"]["path"] == "notes.ndjson")
                .unwrap();
            assert_eq!(
                serde_json::from_value::<Vec<u8>>(notes["bytes_verbatim"].clone()).unwrap(),
                extra
            );
            let actual = m
                .day_inventory
                .iter()
                .filter(|d| d.family == DayFamily::Observations)
                .flat_map(|d| {
                    daily::read_bars(&store.join(d.object.as_ref().unwrap()), &d.date).unwrap()
                })
                .collect::<Vec<_>>();
            let expected = bar_rows(POCKET_START, POCKET_SEED_END)
                .into_iter()
                .map(|b| daily::DailyBar {
                    symbol: Some(b.symbol),
                    symbol_id: Some(b.symbol_id),
                    timestamp_utc: Some(b.timestamp.unwrap_or(b.unix * 1_000_000)),
                    unix_utc_s: Some(b.unix),
                    server_time_s: Some(b.server.unwrap_or(b.unix + POCKET_OFFSET_S)),
                    open: Some(b.ohlcv[0]),
                    high: Some(b.ohlcv[1]),
                    low: Some(b.ohlcv[2]),
                    close: Some(b.ohlcv[3]),
                    volume: Some(b.ohlcv[4]),
                    period_s: Some(b.period.try_into().unwrap()),
                })
                .collect::<Vec<_>>();
            assert_eq!(
                actual, expected,
                "all eleven provider columns survive import"
            );
        } else {
            let clipped = m
                .day_inventory
                .iter()
                .find(|d| d.date == "2025-08-12")
                .unwrap();
            assert_eq!(clipped.state, ds::DayState::Partial);
            assert!(!clipped.unresolved.is_empty());
        }
        let legacy_report = legacy_import(&config).unwrap();
        let legacy = dataset(&store, imported_generation(&legacy_report, instrument));
        if job == "deriv" {
            let expected = expected_ticks(DERIV_SEED_END, DERIV_SEED_END, DERIV_SEED_END);
            assert_eq!(ticks(&store, &m), expected);
            assert_eq!(ticks(&store, &legacy), expected);
        } else {
            assert_eq!(bars(&store, &m), bars(&store, &legacy));
        }
    }
}

#[test]
fn daily_received_journal_interruption_resumes_without_deleting_unresolved_evidence() {
    use std::io::Write;
    let f = fixture("daily_received_interruption");
    let config = f.scratch.path("only-pocket.toml");
    fs::write(
        &config,
        pipeline_toml(
            &f.scratch.path("producer"),
            &f.drive.base,
            &[("pocket", "pocket.toml")],
            None,
            1,
        ),
    )
    .unwrap();
    run(&[
        "data",
        "import",
        "--config",
        f.scratch.path("pocket-import.toml").to_str().unwrap(),
    ])
    .unwrap();
    fs::write(
        f.scratch.path("pocket.toml"),
        pocket_core(&f.pocket.url, "demo", BAR_GRANULARITY, 60, 1, 60),
    )
    .unwrap();
    let end = time_text((POCKET_SEED_END + 300) * 1_000_000);
    let pending = pipeline("update", &config, &["--end", &end]).unwrap_err();
    assert!(pending.contains("status pending"), "{pending}");
    let state = f.scratch.path("producer/pipeline_state/pocket");
    let progress = read_progress(&state.join("progress.json"));
    let single = f.scratch.path("producer/store").join(ds::object_key(
        progress["progress"]["pages"][0]["sha256"].as_str().unwrap(),
    ));
    fs::OpenOptions::new()
        .append(true)
        .open(state.join("progress.received.jsonl"))
        .unwrap()
        .write_all(b"{\"interrupted\":")
        .unwrap();
    fs::write(
        f.scratch.path("pocket.toml"),
        pocket_core(&f.pocket.url, "demo", BAR_GRANULARITY, 60, 50, 60),
    )
    .unwrap();
    let resumed = pipeline("update", &config, &["--end", &end]).unwrap();
    assert!(resumed.contains("incomplete response metadata retained as unresolved"));
    let line = resumed
        .lines()
        .find(|l| l.starts_with("pipeline update "))
        .unwrap();
    let store = f.scratch.path("producer/store");
    let m = dataset(&store, field(line, "dataset"));
    verify::run(&format!("file://{}", store.join(m.key()).display())).unwrap();
    assert!(
        single.exists(),
        "unresolved references are never permission to reclaim"
    );
    assert!(
        !state.join("progress.json").exists(),
        "indexed acquisition closed successfully"
    );
    let fragment = fs::read_dir(&state)
        .unwrap()
        .map(|e| e.unwrap().path())
        .find(|p| {
            p.file_name()
                .unwrap()
                .to_str()
                .unwrap()
                .starts_with("unresolved-received-")
        })
        .unwrap();
    assert_eq!(
        serde_json::from_value::<Vec<u8>>(read_json(&fragment)["fragment"].clone()).unwrap(),
        b"{\"interrupted\":"
    );
}

#[test]
fn deferred_reclamation_survives_retirement_of_superseded_descendant() {
    let f = fixture("review_reclaim_retirement");
    let config = f.scratch.path("only-pocket.toml");
    fs::write(
        &config,
        pipeline_toml(
            &f.scratch.path("producer"),
            &f.drive.base,
            &[("pocket", "pocket.toml")],
            None,
            1,
        ),
    )
    .unwrap();
    run(&[
        "data",
        "import",
        "--config",
        f.scratch.path("pocket-import.toml").to_str().unwrap(),
    ])
    .unwrap();
    fs::write(
        f.scratch.path("pocket.toml"),
        pocket_core(&f.pocket.url, "demo", BAR_GRANULARITY, 60, 1, 60),
    )
    .unwrap();
    let end = time_text((POCKET_SEED_END + 300) * 1_000_000);
    assert!(
        pipeline("update", &config, &["--end", &end])
            .unwrap_err()
            .contains("status pending")
    );
    let state = f.scratch.path("producer/pipeline_state/pocket");
    let progress = read_progress(&state.join("progress.json"));
    let hash = progress["progress"]["pages"][0]["sha256"].as_str().unwrap();
    let other = f.scratch.path("producer/pipeline_state/another-job");
    fs::create_dir_all(&other).unwrap();
    fs::write(
        other.join("progress.json"),
        json!({"page":{"sha256":hash}}).to_string(),
    )
    .unwrap();
    fs::write(
        f.scratch.path("pocket.toml"),
        pocket_core(&f.pocket.url, "demo", BAR_GRANULARITY, 60, 50, 60),
    )
    .unwrap();
    let first = pipeline("update", &config, &["--end", &end]).unwrap();
    let first_generation = field(job_line(&first, "pocket"), "dataset");
    let second = pipeline(
        "update",
        &config,
        &["--end", &time_text((POCKET_SEED_END + 600) * 1_000_000)],
    )
    .unwrap();
    let second_generation = field(job_line(&second, "pocket"), "dataset");
    assert_ne!(first_generation, second_generation);
    let store = f.scratch.path("producer/store");
    let newest = dataset(&store, second_generation);
    verify::run(&format!("file://{}", store.join(newest.key()).display())).unwrap();
    // Retirement keeps the root/newest closure and pending shared single, dropping the old snapshot.
    fs::remove_file(store.join(ds::manifest_key(first_generation))).unwrap();
    assert!(store.join(ds::object_key(hash)).exists());
    let after_retirement = pipeline(
        "update",
        &config,
        &["--end", &time_text((POCKET_SEED_END + 900) * 1_000_000)],
    );
    assert!(
        after_retirement.is_ok(),
        "a deferred old cleanup must not block valid continuation: {after_retirement:?}"
    );
    let report = after_retirement.unwrap();
    let latest = dataset(&store, field(job_line(&report, "pocket"), "dataset"));
    let single = store.join(ds::object_key(hash));
    assert!(
        single.exists(),
        "pending ownership still pins the single after retirement"
    );
    fs::remove_file(other.join("progress.json")).unwrap();
    fs::remove_file(store.join(newest.key())).unwrap();
    fs::remove_file(store.join(latest.key())).unwrap();

    // A valid daily closure with the same payload but a different occurrence is not proof.
    let mut wrong = latest.clone();
    let mut changed = false;
    let mut page_key = None;
    for day in wrong
        .day_inventory
        .iter_mut()
        .filter(|d| d.family == DayFamily::Pages)
    {
        let Some(key) = day.object.clone() else {
            continue;
        };
        let mut pages = daily::read_pages(&store.join(&key), &day.date).unwrap();
        for page in &mut pages {
            if page.payload_sha256 == hash {
                page.ordinal += 1_000_000;
                changed = true;
                page_key = Some(key.clone());
            }
        }
        pages.sort_by(|a, b| (&a.acquisition_id, a.ordinal).cmp(&(&b.acquisition_id, b.ordinal)));
        let file = f.scratch.path("wrong-occurrence.parquet");
        daily::write_pages(&file, &day.date, [pages]).unwrap();
        let object = common::daily::object(
            &store,
            &day.logical_path().unwrap(),
            ObjectRole::Source,
            &file,
        );
        wrong.objects.retain(|o| o.key != key);
        day.object = Some(object.key.clone());
        wrong.objects.push(object);
    }
    assert!(
        changed,
        "fixture must alter the protected response occurrence"
    );
    common::daily::publish(&store, &mut wrong);
    verify::run(&format!("file://{}", store.join(wrong.key()).display())).unwrap();
    let core = fs::read_to_string(f.scratch.path("pocket.toml")).unwrap();
    // Admission fails after the cleanup pass, preventing any new acquisition from changing proof.
    fs::write(f.scratch.path("pocket.toml"), "invalid fixture config").unwrap();
    let error = pipeline("update", &config, &["--end", &end]).unwrap_err();
    assert!(
        error.contains("pipeline job pocket failed: job pocket:"),
        "cleanup must defer and reach the deliberately invalid job configuration: {error}"
    );
    assert!(
        single.exists(),
        "an equal payload with the wrong occurrence cannot authorize reclamation"
    );
    fs::remove_file(store.join(wrong.key())).unwrap();
    fs::write(store.join(latest.key()), latest.to_json()).unwrap();
    let page_path = store.join(page_key.unwrap());
    let hidden = f.scratch.path("temporarily-unavailable-page");
    fs::rename(&page_path, &hidden).unwrap();
    let error = pipeline("update", &config, &["--end", &end]).unwrap_err();
    assert!(
        error.contains("pipeline job pocket failed: job pocket:"),
        "cleanup must defer and reach the deliberately invalid job configuration: {error}"
    );
    assert!(
        single.exists(),
        "unavailable replacement proof must defer deletion"
    );
    fs::rename(hidden, page_path).unwrap();
    fs::write(f.scratch.path("pocket.toml"), core).unwrap();
    let resumed = pipeline(
        "update",
        &config,
        &["--end", &time_text((POCKET_SEED_END + 1200) * 1_000_000)],
    )
    .unwrap();
    assert!(
        !single.exists(),
        "released singles must be reclaimed using the retained descendant"
    );
    let final_manifest = dataset(&store, field(job_line(&resumed, "pocket"), "dataset"));
    verify::run(&format!(
        "file://{}",
        store.join(final_manifest.key()).display()
    ))
    .unwrap();
}

#[test]
fn interrupted_import_reclaims_owned_staging_on_retry() {
    let f = fixture("import_staging_retry");
    let config = f.scratch.path("pocket-import.toml");
    let store = f.scratch.path("producer/store");
    let destination = f.scratch.path("published");
    fs::create_dir_all(&destination).unwrap();
    // Fail publication after source retention, without changing source bytes on retry.
    fs::write(destination.join("objects"), b"publication interrupted").unwrap();
    fs::write(
        &config,
        fs::read_to_string(&config).unwrap().replace(
            &format!("publication_uri = \"file://{}\"", store.display()),
            &format!("publication_uri = \"file://{}\"", destination.display()),
        ),
    )
    .unwrap();
    let preexisting = f
        .scratch
        .path("sources/pocket/AEDCNY_otc/preexisting.ndjson");
    fs::write(&preexisting, b"{\"independent_owner\":true}\n").unwrap();
    let preexisting_key = ds::object_key(
        &binary_alpha_app::store::identify(&preexisting)
            .unwrap()
            .sha256,
    );
    fs::create_dir_all(store.join("objects")).unwrap();
    fs::copy(&preexisting, store.join(&preexisting_key)).unwrap();
    let source = f.scratch.path("sources/pocket/AEDCNY_otc/raw_pages.ndjson");
    let checkpoint = f
        .scratch
        .path("sources/pocket/AEDCNY_otc/checkpoint.ndjson");
    let checkpoint_key = ds::object_key(
        &binary_alpha_app::store::identify(&checkpoint)
            .unwrap()
            .sha256,
    );
    let identity = binary_alpha_app::store::identify(&source).unwrap();
    let key = ds::object_key(&identity.sha256);
    assert!(run(&["data", "import", "--config", config.to_str().unwrap()]).is_err());
    assert_eq!(
        fs::read(store.join(&key)).unwrap(),
        fs::read(&source).unwrap(),
        "failed invocation retained the raw input"
    );
    assert!(store.join(&checkpoint_key).exists());
    // Another ready generation acquires a reference while this import is interrupted.
    let legacy = legacy_import(&f.scratch.path("deriv-import.toml")).unwrap();
    let mut pinned = dataset(&store, imported_generation(&legacy, "deriv:frxEURUSD"));
    let pinned_object = common::daily::object(
        &store,
        "source/shared-metadata.json",
        ObjectRole::Source,
        &f.scratch
            .path("sources/pocket/AEDCNY_otc/download_manifest.json"),
    );
    pinned.objects.push(pinned_object.clone());
    common::daily::publish(&store, &mut pinned);
    verify::run(&format!("file://{}", store.join(pinned.key()).display())).unwrap();
    fs::remove_file(destination.join("objects")).unwrap();
    let report = run(&["data", "import", "--config", config.to_str().unwrap()]).unwrap();
    let manifest = dataset(
        &store,
        imported_generation(&report, "pocket_option:AEDCNY_otc"),
    );
    assert_eq!(manifest.layout, Some(Layout::DailyV2));
    assert!(!manifest.objects.iter().any(|o| o.key == key));
    assert!(
        !store.join(&key).exists(),
        "successful retry must reclaim its earlier raw staging object"
    );
    assert!(
        !store.join(&checkpoint_key).exists(),
        "checkpoint staging must also be reclaimed"
    );
    assert_eq!(
        fs::read(store.join(&preexisting_key)).unwrap(),
        fs::read(preexisting).unwrap(),
        "unowned reused inputs remain untouched"
    );
    assert_eq!(
        binary_alpha_app::store::identify(&store.join(&pinned_object.key))
            .unwrap()
            .sha256,
        pinned_object.sha256,
        "ready-manifest references still protect owned staging"
    );
    verify::run(&format!("file://{}", store.join(pinned.key()).display())).unwrap();
    verify::run(&format!("file://{}", store.join(manifest.key()).display())).unwrap();
}

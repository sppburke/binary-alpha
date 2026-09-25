use super::support::*;
use binary_alpha_app::{broker::transport::RecordedConnector, live};
use binary_alpha_engine::{
    portfolio::Selection,
    research::{self, CertificationManifest, Frozen},
};
use serde_json::{Value, json};
use std::fs;

#[test]
fn projection_consumes_verified_bundle_and_public_envelope() {
    let fixture = Fixture::new("live-projection");
    let frozen_key = research::frozen_key(&fixture.bundle.generation);
    let frozen_bytes = fs::read(fixture.scratch.path("published").join(&frozen_key)).unwrap();
    let source_bytes = object(
        &fixture.scratch.root,
        &fixture.bundle.generation,
        "research.json",
    );
    let frozen = Frozen::from_json(&frozen_bytes).unwrap();
    let selection = Selection::from_json(&object(
        &fixture.scratch.root,
        &fixture.run.selection,
        "selection.json",
    ))
    .unwrap();
    let scenarios =
        serde_json::to_vec(&fixture.run.config.research.as_ref().unwrap().scenarios).unwrap();
    let report = cli(
        &fixture.log(),
        &["live", "replay", "--config", fixture.path.to_str().unwrap()],
    )
    .unwrap_err();
    assert!(report.contains("No such file"), "{report}"); // Projection precedes opening the absent recorded log.
    let log = fs::read_to_string(fixture.log()).unwrap();
    for dataset in fixture
        .datasets
        .iter()
        .filter(|d| d.role == binary_alpha_engine::dataset::DatasetRole::Holdout)
    {
        assert!(!log.contains(&dataset.generation), "{log}");
        for object in &dataset.objects {
            assert!(!log.contains(&object.key), "{log}");
        }
    }
    let cert_uri = fixture
        .config
        .live
        .as_ref()
        .unwrap()
        .certification_manifest
        .to_string();
    let cert: Value =
        serde_json::from_slice(&fs::read(cert_uri.strip_prefix("file://").unwrap()).unwrap())
            .unwrap();
    for child in cert["objects"].as_array().unwrap() {
        assert!(!log.contains(child["key"].as_str().unwrap()), "{log}");
    }
    let definition = fixture.definition();
    let policy = selection.frozen.as_ref().unwrap();
    assert_eq!(definition.definition.replay.accounts.len(), 1);
    assert_eq!(
        definition.definition.replay.accounts,
        fixture
            .run
            .config
            .research
            .as_ref()
            .unwrap()
            .portfolio
            .accounts
    );
    assert_eq!(definition.policy.baseline, policy.contracts);
    assert_eq!(
        definition
            .policy
            .replay
            .bindings
            .iter()
            .map(|b| (
                &b.id,
                &b.strategy,
                &b.account,
                &b.instrument,
                &b.contract,
                &b.risk_policy
            ))
            .collect::<Vec<_>>(),
        policy
            .bindings
            .iter()
            .map(|b| (
                &b.id,
                &b.strategy,
                &b.account,
                &b.instrument,
                &b.contract,
                &b.risk_policy
            ))
            .collect::<Vec<_>>()
    );
    assert_eq!(definition.policy.refit, selection.refit);
    for (i, bound) in definition.definition.instruments.iter().enumerate() {
        assert_eq!(bound.tick_generation, frozen.instruments[i].source);
        assert_eq!(bound.feature_generation, selection.refit[i].generation);
        assert_eq!(bound.plan_identity, selection.refit[i].plan_identity);
    }
    let historical = &fixture.run.outer[0].outer.replay.generation;
    assert_ne!(&definition.manifest.definition, historical);
    binary_alpha_engine::execution::Engine::new(definition.definition.clone()).unwrap();
    assert_eq!(
        fs::read(fixture.scratch.path("published").join(frozen_key)).unwrap(),
        frozen_bytes
    );
    assert_eq!(
        object(
            &fixture.scratch.root,
            &fixture.bundle.generation,
            "research.json"
        ),
        source_bytes
    );
    assert_eq!(
        serde_json::to_vec(&fixture.run.config.research.as_ref().unwrap().scenarios).unwrap(),
        scenarios
    );
}

#[test]
fn live_replay_drives_recorded_log_to_receipt_and_final_manifest() {
    let fixture = Fixture::new("live-replay");
    write(&fixture.scratch.path("broker.jsonl"), matching_log());
    let report = cli(
        &fixture.log(),
        &["live", "replay", "--config", fixture.path.to_str().unwrap()],
    )
    .unwrap();
    assert!(
        report.contains("eligible true"),
        "{report}\n{}",
        std::fs::read_to_string(fixture.scratch.path("journal/health.json")).unwrap()
    );
    let final_uri = report
        .lines()
        .find_map(|line| line.strip_prefix("live final manifest "))
        .unwrap();
    let read_uri = |uri: &str| fs::read(uri.strip_prefix("file://").unwrap()).unwrap();
    let final_manifest: live::FinalManifest = serde_json::from_slice(&read_uri(final_uri)).unwrap();
    let receipt: Value = serde_json::from_slice(&read_uri(&final_manifest.receipt)).unwrap();
    assert_eq!(receipt["promotion"]["eligible"], true);
    for dimension in receipt["dimensions"].as_array().unwrap() {
        assert_eq!(dimension["status"], "matched");
        assert_eq!(dimension["samples"], 1);
        if dimension["name"] != "funds_release" {
            assert!(dimension["reason"].is_null());
        }
    }
    let ledger_manifest: binary_alpha_engine::execution::ReplayManifest =
        serde_json::from_slice(&read_uri(final_manifest.ledger.as_deref().unwrap())).unwrap();
    let ledger = object(
        &fixture.scratch.root,
        &ledger_manifest.generation,
        "ledger/events.jsonl",
    );
    let events: Vec<binary_alpha_engine::execution::FinancialEvent> = ledger
        .split(|b| *b == b'\n')
        .filter(|l| !l.is_empty())
        .map(|l| binary_alpha_engine::execution::FinancialEvent::from_line(l).unwrap())
        .collect();
    let restored =
        binary_alpha_engine::execution::Engine::restore(events.iter().map(|e| Ok(e.to_line())))
            .unwrap();
    assert_eq!(restored.accounts()[0].cash.to_string(), "10008.83");
    assert_eq!(restored.accounts()[0].open, 0);
    let mut journal = Vec::new();
    for segment in &final_manifest.journal_segments {
        let bytes = fs::read(fixture.scratch.path("published").join(&segment.key)).unwrap();
        assert_eq!(research::digest(b"", &bytes), segment.sha256);
        assert_eq!(bytes.len() as u64, segment.bytes);
        if bytes
            .split(|b| *b == b'\n')
            .filter(|line| !line.is_empty())
            .count()
            == 16
        {
            let name = std::path::Path::new(&segment.key)
                .file_name()
                .unwrap()
                .to_str()
                .unwrap();
            assert!(!fixture.scratch.path("journal").join(name).exists());
            assert!(
                !fixture
                    .scratch
                    .path("journal")
                    .join(format!("{name}.uploaded"))
                    .exists()
            );
        }
        journal.extend(
            bytes
                .split(|b| *b == b'\n')
                .filter(|line| !line.is_empty())
                .map(|line| serde_json::from_slice::<live::journal::Record>(line).unwrap()),
        );
    }
    if let Some(tail) = &final_manifest.open_tail {
        let bytes = fs::read(fixture.scratch.path("journal/open.jsonl")).unwrap();
        assert_eq!(research::digest(b"", &bytes), tail.sha256);
        assert_eq!(bytes.len() as u64, tail.bytes);
        let records = bytes
            .split(|b| *b == b'\n')
            .filter(|line| !line.is_empty())
            .map(|line| serde_json::from_slice::<live::journal::Record>(line).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(records.first().unwrap().sequence, tail.first_sequence);
        assert_eq!(records.last().unwrap().sequence, tail.last_sequence);
        journal.extend(records);
    }
    assert_eq!(
        final_manifest.ledger_generation.as_deref(),
        Some(ledger_manifest.generation.as_str())
    );
    journal.sort_by_key(|r| r.sequence);
    let cut=journal.iter().position(|r|matches!(&r.kind,live::journal::RecordKind::Ledger{event} if matches!(event.kind,binary_alpha_engine::execution::EventKind::Accepted{..}))).unwrap()+1;
    // Preserve completed synthetic evidence, then simulate a separate process whose durable tail
    // ended after acknowledgement. Only this invented fixture's local/cloud test state is reset.
    fs::rename(
        fixture.scratch.path("journal"),
        fixture.scratch.path("completed-journal"),
    )
    .unwrap();
    fs::rename(
        fixture
            .scratch
            .path("published/live")
            .join(&final_manifest.deployment),
        fixture.scratch.path("completed-live-artifacts"),
    )
    .unwrap();
    let prefix: Vec<u8> = journal[..cut]
        .iter()
        .flat_map(|r| {
            let mut line = serde_json::to_vec(r).unwrap();
            line.push(b'\n');
            line
        })
        .collect();
    write(&fixture.scratch.path("journal/open.jsonl"), prefix);
    let restarted = cli(
        &fixture.log(),
        &["live", "replay", "--config", fixture.path.to_str().unwrap()],
    )
    .unwrap();
    let final_uri = restarted
        .lines()
        .find_map(|line| line.strip_prefix("live final manifest "))
        .unwrap();
    let restarted: live::FinalManifest = serde_json::from_slice(&read_uri(final_uri)).unwrap();
    assert_eq!(restarted.ledger, final_manifest.ledger);
    assert_eq!(
        object(
            &fixture.scratch.root,
            &ledger_manifest.generation,
            "ledger/events.jsonl"
        ),
        ledger
    );
    assert_ne!(final_manifest.journal_segments, restarted.journal_segments);
    assert_eq!(
        read_uri(&final_manifest.receipt),
        read_uri(&restarted.receipt)
    );
}

#[test]
fn projection_refuses_ineligible_bundles() {
    let fixture = Fixture::new("live-ineligible");
    let definition = fixture.definition();
    let frozen = Frozen::from_json(
        &fs::read(
            fixture
                .scratch
                .path("published")
                .join(research::frozen_key(&fixture.bundle.generation)),
        )
        .unwrap(),
    )
    .unwrap();
    let selection = Selection::from_json(&object(
        &fixture.scratch.root,
        &fixture.run.selection,
        "selection.json",
    ))
    .unwrap();
    let cert_path = fixture
        .config
        .live
        .as_ref()
        .unwrap()
        .certification_manifest
        .to_string();
    let cert_bytes = fs::read(cert_path.strip_prefix("file://").unwrap()).unwrap();
    let cert = CertificationManifest::from_json(&cert_bytes).unwrap();
    let project = |run: &research::Run, selection: &Selection, cert: &CertificationManifest| {
        research::live_policy(
            &fixture.bundle,
            run,
            &frozen,
            selection,
            cert,
            &"deriv".to_string().try_into().unwrap(),
            "a0",
            (
                &definition.policy.replay.decision_start,
                &definition.policy.replay.decision_end,
            ),
            definition.policy.replay.inputs.clone(),
        )
    };
    let mut changed = fixture.run.clone();
    let portfolio = &mut changed.config.research.as_mut().unwrap().portfolio;
    let mut unused = portfolio.accounts[0].clone();
    unused.id = "unused".into();
    portfolio.accounts.push(unused);
    assert_eq!(
        project(&changed, &selection, &cert).unwrap_err(),
        "live policy rule 3: refuse a multiaccount portfolio rather than pruning; exactly one account is required"
    );
    let mut changed = selection.clone();
    changed.frozen.as_mut().unwrap().risk_policies[0].max_proposal_age_micros = None;
    assert!(
        project(&fixture.run, &changed, &cert)
            .unwrap_err()
            .contains("requires a predeclared max_proposal_age_micros")
    );
    let mut changed = selection.clone();
    changed.frozen.as_mut().unwrap().contracts[0]
        .tie
        .gross_return = binary_alpha_engine::execution::Decimal::parse("10").unwrap();
    assert!(
        project(&fixture.run, &changed, &cert)
            .unwrap_err()
            .contains("zero loss/tie gross_return")
    );
    for (field, value, expected) in [
        ("state",json!("rejected"),"live policy rule 2: certification must be certified and match the research generation and bundle_sha256".to_string()),
        ("bundle_sha256",json!("f".repeat(64)),format!("{cert_path}: the research run does not carry the frozen bundle this certification names")),
        ("objects",json!([]),format!("{cert_path}: a certification generation publishes exactly `certification.json`")),
    ] {
        let mut value_json: Value = serde_json::from_slice(&cert_bytes).unwrap();
        value_json[field] = value;
        fs::write(
            cert_path.strip_prefix("file://").unwrap(),
            serde_json::to_vec(&value_json).unwrap(),
        )
        .unwrap();
        assert_eq!(live::definition(&fixture.config).err().unwrap(),expected,"{field}");
    }
    fs::write(cert_path.strip_prefix("file://").unwrap(), cert_bytes).unwrap();
    let recorded = RecordedConnector::from_jsonl(&matching_log()).unwrap();
    let control = live::control::FakeControl::new(START);
    let mut runtime = super::support::runtime(&fixture, live::Mode::Replay, &recorded, control);
    let mut stop = false;
    runtime.hook = Some(Box::new(|at| at == live::Checkpoint::BeforeClaim));
    runtime
        .run_until(|_| {
            let previous = stop;
            stop = recorded.exhausted();
            previous
        })
        .unwrap();
    let mut proposal = runtime
        .records()
        .iter()
        .find_map(|r| {
            if let live::journal::RecordKind::Ledger { event } = &r.kind {
                if let binary_alpha_engine::execution::EventKind::Signal {
                    proposal: Some(p), ..
                } = &event.kind
                {
                    Some(p.clone())
                } else {
                    None
                }
            } else {
                None
            }
        })
        .unwrap();
    let binding = runtime.definition.policy.replay.bindings[0].id.clone();
    let cash = runtime.engine().accounts()[0].cash;
    for gross in ["18.80", "18.84"] {
        proposal.terms.win.gross_return =
            binary_alpha_engine::execution::Decimal::parse(gross).unwrap();
        assert!(runtime.offer(&binding, proposal.clone()).unwrap().is_none());
        assert_eq!(runtime.engine().accounts()[0].cash, cash);
        assert!(matches!(
            runtime.records().last().unwrap().kind,
            live::journal::RecordKind::Refused { .. }
        ));
    }
}

#[test]
fn command_and_mode_boundaries() {
    use binary_alpha_engine::config::{Broker, Config, RunMode};
    let fixture = Fixture::new("live-mode");
    let check = |config: &Config, command: &str, expected: &str| {
        let mut config = config.clone();
        if config.run_mode != RunMode::Research {
            let Broker::Deriv(broker) = &mut config.brokers[0] else {
                unreachable!()
            };
            broker.public_endpoint = "wss://example.invalid/public".into();
            broker.bootstrap_endpoint = "https://example.invalid/trading/v1/options".into();
        }
        write(&fixture.path, config.canonical_toml());
        assert_eq!(
            cli(
                &fixture.log(),
                &["live", command, "--config", fixture.path.to_str().unwrap()]
            )
            .unwrap_err(),
            expected
        );
    };
    for mode in [RunMode::Replay, RunMode::Paper, RunMode::Live] {
        let mut c = fixture.config.clone();
        c.run_mode = mode;
        check(
            &c,
            "replay",
            &format!(
                "storage.publication_uri: a `file://` destination requires run_mode `research`, not `{mode}`"
            ),
        );
    }
    for mode in [RunMode::Research, RunMode::Replay] {
        let mut c = fixture.config.clone();
        c.run_mode = mode;
        if mode == RunMode::Replay {
            c.storage.publication_uri = "gs://synthetic-live-boundary".parse().unwrap();
        }
        check(&c, "run", "live run: run_mode must be paper or live");
        c.live.as_mut().unwrap().replay = None;
        check(
            &c,
            "replay",
            "live.replay: is required for run_mode research or replay",
        );
    }
    for mode in [RunMode::Paper, RunMode::Live] {
        let mut c = fixture.config.clone();
        c.run_mode = mode;
        c.storage.publication_uri = "gs://synthetic-live-boundary".parse().unwrap();
        check(
            &c,
            "replay",
            "live.replay: must be absent for run_mode paper or live",
        );
        c.live.as_mut().unwrap().replay = None;
        check(
            &c,
            "run",
            "live.broker: credential is required for run_mode paper or live",
        );
        let Broker::Deriv(broker) = &mut c.brokers[0] else {
            unreachable!()
        };
        broker.credential = Some("SYNTHETIC_UNRESOLVED".into());
        check(
            &c,
            "replay",
            "live replay: run_mode must be research or replay",
        );
        c.live = None;
        check(&c, "run", "live: is required for run_mode paper or live");
    }
    let mut c = fixture.config.clone();
    c.replay = super::fixture_config::replay_configuration(&fixture.scratch.root).replay;
    check(&c, "replay", "live: cannot be combined with [replay]");
}

#[test]
fn retained_deriv_fixtures_produce_a_nonpassing_receipt() {
    let mut fixture = Fixture::new("live-retained");
    fixture
        .config
        .live
        .as_mut()
        .unwrap()
        .compatibility
        .min_samples = 2;
    let before = object(
        &fixture.scratch.root,
        &fixture.bundle.generation,
        "research.json",
    );
    let recorded = RecordedConnector::from_jsonl(&retained_log()).unwrap();
    let mut runtime = super::support::runtime(
        &fixture,
        live::Mode::Replay,
        &recorded,
        live::control::FakeControl::new(START),
    );
    let mut last = None;
    let completed = runtime
        .run_until(|health| {
            if recorded.exhausted() {
                if last == Some(health.journal_sequence) {
                    return true;
                }
                last = Some(health.journal_sequence);
            }
            false
        })
        .unwrap()
        .unwrap();
    assert!(!completed.receipt.promotion.eligible);
    let timing = &completed.receipt.dimensions[4];
    assert_eq!(timing.status, live::receipt::Status::OutsideEnvelope);
    assert_eq!(
        timing.bound,
        "expiry=entry_time+duration; exit_time=expiry; start=entry_time; exit_price=first_due_tick_price"
    );
    let command = runtime
        .records()
        .iter()
        .find_map(|r| {
            if let live::journal::RecordKind::Ledger { event } = &r.kind {
                if let binary_alpha_engine::execution::EventKind::Signal {
                    command: Some(c), ..
                } = &event.kind
                {
                    Some(c)
                } else {
                    None
                }
            } else {
                None
            }
        })
        .unwrap();
    assert_eq!(
        timing.reason.as_deref(),
        Some(
            format!(
                "{command}: entry_time={}, duration=5000000, expiry={}, exit_time={}, start={}, exit_price=920308, due_tick=920409@{}",
                START + 3_000_000,
                START + 16_000_000,
                START + 15_000_000,
                START + 1_000_000,
                START + 16_000_000
            )
            .as_str()
        )
    );
    let delay = &completed.receipt.dimensions[3];
    assert_eq!(delay.status, live::receipt::Status::OutsideEnvelope);
    assert_eq!(delay.bound, "0 microseconds (whole-second resolution)");
    assert_eq!(delay.reason.as_deref(),Some(format!("{command}: purchase_time_micros - decision_second = 1000000; assessed 0 microseconds").as_str()));
    let offer = &completed.receipt.dimensions[1];
    assert_eq!(offer.status, live::receipt::Status::OutsideEnvelope);
    assert_eq!(
        offer.bound,
        "every admitted command accepted at the assessed terms, no rejections"
    );
    assert_eq!(
        offer.reason.as_deref(),
        Some(
            format!(
                "{}: offer differs from exact baseline {}",
                runtime.definition.policy.replay.bindings[0].id,
                runtime.definition.policy.baseline[0].id
            )
            .as_str()
        )
    );
    let dimensions = &completed.receipt.dimensions;
    assert_eq!(dimensions[0].status, live::receipt::Status::Unavailable);
    assert_eq!(
        dimensions[0].reason.as_deref(),
        Some("1 of 2 required samples")
    );
    assert_eq!(dimensions[2].status, live::receipt::Status::OutsideEnvelope);
    assert_eq!(
        dimensions[2].reason,
        Some(format!(
            "{command}: entry_price_units=920252, quote_price_units=920409, quote_age_micros=0"
        ))
    );
    assert_eq!(dimensions[5].status, live::receipt::Status::OutsideEnvelope);
    assert_eq!(
        dimensions[5].reason,
        Some(format!(
            "{command}: expiry_to_evidence=1000000; evidence_to_application=0; total=1000000 microseconds"
        ))
    );
    assert_eq!(
        recorded
            .writes()
            .iter()
            .filter(|(_, text)| text.contains("\"buy\":"))
            .count(),
        1
    );
    assert_eq!(
        object(
            &fixture.scratch.root,
            &fixture.bundle.generation,
            "research.json"
        ),
        before
    );
}

#[test]
fn authorization_cli_enables_the_same_live_owner() {
    let fixture = Fixture::new("live-authorized");
    let mut records: Vec<Value> = matching_log()
        .lines()
        .map(|l| serde_json::from_str(l).unwrap())
        .collect();
    records.insert(2,json!({"session":"account","at":START,"frame":r#"{"msg_type":"portfolio","req_id":21,"portfolio":{"contracts":[]}}"#}));
    let log: String = records.iter().map(|r| format!("{r}\n")).collect();
    let recorded = RecordedConnector::from_jsonl(&log).unwrap();
    let mut runtime = super::support::runtime(
        &fixture,
        live::Mode::Live,
        &recorded,
        live::control::FakeControl::new(START),
    );
    assert_eq!(
        runtime.health().entries,
        live::Entries::Disabled("causal warmup is incomplete; live authorization is absent".into())
    );
    assert!(
        !recorded
            .writes()
            .iter()
            .any(|(_, text)| text.contains("\"buy\":"))
    );
    let (_, destination) = fixture.stores();
    let deployment_uri = destination.uri(&runtime.definition.manifest.key());
    let bundle_uri = fixture
        .config
        .live
        .as_ref()
        .unwrap()
        .bundle_manifest
        .to_string();
    let args = [
        "live",
        "authorization",
        "create",
        "--deployment-manifest",
        &deployment_uri,
        "--bundle-manifest",
        &bundle_uri,
        "--broker",
        "deriv",
        "--account",
        "a0",
        "--reason",
        "synthetic operator approval",
    ];
    // A non-directory target path makes the former cwd/target staging impossible.
    write(
        &fixture.scratch.path("target"),
        b"target must remain untouched",
    );
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_binary-alpha"))
        .args(args)
        .current_dir(&fixture.scratch.root)
        .env("USER", "synthetic-operator")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(
        fs::read(fixture.scratch.path("target")).unwrap(),
        b"target must remain untouched"
    );
    let authorization = cli(&fixture.log(), &args).unwrap();
    assert_eq!(cli(&fixture.log(), &args).unwrap(), authorization);
    assert!(authorization.starts_with("live authorization "));
    runtime.hook = Some(Box::new(|at| at == live::Checkpoint::AfterAcknowledgement));
    assert!(runtime.run_until(|_| false).unwrap().is_none());
    assert_eq!(runtime.health().entries, live::Entries::Enabled);
    assert_eq!(
        recorded
            .writes()
            .iter()
            .filter(|(_, text)| text.contains("\"buy\":"))
            .count(),
        1
    );
    assert!(runtime.records().iter().any(|r|matches!(&r.kind,live::journal::RecordKind::Ledger{event} if matches!(event.kind,binary_alpha_engine::execution::EventKind::Accepted{..}))));
}

#[test]
fn paper_owner_observes_without_a_dispatch_claim_or_write() {
    let fixture = Fixture::new("live-paper");
    let mut records: Vec<Value> = matching_log()
        .lines()
        .take(6)
        .map(|l| serde_json::from_str(l).unwrap())
        .collect();
    records.insert(2,json!({"session":"account","at":START,"frame":r#"{"msg_type":"portfolio","req_id":21,"portfolio":{"contracts":[]}}"#}));
    let log: String = records.iter().map(|r| format!("{r}\n")).collect();
    let recorded = RecordedConnector::from_jsonl(&log).unwrap();
    let control = live::control::FakeControl::new(START);
    let mut runtime =
        super::support::runtime(&fixture, live::Mode::Paper, &recorded, control.clone());
    let completed = runtime
        .run_until(|_| recorded.exhausted())
        .unwrap()
        .unwrap();
    assert!(!completed.receipt.promotion.eligible);
    assert!(completed.ledger.is_none());
    assert!(completed.manifest.ledger.is_none());
    assert!(completed.manifest.ledger_generation.is_none());
    assert_eq!(completed.receipt.ledger, completed.manifest.definition);
    assert!(runtime.records().iter().any(|r|matches!(&r.kind,live::journal::RecordKind::Ledger{event} if matches!(&event.kind,binary_alpha_engine::execution::EventKind::Released{source,rejected:false,..} if source.id.starts_with("paper:")))));
    assert!(runtime.engine().accounts()[0].reserved.is_zero());
    assert_eq!(runtime.engine().accounts()[0].cash.to_string(), "10000.00");
    assert!(
        !recorded
            .writes()
            .iter()
            .any(|(_, text)| text.contains("\"buy\":"))
    );
    assert!(!runtime.records().iter().any(|r| matches!(
        r.kind,
        live::journal::RecordKind::Claimed { .. } | live::journal::RecordKind::Written { .. }
    )));
}

#[test]
fn a_nonbaseline_offer_never_reserves_or_dispatches() {
    let fixture = Fixture::new("live-refused-before-reservation");
    let mut records: Vec<Value> = matching_log()
        .lines()
        .take(6)
        .map(|l| serde_json::from_str(l).unwrap())
        .collect();
    let proposal = change(
        records[5]["frame"].as_str().unwrap(),
        "proposal",
        "payout",
        "19.53",
    );
    records[5]["frame"] = json!(proposal);
    let log: String = records.iter().map(|r| format!("{r}\n")).collect();
    let recorded = RecordedConnector::from_jsonl(&log).unwrap();
    let mut runtime = super::support::runtime(
        &fixture,
        live::Mode::Replay,
        &recorded,
        live::control::FakeControl::new(START),
    );
    let completed = runtime
        .run_until(|_| recorded.exhausted())
        .unwrap()
        .unwrap();
    assert_eq!(
        completed.receipt.dimensions[1].status,
        live::receipt::Status::OutsideEnvelope
    );
    assert_eq!(runtime.engine().accounts()[0].open, 0);
    assert!(runtime.engine().accounts()[0].reserved.is_zero());
    assert_eq!(runtime.engine().accounts()[0].cash.to_string(), "10000.00");
    assert!(
        !recorded
            .writes()
            .iter()
            .any(|(_, text)| text.contains("\"buy\":"))
    );
}

#[test]
fn recorded_expectation_mismatch_fails_before_final_publication() {
    let fixture = Fixture::new("live-expect-mismatch");
    let mut records: Vec<Value> = matching_log()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    records.insert(
        5,
        json!({"session":"account","expect":r#"{"proposal":1,"unexpected":true,"req_id":1}"#}),
    );
    let log: String = records.iter().map(|record| format!("{record}\n")).collect();
    write(&fixture.scratch.path("broker.jsonl"), log);
    assert_eq!(
        cli(
            &fixture.log(),
            &["live", "replay", "--config", fixture.path.to_str().unwrap()]
        )
        .unwrap_err(),
        "recorded account: write does not match expect"
    );
    let definition = fixture.definition();
    assert!(
        !fixture
            .scratch
            .path("published/live")
            .join(definition.deployment)
            .join("final")
            .exists()
    );
}

#[test]
fn two_instruments_ingest_once_and_evaluate_rows_in_frozen_order() {
    use binary_alpha_engine::execution::EventKind;
    let fixture = Fixture::two("live-two-instruments");
    let recorded = RecordedConnector::from_jsonl(&super::support::two_instrument_log()).unwrap();
    let mut runtime = super::support::runtime(
        &fixture,
        live::Mode::Paper,
        &recorded,
        live::control::FakeControl::new(START),
    );
    assert_eq!(runtime.definition.policy.replay.bindings.len(), 2);
    assert!(!runtime.health().warmup);
    let before: Vec<_> = runtime
        .features()
        .iter()
        .map(|engine| engine.profile().observations)
        .collect();
    let completed = runtime
        .run_until(|_| recorded.exhausted())
        .unwrap()
        .unwrap();
    assert!(runtime.health().warmup);
    assert_eq!(runtime.health().receipt_sequence, 2);
    assert_eq!(
        runtime
            .features()
            .iter()
            .map(|engine| engine.profile().observations)
            .collect::<Vec<_>>(),
        before.iter().map(|count| count + 1).collect::<Vec<_>>()
    );
    let signals = runtime
        .records()
        .iter()
        .filter_map(|record| match &record.kind {
            live::journal::RecordKind::Ledger { event } => match &event.kind {
                EventKind::Signal {
                    instrument,
                    binding,
                    close_time_micros,
                    ..
                } => Some((instrument.as_str(), binding.as_str(), *close_time_micros)),
                _ => None,
            },
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(signals.len(), 2, "{signals:?}");
    assert_eq!(
        signals
            .iter()
            .map(|(_, binding, _)| *binding)
            .collect::<Vec<_>>(),
        runtime
            .definition
            .policy
            .replay
            .bindings
            .iter()
            .map(|binding| binding.id.as_str())
            .collect::<Vec<_>>()
    );
    assert_eq!(
        signals
            .iter()
            .map(|(instrument, _, _)| *instrument)
            .collect::<Vec<_>>(),
        ["deriv:R_50", "deriv:R_100"]
    );
    assert_eq!(runtime.engine().accounts()[0].cash.to_string(), "10000.00");
    assert!(runtime.engine().accounts()[0].reserved.is_zero());
    assert_eq!(runtime.engine().accounts()[0].open, 0);
    assert!(!completed.receipt.promotion.eligible);
    assert!(
        !recorded
            .writes()
            .iter()
            .any(|(_, text)| text.contains("\"buy\":"))
    );
}

#[test]
fn replay_decode_failure_is_returned_before_final_publication() {
    let fixture = Fixture::new("live-malformed-tail");
    let log = matching_log() + &super::support::log_line("market", START + 5_000_000, "{malformed");
    write(&fixture.scratch.path("broker.jsonl"), log);
    let error = cli(
        &fixture.log(),
        &["live", "replay", "--config", fixture.path.to_str().unwrap()],
    )
    .unwrap_err();
    assert!(error.contains("malformed"), "{error}");
    let deployment = fixture.definition().deployment;
    assert!(
        !fixture
            .scratch
            .path("published/live")
            .join(deployment)
            .join("final")
            .exists()
    );
}

#[test]
fn confirmed_zero_credit_loss_releases_capacity_once_from_statement_evidence() {
    use binary_alpha_engine::execution::{Engine, EventKind, Observation, Outcome, Resolution};
    let mut fixture = Fixture::new("live-zero-credit");
    fixture
        .config
        .live
        .as_mut()
        .unwrap()
        .control
        .renewal_interval_micros = 1_000_000;
    let recorded = RecordedConnector::from_jsonl(&super::support::zero_credit_log()).unwrap();
    let mut runtime = super::support::runtime(
        &fixture,
        live::Mode::Replay,
        &recorded,
        live::control::FakeControl::new(START),
    );
    let completed = runtime
        .run_until(|_| recorded.exhausted())
        .unwrap()
        .unwrap();
    let events = runtime
        .records()
        .iter()
        .filter_map(|r| match &r.kind {
            live::journal::RecordKind::Ledger { event } => Some(event.clone()),
            _ => None,
        })
        .collect::<Vec<_>>();
    let reconciled = events
        .iter()
        .filter_map(|event| match &event.kind {
            EventKind::Reconciled {
                command,
                source,
                resolution:
                    resolution @ Resolution::Settled {
                        outcome: Outcome::Loss,
                        gross_return,
                        terminal_fee,
                    },
                ..
            } => {
                assert!(gross_return.is_zero());
                assert!(terminal_fee.is_zero());
                Some(Observation::Reconciliation {
                    command: command.clone(),
                    source: source.clone(),
                    resolution: resolution.clone(),
                })
            }
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(reconciled.len(), 1);
    assert_eq!(runtime.engine().accounts()[0].cash.to_string(), "9990.00");
    assert!(runtime.engine().accounts()[0].reserved.is_zero());
    assert!(runtime.engine().accounts()[0].paid_basis.is_zero());
    assert!(runtime.engine().accounts()[0].unresolved_loss.is_zero());
    assert_eq!(runtime.engine().accounts()[0].open, 0);
    let mut restored = Engine::restore(events.iter().map(|event| Ok(event.to_line()))).unwrap();
    let before = restored.accounts().to_vec();
    restored.step(START + 7_000_000, reconciled).unwrap();
    assert_eq!(restored.accounts(), before);
    assert!(restored.drain().is_empty());
    let command = events
        .iter()
        .find_map(|event| match &event.kind {
            EventKind::Reconciled {
                command,
                resolution: Resolution::Settled { .. },
                ..
            } => Some(command),
            _ => None,
        })
        .unwrap();
    assert_eq!(
        completed.receipt.dimensions[5].reason,
        Some(format!(
            "{command}: expiry_to_evidence=2000000; evidence_to_application=0; total=2000000 microseconds"
        ))
    );
}

#[test]
fn mixing_scenario_terms_across_bindings_is_refused() {
    let fixture = Fixture::two("live-scenario-mixture");
    let log = super::support::two_instrument_log();
    let mut records = log
        .lines()
        .map(|line| serde_json::from_str::<Value>(line).unwrap())
        .collect::<Vec<_>>();
    let row = records.last_mut().unwrap();
    row["frame"] = json!(change(
        row["frame"].as_str().unwrap(),
        "proposal",
        "payout",
        "18.80"
    ));
    let recorded = RecordedConnector::from_jsonl(
        &records
            .iter()
            .map(|row| format!("{row}\n"))
            .collect::<String>(),
    )
    .unwrap();
    let mut runtime = super::support::runtime(
        &fixture,
        live::Mode::Paper,
        &recorded,
        live::control::FakeControl::new(START),
    );
    let completed = runtime
        .run_until(|_| recorded.exhausted())
        .unwrap()
        .unwrap();
    let binding = &runtime.definition.policy.replay.bindings[1];
    assert!(runtime.records().iter().any(|record| matches!(&record.kind,live::journal::RecordKind::Refused {binding:id,proposal:Some(_),reason} if *id == binding.id && *reason == format!("offer differs from exact baseline {}",binding.contract))));
    assert_eq!(
        completed.receipt.dimensions[1].status,
        live::receipt::Status::OutsideEnvelope
    );
    assert_eq!(
        completed.receipt.dimensions[1].reason,
        Some(format!(
            "{}: offer differs from exact baseline {}",
            binding.id, binding.contract
        ))
    );
    assert!(runtime.engine().accounts()[0].reserved.is_zero());
    assert_eq!(runtime.engine().accounts()[0].cash.to_string(), "10000.00");
    assert!(
        !recorded
            .writes()
            .iter()
            .any(|(_, text)| text.contains("\"buy\":"))
    );
}

#[test]
fn two_binding_matching_log_proves_all_six_dimensions() {
    use binary_alpha_engine::execution::EventKind;
    let fixture = Fixture::two("live-two-matching");
    let recorded = RecordedConnector::from_jsonl(&super::support::two_matching_log()).unwrap();
    let mut runtime = super::support::runtime(
        &fixture,
        live::Mode::Replay,
        &recorded,
        live::control::FakeControl::new(START),
    );
    let completed = runtime
        .run_until(|_| recorded.exhausted())
        .unwrap()
        .unwrap();
    assert!(
        completed.receipt.promotion.eligible,
        "{:?}",
        completed.receipt.dimensions
    );
    for dimension in &completed.receipt.dimensions {
        assert_eq!(dimension.status, live::receipt::Status::Matched);
        assert_eq!(dimension.samples, 2);
        if dimension.name != "funds_release" {
            assert_eq!(dimension.reason, None);
        }
    }
    let events: Vec<_> = runtime
        .records()
        .iter()
        .filter_map(|r| match &r.kind {
            live::journal::RecordKind::Ledger { event } => Some(event),
            _ => None,
        })
        .collect();
    assert_eq!(
        events
            .iter()
            .filter(|event| matches!(event.kind, EventKind::Accepted { .. }))
            .count(),
        2
    );
    assert_eq!(
        events
            .iter()
            .filter(|event| matches!(event.kind, EventKind::Settled { .. }))
            .count(),
        2
    );
    assert_eq!(runtime.engine().accounts()[0].cash.to_string(), "10017.66");
    assert_eq!(runtime.engine().accounts()[0].open, 0);
    assert_eq!(completed.manifest.measurements.len(), 2);
    for measurements in completed.manifest.measurements.values() {
        assert_eq!(measurements.market_event_to_decision_micros, Some(0));
        assert_eq!(measurements.claim_to_socket_write_micros, Some(0));
        assert_eq!(measurements.decision_to_acceptance_micros, Some(0));
    }
    assert_eq!(
        recorded
            .writes()
            .iter()
            .filter(|(_, text)| text.contains("\"buy\":"))
            .count(),
        2
    );
}

#[test]
fn financially_empty_started_prefix_restarts_through_the_runtime() {
    let fixture = Fixture::new("live-started-prefix");
    let definition = fixture.definition();
    let (mut journal, records) =
        live::journal::Journal::open(&fixture.scratch.path("journal"), &definition.deployment, 16)
            .unwrap();
    assert!(records.is_empty());
    journal
        .append(
            START,
            live::journal::RecordKind::Started {
                config_hash: definition.manifest.config_hash,
                definition: definition.manifest.definition,
                code_revision: binary_alpha_app::import::CODE_REVISION.into(),
            },
        )
        .unwrap();
    journal
        .append(
            START,
            live::journal::RecordKind::Discontinuity {
                reason: "synthetic restart before first ledger append".into(),
            },
        )
        .unwrap();
    drop(journal);
    let recorded = RecordedConnector::from_jsonl(&matching_log()).unwrap();
    let mut runtime = super::support::runtime(
        &fixture,
        live::Mode::Replay,
        &recorded,
        live::control::FakeControl::new(START),
    );
    assert!(!runtime.health().warmup);
    let completed = runtime
        .run_until(|_| recorded.exhausted())
        .unwrap()
        .unwrap();
    assert!(completed.receipt.promotion.eligible);
    assert_eq!(runtime.engine().accounts()[0].cash.to_string(), "10008.83");
    assert_eq!(runtime.engine().accounts()[0].open, 0);
}

#[test]
fn failed_proposal_is_a_refusal_sample_without_a_proposal() {
    let fixture = Fixture::new("live-proposal-rejection");
    let mut records: Vec<Value> = matching_log()
        .lines()
        .take(6)
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    records[5]["frame"] =
        json!(r#"{"msg_type":"proposal","req_id":4,"error":{"code":"RateLimit"}}"#);
    let recorded = RecordedConnector::from_jsonl(
        &records
            .iter()
            .map(|row| format!("{row}\n"))
            .collect::<String>(),
    )
    .unwrap();
    let mut runtime = super::support::runtime(
        &fixture,
        live::Mode::Replay,
        &recorded,
        live::control::FakeControl::new(START),
    );
    let completed = runtime
        .run_until(|_| recorded.exhausted())
        .unwrap()
        .unwrap();
    assert!(runtime.records().iter().any(|record|matches!(&record.kind,live::journal::RecordKind::Refused {proposal:None,reason,..} if reason == "deriv proposal: RateLimit")));
    assert_eq!(completed.receipt.dimensions[1].samples, 1);
    assert_eq!(
        completed.receipt.dimensions[1].status,
        live::receipt::Status::OutsideEnvelope
    );
    assert_eq!(
        completed.receipt.dimensions[1].reason,
        Some(format!(
            "{}: deriv proposal: RateLimit",
            runtime.definition.policy.replay.bindings[0].id
        ))
    );
    assert!(runtime.engine().accounts()[0].reserved.is_zero());
}

#[test]
fn stalled_recorded_log_fails_without_final_publication() {
    let fixture = Fixture::new("live-stalled");
    let mut records: Vec<Value> = matching_log()
        .lines()
        .take(5)
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    // No matching proposal response exists; both sessions eventually park behind this line.
    records.push(json!({"session":"market","expect":r#"{"unexpected":1}"#}));
    write(
        &fixture.scratch.path("broker.jsonl"),
        records
            .iter()
            .map(|row| format!("{row}\n"))
            .collect::<String>(),
    );
    let error = cli(
        &fixture.log(),
        &["live", "replay", "--config", fixture.path.to_str().unwrap()],
    )
    .unwrap_err();
    assert_eq!(
        error,
        "live replay: recorded log stalled before all frames and expected writes were consumed"
    );
    assert!(
        !fixture
            .scratch
            .path("published/live")
            .join(fixture.definition().deployment)
            .join("final")
            .exists()
    );
}

#[test]
fn unused_broker_template_preserves_historical_provenance_and_window_checks() {
    use binary_alpha_engine::execution::{Engine, SettlementRule};
    let fixture = Fixture::new("unused-broker-template");
    let bytes = object(
        &fixture.scratch.root,
        &fixture.run.outer[0].outer.replay.generation,
        "ledger/events.jsonl",
    );
    let engine = Engine::restore(
        bytes
            .split(|b| *b == b'\n')
            .filter(|l| !l.is_empty())
            .map(|l| Ok(l.to_vec())),
    )
    .unwrap();
    let mut config = fixture.config.clone();
    config.live = None;
    config.replay = Some(engine.definition().replay.clone());
    let replay = config.replay.as_mut().unwrap();
    let mut unused = fixture.definition().policy.replay.contracts[0].clone();
    unused.id = "unused-broker-template".into();
    assert_eq!(
        unused.settlement.rule,
        SettlementRule::BrokerAuthoritativeV1
    );
    assert!(
        replay
            .contracts
            .iter()
            .all(|c| c.settlement.rule == SettlementRule::PriceAtDueV1)
    );
    replay.contracts.push(unused);
    let path = fixture.scratch.path("historical-unused.toml");
    write(&path, config.canonical_toml());
    binary_alpha_app::replay::run(&path, &mut Vec::new()).unwrap();
    let mut mismatched = config.clone();
    let replay = mismatched.replay.as_mut().unwrap();
    let alternative = crate::fixture_config::import_ticks(
        &fixture.scratch.root,
        "other-historical-input",
        replay.role,
        "deriv",
        &["R_50"],
        &[4],
        &[crate::fixture_config::ticks_at_scale(
            crate::fixture_config::BASE + 3 * crate::fixture_config::HOUR + 1_000_000,
            &crate::fixture_config::recipe(PLANTED),
            4,
        )],
    );
    replay.inputs[0].tick_manifest =
        crate::fixture_config::uri(&fixture.scratch.root, &alternative[0].generation);
    write(&path, mismatched.canonical_toml());
    let error = binary_alpha_app::replay::run(&path, &mut Vec::new()).unwrap_err();
    assert!(
        error.contains("was computed from tick generation"),
        "{error}"
    );
    let replay = config.replay.as_mut().unwrap();
    replay.splits = None;
    replay.decision_start = replay.decision_end.clone();
    replay.decision_end = crate::fixture_config::time(
        binary_alpha_engine::market::parse_event_time_micros(&replay.decision_start).unwrap()
            + 20_000_000,
    );
    write(&path, config.canonical_toml());
    let error = binary_alpha_app::replay::run(&path, &mut Vec::new()).unwrap_err();
    assert!(
        error.contains("lies outside the declared decision window"),
        "{error}"
    );
}

#[test]
fn market_before_bootstrap_or_transaction_ack_fails_immediately() {
    let fixture = Fixture::new("impossible-startup-order");
    let source = scenario_rows(&matching_log());
    for (name, input, expected) in [
        (
            "bootstrap",
            vec![source[4].clone(), source[0].clone(), source[1].clone()],
            "recorded bootstrap: expected bootstrap response before market or account frames",
        ),
        (
            "transaction-ack",
            vec![
                source[0].clone(),
                source[1].clone(),
                source[4].clone(),
                source[2].clone(),
                source[3].clone(),
            ],
            "live replay: recorded log stalled before all frames and expected writes were consumed",
        ),
    ] {
        write(&fixture.scratch.path("broker.jsonl"), scenario_log(&input));
        let error = cli(
            &fixture.log(),
            &["live", "replay", "--config", fixture.path.to_str().unwrap()],
        )
        .unwrap_err();
        assert_eq!(error, expected, "{name}");
        assert!(
            !fixture
                .scratch
                .path("published/live")
                .join(fixture.definition().deployment)
                .join("final")
                .exists()
        );
    }
}

#[test]
fn authenticated_broker_or_account_mismatch_is_refused_at_start() {
    let fixture = Fixture::new("authenticated-binding-mismatch");
    for field in ["broker", "account"] {
        let recorded = RecordedConnector::from_jsonl(&matching_log()).unwrap();
        let error = runtime_with(
            &fixture,
            live::Mode::Replay,
            &recorded,
            Box::new(live::control::FakeControl::new(START)),
            |definition| {
                let account = &mut definition.definition.replay.accounts[0];
                if field == "broker" {
                    account.broker = serde_json::from_value(json!("other")).unwrap();
                } else {
                    account.id = "other".into();
                }
            },
            |m| m,
        )
        .err()
        .unwrap();
        assert_eq!(error, "live: broker account binding mismatch", "{field}");
        assert!(!fixture.scratch.path("journal").exists());
        assert!(
            !recorded
                .writes()
                .iter()
                .any(|(_, text)| text.contains("\"buy\":"))
        );
    }
}

#[test]
fn stale_quote_at_decision_uses_engine_disposition_without_dispatch() {
    use binary_alpha_engine::execution::{Disposition, EventKind};
    let fixture = Fixture::new("runtime-stale-quote");
    let mut input = scenario_rows(&matching_log())[..6].to_vec();
    // The feature closes on this tick; its receipt and decision are later than provider time.
    input[4]["at"] = json!(START + 100_001);
    input[5]["at"] = json!(START + 100_001);
    let recorded = RecordedConnector::from_jsonl(&scenario_log(&input)).unwrap();
    let mut owner = runtime_with(
        &fixture,
        live::Mode::Replay,
        &recorded,
        Box::new(live::control::FakeControl::new(START)),
        |definition| {
            for replay in [
                &mut definition.definition.replay,
                &mut definition.policy.replay,
            ] {
                replay.risk_policies[0].max_quote_age_micros = 100_000;
            }
        },
        |m| m,
    )
    .unwrap();
    owner.run_until(|_| recorded.exhausted()).unwrap().unwrap();
    assert!(
        ledger_events(&owner).iter().any(|event| matches!(
            event.kind,
            EventKind::Signal {
                disposition: Disposition::StaleQuote,
                command: None,
                ..
            }
        )),
        "{:?}",
        ledger_events(&owner)
    );
    assert_eq!(owner.engine().accounts()[0].cash.to_string(), "10000.00");
    assert!(
        !owner
            .records()
            .iter()
            .any(|r| matches!(r.kind, live::journal::RecordKind::Claimed { .. }))
    );
    assert!(
        !recorded
            .writes()
            .iter()
            .any(|(_, text)| text.contains("\"buy\":"))
    );
}

#[test]
fn shared_value_readiness_rejects_false_flags_missing_values_and_unready_labels() {
    use binary_alpha_engine::{execution::value_ready, features::Value};
    let unready = vec!["not_ready".into()];
    assert!(!value_ready(
        Some(&Value::Text("up".into())),
        &unready,
        [false]
    ));
    assert!(!value_ready(
        Some(&Value::Text("not_ready".into())),
        &unready,
        [true]
    ));
    assert!(!value_ready(None, &unready, [true]));
    assert!(value_ready(
        Some(&Value::Text("up".into())),
        &unready,
        [true]
    ));
}

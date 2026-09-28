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
    // The deployment and receipt bind the definition identity; the supplied ledger keys its own.
    let definition = fixture.definition().definition;
    let identity = |ledger| {
        binary_alpha_engine::execution::replay_generation_id(
            &definition.config_hash,
            &definition.code_revision,
            &definition.instruments,
            ledger,
        )
    };
    let deployment: Value = serde_json::from_slice(
        &fs::read(
            fixture
                .scratch
                .path("published/live/deployments")
                .join(format!("{}.json", final_manifest.deployment)),
        )
        .unwrap(),
    )
    .unwrap();
    assert_eq!(deployment["definition"], identity(None));
    assert_eq!(receipt["definition"], identity(None));
    assert_eq!(
        ledger_manifest.generation,
        identity(Some(&research::digest(b"", &ledger)))
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
fn pocket_certified_quote_bundle_freezes_live_ages_and_currency() {
    let fixture = Fixture::quote("phase12-pocket-quote");
    let definition = fixture.definition();
    assert_eq!(definition.policy.replay.bindings.len(), 2);
    assert_eq!(definition.policy.replay.inputs.len(), 2);
    assert_eq!(
        definition.policy.replay.accounts[0].currency.to_string(),
        "USD"
    );
    assert_eq!(
        definition.policy.replay.risk_policies[0].max_quote_age_micros,
        1_000_000
    );
    assert_eq!(
        definition.policy.replay.risk_policies[0].max_feature_age_micros,
        5_000_000
    );
    assert_eq!(
        definition.policy.replay.risk_policies[0].max_proposal_age_micros,
        Some(1_000_000)
    );
    assert_eq!(
        fixture.config,
        binary_alpha_engine::config::Config::parse(&fixture.config.canonical_toml()).unwrap()
    );
}

#[test]
fn deriv_quote_bundle_uses_the_currency_guard() {
    let mut fixture = Fixture::quote_deriv_with_candle("phase12-deriv-quote-currency");
    assert_eq!(
        fixture.definition().policy.replay.accounts[0]
            .currency
            .to_string(),
        "USD"
    );
    fixture.config.instruments[0].quote_currency = "EUR".to_string().try_into().unwrap();
    assert!(
        live::definition(&fixture.config)
            .err()
            .unwrap()
            .contains("configured instrument currency")
    );
}

#[test]
fn pocket_real_class_is_refused_before_a_recorded_session_opens() {
    let mut fixture = Fixture::quote("phase12-pocket-real-refused");
    let binary_alpha_engine::config::Broker::PocketOption(settings) =
        &mut fixture.config.brokers[0]
    else {
        panic!("Pocket fixture")
    };
    settings.account_class = binary_alpha_engine::config::AccountClass::Real;
    write(&fixture.path, fixture.config.canonical_toml());
    let definition_error = live::definition(&fixture.config).err().unwrap();
    assert!(
        definition_error.contains("Pocket demo payout settings required"),
        "{definition_error}"
    );
    let absent_log = fixture.scratch.path("no-recorded-session.jsonl");
    assert!(!absent_log.exists());
    let error = cli(
        &absent_log,
        &["live", "replay", "--config", fixture.path.to_str().unwrap()],
    )
    .unwrap_err();
    assert!(error.contains("Pocket real account class"), "{error}");
    assert_eq!(fs::metadata(&absent_log).unwrap().len(), 0);
    assert!(!fixture.scratch.path("journal").exists());
}

#[test]
fn pocket_live_replay_trades_the_certified_quote_policy() {
    let fixture = Fixture::quote("phase12-pocket-live-replay");
    write(
        &fixture.scratch.path("broker.jsonl"),
        super::support::pocket_log(),
    );
    let report = cli(
        &fixture.log(),
        &["live", "replay", "--config", fixture.path.to_str().unwrap()],
    )
    .unwrap();
    let final_uri = report
        .lines()
        .find_map(|line| line.strip_prefix("live final manifest "))
        .expect(&report);
    let read_uri = |uri: &str| fs::read(uri.strip_prefix("file://").unwrap()).unwrap();
    let final_manifest: live::FinalManifest = serde_json::from_slice(&read_uri(final_uri)).unwrap();
    let receipt: Value = serde_json::from_slice(&read_uri(&final_manifest.receipt)).unwrap();
    assert_eq!(receipt["promotion"]["eligible"], false);
    for name in [
        "offer_availability_rejection",
        "acceptance_delay",
        "contract_timing",
        "funds_release",
    ] {
        assert_eq!(
            receipt["dimensions"]
                .as_array()
                .unwrap()
                .iter()
                .find(|dimension| dimension["name"] == name)
                .unwrap()["status"],
            "outside_envelope"
        );
    }
    assert_eq!(receipt["dimensions"][0]["name"], "economics_scope");
    assert_eq!(receipt["dimensions"][0]["status"], "matched");
    let mut journal = Vec::new();
    for segment in &final_manifest.journal_segments {
        journal.extend(fs::read(fixture.scratch.path("published").join(&segment.key)).unwrap());
    }
    journal.extend(fs::read(fixture.scratch.path("journal/open.jsonl")).unwrap());
    let records = journal
        .split(|byte| *byte == b'\n')
        .filter(|line| !line.is_empty())
        .map(|line| serde_json::from_slice::<live::journal::Record>(line).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(records.first().unwrap().sequence, 1);
    assert_eq!(records.last().unwrap().sequence, records.len() as u64);
    let written = records
        .iter()
        .filter_map(|record| match &record.kind {
            live::journal::RecordKind::Written { request_id, .. } => *request_id,
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(written.len(), 2);
    assert_ne!(written[0], written[1]);
    let commands = records
        .iter()
        .filter_map(|record| match &record.kind {
            live::journal::RecordKind::Written {
                command,
                claim,
                request_id: Some(_),
            } => {
                assert!(records.iter().any(|prior| prior.sequence < record.sequence
                    && matches!(&prior.kind, live::journal::RecordKind::Claimed {
                    command: bound, claim: key, .. } if bound == command && key == claim)));
                Some(command.as_str())
            }
            _ => None,
        })
        .collect::<Vec<_>>();
    let refused = records
        .iter()
        .filter(|record| {
            matches!(
                &record.kind,
                live::journal::RecordKind::Refused {
                    listing_cause: Some(live::journal::ListingCause::Ineligible),
                    ..
                }
            )
        })
        .count();
    assert_eq!(refused, 2, "one refusal per binding for the 49% listing");
    assert_eq!(
        records
            .iter()
            .filter(|record| matches!(
                &record.kind,
                live::journal::RecordKind::Refused {
                    listing_cause: Some(live::journal::ListingCause::Stale),
                    ..
                }
            ))
            .count(),
        4
    );
    let ledger = records
        .iter()
        .filter_map(|record| match &record.kind {
            live::journal::RecordKind::Ledger { event } => Some(event),
            _ => None,
        })
        .collect::<Vec<_>>();
    let adjacent = ledger
        .iter()
        .filter_map(|event| match &event.kind {
            binary_alpha_engine::execution::EventKind::Signal {
                close_time_micros,
                disposition,
                quote_price_units,
                proposal,
                ..
            } if *close_time_micros >= QUOTE_START
                && *close_time_micros < QUOTE_START + 1_000_000 =>
            {
                Some((
                    *quote_price_units,
                    *disposition,
                    proposal.as_ref().map(|p| p.identity.clone()),
                    proposal.as_ref().map(|p| p.spot_units),
                    proposal
                        .as_ref()
                        .map(|p| p.terms.win.gross_return.to_string()),
                ))
            }
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(adjacent.len(), 2);
    assert_eq!(adjacent[0].0, Some(100_644));
    assert_eq!(
        adjacent[0].1,
        binary_alpha_engine::execution::Disposition::Admitted
    );
    assert!(adjacent[0].2.is_some());
    assert_eq!(adjacent[0].3, Some(100_644));
    assert_eq!(adjacent[0].4.as_deref(), Some("1.92"));
    assert_eq!(adjacent[1].0, Some(100_800));
    assert_eq!(
        adjacent[1].1,
        binary_alpha_engine::execution::Disposition::CapacityTotal
    );
    assert!(adjacent[1].2.is_none());
    let first = ledger
        .iter()
        .find_map(|event| match &event.kind {
            binary_alpha_engine::execution::EventKind::Signal {
                close_time_micros,
                proposal: Some(proposal),
                disposition: binary_alpha_engine::execution::Disposition::Admitted,
                ..
            } if *close_time_micros == QUOTE_START + 300_000 => Some(proposal),
            _ => None,
        })
        .unwrap();
    assert_eq!(first.spot_units, 100_644);
    assert_eq!(first.terms.win.gross_return.to_string(), "1.92");
    assert!(
        !records
            .iter()
            .take_while(|record| !matches!(&record.kind, live::journal::RecordKind::Written { .. }))
            .any(|record| matches!(
                &record.kind,
                live::journal::RecordKind::PocketCorrelation {
                    correlated: false,
                    ..
                }
            ))
    );
    let offered = ledger
        .iter()
        .filter_map(|event| match &event.kind {
            binary_alpha_engine::execution::EventKind::Signal {
                close_time_micros,
                proposal: Some(proposal),
                ..
            } if *close_time_micros == QUOTE_START + 300_000
                || *close_time_micros == QUOTE_START + 32_500_000 =>
            {
                Some((proposal.identity.as_str(), proposal.spot_units))
            }
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(offered.len(), 2);
    assert_ne!(offered[0].0, offered[1].0);
    assert_ne!(offered[0].1, offered[1].1);
    assert_eq!(
        ledger
            .iter()
            .filter(|event| matches!(
                event.kind,
                binary_alpha_engine::execution::EventKind::Accepted { .. }
            ))
            .count(),
        2
    );
    let settled = ledger
        .iter()
        .filter(|event| {
            matches!(
                event.kind,
                binary_alpha_engine::execution::EventKind::Settled { .. }
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(settled.len(), 2);
    for command in commands {
        assert!(ledger.iter().any(|event| matches!(&event.kind,
            binary_alpha_engine::execution::EventKind::Accepted { command: accepted, .. }
            if accepted == command)));
        assert!(settled.iter().any(|event| matches!(&event.kind,
            binary_alpha_engine::execution::EventKind::Settled { command: done, .. }
            if done == command)));
    }
    assert!(
        settled
            .iter()
            .all(|event| event.to_line().windows(7).any(|bytes| bytes == b"pocket:"))
    );
    let health: Value =
        serde_json::from_slice(&fs::read(fixture.scratch.path("journal/health.json")).unwrap())
            .unwrap();
    assert_eq!(health["risk"]["cash"], "10001.84");
    assert_eq!(health["risk"]["open"], 0);
    assert!(final_manifest.ledger.is_some());
}

#[test]
fn pocket_missing_listing_refuses_once_then_fresh_listing_admits() {
    let fixture = Fixture::quote("phase12-pocket-missing-listing");
    let recorded = RecordedConnector::from_jsonl(&pocket_missing_log()).unwrap();
    let mut runtime = pocket_runtime(
        &fixture,
        live::Mode::Replay,
        &recorded,
        live::control::FakeControl::new(QUOTE_START - 2_000_000),
    )
    .unwrap();
    let mut unavailable = false;
    let mut resumed = false;
    runtime
        .run_until(|health| {
            unavailable |= matches!(&health.entries, live::Entries::Disabled(reason)
                if reason.contains("proposal unavailable"));
            resumed |= unavailable && matches!(health.entries, live::Entries::Enabled);
            recorded.exhausted()
        })
        .unwrap();
    assert!(unavailable && resumed);
    let records = runtime.records();
    assert_eq!(
        records
            .iter()
            .filter(|record| matches!(
                &record.kind,
                live::journal::RecordKind::Refused {
                    listing_cause: Some(live::journal::ListingCause::Missing),
                    ..
                }
            ))
            .count(),
        2
    );
    let first_written = records
        .iter()
        .find(|record| matches!(&record.kind, live::journal::RecordKind::Written { .. }))
        .unwrap();
    assert!(first_written.time_micros > QUOTE_START + 250_000);
    assert!(!records.iter().any(|record| matches!(&record.kind,
        live::journal::RecordKind::Ledger { event: binary_alpha_engine::execution::FinancialEvent {
            kind: binary_alpha_engine::execution::EventKind::Signal { command: Some(_), close_time_micros, .. }, ..
        }} if *close_time_micros <= QUOTE_START + 200_000)));
    assert_eq!(
        records
            .iter()
            .filter(|record| matches!(&record.kind, live::journal::RecordKind::Written { .. }))
            .count(),
        2
    );
    let health: Value =
        serde_json::from_slice(&fs::read(fixture.scratch.path("journal/health.json")).unwrap())
            .unwrap();
    assert_eq!(health["risk"]["cash"], "10001.84");
}

#[test]
fn pocket_old_statement_deal_is_ignored_but_new_foreign_close_vetoes_entries() {
    let fixture = Fixture::quote("phase12-pocket-foreign");
    let recorded = RecordedConnector::from_jsonl(&pocket_foreign_log()).unwrap();
    let mut runtime = pocket_runtime(
        &fixture,
        live::Mode::Replay,
        &recorded,
        live::control::FakeControl::new(QUOTE_START - 2_000_000),
    )
    .unwrap();
    let mut after_old = None;
    runtime
        .run_until(|health| {
            if health.receipt_sequence >= 1 && after_old.is_none() {
                after_old = Some((health.entries.clone(), health.risk.cash.to_string()));
            }
            recorded.exhausted()
        })
        .unwrap();
    let (entries, cash) = after_old.expect("first quote after older closed row");
    assert!(!matches!(&entries, live::Entries::Disabled(reason)
        if reason.contains("uncorrelated liability")));
    assert_eq!(cash, "10000.00");
    assert_eq!(runtime.engine().accounts()[0].cash.to_string(), "10001.84");
    assert!(
        matches!(&runtime.health().entries, live::Entries::Disabled(reason) if reason.contains("uncorrelated liability"))
    );
}

#[test]
fn pocket_old_foreign_open_still_vetoes_entries() {
    let fixture = Fixture::quote("phase12-pocket-old-open");
    let recorded = RecordedConnector::from_jsonl(&pocket_old_foreign_open_log()).unwrap();
    let mut runtime = pocket_runtime(
        &fixture,
        live::Mode::Replay,
        &recorded,
        live::control::FakeControl::new(QUOTE_START - 2_000_000),
    )
    .unwrap();
    runtime.run_until(|_| recorded.exhausted()).unwrap();
    assert!(
        matches!(&runtime.health().entries, live::Entries::Disabled(reason)
        if reason.contains("uncorrelated liability"))
    );
    assert!(runtime.records().iter().any(|record| matches!(&record.kind,
        live::journal::RecordKind::PocketCorrelation { deal_id, correlated: false }
        if deal_id == "synthetic-old-open")));
    assert!(
        !runtime
            .records()
            .iter()
            .any(|record| matches!(record.kind, live::journal::RecordKind::Written { .. }))
    );
}

#[test]
fn pocket_contradictory_live_close_posts_no_cash_and_vetoes() {
    let fixture = Fixture::quote("phase12-pocket-bad-live-close");
    let recorded = RecordedConnector::from_jsonl(&pocket_bad_live_close_log()).unwrap();
    let mut runtime = pocket_runtime(
        &fixture,
        live::Mode::Replay,
        &recorded,
        live::control::FakeControl::new(QUOTE_START - 2_000_000),
    )
    .unwrap();
    runtime.run_until(|_| recorded.exhausted()).unwrap();
    assert_eq!(runtime.engine().accounts()[0].cash.to_string(), "9999.00");
    assert!(
        matches!(&runtime.health().entries, live::Entries::Disabled(reason)
        if reason.contains("contradicts claim"))
    );
    assert!(
        !runtime.records().iter().any(|record| matches!(&record.kind,
        live::journal::RecordKind::Ledger { event }
        if matches!(event.kind, binary_alpha_engine::execution::EventKind::Settled { .. })))
    );
    assert_eq!(
        runtime
            .records()
            .iter()
            .filter(|record| matches!(record.kind, live::journal::RecordKind::Written { .. }))
            .count(),
        1
    );
}

#[test]
fn pocket_known_close_with_changed_percent_vetoes() {
    let fixture = Fixture::quote("phase12-pocket-changed-close");
    let recorded = RecordedConnector::from_jsonl(&pocket_changed_close_log()).unwrap();
    let mut runtime = pocket_runtime(
        &fixture,
        live::Mode::Replay,
        &recorded,
        live::control::FakeControl::new(QUOTE_START - 2_000_000),
    )
    .unwrap();
    runtime.run_until(|_| recorded.exhausted()).unwrap();
    assert_eq!(runtime.engine().accounts()[0].cash.to_string(), "10000.92");
    assert_eq!(runtime.engine().accounts()[0].open, 0);
    assert!(
        matches!(&runtime.health().entries, live::Entries::Disabled(reason)
        if reason.contains("contradicts earlier close"))
    );
    assert_eq!(
        runtime
            .records()
            .iter()
            .filter(|record| matches!(&record.kind,
        live::journal::RecordKind::Ledger { event }
        if matches!(event.kind, binary_alpha_engine::execution::EventKind::Settled { .. })))
            .count(),
        1
    );
}

#[test]
fn pocket_new_deal_list_forces_fresh_balance_into_owner() {
    let fixture = Fixture::quote("phase12-pocket-new-fact-balance");
    let recorded = RecordedConnector::from_jsonl(&pocket_new_fact_balance_log()).unwrap();
    let mut runtime = pocket_runtime(
        &fixture,
        live::Mode::Replay,
        &recorded,
        live::control::FakeControl::new(QUOTE_START - 2_000_000),
    )
    .unwrap();
    runtime.run_until(|_| recorded.exhausted()).unwrap();
    assert!(runtime.health().balance_reconciled);
    assert_eq!(runtime.engine().accounts()[0].cash.to_string(), "10000.00");
    assert!(
        !runtime
            .records()
            .iter()
            .any(|record| matches!(&record.kind, live::journal::RecordKind::Written { .. }))
    );
}

#[test]
fn pocket_quote_beyond_frozen_age_cannot_dispatch() {
    use binary_alpha_engine::execution::{Disposition, EventKind};
    let fixture = Fixture::quote("phase12-pocket-aged-quote");
    let recorded = RecordedConnector::from_jsonl(&pocket_aged_quote_log()).unwrap();
    let mut runtime = pocket_runtime(
        &fixture,
        live::Mode::Replay,
        &recorded,
        live::control::FakeControl::new(QUOTE_START - 2_000_000),
    )
    .unwrap();
    runtime.run_until(|_| recorded.exhausted()).unwrap();
    let signals = runtime
        .records()
        .iter()
        .filter_map(|record| match &record.kind {
            live::journal::RecordKind::Ledger {
                event:
                    binary_alpha_engine::execution::FinancialEvent {
                        kind:
                            EventKind::Signal {
                                close_time_micros,
                                disposition,
                                ..
                            },
                        ..
                    },
            } => Some((*close_time_micros, *disposition)),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert!(runtime.records().iter().any(|record| matches!(&record.kind,
        live::journal::RecordKind::Ledger { event: binary_alpha_engine::execution::FinancialEvent {
            kind: EventKind::Signal { close_time_micros, disposition: Disposition::StaleQuote, command: None, .. }, ..
        }} if *close_time_micros == QUOTE_START + 63_000_000)), "{signals:?}");
    assert_eq!(
        runtime
            .records()
            .iter()
            .filter(|record| matches!(&record.kind, live::journal::RecordKind::Written { .. }))
            .count(),
        2
    );
}

#[test]
fn pocket_economic_discrepancy_stops_demo_entries() {
    let fixture = Fixture::quote("phase12-pocket-economics");
    let recorded = RecordedConnector::from_jsonl(&pocket_economics_log()).unwrap();
    let mut runtime = pocket_runtime(
        &fixture,
        live::Mode::Replay,
        &recorded,
        live::control::FakeControl::new(QUOTE_START - 2_000_000),
    )
    .unwrap();
    let completed = runtime
        .run_until(|_| recorded.exhausted())
        .unwrap()
        .unwrap();
    assert_eq!(completed.receipt.dimensions[0].name, "economics_scope");
    assert_eq!(
        completed.receipt.dimensions[0].status,
        live::receipt::Status::OutsideEnvelope
    );
    assert!(
        matches!(&runtime.health().entries, live::Entries::Disabled(reason) if reason.contains("economics_scope"))
    );
    assert_eq!(
        runtime
            .records()
            .iter()
            .filter(|record| matches!(&record.kind, live::journal::RecordKind::Written { .. }))
            .count(),
        1
    );
}

#[test]
fn pocket_refund_on_a_loss_stops_entries_with_economics_receipt() {
    let fixture = Fixture::quote("phase12-pocket-refund-on-loss");
    let recorded = RecordedConnector::from_jsonl(&pocket_refund_on_loss_log()).unwrap();
    let mut runtime = pocket_runtime(
        &fixture,
        live::Mode::Replay,
        &recorded,
        live::control::FakeControl::new(QUOTE_START - 2_000_000),
    )
    .unwrap();
    let completed = runtime
        .run_until(|_| recorded.exhausted())
        .unwrap()
        .unwrap();
    assert!(
        completed
            .receipt
            .dimensions
            .iter()
            .any(|dimension| dimension.name == "economics_scope"
                && dimension.status == live::receipt::Status::OutsideEnvelope)
    );
    assert!(
        matches!(&runtime.health().entries, live::Entries::Disabled(reason)
        if reason.contains("economics_scope"))
    );
    assert_eq!(
        runtime
            .records()
            .iter()
            .filter(|record| matches!(&record.kind, live::journal::RecordKind::Written { .. }))
            .count(),
        1
    );
    assert!(runtime.records().iter().any(|record| matches!(&record.kind,
        live::journal::RecordKind::Ledger { event: binary_alpha_engine::execution::FinancialEvent {
            kind: binary_alpha_engine::execution::EventKind::Settled {
                outcome: binary_alpha_engine::execution::Outcome::Loss, credit, discrepancy: true, ..
            }, ..
        }} if credit.to_string() == "0.50")));
}

#[test]
fn pocket_reconnect_does_not_trigger_from_the_cross_break_jump() {
    use binary_alpha_engine::execution::EventKind;
    let fixture = Fixture::quote("phase12-pocket-reconnect");
    let recorded = RecordedConnector::from_jsonl(&pocket_reconnect_log()).unwrap();
    let mut runtime = pocket_runtime(
        &fixture,
        live::Mode::Replay,
        &recorded,
        live::control::FakeControl::new(QUOTE_START - 2_000_000),
    )
    .unwrap();
    runtime
        .run_until(|health| recorded.exhausted() && health.connection_generation >= 1)
        .unwrap();
    assert!(
        runtime.records().iter().any(|record| matches!(
            &record.kind,
            live::journal::RecordKind::Discontinuity { .. }
        )),
        "generation={} records={}",
        runtime.health().connection_generation,
        runtime.records().len()
    );
    assert!(
        !runtime.records().iter().any(|record| matches!(&record.kind,
        live::journal::RecordKind::Ledger { event: binary_alpha_engine::execution::FinancialEvent {
            kind: EventKind::Signal { close_time_micros, .. }, ..
        }} if *close_time_micros == QUOTE_START + 63_500_000))
    );
    assert_eq!(
        runtime
            .records()
            .iter()
            .filter(|record| matches!(&record.kind, live::journal::RecordKind::Written { .. }))
            .count(),
        2
    );
}

#[test]
fn pocket_unapproved_quotes_are_ingested_without_deferred_order() {
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
        mpsc,
    };
    let fixture = Fixture::quote_with_candle("phase12-pocket-auth-wait");
    let recorded = RecordedConnector::from_jsonl(&pocket_granted_after_early_quotes_log()).unwrap();
    let (first_parked_tx, first_parked_rx) = mpsc::channel();
    let (first_release_tx, first_release_rx) = mpsc::channel();
    let (second_parked_tx, second_parked_rx) = mpsc::channel();
    let (second_release_tx, second_release_rx) = mpsc::channel();
    let (market_parked_tx, market_parked_rx) = mpsc::channel();
    let (market_release_tx, market_release_rx) = mpsc::channel();
    let mut runtime = pocket_runtime_with_market(
        &fixture,
        live::Mode::Live,
        &recorded,
        live::control::FakeControl::new(QUOTE_START - 2_000_000),
        move |inner| {
            Box::new(super::resilience::MarketProbe {
                inner: Box::new(super::resilience::MarketProbe {
                    inner: Box::new(super::resilience::MarketProbe {
                        inner,
                        panic: false,
                        gate: None,
                        consumed: None,
                        subscribe_gate: None,
                        frame_gate: Some((
                            QUOTE_START + 700_000,
                            market_parked_tx,
                            market_release_rx,
                        )),
                        dropped: Arc::new(AtomicBool::new(false)),
                    }),
                    panic: false,
                    gate: None,
                    consumed: None,
                    subscribe_gate: None,
                    frame_gate: Some((QUOTE_START + 300_000, second_parked_tx, second_release_rx)),
                    dropped: Arc::new(AtomicBool::new(false)),
                }),
                panic: false,
                gate: None,
                consumed: None,
                subscribe_gate: None,
                frame_gate: Some((QUOTE_START, first_parked_tx, first_release_rx)),
                dropped: Arc::new(AtomicBool::new(false)),
            })
        },
    )
    .unwrap();
    let mixed_seen = Arc::new(AtomicBool::new(false));
    let observed = mixed_seen.clone();
    runtime.feature_observer = Some(Box::new(move |instrument, output| {
        if instrument == 0
            && output.rows.iter().any(|(stream, _)| *stream == 0)
            && output.rows.iter().any(|(stream, _)| *stream == 1)
        {
            observed.store(true, Ordering::SeqCst);
        }
    }));
    assert!(!runtime.health().authorization_pending);
    let (local, destination) = fixture.stores();
    live::authorization::create(
        &destination,
        &local,
        live::authorization::Authorization {
            schema_version: 1,
            deployment: runtime.definition.manifest.hash.clone(),
            configuration: runtime.definition.manifest.config_hash.clone(),
            bundle_sha256: runtime.definition.manifest.bundle_sha256.clone(),
            broker: "pocket_option".into(),
            account: "a0".into(),
            operator: "synthetic-operator".into(),
            reason: "fixture authorization".into(),
            hash: String::new(),
        },
    )
    .unwrap();
    let (auth_parked_tx, auth_parked_rx) = mpsc::channel();
    let (auth_release_tx, auth_release_rx) = mpsc::channel();
    runtime
        .authorization_probe_handle()
        .install(live::AuthorizationProbe {
            parked: auth_parked_tx,
            release: auth_release_rx,
        });
    let mut auth_parked = false;
    let mut first_parked = false;
    let mut second_parked = false;
    let mut market_parked = false;
    let mut released_first = false;
    let mut released_second = false;
    let mut released_auth = false;
    let mut released_market = false;
    let mut early_sequence = None;
    runtime
        .run_until(|health| {
            auth_parked |= auth_parked_rx.try_recv().is_ok();
            first_parked |= first_parked_rx.try_recv().is_ok();
            second_parked |= second_parked_rx.try_recv().is_ok();
            market_parked |= market_parked_rx.try_recv().is_ok();
            if auth_parked && first_parked && health.authorization_pending && !released_first {
                first_release_tx.send(()).unwrap();
                released_first = true;
            }
            if released_first
                && second_parked
                && health.receipt_sequence >= 2
                && health.authorization_pending
                && !released_second
            {
                second_release_tx.send(()).unwrap();
                released_second = true;
            }
            if released_second
                && market_parked
                && health.receipt_sequence >= 3
                && health.authorization_pending
                && !released_auth
            {
                early_sequence = Some(health.journal_sequence);
                auth_release_tx.send(()).unwrap();
                released_auth = true;
            }
            if released_auth
                && market_parked
                && matches!(health.entries, live::Entries::Enabled)
                && !released_market
            {
                assert_eq!(
                    health.pending_rows, 0,
                    "retained candle group must be stepped"
                );
                let prefix =
                    fs::read_to_string(fixture.scratch.path("journal/open.jsonl")).unwrap();
                assert!(!prefix.contains("\"kind\":\"written\""));
                assert!(!prefix.lines().any(|line| {
                    let row: Value = serde_json::from_str(line).unwrap();
                    row["kind"] == "ledger"
                        && row["event"]["kind"] == "signal"
                        && row["event"]["stream"]["kind"] == "quote"
                        && [QUOTE_START, QUOTE_START + 300_000].contains(
                            &row["event"]["close_time_micros"]
                                .as_i64()
                                .unwrap_or_default(),
                        )
                }));
                market_release_tx.send(()).unwrap();
                released_market = true;
            }
            recorded.exhausted() && released_market
        })
        .unwrap();
    assert!(auth_parked && released_first && released_second && released_auth && released_market);
    assert!(
        mixed_seen.load(Ordering::SeqCst),
        "early quote must share a tick with a candle"
    );
    let early_sequence = early_sequence.unwrap();
    assert!(early_sequence > 0);
    assert_eq!(runtime.health().receipt_sequence, 5);
    assert!(
        runtime
            .records()
            .iter()
            .filter_map(|record| match &record.kind {
                live::journal::RecordKind::Ledger { event } => Some(&event.kind),
                _ => None,
            })
            .all(|kind| !matches!(kind,
            binary_alpha_engine::execution::EventKind::Signal { stream, close_time_micros, .. }
            if *stream == binary_alpha_engine::config::StreamKey::quote()
                && [QUOTE_START, QUOTE_START + 300_000].contains(close_time_micros)))
    );
    assert_eq!(runtime.health().pending_rows, 0);
    assert_eq!(
        runtime
            .records()
            .iter()
            .filter(|record| matches!(&record.kind, live::journal::RecordKind::Written { .. }))
            .count(),
        1
    );
    assert!(runtime.records().iter().any(|record| matches!(&record.kind,
        live::journal::RecordKind::Ledger { event: binary_alpha_engine::execution::FinancialEvent {
            kind: binary_alpha_engine::execution::EventKind::Signal { stream, close_time_micros, command: Some(_), .. }, ..
        }} if *stream == binary_alpha_engine::config::StreamKey::quote()
            && *close_time_micros == QUOTE_START + 700_000)));
    assert_eq!(runtime.engine().accounts()[0].cash.to_string(), "10000.92");
}

#[derive(Clone)]
struct AuthorizationOwnerClock {
    replay: binary_alpha_app::broker::transport::ReplayClock,
    shift: std::sync::Arc<std::sync::atomic::AtomicI64>,
}
impl binary_alpha_app::broker::Clock for AuthorizationOwnerClock {
    fn now_micros(&self) -> i64 {
        use std::sync::atomic::Ordering;
        self.replay.now_micros() + self.shift.load(Ordering::SeqCst)
    }
    fn sleep(&mut self, micros: i64) {
        use std::sync::atomic::Ordering;
        self.shift.fetch_add(micros, Ordering::SeqCst);
    }
}

#[test]
fn deriv_authorization_read_start_discards_only_the_queued_quote() {
    use std::sync::{
        Arc,
        atomic::{AtomicBool, AtomicI64, Ordering},
        mpsc,
    };
    let fixture = Fixture::quote_deriv_with_candle("phase12-deriv-auth-read-start");
    let recorded = RecordedConnector::from_jsonl(&deriv_quote_authorization_log()).unwrap();
    let (first_parked_tx, first_parked_rx) = mpsc::channel();
    let (first_release_tx, first_release_rx) = mpsc::channel();
    let (later_parked_tx, later_parked_rx) = mpsc::channel();
    let (later_release_tx, later_release_rx) = mpsc::channel();
    let (proposal_parked_tx, proposal_parked_rx) = mpsc::channel();
    let (proposal_release_tx, proposal_release_rx) = mpsc::channel();
    let shift = Arc::new(AtomicI64::new(0));
    let owner_shift = shift.clone();
    let control = live::control::FakeControl::new(QUOTE_START - 2_000_000);
    let mut runtime = runtime_with_owner_clock(
        &fixture,
        live::Mode::Live,
        &recorded,
        Box::new(control.clone()),
        |_| {},
        move |inner| {
            Box::new(super::resilience::MarketProbe {
                inner: Box::new(super::resilience::MarketProbe {
                    inner,
                    panic: false,
                    gate: None,
                    consumed: None,
                    subscribe_gate: None,
                    frame_gate: Some((QUOTE_START + 1_000_000, later_parked_tx, later_release_rx)),
                    dropped: Arc::new(AtomicBool::new(false)),
                }),
                panic: false,
                gate: None,
                consumed: None,
                subscribe_gate: None,
                frame_gate: Some((QUOTE_START, first_parked_tx, first_release_rx)),
                dropped: Arc::new(AtomicBool::new(false)),
            })
        },
        move |inner| {
            Box::new(super::resilience::AccountProposalProbe {
                inner,
                parked: proposal_parked_tx,
                release: Some(proposal_release_rx),
            })
        },
        Some(Box::new(AuthorizationOwnerClock {
            replay: recorded.clock(),
            shift: owner_shift,
        })),
    )
    .unwrap();
    assert!(!runtime.health().authorization_pending);
    let mixed_seen = Arc::new(AtomicBool::new(false));
    let observed = mixed_seen.clone();
    runtime.feature_observer = Some(Box::new(move |instrument, output| {
        if instrument == 0
            && output.rows.iter().any(|(stream, _)| *stream == 0)
            && output.rows.iter().any(|(stream, _)| *stream == 1)
        {
            observed.store(true, Ordering::SeqCst);
        }
    }));
    let handle = runtime.authorization_probe_handle();
    let (auth_parked_tx, auth_parked_rx) = mpsc::channel();
    let (auth_release_tx, auth_release_rx) = mpsc::channel();
    let mut auth_release_rx = Some(auth_release_rx);
    let mut saw_absent_start = false;
    let mut saw_absent_end = false;
    let mut first_parked = false;
    let mut first_released = false;
    let mut proposal_parked = false;
    let mut valid_started = false;
    let mut valid_parked = false;
    let mut proposal_released = false;
    let mut valid_released = false;
    let mut later_parked = false;
    let mut later_released = false;
    let mut early_sequence = None;
    let manifest = runtime.definition.manifest.clone();
    let (local, destination) = fixture.stores();
    runtime.hook = Some(Box::new(|point| point == live::Checkpoint::BeforeClaim));
    let completed = runtime
        .run_until(|health| {
            first_parked |= first_parked_rx.try_recv().is_ok();
            proposal_parked |= proposal_parked_rx.try_recv().is_ok();
            valid_parked |= auth_parked_rx.try_recv().is_ok();
            later_parked |= later_parked_rx.try_recv().is_ok();
            if health.authorization_pending && !first_released {
                saw_absent_start = true;
            }
            if saw_absent_start && !health.authorization_pending && first_parked && !first_released
            {
                saw_absent_end = true;
                first_release_tx.send(()).unwrap();
                first_released = true;
            }
            if first_released
                && proposal_parked
                && health.pending_rows > 0
                && health.pending_proposals > 0
                && !valid_started
            {
                assert!(!health.authorization_pending);
                early_sequence = Some(health.journal_sequence);
                live::authorization::create(
                    &destination,
                    &local,
                    live::authorization::Authorization {
                        schema_version: 1,
                        deployment: manifest.hash.clone(),
                        configuration: manifest.config_hash.clone(),
                        bundle_sha256: manifest.bundle_sha256.clone(),
                        broker: "pocket_option".into(),
                        account: "a0".into(),
                        operator: "synthetic-operator".into(),
                        reason: "fixture authorization".into(),
                        hash: String::new(),
                    },
                )
                .unwrap();
                handle.install(live::AuthorizationProbe {
                    parked: auth_parked_tx.clone(),
                    release: auth_release_rx.take().unwrap(),
                });
                control.clone().advance(21_000_000);
                shift.store(21_000_000, Ordering::SeqCst);
                valid_started = true;
            }
            if valid_started && valid_parked && health.authorization_pending && !proposal_released {
                proposal_release_tx.send(()).unwrap();
                proposal_released = true;
            }
            if proposal_released && health.pending_proposals == 0 && !valid_released {
                auth_release_tx.send(()).unwrap();
                valid_released = true;
            }
            if valid_released
                && later_parked
                && matches!(health.entries, live::Entries::Enabled)
                && !later_released
            {
                assert_eq!(health.pending_rows, 0);
                let prefix = fs::read_to_string(fixture.scratch.path("journal/open.jsonl")).unwrap();
                assert!(!prefix.contains("\"kind\":\"written\""));
                assert!(!prefix.lines().any(|line| {
                    let row: Value = serde_json::from_str(line).unwrap();
                    row["kind"] == "ledger" && row["event"]["kind"] == "signal"
                        && row["event"]["stream"]["kind"] == "quote"
                        && row["event"]["close_time_micros"] == QUOTE_START
                }));
                shift.store(0, Ordering::SeqCst);
                later_release_tx.send(()).unwrap();
                later_released = true;
            }
            recorded.exhausted() && later_released && health.receipt_sequence >= 2
        })
        .unwrap_or_else(|error| panic!("{error}; start={saw_absent_start} end={saw_absent_end} first={first_released} proposal={proposal_parked} valid_started={valid_started} valid_parked={valid_parked} reply={proposal_released} grant={valid_released} later={later_parked} released={later_released} health={:?}", runtime.health()));
    assert!(completed.is_none());
    assert!(saw_absent_start && saw_absent_end && valid_parked && later_released);
    assert!(mixed_seen.load(Ordering::SeqCst));
    assert!(runtime.records().iter().all(|record| !matches!(&record.kind,
        live::journal::RecordKind::Ledger { event: binary_alpha_engine::execution::FinancialEvent {
            kind: binary_alpha_engine::execution::EventKind::Signal { stream, close_time_micros, .. }, ..
        }} if *stream == binary_alpha_engine::config::StreamKey::quote()
            && *close_time_micros == QUOTE_START)));
    assert!(runtime.records().iter().any(|record| matches!(&record.kind,
        live::journal::RecordKind::Ledger { event: binary_alpha_engine::execution::FinancialEvent {
            kind: binary_alpha_engine::execution::EventKind::Signal { stream, close_time_micros, disposition: binary_alpha_engine::execution::Disposition::Admitted, command: Some(_), .. }, ..
        }} if *stream == binary_alpha_engine::config::StreamKey::quote()
            && *close_time_micros == QUOTE_START+1_000_000)));
    assert!(early_sequence.is_some());
    assert!(
        !runtime
            .records()
            .iter()
            .any(|record| matches!(&record.kind, live::journal::RecordKind::Written { .. }))
    );
}

fn pocket_written_claim(name: &str) -> (Fixture, live::control::FakeControl, u64) {
    use live::control::{Control, LeaseKey};
    let fixture = Fixture::quote(name);
    let control = live::control::FakeControl::new(QUOTE_START - 2_000_000);
    let first = RecordedConnector::from_jsonl(&pocket_written_prefix()).unwrap();
    let mut owner = pocket_runtime(&fixture, live::Mode::Replay, &first, control.clone()).unwrap();
    owner.hook = Some(Box::new(|point| point == live::Checkpoint::DuringWrite));
    assert!(owner.run_until(|_| false).unwrap().is_none());
    drop(owner);
    let (_, prior) = live::journal::Journal::open(
        &fixture.scratch.path("journal"),
        &fixture.definition().deployment,
        16,
    )
    .unwrap();
    let ids = prior
        .iter()
        .filter_map(|record| match &record.kind {
            live::journal::RecordKind::Written { request_id, .. } => *request_id,
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(ids.len(), 1);
    let key = LeaseKey {
        broker: "pocket_option",
        account: "a0",
    };
    assert_eq!(control.clone().unresolved(key).unwrap().len(), 1);
    control.clone().advance(70_000_000);
    (fixture, control, ids[0])
}

#[test]
fn pocket_restart_matches_written_request_id_and_settles_closed_deal() {
    use live::control::{ClaimState, Control, LeaseKey};
    let (fixture, control, request_id) = pocket_written_claim("phase12-pocket-restart");
    let (_, before) = live::journal::Journal::open(
        &fixture.scratch.path("journal"),
        &fixture.definition().deployment,
        16,
    )
    .unwrap();
    let before_ledger = before
        .iter()
        .filter(|record| matches!(&record.kind, live::journal::RecordKind::Ledger { .. }))
        .count();
    let restart = RecordedConnector::from_jsonl(&pocket_restart_log(request_id)).unwrap();
    let mut restored =
        pocket_runtime(&fixture, live::Mode::Live, &restart, control.clone()).unwrap();
    let reconciled_before_deletion = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let seen = reconciled_before_deletion.clone();
    let claims = control.clone();
    restored.hook = Some(Box::new(move |point| {
        if point == live::Checkpoint::BeforeClaimDeletion {
            assert_eq!(
                claims
                    .clone()
                    .retained_claims(LeaseKey {
                        broker: "pocket_option",
                        account: "a0"
                    })
                    .unwrap()[0]
                    .state,
                ClaimState::Reconciled
            );
            seen.store(true, std::sync::atomic::Ordering::SeqCst);
        }
        false
    }));
    let completed = restored
        .run_until(|_| restart.exhausted())
        .unwrap()
        .unwrap();
    let key = LeaseKey {
        broker: "pocket_option",
        account: "a0",
    };
    assert_eq!(restored.engine().accounts()[0].cash.to_string(), "10000.92");
    assert_eq!(restored.engine().accounts()[0].open, 0);
    assert_eq!(&restored.records()[..before.len()], before.as_slice());
    assert!(
        restored
            .records()
            .iter()
            .filter(|record| matches!(&record.kind, live::journal::RecordKind::Ledger { .. }))
            .count()
            > before_ledger
    );
    assert!(
        restored
            .records()
            .windows(2)
            .all(|pair| pair[1].sequence == pair[0].sequence + 1)
    );
    assert!(
        reconciled_before_deletion.load(std::sync::atomic::Ordering::SeqCst)
            || control
                .clone()
                .retained_claims(key)
                .unwrap()
                .first()
                .is_some_and(|claim| claim.state == ClaimState::Reconciled)
    );
    assert_eq!(restored.records().iter().filter(|record| matches!(&record.kind,
        live::journal::RecordKind::Ledger { event } if matches!(event.kind, binary_alpha_engine::execution::EventKind::Settled { .. }))).count(), 1);
    assert!(completed.manifest.open_tail.is_some());
    assert_eq!(completed.receipt.broker, "pocket_option");
    assert_eq!(completed.receipt.definition, completed.manifest.definition);
    assert!(!completed.receipt.promotion.eligible);
    assert_eq!(
        fs::read(completed.manifest.receipt.strip_prefix("file://").unwrap()).unwrap(),
        completed.receipt.to_json()
    );
    assert_eq!(
        serde_json::from_slice::<live::FinalManifest>(
            &fs::read(completed.manifest_uri.strip_prefix("file://").unwrap()).unwrap()
        )
        .unwrap(),
        completed.manifest
    );
    assert!(
        completed
            .receipt
            .dimensions
            .iter()
            .any(|dimension| dimension.name == "contract_timing")
    );
}

#[test]
fn pocket_duplicate_request_id_in_statement_keeps_claim_unresolved() {
    use live::control::{ClaimState, Control, LeaseKey};
    let (fixture, control, request_id) = pocket_written_claim("phase12-pocket-duplicate-id");
    let restart = RecordedConnector::from_jsonl(&pocket_duplicate_restart_log(request_id)).unwrap();
    let mut restored =
        pocket_runtime(&fixture, live::Mode::Live, &restart, control.clone()).unwrap();
    restored.run_until(|_| restart.exhausted()).unwrap();
    let key = LeaseKey {
        broker: "pocket_option",
        account: "a0",
    };
    assert_ne!(
        control.clone().retained_claims(key).unwrap()[0].state,
        ClaimState::Reconciled
    );
    assert_eq!(restored.engine().accounts()[0].open, 1);
    assert!(
        matches!(&restored.health().entries, live::Entries::Disabled(reason) if reason.contains("unresolved"))
    );
}

#[test]
fn pocket_foreign_close_veto_survives_a_later_partial_snapshot() {
    use live::control::{ClaimState, Control, LeaseKey};
    let (fixture, control, request_id) = pocket_written_claim("phase12-pocket-sticky-foreign");
    let recorded =
        RecordedConnector::from_jsonl(&pocket_foreign_partial_restart_log(request_id)).unwrap();
    let mut runtime =
        pocket_runtime(&fixture, live::Mode::Live, &recorded, control.clone()).unwrap();
    runtime
        .run_until(|health| recorded.exhausted() && health.receipt_sequence >= 1)
        .unwrap();
    let key = LeaseKey {
        broker: "pocket_option",
        account: "a0",
    };
    assert_ne!(
        control.clone().retained_claims(key).unwrap()[0].state,
        ClaimState::Reconciled
    );
    assert!(
        matches!(&runtime.health().entries, live::Entries::Disabled(reason)
        if reason.contains("uncorrelated liability"))
    );
    assert!(runtime.records().iter().any(|record| matches!(&record.kind,
        live::journal::RecordKind::PocketCorrelation { deal_id, correlated: false }
        if deal_id == "synthetic-foreign-close")));
    assert!(
        !runtime.records().iter().any(|record| matches!(&record.kind,
        live::journal::RecordKind::Ledger { event }
        if matches!(event.kind, binary_alpha_engine::execution::EventKind::Settled { .. })))
    );
    drop(runtime);
    control.clone().advance(70_000_000);
    let omitted =
        RecordedConnector::from_jsonl(&pocket_foreign_omitting_restart_log(request_id)).unwrap();
    let mut restored =
        pocket_runtime(&fixture, live::Mode::Live, &omitted, control.clone()).unwrap();
    restored.run_until(|_| omitted.exhausted()).unwrap();
    assert!(
        matches!(&restored.health().entries, live::Entries::Disabled(reason)
        if reason.contains("uncorrelated liability"))
    );
    assert_eq!(
        restored
            .records()
            .iter()
            .filter(|record| matches!(&record.kind,
        live::journal::RecordKind::PocketCorrelation { deal_id, correlated: false }
        if deal_id == "synthetic-foreign-close"))
            .count(),
        1
    );
    drop(restored);
    control.clone().advance(70_000_000);
    let matched =
        RecordedConnector::from_jsonl(&pocket_foreign_exact_restart_log(request_id)).unwrap();
    let mut reconciled =
        pocket_runtime(&fixture, live::Mode::Live, &matched, control.clone()).unwrap();
    reconciled.run_until(|_| matched.exhausted()).unwrap();
    assert!(
        reconciled
            .records()
            .iter()
            .any(|record| matches!(&record.kind,
        live::journal::RecordKind::PocketCorrelation { deal_id, correlated: true }
        if deal_id == "synthetic-foreign-close"))
    );
    assert!(
        !matches!(&reconciled.health().entries, live::Entries::Disabled(reason)
        if reason.contains("uncorrelated liability"))
    );
}

#[test]
fn pocket_contradictory_statement_facts_keep_claim_unresolved() {
    use live::control::{ClaimState, Control, LeaseKey};
    let (fixture, control, request_id) = pocket_written_claim("phase12-pocket-contradictory");
    let restart =
        RecordedConnector::from_jsonl(&pocket_contradictory_restart_log(request_id)).unwrap();
    let mut restored =
        pocket_runtime(&fixture, live::Mode::Live, &restart, control.clone()).unwrap();
    restored.run_until(|_| restart.exhausted()).unwrap();
    let key = LeaseKey {
        broker: "pocket_option",
        account: "a0",
    };
    assert_ne!(
        control.clone().retained_claims(key).unwrap()[0].state,
        ClaimState::Reconciled
    );
    assert_eq!(restored.engine().accounts()[0].open, 1);
    assert!(
        matches!(&restored.health().entries, live::Entries::Disabled(reason) if reason.contains("unresolved"))
    );
}

#[test]
fn pocket_partial_empty_statement_does_not_infer_a_loss() {
    use live::control::{ClaimState, Control, LeaseKey};
    let (fixture, control, request_id) = pocket_written_claim("phase12-pocket-partial-empty");
    let restart = RecordedConnector::from_jsonl(&pocket_empty_restart_log(request_id)).unwrap();
    let mut restored =
        pocket_runtime(&fixture, live::Mode::Live, &restart, control.clone()).unwrap();
    restored.run_until(|_| restart.exhausted()).unwrap();
    let key = LeaseKey {
        broker: "pocket_option",
        account: "a0",
    };
    assert_ne!(
        control.clone().retained_claims(key).unwrap()[0].state,
        ClaimState::Reconciled
    );
    assert_eq!(restored.engine().accounts()[0].open, 1);
    assert!(!restored.records().iter().any(|record| matches!(&record.kind,
        live::journal::RecordKind::Ledger { event } if matches!(event.kind, binary_alpha_engine::execution::EventKind::Settled { .. }))));
}

#[test]
fn pocket_open_recovery_uses_the_journaled_deal_id() {
    use live::control::{Control, LeaseKey};
    let fixture = Fixture::quote("phase12-pocket-open-recovery");
    let control = live::control::FakeControl::new(QUOTE_START - 2_000_000);
    let first = RecordedConnector::from_jsonl(&pocket_log()).unwrap();
    let mut owner = pocket_runtime(&fixture, live::Mode::Replay, &first, control.clone()).unwrap();
    owner.hook = Some(Box::new(|point| {
        point == live::Checkpoint::AfterAcknowledgement
    }));
    assert!(owner.run_until(|_| first.exhausted()).unwrap().is_none());
    drop(owner);
    let (_, prior) = live::journal::Journal::open(
        &fixture.scratch.path("journal"),
        &fixture.definition().deployment,
        16,
    )
    .unwrap();
    let request_id = prior
        .iter()
        .find_map(|record| match &record.kind {
            live::journal::RecordKind::Written { request_id, .. } => *request_id,
            _ => None,
        })
        .unwrap();
    control.clone().advance(70_000_000);
    let recorded = RecordedConnector::from_jsonl(&pocket_open_restart_log(request_id)).unwrap();
    let mut restored =
        pocket_runtime(&fixture, live::Mode::Live, &recorded, control.clone()).unwrap();
    restored.run_until(|_| recorded.exhausted()).unwrap();
    let key = LeaseKey {
        broker: "pocket_option",
        account: "a0",
    };
    assert_eq!(control.clone().unresolved(key).unwrap().len(), 1);
    assert_eq!(restored.engine().accounts()[0].open, 1);
    assert_eq!(restored.engine().accounts()[0].cash.to_string(), "9999.00");
    assert!(
        matches!(&restored.health().entries, live::Entries::Disabled(reason) if !reason.contains("uncorrelated liability"))
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
    assert!(runtime.records().iter().any(|record| matches!(&record.kind,live::journal::RecordKind::Refused {binding:id,proposal:Some(_),reason,..} if *id == binding.id && *reason == format!("offer differs from exact baseline {}",binding.contract))));
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

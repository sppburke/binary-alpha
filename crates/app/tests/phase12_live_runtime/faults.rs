//! Process/host loss and storage faults through the actual ordered runtime.
//!
//! resilience already proves absence plus time cannot release a paused predecessor,
//! operator accepted/not_sent resolution, and recovered portfolio-prefix receipt parity.
//! These cases add destructive open-tail loss, statement-only evidence, the write
//! checkpoints, interrupted archival, and the real-control reuse below.
use super::support::{self, Fixture, START, change, frame, matching_log};
use binary_alpha_app::{
    broker::{self, Clock, transport::RecordedConnector},
    live::{
        self, Checkpoint, Entries,
        control::{Claim, ClaimOutcome, ClaimState, Control, FakeControl, LeaseKey},
        journal::{Journal, Record, RecordKind},
    },
};
use binary_alpha_engine::execution::{
    Block, BrokerLiability, CashAction, Decimal, Disposition, Engine, EventKind, EventSource,
    FinancialEvent, Resolution,
};
use serde_json::{Value, json};
use std::{fs, sync::Arc};

fn rows(log: &str) -> Vec<Value> {
    log.lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect()
}
fn log(rows: &[Value]) -> String {
    rows.iter().map(|row| format!("{row}\n")).collect()
}
fn account(at: i64, frame: Value) -> Value {
    json!({"session":"account","at":at,"frame":frame.to_string()})
}
fn portfolio(at: i64) -> Value {
    account(
        at,
        json!({"msg_type":"portfolio","req_id":71,"portfolio":{"contracts":[]}}),
    )
}
fn statement(at: i64, purchased: bool) -> Value {
    let transactions = if purchased {
        json!([{"action_type":"buy","amount":-10,"contract_id":12859891379u64,
            "transaction_id":24655144239u64,"transaction_time":START/1_000_000,
            "payout":18.83,"underlying_symbol":"R_50","contract_type":"CALL"}])
    } else {
        json!([])
    };
    account(
        at,
        json!({"msg_type":"statement","req_id":72,"statement":{
        "count":transactions.as_array().unwrap().len(),"transactions":transactions}}),
    )
}
fn startup(at: i64, reconcile: bool, purchased: bool, cash: &str) -> Vec<Value> {
    let source = rows(&matching_log());
    let mut input = source[..2].to_vec();
    for row in &mut input {
        row["at"] = json!(at);
    }
    input.push(portfolio(at));
    if reconcile {
        input.push(statement(at, purchased));
    }
    input.push(account(
        at,
        serde_json::from_str(&frame("transaction-ack")).unwrap(),
    ));
    input.push(account(
        at,
        serde_json::from_str(&change(
            &frame("balance-before"),
            "balance",
            "balance",
            cash,
        ))
        .unwrap(),
    ));
    input
}
fn tick(at: i64, price: &str) -> Value {
    let mut row = rows(&matching_log())[4].clone();
    row["at"] = json!(at);
    row["frame"] = json!(change(
        &change(
            row["frame"].as_str().unwrap(),
            "tick",
            "epoch",
            &(at / 1_000_000).to_string()
        ),
        "tick",
        "quote",
        price
    ));
    row
}
fn proposal(at: i64) -> Value {
    let mut row = rows(&matching_log())[5].clone();
    row["at"] = json!(at);
    let mut text = row["frame"]
        .as_str()
        .unwrap()
        .replace("fixture-id-2", &format!("proposal-{at}"));
    for (field, time) in [
        ("spot_time", at),
        ("date_start", at),
        ("date_expiry", at + 5_000_000),
    ] {
        text = change(&text, "proposal", field, &(time / 1_000_000).to_string());
    }
    row["frame"] = json!(crate::common::broker::replace(
        &text,
        "req_id",
        &((at - START) as u64 + 100).to_string()
    ));
    row
}
fn key(fixture: &Fixture) -> LeaseKey<'_> {
    LeaseKey {
        broker: "deriv",
        account: &fixture.config.live.as_ref().unwrap().account,
    }
}
fn decimal(value: &str) -> Decimal {
    Decimal::parse(value).unwrap()
}
fn ledger(owner: &live::Runtime) -> Vec<FinancialEvent> {
    owner
        .records()
        .iter()
        .filter_map(|record| match &record.kind {
            RecordKind::Ledger { event } => Some(event.clone()),
            _ => None,
        })
        .collect()
}
fn kinds(owner: &live::Runtime) -> Vec<String> {
    ledger(owner)
        .iter()
        .map(|event| {
            serde_json::to_value(event).unwrap()["kind"]
                .as_str()
                .unwrap()
                .into()
        })
        .collect()
}
fn buys(recorded: &RecordedConnector) -> usize {
    recorded
        .writes()
        .iter()
        .filter(|(_, text)| text.contains("\"buy\":"))
        .count()
}
fn balances(owner: &live::Runtime, cash: &str, reserved: &str, paid: &str, loss: &str, open: u32) {
    let state = &owner.engine().accounts()[0];
    assert_eq!(state.cash, decimal(cash).rescale(2).unwrap());
    assert_eq!(state.reserved, decimal(reserved).rescale(2).unwrap());
    assert_eq!(state.paid_basis, decimal(paid).rescale(2).unwrap());
    assert_eq!(state.unresolved_loss, decimal(loss).rescale(2).unwrap());
    assert_eq!(state.open, open);
    assert_eq!(owner.health().risk, *state);
    assert_eq!(owner.health().open_commands, open);
}
fn authorize(fixture: &Fixture) {
    let definition = fixture.definition();
    let manifest = definition.manifest;
    let (local, destination) = fixture.stores();
    live::authorization::create(
        &destination,
        &local,
        live::authorization::Authorization {
            schema_version: 1,
            deployment: manifest.hash,
            configuration: manifest.config_hash,
            bundle_sha256: manifest.bundle_sha256,
            broker: manifest.broker,
            account: manifest.account,
            operator: "synthetic-fault-operator".into(),
            reason: "deterministic fake broker fixture".into(),
            hash: String::new(),
        },
    )
    .unwrap();
}
fn run(
    fixture: &Fixture,
    mode: live::Mode,
    recorded: &RecordedConnector,
    control: Box<dyn Control>,
) -> live::Runtime {
    support::runtime_with(fixture, mode, recorded, control, |_| {}, |m| m).unwrap()
}
fn stop_at(owner: &mut live::Runtime, point: Checkpoint) {
    owner.hook = Some(Box::new(move |at| at == point));
    assert!(owner.run_until(|_| false).unwrap().is_none());
    assert!(owner.interrupted());
}
struct WriteFaultConnector(Box<dyn broker::transport::Connector>);
struct WriteFaultTransport(Box<dyn broker::transport::Transport>);
impl broker::transport::Connector for WriteFaultConnector {
    fn connect(
        &mut self,
        url: &str,
        headers: &[(String, String)],
    ) -> Result<Box<dyn broker::transport::Transport>, String> {
        Ok(Box::new(WriteFaultTransport(self.0.connect(url, headers)?)))
    }
}
impl broker::transport::Transport for WriteFaultTransport {
    fn send(&mut self, frame: broker::transport::Frame) -> Result<(), String> {
        let buy =
            matches!(&frame, broker::transport::Frame::Text(text) if text.contains("\"buy\":"));
        self.0.send(frame)?;
        if buy {
            Err("synthetic loss during socket write".into())
        } else {
            Ok(())
        }
    }
    fn receive(&mut self, timeout: i64) -> Result<Option<broker::transport::Frame>, String> {
        self.0.receive(timeout)
    }
    fn close(&mut self) -> Result<(), String> {
        self.0.close()
    }
}
fn assert_claim(
    claim: &Claim,
    signal: &FinancialEvent,
    deployment: &str,
    token: u64,
    state: ClaimState,
) {
    let EventKind::Signal {
        command: Some(command),
        proposal: Some(proposal),
        reservation: Some(reservation),
        disposition,
        ..
    } = &signal.kind
    else {
        panic!("admitted signal required")
    };
    assert_eq!(*disposition, Disposition::Admitted);
    assert_eq!(*reservation, decimal("10.00"));
    assert_eq!(proposal.terms.quoted_cost, decimal("10"));
    assert_eq!(proposal.terms.win.gross_return, decimal("18.83"));
    assert_eq!(claim.command, *command);
    assert_eq!(claim.claim, format!("{deployment}:{command}"));
    assert_eq!(claim.signal, *signal);
    assert_eq!(claim.deployment, deployment);
    assert_eq!(claim.token, token);
    assert_eq!(claim.max_proposal_age_micros, 1_000_000);
    assert_eq!(claim.state, state);
    assert_eq!(
        claim.contract_ref.as_deref(),
        (state == ClaimState::Accepted).then_some("12859891379")
    );
    assert_eq!(
        claim.transaction_ref.as_deref(),
        (state == ClaimState::Accepted).then_some("24655144239")
    );
}

/// Shared fake/Postgres scenario: real checkpoint, destroyed tail, ambiguous takeover,
/// statement-only purchase recovery, and once-only terminal release.
pub fn checkpoint_recovery_scenario(
    fixture: &Fixture,
    first: Box<dyn Control>,
    second: Box<dyn Control>,
    inspect: &mut dyn Control,
    point: Checkpoint,
) -> Claim {
    assert_eq!(inspect.unresolved(key(fixture)).unwrap(), []);
    let recorded = RecordedConnector::from_jsonl(&matching_log()).unwrap();
    let mut predecessor = support::runtime_with_io(
        fixture,
        live::Mode::Replay,
        &recorded,
        first,
        |_| {},
        |m| m,
        |inner| {
            if point == Checkpoint::DuringWrite {
                Box::new(WriteFaultConnector(inner))
            } else {
                inner
            }
        },
    )
    .unwrap();
    // DuringWrite itself precedes send. Inject the loss inside Transport::send after
    // recording the attempted frame, then stop before applying its uncertain outcome.
    stop_at(
        &mut predecessor,
        if point == Checkpoint::DuringWrite {
            Checkpoint::AfterWriteBeforeAcknowledgement
        } else {
            point
        },
    );
    let events = ledger(&predecessor);
    let signal = events
        .iter()
        .find(|e| {
            matches!(
                e.kind,
                EventKind::Signal {
                    command: Some(_),
                    ..
                }
            )
        })
        .unwrap()
        .clone();
    let acknowledged = point == Checkpoint::AfterAcknowledgement;
    let original = inspect.unresolved(key(fixture)).unwrap().remove(0);
    assert_claim(
        &original,
        &signal,
        &predecessor.definition.deployment,
        predecessor.health().fencing_token,
        if acknowledged {
            ClaimState::Accepted
        } else {
            ClaimState::Claimed
        },
    );
    let expected_buys = usize::from(point != Checkpoint::AfterClaimBeforeWrite);
    assert_eq!(buys(&recorded), expected_buys);
    balances(
        &predecessor,
        if acknowledged { "9990" } else { "10000" },
        if acknowledged { "0" } else { "10" },
        if acknowledged { "10" } else { "0" },
        "10",
        1,
    );
    assert_eq!(
        kinds(&predecessor),
        if acknowledged {
            vec!["run_definition", "signal", "accepted"]
        } else {
            vec!["run_definition", "signal"]
        }
    );
    assert_eq!(
        predecessor
            .records()
            .iter()
            .filter(|r| matches!(r.kind, RecordKind::Claimed { .. }))
            .count(),
        1
    );
    assert_eq!(
        predecessor
            .records()
            .iter()
            .filter(|r| matches!(r.kind, RecordKind::Written { .. }))
            .count(),
        usize::from(point != Checkpoint::AfterClaimBeforeWrite)
    );
    let token = predecessor.health().fencing_token;
    drop(predecessor);
    // Only the invented fixture's unuploaded open segment is destroyed.
    assert_eq!(
        fs::read_dir(fixture.scratch.path("journal"))
            .unwrap()
            .filter(|e| e
                .as_ref()
                .unwrap()
                .file_name()
                .to_string_lossy()
                .ends_with(".uploaded"))
            .count(),
        0
    );
    fs::remove_file(fixture.scratch.path("journal/open.jsonl")).unwrap();
    assert!(
        inspect
            .release(key(fixture), "synthetic-owner", token)
            .unwrap()
    );

    // First takeover sees no purchase evidence. Authorization is absent and the durable
    // command must still reconstruct one possibly-sent reservation without any buy.
    let input = startup(START, true, false, "10000");
    let restart = RecordedConnector::from_jsonl(&log(&input)).unwrap();
    let mut successor = run(fixture, live::Mode::Live, &restart, second);
    balances(&successor, "10000", "10", "0", "10", 1);
    assert_eq!(successor.health().fencing_token, token + 1);
    assert_eq!(successor.health().uncertain_commands, 1);
    assert_eq!(
        successor.engine().accounts()[0].blocked,
        [(original.command.clone(), Block::PossiblySent)].into()
    );
    assert!(
        matches!(&successor.health().entries, Entries::Disabled(r) if r.contains("live authorization is absent") && r.contains("unresolved dispatch claim"))
    );
    assert_eq!(buys(&restart), 0);
    assert_eq!(
        kinds(&successor),
        ["run_definition", "signal", "possibly_sent"]
    );
    assert_eq!(ledger(&successor)[1], signal);
    let mut ambiguous = original.clone();
    ambiguous.state = ClaimState::PossiblySent;
    assert_eq!(
        inspect.unresolved(key(fixture)).unwrap(),
        [ambiguous.clone()]
    );
    // Normal finish of an unresolved owner uploads its evidence but must retain the row.
    successor
        .run_until(|_| restart.exhausted())
        .unwrap()
        .unwrap();
    assert_eq!(inspect.unresolved(key(fixture)).unwrap(), [ambiguous]);
    drop(successor);
    original
}

/// Statement purchase recovery is a distinct continuation shared with postgres_control.
pub fn statement_recovery_scenario(
    fixture: &Fixture,
    control: Box<dyn Control>,
    inspect: &mut dyn Control,
    original: &Claim,
) {
    // Preserve completed observation-only publication so the extended ledger has a new
    // local fixture publication surface; journal/cloud recovery evidence remains intact.
    preserve_ledger_manifest(fixture, "ambiguous");
    let source = rows(&matching_log());
    let mut input = startup(START, true, true, "9990");
    input.extend([source[4].clone(), source[7].clone(), source[5].clone()]);
    input.extend_from_slice(&source[8..]);
    let recorded = RecordedConnector::from_jsonl(&log(&input)).unwrap();
    let mut owner = run(fixture, live::Mode::Live, &recorded, control);
    balances(&owner, "9990", "0", "10", "10", 1);
    let mut accepted = original.clone();
    accepted.state = ClaimState::Accepted;
    accepted.contract_ref = Some("12859891379".into());
    accepted.transaction_ref = Some("24655144239".into());
    assert_eq!(inspect.unresolved(key(fixture)).unwrap(), [accepted]);
    let purchased = ledger(&owner)
        .into_iter()
        .find(|e| {
            matches!(
                e.kind,
                EventKind::Reconciled {
                    resolution: Resolution::Purchased { .. },
                    ..
                }
            )
        })
        .unwrap();
    assert_eq!(
        purchased.kind,
        EventKind::Reconciled {
            command: original.command.clone(),
            source: EventSource {
                id: format!("recovery-purchase:{}", original.claim),
                provider_time_micros: START,
                available_at_micros: START,
                simulated: false
            },
            resolution: Resolution::Purchased {
                debit: decimal("10"),
                liability: BrokerLiability {
                    contract_ref: "12859891379".into(),
                    transaction_ref: "24655144239".into(),
                    purchase_time_micros: START,
                    expected_start_micros: None,
                    payout: decimal("18.83")
                }
            },
            release: decimal("10.00"),
            debit: decimal("10.00"),
            credit: decimal("0.00"),
            profit: None,
        }
    );
    // Purchased reconciliation owns the buy transaction, so its statement cash copy is a no-op.
    assert_eq!(ledger(&owner).iter().filter(|e| matches!(e.kind, EventKind::CashObserved { ref fact, .. } if fact.action == CashAction::Buy)).count(), 0);
    assert_eq!(buys(&recorded), 0);
    assert!(
        matches!(&owner.health().entries, Entries::Disabled(r) if r.contains("live authorization is absent") && !r.contains("unresolved dispatch claim"))
    );
    let completed = owner.run_until(|_| recorded.exhausted()).unwrap().unwrap();
    balances(&owner, "10008.83", "0", "0", "0", 0);
    assert_eq!(
        owner.engine().accounts()[0].completed_profit,
        decimal("8.83")
    );
    assert_eq!(
        kinds(&owner),
        [
            "run_definition",
            "signal",
            "possibly_sent",
            "reconciled",
            "confirmed",
            "cash_observed",
            "unresolved",
            "settled"
        ]
    );
    assert_eq!(buys(&recorded), 0);
    assert_eq!(inspect.unresolved(key(fixture)).unwrap(), []);
    assert_eq!(
        completed
            .receipt
            .dimensions
            .iter()
            .map(|d| d.samples)
            .collect::<Vec<_>>(),
        [1; 6]
    );
    assert_eq!(
        completed
            .receipt
            .dimensions
            .iter()
            .map(|d| d.status)
            .collect::<Vec<_>>(),
        [live::receipt::Status::Matched; 6]
    );
    let restored = Engine::restore(ledger(&owner).iter().map(|e| Ok(e.to_line()))).unwrap();
    assert_eq!(restored.accounts(), owner.engine().accounts());
}
fn preserve_ledger_manifest(fixture: &Fixture, name: &str) {
    let id = fixture.definition().manifest.definition;
    for store in ["local", "published"] {
        let path = fixture.scratch.path(store).join("manifests").join(&id);
        if path.exists() {
            fs::rename(
                &path,
                fixture
                    .scratch
                    .path(&format!("{name}-{store}-ledger-manifest")),
            )
            .unwrap();
        }
    }
}

#[test]
fn loss_after_claim_before_write_reconstructs_exposure() {
    let mut fixture = Fixture::new("t1-claim-tail-loss");
    fixture
        .config
        .live
        .as_mut()
        .unwrap()
        .journal
        .segment_records = 1024;
    let mut control = FakeControl::new(START);
    let claim = checkpoint_recovery_scenario(
        &fixture,
        Box::new(control.clone()),
        Box::new(control.clone()),
        &mut control,
        Checkpoint::AfterClaimBeforeWrite,
    );
    statement_recovery_scenario(&fixture, Box::new(control.clone()), &mut control, &claim);
    // Variant (c) is proved by resilience::predecessor_pause_survives_absence_and_time_until_operator_not_sent.
    // It intentionally requires an operator row update, never elapsed-time NotSent inference.
}

#[test]
fn loss_during_write_after_write_and_after_acknowledgement() {
    for (name, point) in [
        ("during", Checkpoint::DuringWrite),
        ("written", Checkpoint::AfterWriteBeforeAcknowledgement),
        ("acknowledged", Checkpoint::AfterAcknowledgement),
    ] {
        let mut fixture = Fixture::new(&format!("t1-loss-{name}"));
        fixture
            .config
            .live
            .as_mut()
            .unwrap()
            .journal
            .segment_records = 1024;
        let mut control = FakeControl::new(START);
        let claim = checkpoint_recovery_scenario(
            &fixture,
            Box::new(control.clone()),
            Box::new(control.clone()),
            &mut control,
            point,
        );
        statement_recovery_scenario(&fixture, Box::new(control.clone()), &mut control, &claim);
    }
}

#[test]
fn loss_before_claim_commit_writes_nothing_and_releases() {
    let fixture = Fixture::new("t1-before-claim");
    let mut control = FakeControl::new(START);
    let recorded = RecordedConnector::from_jsonl(&matching_log()).unwrap();
    let mut owner = run(
        &fixture,
        live::Mode::Replay,
        &recorded,
        Box::new(control.clone()),
    );
    stop_at(&mut owner, Checkpoint::BeforeClaim);
    let sent = recorded.writes();
    assert_eq!(buys(&recorded), 0);
    assert_eq!(sent.len(), 6);
    assert_eq!(control.unresolved(key(&fixture)).unwrap(), []);
    assert_eq!(kinds(&owner), ["run_definition", "signal"]);
    balances(&owner, "10000", "10", "0", "10", 1);
    let command = match &ledger(&owner)[1].kind {
        EventKind::Signal {
            command: Some(c), ..
        } => c.clone(),
        _ => unreachable!(),
    };
    let warmup = owner.features()[0].profile().observations - 1;
    let token = owner.health().fencing_token;
    drop(owner);
    assert!(
        control
            .release(key(&fixture), "synthetic-owner", token)
            .unwrap()
    );
    authorize(&fixture);
    let mut input = startup(START, false, false, "10000");
    input.extend([tick(START, "180.0000"), proposal(START)]);
    for second in 1..20 {
        input.push(tick(START + second * 1_000_000, "180.0001"));
    }
    input.extend([
        tick(START + 20_000_000, "180.0000"),
        proposal(START + 20_000_000),
    ]);
    let mut buy = rows(&matching_log())[6].clone();
    buy["at"] = json!(START + 20_000_000);
    let mut text = buy["frame"].as_str().unwrap().to_owned();
    for field in ["purchase_time", "start_time"] {
        text = change(&text, "buy", field, &(START / 1_000_000 + 20).to_string());
    }
    buy["frame"] = json!(text);
    input.push(buy);
    let restarted = RecordedConnector::from_jsonl(&log(&input)).unwrap();
    let mut owner = run(
        &fixture,
        live::Mode::Live,
        &restarted,
        Box::new(control.clone()),
    );
    assert_eq!(owner.features()[0].profile().observations, warmup);
    assert!(!owner.health().warmup);
    balances(&owner, "10000", "0", "0", "0", 0);
    assert_eq!(
        ledger(&owner)[2].kind,
        EventKind::Released {
            command: command.clone(),
            source: EventSource {
                id: format!("recovery-before-claim:{command}"),
                provider_time_micros: START,
                available_at_micros: START,
                simulated: false
            },
            rejected: false,
            release: decimal("10.00"),
        }
    );
    assert_eq!(owner.records().iter().filter(|r| matches!(&r.kind, RecordKind::Discontinuity {reason} if reason == "restart: restore financial state and rebuild causal history")).count(), 1);
    stop_at(&mut owner, Checkpoint::AfterAcknowledgement);
    assert!(owner.health().warmup);
    assert_eq!(owner.health().entries, Entries::Enabled);
    assert_eq!(owner.features()[0].profile().observations, warmup + 21);
    assert_eq!(recorded.writes(), sent);
    assert_eq!(buys(&restarted), 1);
    balances(&owner, "9990", "0", "10", "10", 1);
    let claims = control.unresolved(key(&fixture)).unwrap();
    assert_eq!(claims.len(), 1);
    assert_eq!(claims[0].command, format!("d0/{}", START + 20_000_000));
    assert_eq!(claims[0].state, ClaimState::Accepted);
    assert_eq!(
        kinds(&owner),
        ["run_definition", "signal", "released", "signal", "accepted"]
    );
}

#[derive(Clone)]
struct OffsetClock {
    recorded: broker::transport::ReplayClock,
    offset: crate::common::broker::FakeClock,
}
impl Clock for OffsetClock {
    fn now_micros(&self) -> i64 {
        self.recorded.now_micros() + self.offset.now_micros()
    }
    fn sleep(&mut self, micros: i64) {
        self.offset.sleep(micros);
    }
}

#[test]
fn rate_wait_expiry_is_detected_before_claim() {
    let mut fixture = Fixture::new("t1-rate-expiry");
    let binary_alpha_engine::config::Broker::Deriv(settings) = &mut fixture.config.brokers[0]
    else {
        unreachable!()
    };
    settings.budgets = Some(binary_alpha_engine::config::RateBudgets {
        trade: binary_alpha_engine::config::RateLimit {
            per_minute: 1,
            per_hour: 100,
        },
        ..Default::default()
    });
    let settings = &mut fixture.config.live.as_mut().unwrap().control;
    settings.lease_ttl_micros = 600_000_000;
    settings.renewal_interval_micros = 100_000_000;
    let recorded = RecordedConnector::from_jsonl(&log(&rows(&matching_log())[..6])).unwrap();
    let clock = OffsetClock {
        recorded: recorded.clock(),
        offset: crate::common::broker::FakeClock::at(0),
    };
    let mut control = FakeControl::new(START);
    let mut owner = support::runtime_with_test_clock(
        &fixture,
        live::Mode::Replay,
        &recorded,
        Box::new(control.clone()),
        |_| {},
        |m| m,
        |c| c,
        clock.clone(),
    )
    .unwrap();
    // Stop at the first committed row so a defective implementation cannot hide it
    // through subsequent NotSent reconciliation and archival deletion.
    owner.hook = Some(Box::new(|p| p == Checkpoint::AfterClaimBeforeWrite));
    let result = owner.run_until(|_| recorded.exhausted()).unwrap();
    assert_eq!(clock.now_micros() - START, 60_000_000);
    assert_eq!(buys(&recorded), 0);
    assert_eq!(recorded.writes().len(), 6);
    // PRIMARY: Runtime::prepared commits the claim before checking proposal age.
    // One proposal uses the 1/minute trade budget; purchase preparation advances
    // 60s against a 1s proposal bound, yet a Claimed row and Claimed journal record
    // already exist. The age check must precede control.claim and release via NotSent.
    assert_eq!(
        control.unresolved(key(&fixture)).unwrap(),
        [],
        "rate-expired preparation must not commit a dispatch claim"
    );
    assert!(result.is_some());
    assert_eq!(kinds(&owner), ["run_definition", "signal", "released"]);
    balances(&owner, "10000", "0", "0", "0", 0);
}

#[test]
fn pause_after_final_eligibility_check_keeps_claim_possibly_sent() {
    let fixture = Fixture::two("t1-final-check-pause");
    let recorded = RecordedConnector::from_jsonl(&support::two_matching_log()).unwrap();
    let clock = OffsetClock {
        recorded: recorded.clock(),
        offset: crate::common::broker::FakeClock::at(0),
    };
    let mut control = FakeControl::new(START);
    let mut owner = support::runtime_with_test_clock(
        &fixture,
        live::Mode::Replay,
        &recorded,
        Box::new(control.clone()),
        |_| {},
        |m| m,
        |c| c,
        clock.clone(),
    )
    .unwrap();
    let mut pause = clock.clone();
    owner.hook = Some(Box::new(move |point| {
        if point == Checkpoint::DuringWrite {
            pause.sleep(1_000_001);
        }
        point == Checkpoint::AfterWriteBeforeAcknowledgement
    }));
    assert!(owner.run_until(|_| false).unwrap().is_none());
    assert_eq!(clock.now_micros(), START + 1_000_001);
    assert_eq!(buys(&recorded), 1);
    let claims = control.unresolved(key(&fixture)).unwrap();
    assert_eq!(claims.len(), 1);
    assert_eq!(claims[0].state, ClaimState::Claimed);
    assert_eq!(claims[0].max_proposal_age_micros, 1_000_000);
    assert_eq!(claims[0].contract_ref, None);
    assert_eq!(claims[0].transaction_ref, None);
    let events = ledger(&owner);
    let dispositions = events
        .iter()
        .filter_map(|e| match &e.kind {
            EventKind::Signal {
                binding,
                disposition,
                ..
            } => Some((binding.clone(), *disposition)),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(
        dispositions,
        fixture
            .definition()
            .policy
            .replay
            .bindings
            .iter()
            .map(|b| (b.id.clone(), Disposition::Admitted))
            .collect::<Vec<_>>()
    );
    assert_eq!(kinds(&owner), ["run_definition", "signal", "signal"]);
    balances(&owner, "10000", "20", "0", "20", 2);
    assert_eq!(owner.health().uncertain_commands, 1);
    assert!(
        matches!(&owner.health().entries, Entries::Disabled(r) if r.contains("unresolved dispatch claim"))
    );
    // The second admitted signal remains deferred; no second claim or write is issued
    // while the first write's outcome has not reached the owner.
    assert_eq!(
        owner
            .records()
            .iter()
            .filter(|r| matches!(r.kind, RecordKind::Written { .. }))
            .count(),
        1
    );
}

struct BreakMarket {
    inner: Box<dyn broker::MarketDataBroker>,
    at: i64,
}
impl broker::MarketDataBroker for BreakMarket {
    fn discover(&mut self) -> Result<Vec<broker::DiscoveredInstrument>, String> {
        self.inner.discover()
    }
    fn history_page(
        &mut self,
        id: &binary_alpha_engine::market::InstrumentId,
        scale: binary_alpha_engine::market::PriceScale,
        before: Option<i64>,
    ) -> Result<broker::HistoryPage, String> {
        self.inner.history_page(id, scale, before)
    }
    fn subscribe(
        &mut self,
        id: &binary_alpha_engine::market::InstrumentId,
        scale: binary_alpha_engine::market::PriceScale,
    ) -> Result<(), String> {
        self.inner.subscribe(id, scale)
    }
    fn next_live(&mut self, timeout: i64) -> Result<Option<broker::LiveEvent>, String> {
        let event = self.inner.next_live(timeout)?;
        if matches!(&event, Some(broker::LiveEvent::Observation(e)) if e.provider_time_micros == self.at)
        {
            Ok(Some(broker::LiveEvent::Break {
                generation: self.inner.continuity().generation(),
                reason: "synthetic market disconnect".into(),
            }))
        } else {
            Ok(event)
        }
    }
    fn unsubscribe(
        &mut self,
        id: &binary_alpha_engine::market::InstrumentId,
    ) -> Result<broker::Cancellation, String> {
        self.inner.unsubscribe(id)
    }
    fn reconnect(&mut self) -> Result<(), String> {
        self.inner.reconnect()
    }
    fn continuity(&self) -> &broker::Continuity {
        self.inner.continuity()
    }
}

#[test]
fn disconnect_and_incomplete_warmup_disable_entries_but_not_observation() {
    let fixture = Fixture::new("t1-disconnect-unready");
    let mut control = FakeControl::new(START);
    let recorded = RecordedConnector::from_jsonl(&matching_log()).unwrap();
    let mut predecessor = run(
        &fixture,
        live::Mode::Replay,
        &recorded,
        Box::new(control.clone()),
    );
    stop_at(&mut predecessor, Checkpoint::AfterAcknowledgement);
    let token = predecessor.health().fencing_token;
    drop(predecessor);
    assert!(
        control
            .release(key(&fixture), "synthetic-owner", token)
            .unwrap()
    );
    let source = rows(&matching_log());
    let mut input = startup(START, true, false, "9990");
    input.push(source[7].clone());
    input.extend_from_slice(&source[8..]);
    let recorded = RecordedConnector::from_jsonl(&log(&input)).unwrap();
    let mut owner = support::runtime_with(
        &fixture,
        live::Mode::Live,
        &recorded,
        Box::new(control.clone()),
        |_| {},
        |inner| {
            Box::new(BreakMarket {
                inner,
                at: START + 5_000_000,
            })
        },
    )
    .unwrap();
    assert!(!owner.health().warmup);
    balances(&owner, "9990", "0", "10", "10", 1);
    let observed = owner.features()[0].profile().observations;
    let completed = owner.run_until(|_| recorded.exhausted()).unwrap().unwrap();
    assert_eq!(owner.features()[0].profile().observations, observed);
    assert!(!owner.health().warmup);
    assert!(
        matches!(&owner.health().entries, Entries::Disabled(r) if r.contains("market continuity") && r.contains("causal warmup is incomplete"))
    );
    assert_eq!(owner.records().iter().filter(|r| matches!(&r.kind, RecordKind::Discontinuity { reason } if reason == "synthetic market disconnect")).count(), 1);
    balances(&owner, "10008.83", "0", "0", "0", 0);
    assert_eq!(
        ledger(&owner)
            .iter()
            .filter(|e| matches!(e.kind, EventKind::Settled { .. }))
            .count(),
        1
    );
    assert_eq!(buys(&recorded), 0);
    assert_eq!(control.unresolved(key(&fixture)).unwrap(), []);
    assert_eq!(
        completed.receipt.dimensions[4].status,
        live::receipt::Status::Unavailable
    );
    assert_eq!(
        completed.receipt.dimensions[5].status,
        live::receipt::Status::Matched
    );

    let mut short = Fixture::new("t1-one-tick-warmup");
    support::short_warmup(&mut short);
    let mut input = startup(START, false, false, "10000");
    input.push(tick(START, "180.0000"));
    let recorded = RecordedConnector::from_jsonl(&log(&input)).unwrap();
    let mut control = FakeControl::new(START);
    let mut owner = run(
        &short,
        live::Mode::Live,
        &recorded,
        Box::new(control.clone()),
    );
    assert_eq!(owner.features()[0].profile().observations, 1);
    let completed = owner.run_until(|_| recorded.exhausted()).unwrap().unwrap();
    assert_eq!(owner.features()[0].profile().observations, 2);
    assert!(!owner.health().warmup);
    assert!(
        matches!(&owner.health().entries, Entries::Disabled(r) if r.contains("causal warmup is incomplete"))
    );
    balances(&owner, "10000", "0", "0", "0", 0);
    assert_eq!(buys(&recorded), 0);
    assert_eq!(control.unresolved(key(&short)).unwrap(), []);
    assert_eq!(
        completed.receipt.dimensions[0].status,
        live::receipt::Status::Unavailable
    );
}

#[test]
fn duplicate_and_reordered_account_messages_are_idempotent() {
    let fixture = Fixture::new("t1-terminal-before-cash");
    let source = rows(&matching_log());
    let mut input = source[..9].to_vec();
    input.extend([source[10].clone(), source[10].clone()]);
    let mut cash = source[9].clone();
    cash["at"] = json!(START + 6_000_000);
    input.extend([cash.clone(), cash, source[10].clone()]);
    // Repeated terminal retains its source/receipt, but delivery follows the later cash.
    input.last_mut().unwrap()["at"] = json!(START + 6_000_000);
    let recorded = RecordedConnector::from_jsonl(&log(&input)).unwrap();
    let control = FakeControl::new(START);
    let mut owner = run(
        &fixture,
        live::Mode::Replay,
        &recorded,
        Box::new(control.clone()),
    );
    let mut awaiting_cash = false;
    let completed = owner
        .run_until(|health| {
            if health.risk.unresolved_loss == decimal("10.00")
                && health.risk.cash == decimal("9990.00")
                && health.uncertain_commands == 1
            {
                awaiting_cash = true;
                assert_eq!(health.risk.cash, decimal("9990.00"));
                assert_eq!(health.risk.paid_basis, decimal("10.00"));
                assert_eq!(health.open_commands, 1);
            }
            recorded.exhausted()
        })
        .unwrap()
        .unwrap();
    assert!(awaiting_cash);
    balances(&owner, "10008.83", "0", "0", "0", 0);
    let events = ledger(&owner);
    assert_eq!(
        events
            .iter()
            .filter(|e| matches!(e.kind, EventKind::CashObserved { .. }))
            .count(),
        1
    );
    let settled = events
        .iter()
        .filter(|e| matches!(e.kind, EventKind::Settled { .. }))
        .collect::<Vec<_>>();
    assert_eq!(settled.len(), 1);
    assert_eq!(settled[0].time_micros, START + 6_000_000);
    let command = match &settled[0].kind {
        EventKind::Settled {
            command,
            credit,
            profit,
            ..
        } => {
            assert_eq!(*credit, decimal("18.83"));
            assert_eq!(*profit, decimal("8.83"));
            command.clone()
        }
        _ => unreachable!(),
    };
    assert_eq!(
        completed.receipt.dimensions[5].status,
        live::receipt::Status::OutsideEnvelope
    );
    assert_eq!(
        completed.receipt.dimensions[5].reason,
        Some(format!(
            "{command}: expiry_to_evidence=1000000; evidence_to_application=0; total=1000000 microseconds"
        ))
    );
    assert_eq!(buys(&recorded), 1);
    let state = owner.engine().accounts().to_vec();
    drop(owner);
    let mut restart = startup(START + 6_000_000, false, false, "10008.83");
    let mut terminal = source[10].clone();
    terminal["at"] = json!(START + 6_000_000);
    restart.extend([terminal.clone(), terminal]);
    let mut cash = source[9].clone();
    cash["at"] = json!(START + 6_000_000);
    restart.push(cash);
    let recorded = RecordedConnector::from_jsonl(&log(&restart)).unwrap();
    let mut restored = run(&fixture, live::Mode::Live, &recorded, Box::new(control));
    let resumed = restored
        .run_until(|_| recorded.exhausted())
        .unwrap()
        .unwrap();
    assert_eq!(ledger(&restored), events);
    assert_eq!(restored.engine().accounts(), state);
    assert_eq!(resumed.receipt.to_json(), completed.receipt.to_json());
    assert_eq!(buys(&recorded), 0);
}

fn spool_bytes(fixture: &Fixture) -> u64 {
    fs::read_dir(fixture.scratch.path("journal"))
        .unwrap()
        .map(|e| e.unwrap().path())
        .filter(|p| p.extension().is_some_and(|ext| ext == "jsonl"))
        .map(|p| fs::metadata(p).unwrap().len())
        .sum()
}
fn journal_bytes(records: &[Record]) -> Vec<u8> {
    records
        .iter()
        .flat_map(|r| {
            let mut bytes = serde_json::to_vec(r).unwrap();
            bytes.push(b'\n');
            bytes
        })
        .collect()
}
fn restore_cloud(fixture: &Fixture, expected: &[Record]) -> u64 {
    let (_, destination) = fixture.stores();
    let deployment = fixture.definition().deployment;
    let count = u64::from(
        fixture
            .config
            .live
            .as_ref()
            .unwrap()
            .journal
            .segment_records,
    );
    let restored = Journal::restore(
        &fixture.scratch.path("journal"),
        &deployment,
        count,
        &mut |key| {
            let Some(head) = destination.head(key)? else {
                return Ok(None);
            };
            let mut bytes = Vec::new();
            destination.read_to(key, None, &mut bytes)?;
            assert_eq!(bytes.len() as u64, head.bytes);
            Ok(Some(bytes))
        },
    )
    .unwrap();
    let (_, actual) = Journal::open(&fixture.scratch.path("journal"), &deployment, count).unwrap();
    assert_eq!(journal_bytes(&actual), journal_bytes(expected));
    restored
}
fn assert_segments(fixture: &Fixture, completed: &live::Completed) {
    let mut collected = Vec::new();
    for segment in &completed.manifest.journal_segments {
        let bytes = fs::read(fixture.scratch.path("published").join(&segment.key)).unwrap();
        assert_eq!(bytes.len() as u64, segment.bytes);
        assert_eq!(
            binary_alpha_engine::research::digest(b"", &bytes),
            segment.sha256
        );
        let records = bytes
            .split(|b| *b == b'\n')
            .filter(|r| !r.is_empty())
            .map(|r| serde_json::from_slice::<Record>(r).unwrap())
            .collect::<Vec<_>>();
        if records.len()
            == fixture
                .config
                .live
                .as_ref()
                .unwrap()
                .journal
                .segment_records as usize
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
        collected.extend(records);
    }
    collected.sort_by_key(|r| r.sequence);
    assert_eq!(collected.first().unwrap().sequence, 1);
    for pair in collected.windows(2) {
        assert_eq!(pair[1].sequence, pair[0].sequence + 1);
        assert_eq!(
            pair[1].previous_sha256,
            binary_alpha_engine::research::digest(b"", &serde_json::to_vec(&pair[0]).unwrap())
        );
    }
}

#[test]
fn cloud_outage_full_spool_and_restart_with_pending_uploads() {
    let mut fixture = Fixture::new("t1-cloud-full-spool");
    fixture
        .config
        .live
        .as_mut()
        .unwrap()
        .journal
        .segment_records = 4;
    fixture
        .config
        .live
        .as_mut()
        .unwrap()
        .journal
        .max_spool_bytes = 8_000;
    let mut input = rows(&matching_log());
    for second in (1..5).rev() {
        input.insert(8, tick(START + second * 1_000_000, "180.0000"));
    }
    for second in 6..=20 {
        input.push(tick(START + second * 1_000_000, "180.0002"));
    }
    input.push(proposal(START + 20_000_000));
    let recorded = RecordedConnector::from_jsonl(&log(&input)).unwrap();
    let control = FakeControl::new(START);
    let mut owner = run(
        &fixture,
        live::Mode::Replay,
        &recorded,
        Box::new(control.clone()),
    );
    let cloud = fixture
        .scratch
        .path("published/live")
        .join(&owner.definition.deployment)
        .join("journal");
    fs::create_dir_all(cloud.parent().unwrap()).unwrap();
    // A file at the directory path deterministically returns ENOTDIR even as root.
    fs::write(&cloud, b"synthetic unavailable cloud destination").unwrap();
    let before = spool_bytes(&fixture);
    assert!(before < 8_000);
    let mut full = false;
    let mut full_before_settlement = false;
    let mut settled_while_full = false;
    let result = owner.run_until(|health| {
        full |= matches!(&health.entries, Entries::Disabled(r) if r.contains("journal spool bound reached"));
        full_before_settlement |= full && health.risk.cash == decimal("9990.00") && health.open_commands == 1;
        settled_while_full |= full && health.risk.cash == decimal("10008.83") && health.open_commands == 0;
        recorded.exhausted()
    });
    let error = result
        .err()
        .expect("unavailable cloud must prevent final publication");
    assert!(error.contains("Not a directory"), "{error}");
    assert!(full);
    assert!(full_before_settlement);
    assert!(settled_while_full);
    assert!(spool_bytes(&fixture) >= 8_000);
    assert!(spool_bytes(&fixture) > before);
    balances(&owner, "10008.83", "0", "0", "0", 0);
    assert_eq!(buys(&recorded), 1);
    let events = ledger(&owner);
    assert_eq!(
        events
            .iter()
            .filter(|e| matches!(e.kind, EventKind::Settled { .. }))
            .count(),
        1
    );
    assert_eq!(events.iter().filter(|e| matches!(&e.kind, EventKind::Released { source, release, .. } if source.id.starts_with("entry-disabled:") && *release == decimal("10.00"))).count(), 1);
    let retained = owner.records().to_vec();
    assert!(!cloud.parent().unwrap().join("final").exists());
    drop(owner);
    let (pending, replayed) = Journal::open(
        &fixture.scratch.path("journal"),
        &fixture.definition().deployment,
        4,
    )
    .unwrap();
    assert_eq!(replayed, retained);
    assert!(!pending.closed().unwrap().is_empty());
    let names = pending.closed().unwrap();
    drop(pending);
    fs::rename(&cloud, fixture.scratch.path("cloud-outage-marker")).unwrap();
    fs::create_dir_all(&cloud).unwrap();
    let source = rows(&matching_log());
    let mut input = startup(START + 20_000_000, false, false, "10008.83");
    let mut terminal = source[10].clone();
    terminal["at"] = json!(START + 20_000_000);
    input.push(terminal);
    let recorded = RecordedConnector::from_jsonl(&log(&input)).unwrap();
    let mut owner = run(&fixture, live::Mode::Live, &recorded, Box::new(control));
    let completed = owner.run_until(|_| recorded.exhausted()).unwrap().unwrap();
    assert_eq!(ledger(&owner), events);
    assert_eq!(owner.health().cloud_failed_segments, 0);
    assert_eq!(owner.health().cloud_pending_segments, 0);
    assert_eq!(buys(&recorded), 0);
    for name in names {
        assert!(cloud.join(name).is_file());
    }
    assert_segments(&fixture, &completed);
    let records = owner.records().to_vec();
    drop(owner);
    assert!(restore_cloud(&fixture, &records) > 0);
}

#[test]
fn terminal_claim_archival_interrupted() {
    use std::sync::atomic::{AtomicBool, Ordering};
    for (name, point) in [
        ("before-upload", Checkpoint::BeforeUpload),
        ("before-delete", Checkpoint::BeforeClaimDeletion),
        ("after-delete", Checkpoint::AfterClaimDeletion),
        ("after-verify", Checkpoint::AfterUploadVerification),
    ] {
        let mut fixture = Fixture::two(&format!("t1-archive-{name}"));
        fixture
            .config
            .live
            .as_mut()
            .unwrap()
            .journal
            .segment_records = 1024;
        // The first contract settles; the second remains paid/open throughout archival.
        let two = rows(&support::two_matching_log());
        let recorded = RecordedConnector::from_jsonl(&log(&two[..17])).unwrap();
        let mut control = FakeControl::new(START);
        let mut owner = run(
            &fixture,
            live::Mode::Replay,
            &recorded,
            Box::new(control.clone()),
        );
        let armed = Arc::new(AtomicBool::new(false));
        let hook = armed.clone();
        owner.hook = Some(Box::new(move |at| {
            hook.load(Ordering::SeqCst) && at == point
        }));
        let result = owner.run_until(|health| {
            let done = recorded.exhausted()
                && health.risk.cash == decimal("9998.83")
                && health.open_commands == 1;
            armed.store(done, Ordering::SeqCst);
            done
        });
        assert!(owner.interrupted(), "{name}");
        balances(&owner, "9998.83", "0", "10", "10", 1);
        assert_eq!(buys(&recorded), 2);
        let unresolved = control.unresolved(key(&fixture)).unwrap();
        assert_eq!(unresolved.len(), 1);
        assert_eq!(unresolved[0].state, ClaimState::Accepted);
        assert_eq!(unresolved[0].contract_ref.as_deref(), Some("12859891479"));
        assert_eq!(
            unresolved[0].transaction_ref.as_deref(),
            Some("24655144339")
        );
        let events = ledger(&owner);
        let signal = events[1].clone();
        let EventKind::Signal {
            command: Some(command),
            ..
        } = &signal.kind
        else {
            unreachable!()
        };
        let original = Claim {
            command: command.clone(),
            claim: format!("{}:{command}", owner.definition.deployment),
            deployment: owner.definition.deployment.clone(),
            token: 1,
            max_proposal_age_micros: 1_000_000,
            signal,
            state: ClaimState::Claimed,
            contract_ref: None,
            transaction_ref: None,
        };
        let records = owner.records().to_vec();
        let final_dir = fixture
            .scratch
            .path("published/live")
            .join(&owner.definition.deployment)
            .join("final");
        let cloud_dir = final_dir.parent().unwrap().join("journal");
        if point == Checkpoint::BeforeUpload {
            assert_eq!(
                result.err().unwrap(),
                "live: archival interrupted before final publication"
            );
            assert!(!cloud_dir.exists());
            assert!(!final_dir.exists());
        } else if point == Checkpoint::AfterUploadVerification {
            assert_eq!(fs::read_dir(&cloud_dir).unwrap().count(), 1);
            let path = fs::read_dir(&cloud_dir)
                .unwrap()
                .next()
                .unwrap()
                .unwrap()
                .path();
            assert_eq!(fs::read(path).unwrap(), journal_bytes(&records));
            // PRIMARY: finish does not recheck interrupted after receiving Uploaded.
            // AfterUploadVerification returns true with the full lifecycle verified,
            // but finish still publishes the ledger, receipt and final manifest.
            assert!(
                !final_dir.exists(),
                "AfterUploadVerification interruption must stop before final publication"
            );
        } else {
            let completed = result.unwrap().unwrap();
            assert!(final_dir.is_dir());
            assert_eq!(
                fs::read(completed.manifest_uri.strip_prefix("file://").unwrap()).unwrap(),
                serde_json::to_vec(&completed.manifest).unwrap()
            );
            assert_segments(&fixture, &completed);
        }
        drop(owner);
        // Probe the actual row via the public unique-conflict path after reacquiring.
        let lease = control
            .acquire(
                key(&fixture),
                "synthetic-owner",
                &original.deployment,
                60_000_000,
            )
            .unwrap()
            .unwrap();
        let probe = Claim {
            token: lease.token,
            ..original.clone()
        };
        let found = control
            .claim(key(&fixture), "synthetic-owner", lease.token, &probe)
            .unwrap();
        if point == Checkpoint::AfterClaimDeletion {
            assert_eq!(found, ClaimOutcome::Inserted);
            // Remove only the new synthetic probe after its asserted absence result.
            assert!(
                control
                    .update_claim(
                        key(&fixture),
                        "synthetic-owner",
                        lease.token,
                        &probe.command,
                        ClaimState::Reconciled,
                        None,
                        None
                    )
                    .unwrap()
            );
            control
                .delete_reconciled(key(&fixture), &probe.command)
                .unwrap();
        } else {
            assert_eq!(
                found,
                ClaimOutcome::Replay(Claim {
                    state: ClaimState::Reconciled,
                    contract_ref: Some("12859891379".into()),
                    transaction_ref: Some("24655144239".into()),
                    ..original
                })
            );
        }
        assert!(
            control
                .release(key(&fixture), "synthetic-owner", lease.token)
                .unwrap()
        );
        assert_eq!(control.unresolved(key(&fixture)).unwrap(), unresolved);
        let mut input = startup(START + 5_000_000, true, false, "9998.83");
        input.push(rows(&matching_log())[10].clone());
        let mut second_entry = two[11].clone();
        second_entry["at"] = json!(START + 5_000_000);
        input.push(second_entry);
        let recorded = RecordedConnector::from_jsonl(&log(&input)).unwrap();
        let mut restored = run(
            &fixture,
            live::Mode::Live,
            &recorded,
            Box::new(control.clone()),
        );
        let completed = restored
            .run_until(|_| recorded.exhausted())
            .unwrap()
            .unwrap();
        assert_eq!(ledger(&restored), events);
        balances(&restored, "9998.83", "0", "10", "10", 1);
        assert_eq!(control.unresolved(key(&fixture)).unwrap(), unresolved);
        assert_eq!(buys(&recorded), 0);
        assert_segments(&fixture, &completed);
        let records = restored.records().to_vec();
        drop(restored);
        restore_cloud(&fixture, &records);
    }
}

#[test]
fn malformed_journal_tail_refuses_recovery_without_mutating_the_claim() {
    let mut fixture = Fixture::new("t1-journal-tail-corruption");
    fixture
        .config
        .live
        .as_mut()
        .unwrap()
        .journal
        .segment_records = 1024;
    let mut control = FakeControl::new(START);
    let recorded = RecordedConnector::from_jsonl(&matching_log()).unwrap();
    let mut owner = run(
        &fixture,
        live::Mode::Replay,
        &recorded,
        Box::new(control.clone()),
    );
    stop_at(&mut owner, Checkpoint::AfterClaimBeforeWrite);
    let records = owner.records().to_vec();
    let claims = control.unresolved(key(&fixture)).unwrap();
    assert_eq!(claims.len(), 1);
    assert_eq!(claims[0].state, ClaimState::Claimed);
    assert_eq!(buys(&recorded), 0);
    drop(owner);
    let path = fixture.scratch.path("journal/open.jsonl");
    let original = fs::read(&path).unwrap();
    assert_eq!(original, journal_bytes(&records));
    for incomplete in [true, false] {
        let mut damaged = original.clone();
        if incomplete {
            assert_eq!(damaged.pop(), Some(b'\n'));
        } else {
            let mut records = records.clone();
            records.last_mut().unwrap().previous_sha256 = "f".repeat(64);
            damaged = journal_bytes(&records);
        }
        fs::write(&path, &damaged).unwrap();
        let restarted = RecordedConnector::from_jsonl(&matching_log()).unwrap();
        let error = support::runtime_with(
            &fixture,
            live::Mode::Live,
            &restarted,
            Box::new(control.clone()),
            |_| {},
            |m| m,
        )
        .err()
        .unwrap();
        assert_eq!(
            error,
            format!(
                "journal record 5 in open.jsonl: {}",
                if incomplete {
                    "incomplete line"
                } else {
                    "previous hash mismatch"
                }
            )
        );
        assert_eq!(control.unresolved(key(&fixture)).unwrap(), claims);
        assert_eq!(fs::read(&path).unwrap(), damaged);
        assert_eq!(restarted.writes().len(), 2);
        assert_eq!(buys(&restarted), 0);
    }
    fs::write(path, original).unwrap();
}

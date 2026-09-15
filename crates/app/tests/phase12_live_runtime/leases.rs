//! Cooperative lease scenarios with database rows shared between deterministic runtimes.
use super::support::*;
use binary_alpha_app::{
    broker::{
        Clock,
        transport::{RecordedConnector, ReplayClock},
    },
    live::{
        self,
        control::{Claim, ClaimOutcome, ClaimState, Control, FakeControl, Fault, Lease, LeaseKey},
        journal::{LeaseState, RecordKind},
    },
};
use binary_alpha_engine::execution::{EventKind, Resolution};
use serde_json::{Value, json};
use std::{
    collections::VecDeque,
    sync::{
        Arc, Mutex,
        atomic::{AtomicI64, Ordering},
    },
};

fn key() -> LeaseKey<'static> {
    LeaseKey {
        broker: "deriv",
        account: "a0",
    }
}
fn writes(recorded: &RecordedConnector) -> usize {
    recorded
        .writes()
        .iter()
        .filter(|(_, text)| text.contains("\"buy\":"))
        .count()
}
fn states(owner: &live::Runtime) -> Vec<(LeaseState, u64)> {
    owner
        .records()
        .iter()
        .filter_map(|r| match r.kind {
            RecordKind::Lease { state, token } => Some((state, token)),
            _ => None,
        })
        .collect()
}
fn purchase() -> Value {
    json!({"contract_id":12859891379u64,"transaction_id":24655144239u64,"buy_price":10,"payout":18.83,"purchase_time":START/1_000_000,"date_start":START/1_000_000,"expiry_time":START/1_000_000+5,"currency":"USD","underlying_symbol":"R_50","contract_type":"CALL"})
}
fn recovery_rows(at: i64) -> Vec<Value> {
    let source = scenario_rows(&matching_log());
    let mut rows = source[..2].to_vec();
    for row in &mut rows {
        row["at"] = json!(at);
    }
    rows.push(account_row(
        at,
        &json!({"msg_type":"portfolio","req_id":71,"portfolio":{"contracts":[purchase()]}})
            .to_string(),
    ));
    rows.push(account_row(
        at,
        r#"{"msg_type":"statement","req_id":72,"statement":{"count":0,"transactions":[]}}"#,
    ));
    rows.push(account_row(at, &frame("transaction-ack")));
    rows.push(account_row(
        at,
        &change(&frame("balance-before"), "balance", "balance", "9990"),
    ));
    rows
}
#[derive(Clone)]
struct LocalClock {
    replay: ReplayClock,
    delay: Arc<AtomicI64>,
}
impl Clock for LocalClock {
    fn now_micros(&self) -> i64 {
        self.replay.now_micros() + self.delay.load(Ordering::SeqCst)
    }
    fn sleep(&mut self, micros: i64) {
        self.delay.fetch_add(micros, Ordering::SeqCst);
    }
}
type Renewals = Arc<Mutex<Vec<Result<Option<Lease>, String>>>>;
struct ScenarioControl {
    inner: FakeControl,
    owner: &'static str,
    skew: i64,
    acquisition_delay: Option<(Arc<AtomicI64>, i64)>,
    renew_faults: VecDeque<Option<Fault>>,
    release_fault: Option<Fault>,
    renewals: Renewals,
}
impl ScenarioControl {
    fn new(inner: FakeControl, owner: &'static str) -> Self {
        Self {
            inner,
            owner,
            skew: 0,
            acquisition_delay: None,
            renew_faults: VecDeque::new(),
            release_fault: None,
            renewals: Arc::default(),
        }
    }
}
impl Control for ScenarioControl {
    fn advance_to(&mut self, at: i64) {
        self.inner.advance_to(at + self.skew);
    }
    fn acquire(
        &mut self,
        key: LeaseKey<'_>,
        _: &str,
        deployment: &str,
        ttl: i64,
    ) -> Result<Option<Lease>, String> {
        let result = self.inner.acquire(key, self.owner, deployment, ttl);
        if let Some((delay, micros)) = self.acquisition_delay.take() {
            delay.fetch_add(micros, Ordering::SeqCst);
        }
        result
    }
    fn renew(
        &mut self,
        key: LeaseKey<'_>,
        _: &str,
        token: u64,
        ttl: i64,
    ) -> Result<Option<Lease>, String> {
        if let Some(Some(fault)) = self.renew_faults.pop_front() {
            self.inner.fault(fault);
        }
        let result = self.inner.renew(key, self.owner, token, ttl);
        self.renewals.lock().unwrap().push(result.clone());
        result
    }
    fn release(&mut self, key: LeaseKey<'_>, _: &str, token: u64) -> Result<bool, String> {
        if let Some(fault) = self.release_fault.take() {
            self.inner.fault(fault);
        }
        self.inner.release(key, self.owner, token)
    }
    fn claim(
        &mut self,
        key: LeaseKey<'_>,
        _: &str,
        token: u64,
        claim: &Claim,
    ) -> Result<ClaimOutcome, String> {
        self.inner.claim(key, self.owner, token, claim)
    }
    fn update_claim(
        &mut self,
        key: LeaseKey<'_>,
        _: &str,
        token: u64,
        command: &str,
        state: ClaimState,
        contract: Option<&str>,
        transaction: Option<&str>,
    ) -> Result<bool, String> {
        self.inner.update_claim(
            key,
            self.owner,
            token,
            command,
            state,
            contract,
            transaction,
        )
    }
    fn unresolved(&mut self, key: LeaseKey<'_>) -> Result<Vec<Claim>, String> {
        self.inner.unresolved(key)
    }
    fn delete_reconciled(&mut self, key: LeaseKey<'_>, command: &str) -> Result<(), String> {
        self.inner.delete_reconciled(key, command)
    }
}
fn claim_prefix(
    fixture: &Fixture,
    control: &FakeControl,
    point: live::Checkpoint,
) -> (live::Runtime, Claim, RecordedConnector) {
    let recorded = RecordedConnector::from_jsonl(&matching_log()).unwrap();
    let mut owner = runtime_with(
        fixture,
        live::Mode::Replay,
        &recorded,
        Box::new(ScenarioControl::new(control.clone(), "first")),
        |_| {},
        |m| m,
    )
    .unwrap();
    owner.hook = Some(Box::new(move |at| at == point));
    assert!(owner.run_until(|_| false).unwrap().is_none());
    let claims = control.clone().unresolved(key()).unwrap();
    assert_eq!(claims.len(), 1);
    (owner, claims[0].clone(), recorded)
}

#[test]
fn two_runtimes_one_submits_both_observe() {
    let fixture = Fixture::new("t2-two-runtimes");
    let control = FakeControl::new(START);
    let (holder, claim, holder_io) =
        claim_prefix(&fixture, &control, live::Checkpoint::AfterAcknowledgement);
    assert_eq!(writes(&holder_io), 1);
    assert_eq!(claim.state, ClaimState::Accepted);
    let other_host = isolated_fixture(&fixture, "observer");
    let definition = other_host.definition();
    let (local, destination) = other_host.stores();
    live::authorization::create(
        &destination,
        &local,
        live::authorization::Authorization {
            schema_version: 1,
            deployment: definition.deployment,
            configuration: definition.manifest.config_hash,
            bundle_sha256: definition.manifest.bundle_sha256,
            broker: "deriv".into(),
            account: "a0".into(),
            operator: "synthetic-operator".into(),
            reason: "synthetic lease-contention proof".into(),
            hash: String::new(),
        },
    )
    .unwrap();
    let source = scenario_rows(&matching_log());
    let mut rows = recovery_rows(START);
    // The observing runtime receives the same market, proposal, entry, cash and terminal
    // frames; only the purchase reply belongs exclusively to the runtime that dispatched.
    rows.extend([source[4].clone(), source[7].clone(), source[5].clone()]);
    for second in 1..=4 {
        rows.push(scenario_tick(START + second * 1_000_000, "180.0000"));
    }
    rows.extend_from_slice(&source[8..]);
    for second in 6..=20 {
        rows.push(scenario_tick(START + second * 1_000_000, "180.0002"));
    }
    let mut proposal = source[5]["frame"].as_str().unwrap().to_string();
    for field in ["date_start", "spot_time"] {
        proposal = change(
            &proposal,
            "proposal",
            field,
            &(START / 1_000_000 + 20).to_string(),
        );
    }
    proposal = change(
        &proposal,
        "proposal",
        "date_expiry",
        &(START / 1_000_000 + 25).to_string(),
    );
    proposal = change(&proposal, "proposal", "spot", "180.0002");
    proposal = change(&proposal, "proposal", "id", r#""observer-offer""#);
    rows.push(account_row(
        START + 20_000_000,
        &crate::common::broker::replace(&proposal, "req_id", "144"),
    ));
    let recorded = RecordedConnector::from_jsonl(&scenario_log(&rows)).unwrap();
    let mut observer = runtime_with(
        &other_host,
        live::Mode::Live,
        &recorded,
        Box::new(ScenarioControl::new(control.clone(), "observer")),
        |_| {},
        |m| m,
    )
    .unwrap();
    assert_eq!(observer.health().fencing_token, 0);
    let result = observer
        .run_until(|_| recorded.exhausted())
        .unwrap()
        .unwrap();
    assert_eq!(writes(&recorded), 0);
    assert_eq!(holder.health().receipt_sequence, 1);
    assert_eq!(observer.health().receipt_sequence, 21);
    assert_eq!(holder.engine().accounts()[0].cash.to_string(), "9990.00");
    assert_eq!(
        holder.engine().accounts()[0].paid_basis.to_string(),
        "10.00"
    );
    assert_eq!(holder.engine().accounts()[0].open, 1);
    assert_eq!(observer.engine().accounts()[0].cash.to_string(), "10008.83");
    assert_eq!(
        observer.engine().accounts()[0].paid_basis.to_string(),
        "0.00"
    );
    assert_eq!(observer.engine().accounts()[0].reserved.to_string(), "0.00");
    assert_eq!(
        observer.engine().accounts()[0].unresolved_loss.to_string(),
        "0.00"
    );
    assert_eq!(observer.engine().accounts()[0].open, 0);
    let holder_events = ledger_events(&holder);
    let observed = ledger_events(&observer);
    assert_eq!(&observed[..2], &holder_events[..2]);
    assert!(matches!(holder_events[2].kind, EventKind::Accepted { .. }));
    assert_eq!(holder_events.len(), 3);
    let reconciled = observed
        .iter()
        .filter_map(|e| match &e.kind {
            EventKind::Reconciled {
                resolution: Resolution::Purchased { debit, liability },
                ..
            } => Some((debit.to_string(), liability.contract_ref.as_str())),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(reconciled, [("10".into(), "12859891379")]);
    let refused_dispatch = observed
        .iter()
        .filter_map(|e| match &e.kind {
            EventKind::Released {
                command,
                source,
                release,
                rejected,
            } => Some((
                command.clone(),
                source.id.clone(),
                release.to_string(),
                *rejected,
            )),
            _ => None,
        })
        .collect::<Vec<_>>();
    let later = format!(
        "{}/{}",
        observer.definition.policy.replay.bindings[0].id,
        START + 20_000_000
    );
    assert_eq!(
        refused_dispatch,
        [(
            later.clone(),
            format!("entry-disabled:{later}"),
            "10.00".into(),
            false
        )]
    );
    assert!(
        matches!(&observer.health().entries, live::Entries::Disabled(reason) if reason.contains("account lease is held by another owner") && !reason.contains("authorization") && !reason.contains("compatibility outside envelope"))
    );
    assert_eq!(
        observed
            .iter()
            .filter(|e| matches!(e.kind, EventKind::Accepted { .. }))
            .count(),
        0
    );
    assert_eq!(
        observed
            .iter()
            .filter(|e| matches!(e.kind, EventKind::Settled { .. }))
            .count(),
        1
    );
    assert_eq!(result.receipt.dimensions[0].samples, 1);
    assert_eq!(control.clone().unresolved(key()).unwrap(), [claim]); // Observer cannot mutate the holder's durable row.
}

#[test]
fn lease_deadline_mapping_and_uncertain_renewal_stop_entries_not_settlement() {
    let fixture = Fixture::new("t2-lease-clock");
    // Actual Runtime::start mapping, with database time far ahead of the local clock,
    // a measured 800ms acquisition round trip, and a frozen 400ms safety margin.
    let mut early = isolated_fixture(&fixture, "conservative-deadline");
    let settings = &mut early.config.live.as_mut().unwrap().control;
    settings.lease_ttl_micros = 2_000_000;
    settings.renewal_interval_micros = 1_000_000;
    settings.safety_margin_micros = 400_000;
    let source = scenario_rows(&matching_log());
    let recorded = RecordedConnector::from_jsonl(&scenario_log(&source[..6])).unwrap();
    let delay = Arc::new(AtomicI64::new(0));
    let skew = 7_200_000_000;
    let db = FakeControl::new(START + skew);
    let mut control = ScenarioControl::new(db.clone(), "first");
    control.skew = skew;
    control.acquisition_delay = Some((delay.clone(), 800_000));
    let mut owner = runtime_with_owner_clock(
        &early,
        live::Mode::Replay,
        &recorded,
        Box::new(control),
        |d| {
            for risk in &mut d.policy.replay.risk_policies {
                risk.max_quote_age_micros = 800_000;
            }
            d.definition.replay = d.policy.replay.clone();
        },
        |m| m,
        |c| c,
        Some(Box::new(LocalClock {
            replay: recorded.clock(),
            delay,
        })),
    )
    .unwrap();
    assert_eq!(owner.health().lease_deadline_micros, START + 800_000);
    assert_eq!(
        db.clone()
            .acquire(
                key(),
                "early-successor",
                &owner.definition.deployment,
                2_000_000
            )
            .unwrap(),
        None
    );
    assert_eq!(
        live::lease_deadline(
            START,
            START + 800_000,
            &Lease {
                token: 1,
                server_now_micros: START + skew,
                expires_at_micros: START + skew + 2_000_000
            },
            400_000
        )
        .unwrap(),
        START + 800_000
    );
    owner.run_until(|_| recorded.exhausted()).unwrap().unwrap();
    assert_eq!(writes(&recorded), 0);
    assert_eq!(owner.engine().accounts()[0].cash.to_string(), "10000.00");
    assert!(
        matches!(&owner.health().entries, live::Entries::Disabled(r) if r.contains("lease deadline reached"))
    );
    assert_eq!(ledger_events(&owner).iter().filter(|e| matches!(&e.kind, EventKind::Released { source, .. } if source.id.starts_with("entry-disabled:"))).count(), 1);

    let mut renewal = isolated_fixture(&fixture, "renewal-faults");
    renewal
        .config
        .live
        .as_mut()
        .unwrap()
        .control
        .renewal_interval_micros = 2_000_000;
    let mut rows = source[..8].to_vec();
    for second in 1..=4 {
        rows.push(scenario_tick(START + second * 1_000_000, "180.0000"));
    }
    rows.extend_from_slice(&source[8..]);
    rows.push(scenario_tick(START + 6_000_000, "180.0002"));
    rows.push(scenario_tick(START + 8_000_000, "180.0002"));
    let recorded = RecordedConnector::from_jsonl(&scenario_log(&rows)).unwrap();
    let db = FakeControl::new(START);
    let mut control = ScenarioControl::new(db.clone(), "first");
    control.renew_faults = [Some(Fault::LoseResponse), Some(Fault::Partition), None].into();
    let renewals = control.renewals.clone();
    let mut owner = runtime_with(
        &renewal,
        live::Mode::Replay,
        &recorded,
        Box::new(control),
        |_| {},
        |m| m,
    )
    .unwrap();
    let mut renewed_entries = None;
    let mut renewal_veto_with_exposure = false;
    owner
        .run_until(|health| {
            renewal_veto_with_exposure |= health.risk.open == 1
                && matches!(&health.entries, live::Entries::Disabled(reason)
                    if reason.contains("lease renewal unavailable or lost"));
            if renewals.lock().unwrap().len() >= 3 {
                renewed_entries = Some(health.entries.clone());
            }
            recorded.exhausted()
        })
        .unwrap()
        .unwrap();
    assert!(renewal_veto_with_exposure);
    assert_eq!(renewed_entries, Some(live::Entries::Enabled));
    // Observe the financial owner's journal order, independent of worker polling:
    // settlement occurs after lease loss and before a successful renewal.
    let records = owner.records();
    let settled = records
        .iter()
        .position(|r| {
            matches!(&r.kind,
                RecordKind::Ledger { event } if matches!(event.kind, EventKind::Settled { .. })
            )
        })
        .unwrap();
    assert_eq!(
        records[..settled].iter().rev().find_map(|r| match r.kind {
            RecordKind::Lease { state, .. } => Some(state),
            _ => None,
        }),
        Some(LeaseState::Lost)
    );
    assert!(records[settled + 1..].iter().any(|r| matches!(
        r.kind,
        RecordKind::Lease {
            state: LeaseState::Renewed,
            ..
        }
    )));
    let renewed = renewals.lock().unwrap();
    assert_eq!(renewed[0], Err("response lost".into()));
    assert_eq!(renewed[1], Err("partition".into()));
    let lease = renewed[2].as_ref().unwrap().as_ref().unwrap();
    assert_eq!(lease.token, 1);
    assert!((START + 6_000_000..=START + 8_000_000).contains(&lease.server_now_micros));
    assert_eq!(
        lease.expires_at_micros,
        lease.server_now_micros + 60_000_000
    );
    assert_eq!(owner.health().fencing_token, 1);
    assert_eq!(owner.engine().accounts()[0].cash.to_string(), "10008.83");
    assert_eq!(owner.engine().accounts()[0].open, 0);
    assert_eq!(writes(&recorded), 1);
    assert_eq!(
        &states(&owner)[..4],
        [
            (LeaseState::Acquired, 1),
            (LeaseState::Lost, 1),
            (LeaseState::Lost, 1),
            (LeaseState::Renewed, 1)
        ]
    );
}

#[test]
fn takeover_around_expiry_and_stale_token_claims() {
    // Extends resilience's claim-only recovery with explicit expiry-edge fencing.
    let fixture = Fixture::new("t2-takeover-expiry");
    let mut db = FakeControl::new(START);
    let (old, claim, old_io) = claim_prefix(&fixture, &db, live::Checkpoint::AfterClaimBeforeWrite);
    assert_eq!(writes(&old_io), 0);
    db.advance(59_999_999);
    assert_eq!(
        db.acquire(key(), "successor", &claim.deployment, 60_000_000)
            .unwrap(),
        None
    );
    db.advance(1);
    assert_eq!(
        db.claim(key(), "first", 1, &claim).unwrap(),
        ClaimOutcome::LeaseLost
    );
    assert_eq!(db.renew(key(), "first", 1, 60_000_000).unwrap(), None);
    let host = isolated_fixture(&fixture, "successor");
    let source = scenario_rows(&matching_log());
    let mut rows = recovery_rows(START + 60_000_000);
    for row in &source[7..8] {
        let mut row = row.clone();
        row["at"] = json!(START + 60_000_000);
        rows.push(row);
    }
    let recorded = RecordedConnector::from_jsonl(&scenario_log(&rows)).unwrap();
    let mut successor = runtime_with(
        &host,
        live::Mode::Paper,
        &recorded,
        Box::new(ScenarioControl::new(db.clone(), "successor")),
        |_| {},
        |m| m,
    )
    .unwrap();
    assert_eq!(successor.health().fencing_token, 2);
    assert!(
        !db.update_claim(
            key(),
            "first",
            1,
            &claim.command,
            ClaimState::NotSent,
            None,
            None
        )
        .unwrap()
    );
    successor
        .run_until(|_| recorded.exhausted())
        .unwrap()
        .unwrap();
    let remote = db.unresolved(key()).unwrap();
    assert_eq!(remote.len(), 1);
    assert_eq!(remote[0].token, 1); // Origin token is retained while the new owner reconciles.
    assert_eq!(remote[0].state, ClaimState::Accepted);
    assert_eq!(remote[0].contract_ref.as_deref(), Some("12859891379"));
    assert_eq!(remote[0].transaction_ref.as_deref(), Some("24655144239"));
    assert_eq!(remote[0].signal, claim.signal);
    assert_eq!(successor.engine().accounts()[0].cash.to_string(), "9990.00");
    assert_eq!(
        successor.engine().accounts()[0].reserved.to_string(),
        "0.00"
    );
    assert_eq!(
        successor.engine().accounts()[0].paid_basis.to_string(),
        "10.00"
    );
    assert_eq!(
        successor.engine().accounts()[0].unresolved_loss.to_string(),
        "10.00"
    );
    assert_eq!(successor.engine().accounts()[0].open, 1);
    assert_eq!(
        ledger_events(&successor)
            .iter()
            .filter(|e| matches!(
                e.kind,
                EventKind::Reconciled {
                    resolution: Resolution::Purchased { .. },
                    ..
                }
            ))
            .count(),
        1
    );
    assert_eq!(writes(&recorded), 0);
    assert_eq!(old.engine().accounts()[0].reserved.to_string(), "10.00");
}

#[test]
fn release_ordering() {
    // FakeControl serializes every transaction under its shared mutex. These two explicit
    // orders complement control::postgres_control's actual waiting row-lock scenarios.
    let fixture = Fixture::new("t2-release-order");
    let db = FakeControl::new(START);
    let (paused, claim, _) = claim_prefix(&fixture, &db, live::Checkpoint::AfterClaimBeforeWrite);
    for claim_first in [false, true] {
        let mut db = FakeControl::new(START);
        let lease = db
            .acquire(key(), "first", &claim.deployment, 60_000_000)
            .unwrap()
            .unwrap();
        assert_eq!(lease.token, 1);
        if claim_first {
            assert_eq!(
                db.claim(key(), "first", 1, &claim).unwrap(),
                ClaimOutcome::Inserted
            );
        }
        db.fault(Fault::LoseResponse);
        assert_eq!(db.release(key(), "first", 1), Err("response lost".into()));
        assert_eq!(db.renew(key(), "first", 1, 60_000_000).unwrap(), None);
        assert_eq!(
            db.claim(key(), "first", 1, &claim).unwrap(),
            ClaimOutcome::LeaseLost
        );
        assert_eq!(
            db.unresolved(key()).unwrap(),
            if claim_first {
                vec![claim.clone()]
            } else {
                vec![]
            }
        );
        for token in 2..=4 {
            let next = db
                .acquire(key(), "next", &claim.deployment, 60_000_000)
                .unwrap()
                .unwrap();
            assert_eq!(next.token, token);
            assert!(!db.release(key(), "first", 1).unwrap());
            assert_eq!(db.renew(key(), "first", 1, 60_000_000).unwrap(), None);
            assert!(db.release(key(), "next", token).unwrap());
        }
    }
    drop(paused);
    let host = isolated_fixture(&fixture, "release-response-lost");
    let db = FakeControl::new(START);
    let recorded = RecordedConnector::from_jsonl(&matching_log()).unwrap();
    let mut control = ScenarioControl::new(db.clone(), "first");
    control.release_fault = Some(Fault::LoseResponse);
    let mut owner = runtime_with(
        &host,
        live::Mode::Replay,
        &recorded,
        Box::new(control),
        |_| {},
        |m| m,
    )
    .unwrap();
    let result = owner.run_until(|_| recorded.exhausted()).unwrap().unwrap();
    assert_eq!(states(&owner).last(), Some(&(LeaseState::Lost, 1)));
    assert_eq!(
        owner.health().entries,
        live::Entries::Disabled(
            "shutdown: draining observations; shutdown: lease release begun".into()
        )
    );
    assert_eq!(owner.engine().accounts()[0].cash.to_string(), "10008.83");
    assert!(result.receipt.promotion.eligible);
    assert_eq!(writes(&recorded), 1);
    assert_eq!(
        db.clone().renew(key(), "first", 1, 60_000_000).unwrap(),
        None
    );
    let next = db
        .clone()
        .acquire(key(), "next", &owner.definition.deployment, 60_000_000)
        .unwrap()
        .unwrap();
    assert_eq!(next.token, 2);
    assert_eq!(
        owner.health().entries,
        live::Entries::Disabled(
            "shutdown: draining observations; shutdown: lease release begun".into()
        )
    );
}

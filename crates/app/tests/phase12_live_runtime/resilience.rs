use super::support::{self, Fixture, START, change, frame, matching_log, runtime, runtime_with};
use super::support::{
    account_row as account, authorize, ledger_events as ledger, scenario_log as log,
    scenario_rows as rows,
};
use binary_alpha_app::{
    broker::{self, MarketDataBroker, transport::RecordedConnector},
    live::{
        self,
        control::{Claim, ClaimState, Control, FakeControl, Fault, LeaseKey},
        journal::{Journal, Record, RecordKind},
    },
};
use binary_alpha_engine::{
    execution::{EventKind, Resolution},
    market::{InstrumentId, PriceScale},
};
use serde_json::{Value, json};
use std::{
    fs,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

fn portfolio(at: i64, contracts: Value) -> Value {
    account(
        at,
        &json!({"msg_type":"portfolio","req_id":71+(at-START).max(0),"portfolio":{"contracts":contracts}})
            .to_string(),
    )
}
fn statement(at: i64) -> Value {
    account(
        at,
        &json!({"msg_type":"statement","req_id":72+(at-START).max(0),"statement":{"count":0,"transactions":[]}}).to_string(),
    )
}
fn tick(at: i64) -> Value {
    let mut row = rows(&matching_log())[4].clone();
    row["at"] = json!(at);
    row["frame"] = json!(change(
        row["frame"].as_str().unwrap(),
        "tick",
        "epoch",
        &(at / 1_000_000).to_string()
    ));
    row
}
fn refusal(at: i64) -> Value {
    account(
        at,
        &json!({"msg_type":"proposal","req_id":9844+(at-START).max(0),"error":{"code":"RateLimit"}}).to_string(),
    )
}

fn key() -> LeaseKey<'static> {
    LeaseKey {
        broker: "deriv",
        account: "a0",
    }
}
fn reset_prefix(fixture: &Fixture, records: &[Record], name: &str) {
    let journal = fixture.scratch.path("journal");
    if journal.exists() {
        fs::rename(&journal, fixture.scratch.path(&format!("{name}-journal"))).unwrap();
    }
    let cloud = fixture
        .scratch
        .path("published/live")
        .join(fixture.definition().deployment);
    if cloud.exists() {
        fs::rename(&cloud, fixture.scratch.path(&format!("{name}-cloud"))).unwrap();
    }
    let (mut journal, _) = Journal::open(&journal, &fixture.definition().deployment, 16).unwrap();
    for record in records {
        journal
            .append(record.time_micros, record.kind.clone())
            .unwrap();
    }
    journal
        .append(
            records.last().unwrap().time_micros,
            RecordKind::Discontinuity {
                reason: "synthetic process boundary".into(),
            },
        )
        .unwrap();
}
fn claim_prefix(fixture: &Fixture, control: &FakeControl) -> (live::Runtime, Claim) {
    let recorded = RecordedConnector::from_jsonl(&matching_log()).unwrap();
    let mut owner = runtime(fixture, live::Mode::Replay, &recorded, control.clone());
    owner.hook = Some(Box::new(|point| {
        point == live::Checkpoint::AfterClaimBeforeWrite
    }));
    assert!(owner.run_until(|_| false).unwrap().is_none());
    let mut control = control.clone();
    let claim = control.unresolved(key()).unwrap().remove(0);
    assert_eq!(claim.state, ClaimState::Claimed);
    (owner, claim)
}
fn purchase() -> Value {
    json!({"contract_id":12859891379u64,"transaction_id":24655144239u64,"buy_price":10,"payout":18.83,"purchase_time":START/1_000_000,"date_start":START/1_000_000,"expiry_time":START/1_000_000+5,"currency":"USD","underlying_symbol":"R_50","contract_type":"CALL"})
}
fn startup(at: i64, contracts: Value, reconcile: bool, cash: &str) -> Vec<Value> {
    let source = rows(&matching_log());
    let mut result = source[..2].to_vec();
    for row in &mut result {
        row["at"] = json!(at);
    }
    result.push(portfolio(at, contracts));
    if reconcile {
        result.push(statement(at));
    }
    result.push(account(at, &frame("transaction-ack")));
    result.push(account(
        at,
        &change(&frame("balance-before"), "balance", "balance", cash),
    ));
    result
}

#[test]
fn restored_nonbaseline_refusal_vetoes_an_eligible_authorized_purchase() {
    let fixture = Fixture::new("restored-refusal-veto");
    let source = rows(&matching_log());
    let mut input = source[..6].to_vec();
    input[5]["frame"] = json!(change(
        input[5]["frame"].as_str().unwrap(),
        "proposal",
        "payout",
        "19.53"
    ));
    let recorded = RecordedConnector::from_jsonl(&log(&input)).unwrap();
    let mut first = runtime(
        &fixture,
        live::Mode::Replay,
        &recorded,
        FakeControl::new(START),
    );
    first.run_until(|_| recorded.exhausted()).unwrap().unwrap();
    let cut = first
        .records()
        .iter()
        .position(|r| matches!(r.kind, RecordKind::Refused { .. }))
        .unwrap()
        + 1;
    let prefix = first.records()[..cut].to_vec();
    drop(first);
    reset_prefix(&fixture, &prefix, "refusal-prefix");
    authorize(&fixture);
    let mut input = startup(START, json!([]), false, "10000");
    input.extend_from_slice(&source[4..6]);
    let recorded = RecordedConnector::from_jsonl(&log(&input)).unwrap();
    let mut restored = runtime(
        &fixture,
        live::Mode::Live,
        &recorded,
        FakeControl::new(START),
    );
    assert!(
        matches!(restored.health().entries, live::Entries::Disabled(ref reasons) if reasons.contains("compatibility outside envelope"))
    );
    let completed = restored
        .run_until(|_| recorded.exhausted())
        .unwrap()
        .unwrap();
    assert!(restored.health().warmup);
    assert!(restored.health().balance_reconciled);
    assert!(ledger(&restored).iter().any(|e| matches!(
        e.kind,
        EventKind::Signal {
            command: Some(_),
            ..
        }
    )));
    assert!(
        !recorded
            .writes()
            .iter()
            .any(|(_, w)| w.contains("\"buy\":"))
    );
    assert!(
        completed
            .receipt
            .dimensions
            .iter()
            .any(|d| d.status == live::receipt::Status::OutsideEnvelope)
    );
}

#[test]
fn recovered_actual_payout_outside_baseline_disables_entries() {
    let fixture = Fixture::new("recovered-payout");
    let mut control = FakeControl::new(START);
    let (predecessor, claim) = claim_prefix(&fixture, &control);
    let prefix = predecessor.records().iter().take_while(|r| !matches!(&r.kind, RecordKind::Ledger { event } if matches!(event.kind, EventKind::Signal { .. }))).cloned().collect::<Vec<_>>();
    drop(predecessor);
    control
        .release(key(), "synthetic-owner", claim.token)
        .unwrap();
    reset_prefix(&fixture, &prefix, "claim-only");
    let mut actual = purchase();
    actual["payout"] = json!(19.53);
    let mut input = startup(START, json!([actual]), true, "9990");
    input.push(rows(&matching_log())[7].clone());
    let recorded = RecordedConnector::from_jsonl(&log(&input)).unwrap();
    let mut owner = runtime(&fixture, live::Mode::Paper, &recorded, control);
    let completed = owner.run_until(|_| recorded.exhausted()).unwrap().unwrap();
    assert!(ledger(&owner).iter().any(|e| matches!(&e.kind, EventKind::Reconciled { resolution: Resolution::Purchased { debit, liability }, .. } if debit.to_string() == "10" && liability.payout.to_string() == "19.53")));
    assert_eq!(completed.receipt.dimensions[0].samples, 1);
    assert_eq!(
        completed.receipt.dimensions[0].status,
        live::receipt::Status::OutsideEnvelope
    );
    assert!(
        completed.receipt.dimensions[0]
            .reason
            .as_ref()
            .unwrap()
            .contains("recovered actual debit or payout")
    );
    assert!(
        matches!(owner.health().entries, live::Entries::Disabled(ref reasons) if reasons.contains("compatibility outside envelope: economics_scope"))
    );
}

#[test]
fn startup_market_before_balance_fails_as_stalled() {
    let fixture = Fixture::new("r2-startup-stall");
    let mut input = rows(&matching_log());
    input.swap(3, 4);
    let recorded = RecordedConnector::from_jsonl(&log(&input)).unwrap();
    let error = runtime_with(
        &fixture,
        live::Mode::Replay,
        &recorded,
        Box::new(FakeControl::new(START)),
        |_| {},
        |m| m,
    )
    .err()
    .unwrap();
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
fn input_cash(at: i64, id: &str) -> Value {
    let mut cash: Value = serde_json::from_str(&frame("transaction-win")).unwrap();
    cash["transaction"]["transaction_id"] = json!(id.parse::<u64>().unwrap());
    cash["transaction"]["transaction_time"] = json!(at / 1_000_000);
    cash["transaction"]["amount"] = json!(0);
    cash["transaction"]["balance"] = json!(10000);
    account(at, &cash.to_string())
}

#[test]
fn uncertain_insert_enters_reconciliation_and_uses_own_no_write_proof() {
    let fixture = Fixture::new("r2-uncertain-insert");
    let input = rows(&matching_log());
    let recorded = RecordedConnector::from_jsonl(&log(&input[..6])).unwrap();
    let mut control = FakeControl::new(START);
    let mut owner = runtime(&fixture, live::Mode::Replay, &recorded, control.clone());
    let mut injected = false;
    owner.hook = Some(Box::new(move |point| {
        if point == live::Checkpoint::BeforeClaim && !injected {
            control.fault(Fault::LoseResponse);
            injected = true;
        }
        false
    }));
    owner.run_until(|_| recorded.exhausted()).unwrap().unwrap();
    let events = ledger(&owner);
    let uncertain = events
        .iter()
        .position(|e| matches!(e.kind, EventKind::PossiblySent { .. }))
        .unwrap();
    let released = events.iter().position(|e| matches!(&e.kind, EventKind::Reconciled{resolution:Resolution::NotSent,source,..} if source.id.starts_with("claim-commit-no-write:"))).unwrap();
    assert!(uncertain < released);
    assert_eq!(events[uncertain].time_micros, events[released].time_micros);
    assert!(owner.engine().accounts()[0].reserved.is_zero());
    assert_eq!(owner.engine().accounts()[0].open, 0);
    assert!(
        !recorded
            .writes()
            .iter()
            .any(|(_, w)| w.contains("\"buy\":"))
    );
}

#[test]
fn paused_write_retains_successor_exposure_past_the_correlation_window() {
    let mut fixture = Fixture::new("paused-write-successor");
    fixture
        .config
        .live
        .as_mut()
        .unwrap()
        .compatibility
        .observation_end =
        binary_alpha_engine::market::format_event_time_micros(START + 120_000_000);
    fixture
        .config
        .live
        .as_mut()
        .unwrap()
        .control
        .renewal_interval_micros = 40_000_000;
    authorize(&fixture);
    let source = rows(&matching_log());
    let mut input = startup(START, json!([]), false, "10000");
    input.extend_from_slice(&source[4..7]);
    let recorded = RecordedConnector::from_jsonl(&log(&input)).unwrap();
    let mut control = FakeControl::new(START);
    let mut predecessor = runtime(&fixture, live::Mode::Live, &recorded, control.clone());
    let mut definition = Some(fixture.definition());
    let successor_slot = std::rc::Rc::new(std::cell::RefCell::new(None));
    let saved = successor_slot.clone();
    let mut shared = control.clone();
    predecessor.hook = Some(Box::new(move |point| {
        if point == live::Checkpoint::DuringWrite {
            let claim = shared.unresolved(key()).unwrap().remove(0);
            assert_eq!(claim.state, ClaimState::Claimed);
            shared.advance(61_000_000);
            let at = START + 61_000_000;
            fixture.config.live.as_mut().unwrap().journal.dir = "successor-journal".into();
            let mut input = startup(at, json!([]), true, "10000");
            // A complete rising candle after takeover produces a fresh matching base row.
            for second in 61..=100 {
                let now = START + second * 1_000_000;
                let mut row = tick(now);
                if second >= 99 {
                    row["frame"] = json!(change(
                        row["frame"].as_str().unwrap(),
                        "tick",
                        "quote",
                        "180.0001"
                    ));
                }
                input.push(row);
                if second == 61 || second == 100 {
                    let mut proposal = change(
                        source[5]["frame"].as_str().unwrap(),
                        "proposal",
                        "spot",
                        "180.0001",
                    );
                    for (field, value) in [
                        ("date_start", now / 1_000_000),
                        ("date_expiry", now / 1_000_000 + 5),
                        ("spot_time", now / 1_000_000),
                    ] {
                        proposal = change(&proposal, "proposal", field, &value.to_string());
                    }
                    proposal = crate::common::broker::replace(
                        &proposal,
                        "req_id",
                        &(100 + second).to_string(),
                    );
                    input.push(account(now, &proposal));
                }
            }
            let recorded = RecordedConnector::from_jsonl(&log(&input)).unwrap();
            let mut successor = runtime_with(
                &fixture,
                live::Mode::Live,
                &recorded,
                Box::new(shared.clone()),
                |d| *d = definition.take().unwrap(),
                |m| m,
            )
            .unwrap();
            let completed = successor
                .run_until(|_| recorded.exhausted())
                .unwrap_or_else(|e| {
                    panic!(
                        "{e}; writes={:?}; health={:?}",
                        recorded.writes(),
                        successor.health()
                    )
                })
                .unwrap();
            assert!(successor.health().warmup);
            assert!(ledger(&successor).iter().any(|e| matches!(e.kind, EventKind::Signal { close_time_micros, disposition: binary_alpha_engine::execution::Disposition::AccountBlocked, .. } if close_time_micros == START + 100_000_000)));
            assert!(successor.health().balance_reconciled);
            assert!(
                matches!(successor.health().entries, live::Entries::Disabled(ref reasons) if reasons.contains("unresolved dispatch claim") && !reasons.contains("live authorization is absent"))
            );
            assert_eq!(
                successor.engine().accounts()[0].reserved.to_string(),
                "10.00"
            );
            assert_eq!(successor.engine().accounts()[0].open, 1);
            assert!(
                !recorded
                    .writes()
                    .iter()
                    .any(|(_, w)| w.contains("\"buy\":"))
            );
            assert!(ledger(&successor).iter().all(|e| !matches!(
                e.kind,
                EventKind::Reconciled {
                    resolution: Resolution::NotSent,
                    ..
                }
            )));
            assert_eq!(
                shared.unresolved(key()).unwrap()[0].state,
                ClaimState::PossiblySent
            );
            assert!(completed.manifest_uri.contains("/final/"));
            *saved.borrow_mut() = Some(successor);
        }
        point == live::Checkpoint::AfterWriteBeforeAcknowledgement
    }));
    assert!(predecessor.run_until(|_| false).unwrap().is_none());
    assert_eq!(
        recorded
            .writes()
            .iter()
            .filter(|(_, w)| w.contains("\"buy\":"))
            .count(),
        1
    );
    assert_eq!(
        predecessor.engine().accounts()[0].reserved.to_string(),
        "10.00"
    );
    assert_eq!(predecessor.engine().accounts()[0].open, 1);
    let claim = control.unresolved(key()).unwrap().remove(0);
    assert_eq!(claim.state, ClaimState::PossiblySent);
    let slot = successor_slot.borrow();
    let successor = slot.as_ref().unwrap();
    assert_eq!(
        successor.engine().accounts()[0].reserved.to_string(),
        "10.00"
    );
    // Only an explicit operator-provided purchase identity changes this unresolved row.
    let lease = control
        .acquire(key(), "synthetic-operator", &claim.deployment, 60_000_000)
        .unwrap()
        .unwrap();
    assert!(
        control
            .update_claim(
                key(),
                "synthetic-operator",
                lease.token,
                &claim.command,
                ClaimState::Accepted,
                Some("12859891379"),
                Some("24655144239")
            )
            .unwrap()
    );
    assert_eq!(
        control.unresolved(key()).unwrap()[0].state,
        ClaimState::Accepted
    );
}

#[test]
fn sustained_twenty_second_rows_one_hour_proposals_bound_work_and_poll_facts() {
    let mut fixture = Fixture::new("r2-bounded-work");
    let binary_alpha_engine::config::Broker::Deriv(b) = &mut fixture.config.brokers[0] else {
        unreachable!()
    };
    let limits = binary_alpha_engine::config::RateBudgets {
        trade: binary_alpha_engine::config::RateLimit {
            per_minute: 1,
            per_hour: 1,
        },
        ..Default::default()
    };
    b.budgets = Some(limits);
    let source = rows(&matching_log());
    let mut input = startup(START, json!([]), false, "10000");
    input.extend([
        source[4].clone(),
        refusal(START),
        input_cash(START, "90000001"),
    ]);
    for i in 1..=7200 {
        let at = START + i * 1_000_000;
        input.push(tick(at));
        if i % 3600 == 0 {
            let mut reply = refusal(at);
            reply["frame"] = json!(crate::common::broker::replace(
                reply["frame"].as_str().unwrap(),
                "req_id",
                &(40 + i).to_string()
            ));
            input.extend([reply, input_cash(at, &format!("{}", 90000001 + i))]);
        }
    }
    let recorded = RecordedConnector::from_jsonl(&log(&input)).unwrap();
    let mut owner = runtime(
        &fixture,
        live::Mode::Paper,
        &recorded,
        FakeControl::new(START),
    );
    let mut peak = 0;
    owner
        .run_until(|health| {
            peak = peak.max(health.pending_rows);
            assert!(health.pending_rows <= 4, "{} rows", health.pending_rows);
            assert!(health.pending_proposals <= 1);
            recorded.exhausted()
        })
        .unwrap()
        .unwrap();
    assert_eq!(peak, 4);
    assert_eq!(owner.health().receipt_sequence, 7201);
    assert_eq!(
        recorded
            .writes()
            .iter()
            .filter(|(_, w)| w.contains("\"proposal\":1"))
            .count(),
        3
    );
    let facts = ledger(&owner)
        .into_iter()
        .filter(|e| matches!(e.kind, EventKind::CashObserved { .. }))
        .collect::<Vec<_>>();
    assert_eq!(facts.len(), 3);
    assert_eq!(
        facts.iter().map(|e| e.time_micros).collect::<Vec<_>>(),
        [START, START + 3_600_000_000, START + 7_200_000_000]
    );
}

#[test]
fn claim_only_unique_purchase_then_restored_purchase_and_confirmed_prefixes_match() {
    let fixture = Fixture::new("r2-recovered-prefixes");
    let mut control = FakeControl::new(START);
    let (predecessor, claim) = claim_prefix(&fixture, &control);
    let prefix = predecessor.records().iter().take_while(|r| !matches!(&r.kind, RecordKind::Ledger{event} if matches!(event.kind,EventKind::Signal{..}))).cloned().collect::<Vec<_>>();
    drop(predecessor);
    assert!(
        control
            .release(key(), "synthetic-owner", claim.token)
            .unwrap()
    );
    reset_prefix(&fixture, &prefix, "claim-only");
    let source = rows(&matching_log());
    let mut input = startup(START, json!([purchase()]), true, "9990");
    input.push(source[4].clone());
    input.push(source[7].clone());
    input.push(source[5].clone());
    input.extend_from_slice(&source[8..]);
    let recorded = RecordedConnector::from_jsonl(&log(&input)).unwrap();
    let mut owner = runtime(&fixture, live::Mode::Paper, &recorded, control);
    let completed = owner.run_until(|_| recorded.exhausted()).unwrap().unwrap();
    let expected = ledger(&owner);
    let records = owner.records().to_vec();
    let uncertain = expected
        .iter()
        .find_map(|e| match &e.kind {
            EventKind::PossiblySent { source, .. } => Some(source.id.as_str()),
            _ => None,
        })
        .unwrap();
    let purchased = expected
        .iter()
        .find_map(|e| match &e.kind {
            EventKind::Reconciled {
                source,
                resolution: Resolution::Purchased { .. },
                ..
            } => Some(source.id.as_str()),
            _ => None,
        })
        .unwrap();
    assert_eq!(uncertain, format!("recovery-uncertain:{}", claim.claim));
    assert_eq!(purchased, format!("recovery-purchase:{}", claim.claim));
    assert_eq!(owner.engine().accounts()[0].cash.to_string(), "10008.83");
    assert_eq!(owner.engine().accounts()[0].open, 0);
    assert_eq!(completed.receipt.dimensions[0].samples, 1);
    assert_eq!(completed.receipt.dimensions[3].samples, 1);
    assert_eq!(
        completed.receipt.dimensions[3].status,
        live::receipt::Status::Matched
    );
    assert!(
        !recorded
            .writes()
            .iter()
            .any(|(_, w)| w.contains("\"buy\":"))
    );
    drop(owner);
    for confirmed in [false, true] {
        let cut = records
            .iter()
            .position(|r| match &r.kind {
                RecordKind::Ledger { event } => {
                    if confirmed {
                        matches!(event.kind, EventKind::Confirmed { .. })
                    } else {
                        matches!(
                            event.kind,
                            EventKind::Reconciled {
                                resolution: Resolution::Purchased { .. },
                                ..
                            }
                        )
                    }
                }
                _ => false,
            })
            .unwrap()
            + 1;
        reset_prefix(
            &fixture,
            &records[..cut],
            if confirmed { "confirmed" } else { "purchased" },
        );
        // Actual restoration: no replay of the signal/proposal/purchase. The recovered
        // purchase alone must rebuild the contract map; Confirmed must rebuild due tracking.
        let mut input = startup(START, json!([purchase()]), true, "9990");
        input.push(source[4].clone());
        input.push(source[7].clone());
        input.push(source[5].clone());
        input.extend_from_slice(&source[8..]);
        let mut control = FakeControl::new(START);
        let lease = control
            .acquire(key(), "synthetic-owner", &claim.deployment, 60_000_000)
            .unwrap()
            .unwrap();
        let mut accepted = claim.clone();
        accepted.token = lease.token;
        accepted.state = ClaimState::Accepted;
        accepted.contract_ref = Some("12859891379".into());
        accepted.transaction_ref = Some("24655144239".into());
        control
            .claim(key(), "synthetic-owner", lease.token, &accepted)
            .unwrap();
        control
            .release(key(), "synthetic-owner", lease.token)
            .unwrap();
        let recorded = RecordedConnector::from_jsonl(&log(&input)).unwrap();
        let mut restored = runtime(&fixture, live::Mode::Paper, &recorded, control);
        let resumed = restored
            .run_until(|_| recorded.exhausted())
            .unwrap()
            .unwrap();
        assert_eq!(ledger(&restored), expected, "confirmed={confirmed}");
        assert_eq!(
            resumed.receipt.to_json(),
            completed.receipt.to_json(),
            "confirmed={confirmed}"
        );
        assert_eq!(
            restored
                .records()
                .iter()
                .filter(|r| matches!(r.kind, RecordKind::DueTick { .. }))
                .count(),
            1
        );
        assert_eq!(restored.engine().accounts()[0].cash.to_string(), "10008.83");
        assert!(restored.engine().accounts()[0].paid_basis.is_zero());
        assert!(restored.engine().accounts()[0].reserved.is_zero());
        assert_eq!(restored.engine().accounts()[0].open, 0);
        assert!(
            !recorded
                .writes()
                .iter()
                .any(|(_, w)| w.contains("\"buy\":"))
        );
    }
}

#[test]
fn refused_and_due_tick_replay_prefixes_and_refused_restoration_keep_full_receipts() {
    for rejected in [false, true] {
        let fixture = Fixture::new(if rejected {
            "r2-refused-rate"
        } else {
            "r2-refused-offer"
        });
        let mut input = rows(&matching_log());
        input.truncate(6);
        if rejected {
            input[5] = refusal(START);
        } else {
            input[5]["frame"] = json!(change(
                input[5]["frame"].as_str().unwrap(),
                "proposal",
                "payout",
                "19.53"
            ));
        }
        let recorded = RecordedConnector::from_jsonl(&log(&input)).unwrap();
        let mut owner = runtime(
            &fixture,
            live::Mode::Replay,
            &recorded,
            FakeControl::new(START),
        );
        let completed = owner.run_until(|_| recorded.exhausted()).unwrap().unwrap();
        let expected = ledger(&owner);
        let records = owner.records().to_vec();
        let cut = records
            .iter()
            .position(|r| matches!(r.kind, RecordKind::Refused { .. }))
            .unwrap()
            + 1;
        drop(owner);
        for replay in [true, false] {
            let prefix = if replay {
                &records[..cut]
            } else {
                &records[..]
            };
            reset_prefix(
                &fixture,
                prefix,
                if replay {
                    "refused-replay"
                } else {
                    "refused-restored"
                },
            );
            let restart = if replay {
                input.clone()
            } else {
                startup(START, json!([]), false, "10000")
            };
            let recorded = RecordedConnector::from_jsonl(&log(&restart)).unwrap();
            let mut owner = runtime(
                &fixture,
                if replay {
                    live::Mode::Replay
                } else {
                    live::Mode::Paper
                },
                &recorded,
                FakeControl::new(START),
            );
            let resumed = owner.run_until(|_| recorded.exhausted()).unwrap().unwrap();
            assert_eq!(ledger(&owner), expected);
            assert_eq!(resumed.receipt.to_json(), completed.receipt.to_json());
            assert_eq!(
                owner
                    .records()
                    .iter()
                    .filter(|r| matches!(r.kind, RecordKind::Refused { .. }))
                    .count(),
                1
            );
        }
    }
    let fixture = Fixture::new("r2-due-replay");
    let recorded = RecordedConnector::from_jsonl(&matching_log()).unwrap();
    let mut owner = runtime(
        &fixture,
        live::Mode::Replay,
        &recorded,
        FakeControl::new(START),
    );
    let completed = owner.run_until(|_| recorded.exhausted()).unwrap().unwrap();
    let expected = ledger(&owner);
    let records = owner.records().to_vec();
    let cut = records
        .iter()
        .position(|r| matches!(r.kind, RecordKind::DueTick { .. }))
        .unwrap()
        + 1;
    drop(owner);
    reset_prefix(&fixture, &records[..cut], "due-replay");
    let recorded = RecordedConnector::from_jsonl(&matching_log()).unwrap();
    let mut owner = runtime(
        &fixture,
        live::Mode::Replay,
        &recorded,
        FakeControl::new(START),
    );
    let resumed = owner.run_until(|_| recorded.exhausted()).unwrap().unwrap();
    assert_eq!(ledger(&owner), expected);
    assert_eq!(resumed.receipt.to_json(), completed.receipt.to_json());
    assert_eq!(
        owner
            .records()
            .iter()
            .filter(|r| matches!(r.kind, RecordKind::DueTick { .. }))
            .count(),
        1
    );
}

struct MarketProbe {
    inner: Box<dyn MarketDataBroker>,
    panic: bool,
    gate: Option<(Arc<AtomicBool>, i64)>,
    consumed: Option<Arc<AtomicBool>>,
    subscribe_gate: Option<(std::sync::mpsc::Sender<()>, std::sync::mpsc::Receiver<()>)>,
    dropped: Arc<AtomicBool>,
}
impl Drop for MarketProbe {
    fn drop(&mut self) {
        self.dropped.store(true, Ordering::SeqCst);
    }
}
impl MarketDataBroker for MarketProbe {
    fn discover(&mut self) -> Result<Vec<broker::DiscoveredInstrument>, String> {
        self.inner.discover()
    }
    fn history_page(
        &mut self,
        id: &InstrumentId,
        scale: PriceScale,
        before: Option<i64>,
    ) -> Result<broker::HistoryPage, String> {
        self.inner.history_page(id, scale, before)
    }
    fn subscribe(&mut self, id: &InstrumentId, scale: PriceScale) -> Result<(), String> {
        if let Some((started, release)) = self.subscribe_gate.take() {
            started.send(()).unwrap();
            release
                .recv_timeout(std::time::Duration::from_secs(3))
                .unwrap();
        }
        self.inner.subscribe(id, scale)
    }
    fn next_live(&mut self, timeout: i64) -> Result<Option<broker::LiveEvent>, String> {
        assert!(!self.panic, "synthetic market panic");
        let event = self.inner.next_live(timeout)?;
        if event.is_some()
            && let Some(consumed) = &self.consumed
        {
            consumed.store(true, Ordering::SeqCst);
        }
        if let Some((gate, at)) = &self.gate
            && matches!(&event, Some(broker::LiveEvent::Observation(event)) if event.provider_time_micros >= *at)
        {
            let limit = std::time::Instant::now() + std::time::Duration::from_secs(3);
            while !gate.load(Ordering::SeqCst) && std::time::Instant::now() < limit {
                std::thread::sleep(std::time::Duration::from_millis(1));
            }
            assert!(
                gate.load(Ordering::SeqCst),
                "synthetic upload failure did not reach owner"
            );
        }
        Ok(event)
    }
    fn unsubscribe(&mut self, id: &InstrumentId) -> Result<broker::Cancellation, String> {
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
fn market_panic_is_an_error_and_brokers_join_before_publication() {
    for panic in [true, false] {
        let fixture = Fixture::new(if panic {
            "r2-market-panic"
        } else {
            "r2-market-join"
        });
        let recorded = RecordedConnector::from_jsonl(&matching_log()).unwrap();
        let dropped = Arc::new(AtomicBool::new(false));
        let probe = dropped.clone();
        let mut owner = runtime_with(
            &fixture,
            live::Mode::Replay,
            &recorded,
            Box::new(FakeControl::new(START)),
            |_| {},
            move |inner| {
                Box::new(MarketProbe {
                    inner,
                    panic,
                    gate: None,
                    consumed: None,
                    subscribe_gate: None,
                    dropped: probe,
                })
            },
        )
        .unwrap();
        let result = owner.run_until(|_| recorded.exhausted());
        if panic {
            assert!(result.err().unwrap().contains("market worker panicked"));
            assert!(
                !fixture
                    .scratch
                    .path("published/live")
                    .join(owner.definition.deployment.clone())
                    .join("final")
                    .exists()
            );
        } else {
            assert!(result.unwrap().is_some());
        }
        assert!(dropped.load(Ordering::SeqCst));
    }
}

#[test]
fn shutdown_during_initial_subscription_is_not_a_replay_failure() {
    let fixture = Fixture::new("r5-subscription-shutdown");
    let recorded = RecordedConnector::from_jsonl(&matching_log()).unwrap();
    let (started_tx, started_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let mut owner = runtime_with(
        &fixture,
        live::Mode::Replay,
        &recorded,
        Box::new(FakeControl::new(START)),
        |_| {},
        move |inner| {
            Box::new(MarketProbe {
                inner,
                panic: false,
                gate: None,
                consumed: None,
                subscribe_gate: Some((started_tx, release_rx)),
                dropped: Arc::new(AtomicBool::new(false)),
            })
        },
    )
    .unwrap();
    started_rx
        .recv_timeout(std::time::Duration::from_secs(3))
        .unwrap();
    std::thread::scope(|scope| {
        scope.spawn(|| {
            let clock = recorded.clock();
            let limit = std::time::Instant::now() + std::time::Duration::from_secs(3);
            while clock.failure().is_none() {
                assert!(
                    std::time::Instant::now() < limit,
                    "owner did not cancel replay"
                );
                std::thread::yield_now();
            }
            release_tx.send(()).unwrap();
        });
        owner.finish().unwrap();
    });
    assert!(!owner.records().iter().any(|record| matches!(
        &record.kind, RecordKind::Discontinuity { reason } if reason == "recorded transport stopped"
    )));
    assert!(!ledger(&owner).iter().any(|event| matches!(
        event.kind,
        EventKind::Accepted { .. } | EventKind::PossiblySent { .. }
    )));
}

#[test]
fn same_base_row_evaluates_three_bindings_in_frozen_order() {
    let fixture = Fixture::new("r2-same-row-order");
    let source = rows(&matching_log());
    let mut input = startup(START, json!([]), false, "10000");
    input.push(source[4].clone());
    for i in 0..3 {
        let text = source[5]["frame"]
            .as_str()
            .unwrap()
            .replace("fixture-id-2", &format!("same-row-{i}"));
        input.push(account(
            START,
            &crate::common::broker::replace(&text, "req_id", &(40 + i).to_string()),
        ));
    }
    let recorded = RecordedConnector::from_jsonl(&log(&input)).unwrap();
    let mut owner = runtime_with(
        &fixture,
        live::Mode::Paper,
        &recorded,
        Box::new(FakeControl::new(START)),
        |definition| {
            let first = definition.policy.replay.bindings[0].clone();
            let strategy = definition.policy.replay.strategies[0].clone();
            definition.policy.replay.strategies = ["z-first", "a-second", "m-third"]
                .iter()
                .enumerate()
                .map(|(i, id)| {
                    let mut strategy = strategy.clone();
                    strategy.id = (*id).into();
                    let mut condition = strategy.repair.remove(0);
                    condition.threshold =
                        binary_alpha_engine::execution::Threshold::Number(0.08 + i as f64 * 0.01);
                    strategy.conditions.push(condition);
                    strategy
                })
                .collect();
            definition.policy.replay.bindings = ["z-first", "a-second", "m-third"]
                .iter()
                .map(|id| {
                    let mut b = first.clone();
                    b.id = (*id).into();
                    b.strategy = (*id).into();
                    b
                })
                .collect();
            definition.definition.replay = definition.policy.replay.clone();
        },
        |m| m,
    )
    .unwrap();
    owner.run_until(|_| recorded.exhausted()).unwrap().unwrap();
    let signals = ledger(&owner)
        .into_iter()
        .filter_map(|e| match e.kind {
            EventKind::Signal {
                binding,
                close_time_micros,
                ..
            } => Some((binding, close_time_micros)),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(
        signals,
        vec![
            ("z-first".into(), START),
            ("a-second".into(), START),
            ("m-third".into(), START)
        ]
    );
    assert_eq!(owner.health().receipt_sequence, 1);
    assert_eq!(
        recorded
            .writes()
            .iter()
            .filter(|(_, w)| w.contains("\"proposal\":1"))
            .count(),
        3
    );
}

#[test]
fn idle_polls_do_not_rewrite_health() {
    let fixture = Fixture::new("r2-idle-health");
    let recorded =
        RecordedConnector::from_jsonl(&log(&startup(START, json!([]), false, "10000"))).unwrap();
    let mut owner = runtime(
        &fixture,
        live::Mode::Paper,
        &recorded,
        FakeControl::new(START),
    );
    let path = fixture.scratch.path("journal/health.json");
    let mut before = fs::metadata(&path).unwrap().modified().unwrap();
    let mut bytes = fs::read(&path).unwrap();
    let mut polls = 0;
    owner
        .run_until(|_| {
            let current = fs::read(&path).unwrap();
            let modified = fs::metadata(&path).unwrap().modified().unwrap();
            if current == bytes {
                assert_eq!(modified, before);
            } else {
                bytes = current;
                before = modified;
            }
            polls += 1;
            polls == 10
        })
        .unwrap()
        .unwrap();
    assert_eq!(polls, 10);
}

use binary_alpha_app::broker::transport::{Connector, Frame, Transport};
struct EofConnector {
    inner: Box<dyn Connector>,
    reads: Arc<std::sync::atomic::AtomicUsize>,
}
struct EofTransport {
    inner: Box<dyn Transport>,
    dead: bool,
    reads: Arc<std::sync::atomic::AtomicUsize>,
}
impl Connector for EofConnector {
    fn connect(
        &mut self,
        url: &str,
        headers: &[(String, String)],
    ) -> Result<Box<dyn Transport>, String> {
        Ok(Box::new(EofTransport {
            inner: self.inner.connect(url, headers)?,
            dead: false,
            reads: self.reads.clone(),
        }))
    }
}
impl Transport for EofTransport {
    fn send(&mut self, frame: Frame) -> Result<(), String> {
        self.inner.send(frame)
    }
    fn receive(&mut self, timeout: i64) -> Result<Option<Frame>, String> {
        if self.dead {
            self.reads.fetch_add(1, Ordering::SeqCst);
            return Ok(Some(Frame::Close));
        }
        let next = self.inner.receive(timeout)?;
        if matches!(&next,Some(Frame::Text(text)) if text.contains("\"msg_type\":\"proposal\"")) {
            self.dead = true;
        }
        Ok(next)
    }
    fn close(&mut self) -> Result<(), String> {
        self.inner.close()
    }
}
#[test]
fn account_eof_is_recorded_once_while_market_observation_continues() {
    let fixture = Fixture::new("r2-account-eof");
    let source = rows(&matching_log());
    let mut input = startup(START, json!([]), false, "10000");
    input.extend_from_slice(&source[4..6]);
    for i in 1..=10 {
        input.push(tick(START + i * 1_000_000));
    }
    let recorded = RecordedConnector::from_jsonl(&log(&input)).unwrap();
    let reads = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let probe = reads.clone();
    let mut owner = support::runtime_with_io(
        &fixture,
        live::Mode::Paper,
        &recorded,
        Box::new(FakeControl::new(START)),
        |_| {},
        |m| m,
        move |inner| {
            Box::new(EofConnector {
                inner,
                reads: probe,
            })
        },
    )
    .unwrap();
    let mut idle = 0;
    owner
        .run_until(|_| {
            if recorded.exhausted() {
                idle += 1;
            }
            idle > 10
        })
        .unwrap()
        .unwrap();
    assert_eq!(reads.load(Ordering::SeqCst), 1);
    assert_eq!(owner.records().iter().filter(|r|matches!(&r.kind,RecordKind::Discontinuity{reason} if reason=="deriv: connection closed")).count(),1);
    assert_eq!(owner.health().receipt_sequence, 11);
    assert!(
        matches!(owner.health().entries,live::Entries::Disabled(ref r) if r.contains("account connection unavailable"))
    );
}

#[test]
fn successful_loss_reconciliation_keeps_failed_contract_subscription_veto() {
    let mut fixture = Fixture::new("r2-independent-vetoes");
    fixture
        .config
        .live
        .as_mut()
        .unwrap()
        .control
        .renewal_interval_micros = 1_000_000;
    let recorded = RecordedConnector::from_jsonl(&support::zero_credit_log()).unwrap();
    let mut owner = runtime(
        &fixture,
        live::Mode::Replay,
        &recorded,
        FakeControl::new(START),
    );
    owner.run_until(|_| recorded.exhausted()).unwrap().unwrap();
    let records = owner.records().to_vec();
    let cut=records.iter().position(|r|matches!(&r.kind,RecordKind::Ledger{event} if matches!(event.kind,EventKind::Unresolved{terminal:Some(_),..}))).unwrap()+1;
    drop(owner);
    reset_prefix(&fixture, &records[..cut], "unpaid-loss");
    let at = START + 5_000_000;
    let mut input = startup(at, json!([]), false, "9990");
    input.push(account(at,r#"{"msg_type":"proposal_open_contract","req_id":6,"error":{"code":"ContractUnavailable"}}"#));
    input.push(tick(START + 7_000_000));
    input.push(refusal(START + 7_000_000));
    input.push(portfolio(START + 7_000_000, json!([])));
    input.push(statement(START + 7_000_000));
    let recorded = RecordedConnector::from_jsonl(&log(&input)).unwrap();
    let mut owner = runtime(&fixture, live::Mode::Paper, &recorded, FakeControl::new(at));
    // Preserve the earlier completed baseline manifest separately: this fixture now measures
    // a different broker subscription response and therefore different receipt evidence.
    let manifest = fixture
        .scratch
        .path("published/manifests")
        .join(&owner.definition.manifest.definition);
    if manifest.exists() {
        fs::rename(
            &manifest,
            fixture.scratch.path("completed-loss-ledger-manifest"),
        )
        .unwrap();
    }
    let local_manifest = fixture
        .scratch
        .path("local/manifests")
        .join(&owner.definition.manifest.definition);
    if local_manifest.exists() {
        fs::rename(
            &local_manifest,
            fixture.scratch.path("completed-local-loss-ledger-manifest"),
        )
        .unwrap();
    }
    owner.run_until(|_| recorded.exhausted()).unwrap().unwrap();
    assert!(ledger(&owner).iter().any(|e| matches!(
        e.kind,
        EventKind::Reconciled {
            resolution: Resolution::Settled { .. },
            ..
        }
    )));
    assert!(
        matches!(owner.health().entries,live::Entries::Disabled(ref reasons) if reasons.contains("contract subscription unavailable: 12859891379") && !reasons.contains("broker recovery unavailable"))
    );
    assert_eq!(owner.engine().accounts()[0].open, 0);
    assert_eq!(owner.engine().accounts()[0].cash.to_string(), "9990.00");
}

struct ControlProbe {
    inner: FakeControl,
    writer: std::thread::ThreadId,
    updates: Arc<std::sync::Mutex<Vec<ClaimState>>>,
    released: Arc<AtomicBool>,
    brokers_dropped: Arc<AtomicBool>,
}
impl Control for ControlProbe {
    fn advance_to(&mut self, at: i64) {
        self.inner.advance_to(at);
    }
    fn acquire(
        &mut self,
        key: LeaseKey<'_>,
        owner: &str,
        deployment: &str,
        ttl: i64,
    ) -> Result<Option<live::control::Lease>, String> {
        self.inner.acquire(key, owner, deployment, ttl)
    }
    fn renew(
        &mut self,
        key: LeaseKey<'_>,
        owner: &str,
        token: u64,
        ttl: i64,
    ) -> Result<Option<live::control::Lease>, String> {
        self.inner.renew(key, owner, token, ttl)
    }
    fn release(&mut self, key: LeaseKey<'_>, owner: &str, token: u64) -> Result<bool, String> {
        assert_eq!(self.writer, std::thread::current().id());
        assert!(
            self.brokers_dropped.load(Ordering::SeqCst),
            "broker join must precede lease release/publication"
        );
        assert_eq!(
            self.updates.lock().unwrap().last(),
            Some(&ClaimState::Reconciled)
        );
        self.released.store(true, Ordering::SeqCst);
        self.inner.release(key, owner, token)
    }
    fn claim(
        &mut self,
        key: LeaseKey<'_>,
        owner: &str,
        token: u64,
        claim: &Claim,
    ) -> Result<live::control::ClaimOutcome, String> {
        self.inner.claim(key, owner, token, claim)
    }
    fn update_claim(
        &mut self,
        key: LeaseKey<'_>,
        owner: &str,
        token: u64,
        claim: &str,
        state: ClaimState,
        contract: Option<&str>,
        transaction: Option<&str>,
    ) -> Result<bool, String> {
        assert_eq!(
            self.writer,
            std::thread::current().id(),
            "storage worker must not own claim mutation"
        );
        assert!(!self.released.load(Ordering::SeqCst));
        self.updates.lock().unwrap().push(state);
        self.inner
            .update_claim(key, owner, token, claim, state, contract, transaction)
    }
    fn unresolved(&mut self, key: LeaseKey<'_>) -> Result<Vec<Claim>, String> {
        self.inner.unresolved(key)
    }
    fn delete_reconciled(&mut self, key: LeaseKey<'_>, claim: &str) -> Result<(), String> {
        self.inner.delete_reconciled(key, claim)
    }
}
#[test]
fn failed_segment_retries_on_cadence_and_owner_claim_updates_finish_before_release() {
    let mut fixture = Fixture::new("r2-storage-retry-owner-control");
    fixture
        .config
        .live
        .as_mut()
        .unwrap()
        .journal
        .segment_records = 4;
    let mut input = rows(&matching_log());
    for i in (1..5).rev() {
        input.insert(8, tick(START + i * 1_000_000));
    }
    for i in 6..=20 {
        input.push(tick(START + i * 1_000_000));
    }
    let mut reply = refusal(START + 20_000_000);
    reply["frame"] = json!(crate::common::broker::replace(
        reply["frame"].as_str().unwrap(),
        "req_id",
        "44"
    ));
    input.push(reply);
    let recorded = RecordedConnector::from_jsonl(&log(&input)).unwrap();
    let repaired = Arc::new(AtomicBool::new(false));
    let dropped = Arc::new(AtomicBool::new(false));
    let released = Arc::new(AtomicBool::new(false));
    let updates = Arc::new(std::sync::Mutex::new(Vec::new()));
    let control = ControlProbe {
        inner: FakeControl::new(START),
        writer: std::thread::current().id(),
        updates: updates.clone(),
        released: released.clone(),
        brokers_dropped: dropped.clone(),
    };
    let gate = repaired.clone();
    let mut owner = runtime_with(
        &fixture,
        live::Mode::Replay,
        &recorded,
        Box::new(control),
        |_| {},
        move |inner| {
            Box::new(MarketProbe {
                inner,
                panic: false,
                gate: Some((gate, START + 5_000_000)),
                consumed: None,
                subscribe_gate: None,
                dropped,
            })
        },
    )
    .unwrap();
    let cloud = fixture
        .scratch
        .path("published/live")
        .join(&owner.definition.deployment)
        .join("journal");
    fs::create_dir_all(cloud.parent().unwrap()).unwrap();
    fs::write(&cloud, b"synthetic unavailable upload destination").unwrap();
    let verified_before_release = Arc::new(AtomicBool::new(false));
    let verified = verified_before_release.clone();
    let release = released.clone();
    let repair = repaired.clone();
    owner.hook = Some(Box::new(move |point| {
        if point == live::Checkpoint::AfterUploadVerification
            && repair.load(Ordering::SeqCst)
            && !release.load(Ordering::SeqCst)
        {
            verified.store(true, Ordering::SeqCst);
        }
        false
    }));
    let mut failures = 0;
    let completed = owner
        .run_until(|health| {
            if health.cloud_failed_segments > 0 && !repaired.load(Ordering::SeqCst) {
                failures = health.cloud_failed_segments;
                fs::rename(&cloud, fixture.scratch.path("failed-upload-destination")).unwrap();
                fs::create_dir_all(&cloud).unwrap();
                repaired.store(true, Ordering::SeqCst);
            }
            recorded.exhausted() && verified_before_release.load(Ordering::SeqCst)
        })
        .unwrap()
        .unwrap();
    assert_eq!(failures, 1);
    assert!(verified_before_release.load(Ordering::SeqCst));
    assert!(released.load(Ordering::SeqCst));
    assert!(updates.lock().unwrap().contains(&ClaimState::Accepted));
    assert_eq!(owner.health().cloud_failed_segments, 0);
    assert_eq!(owner.health().cloud_pending_segments, 0);
    assert!(completed.manifest.journal_segments.len() > 1);
}

#[test]
fn ambiguous_matches_wait_for_operator_accepted_references() {
    let mut fixture = Fixture::new("r2-operator-accepted");
    fixture
        .config
        .live
        .as_mut()
        .unwrap()
        .control
        .renewal_interval_micros = 1_000_000;
    let mut control = FakeControl::new(START);
    let (predecessor, claim) = claim_prefix(&fixture, &control);
    let prefix = predecessor.records().to_vec();
    drop(predecessor);
    control
        .release(key(), "synthetic-owner", claim.token)
        .unwrap();
    reset_prefix(&fixture, &prefix, "ambiguous");
    let mut second = purchase();
    second["contract_id"] = json!(12859891380u64);
    second["transaction_id"] = json!(24655144240u64);
    let contracts = json!([purchase(), second]);
    let mut input = startup(START, contracts.clone(), true, "10000");
    input.extend([
        tick(START + 1_000_000),
        refusal(START + 1_000_000),
        portfolio(START + 1_000_000, contracts),
        statement(START + 1_000_000),
    ]);
    input.push(account(
        START + 1_000_000,
        rows(&matching_log())[7]["frame"].as_str().unwrap(),
    ));
    let recorded = RecordedConnector::from_jsonl(&log(&input)).unwrap();
    let mut owner = runtime(&fixture, live::Mode::Paper, &recorded, control.clone());
    assert!(!ledger(&owner).iter().any(|e| matches!(
        e.kind,
        EventKind::Reconciled {
            resolution: Resolution::Purchased { .. },
            ..
        }
    )));
    assert!(
        matches!(owner.health().entries,live::Entries::Disabled(ref reasons) if reasons.contains("unresolved dispatch claim"))
    );
    assert!(
        control
            .update_claim(
                key(),
                "synthetic-owner",
                owner.health().fencing_token,
                &claim.command,
                ClaimState::Accepted,
                Some("12859891379"),
                Some("24655144239")
            )
            .unwrap()
    );
    owner.run_until(|_| recorded.exhausted()).unwrap().unwrap();
    let purchases = ledger(&owner)
        .into_iter()
        .filter_map(|e| match e.kind {
            EventKind::Reconciled {
                resolution: Resolution::Purchased { liability, .. },
                ..
            } => Some(liability),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(purchases.len(), 1);
    assert_eq!(purchases[0].contract_ref, "12859891379");
    assert_eq!(owner.engine().accounts()[0].cash.to_string(), "9990.00");
    assert!(owner.engine().accounts()[0].reserved.is_zero());
    assert_eq!(owner.engine().accounts()[0].paid_basis.to_string(), "10.00");
    assert_eq!(owner.engine().accounts()[0].open, 1);
}

#[test]
fn bound_unready_present_value_keeps_runtime_warmup_veto() {
    let fixture = Fixture::new("r2-bound-unready");
    let source = rows(&matching_log());
    let mut input = startup(START, json!([]), false, "10000");
    input.extend_from_slice(&source[4..6]);
    let recorded = RecordedConnector::from_jsonl(&log(&input)).unwrap();
    let mut owner = runtime_with(
        &fixture,
        live::Mode::Paper,
        &recorded,
        Box::new(FakeControl::new(START)),
        |definition| {
            let column = definition.definition.instruments[0].streams[0]
                .columns
                .iter_mut()
                .find(|c| c.name == "candle_direction")
                .unwrap();
            column.unready.push("up".into());
        },
        |m| m,
    )
    .unwrap();
    owner.run_until(|_| recorded.exhausted()).unwrap().unwrap();
    assert!(!owner.health().warmup);
    assert!(
        matches!(owner.health().entries,live::Entries::Disabled(ref reasons) if reasons.contains("causal warmup is incomplete"))
    );
    assert_eq!(owner.health().receipt_sequence, 1);
    assert_eq!(owner.engine().accounts()[0].open, 0);
}

#[test]
fn shutdown_applies_a_consumed_base_row_without_new_broker_work() {
    let fixture = Fixture::new("shutdown-consumed-base-row");
    let input = rows(&matching_log());
    let recorded = RecordedConnector::from_jsonl(&log(&input[..5])).unwrap();
    let consumed = Arc::new(AtomicBool::new(false));
    let proceed = Arc::new(AtomicBool::new(false));
    let dropped = Arc::new(AtomicBool::new(false));
    let (seen, gate, joined) = (consumed.clone(), proceed.clone(), dropped.clone());
    let mut owner = runtime_with(
        &fixture,
        live::Mode::Replay,
        &recorded,
        Box::new(FakeControl::new(START)),
        |_| {},
        move |inner| {
            Box::new(MarketProbe {
                inner,
                panic: false,
                gate: Some((gate, START)),
                consumed: Some(seen),
                subscribe_gate: None,
                dropped: joined,
            })
        },
    )
    .unwrap();
    let limit = std::time::Instant::now() + std::time::Duration::from_secs(3);
    while !consumed.load(Ordering::SeqCst) && std::time::Instant::now() < limit {
        std::thread::yield_now();
    }
    assert!(consumed.load(Ordering::SeqCst));
    assert_eq!(owner.health().receipt_sequence, 0);
    let completed = owner
        .run_until(|_| {
            proceed.store(true, Ordering::SeqCst);
            true
        })
        .unwrap()
        .unwrap();
    assert!(dropped.load(Ordering::SeqCst));
    assert_eq!(owner.health().receipt_sequence, 1);
    assert!(owner.health().warmup);
    assert!(ledger(&owner).iter().any(|e| matches!(
        e.kind,
        EventKind::Signal {
            close_time_micros: START,
            ..
        }
    )));
    assert!(
        !recorded
            .writes()
            .iter()
            .any(|(_, w)| w.contains("\"proposal\":1") || w.contains("\"buy\":"))
    );
    assert_eq!(owner.engine().accounts()[0].open, 0);
    assert!(std::path::Path::new(completed.manifest_uri.strip_prefix("file://").unwrap()).exists());
}

#[test]
fn expectation_free_duplicate_response_keeps_its_original_request() {
    let response = |id| {
        json!({"msg_type":"proposal","req_id":id,"proposal":{"id":format!("P{id}")}}).to_string()
    };
    let input = [
        account(START, &response(1)),
        account(START, &response(1)),
        account(START, &response(2)),
    ];
    assert!(input.iter().all(|r| r.get("expect").is_none()));
    let recorded = RecordedConnector::from_jsonl(&log(&input)).unwrap();
    let mut account = recorded.session("account").unwrap();
    let send = |id| Frame::Text(json!({"proposal":1,"req_id":id}).to_string());
    let receive = |transport: &mut RecordedConnector| {
        let Some(Frame::Text(text)) = transport.receive(1_000_000).unwrap() else {
            panic!("missing response")
        };
        serde_json::from_str::<Value>(&text).unwrap()
    };
    account.send(send(11)).unwrap();
    let first = receive(&mut account);
    recorded.clock().complete();
    account.send(send(12)).unwrap();
    let duplicate = receive(&mut account);
    let second = receive(&mut account);
    recorded.clock().complete();
    assert_eq!(first["req_id"], 11);
    assert_eq!(duplicate, first);
    assert_eq!(second["req_id"], 12);
    assert_eq!(second["proposal"]["id"], "P2");
    assert!(recorded.exhausted());
}

struct RecoveryProbe {
    inner: FakeControl,
    reads: std::rc::Rc<std::cell::Cell<usize>>,
    lose_first: bool,
    reconciled_updates: std::rc::Rc<std::cell::Cell<usize>>,
}
impl Control for RecoveryProbe {
    fn advance_to(&mut self, at: i64) {
        self.inner.advance_to(at);
    }
    fn acquire(
        &mut self,
        key: LeaseKey<'_>,
        owner: &str,
        deployment: &str,
        ttl: i64,
    ) -> Result<Option<live::control::Lease>, String> {
        self.inner.acquire(key, owner, deployment, ttl)
    }
    fn renew(
        &mut self,
        key: LeaseKey<'_>,
        owner: &str,
        token: u64,
        ttl: i64,
    ) -> Result<Option<live::control::Lease>, String> {
        self.inner.renew(key, owner, token, ttl)
    }
    fn release(&mut self, key: LeaseKey<'_>, owner: &str, token: u64) -> Result<bool, String> {
        self.inner.release(key, owner, token)
    }
    fn claim(
        &mut self,
        key: LeaseKey<'_>,
        owner: &str,
        token: u64,
        claim: &Claim,
    ) -> Result<live::control::ClaimOutcome, String> {
        self.inner.claim(key, owner, token, claim)
    }
    fn update_claim(
        &mut self,
        key: LeaseKey<'_>,
        owner: &str,
        token: u64,
        command: &str,
        state: ClaimState,
        contract: Option<&str>,
        transaction: Option<&str>,
    ) -> Result<bool, String> {
        if state == ClaimState::Reconciled {
            self.reconciled_updates
                .set(self.reconciled_updates.get() + 1);
        }
        self.inner
            .update_claim(key, owner, token, command, state, contract, transaction)
    }
    fn unresolved(&mut self, key: LeaseKey<'_>) -> Result<Vec<Claim>, String> {
        let count = self.reads.get();
        self.reads.set(count + 1);
        if self.lose_first && count == 0 {
            self.inner.fault(Fault::LoseResponse);
        }
        self.inner.unresolved(key)
    }
    fn delete_reconciled(&mut self, key: LeaseKey<'_>, command: &str) -> Result<(), String> {
        self.inner.delete_reconciled(key, command)
    }
}

#[test]
fn lost_initial_claim_read_reconstructs_missing_exposure_on_refresh() {
    let mut fixture = Fixture::new("lost-initial-claim-read");
    fixture
        .config
        .live
        .as_mut()
        .unwrap()
        .control
        .renewal_interval_micros = 1_000_000;
    let mut control = FakeControl::new(START);
    let (predecessor, claim) = claim_prefix(&fixture, &control);
    drop(predecessor);
    control
        .release(key(), "synthetic-owner", claim.token)
        .unwrap();
    fs::rename(
        fixture.scratch.path("journal"),
        fixture.scratch.path("predecessor-journal"),
    )
    .unwrap();
    authorize(&fixture);
    let source = rows(&matching_log());
    let mut input = source[..4].to_vec();
    input.push(tick(START + 1_000_000));
    input.push(account(
        START + 1_000_000,
        source[5]["frame"].as_str().unwrap(),
    ));
    input.push(portfolio(START + 1_000_000, json!([])));
    input.push(statement(START + 1_000_000));
    let recorded = RecordedConnector::from_jsonl(&log(&input)).unwrap();
    let reads = std::rc::Rc::new(std::cell::Cell::new(0));
    let mut owner = runtime_with(
        &fixture,
        live::Mode::Live,
        &recorded,
        Box::new(RecoveryProbe {
            inner: control.clone(),
            reads: reads.clone(),
            lose_first: true,
            reconciled_updates: Default::default(),
        }),
        |_| {},
        |m| m,
    )
    .unwrap();
    assert_eq!(reads.get(), 1);
    assert_eq!(owner.engine().accounts()[0].open, 0);
    assert!(
        matches!(owner.health().entries, live::Entries::Disabled(ref reasons) if reasons.contains("claim recovery unavailable"))
    );
    owner.run_until(|_| recorded.exhausted()).unwrap().unwrap();
    assert!(reads.get() >= 2);
    assert_eq!(owner.engine().accounts()[0].open, 1);
    assert_eq!(owner.engine().accounts()[0].reserved.to_string(), "10.00");
    assert!(ledger(&owner).iter().any(
        |e| matches!(&e.kind, EventKind::PossiblySent { command, .. } if command == &claim.command)
    ));
    assert_eq!(
        control.unresolved(key()).unwrap()[0].state,
        ClaimState::PossiblySent
    );
    assert!(
        matches!(owner.health().entries, live::Entries::Disabled(ref reasons) if !reasons.contains("claim recovery unavailable") && reasons.contains("unresolved dispatch claim"))
    );
    assert!(
        !recorded
            .writes()
            .iter()
            .any(|(_, w)| w.contains("\"buy\":"))
    );
}

#[test]
fn partial_tail_stays_local_and_retains_claim_for_host_loss_recovery() {
    let fixture = Fixture::new("partial-tail-host-loss");
    let mut control = FakeControl::new(START);
    let (predecessor, claim) = claim_prefix(&fixture, &control);
    let prefix = predecessor.records().to_vec();
    drop(predecessor);
    control
        .release(key(), "synthetic-owner", claim.token)
        .unwrap();
    reset_prefix(&fixture, &prefix, "predecessor");
    {
        let (mut journal, _) =
            Journal::open(&fixture.scratch.path("journal"), &claim.deployment, 16).unwrap();
        for _ in 0..3 {
            journal
                .append(
                    START,
                    RecordKind::Discontinuity {
                        reason: "synthetic observed reconnect".into(),
                    },
                )
                .unwrap();
        }
    }
    let source = rows(&matching_log());
    let mut input = startup(START, json!([purchase()]), true, "9990");
    input.push(source[4].clone());
    input.push(source[7].clone());
    input.push(source[5].clone());
    input.extend_from_slice(&source[8..]);
    input.push(account(START + 5_000_000, "{"));
    let recorded = RecordedConnector::from_jsonl(&log(&input)).unwrap();
    let mut owner = runtime(&fixture, live::Mode::Paper, &recorded, control.clone());
    let completed = owner.run_until(|_| recorded.exhausted()).unwrap().unwrap();
    assert_eq!(owner.health().journal_sequence, 20);
    assert!(
        matches!(&owner.records()[17].kind, RecordKind::Ledger { event } if matches!(event.kind, EventKind::Settled { .. } | EventKind::Reconciled { resolution: Resolution::Settled { .. }, .. }))
    );
    assert_eq!(completed.manifest.journal_segments.len(), 1);
    assert!(
        completed.manifest.journal_segments[0]
            .key
            .ends_with("00000000000000000001-00000000000000000016.jsonl")
    );
    let tail = completed.manifest.open_tail.as_ref().unwrap();
    assert_eq!((tail.first_sequence, tail.last_sequence), (17, 20));
    let bytes = fs::read(fixture.scratch.path("journal/open.jsonl")).unwrap();
    assert_eq!(
        tail.sha256,
        binary_alpha_engine::research::digest(b"", &bytes)
    );
    assert_eq!(tail.bytes, bytes.len() as u64);
    assert_eq!(
        completed.manifest.ledger_generation,
        completed
            .ledger
            .as_ref()
            .map(|ledger| ledger.generation.clone())
    );
    assert_eq!(
        control.retained_claims(key()).unwrap()[0].state,
        ClaimState::Reconciled
    );
    let cloud = fixture
        .scratch
        .path("published/live")
        .join(&claim.deployment)
        .join("journal");
    assert_eq!(fs::read_dir(&cloud).unwrap().count(), 1);
    let archived = owner.records()[..16].to_vec();
    drop(owner);
    // Preserve this synthetic host's completed evidence, then start with only cloud and control.
    fs::rename(
        fixture.scratch.path("journal"),
        fixture.scratch.path("lost-host-journal"),
    )
    .unwrap();
    let (_, destination) = fixture.stores();
    assert_eq!(
        Journal::restore(
            &fixture.scratch.path("journal"),
            &claim.deployment,
            16,
            &mut |key| {
                if destination.head(key)?.is_none() {
                    return Ok(None);
                }
                let mut bytes = Vec::new();
                destination.read_to(key, None, &mut bytes)?;
                Ok(Some(bytes))
            }
        )
        .unwrap(),
        1
    );
    assert_eq!(
        Journal::open(&fixture.scratch.path("journal"), &claim.deployment, 16)
            .unwrap()
            .1,
        archived
    );
    let mut input = startup(START + 5_000_000, json!([]), true, "10008.83");
    input.push(source[10].clone());
    input.push(source[9].clone());
    let recorded = RecordedConnector::from_jsonl(&log(&input)).unwrap();
    let mut restored = runtime(&fixture, live::Mode::Paper, &recorded, control.clone());
    restored
        .run_until(|_| recorded.exhausted())
        .unwrap()
        .unwrap();
    assert_eq!(restored.engine().accounts()[0].cash.to_string(), "10008.83");
    assert_eq!(restored.engine().accounts()[0].open, 0);
    assert!(restored.records().iter().skip(16).any(|r| matches!(&r.kind, RecordKind::Ledger { event } if matches!(event.kind, EventKind::Settled { .. } | EventKind::Reconciled { resolution: Resolution::Settled { .. }, .. }))));
    assert!(
        !recorded
            .writes()
            .iter()
            .any(|(_, w)| w.contains("\"buy\":"))
    );
}

#[test]
fn restored_not_sent_claim_updates_reconciliation_once() {
    let fixture = Fixture::new("single-reconciliation-update");
    let mut control = FakeControl::new(START);
    let (predecessor, claim) = claim_prefix(&fixture, &control);
    drop(predecessor);
    assert!(
        control
            .update_claim(
                key(),
                "synthetic-owner",
                claim.token,
                &claim.command,
                ClaimState::NotSent,
                None,
                None
            )
            .unwrap()
    );
    control
        .release(key(), "synthetic-owner", claim.token)
        .unwrap();
    let input = startup(START, json!([]), true, "10000");
    let recorded = RecordedConnector::from_jsonl(&log(&input)).unwrap();
    let updates = std::rc::Rc::new(std::cell::Cell::new(0));
    let mut owner = runtime_with(
        &fixture,
        live::Mode::Paper,
        &recorded,
        Box::new(RecoveryProbe {
            inner: control,
            reads: Default::default(),
            lose_first: false,
            reconciled_updates: updates.clone(),
        }),
        |_| {},
        |m| m,
    )
    .unwrap();
    owner.run_until(|_| recorded.exhausted()).unwrap().unwrap();
    assert_eq!(updates.get(), 1);
    assert_eq!(
        ledger(&owner)
            .iter()
            .filter(|e| matches!(
                e.kind,
                EventKind::Reconciled {
                    resolution: Resolution::NotSent,
                    ..
                }
            ))
            .count(),
        1
    );
}

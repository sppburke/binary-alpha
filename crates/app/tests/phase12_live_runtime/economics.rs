//! Economic scenario deltas through the recorded adapter, ordered owner, Engine and receipt.
use super::support::*;
use binary_alpha_app::{
    broker::transport::RecordedConnector,
    live::{self, control::FakeControl, journal::RecordKind, receipt::Status},
};
use binary_alpha_engine::{
    config::AccountClass,
    execution::{
        AccountState, Block, Comparator, Decimal, Disposition, EventKind, FinancialEvent, Threshold,
    },
    research,
};
use serde_json::{Value, json};

fn money(text: &str) -> Decimal {
    Decimal::parse(text).unwrap()
}
fn financial(state: &AccountState) -> (String, String, String, String, u32) {
    (
        state.cash.to_string(),
        state.reserved.to_string(),
        state.paid_basis.to_string(),
        state.unresolved_loss.to_string(),
        state.open,
    )
}
fn expected(
    cash: &str,
    paid: &str,
    exposure: &str,
    open: u32,
) -> (String, String, String, String, u32) {
    (
        cash.into(),
        "0.00".into(),
        paid.into(),
        exposure.into(),
        open,
    )
}
fn commands(events: &[FinancialEvent]) -> Vec<String> {
    events
        .iter()
        .filter_map(|event| match &event.kind {
            EventKind::Signal {
                command: Some(command),
                ..
            } => Some(command.clone()),
            _ => None,
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
fn run(fixture: &Fixture, rows: &[Value]) -> (live::Runtime, live::Completed, RecordedConnector) {
    let recorded = RecordedConnector::from_jsonl(&scenario_log(rows)).unwrap();
    let mut owner = runtime(
        fixture,
        live::Mode::Replay,
        &recorded,
        FakeControl::new(START),
    );
    let completed = owner.run_until(|_| recorded.exhausted()).unwrap().unwrap();
    (owner, completed, recorded)
}
fn source_hashes(fixture: &Fixture) -> Vec<String> {
    let root = fixture.scratch.path("published");
    vec![
        crate::common::sha256(&root.join(fixture.bundle.key())),
        crate::common::sha256(&root.join(research::frozen_key(&fixture.bundle.generation))),
        research::digest(
            b"",
            &object(
                &fixture.scratch.root,
                &fixture.bundle.generation,
                "research.json",
            ),
        ),
        research::digest(
            b"",
            &object(
                &fixture.scratch.root,
                &fixture.run.selection,
                "selection.json",
            ),
        ),
    ]
}
fn only_purchase(
    events: &[FinancialEvent],
    debit: &str,
    discrepancy: bool,
    deficit: Option<&str>,
) -> String {
    let accepted = events
        .iter()
        .filter(|e| matches!(e.kind, EventKind::Accepted { .. }))
        .collect::<Vec<_>>();
    assert_eq!(accepted.len(), 1);
    let EventKind::Accepted {
        command,
        debit: actual,
        reservation,
        liability,
        discrepancy: actual_discrepancy,
        deficit: actual_deficit,
        entry_time_micros,
        entry_price_units,
        due_time_micros,
        ..
    } = &accepted[0].kind
    else {
        unreachable!()
    };
    assert_eq!(*actual, money(debit));
    assert_eq!(*reservation, money("0.00"));
    assert_eq!(*actual_discrepancy, discrepancy);
    assert_eq!(*actual_deficit, deficit.map(money));
    assert_eq!(
        (*entry_time_micros, *entry_price_units, *due_time_micros),
        (None, None, None)
    );
    let liability = liability.as_ref().unwrap();
    assert_eq!(liability.contract_ref, "12859891379");
    assert_eq!(liability.transaction_ref, "24655144239");
    assert_eq!(liability.purchase_time_micros, START);
    assert_eq!(liability.payout, money("18.83"));
    command.clone()
}

#[test]
fn tick_past_due_before_delayed_confirmation_keeps_liability() {
    let fixture = Fixture::new("t2-delayed-confirmation");
    let source = scenario_rows(&matching_log());
    let mut rows = source[..9].to_vec();
    // Confirmed expiry is known, but a tick beyond it is no terminal broker fact.
    rows.push(scenario_tick(START + 6_000_000, "180.0002"));
    for mut row in source[9..].iter().cloned() {
        row["at"] = json!(START + 6_000_000);
        rows.push(row);
    }
    let recorded = RecordedConnector::from_jsonl(&scenario_log(&rows)).unwrap();
    let mut owner = runtime(
        &fixture,
        live::Mode::Replay,
        &recorded,
        FakeControl::new(START),
    );
    let mut before_terminal = None;
    let completed = owner
        .run_until(|health| {
            if health.receipt_sequence == 3 && health.risk.open == 1 {
                before_terminal = Some(financial(&health.risk));
            }
            recorded.exhausted()
        })
        .unwrap()
        .unwrap();
    assert_eq!(
        before_terminal,
        Some(expected("9990.00", "10.00", "10.00", 1))
    );
    assert_eq!(
        financial(&owner.engine().accounts()[0]),
        expected("10008.83", "0.00", "0.00", 0)
    );
    let events = ledger_events(&owner);
    let command = only_purchase(&events, "10.00", false, None);
    let settled = events
        .iter()
        .filter(|e| matches!(e.kind, EventKind::Settled { .. }))
        .collect::<Vec<_>>();
    assert_eq!(settled.len(), 1);
    assert_eq!(settled[0].time_micros, START + 6_000_000);
    assert_eq!(completed.receipt.dimensions[4].status, Status::Matched);
    assert_eq!(completed.receipt.dimensions[4].samples, 1);
    assert_eq!(completed.receipt.dimensions[4].reason, None);
    assert_eq!(
        completed.receipt.dimensions[5].reason,
        Some(format!(
            "{command}: expiry_to_evidence=1000000; evidence_to_application=0; total=1000000 microseconds"
        ))
    );
    assert_eq!(completed.receipt.promotion.reasons, ["funds_release"]);
    assert_eq!(buys(&recorded), 1);
}

#[test]
fn accepted_money_with_missing_entry_facts_is_paid_exposure() {
    let fixture = Fixture::new("t2-missing-entry");
    let mut rows = scenario_rows(&matching_log());
    rows.truncate(7);
    rows[6]["frame"] = json!(change(
        rows[6]["frame"].as_str().unwrap(),
        "buy",
        "start_time",
        "null"
    ));
    let recorded = RecordedConnector::from_jsonl(&scenario_log(&rows)).unwrap();
    let mut owner = runtime(
        &fixture,
        live::Mode::Replay,
        &recorded,
        FakeControl::new(START),
    );
    owner.hook = Some(Box::new(|point| {
        point == live::Checkpoint::AfterAcknowledgement
    }));
    assert!(owner.run_until(|_| false).unwrap().is_none());
    let events = ledger_events(&owner);
    let command = only_purchase(&events, "10.00", false, None);
    assert_eq!(
        financial(&owner.engine().accounts()[0]),
        expected("9990.00", "10.00", "10.00", 1)
    );
    assert_eq!(events.len(), 3); // Definition, admitted signal, paid liability; no Confirmed.
    let receipt = scenario_receipt(&fixture, &owner);
    assert_eq!(receipt.dimensions[2].status, Status::Unavailable);
    assert_eq!(
        receipt.dimensions[2].reason,
        Some(format!(
            "{command}: missing confirmed entry price or signal quote"
        ))
    );
    assert_eq!(receipt.dimensions[5].status, Status::Unavailable);
    assert_eq!(
        receipt.dimensions[5].reason,
        Some(format!(
            "{command}: missing confirmed expiry, unresolved liability, or missing matched cash/zero-credit reconciliation"
        ))
    );
    assert_eq!(buys(&recorded), 1);
    // PRIMARY: a successful purchase with no start/entry/expiry facts leaves entries Enabled.
    // Runtime::drain only vetoes Engine blocks or OutsideEnvelope; mandatory Unavailable
    // quote/timing/release evidence never enters the veto set (live.rs::compatibility).
    assert!(
        matches!(owner.health().entries, live::Entries::Disabled(_)),
        "missing entry facts must veto entries: {:?}",
        owner.health().entries
    );
}

#[test]
fn changing_proposal_terms_are_refused_and_recorded() {
    let fixture = Fixture::new("t2-changed-offer");
    let before = source_hashes(&fixture);
    let mut rows = scenario_rows(&matching_log());
    rows.truncate(6);
    // Retained proposal-later's exact 10/19.53 economics; only duration and epoch/spot
    // are aligned to this wholly synthetic five-second observation window.
    let mut proposal = frame("proposal-later");
    proposal = change(&proposal, "echo_req", "duration", "5");
    for key in ["date_start", "spot_time"] {
        proposal = change(&proposal, "proposal", key, &(START / 1_000_000).to_string());
    }
    proposal = change(
        &proposal,
        "proposal",
        "date_expiry",
        &(START / 1_000_000 + 5).to_string(),
    );
    proposal = change(&proposal, "proposal", "spot", "180.0000");
    rows[5]["frame"] = json!(proposal);
    let (owner, completed, recorded) = run(&fixture, &rows);
    let binding = &owner.definition.policy.replay.bindings[0];
    let refusal = owner
        .records()
        .iter()
        .filter_map(|r| match &r.kind {
            RecordKind::Refused {
                binding,
                proposal,
                reason,
            } => Some((binding, proposal.as_ref().unwrap(), reason)),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(refusal.len(), 1);
    assert_eq!(refusal[0].0, &binding.id);
    assert_eq!(refusal[0].1.terms.quoted_cost, money("10"));
    assert_eq!(refusal[0].1.terms.win.gross_return, money("19.53"));
    assert_eq!(
        refusal[0].2,
        &format!("offer differs from exact baseline {}", binding.contract)
    );
    assert_eq!(
        financial(&owner.engine().accounts()[0]),
        expected("10000.00", "0.00", "0.00", 0)
    );
    assert_eq!(buys(&recorded), 0);
    assert_eq!(commands(&ledger_events(&owner)), Vec::<String>::new());
    assert_eq!(completed.receipt.dimensions[0].status, Status::Unavailable);
    assert_eq!(
        completed.receipt.dimensions[1].status,
        Status::OutsideEnvelope
    );
    assert_eq!(
        completed.receipt.dimensions[1].reason,
        Some(format!(
            "{}: offer differs from exact baseline {}",
            binding.id, binding.contract
        ))
    );
    // Extend runtime::a_nonbaseline_offer_never_reserves_or_dispatches by distinguishing
    // a correctly refused quote from a mismatching *accepted* debit on the same baseline.
    let accepted_fixture = isolated_fixture(&fixture, "accepted-debit");
    let mut rows = scenario_rows(&matching_log());
    rows.truncate(8);
    rows[6]["frame"] = json!(change(
        rows[6]["frame"].as_str().unwrap(),
        "buy",
        "buy_price",
        "9.50"
    ));
    let (accepted, result, recorded) = run(&accepted_fixture, &rows);
    let command = only_purchase(&ledger_events(&accepted), "9.50", true, None);
    assert_eq!(
        financial(&accepted.engine().accounts()[0]),
        expected("9990.50", "9.50", "9.50", 1)
    );
    assert_eq!(buys(&recorded), 1);
    assert_eq!(result.receipt.dimensions[0].status, Status::OutsideEnvelope);
    assert_eq!(
        result.receipt.dimensions[0].reason,
        Some(format!(
            "{command}: accepted economics discrepancy or deficit"
        ))
    );
    assert_eq!(result.receipt.dimensions[1].status, Status::Matched);
    assert_eq!(
        accepted
            .records()
            .iter()
            .filter(|r| matches!(r.kind, RecordKind::Refused { .. }))
            .count(),
        0
    );
    assert_eq!(source_hashes(&fixture), before);
}

#[test]
fn loss_or_tie_only_deterioration_and_debit_deficit() {
    let fixture = Fixture::new("t2-loss-tie-deficit");
    let mut rows = scenario_rows(&matching_log());
    rows.truncate(8);
    rows[6]["frame"] = json!(change(
        rows[6]["frame"].as_str().unwrap(),
        "buy",
        "buy_price",
        "11.25"
    ));
    rows.extend([
        scenario_tick(START + 1_000_000, "180.0000"),
        scenario_tick(START + 2_000_000, "180.0000"),
    ]);
    let (owner, completed, recorded) = run(&fixture, &rows);
    let events = ledger_events(&owner);
    let command = only_purchase(&events, "11.25", true, Some("1.25"));
    assert_eq!(
        financial(&owner.engine().accounts()[0]),
        expected("9988.75", "11.25", "11.25", 1)
    );
    assert_eq!(
        owner.engine().accounts()[0].blocked.get(&command),
        Some(&Block::Purchased {
            debit: money("11.25"),
            maximum: money("10.00"),
            payout: money("18.83"),
            expected_payout: money("18.83")
        })
    );
    assert_eq!(owner.health().receipt_sequence, 3);
    assert!(
        matches!(&owner.health().entries, live::Entries::Disabled(reason) if reason.contains("account has unresolved financial evidence") && reason.contains("compatibility outside envelope: economics_scope"))
    );
    assert_eq!(
        completed.receipt.dimensions[0].reason,
        Some(format!(
            "{command}: accepted economics discrepancy or deficit"
        ))
    );
    assert_eq!(buys(&recorded), 1);
    let proposal = events
        .iter()
        .find_map(|e| match &e.kind {
            EventKind::Signal {
                proposal: Some(p), ..
            } => Some(p.clone()),
            _ => None,
        })
        .unwrap();
    for (i, leg, fee) in [
        (0, "loss", false),
        (1, "tie", false),
        (2, "loss", true),
        (3, "tie", true),
    ] {
        let variant = isolated_fixture(&fixture, &format!("unsupported-{i}"));
        let rows = scenario_rows(&matching_log());
        let recorded = RecordedConnector::from_jsonl(&scenario_log(&rows[..4])).unwrap();
        let mut owner = runtime(
            &variant,
            live::Mode::Replay,
            &recorded,
            FakeControl::new(START),
        );
        let mut changed = proposal.clone();
        let cashflow = if leg == "loss" {
            &mut changed.terms.loss
        } else {
            &mut changed.terms.tie
        };
        if fee {
            cashflow.terminal_fee = money("0.01");
        } else {
            cashflow.gross_return = money("0.01");
        }
        let binding = owner.definition.policy.replay.bindings[0].id.clone();
        assert_eq!(owner.offer(&binding, changed.clone()).unwrap(), None);
        assert_eq!(
            &owner.records().last().unwrap().kind,
            &RecordKind::Refused {
                binding,
                proposal: Some(changed),
                reason: format!(
                    "offer differs from exact baseline {}",
                    owner.definition.policy.baseline[0].id
                )
            }
        );
        let result = owner.finish().unwrap();
        assert_eq!(
            financial(&owner.engine().accounts()[0]),
            expected("10000.00", "0.00", "0.00", 0)
        );
        assert_eq!(buys(&recorded), 0);
        assert_eq!(ledger_events(&owner).len(), 1);
        assert_eq!(result.receipt.dimensions[1].status, Status::OutsideEnvelope);
        assert_eq!(result.receipt.dimensions[1].samples, 1);
    }
}

#[test]
fn receipt_vetoes_missing_support_account_class_and_incomparable_rejection_model() {
    let fixture = Fixture::new("t2-support-and-account-class");
    let before = source_hashes(&fixture);
    let (owner, matched, _) = run(
        &isolated_fixture(&fixture, "matching"),
        &scenario_rows(&matching_log()),
    );
    assert_eq!(
        matched.receipt.promotion,
        live::receipt::Promotion {
            eligible: true,
            reasons: vec![]
        }
    );
    let mut real = isolated_fixture(&fixture, "requires-real");
    real.config
        .live
        .as_mut()
        .unwrap()
        .compatibility
        .required_account_class = AccountClass::Real;
    let (_, mismatch, _) = run(&real, &scenario_rows(&matching_log()));
    assert_eq!(mismatch.receipt.account_class, AccountClass::Demo);
    assert_eq!(mismatch.receipt.required_account_class, AccountClass::Real);
    assert_eq!(mismatch.receipt.dimensions, matched.receipt.dimensions);
    assert_eq!(
        mismatch.receipt.promotion,
        live::receipt::Promotion {
            eligible: false,
            reasons: vec!["account_class".into()]
        }
    );
    let empty = isolated_fixture(&fixture, "no-admitted-commands");
    let (_, empty, recorded) = run(&empty, &scenario_rows(&matching_log())[..4]);
    assert_eq!(empty.receipt.dimensions[1].status, Status::Unavailable);
    assert_eq!(empty.receipt.dimensions[1].samples, 0);
    assert_eq!(
        empty.receipt.dimensions[1].reason.as_deref(),
        Some("no rejection-model evidence")
    );
    assert_eq!(buys(&recorded), 0);
    let mut low = isolated_fixture(&fixture, "low-support");
    low.config.live.as_mut().unwrap().compatibility.min_samples = 2;
    let (_, low, _) = run(&low, &scenario_rows(&matching_log()));
    assert!(!low.receipt.promotion.eligible);
    assert_eq!(
        low.receipt.promotion.reasons,
        matched
            .receipt
            .dimensions
            .iter()
            .map(|d| d.name.to_string())
            .collect::<Vec<_>>()
    );
    for dimension in &low.receipt.dimensions {
        assert_eq!((dimension.samples, dimension.required), (1, 2));
    }
    assert_eq!(source_hashes(&fixture), before);
    assert_eq!(
        financial(&owner.engine().accounts()[0]),
        expected("10008.83", "0.00", "0.00", 0)
    );
    // PRIMARY: receipt::compute leaves 1/2 supported dimensions Matched; only promotion
    // reasons record insufficient support. The requested mandatory classification is Unavailable.
    assert_eq!(
        low.receipt
            .dimensions
            .iter()
            .map(|d| d.status)
            .collect::<Vec<_>>(),
        vec![Status::Unavailable; 6]
    );
}

fn constrained(definition: &mut live::LiveDefinition) {
    for strategy in &mut definition.policy.replay.strategies {
        strategy.conditions.truncate(1);
        strategy.conditions[0].output = "range_bps".into();
        strategy.conditions[0].comparator = Comparator::Ge;
        strategy.conditions[0].threshold = Threshold::Number(0.0);
        strategy.repair.clear();
    }
    for risk in &mut definition.policy.replay.risk_policies {
        risk.max_open_total = Some(1);
    }
    definition.definition.replay = definition.policy.replay.clone();
}
fn shifted(mut row: Value, seconds: i64) -> Value {
    row["at"] = json!(row["at"].as_i64().unwrap() + seconds * 1_000_000);
    let mut text = row["frame"].as_str().unwrap().to_string();
    for offset in [5, 0] {
        text = text.replace(
            &(START / 1_000_000 + offset).to_string(),
            &(START / 1_000_000 + offset + seconds).to_string(),
        );
    }
    row["frame"] = json!(text);
    row
}
fn capacity_rows(delayed: bool, terminal_first: bool, duplicates: bool) -> Vec<Value> {
    let source = scenario_rows(&two_matching_log());
    let mut rows = source[..10].to_vec(); // two ordered signals, first purchase/entry only
    for second in 1..=26 {
        let at = START + second * 1_000_000;
        let mut tick = source[5].clone();
        tick["at"] = json!(at);
        tick["frame"] = json!(change(
            &change(
                tick["frame"].as_str().unwrap(),
                "tick",
                "epoch",
                &(at / 1_000_000).to_string()
            ),
            "tick",
            "quote",
            if second >= 25 { "180.0002" } else { "180.0000" }
        ));
        rows.push(tick);
        if second == 5 {
            rows.push(source[12].clone());
        }
        if second == if delayed { 21 } else { 5 } {
            let order = if terminal_first { [15, 14] } else { [14, 15] };
            for i in order {
                let mut row = source[i].clone();
                row["at"] = json!(at);
                rows.push(row.clone());
                if duplicates {
                    rows.push(row);
                }
            }
        }
        if second == 20 {
            let mut proposal = shifted(source[7].clone(), 20);
            proposal["frame"] = json!(crate::common::broker::replace(
                proposal["frame"].as_str().unwrap(),
                "req_id",
                "144"
            ));
            rows.push(proposal);
            if !delayed {
                for (index, request) in [(10, "155"), (11, "166")] {
                    let mut row = shifted(source[index].clone(), 20);
                    row["frame"] = json!(crate::common::broker::replace(
                        row["frame"].as_str().unwrap(),
                        "req_id",
                        request
                    ));
                    rows.push(row);
                }
            }
        }
        if second == 25 && !delayed {
            for index in [17, 18] {
                let mut row = shifted(source[index].clone(), 20);
                if index == 18 {
                    row["frame"] = json!(crate::common::broker::replace(
                        row["frame"].as_str().unwrap(),
                        "req_id",
                        "166"
                    ));
                }
                rows.push(row);
            }
        }
    }
    rows
}

#[test]
fn two_ordered_strategies_share_constrained_capacity() {
    // Extends runtime::two_binding_matching_log_proves_all_six_dimensions with capacity
    // competition. The zero-credit once-only branch remains proved by
    // runtime::confirmed_zero_credit_loss_releases_capacity_once_from_statement_evidence.
    let fixture = Fixture::two("t2-constrained-capacity");
    let before = source_hashes(&fixture);
    let mut results = Vec::new();
    for delayed in [false, true] {
        let variant = isolated_fixture(&fixture, if delayed { "delayed" } else { "timely" });
        let rows = capacity_rows(delayed, false, true);
        let recorded = RecordedConnector::from_jsonl(&scenario_log(&rows)).unwrap();
        let mut owner = runtime_with(
            &variant,
            live::Mode::Replay,
            &recorded,
            Box::new(FakeControl::new(START)),
            constrained,
            |m| m,
        )
        .unwrap();
        let mut at_opportunity = None;
        let mut at_due_tick = None;
        let result = owner
            .run_until(|health| {
                if health.receipt_sequence == 8 {
                    at_due_tick.get_or_insert_with(|| financial(&health.risk));
                }
                if health.receipt_sequence == 23 && health.pending_rows == 0 {
                    at_opportunity = Some(financial(&health.risk));
                }
                recorded.exhausted()
            })
            .unwrap()
            .unwrap();
        let events = ledger_events(&owner);
        assert_eq!(at_due_tick, Some(expected("9990.00", "10.00", "10.00", 1)));
        let bindings = &owner.definition.policy.replay.bindings;
        let signals = events
            .iter()
            .filter_map(|e| match &e.kind {
                EventKind::Signal {
                    binding,
                    close_time_micros,
                    disposition,
                    ..
                } => Some((binding.as_str(), *close_time_micros, *disposition)),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(
            signals,
            vec![
                (bindings[0].id.as_str(), START, Disposition::Admitted),
                (bindings[1].id.as_str(), START, Disposition::CapacityTotal),
                (
                    bindings[1].id.as_str(),
                    START + 20_000_000,
                    if delayed {
                        Disposition::CapacityTotal
                    } else {
                        Disposition::Admitted
                    }
                )
            ]
        );
        assert_eq!(buys(&recorded), if delayed { 1 } else { 2 });
        assert_eq!(
            financial(&owner.engine().accounts()[0]),
            expected(
                if delayed { "10008.83" } else { "10017.66" },
                "0.00",
                "0.00",
                0
            )
        );
        assert_eq!(
            at_opportunity,
            Some(expected(
                if delayed { "9990.00" } else { "9998.83" },
                "10.00",
                "10.00",
                1
            ))
        );
        let commands = commands(&events);
        let settlements = events
            .iter()
            .filter_map(|e| match &e.kind {
                EventKind::Settled {
                    command,
                    outcome,
                    gross_return,
                    terminal_fee,
                    credit,
                    profit,
                    release,
                    discrepancy,
                    deficit,
                    ..
                } => Some((
                    command.clone(),
                    e.time_micros,
                    *outcome,
                    gross_return.to_string(),
                    terminal_fee.to_string(),
                    credit.to_string(),
                    profit.to_string(),
                    release.to_string(),
                    *discrepancy,
                    *deficit,
                )),
                _ => None,
            })
            .collect::<Vec<_>>();
        let settled_at = if delayed {
            vec![START + 21_000_000]
        } else {
            vec![START + 5_000_000, START + 25_000_000]
        };
        assert_eq!(
            settlements,
            commands
                .iter()
                .zip(settled_at)
                .map(|(command, at)| (
                    command.clone(),
                    at,
                    binary_alpha_engine::execution::Outcome::Win,
                    "18.83".into(),
                    "0.00".into(),
                    "18.83".into(),
                    "8.83".into(),
                    "0.00".into(),
                    false,
                    None
                ))
                .collect::<Vec<_>>()
        );
        assert_eq!(
            result.receipt.dimensions[5].status,
            if delayed {
                Status::OutsideEnvelope
            } else {
                Status::Matched
            }
        );
        let mut detail = commands.iter().map(|command| format!("{command}: expiry_to_evidence={}; evidence_to_application=0; total={} microseconds", if delayed { 16_000_000 } else { 0 }, if delayed { 16_000_000 } else { 0 })).collect::<Vec<_>>();
        detail.sort();
        assert_eq!(result.receipt.dimensions[5].reason, Some(detail.join("; ")));
        assert_eq!(owner.health().receipt_sequence, 29);
        if delayed {
            assert!(
                matches!(&owner.health().entries, live::Entries::Disabled(reason) if reason.contains("compatibility outside envelope: funds_release"))
            );
            assert_eq!(result.receipt.promotion.reasons, ["funds_release"]);
        }
        // Both sides of the release boundary reproduce the complete ordered ledger/receipt.
        let before_cash = owner.records().iter().position(|r| matches!(&r.kind, RecordKind::Ledger { event } if matches!(event.kind, EventKind::CashObserved { .. }))).unwrap();
        let after_release = owner.records().iter().position(|r| matches!(&r.kind, RecordKind::Ledger { event } if matches!(event.kind, EventKind::Settled { .. }))).unwrap() + 1;
        for (prefix, cut) in [("before", before_cash), ("after", after_release)] {
            let restart =
                isolated_fixture(&fixture, &format!("delayed-{delayed}-release-{prefix}"));
            seed_scenario_prefix(&restart, &owner.records()[..cut]);
            let replay = RecordedConnector::from_jsonl(&scenario_log(&rows)).unwrap();
            let mut restored = runtime_with(
                &restart,
                live::Mode::Replay,
                &replay,
                Box::new(FakeControl::new(START)),
                constrained,
                |m| m,
            )
            .unwrap();
            let resumed = restored.run_until(|_| replay.exhausted()).unwrap().unwrap();
            assert_eq!(ledger_events(&restored), events);
            assert_eq!(resumed.receipt.to_json(), result.receipt.to_json());
        }
        // Cash/terminal order is changed without changing either fact or its receipt time.
        let reordered = isolated_fixture(
            &fixture,
            if delayed {
                "delayed-reordered"
            } else {
                "timely-reordered"
            },
        );
        let replay =
            RecordedConnector::from_jsonl(&scenario_log(&capacity_rows(delayed, true, false)))
                .unwrap();
        let mut reordered = runtime_with(
            &reordered,
            live::Mode::Replay,
            &replay,
            Box::new(FakeControl::new(START)),
            constrained,
            |m| m,
        )
        .unwrap();
        let reordered_result = reordered
            .run_until(|_| replay.exhausted())
            .unwrap()
            .unwrap();
        assert_eq!(reordered.engine().accounts(), owner.engine().accounts());
        assert_eq!(reordered_result.receipt.to_json(), result.receipt.to_json());
        // Measure the real terminal-before-cash prefix: expiry and terminal exist,
        // but neither a due tick nor terminal confirmation can manufacture credit.
        let before_cash = reordered.records().iter().position(|r| matches!(&r.kind, RecordKind::Ledger { event } if matches!(event.kind, EventKind::CashObserved { .. }))).unwrap();
        let prefix = &reordered.records()[..before_cash];
        assert_eq!(prefix.iter().filter(|r| matches!(&r.kind, RecordKind::Ledger { event } if matches!(event.kind, EventKind::Unresolved { terminal: Some(_), .. }))).count(), 1);
        let missing_cash = scenario_receipt_records(&variant, &reordered, prefix);
        assert_eq!(missing_cash.dimensions[5].status, Status::Unavailable);
        assert_eq!(missing_cash.dimensions[5].samples, 0);
        assert_eq!(
            missing_cash.dimensions[5].reason,
            Some(format!(
                "{}: missing confirmed expiry, unresolved liability, or missing matched cash/zero-credit reconciliation",
                commands[0]
            ))
        );
        results.push(events);
    }
    assert_eq!(source_hashes(&fixture), before);
    assert_eq!(
        results[0]
            .iter()
            .filter(|e| matches!(e.kind, EventKind::Settled { .. }))
            .count(),
        2
    );
    assert_eq!(
        results[1]
            .iter()
            .filter(|e| matches!(e.kind, EventKind::Settled { .. }))
            .count(),
        1
    );
}

#[test]
fn funds_release_separates_receipt_availability_from_owner_application() {
    let fixture = Fixture::two("t2-release-application-lag");
    let mut rows = capacity_rows(true, true, false);
    let proposal = rows
        .iter()
        .position(|r| r["at"] == START + 20_000_000 && r["session"] == "account")
        .unwrap();
    let mut reply = rows.remove(proposal);
    reply["at"] = json!(START + 22_000_000);
    let at = rows
        .iter()
        .position(|r| r["at"] == START + 22_000_000)
        .unwrap()
        + 1;
    // While the second proposal request is outstanding, the account transport receives
    // terminal/cash at +21s. Its reply at +22s lets the worker deliver those queued facts.
    rows.insert(at, reply);
    let recorded = RecordedConnector::from_jsonl(&scenario_log(&rows)).unwrap();
    let mut owner = runtime_with(
        &fixture,
        live::Mode::Replay,
        &recorded,
        Box::new(FakeControl::new(START)),
        constrained,
        |m| m,
    )
    .unwrap();
    let completed = owner.run_until(|_| recorded.exhausted()).unwrap().unwrap();
    let events = ledger_events(&owner);
    let command = &commands(&events)[0];
    let cash = events
        .iter()
        .filter_map(|e| match &e.kind {
            EventKind::CashObserved { source, .. } => Some((
                source.provider_time_micros,
                source.available_at_micros,
                e.time_micros,
            )),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(
        cash,
        [(START + 5_000_000, START + 21_000_000, START + 22_000_000)]
    );
    let release = events
        .iter()
        .filter_map(|e| matches!(e.kind, EventKind::Settled { .. }).then_some(e.time_micros))
        .collect::<Vec<_>>();
    assert_eq!(release, [START + 22_000_000]);
    assert_eq!(
        completed.receipt.dimensions[5].status,
        Status::OutsideEnvelope
    );
    assert_eq!(
        completed.receipt.dimensions[5].reason,
        Some(format!(
            "{command}: expiry_to_evidence=16000000; evidence_to_application=1000000; total=17000000 microseconds"
        ))
    );
    assert_eq!(
        financial(&owner.engine().accounts()[0]),
        expected("10008.83", "0.00", "0.00", 0)
    );
    assert_eq!(buys(&recorded), 1);
    assert_eq!(owner.health().receipt_sequence, 29);
}

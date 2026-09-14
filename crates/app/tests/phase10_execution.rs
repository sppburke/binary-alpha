mod common;

use binary_alpha_app::broker::deriv::{
    DerivAccounts, DerivOptions, purchase_fact, purchase_observation, recover_purchase,
    to_observation,
};
use binary_alpha_app::broker::transport::{Connector, Frame};
use binary_alpha_app::broker::{
    AccountEvent, AccountIdentity, Clock, PreparedPurchase, ProposalRequest, PurchaseOutcome,
};
use binary_alpha_app::{inspect, replay, store::Store};
use binary_alpha_engine::config::{AccountClass, Config, DerivSettings, RateBudgets, StreamKey};
use binary_alpha_engine::execution::{
    self, AccountState, BrokerLiability, CashAction, ColumnSpec, ContractSemantics, Decimal,
    Direction, Disposition, Engine, EventKind, EventSource, FinancialEvent, InstrumentBinding,
    Observation, Proposal, Resolution, RunDefinition, Settlement, SettlementRule, StreamColumns,
    UnresolvedReason,
};
use binary_alpha_engine::features::{Kind, Value};
use binary_alpha_engine::market::{InstrumentId, PriceScale};
use common::Scratch;
use common::broker::{FakeClock, FakeHttp, connector, correlated, replace};
use serde_json::value::RawValue;
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::rc::Rc;

const SECOND: i64 = 1_000_000;
const PURCHASE: i64 = 1_789_347_036_000_000;
const CALL: &str = "12859891379";
const PUT: &str = "12859892839";
fn decimal(text: &str) -> Decimal {
    Decimal::parse(text).unwrap()
}
fn fixture(name: &str) -> String {
    common::broker::fixture(&format!("deriv-execution-{name}.json"))
}
fn change(text: &str, owner: &str, key: &str, value: &str) -> String {
    let map: BTreeMap<String, Box<RawValue>> = serde_json::from_str(text).unwrap();
    replace(text, owner, &replace(map[owner].get(), key, value))
}
fn frame(name: &str, req_id: u64) -> Frame {
    Frame::Text(correlated(&fixture(name), req_id))
}
fn response(text: &str, req_id: u64) -> Frame {
    Frame::Text(correlated(text, req_id))
}
fn instrument() -> InstrumentId {
    InstrumentId {
        broker: "deriv".to_string().try_into().unwrap(),
        provider_symbol: "R_50".to_string().try_into().unwrap(),
    }
}
fn scale() -> PriceScale {
    4.try_into().unwrap()
}
fn settlement() -> Settlement {
    Settlement {
        rule: SettlementRule::BrokerAuthoritativeV1,
        max_settlement_delay_micros: 120 * SECOND,
        max_tick_gap_micros: 120 * SECOND,
    }
}
fn request(direction: Direction) -> ProposalRequest {
    ProposalRequest {
        binding: if direction == Direction::Buy {
            "call"
        } else {
            "put"
        }
        .into(),
        instrument: instrument(),
        scale: scale(),
        direction,
        duration_seconds: 15,
        stake: decimal("10"),
        currency: "USD".to_string().try_into().unwrap(),
        semantics: ContractSemantics::RiseFallStrictV1,
        settlement: settlement(),
    }
}
fn definition(two: bool) -> RunDefinition {
    let config = Config::parse(include_str!("fixtures/phase10/execution-definition.toml")).unwrap();
    let mut replay = config.replay.unwrap();
    if two {
        let mut terms = replay.contracts[0].clone();
        terms.id = "put".into();
        terms.direction = Direction::Sell;
        replay.contracts.push(terms);
        let mut binding = replay.bindings[0].clone();
        binding.id = "put".into();
        binding.contract = "put".into();
        replay.bindings.push(binding);
    }
    RunDefinition {
        schema_version: execution::REPLAY_SCHEMA_VERSION_BROKER,
        config_hash: if two { "c" } else { "d" }.repeat(64),
        code_revision: "phase10-synthetic-inputs".into(),
        availability: "scripted_broker_receipts".into(),
        replay,
        instruments: vec![InstrumentBinding {
            instrument: instrument().to_string(),
            broker: instrument().broker,
            provider_symbol: instrument().provider_symbol,
            price_scale: 4,
            tick_generation: "1".repeat(64),
            feature_generation: "2".repeat(64),
            plan_identity: "plan".into(),
            raw_identity: "synthetic".into(),
            outcome_generation: None,
            streams: vec![StreamColumns {
                stream: StreamKey {
                    duration_seconds: 5,
                    offset_seconds: 0,
                },
                columns: vec![ColumnSpec {
                    name: "signal".into(),
                    source: "signal".into(),
                    kind: Kind::Bool,
                    encoding: None,
                    readiness: vec![],
                    unready: vec![],
                }],
            }],
        }],
    }
}
fn connect_options(connector: Box<dyn Connector>, clock: &FakeClock, url: &str) -> DerivOptions {
    connect_options_scoped(
        connector,
        clock,
        url,
        AccountClass::Demo,
        "USD",
        RateBudgets::default(),
    )
}
fn connect_options_scoped(
    connector: Box<dyn Connector>,
    clock: &FakeClock,
    url: &str,
    class: AccountClass,
    currency: &str,
    limits: RateBudgets,
) -> DerivOptions {
    let settings = DerivSettings {
        id: instrument().broker,
        public_endpoint: "ws://127.0.0.1/public".into(),
        bootstrap_endpoint: "http://127.0.0.1/trading/v1/options".into(),
        app_id: "SYNTHETIC-APP".into(),
        credential: Some("SYNTHETIC_REFERENCE".into()),
        account_class: Some(class),
        budgets: None,
    };
    let mut http = FakeHttp { responses: vec![format!(r#"{{"data":[{{"account_id":"SYNTHETICACCOUNT","account_type":"{}","status":"active","currency":"{currency}"}}]}}"#, class.as_str()).into_bytes(), format!(r#"{{"data":{{"url":"{url}"}}}}"#).into_bytes()].into(), calls: vec![] };
    let address = DerivAccounts::bootstrap(&settings, &mut http, "synthetic-credential").unwrap();
    let account = AccountIdentity {
        broker: settings.id,
        account: "a".into(),
        class,
        currency: address.currency.clone(),
    };
    DerivOptions::connect(
        address,
        account,
        &[(instrument(), scale())],
        connector,
        Box::new(clock.clone()),
        limits,
    )
    .unwrap()
}
fn options(frames: Vec<Frame>, clock: &FakeClock) -> (DerivOptions, Rc<RefCell<Vec<Frame>>>) {
    let (connector, sent) = connector(vec![frames], clock);
    (
        connect_options(
            connector,
            clock,
            "ws://127.0.0.1/trading/v1/options/ws/demo",
        ),
        sent,
    )
}
fn tick(at: i64) -> Observation {
    Observation::Tick {
        instrument: 0,
        provider_time_micros: at,
        price_units: 920_409,
    }
}
fn row(at: i64) -> Observation {
    Observation::Row {
        instrument: 0,
        stream: 0,
        close_time_micros: at,
        known_at_micros: at,
        values: vec![Some(Value::Bool(true))],
    }
}
fn quote(binding: &str, proposal: Proposal) -> Observation {
    Observation::Proposal {
        binding: binding.into(),
        proposal,
    }
}
fn source(name: &str, at: i64) -> EventSource {
    EventSource {
        id: name.into(),
        provider_time_micros: at,
        available_at_micros: at,
        simulated: false,
    }
}
struct Run {
    engine: Engine,
    lines: Vec<Vec<u8>>,
}
impl Run {
    fn new(two: bool) -> Self {
        let mut engine = Engine::new(definition(two)).unwrap();
        let lines = engine.drain().iter().map(FinancialEvent::to_line).collect();
        Self { engine, lines }
    }
    fn step(&mut self, at: i64, observations: Vec<Observation>) -> Vec<FinancialEvent> {
        self.engine.step(at, observations).unwrap();
        let events = self.engine.drain();
        self.lines
            .extend(events.iter().map(FinancialEvent::to_line));
        let restored = Engine::restore(self.lines.iter().cloned().map(Ok)).unwrap();
        assert_eq!(
            restored.state_identity().as_bytes(),
            self.engine.state_identity().as_bytes()
        );
        assert_eq!(
            restored.summary().to_json(),
            self.engine.summary().to_json()
        );
        self.engine = restored;
        events
    }
    fn account(&self) -> &AccountState {
        &self.engine.accounts()[0]
    }
    fn publish(&self, name: &str, counts: &str) {
        let scratch = Scratch::new(name);
        let local = Store::filesystem(scratch.path("retained"));
        let destination = Store::filesystem(scratch.path("published"));
        let manifest =
            replay::publish_ledger(self.lines.iter().cloned().map(Ok), &local, &destination)
                .unwrap();
        assert_eq!(manifest.schema_version, 2);
        assert_eq!(manifest.events, self.engine.sequence());
        let uri = destination.uri(&manifest.key());
        let lines = common::command(&["data", "verify", "--manifest", &uri]).unwrap();
        assert!(lines[0].contains(counts), "{lines:?}");
    }
}
fn prepared(events: &[FinancialEvent], binding: &str) -> PreparedPurchase {
    events
        .iter()
        .find_map(|event| match &event.kind {
            EventKind::Signal {
                binding: actual,
                command: Some(command),
                proposal: Some(proposal),
                reservation: Some(reservation),
                disposition: Disposition::Admitted,
                ..
            } if actual == binding => {
                assert_eq!(reservation.to_string(), "10.00");
                Some(PreparedPurchase {
                    dispatch_claim: format!("fake-durable-claim:{command}"),
                    command: command.clone(),
                    proposal_identity: proposal.identity.clone(),
                    maximum_price: proposal.terms.quoted_cost,
                })
            }
            _ => None,
        })
        .expect("admitted proposal")
}
fn advance(clock: &mut FakeClock, at: i64) {
    clock.sleep((at - clock.now_micros()).max(0));
}
fn account_event(
    options: &mut DerivOptions,
    run: &mut Run,
    clock: &FakeClock,
    commands: &BTreeMap<String, String>,
) -> Vec<FinancialEvent> {
    let event = options
        .next_account_event(SECOND)
        .unwrap()
        .expect("account event");
    let receipt = clock.now_micros();
    let observation = to_observation(event, &|contract| commands.get(contract).cloned(), receipt)
        .expect("financial observation");
    run.step(receipt, vec![observation])
}

#[test]
fn pinned_rise_and_fall_restore_publish_and_verify_exact_cash() {
    let mut clock = FakeClock::at(PURCHASE - SECOND);
    let balance_call = change(&fixture("balance-before"), "balance", "balance", "9945.74");
    let balance_put = change(&fixture("balance-before"), "balance", "balance", "9935.74");
    let frames = vec![
        frame("balance-before", 1),
        frame("transaction-ack", 2),
        frame("proposal-call", 3),
        frame("buy-call", 4),
        response(&balance_call, 5),
        frame("proposal-put", 6),
        frame("buy-put", 7),
        response(&balance_put, 8),
        frame("transaction-buy-call", 2),
        frame("open-call", 9),
        frame("transaction-buy-put", 2),
        frame("open-put", 10),
        frame("entry-call", 9),
        frame("entry-put", 10),
        frame("won", 9),
        frame("transaction-win", 2),
        frame("lost", 10),
        frame("transaction-zero", 2),
        frame("statement-three", 11),
        frame("statement-four", 12),
        frame("balance-after", 13),
    ];
    let (mut options, sent) = options(frames, &clock);
    assert_eq!(options.balance().unwrap().to_string(), "9955.74");
    options.subscribe_transactions().unwrap();
    assert!(matches!(
        options.next_account_event(SECOND).unwrap(),
        Some(AccountEvent::TransactionAcknowledged)
    ));
    let call = options.proposal(&request(Direction::Buy)).unwrap();
    assert_eq!(call.terms.win.gross_return.to_string(), "18.83");
    assert_eq!(call.spot_units, 920_409);
    assert_eq!(call.spot_time_micros, PURCHASE - 2 * SECOND);
    let mut run = Run::new(true);
    let now = clock.now_micros();
    let signals = run.step(now, vec![quote("call", call), tick(now), row(now)]);
    let call = prepared(&signals, "call");
    assert_eq!(call.maximum_price.to_string(), "10");
    assert_eq!(run.account().reserved.to_string(), "10.00");
    advance(&mut clock, PURCHASE);
    let outcome = options.purchase(&call).unwrap();
    let accepted = run.step(
        clock.now_micros(),
        vec![purchase_observation(
            &call.command,
            &call.dispatch_claim,
            outcome,
            clock.now_micros(),
        )],
    );
    assert!(matches!(
        &accepted[0].kind,
        EventKind::Accepted {
            entry_time_micros: None,
            entry_price_units: None,
            due_time_micros: None,
            liability: Some(_),
            ..
        }
    ));
    assert_eq!(run.account().cash.to_string(), "9945.74");
    assert_eq!(options.balance().unwrap(), decimal("9945.74"));
    advance(&mut clock, PURCHASE + SECOND);
    let put_quote = options.proposal(&request(Direction::Sell)).unwrap();
    let now = clock.now_micros();
    let signals = run.step(now, vec![quote("put", put_quote), tick(now), row(now)]);
    let put = prepared(&signals, "put");
    let commands = BTreeMap::from([
        (CALL.into(), call.command.clone()),
        (PUT.into(), put.command.clone()),
    ]);
    advance(&mut clock, PURCHASE + 2 * SECOND);
    let outcome = options.purchase(&put).unwrap();
    run.step(
        clock.now_micros(),
        vec![purchase_observation(
            &put.command,
            &put.dispatch_claim,
            outcome,
            clock.now_micros(),
        )],
    );
    assert_eq!(run.account().cash.to_string(), "9935.74");
    assert_eq!(options.balance().unwrap(), decimal("9935.74"));
    options.subscribe_contract(CALL).unwrap();
    let confirmed = account_event(&mut options, &mut run, &clock, &commands);
    assert!(matches!(
        &confirmed[0].kind,
        EventKind::Confirmed {
            entry_price_units: None,
            expiry_micros: Some(1_789_347_051_000_000),
            ..
        }
    ));
    account_event(&mut options, &mut run, &clock, &commands);
    options.subscribe_contract(PUT).unwrap();
    account_event(&mut options, &mut run, &clock, &commands);
    account_event(&mut options, &mut run, &clock, &commands);
    let confirmed = account_event(&mut options, &mut run, &clock, &commands);
    assert!(matches!(
        &confirmed[0].kind,
        EventKind::Confirmed {
            entry_price_units: Some(920_252),
            entry_time_micros: Some(1_789_347_038_000_000),
            start_micros: None,
            expiry_micros: None,
            ..
        }
    ));
    advance(&mut clock, PURCHASE + 4 * SECOND);
    account_event(&mut options, &mut run, &clock, &commands);
    advance(&mut clock, PURCHASE + 15 * SECOND);
    assert!(
        run.step(clock.now_micros(), vec![tick(clock.now_micros())])
            .is_empty()
    );
    assert_eq!(run.account().open, 2);
    advance(&mut clock, PURCHASE + 16 * SECOND);
    account_event(&mut options, &mut run, &clock, &commands);
    let terminal = account_event(&mut options, &mut run, &clock, &commands);
    assert!(terminal.iter().any(|event| matches!(&event.kind, EventKind::Unresolved {reason:UnresolvedReason::AwaitingCash,terminal:Some(fact),..} if fact.exit_time_micros==Some(PURCHASE+14*SECOND))));
    assert_eq!(run.account().cash.to_string(), "9935.74");
    account_event(&mut options, &mut run, &clock, &commands);
    assert_eq!(run.account().cash.to_string(), "9954.57");
    advance(&mut clock, PURCHASE + 18 * SECOND);
    account_event(&mut options, &mut run, &clock, &commands);
    account_event(&mut options, &mut run, &clock, &commands);
    assert_eq!(run.account().open, 1);
    account_event(&mut options, &mut run, &clock, &commands);
    assert_eq!(run.account().cash.to_string(), "9954.57");
    assert_eq!(run.account().open, 0);
    assert_eq!(run.engine.summary().portfolio.wins, 1);
    assert_eq!(run.engine.summary().portfolio.losses, 1);
    assert_eq!(run.engine.summary().portfolio.ties, 0);
    for (through, count) in [(1_789_347_053, 3), (1_789_347_054, 4)] {
        let rows = options.statement(1_789_347_035, through).unwrap();
        assert_eq!(rows.len(), count);
        for row in rows {
            let cash = row.cash;
            let observation =
                to_observation(AccountEvent::Cash(cash), &|_| None, clock.now_micros()).unwrap();
            assert!(run.step(clock.now_micros(), vec![observation]).is_empty());
        }
    }
    assert_eq!(
        options.balance().unwrap().to_string(),
        run.account().cash.to_string()
    );
    let requests = sent
        .borrow()
        .iter()
        .filter_map(|frame| {
            if let Frame::Text(text) = frame {
                Some(text.clone())
            } else {
                None
            }
        })
        .collect::<Vec<_>>();
    let statements = requests
        .iter()
        .filter(|text| text.contains("\"statement\":1"))
        .collect::<Vec<_>>();
    assert!(statements[0].contains("\"date_to\":1789347054"));
    assert!(statements[1].contains("\"date_to\":1789347055"));
    assert_eq!(
        requests
            .iter()
            .filter(|text| text.contains("\"buy\":"))
            .count(),
        2
    );
    assert!(
        requests
            .iter()
            .filter(|text| text.contains("\"buy\":"))
            .all(|text| text.contains("\"price\":10"))
    );
    run.publish(
        "phase10_execution_pinned",
        "signals 4 accepted 2 settled 2 unresolved 0",
    );
}

#[test]
fn changing_proposals_and_every_economic_boundary_are_exact() {
    let clock = FakeClock::at(PURCHASE - SECOND);
    let (mut options, _) = options(
        vec![frame("proposal-call", 1), frame("proposal-later", 2)],
        &clock,
    );
    let first = options.proposal(&request(Direction::Buy)).unwrap();
    let later = options.proposal(&request(Direction::Buy)).unwrap();
    assert_eq!(first.terms.win.gross_return.to_string(), "18.83");
    assert_eq!(later.terms.win.gross_return.to_string(), "19.53");
    assert_ne!(first.identity, later.identity);
    assert_eq!(first.request_identity, later.request_identity);
    let mut run = Run::new(false);
    let at = clock.now_micros();
    let events = run.step(
        at,
        vec![
            quote("call", first),
            quote("call", later.clone()),
            tick(at),
            row(at),
        ],
    );
    let purchase = prepared(&events, "call");
    assert_eq!(purchase.proposal_identity, later.identity);
    assert_eq!(purchase.maximum_price.to_string(), "10");
    assert_eq!(run.account().reserved.to_string(), "10.00");
    assert!(matches!(&events[0].kind,EventKind::Signal {proposal:Some(used),..} if *used==later));
    for deterioration in ["loss_fee", "tie_fee", "payout", "cost"] {
        let mut proposal = later.clone();
        // Synthetic neutral fee evidence: no unsupported Deriv fee field is invented.
        match deterioration {
            "loss_fee" => proposal.terms.loss.terminal_fee = decimal("0.01"),
            "tie_fee" => proposal.terms.tie.terminal_fee = decimal("0.01"),
            "payout" => proposal.terms.win.gross_return = decimal("18.82"),
            "cost" => proposal.terms.quoted_cost = decimal("10.01"),
            _ => unreachable!(),
        }
        let mut run = Run::new(false);
        let events = run.step(at, vec![quote("call", proposal), tick(at), row(at)]);
        assert!(
            matches!(
                &events[0].kind,
                EventKind::Signal {
                    disposition: Disposition::QuoteRejected,
                    command: None,
                    ..
                }
            ),
            "{deterioration}: {events:?}"
        );
        assert_eq!(run.account().reserved.to_string(), "0.00");
        assert_eq!(run.account().open, 0);
        assert_eq!(run.account().cash.to_string(), "9955.74");
    }
    // This exact fractional ask is synthetic; retained proposals all ask 10.
    let fractional = change(
        &fixture("proposal-call"),
        "proposal",
        "ask_price",
        "10.100000000000000001",
    );
    let (mut options, _) = self::options(vec![response(&fractional, 1)], &clock);
    let quote = options.proposal(&request(Direction::Buy)).unwrap();
    assert_eq!(quote.terms.quoted_cost.to_string(), "10.100000000000000001");
    assert_eq!(quote.spot_units, 920_409);
    let mut engine = Engine::new(definition(false)).unwrap();
    let error = engine
        .step(clock.now_micros(), vec![self::quote("call", quote)])
        .unwrap_err();
    assert!(error.contains("10.100000000000000001"), "{error}");
    assert_eq!(engine.accounts()[0].reserved.to_string(), "0.00");
    run.publish(
        "phase10_execution_dynamic",
        "signals 1 accepted 0 settled 0 unresolved 0",
    );
}

fn one_prepared(options: &mut DerivOptions, clock: &FakeClock) -> (Run, PreparedPurchase) {
    let proposal = options.proposal(&request(Direction::Buy)).unwrap();
    let mut run = Run::new(false);
    let at = clock.now_micros();
    let events = run.step(at, vec![quote("call", proposal), tick(at), row(at)]);
    let prepared = prepared(&events, "call");
    (run, prepared)
}
fn reconcile(
    run: &mut Run,
    prepared: &PreparedPurchase,
    resolution: Resolution,
    at: i64,
) -> Vec<FinancialEvent> {
    run.step(
        at,
        vec![Observation::Reconciliation {
            command: prepared.command.clone(),
            source: source(&format!("synthetic-reconciliation:{at}"), at),
            resolution,
        }],
    )
}

#[test]
fn excess_debit_is_accepted_blocked_and_reconciled() {
    let mut clock = FakeClock::at(PURCHASE - SECOND);
    let excess = change(&fixture("buy-call"), "buy", "buy_price", "10.50");
    let (mut options, _) = options(
        vec![frame("proposal-call", 1), response(&excess, 2)],
        &clock,
    );
    let (mut run, prepared) = one_prepared(&mut options, &clock);
    advance(&mut clock, PURCHASE);
    let outcome = options.purchase(&prepared).unwrap();
    let PurchaseOutcome::Accepted { debit, liability } = outcome.clone() else {
        panic!("{outcome:?}")
    };
    let events = run.step(
        clock.now_micros(),
        vec![purchase_observation(
            &prepared.command,
            &prepared.dispatch_claim,
            outcome,
            clock.now_micros(),
        )],
    );
    assert!(
        matches!(&events[0].kind,EventKind::Accepted {deficit:Some(deficit),discrepancy:true,..} if deficit.to_string()=="0.50")
    );
    assert_eq!(run.account().cash.to_string(), "9945.24");
    assert_eq!(run.account().paid_basis.to_string(), "10.50");
    assert_eq!(run.account().unresolved_loss.to_string(), "10.50");
    assert_eq!(run.account().blocked.len(), 1);
    let proposal = run
        .lines
        .iter()
        .find_map(|line| match FinancialEvent::from_line(line).unwrap().kind {
            EventKind::Signal {
                proposal: Some(proposal),
                ..
            } => Some(proposal),
            _ => None,
        })
        .unwrap();
    advance(&mut clock, PURCHASE + SECOND);
    let at = clock.now_micros();
    let blocked = run.step(at, vec![quote("call", proposal), tick(at), row(at)]);
    assert!(matches!(
        blocked[0].kind,
        EventKind::Signal {
            disposition: Disposition::AccountBlocked,
            ..
        }
    ));
    let reconciliation = reconcile(
        &mut run,
        &prepared,
        Resolution::Purchased { debit, liability },
        clock.now_micros(),
    );
    assert!(
        matches!(&reconciliation[0].kind, EventKind::Reconciled { release, debit, credit, profit: None, .. } if release.is_zero() && debit.is_zero() && credit.is_zero())
    );
    assert!(run.account().blocked.is_empty());
    assert_eq!(run.account().cash.to_string(), "9945.24");
    run.publish(
        "phase10_execution_deficit",
        "signals 2 accepted 1 settled 0 unresolved 0",
    );
}

#[test]
fn partial_confirmation_duplicates_and_approximate_transaction_expiry() {
    let mut clock = FakeClock::at(PURCHASE - SECOND);
    let first = change(
        &fixture("entry-call"),
        "proposal_open_contract",
        "date_expiry",
        "null",
    );
    let first = change(&first, "proposal_open_contract", "date_start", "null");
    let timing = change(
        &fixture("open-call"),
        "proposal_open_contract",
        "current_spot_time",
        "1789347038",
    );
    let transaction = change(
        &fixture("transaction-buy-call"),
        "transaction",
        "date_expiry",
        "1789347999",
    );
    let frames = vec![
        frame("transaction-ack", 1),
        frame("proposal-call", 2),
        frame("buy-call", 3),
        response(&transaction, 1),
        response(&first, 4),
        response(&timing, 4),
        response(&first, 4),
        response(&timing, 4),
    ];
    let (mut options, _) = options(frames, &clock);
    options.subscribe_transactions().unwrap();
    options.next_account_event(SECOND).unwrap();
    let (mut run, prepared) = one_prepared(&mut options, &clock);
    advance(&mut clock, PURCHASE);
    let outcome = options.purchase(&prepared).unwrap();
    run.step(
        clock.now_micros(),
        vec![purchase_observation(
            &prepared.command,
            &prepared.dispatch_claim,
            outcome,
            clock.now_micros(),
        )],
    );
    let commands = BTreeMap::from([(CALL.into(), prepared.command.clone())]);
    let cash = account_event(&mut options, &mut run, &clock, &commands);
    assert!(
        !cash
            .iter()
            .any(|event| matches!(event.kind, EventKind::Confirmed { .. }))
    );
    assert_eq!(run.account().cash.to_string(), "9945.74");
    advance(&mut clock, PURCHASE + 2 * SECOND);
    options.subscribe_contract(CALL).unwrap();
    let entry = account_event(&mut options, &mut run, &clock, &commands);
    assert!(matches!(
        entry[0].kind,
        EventKind::Confirmed {
            entry_price_units: Some(920_252),
            expiry_micros: None,
            ..
        }
    ));
    let timing = account_event(&mut options, &mut run, &clock, &commands);
    assert!(matches!(
        timing[0].kind,
        EventKind::Confirmed {
            entry_price_units: None,
            expiry_micros: Some(1_789_347_051_000_000),
            ..
        }
    ));
    assert!(account_event(&mut options, &mut run, &clock, &commands).is_empty());
    assert!(account_event(&mut options, &mut run, &clock, &commands).is_empty());
    assert_eq!(run.account().open, 1);
    assert_eq!(run.account().cash.to_string(), "9945.74");
}

fn absent_path(mut text: String) -> String {
    for key in [
        "entry_spot",
        "entry_spot_time",
        "exit_spot",
        "exit_spot_time",
    ] {
        text = change(&text, "proposal_open_contract", key, "null");
    }
    text
}
#[test]
fn missing_path_and_external_closures_use_terminal_and_actual_cash() {
    for status in ["won", "sold", "cancelled"] {
        let mut clock = FakeClock::at(PURCHASE - SECOND);
        let mut terminal = absent_path(fixture("won"));
        let mut cash = fixture("transaction-win");
        if status != "won" {
            // Synthetic externally observed closure, with actual 4.20 credit.
            terminal = change(
                &terminal,
                "proposal_open_contract",
                "status",
                &format!("\"{status}\""),
            );
            terminal = change(
                &terminal,
                "proposal_open_contract",
                "sell_price",
                "\"4.20\"",
            );
            cash = change(&cash, "transaction", "amount", "4.20");
        }
        let frames = vec![
            frame("transaction-ack", 1),
            frame("proposal-call", 2),
            frame("buy-call", 3),
            response(&terminal, 4),
            response(&cash, 1),
        ];
        let (mut options, _) = options(frames, &clock);
        options.subscribe_transactions().unwrap();
        options.next_account_event(SECOND).unwrap();
        let (mut run, prepared) = one_prepared(&mut options, &clock);
        advance(&mut clock, PURCHASE);
        let outcome = options.purchase(&prepared).unwrap();
        run.step(
            clock.now_micros(),
            vec![purchase_observation(
                &prepared.command,
                &prepared.dispatch_claim,
                outcome,
                clock.now_micros(),
            )],
        );
        advance(&mut clock, PURCHASE + 16 * SECOND);
        options.subscribe_contract(CALL).unwrap();
        let commands = BTreeMap::from([(CALL.into(), prepared.command.clone())]);
        account_event(&mut options, &mut run, &clock, &commands);
        account_event(&mut options, &mut run, &clock, &commands);
        assert_eq!(run.account().open, 1);
        assert_eq!(run.account().cash.to_string(), "9945.74");
        let events = account_event(&mut options, &mut run, &clock, &commands);
        if status == "won" {
            assert_eq!(run.account().cash.to_string(), "9964.57");
            assert_eq!(run.engine.summary().portfolio.wins, 1);
            assert!(
                events
                    .iter()
                    .any(|event| matches!(&event.kind, EventKind::Settled { path: None, .. }))
            );
        } else {
            assert_eq!(run.account().cash.to_string(), "9949.94");
            assert!(events.iter().any(|event|matches!(&event.kind,EventKind::Reconciled {resolution:Resolution::ExternallyClosed {status:actual,gross_return,..},..} if actual.as_str()==status && gross_return.to_string()=="4.20")));
            let summary = &run.engine.summary().portfolio;
            assert_eq!(
                (
                    summary.wins,
                    summary.losses,
                    summary.ties,
                    summary.externally_closed
                ),
                (0, 0, 0, 1)
            );
        }
        assert_eq!(run.account().open, 0);
        let projected = binary_alpha_engine::search::project_splits(
            run.lines
                .iter()
                .map(|line| FinancialEvent::from_line(line).unwrap()),
            "USD",
        );
        assert_eq!(projected["call"]["none"], run.engine.summary().portfolio);
        run.publish(
            &format!("phase10_execution_path_{status}"),
            if status == "won" {
                "signals 1 accepted 1 settled 1 unresolved 0"
            } else {
                "signals 1 accepted 1 settled 0 unresolved 0"
            },
        );
    }
}

#[test]
fn dispatch_requires_claim_and_never_retries_an_uncertain_write() {
    for failure in ["not_sent", "closed_before_write", "lost_ack", "rejected"] {
        let mut clock = FakeClock::at(PURCHASE - SECOND);
        let mut frames = vec![frame("proposal-call", 1)];
        if failure == "closed_before_write" || failure == "lost_ack" {
            frames.push(Frame::Close);
        }
        if failure == "rejected" {
            frames.push(Frame::Text(r#"{"msg_type":"buy","req_id":2,"error":{"code":"InvalidContractProposal","message":"Synthetic expired proposal"}}"#.into()));
        }
        let (mut options, sent) = options(frames, &clock);
        let (mut run, mut prepared) = one_prepared(&mut options, &clock);
        let mut missing = prepared.clone();
        missing.dispatch_claim.clear();
        let before = sent.borrow().len();
        assert!(
            options
                .purchase(&missing)
                .unwrap_err()
                .contains("dispatch claim")
        );
        assert_eq!(sent.borrow().len(), before);
        if failure == "not_sent" {
            prepared.proposal_identity = "other-connection:proposal".into();
        }
        if failure == "closed_before_write" {
            assert!(
                options
                    .next_account_event(SECOND)
                    .unwrap_err()
                    .contains("connection closed")
            );
        }
        advance(&mut clock, PURCHASE);
        let outcome = options.purchase(&prepared).unwrap();
        assert!(
            matches!(
                (&outcome, failure),
                (
                    PurchaseOutcome::ProvenNotSent { .. },
                    "not_sent" | "closed_before_write"
                ) | (PurchaseOutcome::PossiblySent { .. }, "lost_ack")
                    | (PurchaseOutcome::Rejected { .. }, "rejected")
            ),
            "{failure}: {outcome:?}"
        );
        run.step(
            clock.now_micros(),
            vec![purchase_observation(
                &prepared.command,
                &prepared.dispatch_claim,
                outcome,
                clock.now_micros(),
            )],
        );
        assert_eq!(run.account().cash.to_string(), "9955.74");
        if failure == "lost_ack" {
            assert_eq!(run.account().blocked.len(), 1);
            assert_eq!(run.account().reserved.to_string(), "10.00");
            let count = sent.borrow().len();
            assert!(
                options
                    .purchase(&prepared)
                    .unwrap_err()
                    .contains("possibly sent; reconcile before any retry")
            );
            assert_eq!(sent.borrow().len(), count);
        } else {
            assert!(run.account().blocked.is_empty());
            assert_eq!(run.account().reserved.to_string(), "0.00");
        }
        assert_eq!(
            sent.borrow().len() - before,
            usize::from(failure == "lost_ack" || failure == "rejected")
        );
    }
}

#[test]
fn statement_and_portfolio_recover_acceptance_without_retry() {
    for portfolio in [false, true] {
        let mut clock = FakeClock::at(PURCHASE - SECOND);
        let (mut original, sent) = options(vec![frame("proposal-call", 1), Frame::Close], &clock);
        let (mut run, prepared) = one_prepared(&mut original, &clock);
        advance(&mut clock, PURCHASE);
        let outcome = original.purchase(&prepared).unwrap();
        run.step(
            clock.now_micros(),
            vec![purchase_observation(
                &prepared.command,
                &prepared.dispatch_claim,
                outcome,
                clock.now_micros(),
            )],
        );
        assert_eq!(run.account().blocked.len(), 1);
        // Synthetic schema-derived portfolio: no retained portfolio observation exists.
        let portfolio_frame = fixture("portfolio");
        let mut frames = if portfolio {
            vec![Frame::Text(portfolio_frame)]
        } else {
            let statement = change(&fixture("statement-four"), "statement", "count", "1");
            let statement = change(
                &statement,
                "statement",
                "transactions",
                r#"[{"action_type":"buy","amount":-10,"transaction_id":24655144239,"contract_id":12859891379,"transaction_time":1789347036,"payout":18.83}]"#,
            );
            vec![response(&statement, 1)]
        };
        frames.extend([
            frame("transaction-ack", 2),
            frame("won", 3),
            frame("transaction-win", 2),
        ]);
        let (mut recovered, recovery_sent) = options(frames, &clock);
        let (debit, liability) = if portfolio {
            let contracts = recovered.open_contracts().unwrap();
            assert_eq!(contracts.len(), 1);
            let c = &contracts[0];
            assert_eq!(c.direction, Direction::Buy);
            assert_eq!(c.expiry_micros, Some(PURCHASE + 15 * SECOND));
            (
                c.buy_price,
                BrokerLiability {
                    contract_ref: c.contract_ref.clone(),
                    transaction_ref: c.transaction_ref.clone(),
                    purchase_time_micros: c.purchase_time_micros,
                    expected_start_micros: c.start_micros,
                    payout: c.payout,
                },
            )
        } else {
            let mut rows = recovered.statement(1_789_347_035, 1_789_347_036).unwrap();
            let row = rows.remove(0);
            assert_eq!(row.cash.action, CashAction::Buy);
            assert_eq!(row.cash.amount.to_string(), "-10");
            let (debit, liability) = recover_purchase(&row).unwrap();
            assert_eq!(debit.to_string(), "10");
            assert_eq!(liability.payout.to_string(), "18.83");
            assert_eq!(liability.purchase_time_micros, PURCHASE);
            assert_eq!(liability.expected_start_micros, None);
            let cash = row.cash;
            run.step(
                clock.now_micros(),
                vec![
                    to_observation(AccountEvent::Cash(cash), &|_| None, clock.now_micros())
                        .unwrap(),
                ],
            );
            (debit, liability)
        };
        reconcile(
            &mut run,
            &prepared,
            Resolution::Purchased { debit, liability },
            clock.now_micros(),
        );
        assert!(run.account().blocked.is_empty());
        assert_eq!(run.account().cash.to_string(), "9945.74");
        assert_eq!(run.account().open, 1);
        recovered.subscribe_transactions().unwrap();
        recovered.next_account_event(SECOND).unwrap();
        advance(&mut clock, PURCHASE + 16 * SECOND);
        recovered.subscribe_contract(CALL).unwrap();
        let commands = BTreeMap::from([(CALL.into(), prepared.command.clone())]);
        for _ in 0..3 {
            account_event(&mut recovered, &mut run, &clock, &commands);
        }
        assert_eq!(run.account().cash.to_string(), "9964.57");
        assert_eq!(run.account().open, 0);
        assert_eq!(
            sent.borrow()
                .iter()
                .filter(|f| matches!(f,Frame::Text(t) if t.contains("\"buy\":")))
                .count(),
            1
        );
        assert!(
            !recovery_sent
                .borrow()
                .iter()
                .any(|f| matches!(f,Frame::Text(t) if t.contains("\"buy\":")))
        );
        run.publish(
            &format!("phase10_execution_recovery_{portfolio}"),
            "signals 1 accepted 1 settled 1 unresolved 0",
        );
    }
}

#[test]
fn inspection_reports_account_acknowledgement_and_both_proposals_without_buying() {
    let clock = FakeClock::at(PURCHASE + 60 * SECOND);
    let (mut options, sent) = options(
        vec![
            frame("balance-before", 1),
            frame("transaction-ack", 2),
            frame("proposal-call", 3),
            frame("proposal-put", 4),
        ],
        &clock,
    );
    let config = Config::parse(
        r#"
schema_version=1
run_mode="research"
[storage]
historical_data_dir="retained"
publication_uri="file:///unused"
[[brokers]]
id="deriv"
kind="deriv"
public_endpoint="wss://example.invalid/public"
bootstrap_endpoint="https://example.invalid/trading/v1/options"
app_id="SYNTHETIC-APP"
credential="SYNTHETIC_REFERENCE"
account_class="demo"
[[instruments]]
broker="deriv"
provider_symbol="R_50"
quote_currency="USD"
price_scale=4
native_granularity={kind="tick"}
candles=[{duration_seconds=1,offset_seconds=0}]
[history]
broker="deriv"
instruments=["R_50"]
role="development"
start="2026-09-13T00:00:00Z"
end="2026-09-13T01:00:00Z"
[inspect]
live_observations=1
live_seconds=1
proposal={stake="10",duration_seconds=15}
"#,
    )
    .unwrap();
    let mut report = inspect::InspectionReport {
        broker: "deriv".into(),
        kind: "deriv".into(),
        endpoint_host: "example.invalid".into(),
        started: "2026-09-14T00:00:00Z".into(),
        checks: vec![],
    };
    inspect::observe_account(&config, &mut options, &mut report);
    assert_eq!(report.checks.len(), 4);
    assert!(report.checks.iter().all(|check| check.result == "verified"));
    assert!(matches!(
        report.checks[1].detail,
        inspect::InspectionDetail::TransactionAcknowledged
    ));
    for (check, direction, spot, spot_time, word) in [
        (
            &report.checks[2],
            Direction::Buy,
            "92.0409",
            "2026-09-14T00:50:34.000000Z",
            "higher",
        ),
        (
            &report.checks[3],
            Direction::Sell,
            "92.0305",
            "2026-09-14T00:50:36.000000Z",
            "lower",
        ),
    ] {
        let inspect::InspectionDetail::Proposal {
            direction: actual,
            ask_price,
            payout,
            spot: actual_spot,
            spot_time: actual_time,
            longcode,
        } = &check.detail
        else {
            panic!("{check:?}")
        };
        assert_eq!(*actual, direction);
        assert_eq!(ask_price.to_string(), "10");
        assert_eq!(payout.to_string(), "18.83");
        assert_eq!(actual_spot, spot);
        assert_eq!(actual_time, spot_time);
        assert!(longcode.contains(word));
    }
    let requests = sent.borrow();
    assert_eq!(requests.len(), 4);
    for frame in requests.iter() {
        let Frame::Text(text) = frame else { panic!() };
        assert!(!text.contains("\"buy\":"));
    }
    let scratch = Scratch::new("phase10_execution_inspection");
    let mut out = Vec::new();
    inspect::publish(
        &report,
        &Store::filesystem(scratch.path("retained")),
        &Store::filesystem(scratch.path("published")),
        &mut out,
    )
    .unwrap();
    let out = String::from_utf8(out).unwrap();
    assert!(out.contains("transaction_acknowledged"));
    assert!(!out.contains("SYNTHETICACCOUNT"));
}

#[test]
fn socket_send_failure_is_possibly_sent_and_claim_is_never_written_twice() {
    let mut clock = FakeClock::at(PURCHASE - SECOND);
    let (connector, sent) =
        common::broker::failing_connector(vec![vec![frame("proposal-call", 1)]], &clock, 2);
    let mut options = connect_options(
        connector,
        &clock,
        "ws://127.0.0.1/trading/v1/options/ws/demo",
    );
    let (mut run, prepared) = one_prepared(&mut options, &clock);
    advance(&mut clock, PURCHASE);
    let outcome = options.purchase(&prepared).unwrap();
    assert!(matches!(outcome, PurchaseOutcome::PossiblySent { .. }));
    run.step(
        clock.now_micros(),
        vec![purchase_observation(
            &prepared.command,
            &prepared.dispatch_claim,
            outcome,
            clock.now_micros(),
        )],
    );
    assert_eq!(run.account().blocked.len(), 1);
    assert_eq!(run.account().reserved.to_string(), "10.00");
    assert_eq!(sent.borrow().len(), 2);
    assert!(options.purchase(&prepared).is_err());
    let mut replacement = prepared.clone();
    replacement.dispatch_claim.push_str(":replacement");
    assert!(options.purchase(&replacement).is_err());
    assert_eq!(sent.borrow().len(), 2);
}

#[test]
fn proposal_scope_and_provider_errors_stop_before_preparation() {
    let clock = FakeClock::at(PURCHASE);
    let (mut options, sent) = options(vec![], &clock);
    for field in ["broker", "currency", "semantics", "scale"] {
        let mut request = request(Direction::Buy);
        match field {
            "broker" => request.instrument.broker = "other".to_string().try_into().unwrap(),
            "currency" => request.currency = "EUR".to_string().try_into().unwrap(),
            "semantics" => request.settlement.rule = SettlementRule::PriceAtDueV1,
            "scale" => request.scale = 3.try_into().unwrap(),
            _ => unreachable!(),
        }
        assert!(options.proposal(&request).is_err());
        assert!(sent.borrow().is_empty());
    }
    for frame in [Frame::Text(r#"{"msg_type":"proposal","req_id":1,"error":{"code":"RateLimit","message":"Synthetic trade limit"}}"#.into()),frame("proposal-call",99),response(&change(&fixture("proposal-call"),"proposal","spot","null"),1), response(&change(&fixture("proposal-call"),"proposal","ask_price","\"10\""),1)] {
        let (mut options,_)=self::options(vec![frame],&clock);
        assert!(options.proposal(&request(Direction::Buy)).is_err());
    }
}

#[test]
fn proposal_before_signal_restart_resupplies_input_and_continues_identically() {
    let clock = FakeClock::at(PURCHASE - SECOND);
    let (mut options, _) = options(vec![frame("proposal-call", 1)], &clock);
    let proposal = options.proposal(&request(Direction::Buy)).unwrap();
    let at = clock.now_micros();
    let mut uninterrupted = Engine::new(definition(false)).unwrap();
    let mut lines = uninterrupted
        .drain()
        .iter()
        .map(FinancialEvent::to_line)
        .collect::<Vec<_>>();
    uninterrupted
        .step(at, vec![quote("call", proposal.clone())])
        .unwrap();
    assert!(uninterrupted.drain().is_empty());
    let mut restored = Engine::restore(lines.iter().cloned().map(Ok)).unwrap();
    restored.step(at, vec![quote("call", proposal)]).unwrap();
    assert!(restored.drain().is_empty());
    for engine in [&mut uninterrupted, &mut restored] {
        engine.step(at, vec![tick(at), row(at)]).unwrap();
    }
    let admitted = uninterrupted.drain();
    assert!(matches!(
        admitted[0].kind,
        EventKind::Signal {
            disposition: Disposition::Admitted,
            ..
        }
    ));
    let continued = restored.drain();
    let suffix = admitted
        .iter()
        .map(FinancialEvent::to_line)
        .collect::<Vec<_>>();
    assert_eq!(
        suffix,
        continued
            .iter()
            .map(FinancialEvent::to_line)
            .collect::<Vec<_>>()
    );
    lines.extend(suffix);
    assert_eq!(uninterrupted.state_identity(), restored.state_identity());
    let prepared = prepared(&admitted, "call");
    let (debit, liability) = purchase_fact(fixture("buy-call").as_bytes()).unwrap();
    for engine in [&mut uninterrupted, &mut restored] {
        engine
            .step(
                PURCHASE,
                vec![Observation::Purchased {
                    command: prepared.command.clone(),
                    source: source("purchase", PURCHASE),
                    debit,
                    liability: liability.clone(),
                }],
            )
            .unwrap();
    }
    assert_eq!(
        uninterrupted
            .drain()
            .iter()
            .map(FinancialEvent::to_line)
            .collect::<Vec<_>>(),
        restored
            .drain()
            .iter()
            .map(FinancialEvent::to_line)
            .collect::<Vec<_>>()
    );
    assert_eq!(uninterrupted.state_identity(), restored.state_identity());
}

#[test]
fn same_second_provider_purchase_is_accepted_as_causal() {
    // The provider supplies integer seconds; preparation and purchase may share a second.
    let clock = FakeClock::at(PURCHASE);
    let (mut options, _) = options(
        vec![frame("proposal-call", 1), frame("buy-call", 2)],
        &clock,
    );
    let (mut run, prepared) = one_prepared(&mut options, &clock);
    let outcome = options.purchase(&prepared).unwrap();
    assert!(matches!(outcome, PurchaseOutcome::Accepted { .. }));
    let observation = purchase_observation(
        &prepared.command,
        &prepared.dispatch_claim,
        outcome,
        clock.now_micros(),
    );
    assert!(
        clock.now_micros() > PURCHASE,
        "the dispatch is later in the same second"
    );
    let events = run.step(clock.now_micros(), vec![observation]);
    assert!(
        events
            .iter()
            .any(|event| matches!(event.kind, EventKind::Accepted { .. })),
        "{events:?}"
    );
    assert_eq!(run.account().cash.to_string(), "9945.74");
}

#[test]
fn real_local_socket_purchase_acceptance_rejection_and_lost_acknowledgement() {
    use binary_alpha_app::broker::transport::WebSocketConnector;
    use futures_util::{SinkExt, StreamExt};
    use tokio_tungstenite::tungstenite::Message;
    for mode in ["accepted", "rejected", "lost_ack"] {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let url = format!(
            "ws://{}/trading/v1/options/ws/demo",
            listener.local_addr().unwrap()
        );
        let server = std::thread::spawn(move || {
            tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async move {
                let listener=tokio::net::TcpListener::from_std(listener).unwrap();
                let (socket,_)=listener.accept().await.unwrap();let mut socket=tokio_tungstenite::accept_async(socket).await.unwrap();
                let Some(Ok(Message::Text(proposal)))=socket.next().await else {panic!("proposal request")};
                assert!(proposal.contains("\"proposal\":1"));
                let Frame::Text(quote)=frame("proposal-call",1) else {unreachable!()};socket.send(Message::Text(quote.into())).await.unwrap();
                let Some(Ok(Message::Text(buy)))=socket.next().await else {panic!("purchase request")};
                assert!(buy.contains("\"buy\":"));assert!(buy.contains("\"price\":10"));
                if mode=="lost_ack" {socket.close(None).await.unwrap();} else {
                    let text=if mode=="accepted" {replace(&fixture("buy-call"),"req_id","2")} else {r#"{"msg_type":"buy","req_id":2,"error":{"code":"InvalidContractProposal","message":"Synthetic rejection"}}"#.into()};
                    socket.send(Message::Text(text.into())).await.unwrap();
                }
            });
        });
        let mut clock = FakeClock::at(PURCHASE - SECOND);
        let mut options =
            connect_options(Box::new(WebSocketConnector::new().unwrap()), &clock, &url);
        let (mut run, prepared) = one_prepared(&mut options, &clock);
        let mut missing = prepared.clone();
        missing.dispatch_claim.clear();
        assert!(options.purchase(&missing).is_err());
        advance(&mut clock, PURCHASE);
        let outcome = options.purchase(&prepared).unwrap();
        assert!(matches!(
            (&outcome, mode),
            (PurchaseOutcome::Accepted { .. }, "accepted")
                | (PurchaseOutcome::Rejected { .. }, "rejected")
                | (PurchaseOutcome::PossiblySent { .. }, "lost_ack")
        ));
        run.step(
            clock.now_micros(),
            vec![purchase_observation(
                &prepared.command,
                &prepared.dispatch_claim,
                outcome,
                clock.now_micros(),
            )],
        );
        if mode == "accepted" {
            assert_eq!(run.account().cash.to_string(), "9945.74");
        } else {
            assert_eq!(run.account().cash.to_string(), "9955.74");
        }
        if mode == "lost_ack" {
            assert!(options.purchase(&prepared).is_err());
            let mut replacement = prepared.clone();
            replacement.dispatch_claim.push_str(":replacement");
            assert!(options.purchase(&replacement).is_err());
            assert_eq!(run.account().blocked.len(), 1);
        }
        server.join().unwrap();
    }
}

#[test]
fn strict_equality_is_a_synthetic_zero_credit_loss() {
    let mut clock = FakeClock::at(PURCHASE - SECOND);
    // Synthetic equal-price CALL under the pinned strict semantics: status, never is_sold, decides loss.
    let terminal = change(
        &change(
            &fixture("won"),
            "proposal_open_contract",
            "status",
            "\"lost\"",
        ),
        "proposal_open_contract",
        "exit_spot",
        "\"92.0252\"",
    );
    let cash = change(&fixture("transaction-win"), "transaction", "amount", "0");
    let (mut options, _) = options(
        vec![
            frame("transaction-ack", 1),
            frame("proposal-call", 2),
            frame("buy-call", 3),
            response(&terminal, 4),
            response(&cash, 1),
        ],
        &clock,
    );
    options.subscribe_transactions().unwrap();
    options.next_account_event(SECOND).unwrap();
    let (mut run, prepared) = one_prepared(&mut options, &clock);
    advance(&mut clock, PURCHASE);
    let outcome = options.purchase(&prepared).unwrap();
    run.step(
        clock.now_micros(),
        vec![purchase_observation(
            &prepared.command,
            &prepared.dispatch_claim,
            outcome,
            clock.now_micros(),
        )],
    );
    advance(&mut clock, PURCHASE + 16 * SECOND);
    options.subscribe_contract(CALL).unwrap();
    let commands = BTreeMap::from([(CALL.into(), prepared.command.clone())]);
    account_event(&mut options, &mut run, &clock, &commands);
    account_event(&mut options, &mut run, &clock, &commands);
    let settled = account_event(&mut options, &mut run, &clock, &commands);
    assert!(settled.iter().any(|event|matches!(&event.kind,EventKind::Settled {outcome:execution::Outcome::Loss,gross_return,path:Some(path),..} if gross_return.is_zero() && path.final_move_units==0)));
    assert_eq!(run.account().cash.to_string(), "9945.74");
    assert_eq!(run.engine.summary().portfolio.losses, 1);
    assert_eq!(run.engine.summary().portfolio.ties, 0);
}

#[test]
fn statement_pages_use_offset_and_preserve_explicit_zero_cash() {
    let clock = FakeClock::at(PURCHASE);
    let rows=(1..=100).map(|id|format!(r#"{{"action_type":"sell","amount":0,"transaction_id":{id},"contract_id":12859891379,"transaction_time":1789347054}}"#)).collect::<Vec<_>>().join(",");
    let first = format!(
        r#"{{"msg_type":"statement","req_id":1,"statement":{{"count":100,"transactions":[{rows}]}}}}"#
    );
    let last = r#"{"msg_type":"statement","req_id":2,"statement":{"count":1,"transactions":[{"action_type":"sell","amount":18.83,"transaction_id":101,"contract_id":12859891379,"transaction_time":1789347054}]}}"#;
    let (mut options, sent) = options(vec![Frame::Text(first), Frame::Text(last.into())], &clock);
    let cash = options.statement(1_789_347_035, 1_789_347_054).unwrap();
    assert_eq!(cash.len(), 101);
    assert!(cash[..100].iter().all(|fact| fact.cash.amount.is_zero()));
    assert_eq!(cash[100].cash.amount.to_string(), "18.83");
    let sent = sent.borrow();
    assert!(matches!(&sent[0],Frame::Text(text) if text.contains("\"offset\":0")));
    assert!(
        matches!(&sent[1],Frame::Text(text) if text.contains("\"offset\":100")&&text.contains("\"date_to\":1789347055"))
    );
}

#[test]
fn reconciliation_net_credit_must_match_recorded_sell_cash_on_generation_and_restore() {
    for external in [false, true] {
        let clock = FakeClock::at(PURCHASE);
        let (mut options, _) = options(vec![frame("proposal-call", 1)], &clock);
        let (mut run, prepared) = one_prepared(&mut options, &clock);
        let (debit, liability) = purchase_fact(fixture("buy-call").as_bytes()).unwrap();
        let at = clock.now_micros();
        run.step(
            at,
            vec![Observation::Purchased {
                command: prepared.command.clone(),
                source: source("buy", at),
                debit,
                liability,
            }],
        );
        run.step(
            at,
            vec![Observation::Cash {
                source: source("cash", at),
                fact: execution::CashFact {
                    account: "a".into(),
                    transaction_ref: "sale".into(),
                    contract_ref: Some(CALL.into()),
                    action: CashAction::Sell,
                    amount: decimal("18.83"),
                    time_micros: at,
                },
            }],
        );
        let resolution = |fee| {
            if external {
                Resolution::ExternallyClosed {
                    status: execution::TerminalStatus::Sold,
                    gross_return: decimal("18.83"),
                    terminal_fee: decimal(fee),
                }
            } else {
                Resolution::Settled {
                    outcome: execution::Outcome::Win,
                    gross_return: decimal("18.83"),
                    terminal_fee: decimal(fee),
                }
            }
        };
        let mut invalid = Engine::restore(run.lines.iter().cloned().map(Ok)).unwrap();
        let error = invalid
            .step(
                at,
                vec![Observation::Reconciliation {
                    command: prepared.command.clone(),
                    source: source("reconcile", at),
                    resolution: resolution("1"),
                }],
            )
            .unwrap_err();
        assert!(
            error.contains("contradicts the recorded cash transaction"),
            "{error}"
        );
        let net_matches = if external {
            Resolution::ExternallyClosed {
                status: execution::TerminalStatus::Sold,
                gross_return: decimal("19.83"),
                terminal_fee: decimal("1"),
            }
        } else {
            Resolution::Settled {
                outcome: execution::Outcome::Win,
                gross_return: decimal("19.83"),
                terminal_fee: decimal("1"),
            }
        };
        let mut exact = Engine::restore(run.lines.iter().cloned().map(Ok)).unwrap();
        exact
            .step(
                at,
                vec![Observation::Reconciliation {
                    command: prepared.command.clone(),
                    source: source("net-matched", at),
                    resolution: net_matches,
                }],
            )
            .unwrap();
        let mut exact_lines = run.lines.clone();
        exact_lines.extend(exact.drain().iter().map(FinancialEvent::to_line));
        let restored = Engine::restore(exact_lines.into_iter().map(Ok)).unwrap();
        assert_eq!(restored.accounts()[0].cash.to_string(), "9964.57");
        reconcile(&mut run, &prepared, resolution("0"), at);
        assert_eq!(run.account().cash.to_string(), "9964.57");
        assert_eq!(run.account().open, 0);
        let altered = run.lines.iter().map(|line| {
            let mut event = FinancialEvent::from_line(line).unwrap();
            if let EventKind::Reconciled {
                resolution: stated, ..
            } = &mut event.kind
            {
                *stated = resolution("1");
            }
            Ok(event.to_line())
        });
        let error = Engine::restore(altered)
            .err()
            .expect("contradictory fee cannot restore");
        assert!(
            error.contains("contradicts the recorded cash transaction"),
            "{error}"
        );
    }
}

fn two_accounts() -> RunDefinition {
    let mut definition = definition(false);
    let mut account = definition.replay.accounts[0].clone();
    account.id = "a2".into();
    definition.replay.accounts.push(account);
    let mut binding = definition.replay.bindings[0].clone();
    binding.id = "call2".into();
    binding.account = "a2".into();
    definition.replay.bindings.push(binding);
    definition
}

#[test]
fn proposal_account_instrument_and_canonical_request_are_verified_on_admission_and_restore() {
    let clock = FakeClock::at(PURCHASE);
    let (mut options, _) = options(vec![frame("proposal-call", 1)], &clock);
    let proposal = options.proposal(&request(Direction::Buy)).unwrap();
    assert_eq!(proposal.account, "a");
    assert_eq!(proposal.instrument, "deriv:R_50");
    assert_eq!(proposal.receipt_micros, clock.now_micros());
    assert_eq!(
        proposal.request_identity,
        proposal.canonical_request_identity().unwrap()
    );
    for binding in ["call2", "call"] {
        let mut bad = proposal.clone();
        if binding == "call" {
            bad.request_identity = "0".repeat(64);
        }
        let mut engine = Engine::new(two_accounts()).unwrap();
        assert!(
            engine
                .step(clock.now_micros(), vec![quote(binding, bad)])
                .unwrap_err()
                .contains("contract request")
        );
        assert!(
            engine
                .accounts()
                .iter()
                .all(|account| account.reserved.is_zero())
        );
    }
    let mut run = Run::new(false);
    run.step(
        clock.now_micros(),
        vec![
            quote("call", proposal),
            tick(clock.now_micros()),
            row(clock.now_micros()),
        ],
    );
    for change_account in [false, true] {
        let altered = run.lines.iter().map(|line| {
            let mut event = FinancialEvent::from_line(line).unwrap();
            if let EventKind::Signal {
                proposal: Some(proposal),
                ..
            } = &mut event.kind
            {
                if change_account {
                    proposal.account = "a2".into();
                } else {
                    proposal.instrument = "deriv:R_100".into();
                }
                proposal.request_identity = proposal.canonical_request_identity().unwrap();
            }
            Ok(event.to_line())
        });
        assert!(
            Engine::restore(altered)
                .err()
                .unwrap()
                .contains("contract request")
        );
    }
}

#[test]
fn terminal_enrichment_is_monotonic_before_and_after_closure() {
    let clock = FakeClock::at(PURCHASE);
    let (mut options, _) = options(vec![frame("proposal-call", 1)], &clock);
    let (mut run, prepared) = one_prepared(&mut options, &clock);
    let (debit, liability) = purchase_fact(fixture("buy-call").as_bytes()).unwrap();
    run.step(
        clock.now_micros(),
        vec![Observation::Purchased {
            command: prepared.command.clone(),
            source: source("buy", PURCHASE),
            debit,
            liability,
        }],
    );
    let at = PURCHASE + 16 * SECOND;
    let fact = execution::TerminalFact {
        status: execution::TerminalStatus::Won,
        exit_price_units: None,
        exit_time_micros: None,
        transaction_ref: None,
    };
    let terminal = |fact| Observation::Terminal {
        command: prepared.command.clone(),
        source: source("same-terminal-source", at),
        fact,
    };
    run.step(at, vec![terminal(fact.clone())]);
    let enriched = execution::TerminalFact {
        transaction_ref: Some("sell".into()),
        ..fact.clone()
    };
    run.step(at, vec![terminal(enriched.clone())]);
    assert!(run.step(at, vec![terminal(fact)]).is_empty());
    run.step(
        at,
        vec![Observation::Cash {
            source: source("cash", at),
            fact: execution::CashFact {
                account: "a".into(),
                transaction_ref: "sell".into(),
                contract_ref: Some(CALL.into()),
                action: CashAction::Sell,
                amount: decimal("18.83"),
                time_micros: at,
            },
        }],
    );
    assert_eq!(run.account().open, 0);
    let enriched = execution::TerminalFact {
        exit_price_units: Some(920_409),
        exit_time_micros: Some(at),
        ..enriched
    };
    run.step(at, vec![terminal(enriched.clone())]);
    assert!(run.step(at, vec![terminal(enriched.clone())]).is_empty());
    assert_eq!(run.account().cash.to_string(), "9964.57");
    let changed = execution::TerminalFact {
        exit_price_units: Some(920_410),
        ..enriched
    };
    assert!(
        run.engine
            .step(at, vec![terminal(changed)])
            .unwrap_err()
            .contains("contradicts")
    );
}

#[test]
fn closed_contract_partial_updates_are_consistent_noops() {
    let clock = FakeClock::at(PURCHASE);
    let (mut options, _) = options(vec![frame("proposal-call", 1)], &clock);
    let (mut run, prepared) = one_prepared(&mut options, &clock);
    let (debit, liability) = purchase_fact(fixture("buy-call").as_bytes()).unwrap();
    run.step(
        clock.now_micros(),
        vec![Observation::Purchased {
            command: prepared.command.clone(),
            source: source("buy", PURCHASE),
            debit,
            liability,
        }],
    );
    let at = PURCHASE + 16 * SECOND;
    run.step(
        at,
        vec![
            Observation::Terminal {
                command: prepared.command.clone(),
                source: source("lost", at),
                fact: execution::TerminalFact {
                    status: execution::TerminalStatus::Lost,
                    exit_price_units: None,
                    exit_time_micros: None,
                    transaction_ref: Some("zero".into()),
                },
            },
            Observation::Cash {
                source: source("zero", at),
                fact: execution::CashFact {
                    account: "a".into(),
                    transaction_ref: "zero".into(),
                    contract_ref: Some(CALL.into()),
                    action: CashAction::Sell,
                    amount: decimal("0"),
                    time_micros: at,
                },
            },
        ],
    );
    let update = |entry| Observation::ContractUpdate {
        command: prepared.command.clone(),
        source: source("late-update", at),
        entry_price_units: Some(920_409),
        entry_time_micros: Some(entry),
        start_micros: Some(PURCHASE),
        expiry_micros: Some(PURCHASE + 15 * SECOND),
    };
    let before = run.lines.clone();
    assert!(run.step(at, vec![update(PURCHASE)]).is_empty());
    assert_eq!(before, run.lines);
    assert_eq!(run.account().open, 0);
    assert!(
        run.engine
            .step(at, vec![update(PURCHASE - SECOND)])
            .unwrap_err()
            .contains("contradicts")
    );
}

#[test]
fn nullable_and_contract_id_only_responses_add_no_fact() {
    for object in [
        format!(r#"{{"contract_id":{CALL}}}"#),
        format!(
            r#"{{"contract_id":{CALL},"status":null,"currency":null,"underlying_symbol":null,"contract_type":null,"entry_spot":null,"entry_spot_time":null,"date_start":null,"date_expiry":null,"exit_spot":null,"exit_spot_time":null,"transaction_ids":null,"current_spot_time":null,"purchase_time":null,"sell_time":null}}"#
        ),
    ] {
        let clock = FakeClock::at(PURCHASE);
        let partial = replace(&fixture("open-call"), "proposal_open_contract", &object);
        let (mut options, _) = options(vec![response(&partial, 1)], &clock);
        options.subscribe_contract(CALL).unwrap();
        assert!(options.next_account_event(1).unwrap().is_none());
    }
}

#[test]
fn identical_transaction_ids_are_independent_across_accounts() {
    let clock = FakeClock::at(PURCHASE);
    let (mut options, _) = options(vec![frame("proposal-call", 1)], &clock);
    let proposal = options.proposal(&request(Direction::Buy)).unwrap();
    let mut second = proposal.clone();
    second.account = "a2".into();
    second.request_identity = second.canonical_request_identity().unwrap();
    let mut run = Run {
        engine: Engine::new(two_accounts()).unwrap(),
        lines: vec![],
    };
    run.lines = run
        .engine
        .drain()
        .iter()
        .map(FinancialEvent::to_line)
        .collect();
    let at = clock.now_micros();
    let signals = run.step(
        at,
        vec![
            quote("call", proposal),
            quote("call2", second),
            tick(at),
            row(at),
        ],
    );
    let (debit, liability) = purchase_fact(fixture("buy-call").as_bytes()).unwrap();
    for account in ["a", "a2"] {
        run.step(
            at,
            vec![Observation::Cash {
                source: source("buy-cash", at),
                fact: execution::CashFact {
                    account: account.into(),
                    transaction_ref: liability.transaction_ref.clone(),
                    contract_ref: Some(liability.contract_ref.clone()),
                    action: CashAction::Buy,
                    amount: decimal("-10"),
                    time_micros: at,
                },
            }],
        );
    }
    for binding in ["call", "call2"] {
        let command = prepared(&signals, binding).command;
        run.step(
            at,
            vec![Observation::Purchased {
                command,
                source: source("buy", at),
                debit,
                liability: liability.clone(),
            }],
        );
    }
    for (account, amount) in [("a", "1"), ("a2", "2")] {
        run.step(
            at,
            vec![Observation::Cash {
                source: source("external", at),
                fact: execution::CashFact {
                    account: account.into(),
                    transaction_ref: "12345".into(),
                    contract_ref: None,
                    action: CashAction::Sell,
                    amount: decimal(amount),
                    time_micros: at,
                },
            }],
        );
    }
    for account in ["a", "a2"] {
        assert!(
            run.engine
                .accounts()
                .iter()
                .find(|a| a.id == account)
                .unwrap()
                .blocked
                .contains_key(&format!("transaction:{account}:12345"))
        );
        run.step(
            at,
            vec![Observation::Reconciliation {
                command: format!("transaction:{account}:12345"),
                source: source("external-resolution", at),
                resolution: Resolution::External,
            }],
        );
    }
    assert!(
        run.engine
            .accounts()
            .iter()
            .all(|account| account.blocked.is_empty()
                && account.cash.to_string() == "9945.74"
                && account.open == 1)
    );
}

#[test]
fn strict_mapping_rejects_unmeasured_scope_before_proposal_write() {
    for (class, currency) in [(AccountClass::Real, "USD"), (AccountClass::Demo, "EUR")] {
        let clock = FakeClock::at(PURCHASE);
        let (connector, sent) = connector(vec![vec![]], &clock);
        let mut options = connect_options_scoped(
            connector,
            &clock,
            &format!("ws://127.0.0.1/trading/v1/options/ws/{class}"),
            class,
            currency,
            RateBudgets::default(),
        );
        let mut request = request(Direction::Buy);
        request.currency = currency.to_string().try_into().unwrap();
        assert_eq!(
            options.proposal(&request).unwrap_err(),
            "deriv: the strict Rise/Fall mapping is measured only for demo USD accounts; inspect and extend the mapping before use"
        );
        assert!(sent.borrow().is_empty());
    }
}

#[test]
fn proposal_commission_requires_resolved_zero_fee_inclusion() {
    for commission in ["25", "0"] {
        let clock = FakeClock::at(PURCHASE);
        let frame = change(
            &fixture("proposal-call"),
            "proposal",
            "commission",
            commission,
        );
        let (mut options, sent) = options(vec![response(&frame, 1)], &clock);
        let result = options.proposal(&request(Direction::Buy));
        if commission == "0" {
            assert!(result.is_ok());
        } else {
            assert!(result.unwrap_err().contains("commission"));
        }
        assert_eq!(sent.borrow().len(), 1);
    }
}

#[test]
fn inspection_and_errors_never_retain_provider_messages_or_malformed_values() {
    const PRIVATE: &str = "SYNTHETICPRIVATEACCOUNT";
    let error = format!(
        r#"{{"msg_type":"balance","req_id":1,"error":{{"code":"AccountDisabled","message":"Account {PRIVATE} is disabled"}}}}"#
    );
    let malformed = format!(r#"{{"msg_type":"balance","req_id":"{PRIVATE}"}}"#);
    for frame in [error, malformed] {
        let clock = FakeClock::at(PURCHASE);
        let (mut options, _) = options(vec![Frame::Text(frame.clone())], &clock);
        let error = options.balance().unwrap_err();
        assert!(!error.contains(PRIVATE));
        assert!(
            error.contains("AccountDisabled") || error.contains("req_id field"),
            "{error}"
        );
        let (mut options, _) = self::options(vec![Frame::Text(frame)], &clock);
        let config =
            Config::parse(include_str!("fixtures/phase10/execution-definition.toml")).unwrap();
        let mut report = inspect::InspectionReport {
            broker: "deriv".into(),
            kind: "deriv".into(),
            endpoint_host: "127.0.0.1".into(),
            started: "synthetic".into(),
            checks: vec![],
        };
        inspect::observe_account(&config, &mut options, &mut report);
        let json = serde_json::to_string(&report).unwrap();
        assert!(!json.contains(PRIVATE));
        assert!(!json.contains("is disabled"));
        assert!(!format!("{report:?}").contains(PRIVATE));
    }
}

#[test]
fn actual_options_requests_share_trade_and_account_rate_windows() {
    let clock = FakeClock::at(PURCHASE);
    let mut limits = RateBudgets::default();
    limits.trade.per_minute = 1;
    limits.account.per_minute = 1;
    let frames = vec![
        frame("proposal-call", 1),
        frame("buy-call", 2),
        frame("open-call", 3),
        frame("balance-before", 4),
        frame("statement-four", 5),
    ];
    let (connector, sent) = connector(vec![frames], &clock);
    let mut options = connect_options_scoped(
        connector,
        &clock,
        "ws://127.0.0.1/trading/v1/options/ws/demo",
        AccountClass::Demo,
        "USD",
        limits,
    );
    let proposal = options.proposal(&request(Direction::Buy)).unwrap();
    let after_proposal = clock.now_micros();
    options
        .purchase(&PreparedPurchase {
            command: "budget-command".into(),
            dispatch_claim: "budget-claim".into(),
            proposal_identity: proposal.identity,
            maximum_price: decimal("10"),
        })
        .unwrap();
    assert!(clock.now_micros() >= after_proposal - 10 + 60 * SECOND);
    let after_buy = clock.now_micros();
    options.subscribe_contract(CALL).unwrap();
    assert!(clock.now_micros() >= after_buy - 10 + 60 * SECOND);
    let before_balance = clock.now_micros();
    options.balance().unwrap();
    assert_eq!(clock.now_micros(), before_balance + 10);
    options.statement(1_789_347_035, 1_789_347_054).unwrap();
    assert!(clock.now_micros() >= before_balance + 60 * SECOND);
    assert_eq!(sent.borrow().len(), 5);
}

#[test]
fn schema_contract_readers_reject_missing_economic_fields() {
    let clock = FakeClock::at(PURCHASE + 60 * SECOND);
    let remove = |text: &str, owner: &str, field: &str| {
        let outer: BTreeMap<String, Box<RawValue>> = serde_json::from_str(text).unwrap();
        let mut body: BTreeMap<String, Box<RawValue>> =
            serde_json::from_str(outer[owner].get()).unwrap();
        body.remove(field);
        replace(text, owner, &serde_json::to_string(&body).unwrap())
    };
    let missing = remove(&fixture("proposal-call"), "proposal", "payout");
    let (mut options, _) = options(vec![response(&missing, 1)], &clock);
    assert!(
        options
            .proposal(&request(Direction::Buy))
            .unwrap_err()
            .contains("proposal: malformed payout field")
    );
    let missing = remove(&fixture("buy-call"), "buy", "buy_price");
    assert!(
        purchase_fact(missing.as_bytes())
            .unwrap_err()
            .contains("buy: malformed buy_price field")
    );
    let portfolio = change(
        &fixture("portfolio"),
        "portfolio",
        "contracts",
        r#"[{"contract_id":12859891379,"transaction_id":24655144239,"buy_price":10,"payout":18.83,"purchase_time":1789347036,"currency":"USD","contract_type":"CALL"}]"#,
    );
    let (mut options, _) = self::options(vec![response(&portfolio, 1)], &clock);
    assert!(
        options
            .open_contracts()
            .unwrap_err()
            .contains("portfolio: malformed underlying_symbol field")
    );
    let statement = change(&fixture("statement-four"), "statement", "count", "1");
    let statement = change(
        &statement,
        "statement",
        "transactions",
        r#"[{"action_type":"buy","transaction_id":24655144239,"contract_id":12859891379,"transaction_time":1789347036,"payout":18.83}]"#,
    );
    let (mut options, _) = self::options(vec![response(&statement, 1)], &clock);
    assert!(
        options
            .statement(1_789_347_035, 1_789_347_054)
            .unwrap_err()
            .contains("statement: malformed amount field")
    );
    let missing = remove(&fixture("transaction-buy-call"), "transaction", "amount");
    let (mut options, _) = self::options(
        vec![frame("transaction-ack", 1), response(&missing, 1)],
        &clock,
    );
    options.subscribe_transactions().unwrap();
    options.next_account_event(SECOND).unwrap();
    assert!(
        options
            .next_account_event(SECOND)
            .unwrap_err()
            .contains("transaction: amount missing")
    );
}

#[test]
fn proposal_receipt_uses_response_time_when_spot_changes_in_flight() {
    let clock = FakeClock::at(PURCHASE - 5);
    let updated = change(
        &fixture("proposal-call"),
        "proposal",
        "spot_time",
        &(PURCHASE / SECOND).to_string(),
    );
    let (mut options, _) = options(vec![response(&updated, 1)], &clock);
    let quote = options.proposal(&request(Direction::Buy)).unwrap();
    assert_eq!(quote.spot_time_micros, PURCHASE);
    assert_eq!(quote.receipt_micros, PURCHASE + 5);
    let mut run = Run::new(false);
    let events = run.step(
        clock.now_micros(),
        vec![
            self::quote("call", quote),
            tick(clock.now_micros()),
            row(clock.now_micros()),
        ],
    );
    assert!(matches!(
        events[0].kind,
        EventKind::Signal {
            disposition: Disposition::Admitted,
            ..
        }
    ));
}

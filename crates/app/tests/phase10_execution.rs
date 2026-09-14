mod common;

use binary_alpha_app::broker::deriv::{
    DerivAccounts, DerivOptions, purchase_fact, purchase_observation, to_observation,
};
use binary_alpha_app::broker::transport::{Connector, Frame, Http};
use binary_alpha_app::broker::{
    AccountEvent, AccountIdentity, Clock, OptionsBroker, PreparedPurchase, ProposalRequest,
    PurchaseOutcome,
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
use common::broker::{FakeClock, connector};
use serde_json::value::RawValue;
use std::cell::RefCell;
use std::collections::{BTreeMap, VecDeque};
use std::fs;
use std::path::Path;
use std::rc::Rc;

const SECOND: i64 = 1_000_000;
const PURCHASE: i64 = 1_789_347_036_000_000;
const CALL: &str = "12859891379";
const PUT: &str = "12859892839";
fn decimal(text: &str) -> Decimal {
    Decimal::parse(text).unwrap()
}
fn fixture(name: &str) -> String {
    fs::read_to_string(
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/phase10")
            .join(format!("deriv-execution-{name}.json")),
    )
    .unwrap()
    .trim_end()
    .into()
}
fn replace(text: &str, key: &str, value: &str) -> String {
    let mut map: BTreeMap<String, Box<RawValue>> = serde_json::from_str(text).unwrap();
    map.insert(key.into(), RawValue::from_string(value.into()).unwrap());
    serde_json::to_string(&map).unwrap()
}
fn change(text: &str, owner: &str, key: &str, value: &str) -> String {
    let map: BTreeMap<String, Box<RawValue>> = serde_json::from_str(text).unwrap();
    replace(text, owner, &replace(map[owner].get(), key, value))
}
fn frame(name: &str, req_id: u64) -> Frame {
    Frame::Text(replace(&fixture(name), "req_id", &req_id.to_string()))
}
fn response(text: &str, req_id: u64) -> Frame {
    Frame::Text(replace(text, "req_id", &req_id.to_string()))
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
    replay.decision_start = "2026-09-13T00:00:00Z".into();
    replay.decision_end = "2026-09-15T00:00:00Z".into();
    replay.accounts[0].broker = instrument().broker;
    replay.accounts[0].currency = request(Direction::Buy).currency.clone();
    replay.accounts[0].initial_cash = decimal("9955.74");
    replay.reporting_currency = replay.accounts[0].currency.clone();
    let terms = &mut replay.contracts[0];
    terms.id = "call".into();
    terms.currency = replay.accounts[0].currency.clone();
    terms.stake = decimal("10");
    terms.quoted_cost = decimal("10");
    terms.duration_micros = 15 * SECOND;
    terms.win.gross_return = decimal("0");
    terms.tie.gross_return = decimal("0");
    terms.settlement = settlement();
    terms.semantics = Some(ContractSemantics::RiseFallStrictV1);
    replay.bindings[0].id = "call".into();
    replay.bindings[0].contract = "call".into();
    replay.bindings[0].instrument = instrument().to_string();
    let envelope = &mut replay.bindings[0].envelope;
    envelope.max_purchase_cost = decimal("10");
    envelope.min_winning_net_return = decimal("8.83");
    envelope.settlement_rule = SettlementRule::BrokerAuthoritativeV1;
    envelope.semantics = terms.semantics;
    let policy = &mut replay.risk_policies[0];
    policy.max_open_per_strategy = Some(3);
    policy.max_proposal_age_micros = Some(60 * SECOND);
    policy.max_quote_age_micros = 120 * SECOND;
    policy.max_feature_age_micros = 120 * SECOND;
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
struct Bootstrap(VecDeque<Vec<u8>>);
impl Http for Bootstrap {
    fn get_json(&mut self, _: &str, _: &[(String, String)]) -> Result<Vec<u8>, String> {
        self.0.pop_front().ok_or("unexpected HTTP request".into())
    }
    fn post_json(&mut self, _: &str, _: &[(String, String)]) -> Result<Vec<u8>, String> {
        self.0.pop_front().ok_or("unexpected HTTP request".into())
    }
}
fn connect_options(connector: Box<dyn Connector>, clock: &FakeClock, url: &str) -> DerivOptions {
    let settings = DerivSettings {
        id: instrument().broker,
        public_endpoint: "ws://127.0.0.1/public".into(),
        bootstrap_endpoint: "http://127.0.0.1/trading/v1/options".into(),
        app_id: "SYNTHETIC-APP".into(),
        credential: Some("SYNTHETIC_REFERENCE".into()),
        account_class: Some(AccountClass::Demo),
        budgets: None,
    };
    let mut http = Bootstrap(vec![br#"{"data":[{"account_id":"SYNTHETICACCOUNT","account_type":"demo","status":"active","currency":"USD"}]}"#.to_vec(), format!(r#"{{"data":{{"url":"{url}"}}}}"#).into_bytes()].into());
    let address = DerivAccounts::bootstrap(&settings, &mut http, "synthetic-credential").unwrap();
    let account = AccountIdentity {
        broker: settings.id,
        account: "a".into(),
        class: AccountClass::Demo,
        currency: address.currency.clone(),
    };
    DerivOptions::connect(
        address,
        account,
        &[(instrument(), scale())],
        connector,
        Box::new(clock.clone()),
        RateBudgets::default(),
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
    let call = options
        .proposal(&request(Direction::Buy), clock.now_micros())
        .unwrap();
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
    let put_quote = options
        .proposal(&request(Direction::Sell), clock.now_micros())
        .unwrap();
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
        for cash in rows {
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
    let first = options
        .proposal(&request(Direction::Buy), clock.now_micros())
        .unwrap();
    let later = options
        .proposal(&request(Direction::Buy), clock.now_micros())
        .unwrap();
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
    let quote = options
        .proposal(&request(Direction::Buy), clock.now_micros())
        .unwrap();
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
    let proposal = options
        .proposal(&request(Direction::Buy), clock.now_micros())
        .unwrap();
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
    reconcile(
        &mut run,
        &prepared,
        Resolution::Purchased { debit, liability },
        clock.now_micros(),
    );
    assert!(run.account().blocked.is_empty());
    assert_eq!(run.account().cash.to_string(), "9945.24");
    run.publish(
        "phase10_execution_deficit",
        "signals 1 accepted 1 settled 0 unresolved 0",
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
        let portfolio_frame = r#"{"msg_type":"portfolio","req_id":1,"portfolio":{"contracts":[{"contract_id":12859891379,"transaction_id":24655144239,"buy_price":10,"payout":18.83,"purchase_time":1789347036,"date_start":1789347036,"expiry_time":1789347051,"currency":"USD","symbol":"R_50","contract_type":"CALL"}]}}"#;
        let mut frames = if portfolio {
            vec![Frame::Text(portfolio_frame.into())]
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
            let cash = rows.remove(0);
            assert_eq!(cash.action, CashAction::Buy);
            assert_eq!(cash.amount.to_string(), "-10");
            // CashFact alone has no payout or purchase clock. Match it to retained buy evidence;
            // do not infer purchase terms or timing from the proposal or cash clock.
            let (debit, liability) = purchase_fact(fixture("buy-call").as_bytes()).unwrap();
            assert_eq!(
                cash.contract_ref.as_deref(),
                Some(liability.contract_ref.as_str())
            );
            assert_eq!(cash.transaction_ref, liability.transaction_ref);
            assert_eq!(
                decimal("0")
                    .checked_sub(cash.amount)
                    .unwrap()
                    .compare(debit)
                    .unwrap(),
                std::cmp::Ordering::Equal
            );
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
    let mut clock = FakeClock::at(PURCHASE + 60 * SECOND);
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
    inspect::observe_account(&config, &mut options, &mut clock, &mut report);
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
        assert!(options.proposal(&request, clock.now_micros()).is_err());
        assert!(sent.borrow().is_empty());
    }
    for frame in [Frame::Text(r#"{"msg_type":"proposal","req_id":1,"error":{"code":"RateLimit","message":"Synthetic trade limit"}}"#.into()),frame("proposal-call",99),response(&change(&fixture("proposal-call"),"proposal","spot","null"),1), response(&change(&fixture("proposal-call"),"proposal","ask_price","\"10\""),1)] {
        let (mut options,_)=self::options(vec![frame],&clock);
        assert!(options.proposal(&request(Direction::Buy),clock.now_micros()).is_err());
    }
}

#[test]
fn financial_ledger_does_not_restore_an_unadmitted_proposal() {
    // Documents the current Engine boundary; changing it belongs to the engine owner.
    let clock = FakeClock::at(PURCHASE - SECOND);
    let (mut options, _) = options(vec![frame("proposal-call", 1)], &clock);
    let proposal = options
        .proposal(&request(Direction::Buy), clock.now_micros())
        .unwrap();
    let mut run = Run::new(false);
    let at = clock.now_micros();
    assert!(run.step(at, vec![quote("call", proposal)]).is_empty());
    let events = run.step(at, vec![tick(at), row(at)]);
    assert!(matches!(
        events[0].kind,
        EventKind::Signal {
            disposition: Disposition::NoProposal,
            ..
        }
    ));
    assert_eq!(run.account().reserved.to_string(), "0.00");
}

#[test]
fn same_second_provider_purchase_is_accepted_as_causal() {
    // The provider supplies integer seconds; a purchase in the dispatch second is causal.
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
    assert!(cash[..100].iter().all(|fact| fact.amount.is_zero()));
    assert_eq!(cash[100].amount.to_string(), "18.83");
    let sent = sent.borrow();
    assert!(matches!(&sent[0],Frame::Text(text) if text.contains("\"offset\":0")));
    assert!(
        matches!(&sent[1],Frame::Text(text) if text.contains("\"offset\":100")&&text.contains("\"date_to\":1789347055"))
    );
}

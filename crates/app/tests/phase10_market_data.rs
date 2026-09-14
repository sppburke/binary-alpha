mod common;

use binary_alpha_app::broker::deriv::{DerivAccounts, DerivAuthenticated, DerivMarketData};
use binary_alpha_app::broker::pocket_option::{PocketMarketData, provider_token, universal_micros};
use binary_alpha_app::broker::transport::{Connector, Frame, Http, Transport, WebSocketConnector};
use binary_alpha_app::broker::wire::WireDecimal;
use binary_alpha_app::broker::{
    Adapter, Cancellation, Clock, Continuity, HistoryPage, LiveEvent, MarketDataBroker, RateBudget,
    RateGroup,
};
use binary_alpha_app::store::Store;
use binary_alpha_app::{archive, broker, fetch, verify};
use binary_alpha_engine::config::{Broker, Config, DerivSettings, PocketSettings, RateBudgets};
use binary_alpha_engine::dataset::{
    Capability, DatasetRole, GenerationManifest, NativeGranularity, ObjectRole, SourceKind,
};
use binary_alpha_engine::execution::Decimal;
use binary_alpha_engine::market::{
    InstrumentId, PriceScale, Tick, format_event_time_micros as time_text,
    parse_event_time_micros as time,
};
use binary_alpha_engine::stream::{InstrumentStream, Observation, Source};
use common::Scratch;
use serde::{Deserialize, de::DeserializeOwned};
use serde_json::value::RawValue;
use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, VecDeque};
use std::fs;
use std::path::Path;
use std::rc::Rc;

fn hash(bytes: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    binary_alpha_engine::hex(&Sha256::digest(bytes))
}
fn fixture(name: &str) -> String {
    fs::read_to_string(
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/phase10")
            .join(name),
    )
    .unwrap()
    .trim_end()
    .to_string()
}
fn raw(text: &str) -> Box<RawValue> {
    RawValue::from_string(text.into()).unwrap()
}
fn field<T: DeserializeOwned>(map: &BTreeMap<String, Box<RawValue>>, key: &str) -> T {
    serde_json::from_str(map[key].get()).unwrap()
}
fn replace(text: &str, key: &str, value: &str) -> String {
    let mut fields: BTreeMap<String, Box<RawValue>> = serde_json::from_str(text).unwrap();
    fields.insert(key.into(), raw(value));
    serde_json::to_string(&fields).unwrap()
}
fn correlated(text: &str, req_id: u64) -> String {
    replace(text, "req_id", &req_id.to_string())
}
fn frame(name: &str, id: u64) -> Frame {
    Frame::Text(correlated(&fixture(name), id))
}
fn attachment(name: &str, payload: String) -> Vec<Frame> {
    vec![
        Frame::Text(format!(
            "451-[\"{name}\",{{\"_placeholder\":true,\"num\":0}}]"
        )),
        Frame::Binary(payload.into_bytes()),
    ]
}
fn handshake() -> Vec<Frame> {
    let mut frames = [
        "pocket-opening.txt",
        "pocket-connected.txt",
        "pocket-authenticated.txt",
        "pocket-account-class.txt",
    ]
    .map(|name| Frame::Text(fixture(name)))
    .to_vec();
    frames.extend(attachment("updateAssets", fixture("pocket-assets.json")));
    frames
}
#[derive(Clone, Default)]
struct FakeClock(Rc<Cell<i64>>);
impl FakeClock {
    fn at(value: i64) -> Self {
        Self(Rc::new(Cell::new(value)))
    }
}
impl Clock for FakeClock {
    fn now_micros(&self) -> i64 {
        self.0.get()
    }
    fn sleep(&mut self, micros: i64) {
        self.0.set(self.0.get() + micros);
    }
}
struct ScriptTransport {
    frames: VecDeque<Frame>,
    sent: Rc<RefCell<Vec<Frame>>>,
    clock: FakeClock,
}
impl Transport for ScriptTransport {
    fn send(&mut self, frame: Frame) -> Result<(), String> {
        self.sent.borrow_mut().push(frame);
        Ok(())
    }
    fn receive(&mut self, timeout: i64) -> Result<Option<Frame>, String> {
        let frame = self.frames.pop_front();
        self.clock.sleep(if frame.is_some() { 10 } else { timeout });
        Ok(frame)
    }
    fn close(&mut self) -> Result<(), String> {
        self.send(Frame::Close)
    }
}
struct ScriptConnector {
    sessions: VecDeque<Vec<Frame>>,
    sent: Rc<RefCell<Vec<Frame>>>,
    clock: FakeClock,
}
impl Connector for ScriptConnector {
    fn connect(&mut self, _: &str, _: &[(String, String)]) -> Result<Box<dyn Transport>, String> {
        Ok(Box::new(ScriptTransport {
            frames: self
                .sessions
                .pop_front()
                .ok_or("unexpected connection")?
                .into(),
            sent: Rc::clone(&self.sent),
            clock: self.clock.clone(),
        }))
    }
}
fn connector(
    sessions: Vec<Vec<Frame>>,
    clock: &FakeClock,
) -> (Box<dyn Connector>, Rc<RefCell<Vec<Frame>>>) {
    let sent = Rc::new(RefCell::new(Vec::new()));
    (
        Box::new(ScriptConnector {
            sessions: sessions.into(),
            sent: Rc::clone(&sent),
            clock: clock.clone(),
        }),
        sent,
    )
}
fn id(broker: &str, symbol: &str) -> InstrumentId {
    InstrumentId {
        broker: broker.to_string().try_into().unwrap(),
        provider_symbol: symbol.to_string().try_into().unwrap(),
    }
}
fn scale(digits: u8) -> PriceScale {
    digits.try_into().unwrap()
}
fn deriv_settings() -> DerivSettings {
    DerivSettings {
        id: "deriv".to_string().try_into().unwrap(),
        public_endpoint: "ws://127.0.0.1/public".into(),
        bootstrap_endpoint: "http://127.0.0.1/trading/v1/options".into(),
        app_id: "SYNTHETIC-APP".into(),
        credential: None,
        account_class: None,
        budgets: None,
    }
}
fn pocket_settings() -> PocketSettings {
    PocketSettings {
        id: "pocket_option".to_string().try_into().unwrap(),
        endpoint: "ws://127.0.0.1/socket.io/?EIO=4&transport=websocket".into(),
        origin: Some("https://example.invalid".into()),
        credential: "PHASE10_SYNTHETIC_AUTH".into(),
        account_class: binary_alpha_engine::config::AccountClass::Demo,
        server_offset_minutes: 120,
    }
}
fn pocket_ids() -> Vec<InstrumentId> {
    vec![
        id("pocket_option", "EURUSD_otc"),
        id("pocket_option", "#AAPL_otc"),
    ]
}
fn pocket(frames: Vec<Frame>) -> (PocketMarketData, Rc<RefCell<Vec<Frame>>>) {
    let clock = FakeClock::at(1_789_348_000_000_000);
    let (connector, sent) = connector(vec![frames], &clock);
    (
        PocketMarketData::connect(
            &pocket_settings(),
            &pocket_ids(),
            connector,
            Box::new(clock),
            "{\"synthetic\":true}".into(),
        )
        .unwrap(),
        sent,
    )
}
fn observation(event: Option<LiveEvent>) -> broker::LiveObservation {
    match event.unwrap() {
        LiveEvent::Observation(row) => row,
        _ => panic!("expected observation"),
    }
}

#[test]
fn exact_wire_numbers_and_socket_io_subset() {
    for token in [
        "18.83",
        "92.0409",
        "19.54",
        "91.9315",
        "0.100000000000000001",
    ] {
        for encoded in [token.to_string(), format!("\"{token}\"")] {
            let wire: WireDecimal = serde_json::from_str(&encoded).unwrap();
            assert_eq!(wire.decimal().unwrap(), Decimal::parse(token).unwrap());
            assert_eq!(
                serde_json::to_string(&WireDecimal::from_decimal(wire.decimal().unwrap())).unwrap(),
                token
            );
        }
    }
    let precise: WireDecimal = serde_json::from_str("0.100000000000000001").unwrap();
    assert_eq!(
        precise.price_units(scale(18)).unwrap(),
        100_000_000_000_000_001
    );
    let escaped: WireDecimal = serde_json::from_str(r#""18.8\u0033""#).unwrap();
    assert_eq!(escaped.decimal().unwrap(), Decimal::parse("18.83").unwrap());
    for invalid in ["1e3", "null", "true", "\"1e3\""] {
        assert!(
            serde_json::from_str::<WireDecimal>(invalid)
                .unwrap()
                .decimal()
                .is_err()
        );
    }
    let wire: WireDecimal = serde_json::from_str("1789354492.749").unwrap();
    assert_eq!(universal_micros(&wire, 120).unwrap(), 1_789_347_292_749_000);
    assert_eq!(
        provider_token(1_789_347_292_749_000, 120).unwrap().0.get(),
        "1789354492.749"
    );
    assert!(universal_micros(&serde_json::from_str("1789354492.7490001").unwrap(), 120).is_err());
    use broker::socket_io::{Packet, decode, encode_event};
    assert!(matches!(
        decode(&fixture("pocket-opening.txt")).unwrap(),
        Packet::Open
    ));
    assert!(matches!(decode("40{}").unwrap(), Packet::Connected));
    assert!(matches!(decode("2").unwrap(), Packet::Ping));
    assert!(matches!(
        decode("42[\"successauth\"]").unwrap(),
        Packet::Event { .. }
    ));
    assert!(matches!(
        decode("451-[\"updateStream\",{\"_placeholder\":true,\"num\":0}]").unwrap(),
        Packet::BinaryHeader { .. }
    ));
    assert_eq!(
        encode_event("subscribeSymbol", "\"EURUSD_otc\""),
        "42[\"subscribeSymbol\",\"EURUSD_otc\"]"
    );
    for bad in [
        "43[]",
        "42[]",
        "42[1]",
        "452-[]",
        "451-[\"x\",{\"_placeholder\":true,\"num\":1}]",
    ] {
        assert!(decode(bad).is_err(), "{bad}");
    }
}

#[test]
fn deriv_market_correlation_precision_duplicates_cancellation_and_reconnect() {
    let clock = FakeClock::at(1_789_348_000_000_000);
    let first = vec![
        frame("deriv-rate-limit.json", 1),
        frame("deriv-contracts_for-R_50.json", 2),
        frame("deriv-history-R_50.json", 3),
        frame("deriv-tick-R_50.json", 4),
        frame("deriv-tick-R_100.json", 5),
        frame("deriv-tick-R_50.json", 4),
        frame("deriv-forget.json", 6),
    ];
    let second = vec![frame("deriv-tick-R_50.json", 1)];
    let (connector, sent) = connector(vec![first, second], &clock);
    let mut broker =
        DerivMarketData::connect(&deriv_settings(), connector, Box::new(clock)).unwrap();
    assert!(broker.discover().unwrap_err().contains("RateLimit"));
    let contracts = broker.contracts_for("R_50").unwrap();
    assert!(
        contracts
            .iter()
            .any(|contract| contract.contract_type == "CALL")
    );
    assert!(
        contracts
            .iter()
            .any(|contract| contract.contract_type == "PUT")
    );
    assert!(
        contracts
            .iter()
            .all(|contract| contract.barriers.decimal().unwrap() == Decimal::parse("1").unwrap())
    );
    let page = broker
        .history_page(&id("deriv", "R_50"), scale(4), None)
        .unwrap();
    assert_eq!(page.rows.len(), 100);
    assert_eq!(page.rows[0].price_units, 920242);
    broker.subscribe(&id("deriv", "R_50"), scale(4)).unwrap();
    broker.subscribe(&id("deriv", "R_100"), scale(2)).unwrap();
    let first = observation(broker.next_live(100).unwrap());
    let second = observation(broker.next_live(100).unwrap());
    let duplicate = observation(broker.next_live(100).unwrap());
    assert_eq!(first.price_units, duplicate.price_units);
    assert_eq!(first.provider_time_micros, duplicate.provider_time_micros);
    assert_eq!(
        (first.sequence, second.sequence, duplicate.sequence),
        (1, 2, 3)
    );
    assert_ne!(first.receipt_micros, first.provider_time_micros);
    let mut consumer = Continuity::default();
    consumer.accept(&first).unwrap();
    assert!(consumer.accept(&duplicate).unwrap_err().contains("loss"));
    consumer.accept(&second).unwrap();
    consumer.accept(&duplicate).unwrap();
    assert_eq!(
        broker.unsubscribe(&id("deriv", "R_100")).unwrap(),
        Cancellation::Acknowledged
    );
    broker.reconnect().unwrap();
    assert!(matches!(
        broker.next_live(100).unwrap(),
        Some(LiveEvent::Break { generation: 1, .. })
    ));
    broker.subscribe(&id("deriv", "R_50"), scale(4)).unwrap();
    let row = observation(broker.next_live(100).unwrap());
    assert_eq!((row.generation, row.sequence), (1, 1));
    consumer.reconnect().unwrap();
    consumer.accept(&row).unwrap();
    let requests = sent.borrow();
    assert!(requests.iter().any(|f| matches!(f, Frame::Text(t) if t.contains("\"count\":100") && t.contains("\"end\":\"latest\""))));
}

#[test]
fn deriv_rejects_malformed_missing_fields_wrong_types_and_wrong_request_ids() {
    let base = fixture("deriv-history-R_50.json");
    let mut missing: BTreeMap<String, Box<RawValue>> = serde_json::from_str(&base).unwrap();
    missing.remove("msg_type");
    let cases = vec![
        (replace(&base, "pip_size", "5"), "pip_size"),
        ("{not json".into(), "malformed"),
        (serde_json::to_string(&missing).unwrap(), "missing field"),
        (
            replace(&base, "history", "{\"prices\":[\"18.83\"],\"times\":[1]}"),
            "numeric token",
        ),
        (
            replace(&base, "msg_type", "\"unknown\""),
            "unexpected msg_type",
        ),
    ];
    for (body, expected) in cases {
        let clock = FakeClock::default();
        let body = if body.starts_with("{not") {
            body
        } else {
            correlated(&body, 1)
        };
        let (connector, _) = connector(vec![vec![Frame::Text(body)]], &clock);
        let mut broker =
            DerivMarketData::connect(&deriv_settings(), connector, Box::new(clock)).unwrap();
        assert!(
            broker
                .history_page(&id("deriv", "R_50"), scale(4), None)
                .unwrap_err()
                .contains(expected),
            "{expected}"
        );
    }
    let clock = FakeClock::default();
    let (connector, _) = connector(vec![vec![frame("deriv-history-R_50.json", 9)]], &clock);
    let mut broker =
        DerivMarketData::connect(&deriv_settings(), connector, Box::new(clock)).unwrap();
    assert!(
        broker
            .history_page(&id("deriv", "R_50"), scale(4), None)
            .unwrap_err()
            .contains("req_id mismatch")
    );
}

#[test]
fn rate_windows_and_lower_configuration_limits_are_enforced() {
    let mut clock = FakeClock::default();
    let mut budget = RateBudget::new(RateBudgets::default()).unwrap();
    for _ in 0..220 {
        budget.admit(RateGroup::Other, &mut clock);
    }
    assert_eq!(clock.now_micros(), 0);
    budget.admit(RateGroup::Other, &mut clock);
    assert_eq!(clock.now_micros(), 60_000_000);
    let mut limits = RateBudgets::default();
    limits.other.per_minute = 2;
    limits.other.per_hour = 3;
    let mut budget = RateBudget::new(limits.clone()).unwrap();
    let mut clock = FakeClock::default();
    for _ in 0..3 {
        budget.admit(RateGroup::Other, &mut clock);
    }
    assert_eq!(clock.now_micros(), 60_000_000);
    budget.admit(RateGroup::Other, &mut clock);
    assert_eq!(clock.now_micros(), 3_600_000_000);
    limits.other.per_minute = 221;
    assert!(limits.validate().unwrap_err().contains("budgets.other"));
    let scratch = Scratch::new("phase10_budget_config");
    let mut config = test_config(&scratch, "deriv", "ws://127.0.0.1/public", false);
    if let Broker::Deriv(settings) = &mut config.brokers[0] {
        settings.budgets = Some(limits);
    }
    assert!(
        Config::parse(&config.canonical_toml())
            .unwrap_err()
            .to_string()
            .contains("budgets.other")
    );
}

#[test]
fn pocket_retained_paging_live_cancellation_and_heartbeats() {
    let mut frames = handshake();
    frames.push(Frame::Text("2".into()));
    frames.push(Frame::Ping(vec![7, 8]));
    frames.extend(attachment(
        "updateHistoryNewFast",
        fixture("pocket-history-initial.json"),
    ));
    frames.extend(attachment(
        "loadHistoryPeriod",
        replace(&fixture("pocket-history-older-1.json"), "index", "1"),
    ));
    frames.extend(attachment(
        "loadHistoryPeriod",
        replace(&fixture("pocket-history-older-2.json"), "index", "2"),
    ));
    for line in [6, 7, 10, 11, 13, 14, 15, 16].into_iter().chain(18..=36) {
        frames.extend(attachment(
            "updateStream",
            fixture(&format!("pocket-live-{line:02}.json")),
        ));
    }
    let (mut broker, sent) = pocket(frames);
    assert_eq!(broker.discover().unwrap().len(), 2);
    let instrument = id("pocket_option", "EURUSD_otc");
    let initial = broker.history_page(&instrument, scale(5), None).unwrap();
    assert_eq!(initial.rows.len(), 1478);
    assert_eq!(initial.rows[0].event_time_micros, 1_789_347_292_749_000);
    let older = broker
        .history_page(&instrument, scale(5), initial.first_micros)
        .unwrap();
    assert_eq!(older.rows.len(), 413);
    assert_eq!(older.anchor_token.as_deref(), Some("1789354492.749"));
    let oldest = broker
        .history_page(&instrument, scale(5), older.first_micros)
        .unwrap();
    assert_eq!(oldest.rows.len(), 409);
    let rows = fetch::prepend_page(oldest.rows, fetch::prepend_page(older.rows, initial.rows));
    assert_eq!(rows.len(), 2297);
    assert!(
        rows.windows(2)
            .all(|w| w[0].event_time_micros <= w[1].event_time_micros)
    );
    for (instrument, digits) in pocket_ids().iter().zip([5, 3]) {
        broker.subscribe(instrument, scale(digits)).unwrap();
    }
    let mut counts = BTreeMap::<String, u64>::new();
    let mut continuity = Continuity::default();
    for _ in 0..8 {
        let row = observation(broker.next_live(1_000_000).unwrap());
        continuity.accept(&row).unwrap();
        *counts
            .entry(row.instrument.provider_symbol.to_string())
            .or_default() += 1;
    }
    assert_eq!(counts.values().copied().collect::<Vec<_>>(), [4, 4]);
    assert_eq!(
        broker.unsubscribe(&pocket_ids()[1]).unwrap(),
        Cancellation::SentWithoutAcknowledgement
    );
    for _ in 0..19 {
        let row = observation(broker.next_live(10_000_000).unwrap());
        continuity.accept(&row).unwrap();
        assert_eq!(row.instrument, instrument);
    }
    assert!(broker.next_live(10_000_000).unwrap().is_none());
    assert_eq!(broker.received_counts()["#AAPL_otc"], 4);
    assert_eq!(broker.received_counts()["EURUSD_otc"], 23);
    assert!(sent.borrow().contains(&Frame::Text("3".into())));
    assert!(sent.borrow().contains(&Frame::Pong(vec![7, 8])));
    assert!(
        sent.borrow()
            .iter()
            .any(|f| matches!(f,Frame::Text(t) if t.contains("\"time\":1789354492.749")))
    );
    assert!(sent.borrow().contains(&Frame::Text(
        "42[\"unSubscribeSymbol\",\"#AAPL_otc\"]".into()
    )));
}

#[test]
fn pocket_rejects_class_membership_and_incomplete_attachments() {
    for (frames, expected) in [
        (
            {
                let mut f = handshake();
                f[3] = Frame::Text(
                    "42[\"successupdateBalance\",{\"isDemo\":0,\"synthetic\":true}]".into(),
                );
                f
            },
            "account-class mismatch",
        ),
        (
            {
                let mut f = handshake();
                *f.last_mut().unwrap() = Frame::Binary(b"[]".to_vec());
                f
            },
            "missing from updateAssets",
        ),
    ] {
        let clock = FakeClock::default();
        let (connector, _) = connector(vec![frames], &clock);
        let error = PocketMarketData::connect(
            &pocket_settings(),
            &pocket_ids(),
            connector,
            Box::new(clock),
            "{}".into(),
        )
        .err()
        .unwrap();
        assert!(error.contains(expected), "{error}");
    }
    // Synthetic faults assert that no incomplete payload becomes an observation.
    for (fault, expected) in [
        (
            vec![
                Frame::Text("451-[\"updateStream\",{\"_placeholder\":true,\"num\":0}]".into()),
                Frame::Close,
            ],
            "incomplete binary attachment",
        ),
        (vec![Frame::Binary(b"[]".to_vec())], "without header"),
        (
            vec![
                Frame::Text("451-[\"updateStream\",{\"_placeholder\":true,\"num\":0}]".into()),
                Frame::Text("42[\"updateStream\",[]]".into()),
            ],
            "interrupted binary attachment",
        ),
        (
            attachment("updateStream", "[[\"EURUSD_otc\",1,2,3]]".into()),
            "live row",
        ),
    ] {
        let mut frames = handshake();
        frames.extend(fault);
        let (mut broker, _) = pocket(frames);
        broker.subscribe(&pocket_ids()[0], scale(5)).unwrap();
        assert!(broker.next_live(1000).unwrap_err().contains(expected));
    }
}

fn instrument_text(broker: &str, symbol: &str, digits: u8, currency: &str) -> String {
    format!(
        "\n[[instruments]]\nbroker = \"{broker}\"\nprovider_symbol = \"{symbol}\"\nquote_currency = \"{currency}\"\nprice_scale = {digits}\nnative_granularity = {{ kind = \"tick\" }}\ngap = {{ max_seconds = 1, reopen_seconds = 10 }}\ncandles = [{{ duration_seconds = 1, offset_seconds = 0, min_observations = 3 }}]\n"
    )
}
fn test_config(scratch: &Scratch, kind: &str, endpoint: &str, two: bool) -> Config {
    let mut broker = if kind == "deriv" {
        Broker::Deriv(deriv_settings())
    } else {
        Broker::PocketOption(pocket_settings())
    };
    match &mut broker {
        Broker::Deriv(s) => s.public_endpoint = endpoint.into(),
        Broker::PocketOption(s) => s.endpoint = endpoint.into(),
    }
    let symbols = if kind == "deriv" {
        [("R_50", 4, "USD"), ("R_100", 2, "EUR")]
    } else {
        [("EURUSD_otc", 5, "USD"), ("#AAPL_otc", 3, "EUR")]
    };
    let text = format!(
        "schema_version = 1\nrun_mode = \"research\"\n[storage]\nhistorical_data_dir = \"{}\"\npublication_uri = \"file://{}\"\n{}{}\n",
        scratch.path("retained").display(),
        scratch.path("published").display(),
        instrument_text(kind, symbols[0].0, symbols[0].1, symbols[0].2),
        if two {
            instrument_text(kind, symbols[1].0, symbols[1].1, symbols[1].2)
        } else {
            String::new()
        }
    );
    let mut config = Config::parse(&text).unwrap();
    config.brokers.push(broker);
    config.history = Some(binary_alpha_engine::config::History {
        broker: kind.to_string().try_into().unwrap(),
        instruments: symbols[..if two { 2 } else { 1 }]
            .iter()
            .map(|s| s.0.to_string().try_into().unwrap())
            .collect(),
        role: DatasetRole::Development,
        start: time_text(0),
        end: time_text(10_000_000),
        refresh_interval_seconds: None,
    });
    config.inspect = Some(binary_alpha_engine::config::Inspect {
        live_observations: 2,
        live_seconds: 1,
        proposal: None,
    });
    Config::parse(&config.canonical_toml()).unwrap()
}

struct HttpCall {
    method: String,
    url: String,
    headers: Vec<(String, String)>,
}
struct FakeHttp {
    responses: VecDeque<Vec<u8>>,
    calls: Vec<HttpCall>,
}
impl Http for FakeHttp {
    fn get_json(&mut self, url: &str, headers: &[(String, String)]) -> Result<Vec<u8>, String> {
        self.calls.push(HttpCall {
            method: "GET".into(),
            url: url.into(),
            headers: headers.to_vec(),
        });
        self.responses
            .pop_front()
            .ok_or("unexpected HTTP request".into())
    }
    fn post_json(&mut self, url: &str, headers: &[(String, String)]) -> Result<Vec<u8>, String> {
        self.calls.push(HttpCall {
            method: "POST".into(),
            url: url.into(),
            headers: headers.to_vec(),
        });
        self.responses
            .pop_front()
            .ok_or("unexpected HTTP request".into())
    }
}
#[test]
fn authenticated_bootstrap_and_balance_are_exact_class_bound_and_non_purchasing() {
    // Synthetic accounts and OTP address: no retained account or subscription identifier is copied.
    let accounts=br#"{"data":[{"account_id":"DOT000001","account_type":"demo","status":"active","currency":"USD"}]}"#.to_vec();
    let address =
        br#"{"data":{"url":"ws://127.0.0.1/trading/v1/options/ws/demo?otp=synthetic"}}"#.to_vec();
    let mut settings = deriv_settings();
    settings.account_class = Some(binary_alpha_engine::config::AccountClass::Demo);
    settings.credential = Some("SYNTHETIC_TOKEN_REFERENCE".into());
    let mut http = FakeHttp {
        responses: vec![accounts.clone(), address.clone()].into(),
        calls: Vec::new(),
    };
    let authenticated = DerivAccounts::bootstrap(&settings, &mut http, "synthetic-token").unwrap();
    assert_eq!(authenticated.currency.as_str(), "USD");
    assert_eq!(http.calls[0].method, "GET");
    assert!(http.calls[0].url.ends_with("/accounts"));
    assert_eq!(http.calls[1].method, "POST");
    assert!(http.calls[1].url.ends_with("/accounts/DOT000001/otp"));
    assert!(http.calls.iter().all(|call| {
        call.headers
            .contains(&("Authorization".into(), "Bearer synthetic-token".into()))
    }));
    let clock = FakeClock::default();
    let balance=Frame::Text(r#"{"msg_type":"balance","req_id":1,"balance":{"balance":18.83,"currency":"USD","loginid":"DOT000001"},"synthetic":true}"#.into());
    let (connector, sent) = connector(vec![vec![balance]], &clock);
    let mut broker = DerivAuthenticated::connect(
        authenticated,
        connector,
        Box::new(clock),
        RateBudgets::default(),
    )
    .unwrap();
    assert_eq!(
        broker.balance().unwrap().amount,
        Decimal::parse("18.83").unwrap()
    );
    assert_eq!(sent.borrow().len(), 1);
    assert!(
        matches!(&sent.borrow()[0],Frame::Text(t) if t.contains("\"balance\":1") && !t.contains("buy"))
    );
    for invalid in [
        r#"{"data":{"url":"ws://127.0.0.1/trading/v1/options/ws/real?otp=synthetic"}}"#,
        r#"{"data":{"url":"ws://secret@127.0.0.1/trading/v1/options/ws/demo?otp=synthetic"}}"#,
    ] {
        let mut http = FakeHttp {
            responses: vec![accounts.clone(), invalid.as_bytes().to_vec()].into(),
            calls: Vec::new(),
        };
        let error = DerivAccounts::bootstrap(&settings, &mut http, "synthetic-token")
            .err()
            .unwrap();
        assert!(error.contains("authenticated address"));
        assert!(
            !error.contains("synthetic-token")
                && !error.contains("otp=")
                && !error.contains("secret@")
        );
    }
    let mut http = FakeHttp {
        responses: vec![br#"{"data":[]}"#.to_vec()].into(),
        calls: Vec::new(),
    };
    assert!(
        DerivAccounts::bootstrap(&settings, &mut http, "synthetic")
            .err()
            .unwrap()
            .contains("no active account")
    );
    assert_eq!(http.calls.len(), 1);
}

fn shifted_tick(name: &str, req_id: u64, seconds: i64) -> Frame {
    let text = fixture(name);
    let fields: BTreeMap<String, Box<RawValue>> = serde_json::from_str(&text).unwrap();
    let tick: BTreeMap<String, Box<RawValue>> = field(&fields, "tick");
    let epoch: i64 = field(&tick, "epoch");
    let shifted = replace(
        fields["tick"].get(),
        "epoch",
        &(epoch + seconds).to_string(),
    );
    Frame::Text(correlated(&replace(&text, "tick", &shifted), req_id))
}
fn stream_source(scale: PriceScale) -> Source {
    Source {
        generation: "synthetic-live-normalization".into(),
        source_kind: SourceKind::BrokerHistory,
        role: DatasetRole::Development,
        native_granularity: NativeGranularity::Tick,
        price_scale: Some(scale),
        capabilities: vec![Capability::Ticks],
    }
}
#[test]
fn both_live_adapters_feed_the_shared_stream_and_replay_identically() {
    for kind in ["deriv", "pocket_option"] {
        let scratch = Scratch::new(&format!("phase10_live_stream_{kind}"));
        let config = test_config(&scratch, kind, "ws://127.0.0.1/", true);
        let clock = FakeClock::at(1_789_348_010_000_000);
        let mut adapter = if kind == "deriv" {
            let mut frames = vec![
                shifted_tick("deriv-tick-R_50.json", 1, 0),
                shifted_tick("deriv-tick-R_100.json", 2, 0),
            ];
            for seconds in [2, 20] {
                frames.push(shifted_tick("deriv-tick-R_50.json", 1, seconds));
                frames.push(shifted_tick("deriv-tick-R_100.json", 2, seconds));
            }
            let (connector, _) = connector(vec![frames], &clock);
            Adapter::Deriv(
                DerivMarketData::connect(&deriv_settings(), connector, Box::new(clock)).unwrap(),
            )
        } else {
            let mut frames = handshake();
            for line in [
                6, 7, 10, 11, 13, 14, 15, 16, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30,
                31, 32, 33, 34, 35, 36,
            ] {
                frames.extend(attachment(
                    "updateStream",
                    fixture(&format!("pocket-live-{line:02}.json")),
                ));
            }
            Adapter::PocketOption(pocket(frames).0)
        };
        for definition in &config.instruments {
            adapter
                .market()
                .subscribe(&definition.id(), definition.price_scale)
                .unwrap();
        }
        let mut observations = Vec::new();
        let mut continuity = Continuity::default();
        while let Some(event) = adapter.market().next_live(1_000_000).unwrap() {
            let row = observation(Some(event));
            continuity.accept(&row).unwrap();
            assert_ne!(row.receipt_micros, row.provider_time_micros);
            observations.push(row);
        }
        for definition in &config.instruments {
            let replay = || {
                let mut stream =
                    InstrumentStream::new(definition, stream_source(definition.price_scale))
                        .unwrap();
                let mut candles = Vec::new();
                for row in observations
                    .iter()
                    .filter(|row| row.instrument == definition.id())
                {
                    stream
                        .push(
                            Observation::Tick(Tick {
                                event_time_micros: row.provider_time_micros,
                                price_units: row.price_units,
                            }),
                            &mut candles,
                        )
                        .unwrap();
                }
                candles
            };
            let first = replay();
            assert!(!first.is_empty(), "{}", definition.id());
            assert_eq!(first, replay());
            assert!(first.iter().any(|(_, candle)| candle.flags.low_activity));
            if kind == "deriv" {
                assert!(
                    first
                        .iter()
                        .any(|(_, candle)| candle.flags.gap_before || candle.flags.gap_inside)
                );
            }
        }
    }
}

struct Pages {
    pages: VecDeque<Result<HistoryPage, String>>,
    anchors: Vec<Option<i64>>,
    continuity: Continuity,
}
impl Pages {
    fn new(pages: Vec<HistoryPage>) -> Self {
        Self {
            pages: pages.into_iter().map(Ok).collect(),
            anchors: Vec::new(),
            continuity: Continuity::default(),
        }
    }
}
impl MarketDataBroker for Pages {
    fn discover(&mut self) -> Result<Vec<broker::DiscoveredInstrument>, String> {
        Ok(Vec::new())
    }
    fn history_page(
        &mut self,
        _: &InstrumentId,
        _: PriceScale,
        before: Option<i64>,
    ) -> Result<HistoryPage, String> {
        self.anchors.push(before);
        self.pages.pop_front().ok_or("unexpected page request")?
    }
    fn subscribe(&mut self, _: &InstrumentId, _: PriceScale) -> Result<(), String> {
        Err("unused subscribe".into())
    }
    fn next_live(&mut self, _: i64) -> Result<Option<LiveEvent>, String> {
        Ok(None)
    }
    fn unsubscribe(&mut self, _: &InstrumentId) -> Result<Cancellation, String> {
        Err("unused unsubscribe".into())
    }
    fn reconnect(&mut self) -> Result<(), String> {
        Err("unused reconnect".into())
    }
    fn continuity(&self) -> &Continuity {
        &self.continuity
    }
}
fn page(rows: &[(i64, i64)]) -> HistoryPage {
    // Synthetic deterministic provider rows, with integer prices already normalized by the fake adapter.
    let raw = serde_json::to_vec(&rows).unwrap();
    HistoryPage {
        raw_name: hash(&raw),
        raw,
        anchor: None,
        anchor_token: None,
        first_micros: rows.first().map(|r| r.0 * 1_000_000),
        last_micros: rows.last().map(|r| r.0 * 1_000_000),
        rows: rows
            .iter()
            .map(|&(t, p)| Tick {
                event_time_micros: t * 1_000_000,
                price_units: p,
            })
            .collect(),
    }
}
fn range_page(start: i64, end: i64) -> HistoryPage {
    page(&(start..end).map(|i| (i, 100_000 + i)).collect::<Vec<_>>())
}
fn stores(scratch: &Scratch) -> (Store, Store) {
    (
        Store::filesystem(scratch.path("retained")),
        Store::filesystem(scratch.path("published")),
    )
}
fn read_coverage(scratch: &Scratch, manifest: &GenerationManifest) -> fetch::HistoryCoverage {
    let object = manifest
        .objects
        .iter()
        .find(|object| object.path == fetch::COVERAGE_PATH)
        .unwrap();
    serde_json::from_slice(&fs::read(scratch.path("published").join(&object.key)).unwrap()).unwrap()
}
fn read_manifests(scratch: &Scratch) -> Vec<GenerationManifest> {
    if !scratch.path("published/manifests").exists() {
        return Vec::new();
    }
    let mut manifests: Vec<_> = scratch
        .manifests("published")
        .iter()
        .map(|path| GenerationManifest::from_json(&fs::read(path).unwrap()).unwrap())
        .collect();
    manifests.sort_by_key(|manifest| manifest.row_count);
    manifests
}

#[test]
fn fetch_refresh_overlap_no_new_data_verification_and_phase03_audit() {
    let scratch = Scratch::new("phase10_fetch_refresh");
    let mut config = test_config(&scratch, "deriv", "ws://127.0.0.1/", false);
    config.history.as_mut().unwrap().refresh_interval_seconds = Some(10);
    let (local, destination) = stores(&scratch);
    let mut pages = Pages::new(vec![
        range_page(5, 10),
        range_page(0, 6),
        range_page(5, 20),
        range_page(15, 30),
    ]);
    let mut clock = FakeClock::at(10_000_000);
    let mut out = Vec::new();
    fetch::passes(
        &config,
        &mut pages,
        &local,
        &destination,
        &mut clock,
        fetch::PassLimit::Exactly(3),
        &mut out,
    )
    .unwrap();
    assert_eq!(clock.now_micros(), 30_000_000);
    assert!(pages.pages.is_empty());
    assert_eq!(pages.anchors, [None, Some(5_000_000), None, None]);
    let manifests = read_manifests(&scratch);
    assert_eq!(
        manifests.iter().map(|m| m.row_count).collect::<Vec<_>>(),
        [10, 20, 30]
    );
    for (index, manifest) in manifests.iter().enumerate() {
        let coverage = read_coverage(&scratch, manifest);
        assert_eq!(
            coverage.verified,
            Some(fetch::Range {
                start: time_text(0),
                end: time_text((index as i64 + 1) * 10_000_000)
            })
        );
        assert!(coverage.shortfall.is_none());
        assert_eq!(coverage.rows, manifest.row_count);
        assert!(
            verify::run(&destination.uri(&manifest.key()))
                .unwrap()
                .contains("verified")
        );
        for object in &manifest.objects {
            assert!(local.local_path(&object.key).unwrap().is_file());
        }
    }
    let before = scratch.objects("published");
    let mut none = Pages::new(vec![range_page(0, 30)]);
    fetch::pass(
        &config,
        &mut none,
        &local,
        &destination,
        &mut clock,
        (0, 40_000_000),
        &mut out,
    )
    .unwrap();
    assert_eq!(read_manifests(&scratch).len(), 3);
    assert_eq!(before, scratch.objects("published"));
    assert!(String::from_utf8(out).unwrap().contains("(no new data)"));
    let config_path = scratch.path("audit.toml");
    fs::write(&config_path, config.canonical_toml()).unwrap();
    let mut report = Vec::new();
    binary_alpha_app::audit::run(
        &config_path,
        &destination.uri(&manifests[2].key()),
        &mut report,
    )
    .unwrap();
    assert!(String::from_utf8(report).unwrap().contains("candles"));
}

#[test]
fn fetch_shortfalls_never_skip_the_unverified_prefix() {
    for reason in ["empty_page", "no_progress"] {
        let scratch = Scratch::new(&format!("phase10_shortfall_{reason}"));
        let config = test_config(&scratch, "deriv", "ws://127.0.0.1/", false);
        let (local, destination) = stores(&scratch);
        let second = if reason == "empty_page" {
            page(&[])
        } else {
            range_page(5, 8)
        };
        let mut pages = Pages::new(vec![range_page(5, 8), second]);
        let mut clock = FakeClock::default();
        let mut out = Vec::new();
        fetch::pass(
            &config,
            &mut pages,
            &local,
            &destination,
            &mut clock,
            (0, 10_000_000),
            &mut out,
        )
        .unwrap();
        let manifests = read_manifests(&scratch);
        assert_eq!(manifests.len(), 1);
        let coverage = read_coverage(&scratch, &manifests[0]);
        assert_eq!(
            coverage.verified.as_ref().unwrap().start,
            time_text(5_000_000)
        );
        assert_eq!(coverage.shortfall.as_ref().unwrap().reason, reason);
        assert_eq!(
            coverage.shortfall.as_ref().unwrap().unresolved.end,
            time_text(5_000_000)
        );
        let mut repair = Pages::new(vec![range_page(0, 10)]);
        fetch::pass(
            &config,
            &mut repair,
            &local,
            &destination,
            &mut clock,
            (0, 10_000_000),
            &mut out,
        )
        .unwrap();
        assert_eq!(repair.anchors, [None]);
        let repaired = read_manifests(&scratch);
        assert_eq!(repaired.last().unwrap().row_count, 10);
        assert!(
            read_coverage(&scratch, repaired.last().unwrap())
                .shortfall
                .is_none()
        );
    }
    let scratch = Scratch::new("phase10_empty_first");
    let config = test_config(&scratch, "deriv", "ws://127.0.0.1/", false);
    let (local, destination) = stores(&scratch);
    let mut out = Vec::new();
    fetch::pass(
        &config,
        &mut Pages::new(vec![page(&[])]),
        &local,
        &destination,
        &mut FakeClock::default(),
        (0, 10_000_000),
        &mut out,
    )
    .unwrap();
    assert!(
        String::from_utf8(out)
            .unwrap()
            .contains("shortfall empty_page")
    );
    assert!(read_manifests(&scratch).is_empty());
    assert!(scratch.objects("retained").iter().any(|key| {
        fs::read(scratch.path("retained/objects").join(key))
            .ok()
            .and_then(|b| serde_json::from_slice::<fetch::HistoryCoverage>(&b).ok())
            .is_some_and(|c| c.verified.is_none() && c.shortfall.is_some())
    }));
}

#[test]
fn fetch_conflicts_provider_errors_and_interrupted_publication_do_not_advance() {
    let scratch = Scratch::new("phase10_fetch_failures");
    let config = test_config(&scratch, "deriv", "ws://127.0.0.1/", false);
    let (local, destination) = stores(&scratch);
    let mut clock = FakeClock::default();
    let mut out = Vec::new();
    let mut conflict = Pages::new(vec![
        page(&[(5, 100), (6, 101)]),
        page(&[(0, 90), (5, 999)]),
    ]);
    assert!(
        fetch::pass(
            &config,
            &mut conflict,
            &local,
            &destination,
            &mut clock,
            (0, 10_000_000),
            &mut out
        )
        .unwrap_err()
        .contains("conflicting")
    );
    assert!(read_manifests(&scratch).is_empty());
    let mut failure = Pages::new(vec![range_page(5, 10)]);
    failure
        .pages
        .push_back(Err("synthetic broker failure".into()));
    assert!(
        fetch::pass(
            &config,
            &mut failure,
            &local,
            &destination,
            &mut clock,
            (0, 10_000_000),
            &mut out
        )
        .unwrap_err()
        .contains("synthetic broker failure")
    );
    assert!(read_manifests(&scratch).is_empty());
    let first = range_page(5, 10);
    let second = range_page(0, 6);
    let failure_path = scratch.path("published/objects").join(hash(&second.raw));
    fs::create_dir_all(&failure_path).unwrap();
    assert!(
        fetch::pass(
            &config,
            &mut Pages::new(vec![first.clone(), second.clone()]),
            &local,
            &destination,
            &mut clock,
            (0, 10_000_000),
            &mut out
        )
        .is_err()
    );
    assert!(
        scratch
            .path("published/objects")
            .join(hash(&first.raw))
            .is_file()
    );
    assert!(read_manifests(&scratch).is_empty());
    fs::remove_dir(&failure_path).unwrap();
    fetch::pass(
        &config,
        &mut Pages::new(vec![first, second]),
        &local,
        &destination,
        &mut clock,
        (0, 10_000_000),
        &mut out,
    )
    .unwrap();
    assert!(String::from_utf8(out).unwrap().contains("reused 1"));
    assert_eq!(read_manifests(&scratch).len(), 1);
    assert!(
        verify::run(&destination.uri(&read_manifests(&scratch)[0].key()))
            .unwrap()
            .contains("rows 10")
    );
}

fn loopback_listener() -> (std::net::TcpListener, String) {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    listener.set_nonblocking(true).unwrap();
    (listener, format!("ws://{address}/"))
}
fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
}

#[test]
fn real_websocket_transport_round_trips_and_redacts_failed_addresses() {
    use futures_util::{SinkExt, StreamExt};
    use tokio_tungstenite::tungstenite::Message;
    let (listener, url) = loopback_listener();
    let server = std::thread::spawn(move || {
        runtime().block_on(async move {
            let listener = tokio::net::TcpListener::from_std(listener).unwrap();
            let (socket, _) = listener.accept().await.unwrap();
            let mut socket = tokio_tungstenite::accept_async(socket).await.unwrap();
            for _ in 0..3 {
                let frame = socket.next().await.unwrap().unwrap();
                let echoed = match frame {
                    Message::Ping(payload) => Message::Pong(payload),
                    other => other,
                };
                socket.send(echoed).await.unwrap();
            }
        })
    });
    let mut connector = WebSocketConnector::new().unwrap();
    let mut transport = connector.connect(&url, &[]).unwrap();
    for (sent, received) in [
        (
            Frame::Text("synthetic text".into()),
            Frame::Text("synthetic text".into()),
        ),
        (Frame::Binary(vec![1, 2, 3]), Frame::Binary(vec![1, 2, 3])),
        (Frame::Ping(vec![4, 5]), Frame::Pong(vec![4, 5])),
    ] {
        transport.send(sent).unwrap();
        assert_eq!(transport.receive(1_000_000).unwrap(), Some(received));
    }
    server.join().unwrap();
    let (listener, closed) = loopback_listener();
    drop(listener);
    let error = connector
        .connect(
            &format!("{closed}?otp=SYNTHETIC-SECRET"),
            &[("Authorization".into(), "SYNTHETIC-HEADER".into())],
        )
        .err()
        .unwrap();
    assert!(error.contains("127.0.0.1"));
    assert!(!error.contains("otp") && !error.contains("SYNTHETIC") && !error.contains(&closed));
}

fn synthetic_pocket_second_history() -> String {
    r##"{"asset":"#AAPL_otc","period":1,"history":[[1789354492.749,181.343],[1789354493.749,181.344],[1789354494.749,181.345]],"synthetic":true}"##.into()
}
fn serve_broker(kind: &'static str) -> (String, std::thread::JoinHandle<()>) {
    use futures_util::{SinkExt, StreamExt};
    use tokio_tungstenite::tungstenite::Message;
    let (listener, url) = loopback_listener();
    let server = std::thread::spawn(move || {
        runtime().block_on(async move {
            let listener = tokio::net::TcpListener::from_std(listener).unwrap();
            // Two fetch invocations (the second reuses the committed range), then one inspection.
            for _ in 0..3 {
                let (socket, _) =
                    tokio::time::timeout(std::time::Duration::from_secs(15), listener.accept())
                        .await
                        .unwrap()
                        .unwrap();
                let mut socket = tokio_tungstenite::accept_async(socket).await.unwrap();
                if kind == "pocket_option" {
                    socket
                        .send(Message::Text(fixture("pocket-opening.txt").into()))
                        .await
                        .unwrap();
                }
                loop {
                    let received =
                        tokio::time::timeout(std::time::Duration::from_secs(10), socket.next())
                            .await
                            .unwrap();
                    let Some(Ok(message)) = received else { break };
                    let text = match message {
                        Message::Text(text) => text.to_string(),
                        Message::Close(_) => break,
                        Message::Ping(bytes) => {
                            socket.send(Message::Pong(bytes)).await.unwrap();
                            continue;
                        }
                        _ => continue,
                    };
                    let mut replies = Vec::<Frame>::new();
                    if kind == "deriv" {
                        let fields: BTreeMap<String, Box<RawValue>> =
                            serde_json::from_str(&text).unwrap();
                        let req_id: u64 = field(&fields, "req_id");
                        assert!(
                            !fields.contains_key("buy"),
                            "inspection must never purchase"
                        );
                        if fields.contains_key("active_symbols") {
                            replies.push(frame("deriv-rate-limit.json", req_id));
                        } else if fields.contains_key("contracts_for") {
                            let symbol: String = field(&fields, "contracts_for");
                            replies
                                .push(frame(&format!("deriv-contracts_for-{symbol}.json"), req_id));
                        } else if fields.contains_key("ticks_history") {
                            let symbol: String = field(&fields, "ticks_history");
                            let style: String = field(&fields, "style");
                            let count: u32 = field(&fields, "count");
                            let end: String = field(&fields, "end");
                            assert_eq!(style, "ticks");
                            assert_eq!(count, 100);
                            assert_eq!(end, "latest");
                            replies.push(frame(&format!("deriv-history-{symbol}.json"), req_id));
                        } else if fields.contains_key("ticks") {
                            let symbol: String = field(&fields, "ticks");
                            replies.push(shifted_tick(
                                &format!("deriv-tick-{symbol}.json"),
                                req_id,
                                0,
                            ));
                            replies.push(shifted_tick(
                                &format!("deriv-tick-{symbol}.json"),
                                req_id,
                                2,
                            ));
                        } else if fields.contains_key("forget") {
                            replies.push(frame("deriv-forget.json", req_id));
                            // Synthetic chronology extension using the retained R_50 price and subscription identity.
                            for seconds in [10, 12, 14] {
                                replies.push(shifted_tick("deriv-tick-R_50.json", 6, seconds));
                            }
                        } else {
                            panic!("unexpected local request");
                        }
                    } else if text == "40" {
                        replies.push(Frame::Text(fixture("pocket-connected.txt")));
                    } else if text == "3" {
                        continue;
                    } else {
                        let broker::socket_io::Packet::Event { name, argument } =
                            broker::socket_io::decode(&text).unwrap()
                        else {
                            panic!("unexpected client framing")
                        };
                        match name.as_str() {
                            "auth" => {
                                assert_eq!(argument, b"{\"synthetic\":true}");
                                replies.push(Frame::Text(fixture("pocket-authenticated.txt")));
                                replies.push(Frame::Text(fixture("pocket-account-class.txt")));
                                replies.extend(attachment(
                                    "updateAssets",
                                    fixture("pocket-assets.json"),
                                ));
                            }
                            "changeSymbol" => {
                                #[derive(Deserialize)]
                                struct Change {
                                    asset: String,
                                    period: u8,
                                }
                                let request: Change = serde_json::from_slice(&argument).unwrap();
                                assert_eq!(request.period, 1);
                                replies.extend(attachment(
                                    "updateHistoryNewFast",
                                    if request.asset == "EURUSD_otc" {
                                        fixture("pocket-history-initial.json")
                                    } else {
                                        assert_eq!(request.asset, "#AAPL_otc");
                                        synthetic_pocket_second_history()
                                    },
                                ));
                            }
                            "subscribeSymbol" => {
                                let symbol: String = serde_json::from_slice(&argument).unwrap();
                                for line in if symbol == "EURUSD_otc" {
                                    [6, 15]
                                } else {
                                    assert_eq!(symbol, "#AAPL_otc");
                                    [7, 16]
                                } {
                                    replies.extend(attachment(
                                        "updateStream",
                                        fixture(&format!("pocket-live-{line:02}.json")),
                                    ));
                                }
                            }
                            "unSubscribeSymbol" => {
                                assert_eq!(
                                    serde_json::from_slice::<String>(&argument).unwrap(),
                                    "#AAPL_otc"
                                );
                                for line in 18..=36 {
                                    replies.extend(attachment(
                                        "updateStream",
                                        fixture(&format!("pocket-live-{line:02}.json")),
                                    ));
                                }
                            }
                            _ => panic!("unexpected Pocket Option command"),
                        }
                    }
                    for reply in replies {
                        let message = match reply {
                            Frame::Text(text) => Message::Text(text.into()),
                            Frame::Binary(bytes) => Message::Binary(bytes.into()),
                            _ => panic!("unexpected scripted frame"),
                        };
                        socket.send(message).await.unwrap();
                    }
                }
            }
        })
    });
    (url, server)
}

#[derive(Deserialize)]
struct ReadInspection {
    broker: String,
    checks: Vec<ReadCheck>,
}
#[derive(Deserialize)]
struct ReadCheck {
    name: String,
    result: String,
    detail: ReadDetail,
}
#[derive(Deserialize)]
struct ReadDetail {
    #[serde(default)]
    rows: Option<usize>,
    #[serde(default)]
    counts: Option<BTreeMap<String, u64>>,
    #[serde(default)]
    control: Option<String>,
}
#[test]
fn binary_fetch_verify_and_inspect_both_providers_over_real_local_transports() {
    for kind in ["deriv", "pocket_option"] {
        let scratch = Scratch::new(&format!("phase10_binary_{kind}"));
        let (url, server) = serve_broker(kind);
        let mut config = test_config(&scratch, kind, &url, true);
        if kind == "deriv" {
            #[derive(Deserialize)]
            struct HistoryResponse {
                history: Times,
            }
            #[derive(Deserialize)]
            struct Times {
                times: Vec<i64>,
            }
            let first: HistoryResponse =
                serde_json::from_str(&fixture("deriv-history-R_50.json")).unwrap();
            let second: HistoryResponse =
                serde_json::from_str(&fixture("deriv-history-R_100.json")).unwrap();
            let start = first.history.times[0].max(second.history.times[0]) * 1_000_000;
            let end = (*first.history.times.last().unwrap())
                .min(*second.history.times.last().unwrap())
                * 1_000_000
                + 1_000_000;
            config.history.as_mut().unwrap().start = time_text(start);
            config.history.as_mut().unwrap().end = time_text(end);
        } else {
            config.history.as_mut().unwrap().start = time_text(1_789_347_292_749_000);
            config.history.as_mut().unwrap().end = time_text(1_789_348_000_000_000);
        }
        let config_path = scratch.path("broker.toml");
        fs::write(&config_path, config.canonical_toml()).unwrap();
        let command = |args: &[&str]| {
            let output = std::process::Command::new(env!("CARGO_BIN_EXE_binary-alpha"))
                .args(args)
                .env("PHASE10_SYNTHETIC_AUTH", "{\"synthetic\":true}")
                .output()
                .unwrap();
            assert!(
                output.status.success(),
                "{kind}: {}",
                String::from_utf8_lossy(&output.stderr)
            );
            assert!(
                output.stderr.is_empty(),
                "{}",
                String::from_utf8_lossy(&output.stderr)
            );
            String::from_utf8(output.stdout).unwrap()
        };
        let args = ["data", "fetch", "--config", config_path.to_str().unwrap()];
        let first = command(&args);
        assert_eq!(first.lines().count(), 2);
        assert!(first.lines().all(
            |line| line.starts_with(&format!("fetch development {kind}:"))
                && line.contains("shortfall none")
        ));
        let manifests = read_manifests(&scratch);
        assert_eq!(manifests.len(), 2);
        for manifest in &manifests {
            let report = command(&[
                "data",
                "verify",
                "--manifest",
                &format!(
                    "file://{}",
                    scratch.path("published").join(manifest.key()).display()
                ),
            ]);
            assert!(report.starts_with("verified"));
            let normalized = manifest
                .objects
                .iter()
                .find(|object| object.role == ObjectRole::Normalized)
                .unwrap();
            let definition = config
                .instrument(
                    &InstrumentId {
                        broker: manifest.broker.clone(),
                        provider_symbol: manifest.provider_symbol.clone(),
                    },
                    NativeGranularity::Tick,
                )
                .unwrap();
            let mut rows = Vec::new();
            archive::read_ticks_with(
                &scratch.path("published").join(&normalized.key),
                definition.price_scale,
                |row| {
                    rows.push(row);
                    Ok(())
                },
            )
            .unwrap();
            assert_eq!(rows.len() as u64, manifest.row_count);
            assert!(!rows.is_empty());
            let history = config.history.as_ref().unwrap();
            assert!(
                rows.iter()
                    .all(|row| time(&history.start).unwrap() <= row.event_time_micros
                        && row.event_time_micros < time(&history.end).unwrap())
            );
            let expected = if kind == "deriv" {
                correlated(
                    &fixture(&format!("deriv-history-{}.json", manifest.provider_symbol)),
                    if manifest.provider_symbol.as_str() == "R_50" {
                        1
                    } else {
                        2
                    },
                )
            } else if manifest.provider_symbol.as_str() == "EURUSD_otc" {
                fixture("pocket-history-initial.json")
            } else {
                synthetic_pocket_second_history()
            };
            assert!(
                manifest
                    .objects
                    .iter()
                    .any(|object| object.role == ObjectRole::Source
                        && object.sha256 == hash(expected.as_bytes()))
            );
            for object in &manifest.objects {
                assert_eq!(
                    fs::read(scratch.path("published").join(&object.key)).unwrap(),
                    fs::read(scratch.path("retained").join(&object.key)).unwrap()
                );
            }
        }
        let objects = scratch.objects("published");
        let before = scratch
            .manifests("published")
            .iter()
            .map(|path| (path.clone(), fs::read(path).unwrap()))
            .collect::<Vec<_>>();
        let second = command(&args);
        assert_eq!(second.matches("(already published)").count(), 2);
        assert_eq!(objects, scratch.objects("published"));
        for (path, bytes) in before {
            assert_eq!(fs::read(path).unwrap(), bytes);
        }
        let inspected = command(&[
            "broker",
            "inspect",
            "--config",
            config_path.to_str().unwrap(),
        ]);
        let uri = inspected
            .lines()
            .last()
            .unwrap()
            .strip_prefix("inspection file://")
            .unwrap();
        let report: ReadInspection = serde_json::from_slice(&fs::read(uri).unwrap()).unwrap();
        assert_eq!(report.broker, kind);
        let discovery = report
            .checks
            .iter()
            .find(|c| c.name == "discovery")
            .unwrap();
        if kind == "deriv" {
            assert!(
                discovery
                    .result
                    .contains("unavailable: deriv active_symbols: RateLimit")
            );
        } else {
            assert_eq!(discovery.result, "verified");
        }
        assert_eq!(
            report
                .checks
                .iter()
                .filter(|c| c.name == "history" && c.detail.rows.is_some_and(|rows| rows > 0))
                .count(),
            2
        );
        let live = report.checks.iter().find(|c| c.name == "live").unwrap();
        assert!(
            live.detail
                .counts
                .as_ref()
                .unwrap()
                .values()
                .all(|n| *n >= 2)
        );
        let cancellation = report
            .checks
            .iter()
            .find(|c| c.name == "cancellation")
            .unwrap();
        assert_eq!(
            cancellation.detail.control.as_deref(),
            Some(if kind == "deriv" {
                "acknowledged"
            } else {
                "sent_without_acknowledgement"
            })
        );
        let counts = cancellation.detail.counts.as_ref().unwrap();
        assert_eq!(counts[&config.instruments[1].id().to_string()], 0);
        assert_eq!(
            counts[&config.instruments[0].id().to_string()],
            if kind == "deriv" { 3 } else { 19 }
        );
        assert_eq!(
            fs::read_dir(scratch.path("retained/inspections"))
                .unwrap()
                .count(),
            1
        );
        server.join().unwrap();
    }
}

#[test]
fn configuration_rejects_unsupported_selections_before_connection_and_skeleton_clears_tables() {
    use binary_alpha_engine::config::{AccountClass, InspectProposal, RunMode};
    let scratch = Scratch::new("phase10_config_selections");
    let base = test_config(&scratch, "deriv", "ws://127.0.0.1/", false);
    let mut cases = Vec::new();
    let mut changed = base.clone();
    changed.brokers.push(changed.brokers[0].clone());
    cases.push((changed, "duplicate broker"));
    let mut changed = base.clone();
    changed.history.as_mut().unwrap().instruments.clear();
    cases.push((changed, "history: instruments"));
    let mut changed = base.clone();
    changed.history.as_mut().unwrap().instruments = vec!["UNKNOWN".to_string().try_into().unwrap()];
    cases.push((changed, "history: instruments"));
    let mut changed = base.clone();
    changed.history.as_mut().unwrap().role = DatasetRole::Holdout;
    cases.push((changed, "history: role"));
    let mut changed = base.clone();
    changed.history.as_mut().unwrap().end = time_text(0);
    cases.push((changed, "history: start"));
    let mut changed = base.clone();
    changed.history.as_mut().unwrap().refresh_interval_seconds = Some(0);
    cases.push((changed, "refresh_interval_seconds"));
    let mut changed = base.clone();
    changed.inspect.as_mut().unwrap().live_seconds = 0;
    cases.push((changed, "inspect:"));
    let mut changed = base.clone();
    changed.inspect.as_mut().unwrap().proposal = Some(InspectProposal {
        stake: Decimal::parse("10").unwrap(),
        duration_seconds: 15,
    });
    cases.push((changed, "inspect: proposal"));
    let mut changed = base.clone();
    changed.run_mode = RunMode::Live;
    changed.storage.publication_uri = "gs://example-bucket/test".parse().unwrap();
    cases.push((changed, "requires run_mode research"));
    let mut changed = base.clone();
    if let Broker::Deriv(settings) = &mut changed.brokers[0] {
        settings.credential = Some("SYNTHETIC_REFERENCE".into());
    }
    cases.push((changed, "account_class is required"));
    for (config, message) in cases {
        assert!(
            Config::parse(&config.canonical_toml())
                .unwrap_err()
                .to_string()
                .contains(message),
            "{message}"
        );
    }
    let mut allowed = base.clone();
    if let Broker::Deriv(settings) = &mut allowed.brokers[0] {
        settings.credential = Some("SYNTHETIC_REFERENCE".into());
        settings.account_class = Some(AccountClass::Demo);
    }
    allowed.inspect.as_mut().unwrap().proposal = Some(InspectProposal {
        stake: Decimal::parse("10").unwrap(),
        duration_seconds: 15,
    });
    Config::parse(&allowed.canonical_toml()).unwrap();
    broker::validate_capabilities(&allowed).unwrap();
    let mut unsupported = test_config(&scratch, "pocket_option", "ws://127.0.0.1/", false);
    unsupported.inspect.as_mut().unwrap().proposal = allowed.inspect.unwrap().proposal;
    assert!(
        broker::validate_capabilities(&unsupported)
            .unwrap_err()
            .starts_with("inspect:")
    );
    assert!(
        Config::parse(&unsupported.canonical_toml())
            .unwrap_err()
            .to_string()
            .contains("inspect: proposal")
    );
    let skeleton = binary_alpha_app::skeleton(&base);
    assert!(
        skeleton.brokers.is_empty() && skeleton.history.is_none() && skeleton.inspect.is_none()
    );
    assert!(!skeleton.canonical_toml().contains("brokers"));
}

#[test]
fn explicit_pocket_reconnect_breaks_continuity_and_requires_resubscription() {
    let clock = FakeClock::at(1_789_348_010_000_000);
    let session = || {
        let mut frames = handshake();
        frames.extend(attachment("updateStream", fixture("pocket-live-06.json")));
        frames
    };
    let (connector, sent) = connector(vec![session(), session()], &clock);
    let mut broker = PocketMarketData::connect(
        &pocket_settings(),
        &pocket_ids(),
        connector,
        Box::new(clock),
        "{}".into(),
    )
    .unwrap();
    broker.subscribe(&pocket_ids()[0], scale(5)).unwrap();
    let first = observation(broker.next_live(1000).unwrap());
    broker.reconnect().unwrap();
    assert!(matches!(
        broker.next_live(1000).unwrap(),
        Some(LiveEvent::Break { generation: 1, .. })
    ));
    assert_eq!(
        sent.borrow()
            .iter()
            .filter(|frame| matches!(frame,Frame::Text(text) if text.contains("subscribeSymbol")))
            .count(),
        1
    );
    broker.subscribe(&pocket_ids()[0], scale(5)).unwrap();
    let second = observation(broker.next_live(1000).unwrap());
    assert_eq!(
        (
            first.generation,
            first.sequence,
            second.generation,
            second.sequence
        ),
        (0, 1, 1, 1)
    );
}

#[test]
fn repeated_shortfall_reuses_verified_content_and_preserves_within_page_repeats() {
    let scratch = Scratch::new("phase10_repeat_shortfall");
    let config = test_config(&scratch, "deriv", "ws://127.0.0.1/", false);
    let (local, destination) = stores(&scratch);
    let mut out = Vec::new();
    let mut clock = FakeClock::default();
    let first = page(&[(5, 100), (5, 100), (6, 101)]);
    fetch::pass(
        &config,
        &mut Pages::new(vec![first.clone(), page(&[])]),
        &local,
        &destination,
        &mut clock,
        (0, 10_000_000),
        &mut out,
    )
    .unwrap();
    assert_eq!(read_manifests(&scratch)[0].row_count, 3);
    let count = scratch.objects("published").len();
    // Synthetic change to framing only: the normalized rows and coverage have not changed.
    let mut repeated = first;
    repeated.raw.push(b' ');
    fetch::pass(
        &config,
        &mut Pages::new(vec![repeated, page(&[])]),
        &local,
        &destination,
        &mut clock,
        (0, 10_000_000),
        &mut out,
    )
    .unwrap();
    assert_eq!(read_manifests(&scratch).len(), 1);
    assert_eq!(scratch.objects("published").len(), count);
    assert!(String::from_utf8(out).unwrap().contains("(no new data)"));
}

#[test]
fn deriv_resubscription_uses_the_new_subscription_identity() {
    let clock = FakeClock::default();
    let original = fixture("deriv-tick-R_50.json");
    let header: broker::deriv::Envelope = serde_json::from_str(&original).unwrap();
    let new = original.replace(&header.subscription.unwrap().id, "synthetic-resubscribed");
    let (connector, sent) = connector(
        vec![vec![
            frame("deriv-tick-R_50.json", 1),
            frame("deriv-forget.json", 2),
            Frame::Text(correlated(&new, 3)),
            frame("deriv-forget.json", 4),
        ]],
        &clock,
    );
    let mut broker =
        DerivMarketData::connect(&deriv_settings(), connector, Box::new(clock)).unwrap();
    let instrument = id("deriv", "R_50");
    broker.subscribe(&instrument, scale(4)).unwrap();
    assert_eq!(observation(broker.next_live(1000).unwrap()).sequence, 1);
    broker.unsubscribe(&instrument).unwrap();
    broker.subscribe(&instrument, scale(4)).unwrap();
    assert_eq!(observation(broker.next_live(1000).unwrap()).sequence, 2);
    broker.unsubscribe(&instrument).unwrap();
    assert!(
        matches!(sent.borrow().last().unwrap(), Frame::Text(text) if text.contains("\"forget\":\"synthetic-resubscribed\""))
    );
}

#[test]
fn history_resume_is_bound_to_the_declared_provider_clock() {
    let scratch = Scratch::new("phase10_source_clock");
    let mut config = test_config(&scratch, "pocket_option", "ws://127.0.0.1/", false);
    let (local, destination) = stores(&scratch);
    let mut clock = FakeClock::default();
    let mut out = Vec::new();
    fetch::pass(
        &config,
        &mut Pages::new(vec![range_page(0, 10)]),
        &local,
        &destination,
        &mut clock,
        (0, 10_000_000),
        &mut out,
    )
    .unwrap();
    let original = read_manifests(&scratch).pop().unwrap();
    let source = read_coverage(&scratch, &original).source_identity;
    if let Broker::PocketOption(settings) = &mut config.brokers[0] {
        settings.server_offset_minutes = 0;
    }
    let mut pages = Pages::new(vec![range_page(0, 10)]);
    fetch::pass(
        &config,
        &mut pages,
        &local,
        &destination,
        &mut clock,
        (0, 10_000_000),
        &mut out,
    )
    .unwrap();
    assert_eq!(pages.anchors, [None]);
    let manifests = read_manifests(&scratch);
    assert_eq!(manifests.len(), 2);
    let new = manifests
        .iter()
        .find(|manifest| manifest.generation != original.generation)
        .unwrap();
    assert_ne!(source, read_coverage(&scratch, new).source_identity);
}

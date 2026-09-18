mod common;
#[path = "common/research.rs"]
mod research_fixture;

use binary_alpha_app::broker::deriv::{DerivAccounts, DerivAuthenticated, DerivMarketData};
use binary_alpha_app::broker::pocket_option::{PocketMarketData, provider_token, universal_micros};
use binary_alpha_app::broker::transport::{Connector, Frame, Transport, WebSocketConnector};
use binary_alpha_app::broker::wire::WireDecimal;
use binary_alpha_app::broker::{
    Adapter, Cancellation, Clock, Continuity, HistoryPage, HistoryRows, LiveEvent,
    MarketDataBroker, RateBudget, RateGroup,
};
use binary_alpha_app::store::Store;
use binary_alpha_app::{broker, fetch, verify};
use binary_alpha_engine::config::{Broker, Config, DerivSettings, PocketSettings, RateBudgets};
use binary_alpha_engine::dataset::{
    Capability, DatasetRole, GenerationManifest, NativeGranularity, SourceKind,
};
use binary_alpha_engine::execution::Decimal;
use binary_alpha_engine::market::{
    InstrumentId, PriceScale, Tick, format_event_time_micros as time_text,
    parse_event_time_micros as time,
};
use binary_alpha_engine::stream::{Candle, Flags, InstrumentStream, Observation, Source};
use common::Scratch;
use common::broker::{FakeClock, FakeHttp, connector, correlated, fixture, replace};
use serde::{Deserialize, de::DeserializeOwned};
use serde_json::value::RawValue;
use std::collections::{BTreeMap, VecDeque};
use std::fs;
use std::sync::Arc;
use std::sync::Mutex;

fn hash(bytes: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    binary_alpha_engine::hex(&Sha256::digest(bytes))
}
fn expected_rows(kind: &str, symbol: &str) -> Vec<Tick> {
    fixture(&format!(
        "expected-{kind}-{}.csv",
        symbol.trim_start_matches('#')
    ))
    .lines()
    .map(|line| {
        let (time, price) = line.split_once(',').unwrap();
        Tick {
            event_time_micros: time.parse().unwrap(),
            price_units: price.parse().unwrap(),
        }
    })
    .collect()
}
fn field<T: DeserializeOwned>(map: &BTreeMap<String, Box<RawValue>>, key: &str) -> T {
    serde_json::from_str(map[key].get()).unwrap()
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
        credential_command: None,
        account_class: binary_alpha_engine::config::AccountClass::Demo,
        server_offset_minutes: 120,
        history_pages_in_flight: Some(1),
    }
}
fn pocket_ids() -> Vec<InstrumentId> {
    vec![
        id("pocket_option", "EURUSD_otc"),
        id("pocket_option", "#AAPL_otc"),
    ]
}
fn pocket(frames: Vec<Frame>) -> (PocketMarketData, Arc<Mutex<Vec<Frame>>>) {
    let clock = FakeClock::at(1_789_348_000_000_000);
    let (connector, sent) = pocket_connector(vec![frames], &clock);
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
    assert!(matches!(decode("41").unwrap(), Packet::Disconnected));
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
fn deriv_discovery_reads_plain_and_exponent_pip_sizes() {
    let clock = FakeClock::at(1_789_348_000_000_000);
    let (connector, _) = connector(
        vec![vec![
            frame("deriv-active_symbols-excerpt.json", 1),
            Frame::Text(correlated(
                &fixture("deriv-active_symbols-excerpt.json").replace("1e-05", "2e-05"),
                2,
            )),
        ]],
        &clock,
    );
    let mut broker =
        DerivMarketData::connect(&deriv_settings(), connector, Box::new(clock)).unwrap();
    let discovered = broker.discover().unwrap();
    let precision: Vec<(&str, Option<u8>, Option<bool>)> = discovered
        .iter()
        .map(|instrument| {
            (
                instrument.symbol.as_str(),
                instrument.precision,
                instrument.open,
            )
        })
        .collect();
    assert_eq!(
        precision,
        vec![
            ("R_100", Some(2), Some(true)),
            ("R_50", Some(4), Some(true)),
            ("frxEURUSD", Some(5), Some(true)),
        ]
    );
    assert_eq!(discovered[2].display_name.as_deref(), Some("EUR/USD"));
    // A pip size whose mantissa is not one is still unsupported, in either spelling.
    assert!(
        broker
            .discover()
            .unwrap_err()
            .contains("unsupported pip_size precision")
    );
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
        .history_page(
            &id("deriv", "R_50"),
            scale(4),
            None,
            NativeGranularity::Tick,
        )
        .unwrap();
    let (_, rows) = broker
        .decode_history(
            &id("deriv", "R_50"),
            &page.raw,
            scale(4),
            NativeGranularity::Tick,
        )
        .unwrap();
    assert_eq!(rows.len(), 100);
    assert_eq!(rows.ticks().unwrap()[0].price_units, 920242);
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
    let requests = sent.lock().unwrap();
    assert!(requests.iter().any(|f| matches!(f, Frame::Text(t) if t.contains("\"count\":1000") && t.contains("\"end\":\"latest\""))));
}

#[test]
fn deriv_history_retries_rate_limits_and_retains_only_the_successful_page() {
    let clock = FakeClock::default();
    let successful = correlated(&fixture("deriv-history-R_50.json"), 3);
    // Keep the scripted transport's receipt latency separate from the adapter's backoff clock.
    let (connector, sent) = connector(
        vec![vec![
            deriv_history_error(1, "RateLimit"),
            deriv_history_error(2, "RateLimit"),
            Frame::Text(successful.clone()),
        ]],
        &FakeClock::default(),
    );
    let mut broker =
        DerivMarketData::connect(&deriv_settings(), connector, Box::new(clock.clone())).unwrap();
    let page = broker
        .history_page(
            &id("deriv", "R_50"),
            scale(4),
            Some(1_789_348_000_000_000),
            NativeGranularity::Tick,
        )
        .unwrap();
    let (_, rows) = broker
        .decode_history(
            &id("deriv", "R_50"),
            &page.raw,
            scale(4),
            NativeGranularity::Tick,
        )
        .unwrap();
    assert_eq!(rows.ticks().unwrap(), expected_rows("history", "R_50"));
    assert_eq!(page.raw, successful.as_bytes());
    assert_eq!(page.anchor_token.as_deref(), Some("1789348000"));
    assert_eq!(clock.now_micros(), 3_000_000);
    assert_eq!(page.receipt_micros, 3_000_000);
    let requests = sent.lock().unwrap();
    assert_eq!(requests.len(), 3);
    for (index, request) in requests.iter().enumerate() {
        let Frame::Text(request) = request else {
            panic!("expected a history request");
        };
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(request).unwrap(),
            serde_json::json!({
                "ticks_history": "R_50",
                "style": "ticks",
                "start": "1788743200",
                "end": "1789348000",
                "count": 1000,
                "req_id": index + 1,
            })
        );
    }
}

#[test]
fn deriv_history_rate_limits_exhaust_the_page_waiting_budget() {
    let clock = FakeClock::default();
    let (connector, sent) = connector(
        vec![
            (1..=20)
                .map(|req_id| deriv_history_error(req_id, "RateLimit"))
                .collect(),
        ],
        &FakeClock::default(),
    );
    let mut broker =
        DerivMarketData::connect(&deriv_settings(), connector, Box::new(clock.clone())).unwrap();
    let error = broker
        .history_page(
            &id("deriv", "R_50"),
            scale(4),
            Some(1_789_348_000_000_000),
            NativeGranularity::Tick,
        )
        .unwrap_err();
    assert_eq!(error, "deriv history: RateLimit (retried for 120 s)");
    assert_eq!(clock.now_micros(), 120_000_000);
    // 1 + 2 + 4 + fourteen 8-second waits + the final 1-second remainder.
    assert_eq!(sent.lock().unwrap().len(), 18);
}

fn deriv_history_error(req_id: u64, code: &str) -> Frame {
    Frame::Text(
        serde_json::json!({
            "error": {"code": code, "message": "Synthetic history rejection"},
            "msg_type": "history",
            "echo_req": {
                "ticks_history": "R_50",
                "style": "ticks",
                "start": "1788743200",
                "end": "1789348000",
                "count": 1000,
                "req_id": req_id,
            },
            "req_id": req_id,
        })
        .to_string(),
    )
}

#[test]
fn deriv_history_rate_limit_deadline_includes_rate_admission() {
    let clock = FakeClock::default();
    let (connector, sent) = connector(
        vec![vec![
            deriv_history_error(1, "RateLimit"),
            deriv_history_error(2, "RateLimit"),
            frame("deriv-history-R_50.json", 3),
        ]],
        &FakeClock::default(),
    );
    let mut settings = deriv_settings();
    let mut limits = RateBudgets::default();
    limits.other.per_minute = 1;
    settings.budgets = Some(limits);
    let mut broker =
        DerivMarketData::connect(&settings, connector, Box::new(clock.clone())).unwrap();
    let error = broker
        .history_page(
            &id("deriv", "R_50"),
            scale(4),
            Some(1_789_348_000_000_000),
            NativeGranularity::Tick,
        )
        .unwrap_err();
    assert_eq!(error, "deriv history: RateLimit (retried for 120 s)");
    assert_eq!(clock.now_micros(), 120_000_000);
    assert_eq!(
        sent.lock().unwrap().len(),
        2,
        "expired rate admission must not send another request"
    );
}

#[test]
fn deriv_history_rate_limit_deadline_includes_response_latency() {
    struct DelayedConnector {
        inner: Box<dyn Connector>,
        clock: FakeClock,
    }
    struct DelayedTransport {
        inner: Box<dyn Transport>,
        clock: FakeClock,
    }
    impl Connector for DelayedConnector {
        fn connect(
            &mut self,
            url: &str,
            headers: &[(String, String)],
        ) -> Result<Box<dyn Transport>, String> {
            Ok(Box::new(DelayedTransport {
                inner: self.inner.connect(url, headers)?,
                clock: self.clock.clone(),
            }))
        }
    }
    impl Transport for DelayedTransport {
        fn send(&mut self, frame: Frame) -> Result<(), String> {
            self.inner.send(frame)
        }
        fn receive(&mut self, timeout: i64) -> Result<Option<Frame>, String> {
            self.clock.sleep(17_000_000);
            self.inner.receive(timeout)
        }
        fn close(&mut self) -> Result<(), String> {
            self.inner.close()
        }
    }
    for successful_last in [false, true] {
        let clock = FakeClock::default();
        let (inner, sent) = connector(
            vec![
                (1..=7)
                    .map(|index| {
                        if index == 7 && successful_last {
                            frame("deriv-history-R_50.json", index)
                        } else {
                            deriv_history_error(index, "RateLimit")
                        }
                    })
                    .collect(),
            ],
            &FakeClock::default(),
        );
        let connector = Box::new(DelayedConnector {
            inner,
            clock: clock.clone(),
        });
        let mut broker =
            DerivMarketData::connect(&deriv_settings(), connector, Box::new(clock.clone()))
                .unwrap();
        let error = broker
            .history_page(
                &id("deriv", "R_50"),
                scale(4),
                Some(1_789_348_000_000_000),
                NativeGranularity::Tick,
            )
            .unwrap_err();
        assert_eq!(error, "deriv history: RateLimit (retried for 120 s)");
        assert_eq!(sent.lock().unwrap().len(), 7);
        assert_eq!(clock.now_micros(), 150_000_000);
    }
}

#[test]
fn deriv_rejects_malformed_missing_fields_wrong_types_and_wrong_request_ids() {
    let base = fixture("deriv-history-R_50.json");
    let mut missing: BTreeMap<String, Box<RawValue>> = serde_json::from_str(&base).unwrap();
    missing.remove("msg_type");
    let cases = vec![
        (replace(&base, "pip_size", "5"), "pip_size"),
        ("{not json".into(), "malformed"),
        (serde_json::to_string(&missing).unwrap(), "msg_type field"),
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
        // Envelope faults fail the request; body faults fail the decode of the matched page.
        assert!(
            broker
                .history_page(
                    &id("deriv", "R_50"),
                    scale(4),
                    None,
                    NativeGranularity::Tick
                )
                .and_then(|page| broker.decode_history(
                    &id("deriv", "R_50"),
                    &page.raw,
                    scale(4),
                    NativeGranularity::Tick
                ))
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
            .history_page(
                &id("deriv", "R_50"),
                scale(4),
                None,
                NativeGranularity::Tick
            )
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
        replace(&fixture("pocket-history-older-1.json"), "index", "0"),
    ));
    frames.extend(attachment(
        "loadHistoryPeriod",
        replace(&fixture("pocket-history-older-2.json"), "index", "1"),
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
    let decode = |broker: &PocketMarketData, page: &HistoryPage| {
        broker
            .decode_history(&instrument, &page.raw, scale(5), NativeGranularity::Tick)
            .unwrap()
            .1
    };
    let initial = broker
        .history_page(&instrument, scale(5), None, NativeGranularity::Tick)
        .unwrap();
    let initial_rows = decode(&broker, &initial);
    assert_eq!(initial_rows.len(), 1478);
    assert_eq!(
        initial_rows.ticks().unwrap()[0].event_time_micros,
        1_789_347_292_749_000
    );
    let older = broker
        .history_page(
            &instrument,
            scale(5),
            initial_rows.first_time_micros(),
            NativeGranularity::Tick,
        )
        .unwrap();
    let older_rows = decode(&broker, &older);
    assert_eq!(older_rows.len(), 413);
    assert_eq!(older.anchor_token.as_deref(), Some("1789354492.749"));
    let oldest = broker
        .history_page(
            &instrument,
            scale(5),
            older_rows.first_time_micros(),
            NativeGranularity::Tick,
        )
        .unwrap();
    let oldest_rows = decode(&broker, &oldest);
    assert_eq!(oldest_rows.len(), 409);
    let rows = fetch::prepend_page(
        oldest_rows.into_ticks().unwrap(),
        fetch::prepend_page(
            older_rows.into_ticks().unwrap(),
            initial_rows.into_ticks().unwrap(),
        ),
    );
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
    assert!(sent.lock().unwrap().contains(&Frame::Text("3".into())));
    assert!(sent.lock().unwrap().contains(&Frame::Pong(vec![7, 8])));
    assert!(
        sent.lock()
            .unwrap()
            .iter()
            .any(|f| matches!(f,Frame::Text(t) if t.contains("\"time\":1789354492.749")))
    );
    assert!(sent.lock().unwrap().contains(&Frame::Text(
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

fn pocket_candle_page(index: u64, starts: &[i64]) -> String {
    serde_json::json!({
        "asset": "EURUSD_otc",
        "index": index,
        "period": 5,
        "data": starts.iter().map(|start| serde_json::json!({
            "symbol_id": 7,
            "time": start + 7200,
            "open": 1.25,
            "high": 1.5,
            "low": 1.0,
            "close": 1.375,
            "volume": 2,
        })).collect::<Vec<_>>(),
    })
    .to_string()
}

#[test]
fn pocket_parallel_connections_use_distinct_increasing_positive_indexes() {
    let indexes = std::thread::scope(|scope| {
        let jobs: Vec<_> = (0..2)
            .map(|_| {
                scope.spawn(|| {
                    let mut frames = handshake();
                    for ordinal in 0..2 {
                        frames.extend(attachment(
                            "loadHistoryPeriodFast",
                            full_candle_page(ordinal, 10),
                        ));
                    }
                    let (mut adapter, sent) = pocket(frames);
                    for _ in 0..2 {
                        adapter
                            .history_page(
                                &pocket_ids()[0],
                                scale(5),
                                Some(10_000_000),
                                NativeGranularity::Bar { period_seconds: 5 },
                            )
                            .unwrap();
                    }
                    let indexes: Vec<_> = pocket_sent_events(&sent, "loadHistoryPeriod")
                        .iter()
                        .map(|request| request["index"].as_u64().unwrap())
                        .collect();
                    assert_eq!(indexes.len(), 2);
                    assert!(
                        indexes
                            .iter()
                            .all(|index| *index > 0 && *index <= i64::MAX as u64)
                    );
                    assert_eq!(indexes[1], indexes[0] + 1);
                    indexes
                })
            })
            .collect();
        jobs.into_iter()
            .map(|job| job.join().unwrap())
            .collect::<Vec<_>>()
    });
    assert!(
        indexes[0].iter().all(|index| !indexes[1].contains(index)),
        "{indexes:?}"
    );
}

fn pocket_sent_events(sent: &Mutex<Vec<Frame>>, name: &str) -> Vec<serde_json::Value> {
    sent.lock()
        .unwrap()
        .iter()
        .filter_map(|frame| {
            let Frame::Text(text) = frame else {
                return None;
            };
            match broker::socket_io::decode(text).ok()? {
                broker::socket_io::Packet::Event {
                    name: event,
                    argument,
                } if event == name => Some(serde_json::from_slice(&argument).unwrap()),
                _ => None,
            }
        })
        .collect()
}

// Scripted Pocket pages use zero-based request ordinals. Echo the actual wire index without
// coupling fixtures to the adapter's seed; an ordinal with no request remains foreign.
fn pocket_response(raw: &str, sent: &Mutex<Vec<Frame>>) -> String {
    let value: serde_json::Value = serde_json::from_str(raw).unwrap();
    let Some(ordinal) = value["index"]
        .as_u64()
        .and_then(|n| usize::try_from(n).ok())
    else {
        return raw.to_string();
    };
    let requests = pocket_sent_events(sent, "loadHistoryPeriod");
    let index = requests
        .get(ordinal)
        .map_or(u64::MAX, |request| request["index"].as_u64().unwrap());
    replace(raw, "index", &index.to_string())
}

struct IndexedConnector {
    inner: Box<dyn Connector>,
    sent: Arc<Mutex<Vec<Frame>>>,
}
impl Connector for IndexedConnector {
    fn connect(
        &mut self,
        url: &str,
        headers: &[(String, String)],
    ) -> Result<Box<dyn Transport>, String> {
        Ok(Box::new(IndexedTransport {
            inner: self.inner.connect(url, headers)?,
            sent: Arc::clone(&self.sent),
        }))
    }
}
struct IndexedTransport {
    inner: Box<dyn Transport>,
    sent: Arc<Mutex<Vec<Frame>>>,
}
impl Transport for IndexedTransport {
    fn send(&mut self, frame: Frame) -> Result<(), String> {
        self.inner.send(frame)
    }
    fn receive(&mut self, timeout: i64) -> Result<Option<Frame>, String> {
        Ok(self.inner.receive(timeout)?.map(|frame| match frame {
            Frame::Binary(raw) => Frame::Binary(
                pocket_response(std::str::from_utf8(&raw).unwrap(), &self.sent).into_bytes(),
            ),
            other => other,
        }))
    }
    fn close(&mut self) -> Result<(), String> {
        self.inner.close()
    }
}
fn pocket_connector(
    sessions: Vec<Vec<Frame>>,
    clock: &FakeClock,
) -> (Box<dyn Connector>, Arc<Mutex<Vec<Frame>>>) {
    let (inner, sent) = connector(sessions, clock);
    (
        Box::new(IndexedConnector {
            inner,
            sent: Arc::clone(&sent),
        }),
        sent,
    )
}

#[derive(Clone)]
struct CandleTrace {
    sent: Arc<Mutex<Vec<Frame>>>,
    // Request index -> (requests sent before receipt, actual receipt clock).
    arrivals: Arc<Mutex<BTreeMap<u64, (usize, i64)>>>,
    clock: FakeClock,
}
struct CandleConnector {
    inner: Box<dyn Connector>,
    trace: CandleTrace,
}
impl Connector for CandleConnector {
    fn connect(
        &mut self,
        url: &str,
        headers: &[(String, String)],
    ) -> Result<Box<dyn Transport>, String> {
        Ok(Box::new(CandleTransport {
            inner: self.inner.connect(url, headers)?,
            trace: self.trace.clone(),
        }))
    }
}
struct CandleTransport {
    inner: Box<dyn Transport>,
    trace: CandleTrace,
}
impl Transport for CandleTransport {
    fn send(&mut self, frame: Frame) -> Result<(), String> {
        self.inner.send(frame)
    }
    fn receive(&mut self, timeout: i64) -> Result<Option<Frame>, String> {
        let frame = self.inner.receive(timeout)?;
        if let Some(Frame::Binary(raw)) = &frame {
            let value: serde_json::Value = serde_json::from_slice(raw).unwrap();
            if let Some(index) = value["index"].as_u64() {
                self.trace.arrivals.lock().unwrap().insert(
                    index,
                    (
                        pocket_sent_events(&self.trace.sent, "loadHistoryPeriod").len(),
                        self.trace.clock.now_micros(),
                    ),
                );
            }
        }
        Ok(frame)
    }
    fn close(&mut self) -> Result<(), String> {
        self.inner.close()
    }
}
fn candle_adapter(
    sessions: Vec<Vec<Frame>>,
    clock: &FakeClock,
    limit: Option<u16>,
) -> (PocketMarketData, CandleTrace) {
    let (inner, sent) = pocket_connector(sessions, clock);
    let trace = CandleTrace {
        sent,
        arrivals: Arc::default(),
        clock: clock.clone(),
    };
    let settings = PocketSettings {
        history_pages_in_flight: limit,
        ..pocket_settings()
    };
    let adapter = PocketMarketData::connect(
        &settings,
        &pocket_ids(),
        Box::new(CandleConnector {
            inner,
            trace: trace.clone(),
        }),
        Box::new(clock.clone()),
        "{}".into(),
    )
    .unwrap();
    (adapter, trace)
}
fn full_candle_page(index: u64, anchor: i64) -> String {
    pocket_candle_page(
        index,
        &(anchor - 195..=anchor).step_by(5).collect::<Vec<_>>(),
    )
}
fn assert_candle_page(
    adapter: &PocketMarketData,
    page: &HistoryPage,
    instrument: &InstrumentId,
    anchor: i64,
) {
    assert_candle_rows(
        adapter,
        page,
        instrument,
        anchor,
        &(anchor - 195..=anchor).step_by(5).collect::<Vec<_>>(),
    );
}
fn assert_candle_rows(
    adapter: &PocketMarketData,
    page: &HistoryPage,
    instrument: &InstrumentId,
    anchor: i64,
    starts: &[i64],
) {
    use binary_alpha_engine::market::Bar;
    assert_eq!(page.anchor_token, Some((anchor + 7200).to_string()));
    let (symbol_id, rows) = adapter
        .decode_history(
            instrument,
            &page.raw,
            scale(5),
            NativeGranularity::Bar { period_seconds: 5 },
        )
        .unwrap();
    assert_eq!(symbol_id, Some(7));
    let HistoryRows::Bars(rows) = rows else {
        panic!("expected candles")
    };
    assert_eq!(
        rows,
        starts
            .iter()
            .map(|&start_unix_s| Bar {
                provider: (),
                start_unix_s,
                open: 1.25,
                high: 1.5,
                low: 1.0,
                close: 1.375,
                volume: 2.0,
                period_s: 5,
            })
            .collect::<Vec<_>>()
    );
}

#[test]
fn pocket_candle_prefetch_walk_batches_requests_and_preserves_each_page() {
    let clock = FakeClock::at(1_789_348_000_000_000);
    let index = 0;
    let mut frames = handshake();
    let raw: Vec<_> = (0..5)
        .map(|n| full_candle_page(index + n, 2000 - n as i64 * 195))
        .collect();
    for page in &raw {
        frames.extend(attachment("loadHistoryPeriodFast", page.clone()));
    }
    let (mut adapter, trace) = candle_adapter(vec![frames], &clock, Some(3));
    let instrument = &pocket_ids()[0];
    let granularity = NativeGranularity::Bar { period_seconds: 5 };
    let mut anchor = 2_000_000_000;
    for (n, raw) in raw.iter().enumerate() {
        let page = adapter
            .history_page(instrument, scale(5), Some(anchor), granularity)
            .unwrap();
        assert_candle_page(&adapter, &page, instrument, anchor / 1_000_000);
        assert_eq!(page.raw, pocket_response(raw, &trace.sent).as_bytes());
        let request_index = pocket_sent_events(&trace.sent, "loadHistoryPeriod")[n]["index"]
            .as_u64()
            .unwrap();
        let (sent_before_receipt, receipt) = trace.arrivals.lock().unwrap()[&request_index];
        assert_eq!(sent_before_receipt, 3 + n);
        assert_eq!(page.receipt_micros, receipt);
        assert_eq!(receipt, clock.now_micros());
        // Follow the fetch owner's cursor, derived from the returned rows.
        anchor = adapter
            .decode_history(instrument, &page.raw, scale(5), granularity)
            .unwrap()
            .1
            .first_time_micros()
            .unwrap();
    }
    let requests = pocket_sent_events(&trace.sent, "loadHistoryPeriod");
    assert_eq!(requests.len(), 7);
    for (n, request) in requests.iter().enumerate() {
        assert_eq!(
            *request,
            serde_json::json!({
                "asset": "EURUSD_otc", "index": requests[0]["index"].as_u64().unwrap() + n as u64,
                "time": 9200 - n * 195, "offset": 200, "period": 5,
            })
        );
    }
}

#[test]
fn pocket_candle_prefetch_out_of_order_keeps_original_receipt_and_default_window() {
    let mut clock = FakeClock::at(1_789_348_000_000_000);
    let index = 0;
    let mut frames = handshake();
    let first = full_candle_page(index, 2000);
    let second = full_candle_page(index + 1, 1805);
    frames.extend(attachment("loadHistoryPeriodFast", second.clone()));
    frames.extend(attachment("loadHistoryPeriodFast", first.clone()));
    let (mut adapter, trace) = candle_adapter(vec![frames], &clock, None);
    let instrument = &pocket_ids()[0];
    let granularity = NativeGranularity::Bar { period_seconds: 5 };
    let first_page = adapter
        .history_page(instrument, scale(5), Some(2_000_000_000), granularity)
        .unwrap();
    assert_eq!(
        first_page.raw,
        pocket_response(&first, &trace.sent).as_bytes()
    );
    assert_candle_page(&adapter, &first_page, instrument, 2000);
    clock.sleep(1000);
    let second_page = adapter
        .history_page(instrument, scale(5), Some(1_805_000_000), granularity)
        .unwrap();
    assert_eq!(
        second_page.raw,
        pocket_response(&second, &trace.sent).as_bytes()
    );
    assert_candle_page(&adapter, &second_page, instrument, 1805);
    let requests = pocket_sent_events(&trace.sent, "loadHistoryPeriod");
    let index = requests[0]["index"].as_u64().unwrap();
    let arrivals = trace.arrivals.lock().unwrap();
    assert_eq!(arrivals[&index], (8, first_page.receipt_micros));
    assert_eq!(arrivals[&(index + 1)], (8, second_page.receipt_micros));
    assert!(second_page.receipt_micros < first_page.receipt_micros);
    assert!(second_page.receipt_micros < clock.now_micros());
    assert_eq!(
        pocket_sent_events(&trace.sent, "loadHistoryPeriod").len(),
        9
    );
    assert_eq!(adapter.foreign_history_responses(), 0);
}

#[test]
fn pocket_candle_prefetch_gap_discards_skipped_anchors_and_stale_responses() {
    let clock = FakeClock::at(1_789_348_000_000_000);
    let index = 0;
    let mut frames = handshake();
    // The third page omits 1225..=1415, reaching back to 1220 to fill 40 bars.
    // Its first row skips the predicted 1415 anchor, even though 1220 is buffered.
    let gap_starts: Vec<_> = std::iter::once(1220)
        .chain((1420..=1610).step_by(5))
        .collect();
    for (n, page_anchor) in [2000, 1805, 1610, 1415, 1220, 1220].into_iter().enumerate() {
        let page = if n == 2 {
            pocket_candle_page(index + n as u64, &gap_starts)
        } else {
            full_candle_page(index + n as u64, page_anchor)
        };
        frames.extend(attachment("loadHistoryPeriodFast", page));
    }
    let (mut adapter, trace) = candle_adapter(vec![frames], &clock, Some(3));
    let instrument = &pocket_ids()[0];
    let granularity = NativeGranularity::Bar { period_seconds: 5 };
    let mut anchor = 2_000_000_000;
    for first in [1805, 1610, 1220, 1025] {
        let page = adapter
            .history_page(instrument, scale(5), Some(anchor), granularity)
            .unwrap();
        if first == 1220 {
            assert_candle_rows(&adapter, &page, instrument, 1610, &gap_starts);
        } else {
            assert_candle_page(&adapter, &page, instrument, anchor / 1_000_000);
        }
        anchor = adapter
            .decode_history(instrument, &page.raw, scale(5), granularity)
            .unwrap()
            .1
            .first_time_micros()
            .unwrap();
        assert_eq!(anchor, first * 1_000_000);
    }
    let requests = pocket_sent_events(&trace.sent, "loadHistoryPeriod");
    assert_eq!(requests.len(), 8);
    assert_eq!(requests[4]["time"], 8420);
    assert_eq!(requests[5]["time"], 8420);
    let index = requests[0]["index"].as_u64().unwrap();
    assert_eq!(requests[5]["index"], index + 5);
    assert_eq!(trace.arrivals.lock().unwrap()[&(index + 5)].0, 8);
    assert_eq!(adapter.foreign_history_responses(), 2);
}

#[test]
fn pocket_candle_prefetch_reconnect_resends_only_outstanding_pages() {
    for disconnect in [true, false] {
        let clock = FakeClock::at(1_789_348_000_000_000);
        let index = 0;
        let mut first = handshake();
        let buffered = full_candle_page(index + 1, 1805);
        first.extend(attachment("loadHistoryPeriodFast", buffered.clone()));
        if disconnect {
            first.push(Frame::Text("41".into()));
        }
        let reconnected_index = 3;
        let mut second = handshake();
        for (index, anchor) in [(reconnected_index, 2000), (reconnected_index + 1, 1610)] {
            second.extend(attachment(
                "loadHistoryPeriodFast",
                full_candle_page(index, anchor),
            ));
        }
        let (mut adapter, trace) = candle_adapter(vec![first, second], &clock, Some(3));
        let instrument = &pocket_ids()[0];
        let granularity = NativeGranularity::Bar { period_seconds: 5 };
        for anchor in [2000, 1805, 1610] {
            let page = adapter
                .history_page(instrument, scale(5), Some(anchor * 1_000_000), granularity)
                .unwrap();
            assert_candle_page(&adapter, &page, instrument, anchor);
            if anchor == 1805 {
                assert_eq!(page.raw, pocket_response(&buffered, &trace.sent).as_bytes());
                let index = pocket_sent_events(&trace.sent, "loadHistoryPeriod")[0]["index"]
                    .as_u64()
                    .unwrap();
                assert_eq!(
                    page.receipt_micros,
                    trace.arrivals.lock().unwrap()[&(index + 1)].1
                );
                assert!(page.receipt_micros < clock.now_micros());
            }
        }
        let requests = pocket_sent_events(&trace.sent, "loadHistoryPeriod");
        assert_eq!(requests.len(), 7);
        assert_eq!(requests[3]["time"], 9200);
        assert!(
            requests
                .windows(2)
                .all(|pair| pair[0]["index"].as_u64() < pair[1]["index"].as_u64())
        );
        assert_eq!(requests[4]["time"], 8810);
        assert_eq!(
            requests[4]["index"].as_u64().unwrap(),
            requests[3]["index"].as_u64().unwrap() + 1
        );
        assert_eq!(
            requests
                .iter()
                .filter(|request| request["time"] == 9005)
                .count(),
            1
        );
        assert_eq!(adapter.history_reconnects(), 1);
        assert_eq!(adapter.foreign_history_responses(), 0);
    }
}

#[test]
fn pocket_candle_prefetch_instrument_change_discards_buffered_and_outstanding_pages() {
    let clock = FakeClock::at(1_789_348_000_000_000);
    let index = 0;
    let mut frames = handshake();
    for (n, anchor) in [(1, 1805), (0, 2000), (2, 1610)] {
        frames.extend(attachment(
            "loadHistoryPeriodFast",
            full_candle_page(index + n, anchor),
        ));
    }
    let other = replace(&full_candle_page(index + 3, 1805), "asset", "\"#AAPL_otc\"");
    frames.extend(attachment("loadHistoryPeriodFast", other.clone()));
    let (mut adapter, trace) = candle_adapter(vec![frames], &clock, Some(3));
    let ids = pocket_ids();
    let granularity = NativeGranularity::Bar { period_seconds: 5 };
    adapter
        .history_page(&ids[0], scale(5), Some(2_000_000_000), granularity)
        .unwrap();
    let page = adapter
        .history_page(&ids[1], scale(5), Some(1_805_000_000), granularity)
        .unwrap();
    assert_eq!(page.raw, pocket_response(&other, &trace.sent).as_bytes());
    assert_candle_page(&adapter, &page, &ids[1], 1805);
    let requests = pocket_sent_events(&trace.sent, "loadHistoryPeriod");
    assert_eq!(requests.len(), 6);
    for (n, request) in requests[3..].iter().enumerate() {
        assert_eq!(request["asset"], "#AAPL_otc");
        assert_eq!(request["time"], 9005 - n * 195);
    }
    assert_eq!(adapter.foreign_history_responses(), 1);
}

#[test]
fn pocket_candle_history_reconnects_after_second_page_and_preserves_rows() {
    let clock = FakeClock::at(1_789_348_000_000_000);
    let first_index = 0;
    let mut first = handshake();
    first.extend(attachment(
        "loadHistoryPeriodFast",
        full_candle_page(first_index, 2000),
    ));
    first.extend(attachment(
        "loadHistoryPeriodFast",
        full_candle_page(first_index + 1, 1805),
    ));
    first.push(Frame::Text("41".into()));
    // The fourth request retries the outstanding page after reconnect.
    let second_index = 3;
    let mut second = handshake();
    second.extend(attachment(
        "loadHistoryPeriodFast",
        full_candle_page(second_index, 1610),
    ));
    let (connector, sent) = pocket_connector(vec![first, second], &clock);
    let mut adapter = PocketMarketData::connect(
        &pocket_settings(),
        &pocket_ids(),
        connector,
        Box::new(clock),
        "{}".into(),
    )
    .unwrap();
    let instrument = &pocket_ids()[0];
    adapter.subscribe(instrument, scale(5)).unwrap();
    let granularity = NativeGranularity::Bar { period_seconds: 5 };
    for anchor in [2000, 1805, 1610] {
        let page = adapter
            .history_page(instrument, scale(5), Some(anchor * 1_000_000), granularity)
            .unwrap();
        assert_candle_page(&adapter, &page, instrument, anchor);
    }
    assert_eq!(adapter.history_reconnects(), 1);
    assert_eq!(adapter.foreign_history_responses(), 0);
    assert_eq!(pocket_sent_events(&sent, "auth").len(), 2);
    assert_eq!(
        sent.lock()
            .unwrap()
            .iter()
            .filter(|frame| **frame == Frame::Close)
            .count(),
        1
    );
    let requests = pocket_sent_events(&sent, "loadHistoryPeriod");
    assert_eq!(requests.len(), 4);
    assert!(
        requests
            .windows(2)
            .all(|pair| pair[0]["index"].as_u64() < pair[1]["index"].as_u64())
    );
    for (request, anchor) in requests.iter().zip([9200, 9005, 8810, 8810]) {
        assert_eq!(
            *request,
            serde_json::json!({
                "asset": "EURUSD_otc", "index": request["index"], "time": anchor, "offset": 200, "period": 5,
            })
        );
    }
    assert!(matches!(
        adapter.next_live(0).unwrap(),
        Some(LiveEvent::Break { generation: 1, .. })
    ));
    assert!(
        adapter
            .unsubscribe(instrument)
            .unwrap_err()
            .contains("not subscribed")
    );
}

#[test]
fn pocket_history_skips_foreign_assets_and_indexes_without_extending_deadline() {
    for granularity in [
        NativeGranularity::Tick,
        NativeGranularity::Bar { period_seconds: 5 },
    ] {
        for matching in [true, false] {
            let clock = FakeClock::at(1_789_348_000_000_000);
            let index = 0;
            let (name, raw) = if granularity == NativeGranularity::Tick {
                (
                    "loadHistoryPeriod",
                    serde_json::json!({
                        "asset": "EURUSD_otc", "index": index, "period": 0,
                        "data": [{"time": 7205, "price": 1.25}],
                    })
                    .to_string(),
                )
            } else {
                ("loadHistoryPeriodFast", full_candle_page(index, 10))
            };
            let mut frames = handshake();
            frames.extend(attachment(
                "loadHistoryPeriodFast",
                replace(&raw, "asset", "\"#AAPL_otc\""),
            ));
            frames.extend(attachment(
                "loadHistoryPeriod",
                replace(&raw, "index", &(index + 99).to_string()),
            ));
            if granularity != NativeGranularity::Tick {
                frames.extend(attachment(
                    "loadHistoryPeriodFast",
                    full_candle_page(index + 1, -185),
                ));
            }
            if matching {
                frames.extend(attachment(name, raw.clone()));
            }
            let mut sessions = vec![frames];
            if !matching {
                for _ in 0..3 {
                    let mut retry = handshake();
                    retry.extend(attachment(name, replace(&raw, "asset", "\"#AAPL_otc\"")));
                    retry.extend(attachment(name, replace(&raw, "index", "99")));
                    sessions.push(retry);
                }
            }
            let (connector, sent) = pocket_connector(sessions, &clock);
            let mut adapter = PocketMarketData::connect(
                &PocketSettings {
                    history_pages_in_flight: Some(3),
                    ..pocket_settings()
                },
                &pocket_ids(),
                connector,
                Box::new(clock.clone()),
                "{}".into(),
            )
            .unwrap();
            let started = clock.now_micros();
            let result =
                adapter.history_page(&pocket_ids()[0], scale(5), Some(10_000_000), granularity);
            if matching {
                let page = result.unwrap();
                assert_eq!(page.raw, pocket_response(&raw, &sent).as_bytes());
                let (symbol_id, rows) = adapter
                    .decode_history(&pocket_ids()[0], &page.raw, scale(5), granularity)
                    .unwrap();
                if granularity == NativeGranularity::Tick {
                    assert_eq!(rows.len(), 1);
                    assert_eq!(rows.first_time_micros(), Some(5_000_000));
                } else {
                    assert_candle_page(&adapter, &page, &pocket_ids()[0], 10);
                }
                assert_eq!(
                    symbol_id,
                    if granularity == NativeGranularity::Tick {
                        None
                    } else {
                        Some(7)
                    }
                );
            } else {
                let error = result.unwrap_err();
                assert_eq!(
                    error,
                    format!(
                        "pocket_option: no matching history response after asset or index mismatch; pocket_option: history response timeout reconnect limit reached; pocket_option {name}: response timeout"
                    )
                );
                // Four fixed 20s deadlines plus three six-frame handshakes at 10us/frame.
                assert_eq!(clock.now_micros() - started, 80_000_180);
            }
            assert_eq!(
                adapter.foreign_history_responses(),
                if matching { 2 } else { 8 }
            );
            assert_eq!(adapter.history_reconnects(), if matching { 0 } else { 3 });
            assert_eq!(
                pocket_sent_events(&sent, "auth").len(),
                if matching { 1 } else { 4 }
            );
            assert_eq!(
                pocket_sent_events(&sent, "loadHistoryPeriod").len(),
                if granularity == NativeGranularity::Tick {
                    if matching { 1 } else { 4 }
                } else {
                    // The received prefetched page survives all three reconnects.
                    if matching { 3 } else { 9 }
                }
            );
        }
    }
}

#[test]
fn pocket_history_reconnects_after_silent_response_timeout() {
    for limit in [1, 3] {
        let clock = FakeClock::at(1_789_348_000_000_000);
        let mut first = handshake();
        first.extend(attachment(
            "loadHistoryPeriodFast",
            full_candle_page(0, 2000),
        ));
        // Exhausted scripted frames leave the transport open and advance the fake clock
        // by the receive timeout: no namespace disconnect or transport Close is returned.
        let mut second = handshake();
        second.extend(attachment(
            "loadHistoryPeriodFast",
            full_candle_page(u64::from(limit) + 1, 1805),
        ));
        let (mut adapter, trace) = candle_adapter(vec![first, second], &clock, Some(limit));
        let instrument = &pocket_ids()[0];
        let granularity = NativeGranularity::Bar { period_seconds: 5 };
        let page = adapter
            .history_page(instrument, scale(5), Some(2_000_000_000), granularity)
            .unwrap();
        assert_candle_page(&adapter, &page, instrument, 2000);
        assert_eq!(adapter.history_reconnects(), 0);
        let started = clock.now_micros();
        let page = adapter
            .history_page(instrument, scale(5), Some(1_805_000_000), granularity)
            .unwrap();
        assert_candle_page(&adapter, &page, instrument, 1805);
        assert_eq!(clock.now_micros() - started, 20_000_080);
        assert_eq!(adapter.history_reconnects(), 1);
        assert_eq!(pocket_sent_events(&trace.sent, "auth").len(), 2);
        let requests = pocket_sent_events(&trace.sent, "loadHistoryPeriod");
        let limit = usize::from(limit);
        assert_eq!(requests.len(), 2 * limit + 1);
        for (original, resent) in requests[1..=limit].iter().zip(&requests[limit + 1..]) {
            assert_eq!(original["time"], resent["time"]);
            assert!(resent["index"].as_u64() > original["index"].as_u64());
        }
    }
}

#[test]
fn pocket_history_reconnects_before_sending_on_stale_session() {
    for silent_retries in [0, 3] {
        let mut clock = FakeClock::at(1_789_348_000_000_000);
        let mut first = handshake();
        first.extend(attachment(
            "loadHistoryPeriodFast",
            full_candle_page(0, 2000),
        ));
        let mut sessions = vec![first];
        sessions.extend((0..silent_retries).map(|_| handshake()));
        let mut last = handshake();
        last.extend(attachment(
            "loadHistoryPeriodFast",
            full_candle_page(1 + silent_retries, 1805),
        ));
        sessions.push(last);
        let (mut adapter, trace) = candle_adapter(sessions, &clock, Some(1));
        let instrument = &pocket_ids()[0];
        let granularity = NativeGranularity::Bar { period_seconds: 5 };
        let page = adapter
            .history_page(instrument, scale(5), Some(2_000_000_000), granularity)
            .unwrap();
        assert_candle_page(&adapter, &page, instrument, 2000);
        let sent_before_advance = trace.sent.lock().unwrap().len();
        clock.sleep(30_000_000);
        let page = adapter
            .history_page(instrument, scale(5), Some(1_805_000_000), granularity)
            .unwrap();
        assert_candle_page(&adapter, &page, instrument, 1805);
        // A preflight reconnect leaves all three per-page retries available.
        assert_eq!(adapter.history_reconnects(), 1 + silent_retries);
        assert_eq!(
            pocket_sent_events(&trace.sent, "auth").len() as u64,
            2 + silent_retries
        );
        let requests = pocket_sent_events(&trace.sent, "loadHistoryPeriod");
        assert_eq!(requests.len() as u64, 2 + silent_retries);
        assert!(requests[1..].iter().all(|request| request["time"] == 9005));
        let sent = trace.sent.lock().unwrap();
        // Close is the very first action after the clock advance: the stale connection
        // receives no further history request, and the new connection authenticates first.
        assert_eq!(sent[sent_before_advance], Frame::Close);
        assert_eq!(sent[sent_before_advance + 1], Frame::Text("40".into()));
        assert!(
            matches!(&sent[sent_before_advance + 2], Frame::Text(text) if text.starts_with("42[\"auth\","))
        );
        assert!(
            matches!(&sent[sent_before_advance + 3], Frame::Text(text) if text.starts_with("42[\"loadHistoryPeriod\","))
        );
    }
}

#[test]
fn pocket_history_stale_check_uses_heartbeat_receipt_and_strict_threshold() {
    for heartbeat in [
        Frame::Text("2".into()),
        Frame::Ping(vec![1]),
        Frame::Pong(vec![2]),
    ] {
        let mut clock = FakeClock::at(1_789_348_000_000_000);
        let mut frames = handshake();
        frames.extend(attachment(
            "loadHistoryPeriodFast",
            full_candle_page(0, 2000),
        ));
        frames.push(heartbeat);
        frames.extend(attachment(
            "loadHistoryPeriodFast",
            full_candle_page(1, 1805),
        ));
        let (mut adapter, trace) = candle_adapter(vec![frames], &clock, Some(1));
        let instrument = &pocket_ids()[0];
        let granularity = NativeGranularity::Bar { period_seconds: 5 };
        adapter
            .history_page(instrument, scale(5), Some(2_000_000_000), granularity)
            .unwrap();
        clock.sleep(20_000_000);
        assert!(adapter.next_live(0).unwrap().is_none());
        clock.sleep(25_000_000);
        let page = adapter
            .history_page(instrument, scale(5), Some(1_805_000_000), granularity)
            .unwrap();
        assert_candle_page(&adapter, &page, instrument, 1805);
        assert_eq!(adapter.history_reconnects(), 0);
        assert_eq!(pocket_sent_events(&trace.sent, "auth").len(), 1);
    }
}

#[test]
fn pocket_history_stale_reconnects_stop_at_adapter_limit() {
    let mut clock = FakeClock::at(1_789_348_000_000_000);
    let sessions = (0..=20)
        .map(|index| {
            let mut frames = handshake();
            frames.extend(attachment(
                "loadHistoryPeriodFast",
                full_candle_page(index, 2000),
            ));
            frames
        })
        .collect();
    let (mut adapter, trace) = candle_adapter(sessions, &clock, Some(1));
    for reconnects in 0..=20 {
        let page = adapter
            .history_page(
                &pocket_ids()[0],
                scale(5),
                Some(2_000_000_000),
                NativeGranularity::Bar { period_seconds: 5 },
            )
            .unwrap();
        assert_candle_page(&adapter, &page, &pocket_ids()[0], 2000);
        assert_eq!(adapter.history_reconnects(), reconnects);
        clock.sleep(30_000_000);
    }
    let sent_before = trace.sent.lock().unwrap().len();
    assert_eq!(
        adapter
            .history_page(
                &pocket_ids()[0],
                scale(5),
                Some(2_000_000_000),
                NativeGranularity::Bar { period_seconds: 5 }
            )
            .unwrap_err(),
        "pocket_option: stale history session reconnect limit reached"
    );
    assert_eq!(adapter.history_reconnects(), 20);
    assert_eq!(pocket_sent_events(&trace.sent, "auth").len(), 21);
    assert_eq!(trace.sent.lock().unwrap().len(), sent_before);
}

#[test]
fn pocket_history_stops_after_three_response_timeout_reconnects_for_one_page() {
    for (before, granularity, event) in [
        (None, NativeGranularity::Tick, "updateHistoryNewFast"),
        (
            Some(10_000_000),
            NativeGranularity::Tick,
            "loadHistoryPeriod",
        ),
        (
            Some(10_000_000),
            NativeGranularity::Bar { period_seconds: 5 },
            "loadHistoryPeriodFast",
        ),
    ] {
        let clock = FakeClock::at(1_789_348_000_000_000);
        let (mut adapter, trace) =
            candle_adapter((0..4).map(|_| handshake()).collect(), &clock, Some(1));
        let started = clock.now_micros();
        let error = adapter
            .history_page(&pocket_ids()[0], scale(5), before, granularity)
            .unwrap_err();
        assert_eq!(
            error,
            format!(
                "pocket_option: history response timeout reconnect limit reached; pocket_option {event}: response timeout"
            )
        );
        assert_eq!(adapter.history_reconnects(), 3);
        assert_eq!(pocket_sent_events(&trace.sent, "auth").len(), 4);
        assert_eq!(
            pocket_sent_events(
                &trace.sent,
                if before.is_none() {
                    "changeSymbol"
                } else {
                    "loadHistoryPeriod"
                }
            )
            .len(),
            4
        );
        assert_eq!(clock.now_micros() - started, 80_000_180);
    }
}

#[test]
fn pocket_history_reconnects_after_send_and_close_fail() {
    struct FailingConnector {
        inner: Box<dyn Connector>,
        fail: bool,
    }
    struct FailingTransport(Box<dyn Transport>);
    impl Connector for FailingConnector {
        fn connect(
            &mut self,
            url: &str,
            headers: &[(String, String)],
        ) -> Result<Box<dyn Transport>, String> {
            let inner = self.inner.connect(url, headers)?;
            if std::mem::take(&mut self.fail) {
                Ok(Box::new(FailingTransport(inner)))
            } else {
                Ok(inner)
            }
        }
    }
    impl Transport for FailingTransport {
        fn send(&mut self, frame: Frame) -> Result<(), String> {
            if matches!(&frame, Frame::Text(text) if text.starts_with("42[\"loadHistoryPeriod\"")) {
                return Err("websocket 127.0.0.1: send failed".into());
            }
            self.0.send(frame)
        }
        fn receive(&mut self, timeout: i64) -> Result<Option<Frame>, String> {
            self.0.receive(timeout)
        }
        fn close(&mut self) -> Result<(), String> {
            Err("websocket 127.0.0.1: send failed".into())
        }
    }
    let clock = FakeClock::at(1_789_348_000_000_000);
    let mut second = handshake();
    second.extend(attachment("loadHistoryPeriodFast", full_candle_page(0, 10)));
    let (inner, sent) = pocket_connector(vec![handshake(), second], &clock);
    let mut adapter = PocketMarketData::connect(
        &pocket_settings(),
        &pocket_ids(),
        Box::new(FailingConnector { inner, fail: true }),
        Box::new(clock),
        "{}".into(),
    )
    .unwrap();
    let page = adapter
        .history_page(
            &pocket_ids()[0],
            scale(5),
            Some(10_000_000),
            NativeGranularity::Bar { period_seconds: 5 },
        )
        .unwrap();
    assert_candle_page(&adapter, &page, &pocket_ids()[0], 10);
    assert_eq!(adapter.history_reconnects(), 1);
    assert_eq!(pocket_sent_events(&sent, "auth").len(), 2);
    assert_eq!(pocket_sent_events(&sent, "loadHistoryPeriod").len(), 1);
}

#[test]
fn pocket_history_reconnects_after_tcp_close_between_pages() {
    use futures_util::{SinkExt, StreamExt};
    use tokio_tungstenite::tungstenite::Message;

    let (listener, endpoint) = loopback_listener();
    let (closed, dropped) = std::sync::mpsc::channel();
    let server = std::thread::spawn(move || {
        runtime().block_on(async move {
            let listener = tokio::net::TcpListener::from_std(listener).unwrap();
            let mut requests = Vec::new();
            for anchor in [2000, 1805] {
                let (socket, _) =
                    tokio::time::timeout(std::time::Duration::from_secs(5), listener.accept())
                        .await
                        .unwrap()
                        .unwrap();
                let mut socket = tokio_tungstenite::accept_async(socket).await.unwrap();
                for frame in handshake() {
                    socket
                        .send(match frame {
                            Frame::Text(text) => Message::Text(text.into()),
                            Frame::Binary(bytes) => Message::Binary(bytes.into()),
                            _ => unreachable!(),
                        })
                        .await
                        .unwrap();
                }
                loop {
                    let message = socket.next().await.unwrap().unwrap();
                    let Message::Text(text) = message else {
                        continue;
                    };
                    let Ok(broker::socket_io::Packet::Event { name, argument }) =
                        broker::socket_io::decode(&text)
                    else {
                        continue;
                    };
                    if name != "loadHistoryPeriod" {
                        continue;
                    }
                    let request: serde_json::Value = serde_json::from_slice(&argument).unwrap();
                    assert_eq!(request["time"], anchor + 7200);
                    assert_eq!(request["asset"], "EURUSD_otc");
                    let index = request["index"].as_u64().unwrap();
                    requests.push(index);
                    for frame in
                        attachment("loadHistoryPeriodFast", full_candle_page(index, anchor))
                    {
                        socket
                            .send(match frame {
                                Frame::Text(text) => Message::Text(text.into()),
                                Frame::Binary(bytes) => Message::Binary(bytes.into()),
                                _ => unreachable!(),
                            })
                            .await
                            .unwrap();
                    }
                    break;
                }
                // Drop TCP directly: neither a Socket.IO 41 nor a WebSocket Close is sent.
                drop(socket);
                if anchor == 2000 {
                    closed.send(()).unwrap();
                }
            }
            requests
        })
    });
    let mut adapter = PocketMarketData::connect(
        &PocketSettings {
            endpoint,
            ..pocket_settings()
        },
        &pocket_ids(),
        Box::new(WebSocketConnector::new().unwrap()),
        Box::new(FakeClock::at(1_789_348_000_000_000)),
        "{}".into(),
    )
    .unwrap();
    for anchor in [2000, 1805] {
        let page = adapter
            .history_page(
                &pocket_ids()[0],
                scale(5),
                Some(anchor * 1_000_000),
                NativeGranularity::Bar { period_seconds: 5 },
            )
            .unwrap();
        assert_candle_page(&adapter, &page, &pocket_ids()[0], anchor);
        if anchor == 2000 {
            assert_eq!(adapter.history_reconnects(), 0);
            dropped
                .recv_timeout(std::time::Duration::from_secs(5))
                .unwrap();
        }
    }
    assert_eq!(adapter.history_reconnects(), 1);
    let requests = server.join().unwrap();
    assert!(
        requests[1] > requests[0] + 1,
        "the failed request must be resent with a new index: {requests:?}"
    );
}

#[test]
fn pocket_history_stops_after_three_namespace_reconnects_for_one_page() {
    let clock = FakeClock::at(1_789_348_000_000_000);
    let sessions = (0..4)
        .map(|_| {
            let mut frames = handshake();
            frames.push(Frame::Text("41".into()));
            frames
        })
        .collect();
    let (connector, sent) = pocket_connector(sessions, &clock);
    let mut adapter = PocketMarketData::connect(
        &pocket_settings(),
        &pocket_ids(),
        connector,
        Box::new(clock),
        "{}".into(),
    )
    .unwrap();
    let error = adapter
        .history_page(
            &pocket_ids()[0],
            scale(5),
            Some(10_000_000),
            NativeGranularity::Bar { period_seconds: 5 },
        )
        .unwrap_err();
    assert_eq!(
        error,
        "pocket_option: the server keeps disconnecting the namespace"
    );
    assert_eq!(adapter.history_reconnects(), 3);
    assert_eq!(pocket_sent_events(&sent, "auth").len(), 4);
    assert_eq!(
        sent.lock()
            .unwrap()
            .iter()
            .filter(|frame| **frame == Frame::Close)
            .count(),
        3
    );
    let requests = pocket_sent_events(&sent, "loadHistoryPeriod");
    assert_eq!(requests.len(), 4);
    assert!(
        requests
            .windows(2)
            .all(|pair| pair[0]["index"].as_u64() < pair[1]["index"].as_u64())
    );
    assert!(requests.iter().all(|request| request["time"] == 7210));
}

#[test]
fn pocket_history_reconnect_limit_persists_across_pages() {
    for disconnect in [true, false] {
        let clock = FakeClock::at(1_789_348_000_000_000);
        let mut index = 0;
        let mut sessions = Vec::new();
        for session in 0..=20 {
            let mut frames = handshake();
            if session > 0 {
                frames.extend(attachment(
                    "loadHistoryPeriodFast",
                    full_candle_page(index, 10),
                ));
            }
            if disconnect {
                frames.push(Frame::Text("41".into()));
            }
            index += if session == 0 { 1 } else { 2 };
            sessions.push(frames);
        }
        let (connector, sent) = pocket_connector(sessions, &clock);
        let mut adapter = PocketMarketData::connect(
            &pocket_settings(),
            &pocket_ids(),
            connector,
            Box::new(clock),
            "{}".into(),
        )
        .unwrap();
        for expected_reconnects in 1..=20 {
            adapter
                .history_page(
                    &pocket_ids()[0],
                    scale(5),
                    Some(10_000_000),
                    NativeGranularity::Bar { period_seconds: 5 },
                )
                .unwrap();
            assert_eq!(adapter.history_reconnects(), expected_reconnects);
        }
        assert_eq!(
            adapter
                .history_page(
                    &pocket_ids()[0],
                    scale(5),
                    Some(10_000_000),
                    NativeGranularity::Bar { period_seconds: 5 },
                )
                .unwrap_err(),
            if disconnect {
                "pocket_option: the server keeps disconnecting the namespace"
            } else {
                "pocket_option: history response timeout reconnect limit reached; pocket_option loadHistoryPeriodFast: response timeout"
            }
        );
        assert_eq!(adapter.history_reconnects(), 20);
        assert_eq!(pocket_sent_events(&sent, "auth").len(), 21);
        assert_eq!(pocket_sent_events(&sent, "loadHistoryPeriod").len(), 41);
    }
}

#[test]
fn pocket_connect_time_namespace_disconnect_keeps_origin_diagnostic() {
    let expected = "socket.io: the server disconnected the namespace (an `origin` setting is usually required)";
    for during_reconnect in [false, true] {
        let clock = FakeClock::default();
        let mut sessions = Vec::new();
        if during_reconnect {
            let mut first = handshake();
            first.push(Frame::Text("41".into()));
            sessions.push(first);
        }
        let mut rejected = handshake()[..2].to_vec();
        rejected.push(Frame::Text("41".into()));
        sessions.push(rejected);
        let (connector, sent) = connector(sessions, &clock);
        let result = PocketMarketData::connect(
            &pocket_settings(),
            &pocket_ids(),
            connector,
            Box::new(clock),
            "{}".into(),
        );
        let error = if during_reconnect {
            let mut adapter = result.ok().unwrap();
            let error = adapter
                .history_page(
                    &pocket_ids()[0],
                    scale(5),
                    Some(10_000_000),
                    NativeGranularity::Bar { period_seconds: 5 },
                )
                .unwrap_err();
            assert_eq!(adapter.history_reconnects(), 1);
            error
        } else {
            result.err().unwrap()
        };
        assert_eq!(error, expected);
        assert_eq!(
            pocket_sent_events(&sent, "auth").len(),
            if during_reconnect { 2 } else { 1 }
        );
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
        native_granularity: NativeGranularity::Tick,
        seeds: Vec::new(),
        overlap_seconds: None,
        max_pages: None,
        max_elapsed_seconds: None,
    });
    config.inspect = Some(binary_alpha_engine::config::Inspect {
        live_observations: 2,
        live_seconds: 1,
        proposal: None,
    });
    Config::parse(&config.canonical_toml()).unwrap()
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
    assert_eq!(sent.lock().unwrap().len(), 1);
    assert!(
        matches!(&sent.lock().unwrap()[0],Frame::Text(t) if t.contains("\"balance\":1") && !t.contains("buy"))
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
        let mut streams = config
            .instruments
            .iter()
            .map(|definition| {
                (
                    definition.id(),
                    (
                        InstrumentStream::new(definition, stream_source(definition.price_scale))
                            .unwrap(),
                        Vec::new(),
                    ),
                )
            })
            .collect::<BTreeMap<_, _>>();
        let mut observations = Vec::new();
        let mut continuity = Continuity::default();
        while let Some(event) = adapter.market().next_live(1_000_000).unwrap() {
            let row = observation(Some(event));
            continuity.accept(&row).unwrap();
            assert_ne!(row.receipt_micros, row.provider_time_micros);
            let (stream, candles) = streams.get_mut(&row.instrument).unwrap();
            stream
                .push(
                    Observation::Tick(Tick {
                        event_time_micros: row.provider_time_micros,
                        price_units: row.price_units,
                    }),
                    candles,
                )
                .unwrap();
            let settings = if kind == "deriv" {
                Broker::Deriv(deriv_settings())
            } else {
                Broker::PocketOption(pocket_settings())
            };
            assert_eq!(row.source, broker::source_identity(&settings));
            observations.push(row);
        }
        for definition in &config.instruments {
            let normalized = observations
                .iter()
                .filter(|row| row.instrument == definition.id())
                .map(|row| Tick {
                    event_time_micros: row.provider_time_micros,
                    price_units: row.price_units,
                })
                .collect::<Vec<_>>();
            assert_eq!(
                normalized,
                expected_rows("live", definition.provider_symbol.as_str())
            );
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
            let (_, first) = streams.remove(&definition.id()).unwrap();

            let deriv = kind == "deriv";
            let aapl = definition.provider_symbol.as_str() == "#AAPL_otc";
            let (open, high, low, close) = match definition.provider_symbol.as_str() {
                "R_50" => (918_608, 918_608, 918_608, 918_608),
                "R_100" => (57_252, 57_252, 57_252, 57_252),
                "EURUSD_otc" => (114_806, 114_808, 114_806, 114_808),
                "#AAPL_otc" => (181_338, 181_338, 181_334, 181_334),
                _ => unreachable!(),
            };
            let start = if deriv {
                1_789_346_764_000_000
            } else {
                1_789_347_995_000_000
            };
            assert_eq!(
                first[usize::from(!deriv)].1,
                Candle {
                    open_time_micros: start,
                    close_time_micros: start + 1_000_000,
                    known_at_micros: if deriv {
                        start + 2_000_000
                    } else {
                        start + 1_061_000 + i64::from(aapl) * 1000
                    },
                    first_event_micros: if deriv { start } else { start + 61_000 },
                    last_event_micros: if deriv { start } else { start + 561_000 },
                    active_span_micros: if deriv { 0 } else { 500_000 },
                    open_units: open,
                    high_units: high,
                    low_units: low,
                    close_units: close,
                    observations: if deriv { 1 } else { 2 },
                    duplicates: 0,
                    volume: None,
                    gap_before_micros: if deriv { None } else { Some(500_000) },
                    max_gap_inside_micros: if deriv { 0 } else { 500_000 },
                    missing_buckets_before: 0,
                    frozen_observations: 1,
                    frozen_micros: 0,
                    max_jump_basis_points: 0,
                    max_delayed_jump_basis_points: 0,
                    max_reopen_jump_basis_points: 0,
                    flags: Flags {
                        low_activity: true,
                        ..Flags::default()
                    },
                }
            );
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
        _: NativeGranularity,
    ) -> Result<HistoryPage, String> {
        self.anchors.push(before);
        self.pages.pop_front().ok_or("unexpected page request")?
    }
    fn decode_history(
        &self,
        _: &InstrumentId,
        raw: &[u8],
        scale: PriceScale,
        native: NativeGranularity,
    ) -> Result<(Option<i32>, HistoryRows), String> {
        let rows: Vec<(i64, i64)> = serde_json::from_slice(raw).map_err(|e| e.to_string())?;
        if let NativeGranularity::Bar { period_seconds } = native {
            return Ok((
                Some(538),
                HistoryRows::Bars(
                    rows.iter()
                        .map(|&(t, p)| {
                            let price = p as f64 / scale.unit() as f64;
                            binary_alpha_engine::market::Bar {
                                provider: (),
                                start_unix_s: t / 1_000_000,
                                open: price,
                                high: price,
                                low: price,
                                close: price,
                                volume: 1.,
                                period_s: period_seconds,
                            }
                        })
                        .collect(),
                ),
            ));
        }
        Ok((
            None,
            HistoryRows::Ticks(
                rows.iter()
                    .map(|&(t, p)| Tick {
                        event_time_micros: t,
                        price_units: p,
                    })
                    .collect(),
            ),
        ))
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
/// A synthetic page of `(seconds, units)` rows; the fake adapter decodes it back exactly.
fn page(rows: &[(i64, i64)]) -> HistoryPage {
    page_micros(
        &rows
            .iter()
            .map(|&(t, p)| (t * 1_000_000, p))
            .collect::<Vec<_>>(),
    )
}
/// A synthetic page of `(microseconds, units)` rows.
fn page_micros(rows: &[(i64, i64)]) -> HistoryPage {
    HistoryPage {
        raw: serde_json::to_vec(&rows).unwrap(),
        anchor_token: None,
        receipt_micros: 0,
    }
}
fn rows_of(rows: &[(i64, i64)]) -> Vec<Tick> {
    rows.iter()
        .map(|&(t, p)| Tick {
            event_time_micros: t * 1_000_000,
            price_units: p,
        })
        .collect()
}
fn range_rows(start: i64, end: i64) -> Vec<(i64, i64)> {
    (start..end).map(|i| (i, 100_000 + i)).collect()
}
fn range_page(start: i64, end: i64) -> HistoryPage {
    page(&range_rows(start, end))
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
    use binary_alpha_engine::dataset::coverage::DailyCoverage;
    assert_eq!(
        manifest.layout,
        Some(binary_alpha_engine::dataset::Layout::DailyV2)
    );
    let coverage =
        DailyCoverage::from_json(&fs::read(scratch.path("published").join(&object.key)).unwrap())
            .unwrap();
    coverage.check_manifest(manifest).unwrap();
    let lineage: serde_json::Value = serde_json::from_slice(
        &fs::read(
            scratch.path("published").join(
                &manifest
                    .objects
                    .iter()
                    .find(|o| o.path == "provenance/lineage.json")
                    .unwrap()
                    .key,
            ),
        )
        .unwrap(),
    )
    .unwrap();
    let acquisition = coverage
        .acquisitions
        .iter()
        .find(|a| a.acquisition_id == lineage["continuation"]["acquisition_id"])
        .unwrap();
    let range = |r: &binary_alpha_engine::dataset::coverage::CoverageRange| fetch::Range {
        start: r.start.clone(),
        end: r.end.clone(),
    };
    let shortfall =
        |s: &binary_alpha_engine::dataset::coverage::CoverageShortfall| fetch::Shortfall {
            reason: s.reason.clone(),
            unresolved: range(&s.unresolved),
        };
    fetch::HistoryCoverage {
        schema_version: 1,
        source_identity: acquisition.source_identity.clone(),
        broker: manifest.broker.to_string(),
        provider_symbol: manifest.provider_symbol.to_string(),
        role: manifest.role,
        requested: range(&acquisition.requested[0]),
        verified: acquisition.verified.first().map(range),
        actual: Some(fetch::Actual {
            first: manifest.coverage.first_event_time.clone(),
            last: manifest.coverage.last_event_time.clone(),
        }),
        rows: manifest.row_count,
        pages: vec![],
        bundle: None,
        shortfall: acquisition.shortfalls.first().map(shortfall),
        tail_shortfall: acquisition.shortfalls.get(1).map(shortfall),
        native_granularity: manifest.native_granularity,
        seed: serde_json::from_value(lineage["continuation"]["seed"].clone()).unwrap(),
    }
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
    assert_eq!(
        pages.anchors,
        [
            Some(10_000_000),
            Some(5_000_000),
            Some(20_000_000),
            Some(30_000_000)
        ]
    );
    let manifests = read_manifests(&scratch);
    assert_eq!(
        manifests.iter().map(|m| m.row_count).collect::<Vec<_>>(),
        [10, 20, 30]
    );
    for (index, manifest) in manifests.iter().enumerate() {
        assert_eq!(
            manifest.layout,
            Some(binary_alpha_engine::dataset::Layout::DailyV2)
        );
        let coverage = read_coverage(&scratch, manifest);
        assert_eq!(
            coverage.verified,
            Some(fetch::Range {
                start: time_text(0),
                end: time_text((index as i64 + 1) * 10_000_000 - 999_999)
            })
        );
        assert_eq!(
            coverage.shortfall.as_ref().unwrap().reason,
            "unresolved_tail"
        );
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
        (0, 30_000_000),
        &mut out,
    )
    .unwrap();
    let after = read_manifests(&scratch);
    assert_eq!(after.len(), 4);
    assert!(scratch.objects("published").len() > before.len());
    assert_eq!(after.last().unwrap().row_count, 30);
    let old_observations: Vec<_> = manifests[2]
        .objects
        .iter()
        .filter(|o| o.path.starts_with("observations/"))
        .collect();
    for old in old_observations {
        assert!(
            after
                .last()
                .unwrap()
                .objects
                .iter()
                .any(|o| o.path == old.path && o.key == old.key)
        );
    }
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

        let mut out = Vec::new();
        fetch::pass(
            &config,
            &mut pages,
            &local,
            &destination,
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
        assert_eq!(
            coverage.verified.as_ref().unwrap().end,
            time_text(7_000_001)
        );
        assert_eq!(
            coverage.tail_shortfall,
            Some(fetch::Shortfall {
                reason: "unresolved_tail".into(),
                unresolved: fetch::Range {
                    start: time_text(7_000_001),
                    end: time_text(10_000_000)
                },
            })
        );
        let mut repair = Pages::new(vec![range_page(0, 10)]);
        fetch::pass(
            &config,
            &mut repair,
            &local,
            &destination,
            (0, 10_000_000),
            &mut out,
        )
        .unwrap();
        assert_eq!(repair.anchors, [Some(10_000_000)]);
        let repaired = read_manifests(&scratch);
        assert_eq!(repaired.last().unwrap().row_count, 10);
        assert_eq!(
            read_coverage(&scratch, repaired.last().unwrap())
                .shortfall
                .unwrap()
                .reason,
            "unresolved_tail"
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
            (0, 10_000_000),
            &mut out
        )
        .unwrap_err()
        .contains("synthetic broker failure")
    );
    assert!(read_manifests(&scratch).is_empty());
    let first = range_page(5, 10);
    let second = range_page(0, 6);
    let file = scratch.path("expected-daily-ticks.parquet");
    let instrument = &config.instruments[0];
    binary_alpha_app::daily::write_ticks(
        &file,
        "1970-01-01",
        &InstrumentId {
            broker: instrument.broker.clone(),
            provider_symbol: instrument.provider_symbol.clone(),
        },
        instrument.price_scale,
        [range_rows(0, 10)
            .into_iter()
            .map(|(t, p)| Tick {
                event_time_micros: t * 1_000_000,
                price_units: p,
            })
            .collect::<Vec<_>>()],
    )
    .unwrap();
    let failure_path = scratch
        .path("published/objects")
        .join(hash(&fs::read(file).unwrap()));
    fs::create_dir_all(&failure_path).unwrap();
    assert!(
        fetch::pass(
            &config,
            &mut Pages::new(vec![first.clone(), second.clone()]),
            &local,
            &destination,
            (0, 10_000_000),
            &mut out
        )
        .is_err()
    );
    assert!(
        scratch
            .path("retained/objects")
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
        (0, 10_000_000),
        &mut out,
    )
    .unwrap();
    assert!(String::from_utf8(out).unwrap().contains("reused 0"));
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
// Synthetic page boundaries cut the retained numeric tokens without changing their values.
fn deriv_history_response(symbol: &str, end: &str, req_id: u64) -> String {
    let original = fixture(&format!("deriv-history-{symbol}.json"));
    if end == "latest" {
        return correlated(&original, req_id);
    }
    let fields: BTreeMap<String, Box<RawValue>> = serde_json::from_str(&original).unwrap();
    let history: BTreeMap<String, Box<RawValue>> =
        serde_json::from_str(fields["history"].get()).unwrap();
    let times: Vec<i64> = field(&history, "times");
    let prices: Vec<Box<RawValue>> = field(&history, "prices");
    let end: i64 = end.parse().unwrap();
    let upper = times.partition_point(|time| *time <= end);
    let lower = upper.saturating_sub(40);
    let history = replace(
        fields["history"].get(),
        "times",
        &serde_json::to_string(&times[lower..upper]).unwrap(),
    );
    let history = replace(
        &history,
        "prices",
        &serde_json::to_string(&prices[lower..upper]).unwrap(),
    );
    correlated(&replace(&original, "history", &history), req_id)
}

fn pocket_history_response(symbol: &str, before: &WireDecimal, index: u64) -> String {
    let initial = if symbol == "EURUSD_otc" {
        fixture("pocket-history-initial.json")
    } else {
        synthetic_pocket_second_history()
    };
    let initial: BTreeMap<String, Box<RawValue>> = serde_json::from_str(&initial).unwrap();
    let rows: Vec<[Box<RawValue>; 2]> = field(&initial, "history");
    let data = rows
        .iter()
        .filter(|row| {
            let time: WireDecimal = serde_json::from_str(row[0].get()).unwrap();
            time.decimal()
                .unwrap()
                .compare(before.decimal().unwrap())
                .unwrap()
                != std::cmp::Ordering::Greater
        })
        .map(|row| format!(r#"{{"time":{},"price":{}}}"#, row[0].get(), row[1].get()))
        .collect::<Vec<_>>()
        .join(",");
    format!(
        r#"{{"asset":{},"index":{index},"period":0,"data":[{data}]}}"#,
        serde_json::to_string(symbol).unwrap()
    )
}

type SentHistory = std::sync::Arc<std::sync::Mutex<Vec<(usize, String, Vec<u8>)>>>;
fn serve_broker(kind: &'static str) -> (String, std::thread::JoinHandle<()>, SentHistory) {
    use futures_util::{SinkExt, StreamExt};
    use tokio_tungstenite::tungstenite::Message;
    let (listener, url) = loopback_listener();
    let sent: SentHistory = Default::default();
    let server_sent = sent.clone();
    let server = std::thread::spawn(move || {
        runtime().block_on(async move {
            let listener = tokio::net::TcpListener::from_std(listener).unwrap();
            // Two fetch invocations (the second reuses the committed range), then one inspection.
            for invocation in 0..3 {
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
                            assert_eq!(count, 1000);
                            replies
                                .push(Frame::Text(deriv_history_response(&symbol, &end, req_id)));
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
                            "loadHistoryPeriod" => {
                                #[derive(Deserialize)]
                                struct Older {
                                    asset: String,
                                    time: WireDecimal,
                                    index: u64,
                                    period: u8,
                                    offset: u16,
                                }
                                let request: Older = serde_json::from_slice(&argument).unwrap();
                                assert_eq!((request.period, request.offset), (1, 200));
                                let payload = pocket_history_response(
                                    &request.asset,
                                    &request.time,
                                    request.index,
                                );
                                server_sent.lock().unwrap().push((
                                    invocation,
                                    request.asset.clone(),
                                    payload.as_bytes().to_vec(),
                                ));
                                replies.extend(attachment("loadHistoryPeriod", payload));
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
    (url, server, sent)
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
        let (url, server, sent) = serve_broker(kind);
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
                + 1;
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
                && line.contains("shortfall ")
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
            assert_eq!(
                manifest.layout,
                Some(binary_alpha_engine::dataset::Layout::DailyV2)
            );
            let rows = common::read_normalized_ticks(&scratch.path("published"), manifest);
            assert_eq!(rows.len() as u64, manifest.row_count);
            assert_eq!(
                rows,
                expected_rows("history", manifest.provider_symbol.as_str())
            );
            let history = config.history.as_ref().unwrap();
            assert!(
                rows.iter()
                    .all(|row| time(&history.start).unwrap() <= row.event_time_micros
                        && row.event_time_micros < time(&history.end).unwrap())
            );
            let pages: Vec<_> = manifest
                .day_inventory
                .iter()
                .filter(|d| d.family == binary_alpha_engine::dataset::DayFamily::Pages)
                .flat_map(|d| {
                    binary_alpha_app::daily::read_pages(
                        &scratch.path("published").join(d.object.as_ref().unwrap()),
                        &d.date,
                    )
                    .unwrap()
                })
                .collect();
            if kind == "deriv" {
                assert_eq!(pages.len(), 3);
            }
            for page in &pages {
                let raw = page.payload.as_slice();
                assert_eq!(
                    raw,
                    fs::read(scratch.path("retained").join(
                        binary_alpha_engine::dataset::object_key(&page.payload_sha256)
                    ))
                    .unwrap()
                );
                assert_eq!(hash(raw), page.payload_sha256);
                let expected = if kind == "deriv" {
                    let envelope: binary_alpha_app::broker::deriv::Envelope =
                        serde_json::from_slice(raw).unwrap();
                    deriv_history_response(
                        manifest.provider_symbol.as_str(),
                        page.request_token.as_deref().unwrap(),
                        envelope.req_id.unwrap(),
                    )
                } else {
                    #[derive(Deserialize)]
                    struct Page {
                        index: u64,
                    }
                    let response: Page = serde_json::from_slice(raw).unwrap();
                    let anchor: WireDecimal =
                        serde_json::from_str(page.request_token.as_deref().unwrap()).unwrap();
                    pocket_history_response(
                        manifest.provider_symbol.as_str(),
                        &anchor,
                        response.index,
                    )
                };
                assert_eq!(raw, expected.as_bytes());
            }
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
        let retained_before: std::collections::BTreeSet<_> =
            scratch.objects("retained").into_iter().collect();
        let second = command(&args);
        assert_eq!(
            second.matches("(already published)").count(),
            if kind == "deriv" { 2 } else { 0 }
        );
        let mut audited = manifests.clone();
        if kind == "pocket_option" {
            let all = read_manifests(&scratch);
            assert_eq!(
                all.len(),
                4,
                "both instruments publish new response evidence"
            );
            let new: Vec<_> = all
                .into_iter()
                .filter(|m| !manifests.iter().any(|old| old.generation == m.generation))
                .collect();
            assert_eq!(new.len(), 2);
            assert_eq!(
                new.iter()
                    .map(|m| &m.instrument)
                    .collect::<std::collections::BTreeSet<_>>(),
                manifests
                    .iter()
                    .map(|m| &m.instrument)
                    .collect::<std::collections::BTreeSet<_>>(),
                "one new generation for each original instrument"
            );
            let acquisitions: Vec<_> = scratch
                .objects("retained")
                .into_iter()
                .filter(|key| !retained_before.contains(key))
                .filter_map(|key| {
                    let bytes = fs::read(scratch.path("retained/objects").join(&key)).unwrap();
                    let value: serde_json::Value = serde_json::from_slice(&bytes).ok()?;
                    (value["kind"] == "acquisition" && value["instrument"].is_string())
                        .then_some((format!("objects/{key}"), value))
                })
                .collect();
            assert_eq!(acquisitions.len(), 2);
            let sent = sent.lock().unwrap();
            for manifest in &new {
                common::verify(&scratch.path("published").join(manifest.key())).unwrap();
                let old = manifests
                    .iter()
                    .find(|old| old.instrument == manifest.instrument)
                    .unwrap();
                let observations = |m: &GenerationManifest| {
                    m.objects
                        .iter()
                        .filter(|o| o.path.starts_with("observations/"))
                        .map(|o| (o.path.clone(), o.key.clone()))
                        .collect::<Vec<_>>()
                };
                assert_eq!(observations(manifest), observations(old));
                assert_eq!(manifest.row_count, old.row_count);
                assert_eq!(
                    common::read_normalized_ticks(&scratch.path("published"), manifest),
                    common::read_normalized_ticks(&scratch.path("published"), old)
                );
                let acquisition = &acquisitions
                    .iter()
                    .find(|(_, value)| value["instrument"] == manifest.instrument)
                    .unwrap()
                    .0;
                let pages = common::daily::pages(&scratch.path("published"), manifest);
                let mut received: Vec<_> = pages
                    .iter()
                    .filter(|p| &p.acquisition_id == acquisition)
                    .collect();
                received.sort_by_key(|p| p.ordinal);
                let expected: Vec<_> = sent
                    .iter()
                    .filter(|(invocation, symbol, _)| {
                        *invocation == 1 && symbol == manifest.provider_symbol.as_str()
                    })
                    .map(|(_, _, bytes)| bytes)
                    .collect();
                assert!(!expected.is_empty());
                assert_eq!(received.len(), expected.len());
                for (ordinal, (page, bytes)) in received.iter().zip(expected).enumerate() {
                    assert_eq!(page.ordinal, ordinal as u64);
                    assert_eq!(&page.payload, bytes);
                    assert_eq!(page.payload_sha256, hash(bytes));
                    assert_eq!(
                        page.disposition,
                        binary_alpha_app::daily::PageDisposition::Indexed
                    );
                    assert!(page.receipt_time_utc.is_some());
                }
                let old_pages = common::daily::pages(&scratch.path("published"), old);
                assert_eq!(pages.len(), old_pages.len() + received.len());
                for page in old_pages {
                    assert!(pages.contains(&page), "prior occurrence lost");
                }
            }
            audited.extend(new);
            assert!(scratch.objects("published").len() > objects.len());
        } else {
            assert_eq!(objects, scratch.objects("published"));
        }
        for (path, bytes) in before {
            assert_eq!(fs::read(path).unwrap(), bytes);
        }
        for manifest in &audited {
            let report = command(&[
                "data",
                "audit",
                "--config",
                config_path.to_str().unwrap(),
                "--manifest",
                &format!(
                    "file://{}",
                    scratch.path("published").join(manifest.key()).display()
                ),
            ]);
            assert!(report.contains("candles"), "{report}");
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
        let history_counts = report
            .checks
            .iter()
            .filter(|check| check.name == "history")
            .map(|check| check.detail.rows.unwrap())
            .collect::<Vec<_>>();
        assert_eq!(
            history_counts,
            if kind == "deriv" {
                vec![100, 100]
            } else {
                vec![1478, 3]
            }
        );
        let live = report.checks.iter().find(|c| c.name == "live").unwrap();
        assert_eq!(
            live.detail
                .counts
                .as_ref()
                .unwrap()
                .values()
                .copied()
                .collect::<Vec<_>>(),
            vec![2, 2]
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
    let mut unsupported = test_config(&scratch, "pocket_option", "ws://127.0.0.1/", false);
    unsupported.inspect.as_mut().unwrap().proposal = allowed.inspect.unwrap().proposal;
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
        sent.lock()
            .unwrap()
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

    let first = page(&[(5, 100), (5, 100), (6, 101)]);
    fetch::pass(
        &config,
        &mut Pages::new(vec![first.clone(), page(&[])]),
        &local,
        &destination,
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
        (0, 10_000_000),
        &mut out,
    )
    .unwrap();
    let manifests = read_manifests(&scratch);
    assert_eq!(manifests.len(), 2);
    assert!(scratch.objects("published").len() > count);
    let observations = |m: &GenerationManifest| {
        m.objects
            .iter()
            .filter(|o| o.path.starts_with("observations/"))
            .map(|o| o.key.clone())
            .collect::<Vec<_>>()
    };
    assert_eq!(observations(&manifests[0]), observations(&manifests[1]));
    assert!(manifests.iter().all(|m| m.row_count == 3));
    for manifest in manifests {
        verify::run(&destination.uri(&manifest.key())).unwrap();
    }
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
        matches!(sent.lock().unwrap().last().unwrap(), Frame::Text(text) if text.contains("\"forget\":\"synthetic-resubscribed\""))
    );
}

#[test]
fn history_resume_is_bound_to_the_declared_provider_clock() {
    let scratch = Scratch::new("phase10_source_clock");
    let mut config = test_config(&scratch, "pocket_option", "ws://127.0.0.1/", false);
    let (local, destination) = stores(&scratch);

    let mut out = Vec::new();
    fetch::pass(
        &config,
        &mut Pages::new(vec![range_page(0, 10)]),
        &local,
        &destination,
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
        (0, 10_000_000),
        &mut out,
    )
    .unwrap();
    assert_eq!(pages.anchors, [Some(10_000_000)]);
    let manifests = read_manifests(&scratch);
    assert_eq!(manifests.len(), 2);
    let new = manifests
        .iter()
        .find(|manifest| manifest.generation != original.generation)
        .unwrap();
    assert_ne!(source, read_coverage(&scratch, new).source_identity);
}

#[test]
fn prefix_repair_preserves_verified_rows_and_repeats() {
    for consistent in [false, true] {
        let scratch = Scratch::new(&format!("phase10_prefix_repair_{consistent}"));
        let config = test_config(&scratch, "deriv", "ws://127.0.0.1/", false);
        let (local, destination) = stores(&scratch);
        let verified = [(5, 100), (5, 100), (6, 101), (7, 102)];
        let mut initial = Pages::new(vec![page(&verified), page(&[])]);

        fetch::pass(
            &config,
            &mut initial,
            &local,
            &destination,
            (0, 10_000_000),
            &mut Vec::new(),
        )
        .unwrap();
        let before = scratch.manifests("published");
        let repair = if consistent {
            vec![(0, 90), (5, 100), (5, 100), (6, 101), (7, 102)]
        } else {
            vec![(0, 90), (5, 100), (7, 102)]
        };
        let mut pages = Pages::new(vec![page(&repair)]);
        let result = fetch::pass(
            &config,
            &mut pages,
            &local,
            &destination,
            (0, 10_000_000),
            &mut Vec::new(),
        );
        if consistent {
            result.unwrap();
            let manifests = read_manifests(&scratch);
            let manifest = manifests.last().unwrap();
            assert_eq!(manifest.row_count, 5);
            let rows = common::read_normalized_ticks(&scratch.path("published"), manifest);
            assert_eq!(rows, rows_of(&repair));
            assert!(
                verify::run(&destination.uri(&manifest.key()))
                    .unwrap()
                    .contains("rows 5")
            );
        } else {
            assert!(result.unwrap_err().contains("inconsistent reread"));
            assert_eq!(scratch.manifests("published"), before);
        }
    }
}

#[test]
fn reused_history_is_published_to_the_current_destination() {
    // Set the store-log environment only on an isolated test process. Other parallel tests
    // keep their own environment, and the fake broker remains entirely in memory.
    if std::env::var_os("BINARY_ALPHA_FETCH_REUSE_CHILD").is_none() {
        let driver = Scratch::new("phase10_reuse_driver");
        let result = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "reused_history_is_published_to_the_current_destination",
                "--nocapture",
            ])
            .env("BINARY_ALPHA_FETCH_REUSE_CHILD", "1")
            .env("BINARY_ALPHA_STORE_LOG", driver.path("access.log"))
            .output()
            .unwrap();
        assert!(
            result.status.success(),
            "{}\n{}",
            String::from_utf8_lossy(&result.stdout),
            String::from_utf8_lossy(&result.stderr)
        );
        return;
    }
    let scratch = Scratch::new("phase10_destination_switch");
    let mut config = test_config(&scratch, "deriv", "ws://127.0.0.1/", false);
    let (local, first) = stores(&scratch);
    let second = Store::filesystem(scratch.path("destination-b"));

    fetch::pass(
        &config,
        &mut Pages::new(vec![range_page(0, 10)]),
        &local,
        &first,
        (0, 9_000_001),
        &mut Vec::new(),
    )
    .unwrap();
    config.storage.publication_uri = second.uri("").parse().unwrap();
    let mut no_refetch = Pages::new(vec![]);
    fetch::pass(
        &config,
        &mut no_refetch,
        &local,
        &second,
        (0, 9_000_001),
        &mut Vec::new(),
    )
    .unwrap();
    assert!(no_refetch.anchors.is_empty());
    let manifest = read_manifests(&scratch).remove(0);
    for object in &manifest.objects {
        assert!(second.head(&object.key).unwrap().is_some());
    }
    assert!(second.head(&manifest.key()).unwrap().is_some());
    assert!(
        verify::run(&second.uri(&manifest.key()))
            .unwrap()
            .contains("rows 10")
    );

    use binary_alpha_engine::research::{Declaration, Population, to_json};
    let mut declaration = Declaration {
        schema_version: 1,
        operator: "synthetic-operator".into(),
        root: format!("file://{}", scratch.path("governance").display())
            .parse()
            .unwrap(),
        namespace: "fetch-reuse".into(),
        populations: vec![Population {
            id: "history".into(),
            role: DatasetRole::Development,
            instrument: manifest.instrument.clone(),
            source: "synthetic-pages".into(),
            coverage: manifest.coverage.clone(),
            generations: vec![manifest.generation.clone()],
            tokens: vec!["history-observations".into()],
            exposure: vec![],
        }],
    };
    let declaration_path = scratch.path("declaration.json");
    config.research = research_fixture::configuration(&scratch.root).research;
    let log = std::path::PathBuf::from(std::env::var_os("BINARY_ALPHA_STORE_LOG").unwrap());
    for permitted in [true, false] {
        if !permitted {
            declaration.populations.clear();
        }
        fs::write(&declaration_path, to_json(&declaration)).unwrap();
        let destination =
            Store::filesystem(scratch.path(if permitted { "permitted" } else { "undeclared" }));
        config.storage.publication_uri = destination.uri("").parse().unwrap();
        let refreshed_rows: Vec<(i64, i64)> =
            (0..10).map(|second| (second, 200_000 + second)).collect();
        let refreshed = page(&refreshed_rows);
        let mut pages = Pages::new(if permitted {
            vec![]
        } else {
            vec![refreshed.clone()]
        });
        fs::write(&log, []).unwrap();
        fetch::pass(
            &config,
            &mut pages,
            &local,
            &destination,
            (0, 9_000_001),
            &mut Vec::new(),
        )
        .unwrap();
        let accesses = fs::read_to_string(&log).unwrap();
        assert_eq!(
            pages.anchors,
            if permitted {
                vec![]
            } else {
                vec![Some(9_000_001)]
            }
        );
        assert_eq!(
            accesses
                .lines()
                .any(|line| line == format!("read_to {}", manifest.key())),
            permitted
        );
        if !permitted {
            // The undeclared prior is discovered by the logged listing only: no metadata, read,
            // or local-path operation names it.
            assert!(
                accesses
                    .lines()
                    .filter(|line| line.contains(&manifest.generation))
                    .all(|line| line.starts_with("probe ")),
                "{accesses}"
            );
        }
        if permitted {
            assert_eq!(
                fs::read(destination.local_path(&manifest.key()).unwrap()).unwrap(),
                manifest.to_json()
            );
            for object in &manifest.objects {
                assert_eq!(
                    fs::read(destination.local_path(&object.key).unwrap()).unwrap(),
                    fs::read(first.local_path(&object.key).unwrap()).unwrap()
                );
            }
            assert!(
                verify::run(&destination.uri(&manifest.key()))
                    .unwrap()
                    .contains("rows 10")
            );
        } else {
            let root = destination.local_path("manifests").unwrap();
            let entries = fs::read_dir(root)
                .unwrap()
                .map(|e| e.unwrap().path().join("ready.json"))
                .collect::<Vec<_>>();
            assert_eq!(entries.len(), 1);
            let fresh = GenerationManifest::from_json(&fs::read(&entries[0]).unwrap()).unwrap();
            assert_ne!(fresh.generation, manifest.generation);
            assert_eq!(
                common::read_normalized_ticks(&destination.local_path("").unwrap(), &fresh),
                rows_of(&refreshed_rows)
            );
            assert!(
                verify::run(&destination.uri(&fresh.key()))
                    .unwrap()
                    .contains("rows 10")
            );
        }
    }
}

#[test]
fn inspection_reports_actual_counts_during_uneven_arrivals() {
    let scratch = Scratch::new("phase10_inspection_uneven");
    let mut config = test_config(&scratch, "deriv", "ws://127.0.0.1/", true);
    config.inspect.as_mut().unwrap().live_observations = 1;
    let mut clock = FakeClock::default();
    let frames = vec![
        frame("deriv-rate-limit.json", 1),
        frame("deriv-contracts_for-R_50.json", 2),
        frame("deriv-history-R_50.json", 3),
        frame("deriv-contracts_for-R_100.json", 4),
        frame("deriv-history-R_100.json", 5),
        frame("deriv-tick-R_50.json", 6),
        frame("deriv-tick-R_50.json", 6),
        frame("deriv-tick-R_50.json", 6),
        frame("deriv-tick-R_100.json", 7),
        frame("deriv-forget.json", 8),
    ];
    let (connector, _) = connector(vec![frames], &clock);
    let mut adapter = Adapter::Deriv(
        DerivMarketData::connect(&deriv_settings(), connector, Box::new(clock.clone())).unwrap(),
    );
    let report = binary_alpha_app::inspect::observe(&config, &mut adapter, &mut clock).unwrap();
    let live = report
        .checks
        .iter()
        .find(|check| check.name == "live")
        .unwrap();
    let binary_alpha_app::inspect::InspectionDetail::Live { counts } = &live.detail else {
        panic!("{live:?}")
    };
    assert_eq!(live.result, "verified");
    assert_eq!(
        counts,
        &BTreeMap::from([("deriv:R_50".into(), 3), ("deriv:R_100".into(), 1)])
    );
}

#[test]
fn pocket_period_pins_and_retained_anchor_text_are_exact() {
    for token in ["1789354492.749", "1789354293.216"] {
        let wire: WireDecimal = serde_json::from_str(token).unwrap();
        let universal = universal_micros(&wire, 120).unwrap();
        assert_eq!(
            provider_token(universal, 120).unwrap().token().unwrap(),
            token
        );
    }
    for (file, event, anchor) in [
        (
            "pocket-history-initial-period60.json",
            "updateHistoryNewFast",
            None,
        ),
        (
            "pocket-history-older-period60.json",
            "loadHistoryPeriod",
            Some(1_789_347_292_749_000),
        ),
    ] {
        let mut frames = handshake();
        frames.extend(attachment(event, replace(&fixture(file), "index", "0")));
        let (mut broker, _) = pocket(frames);
        let error = broker
            .history_page(&pocket_ids()[0], scale(5), anchor, NativeGranularity::Tick)
            .unwrap_err();
        assert!(
            error.contains(event) && error.contains("period 60"),
            "{error}"
        );
    }
}

#[test]
fn concrete_deriv_tail_shortfall_is_requested_again() {
    let scratch = Scratch::new("phase10_deriv_tail");
    let config = test_config(&scratch, "deriv", "ws://127.0.0.1/", false);
    let (local, destination) = stores(&scratch);
    let clock = FakeClock::default();
    let body = r#"{"msg_type":"history","pip_size":4,"history":{"prices":[1,1,1,1,1],"times":[0,1,2,3,4]}}"#;
    let (connector, sent) = connector(
        vec![vec![
            Frame::Text(correlated(body, 1)),
            Frame::Text(correlated(body, 2)),
        ]],
        &clock,
    );
    let mut adapter =
        DerivMarketData::connect(&deriv_settings(), connector, Box::new(clock)).unwrap();
    let mut out = Vec::new();
    fetch::pass(
        &config,
        &mut adapter,
        &local,
        &destination,
        (0, 10_000_000),
        &mut out,
    )
    .unwrap();
    let manifests = read_manifests(&scratch);
    let coverage = read_coverage(&scratch, &manifests[0]);
    assert_eq!(
        coverage.verified,
        Some(fetch::Range {
            start: time_text(0),
            end: time_text(4_000_001)
        })
    );
    assert_eq!(
        coverage.shortfall,
        Some(fetch::Shortfall {
            reason: "unresolved_tail".into(),
            unresolved: fetch::Range {
                start: time_text(4_000_001),
                end: time_text(10_000_000)
            }
        })
    );
    fetch::pass(
        &config,
        &mut adapter,
        &local,
        &destination,
        (0, 10_000_000),
        &mut out,
    )
    .unwrap();
    assert_eq!(read_manifests(&scratch).len(), 2);
    assert_eq!(
        sent.lock().unwrap().len(),
        2,
        "the incomplete tail must request evidence again"
    );
    for frame in sent.lock().unwrap().iter() {
        assert!(matches!(frame, Frame::Text(text) if text.contains("\"end\":\"10\"")));
    }
}

#[test]
fn concrete_adapters_resume_interrupted_publication_and_page_backward() {
    for kind in ["deriv", "pocket_option"] {
        let scratch = Scratch::new(&format!("phase10_concrete_resume_{kind}"));
        let config = test_config(&scratch, kind, "ws://127.0.0.1/", false);
        let (local, destination) = stores(&scratch);
        let clock = FakeClock::at(1_789_348_010_000_000);
        let (range, history_frames, raw_pages) = if kind == "deriv" {
            let mut end = "1789346760".to_string();
            let mut frames = Vec::new();
            let mut raw_pages = Vec::new();
            for request in 1..=3 {
                let raw = deriv_history_response("R_50", &end, request);
                #[derive(Deserialize)]
                struct Response {
                    history: Times,
                }
                #[derive(Deserialize)]
                struct Times {
                    times: Vec<i64>,
                }
                let response: Response = serde_json::from_str(&raw).unwrap();
                end = response.history.times[0].to_string();
                raw_pages.push(raw.clone());
                frames.push(Frame::Text(raw));
            }
            (
                (1_789_346_562_000_000, 1_789_346_760_000_001),
                frames,
                raw_pages,
            )
        } else {
            let first = replace(&fixture("pocket-history-older-1.json"), "index", "0");
            let second = replace(&fixture("pocket-history-older-2.json"), "index", "1");
            let mut frames = attachment("loadHistoryPeriod", first.clone());
            frames.extend(attachment("loadHistoryPeriod", second.clone()));
            (
                (1_789_346_894_149_000, 1_789_347_292_749_001),
                frames,
                vec![first, second],
            )
        };
        let create = || {
            // Each simulated process starts at this scenario's epoch, keeping retained raw
            // response identities stable across the interrupted-publication retry.
            let clock = FakeClock::at(clock.now_micros());
            let mut frames = if kind == "pocket_option" {
                handshake()
            } else {
                vec![]
            };
            frames.extend(history_frames.clone());
            let (connector, sent) = if kind == "pocket_option" {
                pocket_connector(vec![frames], &clock)
            } else {
                connector(vec![frames], &clock)
            };
            let adapter = if kind == "deriv" {
                Adapter::Deriv(
                    DerivMarketData::connect(&deriv_settings(), connector, Box::new(clock.clone()))
                        .unwrap(),
                )
            } else {
                Adapter::PocketOption(
                    PocketMarketData::connect(
                        &pocket_settings(),
                        &pocket_ids(),
                        connector,
                        Box::new(clock.clone()),
                        "{}".into(),
                    )
                    .unwrap(),
                )
            };
            (adapter, sent)
        };
        let failed_object = scratch.path("published/objects");
        fs::create_dir_all(scratch.path("published")).unwrap();
        fs::write(&failed_object, b"synthetic interrupted publication").unwrap();
        let (mut first, first_sent) = create();
        assert!(
            fetch::pass(
                &config,
                first.market(),
                &local,
                &destination,
                range,
                &mut Vec::new()
            )
            .is_err()
        );
        assert!(read_manifests(&scratch).is_empty());
        let raw_pages: Vec<_> = raw_pages
            .iter()
            .map(|raw| {
                if kind == "pocket_option" {
                    pocket_response(raw, &first_sent)
                } else {
                    raw.clone()
                }
            })
            .collect();
        let retained = fs::read(
            scratch
                .path("retained/objects")
                .join(hash(raw_pages[0].as_bytes())),
        )
        .unwrap();
        fs::remove_file(&failed_object).unwrap();
        let (mut resumed, sent) = create();
        fetch::pass(
            &config,
            resumed.market(),
            &local,
            &destination,
            range,
            &mut Vec::new(),
        )
        .unwrap();
        let manifest = read_manifests(&scratch).remove(0);
        assert!(
            verify::run(&destination.uri(&manifest.key()))
                .unwrap()
                .contains("verified")
        );
        assert_eq!(
            retained,
            fs::read(
                scratch
                    .path("retained/objects")
                    .join(hash(raw_pages[0].as_bytes()))
            )
            .unwrap()
        );
        let coverage = read_coverage(&scratch, &manifest);
        assert_eq!(
            coverage.verified,
            Some(fetch::Range {
                start: time_text(range.0),
                end: time_text(range.1)
            })
        );
        assert!(coverage.shortfall.is_none());
        let rows = common::read_normalized_ticks(&scratch.path("published"), &manifest);
        if kind == "deriv" {
            assert_eq!(rows, expected_rows("history", "R_50"));
            let requests = sent.lock().unwrap();
            let anchors = requests
                .iter()
                .filter_map(|frame| match frame {
                    Frame::Text(text) => {
                        let fields: BTreeMap<String, Box<RawValue>> =
                            serde_json::from_str(text).unwrap();
                        Some(field::<String>(&fields, "end"))
                    }
                    _ => None,
                })
                .collect::<Vec<_>>();
            assert_eq!(anchors, ["1789346760", "1789346682", "1789346604"]);
        } else {
            assert_eq!(rows.len(), 820);
            assert_eq!(rows.first().unwrap().event_time_micros, range.0);
            assert_eq!(rows.last().unwrap().event_time_micros, range.1 - 1);
            assert!(sent.lock().unwrap().iter().any(|frame| matches!(frame, Frame::Text(text) if text.contains("\"time\":1789354293.216"))));
        }
    }
}

#[test]
fn reconnect_rebuilds_causal_stream_from_verified_history_before_live_finalization() {
    let scratch = Scratch::new("phase10_reconnect_warmup");
    let config = test_config(&scratch, "deriv", "ws://127.0.0.1/", false);
    let (local, destination) = stores(&scratch);
    let clock = FakeClock::at(1_789_348_010_000_000);
    let (connector, _) = connector(
        vec![
            vec![shifted_tick("deriv-tick-R_50.json", 1, 0)],
            vec![
                frame("deriv-history-R_50.json", 1),
                shifted_tick("deriv-tick-R_50.json", 2, 0),
            ],
        ],
        &clock,
    );
    let mut adapter =
        DerivMarketData::connect(&deriv_settings(), connector, Box::new(clock)).unwrap();
    adapter.subscribe(&id("deriv", "R_50"), scale(4)).unwrap();
    let before = observation(adapter.next_live(1_000_000).unwrap());
    adapter.reconnect().unwrap();
    assert!(matches!(
        adapter.next_live(1_000_000).unwrap(),
        Some(LiveEvent::Break { generation: 1, .. })
    ));
    fetch::pass(
        &config,
        &mut adapter,
        &local,
        &destination,
        (1_789_346_562_000_000, 1_789_346_760_000_001),
        &mut Vec::new(),
    )
    .unwrap();
    let manifest = read_manifests(&scratch).remove(0);
    verify::run(&destination.uri(&manifest.key())).unwrap();
    let definition = &config.instruments[0];
    let mut warmed = InstrumentStream::new(definition, stream_source(scale(4))).unwrap();
    let mut candles = Vec::new();
    for tick in common::read_normalized_ticks(&scratch.path("published"), &manifest) {
        warmed.push(Observation::Tick(tick), &mut candles).unwrap();
    }
    let history_count = candles.len();
    adapter.subscribe(&definition.id(), scale(4)).unwrap();
    let live = observation(adapter.next_live(1_000_000).unwrap());
    assert_eq!(
        (before.generation, live.generation, live.sequence),
        (0, 1, 1)
    );
    let tick = Observation::Tick(Tick {
        event_time_micros: live.provider_time_micros,
        price_units: live.price_units,
    });
    let mut cold = InstrumentStream::new(definition, stream_source(scale(4))).unwrap();
    let mut cold_candles = Vec::new();
    cold.push(tick, &mut cold_candles).unwrap();
    assert!(
        cold_candles.is_empty(),
        "live rows alone have not warmed a completed candle"
    );
    warmed.push(tick, &mut candles).unwrap();
    assert_eq!(candles.len(), history_count + 1);
    let candle = &candles.last().unwrap().1;
    assert_eq!(candle.open_time_micros, 1_789_346_760_000_000);
    assert_eq!(candle.close_time_micros, 1_789_346_761_000_000);
    assert_eq!(candle.known_at_micros, live.provider_time_micros);
}

#[test]
fn received_upper_boundary_caps_coverage_without_publishing_out_of_range_rows() {
    for last in [9_999_999, 10_000_000, 11_000_000] {
        let scratch = Scratch::new(&format!("phase10_received_upper_bound_{last}"));
        let config = test_config(&scratch, "deriv", "ws://127.0.0.1/", false);
        let (local, destination) = stores(&scratch);
        let input = page_micros(&[(0, 1), (4_000_000, 1), (last, 1)]);
        let mut pages = Pages::new(vec![input]);
        fetch::pass(
            &config,
            &mut pages,
            &local,
            &destination,
            (0, 10_000_000),
            &mut Vec::new(),
        )
        .unwrap();
        let manifest = read_manifests(&scratch).remove(0);
        let coverage = read_coverage(&scratch, &manifest);
        assert_eq!(coverage.verified.unwrap().end, time_text(10_000_000));
        assert!(coverage.shortfall.is_none());
        assert!(
            common::read_normalized_ticks(&scratch.path("published"), &manifest)
                .iter()
                .all(|row| row.event_time_micros < 10_000_000)
        );
    }
}

#[test]
fn repeated_boundary_observations_split_across_repair_pages_keep_multiplicity() {
    let repeated = [(5, 100_005), (5, 100_005), (6, 100_006), (7, 100_007)];
    let older = [
        (0, 100_000),
        (1, 100_001),
        (2, 100_002),
        (3, 100_003),
        (4, 100_004),
        (5, 100_005),
        (5, 100_005),
    ];
    // The repeat at second 5 arrives split across the repair's page boundary, or whole in one page.
    let split = vec![
        page(&[
            (5, 100_005),
            (6, 100_006),
            (7, 100_007),
            (8, 100_008),
            (9, 100_009),
        ]),
        page(&older),
    ];
    let whole = vec![
        page(&[
            (5, 100_005),
            (5, 100_005),
            (6, 100_006),
            (7, 100_007),
            (8, 100_008),
            (9, 100_009),
        ]),
        page(&older[..5]),
    ];
    for (name, repair) in [("split", split), ("whole", whole)] {
        let scratch = Scratch::new(&format!("phase10_boundary_repeat_{name}"));
        let config = test_config(&scratch, "deriv", "ws://127.0.0.1/", false);
        let (local, destination) = stores(&scratch);
        let pass = |pages: Vec<HistoryPage>| {
            fetch::pass(
                &config,
                &mut Pages::new(pages),
                &local,
                &destination,
                (0, 10_000_000),
                &mut Vec::new(),
            )
        };
        pass(vec![page(&repeated), page(&[])]).unwrap();
        pass(repair).unwrap();
        let manifests = read_manifests(&scratch);
        assert_eq!(manifests.len(), 2, "{name}");
        let repaired = read_coverage(&scratch, &manifests[1]);
        assert_eq!(repaired.rows, 11, "{name}");
        let rows = common::read_normalized_ticks(&scratch.path("published"), &manifests[1]);
        assert_eq!(
            rows.iter()
                .filter(|row| row.event_time_micros == 5_000_000)
                .count(),
            2,
            "{name}"
        );
        assert_eq!(
            repaired.verified,
            Some(fetch::Range {
                start: time_text(0),
                end: time_text(9_000_001)
            }),
            "{name}"
        );
    }
}

#[test]
fn prefix_repair_preserves_verified_end_and_restart_rejects_changed_prefix() {
    let scratch = Scratch::new("phase10_monotonic_repair");
    let config = test_config(&scratch, "deriv", "ws://127.0.0.1/", false);
    let (local, destination) = stores(&scratch);
    let pass = |pages: &mut Pages| {
        fetch::pass(
            &config,
            pages,
            &local,
            &destination,
            (0, 10_000_000),
            &mut Vec::new(),
        )
    };
    pass(&mut Pages::new(vec![range_page(5, 8), page(&[])])).unwrap();
    let manifests = read_manifests(&scratch);
    let initial = read_coverage(&scratch, &manifests[0]);
    assert_eq!(
        initial.verified,
        Some(fetch::Range {
            start: time_text(5_000_000),
            end: time_text(7_000_001)
        })
    );
    assert_eq!(
        initial.shortfall.unwrap().unresolved,
        fetch::Range {
            start: time_text(0),
            end: time_text(5_000_000)
        }
    );
    assert_eq!(
        initial.tail_shortfall.unwrap().unresolved,
        fetch::Range {
            start: time_text(7_000_001),
            end: time_text(10_000_000)
        }
    );
    pass(&mut Pages::new(vec![range_page(0, 7)])).unwrap();
    let manifests = read_manifests(&scratch);
    assert_eq!(manifests.len(), 2);
    let repaired = read_coverage(&scratch, &manifests[1]);
    assert_eq!(
        repaired.verified,
        Some(fetch::Range {
            start: time_text(0),
            end: time_text(7_000_001)
        })
    );
    assert_eq!(repaired.rows, 8);
    assert_eq!(
        common::read_normalized_ticks(&scratch.path("published"), &manifests[1]),
        rows_of(&range_rows(0, 8))
    );
    assert_eq!(
        repaired.shortfall,
        Some(fetch::Shortfall {
            reason: "unresolved_tail".into(),
            unresolved: fetch::Range {
                start: time_text(7_000_001),
                end: time_text(10_000_000)
            }
        })
    );
    assert!(repaired.tail_shortfall.is_none());
    let before = scratch
        .manifests("published")
        .iter()
        .map(|path| (path.clone(), fs::read(path).unwrap()))
        .collect::<Vec<_>>();
    // A new pass reloads prior generations: equal ends must choose the repaired start.
    let mut changed = (0..10)
        .map(|second| (second, 100_000 + second))
        .collect::<Vec<_>>();
    changed[2].1 = 999_999;
    let error = pass(&mut Pages::new(vec![page(&changed)])).unwrap_err();
    assert!(error.contains("reread of verified observations"), "{error}");
    assert_eq!(read_manifests(&scratch).len(), 2);
    for (path, bytes) in before {
        assert_eq!(fs::read(path).unwrap(), bytes);
    }
    for manifest in &manifests {
        assert!(
            verify::run(&destination.uri(&manifest.key()))
                .unwrap()
                .starts_with("verified")
        );
    }
    // Consistent overlap below the resume boundary remains accepted and retains every row.
    pass(&mut Pages::new(vec![range_page(0, 11)])).unwrap();
    let manifests = read_manifests(&scratch);
    assert_eq!(manifests.len(), 3);
    let complete = read_coverage(&scratch, &manifests[2]);
    assert_eq!(
        complete.verified,
        Some(fetch::Range {
            start: time_text(0),
            end: time_text(10_000_000)
        })
    );
    assert!(complete.shortfall.is_none());
    assert_eq!(
        common::read_normalized_ticks(&scratch.path("published"), &manifests[2]),
        rows_of(&range_rows(0, 10))
    );
}

#[test]
fn fetch_refuses_v1_seed_and_prior_before_requesting_any_page() {
    use binary_alpha_engine::dataset::{ObjectRole, SourceKind};
    for seed in [false, true] {
        let scratch = Scratch::new(&format!("no_v1_fetch_{seed}"));
        let pair = common::daily::pair(&scratch, false);
        let mut config = test_config(&scratch, "deriv", "ws://127.0.0.1/", false);
        let root = scratch.path("published");
        let local = Store::filesystem(&root);
        let destination = Store::filesystem(scratch.path("refused"));
        let source_identity = broker::source_identity(&config.brokers[0]);
        if seed {
            config
                .history
                .as_mut()
                .unwrap()
                .seeds
                .push(binary_alpha_engine::config::Seed {
                    provider_symbol: pair.v1.provider_symbol.clone(),
                    manifest: common::daily::uri(&pair.path(&scratch, false))
                        .parse()
                        .unwrap(),
                    source_identity,
                });
        } else {
            let mut prior = pair.v1.clone();
            prior.source_kind = SourceKind::BrokerHistory;
            let coverage = fetch::HistoryCoverage {
                schema_version: 1,
                source_identity,
                broker: prior.broker.to_string(),
                provider_symbol: prior.provider_symbol.to_string(),
                role: prior.role,
                requested: fetch::Range {
                    start: prior.coverage.first_event_time.clone(),
                    end: prior.coverage.last_event_time.clone(),
                },
                verified: None,
                actual: Some(fetch::Actual {
                    first: prior.coverage.first_event_time.clone(),
                    last: prior.coverage.last_event_time.clone(),
                }),
                rows: prior.row_count,
                pages: vec![],
                bundle: None,
                shortfall: None,
                tail_shortfall: None,
                native_granularity: prior.native_granularity,
                seed: None,
            };
            let file = scratch.path("legacy-coverage.json");
            fs::write(&file, serde_json::to_vec(&coverage).unwrap()).unwrap();
            prior.objects.retain(|o| o.path != fetch::COVERAGE_PATH);
            prior.objects.push(common::daily::object(
                &root,
                fetch::COVERAGE_PATH,
                ObjectRole::Provenance,
                &file,
            ));
            let raw = scratch.path("legacy-raw.json");
            fs::write(&raw, b"{}").unwrap();
            prior.objects.push(common::daily::object(
                &root,
                "raw/fixture.json",
                ObjectRole::Source,
                &raw,
            ));
            common::daily::publish(&root, &mut prior);
        }
        let mut pages = Pages::new(vec![]);
        let error = fetch::pass(
            &config,
            &mut pages,
            &local,
            &destination,
            (0, 10_000_000),
            &mut Vec::new(),
        )
        .unwrap_err();
        assert!(error.contains("data pipeline migrate"), "{error}");
        assert!(pages.anchors.is_empty());
        assert!(!scratch.path("refused/manifests").exists());
    }
}

#[test]
fn unseeded_multiday_fetch_publishes_daily_rows_and_exact_page_payloads() {
    let scratch = Scratch::new("unseeded_multiday_daily");
    let config = test_config(&scratch, "deriv", "ws://127.0.0.1/", false);
    let (local, destination) = stores(&scratch);
    let rows = [
        (86_399_000_000, 100),
        (172_800_000_000, 101),
        (172_800_000_000, 101),
        (172_801_000_000, 102),
    ];
    let page = page_micros(&rows);
    let raw = page.raw.clone();
    fetch::pass(
        &config,
        &mut Pages::new(vec![page]),
        &local,
        &destination,
        (86_399_000_000, 172_801_000_001),
        &mut Vec::new(),
    )
    .unwrap();
    let manifest = read_manifests(&scratch).remove(0);
    assert_eq!(
        manifest.layout,
        Some(binary_alpha_engine::dataset::Layout::DailyV2)
    );
    assert_eq!(
        common::read_normalized_ticks(&scratch.path("published"), &manifest),
        rows.iter()
            .map(|(t, p)| Tick {
                event_time_micros: *t,
                price_units: *p
            })
            .collect::<Vec<_>>()
    );
    let observations: Vec<_> = manifest
        .day_inventory
        .iter()
        .filter(|d| d.family == binary_alpha_engine::dataset::DayFamily::Observations)
        .collect();
    assert_eq!(
        observations
            .iter()
            .map(|d| (&*d.date, d.rows))
            .collect::<Vec<_>>(),
        [("1970-01-01", 1), ("1970-01-02", 0), ("1970-01-03", 3)]
    );
    let pages: Vec<_> = manifest
        .day_inventory
        .iter()
        .filter(|d| d.family == binary_alpha_engine::dataset::DayFamily::Pages)
        .collect();
    assert_eq!(pages.len(), 1);
    assert_eq!(pages[0].date, "1970-01-03");
    assert_eq!(
        observations[1].state,
        binary_alpha_engine::dataset::DayState::EmptyKnown
    );
    assert!(observations[1].object.is_none());
    let occurrences = binary_alpha_app::daily::read_pages(
        &scratch
            .path("published")
            .join(pages[0].object.as_ref().unwrap()),
        &pages[0].date,
    )
    .unwrap();
    assert_eq!(occurrences.len(), 1);
    assert_eq!(occurrences[0].payload, raw);
    assert_eq!(occurrences[0].rows, 4);
    assert!(!occurrences[0].acquisition_id.is_empty());
    assert!(
        manifest
            .objects
            .iter()
            .all(|o| !o.path.starts_with("raw/") && !o.path.starts_with("normalized/"))
    );
    verify::run(&destination.uri(&manifest.key())).unwrap();
}

#[test]
fn unseeded_multiday_bar_fetch_preserves_provider_columns_and_audits_daily() {
    let scratch = Scratch::new("unseeded_multiday_daily_bars");
    let mut config = test_config(&scratch, "pocket_option", "ws://127.0.0.1/", false);
    let native = NativeGranularity::Bar { period_seconds: 5 };
    config.history.as_mut().unwrap().native_granularity = native;
    config.instruments[0].native_granularity = native;
    for candle in &mut config.instruments[0].candles {
        candle.duration_seconds = 5;
        candle.offset_seconds = 0;
    }
    let (local, destination) = stores(&scratch);
    let rows = [
        (86_395_000_000, 100_000),
        (86_400_000_000, 100_100),
        (86_405_000_000, 100_200),
    ];
    fetch::pass(
        &config,
        &mut Pages::new(vec![page_micros(&rows)]),
        &local,
        &destination,
        (86_395_000_000, 86_410_000_000),
        &mut Vec::new(),
    )
    .unwrap();
    let manifest = read_manifests(&scratch).remove(0);
    assert_eq!(
        manifest.layout,
        Some(binary_alpha_engine::dataset::Layout::DailyV2)
    );
    let mut actual = Vec::new();
    binary_alpha_app::daily::read_generation_lossless(&destination, &manifest, |row| {
        actual.push(row);
        Ok(())
    })
    .unwrap();
    assert_eq!(actual.len(), rows.len());
    let Broker::PocketOption(settings) = &config.brokers[0] else {
        unreachable!()
    };
    for (row, (at, units)) in actual.iter().zip(rows) {
        let binary_alpha_app::daily::LosslessRow::Bar(bar) = row else {
            panic!("bar row expected")
        };
        assert_eq!(bar.symbol.as_deref(), Some("EURUSD_otc"));
        assert_eq!(bar.symbol_id, Some(538));
        assert_eq!(bar.timestamp_utc, Some(at));
        assert_eq!(bar.unix_utc_s, Some(at / 1_000_000));
        assert_eq!(
            bar.server_time_s,
            Some(at / 1_000_000 + i64::from(settings.server_offset_minutes) * 60)
        );
        assert_eq!(
            [bar.open, bar.high, bar.low, bar.close],
            [Some(units as f64 / 100_000.); 4]
        );
        assert_eq!(bar.volume, Some(1.));
        assert_eq!(bar.period_s, Some(5));
    }
    verify::run(&destination.uri(&manifest.key())).unwrap();
    let config_path = scratch.path("audit-bars.toml");
    fs::write(&config_path, config.canonical_toml()).unwrap();
    let mut report = Vec::new();
    binary_alpha_app::audit::run(&config_path, &destination.uri(&manifest.key()), &mut report)
        .unwrap();
    let generation = common::generation(&String::from_utf8(report).unwrap());
    let key = binary_alpha_engine::dataset::manifest_key(&generation);
    let stream = binary_alpha_engine::stream::StreamManifest::from_json(
        &fs::read(scratch.path("published").join(&key)).unwrap(),
    )
    .unwrap();
    assert_eq!(
        stream.layout,
        Some(binary_alpha_engine::dataset::Layout::DailyV2)
    );
    verify::run(&destination.uri(&key)).unwrap();
}

#[test]
fn daily_prior_continues_when_its_original_seed_manifest_is_absent() {
    let scratch = Scratch::new("daily_prior_without_seed");
    let pair = common::daily::pair(&scratch, false);
    let root = scratch.path("published");
    let local = Store::filesystem(&root);
    let mut config = test_config(&scratch, "deriv", "ws://127.0.0.1/", false);
    let start = pair.ticks.last().unwrap().event_time_micros + 1;
    let source_identity = broker::source_identity(&config.brokers[0]);
    let history = config.history.as_mut().unwrap();
    history.start = pair.v2.coverage.first_event_time.clone();
    history.end = time_text(start + 2);
    history.seeds.push(binary_alpha_engine::config::Seed {
        provider_symbol: pair.v2.provider_symbol.clone(),
        manifest: common::daily::uri(&pair.path(&scratch, true))
            .parse()
            .unwrap(),
        source_identity,
    });
    fetch::pass(
        &config,
        &mut Pages::new(vec![page_micros(&[(start, 12345)])]),
        &local,
        &local,
        (start, start + 1),
        &mut Vec::new(),
    )
    .unwrap();
    fs::remove_file(root.join(pair.v2.key())).unwrap();
    let mut report = Vec::new();
    fetch::pass(
        &config,
        &mut Pages::new(vec![page_micros(&[(start + 1, 12346)])]),
        &local,
        &local,
        (start, start + 2),
        &mut report,
    )
    .unwrap();
    let generation = common::generation(&String::from_utf8(report).unwrap());
    let path = root.join(binary_alpha_engine::dataset::manifest_key(&generation));
    common::verify(&path).unwrap();
    let manifest = GenerationManifest::from_json(&fs::read(path).unwrap()).unwrap();
    assert_eq!(
        manifest.layout,
        Some(binary_alpha_engine::dataset::Layout::DailyV2)
    );
    assert_eq!(manifest.row_count, pair.v2.row_count + 2);
    let actual = common::read_normalized_ticks(&root, &manifest);
    assert_eq!(&actual[..pair.ticks.len()], pair.ticks);
    assert_eq!(actual.last().unwrap().price_units, 12346);
}

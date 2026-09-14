use super::socket_io::{self, Packet};
use super::transport::{Connector, Frame, Transport};
use super::wire::WireDecimal;
use super::{
    Cancellation, Clock, Continuity, DiscoveredInstrument, HistoryPage, LiveEvent, LiveObservation,
    MarketDataBroker, payload_hash,
};
use binary_alpha_engine::config::{AccountClass, PocketSettings};
use binary_alpha_engine::execution::Decimal;
use binary_alpha_engine::market::{InstrumentId, PriceScale, Tick};
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;
use std::collections::{BTreeMap, VecDeque};

/// Converts the declared provider clock directly to universal integer microseconds.
pub fn universal_micros(token: &WireDecimal, offset_minutes: i32) -> Result<i64, String> {
    let value = token.require_number()?;
    if value.scale() > 6 {
        return Err("pocket_option: provider time has more than six fractional digits".into());
    }
    let offset = Decimal::parse(&(i64::from(offset_minutes) * 60).to_string())?;
    i64::try_from(value.checked_sub(offset)?.rescale(6)?.coefficient())
        .map_err(|_| "pocket_option: time overflows microseconds".into())
}
/// Maps a universal anchor back to the provider clock without floating point.
pub fn provider_token(micros: i64, offset_minutes: i32) -> Result<WireDecimal, String> {
    let universal =
        Decimal::parse(&micros.to_string())?.checked_mul(Decimal::parse("0.000001")?)?;
    let offset = Decimal::parse(&(i64::from(offset_minutes) * 60).to_string())?;
    Ok(WireDecimal::from_decimal(
        universal.checked_add(offset)?.normalized(),
    ))
}
struct Event {
    name: String,
    raw: Vec<u8>,
    receipt_micros: i64,
}
#[derive(Deserialize)]
struct BalanceClass {
    #[serde(rename = "isDemo")]
    is_demo: u8,
}
#[derive(Deserialize)]
struct InitialHistory {
    asset: String,
    period: WireDecimal,
    history: Vec<[WireDecimal; 2]>,
}
#[derive(Deserialize)]
struct OlderHistory {
    asset: String,
    index: u64,
    period: WireDecimal,
    data: Vec<PageRow>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PageRow {
    time: WireDecimal,
    price: WireDecimal,
}
#[derive(Serialize)]
struct Change<'a> {
    asset: &'a str,
    period: u8,
}
#[derive(Serialize)]
struct OlderRequest<'a> {
    asset: &'a str,
    index: u64,
    time: WireDecimal,
    offset: u16,
    period: u8,
}

pub struct PocketMarketData {
    settings: PocketSettings,
    instruments: Vec<InstrumentId>,
    connector: Box<dyn Connector>,
    transport: Box<dyn Transport>,
    clock: Box<dyn Clock>,
    credential_json: String,
    pending: Option<String>,
    discovered: Vec<DiscoveredInstrument>,
    subscribed: BTreeMap<String, (InstrumentId, PriceScale)>,
    next_index: u64,
    continuity: Continuity,
    events: VecDeque<LiveEvent>,
    received_counts: BTreeMap<String, u64>,
    source: String,
}
impl PocketMarketData {
    pub fn connect(
        settings: &PocketSettings,
        instruments: &[InstrumentId],
        mut connector: Box<dyn Connector>,
        clock: Box<dyn Clock>,
        credential_json: String,
    ) -> Result<Self, String> {
        serde_json::from_str::<BTreeMap<String, Box<RawValue>>>(&credential_json)
            .map_err(|_| "pocket_option: credential must be a JSON object")?;
        if instruments.iter().any(|id| id.broker != settings.id) {
            return Err(
                "pocket_option: configured instrument belongs to a different broker".into(),
            );
        }
        let headers = settings
            .origin
            .as_ref()
            .map(|value| vec![("Origin".into(), value.clone())])
            .unwrap_or_default();
        let transport = connector.connect(&settings.endpoint, &headers)?;
        let mut broker = Self {
            settings: settings.clone(),
            instruments: instruments.to_vec(),
            connector,
            transport,
            clock,
            credential_json,
            pending: None,
            discovered: Vec::new(),
            subscribed: BTreeMap::new(),
            next_index: 1,
            continuity: Continuity::default(),
            events: VecDeque::new(),
            received_counts: BTreeMap::new(),
            source: super::source_identity(&binary_alpha_engine::config::Broker::PocketOption(
                settings.clone(),
            )),
        };
        broker.handshake()?;
        Ok(broker)
    }
    fn send<T: Serialize>(&mut self, name: &str, argument: &T) -> Result<(), String> {
        let argument = serde_json::to_string(argument)
            .map_err(|_| "pocket_option: request serialization failed")?;
        self.transport
            .send(Frame::Text(socket_io::encode_event(name, &argument)))
    }
    fn receive(&mut self, timeout_micros: i64) -> Result<Option<Event>, String> {
        let deadline = self
            .clock
            .now_micros()
            .saturating_add(timeout_micros.max(0));
        loop {
            let Some(frame) = self
                .transport
                .receive(deadline.saturating_sub(self.clock.now_micros()).max(0))?
            else {
                return Ok(None);
            };
            let receipt_micros = self.clock.now_micros();
            match frame {
                Frame::Ping(bytes) => self.transport.send(Frame::Pong(bytes))?,
                Frame::Pong(_) => (),
                Frame::Close => {
                    return Err(if self.pending.is_some() {
                        "pocket_option: close with incomplete binary attachment"
                    } else {
                        "pocket_option: connection closed"
                    }
                    .into());
                }
                Frame::Binary(raw) => {
                    let name = self
                        .pending
                        .take()
                        .ok_or("pocket_option: binary attachment without header")?;
                    return Ok(Some(Event {
                        name,
                        raw,
                        receipt_micros,
                    }));
                }
                Frame::Text(text) => match socket_io::decode(&text)? {
                    Packet::Ping => self.transport.send(Frame::Text(socket_io::PONG.into()))?,
                    Packet::BinaryHeader { name } => {
                        if self.pending.is_some() {
                            return Err("pocket_option: overlapping binary event headers".into());
                        }
                        self.pending = Some(name);
                    }
                    Packet::Event { name, argument } => {
                        if self.pending.is_some() {
                            return Err(
                                "pocket_option: text event interrupted binary attachment".into()
                            );
                        }
                        return Ok(Some(Event {
                            name,
                            raw: argument,
                            receipt_micros,
                        }));
                    }
                    Packet::Open => {
                        return Ok(Some(Event {
                            name: "open".into(),
                            raw: Vec::new(),
                            receipt_micros,
                        }));
                    }
                    Packet::Connected => {
                        return Ok(Some(Event {
                            name: "connected".into(),
                            raw: Vec::new(),
                            receipt_micros,
                        }));
                    }
                },
            }
            if self.clock.now_micros() >= deadline {
                return Ok(None);
            }
        }
    }
    fn handshake(&mut self) -> Result<(), String> {
        let deadline = self.clock.now_micros().saturating_add(12_000_000);
        let (mut opened, mut auth_sent, mut authenticated, mut class_confirmed, mut assets) =
            (false, false, false, false, false);
        while !(authenticated && class_confirmed && assets) {
            let remaining = deadline.saturating_sub(self.clock.now_micros());
            if remaining <= 0 {
                return Err("pocket_option: handshake deadline reached".into());
            }
            let event = self
                .receive(remaining)?
                .ok_or("pocket_option: handshake deadline reached")?;
            match event.name.as_str() {
                "open" if !opened => {
                    self.transport
                        .send(Frame::Text(socket_io::CONNECT.into()))?;
                    opened = true;
                }
                "connected" if opened && !auth_sent => {
                    self.transport.send(Frame::Text(socket_io::encode_event(
                        "auth",
                        &self.credential_json,
                    )))?;
                    auth_sent = true;
                }
                "successauth" if auth_sent => authenticated = true,
                "successupdateBalance" if auth_sent => {
                    let response: BalanceClass = serde_json::from_slice(&event.raw)
                        .map_err(|_| "pocket_option: missing server account-class confirmation")?;
                    let expected = u8::from(self.settings.account_class == AccountClass::Demo);
                    if response.is_demo != expected {
                        return Err("pocket_option: server account-class mismatch".into());
                    }
                    class_confirmed = true;
                }
                "updateAssets" if auth_sent => {
                    let rows: Vec<[Box<RawValue>; 19]> = serde_json::from_slice(&event.raw)
                        .map_err(|_| "pocket_option: updateAssets row must have 19 elements")?;
                    self.discovered = rows
                        .into_iter()
                        .map(|row| {
                            let symbol = serde_json::from_str::<String>(row[1].get()).map_err(
                                |_| "pocket_option: updateAssets symbol must be a string",
                            )?;
                            Ok(DiscoveredInstrument {
                                symbol,
                                display_name: None,
                                precision: None,
                                open: None,
                            })
                        })
                        .collect::<Result<_, String>>()?;
                    if self.instruments.iter().any(|instrument| {
                        !self
                            .discovered
                            .iter()
                            .any(|entry| entry.symbol == instrument.provider_symbol.as_str())
                    }) {
                        return Err(
                            "pocket_option: configured instrument missing from updateAssets".into(),
                        );
                    }
                    assets = true;
                }
                "open" | "connected" | "successauth" | "successupdateBalance" | "updateAssets" => {
                    return Err("pocket_option: unexpected handshake order".into());
                }
                name if name.starts_with("error") || name.starts_with("fail") => {
                    return Err("pocket_option: authentication rejected".into());
                }
                _ => (),
            }
        }
        Ok(())
    }
    fn check_instrument(&self, instrument: &InstrumentId) -> Result<(), String> {
        if !self.instruments.contains(instrument) {
            return Err("pocket_option: instrument is not configured for this connection".into());
        }
        Ok(())
    }
    fn live(&mut self, event: Event) -> Result<(), String> {
        let rows: Vec<(String, WireDecimal, WireDecimal)> = serde_json::from_slice(&event.raw)
            .map_err(|_| "pocket_option: live row must have symbol, time, price")?;
        let hash = payload_hash(&event.raw);
        for (symbol, time, price) in rows {
            time.require_number()?;
            price.require_number()?;
            if self
                .instruments
                .iter()
                .any(|instrument| instrument.provider_symbol.as_str() == symbol)
            {
                *self.received_counts.entry(symbol.clone()).or_default() += 1;
            }
            if let Some((instrument, scale)) = self.subscribed.get(&symbol) {
                let (generation, sequence) = self.continuity.assign()?;
                self.events
                    .push_back(LiveEvent::Observation(LiveObservation {
                        instrument: instrument.clone(),
                        provider_time_micros: universal_micros(
                            &time,
                            self.settings.server_offset_minutes,
                        )?,
                        price_units: price.price_units(*scale)?,
                        receipt_micros: event.receipt_micros,
                        generation,
                        sequence,
                        payload_sha256: hash.clone(),
                        source: self.source.clone(),
                    }));
            }
        }
        Ok(())
    }
    fn wait_event(&mut self, name: &str) -> Result<Event, String> {
        let deadline = self.clock.now_micros().saturating_add(20_000_000);
        loop {
            let remaining = deadline.saturating_sub(self.clock.now_micros());
            if remaining <= 0 {
                return Err(format!("pocket_option {name}: response timeout"));
            }
            let event = self
                .receive(remaining)?
                .ok_or_else(|| format!("pocket_option {name}: response timeout"))?;
            if event.name == name {
                return Ok(event);
            }
            match event.name.as_str() {
                "updateStream" => self.live(event)?,
                "open" | "connected" => {
                    return Err("pocket_option: unexpected new handshake".into());
                }
                other if other.starts_with("error") || other.starts_with("fail") => {
                    return Err(format!("pocket_option {name}: provider rejected request"));
                }
                _ => (),
            }
        }
    }
    /// All configured symbols observed on the wire, including a cancelled symbol, for inspection.
    pub fn received_counts(&self) -> &BTreeMap<String, u64> {
        &self.received_counts
    }
}
impl MarketDataBroker for PocketMarketData {
    fn discover(&mut self) -> Result<Vec<DiscoveredInstrument>, String> {
        Ok(self.discovered.clone())
    }
    fn history_page(
        &mut self,
        instrument: &InstrumentId,
        scale: PriceScale,
        before_micros: Option<i64>,
    ) -> Result<HistoryPage, String> {
        self.check_instrument(instrument)?;
        let (event, raw_rows, anchor_token) = match before_micros {
            None => {
                self.send(
                    "changeSymbol",
                    &Change {
                        asset: instrument.provider_symbol.as_str(),
                        period: 1,
                    },
                )?;
                let event = self.wait_event("updateHistoryNewFast")?;
                let response: InitialHistory = serde_json::from_slice(&event.raw)
                    .map_err(|_| "pocket_option: malformed initial history or row shape")?;
                if response.asset != instrument.provider_symbol.as_str() {
                    return Err("pocket_option: initial history asset mismatch".into());
                }
                let period = response.period.require_number()?;
                if period.compare(Decimal::parse("1")?)? != std::cmp::Ordering::Equal {
                    return Err(format!(
                        "pocket_option updateHistoryNewFast: unsupported period {period}"
                    ));
                }
                (
                    event,
                    response
                        .history
                        .into_iter()
                        .map(|[time, price]| PageRow { time, price })
                        .collect::<Vec<_>>(),
                    None,
                )
            }
            Some(before) => {
                let time = provider_token(before, self.settings.server_offset_minutes)?;
                let anchor_token = Some(time.token()?.into_owned());
                let index = self.next_index;
                self.next_index = index
                    .checked_add(1)
                    .ok_or("pocket_option: history index overflow")?;
                self.send(
                    "loadHistoryPeriod",
                    &OlderRequest {
                        asset: instrument.provider_symbol.as_str(),
                        index,
                        time,
                        offset: 200,
                        period: 1,
                    },
                )?;
                let event = self.wait_event("loadHistoryPeriod")?;
                let response: OlderHistory = serde_json::from_slice(&event.raw)
                    .map_err(|_| "pocket_option: malformed older history or row shape")?;
                if response.asset != instrument.provider_symbol.as_str() || response.index != index
                {
                    return Err("pocket_option: history asset or index mismatch".into());
                }
                // Request period 1 produced response period 0 in both retained older pages.
                let period = response.period.require_number()?;
                if !period.is_zero() {
                    return Err(format!(
                        "pocket_option loadHistoryPeriod: unsupported period {period}"
                    ));
                }
                (event, response.data, anchor_token)
            }
        };
        let rows = raw_rows
            .iter()
            .map(|row| {
                row.price.require_number()?;
                Ok(Tick {
                    event_time_micros: universal_micros(
                        &row.time,
                        self.settings.server_offset_minutes,
                    )?,
                    price_units: row.price.price_units(scale)?,
                })
            })
            .collect::<Result<Vec<_>, String>>()?;
        Ok(HistoryPage {
            raw: event.raw,
            anchor_token,
            rows,
        })
    }
    fn subscribe(&mut self, instrument: &InstrumentId, scale: PriceScale) -> Result<(), String> {
        self.check_instrument(instrument)?;
        let symbol = instrument.provider_symbol.as_str();
        if self.subscribed.contains_key(symbol) {
            return Err("pocket_option: instrument already subscribed".into());
        }
        self.send("subscribeSymbol", &symbol)?;
        self.subscribed
            .insert(symbol.to_string(), (instrument.clone(), scale));
        Ok(())
    }
    fn next_live(&mut self, timeout_micros: i64) -> Result<Option<LiveEvent>, String> {
        if let Some(event) = self.events.pop_front() {
            return Ok(Some(event));
        }
        let deadline = self
            .clock
            .now_micros()
            .saturating_add(timeout_micros.max(0));
        loop {
            let Some(event) =
                self.receive(deadline.saturating_sub(self.clock.now_micros()).max(0))?
            else {
                return Ok(None);
            };
            match event.name.as_str() {
                "updateStream" => self.live(event)?,
                name if name.starts_with("error") || name.starts_with("fail") => {
                    return Err("pocket_option: live provider error".into());
                }
                "open" | "connected" => {
                    return Err("pocket_option: unexpected new handshake".into());
                }
                _ => (),
            }
            if let Some(event) = self.events.pop_front() {
                return Ok(Some(event));
            }
            if self.clock.now_micros() >= deadline {
                return Ok(None);
            }
        }
    }
    fn unsubscribe(&mut self, instrument: &InstrumentId) -> Result<Cancellation, String> {
        let symbol = instrument.provider_symbol.as_str();
        if !self.subscribed.contains_key(symbol) {
            return Err("pocket_option: instrument is not subscribed".into());
        }
        self.send("unSubscribeSymbol", &symbol)?;
        self.subscribed.remove(symbol);
        Ok(Cancellation::SentWithoutAcknowledgement)
    }
    fn reconnect(&mut self) -> Result<(), String> {
        self.transport.close()?;
        let headers = self
            .settings
            .origin
            .as_ref()
            .map(|value| vec![("Origin".into(), value.clone())])
            .unwrap_or_default();
        self.transport = self.connector.connect(&self.settings.endpoint, &headers)?;
        self.pending = None;
        self.subscribed.clear();
        self.discovered.clear();
        self.events.clear();
        self.next_index = 1;
        let generation = self.continuity.reconnect()?;
        self.handshake()?;
        self.events.push_back(LiveEvent::Break {
            generation,
            reason: "explicit reconnect; subscriptions and causal warm-up must be rebuilt".into(),
        });
        Ok(())
    }
    fn continuity(&self) -> &Continuity {
        &self.continuity
    }
}

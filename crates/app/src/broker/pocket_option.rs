use super::socket_io::{self, Packet};
use super::transport::{Connector, Frame, Transport};
use super::wire::WireDecimal;
use super::{
    Cancellation, Clock, Continuity, DiscoveredInstrument, HistoryPage, HistoryRows, LiveEvent,
    LiveObservation, MarketDataBroker, payload_hash,
};
use binary_alpha_engine::config::{AccountClass, PocketSettings};
use binary_alpha_engine::dataset::NativeGranularity;
use binary_alpha_engine::execution::Decimal;
use binary_alpha_engine::market::{
    Bar, BarSequence, InstrumentId, PriceScale, Tick, float_price_units,
};
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;
use std::collections::{BTreeMap, VecDeque};
use std::hash::{DefaultHasher, Hash, Hasher};

/// The request index travels as a JSON number and comes back through the provider's
/// JavaScript runtime, which represents integers exactly only below 2^53. Observed on the
/// real endpoint (2026-09-16): a seed above that range was echoed inexactly, so every
/// response looked foreign and the page timed out. The seed stays below 2^52, leaving 2^52
/// increments of headroom.
const HISTORY_INDEX_SEED_BITS: u32 = 52;

fn history_index_seed(now_micros: i64) -> u64 {
    let mut hasher = DefaultHasher::new();
    (
        now_micros,
        std::process::id(),
        format!("{:?}", std::thread::current().id()),
    )
        .hash(&mut hasher);
    hasher.finish() & ((1u64 << HISTORY_INDEX_SEED_BITS) - 1)
}

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
enum ReceiveError {
    Disconnected,
    Transport(String),
    Other(String),
}
impl From<String> for ReceiveError {
    fn from(error: String) -> Self {
        Self::Other(error)
    }
}
impl From<&str> for ReceiveError {
    fn from(error: &str) -> Self {
        Self::Other(error.into())
    }
}
impl From<ReceiveError> for String {
    fn from(error: ReceiveError) -> Self {
        match error {
            ReceiveError::Disconnected => "socket.io: the server disconnected the namespace (an `origin` setting is usually required)".into(),
            ReceiveError::Transport(error) | ReceiveError::Other(error) => error,
        }
    }
}
#[derive(Deserialize)]
struct BalanceClass {
    #[serde(rename = "isDemo")]
    is_demo: u8,
}
/// The row bodies decoded from retained pages; their envelopes were matched on receipt.
#[derive(Deserialize)]
struct InitialHistory {
    asset: String,
    history: Vec<[WireDecimal; 2]>,
}
#[derive(Deserialize)]
struct OlderHistory {
    asset: String,
    data: Vec<PageRow>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PageRow {
    time: WireDecimal,
    price: WireDecimal,
}
/// The envelopes checked before a page is retained; rows are decoded afterwards.
#[derive(Deserialize)]
struct InitialEnvelope {
    asset: String,
    period: WireDecimal,
}
#[derive(Deserialize)]
struct OlderEnvelope {
    asset: String,
    index: u64,
    period: WireDecimal,
}
/// The observed `loadHistoryPeriodFast` candle page: the same envelope as older tick history
/// with one row object per provider bar.
#[derive(Deserialize)]
struct CandleHistory {
    asset: String,
    period: WireDecimal,
    data: Vec<CandleRow>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CandleRow {
    symbol_id: i32,
    time: WireDecimal,
    open: WireDecimal,
    close: WireDecimal,
    high: WireDecimal,
    low: WireDecimal,
    volume: WireDecimal,
}
/// The only provider bar period this checkout admits, in seconds.
const BAR_PERIOD_S: u16 = 5;
const HISTORY_OFFSET_S: u16 = 200;

#[derive(Default)]
struct CandleHistoryBuffer {
    asset: String,
    pages: BTreeMap<i64, PrefetchedPage>,
}
struct PrefetchedPage {
    index: Option<u64>,
    page: Option<HistoryPage>,
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
    candle_history: CandleHistoryBuffer,
    continuity: Continuity,
    events: VecDeque<LiveEvent>,
    received_counts: BTreeMap<String, u64>,
    foreign_history_responses: u64,
    history_reconnects: u64,
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
        let next_index = history_index_seed(clock.now_micros());
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
            next_index,
            candle_history: CandleHistoryBuffer::default(),
            continuity: Continuity::default(),
            events: VecDeque::new(),
            received_counts: BTreeMap::new(),
            foreign_history_responses: 0,
            history_reconnects: 0,
            source: super::source_identity(&binary_alpha_engine::config::Broker::PocketOption(
                settings.clone(),
            )),
        };
        broker.handshake()?;
        Ok(broker)
    }
    fn send<T: Serialize>(&mut self, name: &str, argument: &T) -> Result<(), ReceiveError> {
        let argument = serde_json::to_string(argument)
            .map_err(|_| "pocket_option: request serialization failed")?;
        self.transport
            .send(Frame::Text(socket_io::encode_event(name, &argument)))
            .map_err(ReceiveError::Transport)
    }
    fn receive(&mut self, timeout_micros: i64) -> Result<Option<Event>, ReceiveError> {
        let deadline = self
            .clock
            .now_micros()
            .saturating_add(timeout_micros.max(0));
        loop {
            let Some(frame) = self
                .transport
                .receive(deadline.saturating_sub(self.clock.now_micros()).max(0))
                .map_err(ReceiveError::Transport)?
            else {
                return Ok(None);
            };
            let receipt_micros = self.clock.now_micros();
            match frame {
                Frame::Ping(bytes) => self
                    .transport
                    .send(Frame::Pong(bytes))
                    .map_err(ReceiveError::Transport)?,
                Frame::Pong(_) => (),
                Frame::Close => {
                    return Err(if self.pending.is_some() {
                        ReceiveError::Other(
                            "pocket_option: close with incomplete binary attachment".into(),
                        )
                    } else {
                        ReceiveError::Transport("pocket_option: connection closed".into())
                    });
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
                    Packet::Disconnected => return Err(ReceiveError::Disconnected),
                    Packet::Ping => self
                        .transport
                        .send(Frame::Text(socket_io::PONG.into()))
                        .map_err(ReceiveError::Transport)?,
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
    fn wait_event(&mut self, names: &[&str], deadline: i64) -> Result<Event, ReceiveError> {
        let name = names[0];
        loop {
            let remaining = deadline.saturating_sub(self.clock.now_micros());
            if remaining <= 0 {
                return Err(format!("pocket_option {name}: response timeout").into());
            }
            let event = self
                .receive(remaining)?
                .ok_or_else(|| format!("pocket_option {name}: response timeout"))?;
            if names.contains(&event.name.as_str()) {
                return Ok(event);
            }
            match event.name.as_str() {
                "updateStream" => self.live(event)?,
                "open" | "connected" => {
                    return Err("pocket_option: unexpected new handshake".into());
                }
                other if other.starts_with("error") || other.starts_with("fail") => {
                    return Err(format!("pocket_option {name}: provider rejected request").into());
                }
                _ => (),
            }
        }
    }
    fn send_history_request(
        &mut self,
        symbol: &str,
        before: i64,
        period: u8,
    ) -> Result<u64, ReceiveError> {
        let time = provider_token(before, self.settings.server_offset_minutes)?;
        let index = self.next_index;
        self.next_index = index
            .checked_add(1)
            .ok_or("pocket_option: history index overflow")?;
        self.send(
            "loadHistoryPeriod",
            &OlderRequest {
                asset: symbol,
                index,
                time,
                offset: HISTORY_OFFSET_S,
                period,
            },
        )?;
        Ok(index)
    }
    fn prefetch_candles(
        &mut self,
        symbol: &str,
        before: i64,
        period: u8,
    ) -> Result<(), ReceiveError> {
        let limit = usize::from(self.settings.history_pages_in_flight.unwrap_or(8));
        if limit == 0 {
            return Err("history_pages_in_flight must be positive".into());
        }
        // A live producer's durable progress record (2026-09-16) shows offset=200,
        // period=5 returning 40 bars from A-195s through A inclusive. Anchors
        // 1788228000, 1788227805, 1788227610 each end at A and overlap the next
        // page by one bar. The fetch owner uses the first row as its next anchor,
        // so predict offset minus period; reset below if real gaps change that cursor.
        let page_step_micros = (i64::from(HISTORY_OFFSET_S) - i64::from(period)) * 1_000_000;
        // Skipping an unconsumed anchor is a gap even if an older buffered anchor matches.
        if self.candle_history.asset != symbol
            || self
                .candle_history
                .pages
                .last_key_value()
                .map(|(anchor, _)| *anchor)
                != Some(before)
        {
            self.candle_history = CandleHistoryBuffer {
                asset: symbol.into(),
                pages: BTreeMap::new(),
            };
        }
        // A reconnect keeps received pages and invalidates only outstanding request indexes.
        let resend: Vec<_> = self
            .candle_history
            .pages
            .iter()
            .rev()
            .filter_map(|(&anchor, entry)| {
                (entry.index.is_none() && entry.page.is_none()).then_some(anchor)
            })
            .collect();
        for anchor in resend {
            let index = self.send_history_request(symbol, anchor, period)?;
            self.candle_history.pages.get_mut(&anchor).unwrap().index = Some(index);
        }
        while self.candle_history.pages.len() < limit {
            let anchor = match self.candle_history.pages.first_key_value() {
                Some((anchor, _)) => anchor
                    .checked_sub(page_step_micros)
                    .ok_or("pocket_option: history anchor overflow")?,
                None => before,
            };
            let index = self.send_history_request(symbol, anchor, period)?;
            self.candle_history.pages.insert(
                anchor,
                PrefetchedPage {
                    index: Some(index),
                    page: None,
                },
            );
        }
        Ok(())
    }
    fn request_history_page(
        &mut self,
        symbol: &str,
        before_micros: Option<i64>,
        period: u8,
    ) -> Result<HistoryPage, ReceiveError> {
        if period == 1 {
            self.candle_history = CandleHistoryBuffer::default();
        }
        let Some(before) = before_micros else {
            // The anchor-free page exists only for ticks; the evidenced candle family is the
            // indexed older-history request answered as `loadHistoryPeriodFast`.
            if period != 1 {
                return Err("pocket_option: bar history requires an anchor".into());
            }
            self.send(
                "changeSymbol",
                &Change {
                    asset: symbol,
                    period: 1,
                },
            )?;
            let deadline = self.clock.now_micros().saturating_add(20_000_000);
            let event = self.wait_event(&["updateHistoryNewFast"], deadline)?;
            let envelope: InitialEnvelope = serde_json::from_slice(&event.raw)
                .map_err(|_| "pocket_option: malformed initial history")?;
            if envelope.asset != symbol {
                return Err("pocket_option: initial history asset mismatch".into());
            }
            let period = envelope.period.require_number()?;
            if period.compare(Decimal::parse("1")?)? != std::cmp::Ordering::Equal {
                return Err(format!(
                    "pocket_option updateHistoryNewFast: unsupported period {period}"
                )
                .into());
            }
            return Ok(HistoryPage {
                raw: event.raw,
                anchor_token: None,
                receipt_micros: event.receipt_micros,
            });
        };
        let index = if period == 1 {
            self.send_history_request(symbol, before, period)?
        } else {
            self.prefetch_candles(symbol, before, period)?;
            let entry = self.candle_history.pages.get_mut(&before).unwrap();
            if let Some(page) = entry.page.take() {
                self.candle_history.pages.remove(&before);
                return Ok(page);
            }
            entry.index.unwrap()
        };
        let (event_name, expected_period) = if period == 1 {
            // Request period 1 produced response period 0 in both retained older pages.
            ("loadHistoryPeriod", 0)
        } else {
            ("loadHistoryPeriodFast", i64::from(period))
        };
        let deadline = self.clock.now_micros().saturating_add(20_000_000);
        let skipped_before = self.foreign_history_responses;
        loop {
            let event = match self.wait_event(
                &[
                    event_name,
                    if period == 1 {
                        "loadHistoryPeriodFast"
                    } else {
                        "loadHistoryPeriod"
                    },
                ],
                deadline,
            ) {
                Err(ReceiveError::Other(error))
                    if self.foreign_history_responses > skipped_before =>
                {
                    return Err(format!(
                        "pocket_option: no matching history response after asset or index mismatch; {error}"
                    ).into());
                }
                result => result?,
            };
            let envelope: OlderEnvelope = serde_json::from_slice(&event.raw)
                .map_err(|_| format!("pocket_option: malformed {} history", event.name))?;
            let anchor = if period == 1 {
                (envelope.index == index).then_some(before)
            } else {
                self.candle_history
                    .pages
                    .iter()
                    .find_map(|(&anchor, entry)| {
                        (entry.index == Some(envelope.index) && entry.page.is_none())
                            .then_some(anchor)
                    })
            };
            let Some(anchor) = anchor.filter(|_| envelope.asset == symbol) else {
                self.foreign_history_responses += 1;
                continue;
            };
            if event.name != event_name {
                continue;
            }
            let response_period = envelope.period.require_number()?;
            if response_period.compare(Decimal::parse(&expected_period.to_string())?)?
                != std::cmp::Ordering::Equal
            {
                return Err(format!(
                    "pocket_option {event_name}: unsupported period {response_period}"
                )
                .into());
            }
            let page = HistoryPage {
                raw: event.raw,
                anchor_token: Some(
                    provider_token(anchor, self.settings.server_offset_minutes)?
                        .token()?
                        .into_owned(),
                ),
                receipt_micros: event.receipt_micros,
            };
            if envelope.index == index {
                self.candle_history.pages.remove(&anchor);
                return Ok(page);
            }
            self.candle_history.pages.get_mut(&anchor).unwrap().page = Some(page);
        }
    }
    /// All configured symbols observed on the wire, including a cancelled symbol, for inspection.
    pub fn received_counts(&self) -> &BTreeMap<String, u64> {
        &self.received_counts
    }
    /// History responses skipped because their asset or request index belonged elsewhere.
    pub fn foreign_history_responses(&self) -> u64 {
        self.foreign_history_responses
    }
    /// Reconnect attempts triggered by namespace disconnects or transport failures in history.
    pub fn history_reconnects(&self) -> u64 {
        self.history_reconnects
    }
    fn tick_rows(&self, rows: &[PageRow], scale: PriceScale) -> Result<Vec<Tick>, String> {
        rows.iter()
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
            .collect()
    }
    /// Decodes one candle page: the asset must match, the period must be the admitted five
    /// seconds, every row must carry one constant provider
    /// identifier, whole-second grid-aligned starts and finite consistent prices, and every
    /// price must convert exactly to units at `scale` both as decimal text and as the archive's
    /// binary floating point.
    fn decode_candles(
        &self,
        instrument: &InstrumentId,
        raw: &[u8],
        scale: PriceScale,
    ) -> Result<(i32, Vec<Bar>), String> {
        let response: CandleHistory = serde_json::from_slice(raw)
            .map_err(|_| "pocket_option: malformed candle history or row shape")?;
        if response.asset != instrument.provider_symbol.as_str() {
            return Err("pocket_option: candle history asset mismatch".into());
        }
        let period = response.period.require_number()?;
        if period.compare(Decimal::parse(&BAR_PERIOD_S.to_string())?)? != std::cmp::Ordering::Equal
        {
            return Err(format!(
                "pocket_option loadHistoryPeriodFast: unsupported period {period}"
            ));
        }
        let exact = |token: &WireDecimal| -> Result<f64, String> {
            let units = token.price_units(scale)?;
            let value: f64 = token
                .token()?
                .parse()
                .map_err(|_| "pocket_option: candle price is not a number".to_string())?;
            if float_price_units(value, scale)? != units {
                return Err(format!(
                    "pocket_option: candle price {} does not round-trip at price scale {}",
                    token.token()?,
                    scale.digits()
                ));
            }
            Ok(value)
        };
        let mut symbol_id = None;
        let mut sequence = BarSequence::default();
        let mut bars = Vec::with_capacity(response.data.len());
        for row in &response.data {
            match symbol_id {
                Some(id) if id != row.symbol_id => {
                    return Err(
                        "pocket_option: candle rows carry different symbol identifiers".into(),
                    );
                }
                Some(_) => {}
                None => symbol_id = Some(row.symbol_id),
            }
            let start_micros = universal_micros(&row.time, self.settings.server_offset_minutes)?;
            if start_micros % 1_000_000 != 0 {
                return Err("pocket_option: candle start is not a whole second".into());
            }
            let bar = Bar {
                start_unix_s: start_micros / 1_000_000,
                open: exact(&row.open)?,
                high: exact(&row.high)?,
                low: exact(&row.low)?,
                close: exact(&row.close)?,
                volume: row
                    .volume
                    .token()?
                    .parse()
                    .map_err(|_| "pocket_option: candle volume is not a number".to_string())?,
                period_s: BAR_PERIOD_S,
            };
            bar.validate(BAR_PERIOD_S)
                .map_err(|reason| format!("pocket_option: {reason}"))?;
            sequence
                .accept(bar.start_unix_s)
                .map_err(|reason| format!("pocket_option: {reason}"))?;
            bars.push(bar);
        }
        let symbol_id = symbol_id.ok_or("pocket_option: candle history page holds no rows")?;
        Ok((symbol_id, bars))
    }
}
impl MarketDataBroker for PocketMarketData {
    fn discover(&mut self) -> Result<Vec<DiscoveredInstrument>, String> {
        Ok(self.discovered.clone())
    }
    fn history_page(
        &mut self,
        instrument: &InstrumentId,
        _: PriceScale,
        before_micros: Option<i64>,
        granularity: NativeGranularity,
    ) -> Result<HistoryPage, String> {
        self.check_instrument(instrument)?;
        let symbol = instrument.provider_symbol.as_str();
        let period = match granularity {
            NativeGranularity::Tick => 1,
            NativeGranularity::Bar {
                period_seconds: BAR_PERIOD_S,
            } => BAR_PERIOD_S as u8,
            NativeGranularity::Bar { .. } => {
                return Err(format!(
                    "pocket_option: {granularity} history is not supported; only ticks and {BAR_PERIOD_S}-second bars are"
                ));
            }
        };
        let mut reconnects = 0;
        let skipped_before = self.foreign_history_responses;
        loop {
            match self.request_history_page(symbol, before_micros, period) {
                Ok(page) => return Ok(page),
                Err(error @ (ReceiveError::Disconnected | ReceiveError::Transport(_))) => {
                    if reconnects >= 3 || self.history_reconnects >= 20 {
                        let reason = match error {
                            ReceiveError::Transport(error) => format!(
                                "pocket_option: history transport reconnect limit reached; {error}"
                            ),
                            _ => {
                                "pocket_option: the server keeps disconnecting the namespace".into()
                            }
                        };
                        return Err(if self.foreign_history_responses > skipped_before {
                            format!(
                                "pocket_option: no matching history response after asset or index mismatch; {reason}"
                            )
                        } else {
                            reason
                        });
                    }
                    reconnects += 1;
                    self.history_reconnects += 1;
                    // Re-authentication failures, including connect-time namespace disconnects,
                    // retain their original diagnostic and are not history retries.
                    self.reconnect_transport(matches!(error, ReceiveError::Transport(_)))?;
                }
                Err(error) => return Err(error.into()),
            }
        }
    }
    fn decode_history(
        &self,
        instrument: &InstrumentId,
        raw: &[u8],
        scale: PriceScale,
        granularity: NativeGranularity,
    ) -> Result<(Option<i32>, HistoryRows), String> {
        if granularity != NativeGranularity::Tick {
            let (symbol_id, rows) = self.decode_candles(instrument, raw, scale)?;
            return Ok((Some(symbol_id), HistoryRows::Bars(rows)));
        }
        // A retained tick page is the older shape when it carries `data`, else the initial one.
        let rows = match serde_json::from_slice::<OlderHistory>(raw) {
            Ok(response) => {
                if response.asset != instrument.provider_symbol.as_str() {
                    return Err("pocket_option: history asset mismatch".into());
                }
                response.data
            }
            Err(_) => {
                let response: InitialHistory = serde_json::from_slice(raw)
                    .map_err(|_| "pocket_option: malformed retained history page")?;
                if response.asset != instrument.provider_symbol.as_str() {
                    return Err("pocket_option: history asset mismatch".into());
                }
                response
                    .history
                    .into_iter()
                    .map(|[time, price]| PageRow { time, price })
                    .collect()
            }
        };
        Ok((None, HistoryRows::Ticks(self.tick_rows(&rows, scale)?)))
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
        self.reconnect_transport(false)
    }
    fn continuity(&self) -> &Continuity {
        &self.continuity
    }
}

impl PocketMarketData {
    fn reconnect_transport(&mut self, failed_transport: bool) -> Result<(), String> {
        let closed = self.transport.close();
        // A failed history transport may already be closed; discard it even if Close fails.
        if !failed_transport {
            closed?;
        }
        let headers = self
            .settings
            .origin
            .as_ref()
            .map(|value| vec![("Origin".into(), value.clone())])
            .unwrap_or_default();
        self.transport = self.connector.connect(&self.settings.endpoint, &headers)?;
        self.pending = None;
        for entry in self.candle_history.pages.values_mut() {
            if entry.page.is_none() {
                entry.index = None;
            }
        }
        self.subscribed.clear();
        self.discovered.clear();
        self.events.clear();
        self.next_index = self
            .next_index
            .max(history_index_seed(self.clock.now_micros()));
        let generation = self.continuity.reconnect()?;
        self.handshake()?;
        self.events.push_back(LiveEvent::Break {
            generation,
            reason: "explicit reconnect; subscriptions and causal warm-up must be rebuilt".into(),
        });
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn history_index_seed_is_an_exact_javascript_integer() {
        for now in [0, 1_789_593_000_000_000, i64::MAX] {
            let seed = super::history_index_seed(now);
            assert!(seed < (1u64 << 53) - (1u64 << 52), "{seed}");
        }
    }
}

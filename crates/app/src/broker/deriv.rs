#[path = "deriv_options.rs"]
mod options;
pub use options::{
    DerivOptions, StatementRow, purchase_fact, purchase_observation, recover_purchase,
    to_observation,
};

use super::transport::{Connector, Frame, Http, Transport};
use super::wire::WireDecimal;
use super::{
    Cancellation, Clock, Continuity, DiscoveredInstrument, HistoryPage, LiveEvent, LiveObservation,
    MarketDataBroker, RateBudget, RateGroup, payload_hash,
};
use binary_alpha_engine::config::{AccountClass, DerivSettings, RateBudgets};
use binary_alpha_engine::execution::Decimal;
use binary_alpha_engine::market::{BrokerId, Currency, InstrumentId, PriceScale, Tick};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::collections::{BTreeMap, BTreeSet, VecDeque};

pub const SCHEMA: &str = "deriv:production_v20260819_0";
#[derive(Debug, Deserialize)]
pub struct WireError {
    pub code: String,
}
#[derive(Debug, Deserialize)]
pub struct Subscription {
    pub id: String,
}
#[derive(Debug, Deserialize)]
pub struct Envelope {
    pub msg_type: String,
    pub req_id: Option<u64>,
    pub error: Option<WireError>,
    pub subscription: Option<Subscription>,
}
/// The original response bytes and local receipt identity, before typed body decoding.
struct Response {
    raw: Vec<u8>,
    header: Envelope,
    receipt_micros: i64,
    stamp: Option<(u64, u64)>,
}
fn decode<T: DeserializeOwned>(raw: &[u8], context: &str) -> Result<T, String> {
    serde_json::from_slice(raw).map_err(|error| {
        // Only schema field names enter diagnostics; deserializer messages can echo private values.
        const FIELDS: &str = concat!(
            "msg_type req_id error code subscription id proposal ",
            "ask_price payout spot spot_time longcode commission buy ",
            "buy_price purchase_time contract_id transaction_id start_time transaction action ",
            "amount currency transaction_time proposal_open_contract contract_type status underlying_symbol ",
            "entry_spot entry_spot_time date_start date_expiry exit_spot exit_spot_time transaction_ids ",
            "sell current_spot_time sell_time portfolio contracts expiry_time statement ",
            "count transactions action_type history prices times pip_size ",
            "tick epoch quote symbol balance loginid data ",
            "account_id account_type url active_symbols contracts_for available contract_category ",
            "barriers min_contract_duration expiry_type forget ",
        );
        let message = error.to_string();
        let missing = FIELDS
            .split_whitespace()
            .find(|field| message.starts_with(&format!("missing field `{field}`")));
        let prefix = raw
            .split(|byte| *byte == b'\n')
            .take(error.line())
            .enumerate()
            .flat_map(|(line, bytes)| {
                bytes.iter().copied().take(if line + 1 == error.line() {
                    error.column()
                } else {
                    bytes.len()
                })
            })
            .collect::<Vec<_>>();
        let prefix = String::from_utf8_lossy(&prefix);
        let field = missing.or_else(|| {
                FIELDS
                    .split_whitespace()
                    .filter_map(|field| {
                        prefix
                            .rfind(&format!("\"{field}\""))
                            .map(|position| (position, field))
                    })
                    .max_by_key(|(position, _)| *position)
                    .map(|(_, field)| field)
            })
            .unwrap_or("object");
        format!("deriv {context}: malformed {field} field")
    })
}

/// Correlation, rate admission, and explicit connection replacement shared by Deriv methods.
struct DerivConnection {
    connector: Box<dyn Connector>,
    transport: Box<dyn Transport>,
    clock: Box<dyn Clock>,
    budget: RateBudget,
    url: String,
    next_id: u64,
    closed: bool,
    continuity: Continuity,
    subscriptions: BTreeSet<String>,
    queued: VecDeque<Response>,
}
impl DerivConnection {
    fn connect(
        url: &str,
        mut connector: Box<dyn Connector>,
        clock: Box<dyn Clock>,
        limits: RateBudgets,
    ) -> Result<Self, String> {
        let budget = RateBudget::new(limits)?;
        let transport = connector.connect(url, &[])?;
        Ok(Self {
            connector,
            transport,
            clock,
            budget,
            url: url.to_string(),
            next_id: 1,
            closed: false,
            continuity: Continuity::default(),
            subscriptions: BTreeSet::new(),
            queued: VecDeque::new(),
        })
    }
    fn receive(&mut self, timeout_micros: i64) -> Result<Option<Response>, String> {
        let deadline = self
            .clock
            .now_micros()
            .saturating_add(timeout_micros.max(0));
        loop {
            let remaining = deadline.saturating_sub(self.clock.now_micros()).max(0);
            let received = self.transport.receive(remaining);
            if received.is_err() {
                self.closed = true;
            }
            let Some(frame) = received? else {
                return Ok(None);
            };
            match frame {
                Frame::Ping(bytes) => self.transport.send(Frame::Pong(bytes))?,
                Frame::Pong(_) => (),
                Frame::Close => {
                    self.closed = true;
                    return Err("deriv: connection closed".into());
                }
                Frame::Binary(_) => return Err("deriv: unexpected binary frame".into()),
                Frame::Text(text) => {
                    let receipt_micros = self.clock.now_micros();
                    let raw = text.into_bytes();
                    let header: Envelope = decode(&raw, "envelope")?;
                    let stamp = if header.msg_type == "tick" && header.error.is_none() {
                        Some(self.continuity.assign()?)
                    } else {
                        None
                    };
                    return Ok(Some(Response {
                        raw,
                        header,
                        receipt_micros,
                        stamp,
                    }));
                }
            }
            if self.clock.now_micros() >= deadline {
                return Ok(None);
            }
        }
    }
    fn request<T: Serialize>(
        &mut self,
        group: RateGroup,
        expected: &str,
        build: impl FnOnce(u64) -> T,
    ) -> Result<Response, String> {
        let (id, text) = self.prepare(group, build)?;
        self.transport.send(Frame::Text(text))?;
        let response = self.response(id, expected)?;
        if let Some(error) = &response.header.error {
            return Err(format!("deriv {expected}: {}", error.code));
        }
        Ok(response)
    }
    fn prepare<T: Serialize>(
        &mut self,
        group: RateGroup,
        build: impl FnOnce(u64) -> T,
    ) -> Result<(u64, String), String> {
        if self.closed {
            return Err("deriv: connection closed before write".into());
        }
        let id = self.next_id;
        self.next_id = id
            .checked_add(1)
            .ok_or("deriv: request identity overflow")?;
        let text =
            serde_json::to_string(&build(id)).map_err(|_| "deriv: request serialization failed")?;
        self.budget.admit(group, &mut *self.clock);
        Ok((id, text))
    }
    fn response(&mut self, id: u64, expected: &str) -> Result<Response, String> {
        let deadline = self.clock.now_micros().saturating_add(20_000_000);
        loop {
            let remaining = deadline.saturating_sub(self.clock.now_micros());
            if remaining <= 0 {
                return Err(format!("deriv {expected}: response timeout"));
            }
            let response = self
                .receive(remaining)?
                .ok_or_else(|| format!("deriv {expected}: response timeout"))?;
            if response.header.req_id == Some(id) {
                if response.header.msg_type != expected {
                    return Err(format!("deriv {expected}: unexpected msg_type field"));
                }
                return Ok(response);
            }
            if matches!(
                response.header.msg_type.as_str(),
                "tick" | "transaction" | "proposal_open_contract"
            ) && response
                .header
                .subscription
                .as_ref()
                .is_some_and(|s| self.subscriptions.contains(&s.id))
            {
                if let Some(error) = &response.header.error {
                    return Err(format!("deriv tick: {}", error.code));
                }
                self.queued.push_back(response);
            } else {
                return Err(format!("deriv {expected}: req_id mismatch"));
            }
        }
    }
    fn next(&mut self, timeout_micros: i64) -> Result<Option<Response>, String> {
        if let Some(response) = self.queued.pop_front() {
            Ok(Some(response))
        } else {
            self.receive(timeout_micros)
        }
    }
    fn reconnect(&mut self) -> Result<u64, String> {
        self.transport.close()?;
        self.transport = self.connector.connect(&self.url, &[])?;
        self.next_id = 1;
        self.closed = false;
        self.subscriptions.clear();
        self.queued.clear();
        self.continuity.reconnect()
    }
    fn continuity(&self) -> &Continuity {
        &self.continuity
    }
}

#[derive(Serialize)]
struct DiscoverRequest {
    active_symbols: &'static str,
    req_id: u64,
}
#[derive(Deserialize)]
struct SymbolsResponse {
    active_symbols: Vec<Symbol>,
}
#[derive(Deserialize)]
struct Symbol {
    underlying_symbol: String,
    underlying_symbol_name: String,
    pip_size: WireDecimal,
    exchange_is_open: u8,
    is_trading_suspended: u8,
}
#[derive(Serialize)]
struct ContractsRequest<'a> {
    contracts_for: &'a str,
    req_id: u64,
}
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ContractAvailability {
    pub contract_type: String,
    pub contract_category: String,
    pub barriers: WireDecimal,
    pub min_contract_duration: String,
    pub expiry_type: String,
    pub underlying_symbol: String,
}
#[derive(Deserialize)]
struct ContractsResponse {
    contracts_for: Contracts,
}
#[derive(Deserialize)]
struct Contracts {
    available: Vec<ContractAvailability>,
}
#[derive(Serialize)]
struct HistoryRequest<'a> {
    ticks_history: &'a str,
    style: &'static str,
    end: String,
    count: u32,
    req_id: u64,
}
#[derive(Deserialize)]
struct HistoryResponse {
    history: History,
    pip_size: WireDecimal,
}
#[derive(Deserialize)]
struct History {
    prices: Vec<WireDecimal>,
    times: Vec<i64>,
}
#[derive(Serialize)]
struct TicksRequest<'a> {
    ticks: &'a str,
    subscribe: u8,
    req_id: u64,
}
#[derive(Deserialize)]
struct TickResponse {
    tick: WireTick,
}
#[derive(Deserialize)]
struct WireTick {
    epoch: i64,
    quote: WireDecimal,
    pip_size: WireDecimal,
    symbol: String,
}
#[derive(Serialize)]
struct ForgetRequest<'a> {
    forget: &'a str,
    req_id: u64,
}
#[derive(Deserialize)]
struct ForgetResponse {
    forget: u8,
}
fn micros(seconds: i64) -> Result<i64, String> {
    seconds
        .checked_mul(1_000_000)
        .ok_or("deriv: epoch overflows microseconds".into())
}
/// The digit count of a discovered pip size. The schema describes it as the minimum
/// fluctuation, a JSON number the provider writes plainly (`0.0001`) or, for five-decimal
/// pairs, in exponent form (`1e-05`); both are read exactly, without floating point.
fn pip_digits(pip_size: &WireDecimal) -> Result<u8, String> {
    const INVALID: &str = "deriv active_symbols: unsupported pip_size precision";
    let token = pip_size.0.get();
    let (mantissa, exponent) = match token.split_once(['e', 'E']) {
        Some((mantissa, exponent)) => (mantissa, exponent.parse::<i16>().map_err(|_| INVALID)?),
        None => (token, 0),
    };
    let pip = Decimal::parse(mantissa).map_err(|_| INVALID)?.normalized();
    let digits = i32::from(pip.scale()) - i32::from(exponent);
    if token.starts_with('"') || pip.coefficient() != 1 || exponent > 0 || digits < 0 {
        return Err(INVALID.into());
    }
    u8::try_from(digits)
        .ok()
        .filter(|digits| *digits <= binary_alpha_engine::execution::MAX_SCALE)
        .ok_or_else(|| INVALID.into())
}
fn precision(pip_size: &WireDecimal, scale: PriceScale) -> Result<(), String> {
    let digits = pip_size.require_number()?.rescale(0)?.coefficient();
    if digits < 0 || digits > i128::from(scale.digits()) {
        return Err("deriv: pip_size exceeds configured price scale".into());
    }
    Ok(())
}

pub struct DerivMarketData {
    broker: BrokerId,
    connection: DerivConnection,
    subscriptions: BTreeMap<String, (InstrumentId, PriceScale)>,
    active: BTreeMap<InstrumentId, String>,
    events: VecDeque<LiveEvent>,
    source: String,
}
impl DerivMarketData {
    pub fn connect(
        settings: &DerivSettings,
        connector: Box<dyn Connector>,
        clock: Box<dyn Clock>,
    ) -> Result<Self, String> {
        let connection = DerivConnection::connect(
            &settings.public_endpoint,
            connector,
            clock,
            settings.budgets.clone().unwrap_or_default(),
        )?;
        Ok(Self {
            broker: settings.id.clone(),
            connection,
            subscriptions: BTreeMap::new(),
            active: BTreeMap::new(),
            events: VecDeque::new(),
            source: super::source_identity(&binary_alpha_engine::config::Broker::Deriv(
                settings.clone(),
            )),
        })
    }
    pub fn contracts_for(&mut self, symbol: &str) -> Result<Vec<ContractAvailability>, String> {
        let response = self
            .connection
            .request(RateGroup::Other, "contracts_for", |req_id| {
                ContractsRequest {
                    contracts_for: symbol,
                    req_id,
                }
            })?;
        let contracts: ContractsResponse = decode(&response.raw, "contracts_for")?;
        let mut selected = Vec::new();
        for contract in contracts.contracts_for.available {
            contract.barriers.require_number()?;
            if contract.underlying_symbol == symbol
                && contract.contract_category == options::MEASURED_CONTRACT_CATEGORY
                && matches!(contract.contract_type.as_str(), "CALL" | "PUT")
            {
                selected.push(contract);
            }
        }
        Ok(selected)
    }
    fn check_instrument(&self, instrument: &InstrumentId) -> Result<(), String> {
        if instrument.broker != self.broker {
            return Err("deriv: instrument belongs to a different broker".into());
        }
        Ok(())
    }
    fn observation(&self, response: Response) -> Result<LiveEvent, String> {
        if let Some(error) = &response.header.error {
            return Err(format!("deriv market: {}", error.code));
        }
        if response.header.msg_type != "tick" {
            return Err("deriv: unexpected msg_type field".into());
        }
        let subscription = response
            .header
            .subscription
            .as_ref()
            .ok_or("deriv tick: subscription id missing")?;
        let (instrument, scale) = self
            .subscriptions
            .get(&subscription.id)
            .ok_or("deriv tick: unknown subscription")?;
        let body: TickResponse = decode(&response.raw, "tick")?;
        if body.tick.symbol != instrument.provider_symbol.as_str() {
            return Err("deriv tick: subscription symbol mismatch".into());
        }
        precision(&body.tick.pip_size, *scale)?;
        body.tick.quote.require_number()?;
        let (generation, sequence) = response
            .stamp
            .ok_or("deriv tick: receipt sequence missing")?;
        Ok(LiveEvent::Observation(LiveObservation {
            instrument: instrument.clone(),
            provider_time_micros: micros(body.tick.epoch)?,
            price_units: body.tick.quote.price_units(*scale)?,
            receipt_micros: response.receipt_micros,
            generation,
            sequence,
            payload_sha256: payload_hash(&response.raw),
            source: self.source.clone(),
        }))
    }
}
impl MarketDataBroker for DerivMarketData {
    fn discover(&mut self) -> Result<Vec<DiscoveredInstrument>, String> {
        let response = self
            .connection
            .request(RateGroup::Other, "active_symbols", |req_id| {
                DiscoverRequest {
                    active_symbols: "full",
                    req_id,
                }
            })?;
        let response: SymbolsResponse = decode(&response.raw, "active_symbols")?;
        response
            .active_symbols
            .into_iter()
            .map(|symbol| {
                if symbol.exchange_is_open > 1 || symbol.is_trading_suspended > 1 {
                    return Err("deriv active_symbols: invalid market status".into());
                }
                Ok(DiscoveredInstrument {
                    symbol: symbol.underlying_symbol,
                    display_name: Some(symbol.underlying_symbol_name),
                    precision: Some(pip_digits(&symbol.pip_size)?),
                    open: Some(symbol.exchange_is_open == 1 && symbol.is_trading_suspended == 0),
                })
            })
            .collect()
    }
    fn history_page(
        &mut self,
        instrument: &InstrumentId,
        scale: PriceScale,
        before_micros: Option<i64>,
    ) -> Result<HistoryPage, String> {
        self.check_instrument(instrument)?;
        let end = before_micros.map_or_else(
            || "latest".to_string(),
            |t| t.div_euclid(1_000_000).to_string(),
        );
        let response = self
            .connection
            .request(RateGroup::Other, "history", |req_id| HistoryRequest {
                ticks_history: instrument.provider_symbol.as_str(),
                style: "ticks",
                end,
                // ticks_history_request.schema.json:20-24 declares no maximum; the retained request used 100.
                count: 100,
                req_id,
            })?;
        let body: HistoryResponse = decode(&response.raw, "history")?;
        precision(&body.pip_size, scale)?;
        if body.history.prices.len() != body.history.times.len() {
            return Err("deriv history: ragged prices and times arrays".into());
        }
        let rows = body
            .history
            .times
            .into_iter()
            .zip(body.history.prices)
            .map(|(time, price)| {
                price.require_number()?;
                Ok(Tick {
                    event_time_micros: micros(time)?,
                    price_units: price.price_units(scale)?,
                })
            })
            .collect::<Result<Vec<_>, String>>()?;
        Ok(HistoryPage {
            raw: response.raw,
            anchor_token: before_micros.map(|t| t.div_euclid(1_000_000).to_string()),
            rows,
        })
    }
    fn subscribe(&mut self, instrument: &InstrumentId, scale: PriceScale) -> Result<(), String> {
        self.check_instrument(instrument)?;
        if self.active.contains_key(instrument) {
            return Err("deriv ticks: instrument already subscribed".into());
        }
        let response = self
            .connection
            .request(RateGroup::Other, "tick", |req_id| TicksRequest {
                ticks: instrument.provider_symbol.as_str(),
                subscribe: 1,
                req_id,
            })?;
        let id = response
            .header
            .subscription
            .as_ref()
            .ok_or("deriv ticks: subscription id missing")?
            .id
            .clone();
        if self.subscriptions.contains_key(&id) {
            return Err("deriv ticks: reused subscription identity".into());
        }
        self.connection.subscriptions.insert(id.clone());
        self.active.insert(instrument.clone(), id.clone());
        self.subscriptions.insert(id, (instrument.clone(), scale));
        self.connection.queued.push_back(response);
        Ok(())
    }
    fn next_live(&mut self, timeout_micros: i64) -> Result<Option<LiveEvent>, String> {
        if let Some(event) = self.events.pop_front() {
            return Ok(Some(event));
        }
        self.connection
            .next(timeout_micros)?
            .map(|response| self.observation(response))
            .transpose()
    }
    fn unsubscribe(&mut self, instrument: &InstrumentId) -> Result<Cancellation, String> {
        let id = self
            .active
            .get(instrument)
            .cloned()
            .ok_or("deriv forget: instrument is not subscribed")?;
        let response = self
            .connection
            .request(RateGroup::Other, "forget", |req_id| ForgetRequest {
                forget: &id,
                req_id,
            })?;
        let body: ForgetResponse = decode(&response.raw, "forget")?;
        if body.forget != 1 {
            return Err("deriv forget: cancellation was not acknowledged".into());
        }
        // Keep the mapping for already received ticks; it no longer owns an active subscription.
        self.active.remove(instrument);
        Ok(Cancellation::Acknowledged)
    }
    fn reconnect(&mut self) -> Result<(), String> {
        let generation = self.connection.reconnect()?;
        self.subscriptions.clear();
        self.active.clear();
        self.events.clear();
        self.events.push_back(LiveEvent::Break {
            generation,
            reason: "explicit reconnect; subscriptions and causal warm-up must be rebuilt".into(),
        });
        Ok(())
    }
    fn continuity(&self) -> &Continuity {
        self.connection.continuity()
    }
}

/// The validated one-time address stays private and is never included in diagnostics.
pub struct AuthenticatedAddress {
    url: String,
    account: String,
    pub currency: Currency,
    pub account_class: AccountClass,
}
pub struct DerivAccounts;
#[derive(Deserialize)]
struct Accounts {
    data: Vec<Account>,
}
#[derive(Deserialize)]
struct Account {
    account_id: String,
    account_type: AccountClass,
    status: String,
    currency: Currency,
}
#[derive(Deserialize)]
struct AddressResponse {
    data: AddressData,
}
#[derive(Deserialize)]
struct AddressData {
    url: String,
}
impl DerivAccounts {
    pub fn bootstrap(
        settings: &DerivSettings,
        http: &mut dyn Http,
        credential: &str,
    ) -> Result<AuthenticatedAddress, String> {
        let class = settings
            .account_class
            .ok_or("deriv bootstrap: account_class is required")?;
        let headers = vec![
            ("Authorization".into(), format!("Bearer {credential}")),
            ("Deriv-App-ID".into(), settings.app_id.clone()),
        ];
        let base = settings.bootstrap_endpoint.trim_end_matches('/');
        let bytes = http.get_json(&format!("{base}/accounts"), &headers)?;
        // Bootstrap parse errors omit provider values, including account identifiers and OTPs.
        let accounts: Accounts = serde_json::from_slice(&bytes)
            .map_err(|_| "deriv bootstrap: malformed accounts response")?;
        if accounts
            .data
            .iter()
            .any(|a| !matches!(a.status.as_str(), "active" | "inactive"))
        {
            return Err("deriv bootstrap: invalid account status".into());
        }
        let mut matching = accounts
            .data
            .into_iter()
            .filter(|a| a.account_type == class && a.status == "active");
        let account = matching
            .next()
            .ok_or("deriv bootstrap: no active account of configured class")?;
        if matching.next().is_some() {
            return Err("deriv bootstrap: multiple active accounts of configured class".into());
        }
        if account.account_id.is_empty()
            || !account
                .account_id
                .bytes()
                .all(|b| b.is_ascii_alphanumeric())
        {
            return Err("deriv bootstrap: unsupported account identifier".into());
        }
        let bytes = http.post_json(
            &format!("{base}/accounts/{}/otp", account.account_id),
            &headers,
        )?;
        let response: AddressResponse = serde_json::from_slice(&bytes)
            .map_err(|_| "deriv bootstrap: malformed address response")?;
        let url = reqwest::Url::parse(&response.data.url)
            .map_err(|_| "deriv bootstrap: invalid authenticated address")?;
        let local = settings.bootstrap_endpoint.starts_with("http://");
        if !(url.scheme() == "wss" || local && url.scheme() == "ws")
            || url.host_str().is_none()
            || url.path() != format!("/trading/v1/options/ws/{}", class.as_str())
            || !url.username().is_empty()
            || url.password().is_some()
            || url.fragment().is_some()
        {
            return Err("deriv bootstrap: authenticated address does not match configured account class and transport".into());
        }
        Ok(AuthenticatedAddress {
            url: response.data.url,
            account: account.account_id,
            currency: account.currency,
            account_class: class,
        })
    }
    pub fn resolve(
        settings: &DerivSettings,
        http: &mut dyn Http,
    ) -> Result<AuthenticatedAddress, String> {
        let reference = settings
            .credential
            .as_deref()
            .ok_or("deriv bootstrap: credential reference is required")?;
        Self::bootstrap(settings, http, &super::resolve_secret(reference)?)
    }
}
#[derive(Serialize)]
struct BalanceRequest {
    balance: u8,
    req_id: u64,
}
#[derive(Deserialize)]
struct BalanceResponse {
    balance: WireBalance,
}
#[derive(Deserialize)]
struct WireBalance {
    balance: WireDecimal,
    currency: Currency,
    loginid: String,
}
#[derive(Debug, Serialize)]
pub struct Balance {
    pub amount: Decimal,
    pub currency: Currency,
    pub account_class: AccountClass,
}
pub struct DerivAuthenticated {
    connection: DerivConnection,
    account: String,
    currency: Currency,
    account_class: AccountClass,
}
impl DerivAuthenticated {
    pub fn connect(
        address: AuthenticatedAddress,
        connector: Box<dyn Connector>,
        clock: Box<dyn Clock>,
        limits: RateBudgets,
    ) -> Result<Self, String> {
        Ok(Self {
            connection: DerivConnection::connect(&address.url, connector, clock, limits)?,
            account: address.account,
            currency: address.currency,
            account_class: address.account_class,
        })
    }
    pub fn balance(&mut self) -> Result<Balance, String> {
        let response = self
            .connection
            .request(RateGroup::Account, "balance", |req_id| BalanceRequest {
                balance: 1,
                req_id,
            })?;
        let response: BalanceResponse = decode(&response.raw, "balance")?;
        if response.balance.currency != self.currency || response.balance.loginid != self.account {
            return Err("deriv balance: account or currency mismatch".into());
        }
        let amount = response.balance.balance.require_number()?;
        if amount.is_negative() {
            return Err("deriv balance: negative balance".into());
        }
        Ok(Balance {
            amount,
            currency: self.currency.clone(),
            account_class: self.account_class,
        })
    }
}

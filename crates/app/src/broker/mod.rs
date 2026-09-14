pub mod deriv;
pub mod pocket_option;
pub mod socket_io;
pub mod transport;
pub mod wire;

use std::collections::VecDeque;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use binary_alpha_engine::config::{AccountClass, Broker, Config, RateBudgets, RateLimit};
pub use binary_alpha_engine::config::{BrokerKind, Capabilities};
use binary_alpha_engine::execution::{
    BrokerLiability, CashFact, ContractSemantics, Decimal, Direction, EventSource, Settlement,
    TerminalFact,
};
use binary_alpha_engine::market::{BrokerId, Currency, InstrumentId, PriceScale, Tick};
use serde::Serialize;
use sha2::{Digest, Sha256};

/// Resolves a reference without including its value in diagnostics.
pub fn resolve_secret(reference: &str) -> Result<String, String> {
    std::env::var(reference)
        .ok()
        .filter(|value| !value.is_empty())
        .ok_or_else(|| format!("credential reference {reference} is unavailable"))
}

pub trait Clock {
    fn now_micros(&self) -> i64;
    fn sleep(&mut self, micros: i64);
}
pub struct SystemClock;
impl Clock for SystemClock {
    fn now_micros(&self) -> i64 {
        match SystemTime::now().duration_since(UNIX_EPOCH) {
            Ok(time) => i64::try_from(time.as_micros()).expect("system clock fits microseconds"),
            Err(error) => -i64::try_from(error.duration().as_micros())
                .expect("system clock fits microseconds"),
        }
    }
    fn sleep(&mut self, micros: i64) {
        if micros > 0 {
            std::thread::sleep(Duration::from_micros(micros as u64));
        }
    }
}

/// Normalized receipt provenance; sequence is local, never a provider sequence.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LiveObservation {
    pub instrument: InstrumentId,
    pub provider_time_micros: i64,
    pub price_units: i64,
    pub receipt_micros: i64,
    pub generation: u64,
    pub sequence: u64,
    pub payload_sha256: String,
    pub source: String,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LiveEvent {
    Observation(LiveObservation),
    Break { generation: u64, reason: String },
}

/// One raw provider page and its directly normalized rows in provider order.
#[derive(Debug, Clone)]
pub struct HistoryPage {
    pub raw: Vec<u8>,
    pub anchor_token: Option<String>,
    pub rows: Vec<Tick>,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Cancellation {
    Acknowledged,
    SentWithoutAcknowledgement,
}
#[derive(Debug, Clone, Serialize)]
pub struct DiscoveredInstrument {
    pub symbol: String,
    pub display_name: Option<String>,
    pub precision: Option<u8>,
    pub open: Option<bool>,
}

/// Producer receipt numbering or an independent consumer's expected next receipt.
#[derive(Debug, Clone)]
pub struct Continuity {
    generation: u64,
    next_sequence: u64,
}
impl Default for Continuity {
    fn default() -> Self {
        Self {
            generation: 0,
            next_sequence: 1,
        }
    }
}
impl Continuity {
    pub fn generation(&self) -> u64 {
        self.generation
    }
    pub fn assign(&mut self) -> Result<(u64, u64), String> {
        let sequence = self.next_sequence;
        self.next_sequence = sequence.checked_add(1).ok_or("receipt sequence overflow")?;
        Ok((self.generation, sequence))
    }
    pub fn accept(&mut self, observation: &LiveObservation) -> Result<(), String> {
        if observation.generation != self.generation || observation.sequence != self.next_sequence {
            return Err(format!(
                "continuity loss or reordering: expected generation {} sequence {}, received generation {} sequence {}",
                self.generation, self.next_sequence, observation.generation, observation.sequence
            ));
        }
        self.assign()?;
        Ok(())
    }
    pub fn reconnect(&mut self) -> Result<u64, String> {
        self.generation = self
            .generation
            .checked_add(1)
            .ok_or("connection generation overflow")?;
        self.next_sequence = 1;
        Ok(self.generation)
    }
}

pub trait MarketDataBroker {
    fn discover(&mut self) -> Result<Vec<DiscoveredInstrument>, String>;
    fn history_page(
        &mut self,
        instrument: &InstrumentId,
        scale: PriceScale,
        before_micros: Option<i64>,
    ) -> Result<HistoryPage, String>;
    fn subscribe(&mut self, instrument: &InstrumentId, scale: PriceScale) -> Result<(), String>;
    fn next_live(&mut self, timeout_micros: i64) -> Result<Option<LiveEvent>, String>;
    fn unsubscribe(&mut self, instrument: &InstrumentId) -> Result<Cancellation, String>;
    fn reconnect(&mut self) -> Result<(), String>;
    fn continuity(&self) -> &Continuity;
}

#[derive(Debug, Clone, Copy)]
pub enum RateGroup {
    Trade,
    Account,
    Portfolio,
    Other,
}
struct Window {
    limit: RateLimit,
    requests: VecDeque<i64>,
}
/// A connection's documented minute and hour sliding request windows.
pub struct RateBudget {
    windows: [Window; 4],
}
impl RateBudget {
    pub fn new(limits: RateBudgets) -> Result<Self, String> {
        limits.validate()?;
        Ok(Self {
            windows: [limits.trade, limits.account, limits.portfolio, limits.other].map(|limit| {
                Window {
                    limit,
                    requests: VecDeque::new(),
                }
            }),
        })
    }
    pub fn admit(&mut self, group: RateGroup, clock: &mut dyn Clock) {
        let window = &mut self.windows[group as usize];
        loop {
            let now = clock.now_micros();
            while window
                .requests
                .front()
                .is_some_and(|time| now.saturating_sub(*time) >= 3_600_000_000)
            {
                window.requests.pop_front();
            }
            let mut wait = 0;
            if window.requests.len() >= window.limit.per_hour as usize {
                wait = window.requests[0]
                    .saturating_add(3_600_000_000)
                    .saturating_sub(now);
            }
            if let Some(oldest) = window
                .requests
                .iter()
                .rev()
                .nth(window.limit.per_minute as usize - 1)
                && now.saturating_sub(*oldest) < 60_000_000
            {
                wait = wait.max(oldest.saturating_add(60_000_000).saturating_sub(now));
            }
            if wait <= 0 {
                window.requests.push_back(now);
                return;
            }
            clock.sleep(wait);
        }
    }
}

/// Binds normalized history to its provider mapping, including the declared source clock.
pub fn source_identity(settings: &Broker) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"binary-alpha broker market source v1\n");
    hasher.update(settings.kind().as_str().as_bytes());
    hasher.update(b"\n");
    hasher.update(settings.endpoint().as_bytes());
    hasher.update(b"\n");
    match settings {
        Broker::Deriv(_) => hasher.update(deriv::SCHEMA.as_bytes()),
        Broker::PocketOption(settings) => hasher.update(
            format!(
                "socket_io_v1\n{}\n{}",
                settings.account_class, settings.server_offset_minutes
            )
            .as_bytes(),
        ),
    }
    binary_alpha_engine::hex(&hasher.finalize())
}

pub(crate) fn payload_hash(bytes: &[u8]) -> String {
    binary_alpha_engine::hex(&Sha256::digest(bytes))
}

/// The two compiled market adapters; no dynamic registration is involved.
pub enum Adapter {
    Deriv(deriv::DerivMarketData),
    PocketOption(pocket_option::PocketMarketData),
}
impl Adapter {
    pub fn market(&mut self) -> &mut dyn MarketDataBroker {
        match self {
            Self::Deriv(broker) => broker,
            Self::PocketOption(broker) => broker,
        }
    }
}
pub fn connect(config: &Config) -> Result<Adapter, String> {
    let history = config
        .history
        .as_ref()
        .ok_or("history: configuration declares no history table")?;
    let settings = config
        .brokers
        .iter()
        .find(|broker| broker.id() == &history.broker)
        .ok_or("history: broker is not declared")?;
    let connector = Box::new(transport::WebSocketConnector::new()?);
    let clock = Box::new(SystemClock);
    match settings {
        Broker::Deriv(settings) => Ok(Adapter::Deriv(deriv::DerivMarketData::connect(
            settings, connector, clock,
        )?)),
        Broker::PocketOption(settings) => {
            let instruments = history
                .instruments
                .iter()
                .map(|symbol| InstrumentId {
                    broker: history.broker.clone(),
                    provider_symbol: symbol.clone(),
                })
                .collect::<Vec<_>>();
            Ok(Adapter::PocketOption(
                pocket_option::PocketMarketData::connect(
                    settings,
                    &instruments,
                    connector,
                    clock,
                    resolve_secret(&settings.credential)?,
                )?,
            ))
        }
    }
}

/// The configured account identity; provider login identifiers stay inside the adapter.
#[derive(Debug, Clone)]
pub struct AccountIdentity {
    pub broker: BrokerId,
    pub account: String,
    pub class: AccountClass,
    pub currency: Currency,
}
#[derive(Debug, Clone)]
pub struct ProposalRequest {
    pub binding: String,
    pub instrument: InstrumentId,
    pub scale: PriceScale,
    pub direction: Direction,
    pub duration_seconds: u32,
    pub stake: Decimal,
    pub currency: Currency,
    pub semantics: ContractSemantics,
    pub settlement: Settlement,
}
#[derive(Debug, Clone)]
pub struct PreparedPurchase {
    pub dispatch_claim: String,
    pub command: String,
    pub proposal_identity: String,
    pub maximum_price: Decimal,
}
#[derive(Debug, Clone)]
pub enum PurchaseOutcome {
    Accepted {
        debit: Decimal,
        liability: BrokerLiability,
        receipt_micros: i64,
    },
    Rejected {
        code: String,
        receipt_micros: i64,
    },
    ProvenNotSent {
        reason: String,
    },
    PossiblySent {
        reason: String,
    },
}
/// Confirmed facts with provider provenance, ready for the single Engine mapping.
#[derive(Debug, Clone)]
pub enum AccountEvent {
    TransactionAcknowledged,
    Cash {
        fact: CashFact,
        receipt_micros: i64,
    },
    ContractUpdate {
        contract_ref: String,
        source: EventSource,
        entry_price_units: Option<i64>,
        entry_time_micros: Option<i64>,
        start_micros: Option<i64>,
        expiry_micros: Option<i64>,
    },
    Terminal {
        contract_ref: String,
        source: EventSource,
        fact: TerminalFact,
    },
}
#[derive(Debug, Clone)]
pub struct OpenContract {
    pub contract_ref: String,
    pub transaction_ref: String,
    pub buy_price: Decimal,
    pub payout: Decimal,
    pub purchase_time_micros: i64,
    pub start_micros: Option<i64>,
    pub expiry_micros: Option<i64>,
    pub instrument: String,
    pub direction: Direction,
}

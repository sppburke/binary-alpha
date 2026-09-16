pub mod deriv;
pub mod pocket_option;
pub mod socket_io;
pub mod transport;
pub mod wire;

use std::collections::VecDeque;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use binary_alpha_engine::config::{AccountClass, Broker, Config, RateBudgets, RateLimit};
pub use binary_alpha_engine::config::{BrokerKind, Capabilities};
use binary_alpha_engine::dataset::NativeGranularity;
use binary_alpha_engine::execution::{
    BrokerLiability, CashFact, ContractSemantics, Decimal, Direction, EventSource, Settlement,
    TerminalFact,
};
use binary_alpha_engine::market::{Bar, BrokerId, Currency, InstrumentId, PriceScale, Tick};
use serde::Serialize;
use sha2::{Digest, Sha256};

/// Resolves a reference without including its value in diagnostics.
pub fn resolve_secret(reference: &str) -> Result<String, String> {
    std::env::var(reference)
        .ok()
        .filter(|value| !value.is_empty())
        .ok_or_else(|| format!("credential reference {reference} is unavailable"))
}

pub trait Clock: Send + Sync {
    fn now_micros(&self) -> i64;
    fn sleep(&mut self, micros: i64);
    /// Wait for an absolute clock deadline; concurrent time advancement cannot move it.
    fn sleep_until(&mut self, deadline_micros: i64) {
        self.sleep(deadline_micros.saturating_sub(self.now_micros()).max(0));
    }
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

/// The rows one history page decodes to, at the granularity the request named.
#[derive(Debug, Clone, PartialEq)]
pub enum HistoryRows {
    Ticks(Vec<Tick>),
    Bars(Vec<Bar>),
}
impl HistoryRows {
    pub fn len(&self) -> usize {
        match self {
            Self::Ticks(rows) => rows.len(),
            Self::Bars(rows) => rows.len(),
        }
    }
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
    /// The tick rows of a tick page.
    pub fn ticks(&self) -> Option<&[Tick]> {
        match self {
            Self::Ticks(rows) => Some(rows),
            Self::Bars(_) => None,
        }
    }
    pub fn into_ticks(self) -> Option<Vec<Tick>> {
        match self {
            Self::Ticks(rows) => Some(rows),
            Self::Bars(_) => None,
        }
    }
    /// The provider event time of the first row: the tick time or the bar start.
    pub fn first_time_micros(&self) -> Option<i64> {
        match self {
            Self::Ticks(rows) => rows.first().map(|row| row.event_time_micros),
            Self::Bars(rows) => rows.first().map(|row| row.start_unix_s * 1_000_000),
        }
    }
    pub fn last_time_micros(&self) -> Option<i64> {
        match self {
            Self::Ticks(rows) => rows.last().map(|row| row.event_time_micros),
            Self::Bars(rows) => rows.last().map(|row| row.start_unix_s * 1_000_000),
        }
    }
}

/// One raw provider page whose envelope matched the request, with its local receipt; the
/// caller retains the bytes before `decode_history` can reject their rows.
#[derive(Debug, Clone)]
pub struct HistoryPage {
    pub raw: Vec<u8>,
    pub anchor_token: Option<String>,
    /// Local receipt time of the response on the adapter's clock.
    pub receipt_micros: i64,
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

pub trait MarketDataBroker: Send {
    fn discover(&mut self) -> Result<Vec<DiscoveredInstrument>, String>;
    /// One bounded page of native history ending before `before_micros` (the latest page when
    /// absent) at the requested granularity, returned once its envelope matches the request; an
    /// adapter refuses a granularity it cannot serve before it sends anything.
    fn history_page(
        &mut self,
        instrument: &InstrumentId,
        scale: PriceScale,
        before_micros: Option<i64>,
        granularity: NativeGranularity,
    ) -> Result<HistoryPage, String>;
    /// Decodes and validates the rows of a page's bytes, returning the provider's numeric
    /// instrument identifier when its rows carry one; pure, so retained pages replay exactly.
    fn decode_history(
        &self,
        instrument: &InstrumentId,
        raw: &[u8],
        scale: PriceScale,
        granularity: NativeGranularity,
    ) -> Result<(Option<i32>, HistoryRows), String>;
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
            clock.sleep_until(now.saturating_add(wait));
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
    match settings {
        Broker::Deriv(settings) => Ok(Adapter::Deriv(deriv::DerivMarketData::connect(
            settings,
            Box::new(transport::WebSocketConnector::new()?),
            Box::new(SystemClock),
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
            let attempt = |credential: String| {
                pocket_option::PocketMarketData::connect(
                    settings,
                    &instruments,
                    Box::new(transport::WebSocketConnector::new()?),
                    Box::new(SystemClock),
                    credential,
                )
            };
            let credential = match resolve_secret(&settings.credential) {
                Ok(credential) => credential,
                Err(reason) => match &settings.credential_command {
                    Some(command) => renew_credential(command)?,
                    None => return Err(reason),
                },
            };
            match attempt(credential) {
                Ok(adapter) => Ok(Adapter::PocketOption(adapter)),
                // A rejected or stale session is renewed once through the operator's command;
                // any other failure of the fresh session is reported as is.
                Err(reason) => match &settings.credential_command {
                    Some(command) => attempt(renew_credential(command)?)
                        .map(Adapter::PocketOption)
                        .map_err(|renewed| {
                            format!("{renewed} (after credential renewal; first attempt: {reason})")
                        }),
                    None => Err(reason),
                },
            }
        }
    }
}

/// Runs the operator's credential command and returns the authentication object it printed,
/// without letting the value into any diagnostic.
pub fn renew_credential(command: &[String]) -> Result<String, String> {
    // Parallel jobs of one broker renew one at a time; a browser login must not race itself.
    static RENEWAL: std::sync::Mutex<()> = std::sync::Mutex::new(());
    let _serial = RENEWAL
        .lock()
        .map_err(|_| "credential_command: renewal lock poisoned")?;
    let (program, arguments) = command
        .split_first()
        .ok_or("credential_command must name a program")?;
    let output = std::process::Command::new(program)
        .args(arguments)
        .stdin(std::process::Stdio::null())
        .output()
        .map_err(|error| format!("credential_command {program}: {error}"))?;
    if !output.status.success() {
        return Err(format!(
            "credential_command {program}: exited with {}",
            output.status
        ));
    }
    let credential = String::from_utf8(output.stdout)
        .map_err(|_| format!("credential_command {program}: output is not UTF-8"))?;
    let credential = credential.trim().to_string();
    if credential.is_empty() {
        return Err(format!("credential_command {program}: printed nothing"));
    }
    Ok(credential)
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

#[cfg(test)]
mod clock_api_regressions {
    use super::Clock;
    struct TimeOnly;
    impl Clock for TimeOnly {
        fn now_micros(&self) -> i64 {
            1
        }
        fn sleep(&mut self, _: i64) {}
    }
    // Compile-time regression: these calls become ambiguous if scheduler lifecycle methods
    // are added to Clock, even when those methods have default implementations.
    trait SessionOwner {
        fn complete(&self) {}
        fn cancel(&self) {}
        fn wake(&self, _: &str) {}
        fn begin(&self, _: &str) {}
        fn stalled(&self) -> bool {
            false
        }
        fn failure(&self) -> Option<String> {
            None
        }
    }
    impl SessionOwner for TimeOnly {}
    #[test]
    fn clock_does_not_claim_session_lifecycle_methods() {
        let clock = TimeOnly;
        clock.complete();
        clock.cancel();
        clock.wake("account");
        clock.begin("account");
        assert!(!clock.stalled());
        assert_eq!(clock.failure(), None);
        assert_eq!(clock.now_micros(), 1);
    }
}

//! The Pocket demo account session. Market quotes use a separate connection.
use super::socket_io::{self, Event};
use super::transport::{Connector, Frame, Transport};
use super::wire::WireDecimal;
use super::{
    AccountEvent, AccountIdentity, Clock, LiveObservation, OpenContract, PreparedPurchase,
    PurchaseOutcome, Statement, StatementCoverage, payload_hash,
};
use binary_alpha_engine::config::{AccountClass, PocketPayout, PocketSettings};
use binary_alpha_engine::execution::{
    BrokerLiability, CashAction, CashFact, Decimal, Direction, EventSource, Proposal, TerminalFact,
    TerminalStatus,
};
use binary_alpha_engine::market::{Currency, InstrumentId, PriceScale};
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;
use std::collections::{BTreeMap, BTreeSet, VecDeque};

const SCHEMA: &str = "pocket:assumed_offer_v1";

#[derive(Debug, Clone)]
pub struct Listing {
    pub listed_percent: u8,
    pub receipt_micros: i64,
    pub frame_sha256: String,
}

pub fn mapped_percent(listed: u8, rule: PocketPayout) -> Result<u8, String> {
    if listed > 100 {
        return Err("pocket listing: percent exceeds 100".into());
    }
    Ok(u16::from(rule.cap_percent).min(u16::from(listed) + u16::from(rule.add_percent)) as u8)
}

pub fn gross_return(stake: Decimal, listed: u8, rule: PocketPayout) -> Result<Decimal, String> {
    let percent = mapped_percent(listed, rule)?;
    stake.checked_mul(
        Decimal::parse("1")?.checked_add(
            Decimal::parse(&percent.to_string())?.checked_mul(Decimal::parse("0.01")?)?,
        )?,
    )
}

pub fn fresh(listing: &Listing, at: i64, rule: PocketPayout) -> bool {
    at.checked_sub(listing.receipt_micros)
        .zip(i64::from(rule.max_age_seconds).checked_mul(1_000_000))
        .is_some_and(|(age, limit)| age >= 0 && age <= limit)
}

pub fn offer(
    request: &super::ProposalRequest,
    template: &binary_alpha_engine::execution::ContractTerms,
    account: &AccountIdentity,
    quote: &LiveObservation,
    listing: &Listing,
    rule: PocketPayout,
) -> Result<Proposal, String> {
    if !fresh(listing, quote.receipt_micros, rule) {
        return Err("pocket offer: listing unavailable or stale".into());
    }
    if template.direction != request.direction
        || template.duration_micros != i64::from(request.duration_seconds) * 1_000_000
        || template.stake != request.stake
        || template.currency != request.currency
        || template.settlement != request.settlement
        || template.semantics != Some(request.semantics)
    {
        return Err("pocket offer: request differs from frozen terms".into());
    }
    let gross = gross_return(request.stake, listing.listed_percent, rule)?;
    let identity = binary_alpha_engine::research::digest(
        b"pocket assumed offer v1\n",
        &serde_json::to_vec(&(
            &request.binding,
            &request.instrument,
            request.direction,
            request.duration_seconds,
            request.stake,
            &request.currency,
            template,
            quote.price_units,
            quote.provider_time_micros,
            quote.receipt_micros,
            &quote.payload_sha256,
            listing.listed_percent,
            listing.receipt_micros,
            &listing.frame_sha256,
            rule,
        ))
        .map_err(|error| error.to_string())?,
    );
    let mut terms = template.clone();
    terms.id = identity.clone();
    terms.win.gross_return = gross;
    let mut proposal = Proposal {
        identity: identity.clone(),
        request_identity: String::new(),
        account: account.account.clone(),
        instrument: request.instrument.to_string(),
        terms,
        spot_units: quote.price_units,
        spot_time_micros: quote.provider_time_micros,
        receipt_micros: quote.receipt_micros,
        schema: SCHEMA.into(),
        payload_sha256: listing.frame_sha256.clone(),
    };
    proposal.request_identity = proposal.canonical_request_identity()?;
    Ok(proposal)
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Deal {
    pub id: String,
    pub asset: String,
    pub command: u8,
    pub amount: WireDecimal,
    pub profit: WireDecimal,
    #[serde(rename = "percentProfit")]
    pub percent_profit: WireDecimal,
    #[serde(rename = "openPrice")]
    pub open_price: WireDecimal,
    #[serde(rename = "closePrice")]
    pub close_price: Option<WireDecimal>,
    #[serde(rename = "openTimestamp")]
    pub open_seconds: i64,
    #[serde(rename = "openMs")]
    pub open_ms: u16,
    #[serde(rename = "closeTimestamp")]
    pub close_seconds: i64,
    #[serde(rename = "closeMs")]
    pub close_ms: Option<u16>,
    #[serde(rename = "isDemo")]
    pub is_demo: u8,
    pub currency: Option<Currency>,
    #[serde(rename = "requestId")]
    pub request_id: Option<u64>,
    #[serde(rename = "optionType")]
    pub option_type: u16,
}

impl Deal {
    pub fn direction(&self) -> Result<Direction, String> {
        match self.command {
            0 => Ok(Direction::Buy),
            1 => Ok(Direction::Sell),
            _ => Err("pocket deal: unsupported direction".into()),
        }
    }
    fn time(&self, seconds: i64, ms: u16, offset: i32) -> Result<i64, String> {
        if ms >= 1_000 {
            return Err("pocket deal: millisecond field is out of range".into());
        }
        seconds
            .checked_sub(i64::from(offset) * 60)
            .and_then(|s| s.checked_mul(1_000_000))
            .and_then(|s| s.checked_add(i64::from(ms) * 1_000))
            .ok_or("pocket deal: timestamp overflow".into())
    }
    pub fn entry_time(&self, offset: i32) -> Result<i64, String> {
        self.time(self.open_seconds, self.open_ms, offset)
    }
    pub fn expiry_time(&self, offset: i32) -> Result<i64, String> {
        self.time(self.close_seconds, 0, offset)
    }
    pub fn close_time(&self, offset: i32) -> Result<i64, String> {
        self.time(self.close_seconds, self.close_ms.unwrap_or(0), offset)
    }
    pub fn gross_credit(&self) -> Result<Decimal, String> {
        self.amount
            .require_number()?
            .checked_add(self.profit.require_number()?)
    }
    pub fn payout(&self) -> Result<Decimal, String> {
        self.amount.require_number()?.checked_mul(
            Decimal::parse("1")?.checked_add(
                self.percent_profit
                    .require_number()?
                    .checked_mul(Decimal::parse("0.01")?)?,
            )?,
        )
    }
    fn validate(&self, account: &AccountIdentity) -> Result<(), String> {
        if self.id.is_empty()
            || self.is_demo != 1
            || self.option_type != 100
            || self.currency.as_ref() != Some(&account.currency)
            || self.amount.require_number()?.coefficient() <= 0
        {
            return Err("pocket deal: identity, demo class, currency, or amount mismatch".into());
        }
        self.direction()?;
        self.open_price.require_number()?;
        self.close_price
            .as_ref()
            .map(WireDecimal::require_number)
            .transpose()?;
        self.profit.require_number()?;
        self.percent_profit.require_number()?;
        Ok(())
    }
    pub fn liability(&self, offset: i32) -> Result<BrokerLiability, String> {
        Ok(BrokerLiability {
            contract_ref: self.id.clone(),
            transaction_ref: self.id.clone(),
            purchase_time_micros: self.entry_time(offset)?,
            expected_start_micros: Some(self.entry_time(offset)?),
            payout: self.payout()?,
        })
    }
    pub fn update(
        &self,
        scale: PriceScale,
        offset: i32,
        receipt: i64,
    ) -> Result<AccountEvent, String> {
        Ok(AccountEvent::ContractUpdate {
            contract_ref: self.id.clone(),
            source: self.source("entry", self.entry_time(offset)?, receipt),
            entry_price_units: Some(self.open_price.price_units(scale)?),
            entry_time_micros: Some(self.entry_time(offset)?),
            start_micros: Some(self.entry_time(offset)?),
            expiry_micros: Some(self.expiry_time(offset)?),
        })
    }
    fn source(&self, suffix: &str, provider: i64, receipt: i64) -> EventSource {
        EventSource {
            id: format!("pocket:deal:{}:{suffix}", self.id),
            provider_time_micros: provider,
            available_at_micros: receipt,
            simulated: false,
        }
    }
    pub fn terminal(
        &self,
        scale: PriceScale,
        offset: i32,
        receipt: i64,
    ) -> Result<AccountEvent, String> {
        let exit = self
            .close_price
            .as_ref()
            .ok_or("pocket deal: close price missing")?
            .price_units(scale)?;
        let entry = self.open_price.price_units(scale)?;
        let won = match self.direction()? {
            Direction::Buy => exit > entry,
            Direction::Sell => exit < entry,
        };
        let at = self.close_time(offset)?;
        Ok(AccountEvent::Terminal {
            contract_ref: self.id.clone(),
            source: self.source("terminal", at, receipt),
            fact: TerminalFact {
                status: if won {
                    TerminalStatus::Won
                } else {
                    TerminalStatus::Lost
                },
                exit_price_units: Some(exit),
                exit_time_micros: Some(at),
                transaction_ref: Some(format!("{}:close", self.id)),
            },
        })
    }
    pub fn cash(
        &self,
        account: &AccountIdentity,
        offset: i32,
        receipt: i64,
    ) -> Result<AccountEvent, String> {
        Ok(AccountEvent::Cash {
            fact: CashFact {
                account: account.account.clone(),
                transaction_ref: format!("{}:close", self.id),
                contract_ref: Some(self.id.clone()),
                action: CashAction::Sell,
                amount: self.gross_credit()?,
                time_micros: self.close_time(offset)?,
            },
            receipt_micros: receipt,
        })
    }
}

#[derive(Debug, Clone)]
pub struct Encoded {
    pub prepared: PreparedPurchase,
    pub request_id: u64,
    pub text: String,
    pub listing: Listing,
}
impl Encoded {
    pub fn command(&self) -> &str {
        &self.prepared.command
    }
}

pub struct PocketOptions {
    settings: PocketSettings,
    account: AccountIdentity,
    instruments: BTreeMap<String, PriceScale>,
    transport: Box<dyn Transport>,
    clock: Box<dyn Clock>,
    session: socket_io::Session,
    events: VecDeque<AccountEvent>,
    listings: BTreeMap<String, Listing>,
    opened: Vec<Deal>,
    closed: BTreeMap<(String, String), Deal>,
    balance: Option<Decimal>,
    written: BTreeSet<String>,
    request_ids: BTreeSet<u64>,
    next_request_id: u64,
    seen_open: BTreeSet<(String, String)>,
}

impl PocketOptions {
    pub fn connect(
        settings: &PocketSettings,
        account: AccountIdentity,
        instruments: &[(InstrumentId, PriceScale)],
        mut connector: Box<dyn Connector>,
        clock: Box<dyn Clock>,
        credential_json: String,
    ) -> Result<Self, String> {
        if settings.account_class != AccountClass::Demo
            || account.class != AccountClass::Demo
            || account.broker != settings.id
            || settings.payout.is_none()
        {
            return Err("pocket options: demo account and payout are required".into());
        }
        serde_json::from_str::<BTreeMap<String, Box<RawValue>>>(&credential_json)
            .map_err(|_| "pocket options: credential must be a JSON object")?;
        let mut selected = BTreeMap::new();
        for (instrument, scale) in instruments {
            if instrument.broker != settings.id
                || selected
                    .insert(instrument.provider_symbol.to_string(), *scale)
                    .is_some()
            {
                return Err("pocket options: invalid instrument scope".into());
            }
        }
        let headers = settings
            .origin
            .as_ref()
            .map(|origin| vec![("Origin".into(), origin.clone())])
            .unwrap_or_default();
        let transport = connector.connect(&settings.endpoint, &headers)?;
        let seed = clock.now_micros().unsigned_abs() % 10_000_000;
        let framing = socket_io::Session::new(clock.now_micros());
        let mut session = Self {
            settings: settings.clone(),
            account,
            instruments: selected,
            transport,
            clock,
            session: framing,
            events: VecDeque::new(),
            listings: BTreeMap::new(),
            opened: Vec::new(),
            closed: BTreeMap::new(),
            balance: None,
            written: BTreeSet::new(),
            request_ids: BTreeSet::new(),
            next_request_id: 10_000_000 + seed,
            seen_open: BTreeSet::new(),
        };
        session.handshake(&credential_json)?;
        Ok(session)
    }
    pub fn account(&self) -> &AccountIdentity {
        &self.account
    }
    pub fn now_micros(&self) -> i64 {
        self.clock.now_micros()
    }
    pub fn rejected(&self, _: &str) -> bool {
        false
    }
    pub fn reserve_request_ids(&mut self, ids: impl IntoIterator<Item = u64>) {
        self.request_ids.extend(ids);
    }
    fn allocate_request_id(&mut self) -> Result<u64, String> {
        if self.request_ids.len() >= 10_000_000 {
            return Err("pocket prepare: request ids exhausted".into());
        }
        loop {
            let id = self.next_request_id;
            self.next_request_id = 10_000_000 + (id - 10_000_000 + 1) % 10_000_000;
            if self.request_ids.insert(id) {
                return Ok(id);
            }
        }
    }
    fn receive(&mut self, timeout: i64) -> Result<Option<Event>, String> {
        self.session
            .receive(&mut *self.transport, &*self.clock, timeout)
            .map_err(Into::into)
    }
    fn handshake(&mut self, credential: &str) -> Result<(), String> {
        let deadline = self.clock.now_micros().saturating_add(12_000_000);
        let (
            mut opened,
            mut connected,
            mut authenticated,
            mut class,
            mut listing,
            mut opened_deals,
            mut closed_deals,
        ) = (false, false, false, false, false, false, false);
        while !(authenticated && class && listing && opened_deals && closed_deals) {
            let remaining = deadline.saturating_sub(self.clock.now_micros());
            if remaining <= 0 {
                return Err("pocket options: handshake deadline reached".into());
            }
            let event = self
                .receive(remaining)?
                .ok_or("pocket options: handshake deadline reached")?;
            match event.name.as_str() {
                "open" if !opened => {
                    self.transport
                        .send(Frame::Text(socket_io::CONNECT.into()))?;
                    opened = true;
                }
                "connected" if opened && !connected => {
                    self.transport
                        .send(Frame::Text(socket_io::encode_event("auth", credential)))?;
                    connected = true;
                }
                "successauth" if connected => {
                    self.session
                        .login(&mut *self.transport, &*self.clock)
                        .map_err(String::from)?;
                    authenticated = true;
                }
                "successupdateBalance" if connected => {
                    #[derive(Deserialize)]
                    struct Balance {
                        #[serde(rename = "isDemo")]
                        is_demo: u8,
                        balance: WireDecimal,
                    }
                    let balance: Balance = serde_json::from_slice(&event.raw)
                        .map_err(|_| "pocket options: invalid balance")?;
                    if balance.is_demo != 1 {
                        return Err("pocket options: server account class mismatch".into());
                    }
                    self.balance = Some(balance.balance.require_number()?);
                    class = true;
                }
                "updateAssets" if connected => {
                    self.ingest_with_login(event, true)?;
                    listing = true;
                }
                "updateOpenedDeals" if connected => {
                    self.ingest_with_login(event, true)?;
                    opened_deals = true;
                }
                "updateClosedDeals" if connected => {
                    self.ingest_with_login(event, true)?;
                    closed_deals = true;
                }
                name if name.starts_with("error") || name.starts_with("fail") => {
                    return Err("pocket options: authentication rejected".into());
                }
                _ if connected => {
                    self.ingest_with_login(event, true)?;
                }
                _ => return Err("pocket options: unexpected handshake order".into()),
            }
        }
        Ok(())
    }
    #[cfg(test)]
    fn ingest(&mut self, event: Event) -> Result<Option<Deal>, String> {
        self.ingest_with_login(event, false)
    }
    fn ingest_with_login(&mut self, event: Event, login: bool) -> Result<Option<Deal>, String> {
        match event.name.as_str() {
            "updateAssets" => {
                let rows: Vec<[Box<RawValue>; 19]> = serde_json::from_slice(&event.raw)
                    .map_err(|_| "pocket options: invalid updateAssets")?;
                let hash = payload_hash(&event.raw);
                for row in rows {
                    let symbol: String = serde_json::from_str(row[1].get())
                        .map_err(|_| "pocket options: invalid listing symbol")?;
                    if !self.instruments.contains_key(&symbol) {
                        continue;
                    }
                    let percent: u8 = serde_json::from_str(row[5].get())
                        .map_err(|_| "pocket options: invalid listing percent")?;
                    if percent > 100 {
                        return Err("pocket options: listing percent exceeds 100".into());
                    }
                    self.listings.insert(
                        symbol.clone(),
                        Listing {
                            listed_percent: percent,
                            receipt_micros: event.receipt_micros,
                            frame_sha256: hash.clone(),
                        },
                    );
                    self.events.push_back(AccountEvent::Listing {
                        instrument: InstrumentId {
                            broker: self.account.broker.clone(),
                            provider_symbol: symbol.try_into()?,
                        },
                        listed_percent: percent,
                        receipt_micros: event.receipt_micros,
                        frame_sha256: hash.clone(),
                    });
                }
            }
            "successupdateBalance" => {
                #[derive(Deserialize)]
                struct Balance {
                    #[serde(rename = "isDemo")]
                    is_demo: u8,
                    balance: WireDecimal,
                }
                let balance: Balance = serde_json::from_slice(&event.raw)
                    .map_err(|_| "pocket options: invalid balance")?;
                if balance.is_demo != 1 {
                    return Err("pocket options: server account class mismatch".into());
                }
                self.balance = Some(balance.balance.require_number()?);
            }
            "updateOpenedDeals" => {
                let deals: Vec<Deal> = serde_json::from_slice(&event.raw)
                    .map_err(|_| "pocket options: invalid opened deal list")?;
                for deal in &deals {
                    deal.validate(&self.account)?;
                    if let Some(id) = deal.request_id {
                        self.request_ids.insert(id);
                    }
                    if self.seen_open.insert((
                        deal.id.clone(),
                        serde_json::to_string(deal).map_err(|e| e.to_string())?,
                    )) {
                        if !login {
                            self.balance = None;
                        }
                        self.events.push_back(AccountEvent::PocketDeal {
                            deal: deal.clone(),
                            closed: false,
                            receipt_micros: event.receipt_micros,
                        });
                    }
                }
                self.opened = deals;
            }
            "updateClosedDeals" => {
                let deals: Vec<Deal> = serde_json::from_slice(&event.raw)
                    .map_err(|_| "pocket options: invalid closed deal list")?;
                for deal in &deals {
                    deal.validate(&self.account)?;
                    self.close_deal(deal.clone(), event.receipt_micros, login)?;
                }
            }
            "successcloseOrder" => {
                #[derive(Deserialize)]
                struct Close {
                    profit: WireDecimal,
                    deals: Vec<Deal>,
                }
                let close: Close = serde_json::from_slice(&event.raw)
                    .map_err(|_| "pocket options: invalid close")?;
                let mut total = Decimal::zero(0);
                for deal in &close.deals {
                    deal.validate(&self.account)?;
                    total = total.checked_add(deal.gross_credit()?)?;
                }
                if total.compare(close.profit.require_number()?)? != std::cmp::Ordering::Equal {
                    return Err("pocket options: close credit mismatch".into());
                }
                for deal in close.deals {
                    self.close_deal(deal, event.receipt_micros, login)?;
                }
            }
            "successopenOrder" => {
                let deal: Deal = serde_json::from_slice(&event.raw)
                    .map_err(|_| "pocket options: invalid open reply")?;
                deal.validate(&self.account)?;
                if let Some(id) = deal.request_id {
                    self.request_ids.insert(id);
                }
                self.opened.retain(|opened| opened.id != deal.id);
                self.opened.push(deal.clone());
                if !login {
                    self.balance = None;
                }
                return Ok(Some(deal));
            }
            _ => (),
        }
        Ok(None)
    }
    fn close_deal(&mut self, deal: Deal, receipt: i64, login: bool) -> Result<(), String> {
        if let Some(id) = deal.request_id {
            self.request_ids.insert(id);
        }
        self.opened.retain(|opened| opened.id != deal.id);
        if self
            .closed
            .insert(
                (
                    deal.id.clone(),
                    serde_json::to_string(&deal).map_err(|e| e.to_string())?,
                ),
                deal.clone(),
            )
            .is_none()
        {
            if !login {
                self.balance = None;
            }
            self.events.push_back(AccountEvent::PocketDeal {
                deal,
                closed: true,
                receipt_micros: receipt,
            });
        }
        Ok(())
    }
    pub fn subscribe_transactions(&mut self) -> Result<(), String> {
        self.events.push_back(AccountEvent::TransactionAcknowledged);
        Ok(())
    }
    pub fn balance(&mut self) -> Result<Decimal, String> {
        let deadline = self.clock.now_micros().saturating_add(12_000_000);
        while self.balance.is_none() {
            let event = self
                .receive(deadline.saturating_sub(self.clock.now_micros()).max(0))?
                .ok_or("pocket options: balance snapshot unavailable")?;
            if let Some(deal) = self.ingest_with_login(event, false)? {
                self.events.push_back(AccountEvent::PocketDeal {
                    deal,
                    closed: false,
                    receipt_micros: self.clock.now_micros(),
                });
            }
        }
        Ok(self.balance.unwrap())
    }
    pub fn prepare_purchase(&mut self, prepared: &PreparedPurchase) -> Result<Encoded, String> {
        let offer = prepared
            .offer
            .as_ref()
            .ok_or("pocket prepare: admitted offer missing")?;
        if self.written.contains(&prepared.dispatch_claim)
            || self.request_ids.len() >= 10_000_000
            || offer.account != self.account.account
            || offer.terms.currency != self.account.currency
            || offer.identity != prepared.proposal_identity
            || offer.terms.quoted_cost.compare(prepared.maximum_price)? != std::cmp::Ordering::Equal
        {
            return Err("pocket prepare: order scope or claim mismatch".into());
        }
        let (_, symbol) = offer
            .instrument
            .split_once(':')
            .ok_or("pocket prepare: invalid instrument")?;
        if !self.instruments.contains_key(symbol)
            || offer.terms.duration_micros <= 0
            || offer.terms.duration_micros % 1_000_000 != 0
            || offer.terms.stake.rescale(0).is_err()
        {
            return Err("pocket prepare: unsupported order fields".into());
        }
        let listing = self
            .listings
            .get(symbol)
            .ok_or("pocket prepare: listing unavailable")?
            .clone();
        let rule = self
            .settings
            .payout
            .ok_or("pocket prepare: payout missing")?;
        let gross = gross_return(offer.terms.stake, listing.listed_percent, rule)?;
        if !fresh(&listing, self.clock.now_micros(), rule)
            || listing.frame_sha256 != offer.payload_sha256
            || gross.compare(offer.terms.win.gross_return)? != std::cmp::Ordering::Equal
        {
            return Err("pocket prepare: listing changed or stale".into());
        }
        let id = self.allocate_request_id()?;
        #[derive(Serialize)]
        #[serde(rename_all = "camelCase")]
        struct Order<'a> {
            asset: &'a str,
            amount: WireDecimal,
            action: &'static str,
            is_demo: u8,
            request_id: u64,
            option_type: u16,
            time: i64,
        }
        let order = Order {
            asset: symbol,
            amount: WireDecimal::from_decimal(offer.terms.stake),
            action: match offer.terms.direction {
                Direction::Buy => "call",
                Direction::Sell => "put",
            },
            is_demo: 1,
            request_id: id,
            option_type: 100,
            time: offer.terms.duration_micros / 1_000_000,
        };
        let text = socket_io::encode_event(
            "openOrder",
            &serde_json::to_string(&order).map_err(|error| error.to_string())?,
        );
        Ok(Encoded {
            prepared: prepared.clone(),
            request_id: id,
            text,
            listing,
        })
    }
    pub fn write_purchase(&mut self, encoded: Encoded) -> Result<PurchaseOutcome, String> {
        let offer = encoded
            .prepared
            .offer
            .as_ref()
            .ok_or("pocket write: offer missing")?;
        let (_, symbol) = offer
            .instrument
            .split_once(':')
            .ok_or("pocket write: instrument missing")?;
        let rule = self.settings.payout.ok_or("pocket write: payout missing")?;
        let current = self.listings.get(symbol);
        if current.is_none_or(|listing| {
            listing.frame_sha256 != encoded.listing.frame_sha256
                || listing.receipt_micros != encoded.listing.receipt_micros
                || !fresh(listing, self.clock.now_micros(), rule)
        }) {
            return Ok(PurchaseOutcome::ProvenNotSent {
                reason: "pocket write: listing changed or stale".into(),
            });
        }
        if !self.written.insert(encoded.prepared.dispatch_claim) {
            return Ok(PurchaseOutcome::ProvenNotSent {
                reason: "pocket write: claim already written".into(),
            });
        }
        if let Err(reason) = self.transport.send(Frame::Text(encoded.text)) {
            return Ok(PurchaseOutcome::PossiblySent { reason });
        }
        let deadline = self.clock.now_micros().saturating_add(12_000_000);
        loop {
            let event = match self.receive(deadline.saturating_sub(self.clock.now_micros()).max(0))
            {
                Ok(Some(event)) => event,
                Ok(None) => {
                    return Ok(PurchaseOutcome::PossiblySent {
                        reason: "pocket write: reply timeout".into(),
                    });
                }
                Err(reason) => return Ok(PurchaseOutcome::PossiblySent { reason }),
            };
            let receipt = event.receipt_micros;
            match self.ingest_with_login(event, false) {
                Ok(Some(deal)) if deal.request_id == Some(encoded.request_id) => {
                    let (_, symbol) = offer.instrument.split_once(':').unwrap();
                    let matching = deal.asset == symbol
                        && deal.direction().ok() == Some(offer.terms.direction)
                        && deal.amount.require_number().ok().is_some_and(|amount| {
                            amount.compare(offer.terms.stake).ok()
                                == Some(std::cmp::Ordering::Equal)
                        })
                        && deal
                            .expiry_time(self.settings.server_offset_minutes)
                            .ok()
                            .zip(deal.entry_time(self.settings.server_offset_minutes).ok())
                            .is_some_and(|(end, start)| {
                                end - start >= offer.terms.duration_micros - 1_000_000
                                    && end - start <= offer.terms.duration_micros
                            });
                    if !matching {
                        return Ok(PurchaseOutcome::PossiblySent {
                            reason: "pocket write: contradictory open reply".into(),
                        });
                    }
                    if let Some(scale) = self.instruments.get(symbol).copied() {
                        self.events.push_back(deal.update(
                            scale,
                            self.settings.server_offset_minutes,
                            receipt,
                        )?);
                    }
                    return Ok(PurchaseOutcome::Accepted {
                        debit: deal.amount.require_number()?,
                        liability: deal.liability(self.settings.server_offset_minutes)?,
                        receipt_micros: receipt,
                    });
                }
                Ok(Some(deal)) => self.events.push_back(AccountEvent::PocketDeal {
                    deal,
                    closed: false,
                    receipt_micros: receipt,
                }),
                Ok(None) => (),
                Err(reason) => return Ok(PurchaseOutcome::PossiblySent { reason }),
            }
        }
    }
    pub fn subscribe_contract(&mut self, _: &str) -> Result<(), String> {
        Ok(())
    }
    pub fn queued_account_event(&mut self) -> Result<Option<AccountEvent>, String> {
        Ok(self.events.pop_front())
    }
    pub fn next_account_event(&mut self, timeout: i64) -> Result<Option<AccountEvent>, String> {
        if let Some(event) = self.events.pop_front() {
            return Ok(Some(event));
        }
        let received = match self
            .session
            .poll(&mut *self.transport, &*self.clock, timeout)
        {
            Ok(event) => event,
            Err(socket_io::ReceiveError::Interrupted) => return Ok(None),
            Err(error) => return Err(error.into()),
        };
        let Some(event) = received else {
            return Ok(None);
        };
        if let Some(deal) = self.ingest_with_login(event, false)? {
            self.events.push_back(AccountEvent::PocketDeal {
                deal,
                closed: false,
                receipt_micros: self.clock.now_micros(),
            });
        }
        Ok(self.events.pop_front())
    }
    pub fn open_contracts(&mut self) -> Result<Vec<OpenContract>, String> {
        self.opened
            .iter()
            .map(|deal| {
                Ok(OpenContract {
                    contract_ref: deal.id.clone(),
                    transaction_ref: deal.id.clone(),
                    buy_price: deal.amount.require_number()?,
                    payout: deal.payout()?,
                    purchase_time_micros: deal.entry_time(self.settings.server_offset_minutes)?,
                    start_micros: Some(deal.entry_time(self.settings.server_offset_minutes)?),
                    expiry_micros: Some(deal.expiry_time(self.settings.server_offset_minutes)?),
                    instrument: deal.asset.clone(),
                    direction: deal.direction()?,
                })
            })
            .collect()
    }
    pub fn statement(&mut self, _: i64, _: i64) -> Result<Statement, String> {
        let rows = self
            .closed
            .values()
            .map(|deal| {
                Ok(super::deriv::StatementRow {
                    request_id: deal.request_id,
                    pocket: Some(deal.clone()),
                    instrument: Some(deal.asset.clone()),
                    direction: Some(deal.direction()?),
                    cash: match deal.cash(
                        &self.account,
                        self.settings.server_offset_minutes,
                        self.clock.now_micros(),
                    )? {
                        AccountEvent::Cash { fact, .. } => fact,
                        _ => unreachable!(),
                    },
                    payout: Some(deal.payout()?),
                    receipt_micros: self.clock.now_micros(),
                })
            })
            .collect::<Result<_, String>>()?;
        Ok(Statement {
            coverage: StatementCoverage::PartialSnapshot,
            rows,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{Value, json};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicI64, Ordering};

    struct TestClock(Arc<AtomicI64>);
    impl Clock for TestClock {
        fn now_micros(&self) -> i64 {
            self.0.load(Ordering::SeqCst)
        }
        fn sleep(&mut self, micros: i64) {
            self.0.fetch_add(micros, Ordering::SeqCst);
        }
    }
    struct NoSocket(Arc<AtomicI64>);
    impl Transport for NoSocket {
        fn send(&mut self, _: Frame) -> Result<(), String> {
            Ok(())
        }
        fn receive(&mut self, timeout: i64) -> Result<Option<Frame>, String> {
            self.0.fetch_add(timeout.max(0), Ordering::SeqCst);
            Ok(None)
        }
        fn close(&mut self) -> Result<(), String> {
            Ok(())
        }
    }
    fn session() -> PocketOptions {
        let clock = Arc::new(AtomicI64::new(100_000_000));
        let settings = serde_json::from_value(json!({
            "id":"p", "endpoint":"wss://example.invalid", "credential":"SYNTHETIC",
            "account_class":"demo", "server_offset_minutes":0,
            "payout":{"add_percent":8,"cap_percent":92,"max_age_seconds":120}
        }))
        .unwrap();
        PocketOptions {
            settings,
            account: AccountIdentity {
                broker: "p".to_string().try_into().unwrap(),
                account: "synthetic".into(),
                class: AccountClass::Demo,
                currency: "USD".to_string().try_into().unwrap(),
            },
            instruments: BTreeMap::from([("TEST".into(), PriceScale::try_from(5).unwrap())]),
            transport: Box::new(NoSocket(Arc::clone(&clock))),
            clock: Box::new(TestClock(clock)),
            session: socket_io::Session::new(0),
            events: VecDeque::new(),
            listings: BTreeMap::new(),
            opened: Vec::new(),
            closed: BTreeMap::new(),
            balance: None,
            written: BTreeSet::new(),
            request_ids: BTreeSet::new(),
            next_request_id: 10_000_000,
            seen_open: BTreeSet::new(),
        }
    }
    fn deal(id: &str, asset: &str, close_price: &str, profit: &str) -> Value {
        json!({"id":id,"asset":asset,"command":0,"amount":10,"profit":serde_json::from_str::<Value>(profit).unwrap(),
            "percentProfit":92,"openPrice":1.00000,"closePrice":serde_json::from_str::<Value>(close_price).unwrap(),
            "openTimestamp":100,"openMs":100,"closeTimestamp":130,"closeMs":200,
            "isDemo":1,"currency":"USD","requestId":10000000,"optionType":100})
    }
    fn event(name: &str, value: Value) -> Event {
        Event {
            name: name.into(),
            raw: serde_json::to_vec(&value).unwrap(),
            receipt_micros: 100_000_000,
        }
    }

    #[test]
    fn payout_caps_high_listings_and_refuses_stale_or_future_rows() {
        let rule = session().settings.payout.unwrap();
        assert_eq!(mapped_percent(84, rule).unwrap(), 92);
        assert_eq!(mapped_percent(49, rule).unwrap(), 57);
        assert_eq!(
            gross_return(Decimal::parse("1").unwrap(), 84, rule)
                .unwrap()
                .to_string(),
            "1.92"
        );
        assert_eq!(
            gross_return(Decimal::parse("1").unwrap(), 49, rule)
                .unwrap()
                .to_string(),
            "1.57"
        );
        assert_eq!(mapped_percent(100, rule).unwrap(), 92);
        let listing = Listing {
            listed_percent: 84,
            receipt_micros: 10,
            frame_sha256: "synthetic".into(),
        };
        assert!(!fresh(&listing, 9, rule));
        assert!(fresh(&listing, 120_000_010, rule));
        assert!(!fresh(&listing, 120_000_011, rule));
    }

    #[test]
    fn direct_closes_update_partial_statement_and_check_outer_credit() {
        let mut account = session();
        let close = deal("synthetic-deal", "TEST", "1.00100", "9.2");
        account
            .ingest(event("successopenOrder", close.clone()))
            .unwrap();
        assert_eq!(account.open_contracts().unwrap().len(), 1);
        assert!(
            account
                .ingest(event(
                    "successcloseOrder",
                    json!({"profit":18.2,"deals":[close.clone()]})
                ))
                .is_err()
        );
        assert_eq!(account.open_contracts().unwrap().len(), 1);
        account
            .ingest(event(
                "successcloseOrder",
                json!({"profit":19.2,"deals":[close]}),
            ))
            .unwrap();
        assert!(account.open_contracts().unwrap().is_empty());
        let statement = account.statement(0, 200).unwrap();
        assert_eq!(statement.coverage, StatementCoverage::PartialSnapshot);
        assert_eq!(statement.rows.len(), 1);
        assert_eq!(statement.rows[0].cash.amount.to_string(), "19.2");
        assert!(account.request_ids.contains(&10_000_000));
    }

    #[test]
    fn closed_list_removes_the_open_contract() {
        let mut account = session();
        let closed = deal("synthetic-deal", "TEST", "1.00100", "9.2");
        account
            .ingest(event("successopenOrder", closed.clone()))
            .unwrap();
        assert_eq!(account.open_contracts().unwrap().len(), 1);
        account
            .ingest(event("updateClosedDeals", json!([closed])))
            .unwrap();
        assert!(account.open_contracts().unwrap().is_empty());
    }

    #[test]
    fn partial_closed_lists_retain_distinct_ids_and_contradictory_facts() {
        let mut account = session();
        let first = deal("synthetic-first", "TEST", "1.00100", "9.2");
        let mut second = first.clone();
        second["id"] = json!("synthetic-second");
        let mut changed = first.clone();
        changed["command"] = json!(1);
        for row in [&first, &second, &changed] {
            account
                .ingest(event("updateClosedDeals", json!([row])))
                .unwrap();
        }
        let statement = account.statement(0, 200).unwrap();
        assert_eq!(statement.rows.len(), 3);
        assert_eq!(
            statement
                .rows
                .iter()
                .filter(|row| row.cash.contract_ref.as_deref() == Some("synthetic-first"))
                .count(),
            2
        );
        assert_eq!(
            statement
                .rows
                .iter()
                .filter(|row| row.request_id == Some(10_000_000))
                .count(),
            3
        );
    }

    #[test]
    fn a_new_deal_list_requires_a_later_balance() {
        let mut account = session();
        account
            .ingest(event(
                "successupdateBalance",
                json!({"isDemo":1,"balance":100}),
            ))
            .unwrap();
        let fact = event(
            "updateOpenedDeals",
            json!([deal("synthetic-open", "TEST", "1.00100", "0")]),
        );
        account.ingest(fact).unwrap();
        assert!(account.balance().is_err());
        let mut later = event("successupdateBalance", json!({"isDemo":1,"balance":90}));
        later.receipt_micros += 1;
        account.ingest(later).unwrap();
        assert_eq!(account.balance().unwrap().to_string(), "90");
    }

    #[test]
    fn zero_credit_tie_is_lost_and_foreign_history_remains_visible() {
        let mut account = session();
        let loss = deal("synthetic-loss", "OTHER", "1.00000", "-10");
        account
            .ingest(event("updateClosedDeals", json!([loss.clone()])))
            .unwrap();
        let statement = account.statement(0, 200).unwrap();
        assert_eq!(statement.rows[0].cash.amount.to_string(), "0");
        let deal: Deal = serde_json::from_value(loss).unwrap();
        match deal
            .terminal(PriceScale::try_from(5).unwrap(), 0, 100_000_000)
            .unwrap()
        {
            AccountEvent::Terminal { fact, .. } => assert_eq!(fact.status, TerminalStatus::Lost),
            _ => panic!("terminal event expected"),
        }
    }

    #[test]
    fn open_and_statement_deals_require_the_account_currency() {
        let mut account = session();
        let mut wrong = deal("synthetic-wrong-currency", "TEST", "1.00100", "9.2");
        wrong["currency"] = json!("EUR");
        assert!(
            account
                .ingest(event("successopenOrder", wrong.clone()))
                .is_err()
        );
        assert!(
            account
                .ingest(event("updateClosedDeals", json!([wrong])))
                .is_err()
        );
        let mut missing = deal("synthetic-missing-currency", "TEST", "1.00100", "9.2");
        missing.as_object_mut().unwrap().remove("currency");
        assert!(
            account
                .ingest(event("successopenOrder", missing.clone()))
                .is_err()
        );
        assert!(
            account
                .ingest(event("updateClosedDeals", json!([missing])))
                .is_err()
        );
        assert!(account.open_contracts().unwrap().is_empty());
        assert!(account.statement(0, 200).unwrap().rows.is_empty());
    }

    #[test]
    fn reserved_request_ids_are_skipped_after_restart() {
        let mut account = session();
        account.reserve_request_ids([10_000_000, 10_000_001]);
        assert_eq!(account.allocate_request_id().unwrap(), 10_000_002);
    }

    #[test]
    fn write_rechecks_the_listing_and_silence_remains_possibly_sent() {
        use binary_alpha_engine::execution::ContractTerms;
        let mut account = session();
        let listing = Listing {
            listed_percent: 84,
            receipt_micros: 100_000_000,
            frame_sha256: "synthetic-listing".into(),
        };
        account.listings.insert("TEST".into(), listing.clone());
        let terms: ContractTerms = serde_json::from_value(json!({
            "id":"synthetic","direction":"buy","duration_micros":30000000,"currency":"USD",
            "stake":"1","quoted_cost":"1","entry_fee":"0",
            "win":{"gross_return":"1.92","terminal_fee":"0"},
            "loss":{"gross_return":"0","terminal_fee":"0"},
            "tie":{"gross_return":"0","terminal_fee":"0"},
            "settlement":{"rule":"price_at_due_v1","max_settlement_delay_micros":1000000,"max_tick_gap_micros":2000000},
            "semantics":"rise_fall_strict_v1"
        })).unwrap();
        let request = super::super::ProposalRequest {
            binding: "synthetic-binding".into(),
            instrument: InstrumentId {
                broker: account.account.broker.clone(),
                provider_symbol: "TEST".to_string().try_into().unwrap(),
            },
            scale: PriceScale::try_from(5).unwrap(),
            direction: terms.direction,
            duration_seconds: 30,
            stake: terms.stake,
            currency: terms.currency.clone(),
            semantics: terms.semantics.unwrap(),
            settlement: terms.settlement,
        };
        let quote = LiveObservation {
            instrument: request.instrument.clone(),
            provider_time_micros: 99_800_000,
            price_units: 100_000,
            receipt_micros: 100_000_000,
            generation: 0,
            sequence: 1,
            payload_sha256: "synthetic-quote".into(),
            source: "synthetic".into(),
        };
        let first_offer = offer(
            &request,
            &terms,
            &account.account,
            &quote,
            &listing,
            account.settings.payout.unwrap(),
        )
        .unwrap();
        assert_eq!(first_offer.terms.win.gross_return.to_string(), "1.92");
        let mut next_quote = quote;
        next_quote.price_units += 150;
        next_quote.provider_time_micros += 300_000;
        next_quote.receipt_micros += 300_000;
        let next_offer = offer(
            &request,
            &terms,
            &account.account,
            &next_quote,
            &listing,
            account.settings.payout.unwrap(),
        )
        .unwrap();
        assert_eq!(next_offer.spot_units, first_offer.spot_units + 150);
        assert_ne!(next_offer.identity, first_offer.identity);
        let prepared = PreparedPurchase {
            dispatch_claim: "synthetic-claim".into(),
            command: "synthetic-command".into(),
            proposal_identity: first_offer.identity.clone(),
            maximum_price: terms.quoted_cost,
            offer: Some(first_offer),
        };
        let encoded = account.prepare_purchase(&prepared).unwrap();
        assert_eq!(encoded.request_id, 10_000_000);
        account.listings.get_mut("TEST").unwrap().frame_sha256 = "changed".into();
        let not_sent = account.write_purchase(encoded.clone()).unwrap();
        assert!(matches!(not_sent, PurchaseOutcome::ProvenNotSent { .. }));
        assert!(matches!(
            super::super::options::purchase_observation(
                "synthetic-command", "synthetic-claim", not_sent, 100_000_000,
                binary_alpha_engine::config::BrokerKind::PocketOption,
            ),
            binary_alpha_engine::execution::Observation::NotSent { source, .. }
                if source.id.starts_with("pocket:")
        ));
        account.listings.insert("TEST".into(), listing);
        assert!(matches!(
            account.write_purchase(encoded).unwrap(),
            PurchaseOutcome::PossiblySent { .. }
        ));
        account.listings.get_mut("TEST").unwrap().listed_percent = 49;
        assert!(account.prepare_purchase(&prepared).is_err());
    }
}

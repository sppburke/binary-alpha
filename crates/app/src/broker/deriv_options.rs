use super::{AuthenticatedAddress, DerivAuthenticated, Response, SCHEMA, decode, micros};
use crate::broker::transport::{Connector, Frame};
use crate::broker::wire::WireDecimal;
use crate::broker::{
    AccountEvent, AccountIdentity, Clock, OpenContract, PreparedPurchase, ProposalRequest,
    PurchaseOutcome, RateGroup, payload_hash,
};
use binary_alpha_engine::config::{AccountClass, RateBudgets};
use binary_alpha_engine::execution::{
    self, BrokerLiability, CashAction, CashFact, Cashflow, ContractSemantics, ContractTerms,
    Decimal, Direction, EventSource, Proposal, SettlementRule, TerminalFact, TerminalStatus,
};
use binary_alpha_engine::market::{Currency, InstrumentId, PriceScale};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet, VecDeque};

// deriv-demo-run-01 is the measured scope of this economic mapping.
const MEASURED_ACCOUNT_CLASS: AccountClass = AccountClass::Demo;
const MEASURED_CURRENCY: &str = "USD";
pub(super) const MEASURED_CONTRACT_CATEGORY: &str = "callput";
const MEASURED_CONTRACT_TYPES: [&str; 2] = ["CALL", "PUT"];

#[derive(Debug, Clone)]
pub struct StatementRow {
    pub cash: CashFact,
    pub payout: Option<Decimal>,
}

/// A statement buy row alone can recover the debit and purchased liability.
pub fn recover_purchase(row: &StatementRow) -> Option<(Decimal, BrokerLiability)> {
    if row.cash.action != CashAction::Buy || !row.cash.amount.is_negative() {
        return None;
    }
    Some((
        Decimal::zero(0).checked_sub(row.cash.amount).ok()?,
        BrokerLiability {
            contract_ref: row.cash.contract_ref.clone()?,
            transaction_ref: row.cash.transaction_ref.clone(),
            purchase_time_micros: row.cash.time_micros,
            expected_start_micros: None,
            payout: row.payout?,
        },
    ))
}

#[derive(Serialize)]
struct SubscribeRequest {
    transaction: u8,
    subscribe: u8,
    req_id: u64,
}
#[derive(Serialize)]
struct QuoteRequest<'a> {
    proposal: u8,
    amount: WireDecimal,
    basis: &'static str,
    contract_type: &'static str,
    currency: &'a Currency,
    duration: u32,
    duration_unit: &'static str,
    underlying_symbol: &'a str,
    req_id: u64,
}
#[derive(Serialize)]
struct BuyRequest<'a> {
    buy: &'a str,
    price: WireDecimal,
    req_id: u64,
}
#[derive(Serialize)]
struct ContractRequest {
    proposal_open_contract: u8,
    contract_id: WireDecimal,
    subscribe: u8,
    req_id: u64,
}
#[derive(Serialize)]
struct PortfolioRequest {
    portfolio: u8,
    req_id: u64,
}
#[derive(Serialize)]
struct StatementRequest {
    statement: u8,
    date_from: i64,
    date_to: i64,
    description: u8,
    limit: u32,
    offset: u32,
    req_id: u64,
}
#[derive(Deserialize)]
struct QuoteResponse {
    proposal: Quote,
}
#[derive(Deserialize)]
struct Quote {
    commission: Option<WireDecimal>,
    id: String,
    ask_price: WireDecimal,
    payout: WireDecimal,
    spot: WireDecimal,
    spot_time: i64,
    longcode: String,
}
#[derive(Deserialize)]
struct BuyResponse {
    buy: Purchase,
}
#[derive(Deserialize)]
struct Purchase {
    buy_price: WireDecimal,
    purchase_time: i64,
    contract_id: WireDecimal,
    transaction_id: WireDecimal,
    payout: WireDecimal,
    start_time: Option<i64>,
}
#[derive(Deserialize)]
struct TransactionResponse {
    transaction: Transaction,
}
#[derive(Deserialize)]
struct Transaction {
    id: Option<String>,
    action: Option<CashAction>,
    amount: Option<WireDecimal>,
    currency: Option<Currency>,
    contract_id: Option<WireDecimal>,
    transaction_id: Option<WireDecimal>,
    transaction_time: Option<i64>,
}
#[derive(Deserialize)]
struct ContractResponse {
    proposal_open_contract: Contract,
}
#[derive(Deserialize)]
struct Contract {
    contract_type: Option<String>,
    contract_id: WireDecimal,
    status: Option<String>,
    currency: Option<Currency>,
    underlying_symbol: Option<String>,
    entry_spot: Option<WireDecimal>,
    entry_spot_time: Option<i64>,
    date_start: Option<i64>,
    date_expiry: Option<i64>,
    exit_spot: Option<WireDecimal>,
    exit_spot_time: Option<i64>,
    transaction_ids: Option<TransactionIds>,
    current_spot_time: Option<i64>,
    purchase_time: Option<i64>,
    sell_time: Option<i64>,
}
#[derive(Deserialize)]
struct TransactionIds {
    sell: Option<WireDecimal>,
}
#[derive(Deserialize)]
struct PortfolioResponse {
    portfolio: Portfolio,
}
#[derive(Deserialize)]
struct Portfolio {
    contracts: Vec<PortfolioContract>,
}
#[derive(Deserialize)]
struct PortfolioContract {
    contract_id: WireDecimal,
    transaction_id: WireDecimal,
    buy_price: WireDecimal,
    payout: WireDecimal,
    purchase_time: i64,
    date_start: Option<i64>,
    expiry_time: Option<i64>,
    currency: Currency,
    underlying_symbol: String,
    contract_type: String,
}
#[derive(Deserialize)]
struct StatementResponse {
    statement: Statement,
}
#[derive(Deserialize)]
struct Statement {
    count: u32,
    transactions: Vec<StatementTransaction>,
}
#[derive(Deserialize)]
struct StatementTransaction {
    payout: Option<WireDecimal>,
    action_type: CashAction,
    amount: WireDecimal,
    contract_id: Option<WireDecimal>,
    transaction_id: WireDecimal,
    transaction_time: i64,
}
struct IssuedProposal {
    spot: String,
    provider_id: String,
    longcode: String,
}

/// One authenticated connection, its account scope, and its written dispatch claims.
pub struct DerivOptions {
    authenticated: DerivAuthenticated,
    account: AccountIdentity,
    instruments: BTreeMap<String, PriceScale>,
    proposals: BTreeMap<String, IssuedProposal>,
    written: BTreeSet<String>,
    possibly_sent: BTreeSet<String>,
    transactions: bool,
    contracts: BTreeSet<String>,
    events: VecDeque<AccountEvent>,
}
impl DerivOptions {
    pub fn connect(
        address: AuthenticatedAddress,
        account: AccountIdentity,
        instruments: &[(InstrumentId, PriceScale)],
        connector: Box<dyn Connector>,
        clock: Box<dyn Clock>,
        limits: RateBudgets,
    ) -> Result<Self, String> {
        if account.account.is_empty()
            || account.currency != address.currency
            || account.class != address.account_class
        {
            return Err("deriv options: configured account class or currency mismatch".into());
        }
        let mut selected = BTreeMap::new();
        for (instrument, scale) in instruments {
            if instrument.broker != account.broker
                || selected
                    .insert(instrument.provider_symbol.to_string(), *scale)
                    .is_some()
            {
                return Err("deriv options: invalid or duplicate instrument scope".into());
            }
        }
        Ok(Self {
            authenticated: DerivAuthenticated::connect(address, connector, clock, limits)?,
            account,
            instruments: selected,
            proposals: BTreeMap::new(),
            written: BTreeSet::new(),
            possibly_sent: BTreeSet::new(),
            transactions: false,
            contracts: BTreeSet::new(),
            events: VecDeque::new(),
        })
    }
    pub fn proposal_details(&self, identity: &str) -> Option<(&str, &str)> {
        self.proposals
            .get(identity)
            .map(|proposal| (proposal.spot.as_str(), proposal.longcode.as_str()))
    }
    fn currency(&self, currency: &Currency) -> Result<(), String> {
        if *currency != self.account.currency {
            return Err("deriv options: currency mismatch".into());
        }
        Ok(())
    }
    fn decode_event(&mut self, response: Response) -> Result<(), String> {
        if let Some(error) = response.header.error {
            return Err(format!("deriv account: {}", error.code));
        }
        match response.header.msg_type.as_str() {
            "transaction" => {
                let body: TransactionResponse = decode(&response.raw, "transaction")?;
                let t = body.transaction;
                if t.action.is_none()
                    && t.amount.is_none()
                    && t.transaction_id.is_none()
                    && t.transaction_time.is_none()
                    && t.id.is_some()
                {
                    self.events.push_back(AccountEvent::TransactionAcknowledged);
                } else {
                    self.currency(&t.currency.ok_or("deriv transaction: currency missing")?)?;
                    self.events.push_back(AccountEvent::Cash(CashFact {
                        account: self.account.account.clone(),
                        transaction_ref: identifier(
                            &t.transaction_id
                                .ok_or("deriv transaction: transaction identity missing")?,
                        )?,
                        contract_ref: t.contract_id.as_ref().map(identifier).transpose()?,
                        action: t.action.ok_or("deriv transaction: action missing")?,
                        amount: number(&t.amount.ok_or("deriv transaction: amount missing")?)?,
                        time_micros: micros(
                            t.transaction_time
                                .ok_or("deriv transaction: time missing")?,
                        )?,
                    }));
                }
            }
            "proposal_open_contract" => {
                let body: ContractResponse = decode(&response.raw, "proposal_open_contract")?;
                let c = body.proposal_open_contract;
                if let Some(currency) = &c.currency {
                    self.currency(currency)?;
                }
                if let Some(kind) = &c.contract_type {
                    direction(kind)?;
                }
                let scale = c
                    .underlying_symbol
                    .as_ref()
                    .map(|symbol| {
                        self.instruments
                            .get(symbol)
                            .copied()
                            .ok_or("deriv contract: instrument outside configured scope")
                    })
                    .transpose()?;
                let contract_ref = identifier(&c.contract_id)?;
                if !self.contracts.contains(&contract_ref) {
                    return Err("deriv contract: unexpected contract identity".into());
                }
                let status = match c.status.as_deref() {
                    None | Some("open") => None,
                    Some("won") => Some(TerminalStatus::Won),
                    Some("lost") => Some(TerminalStatus::Lost),
                    Some("sold") => Some(TerminalStatus::Sold),
                    Some("cancelled") => Some(TerminalStatus::Cancelled),
                    _ => return Err("deriv contract: unsupported status".into()),
                };
                let has_update = c.entry_spot.is_some()
                    || c.entry_spot_time.is_some()
                    || c.date_start.is_some()
                    || c.date_expiry.is_some();
                if !has_update && status.is_none() {
                    return Ok(());
                }
                let update_time = c
                    .current_spot_time
                    .or(c.purchase_time)
                    .or(c.entry_spot_time)
                    .or(c.date_start)
                    .or(c.date_expiry)
                    .map(micros)
                    .transpose()?
                    .unwrap_or(response.receipt_micros);
                let update = AccountEvent::ContractUpdate {
                    contract_ref: contract_ref.clone(),
                    source: source(
                        format!(
                            "deriv:contract:{contract_ref}:update:{}",
                            payload_hash(&response.raw)
                        ),
                        update_time,
                        response.receipt_micros,
                    ),
                    entry_price_units: c
                        .entry_spot
                        .as_ref()
                        .map(|price| {
                            price.price_units(
                                scale
                                    .ok_or("deriv contract: underlying_symbol missing for price")?,
                            )
                        })
                        .transpose()?,
                    entry_time_micros: c.entry_spot_time.map(micros).transpose()?,
                    start_micros: c.date_start.map(micros).transpose()?,
                    expiry_micros: c.date_expiry.map(micros).transpose()?,
                };
                let terminal = status
                    .map(|status| {
                        Ok::<_, String>(AccountEvent::Terminal {
                            source: source(
                                format!(
                                    "deriv:contract:{contract_ref}:{status}:{}",
                                    payload_hash(&response.raw)
                                ),
                                micros(
                                    c.sell_time
                                        .or(c.exit_spot_time)
                                        .or(c.current_spot_time)
                                        .ok_or("deriv contract: terminal source time missing")?,
                                )?,
                                response.receipt_micros,
                            ),
                            contract_ref,
                            fact: TerminalFact {
                                status,
                                exit_price_units: c
                                    .exit_spot
                                    .as_ref()
                                    .map(|price| {
                                        price.price_units(scale.ok_or(
                                            "deriv contract: underlying_symbol missing for price",
                                        )?)
                                    })
                                    .transpose()?,
                                exit_time_micros: c.exit_spot_time.map(micros).transpose()?,
                                transaction_ref: c
                                    .transaction_ids
                                    .and_then(|ids| ids.sell)
                                    .as_ref()
                                    .map(identifier)
                                    .transpose()?,
                            },
                        })
                    })
                    .transpose()?;
                if has_update {
                    self.events.push_back(update);
                }
                if let Some(terminal) = terminal {
                    self.events.push_back(terminal);
                }
            }
            _ => return Err("deriv account: unexpected message type".into()),
        }
        Ok(())
    }
}
fn number(value: &WireDecimal) -> Result<Decimal, String> {
    value.require_number()
}
fn identifier(value: &WireDecimal) -> Result<String, String> {
    let id = number(value)?.rescale(0)?;
    if id.coefficient() <= 0 {
        return Err("deriv: invalid provider identity".into());
    }
    Ok(id.to_string())
}
fn direction(value: &str) -> Result<Direction, String> {
    match value {
        "CALL" => Ok(Direction::Buy),
        "PUT" => Ok(Direction::Sell),
        _ => Err("deriv: unsupported contract type".into()),
    }
}
fn source(id: String, provider_time_micros: i64, available_at_micros: i64) -> EventSource {
    EventSource {
        id,
        provider_time_micros,
        available_at_micros,
        simulated: false,
    }
}
impl DerivOptions {
    pub fn account(&self) -> &AccountIdentity {
        &self.account
    }
    pub fn balance(&mut self) -> Result<Decimal, String> {
        Ok(self.authenticated.balance()?.amount)
    }
    pub fn subscribe_transactions(&mut self) -> Result<(), String> {
        if self.transactions {
            return Err("deriv transaction: already subscribed".into());
        }
        let response =
            self.authenticated
                .connection
                .request(RateGroup::Other, "transaction", |req_id| SubscribeRequest {
                    transaction: 1,
                    subscribe: 1,
                    req_id,
                })?;
        let id = response
            .header
            .subscription
            .as_ref()
            .ok_or("deriv transaction: subscription identity missing")?
            .id
            .clone();
        let acknowledgement: TransactionResponse = decode(&response.raw, "transaction")?;
        if acknowledgement.transaction.id.is_none()
            || acknowledgement.transaction.action.is_some()
            || acknowledgement.transaction.amount.is_some()
        {
            return Err("deriv transaction: expected subscription acknowledgement".into());
        }
        self.decode_event(response)?;
        self.authenticated.connection.subscriptions.insert(id);
        self.transactions = true;
        Ok(())
    }
    pub fn proposal(&mut self, request: &ProposalRequest) -> Result<Proposal, String> {
        // deriv-demo-run-01 measured strict callput CALL/PUT economics on demo USD only.
        if self.account.class != MEASURED_ACCOUNT_CLASS
            || self.account.currency.as_str() != MEASURED_CURRENCY
        {
            return Err("deriv: the strict Rise/Fall mapping is measured only for demo USD accounts; inspect and extend the mapping before use".into());
        }
        self.currency(&request.currency)?;
        if request.binding.is_empty()
            || request.instrument.broker != self.account.broker
            || self
                .instruments
                .get(request.instrument.provider_symbol.as_str())
                != Some(&request.scale)
            || request.duration_seconds == 0
            || request.stake.coefficient() <= 0
            || request.settlement.rule != SettlementRule::BrokerAuthoritativeV1
            || request.semantics != ContractSemantics::RiseFallStrictV1
        {
            return Err("deriv proposal: unsupported request scope or semantics".into());
        }
        let response =
            self.authenticated
                .connection
                .request(RateGroup::Trade, "proposal", |req_id| QuoteRequest {
                    proposal: 1,
                    amount: WireDecimal::from_decimal(request.stake),
                    basis: "stake",
                    contract_type: match request.direction {
                        Direction::Buy => MEASURED_CONTRACT_TYPES[0],
                        Direction::Sell => MEASURED_CONTRACT_TYPES[1],
                    },
                    currency: &request.currency,
                    duration: request.duration_seconds,
                    duration_unit: "s",
                    underlying_symbol: request.instrument.provider_symbol.as_str(),
                    req_id,
                })?;
        let body: QuoteResponse = decode(&response.raw, "proposal")?;
        let q = body.proposal;
        if q.commission
            .as_ref()
            .map(number)
            .transpose()?
            .is_some_and(|fee| !fee.is_zero())
        {
            return Err("deriv proposal: commission has unresolved fee inclusion".into());
        }
        q.spot.require_number()?;
        if q.id.is_empty() {
            return Err("deriv proposal: empty identity".into());
        }
        let identity = format!(
            "{}:{}",
            self.authenticated.connection.continuity().generation(),
            q.id
        );
        let zero = Cashflow {
            gross_return: Decimal::zero(0),
            terminal_fee: Decimal::zero(0),
        };
        let mut proposal = Proposal {
            identity: identity.clone(),
            request_identity: String::new(),
            account: self.account.account.clone(),
            instrument: request.instrument.to_string(),
            terms: ContractTerms {
                id: identity.clone(),
                direction: request.direction,
                duration_micros: i64::from(request.duration_seconds) * 1_000_000,
                currency: request.currency.clone(),
                stake: request.stake,
                quoted_cost: number(&q.ask_price)?,
                entry_fee: Decimal::zero(0),
                win: Cashflow {
                    gross_return: number(&q.payout)?,
                    ..zero
                },
                loss: zero,
                tie: zero,
                settlement: request.settlement,
                semantics: Some(request.semantics),
            },
            spot_units: q.spot.price_units(request.scale)?,
            spot_time_micros: micros(q.spot_time)?,
            receipt_micros: response.receipt_micros,
            schema: format!("{SCHEMA}:proposal"),
            payload_sha256: payload_hash(&response.raw),
        };
        proposal.request_identity = proposal.canonical_request_identity()?;
        if proposal.terms.quoted_cost.coefficient() <= 0
            || proposal.terms.win.gross_return.is_negative()
        {
            return Err("deriv proposal: invalid economics".into());
        }
        self.proposals.insert(
            identity,
            IssuedProposal {
                spot: q.spot.token()?.into_owned(),
                provider_id: q.id,
                longcode: q.longcode,
            },
        );
        Ok(proposal)
    }
    pub fn purchase(&mut self, prepared: &PreparedPurchase) -> Result<PurchaseOutcome, String> {
        if prepared.dispatch_claim.is_empty() {
            return Err("deriv buy: dispatch claim is required".into());
        }
        if self.written.contains(&prepared.dispatch_claim)
            || self.possibly_sent.contains(&prepared.command)
        {
            return Err("deriv buy: dispatch claim already written or possibly sent; reconcile before any retry".into());
        }
        let encoded = (|| {
            if prepared.command.is_empty() || prepared.maximum_price.coefficient() <= 0 {
                return Err("deriv buy: invalid prepared command".into());
            }
            let proposal = self
                .proposals
                .get(&prepared.proposal_identity)
                .ok_or("deriv buy: proposal is not from this connection")?;
            self.authenticated
                .connection
                .prepare(RateGroup::Trade, |req_id| BuyRequest {
                    buy: &proposal.provider_id,
                    price: WireDecimal::from_decimal(prepared.maximum_price),
                    req_id,
                })
        })();
        let (id, text) = match encoded {
            Ok(encoded) => encoded,
            Err(reason) => return Ok(PurchaseOutcome::ProvenNotSent { reason }),
        };
        self.written.insert(prepared.dispatch_claim.clone());
        self.possibly_sent.insert(prepared.command.clone());
        let response = self
            .authenticated
            .connection
            .transport
            .send(Frame::Text(text))
            .and_then(|()| self.authenticated.connection.response(id, "buy"));
        let response = match response {
            Ok(response) => response,
            Err(reason) => return Ok(PurchaseOutcome::PossiblySent { reason }),
        };
        if let Some(error) = response.header.error {
            self.possibly_sent.remove(&prepared.command);
            return Ok(PurchaseOutcome::Rejected { code: error.code });
        }
        Ok(match purchase_fact(&response.raw) {
            Ok((debit, liability)) => {
                self.possibly_sent.remove(&prepared.command);
                PurchaseOutcome::Accepted { debit, liability }
            }
            Err(reason) => PurchaseOutcome::PossiblySent { reason },
        })
    }
    pub fn subscribe_contract(&mut self, contract_ref: &str) -> Result<(), String> {
        if self.contracts.contains(contract_ref) {
            return Err("deriv contract: already subscribed".into());
        }
        let contract_id = WireDecimal::from_decimal(Decimal::parse(contract_ref)?);
        identifier(&contract_id)?;
        let response = self.authenticated.connection.request(
            RateGroup::Trade,
            "proposal_open_contract",
            |req_id| ContractRequest {
                proposal_open_contract: 1,
                contract_id,
                subscribe: 1,
                req_id,
            },
        )?;
        let id = response
            .header
            .subscription
            .as_ref()
            .ok_or("deriv contract: subscription identity missing")?
            .id
            .clone();
        self.contracts.insert(contract_ref.into());
        self.decode_event(response)?;
        self.authenticated.connection.subscriptions.insert(id);
        Ok(())
    }
    pub fn next_account_event(
        &mut self,
        timeout_micros: i64,
    ) -> Result<Option<AccountEvent>, String> {
        if let Some(event) = self.events.pop_front() {
            return Ok(Some(event));
        }
        let Some(response) = self.authenticated.connection.next(timeout_micros)? else {
            return Ok(None);
        };
        if !response
            .header
            .subscription
            .as_ref()
            .is_some_and(|s| self.authenticated.connection.subscriptions.contains(&s.id))
        {
            return Err("deriv account: unknown subscription".into());
        }
        self.decode_event(response)?;
        Ok(self.events.pop_front())
    }
    pub fn open_contracts(&mut self) -> Result<Vec<OpenContract>, String> {
        let response =
            self.authenticated
                .connection
                .request(RateGroup::Portfolio, "portfolio", |req_id| {
                    PortfolioRequest {
                        portfolio: 1,
                        req_id,
                    }
                })?;
        let body: PortfolioResponse = decode(&response.raw, "portfolio")?;
        body.portfolio
            .contracts
            .into_iter()
            .map(|c| {
                self.currency(&c.currency)?;
                if !self.instruments.contains_key(&c.underlying_symbol) {
                    return Err("deriv portfolio: instrument outside configured scope".into());
                }
                Ok(OpenContract {
                    contract_ref: identifier(&c.contract_id)?,
                    transaction_ref: identifier(&c.transaction_id)?,
                    buy_price: number(&c.buy_price)?,
                    payout: number(&c.payout)?,
                    purchase_time_micros: micros(c.purchase_time)?,
                    start_micros: c.date_start.map(micros).transpose()?,
                    expiry_micros: c.expiry_time.map(micros).transpose()?,
                    instrument: c.underlying_symbol,
                    direction: direction(&c.contract_type)?,
                })
            })
            .collect()
    }
    pub fn statement(
        &mut self,
        from_secs: i64,
        through_secs: i64,
    ) -> Result<Vec<StatementRow>, String> {
        if from_secs > through_secs {
            return Err("deriv statement: invalid range".into());
        }
        let date_to = through_secs
            .checked_add(1)
            .ok_or("deriv statement: upper bound overflow")?;
        let mut offset = 0u32;
        let mut facts = Vec::new();
        loop {
            let response = self.authenticated.connection.request(
                RateGroup::Account,
                "statement",
                |req_id| StatementRequest {
                    statement: 1,
                    date_from: from_secs,
                    date_to,
                    description: 1,
                    limit: 100,
                    offset,
                    req_id,
                },
            )?;
            let body: StatementResponse = decode(&response.raw, "statement")?;
            if body.statement.count as usize != body.statement.transactions.len()
                || body.statement.count > 100
            {
                return Err("deriv statement: page count mismatch".into());
            }
            for t in body.statement.transactions {
                if t.transaction_time < from_secs || t.transaction_time >= date_to {
                    return Err("deriv statement: transaction outside requested range".into());
                }
                facts.push(StatementRow {
                    payout: t.payout.as_ref().map(number).transpose()?,
                    cash: CashFact {
                        account: self.account.account.clone(),
                        transaction_ref: identifier(&t.transaction_id)?,
                        contract_ref: t.contract_id.as_ref().map(identifier).transpose()?,
                        action: t.action_type,
                        amount: number(&t.amount)?,
                        time_micros: micros(t.transaction_time)?,
                    },
                });
            }
            if body.statement.count < 100 {
                break;
            }
            offset = offset
                .checked_add(100)
                .ok_or("deriv statement: offset overflow")?;
        }
        Ok(facts)
    }
}

/// Normalizes retained purchase evidence for reconciliation without another submission.
pub fn purchase_fact(raw: &[u8]) -> Result<(Decimal, BrokerLiability), String> {
    let header: super::Envelope = decode(raw, "buy")?;
    if header.msg_type != "buy" || header.error.is_some() {
        return Err("deriv buy: response does not prove purchase acceptance".into());
    }
    let body: BuyResponse = decode(raw, "buy")?;
    let b = body.buy;
    let debit = number(&b.buy_price)?;
    let payout = number(&b.payout)?;
    if debit.is_negative() || payout.is_negative() {
        return Err("deriv buy: invalid confirmed economics".into());
    }
    Ok((
        debit,
        BrokerLiability {
            contract_ref: identifier(&b.contract_id)?,
            transaction_ref: identifier(&b.transaction_id)?,
            purchase_time_micros: micros(b.purchase_time)?,
            expected_start_micros: b.start_time.map(micros).transpose()?,
            payout,
        },
    ))
}

/// Maps contract and account identities once, preserving broker clocks and cash deduplication.
pub fn to_observation(
    event: AccountEvent,
    command_of: &dyn Fn(&str) -> Option<String>,
    receipt: i64,
) -> Option<execution::Observation> {
    Some(match event {
        AccountEvent::TransactionAcknowledged => return None,
        AccountEvent::Cash(fact) => execution::Observation::Cash {
            source: source(
                format!("deriv:transaction:{}", fact.transaction_ref),
                fact.time_micros,
                receipt,
            ),
            fact,
        },
        AccountEvent::ContractUpdate {
            contract_ref,
            source,
            entry_price_units,
            entry_time_micros,
            start_micros,
            expiry_micros,
        } => execution::Observation::ContractUpdate {
            command: command_of(&contract_ref)?,
            source,
            entry_price_units,
            entry_time_micros,
            start_micros,
            expiry_micros,
        },
        AccountEvent::Terminal {
            contract_ref,
            source,
            fact,
        } => execution::Observation::Terminal {
            command: command_of(&contract_ref)?,
            source,
            fact,
        },
    })
}
/// Maps a claimed dispatch result to the existing Engine transitions.
pub fn purchase_observation(
    command: &str,
    claim: &str,
    outcome: PurchaseOutcome,
    receipt: i64,
) -> execution::Observation {
    let dispatch = source(format!("deriv:dispatch:{claim}"), receipt, receipt);
    match outcome {
        PurchaseOutcome::Accepted { debit, liability } => execution::Observation::Purchased {
            command: command.into(),
            source: source(
                format!("deriv:buy:{}", liability.transaction_ref),
                liability.purchase_time_micros,
                receipt,
            ),
            debit,
            liability,
        },
        PurchaseOutcome::Rejected { .. } => execution::Observation::Rejected {
            command: command.into(),
            source: dispatch,
        },
        PurchaseOutcome::ProvenNotSent { .. } => execution::Observation::NotSent {
            command: command.into(),
            source: dispatch,
        },
        PurchaseOutcome::PossiblySent { .. } => execution::Observation::PossiblySent {
            command: command.into(),
            source: dispatch,
        },
    }
}

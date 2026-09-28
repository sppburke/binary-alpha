//! The two account adapters behind the existing ordered account worker.
use super::deriv::{self, DerivOptions};
use super::pocket_options::{self, PocketOptions};
use super::{
    AccountEvent, AccountIdentity, OpenContract, PreparedPurchase, ProposalRequest,
    PurchaseOutcome, Statement, StatementCoverage,
};
use binary_alpha_engine::config::BrokerKind;
use binary_alpha_engine::execution::{self, Decimal, EventSource, Proposal};

#[derive(Debug, Clone)]
pub enum Encoded {
    Deriv(deriv::Encoded),
    Pocket(pocket_options::Encoded),
}
impl Encoded {
    pub fn command(&self) -> &str {
        match self {
            Self::Deriv(encoded) => encoded.command(),
            Self::Pocket(encoded) => encoded.command(),
        }
    }
    pub fn request_id(&self) -> Option<u64> {
        match self {
            Self::Deriv(_) => None,
            Self::Pocket(encoded) => Some(encoded.request_id),
        }
    }
}

pub enum Options {
    Deriv(DerivOptions),
    Pocket(PocketOptions),
}
impl From<DerivOptions> for Options {
    fn from(value: DerivOptions) -> Self {
        Self::Deriv(value)
    }
}
impl From<PocketOptions> for Options {
    fn from(value: PocketOptions) -> Self {
        Self::Pocket(value)
    }
}
impl Options {
    pub fn reserve_request_ids(&mut self, ids: impl IntoIterator<Item = u64>) {
        if let Self::Pocket(account) = self {
            account.reserve_request_ids(ids);
        }
    }
    pub fn account(&self) -> &AccountIdentity {
        match self {
            Self::Deriv(a) => a.account(),
            Self::Pocket(a) => a.account(),
        }
    }
    pub fn now_micros(&self) -> i64 {
        match self {
            Self::Deriv(a) => a.now_micros(),
            Self::Pocket(a) => a.now_micros(),
        }
    }
    pub fn rejected(&self, reason: &str) -> bool {
        match self {
            Self::Deriv(a) => a.rejected(reason),
            Self::Pocket(a) => a.rejected(reason),
        }
    }
    pub fn balance(&mut self) -> Result<Decimal, String> {
        match self {
            Self::Deriv(a) => a.balance(),
            Self::Pocket(a) => a.balance(),
        }
    }
    pub fn subscribe_transactions(&mut self) -> Result<(), String> {
        match self {
            Self::Deriv(a) => a.subscribe_transactions(),
            Self::Pocket(a) => a.subscribe_transactions(),
        }
    }
    pub fn proposal(&mut self, request: &ProposalRequest) -> Result<Proposal, String> {
        match self {
            Self::Deriv(a) => a.proposal(request),
            Self::Pocket(_) => Err("pocket offers are synchronous in the owner".into()),
        }
    }
    pub fn prepare_purchase(&mut self, prepared: &PreparedPurchase) -> Result<Encoded, String> {
        match self {
            Self::Deriv(a) => a.prepare_purchase(prepared).map(Encoded::Deriv),
            Self::Pocket(a) => a.prepare_purchase(prepared).map(Encoded::Pocket),
        }
    }
    pub fn write_purchase(&mut self, encoded: Encoded) -> Result<PurchaseOutcome, String> {
        match (self, encoded) {
            (Self::Deriv(a), Encoded::Deriv(e)) => a.write_purchase(e),
            (Self::Pocket(a), Encoded::Pocket(e)) => a.write_purchase(e),
            _ => Err("account adapter and prepared order differ".into()),
        }
    }
    pub fn subscribe_contract(&mut self, id: &str) -> Result<(), String> {
        match self {
            Self::Deriv(a) => a.subscribe_contract(id),
            Self::Pocket(a) => a.subscribe_contract(id),
        }
    }
    pub fn queued_account_event(&mut self) -> Result<Option<AccountEvent>, String> {
        match self {
            Self::Deriv(a) => a.queued_account_event(),
            Self::Pocket(a) => a.queued_account_event(),
        }
    }
    pub fn next_account_event(&mut self, timeout: i64) -> Result<Option<AccountEvent>, String> {
        match self {
            Self::Deriv(a) => a.next_account_event(timeout),
            Self::Pocket(a) => a.next_account_event(timeout),
        }
    }
    pub fn open_contracts(&mut self) -> Result<Vec<OpenContract>, String> {
        match self {
            Self::Deriv(a) => a.open_contracts(),
            Self::Pocket(a) => a.open_contracts(),
        }
    }
    pub fn statement(&mut self, from: i64, through: i64) -> Result<Statement, String> {
        match self {
            Self::Deriv(a) => Ok(Statement {
                coverage: StatementCoverage::CompleteRange,
                rows: a.statement(from, through)?,
            }),
            Self::Pocket(a) => a.statement(from, through),
        }
    }
}

pub fn to_observation(
    event: AccountEvent,
    kind: BrokerKind,
    command_of: &dyn Fn(&str) -> Option<String>,
) -> Option<execution::Observation> {
    match event {
        AccountEvent::Cash {
            fact,
            receipt_micros,
        } if kind == BrokerKind::PocketOption => Some(execution::Observation::Cash {
            source: EventSource {
                id: format!("pocket:cash:{}", fact.transaction_ref),
                provider_time_micros: fact.time_micros,
                available_at_micros: receipt_micros,
                simulated: false,
            },
            fact,
        }),
        event => deriv::to_observation(event, command_of),
    }
}

pub fn purchase_observation(
    command: &str,
    claim: &str,
    outcome: PurchaseOutcome,
    receipt: i64,
    kind: BrokerKind,
) -> execution::Observation {
    if kind == BrokerKind::Deriv {
        return deriv::purchase_observation(command, claim, outcome, receipt);
    }
    let dispatch = EventSource {
        id: format!("pocket:dispatch:{claim}"),
        provider_time_micros: receipt,
        available_at_micros: receipt,
        simulated: false,
    };
    match outcome {
        PurchaseOutcome::Accepted {
            debit,
            liability,
            receipt_micros,
        } => execution::Observation::Purchased {
            command: command.into(),
            source: EventSource {
                id: format!("pocket:buy:{}", liability.contract_ref),
                provider_time_micros: liability.purchase_time_micros,
                available_at_micros: receipt_micros,
                simulated: false,
            },
            debit,
            liability,
        },
        PurchaseOutcome::Rejected { receipt_micros, .. } => execution::Observation::Rejected {
            command: command.into(),
            source: EventSource {
                provider_time_micros: receipt_micros,
                available_at_micros: receipt_micros,
                ..dispatch
            },
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

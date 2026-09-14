//! Cooperative account leases and the minimal durable dispatch claim.

use std::collections::{BTreeMap, VecDeque};
use std::path::Path;
use std::sync::{Arc, Mutex};

use binary_alpha_engine::execution::{Disposition, EventKind, FinancialEvent, Proposal};
use rustls::pki_types::{CertificateDer, pem::PemObject};
use serde::{Deserialize, Serialize};
use tokio_postgres::{Client, Row, Transaction};

#[derive(Debug, Clone, Copy)]
pub struct LeaseKey<'a> {
    pub broker: &'a str,
    pub account: &'a str,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Lease {
    pub token: u64,
    pub expires_at_micros: i64,
    pub server_now_micros: i64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Claim {
    pub command: String,
    pub claim: String,
    pub deployment: String,
    pub token: u64,
    pub max_proposal_age_micros: i64,
    pub signal: FinancialEvent,
    pub state: ClaimState,
    pub contract_ref: Option<String>,
    pub transaction_ref: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ClaimState {
    Claimed,
    NotSent,
    PossiblySent,
    Accepted,
    Rejected,
    Reconciled,
}

impl ClaimState {
    fn text(self) -> &'static str {
        match self {
            Self::Claimed => "claimed",
            Self::NotSent => "not_sent",
            Self::PossiblySent => "possibly_sent",
            Self::Accepted => "accepted",
            Self::Rejected => "rejected",
            Self::Reconciled => "reconciled",
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
#[allow(clippy::large_enum_variant)]
pub enum ClaimOutcome {
    Inserted,
    Replay(Claim),
    LeaseLost,
}

pub trait Control {
    fn advance_to(&mut self, _micros: i64) {}
    fn acquire(
        &mut self,
        key: LeaseKey<'_>,
        owner: &str,
        deployment: &str,
        ttl_micros: i64,
    ) -> Result<Option<Lease>, String>;
    fn renew(
        &mut self,
        key: LeaseKey<'_>,
        owner: &str,
        token: u64,
        ttl_micros: i64,
    ) -> Result<Option<Lease>, String>;
    fn release(&mut self, key: LeaseKey<'_>, owner: &str, token: u64) -> Result<bool, String>;
    fn claim(
        &mut self,
        key: LeaseKey<'_>,
        owner: &str,
        token: u64,
        claim: &Claim,
    ) -> Result<ClaimOutcome, String>;
    #[allow(clippy::too_many_arguments)]
    fn update_claim(
        &mut self,
        key: LeaseKey<'_>,
        owner: &str,
        token: u64,
        claim: &str,
        state: ClaimState,
        contract_ref: Option<&str>,
        transaction_ref: Option<&str>,
    ) -> Result<bool, String>;
    fn unresolved(&mut self, key: LeaseKey<'_>) -> Result<Vec<Claim>, String>;
    /// Durable recovery rows, including reconciled claims awaiting verified journal archival.
    /// Controls that retain reconciled rows must include them here until deletion.
    fn retained_claims(&mut self, key: LeaseKey<'_>) -> Result<Vec<Claim>, String> {
        self.unresolved(key)
    }
    fn delete_reconciled(&mut self, key: LeaseKey<'_>, claim: &str) -> Result<(), String>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Fault {
    LoseResponse,
    Partition,
}

#[derive(Clone)]
struct LeaseRow {
    owner: String,
    deployment: String,
    token: u64,
    expires: i64,
}

#[derive(Default)]
struct FakeState {
    clock: i64,
    faults: VecDeque<Fault>,
    leases: BTreeMap<(String, String), LeaseRow>,
    claims: BTreeMap<(String, String, String), (i64, Claim)>,
}

/// Clones share database rows, server time, and the scripted fault queue.
#[derive(Clone, Default)]
pub struct FakeControl {
    state: Arc<Mutex<FakeState>>,
}

impl FakeControl {
    pub fn new(clock_micros: i64) -> Self {
        Self {
            state: Arc::new(Mutex::new(FakeState {
                clock: clock_micros,
                ..FakeState::default()
            })),
        }
    }
    pub fn advance(&mut self, micros: i64) {
        assert!(micros >= 0, "fake database time cannot go backwards");
        let mut state = self.state.lock().unwrap();
        state.clock = state
            .clock
            .checked_add(micros)
            .expect("fake clock overflow");
    }
    pub fn fault(&mut self, fault: Fault) {
        self.state.lock().unwrap().faults.push_back(fault);
    }
    fn apply<T>(
        &mut self,
        operation: impl FnOnce(&mut FakeState) -> Result<T, String>,
    ) -> Result<T, String> {
        let mut state = self.state.lock().map_err(|error| error.to_string())?;
        let fault = state.faults.pop_front();
        if fault == Some(Fault::Partition) {
            return Err("partition".into());
        }
        let value = operation(&mut state)?;
        if fault == Some(Fault::LoseResponse) {
            return Err("response lost".into());
        }
        Ok(value)
    }
}

fn owned_key(key: LeaseKey<'_>) -> (String, String) {
    (key.broker.into(), key.account.into())
}
fn claim_key(key: LeaseKey<'_>, command: &str) -> (String, String, String) {
    (key.broker.into(), key.account.into(), command.into())
}
fn expiry(now: i64, ttl: i64) -> Result<i64, String> {
    if ttl <= 0 {
        return Err("lease ttl must be positive".into());
    }
    now.checked_add(ttl)
        .ok_or_else(|| "lease expiry overflow".into())
}
fn current(row: &LeaseRow, owner: &str, token: u64, now: i64) -> bool {
    row.owner == owner && row.token == token && row.expires > now
}
fn proposal<'a>(
    claim: &'a Claim,
    key: LeaseKey<'_>,
    token: u64,
    deployment: &str,
) -> Result<&'a Proposal, String> {
    if claim.token != token || claim.deployment != deployment {
        return Err("claim lease binding mismatch".into());
    }
    let EventKind::Signal {
        command: Some(command),
        disposition: Disposition::Admitted,
        proposal: Some(proposal),
        reservation: Some(_),
        instrument,
        ..
    } = &claim.signal.kind
    else {
        return Err("claim requires an admitted signal with proposal and reservation".into());
    };
    if command != &claim.command
        || proposal.account != key.account
        || &proposal.instrument != instrument
        || !proposal.instrument.starts_with(&format!("{}:", key.broker))
        || claim.claim.is_empty()
    {
        return Err("claim signal binding mismatch".into());
    }
    Ok(proposal)
}

impl Control for FakeControl {
    fn advance_to(&mut self, micros: i64) {
        let mut state = self.state.lock().unwrap();
        state.clock = state.clock.max(micros);
    }
    fn acquire(
        &mut self,
        key: LeaseKey<'_>,
        owner: &str,
        deployment: &str,
        ttl_micros: i64,
    ) -> Result<Option<Lease>, String> {
        self.apply(|state| {
            let expires = expiry(state.clock, ttl_micros)?;
            let previous = state.leases.get(&owned_key(key));
            if previous.is_some_and(|row| row.expires > state.clock) {
                return Ok(None);
            }
            let token = previous
                .map_or(0, |row| row.token)
                .checked_add(1)
                .filter(|token| *token <= i64::MAX as u64)
                .ok_or("lease token overflow")?;
            state.leases.insert(
                owned_key(key),
                LeaseRow {
                    owner: owner.into(),
                    deployment: deployment.into(),
                    token,
                    expires,
                },
            );
            Ok(Some(Lease {
                token,
                expires_at_micros: expires,
                server_now_micros: state.clock,
            }))
        })
    }
    fn renew(
        &mut self,
        key: LeaseKey<'_>,
        owner: &str,
        token: u64,
        ttl_micros: i64,
    ) -> Result<Option<Lease>, String> {
        self.apply(|state| {
            let expires = expiry(state.clock, ttl_micros)?;
            let Some(row) = state
                .leases
                .get_mut(&owned_key(key))
                .filter(|row| current(row, owner, token, state.clock))
            else {
                return Ok(None);
            };
            row.expires = expires;
            Ok(Some(Lease {
                token,
                expires_at_micros: expires,
                server_now_micros: state.clock,
            }))
        })
    }
    fn release(&mut self, key: LeaseKey<'_>, owner: &str, token: u64) -> Result<bool, String> {
        self.apply(|state| {
            let Some(row) = state
                .leases
                .get_mut(&owned_key(key))
                .filter(|row| row.owner == owner && row.token == token)
            else {
                return Ok(false);
            };
            row.expires = state.clock;
            Ok(true)
        })
    }
    fn claim(
        &mut self,
        key: LeaseKey<'_>,
        owner: &str,
        token: u64,
        claim: &Claim,
    ) -> Result<ClaimOutcome, String> {
        self.apply(|state| {
            let Some(row) = state
                .leases
                .get(&owned_key(key))
                .filter(|row| current(row, owner, token, state.clock))
            else {
                return Ok(ClaimOutcome::LeaseLost);
            };
            proposal(claim, key, token, &row.deployment)?;
            let identity = claim_key(key, &claim.command);
            if let Some((_, existing)) = state.claims.get(&identity) {
                return Ok(ClaimOutcome::Replay(existing.clone()));
            }
            state.claims.insert(identity, (state.clock, claim.clone()));
            Ok(ClaimOutcome::Inserted)
        })
    }
    fn update_claim(
        &mut self,
        key: LeaseKey<'_>,
        owner: &str,
        token: u64,
        command: &str,
        status: ClaimState,
        contract_ref: Option<&str>,
        transaction_ref: Option<&str>,
    ) -> Result<bool, String> {
        self.apply(|state| {
            if !state
                .leases
                .get(&owned_key(key))
                .is_some_and(|row| current(row, owner, token, state.clock))
            {
                return Ok(false);
            }
            let Some((_, claim)) = state
                .claims
                .get_mut(&claim_key(key, command))
                .filter(|(_, claim)| claim.token <= token)
            else {
                return Ok(false);
            };
            claim.state = status;
            if let Some(value) = contract_ref {
                claim.contract_ref = Some(value.into());
            }
            if let Some(value) = transaction_ref {
                claim.transaction_ref = Some(value.into());
            }
            Ok(true)
        })
    }
    fn unresolved(&mut self, key: LeaseKey<'_>) -> Result<Vec<Claim>, String> {
        Ok(self
            .retained_claims(key)?
            .into_iter()
            .filter(|claim| claim.state != ClaimState::Reconciled)
            .collect())
    }
    fn retained_claims(&mut self, key: LeaseKey<'_>) -> Result<Vec<Claim>, String> {
        self.apply(|state| {
            let mut claims: Vec<_> = state
                .claims
                .iter()
                .filter(|((broker, account, _), _)| broker == key.broker && account == key.account)
                .map(|(_, value)| value)
                .collect();
            claims.sort_by(|(a, x), (b, y)| (a, &x.command).cmp(&(b, &y.command)));
            Ok(claims.into_iter().map(|(_, claim)| claim.clone()).collect())
        })
    }
    fn delete_reconciled(&mut self, key: LeaseKey<'_>, command: &str) -> Result<(), String> {
        self.apply(|state| {
            let key = claim_key(key, command);
            if state
                .claims
                .get(&key)
                .is_some_and(|(_, claim)| claim.state == ClaimState::Reconciled)
            {
                state.claims.remove(&key);
            }
            Ok(())
        })
    }
}

pub const MIGRATION_SQL: &str = r#"
CREATE TABLE IF NOT EXISTS live_leases (
    broker text, account text, owner text NOT NULL, deployment text NOT NULL,
    token bigint NOT NULL, acquired_at timestamptz NOT NULL,
    expires_at timestamptz NOT NULL, updated_at timestamptz NOT NULL,
    PRIMARY KEY (broker, account)
);
CREATE TABLE IF NOT EXISTS live_dispatch_claims (
    broker text, account text, command text, claim text NOT NULL,
    proposal text NOT NULL, request text NOT NULL, deployment text NOT NULL,
    token bigint NOT NULL, payload jsonb NOT NULL, state text NOT NULL,
    contract_ref text, transaction_ref text,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (broker, account, command)
);
COMMENT ON TABLE live_leases IS 'binary-alpha live control schema v1';
COMMENT ON TABLE live_dispatch_claims IS 'binary-alpha live control schema v1';
"#;

const LOCK_SQL: &str = "SELECT owner, deployment, token, (EXTRACT(EPOCH FROM expires_at) * 1000000)::bigint AS expires FROM live_leases WHERE broker=$1 AND account=$2 FOR UPDATE";
const CLOCK_SQL: &str = "SELECT (EXTRACT(EPOCH FROM clock_timestamp()) * 1000000)::bigint";
const ACQUIRE_SQL: &str = "WITH stamp AS (SELECT clock_timestamp() AS now) INSERT INTO live_leases (broker, account, owner, deployment, token, acquired_at, expires_at, updated_at) SELECT $1, $2, $3, $4, 1, now, now + ($5::bigint::text || ' microseconds')::interval, now FROM stamp ON CONFLICT (broker, account) DO NOTHING RETURNING token, (EXTRACT(EPOCH FROM expires_at) * 1000000)::bigint AS expires, (EXTRACT(EPOCH FROM acquired_at) * 1000000)::bigint AS now";
const TAKEOVER_SQL: &str = "WITH stamp AS (SELECT clock_timestamp() AS now) UPDATE live_leases SET owner=$3, deployment=$4, token=token+1, acquired_at=stamp.now, expires_at=stamp.now + ($5::bigint::text || ' microseconds')::interval, updated_at=stamp.now FROM stamp WHERE broker=$1 AND account=$2 RETURNING token, (EXTRACT(EPOCH FROM expires_at) * 1000000)::bigint AS expires, (EXTRACT(EPOCH FROM updated_at) * 1000000)::bigint AS now";
const RENEW_SQL: &str = "WITH stamp AS (SELECT clock_timestamp() AS now) UPDATE live_leases SET expires_at=stamp.now + ($5::bigint::text || ' microseconds')::interval, updated_at=stamp.now FROM stamp WHERE broker=$1 AND account=$2 AND owner=$3 AND token=$4 AND expires_at > stamp.now RETURNING token, (EXTRACT(EPOCH FROM expires_at) * 1000000)::bigint AS expires, (EXTRACT(EPOCH FROM updated_at) * 1000000)::bigint AS now";
const RELEASE_SQL: &str = "WITH stamp AS (SELECT clock_timestamp() AS now) UPDATE live_leases SET expires_at=stamp.now, updated_at=stamp.now FROM stamp WHERE broker=$1 AND account=$2 AND owner=$3 AND token=$4";
const CLAIM_SQL: &str = "INSERT INTO live_dispatch_claims (broker, account, command, claim, proposal, request, deployment, token, payload, state, contract_ref, transaction_ref) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9::text::jsonb,$10,$11,$12) ON CONFLICT (broker, account, command) DO NOTHING RETURNING claim";
const REPLAY_SQL: &str = "SELECT payload::text, state, contract_ref, transaction_ref FROM live_dispatch_claims WHERE broker=$1 AND account=$2 AND command=$3";
const UPDATE_SQL: &str = "UPDATE live_dispatch_claims SET state=$4, contract_ref=COALESCE($5, contract_ref), transaction_ref=COALESCE($6, transaction_ref), updated_at=clock_timestamp() WHERE broker=$1 AND account=$2 AND command=$3 AND token <= $7";
const UNRESOLVED_SQL: &str = "SELECT payload::text, state, contract_ref, transaction_ref FROM live_dispatch_claims WHERE broker=$1 AND account=$2 AND state <> 'reconciled' ORDER BY created_at, command";
const RETAINED_SQL: &str = "SELECT payload::text, state, contract_ref, transaction_ref FROM live_dispatch_claims WHERE broker=$1 AND account=$2 ORDER BY created_at, command";
const DELETE_SQL: &str = "DELETE FROM live_dispatch_claims WHERE broker=$1 AND account=$2 AND command=$3 AND state='reconciled'";

/// One PostgreSQL session, driven only while its current-thread runtime is entered.
pub struct Postgres {
    runtime: tokio::runtime::Runtime,
    client: Client,
}

impl Postgres {
    pub fn connect(
        host: &str,
        port: u16,
        database: &str,
        user: &str,
        password_reference: &str,
        root_certificate: &Path,
    ) -> Result<Self, String> {
        let mut roots = rustls::RootCertStore::empty();
        for certificate in
            CertificateDer::pem_file_iter(root_certificate).map_err(|error| error.to_string())?
        {
            roots
                .add(certificate.map_err(|error| error.to_string())?)
                .map_err(|error| error.to_string())?;
        }
        let tls = rustls::ClientConfig::builder_with_provider(Arc::new(
            rustls::crypto::aws_lc_rs::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .map_err(|error| error.to_string())?
        .with_root_certificates(roots)
        .with_no_client_auth();
        let mut config = tokio_postgres::Config::new();
        config
            .host(host)
            .port(port)
            .dbname(database)
            .user(user)
            .password(crate::broker::resolve_secret(password_reference)?)
            .ssl_mode(tokio_postgres::config::SslMode::Require);
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|error| error.to_string())?;
        let client = runtime.block_on(async {
            let (client, connection) = config
                .connect(tokio_postgres_rustls::MakeRustlsConnect::new(tls))
                .await
                .map_err(pg_error)?;
            tokio::spawn(async move {
                let _ = connection.await;
            });
            Ok::<_, String>(client)
        })?;
        Ok(Self { runtime, client })
    }

    fn read_claims(&mut self, key: LeaseKey<'_>, query: &str) -> Result<Vec<Claim>, String> {
        async fn operation(
            transaction: &Transaction<'_>,
            key: LeaseKey<'_>,
            query: &str,
        ) -> Result<Vec<Claim>, String> {
            transaction
                .query(query, &[&key.broker, &key.account])
                .await
                .map_err(pg_error)?
                .into_iter()
                .map(decode_claim)
                .collect()
        }
        self.runtime.block_on(async {
            let transaction = self.client.transaction().await.map_err(pg_error)?;
            let result = operation(&transaction, key, query).await;
            finish(transaction, result).await
        })
    }

    pub fn migrate(&mut self) -> Result<(), String> {
        async fn operation(transaction: &Transaction<'_>) -> Result<(), String> {
            transaction
                .batch_execute(MIGRATION_SQL)
                .await
                .map_err(pg_error)
        }
        self.runtime.block_on(async {
            let transaction = self.client.transaction().await.map_err(pg_error)?;
            let result = operation(&transaction).await;
            finish(transaction, result).await
        })
    }

    /// The database backend owning this session, for lock diagnostics.
    pub fn backend_pid(&mut self) -> Result<i32, String> {
        async fn operation(transaction: &Transaction<'_>) -> Result<i32, String> {
            transaction
                .query_one("SELECT pg_backend_pid()", &[])
                .await
                .map_err(pg_error)
                .map(|row| row.get(0))
        }
        self.runtime.block_on(async {
            let transaction = self.client.transaction().await.map_err(pg_error)?;
            let result = operation(&transaction).await;
            finish(transaction, result).await
        })
    }
}

fn pg_error(error: tokio_postgres::Error) -> String {
    let mut text = error.to_string();
    let mut cause = std::error::Error::source(&error);
    while let Some(error) = cause {
        text.push_str(&format!(": {error}"));
        cause = error.source();
    }
    text
}
fn sql_token(token: u64) -> Result<i64, String> {
    i64::try_from(token).map_err(|_| "lease token overflow".into())
}
fn lease(row: &Row) -> Result<Lease, String> {
    Ok(Lease {
        token: u64::try_from(row.get::<_, i64>("token")).map_err(|_| "negative lease token")?,
        expires_at_micros: row.get("expires"),
        server_now_micros: row.get("now"),
    })
}
async fn lock(
    transaction: &Transaction<'_>,
    key: LeaseKey<'_>,
) -> Result<(Option<LeaseRow>, i64), String> {
    let row = transaction
        .query_opt(LOCK_SQL, &[&key.broker, &key.account])
        .await
        .map_err(pg_error)?;
    // This separate statement starts after the lease row lock has been obtained.
    let now = transaction
        .query_one(CLOCK_SQL, &[])
        .await
        .map_err(pg_error)?
        .get(0);
    let row = row
        .map(|row| {
            Ok::<_, String>(LeaseRow {
                owner: row.get("owner"),
                deployment: row.get("deployment"),
                token: u64::try_from(row.get::<_, i64>("token"))
                    .map_err(|_| "negative lease token")?,
                expires: row.get("expires"),
            })
        })
        .transpose()?;
    Ok((row, now))
}
fn decode_claim(row: Row) -> Result<Claim, String> {
    let mut claim: Claim = serde_json::from_str(row.get(0)).map_err(|error| error.to_string())?;
    claim.state = match row.get::<_, &str>(1) {
        "claimed" => ClaimState::Claimed,
        "not_sent" => ClaimState::NotSent,
        "possibly_sent" => ClaimState::PossiblySent,
        "accepted" => ClaimState::Accepted,
        "rejected" => ClaimState::Rejected,
        "reconciled" => ClaimState::Reconciled,
        state => return Err(format!("unknown claim state {state}")),
    };
    claim.contract_ref = row.get(2);
    claim.transaction_ref = row.get(3);
    Ok(claim)
}

async fn finish<T>(transaction: Transaction<'_>, result: Result<T, String>) -> Result<T, String> {
    match result {
        Ok(value) => {
            transaction.commit().await.map_err(pg_error)?;
            Ok(value)
        }
        Err(error) => {
            let _ = transaction.rollback().await;
            Err(error)
        }
    }
}

impl Control for Postgres {
    fn acquire(
        &mut self,
        key: LeaseKey<'_>,
        owner: &str,
        deployment: &str,
        ttl_micros: i64,
    ) -> Result<Option<Lease>, String> {
        async fn operation(
            transaction: &Transaction<'_>,
            key: LeaseKey<'_>,
            owner: &str,
            deployment: &str,
            ttl_micros: i64,
        ) -> Result<Option<Lease>, String> {
            expiry(0, ttl_micros)?;
            let (row, now) = lock(transaction, key).await?;
            expiry(now, ttl_micros)?;
            if row.as_ref().is_some_and(|row| row.expires > now) {
                return Ok(None);
            }
            let sql = if row.is_some() {
                TAKEOVER_SQL
            } else {
                ACQUIRE_SQL
            };
            let row = transaction
                .query_opt(
                    sql,
                    &[&key.broker, &key.account, &owner, &deployment, &ttl_micros],
                )
                .await
                .map_err(pg_error)?;
            row.as_ref().map(lease).transpose()
        }
        self.runtime.block_on(async {
            let transaction = self.client.transaction().await.map_err(pg_error)?;
            let result = operation(&transaction, key, owner, deployment, ttl_micros).await;
            finish(transaction, result).await
        })
    }
    fn renew(
        &mut self,
        key: LeaseKey<'_>,
        owner: &str,
        token: u64,
        ttl_micros: i64,
    ) -> Result<Option<Lease>, String> {
        async fn operation(
            transaction: &Transaction<'_>,
            key: LeaseKey<'_>,
            owner: &str,
            token: u64,
            ttl_micros: i64,
        ) -> Result<Option<Lease>, String> {
            let token_sql = sql_token(token)?;
            expiry(0, ttl_micros)?;
            let (row, now) = lock(transaction, key).await?;
            expiry(now, ttl_micros)?;
            if !row.is_some_and(|row| current(&row, owner, token, now)) {
                return Ok(None);
            }
            let row = transaction
                .query_opt(
                    RENEW_SQL,
                    &[&key.broker, &key.account, &owner, &token_sql, &ttl_micros],
                )
                .await
                .map_err(pg_error)?;
            let lease = row.as_ref().map(lease).transpose()?;
            Ok(lease)
        }
        self.runtime.block_on(async {
            let transaction = self.client.transaction().await.map_err(pg_error)?;
            let result = operation(&transaction, key, owner, token, ttl_micros).await;
            finish(transaction, result).await
        })
    }
    fn release(&mut self, key: LeaseKey<'_>, owner: &str, token: u64) -> Result<bool, String> {
        async fn operation(
            transaction: &Transaction<'_>,
            key: LeaseKey<'_>,
            owner: &str,
            token: u64,
        ) -> Result<bool, String> {
            let token_sql = sql_token(token)?;
            let (row, _) = lock(transaction, key).await?;
            if !row.is_some_and(|row| row.owner == owner && row.token == token) {
                return Ok(false);
            }
            let changed = transaction
                .execute(
                    RELEASE_SQL,
                    &[&key.broker, &key.account, &owner, &token_sql],
                )
                .await
                .map_err(pg_error)?;
            Ok(changed == 1)
        }
        self.runtime.block_on(async {
            let transaction = self.client.transaction().await.map_err(pg_error)?;
            let result = operation(&transaction, key, owner, token).await;
            finish(transaction, result).await
        })
    }
    fn claim(
        &mut self,
        key: LeaseKey<'_>,
        owner: &str,
        token: u64,
        claim: &Claim,
    ) -> Result<ClaimOutcome, String> {
        async fn operation(
            transaction: &Transaction<'_>,
            key: LeaseKey<'_>,
            owner: &str,
            token: u64,
            claim: &Claim,
        ) -> Result<ClaimOutcome, String> {
            let token_sql = sql_token(token)?;
            let (row, now) = lock(transaction, key).await?;
            let Some(row) = row.filter(|row| current(row, owner, token, now)) else {
                return Ok(ClaimOutcome::LeaseLost);
            };
            let proposal = proposal(claim, key, token, &row.deployment)?;
            let payload = serde_json::to_string(claim).map_err(|error| error.to_string())?;
            let inserted = transaction
                .query_opt(
                    CLAIM_SQL,
                    &[
                        &key.broker,
                        &key.account,
                        &claim.command,
                        &claim.claim,
                        &proposal.identity,
                        &proposal.request_identity,
                        &claim.deployment,
                        &token_sql,
                        &payload,
                        &claim.state.text(),
                        &claim.contract_ref,
                        &claim.transaction_ref,
                    ],
                )
                .await
                .map_err(pg_error)?
                .is_some();
            let outcome = if inserted {
                ClaimOutcome::Inserted
            } else {
                ClaimOutcome::Replay(decode_claim(
                    transaction
                        .query_one(REPLAY_SQL, &[&key.broker, &key.account, &claim.command])
                        .await
                        .map_err(pg_error)?,
                )?)
            };
            Ok(outcome)
        }
        self.runtime.block_on(async {
            let transaction = self.client.transaction().await.map_err(pg_error)?;
            let result = operation(&transaction, key, owner, token, claim).await;
            finish(transaction, result).await
        })
    }
    fn update_claim(
        &mut self,
        key: LeaseKey<'_>,
        owner: &str,
        token: u64,
        command: &str,
        state: ClaimState,
        contract_ref: Option<&str>,
        transaction_ref: Option<&str>,
    ) -> Result<bool, String> {
        #[allow(clippy::too_many_arguments)]
        async fn operation(
            transaction: &Transaction<'_>,
            key: LeaseKey<'_>,
            owner: &str,
            token: u64,
            command: &str,
            state: ClaimState,
            contract_ref: Option<&str>,
            transaction_ref: Option<&str>,
        ) -> Result<bool, String> {
            let token_sql = sql_token(token)?;
            let (row, now) = lock(transaction, key).await?;
            if !row.is_some_and(|row| current(&row, owner, token, now)) {
                return Ok(false);
            }
            let changed = transaction
                .execute(
                    UPDATE_SQL,
                    &[
                        &key.broker,
                        &key.account,
                        &command,
                        &state.text(),
                        &contract_ref,
                        &transaction_ref,
                        &token_sql,
                    ],
                )
                .await
                .map_err(pg_error)?;
            Ok(changed == 1)
        }
        self.runtime.block_on(async {
            let transaction = self.client.transaction().await.map_err(pg_error)?;
            let result = operation(
                &transaction,
                key,
                owner,
                token,
                command,
                state,
                contract_ref,
                transaction_ref,
            )
            .await;
            finish(transaction, result).await
        })
    }
    fn unresolved(&mut self, key: LeaseKey<'_>) -> Result<Vec<Claim>, String> {
        self.read_claims(key, UNRESOLVED_SQL)
    }
    fn retained_claims(&mut self, key: LeaseKey<'_>) -> Result<Vec<Claim>, String> {
        self.read_claims(key, RETAINED_SQL)
    }
    fn delete_reconciled(&mut self, key: LeaseKey<'_>, command: &str) -> Result<(), String> {
        async fn operation(
            transaction: &Transaction<'_>,
            key: LeaseKey<'_>,
            command: &str,
        ) -> Result<(), String> {
            transaction
                .execute(DELETE_SQL, &[&key.broker, &key.account, &command])
                .await
                .map_err(pg_error)?;
            Ok(())
        }
        self.runtime.block_on(async {
            let transaction = self.client.transaction().await.map_err(pg_error)?;
            let result = operation(&transaction, key, command).await;
            finish(transaction, result).await
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fake_release_sets_expiry_to_server_now() {
        let mut control = FakeControl::new(100);
        let key = LeaseKey {
            broker: "deriv",
            account: "clock",
        };
        let first = control
            .acquire(key, "owner", "deployment", 20)
            .unwrap()
            .unwrap();
        control.advance(7);
        assert!(control.release(key, "owner", first.token).unwrap());
        assert_eq!(
            control.state.lock().unwrap().leases[&owned_key(key)].expires,
            107
        );
        let second = control
            .acquire(key, "owner", "deployment", 20)
            .unwrap()
            .unwrap();
        assert_eq!(second.token, 2);
        assert!(!control.release(key, "owner", first.token).unwrap());
        assert_eq!(
            control.state.lock().unwrap().leases[&owned_key(key)].expires,
            127
        );
    }
}

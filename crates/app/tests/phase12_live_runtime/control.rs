//! Identical lease/claim assertions for the fake and PostgreSQL transports.

use std::path::{Path, PathBuf};
use std::sync::{Arc, Barrier, mpsc};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use binary_alpha_engine::config::{Config, StreamKey};
use binary_alpha_engine::execution::{Disposition, EventKind, FinancialEvent, Proposal};
use rustls::pki_types::{CertificateDer, pem::PemObject};
use serde::Deserialize;
use tokio::runtime::Runtime;
use tokio_postgres::Client;

use crate::live::control::{
    Claim, ClaimOutcome, ClaimState, Control, FakeControl, Fault, LeaseKey, Postgres,
};

const TTL: i64 = 2_000_000;
fn key(account: &str) -> LeaseKey<'_> {
    LeaseKey {
        broker: "deriv",
        account,
    }
}
fn claim(account: &str, command: &str, token: u64) -> Claim {
    let config = Config::parse(include_str!(
        "../fixtures/phase10/execution-definition.toml"
    ))
    .unwrap();
    let terms = config.replay.unwrap().contracts.remove(0);
    let reservation = terms.purchase().unwrap();
    Claim {
        command: command.into(),
        claim: format!("dispatch-{command}"),
        deployment: "deployment".into(),
        token,
        max_proposal_age_micros: 500,
        signal: FinancialEvent {
            sequence: 1,
            time_micros: 10,
            kind: EventKind::Signal {
                instrument: "deriv:R_50".into(),
                binding: "call".into(),
                deployment_identity: "binding-deployment".into(),
                signal_logic_identity: "logic".into(),
                stream: StreamKey {
                    duration_seconds: 5,
                    offset_seconds: 0,
                },
                close_time_micros: 10,
                known_at_micros: 10,
                split: None,
                disposition: Disposition::Admitted,
                quote_price_units: Some(10000),
                quote_time_micros: Some(10),
                command: Some(command.into()),
                reservation: Some(reservation),
                rates: vec![],
                proposal: Some(Proposal {
                    identity: "proposal".into(),
                    request_identity: "request".into(),
                    account: account.into(),
                    instrument: "deriv:R_50".into(),
                    terms,
                    spot_units: 10000,
                    spot_time_micros: 9,
                    receipt_micros: 10,
                    schema: "synthetic".into(),
                    payload_sha256: "a".repeat(64),
                }),
            },
        },
        state: ClaimState::Claimed,
        contract_ref: None,
        transaction_ref: None,
    }
}

/// Every assertion here runs through both implementations without transport branches.
fn scenario(control: &mut dyn Control, mut advance: impl FnMut(i64), account: &str) {
    let key = key(account);
    let first = control
        .acquire(key, "first", "deployment", TTL)
        .unwrap()
        .unwrap();
    assert_eq!(first.token, 1);
    assert_eq!(first.expires_at_micros, first.server_now_micros + TTL);
    assert_eq!(
        control.acquire(key, "second", "deployment", TTL).unwrap(),
        None
    );
    assert_eq!(control.renew(key, "wrong", first.token, TTL).unwrap(), None);
    assert_eq!(
        control.renew(key, "first", first.token + 1, TTL).unwrap(),
        None
    );
    advance(1_000);
    let renewed = control
        .renew(key, "first", first.token, TTL)
        .unwrap()
        .unwrap();
    assert_eq!(renewed.token, first.token);
    assert!(renewed.server_now_micros >= first.server_now_micros + 1_000);
    assert_eq!(renewed.expires_at_micros, renewed.server_now_micros + TTL);
    advance(TTL + 1);
    assert_eq!(control.renew(key, "first", first.token, TTL).unwrap(), None);
    let second = control
        .acquire(key, "second", "deployment", TTL)
        .unwrap()
        .unwrap();
    assert_eq!(second.token, 2);
    assert!(second.server_now_micros >= renewed.expires_at_micros);
    assert_eq!(second.expires_at_micros, second.server_now_micros + TTL);
    assert!(!control.release(key, "first", first.token).unwrap());
    let original = claim(account, "command", second.token);
    assert_eq!(
        control
            .claim(key, "second", second.token, &original)
            .unwrap(),
        ClaimOutcome::Inserted
    );
    assert_eq!(
        control
            .claim(key, "second", second.token, &original)
            .unwrap(),
        ClaimOutcome::Replay(original.clone())
    );
    let stored = control.unresolved(key).unwrap();
    assert_eq!(stored[0].max_proposal_age_micros, 500);
    assert_eq!(stored, vec![original.clone()]);
    assert!(
        !control
            .update_claim(
                key,
                "first",
                first.token,
                "command",
                ClaimState::Accepted,
                Some("wrong"),
                None
            )
            .unwrap()
    );
    assert_eq!(control.unresolved(key).unwrap(), vec![original.clone()]);
    control.delete_reconciled(key, "command").unwrap();
    assert_eq!(control.unresolved(key).unwrap(), vec![original.clone()]);
    assert!(
        control
            .update_claim(
                key,
                "second",
                second.token,
                "command",
                ClaimState::Accepted,
                Some("contract"),
                Some("transaction")
            )
            .unwrap()
    );
    let mut accepted = original.clone();
    accepted.state = ClaimState::Accepted;
    accepted.contract_ref = Some("contract".into());
    accepted.transaction_ref = Some("transaction".into());
    assert_eq!(control.unresolved(key).unwrap(), vec![accepted.clone()]);
    assert!(control.release(key, "second", second.token).unwrap());
    assert_eq!(
        control.renew(key, "second", second.token, TTL).unwrap(),
        None
    );
    assert_eq!(
        control
            .claim(key, "second", second.token, &original)
            .unwrap(),
        ClaimOutcome::LeaseLost
    );
    assert!(
        !control
            .update_claim(
                key,
                "second",
                second.token,
                "command",
                ClaimState::Reconciled,
                None,
                None
            )
            .unwrap()
    );
    let third = control
        .acquire(key, "third", "deployment", TTL)
        .unwrap()
        .unwrap();
    assert_eq!(third.token, 3);
    assert_eq!(third.expires_at_micros, third.server_now_micros + TTL);
    let replay = claim(account, "command", third.token);
    assert_eq!(
        control.claim(key, "third", third.token, &replay).unwrap(),
        ClaimOutcome::Replay(accepted.clone())
    );
    assert!(
        control
            .update_claim(
                key,
                "third",
                third.token,
                "command",
                ClaimState::Reconciled,
                None,
                None
            )
            .unwrap()
    );
    accepted.state = ClaimState::Reconciled;
    assert_eq!(
        control.claim(key, "third", third.token, &replay).unwrap(),
        ClaimOutcome::Replay(accepted)
    );
    assert!(control.unresolved(key).unwrap().is_empty());
    control.delete_reconciled(key, "command").unwrap();
    control.delete_reconciled(key, "command").unwrap();
    assert_eq!(
        control.claim(key, "third", third.token, &replay).unwrap(),
        ClaimOutcome::Inserted
    );
    assert_eq!(control.unresolved(key).unwrap(), vec![replay]);
    assert!(control.release(key, "third", third.token).unwrap());
}

#[test]
fn fake_control_scenarios() {
    let mut control = FakeControl::new(1_000_000);
    let mut clock = control.clone();
    scenario(
        &mut control,
        |micros| clock.advance(micros),
        "shared-scenario",
    );
}

#[test]
fn fake_response_loss_and_partition() {
    let mut control = FakeControl::new(100);
    let key = key("faults");
    control.fault(Fault::Partition);
    assert_eq!(
        control.acquire(key, "owner", "deployment", TTL),
        Err("partition".into())
    );
    control.fault(Fault::LoseResponse);
    assert_eq!(
        control.acquire(key, "owner", "deployment", TTL),
        Err("response lost".into())
    );
    assert_eq!(
        control.acquire(key, "second", "deployment", TTL).unwrap(),
        None
    );
    control.advance(5);
    control.fault(Fault::LoseResponse);
    assert_eq!(
        control.renew(key, "owner", 1, TTL),
        Err("response lost".into())
    );
    control.advance(TTL - 4);
    assert_eq!(
        control.acquire(key, "second", "deployment", TTL).unwrap(),
        None
    );
    let value = claim("faults", "command", 1);
    control.fault(Fault::Partition);
    assert_eq!(
        control.claim(key, "owner", 1, &value),
        Err("partition".into())
    );
    assert!(control.unresolved(key).unwrap().is_empty());
    control.fault(Fault::LoseResponse);
    assert_eq!(
        control.claim(key, "owner", 1, &value),
        Err("response lost".into())
    );
    assert_eq!(control.unresolved(key).unwrap(), vec![value.clone()]);
    control.fault(Fault::Partition);
    assert_eq!(
        control.update_claim(
            key,
            "owner",
            1,
            "command",
            ClaimState::Accepted,
            Some("contract"),
            None
        ),
        Err("partition".into())
    );
    assert_eq!(control.unresolved(key).unwrap(), vec![value]);
    control.fault(Fault::LoseResponse);
    assert_eq!(
        control.release(key, "owner", 1),
        Err("response lost".into())
    );
    assert_eq!(control.renew(key, "owner", 1, TTL).unwrap(), None);
    let successor = control
        .acquire(key, "second", "deployment", TTL)
        .unwrap()
        .unwrap();
    assert_eq!(successor.token, 2);
    assert_eq!(successor.server_now_micros, 100 + 5 + TTL - 4);
    control.fault(Fault::Partition);
    assert_eq!(control.release(key, "second", 2), Err("partition".into()));
    assert!(control.renew(key, "second", 2, TTL).unwrap().is_some());
}

#[derive(Clone, Deserialize)]
struct Settings {
    host: String,
    port: u16,
    database: String,
    user: String,
    credential: String,
    root_certificate: PathBuf,
}
#[derive(Deserialize)]
struct TestConfig {
    control: Settings,
    wrong_root_certificate: PathBuf,
    wrong_host: String,
}
impl Settings {
    fn connect(&self) -> Result<Postgres, String> {
        Postgres::connect(
            &self.host,
            self.port,
            &self.database,
            &self.user,
            &self.credential,
            &self.root_certificate,
        )
    }
    // A test-only raw session controls real lock ordering and inspects durable SQL rows.
    fn raw(&self) -> (Runtime, Client) {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let mut roots = rustls::RootCertStore::empty();
        for certificate in CertificateDer::pem_file_iter(&self.root_certificate).unwrap() {
            roots.add(certificate.unwrap()).unwrap();
        }
        let tls = rustls::ClientConfig::builder_with_provider(Arc::new(
            rustls::crypto::aws_lc_rs::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_root_certificates(roots)
        .with_no_client_auth();
        let mut config = tokio_postgres::Config::new();
        config
            .host(&self.host)
            .port(self.port)
            .dbname(&self.database)
            .user(&self.user)
            .password(crate::broker::resolve_secret(&self.credential).unwrap())
            .ssl_mode(tokio_postgres::config::SslMode::Require);
        let client = runtime.block_on(async {
            let (client, connection) = config
                .connect(tokio_postgres_rustls::MakeRustlsConnect::new(tls))
                .await
                .unwrap();
            tokio::spawn(async move {
                let _ = connection.await;
            });
            client
        });
        (runtime, client)
    }
}

const TEST_LOCK_SQL: &str =
    "SELECT token FROM live_leases WHERE broker='deriv' AND account=$1 FOR UPDATE";
const TEST_RELEASE_SQL: &str = "WITH stamp AS (SELECT clock_timestamp() AS now) UPDATE live_leases SET expires_at=stamp.now, updated_at=stamp.now FROM stamp WHERE broker='deriv' AND account=$1 AND owner='owner' AND token=$2 RETURNING (EXTRACT(EPOCH FROM expires_at) * 1000000)::bigint, (EXTRACT(EPOCH FROM updated_at) * 1000000)::bigint";
const TEST_ROWS_SQL: &str = "SELECT payload::text, state, contract_ref, transaction_ref, token, proposal, request, claim, deployment, (EXTRACT(EPOCH FROM created_at) * 1000000)::bigint, (EXTRACT(EPOCH FROM updated_at) * 1000000)::bigint FROM live_dispatch_claims WHERE broker='deriv' AND account=$1 AND command=$2";
const TEST_SNAPSHOT_SQL: &str = "SELECT jsonb_build_object('leases', (SELECT COALESCE(jsonb_agg(to_jsonb(l) ORDER BY broker, account), '[]'::jsonb) FROM (SELECT broker, account, owner, deployment, token, (EXTRACT(EPOCH FROM acquired_at) * 1000000)::bigint AS acquired_at, (EXTRACT(EPOCH FROM expires_at) * 1000000)::bigint AS expires_at, (EXTRACT(EPOCH FROM updated_at) * 1000000)::bigint AS updated_at FROM live_leases) l), 'claims', (SELECT COALESCE(jsonb_agg(to_jsonb(c) ORDER BY broker, account, command), '[]'::jsonb) FROM (SELECT broker, account, command, claim, proposal, request, deployment, token, payload, state, contract_ref, transaction_ref, (EXTRACT(EPOCH FROM created_at) * 1000000)::bigint AS created_at, (EXTRACT(EPOCH FROM updated_at) * 1000000)::bigint AS updated_at FROM live_dispatch_claims) c), 'schema', obj_description('live_leases'::regclass))::text";

fn wait_for_lock(runtime: &Runtime, client: &Client, waiter: i32, blocker: i32) {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let blocked: bool = runtime
            .block_on(client.query_one(
                "SELECT $1::integer = ANY(pg_blocking_pids($2))",
                &[&blocker, &waiter],
            ))
            .unwrap()
            .get(0);
        if blocked {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "backend {waiter} never blocked behind {blocker}"
        );
        thread::sleep(Duration::from_millis(5));
    }
}
fn lock(runtime: &Runtime, client: &Client, account: &str) {
    runtime.block_on(client.batch_execute("BEGIN")).unwrap();
    let token: i64 = runtime
        .block_on(client.query_one(TEST_LOCK_SQL, &[&account]))
        .unwrap()
        .get(0);
    assert_eq!(token, 1);
}
fn release_locked(runtime: &Runtime, client: &Client, account: &str) {
    let row = runtime
        .block_on(client.query_one(TEST_RELEASE_SQL, &[&account, &1i64]))
        .unwrap();
    assert_eq!(row.get::<_, i64>(0), row.get::<_, i64>(1));
    runtime.block_on(client.batch_execute("COMMIT")).unwrap();
}
fn durable(runtime: &Runtime, client: &Client, account: &str, expected: &Claim) {
    let row = runtime
        .block_on(client.query_one(TEST_ROWS_SQL, &[&account, &expected.command]))
        .unwrap();
    let payload: Claim = serde_json::from_str(row.get(0)).unwrap();
    assert_eq!(
        payload,
        Claim {
            state: ClaimState::Claimed,
            contract_ref: None,
            transaction_ref: None,
            ..expected.clone()
        }
    );
    assert_eq!(row.get::<_, i64>(4), expected.token as i64);
    assert_eq!(row.get::<_, &str>(5), "proposal");
    assert_eq!(row.get::<_, &str>(6), "request");
    assert_eq!(row.get::<_, &str>(7), expected.claim);
    assert_eq!(row.get::<_, &str>(8), expected.deployment);
    assert!(row.get::<_, i64>(10) >= row.get::<_, i64>(9));
    assert_eq!(row.get::<_, Option<String>>(2), expected.contract_ref);
    assert_eq!(row.get::<_, Option<String>>(3), expected.transaction_ref);
    assert_eq!(
        serde_json::from_value::<ClaimState>(serde_json::Value::String(row.get(1))).unwrap(),
        expected.state
    );
}

pub fn postgres_control() {
    let path = std::env::var("BINARY_ALPHA_TEST_CONFIG")
        .unwrap_or_else(|error| panic!("unavailable: BINARY_ALPHA_TEST_CONFIG: {error}"));
    let bytes = std::fs::read(Path::new(&path)).unwrap_or_else(|error| {
        panic!("unavailable: cannot read BINARY_ALPHA_TEST_CONFIG: {error}")
    });
    let test: TestConfig = serde_json::from_slice(&bytes).unwrap();
    let settings = &test.control;
    let mut first = settings
        .connect()
        .unwrap_or_else(|error| panic!("unavailable: {error}"));
    first.migrate().unwrap();
    first.migrate().unwrap();
    let mut second = settings.connect().unwrap();
    let (runtime, client) = settings.raw();
    assert_eq!(
        runtime
            .block_on(client.query_one("SELECT obj_description('live_leases'::regclass)", &[]))
            .unwrap()
            .get::<_, String>(0),
        "binary-alpha live control schema v1"
    );
    assert_eq!(
        runtime
            .block_on(client.query_one(
                "SELECT obj_description('live_dispatch_claims'::regclass)",
                &[],
            ))
            .unwrap()
            .get::<_, String>(0),
        "binary-alpha live control schema v1"
    );
    let holder_pid: i32 = runtime
        .block_on(client.query_one("SELECT pg_backend_pid()", &[]))
        .unwrap()
        .get(0);
    let namespace = format!(
        "phase12-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    scenario(
        &mut first,
        |micros| thread::sleep(Duration::from_micros(micros as u64)),
        &format!("{namespace}-shared"),
    );

    // Both independent sessions attempt exactly one acquisition of the absent key.
    let account = format!("{namespace}-contention");
    let barrier = Arc::new(Barrier::new(3));
    let mut contenders = Vec::new();
    let (result_tx, result_rx) = mpsc::channel();
    for owner in ["first", "second"] {
        let mut session = settings.connect().unwrap();
        let account = account.clone();
        let barrier = barrier.clone();
        let result_tx = result_tx.clone();
        contenders.push(thread::spawn(move || {
            barrier.wait();
            result_tx
                .send(session.acquire(key(&account), owner, "deployment", TTL))
                .unwrap();
        }));
    }
    barrier.wait();
    let results: Vec<_> = (0..2)
        .map(|_| {
            result_rx
                .recv_timeout(Duration::from_secs(5))
                .expect("concurrent acquisition stalled")
                .unwrap()
        })
        .collect();
    for contender in contenders {
        contender.join().unwrap();
    }
    assert_eq!(results.iter().filter(|result| result.is_none()).count(), 1);
    let winners: Vec<_> = results.into_iter().flatten().collect();
    assert_eq!(winners.len(), 1);
    assert_eq!(winners[0].token, 1);

    // Validation fails after A obtains the lock; retain A idle while B renews.
    let account = format!("{namespace}-rollback");
    first
        .acquire(key(&account), "owner", "deployment", TTL * 5)
        .unwrap()
        .unwrap();
    let first_pid = first.backend_pid().unwrap();
    lock(&runtime, &client, &account);
    let account_copy = account.clone();
    let (failed_tx, failed_rx) = mpsc::channel();
    let invalid = thread::spawn(move || {
        let mut value = claim(&account_copy, "invalid", 1);
        value.claim.clear();
        let result = first.claim(key(&account_copy), "owner", 1, &value);
        failed_tx.send((first, result)).unwrap();
    });
    wait_for_lock(&runtime, &client, first_pid, holder_pid);
    runtime.block_on(client.batch_execute("COMMIT")).unwrap();
    let (idle, result) = failed_rx
        .recv_timeout(Duration::from_secs(5))
        .expect("invalid claim stalled");
    first = idle;
    invalid.join().unwrap();
    assert_eq!(result, Err("claim signal binding mismatch".into()));
    let account_copy = account.clone();
    let (renewed_tx, renewed_rx) = mpsc::channel();
    let renewal = thread::spawn(move || {
        let result = second.renew(key(&account_copy), "owner", 1, TTL);
        renewed_tx.send((second, result)).unwrap();
    });
    let (session, result) = renewed_rx
        .recv_timeout(Duration::from_secs(5))
        .expect("B's renewal blocked after A returned its validation error");
    second = session;
    renewal.join().unwrap();
    assert_eq!(result.unwrap().unwrap().token, 1);
    assert!(second.unresolved(key(&account)).unwrap().is_empty());

    // Renewal cannot revive a lease released ahead of it in row-lock order.
    let account = format!("{namespace}-renew-release");
    first
        .acquire(key(&account), "owner", "deployment", TTL)
        .unwrap()
        .unwrap();
    lock(&runtime, &client, &account);
    let mut session = settings.connect().unwrap();
    let waiter_pid = session.backend_pid().unwrap();
    let account_copy = account.clone();
    let renewal = thread::spawn(move || session.renew(key(&account_copy), "owner", 1, TTL));
    wait_for_lock(&runtime, &client, waiter_pid, holder_pid);
    release_locked(&runtime, &client, &account);
    assert_eq!(renewal.join().unwrap().unwrap(), None);

    // Release and takeover ahead of a waiting old dispatcher fence its claim.
    let account = format!("{namespace}-claim-release");
    first
        .acquire(key(&account), "owner", "deployment", TTL)
        .unwrap()
        .unwrap();
    lock(&runtime, &client, &account);
    let mut session = settings.connect().unwrap();
    let claimant_pid = session.backend_pid().unwrap();
    let account_copy = account.clone();
    let claimant = thread::spawn(move || {
        session.claim(
            key(&account_copy),
            "owner",
            1,
            &claim(&account_copy, "old", 1),
        )
    });
    wait_for_lock(&runtime, &client, claimant_pid, holder_pid);
    release_locked(&runtime, &client, &account);
    let successor = second
        .acquire(key(&account), "successor", "deployment", TTL)
        .unwrap()
        .unwrap();
    assert_eq!(successor.token, 2);
    assert_eq!(claimant.join().unwrap().unwrap(), ClaimOutcome::LeaseLost);
    assert!(second.unresolved(key(&account)).unwrap().is_empty());
    let value = claim(&account, "new", 2);
    assert_eq!(
        second.claim(key(&account), "successor", 2, &value).unwrap(),
        ClaimOutcome::Inserted
    );
    durable(&runtime, &client, &account, &value);
    assert!(
        !first
            .update_claim(
                key(&account),
                "owner",
                1,
                "new",
                ClaimState::Accepted,
                Some("stale"),
                None
            )
            .unwrap()
    );
    durable(&runtime, &client, &account, &value);

    // A claim that locks first commits before release; its full liability survives takeover.
    let account = format!("{namespace}-claim-first");
    first
        .acquire(key(&account), "owner", "deployment", TTL)
        .unwrap()
        .unwrap();
    lock(&runtime, &client, &account);
    let mut session = settings.connect().unwrap();
    let claimant_pid = session.backend_pid().unwrap();
    let account_copy = account.clone();
    let claimant = thread::spawn(move || {
        session.claim(
            key(&account_copy),
            "owner",
            1,
            &claim(&account_copy, "command", 1),
        )
    });
    wait_for_lock(&runtime, &client, claimant_pid, holder_pid);
    let mut session = settings.connect().unwrap();
    let releaser_pid = session.backend_pid().unwrap();
    let account_copy = account.clone();
    let releaser = thread::spawn(move || session.release(key(&account_copy), "owner", 1));
    // The exact release backend is queued behind this claim backend, not an unrelated session.
    wait_for_lock(&runtime, &client, releaser_pid, claimant_pid);
    runtime.block_on(client.batch_execute("COMMIT")).unwrap();
    assert_eq!(claimant.join().unwrap().unwrap(), ClaimOutcome::Inserted);
    assert!(releaser.join().unwrap().unwrap());
    assert_eq!(
        second
            .acquire(key(&account), "successor", "deployment", TTL)
            .unwrap()
            .unwrap()
            .token,
        2
    );
    durable(&runtime, &client, &account, &claim(&account, "command", 1));

    // Prove the claimant is blocked before expiry, then keep the holder past expiry.
    let account = format!("{namespace}-expiry-wait");
    let mut session = settings.connect().unwrap();
    let claimant_pid = session.backend_pid().unwrap();
    let value = claim(&account, "expired", 1);
    let lease = first
        .acquire(key(&account), "owner", "deployment", TTL)
        .unwrap()
        .unwrap();
    lock(&runtime, &client, &account);
    let account_copy = account.clone();
    let claimant = thread::spawn(move || session.claim(key(&account_copy), "owner", 1, &value));
    wait_for_lock(&runtime, &client, claimant_pid, holder_pid);
    let now: i64 = runtime
        .block_on(client.query_one(
            "SELECT (EXTRACT(EPOCH FROM clock_timestamp()) * 1000000)::bigint",
            &[],
        ))
        .unwrap()
        .get(0);
    assert!(
        now < lease.expires_at_micros,
        "claimant was not proved blocked before expiry"
    );
    thread::sleep(Duration::from_micros(
        (lease.expires_at_micros - now + 1_000) as u64,
    ));
    let now: i64 = runtime
        .block_on(client.query_one(
            "SELECT (EXTRACT(EPOCH FROM clock_timestamp()) * 1000000)::bigint",
            &[],
        ))
        .unwrap()
        .get(0);
    assert!(now > lease.expires_at_micros);
    wait_for_lock(&runtime, &client, claimant_pid, holder_pid);
    runtime.block_on(client.batch_execute("COMMIT")).unwrap();
    assert_eq!(claimant.join().unwrap().unwrap(), ClaimOutcome::LeaseLost);
    assert!(first.unresolved(key(&account)).unwrap().is_empty());

    // Discard successful commit responses, then read the exact durable claim on another session.
    let account = format!("{namespace}-lost-response");
    let lease = first
        .acquire(key(&account), "owner", "deployment", TTL)
        .unwrap()
        .unwrap();
    let mut value = claim(&account, "command", lease.token);
    let _ = first.claim(key(&account), "owner", lease.token, &value);
    assert_eq!(
        second.unresolved(key(&account)).unwrap(),
        vec![value.clone()]
    );
    durable(&runtime, &client, &account, &value);
    let _ = first.update_claim(
        key(&account),
        "owner",
        lease.token,
        "command",
        ClaimState::Accepted,
        Some("contract"),
        Some("transaction"),
    );
    value.state = ClaimState::Accepted;
    value.contract_ref = Some("contract".into());
    value.transaction_ref = Some("transaction".into());
    assert_eq!(
        second.unresolved(key(&account)).unwrap(),
        vec![value.clone()]
    );
    durable(&runtime, &client, &account, &value);

    // Ignore successful renewal/release responses and inspect their committed rows directly.
    let _ = first.renew(key(&account), "owner", lease.token, TTL);
    let row = runtime.block_on(client.query_one(
        "SELECT token, owner, deployment, (EXTRACT(EPOCH FROM expires_at) * 1000000)::bigint, (EXTRACT(EPOCH FROM updated_at) * 1000000)::bigint FROM live_leases WHERE broker='deriv' AND account=$1", &[&account],
    )).unwrap();
    assert_eq!(row.get::<_, i64>(0), lease.token as i64);
    assert_eq!(row.get::<_, &str>(1), "owner");
    assert_eq!(row.get::<_, &str>(2), "deployment");
    let renewed_expiry: i64 = row.get(3);
    let renewed_at: i64 = row.get(4);
    assert!(renewed_expiry > lease.expires_at_micros);
    assert_eq!(renewed_expiry, renewed_at + TTL);
    let _ = first.release(key(&account), "owner", lease.token);
    let row = runtime.block_on(client.query_one(
        "SELECT token, owner, deployment, (EXTRACT(EPOCH FROM expires_at) * 1000000)::bigint, (EXTRACT(EPOCH FROM updated_at) * 1000000)::bigint FROM live_leases WHERE broker='deriv' AND account=$1", &[&account],
    )).unwrap();
    assert_eq!(row.get::<_, i64>(0), lease.token as i64);
    assert_eq!(row.get::<_, &str>(1), "owner");
    assert_eq!(row.get::<_, &str>(2), "deployment");
    assert_eq!(row.get::<_, i64>(3), row.get::<_, i64>(4));
    assert!(row.get::<_, i64>(4) > renewed_at);
    assert!(
        second
            .renew(key(&account), "owner", lease.token, TTL)
            .unwrap()
            .is_none()
    );

    let before: String = runtime
        .block_on(client.query_one(TEST_SNAPSHOT_SQL, &[]))
        .unwrap()
        .get(0);
    let mut wrong = settings.clone();
    wrong.root_certificate = test.wrong_root_certificate;
    let error = wrong
        .connect()
        .err()
        .expect("wrong root must fail TLS verification");
    assert!(
        error.contains("invalid peer certificate") && error.contains("UnknownIssuer"),
        "{error}"
    );
    let mut wrong = settings.clone();
    wrong.host = test.wrong_host;
    let error = wrong
        .connect()
        .err()
        .expect("wrong host must fail TLS verification");
    assert!(
        error.contains("invalid peer certificate")
            && error.contains("certificate not valid for name \"127.0.0.1\""),
        "{error}"
    );
    let after: String = runtime
        .block_on(client.query_one(TEST_SNAPSHOT_SQL, &[]))
        .unwrap()
        .get(0);
    assert_eq!(before, after, "failed TLS connections mutated control rows");

    // Runtime recovery half: two actual Postgres sessions, the same checkpoint and
    // statement scenarios as the ordinary fake-control gate, and recorded broker I/O.
    for (name, point) in [
        ("claimed", crate::live::Checkpoint::AfterClaimBeforeWrite),
        ("during", crate::live::Checkpoint::DuringWrite),
        (
            "written",
            crate::live::Checkpoint::AfterWriteBeforeAcknowledgement,
        ),
        (
            "acknowledged",
            crate::live::Checkpoint::AfterAcknowledgement,
        ),
    ] {
        let account = format!("{namespace}-runtime-{name}");
        let mut fixture = super::support::Fixture::for_account(&format!("t1-pg-{name}"), &account);
        fixture
            .config
            .live
            .as_mut()
            .unwrap()
            .journal
            .segment_records = 1024;
        let first = RecoverySession(Arc::new(std::sync::Mutex::new(settings.connect().unwrap())));
        let second = RecoverySession(Arc::new(std::sync::Mutex::new(settings.connect().unwrap())));
        let mut inspect = second.clone();
        let original = super::faults::checkpoint_recovery_scenario(
            &fixture,
            Box::new(first.clone()),
            Box::new(second.clone()),
            &mut inspect,
            point,
        );
        let row = runtime
            .block_on(client.query_one(TEST_ROWS_SQL, &[&account, &original.command]))
            .unwrap();
        let payload: Claim = serde_json::from_str(row.get(0)).unwrap();
        assert_eq!(
            payload,
            Claim {
                state: ClaimState::Claimed,
                contract_ref: None,
                transaction_ref: None,
                ..original.clone()
            }
        );
        assert_eq!(row.get::<_, &str>(1), "possibly_sent");
        assert_eq!(row.get::<_, Option<String>>(2), original.contract_ref);
        assert_eq!(row.get::<_, Option<String>>(3), original.transaction_ref);
        assert_eq!(row.get::<_, i64>(4), original.token as i64);
        let EventKind::Signal {
            proposal: Some(proposal),
            ..
        } = &original.signal.kind
        else {
            unreachable!()
        };
        assert_eq!(row.get::<_, &str>(5), proposal.identity);
        assert_eq!(row.get::<_, &str>(6), proposal.request_identity);
        assert_eq!(row.get::<_, &str>(7), original.claim);
        assert_eq!(row.get::<_, &str>(8), original.deployment);
        assert!(row.get::<_, i64>(10) >= row.get::<_, i64>(9));
        super::faults::statement_recovery_scenario(
            &fixture,
            Box::new(first),
            &mut inspect,
            &original,
        );
        assert!(
            runtime
                .block_on(client.query(TEST_ROWS_SQL, &[&account, &original.command]))
                .unwrap()
                .is_empty()
        );
    }
}

/// Shares access to one actual session so runtime ownership and the independent
/// readback use exactly the two database sessions in each recovery scenario.
#[derive(Clone)]
struct RecoverySession(Arc<std::sync::Mutex<Postgres>>);
impl Control for RecoverySession {
    fn acquire(
        &mut self,
        key: LeaseKey<'_>,
        owner: &str,
        deployment: &str,
        ttl: i64,
    ) -> Result<Option<crate::live::control::Lease>, String> {
        self.0.lock().unwrap().acquire(key, owner, deployment, ttl)
    }
    fn renew(
        &mut self,
        key: LeaseKey<'_>,
        owner: &str,
        token: u64,
        ttl: i64,
    ) -> Result<Option<crate::live::control::Lease>, String> {
        self.0.lock().unwrap().renew(key, owner, token, ttl)
    }
    fn release(&mut self, key: LeaseKey<'_>, owner: &str, token: u64) -> Result<bool, String> {
        self.0.lock().unwrap().release(key, owner, token)
    }
    fn claim(
        &mut self,
        key: LeaseKey<'_>,
        owner: &str,
        token: u64,
        claim: &Claim,
    ) -> Result<ClaimOutcome, String> {
        self.0.lock().unwrap().claim(key, owner, token, claim)
    }
    fn update_claim(
        &mut self,
        key: LeaseKey<'_>,
        owner: &str,
        token: u64,
        claim: &str,
        state: ClaimState,
        contract: Option<&str>,
        transaction: Option<&str>,
    ) -> Result<bool, String> {
        self.0
            .lock()
            .unwrap()
            .update_claim(key, owner, token, claim, state, contract, transaction)
    }
    fn unresolved(&mut self, key: LeaseKey<'_>) -> Result<Vec<Claim>, String> {
        self.0.lock().unwrap().unresolved(key)
    }
    fn delete_reconciled(&mut self, key: LeaseKey<'_>, claim: &str) -> Result<(), String> {
        self.0.lock().unwrap().delete_reconciled(key, claim)
    }
}

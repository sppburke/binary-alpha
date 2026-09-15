//! One ordered feature, execution, and journal owner with independent I/O workers.
//! Restart rebuilds causal state from verified warm-up history and subsequent live ticks
//! under the feature stream's existing gap rule. Every restart and break requires a ready
//! base row for every bound instrument before entries resume; ticks are not journaled.
//!
//! An ambiguous predecessor dispatch never becomes NotSent merely because time passed or
//! portfolio/statement queries found nothing. After confirming the predecessor cannot write,
//! the operator may set its durable row to `not_sent`, or to `accepted` with broker references.
//! The owner reads `unresolved()` on the reconciliation cadence: `not_sent` releases through
//! Engine reconciliation; `accepted` needs matching broker purchase evidence. Several matches
//! remain possibly sent. Successor entries remain vetoed until ambiguity is resolved.
pub mod authorization;
pub mod control;
pub mod journal;
mod owner;
pub mod receipt;
mod workers;
pub use workers::{Ingress, Intent, Reply, ReplyTiming, ReplyValue};
use workers::{Storage, Workers};

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::time::{Duration, Instant};

use binary_alpha_engine::config::{AccountClass, Broker, Config, Live, RunMode};
use binary_alpha_engine::dataset::{DatasetRole, manifest_key};
use binary_alpha_engine::execution::{
    AccountState, BrokerLiability, CashAction, Decimal, Engine, EventKind, EventSource,
    FinancialEvent, Observation, Outcome, Proposal, REPLAY_SCHEMA_VERSION_BROKER, ReplayInput,
    ReplayManifest, Resolution, RunDefinition, TerminalStatus, replay_generation_id,
};
use binary_alpha_engine::features::{FeatureEngine, FeatureOutput, FeaturePlan};
use binary_alpha_engine::market::{InstrumentId, PriceScale, Tick, parse_event_time_micros};
use binary_alpha_engine::research::{
    self as policy, Access, CertificationManifest, Frozen, LivePolicy, RUN_OBJECT_PATH, Run,
    RunManifest, Window, digest,
};
use binary_alpha_engine::stream::{Observation as StreamObservation, Source};
use serde::{Deserialize, Serialize};

use crate::broker::deriv::{
    DerivAccounts, DerivMarketData, DerivOptions, Encoded, StatementRow, purchase_observation,
    recover_purchase, to_observation,
};
use crate::broker::transport::{RecordedConnector, ReplayClock, WebSocketConnector};
use crate::broker::{
    AccountEvent, AccountIdentity, Clock, Continuity, LiveEvent, MarketDataBroker, OpenContract,
    PreparedPurchase, ProposalRequest, PurchaseOutcome, SystemClock,
};
use crate::import::CODE_REVISION;
use crate::research::{publish_record, read_key, ready_uri};
use crate::store::{self, Store};
use control::{Claim, ClaimOutcome, ClaimState, Control, FakeControl, Lease, LeaseKey, Postgres};
use journal::{Journal, LeaseState, Record, RecordKind};

pub const DEPLOYMENT_KIND: &str = "live_deployment";
pub const DEPLOYMENT_SCHEMA_VERSION: u32 = 1;
const POLL_MICROS: i64 = 10_000;
const AVAILABILITY: &str = "ordered_broker_receipts_v1";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeploymentManifest {
    pub kind: String,
    pub schema_version: u32,
    pub execution_contract: String,
    pub research: String,
    pub bundle_sha256: String,
    pub frozen: String,
    pub selection: String,
    pub policy: String,
    pub certification: String,
    pub definition: String,
    pub config_hash: String,
    pub code_revision: String,
    pub broker: String,
    pub account: String,
    pub hash: String,
}
impl DeploymentManifest {
    pub fn content_hash(&self) -> String {
        let mut value = serde_json::to_value(self).expect("deployment serializes");
        value.as_object_mut().expect("object").remove("hash");
        digest(
            b"binary-alpha live deployment v1\n",
            &serde_json::to_vec(&value).expect("object serializes"),
        )
    }
    pub fn key(&self) -> String {
        format!("live/deployments/{}.json", self.hash)
    }
    pub fn to_json(&self) -> Vec<u8> {
        serde_json::to_vec(self).expect("deployment serializes")
    }
    pub fn from_json(bytes: &[u8]) -> Result<Self, String> {
        let value: Self = serde_json::from_slice(bytes).map_err(|error| error.to_string())?;
        if value.kind != DEPLOYMENT_KIND
            || value.schema_version != DEPLOYMENT_SCHEMA_VERSION
            || value.execution_contract != policy::EXECUTION_CONTRACT_V1
            || value.hash != value.content_hash()
        {
            return Err(
                "live deployment: kind, schema, execution contract, or hash mismatch".into(),
            );
        }
        Ok(value)
    }
}

pub struct LiveDefinition {
    pub policy: LivePolicy,
    pub definition: RunDefinition,
    pub deployment: String,
    pub manifest: DeploymentManifest,
    pub scenarios: Vec<String>,
    plans: Vec<FeaturePlan>,
}

/// Reads the verified research run, frozen selection, and public certification envelope.
pub fn definition(
    config: &Config,
    _local: &Store,
    _destination: &Store,
) -> Result<LiveDefinition, String> {
    let settings = config.live.as_ref().ok_or("live: the table is required")?;
    let uri = settings.bundle_manifest.to_string();
    let (source, key) = crate::verify::open(&uri)?;
    let bytes = read_key(&source, &key)?;
    crate::research::verify_run(&uri, &source, &key, &bytes, Access::ORDINARY)?;
    let manifest = RunManifest::from_json(&bytes)?;
    let run = Run::from_json(&crate::search::read_object(
        &source,
        &manifest.objects,
        RUN_OBJECT_PATH,
    )?)?;
    let frozen = Frozen::from_json(&read_key(
        &source,
        &policy::frozen_key(&manifest.generation),
    )?)?;
    let selection_key = manifest_key(&run.selection);
    let (_, selection, _) = crate::portfolio::verified_selection(
        &source.uri(&selection_key),
        &source,
        &selection_key,
        &read_key(&source, &selection_key)?,
        Access::ORDINARY,
    )?;
    let uri = settings.certification_manifest.to_string();
    let (cert_store, cert_key) = crate::verify::open(&uri)?;
    let cert_bytes = read_key(&cert_store, &cert_key)?;
    crate::research::verify_certification(
        &uri,
        &cert_store,
        &cert_key,
        &cert_bytes,
        Access::ORDINARY,
    )?;
    let certification = CertificationManifest::from_json(&cert_bytes)?;
    if selection.refit.len() != frozen.instruments.len()
        || settings.warmup.len() != frozen.instruments.len()
    {
        return Err("live: exactly one refit and warmup per frozen instrument is required".into());
    }
    let inputs = frozen
        .instruments
        .iter()
        .zip(&selection.refit)
        .map(|(instrument, fit)| {
            if fit.instrument != instrument.instrument {
                return Err("live: refit instrument order mismatch".into());
            }
            Ok(ReplayInput {
                tick_manifest: ready_uri(&source, &instrument.source)?,
                feature_manifest: ready_uri(&source, &fit.generation)?,
                outcome_manifest: None,
            })
        })
        .collect::<Result<Vec<_>, String>>()?;
    let policy = policy::live_policy(
        &manifest,
        &run,
        &frozen,
        &selection,
        &certification,
        &settings.broker,
        &settings.account,
        (
            &settings.compatibility.observation_start,
            &settings.compatibility.observation_end,
        ),
        inputs,
    )?;
    if !matches!(
        config.brokers.iter().find(|b| b.id() == &settings.broker),
        Some(Broker::Deriv(_))
    ) {
        return Err("live: only the current Deriv options adapter is supported".into());
    }
    let bound = (0..policy.replay.inputs.len())
        .map(|i| crate::replay::bind_instrument(&policy.replay, i, Access::ORDINARY))
        .collect::<Result<Vec<_>, _>>()?;
    for (index, instrument) in bound.iter().enumerate() {
        if instrument.binding.plan_identity != policy.refit[index].plan_identity {
            return Err("live: refit plan identity mismatch".into());
        }
        let declared = config
            .instrument(
                &InstrumentId {
                    broker: instrument.binding.broker.clone(),
                    provider_symbol: instrument.binding.provider_symbol.clone(),
                },
                binary_alpha_engine::dataset::NativeGranularity::Tick,
            )
            .ok_or("live: frozen instrument is not configured")?;
        if declared.price_scale != instrument.inputs.scale
            || declared.quote_currency != policy.replay.accounts[0].currency
            || declared != &instrument.inputs.plan.profile.definition
        {
            return Err("live: configured instrument currency, scale, or feature definition differs from the frozen refit".into());
        }
    }
    let definition = RunDefinition {
        schema_version: REPLAY_SCHEMA_VERSION_BROKER,
        config_hash: config.content_hash(),
        code_revision: CODE_REVISION.into(),
        availability: AVAILABILITY.into(),
        replay: policy.replay.clone(),
        instruments: bound.iter().map(|b| b.binding.clone()).collect(),
    };
    Engine::new(definition.clone())?;
    let source = &policy.source;
    let mut manifest = DeploymentManifest {
        kind: DEPLOYMENT_KIND.into(),
        schema_version: DEPLOYMENT_SCHEMA_VERSION,
        execution_contract: policy::EXECUTION_CONTRACT_V1.into(),
        research: source.research.clone(),
        bundle_sha256: source.bundle_sha256.clone(),
        frozen: source.frozen.clone(),
        selection: source.selection.clone(),
        policy: source.policy.clone(),
        certification: source.certification.clone(),
        definition: replay_generation_id(&definition.config_hash, &definition.instruments),
        config_hash: definition.config_hash.clone(),
        code_revision: definition.code_revision.clone(),
        broker: settings.broker.to_string(),
        account: settings.account.clone(),
        hash: String::new(),
    };
    manifest.hash = manifest.content_hash();
    let mut scenarios = frozen.descriptor.scenarios.clone();
    for binding in &selection
        .frozen
        .as_ref()
        .expect("live policy validated")
        .bindings
    {
        scenarios.push(digest(
            b"",
            &serde_json::to_vec(&binding.envelope).map_err(|error| error.to_string())?,
        ));
    }
    for scenario in &run
        .config
        .research
        .as_ref()
        .expect("verified research")
        .scenarios
    {
        for alternative in &scenario.alternatives {
            scenarios.push(digest(
                b"",
                &serde_json::to_vec(&alternative.envelope).map_err(|error| error.to_string())?,
            ));
        }
    }
    Ok(LiveDefinition {
        policy,
        definition,
        deployment: manifest.hash.clone(),
        manifest,
        scenarios,
        plans: bound.into_iter().map(|b| b.inputs.plan).collect(),
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Mode {
    Replay,
    Paper,
    Live,
}
impl Mode {
    fn text(self) -> &'static str {
        match self {
            Self::Replay => "replay",
            Self::Paper => "paper",
            Self::Live => "live",
        }
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "state", content = "reason", rename_all = "snake_case")]
pub enum Entries {
    Enabled,
    Disabled(String),
}
#[derive(Debug, Clone, Serialize)]
pub struct Health {
    pub bundle_sha256: String,
    pub lease_owner: String,
    pub fencing_token: u64,
    pub lease_deadline_micros: i64,
    pub broker: String,
    pub account: String,
    pub account_class: AccountClass,
    pub connection_generation: u64,
    pub receipt_sequence: u64,
    pub last_event_age_micros: Option<i64>,
    pub warmup: bool,
    pub journal_sequence: u64,
    pub open_commands: u32,
    pub uncertain_commands: u64,
    pub cloud_pending_segments: usize,
    pub cloud_failed_segments: usize,
    pub pending_rows: usize,
    pub pending_proposals: usize,
    pub balance_reconciled: bool,
    pub entries: Entries,
    pub risk: AccountState,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Checkpoint {
    BeforeClaim,
    AfterClaimBeforeWrite,
    DuringWrite,
    AfterWriteBeforeAcknowledgement,
    AfterAcknowledgement,
    BeforeUpload,
    AfterUploadVerification,
    BeforeClaimDeletion,
    AfterClaimDeletion,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FinalManifest {
    pub deployment: String,
    pub ledger: String,
    pub ledger_generation: String,
    pub receipt: String,
    pub journal_segments: Vec<receipt::Segment>,
    pub open_tail: Option<journal::OpenTail>,
    pub measurements: BTreeMap<String, Measurements>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Measurements {
    pub market_event_to_decision_micros: Option<i64>,
    pub claim_to_socket_write_micros: Option<i64>,
    pub decision_to_acceptance_micros: Option<i64>,
}
struct PendingRows {
    instrument: usize,
    close_micros: i64,
    bindings: BTreeSet<String>,
    observations: Vec<Observation>,
    receipt_micros: i64,
}
struct Dispatch {
    signal: FinancialEvent,
    prepared: PreparedPurchase,
    claim_at: Option<i64>,
}

/// All financial mutation stays on the caller's thread. A hook returning true simulates loss.
pub struct Runtime {
    pub definition: LiveDefinition,
    pub hook: Option<Box<dyn FnMut(Checkpoint) -> bool>>,
    engine: Engine,
    features: Vec<FeatureEngine>,
    journal: Journal,
    records: Vec<Record>,
    control: Box<dyn Control>,
    workers: Workers,
    clock: Box<dyn Clock>,
    scheduler: Option<ReplayClock>,
    destination_uri: String,
    settings: Live,
    dir: PathBuf,
    mode: Mode,
    health: Health,
    vetoes: BTreeSet<String>,
    warm: BTreeSet<usize>,
    pending_rows: VecDeque<PendingRows>,
    proposals_pending: BTreeSet<String>,
    dispatches: BTreeMap<String, Dispatch>,
    dispatch_order: VecDeque<String>,
    measurements: BTreeMap<String, Measurements>,
    decision_receipt: Option<i64>,
    due: BTreeMap<String, (usize, i64)>,
    uploads: BTreeSet<String>,
    upload_errors: BTreeMap<String, String>,
    authorization_pending: bool,
    balance_pending: bool,
    failure: Option<String>,
    recovery_open: Option<Vec<OpenContract>>,
    recovery_pending: bool,
    claims_restored: bool,
    ledger_result: Option<Result<ReplayManifest, String>>,
    publication_result: Option<Result<String, String>>,
    last_provider_time: Option<i64>,
    control_time: i64,
    continuity: Continuity,
    contracts: BTreeMap<String, String>,
    claims: BTreeMap<String, Claim>,
    subscribed: BTreeSet<String>,
    subscriptions_pending: BTreeSet<String>,
    deadline: i64,
    renewal_stop: Arc<AtomicBool>,
    renewal: Option<std::thread::JoinHandle<()>>,
    monotonic: Option<(Instant, i64)>,
    last_renewal: i64,
    interrupted: bool,
    draining: bool,
    replay_prefix: Vec<(i64, RecordKind)>,
    published_health: Option<Vec<u8>>,
    health_at: i64,
    prefix_at: usize,
    segments: Vec<receipt::Segment>,
}
impl Runtime {
    /// Starts with injected broker/control/clock owners. Paper observes the account too.
    #[allow(clippy::too_many_arguments)]
    pub fn start(
        config: &Config,
        base: &Path,
        definition: LiveDefinition,
        local: Store,
        destination: Store,
        control: Box<dyn Control>,
        market: Box<dyn MarketDataBroker>,
        options: DerivOptions,
        clock: Box<dyn Clock>,
        scheduler: Option<ReplayClock>,
        mode: Mode,
    ) -> Result<Self, String> {
        Self::start_at(
            config,
            base,
            definition,
            local,
            destination,
            control,
            market,
            options,
            clock,
            scheduler,
            mode,
            None,
        )
    }
    #[allow(clippy::too_many_arguments)]
    fn start_at(
        config: &Config,
        base: &Path,
        definition: LiveDefinition,
        local: Store,
        destination: Store,
        control: Box<dyn Control>,
        market: Box<dyn MarketDataBroker>,
        options: DerivOptions,
        clock: Box<dyn Clock>,
        scheduler: Option<ReplayClock>,
        mode: Mode,
        monotonic: Option<(Instant, i64)>,
    ) -> Result<Self, String> {
        let settings = config.live.clone().ok_or("live: the table is required")?;
        let account = &definition.definition.replay.accounts[0];
        if options.account().broker != account.broker
            || options.account().account != account.id
            || options.account().currency != account.currency
        {
            return Err("live: broker account binding mismatch".into());
        }
        let dir = base.join(&settings.journal.dir);
        let mut features = Vec::new();
        for (index, uri) in settings.warmup.iter().enumerate() {
            let (store, manifest, scale) = crate::outcomes::bind_tick(
                &format!("live.warmup[{index}]"),
                DatasetRole::Development,
                uri,
                "live warmup",
                Access::ORDINARY,
            )?;
            if manifest.instrument != definition.definition.instruments[index].instrument
                || scale != definition.plans[index].price_scale
            {
                return Err("live: warmup instrument or scale mismatch".into());
            }
            if parse_event_time_micros(&manifest.coverage.last_event_time)?
                >= parse_event_time_micros(&settings.compatibility.observation_start)?
            {
                return Err("live: warmup must precede the observation window".into());
            }
            let mut engine =
                FeatureEngine::new(&definition.plans[index], Source::from_manifest(&manifest))?;
            let mut produced = FeatureOutput::default();
            crate::audit::feed_generation(&store, &manifest, scale, &mut |observation| {
                engine
                    .push(observation, &mut produced)
                    .map_err(|error| error.to_string())?;
                produced.clear();
                Ok(())
            })?;
            if engine.profile().observations != manifest.row_count
                || engine.profile().coverage.as_ref() != Some(&manifest.coverage)
            {
                return Err("live: warmup coverage or row count mismatch".into());
            }
            features.push(engine);
        }
        publish_record(
            &local,
            &destination,
            &definition.manifest.key(),
            &definition.manifest.to_json(),
        )?;
        Journal::restore(
            &dir,
            &definition.deployment,
            u64::from(settings.journal.segment_records),
            &mut |key| {
                let Some(head) = destination.head(key)? else {
                    return Ok(None);
                };
                let bytes = read_key(&destination, key)?;
                if bytes.len() as u64 != head.bytes {
                    return Err("live: cloud journal length mismatch".into());
                }
                Ok(Some(bytes))
            },
        )?;
        let (journal, records) = Journal::open(
            &dir,
            &definition.deployment,
            u64::from(settings.journal.segment_records),
        )?;
        let ledger: Vec<_> = records
            .iter()
            .filter_map(|record| {
                if let RecordKind::Ledger { event } = &record.kind {
                    Some(event.clone())
                } else {
                    None
                }
            })
            .collect();
        let restored = if records.is_empty() {
            None
        } else {
            match &records[0].kind {
                RecordKind::Started {
                    config_hash,
                    definition: identity,
                    ..
                } if *config_hash == definition.manifest.config_hash
                    && *identity == definition.manifest.definition => {}
                _ => {
                    return Err("live: journal Started configuration or definition mismatch".into());
                }
            }
            let engine = if ledger.is_empty() {
                if records.iter().any(|record| {
                    !matches!(
                        record.kind,
                        RecordKind::Started { .. } | RecordKind::Discontinuity { .. }
                    )
                }) {
                    return Err("live: journal has financial records without a ledger".into());
                }
                Engine::new(definition.definition.clone())?
            } else {
                Engine::restore(ledger.iter().map(|event| Ok(event.to_line())))?
            };
            if engine.definition() != &definition.definition {
                return Err("live: restored definition mismatch".into());
            }
            Some(engine)
        };
        let engine = match restored {
            Some(engine) if mode != Mode::Replay => engine,
            _ => Engine::new(definition.definition.clone())?,
        };
        let mut segments = Vec::new();
        for entry in fs::read_dir(&dir).map_err(|e| e.to_string())? {
            let path = entry.map_err(|e| e.to_string())?.path();
            if let Some(name) = path
                .file_name()
                .and_then(|n| n.to_str())
                .and_then(|n| n.strip_suffix(".uploaded"))
            {
                let identity = store::identify(&path)?;
                segments.push(receipt::Segment {
                    key: journal.key(name)?,
                    sha256: identity.sha256,
                    bytes: identity.bytes,
                });
            }
        }
        let now = clock.now_micros();
        let account_class = options.account().class;
        if let Some(scheduler) = &scheduler {
            scheduler.complete();
        }
        let destination_uri = format!("{}/", destination.uri("").trim_end_matches('/'));
        let workers = Workers::start(market, options, scheduler.clone(), local, destination);
        let mut runtime = Self {
            health: Health {
                bundle_sha256: definition.manifest.bundle_sha256.clone(),
                lease_owner: settings.control.owner.clone(),
                fencing_token: 0,
                lease_deadline_micros: 0,
                broker: settings.broker.to_string(),
                account: settings.account.clone(),
                account_class,
                connection_generation: 0,
                receipt_sequence: 0,
                last_event_age_micros: None,
                warmup: false,
                journal_sequence: journal.next_sequence() - 1,
                open_commands: 0,
                uncertain_commands: 0,
                cloud_pending_segments: 0,
                cloud_failed_segments: 0,
                pending_rows: 0,
                pending_proposals: 0,
                balance_reconciled: false,
                entries: Entries::Enabled,
                risk: engine.accounts()[0].clone(),
            },
            replay_prefix: if mode == Mode::Replay {
                records
                    .iter()
                    .filter(|r| {
                        matches!(
                            r.kind,
                            RecordKind::Ledger { .. }
                                | RecordKind::Refused { .. }
                                | RecordKind::DueTick { .. }
                        )
                    })
                    .map(|r| (r.time_micros, r.kind.clone()))
                    .collect()
            } else {
                Vec::new()
            },
            definition,
            hook: None,
            engine,
            features,
            journal,
            records,
            control,
            workers,
            clock,
            scheduler,
            destination_uri,
            settings,
            dir,
            mode,
            vetoes: BTreeSet::new(),
            warm: BTreeSet::new(),
            pending_rows: VecDeque::new(),
            proposals_pending: BTreeSet::new(),
            dispatches: BTreeMap::new(),
            dispatch_order: VecDeque::new(),
            measurements: BTreeMap::new(),
            decision_receipt: None,
            due: BTreeMap::new(),
            uploads: BTreeSet::new(),
            upload_errors: BTreeMap::new(),
            authorization_pending: false,
            balance_pending: true,
            failure: None,
            recovery_open: None,
            recovery_pending: false,
            claims_restored: mode == Mode::Replay,
            ledger_result: None,
            publication_result: None,
            last_provider_time: None,
            control_time: i64::MIN,
            continuity: Continuity::default(),
            contracts: BTreeMap::new(),
            claims: BTreeMap::new(),
            subscribed: BTreeSet::new(),
            subscriptions_pending: BTreeSet::new(),
            deadline: 0,
            renewal_stop: Arc::new(AtomicBool::new(false)),
            renewal: None,
            monotonic,
            last_renewal: now,
            interrupted: false,
            draining: false,
            published_health: None,
            health_at: now,
            prefix_at: 0,
            segments,
        };
        if monotonic.is_none() {
            runtime.workers.sender.take();
        }
        if mode != Mode::Replay {
            runtime.restore_due();
            runtime.compatibility()?;
        }
        if runtime.records.is_empty() {
            runtime.record(RecordKind::Started {
                config_hash: runtime.definition.manifest.config_hash.clone(),
                definition: runtime.definition.manifest.definition.clone(),
                code_revision: CODE_REVISION.into(),
            })?;
        } else {
            runtime.record(RecordKind::Discontinuity {
                reason: "restart: restore financial state and rebuild causal history".into(),
            })?;
        }
        runtime.veto("causal warmup is incomplete", true);
        runtime.veto(
            "broker balance differs from assessed or restored cash",
            true,
        );
        runtime.drain()?;
        let before = runtime.local_now();
        runtime.control.advance_to(runtime.clock.now_micros());
        let acquired = runtime.control.acquire(
            LeaseKey {
                broker: &runtime.health.broker,
                account: &runtime.health.account,
            },
            &runtime.settings.control.owner,
            &runtime.definition.deployment,
            runtime.settings.control.lease_ttl_micros,
        );
        match acquired {
            Ok(Some(lease)) => {
                runtime.set_lease(before, lease)?;
                runtime.record(RecordKind::Lease {
                    state: LeaseState::Acquired,
                    token: runtime.health.fencing_token,
                })?;
            }
            Ok(None) => runtime.disable("account lease is held by another owner"),
            Err(error) => runtime.disable(&format!("lease unavailable: {error}")),
        }
        if mode != Mode::Replay {
            runtime.recover()?;
        }
        runtime.send(Intent::Transactions)?;
        runtime.send(Intent::Balance)?;
        for contract in runtime.contracts.clone().into_keys() {
            runtime.subscribe_contract(&contract)?;
        }
        if mode == Mode::Live {
            runtime.veto("live authorization is absent", true);
        }
        runtime.check_authorization()?;
        // Startup account and authorization results enter the same ordered receiver.
        while (runtime.balance_pending || runtime.authorization_pending)
            && runtime.failure.is_none()
        {
            let generation = runtime.scheduler.as_ref().map(ReplayClock::generation);
            runtime.receive()?;
            if runtime.balance_pending || runtime.authorization_pending {
                runtime.check_progress()?;
                runtime.check_replay_progress(generation)?;
            }
        }
        for (id, scale) in runtime.instruments()? {
            if let Some(scheduler) = &runtime.scheduler {
                scheduler.wake("market");
            }
            runtime
                .workers
                .market
                .send(Intent::Subscribe(id, scale))
                .map_err(|e| e.to_string())?;
        }
        runtime.write_health()?;
        Ok(runtime)
    }
    pub fn health(&self) -> &Health {
        &self.health
    }
    pub fn engine(&self) -> &Engine {
        &self.engine
    }
    pub fn features(&self) -> &[FeatureEngine] {
        &self.features
    }
    pub fn records(&self) -> &[Record] {
        &self.records
    }
    pub fn interrupted(&self) -> bool {
        self.interrupted
    }
    fn local_now(&self) -> i64 {
        self.monotonic.map_or_else(
            || self.clock.now_micros(),
            |(instant, epoch)| epoch.saturating_add(instant.elapsed().as_micros() as i64),
        )
    }
    fn veto(&mut self, reason: &str, present: bool) {
        if present {
            self.vetoes.insert(reason.into());
        } else {
            self.vetoes.remove(reason);
        }
        self.health.entries = if self.vetoes.is_empty() {
            Entries::Enabled
        } else {
            Entries::Disabled(self.vetoes.iter().cloned().collect::<Vec<_>>().join("; "))
        };
    }
    fn disable(&mut self, reason: &str) {
        self.veto(reason, true);
    }
    fn send(&self, intent: Intent) -> Result<(), String> {
        if self.draining {
            return Ok(());
        }
        if let Some(scheduler) = &self.scheduler {
            scheduler.wake("account");
        }
        self.workers
            .account
            .send(intent)
            .map_err(|e| e.to_string())?;
        Ok(())
    }
    fn record(&mut self, kind: RecordKind) -> Result<(), String> {
        if matches!(
            kind,
            RecordKind::Ledger { .. } | RecordKind::Refused { .. } | RecordKind::DueTick { .. }
        ) && let Some((at, expected)) = self.replay_prefix.get(self.prefix_at)
        {
            if *expected != kind
                || (!matches!(kind, RecordKind::Ledger { .. }) && *at != self.clock.now_micros())
            {
                return Err(format!(
                    "live replay: journal evidence diverges at prefix {}",
                    self.prefix_at
                ));
            }
            self.prefix_at += 1;
            return Ok(());
        }
        self.records
            .push(self.journal.append(self.clock.now_micros(), kind)?);
        Ok(())
    }
    fn checkpoint(&mut self, point: Checkpoint) -> bool {
        if self.hook.as_mut().is_some_and(|hook| hook(point)) {
            self.interrupted = true;
        }
        self.interrupted
    }
    fn instruments(&self) -> Result<Vec<(InstrumentId, PriceScale)>, String> {
        self.definition
            .definition
            .instruments
            .iter()
            .map(|i| {
                Ok((
                    InstrumentId {
                        broker: i.broker.clone(),
                        provider_symbol: i.provider_symbol.clone(),
                    },
                    i.price_scale.try_into()?,
                ))
            })
            .collect()
    }
    fn check_authorization(&mut self) -> Result<(), String> {
        if self.mode == Mode::Live
            && !self.authorization_pending
            && (self.vetoes.contains("live authorization is absent")
                || self.health.journal_sequence == 0)
        {
            self.authorization_pending = true;
            self.veto("live authorization is absent", true);
            self.workers
                .storage
                .send(Storage::Authorization(self.definition.deployment.clone()))
                .map_err(|e| e.to_string())?;
        }
        Ok(())
    }
    fn set_lease(&mut self, before: i64, lease: Lease) -> Result<(), String> {
        self.control_time = self.control_time.max(lease.server_now_micros);
        let deadline = lease_deadline(
            before,
            self.local_now(),
            &lease,
            self.settings.control.safety_margin_micros,
        )?;
        self.deadline = deadline;
        self.health.fencing_token = lease.token;
        self.health.lease_deadline_micros = deadline;
        self.veto("lease lost", false);
        self.veto("lease deadline reached", false);
        Ok(())
    }
    fn drain(&mut self) -> Result<Vec<FinancialEvent>, String> {
        let events = self.engine.drain();
        let changed = events.iter().any(|event| {
            matches!(
                event.kind,
                EventKind::Accepted { .. }
                    | EventKind::Confirmed { .. }
                    | EventKind::Settled { .. }
                    | EventKind::Reconciled { .. }
                    | EventKind::Released { .. }
                    | EventKind::Unresolved { .. }
                    | EventKind::CashObserved { .. }
            )
        });
        let mut signals = Vec::new();
        for event in events {
            self.record(RecordKind::Ledger {
                event: event.clone(),
            })?;
            match &event.kind {
                EventKind::Signal {
                    command: Some(_), ..
                } => signals.push(event.clone()),
                EventKind::Accepted {
                    command,
                    liability: Some(liability),
                    discrepancy,
                    deficit,
                    ..
                } => {
                    self.contracts
                        .insert(liability.contract_ref.clone(), command.clone());
                    if *discrepancy || deficit.is_some() {
                        self.veto("account has unresolved financial evidence", true);
                    }
                }
                EventKind::Reconciled {
                    command,
                    resolution: Resolution::Purchased { liability, .. },
                    ..
                } => {
                    self.contracts
                        .insert(liability.contract_ref.clone(), command.clone());
                }
                EventKind::Confirmed {
                    command,
                    expiry_micros: Some(expiry),
                    ..
                } => {
                    let instrument = self.records.iter().find_map(|record| match &record.kind {
                        RecordKind::Ledger {
                            event:
                                FinancialEvent {
                                    kind:
                                        EventKind::Signal {
                                            command: Some(c),
                                            instrument,
                                            ..
                                        },
                                    ..
                                },
                        } if c == command => self
                            .definition
                            .definition
                            .instruments
                            .iter()
                            .position(|i| i.instrument == *instrument),
                        _ => None,
                    });
                    if let Some(instrument) = instrument && (self.mode == Mode::Replay || !self.records.iter().any(|r| matches!(&r.kind,RecordKind::DueTick {command:c,..} if c == command))) { self.due.entry(command.clone()).or_insert((instrument,*expiry)); }
                }
                EventKind::Settled {
                    command,
                    discrepancy,
                    deficit,
                    ..
                } => {
                    self.update_claim(command, ClaimState::Reconciled, None, None)?;
                    if *discrepancy || deficit.is_some() {
                        self.veto("account has unresolved financial evidence", true);
                    }
                }
                EventKind::Reconciled {
                    command,
                    resolution:
                        Resolution::Settled { .. }
                        | Resolution::ExternallyClosed { .. }
                        | Resolution::NotSent,
                    ..
                } => {
                    self.update_claim(command, ClaimState::Reconciled, None, None)?;
                }
                _ => {}
            }
        }
        self.veto(
            "account has unresolved financial evidence",
            !self.engine.accounts()[0].blocked.is_empty(),
        );
        self.veto(
            "unresolved dispatch claim",
            self.claims
                .values()
                .any(|c| matches!(c.state, ClaimState::Claimed | ClaimState::PossiblySent)),
        );
        if changed && self.prefix_at == self.replay_prefix.len() {
            self.compatibility()?;
        }
        Ok(signals)
    }
    fn compatibility(&mut self) -> Result<(), String> {
        let events: Vec<_> = self
            .records
            .iter()
            .filter_map(|record| match &record.kind {
                RecordKind::Ledger { event } => Some(event.clone()),
                _ => None,
            })
            .collect();
        let observation = Window {
            decision_start: self.settings.compatibility.observation_start.clone(),
            decision_end: self.settings.compatibility.observation_end.clone(),
        };
        let receipt = receipt::compute(&receipt::Inputs {
            deployment: &self.definition.deployment,
            definition: &self.definition.definition,
            baseline: &self.definition.policy.baseline,
            source: &self.definition.policy.source,
            account_class: self.health.account_class,
            required_account_class: self.settings.compatibility.required_account_class,
            observation: &observation,
            min_samples: self.settings.compatibility.min_samples,
            scenarios: &self.definition.scenarios,
            ledger: &self.definition.manifest.definition,
            events: &events,
            refusals: &self.records,
        });
        if let Some(dimension) = receipt
            .dimensions
            .iter()
            .find(|d| d.status == receipt::Status::OutsideEnvelope)
        {
            let reason = format!("compatibility outside envelope: {}", dimension.name);
            self.disable(&reason);
        }
        Ok(())
    }
    fn step(&mut self, observations: Vec<Observation>) -> Result<Vec<FinancialEvent>, String> {
        self.engine.step(self.clock.now_micros(), observations)?;
        self.drain()
    }
    fn update_claim(
        &mut self,
        command: &str,
        state: ClaimState,
        contract: Option<&str>,
        transaction: Option<&str>,
    ) -> Result<(), String> {
        let Some(claim) = self.claims.get_mut(command) else {
            return Ok(());
        };
        if claim.deployment != self.definition.deployment {
            return Ok(());
        }
        claim.state = state;
        if let Some(contract) = contract {
            claim.contract_ref = Some(contract.into());
        }
        if let Some(transaction) = transaction {
            claim.transaction_ref = Some(transaction.into());
        }
        self.control.advance_to(self.clock.now_micros());
        let result = self.control.update_claim(
            LeaseKey {
                broker: &self.health.broker,
                account: &self.health.account,
            },
            &self.settings.control.owner,
            self.health.fencing_token,
            command,
            state,
            contract,
            transaction,
        );
        self.veto(
            &format!("claim update unavailable: {command}"),
            !matches!(result, Ok(true)),
        );
        match result {
            Ok(false) => self.veto("lease lost", true),
            Err(reason) => self.record(RecordKind::Discontinuity {
                reason: format!("claim update {command}: {reason}"),
            })?,
            Ok(true) => {}
        }
        self.veto(
            "unresolved dispatch claim",
            self.claims
                .values()
                .any(|c| matches!(c.state, ClaimState::Claimed | ClaimState::PossiblySent)),
        );
        Ok(())
    }
    fn recover(&mut self) -> Result<(), String> {
        self.control.advance_to(self.clock.now_micros());
        let recovered = self.control.retained_claims(LeaseKey {
            broker: &self.health.broker,
            account: &self.health.account,
        });
        let remote = match recovered {
            Ok(claims) => claims,
            Err(error) => {
                self.veto("claim recovery unavailable", true);
                self.record(RecordKind::Discontinuity {
                    reason: format!("claim recovery unavailable: {error}"),
                })?;
                return Ok(());
            }
        };
        let mut ledger: Vec<_> = self
            .records
            .iter()
            .filter_map(|r| {
                if let RecordKind::Ledger { event } = &r.kind {
                    Some(event.clone())
                } else {
                    None
                }
            })
            .collect();
        for claim in remote {
            if claim.deployment != self.definition.deployment {
                self.disable("unresolved claim belongs to another deployment");
                self.claims.insert(claim.command.clone(), claim);
                continue;
            }
            if !ledger.iter().any(|event| matches!(&event.kind, EventKind::Signal { command: Some(command), .. } if *command == claim.command)) {
                let mut signal = claim.signal.clone();
                signal.sequence = ledger.len() as u64;
                ledger.push(signal.clone());
                self.engine = Engine::restore(ledger.iter().map(|event| Ok(event.to_line())))?;
                self.record(RecordKind::Ledger { event: signal })?;
            }
            self.claims.insert(claim.command.clone(), claim);
        }
        let mut open = BTreeSet::new();
        let mut accepted = BTreeSet::new();
        for event in &ledger {
            match &event.kind {
                EventKind::Signal {
                    command: Some(command),
                    ..
                } => {
                    open.insert(command.clone());
                }
                EventKind::Accepted {
                    command,
                    liability: Some(liability),
                    ..
                }
                | EventKind::Reconciled {
                    command,
                    resolution: Resolution::Purchased { liability, .. },
                    ..
                } => {
                    accepted.insert(command.clone());
                    self.contracts
                        .insert(liability.contract_ref.clone(), command.clone());
                }
                EventKind::Released { command, .. }
                | EventKind::Settled { command, .. }
                | EventKind::Reconciled {
                    command,
                    resolution:
                        Resolution::NotSent
                        | Resolution::Settled { .. }
                        | Resolution::ExternallyClosed { .. },
                    ..
                } => {
                    open.remove(command);
                }
                _ => {}
            }
        }
        for command in open
            .iter()
            .filter(|command| !self.claims.contains_key(*command))
            .cloned()
            .collect::<Vec<_>>()
        {
            if !accepted.contains(&command) {
                let possibly_sent = ledger.iter().any(|event|matches!(&event.kind,EventKind::PossiblySent {command: id,..} if *id == command));
                let committed = self.records.iter().any(|record|matches!(&record.kind,RecordKind::Claimed {command: id,..}|RecordKind::Written {command: id,..} if *id == command));
                if committed || possibly_sent {
                    if !possibly_sent {
                        let source = self.source(&format!("recovery-missing-claim:{command}"));
                        self.step(vec![Observation::PossiblySent {
                            command: command.clone(),
                            source,
                        }])?;
                    }
                    self.disable("journaled dispatch has no control row; reconciliation required");
                } else {
                    self.not_sent(&command, "recovery-before-claim")?;
                }
            }
        }
        for claim in self.claims.values().cloned().collect::<Vec<_>>() {
            if claim.deployment != self.definition.deployment {
                continue;
            }
            if !open.contains(&claim.command) {
                self.update_claim(&claim.command, ClaimState::Reconciled, None, None)?;
            } else if matches!(claim.state, ClaimState::NotSent | ClaimState::Rejected)
                && !accepted.contains(&claim.command)
            {
                self.resolve_not_sent(&claim.command, "recovery-proven-not-sent")?;
            } else if !accepted.contains(&claim.command) {
                if !ledger.iter().any(|event| matches!(&event.kind,EventKind::PossiblySent { command,.. } if *command == claim.command)) {
                    self.step(vec![Observation::PossiblySent {command: claim.command.clone(),source:self.source(&format!("recovery-uncertain:{}",claim.claim))}])?;
                }
                self.update_claim(&claim.command, ClaimState::PossiblySent, None, None)?;
            }
        }
        self.claims_restored = true;
        self.veto("claim recovery unavailable", false);
        if !self.claims.is_empty() {
            self.request_reconciliation()?;
        } else {
            self.send(Intent::OpenContracts)?;
        }
        Ok(())
    }
    fn source(&self, id: &str) -> EventSource {
        let now = self.clock.now_micros();
        EventSource {
            id: id.into(),
            provider_time_micros: now,
            available_at_micros: now,
            simulated: self.mode == Mode::Replay,
        }
    }
    fn not_sent(&mut self, command: &str, prefix: &str) -> Result<(), String> {
        self.complete_dispatch(command)?;
        let source = self.source(&format!("{prefix}:{command}"));
        self.step(vec![Observation::NotSent {
            command: command.into(),
            source,
        }])?;
        Ok(())
    }
    fn subscribe_contract(&mut self, contract: &str) -> Result<(), String> {
        if !self.subscribed.contains(contract) && self.subscriptions_pending.insert(contract.into())
        {
            self.send(Intent::SubscribeContract(contract.into()))?;
        }
        Ok(())
    }
    /// Exact baseline admission occurs before installing the offer or reserving money.
    pub fn offer(
        &mut self,
        binding: &str,
        proposal: impl Into<Option<Proposal>>,
    ) -> Result<Option<Observation>, String> {
        self.engine.withdraw_proposal(binding)?;
        let Some(proposal) = proposal.into() else {
            return Ok(None);
        };
        let bound = self
            .definition
            .policy
            .replay
            .bindings
            .iter()
            .find(|b| b.id == binding)
            .ok_or("live: unknown proposal binding")?;
        let index = self
            .definition
            .policy
            .replay
            .contracts
            .iter()
            .position(|c| c.id == bound.contract)
            .ok_or("live: missing request template")?;
        let template = &self.definition.policy.replay.contracts[index];
        let baseline = &self.definition.policy.baseline[index];
        if !proposal.terms.same_economics(baseline)?
            || proposal.terms.settlement != template.settlement
            || proposal.terms.semantics != template.semantics
            || proposal.terms.id != proposal.identity
            || proposal.request_identity != proposal.canonical_request_identity()?
        {
            self.record(RecordKind::Refused {
                binding: binding.into(),
                proposal: Some(proposal),
                reason: format!("offer differs from exact baseline {}", baseline.id),
            })?;
            self.compatibility()?;
            return Ok(None);
        }
        Ok(Some(Observation::Proposal {
            binding: binding.into(),
            proposal,
        }))
    }
    fn market_event(&mut self, event: LiveEvent) -> Result<(), String> {
        match event {
            LiveEvent::Break { generation, reason } => {
                self.veto("market continuity", true);
                self.warm.clear();
                self.veto("causal warmup is incomplete", true);
                self.health.warmup = false;
                while self.continuity.generation() < generation {
                    self.continuity.reconnect()?;
                }
                self.record(RecordKind::Discontinuity { reason })?;
                if self.draining {
                    return Ok(());
                }
                for (id, scale) in self.instruments()? {
                    if let Some(scheduler) = &self.scheduler {
                        scheduler.wake("market");
                    }
                    self.workers
                        .market
                        .send(Intent::Subscribe(id, scale))
                        .map_err(|e| e.to_string())?;
                }
            }
            LiveEvent::Observation(event) => {
                if let Err(error) = self.continuity.accept(&event) {
                    self.veto("market continuity", true);
                    if self.mode == Mode::Replay {
                        self.failure.get_or_insert(error);
                    }
                    return Ok(());
                }
                self.health.connection_generation = event.generation;
                self.health.receipt_sequence = event.sequence;
                self.last_provider_time = Some(event.provider_time_micros);
                let instrument = self
                    .definition
                    .definition
                    .instruments
                    .iter()
                    .position(|i| i.instrument == event.instrument.to_string())
                    .ok_or("live: unbound market instrument")?;
                let mut produced = FeatureOutput::default();
                if let Err(error) = self.features[instrument].push(
                    StreamObservation::Tick(Tick {
                        event_time_micros: event.provider_time_micros,
                        price_units: event.price_units,
                    }),
                    &mut produced,
                ) {
                    self.veto("market continuity", true);
                    if self.mode == Mode::Replay {
                        self.failure.get_or_insert(error.to_string());
                    }
                    return Ok(());
                }
                let plan = &self.definition.plans[instrument];
                let bound = &self.definition.definition.instruments[instrument];
                let mut rows = Vec::new();
                for (index, row) in produced.rows {
                    let stream_plan = &plan.streams[index];
                    if let Some(stream) = bound
                        .streams
                        .iter()
                        .position(|s| s.stream == stream_plan.key())
                    {
                        let values: Vec<_> = bound.streams[stream]
                            .columns
                            .iter()
                            .map(|column| {
                                let i = stream_plan
                                    .outputs
                                    .iter()
                                    .position(|o| o.name == column.source)
                                    .expect("bound column");
                                row.values[i].clone()
                            })
                            .collect();
                        rows.push(Observation::Row {
                            instrument,
                            stream,
                            close_time_micros: row.close_time_micros,
                            known_at_micros: row.known_at_micros.max(event.receipt_micros),
                            values,
                        });
                    }
                }

                let requests = self.definition.policy.replay.bindings.iter().filter(|binding| binding.instrument == event.instrument.to_string()).filter(|binding| {
                    let strategy = self.definition.policy.replay.strategies.iter().find(|s| s.id == binding.strategy).expect("validated");
                    rows.iter().any(|row| matches!(row, Observation::Row { stream, .. } if bound.streams[*stream].stream == strategy.base_stream))
                }).map(|binding| {
                    let terms = self.definition.policy.replay.contracts.iter().find(|c| c.id == binding.contract).expect("validated");
                    Ok(ProposalRequest { binding: binding.id.clone(), instrument: event.instrument.clone(), scale: bound.price_scale.try_into()?, direction: terms.direction, duration_seconds: u32::try_from(terms.duration_micros / 1_000_000).map_err(|_| "live: duration does not fit seconds")?, stake: terms.stake, currency: terms.currency.clone(), semantics: terms.semantics.expect("broker template"), settlement: terms.settlement })
                }).collect::<Result<Vec<_>, String>>()?;
                let ready = self.definition.policy.replay.bindings.iter().filter(|b| b.instrument == event.instrument.to_string()).all(|binding| {
                    let strategy = self.definition.policy.replay.strategies.iter().find(|s| s.id == binding.strategy).unwrap();
                    rows.iter().any(|row| matches!(row, Observation::Row {stream,values,..} if bound.streams[*stream].stream == strategy.base_stream && row_ready(&bound.streams[*stream], values)))
                });
                if ready {
                    self.warm.insert(instrument);
                }
                self.health.warmup = self.warm.len() == self.features.len();
                self.veto("causal warmup is incomplete", !self.health.warmup);
                if self.health.warmup {
                    self.veto("market continuity", false);
                }
                let ticks = self
                    .due
                    .iter()
                    .filter(|(_, (i, expiry))| {
                        *i == instrument && event.provider_time_micros >= *expiry
                    })
                    .map(|(command, _)| command.clone())
                    .collect::<Vec<_>>();
                let has_due = !ticks.is_empty();
                for command in ticks {
                    self.due.remove(&command);
                    self.record(RecordKind::DueTick {
                        command,
                        provider_time_micros: event.provider_time_micros,
                        price_units: event.price_units,
                    })?;
                }
                self.step(vec![Observation::Tick {
                    instrument,
                    provider_time_micros: event.provider_time_micros,
                    price_units: event.price_units,
                }])?;
                if !rows.is_empty() {
                    let close_micros = rows
                        .iter()
                        .filter_map(|row| match row {
                            Observation::Row {
                                close_time_micros, ..
                            } => Some(*close_time_micros),
                            _ => None,
                        })
                        .max()
                        .expect("base rows");
                    self.pending_rows.push_back(PendingRows {
                        instrument,
                        close_micros,
                        bindings: if self.draining {
                            BTreeSet::new()
                        } else {
                            requests.iter().map(|r| r.binding.clone()).collect()
                        },
                        observations: rows,
                        receipt_micros: event.receipt_micros,
                    });
                    self.prune_rows();
                    for request in requests {
                        if self.proposals_pending.insert(request.binding.clone()) {
                            self.send(Intent::Proposal(request))?;
                        }
                    }
                    self.rows_ready()?;
                }
                if has_due {
                    self.compatibility()?;
                }
            }
        }
        Ok(())
    }
    fn dispatch(&mut self, signal: FinancialEvent) -> Result<(), String> {
        let EventKind::Signal {
            command: Some(command),
            proposal: Some(proposal),
            ..
        } = &signal.kind
        else {
            return Err("live: dispatch requires an admitted broker signal".into());
        };
        self.measurements
            .entry(command.clone())
            .or_default()
            .market_event_to_decision_micros = self
            .decision_receipt
            .and_then(|at| signal.time_micros.checked_sub(at));
        if self.mode == Mode::Paper {
            return self.not_sent(command, "paper");
        }
        self.entry_gates()?;
        if !matches!(self.health.entries, Entries::Enabled) {
            return self.not_sent(command, "entry-disabled");
        }
        let prepared = PreparedPurchase {
            dispatch_claim: format!("{}:{command}", self.definition.deployment),
            command: command.clone(),
            proposal_identity: proposal.identity.clone(),
            maximum_price: proposal.terms.quoted_cost,
        };
        self.dispatch_order.push_back(command.clone());
        if self.dispatch_order.len() == 1 {
            self.send(Intent::Prepare(prepared.clone()))?;
        }
        self.dispatches.insert(
            command.clone(),
            Dispatch {
                signal,
                prepared,
                claim_at: None,
            },
        );
        Ok(())
    }
    fn complete_dispatch(&mut self, command: &str) -> Result<(), String> {
        self.dispatches.remove(command);
        if self.dispatch_order.front().is_some_and(|c| c == command) {
            self.dispatch_order.pop_front();
            if let Some(next) = self.dispatch_order.front() {
                self.send(Intent::Prepare(self.dispatches[next].prepared.clone()))?;
            }
        }
        Ok(())
    }
    fn prepared(&mut self, command: &str, encoded: Encoded) -> Result<(), String> {
        let dispatch = self
            .dispatches
            .get(command)
            .ok_or("live: prepared reply without command")?;
        let signal = dispatch.signal.clone();
        let prepared = dispatch.prepared.clone();
        let EventKind::Signal {
            binding,
            proposal: Some(proposal),
            ..
        } = &signal.kind
        else {
            return Err("live: missing dispatch proposal".into());
        };
        self.entry_gates()?;
        if !matches!(self.health.entries, Entries::Enabled) {
            return self.not_sent(command, "entry-disabled-after-prepare");
        }
        if self.checkpoint(Checkpoint::BeforeClaim) {
            return Ok(());
        }
        let bound = self
            .definition
            .policy
            .replay
            .bindings
            .iter()
            .find(|b| b.id == *binding)
            .expect("signal binding");
        let risk = self
            .definition
            .policy
            .replay
            .risk_policies
            .iter()
            .find(|r| r.id == bound.risk_policy)
            .expect("risk");
        let max_age = risk
            .max_proposal_age_micros
            .expect("projection requires bound");
        if self
            .clock
            .now_micros()
            .checked_sub(proposal.receipt_micros)
            .is_none_or(|age| age > max_age)
            || self.local_now() >= self.deadline
        {
            self.not_sent(command, "claim-boundary-expired")?;
            self.veto(&format!("proposal unavailable: {binding}"), true);
            return Ok(());
        }
        let claim = Claim {
            command: command.to_string(),
            claim: prepared.dispatch_claim.clone(),
            deployment: self.definition.deployment.clone(),
            token: self.health.fencing_token,
            max_proposal_age_micros: max_age,
            signal: signal.clone(),
            state: ClaimState::Claimed,
            contract_ref: None,
            transaction_ref: None,
        };
        self.control.advance_to(self.clock.now_micros());
        let claimed = self.control.claim(
            LeaseKey {
                broker: &self.health.broker,
                account: &self.health.account,
            },
            &self.settings.control.owner,
            self.health.fencing_token,
            &claim,
        );
        match claimed {
            Ok(ClaimOutcome::Inserted) => {
                self.claims.insert(command.to_string(), claim);
                self.dispatches.get_mut(command).unwrap().claim_at = Some(self.clock.now_micros());
                self.record(RecordKind::Claimed {
                    command: command.to_string(),
                    claim: prepared.dispatch_claim.clone(),
                    token: self.health.fencing_token,
                })?;
            }
            Ok(ClaimOutcome::LeaseLost) => {
                self.veto("lease lost", true);
                return self.not_sent(command, "claim-lease-lost");
            }
            Ok(ClaimOutcome::Replay(existing)) => {
                let mut existing = existing;
                existing.state = ClaimState::PossiblySent;
                self.claims.insert(command.to_string(), existing);
                self.complete_dispatch(command)?;
                self.disable("unresolved dispatch claim");
                let source = self.source(&format!("claim-replay:{command}"));
                self.step(vec![Observation::PossiblySent {
                    command: command.to_string(),
                    source,
                }])?;
                return Ok(());
            }
            Err(error) => {
                let mut claim = claim;
                claim.state = ClaimState::PossiblySent;
                self.claims.insert(command.to_string(), claim);
                self.complete_dispatch(command)?;
                self.disable("unresolved dispatch claim");
                self.record(RecordKind::Discontinuity {
                    reason: format!("claim commit uncertain: {error}"),
                })?;
                let source = self.source(&format!("claim-uncertain:{command}"));
                self.step(vec![Observation::PossiblySent {
                    command: command.to_string(),
                    source,
                }])?;
                // This instance attempted only the insert; no socket write was issued.
                // Enter reconciliation with the uncertain owner state before using that proof.
                self.refresh_claims()?;
                self.resolve_not_sent(command, "claim-commit-no-write")?;
                return Ok(());
            }
        }
        if self.checkpoint(Checkpoint::AfterClaimBeforeWrite) {
            return Ok(());
        }
        let bound = self
            .definition
            .policy
            .replay
            .bindings
            .iter()
            .find(|b| b.id == *binding)
            .expect("binding");
        let baseline = self
            .definition
            .policy
            .baseline
            .iter()
            .find(|c| c.id == bound.contract)
            .expect("baseline");
        let template = self
            .definition
            .policy
            .replay
            .contracts
            .iter()
            .find(|c| c.id == bound.contract)
            .expect("template");
        let valid = proposal.terms.same_economics(baseline)?
            && proposal.terms.settlement == template.settlement
            && proposal.terms.semantics == template.semantics;
        if !valid
            || self
                .clock
                .now_micros()
                .checked_sub(proposal.receipt_micros)
                .is_none_or(|age| age > max_age)
            || self.local_now() >= self.deadline
        {
            self.update_claim(command, ClaimState::NotSent, None, None)?;
            self.not_sent(command, "write-boundary-expired")?;
            self.update_claim(command, ClaimState::Reconciled, None, None)?;
            self.veto(&format!("proposal unavailable: {binding}"), true);
            return Ok(());
        }
        self.record(RecordKind::Written {
            command: command.to_string(),
            claim: prepared.dispatch_claim.clone(),
        })?;
        if self.checkpoint(Checkpoint::DuringWrite) {
            return Ok(());
        }
        self.send(Intent::Write(encoded))?;
        Ok(())
    }
    fn written(
        &mut self,
        command: &str,
        outcome: PurchaseOutcome,
        sent: i64,
        received: i64,
    ) -> Result<(), String> {
        if self.checkpoint(Checkpoint::AfterWriteBeforeAcknowledgement) {
            return Ok(());
        }
        let dispatch = self
            .dispatches
            .get(command)
            .ok_or("live: write reply without command")?;
        let prepared = dispatch.prepared.clone();
        let measured = self.measurements.entry(command.into()).or_default();
        measured.claim_to_socket_write_micros =
            dispatch.claim_at.and_then(|at| sent.checked_sub(at));
        if matches!(outcome, PurchaseOutcome::Accepted { .. }) {
            measured.decision_to_acceptance_micros =
                received.checked_sub(dispatch.signal.time_micros);
        }
        if self.mode == Mode::Replay
            && let PurchaseOutcome::PossiblySent { reason } = &outcome
        {
            self.failure.get_or_insert(reason.clone());
        }
        let (state, contract, transaction) = match &outcome {
            PurchaseOutcome::Accepted { liability, .. } => (
                ClaimState::Accepted,
                Some(liability.contract_ref.clone()),
                Some(liability.transaction_ref.clone()),
            ),
            PurchaseOutcome::Rejected { .. } => (ClaimState::Rejected, None, None),
            PurchaseOutcome::ProvenNotSent { .. } => (ClaimState::NotSent, None, None),
            PurchaseOutcome::PossiblySent { .. } => (ClaimState::PossiblySent, None, None),
        };
        self.step(vec![purchase_observation(
            command,
            &prepared.dispatch_claim,
            outcome,
            self.clock.now_micros(),
        )])?;
        self.update_claim(command, state, contract.as_deref(), transaction.as_deref())?;
        if state == ClaimState::PossiblySent {
            self.disable("unresolved dispatch claim");
        }
        if matches!(state, ClaimState::Rejected | ClaimState::NotSent) {
            self.update_claim(command, ClaimState::Reconciled, None, None)?;
        }
        if self.checkpoint(Checkpoint::AfterAcknowledgement) {
            return Ok(());
        }
        if let Some(contract) = contract {
            self.subscribe_contract(&contract)?;
        }
        self.complete_dispatch(command)
    }
    fn entry_gates(&mut self) -> Result<(), String> {
        self.veto("causal warmup is incomplete", !self.health.warmup);
        self.veto(
            "broker balance differs from assessed or restored cash",
            !self.health.balance_reconciled,
        );
        self.veto("lease deadline reached", self.local_now() >= self.deadline);
        self.veto(
            "journal spool bound reached",
            self.journal.spool_bytes()? >= self.settings.journal.max_spool_bytes,
        );
        self.veto(
            "unresolved dispatch claim",
            self.claims
                .values()
                .any(|c| matches!(c.state, ClaimState::Claimed | ClaimState::PossiblySent)),
        );
        Ok(())
    }
    fn lease(
        &mut self,
        sent_micros: i64,
        result: Result<Option<Lease>, String>,
    ) -> Result<(), String> {
        let state = match result {
            Ok(Some(lease)) => {
                self.set_lease(sent_micros, lease)?;
                self.veto("lease renewal unavailable or lost", false);
                LeaseState::Renewed
            }
            Ok(None) | Err(_) => {
                self.deadline = i64::MIN;
                self.veto("lease renewal unavailable or lost", true);
                LeaseState::Lost
            }
        };
        self.record(RecordKind::Lease {
            state,
            token: self.health.fencing_token,
        })
    }
    fn periodic(&mut self) -> Result<(), String> {
        let now = self.local_now();
        self.prune_rows();
        let renewal_due = now - self.last_renewal >= self.settings.control.renewal_interval_micros;
        if now - self.last_renewal >= self.settings.control.renewal_interval_micros {
            self.last_renewal = now;
            if self.monotonic.is_none() && self.renewal.is_none() && self.health.fencing_token != 0
            {
                self.control.advance_to(self.clock.now_micros());
                let result = self.control.renew(
                    LeaseKey {
                        broker: &self.health.broker,
                        account: &self.health.account,
                    },
                    &self.settings.control.owner,
                    self.health.fencing_token,
                    self.settings.control.lease_ttl_micros,
                );
                self.lease(now, result)?;
            }
            self.check_authorization()?;
            self.refresh_claims()?;
            if self
                .claims
                .values()
                .any(|c| c.state == ClaimState::PossiblySent)
                || self.has_unpaid_loss()
            {
                self.request_reconciliation()?;
            }
        }
        if renewal_due || self.upload_errors.is_empty() {
            self.upload()?;
        }
        self.entry_gates()?;
        self.write_health()
    }
    fn upload(&mut self) -> Result<(), String> {
        for name in self.journal.closed()? {
            if !self.uploads.is_empty() {
                break;
            }
            if self.checkpoint(Checkpoint::BeforeUpload) {
                return Ok(());
            }
            self.workers
                .storage
                .send(Storage::Upload {
                    key: self.journal.key(&name)?,
                    path: self.dir.join(&name),
                    name: name.clone(),
                })
                .map_err(|e| e.to_string())?;
            self.uploads.insert(name);
        }
        Ok(())
    }
    fn write_health(&mut self) -> Result<(), String> {
        self.health.cloud_failed_segments = self.upload_errors.len();
        self.health.pending_rows = self.pending_rows.len();
        self.health.pending_proposals = self.proposals_pending.len();
        self.health.risk = self.engine.accounts()[0].clone();
        self.health.journal_sequence = self.journal.next_sequence() - 1;
        self.health.open_commands = self
            .engine
            .accounts()
            .iter()
            .map(|account| account.open)
            .sum();
        let uncertain: BTreeSet<_> = self
            .engine
            .accounts()
            .iter()
            .flat_map(|account| &account.blocked)
            .filter_map(|(command, block)| {
                matches!(block, binary_alpha_engine::execution::Block::PossiblySent)
                    .then_some(command.as_str())
            })
            .collect();
        let pending_claims = self
            .claims
            .values()
            .filter(|claim| {
                matches!(claim.state, ClaimState::Claimed | ClaimState::PossiblySent)
                    && !uncertain.contains(claim.command.as_str())
            })
            .count();
        self.health.uncertain_commands =
            self.engine.summary().portfolio.unresolved + pending_claims as u64;
        let now = self.local_now();
        let cadence =
            now.saturating_sub(self.health_at) >= self.settings.control.renewal_interval_micros;
        if cadence || self.published_health.is_none() {
            self.health.last_event_age_micros = self
                .last_provider_time
                .map(|time| self.clock.now_micros().saturating_sub(time));
        }
        self.health.cloud_pending_segments = self.journal.closed()?.len();
        let bytes = serde_json::to_vec(&self.health).map_err(|error| error.to_string())?;
        if cadence || self.published_health.as_ref() != Some(&bytes) {
            fs::write(self.dir.join("health.json"), &bytes).map_err(|error| error.to_string())?;
            self.published_health = Some(bytes);
            if cadence {
                self.health_at = now;
            }
        }
        Ok(())
    }
    /// Polls both existing connections. A stop predicate requests ordinary final publication.
    /// A checkpoint interruption returns `None`, retaining the lease and committed claims.
    pub fn run_until(
        &mut self,
        mut stop: impl FnMut(&Health) -> bool,
    ) -> Result<Option<Completed>, String> {
        self.check_authorization()?;
        self.drive(|health| Ok(stop(health)))
    }
    fn drive(
        &mut self,
        mut stop: impl FnMut(&Health) -> Result<bool, String>,
    ) -> Result<Option<Completed>, String> {
        loop {
            if self.interrupted {
                self.write_health()?;
                return Ok(None);
            }
            if stop(&self.health)? && self.pending_rows.is_empty() && self.dispatches.is_empty() {
                return self.finish().map(Some);
            }
            let generation = self.scheduler.as_ref().map(ReplayClock::generation);
            self.receive()?;
            if self.interrupted {
                continue;
            }
            self.check_progress()?;
            self.periodic()?;
            // Due reconciliation can make the next recorded response ready.
            self.check_replay_progress(generation)?;
        }
    }
    /// Releases ownership, verifies closed segments, and publishes the restored ledger and receipt.
    pub fn finish(&mut self) -> Result<Completed, String> {
        self.check_archival_interruption()?;
        if self.prefix_at < self.replay_prefix.len() {
            return Err(
                "live replay: recorded log ended before the journal prefix reproduced".into(),
            );
        }
        if let Some(error) = self
            .failure
            .clone()
            .or_else(|| self.scheduler.as_ref().and_then(ReplayClock::failure))
        {
            return Err(error);
        }
        self.draining = true;
        self.disable("shutdown: draining observations");
        for rows in &mut self.pending_rows {
            rows.bindings.clear();
        }
        self.workers.stop_brokers(self.scheduler.as_ref());
        self.stop_renewal()?;
        let joined = self.workers.join_brokers();
        while let Ok(event) = self.workers.ingress.try_recv() {
            self.ingress(event)?;
            self.check_archival_interruption()?;
        }
        joined?;
        self.rows_ready()?;
        self.check_archival_interruption()?;
        if let Some(error) = &self.failure {
            return Err(error.clone());
        }
        self.disable("shutdown: lease release begun");
        if self.health.fencing_token != 0 {
            self.control.advance_to(self.clock.now_micros());
            let released = self.control.release(
                LeaseKey {
                    broker: &self.health.broker,
                    account: &self.health.account,
                },
                &self.settings.control.owner,
                self.health.fencing_token,
            );
            match released {
                Ok(true) => self.record(RecordKind::Lease {
                    state: LeaseState::Released,
                    token: self.health.fencing_token,
                })?,
                Ok(false) | Err(_) => self.record(RecordKind::Lease {
                    state: LeaseState::Lost,
                    token: self.health.fencing_token,
                })?,
            }
        }
        self.upload()?;
        self.check_archival_interruption()?;
        while !self.uploads.is_empty() {
            self.receive()?;
            self.check_archival_interruption()?;
            if self.uploads.is_empty() && self.upload_errors.is_empty() {
                self.upload()?;
                self.check_archival_interruption()?;
            }
        }
        if let Some(error) = self.upload_errors.values().next().cloned() {
            return Err(error);
        }
        self.segments.sort_by(|a, b| a.key.cmp(&b.key));
        // Verified full segments own the complete lifecycle before its claim is removed.
        // Keep deletion checkpoints before any final publication.
        for command in self
            .claims
            .values()
            .filter(|c| c.state == ClaimState::Reconciled && self.claim_archived(&c.command))
            .map(|c| c.command.clone())
            .collect::<Vec<_>>()
        {
            self.checkpoint(Checkpoint::BeforeClaimDeletion);
            self.check_archival_interruption()?;
            self.control.advance_to(self.clock.now_micros());
            self.control.delete_reconciled(
                LeaseKey {
                    broker: &self.health.broker,
                    account: &self.health.account,
                },
                &command,
            )?;
            self.checkpoint(Checkpoint::AfterClaimDeletion);
            self.check_archival_interruption()?;
        }
        let events = self
            .records
            .iter()
            .filter_map(|record| {
                if let RecordKind::Ledger { event } = &record.kind {
                    Some(event.clone())
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();
        self.workers
            .storage
            .send(Storage::Ledger(events.clone()))
            .map_err(|e| e.to_string())?;
        while self.ledger_result.is_none() {
            self.receive()?;
            self.check_archival_interruption()?;
        }
        let ledger = self.ledger_result.take().unwrap()?;
        let observation = Window {
            decision_start: self.settings.compatibility.observation_start.clone(),
            decision_end: self.settings.compatibility.observation_end.clone(),
        };
        let receipt = receipt::compute(&receipt::Inputs {
            deployment: &self.definition.deployment,
            definition: &self.definition.definition,
            baseline: &self.definition.policy.baseline,
            source: &self.definition.policy.source,
            account_class: self.health.account_class,
            required_account_class: self.settings.compatibility.required_account_class,
            observation: &observation,
            min_samples: self.settings.compatibility.min_samples,
            scenarios: &self.definition.scenarios,
            ledger: &ledger.generation,
            events: &events,
            refusals: &self.records,
        });
        let final_manifest = FinalManifest {
            deployment: self.definition.deployment.clone(),
            ledger: format!("{}{}", self.destination_uri, ledger.key()),
            ledger_generation: ledger.generation.clone(),
            receipt: format!("{}{}", self.destination_uri, receipt.key()),
            journal_segments: self.segments.clone(),
            open_tail: self.journal.open_tail()?,
            measurements: self.measurements.clone(),
        };
        if let Some(error) = self.failure.clone() {
            return Err(error);
        }
        self.workers
            .storage
            .send(Storage::Publish {
                receipt: receipt.clone(),
                manifest: final_manifest.clone(),
            })
            .map_err(|e| e.to_string())?;
        while self.publication_result.is_none() {
            self.receive()?;
            self.check_archival_interruption()?;
        }
        let manifest_uri = self.publication_result.take().unwrap()?;
        self.workers.join_storage()?;
        self.write_health()?;
        Ok(Completed {
            ledger,
            receipt,
            manifest: final_manifest,
            manifest_uri,
        })
    }
    fn check_archival_interruption(&self) -> Result<(), String> {
        if self.interrupted {
            return Err("live: archival interrupted before final publication".into());
        }
        Ok(())
    }
    fn claim_archived(&self, command: &str) -> bool {
        let mut through = 0;
        for segment in &self.segments {
            let Some((first, last)) = segment
                .key
                .rsplit('/')
                .next()
                .and_then(journal::segment_range)
            else {
                return false;
            };
            if first != through + 1
                || last - first + 1 != u64::from(self.settings.journal.segment_records)
            {
                return false;
            }
            through = last;
        }
        let mut terminal = false;
        for record in &self.records {
            let related = match &record.kind {
                RecordKind::Ledger { event } => match &event.kind {
                    EventKind::Settled { command: id, .. }
                    | EventKind::Reconciled {
                        command: id,
                        resolution:
                            Resolution::NotSent
                            | Resolution::Settled { .. }
                            | Resolution::ExternallyClosed { .. },
                        ..
                    } => {
                        terminal |= id == command;
                        id == command
                    }
                    EventKind::Signal {
                        command: Some(id), ..
                    }
                    | EventKind::Acknowledged { command: id, .. }
                    | EventKind::Accepted { command: id, .. }
                    | EventKind::Confirmed { command: id, .. }
                    | EventKind::Released { command: id, .. }
                    | EventKind::PossiblySent { command: id, .. }
                    | EventKind::Unresolved { command: id, .. }
                    | EventKind::Reconciled { command: id, .. }
                    | EventKind::CashObserved {
                        matched: Some(id), ..
                    } => id == command,
                    _ => false,
                },
                RecordKind::Claimed { command: id, .. }
                | RecordKind::Written { command: id, .. }
                | RecordKind::DueTick { command: id, .. } => id == command,
                _ => false,
            };
            if related && record.sequence > through {
                return false;
            }
        }
        terminal
    }
    fn stop_renewal(&mut self) -> Result<(), String> {
        self.renewal_stop.store(true, Ordering::SeqCst);
        if let Some(thread) = self.renewal.take() {
            thread.thread().unpark();
            thread.join().map_err(|_| "live: renewal worker panicked")?;
        }
        Ok(())
    }
    fn start_renewal(&mut self, base: &Path) {
        let settings = self.settings.control.clone();
        let base = base.to_path_buf();
        let broker = self.health.broker.clone();
        let account = self.health.account.clone();
        let ingress = self.workers.sender.take().expect("renewal ingress");
        let token = self.health.fencing_token;
        let stopped = self.renewal_stop.clone();
        let anchor = self
            .monotonic
            .expect("production renewal uses the startup clock anchor");
        self.renewal = Some(std::thread::spawn(move || {
            let now = || {
                anchor
                    .1
                    .saturating_add(anchor.0.elapsed().as_micros() as i64)
            };
            let Some(mut control) =
                renewal_connection(&ingress, now, || connect_control(&settings, &base))
            else {
                return;
            };
            loop {
                std::thread::park_timeout(Duration::from_micros(
                    settings.renewal_interval_micros as u64,
                ));
                if stopped.load(Ordering::SeqCst) {
                    return;
                }
                let sent_micros = now();
                let lease = control.renew(
                    LeaseKey {
                        broker: &broker,
                        account: &account,
                    },
                    &settings.owner,
                    token,
                    settings.lease_ttl_micros,
                );
                if ingress
                    .send(Ingress::Lease {
                        sent_micros,
                        result: lease,
                    })
                    .is_err()
                {
                    return;
                }
            }
        }));
    }
}
impl Drop for Runtime {
    fn drop(&mut self) {
        self.workers.stop_brokers(self.scheduler.as_ref());
        if let Err(error) = self.stop_renewal() {
            eprintln!("{error}");
        }
    }
}

pub struct Completed {
    pub ledger: ReplayManifest,
    pub receipt: receipt::Receipt,
    pub manifest: FinalManifest,
    pub manifest_uri: String,
}

/// Conservatively maps the database lifetime using the measured round trip and frozen margin.
pub fn lease_deadline(sent: i64, received: i64, lease: &Lease, margin: i64) -> Result<i64, String> {
    if received < sent || margin < 0 {
        return Err("live: invalid lease clock mapping".into());
    }
    let round_trip = received
        .checked_sub(sent)
        .ok_or("live: lease clock arithmetic overflow")?;
    lease
        .expires_at_micros
        .checked_sub(lease.server_now_micros)
        .and_then(|ttl| sent.checked_add(ttl))
        .and_then(|value| value.checked_sub(round_trip))
        .and_then(|value| value.checked_sub(margin))
        .ok_or("live: lease clock mapping overflow".into())
}
fn renewal_connection<T>(
    ingress: &std::sync::mpsc::Sender<Ingress>,
    now: impl FnOnce() -> i64,
    connect: impl FnOnce() -> Result<T, String>,
) -> Option<T> {
    match connect() {
        Ok(control) => Some(control),
        Err(_) => {
            let _ = ingress.send(Ingress::Lease {
                sent_micros: now(),
                result: Err("lease connection unavailable".into()),
            });
            None
        }
    }
}

fn renewal_finished(renewal: &mut Option<std::thread::JoinHandle<()>>) -> Result<bool, String> {
    if renewal.as_ref().is_none_or(|thread| !thread.is_finished()) {
        return Ok(false);
    }
    renewal
        .take()
        .unwrap()
        .join()
        .map_err(|_| "live: renewal worker panicked")?;
    Ok(true)
}

fn connect_control(
    settings: &binary_alpha_engine::config::ControlSettings,
    base: &Path,
) -> Result<Postgres, String> {
    Postgres::connect(
        &settings.host,
        settings.port,
        &settings.database,
        &settings.user,
        &settings.credential,
        &base.join(&settings.root_certificate),
    )
}
fn stores(config: &Config, base: &Path) -> Result<(Store, Store), String> {
    Ok((
        Store::filesystem(base.join(config.storage.historical_data_dir.as_path())),
        Store::open(&config.storage.publication_uri)?,
    ))
}
fn report(
    mode: Mode,
    deployment: &str,
    completed: &Completed,
    out: &mut dyn Write,
) -> Result<(), String> {
    writeln!(
        out,
        "live {} deployment {} receipt {} eligible {}",
        mode.text(),
        deployment,
        completed.manifest.receipt,
        completed.receipt.promotion.eligible
    )
    .and_then(|()| writeln!(out, "live final manifest {}", completed.manifest_uri))
    .map_err(|error| error.to_string())
}

/// Runs a recorded broker log without resolving a broker secret or opening a broker connection.
pub fn replay(config_path: &Path, out: &mut dyn Write) -> Result<(), String> {
    let config = crate::load_config(config_path)?;
    if !matches!(config.run_mode, RunMode::Research | RunMode::Replay) {
        return Err("live replay: run_mode must be research or replay".into());
    }
    let settings = config.live.as_ref().ok_or("live: the table is required")?;
    let replay = settings
        .replay
        .as_ref()
        .ok_or("live replay: live.replay is required")?;
    let base = config_path.parent().unwrap_or(Path::new("."));
    let (local, destination) = stores(&config, base)?;
    let definition = definition(&config, &local, &destination)?;
    let Broker::Deriv(broker) = config
        .brokers
        .iter()
        .find(|b| b.id() == &settings.broker)
        .ok_or("live: broker missing")?
    else {
        return Err("live: Deriv broker required".into());
    };
    let recorded = RecordedConnector::open(&base.join(&replay.broker_log))?;
    let clock = recorded.clock();
    let mut broker = broker.clone();
    broker.account_class = Some(settings.compatibility.required_account_class);
    let address =
        DerivAccounts::bootstrap(&broker, &mut recorded.http(), "recorded-no-credential")?;
    let account = AccountIdentity {
        broker: broker.id.clone(),
        account: settings.account.clone(),
        class: address.account_class,
        currency: address.currency.clone(),
    };
    let instruments = definition
        .definition
        .instruments
        .iter()
        .map(|i| {
            Ok((
                InstrumentId {
                    broker: i.broker.clone(),
                    provider_symbol: i.provider_symbol.clone(),
                },
                i.price_scale.try_into()?,
            ))
        })
        .collect::<Result<Vec<_>, String>>()?;
    let options = DerivOptions::connect(
        address,
        account,
        &instruments,
        Box::new(recorded.session("account")?),
        Box::new(clock.clone()),
        broker.budgets.clone().unwrap_or_default(),
    )?;
    let market = DerivMarketData::connect(
        &broker,
        Box::new(recorded.session("market")?),
        Box::new(clock.clone()),
    )?;
    let control = FakeControl::new(clock.now_micros());
    let mut runtime = Runtime::start(
        &config,
        base,
        definition,
        local,
        destination,
        Box::new(control),
        Box::new(market),
        options,
        Box::new(clock.clone()),
        Some(clock.clone()),
        Mode::Replay,
    )?;
    let mut exhausted_at = None;
    let completed = runtime
        .drive(|health| {
            if recorded.exhausted() {
                // Two idle polls drain an update/terminal pair decoded from the final frame.
                if exhausted_at == Some(health.journal_sequence) {
                    return Ok(true);
                }
                exhausted_at = Some(health.journal_sequence);
            }
            Ok(false)
        })?
        .ok_or("live replay: interrupted")?;
    report(
        Mode::Replay,
        &runtime.definition.deployment,
        &completed,
        out,
    )
}

/// Runs separately authorized paper/live observations; exact authorization gates live entries.
pub fn run(config_path: &Path, out: &mut dyn Write) -> Result<(), String> {
    let config = crate::load_config(config_path)?;
    let mode = match config.run_mode {
        RunMode::Paper => Mode::Paper,
        RunMode::Live => Mode::Live,
        _ => return Err("live run: run_mode must be paper or live".into()),
    };
    let base = config_path.parent().unwrap_or(Path::new("."));
    let (local, destination) = stores(&config, base)?;
    let definition = definition(&config, &local, &destination)?;
    let settings = config.live.as_ref().expect("definition requires live");
    let Broker::Deriv(broker) = config
        .brokers
        .iter()
        .find(|b| b.id() == &settings.broker)
        .expect("validated")
    else {
        return Err("live: Deriv broker required".into());
    };
    let connector = WebSocketConnector::new()?;
    let address = DerivAccounts::resolve(broker, &mut connector.http())?;
    let account = AccountIdentity {
        broker: broker.id.clone(),
        account: settings.account.clone(),
        class: address.account_class,
        currency: address.currency.clone(),
    };
    let instruments = definition
        .definition
        .instruments
        .iter()
        .map(|i| {
            Ok((
                InstrumentId {
                    broker: i.broker.clone(),
                    provider_symbol: i.provider_symbol.clone(),
                },
                i.price_scale.try_into()?,
            ))
        })
        .collect::<Result<Vec<_>, String>>()?;
    let options = DerivOptions::connect(
        address,
        account,
        &instruments,
        Box::new(connector),
        Box::new(SystemClock),
        broker.budgets.clone().unwrap_or_default(),
    )?;
    let market = DerivMarketData::connect(
        broker,
        Box::new(WebSocketConnector::new()?),
        Box::new(SystemClock),
    )?;
    let control = connect_control(&settings.control, base)?;
    let end = parse_event_time_micros(&settings.compatibility.observation_end)?;
    let mut runtime = Runtime::start_at(
        &config,
        base,
        definition,
        local,
        destination,
        Box::new(control),
        Box::new(market),
        options,
        Box::new(SystemClock),
        None,
        mode,
        Some((Instant::now(), SystemClock.now_micros())),
    )?;
    runtime.start_renewal(base);
    let completed = runtime
        .run_until(|_| SystemClock.now_micros() >= end)?
        .ok_or("live run: interrupted")?;
    report(mode, &runtime.definition.deployment, &completed, out)
}

/// Creates the immutable operator authorization under the deployment manifest's store root.
pub fn authorize(
    deployment_manifest: &str,
    bundle_manifest: &str,
    broker: &str,
    account: &str,
    reason: &str,
    out: &mut dyn Write,
) -> Result<(), String> {
    let (store, key) = crate::research::open_object(deployment_manifest)?;
    let deployment = DeploymentManifest::from_json(&read_key(&store, &key)?)?;
    let root = deployment_manifest
        .strip_suffix(&deployment.key())
        .ok_or("live authorization: deployment manifest key mismatch")?
        .trim_end_matches('/');
    let destination = Store::open(&root.parse()?)?;
    let (bundle_store, key) = crate::verify::open(bundle_manifest)?;
    let bytes = read_key(&bundle_store, &key)?;
    crate::research::verify_run(
        bundle_manifest,
        &bundle_store,
        &key,
        &bytes,
        Access::ORDINARY,
    )?;
    let bundle = RunManifest::from_json(&bytes)?;
    if bundle.bundle_sha256() != deployment.bundle_sha256
        || bundle.generation != deployment.research
        || broker != deployment.broker
        || account != deployment.account
    {
        return Err("live authorization: deployment, bundle, broker, or account mismatch".into());
    }
    let local = Store::filesystem(
        std::env::current_dir()
            .map_err(|error| error.to_string())?
            .join("target/live-authorization"),
    );
    let (authorization, _) = authorization::create(
        &destination,
        &local,
        authorization::Authorization {
            schema_version: 1,
            deployment: deployment.hash,
            configuration: deployment.config_hash,
            bundle_sha256: deployment.bundle_sha256,
            broker: broker.into(),
            account: account.into(),
            operator: std::env::var("USER").unwrap_or_else(|_| "unavailable".into()),
            reason: reason.into(),
            hash: String::new(),
        },
    )?;
    writeln!(
        out,
        "live authorization {} at {}",
        authorization.hash,
        destination.uri(&authorization::key(&authorization.deployment))
    )
    .map_err(|error| error.to_string())
}

fn row_ready(
    stream: &binary_alpha_engine::execution::StreamColumns,
    values: &[Option<binary_alpha_engine::features::Value>],
) -> bool {
    use binary_alpha_engine::features::Value;
    stream.columns.iter().zip(values).all(|(column, value)| {
        value.as_ref().is_some_and(|value| {
            !matches!(value, Value::Text(text) if column.unready.iter().any(|unready| unready == text.as_ref()))
        }) && column.readiness.iter().all(|flag| {
            stream.columns.iter().position(|c| c.name == *flag).is_some_and(|i| values[i] == Some(Value::Bool(true)))
        })
    })
}

#[cfg(test)]
mod readiness_regressions {
    use super::*;
    use binary_alpha_engine::{
        config::StreamKey,
        execution::{ColumnSpec, StreamColumns},
        features::{Kind, Value},
    };
    #[test]
    fn present_false_ema_flag_and_not_ready_ema_state_keep_warmup_unready() {
        let flag = ColumnSpec {
            name: "ema_ready".into(),
            source: "ema_ready".into(),
            kind: Kind::Bool,
            encoding: None,
            readiness: vec![],
            unready: vec![],
        };
        let state = ColumnSpec {
            name: "ema_state".into(),
            source: "ema_state".into(),
            kind: Kind::Text,
            encoding: None,
            readiness: vec!["ema_ready".into()],
            unready: vec!["not_ready".into()],
        };
        let stream = StreamColumns {
            stream: StreamKey {
                duration_seconds: 20,
                offset_seconds: 0,
            },
            columns: vec![flag, state],
        };
        for values in [
            vec![Some(Value::Bool(false)), Some(Value::Text("up".into()))],
            vec![
                Some(Value::Bool(true)),
                Some(Value::Text("not_ready".into())),
            ],
        ] {
            assert!(values.iter().all(Option::is_some));
            assert!(!row_ready(&stream, &values));
        }
        assert!(row_ready(
            &stream,
            &[Some(Value::Bool(true)), Some(Value::Text("up".into()))]
        ));
    }
}

#[cfg(test)]
mod lease_ownership_regressions {
    use super::*;
    #[test]
    fn owner_deadline_is_a_local_integer() {
        // Type-check the actual owner field, so the obsolete Arc<AtomicI64> fails compilation.
        let _: fn(&Runtime) -> i64 = |owner| owner.deadline;
    }
}

#[cfg(test)]
mod renewal_regressions {
    use super::*;
    #[test]
    fn renewal_connection_failure_is_lease_loss_and_only_panic_is_fatal() {
        let mut initial = FakeControl::new(100);
        let key = LeaseKey {
            broker: "synthetic",
            account: "account",
        };
        assert!(
            initial
                .acquire(key, "owner", "deployment", 100)
                .unwrap()
                .is_some()
        );
        let (sender, receiver) = std::sync::mpsc::channel();
        let mut worker = Some(std::thread::spawn(move || {
            assert!(
                renewal_connection::<FakeControl>(
                    &sender,
                    || 101,
                    || Err("synthetic separate connection failure".into())
                )
                .is_none()
            );
        }));
        let event = receiver.recv_timeout(Duration::from_secs(2)).unwrap();
        assert!(matches!(
            event,
            Ingress::Lease {
                sent_micros: 101,
                result: Err(_)
            }
        ));
        let limit = Instant::now() + Duration::from_secs(2);
        while !worker.as_ref().unwrap().is_finished() && Instant::now() < limit {
            std::thread::yield_now();
        }
        assert_eq!(renewal_finished(&mut worker), Ok(true));
        assert!(worker.is_none());
        assert_eq!(renewal_finished(&mut worker), Ok(false));
        let mut panicked = Some(std::thread::spawn(|| panic!("synthetic renewal panic")));
        let limit = Instant::now() + Duration::from_secs(2);
        while !panicked.as_ref().unwrap().is_finished() && Instant::now() < limit {
            std::thread::yield_now();
        }
        assert_eq!(
            renewal_finished(&mut panicked),
            Err("live: renewal worker panicked".into())
        );
    }
}

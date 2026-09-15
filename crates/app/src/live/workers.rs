//! Broker and storage I/O workers. Only the receiver's owner changes financial state.
use super::*;
use crate::broker::OpenContract;
use crate::broker::deriv::{Encoded, StatementRow};
use binary_alpha_engine::execution::Decimal;
use std::sync::mpsc::{self, Receiver, Sender};

#[derive(Debug, Clone)]
#[allow(clippy::large_enum_variant)]
pub enum Intent {
    Subscribe(InstrumentId, PriceScale),
    Transactions,
    Proposal(ProposalRequest),
    Prepare(PreparedPurchase),
    Write(Encoded),
    SubscribeContract(String),
    Balance,
    OpenContracts,
    Statement { from: i64, through: i64 },
}
#[derive(Debug)]
#[allow(clippy::large_enum_variant)]
pub enum ReplyValue {
    Subscribed {
        contract: Option<String>,
    },
    Proposal {
        binding: String,
        proposal: Proposal,
    },
    Prepared {
        command: String,
        encoded: Encoded,
    },
    Written {
        command: String,
        outcome: PurchaseOutcome,
    },
    Balance(Decimal),
    OpenContracts(Vec<OpenContract>),
    Statement {
        from: i64,
        through: i64,
        rows: Vec<StatementRow>,
    },
}
#[derive(Debug, Clone, Copy)]
pub struct ReplyTiming {
    pub sent_micros: i64,
    pub received_micros: i64,
}
#[derive(Debug)]
#[allow(clippy::large_enum_variant)]
pub enum Reply {
    Completed {
        value: ReplyValue,
        timing: ReplyTiming,
    },
    Failed {
        intent: Intent,
        reason: String,
        timing: ReplyTiming,
        rejected: bool,
    },
}
#[allow(clippy::large_enum_variant)]
pub enum Ingress {
    Market(LiveEvent),
    Account(AccountEvent),
    Reply(Reply),
    MarketFailed(String),
    AccountFailed(String),
    Uploaded {
        name: String,
        segment: receipt::Segment,
    },
    UploadFailed {
        name: String,
        reason: String,
    },
    Authorization(Result<Option<authorization::Authorization>, String>),
    WorkerFailed(String),
    Lease {
        sent_micros: i64,
        result: Result<Option<Lease>, String>,
    },
    Ledger(Result<ReplayManifest, String>),
    Published(Result<String, String>),
}
#[allow(clippy::large_enum_variant)]
pub(super) enum Storage {
    Upload {
        name: String,
        key: String,
        path: PathBuf,
    },
    Authorization(String),
    Ledger(Vec<FinancialEvent>),
    Publish {
        receipt: receipt::Receipt,
        manifest: FinalManifest,
    },
}
pub(super) struct Workers {
    pub ingress: Receiver<Ingress>,
    pub sender: Option<Sender<Ingress>>,
    pub market: Sender<Intent>,
    pub account: Sender<Intent>,
    pub storage: Sender<Storage>,
    pub market_ack: Sender<()>,
    pub account_ack: Sender<()>,
    pub stop: Arc<AtomicBool>,
    brokers: Vec<std::thread::JoinHandle<()>>,
    storage_thread: Option<std::thread::JoinHandle<()>>,
}
fn deliver(tx: &Sender<Ingress>, ack: &Receiver<()>, stopped: &AtomicBool, event: Ingress) -> bool {
    if tx.send(event).is_err() {
        return false;
    }
    loop {
        if stopped.load(Ordering::SeqCst) {
            // Shutdown stops receiving new work, but hands already consumed facts to the
            // owner without waiting for acknowledgements; it drains ingress after joining.
            return true;
        }
        match ack.recv_timeout(Duration::from_micros(POLL_MICROS as u64)) {
            Ok(()) => return true,
            Err(mpsc::RecvTimeoutError::Disconnected) => return false,
            Err(mpsc::RecvTimeoutError::Timeout) => {}
        }
    }
}
impl Workers {
    pub fn start(
        mut market: Box<dyn MarketDataBroker>,
        mut options: DerivOptions,
        scheduler: Option<ReplayClock>,
        local: Store,
        destination: Store,
    ) -> Self {
        let (sender, ingress) = mpsc::channel();
        let (market_tx, market_rx) = mpsc::channel();
        let (account, account_rx) = mpsc::channel();
        let (storage, storage_rx) = mpsc::channel();
        let (market_ack, market_done) = mpsc::channel();
        let (account_ack, account_done) = mpsc::channel();
        let stop = Arc::new(AtomicBool::new(false));
        let tx = sender.clone();
        let stopped = stop.clone();
        let market_clock = scheduler.clone();
        let market_thread = spawn("market", sender.clone(), move || {
            let mut subscribed = false;
            while !stopped.load(Ordering::SeqCst) {
                if let Ok(intent) = market_rx.try_recv() {
                    if let Some(clock) = &market_clock {
                        clock.begin("market");
                    }
                    if let Intent::Subscribe(id, scale) = intent {
                        match market.subscribe(&id, scale) {
                            Ok(()) => subscribed = true,
                            Err(reason) => {
                                // Cancellation may interrupt the initial subscription just as
                                // it interrupts an ordinary poll; shutdown is not an adapter fault.
                                if stopped.load(Ordering::SeqCst) {
                                    return;
                                }
                                if !deliver(
                                    &tx,
                                    &market_done,
                                    &stopped,
                                    Ingress::MarketFailed(reason),
                                ) {
                                    return;
                                }
                            }
                        }
                    }
                }
                if !subscribed {
                    if let Some(clock) = &market_clock {
                        clock.idle("market");
                    } else {
                        std::thread::park_timeout(Duration::from_micros(POLL_MICROS as u64));
                    }
                    continue;
                }
                let event = match market.next_live(POLL_MICROS) {
                    Ok(Some(event)) => Ingress::Market(event),
                    Ok(None) => {
                        if let Some(clock) = &market_clock {
                            clock.complete();
                        }
                        continue;
                    }
                    Err(reason) => {
                        if stopped.load(Ordering::SeqCst) {
                            return;
                        }
                        if !deliver(
                            &tx,
                            &market_done,
                            &stopped,
                            Ingress::MarketFailed(reason.clone()),
                        ) {
                            return;
                        }
                        if let Some(clock) = &market_clock {
                            clock.complete();
                        }
                        match market.reconnect() {
                            Ok(()) => {
                                subscribed = false;
                                match market.next_live(0) {
                                    Ok(Some(event @ LiveEvent::Break { .. })) => {
                                        Ingress::Market(event)
                                    }
                                    _ => Ingress::Market(LiveEvent::Break {
                                        generation: market.continuity().generation(),
                                        reason,
                                    }),
                                }
                            }
                            Err(error) => {
                                subscribed = false;
                                Ingress::MarketFailed(error)
                            }
                        }
                    }
                };
                if !deliver(&tx, &market_done, &stopped, event) {
                    return;
                }
                if let Some(clock) = &market_clock {
                    clock.complete();
                }
            }
        });
        let tx = sender.clone();
        let stopped = stop.clone();
        let account_clock = scheduler;
        let account_thread =
            spawn("account", sender.clone(), move || {
                let mut failed = false;
                let mut initialized = false;
                while !stopped.load(Ordering::SeqCst) {
                    if let Ok(intent) = account_rx.try_recv() {
                        if let Some(clock) = &account_clock {
                            clock.begin("account");
                        }
                        failed = false;
                        initialized = true;
                        let sent_micros = options.now_micros();
                        let result =
                            match &intent {
                                Intent::Transactions => options
                                    .subscribe_transactions()
                                    .map(|()| ReplyValue::Subscribed { contract: None }),
                                Intent::Proposal(request) => {
                                    options
                                        .proposal(request)
                                        .map(|proposal| ReplyValue::Proposal {
                                            binding: request.binding.clone(),
                                            proposal,
                                        })
                                }
                                Intent::Prepare(prepared) => options
                                    .prepare_purchase(prepared)
                                    .map(|encoded| ReplyValue::Prepared {
                                        command: prepared.command.clone(),
                                        encoded,
                                    }),
                                Intent::Write(encoded) => options
                                    .write_purchase(encoded.clone())
                                    .map(|outcome| ReplyValue::Written {
                                        command: encoded.command().into(),
                                        outcome,
                                    }),
                                Intent::SubscribeContract(id) => options
                                    .subscribe_contract(id)
                                    .map(|()| ReplyValue::Subscribed {
                                        contract: Some(id.clone()),
                                    }),
                                Intent::Balance => options.balance().map(ReplyValue::Balance),
                                Intent::OpenContracts => {
                                    options.open_contracts().map(ReplyValue::OpenContracts)
                                }
                                Intent::Statement { from, through } => options
                                    .statement(*from, *through)
                                    .map(|rows| ReplyValue::Statement {
                                        from: *from,
                                        through: *through,
                                        rows,
                                    }),
                                Intent::Subscribe(..) => {
                                    Err("account worker received market intent".into())
                                }
                            };
                        let timing = ReplyTiming {
                            sent_micros,
                            received_micros: options.now_micros(),
                        };
                        let reply = match result {
                            Ok(value) => Reply::Completed { value, timing },
                            Err(reason) => Reply::Failed {
                                rejected: options.rejected(&reason),
                                intent,
                                reason,
                                timing,
                            },
                        };
                        let event = Ingress::Reply(reply);
                        if !deliver(&tx, &account_done, &stopped, event) {
                            return;
                        }
                        if !drain_account(&mut options, &tx, &account_done, &stopped) {
                            return;
                        }
                        if let Some(clock) = &account_clock {
                            clock.complete();
                        }
                    }
                    // Poll facts after each completed intent, before the next queued intent.
                    if !initialized || failed {
                        if let Some(clock) = &account_clock {
                            clock.idle("account");
                        } else {
                            std::thread::park_timeout(Duration::from_micros(POLL_MICROS as u64));
                        }
                        continue;
                    }
                    let event = match options.next_account_event(POLL_MICROS) {
                        Ok(Some(event)) => Ingress::Account(event),
                        Ok(None) => {
                            if let Some(clock) = &account_clock {
                                clock.complete();
                            }
                            continue;
                        }
                        Err(error) => {
                            if stopped.load(Ordering::SeqCst) {
                                return;
                            }
                            failed = true;
                            Ingress::AccountFailed(error)
                        }
                    };
                    if !deliver(&tx, &account_done, &stopped, event) {
                        return;
                    }
                    if !drain_account(&mut options, &tx, &account_done, &stopped) {
                        return;
                    }
                    if let Some(clock) = &account_clock {
                        clock.complete();
                    }
                }
            });
        let tx = sender.clone();
        let storage_thread = spawn("storage", sender.clone(), move || {
            while let Ok(job) = storage_rx.recv() {
                let event = match job {
                    Storage::Authorization(deployment) => {
                        Ingress::Authorization(authorization::read(&destination, &deployment))
                    }
                    Storage::Upload { name, key, path } => {
                        let result = (|| {
                            let identity = store::identify(&path)?;
                            destination.put_new(&key, &path, &identity)?;
                            let head = destination
                                .head(&key)?
                                .ok_or("live: uploaded journal missing")?;
                            let bytes = read_key(&destination, &key)?;
                            if head.bytes != identity.bytes
                                || digest(b"", &bytes) != identity.sha256
                            {
                                return Err("live: journal upload identity mismatch".into());
                            }
                            Ok(receipt::Segment {
                                key,
                                sha256: identity.sha256,
                                bytes: identity.bytes,
                            })
                        })();
                        match result {
                            Ok(segment) => Ingress::Uploaded { name, segment },
                            Err(reason) => Ingress::UploadFailed { name, reason },
                        }
                    }
                    Storage::Ledger(events) => Ingress::Ledger(crate::replay::publish_ledger(
                        events.iter().map(|event| Ok(event.to_line())),
                        &local,
                        &destination,
                    )),
                    Storage::Publish { receipt, manifest } => Ingress::Published((|| {
                        publish_record(&local, &destination, &receipt.key(), &receipt.to_json())?;
                        let bytes = serde_json::to_vec(&manifest).map_err(|e| e.to_string())?;
                        let key = format!(
                            "live/{}/final/{}.json",
                            manifest.deployment,
                            digest(b"", &bytes)
                        );
                        publish_record(&local, &destination, &key, &bytes)?;
                        Ok(destination.uri(&key))
                    })(
                    )),
                };
                if tx.send(event).is_err() {
                    return;
                }
            }
        });
        Self {
            ingress,
            sender: Some(sender),
            market: market_tx,
            account,
            storage,
            market_ack,
            account_ack,
            stop,
            brokers: vec![market_thread, account_thread],
            storage_thread: Some(storage_thread),
        }
    }
    pub fn stop_brokers(&self, scheduler: Option<&ReplayClock>) {
        self.stop.store(true, Ordering::SeqCst);
        if let Some(scheduler) = scheduler {
            scheduler.cancel();
        }
        let _ = self.market_ack.send(());
        let _ = self.account_ack.send(());
    }
    pub fn join_brokers(&mut self) -> Result<(), String> {
        let mut failed = false;
        for thread in self.brokers.drain(..) {
            failed |= thread.join().is_err();
        }
        if failed {
            Err("live: broker worker panicked".into())
        } else {
            Ok(())
        }
    }
    pub fn join_storage(&mut self) -> Result<(), String> {
        let (tx, _) = mpsc::channel();
        drop(std::mem::replace(&mut self.storage, tx));
        if let Some(thread) = self.storage_thread.take() {
            thread.join().map_err(|_| "live: storage worker panicked")?;
        }
        Ok(())
    }
}
fn spawn(
    name: &'static str,
    sender: Sender<Ingress>,
    work: impl FnOnce() + Send + 'static,
) -> std::thread::JoinHandle<()> {
    std::thread::spawn(move || {
        if let Err(panic) = std::panic::catch_unwind(std::panic::AssertUnwindSafe(work)) {
            let _ = sender.send(Ingress::WorkerFailed(format!(
                "live: {name} worker panicked"
            )));
            std::panic::resume_unwind(panic);
        }
    })
}
impl Drop for Workers {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        if let Err(error) = self.join_brokers() {
            eprintln!("{error}");
        }
        if let Err(error) = self.join_storage() {
            eprintln!("{error}");
        }
    }
}

fn drain_account(
    options: &mut DerivOptions,
    tx: &Sender<Ingress>,
    ack: &Receiver<()>,
    stopped: &AtomicBool,
) -> bool {
    loop {
        match options.queued_account_event() {
            Ok(Some(event)) => {
                if !deliver(tx, ack, stopped, Ingress::Account(event)) {
                    return false;
                }
            }
            Ok(None) => return true,
            Err(error) => {
                let _ = tx.send(Ingress::WorkerFailed(format!(
                    "live: queued account fact: {error}"
                )));
                return false;
            }
        }
    }
}

#[cfg(test)]
mod regressions {
    use super::*;
    #[test]
    fn shutdown_delivers_consumed_facts_and_panic_senders_disconnect() {
        let (sender, receiver) = mpsc::channel();
        let (_ack, acknowledgements) = mpsc::channel();
        let stopped = AtomicBool::new(true);
        assert!(deliver(
            &sender,
            &acknowledgements,
            &stopped,
            Ingress::AccountFailed("first consumed fact".into())
        ));
        assert!(deliver(
            &sender,
            &acknowledgements,
            &stopped,
            Ingress::AccountFailed("second consumed fact".into())
        ));
        let worker = spawn("market", sender.clone(), || {
            panic!("synthetic worker panic")
        });
        assert!(worker.join().is_err());
        drop(sender);
        assert!(matches!(
            receiver.recv().unwrap(),
            Ingress::AccountFailed(_)
        ));
        assert!(matches!(
            receiver.recv().unwrap(),
            Ingress::AccountFailed(_)
        ));
        assert!(matches!(receiver.recv().unwrap(), Ingress::WorkerFailed(_)));
        assert!(matches!(
            receiver.try_recv(),
            Err(mpsc::TryRecvError::Disconnected)
        ));
    }
}

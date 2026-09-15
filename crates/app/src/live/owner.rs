//! Typed ingress handling and reconciliation on the ordered financial owner.
use super::*;
use std::sync::mpsc::RecvTimeoutError;

impl Runtime {
    pub(super) fn receive(&mut self) -> Result<(), String> {
        let event = match self
            .workers
            .ingress
            .recv_timeout(Duration::from_micros(POLL_MICROS as u64))
        {
            Ok(event) => event,
            Err(RecvTimeoutError::Timeout) => return Ok(()),
            Err(error) => return Err(error.to_string()),
        };
        let market = matches!(event, Ingress::Market(_) | Ingress::MarketFailed(_));
        let account = matches!(
            event,
            Ingress::Account(_) | Ingress::Reply(_) | Ingress::AccountFailed(_)
        );
        let result = self.ingress(event);
        if self.interrupted {
            self.workers.stop_brokers(self.scheduler.as_ref());
            return result;
        }
        if market {
            let _ = self.workers.market_ack.send(());
        }
        if account {
            let _ = self.workers.account_ack.send(());
        }
        result
    }
    pub(super) fn ingress(&mut self, event: Ingress) -> Result<(), String> {
        match event {
            Ingress::Market(event) => self.market_event(event)?,
            Ingress::Account(event) => {
                if let Some(observation) =
                    to_observation(event, &|contract| self.contracts.get(contract).cloned())
                {
                    self.step(vec![observation])?;
                }
            }
            Ingress::Reply(reply) => self.reply(reply)?,
            Ingress::MarketFailed(error) => {
                self.veto("market continuity", true);
                self.veto("causal warmup is incomplete", true);
                self.warm.clear();
                self.health.warmup = false;
                self.record(RecordKind::Discontinuity {
                    reason: error.clone(),
                })?;
                if self.mode == Mode::Replay {
                    self.failure.get_or_insert(error);
                }
            }
            Ingress::AccountFailed(error) => {
                self.veto("account connection unavailable", true);
                self.record(RecordKind::Discontinuity {
                    reason: error.clone(),
                })?;
                if self.mode == Mode::Replay {
                    self.failure.get_or_insert(error);
                }
            }
            Ingress::Uploaded { name, segment } => {
                self.uploads.remove(&name);
                self.upload_errors.remove(&name);
                if !self.segments.iter().any(|s| s.key == segment.key) {
                    self.segments.push(segment);
                }
                if !self.checkpoint(Checkpoint::AfterUploadVerification) {
                    self.journal.mark_uploaded(&name)?;
                    self.journal.remove_uploaded()?;
                }
            }
            Ingress::UploadFailed { name, reason } => {
                self.uploads.remove(&name);
                self.upload_errors.insert(name, reason);
            }
            Ingress::Authorization(result) => {
                self.authorization_pending = false;
                let manifest = &self.definition.manifest;
                let checked = result
                    .and_then(|value| value.ok_or("live authorization is absent".into()))
                    .and_then(|value| {
                        value.validate(
                            &manifest.hash,
                            &manifest.config_hash,
                            &manifest.bundle_sha256,
                            &manifest.broker,
                            &manifest.account,
                        )
                    });
                self.veto("live authorization is absent", checked.is_err());
                self.rows_ready()?;
            }
            Ingress::WorkerFailed(error) => return Err(error),
            Ingress::Lease {
                sent_micros,
                result,
            } => self.lease(sent_micros, result)?,
            Ingress::Ledger(result) => self.ledger_result = Some(result),
            Ingress::Published(result) => self.publication_result = Some(result),
        }
        Ok(())
    }
    fn reply(&mut self, reply: Reply) -> Result<(), String> {
        let (value, timing) = match reply {
            Reply::Completed { value, timing } => (value, timing),
            Reply::Failed {
                intent,
                reason,
                timing,
                rejected,
            } => return self.failed(intent, reason, timing, rejected),
        };
        match value {
            ReplyValue::Subscribed { contract } => {
                if let Some(contract) = contract {
                    self.subscriptions_pending.remove(&contract);
                    self.subscribed.insert(contract.clone());
                    self.veto(
                        &format!("contract subscription unavailable: {contract}"),
                        false,
                    );
                } else {
                    self.veto("transaction subscription unavailable", false);
                }
            }
            ReplyValue::Proposal { binding, proposal } => {
                let observation = self.offer(&binding, proposal)?;
                self.veto(&format!("proposal unavailable: {binding}"), false);
                self.proposal_ready(&binding, observation)?;
            }
            ReplyValue::Prepared { command, encoded } => self.prepared(&command, encoded)?,
            ReplyValue::Written { command, outcome } => self.written(
                &command,
                outcome,
                timing.sent_micros,
                timing.received_micros,
            )?,
            ReplyValue::Balance(balance) => {
                self.balance_pending = false;
                self.health.balance_reconciled = !self.balance_refresh_due
                    && balance.compare(self.engine.accounts()[0].cash)?
                        == std::cmp::Ordering::Equal;
                self.veto(
                    "broker balance differs from assessed or restored cash",
                    !self.health.balance_reconciled,
                );
                // A returned mismatch is a completed startup read, not a reason to poll forever.
                self.veto("balance unavailable", false);
                self.refresh_balance()?;
            }
            ReplyValue::OpenContracts(contracts) => {
                if self.recovery_pending {
                    self.recovery_open = Some(contracts);
                } else {
                    self.veto(
                        "broker portfolio contains an uncorrelated liability",
                        contracts
                            .iter()
                            .any(|c| !self.contracts.contains_key(&c.contract_ref)),
                    );
                }
            }
            ReplyValue::Statement {
                from,
                through,
                rows,
            } => {
                self.recovery_pending = false;
                if let Some(contracts) = self.recovery_open.take() {
                    self.reconcile(&contracts, &rows, from, through, timing.received_micros)?;
                }
            }
        }
        Ok(())
    }
    fn failed(
        &mut self,
        intent: Intent,
        reason: String,
        timing: ReplyTiming,
        rejected: bool,
    ) -> Result<(), String> {
        if !matches!(intent, Intent::Proposal(_)) {
            self.record(RecordKind::Discontinuity {
                reason: reason.clone(),
            })?;
        }
        match intent {
            Intent::Proposal(request) => {
                self.offer(&request.binding, None)?;
                self.record(RecordKind::Refused {
                    binding: request.binding.clone(),
                    proposal: None,
                    reason: reason.clone(),
                })?;
                self.veto(&format!("proposal unavailable: {}", request.binding), true);
                self.compatibility()?;
                self.proposal_ready(&request.binding, None)?;
            }
            Intent::Prepare(prepared) => {
                if let Some(dispatch) = self.dispatches.get(&prepared.command)
                    && let EventKind::Signal { binding, .. } = &dispatch.signal.kind
                {
                    self.veto(&format!("proposal unavailable: {binding}"), true);
                }
                self.not_sent(&prepared.command, "prepare-failed")?;
            }
            Intent::Write(encoded) => {
                self.written(
                    encoded.command(),
                    PurchaseOutcome::PossiblySent {
                        reason: reason.clone(),
                    },
                    timing.sent_micros,
                    timing.received_micros,
                )?;
            }
            Intent::Balance => {
                self.balance_pending = false;
                self.veto("balance unavailable", true);
            }
            Intent::Statement { .. } | Intent::OpenContracts => {
                self.recovery_pending = false;
                self.veto("broker recovery unavailable", true);
            }
            Intent::SubscribeContract(contract) => {
                self.subscriptions_pending.remove(&contract);
                self.veto(
                    &format!("contract subscription unavailable: {contract}"),
                    true,
                );
            }
            Intent::Transactions => self.veto("transaction subscription unavailable", true),
            Intent::Subscribe(..) => self.veto("market continuity", true),
        }
        // A recorded adapter failure invalidates the entire replay, including a prior match.
        if self.mode == Mode::Replay && !rejected {
            self.failure.get_or_insert(reason);
        }
        Ok(())
    }
    fn proposal_ready(
        &mut self,
        binding: &str,
        observation: Option<Observation>,
    ) -> Result<(), String> {
        self.proposals_pending.remove(binding);
        self.prune_rows();
        for rows in &mut self.pending_rows {
            if rows.bindings.remove(binding)
                && let Some(observation) = &observation
            {
                rows.observations.insert(0, observation.clone());
            }
        }
        self.rows_ready()
    }
    pub(super) fn rows_ready(&mut self) -> Result<(), String> {
        self.prune_rows();
        if self.authorization_pending && !self.draining {
            return Ok(());
        }
        while self
            .pending_rows
            .front()
            .is_some_and(|rows| rows.bindings.is_empty())
        {
            let rows = self.pending_rows.pop_front().unwrap();
            self.decision_receipt = Some(rows.receipt_micros);
            for signal in self.step(rows.observations)? {
                self.dispatch(signal)?;
                if self.interrupted {
                    return Ok(());
                }
            }
        }
        Ok(())
    }
    pub(super) fn prune_rows(&mut self) {
        let now = self.clock.now_micros();
        let ages: Vec<_> = self
            .definition
            .definition
            .instruments
            .iter()
            .map(|instrument| {
                self.definition
                    .policy
                    .replay
                    .bindings
                    .iter()
                    .filter(|binding| binding.instrument == instrument.instrument)
                    .filter_map(|binding| {
                        self.definition
                            .policy
                            .replay
                            .risk_policies
                            .iter()
                            .find(|risk| risk.id == binding.risk_policy)
                    })
                    .map(|risk| risk.max_feature_age_micros)
                    .max()
                    .unwrap_or(0)
            })
            .collect();
        self.pending_rows
            .retain(|rows| now.saturating_sub(rows.close_micros) <= ages[rows.instrument]);
    }
    pub(super) fn check_progress(&mut self) -> Result<(), String> {
        if let Some(error) = self
            .failure
            .clone()
            .or_else(|| self.scheduler.as_ref().and_then(ReplayClock::failure))
        {
            return Err(error);
        }
        if renewal_finished(&mut self.renewal)? {
            self.lease(
                self.local_now(),
                Err("lease renewal connection ended".into()),
            )?;
        }
        Ok(())
    }
    pub(super) fn check_replay_progress(&self, generation: Option<u64>) -> Result<(), String> {
        if !self.authorization_pending
            && self.uploads.is_empty()
            && self
                .scheduler
                .as_ref()
                .zip(generation)
                .is_some_and(|(clock, generation)| clock.stalled(generation))
        {
            return Err("live replay: recorded log stalled before all frames and expected writes were consumed".into());
        }
        Ok(())
    }
    pub(super) fn restore_due(&mut self) {
        for record in &self.records {
            match &record.kind {
                RecordKind::Ledger {
                    event:
                        FinancialEvent {
                            kind:
                                EventKind::Confirmed {
                                    command,
                                    expiry_micros: Some(expiry),
                                    ..
                                },
                            ..
                        },
                } => {
                    if let Some(instrument) = self.records.iter().find_map(|r| match &r.kind {
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
                    }) {
                        self.due
                            .entry(command.clone())
                            .or_insert((instrument, *expiry));
                    }
                }
                RecordKind::DueTick { command, .. } => {
                    self.due.remove(command);
                }
                _ => {}
            }
        }
        self.due.retain(|command, _| !self.records.iter().any(|record| matches!(&record.kind, RecordKind::DueTick {command: completed, ..} if completed == command)));
    }
    pub(super) fn resolve_not_sent(&mut self, command: &str, prefix: &str) -> Result<(), String> {
        self.step(vec![Observation::Reconciliation {
            command: command.into(),
            source: self.source(&format!("{prefix}:{command}")),
            resolution: Resolution::NotSent,
        }])?;
        Ok(())
    }
    pub(super) fn refresh_claims(&mut self) -> Result<(), String> {
        if !self.claims_restored {
            return self.recover();
        }
        self.control.advance_to(self.clock.now_micros());
        let remote = self.control.unresolved(LeaseKey {
            broker: &self.health.broker,
            account: &self.health.account,
        });
        let remote = match remote {
            Ok(remote) => {
                self.veto("claim recovery unavailable", false);
                remote
            }
            Err(error) => {
                self.veto("claim recovery unavailable", true);
                self.record(RecordKind::Discontinuity {
                    reason: format!("claim recovery unavailable: {error}"),
                })?;
                return Ok(());
            }
        };
        for claim in remote {
            let Some(local) = self.claims.get(&claim.command) else {
                continue;
            };
            if claim.deployment != self.definition.deployment
                || local.state != ClaimState::PossiblySent
            {
                continue;
            }
            if claim.state == ClaimState::NotSent {
                self.resolve_not_sent(&claim.command, "operator-not-sent")?;
            } else if claim.state == ClaimState::Accepted {
                let local = self.claims.get_mut(&claim.command).unwrap();
                local.contract_ref = claim.contract_ref;
                local.transaction_ref = claim.transaction_ref;
            }
        }
        Ok(())
    }
    fn command_window(&self, signal: &FinancialEvent) -> Option<(i64, i64)> {
        let EventKind::Signal {
            proposal: Some(proposal),
            binding,
            ..
        } = &signal.kind
        else {
            return None;
        };
        let bound = self
            .definition
            .policy
            .replay
            .bindings
            .iter()
            .find(|b| b.id == *binding)?;
        let risk = self
            .definition
            .policy
            .replay
            .risk_policies
            .iter()
            .find(|r| r.id == bound.risk_policy)?;
        let start = signal.time_micros.div_euclid(1_000_000) * 1_000_000;
        let end = start
            .checked_add(risk.max_proposal_age_micros?)?
            .checked_add(proposal.terms.duration_micros)?
            .checked_add(proposal.terms.settlement.max_settlement_delay_micros)?;
        Some((start, end))
    }
    pub(super) fn has_unpaid_loss(&self) -> bool {
        !self.unpaid_losses().is_empty()
    }
    fn unpaid_losses(&self) -> BTreeMap<String, (String, i64)> {
        let mut losses = BTreeMap::new();
        for record in &self.records {
            if let RecordKind::Ledger { event } = &record.kind {
                match &event.kind {
                    EventKind::Unresolved {
                        command,
                        terminal: Some(terminal),
                        ..
                    } if terminal.status == TerminalStatus::Lost => {
                        if let Some(contract) = self
                            .contracts
                            .iter()
                            .find_map(|(contract, c)| (c == command).then_some(contract.clone()))
                        {
                            let expiry = self
                                .records
                                .iter()
                                .filter_map(|r| match &r.kind {
                                    RecordKind::Ledger {
                                        event:
                                            FinancialEvent {
                                                kind:
                                                    EventKind::Confirmed {
                                                        command: c,
                                                        expiry_micros: Some(expiry),
                                                        ..
                                                    },
                                                ..
                                            },
                                    } if c == command => Some(*expiry),
                                    _ => None,
                                })
                                .next();
                            if let Some(expiry) = expiry {
                                losses.insert(command.clone(), (contract, expiry));
                            }
                        }
                    }
                    EventKind::Settled { command, .. }
                    | EventKind::Reconciled {
                        command,
                        resolution: Resolution::Settled { .. },
                        ..
                    } => {
                        losses.remove(command);
                    }
                    _ => {}
                }
            }
        }
        losses
    }
    pub(super) fn request_reconciliation(&mut self) -> Result<(), String> {
        if self.recovery_pending {
            return Ok(());
        }
        let from = self
            .claims
            .values()
            .filter(|c| c.deployment == self.definition.deployment)
            .map(|c| c.signal.time_micros.div_euclid(1_000_000))
            .chain(
                self.unpaid_losses()
                    .values()
                    .map(|(_, expiry)| expiry.div_euclid(1_000_000)),
            )
            .min()
            .unwrap_or(self.clock.now_micros().div_euclid(1_000_000));
        self.recovery_pending = true;
        self.send(Intent::OpenContracts)?;
        self.send(Intent::Statement {
            from,
            through: self.clock.now_micros().div_euclid(1_000_000),
        })
    }
    fn reconcile(
        &mut self,
        contracts: &[OpenContract],
        rows: &[StatementRow],
        from: i64,
        through: i64,
        received: i64,
    ) -> Result<(), String> {
        self.veto("broker recovery unavailable", false);
        let observed_now = self
            .control_time
            .max(self.last_provider_time.unwrap_or(i64::MIN));
        for claim in self.claims.values().cloned().collect::<Vec<_>>() {
            if claim.deployment != self.definition.deployment
                || !matches!(
                    claim.state,
                    ClaimState::Claimed | ClaimState::PossiblySent | ClaimState::Accepted
                )
            {
                continue;
            }
            if self
                .contracts
                .values()
                .any(|command| *command == claim.command)
            {
                continue;
            }
            let EventKind::Signal {
                proposal: Some(proposal),
                ..
            } = &claim.signal.kind
            else {
                continue;
            };
            let Some((start, end)) = self.command_window(&claim.signal) else {
                continue;
            };
            let uncorrelated = |contract: &str| {
                !self.contracts.contains_key(contract)
                    && !self.claims.values().any(|c| {
                        c.command != claim.command && c.contract_ref.as_deref() == Some(contract)
                    })
            };
            let matches = |symbol: &str, direction, price: Decimal, at| {
                symbol
                    == proposal
                        .instrument
                        .split_once(':')
                        .map(|(_, symbol)| symbol)
                        .unwrap_or("")
                    && direction == proposal.terms.direction
                    && price.compare(proposal.terms.quoted_cost) == Ok(std::cmp::Ordering::Equal)
                    && start <= at
                    && at <= end
            };
            let mut candidates = BTreeMap::new();
            for contract in contracts {
                if uncorrelated(&contract.contract_ref)
                    && matches(
                        &contract.instrument,
                        contract.direction,
                        contract.buy_price,
                        contract.purchase_time_micros,
                    )
                    && claim
                        .contract_ref
                        .as_deref()
                        .is_none_or(|id| id == contract.contract_ref)
                    && claim
                        .transaction_ref
                        .as_deref()
                        .is_none_or(|id| id == contract.transaction_ref)
                {
                    candidates.insert(
                        contract.contract_ref.clone(),
                        (
                            contract.buy_price,
                            BrokerLiability {
                                contract_ref: contract.contract_ref.clone(),
                                transaction_ref: contract.transaction_ref.clone(),
                                purchase_time_micros: contract.purchase_time_micros,
                                expected_start_micros: contract.start_micros,
                                payout: contract.payout,
                            },
                        ),
                    );
                }
            }
            for row in rows {
                if let Some((debit, liability)) = recover_purchase(row) {
                    let references =
                        claim.transaction_ref.as_deref() == Some(&liability.transaction_ref);
                    let economics = row.instrument.as_deref().zip(row.direction).is_some_and(
                        |(symbol, direction)| {
                            matches(symbol, direction, debit, liability.purchase_time_micros)
                        },
                    );
                    if uncorrelated(&liability.contract_ref)
                        && (references || economics)
                        && claim
                            .contract_ref
                            .as_deref()
                            .is_none_or(|id| id == liability.contract_ref)
                        && claim
                            .transaction_ref
                            .as_deref()
                            .is_none_or(|id| id == liability.transaction_ref)
                    {
                        candidates
                            .entry(liability.contract_ref.clone())
                            .or_insert((debit, liability));
                    }
                }
            }
            if candidates.len() == 1 {
                let (debit, liability) = candidates.into_values().next().unwrap();
                self.step(vec![Observation::Reconciliation {
                    command: claim.command.clone(),
                    source: self.source(&format!("recovery-purchase:{}", claim.claim)),
                    resolution: Resolution::Purchased {
                        debit,
                        liability: liability.clone(),
                    },
                }])?;
                self.contracts
                    .insert(liability.contract_ref.clone(), claim.command.clone());
                self.update_claim(
                    &claim.command,
                    ClaimState::Accepted,
                    Some(&liability.contract_ref),
                    Some(&liability.transaction_ref),
                )?;
                self.subscribe_contract(&liability.contract_ref)?;
            } else if let Some(local) = self.claims.get_mut(&claim.command) {
                local.state = ClaimState::PossiblySent;
            }
        }
        for row in rows {
            if let Some(observation) = to_observation(
                AccountEvent::Cash {
                    fact: row.cash.clone(),
                    receipt_micros: row.receipt_micros,
                },
                &|_| None,
            ) {
                self.step(vec![observation])?;
            }
        }
        for (command, (contract, expiry)) in self.unpaid_losses() {
            let Some(signal) = self.records.iter().find_map(|r| match &r.kind { RecordKind::Ledger {event} if matches!(&event.kind,EventKind::Signal {command:Some(c),..} if *c == command) => Some(event), _ => None }) else {continue;};
            let EventKind::Signal {
                proposal: Some(proposal),
                ..
            } = &signal.kind
            else {
                continue;
            };
            let horizon =
                expiry.saturating_add(proposal.terms.settlement.max_settlement_delay_micros);
            if observed_now < horizon
                || through * 1_000_000 < horizon
                || from * 1_000_000 > expiry
                || rows.iter().any(|row| {
                    row.cash.action == CashAction::Sell
                        && row.cash.contract_ref.as_deref() == Some(&contract)
                })
            {
                continue;
            }
            let mut source = self.source(&format!("zero-credit:{command}:{expiry}"));
            source.available_at_micros = received;
            self.step(vec![Observation::Reconciliation {
                command,
                source,
                resolution: Resolution::Settled {
                    outcome: Outcome::Loss,
                    gross_return: Decimal::zero(0),
                    terminal_fee: Decimal::zero(0),
                },
            }])?;
        }
        self.veto(
            "broker portfolio contains an uncorrelated liability",
            contracts
                .iter()
                .any(|c| !self.contracts.contains_key(&c.contract_ref)),
        );
        self.entry_gates()
    }
}

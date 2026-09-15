use crate::broker::deriv::{Balance, ContractAvailability, DerivAccounts, DerivOptions};
use crate::broker::transport::{WebSocketConnector, endpoint_host};
use crate::broker::{
    self, AccountEvent, AccountIdentity, Adapter, Cancellation, Clock, Continuity,
    DiscoveredInstrument, LiveEvent, ProposalRequest, SystemClock,
};
use crate::store::Store;
use binary_alpha_engine::config::{Broker, Config};
use binary_alpha_engine::dataset::{NativeGranularity, object_key};
use binary_alpha_engine::execution::{
    ContractSemantics, Decimal, Direction, Settlement, SettlementRule,
};
use binary_alpha_engine::market::{InstrumentId, format_event_time_micros};
use serde::Serialize;
use std::collections::BTreeMap;
use std::io::Write;
use std::path::Path;

#[derive(Debug, Serialize)]
pub struct InspectionReport {
    pub broker: String,
    pub kind: String,
    pub endpoint_host: String,
    pub started: String,
    pub checks: Vec<InspectionCheck>,
}
#[derive(Debug, Serialize)]
pub struct InspectionCheck {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub instrument: Option<String>,
    pub result: String,
    pub detail: InspectionDetail,
}
#[derive(Debug, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum InspectionDetail {
    Unavailable,
    Discovery {
        instruments: Vec<DiscoveredInstrument>,
    },
    Contracts {
        contracts: Vec<ContractAvailability>,
    },
    History {
        rows: usize,
        first: Option<String>,
        last: Option<String>,
    },
    Live {
        counts: BTreeMap<String, u64>,
    },
    Cancellation {
        control: String,
        counts: BTreeMap<String, u64>,
    },
    Balance {
        balance: Balance,
    },
    TransactionAcknowledged,
    Proposal {
        direction: Direction,
        ask_price: Decimal,
        payout: Decimal,
        spot: String,
        spot_time: String,
        longcode: String,
    },
    AccountClass {
        account_class: String,
    },
}
impl InspectionReport {
    fn check(
        &mut self,
        name: &str,
        instrument: Option<String>,
        result: Result<InspectionDetail, String>,
    ) {
        let (result, detail) = match result {
            Ok(detail) => (
                if matches!(&detail, InspectionDetail::History { rows: 0, .. }) {
                    "observed: empty history page"
                } else {
                    "verified"
                }
                .into(),
                detail,
            ),
            Err(error) => (
                format!("unavailable: {error}"),
                InspectionDetail::Unavailable,
            ),
        };
        self.checks.push(InspectionCheck {
            name: name.into(),
            instrument,
            result,
            detail,
        });
    }
}

pub fn run(config_path: &Path, out: &mut dyn Write) -> Result<(), String> {
    let config = crate::load_config(config_path)?;
    config
        .history
        .as_ref()
        .ok_or("inspect: the configuration declares no history table")?;
    config
        .inspect
        .as_ref()
        .ok_or("inspect: the configuration declares no inspect table")?;
    let mut adapter = broker::connect(&config)?;
    let mut report = observe(&config, &mut adapter, &mut SystemClock)?;
    let history = config.history.as_ref().expect("checked history");
    let settings = config
        .brokers
        .iter()
        .find(|broker| broker.id() == &history.broker)
        .expect("validated broker");
    match settings {
        Broker::Deriv(settings) if settings.credential.is_some() => {
            let authenticated = (|| {
                let connector = WebSocketConnector::new()?;
                let mut http = connector.http();
                let address = DerivAccounts::resolve(settings, &mut http)?;
                let account = AccountIdentity {
                    broker: settings.id.clone(),
                    account: "inspection".into(),
                    class: address.account_class,
                    currency: address.currency.clone(),
                };
                let instruments = history
                    .instruments
                    .iter()
                    .map(|symbol| {
                        let id = InstrumentId {
                            broker: history.broker.clone(),
                            provider_symbol: symbol.clone(),
                        };
                        let scale = config
                            .instrument(&id, NativeGranularity::Tick)
                            .expect("validated instrument")
                            .price_scale;
                        (id, scale)
                    })
                    .collect::<Vec<_>>();
                DerivOptions::connect(
                    address,
                    account,
                    &instruments,
                    Box::new(connector),
                    Box::new(SystemClock),
                    settings.budgets.clone().unwrap_or_default(),
                )
            })();
            match authenticated {
                Ok(mut authenticated) => observe_account(&config, &mut authenticated, &mut report),
                Err(error) => report.check("authentication", None, Err(error)),
            }
        }
        Broker::PocketOption(settings) => report.check(
            "authentication",
            None,
            Ok(InspectionDetail::AccountClass {
                account_class: settings.account_class.to_string(),
            }),
        ),
        _ => (),
    }
    let local = Store::filesystem(
        config_path
            .parent()
            .unwrap_or(Path::new("."))
            .join(config.storage.historical_data_dir.as_path()),
    );
    let destination = Store::open(&config.storage.publication_uri)?;
    publish(&report, &local, &destination, out)
}

/// Checks authenticated economics without preparing or purchasing an order.
pub fn observe_account(config: &Config, options: &mut DerivOptions, report: &mut InspectionReport) {
    let balance = options.balance().map(|amount| InspectionDetail::Balance {
        balance: Balance {
            amount,
            currency: options.account().currency.clone(),
            account_class: options.account().class,
        },
    });
    let ready = balance.is_ok();
    report.check("authentication", None, balance);
    if !ready {
        return;
    }
    let acknowledgement = (|| {
        options.subscribe_transactions()?;
        match options.next_account_event(20_000_000)? {
            Some(AccountEvent::TransactionAcknowledged) => {
                Ok(InspectionDetail::TransactionAcknowledged)
            }
            _ => Err("deriv transaction: subscription acknowledgement missing".into()),
        }
    })();
    let ready = acknowledgement.is_ok();
    report.check("transactions", None, acknowledgement);
    if !ready {
        return;
    }
    let Some(proposal) = config
        .inspect
        .as_ref()
        .and_then(|inspect| inspect.proposal.as_ref())
    else {
        return;
    };
    let history = config.history.as_ref().expect("validated history");
    for symbol in &history.instruments {
        let instrument = InstrumentId {
            broker: history.broker.clone(),
            provider_symbol: symbol.clone(),
        };
        let definition = config
            .instrument(&instrument, NativeGranularity::Tick)
            .expect("validated instrument");
        for direction in [Direction::Buy, Direction::Sell] {
            let request = ProposalRequest {
                binding: format!("inspection:{instrument}:{direction}"),
                instrument: instrument.clone(),
                scale: definition.price_scale,
                direction,
                duration_seconds: proposal.duration_seconds,
                stake: proposal.stake,
                currency: options.account().currency.clone(),
                semantics: ContractSemantics::RiseFallStrictV1,
                settlement: Settlement {
                    rule: SettlementRule::BrokerAuthoritativeV1,
                    max_settlement_delay_micros: 0,
                    max_tick_gap_micros: 0,
                },
            };
            let result = options.proposal(&request).map(|quote| {
                let (spot, longcode) = options
                    .proposal_details(&quote.identity)
                    .expect("issued proposal");
                InspectionDetail::Proposal {
                    direction,
                    ask_price: quote.terms.quoted_cost,
                    payout: quote.terms.win.gross_return,
                    spot: spot.into(),
                    spot_time: format_event_time_micros(quote.spot_time_micros),
                    longcode: longcode.into(),
                }
            });
            report.check("proposal", Some(instrument.to_string()), result);
        }
    }
}

/// Runs the finite market checks against the same adapter used by fetch.
pub fn observe(
    config: &Config,
    adapter: &mut Adapter,
    clock: &mut dyn Clock,
) -> Result<InspectionReport, String> {
    let history = config
        .history
        .as_ref()
        .ok_or("inspect: the configuration declares no history table")?;
    let inspect = config
        .inspect
        .as_ref()
        .ok_or("inspect: the configuration declares no inspect table")?;
    let settings = config
        .brokers
        .iter()
        .find(|broker| broker.id() == &history.broker)
        .ok_or("inspect: broker is not declared")?;
    let mut report = InspectionReport {
        broker: history.broker.to_string(),
        kind: settings.kind().to_string(),
        endpoint_host: endpoint_host(settings.endpoint())?,
        started: format_event_time_micros(clock.now_micros()),
        checks: Vec::new(),
    };
    report.check(
        "discovery",
        None,
        adapter
            .market()
            .discover()
            .map(|instruments| InspectionDetail::Discovery { instruments }),
    );
    let instruments: Vec<_> = history
        .instruments
        .iter()
        .map(|symbol| InstrumentId {
            broker: history.broker.clone(),
            provider_symbol: symbol.clone(),
        })
        .collect();
    for instrument in &instruments {
        if let Adapter::Deriv(broker) = adapter {
            report.check(
                "contracts",
                Some(instrument.to_string()),
                broker
                    .contracts_for(instrument.provider_symbol.as_str())
                    .map(|contracts| InspectionDetail::Contracts { contracts }),
            );
        }
        let definition = config
            .instrument(instrument, NativeGranularity::Tick)
            .ok_or("inspect: instrument is not declared")?;
        report.check("history", Some(instrument.to_string()), {
            let market = adapter.market();
            market
                .history_page(
                    instrument,
                    definition.price_scale,
                    None,
                    NativeGranularity::Tick,
                )
                .and_then(|page| {
                    market.decode_history(
                        instrument,
                        &page.raw,
                        definition.price_scale,
                        NativeGranularity::Tick,
                    )
                })
                .map(|(_, rows)| InspectionDetail::History {
                    rows: rows.len(),
                    first: rows.first_time_micros().map(format_event_time_micros),
                    last: rows.last_time_micros().map(format_event_time_micros),
                })
        });
    }
    let mut subscribed = Vec::new();
    for instrument in &instruments {
        let definition = config
            .instrument(instrument, NativeGranularity::Tick)
            .expect("checked instrument");
        match adapter
            .market()
            .subscribe(instrument, definition.price_scale)
        {
            Ok(()) => subscribed.push(instrument.clone()),
            Err(error) => report.check("subscribe", Some(instrument.to_string()), Err(error)),
        }
    }
    let mut continuity = Continuity::default();
    let duration = i64::from(inspect.live_seconds) * 1_000_000;
    let live = collect(
        adapter,
        &subscribed,
        clock,
        duration,
        Some(u64::from(inspect.live_observations)),
        &mut continuity,
        None,
    );
    let incomplete = live.as_ref().is_ok_and(|counts| {
        counts.len() != instruments.len()
            || counts
                .values()
                .any(|count| *count < u64::from(inspect.live_observations))
    });
    report.check(
        "live",
        None,
        live.map(|counts| InspectionDetail::Live { counts }),
    );
    if incomplete {
        report.checks.last_mut().expect("live check").result =
            "observed: live deadline reached before row target".into();
    }
    if let Some(last) = subscribed.last() {
        let control = adapter.market().unsubscribe(last);
        match control {
            Err(error) => report.check("cancellation", Some(last.to_string()), Err(error)),
            Ok(control) => {
                let after = clock.now_micros();
                let before_counts = match adapter {
                    Adapter::PocketOption(broker) => Some(broker.received_counts().clone()),
                    _ => None,
                };
                let counts = collect(
                    adapter,
                    &subscribed,
                    clock,
                    duration,
                    None,
                    &mut continuity,
                    Some(after),
                );
                let counts = counts.map(|mut counts| {
                    if let (Some(before), Adapter::PocketOption(broker)) =
                        (before_counts, &*adapter)
                    {
                        for instrument in &subscribed {
                            let symbol = instrument.provider_symbol.as_str();
                            counts.insert(
                                instrument.to_string(),
                                broker.received_counts().get(symbol).copied().unwrap_or(0)
                                    - before.get(symbol).copied().unwrap_or(0),
                            );
                        }
                    }
                    counts
                });
                let control_text = match control {
                    Cancellation::Acknowledged => "acknowledged",
                    Cancellation::SentWithoutAcknowledgement => "sent_without_acknowledgement",
                };
                report.check(
                    "cancellation",
                    Some(last.to_string()),
                    counts.map(|counts| InspectionDetail::Cancellation {
                        control: control_text.into(),
                        counts,
                    }),
                );
                if let Some(check) = report.checks.last_mut()
                    && !check.result.starts_with("unavailable")
                {
                    check.result = format!("observed: {control_text}");
                }
            }
        }
    }
    Ok(report)
}
fn collect(
    adapter: &mut Adapter,
    instruments: &[InstrumentId],
    clock: &mut dyn Clock,
    duration: i64,
    maximum: Option<u64>,
    continuity: &mut Continuity,
    after: Option<i64>,
) -> Result<BTreeMap<String, u64>, String> {
    let mut counts: BTreeMap<String, u64> = instruments
        .iter()
        .map(|instrument| (instrument.to_string(), 0))
        .collect();
    let deadline = clock.now_micros().saturating_add(duration);
    while !counts.is_empty()
        && maximum.is_none_or(|limit| counts.values().any(|count| *count < limit))
    {
        let remaining = deadline.saturating_sub(clock.now_micros());
        if remaining <= 0 {
            break;
        }
        match adapter.market().next_live(remaining)? {
            None => break,
            Some(LiveEvent::Break { reason, .. }) => {
                return Err(format!("continuity break: {reason}"));
            }
            Some(LiveEvent::Observation(observation)) => {
                continuity.accept(&observation)?;
                if after.is_none_or(|after| observation.receipt_micros >= after) {
                    let count = counts
                        .get_mut(&observation.instrument.to_string())
                        .ok_or("inspect: unexpected live instrument")?;
                    *count += 1;
                }
            }
        }
    }
    Ok(counts)
}

pub fn publish(
    report: &InspectionReport,
    local: &Store,
    destination: &Store,
    out: &mut dyn Write,
) -> Result<(), String> {
    let bytes = crate::fetch::json_bytes(report)?;
    let identity = crate::import::retain_bytes(local, &bytes, "inspection")?;
    let key = object_key(&identity.sha256);
    let path = local
        .local_path(&key)
        .ok_or("inspect: local retention must be a filesystem store")?;
    let inspection = format!("inspections/{}-{}.json", report.started, report.broker);
    binary_alpha_engine::config::relative_path(&inspection)?;
    local.put_new(&inspection, &path, &identity)?;
    destination.put_new(&key, &path, &identity)?;
    for check in &report.checks {
        writeln!(
            out,
            "inspect {} {} {} {}",
            report.broker,
            check.name,
            check.instrument.as_deref().unwrap_or("all"),
            check.result
        )
        .map_err(|error| format!("cannot write inspection: {error}"))?;
    }
    out.write_all(&bytes)
        .map_err(|error| format!("cannot write inspection: {error}"))?;
    writeln!(out, "inspection {}", destination.uri(&key))
        .map_err(|error| format!("cannot write inspection: {error}"))
}

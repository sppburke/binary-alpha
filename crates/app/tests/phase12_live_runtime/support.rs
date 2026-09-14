//! Synthetic bundle and recorded-transport helpers for the live runtime scenario modules.
#![allow(dead_code)]

use crate::common::Scratch;
use crate::fixture_config::{self as shared, BASE, CANDLE, HOUR, ROWS, time, uri};
use binary_alpha_app::{live, store::Store};
use binary_alpha_engine::config::Config;
use binary_alpha_engine::dataset::{DatasetRole, GenerationManifest, manifest_key};
use binary_alpha_engine::research::{self, Declaration, Grant, Population, Run, RunManifest};
use serde_json::{Value, json};
use std::fs;
use std::path::{Path, PathBuf};

pub const START: i64 = BASE + 5 * HOUR + (ROWS as i64 + 1) * CANDLE;
pub const PLANTED: [u8; 4] = [0b0011_1111, 0b0000_0011, 0b0001_1111, 0b0000_1111];

pub fn write(path: &Path, bytes: impl AsRef<[u8]>) {
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    fs::write(path, bytes).unwrap();
}
pub fn cli(log: &Path, args: &[&str]) -> Result<String, String> {
    crate::common::cli_as(log, "synthetic-operator", args).map_err(|s| s.trim_end().into())
}
pub fn object(root: &Path, generation: &str, path: &str) -> Vec<u8> {
    let manifest: Value = serde_json::from_slice(
        &fs::read(root.join("published").join(manifest_key(generation))).unwrap(),
    )
    .unwrap();
    let object = manifest["objects"]
        .as_array()
        .unwrap()
        .iter()
        .find(|o| o["path"] == path)
        .unwrap();
    fs::read(root.join("published").join(object["key"].as_str().unwrap())).unwrap()
}

pub struct Fixture {
    pub scratch: Scratch,
    pub config: Config,
    pub path: PathBuf,
    pub bundle: RunManifest,
    pub run: Run,
    pub datasets: Vec<GenerationManifest>,
}
impl Fixture {
    pub fn new(name: &str) -> Self {
        Self::with_instruments(name, 1)
    }
    pub fn two(name: &str) -> Self {
        Self::with_instruments(name, 2)
    }
    fn with_instruments(name: &str, count: usize) -> Self {
        Self::with_account(name, count, "a0")
    }
    /// Namespaces real database scenarios without changing their frozen logical account binding.
    pub fn for_account(name: &str, account: &str) -> Self {
        Self::with_account(name, 1, account)
    }
    fn with_account(name: &str, count: usize, account: &str) -> Self {
        let scratch = Scratch::new(name);
        let mut value = serde_json::to_value(shared::configuration(&scratch.root)).unwrap();
        value["instruments"].as_array_mut().unwrap().truncate(1);
        let instrument = &mut value["instruments"][0];
        instrument["broker"] = json!("deriv");
        instrument["provider_symbol"] = json!("R_50");
        instrument["quote_currency"] = json!("USD");
        instrument["price_scale"] = json!(4);
        let baseline = json!({"id":"0-small","direction":"buy","duration_micros":5_000_000,"currency":"USD",
            "stake":"10","quoted_cost":"10","entry_fee":"0","win":{"gross_return":"18.83","terminal_fee":"0"},
            "loss":{"gross_return":"0","terminal_fee":"0"},"tie":{"gross_return":"0","terminal_fee":"0"},
            "settlement":{"rule":"price_at_due_v1","max_settlement_delay_micros":2_000_000,"max_tick_gap_micros":2_000_000}});
        let envelope = json!({"max_purchase_cost":"10","max_entry_fee":"0","max_win_terminal_fee":"0",
            "max_loss_terminal_fee":"0","max_tie_terminal_fee":"0","min_winning_net_return":"8.83","settlement_rule":"price_at_due_v1"});
        let research = &mut value["research"];
        research["instruments"].as_array_mut().unwrap().truncate(1);
        research["instruments"][0]["instrument"] = json!("deriv:R_50");
        let search = &mut research["instruments"][0]["search"];
        search["account"]["broker"] = json!("deriv");
        search["account"]["currency"] = json!("USD");
        search["contracts"] = json!([baseline.clone()]);
        search["envelope"] = envelope.clone();
        search["risk_policy"]["max_proposal_age_micros"] = json!(1_000_000);
        research["folds"][0]["inputs"]
            .as_array_mut()
            .unwrap()
            .truncate(1);
        research["refit"]["fits"]
            .as_array_mut()
            .unwrap()
            .truncate(1);
        research["evaluation"]["inputs"]
            .as_array_mut()
            .unwrap()
            .truncate(1);
        research["holdout"]["inputs"]
            .as_array_mut()
            .unwrap()
            .truncate(1);
        let portfolio = &mut research["portfolio"];
        portfolio["accounts"].as_array_mut().unwrap().truncate(1);
        portfolio["accounts"][0]["broker"] = json!("deriv");
        portfolio["accounts"][0]["currency"] = json!("USD");
        portfolio["reporting_currency"] = json!("USD");
        portfolio["rates"] = json!([]);
        portfolio["bindings"] = json!([{"id":"b0","account":"a0","instrument":"deriv:R_50","alternatives":[{"contract":baseline,"envelope":envelope}]}]);
        portfolio["members"] = json!([{"family":0,"member":0}]);
        portfolio["subsets"] = json!([{"deployments":[{"member":0,"repair":1,"binding":0}]}]);
        portfolio["risk_policies"][0]["max_proposal_age_micros"] = json!(1_000_000);
        let mut worse = baseline.clone();
        worse["win"]["gross_return"] = json!("18.80");
        let mut worse_envelope = envelope.clone();
        worse_envelope["min_winning_net_return"] = json!("8.80");
        research["scenarios"] = json!([
            {"id":"delayed","acceptance_delay_micros":100_000,"alternatives":[{"binding":"b0","contract":baseline,"envelope":envelope}]},
            {"id":"worse_terms","acceptance_delay_micros":0,"alternatives":[{"binding":"b0","contract":worse,"envelope":worse_envelope}]}]);
        if count == 2 {
            let mut second = value["instruments"][0].clone();
            second["provider_symbol"] = json!("R_100");
            value["instruments"].as_array_mut().unwrap().push(second);
            let r = &mut value["research"];
            let mut second = r["instruments"][0].clone();
            second["instrument"] = json!("deriv:R_100");
            r["instruments"].as_array_mut().unwrap().push(second);
            for path in ["refit", "evaluation", "holdout"] {
                let key = if path == "refit" { "fits" } else { "inputs" };
                let second = r[path][key][0].clone();
                r[path][key].as_array_mut().unwrap().push(second);
            }
            let second = r["folds"][0]["inputs"][0].clone();
            r["folds"][0]["inputs"].as_array_mut().unwrap().push(second);
            let mut second = r["portfolio"]["bindings"][0].clone();
            second["id"] = json!("b1");
            second["instrument"] = json!("deriv:R_100");
            r["portfolio"]["bindings"]
                .as_array_mut()
                .unwrap()
                .push(second);
            r["portfolio"]["members"] = json!([{"family":0,"member":0},{"family":1,"member":0}]);
            r["portfolio"]["subsets"] = json!([{"deployments":[{"member":0,"repair":1,"binding":0},{"member":1,"repair":1,"binding":1}]}]);
            for scenario in r["scenarios"].as_array_mut().unwrap() {
                let mut second = scenario["alternatives"][0].clone();
                second["binding"] = json!("b1");
                scenario["alternatives"]
                    .as_array_mut()
                    .unwrap()
                    .push(second);
            }
        }
        value["research"]["portfolio"]["accounts"][0]["id"] = json!(account);
        for binding in value["research"]["portfolio"]["bindings"]
            .as_array_mut()
            .unwrap()
        {
            binding["account"] = json!(account);
        }
        let mut config: Config = serde_json::from_value(value).unwrap();
        let mut datasets = Vec::new();
        let mut populations = Vec::new();
        for (hour, name, role) in [
            (0, "source", DatasetRole::Development),
            (1, "assessment", DatasetRole::Development),
            (2, "refit", DatasetRole::Development),
            (3, "evaluation", DatasetRole::Evaluation),
            (4, "holdout", DatasetRole::Holdout),
        ] {
            let imported = shared::import_ticks(
                &scratch.root,
                name,
                role,
                "deriv",
                &["R_50", "R_100"][..count],
                &vec![4; count],
                &vec![
                    shared::ticks_at_scale(BASE + hour * HOUR, &shared::recipe(PLANTED), 4);
                    count
                ],
            );
            for (i, manifest) in imported.into_iter().enumerate() {
                populations.push(Population {
                    id: format!("{name}-{i}"),
                    role,
                    instrument: manifest.instrument.clone(),
                    source: "invented-quarter-second-ticks-v1".into(),
                    coverage: manifest.coverage.clone(),
                    generations: vec![manifest.generation.clone()],
                    tokens: vec![format!("{name}-{i}-a"), format!("{name}-{i}-b")],
                    exposure: vec![],
                });
                datasets.push(manifest);
            }
        }
        let r = config.research.as_mut().unwrap();
        for i in 0..count {
            r.instruments[i].source_manifest = uri(&scratch.root, &datasets[i].generation);
            r.folds[0].inputs[i].fit_manifest = uri(&scratch.root, &datasets[i].generation);
            r.folds[0].inputs[i].assessment_manifest =
                uri(&scratch.root, &datasets[count + i].generation);
            r.refit.fits[i] = uri(&scratch.root, &datasets[2 * count + i].generation);
            r.evaluation.inputs[i] = uri(&scratch.root, &datasets[3 * count + i].generation);
            r.holdout.inputs[i] = uri(&scratch.root, &datasets[4 * count + i].generation);
        }
        let declaration = Declaration {
            schema_version: 1,
            operator: "synthetic-operator".into(),
            root: format!("file://{}/governance", scratch.root.display())
                .parse()
                .unwrap(),
            namespace: "phase12".into(),
            populations,
        };
        write(
            &scratch.path("declaration.json"),
            research::to_json(&declaration),
        );
        let path = scratch.path("research.toml");
        write(&path, config.canonical_toml());
        let log = scratch.path("access.log");
        let report = cli(
            &log,
            &["research", "run", "--config", path.to_str().unwrap()],
        )
        .unwrap();
        assert!(
            report.contains("awaiting_holdout_authorization"),
            "{report}"
        );
        let generation = research::run_generation_id(
            &config.content_hash(),
            binary_alpha_app::import::CODE_REVISION,
            &declaration.identity(),
        );
        let bundle = RunManifest::from_json(
            &fs::read(scratch.path("published").join(manifest_key(&generation))).unwrap(),
        )
        .unwrap();
        let run = Run::from_json(&object(&scratch.root, &generation, "research.json")).unwrap();
        let mut grant_args = vec![
            "holdout".to_string(),
            "grant".into(),
            "create".into(),
            "--config".into(),
            path.to_str().unwrap().into(),
            "--bundle-manifest".into(),
            uri(&scratch.root, &generation).to_string(),
        ];
        for i in 0..count {
            grant_args.extend([
                "--holdout-manifest".into(),
                uri(&scratch.root, &datasets[4 * count + i].generation).to_string(),
            ]);
        }
        grant_args.extend([
            "--reason".into(),
            "synthetic runtime integration fixture".into(),
        ]);
        cli(
            &log,
            &grant_args.iter().map(String::as_str).collect::<Vec<_>>(),
        )
        .unwrap();
        let grant = Grant::from_json(
            &fs::read(
                scratch
                    .path("governance")
                    .join(declaration.key(&research::grant_key(&generation))),
            )
            .unwrap(),
        )
        .unwrap();
        let report = cli(
            &log,
            &["research", "run", "--config", path.to_str().unwrap()],
        )
        .unwrap();
        assert!(report.contains("certified"), "{report}");
        let cert = research::certification_generation_id(&generation, &grant.hash);
        let mut warmup_ticks = shared::ticks_at_scale(BASE + 5 * HOUR, &shared::recipe(PLANTED), 4);
        *warmup_ticks.last_mut().unwrap() = format!("{},SYNTHETIC,180.0001", time(START - 250_000));
        let warmup = shared::import_ticks(
            &scratch.root,
            "warmup",
            DatasetRole::Development,
            "deriv",
            &["R_50", "R_100"][..count],
            &vec![4; count],
            &vec![warmup_ticks; count],
        );
        config.research = None;
        config.brokers=serde_json::from_value(json!([{"kind":"deriv","id":"deriv","public_endpoint":"ws://127.0.0.1/public",
            "bootstrap_endpoint":"http://127.0.0.1/trading/v1/options","app_id":"SYNTHETIC","account_class":"demo"}])).unwrap();
        config.live=Some(serde_json::from_value(json!({"execution_contract":research::EXECUTION_CONTRACT_V1,
            "bundle_manifest":uri(&scratch.root,&generation),"certification_manifest":uri(&scratch.root,&cert),"broker":"deriv","account":account,
            "warmup":warmup.iter().map(|w| uri(&scratch.root,&w.generation)).collect::<Vec<_>>(),"compatibility":{"observation_start":time(START),"observation_end":time(START+2*CANDLE),"required_account_class":"demo","min_samples":1},
            "journal":{"dir":"journal","segment_records":16,"max_spool_bytes":10_000_000},
            "control":{"host":"localhost","port":5432,"database":"synthetic","user":"synthetic","credential":"SYNTHETIC_PASSWORD",
                "root_certificate":"synthetic.pem","owner":"synthetic-owner","lease_ttl_micros":60_000_000,"renewal_interval_micros":20_000_000,"safety_margin_micros":1_000},
            "replay":{"broker_log":"broker.jsonl"}})).unwrap());
        datasets.extend(warmup);
        let path = scratch.path("live.toml");
        write(&path, config.canonical_toml());
        Config::parse(&config.canonical_toml()).unwrap();
        Self {
            scratch,
            config,
            path,
            bundle,
            run,
            datasets,
        }
    }
    pub fn stores(&self) -> (Store, Store) {
        (
            Store::filesystem(self.scratch.path("local")),
            Store::filesystem(self.scratch.path("published")),
        )
    }
    pub fn definition(&self) -> live::LiveDefinition {
        let (local, destination) = self.stores();
        live::definition(&self.config, &local, &destination).unwrap()
    }
    pub fn log(&self) -> PathBuf {
        self.scratch.path("access.log")
    }
}

pub fn frame(name: &str) -> String {
    crate::common::broker::fixture(&format!("deriv-execution-{name}.json"))
}

/// A verified one-tick warm-up cannot supply a completed feature-history interval.
pub fn short_warmup(fixture: &mut Fixture) {
    let manifests = shared::import_ticks(
        &fixture.scratch.root,
        "short-warmup",
        DatasetRole::Development,
        "deriv",
        &["R_50"],
        &[4],
        &[vec![format!(
            "{},SYNTHETIC,180.0000",
            time(START - 500_000)
        )]],
    );
    fixture.config.live.as_mut().unwrap().warmup =
        vec![uri(&fixture.scratch.root, &manifests[0].generation)];
}
pub fn change(text: &str, owner: &str, key: &str, value: &str) -> String {
    let fields: std::collections::BTreeMap<String, Box<serde_json::value::RawValue>> =
        serde_json::from_str(text).unwrap();
    crate::common::broker::replace(
        text,
        owner,
        &crate::common::broker::replace(fields[owner].get(), key, value),
    )
}
pub fn log_line(session: &str, at: i64, frame: &str) -> String {
    format!("{}\n", json!({"session":session,"at":at,"frame":frame}))
}
pub fn matching_log() -> String {
    let mut log = String::new();
    for frame in [
        r#"{"data":[{"account_id":"SYNTHETICACCOUNT","account_type":"demo","status":"active","currency":"USD"}]}"#,
        r#"{"data":{"url":"ws://127.0.0.1/trading/v1/options/ws/demo"}}"#,
    ] {
        log.push_str(&log_line("bootstrap", START, frame));
    }
    log.push_str(&log_line("account", START, &frame("transaction-ack")));
    log.push_str(&log_line(
        "account",
        START,
        &change(&frame("balance-before"), "balance", "balance", "10000"),
    ));
    let tick = crate::common::broker::fixture("deriv-tick-R_50.json");
    let tick = change(
        &change(&tick, "tick", "epoch", &(START / 1_000_000).to_string()),
        "tick",
        "quote",
        "180.0000",
    );
    log.push_str(&log_line("market", START, &tick));
    let mut proposal = frame("proposal-call");
    proposal = change(&proposal, "echo_req", "duration", "5");
    proposal = change(
        &proposal,
        "proposal",
        "date_start",
        &(START / 1_000_000).to_string(),
    );
    proposal = change(
        &proposal,
        "proposal",
        "date_expiry",
        &(START / 1_000_000 + 5).to_string(),
    );
    proposal = change(&proposal, "proposal", "spot", "180.0000");
    proposal = change(
        &proposal,
        "proposal",
        "spot_time",
        &(START / 1_000_000).to_string(),
    );
    log.push_str(&log_line("account", START, &proposal));
    let mut buy = frame("buy-call");
    buy = change(
        &buy,
        "buy",
        "purchase_time",
        &(START / 1_000_000).to_string(),
    );
    buy = change(&buy, "buy", "start_time", &(START / 1_000_000).to_string());
    buy = change(&buy, "buy", "balance_after", "9990");
    log.push_str(&log_line("account", START, &buy));
    for (name, at) in [("entry-call", START), ("won", START + 5_000_000)] {
        let mut value = frame(name);
        for key in ["purchase_time", "date_start", "entry_spot_time"] {
            value = change(
                &value,
                "proposal_open_contract",
                key,
                &(START / 1_000_000).to_string(),
            );
        }
        for key in ["date_expiry", "date_settlement", "expiry_time"] {
            value = change(
                &value,
                "proposal_open_contract",
                key,
                &(START / 1_000_000 + 5).to_string(),
            );
        }
        value = change(
            &value,
            "proposal_open_contract",
            "entry_spot",
            r#""180.0000""#,
        );
        value = change(
            &value,
            "proposal_open_contract",
            "current_spot_time",
            &(at / 1_000_000).to_string(),
        );
        if name == "won" {
            value = change(
                &value,
                "proposal_open_contract",
                "exit_spot_time",
                &(at / 1_000_000).to_string(),
            );
            value = change(
                &value,
                "proposal_open_contract",
                "sell_time",
                &(at / 1_000_000).to_string(),
            );
            value = change(
                &value,
                "proposal_open_contract",
                "exit_spot",
                r#""180.0002""#,
            );
        }
        log.push_str(&log_line("account", at, &value));
    }
    let mut cash = frame("transaction-win");
    cash = change(&cash, "transaction", "balance", "10008.83");
    cash = change(
        &cash,
        "transaction",
        "transaction_time",
        &(START / 1_000_000 + 5).to_string(),
    );
    let mut lines: Vec<String> = log.lines().map(|line| format!("{line}\n")).collect();
    let terminal = lines.pop().unwrap();
    let due = change(
        &change(&tick, "tick", "epoch", &(START / 1_000_000 + 5).to_string()),
        "tick",
        "quote",
        "180.0002",
    );
    lines.push(log_line("market", START + 5_000_000, &due));
    lines.push(log_line("account", START + 5_000_000, &cash));
    lines.push(terminal.clone());
    lines.push(terminal);
    lines.concat()
}

/// The same owner with injectable control and clock; no credentials or network are resolved.
pub fn runtime(
    fixture: &Fixture,
    mode: live::Mode,
    recorded: &binary_alpha_app::broker::transport::RecordedConnector,
    control: binary_alpha_app::live::control::FakeControl,
) -> live::Runtime {
    runtime_with(
        fixture,
        mode,
        recorded,
        Box::new(control),
        |_| {},
        |market| market,
    )
    .unwrap()
}

pub fn runtime_with(
    fixture: &Fixture,
    mode: live::Mode,
    recorded: &binary_alpha_app::broker::transport::RecordedConnector,
    control: Box<dyn binary_alpha_app::live::control::Control>,
    edit: impl FnOnce(&mut live::LiveDefinition),
    wrap_market: impl FnOnce(
        Box<dyn binary_alpha_app::broker::MarketDataBroker>,
    ) -> Box<dyn binary_alpha_app::broker::MarketDataBroker>,
) -> Result<live::Runtime, String> {
    runtime_with_io(
        fixture,
        mode,
        recorded,
        control,
        edit,
        wrap_market,
        |connector| connector,
    )
}

pub fn runtime_with_io(
    fixture: &Fixture,
    mode: live::Mode,
    recorded: &binary_alpha_app::broker::transport::RecordedConnector,
    control: Box<dyn binary_alpha_app::live::control::Control>,
    edit: impl FnOnce(&mut live::LiveDefinition),
    wrap_market: impl FnOnce(
        Box<dyn binary_alpha_app::broker::MarketDataBroker>,
    ) -> Box<dyn binary_alpha_app::broker::MarketDataBroker>,
    wrap_account: impl FnOnce(
        Box<dyn binary_alpha_app::broker::transport::Connector>,
    ) -> Box<dyn binary_alpha_app::broker::transport::Connector>,
) -> Result<live::Runtime, String> {
    runtime_with_test_clock(
        fixture,
        mode,
        recorded,
        control,
        edit,
        wrap_market,
        wrap_account,
        recorded.clock(),
    )
}

/// Keeps the recorded scheduler while injecting a shared deterministic owner/adapter clock.
#[allow(clippy::too_many_arguments)]
pub fn runtime_with_test_clock(
    fixture: &Fixture,
    mode: live::Mode,
    recorded: &binary_alpha_app::broker::transport::RecordedConnector,
    control: Box<dyn binary_alpha_app::live::control::Control>,
    edit: impl FnOnce(&mut live::LiveDefinition),
    wrap_market: impl FnOnce(
        Box<dyn binary_alpha_app::broker::MarketDataBroker>,
    ) -> Box<dyn binary_alpha_app::broker::MarketDataBroker>,
    wrap_account: impl FnOnce(
        Box<dyn binary_alpha_app::broker::transport::Connector>,
    ) -> Box<dyn binary_alpha_app::broker::transport::Connector>,
    clock: impl binary_alpha_app::broker::Clock + Clone + 'static,
) -> Result<live::Runtime, String> {
    use binary_alpha_app::broker::{
        AccountIdentity,
        deriv::{DerivAccounts, DerivMarketData, DerivOptions},
    };
    use binary_alpha_engine::config::Broker;
    let Broker::Deriv(settings) = &fixture.config.brokers[0] else {
        panic!("fixture broker")
    };
    let address =
        DerivAccounts::bootstrap(settings, &mut recorded.http(), "synthetic-no-credential")
            .unwrap();
    let account = AccountIdentity {
        broker: settings.id.clone(),
        account: fixture.config.live.as_ref().unwrap().account.clone(),
        class: address.account_class,
        currency: address.currency.clone(),
    };
    let mut definition = fixture.definition();
    edit(&mut definition);
    let instruments = definition
        .definition
        .instruments
        .iter()
        .map(|i| {
            (
                binary_alpha_engine::market::InstrumentId {
                    broker: i.broker.clone(),
                    provider_symbol: i.provider_symbol.clone(),
                },
                i.price_scale.try_into().unwrap(),
            )
        })
        .collect::<Vec<_>>();
    let options = DerivOptions::connect(
        address,
        account,
        &instruments,
        wrap_account(Box::new(recorded.session("account").unwrap())),
        Box::new(clock.clone()),
        settings.budgets.clone().unwrap_or_default(),
    )
    .unwrap();
    let market = DerivMarketData::connect(
        settings,
        Box::new(recorded.session("market").unwrap()),
        Box::new(clock.clone()),
    )
    .unwrap();
    let (local, destination) = fixture.stores();
    live::Runtime::start(
        &fixture.config,
        &fixture.scratch.root,
        definition,
        local,
        destination,
        control,
        wrap_market(Box::new(market)),
        options,
        Box::new(clock.clone()),
        Some(recorded.clock()),
        mode,
    )
}

/// Retains the provider's relative entry/expiry/exit and purchase delays; shifts only the epoch.
pub fn retained_log() -> String {
    let mut records: Vec<(i64, String, String)> = matching_log()
        .lines()
        .take(4)
        .map(|line| {
            let v: Value = serde_json::from_str(line).unwrap();
            (
                v["at"].as_i64().unwrap(),
                v["session"].as_str().unwrap().into(),
                v["frame"].as_str().unwrap().into(),
            )
        })
        .collect();
    let shift = |mut text: String| {
        for second in 1_789_346_898i64..=1_789_347_054 {
            text = text.replace(
                &second.to_string(),
                &(START / 1_000_000 + second - 1_789_347_035).to_string(),
            );
        }
        text
    };
    let tick = crate::common::broker::fixture("deriv-tick-R_50.json");
    for second in 0..=20 {
        let price = if second == 0 {
            "92.0409"
        } else if second == 19 {
            "92.0410"
        } else {
            "92.0409"
        };
        let text = change(
            &change(
                &tick,
                "tick",
                "epoch",
                &(START / 1_000_000 + second).to_string(),
            ),
            "tick",
            "quote",
            price,
        );
        records.push((
            START
                + if second == 0 {
                    0
                } else {
                    second.max(3) * 1_000_000
                },
            "market".into(),
            text,
        ));
    }
    records.push((
        START,
        "account".into(),
        change(&shift(frame("proposal-call")), "echo_req", "duration", "5"),
    ));
    records.push((
        START + 1_000_000,
        "account".into(),
        change(&shift(frame("buy-call")), "buy", "balance_after", "9990"),
    ));
    records.push((
        START + 3_000_000,
        "account".into(),
        shift(frame("entry-call")),
    ));
    records.push((START + 17_000_000, "account".into(), shift(frame("won"))));
    records.push((
        START + 17_000_000,
        "account".into(),
        change(
            &shift(frame("transaction-win")),
            "transaction",
            "balance",
            "10008.83",
        ),
    ));
    records.push((
        START + 20_000_000,
        "account".into(),
        crate::common::broker::replace(
            &change(&shift(frame("proposal-later")), "echo_req", "duration", "5"),
            "req_id",
            "33",
        ),
    ));
    records.sort_by_key(|r| r.0);
    records
        .into_iter()
        .map(|(at, session, frame)| log_line(&session, at, &frame))
        .collect()
}

/// Two ready instruments at the same receipt time, with replies in frozen binding order.
pub fn two_instrument_log() -> String {
    let rows: Vec<Value> = matching_log()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    let mut log = rows[..2]
        .iter()
        .map(|row| format!("{row}\n"))
        .collect::<String>();
    log.push_str(&log_line(
        "account",
        START,
        r#"{"msg_type":"portfolio","req_id":21,"portfolio":{"contracts":[]}}"#,
    ));
    for row in &rows[2..4] {
        log.push_str(&format!("{row}\n"));
    }
    log.push_str(&format!("{}\n", rows[4]));
    let tick = crate::common::broker::fixture("deriv-tick-R_100.json");
    let tick = change(
        &change(
            &change(&tick, "tick", "epoch", &(START / 1_000_000).to_string()),
            "tick",
            "pip_size",
            "4",
        ),
        "tick",
        "quote",
        "180.0000",
    );
    log.push_str(&log_line("market", START, &tick));
    log.push_str(&format!("{}\n", rows[5]));
    let proposal = rows[5]["frame"].as_str().unwrap();
    let proposal = change(
        &change(proposal, "echo_req", "underlying_symbol", r#""R_100""#),
        "proposal",
        "id",
        r#""second-proposal""#,
    );
    // Distinct recorded identities let delayed duplicates remain correlated to their request.
    let proposal = crate::common::broker::replace(&proposal, "req_id", "44");
    log.push_str(&log_line("account", START, &proposal));
    log
}

pub fn zero_credit_log() -> String {
    let rows: Vec<Value> = matching_log()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    let mut log = rows[..9]
        .iter()
        .map(|row| format!("{row}\n"))
        .collect::<String>();
    let terminal = rows[10]["frame"].as_str().unwrap();
    let mut lost: Value = serde_json::from_str(terminal).unwrap();
    let contract = &mut lost["proposal_open_contract"];
    contract["status"] = json!("lost");
    contract["exit_spot"] = json!("179.9998");
    contract["sell_price"] = json!(0);
    contract["profit"] = json!(-10);
    contract["transaction_ids"]["sell"] = Value::Null;
    log.push_str(&log_line("account", START + 5_000_000, &lost.to_string()));
    let tick = rows[8]["frame"].as_str().unwrap();
    log.push_str(&log_line(
        "market",
        START + 7_000_000,
        &change(tick, "tick", "epoch", &(START / 1_000_000 + 7).to_string()),
    ));
    // Portfolio and statement are requested by the owner's renewal-cadence reconciliation.
    log.push_str(&log_line(
        "account",
        START + 7_000_000,
        r#"{"msg_type":"portfolio","req_id":71,"portfolio":{"contracts":[]}}"#,
    ));
    log.push_str(&log_line(
        "account",
        START + 7_000_000,
        r#"{"msg_type":"statement","req_id":72,"statement":{"count":0,"transactions":[]}}"#,
    ));
    log
}

/// The two instruments' matching lifecycles, including reordered cash and duplicate terminals.
pub fn two_matching_log() -> String {
    let one: Vec<Value> = matching_log()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    let two: Vec<Value> = two_instrument_log()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    let mut result = one[..4].to_vec();
    result.extend_from_slice(&two[5..]); // market pair, then the two proposal responses
    let second = |row: &Value, req: u64| {
        let text = row["frame"]
            .as_str()
            .unwrap()
            .replace("R_50", "R_100")
            .replace("12859891379", "12859891479")
            .replace("24655144239", "24655144339")
            .replace("24655172099", "24655172199")
            .replace("fixture-id-2", "second-proposal")
            .replace("fixture-id-3", "second-contract-subscription");
        let mut row = row.clone();
        row["frame"] = json!(crate::common::broker::replace(
            &text,
            "req_id",
            &req.to_string()
        ));
        row
    };
    result.extend([
        one[6].clone(),
        one[7].clone(),
        second(&one[6], 55),
        second(&one[7], 66),
    ]);
    result.push(one[8].clone());
    let mut tick = two[6].clone();
    tick["at"] = json!(START + 5_000_000);
    tick["frame"] = json!(change(
        &change(
            tick["frame"].as_str().unwrap(),
            "tick",
            "epoch",
            &(START / 1_000_000 + 5).to_string()
        ),
        "tick",
        "quote",
        "180.0002"
    ));
    result.push(tick);
    result.extend_from_slice(&one[9..]);
    result.extend([
        second(&one[9], 3),
        second(&one[10], 66),
        second(&one[11], 66),
    ]);
    result.iter().map(|row| format!("{row}\n")).collect()
}

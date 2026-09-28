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

#[path = "../common/quote.rs"]
mod quote_fixture;

pub const START: i64 = BASE + 5 * HOUR + (ROWS as i64 + 1) * CANDLE;
pub const QUOTE_START: i64 = BASE + 5 * HOUR + 32 * 40_000_000;
pub const PLANTED: [u8; 4] = [0b0011_1111, 0b0000_0011, 0b0001_1111, 0b0000_1111];

pub fn write(path: &Path, bytes: impl AsRef<[u8]>) {
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    fs::write(path, bytes).unwrap();
}
pub fn cli(log: &Path, args: &[&str]) -> Result<String, String> {
    crate::common::cli_as(log, "synthetic-operator", args).map_err(|s| s.trim_end().into())
}
pub fn assert_recorded_stall(error: &str) {
    assert!(error.starts_with("live replay: recorded log stalled before all frames and expected writes were consumed: head session="), "{error}");
    for field in [
        "; parked: bootstrap=",
        ", market=",
        ", account=",
        "; clock=",
    ] {
        assert!(error.contains(field), "missing {field}: {error}");
    }
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

fn certify(
    scratch: &Scratch,
    config: &Config,
    declaration: &Declaration,
    datasets: &[GenerationManifest],
) -> (String, RunManifest, Run, String) {
    write(
        &scratch.path("declaration.json"),
        research::to_json(&declaration),
    );
    let path = scratch.path("research.toml");
    write(&path, config.canonical_toml());
    let log = scratch.path("access.log");
    let count = datasets.len() / 5;
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
    (generation, bundle, run, cert)
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
    pub fn quote(name: &str) -> Self {
        Self::quote_streams(name, false, false)
    }
    pub fn quote_with_candle(name: &str) -> Self {
        Self::quote_streams(name, true, false)
    }
    pub fn quote_deriv_with_candle(name: &str) -> Self {
        Self::quote_streams(name, true, true)
    }
    fn quote_streams(name: &str, candle: bool, deriv: bool) -> Self {
        let scratch = Scratch::new(name);
        let (mut config, mut declaration, mut datasets) = quote_fixture::build(&scratch);
        if deriv {
            datasets = datasets
                .into_iter()
                .enumerate()
                .filter_map(|(index, dataset)| (index % 2 == 0).then_some(dataset))
                .collect();
            declaration
                .populations
                .retain(|population| population.instrument == shared::INSTRUMENTS[0]);
            config.instruments.truncate(1);
            for instrument in &mut config.instruments {
                instrument.quote_currency = "USD".to_string().try_into().unwrap();
            }
            let research = config.research.as_mut().unwrap();
            research.instruments.truncate(1);
            for fold in &mut research.folds {
                fold.inputs.truncate(1);
            }
            research.refit.fits.truncate(1);
            research.evaluation.inputs.truncate(1);
            research.holdout.inputs.truncate(1);
        }
        if candle {
            let features = &mut config.research.as_mut().unwrap().instruments[0].features;
            features.streams = Some(vec![
                binary_alpha_engine::config::StreamKey::candle(20, 0),
                binary_alpha_engine::config::StreamKey::quote(),
            ]);
            features.outputs = Some(binary_alpha_engine::config::Outputs::AllSupported);
        }
        if deriv && candle {
            let portfolio = &mut config.research.as_mut().unwrap().portfolio;
            portfolio.repairs[0]
                .conditions
                .push(binary_alpha_engine::execution::Condition {
                    stream: binary_alpha_engine::config::StreamKey::candle(20, 0),
                    output: "candle_direction".into(),
                    comparator: binary_alpha_engine::execution::Comparator::Eq,
                    threshold: binary_alpha_engine::execution::Threshold::Text("down".into()),
                });
            portfolio.risk_policies[0].max_feature_age_micros = 30_000_000;
        }
        let (generation, bundle, run, cert) = certify(&scratch, &config, &declaration, &datasets);
        let ticks = quote_fixture::quote_ticks(BASE + 5 * HOUR);
        let warmup = shared::import_ticks(
            &scratch.root,
            "warmup",
            DatasetRole::Development,
            "pocket_option",
            &shared::SYMBOLS,
            &[5, 5],
            &[ticks.clone(), ticks],
        );
        datasets.extend(warmup.clone());
        config.research = None;
        config.brokers = if deriv {
            serde_json::from_value(json!([{
                "kind":"deriv", "id":"pocket_option", "public_endpoint":"ws://127.0.0.1/public",
                "bootstrap_endpoint":"http://127.0.0.1/trading/v1/options", "app_id":"SYNTHETIC",
                "account_class":"demo"
            }]))
            .unwrap()
        } else {
            serde_json::from_value(json!([{
                "kind":"pocket_option", "id":"pocket_option", "endpoint":"wss://example.invalid/socket.io/?EIO=4&transport=websocket",
                "credential":"SYNTHETIC_AUTH", "account_class":"demo", "server_offset_minutes":0,
                "payout":{"add_percent":8,"cap_percent":92,"max_age_seconds":3}
            }])).unwrap()
        };
        config.live = Some(serde_json::from_value(json!({
            "execution_contract":research::EXECUTION_CONTRACT_V1,
            "bundle_manifest":uri(&scratch.root,&generation), "certification_manifest":uri(&scratch.root,&cert),
            "broker":"pocket_option", "account":"a0",
            "warmup":warmup.iter().take(if deriv {1} else {2}).map(|w| uri(&scratch.root,&w.generation)).collect::<Vec<_>>(),
            "compatibility":{"observation_start":time(QUOTE_START),"observation_end":time(QUOTE_START+80_000_000),"required_account_class":"demo","min_samples":1},
            "journal":{"dir":"journal","segment_records":16,"max_spool_bytes":10_000_000},
            "control":{"host":"localhost","port":5432,"database":"synthetic","user":"synthetic","credential":"SYNTHETIC_PASSWORD",
                "root_certificate":"synthetic.pem","owner":"synthetic-owner","lease_ttl_micros":60_000_000,"renewal_interval_micros":20_000_000,"safety_margin_micros":1_000},
            "replay":{"broker_log":"broker.jsonl"}
        })).unwrap());
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
        let (generation, bundle, run, cert) = certify(&scratch, &config, &declaration, &datasets);
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
        live::definition(&self.config).unwrap()
    }
    pub fn log(&self) -> PathBuf {
        self.scratch.path("access.log")
    }
}

pub fn frame(name: &str) -> String {
    crate::common::broker::fixture(&format!("deriv-execution-{name}.json"))
}

fn pocket_event(name: &str, value: Value) -> String {
    format!("42[\"{name}\",{value}]")
}
fn pocket_line(session: &str, at: i64, frame: &str) -> String {
    format!("{}\n", json!({"session":session,"at":at,"frame":frame}))
}
fn pocket_binary(session: &str, at: i64, name: &str, value: Value) -> String {
    let header = pocket_line(
        session,
        at - 200_000,
        &format!("451-[\"{name}\",{{\"_placeholder\":true,\"num\":0}}]"),
    );
    let bytes = serde_json::to_vec(&value).unwrap();
    header + &format!("{}\n", json!({"session":session,"at":at,"binary":bytes}))
}
/// Insert the writes a polled Pocket session makes as the shared replay clock advances.
/// Input contains only ordered receipts and non-keepalive expectations.
pub fn pocket_keepalives(input: &str) -> (String, Vec<String>) {
    let mut deadlines = [None, None]; // market, account
    let mut attachments = [false, false];
    let mut due = [false, false];
    let mut output = String::new();
    let mut writes = Vec::new();
    let mut clock = i64::MIN;
    let mut expect = |session: &str, output: &mut String| {
        output.push_str(&format!(
            "{}\n",
            json!({"session":session,"expect":"42[\"ps\",null]"})
        ));
        writes.push(session.to_string());
    };
    for line in input.lines() {
        let row: Value = serde_json::from_str(line).unwrap();
        if row["expect"] == "42[\"ps\",null]" {
            continue;
        }
        let session = row["session"].as_str().unwrap();
        let index = if session == "market" { 0 } else { 1 };
        if let Some(at) = row["at"].as_i64() {
            clock = at;
        }
        let at = clock;
        let frame = row["frame"].as_str().unwrap_or_default();
        output.push_str(&format!("{row}\n"));
        if frame == "41" {
            deadlines[index] = None;
            due[index] = false;
            attachments[index] = false;
        }
        if frame.contains("successauth") {
            expect(session, &mut output);
            deadlines[index] = Some(at + 30_000_000);
        }
        if frame.starts_with("451-") {
            attachments[index] = true;
        } else if row.get("binary").is_some() {
            assert!(attachments[index], "binary attachment needs its header");
            attachments[index] = false;
        }
        for i in 0..2 {
            if deadlines[i].is_some_and(|deadline| at >= deadline) {
                due[i] = true;
            }
        }
        for (i, name) in ["market", "account"].into_iter().enumerate() {
            if due[i] && !attachments[i] && !(i == 1 && due[0]) {
                expect(name, &mut output);
                due[i] = false;
                deadlines[i] = Some(at + 30_000_000);
            }
        }
    }
    (output, writes)
}
pub fn pocket_log() -> String {
    pocket_log_with_initial_listing(false)
}
pub fn pocket_sparse_keepalive_log() -> (String, Vec<String>) {
    let start = QUOTE_START;
    let mut log = pocket_log();
    let balance = |session, offset| {
        pocket_line(
            session,
            start + offset,
            &pocket_event(
                "successupdateBalance",
                json!({"isDemo":1,"balance":10001.84}),
            ),
        )
    };
    log.push_str(&balance("market", 95_000_000));
    log.push_str(&balance("market", 125_500_000));
    log.push_str(&balance("account", 126_000_000));
    let mut asset = vec![Value::Null; 19];
    asset[1] = json!(shared::SYMBOLS[0]);
    asset[5] = json!(84);
    let mut second = asset.clone();
    second[1] = json!(shared::SYMBOLS[1]);
    log.push_str(&pocket_binary(
        "market",
        start + 157_000_000,
        "updateAssets",
        json!([asset.clone()]),
    ));
    log.push_str(&balance("market", 187_500_000));
    log.push_str(&balance("account", 188_000_000));
    log.push_str(&balance("market", 219_000_000));
    log.push_str(&balance("market", 249_500_000));
    log.push_str(&balance("account", 250_000_000));
    log.push_str(&balance("market", 280_000_000));
    log.push_str(&balance("market", 310_500_000));
    log.push_str(&balance("account", 311_000_000));
    log.push_str(&pocket_line("market", start + 312_000_000, "41"));
    log.push_str(&pocket_line("market", start + 312_100_000, "0{}"));
    log.push_str(&pocket_line("market", start + 312_100_001, "40{}"));
    log.push_str(&pocket_line(
        "market",
        start + 312_100_002,
        &pocket_event("successauth", json!({})),
    ));
    log.push_str(&balance("market", 312_200_000));
    log.push_str(&pocket_binary(
        "market",
        start + 312_500_000,
        "updateAssets",
        json!([asset.clone(), second]),
    ));
    log.push_str(&pocket_binary(
        "account",
        start + 313_900_000,
        "updateAssets",
        json!([asset.clone()]),
    ));
    log.push_str(&pocket_binary(
        "account",
        start + 339_000_000,
        "updateAssets",
        json!([asset]),
    ));
    let quote = |offset: i64, units: i64| {
        let at = start + offset;
        json!([[
            shared::SYMBOLS[0],
            serde_json::from_str::<Value>(&format!("{}.{:06}", at / 1_000_000, at % 1_000_000))
                .unwrap(),
            serde_json::from_str::<Value>(&format!("{}.{:05}", units / 100_000, units % 100_000))
                .unwrap()
        ]])
    };
    let mut first = quote(340_000_000, 101_000);
    first.as_array_mut().unwrap().push(json!([
        shared::SYMBOLS[1],
        serde_json::from_str::<Value>(&format!(
            "{}.{:06}",
            (start + 340_000_000) / 1_000_000,
            (start + 340_000_000) % 1_000_000
        ))
        .unwrap(),
        1.01
    ]));
    log.push_str(&pocket_binary(
        "market",
        start + 340_200_000,
        "updateStream",
        first,
    ));
    let mut jump = quote(340_300_000, 101_150);
    jump.as_array_mut().unwrap().push(json!([
        shared::SYMBOLS[1],
        serde_json::from_str::<Value>(&format!(
            "{}.{:06}",
            (start + 340_300_000) / 1_000_000,
            (start + 340_300_000) % 1_000_000
        ))
        .unwrap(),
        1.01001
    ]));
    log.push_str(&pocket_line(
        "market",
        start + 340_400_000,
        &pocket_event("updateStream", jump),
    ));
    log.push_str(&format!("{}\n", json!({"session":"account","at":start+340_450_000,
        "expect":format!("42[\"openOrder\",{{\"asset\":\"{}\",\"amount\":1,\"action\":\"put\",\"isDemo\":1,\"requestId\":33333333,\"optionType\":100,\"time\":30}}]", shared::SYMBOLS[0])})));
    log.push_str(&pocket_line(
        "market",
        start + 340_600_000,
        &pocket_event("updateStream", quote(340_500_000, 101_151)),
    ));
    let opened = json!({"id":"synthetic-three","asset":shared::SYMBOLS[0],"command":1,"amount":1,
        "profit":0,"percentProfit":92,"openPrice":1.01150,"closePrice":null,
        "openTimestamp":(start+340_000_000)/1_000_000,"openMs":700,
        "closeTimestamp":(start+370_000_000)/1_000_000,"isDemo":1,"currency":"USD",
        "requestId":33333333,"optionType":100});
    log.push_str(&pocket_line(
        "account",
        start + 340_700_000,
        &pocket_event("successopenOrder", opened.clone()),
    ));
    log.push_str(&pocket_line(
        "account",
        start + 340_800_000,
        &pocket_event(
            "successupdateBalance",
            json!({"isDemo":1,"balance":10000.84}),
        ),
    ));
    let mut closed = opened;
    closed["profit"] = json!(0.92);
    closed["closePrice"] = json!(1.01000);
    closed["closeMs"] = json!(200);
    log.push_str(&pocket_line(
        "account",
        start + 370_200_000,
        &pocket_event("successcloseOrder", json!({"profit":1.92,"deals":[closed]})),
    ));
    log.push_str(&pocket_line(
        "account",
        start + 370_300_000,
        &pocket_event(
            "successupdateBalance",
            json!({"isDemo":1,"balance":10002.76}),
        ),
    ));
    log.push_str(&pocket_line(
        "market",
        start + 370_400_000,
        &pocket_event("updateStream", quote(370_350_000, 101_151)),
    ));
    log.push_str(&pocket_line(
        "market",
        start + 400_000_000,
        &pocket_event("updateStream", quote(399_900_000, 101_152)),
    ));
    pocket_keepalives(&log)
}
pub fn pocket_missing_log() -> String {
    pocket_log_with_initial_listing(true)
}
pub fn pocket_foreign_log() -> String {
    let mut log = pocket_log();
    let foreign = json!({"id":"synthetic-foreign","asset":shared::SYMBOLS[0],"command":1,
        "amount":1,"profit":-1,"percentProfit":92,"openPrice":1.0,"closePrice":1.1,
        "openTimestamp":QUOTE_START/1_000_000+33,"openMs":0,
        "closeTimestamp":QUOTE_START/1_000_000+63,"closeMs":0,
        "isDemo":1,"currency":"USD","requestId":44444444,"optionType":100});
    log.push_str(&pocket_line(
        "account",
        QUOTE_START + 63_400_000,
        &pocket_event("successcloseOrder", json!({"profit":0,"deals":[foreign]})),
    ));
    log
}
pub fn pocket_aged_quote_log() -> String {
    let mut log = pocket_log();
    let mut asset = vec![Value::Null; 19];
    asset[1] = json!(shared::SYMBOLS[0]);
    asset[5] = json!(84);
    let mut second = asset.clone();
    second[1] = json!(shared::SYMBOLS[1]);
    log.push_str(&pocket_binary(
        "account",
        QUOTE_START + 63_500_000,
        "updateAssets",
        json!([asset, second]),
    ));
    let quote = |at: i64, units: i64| {
        json!([[
            shared::SYMBOLS[0],
            serde_json::from_str::<Value>(&format!("{}.{:06}", at / 1_000_000, at % 1_000_000))
                .unwrap(),
            serde_json::from_str::<Value>(&format!("{}.{:05}", units / 100_000, units % 100_000))
                .unwrap()
        ]])
    };
    log.push_str(&pocket_binary(
        "market",
        QUOTE_START + 65_000_000,
        "updateStream",
        quote(QUOTE_START + 63_000_000, 100_850),
    ));
    log.push_str(&pocket_binary(
        "market",
        QUOTE_START + 65_300_000,
        "updateStream",
        quote(QUOTE_START + 65_100_000, 100_851),
    ));
    log
}
pub fn pocket_economics_log() -> String {
    pocket_log()
        .lines()
        .filter_map(|line| {
            let mut row: Value = serde_json::from_str(line).unwrap();
            if row["at"]
                .as_i64()
                .is_some_and(|at| at >= QUOTE_START + 31_000_000)
            {
                return None;
            }
            if let Some(frame) = row["frame"].as_str()
                && frame.contains("successopenOrder")
            {
                row["frame"] = json!(frame.replace("\"percentProfit\":92", "\"percentProfit\":91"));
            }
            Some(format!("{}\n", row))
        })
        .collect()
}
pub fn pocket_reconnect_log() -> String {
    let mut log = pocket_log();
    log.push_str(&pocket_line("market", QUOTE_START + 63_000_000, "41"));
    log.push_str(&pocket_line("market", QUOTE_START + 63_100_000, "0{}"));
    log.push_str(&pocket_line("market", QUOTE_START + 63_100_001, "40{}"));
    log.push_str(&pocket_line(
        "market",
        QUOTE_START + 63_100_002,
        &pocket_event("successauth", json!({})),
    ));
    log.push_str(&pocket_line(
        "market",
        QUOTE_START + 63_100_003,
        &pocket_event(
            "successupdateBalance",
            json!({"isDemo":1,"balance":10001.84}),
        ),
    ));
    let mut asset = vec![Value::Null; 19];
    asset[1] = json!(shared::SYMBOLS[0]);
    asset[5] = json!(84);
    let mut second = asset.clone();
    second[1] = json!(shared::SYMBOLS[1]);
    log.push_str(&pocket_binary(
        "market",
        QUOTE_START + 63_400_000,
        "updateAssets",
        json!([asset, second]),
    ));
    let quote = |at: i64, units: i64| {
        json!([[
            shared::SYMBOLS[0],
            serde_json::from_str::<Value>(&format!("{}.{:06}", at / 1_000_000, at % 1_000_000))
                .unwrap(),
            serde_json::from_str::<Value>(&format!("{}.{:05}", units / 100_000, units % 100_000))
                .unwrap()
        ]])
    };
    log.push_str(&pocket_binary(
        "market",
        QUOTE_START + 63_600_000,
        "updateStream",
        quote(QUOTE_START + 63_500_000, 100_950),
    ));
    log.push_str(&pocket_binary(
        "market",
        QUOTE_START + 63_900_000,
        "updateStream",
        quote(QUOTE_START + 63_800_000, 100_960),
    ));
    log
}
pub fn pocket_authorization_wait_log() -> String {
    let mut log = pocket_log()
        .lines()
        .take_while(|line| !line.contains("updateStream"))
        .map(|line| format!("{line}\n"))
        .collect::<String>();
    let row = |offset: i64, price: f64| {
        json!([
            shared::SYMBOLS[0],
            serde_json::from_str::<Value>(&format!(
                "{}.{:06}",
                (QUOTE_START + offset) / 1_000_000,
                (QUOTE_START + offset) % 1_000_000
            ))
            .unwrap(),
            price
        ])
    };
    log.push_str(&pocket_binary(
        "market",
        QUOTE_START + 1_100_000,
        "updateStream",
        json!([
            row(0, 1.00494),
            row(300_000, 1.00644),
            row(600_000, 1.00800),
            row(900_000, 1.00950)
        ]),
    ));
    log
}
pub fn pocket_granted_after_early_quotes_log() -> String {
    let start = QUOTE_START;
    let mut log = pocket_log()
        .lines()
        .take_while(|line| !line.contains("updateStream"))
        .map(|line| format!("{line}\n"))
        .collect::<String>();
    let quote = |offset: i64, units: i64| {
        let at = start + offset;
        json!([[
            shared::SYMBOLS[0],
            serde_json::from_str::<Value>(&format!("{}.{:06}", at / 1_000_000, at % 1_000_000))
                .unwrap(),
            serde_json::from_str::<Value>(&format!("{}.{:05}", units / 100_000, units % 100_000))
                .unwrap()
        ]])
    };
    let mut first = quote(0, 100_644);
    first
        .as_array_mut()
        .unwrap()
        .push(json!([shared::SYMBOLS[1], start / 1_000_000, 1.00494]));
    log.push_str(&pocket_binary(
        "market",
        start + 200_000,
        "updateStream",
        first,
    ));
    log.push_str(&pocket_line(
        "market",
        start + 400_000,
        &pocket_event("updateStream", quote(300_000, 100_800)),
    ));
    log.push_str(&pocket_line(
        "market",
        start + 800_000,
        &pocket_event("updateStream", quote(700_000, 100_950)),
    ));
    log.push_str(&format!("{}\n", json!({"session":"account","at":start+900_000,
        "expect":format!("42[\"openOrder\",{{\"asset\":\"{}\",\"amount\":1,\"action\":\"put\",\"isDemo\":1,\"requestId\":11111111,\"optionType\":100,\"time\":30}}]", shared::SYMBOLS[0])})));
    let opened = json!({"id":"synthetic-late","asset":shared::SYMBOLS[0],"command":1,"amount":1,"profit":0,
        "percentProfit":92,"openPrice":1.00950,"closePrice":null,"openTimestamp":start/1_000_000,
        "openMs":900,"closeTimestamp":start/1_000_000+30,"isDemo":1,"currency":"USD","requestId":11111111,"optionType":100});
    log.push_str(&pocket_line(
        "account",
        start + 1_000_000,
        &pocket_event("successopenOrder", opened.clone()),
    ));
    log.push_str(&pocket_line(
        "account",
        start + 1_100_000,
        &pocket_event("successupdateBalance", json!({"isDemo":1,"balance":9999})),
    ));
    log.push_str(&pocket_line(
        "market",
        start + 30_900_000,
        &pocket_event("updateStream", quote(30_800_000, 100_800)),
    ));
    let mut closed = opened;
    closed["profit"] = json!(0.92);
    closed["closePrice"] = json!(1.00800);
    closed["closeMs"] = json!(200);
    log.push_str(&pocket_line(
        "account",
        start + 31_100_000,
        &pocket_event("successcloseOrder", json!({"profit":1.92,"deals":[closed]})),
    ));
    log.push_str(&pocket_line(
        "account",
        start + 31_200_000,
        &pocket_event(
            "successupdateBalance",
            json!({"isDemo":1,"balance":10000.92}),
        ),
    ));
    log
}
pub fn pocket_old_foreign_open_log() -> String {
    let old = json!({"id":"synthetic-old-open","asset":shared::SYMBOLS[0],"command":1,
        "amount":1,"profit":0,"percentProfit":92,"openPrice":1.0,"closePrice":null,
        "openTimestamp":QUOTE_START/1_000_000-7200,"openMs":0,
        "closeTimestamp":QUOTE_START/1_000_000+3600,
        "isDemo":1,"currency":"USD","optionType":100});
    pocket_authorization_wait_log()
        .lines()
        .map(|line| {
            let mut row: Value = serde_json::from_str(line).unwrap();
            if row["frame"]
                .as_str()
                .is_some_and(|frame| frame.contains("updateOpenedDeals"))
            {
                row["frame"] = json!(pocket_event("updateOpenedDeals", json!([old])));
            }
            format!("{row}\n")
        })
        .collect()
}
pub fn pocket_new_fact_balance_log() -> String {
    let mut log = pocket_authorization_wait_log();
    log.push_str(&pocket_line(
        "account",
        QUOTE_START + 1_200_000,
        &pocket_event(
            "updateClosedDeals",
            json!([{
                "id":"synthetic-later-history","asset":shared::SYMBOLS[0],
                "command":1,"amount":1,"profit":-1,"percentProfit":92,
                "openPrice":1.0,"closePrice":1.1,
                "openTimestamp":QUOTE_START/1_000_000-7200,"openMs":0,
                "closeTimestamp":QUOTE_START/1_000_000-3600,"closeMs":0,
                "isDemo":1,"currency":"USD","requestId":55555555,"optionType":100
            }]),
        ),
    ));
    log.push_str(&pocket_line(
        "account",
        QUOTE_START + 1_300_000,
        &pocket_event("successupdateBalance", json!({"isDemo":1,"balance":10000})),
    ));
    log
}
pub fn pocket_changed_old_fact_log() -> String {
    let changed = json!({
        "id":"synthetic-older","asset":shared::SYMBOLS[0],
        "command":1,"amount":1,"profit":0,"percentProfit":92,
        "openPrice":1.0,"closePrice":1.2,
        "openTimestamp":QUOTE_START/1_000_000-7200,"openMs":0,
        "closeTimestamp":QUOTE_START/1_000_000-3600,"closeMs":0,
        "isDemo":1,"currency":"USD","requestId":33333333,"optionType":100
    });
    let mut log: String = pocket_restart_log(33333333)
        .lines()
        .map(|line| {
            let mut row: Value = serde_json::from_str(line).unwrap();
            if row["frame"]
                .as_str()
                .is_some_and(|frame| frame.contains("updateClosedDeals"))
            {
                row["frame"] = json!(pocket_event("updateClosedDeals", json!([changed])));
            } else if row["frame"]
                .as_str()
                .is_some_and(|frame| frame.contains("successupdateBalance"))
            {
                row["frame"] = json!(pocket_event(
                    "successupdateBalance",
                    json!({"isDemo":1,"balance":10000})
                ));
            }
            format!("{row}\n")
        })
        .collect();
    for step in 0..3 {
        let at = QUOTE_START + 32_000_000 + step * 300_000;
        let provider_at = at - 200_000;
        let provider = serde_json::from_str::<Value>(&format!(
            "{}.{:06}",
            provider_at / 1_000_000,
            provider_at % 1_000_000
        ))
        .unwrap();
        let price = serde_json::from_str::<Value>(&format!("1.{:05}", 494 + step)).unwrap();
        log.push_str(&pocket_binary(
            "market",
            at,
            "updateStream",
            json!([
                [shared::SYMBOLS[0], provider, price],
                [shared::SYMBOLS[1], provider, price]
            ]),
        ));
    }
    log
}
pub fn pocket_bad_live_close_log() -> String {
    let mut log = pocket_log()
        .lines()
        .take_while(|line| !line.contains("successcloseOrder"))
        .map(|line| format!("{line}\n"))
        .collect::<String>();
    let close = json!({"id":"synthetic-one","asset":shared::SYMBOLS[0],"command":1,
        "amount":2,"profit":-2,"percentProfit":92,"openPrice":1.00644,"closePrice":1.00700,
        "openTimestamp":QUOTE_START/1_000_000,"openMs":600,
        "closeTimestamp":QUOTE_START/1_000_000+30,"closeMs":200,
        "isDemo":1,"currency":"USD","requestId":11111111,"optionType":100});
    log.push_str(&pocket_line(
        "account",
        QUOTE_START + 30_400_000,
        &pocket_event("successcloseOrder", json!({"profit":0,"deals":[close]})),
    ));
    log.push_str(&pocket_line(
        "account",
        QUOTE_START + 30_500_000,
        &pocket_event("successupdateBalance", json!({"isDemo":1,"balance":9999})),
    ));
    log
}
pub fn pocket_changed_close_log() -> String {
    let mut log = pocket_log()
        .lines()
        .take_while(|line| !line.contains("10000.92"))
        .map(|line| format!("{line}\n"))
        .collect::<String>();
    let changed = json!({"id":"synthetic-one","asset":shared::SYMBOLS[0],"command":1,
        "amount":1,"profit":0.92,"percentProfit":91,"openPrice":1.00644,"closePrice":1.00500,
        "openTimestamp":QUOTE_START/1_000_000,"openMs":600,
        "closeTimestamp":QUOTE_START/1_000_000+30,"closeMs":200,
        "isDemo":1,"currency":"USD","requestId":11111111,"optionType":100});
    log.push_str(&pocket_line(
        "account",
        QUOTE_START + 30_500_000,
        &pocket_event("updateClosedDeals", json!([changed])),
    ));
    log.push_str(&pocket_line(
        "account",
        QUOTE_START + 30_600_000,
        &pocket_event(
            "successupdateBalance",
            json!({"isDemo":1,"balance":10000.92}),
        ),
    ));
    log
}
pub fn pocket_refund_on_loss_log() -> String {
    let mut log = pocket_log()
        .lines()
        .take_while(|line| !line.contains("successcloseOrder"))
        .map(|line| format!("{line}\n"))
        .collect::<String>();
    let closed = json!({"id":"synthetic-one","asset":shared::SYMBOLS[0],"command":1,
        "amount":1,"profit":-0.5,"percentProfit":92,"openPrice":1.00644,"closePrice":1.00700,
        "openTimestamp":QUOTE_START/1_000_000,"openMs":600,
        "closeTimestamp":QUOTE_START/1_000_000+30,"closeMs":200,
        "isDemo":1,"currency":"USD","requestId":11111111,"optionType":100});
    log.push_str(&pocket_line(
        "account",
        QUOTE_START + 30_400_000,
        &pocket_event("successcloseOrder", json!({"profit":0.5,"deals":[closed]})),
    ));
    log.push_str(&pocket_line(
        "account",
        QUOTE_START + 30_500_000,
        &pocket_event("successupdateBalance", json!({"isDemo":1,"balance":9999.5})),
    ));
    log
}
pub fn pocket_open_restart_log(request_id: u64) -> String {
    let at = QUOTE_START + 1_000_000;
    let mut asset = vec![Value::Null; 19];
    asset[1] = json!(shared::SYMBOLS[0]);
    asset[5] = json!(84);
    let mut second = asset.clone();
    second[1] = json!(shared::SYMBOLS[1]);
    let opened = json!({"id":"synthetic-one","asset":shared::SYMBOLS[0],"command":1,"amount":1,"profit":0,
        "percentProfit":92,"openPrice":1.00644,"closePrice":null,
        "openTimestamp":QUOTE_START/1_000_000,"openMs":600,
        "closeTimestamp":QUOTE_START/1_000_000+30,"isDemo":1,"currency":"USD",
        "requestId":request_id,"optionType":100});
    let mut log = String::new();
    for session in ["account", "market"] {
        let base = at + if session == "account" { 0 } else { 500_000 };
        log.push_str(&pocket_line(session, base, "0{}"));
        log.push_str(&pocket_line(session, base + 1, "40{}"));
        log.push_str(&pocket_line(
            session,
            base + 2,
            &pocket_event("successauth", json!({})),
        ));
        log.push_str(&pocket_line(
            session,
            base + 3,
            &pocket_event("successupdateBalance", json!({"isDemo":1,"balance":9999})),
        ));
        log.push_str(&pocket_binary(
            session,
            base + 200_003,
            "updateAssets",
            json!([asset.clone(), second.clone()]),
        ));
        if session == "account" {
            log.push_str(&pocket_line(
                session,
                base + 200_004,
                &pocket_event("updateOpenedDeals", json!([opened])),
            ));
            log.push_str(&pocket_line(
                session,
                base + 200_005,
                &pocket_event("updateClosedDeals", json!([])),
            ));
        }
    }
    log.push_str(&pocket_line(
        "account",
        at + 800_000,
        &pocket_event("successupdateBalance", json!({"isDemo":1,"balance":9999})),
    ));
    log
}
pub fn pocket_restart_log(request_id: u64) -> String {
    pocket_restart_log_with(request_id, 0)
}
pub fn pocket_duplicate_restart_log(request_id: u64) -> String {
    pocket_restart_log_with(request_id, 1)
}
pub fn pocket_contradictory_restart_log(request_id: u64) -> String {
    pocket_restart_log_with(request_id, 2)
}
pub fn pocket_empty_restart_log(request_id: u64) -> String {
    pocket_restart_log_with(request_id, 3)
}
pub fn pocket_foreign_partial_restart_log(request_id: u64) -> String {
    pocket_restart_log_with(request_id, 4)
}
pub fn pocket_foreign_exact_restart_log(request_id: u64) -> String {
    pocket_restart_log(request_id).replace("synthetic-one", "synthetic-foreign-close")
}
pub fn pocket_foreign_omitting_restart_log(request_id: u64) -> String {
    pocket_restart_log_with(request_id, 3).replace("10000.92", "9999")
}
fn pocket_restart_log_with(request_id: u64, variant: u8) -> String {
    let at = QUOTE_START + 31_000_000;
    let mut log = String::new();
    let mut asset = vec![Value::Null; 19];
    asset[1] = json!(shared::SYMBOLS[0]);
    asset[5] = json!(84);
    let mut second = asset.clone();
    second[1] = json!(shared::SYMBOLS[1]);
    let closed = json!({"id":"synthetic-one","asset":shared::SYMBOLS[0],"command":1,"amount":1,"profit":0.92,
        "percentProfit":92,"openPrice":1.00644,"closePrice":1.00500,
        "openTimestamp":QUOTE_START/1_000_000,"openMs":600,"closeTimestamp":QUOTE_START/1_000_000+30,
        "closeMs":200,"isDemo":1,"currency":"USD","requestId":request_id,"optionType":100});
    let deals = match variant {
        0 => vec![closed],
        1 => {
            let mut duplicate = closed.clone();
            duplicate["id"] = json!("synthetic-duplicate");
            vec![closed, duplicate]
        }
        2 => {
            let mut changed = closed.clone();
            changed["command"] = json!(0);
            vec![closed, changed]
        }
        3 => Vec::new(),
        4 => vec![
            json!({"id":"synthetic-foreign-close","asset":shared::SYMBOLS[0],"command":1,
            "amount":1,"profit":-1,"percentProfit":92,"openPrice":1.0,"closePrice":1.1,
            "openTimestamp":QUOTE_START/1_000_000,"openMs":0,
            "closeTimestamp":QUOTE_START/1_000_000+30,"closeMs":0,
            "isDemo":1,"currency":"USD","requestId":44444444,"optionType":100}),
        ],
        _ => unreachable!(),
    };
    for session in ["account", "market"] {
        let base = at + if session == "account" { 0 } else { 500_000 };
        log.push_str(&pocket_line(session, base, "0{}"));
        log.push_str(&pocket_line(session, base + 1, "40{}"));
        log.push_str(&pocket_line(
            session,
            base + 2,
            &pocket_event("successauth", json!({})),
        ));
        log.push_str(&pocket_line(
            session,
            base + 3,
            &pocket_event(
                "successupdateBalance",
                json!({"isDemo":1,"balance":if variant == 4 {10000.0} else {10000.92}}),
            ),
        ));
        log.push_str(&pocket_binary(
            session,
            base + 200_003,
            "updateAssets",
            json!([asset.clone(), second.clone()]),
        ));
        if session == "account" {
            if matches!(variant, 1 | 2) {
                for (index, deal) in deals.iter().enumerate() {
                    log.push_str(&pocket_line(
                        session,
                        base + 200_004 + index as i64,
                        &pocket_event("updateClosedDeals", json!([deal])),
                    ));
                }
                log.push_str(&pocket_line(
                    session,
                    base + 200_006,
                    &pocket_event("updateOpenedDeals", json!([])),
                ));
            } else {
                log.push_str(&pocket_line(
                    session,
                    base + 200_004,
                    &pocket_event("updateOpenedDeals", json!([])),
                ));
                log.push_str(&pocket_line(
                    session,
                    base + 200_005,
                    &pocket_event("updateClosedDeals", json!(deals.clone())),
                ));
            }
        }
    }
    if !deals.is_empty() {
        log.push_str(&pocket_line(
            "account",
            at + 800_000,
            &pocket_event(
                "successupdateBalance",
                json!({"isDemo":1,"balance":if variant == 4 {10000.0} else {10000.92}}),
            ),
        ));
    }
    if variant == 4 {
        log.push_str(&pocket_line(
            "account",
            at + 1_000_000,
            &pocket_event("updateClosedDeals", json!([])),
        ));
        log.push_str(&pocket_line(
            "market",
            at + 21_000_000,
            &pocket_event(
                "updateStream",
                json!([[shared::SYMBOLS[0], (at + 21_000_000) / 1_000_000, 1.0]]),
            ),
        ));
    }
    log
}
fn pocket_log_with_initial_listing(missing: bool) -> String {
    let start = QUOTE_START;
    let mut log = String::new();
    let asset = |symbol: &str| {
        let mut row = vec![Value::Null; 19];
        row[1] = json!(symbol);
        row[5] = json!(84);
        row
    };
    let assets = json!([asset(shared::SYMBOLS[0]), asset(shared::SYMBOLS[1])]);
    for session in ["account", "market"] {
        let at = start
            - if session == "account" {
                2_000_000
            } else {
                1_500_000
            };
        log.push_str(&pocket_line(session, at, "0{}"));
        log.push_str(&pocket_line(session, at + 1, "40{}"));
        log.push_str(&pocket_line(
            session,
            at + 2,
            &pocket_event("successauth", json!({})),
        ));
        log.push_str(&format!(
            "{}\n",
            json!({"session":session,"at":at+2,"expect":"42[\"ps\",null]"})
        ));
        if session == "market" {
            log.push_str(&pocket_line(
                session,
                at + 3,
                &pocket_event("successupdateBalance", json!({"isDemo":1,"balance":10000})),
            ));
        }
        log.push_str(&pocket_binary(
            session,
            at + 200_003,
            "updateAssets",
            if missing && session == "account" {
                json!([])
            } else {
                assets.clone()
            },
        ));
        if session == "account" {
            let older = json!({"id":"synthetic-older","asset":shared::SYMBOLS[0],"command":1,
                "amount":1,"profit":0,"percentProfit":92,"openPrice":1.0,"closePrice":1.0,
                "openTimestamp":start/1_000_000-7200,"openMs":0,
                "closeTimestamp":start/1_000_000-3600,"closeMs":0,
                "isDemo":1,"currency":"USD","requestId":33333333,"optionType":100});
            log.push_str(&pocket_line(
                session,
                at + 200_004,
                &pocket_event("updateOpenedDeals", json!([])),
            ));
            log.push_str(&pocket_line(
                session,
                at + 200_005,
                &pocket_event("successupdateBalance", json!({"isDemo":1,"balance":10000})),
            ));
            log.push_str(&pocket_line(
                session,
                at + 200_006,
                &pocket_event("updateClosedDeals", json!([older])),
            ));
        }
    }
    let quote = |at: i64, units: i64| {
        let provider =
            serde_json::from_str::<Value>(&format!("{}.{:06}", at / 1_000_000, at % 1_000_000))
                .unwrap();
        let price =
            serde_json::from_str::<Value>(&format!("{}.{:05}", units / 100_000, units % 100_000))
                .unwrap();
        json!([[shared::SYMBOLS[0], provider, price]])
    };
    let mut first = quote(start, 100_494);
    first
        .as_array_mut()
        .unwrap()
        .push(json!([shared::SYMBOLS[1], start / 1_000_000, 1.00494]));
    log.push_str(&pocket_binary(
        "market",
        start + 200_000,
        "updateStream",
        first,
    ));
    if missing {
        log.push_str(&pocket_line(
            "market",
            start + 220_000,
            &pocket_event("updateStream", quote(start + 100_000, 100_344)),
        ));
        log.push_str(&pocket_line(
            "market",
            start + 230_000,
            &pocket_event("updateStream", quote(start + 200_000, 100_494)),
        ));
        log.push_str(&pocket_line(
            "account",
            start + 250_000,
            &pocket_event("updateAssets", assets.clone()),
        ));
    }
    log.push_str(&pocket_binary(
        "market",
        start + 500_000,
        "updateStream",
        quote(start + 300_000, 100_644),
    ));
    log.push_str(&format!("{}\n", json!({"session":"account","at":start+600_000,
        "expect":format!("42[\"openOrder\",{{\"asset\":\"{}\",\"amount\":1,\"action\":\"put\",\"isDemo\":1,\"requestId\":11111111,\"optionType\":100,\"time\":30}}]", shared::SYMBOLS[0])})));
    let opened = json!({"id":"synthetic-one","asset":shared::SYMBOLS[0],"command":1,"amount":1,"profit":0,
        "percentProfit":92,"openPrice":1.00644,"closePrice":null,"openTimestamp":start/1_000_000,
        "openMs":600,"closeTimestamp":start/1_000_000+30,"isDemo":1,"currency":"USD","requestId":11111111,"optionType":100});
    log.push_str(&pocket_line(
        "account",
        start + 700_000,
        &pocket_event("successopenOrder", opened.clone()),
    ));
    log.push_str(&pocket_line(
        "account",
        start + 800_000,
        &pocket_event("successupdateBalance", json!({"isDemo":1,"balance":9999})),
    ));
    log.push_str(&pocket_binary(
        "market",
        start + 1_000_000,
        "updateStream",
        quote(start + 800_000, 100_800),
    ));
    let mut closed = opened;
    closed["profit"] = json!(0.92);
    closed["closePrice"] = json!(1.00500);
    closed["closeMs"] = json!(200);
    log.push_str(&pocket_binary(
        "market",
        start + 30_200_000,
        "updateStream",
        quote(start + 30_000_000, 100_500),
    ));
    log.push_str(&pocket_line(
        "market",
        start + 30_250_000,
        &pocket_event("updateStream", quote(start + 30_050_000, 100_650)),
    ));
    log.push_str(&pocket_line(
        "market",
        start + 30_300_000,
        &pocket_event("updateStream", quote(start + 30_100_000, 100_800)),
    ));
    log.push_str(&pocket_line(
        "account",
        start + 30_400_000,
        &pocket_event("successcloseOrder", json!({"profit":1.92,"deals":[closed]})),
    ));
    log.push_str(&pocket_line(
        "account",
        start + 30_500_000,
        &pocket_event(
            "successupdateBalance",
            json!({"isDemo":1,"balance":10000.92}),
        ),
    ));
    let listing = |percent| {
        let mut rows = assets.clone();
        rows[0][5] = json!(percent);
        rows
    };
    log.push_str(&pocket_binary(
        "account",
        start + 31_000_000,
        "updateAssets",
        listing(49),
    ));
    log.push_str(&pocket_binary(
        "market",
        start + 31_500_000,
        "updateStream",
        quote(start + 31_300_000, 100_650),
    ));
    log.push_str(&pocket_binary(
        "market",
        start + 31_800_000,
        "updateStream",
        quote(start + 31_600_000, 100_660),
    ));
    log.push_str(&pocket_binary(
        "account",
        start + 32_200_000,
        "updateAssets",
        listing(84),
    ));
    log.push_str(&pocket_binary(
        "market",
        start + 32_700_000,
        "updateStream",
        quote(start + 32_500_000, 100_810),
    ));
    log.push_str(&format!("{}\n", json!({"session":"account","at":start+32_800_000,
        "expect":format!("42[\"openOrder\",{{\"asset\":\"{}\",\"amount\":1,\"action\":\"put\",\"isDemo\":1,\"requestId\":22222222,\"optionType\":100,\"time\":30}}]", shared::SYMBOLS[0])})));
    let second = json!({"id":"synthetic-two","asset":shared::SYMBOLS[0],"command":1,"amount":1,"profit":0,
        "percentProfit":92,"openPrice":1.00810,"closePrice":null,"openTimestamp":start/1_000_000+32,
        "openMs":800,"closeTimestamp":start/1_000_000+62,"isDemo":1,"currency":"USD","requestId":22222222,"optionType":100});
    log.push_str(&pocket_line(
        "account",
        start + 32_900_000,
        &pocket_event("successopenOrder", second.clone()),
    ));
    log.push_str(&pocket_line(
        "account",
        start + 33_000_000,
        &pocket_event(
            "successupdateBalance",
            json!({"isDemo":1,"balance":9999.92}),
        ),
    ));
    let mut second_close = second;
    second_close["profit"] = json!(0.92);
    second_close["closePrice"] = json!(1.00700);
    second_close["closeMs"] = json!(200);
    log.push_str(&pocket_binary(
        "market",
        start + 62_200_000,
        "updateStream",
        quote(start + 62_000_000, 100_700),
    ));
    log.push_str(&pocket_line(
        "market",
        start + 62_250_000,
        &pocket_event("updateStream", quote(start + 62_050_000, 100_850)),
    ));
    log.push_str(&pocket_line(
        "market",
        start + 62_300_000,
        &pocket_event("updateStream", quote(start + 62_100_000, 101_000)),
    ));
    log.push_str(&pocket_line(
        "account",
        start + 62_400_000,
        &pocket_event(
            "successcloseOrder",
            json!({"profit":1.92,"deals":[second_close]}),
        ),
    ));
    log.push_str(&pocket_line(
        "account",
        start + 62_500_000,
        &pocket_event(
            "successupdateBalance",
            json!({"isDemo":1,"balance":10001.84}),
        ),
    ));
    log
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
pub fn deriv_quote_authorization_log(pregrant_reply: bool) -> String {
    let start = QUOTE_START;
    let base = start - 2_000_000;
    let mut log = String::new();
    for frame in [
        r#"{"data":[{"account_id":"SYNTHETICACCOUNT","account_type":"demo","status":"active","currency":"USD"}]}"#,
        r#"{"data":{"url":"ws://127.0.0.1/trading/v1/options/ws/demo"}}"#,
    ] {
        log.push_str(&log_line("bootstrap", base, frame));
    }
    log.push_str(&log_line(
        "account",
        base,
        r#"{"msg_type":"portfolio","req_id":21,"portfolio":{"contracts":[]}}"#,
    ));
    log.push_str(&log_line("account", base, &frame("transaction-ack")));
    log.push_str(&log_line(
        "account",
        base,
        &change(&frame("balance-before"), "balance", "balance", "10000"),
    ));
    let tick = |symbol: &str, at: i64, quote: &str| {
        let mut value = crate::common::broker::fixture("deriv-tick-R_50.json");
        let subscription = if symbol == shared::SYMBOLS[0] {
            "synthetic-subscription-1"
        } else {
            "synthetic-subscription-2"
        };
        for (owner, key, replacement) in [
            ("echo_req", "ticks", format!("\"{symbol}\"")),
            ("tick", "symbol", format!("\"{symbol}\"")),
            ("tick", "id", format!("\"{subscription}\"")),
            ("subscription", "id", format!("\"{subscription}\"")),
            ("tick", "quote", quote.into()),
            ("tick", "epoch", (at / 1_000_000).to_string()),
            ("tick", "pip_size", "5".into()),
        ] {
            value = change(&value, owner, key, &replacement);
        }
        value
    };
    log.push_str(&log_line(
        "market",
        start,
        &tick(shared::SYMBOLS[0], start, "1.00644"),
    ));
    let proposal = |id: u64, spot: &str, side: &str| {
        let mut value = frame("proposal-call");
        for (owner, key, replacement) in [
            ("echo_req", "amount", "1".into()),
            ("echo_req", "contract_type", format!("\"{side}\"")),
            ("echo_req", "duration", "30".into()),
            (
                "echo_req",
                "underlying_symbol",
                format!("\"{}\"", shared::SYMBOLS[0]),
            ),
            ("echo_req", "req_id", id.to_string()),
            ("proposal", "ask_price", "1".into()),
            ("proposal", "payout", "1.92".into()),
            ("proposal", "date_start", (start / 1_000_000).to_string()),
            (
                "proposal",
                "date_expiry",
                (start / 1_000_000 + 30).to_string(),
            ),
            ("proposal", "spot", spot.into()),
            ("proposal", "spot_time", (start / 1_000_000).to_string()),
            ("proposal", "id", format!("\"synthetic-proposal-{id}\"")),
        ] {
            value = change(&value, owner, key, &replacement);
        }
        let mut value: Value = serde_json::from_str(&value).unwrap();
        value["req_id"] = json!(id);
        value.to_string()
    };
    if pregrant_reply {
        log.push_str(&log_line(
            "account",
            start + 200_000,
            &proposal(4, "1.00644", "PUT"),
        ));
    }
    log.push_str(&log_line(
        "market",
        start + 1_000_000,
        &tick(shared::SYMBOLS[0], start + 1_000_000, "1.00800"),
    ));
    log.push_str(&log_line(
        "account",
        start + 1_100_000,
        &proposal(if pregrant_reply { 5 } else { 4 }, "1.00800", "PUT"),
    ));
    log
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
    runtime_with_clocks(
        fixture,
        mode,
        recorded,
        control,
        edit,
        wrap_market,
        wrap_account,
        recorded.clock(),
        None,
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
    runtime_with_clocks(
        fixture,
        mode,
        recorded,
        control,
        edit,
        wrap_market,
        wrap_account,
        clock,
        None,
    )
}

/// Overrides only the owner's local clock for measured database round-trip/skew scenarios.
#[allow(clippy::too_many_arguments)]
pub fn runtime_with_owner_clock(
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
    owner_clock: Option<Box<dyn binary_alpha_app::broker::Clock>>,
) -> Result<live::Runtime, String> {
    runtime_with_clocks(
        fixture,
        mode,
        recorded,
        control,
        edit,
        wrap_market,
        wrap_account,
        recorded.clock(),
        owner_clock,
    )
}

/// The adapters use `clock`; the owner uses `owner_clock` or the same clock; the recorded
/// scheduler always drives the log.
#[allow(clippy::too_many_arguments)]
fn runtime_with_clocks(
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
    owner_clock: Option<Box<dyn binary_alpha_app::broker::Clock>>,
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
        DerivAccounts::bootstrap(settings, &mut recorded.http(), "synthetic-no-credential")?;
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
        owner_clock.unwrap_or_else(|| Box::new(clock.clone())),
        Some(recorded.clock()),
        mode,
    )
}

/// A separate synthetic host, retaining the exact source/configuration/deployment identity.
pub fn isolated_fixture(fixture: &Fixture, name: &str) -> Fixture {
    let root = fixture.scratch.path(name);
    fs::create_dir_all(&root).unwrap();
    Fixture {
        scratch: Scratch { root: root.clone() },
        config: fixture.config.clone(),
        path: root.join("live.toml"),
        bundle: fixture.bundle.clone(),
        run: fixture.run.clone(),
        datasets: fixture.datasets.clone(),
    }
}

pub fn pocket_runtime(
    fixture: &Fixture,
    mode: live::Mode,
    recorded: &binary_alpha_app::broker::transport::RecordedConnector,
    control: live::control::FakeControl,
) -> Result<live::Runtime, String> {
    pocket_runtime_with_market(fixture, mode, recorded, control, |market| market)
}
pub fn pocket_runtime_with_market(
    fixture: &Fixture,
    mode: live::Mode,
    recorded: &binary_alpha_app::broker::transport::RecordedConnector,
    control: live::control::FakeControl,
    wrap_market: impl FnOnce(
        Box<dyn binary_alpha_app::broker::MarketDataBroker>,
    ) -> Box<dyn binary_alpha_app::broker::MarketDataBroker>,
) -> Result<live::Runtime, String> {
    pocket_runtime_with_transports(
        fixture,
        mode,
        recorded,
        control,
        |account| Box::new(account),
        wrap_market,
    )
}
pub fn pocket_runtime_with_transports(
    fixture: &Fixture,
    mode: live::Mode,
    recorded: &binary_alpha_app::broker::transport::RecordedConnector,
    control: live::control::FakeControl,
    wrap_account: impl FnOnce(
        binary_alpha_app::broker::transport::RecordedConnector,
    ) -> Box<dyn binary_alpha_app::broker::transport::Connector>,
    wrap_market: impl FnOnce(
        Box<dyn binary_alpha_app::broker::MarketDataBroker>,
    ) -> Box<dyn binary_alpha_app::broker::MarketDataBroker>,
) -> Result<live::Runtime, String> {
    use binary_alpha_app::broker::{
        AccountIdentity, pocket_option::PocketMarketData, pocket_options::PocketOptions,
    };
    use binary_alpha_engine::config::{AccountClass, Broker};
    let Broker::PocketOption(settings) = &fixture.config.brokers[0] else {
        panic!("Pocket fixture")
    };
    let definition = fixture.definition();
    let instruments = definition
        .definition
        .instruments
        .iter()
        .map(|instrument| {
            Ok((
                binary_alpha_engine::market::InstrumentId {
                    broker: instrument.broker.clone(),
                    provider_symbol: instrument.provider_symbol.clone(),
                },
                instrument.price_scale.try_into()?,
            ))
        })
        .collect::<Result<Vec<_>, String>>()?;
    let ids = instruments
        .iter()
        .map(|(id, _)| id.clone())
        .collect::<Vec<_>>();
    let clock = recorded.clock();
    let account = AccountIdentity {
        broker: settings.id.clone(),
        account: fixture.config.live.as_ref().unwrap().account.clone(),
        class: AccountClass::Demo,
        currency: definition.policy.replay.accounts[0].currency.clone(),
    };
    let options = PocketOptions::connect(
        settings,
        account,
        &instruments,
        wrap_account(recorded.session("account")?),
        Box::new(clock.clone()),
        "{}".into(),
    )?;
    let market = PocketMarketData::connect(
        settings,
        &ids,
        Box::new(recorded.session("market")?),
        Box::new(clock.clone()),
        "{}".into(),
    )?;
    let (local, destination) = fixture.stores();
    live::Runtime::start(
        &fixture.config,
        &fixture.scratch.root,
        definition,
        local,
        destination,
        Box::new(control),
        wrap_market(Box::new(market)),
        options,
        Box::new(clock.clone()),
        Some(clock),
        mode,
    )
}

pub fn scenario_rows(log: &str) -> Vec<Value> {
    log.lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect()
}
pub fn scenario_log(rows: &[Value]) -> String {
    rows.iter().map(|row| format!("{row}\n")).collect()
}
pub fn account_row(at: i64, frame: &str) -> Value {
    serde_json::from_str(&log_line("account", at, frame)).unwrap()
}
pub fn scenario_tick(at: i64, price: &str) -> Value {
    let tick = change(
        &change(
            &crate::common::broker::fixture("deriv-tick-R_50.json"),
            "tick",
            "epoch",
            &(at / 1_000_000).to_string(),
        ),
        "tick",
        "quote",
        price,
    );
    serde_json::from_str(&log_line("market", at, &tick)).unwrap()
}
pub fn ledger_events(owner: &live::Runtime) -> Vec<binary_alpha_engine::execution::FinancialEvent> {
    owner
        .records()
        .iter()
        .filter_map(|r| match &r.kind {
            live::journal::RecordKind::Ledger { event } => Some(event.clone()),
            _ => None,
        })
        .collect()
}
/// Computes the real receipt owner against a runtime's current (possibly incomplete) journal.
pub fn scenario_receipt(fixture: &Fixture, owner: &live::Runtime) -> live::receipt::Receipt {
    scenario_receipt_records(fixture, owner, owner.records())
}

/// Measures an exact journal prefix without synthesizing or editing financial events.
pub fn scenario_receipt_records(
    fixture: &Fixture,
    owner: &live::Runtime,
    records: &[live::journal::Record],
) -> live::receipt::Receipt {
    let settings = &fixture.config.live.as_ref().unwrap().compatibility;
    live::receipt::compute(&live::receipt::Inputs {
        deployment: &owner.definition.deployment,
        definition: &owner.definition.definition,
        baseline: &owner.definition.policy.baseline,
        source: &owner.definition.policy.source,
        account_class: owner.health().account_class,
        required_account_class: settings.required_account_class,
        observation: &binary_alpha_engine::research::Window {
            decision_start: settings.observation_start.clone(),
            decision_end: settings.observation_end.clone(),
        },
        min_samples: settings.min_samples,
        scenarios: &owner.definition.scenarios,
        ledger: &owner.definition.manifest.definition,
        events: &records
            .iter()
            .filter_map(|r| match &r.kind {
                live::journal::RecordKind::Ledger { event } => Some(event.clone()),
                _ => None,
            })
            .collect::<Vec<_>>(),
        refusals: records,
    })
}

/// Writes an interrupted synthetic host's prefix into a fresh host; completed evidence stays put.
pub fn seed_scenario_prefix(fixture: &Fixture, records: &[live::journal::Record]) {
    let (mut journal, existing) = live::journal::Journal::open(
        &fixture.scratch.path("journal"),
        &fixture.definition().deployment,
        u64::from(
            fixture
                .config
                .live
                .as_ref()
                .unwrap()
                .journal
                .segment_records,
        ),
    )
    .unwrap();
    assert_eq!(existing, []);
    for record in records {
        assert_eq!(
            journal
                .append(record.time_micros, record.kind.clone())
                .unwrap(),
            *record
        );
    }
    journal
        .append(
            records.last().unwrap().time_micros,
            live::journal::RecordKind::Discontinuity {
                reason: "synthetic scenario restart boundary".into(),
            },
        )
        .unwrap();
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

pub fn authorize(fixture: &Fixture) {
    let definition = fixture.definition();
    let manifest = definition.manifest;
    let (local, destination) = fixture.stores();
    live::authorization::create(
        &destination,
        &local,
        live::authorization::Authorization {
            schema_version: 1,
            deployment: manifest.hash,
            configuration: manifest.config_hash,
            bundle_sha256: manifest.bundle_sha256,
            broker: manifest.broker,
            account: manifest.account,
            operator: "synthetic-operator".into(),
            reason: "synthetic runtime regression".into(),
            hash: String::new(),
        },
    )
    .unwrap();
}

/// Snapshots immutable fixture publications before a continued run.
pub fn published_snapshot(fixture: &Fixture) -> std::collections::BTreeMap<PathBuf, Vec<u8>> {
    fn visit(root: &Path, out: &mut std::collections::BTreeMap<PathBuf, Vec<u8>>) {
        for entry in fs::read_dir(root).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                visit(&path, out);
            } else {
                out.insert(path.clone(), fs::read(&path).unwrap());
            }
        }
    }
    let mut out = std::collections::BTreeMap::new();
    visit(&fixture.scratch.path("published"), &mut out);
    out
}

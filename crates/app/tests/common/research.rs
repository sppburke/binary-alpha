//! Pure, non-sensitive Phase 11 configuration shared by CLI parsing and integration fixtures.
#![allow(dead_code)]

use std::path::Path;

use binary_alpha_engine::config::{Config, ManifestUri};
use binary_alpha_engine::market::format_event_time_micros;
use serde_json::{Value, json};

pub const BASE: i64 = 1_767_571_200_000_000;
pub const HOUR: i64 = 3_600_000_000;
pub const CANDLE: i64 = 20_000_000;
pub const ROWS: usize = 32;
pub const INSTRUMENTS: [&str; 2] = ["pocket_option:AEDCNY_otc", "pocket_option:SECOND_otc"];
pub const SYMBOLS: [&str; 2] = ["AEDCNY_otc", "SECOND_otc"];
pub const CURRENCIES: [&str; 2] = ["unit", "eur"];
pub const SCALES: [u8; 2] = [6, 3];
pub const STREAM: &str = "{ duration_seconds = 20, offset_seconds = 0 }";

pub fn time(micros: i64) -> String {
    format_event_time_micros(micros)
}

pub fn uri(root: &Path, generation: &str) -> ManifestUri {
    format!(
        "file://{}/published/manifests/{generation}/ready.json",
        root.display()
    )
    .parse()
    .unwrap()
}

pub fn contract(id: &str, currency: &str, large: bool, worse: bool) -> Value {
    let (cost, fee, win, tie) = match (large, worse) {
        (false, false) => ("1", "0", "1.80", "1"),
        (true, false) => ("2", "0.05", "3.60", "2.05"),
        (false, true) => ("1", "0", "1.70", "1"),
        (true, true) => ("2", "0.05", "3.50", "2.05"),
    };
    json!({"id":id,"direction":"buy","duration_micros":5_000_000,"currency":currency,
        "stake":cost,"quoted_cost":cost,"entry_fee":fee,
        "win":{"gross_return":win,"terminal_fee":"0"},
        "loss":{"gross_return":"0","terminal_fee":"0"},
        "tie":{"gross_return":tie,"terminal_fee":"0"},
        "settlement":{"rule":"price_at_due_v1","max_settlement_delay_micros":2_000_000,"max_tick_gap_micros":2_000_000}})
}

pub fn envelope(large: bool, worse: bool) -> Value {
    json!({"max_purchase_cost":if large {"2"} else {"1"},
        "max_entry_fee":if large {"0.05"} else {"0"},
        "max_win_terminal_fee":"0","max_loss_terminal_fee":"0","max_tie_terminal_fee":"0",
        "min_winning_net_return":match (large,worse) {(true,false)=>"1.55",(true,true)=>"1.45",(false,false)=>"0.80",(false,true)=>"0.70"},
        "settlement_rule":"price_at_due_v1"})
}

/// No input objects are needed to validate this complete TOML-compatible configuration.
/// Integration fixtures replace the placeholder references with imported synthetic generations.
pub fn configuration(root: &Path) -> Config {
    let mut config = Config::parse(&format!(
        "schema_version = 1\nrun_mode = \"research\"\n[storage]\nhistorical_data_dir = \"{}/retained\"\npublication_uri = \"file://{}/published\"\n",
        root.display(), root.display()
    )).unwrap();
    config.instruments = (0..2).map(|i| serde_json::from_value(json!({
        "broker":"pocket_option","provider_symbol":SYMBOLS[i],"quote_currency":CURRENCIES[i],
        "price_scale":SCALES[i],"native_granularity":{"kind":"tick"},
        "session":{"kind":"always"},
        "gap":{"max_seconds":2,"reopen_seconds":60},
        "candles":[{"duration_seconds":20,"offset_seconds":0,"min_observations":9,"hard_min_observations":5}]
    })).unwrap()).collect();
    let reference = |n: usize| uri(root, &format!("{n:064x}")).to_string();
    let window = |hour: i64, first: usize| {
        json!({
            "decision_start":time(BASE+hour*HOUR+CANDLE),
            "decision_end":time(BASE+hour*HOUR+(ROWS as i64+1)*CANDLE),
            "inputs":[reference(first),reference(first+1)],
            "splits":[
                {"name":"a","start":time(BASE+hour*HOUR+CANDLE),"end":time(BASE+hour*HOUR+17*CANDLE+2_000_000)},
                {"name":"b","start":time(BASE+hour*HOUR+17*CANDLE+2_000_000),"end":time(BASE+hour*HOUR+(ROWS as i64+1)*CANDLE)}]
        })
    };
    let gates = json!({"min_settled":1,"max_unresolved":0,"min_profit":"0","max_drawdown":"1000"});
    let risk = json!({"id":"cap2","max_open_total":2,"same_entry":"all","deduplicate_signal_logic":false,
        "max_feature_age_micros":60_000_000,"max_quote_age_micros":0});
    let instruments: Vec<Value> = (0..2).map(|i| json!({
        "instrument":INSTRUMENTS[i],"source_manifest":reference(1+i),
        "features":{"streams":[{"duration_seconds":20,"offset_seconds":0}],"outputs":["candle_direction","range_bps"]},
        "outcomes":{"expiry_seconds":[5],"max_entry_delay_ms":2000,"max_settlement_delay_ms":2000,"max_tick_gap_ms":2000,
            "true_jump_max_gap_ms":2000,"true_jump_basis_points":"5","frozen_min_ticks":10,"frozen_min_ms":5000},
        "search":{"decision_start":time(BASE+CANDLE),"decision_end":time(BASE+(ROWS as i64+1)*CANDLE),
            "scope":"exhaustive","seed":7,"chunk_size":8,"max_candidates":100,"min_conditions":1,"max_conditions":1,
            "embargo_micros":7_000_000,"base_stream":{"duration_seconds":20,"offset_seconds":0},
            "conditions":[{"stream":{"duration_seconds":20,"offset_seconds":0},"output":"candle_direction","comparator":"eq","thresholds":["up","down"]}],
            "contracts":[contract(&format!("{i}-small"),CURRENCIES[i],false,false)],
            "account":{"broker":"pocket_option","currency":CURRENCIES[i],"scale":2,"initial_cash":"1000"},
            "risk_policy":{"id":"one","max_open_per_strategy":1,"same_entry":"all","deduplicate_signal_logic":false,"max_feature_age_micros":60_000_000,"max_quote_age_micros":0},
            "envelope":envelope(false,false),"gates":{"min_settled":1,"max_unresolved":0,"min_net_profit":"0"},
            "stability":{"block_length":4,"simulations":16,"rolling_horizon":4}}
    })).collect();
    let bindings: Vec<Value> = (0..2).map(|i| json!({
        "id":format!("b{i}"),"account":format!("a{i}"),"instrument":INSTRUMENTS[i],
        "alternatives":[
            {"contract":contract(&format!("{i}-small"),CURRENCIES[i],false,false),"envelope":envelope(false,false)},
            {"contract":contract(&format!("{i}-large"),CURRENCIES[i],true,false),"envelope":envelope(true,false)}]
    })).collect();
    let accounts: Vec<Value> = (0..2).map(|i| json!({"id":format!("a{i}"),"broker":"pocket_option","currency":CURRENCIES[i],"scale":2,"initial_cash":"10000"})).collect();
    let scenarios: Vec<Value> = [("delayed",100_000,false),("worse_terms",0,true)].into_iter().map(|(id,delay,worse)| {
        let alternatives: Vec<Value> = (0..2).map(|i| json!({"binding":format!("b{i}"),
            "contract":contract(&format!("{i}-large"),CURRENCIES[i],true,worse),"envelope":envelope(true,worse)})).collect();
        json!({"id":id,"acceptance_delay_micros":delay,"alternatives":alternatives})
    }).collect();
    config.research = Some(serde_json::from_value(json!({
        "study":{"study":"synthetic","attempt":"first","governance_manifest":format!("file://{}/declaration.json",root.display()),"changes":"initial attempt"},
        "instruments":instruments,
        "folds":[{"cutoff":time(BASE+(ROWS as i64+1)*CANDLE),"decision_start":time(BASE+HOUR+CANDLE),"decision_end":time(BASE+HOUR+(ROWS as i64+1)*CANDLE),
            "inputs":[{"fit_manifest":reference(1),"assessment_manifest":reference(3)},{"fit_manifest":reference(2),"assessment_manifest":reference(4)}]}],
        "refit":{"cutoff":time(BASE+2*HOUR+(ROWS as i64+1)*CANDLE),"fits":[reference(5),reference(6)]},
        "evaluation":window(3,7),"holdout":window(4,9),
        "portfolio":{"max_policies":12,"embargo_micros":7_000_000,"objective":"profit_then_drawdown","gates":gates,
            "accounts":accounts,"reporting_currency":"unit","reporting_scale":2,"max_rate_age_micros":24*HOUR,
            "rates":[{"id":"eur-unit","source_currency":"eur","reporting_currency":"unit","provider":"synthetic-fx","provider_time":time(BASE),"available_at":time(BASE),"rate":"2"}],
            "members":[{"family":0,"member":0},{"family":0,"member":1},{"family":1,"member":0},{"family":1,"member":1}],
            "repairs":[{"id":"none"},{"id":"narrow","conditions":[{"stream":{"duration_seconds":20,"offset_seconds":0},"output":"range_bps","comparator":"lt","threshold":0.08}]}],
            "bindings":bindings,
            "subsets":[
                {"deployments":[{"member":0,"repair":1,"binding":0}]},
                {"deployments":[{"member":3,"repair":1,"binding":1}]},
                {"deployments":[{"member":0,"repair":1,"binding":0},{"member":3,"repair":1,"binding":1}]},
                {"deployments":[{"member":0,"repair":0,"binding":0}]},
                {"deployments":[{"member":3,"repair":0,"binding":1}]}],
            "risk_policies":[risk]},
        "scenarios":scenarios,"qualification":{"claim":"empirical_policy_qualification_v1","gates":gates}
    })).unwrap());
    Config::parse(&config.canonical_toml()).unwrap()
}

pub fn replay_configuration(root: &Path) -> Config {
    let mut config = configuration(root);
    let research = config.research.take().unwrap();
    let inputs: Vec<Value> = (0..2).map(|i| json!({"tick_manifest":uri(root,&format!("{:064x}",i+1)),"feature_manifest":uri(root,&format!("{:064x}",i+3))})).collect();
    let strategies: Vec<Value> = (0..2).map(|i| json!({"id":format!("s{i}"),"plan_identity":format!("plan{i}"),"base_stream":{"duration_seconds":20,"offset_seconds":0},
        "conditions":[{"stream":{"duration_seconds":20,"offset_seconds":0},"output":"candle_direction","comparator":"eq","threshold":"down"}]})).collect();
    let bindings: Vec<Value> = (0..2).map(|i| json!({"id":format!("b{i}"),"strategy":format!("s{i}"),"account":format!("a{i}"),"instrument":INSTRUMENTS[i],
        "contract":format!("{i}-small"),"risk_policy":"cap2","envelope":envelope(false,false)})).collect();
    config.replay = Some(serde_json::from_value(json!({
        "role":"development","decision_start":time(BASE+CANDLE),"decision_end":time(BASE+(ROWS as i64+1)*CANDLE),
        "inputs":inputs,"accounts":research.portfolio.accounts,"strategies":strategies,"bindings":bindings,
        "contracts":[contract("0-small","unit",false,false),contract("1-small","eur",false,false)],
        "risk_policies":research.portfolio.risk_policies,"reporting_currency":"unit","reporting_scale":2,
        "max_rate_age_micros":research.portfolio.max_rate_age_micros,"rates":research.portfolio.rates
    })).unwrap());
    Config::parse(&config.canonical_toml()).unwrap()
}

#[derive(Clone, Copy)]
pub struct Row {
    pub up: bool,
    pub wide: bool,
    pub win: bool,
}

pub fn recipe(cells: [u8; 4]) -> Vec<Row> {
    (0..ROWS)
        .map(|k| {
            let (up, wide) = (k % 4 >= 2, k % 2 == 1);
            let cell = match (up, wide) {
                (true, false) => 0,
                (true, true) => 1,
                (false, false) => 2,
                (false, true) => 3,
            };
            Row {
                up,
                wide,
                win: cells[cell] & (1 << (k / 4)) != 0,
            }
        })
        .collect()
}

/// Phase 09's planted candle recipe; the five-second outcome persists for one extra tick
/// so the required 100 ms acceptance delay has the same known outcomes. Both scales have
/// the same relative movement: B's numeric price is 1000 times A's, not a rounded A price.
pub fn ticks_at_scale(base: i64, rows: &[Row], scale: u8) -> Vec<String> {
    let mut lines = Vec::new();
    for k in 0..=rows.len() {
        for step in 0..80 {
            let previous = k.checked_sub(1).map(|i| rows[i]);
            let current = rows.get(k);
            let price = 1_800_000
                + match (step, current) {
                    (0, _) => 0,
                    (20 | 21, _) => {
                        if previous.is_none_or(|r| r.win) {
                            2
                        } else {
                            -2
                        }
                    }
                    (32, Some(r)) => {
                        if r.wide {
                            12
                        } else {
                            4
                        }
                    }
                    (48, Some(r)) => {
                        if r.wide {
                            -12
                        } else {
                            -4
                        }
                    }
                    (79, Some(r)) => {
                        if r.up {
                            1
                        } else {
                            -1
                        }
                    }
                    _ => {
                        if step % 2 == 0 {
                            1
                        } else {
                            -1
                        }
                    }
                };
            let divisor = 10_i64.pow(u32::from(scale));
            lines.push(format!(
                "{},SYNTHETIC,{}.{:0width$}",
                time(base + k as i64 * CANDLE + step * 250_000),
                price / divisor,
                price % divisor,
                width = usize::from(scale)
            ));
        }
    }
    lines
}

/// Writes invented consumer inputs directly. Development uses daily-v2; synthetic-only
/// evaluation and holdout use the supported legacy reader format without a CLI writer.
pub fn import_ticks(
    root: &Path,
    name: &str,
    role: binary_alpha_engine::dataset::DatasetRole,
    broker: &str,
    symbols: &[&str],
    scales: &[u8],
    lines: &[Vec<String>],
) -> Vec<binary_alpha_engine::dataset::GenerationManifest> {
    use binary_alpha_engine::market::{InstrumentId, ProviderSymbol, parse_tick_line};
    let source_symbol: ProviderSymbol = "SYNTHETIC".to_string().try_into().unwrap();
    lines
        .iter()
        .enumerate()
        .map(|(i, lines)| {
            // Keep the independent invented source inventory available to fixture oracles.
            crate::common::write_ticks(
                &root.join(format!("sources/{name}-{i}.csv")),
                &lines.iter().map(String::as_str).collect::<Vec<_>>(),
            );
            let scale = scales[i].try_into().unwrap();
            let rows = lines
                .iter()
                .map(|line| parse_tick_line(line, &source_symbol, scale).unwrap())
                .collect::<Vec<_>>();
            crate::common::current::ticks_with_source(
                &root.join("published"),
                &InstrumentId {
                    broker: broker.to_string().try_into().unwrap(),
                    provider_symbol: symbols[i].to_string().try_into().unwrap(),
                },
                role,
                scale,
                &rows,
                Some(&root.join(format!("sources/{name}-{i}.csv"))),
            )
            .unwrap()
        })
        .collect()
}

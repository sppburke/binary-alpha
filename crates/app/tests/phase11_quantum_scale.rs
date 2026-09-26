//! Manual, synthetic-only Study P resource gate on the quantum CUDA runner. Run it in a
//! swap-disabled memory scope so shared-host reclaim cannot swap gate memory, and memory that
//! does not fit fails the gate:
//! `systemd-run --user --scope -p MemorySwapMax=0 cargo test --release --locked -p binary-alpha-app --features cuda --test phase11_quantum_scale quantum_study_p_combined_scale -- --exact --ignored --nocapture`.
#![cfg(feature = "cuda")]
mod common;
#[path = "common/research.rs"]
mod fixture;

use std::collections::BTreeSet;
use std::fs;
use std::path::Path;
use std::process::Command;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use binary_alpha_engine::config::{
    DataSplit, EncodingSpec, Encodings, GeneratedSearchCondition, Outputs, PortfolioGenerate,
    Scope, Screen, SearchCondition, StreamKey,
};
use binary_alpha_engine::dataset::coverage::CoverageRange;
use binary_alpha_engine::dataset::daily::DAY_MICROS;
use binary_alpha_engine::dataset::manifest_key;
use binary_alpha_engine::execution::{Comparator, Decimal, Direction};
use binary_alpha_engine::features::{FeatureManifest, FeaturePlan};
use binary_alpha_engine::portfolio::Selection;
use binary_alpha_engine::research::{self, Declaration, Grant, Run, RunState};
use binary_alpha_engine::search::{Family, FamilyManifest};
use common::{Scratch, cli, cli_as};
use fixture::{BASE, Row, bar_rows, time};
use serde_json::Value;
use sha2::{Digest, Sha256};

const RESOURCES: &[u8] = include_bytes!("../../../configs/study_p_resources.toml");
const RESOURCES_SHA256: &str = "09a7976b6cacd2c5efea2895a155898b50fc54012342df026786a82c223c37b1";
const DAYS: i64 = 493;
const BARS_PER_DAY: usize = 17_280;
const OPERATOR: &str = "synthetic-quantum-operator";

fn number(config: &toml::Value, path: &[&str]) -> u64 {
    let mut value = config;
    for key in path {
        value = value
            .get(key)
            .unwrap_or_else(|| panic!("missing resource {path:?}"));
    }
    value.as_integer().unwrap().try_into().unwrap()
}

fn numbers(config: &toml::Value, path: &[&str]) -> Vec<u64> {
    let mut value = config;
    for key in path {
        value = value
            .get(key)
            .unwrap_or_else(|| panic!("missing resource {path:?}"));
    }
    value
        .as_array()
        .unwrap()
        .iter()
        .map(|item| item.as_integer().unwrap().try_into().unwrap())
        .collect()
}

fn stage(name: &str, started: Instant, report: &str) {
    println!(
        "stage {name} elapsed {:.3}s {report}",
        started.elapsed().as_secs_f64()
    );
}

fn swap_used_kb() -> u64 {
    let text = fs::read_to_string("/proc/meminfo").unwrap();
    let at = |name: &str| -> u64 {
        text.lines()
            .find_map(|line| line.strip_prefix(name))
            .unwrap()
            .split_whitespace()
            .next()
            .unwrap()
            .parse()
            .unwrap()
    };
    at("SwapTotal:") - at("SwapFree:")
}

/// Swap held by `root` and its descendants; a process that exits mid-sample has released its memory.
fn process_tree_swap_kb(root: u32) -> u64 {
    let mut pending = vec![root];
    let mut total = 0;
    while let Some(pid) = pending.pop() {
        let Ok(status) = fs::read_to_string(format!("/proc/{pid}/status")) else {
            continue;
        };
        total += status
            .lines()
            .find_map(|line| line.strip_prefix("VmSwap:"))
            .and_then(|kb| kb.split_whitespace().next()?.parse::<u64>().ok())
            .unwrap_or(0);
        let Ok(tasks) = fs::read_dir(format!("/proc/{pid}/task")) else {
            continue;
        };
        for task in tasks.flatten() {
            if let Ok(children) = fs::read_to_string(task.path().join("children")) {
                pending.extend(
                    children
                        .split_whitespace()
                        .filter_map(|pid| pid.parse::<u32>().ok()),
                );
            }
        }
    }
    total
}

/// The swap limit of this process's cgroup v2 memory scope, recorded with the gate.
fn scope_swap_max() -> String {
    fs::read_to_string("/proc/self/cgroup")
        .ok()
        .and_then(|groups| {
            let path = groups
                .lines()
                .find_map(|line| line.strip_prefix("0::"))?
                .to_owned();
            fs::read_to_string(format!("/sys/fs/cgroup{path}/memory.swap.max")).ok()
        })
        .map_or_else(|| "unavailable".to_owned(), |value| value.trim().to_owned())
}

fn gpu_memory() -> (u64, u64) {
    let output = Command::new("nvidia-smi")
        .args([
            "--query-gpu=memory.used,memory.free",
            "--format=csv,noheader,nounits",
        ])
        .output()
        .unwrap();
    assert!(output.status.success());
    let line = String::from_utf8(output.stdout).unwrap();
    let (used, free) = line.trim().split_once(',').unwrap();
    (used.trim().parse().unwrap(), free.trim().parse().unwrap())
}

fn counter(line: &str, name: &str) -> u64 {
    line.split_whitespace()
        .collect::<Vec<_>>()
        .windows(2)
        .find(|pair| pair[0] == name)
        .unwrap_or_else(|| panic!("missing {name} in {line}"))[1]
        .parse()
        .unwrap()
}

fn object(root: &Path, generation: &str, name: &str) -> Vec<u8> {
    let manifest: Value =
        serde_json::from_slice(&fs::read(root.join(manifest_key(generation))).unwrap()).unwrap();
    let record = manifest["objects"]
        .as_array()
        .unwrap()
        .iter()
        .find(|entry| entry["path"] == name)
        .unwrap();
    fs::read(root.join(record["key"].as_str().unwrap())).unwrap()
}

fn manifest_uri(root: &Path, generation: &str) -> String {
    format!("file://{}", root.join(manifest_key(generation)).display())
}

fn check_role(
    root: &Path,
    resources: &toml::Value,
    role: &str,
    features: &[String],
    replays: usize,
) {
    let minimum = number(resources, &["roles", role, "minimum_rows"]);
    let days = number(resources, &["roles", role, "days"]);
    assert_eq!(
        minimum,
        days * 86_400 / number(resources, &["gate", "base_bar_seconds"]),
        "pinned {role} five-second grid"
    );
    assert_eq!(
        features.len() as u64,
        number(resources, &["roles", role, "complete_feature_builds"]),
        "{role} feature-build count"
    );
    assert_eq!(
        replays as u64,
        number(resources, &["roles", role, "complete_replays"]),
        "{role} replay count"
    );
    for generation in features {
        let manifest =
            FeatureManifest::from_json(&fs::read(root.join(manifest_key(generation))).unwrap())
                .unwrap();
        assert_eq!(manifest.streams.len(), 7, "{role} feature streams");
        assert!(
            manifest.streams[0].rows >= minimum,
            "{role} rows {} below pinned {minimum}",
            manifest.streams[0].rows
        );
        println!(
            "role {role} feature_generation {generation} observations {} stream_rows {:?}",
            manifest.observations,
            manifest
                .streams
                .iter()
                .map(|stream| stream.rows)
                .collect::<Vec<_>>()
        );
    }
    println!(
        "role {role} minimum_rows {minimum} feature_builds {} replays {replays} PASS",
        features.len()
    );
}

fn run_command(log: &Path, args: &[&str], name: &str) -> String {
    let started = Instant::now();
    let report = cli(log, args).unwrap_or_else(|error| {
        panic!(
            "{name} failed after {:.3}s: {error}",
            started.elapsed().as_secs_f64()
        )
    });
    stage(
        name,
        started,
        if name == "validate_config" {
            report.lines().next().unwrap()
        } else {
            &report
        },
    );
    report
}

#[cfg(feature = "cuda")]
#[test]
#[ignore]
fn quantum_study_p_combined_scale() {
    assert_eq!(
        Sha256::digest(RESOURCES)
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>(),
        RESOURCES_SHA256
    );
    let resources: toml::Value = toml::from_str(std::str::from_utf8(RESOURCES).unwrap()).unwrap();
    let started = Instant::now();
    let swap_before = swap_used_kb();
    let (vram_before, free_mib) = gpu_memory();
    let stability_bytes = number(&resources, &["stability", "development_bytes"]);
    println!(
        "resource start gpu_used_mib {vram_before} gpu_free_mib {free_mib} swap_used_kb {swap_before} scope_swap_max {} stability_full_rows_bytes {stability_bytes} concurrent_cuda_reserve_bytes {}",
        scope_swap_max(),
        1024 * 1024 * 1024_u64
    );
    assert!(
        free_mib * 1024 * 1024 >= stability_bytes + 1024 * 1024 * 1024,
        "first device free {free_mib} MiB cannot contain stability {stability_bytes} bytes plus CUDA outputs/concurrent allocation reserve"
    );
    let sampling = Arc::new(AtomicBool::new(true));
    let peak_vram = Arc::new(AtomicU64::new(vram_before));
    let peak_host_swap = Arc::new(AtomicU64::new(swap_before));
    let peak_process_swap = Arc::new(AtomicU64::new(process_tree_swap_kb(std::process::id())));
    let sample_flag = Arc::clone(&sampling);
    let sample_peak = Arc::clone(&peak_vram);
    let sample_host_swap = Arc::clone(&peak_host_swap);
    let sample_process_swap = Arc::clone(&peak_process_swap);
    let sampler = std::thread::spawn(move || {
        while sample_flag.load(Ordering::Relaxed) {
            let swapped = process_tree_swap_kb(std::process::id());
            if swapped > sample_process_swap.fetch_max(swapped, Ordering::Relaxed) {
                println!(
                    "process swap peak {swapped} KiB at {:.0}s",
                    started.elapsed().as_secs_f64()
                );
            }
            if let Ok(output) = Command::new("nvidia-smi")
                .args(["--query-gpu=memory.used", "--format=csv,noheader,nounits"])
                .output()
                && let Ok(value) = String::from_utf8(output.stdout)
                && let Ok(mib) = value.trim().parse::<u64>()
            {
                sample_peak.fetch_max(mib, Ordering::Relaxed);
            }
            sample_host_swap.fetch_max(swap_used_kb(), Ordering::Relaxed);
            std::thread::sleep(Duration::from_secs(1));
        }
    });
    let scratch = Scratch::new("phase11_quantum_study_p_s7b");
    let published = scratch.path("split-published");
    let log = scratch.path("scale-access.log");
    let source_start = Instant::now();
    let mut bars = Vec::with_capacity(DAYS as usize * BARS_PER_DAY);
    let planted = fixture::recipe([0b0011_1111, 0b0000_0011, 0b0001_1111, 0b0000_1111]);
    let rows: Vec<Row> = (0..BARS_PER_DAY / 4)
        .map(|index| planted[index % planted.len()])
        .collect();
    for day in 0..DAYS {
        let mut daily = bar_rows(BASE + day * DAY_MICROS, &rows, 0);
        daily.truncate(BARS_PER_DAY);
        for (index, bar) in daily.iter_mut().enumerate() {
            let shift = ((day as usize * BARS_PER_DAY + index) % 10_003) as i64 - 5_000;
            let open = (bar.ohlcv[0] * 1_000_000.0).round() as i64 + shift;
            let close =
                (bar.ohlcv[3] * 1_000_000.0).round() as i64 + shift + (index % 31) as i64 - 15;
            let high = open.max(close) + 5 + ((index * 37 + day as usize) % 200) as i64;
            let low = open.min(close) - 5 - ((index * 43 + day as usize) % 150) as i64;
            bar.ohlcv = [
                open as f64 / 1_000_000.0,
                high as f64 / 1_000_000.0,
                low as f64 / 1_000_000.0,
                close as f64 / 1_000_000.0,
                (index % 97 + 1) as f64,
            ];
        }
        bars.extend(daily);
        if day % 50 == 0 {
            println!(
                "synthetic bars days {} rows {} elapsed {:.3}s",
                day + 1,
                bars.len(),
                source_start.elapsed().as_secs_f64()
            );
        }
    }
    assert_eq!(bars.len(), DAYS as usize * BARS_PER_DAY);
    common::write_collection(
        &scratch.path("sources/study-p"),
        &[common::AssetSpec {
            asset: fixture::SYMBOLS[0],
            expected_symbol_id: Some(7),
            symbol_id: Some(7),
            files: vec![bars],
            metadata: true,
        }],
    );
    stage(
        "synthetic_source",
        source_start,
        "complete five-second grid",
    );
    let import_path = scratch.config(
        "scale-import.toml",
        &scratch
            .bar_source()
            .replace("sources/bars", "sources/study-p")
            .replace("role = \"evaluation\"", "role = \"development\""),
    );
    let imported = run_command(
        &log,
        &["data", "import", "--config", import_path.to_str().unwrap()],
        "import",
    );
    let root_generation = common::generation(imported.lines().next().unwrap());
    let source_root = scratch.path("published");
    let root_manifest = manifest_uri(&source_root, &root_generation);
    run_command(
        &log,
        &["data", "verify", "--manifest", &root_manifest],
        "verify_import",
    );

    let mut split = binary_alpha_app::skeleton(&fixture::configuration(&scratch.root));
    split.storage.historical_data_dir =
        serde_json::from_value(serde_json::json!(scratch.path("split-retained"))).unwrap();
    split.storage.publication_uri = format!("file://{}", published.display()).parse().unwrap();
    let day = |first, end| CoverageRange::new(BASE + first * DAY_MICROS, BASE + end * DAY_MICROS);
    split.split = Some(DataSplit {
        namespace: "synthetic-study-p-scale".into(),
        sources: vec![root_manifest.parse().unwrap()],
        development: vec![day(0, 257), day(0, 128), day(129, 193)],
        evaluation: vec![day(258, 443)],
        holdout: vec![day(444, 493)],
    });
    let split_path = scratch.path("scale-split.toml");
    fs::write(&split_path, split.canonical_toml()).unwrap();
    let split_started = Instant::now();
    let split_report = cli_as(
        &log,
        OPERATOR,
        &["data", "split", "--config", split_path.to_str().unwrap()],
    )
    .unwrap();
    stage("split", split_started, &split_report);
    let declaration_uri = split_report
        .lines()
        .last()
        .unwrap()
        .strip_prefix("declaration ")
        .unwrap();
    let declaration = Declaration::from_json(
        &fs::read(declaration_uri.strip_prefix("file://").unwrap()).unwrap(),
    )
    .unwrap();
    assert_eq!(declaration.populations.len(), 5);
    for population in declaration
        .populations
        .iter()
        .filter(|population| population.role != binary_alpha_engine::dataset::DatasetRole::Holdout)
    {
        run_command(
            &log,
            &[
                "data",
                "verify",
                "--manifest",
                &manifest_uri(&published, &population.id),
            ],
            "verify_split",
        );
    }

    let mut config = fixture::configuration(&scratch.root);
    config.storage = split.storage;
    config.instruments.truncate(1);
    config.instruments[0].native_granularity =
        binary_alpha_engine::dataset::NativeGranularity::Bar { period_seconds: 5 };
    let streams: Vec<_> = numbers(&resources, &["gate", "streams_seconds"])
        .into_iter()
        .map(|duration_seconds| StreamKey {
            duration_seconds: duration_seconds.try_into().unwrap(),
            offset_seconds: 0,
        })
        .collect();
    let generated_streams = numbers(&resources, &["gate", "generated_streams_seconds"]);
    let expiries: Vec<u32> = numbers(&resources, &["gate", "expiries_seconds"])
        .into_iter()
        .map(|seconds| seconds.try_into().unwrap())
        .collect();
    let rolling_windows: Vec<u32> = numbers(&resources, &["search", "rolling_windows"])
        .into_iter()
        .map(|window| window.try_into().unwrap())
        .collect();
    let moving_average_periods: Vec<u32> =
        numbers(&resources, &["search", "moving_average_periods"])
            .into_iter()
            .map(|period| period.try_into().unwrap())
            .collect();
    config.instruments[0].candles = streams
        .iter()
        .map(|stream| binary_alpha_engine::config::CandleSpec {
            duration_seconds: stream.duration_seconds,
            offset_seconds: 0,
            min_observations: Some(1),
            hard_min_observations: Some(1),
        })
        .collect();
    let research = config.research.as_mut().unwrap();
    research.study.governance_manifest = declaration_uri.parse().unwrap();
    research.instruments.truncate(1);
    research.folds[0].inputs.truncate(1);
    research.refit.fits.truncate(1);
    research.evaluation.inputs.truncate(1);
    research.holdout.inputs.truncate(1);
    let reference = |index: usize| {
        manifest_uri(&published, &declaration.populations[index].id)
            .parse()
            .unwrap()
    };
    let instrument = &mut research.instruments[0];
    instrument.source_manifest = reference(0);
    instrument.features.streams = Some(streams.clone());
    instrument.features.outputs = Some(Outputs::AllSupported);
    instrument.features.moving_average_periods = Some(moving_average_periods.clone());
    instrument.features.rolling_window = Some(*rolling_windows.iter().max().unwrap());
    instrument.features.min_history = Some(*moving_average_periods.iter().min().unwrap());
    instrument.features.price_epsilon = Some("0".into());
    instrument.features.structure = Some(
        serde_json::from_value(serde_json::json!({
            "swing_left":2,"swing_right":2,"rolling_windows":rolling_windows,"direction_window":8,
            "trend_efficiency_threshold":0.35,"trend_min_abs_momentum_bps":3.0,
            "range_efficiency_threshold":0.25,"compression_ratio_threshold":0.7,
            "expanded_ratio_threshold":1.3,"extreme_ratio_threshold":1.8,
            "pullback_min_trend_age":2,"trend_reset_sideways_bars":2,"failed_breakout_max_bars":2
        }))
        .unwrap(),
    );
    instrument.features.encodings = Some(Encodings {
        max_labels: number(&resources, &["search", "max_labels"])
            .try_into()
            .unwrap(),
        outputs: vec![EncodingSpec {
            output: "all_supported".into(),
            bins: None,
        }],
    });
    instrument.outcomes.expiry_seconds = expiries.clone();
    instrument.outcomes.max_entry_delay_ms = 5_000;
    instrument.outcomes.max_settlement_delay_ms = 5_000;
    instrument.outcomes.max_tick_gap_ms = 5_000;
    instrument.outcomes.true_jump_max_gap_ms = 5_000;
    let search = &mut instrument.search;
    search.decision_start = time(BASE);
    search.decision_end = time(BASE + 257 * DAY_MICROS + 1_000_000);
    search.base_stream = streams[0];
    search.scope = Scope::Heuristic;
    search.min_conditions = number(&resources, &["search", "max_conditions"])
        .try_into()
        .unwrap();
    search.max_conditions = search.min_conditions;
    search.max_candidates = number(&resources, &["search", "max_candidates"]);
    search.chunk_size = number(&resources, &["search", "chunk_size"])
        .try_into()
        .unwrap();
    search.screen = Some(Screen {
        max_adjusted_score: 1.0,
        top: Some(
            number(&resources, &["search", "screen_top"])
                .try_into()
                .unwrap(),
        ),
    });
    search.stability.simulations = number(&resources, &["search", "stability_simulations"])
        .try_into()
        .unwrap();
    search.embargo_micros = 305_000_000;
    search.gates.min_settled = 0;
    search.gates.min_net_profit = Decimal::parse("-1000000000").unwrap();
    search.gates.max_unresolved = 1_000_000;
    search.risk_policy.max_feature_age_micros = 7_200_000_000;
    search.conditions = streams
        .iter()
        .filter(|stream| generated_streams.contains(&u64::from(stream.duration_seconds)))
        .map(|&stream| {
            SearchCondition::Generate(GeneratedSearchCondition {
                stream,
                output: resources["search"]["generated_output"]
                    .as_str()
                    .unwrap()
                    .into(),
                comparator: Comparator::Eq,
            })
        })
        .collect();
    let template = search.contracts[0].clone();
    search.contracts = expiries
        .into_iter()
        .flat_map(|expiry| {
            [Direction::Buy, Direction::Sell]
                .into_iter()
                .map(move |direction| (expiry, direction))
        })
        .enumerate()
        .map(|(index, (expiry, direction))| {
            let mut contract = template.clone();
            contract.id = format!("synthetic-{index}");
            contract.direction = direction;
            contract.duration_micros = i64::from(expiry) * 1_000_000;
            contract.settlement.max_settlement_delay_micros = 5_000_000;
            contract.settlement.max_tick_gap_micros = 5_000_000;
            contract
        })
        .collect();
    research.folds[0].cutoff = time(BASE + 128 * DAY_MICROS + 1_000_000);
    research.folds[0].decision_start = time(BASE + 129 * DAY_MICROS);
    research.folds[0].decision_end = time(BASE + 193 * DAY_MICROS + 1_000_000);
    research.folds[0].inputs[0].fit_manifest = reference(1);
    research.folds[0].inputs[0].assessment_manifest = reference(2);
    research.refit.cutoff = time(BASE + 257 * DAY_MICROS + 1_000_000);
    research.refit.fits[0] = reference(0);
    research.evaluation.decision_start = time(BASE + 258 * DAY_MICROS);
    research.evaluation.decision_end = time(BASE + 443 * DAY_MICROS + 1_000_000);
    research.evaluation.inputs[0] = reference(3);
    research.evaluation.splits = None;
    research.holdout.decision_start = time(BASE + 444 * DAY_MICROS);
    research.holdout.decision_end = time(BASE + 493 * DAY_MICROS + 1_000_000);
    research.holdout.inputs[0] = reference(4);
    research.holdout.splits = None;
    research.portfolio.accounts.truncate(1);
    research.portfolio.risk_policies.truncate(1);
    research.portfolio.risk_policies[0].max_feature_age_micros = 7_200_000_000;
    research.portfolio.repairs.truncate(1);
    research.portfolio.members.clear();
    research.portfolio.subsets.clear();
    research.portfolio.generate = Some(PortfolioGenerate {
        top: number(&resources, &["search", "portfolio_top"])
            .try_into()
            .unwrap(),
        nested: false,
    });
    research.portfolio.max_policies = 128;
    research.portfolio.embargo_micros = 305_000_000;
    research.portfolio.max_rate_age_micros = 493 * DAY_MICROS;
    research.portfolio.gates.min_profit = Decimal::parse("-1000000000").unwrap();
    research.portfolio.gates.max_drawdown = Decimal::parse("1000000000").unwrap();
    research.portfolio.gates.max_unresolved = 1_000_000;
    research.portfolio.gates.min_decisive = Some(1);
    research.portfolio.gates.min_win_rate = Some(Decimal::parse("0").unwrap());
    research.qualification.gates = research.portfolio.gates.clone();
    let binding = research.portfolio.bindings[0].clone();
    research.portfolio.bindings = search
        .contracts
        .iter()
        .enumerate()
        .map(|(index, contract)| {
            let mut item = binding.clone();
            item.id = format!("scale-binding-{index}");
            item.alternatives.truncate(1);
            item.alternatives[0].contract = contract.clone();
            item
        })
        .collect();
    for scenario in &mut research.scenarios {
        let alternative = scenario.alternatives[0].clone();
        scenario.alternatives = research
            .portfolio
            .bindings
            .iter()
            .map(|binding| {
                let mut item = alternative.clone();
                item.binding = binding.id.clone();
                let base = &binding.alternatives[0].contract;
                item.contract.id = base.id.clone();
                item.contract.direction = base.direction;
                item.contract.duration_micros = base.duration_micros;
                item.contract.currency = base.currency.clone();
                item.contract.settlement = base.settlement;
                item
            })
            .collect();
    }
    config.accelerator =
        Some(serde_json::from_value(serde_json::json!({"backend":"cuda","devices":[0]})).unwrap());
    let research = config.research.as_ref().unwrap();
    assert_eq!(
        research.scenarios.len() as u64 + 1,
        number(&resources, &["minimum", "scenarios"])
    );
    for role in [
        "development",
        "fold_fit",
        "fold_assessment",
        "refit",
        "evaluation",
        "certification",
    ] {
        assert_eq!(
            number(&resources, &["stability", &format!("{role}_bytes")]),
            number(&resources, &["search", "stability_simulations"])
                * number(&resources, &["roles", role, "minimum_rows"])
                * 8
        );
    }
    let path = scratch.path("study-p-scale.toml");
    fs::write(&path, config.canonical_toml()).unwrap();
    let setup = run_command(
        &log,
        &["config", "validate", "--config", path.to_str().unwrap()],
        "validate_config",
    );
    assert!(setup.contains("content-hash"));
    let audit = run_command(
        &log,
        &[
            "data",
            "audit",
            "--config",
            path.to_str().unwrap(),
            "--manifest",
            &manifest_uri(&published, &declaration.populations[0].id),
        ],
        "audit_development",
    );
    let profile_generation = common::generation(audit.lines().next().unwrap());
    let profile = manifest_uri(&published, &profile_generation);
    let mut standalone = binary_alpha_app::skeleton(&config);
    standalone.instruments = config.instruments.clone();
    let mut feature = serde_json::to_value(&research.instruments[0].features).unwrap();
    feature["role"] = "development".into();
    feature["input_manifest"] = reference(0).to_string().into();
    feature["profile_manifest"] = profile.into();
    standalone.features =
        Some(serde_json::from_value(serde_json::json!({"instruments":[feature]})).unwrap());
    let standalone_path = scratch.path("standalone.toml");
    fs::write(&standalone_path, standalone.canonical_toml()).unwrap();
    let built = run_command(
        &log,
        &[
            "features",
            "build",
            "--config",
            standalone_path.to_str().unwrap(),
        ],
        "features_development",
    );
    let feature_generation = common::generation(built.lines().next().unwrap());
    let feature_manifest = manifest_uri(&published, &feature_generation);
    let summary = FeatureManifest::from_json(
        &fs::read(published.join(manifest_key(&feature_generation))).unwrap(),
    )
    .unwrap();
    let rows = summary.streams[0].rows;
    assert!(
        rows >= number(&resources, &["roles", "development", "minimum_rows"]),
        "development rows {rows}"
    );
    let plan =
        FeaturePlan::from_json(&object(&published, &feature_generation, "plan.json")).unwrap();
    assert_eq!(plan.streams.len(), 7);
    standalone.features = None;
    let mut outcomes = serde_json::to_value(&research.instruments[0].outcomes).unwrap();
    outcomes["role"] = "development".into();
    outcomes["tick_manifest"] = reference(0).to_string().into();
    outcomes["feature_manifest"] = feature_manifest.clone().into();
    standalone.outcomes = Some(serde_json::from_value(outcomes).unwrap());
    fs::write(&standalone_path, standalone.canonical_toml()).unwrap();
    let built = run_command(
        &log,
        &[
            "outcomes",
            "build",
            "--config",
            standalone_path.to_str().unwrap(),
        ],
        "outcomes_development",
    );
    let outcome_generation = common::generation(built.lines().next().unwrap());
    standalone.outcomes = None;
    let mut search = serde_json::to_value(&research.instruments[0].search).unwrap();
    let start = search
        .as_object_mut()
        .unwrap()
        .remove("decision_start")
        .unwrap();
    let end = search
        .as_object_mut()
        .unwrap()
        .remove("decision_end")
        .unwrap();
    search["development"] = serde_json::json!({"decision_start":start,"decision_end":end,
        "inputs":[{"tick_manifest":reference(0),"feature_manifest":feature_manifest,
        "outcome_manifest":manifest_uri(&published, &outcome_generation)}]});
    standalone.search = Some(serde_json::from_value(search).unwrap());
    standalone.accelerator = config.accelerator.clone();
    let resolved =
        binary_alpha_engine::search::resolve_conditions(standalone.search.as_ref().unwrap(), &plan)
            .unwrap();
    println!(
        "pre-search resolved_conditions {} members {}",
        resolved.conditions.len(),
        resolved.members
    );
    assert!(
        resolved.conditions.len() as u64 >= number(&resources, &["minimum", "resolved_conditions"])
    );
    assert!(resolved.members >= number(&resources, &["minimum", "members"]));
    fs::write(&standalone_path, standalone.canonical_toml()).unwrap();
    let cuda_digest = scratch.path("combined-cuda-screen.json");
    let search_started = Instant::now();
    let output = Command::new(env!("CARGO_BIN_EXE_binary-alpha"))
        .args(["search", "--config", standalone_path.to_str().unwrap()])
        .env("BINARY_ALPHA_TEST_SCREEN_DIGEST", &cuda_digest)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "search failed after {:.3}s: {}",
        search_started.elapsed().as_secs_f64(),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        output.stderr.is_empty(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let report = String::from_utf8(output.stdout).unwrap();
    stage("generated_search_cuda", search_started, &report);
    let first = report.lines().next().unwrap();
    let generation = common::generation(first);
    let family_manifest =
        FamilyManifest::from_json(&fs::read(published.join(manifest_key(&generation))).unwrap())
            .unwrap();
    let family = Family::from_json(&object(&published, &generation, "family.json")).unwrap();
    assert!(family_manifest.members >= number(&resources, &["minimum", "members"]));
    assert!(
        family.resolved_conditions.as_ref().unwrap().len() as u64
            >= number(&resources, &["minimum", "resolved_conditions"])
    );
    assert!(family.members.len() as u64 >= number(&resources, &["minimum", "screened_survivors"]));
    for (name, minimum) in [
        ("columns", "projected_columns"),
        ("tuples", "block_tuples"),
        ("list_entries", "sparse_list_entries"),
        ("construction_visits", "construction_visits"),
        ("validation_visits", "validation_visits"),
        ("driver_visits", "candidate_driver_row_visits"),
        ("transfer_bytes", "transfer_bytes"),
    ] {
        assert!(
            counter(first, name) >= number(&resources, &["minimum", minimum]),
            "{name}={} below pinned {}",
            counter(first, name),
            number(&resources, &["minimum", minimum])
        );
    }
    let verify_started = Instant::now();
    let verify_output = Command::new(env!("CARGO_BIN_EXE_binary-alpha"))
        .args([
            "data",
            "verify",
            "--manifest",
            &manifest_uri(&published, &generation),
            "--config",
            standalone_path.to_str().unwrap(),
        ])
        .output()
        .unwrap();
    assert!(
        verify_output.status.success(),
        "CUDA verify failed after {:.3}s: {}",
        verify_started.elapsed().as_secs_f64(),
        String::from_utf8_lossy(&verify_output.stderr)
    );
    assert!(
        verify_output.stderr.is_empty(),
        "{}",
        String::from_utf8_lossy(&verify_output.stderr)
    );
    let verify = String::from_utf8(verify_output.stdout).unwrap();
    stage("independent_family_verify_cuda", verify_started, &verify);
    assert!(verify.contains("verified search generation"));
    println!(
        "combined CUDA screen digest {}",
        fs::read_to_string(&cuda_digest).unwrap()
    );
    let run_started = Instant::now();
    let run_report = cli(
        &log,
        &["research", "run", "--config", path.to_str().unwrap()],
    )
    .unwrap();
    stage("generated_portfolio_folds_outer", run_started, &run_report);
    let development_line = run_report
        .lines()
        .find(|line| line.starts_with("features ") && line.contains(" development generation "))
        .expect("research development feature report");
    assert_eq!(common::generation(development_line), feature_generation);
    assert!(
        development_line.ends_with(" (already published)"),
        "research development did not reuse standalone features: {development_line}"
    );
    let run_generation = research::run_generation_id(
        &config.content_hash(),
        binary_alpha_app::import::CODE_REVISION,
        &declaration.identity(),
    );
    let run = Run::from_json(&object(&published, &run_generation, "research.json")).unwrap();
    assert_eq!(run.state, RunState::AwaitingHoldoutAuthorization);
    assert!(run.outer.len() as u64 >= number(&resources, &["minimum", "scenarios"]));
    let selection =
        Selection::from_json(&object(&published, &run.selection, "selection.json")).unwrap();
    assert!(selection.folds.len() as u64 >= number(&resources, &["minimum", "folds"]));
    assert_eq!(run.instruments[0].feature, feature_generation);
    assert_eq!(selection.refit.len(), 1);
    assert_eq!(selection.refit[0].generation, feature_generation);
    let chosen = &selection.choices[selection.selected.unwrap()];
    check_role(
        &published,
        &resources,
        "development",
        &[run.instruments[0].feature.clone()],
        family
            .chunks
            .iter()
            .filter(|chunk| chunk.role == "development")
            .count(),
    );
    for (index, fold) in selection.folds.iter().enumerate() {
        assert_eq!(fold.fits.len(), 1);
        assert_eq!(fold.assessments.len(), 1);
        println!("fold {index} fit/assessment");
        check_role(
            &published,
            &resources,
            "fold_fit",
            &[fold.fits[0].generation.clone()],
            0,
        );
        check_role(
            &published,
            &resources,
            "fold_assessment",
            &[fold.assessments[0].generation.clone()],
            usize::from(chosen.folds[index].replay.is_some()),
        );
    }
    check_role(
        &published,
        &resources,
        "refit",
        &selection
            .refit
            .iter()
            .map(|entry| entry.generation.clone())
            .collect::<Vec<_>>(),
        0,
    );
    let evaluation_features: BTreeSet<_> = run
        .outer
        .iter()
        .flat_map(|result| {
            result
                .outer
                .features
                .iter()
                .map(|feature| feature.generation.clone())
        })
        .collect();
    check_role(
        &published,
        &resources,
        "evaluation",
        &evaluation_features.into_iter().collect::<Vec<_>>(),
        run.outer.len(),
    );
    let grant_started = Instant::now();
    let grant_report = cli_as(
        &log,
        OPERATOR,
        &[
            "holdout",
            "grant",
            "create",
            "--config",
            path.to_str().unwrap(),
            "--bundle-manifest",
            &manifest_uri(&published, &run_generation),
            "--holdout-manifest",
            &reference(4).to_string(),
            "--reason",
            "synthetic quantum release gate",
        ],
    )
    .unwrap();
    stage("synthetic_grant", grant_started, &grant_report);
    let grant_key = declaration.key(&research::grant_key(&run_generation));
    let grant_path = Path::new(
        declaration
            .root
            .to_string()
            .strip_prefix("file://")
            .unwrap(),
    )
    .join(grant_key);
    let grant = Grant::from_json(&fs::read(grant_path).unwrap()).unwrap();
    let certify_started = Instant::now();
    let certified = cli(
        &log,
        &["research", "run", "--config", path.to_str().unwrap()],
    )
    .unwrap();
    stage("synthetic_certification", certify_started, &certified);
    let certification_generation =
        research::certification_generation_id(&run_generation, &grant.hash);
    assert!(
        published
            .join(manifest_key(&certification_generation))
            .is_file()
    );
    let certificate = research::CertificationRecord::from_json(&object(
        &published,
        &certification_generation,
        "certification.json",
    ))
    .unwrap();
    assert_eq!(
        certificate.scenarios.len() as u64,
        number(&resources, &["minimum", "scenarios"])
    );
    let certification_features: BTreeSet<_> = certificate
        .scenarios
        .iter()
        .flat_map(|result| {
            result
                .outer
                .features
                .iter()
                .map(|feature| feature.generation.clone())
        })
        .collect();
    check_role(
        &published,
        &resources,
        "certification",
        &certification_features.into_iter().collect::<Vec<_>>(),
        certificate.scenarios.len(),
    );
    let elapsed = started.elapsed().as_secs_f64();
    assert!(
        elapsed <= number(&resources, &["gate", "max_elapsed_seconds"]) as f64,
        "combined gate took {elapsed:.3}s"
    );
    let cpu_digest = scratch.path("combined-cpu-screen.json");
    let cpu_started = Instant::now();
    let cpu_output = Command::new(env!("CARGO_BIN_EXE_binary-alpha"))
        .args([
            "data",
            "verify",
            "--manifest",
            &manifest_uri(&published, &generation),
        ])
        .env("BINARY_ALPHA_TEST_SCREEN_DIGEST", &cpu_digest)
        .output()
        .unwrap();
    assert!(
        cpu_output.status.success(),
        "CPU verify failed after {:.3}s: {}",
        cpu_started.elapsed().as_secs_f64(),
        String::from_utf8_lossy(&cpu_output.stderr)
    );
    assert!(
        cpu_output.stderr.is_empty(),
        "{}",
        String::from_utf8_lossy(&cpu_output.stderr)
    );
    let cpu_report = String::from_utf8(cpu_output.stdout).unwrap();
    stage("independent_family_verify_cpu", cpu_started, &cpu_report);
    assert!(cpu_report.contains("verified search generation"));
    assert_eq!(
        fs::read(&cpu_digest).unwrap(),
        fs::read(&cuda_digest).unwrap()
    );
    sampling.store(false, Ordering::Relaxed);
    sampler.join().unwrap();
    let host_swap_growth = peak_host_swap
        .load(Ordering::Relaxed)
        .saturating_sub(swap_before);
    let process_swap = peak_process_swap.load(Ordering::Relaxed);
    println!(
        "combined total {:.3}s rows {rows} conditions {} members {} survivors {} peak_rss_kb {} peak_vram_mib {} peak_process_swap_kb {} host_swap_growth_kb {}",
        elapsed,
        family.resolved_conditions.as_ref().unwrap().len(),
        family_manifest.members,
        family.members.len(),
        counter(first, "peak_rss_kb"),
        peak_vram.load(Ordering::Relaxed),
        process_swap,
        host_swap_growth
    );
    assert!(
        process_swap <= number(&resources, &["gate", "max_process_swap_kb"]),
        "gate processes used {process_swap} KiB of swap"
    );
}

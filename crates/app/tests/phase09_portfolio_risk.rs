//! Phase 09: portfolio selection through the real command line on known-truth tick fixtures:
//! the exact counting grid with its joint winner, the restored-ledger valuation of two
//! currencies, interval ordinals under separately fitted folds, refusal of later-role and
//! ill-formed inputs before any output, and the distinct terminal states.

mod common;

use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::Duration;

use binary_alpha_engine::config::StreamKey;
use binary_alpha_engine::dataset::{
    DatasetRole, GenerationManifest, PriceRepresentation, generation_id,
};
use binary_alpha_engine::execution::{Decimal, EventKind, FinancialEvent, Summary, Threshold};
use binary_alpha_engine::features::{FeatureManifest, FeaturePlan, PLAN_OBJECT_PATH};
use binary_alpha_engine::market::{InstrumentId, format_event_time_micros};
use binary_alpha_engine::portfolio::{
    Selection, SelectionManifest, State, selection_generation_id,
};
use binary_alpha_engine::search::Family;
use common::{Scratch, command, generation, import, verify, write_ticks};
use serde_json::Value;

const BASE_MS: i64 = 1_767_571_200_000; // 2026-01-05T00:00:00Z, a Monday
const HOUR_MS: i64 = 3_600_000;
const CANDLE_MS: i64 = 20_000;
const BASIS: i64 = 1_800_000;
const ROWS: usize = 32;
const INSTRUMENT: &str = "pocket_option:AEDCNY_otc";
const STREAM: &str = "{ duration_seconds = 20, offset_seconds = 0 }";
const TICK_INSTRUMENT: &str = "\n[[instruments]]\nbroker = \"pocket_option\"\nprovider_symbol = \"AEDCNY_otc\"\nbase_currency = \"AED\"\nquote_currency = \"CNY\"\nprice_scale = 6\nnative_granularity = { kind = \"tick\" }\ngap = { max_seconds = 2, reopen_seconds = 60 }\nfrozen = { min_observations = 10, min_seconds = 5 }\njump = { min_basis_points = 5 }\nspan = { min_percent = 75 }\nsessions = [{ name = \"week\", open_seconds = 0, close_seconds = 604800 }]\ncandles = [{ duration_seconds = 20, offset_seconds = 0, min_observations = 9, hard_min_observations = 5 }]\n";
/// The settlement rule of the search family's contract and of every alternative.
const SETTLEMENT: &str = "settlement = { rule = \"price_at_due_v1\", max_settlement_delay_micros = 2000000, max_tick_gap_micros = 2000000 }";
/// Five seconds of contract plus two seconds of permitted settlement delay.
const EMBARGO_MICROS: i64 = 7_000_000;

/// One decision row's truth: the candle's direction, its range, the outcome of a buy taken at
/// its close, and the number of ticks the candle carries.
#[derive(Clone, Copy)]
struct Row {
    up: bool,
    wide: bool,
    win: bool,
    volume: usize,
}

/// Which of the eight rows of each cell win, as bit masks by position: up-narrow, up-wide,
/// down-narrow, down-wide.
#[derive(Clone, Copy)]
struct Cells([u8; 4]);

/// The mask whose first `wins` positions win.
const fn first(wins: u8) -> u8 {
    (1_u16 << wins) as u8 - 1
}

/// Thirty-two rows: eight per cell, interleaved by cell (down-narrow, down-wide, up-narrow,
/// up-wide), each row winning when its cell mask has its position set.
fn recipe(cells: Cells) -> Vec<Row> {
    (0..ROWS)
        .map(|k| {
            let (up, wide) = (k % 4 >= 2, k % 2 == 1);
            let mask = match (up, wide) {
                (true, false) => cells.0[0],
                (true, true) => cells.0[1],
                (false, false) => cells.0[2],
                (false, true) => cells.0[3],
            };
            Row {
                up,
                wide,
                win: mask & (1 << (k / 4)) != 0,
                volume: 80,
            }
        })
        .collect()
}

fn with_volumes(mut rows: Vec<Row>, volumes: &[usize]) -> Vec<Row> {
    for (k, row) in rows.iter_mut().enumerate() {
        row.volume = volumes[k % volumes.len()];
    }
    rows
}

fn tick_line(millis: i64, price_units: i64) -> String {
    let seconds = millis.div_euclid(1_000);
    format!(
        "{}.{:03}Z,AEDCNY,{}.{:06}",
        format_event_time_micros(seconds * 1_000_000)
            .strip_suffix(".000000Z")
            .unwrap(),
        millis.rem_euclid(1_000),
        price_units / 1_000_000,
        price_units % 1_000_000
    )
}

/// The quarter-second steps of one candle of `volume` ticks: the mandatory steps (the open,
/// the preceding decision's outcome at five seconds, the extrema, the closing direction) plus
/// fillers at one-second, then half-second, then quarter-second spacing.
fn candle_steps(volume: usize) -> Vec<i64> {
    let mandatory = [0, 20, 32, 48, 79];
    let mut steps: Vec<i64> = mandatory.to_vec();
    let fillers = (0..80)
        .filter(|step| step % 4 == 0)
        .chain((0..80).filter(|step| step % 4 == 2))
        .chain((0..80).filter(|step| step % 2 == 1))
        .filter(|step| !mandatory.contains(step));
    for step in fillers {
        if steps.len() == volume {
            break;
        }
        steps.push(step);
    }
    assert_eq!(steps.len(), volume, "a candle holds 5 to 80 ticks");
    steps.sort_unstable();
    steps
}

/// The frozen ordered tick recipe: each candle opens at the basis (the preceding decision's
/// entry), encodes that decision's outcome five seconds later, then its own range and direction
/// with later extrema and its final tick. One tail candle settles the last row.
fn ticks(base_ms: i64, rows: &[Row]) -> Vec<String> {
    let mut lines = Vec::new();
    for k in 0..=rows.len() {
        let start = base_ms + k as i64 * CANDLE_MS;
        let previous = k.checked_sub(1).map(|i| rows[i]);
        let current = rows.get(k).copied();
        let volume = current.map_or(80, |row| row.volume);
        for (ordinal, step) in candle_steps(volume).into_iter().enumerate() {
            let at = start + step * 250;
            let price = match (step, current) {
                (0, _) => BASIS,
                (20, _) => {
                    BASIS
                        + if previous.is_none_or(|row| row.win) {
                            2
                        } else {
                            -2
                        }
                }
                (32, Some(row)) => BASIS + if row.wide { 12 } else { 4 },
                (48, Some(row)) => BASIS - if row.wide { 12 } else { 4 },
                (79, Some(row)) => BASIS + if row.up { 1 } else { -1 },
                _ => BASIS + if ordinal % 2 == 0 { 1 } else { -1 },
            };
            lines.push(tick_line(at, price));
        }
    }
    lines
}

fn manifest_uri(path: &Path) -> String {
    format!("file://{}", path.display())
}

fn time(ms: i64) -> String {
    format_event_time_micros(ms * 1_000)
}

/// The decision window of one slice: every candle close of its rows.
fn window(base_ms: i64) -> (String, String) {
    (
        time(base_ms + CANDLE_MS),
        time(base_ms + (ROWS as i64 + 1) * CANDLE_MS),
    )
}

/// A cutoff after the whole slice, including its tail candle.
fn cutoff(base_ms: i64) -> String {
    time(base_ms + (ROWS as i64 + 1) * CANDLE_MS)
}

fn published(scratch: &Scratch, line: &str) -> PathBuf {
    scratch.path(&format!(
        "published/manifests/{}/ready.json",
        generation(line)
    ))
}

fn generation_of(manifest: &Path) -> String {
    manifest
        .parent()
        .unwrap()
        .file_name()
        .unwrap()
        .to_string_lossy()
        .into_owned()
}

/// Imports one tick slice of `role` and returns its ready manifest.
fn import_slice(scratch: &Scratch, name: &str, role: &str, base_ms: i64, rows: &[Row]) -> PathBuf {
    let lines = ticks(base_ms, rows);
    write_ticks(
        &scratch.path(&format!("sources/{name}/ticks.csv")),
        &lines.iter().map(String::as_str).collect::<Vec<_>>(),
    );
    let source = scratch
        .tick_source()
        .replace(
            "sources/ticks/ticks.csv",
            &format!("sources/{name}/ticks.csv"),
        )
        .replace("\"development\"", &format!("\"{role}\""));
    published(
        scratch,
        &import(&scratch.config(&format!("import_{name}.toml"), &source))
            .unwrap()
            .remove(0),
    )
}

/// Audits one imported slice and returns its stream (profile) manifest.
fn audit(scratch: &Scratch, name: &str, tick: &Path) -> PathBuf {
    let config = scratch.config(&format!("audit_{name}.toml"), TICK_INSTRUMENT);
    published(
        scratch,
        &command(&[
            "data",
            "audit",
            "--config",
            config.to_str().unwrap(),
            "--manifest",
            &manifest_uri(tick),
        ])
        .unwrap()
        .remove(0),
    )
}

/// The new-plan settings of every fit: the direction and range outputs, plus the tick-volume
/// output and its development-fifths projection when `volume` is set.
fn fit_settings(volume: bool) -> String {
    if volume {
        format!(
            "streams = [{STREAM}]\noutputs = [\"candle_direction\", \"range_bps\", \"tick_volume\"]\nencodings = {{ max_labels = 32768, outputs = [{{ output = \"tick_volume_dev_quantile\" }}] }}\n"
        )
    } else {
        format!("streams = [{STREAM}]\noutputs = [\"candle_direction\", \"range_bps\"]\n")
    }
}

/// Builds one feature generation and returns its ready manifest.
fn build_features(
    scratch: &Scratch,
    name: &str,
    role: &str,
    tick: &Path,
    profile: &Path,
    settings: &str,
) -> PathBuf {
    let config = scratch.config(
        &format!("features_{name}.toml"),
        &format!(
            "\n[[features.instruments]]\nrole = \"{role}\"\ninput_manifest = \"{}\"\nprofile_manifest = \"{}\"\n{settings}",
            manifest_uri(tick),
            manifest_uri(profile)
        ),
    );
    published(
        scratch,
        &command(&["features", "build", "--config", config.to_str().unwrap()])
            .unwrap()
            .remove(0),
    )
}

fn build_outcomes(scratch: &Scratch, name: &str, tick: &Path, feature: &Path) -> PathBuf {
    let config = scratch.config(
        &format!("outcomes_{name}.toml"),
        &format!(
            "\n[outcomes]\nrole = \"development\"\ntick_manifest = \"{}\"\nfeature_manifest = \"{}\"\nexpiry_seconds = [5]\nmax_entry_delay_ms = 2000\nmax_settlement_delay_ms = 2000\nmax_tick_gap_ms = 2000\ntrue_jump_max_gap_ms = 2000\ntrue_jump_basis_points = \"5\"\nfrozen_min_ticks = 10\nfrozen_min_ms = 5000\n",
            manifest_uri(tick),
            manifest_uri(feature)
        ),
    );
    published(
        scratch,
        &command(&["outcomes", "build", "--config", config.to_str().unwrap()])
            .unwrap()
            .remove(0),
    )
}

/// One development slice with everything a search family needs.
struct Development {
    tick: PathBuf,
    profile: PathBuf,
    feature: PathBuf,
    outcome: PathBuf,
}

fn development(
    scratch: &Scratch,
    name: &str,
    base_ms: i64,
    rows: &[Row],
    volume: bool,
) -> Development {
    let tick = import_slice(scratch, name, "development", base_ms, rows);
    let profile = audit(scratch, name, &tick);
    let feature = build_features(
        scratch,
        name,
        "development",
        &tick,
        &profile,
        &fit_settings(volume),
    );
    let outcome = build_outcomes(scratch, name, &tick, &feature);
    Development {
        tick,
        profile,
        feature,
        outcome,
    }
}

/// One search family over `development` with the given single-condition menu, the buy
/// contract A, and an optional evaluation window; returns its ready manifest and the family.
fn family(
    scratch: &Scratch,
    name: &str,
    development: &Development,
    base_ms: i64,
    menu: &str,
    evaluation: &str,
) -> (PathBuf, Family) {
    let (start, end) = window(base_ms);
    let table = format!(
        "\n[search]\nscope = \"exhaustive\"\nseed = 7\nchunk_size = 8\nmax_candidates = 1000\nmin_conditions = 1\nmax_conditions = 1\nembargo_micros = {EMBARGO_MICROS}\nbase_stream = {STREAM}\n\n[search.development]\ndecision_start = \"{start}\"\ndecision_end = \"{end}\"\ninputs = [{{ tick_manifest = \"{}\", feature_manifest = \"{}\", outcome_manifest = \"{}\" }}]\n{evaluation}{menu}\n[[search.contracts]]\nid = \"A\"\ndirection = \"buy\"\nduration_micros = 5000000\ncurrency = \"unit\"\nstake = \"1\"\nquoted_cost = \"1\"\nentry_fee = \"0\"\nwin = {{ gross_return = \"1.80\", terminal_fee = \"0\" }}\nloss = {{ gross_return = \"0\", terminal_fee = \"0\" }}\ntie = {{ gross_return = \"1\", terminal_fee = \"0\" }}\n{SETTLEMENT}\n\n[search.account]\nbroker = \"pocket_option\"\ncurrency = \"unit\"\nscale = 2\ninitial_cash = \"1000\"\n\n[search.risk_policy]\nid = \"one\"\nmax_open_per_strategy = 1\nsame_entry = \"all\"\ndeduplicate_signal_logic = false\nmax_feature_age_micros = 60000000\nmax_quote_age_micros = 0\n\n[search.envelope]\nmax_purchase_cost = \"1\"\nmax_entry_fee = \"0.10\"\nmax_win_terminal_fee = \"0\"\nmax_loss_terminal_fee = \"0\"\nmax_tie_terminal_fee = \"0\"\nmin_winning_net_return = \"0.70\"\nsettlement_rule = \"price_at_due_v1\"\n\n[search.gates]\nmin_settled = 1\nmax_unresolved = 0\nmin_net_profit = \"0\"\n\n[search.stability]\nblock_length = 4\nsimulations = 16\nrolling_horizon = 4\n",
        manifest_uri(&development.tick),
        manifest_uri(&development.feature),
        manifest_uri(&development.outcome)
    );
    let config = scratch.config(&format!("search_{name}.toml"), &table);
    let lines = command(&["search", "--config", config.to_str().unwrap()]).unwrap();
    let manifest = published(scratch, &lines[0]);
    let manifest_json: Value = serde_json::from_slice(&fs::read(&manifest).unwrap()).unwrap();
    let family = Family::from_json(
        &fs::read(
            scratch
                .path("published")
                .join(manifest_json["objects"][0]["key"].as_str().unwrap()),
        )
        .unwrap(),
    )
    .unwrap();
    (manifest, family)
}

const DIRECTION_MENU: &str = "\n[[search.conditions]]\nstream = { duration_seconds = 20, offset_seconds = 0 }\noutput = \"candle_direction\"\ncomparator = \"eq\"\nthresholds = [\"up\", \"down\"]\n";

/// The family member whose one condition compares with `label`.
fn member(family: &Family, label: &str) -> usize {
    family
        .members
        .iter()
        .position(|member| {
            matches!(&member.conditions[0].threshold, Threshold::Text(text) if text == label)
        })
        .unwrap()
}

/// The exact contract and envelope alternative `id`.
#[allow(clippy::too_many_arguments)]
fn alternative(
    id: &str,
    currency: &str,
    cost: &str,
    fee: &str,
    win: &str,
    tie: &str,
    net: &str,
    envelope_fee: &str,
) -> String {
    format!(
        "[[portfolio.bindings.alternatives]]\ncontract = {{ id = \"{id}\", direction = \"buy\", duration_micros = 5000000, currency = \"{currency}\", stake = \"{cost}\", quoted_cost = \"{cost}\", entry_fee = \"{fee}\", win = {{ gross_return = \"{win}\", terminal_fee = \"0\" }}, loss = {{ gross_return = \"0\", terminal_fee = \"0\" }}, tie = {{ gross_return = \"{tie}\", terminal_fee = \"0\" }}, {SETTLEMENT} }}\nenvelope = {{ max_purchase_cost = \"{cost}\", max_entry_fee = \"{envelope_fee}\", max_win_terminal_fee = \"0\", max_loss_terminal_fee = \"0\", max_tie_terminal_fee = \"0\", min_winning_net_return = \"{net}\", settlement_rule = \"price_at_due_v1\" }}\n"
    )
}

/// Terms A: cost 1, no fee, win 1.80, tie 1. Terms B: cost 2, fee 0.05, win 3.60, tie 2.05.
fn terms_a(id: &str, currency: &str) -> String {
    alternative(id, currency, "1", "0", "1.80", "1", "0.80", "0")
}

fn terms_b(id: &str, currency: &str) -> String {
    alternative(id, currency, "2", "0.05", "3.60", "2.05", "1.55", "0.05")
}

fn risk_policy(id: &str, max_open_total: u32) -> String {
    format!(
        "{{ id = \"{id}\", max_open_total = {max_open_total}, same_entry = \"all\", deduplicate_signal_logic = false, max_feature_age_micros = 60000000, max_quote_age_micros = 0 }}"
    )
}

/// One fit input: the imported slice, its profile, and whether the tick-volume projection is
/// fitted.
#[derive(Clone)]
struct Fit {
    tick: PathBuf,
    profile: PathBuf,
    volume: bool,
}

fn fit(scratch: &Scratch, name: &str, base_ms: i64, rows: &[Row], volume: bool) -> Fit {
    let tick = import_slice(scratch, name, "development", base_ms, rows);
    let profile = audit(scratch, name, &tick);
    Fit {
        tick,
        profile,
        volume,
    }
}

fn fit_toml(fit: &Fit) -> String {
    format!(
        "role = \"development\"\ninput_manifest = \"{}\"\nprofile_manifest = \"{}\"\n{}",
        manifest_uri(&fit.tick),
        manifest_uri(&fit.profile),
        fit_settings(fit.volume)
    )
}

/// One inner fold: the fit slice's cutoff and the assessment slice's window.
fn fold_toml(fit_base_ms: i64, fit: &Fit, assessment_base_ms: i64, assessment: &Path) -> String {
    let (start, end) = window(assessment_base_ms);
    format!(
        "\n[[portfolio.folds]]\ncutoff = \"{}\"\ndecision_start = \"{start}\"\ndecision_end = \"{end}\"\n[[portfolio.folds.inputs]]\nassessment_manifest = \"{}\"\n[portfolio.folds.inputs.fit]\n{}",
        cutoff(fit_base_ms),
        manifest_uri(assessment),
        fit_toml(fit)
    )
}

fn refit_toml(fit_base_ms: i64, fit: &Fit) -> String {
    format!(
        "\n[portfolio.refit]\ncutoff = \"{}\"\n[[portfolio.refit.fits]]\n{}",
        cutoff(fit_base_ms),
        fit_toml(fit)
    )
}

/// The evaluation window over one evaluation slice, split two seconds after the seventeenth
/// close so that the seventeenth decision settles inside the second split.
fn evaluation_toml(base_ms: i64, tick: &Path) -> String {
    let (start, end) = window(base_ms);
    let boundary = time(base_ms + 17 * CANDLE_MS + 2_000);
    format!(
        "\n[portfolio.evaluation]\ndecision_start = \"{start}\"\ndecision_end = \"{end}\"\ninputs = [\"{}\"]\nsplits = [{{ name = \"a\", start = \"{start}\", end = \"{boundary}\" }}, {{ name = \"b\", start = \"{boundary}\", end = \"{end}\" }}]\n",
        manifest_uri(tick)
    )
}

/// The head of a `portfolio` table.
#[allow(clippy::too_many_arguments)]
fn head_toml(
    families: &[&Path],
    objective: &str,
    gates: &str,
    accounts: &str,
    rates: &str,
    members: &str,
    repairs: &str,
    subsets: &str,
    policies: &str,
    max_policies: u64,
) -> String {
    format!(
        "\n[portfolio]\nfamilies = [{}]\nmax_policies = {max_policies}\nembargo_micros = {EMBARGO_MICROS}\nobjective = \"{objective}\"\ngates = {gates}\naccounts = [{accounts}]\nreporting_currency = \"unit\"\nreporting_scale = 2\n{rates}members = [{members}]\nrepairs = [{repairs}]\nsubsets = [{subsets}]\nrisk_policies = [{policies}]\n",
        families
            .iter()
            .map(|path| format!("\"{}\"", manifest_uri(path)))
            .collect::<Vec<_>>()
            .join(", ")
    )
}

const ACCOUNT: &str = "{ id = \"a\", broker = \"pocket_option\", currency = \"unit\", scale = 2, initial_cash = \"10000\" }";
const NARROW: &str = "{ id = \"narrow\", conditions = [{ stream = { duration_seconds = 20, offset_seconds = 0 }, output = \"range_bps\", comparator = \"lt\", threshold = 0.08 }] }";
const GATES: &str =
    "{ min_settled = 1, max_unresolved = 0, min_profit = \"0\", max_drawdown = \"1000\" }";
const NO_RATES: &str = "max_rate_age_micros = 0\n";

fn deployment(member: usize, repair: usize, binding: usize) -> String {
    format!("{{ member = {member}, repair = {repair}, binding = {binding} }}")
}

fn subset(deployments: &[String]) -> String {
    format!("{{ deployments = [{}] }}", deployments.join(", "))
}

/// Runs `portfolio optimize` and returns its lines, the manifest path, and the selection.
fn optimize(scratch: &Scratch, config: &Path) -> Result<(Vec<String>, PathBuf, Selection), String> {
    let lines = command(&[
        "portfolio",
        "optimize",
        "--config",
        config.to_str().unwrap(),
    ])?;
    assert_eq!(lines.len(), 2, "{lines:?}");
    assert!(
        lines[0].starts_with("portfolio generation "),
        "{}",
        lines[0]
    );
    assert!(
        lines[1].starts_with("verified portfolio generation "),
        "{}",
        lines[1]
    );
    let manifest = published(scratch, &lines[0]);
    let selection = selection(scratch, &manifest);
    Ok((lines, manifest, selection))
}

fn selection(scratch: &Scratch, manifest: &Path) -> Selection {
    let manifest = SelectionManifest::from_json(&fs::read(manifest).unwrap()).unwrap();
    Selection::from_json(
        &fs::read(scratch.path("published").join(&manifest.objects[0].key)).unwrap(),
    )
    .unwrap()
}

fn decimal(text: &str) -> Decimal {
    Decimal::parse(text).unwrap()
}

/// Cents as a scale-two decimal.
fn cents(value: i64) -> Decimal {
    decimal(&format!(
        "{}{}.{:02}",
        if value < 0 { "-" } else { "" },
        value.abs() / 100,
        value.abs() % 100
    ))
}

fn read_manifest(scratch: &Scratch, generation: &str) -> Value {
    serde_json::from_slice(
        &fs::read(scratch.path(&format!("published/manifests/{generation}/ready.json"))).unwrap(),
    )
    .unwrap()
}

fn object_key(manifest: &Value, path: &str) -> String {
    manifest["objects"]
        .as_array()
        .unwrap()
        .iter()
        .find(|object| object["path"] == path)
        .unwrap()["key"]
        .as_str()
        .unwrap()
        .to_string()
}

/// The published summary of one replay generation.
fn replay_summary(scratch: &Scratch, generation: &str) -> Summary {
    let key = object_key(&read_manifest(scratch, generation), "summary.json");
    Summary::from_json(&fs::read(scratch.path("published").join(key)).unwrap()).unwrap()
}

/// The ledger records of one replay generation.
fn replay_events(scratch: &Scratch, generation: &str) -> Vec<FinancialEvent> {
    let key = object_key(&read_manifest(scratch, generation), "ledger/events.jsonl");
    fs::read(scratch.path("published").join(key))
        .unwrap()
        .split(|&byte| byte == b'\n')
        .filter(|line| !line.is_empty())
        .map(|line| FinancialEvent::from_line(line).unwrap())
        .collect()
}

/// The plan of one published feature generation.
fn plan(scratch: &Scratch, generation: &str) -> FeaturePlan {
    let manifest = FeatureManifest::from_json(
        &fs::read(scratch.path(&format!("published/manifests/{generation}/ready.json"))).unwrap(),
    )
    .unwrap();
    let object = manifest
        .objects
        .iter()
        .find(|object| object.path == PLAN_OBJECT_PATH)
        .unwrap();
    FeaturePlan::from_json(&fs::read(scratch.path("published").join(&object.key)).unwrap()).unwrap()
}

fn sha256_hex(bytes: &[u8]) -> String {
    use sha2::Digest;
    binary_alpha_engine::hex(&sha2::Sha256::digest(bytes))
}

// ----------------------------------------------------------------------------------------------
// The independent expectation: row-by-row admission under binding order and total capacity
// ----------------------------------------------------------------------------------------------

/// One deployment as the fixture reasons about it: the rows it signals on and what a win or a
/// loss nets, in cents.
#[derive(Clone, Copy)]
struct Sim {
    up: bool,
    narrow: bool,
    win_net: i64,
    loss_net: i64,
}

const NET_A: (i64, i64) = (80, -100);
const NET_B: (i64, i64) = (155, -205);

fn sim(up: bool, narrow: bool, terms: usize) -> Sim {
    let (win_net, loss_net) = if terms == 0 { NET_A } else { NET_B };
    Sim {
        up,
        narrow,
        win_net,
        loss_net,
    }
}

fn signals(row: &Row, deployment: &Sim) -> bool {
    row.up == deployment.up && (!deployment.narrow || !row.wide)
}

/// Replays deployments jointly: every row admits signalling deployments in order while the
/// open total is below `cap`; every admitted contract settles before the next row; the
/// drawdown is the largest gap below the running peak of completed profit.
fn joint(rows: &[Row], deployments: &[Sim], cap: usize) -> (i64, i64, u64) {
    let (mut profit, mut peak, mut drawdown, mut settled) = (0_i64, 0_i64, 0_i64, 0_u64);
    for row in rows {
        let mut open = 0;
        for deployment in deployments {
            if !signals(row, deployment) || open >= cap {
                continue;
            }
            open += 1;
            settled += 1;
            profit += if row.win {
                deployment.win_net
            } else {
                deployment.loss_net
            };
            peak = peak.max(profit);
            drawdown = drawdown.max(peak - profit);
        }
    }
    (profit, drawdown, settled)
}

fn choice_index(
    selection: &Selection,
    subset: usize,
    alternatives: &[usize],
    policy: usize,
) -> usize {
    selection
        .choices
        .iter()
        .position(|choice| {
            choice.subset == subset
                && choice.alternatives == alternatives
                && choice.risk_policy == policy
        })
        .unwrap()
}

// ----------------------------------------------------------------------------------------------
// The exact counting grid
// ----------------------------------------------------------------------------------------------

const PLANTED: Cells = Cells([first(6), first(2), first(5), first(4)]);
const SECOND: Cells = Cells([first(5), first(3), first(6), first(2)]);
const LOSING: Cells = Cells([first(2), first(2), first(3), first(3)]);

/// The four logical choices in declared order as (up, narrow): up, up/narrow, down,
/// down/narrow, each `(member, repair)` with member 0 = up and repair 1 = narrow.
const LOGICAL: [(bool, bool); 4] = [(true, false), (true, true), (false, false), (false, true)];

/// The sixteen allowed ordered subsets over the logical choices: every singleton once, then
/// every ordered pair of two distinct choices.
fn counting_subsets() -> Vec<Vec<usize>> {
    let mut subsets: Vec<Vec<usize>> = (0..4).map(|i| vec![i]).collect();
    for i in 0..4 {
        for j in 0..4 {
            if i != j {
                subsets.push(vec![i, j]);
            }
        }
    }
    subsets
}

/// The subset index of the ordered pair `(i, j)` of distinct logical choices.
fn pair(i: usize, j: usize) -> usize {
    assert_ne!(i, j);
    4 + i * 3 + if j > i { j - 1 } else { j }
}

fn subsets_toml(subsets: &[Vec<usize>]) -> String {
    subsets
        .iter()
        .map(|choices| {
            subset(
                &choices
                    .iter()
                    .map(|&choice| {
                        deployment(
                            usize::from(!LOGICAL[choice].0),
                            usize::from(LOGICAL[choice].1),
                            0,
                        )
                    })
                    .collect::<Vec<_>>(),
            )
        })
        .collect::<Vec<_>>()
        .join(", ")
}

/// The counting fixture: the family, two folds, the refit, and the evaluation slice.
struct Counting {
    family: PathBuf,
    fits: [Fit; 3],
    assessments: [PathBuf; 2],
    evaluation: PathBuf,
    members: String,
}

fn counting_fixture(scratch: &Scratch, evaluation_cells: Cells) -> Counting {
    let dev = development(scratch, "dev", BASE_MS, &recipe(PLANTED), false);
    let (family_manifest, family) = family(scratch, "grid", &dev, BASE_MS, DIRECTION_MENU, "");
    assert_eq!(family.members.len(), 2);
    Counting {
        family: family_manifest,
        members: format!(
            "{{ family = 0, member = {} }}, {{ family = 0, member = {} }}",
            member(&family, "up"),
            member(&family, "down")
        ),
        fits: [
            Fit {
                tick: dev.tick.clone(),
                profile: dev.profile.clone(),
                volume: false,
            },
            fit(
                scratch,
                "fit2",
                BASE_MS + 2 * HOUR_MS,
                &recipe(SECOND),
                false,
            ),
            fit(
                scratch,
                "refit",
                BASE_MS + 4 * HOUR_MS,
                &recipe(PLANTED),
                false,
            ),
        ],
        assessments: [
            import_slice(
                scratch,
                "assess1",
                "development",
                BASE_MS + HOUR_MS,
                &recipe(PLANTED),
            ),
            import_slice(
                scratch,
                "assess2",
                "development",
                BASE_MS + 3 * HOUR_MS,
                &recipe(SECOND),
            ),
        ],
        evaluation: import_slice(
            scratch,
            "eval",
            "evaluation",
            BASE_MS + 6 * HOUR_MS,
            &recipe(evaluation_cells),
        ),
    }
}

impl Counting {
    /// The complete table: the grid head, one binding with terms A and B, both folds, the refit,
    /// and the evaluation.
    fn table(
        &self,
        gates: &str,
        repairs: &str,
        subsets: &str,
        policies: &str,
        max_policies: u64,
    ) -> String {
        format!(
            "{}\n[[portfolio.bindings]]\nid = \"b\"\naccount = \"a\"\ninstrument = \"{INSTRUMENT}\"\n{}{}{}{}{}{}",
            head_toml(
                &[&self.family],
                "profit_then_drawdown",
                gates,
                ACCOUNT,
                NO_RATES,
                &self.members,
                repairs,
                subsets,
                policies,
                max_policies
            ),
            terms_a("A", "unit"),
            terms_b("B", "unit"),
            fold_toml(
                BASE_MS,
                &self.fits[0],
                BASE_MS + HOUR_MS,
                &self.assessments[0]
            ),
            fold_toml(
                BASE_MS + 2 * HOUR_MS,
                &self.fits[1],
                BASE_MS + 3 * HOUR_MS,
                &self.assessments[1]
            ),
            refit_toml(BASE_MS + 4 * HOUR_MS, &self.fits[2]),
            evaluation_toml(BASE_MS + 6 * HOUR_MS, &self.evaluation)
        )
    }
}

/// The expected values of every declared choice of the counting grid, in enumeration order:
/// the subset, its alternatives, the policy, whether it is a duplicate deployment, every fold's
/// (profit, drawdown, settled), and whether it passes the gates.
struct Expected {
    subset: usize,
    alternatives: Vec<usize>,
    policy: usize,
    rejected: bool,
    folds: Vec<(i64, i64, u64)>,
    passing: bool,
}

fn sims(choices: &[usize], alternatives: &[usize]) -> Vec<Sim> {
    choices
        .iter()
        .zip(alternatives)
        .map(|(&choice, &alternative)| sim(LOGICAL[choice].0, LOGICAL[choice].1, alternative))
        .collect()
}

fn counting_expectations(folds: &[Vec<Row>]) -> Vec<Expected> {
    let mut expected = Vec::new();
    for (subset, choices) in counting_subsets().iter().enumerate() {
        let combos: Vec<Vec<usize>> = if choices.len() == 1 {
            vec![vec![0], vec![1]]
        } else {
            vec![vec![0, 0], vec![0, 1], vec![1, 0], vec![1, 1]]
        };
        for alternatives in combos {
            for (policy, cap) in [(0, 1), (1, 2)] {
                // Two deployments of one member differing only by repair, with equal terms and
                // envelopes, are one deployment strategy on one account.
                let rejected = choices.len() == 2
                    && LOGICAL[choices[0]].0 == LOGICAL[choices[1]].0
                    && alternatives[0] == alternatives[1];
                let results: Vec<(i64, i64, u64)> = if rejected {
                    Vec::new()
                } else {
                    let deployments = sims(choices, &alternatives);
                    folds
                        .iter()
                        .map(|rows| joint(rows, &deployments, cap))
                        .collect()
                };
                let passing = !rejected && results.iter().all(|&(profit, _, _)| profit >= 0);
                expected.push(Expected {
                    subset,
                    alternatives: alternatives.clone(),
                    policy,
                    rejected,
                    folds: results,
                    passing,
                });
            }
        }
    }
    assert_eq!(expected.len(), 112);
    expected
}

#[test]
fn counting_grid_selects_verifies_and_resumes() {
    let scratch = Scratch::new("phase09_counting");
    let fixture = counting_fixture(&scratch, PLANTED);
    let repairs = format!("{{ id = \"none\" }}, {NARROW}");
    let policies = format!("{}, {}", risk_policy("cap1", 1), risk_policy("cap2", 2));
    let subsets = subsets_toml(&counting_subsets());
    let config = scratch.config(
        "portfolio.toml",
        &fixture.table(GATES, &repairs, &subsets, &policies, 1000),
    );

    // An interruption after some policy replays leaves completed replay generations and no
    // selection; the rerun reuses every one of them and reaches the same result.
    let before = scratch.manifests("published").len();
    let mut child = Command::new(env!("CARGO_BIN_EXE_binary-alpha"))
        .args([
            "portfolio",
            "optimize",
            "--config",
            config.to_str().unwrap(),
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    loop {
        std::thread::sleep(Duration::from_millis(5));
        if scratch.manifests("published").len() >= before + 12 {
            break;
        }
        assert!(
            child.try_wait().unwrap().is_none(),
            "the run finished before it was interrupted"
        );
    }
    child.kill().unwrap();
    child.wait().unwrap();
    let interrupted = scratch.manifests("published");
    for manifest in &interrupted {
        let json: Value = serde_json::from_slice(&fs::read(manifest).unwrap()).unwrap();
        assert_ne!(
            json["kind"], "portfolio_selection",
            "no selection after an interruption"
        );
    }
    let modified: Vec<_> = interrupted
        .iter()
        .map(|manifest| fs::metadata(manifest).unwrap().modified().unwrap())
        .collect();
    let (lines, manifest, selection) = optimize(&scratch, &config).unwrap();
    assert!(
        lines[0].contains(" declared 112 rejected 16 valid 96 passing "),
        "{}",
        lines[0]
    );
    assert!(
        lines[0].contains(" state selected objects 1 ["),
        "{}",
        lines[0]
    );
    assert_eq!(verify(&manifest).unwrap(), lines[1]);
    for (manifest, before) in interrupted.iter().zip(modified) {
        assert_eq!(
            fs::metadata(manifest).unwrap().modified().unwrap(),
            before,
            "{} was republished instead of reused",
            manifest.display()
        );
    }
    let again = optimize(&scratch, &config).unwrap();
    assert!(
        again.0[0].ends_with("(already published)"),
        "{}",
        again.0[0]
    );
    assert_eq!(again.2, selection);

    // Every declared choice matches the independent row-by-row expectation, in enumeration
    // order, with the existing duplicate-deployment rejection exactly where two deployments of
    // one member differ only by repair under equal terms.
    let folds = [recipe(PLANTED), recipe(SECOND)];
    let expected = counting_expectations(&folds);
    assert_eq!(
        (selection.declared, selection.rejected, selection.valid),
        (112, 16, 96)
    );
    assert_eq!(selection.choices.len(), 112);
    let mut passing = 0;
    for (choice, expected) in selection.choices.iter().zip(&expected) {
        assert_eq!(
            (choice.subset, &choice.alternatives, choice.risk_policy),
            (expected.subset, &expected.alternatives, expected.policy)
        );
        match (&choice.rejection, expected.rejected) {
            (Some(reason), true) => assert!(
                reason.contains(
                    "the same deployment strategy on account `a` is already bound by bindings[0]"
                ),
                "{reason}"
            ),
            (None, false) => {}
            other => panic!("{choice:?}: {other:?}"),
        }
        assert_eq!(choice.folds.len(), expected.folds.len());
        for (fold, &(profit, drawdown, settled)) in choice.folds.iter().zip(&expected.folds) {
            let projection = fold.projection.as_ref().unwrap();
            assert_eq!(fold.inapplicable, None);
            assert_eq!((projection.settled, projection.unresolved), (settled, 0));
            assert_eq!(projection.profit, Some(cents(profit)), "{choice:?}");
            assert_eq!(projection.drawdown, Some(cents(drawdown)), "{choice:?}");
            assert!(projection.rates.is_empty());
            assert_eq!(projection.unavailable_observations, 0);
            assert_eq!(projection.failure.is_none(), profit >= 0, "{choice:?}");
        }
        assert_eq!(choice.rank.is_some(), expected.passing, "{choice:?}");
        if expected.passing {
            passing += 1;
            let total: i64 = expected.folds.iter().map(|result| result.0).sum();
            let worst = expected.folds.iter().map(|result| result.1).max().unwrap();
            assert_eq!(
                (choice.profit, choice.drawdown),
                (Some(cents(total)), Some(cents(worst)))
            );
        } else if !expected.rejected {
            assert!(
                choice
                    .failure
                    .as_deref()
                    .is_some_and(|reason| reason.starts_with("fold ")),
                "{choice:?}"
            );
        }
    }
    assert_eq!(selection.passing, passing);
    assert!(passing > 0);
    // Ranking: larger profit, smaller drawdown, fewer deployments, then the canonical identity.
    let identities: BTreeSet<&str> = selection
        .choices
        .iter()
        .map(|choice| choice.identity.as_str())
        .collect();
    assert_eq!(
        identities.len(),
        112,
        "every complete choice has its own identity"
    );
    let mut ranked: Vec<usize> = (0..112)
        .filter(|&i| selection.choices[i].rank.is_some())
        .collect();
    ranked.sort_by(|&a, &b| {
        let (x, y) = (&selection.choices[a], &selection.choices[b]);
        y.profit
            .unwrap()
            .compare(x.profit.unwrap())
            .unwrap()
            .then(x.drawdown.unwrap().compare(y.drawdown.unwrap()).unwrap())
            .then(
                expected[a]
                    .alternatives
                    .len()
                    .cmp(&expected[b].alternatives.len()),
            )
            .then(x.identity.cmp(&y.identity))
    });
    for (rank, &index) in ranked.iter().enumerate() {
        assert_eq!(selection.choices[index].rank, Some(rank as u32 + 1));
    }
    assert_eq!(selection.selected, Some(ranked[0]));

    // The story the grid tells, never inferred from standalone sums.
    let fold_profit = |subset: usize, alternatives: &[usize], policy: usize| {
        selection.choices[choice_index(&selection, subset, alternatives, policy)].folds[0]
            .projection
            .as_ref()
            .unwrap()
            .profit
            .unwrap()
    };
    let simulated = |choices: &[usize], alternatives: &[usize], cap: usize| {
        cents(joint(&folds[0], &sims(choices, alternatives), cap).0)
    };
    let (up_a, narrow_a, narrow_b, weak) = (
        fold_profit(0, &[0], 0),
        fold_profit(1, &[0], 0),
        fold_profit(1, &[1], 0),
        fold_profit(3, &[0], 0),
    );
    assert_eq!(
        (up_a, narrow_a, narrow_b, weak),
        (cents(-160), cents(280), cents(520), cents(100))
    );
    // A positive standalone member first (up/narrow with A) blocks the better opportunity on
    // the contested narrow rows (up with B) under capacity one; the reversed order admits the
    // better one and blocks the first, so binding order changes admissions and results.
    let blocking = fold_profit(pair(1, 0), &[0, 1], 0);
    let reversed = fold_profit(pair(0, 1), &[1, 0], 0);
    assert_eq!(blocking, simulated(&[1, 0], &[0, 1], 1));
    assert_eq!(reversed, simulated(&[0, 1], &[1, 0], 1));
    assert_ne!(blocking, reversed);
    assert_eq!(
        reversed.compare(blocking).unwrap(),
        std::cmp::Ordering::Greater
    );
    // Under capacity two both trade every row they signal on.
    assert_eq!(
        fold_profit(pair(1, 0), &[0, 1], 1),
        narrow_a.checked_add(fold_profit(0, &[1], 0)).unwrap()
    );
    // The weak standalone member (down/narrow with A, +1.00) is useful jointly.
    assert_eq!(
        fold_profit(pair(1, 3), &[1, 0], 1),
        narrow_b.checked_add(weak).unwrap()
    );
    // The winner pairs up/narrow and down/narrow, both with B, in either order and under either
    // capacity (they never contest a row), with the tie broken by the canonical identity.
    let winner = &selection.choices[selection.selected.unwrap()];
    assert_eq!(winner.profit, Some(cents(1360)));
    assert!(
        winner.subset == pair(1, 3) || winner.subset == pair(3, 1),
        "{winner:?}"
    );
    assert_eq!(winner.alternatives, [1, 1]);
    let tied: Vec<&str> = selection
        .choices
        .iter()
        .filter(|choice| choice.profit == winner.profit && choice.drawdown == winner.drawdown)
        .map(|choice| choice.identity.as_str())
        .collect();
    assert_eq!(tied.len(), 4);
    assert_eq!(winner.identity.as_str(), *tied.iter().min().unwrap());

    // Exact stakes and non-proportional fees preserve cash and reservations: the winner's fold
    // ledgers end with cash equal to the initial cash plus the completed profit, nothing
    // reserved, nothing open, and every settlement posted the exact net amounts of terms B.
    for fold in &winner.folds {
        let generation = &fold.replay.as_ref().unwrap().generation;
        let summary = replay_summary(&scratch, generation);
        let account = &summary.accounts[0];
        let profit = fold.projection.as_ref().unwrap().profit.unwrap();
        assert_eq!(
            account.cash,
            decimal("10000.00").checked_add(profit).unwrap()
        );
        assert_eq!(account.completed_profit, profit);
        assert!(account.reserved.is_zero() && account.paid_basis.is_zero());
        assert_eq!(account.open, 0);
        let events = replay_events(&scratch, generation);
        let nets: BTreeSet<String> = events
            .iter()
            .filter_map(|event| match &event.kind {
                EventKind::Settled { profit, .. } => Some(profit.to_string()),
                _ => None,
            })
            .collect();
        assert_eq!(
            nets,
            BTreeSet::from(["-2.05".to_string(), "1.55".to_string()])
        );
        // Every acceptance debits exactly the purchase basis of terms B (cost 2 plus the 0.05
        // entry fee) and reserves exactly the terminal reserve beyond it, which is zero.
        let accepted: Vec<(String, String)> = events
            .iter()
            .filter_map(|event| match &event.kind {
                EventKind::Accepted {
                    debit, reservation, ..
                } => Some((debit.to_string(), reservation.to_string())),
                _ => None,
            })
            .collect();
        assert_eq!(
            accepted.len() as u64,
            fold.projection.as_ref().unwrap().settled
        );
        assert!(
            accepted
                .iter()
                .all(|posting| *posting == ("2.05".to_string(), "0.00".to_string())),
            "{accepted:?}"
        );
    }

    // The frozen policy carries the refit plan, the exact terms, and the order; the outer
    // evaluation follows the fixed choice once with reporting splits that never reset cash,
    // and a decision settling after the split boundary stays with its admission split.
    let frozen = selection.frozen.as_ref().unwrap();
    assert_eq!(selection.refit.len(), 1);
    assert!(
        frozen
            .strategies
            .iter()
            .all(|strategy| strategy.plan_identity == selection.refit[0].plan_identity)
    );
    assert_eq!(frozen.bindings.len(), 2);
    assert!(
        frozen
            .bindings
            .iter()
            .all(|binding| binding.contract == "B")
    );
    assert_eq!(frozen.contracts.len(), 1);
    assert_eq!(selection.state, State::Selected);
    let outer = selection.outer.as_ref().unwrap();
    assert_eq!(outer.projection.failure, None);
    let rows = recipe(PLANTED);
    let winner_choices = counting_subsets()[winner.subset].clone();
    let deployments = sims(&winner_choices, &winner.alternatives);
    let cap = if winner.risk_policy == 0 { 1 } else { 2 };
    let (profit, drawdown, settled) = joint(&rows, &deployments, cap);
    assert_eq!(
        (
            outer.projection.profit,
            outer.projection.drawdown,
            outer.projection.settled
        ),
        (Some(cents(profit)), Some(cents(drawdown)), settled)
    );
    let splits = &outer.splits;
    assert_eq!(splits.keys().cloned().collect::<Vec<_>>(), ["a", "b"]);
    assert_eq!(splits["a"].settled + splits["b"].settled, settled);
    assert_eq!(
        splits["a"].profit["unit"]
            .unwrap()
            .checked_add(splits["b"].profit["unit"].unwrap())
            .unwrap(),
        cents(profit)
    );
    // Rows 0..=16 are the seventeen decisions of split a; row 16 settles two seconds into
    // split b and stays attributed to a.
    let in_a: u64 = rows[..17]
        .iter()
        .map(|row| deployments.iter().filter(|sim| signals(row, sim)).count() as u64)
        .sum();
    assert!(signals(&rows[16], &deployments[0]) || signals(&rows[16], &deployments[1]));
    assert_eq!(splits["a"].settled, in_a);
    let summary = replay_summary(&scratch, &outer.replay.generation);
    assert_eq!(
        summary.accounts[0].cash,
        decimal("10000.00").checked_add(cents(profit)).unwrap()
    );
    assert_eq!(summary.splits, *splits);

    // A larger grid changes the procedure identity while the winner is unchanged; a count
    // above the maximum and an overflowing count fail before any allocation.
    let wide = format!(
        "{{ id = \"none\" }}, {NARROW}, {{ id = \"wide\", conditions = [{{ stream = {STREAM}, output = \"range_bps\", comparator = \"gt\", threshold = 0.08 }}] }}"
    );
    let larger_subsets = format!("{subsets}, {}", subset(&[deployment(0, 2, 0)]));
    let larger = scratch.config(
        "portfolio_larger.toml",
        &fixture.table(GATES, &wide, &larger_subsets, &policies, 1000),
    );
    let (_, larger_manifest, larger_selection) = optimize(&scratch, &larger).unwrap();
    assert_ne!(larger_manifest, manifest);
    assert_eq!(
        (larger_selection.declared, larger_selection.rejected),
        (116, 16)
    );
    assert_eq!(
        larger_selection.choices[larger_selection.selected.unwrap()].identity,
        winner.identity
    );
    assert_eq!(larger_selection.frozen, selection.frozen);
    let oversized = scratch.config(
        "portfolio_oversized.toml",
        &fixture.table(GATES, &repairs, &subsets, &policies, 111),
    );
    let error = command(&[
        "config",
        "validate",
        "--config",
        oversized.to_str().unwrap(),
    ])
    .unwrap_err();
    assert!(
        error.contains("max_policies: the grid declares 112 policies, above the maximum 111"),
        "{error}"
    );
    let sixty_four = subset(&vec![deployment(0, 0, 0); 64]);
    let overflowing = scratch.config(
        "portfolio_overflow.toml",
        &fixture.table(GATES, &repairs, &sixty_four, &policies, u64::MAX),
    );
    let error = command(&[
        "config",
        "validate",
        "--config",
        overflowing.to_str().unwrap(),
    ])
    .unwrap_err();
    assert!(
        error.contains("the declared policy count overflows"),
        "{error}"
    );

    // The typed reader preserves every identity and reason; verification restores every
    // referenced replay, so missing or altered referenced work cannot verify.
    let bytes = fs::read(&manifest).unwrap();
    let published = SelectionManifest::from_json(&bytes).unwrap();
    let selection_bytes =
        fs::read(scratch.path("published").join(&published.objects[0].key)).unwrap();
    assert_eq!(Selection::from_json(&selection_bytes).unwrap(), selection);
    let fold_generation = &winner.folds[0].replay.as_ref().unwrap().generation;
    let ledger_key = object_key(
        &read_manifest(&scratch, fold_generation),
        "ledger/events.jsonl",
    );
    let ledger_path = scratch.path("published").join(&ledger_key);
    let ledger_bytes = fs::read(&ledger_path).unwrap();
    fs::remove_file(&ledger_path).unwrap();
    let error = verify(&manifest).unwrap_err();
    assert!(error.contains("is missing"), "{error}");
    fs::write(&ledger_path, &ledger_bytes).unwrap();
    let rows_key = object_key(
        &read_manifest(&scratch, &selection.folds[0].assessments[0].generation),
        "rows/20s_0s.parquet",
    );
    let rows_path = scratch.path("published").join(&rows_key);
    let rows_bytes = fs::read(&rows_path).unwrap();
    fs::remove_file(&rows_path).unwrap();
    let error = verify(&manifest).unwrap_err();
    assert!(error.contains("is missing"), "{error}");
    fs::write(&rows_path, &rows_bytes).unwrap();
    let mut forged: Value = serde_json::from_slice(&selection_bytes).unwrap();
    let index = selection.selected.unwrap();
    forged["choices"][index]["profit"] = Value::from("999.00");
    let forged = serde_json::to_vec_pretty(&forged).unwrap();
    let sha = sha256_hex(&forged);
    fs::write(scratch.path(&format!("published/objects/{sha}")), &forged).unwrap();
    let mut manifest_json: Value = serde_json::from_slice(&bytes).unwrap();
    manifest_json["objects"][0]["key"] = Value::from(format!("objects/{sha}"));
    manifest_json["objects"][0]["sha256"] = Value::from(sha);
    manifest_json["objects"][0]["bytes"] = Value::from(forged.len());
    fs::write(
        &manifest,
        serde_json::to_vec_pretty(&manifest_json).unwrap(),
    )
    .unwrap();
    let error = verify(&manifest).unwrap_err();
    assert!(
        error.contains("records an identity, structure, folds, aggregate, or rank the procedure does not produce"),
        "{error}"
    );
    fs::write(&manifest, &bytes).unwrap();
    assert!(verify(&manifest).is_ok());
}

// ----------------------------------------------------------------------------------------------
// Two currencies valued at the restored ledger's final event time
// ----------------------------------------------------------------------------------------------

#[test]
fn two_currencies_are_valued_at_the_restored_ledger_time() {
    let scratch = Scratch::new("phase09_currency");
    let dev = development(&scratch, "dev", BASE_MS, &recipe(PLANTED), false);
    let (family_manifest, family) = family(&scratch, "fx", &dev, BASE_MS, DIRECTION_MENU, "");
    let assessment = import_slice(
        &scratch,
        "assess",
        "development",
        BASE_MS + HOUR_MS,
        &recipe(PLANTED),
    );
    let fit = Fit {
        tick: dev.tick.clone(),
        profile: dev.profile.clone(),
        volume: false,
    };
    let accounts = format!(
        "{ACCOUNT}, {{ id = \"e\", broker = \"pocket_option\", currency = \"eur\", scale = 2, initial_cash = \"10000\" }}"
    );
    // The rate is fresh at the last settlement (645 s after the slice opens) and stale at the
    // last tick (659.75 s), so valuation at the restored ledger time succeeds where valuation
    // at the last market observation would not.
    let rates = |present: bool| {
        let rate = if present {
            format!(
                "rates = [{{ id = \"r1\", source_currency = \"eur\", reporting_currency = \"unit\", provider = \"fx\", provider_time = \"{0}\", available_at = \"{0}\", rate = \"2.5\" }}]\n",
                time(BASE_MS + HOUR_MS)
            )
        } else {
            String::new()
        };
        format!("max_rate_age_micros = 650000000\n{rate}")
    };
    let table = |rates: &str, gates: &str| {
        format!(
            "{}\n[[portfolio.bindings]]\nid = \"unit\"\naccount = \"a\"\ninstrument = \"{INSTRUMENT}\"\n{}\n[[portfolio.bindings]]\nid = \"euro\"\naccount = \"e\"\ninstrument = \"{INSTRUMENT}\"\n{}{}{}",
            head_toml(
                &[&family_manifest],
                "profit_then_drawdown",
                gates,
                &accounts,
                rates,
                &format!("{{ family = 0, member = {} }}", member(&family, "up")),
                NARROW,
                &subset(&[deployment(0, 0, 0), deployment(0, 0, 1)]),
                &risk_policy("cap2", 2),
                10
            ),
            terms_a("A", "unit"),
            terms_a("E", "eur"),
            fold_toml(BASE_MS, &fit, BASE_MS + HOUR_MS, &assessment),
            refit_toml(BASE_MS, &fit)
        )
    };
    let config = scratch.config("portfolio_fx.toml", &table(&rates(true), GATES));
    let (lines, manifest, selection) = optimize(&scratch, &config).unwrap();
    assert!(
        lines[0].contains(" declared 1 rejected 0 valid 1 passing 1 state selected "),
        "{}",
        lines[0]
    );
    let choice = &selection.choices[0];
    let projection = choice.folds[0].projection.as_ref().unwrap();
    let (narrow, _, settled) = joint(&recipe(PLANTED), &[sim(true, true, 0)], 1);
    // The unit account nets the narrow profit; the euro account nets the same in euros, which
    // the ledger's rate converts once: 2.80 + 2.5 * 2.80 = 9.80.
    assert_eq!(narrow, 280);
    assert_eq!(projection.settled, 2 * settled);
    assert_eq!(projection.profit, Some(cents(narrow + narrow * 5 / 2)));
    assert_eq!(projection.rates, BTreeSet::from(["r1".to_string()]));
    let generation = &choice.folds[0].replay.as_ref().unwrap().generation;
    let summary = replay_summary(&scratch, generation);
    assert_eq!(summary.accounts[1].completed_profit, cents(narrow));
    // The last financial record is the repair-blocked signal of the final (wide) row at 640 s;
    // the last settlement precedes it and the trailing ticks of the tail candle, through
    // 659.75 s, produce no record.
    let last_record = time(BASE_MS + HOUR_MS + ROWS as i64 * CANDLE_MS);
    let last_tick_micros = (BASE_MS + HOUR_MS + (ROWS as i64 + 1) * CANDLE_MS - 250) * 1_000;
    assert_eq!(
        summary.last_time_micros.map(format_event_time_micros),
        Some(last_record.clone())
    );
    assert_eq!(
        replay_events(&scratch, generation)
            .iter()
            .map(|event| event.time_micros)
            .max()
            .map(format_event_time_micros),
        Some(last_record.clone())
    );
    assert!(summary.last_time_micros.unwrap() + 10_000_000 < last_tick_micros);
    assert!(
        last_tick_micros - (BASE_MS + HOUR_MS) * 1_000 > 650_000_000,
        "stale at the last tick"
    );
    assert_eq!(projection.valued_at, Some(last_record));
    assert!(projection.drawdown.is_some());
    assert_eq!(verify(&manifest).unwrap(), lines[1]);

    // Without the rate the euro profit is unavailable: the choice cannot pass, and the run
    // completes as a no-feasible result rather than an error.
    let missing = scratch.config("portfolio_fx_missing.toml", &table(&rates(false), GATES));
    let (lines, manifest, selection) = optimize(&scratch, &missing).unwrap();
    assert!(
        lines[0].contains(" passing 0 state no_feasible_policy "),
        "{}",
        lines[0]
    );
    let failure = selection.choices[0].folds[0]
        .projection
        .as_ref()
        .unwrap()
        .failure
        .clone()
        .unwrap();
    assert!(
        failure.contains("the completed profit of account `e` is unavailable: no eur to unit rate"),
        "{failure}"
    );
    assert_eq!(selection.state, State::NoFeasiblePolicy);
    assert_eq!(
        (
            selection.selected,
            selection.refit.len(),
            &selection.frozen,
            &selection.outer
        ),
        (None, 0, &None, &None)
    );
    assert_eq!(verify(&manifest).unwrap(), lines[1]);

    // Insufficient settlement support fails before any valuation.
    let unsupported = scratch.config(
        "portfolio_fx_support.toml",
        &table(
            &rates(true),
            "{ min_settled = 1000, max_unresolved = 0, min_profit = \"0\", max_drawdown = \"1000\" }",
        ),
    );
    let (lines, _, selection) = optimize(&scratch, &unsupported).unwrap();
    assert!(
        lines[0].contains(" passing 0 state no_feasible_policy "),
        "{}",
        lines[0]
    );
    let projection = selection.choices[0].folds[0].projection.as_ref().unwrap();
    assert_eq!(
        projection.failure.as_deref(),
        Some("settled 16 below the minimum 1000")
    );
    assert_eq!((projection.profit, projection.rates.len()), (None, 0));
}

// ----------------------------------------------------------------------------------------------
// Interval ordinals under separately fitted folds
// ----------------------------------------------------------------------------------------------

/// The tick-volume projection of one fitted plan.
fn volume_encoding(
    scratch: &Scratch,
    feature_generation: &str,
) -> binary_alpha_engine::features::FittedEncoding {
    plan(scratch, feature_generation)
        .stream(StreamKey {
            duration_seconds: 20,
            offset_seconds: 0,
        })
        .unwrap()
        .encodings
        .iter()
        .find(|encoding| encoding.output == "tick_volume_dev_quantile")
        .unwrap()
        .clone()
}

/// The rows whose volume falls in interval `ordinal` of the fitted cuts: right-closed bins
/// with unbounded tails.
fn rows_in_interval(edges: &[f64], ordinal: usize, rows: &[Row]) -> u64 {
    assert_eq!(edges.len(), 4);
    let mut full = vec![f64::NEG_INFINITY];
    full.extend_from_slice(edges);
    full.push(f64::INFINITY);
    rows.iter()
        .filter(|row| {
            let value = row.volume as f64;
            full.partition_point(|edge| *edge < value).saturating_sub(1) == ordinal
        })
        .count() as u64
}

#[test]
fn interval_ordinals_follow_each_fold_fit() {
    let scratch = Scratch::new("phase09_ordinals");
    // Fit one: cuts 22.8, 34, 52, 60 (the 0.8 quantile lands exactly on 60). Fit two: cuts
    // 40, 48, 66, 78. Their top intervals hold six and seven rows, so the fitted label codes
    // rank differently while the ordinal means the same interval.
    let fit1_volumes: [usize; 32] = [
        21, 21, 21, 21, 21, 21, 21, 30, 30, 30, 30, 30, 30, 40, 40, 40, 40, 40, 40, 60, 60, 60, 60,
        60, 60, 60, 80, 80, 80, 80, 80, 80,
    ];
    let fit2_volumes: [usize; 32] = [
        21, 21, 21, 21, 21, 21, 40, 40, 40, 40, 40, 40, 40, 60, 60, 60, 60, 60, 60, 70, 70, 70, 70,
        70, 70, 80, 80, 80, 80, 80, 80, 80,
    ];
    let assessment_volumes = [80, 70, 60, 40];
    let assessed = |cells: Cells| with_volumes(recipe(cells), &assessment_volumes);
    let dev = development(
        &scratch,
        "dev",
        BASE_MS,
        &with_volumes(recipe(PLANTED), &fit1_volumes),
        true,
    );
    let top = volume_encoding(&scratch, &generation_of(&dev.feature))
        .interval_label(4)
        .unwrap();
    let menu = format!(
        "\n[[search.conditions]]\nstream = {STREAM}\noutput = \"tick_volume_dev_quantile\"\ncomparator = \"eq\"\nthresholds = [\"{top}\"]\n"
    );
    let (family_manifest, volume) = family(&scratch, "volume", &dev, BASE_MS, &menu, "");
    assert_eq!(volume.members.len(), 1);
    let fits = [
        Fit {
            tick: dev.tick.clone(),
            profile: dev.profile.clone(),
            volume: true,
        },
        fit(
            &scratch,
            "fit2",
            BASE_MS + 2 * HOUR_MS,
            &with_volumes(recipe(SECOND), &fit2_volumes),
            true,
        ),
        fit(
            &scratch,
            "flat",
            BASE_MS + 8 * HOUR_MS,
            &recipe(PLANTED),
            true,
        ),
    ];
    let refit = fit(
        &scratch,
        "refit",
        BASE_MS + 4 * HOUR_MS,
        &with_volumes(recipe(PLANTED), &fit1_volumes),
        true,
    );
    let assessments = [
        import_slice(
            &scratch,
            "assess1",
            "development",
            BASE_MS + HOUR_MS,
            &assessed(PLANTED),
        ),
        import_slice(
            &scratch,
            "assess2",
            "development",
            BASE_MS + 3 * HOUR_MS,
            &assessed(SECOND),
        ),
        import_slice(
            &scratch,
            "assess3",
            "development",
            BASE_MS + 9 * HOUR_MS,
            &assessed(PLANTED),
        ),
    ];
    let other_assessment = import_slice(
        &scratch,
        "assess_other",
        "development",
        BASE_MS + HOUR_MS,
        &assessed(LOSING),
    );
    let evaluation = import_slice(
        &scratch,
        "eval",
        "evaluation",
        BASE_MS + 6 * HOUR_MS,
        &assessed(PLANTED),
    );
    let other_evaluation = import_slice(
        &scratch,
        "eval_other",
        "evaluation",
        BASE_MS + 6 * HOUR_MS,
        &assessed(LOSING),
    );
    let gates =
        "{ min_settled = 1, max_unresolved = 0, min_profit = \"-1000\", max_drawdown = \"1000\" }";
    let (direction_manifest, direction) =
        family(&scratch, "direction", &dev, BASE_MS, DIRECTION_MENU, "");
    let ordinal_member = "{ family = 0, member = 0, ordinals = [{ condition = 0, ordinal = 4 }] }";
    let runner_up = format!(
        "{ordinal_member}, {{ family = 1, member = {} }}",
        member(&direction, "down")
    );
    let table = |folds: &str,
                 refit_base_ms: i64,
                 refit: &Fit,
                 evaluation_base_ms: i64,
                 evaluation: &Path,
                 max_labels: u32,
                 with_runner_up: bool| {
        let families: Vec<&Path> = if with_runner_up {
            vec![&family_manifest, &direction_manifest]
        } else {
            vec![&family_manifest]
        };
        let subsets = if with_runner_up {
            format!(
                "{}, {}",
                subset(&[deployment(0, 0, 0)]),
                subset(&[deployment(1, 0, 0)])
            )
        } else {
            subset(&[deployment(0, 0, 0)])
        };
        format!(
            "{}\n[[portfolio.bindings]]\nid = \"b\"\naccount = \"a\"\ninstrument = \"{INSTRUMENT}\"\n{}{folds}{}{}",
            head_toml(
                &families,
                "profit_then_drawdown",
                gates,
                ACCOUNT,
                NO_RATES,
                if with_runner_up { &runner_up } else { ordinal_member },
                "{ id = \"none\" }",
                &subsets,
                &risk_policy("cap1", 1),
                10
            ),
            terms_a("A", "unit"),
            refit_toml(refit_base_ms, refit),
            evaluation_toml(evaluation_base_ms, evaluation)
        )
        .replace("max_labels = 32768", &format!("max_labels = {max_labels}"))
    };
    let two_folds = format!(
        "{}{}",
        fold_toml(BASE_MS, &fits[0], BASE_MS + HOUR_MS, &assessments[0]),
        fold_toml(
            BASE_MS + 2 * HOUR_MS,
            &fits[1],
            BASE_MS + 3 * HOUR_MS,
            &assessments[1]
        )
    );
    let config = scratch.config(
        "portfolio_ordinals.toml",
        &table(
            &two_folds,
            BASE_MS + 4 * HOUR_MS,
            &refit,
            BASE_MS + 6 * HOUR_MS,
            &evaluation,
            32768,
            false,
        ),
    );
    let (lines, manifest, selection) = optimize(&scratch, &config).unwrap();
    assert!(
        lines[0].contains(" declared 1 rejected 0 valid 1 passing 1 state selected "),
        "{}",
        lines[0]
    );
    assert_eq!(verify(&manifest).unwrap(), lines[1]);
    // A recorded configuration whose fit declares another label limit, republished under the
    // generation its own hash names, does not resolve to the recorded fits.
    let published = SelectionManifest::from_json(&fs::read(&manifest).unwrap()).unwrap();
    let mut forged: Value = serde_json::from_slice(
        &fs::read(scratch.path("published").join(&published.objects[0].key)).unwrap(),
    )
    .unwrap();
    forged["config"]["portfolio"]["folds"][0]["inputs"][0]["fit"]["encodings"]["max_labels"] =
        Value::from(4);
    let forged_config: binary_alpha_engine::config::Config =
        serde_json::from_value(forged["config"].clone()).unwrap();
    let forged_bytes = serde_json::to_vec_pretty(&forged).unwrap();
    let sha = sha256_hex(&forged_bytes);
    fs::write(
        scratch.path(&format!("published/objects/{sha}")),
        &forged_bytes,
    )
    .unwrap();
    let mut forged_manifest = published.clone();
    forged_manifest.config_hash = forged_config.content_hash();
    forged_manifest.generation = selection_generation_id(
        &forged_manifest.config_hash,
        &forged_manifest.code_revision,
        &forged_manifest.families,
    );
    forged_manifest.objects[0].key = format!("objects/{sha}");
    forged_manifest.objects[0].sha256 = sha;
    forged_manifest.objects[0].bytes = forged_bytes.len() as u64;
    let forged_path = scratch.path(&format!(
        "published/manifests/{}/ready.json",
        forged_manifest.generation
    ));
    fs::create_dir_all(forged_path.parent().unwrap()).unwrap();
    fs::write(&forged_path, forged_manifest.to_json()).unwrap();
    let error = verify(&forged_path).unwrap_err();
    assert!(
        error.contains("does not resolve to the recorded plan before its fit"),
        "{error}"
    );
    let choice = &selection.choices[0];
    let assessment_rows = [assessed(PLANTED), assessed(SECOND)];
    let mut cuts = Vec::new();
    for (index, fold) in choice.folds.iter().enumerate() {
        let fit = &selection.folds[index].fits[0];
        let encoding = volume_encoding(&scratch, &fit.generation);
        let label = encoding.interval_label(4).unwrap();
        let generation = &fold.replay.as_ref().unwrap().generation;
        let events = replay_events(&scratch, generation);
        let EventKind::RunDefinition { definition } = &events[0].kind else {
            panic!("{:?}", events[0].kind);
        };
        let strategy = &definition.replay.strategies[0];
        assert_eq!(strategy.plan_identity, fit.plan_identity);
        assert_eq!(
            strategy.conditions[0].threshold,
            Threshold::Text(label.clone())
        );
        // The label is the interval's, whatever its frequency-ranked code.
        let code = encoding
            .labels
            .iter()
            .position(|text| *text == label)
            .unwrap();
        cuts.push((encoding.edges.clone().unwrap(), code));
        let summary = replay_summary(&scratch, generation);
        let expected = rows_in_interval(&cuts[index].0, 4, &assessment_rows[index]);
        assert_eq!(summary.strategies["d0"].signals, expected, "fold {index}");
        assert_eq!(summary.strategies["d0"].settled, expected, "fold {index}");
    }
    // Fold one's top interval is (60, inf): the 70- and 80-tick rows, while a 60-tick row sits
    // on the cut and stays below it; fold two's top interval is (78, inf): the 80-tick rows.
    for (fitted, expected) in [
        (&cuts[0].0, [22.8, 34.0, 52.0, 60.0]),
        (&cuts[1].0, [40.0, 48.0, 66.0, 78.0]),
    ] {
        assert!(
            fitted
                .iter()
                .zip(expected)
                .all(|(cut, expected)| (cut - expected).abs() < 1e-9),
            "{fitted:?}"
        );
    }
    assert_eq!(
        cuts[0].0[3], 60.0,
        "the 0.8 quantile lands exactly on the cut"
    );
    assert_ne!(cuts[0].1, cuts[1].1, "the fitted codes rank differently");
    let signals_of = |fold: usize| {
        replay_summary(
            &scratch,
            &choice.folds[fold].replay.as_ref().unwrap().generation,
        )
        .strategies["d0"]
            .signals
    };
    assert_eq!((signals_of(0), signals_of(1)), (16, 8));
    let frozen = selection.frozen.clone().unwrap();
    assert_eq!(
        frozen.strategies[0].plan_identity,
        selection.refit[0].plan_identity
    );

    // Mutating the inner assessment observations cannot alter the earlier fit.
    let other_folds = format!(
        "{}{}",
        fold_toml(BASE_MS, &fits[0], BASE_MS + HOUR_MS, &other_assessment),
        fold_toml(
            BASE_MS + 2 * HOUR_MS,
            &fits[1],
            BASE_MS + 3 * HOUR_MS,
            &assessments[1]
        )
    );
    let other = scratch.config(
        "portfolio_ordinals_other.toml",
        &table(
            &other_folds,
            BASE_MS + 4 * HOUR_MS,
            &refit,
            BASE_MS + 6 * HOUR_MS,
            &evaluation,
            32768,
            false,
        ),
    );
    let (_, _, changed) = optimize(&scratch, &other).unwrap();
    assert_eq!(changed.folds[0].fits, selection.folds[0].fits);
    assert_ne!(changed.folds[0].assessments, selection.folds[0].assessments);
    assert_ne!(changed.choices[0].folds[0], selection.choices[0].folds[0]);

    // Mutating the outer ticks cannot alter the chosen template, the resolved strategy, the
    // terms, the risk settings, the order, or the fitted-plan identities.
    let outer = scratch.config(
        "portfolio_ordinals_outer.toml",
        &table(
            &two_folds,
            BASE_MS + 4 * HOUR_MS,
            &refit,
            BASE_MS + 6 * HOUR_MS,
            &other_evaluation,
            32768,
            false,
        ),
    );
    let (_, _, moved) = optimize(&scratch, &outer).unwrap();
    assert_eq!(
        (&moved.choices, moved.selected, &moved.refit, &moved.frozen),
        (
            &selection.choices,
            selection.selected,
            &selection.refit,
            &selection.frozen
        )
    );
    assert_ne!(moved.outer, selection.outer);

    // Collapsed cuts (one distinct volume) make the choice inapplicable for that required fold.
    let three_folds = format!(
        "{two_folds}{}",
        fold_toml(
            BASE_MS + 8 * HOUR_MS,
            &fits[2],
            BASE_MS + 9 * HOUR_MS,
            &assessments[2]
        )
    );
    let collapsed = scratch.config(
        "portfolio_ordinals_collapsed.toml",
        &table(
            &three_folds,
            BASE_MS + 4 * HOUR_MS,
            &refit,
            BASE_MS + 6 * HOUR_MS,
            &evaluation,
            32768,
            false,
        ),
    );
    let (lines, manifest, selection) = optimize(&scratch, &collapsed).unwrap();
    assert!(
        lines[0].contains(" passing 0 state no_feasible_policy "),
        "{}",
        lines[0]
    );
    let reason = selection.choices[0].folds[2].inapplicable.clone().unwrap();
    assert!(
        reason.contains("fitted 0 distinct cuts, not four"),
        "{reason}"
    );
    assert_eq!(selection.choices[0].folds[2].replay, None);
    assert_eq!(
        selection.choices[0].failure.as_deref(),
        Some(format!("fold 2: {reason}").as_str())
    );
    assert_eq!(verify(&manifest).unwrap(), lines[1]);

    // An omitted label (a label limit below the five intervals) makes the fold inapplicable.
    let omitted = scratch.config(
        "portfolio_ordinals_omitted.toml",
        &table(
            &two_folds,
            BASE_MS + 4 * HOUR_MS,
            &refit,
            BASE_MS + 6 * HOUR_MS,
            &evaluation,
            4,
            false,
        ),
    );
    let (lines, _, selection) = optimize(&scratch, &omitted).unwrap();
    assert!(
        lines[0].contains(" passing 0 state no_feasible_policy "),
        "{}",
        lines[0]
    );
    let reason = selection.choices[0].folds[0].inapplicable.clone().unwrap();
    assert!(reason.contains("did not retain interval 4"), "{reason}");

    // A final refit whose cuts collapse is terminal without a deployable selection; it does not
    // choose the next-ranked policy, and no outer evaluation is read.
    // The evaluation manifest of this configuration does not exist: a failed refit never reads
    // it.
    let absent = scratch.path(&format!(
        "published/manifests/{}/ready.json",
        "0".repeat(64)
    ));
    let refit_config = scratch.config(
        "portfolio_ordinals_refit.toml",
        &table(
            &two_folds,
            BASE_MS + 8 * HOUR_MS,
            &fits[2],
            BASE_MS + 10 * HOUR_MS,
            &absent,
            32768,
            true,
        ),
    );
    let (lines, manifest, selection) = optimize(&scratch, &refit_config).unwrap();
    assert!(
        lines[0].contains(" declared 2 rejected 0 valid 2 passing 2 state refit_inapplicable "),
        "{}",
        lines[0]
    );
    // The feasible runner-up (down, rank two) is never chosen in the winner's place.
    assert_eq!(
        (selection.choices[0].rank, selection.choices[1].rank),
        (Some(1), Some(2))
    );
    assert!(
        matches!(&selection.state, State::RefitInapplicable { reason } if reason.contains("fitted 0 distinct cuts, not four")),
        "{:?}",
        selection.state
    );
    assert_eq!(
        (
            selection.selected,
            selection.refit.len(),
            &selection.frozen,
            &selection.outer
        ),
        (Some(0), 1, &None, &None)
    );
    assert_eq!(verify(&manifest).unwrap(), lines[1]);
}

// ----------------------------------------------------------------------------------------------
// Refusals before any output
// ----------------------------------------------------------------------------------------------

#[test]
fn later_role_evidence_and_ill_formed_inputs_are_refused_before_output() {
    let scratch = Scratch::new("phase09_refusals");
    let dev = development(&scratch, "dev", BASE_MS, &recipe(PLANTED), false);
    let (family_manifest, grid) = family(&scratch, "grid", &dev, BASE_MS, DIRECTION_MENU, "");
    let assessment = import_slice(
        &scratch,
        "assess",
        "development",
        BASE_MS + HOUR_MS,
        &recipe(PLANTED),
    );
    let fit = Fit {
        tick: dev.tick.clone(),
        profile: dev.profile.clone(),
        volume: false,
    };
    let table = |family_manifest: &Path, fold: &str| {
        format!(
            "{}\n[[portfolio.bindings]]\nid = \"b\"\naccount = \"a\"\ninstrument = \"{INSTRUMENT}\"\n{}{fold}{}",
            head_toml(
                &[family_manifest],
                "profit_then_drawdown",
                GATES,
                ACCOUNT,
                NO_RATES,
                &format!("{{ family = 0, member = {} }}", member(&grid, "up")),
                "{ id = \"none\" }",
                &subset(&[deployment(0, 0, 0)]),
                &risk_policy("cap1", 1),
                10
            ),
            terms_a("A", "unit"),
            refit_toml(BASE_MS, &fit)
        )
    };
    let fold = fold_toml(BASE_MS, &fit, BASE_MS + HOUR_MS, &assessment);
    let snapshot =
        |scratch: &Scratch| (scratch.manifests("published"), scratch.objects("published"));

    // An evaluation-bearing family is refused before its inaccessible evaluation references
    // are opened and before any selection output exists.
    let evaluation_tick = import_slice(
        &scratch,
        "eval",
        "evaluation",
        BASE_MS + 6 * HOUR_MS,
        &recipe(PLANTED),
    );
    let evaluation_feature = build_features(
        &scratch,
        "eval",
        "evaluation",
        &evaluation_tick,
        &dev.profile,
        &format!("frozen_plan = \"{}\"\n", manifest_uri(&dev.feature)),
    );
    let (start, end) = window(BASE_MS + 6 * HOUR_MS);
    let evaluation = format!(
        "\n[search.evaluation]\ndecision_start = \"{start}\"\ndecision_end = \"{end}\"\ninputs = [{{ tick_manifest = \"{}\", feature_manifest = \"{}\" }}]\n",
        manifest_uri(&evaluation_tick),
        manifest_uri(&evaluation_feature)
    );
    let (bearing, _) = family(
        &scratch,
        "bearing",
        &dev,
        BASE_MS,
        DIRECTION_MENU,
        &evaluation,
    );
    fs::remove_file(&evaluation_tick).unwrap();
    assert!(
        verify(&bearing).is_err(),
        "the evaluation reference is inaccessible"
    );
    let before = snapshot(&scratch);
    let config = scratch.config("portfolio_bearing.toml", &table(&bearing, &fold));
    let error = optimize(&scratch, &config).unwrap_err();
    assert!(
        error.contains("is `evaluation`; a portfolio universe reads development-only families"),
        "{error}"
    );
    assert_eq!(snapshot(&scratch), before);

    // A holdout-role input is refused on its manifest bytes before any output.
    let mut holdout = GenerationManifest::from_json(&fs::read(&assessment).unwrap()).unwrap();
    holdout.role = DatasetRole::Holdout;
    let PriceRepresentation::IntegerUnits { scale } = holdout.price_representation else {
        panic!("tick sources carry integer units");
    };
    holdout.generation = generation_id(
        &InstrumentId {
            broker: holdout.broker.clone(),
            provider_symbol: holdout.provider_symbol.clone(),
        },
        holdout.source_kind,
        DatasetRole::Holdout,
        Some(scale),
        &holdout.objects,
    );
    let holdout_path = scratch.path(&format!(
        "published/manifests/{}/ready.json",
        holdout.generation
    ));
    fs::create_dir_all(holdout_path.parent().unwrap()).unwrap();
    fs::write(&holdout_path, holdout.to_json()).unwrap();
    let before = snapshot(&scratch);
    let config = scratch.config(
        "portfolio_holdout.toml",
        &table(
            &family_manifest,
            &fold_toml(BASE_MS, &fit, BASE_MS + HOUR_MS, &holdout_path),
        ),
    );
    let error = optimize(&scratch, &config).unwrap_err();
    assert!(
        error.contains("assessment_manifest: holdout data never enters a portfolio selection"),
        "{error}"
    );
    assert_eq!(snapshot(&scratch), before);

    // A fit whose profile was built from another generation is refused before any output.
    let wrong = Fit {
        tick: dev.tick.clone(),
        profile: audit(&scratch, "other", &assessment),
        volume: false,
    };
    let before = snapshot(&scratch);
    let config = scratch.config(
        "portfolio_profile.toml",
        &table(
            &family_manifest,
            &fold_toml(BASE_MS, &wrong, BASE_MS + HOUR_MS, &assessment),
        ),
    );
    let error = optimize(&scratch, &config).unwrap_err();
    assert!(
        error.contains("a new plan fits on the generation its profile was built from"),
        "{error}"
    );
    assert_eq!(snapshot(&scratch), before);

    // A fit whose coverage crosses its cutoff is refused before any output; an assessment that
    // starts inside the embargo, a reversed outer chronology, and an empty fold list are
    // refused by configuration validation.
    let crossing = scratch.config(
        "portfolio_crossing.toml",
        &table(&family_manifest, &fold).replace(&cutoff(BASE_MS), &time(BASE_MS + 10 * CANDLE_MS)),
    );
    let error = optimize(&scratch, &crossing).unwrap_err();
    assert!(error.contains("not before the cutoff"), "{error}");
    assert_eq!(snapshot(&scratch), before);
    let early = scratch.config(
        "portfolio_early.toml",
        &table(&family_manifest, &fold).replace(
            &format!("decision_start = \"{}\"", window(BASE_MS + HOUR_MS).0),
            &format!(
                "decision_start = \"{}\"",
                time(BASE_MS + (ROWS as i64 + 1) * CANDLE_MS + 1_000)
            ),
        ),
    );
    let error = command(&["config", "validate", "--config", early.to_str().unwrap()]).unwrap_err();
    assert!(
        error.contains("folds[0].decision_start")
            && error.contains("begins less than the embargo after the cutoff"),
        "{error}"
    );
    let reversed = scratch.config(
        "portfolio_reversed.toml",
        &format!(
            "{}{}",
            table(&family_manifest, &fold),
            evaluation_toml(BASE_MS - HOUR_MS, &assessment)
        ),
    );
    let error = optimize(&scratch, &reversed).unwrap_err();
    assert_eq!(snapshot(&scratch), before);
    assert!(
        error.contains("evaluation.decision_start")
            && error.contains("begins less than the embargo after the cutoff"),
        "{error}"
    );
    let overlapping = scratch.config(
        "portfolio_overlapping.toml",
        &format!(
            "{}{}",
            table(&family_manifest, &fold),
            evaluation_toml(BASE_MS, &dev.tick)
        ),
    );
    let error = optimize(&scratch, &overlapping).unwrap_err();
    assert_eq!(snapshot(&scratch), before);
    assert!(
        error.contains("evaluation.decision_start")
            && error.contains("begins less than the embargo after the cutoff"),
        "{error}"
    );
    let empty = scratch.config(
        "portfolio_empty.toml",
        &table(&family_manifest, "").replace("\nmembers = [", "\nfolds = []\nmembers = ["),
    );
    let error = optimize(&scratch, &empty).unwrap_err();
    assert_eq!(snapshot(&scratch), before);
    assert!(
        error.contains("folds: at least one inner fit and assessment pair is required"),
        "{error}"
    );
}

// ----------------------------------------------------------------------------------------------
// Synchronized and separated losses, and the terminal states
// ----------------------------------------------------------------------------------------------

#[test]
fn synchronized_and_separated_losses_differ_and_terminal_states_are_distinct() {
    let scratch = Scratch::new("phase09_states");
    // Up/narrow and down/narrow each win six of eight rows with their losses at different
    // positions, so two deployments with equal marginal returns lose together when they trade
    // the same signal and apart when they trade different rows.
    let cells = Cells([0b1110_1110, first(2), 0b1011_1011, first(3)]);
    let dev = development(&scratch, "dev", BASE_MS, &recipe(cells), false);
    let (family_manifest, family) = family(&scratch, "grid", &dev, BASE_MS, DIRECTION_MENU, "");
    let assessment = import_slice(
        &scratch,
        "assess",
        "development",
        BASE_MS + HOUR_MS,
        &recipe(cells),
    );
    let fit = Fit {
        tick: dev.tick.clone(),
        profile: dev.profile.clone(),
        volume: false,
    };
    let rows = recipe(cells);
    let up = sim(true, true, 0);
    let down = sim(false, true, 0);
    let separated = joint(&rows, &[up, down], 2);
    let synchronized = joint(&rows, &[up, up], 2);
    let blocked = joint(&rows, &[up, up], 1);
    assert_eq!(separated.0, synchronized.0, "equal marginal returns");
    assert!(
        synchronized.1 > separated.1,
        "{synchronized:?} {separated:?}"
    );
    assert_eq!(
        blocked.0,
        joint(&rows, &[up], 1).0,
        "the second deployment is blocked on every row"
    );
    // Terms A and its envelope variant A' admit the same contract under distinct deployment
    // identities; member 0 is up, member 1 is down, and the one repair is narrow.
    let table = |objective: &str, gates: &str, evaluation: &str| {
        format!(
            "{}\n[[portfolio.bindings]]\nid = \"b\"\naccount = \"a\"\ninstrument = \"{INSTRUMENT}\"\n{}{}{}{}{evaluation}",
            head_toml(
                &[&family_manifest],
                objective,
                gates,
                ACCOUNT,
                NO_RATES,
                &format!(
                    "{{ family = 0, member = {} }}, {{ family = 0, member = {} }}",
                    member(&family, "up"),
                    member(&family, "down")
                ),
                NARROW,
                &format!(
                    "{}, {}",
                    subset(&[deployment(0, 0, 0), deployment(0, 0, 0)]),
                    subset(&[deployment(0, 0, 0), deployment(1, 0, 0)])
                ),
                &format!(
                    "{}, {}, {}",
                    risk_policy("cap2", 2),
                    risk_policy("cap1", 1),
                    risk_policy("cap2", 2).replace("cap2", "loss").replace(
                        "max_open_total = 2",
                        "max_open_total = 2, max_unresolved_loss_total = \"1.50\""
                    )
                ),
                24
            ),
            terms_a("A", "unit"),
            alternative("A2", "unit", "1", "0", "1.80", "1", "0.80", "0.01"),
            fold_toml(BASE_MS, &fit, BASE_MS + HOUR_MS, &assessment),
            refit_toml(BASE_MS, &fit)
        )
    };
    let config = scratch.config(
        "portfolio_sync.toml",
        &table("drawdown_then_profit", GATES, ""),
    );
    let (lines, manifest, selection) = optimize(&scratch, &config).unwrap();
    assert!(
        lines[0].contains(" declared 24 rejected 6 valid 18 "),
        "{}",
        lines[0]
    );
    assert_eq!(verify(&manifest).unwrap(), lines[1]);
    let value = |subset: usize, alternatives: &[usize], policy: usize| {
        let choice = &selection.choices[choice_index(&selection, subset, alternatives, policy)];
        let projection = choice.folds[0].projection.as_ref().unwrap();
        (
            projection.profit.unwrap(),
            projection.drawdown.unwrap(),
            projection.settled,
        )
    };
    let as_cents = |result: (i64, i64, u64)| (cents(result.0), cents(result.1), result.2);
    assert!(
        selection.choices[choice_index(&selection, 0, &[0, 0], 0)]
            .rejection
            .is_some()
    );
    assert!(
        selection.choices[choice_index(&selection, 0, &[1, 1], 1)]
            .rejection
            .is_some()
    );
    assert_eq!(value(0, &[0, 1], 0), as_cents(synchronized));
    assert_eq!(value(1, &[0, 0], 0), as_cents(separated));
    assert_eq!(value(0, &[0, 1], 1), as_cents(blocked));
    assert_eq!(value(1, &[0, 0], 1), as_cents(separated));
    // A total unresolved-loss limit of 1.50 binds on the exact worst-loss reservation of 1.00
    // per open contract: the synchronized second deployment is blocked, the separated pair is
    // untouched.
    assert_eq!(value(0, &[0, 1], 2), as_cents(blocked));
    assert_eq!(value(1, &[0, 0], 2), as_cents(separated));
    let sync_selected = selection.selected.unwrap();
    let winner = &selection.choices[sync_selected];
    assert_eq!(
        (winner.subset, winner.drawdown),
        (1, Some(cents(separated.1)))
    );
    assert_eq!(winner.profit, Some(cents(separated.0)));

    // Every completed choice failing the gates is a no-feasible result with no refit and no
    // outer read; an outer rejection keeps the frozen choice without changing it.
    // The configured evaluation manifest does not exist: an all-infeasible grid never reads it.
    let absent = scratch.path(&format!(
        "published/manifests/{}/ready.json",
        "0".repeat(64)
    ));
    let infeasible = scratch.config(
        "portfolio_infeasible.toml",
        &table(
            "drawdown_then_profit",
            "{ min_settled = 1, max_unresolved = 0, min_profit = \"1000\", max_drawdown = \"1000\" }",
            &evaluation_toml(BASE_MS + 6 * HOUR_MS, &absent),
        ),
    );
    let (lines, manifest, selection) = optimize(&scratch, &infeasible).unwrap();
    assert!(
        lines[0].contains(" valid 18 passing 0 state no_feasible_policy "),
        "{}",
        lines[0]
    );
    assert!(
        selection
            .choices
            .iter()
            .filter(|choice| choice.rejection.is_none())
            .all(|choice| choice
                .failure
                .as_deref()
                .is_some_and(|reason| reason.contains("below the minimum 1000")))
    );
    assert_eq!(
        (
            selection.selected,
            &selection.frozen,
            &selection.outer,
            &selection.state
        ),
        (None, &None, &None, &State::NoFeasiblePolicy)
    );
    assert_eq!(verify(&manifest).unwrap(), lines[1]);
    let losing = import_slice(
        &scratch,
        "eval",
        "evaluation",
        BASE_MS + 6 * HOUR_MS,
        &recipe(LOSING),
    );
    let rejected = scratch.config(
        "portfolio_rejected.toml",
        &table(
            "drawdown_then_profit",
            GATES,
            &evaluation_toml(BASE_MS + 6 * HOUR_MS, &losing),
        ),
    );
    let (lines, manifest, rejected_selection) = optimize(&scratch, &rejected).unwrap();
    assert!(lines[0].contains(" state outer_rejected "), "{}", lines[0]);
    let expected = joint(&recipe(LOSING), &[up, down], 2);
    let outer = rejected_selection.outer.as_ref().unwrap();
    assert_eq!(outer.projection.profit, Some(cents(expected.0)));
    assert!(
        matches!(&rejected_selection.state, State::OuterRejected { reason } if reason.contains("below the minimum 0")),
        "{:?}",
        rejected_selection.state
    );
    assert_eq!(rejected_selection.selected, Some(sync_selected));
    assert!(rejected_selection.frozen.is_some());
    assert_eq!(verify(&manifest).unwrap(), lines[1]);
}

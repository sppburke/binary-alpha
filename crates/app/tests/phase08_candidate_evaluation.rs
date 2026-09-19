//! Phase 08: candidate search through the real command line on a known-truth tick fixture, then
//! the same command on the governed development replay under both backends (ignored; needs the
//! NVIDIA runner).

mod common;

use std::fs;
use std::path::{Path, PathBuf};

use binary_alpha_engine::execution::{Decimal, Summary};
use binary_alpha_engine::market::format_event_time_micros;
use binary_alpha_engine::search::{Family, FamilyManifest, StabilityOutcome, family_generation_id};
use common::current::import;
use common::{Scratch, command, generation, read_table, verify, write_ticks};
use serde_json::Value;

const BASE_DEV_MS: i64 = 1_767_571_200_000; // 2026-01-05T00:00:00Z, a Monday
const BASE_EVAL_MS: i64 = BASE_DEV_MS + 7_200_000; // two hours later
const CANDLE_MS: i64 = 20_000;
const BASIS: i64 = 1_800_000;
const ROWS: usize = 64;

const TICK_INSTRUMENT: &str = "\n[[instruments]]\nbroker = \"pocket_option\"\nprovider_symbol = \"AEDCNY_otc\"\nbase_currency = \"AED\"\nquote_currency = \"CNY\"\nprice_scale = 6\nsession = { kind = \"always\" }\nnative_granularity = { kind = \"tick\" }\ngap = { max_seconds = 2, reopen_seconds = 60 }\nfrozen = { min_observations = 10, min_seconds = 5 }\njump = { min_basis_points = 5 }\nspan = { min_percent = 75 }\nsessions = [{ name = \"week\", open_seconds = 0, close_seconds = 604800 }]\ncandles = [{ duration_seconds = 20, offset_seconds = 0, min_observations = 9, hard_min_observations = 5 }]\n";

/// One decision row's truth: the candle's direction bit, its range bit, and the outcome of the
/// decision taken at its close.
#[derive(Clone, Copy)]
struct Row {
    up: bool,
    wide: bool,
    outcome_up: bool,
}

/// Sixteen rows per feature-bit pair, interleaved by pair. The balanced control gives every
/// pair eight up and eight down outcomes; the planted control gives equal-bit pairs twelve up
/// and unequal-bit pairs four up.
fn recipe(planted: bool) -> Vec<Row> {
    (0..ROWS)
        .map(|k| {
            let (up, wide) = (k % 4 >= 2, k % 2 == 1);
            let position = k / 4;
            let ups = if !planted {
                8
            } else if up == wide {
                12
            } else {
                4
            };
            Row {
                up,
                wide,
                outcome_up: position < ups,
            }
        })
        .collect()
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

/// The frozen ordered tick recipe: each candle opens at the basis (the preceding decision's
/// entry), encodes that decision's outcome five seconds later, then its own range and direction
/// with later extrema and its final tick. One tail candle settles the last row.
fn ticks(base_ms: i64, rows: &[Row]) -> Vec<String> {
    let mut lines = Vec::new();
    for k in 0..=rows.len() {
        let start = base_ms + k as i64 * CANDLE_MS;
        let previous = k.checked_sub(1).map(|i| rows[i]);
        let current = rows.get(k).copied();
        for step in 0..(CANDLE_MS / 250) {
            let at = start + step * 250;
            let price = match (step, current) {
                (0, _) => BASIS,
                (20, _) => {
                    BASIS
                        + if previous.is_none_or(|row| row.outcome_up) {
                            2
                        } else {
                            -2
                        }
                }
                (32, Some(row)) => BASIS + if row.wide { 12 } else { 4 },
                (48, Some(row)) => BASIS - if row.wide { 12 } else { 4 },
                (79, Some(row)) => BASIS + if row.up { 1 } else { -1 },
                _ => BASIS + if step % 2 == 0 { 1 } else { -1 },
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

/// The published generations of one role: tick, profile, feature, and (development) outcome.
struct Role {
    tick: PathBuf,
    feature: PathBuf,
    outcome: Option<PathBuf>,
}

fn publish_role(
    scratch: &Scratch,
    name: &str,
    role: &str,
    base_ms: i64,
    rows: &[Row],
    frozen: Option<(&Path, &Path)>,
) -> Role {
    publish_role_with(
        scratch,
        name,
        role,
        base_ms,
        rows,
        frozen,
        "[\"candle_direction\", \"range_bps\"]",
    )
}

/// `publish_role` with an explicit compiled-output list for a new development plan.
fn publish_role_with(
    scratch: &Scratch,
    name: &str,
    role: &str,
    base_ms: i64,
    rows: &[Row],
    frozen: Option<(&Path, &Path)>,
    outputs: &str,
) -> Role {
    let lines = ticks(base_ms, rows);
    write_ticks(
        &scratch.path(&format!("sources/{name}/ticks.csv")),
        &lines.iter().map(String::as_str).collect::<Vec<_>>(),
    );
    let published = |line: String| {
        scratch.path(&format!(
            "published/manifests/{}/ready.json",
            generation(&line)
        ))
    };
    let source = scratch
        .tick_source()
        .replace(
            "sources/ticks/ticks.csv",
            &format!("sources/{name}/ticks.csv"),
        )
        .replace("\"development\"", &format!("\"{role}\""));
    let tick = published(
        import(&scratch.config(&format!("import_{name}.toml"), &source))
            .unwrap()
            .remove(0),
    );
    let (profile, frozen_entry) = match frozen {
        None => {
            let audit = scratch.config(&format!("audit_{name}.toml"), TICK_INSTRUMENT);
            let profile = published(
                command(&[
                    "data",
                    "audit",
                    "--config",
                    audit.to_str().unwrap(),
                    "--manifest",
                    &manifest_uri(&tick),
                ])
                .unwrap()
                .remove(0),
            );
            (profile, String::new())
        }
        Some((profile, plan)) => (
            profile.to_path_buf(),
            format!("frozen_plan = \"{}\"\n", manifest_uri(plan)),
        ),
    };
    let settings = if frozen.is_none() {
        format!(
            "streams = [{{ duration_seconds = 20, offset_seconds = 0 }}]\noutputs = {outputs}\n"
        )
    } else {
        String::new()
    };
    let features = scratch.config(
        &format!("features_{name}.toml"),
        &format!(
            "\n[[features.instruments]]\nrole = \"{role}\"\ninput_manifest = \"{}\"\nprofile_manifest = \"{}\"\n{frozen_entry}{settings}",
            manifest_uri(&tick),
            manifest_uri(&profile)
        ),
    );
    let feature = published(
        command(&["features", "build", "--config", features.to_str().unwrap()])
            .unwrap()
            .remove(0),
    );
    let outcome = (role == "development").then(|| {
        let outcomes = scratch.config(
            "outcomes.toml",
            &format!(
                "\n[outcomes]\nrole = \"development\"\ntick_manifest = \"{}\"\nfeature_manifest = \"{}\"\nexpiry_seconds = [5]\nmax_entry_delay_ms = 2000\nmax_settlement_delay_ms = 2000\nmax_tick_gap_ms = 2000\ntrue_jump_max_gap_ms = 2000\ntrue_jump_basis_points = \"5\"\nfrozen_min_ticks = 10\nfrozen_min_ms = 5000\n",
                manifest_uri(&tick),
                manifest_uri(&feature)
            ),
        );
        published(
            command(&["outcomes", "build", "--config", outcomes.to_str().unwrap()])
                .unwrap()
                .remove(0),
        )
    });
    let _ = profile;
    Role {
        tick,
        feature,
        outcome,
    }
}

fn contract(id: &str, direction: &str, entry_fee: &str, tie: &str) -> String {
    format!(
        "\n[[search.contracts]]\nid = \"{id}\"\ndirection = \"{direction}\"\nduration_micros = 5000000\ncurrency = \"unit\"\nstake = \"1\"\nquoted_cost = \"1\"\nentry_fee = \"{entry_fee}\"\nwin = {{ gross_return = \"1.80\", terminal_fee = \"0\" }}\nloss = {{ gross_return = \"0\", terminal_fee = \"0\" }}\ntie = {{ gross_return = \"{tie}\", terminal_fee = \"0\" }}\nsettlement = {{ rule = \"price_at_due_v1\", max_settlement_delay_micros = 2000000, max_tick_gap_micros = 2000000 }}\n"
    )
}

/// The search table of the fixture. `extra` appends contracts or replaces defaults.
struct SearchSpec<'a> {
    development: &'a Role,
    evaluation: Option<&'a Role>,
    scope: &'a str,
    screen: &'a str,
    horizon: u32,
    extra_contracts: &'a str,
    policy_extra: &'a str,
    chunk_size: u32,
}

fn search_table(spec: &SearchSpec<'_>) -> String {
    let window = |base: i64, rows: usize| {
        format!(
            "decision_start = \"{}\"\ndecision_end = \"{}\"\n",
            time(base + CANDLE_MS),
            time(base + (rows as i64 + 1) * CANDLE_MS)
        )
    };
    let development = format!(
        "\n[search.development]\n{}inputs = [{{ tick_manifest = \"{}\", feature_manifest = \"{}\", outcome_manifest = \"{}\" }}]\n",
        window(BASE_DEV_MS, ROWS),
        manifest_uri(&spec.development.tick),
        manifest_uri(&spec.development.feature),
        manifest_uri(spec.development.outcome.as_ref().unwrap())
    );
    let evaluation = spec.evaluation.map_or(String::new(), |role| {
        let boundary = BASE_EVAL_MS + 33 * CANDLE_MS + 2_000;
        format!(
            "\n[search.evaluation]\n{}inputs = [{{ tick_manifest = \"{}\", feature_manifest = \"{}\" }}]\nsplits = [{{ name = \"a\", start = \"{}\", end = \"{}\" }}, {{ name = \"b\", start = \"{}\", end = \"{}\" }}]\n",
            window(BASE_EVAL_MS, ROWS),
            manifest_uri(&role.tick),
            manifest_uri(&role.feature),
            time(BASE_EVAL_MS + CANDLE_MS),
            time(boundary),
            time(boundary),
            time(BASE_EVAL_MS + (ROWS as i64 + 1) * CANDLE_MS)
        )
    });
    format!(
        "\n[search]\nscope = \"{}\"\nseed = 7\nchunk_size = {}\nmax_candidates = 1000\nmin_conditions = 1\nmax_conditions = 2\nembargo_micros = 3600000000\nbase_stream = {{ duration_seconds = 20, offset_seconds = 0 }}\n{development}{evaluation}\n[[search.conditions]]\nstream = {{ duration_seconds = 20, offset_seconds = 0 }}\noutput = \"candle_direction\"\ncomparator = \"eq\"\nthresholds = [\"up\", \"down\"]\n\n[[search.conditions]]\nstream = {{ duration_seconds = 20, offset_seconds = 0 }}\noutput = \"range_bps\"\ncomparator = \"lt\"\nthresholds = [0.08]\n\n[[search.conditions]]\nstream = {{ duration_seconds = 20, offset_seconds = 0 }}\noutput = \"range_bps\"\ncomparator = \"gt\"\nthresholds = [0.08]\n{}{}{}\n[search.account]\nbroker = \"pocket_option\"\ncurrency = \"unit\"\nscale = 2\ninitial_cash = \"1000\"\n\n[search.risk_policy]\nid = \"one\"\nmax_open_per_strategy = 1\nsame_entry = \"all\"\ndeduplicate_signal_logic = false\nmax_feature_age_micros = 60000000\nmax_quote_age_micros = 0\n{}\n[search.envelope]\nmax_purchase_cost = \"1\"\nmax_entry_fee = \"0.10\"\nmax_win_terminal_fee = \"0\"\nmax_loss_terminal_fee = \"0\"\nmax_tie_terminal_fee = \"0\"\nmin_winning_net_return = \"0.70\"\nsettlement_rule = \"price_at_due_v1\"\n\n[search.gates]\nmin_settled = 1\nmax_unresolved = 0\nmin_net_profit = \"0\"\n{}\n[search.stability]\nblock_length = 4\nsimulations = 64\nrolling_horizon = {}\n",
        spec.scope,
        spec.chunk_size,
        contract("buy", "buy", "0", "1"),
        contract("sell", "sell", "0", "1"),
        spec.extra_contracts,
        spec.policy_extra,
        spec.screen,
        spec.horizon
    )
}

/// Runs `search` and returns its report lines, the manifest path, and the published family.
fn run_search(scratch: &Scratch, name: &str, table: &str) -> (Vec<String>, PathBuf, Family) {
    let config = scratch.config(name, table);
    let lines = command(&["search", "--config", config.to_str().unwrap()]).unwrap();
    assert_eq!(lines.len(), 2, "{lines:?}");
    assert!(lines[0].starts_with("search "), "{}", lines[0]);
    assert!(
        lines[1].starts_with("verified search generation "),
        "{}",
        lines[1]
    );
    let manifest = scratch.path(&format!(
        "published/manifests/{}/ready.json",
        generation(&lines[0])
    ));
    (lines, manifest.clone(), family(scratch, &manifest))
}

fn family(scratch: &Scratch, manifest: &Path) -> Family {
    let manifest = FamilyManifest::from_json(&fs::read(manifest).unwrap()).unwrap();
    Family::from_json(&fs::read(scratch.path("published").join(&manifest.objects[0].key)).unwrap())
        .unwrap()
}

fn decimal(text: &str) -> Decimal {
    Decimal::parse(text).unwrap()
}

fn profit(family: &Family, index: usize) -> Decimal {
    family.members[index].development.as_ref().unwrap().profit["unit"].unwrap()
}

/// The member index of one logic (condition threshold texts) and contract.
fn member(family: &Family, thresholds: &[&str], contract: &str) -> usize {
    family
        .members
        .iter()
        .position(|member| {
            member.contract == contract
                && member.conditions.len() == thresholds.len()
                && thresholds.iter().all(|wanted| {
                    member
                        .conditions
                        .iter()
                        .any(|condition| match &condition.threshold {
                            binary_alpha_engine::execution::Threshold::Text(text) => text == wanted,
                            binary_alpha_engine::execution::Threshold::Number(_) => {
                                *wanted == format!("range_{}", condition.comparator)
                            }
                            _ => false,
                        })
                })
        })
        .unwrap_or_else(|| panic!("no member {thresholds:?} {contract}"))
}

#[test]
fn candidate_search_publishes_verifies_and_resumes() {
    let scratch = Scratch::new("phase08");
    // Balanced negative control: every populated conjunction loses 1.60, every singleton 3.20.
    let balanced = publish_role(
        &scratch,
        "development",
        "development",
        BASE_DEV_MS,
        &recipe(false),
        None,
    );
    let (names, rows) = read_table(
        &scratch
            .path("published")
            .join(feature_rows_key(&balanced.feature)),
    );
    let column = |name: &str| names.iter().position(|n| n == name).unwrap();
    assert_eq!(
        rows.len(),
        ROWS,
        "one decision row per candle before the tail"
    );
    let expected = recipe(false);
    for (k, row) in rows.iter().enumerate() {
        let direction = match &row[column("candle_direction")] {
            Some(binary_alpha_engine::features::Value::Text(text)) => text.to_string(),
            other => panic!("row {k}: {other:?}"),
        };
        let range = match &row[column("range_bps")] {
            Some(binary_alpha_engine::features::Value::Float(value)) => *value,
            other => panic!("row {k}: {other:?}"),
        };
        assert_eq!(
            direction,
            if expected[k].up { "up" } else { "down" },
            "row {k}"
        );
        assert_eq!(range > 0.08, expected[k].wide, "row {k}: range {range}");
    }
    let spec = SearchSpec {
        development: &balanced,
        evaluation: None,
        scope: "exhaustive",
        screen: "",
        horizon: 4,
        extra_contracts: "",
        policy_extra: "",
        chunk_size: 8,
    };
    let (lines, manifest, family) =
        run_search(&scratch, "search_balanced.toml", &search_table(&spec));
    assert!(
        lines[0].contains(
            " members 20 applicable 20 screened 0 replayed 20 passed 0 evaluated 0 objects 1 ["
        ),
        "{}",
        lines[0]
    );
    assert_eq!(family.members.len(), 20);
    assert_eq!(
        family.chunks.len(),
        3,
        "eight members per development chunk"
    );
    for (index, member) in family.members.iter().enumerate() {
        let group = member.development.as_ref().expect("replayed");
        let expected = match member.conditions.len() {
            1 => Some("-3.20"),
            2 if group.signals == 0 => None,
            _ => Some("-1.60"),
        };
        match expected {
            Some(text) => {
                assert_eq!(profit(&family, index), decimal(text), "member {index}");
                assert_eq!(
                    group.settled,
                    if member.conditions.len() == 1 { 32 } else { 16 }
                );
                assert_eq!(
                    member.raw.wins + member.raw.losses,
                    group.settled as i64,
                    "member {index}"
                );
            }
            None => assert_eq!(
                member.rejected.as_deref(),
                Some("settled 0 below the minimum 1")
            ),
        }
        assert!(
            member.rejected.is_some(),
            "member {index} passed the negative control"
        );
        assert_eq!(member.rank, None);
        assert!(member.stability.is_empty());
        let null = member.null.as_ref().unwrap();
        assert_eq!(
            (null.net_win.to_string(), null.net_loss.to_string()),
            ("0.80".into(), "1".into())
        );
        assert!((null.break_even - 1.0 / 1.8).abs() < 1e-15);
    }
    // Balanced: 16 decisive trials at 8/8 give the same score for every populated conjunction.
    let up_narrow_buy = member(&family, &["up", "range_lt"], "buy");
    assert_eq!(family.members[up_narrow_buy].raw.wins, 8);
    assert_eq!(family.members[up_narrow_buy].raw.losses, 8);
    assert!(family.members[up_narrow_buy].score.unwrap() > 0.3);
    let empty = member(&family, &["up", "down"], "buy");
    assert_eq!(family.members[empty].score, Some(1.0));
    assert_eq!(family.members[empty].adjusted, Some(1.0));
    assert_eq!(verify(&manifest).unwrap(), lines[1]);

    // Planted interaction: the four correctly directed conjunctions earn 5.60 and pass; every
    // singleton still loses 3.20. Evaluation follows the frozen ranking across two splits.
    let scratch = Scratch::new("phase08_planted");
    let planted = publish_role(
        &scratch,
        "development",
        "development",
        BASE_DEV_MS,
        &recipe(true),
        None,
    );
    let profile = scratch.path(&format!(
        "published/manifests/{}/ready.json",
        profile_generation(&scratch, &planted)
    ));
    let evaluation = publish_role(
        &scratch,
        "evaluation",
        "evaluation",
        BASE_EVAL_MS,
        &recipe(true),
        Some((&profile, &planted.feature)),
    );
    // An interrupted run: the evaluation input applies another development plan, so the search
    // fails after the development chunks are published; the corrected run reuses every one of
    // them and adds exactly one evaluation chunk and the family.
    let other = publish_role_with(
        &scratch,
        "development_other",
        "development",
        BASE_DEV_MS,
        &recipe(true),
        None,
        "[\"candle_direction\"]",
    );
    let fitted = publish_role(
        &scratch,
        "evaluation_fitted",
        "evaluation",
        BASE_EVAL_MS,
        &recipe(true),
        Some((&profile, &other.feature)),
    );
    let interrupted = SearchSpec {
        development: &planted,
        evaluation: Some(&fitted),
        scope: "exhaustive",
        screen: "",
        horizon: 4,
        extra_contracts: &format!(
            "{}{}",
            contract("fee_buy", "buy", "0.10", "1.10"),
            contract("odd_tie_buy", "buy", "0", "0.95")
        ),
        policy_extra: "",
        chunk_size: 8,
    };
    let wrong = scratch.config("search_interrupted.toml", &search_table(&interrupted));
    let error = command(&["search", "--config", wrong.to_str().unwrap()]).unwrap_err();
    assert!(
        error.contains("does not apply the development plan"),
        "{error}"
    );
    let after_interruption = scratch.manifests("published").len();
    let spec = SearchSpec {
        development: &planted,
        evaluation: Some(&evaluation),
        scope: "exhaustive",
        screen: "",
        horizon: 4,
        extra_contracts: &format!(
            "{}{}",
            contract("fee_buy", "buy", "0.10", "1.10"),
            contract("odd_tie_buy", "buy", "0", "0.95")
        ),
        policy_extra: "",
        chunk_size: 8,
    };
    let (lines, manifest, family) =
        run_search(&scratch, "search_planted.toml", &search_table(&spec));
    assert!(
        lines[0].contains(
            " members 40 applicable 30 screened 0 replayed 40 passed 8 evaluated 8 objects 1 ["
        ),
        "{}",
        lines[0]
    );
    assert_eq!(
        scratch.manifests("published").len(),
        after_interruption + 2,
        "the corrected run reuses the lowering and every development chunk"
    );
    let winners = [
        (member(&family, &["up", "range_gt"], "buy"), "buy"),
        (member(&family, &["down", "range_lt"], "buy"), "buy"),
        (member(&family, &["up", "range_lt"], "sell"), "sell"),
        (member(&family, &["down", "range_gt"], "sell"), "sell"),
    ];
    for (index, _) in winners {
        assert_eq!(profit(&family, index), decimal("5.60"), "member {index}");
        assert_eq!(family.members[index].rejected, None);
        assert!(
            family.members[index].rank.is_some_and(|rank| rank <= 6),
            "member {index}"
        );
        let evaluation = family.members[index].evaluation.as_ref().unwrap();
        assert_eq!(evaluation.settled, 16);
        let splits = &family.members[index].evaluation_splits;
        assert_eq!(splits.keys().cloned().collect::<Vec<_>>(), ["a", "b"]);
        assert_eq!(splits["a"].settled + splits["b"].settled, 16);
        assert_eq!(
            splits["a"].profit["unit"]
                .unwrap()
                .checked_add(splits["b"].profit["unit"].unwrap())
                .unwrap(),
            evaluation.profit["unit"].unwrap()
        );
        match &family.members[index].stability["development"] {
            StabilityOutcome::Available(stability) => {
                assert_eq!(
                    (
                        stability.trade_count,
                        stability.simulations,
                        stability.block_length,
                        stability.rolling_horizon
                    ),
                    (16, 64, 4, 4)
                );
                assert!(stability.p95_max_drawdown >= stability.median_max_drawdown);
                assert!((0.0..=1.0).contains(&stability.negative_rolling_share));
            }
            other => panic!("{other:?}"),
        }
        assert!(matches!(
            family.members[index].stability["evaluation"],
            StabilityOutcome::Available(_)
        ));
    }
    // The crossing settlement: the decision at the thirty-third close (row 32, a down/narrow
    // row) settles two seconds into split b but is attributed to split a, so the down/narrow
    // winner holds nine decisions and nine settlements in a and seven in b.
    let crossing = member(&family, &["down", "range_lt"], "buy");
    let splits = &family.members[crossing].evaluation_splits;
    assert_eq!(
        (
            splits["a"].signals,
            splits["a"].settled,
            splits["b"].signals,
            splits["b"].settled
        ),
        (9, 9, 7, 7)
    );
    let singleton_up = member(&family, &["up"], "buy");
    assert_eq!(profit(&family, singleton_up), decimal("-3.20"));
    assert_eq!(family.members[singleton_up].rank, None);
    assert_eq!(family.members[singleton_up].evaluation, None);
    assert!(family.members[singleton_up].evaluation_splits.is_empty());
    // The fee contract is applicable with W = 0.70, L = 1.10; the odd tie is inapplicable yet
    // fully replayed.
    let fee = member(&family, &["up", "range_gt"], "fee_buy");
    let null = family.members[fee].null.as_ref().unwrap();
    assert_eq!(
        (null.net_win.to_string(), null.net_loss.to_string()),
        ("0.70".into(), "1.10".into())
    );
    assert!((null.break_even - 11.0 / 18.0).abs() < 1e-15);
    assert_eq!(
        profit(&family, fee),
        decimal("4.00"),
        "12 * 0.70 - 4 * 1.10"
    );
    let odd = member(&family, &["up", "range_gt"], "odd_tie_buy");
    assert_eq!(
        family.members[odd].inapplicable.as_deref(),
        Some("a tie nets -0.05, not zero")
    );
    assert_eq!(family.members[odd].score, None);
    assert_eq!(family.members[odd].adjusted, None);
    assert_eq!(profit(&family, odd), decimal("5.60"));
    assert_eq!(family.members[odd].rejected, None);
    // Ranking: winners by profit, then settled, then canonical order; the fee member follows.
    let ranked: Vec<usize> = (1..=8)
        .map(|rank| {
            family
                .members
                .iter()
                .position(|m| m.rank == Some(rank))
                .unwrap()
        })
        .collect();
    assert_eq!(
        ranked,
        [21, 24, 27, 28, 31, 33, 26, 30],
        "ties keep member order"
    );
    assert!(
        ranked[..6]
            .iter()
            .all(|&i| profit(&family, i) == decimal("5.60"))
    );
    assert_eq!(profit(&family, ranked[6]), decimal("4.00"));
    assert_eq!(verify(&manifest).unwrap(), lines[1]);

    // One survivor's chunk group equals a standalone replay of that member alone.
    let standalone = scratch.config(
        "standalone.toml",
        &standalone_replay(&planted, &family, winners[0].0),
    );
    let replayed = command(&["replay", "--config", standalone.to_str().unwrap()]).unwrap();
    let replay_manifest: Value = serde_json::from_slice(
        &fs::read(scratch.path(&format!(
            "published/manifests/{}/ready.json",
            generation(&replayed[0])
        )))
        .unwrap(),
    )
    .unwrap();
    let summary_key = replay_manifest["objects"]
        .as_array()
        .unwrap()
        .iter()
        .find(|object| object["path"] == "summary.json")
        .unwrap()["key"]
        .as_str()
        .unwrap()
        .to_string();
    let summary =
        Summary::from_json(&fs::read(scratch.path("published").join(summary_key)).unwrap())
            .unwrap();
    assert_eq!(
        summary.strategies["solo"],
        *family.members[winners[0].0].development.as_ref().unwrap()
    );

    // A cross-account risk scope is rejected before any manifest is read.
    let contested = scratch.config(
        "contested.toml",
        &search_table(&SearchSpec {
            policy_extra: "max_open_total = 5\n",
            ..spec
        }),
    );
    let error = command(&[
        "config",
        "validate",
        "--config",
        contested.to_str().unwrap(),
    ])
    .unwrap_err();
    assert!(
        error.contains("search.risk_policy.max_open_total"),
        "{error}"
    );

    // Heuristic scope keeps the eliminated members, replays only survivors, and reports
    // unavailable stability when the horizon exceeds the settlements.
    let (lines, heuristic_manifest, heuristic) = run_search(
        &scratch,
        "search_heuristic.toml",
        &search_table(&SearchSpec {
            scope: "heuristic",
            screen: "\n[search.screen]\nmax_adjusted_score = 0.7\ntop = 12\n",
            horizon: 20,
            ..spec
        }),
    );
    assert!(
        lines[0].contains(
            " members 40 applicable 30 screened 36 replayed 4 passed 4 evaluated 4 objects 1 ["
        ),
        "{}",
        lines[0]
    );
    let screened: Vec<&binary_alpha_engine::search::Member> = heuristic
        .members
        .iter()
        .filter(|m| m.screened.is_some())
        .collect();
    assert_eq!(screened.len(), 36);
    assert!(
        screened
            .iter()
            .all(|m| m.development.is_none() && m.rank.is_none() && m.stability.is_empty())
    );
    assert!(
        heuristic
            .members
            .iter()
            .filter(|m| m.inapplicable.is_some())
            .all(|m| m.screened.as_deref().unwrap().starts_with("inapplicable: "))
    );
    let survivors: Vec<usize> = (0..heuristic.members.len())
        .filter(|&i| heuristic.members[i].screened.is_none())
        .collect();
    assert_eq!(
        survivors,
        winners
            .iter()
            .map(|(index, _)| *index)
            .collect::<Vec<_>>()
            .iter()
            .copied()
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>()
    );
    for &index in &survivors {
        let member = &heuristic.members[index];
        assert!(
            (0.69..=0.70).contains(&member.adjusted.unwrap()),
            "{:?}",
            member.adjusted
        );
        assert_eq!(member.rejected, None);
        assert_eq!(
            member.stability["development"],
            StabilityOutcome::Unavailable {
                reason: "rolling horizon 20 exceeds 16 settlements".into()
            }
        );
    }
    assert_eq!(verify(&heuristic_manifest).unwrap(), lines[1]);

    // Altered evaluation ticks change only evaluation fields; the development result, its
    // scores, gates and ranks are identical.
    let altered = publish_role(
        &scratch,
        "evaluation_altered",
        "evaluation",
        BASE_EVAL_MS,
        &recipe(false),
        Some((&profile, &planted.feature)),
    );
    let (_, _, changed) = run_search(
        &scratch,
        "search_altered.toml",
        &search_table(&SearchSpec {
            evaluation: Some(&altered),
            ..spec
        }),
    );
    for (before, after) in family.members.iter().zip(&changed.members) {
        assert_eq!(
            (
                &before.conditions,
                &before.contract,
                &before.raw,
                &before.score,
                &before.adjusted
            ),
            (
                &after.conditions,
                &after.contract,
                &after.raw,
                &after.score,
                &after.adjusted
            )
        );
        assert_eq!(
            (&before.development, &before.rejected, &before.rank),
            (&after.development, &after.rejected, &after.rank)
        );
        assert_eq!(
            before.stability.get("development"),
            after.stability.get("development")
        );
    }
    assert_ne!(
        family.members[winners[0].0].evaluation,
        changed.members[winners[0].0].evaluation
    );

    // Resume: with the family manifest removed, the rerun reuses every chunk generation and
    // republishes byte-identical family and manifest.
    let manifest_before = fs::read(&heuristic_manifest).unwrap();
    let family_bytes = fs::read(
        scratch
            .path("published")
            .join(&FamilyManifest::from_json(&manifest_before).unwrap().objects[0].key),
    )
    .unwrap();
    let manifests_before = scratch.manifests("published").len();
    fs::remove_file(&heuristic_manifest).unwrap();
    let config = scratch.path("search_heuristic.toml");
    let rerun = command(&["search", "--config", config.to_str().unwrap()]).unwrap();
    assert!(rerun[0].contains(" objects 1 ["), "{}", rerun[0]);
    assert_eq!(fs::read(&heuristic_manifest).unwrap(), manifest_before);
    assert_eq!(scratch.manifests("published").len(), manifests_before);
    let published = FamilyManifest::from_json(&manifest_before).unwrap();
    assert_eq!(
        fs::read(scratch.path("published").join(&published.objects[0].key)).unwrap(),
        family_bytes
    );
    let again = command(&["search", "--config", config.to_str().unwrap()]).unwrap();
    assert!(again[0].ends_with("(already published)"), "{}", again[0]);

    // Verification rejects a manifest whose family disagrees with its replay, and a member count
    // that disagrees with the family.
    let manifest_path = heuristic_manifest.clone();
    let mut forged: Value = serde_json::from_slice(&family_bytes).unwrap();
    let index = forged["members"]
        .as_array()
        .unwrap()
        .iter()
        .position(|m| m["development"].is_object())
        .unwrap();
    forged["members"][index]["development"]["wins"] = Value::from(999);
    let forged = serde_json::to_vec_pretty(&forged).unwrap();
    let sha = sha256_hex(&forged);
    fs::write(scratch.path(&format!("published/objects/{sha}")), &forged).unwrap();
    let mut manifest_json: Value = serde_json::from_slice(&manifest_before).unwrap();
    manifest_json["objects"][0]["key"] = Value::from(format!("objects/{sha}"));
    manifest_json["objects"][0]["sha256"] = Value::from(sha.clone());
    manifest_json["objects"][0]["bytes"] = Value::from(forged.len());
    fs::write(
        &manifest_path,
        serde_json::to_vec_pretty(&manifest_json).unwrap(),
    )
    .unwrap();
    let error = verify(&manifest_path).unwrap_err();
    assert!(
        error.contains("records development groups its replay"),
        "{error}"
    );
    let mut short: Value = serde_json::from_slice(&manifest_before).unwrap();
    short["members"] = Value::from(39);
    fs::write(&manifest_path, serde_json::to_vec_pretty(&short).unwrap()).unwrap();
    let error = verify(&manifest_path).unwrap_err();
    assert!(
        error.contains("records 39 members but the family holds 40"),
        "{error}"
    );
    // Forged provenance: an input identity that is not the bound generation, republished at the
    // key its recomputed generation names, fails against the bound state.
    let mut forged_manifest = FamilyManifest::from_json(&manifest_before).unwrap();
    forged_manifest.inputs[0].tick_generation = "0".repeat(64);
    forged_manifest.generation = family_generation_id(
        &forged_manifest.config_hash,
        &forged_manifest.code_revision,
        &forged_manifest.inputs,
    );
    let forged_path = scratch.path(&format!(
        "published/manifests/{}/ready.json",
        forged_manifest.generation
    ));
    fs::create_dir_all(forged_path.parent().unwrap()).unwrap();
    fs::write(&forged_path, forged_manifest.to_json()).unwrap();
    let error = verify(&forged_path).unwrap_err();
    assert!(error.contains("not the bound generations"), "{error}");
    // A screened member with a fabricated development group is not verified evidence.
    let mut fabricated: Value = serde_json::from_slice(&family_bytes).unwrap();
    let screened = fabricated["members"]
        .as_array()
        .unwrap()
        .iter()
        .position(|m| m["screened"].is_string())
        .unwrap();
    fabricated["members"][screened]["development"] =
        fabricated["members"][index]["development"].clone();
    let fabricated = serde_json::to_vec_pretty(&fabricated).unwrap();
    let sha = sha256_hex(&fabricated);
    fs::write(
        scratch.path(&format!("published/objects/{sha}")),
        &fabricated,
    )
    .unwrap();
    let mut manifest_json: Value = serde_json::from_slice(&manifest_before).unwrap();
    manifest_json["objects"][0]["key"] = Value::from(format!("objects/{sha}"));
    manifest_json["objects"][0]["sha256"] = Value::from(sha);
    manifest_json["objects"][0]["bytes"] = Value::from(fabricated.len());
    fs::write(
        &manifest_path,
        serde_json::to_vec_pretty(&manifest_json).unwrap(),
    )
    .unwrap();
    let error = verify(&manifest_path).unwrap_err();
    assert!(
        error.contains("is not replayed and resampled exactly as its status requires"),
        "{error}"
    );
    // A family whose chunks vanished cannot verify even though every recorded field is intact.
    let mut missing: Value = serde_json::from_slice(&family_bytes).unwrap();
    missing["chunks"] = Value::Array(Vec::new());
    let missing = serde_json::to_vec_pretty(&missing).unwrap();
    let sha = sha256_hex(&missing);
    fs::write(scratch.path(&format!("published/objects/{sha}")), &missing).unwrap();
    let mut manifest_json: Value = serde_json::from_slice(&manifest_before).unwrap();
    manifest_json["objects"][0]["key"] = Value::from(format!("objects/{sha}"));
    manifest_json["objects"][0]["sha256"] = Value::from(sha);
    manifest_json["objects"][0]["bytes"] = Value::from(missing.len());
    fs::write(
        &manifest_path,
        serde_json::to_vec_pretty(&manifest_json).unwrap(),
    )
    .unwrap();
    let error = verify(&manifest_path).unwrap_err();
    assert!(
        error.contains("is not replayed and resampled exactly as its status requires"),
        "{error}"
    );
    fs::write(&manifest_path, &manifest_before).unwrap();
    assert!(verify(&manifest_path).is_ok());
}

fn feature_rows_key(manifest: &Path) -> String {
    let manifest: Value = serde_json::from_slice(&fs::read(manifest).unwrap()).unwrap();
    manifest["objects"]
        .as_array()
        .unwrap()
        .iter()
        .find(|object| object["path"] == "rows/20s_0s.parquet")
        .unwrap()["key"]
        .as_str()
        .unwrap()
        .to_string()
}

fn profile_generation(scratch: &Scratch, role: &Role) -> String {
    let feature: Value = serde_json::from_slice(&fs::read(&role.feature).unwrap()).unwrap();
    let _ = scratch;
    feature["profile_generation"].as_str().unwrap().to_string()
}

fn standalone_replay(role: &Role, family: &Family, index: usize) -> String {
    let member = &family.members[index];
    let conditions: Vec<String> = member
        .conditions
        .iter()
        .map(|condition| {
            let threshold = match &condition.threshold {
                binary_alpha_engine::execution::Threshold::Text(text) => format!("\"{text}\""),
                binary_alpha_engine::execution::Threshold::Number(value) => value.to_string(),
                binary_alpha_engine::execution::Threshold::Bool(value) => value.to_string(),
            };
            format!(
                "{{ stream = {{ duration_seconds = 20, offset_seconds = 0 }}, output = \"{}\", comparator = \"{}\", threshold = {threshold} }}",
                condition.output, condition.comparator
            )
        })
        .collect();
    let contract = family
        .search
        .contracts
        .iter()
        .find(|contract| contract.id == member.contract)
        .unwrap();
    format!(
        "\n[replay]\nrole = \"development\"\ndecision_start = \"{}\"\ndecision_end = \"{}\"\ninputs = [{{ tick_manifest = \"{}\", feature_manifest = \"{}\" }}]\naccounts = [{{ id = \"solo\", broker = \"pocket_option\", currency = \"unit\", scale = 2, initial_cash = \"1000\" }}]\nreporting_currency = \"unit\"\nreporting_scale = 2\nmax_rate_age_micros = 0\n\n[[replay.strategies]]\nid = \"solo\"\nplan_identity = \"{}\"\nbase_stream = {{ duration_seconds = 20, offset_seconds = 0 }}\nconditions = [{}]\n\n[[replay.contracts]]\nid = \"{}\"\ndirection = \"{}\"\nduration_micros = 5000000\ncurrency = \"unit\"\nstake = \"1\"\nquoted_cost = \"1\"\nentry_fee = \"{}\"\nwin = {{ gross_return = \"{}\", terminal_fee = \"0\" }}\nloss = {{ gross_return = \"0\", terminal_fee = \"0\" }}\ntie = {{ gross_return = \"{}\", terminal_fee = \"0\" }}\nsettlement = {{ rule = \"price_at_due_v1\", max_settlement_delay_micros = 2000000, max_tick_gap_micros = 2000000 }}\n\n[[replay.risk_policies]]\nid = \"one\"\nmax_open_per_strategy = 1\nsame_entry = \"all\"\ndeduplicate_signal_logic = false\nmax_feature_age_micros = 60000000\nmax_quote_age_micros = 0\n\n[[replay.bindings]]\nid = \"solo\"\nstrategy = \"solo\"\naccount = \"solo\"\ninstrument = \"pocket_option:AEDCNY_otc\"\ncontract = \"{}\"\nrisk_policy = \"one\"\nenvelope = {{ max_purchase_cost = \"1\", max_entry_fee = \"0.10\", max_win_terminal_fee = \"0\", max_loss_terminal_fee = \"0\", max_tie_terminal_fee = \"0\", min_winning_net_return = \"0.70\", settlement_rule = \"price_at_due_v1\" }}\n",
        time(BASE_DEV_MS + CANDLE_MS),
        time(BASE_DEV_MS + (ROWS as i64 + 1) * CANDLE_MS),
        manifest_uri(&role.tick),
        manifest_uri(&role.feature),
        family.plan_identity,
        conditions.join(", "),
        contract.id,
        contract.direction,
        contract.entry_fee,
        contract.win.gross_return,
        contract.tie.gross_return,
        contract.id
    )
}

fn sha256_hex(bytes: &[u8]) -> String {
    use sha2::Digest;
    binary_alpha_engine::hex(&sha2::Sha256::digest(bytes))
}

/// The governed proof: the wrapper's development replay becomes a heuristic search whose menu is
/// its strategies' conditions and whose contracts, account, envelope and policy (without the
/// four cross-account scopes) are its own; the family published under `cpu` and under `cuda`
/// must be byte-identical, at least one member must pass with available stability, and the
/// rerun must reuse the publication.
#[test]
#[ignore = "needs BINARY_ALPHA_TEST_CONFIG naming the governed wrapper and an NVIDIA device"]
#[cfg(feature = "cuda")]
fn governed_candidate_evaluation() {
    use binary_alpha_engine::config::{
        Accelerator, Backend, Config, ConfigPath, Scope, Screen, Search, SearchAccount,
        SearchCondition, SearchWindow, StabilitySettings,
    };
    use binary_alpha_engine::search::Gates;
    use common::{manifest_json, timed};

    let wrapper = manifest_json(Path::new(
        &std::env::var("BINARY_ALPHA_TEST_CONFIG")
            .expect("BINARY_ALPHA_TEST_CONFIG names the governed wrapper"),
    ));
    let source = fs::read_to_string(wrapper["application_config"].as_str().unwrap()).unwrap();
    let governed = Config::parse(&source).unwrap();
    let replay = governed
        .replay
        .clone()
        .expect("the governed configuration has a replay");
    assert_eq!(replay.inputs.len(), 1);
    assert!(
        replay.inputs[0].outcome_manifest.is_some(),
        "the governed replay binds outcomes"
    );
    let mut menu: Vec<SearchCondition> = Vec::new();
    for strategy in &replay.strategies {
        assert_eq!(
            strategy.conditions.len(),
            1,
            "governed strategies have one condition"
        );
        let condition = &strategy.conditions[0];
        match menu.iter_mut().find(|entry| {
            entry.stream == condition.stream
                && entry.output == condition.output
                && entry.comparator == condition.comparator
        }) {
            Some(entry) => {
                if !entry.thresholds.contains(&condition.threshold) {
                    entry.thresholds.push(condition.threshold.clone());
                }
            }
            None => menu.push(SearchCondition {
                stream: condition.stream,
                output: condition.output.clone(),
                comparator: condition.comparator,
                thresholds: vec![condition.threshold.clone()],
            }),
        }
    }
    let mut policy = replay.risk_policies[0].clone();
    policy.max_open_per_duration = None;
    policy.max_open_per_instrument = None;
    policy.max_open_total = None;
    policy.max_unresolved_loss_total = None;
    let account = &replay.accounts[0];
    let embargo = replay
        .contracts
        .iter()
        .map(|contract| contract.duration_micros + contract.settlement.max_settlement_delay_micros)
        .max()
        .unwrap();
    let search = Search {
        scope: Scope::Heuristic,
        seed: 0,
        chunk_size: 64,
        max_candidates: 10_000,
        min_conditions: 1,
        max_conditions: 1,
        embargo_micros: embargo,
        base_stream: replay.strategies[0].base_stream,
        development: SearchWindow {
            decision_start: replay.decision_start.clone(),
            decision_end: replay.decision_end.clone(),
            inputs: replay.inputs.clone(),
            splits: None,
        },
        evaluation: None,
        conditions: menu,
        contracts: replay.contracts.clone(),
        account: SearchAccount {
            broker: account.broker.clone(),
            currency: account.currency.clone(),
            scale: account.scale,
            initial_cash: account.initial_cash,
        },
        risk_policy: policy,
        envelope: replay.bindings[0].envelope.clone(),
        gates: Gates {
            min_settled: 2,
            max_unresolved: 1_000_000,
            min_net_profit: decimal("-1000000"),
        },
        screen: Some(Screen {
            max_adjusted_score: 1.0,
            top: None,
        }),
        stability: StabilitySettings {
            block_length: 8,
            simulations: 256,
            rolling_horizon: 50,
        },
    };
    let scratch = Scratch::new("phase08_governed");
    let write = |backend: Backend| -> PathBuf {
        let mut config = governed.clone();
        config.replay = None;
        config.storage.historical_data_dir =
            ConfigPath::try_from(PathBuf::from("retained")).unwrap();
        config.storage.publication_uri = format!("file://{}", scratch.path("published").display())
            .parse()
            .unwrap();
        config.accelerator = Some(Accelerator { backend });
        config.search = Some(search.clone());
        let path = scratch.path(&format!("search_{backend}.toml"));
        fs::write(&path, config.canonical_toml()).unwrap();
        path
    };
    let run = |backend: Backend| {
        let path = write(backend);
        let (lines, wall, peak_kb) = timed(&["search", "--config", path.to_str().unwrap()]);
        println!(
            "governed {backend}: {} | wall {wall:.3}s peak_rss_kb {peak_kb}",
            lines[0]
        );
        assert!(
            lines[1].starts_with("verified search generation "),
            "{}",
            lines[1]
        );
        let manifest = scratch.path(&format!(
            "published/manifests/{}/ready.json",
            generation(&lines[0])
        ));
        let published = FamilyManifest::from_json(&fs::read(&manifest).unwrap()).unwrap();
        let bytes = fs::read(scratch.path("published").join(&published.objects[0].key)).unwrap();
        (lines, manifest, bytes)
    };
    let (cpu_lines, cpu_manifest, cpu_bytes) = run(Backend::Cpu);
    let (cuda_lines, cuda_manifest, cuda_bytes) = run(Backend::Cuda);
    assert_eq!(
        cpu_bytes, cuda_bytes,
        "the family bytes are backend-independent"
    );
    assert_ne!(
        cpu_manifest, cuda_manifest,
        "the backend changes only the configuration hash"
    );
    let family = Family::from_json(&cpu_bytes).unwrap();
    let passing: Vec<_> = family
        .members
        .iter()
        .filter(|member| member.rank.is_some())
        .collect();
    assert!(
        !passing.is_empty(),
        "at least one governed member passes the development gates"
    );
    assert!(
        passing.iter().any(|member| matches!(
            member.stability.get("development"),
            Some(StabilityOutcome::Available(_))
        )),
        "at least one passing member has available stability"
    );
    let rerun = command(&[
        "search",
        "--config",
        scratch.path("search_cuda.toml").to_str().unwrap(),
    ])
    .unwrap();
    assert!(rerun[0].ends_with("(already published)"), "{}", rerun[0]);
    assert_eq!(verify(&cuda_manifest).unwrap(), cuda_lines[1]);
    assert_eq!(verify(&cpu_manifest).unwrap(), cpu_lines[1]);
    println!(
        "governed family: {} members, {} applicable, {} passing, lowering {} chunks {}",
        family.members.len(),
        family.applicable,
        passing.len(),
        family.lowering.generation,
        family.chunks.len()
    );
}

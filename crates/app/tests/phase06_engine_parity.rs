//! `binary-alpha replay` and the engine: the deterministic publication, readback, restoration,
//! and byte-identical live-adapter proofs always; the integrated financial scenario suite
//! always; and the governed reference comparison when `BINARY_ALPHA_TEST_CONFIG` names it.

mod common;

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use std::collections::HashMap;

use binary_alpha_engine::config::{Config, Replay, StreamKey};
use binary_alpha_engine::dataset::GenerationManifest;
use binary_alpha_engine::execution::{
    AccountSpec, AccountState, Cashflow, ColumnSpec, Comparator, Condition, ContractTerms, Decimal,
    DeploymentBinding, Direction, Disposition, EVENTS_OBJECT_PATH, Engine, Envelope, EventKind,
    EventSource, FinancialEvent, HISTORICAL_AVAILABILITY, InstrumentBinding, Observation, Outcome,
    PathMetrics, Pause, REPLAY_SCHEMA_VERSION, RateEvent, ReplayInput, ReplayManifest, Resolution,
    RiskPolicy, RunDefinition, SUMMARY_OBJECT_PATH, SameEntry, SettlementRule, StrategySpec,
    StreamColumns, Summary, Threshold, UnresolvedReason, basis_points_text, project_fitted_label,
};
use binary_alpha_engine::features::{FeatureManifest, FittedEncoding, Kind, ProjectionKind, Value};
use binary_alpha_engine::market::{Currency, format_event_time_micros};
use common::current::import;
use common::*;

/// The rows of one stream: close time, availability, and the bound column values.
type StreamRows = Vec<(i64, i64, Vec<Option<Value>>)>;

fn manifest_uri(path: &Path) -> String {
    format!("file://{}", path.display())
}

/// One tick line at `millis` since the Unix epoch.
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

const TICK_INSTRUMENT: &str = "\n[[instruments]]\nbroker = \"pocket_option\"\nprovider_symbol = \"AEDCNY_otc\"\nbase_currency = \"AED\"\nquote_currency = \"CNY\"\nprice_scale = 6\nsession = { kind = \"always\" }\nnative_granularity = { kind = \"tick\" }\ngap = { max_seconds = 2, reopen_seconds = 60 }\nfrozen = { min_observations = 10, min_seconds = 5 }\njump = { min_basis_points = 5 }\nspan = { min_percent = 75 }\nsessions = [{ name = \"week\", open_seconds = 0, close_seconds = 604800 }]\ncandles = [{ duration_seconds = 5, offset_seconds = 0, min_observations = 9, hard_min_observations = 5 }, { duration_seconds = 15, offset_seconds = 5, min_observations = 29, hard_min_observations = 15 }]\n";

/// Twenty minutes of deterministic ticks four per second on a unit random walk, with one
/// seventy-second gap after the tenth minute, starting on a Monday.
fn synthetic_ticks() -> Vec<(i64, i64)> {
    const BASE: i64 = 1_767_571_200_000; // 2026-01-05T00:00:00Z
    let mut state: u64 = 0x2545_F491_4F6C_DD1D;
    let mut step = move || -> i64 {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        (state % 5) as i64 - 2
    };
    let mut price = 1_800_000_i64;
    let mut ticks = Vec::new();
    let mut offset = 0;
    while offset < 1_200_000 {
        if (600_000..670_000).contains(&offset) {
            offset += 250;
            continue;
        }
        price += step();
        ticks.push((BASE + offset, price));
        offset += 250;
    }
    ticks
}

fn feature_entry(input: &Path, profile: &Path) -> String {
    format!(
        "\n[[features.instruments]]\nrole = \"development\"\ninput_manifest = \"{}\"\nprofile_manifest = \"{}\"\nstreams = [{{ duration_seconds = 5, offset_seconds = 0 }}, {{ duration_seconds = 15, offset_seconds = 5 }}]\noutputs = [\"candle_direction\"]\n",
        manifest_uri(input),
        manifest_uri(profile)
    )
}

/// The replay table of the deterministic fixture: two ten-second bindings on the fifteen-second
/// stream, each also requiring the five-second stream's latest candle to agree, with the
/// explicit reference-style economics on one funded account.
fn replay_table(tick: &Path, feature: &Path, plan_identity: &str) -> String {
    format!(
        "\n[replay]\nrole = \"development\"\ndecision_start = \"2026-01-05T00:00:00Z\"\ndecision_end = \"2026-01-05T00:20:00Z\"\ninputs = [{{ tick_manifest = \"{}\", feature_manifest = \"{}\" }}]\nsplits = [{{ name = \"early\", start = \"2026-01-05T00:00:00Z\", end = \"2026-01-05T00:10:00Z\" }}, {{ name = \"late\", start = \"2026-01-05T00:10:00Z\", end = \"2026-01-05T00:20:00Z\" }}]\naccounts = [{{ id = \"sim\", broker = \"pocket_option\", currency = \"fixture_unit\", scale = 2, initial_cash = \"1000\" }}]\nreporting_currency = \"fixture_unit\"\nreporting_scale = 2\nmax_rate_age_micros = 0\n\n[[replay.strategies]]\nid = \"up\"\nplan_identity = \"{plan_identity}\"\nbase_stream = {{ duration_seconds = 15, offset_seconds = 5 }}\nconditions = [{{ stream = {{ duration_seconds = 15, offset_seconds = 5 }}, output = \"candle_direction\", comparator = \"eq\", threshold = \"up\" }}, {{ stream = {{ duration_seconds = 5, offset_seconds = 0 }}, output = \"candle_direction\", comparator = \"ne\", threshold = \"flat\" }}]\n\n[[replay.strategies]]\nid = \"down\"\nplan_identity = \"{plan_identity}\"\nbase_stream = {{ duration_seconds = 15, offset_seconds = 5 }}\nconditions = [{{ stream = {{ duration_seconds = 15, offset_seconds = 5 }}, output = \"candle_direction\", comparator = \"eq\", threshold = \"down\" }}, {{ stream = {{ duration_seconds = 5, offset_seconds = 0 }}, output = \"candle_direction\", comparator = \"ne\", threshold = \"flat\" }}]\n\n[[replay.contracts]]\nid = \"buy_10s\"\ndirection = \"buy\"\nduration_micros = 10000000\ncurrency = \"fixture_unit\"\nstake = \"1\"\nquoted_cost = \"1\"\nentry_fee = \"0\"\nwin = {{ gross_return = \"1.92\", terminal_fee = \"0\" }}\nloss = {{ gross_return = \"0\", terminal_fee = \"0\" }}\ntie = {{ gross_return = \"1\", terminal_fee = \"0\" }}\nsettlement = {{ rule = \"price_at_due_v1\", max_settlement_delay_micros = 60000000, max_tick_gap_micros = 60000000 }}\n\n[[replay.contracts]]\nid = \"sell_10s\"\ndirection = \"sell\"\nduration_micros = 10000000\ncurrency = \"fixture_unit\"\nstake = \"1\"\nquoted_cost = \"1\"\nentry_fee = \"0\"\nwin = {{ gross_return = \"1.92\", terminal_fee = \"0\" }}\nloss = {{ gross_return = \"0\", terminal_fee = \"0\" }}\ntie = {{ gross_return = \"1\", terminal_fee = \"0\" }}\nsettlement = {{ rule = \"price_at_due_v1\", max_settlement_delay_micros = 60000000, max_tick_gap_micros = 60000000 }}\n\n[[replay.risk_policies]]\nid = \"one_each\"\nmax_open_per_strategy = 1\nmax_open_total = 50000\nsame_entry = \"all\"\ndeduplicate_signal_logic = false\nmax_feature_age_micros = 60000000\nmax_quote_age_micros = 0\n\n[[replay.bindings]]\nid = \"buy_on_up\"\nstrategy = \"up\"\naccount = \"sim\"\ninstrument = \"pocket_option:AEDCNY_otc\"\ncontract = \"buy_10s\"\nrisk_policy = \"one_each\"\nenvelope = {{ max_purchase_cost = \"1\", max_entry_fee = \"0\", max_win_terminal_fee = \"0\", max_loss_terminal_fee = \"0\", max_tie_terminal_fee = \"0\", min_winning_net_return = \"0.92\", settlement_rule = \"price_at_due_v1\" }}\n\n[[replay.bindings]]\nid = \"sell_on_down\"\nstrategy = \"down\"\naccount = \"sim\"\ninstrument = \"pocket_option:AEDCNY_otc\"\ncontract = \"sell_10s\"\nrisk_policy = \"one_each\"\nenvelope = {{ max_purchase_cost = \"1\", max_entry_fee = \"0\", max_win_terminal_fee = \"0\", max_loss_terminal_fee = \"0\", max_tie_terminal_fee = \"0\", min_winning_net_return = \"0.92\", settlement_rule = \"price_at_due_v1\" }}\n",
        manifest_uri(tick),
        manifest_uri(feature)
    )
}

fn replay(config: &Path) -> Result<Vec<String>, String> {
    command(&["replay", "--config", config.to_str().unwrap()])
}

fn object_path(store: &Path, manifest: &ReplayManifest, path: &str) -> PathBuf {
    store.join(
        &manifest
            .objects
            .iter()
            .find(|object| object.path == path)
            .unwrap()
            .key,
    )
}

fn ledger_lines(path: &Path) -> Vec<Vec<u8>> {
    fs::read(path)
        .unwrap()
        .split(|&byte| byte == b'\n')
        .filter(|line| !line.is_empty())
        .map(<[u8]>::to_vec)
        .collect()
}

fn events(lines: &[Vec<u8>]) -> Vec<FinancialEvent> {
    lines
        .iter()
        .map(|line| FinancialEvent::from_line(line).unwrap())
        .collect()
}

/// The ledger without the records `omit` selects, renumbered.
fn without(lines: &[Vec<u8>], omit: impl Fn(&EventKind) -> bool) -> Vec<Vec<u8>> {
    events(lines)
        .into_iter()
        .filter(|event| !omit(&event.kind))
        .enumerate()
        .map(|(sequence, event)| {
            FinancialEvent {
                sequence: sequence as u64,
                ..event
            }
            .to_line()
        })
        .collect()
}

fn decimal(text: &str) -> Decimal {
    Decimal::parse(text).unwrap()
}

/// Publishes the deterministic fixture's tick, stream, and feature generations.
fn publish_inputs(scratch: &Scratch) -> (PathBuf, PathBuf, PathBuf) {
    let ticks = synthetic_ticks();
    let lines: Vec<String> = ticks
        .iter()
        .map(|&(millis, price)| tick_line(millis, price))
        .collect();
    write_ticks(
        &scratch.path("sources/ticks/ticks.csv"),
        &lines.iter().map(String::as_str).collect::<Vec<_>>(),
    );
    let published = |line: String| {
        scratch.path(&format!(
            "published/manifests/{}/ready.json",
            generation(&line)
        ))
    };
    let tick = published(
        import(&scratch.config("import.toml", &scratch.tick_source()))
            .unwrap()
            .remove(0),
    );
    let audit_config = scratch.config("audit.toml", TICK_INSTRUMENT);
    let profile = published(
        command(&[
            "data",
            "audit",
            "--config",
            audit_config.to_str().unwrap(),
            "--manifest",
            &manifest_uri(&tick),
        ])
        .unwrap()
        .remove(0),
    );
    let features_config = scratch.config("features.toml", &feature_entry(&tick, &profile));
    let feature = published(
        command(&[
            "features",
            "build",
            "--config",
            features_config.to_str().unwrap(),
        ])
        .unwrap()
        .remove(0),
    );
    (tick, profile, feature)
}

#[test]
fn replay_publishes_reconstructs_and_reuses() {
    let scratch = Scratch::new("phase06_replay");
    let (tick, _, feature) = publish_inputs(&scratch);
    let store = scratch.path("published");
    let feature_manifest = FeatureManifest::from_json(&fs::read(&feature).unwrap()).unwrap();
    let plan_identity = feature_manifest.plan_identity.clone();
    let config_path = scratch.config(
        "replay.toml",
        &replay_table(&tick, &feature, &plan_identity),
    );
    let config = Config::parse(&fs::read_to_string(&config_path).unwrap()).unwrap();

    // The command publishes, reconstructs, and reports; verify re-reads the same generation.
    let lines = replay(&config_path).unwrap();
    assert_eq!(lines.len(), 2, "{lines:?}");
    let replay_generation = generation(&lines[0]);
    let manifest_path = store.join(format!("manifests/{replay_generation}/ready.json"));
    assert_eq!(verify(&manifest_path).unwrap(), lines[1]);
    let manifest = ReplayManifest::from_json(&fs::read(&manifest_path).unwrap()).unwrap();
    assert_eq!(manifest.generation, replay_generation);
    assert_eq!(manifest.schema_version, REPLAY_SCHEMA_VERSION);
    assert_eq!(manifest.config_hash, config.content_hash());
    assert_eq!(manifest.code_revision, env!("BINARY_ALPHA_CODE_REVISION"));
    assert_eq!(manifest.availability, HISTORICAL_AVAILABILITY);
    assert_eq!(manifest.instruments.len(), 1);
    let bound = &manifest.instruments[0];
    assert_eq!(bound.plan_identity, plan_identity);
    assert_eq!(bound.feature_generation, feature_manifest.generation);
    assert_eq!(bound.tick_generation, feature_manifest.input_generation);
    assert_eq!(bound.outcome_generation, None);
    assert_eq!(
        bound
            .streams
            .iter()
            .map(|stream| (stream.stream.to_string(), stream.columns.len()))
            .collect::<Vec<_>>(),
        [("5s/0s".to_string(), 1), ("15s/5s".to_string(), 1)],
        "only the named streams and columns are bound, in frozen-plan order"
    );
    assert!(
        lines[0].starts_with(&format!(
            "replay development generation {replay_generation} instruments 1 events {} ",
            manifest.events
        )),
        "{}",
        lines[0]
    );
    assert!(
        lines[0].contains("objects 2 reused 0 [load "),
        "{}",
        lines[0]
    );

    // The ledger: contiguous records, the definition first, every posting exact and consistent.
    let lines_of_ledger = ledger_lines(&object_path(&store, &manifest, EVENTS_OBJECT_PATH));
    let ledger = events(&lines_of_ledger);
    assert_eq!(ledger.len() as u64, manifest.events);
    assert!(
        ledger
            .iter()
            .enumerate()
            .all(|(index, event)| event.sequence == index as u64)
    );
    let EventKind::RunDefinition { definition } = &ledger[0].kind else {
        panic!("the first record is the definition");
    };
    assert_eq!(definition.replay, *config.replay.as_ref().unwrap());
    assert_eq!(definition.instruments, manifest.instruments);
    let mut signals = BTreeMap::new();
    let mut accepted = BTreeMap::new();
    let mut settled = 0;
    let mut unresolved = 0;
    let mut profit = decimal("0.00");
    let mut open_basis = decimal("0.00");
    for event in &ledger[1..] {
        match &event.kind {
            EventKind::Signal {
                stream,
                close_time_micros,
                known_at_micros,
                disposition,
                command,
                reservation,
                quote_time_micros,
                split,
                ..
            } => {
                assert_eq!(stream.duration_seconds, 15);
                assert!(known_at_micros >= close_time_micros);
                assert_eq!(
                    event.time_micros, *known_at_micros,
                    "decision at availability"
                );
                assert_eq!(quote_time_micros, &Some(*known_at_micros), "provider order");
                assert!(split.is_some());
                if *disposition == Disposition::Admitted {
                    assert_eq!(reservation, &Some(decimal("1.00")));
                    signals.insert(command.clone().unwrap(), *known_at_micros);
                } else {
                    assert_eq!(*disposition, Disposition::CapacityStrategy, "{event:?}");
                    assert!(command.is_none() && reservation.is_none());
                }
            }
            EventKind::Accepted {
                command,
                source,
                entry_time_micros: Some(entry_time_micros),
                due_time_micros: Some(due_time_micros),
                debit,
                reservation,
                ..
            } => {
                assert_eq!(signals[command], *entry_time_micros);
                assert!(source.simulated && source.id.starts_with(HISTORICAL_AVAILABILITY));
                assert_eq!(*due_time_micros, entry_time_micros + 10_000_000);
                assert_eq!(
                    (debit.to_string(), reservation.to_string()),
                    ("1.00".into(), "0.00".into())
                );
                open_basis = open_basis.checked_add(*debit).unwrap();
                accepted.insert(command.clone(), *due_time_micros);
            }
            EventKind::Settled {
                command,
                settlement_time_micros,
                outcome,
                credit,
                profit: trade_profit,
                discrepancy,
                deficit,
                path: Some(path),
                ..
            } => {
                let due = accepted.remove(command).unwrap();
                assert!(
                    *settlement_time_micros >= due && *settlement_time_micros - due <= 60_000_000
                );
                let expected = match outcome {
                    Outcome::Win => "1.92",
                    Outcome::Loss => "0.00",
                    Outcome::Tie => "1.00",
                };
                assert_eq!(credit.to_string(), expected);
                assert_eq!(
                    trade_profit.to_string(),
                    credit
                        .checked_sub(decimal("1"))
                        .unwrap()
                        .rescale(2)
                        .unwrap()
                        .to_string()
                );
                assert!(!discrepancy && deficit.is_none());
                assert!(path.max_favorable_time_micros >= signals[command]);
                profit = profit.checked_add(*trade_profit).unwrap();
                open_basis = open_basis.checked_sub(decimal("1.00")).unwrap();
                settled += 1;
            }
            EventKind::Unresolved {
                command, reason, ..
            } => {
                assert!(accepted.contains_key(command));
                assert!(
                    matches!(
                        reason,
                        UnresolvedReason::Gap | UnresolvedReason::WindowExhausted
                    ),
                    "{event:?}"
                );
                unresolved += 1;
            }
            other => panic!("unexpected record {other:?}"),
        }
    }
    assert!(
        settled > 20 && unresolved >= 1,
        "settled {settled} unresolved {unresolved}"
    );
    let summary =
        Summary::from_json(&fs::read(object_path(&store, &manifest, SUMMARY_OBJECT_PATH)).unwrap())
            .unwrap();
    let account = &summary.accounts[0];
    assert_eq!(account.completed_profit, profit.rescale(2).unwrap());
    assert_eq!(account.paid_basis, open_basis.rescale(2).unwrap());
    assert_eq!(
        account.cash,
        decimal("1000.00")
            .checked_add(profit)
            .unwrap()
            .checked_sub(open_basis)
            .unwrap()
            .rescale(2)
            .unwrap(),
        "native cash is initial cash plus completed profit minus the paid basis of open contracts"
    );
    assert_eq!(account.reserved.to_string(), "0.00");
    assert_eq!(account.open as usize, accepted.len());
    assert_eq!(summary.portfolio.settled, settled);
    assert_eq!(summary.portfolio.unresolved, unresolved);
    assert_eq!(summary.portfolio.accepted, signals.len() as u64);
    assert_eq!(summary.splits.len(), 2);
    assert_eq!(
        summary.reporting.settled_equity,
        Some(account.cash.checked_add(account.paid_basis).unwrap())
    );

    // Restoring the ledger alone reproduces the final state and summary; altered bytes, a
    // dropped record, and a reordered record fail.
    let restored = Engine::restore(lines_of_ledger.iter().cloned().map(Ok)).unwrap();
    assert_eq!(restored.state_identity(), manifest.final_state_identity);
    assert_eq!(restored.summary(), &summary);
    assert_eq!(restored.summary().identity(), manifest.summary_identity);
    let mut altered = lines_of_ledger.clone();
    let position = altered[5].iter().rposition(|&byte| byte == b'"').unwrap() - 1;
    altered[5][position] = if altered[5][position] == b'1' {
        b'2'
    } else {
        b'1'
    };
    assert!(Engine::restore(altered.iter().cloned().map(Ok)).is_err());
    let mut dropped = lines_of_ledger.clone();
    dropped.remove(3);
    assert!(
        Engine::restore(dropped.iter().cloned().map(Ok))
            .err()
            .unwrap()
            .contains("sequence")
    );
    let mut swapped = lines_of_ledger.clone();
    swapped.swap(1, 2);
    assert!(Engine::restore(swapped.iter().cloned().map(Ok)).is_err());

    // A deterministic live adapter feeding the same observations with the same availability
    // produces byte-identical records.
    let dataset = GenerationManifest::from_json(&fs::read(&tick).unwrap()).unwrap();
    let ticks = read_normalized_ticks(&store, &dataset);
    let rows: Vec<StreamRows> = bound
        .streams
        .iter()
        .map(|stream| {
            let object = feature_manifest
                .objects
                .iter()
                .find(|object| {
                    object.path
                        == format!(
                            "rows/{}s_{}s.parquet",
                            stream.stream.duration_seconds, stream.stream.offset_seconds
                        )
                })
                .unwrap();
            let (names, table) = read_table(&store.join(&object.key));
            let column = |name: &str| names.iter().position(|column| column == name).unwrap();
            let (close, known) = (column("close_time_micros"), column("known_at_micros"));
            let sources: Vec<usize> = stream
                .columns
                .iter()
                .map(|spec| column(&spec.source))
                .collect();
            table
                .into_iter()
                .map(|row| {
                    let time = |index: usize| match row[index] {
                        Some(Value::Time(micros)) => micros,
                        _ => panic!("clock"),
                    };
                    (
                        time(close),
                        time(known),
                        sources.iter().map(|&index| row[index].clone()).collect(),
                    )
                })
                .collect()
        })
        .collect();
    let live = live_adapter((**definition).clone(), &ticks, rows);
    assert_eq!(live.len(), lines_of_ledger.len());
    for (index, (line, expected)) in live.iter().zip(&lines_of_ledger).enumerate() {
        assert_eq!(
            line.strip_suffix(b"\n").unwrap(),
            expected.as_slice(),
            "record {index}"
        );
    }

    // Publishing the same generation again reuses it; the same inputs under another
    // configuration are another generation.
    let again = replay(&config_path).unwrap();
    assert_eq!(
        again[0],
        format!(
            "{} reused 2 (already published)",
            lines[0].split(" reused 0 [load").next().unwrap()
        )
    );
    assert_eq!(again[1], lines[1]);
    let other = scratch.config(
        "replay2.toml",
        &replay_table(&tick, &feature, &plan_identity)
            .replace("max_open_per_strategy = 1", "max_open_per_strategy = 2"),
    );
    assert_ne!(generation(&replay(&other).unwrap()[0]), replay_generation);

    // A strategy naming an output the plan does not compile, or another plan, is refused.
    for (from, to, expected) in [
        (
            "output = \"candle_direction\", comparator = \"ne\"",
            "output = \"regime_trend_state\", comparator = \"ne\"",
            "not a compiled output",
        ),
        ("threshold = \"up\" }", "threshold = 1 }", "threshold type"),
        (
            &plan_identity[..],
            &"0".repeat(64),
            "no replay input carries frozen plan",
        ),
    ] {
        let path = scratch.config(
            "replay3.toml",
            &replay_table(&tick, &feature, &plan_identity).replacen(from, to, 1),
        );
        let error = replay(&path).unwrap_err();
        assert!(error.contains(expected), "{error}");
    }
}

/// The test live adapter: the same observations, availability, and simulated acceptances the
/// historical adapter feeds, through the same engine, rendered as ledger lines.
fn live_adapter(
    definition: RunDefinition,
    ticks: &[binary_alpha_engine::market::Tick],
    mut rows: Vec<StreamRows>,
) -> Vec<Vec<u8>> {
    let mut engine = Engine::new(definition).unwrap();
    let mut lines: Vec<Vec<u8>> = engine.drain().iter().map(FinancialEvent::to_line).collect();
    let mut next_tick = 0;
    let mut next_row = vec![0; rows.len()];
    loop {
        let mut time = ticks.get(next_tick).map(|tick| tick.event_time_micros);
        for (stream, rows) in rows.iter().enumerate() {
            if let Some(row) = rows.get(next_row[stream]) {
                time = Some(time.map_or(row.1, |time| time.min(row.1)));
            }
        }
        let Some(time) = time else { break };
        let mut observations = Vec::new();
        while ticks
            .get(next_tick)
            .is_some_and(|tick| tick.event_time_micros == time)
        {
            observations.push(Observation::Tick {
                instrument: 0,
                provider_time_micros: time,
                price_units: ticks[next_tick].price_units,
            });
            next_tick += 1;
        }
        for stream in 0..rows.len() {
            while rows[stream]
                .get(next_row[stream])
                .is_some_and(|row| row.1 == time)
            {
                let (close_time_micros, known_at_micros, values) =
                    std::mem::take(&mut rows[stream][next_row[stream]]);
                observations.push(Observation::Row {
                    instrument: 0,
                    stream,
                    close_time_micros,
                    known_at_micros,
                    values,
                });
                next_row[stream] += 1;
            }
        }
        engine.step(time, observations).unwrap();
        let mut acceptances = Vec::new();
        for event in engine.drain() {
            if let EventKind::Signal {
                command: Some(command),
                quote_price_units: Some(price),
                quote_time_micros: Some(price_time),
                ..
            } = &event.kind
            {
                acceptances.push(Observation::Accepted {
                    command: command.clone(),
                    source: EventSource {
                        id: format!("{HISTORICAL_AVAILABILITY}:{command}"),
                        provider_time_micros: time,
                        available_at_micros: time,
                        simulated: true,
                    },
                    entry_time_micros: time,
                    entry_price_units: *price,
                    price_time_micros: *price_time,
                });
            }
            lines.push(event.to_line());
        }
        if !acceptances.is_empty() {
            engine.step(time, acceptances).unwrap();
            lines.extend(engine.drain().iter().map(FinancialEvent::to_line));
        }
    }
    engine.finish().unwrap();
    lines.extend(engine.drain().iter().map(FinancialEvent::to_line));
    lines
}

// ---------------------------------------------------------------------------------------------
// Financial scenario suite over the engine's observation interface
// ---------------------------------------------------------------------------------------------

const BASE: &str = "\n[replay]\nrole = \"development\"\ndecision_start = \"1970-01-01T00:00:00Z\"\ndecision_end = \"1970-01-01T01:00:00Z\"\ninputs = [{ tick_manifest = \"file:///p/manifests/1111111111111111111111111111111111111111111111111111111111111111/ready.json\", feature_manifest = \"file:///p/manifests/2222222222222222222222222222222222222222222222222222222222222222/ready.json\" }]\naccounts = [{ id = \"a\", broker = \"b\", currency = \"u\", scale = 2, initial_cash = \"1000\" }]\nreporting_currency = \"u\"\nreporting_scale = 2\nmax_rate_age_micros = 5\n\n[[replay.strategies]]\nid = \"s\"\nplan_identity = \"plan\"\nbase_stream = { duration_seconds = 5, offset_seconds = 0 }\nconditions = [{ stream = { duration_seconds = 5, offset_seconds = 0 }, output = \"signal\", comparator = \"eq\", threshold = true }]\n\n[[replay.contracts]]\nid = \"c\"\ndirection = \"buy\"\nduration_micros = 10\ncurrency = \"u\"\nstake = \"1\"\nquoted_cost = \"1\"\nentry_fee = \"0\"\nwin = { gross_return = \"1.92\", terminal_fee = \"0\" }\nloss = { gross_return = \"0\", terminal_fee = \"0\" }\ntie = { gross_return = \"1\", terminal_fee = \"0\" }\nsettlement = { rule = \"price_at_due_v1\", max_settlement_delay_micros = 5, max_tick_gap_micros = 60 }\n\n[[replay.risk_policies]]\nid = \"p\"\nmax_open_per_strategy = 1\nsame_entry = \"all\"\ndeduplicate_signal_logic = false\nmax_feature_age_micros = 1000\nmax_quote_age_micros = 1000\n\n[[replay.bindings]]\nid = \"b1\"\nstrategy = \"s\"\naccount = \"a\"\ninstrument = \"b:X\"\ncontract = \"c\"\nrisk_policy = \"p\"\nenvelope = { max_purchase_cost = \"1\", max_entry_fee = \"0\", max_win_terminal_fee = \"0\", max_loss_terminal_fee = \"0\", max_tie_terminal_fee = \"0\", min_winning_net_return = \"0.92\", settlement_rule = \"price_at_due_v1\" }\n";

const HEAD: &str = "schema_version = 1\nrun_mode = \"research\"\n\n[storage]\nhistorical_data_dir = \"h\"\npublication_uri = \"file:///p\"\n";

fn stream(duration_seconds: u32, offset_seconds: u32) -> StreamKey {
    StreamKey {
        duration_seconds,
        offset_seconds,
    }
}

fn condition(
    key: StreamKey,
    output: &str,
    comparator: Comparator,
    threshold: Threshold,
) -> Condition {
    Condition {
        stream: key,
        output: output.to_string(),
        comparator,
        threshold,
    }
}

/// The bound instrument `BROKER:SYMBOL` with a boolean `signal` column on stream 5s/0s and the
/// columns `signal`, `other`, `count`, `count_ready` (the readiness flag of `count`), and the
/// text `state` (not ready while it reads `not_ready`) on stream 15s/5s.
fn instrument(id: &str, generation: char, plan: &str) -> InstrumentBinding {
    let column = |name: &str, kind: Kind| ColumnSpec {
        name: name.to_string(),
        source: name.to_string(),
        kind,
        encoding: None,
        readiness: Vec::new(),
        unready: Vec::new(),
    };
    let (broker, symbol) = id.split_once(':').unwrap();
    InstrumentBinding {
        instrument: id.to_string(),
        broker: broker.to_string().try_into().unwrap(),
        provider_symbol: symbol.to_string().try_into().unwrap(),
        price_scale: 2,
        tick_generation: generation.to_string().repeat(64),
        feature_generation: "2".repeat(64),
        plan_identity: plan.to_string(),
        raw_identity: "raw".into(),
        outcome_generation: None,
        streams: vec![
            StreamColumns {
                stream: stream(5, 0),
                columns: vec![column("signal", Kind::Bool)],
            },
            StreamColumns {
                stream: stream(15, 5),
                columns: vec![
                    column("signal", Kind::Bool),
                    column("other", Kind::Bool),
                    ColumnSpec {
                        readiness: vec!["count_ready".into()],
                        ..column("count", Kind::Int)
                    },
                    column("count_ready", Kind::Bool),
                    ColumnSpec {
                        unready: vec!["not_ready".into()],
                        ..column("state", Kind::Text)
                    },
                ],
            },
        ],
    }
}

/// The base run: one instrument `b:X` bound to plan `plan`, account `a` (currency `u`, scale 2,
/// cash 1000), strategy `s` on stream 5s/0s requiring `signal == true`, contract `c` (buy, ten
/// microseconds, cost 1, win 1.92, tie 1, no fees), policy `p` (one open per strategy), and
/// binding `b1`; `edit` changes the typed records before the engine compiles them.
fn definition(edit: impl FnOnce(&mut Replay)) -> RunDefinition {
    let config = Config::parse(&format!("{HEAD}{BASE}")).unwrap();
    let mut replay = config.replay.unwrap();
    edit(&mut replay);
    RunDefinition {
        schema_version: REPLAY_SCHEMA_VERSION,
        config_hash: "hash".into(),
        code_revision: "revision".into(),
        availability: "test_live".into(),
        replay,
        instruments: vec![instrument("b:X", '1', "plan")],
    }
}

/// A second strategy `id` on `base` requiring that stream's `signal`, frozen on the same plan.
fn strategy(replay: &Replay, id: &str, base: StreamKey) -> StrategySpec {
    StrategySpec {
        id: id.to_string(),
        plan_identity: replay.strategies[0].plan_identity.clone(),
        base_stream: base,
        conditions: vec![condition(
            base,
            "signal",
            Comparator::Eq,
            Threshold::Bool(true),
        )],
        repair: Vec::new(),
    }
}

/// A second binding `id` of strategy `strategy` with the first binding's account, contract,
/// policy, and envelope.
fn binding(replay: &Replay, id: &str, strategy: &str) -> DeploymentBinding {
    DeploymentBinding {
        id: id.to_string(),
        strategy: strategy.to_string(),
        ..replay.bindings[0].clone()
    }
}

fn tick(time: i64, price: i64) -> Observation {
    tick_of(0, time, price)
}

fn tick_of(instrument: usize, time: i64, price: i64) -> Observation {
    Observation::Tick {
        instrument,
        provider_time_micros: time,
        price_units: price,
    }
}

/// A row of `stream` with `signal`; the second stream's `other` is true, its `count` a ready
/// four, and its `state` `flat`.
fn row(stream: usize, close: i64, known: i64, signal: bool) -> Observation {
    let mut values = vec![Some(Value::Bool(signal))];
    if stream == 1 {
        values.extend([
            Some(Value::Bool(true)),
            Some(Value::Int(4)),
            Some(Value::Bool(true)),
            Some(Value::Text(std::borrow::Cow::Borrowed("flat"))),
        ]);
    }
    row_of(0, stream, close, known, values)
}

fn row_of(
    instrument: usize,
    stream: usize,
    close: i64,
    known: i64,
    values: Vec<Option<Value>>,
) -> Observation {
    Observation::Row {
        instrument,
        stream,
        close_time_micros: close,
        known_at_micros: known,
        values,
    }
}

fn source(id: &str, time: i64) -> EventSource {
    EventSource {
        id: id.to_string(),
        provider_time_micros: time,
        available_at_micros: time,
        simulated: false,
    }
}

fn settlement(
    command: &str,
    time: i64,
    outcome: Outcome,
    gross: &str,
    fee: &str,
    price: i64,
) -> Observation {
    Observation::Settlement {
        command: command.to_string(),
        source: source(&format!("broker:settle:{command}:{time}"), time),
        outcome,
        gross_return: decimal(gross),
        terminal_fee: decimal(fee),
        settlement_price_units: price,
    }
}

fn reconciliation(command: &str, time: i64, resolution: Resolution) -> Observation {
    Observation::Reconciliation {
        command: command.to_string(),
        source: source(&format!("broker:reconcile:{command}:{time}"), time),
        resolution,
    }
}

#[test]
fn fitted_projection_matches_engine_signals_at_causal_base_decisions() {
    const SECOND: i64 = 1_000_000;
    struct Case {
        name: &'static str,
        condition_rows: Vec<(i64, i64, &'static str, bool)>,
        base_close: i64,
        base_known: i64,
        base_signal: bool,
        start: i64,
        end: i64,
        expected: bool,
    }
    let cases = [
        Case {
            name: "offset installed earlier",
            condition_rows: vec![(5, 10, "flat", true)],
            base_close: 15,
            base_known: 20,
            base_signal: true,
            start: 0,
            end: 30,
            expected: true,
        },
        Case {
            name: "delayed known at same time",
            condition_rows: vec![(5, 20, "flat", true)],
            base_close: 15,
            base_known: 20,
            base_signal: true,
            start: 0,
            end: 30,
            expected: true,
        },
        Case {
            name: "later close replaces delayed earlier",
            condition_rows: vec![(5, 10, "flat", true), (20, 20, "flat", true)],
            base_close: 15,
            base_known: 20,
            base_signal: true,
            start: 0,
            end: 30,
            expected: false,
        },
        Case {
            name: "later close replaces earlier",
            condition_rows: vec![(5, 5, "flat", true), (20, 20, "flat", true)],
            base_close: 15,
            base_known: 20,
            base_signal: true,
            start: 0,
            end: 30,
            expected: false,
        },
        Case {
            name: "tick finalized 5s at 15 with 15s at 20",
            condition_rows: vec![(20, 20, "flat", true)],
            base_close: 15,
            base_known: 20,
            base_signal: true,
            start: 0,
            end: 30,
            expected: false,
        },
        Case {
            name: "window starts at installation",
            condition_rows: vec![(5, 20, "flat", true)],
            base_close: 15,
            base_known: 20,
            base_signal: true,
            start: 20,
            end: 30,
            expected: true,
        },
        Case {
            name: "window ends at installation",
            condition_rows: vec![(20, 30, "flat", true)],
            base_close: 25,
            base_known: 30,
            base_signal: true,
            start: 0,
            end: 30,
            expected: false,
        },
        Case {
            name: "declared unready",
            condition_rows: vec![(5, 20, "not_ready", true)],
            base_close: 15,
            base_known: 20,
            base_signal: true,
            start: 0,
            end: 30,
            expected: false,
        },
        Case {
            name: "readiness flag false",
            condition_rows: vec![(5, 20, "flat", false)],
            base_close: 15,
            base_known: 20,
            base_signal: true,
            start: 0,
            end: 30,
            expected: false,
        },
        Case {
            name: "uncoded label",
            condition_rows: vec![(5, 20, "none", true)],
            base_close: 15,
            base_known: 20,
            base_signal: true,
            start: 0,
            end: 30,
            expected: false,
        },
        Case {
            name: "base conjunction false",
            condition_rows: vec![(5, 20, "flat", true)],
            base_close: 15,
            base_known: 20,
            base_signal: false,
            start: 0,
            end: 30,
            expected: false,
        },
    ];
    for case in cases {
        let mut definition = definition(|replay| {
            replay.decision_start = format_event_time_micros(case.start * SECOND);
            replay.decision_end = format_event_time_micros(case.end * SECOND);
            replay.strategies[0].conditions.push(condition(
                stream(15, 5),
                "state_encoded",
                Comparator::Eq,
                Threshold::Text("flat".into()),
            ));
        });
        let spec = &mut definition.instruments[0].streams[1].columns[4];
        spec.name = "state_encoded".into();
        spec.source = "state".into();
        spec.readiness = vec!["count_ready".into()];
        spec.unready = vec!["not_ready".into()];
        spec.encoding = Some(FittedEncoding {
            output: "state_encoded".into(),
            input: "state".into(),
            automatic: true,
            encoding: ProjectionKind::Category,
            edges: None,
            input_divisor: 1.0,
            labels: vec!["flat".into(), "other".into()],
        });
        let spec = spec.clone();
        let mut engine = Engine::new(definition).unwrap();
        engine.drain();
        let mut at_base = vec![tick(case.base_known * SECOND, 100)];
        let mut latest = None;
        for (close, known, text, ready) in case.condition_rows {
            let mut values = vec![
                Some(Value::Bool(true)),
                Some(Value::Bool(true)),
                Some(Value::Int(4)),
                Some(Value::Bool(ready)),
                Some(Value::Text(text.into())),
            ];
            let observation = row_of(0, 1, close * SECOND, known * SECOND, values.clone());
            if known == case.base_known {
                at_base.push(observation);
            } else {
                engine
                    .step(known * SECOND, vec![tick(known * SECOND, 100), observation])
                    .unwrap();
                engine.drain();
            }
            latest = Some((close * SECOND, std::mem::take(&mut values)));
        }
        at_base.push(row_of(
            0,
            0,
            case.base_close * SECOND,
            case.base_known * SECOND,
            vec![Some(Value::Bool(case.base_signal))],
        ));
        engine.step(case.base_known * SECOND, at_base).unwrap();
        let signals = engine.drain();
        let lowered = signals
            .iter()
            .any(|event| matches!(event.kind, EventKind::Signal { .. }));
        let code = project_fitted_label(
            case.base_close * SECOND,
            latest
                .as_ref()
                .map(|(close, values)| (*close, values[4].as_ref())),
            &spec,
            latest
                .as_ref()
                .map(|(_, values)| values[3] == Some(Value::Bool(true))),
        );
        let projected = code == 0
            && case.base_signal
            && case.start <= case.base_known
            && case.base_known < case.end;
        assert_eq!(projected, lowered, "{}: {signals:?}", case.name);
        assert_eq!(lowered, case.expected, "{}", case.name);
    }
}

#[test]
fn dropped_fitted_label_keeps_lowering_and_replay_equality() {
    for (label, retained, expected_code) in [("other", false, -1), ("flat", true, 0)] {
        for lowering in [true, false] {
            let mut definition = definition(|replay| {
                replay.strategies[0].conditions = vec![condition(
                    stream(15, 5),
                    "state_encoded",
                    Comparator::Eq,
                    Threshold::Text(label.into()),
                )];
                if lowering {
                    replay.accounts[0].initial_cash = Decimal::zero(0);
                }
            });
            let spec = &mut definition.instruments[0].streams[1].columns[4];
            spec.name = "state_encoded".into();
            spec.source = "state".into();
            spec.encoding = Some(FittedEncoding {
                output: "state_encoded".into(),
                input: "state".into(),
                automatic: true,
                encoding: ProjectionKind::Category,
                edges: None,
                input_divisor: 1.0,
                labels: vec!["flat".into()], // The other category was dropped by max_labels = 1.
            });
            let spec = spec.clone();
            let mut engine = Engine::new(definition).unwrap();
            engine.drain();
            let value = Value::Text(label.into());
            let values = vec![
                Some(Value::Bool(true)),
                Some(Value::Bool(true)),
                Some(Value::Int(4)),
                Some(Value::Bool(true)),
                Some(value.clone()),
            ];
            engine
                .step(
                    20_000_000,
                    vec![
                        tick(20_000_000, 100),
                        row_of(0, 1, 5_000_000, 20_000_000, values),
                        row_of(0, 0, 15_000_000, 20_000_000, vec![Some(Value::Bool(true))]),
                    ],
                )
                .unwrap();
            let events = engine.drain();
            assert!(
                events
                    .iter()
                    .any(|event| matches!(event.kind, EventKind::Signal { .. })),
                "{label} lowering={lowering}: {events:?}"
            );
            assert_eq!(
                project_fitted_label(15_000_000, Some((5_000_000, Some(&value))), &spec, []),
                expected_code
            );
            assert_eq!(retained, expected_code >= 0);
        }
    }
}

/// The ledger lines with `from` replaced by `to` in exactly one line.
fn tampered(lines: &[Vec<u8>], from: &str, to: &str) -> Vec<Vec<u8>> {
    let mut changed = 0;
    let lines = lines
        .iter()
        .map(|line| {
            let text = String::from_utf8(line.clone()).unwrap();
            if text.contains(from) {
                changed += 1;
            }
            text.replacen(from, to, 1).into_bytes()
        })
        .collect();
    assert_eq!(changed, 1, "{from}");
    lines
}

/// A live-adapter harness: every generated record is kept as ledger lines so the run can be
/// restored and compared at any point.
struct Live {
    engine: Engine,
    lines: Vec<Vec<u8>>,
}

impl Live {
    fn new(definition: RunDefinition) -> Self {
        let mut engine = Engine::new(definition).unwrap();
        let lines = engine.drain().iter().map(FinancialEvent::to_line).collect();
        Self { engine, lines }
    }

    /// An engine restored from a ledger prefix.
    fn from_lines(lines: Vec<Vec<u8>>) -> Self {
        Self {
            engine: Engine::restore(lines.iter().cloned().map(Ok)).unwrap(),
            lines,
        }
    }

    fn step(&mut self, time: i64, observations: Vec<Observation>) -> Vec<FinancialEvent> {
        self.try_step(time, observations).unwrap()
    }

    fn try_step(
        &mut self,
        time: i64,
        observations: Vec<Observation>,
    ) -> Result<Vec<FinancialEvent>, String> {
        self.engine.step(time, observations)?;
        let events = self.engine.drain();
        self.lines
            .extend(events.iter().map(FinancialEvent::to_line));
        Ok(events)
    }

    /// One step plus the historical simulation's acceptance echo for every admitted signal.
    fn simulate(&mut self, time: i64, observations: Vec<Observation>) -> Vec<FinancialEvent> {
        let mut events = self.step(time, observations);
        let acceptances: Vec<Observation> = events
            .iter()
            .filter_map(|event| match &event.kind {
                EventKind::Signal {
                    command: Some(command),
                    quote_price_units: Some(price),
                    quote_time_micros: Some(price_time),
                    ..
                } => Some(Observation::Accepted {
                    command: command.clone(),
                    source: source(&format!("accept:{command}"), time),
                    entry_time_micros: time,
                    entry_price_units: *price,
                    price_time_micros: *price_time,
                }),
                _ => None,
            })
            .collect();
        if !acceptances.is_empty() {
            events.extend(self.step(time, acceptances));
        }
        events
    }

    /// The command of the one admitted signal in `events`.
    fn command(events: &[FinancialEvent]) -> String {
        events
            .iter()
            .find_map(|event| match &event.kind {
                EventKind::Signal {
                    command: Some(command),
                    ..
                } => Some(command.clone()),
                _ => None,
            })
            .unwrap()
    }

    /// Ends the run: unresolved marks for open obligations, then the rates due by the decision
    /// end.
    fn finish(&mut self) -> Vec<FinancialEvent> {
        self.engine.finish().unwrap();
        let events = self.engine.drain();
        self.lines
            .extend(events.iter().map(FinancialEvent::to_line));
        events
    }

    fn cash(&self) -> String {
        self.engine.accounts()[0].cash.to_string()
    }

    fn account(&self, id: &str) -> &AccountState {
        self.engine
            .accounts()
            .iter()
            .find(|account| account.id == id)
            .unwrap()
    }

    /// The account's cash, reserved, paid basis, unresolved loss, completed profit, and open
    /// count, as text.
    fn balances(&self, id: &str) -> (String, String, String, String, String, u32) {
        let account = self.account(id);
        (
            account.cash.to_string(),
            account.reserved.to_string(),
            account.paid_basis.to_string(),
            account.unresolved_loss.to_string(),
            account.completed_profit.to_string(),
            account.open,
        )
    }

    /// Restores the ledger so far into a second harness and asserts it reproduces this one.
    fn restored(&self) -> Live {
        let restored = Engine::restore(self.lines.iter().cloned().map(Ok)).unwrap();
        assert_eq!(restored.sequence(), self.engine.sequence());
        assert_eq!(restored.state_identity(), self.engine.state_identity());
        assert_eq!(restored.summary(), self.engine.summary());
        Live {
            engine: restored,
            lines: self.lines.clone(),
        }
    }

    fn assert_restorable(&self) {
        self.restored();
    }
}

fn dispositions(events: &[FinancialEvent]) -> Vec<Disposition> {
    events
        .iter()
        .filter_map(|event| match &event.kind {
            EventKind::Signal { disposition, .. } => Some(*disposition),
            _ => None,
        })
        .collect()
}

fn kinds(events: &[FinancialEvent]) -> Vec<&'static str> {
    events
        .iter()
        .map(|event| match &event.kind {
            EventKind::RunDefinition { .. } => "definition",
            EventKind::Signal { .. } => "signal",
            EventKind::Acknowledged { .. } => "acknowledged",
            EventKind::Accepted { .. } => "accepted",
            EventKind::Confirmed { .. } => "confirmed",
            EventKind::CashObserved { .. } => "cash_observed",
            EventKind::Released { .. } => "released",
            EventKind::PossiblySent { .. } => "possibly_sent",
            EventKind::Settled { .. } => "settled",
            EventKind::Unresolved { .. } => "unresolved",
            EventKind::Reconciled { .. } => "reconciled",
            EventKind::RateAvailable { .. } => "rate_available",
            EventKind::PauseStarted { .. } => "pause_started",
            EventKind::PauseEnded { .. } => "pause_ended",
        })
        .collect()
}

#[test]
fn causality_settlement_and_gaps_follow_availability() {
    // A price tick at 100 available only at 103 admits at decision 103 with duration ten:
    // entry 103, due 113, and the quote keeps its provider time.
    let mut live = Live::new(definition(|_| {}));
    let events = live.simulate(103, vec![tick(100, 500), row(0, 100, 103, true)]);
    assert_eq!(kinds(&events), ["signal", "accepted"]);
    let EventKind::Accepted {
        entry_time_micros: Some(entry_time_micros),
        due_time_micros: Some(due_time_micros),
        price_time_micros: Some(price_time_micros),
        entry_price_units: Some(entry_price_units),
        ..
    } = &events[1].kind
    else {
        unreachable!()
    };
    assert_eq!(
        (
            *entry_time_micros,
            *due_time_micros,
            *price_time_micros,
            *entry_price_units
        ),
        (103, 113, 100, 500)
    );
    assert_eq!(live.cash(), "999.00");
    // Equal-time settlement before entry: the settling tick at 113 frees the strategy's one
    // slot before the row at 113 is evaluated, and the settlement tick is in the path.
    let events = live.simulate(113, vec![tick(113, 520), row(0, 110, 113, true)]);
    assert_eq!(kinds(&events), ["settled", "signal", "accepted"]);
    let EventKind::Settled {
        outcome,
        credit,
        profit,
        path: Some(path),
        ..
    } = &events[0].kind
    else {
        unreachable!()
    };
    assert_eq!(
        (*outcome, credit.to_string(), profit.to_string()),
        (Outcome::Win, "1.92".into(), "0.92".into())
    );
    assert_eq!(
        (
            path.final_move_units,
            path.max_favorable_units,
            path.max_favorable_time_micros
        ),
        (20, 20, 113)
    );
    assert_eq!(dispositions(&events), [Disposition::Admitted]);
    assert_eq!(live.cash(), "999.92");
    // Without that settlement the slot is held: a row while the contract is open is capacity
    // blocked, and a gap larger than the maximum inside the window leaves it unresolved with
    // its cash, capacity, and exposure retained; a later authoritative settlement resolves it.
    let events = live.simulate(115, vec![tick(115, 519), row(0, 114, 115, true)]);
    assert_eq!(dispositions(&events), [Disposition::CapacityStrategy]);
    let events = live.simulate(190, vec![tick(190, 530)]);
    let EventKind::Unresolved {
        reason,
        path: Some(path),
        ..
    } = &events[0].kind
    else {
        panic!("{events:?}")
    };
    assert_eq!(*reason, UnresolvedReason::Gap);
    assert_eq!(
        path.final_move_units, -1,
        "the gap tick is not path evidence"
    );
    assert_eq!(
        live.balances("a"),
        (
            "999.92".into(),
            "0.00".into(),
            "1.00".into(),
            "1.00".into(),
            "0.92".into(),
            1
        )
    );
    let events = live.simulate(200, vec![tick(200, 531), row(0, 195, 200, true)]);
    assert_eq!(
        dispositions(&events),
        [Disposition::CapacityStrategy],
        "no refund frees the slot"
    );
    let events = live.step(
        210,
        vec![settlement("b1/110", 205, Outcome::Loss, "0", "0", 400)],
    );
    assert_eq!(kinds(&events), ["settled"]);
    let EventKind::Settled {
        path: Some(path), ..
    } = &events[0].kind
    else {
        unreachable!()
    };
    assert_eq!(
        (
            path.final_move_units,
            path.max_adverse_units,
            path.max_adverse_time_micros
        ),
        (-120, 120, 205),
        "the authoritative settlement price is observed in the path"
    );
    assert_eq!(
        live.balances("a"),
        (
            "999.92".into(),
            "0.00".into(),
            "0.00".into(),
            "0.00".into(),
            "-0.08".into(),
            0
        )
    );
    // A later frozen run of equal prices cannot change an earlier settlement.
    let events = live.simulate(211, vec![tick(211, 531)]);
    assert!(events.is_empty());
    let events = live.simulate(220, vec![tick(220, 531), row(0, 215, 220, true)]);
    assert_eq!(kinds(&events), ["signal", "accepted"]);
    for time in 221..=240 {
        live.simulate(time, vec![tick(time, 531)]);
    }
    let account = live.account("a").clone();
    assert_eq!(
        (
            account.open,
            account.completed_profit.to_string(),
            account.cash.to_string()
        ),
        (0, "-0.08".into(), "999.92".into()),
        "the tie at due settles once; equal later ticks change nothing"
    );
    // A late settlement tick leaves the obligation unresolved; the window's end marks every
    // still-open obligation.
    let events = live.simulate(300, vec![tick(300, 531), row(0, 295, 300, true)]);
    assert_eq!(kinds(&events), ["signal", "accepted"]);
    let events = live.simulate(320, vec![tick(320, 540)]);
    let EventKind::Unresolved { reason, .. } = &events[0].kind else {
        panic!("{events:?}")
    };
    assert_eq!(*reason, UnresolvedReason::LateSettlement);
    let events = live.simulate(330, vec![tick(330, 540), row(0, 325, 330, true)]);
    assert_eq!(dispositions(&events), [Disposition::CapacityStrategy]);
    live.assert_restorable();
    live.engine.finish().unwrap();
    assert!(
        live.engine.drain().is_empty(),
        "an unresolved obligation is marked once"
    );
}

#[test]
fn settlement_availability_and_duplicate_events_are_explicit() {
    // A settlement with provider time 100 known at 103 cannot free capacity at decision 100.
    let mut live = Live::new(definition(|replay| {
        replay.contracts[0].duration_micros = 1000
    }));
    live.simulate(50, vec![tick(50, 500), row(0, 45, 50, true)]);
    let mut delayed = settlement("b1/45", 100, Outcome::Win, "1.92", "0", 510);
    let events = live.simulate(100, vec![tick(100, 510), row(0, 95, 100, true)]);
    assert_eq!(dispositions(&events), [Disposition::CapacityStrategy]);
    if let Observation::Settlement { source, .. } = &mut delayed {
        source.available_at_micros = 103;
    }
    assert!(
        live.restored()
            .try_step(102, vec![delayed.clone()])
            .is_err(),
        "not yet available"
    );
    let events = live.step(103, vec![delayed.clone(), row(0, 100, 103, true)]);
    assert_eq!(kinds(&events), ["settled", "signal"]);
    assert_eq!(dispositions(&events), [Disposition::Admitted]);
    // The same external identity and payload again is a no-op, before and after restoration; a
    // payload that differs at all, even an equal amount written with another scale, fails.
    assert!(live.step(104, vec![delayed.clone()]).is_empty());
    let mut restored = live.restored();
    assert!(restored.step(104, vec![delayed.clone()]).is_empty());
    let mut rescaled = delayed.clone();
    if let Observation::Settlement { gross_return, .. } = &mut rescaled {
        *gross_return = decimal("1.920");
    }
    let mut conflicting = delayed;
    if let Observation::Settlement { outcome, .. } = &mut conflicting {
        *outcome = Outcome::Loss;
    }
    for observation in [rescaled, conflicting] {
        for mut engine in [live.restored(), restored.restored()] {
            assert!(
                engine
                    .try_step(104, vec![observation.clone()])
                    .unwrap_err()
                    .contains("reconciliation failed")
            );
        }
    }
    live.assert_restorable();
    // A settled reconciliation's evidence is no earlier than the entry, or than the dispatch
    // when no acceptance was proved, at application and at restoration alike.
    let settled = Resolution::Settled {
        outcome: Outcome::Win,
        gross_return: decimal("1.92"),
        terminal_fee: decimal("0"),
    };
    let mut accepted = Live::new(definition(|_| {}));
    let command = Live::command(&accepted.simulate(10, vec![tick(10, 500), row(0, 10, 10, true)]));
    let error = accepted
        .try_step(20, vec![reconciliation(&command, 9, settled.clone())])
        .unwrap_err();
    assert!(error.contains("precedes its entry or dispatch"), "{error}");
    let mut dispatched = Live::new(definition(|_| {}));
    dispatched.step(10, vec![tick(10, 500), row(0, 10, 10, true)]);
    let error = dispatched
        .try_step(20, vec![reconciliation(&command, 9, settled.clone())])
        .unwrap_err();
    assert!(error.contains("precedes its entry or dispatch"), "{error}");
    let mut valid = Live::new(definition(|_| {}));
    valid.simulate(10, vec![tick(10, 500), row(0, 10, 10, true)]);
    assert_eq!(
        kinds(&valid.step(20, vec![reconciliation(&command, 20, settled)])),
        ["reconciled"]
    );
    let error = Engine::restore(
        tampered(
            &valid.lines,
            "\"provider_time_micros\":20,\"available_at_micros\":20",
            "\"provider_time_micros\":9,\"available_at_micros\":20",
        )
        .into_iter()
        .map(Ok),
    )
    .err()
    .unwrap();
    assert!(error.contains("precedes its entry or dispatch"), "{error}");
}

#[test]
fn alignment_follows_the_latest_row_of_the_other_stream() {
    let definition = definition(|replay| {
        replay.strategies[0].conditions.push(condition(
            stream(15, 5),
            "signal",
            Comparator::Eq,
            Threshold::Bool(true),
        ));
    });
    // The other stream's latest row must exist and must not close after the base row; the
    // engine never searches backward for an older acceptable row.
    let mut live = Live::new(definition);
    assert!(
        live.simulate(10, vec![tick(10, 500), row(0, 5, 10, true)])
            .is_empty(),
        "no row of the other stream yet"
    );
    live.simulate(12, vec![row(1, 12, 12, true)]);
    assert!(
        live.simulate(13, vec![tick(13, 500), row(0, 11, 13, true)])
            .is_empty(),
        "the other row closes after the base row"
    );
    let events = live.simulate(16, vec![tick(16, 500), row(0, 16, 16, true)]);
    assert_eq!(dispositions(&events), [Disposition::Admitted]);
    live.simulate(30, vec![tick(30, 500), row(1, 30, 30, false)]);
    assert!(
        live.simulate(31, vec![tick(31, 500), row(0, 31, 31, true)])
            .is_empty(),
        "the latest other row decides"
    );
    live.assert_restorable();
}

#[test]
fn rows_are_monotonic_and_a_failed_step_ends_the_engine() {
    let mut live = Live::new(definition(|_| {}));
    live.simulate(10, vec![tick(10, 500), row(0, 10, 10, true)]);
    assert!(
        live.step(11, vec![row(0, 10, 10, true)]).is_empty(),
        "the identical row again installs and evaluates nothing"
    );
    // A refused delivery may leave state the ledger does not hold, so the engine refuses every
    // later step; an engine restored from its ledger holds only the applied records and
    // continues. Rows are inputs, not ledger state, so each probe starts from the same steps.
    let fresh = || {
        let mut fresh = Live::new(definition(|_| {}));
        fresh.simulate(10, vec![tick(10, 500), row(0, 10, 10, true)]);
        fresh
    };
    let refused: [(&str, Vec<Observation>, &str); 4] = [
        (
            "an older row",
            vec![row(0, 9, 12, true)],
            "arrives after the row closing at",
        ),
        (
            "a conflicting row at the same close",
            vec![row(0, 10, 12, false)],
            "arrives after the row closing at",
        ),
        (
            "two rows of one stream at one time",
            vec![row(0, 11, 12, true), row(0, 12, 12, true)],
            "two rows of stream 0",
        ),
        (
            "a row known after the step time",
            vec![row(0, 12, 13, true)],
            "not available",
        ),
    ];
    for (what, observations, expected) in refused {
        let mut failed = fresh();
        let error = failed.try_step(12, observations).unwrap_err();
        assert!(error.contains(expected), "{what}: {error}");
        assert!(
            failed
                .try_step(13, vec![tick(13, 500)])
                .unwrap_err()
                .contains("restore it from its ledger"),
            "{what}: the failed engine refuses to continue"
        );
        assert!(failed.restored().step(13, vec![tick(13, 500)]).is_empty());
    }
    let events = live.simulate(20, vec![tick(20, 500), row(0, 20, 20, true)]);
    assert_eq!(kinds(&events), ["settled", "signal", "accepted"]);
    live.assert_restorable();
}

#[test]
fn continuity_is_the_obligation_s_own_evidence() {
    // With a maximum gap of five and ticks at 10, 14, and 18, the uninterrupted engine settles
    // at 21; an engine restored right after the acceptance knows no tick after the quote, so
    // the same tick is a gap from the quote tick: the obligation stays unresolved with its cash
    // and capacity retained, and no settlement is manufactured.
    let mut live = Live::new(definition(|replay| {
        replay.contracts[0].settlement.max_tick_gap_micros = 5;
    }));
    live.simulate(10, vec![tick(10, 500), row(0, 10, 10, true)]);
    let mut restored = live.restored();
    live.simulate(14, vec![tick(14, 510)]);
    live.simulate(18, vec![tick(18, 520)]);
    assert_eq!(kinds(&live.simulate(21, vec![tick(21, 530)])), ["settled"]);
    let events = restored.simulate(21, vec![tick(21, 530)]);
    let EventKind::Unresolved {
        reason, evidence, ..
    } = &events[0].kind
    else {
        panic!("{events:?}")
    };
    assert_eq!(*reason, UnresolvedReason::Gap);
    assert!(
        evidence.contains("from 1970-01-01T00:00:00.000010Z"),
        "{evidence}"
    );
    assert_eq!(
        restored.balances("a"),
        (
            "999.00".into(),
            "0.00".into(),
            "1.00".into(),
            "1.00".into(),
            "0.00".into(),
            1
        )
    );
    restored.assert_restorable();
    // The anchor is the quote tick, not the entry: quote 100, entry 103, maximum gap 12, next
    // tick 113. The uninterrupted engine measures 13 from the quote tick and retains the
    // obligation; so does an engine restored after the acceptance.
    let mut live = Live::new(definition(|replay| {
        replay.contracts[0].settlement.max_tick_gap_micros = 12;
    }));
    live.simulate(103, vec![tick(100, 500), row(0, 100, 103, true)]);
    let mut restored = live.restored();
    for engine in [&mut live, &mut restored] {
        let events = engine.simulate(113, vec![tick(113, 520)]);
        let EventKind::Unresolved { reason, .. } = &events[0].kind else {
            panic!("{events:?}")
        };
        assert_eq!(*reason, UnresolvedReason::Gap);
    }
    // A tick too late to settle is not path evidence, and an authoritative settlement's path is
    // the path recorded so far plus the settlement price at its own time.
    let mut live = Live::new(definition(|_| {}));
    live.simulate(10, vec![tick(10, 500), row(0, 10, 10, true)]);
    let events = live.simulate(26, vec![tick(26, 600)]);
    let EventKind::Unresolved {
        reason,
        path: Some(path),
        ..
    } = &events[0].kind
    else {
        panic!("{events:?}")
    };
    assert_eq!(*reason, UnresolvedReason::LateSettlement);
    assert_eq!(path.max_favorable_units, 0, "the late tick is not evidence");
    let events = live.step(
        27,
        vec![Observation::Settlement {
            command: "b1/10".into(),
            source: EventSource {
                id: "broker:late-settle".into(),
                provider_time_micros: 20,
                available_at_micros: 27,
                simulated: false,
            },
            outcome: Outcome::Win,
            gross_return: decimal("1.92"),
            terminal_fee: decimal("0"),
            settlement_price_units: 510,
        }],
    );
    let EventKind::Settled {
        path: Some(path), ..
    } = &events[0].kind
    else {
        panic!("{events:?}")
    };
    assert_eq!(
        (
            path.final_move_units,
            path.max_favorable_units,
            path.max_favorable_time_micros,
            path.first_favorable_time_micros
        ),
        (10, 10, 20, Some(20))
    );
    live.assert_restorable();
    // Ticks at or before the entry time, delivered late, are continuity evidence but not path
    // evidence.
    let mut live = Live::new(definition(|_| {}));
    live.simulate(103, vec![tick(100, 500), row(0, 100, 103, true)]);
    assert!(live.step(104, vec![tick(101, 520)]).is_empty());
    let events = live.simulate(113, vec![tick(113, 500)]);
    let EventKind::Settled {
        outcome,
        path: Some(path),
        ..
    } = &events[0].kind
    else {
        unreachable!()
    };
    assert_eq!(*outcome, Outcome::Tie);
    assert_eq!(
        (path.max_favorable_units, path.first_favorable_time_micros),
        (0, None)
    );
    // A delayed acceptance: ticks the instrument saw while the command was unaccepted are not
    // the obligation's evidence. Sent at 10, a tick at 20 during the wait, accepted at 21 with
    // entry 10: the next tick at 22 is 12 from the quote tick, so the obligation stays
    // unresolved instead of settling across the unobserved interval.
    let mut live = Live::new(definition(|replay| {
        replay.contracts[0].settlement.max_tick_gap_micros = 5;
    }));
    let command = Live::command(&live.step(10, vec![tick(10, 500), row(0, 10, 10, true)]));
    assert!(live.step(20, vec![tick(20, 400)]).is_empty());
    let events = live.step(
        21,
        vec![Observation::Accepted {
            command: command.clone(),
            source: source("broker:late", 21),
            entry_time_micros: 10,
            entry_price_units: 500,
            price_time_micros: 10,
        }],
    );
    assert_eq!(kinds(&events), ["accepted"]);
    let events = live.simulate(22, vec![tick(22, 600)]);
    let EventKind::Unresolved { reason, .. } = &events[0].kind else {
        panic!("{events:?}")
    };
    assert_eq!(*reason, UnresolvedReason::Gap);
    // Acceptance clocks: a quote dated after its entry, or an entry after the decision, fail.
    for (what, entry, price_time) in [
        ("quote after entry", 30, 31),
        ("entry after decision", 32, 30),
        ("entry before dispatch", 29, 29),
    ] {
        let mut fresh = Live::new(definition(|_| {}));
        let command = Live::command(&fresh.step(30, vec![tick(30, 500), row(0, 30, 30, true)]));
        let error = fresh
            .try_step(
                31,
                vec![Observation::Accepted {
                    command,
                    source: source("broker:accept", 31),
                    entry_time_micros: entry,
                    entry_price_units: 500,
                    price_time_micros: price_time,
                }],
            )
            .unwrap_err();
        assert!(error.contains("acceptance clocks"), "{what}: {error}");
    }
}

#[test]
fn freshness_bounds_are_exact() {
    let fresh = |feature: i64, quote: i64| {
        definition(|replay| {
            replay.risk_policies[0].max_feature_age_micros = feature;
            replay.risk_policies[0].max_quote_age_micros = quote;
        })
    };
    for (feature, quote, expected) in [
        (3, 5, Disposition::Admitted),
        (2, 5, Disposition::StaleFeature),
        (3, 4, Disposition::StaleQuote),
    ] {
        let mut live = Live::new(fresh(feature, quote));
        live.simulate(98, vec![tick(98, 500)]);
        let events = live.simulate(103, vec![row(0, 100, 103, true)]);
        assert_eq!(
            dispositions(&events),
            [expected],
            "feature {feature} quote {quote}"
        );
    }
    // No quote at all, and a gap into the quote tick larger than the contract's maximum, block.
    let mut live = Live::new(fresh(1000, 1000));
    assert_eq!(
        dispositions(&live.simulate(5, vec![row(0, 5, 5, true)])),
        [Disposition::NoQuote]
    );
    live.simulate(10, vec![tick(10, 500)]);
    let mut conflicting = Live::new(fresh(1000, 1000));
    conflicting.simulate(10, vec![tick(10, 500)]);
    assert!(
        conflicting
            .try_step(11, vec![tick(10, 400), row(0, 11, 11, true)])
            .unwrap_err()
            .contains("conflicting tick"),
        "another price at the same time is refused before any admission"
    );
    assert_eq!(
        dispositions(&live.simulate(71, vec![tick(71, 500), tick(71, 500), row(0, 70, 71, true)])),
        [Disposition::GapAtEntry],
        "a repeated tick at the same time keeps the gap into that time"
    );
    assert_eq!(
        dispositions(&live.simulate(131, vec![tick(131, 500), row(0, 130, 131, true)])),
        [Disposition::Admitted],
        "a gap of exactly the maximum is permitted"
    );
}

#[test]
fn selection_deduplication_and_repair_keep_their_slots() {
    let two = |same_entry: SameEntry, deduplicate: bool, repair: Vec<Condition>| {
        definition(|replay| {
            replay.risk_policies[0].same_entry = same_entry;
            replay.risk_policies[0].deduplicate_signal_logic = deduplicate;
            replay.strategies[0].repair = repair;
            let second = strategy(replay, "t", stream(5, 0));
            replay.strategies.push(second);
            let mut second = binding(replay, "b2", "t");
            second.envelope.max_purchase_cost = decimal("2");
            replay.bindings.push(second);
        })
    };
    let entry = |live: &mut Live| {
        dispositions(&live.simulate(
            10,
            vec![tick(10, 500), row(0, 10, 10, true), row(1, 10, 10, true)],
        ))
    };
    assert_eq!(
        entry(&mut Live::new(two(SameEntry::All, false, Vec::new()))),
        [Disposition::Admitted, Disposition::Admitted]
    );
    assert_eq!(
        entry(&mut Live::new(two(SameEntry::First, false, Vec::new()))),
        [Disposition::Admitted, Disposition::SameEntryDuplicate]
    );
    assert_eq!(
        entry(&mut Live::new(two(SameEntry::All, true, Vec::new()))),
        [Disposition::Admitted, Disposition::DuplicateLogic],
        "the same frozen logic at one entry event"
    );
    // A repair-blocked first match keeps its selection slot; the second matching candidate does
    // not replace it. Repair conditions are not signal logic: both strategies share one logic
    // identity, so the second binding needs its own envelope to be a distinct deployment.
    let repair = vec![condition(
        stream(15, 5),
        "other",
        Comparator::Eq,
        Threshold::Bool(false),
    )];
    assert_eq!(
        entry(&mut Live::new(two(SameEntry::First, false, repair.clone()))),
        [Disposition::RepairBlocked, Disposition::SameEntryDuplicate]
    );
    assert_eq!(
        entry(&mut Live::new(two(SameEntry::All, false, repair.clone()))),
        [Disposition::RepairBlocked, Disposition::Admitted]
    );
    // The slots belong to one instant and are ledger state: the next instant starts with free
    // slots (after the first contract settles), an engine restored between the two decisions
    // of one instant and given the batch again skips the decided first candidate and still
    // refuses the second its slot, and a ledger that admits the second candidate over an
    // occupied slot fails restoration.
    for (same_entry, deduplicate, expected) in [
        (SameEntry::First, false, Disposition::SameEntryDuplicate),
        (SameEntry::All, true, Disposition::DuplicateLogic),
    ] {
        let mut live = Live::new(two(same_entry, deduplicate, Vec::new()));
        let batch = |time: i64| {
            vec![
                tick(time, 500),
                row(0, time, time, true),
                row(1, time, time, true),
            ]
        };
        let events = live.simulate(10, batch(10));
        assert_eq!(dispositions(&events), [Disposition::Admitted, expected]);
        let first_decision = live.lines.len() - events.len() + 1;
        assert_eq!(
            dispositions(&live.simulate(20, batch(20))),
            [Disposition::Admitted, expected],
            "the previous instant's slots do not bind"
        );
        let mut restored = Live::from_lines(live.lines[..first_decision].to_vec());
        assert_eq!(dispositions(&restored.simulate(10, batch(10))), [expected]);
        let error = Engine::restore(
            tampered(
                &live.lines[..first_decision + 1],
                &format!("\"disposition\":\"{expected}\""),
                "\"disposition\":\"admitted\",\"command\":\"b2/10\",\"reservation\":\"1.00\"",
            )
            .into_iter()
            .map(Ok),
        )
        .err()
        .unwrap();
        assert!(
            error.contains("selection and deduplication slots"),
            "{error}"
        );
    }
    // Selection precedes deduplication: under both policies a deduplicated candidate keeps
    // the selection slot it passed, so a later candidate of another logic at the same duration
    // is a same-entry duplicate, before and after restoration from the prefix.
    let mut live = Live::new(definition(|replay| {
        replay.risk_policies[0].same_entry = SameEntry::First;
        replay.risk_policies[0].deduplicate_signal_logic = true;
        let mut longer = replay.contracts[0].clone();
        longer.id = "d".into();
        longer.duration_micros = 20;
        replay.contracts.push(longer);
        let same_logic = strategy(replay, "t", stream(5, 0));
        replay.strategies.push(same_logic);
        let mut other_logic = strategy(replay, "u", stream(5, 0));
        other_logic.conditions = vec![condition(
            stream(15, 5),
            "other",
            Comparator::Eq,
            Threshold::Bool(true),
        )];
        replay.strategies.push(other_logic);
        let mut second = binding(replay, "b2", "t");
        second.contract = "d".into();
        replay.bindings.push(second);
        let mut third = binding(replay, "b3", "u");
        third.contract = "d".into();
        replay.bindings.push(third);
    }));
    let batch = || vec![tick(10, 500), row(0, 10, 10, true), row(1, 10, 10, true)];
    let events = live.simulate(10, batch());
    assert_eq!(
        dispositions(&events),
        [
            Disposition::Admitted,
            Disposition::DuplicateLogic,
            Disposition::SameEntryDuplicate
        ]
    );
    assert_eq!(live.balances("a").5, 1);
    let after_second = live.lines.len() - events.len() + 2;
    let mut restored = Live::from_lines(live.lines[..after_second].to_vec());
    assert_eq!(
        dispositions(&restored.simulate(10, batch())),
        [Disposition::SameEntryDuplicate]
    );
    // A repair-blocked first candidate claims the slot at each instant.
    let mut live = Live::new(two(SameEntry::First, false, repair));
    for time in [10, 20] {
        assert_eq!(
            dispositions(&live.simulate(
                time,
                vec![
                    tick(time, 500),
                    row(0, time, time, true),
                    row(1, time, time, true)
                ]
            )),
            [Disposition::RepairBlocked, Disposition::SameEntryDuplicate]
        );
    }
}

#[test]
fn every_capacity_scope_cash_and_loss_limits_bind_with_equality_allowed() {
    let policy = |limit: fn(&mut RiskPolicy)| {
        definition(|replay| {
            replay.risk_policies[0].max_open_per_strategy = None;
            limit(&mut replay.risk_policies[0]);
            let second = strategy(replay, "t", stream(15, 5));
            replay.strategies.push(second);
            let second = binding(replay, "b2", "t");
            replay.bindings.push(second);
        })
    };
    type Limit = (&'static str, fn(&mut RiskPolicy), [Disposition; 3]);
    let limits: [Limit; 6] = [
        (
            "strategy",
            |p| p.max_open_per_strategy = Some(1),
            [
                Disposition::Admitted,
                Disposition::Admitted,
                Disposition::CapacityStrategy,
            ],
        ),
        (
            "duration",
            |p| p.max_open_per_duration = Some(1),
            [
                Disposition::Admitted,
                Disposition::CapacityDuration,
                Disposition::CapacityDuration,
            ],
        ),
        (
            "instrument",
            |p| p.max_open_per_instrument = Some(1),
            [
                Disposition::Admitted,
                Disposition::CapacityInstrument,
                Disposition::CapacityInstrument,
            ],
        ),
        (
            "account",
            |p| p.max_open_per_account = Some(1),
            [
                Disposition::Admitted,
                Disposition::CapacityAccount,
                Disposition::CapacityAccount,
            ],
        ),
        (
            "total 1",
            |p| p.max_open_total = Some(1),
            [
                Disposition::Admitted,
                Disposition::CapacityTotal,
                Disposition::CapacityTotal,
            ],
        ),
        (
            "total 2",
            |p| p.max_open_total = Some(2),
            [
                Disposition::Admitted,
                Disposition::Admitted,
                Disposition::CapacityTotal,
            ],
        ),
    ];
    for (name, limit, expected) in limits {
        let mut live = Live::new(policy(limit));
        let mut seen = dispositions(&live.simulate(
            10,
            vec![tick(10, 500), row(0, 10, 10, true), row(1, 10, 10, true)],
        ));
        seen.extend(dispositions(
            &live.simulate(12, vec![tick(12, 500), row(0, 12, 12, true)]),
        ));
        assert_eq!(seen, expected, "{name}");
    }
    // Cash: admission needs native cash minus unpaid reservations to cover `A + F`; equality is
    // allowed, one cent less is not, and zero cash blocks a positive purchase.
    for (cash, expected) in [
        ("2.00", Disposition::Admitted),
        ("1.99", Disposition::InsufficientCash),
        ("0", Disposition::InsufficientCash),
    ] {
        let mut definition = policy(|p| p.max_open_per_strategy = Some(5));
        definition.replay.accounts[0].initial_cash = decimal(cash);
        let mut live = Live::new(definition);
        live.simulate(10, vec![tick(10, 500), row(0, 10, 10, true)]);
        assert_eq!(
            dispositions(&live.simulate(12, vec![tick(12, 500), row(0, 12, 12, true)])),
            [expected],
            "{cash}"
        );
    }
    // Unresolved loss: the prospective worst loss may equal the limit, not exceed it.
    for (limit, expected) in [
        ("2", Disposition::Admitted),
        ("1.99", Disposition::UnresolvedLossAccount),
    ] {
        let mut definition = policy(|p| p.max_open_per_strategy = Some(5));
        definition.replay.risk_policies[0].max_unresolved_loss_per_account = Some(decimal(limit));
        let mut live = Live::new(definition);
        live.simulate(10, vec![tick(10, 500), row(0, 10, 10, true)]);
        assert_eq!(
            dispositions(&live.simulate(12, vec![tick(12, 500), row(0, 12, 12, true)])),
            [expected],
            "{limit}"
        );
    }
    // A stake-one account cannot fund losses beyond its available cash.
    let mut definition = policy(|p| p.max_open_per_strategy = Some(5));
    definition.replay.accounts[0].initial_cash = decimal("2");
    let mut live = Live::new(definition);
    live.simulate(10, vec![tick(10, 500), row(0, 10, 10, true)]);
    live.simulate(11, vec![tick(11, 500), row(0, 11, 11, true)]);
    live.simulate(21, vec![tick(21, 400)]);
    assert_eq!(live.cash(), "0.00");
    assert_eq!(
        dispositions(&live.simulate(22, vec![tick(22, 400), row(0, 22, 22, true)])),
        [Disposition::InsufficientCash]
    );
    live.assert_restorable();
}

/// The exact counterexample: stake 10, cost 9.50, entry fee 0.10, win 19 less 0.20, loss fee
/// 0.30, tie 9.50 less 0.05, in an account holding 19.
fn fractional() -> RunDefinition {
    definition(|replay| {
        let contract = &mut replay.contracts[0];
        contract.stake = decimal("10");
        contract.quoted_cost = decimal("9.50");
        contract.entry_fee = decimal("0.10");
        contract.win = Cashflow {
            gross_return: decimal("19"),
            terminal_fee: decimal("0.20"),
        };
        contract.loss = Cashflow {
            gross_return: decimal("0"),
            terminal_fee: decimal("0.30"),
        };
        contract.tie = Cashflow {
            gross_return: decimal("9.50"),
            terminal_fee: decimal("0.05"),
        };
        replay.bindings[0].envelope = Envelope {
            max_purchase_cost: decimal("9.5"),
            max_entry_fee: decimal("0.1"),
            max_win_terminal_fee: decimal("0.2"),
            max_loss_terminal_fee: decimal("0.3"),
            max_tie_terminal_fee: decimal("0.05"),
            min_winning_net_return: decimal("9.2"),
            settlement_rule: SettlementRule::PriceAtDueV1,
            semantics: None,
        };
        replay.accounts[0].initial_cash = decimal("19");
    })
}

#[test]
fn the_exact_cashflow_counterexample_and_fees_post_exactly() {
    let mut live = Live::new(fractional());
    let events = live.simulate(10, vec![tick(10, 500), row(0, 10, 10, true)]);
    let EventKind::Signal { reservation, .. } = &events[0].kind else {
        unreachable!()
    };
    assert_eq!(
        reservation.unwrap().to_string(),
        "9.90",
        "A + F reserves before dispatch"
    );
    let EventKind::Accepted {
        debit, reservation, ..
    } = &events[1].kind
    else {
        unreachable!()
    };
    assert_eq!(
        (debit.to_string(), reservation.to_string()),
        ("9.60".into(), "0.30".into())
    );
    assert_eq!(
        live.balances("a"),
        (
            "9.40".into(),
            "0.30".into(),
            "9.60".into(),
            "9.90".into(),
            "0.00".into(),
            1
        )
    );
    let events = live.simulate(20, vec![tick(20, 600)]);
    let EventKind::Settled {
        credit,
        profit,
        release,
        ..
    } = &events[0].kind
    else {
        unreachable!()
    };
    assert_eq!(
        (credit.to_string(), profit.to_string(), release.to_string()),
        ("18.80".into(), "9.20".into(), "0.30".into())
    );
    assert_eq!(live.cash(), "28.20");
    // Tie and loss rows post their own fees; the loss fee comes out of the terminal reserve.
    live.simulate(30, vec![tick(30, 600), row(0, 30, 30, true)]);
    let events = live.simulate(40, vec![tick(40, 600)]);
    let EventKind::Settled {
        outcome, profit, ..
    } = &events[0].kind
    else {
        unreachable!()
    };
    assert_eq!(
        (*outcome, profit.to_string()),
        (Outcome::Tie, "-0.15".into())
    );
    live.simulate(50, vec![tick(50, 600), row(0, 50, 50, true)]);
    let events = live.simulate(60, vec![tick(60, 500)]);
    let EventKind::Settled {
        outcome,
        credit,
        profit,
        deficit,
        ..
    } = &events[0].kind
    else {
        unreachable!()
    };
    assert_eq!(
        (*outcome, credit.to_string(), profit.to_string(), *deficit),
        (Outcome::Loss, "-0.30".into(), "-9.90".into(), None)
    );
    assert_eq!(live.cash(), "18.15");
    // A possibly sent reservation of 9.90 cannot fund another purchase: the cash is reserved and
    // the account blocks until reconciliation; a proven not-sent reconciliation releases it.
    let command = Live::command(&live.step(70, vec![tick(70, 500), row(0, 70, 70, true)]));
    live.step(
        71,
        vec![Observation::PossiblySent {
            command: command.clone(),
            source: source("adapter:lost", 71),
        }],
    );
    let account = live.account("a").clone();
    assert_eq!(
        (
            account.reserved.to_string(),
            account.cash.to_string(),
            account.blocked.keys().cloned().collect::<Vec<_>>()
        ),
        ("9.90".into(), "18.15".into(), vec![command.clone()])
    );
    assert_eq!(
        dispositions(&live.simulate(72, vec![tick(72, 500), row(0, 72, 72, true)])),
        [Disposition::AccountBlocked]
    );
    let error = live
        .restored()
        .try_step(
            72,
            vec![Observation::Accepted {
                command: command.clone(),
                source: source("broker:late", 72),
                entry_time_micros: 70,
                entry_price_units: 500,
                price_time_micros: 70,
            }],
        )
        .unwrap_err();
    assert!(
        error.contains("not sent or acknowledged"),
        "a possibly sent command resolves only through reconciliation: {error}"
    );
    let events = live.step(73, vec![reconciliation(&command, 73, Resolution::NotSent)]);
    let EventKind::Reconciled { release, .. } = &events[0].kind else {
        unreachable!()
    };
    assert_eq!(release.to_string(), "9.90");
    assert!(live.account("a").blocked.is_empty());
    assert_eq!(
        live.balances("a"),
        (
            "18.15".into(),
            "0.00".into(),
            "0.00".into(),
            "0.00".into(),
            "-0.85".into(),
            0
        )
    );
    live.assert_restorable();
    // A command left unresolved by the window's end and then reported possibly sent stays one
    // unresolved obligation; its not-sent reconciliation leaves none.
    let mut exhausted = Live::new(fractional());
    let command = Live::command(&exhausted.step(10, vec![tick(10, 500), row(0, 10, 10, true)]));
    assert_eq!(kinds(&exhausted.finish()), ["unresolved"]);
    let mut exhausted = exhausted.restored();
    exhausted.step(
        11,
        vec![Observation::PossiblySent {
            command: command.clone(),
            source: source("adapter:lost-late", 11),
        }],
    );
    assert_eq!(exhausted.engine.summary().portfolio.unresolved, 1);
    exhausted.step(12, vec![reconciliation(&command, 12, Resolution::NotSent)]);
    assert_eq!(
        (
            exhausted.engine.summary().portfolio.unresolved,
            exhausted.engine.summary().portfolio.open
        ),
        (0, 0)
    );
    exhausted.assert_restorable();
    // A known rejection releases without debit; an acknowledgement posts nothing and still
    // permits acceptance; an actual cashflow contradicting the frozen terms is recorded as a
    // discrepancy, and a deficit beyond the remaining reservation is stated, and both block the
    // account until the settled command is reconciled.
    let mut live = Live::new(fractional());
    let command = Live::command(&live.step(10, vec![tick(10, 500), row(0, 10, 10, true)]));
    let events = live.step(
        11,
        vec![Observation::Rejected {
            command,
            source: source("broker:reject", 11),
        }],
    );
    assert_eq!(kinds(&events), ["released"]);
    assert_eq!(
        live.balances("a"),
        (
            "19.00".into(),
            "0.00".into(),
            "0.00".into(),
            "0.00".into(),
            "0.00".into(),
            0
        )
    );
    let command = Live::command(&live.step(20, vec![tick(20, 500), row(0, 20, 20, true)]));
    let events = live.step(
        21,
        vec![Observation::Acknowledged {
            command: command.clone(),
            source: source("broker:ack", 21),
        }],
    );
    assert_eq!(kinds(&events), ["acknowledged"]);
    assert_eq!(
        live.balances("a").1,
        "9.90",
        "an acknowledgement posts nothing"
    );
    let events = live.step(
        22,
        vec![Observation::Accepted {
            command: command.clone(),
            source: source("broker:accept", 22),
            entry_time_micros: 22,
            entry_price_units: 500,
            price_time_micros: 20,
        }],
    );
    assert_eq!(kinds(&events), ["accepted"]);
    let events = live.step(
        30,
        vec![settlement(&command, 30, Outcome::Loss, "0", "0.50", 400)],
    );
    let EventKind::Settled {
        discrepancy,
        credit,
        deficit,
        profit,
        ..
    } = &events[0].kind
    else {
        unreachable!()
    };
    assert_eq!(
        (
            *discrepancy,
            credit.to_string(),
            deficit.map(|d| d.to_string()),
            profit.to_string()
        ),
        (true, "-0.50".into(), Some("0.20".into()), "-10.10".into()),
        "the actual cashflow is recorded, never the configured amount"
    );
    assert_eq!(
        live.balances("a"),
        (
            "8.90".into(),
            "0.00".into(),
            "0.00".into(),
            "0.00".into(),
            "-10.10".into(),
            0
        )
    );
    assert!(live.account("a").blocked.contains_key(&command));
    assert_eq!(
        dispositions(&live.simulate(31, vec![tick(31, 400), row(0, 31, 31, true)])),
        [Disposition::AccountBlocked]
    );
    // A reconciliation contradicting the booked settlement, or proving it not sent, fails and
    // keeps the block: no corrective posting exists.
    for (what, resolution) in [
        (
            "another cashflow",
            Resolution::Settled {
                outcome: Outcome::Win,
                gross_return: decimal("19"),
                terminal_fee: decimal("0.20"),
            },
        ),
        ("not sent", Resolution::NotSent),
    ] {
        let mut fresh = live.restored();
        let error = fresh
            .try_step(32, vec![reconciliation(&command, 32, resolution)])
            .unwrap_err();
        assert!(
            error.contains("contradicts the settlement already booked"),
            "{what}: {error}"
        );
        assert_eq!(fresh.account("a"), live.account("a"));
    }
    // The same settlement with evidence before the entry cannot lift the block either.
    let booked = Resolution::Settled {
        outcome: Outcome::Loss,
        gross_return: decimal("0"),
        terminal_fee: decimal("0.50"),
    };
    let mut fresh = live.restored();
    let error = fresh
        .try_step(32, vec![reconciliation(&command, 21, booked)])
        .unwrap_err();
    assert!(error.contains("precedes its entry or dispatch"), "{error}");
    // Tampered ledgers: the accepted record under the acknowledgement's source identity, an
    // acceptance available after its decision time, and an admission the definition's cash
    // cannot fund all fail restoration.
    for (what, from, to, expected) in [
        (
            "repeated identity",
            "\"broker:accept\"",
            "\"broker:ack\"",
            "already applied",
        ),
        (
            "future availability",
            "\"available_at_micros\":22,",
            "\"available_at_micros\":23,",
            "not available",
        ),
        (
            "unfunded admission",
            "\"initial_cash\":\"19\"",
            "\"initial_cash\":\"9\"",
            "not admissible",
        ),
        (
            "a signal known after its decision",
            "\"known_at_micros\":10,",
            "\"known_at_micros\":11,",
            "clocks its decision could not have seen",
        ),
        (
            "a settlement dated after its source",
            "\"settlement_time_micros\":30,",
            "\"settlement_time_micros\":31,",
            "settlement time disagrees",
        ),
        (
            "a signal under another logic identity",
            "\",\"stream\":{\"duration_seconds\":5,\"offset_seconds\":0},\"close_time_micros\":10,",
            "0\",\"stream\":{\"duration_seconds\":5,\"offset_seconds\":0},\"close_time_micros\":10,",
            "disagrees with its binding's definition",
        ),
    ] {
        let error = Engine::restore(tampered(&live.lines, from, to).into_iter().map(Ok))
            .err()
            .unwrap();
        assert!(error.contains(expected), "{what}: {error}");
    }
    let events = live.step(
        32,
        vec![reconciliation(
            &command,
            32,
            Resolution::Settled {
                outcome: Outcome::Loss,
                gross_return: decimal("0"),
                terminal_fee: decimal("0.50"),
            },
        )],
    );
    let EventKind::Reconciled {
        release,
        debit,
        credit,
        profit,
        ..
    } = &events[0].kind
    else {
        unreachable!()
    };
    assert_eq!(
        (
            release.to_string(),
            debit.to_string(),
            credit.to_string(),
            *profit
        ),
        ("0.00".into(), "0.00".into(), "0.00".into(), None),
        "reconciling a settled discrepancy posts nothing and lifts the block"
    );
    assert_eq!(live.cash(), "8.90");
    assert!(live.account("a").blocked.is_empty());
    live.assert_restorable();
    let error = Engine::restore(
        tampered(
            &live.lines,
            "\"provider_time_micros\":32,\"available_at_micros\":32",
            "\"provider_time_micros\":21,\"available_at_micros\":32",
        )
        .into_iter()
        .map(Ok),
    )
    .err()
    .unwrap();
    assert!(error.contains("precedes its entry or dispatch"), "{error}");
    // Blocks are kept per command: two discrepant settlements block until each is reconciled.
    let mut definition = fractional();
    definition.replay.risk_policies[0].max_open_per_strategy = Some(5);
    definition.replay.accounts[0].initial_cash = decimal("40");
    let mut live = Live::new(definition);
    let first = Live::command(&live.simulate(10, vec![tick(10, 500), row(0, 10, 10, true)]));
    let second = Live::command(&live.simulate(11, vec![tick(11, 500), row(0, 11, 11, true)]));
    live.step(
        15,
        vec![settlement(&first, 15, Outcome::Win, "18", "0.20", 600)],
    );
    live.step(
        16,
        vec![settlement(&second, 16, Outcome::Win, "18", "0.20", 600)],
    );
    assert_eq!(
        live.account("a")
            .blocked
            .keys()
            .cloned()
            .collect::<Vec<_>>(),
        [first.clone(), second.clone()]
    );
    live.step(
        17,
        vec![reconciliation(
            &second,
            17,
            Resolution::Settled {
                outcome: Outcome::Win,
                gross_return: decimal("18"),
                terminal_fee: decimal("0.20"),
            },
        )],
    );
    assert_eq!(
        live.account("a")
            .blocked
            .keys()
            .cloned()
            .collect::<Vec<_>>(),
        std::slice::from_ref(&first)
    );
    assert_eq!(
        dispositions(&live.simulate(18, vec![tick(18, 600), row(0, 18, 18, true)])),
        [Disposition::AccountBlocked]
    );
    live.step(
        19,
        vec![reconciliation(
            &first,
            19,
            Resolution::Settled {
                outcome: Outcome::Win,
                gross_return: decimal("18"),
                terminal_fee: decimal("0.20"),
            },
        )],
    );
    assert!(live.account("a").blocked.is_empty());
    assert_eq!(
        dispositions(&live.simulate(20, vec![tick(20, 600), row(0, 20, 20, true)])),
        [Disposition::Admitted]
    );
    live.assert_restorable();
    // Reconciliation of a sent command as accepted posts the purchase and keeps the terminal
    // reserve; the obligation then settles on ticks like any accepted contract. Reconciliation
    // as settled posts the purchase and the actual cashflow at once.
    let mut live = Live::new(fractional());
    let command = Live::command(&live.step(10, vec![tick(10, 500), row(0, 10, 10, true)]));
    let events = live.step(
        11,
        vec![reconciliation(
            &command,
            11,
            Resolution::Accepted {
                entry_time_micros: 10,
                entry_price_units: 500,
                price_time_micros: 10,
            },
        )],
    );
    let EventKind::Reconciled {
        release,
        debit,
        credit,
        profit,
        ..
    } = &events[0].kind
    else {
        unreachable!()
    };
    assert_eq!(
        (
            release.to_string(),
            debit.to_string(),
            credit.to_string(),
            *profit
        ),
        ("9.60".into(), "9.60".into(), "0.00".into(), None)
    );
    assert_eq!(
        live.balances("a"),
        (
            "9.40".into(),
            "0.30".into(),
            "9.60".into(),
            "9.90".into(),
            "0.00".into(),
            1
        )
    );
    let events = live.simulate(20, vec![tick(20, 600)]);
    assert_eq!(kinds(&events), ["settled"]);
    assert_eq!(
        live.balances("a"),
        (
            "28.20".into(),
            "0.00".into(),
            "0.00".into(),
            "0.00".into(),
            "9.20".into(),
            0
        )
    );
    let command = Live::command(&live.step(30, vec![tick(30, 600), row(0, 30, 30, true)]));
    let events = live.step(
        31,
        vec![reconciliation(
            &command,
            31,
            Resolution::Settled {
                outcome: Outcome::Win,
                gross_return: decimal("19"),
                terminal_fee: decimal("0.20"),
            },
        )],
    );
    let EventKind::Reconciled {
        release,
        debit,
        credit,
        profit,
        ..
    } = &events[0].kind
    else {
        unreachable!()
    };
    assert_eq!(
        (
            release.to_string(),
            debit.to_string(),
            credit.to_string(),
            *profit
        ),
        (
            "9.90".into(),
            "9.60".into(),
            "18.80".into(),
            Some(decimal("9.20"))
        )
    );
    assert_eq!(
        live.balances("a"),
        (
            "37.40".into(),
            "0.00".into(),
            "0.00".into(),
            "0.00".into(),
            "18.40".into(),
            0
        )
    );
    assert_eq!(live.engine.summary().portfolio.wins, 2);
    live.assert_restorable();
}

#[test]
fn quote_envelopes_pauses_conversion_and_projections_are_exact() {
    // Worse terms than the envelope are rejected at admission; equal terms pass.
    let mut live = Live::new(definition(|replay| {
        replay.bindings[0].envelope.min_winning_net_return = decimal("0.93");
    }));
    assert_eq!(
        dispositions(&live.simulate(10, vec![tick(10, 500), row(0, 10, 10, true)])),
        [Disposition::QuoteRejected]
    );
    // A drawdown pause starts when the epoch drawdown reaches the threshold, blocks new entries,
    // continues settlements, and resumes at its deadline with the epoch peak reset.
    let mut live = Live::new(definition(|replay| {
        replay.risk_policies[0].pause = Some(Pause {
            drawdown: decimal("1"),
            duration_micros: 100,
        });
        replay.risk_policies[0].max_open_per_strategy = Some(5);
        replay.contracts[0].settlement.max_tick_gap_micros = 1000;
    }));
    live.simulate(10, vec![tick(10, 500), row(0, 10, 10, true)]);
    live.simulate(11, vec![tick(11, 500), row(0, 11, 11, true)]);
    let events = live.simulate(20, vec![tick(20, 400)]);
    assert_eq!(kinds(&events), ["settled", "pause_started"]);
    let EventKind::PauseStarted {
        until_micros,
        drawdown,
        ..
    } = &events[1].kind
    else {
        unreachable!()
    };
    assert_eq!((*until_micros, drawdown.to_string()), (120, "1.00".into()));
    let events = live.simulate(21, vec![tick(21, 400), row(0, 21, 21, true)]);
    assert_eq!(
        kinds(&events),
        ["settled", "signal"],
        "settlements continue while paused"
    );
    assert_eq!(dispositions(&events), [Disposition::AccountPaused]);
    assert_eq!(live.account("a").completed_profit.to_string(), "-2.00");
    assert_eq!(
        dispositions(&live.simulate(119, vec![tick(119, 400), row(0, 119, 119, true)])),
        [Disposition::AccountPaused]
    );
    let events = live.simulate(120, vec![tick(120, 400), row(0, 120, 120, true)]);
    assert_eq!(kinds(&events), ["pause_ended", "signal", "accepted"]);
    assert_eq!(live.account("a").epoch_peak.to_string(), "-2.00");
    assert_eq!(live.account("a").paused_until_micros, None);
    live.assert_restorable();
    // An expired pause must end before any other record at or after its deadline: a ledger
    // that omits the end fails at the record that follows it.
    let without_end = without(&live.lines, |kind| {
        matches!(kind, EventKind::PauseEnded { .. })
    });
    let error = Engine::restore(without_end.into_iter().map(Ok))
        .err()
        .unwrap();
    assert!(error.contains("end record is required"), "{error}");
    // Two pauses expiring together end in account order; a ledger cut after the first end still
    // holds an expired pause and fails at its end.
    let mut paired = Live::new(definition(|replay| {
        replay.risk_policies[0].pause = Some(Pause {
            drawdown: decimal("1"),
            duration_micros: 100,
        });
        replay.accounts.push(AccountSpec {
            id: "z".into(),
            broker: "b".to_string().try_into().unwrap(),
            currency: "u".to_string().try_into().unwrap(),
            scale: 2,
            initial_cash: decimal("1000"),
        });
        let second = strategy(replay, "t", stream(5, 0));
        replay.strategies.push(second);
        let mut second = binding(replay, "b2", "t");
        second.account = "z".into();
        replay.bindings.push(second);
    }));
    paired.simulate(10, vec![tick(10, 500), row(0, 10, 10, true)]);
    assert_eq!(
        kinds(&paired.simulate(20, vec![tick(20, 400)])),
        ["settled", "pause_started", "settled", "pause_started"]
    );
    assert_eq!(
        kinds(&paired.simulate(120, vec![tick(120, 400)])),
        ["pause_ended", "pause_ended"]
    );
    paired.assert_restorable();
    let cut = paired.lines[..paired.lines.len() - 1].to_vec();
    let error = Engine::restore(cut.into_iter().map(Ok)).err().unwrap();
    assert!(error.contains("has an expired pause"), "{error}");
    // A ledger cut right before a required pause start fails at its end.
    let start = live
        .lines
        .iter()
        .position(|line| line.windows(15).any(|w| w == b"\"pause_started\""))
        .unwrap();
    let error = Engine::restore(live.lines[..start].iter().cloned().map(Ok))
        .err()
        .unwrap();
    assert!(error.contains("still requires its pause record"), "{error}");
    // The pause record is checked against the account's drawdown and policy on application: a
    // shortened deadline fails, a record postponed past the closure that made it due fails, and
    // a ledger that omits the pause and its end fails at the record after that closure.
    let error = Engine::restore(
        tampered(&live.lines, "\"until_micros\":120,", "\"until_micros\":21,")
            .into_iter()
            .map(Ok),
    )
    .err()
    .unwrap();
    assert!(error.contains("pause disagrees"), "{error}");
    let postponed = tampered(
        &tampered(
            &live.lines[..=start],
            "\"time_micros\":20,\"kind\":\"pause_started\"",
            "\"time_micros\":25,\"kind\":\"pause_started\"",
        ),
        "\"until_micros\":120,",
        "\"until_micros\":125,",
    );
    let error = Engine::restore(postponed.into_iter().map(Ok))
        .err()
        .unwrap();
    assert!(
        error.contains("pause record is required next at"),
        "{error}"
    );
    let without_pause = without(&live.lines, |kind| {
        matches!(
            kind,
            EventKind::PauseStarted { .. } | EventKind::PauseEnded { .. }
        )
    });
    let error = Engine::restore(without_pause.into_iter().map(Ok))
        .err()
        .unwrap();
    assert!(error.contains("pause record is required next"), "{error}");
    // A pause omitted before a win that recovers the drawdown below the threshold fails the
    // same way, so no later admission can slip through the recovered drawdown.
    let mut recovering = Live::new(definition(|replay| {
        replay.risk_policies[0].pause = Some(Pause {
            drawdown: decimal("1"),
            duration_micros: 100,
        });
        replay.risk_policies[0].max_open_per_strategy = Some(5);
        replay.contracts[0].settlement.max_tick_gap_micros = 1000;
    }));
    recovering.simulate(10, vec![tick(10, 500), row(0, 10, 10, true)]);
    recovering.simulate(11, vec![tick(11, 500), row(0, 11, 11, true)]);
    assert_eq!(
        kinds(&recovering.simulate(20, vec![tick(20, 400)])),
        ["settled", "pause_started"]
    );
    assert_eq!(
        kinds(&recovering.simulate(21, vec![tick(21, 600)])),
        ["settled"]
    );
    assert_eq!(
        recovering.account("a").completed_profit.to_string(),
        "-0.08"
    );
    let without_pause = without(&recovering.lines, |kind| {
        matches!(kind, EventKind::PauseStarted { .. })
    });
    let error = Engine::restore(without_pause.into_iter().map(Ok))
        .err()
        .unwrap();
    assert!(error.contains("pause record is required next"), "{error}");
    // Conversion: exact same-currency rescaling, and a supplied rate only when its provider and
    // availability times are no later than the decision and its provider age is within bound.
    let v: Currency = "v".to_string().try_into().unwrap();
    let u: Currency = "u".to_string().try_into().unwrap();
    let rate = RateEvent {
        id: "r1".into(),
        source_currency: v.clone(),
        reporting_currency: u.clone(),
        provider: "fx".into(),
        provider_time: "1970-01-01T00:00:00.000100Z".into(),
        available_at: "1970-01-01T00:00:00.000103Z".into(),
        rate: decimal("2.5"),
    };
    let mut live = Live::new(definition(|replay| replay.rates = Some(vec![rate.clone()])));
    live.simulate(100, vec![tick(100, 500)]);
    assert_eq!(
        live.engine
            .convert(decimal("1.5"), &u)
            .unwrap()
            .unwrap()
            .amount
            .to_string(),
        "1.50"
    );
    assert_eq!(
        live.engine.convert(decimal("1"), &v).unwrap(),
        None,
        "future availability"
    );
    live.simulate(103, vec![tick(103, 500)]);
    let converted = live.engine.convert(decimal("1.5"), &v).unwrap().unwrap();
    assert_eq!(
        (converted.amount.to_string(), converted.rate),
        ("3.75".into(), Some("r1".into()))
    );
    assert!(
        live.engine
            .convert(decimal("1.111"), &v)
            .unwrap_err()
            .contains("loses precision")
    );
    live.simulate(106, vec![tick(106, 500)]);
    assert_eq!(
        live.engine.convert(decimal("1"), &v).unwrap(),
        None,
        "stale beyond the maximum age"
    );
    // A foreign-currency account: the portfolio projection is observed at the start and after
    // every account-changing record; without a usable rate the observation is unavailable, and
    // a total unresolved-loss limit needing that rate blocks admission explicitly.
    let foreign = |limit: Option<Decimal>| {
        definition(|replay| {
            replay.rates = Some(vec![rate.clone()]);
            replay.accounts.push(AccountSpec {
                id: "z".into(),
                broker: "b".to_string().try_into().unwrap(),
                currency: v.clone(),
                scale: 2,
                initial_cash: decimal("1000"),
            });
            replay.risk_policies[0].max_unresolved_loss_total = limit;
        })
    };
    let mut live = Live::new(foreign(None));
    let reporting = live.engine.summary().reporting.clone();
    assert_eq!(
        (
            reporting.observations,
            reporting.unavailable_observations,
            reporting.settled_equity
        ),
        (0, 1, None),
        "the definition record is observed without a rate"
    );
    // Rates are observed at their own availability times, between market steps: 2.5 at 103
    // then 1 at 106 without any account posting show the peak 3500 and the drawdown 1500.
    let mut two_rates = foreign(None);
    two_rates.replay.rates.as_mut().unwrap().push(RateEvent {
        id: "r2".into(),
        provider_time: "1970-01-01T00:00:00.000105Z".into(),
        available_at: "1970-01-01T00:00:00.000106Z".into(),
        rate: decimal("1"),
        ..rate.clone()
    });
    let two_rates_definition = two_rates.clone();
    let mut between = Live::new(two_rates);
    let events = between.simulate(110, vec![tick(110, 500)]);
    assert_eq!(kinds(&events), ["rate_available", "rate_available"]);
    assert_eq!(
        events.iter().map(|e| e.time_micros).collect::<Vec<_>>(),
        [103, 106]
    );
    let reporting = between.engine.summary().reporting.clone();
    assert_eq!(
        (
            reporting.settled_equity.map(|e| e.to_string()),
            reporting.peak_equity.map(|e| e.to_string()),
            reporting.max_drawdown.map(|e| e.to_string())
        ),
        (
            Some("2000.00".into()),
            Some("3500.00".into()),
            Some("1500.00".into())
        )
    );
    between.assert_restorable();
    // A rate record is at its availability or the first later record time and precedes any other
    // record at that time: a postponed or omitted rate record fails restoration.
    let error = Engine::restore(
        tampered(
            &between.lines,
            "\"time_micros\":103,\"kind\":\"rate_available\"",
            "\"time_micros\":106,\"kind\":\"rate_available\"",
        )
        .into_iter()
        .map(Ok),
    )
    .err()
    .unwrap();
    assert!(error.contains("must be observed at"), "{error}");
    let error = Engine::restore(
        without(
            &between.lines,
            |kind| matches!(kind, EventKind::RateAvailable { rate } if rate == "r1"),
        )
        .into_iter()
        .map(Ok),
    )
    .err()
    .unwrap();
    assert!(error.contains("must be observed at"), "{error}");
    let mut tail = Live::new(two_rates_definition);
    tail.simulate(100, vec![tick(100, 500)]);
    let events = tail.finish();
    assert_eq!(
        events.iter().map(|e| e.time_micros).collect::<Vec<_>>(),
        [103, 106],
        "rates after the last market observation are observed by the decision end"
    );
    assert_eq!(
        tail.engine
            .summary()
            .reporting
            .max_drawdown
            .map(|d| d.to_string()),
        Some("1500.00".into())
    );
    tail.assert_restorable();
    // A pause that expires before a tail rate ends when the rate observation advances the clock.
    let mut paused = Live::new(definition(|replay| {
        replay.risk_policies[0].pause = Some(Pause {
            drawdown: decimal("1"),
            duration_micros: 100,
        });
        replay.rates = Some(vec![RateEvent {
            id: "late".into(),
            provider_time: "1970-01-01T00:00:00.000125Z".into(),
            available_at: "1970-01-01T00:00:00.000130Z".into(),
            ..rate.clone()
        }]);
    }));
    paused.simulate(10, vec![tick(10, 500), row(0, 10, 10, true)]);
    assert_eq!(
        kinds(&paused.simulate(20, vec![tick(20, 400)])),
        ["settled", "pause_started"]
    );
    let events = paused.finish();
    assert_eq!(kinds(&events), ["pause_ended", "rate_available"]);
    assert_eq!(
        events.iter().map(|e| e.time_micros).collect::<Vec<_>>(),
        [130, 130]
    );
    assert_eq!(paused.account("a").paused_until_micros, None);
    paused.assert_restorable();
    let cut = paused.lines[..paused.lines.len() - 1].to_vec();
    let error = Engine::restore(cut.into_iter().map(Ok)).err().unwrap();
    assert!(error.contains("ends while rate"), "{error}");
    assert!(
        live.simulate(100, vec![tick(100, 500)]).is_empty(),
        "the rate's provider time is not its availability"
    );
    let events = live.simulate(103, vec![tick(103, 500), row(0, 103, 103, true)]);
    assert_eq!(
        kinds(&events),
        ["rate_available", "signal", "accepted"],
        "a rate becoming available is observed before any decision at that time"
    );
    let reporting = live.engine.summary().reporting.clone();
    assert_eq!(
        (
            reporting.observations,
            reporting.settled_equity.map(|e| e.to_string()),
            reporting.peak_equity.map(|e| e.to_string()),
            reporting.max_drawdown.map(|e| e.to_string()),
            reporting.used_rates.iter().cloned().collect::<Vec<_>>()
        ),
        (
            3,
            Some("3500.00".into()),
            Some("3500.00".into()),
            Some("0.00".into()),
            vec!["r1".to_string()]
        )
    );
    let events = live.simulate(113, vec![tick(113, 400)]);
    assert_eq!(kinds(&events), ["settled"]);
    let reporting = live.engine.summary().reporting.clone();
    assert_eq!(
        (
            reporting.observations,
            reporting.unavailable_observations,
            reporting.settled_equity
        ),
        (3, 2, None),
        "a stale rate leaves the settlement's observation unavailable, never native history"
    );
    live.assert_restorable();
    let mut live = Live::new(foreign(Some(decimal("100"))));
    assert_eq!(
        dispositions(&live.simulate(50, vec![tick(50, 500), row(0, 50, 50, true)])),
        [Disposition::ConversionUnavailable]
    );
    let events = live.simulate(103, vec![tick(103, 500), row(0, 103, 103, true)]);
    assert_eq!(dispositions(&events), [Disposition::Admitted]);
    let EventKind::Signal { rates, .. } = &events[1].kind else {
        unreachable!()
    };
    assert_eq!(
        rates,
        &["r1".to_string()],
        "the admission records the rate it used"
    );
    // An aggregate the reporting projection cannot hold is an unavailable observation; the
    // native settlement it follows is recorded and restorable.
    let half = "850705917302346158658436518579420528.63";
    let mut vast = Live::new(definition(|replay| {
        replay.accounts[0].initial_cash = decimal(half);
        replay.accounts.push(AccountSpec {
            id: "z".into(),
            broker: "b".to_string().try_into().unwrap(),
            currency: "u".to_string().try_into().unwrap(),
            scale: 2,
            initial_cash: decimal(half),
        });
    }));
    assert_eq!(vast.engine.summary().reporting.observations, 1);
    vast.simulate(10, vec![tick(10, 500), row(0, 10, 10, true)]);
    let events = vast.simulate(20, vec![tick(20, 600)]);
    assert_eq!(kinds(&events), ["settled"]);
    let reporting = vast.engine.summary().reporting.clone();
    assert_eq!(
        (reporting.unavailable_observations, reporting.settled_equity),
        (1, None)
    );
    assert_eq!(vast.account("a").completed_profit.to_string(), "0.92");
    vast.assert_restorable();
    // A grouped profit the currency's range cannot hold is unavailable; every native settlement
    // and account stands. Two wins of (i128::MAX - 1) / 2 + 1 each exceed i128::MAX together.
    let mut grouped = Live::new(definition(|replay| {
        replay.reporting_scale = 0;
        replay.accounts[0].scale = 0;
        replay.accounts[0].initial_cash = decimal("2");
        replay.accounts.push(AccountSpec {
            id: "z".into(),
            broker: "b".to_string().try_into().unwrap(),
            currency: "u".to_string().try_into().unwrap(),
            scale: 0,
            initial_cash: decimal("2"),
        });
        replay.contracts[0].win.gross_return = decimal("85070591730234615865843651857942052865");
        replay.bindings[0].envelope.min_winning_net_return = decimal("1");
        let second = strategy(replay, "t", stream(5, 0));
        replay.strategies.push(second);
        let mut second = binding(replay, "b2", "t");
        second.account = "z".into();
        replay.bindings.push(second);
    }));
    grouped.simulate(10, vec![tick(10, 500), row(0, 10, 10, true)]);
    let events = grouped.simulate(20, vec![tick(20, 600)]);
    assert_eq!(kinds(&events), ["settled", "settled"]);
    let summary = grouped.engine.summary().clone();
    assert_eq!(summary.portfolio.profit.get("u"), Some(&None));
    assert_eq!(
        summary.strategies["b2"].profit["u"].map(|profit| profit.to_string()),
        Some("85070591730234615865843651857942052864".into())
    );
    assert_eq!(
        (grouped.cash(), grouped.account("z").cash.to_string()),
        (
            "85070591730234615865843651857942052866".into(),
            "85070591730234615865843651857942052866".into()
        )
    );
    grouped.assert_restorable();
    // A zero entry price still settles and records exact movement; only the normalized
    // excursion is absent with its reason.
    let mut live = Live::new(definition(|_| {}));
    live.simulate(10, vec![tick(10, 0), row(0, 10, 10, true)]);
    let events = live.simulate(20, vec![tick(20, 7)]);
    let EventKind::Settled {
        outcome,
        path: Some(path),
        ..
    } = &events[0].kind
    else {
        unreachable!()
    };
    assert_eq!(
        (*outcome, path.final_move_units, path.max_favorable_units),
        (Outcome::Win, 7, 7)
    );
    assert!(
        basis_points_text(path.max_favorable_units, 0)
            .unwrap_err()
            .contains("zero entry price")
    );
    assert_eq!(basis_points_text(7, 700).unwrap(), "100.0000000000");
}

#[test]
fn two_instruments_and_the_decision_window_are_independent() {
    let mut definition = definition(|replay| {
        replay.inputs.push(ReplayInput {
            tick_manifest: "file:///p/manifests/3333333333333333333333333333333333333333333333333333333333333333/ready.json".parse().unwrap(),
            feature_manifest: replay.inputs[0].feature_manifest.clone(),
            outcome_manifest: None,
        });
        let mut second = strategy(replay, "t", stream(5, 0));
        second.plan_identity = "plan2".into();
        replay.strategies.push(second);
        let mut second = binding(replay, "b2", "t");
        second.instrument = "b:Y".into();
        replay.bindings.push(second);
        replay.decision_end = "1970-01-01T00:00:00.000100Z".into();
    });
    definition.instruments.push(instrument("b:Y", '3', "plan2"));
    let mut live = Live::new(definition);
    // Quotes are per instrument: the second instrument's tick is no quote for the first.
    let events = live.simulate(
        10,
        vec![
            tick_of(1, 10, 900),
            row(0, 10, 10, true),
            row_of(1, 0, 10, 10, vec![Some(Value::Bool(true))]),
        ],
    );
    assert_eq!(
        dispositions(&events),
        [Disposition::NoQuote, Disposition::Admitted]
    );
    assert_eq!(live.balances("a").5, 1);
    // The first instrument's ticks do not drive the second instrument's obligation.
    assert!(live.simulate(20, vec![tick(20, 500)]).is_empty());
    let events = live.simulate(21, vec![tick_of(1, 21, 950)]);
    assert_eq!(kinds(&events), ["settled"]);
    assert_eq!(live.account("a").completed_profit.to_string(), "0.92");
    // Rows at or after the decision end produce no signal.
    assert_eq!(
        dispositions(&live.simulate(70, vec![tick(70, 500), row(0, 70, 70, true)])),
        [Disposition::Admitted]
    );
    let events = live.simulate(100, vec![tick(100, 500), row(0, 100, 100, true)]);
    assert_eq!(
        kinds(&events),
        ["unresolved"],
        "the late tick, and no signal at the window end"
    );
    live.assert_restorable();
}

#[test]
fn restored_engines_continue_byte_identically() {
    let mut live = Live::new(definition(|replay| {
        replay.risk_policies[0].max_open_per_strategy = Some(3)
    }));
    live.simulate(10, vec![tick(10, 500), row(0, 10, 10, true)]);
    live.simulate(11, vec![tick(11, 501), row(0, 11, 11, true)]);
    let events = live.simulate(80, vec![tick(80, 505)]);
    assert_eq!(kinds(&events), ["unresolved", "unresolved"]);
    let mut restored = live.restored();
    // The same later observations: the retained obligations are not driven by ticks, the
    // authoritative settlement resolves one, and every record matches byte for byte.
    let later: Vec<(i64, Vec<Observation>)> = vec![
        (90, vec![tick(90, 600), row(0, 90, 90, true)]),
        (100, vec![tick(100, 610)]),
        (
            110,
            vec![settlement("b1/10", 105, Outcome::Win, "1.92", "0", 610)],
        ),
        (111, vec![tick(111, 610), row(0, 111, 111, true)]),
    ];
    for (time, observations) in later {
        let expected = live.simulate(time, observations.clone());
        let actual = restored.simulate(time, observations);
        assert_eq!(
            actual
                .iter()
                .map(FinancialEvent::to_line)
                .collect::<Vec<_>>(),
            expected
                .iter()
                .map(FinancialEvent::to_line)
                .collect::<Vec<_>>()
        );
    }
    assert_eq!(live.lines, restored.lines);
    assert_eq!(
        live.engine.state_identity(),
        restored.engine.state_identity()
    );
    assert_eq!(
        live.balances("a").5,
        2,
        "one retained and one new obligation"
    );
    assert_eq!(live.engine.summary().portfolio.unresolved, 1);
    // An authoritative settlement's path is the ledger-recorded path (empty right after the
    // acceptance) plus its own price, so an engine restored before any record captured the
    // ticks it saw settles byte for byte like the uninterrupted one.
    let mut live = Live::new(definition(|_| {}));
    live.simulate(10, vec![tick(10, 500), row(0, 10, 10, true)]);
    assert!(live.simulate(15, vec![tick(15, 600)]).is_empty());
    let mut restored = live.restored();
    let authoritative = || {
        vec![Observation::Settlement {
            command: "b1/10".into(),
            source: EventSource {
                id: "broker:settle-early".into(),
                provider_time_micros: 20,
                available_at_micros: 21,
                simulated: false,
            },
            outcome: Outcome::Win,
            gross_return: decimal("1.92"),
            terminal_fee: decimal("0"),
            settlement_price_units: 510,
        }]
    };
    let expected = live.step(21, authoritative());
    let actual = restored.step(21, authoritative());
    assert_eq!(
        actual
            .iter()
            .map(FinancialEvent::to_line)
            .collect::<Vec<_>>(),
        expected
            .iter()
            .map(FinancialEvent::to_line)
            .collect::<Vec<_>>()
    );
    let EventKind::Settled {
        path: Some(path), ..
    } = &expected[0].kind
    else {
        unreachable!()
    };
    assert_eq!(
        (path.max_favorable_units, path.max_favorable_time_micros),
        (10, 20)
    );
    // A row already decided, redelivered to a restored engine whose row cursors are empty, is
    // installed but never decided twice: the completed command is not dispatched again.
    let mut completed = Live::new(definition(|_| {}));
    completed.simulate(10, vec![tick(10, 500), row(0, 10, 10, true)]);
    assert_eq!(
        kinds(&completed.simulate(20, vec![tick(20, 520)])),
        ["settled"]
    );
    let mut restored = completed.restored();
    for engine in [&mut completed, &mut restored] {
        assert!(
            engine
                .simulate(21, vec![tick(21, 520), row(0, 10, 10, true)])
                .is_empty()
        );
        assert_eq!(engine.balances("a").5, 0);
    }
}

#[test]
fn definitions_reject_mismatched_plans_columns_and_negative_cash_and_accept_many_conditions() {
    let rejected = |edit: fn(&mut Replay), expected: &str| {
        let error = Engine::new(definition(edit)).err().unwrap();
        assert!(error.contains(expected), "{error}");
    };
    rejected(
        |replay| replay.strategies[0].plan_identity = "other".into(),
        "no replay input carries frozen plan",
    );
    rejected(
        |replay| replay.strategies[0].conditions[0].output = "missing".into(),
        "not a compiled output or fitted encoding",
    );
    rejected(
        |replay| replay.strategies[0].conditions[0].threshold = Threshold::Text("yes".into()),
        "threshold type does not match",
    );
    rejected(
        |replay| replay.accounts[0].initial_cash = decimal("-1"),
        "initial_cash",
    );
    rejected(
        |replay| replay.contracts[0].stake = decimal("0.001"),
        "stake 0.001 loses precision",
    );
    // Five conditions over distinct columns and comparators: one failing condition, and only
    // that one, blocks the conjunction.
    let mut live = Live::new(definition(|replay| {
        let other = stream(15, 5);
        replay.strategies[0].conditions.extend([
            condition(other, "signal", Comparator::Eq, Threshold::Bool(true)),
            condition(other, "other", Comparator::Ne, Threshold::Bool(false)),
            condition(other, "count", Comparator::Ge, Threshold::Number(4.0)),
            condition(other, "count", Comparator::Lt, Threshold::Number(6.0)),
        ]);
    }));
    live.simulate(9, vec![row(1, 9, 9, true)]);
    assert_eq!(
        dispositions(&live.simulate(10, vec![tick(10, 500), row(0, 10, 10, true)])),
        [Disposition::Admitted]
    );
    let count = |close: i64, count: i64, ready: bool| {
        row_of(
            0,
            1,
            close,
            close,
            vec![
                Some(Value::Bool(true)),
                Some(Value::Bool(true)),
                Some(Value::Int(count)),
                Some(Value::Bool(ready)),
                Some(Value::Text(std::borrow::Cow::Borrowed("flat"))),
            ],
        )
    };
    live.simulate(15, vec![count(15, 6, true)]);
    assert!(
        live.simulate(16, vec![tick(16, 500), row(0, 16, 16, true)])
            .is_empty(),
        "only the fifth condition fails"
    );
    live.simulate(20, vec![count(20, 5, false)]);
    assert!(
        dispositions(&live.simulate(21, vec![tick(21, 500), row(0, 21, 21, true)])).is_empty(),
        "a value whose readiness flag is false is not ready, whatever it reads"
    );
    live.simulate(25, vec![count(25, 5, true)]);
    assert_eq!(
        dispositions(&live.simulate(26, vec![tick(26, 500), row(0, 26, 26, true)])),
        [Disposition::Admitted],
        "all five hold again"
    );
    // A text value the feature owner declares not ready fails its condition before any
    // comparison, even one that `not_ready` would otherwise satisfy.
    let mut live = Live::new(definition(|replay| {
        replay.strategies[0].conditions.push(condition(
            stream(15, 5),
            "state",
            Comparator::Ne,
            Threshold::Text("up".into()),
        ));
    }));
    let state = |close: i64, state: &'static str| {
        row_of(
            0,
            1,
            close,
            close,
            vec![
                Some(Value::Bool(true)),
                Some(Value::Bool(true)),
                Some(Value::Int(4)),
                Some(Value::Bool(true)),
                Some(Value::Text(std::borrow::Cow::Borrowed(state))),
            ],
        )
    };
    live.simulate(9, vec![state(9, "not_ready")]);
    assert!(dispositions(&live.simulate(10, vec![tick(10, 500), row(0, 10, 10, true)])).is_empty());
    live.simulate(15, vec![state(15, "flat")]);
    assert_eq!(
        dispositions(&live.simulate(16, vec![tick(16, 500), row(0, 16, 16, true)])),
        [Disposition::Admitted]
    );
}

// ---------------------------------------------------------------------------------------------
// Governed reference comparison
// ---------------------------------------------------------------------------------------------

#[derive(serde::Deserialize)]
struct GovernedConfig {
    /// The research configuration `replay` runs: filesystem publication and the permitted
    /// development tick, feature, and outcome generations.
    application_config: PathBuf,
    /// The legacy reference root whose expected files the checked-in allowlist names.
    reference_root: PathBuf,
}

#[derive(serde::Deserialize)]
struct ReferenceFixture {
    legacy_revision: String,
    source_sha256: String,
    source_rows: u64,
    receipt: Receipt,
    reference_files: Vec<ReferenceFile>,
    rows: BTreeMap<String, u64>,
    run: serde_json::Value,
    totals: BTreeMap<String, serde_json::Value>,
    candidates: Vec<Candidate>,
    fields: BTreeMap<String, BTreeMap<String, serde_json::Value>>,
}

#[derive(serde::Deserialize)]
struct Receipt {
    sha256: String,
    source_revision: String,
    run_revision: String,
    elapsed_seconds: f64,
    feature_engine_seconds: f64,
}

#[derive(serde::Deserialize)]
struct ReferenceFile {
    path: String,
    sha256: String,
}

#[derive(serde::Deserialize)]
struct Candidate {
    rank: usize,
    candidate_id: String,
    candle_set: String,
    expiry_seconds: u32,
    direction: String,
    output: String,
    value: String,
    strategy: String,
}

fn micros(text: &str) -> i64 {
    binary_alpha_engine::market::parse_event_time_micros(text).unwrap()
}

/// The legacy path updater in its own floating-point arithmetic: an unrounded movement compared
/// with the previously rounded stored maximum, ten-place rounding, and the `.10f` rendering.
struct LegacyPath {
    final_move: f64,
    mfe: f64,
    mae: f64,
    mfe_time: i64,
    mae_time: i64,
}

fn round10(value: f64) -> f64 {
    format!("{value:.10}").parse().unwrap()
}

fn legacy_path(
    times: &[i64],
    prices: &[i64],
    entry: usize,
    settlement: usize,
    sell: bool,
) -> LegacyPath {
    let entry_price = prices[entry] as f64 / 1e6;
    let mut path = LegacyPath {
        final_move: 0.0,
        mfe: 0.0,
        mae: 0.0,
        mfe_time: times[entry],
        mae_time: times[entry],
    };
    for index in entry + 1..=settlement {
        let raw = if entry_price <= 0.0 {
            0.0
        } else {
            (prices[index] as f64 / 1e6 - entry_price) / entry_price * 10_000.0
        };
        let movement = if sell { -raw } else { raw };
        path.final_move = round10(movement);
        if movement > path.mfe {
            path.mfe = round10(movement);
            path.mfe_time = times[index];
        }
        let adverse = (-movement).max(0.0);
        if adverse > path.mae {
            path.mae = round10(adverse);
            path.mae_time = times[index];
        }
    }
    path
}

/// The target's exact path over the same window, in integer units.
fn exact_path(
    times: &[i64],
    prices: &[i64],
    entry: usize,
    settlement: usize,
    sell: bool,
) -> binary_alpha_engine::execution::PathMetrics {
    let mut path = binary_alpha_engine::execution::PathMetrics::new(times[entry]);
    for index in entry + 1..=settlement {
        let delta = prices[index] - prices[entry];
        path.observe(times[index], if sell { -delta } else { delta });
    }
    path
}

/// One target signal record with the state the ledger held when it was decided.
struct TargetSignal {
    stream: StreamKey,
    disposition: Disposition,
    known_at: i64,
    quote: i64,
    split: Option<String>,
    command: Option<String>,
    /// Native cash less unpaid reservations before this signal.
    available_before: Decimal,
    /// The command holding the binding's strategy slot at decision time, and whether it was
    /// unresolved then.
    holder: Option<(String, bool)>,
}

struct TargetSettlement {
    time: i64,
    price: i64,
    outcome: Outcome,
    path: PathMetrics,
}

/// The retained evidence of the governed comparison: every classified divergence and every
/// legacy-rounding correction, one tab-separated line each, in a file named by the replay
/// generation and the code revision it was produced at.
struct Evidence {
    writer: std::io::BufWriter<fs::File>,
    path: PathBuf,
    retained: u64,
    classes: BTreeMap<&'static str, u64>,
    examples: BTreeMap<&'static str, String>,
}

impl Evidence {
    fn create(manifest: &ReplayManifest) -> Self {
        use std::io::Write;
        let path = Path::new(env!("CARGO_TARGET_TMPDIR")).join(format!(
            "phase06_reference_divergences_{}_{}.tsv",
            &manifest.generation[..16],
            manifest.code_revision
        ));
        let mut writer = std::io::BufWriter::new(fs::File::create(&path).unwrap());
        writeln!(writer, "signal_number\tclass\tcitation").unwrap();
        Self {
            writer,
            path,
            retained: 0,
            classes: BTreeMap::new(),
            examples: BTreeMap::new(),
        }
    }

    fn retain(&mut self, number: u64, class: &str, citation: &str) {
        use std::io::Write;
        writeln!(self.writer, "{number}\t{class}\t{citation}").unwrap();
        self.retained += 1;
    }

    /// Counts one signal's disposition class, retaining every class that is not a match.
    fn classify(&mut self, number: u64, class: &'static str, citation: String) -> &'static str {
        *self.classes.entry(class).or_default() += 1;
        if !class.starts_with("matched_") {
            self.retain(number, class, &citation);
        }
        self.examples.entry(class).or_insert(citation);
        class
    }
}

/// Reference rows keyed by signal number, each kept as one unit-separated line.
fn index_rows(csv: &mut LegacyCsv) -> HashMap<u64, String> {
    let signal_column = csv.column("signal_number");
    let mut rows = HashMap::new();
    while let Some(row) = csv.next_row() {
        let number = row[signal_column].parse().unwrap();
        assert!(rows.insert(number, row.join("\u{1f}")).is_none());
    }
    rows
}

#[test]
#[ignore = "needs BINARY_ALPHA_TEST_CONFIG naming the research configuration and the reference root"]
fn governed_reference_parity() {
    use binary_alpha_engine::market::{PriceScale, parse_price_units};
    use binary_alpha_engine::outcomes::{
        InvalidReason, OutcomeBuilder, OutcomeManifest, TICK_PRICE_OBJECT_PATH,
        TICK_TIME_OBJECT_PATH, stream_object_paths,
    };
    use std::io::Write;

    let config_path = std::env::var("BINARY_ALPHA_TEST_CONFIG")
        .expect("BINARY_ALPHA_TEST_CONFIG names the governed test configuration");
    let governed: GovernedConfig =
        serde_json::from_slice(&fs::read(&config_path).unwrap()).unwrap();
    let fixture: ReferenceFixture = serde_json::from_slice(
        &fs::read(
            Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/phase06_reference.json"),
        )
        .unwrap(),
    )
    .unwrap();
    let root = &governed.reference_root;

    // Every allowlisted reference file carries exactly its recorded byte fingerprint, and the
    // files have the recorded rows and header widths, every header field classified.
    let started = std::time::Instant::now();
    for file in &fixture.reference_files {
        let path = root.join(&file.path);
        assert_eq!(sha256(&path), file.sha256, "{}", path.display());
    }
    let count = |name: &str| {
        let mut csv = LegacyCsv::open(&root.join(format!("parity_cpu_run_v2/{name}.csv")));
        let mut rows = 0;
        while csv.next_row().is_some() {
            rows += 1;
        }
        (csv.header.len(), rows)
    };
    for (name, width) in [("signals", 40), ("trades", 43), ("invalid_trades", 30)] {
        let (fields, rows) = count(name);
        assert_eq!((fields, rows), (width, fixture.rows[name]), "{name}");
        let mapping = &fixture.fields[name];
        let header = LegacyCsv::open(&root.join(format!("parity_cpu_run_v2/{name}.csv"))).header;
        for column in &header {
            let class = mapping
                .get(column)
                .unwrap_or_else(|| panic!("{name}: field {column} has no mapping"))["class"]
                .as_str()
                .unwrap();
            assert!(
                matches!(class, "shared" | "derived" | "diagnostic"),
                "{name}: field {column} has class {class}"
            );
        }
        assert_eq!(
            mapping.len(),
            header.len(),
            "{name}: every mapped field is a header"
        );
    }
    let mut candidates = LegacyCsv::open(&root.join("parity_search_local_v2/candidates.csv"));
    let mut frozen = Vec::new();
    while let Some(row) = candidates.next_row() {
        frozen.push((
            row[candidates.column("candidate_id")].clone(),
            row[candidates.column("candle_set")].clone(),
            row[candidates.column("expiry_seconds")]
                .parse::<u32>()
                .unwrap(),
            row[candidates.column("direction")].clone(),
            row[candidates.column("predicate")].clone(),
            row[candidates.column("params_json")].clone(),
        ));
    }
    assert_eq!(frozen.len(), fixture.rows["candidates"] as usize);
    for (position, (candidate, (id, candle_set, expiry, direction, predicate, params))) in
        fixture.candidates.iter().zip(&frozen).enumerate()
    {
        assert_eq!(
            candidate.rank,
            position + 1,
            "candidate physical order is preserved"
        );
        assert_eq!(
            (
                &candidate.candidate_id,
                &candidate.candle_set,
                candidate.expiry_seconds,
                &candidate.direction
            ),
            (id, candle_set, *expiry, direction)
        );
        assert_eq!(
            predicate,
            &format!("{} == '{}'", candidate.output, candidate.value)
        );
        let params: serde_json::Value =
            serde_json::from_str(params).expect("the quoted JSON field parses");
        assert_eq!(
            params["predicate"],
            serde_json::Value::String(predicate.clone())
        );
        assert_eq!(params["max_open_trades_per_strategy"], 1);
    }
    println!(
        "reference allowlist: {} files hashed in {:.1} s against legacy revision {}; receipt {} recorded at source {} for run {} ({:.3} s historical elapsed)",
        fixture.reference_files.len(),
        started.elapsed().as_secs_f64(),
        fixture.legacy_revision,
        fixture.receipt.sha256,
        fixture.receipt.source_revision,
        fixture.receipt.run_revision,
        fixture.receipt.elapsed_seconds
    );

    // The application configuration freezes the translated candidates in physical order before
    // the comparison, binds the registered source, and declares the fixture economics.
    let config = Config::parse(&fs::read_to_string(&governed.application_config).unwrap()).unwrap();
    let settings = config.replay.as_ref().unwrap();
    let local = |uri: &str| PathBuf::from(uri.strip_prefix("file://").unwrap());
    assert_eq!(settings.inputs.len(), 1);
    let tick = GenerationManifest::from_json(
        &fs::read(local(&settings.inputs[0].tick_manifest.to_string())).unwrap(),
    )
    .unwrap();
    assert_eq!(tick.row_count, fixture.source_rows);
    assert_eq!(
        tick.inputs[0].sha256, fixture.source_sha256,
        "imported from the registered source"
    );
    assert_eq!(settings.bindings.len(), fixture.candidates.len());
    let run = &fixture.run;
    let mut contract_of: HashMap<&str, &ContractTerms> = HashMap::new();
    for (binding, candidate) in settings.bindings.iter().zip(&fixture.candidates) {
        assert_eq!(binding.id, candidate.candidate_id);
        assert_eq!(binding.strategy, candidate.strategy);
        let strategy = settings
            .strategies
            .iter()
            .find(|s| s.id == binding.strategy)
            .unwrap();
        assert_eq!(strategy.base_stream, stream(30, 15));
        assert_eq!(strategy.conditions.len(), 1);
        assert_eq!(strategy.conditions[0].output, candidate.output);
        assert_eq!(
            strategy.conditions[0].threshold,
            Threshold::Text(candidate.value.clone())
        );
        let contract = settings
            .contracts
            .iter()
            .find(|c| c.id == binding.contract)
            .unwrap();
        contract_of.insert(&binding.id, contract);
        assert_eq!(
            contract.duration_micros,
            i64::from(candidate.expiry_seconds) * 1_000_000
        );
        assert_eq!(
            contract.direction.to_string(),
            candidate.direction.to_lowercase()
        );
        assert_eq!(
            (
                contract.stake.to_string(),
                contract.quoted_cost.to_string(),
                contract.entry_fee.to_string(),
                contract.win.gross_return.to_string(),
                contract.win.terminal_fee.to_string(),
                contract.loss.gross_return.to_string(),
                contract.loss.terminal_fee.to_string(),
                contract.tie.gross_return.to_string(),
                contract.tie.terminal_fee.to_string()
            ),
            (
                "1".into(),
                "1".into(),
                "0".into(),
                "1.92".into(),
                "0".into(),
                "0".into(),
                "0".into(),
                "1".into(),
                "0".into()
            ),
            "the complete fixture cashflow table: no refund on a loss and no fees"
        );
        assert_eq!(
            (
                contract.settlement.rule,
                contract.settlement.max_tick_gap_micros,
                contract.settlement.max_settlement_delay_micros
            ),
            (
                SettlementRule::PriceAtDueV1,
                run["max_valid_tick_gap_ms"].as_i64().unwrap() * 1000,
                run["max_valid_tick_gap_ms"].as_i64().unwrap() * 1000
            )
        );
        assert_eq!(
            (binding.account.as_str(), contract.currency.as_str()),
            ("simulated", settings.accounts[0].currency.as_str())
        );
        assert_eq!(
            (
                binding.envelope.max_purchase_cost.to_string(),
                binding.envelope.max_entry_fee.to_string(),
                binding.envelope.max_win_terminal_fee.to_string(),
                binding.envelope.max_loss_terminal_fee.to_string(),
                binding.envelope.max_tie_terminal_fee.to_string(),
                binding.envelope.min_winning_net_return.to_string(),
                binding.envelope.settlement_rule
            ),
            (
                "1".into(),
                "0".into(),
                "0".into(),
                "0".into(),
                "0".into(),
                "0.92".into(),
                SettlementRule::PriceAtDueV1
            )
        );
        let policy = settings
            .risk_policies
            .iter()
            .find(|p| p.id == binding.risk_policy)
            .unwrap();
        assert_eq!(policy.max_open_per_strategy, Some(1));
        assert_eq!(
            policy.max_open_per_duration,
            run["max_open_trades_per_expiry"].as_u64().map(|v| v as u32)
        );
        assert_eq!(
            policy.max_open_total,
            run["max_open_trades_total"].as_u64().map(|v| v as u32)
        );
        assert_eq!(policy.same_entry, SameEntry::All);
        assert!(!policy.deduplicate_signal_logic && policy.pause.is_none());
    }
    assert_eq!(settings.accounts.len(), 1);
    assert_eq!(
        (
            settings.accounts[0].id.as_str(),
            settings.accounts[0].scale,
            settings.accounts[0].initial_cash.to_string()
        ),
        (
            "simulated",
            2,
            run["starting_amount"].as_str().unwrap().to_string()
        )
    );
    assert_eq!(
        (
            settings.accounts[0].currency.as_str(),
            settings.reporting_currency.as_str(),
            settings.reporting_scale,
            settings.rates.is_none()
        ),
        ("fixture_unit", "fixture_unit", 2, true),
        "the fixture unit at scale two for the account and the reporting projection, no supplied rates"
    );
    let store = match &config.storage.publication_uri {
        binary_alpha_engine::config::PublicationUri::Filesystem(path) => path.clone(),
        other => panic!("{other} is not the filesystem boundary"),
    };

    // The application command over the governed inputs under GNU time, then verify.
    let (replay_lines, replay_wall, replay_peak) = timed(&[
        "replay",
        "--config",
        governed.application_config.to_str().unwrap(),
    ]);
    println!("replay: {}", replay_lines[0]);
    println!("reconstruction: {}", replay_lines[1]);
    println!(
        "replay wall {replay_wall:.3} s, peak resident {replay_peak} kB (load, simulate, publish, and restoration in one process)"
    );
    let replay_generation = generation(&replay_lines[0]);
    let manifest_path = store.join(format!("manifests/{replay_generation}/ready.json"));
    let (verify_lines, verify_wall, verify_peak) = timed(&[
        "data",
        "verify",
        "--manifest",
        &manifest_uri(&manifest_path),
    ]);
    assert_eq!(verify_lines, replay_lines[1..]);
    println!("verify wall {verify_wall:.3} s, peak resident {verify_peak} kB");
    let manifest = ReplayManifest::from_json(&fs::read(&manifest_path).unwrap()).unwrap();
    assert_eq!(manifest.code_revision, env!("BINARY_ALPHA_CODE_REVISION"));
    assert!(
        !manifest.code_revision.ends_with("-dirty") && manifest.code_revision != "unavailable",
        "the governed proof binds to a clean commit, not {}",
        manifest.code_revision
    );
    assert_eq!(manifest.config_hash, config.content_hash());
    let bound = &manifest.instruments[0];
    assert_eq!(bound.tick_generation, tick.generation);
    println!(
        "replay generation {} config {} tick {} feature {} plan {} outcome {} events {} final state {} summary {}",
        manifest.generation,
        manifest.config_hash,
        bound.tick_generation,
        bound.feature_generation,
        bound.plan_identity,
        bound.outcome_generation.as_deref().unwrap(),
        manifest.events,
        manifest.final_state_identity,
        manifest.summary_identity
    );

    // The historical diagnostics: the outcome generation's tick arrays and the thirty-second
    // stream's labels, decoded through the engine's own reader.
    let outcome_path = store.join(format!(
        "manifests/{}/ready.json",
        bound.outcome_generation.as_deref().unwrap()
    ));
    let (outcome_lines, _, _) =
        timed(&["data", "verify", "--manifest", &manifest_uri(&outcome_path)]);
    assert!(
        outcome_lines[0].starts_with("verified "),
        "the outcome generation's objects carry their recorded fingerprints: {}",
        outcome_lines[0]
    );
    let outcome = OutcomeManifest::from_json(&fs::read(&outcome_path).unwrap()).unwrap();
    let object = |path: &str| {
        store.join(
            &outcome
                .objects
                .iter()
                .find(|object| object.path == path)
                .unwrap()
                .key,
        )
    };
    let builder = OutcomeBuilder::new(
        outcome.rule.clone(),
        read_le(&object(TICK_TIME_OBJECT_PATH), i64::from_le_bytes),
        read_le(&object(TICK_PRICE_OBJECT_PATH), i64::from_le_bytes),
    )
    .unwrap();
    let (times, prices) = (builder.times(), builder.prices());
    assert_eq!(times.len() as u64, fixture.source_rows);
    let expiries = &outcome.rule.expiry_seconds;
    assert_eq!(
        expiries,
        &run["expiries"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v.as_u64().unwrap() as u32)
            .collect::<Vec<_>>()
    );
    let paths = stream_object_paths(30, 15);
    let references = read_le(&object(&paths[0]), i64::from_le_bytes);
    let entries = read_le(&object(&paths[1]), u32::from_le_bytes);
    let settlements = read_le(&object(&paths[2]), u32::from_le_bytes);
    let reasons = fs::read(object(&paths[3])).unwrap();
    let row_of: HashMap<i64, usize> = references
        .iter()
        .enumerate()
        .map(|(index, &close)| (close, index))
        .collect();
    let columns = expiries.len();

    // The feature rows the signals were decided on: clocks and the regime states.
    let feature = FeatureManifest::from_json(
        &fs::read(local(&settings.inputs[0].feature_manifest.to_string())).unwrap(),
    )
    .unwrap();
    let rows_object = feature
        .objects
        .iter()
        .find(|object| object.path == "rows/30s_15s.parquet")
        .unwrap();
    let regime_names = [
        "regime_trend_state",
        "regime_volatility_state",
        "regime_structure_state",
        "regime_transition_state",
        "regime_quality_state",
        "regime_directional_bias",
    ];
    let regimes: HashMap<i64, [String; 6]> = {
        use parquet::file::reader::{FileReader, SerializedFileReader};
        use parquet::record::Field;
        let reader =
            SerializedFileReader::new(fs::File::open(store.join(&rows_object.key)).unwrap())
                .unwrap();
        let schema = reader
            .metadata()
            .file_metadata()
            .schema_descr()
            .root_schema();
        let wanted: Vec<&str> = std::iter::once("close_time_micros")
            .chain(regime_names)
            .collect();
        let fields: Vec<_> = wanted
            .iter()
            .map(|name| {
                schema
                    .get_fields()
                    .iter()
                    .find(|field| field.name() == *name)
                    .unwrap()
                    .clone()
            })
            .collect();
        let projection = parquet::schema::types::Type::group_type_builder(schema.name())
            .with_fields(fields)
            .build()
            .unwrap();
        reader
            .get_row_iter(Some(projection))
            .unwrap()
            .map(|row| {
                let row = row.unwrap();
                let mut values = row.get_column_iter();
                let Some((_, Field::TimestampMicros(close))) = values.next() else {
                    panic!("clock")
                };
                let states: Vec<String> = values
                    .map(|(_, field)| match field {
                        Field::Str(text) => text.clone(),
                        Field::Null => String::new(),
                        other => panic!("{other:?}"),
                    })
                    .collect();
                (*close, states.try_into().unwrap())
            })
            .collect()
    };
    assert_eq!(regimes.len(), references.len());

    // The target ledger, indexed by signal identity and command, with the available cash and
    // the strategy-slot holder at each decision.
    let ledger = ledger_lines(&object_path(&store, &manifest, EVENTS_OBJECT_PATH));
    let mut signals: HashMap<(String, i64), TargetSignal> = HashMap::new();
    let mut accepted: HashMap<String, (i64, i64, i64)> = HashMap::new();
    let mut settled: HashMap<String, TargetSettlement> = HashMap::new();
    let mut unresolved: HashMap<String, (UnresolvedReason, String)> = HashMap::new();
    let mut cash = settings.accounts[0].initial_cash.rescale(2).unwrap();
    let mut reserved = Decimal::zero(2);
    let mut reservation_of: HashMap<String, Decimal> = HashMap::new();
    let mut holder: HashMap<String, (String, bool)> = HashMap::new();
    let mut binding_of: HashMap<String, String> = HashMap::new();
    let started = std::time::Instant::now();
    for line in &ledger {
        let event = FinancialEvent::from_line(line).unwrap();
        match event.kind {
            EventKind::RunDefinition { .. } => {}
            EventKind::Signal {
                binding,
                stream,
                close_time_micros,
                known_at_micros,
                disposition,
                quote_price_units,
                split,
                command,
                reservation,
                ..
            } => {
                let available_before = cash.checked_sub(reserved).unwrap();
                if let Some(command) = &command {
                    let reservation = reservation.unwrap();
                    reserved = reserved.checked_add(reservation).unwrap();
                    reservation_of.insert(command.clone(), reservation);
                    holder.insert(binding.clone(), (command.clone(), false));
                    binding_of.insert(command.clone(), binding.clone());
                }
                let slot = if disposition == Disposition::Admitted {
                    None
                } else {
                    holder.get(&binding).cloned()
                };
                assert!(
                    signals
                        .insert(
                            (binding, close_time_micros),
                            TargetSignal {
                                stream,
                                disposition,
                                known_at: known_at_micros,
                                quote: quote_price_units.unwrap(),
                                split,
                                command,
                                available_before,
                                holder: slot
                            }
                        )
                        .is_none()
                );
            }
            EventKind::Accepted {
                command,
                entry_time_micros: Some(entry_time_micros),
                entry_price_units: Some(entry_price_units),
                due_time_micros: Some(due_time_micros),
                debit,
                reservation,
                ..
            } => {
                cash = cash.checked_sub(debit).unwrap();
                reserved = reserved
                    .checked_sub(reservation_of[&command])
                    .unwrap()
                    .checked_add(reservation)
                    .unwrap();
                accepted.insert(
                    command,
                    (entry_time_micros, entry_price_units, due_time_micros),
                );
            }
            EventKind::Settled {
                command,
                settlement_time_micros,
                settlement_price_units: Some(settlement_price_units),
                outcome,
                credit,
                release,
                path: Some(path),
                ..
            } => {
                cash = cash.checked_add(credit).unwrap();
                reserved = reserved.checked_sub(release).unwrap();
                holder.remove(&binding_of[&command]);
                settled.insert(
                    command,
                    TargetSettlement {
                        time: settlement_time_micros,
                        price: settlement_price_units,
                        outcome,
                        path,
                    },
                );
            }
            EventKind::Unresolved {
                command,
                reason,
                evidence,
                ..
            } => {
                holder.insert(binding_of[&command].clone(), (command.clone(), true));
                unresolved.insert(command, (reason, evidence));
            }
            other => panic!("unexpected record in a historical replay: {other:?}"),
        }
    }
    println!(
        "ledger indexed: {} records, {} signals, {} accepted, {} settled, {} unresolved in {:.1} s",
        ledger.len(),
        signals.len(),
        accepted.len(),
        settled.len(),
        unresolved.len(),
        started.elapsed().as_secs_f64()
    );
    assert_eq!(
        signals.len() as u64,
        fixture.totals["raw_signals"].as_u64().unwrap(),
        "the frozen strategies reproduce every reference signal"
    );

    // Reference trades and invalidations joined by signal number.
    let started = std::time::Instant::now();
    let mut trades = LegacyCsv::open(&root.join("parity_cpu_run_v2/trades.csv"));
    let trades_header = trades.header.clone();
    let trade_rows = index_rows(&mut trades);
    let mut invalid = LegacyCsv::open(&root.join("parity_cpu_run_v2/invalid_trades.csv"));
    let invalid_header = invalid.header.clone();
    let invalid_rows = index_rows(&mut invalid);
    assert_eq!(
        (trade_rows.len() as u64, invalid_rows.len() as u64),
        (
            fixture.totals["settled"].as_u64().unwrap(),
            fixture.totals["invalidated"].as_u64().unwrap()
        )
    );
    assert!(
        trade_rows
            .keys()
            .all(|number| !invalid_rows.contains_key(number))
    );
    println!(
        "reference trades and invalidations indexed in {:.1} s",
        started.elapsed().as_secs_f64()
    );

    // Every reference signal: the classified disposition citing the first differing transition,
    // then every field the fixture declares shared, compared by header name across the three
    // reference files. Every divergence is retained with its citation.
    let started = std::time::Instant::now();
    let mut signals_csv = LegacyCsv::open(&root.join("parity_cpu_run_v2/signals.csv"));
    let signals_header = signals_csv.header.clone();
    let column = |name: &str| signals_csv.column(name);
    let (c_number, c_candidate, c_decision, c_opened, c_block) = (
        column("signal_number"),
        column("candidate_id"),
        column("row_decision_time_utc"),
        column("opened_trade"),
        column("block_reasons"),
    );
    let by_candidate: HashMap<&str, &Candidate> = fixture
        .candidates
        .iter()
        .map(|candidate| (candidate.candidate_id.as_str(), candidate))
        .collect();
    let scale8 = PriceScale::try_from(8).unwrap();
    let mut evidence = Evidence::create(&manifest);
    let mut divergent_commands: std::collections::HashSet<String> =
        std::collections::HashSet::new();
    // Per candidate: the reference's last opened trade (signal number, the time it closed) and
    // the target's last divergence.
    let mut reference_holder: HashMap<&str, (u64, i64, i64)> = HashMap::new();
    let mut last_divergence: HashMap<&str, u64> = HashMap::new();
    let mut path_source_rounding = 0u64;
    let mut path_time_source_rounding = 0u64;
    let mut compared_paths = 0u64;
    let mut compared_outcomes = 0u64;
    let mut compared_fields = 0u64;
    let mut reference_counts = BTreeMap::from([
        ("opened", 0u64),
        ("blocked_by_strategy_capacity", 0),
        ("blocked_by_entry_gap", 0),
        ("wins", 0),
        ("losses", 0),
        ("ties", 0),
    ]);
    let time_pair =
        |reference: &str, target: i64| (micros(reference).to_string(), target.to_string());
    let price_pair = |reference: &str, target_units: i64| {
        (
            parse_price_units(reference, scale8).unwrap().to_string(),
            (target_units * 100).to_string(),
        )
    };
    let optional_time = |text: &str| (!text.is_empty()).then(|| micros(text));
    let mut rows_seen = 0u64;
    while let Some(row) = signals_csv.next_row() {
        rows_seen += 1;
        let number: u64 = row[c_number].parse().unwrap();
        let candidate = by_candidate[row[c_candidate].as_str()];
        let contract = contract_of[candidate.candidate_id.as_str()];
        let close = micros(&row[c_decision]);
        let target = signals
            .get(&(candidate.candidate_id.clone(), close))
            .unwrap_or_else(|| {
                panic!(
                    "signal {number}: no target decision for {} at {}",
                    candidate.candidate_id, row[c_decision]
                )
            });
        let row_index = row_of[&close];
        // Historical diagnostics from the outcome labels for this row and expiry.
        let expiry_column = expiries
            .iter()
            .position(|&seconds| seconds == candidate.expiry_seconds)
            .unwrap();
        let cell = builder
            .cell(
                entries[row_index],
                expiry_column,
                settlements[row_index * columns + expiry_column],
                reasons[row_index * columns + expiry_column],
            )
            .unwrap();
        let entry = cell.entry.unwrap();
        assert_eq!(
            entry.event_time_micros, target.known_at,
            "signal {number}: the label entry tick is the trigger tick"
        );
        let sell = contract.direction == Direction::Sell;
        let reference_opened = row[c_opened] == "1";
        let reference_block = row[c_block].as_str();
        let trade: Option<Vec<&str>> = trade_rows
            .get(&number)
            .map(|line| line.split('\u{1f}').collect());
        let invalidated: Option<Vec<&str>> = invalid_rows
            .get(&number)
            .map(|line| line.split('\u{1f}').collect());
        if reference_opened {
            *reference_counts.get_mut("opened").unwrap() += 1;
        }
        // The reference's settled trade: the label agrees, and the exact integer path and the
        // legacy floating path over the same window.
        let reference_trade = trade.as_ref().map(|_| {
            compared_outcomes += 1;
            let settlement = cell.settlement.unwrap();
            assert_eq!(
                cell.reason,
                InvalidReason::Valid,
                "signal {number}: the settled reference trade is a valid label"
            );
            let outcome = match (cell.outcome.unwrap(), sell) {
                (binary_alpha_engine::outcomes::Outcome::Tie, _) => "tie",
                (binary_alpha_engine::outcomes::Outcome::BuyWin, false)
                | (binary_alpha_engine::outcomes::Outcome::SellWin, true) => "win",
                _ => "loss",
            };
            *reference_counts
                .get_mut(match outcome {
                    "win" => "wins",
                    "loss" => "losses",
                    _ => "ties",
                })
                .unwrap() += 1;
            let exact = exact_path(
                times,
                prices,
                entry.index as usize,
                settlement.index as usize,
                sell,
            );
            let legacy = legacy_path(
                times,
                prices,
                entry.index as usize,
                settlement.index as usize,
                sell,
            );
            compared_paths += 1;
            (settlement, outcome, exact, legacy)
        });
        // The reference's invalidated trade: a gap transition in the tick arrays inside the
        // contract window, and a gap or stale-settlement label.
        let gap = invalidated.as_ref().map(|invalidated| {
            assert!(
                matches!(
                    cell.reason,
                    InvalidReason::InternalGap | InvalidReason::StaleSettlement
                ),
                "signal {number}: the invalidated trade's label reason is {}",
                cell.reason
            );
            let gap_end = micros(
                invalidated[invalid_header
                    .iter()
                    .position(|h| h == "gap_end_time_utc")
                    .unwrap()],
            );
            let gap_end_index = times.partition_point(|&t| t < gap_end);
            let gap_start = times[gap_end_index - 1];
            assert!(
                entry.event_time_micros <= gap_start && gap_start < cell.due_time_micros.unwrap()
            );
            (gap_start, gap_end)
        });
        // Dispositions, classified with the first differing transition cited.
        let citation = |what: &str| {
            format!(
                "signal {number} ({} at {}): {what}",
                candidate.candidate_id, row[c_decision]
            )
        };
        let class = match (reference_opened, reference_block, target.disposition) {
            (true, _, Disposition::Admitted) => {
                let command = target.command.as_ref().unwrap();
                if let Some((_, gap_end)) = gap {
                    let (reason, retained) = unresolved.get(command).unwrap_or_else(|| panic!("signal {number}: the reference invalidated this trade but the target settled or kept it"));
                    assert_eq!(*reason, UnresolvedReason::Gap);
                    assert!(
                        retained.contains(&format_event_time_micros(gap_end)),
                        "signal {number}: {retained}"
                    );
                    divergent_commands.insert(command.clone());
                    last_divergence.insert(candidate.candidate_id.as_str(), number);
                    evidence.classify(
                        number,
                        "retained_unresolved_settlement_evidence",
                        citation(&format!(
                            "the reference removed the trade at its gap; the target retains {command}: {retained}"
                        )),
                    )
                } else {
                    assert!(
                        settled.contains_key(command)
                            || accepted.contains_key(command) && !unresolved.contains_key(command),
                        "signal {number}: the target left {command} unresolved where the reference settled"
                    );
                    evidence.classify(number, "matched_admitted", citation("both admitted"))
                }
            }
            (false, "[\"max_open_trades_per_strategy\"]", Disposition::CapacityStrategy) => {
                *reference_counts
                    .get_mut("blocked_by_strategy_capacity")
                    .unwrap() += 1;
                evidence.classify(
                    number,
                    "matched_capacity",
                    citation("both blocked by strategy capacity"),
                )
            }
            (
                false,
                "[\"invalid_recent_tick_gap\"]",
                Disposition::GapAtEntry | Disposition::StaleFeature,
            ) => {
                *reference_counts.get_mut("blocked_by_entry_gap").unwrap() += 1;
                let index = entry.index as usize;
                assert!(
                    times[index] - times[index - 1] > 60_000_000,
                    "signal {number}: the gap into the trigger tick"
                );
                evidence.classify(
                    number,
                    "matched_gap_at_entry",
                    citation(&format!(
                        "gap of {} microseconds into the trigger tick; target {}",
                        times[index] - times[index - 1],
                        target.disposition
                    )),
                )
            }
            (true, _, Disposition::InsufficientCash) => {
                let reservation = contract.reservation().unwrap().rescale(2).unwrap();
                assert!(
                    target.available_before.compare(reservation).unwrap()
                        == std::cmp::Ordering::Less,
                    "signal {number}: available {}",
                    target.available_before
                );
                last_divergence.insert(candidate.candidate_id.as_str(), number);
                evidence.classify(
                    number,
                    "insufficient_cash",
                    citation(&format!(
                        "available cash {} (native cash less unpaid reservations) cannot fund the reservation {reservation}",
                        target.available_before
                    )),
                )
            }
            (true, _, Disposition::CapacityStrategy) => {
                let (held_by, retained) = target.holder.clone().unwrap_or_else(|| {
                    panic!("signal {number}: capacity blocked without a slot holder")
                });
                assert!(
                    divergent_commands.contains(&held_by),
                    "signal {number}: the slot holder {held_by} is not a classified divergence"
                );
                last_divergence.insert(candidate.candidate_id.as_str(), number);
                evidence.classify(
                    number,
                    if retained {
                        "capacity_held_by_retained_unresolved"
                    } else {
                        "capacity_held_after_divergence"
                    },
                    citation(&format!(
                        "the strategy slot is held by {held_by}, first differing transition of this binding"
                    )),
                )
            }
            (false, "[\"max_open_trades_per_strategy\"]", Disposition::Admitted) => {
                *reference_counts
                    .get_mut("blocked_by_strategy_capacity")
                    .unwrap() += 1;
                let earlier = last_divergence.get(candidate.candidate_id.as_str()).copied().unwrap_or_else(|| panic!("signal {number}: admitted where the reference was capacity blocked without an earlier divergence"));
                let (holder_number, holder_decision, holder_close) = *reference_holder
                    .get(candidate.candidate_id.as_str())
                    .unwrap_or_else(|| panic!("signal {number}: the reference was capacity blocked without an opened trade of this candidate"));
                assert!(
                    holder_close > target.known_at,
                    "signal {number}: the reference's last trade of this candidate (signal {holder_number}) closed at {} before the trigger tick",
                    format_event_time_micros(holder_close)
                );
                let holder_disposition =
                    signals[&(candidate.candidate_id.clone(), holder_decision)].disposition;
                assert_ne!(
                    holder_disposition,
                    Disposition::Admitted,
                    "signal {number}: the target also opened the reference's holding trade"
                );
                divergent_commands.insert(target.command.clone().unwrap());
                evidence.classify(
                    number,
                    "admitted_after_divergence",
                    citation(&format!(
                        "the reference slot is held by its trade of signal {holder_number}, open until {}, which the target did not open (first differing transition of this binding: signal {earlier})",
                        format_event_time_micros(holder_close)
                    )),
                )
            }
            (opened, block, disposition) => panic!(
                "signal {number}: unclassified difference: reference opened {opened} block {block}, target {disposition}"
            ),
        };
        if reference_opened {
            let closed = match (&reference_trade, gap) {
                (Some((settlement, ..)), None) => settlement.event_time_micros,
                (None, Some((_, gap_end))) => gap_end,
                _ => {
                    panic!("signal {number}: an opened reference signal is settled or invalidated")
                }
            };
            reference_holder.insert(candidate.candidate_id.as_str(), (number, close, closed));
        }
        let opened_class = matches!(
            class,
            "matched_admitted"
                | "insufficient_cash"
                | "capacity_held_by_retained_unresolved"
                | "capacity_held_after_divergence"
                | "retained_unresolved_settlement_evidence"
        );
        // The shared fields of every reference file this signal appears in.
        let mut field = |file: &str, name: &str, reference: &str| -> (String, String) {
            let text = |target: String| (reference.to_string(), target);
            match name {
                "candidate_id" => text(candidate.candidate_id.clone()),
                "candle_set" => text(format!(
                    "{}s_offset{}s",
                    target.stream.duration_seconds, target.stream.offset_seconds
                )),
                "expiry_seconds" => text((contract.duration_micros / 1_000_000).to_string()),
                "direction" => text(contract.direction.to_string().to_uppercase()),
                "row_decision_time_utc" => time_pair(reference, close),
                "entry_tick_time_utc" => time_pair(reference, target.known_at),
                "entry_time_utc" => {
                    if let Some(command) = &target.command
                        && let Some((entry_time, entry_price, _)) = accepted.get(command)
                    {
                        assert_eq!(
                            (*entry_time, *entry_price),
                            (target.known_at, target.quote),
                            "signal {number}: accepted entry"
                        );
                    }
                    time_pair(reference, entry.event_time_micros)
                }
                "split_label" => text(target.split.clone().unwrap_or_default()),
                "entry_price" => price_pair(reference, target.quote),
                "predicate" => text(format!("{} == '{}'", candidate.output, candidate.value)),
                "opened_trade" => text(if opened_class { "1" } else { "0" }.into()),
                "block_reasons" => text(
                    match class {
                        "matched_capacity" | "admitted_after_divergence" => {
                            "[\"max_open_trades_per_strategy\"]"
                        }
                        "matched_gap_at_entry" => "[\"invalid_recent_tick_gap\"]",
                        _ => "[]",
                    }
                    .into(),
                ),
                "validity_block_reason" => text(
                    if class == "matched_gap_at_entry" {
                        "invalid_recent_tick_gap"
                    } else {
                        ""
                    }
                    .into(),
                ),
                _ if regime_names.contains(&name) => {
                    let index = regime_names.iter().position(|n| *n == name).unwrap();
                    text(regimes[&close][index].clone())
                }
                "due_time_utc" => {
                    if let Some(command) = &target.command
                        && let Some((_, _, due)) = accepted.get(command)
                    {
                        assert_eq!(
                            *due,
                            cell.due_time_micros.unwrap(),
                            "signal {number}: accepted due time"
                        );
                    }
                    time_pair(reference, cell.due_time_micros.unwrap())
                }
                "settlement_tick_time_utc" | "settlement_price" | "outcome" => {
                    let (label, outcome, exact, _) = reference_trade.as_ref().unwrap();
                    if let Some(command) = &target.command
                        && let Some(target_settlement) = settled.get(command)
                    {
                        assert_eq!(
                            (
                                target_settlement.time,
                                target_settlement.price,
                                target_settlement.outcome.to_string(),
                                target_settlement.path
                            ),
                            (
                                label.event_time_micros,
                                label.price_units,
                                (*outcome).to_string(),
                                *exact
                            ),
                            "signal {number}: the target's own settlement of the same contract"
                        );
                    }
                    match name {
                        "settlement_tick_time_utc" => time_pair(reference, label.event_time_micros),
                        "settlement_price" => price_pair(reference, label.price_units),
                        _ => text((*outcome).to_string()),
                    }
                }
                "final_directional_move_bps"
                | "max_favorable_excursion_bps"
                | "max_adverse_excursion_bps" => {
                    let (_, _, exact, legacy) = reference_trade.as_ref().unwrap();
                    let entry_units = prices[entry.index as usize];
                    let (units, legacy_value) = match name {
                        "final_directional_move_bps" => (exact.final_move_units, legacy.final_move),
                        "max_favorable_excursion_bps" => (exact.max_favorable_units, legacy.mfe),
                        _ => (exact.max_adverse_units, legacy.mae),
                    };
                    let exact_text = basis_points_text(units, entry_units).unwrap();
                    if exact_text == reference {
                        text(exact_text)
                    } else {
                        path_source_rounding += 1;
                        evidence.retain(
                            number,
                            "legacy_rounding_value",
                            &format!(
                                "{name}: reference {reference}, exact {exact_text}, legacy floating rule {legacy_value:.10} from {units} units at entry {entry_units}"
                            ),
                        );
                        text(format!("{legacy_value:.10}"))
                    }
                }
                "mfe_time_utc" | "mae_time_utc" => {
                    let (_, _, exact, legacy) = reference_trade.as_ref().unwrap();
                    let (exact_time, legacy_time) = if name == "mfe_time_utc" {
                        (exact.max_favorable_time_micros, legacy.mfe_time)
                    } else {
                        (exact.max_adverse_time_micros, legacy.mae_time)
                    };
                    let reference_time = micros(reference);
                    if reference_time == exact_time {
                        time_pair(reference, exact_time)
                    } else {
                        assert_eq!(
                            prices[times.partition_point(|&t| t < reference_time)],
                            prices[times.partition_point(|&t| t < exact_time)],
                            "signal {number}: {name} names an equal extremum"
                        );
                        path_time_source_rounding += 1;
                        evidence.retain(
                            number,
                            "legacy_rounding_time",
                            &format!(
                                "{name}: reference {reference}, exact {}, legacy floating rule {}; both name price {}",
                                format_event_time_micros(exact_time),
                                format_event_time_micros(legacy_time),
                                prices[times.partition_point(|&t| t < exact_time)]
                            ),
                        );
                        time_pair(reference, legacy_time)
                    }
                }
                "first_favorable_time_utc" | "first_adverse_time_utc" => {
                    let (_, _, exact, _) = reference_trade.as_ref().unwrap();
                    let target_time = if name == "first_favorable_time_utc" {
                        exact.first_favorable_time_micros
                    } else {
                        exact.first_adverse_time_micros
                    };
                    (
                        format!("{:?}", optional_time(reference)),
                        format!("{target_time:?}"),
                    )
                }
                "favorable_before_adverse" | "adverse_before_favorable" => {
                    let (_, _, exact, _) = reference_trade.as_ref().unwrap();
                    let flag = if name == "favorable_before_adverse" {
                        exact.favorable_before_adverse
                    } else {
                        exact.adverse_before_favorable
                    };
                    text(if flag { "1" } else { "0" }.into())
                }
                "invalidated_time_utc" => time_pair(reference, gap.unwrap().1),
                "invalid_reason" => text("expiry_window_crossed_tick_gap".into()),
                "gap_start_time_utc" => time_pair(reference, gap.unwrap().0),
                "gap_end_time_utc" => time_pair(reference, gap.unwrap().1),
                "gap_ms" => {
                    let (start, end) = gap.unwrap();
                    text(((end - start) / 1000).to_string())
                }
                other => panic!("{file}: shared field {other} has no comparison"),
            }
        };
        let signal_fields: Vec<&str> = row.iter().map(String::as_str).collect();
        for (file, header, fields) in [
            ("signals", &signals_header, Some(&signal_fields)),
            ("trades", &trades_header, trade.as_ref()),
            ("invalid_trades", &invalid_header, invalidated.as_ref()),
        ] {
            let Some(fields) = fields else { continue };
            for (name, reference) in header.iter().zip(fields) {
                if fixture.fields[file][name]["class"] != "shared" {
                    continue;
                }
                let (expected, actual) = field(file, name, reference);
                assert_eq!(expected, actual, "signal {number}: {file}.{name}");
                compared_fields += 1;
            }
        }
    }
    evidence.writer.flush().unwrap();
    assert_eq!(rows_seen, fixture.rows["signals"]);
    for name in [
        "opened",
        "blocked_by_strategy_capacity",
        "blocked_by_entry_gap",
        "wins",
        "losses",
        "ties",
    ] {
        assert_eq!(
            reference_counts[name],
            fixture.totals[name].as_u64().unwrap(),
            "{name}"
        );
    }
    assert_eq!(
        compared_outcomes,
        fixture.totals["settled"].as_u64().unwrap()
    );
    assert_eq!(
        evidence.classes.values().sum::<u64>(),
        fixture.rows["signals"]
    );
    let net = Decimal::parse("0.92")
        .unwrap()
        .checked_mul(Decimal::parse(&reference_counts["wins"].to_string()).unwrap())
        .unwrap()
        .checked_sub(Decimal::parse(&reference_counts["losses"].to_string()).unwrap())
        .unwrap();
    assert_eq!(
        net.to_string(),
        fixture.totals["net_units"].as_str().unwrap(),
        "exact unit arithmetic of the reference dispositions"
    );
    println!(
        "compared {rows_seen} reference signals in {:.1} s: {compared_fields} shared fields, {compared_outcomes} settled outcomes and {compared_paths} paths agree; {path_source_rounding} path values and {path_time_source_rounding} extremum times reproduce only through the legacy floating rounding",
        started.elapsed().as_secs_f64()
    );
    for (class, count) in &evidence.classes {
        println!("  {class}: {count} (first: {})", evidence.examples[class]);
    }
    println!(
        "retained {} divergence and rounding citations at {}",
        evidence.retained,
        evidence.path.display()
    );

    // The target's actual funded results, the reference's historical diagnostics, and the run
    // evidence, side by side.
    let summary =
        Summary::from_json(&fs::read(object_path(&store, &manifest, SUMMARY_OBJECT_PATH)).unwrap())
            .unwrap();
    let account = &summary.accounts[0];
    println!(
        "target funded account: cash {} paid basis {} unresolved loss {} completed profit {} open {} max drawdown {}; portfolio signals {} accepted {} settled {} wins {} losses {} ties {} unresolved {}",
        account.cash,
        account.paid_basis,
        account.unresolved_loss,
        account.completed_profit,
        account.open,
        account.max_drawdown,
        summary.portfolio.signals,
        summary.portfolio.accepted,
        summary.portfolio.settled,
        summary.portfolio.wins,
        summary.portfolio.losses,
        summary.portfolio.ties,
        summary.portfolio.unresolved
    );
    println!(
        "reference diagnostics: raw {} blocked {} opened {} settled {} wins {} losses {} ties {} invalidated {} exact net {}; historical elapsed {:.3} s of which feature engine {:.3} s, peak memory unavailable",
        fixture.totals["raw_signals"],
        fixture.totals["blocked"],
        fixture.totals["opened"],
        fixture.totals["settled"],
        fixture.totals["wins"],
        fixture.totals["losses"],
        fixture.totals["ties"],
        fixture.totals["invalidated"],
        fixture.totals["net_units"],
        fixture.receipt.elapsed_seconds,
        fixture.receipt.feature_engine_seconds
    );
    println!("test process peak resident {} kB", in_process_peak_kb());
}

mod broker_authoritative {
    use super::*;
    use binary_alpha_engine::execution::{
        Block, BrokerLiability, CashAction, CashFact, ContractSemantics, Proposal,
        REPLAY_SCHEMA_VERSION_BROKER, TerminalFact, TerminalStatus,
    };

    const PURCHASE: i64 = 1_789_347_036_000_000;
    const SECOND: i64 = 1_000_000;

    fn broker_definition() -> RunDefinition {
        let mut definition = definition(|replay| {
            replay.decision_start = "2026-09-13T00:00:00Z".into();
            replay.decision_end = "2026-09-15T00:00:00Z".into();
            replay.accounts[0].initial_cash = decimal("9954.57");
            let terms = &mut replay.contracts[0];
            terms.duration_micros = 15 * SECOND;
            terms.stake = decimal("10");
            terms.quoted_cost = decimal("10");
            terms.win.gross_return = decimal("0");
            terms.tie.gross_return = decimal("0");
            terms.settlement.rule = SettlementRule::BrokerAuthoritativeV1;
            terms.settlement.max_tick_gap_micros = 120 * SECOND;
            terms.semantics = Some(ContractSemantics::RiseFallStrictV1);
            let envelope = &mut replay.bindings[0].envelope;
            envelope.settlement_rule = SettlementRule::BrokerAuthoritativeV1;
            envelope.semantics = terms.semantics;
            envelope.max_purchase_cost = decimal("10");
            envelope.min_winning_net_return = decimal("8.83");
            let policy = &mut replay.risk_policies[0];
            policy.max_proposal_age_micros = Some(5 * SECOND);
            policy.max_quote_age_micros = 120 * SECOND;
            policy.max_feature_age_micros = 120 * SECOND;
            policy.max_open_per_strategy = Some(3);
        });
        definition.schema_version = REPLAY_SCHEMA_VERSION_BROKER;
        definition.instruments[0].price_scale = 4;
        definition
    }

    fn quote(live: &Live, id: &str, at: i64, payout: &str) -> Proposal {
        let mut terms = live.engine.definition().replay.contracts[0].clone();
        terms.id = format!("1:{id}");
        terms.win.gross_return = decimal(payout);
        let mut proposal = Proposal {
            identity: terms.id.clone(),
            request_identity: String::new(),
            account: live.engine.definition().replay.bindings[0].account.clone(),
            instrument: live.engine.definition().replay.bindings[0]
                .instrument
                .clone(),
            terms,
            spot_units: 920_409,
            spot_time_micros: at,
            receipt_micros: at,
            schema: "synthetic:proposal".into(),
            payload_sha256: "b".repeat(64),
        };
        proposal.request_identity = proposal.canonical_request_identity().unwrap();
        proposal
    }

    fn proposal(proposal: Proposal) -> Observation {
        Observation::Proposal {
            binding: "b1".into(),
            proposal,
        }
    }

    fn checked_step(
        live: &mut Live,
        at: i64,
        observations: Vec<Observation>,
    ) -> Vec<FinancialEvent> {
        let events = live.step(at, observations);
        let restored = live.restored();
        assert_eq!(
            restored.engine.summary().to_json(),
            live.engine.summary().to_json()
        );
        events
    }

    fn prepared(mut definition: RunDefinition) -> (Live, String) {
        definition.replay.risk_policies[0].max_proposal_age_micros = Some(120 * SECOND);
        let mut live = Live::new(definition);
        let quote = quote(&live, "quote", PURCHASE, "18.83");
        let events = checked_step(
            &mut live,
            PURCHASE,
            vec![
                proposal(quote),
                tick(PURCHASE, 920_409),
                row(0, PURCHASE, PURCHASE, true),
            ],
        );
        (live, Live::command(&events))
    }

    fn liability() -> BrokerLiability {
        BrokerLiability {
            contract_ref: "fixture-contract".into(),
            transaction_ref: "fixture-buy".into(),
            purchase_time_micros: PURCHASE,
            expected_start_micros: Some(PURCHASE),
            payout: decimal("18.83"),
        }
    }

    fn purchase(command: &str, debit: &str) -> Observation {
        Observation::Purchased {
            command: command.into(),
            source: source("purchase", PURCHASE),
            debit: decimal(debit),
            liability: liability(),
        }
    }

    fn confirmed(
        command: &str,
        at: i64,
        price: Option<i64>,
        entry: Option<i64>,
        start: Option<i64>,
        expiry: Option<i64>,
    ) -> Observation {
        Observation::ContractUpdate {
            command: command.into(),
            source: source(&format!("confirmed:{at}:{price:?}"), at),
            entry_price_units: price,
            entry_time_micros: entry,
            start_micros: start,
            expiry_micros: expiry,
        }
    }

    fn terminal(
        command: &str,
        status: TerminalStatus,
        transaction: &str,
        exit: Option<(i64, i64)>,
    ) -> Observation {
        Observation::Terminal {
            command: command.into(),
            source: source("terminal", PURCHASE + 16 * SECOND),
            fact: TerminalFact {
                status,
                exit_price_units: exit.map(|pair| pair.0),
                exit_time_micros: exit.map(|pair| pair.1),
                transaction_ref: Some(transaction.into()),
            },
        }
    }

    fn cash(action: CashAction, amount: &str, transaction: &str, at: i64) -> Observation {
        Observation::Cash {
            source: source(&format!("cash:{transaction}"), at),
            fact: CashFact {
                account: "a".into(),
                contract_ref: Some("fixture-contract".into()),
                transaction_ref: transaction.into(),
                action,
                amount: decimal(amount),
                time_micros: at,
            },
        }
    }

    #[test]
    fn proposals_replace_expire_and_freeze_exact_economics() {
        let mut live = Live::new(broker_definition());
        assert_eq!(
            dispositions(&checked_step(
                &mut live,
                PURCHASE,
                vec![tick(PURCHASE, 920_409), row(0, PURCHASE, PURCHASE, true)]
            )),
            [Disposition::NoProposal]
        );
        let first = quote(&live, "first", PURCHASE, "19.54");
        checked_step(&mut live, PURCHASE, vec![proposal(first)]);
        let stale = PURCHASE + 5 * SECOND + 1;
        assert_eq!(
            dispositions(&checked_step(
                &mut live,
                stale,
                vec![tick(stale, 919_315), row(0, stale, stale, true)]
            )),
            [Disposition::StaleProposal]
        );
        let later = quote(&live, "later", stale, "18.83");
        checked_step(&mut live, stale, vec![proposal(later.clone())]);
        let at = stale + 5 * SECOND;
        let events = checked_step(&mut live, at, vec![tick(at, 920_252), row(0, at, at, true)]);
        let EventKind::Signal {
            proposal: Some(recorded),
            reservation: Some(reservation),
            ..
        } = &events[0].kind
        else {
            panic!("{events:?}")
        };
        assert_eq!(*recorded, later);
        assert_eq!(reservation.to_string(), "10.00");
        assert_eq!(live.account("a").unresolved_loss.to_string(), "10.00");
        let worse = quote(&live, "worse", at + 1, "18.82");
        let events = checked_step(
            &mut live,
            at + 1,
            vec![
                proposal(worse),
                tick(at + 1, 920_252),
                row(0, at + 1, at + 1, true),
            ],
        );
        assert_eq!(dispositions(&events), [Disposition::QuoteRejected]);
        assert_eq!(live.account("a").reserved.to_string(), "10.00");
        assert!(
            live.lines
                .iter()
                .any(|line| String::from_utf8_lossy(line).contains("1:later"))
        );
        let mut stale_delivery = live.restored();
        let current = quote(&stale_delivery, "current", at + 2, "18.83");
        checked_step(&mut stale_delivery, at + 2, vec![proposal(current)]);
        let old = quote(&stale_delivery, "old", at, "18.83");
        assert!(
            stale_delivery
                .try_step(at + 2, vec![proposal(old)])
                .unwrap_err()
                .contains("precedes the installed")
        );
    }

    #[test]
    fn purchased_unknown_entry_confirmed_sparse_exit_and_cash_restore_exactly() {
        let (mut live, command) = prepared(broker_definition());
        let events = checked_step(&mut live, PURCHASE, vec![purchase(&command, "10")]);
        assert!(matches!(
            &events[0].kind,
            EventKind::Accepted {
                entry_time_micros: None,
                entry_price_units: None,
                due_time_micros: None,
                liability: Some(_),
                deficit: None,
                discrepancy: false,
                ..
            }
        ));
        assert_eq!(
            live.balances("a"),
            (
                "9944.57".into(),
                "0.00".into(),
                "10.00".into(),
                "10.00".into(),
                "0.00".into(),
                1
            )
        );
        let entry = PURCHASE + 2 * SECOND;
        let expiry = PURCHASE + 15 * SECOND;
        let timing = confirmed(&command, entry, None, None, Some(PURCHASE), Some(expiry));
        checked_step(&mut live, entry, vec![timing.clone()]);
        let entry_fact = confirmed(&command, entry, Some(920_252), Some(entry), None, None);
        checked_step(&mut live, entry, vec![entry_fact.clone()]);
        assert!(checked_step(&mut live, entry, vec![entry_fact, timing]).is_empty());
        let mut contradiction = live.restored();
        assert!(
            contradiction
                .try_step(
                    entry,
                    vec![confirmed(
                        &command,
                        entry,
                        None,
                        None,
                        None,
                        Some(expiry + 1)
                    )]
                )
                .unwrap_err()
                .contains("contradicts recorded")
        );
        assert!(checked_step(&mut live, expiry, vec![tick(expiry, 920_500)]).is_empty());
        assert_eq!(live.account("a").open, 1);
        let end = terminal(
            &command,
            TerminalStatus::Won,
            "24655172099",
            Some((920_308, PURCHASE + 14 * SECOND)),
        );
        let events = checked_step(&mut live, PURCHASE + 16 * SECOND, vec![end.clone()]);
        assert!(matches!(
            &events[0].kind,
            EventKind::Unresolved {
                reason: UnresolvedReason::AwaitingCash,
                terminal: Some(_),
                ..
            }
        ));
        assert_eq!(live.account("a").paid_basis.to_string(), "10.00");
        let posting = cash(
            CashAction::Sell,
            "18.83",
            "24655172099",
            PURCHASE + 16 * SECOND,
        );
        let mut restored = live.restored();
        let events = checked_step(&mut live, PURCHASE + 16 * SECOND, vec![posting.clone()]);
        let restored_events =
            checked_step(&mut restored, PURCHASE + 16 * SECOND, vec![posting.clone()]);
        assert_eq!(
            events
                .iter()
                .map(FinancialEvent::to_line)
                .collect::<Vec<_>>(),
            restored_events
                .iter()
                .map(FinancialEvent::to_line)
                .collect::<Vec<_>>()
        );
        let EventKind::Settled {
            outcome,
            credit,
            profit,
            path: Some(path),
            discrepancy,
            settlement_time_micros,
            transaction_ref,
            ..
        } = &events[1].kind
        else {
            panic!("{events:?}")
        };
        assert_eq!(
            (
                *outcome,
                credit.to_string(),
                profit.to_string(),
                *discrepancy
            ),
            (Outcome::Win, "18.83".into(), "8.83".into(), false)
        );
        assert_eq!(path.final_move_units, 56);
        assert_eq!(*settlement_time_micros, PURCHASE + 14 * SECOND);
        assert_eq!(transaction_ref.as_deref(), Some("24655172099"));
        assert!(
            checked_step(
                &mut live,
                PURCHASE + 17 * SECOND,
                vec![posting, end, purchase(&command, "10")]
            )
            .is_empty()
        );
        assert_eq!(live.cash(), "9963.40");
        assert_eq!(live.engine.summary().portfolio.unresolved, 0);
        assert_eq!(live.account("a").open, 0);
        // The ledger can stop after the cash fact and before its posting.
        let mut interrupted = Live::from_lines(live.lines[..live.lines.len() - 1].to_vec());
        let resumed = checked_step(&mut interrupted, PURCHASE + 16 * SECOND, vec![]);
        assert_eq!(resumed[0].to_line(), events[1].to_line());
        assert_eq!(interrupted.lines, live.lines);
    }

    #[test]
    fn zero_credit_loss_and_missing_path_are_authoritative() {
        for with_entry in [true, false] {
            let mut definition = broker_definition();
            definition.replay.contracts[0].direction = Direction::Sell;
            let (mut live, command) = prepared(definition);
            checked_step(&mut live, PURCHASE, vec![purchase(&command, "10")]);
            if with_entry {
                checked_step(
                    &mut live,
                    PURCHASE + 4 * SECOND,
                    vec![confirmed(
                        &command,
                        PURCHASE + 4 * SECOND,
                        Some(920_082),
                        Some(PURCHASE + 4 * SECOND),
                        Some(PURCHASE + 2 * SECOND),
                        Some(PURCHASE + 17 * SECOND),
                    )],
                );
            }
            let exit = with_entry.then_some((920_232, PURCHASE + 16 * SECOND));
            checked_step(
                &mut live,
                PURCHASE + 16 * SECOND,
                vec![terminal(
                    &command,
                    TerminalStatus::Lost,
                    "24655176019",
                    exit,
                )],
            );
            let events = checked_step(
                &mut live,
                PURCHASE + 18 * SECOND,
                vec![cash(
                    CashAction::Sell,
                    "0",
                    "24655176019",
                    PURCHASE + 18 * SECOND,
                )],
            );
            let EventKind::Settled {
                outcome,
                credit,
                profit,
                settlement_price_units,
                path,
                ..
            } = &events[1].kind
            else {
                panic!("{events:?}")
            };
            assert_eq!(
                (*outcome, credit.to_string(), profit.to_string()),
                (Outcome::Loss, "0.00".into(), "-10.00".into())
            );
            assert_eq!(settlement_price_units.is_some(), with_entry);
            assert_eq!(path.is_some(), with_entry);
            if let Some(path) = path {
                assert_eq!(path.final_move_units, -150);
            }
            assert_eq!(live.engine.summary().portfolio.ties, 0);
        }
    }

    #[test]
    fn cash_order_unmatched_external_and_recovered_purchase_are_durable() {
        let (mut live, command) = prepared(broker_definition());
        let buy = cash(CashAction::Buy, "-10", "fixture-buy", PURCHASE);
        checked_step(&mut live, PURCHASE, vec![buy.clone()]);
        assert!(matches!(
            live.account("a").blocked["transaction:a:fixture-buy"],
            Block::UnmatchedCash { .. }
        ));
        checked_step(&mut live, PURCHASE, vec![purchase(&command, "10")]);
        assert!(live.account("a").blocked.is_empty());
        assert!(checked_step(&mut live, PURCHASE, vec![buy]).is_empty());
        let at = PURCHASE + 16 * SECOND;
        let sell = cash(CashAction::Sell, "18.83", "sell-before-terminal", at);
        let events = checked_step(&mut live, at, vec![sell]);
        assert!(
            matches!(&events[0].kind, EventKind::CashObserved { matched: Some(found), .. } if found == &command)
        );
        assert_eq!(live.account("a").open, 1);
        checked_step(
            &mut live,
            at,
            vec![terminal(
                &command,
                TerminalStatus::Won,
                "sell-before-terminal",
                None,
            )],
        );
        assert_eq!(live.cash(), "9963.40");
        let external = cash(CashAction::Sell, "4.20", "external", at);
        checked_step(&mut live, at, vec![external.clone()]);
        assert!(!live.account("a").blocked.is_empty());
        checked_step(
            &mut live,
            at,
            vec![reconciliation(
                "transaction:a:external",
                at,
                Resolution::External,
            )],
        );
        assert!(live.account("a").blocked.is_empty());
        assert!(checked_step(&mut live, at, vec![external]).is_empty());
        assert_eq!(live.cash(), "9963.40");
        let (mut unknown, command) = prepared(broker_definition());
        checked_step(
            &mut unknown,
            PURCHASE,
            vec![Observation::PossiblySent {
                command: command.clone(),
                source: source("uncertain", PURCHASE),
            }],
        );
        checked_step(
            &mut unknown,
            PURCHASE + SECOND,
            vec![reconciliation(
                &command,
                PURCHASE + SECOND,
                Resolution::Purchased {
                    debit: decimal("10"),
                    liability: liability(),
                },
            )],
        );
        assert!(unknown.account("a").blocked.is_empty());
        assert_eq!(unknown.account("a").open, 1);
        assert_eq!(unknown.engine.summary().portfolio.unresolved, 0);
    }

    #[test]
    fn version_guards_and_broker_request_validation() {
        let mut old = broker_definition();
        old.schema_version = REPLAY_SCHEMA_VERSION;
        assert!(
            Engine::new(old)
                .err()
                .unwrap()
                .contains("schema_version 1 forbids")
        );
        let mut historical = Live::new(definition(|_| {}));
        historical.simulate(10, vec![tick(10, 500), row(0, 10, 10, true)]);
        historical.lines.push(
            FinancialEvent {
                sequence: historical.engine.sequence(),
                time_micros: 10,
                kind: EventKind::Confirmed {
                    command: "b1/10".into(),
                    source: source("invalid-v1", 10),
                    entry_price_units: None,
                    entry_time_micros: None,
                    start_micros: None,
                    expiry_micros: Some(20),
                },
            }
            .to_line(),
        );
        assert!(
            Engine::restore(historical.lines.into_iter().map(Ok))
                .err()
                .unwrap()
                .contains("schema_version 1 cannot")
        );
        let mut bad = broker_definition();
        bad.replay.contracts[0].win.gross_return = decimal("18.83");
        assert!(
            Engine::new(bad)
                .err()
                .unwrap()
                .contains("its economics come from proposals")
        );
        let mut bad = broker_definition();
        bad.replay.risk_policies[0].max_proposal_age_micros = None;
        assert!(
            Engine::new(bad)
                .err()
                .unwrap()
                .contains("requires max_proposal_age_micros")
        );
        let mut bad = broker_definition();
        bad.replay.bindings[0].envelope.semantics = None;
        assert!(
            Engine::new(bad)
                .err()
                .unwrap()
                .contains("semantics must equal")
        );
    }
    #[test]
    fn shared_frame_sources_and_closed_contract_updates_are_idempotent() {
        let (mut live, command) = prepared(broker_definition());
        checked_step(&mut live, PURCHASE, vec![purchase(&command, "10")]);
        let at = PURCHASE + 16 * SECOND;
        let frame = source("one-terminal-frame", at);
        let update = Observation::ContractUpdate {
            command: command.clone(),
            source: frame.clone(),
            entry_price_units: Some(920_252),
            entry_time_micros: Some(PURCHASE + 2 * SECOND),
            start_micros: Some(PURCHASE),
            expiry_micros: Some(PURCHASE + 15 * SECOND),
        };
        let end = Observation::Terminal {
            command: command.clone(),
            source: frame,
            fact: TerminalFact {
                status: TerminalStatus::Won,
                exit_price_units: Some(920_308),
                exit_time_micros: Some(PURCHASE + 14 * SECOND),
                transaction_ref: Some("shared-frame-sell".into()),
            },
        };
        let events = checked_step(&mut live, at, vec![update.clone(), end.clone()]);
        assert_eq!(kinds(&events), ["confirmed", "unresolved"]);
        let prefix = &live.lines[..live.lines.len() - 1];
        let mut interrupted = Live::from_lines(prefix.to_vec());
        checked_step(&mut interrupted, at, vec![update.clone(), end.clone()]);
        assert_eq!(interrupted.lines, live.lines);
        checked_step(
            &mut live,
            at,
            vec![cash(CashAction::Sell, "18.83", "shared-frame-sell", at)],
        );
        let mut restored = live.restored();
        assert!(checked_step(&mut restored, at + SECOND, vec![update, end]).is_empty());
        assert_eq!(restored.cash(), "9963.40");
        let bad = confirmed(
            &command,
            at + SECOND,
            None,
            None,
            None,
            Some(PURCHASE + 17 * SECOND),
        );
        assert!(
            restored
                .try_step(at + SECOND, vec![bad])
                .unwrap_err()
                .contains("contradicts its closed contract")
        );
    }

    #[test]
    fn mismatched_cash_stays_blocked_in_either_arrival_order() {
        for terminal_first in [true, false] {
            let (mut live, command) = prepared(broker_definition());
            checked_step(&mut live, PURCHASE, vec![purchase(&command, "10")]);
            let at = PURCHASE + 16 * SECOND;
            let end = terminal(&command, TerminalStatus::Won, "right-sell", None);
            if terminal_first {
                checked_step(&mut live, at, vec![end.clone()]);
            }
            checked_step(
                &mut live,
                at,
                vec![cash(CashAction::Sell, "1.00", "wrong-sell", at)],
            );
            if !terminal_first {
                checked_step(&mut live, at, vec![end]);
            }
            assert!(
                live.account("a")
                    .blocked
                    .contains_key("transaction:a:wrong-sell")
            );
            checked_step(
                &mut live,
                at,
                vec![cash(CashAction::Sell, "18.83", "right-sell", at)],
            );
            assert_eq!(live.account("a").open, 0);
            assert!(
                live.account("a")
                    .blocked
                    .contains_key("transaction:a:wrong-sell")
            );
            assert_eq!(live.cash(), "9963.40");
            checked_step(
                &mut live,
                at,
                vec![reconciliation(
                    "transaction:a:wrong-sell",
                    at,
                    Resolution::External,
                )],
            );
            assert!(live.account("a").blocked.is_empty());
        }
        // Different accounts cannot establish each other's liabilities.
        let mut definition = broker_definition();
        let mut second = definition.replay.accounts[0].clone();
        second.id = "other-account".into();
        definition.replay.accounts.push(second);
        let (mut live, command) = prepared(definition);
        checked_step(&mut live, PURCHASE, vec![purchase(&command, "10")]);
        let at = PURCHASE + 16 * SECOND;
        let mut other_cash = cash(CashAction::Sell, "18.83", "other-account-cash", at);
        if let Observation::Cash { fact, .. } = &mut other_cash {
            fact.account = "other-account".into();
        }
        checked_step(
            &mut live,
            at,
            vec![
                other_cash,
                terminal(&command, TerminalStatus::Won, "other-account-cash", None),
            ],
        );
        assert_eq!(live.account("a").open, 1);
        assert!(
            live.account("other-account")
                .blocked
                .contains_key("transaction:other-account:other-account-cash")
        );
    }

    #[test]
    fn both_cash_actions_before_acceptance_survive_and_recovered_projection_agrees() {
        let (mut live, command) = prepared(broker_definition());
        checked_step(
            &mut live,
            PURCHASE,
            vec![Observation::PossiblySent {
                command: command.clone(),
                source: source("uncertain", PURCHASE),
            }],
        );
        let at = PURCHASE + 16 * SECOND;
        checked_step(
            &mut live,
            at,
            vec![
                cash(CashAction::Buy, "-10", "fixture-buy", PURCHASE),
                cash(CashAction::Sell, "18.83", "pre-acceptance-sell", at),
            ],
        );
        assert_eq!(live.account("a").blocked.len(), 3);
        checked_step(
            &mut live,
            at,
            vec![reconciliation(
                &command,
                at,
                Resolution::Purchased {
                    debit: decimal("10"),
                    liability: liability(),
                },
            )],
        );
        assert!(live.account("a").blocked.is_empty());
        checked_step(
            &mut live,
            at,
            vec![terminal(
                &command,
                TerminalStatus::Won,
                "pre-acceptance-sell",
                None,
            )],
        );
        let projected = binary_alpha_engine::search::project_splits(
            live.lines
                .iter()
                .map(|line| FinancialEvent::from_line(line).unwrap()),
            "u",
        );
        assert_eq!(projected["b1"]["none"], live.engine.summary().portfolio);
        assert_eq!(live.cash(), "9963.40");
        assert!(
            checked_step(
                &mut live,
                at,
                vec![
                    cash(CashAction::Buy, "-10.00", "fixture-buy", PURCHASE),
                    cash(CashAction::Sell, "18.830", "pre-acceptance-sell", at)
                ]
            )
            .is_empty()
        );
        let wrong = cash(CashAction::Sell, "18.82", "pre-acceptance-sell", at);
        assert!(
            live.try_step(at, vec![wrong])
                .unwrap_err()
                .contains("different cash fact")
        );
    }

    #[test]
    fn admitted_proposals_keep_separate_money_and_authoritative_reconciliation() {
        let (mut live, first) = prepared(broker_definition());
        let next = PURCHASE + SECOND;
        let mut proposal_two = quote(&live, "two", next, "19.54");
        proposal_two.terms.quoted_cost = decimal("9.50");
        let events = checked_step(
            &mut live,
            next,
            vec![
                proposal(proposal_two),
                tick(next, 919_315),
                row(0, next, next, true),
            ],
        );
        let second = Live::command(&events);
        assert_eq!(live.account("a").reserved.to_string(), "19.50");
        checked_step(&mut live, next, vec![purchase(&first, "10")]);
        let mut second_liability = liability();
        second_liability.contract_ref = "second-contract".into();
        second_liability.transaction_ref = "second-buy".into();
        second_liability.purchase_time_micros = next;
        second_liability.payout = decimal("19.54");
        let events = checked_step(
            &mut live,
            next,
            vec![Observation::Purchased {
                command: second.clone(),
                source: source("second-purchase", next),
                debit: decimal("9.50"),
                liability: second_liability,
            }],
        );
        assert!(matches!(
            &events[0].kind,
            EventKind::Accepted {
                discrepancy: false,
                ..
            }
        ));
        assert_eq!(live.account("a").paid_basis.to_string(), "19.50");
        checked_step(
            &mut live,
            next,
            vec![reconciliation(
                &first,
                next,
                Resolution::Settled {
                    outcome: Outcome::Win,
                    gross_return: decimal("18.83"),
                    terminal_fee: decimal("0"),
                },
            )],
        );
        checked_step(
            &mut live,
            next,
            vec![reconciliation(
                &second,
                next,
                Resolution::Settled {
                    outcome: Outcome::Loss,
                    gross_return: decimal("0"),
                    terminal_fee: decimal("0"),
                },
            )],
        );
        assert_eq!(live.account("a").completed_profit.to_string(), "-0.67");
        let projected = binary_alpha_engine::search::project_splits(
            live.lines
                .iter()
                .map(|line| FinancialEvent::from_line(line).unwrap()),
            "u",
        );
        assert_eq!(projected["b1"]["none"], live.engine.summary().portfolio);
    }

    fn rejects_altered(live: &Live, mut edit: impl FnMut(&mut EventKind) -> bool, expected: &str) {
        let mut events = live
            .lines
            .iter()
            .map(|line| FinancialEvent::from_line(line).unwrap())
            .collect::<Vec<_>>();
        assert!(events.iter_mut().any(|event| edit(&mut event.kind)));
        let error = Engine::restore(events.iter().map(|event| Ok(event.to_line())))
            .err()
            .unwrap();
        assert!(error.contains(expected), "{error}");
    }

    #[test]
    fn altered_broker_ledger_facts_and_postings_fail_restoration() {
        let (mut live, command) = prepared(broker_definition());
        checked_step(&mut live, PURCHASE, vec![purchase(&command, "10")]);
        let at = PURCHASE + 16 * SECOND;
        checked_step(
            &mut live,
            at,
            vec![terminal(
                &command,
                TerminalStatus::Won,
                "verified-sell",
                None,
            )],
        );
        checked_step(
            &mut live,
            at,
            vec![cash(CashAction::Sell, "18.83", "verified-sell", at)],
        );
        rejects_altered(
            &live,
            |kind| {
                if let EventKind::Signal {
                    proposal: Some(proposal),
                    ..
                } = kind
                {
                    proposal.terms.quoted_cost = decimal("9.50");
                    true
                } else {
                    false
                }
            },
            "reservation",
        );
        rejects_altered(
            &live,
            |kind| {
                if let EventKind::Signal {
                    proposal: Some(proposal),
                    ..
                } = kind
                {
                    proposal.terms.direction = Direction::Sell;
                    true
                } else {
                    false
                }
            },
            "contract request",
        );
        rejects_altered(
            &live,
            |kind| {
                if let EventKind::Accepted { debit, .. } = kind {
                    *debit = decimal("10.50");
                    true
                } else {
                    false
                }
            },
            "acceptance postings",
        );
        rejects_altered(
            &live,
            |kind| {
                if let EventKind::Unresolved {
                    terminal: Some(fact),
                    ..
                } = kind
                {
                    fact.transaction_ref = Some("wrong-sell".into());
                    true
                } else {
                    false
                }
            },
            "matching disagrees",
        );
        rejects_altered(
            &live,
            |kind| {
                if let EventKind::Settled { gross_return, .. } = kind {
                    *gross_return = decimal("18.82");
                    true
                } else {
                    false
                }
            },
            "terminal and cash evidence",
        );
        rejects_altered(
            &live,
            |kind| {
                if let EventKind::Settled {
                    transaction_ref, ..
                } = kind
                {
                    *transaction_ref = Some("wrong-sell".into());
                    true
                } else {
                    false
                }
            },
            "terminal and cash evidence",
        );
    }
    #[test]
    fn purchase_discrepancies_survive_settlement_and_external_closure() {
        for purchase_first in [true, false] {
            let (mut live, command) = prepared(broker_definition());
            checked_step(&mut live, PURCHASE, vec![purchase(&command, "10.50")]);
            let at = PURCHASE + 16 * SECOND;
            checked_step(
                &mut live,
                at,
                vec![
                    terminal(&command, TerminalStatus::Won, "discrepant-sell", None),
                    cash(CashAction::Sell, "18.82", "discrepant-sell", at),
                ],
            );
            assert_eq!(live.account("a").blocked.len(), 2);
            assert!(matches!(
                live.account("a").blocked[&command],
                Block::Purchased { .. }
            ));
            let before = live.cash();
            let purchase = Resolution::Purchased {
                debit: decimal("10.50"),
                liability: liability(),
            };
            let settled = Resolution::Settled {
                outcome: Outcome::Win,
                gross_return: decimal("18.82"),
                terminal_fee: decimal("0"),
            };
            let resolutions = if purchase_first {
                [purchase, settled]
            } else {
                [settled, purchase]
            };
            for (index, resolution) in resolutions.into_iter().enumerate() {
                checked_step(
                    &mut live,
                    at + index as i64,
                    vec![reconciliation(&command, at + index as i64, resolution)],
                );
                assert_eq!(live.account("a").blocked.len(), 1 - index);
                assert_eq!(live.cash(), before);
            }
        }
        let (mut live, command) = prepared(broker_definition());
        checked_step(&mut live, PURCHASE, vec![purchase(&command, "10.50")]);
        let at = PURCHASE + 16 * SECOND;
        checked_step(
            &mut live,
            at,
            vec![
                terminal(&command, TerminalStatus::Sold, "sold", None),
                cash(CashAction::Sell, "4.20", "sold", at),
            ],
        );
        assert!(matches!(
            live.account("a").blocked[&command],
            Block::Purchased { .. }
        ));
        checked_step(
            &mut live,
            at + 1,
            vec![reconciliation(
                &command,
                at + 1,
                Resolution::Purchased {
                    debit: decimal("10.50"),
                    liability: liability(),
                },
            )],
        );
        assert!(live.account("a").blocked.is_empty());
        assert_eq!(live.cash(), "9948.27");
    }

    #[test]
    fn partial_acceptance_clocks_are_optional_but_causal_on_restore() {
        let (mut live, command) = prepared(broker_definition());
        checked_step(&mut live, PURCHASE, vec![purchase(&command, "10")]);
        let mut events = live
            .lines
            .iter()
            .map(|line| FinancialEvent::from_line(line).unwrap())
            .collect::<Vec<_>>();
        if let EventKind::Accepted {
            entry_price_units,
            entry_time_micros,
            due_time_micros,
            ..
        } = &mut events.last_mut().unwrap().kind
        {
            *entry_price_units = Some(920_252);
            *entry_time_micros = Some(PURCHASE);
            *due_time_micros = Some(PURCHASE + 15 * SECOND);
        }
        let restored = Engine::restore(events.iter().map(|event| Ok(event.to_line()))).unwrap();
        assert_eq!(restored.accounts()[0].paid_basis.to_string(), "10.00");
        rejects_altered(
            &live,
            |kind| {
                if let EventKind::Accepted {
                    entry_time_micros, ..
                } = kind
                {
                    *entry_time_micros = Some(PURCHASE + SECOND);
                    true
                } else {
                    false
                }
            },
            "confirmed clocks",
        );
        rejects_altered(
            &live,
            |kind| {
                if let EventKind::Accepted {
                    price_time_micros, ..
                } = kind
                {
                    *price_time_micros = Some(PURCHASE + SECOND);
                    true
                } else {
                    false
                }
            },
            "acceptance clocks",
        );
        let mut bad = live.restored();
        assert!(
            bad.try_step(
                PURCHASE,
                vec![confirmed(
                    &command,
                    PURCHASE,
                    None,
                    Some(PURCHASE + SECOND),
                    None,
                    None
                )]
            )
            .unwrap_err()
            .contains("confirmed clocks")
        );
        let mut wrong_purchase = Live::from_lines(live.lines[..live.lines.len() - 1].to_vec());
        let mut late = liability();
        late.purchase_time_micros = PURCHASE - 1;
        assert!(
            wrong_purchase
                .try_step(
                    PURCHASE,
                    vec![Observation::Purchased {
                        command,
                        source: source("bad-clock", PURCHASE),
                        debit: decimal("10"),
                        liability: late
                    }]
                )
                .unwrap_err()
                .contains("acceptance clocks")
        );
    }

    #[test]
    fn historical_command_refuses_broker_settlement_before_reading_inputs() {
        let scratch = Scratch::new("broker_replay_scope");
        let mut config = Config::parse(&format!("{HEAD}{BASE}")).unwrap();
        config.replay = Some(broker_definition().replay);
        let path = scratch.path("broker.toml");
        fs::write(&path, config.canonical_toml()).unwrap();
        let error = common::command(&["replay", "--config", path.to_str().unwrap()]).unwrap_err();
        assert!(error.contains("broker_authoritative_v1 settlement needs a broker; research and historical replay use price_at_due_v1"), "{error}");
    }

    #[test]
    fn cash_ambiguity_resolves_without_leaving_a_consumed_transaction_blocked() {
        let (mut live, command) = prepared(broker_definition());
        checked_step(&mut live, PURCHASE, vec![purchase(&command, "10")]);
        let at = PURCHASE + 16 * SECOND;
        let mut end = terminal(&command, TerminalStatus::Won, "unused", None);
        if let Observation::Terminal { fact, .. } = &mut end {
            fact.transaction_ref = None;
        }
        checked_step(
            &mut live,
            at,
            vec![
                cash(CashAction::Sell, "18.83", "right", at),
                cash(CashAction::Sell, "1", "wrong", at),
                end,
            ],
        );
        assert_eq!(live.account("a").blocked.len(), 2);
        let mut restored = live.restored();
        let events = checked_step(
            &mut restored,
            at,
            vec![reconciliation(
                "transaction:a:wrong",
                at,
                Resolution::External,
            )],
        );
        assert_eq!(kinds(&events), ["reconciled", "settled"]);
        assert!(restored.account("a").blocked.is_empty());
        assert_eq!(restored.cash(), "9963.40");
        assert_eq!(restored.account("a").open, 0);
    }

    #[test]
    fn broker_ticks_before_terminal_restore_byte_identical_continuation() {
        let (mut live, command) = prepared(broker_definition());
        checked_step(&mut live, PURCHASE, vec![purchase(&command, "10")]);
        checked_step(
            &mut live,
            PURCHASE,
            vec![confirmed(
                &command,
                PURCHASE,
                Some(920_252),
                Some(PURCHASE),
                Some(PURCHASE),
                Some(PURCHASE + 15 * SECOND),
            )],
        );
        checked_step(
            &mut live,
            PURCHASE + SECOND,
            vec![tick(PURCHASE + SECOND, 920_409)],
        );
        let mut restored = live.restored();
        let at = PURCHASE + 16 * SECOND;
        let end = terminal(
            &command,
            TerminalStatus::Lost,
            "equal-loss",
            Some((920_252, PURCHASE + 14 * SECOND)),
        );
        let events = checked_step(&mut live, at, vec![end.clone()]);
        checked_step(&mut restored, at, vec![end]);
        assert_eq!(live.lines, restored.lines);
        let EventKind::Unresolved {
            path: Some(path), ..
        } = &events[0].kind
        else {
            panic!("{events:?}")
        };
        assert_eq!(*path, PathMetrics::new(PURCHASE));
        rejects_altered(
            &live,
            |kind| {
                if let EventKind::Unresolved {
                    path: Some(path), ..
                } = kind
                {
                    path.max_favorable_units = 157;
                    true
                } else {
                    false
                }
            },
            "terminal path",
        );
        let sell = cash(CashAction::Sell, "0", "equal-loss", at);
        let events = checked_step(&mut live, at, vec![sell.clone()]);
        checked_step(&mut restored, at, vec![sell]);
        assert_eq!(live.lines, restored.lines);
        let EventKind::Settled {
            outcome,
            profit,
            path: Some(path),
            ..
        } = &events[1].kind
        else {
            panic!("{events:?}")
        };
        assert_eq!(*outcome, Outcome::Loss);
        assert_eq!(profit.to_string(), "-10.00");
        assert_eq!(*path, PathMetrics::new(PURCHASE));
        assert_eq!(restored.cash(), "9944.57");
        assert_eq!(restored.engine.summary().portfolio.ties, 0);
    }

    fn closure_resolution(status: TerminalStatus, gross: &str) -> Resolution {
        let gross_return = decimal(gross);
        let terminal_fee = decimal("0");
        match status {
            TerminalStatus::Won | TerminalStatus::Lost => Resolution::Settled {
                outcome: if status == TerminalStatus::Won {
                    Outcome::Win
                } else {
                    Outcome::Loss
                },
                gross_return,
                terminal_fee,
            },
            _ => Resolution::ExternallyClosed {
                status,
                gross_return,
                terminal_fee,
            },
        }
    }

    #[test]
    fn contradictory_reconciled_replacement_cannot_restore_recorded_terminal_or_cash() {
        for (status, gross) in [
            (TerminalStatus::Won, "18.83"),
            (TerminalStatus::Lost, "0"),
            (TerminalStatus::Sold, "4.20"),
            (TerminalStatus::Cancelled, "4.20"),
        ] {
            let (mut live, command) = prepared(broker_definition());
            checked_step(&mut live, PURCHASE, vec![purchase(&command, "10")]);
            let at = PURCHASE + 16 * SECOND;
            checked_step(
                &mut live,
                at,
                vec![
                    terminal(&command, status, "sell", None),
                    cash(CashAction::Sell, gross, "sell", at),
                ],
            );
            for stated in [
                TerminalStatus::Won,
                TerminalStatus::Lost,
                TerminalStatus::Sold,
                TerminalStatus::Cancelled,
            ] {
                let resolution = closure_resolution(stated, "1.00");
                let expected = if stated == status {
                    "recorded cash transaction sell amount".into()
                } else {
                    format!("recorded terminal {status}")
                };
                rejects_altered(
                    &live,
                    |kind| {
                        let source = match kind {
                            EventKind::Settled { source, .. }
                            | EventKind::Reconciled {
                                source,
                                resolution: Resolution::ExternallyClosed { .. },
                                ..
                            } => source.clone(),
                            _ => return false,
                        };
                        *kind = EventKind::Reconciled {
                            command: command.clone(),
                            source,
                            resolution: resolution.clone(),
                            release: decimal("0.00"),
                            debit: decimal("0.00"),
                            credit: decimal("1.00"),
                            profit: Some(decimal("-9.00")),
                        };
                        true
                    },
                    &expected,
                );
            }
        }
    }

    #[test]
    fn live_reconciliation_cannot_contradict_recorded_terminal_or_pending_cash() {
        for status in [
            TerminalStatus::Won,
            TerminalStatus::Lost,
            TerminalStatus::Sold,
            TerminalStatus::Cancelled,
        ] {
            let (mut live, command) = prepared(broker_definition());
            checked_step(&mut live, PURCHASE, vec![purchase(&command, "10")]);
            let at = PURCHASE + 16 * SECOND;
            checked_step(
                &mut live,
                at,
                vec![terminal(&command, status, "sell", None)],
            );
            for stated in [
                TerminalStatus::Won,
                TerminalStatus::Lost,
                TerminalStatus::Sold,
                TerminalStatus::Cancelled,
            ] {
                if stated == status {
                    continue;
                }
                let mut fork = live.restored();
                let error = fork
                    .try_step(
                        at,
                        vec![reconciliation(
                            &command,
                            at,
                            closure_resolution(stated, "4.20"),
                        )],
                    )
                    .unwrap_err();
                assert!(
                    error.contains(&format!("recorded terminal {status}")),
                    "{error}"
                );
            }
            // Missing cash may be supplied without changing the recorded terminal.
            checked_step(
                &mut live,
                at,
                vec![reconciliation(
                    &command,
                    at,
                    closure_resolution(status, "4.20"),
                )],
            );
            assert_eq!(live.cash(), "9948.77");
            assert_eq!(live.account("a").open, 0);
        }
        for status in [TerminalStatus::Won, TerminalStatus::Sold] {
            let (mut live, command) = prepared(broker_definition());
            checked_step(&mut live, PURCHASE, vec![purchase(&command, "10")]);
            let at = PURCHASE + 16 * SECOND;
            checked_step(
                &mut live,
                at,
                vec![cash(CashAction::Sell, "4.20", "pending", at)],
            );
            let mut fork = live.restored();
            let error = fork
                .try_step(
                    at,
                    vec![reconciliation(
                        &command,
                        at,
                        closure_resolution(status, "0"),
                    )],
                )
                .unwrap_err();
            assert!(
                error.contains("recorded cash transaction pending amount 4.20"),
                "{error}"
            );
            // A terminal supplied by reconciliation consumes the matching cash once.
            checked_step(
                &mut live,
                at,
                vec![reconciliation(
                    &command,
                    at,
                    closure_resolution(status, "4.200"),
                )],
            );
            assert_eq!(live.cash(), "9948.77");
            assert!(live.account("a").blocked.is_empty());
            assert!(
                checked_step(
                    &mut live,
                    at,
                    vec![cash(CashAction::Sell, "4.20", "pending", at)]
                )
                .is_empty()
            );
        }
    }

    #[test]
    fn buy_transaction_clock_is_independent_in_both_purchase_arrival_orders() {
        for cash_first in [false, true] {
            let (mut live, command) = prepared(broker_definition());
            let at = PURCHASE + SECOND;
            let buy = cash(CashAction::Buy, "-10", "fixture-buy", at);
            if cash_first {
                let events = checked_step(&mut live, at, vec![buy.clone()]);
                assert!(
                    matches!(&events[0].kind, EventKind::CashObserved { fact, matched: None, .. } if fact.time_micros == at)
                );
            }
            checked_step(&mut live, at, vec![purchase(&command, "10")]);
            assert!(checked_step(&mut live, at, vec![buy.clone(), buy]).is_empty());
            assert_eq!(live.cash(), "9944.57");
            assert_eq!(live.account("a").paid_basis.to_string(), "10.00");
            assert!(live.account("a").blocked.is_empty());
            let mut restored = live.restored();
            assert!(
                checked_step(
                    &mut restored,
                    at,
                    vec![cash(CashAction::Buy, "-10.00", "fixture-buy", at)]
                )
                .is_empty()
            );
            let error = restored
                .try_step(at, vec![cash(CashAction::Buy, "-10.01", "fixture-buy", at)])
                .unwrap_err();
            assert!(
                error.contains("fixture-buy")
                    && (error.contains("contradicts") || error.contains("different cash fact")),
                "{error}"
            );
        }
    }

    #[test]
    fn scale_equivalent_liability_redelivery_is_a_noop_for_same_and_new_sources() {
        let (mut live, command) = prepared(broker_definition());
        checked_step(&mut live, PURCHASE, vec![purchase(&command, "10")]);
        let original_lines = live.lines.clone();
        for new_source in [false, true] {
            let mut repeated = purchase(&command, "10.00");
            if let Observation::Purchased {
                liability,
                source: provenance,
                ..
            } = &mut repeated
            {
                liability.payout = decimal("18.830");
                if new_source {
                    *provenance = source("other-source", PURCHASE + SECOND);
                }
            }
            assert!(checked_step(&mut live, PURCHASE + SECOND, vec![repeated]).is_empty());
        }
        assert_eq!(live.lines, original_lines);
        let mut changed = purchase(&command, "10");
        if let Observation::Purchased {
            liability,
            source: provenance,
            ..
        } = &mut changed
        {
            liability.payout = decimal("18.84");
            *provenance = source("changed-source", PURCHASE + SECOND);
        }
        assert!(
            live.try_step(PURCHASE + SECOND, vec![changed])
                .unwrap_err()
                .contains("recorded liability")
        );
    }

    #[test]
    fn discrepancy_confirmation_at_another_scale_works_open_and_closed() {
        for closed in [false, true] {
            let (mut live, command) = prepared(broker_definition());
            checked_step(&mut live, PURCHASE, vec![purchase(&command, "10.50")]);
            let at = PURCHASE + 16 * SECOND;
            if closed {
                checked_step(
                    &mut live,
                    at,
                    vec![
                        terminal(&command, TerminalStatus::Sold, "sell", None),
                        cash(CashAction::Sell, "4.20", "sell", at),
                    ],
                );
            }
            let mut equivalent = liability();
            equivalent.payout = decimal("18.830");
            let before = live.cash();
            let resolution = Resolution::Purchased {
                debit: decimal("10.500"),
                liability: equivalent,
            };
            let events = checked_step(
                &mut live,
                at,
                vec![reconciliation(&command, at, resolution)],
            );
            assert!(
                matches!(&events[0].kind, EventKind::Reconciled { release, debit, credit, profit: None, .. } if release.is_zero() && debit.is_zero() && credit.is_zero())
            );
            assert!(
                checked_step(
                    &mut live,
                    at,
                    vec![reconciliation(
                        &command,
                        at,
                        Resolution::Purchased {
                            debit: decimal("10.50"),
                            liability: liability()
                        }
                    )]
                )
                .is_empty()
            );
            assert_eq!(live.cash(), before);
            assert!(live.account("a").blocked.is_empty());
        }
    }

    #[test]
    fn purchase_block_lift_while_awaiting_cash_preserves_engine_and_projection_unresolved() {
        let (mut live, command) = prepared(broker_definition());
        checked_step(&mut live, PURCHASE, vec![purchase(&command, "10.50")]);
        let at = PURCHASE + 16 * SECOND;
        checked_step(
            &mut live,
            at,
            vec![terminal(&command, TerminalStatus::Won, "sell", None)],
        );
        checked_step(
            &mut live,
            at,
            vec![reconciliation(
                &command,
                at,
                Resolution::Purchased {
                    debit: decimal("10.50"),
                    liability: liability(),
                },
            )],
        );
        let projected = binary_alpha_engine::search::project_splits(
            live.lines
                .iter()
                .map(|line| FinancialEvent::from_line(line).unwrap()),
            "u",
        );
        assert_eq!(live.engine.summary().portfolio.unresolved, 1);
        assert_eq!(projected["b1"]["none"].unresolved, 1);
        assert_eq!(projected["b1"]["none"], live.engine.summary().portfolio);
        assert_eq!(live.cash(), "9944.07");
        assert_eq!(live.account("a").paid_basis.to_string(), "10.50");
        assert_eq!(live.account("a").unresolved_loss.to_string(), "10.50");
        assert!(live.account("a").blocked.is_empty());
        checked_step(
            &mut live,
            at,
            vec![cash(CashAction::Sell, "18.83", "sell", at)],
        );
        assert_eq!(live.cash(), "9962.90");
        assert_eq!(live.engine.summary().portfolio.unresolved, 0);
    }

    #[test]
    fn partial_confirmation_accepts_expiry_first_and_entry_first() {
        for expiry_first in [true, false] {
            let (mut live, command) = prepared(broker_definition());
            checked_step(&mut live, PURCHASE, vec![purchase(&command, "10")]);
            let at = PURCHASE + 2 * SECOND;
            let expiry = confirmed(&command, at, None, None, None, Some(PURCHASE + 15 * SECOND));
            let entry = confirmed(&command, at, Some(920_252), Some(at), Some(PURCHASE), None);
            let ordered = if expiry_first {
                [expiry, entry]
            } else {
                [entry, expiry]
            };
            for observation in &ordered {
                let events = checked_step(&mut live, at, vec![observation.clone()]);
                assert_eq!(kinds(&events), ["confirmed"]);
            }
            assert!(checked_step(&mut live, at, ordered.into_iter().rev().collect()).is_empty());
            let at = PURCHASE + 16 * SECOND;
            checked_step(
                &mut live,
                at,
                vec![
                    terminal(
                        &command,
                        TerminalStatus::Won,
                        "sell",
                        Some((920_308, PURCHASE + 14 * SECOND)),
                    ),
                    cash(CashAction::Sell, "18.83", "sell", at),
                ],
            );
            assert_eq!(live.cash(), "9963.40");
            assert_eq!(live.engine.summary().portfolio.wins, 1);
        }
    }

    #[test]
    fn unknown_expiry_ticks_are_continuity_only_and_restore_before_terminal() {
        let mut definition = broker_definition();
        definition.replay.contracts[0]
            .settlement
            .max_tick_gap_micros = SECOND;
        let (mut live, command) = prepared(definition);
        checked_step(&mut live, PURCHASE, vec![purchase(&command, "10")]);
        checked_step(
            &mut live,
            PURCHASE,
            vec![confirmed(
                &command,
                PURCHASE,
                Some(920_252),
                Some(PURCHASE),
                None,
                None,
            )],
        );
        assert!(
            checked_step(
                &mut live,
                PURCHASE + 10 * SECOND,
                vec![tick(PURCHASE + 10 * SECOND, 920_409)]
            )
            .is_empty()
        );
        assert_eq!(live.engine.summary().portfolio.unresolved, 0);
        assert_eq!(live.account("a").open, 1);
        let mut restored = live.restored();
        let at = PURCHASE + 16 * SECOND;
        let observations = vec![
            terminal(
                &command,
                TerminalStatus::Won,
                "sell",
                Some((920_308, PURCHASE + 14 * SECOND)),
            ),
            cash(CashAction::Sell, "18.83", "sell", at),
        ];
        let events = checked_step(&mut live, at, observations.clone());
        checked_step(&mut restored, at, observations);
        assert_eq!(live.lines, restored.lines);
        let EventKind::Settled {
            path: Some(path), ..
        } = &events[2].kind
        else {
            panic!("{events:?}")
        };
        assert_eq!(path.final_move_units, 56);
        assert_eq!(path.max_favorable_units, 56);
        assert_eq!(live.cash(), "9963.40");
    }

    #[test]
    fn known_entry_unavailable_exit_settles_without_price_or_path() {
        let (mut live, command) = prepared(broker_definition());
        checked_step(&mut live, PURCHASE, vec![purchase(&command, "10")]);
        checked_step(
            &mut live,
            PURCHASE,
            vec![confirmed(
                &command,
                PURCHASE,
                Some(920_252),
                Some(PURCHASE),
                Some(PURCHASE),
                None,
            )],
        );
        checked_step(
            &mut live,
            PURCHASE + SECOND,
            vec![tick(PURCHASE + SECOND, 920_409)],
        );
        let at = PURCHASE + 16 * SECOND;
        let events = checked_step(
            &mut live,
            at,
            vec![
                terminal(&command, TerminalStatus::Won, "sell", None),
                cash(CashAction::Sell, "18.83", "sell", at),
            ],
        );
        assert!(matches!(
            &events[0].kind,
            EventKind::Unresolved { path: None, .. }
        ));
        let EventKind::Settled {
            settlement_price_units: None,
            path: None,
            credit,
            profit,
            ..
        } = &events[2].kind
        else {
            panic!("{events:?}")
        };
        assert_eq!(credit.to_string(), "18.83");
        assert_eq!(profit.to_string(), "8.83");
        let line = String::from_utf8(events[2].to_line()).unwrap();
        assert!(!line.contains("settlement_price_units") && !line.contains("\"path\""));
        assert_eq!(live.cash(), "9963.40");
        assert_eq!(live.account("a").open, 0);
    }

    #[test]
    fn strict_proposal_fees_are_broker_declared_and_returns_remain_zero() {
        let mut definition = broker_definition();
        definition.replay.bindings[0].envelope.max_loss_terminal_fee = decimal("0.30");
        definition.replay.bindings[0].envelope.max_tie_terminal_fee = decimal("0.20");
        let mut live = Live::new(definition);
        let mut quoted = quote(&live, "fees", PURCHASE, "18.83");
        quoted.terms.loss.terminal_fee = decimal("0.30");
        quoted.terms.tie.terminal_fee = decimal("0.20");
        let events = checked_step(
            &mut live,
            PURCHASE,
            vec![
                proposal(quoted.clone()),
                tick(PURCHASE, 920_409),
                row(0, PURCHASE, PURCHASE, true),
            ],
        );
        assert_eq!(dispositions(&events), [Disposition::Admitted]);
        assert_eq!(live.account("a").reserved.to_string(), "10.30");
        let command = Live::command(&events);
        checked_step(&mut live, PURCHASE, vec![purchase(&command, "10")]);
        assert_eq!(live.account("a").reserved.to_string(), "0.30");
        assert_eq!(live.account("a").unresolved_loss.to_string(), "10.30");
        for loss in [true, false] {
            for invalid_return in [true, false] {
                let mut fork = live.restored();
                let mut invalid = quoted.clone();
                let cashflow = if loss {
                    &mut invalid.terms.loss
                } else {
                    &mut invalid.terms.tie
                };
                if invalid_return {
                    cashflow.gross_return = decimal("0.01");
                } else {
                    cashflow.terminal_fee = decimal("-0.01");
                }
                let error = fork
                    .try_step(PURCHASE, vec![proposal(invalid)])
                    .unwrap_err();
                assert!(
                    error.contains(if invalid_return {
                        "loss and tie returns must be zero"
                    } else {
                        "must not be negative"
                    }),
                    "{error}"
                );
            }
        }
    }

    #[test]
    fn scale_equivalent_proposal_preserves_the_installed_fact() {
        let mut live = Live::new(broker_definition());
        let original = quote(&live, "same", PURCHASE, "18.83");
        checked_step(&mut live, PURCHASE, vec![proposal(original.clone())]);
        let mut equivalent = original.clone();
        equivalent.terms.stake = decimal("10.00");
        equivalent.terms.quoted_cost = decimal("10.000");
        equivalent.terms.entry_fee = decimal("0.00");
        equivalent.terms.win.gross_return = decimal("18.830");
        equivalent.terms.win.terminal_fee = decimal("0.00");
        equivalent.terms.loss.gross_return = decimal("0.000");
        equivalent.terms.loss.terminal_fee = decimal("0.000");
        equivalent.terms.tie.gross_return = decimal("0.00");
        equivalent.terms.tie.terminal_fee = decimal("0.00");
        checked_step(&mut live, PURCHASE, vec![proposal(equivalent)]);
        let events = checked_step(
            &mut live,
            PURCHASE,
            vec![tick(PURCHASE, 920_409), row(0, PURCHASE, PURCHASE, true)],
        );
        assert!(
            matches!(&events[0].kind, EventKind::Signal { proposal: Some(recorded), .. } if recorded == &original)
        );
        assert_eq!(live.account("a").reserved.to_string(), "10.00");
    }

    #[test]
    fn broker_ledger_postings_compare_values_and_keep_account_scale() {
        let (mut live, command) = prepared(broker_definition());
        checked_step(&mut live, PURCHASE, vec![purchase(&command, "10")]);
        let at = PURCHASE + 16 * SECOND;
        checked_step(
            &mut live,
            at,
            vec![
                terminal(&command, TerminalStatus::Won, "sell", None),
                cash(CashAction::Sell, "18.83", "sell", at),
            ],
        );
        let mut events: Vec<_> = live
            .lines
            .iter()
            .map(|line| FinancialEvent::from_line(line).unwrap())
            .collect();
        for event in &mut events {
            match &mut event.kind {
                EventKind::Signal {
                    reservation: Some(reservation),
                    ..
                } => *reservation = decimal("10.000"),
                EventKind::Accepted {
                    debit, reservation, ..
                } => {
                    *debit = decimal("10.000");
                    *reservation = decimal("0.000");
                }
                EventKind::Settled {
                    credit,
                    profit,
                    release,
                    gross_return,
                    terminal_fee,
                    ..
                } => {
                    *credit = decimal("18.830");
                    *profit = decimal("8.830");
                    *release = decimal("0.000");
                    *gross_return = decimal("18.830");
                    *terminal_fee = decimal("0.000");
                }
                _ => {}
            }
        }
        let restored = Engine::restore(events.iter().map(|event| Ok(event.to_line()))).unwrap();
        assert_eq!(restored.accounts(), live.engine.accounts());
        assert_eq!(restored.state_identity(), live.engine.state_identity());
        assert_eq!(
            restored.summary().to_json(),
            live.engine.summary().to_json()
        );
    }
}

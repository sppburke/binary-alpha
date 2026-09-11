//! `binary-alpha replay` and the engine: the deterministic publication, readback, restoration,
//! and byte-identical live-adapter proofs always; the integrated financial scenario suite
//! always; and the governed reference comparison when `BINARY_ALPHA_TEST_CONFIG` names it.

mod common;

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use binary_alpha_engine::config::{Config, StreamKey};
use binary_alpha_engine::dataset::GenerationManifest;
use binary_alpha_engine::execution::{
    ColumnSpec, Decimal, Disposition, EVENTS_OBJECT_PATH, Engine, EventKind, EventSource,
    FinancialEvent, HISTORICAL_AVAILABILITY, InstrumentBinding, Observation, Outcome,
    REPLAY_SCHEMA_VERSION, ReplayManifest, Resolution, RunDefinition, SUMMARY_OBJECT_PATH,
    StreamColumns, Summary, UnresolvedReason, basis_points_text,
};
use binary_alpha_engine::features::{FeatureManifest, Kind, Value};
use binary_alpha_engine::market::format_event_time_micros;
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

const TICK_INSTRUMENT: &str = "\n[[instruments]]\nbroker = \"pocket_option\"\nprovider_symbol = \"AEDCNY_otc\"\nbase_currency = \"AED\"\nquote_currency = \"CNY\"\nprice_scale = 6\nnative_granularity = { kind = \"tick\" }\ngap = { max_seconds = 2, reopen_seconds = 60 }\nfrozen = { min_observations = 10, min_seconds = 5 }\njump = { min_basis_points = 5 }\nspan = { min_percent = 75 }\nsessions = [{ name = \"week\", open_seconds = 0, close_seconds = 604800 }]\ncandles = [{ duration_seconds = 5, offset_seconds = 0, min_observations = 9, hard_min_observations = 5 }, { duration_seconds = 15, offset_seconds = 5, min_observations = 29, hard_min_observations = 15 }]\n";

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
fn replay_table(tick: &Path, feature: &Path, plan_identity: &str, extra: &str) -> String {
    format!(
        "\n[replay]\nrole = \"development\"\ndecision_start = \"2026-01-05T00:00:00Z\"\ndecision_end = \"2026-01-05T00:20:00Z\"\ninputs = [{{ tick_manifest = \"{}\", feature_manifest = \"{}\" }}]\nsplits = [{{ name = \"early\", start = \"2026-01-05T00:00:00Z\", end = \"2026-01-05T00:10:00Z\" }}, {{ name = \"late\", start = \"2026-01-05T00:10:00Z\", end = \"2026-01-05T00:20:00Z\" }}]\naccounts = [{{ id = \"sim\", broker = \"pocket_option\", currency = \"fixture_unit\", scale = 2, initial_cash = \"1000\" }}]\nreporting_currency = \"fixture_unit\"\nreporting_scale = 2\nmax_rate_age_micros = 0\n{extra}\n[[replay.strategies]]\nid = \"up\"\nplan_identity = \"{plan_identity}\"\nbase_stream = {{ duration_seconds = 15, offset_seconds = 5 }}\nconditions = [{{ stream = {{ duration_seconds = 15, offset_seconds = 5 }}, output = \"candle_direction\", comparator = \"eq\", threshold = \"up\" }}, {{ stream = {{ duration_seconds = 5, offset_seconds = 0 }}, output = \"candle_direction\", comparator = \"ne\", threshold = \"flat\" }}]\n\n[[replay.strategies]]\nid = \"down\"\nplan_identity = \"{plan_identity}\"\nbase_stream = {{ duration_seconds = 15, offset_seconds = 5 }}\nconditions = [{{ stream = {{ duration_seconds = 15, offset_seconds = 5 }}, output = \"candle_direction\", comparator = \"eq\", threshold = \"down\" }}, {{ stream = {{ duration_seconds = 5, offset_seconds = 0 }}, output = \"candle_direction\", comparator = \"ne\", threshold = \"flat\" }}]\n\n[[replay.contracts]]\nid = \"buy_10s\"\ndirection = \"buy\"\nduration_micros = 10000000\ncurrency = \"fixture_unit\"\nstake = \"1\"\nquoted_cost = \"1\"\nentry_fee = \"0\"\nwin = {{ gross_return = \"1.92\", terminal_fee = \"0\" }}\nloss = {{ gross_return = \"0\", terminal_fee = \"0\" }}\ntie = {{ gross_return = \"1\", terminal_fee = \"0\" }}\nsettlement = {{ rule = \"price_at_due_v1\", max_settlement_delay_micros = 60000000, max_tick_gap_micros = 60000000 }}\n\n[[replay.contracts]]\nid = \"sell_10s\"\ndirection = \"sell\"\nduration_micros = 10000000\ncurrency = \"fixture_unit\"\nstake = \"1\"\nquoted_cost = \"1\"\nentry_fee = \"0\"\nwin = {{ gross_return = \"1.92\", terminal_fee = \"0\" }}\nloss = {{ gross_return = \"0\", terminal_fee = \"0\" }}\ntie = {{ gross_return = \"1\", terminal_fee = \"0\" }}\nsettlement = {{ rule = \"price_at_due_v1\", max_settlement_delay_micros = 60000000, max_tick_gap_micros = 60000000 }}\n\n[[replay.risk_policies]]\nid = \"one_each\"\nmax_open_per_strategy = 1\nmax_open_total = 50000\nsame_entry = \"all\"\ndeduplicate_signal_logic = false\nmax_feature_age_micros = 60000000\nmax_quote_age_micros = 0\n\n[[replay.bindings]]\nid = \"buy_on_up\"\nstrategy = \"up\"\naccount = \"sim\"\ninstrument = \"pocket_option:AEDCNY_otc\"\ncontract = \"buy_10s\"\nrisk_policy = \"one_each\"\nenvelope = {{ max_purchase_cost = \"1\", max_entry_fee = \"0\", max_win_terminal_fee = \"0\", max_loss_terminal_fee = \"0\", max_tie_terminal_fee = \"0\", min_winning_net_return = \"0.92\", settlement_rule = \"price_at_due_v1\" }}\n\n[[replay.bindings]]\nid = \"sell_on_down\"\nstrategy = \"down\"\naccount = \"sim\"\ninstrument = \"pocket_option:AEDCNY_otc\"\ncontract = \"sell_10s\"\nrisk_policy = \"one_each\"\nenvelope = {{ max_purchase_cost = \"1\", max_entry_fee = \"0\", max_win_terminal_fee = \"0\", max_loss_terminal_fee = \"0\", max_tie_terminal_fee = \"0\", min_winning_net_return = \"0.92\", settlement_rule = \"price_at_due_v1\" }}\n",
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
        &replay_table(&tick, &feature, &plan_identity, ""),
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
                entry_time_micros,
                due_time_micros,
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
                path,
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
        &replay_table(&tick, &feature, &plan_identity, "")
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
            &replay_table(&tick, &feature, &plan_identity, "").replacen(from, to, 1),
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
// Integrated financial scenarios through the engine's public surface
// ---------------------------------------------------------------------------------------------

const BASE: &str = "\n[replay]\nrole = \"development\"\ndecision_start = \"1970-01-01T00:00:00Z\"\ndecision_end = \"1970-01-01T01:00:00Z\"\ninputs = [{ tick_manifest = \"file:///p/manifests/1111111111111111111111111111111111111111111111111111111111111111/ready.json\", feature_manifest = \"file:///p/manifests/2222222222222222222222222222222222222222222222222222222222222222/ready.json\" }]\naccounts = [{ id = \"a\", broker = \"b\", currency = \"u\", scale = 2, initial_cash = \"1000\" }]\nreporting_currency = \"u\"\nreporting_scale = 2\nmax_rate_age_micros = 5\n\n[[replay.strategies]]\nid = \"s\"\nplan_identity = \"plan\"\nbase_stream = { duration_seconds = 5, offset_seconds = 0 }\nconditions = [{ stream = { duration_seconds = 5, offset_seconds = 0 }, output = \"signal\", comparator = \"eq\", threshold = true }]\n\n[[replay.contracts]]\nid = \"c\"\ndirection = \"buy\"\nduration_micros = 10\ncurrency = \"u\"\nstake = \"1\"\nquoted_cost = \"1\"\nentry_fee = \"0\"\nwin = { gross_return = \"1.92\", terminal_fee = \"0\" }\nloss = { gross_return = \"0\", terminal_fee = \"0\" }\ntie = { gross_return = \"1\", terminal_fee = \"0\" }\nsettlement = { rule = \"price_at_due_v1\", max_settlement_delay_micros = 5, max_tick_gap_micros = 60 }\n\n[[replay.risk_policies]]\nid = \"p\"\nmax_open_per_strategy = 1\nsame_entry = \"all\"\ndeduplicate_signal_logic = false\nmax_feature_age_micros = 1000\nmax_quote_age_micros = 1000\n\n[[replay.bindings]]\nid = \"b1\"\nstrategy = \"s\"\naccount = \"a\"\ninstrument = \"b:X\"\ncontract = \"c\"\nrisk_policy = \"p\"\nenvelope = { max_purchase_cost = \"1\", max_entry_fee = \"0\", max_win_terminal_fee = \"0\", max_loss_terminal_fee = \"0\", max_tie_terminal_fee = \"0\", min_winning_net_return = \"0.92\", settlement_rule = \"price_at_due_v1\" }\n";

const HEAD: &str = "schema_version = 1\nrun_mode = \"research\"\n\n[storage]\nhistorical_data_dir = \"h\"\npublication_uri = \"file:///p\"\n";

fn stream(duration_seconds: u32, offset_seconds: u32) -> StreamKey {
    StreamKey {
        duration_seconds,
        offset_seconds,
    }
}

/// A definition over one instrument with a boolean `signal` column on two streams, from the base
/// table edited by `edit`, bound to the plan `plan`.
fn definition(edit: impl FnOnce(String) -> String) -> RunDefinition {
    let source = format!("{HEAD}{}", edit(BASE.to_string()));
    let config = Config::parse(&source).unwrap_or_else(|error| panic!("{source}\n{error}"));
    let columns = || {
        vec![ColumnSpec {
            name: "signal".into(),
            source: "signal".into(),
            kind: Kind::Bool,
            encoding: None,
        }]
    };
    RunDefinition {
        schema_version: REPLAY_SCHEMA_VERSION,
        config_hash: "hash".into(),
        code_revision: "revision".into(),
        availability: "test_live".into(),
        replay: config.replay.unwrap(),
        instruments: vec![InstrumentBinding {
            instrument: "b:X".into(),
            broker: "b".to_string().try_into().unwrap(),
            provider_symbol: "X".to_string().try_into().unwrap(),
            price_scale: 2,
            tick_generation: "1".repeat(64),
            feature_generation: "2".repeat(64),
            plan_identity: "plan".into(),
            raw_identity: "raw".into(),
            outcome_generation: None,
            streams: vec![
                StreamColumns {
                    stream: stream(5, 0),
                    columns: columns(),
                },
                StreamColumns {
                    stream: stream(15, 5),
                    columns: columns(),
                },
            ],
        }],
    }
}

fn tick(time: i64, price: i64) -> Observation {
    Observation::Tick {
        instrument: 0,
        provider_time_micros: time,
        price_units: price,
    }
}

fn row(stream: usize, close: i64, known: i64, signal: bool) -> Observation {
    Observation::Row {
        instrument: 0,
        stream,
        close_time_micros: close,
        known_at_micros: known,
        values: vec![Some(Value::Bool(signal))],
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

    fn step(&mut self, time: i64, observations: Vec<Observation>) -> Vec<FinancialEvent> {
        self.engine.step(time, observations).unwrap();
        let events = self.engine.drain();
        self.lines
            .extend(events.iter().map(FinancialEvent::to_line));
        events
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

    fn cash(&self) -> String {
        self.engine.accounts()[0].cash.to_string()
    }

    fn account(&self, id: &str) -> &binary_alpha_engine::execution::AccountState {
        self.engine
            .accounts()
            .iter()
            .find(|account| account.id == id)
            .unwrap()
    }

    /// Restores the ledger so far and asserts it reproduces the live engine.
    fn assert_restorable(&self) {
        let restored = Engine::restore(self.lines.iter().cloned().map(Ok)).unwrap();
        assert_eq!(restored.state_identity(), self.engine.state_identity());
        assert_eq!(restored.summary(), self.engine.summary());
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
            EventKind::Accepted { .. } => "accepted",
            EventKind::Released { .. } => "released",
            EventKind::PossiblySent { .. } => "possibly_sent",
            EventKind::Settled { .. } => "settled",
            EventKind::Unresolved { .. } => "unresolved",
            EventKind::Reconciled { .. } => "reconciled",
            EventKind::PauseStarted { .. } => "pause_started",
            EventKind::PauseEnded { .. } => "pause_ended",
        })
        .collect()
}

#[test]
fn causality_settlement_and_gaps_follow_availability() {
    // A price tick at 100 available only at 103 admits at decision 103 with duration ten:
    // entry 103, due 113, and the quote keeps its provider time.
    let mut live = Live::new(definition(|base| base));
    let events = live.simulate(103, vec![tick(100, 500), row(0, 100, 103, true)]);
    assert_eq!(kinds(&events), ["signal", "accepted"]);
    let EventKind::Accepted {
        entry_time_micros,
        due_time_micros,
        price_time_micros,
        entry_price_units,
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
        path,
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
    let account = live.account("a").clone();
    assert_eq!(
        (
            account.open,
            account.paid_basis.to_string(),
            account.unresolved_loss.to_string(),
            account.cash.to_string()
        ),
        (1, "1.00".into(), "1.00".into(), "999.92".into())
    );
    let events = live.simulate(200, vec![tick(200, 531), row(0, 195, 200, true)]);
    assert_eq!(
        dispositions(&events),
        [Disposition::CapacityStrategy],
        "no refund frees the slot"
    );
    let events = live.step(
        210,
        vec![Observation::Settlement {
            command: "b1/110".into(),
            source: source("broker:1", 205),
            outcome: Outcome::Loss,
            gross_return: decimal("0"),
            terminal_fee: decimal("0"),
            settlement_price_units: 400,
        }],
    );
    assert_eq!(kinds(&events), ["settled"]);
    let account = live.account("a").clone();
    assert_eq!(
        (
            account.open,
            account.cash.to_string(),
            account.completed_profit.to_string()
        ),
        (0, "999.92".into(), "-0.08".into())
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
    let mut live = Live::new(definition(|base| {
        base.replace("duration_micros = 10", "duration_micros = 1000")
    }));
    live.simulate(50, vec![tick(50, 500), row(0, 45, 50, true)]);
    let settlement = Observation::Settlement {
        command: "b1/45".into(),
        source: source("broker:settle", 100),
        outcome: Outcome::Win,
        gross_return: decimal("1.92"),
        terminal_fee: decimal("0"),
        settlement_price_units: 510,
    };
    let events = live.simulate(100, vec![tick(100, 510), row(0, 95, 100, true)]);
    assert_eq!(dispositions(&events), [Disposition::CapacityStrategy]);
    let mut delayed = settlement;
    if let Observation::Settlement { source, .. } = &mut delayed {
        source.available_at_micros = 103;
    }
    assert!(
        live.engine.step(102, vec![delayed.clone()]).is_err(),
        "not yet available"
    );
    let events = live.step(103, vec![delayed.clone(), row(0, 100, 103, true)]);
    assert_eq!(kinds(&events), ["settled", "signal"]);
    assert_eq!(dispositions(&events), [Disposition::Admitted]);
    // The same external identity and payload again is a no-op; a conflicting payload fails.
    let events = live.step(104, vec![delayed.clone()]);
    assert!(events.is_empty());
    let mut conflicting = delayed;
    if let Observation::Settlement { outcome, .. } = &mut conflicting {
        *outcome = Outcome::Loss;
    }
    assert!(
        live.engine
            .step(104, vec![conflicting])
            .unwrap_err()
            .contains("reconciliation failed")
    );
    live.assert_restorable();
}

#[test]
fn alignment_follows_the_latest_row_of_the_other_stream() {
    let definition = definition(|base| {
        base.replace(
            "conditions = [{ stream = { duration_seconds = 5, offset_seconds = 0 }, output = \"signal\", comparator = \"eq\", threshold = true }]",
            "conditions = [{ stream = { duration_seconds = 5, offset_seconds = 0 }, output = \"signal\", comparator = \"eq\", threshold = true }, { stream = { duration_seconds = 15, offset_seconds = 5 }, output = \"signal\", comparator = \"eq\", threshold = true }]",
        )
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
fn freshness_bounds_are_exact() {
    let fresh = |feature: i64, quote: i64| {
        definition(|base| {
            base.replace(
                "max_feature_age_micros = 1000\nmax_quote_age_micros = 1000",
                &format!("max_feature_age_micros = {feature}\nmax_quote_age_micros = {quote}"),
            )
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
    assert_eq!(
        dispositions(&live.simulate(71, vec![tick(71, 500), row(0, 70, 71, true)])),
        [Disposition::GapAtEntry]
    );
    assert_eq!(
        dispositions(&live.simulate(131, vec![tick(131, 500), row(0, 130, 131, true)])),
        [Disposition::Admitted],
        "a gap of exactly the maximum is permitted"
    );
}

#[test]
fn selection_deduplication_and_repair_keep_their_slots() {
    let two = |same_entry: &str, dedup: bool, repair: &str| {
        definition(|base| {
            base.replace("same_entry = \"all\"", &format!("same_entry = \"{same_entry}\""))
                .replace("deduplicate_signal_logic = false", &format!("deduplicate_signal_logic = {dedup}"))
                .replace("threshold = true }]\n", &format!("threshold = true }}]\n{repair}\n[[replay.strategies]]\nid = \"t\"\nplan_identity = \"plan\"\nbase_stream = {{ duration_seconds = 5, offset_seconds = 0 }}\nconditions = [{{ stream = {{ duration_seconds = 5, offset_seconds = 0 }}, output = \"signal\", comparator = \"eq\", threshold = true }}]\n"))
                + "\n[[replay.bindings]]\nid = \"b2\"\nstrategy = \"t\"\naccount = \"a\"\ninstrument = \"b:X\"\ncontract = \"c\"\nrisk_policy = \"p\"\nenvelope = { max_purchase_cost = \"2\", max_entry_fee = \"0\", max_win_terminal_fee = \"0\", max_loss_terminal_fee = \"0\", max_tie_terminal_fee = \"0\", min_winning_net_return = \"0.92\", settlement_rule = \"price_at_due_v1\" }\n"
        })
    };
    let entry = |live: &mut Live| {
        dispositions(&live.simulate(
            10,
            vec![tick(10, 500), row(0, 10, 10, true), row(1, 10, 10, true)],
        ))
    };
    assert_eq!(
        entry(&mut Live::new(two("all", false, ""))),
        [Disposition::Admitted, Disposition::Admitted]
    );
    assert_eq!(
        entry(&mut Live::new(two("first", false, ""))),
        [Disposition::Admitted, Disposition::SameEntryDuplicate]
    );
    assert_eq!(
        entry(&mut Live::new(two("all", true, ""))),
        [Disposition::Admitted, Disposition::DuplicateLogic],
        "the same frozen logic at one entry event"
    );
    // A repair-blocked first match keeps its selection slot; the second matching candidate does
    // not replace it.
    let repair = "repair = [{ stream = { duration_seconds = 15, offset_seconds = 5 }, output = \"signal\", comparator = \"eq\", threshold = false }]\n";
    assert_eq!(
        entry(&mut Live::new(two("first", false, repair))),
        [Disposition::RepairBlocked, Disposition::SameEntryDuplicate]
    );
    assert_eq!(
        entry(&mut Live::new(two("all", false, repair))),
        [Disposition::RepairBlocked, Disposition::Admitted]
    );
}

#[test]
fn every_capacity_scope_cash_and_loss_limits_bind_with_equality_allowed() {
    let second = "\n[[replay.strategies]]\nid = \"t\"\nplan_identity = \"plan\"\nbase_stream = { duration_seconds = 15, offset_seconds = 5 }\nconditions = [{ stream = { duration_seconds = 15, offset_seconds = 5 }, output = \"signal\", comparator = \"eq\", threshold = true }]\n\n[[replay.bindings]]\nid = \"b2\"\nstrategy = \"t\"\naccount = \"a\"\ninstrument = \"b:X\"\ncontract = \"c\"\nrisk_policy = \"p\"\nenvelope = { max_purchase_cost = \"1\", max_entry_fee = \"0\", max_win_terminal_fee = \"0\", max_loss_terminal_fee = \"0\", max_tie_terminal_fee = \"0\", min_winning_net_return = \"0.92\", settlement_rule = \"price_at_due_v1\" }\n";
    let policy = |limits: &str| {
        definition(|base| {
            base.replace("max_open_per_strategy = 1\n", &format!("{limits}\n")) + second
        })
    };
    for (limits, expected) in [
        (
            "max_open_per_strategy = 1",
            [
                Disposition::Admitted,
                Disposition::Admitted,
                Disposition::CapacityStrategy,
            ],
        ),
        (
            "max_open_per_duration = 1",
            [
                Disposition::Admitted,
                Disposition::CapacityDuration,
                Disposition::CapacityDuration,
            ],
        ),
        (
            "max_open_per_instrument = 1",
            [
                Disposition::Admitted,
                Disposition::CapacityInstrument,
                Disposition::CapacityInstrument,
            ],
        ),
        (
            "max_open_per_account = 1",
            [
                Disposition::Admitted,
                Disposition::CapacityAccount,
                Disposition::CapacityAccount,
            ],
        ),
        (
            "max_open_total = 1",
            [
                Disposition::Admitted,
                Disposition::CapacityTotal,
                Disposition::CapacityTotal,
            ],
        ),
        (
            "max_open_total = 2",
            [
                Disposition::Admitted,
                Disposition::Admitted,
                Disposition::CapacityTotal,
            ],
        ),
    ] {
        let mut live = Live::new(policy(limits));
        let mut seen = dispositions(&live.simulate(
            10,
            vec![tick(10, 500), row(0, 10, 10, true), row(1, 10, 10, true)],
        ));
        seen.extend(dispositions(
            &live.simulate(12, vec![tick(12, 500), row(0, 12, 12, true)]),
        ));
        assert_eq!(seen, expected, "{limits}");
    }
    // Cash: admission needs native cash minus unpaid reservations to cover `A + F`; equality is
    // allowed, one cent less is not, and zero cash blocks a positive purchase.
    for (cash, expected) in [
        ("2.00", Disposition::Admitted),
        ("1.99", Disposition::InsufficientCash),
        ("0", Disposition::InsufficientCash),
    ] {
        let mut live = Live::new(policy("max_open_per_strategy = 5").tap(|definition| {
            definition.replay.accounts[0].initial_cash = decimal(cash);
        }));
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
        let mut live = Live::new(policy(&format!(
            "max_open_per_strategy = 5\nmax_unresolved_loss_per_account = \"{limit}\""
        )));
        live.simulate(10, vec![tick(10, 500), row(0, 10, 10, true)]);
        assert_eq!(
            dispositions(&live.simulate(12, vec![tick(12, 500), row(0, 12, 12, true)])),
            [expected],
            "{limit}"
        );
    }
    // A stake-one account cannot fund losses beyond its available cash.
    let mut live = Live::new(policy("max_open_per_strategy = 5").tap(|definition| {
        definition.replay.accounts[0].initial_cash = decimal("2");
    }));
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

trait Tap: Sized {
    fn tap(mut self, edit: impl FnOnce(&mut Self)) -> Self {
        edit(&mut self);
        self
    }
}

impl Tap for RunDefinition {}

#[test]
fn the_exact_cashflow_counterexample_and_fees_post_exactly() {
    let fractional = || {
        definition(|base| {
            base.replace("stake = \"1\"\nquoted_cost = \"1\"\nentry_fee = \"0\"\nwin = { gross_return = \"1.92\", terminal_fee = \"0\" }\nloss = { gross_return = \"0\", terminal_fee = \"0\" }\ntie = { gross_return = \"1\", terminal_fee = \"0\" }",
                "stake = \"10\"\nquoted_cost = \"9.50\"\nentry_fee = \"0.10\"\nwin = { gross_return = \"19\", terminal_fee = \"0.20\" }\nloss = { gross_return = \"0\", terminal_fee = \"0.30\" }\ntie = { gross_return = \"9.50\", terminal_fee = \"0.05\" }")
                .replace("max_purchase_cost = \"1\", max_entry_fee = \"0\", max_win_terminal_fee = \"0\", max_loss_terminal_fee = \"0\", max_tie_terminal_fee = \"0\", min_winning_net_return = \"0.92\"", "max_purchase_cost = \"9.5\", max_entry_fee = \"0.1\", max_win_terminal_fee = \"0.2\", max_loss_terminal_fee = \"0.3\", max_tie_terminal_fee = \"0.05\", min_winning_net_return = \"9.2\"")
                .replace("initial_cash = \"1000\"", "initial_cash = \"19\"")
        })
    };
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
    let account = live.account("a").clone();
    assert_eq!(
        (
            account.cash.to_string(),
            account.reserved.to_string(),
            account.unresolved_loss.to_string()
        ),
        ("9.40".into(), "0.30".into(), "9.90".into())
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
    let events = live.step(70, vec![tick(70, 500), row(0, 70, 70, true)]);
    let EventKind::Signal {
        command: Some(command),
        ..
    } = &events[0].kind
    else {
        unreachable!()
    };
    let command = command.clone();
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
            account.blocked.is_some()
        ),
        ("9.90".into(), "18.15".into(), true)
    );
    assert_eq!(
        dispositions(&live.simulate(72, vec![tick(72, 500), row(0, 72, 72, true)])),
        [Disposition::AccountBlocked]
    );
    live.step(
        73,
        vec![Observation::Reconciliation {
            command,
            source: source("broker:reconcile", 73),
            resolution: Resolution::NotSent,
        }],
    );
    let account = live.account("a").clone();
    assert_eq!(
        (
            account.reserved.to_string(),
            account.cash.to_string(),
            account.blocked,
            account.open
        ),
        ("0.00".into(), "18.15".into(), None, 0)
    );
    live.assert_restorable();
    // A known rejection releases without debit; an actual cashflow contradicting the frozen
    // terms is recorded as a discrepancy and blocks the account.
    let mut live = Live::new(fractional());
    let events = live.step(10, vec![tick(10, 500), row(0, 10, 10, true)]);
    let EventKind::Signal {
        command: Some(command),
        ..
    } = &events[0].kind
    else {
        unreachable!()
    };
    let events = live.step(
        11,
        vec![Observation::Rejected {
            command: command.clone(),
            source: source("broker:reject", 11),
        }],
    );
    assert_eq!(kinds(&events), ["released"]);
    assert_eq!(
        (
            live.cash(),
            live.account("a").reserved.to_string(),
            live.account("a").open
        ),
        ("19.00".into(), "0.00".into(), 0)
    );
    live.simulate(20, vec![tick(20, 500), row(0, 20, 20, true)]);
    let events = live.step(
        30,
        vec![Observation::Settlement {
            command: "b1/20".into(),
            source: source("broker:settle", 30),
            outcome: Outcome::Win,
            gross_return: decimal("18"),
            terminal_fee: decimal("0.20"),
            settlement_price_units: 600,
        }],
    );
    let EventKind::Settled {
        discrepancy,
        credit,
        ..
    } = &events[0].kind
    else {
        unreachable!()
    };
    assert!(*discrepancy);
    assert_eq!(
        credit.to_string(),
        "17.80",
        "the actual cashflow is recorded, never the configured amount"
    );
    assert!(live.account("a").blocked.is_some());
    assert_eq!(
        dispositions(&live.simulate(31, vec![tick(31, 600), row(0, 31, 31, true)])),
        [Disposition::AccountBlocked]
    );
    live.assert_restorable();
}

#[test]
fn quote_envelopes_pauses_conversion_and_projections_are_exact() {
    // Worse terms than the envelope are rejected at admission; equal terms pass.
    let mut live = Live::new(definition(|base| {
        base.replace(
            "min_winning_net_return = \"0.92\"",
            "min_winning_net_return = \"0.93\"",
        )
    }));
    assert_eq!(
        dispositions(&live.simulate(10, vec![tick(10, 500), row(0, 10, 10, true)])),
        [Disposition::QuoteRejected]
    );
    // A drawdown pause starts when the epoch drawdown reaches the threshold, blocks new entries,
    // continues settlements, and resumes at its deadline with the epoch peak reset.
    let mut live = Live::new(definition(|base| {
        base.replace(
            "max_quote_age_micros = 1000",
            "max_quote_age_micros = 1000\npause = { drawdown = \"1\", duration_micros = 100 }",
        )
        .replace("max_open_per_strategy = 1", "max_open_per_strategy = 5")
        .replace("max_tick_gap_micros = 60", "max_tick_gap_micros = 1000")
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
    // Conversion: exact same-currency rescaling, and a supplied rate only when its provider and
    // availability times are no later than the decision and its provider age is within bound.
    let rates = "rates = [{ id = \"r1\", source_currency = \"v\", reporting_currency = \"u\", provider = \"fx\", provider_time = \"1970-01-01T00:00:00.000100Z\", available_at = \"1970-01-01T00:00:00.000103Z\", rate = \"2.5\" }]\n";
    let mut live = Live::new(definition(|base| {
        base.replace(
            "max_rate_age_micros = 5\n",
            &format!("max_rate_age_micros = 5\n{rates}"),
        )
    }));
    let v: binary_alpha_engine::market::Currency = "v".to_string().try_into().unwrap();
    let u: binary_alpha_engine::market::Currency = "u".to_string().try_into().unwrap();
    live.simulate(100, vec![tick(100, 500)]);
    assert_eq!(
        live.engine
            .convert(decimal("1.5"), &u)
            .unwrap()
            .amount
            .to_string(),
        "1.50"
    );
    assert!(
        live.engine
            .convert(decimal("1"), &v)
            .unwrap_err()
            .contains("no v to u rate"),
        "future availability"
    );
    live.simulate(103, vec![tick(103, 500)]);
    let converted = live.engine.convert(decimal("1.5"), &v).unwrap();
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
    assert!(
        live.engine
            .convert(decimal("1"), &v)
            .unwrap_err()
            .contains("no v to u rate"),
        "stale beyond the maximum age"
    );
    // A total unresolved-loss limit needing an unavailable rate blocks admission explicitly.
    let mut live = Live::new(definition(|base| {
        base.replace("accounts = [{ id = \"a\", broker = \"b\", currency = \"u\", scale = 2, initial_cash = \"1000\" }]", "accounts = [{ id = \"a\", broker = \"b\", currency = \"u\", scale = 2, initial_cash = \"1000\" }, { id = \"z\", broker = \"b\", currency = \"v\", scale = 2, initial_cash = \"1000\" }]")
            .replace("max_quote_age_micros = 1000", "max_quote_age_micros = 1000\nmax_unresolved_loss_total = \"100\"")
    }));
    assert_eq!(
        dispositions(&live.simulate(10, vec![tick(10, 500), row(0, 10, 10, true)])),
        [Disposition::ConversionUnavailable]
    );
    // A zero entry price still settles and records exact movement; only the normalized
    // excursion is absent with its reason.
    let mut live = Live::new(definition(|base| base));
    live.simulate(10, vec![tick(10, 0), row(0, 10, 10, true)]);
    let events = live.simulate(20, vec![tick(20, 7)]);
    let EventKind::Settled { outcome, path, .. } = &events[0].kind else {
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
fn definitions_reject_mismatched_plans_columns_and_negative_cash_and_accept_many_conditions() {
    let error = Engine::new(
        definition(|base| base)
            .tap(|definition| definition.replay.strategies[0].plan_identity = "other".into()),
    )
    .err()
    .unwrap();
    assert!(
        error.contains("no replay input carries frozen plan"),
        "{error}"
    );
    let error = Engine::new(definition(|base| {
        base.replace("output = \"signal\"", "output = \"missing\"")
    }))
    .err()
    .unwrap();
    assert!(
        error.contains("not a compiled output or fitted encoding"),
        "{error}"
    );
    let error = Engine::new(definition(|base| {
        base.replace("threshold = true", "threshold = \"yes\"")
    }))
    .err()
    .unwrap();
    assert!(error.contains("threshold type does not match"), "{error}");
    let error = Engine::new(
        definition(|base| base)
            .tap(|definition| definition.replay.accounts[0].initial_cash = decimal("-1")),
    )
    .err()
    .unwrap();
    assert!(error.contains("initial_cash"), "{error}");
    let five = ", ".to_string() + &(0..4).map(|_| "{ stream = { duration_seconds = 15, offset_seconds = 5 }, output = \"signal\", comparator = \"eq\", threshold = true }").collect::<Vec<_>>().join(", ");
    let mut live = Live::new(definition(|base| {
        base.replace(
            "threshold = true }]",
            &format!("threshold = true }}{five}]"),
        )
    }));
    live.simulate(9, vec![row(1, 9, 9, true)]);
    assert_eq!(
        dispositions(&live.simulate(10, vec![tick(10, 500), row(0, 10, 10, true)])),
        [Disposition::Admitted]
    );
    assert!(live.simulate(15, vec![row(1, 15, 15, false)]).is_empty());
    assert!(
        live.simulate(16, vec![tick(16, 500), row(0, 16, 16, true)])
            .is_empty(),
        "one false condition among five fails the conjunction"
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

fn read_le<T, const N: usize>(path: &Path, decode: fn([u8; N]) -> T) -> Vec<T> {
    fs::read(path)
        .unwrap()
        .as_chunks::<N>()
        .0
        .iter()
        .map(|chunk| decode(*chunk))
        .collect()
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
    first_favorable: Option<i64>,
    first_adverse: Option<i64>,
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
        first_favorable: None,
        first_adverse: None,
    };
    for index in entry + 1..=settlement {
        let raw = if entry_price <= 0.0 {
            0.0
        } else {
            (prices[index] as f64 / 1e6 - entry_price) / entry_price * 10_000.0
        };
        let movement = if sell { -raw } else { raw };
        path.final_move = round10(movement);
        if movement > 0.0 && path.first_favorable.is_none() {
            path.first_favorable = Some(times[index]);
        }
        if movement < 0.0 && path.first_adverse.is_none() {
            path.first_adverse = Some(times[index]);
        }
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
    disposition: Disposition,
    known_at: i64,
    quote: i64,
    split: Option<String>,
    command: Option<String>,
    cash_before: Decimal,
    /// The command holding the binding's strategy slot at decision time, and whether it was
    /// unresolved then.
    holder: Option<(String, bool)>,
}

struct TargetSettlement {
    time: i64,
    price: i64,
    outcome: Outcome,
    path: binary_alpha_engine::execution::PathMetrics,
}

#[test]
#[ignore = "needs BINARY_ALPHA_TEST_CONFIG naming the research configuration and the reference root"]
fn governed_reference_parity() {
    use binary_alpha_engine::market::{PriceScale, parse_price_units};
    use binary_alpha_engine::outcomes::{
        InvalidReason, OutcomeBuilder, OutcomeManifest, TICK_PRICE_OBJECT_PATH,
        TICK_TIME_OBJECT_PATH, stream_object_paths,
    };
    use std::collections::HashMap;

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
    // files have the recorded rows and header widths.
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
            assert!(
                mapping.contains_key(column),
                "{name}: field {column} has no mapping"
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
        let contract = settings
            .contracts
            .iter()
            .find(|c| c.id == binding.contract)
            .unwrap();
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
                contract.win.gross_return.to_string(),
                contract.tie.gross_return.to_string()
            ),
            ("1".into(), "1".into(), "1.92".into(), "1".into())
        );
        assert_eq!(
            contract.settlement.max_tick_gap_micros,
            run["max_valid_tick_gap_ms"].as_i64().unwrap() * 1000
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
        assert_eq!(policy.same_entry.to_string(), "all");
        assert!(!policy.deduplicate_signal_logic && policy.pause.is_none());
    }
    assert_eq!(
        settings.accounts[0].initial_cash.to_string(),
        run["starting_amount"].as_str().unwrap()
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
    let times = read_le(&object(TICK_TIME_OBJECT_PATH), i64::from_le_bytes);
    let prices = read_le(&object(TICK_PRICE_OBJECT_PATH), i64::from_le_bytes);
    assert_eq!(times.len() as u64, fixture.source_rows);
    let builder = OutcomeBuilder::new(outcome.rule.clone(), times.clone(), prices.clone()).unwrap();
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

    // The target ledger, indexed by signal identity and command, with the cash and the
    // strategy-slot holder at each decision.
    let ledger = ledger_lines(&object_path(&store, &manifest, EVENTS_OBJECT_PATH));
    let mut signals: HashMap<(String, i64), TargetSignal> = HashMap::new();
    let mut accepted: HashMap<String, (i64, i64, i64)> = HashMap::new();
    let mut settled: HashMap<String, TargetSettlement> = HashMap::new();
    let mut unresolved: HashMap<String, (UnresolvedReason, String)> = HashMap::new();
    let mut cash = settings.accounts[0].initial_cash.rescale(2).unwrap();
    let mut holder: HashMap<String, (String, bool)> = HashMap::new();
    let mut binding_of: HashMap<String, String> = HashMap::new();
    let started = std::time::Instant::now();
    for line in &ledger {
        let event = FinancialEvent::from_line(line).unwrap();
        match event.kind {
            EventKind::RunDefinition { .. } => {}
            EventKind::Signal {
                binding,
                close_time_micros,
                known_at_micros,
                disposition,
                quote_price_units,
                split,
                command,
                ..
            } => {
                if let Some(command) = &command {
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
                                disposition,
                                known_at: known_at_micros,
                                quote: quote_price_units.unwrap(),
                                split,
                                command,
                                cash_before: cash,
                                holder: slot
                            }
                        )
                        .is_none()
                );
            }
            EventKind::Accepted {
                command,
                entry_time_micros,
                entry_price_units,
                due_time_micros,
                debit,
                ..
            } => {
                cash = cash.checked_sub(debit).unwrap();
                accepted.insert(
                    command,
                    (entry_time_micros, entry_price_units, due_time_micros),
                );
            }
            EventKind::Settled {
                command,
                settlement_time_micros,
                settlement_price_units,
                outcome,
                credit,
                path,
                ..
            } => {
                cash = cash.checked_add(credit).unwrap();
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
    let trade_columns: Vec<usize> = [
        "entry_time_utc",
        "due_time_utc",
        "settlement_tick_time_utc",
        "settlement_price",
        "outcome",
        "net_units",
        "final_directional_move_bps",
        "max_favorable_excursion_bps",
        "max_adverse_excursion_bps",
        "mfe_time_utc",
        "mae_time_utc",
        "first_favorable_time_utc",
        "first_adverse_time_utc",
        "favorable_before_adverse",
        "adverse_before_favorable",
        "split_label",
    ]
    .iter()
    .map(|name| trades.column(name))
    .collect();
    let signal_column = trades.column("signal_number");
    let mut trade_rows: HashMap<u64, Vec<String>> = HashMap::new();
    while let Some(row) = trades.next_row() {
        let number = row[signal_column].parse().unwrap();
        assert!(
            trade_rows
                .insert(
                    number,
                    trade_columns
                        .iter()
                        .map(|&index| row[index].clone())
                        .collect()
                )
                .is_none()
        );
    }
    let mut invalid = LegacyCsv::open(&root.join("parity_cpu_run_v2/invalid_trades.csv"));
    let invalid_columns: Vec<usize> = [
        "entry_time_utc",
        "due_time_utc",
        "invalidated_time_utc",
        "invalid_reason",
        "gap_start_time_utc",
        "gap_end_time_utc",
        "gap_ms",
        "split_label",
    ]
    .iter()
    .map(|name| invalid.column(name))
    .collect();
    let signal_column = invalid.column("signal_number");
    let mut invalid_rows: HashMap<u64, Vec<String>> = HashMap::new();
    while let Some(row) = invalid.next_row() {
        let number = row[signal_column].parse().unwrap();
        assert!(
            invalid_rows
                .insert(
                    number,
                    invalid_columns
                        .iter()
                        .map(|&index| row[index].clone())
                        .collect()
                )
                .is_none()
        );
    }
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

    // Every reference signal: identity, shared evaluation fields, historical diagnostics, path
    // projections, and the classified disposition.
    let started = std::time::Instant::now();
    let mut signals_csv = LegacyCsv::open(&root.join("parity_cpu_run_v2/signals.csv"));
    let column = |name: &str| signals_csv.column(name);
    let (
        c_number,
        c_candidate,
        c_candle_set,
        c_expiry,
        c_direction,
        c_decision,
        c_entry_tick,
        c_split,
        c_entry_price,
        c_opened,
        c_block,
        c_validity,
        c_predicate,
    ) = (
        column("signal_number"),
        column("candidate_id"),
        column("candle_set"),
        column("expiry_seconds"),
        column("direction"),
        column("row_decision_time_utc"),
        column("entry_tick_time_utc"),
        column("split_label"),
        column("entry_price"),
        column("opened_trade"),
        column("block_reasons"),
        column("validity_block_reason"),
        column("predicate"),
    );
    let c_regimes: Vec<usize> = regime_names.iter().map(|name| column(name)).collect();
    let by_candidate: HashMap<&str, &Candidate> = fixture
        .candidates
        .iter()
        .map(|candidate| (candidate.candidate_id.as_str(), candidate))
        .collect();
    let scale8 = PriceScale::try_from(8).unwrap();
    let mut classes: BTreeMap<&str, u64> = BTreeMap::new();
    let mut examples: BTreeMap<&str, String> = BTreeMap::new();
    let mut divergent_commands: std::collections::HashSet<String> =
        std::collections::HashSet::new();
    let mut last_divergence: HashMap<&str, u64> = HashMap::new();
    let mut path_source_rounding = 0u64;
    let mut path_time_source_rounding = 0u64;
    let mut compared_paths = 0u64;
    let mut compared_outcomes = 0u64;
    let mut reference_counts = BTreeMap::from([
        ("opened", 0u64),
        ("blocked_by_strategy_capacity", 0),
        ("blocked_by_entry_gap", 0),
        ("wins", 0),
        ("losses", 0),
        ("ties", 0),
    ]);
    let mut classify = |class: &'static str, citation: String| {
        *classes.entry(class).or_default() += 1;
        examples.entry(class).or_insert(citation);
    };
    let mut rows_seen = 0u64;
    while let Some(row) = signals_csv.next_row() {
        rows_seen += 1;
        let number: u64 = row[c_number].parse().unwrap();
        let candidate = by_candidate[row[c_candidate].as_str()];
        assert_eq!(
            (
                row[c_candle_set].as_str(),
                row[c_expiry].parse::<u32>().unwrap(),
                row[c_direction].as_str()
            ),
            (
                candidate.candle_set.as_str(),
                candidate.expiry_seconds,
                candidate.direction.as_str()
            ),
            "signal {number}"
        );
        assert_eq!(
            row[c_predicate],
            format!("{} == '{}'", candidate.output, candidate.value)
        );
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
        // Shared evaluation fields: the trigger tick, its price, the split, and the regimes.
        assert_eq!(
            target.known_at,
            micros(&row[c_entry_tick]),
            "signal {number}: entry tick"
        );
        assert_eq!(
            target.quote * 100,
            parse_price_units(&row[c_entry_price], scale8).unwrap(),
            "signal {number}: entry price"
        );
        assert_eq!(
            target.split.as_deref(),
            Some(row[c_split].as_str()),
            "signal {number}: split"
        );
        for (index, name) in regime_names.iter().enumerate() {
            assert_eq!(
                regimes[&close][index], row[c_regimes[index]],
                "signal {number}: {name}"
            );
        }
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
        let sell = candidate.direction == "SELL";
        let reference_opened = row[c_opened] == "1";
        let reference_block = row[c_block].as_str();
        let trade = trade_rows.get(&number);
        let invalidated = invalid_rows.get(&number);
        if reference_opened {
            *reference_counts.get_mut("opened").unwrap() += 1;
        }
        if let Some(trade) = trade {
            compared_outcomes += 1;
            let settlement = cell.settlement.unwrap();
            assert_eq!(
                cell.reason,
                InvalidReason::Valid,
                "signal {number}: the settled reference trade is a valid label"
            );
            assert_eq!(
                micros(&trade[0]),
                entry.event_time_micros,
                "signal {number}: entry time"
            );
            assert_eq!(
                micros(&trade[1]),
                cell.due_time_micros.unwrap(),
                "signal {number}: due time"
            );
            assert_eq!(
                micros(&trade[2]),
                settlement.event_time_micros,
                "signal {number}: settlement tick"
            );
            assert_eq!(
                settlement.price_units * 100,
                parse_price_units(&trade[3], scale8).unwrap(),
                "signal {number}: settlement price"
            );
            let expected = match (cell.outcome.unwrap(), sell) {
                (binary_alpha_engine::outcomes::Outcome::Tie, _) => "tie",
                (binary_alpha_engine::outcomes::Outcome::BuyWin, false)
                | (binary_alpha_engine::outcomes::Outcome::SellWin, true) => "win",
                _ => "loss",
            };
            assert_eq!(trade[4], expected, "signal {number}: outcome");
            *reference_counts
                .get_mut(match expected {
                    "win" => "wins",
                    "loss" => "losses",
                    _ => "ties",
                })
                .unwrap() += 1;
            assert_eq!(trade[15], row[c_split]);
            // Path projections: the exact integer path, or the legacy floating rounding.
            let exact = exact_path(
                &times,
                &prices,
                entry.index as usize,
                settlement.index as usize,
                sell,
            );
            let legacy = legacy_path(
                &times,
                &prices,
                entry.index as usize,
                settlement.index as usize,
                sell,
            );
            let entry_units = prices[entry.index as usize];
            for (name, reference, exact_text, legacy_value) in [
                (
                    "final_directional_move_bps",
                    &trade[6],
                    basis_points_text(exact.final_move_units, entry_units).unwrap(),
                    legacy.final_move,
                ),
                (
                    "max_favorable_excursion_bps",
                    &trade[7],
                    basis_points_text(exact.max_favorable_units, entry_units).unwrap(),
                    legacy.mfe,
                ),
                (
                    "max_adverse_excursion_bps",
                    &trade[8],
                    basis_points_text(exact.max_adverse_units, entry_units).unwrap(),
                    legacy.mae,
                ),
            ] {
                if exact_text == *reference {
                    continue;
                }
                assert_eq!(
                    format!("{legacy_value:.10}"),
                    *reference,
                    "signal {number}: {name} differs from the exact projection and the legacy rounding does not reproduce it"
                );
                path_source_rounding += 1;
            }
            for (name, reference, exact_time, legacy_time) in [
                (
                    "mfe_time_utc",
                    &trade[9],
                    exact.max_favorable_time_micros,
                    legacy.mfe_time,
                ),
                (
                    "mae_time_utc",
                    &trade[10],
                    exact.max_adverse_time_micros,
                    legacy.mae_time,
                ),
            ] {
                let reference = micros(reference);
                if reference == exact_time {
                    continue;
                }
                assert_eq!(
                    legacy_time, reference,
                    "signal {number}: {name} differs and the legacy rounded comparison does not reproduce it"
                );
                assert_eq!(
                    prices[times.partition_point(|&t| t < reference)],
                    prices[times.partition_point(|&t| t < exact_time)],
                    "signal {number}: {name} names an equal extremum"
                );
                path_time_source_rounding += 1;
            }
            let optional = |text: &str| (!text.is_empty()).then(|| micros(text));
            assert_eq!(
                exact.first_favorable_time_micros,
                optional(&trade[11]),
                "signal {number}: first favorable"
            );
            assert_eq!(
                exact.first_adverse_time_micros,
                optional(&trade[12]),
                "signal {number}: first adverse"
            );
            assert_eq!(
                (
                    exact.favorable_before_adverse,
                    exact.adverse_before_favorable
                ),
                (trade[13] == "1", trade[14] == "1"),
                "signal {number}: ordering flags"
            );
            compared_paths += 1;
            // The target's own settlement of the same contract agrees exactly.
            if let Some(command) = &target.command
                && let Some(target_settlement) = settled.get(command)
            {
                assert_eq!(
                    (target_settlement.time, target_settlement.price),
                    (settlement.event_time_micros, settlement.price_units),
                    "signal {number}: target settlement"
                );
                assert_eq!(
                    target_settlement.outcome.to_string(),
                    expected,
                    "signal {number}: target outcome"
                );
                assert_eq!(
                    target_settlement.path, exact,
                    "signal {number}: target path"
                );
            }
        }
        if let Some(invalidated) = invalidated {
            assert!(
                matches!(
                    cell.reason,
                    InvalidReason::InternalGap | InvalidReason::StaleSettlement
                ),
                "signal {number}: the invalidated trade's label reason is {}",
                cell.reason
            );
            assert_eq!(micros(&invalidated[0]), entry.event_time_micros);
            assert_eq!(micros(&invalidated[1]), cell.due_time_micros.unwrap());
            assert_eq!(invalidated[3], "expiry_window_crossed_tick_gap");
            let (gap_start, gap_end) = (micros(&invalidated[4]), micros(&invalidated[5]));
            let gap_end_index = times.partition_point(|&t| t < gap_end);
            assert_eq!(
                (times[gap_end_index - 1], times[gap_end_index]),
                (gap_start, gap_end),
                "signal {number}: the gap is a tick transition"
            );
            assert_eq!(
                (gap_end - gap_start) / 1000,
                invalidated[6].parse::<i64>().unwrap()
            );
            assert!(
                entry.event_time_micros <= gap_start && gap_start < cell.due_time_micros.unwrap()
            );
            assert_eq!(micros(&invalidated[2]), gap_end);
            assert_eq!(invalidated[7], row[c_split]);
        }
        // Dispositions, classified with the first differing transition cited.
        let citation = |what: &str| {
            format!(
                "signal {number} ({} at {}): {what}",
                candidate.candidate_id, row[c_decision]
            )
        };
        match (reference_opened, reference_block, target.disposition) {
            (true, _, Disposition::Admitted) => {
                let command = target.command.as_ref().unwrap();
                if let Some(invalidated) = invalidated {
                    let (reason, evidence) = unresolved.get(command).unwrap_or_else(|| panic!("signal {number}: the reference invalidated this trade but the target settled or kept it"));
                    assert_eq!(*reason, UnresolvedReason::Gap);
                    assert!(
                        evidence.contains(&format_event_time_micros(micros(&invalidated[5]))),
                        "signal {number}: {evidence}"
                    );
                    divergent_commands.insert(command.clone());
                    last_divergence.insert(candidate.candidate_id.as_str(), number);
                    classify(
                        "retained_unresolved_settlement_evidence",
                        citation(&format!(
                            "the reference removed the trade at its gap; the target retains {command}: {evidence}"
                        )),
                    );
                } else {
                    assert!(
                        settled.contains_key(command)
                            || accepted.contains_key(command) && !unresolved.contains_key(command),
                        "signal {number}: the target left {command} unresolved where the reference settled"
                    );
                    classify("matched_admitted", citation("both admitted"));
                }
            }
            (false, "[\"max_open_trades_per_strategy\"]", Disposition::CapacityStrategy) => {
                *reference_counts
                    .get_mut("blocked_by_strategy_capacity")
                    .unwrap() += 1;
                classify(
                    "matched_capacity",
                    citation("both blocked by strategy capacity"),
                );
            }
            (
                false,
                "[\"invalid_recent_tick_gap\"]",
                Disposition::GapAtEntry | Disposition::StaleFeature,
            ) => {
                *reference_counts.get_mut("blocked_by_entry_gap").unwrap() += 1;
                assert_eq!(row[c_validity], "invalid_recent_tick_gap");
                let index = entry.index as usize;
                assert!(
                    times[index] - times[index - 1] > 60_000_000,
                    "signal {number}: the gap into the trigger tick"
                );
                classify(
                    "matched_gap_at_entry",
                    citation(&format!(
                        "gap of {} microseconds into the trigger tick; target {}",
                        times[index] - times[index - 1],
                        target.disposition
                    )),
                );
            }
            (true, _, Disposition::InsufficientCash) => {
                assert!(
                    target.cash_before.compare(decimal("1.00")).unwrap()
                        == std::cmp::Ordering::Less,
                    "signal {number}: cash {}",
                    target.cash_before
                );
                last_divergence.insert(candidate.candidate_id.as_str(), number);
                classify(
                    "insufficient_cash",
                    citation(&format!(
                        "native cash {} cannot fund the reservation 1.00",
                        target.cash_before
                    )),
                );
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
                classify(
                    if retained {
                        "capacity_held_by_retained_unresolved"
                    } else {
                        "capacity_held_after_divergence"
                    },
                    citation(&format!(
                        "the strategy slot is held by {held_by}, first differing transition of this binding"
                    )),
                );
            }
            (false, "[\"max_open_trades_per_strategy\"]", Disposition::Admitted) => {
                *reference_counts
                    .get_mut("blocked_by_strategy_capacity")
                    .unwrap() += 1;
                let earlier = last_divergence.get(candidate.candidate_id.as_str()).copied().unwrap_or_else(|| panic!("signal {number}: admitted where the reference was capacity blocked without an earlier divergence"));
                divergent_commands.insert(target.command.clone().unwrap());
                classify(
                    "admitted_after_divergence",
                    citation(&format!(
                        "the reference slot was held by a trade the target lacks since signal {earlier}"
                    )),
                );
            }
            (opened, block, disposition) => panic!(
                "signal {number}: unclassified difference: reference opened {opened} block {block}, target {disposition}"
            ),
        }
    }
    assert_eq!(rows_seen, fixture.rows["signals"]);
    for (name, expected) in [
        ("opened", "opened"),
        (
            "blocked_by_strategy_capacity",
            "blocked_by_strategy_capacity",
        ),
        ("blocked_by_entry_gap", "blocked_by_entry_gap"),
        ("wins", "wins"),
        ("losses", "losses"),
        ("ties", "ties"),
    ] {
        assert_eq!(
            reference_counts[name],
            fixture.totals[expected].as_u64().unwrap(),
            "{name}"
        );
    }
    assert_eq!(
        compared_outcomes,
        fixture.totals["settled"].as_u64().unwrap()
    );
    assert_eq!(classes.values().sum::<u64>(), fixture.rows["signals"]);
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
        "compared {rows_seen} reference signals in {:.1} s: {compared_outcomes} settled outcomes and {compared_paths} paths agree; {path_source_rounding} path values and {path_time_source_rounding} extremum times reproduce only through the legacy floating rounding",
        started.elapsed().as_secs_f64()
    );
    for (class, count) in &classes {
        println!("  {class}: {count} (first: {})", examples[class]);
    }

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

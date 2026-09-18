use super::common::{self, Scratch, daily::*};
use binary_alpha_engine::{dataset::*, features::*, stream::*};
use std::{
    fs,
    path::{Path, PathBuf},
};

fn run(scratch: &Scratch, name: &str, suffix: &str, command: &[&str]) -> Result<PathBuf, String> {
    let config = scratch.config(name, suffix);
    let mut args = command.to_vec();
    args.extend(["--config", config.to_str().unwrap()]);
    let report = common::command(&args)?;
    Ok(scratch
        .path("published")
        .join(manifest_key(&common::generation(&report[0]))))
}
fn audit(scratch: &Scratch, pair: &Pair, v2: bool) -> PathBuf {
    let config = scratch.config("audit.toml", &pair.instrument());
    let path = pair.path(scratch, v2);
    if !v2 {
        let stream = common::legacy::stream(&config, &scratch.path("published"), &pair.v1);
        return scratch
            .path("published")
            .join(manifest_key(&stream.generation));
    }
    let lines = common::command(&[
        "data",
        "audit",
        "--config",
        config.to_str().unwrap(),
        "--manifest",
        &uri(&path),
    ])
    .unwrap();
    scratch
        .path("published")
        .join(manifest_key(&common::generation(&lines[0])))
}
fn object(root: &Path, objects: &[ObjectRecord], path: &str) -> PathBuf {
    root.join(&objects.iter().find(|o| o.path == path).unwrap().key)
}
fn features(
    scratch: &Scratch,
    pair: &Pair,
    v2: bool,
    stream: &Path,
    frozen: Option<&Path>,
) -> PathBuf {
    let settings = if let Some(path) = frozen {
        format!("frozen_plan=\"{}\"\n", uri(path))
    } else {
        "streams=[{duration_seconds=5,offset_seconds=0},{duration_seconds=15,offset_seconds=5}]\noutputs=[\"candle_direction\",\"range_bps\",\"return_1_bps\"]\nstructure={swing_left=2,swing_right=2,rolling_windows=[5,10,20],direction_window=10,trend_efficiency_threshold=0.35,trend_min_abs_momentum_bps=3.0,range_efficiency_threshold=0.25,compression_ratio_threshold=0.7,expanded_ratio_threshold=1.3,extreme_ratio_threshold=1.8,pullback_min_trend_age=3,trend_reset_sideways_bars=3,failed_breakout_max_bars=5}\n".into()
    };
    run(scratch,"features.toml",&format!("\n[[features.instruments]]\nrole=\"development\"\ninput_manifest=\"{}\"\nprofile_manifest=\"{}\"\n{settings}",uri(&pair.path(scratch,v2)),uri(stream)),&["features","build"]).unwrap()
}
fn normalize_profile(mut profile: InstrumentProfile, generation: &str) -> InstrumentProfile {
    profile.source.generation = generation.into();
    for support in &mut profile.calculations {
        if let Some(reason) = &mut support.reason {
            let mut value: serde_json::Value = serde_json::from_str(reason).unwrap();
            value["generation"] = generation.into();
            *reason = serde_json::to_string(&value).unwrap();
        }
    }
    profile
}

fn candles(
    root: &Path,
    manifest: &StreamManifest,
    spec: &StreamSummary,
) -> Vec<Vec<Option<Value>>> {
    let prefix = format!(
        "candles/{}s_{}s",
        spec.duration_seconds, spec.offset_seconds
    );
    manifest
        .objects
        .iter()
        .filter(|o| {
            o.path == format!("{prefix}.parquet") || o.path.starts_with(&format!("{prefix}/"))
        })
        .flat_map(|o| common::read_table(&root.join(&o.key)).1)
        .collect()
}

fn parity(pocket: bool) {
    let scratch = Scratch::new(if pocket {
        "daily_pocket_parity"
    } else {
        "daily_deriv_parity"
    });
    let pair = pair(&scratch, pocket);
    let root = scratch.path("published");
    for v2 in [false, true] {
        common::verify(&pair.path(&scratch, v2)).unwrap();
    }
    if !pocket {
        assert_eq!(common::read_normalized_ticks(&root, &pair.v2), pair.ticks);
    }
    let streams = [audit(&scratch, &pair, false), audit(&scratch, &pair, true)];
    let manifests: Vec<_> = streams
        .iter()
        .map(|p| {
            common::verify(p).unwrap();
            StreamManifest::from_json(&fs::read(p).unwrap()).unwrap()
        })
        .collect();
    let profiles: Vec<_> = manifests
        .iter()
        .map(|m| {
            InstrumentProfile::from_json(
                &fs::read(object(&root, &m.objects, "profile.json")).unwrap(),
            )
            .unwrap()
        })
        .collect();
    assert_eq!(
        normalize_profile(profiles[0].clone(), "source"),
        normalize_profile(profiles[1].clone(), "source")
    );
    assert_eq!(manifests[0].streams, manifests[1].streams);
    let mut later = false;
    let mut weekend_finalized = false;
    for spec in &manifests[0].streams {
        let a = candles(&root, &manifests[0], spec);
        let b = candles(&root, &manifests[1], spec);
        assert_eq!(a, b);
        assert!(!a.is_empty());
        for day in manifests[1].day_inventory.iter().filter(|d| {
            d.duration == Some(spec.duration_seconds) && d.offset == Some(spec.offset_seconds)
        }) {
            if let Some(key) = &day.object {
                for c in binary_alpha_app::daily::read_candles(
                    &root.join(key),
                    &day.date,
                    &manifests[1].definition.id(),
                    scale(),
                    spec.duration_seconds,
                    spec.offset_seconds,
                )
                .unwrap()
                {
                    later |= date(c.known_at_micros) > date(c.open_time_micros);
                    if !pocket
                        && date(c.open_time_micros) == SECOND
                        && date(c.known_at_micros) == LAST
                    {
                        weekend_finalized = true;
                        assert!(
                            c.flags.gap_before
                                || c.flags.missing_before
                                || c.known_at_micros - c.close_time_micros > 86_400_000_000
                        );
                    }
                }
            }
        }
    }
    assert!(
        later,
        "a candle opened before midnight must finalize on later input"
    );
    assert!(
        manifests[1]
            .day_inventory
            .iter()
            .any(|d| d.state == DayState::Partial)
    );
    assert!(
        pocket || weekend_finalized,
        "Friday candle must finalize on Monday input"
    );
    let independent_features = features(&scratch, &pair, true, &streams[1], None);
    common::verify(&independent_features).unwrap();
    let independent_manifest =
        FeatureManifest::from_json(&fs::read(independent_features).unwrap()).unwrap();
    let first_features = features(&scratch, &pair, false, &streams[0], None);
    let second_features = features(&scratch, &pair, true, &streams[0], Some(&first_features));
    let feature_paths = [first_features, second_features];
    let features: Vec<_> = feature_paths
        .iter()
        .map(|p| {
            common::verify(p).unwrap();
            FeatureManifest::from_json(&fs::read(p).unwrap()).unwrap()
        })
        .collect();
    let plans: Vec<_> = features
        .iter()
        .map(|m| {
            FeaturePlan::from_json(&fs::read(object(&root, &m.objects, "plan.json")).unwrap())
                .unwrap()
        })
        .collect();
    for plan in &plans[0].streams {
        for path in plan.object_paths() {
            assert_eq!(
                common::read_table(&object(&root, &features[0].objects, &path)),
                common::read_table(&object(&root, &independent_manifest.objects, &path))
            );
            assert_eq!(
                common::read_table(&object(&root, &features[0].objects, &path)),
                common::read_table(&object(&root, &features[1].objects, &path))
            );
        }
    }
    // Continue both warmed engines with identical future rows: output and profile equality
    // prove rolling state crosses partition boundaries and retains the unfinished candle.
    let mut engines: Vec<_> = [&pair.v1, &pair.v2]
        .iter()
        .zip(&plans)
        .map(|(m, p)| {
            let mut engine = FeatureEngine::new(p, Source::from_manifest(m)).unwrap();
            let mut output = FeatureOutput::default();
            binary_alpha_app::daily::read_generation(
                &binary_alpha_app::store::Store::filesystem(&root),
                m,
                |r| {
                    engine
                        .push(r.observation(scale())?, &mut output)
                        .map_err(|e| format!("{e:?}"))
                },
            )
            .unwrap();
            engine
        })
        .collect();
    for i in 150..210 {
        let time = micros(LAST) + i * 1_000_000;
        let observation = if pocket {
            if i % 5 != 0 {
                continue;
            }
            Observation::from_bar(
                &binary_alpha_engine::market::Bar {
                    provider: (),
                    start_unix_s: time / 1_000_000,
                    open: 1.8,
                    high: 1.81,
                    low: 1.79,
                    close: 1.801,
                    volume: 1.,
                    period_s: 5,
                },
                scale(),
            )
            .unwrap()
        } else {
            Observation::Tick(binary_alpha_engine::market::Tick {
                event_time_micros: time,
                price_units: 18000 + i % 7,
            })
        };
        let mut outputs = [FeatureOutput::default(), FeatureOutput::default()];
        for (e, out) in engines.iter_mut().zip(&mut outputs) {
            e.push(observation, &mut *out).unwrap();
        }
        assert_eq!(outputs[0], outputs[1]);
    }
    assert_eq!(
        normalize_profile(engines[0].profile(), "source"),
        normalize_profile(engines[1].profile(), "source")
    );
    outcome_replay_parity(&scratch, &pair, &feature_paths, &features, pocket);
}
#[test]
fn deriv_daily_readers_match_v1_through_every_consumer() {
    parity(false);
}
#[test]
fn pocket_daily_readers_match_v1_and_keep_tick_only_rejections() {
    parity(true);
}

fn replay_table(tick: &Path, feature: &Path, plan_identity: &str) -> String {
    format!(
        "\n[replay]\nrole = \"development\"\ndecision_start = \"2026-09-17T00:00:00Z\"\ndecision_end = \"2026-09-22T00:00:00Z\"\ninputs = [{{ tick_manifest = \"{}\", feature_manifest = \"{}\" }}]\nsplits = [{{ name = \"early\", start = \"2026-09-17T00:00:00Z\", end = \"2026-09-18T12:00:00Z\" }}, {{ name = \"late\", start = \"2026-09-18T12:00:00Z\", end = \"2026-09-22T00:00:00Z\" }}]\naccounts = [{{ id = \"sim\", broker = \"pocket_option\", currency = \"fixture_unit\", scale = 2, initial_cash = \"1000\" }}]\nreporting_currency = \"fixture_unit\"\nreporting_scale = 2\nmax_rate_age_micros = 0\n\n[[replay.strategies]]\nid = \"up\"\nplan_identity = \"{plan_identity}\"\nbase_stream = {{ duration_seconds = 15, offset_seconds = 5 }}\nconditions = [{{ stream = {{ duration_seconds = 15, offset_seconds = 5 }}, output = \"candle_direction\", comparator = \"eq\", threshold = \"up\" }}, {{ stream = {{ duration_seconds = 5, offset_seconds = 0 }}, output = \"candle_direction\", comparator = \"ne\", threshold = \"flat\" }}]\n\n[[replay.strategies]]\nid = \"down\"\nplan_identity = \"{plan_identity}\"\nbase_stream = {{ duration_seconds = 15, offset_seconds = 5 }}\nconditions = [{{ stream = {{ duration_seconds = 15, offset_seconds = 5 }}, output = \"candle_direction\", comparator = \"eq\", threshold = \"down\" }}, {{ stream = {{ duration_seconds = 5, offset_seconds = 0 }}, output = \"candle_direction\", comparator = \"ne\", threshold = \"flat\" }}]\n\n[[replay.contracts]]\nid = \"buy_10s\"\ndirection = \"buy\"\nduration_micros = 10000000\ncurrency = \"fixture_unit\"\nstake = \"1\"\nquoted_cost = \"1\"\nentry_fee = \"0\"\nwin = {{ gross_return = \"1.92\", terminal_fee = \"0\" }}\nloss = {{ gross_return = \"0\", terminal_fee = \"0\" }}\ntie = {{ gross_return = \"1\", terminal_fee = \"0\" }}\nsettlement = {{ rule = \"price_at_due_v1\", max_settlement_delay_micros = 60000000, max_tick_gap_micros = 60000000 }}\n\n[[replay.contracts]]\nid = \"sell_10s\"\ndirection = \"sell\"\nduration_micros = 10000000\ncurrency = \"fixture_unit\"\nstake = \"1\"\nquoted_cost = \"1\"\nentry_fee = \"0\"\nwin = {{ gross_return = \"1.92\", terminal_fee = \"0\" }}\nloss = {{ gross_return = \"0\", terminal_fee = \"0\" }}\ntie = {{ gross_return = \"1\", terminal_fee = \"0\" }}\nsettlement = {{ rule = \"price_at_due_v1\", max_settlement_delay_micros = 60000000, max_tick_gap_micros = 60000000 }}\n\n[[replay.risk_policies]]\nid = \"one_each\"\nmax_open_per_strategy = 1\nmax_open_total = 50000\nsame_entry = \"all\"\ndeduplicate_signal_logic = false\nmax_feature_age_micros = 60000000\nmax_quote_age_micros = 0\n\n[[replay.bindings]]\nid = \"buy_on_up\"\nstrategy = \"up\"\naccount = \"sim\"\ninstrument = \"pocket_option:AEDCNY_otc\"\ncontract = \"buy_10s\"\nrisk_policy = \"one_each\"\nenvelope = {{ max_purchase_cost = \"1\", max_entry_fee = \"0\", max_win_terminal_fee = \"0\", max_loss_terminal_fee = \"0\", max_tie_terminal_fee = \"0\", min_winning_net_return = \"0.92\", settlement_rule = \"price_at_due_v1\" }}\n\n[[replay.bindings]]\nid = \"sell_on_down\"\nstrategy = \"down\"\naccount = \"sim\"\ninstrument = \"pocket_option:AEDCNY_otc\"\ncontract = \"sell_10s\"\nrisk_policy = \"one_each\"\nenvelope = {{ max_purchase_cost = \"1\", max_entry_fee = \"0\", max_win_terminal_fee = \"0\", max_loss_terminal_fee = \"0\", max_tie_terminal_fee = \"0\", min_winning_net_return = \"0.92\", settlement_rule = \"price_at_due_v1\" }}\n",
        uri(tick),
        uri(feature)
    )
}

fn outcome_replay_parity(
    scratch: &Scratch,
    pair: &Pair,
    feature_paths: &[PathBuf; 2],
    features: &[FeatureManifest],
    pocket: bool,
) {
    use binary_alpha_engine::{
        execution::ReplayManifest,
        outcomes::{MISSING_INDEX, OutcomeManifest, stream_object_paths},
    };
    let root = scratch.path("published");
    let mut outcomes = Vec::new();
    let mut replays = Vec::new();
    for (i, v2) in [false, true].into_iter().enumerate() {
        let suffix = format!(
            "\n[outcomes]\nrole=\"development\"\ntick_manifest=\"{}\"\nfeature_manifest=\"{}\"\nexpiry_seconds=[5,10,60]\nmax_entry_delay_ms=2000\nmax_settlement_delay_ms=2000\nmax_tick_gap_ms=2000\ntrue_jump_max_gap_ms=2000\ntrue_jump_basis_points=\"5\"\nfrozen_min_ticks=10\nfrozen_min_ms=5000\n",
            uri(&pair.path(scratch, v2)),
            uri(&feature_paths[i])
        );
        let outcome = run(scratch, "outcomes.toml", &suffix, &["outcomes", "build"]);
        let suffix = replay_table(
            &pair.path(scratch, v2),
            &feature_paths[i],
            &features[i].plan_identity,
        )
        .replace("pocket_option", pair.v1.broker.as_str())
        .replace("AEDCNY_otc", pair.v1.provider_symbol.as_str());
        let replay = run(scratch, "replay.toml", &suffix, &["replay"]);
        if pocket {
            let outcome = outcome.unwrap_err();
            let replay = replay.unwrap_err();
            assert!(
                outcome.contains("ticks") || outcome.contains("tick"),
                "{outcome}"
            );
            assert!(
                replay.contains("ticks") || replay.contains("tick"),
                "{replay}"
            );
            continue;
        }
        let path = outcome.unwrap();
        common::verify(&path).unwrap();
        outcomes.push(OutcomeManifest::from_json(&fs::read(path).unwrap()).unwrap());
        let path = replay.unwrap();
        common::verify(&path).unwrap();
        replays.push(ReplayManifest::from_json(&fs::read(path).unwrap()).unwrap());
    }
    if pocket {
        return;
    }
    for a in &outcomes[0].objects {
        assert_eq!(
            fs::read(root.join(&a.key)).unwrap(),
            fs::read(object(&root, &outcomes[1].objects, &a.path)).unwrap(),
            "{}",
            a.path
        );
    }
    let mut global = false;
    let mut reasons = std::collections::BTreeSet::new();
    for stream in &outcomes[0].streams {
        let paths = stream_object_paths(stream.duration_seconds, stream.offset_seconds);
        let indices = common::read_le(
            &object(&root, &outcomes[0].objects, &paths[1]),
            u32::from_le_bytes,
        );
        global |= indices.iter().any(|&i| i != MISSING_INDEX && i > 240);
        reasons.extend(fs::read(object(&root, &outcomes[0].objects, &paths[3])).unwrap());
    }
    assert!(global, "outcome indices remain global across all days");
    assert!(
        reasons.len() > 1,
        "valid and delayed/end-of-data reasons exercised"
    );
    let ledgers: Vec<_> = replays
        .iter()
        .map(|m| fs::read_to_string(object(&root, &m.objects, "ledger/events.jsonl")).unwrap())
        .collect();
    let events: Vec<Vec<serde_json::Value>> = ledgers
        .iter()
        .map(|s| {
            s.lines()
                .map(|s| serde_json::from_str(s).unwrap())
                .collect()
        })
        .collect();
    assert!(events[0].len() > 10, "nontrivial financial ledger");
    // The first record declares source generation and configuration identities; all decisions,
    // entries, settlements, balances and subsequent ledger records must match byte-for-byte.
    let mut definitions = [events[0][0].clone(), events[1][0].clone()];
    for definition in &mut definitions {
        for pointer in [
            "/definition/config_hash",
            "/definition/instruments/0/tick_generation",
            "/definition/instruments/0/feature_generation",
            "/definition/replay/inputs/0/tick_manifest",
            "/definition/replay/inputs/0/feature_manifest",
        ] {
            *definition
                .pointer_mut(pointer)
                .expect("source identity field") = "source identity".into();
        }
    }
    assert_eq!(definitions[0], definitions[1]);
    assert_eq!(events[0][1..], events[1][1..]);
    assert_eq!(
        ledgers[0].split_once('\n').unwrap().1,
        ledgers[1].split_once('\n').unwrap().1
    );
    assert_eq!(
        fs::read(object(&root, &replays[0].objects, "summary.json")).unwrap(),
        fs::read(object(&root, &replays[1].objects, "summary.json")).unwrap()
    );
}

#[test]
fn daily_recorded_warmup_preserves_full_history_and_pending_state() {
    use super::live_support as support;
    use binary_alpha_app::{broker::transport::RecordedConnector, live};
    use binary_alpha_engine::market::format_event_time_micros;
    let base = support::Fixture::new("daily_recorded_warmup");
    let scratch = Scratch::new("daily_recorded_warmup_inputs");
    let pair = pair(&scratch, false);
    let start = micros(LAST) + 200_000_000;
    let mut records: Vec<serde_json::Value> = support::matching_log()
        .lines()
        .take(5)
        .map(|s| serde_json::from_str(s).unwrap())
        .collect();
    for record in &mut records {
        record["at"] = start.into();
    }
    // A recorded market session exists but its first row is beyond the warm-up boundary.
    records[4] = support::scenario_tick(start, "1.8000");
    for seconds in [5, 10, 15, 20, 25, 30] {
        records.push(support::scenario_tick(
            start + seconds * 1_000_000,
            "1.8010",
        ));
    }
    let log = support::scenario_log(&records);
    let mut profiles = Vec::new();
    let mut outputs = Vec::new();
    for v2 in [false, true] {
        let mut fixture = support::isolated_fixture(&base, if v2 { "v2" } else { "v1" });
        let live = fixture.config.live.as_mut().unwrap();
        live.warmup = vec![uri(&pair.path(&scratch, v2)).parse().unwrap()];
        live.compatibility.observation_start = format_event_time_micros(start);
        live.compatibility.observation_end = format_event_time_micros(start + 40_000_000);
        let recorded = RecordedConnector::from_jsonl(&log).unwrap();
        let mut owner = support::runtime_with(
            &fixture,
            live::Mode::Replay,
            &recorded,
            Box::new(live::control::FakeControl::new(start)),
            |definition| definition.policy.replay.bindings.clear(),
            |m| m,
        )
        .unwrap();
        let profile = owner.features()[0].profile();
        assert_eq!(profile.observations, pair.ticks.len() as u64);
        assert_eq!(profile.coverage, Some(pair.v1.coverage.clone()));
        assert!(profile.streams.iter().any(|s| s.withheld_observations > 0));
        profiles.push(normalize_profile(profile, "source"));
        let observed = std::rc::Rc::new(std::cell::RefCell::new(Vec::new()));
        let sink = observed.clone();
        owner.feature_observer = Some(Box::new(move |instrument, produced| {
            sink.borrow_mut().push((instrument, produced.clone()));
        }));
        owner.run_until(|_| recorded.exhausted()).unwrap().unwrap();
        assert_eq!(
            owner.features()[0].profile().observations,
            pair.ticks.len() as u64 + 7
        );
        let observed = observed.borrow().clone();
        assert_eq!(
            observed.len(),
            7,
            "every recorded observation advanced features"
        );
        assert!(observed.iter().any(|(_, output)| !output.rows.is_empty()));
        assert!(
            observed
                .iter()
                .flat_map(|(_, output)| &output.rows)
                .any(|(_, row)| row.values.iter().any(Option::is_some))
        );
        outputs.push(observed);
    }
    assert_eq!(profiles[0], profiles[1]);
    assert_eq!(outputs[0], outputs[1]);
    // The same real warm-up binder still rejects bar datasets before workers ingest them.
    let pocket_scratch = Scratch::new("daily_pocket_warmup_inputs");
    let pocket = common::daily::pair(&pocket_scratch, true);
    for v2 in [false, true] {
        let mut fixture =
            support::isolated_fixture(&base, if v2 { "pocket-v2" } else { "pocket-v1" });
        fixture.config.live.as_mut().unwrap().warmup =
            vec![uri(&pocket.path(&pocket_scratch, v2)).parse().unwrap()];
        let recorded = RecordedConnector::from_jsonl(&log).unwrap();
        let error = support::runtime_with(
            &fixture,
            live::Mode::Replay,
            &recorded,
            Box::new(live::control::FakeControl::new(start)),
            |_| {},
            |m| m,
        )
        .err()
        .unwrap();
        assert!(error.contains("ticks") || error.contains("tick"), "{error}");
    }
}

#[test]
fn daily_verify_refuses_wrong_date_missing_object_payload_and_inventory_tampering() {
    use binary_alpha_engine::market::{format_event_time_micros, parse_event_time_micros};
    for pocket in [false, true] {
        for tamper in ["wrong-date", "missing-object", "payload", "row-count"] {
            let scratch = Scratch::new(&format!("daily_tamper_{pocket}_{tamper}"));
            let mut pair = pair(&scratch, pocket);
            let root = scratch.path("published");
            let expected = match tamper {
                "wrong-date" => {
                    let day = &mut pair.v2.day_inventory[0];
                    let old = day.logical_path().unwrap();
                    day.date = "2026-09-16".into();
                    for time in [&mut day.first_time, &mut day.last_time] {
                        *time = Some(format_event_time_micros(
                            parse_event_time_micros(time.as_ref().unwrap()).unwrap()
                                - 86_400_000_000,
                        ));
                    }
                    for interval in &mut day.unresolved {
                        interval.start = format_event_time_micros(
                            parse_event_time_micros(&interval.start).unwrap() - 86_400_000_000,
                        );
                        interval.end = format_event_time_micros(
                            parse_event_time_micros(&interval.end).unwrap() - 86_400_000_000,
                        );
                    }
                    pair.v2
                        .objects
                        .iter_mut()
                        .find(|o| o.path == old)
                        .unwrap()
                        .path = day.logical_path().unwrap();
                    "row outside UTC day 2026-09-16"
                }
                "missing-object" => {
                    let path = pair.v2.day_inventory[1].logical_path().unwrap();
                    pair.v2.objects.retain(|o| o.path != path);
                    "inventory object `observations/2026-09-18.parquet` disagrees with the object list"
                }
                "payload" => {
                    let index = pair
                        .v2
                        .objects
                        .iter()
                        .position(|o| o.path.starts_with("pages/"))
                        .unwrap();
                    let old = pair.v2.objects[index].clone();
                    let file = scratch.path("corrupt-page.parquet");
                    flip_page_payload(&root.join(&old.key), &file);
                    let new = common::daily::object(&root, &old.path, old.role, &file);
                    pair.v2
                        .day_inventory
                        .iter_mut()
                        .find(|d| d.object.as_ref() == Some(&old.key))
                        .unwrap()
                        .object = Some(new.key.clone());
                    pair.v2.objects[index] = new;
                    "page payload_sha256 does not match payload"
                }
                "row-count" => {
                    pair.v2.day_inventory[1].rows += 1;
                    pair.v2.row_count += 1;
                    "observations/2026-09-18.parquet: day inventory mismatch"
                }
                _ => unreachable!(),
            };
            let path = publish(&root, &mut pair.v2);
            let error = common::verify(&path).unwrap_err();
            assert!(error.contains(expected), "{tamper}: {error}");
        }
    }
}

#[test]
fn daily_stream_keeps_an_earlier_pending_day_partial_and_rejects_tampering() {
    use binary_alpha_engine::market::format_event_time_micros;
    let scratch = Scratch::new("daily_pending_candle_day");
    let mut pair = pair(&scratch, false);
    let root = scratch.path("published");
    let index = pair
        .v2
        .day_inventory
        .iter()
        .position(|d| d.family == DayFamily::Observations && d.date == LAST)
        .unwrap();
    let day = &mut pair.v2.day_inventory[index];
    let old = day.object.clone().unwrap();
    let tick = *pair
        .ticks
        .iter()
        .find(|t| t.event_time_micros == micros(LAST))
        .unwrap();
    let file = scratch.path("last-tick.parquet");
    binary_alpha_app::daily::write_ticks(
        &file,
        LAST,
        &binary_alpha_engine::market::InstrumentId {
            broker: pair.v2.broker.clone(),
            provider_symbol: pair.v2.provider_symbol.clone(),
        },
        scale(),
        [[tick]],
    )
    .unwrap();
    let o = common::daily::object(
        &root,
        &day.logical_path().unwrap(),
        ObjectRole::Normalized,
        &file,
    );
    *pair.v2.objects.iter_mut().find(|o| o.key == old).unwrap() = o.clone();
    pair.v2.row_count -= day.rows - 1;
    day.rows = 1;
    day.object = Some(o.key);
    day.last_time = day.first_time.clone();
    day.unresolved[0].start = format_event_time_micros(tick.event_time_micros + 1);
    pair.v2.coverage.last_event_time = format_event_time_micros(tick.event_time_micros);
    write_coverage(&scratch, &mut pair.v2);
    publish(&root, &mut pair.v2);
    common::verify(&pair.path(&scratch, true)).unwrap();
    let path = audit(&scratch, &pair, true);
    common::verify(&path).unwrap();
    let manifest = StreamManifest::from_json(&fs::read(&path).unwrap()).unwrap();
    let pending = manifest
        .day_inventory
        .iter()
        .find(|d| d.date == "2026-09-20" && d.duration == Some(15) && d.offset == Some(5))
        .unwrap();
    assert_eq!(pending.state, DayState::Partial);
    assert_eq!(pending.rows, 0);
    assert!(pending.object.is_some());
    for tamper in ["state", "missing", "count", "summary"] {
        let mut changed = manifest.clone();
        let expected = match tamper {
            "state" => {
                let day = changed
                    .day_inventory
                    .iter_mut()
                    .find(|d| {
                        d.date == pending.date
                            && d.duration == pending.duration
                            && d.offset == pending.offset
                    })
                    .unwrap();
                day.state = DayState::Complete;
                day.unresolved.clear();
                day.reason = None;
                "candle day 2026-09-20 must be partial"
            }
            "missing" => {
                changed
                    .objects
                    .retain(|o| Some(&o.key) != pending.object.as_ref());
                "disagrees with the object list"
            }
            "count" => {
                let mut days = changed
                    .day_inventory
                    .iter_mut()
                    .filter(|d| d.duration == Some(5) && d.rows > 1);
                days.next().unwrap().rows += 1;
                days.next().unwrap().rows -= 1;
                "day inventory mismatch"
            }
            "summary" => {
                changed.streams[0].last_close_time =
                    Some(format_event_time_micros(micros(LAST) + 5_000_000));
                "aggregate summary mismatch"
            }
            _ => unreachable!(),
        };
        fs::write(&path, changed.to_json()).unwrap();
        let error = common::verify(&path).unwrap_err();
        assert!(error.contains(expected), "{tamper}: {error}");
    }
}

#[test]
fn daily_offset_candle_completeness_requires_the_next_day_prefix() {
    use binary_alpha_engine::market::{InstrumentId, Tick, format_event_time_micros};
    for case in [
        "unknown",
        "partial-prefix",
        "partial-finalization",
        "partial-tail",
        "complete",
    ] {
        let scratch = Scratch::new(&format!("daily_offset_dependency_{case}"));
        let mut pair = pair(&scratch, false);
        let root = scratch.path("published");
        let day = pair
            .v2
            .day_inventory
            .iter_mut()
            .find(|d| d.family == DayFamily::Observations && d.date == LAST)
            .unwrap();
        let ticks = [2, 6].map(|second| Tick {
            event_time_micros: micros(LAST) + second * 1_000_000,
            price_units: 18_000,
        });
        let file = scratch.path("next-day-prefix.parquet");
        binary_alpha_app::daily::write_ticks(
            &file,
            LAST,
            &InstrumentId {
                broker: pair.v2.broker.clone(),
                provider_symbol: pair.v2.provider_symbol.clone(),
            },
            scale(),
            [ticks],
        )
        .unwrap();
        let old = day.object.clone().unwrap();
        let o = common::daily::object(
            &root,
            &day.logical_path().unwrap(),
            ObjectRole::Normalized,
            &file,
        );
        *pair.v2.objects.iter_mut().find(|o| o.key == old).unwrap() = o.clone();
        pair.v2.row_count -= day.rows - 2;
        day.rows = 2;
        day.object = Some(o.key);
        day.first_time = Some(format_event_time_micros(ticks[0].event_time_micros));
        day.last_time = Some(format_event_time_micros(ticks[1].event_time_micros));
        day.state = match case {
            "complete" => DayState::Complete,
            "unknown" => DayState::Unknown,
            _ => DayState::Partial,
        };
        day.reason = (case != "complete").then(|| format!("synthetic {case}"));
        day.unresolved = match case {
            "partial-prefix" => vec![UnresolvedInterval {
                start: format_event_time_micros(micros(LAST)),
                end: format_event_time_micros(ticks[0].event_time_micros),
            }],
            "partial-finalization" => vec![UnresolvedInterval {
                start: format_event_time_micros(micros(LAST) + 5_000_000),
                end: format_event_time_micros(ticks[1].event_time_micros),
            }],
            "partial-tail" => vec![UnresolvedInterval {
                start: format_event_time_micros(ticks[1].event_time_micros + 1),
                end: format_event_time_micros(micros(LAST) + 86_400_000_000),
            }],
            _ => vec![],
        };
        pair.v2.coverage.last_event_time = day.last_time.clone().unwrap();
        write_coverage(&scratch, &mut pair.v2);
        publish(&root, &mut pair.v2);
        common::verify(&pair.path(&scratch, true)).unwrap();
        let path = audit(&scratch, &pair, true);
        common::verify(&path).unwrap();
        let manifest = StreamManifest::from_json(&fs::read(path).unwrap()).unwrap();
        let sunday = manifest
            .day_inventory
            .iter()
            .find(|d| d.date == "2026-09-20" && d.duration == Some(15) && d.offset == Some(5))
            .unwrap();
        assert_eq!(sunday.rows, 1, "the offset candle actually exists");
        assert_eq!(
            sunday.state,
            if matches!(case, "complete" | "partial-tail") {
                DayState::Complete
            } else {
                DayState::Unknown
            },
            "{case}"
        );
    }
}

/// Consumer proof over lifecycle artifacts. Outputs live outside the pipeline store so they
/// cannot introduce research roots into the retirement fixture.
pub(super) struct LifecycleReaders {
    scratch: Scratch,
    profile: PathBuf,
    frozen: PathBuf,
    baseline: ReaderOutput,
}

#[derive(Debug, PartialEq)]
struct ReaderOutput {
    features: std::collections::BTreeMap<String, common::Table>,
    continued: Vec<FeatureOutput>,
    profile: InstrumentProfile,
    candles: Vec<Vec<Vec<Option<Value>>>>,
    outcomes: std::collections::BTreeMap<String, Vec<u8>>,
    ledger: String,
    summary: Vec<u8>,
}

impl LifecycleReaders {
    pub(super) fn new(root: &Path, dataset: &GenerationManifest, stream: &StreamManifest) -> Self {
        let scratch = Scratch::new(&format!("lifecycle_readers_{}", dataset.broker));
        let published = scratch.path("published");
        // The frozen plan's profile stays reachable even when the producer store is removed.
        for object in &stream.objects {
            let target = published.join(&object.key);
            fs::create_dir_all(target.parent().unwrap()).unwrap();
            fs::copy(root.join(&object.key), target).unwrap();
        }
        let profile = published.join(stream.key());
        fs::create_dir_all(profile.parent().unwrap()).unwrap();
        fs::copy(root.join(stream.key()), &profile).unwrap();
        let (frozen, baseline) = lifecycle_output(&scratch, root, dataset, stream, &profile, None);
        Self {
            scratch,
            profile,
            frozen,
            baseline,
        }
    }

    pub(super) fn assert_parity(
        &self,
        root: &Path,
        dataset: &GenerationManifest,
        stream: &StreamManifest,
    ) {
        let (_, actual) = lifecycle_output(
            &self.scratch,
            root,
            dataset,
            stream,
            &self.profile,
            Some(&self.frozen),
        );
        assert_eq!(
            actual, self.baseline,
            "actual lifecycle consumer outputs for {}",
            dataset.instrument
        );
    }
}

fn lifecycle_output(
    scratch: &Scratch,
    input_root: &Path,
    dataset: &GenerationManifest,
    stream: &StreamManifest,
    profile: &Path,
    frozen: Option<&Path>,
) -> (PathBuf, ReaderOutput) {
    use binary_alpha_engine::market::{format_event_time_micros, parse_event_time_micros};
    use binary_alpha_engine::{
        execution::ReplayManifest,
        outcomes::{MISSING_INDEX, OutcomeManifest, stream_object_paths},
    };
    use std::collections::{BTreeMap, BTreeSet};
    let input = input_root.join(dataset.key());
    common::verify(&input).unwrap();
    common::verify(&input_root.join(stream.key())).unwrap();
    let settings = frozen.map(|path| format!("frozen_plan=\"{}\"\n", uri(path))).unwrap_or_else(|| {
        let streams = stream.streams.iter().map(|s| format!("{{duration_seconds={},offset_seconds={}}}", s.duration_seconds, s.offset_seconds)).collect::<Vec<_>>().join(",");
        format!("streams=[{streams}]\noutputs=[\"candle_direction\",\"range_bps\",\"return_1_bps\"]\nstructure={{swing_left=2,swing_right=2,rolling_windows=[5,10,20],direction_window=10,trend_efficiency_threshold=0.35,trend_min_abs_momentum_bps=3.0,range_efficiency_threshold=0.25,compression_ratio_threshold=0.7,expanded_ratio_threshold=1.3,extreme_ratio_threshold=1.8,pullback_min_trend_age=3,trend_reset_sideways_bars=3,failed_breakout_max_bars=5}}\n")
    });
    let feature_path = run(scratch, "lifecycle-features.toml", &format!(
        "\n[[features.instruments]]\nrole=\"development\"\ninput_manifest=\"{}\"\nprofile_manifest=\"{}\"\n{settings}", uri(&input), uri(profile)), &["features", "build"]).unwrap();
    common::verify(&feature_path).unwrap();
    let feature = FeatureManifest::from_json(&fs::read(&feature_path).unwrap()).unwrap();
    let root = scratch.path("published");
    let plan =
        FeaturePlan::from_json(&fs::read(object(&root, &feature.objects, "plan.json")).unwrap())
            .unwrap();
    let features: BTreeMap<_, _> = plan
        .streams
        .iter()
        .flat_map(|s| s.object_paths())
        .map(|path| {
            let rows = common::read_table(&object(&root, &feature.objects, &path));
            if path.starts_with("rows/") {
                assert!(!rows.1.is_empty(), "nonempty feature partition {path}");
            }
            (path, rows)
        })
        .collect();
    let scale = stream.definition.price_scale;
    let mut engine = FeatureEngine::new(&plan, Source::from_manifest(dataset)).unwrap();
    let mut last = None;
    let mut first_day_rows = 0u32;
    let first_day = &dataset.coverage.first_event_time[..10];
    binary_alpha_app::daily::read_generation(
        &binary_alpha_app::store::Store::filesystem(input_root),
        dataset,
        |row| {
            let observation = row.observation(scale)?;
            let mut output = FeatureOutput::default();
            engine
                .push(observation, &mut output)
                .map_err(|e| format!("{e:?}"))?;
            let time = match observation {
                Observation::Tick(t) => t.event_time_micros,
                Observation::Bar(b) => b.start_micros,
            };
            if &format_event_time_micros(time)[..10] == first_day {
                first_day_rows += 1;
            }
            last = Some(observation);
            Ok(())
        },
    )
    .unwrap();
    assert_eq!(engine.profile().observations, dataset.row_count);
    let mut next = last.unwrap();
    let mut continued = Vec::new();
    for _ in 0..60 {
        match &mut next {
            Observation::Tick(t) => {
                t.event_time_micros += 2_000_000;
                t.price_units += 1;
            }
            Observation::Bar(b) => {
                b.start_micros += 5_000_000;
            }
        }
        let mut output = FeatureOutput::default();
        engine.push(next, &mut output).unwrap();
        continued.push(output);
    }
    assert!(
        continued.iter().any(|out| !out.rows.is_empty()),
        "continuation exercises retained engine state"
    );
    let profile = normalize_profile(engine.profile(), "source");
    let candles = stream
        .streams
        .iter()
        .map(|spec| candles(input_root, stream, spec))
        .collect();
    let suffix = format!(
        "\n[outcomes]\nrole=\"development\"\ntick_manifest=\"{}\"\nfeature_manifest=\"{}\"\nexpiry_seconds=[5,10,60]\nmax_entry_delay_ms=2000\nmax_settlement_delay_ms=2000\nmax_tick_gap_ms=2000\ntrue_jump_max_gap_ms=2000\ntrue_jump_basis_points=\"5\"\nfrozen_min_ticks=10\nfrozen_min_ms=5000\n",
        uri(&input),
        uri(&feature_path)
    );
    let outcome = run(
        scratch,
        "lifecycle-outcomes.toml",
        &suffix,
        &["outcomes", "build"],
    );
    let start = parse_event_time_micros(&dataset.coverage.first_event_time).unwrap();
    let end = parse_event_time_micros(&dataset.coverage.last_event_time).unwrap() + 1;
    let spec = &stream.streams[0];
    let spec_text = format!(
        "duration_seconds = {}, offset_seconds = {}",
        spec.duration_seconds, spec.offset_seconds
    );
    let suffix = replay_table(&input, &feature_path, &feature.plan_identity)
        .replace("pocket_option", dataset.broker.as_str())
        .replace("AEDCNY_otc", dataset.provider_symbol.as_str())
        .replace("2026-09-17T00:00:00Z", &format_event_time_micros(start))
        .replace("2026-09-22T00:00:00Z", &format_event_time_micros(end))
        .replace(
            "2026-09-18T12:00:00Z",
            &format_event_time_micros(start + (end - start) / 2),
        )
        .replace("duration_seconds = 15, offset_seconds = 5", &spec_text)
        .replace("duration_seconds = 5, offset_seconds = 0", &spec_text);
    let replay = run(scratch, "lifecycle-replay.toml", &suffix, &["replay"]);
    let (outcomes, ledger, summary) = if dataset.capabilities.contains(&Capability::Bars) {
        for error in [outcome.unwrap_err(), replay.unwrap_err()] {
            assert!(error.contains("tick"), "{error}");
        }
        (BTreeMap::new(), String::new(), Vec::new())
    } else {
        let outcome = outcome.unwrap();
        common::verify(&outcome).unwrap();
        let outcome = OutcomeManifest::from_json(&fs::read(outcome).unwrap()).unwrap();
        let outcomes: BTreeMap<_, _> = outcome
            .objects
            .iter()
            .map(|o| (o.path.clone(), fs::read(root.join(&o.key)).unwrap()))
            .collect();
        let mut global = false;
        let mut reasons = BTreeSet::new();
        for spec in &outcome.streams {
            let paths = stream_object_paths(spec.duration_seconds, spec.offset_seconds);
            let indices = common::read_le(
                &object(&root, &outcome.objects, &paths[1]),
                u32::from_le_bytes,
            );
            global |= indices
                .iter()
                .any(|&i| i != MISSING_INDEX && i >= first_day_rows);
            reasons.extend(outcomes[&paths[3]].iter().copied());
        }
        assert!(
            first_day_rows > 0 && global,
            "outcomes use global indices beyond the first UTC day"
        );
        assert!(
            reasons.len() > 1,
            "valid and unavailable outcomes exercised"
        );
        let replay = replay.unwrap();
        common::verify(&replay).unwrap();
        let replay = ReplayManifest::from_json(&fs::read(replay).unwrap()).unwrap();
        let ledger =
            fs::read_to_string(object(&root, &replay.objects, "ledger/events.jsonl")).unwrap();
        assert!(
            ledger.lines().count() > 10,
            "nontrivial replay decisions and settlements"
        );
        let (definition, tail) = ledger.split_once('\n').unwrap();
        let mut definition: serde_json::Value = serde_json::from_str(definition).unwrap();
        for pointer in [
            "/definition/config_hash",
            "/definition/instruments/0/tick_generation",
            "/definition/instruments/0/feature_generation",
            "/definition/replay/inputs/0/tick_manifest",
            "/definition/replay/inputs/0/feature_manifest",
        ] {
            *definition.pointer_mut(pointer).unwrap() = "source identity".into();
        }
        let ledger = format!("{}\n{tail}", serde_json::to_string(&definition).unwrap());
        let summary = fs::read(object(&root, &replay.objects, "summary.json")).unwrap();
        (outcomes, ledger, summary)
    };
    (
        feature_path,
        ReaderOutput {
            features,
            continued,
            profile,
            candles,
            outcomes,
            ledger,
            summary,
        },
    )
}

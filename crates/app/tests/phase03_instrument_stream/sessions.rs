//! Independent calendar expectations through real audit publication and data verify.
use crate::common::{self, Scratch, daily::*};
use binary_alpha_app::daily;
use binary_alpha_engine::{
    continuous::{Fill, fill},
    dataset::*,
    market::{
        Bar, BarProviderColumns, Tick, format_event_time_micros as text,
        parse_event_time_micros as parse,
    },
    stream::StreamManifest,
};
use std::{collections::BTreeMap, fs};
fn t(s: &str) -> i64 {
    parse(s).unwrap()
}

fn fixture(
    scratch: &Scratch,
    kind: &str,
    outside_pending: bool,
) -> (GenerationManifest, String, Vec<i64>) {
    let pocket = kind != "deriv";
    let p = pair(scratch, pocket);
    let mut m = p.v2;
    let symbol = match kind {
        "deriv" => "frxUSDJPY",
        "pocket" => "USDJPY",
        _ => "USDJPY_otc",
    };
    m.provider_symbol = symbol.to_string().try_into().unwrap();
    m.instrument = format!("{}:{symbol}", m.broker);
    m.day_inventory.clear();
    m.objects.retain(|o| o.path == "provenance/coverage.json");
    let first = t("2026-09-04T20:54:50Z");
    let monday = t("2026-09-07T00:00:00Z");
    let mut times = vec![first, first + 5_000_000];
    if pocket || outside_pending {
        times.push(t("2026-09-05T00:00:00Z"));
    }
    if pocket {
        times.push(t("2026-09-05T00:00:05Z"));
    }
    if !outside_pending {
        times.extend([monday + 10_000_000, monday + 15_000_000]);
    }
    m.row_count = times.len() as u64;
    m.coverage = Coverage {
        first_event_time: text(first),
        last_event_time: text(*times.last().unwrap()),
    };
    let root = scratch.path("published");
    let file = scratch.path("session-input.parquet");
    for date in ["2026-09-04", "2026-09-05", "2026-09-06", "2026-09-07"] {
        let (start, end) = binary_alpha_engine::dataset::daily::day_bounds(date).unwrap();
        let day: Vec<_> = times
            .iter()
            .copied()
            .filter(|v| (start..end).contains(v))
            .collect();
        let summary = if pocket {
            daily::write_bars(
                &file,
                date,
                [day.iter()
                    .map(|&at| {
                        let zero = at == first + 5_000_000;
                        Bar {
                            provider: BarProviderColumns {
                                symbol: symbol.into(),
                                symbol_id: 1,
                                timestamp_utc: at,
                                server_time_s: at / 1_000_000,
                            },
                            start_unix_s: at / 1_000_000,
                            open: 100.0,
                            high: if zero { 100.0 } else { 101.0 },
                            low: 100.0,
                            close: 100.0,
                            volume: if zero { 0.0 } else { 1.0 },
                            period_s: 5,
                        }
                    })
                    .collect::<Vec<_>>()],
            )
            .unwrap()
        } else {
            daily::write_ticks(
                &file,
                date,
                &binary_alpha_engine::market::InstrumentId {
                    broker: m.broker.clone(),
                    provider_symbol: m.provider_symbol.clone(),
                },
                scale(),
                [day.iter()
                    .map(|&at| Tick {
                        event_time_micros: at,
                        price_units: 1_000_000,
                    })
                    .collect::<Vec<_>>()],
            )
            .unwrap()
        };
        let logical = format!("observations/{date}.parquet");
        let object = object(&root, &logical, ObjectRole::Normalized, &file);
        let cutoff = monday + 22_000_000;
        let partial = date == "2026-09-07" && !outside_pending;
        m.day_inventory.push(DayInventoryEntry {
            date: date.into(),
            family: DayFamily::Observations,
            duration: None,
            offset: None,
            object: if day.is_empty() {
                None
            } else {
                Some(object.key.clone())
            },
            rows: summary.rows,
            first_time: summary.first_event_micros.map(text),
            last_time: summary.last_event_micros.map(text),
            state: if partial {
                DayState::Partial
            } else if day.is_empty() {
                DayState::EmptyKnown
            } else {
                DayState::Complete
            },
            reason: partial.then(|| "synthetic cutoff".into()),
            unresolved: if partial {
                vec![UnresolvedInterval {
                    start: text(cutoff),
                    end: text(end),
                }]
            } else {
                vec![]
            },
        });
        if !day.is_empty() {
            m.objects.push(object);
        }
    }
    write_coverage(scratch, &mut m);
    publish(&root, &mut m);
    let session = match kind {
        "deriv" => {
            r#"{kind="weekly",timezone="UTC",open={day="monday",time="00:00:00"},close={day="friday",time="20:55:00"},closed_dates=[],early_closes=[]}"#
        }
        "pocket" => {
            r#"{kind="weekly",timezone="America/New_York",open={day="sunday",time="17:00:00"},close={day="friday",time="17:00:00"},closed_dates=[],early_closes=[]}"#
        }
        _ => r#"{kind="always"}"#,
    };
    let native = if pocket {
        r#"{kind="bar",period_seconds=5}"#
    } else {
        r#"{kind="tick"}"#
    };
    let definition = format!(
        "\n[[instruments]]\nbroker=\"{}\"\nprovider_symbol=\"{symbol}\"\nquote_currency=\"JPY\"\nprice_scale=4\nnative_granularity={native}\nsession={session}\ncandles=[{{duration_seconds=5,offset_seconds=0}}]\n",
        m.broker
    );
    let last = if outside_pending {
        monday + 86_400_000_000
    } else if pocket {
        monday + 20_000_000
    } else {
        monday + 15_000_000
    };
    let expected = match kind {
        // Inclusive bucket-open membership adds the closing bucket to both FX products.
        "deriv" => (first..=t("2026-09-04T20:55:00Z"))
            .step_by(5_000_000)
            .chain((monday..last).step_by(5_000_000))
            .collect(),
        "pocket" => (first..=t("2026-09-04T21:00:00Z"))
            .step_by(5_000_000)
            .chain((t("2026-09-06T21:00:00Z")..last).step_by(5_000_000))
            .collect(),
        _ => (first..last).step_by(5_000_000).collect(),
    };
    (m, definition, expected)
}
fn check(kind: &str, outside_pending: bool) {
    let scratch = Scratch::new(&format!("session_{kind}_{outside_pending}"));
    let (source, definition, expected) = fixture(&scratch, kind, outside_pending);
    common::verify(&scratch.path("published").join(source.key())).unwrap();
    let config = scratch.config("audit.toml", &definition);
    let report = common::command(&[
        "data",
        "audit",
        "--config",
        config.to_str().unwrap(),
        "--manifest",
        &uri(&scratch.path("published").join(source.key())),
    ])
    .unwrap();
    let path = scratch
        .path("published")
        .join(manifest_key(&common::generation(&report[0])));
    common::verify(&path).unwrap();
    let manifest = StreamManifest::from_json(&fs::read(&path).unwrap()).unwrap();
    let mut actual = BTreeMap::<String, Vec<i64>>::new();
    let mut source_fill = false;
    let mut engine_fill = false;
    for day in &manifest.day_inventory {
        if let Some(key) = &day.object {
            let rows = daily::read_candles(
                &scratch.path("published").join(key),
                &day.date,
                &manifest.definition.id(),
                scale(),
                5,
                0,
            )
            .unwrap();
            for row in rows {
                actual
                    .entry(day.date.clone())
                    .or_default()
                    .push(row.open_time_micros);
                match fill(&row) {
                    Fill::Source => {
                        source_fill = true;
                        assert!(!row.flags.clean());
                    }
                    Fill::Engine => {
                        engine_fill = true;
                        assert!(!row.flags.clean());
                        assert!(!row.flags.complete());
                    }
                    Fill::None => {}
                }
            }
        }
    }
    let mut by_day = BTreeMap::<String, Vec<i64>>::new();
    for at in expected {
        by_day.entry(date(at)).or_default().push(at);
    }
    assert_eq!(actual, by_day);
    assert!(engine_fill);
    if kind != "deriv" {
        assert!(source_fill);
    }
    if outside_pending {
        assert!(
            manifest
                .day_inventory
                .iter()
                .all(|d| d.state != DayState::Partial)
        );
        assert_eq!(
            manifest
                .day_inventory
                .iter()
                .find(|d| d.date == "2026-09-07")
                .unwrap()
                .state,
            DayState::Complete
        );
    } else {
        assert_eq!(
            manifest.day_inventory.last().unwrap().state,
            DayState::Partial
        );
    }
    // Exact deterministic daily bytes and existing-generation reuse, through the command.
    let again = common::command(&[
        "data",
        "audit",
        "--config",
        config.to_str().unwrap(),
        "--manifest",
        &uri(&scratch.path("published").join(source.key())),
    ])
    .unwrap();
    assert!(again[0].contains("already published"));
    if !outside_pending {
        // FeatureEngine reconstructs feed candles; it must also reject source fills and
        // the second (otherwise clean) Saturday bar for the non-OTC instrument.
        let settings = format!(
            "\n[[features.instruments]]\nrole=\"development\"\ninput_manifest=\"{}\"\nprofile_manifest=\"{}\"\nstreams=[{{duration_seconds=5,offset_seconds=0}}]\noutputs=[\"candle_direction\"]\n",
            uri(&scratch.path("published").join(source.key())),
            uri(&path)
        );
        let config = scratch.config("features.toml", &settings);
        let report =
            common::command(&["features", "build", "--config", config.to_str().unwrap()]).unwrap();
        let feature_path = scratch
            .path("published")
            .join(manifest_key(&common::generation(&report[0])));
        common::verify(&feature_path).unwrap();
        let features = binary_alpha_engine::features::FeatureManifest::from_json(
            &fs::read(feature_path).unwrap(),
        )
        .unwrap();
        assert_eq!(features.streams[0].rows, if kind == "otc" { 3 } else { 2 });
    }
    // Profiles are feed evidence, not the filled product count. Re-hashing a forged
    // finalized or pending count must still fail independent reconstruction.
    for field in ["finalized", "withheld_observations"] {
        let mut bad = manifest.clone();
        let slot = bad
            .objects
            .iter_mut()
            .find(|o| o.path == "profile.json")
            .unwrap();
        let mut profile: serde_json::Value =
            serde_json::from_slice(&fs::read(scratch.path("published").join(&slot.key)).unwrap())
                .unwrap();
        let old = profile["streams"][0][field].as_u64().unwrap();
        profile["streams"][0][field] = serde_json::json!(old + 1);
        let file = scratch.path("forged-profile.json");
        fs::write(&file, serde_json::to_vec(&profile).unwrap()).unwrap();
        *slot = object(
            &scratch.path("published"),
            "profile.json",
            ObjectRole::Normalized,
            &file,
        );
        fs::write(&path, bad.to_json()).unwrap();
        let error = common::verify(&path).unwrap_err();
        assert!(
            error.contains("reconstructed feed profile differs"),
            "{error}"
        );
    }
    // Removing a bucket while retaining valid file hashes must fail calendar verification.
    let mut bad = manifest.clone();
    let day = bad.day_inventory.iter_mut().find(|d| d.rows > 2).unwrap();
    let old = day.object.clone().unwrap();
    let file = scratch.path("missing-bucket.parquet");
    let mut rows = daily::read_candles(
        &scratch.path("published").join(old),
        &day.date,
        &bad.definition.id(),
        scale(),
        5,
        0,
    )
    .unwrap();
    rows.remove(1);
    let summary = daily::write_candles(
        &file,
        &day.date,
        &bad.definition.id(),
        scale(),
        5,
        0,
        [rows],
    )
    .unwrap();
    let logical = day.logical_path().unwrap();
    let replacement = object(
        &scratch.path("published"),
        &logical,
        ObjectRole::Normalized,
        &file,
    );
    day.rows = summary.rows;
    day.object = Some(replacement.key.clone());
    bad.streams[0].rows -= 1;
    *bad.objects.iter_mut().find(|o| o.path == logical).unwrap() = replacement;
    fs::write(&path, bad.to_json()).unwrap();
    assert!(
        common::verify(&path)
            .unwrap_err()
            .contains("session continuity mismatch")
    );
}
#[test]
fn deriv_session_exact_days() {
    check("deriv", false);
}
#[test]
fn pocket_session_exact_days() {
    check("pocket", false);
}
#[test]
fn otc_session_exact_days() {
    check("otc", false);
}
#[test]
fn outside_pending_does_not_block_later_verified_session() {
    check("deriv", true);
}
#[test]
fn missing_calendar_is_refused_before_v2_writing() {
    let scratch = Scratch::new("session_missing");
    let (source, definition, _) = fixture(&scratch, "deriv", false);
    let definition = definition
        .lines()
        .filter(|l| !l.starts_with("session="))
        .collect::<Vec<_>>()
        .join("\n");
    let config = scratch.config("audit.toml", &definition);
    let error = common::command(&[
        "data",
        "audit",
        "--config",
        config.to_str().unwrap(),
        "--manifest",
        &uri(&scratch.path("published").join(source.key())),
    ])
    .unwrap_err();
    assert!(
        error.contains("requires an explicit instruments.session table"),
        "{error}"
    );
}

#[test]
fn closed_friday_offset_bucket_needs_no_saturday_coverage() {
    for finalized in [false, true] {
        let scratch = Scratch::new(&format!("session_offset_friday_{finalized}"));
        let (mut source, definition, _) = fixture(&scratch, "deriv", false);
        let root = scratch.path("published");
        let mut ticks: Vec<_> = [
            "2026-09-04T20:54:35Z",
            "2026-09-04T20:54:40Z",
            "2026-09-04T20:55:00Z",
        ]
        .into_iter()
        .map(|time| Tick {
            event_time_micros: t(time),
            price_units: 1_000_000,
        })
        .collect();
        if finalized {
            // This later outside-session tick finalizes the bucket containing the closing quote.
            ticks.push(Tick {
                event_time_micros: t("2026-09-04T20:55:05Z"),
                price_units: 1_000_000,
            });
        }
        let id = binary_alpha_engine::market::InstrumentId {
            broker: source.broker.clone(),
            provider_symbol: source.provider_symbol.clone(),
        };
        let file = scratch.path("offset-source.parquet");
        let summary =
            daily::write_ticks(&file, "2026-09-04", &id, scale(), [ticks.clone()]).unwrap();
        let replacement = object(
            &root,
            "observations/2026-09-04.parquet",
            ObjectRole::Normalized,
            &file,
        );
        source
            .objects
            .retain(|o| o.path == "provenance/coverage.json");
        source.objects.push(replacement.clone());
        source.day_inventory.retain(|d| d.date == "2026-09-04");
        let day = &mut source.day_inventory[0];
        day.object = Some(replacement.key);
        day.rows = ticks.len() as u64;
        day.first_time = summary.first_event_micros.map(text);
        day.last_time = summary.last_event_micros.map(text);
        source.row_count = ticks.len() as u64;
        source.coverage = Coverage {
            first_event_time: text(ticks[0].event_time_micros),
            last_event_time: text(ticks.last().unwrap().event_time_micros),
        };
        write_coverage(&scratch, &mut source);
        publish(&root, &mut source);
        let definition = definition.replace(
            "duration_seconds=5,offset_seconds=0",
            "duration_seconds=15,offset_seconds=5",
        );
        let config = scratch.config("audit.toml", &definition);
        let lines = common::command(&[
            "data",
            "audit",
            "--config",
            config.to_str().unwrap(),
            "--manifest",
            &uri(&root.join(source.key())),
        ])
        .unwrap();
        let path = root.join(manifest_key(&common::generation(&lines[0])));
        common::verify(&path).unwrap();
        let manifest = StreamManifest::from_json(&fs::read(path).unwrap()).unwrap();
        assert_eq!(manifest.day_inventory.len(), 1);
        // Open-instant membership now includes [20:54:50,20:55:05). The 20:55 quote is
        // pending until later input finalizes it, so the original fixture is Partial, not Complete.
        assert_eq!(
            manifest.day_inventory[0].state,
            if finalized {
                DayState::Complete
            } else {
                DayState::Partial
            }
        );
        assert_eq!(
            manifest.day_inventory[0].rows,
            if finalized { 2 } else { 1 }
        );
        assert_eq!(
            manifest.day_inventory[0].unresolved,
            if finalized {
                vec![]
            } else {
                vec![UnresolvedInterval {
                    start: text(t("2026-09-04T20:54:50Z")),
                    end: text(t("2026-09-05T00:00:00Z")),
                }]
            }
        );
        let rows = daily::read_candles(
            &root.join(manifest.day_inventory[0].object.as_ref().unwrap()),
            "2026-09-04",
            &id,
            scale(),
            15,
            5,
        )
        .unwrap();
        assert_eq!(rows[0].open_time_micros, t("2026-09-04T20:54:35Z"));
        if finalized {
            assert_eq!(rows[1].open_time_micros, t("2026-09-04T20:54:50Z"));
            assert_eq!(rows[1].close_time_micros, t("2026-09-04T20:55:05Z"));
            assert_eq!(rows[1].last_event_micros, t("2026-09-04T20:55:00Z"));
            assert_eq!(rows[1].observations, 1);
        }
    }
}

#[test]
fn yearly_closed_date_removes_an_observed_day() {
    let scratch = Scratch::new("session_yearly_closed");
    let (source, definition, _) = fixture(&scratch, "deriv", false);
    assert!(
        source
            .day_inventory
            .iter()
            .any(|d| d.date == "2026-09-04" && d.rows > 0)
    );
    let definition = definition.replace("closed_dates=[]", "closed_dates=[\"09-04\"]");
    let config = scratch.config("audit.toml", &definition);
    let report = common::command(&[
        "data",
        "audit",
        "--config",
        config.to_str().unwrap(),
        "--manifest",
        &uri(&scratch.path("published").join(source.key())),
    ])
    .unwrap();
    let path = scratch
        .path("published")
        .join(manifest_key(&common::generation(&report[0])));
    common::verify(&path).unwrap();
    let manifest = StreamManifest::from_json(&fs::read(&path).unwrap()).unwrap();
    let day = manifest
        .day_inventory
        .iter()
        .find(|d| d.date == "2026-09-04")
        .unwrap();
    assert_eq!(
        (day.state, day.rows, &day.object),
        (DayState::EmptyKnown, 0, &None)
    );
}

#[test]
fn closing_quotes_survive_audit_and_verifier_requires_the_close_bucket() {
    for (kind, close_text, duration, early) in [
        ("deriv", "2026-09-04T20:55:00Z", 5, None),
        ("deriv", "2026-09-04T20:55:00Z", 60, None),
        ("deriv", "2025-12-24T22:00:00Z", 5, Some("2025-12-24")),
        ("deriv", "2025-12-24T22:00:00Z", 60, Some("12-24")),
        ("pocket", "2026-09-04T21:00:00Z", 5, None),
        ("pocket", "2026-11-06T22:00:00Z", 5, None),
    ] {
        let scratch = Scratch::new(&format!("inclusive_close_{kind}_{duration}_{close_text}"));
        let (mut source, mut definition, _) = fixture(&scratch, kind, false);
        let close = t(close_text);
        let width = i64::from(duration) * 1_000_000;
        let day_date = date(close);
        let times = [close - width, close, close + width];
        let id = binary_alpha_engine::market::InstrumentId {
            broker: source.broker.clone(),
            provider_symbol: source.provider_symbol.clone(),
        };
        let root = scratch.path("published");
        let file = scratch.path("closing-quotes.parquet");
        let summary = if kind == "deriv" {
            daily::write_ticks(
                &file,
                &day_date,
                &id,
                scale(),
                [times
                    .iter()
                    .enumerate()
                    .map(|(i, &at)| Tick {
                        event_time_micros: at,
                        price_units: 1_000_000 + i as i64 * 10_000,
                    })
                    .collect::<Vec<_>>()],
            )
            .unwrap()
        } else {
            daily::write_bars(
                &file,
                &day_date,
                [times
                    .iter()
                    .enumerate()
                    .map(|(i, &at)| {
                        let price = 100.0 + i as f64;
                        Bar {
                            provider: BarProviderColumns {
                                symbol: source.provider_symbol.to_string(),
                                symbol_id: 1,
                                timestamp_utc: at,
                                server_time_s: at / 1_000_000,
                            },
                            start_unix_s: at / 1_000_000,
                            open: price,
                            high: if i == 1 { price } else { price + 1.0 },
                            low: price,
                            close: price,
                            volume: if i == 1 { 0.0 } else { 1.0 },
                            period_s: 5,
                        }
                    })
                    .collect::<Vec<_>>()],
            )
            .unwrap()
        };
        let observation = object(
            &root,
            &format!("observations/{day_date}.parquet"),
            ObjectRole::Normalized,
            &file,
        );
        source
            .objects
            .retain(|o| o.path == "provenance/coverage.json");
        source.objects.push(observation.clone());
        source.day_inventory.truncate(1);
        let day = &mut source.day_inventory[0];
        day.date = day_date.clone();
        day.object = Some(observation.key);
        day.rows = summary.rows;
        day.first_time = summary.first_event_micros.map(text);
        day.last_time = summary.last_event_micros.map(text);
        source.row_count = 3;
        source.coverage = Coverage {
            first_event_time: text(times[0]),
            last_event_time: text(times[2]),
        };
        write_coverage(&scratch, &mut source);
        publish(&root, &mut source);
        definition = definition.replace(
            "duration_seconds=5",
            &format!("duration_seconds={duration}"),
        );
        if let Some(early) = early {
            definition = definition.replace(
                "early_closes=[]",
                &format!("early_closes=[{{date=\"{early}\",time=\"22:00:00\"}}]"),
            );
        }
        let config = scratch.config("audit.toml", &definition);
        let lines = common::command(&[
            "data",
            "audit",
            "--config",
            config.to_str().unwrap(),
            "--manifest",
            &uri(&root.join(source.key())),
        ])
        .unwrap();
        let path = root.join(manifest_key(&common::generation(&lines[0])));
        common::verify(&path).unwrap();
        let mut manifest = StreamManifest::from_json(&fs::read(&path).unwrap()).unwrap();
        assert_eq!(manifest.day_inventory.len(), 1);
        let day = &mut manifest.day_inventory[0];
        assert_eq!(day.state, DayState::Complete);
        // Inclusive bucket-open membership retains the closing quote for both durations.
        assert_eq!(day.rows, 2);
        let mut rows = daily::read_candles(
            &root.join(day.object.as_ref().unwrap()),
            &day_date,
            &id,
            scale(),
            duration,
            0,
        )
        .unwrap();
        assert_eq!(
            rows.iter().map(|c| c.open_time_micros).collect::<Vec<_>>(),
            times[..2]
        );
        let closing = &rows[1];
        assert_eq!(closing.close_time_micros, close + width);
        assert_eq!(closing.last_event_micros, close);
        assert_eq!(closing.observations, 1);
        assert_eq!(
            closing.close_units, 1_010_000,
            "the closing source price is retained"
        );
        assert_eq!(
            fill(closing),
            if kind == "pocket" {
                Fill::Source
            } else {
                Fill::None
            }
        );
        if kind == "pocket" {
            assert!(!closing.flags.clean());
            assert_eq!(closing.volume, Some(0.0));
        }
        // A self-consistent older half-open product must fail reconstruction: remove only
        // the required closing bucket, repairing its file hash, inventory, and summary.
        rows.pop();
        let file = scratch.path("omitted-close.parquet");
        let summary =
            daily::write_candles(&file, &day_date, &id, scale(), duration, 0, [rows]).unwrap();
        let logical = day.logical_path().unwrap();
        let replacement = object(&root, &logical, ObjectRole::Normalized, &file);
        day.rows = summary.rows;
        day.last_time = summary.last_event_micros.map(text);
        day.object = Some(replacement.key.clone());
        manifest.streams[0].rows = 1;
        manifest.streams[0].last_close_time = Some(text(close));
        *manifest
            .objects
            .iter_mut()
            .find(|o| o.path == logical)
            .unwrap() = replacement;
        fs::write(&path, manifest.to_json()).unwrap();
        let error = common::verify(&path).unwrap_err();
        assert!(error.contains("session continuity mismatch"), "{error}");
    }
}

#[test]
fn completed_filled_day_keeps_identical_bytes_in_an_extended_dataset() {
    let scratch = Scratch::new("session_complete_day_stability");
    let (mut extended, definition, _) = fixture(&scratch, "pocket", false);
    let mut old = extended.clone();
    let root = scratch.path("published");
    old.day_inventory.retain(|d| d.date == "2026-09-04");
    old.objects.retain(|o| {
        o.path == "provenance/coverage.json" || o.path == "observations/2026-09-04.parquet"
    });
    old.row_count = 2;
    old.coverage.last_event_time = old.day_inventory[0].last_time.clone().unwrap();
    write_coverage(&scratch, &mut old);
    publish(&root, &mut old);
    let lineage = scratch.path("extended-lineage.json");
    fs::write(
        &lineage,
        serde_json::json!({"schema_version":1,"parent_generation":old.generation}).to_string(),
    )
    .unwrap();
    extended.objects.push(object(
        &root,
        "provenance/lineage.json",
        ObjectRole::Provenance,
        &lineage,
    ));
    publish(&root, &mut extended);
    let config = scratch.config("audit.toml", &definition);
    let mut hashes = Vec::new();
    for source in [old, extended] {
        let lines = common::command(&[
            "data",
            "audit",
            "--config",
            config.to_str().unwrap(),
            "--manifest",
            &uri(&root.join(source.key())),
        ])
        .unwrap();
        if !hashes.is_empty() {
            assert!(
                lines[0].contains("candle days encoded 2 reused 1"),
                "{lines:?}"
            );
        }
        let path = root.join(manifest_key(&common::generation(&lines[0])));
        common::verify(&path).unwrap();
        let m = StreamManifest::from_json(&fs::read(path).unwrap()).unwrap();
        let day = m
            .day_inventory
            .iter()
            .find(|d| d.date == "2026-09-04")
            .unwrap();
        assert_eq!(day.state, DayState::Complete);
        // Friday's inclusive 17:00 New York close adds one engine fill, with stable bytes.
        assert_eq!(day.rows, 63);
        hashes.push(day.object.clone().unwrap());
    }
    assert_eq!(
        hashes[0], hashes[1],
        "adding later observations cannot rewrite a completed filled day"
    );
}

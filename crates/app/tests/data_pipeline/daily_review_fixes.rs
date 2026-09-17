use super::common::{self, Scratch, daily::*};
use binary_alpha_app::daily;
use binary_alpha_engine::{dataset::*, stream::StreamManifest};
use std::{fs, path::PathBuf};

fn audited(scratch: &Scratch, pair: &Pair) -> (PathBuf, StreamManifest) {
    let config = scratch.config("audit-review.toml", &pair.instrument());
    let lines = common::command(&[
        "data",
        "audit",
        "--config",
        config.to_str().unwrap(),
        "--manifest",
        &uri(&pair.path(scratch, true)),
    ])
    .unwrap();
    let path = scratch
        .path("published")
        .join(manifest_key(&common::generation(&lines[0])));
    common::verify(&path).unwrap();
    let manifest = StreamManifest::from_json(&fs::read(&path).unwrap()).unwrap();
    (path, manifest)
}

#[test]
fn daily_dataset_requires_typed_coverage_and_every_inventory_state() {
    for pocket in [false, true] {
        for change in [
            "promote-cutoff",
            "invalid-coverage-json",
            "remove-empty-day",
            "change-unresolved",
            "promote-pages",
        ] {
            let scratch = Scratch::new(&format!("fix2_dataset_{pocket}_{change}"));
            let mut p = pair(&scratch, pocket);
            let root = scratch.path("published");
            common::verify(&p.path(&scratch, true)).unwrap();
            let expected = match change {
                "promote-cutoff" | "promote-pages" => {
                    let family = if change == "promote-pages" {
                        DayFamily::Pages
                    } else {
                        DayFamily::Observations
                    };
                    let day =
                        p.v2.day_inventory
                            .iter_mut()
                            .find(|d| d.family == family && d.date == LAST)
                            .unwrap();
                    day.state = DayState::Complete;
                    day.reason = None;
                    day.unresolved.clear();
                    "disagrees with acquisition coverage"
                }
                "invalid-coverage-json" => {
                    let file = scratch.path("invalid-coverage.json");
                    fs::write(&file, b"not even JSON").unwrap();
                    *p.v2
                        .objects
                        .iter_mut()
                        .find(|o| o.path == "provenance/coverage.json")
                        .unwrap() = object(
                        &root,
                        "provenance/coverage.json",
                        ObjectRole::Provenance,
                        &file,
                    );
                    "daily coverage:"
                }
                "remove-empty-day" => {
                    p.v2.day_inventory.retain(|d| {
                        !(d.family == DayFamily::Observations && d.date == "2026-09-19")
                    });
                    "exactly the same days"
                }
                "change-unresolved" => {
                    let day =
                        p.v2.day_inventory
                            .iter_mut()
                            .find(|d| d.family == DayFamily::Observations && d.date == LAST)
                            .unwrap();
                    day.unresolved[0].start = binary_alpha_engine::market::format_event_time_micros(
                        micros(LAST) + 200_000_000,
                    );
                    "disagrees with acquisition coverage"
                }
                _ => unreachable!(),
            };
            let path = publish(&root, &mut p.v2);
            let error = common::verify(&path).unwrap_err();
            assert!(error.contains(expected), "{change}: {error}");
        }
    }
}

#[test]
fn daily_stream_checks_earlier_unknown_and_empty_days() {
    for pocket in [false, true] {
        for change in ["promote-unknown", "remove-empty-day", "change-unresolved"] {
            let scratch = Scratch::new(&format!("fix2_stream_{pocket}_{change}"));
            let p = pair(&scratch, pocket);
            let (path, mut manifest) = audited(&scratch, &p);
            match change {
                "promote-unknown" => {
                    let day = manifest
                        .day_inventory
                        .iter_mut()
                        .find(|d| d.date == FIRST && d.duration == Some(5))
                        .unwrap();
                    assert_eq!(day.state, DayState::Unknown);
                    day.state = DayState::Complete;
                    day.reason = None;
                    day.unresolved.clear();
                }
                "remove-empty-day" => {
                    let day = manifest
                        .day_inventory
                        .iter()
                        .find(|d| d.date == "2026-09-19" && d.duration == Some(5))
                        .unwrap();
                    // R_50 and OTC are always open: the empty observation day now
                    // yields 17,280 engine candles and remains fully covered.
                    assert_eq!(day.state, DayState::Complete);
                    assert_eq!(day.rows, 17280);
                    let removed = day.object.clone().unwrap();
                    manifest
                        .streams
                        .iter_mut()
                        .find(|s| s.duration_seconds == 5 && s.offset_seconds == 0)
                        .unwrap()
                        .rows -= 17280;
                    manifest.objects.retain(|o| o.key != removed);
                    manifest
                        .day_inventory
                        .retain(|d| !(d.date == "2026-09-19" && d.duration == Some(5)));
                }
                "change-unresolved" => {
                    let day = manifest
                        .day_inventory
                        .iter_mut()
                        .find(|d| d.date == FIRST && d.duration == Some(5))
                        .unwrap();
                    day.unresolved.clear();
                }
                _ => unreachable!(),
            }
            fs::write(&path, manifest.to_json()).unwrap();
            let error = common::verify(&path).unwrap_err();
            let expected = if change == "remove-empty-day" {
                "session continuity mismatch"
            } else {
                "must be unknown"
            };
            assert!(error.contains(expected), "{change}: {error}");
        }
    }
}

#[test]
fn regression_completed_bar_candle_must_not_depend_on_next_bar_start() {
    use binary_alpha_engine::market::format_event_time_micros;
    let scratch = Scratch::new("fix2_bar_closed_candle");
    let mut p = pair(&scratch, true);
    let root = scratch.path("published");
    let last_bar = p
        .bars
        .iter()
        .find(|b| b.start_unix_s * 1_000_000 == micros(LAST))
        .unwrap()
        .clone();
    let file = scratch.path("one-finalizing-bar.parquet");
    daily::write_bars(&file, LAST, [vec![last_bar]]).unwrap();
    let day =
        p.v2.day_inventory
            .iter_mut()
            .find(|d| d.family == DayFamily::Observations && d.date == LAST)
            .unwrap();
    let logical = day.logical_path().unwrap();
    let replacement = object(&root, &logical, ObjectRole::Normalized, &file);
    p.v2.row_count -= day.rows - 1;
    day.rows = 1;
    day.object = Some(replacement.key.clone());
    day.last_time = day.first_time.clone();
    day.reason = Some("known bar starts before 00:00:05; cutoff at 00:00:05".into());
    day.unresolved[0].start = format_event_time_micros(micros(LAST) + 5_000_000);
    *p.v2.objects.iter_mut().find(|o| o.path == logical).unwrap() = replacement;
    p.v2.coverage.last_event_time = day.last_time.clone().unwrap();
    write_coverage(&scratch, &mut p.v2);
    publish(&root, &mut p.v2);
    common::verify(&p.path(&scratch, true)).unwrap();
    let (_path, m) = audited(&scratch, &p);
    let sunday = m
        .day_inventory
        .iter()
        .find(|d| d.date == "2026-09-20" && d.duration == Some(15))
        .unwrap();
    let rows = daily::read_candles(
        &root.join(sunday.object.as_ref().unwrap()),
        &sunday.date,
        &m.definition.id(),
        scale(),
        15,
        5,
    )
    .unwrap();
    // The always session fills the preceding 5,759 buckets. The last row is the
    // same real cross-midnight candle, finalized by the bar ending at 00:00:05.
    assert_eq!(rows.len(), 5760);
    assert!(
        rows[..5759]
            .iter()
            .all(|c| c.observations == 0 && !c.flags.clean())
    );
    let final_row = rows.last().unwrap();
    assert_eq!(final_row.close_time_micros, micros(LAST) + 5_000_000);
    assert_eq!(final_row.known_at_micros, final_row.close_time_micros);
    println!(
        "BAR DAY actual {:?}, reason {:?}; candle close/known_at {}, source unresolved starts {}",
        sunday.state,
        sunday.reason,
        format_event_time_micros(final_row.known_at_micros),
        format_event_time_micros(micros(LAST) + 5_000_000)
    );
    assert_eq!(
        sunday.state,
        DayState::Complete,
        "later bar starts cannot alter this already-finalized Sunday candle"
    );
}

#[test]
fn daily_stream_resolves_separate_source_and_restored_closure() {
    let scratch = Scratch::new("fix2_stream_source_resolution");
    let pair = pair(&scratch, false);
    let source_root = scratch.path("published");
    let stream_root = scratch.path("separate-stream-store");
    let config = scratch.config("separate-audit.toml", &pair.instrument());
    let settings = fs::read_to_string(&config).unwrap().replace(
        &format!("publication_uri = \"{}\"", uri(&source_root)),
        &format!("publication_uri = \"{}\"", uri(&stream_root)),
    );
    fs::write(&config, settings).unwrap();
    let lines = common::command(&[
        "data",
        "audit",
        "--config",
        config.to_str().unwrap(),
        "--manifest",
        &uri(&pair.path(&scratch, true)),
    ])
    .unwrap();
    let path = stream_root.join(manifest_key(&common::generation(&lines[0])));
    let manifest = StreamManifest::from_json(&fs::read(&path).unwrap()).unwrap();
    assert_eq!(
        manifest.source_manifest_uri,
        Some(uri(&pair.path(&scratch, true)))
    );
    assert!(!stream_root.join(pair.v2.key()).exists());
    common::verify(&path).unwrap();

    // Restore the source closure beside the stream, then take its original source offline.
    let restored = stream_root.join(pair.v2.key());
    fs::create_dir_all(restored.parent().unwrap()).unwrap();
    fs::copy(pair.path(&scratch, true), &restored).unwrap();
    for object in &pair.v2.objects {
        fs::copy(source_root.join(&object.key), stream_root.join(&object.key)).unwrap();
    }
    fs::rename(&source_root, scratch.path("offline-source")).unwrap();
    common::verify(&path).unwrap();

    fs::remove_file(&restored).unwrap();
    let error = common::verify(&path).unwrap_err();
    assert!(
        error.contains("stream source evidence unavailable"),
        "{error}"
    );
}

#[test]
fn daily_coverage_rejects_unsupported_and_contradictory_evidence() {
    use binary_alpha_engine::dataset::coverage::DailyCoverage;
    let scratch = Scratch::new("fix2_coverage_contract");
    let pair = pair(&scratch, false);
    let object = pair
        .v2
        .objects
        .iter()
        .find(|o| o.path == "provenance/coverage.json")
        .unwrap();
    let bytes = fs::read(scratch.path("published").join(&object.key)).unwrap();
    let original: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    for change in [
        "schema",
        "page-index",
        "complement",
        "acquisition-support",
        "shortfall",
        "missing-reference",
        "duplicate-day",
    ] {
        let mut value = original.clone();
        match change {
            "schema" => value["schema_version"] = 1.into(),
            "page-index" => value["pages"] = serde_json::json!([]),
            "complement" => value["days"][0]["unresolved"] = serde_json::json!([]),
            "acquisition-support" => {
                value["acquisitions"][1]["requested"] = serde_json::json!([]);
                value["acquisitions"][1]["verified"] = serde_json::json!([]);
            }
            "shortfall" => value["acquisitions"][0]["shortfalls"][0]["reason"] = "".into(),
            "missing-reference" => {
                value["days"][0]["acquisition_ids"] = serde_json::json!(["absent"])
            }
            "duplicate-day" => {
                let day = value["days"][0].clone();
                value["days"].as_array_mut().unwrap().push(day);
            }
            _ => unreachable!(),
        }
        let error = DailyCoverage::from_json(&serde_json::to_vec(&value).unwrap()).unwrap_err();
        assert!(!error.is_empty(), "{change}");
    }
    // Prior verified coverage may precede the current request; preserving it is valid.
    let mut coverage = DailyCoverage::from_json(&bytes).unwrap();
    coverage.acquisitions[1].requested[0].start =
        binary_alpha_engine::market::format_event_time_micros(micros(SECOND) + 1_000_000);
    coverage.check_manifest(&pair.v2).unwrap();
}

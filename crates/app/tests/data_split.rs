//! Offline role slicing through the CLI, daily readers, and governed research consumers.
mod common;
#[path = "common/research.rs"]
mod fixture;

use binary_alpha_app::{daily, store::Store};
use binary_alpha_engine::config::{Config, DataSplit, ManifestUri};
use binary_alpha_engine::dataset::coverage::{CoverageRange, DailyCoverage};
use binary_alpha_engine::dataset::daily::DAY_MICROS;
use binary_alpha_engine::dataset::*;
use binary_alpha_engine::features::FeatureManifest;
use binary_alpha_engine::market::{InstrumentId, Tick, parse_event_time_micros, parse_tick_line};
use binary_alpha_engine::research::{
    self, CertificationManifest, CertificationRecord, Claim, ClaimKind, Declaration, Grant, Run,
    RunManifest, RunState, Verdict,
};
use common::{Scratch, cli_as, command, snapshot_tree as snapshots};
use fixture::{BASE, time};
use serde_json::Value;
use std::collections::BTreeSet;
use std::{fs, path::Path};

const OPERATOR: &str = "split-fixture-operator";
const PLANTED: [u8; 4] = [0b0011_1111, 0b0000_0011, 0b0001_1111, 0b0000_1111];
fn window(first: i64, end: i64) -> CoverageRange {
    CoverageRange::new(BASE + first * DAY_MICROS, BASE + end * DAY_MICROS)
}
fn uri(root: &Path, generation: &str) -> ManifestUri {
    format!("file://{}", root.join(manifest_key(generation)).display())
        .parse()
        .unwrap()
}
fn read_manifest(root: &Path, generation: &str) -> GenerationManifest {
    GenerationManifest::from_json(&fs::read(root.join(manifest_key(generation))).unwrap()).unwrap()
}
fn object(root: &Path, generation: &str, path: &str) -> Vec<u8> {
    let manifest: Value =
        serde_json::from_slice(&fs::read(root.join(manifest_key(generation))).unwrap()).unwrap();
    let record = manifest["objects"]
        .as_array()
        .unwrap()
        .iter()
        .find(|o| o["path"] == path)
        .unwrap();
    fs::read(root.join(record["key"].as_str().unwrap())).unwrap()
}
fn split_config(scratch: &Scratch, sources: Vec<ManifestUri>) -> Config {
    let mut config = binary_alpha_app::skeleton(&fixture::configuration(&scratch.root));
    config.split = Some(DataSplit {
        namespace: "split-fixture".into(),
        sources,
        development: vec![window(-1, 5), window(-1, 2), window(3, 4), window(-1, 5)],
        evaluation: vec![window(5, 6)],
        holdout: vec![window(6, 7)],
    });
    config
}
fn split(scratch: &Scratch, config: &Config) -> (String, Declaration, String) {
    let path = scratch.path("split.toml");
    fs::write(&path, config.canonical_toml()).unwrap();
    let report = cli_as(
        &scratch.path("split.log"),
        OPERATOR,
        &["data", "split", "--config", path.to_str().unwrap()],
    )
    .unwrap();
    println!("{report}");
    let uri = report
        .lines()
        .last()
        .unwrap()
        .strip_prefix("declaration ")
        .unwrap()
        .to_string();
    let declaration =
        Declaration::from_json(&fs::read(uri.strip_prefix("file://").unwrap()).unwrap()).unwrap();
    (report, declaration, uri)
}
fn tick_root(scratch: &Scratch, instrument: usize) -> (GenerationManifest, Vec<Tick>) {
    let scale = fixture::SCALES[instrument].try_into().unwrap();
    let id = InstrumentId {
        broker: "pocket_option".to_string().try_into().unwrap(),
        provider_symbol: fixture::SYMBOLS[instrument].to_string().try_into().unwrap(),
    };
    let lines: Vec<_> = [0, 3, 4, 5, 6]
        .into_iter()
        .flat_map(|day| {
            fixture::ticks_at_scale(
                BASE + day * DAY_MICROS,
                &fixture::recipe(PLANTED),
                fixture::SCALES[instrument],
            )
        })
        .collect();
    let symbol = "SYNTHETIC".to_string().try_into().unwrap();
    let rows: Vec<_> = lines
        .iter()
        .enumerate()
        .flat_map(|(index, line)| {
            let tick = parse_tick_line(line, &symbol, scale).unwrap();
            std::iter::repeat_n(tick, if index % 80 == 0 { 2 } else { 1 })
        })
        .collect();
    let root = scratch.path("source-store");
    fs::create_dir_all(&root).unwrap();
    let source = scratch.path(&format!("source-{instrument}.csv"));
    fs::write(&source, lines.join("\n")).unwrap();
    let mut manifest = common::current::ticks_with_source(
        &root,
        &id,
        DatasetRole::Development,
        scale,
        &rows,
        Some(&source),
    )
    .unwrap();
    for day in [-1, 1] {
        manifest.day_inventory.push(DayInventoryEntry {
            date: time(BASE + day * DAY_MICROS)[..10].into(),
            family: DayFamily::Observations,
            duration: None,
            offset: None,
            object: None,
            rows: 0,
            first_time: None,
            last_time: None,
            state: DayState::EmptyKnown,
            reason: None,
            unresolved: vec![],
        });
    }
    manifest.day_inventory.sort_by(|a, b| a.date.cmp(&b.date));
    common::daily::write_coverage_at(&root, &mut manifest);
    common::daily::publish(&root, &mut manifest);
    GenerationManifest::from_json(&manifest.to_json()).unwrap();
    (manifest, rows)
}

#[test]
fn ticks_split_reuses_exact_populations_and_certifies_without_root_or_early_holdout_reads() {
    let scratch = Scratch::new("split_ticks");
    let roots = [tick_root(&scratch, 0), tick_root(&scratch, 1)];
    let config = split_config(
        &scratch,
        roots
            .iter()
            .map(|(m, _)| uri(&scratch.path("source-store"), &m.generation))
            .collect(),
    );
    let source_before = snapshots(&scratch.path("source-store"));
    let (first, declaration, declaration_uri) = split(&scratch, &config);
    assert_eq!(declaration.populations.len(), 10);
    assert_eq!(declaration.operator, OPERATOR);
    assert_eq!(declaration.root, config.storage.publication_uri);
    let unique: BTreeSet<_> = declaration.populations.iter().map(|p| &p.id).collect();
    assert_eq!(unique.len(), 10);
    let published = scratch.path("published");
    let windows = [
        window(-1, 5),
        window(-1, 2),
        window(3, 4),
        window(5, 6),
        window(6, 7),
    ];
    let mut published_objects = BTreeSet::new();
    for (index, population) in declaration.populations.iter().enumerate() {
        let (source, rows) = &roots[index / 5];
        let range = &windows[index % 5];
        let (start, end) = range.bounds().unwrap();
        let manifest = read_manifest(&published, &population.id);
        assert_eq!(manifest.layout, Some(Layout::DailyV2));
        let reused = manifest
            .objects
            .iter()
            .filter(|object| !published_objects.insert(object.key.clone()))
            .count();
        assert_eq!(
            first.lines().nth(index).unwrap(),
            format!(
                "published {} {} generation {} rows {} objects {} reused {}",
                manifest.instrument,
                manifest.role,
                manifest.generation,
                manifest.row_count,
                manifest.objects.len(),
                reused,
            )
        );
        assert_eq!(population.generations, std::slice::from_ref(&population.id));
        assert_eq!(population.source, source.generation);
        assert!(declaration.population(&source.generation).is_none());
        let expected: Vec<_> = rows
            .iter()
            .filter(|row| (start..end).contains(&row.event_time_micros))
            .copied()
            .collect();
        assert!(expected.windows(2).any(|pair| pair[0] == pair[1]));
        let mut actual = Vec::new();
        daily::read_generation_lossless(
            &Store::filesystem(scratch.path("retained")),
            &manifest,
            |row| {
                let daily::LosslessRow::Tick(tick) = row else {
                    panic!("tick fixture")
                };
                actual.push(tick);
                Ok(())
            },
        )
        .unwrap();
        assert_eq!(actual, expected);
        assert_eq!(manifest.row_count, expected.len() as u64);
        assert_eq!(
            manifest.coverage.first_event_time,
            time(expected.first().unwrap().event_time_micros)
        );
        assert_eq!(
            manifest.coverage.last_event_time,
            time(expected.last().unwrap().event_time_micros)
        );
        assert_eq!(population.coverage, manifest.coverage);
        let tokens: Vec<_> = (start..end)
            .step_by(DAY_MICROS as usize)
            .map(|day| format!("{}:{}", source.instrument, &time(day)[..10]))
            .collect();
        assert_eq!(population.tokens, tokens);
        let lineage: Value = serde_json::from_slice(&object(
            &published,
            &population.id,
            "provenance/lineage.json",
        ))
        .unwrap();
        assert_eq!(lineage["window"], serde_json::to_value(range).unwrap());
        assert_eq!(lineage["source"]["generation"], source.generation);
        assert_eq!(
            lineage["source"]["uri"],
            config.split.as_ref().unwrap().sources[index / 5].to_string()
        );
        assert_eq!(
            lineage["source"]["manifest_sha256"],
            manifest.inputs[0].sha256
        );
        assert!(lineage.get("parent_generation").is_none());
        let coverage = DailyCoverage::from_json(&object(
            &published,
            &population.id,
            "provenance/coverage.json",
        ))
        .unwrap();
        coverage.check_manifest(&manifest).unwrap();
        assert_eq!(coverage.days.len(), manifest.day_inventory.len());
        assert_eq!(coverage.acquisitions.len(), coverage.days.len());
        if index % 5 == 1 {
            assert_eq!(
                manifest
                    .day_inventory
                    .iter()
                    .map(|d| d.rows)
                    .collect::<Vec<_>>(),
                [0, 2673, 0]
            );
            assert_eq!(manifest.day_inventory.first().unwrap().first_time, None);
            assert_eq!(manifest.day_inventory.last().unwrap().last_time, None);
        }
    }
    for index in [0, 3] {
        let manifest = read_manifest(&published, &declaration.populations[index].id);
        let lines = command(&[
            "data",
            "verify",
            "--manifest",
            &uri(&published, &manifest.generation).to_string(),
        ])
        .unwrap();
        assert!(lines[0].starts_with(&format!(
            "verified {} {} generation {} rows {} objects {} bytes ",
            manifest.instrument,
            manifest.role,
            manifest.generation,
            manifest.row_count,
            manifest.objects.len()
        )));
        println!("{}", lines[0]);
    }
    let error = command(&[
        "data",
        "verify",
        "--manifest",
        &uri(&published, &declaration.populations[4].id).to_string(),
    ])
    .unwrap_err();
    assert!(error.contains("holdout data is protected"), "{error}");
    println!("{error}");
    let (again, same, same_uri) = split(&scratch, &config);
    assert_eq!(declaration, same);
    assert_eq!(declaration_uri, same_uri);
    for (a, b) in first.lines().take(10).zip(again.lines()) {
        let (prefix, _) = a.rsplit_once(" reused ").unwrap();
        let objects = prefix.rsplit_once(" objects ").unwrap().1;
        assert_eq!(b, format!("{prefix} reused {objects}"));
    }
    let mut inserted = config.clone();
    inserted
        .split
        .as_mut()
        .unwrap()
        .development
        .insert(0, window(0, 1));
    let (_, extended, _) = split(&scratch, &inserted);
    for population in &declaration.populations {
        assert_eq!(
            extended.population(&population.id).unwrap().tokens,
            population.tokens
        );
    }
    let mut reordered = config.clone();
    reordered.split.as_mut().unwrap().development.reverse();
    for range in &mut reordered.split.as_mut().unwrap().development {
        range.start = range.start.replace(".000000Z", "Z");
    }
    let (_, reordered, _) = split(&scratch, &reordered);
    for population in &declaration.populations {
        assert_eq!(reordered.population(&population.id).unwrap(), population);
    }
    assert_eq!(snapshots(&scratch.path("source-store")), source_before);
    research_consumes_slices(&scratch, &declaration, &declaration_uri, &roots);
}

fn research_consumes_slices(
    scratch: &Scratch,
    declaration: &Declaration,
    declaration_uri: &str,
    roots: &[(GenerationManifest, Vec<Tick>); 2],
) {
    let mut value = serde_json::to_value(fixture::configuration(&scratch.root)).unwrap();
    fn remap(value: &mut Value) {
        match value {
            Value::String(text) if text.starts_with("2026-") => {
                let micros = parse_event_time_micros(text).unwrap();
                let day = [0, 3, 4, 5, 6][((micros - BASE) / fixture::HOUR) as usize];
                *text = time(BASE + day * DAY_MICROS + (micros - BASE) % fixture::HOUR);
            }
            Value::Array(values) => values.iter_mut().for_each(remap),
            Value::Object(values) => values.values_mut().for_each(remap),
            _ => (),
        }
    }
    remap(&mut value["research"]);
    let r = &mut value["research"];
    r["study"]["governance_manifest"] = declaration_uri.into();
    r["portfolio"]["max_rate_age_micros"] = (8 * DAY_MICROS).into();
    r["folds"][0]["cutoff"] = time(BASE + 2 * DAY_MICROS).into();
    r["folds"][0]["decision_start"] = time(BASE + 3 * DAY_MICROS).into();
    r["folds"][0]["decision_end"] = time(BASE + 4 * DAY_MICROS).into();
    r["refit"]["cutoff"] = time(BASE + 5 * DAY_MICROS).into();
    for (name, day) in [("evaluation", 5), ("holdout", 6)] {
        r[name]["decision_start"] = time(BASE + day * DAY_MICROS + fixture::CANDLE).into();
        r[name]["decision_end"] = time(BASE + (day + 1) * DAY_MICROS).into();
    }
    let published = scratch.path("published");
    let reference = |index: usize| uri(&published, &declaration.populations[index].id).to_string();
    for i in 0..2 {
        r["instruments"][i]["source_manifest"] = reference(i * 5).into();
        r["instruments"][i]["search"]["decision_start"] = time(BASE - DAY_MICROS).into();
        r["instruments"][i]["search"]["decision_end"] = time(BASE + 5 * DAY_MICROS).into();
        r["folds"][0]["inputs"][i]["fit_manifest"] = reference(i * 5 + 1).into();
        r["folds"][0]["inputs"][i]["assessment_manifest"] = reference(i * 5 + 2).into();
        r["refit"]["fits"][i] = reference(i * 5).into();
        r["evaluation"]["inputs"][i] = reference(i * 5 + 3).into();
        r["holdout"]["inputs"][i] = reference(i * 5 + 4).into();
    }
    let config: Config = serde_json::from_value(value).unwrap();
    let config = Config::parse(&config.canonical_toml()).unwrap();
    let path = scratch.path("research.toml");
    fs::write(&path, config.canonical_toml()).unwrap();
    let log = scratch.path("research-access.log");
    let args = ["research", "run", "--config", path.to_str().unwrap()];
    let report = cli_as(&log, "split-fixture-researcher", &args).unwrap();
    println!("{report}");
    let generation = research::run_generation_id(
        &config.content_hash(),
        binary_alpha_app::import::CODE_REVISION,
        &declaration.identity(),
    );
    let manifest =
        RunManifest::from_json(&fs::read(published.join(manifest_key(&generation))).unwrap())
            .unwrap();
    let run = Run::from_json(&object(&published, &generation, "research.json")).unwrap();
    assert_eq!(manifest.state, "awaiting_holdout_authorization");
    assert_eq!(run.state, RunState::AwaitingHoldoutAuthorization);
    assert!(report.contains(&format!("verified research generation {generation} state awaiting_holdout_authorization instruments 2 scenarios 3 objects 1 bytes ")));
    let access = fs::read_to_string(&log).unwrap();
    assert!(!access.contains("holdout-use"));
    for (root, _) in roots {
        assert!(!access.contains(&root.key()));
    }
    for index in [4, 9] {
        let holdout = read_manifest(&published, &declaration.populations[index].id);
        assert!(!access.contains(&holdout.key()));
        for object in holdout.objects {
            assert!(!access.contains(&object.key), "{access}");
        }
    }
    let expected_tokens: BTreeSet<_> = [3, 8]
        .into_iter()
        .flat_map(|i| declaration.populations[i].tokens.clone())
        .collect();
    let mut claimed = BTreeSet::new();
    assert_eq!(run.claims.len(), 2);
    let freeze = access
        .find(&format!("put_new {}", research::frozen_key(&generation)))
        .unwrap();
    let first_read = [3, 8]
        .into_iter()
        .map(|i| {
            access
                .find(&manifest_key(&declaration.populations[i].id))
                .unwrap()
        })
        .min()
        .unwrap();
    for key in &run.claims {
        let claim: Claim = serde_json::from_slice(&fs::read(published.join(key)).unwrap()).unwrap();
        assert_eq!(claim.kind, ClaimKind::AssessmentUse);
        assert_eq!(
            claim.tokens.iter().cloned().collect::<BTreeSet<_>>(),
            expected_tokens
        );
        claimed.insert(claim.token);
        let created = access.find(&format!("put_new {key}")).unwrap();
        assert!(freeze < created && created < first_read);
    }
    assert_eq!(claimed, expected_tokens);
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
            &uri(&published, &generation).to_string(),
            "--holdout-manifest",
            &reference(4),
            "--holdout-manifest",
            &reference(9),
            "--reason",
            "invented split fixture",
        ],
    )
    .unwrap();
    println!("{grant_report}");
    let grant = Grant::from_json(
        &fs::read(published.join(declaration.key(&research::grant_key(&generation)))).unwrap(),
    )
    .unwrap();
    assert_eq!(
        grant.tokens,
        [4, 9]
            .into_iter()
            .flat_map(|i| declaration.populations[i].tokens.clone())
            .collect::<Vec<_>>()
    );
    let report = cli_as(&log, "split-fixture-researcher", &args).unwrap();
    println!("{report}");
    let certified = research::certification_generation_id(&generation, &grant.hash);
    let certification = CertificationManifest::from_json(
        &fs::read(published.join(manifest_key(&certified))).unwrap(),
    )
    .unwrap();
    let record =
        CertificationRecord::from_json(&object(&published, &certified, "certification.json"))
            .unwrap();
    assert_eq!(certification.state, "certified");
    assert_eq!(record.verdict, Verdict::Pass);
    assert_eq!(record.holdout, grant.holdout);
    assert_eq!(
        record
            .holdout
            .iter()
            .map(|h| h.manifest.generation())
            .collect::<Vec<_>>(),
        [
            declaration.populations[4].id.as_str(),
            declaration.populations[9].id.as_str()
        ]
    );
    assert!(report.contains(&format!(
        "verified research certification {certified} state certified"
    )));
}

fn bar_split_config(scratch: &Scratch, manifest: &GenerationManifest) -> Config {
    let mut config = split_config(
        scratch,
        vec![uri(&scratch.path("published"), &manifest.generation)],
    );
    config.storage.historical_data_dir =
        serde_json::from_value(serde_json::json!(scratch.path("split-retained"))).unwrap();
    config.storage.publication_uri =
        format!("file://{}", scratch.path("split-published").display())
            .parse()
            .unwrap();
    let split = config.split.as_mut().unwrap();
    let day = |date| {
        CoverageRange::new(
            common::daily::micros(date),
            common::daily::micros(date) + DAY_MICROS,
        )
    };
    split.development = vec![day(common::daily::FIRST)];
    split.evaluation = vec![day(common::daily::SECOND)];
    split.holdout = vec![day(common::daily::LAST)];
    config
}
fn generation(report: &[String]) -> &str {
    report[0]
        .split(" generation ")
        .nth(1)
        .unwrap()
        .split_whitespace()
        .next()
        .unwrap()
}

#[test]
fn bars_keep_only_observation_days_and_apply_frozen_evaluation_features() {
    let scratch = Scratch::new("split_bars");
    let pair = common::daily::pair(&scratch, true);
    assert!(pair.pages.iter().any(|p| p.first_event_time.unwrap()
        < common::daily::micros(common::daily::SECOND)
        && p.last_event_time.unwrap() >= common::daily::micros(common::daily::SECOND)));
    let config = bar_split_config(&scratch, &pair.v2);
    let (_, declaration, _) = split(&scratch, &config);
    let published = scratch.path("split-published");
    for (population, date) in declaration.populations.iter().zip([
        common::daily::FIRST,
        common::daily::SECOND,
        common::daily::LAST,
    ]) {
        let manifest = read_manifest(&published, &population.id);
        assert_eq!(manifest.day_inventory.len(), 1);
        assert_eq!(manifest.day_inventory[0].date, date);
        assert_eq!(
            manifest
                .objects
                .iter()
                .map(|o| o.path.clone())
                .collect::<Vec<_>>(),
            [
                format!("observations/{date}.parquet"),
                "provenance/coverage.json".into(),
                "provenance/lineage.json".into()
            ]
        );
        let observation = &manifest.objects[0];
        let original = pair
            .v2
            .objects
            .iter()
            .find(|o| o.path == observation.path)
            .unwrap();
        let mut expected_record = original.clone();
        expected_record.crc32c = None;
        expected_record.generation = None;
        assert_eq!(observation, &expected_record);
        assert_eq!(
            fs::read(published.join(&observation.key)).unwrap(),
            fs::read(scratch.path("published").join(&original.key)).unwrap()
        );
        assert_eq!(
            binary_alpha_app::store::identify(&published.join(&observation.key))
                .unwrap()
                .sha256,
            original.sha256
        );
        let mut actual = Vec::new();
        daily::read_generation_lossless(
            &Store::filesystem(scratch.path("split-retained")),
            &manifest,
            |row| {
                let daily::LosslessRow::Bar(bar) = row else {
                    panic!("bar fixture")
                };
                actual.push(bar);
                Ok(())
            },
        )
        .unwrap();
        let expected: Vec<_> = pair
            .bars
            .iter()
            .filter(|b| common::daily::date(b.start_unix_s * 1_000_000) == date)
            .cloned()
            .map(daily::DailyBar::from)
            .collect();
        assert_eq!(actual, expected);
        assert_eq!(manifest.row_count, expected.len() as u64);
    }
    let mut consumer = binary_alpha_app::skeleton(&config);
    consumer = Config::parse(&format!(
        "{}{}",
        consumer.canonical_toml(),
        pair.instrument()
    ))
    .unwrap();
    let path = scratch.path("consumer.toml");
    fs::write(&path, consumer.canonical_toml()).unwrap();
    let development = uri(&published, &declaration.populations[0].id).to_string();
    let evaluation = uri(&published, &declaration.populations[1].id).to_string();
    let audited = command(&[
        "data",
        "audit",
        "--config",
        path.to_str().unwrap(),
        "--manifest",
        &development,
    ])
    .unwrap();
    println!("{}", audited.join("\n"));
    let profile = uri(&published, generation(&audited)).to_string();
    let stream = binary_alpha_engine::stream::StreamManifest::from_json(
        &fs::read(published.join(manifest_key(generation(&audited)))).unwrap(),
    )
    .unwrap();
    assert_eq!(stream.source_generation, declaration.populations[0].id);
    let before = (
        snapshots(&published),
        snapshots(&scratch.path("split-retained")),
    );
    let error = command(&[
        "data",
        "audit",
        "--config",
        path.to_str().unwrap(),
        "--manifest",
        &evaluation,
    ])
    .unwrap_err();
    assert!(
        error.contains("research audits development data only"),
        "{error}"
    );
    println!("{error}");
    assert_eq!(
        before,
        (
            snapshots(&published),
            snapshots(&scratch.path("split-retained"))
        )
    );
    let feature = format!(
        "\n[[features.instruments]]\nrole = \"development\"\ninput_manifest = \"{development}\"\nprofile_manifest = \"{profile}\"\nstreams = [{{ duration_seconds = 5, offset_seconds = 0 }}]\noutputs = [\"candle_direction\"]\n"
    );
    fs::write(&path, format!("{}{feature}", consumer.canonical_toml())).unwrap();
    let fitted = command(&["features", "build", "--config", path.to_str().unwrap()]).unwrap();
    let frozen = uri(&published, generation(&fitted));
    fs::write(&path, format!("{}\n[[features.instruments]]\nrole = \"evaluation\"\ninput_manifest = \"{evaluation}\"\nprofile_manifest = \"{profile}\"\nfrozen_plan = \"{frozen}\"\n", consumer.canonical_toml())).unwrap();
    let applied = command(&["features", "build", "--config", path.to_str().unwrap()]).unwrap();
    println!("{}", applied.join("\n"));
    let manifest = FeatureManifest::from_json(
        &fs::read(published.join(manifest_key(generation(&applied)))).unwrap(),
    )
    .unwrap();
    assert_eq!(manifest.input_generation, declaration.populations[1].id);
    assert_eq!(manifest.role, DatasetRole::Evaluation);
    assert_eq!(manifest.frozen_from.as_deref(), Some(frozen.generation()));
    assert_eq!(manifest.streams.len(), 1);
    assert_eq!(
        manifest.streams[0].first_decision_time.as_deref(),
        Some("2026-09-18T00:00:05.000000Z")
    );
    assert_eq!(
        manifest.streams[0].last_decision_time.as_deref(),
        Some("2026-09-18T00:02:00.000000Z")
    );
    assert_eq!(manifest.streams[0].rows, 24);
    let verified = command(&[
        "data",
        "verify",
        "--manifest",
        &uri(&published, &manifest.generation).to_string(),
    ])
    .unwrap();
    assert!(verified[0].starts_with(&format!(
        "verified {} evaluation generation {} rows 24 events 0 objects 4 bytes ",
        manifest.instrument, manifest.generation
    )));
    println!("{}", verified.join("\n"));
}

#[test]
fn invalid_configuration_sources_windows_and_locations_write_nothing() {
    let scratch = Scratch::new("split_refusals");
    let pair = common::daily::pair(&scratch, true);
    let base = bar_split_config(&scratch, &pair.v2);
    let mut evaluation = pair.v2.clone();
    evaluation.role = DatasetRole::Evaluation;
    common::daily::write_coverage(&scratch, &mut evaluation);
    common::daily::publish(&scratch.path("published"), &mut evaluation);
    let alias = scratch.path("source-alias");
    for (key, bytes) in snapshots(&scratch.path("published")) {
        let path = alias.join(key);
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(path, bytes).unwrap();
    }
    fs::create_dir_all(scratch.path("managed/pipeline_state")).unwrap();
    fs::create_dir_all(scratch.path("managed/store")).unwrap();
    std::os::unix::fs::symlink(scratch.path("managed/store"), scratch.path("managed-alias"))
        .unwrap();
    std::os::unix::fs::symlink(scratch.path("published"), scratch.path("published-alias")).unwrap();
    // An alias whose own target steps through an uncreated child must resolve the same way.
    std::os::unix::fs::symlink(
        scratch.path("not-created/../managed/store"),
        scratch.path("managed-parent-alias"),
    )
    .unwrap();
    std::os::unix::fs::symlink(
        scratch.path("not-created/../published"),
        scratch.path("source-parent-alias"),
    )
    .unwrap();
    // A namespace alias inside the destination, a source store nested inside an output folder,
    // an alias that cycles through its own parent, and a long parent chain with no alias.
    for (dir, target) in [
        ("ns-source-dest", "published"),
        ("ns-managed-dest", "managed/store"),
    ] {
        fs::create_dir_all(scratch.path(dir)).unwrap();
        std::os::unix::fs::symlink(
            scratch.path(target),
            scratch.path(dir).join("split-fixture"),
        )
        .unwrap();
    }
    for root in ["nested-dest/manifests", "nested-retained/objects"] {
        for (key, bytes) in snapshots(&scratch.path("published")) {
            let path = scratch.path(root).join(key);
            fs::create_dir_all(path.parent().unwrap()).unwrap();
            fs::write(path, bytes).unwrap();
        }
    }
    std::os::unix::fs::symlink(
        Path::new("recursive-cycle").join(".."),
        scratch.path("recursive-cycle"),
    )
    .unwrap();
    let long_chain = format!("{}managed/store", "x/../".repeat(45));
    let mut cases: Vec<(&str, Config, &str)> = Vec::new();
    for (name, from, to) in [
        ("evaluation-holdout", "evaluation", "holdout"),
        ("development-evaluation", "development", "evaluation"),
        ("development-holdout", "development", "holdout"),
    ] {
        let mut value = serde_json::to_value(&base).unwrap();
        value["split"][to] = value["split"][from].clone();
        cases.push((
            name,
            serde_json::from_value(value).unwrap(),
            "window overlaps",
        ));
    }
    for (name, mutation, reason) in [
        ("empty development", "development", "at least one window"),
        ("empty evaluation", "evaluation", "at least one window"),
        ("empty holdout", "holdout", "at least one window"),
        ("empty sources", "sources", "at least one source"),
    ] {
        let mut value = serde_json::to_value(&base).unwrap();
        value["split"][mutation] = serde_json::json!([]);
        cases.push((name, serde_json::from_value(value).unwrap(), reason));
    }
    for (name, start, end, reason) in [
        (
            "equal bounds",
            "2026-09-17T00:00:00Z",
            "2026-09-17T00:00:00Z",
            "nonempty and increasing",
        ),
        (
            "reversed bounds",
            "2026-09-18T00:00:00Z",
            "2026-09-17T00:00:00Z",
            "nonempty and increasing",
        ),
        (
            "non-midnight",
            "2026-09-17T00:00:01Z",
            "2026-09-18T00:00:00Z",
            "UTC midnights",
        ),
        (
            "empty window",
            "2026-09-19T00:00:00Z",
            "2026-09-20T00:00:00Z",
            "has no rows",
        ),
    ] {
        let mut config = base.clone();
        config.split.as_mut().unwrap().development = vec![CoverageRange {
            start: start.into(),
            end: end.into(),
        }];
        cases.push((name, config, reason));
    }
    for role in ["evaluation", "holdout"] {
        let mut value = serde_json::to_value(&base).unwrap();
        let repeated = value["split"][role][0].clone();
        value["split"][role].as_array_mut().unwrap().push(repeated);
        cases.push((
            "same-role overlap",
            serde_json::from_value(value).unwrap(),
            "window overlaps",
        ));
    }
    let mut config = base.clone();
    config.split.as_mut().unwrap().holdout = vec![CoverageRange {
        start: "2026-09-22T00:00:00Z".into(),
        end: "2026-09-23T00:00:00Z".into(),
    }];
    cases.push(("last window empty", config, "has no rows"));
    let mut config = base.clone();
    config.split.as_mut().unwrap().namespace = "bad/name".into();
    cases.push(("namespace", config, "identifier"));
    let mut config = base.clone();
    let source = config.split.as_ref().unwrap().sources[0].clone();
    config.split.as_mut().unwrap().sources.push(source);
    cases.push(("duplicate source", config, "source is declared twice"));
    let mut config = base.clone();
    config
        .split
        .as_mut()
        .unwrap()
        .sources
        .push(uri(&alias, &pair.v2.generation));
    cases.push(("same instrument", config, "distinct instruments"));
    for (name, manifest) in [("v1 source", &pair.v1), ("evaluation source", &evaluation)] {
        let mut config = base.clone();
        config
            .split
            .as_mut()
            .unwrap()
            .sources
            .push(uri(&scratch.path("published"), &manifest.generation));
        cases.push((name, config, "development daily-v2 roots"));
    }
    for (name, path, reason) in [
        ("source store", "published", "source store"),
        ("source alias", "published-alias", "source store"),
        ("source child", "published/nested", "source store"),
        ("managed store", "managed/store", "managed pipeline store"),
        (
            "managed child",
            "managed/store/manifests/candidate/research",
            "managed pipeline store",
        ),
        (
            "managed alias",
            "managed-alias/nested",
            "managed pipeline store",
        ),
        (
            "managed parent traversal",
            "not-created/../managed/store",
            "managed pipeline store",
        ),
        (
            "source parent traversal",
            "not-created/../published",
            "source store",
        ),
        (
            "managed alias traversal",
            "managed-parent-alias/nested",
            "managed pipeline store",
        ),
        (
            "source alias traversal",
            "source-parent-alias",
            "source store",
        ),
    ] {
        for retained in [false, true] {
            let mut config = base.clone();
            if retained {
                config.storage.historical_data_dir =
                    serde_json::from_value(serde_json::json!(scratch.path(path))).unwrap();
            } else {
                config.storage.publication_uri = format!("file://{}", scratch.path(path).display())
                    .parse()
                    .unwrap();
            }
            cases.push((name, config, reason));
        }
    }
    // The declaration lands under the namespace, which must not name a protected store.
    for (name, destination, namespace, reason) in [
        (
            "namespace into managed store",
            "managed",
            "store",
            "managed pipeline store",
        ),
        (
            "namespace into source store",
            "",
            "published",
            "source store",
        ),
    ] {
        let mut config = base.clone();
        config.split.as_mut().unwrap().namespace = namespace.into();
        config.storage.publication_uri = format!("file://{}", scratch.path(destination).display())
            .parse()
            .unwrap();
        cases.push((name, config, reason));
    }
    for (name, destination, reason) in [
        (
            "namespace alias into source store",
            "ns-source-dest",
            "source store",
        ),
        (
            "namespace alias into managed store",
            "ns-managed-dest",
            "managed pipeline store",
        ),
        (
            "cyclic parent alias",
            "recursive-cycle/nested",
            "too many destination symlinks",
        ),
        (
            "long parent chain",
            long_chain.as_str(),
            "managed pipeline store",
        ),
    ] {
        let mut config = base.clone();
        config.storage.publication_uri = format!("file://{}", scratch.path(destination).display())
            .parse()
            .unwrap();
        cases.push((name, config, reason));
    }
    let generation = base.split.as_ref().unwrap().sources[0]
        .generation()
        .to_string();
    for (name, root, retained) in [
        ("source below destination", "nested-dest/manifests", false),
        (
            "source below retained folder",
            "nested-retained/objects",
            true,
        ),
    ] {
        let mut config = base.clone();
        config.split.as_mut().unwrap().sources = vec![uri(&scratch.path(root), &generation)];
        let parent = scratch.path(root).parent().unwrap().to_path_buf();
        if retained {
            config.storage.historical_data_dir =
                serde_json::from_value(serde_json::json!(parent)).unwrap();
        } else {
            config.storage.publication_uri =
                format!("file://{}", parent.display()).parse().unwrap();
        }
        cases.push((name, config, "source store"));
    }
    let mut config = base.clone();
    config.split.as_mut().unwrap().namespace = "..".into();
    cases.push(("namespace parent step", config, "not identifiers"));
    for (index, (name, config, reason)) in cases.into_iter().enumerate() {
        let path = scratch.path(&format!("invalid-{index}.toml"));
        fs::write(&path, config.canonical_toml()).unwrap();
        let before = snapshots(&scratch.root);
        let error = command(&["data", "split", "--config", path.to_str().unwrap()]).unwrap_err();
        assert!(error.contains(reason), "{name}: {error}");
        assert_eq!(snapshots(&scratch.root), before, "{name}");
        println!("{name}: {}", error.trim());
    }
}

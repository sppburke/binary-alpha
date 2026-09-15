//! Exercises `binary-alpha config validate` end to end against checked-in fixtures.

mod common;

use std::process::{Command, Output};

const EXPECTED_REPORT: &str = "# content-hash: v3:sha256:69c7e52a379adf4c76edd6745a60b12cdccf6c56658afdaa37d6b2ecb7185c1b\nschema_version = 1\nrun_mode = \"research\"\n\n[storage]\nhistorical_data_dir = \"../historical_data\"\npublication_uri = \"gs://example-bucket/historical\"\n\n[[instruments]]\nbroker = \"pocket_option\"\nprovider_symbol = \"AEDCNY_otc\"\nbase_currency = \"AED\"\nquote_currency = \"CNY\"\nprice_scale = 6\n\n[instruments.native_granularity]\nkind = \"tick\"\n\n[instruments.gap]\nmax_seconds = 2\nreopen_seconds = 60\n\n[instruments.frozen]\nmin_observations = 10\nmin_seconds = 5\n\n[instruments.jump]\nmin_basis_points = 5\n\n[instruments.span]\nmin_percent = 75\n\n[[instruments.sessions]]\nname = \"week\"\nopen_seconds = 0\nclose_seconds = 604800\n\n[[instruments.candles]]\nduration_seconds = 5\noffset_seconds = 0\nmin_observations = 9\nhard_min_observations = 5\n\n[[instruments.candles]]\nduration_seconds = 15\noffset_seconds = 5\nmin_observations = 29\nhard_min_observations = 15\n";

fn binary_alpha(args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_binary-alpha"))
        .args(args)
        .output()
        .expect("binary-alpha runs")
}

fn validate(path: &str) -> Output {
    binary_alpha(&["config", "validate", "--config", path])
}

fn fixture(name: &str) -> String {
    format!("{}/tests/fixtures/{name}", env!("CARGO_MANIFEST_DIR"))
}

fn example() -> String {
    format!("{}/../../configs/example.toml", env!("CARGO_MANIFEST_DIR"))
}

#[test]
fn help_names_every_command() {
    let output = binary_alpha(&["--help"]);
    assert!(output.status.success());
    let help = String::from_utf8(output.stdout).unwrap();
    assert!(help.contains("config") && help.contains("data"));
    let data = String::from_utf8(binary_alpha(&["data", "--help"]).stdout).unwrap();
    assert!(data.contains("import") && data.contains("audit") && data.contains("verify"));
}

#[test]
fn existing_equivalent_preserves_the_fixed_report() {
    let path = fixture("equivalent.toml");
    let output = validate(&path);
    assert!(
        output.status.success(),
        "{path}: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(
        String::from_utf8(output.stdout).unwrap(),
        EXPECTED_REPORT,
        "{path}"
    );
    assert!(output.stderr.is_empty(), "{path}");
}

#[test]
fn checked_in_example_adds_validated_broker_history() {
    let output = validate(&example());
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let config = binary_alpha_app::load_config(std::path::Path::new(&example())).unwrap();
    assert_eq!(config.brokers.len(), 2);
    assert_eq!(
        config.history.as_ref().unwrap().instruments[0].as_str(),
        "AEDCNY_otc"
    );
    let mut original = config;
    original.brokers.clear();
    original.history = None;
    assert_eq!(
        format!(
            "# content-hash: {}\n{}",
            original.content_hash(),
            original.canonical_toml()
        ),
        EXPECTED_REPORT
    );
}

#[test]
fn the_report_validates_to_itself() {
    let first = validate(&example());
    let path = format!("{}/report.toml", env!("CARGO_TARGET_TMPDIR"));
    std::fs::write(&path, &first.stdout).unwrap();
    let second = validate(&path);
    assert!(first.status.success() && second.status.success());
    assert!(first.stderr.is_empty() && second.stderr.is_empty());
    assert_eq!(second.stdout, first.stdout);
}

#[test]
fn invalid_documents_fail_with_field_specific_errors() {
    let cases = [
        (
            "phase10/history_unknown_broker.toml",
            "history:",
            "broker is not declared",
        ),
        (
            "phase10/unknown_broker_field.toml",
            "unexpected_option",
            "unknown field `unexpected_option`",
        ),
        (
            "missing_schema_version.toml",
            "schema_version",
            "missing field `schema_version`",
        ),
        (
            "missing_run_mode.toml",
            "run_mode",
            "missing field `run_mode`",
        ),
        (
            "unsupported_schema_version.toml",
            "schema_version",
            "unsupported schema_version 2, expected 1",
        ),
        (
            "schema_version_string.toml",
            "schema_version",
            "expected u32",
        ),
        (
            "unsupported_run_mode.toml",
            "run_mode",
            "unknown run_mode `browser`, expected one of `research`, `replay`, `paper`, `live`",
        ),
        ("run_mode_table.toml", "run_mode", "expected a string"),
        (
            "unknown_field.toml",
            "retry_count",
            "unknown field `retry_count`",
        ),
        ("raw_secret.toml", "api_token", "unknown field `api_token`"),
        ("missing_storage.toml", "storage", "missing field `storage`"),
        (
            "missing_publication_uri.toml",
            "publication_uri",
            "missing field `publication_uri`",
        ),
        (
            "unsupported_publication_uri.toml",
            "publication_uri",
            "must start with `gs://` or `file:///`",
        ),
        (
            "file_destination_outside_research.toml",
            "storage.publication_uri",
            "requires run_mode `research`, not `replay`",
        ),
        (
            "holdout_source.toml",
            "import.sources[0].role",
            "holdout data is never an import input",
        ),
        (
            "unknown_source_kind.toml",
            "kind",
            "unknown variant `browser_capture`, expected one of `tick_csv`, `bar_parquet_collection`, `tick_parquet_daily`",
        ),
        (
            "escaping_manifest.toml",
            "import.sources[0].manifest",
            "must stay inside its root",
        ),
        (
            "escaping_instrument.toml",
            "import.sources[0].instruments",
            "must stay inside its root",
        ),
        (
            "control_character_symbol.toml",
            "import.sources",
            "contains a control character",
        ),
        (
            "price_scale_too_large.toml",
            "import.sources",
            "price_scale 19 exceeds 18",
        ),
        (
            "instrument_mapped_twice.toml",
            "instruments[1].provider_symbol",
            "pocket_option:AEDCNY_otc with tick granularity is already mapped by instruments[0]",
        ),
        (
            "candle_off_the_bar_grid.toml",
            "instruments[0].candles[0].duration_seconds",
            "must be multiples of the 5-second bar",
        ),
    ];
    for (name, key, message) in cases {
        let output = validate(&fixture(name));
        let stderr = String::from_utf8(output.stderr).unwrap();
        assert_eq!(output.status.code(), Some(1), "{name}");
        assert!(output.stdout.is_empty(), "{name}");
        assert!(stderr.contains(key), "{name}: {stderr}");
        assert!(stderr.contains(message), "{name}: {stderr}");
    }
}

#[test]
fn cuda_configuration_requires_the_build_feature() {
    let output = validate(&fixture("accelerator_cuda.toml"));
    assert_eq!(output.status.success(), cfg!(feature = "cuda"));
    if cfg!(feature = "cuda") {
        assert!(output.stderr.is_empty());
        assert!(
            String::from_utf8(output.stdout)
                .unwrap()
                .contains("[accelerator]")
        );
    } else {
        assert_eq!(output.status.code(), Some(1));
        assert!(output.stdout.is_empty());
        assert!(String::from_utf8(output.stderr).unwrap().contains(
            "accelerator.backend: `cuda` requested but this binary was built without the `cuda` feature"));
    }
}

#[path = "common/research.rs"]
mod research_fixture;

fn validate_document(name: &str, document: &str) -> Output {
    let root = std::path::Path::new(env!("CARGO_TARGET_TMPDIR")).join("phase11_config_documents");
    std::fs::create_dir_all(&root).unwrap();
    let path = root.join(format!("{name}.toml"));
    std::fs::write(&path, document).unwrap();
    validate(path.to_str().unwrap())
}

#[test]
fn research_and_replay_scenario_documents_validate_and_round_trip() {
    use binary_alpha_engine::config::{Config, ReplayScenario};
    let root = std::path::Path::new("/synthetic/config-only-never-opened");
    let research = research_fixture::configuration(root);
    let mut replay = research_fixture::replay_configuration(root);
    replay.replay.as_mut().unwrap().scenario = Some(ReplayScenario {
        schema_version: 1,
        id: "delay".into(),
        acceptance_delay_micros: 100_000,
    });
    for (name, config) in [("research", research), ("scenario", replay)] {
        let document = config.canonical_toml();
        let result = validate_document(name, &document);
        assert!(
            result.status.success(),
            "{}",
            String::from_utf8_lossy(&result.stderr)
        );
        assert!(result.stderr.is_empty());
        assert_eq!(
            String::from_utf8(result.stdout).unwrap(),
            format!("# content-hash: {}\n{document}", config.content_hash())
        );
        assert_eq!(Config::parse(&document).unwrap(), config);
    }
}

#[test]
fn research_and_scenario_rejections_name_the_exact_rule() {
    use binary_alpha_engine::config::ReplayScenario;
    let root = std::path::Path::new("/synthetic/config-only-never-opened");
    let research = research_fixture::configuration(root);
    let mut cases = Vec::new();
    let mut changed = research.clone();
    changed.research.as_mut().unwrap().scenarios[0]
        .alternatives
        .pop();
    cases.push((
        changed,
        "research.scenarios[0].alternatives: every portfolio binding needs exactly one alternative"
            .to_string(),
    ));
    let mut changed = research.clone();
    changed.research.as_mut().unwrap().scenarios[0].id = "baseline".into();
    cases.push((
        changed,
        "research.scenarios[0].id: `baseline` is the baseline or is listed twice".into(),
    ));
    let mut changed = research.clone();
    changed.research.as_mut().unwrap().scenarios[0].acceptance_delay_micros = -1;
    cases.push((
        changed,
        "research.scenarios[0].acceptance_delay_micros: must be non-negative".into(),
    ));
    let mut changed = research.clone();
    changed.research.as_mut().unwrap().qualification.claim = "positive_expected_profit".into();
    cases.push((changed, "research.qualification.claim: `positive_expected_profit` is not the supported claim `empirical_policy_qualification_v1`".into()));
    let mut changed = research.clone();
    changed.research.as_mut().unwrap().evaluation.inputs.pop();
    cases.push((changed, "research.evaluation.inputs: 1 entries for 2 instruments; one entry per instrument in instrument order is required".into()));
    let mut changed = research.clone();
    changed.research.as_mut().unwrap().portfolio.max_policies = 11;
    cases.push((
        changed,
        "research.portfolio.max_policies: the grid declares 12 policies, above the maximum 11"
            .into(),
    ));
    let mut changed = research.clone();
    changed.research.as_mut().unwrap().instruments[0]
        .search
        .max_candidates = 1;
    cases.push((changed, "research.instruments[0].search.max_candidates: the menu enumerates 2 members, above the maximum 1".into()));
    let mut changed = research;
    let table = &mut changed.research.as_mut().unwrap().portfolio;
    table.max_policies = u64::MAX;
    table.subsets = vec![binary_alpha_engine::config::Subset {
        deployments: vec![table.subsets[0].deployments[0]; 64],
    }];
    cases.push((
        changed,
        "research.portfolio.subsets[0]: the declared policy count overflows".into(),
    ));
    for (version, delay, message) in [
        (
            2,
            0,
            "replay.scenario.schema_version: unsupported version 2, expected 1",
        ),
        (
            1,
            -1,
            "replay.scenario.acceptance_delay_micros: must be non-negative",
        ),
    ] {
        let mut changed = research_fixture::replay_configuration(root);
        changed.replay.as_mut().unwrap().scenario = Some(ReplayScenario {
            schema_version: version,
            id: "test".into(),
            acceptance_delay_micros: delay,
        });
        cases.push((changed, message.into()));
    }
    for (index, (config, message)) in cases.into_iter().enumerate() {
        let output = validate_document(&format!("invalid-{index}"), &config.canonical_toml());
        assert_eq!(output.status.code(), Some(1));
        assert!(output.stdout.is_empty());
        let error = String::from_utf8(output.stderr).unwrap();
        assert_eq!(error.trim(), message, "case {index}");
    }
}

#[test]
fn omitted_research_and_scenario_preserve_canonical_bytes_and_hash() {
    use binary_alpha_engine::config::Config;
    let legacy = std::fs::read_to_string(fixture("equivalent.toml")).unwrap();
    let config = Config::parse(&legacy).unwrap();
    assert!(config.research.is_none());
    assert_eq!(
        format!(
            "# content-hash: {}\n{}",
            config.content_hash(),
            config.canonical_toml()
        ),
        EXPECTED_REPORT
    );
    // Explicit None deserializes from JSON; TOML has no null spelling. Neither optional field
    // appears in the canonical TOML, and the pre-scenario replay document hashes identically.
    let replay = research_fixture::replay_configuration(std::path::Path::new(
        "/synthetic/config-only-never-opened",
    ));
    let legacy_replay = replay.canonical_toml();
    assert!(!legacy_replay.contains("[research]") && !legacy_replay.contains("scenario"));
    let mut explicit = serde_json::to_value(&replay).unwrap();
    explicit["research"] = serde_json::Value::Null;
    explicit["replay"]["scenario"] = serde_json::Value::Null;
    let explicit: Config = serde_json::from_value(explicit).unwrap();
    let without = Config::parse(&legacy_replay).unwrap();
    assert_eq!(explicit.canonical_toml(), legacy_replay);
    assert_eq!(explicit.content_hash(), without.content_hash());
}

#[test]
fn live_documents_validate_round_trip_and_bind_the_hash() {
    use binary_alpha_engine::config::{Config, RunMode};
    let source = std::fs::read_to_string(fixture("live.toml")).unwrap();
    let original = Config::parse(&source).unwrap();
    let mut without = original.clone();
    without.live = None;
    assert_ne!(original.content_hash(), without.content_hash());
    for (name, config) in [
        ("live-research", original.clone()),
        ("without-live", without),
    ] {
        let output = validate_document(name, &config.canonical_toml());
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(output.stderr.is_empty());
        assert_eq!(
            String::from_utf8(output.stdout).unwrap(),
            format!(
                "# content-hash: {}\n{}",
                config.content_hash(),
                config.canonical_toml()
            )
        );
    }
    // Validation alone reads no broker credentials, certificate, event log, or bundle objects.
    for mode in [
        RunMode::Research,
        RunMode::Replay,
        RunMode::Paper,
        RunMode::Live,
    ] {
        let mut config = original.clone();
        config.run_mode = mode;
        if matches!(mode, RunMode::Paper | RunMode::Live) {
            config.live.as_mut().unwrap().replay = None;
        }
        let output = validate_document(&format!("live-mode-{mode}"), &config.canonical_toml());
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(output.stderr.is_empty());
        let canonical = String::from_utf8(output.stdout).unwrap();
        assert_eq!(Config::parse(&canonical).unwrap(), config);
    }
}

#[test]
fn live_mode_and_binding_rejections_name_the_exact_rule() {
    use binary_alpha_engine::config::{Broker, Config, RunMode};
    let source = std::fs::read_to_string(fixture("live.toml")).unwrap();
    let original = Config::parse(&source).unwrap();
    let mut cases = Vec::new();
    let mut changed = original.clone();
    changed.live.as_mut().unwrap().execution_contract = "other".into();
    cases.push((changed, "live.execution_contract:"));
    for mode in [RunMode::Paper, RunMode::Live] {
        let mut changed = original.clone();
        changed.run_mode = mode;
        changed.live = None;
        cases.push((changed, "live: is required for run_mode paper or live"));
        let mut changed = original.clone();
        changed.run_mode = mode;
        cases.push((
            changed,
            "live.replay: must be absent for run_mode paper or live",
        ));
        for missing in ["credential", "account_class"] {
            let mut changed = original.clone();
            changed.run_mode = mode;
            changed.live.as_mut().unwrap().replay = None;
            let Broker::Deriv(broker) = &mut changed.brokers[0] else {
                unreachable!()
            };
            if missing == "credential" {
                broker.credential = None;
            } else {
                broker.account_class = None;
            }
            cases.push((
                changed,
                if missing == "credential" {
                    "live.broker: credential is required for run_mode paper or live"
                } else {
                    "brokers[0]: account_class is required with credential"
                },
            ));
        }
    }
    for mode in [RunMode::Research, RunMode::Replay] {
        let mut changed = original.clone();
        changed.run_mode = mode;
        changed.live.as_mut().unwrap().replay = None;
        cases.push((
            changed,
            "live.replay: is required for run_mode research or replay",
        ));
    }
    let mut changed = original.clone();
    changed.replay = research_fixture::replay_configuration(std::path::Path::new(
        "/synthetic/config-only-never-opened",
    ))
    .replay;
    cases.push((changed, "live: cannot be combined with [replay]"));
    let mut changed = original.clone();
    changed.live.as_mut().unwrap().broker = "undeclared".to_string().try_into().unwrap();
    cases.push((
        changed,
        "live.broker: broker is not declared under [[brokers]]",
    ));
    let mut changed = original;
    changed.run_mode = RunMode::Live;
    changed.live.as_mut().unwrap().replay = None;
    changed.storage.publication_uri = "file:///synthetic/publication".parse().unwrap();
    cases.push((changed, "storage.publication_uri: a `file://` destination is the non-live test boundary and requires run_mode `research`, not `live`"));
    for (index, (config, reason)) in cases.into_iter().enumerate() {
        let output = validate_document(&format!("live-invalid-{index}"), &config.canonical_toml());
        assert_eq!(output.status.code(), Some(1), "case {index}");
        assert!(output.stdout.is_empty());
        let error = String::from_utf8(output.stderr).unwrap();
        assert!(error.starts_with(reason), "case {index}: {error}");
    }
}

#[test]
fn live_tables_reject_unknown_fields_and_invalid_measurement_bounds() {
    use binary_alpha_engine::config::Config;
    use serde_json::json;
    let source = std::fs::read_to_string(fixture("live.toml")).unwrap();
    for table in [
        "live",
        "live.compatibility",
        "live.journal",
        "live.control",
        "live.replay",
    ] {
        let document = source.replace(
            &format!("[{table}]\n"),
            &format!("[{table}]\nunexpected_option = true\n"),
        );
        let output = validate_document(&format!("unknown-{table}"), &document);
        assert_eq!(output.status.code(), Some(1));
        assert!(output.stdout.is_empty());
        assert!(
            String::from_utf8(output.stderr)
                .unwrap()
                .contains("unknown field `unexpected_option`")
        );
    }
    let original = serde_json::to_value(Config::parse(&source).unwrap()).unwrap();
    for (index, (field, value)) in [
        ("account", json!("")),
        ("warmup", json!([])),
        ("compatibility.observation_start", json!("not-a-time")),
        (
            "compatibility.observation_end",
            json!("2026-01-05T00:00:00Z"),
        ),
        ("compatibility.min_samples", json!(0)),
        ("journal.dir", json!("../journal")),
        ("journal.segment_records", json!(0)),
        ("journal.max_spool_bytes", json!(0)),
        ("control.credential", json!("1INVALID")),
        ("control.root_certificate", json!("/root.pem")),
        ("control.lease_ttl_micros", json!(0)),
        ("control.renewal_interval_micros", json!(0)),
        ("control.renewal_interval_micros", json!(10_000_000)),
        ("control.safety_margin_micros", json!(-1)),
        ("control.safety_margin_micros", json!(8_000_000)),
        ("control.safety_margin_micros", json!(i64::MAX)),
        ("replay.broker_log", json!("../events.jsonl")),
    ]
    .into_iter()
    .enumerate()
    {
        let mut changed = original.clone();
        let target = field
            .split('.')
            .fold(&mut changed["live"], |value, key| &mut value[key]);
        *target = value;
        let changed: Config = serde_json::from_value(changed).unwrap();
        let output = validate_document(&format!("live-bound-{index}"), &changed.canonical_toml());
        assert_eq!(output.status.code(), Some(1), "{field}");
        assert!(output.stdout.is_empty());
        let error = String::from_utf8(output.stderr).unwrap();
        assert!(
            error.starts_with(&format!("live.{field}:")),
            "{field}: {error}"
        );
    }
}

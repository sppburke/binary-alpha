//! Exercises `binary-alpha config validate` end to end against checked-in fixtures.

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
fn checked_in_example_and_its_equivalent_produce_the_fixed_report() {
    for path in [example(), fixture("equivalent.toml")] {
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

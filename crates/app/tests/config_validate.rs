//! Exercises `binary-alpha config validate` end to end against checked-in fixtures.

use std::process::{Command, Output};

const EXPECTED_REPORT: &str = "# content-hash: v1:sha256:c62f3b3e1a61e1897c2c08f5d39db1e2b7aa8e96229623c73affb9a1862b7e2d\nschema_version = 1\nrun_mode = \"research\"\n";

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
fn help_names_the_config_command() {
    let output = binary_alpha(&["--help"]);
    assert!(output.status.success());
    assert!(String::from_utf8(output.stdout).unwrap().contains("config"));
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
    let report = String::from_utf8(validate(&example()).stdout).unwrap();
    let path = format!("{}/report.toml", env!("CARGO_TARGET_TMPDIR"));
    std::fs::write(&path, &report).unwrap();
    assert_eq!(String::from_utf8(validate(&path).stdout).unwrap(), report);
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

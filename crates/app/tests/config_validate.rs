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

#[test]
fn help_names_the_config_command() {
    let output = binary_alpha(&["--help"]);
    assert!(output.status.success());
    assert!(String::from_utf8(output.stdout).unwrap().contains("config"));
}

#[test]
fn checked_in_example_and_its_equivalent_produce_the_fixed_report() {
    let example = format!("{}/../../configs/example.toml", env!("CARGO_MANIFEST_DIR"));
    for path in [example, fixture("equivalent.toml")] {
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
fn invalid_documents_fail_with_field_specific_errors() {
    let cases = [
        (
            "missing_schema_version.toml",
            "missing field `schema_version`",
        ),
        ("missing_run_mode.toml", "missing field `run_mode`"),
        (
            "unsupported_schema_version.toml",
            "unsupported schema_version 2, expected 1",
        ),
        ("unsupported_run_mode.toml", "unknown variant `browser`"),
        ("unknown_field.toml", "unknown field `retry_count`"),
        ("raw_secret.toml", "unknown field `api_token`"),
    ];
    for (name, expected) in cases {
        let output = validate(&fixture(name));
        let stderr = String::from_utf8(output.stderr).unwrap();
        assert_eq!(output.status.code(), Some(1), "{name}");
        assert!(output.stdout.is_empty(), "{name}");
        assert!(stderr.contains(expected), "{name}: {stderr}");
    }
}

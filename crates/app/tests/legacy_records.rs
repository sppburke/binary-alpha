//! Immutable records from the schema-1 writers at fe93017, verified through the public CLI.

mod common;

use std::fs;
use std::path::Path;
use std::process::Command;

use binary_alpha_engine::config::Outputs;
use binary_alpha_engine::features::FeaturePlan;
use common::Scratch;
use serde_json::{Value, json};

const WRITER: &str = "fe93017a2fdec8d209d036ed8d7da9c370e185f9";
const RUN: &str = "e735202ea7ecc1345825215733510b9276a11e2927f523d40ae33a46e45a68cf";
const SELECTION: &str = "f41b61b63bbfde46c43b2bcebcb039f463f26562521852358279a0cb31e25aa7";
const FAMILIES: [(&str, &str); 2] = [
    (
        "d7924115f6ec1c2219d5239081221020315db2bf998d950c10cb60d194c53f67",
        "c2c579fbc3504ab7dfc4a632633a7be73334fadd322fe6f5048633735bec5b74",
    ),
    (
        "73486072a7f59ab1df2e6f2f2b704454759ec5c23ff977b874b58c0f3fd180d7",
        "09c0ab2fc423f0024f66af3b2d4ab46f2593356fd0322f4cf7f797cab123850d",
    ),
];
const FEATURES: [(&str, &str); 8] = [
    (
        "24409612301a6ee325fcdac35b37641c65cca1475a72f93b244f7df2efe79cc1",
        "09c0ab2fc423f0024f66af3b2d4ab46f2593356fd0322f4cf7f797cab123850d",
    ),
    (
        "40263899da5e4e58aa331cbc978d044356b7f60ffd6f32db2463056cd029a90e",
        "09c0ab2fc423f0024f66af3b2d4ab46f2593356fd0322f4cf7f797cab123850d",
    ),
    (
        "66cf1bc75d8b7ca622496619345560d56a5c73fe9e6bf02853bcc44387aa3fc9",
        "c2c579fbc3504ab7dfc4a632633a7be73334fadd322fe6f5048633735bec5b74",
    ),
    (
        "97683c23ab072d6763cbe4aff952189ae477b990b1b370b5828a56fd86a19654",
        "f3b004aa565f36508bbd255e43c761bde255bfededb9f8d7090a5bf49c3c3c62",
    ),
    (
        "a7ccab4e17b84ad665de5b29c9ccbca10df2b926d7dfe7a3c4987267092f8f29",
        "f3b004aa565f36508bbd255e43c761bde255bfededb9f8d7090a5bf49c3c3c62",
    ),
    (
        "bc01ba9344f078a18659c982b05c74801d43ae44e2eaff729e8e7defa60bf05c",
        "c2c579fbc3504ab7dfc4a632633a7be73334fadd322fe6f5048633735bec5b74",
    ),
    (
        "c767abdbea7095058a78be04182f27da1b69a645171ae80f9ea0e7972f9fd152",
        "d232fc2882aa19ba8dbf495110d16e5922c38535fb19abd1932e435fa288c0c4",
    ),
    (
        "e61ee3c55fd134576dbdc7d8a0811d409630b2f21a639c5c7521494449095d35",
        "d232fc2882aa19ba8dbf495110d16e5922c38535fb19abd1932e435fa288c0c4",
    ),
];

fn copy_tree(source: &Path, destination: &Path) {
    fs::create_dir_all(destination).unwrap();
    for entry in fs::read_dir(source).unwrap() {
        let entry = entry.unwrap();
        let target = destination.join(entry.file_name());
        let kind = entry.file_type().unwrap();
        if kind.is_dir() {
            copy_tree(&entry.path(), &target);
        } else {
            assert!(kind.is_file(), "fixture may contain only regular files");
            fs::copy(entry.path(), target).unwrap();
        }
    }
}

fn manifest(root: &Path, generation: &str) -> Value {
    serde_json::from_slice(
        &fs::read(
            root.join("published/manifests")
                .join(generation)
                .join("ready.json"),
        )
        .unwrap(),
    )
    .unwrap()
}

fn object(root: &Path, manifest: &Value, name: &str) -> Vec<u8> {
    let key = manifest["objects"]
        .as_array()
        .unwrap()
        .iter()
        .find(|object| object["path"] == name)
        .unwrap()["key"]
        .as_str()
        .unwrap();
    fs::read(root.join("published").join(key)).unwrap()
}

fn verify(scratch: &Scratch, generation: &str) {
    // The schema-1 config embeds file:///proc/self/cwd paths. Set only the child CLI's cwd
    // to the copied scratch store, so immutable records resolve without rewriting identities.
    let uri = format!("file:///proc/self/cwd/published/manifests/{generation}/ready.json");
    let output = Command::new(env!("CARGO_BIN_EXE_binary-alpha"))
        .current_dir(&scratch.root)
        .args([
            "data",
            "verify",
            "--manifest",
            &uri,
            "--config",
            scratch.path("research.toml").to_str().unwrap(),
        ])
        .env("BINARY_ALPHA_STORE_LOG", scratch.path("access.log"))
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{generation}: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(output.stderr.is_empty());
    assert!(
        String::from_utf8(output.stdout)
            .unwrap()
            .starts_with("verified ")
    );
}

#[test]
fn schema1_records_remain_verifiable() {
    let fixture = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/legacy_schema1");
    let scratch = Scratch::new("legacy_schema1");
    copy_tree(&fixture, &scratch.root);

    let mut generations: Vec<_> = fs::read_dir(scratch.path("published/manifests"))
        .unwrap()
        .map(|entry| entry.unwrap())
        .filter(|entry| entry.path().join("ready.json").is_file())
        .map(|entry| entry.file_name().to_string_lossy().into_owned())
        .collect();
    generations.sort();
    assert_eq!(generations.len(), 45);
    for generation in &generations {
        verify(&scratch, generation);
    }

    let run_manifest = manifest(&scratch.root, RUN);
    assert_eq!(run_manifest["schema_version"], 1);
    assert_eq!(run_manifest["code_revision"], WRITER);
    assert_eq!(run_manifest["state"], "awaiting_holdout_authorization");
    let run: Value =
        serde_json::from_slice(&object(&scratch.root, &run_manifest, "research.json")).unwrap();
    assert_eq!(
        run["state"],
        json!({"status":"awaiting_holdout_authorization"})
    );
    assert_eq!(run["selection"], SELECTION);
    assert_eq!(run["outer"].as_array().unwrap().len(), 3);
    assert_eq!(run["outer"][0]["outer"]["projection"]["settled"], 2);
    assert_eq!(run["outer"][0]["outer"]["projection"]["profit"], "6.20");
    assert_eq!(run["outer"][2]["outer"]["projection"]["profit"], "5.80");

    for (index, (generation, plan_identity)) in FAMILIES.iter().enumerate() {
        assert_eq!(run["instruments"][index]["family"], *generation);
        let ready = manifest(&scratch.root, generation);
        assert_eq!(ready["schema_version"], 1);
        assert_eq!(ready["code_revision"], WRITER);
        assert_eq!(ready["members"], 2);
        let family: Value =
            serde_json::from_slice(&object(&scratch.root, &ready, "family.json")).unwrap();
        assert_eq!(family["plan_identity"], *plan_identity);
        assert_eq!(family["members"].as_array().unwrap().len(), 2);
        assert_eq!(
            family["members"]
                .as_array()
                .unwrap()
                .iter()
                .filter(|member| !member["rank"].is_null())
                .count(),
            2,
            "schema-1 survivors"
        );
        assert_eq!(family["members"][0]["rank"], 1);
        assert_eq!(family["members"][1]["rank"], 2);
    }

    let selection_manifest = manifest(&scratch.root, SELECTION);
    assert_eq!(selection_manifest["schema_version"], 1);
    assert_eq!(selection_manifest["code_revision"], WRITER);
    let selection: Value = serde_json::from_slice(&object(
        &scratch.root,
        &selection_manifest,
        "selection.json",
    ))
    .unwrap();
    assert_eq!(selection["state"], json!({"status":"selected"}));
    assert_eq!(selection["selected"], 11);
    assert_eq!(selection["members"].as_array().unwrap().len(), 4);
    assert_eq!(
        selection["members"]
            .as_array()
            .unwrap()
            .iter()
            .map(|member| (
                member["family"].as_u64().unwrap(),
                member["member"].as_u64().unwrap()
            ))
            .collect::<Vec<_>>(),
        [(0, 0), (0, 1), (1, 0), (1, 1)]
    );
    assert_eq!(selection["families"][0]["members"][0]["bases"], json!([0]));
    assert_eq!(selection["families"][0]["members"][1]["bases"], json!([1]));
    assert_eq!(selection["families"][1]["members"][0]["bases"], json!([2]));
    assert_eq!(selection["families"][1]["members"][1]["bases"], json!([3]));
    let choice = &selection["choices"][11];
    assert_eq!(choice["subset"], 4);
    assert_eq!(choice["alternatives"], json!([1]));
    assert_eq!(choice["folds"][0]["projection"]["settled"], 2);
    assert_eq!(choice["folds"][0]["projection"]["profit"], "6.20");
    let chosen_member = selection["config"]["portfolio"]["subsets"][4]["deployments"][0]["member"]
        .as_u64()
        .unwrap() as usize;
    assert_eq!(chosen_member, 3);
    assert_eq!(selection["members"][chosen_member]["family"], 1);
    assert_eq!(selection["members"][chosen_member]["member"], 1);
    assert_eq!(
        selection["frozen"]["strategies"][0]["conditions"][0]["threshold"],
        "down"
    );
    assert_eq!(
        selection["frozen"]["strategies"][0]["plan_identity"],
        "d232fc2882aa19ba8dbf495110d16e5922c38535fb19abd1932e435fa288c0c4"
    );

    for (generation, identity) in FEATURES {
        let ready = manifest(&scratch.root, generation);
        assert_eq!(ready["schema_version"], 1);
        assert_eq!(ready["plan_identity"], identity);
        assert_eq!(ready["streams"][0]["rows"], 4);
        let plan = FeaturePlan::from_json(&object(&scratch.root, &ready, "plan.json")).unwrap();
        assert_eq!(plan.identity(), identity);
        assert_eq!(plan.settings.outputs, Outputs::AllSupported);
    }

    let declaration: Value =
        serde_json::from_slice(&fs::read(scratch.path("declaration.json")).unwrap()).unwrap();
    for population in declaration["populations"].as_array().unwrap() {
        if population["role"] == "holdout" {
            let generation = population["generations"][0].as_str().unwrap();
            assert!(
                !scratch
                    .path("published/manifests")
                    .join(generation)
                    .exists()
            );
            assert!(
                !fs::read_to_string(scratch.path("access.log"))
                    .unwrap()
                    .contains(generation)
            );
        }
    }
}

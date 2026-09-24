//! Immutable records from the schema-1 writers at fe93017, verified through the public CLI.

mod common;

use std::fs;
use std::path::Path;
use std::process::Command;

use binary_alpha_engine::features::FeaturePlan;
use common::Scratch;
use serde_json::{Value, json};

const WRITER: &str = "fe93017a2fdec8d209d036ed8d7da9c370e185f9";
const RUN: &str = "1ff6bcd7bd7cc941ba6ab93685edc42985457d3b455f62f96c1d9e3e7d880740";
const SELECTION: &str = "2ee96a1b86f8aa23ac9351bcaf5f502f2a1ffdf5afb25c59ae1199d1bd594d2d";
const FAMILIES: [(&str, &str); 2] = [
    (
        "409663ae0efeeb5f7a8cc0b4014f80827e11e3f7bbada6dfe4746211a8d493e9",
        "bdb7400d739469757be2196113cb51d382c65c92bbfd2d760e5d9153ca22c118",
    ),
    (
        "4421d5db7f772c227901e75804145cda10079a6afdf1cd9cce0c77fab73b90cf",
        "1e849138f50b67ecbe767c117ea324b0d05ef6d2c7ba41bbeee3e46e5b6e551f",
    ),
];
const FEATURES: [(&str, &str); 8] = [
    (
        "057aae44fd62c232dc1aea9bbb25df1b732efd0f041a1f714aae3dea12ba79ac",
        "976c576c1087085825d10d7a13d53a793ab8bb84ef9a921a7d31c38a070ca932",
    ),
    (
        "096abefd050eeb42e68e17ed4d8a52e7bd0298f48a51794a78615cdb9dcdba8f",
        "bdb7400d739469757be2196113cb51d382c65c92bbfd2d760e5d9153ca22c118",
    ),
    (
        "212696cf966a83bbb1cb21941a2e83e644f29d3a065c8a59410b5e3593cd1b31",
        "fd08eeb5231757853942e275a03d88e39edd1de2fef9ccf9a424fb413fbcb956",
    ),
    (
        "49988f58f2b54839ab1f1fddaa13f9294c277e7d86a87a6786d7771cb3a92109",
        "fd08eeb5231757853942e275a03d88e39edd1de2fef9ccf9a424fb413fbcb956",
    ),
    (
        "815f7ff188ace34ada25c914d6b806d6b3376b008c0d1d2c55c646bb2f00d19f",
        "1e849138f50b67ecbe767c117ea324b0d05ef6d2c7ba41bbeee3e46e5b6e551f",
    ),
    (
        "95dbb99ab1490049f7d2f199e84750d739a9801665bec176b6c58c32e31f77f7",
        "976c576c1087085825d10d7a13d53a793ab8bb84ef9a921a7d31c38a070ca932",
    ),
    (
        "a17b7a09bd13b53be037fbfb740a8971daf1479ff06e7d80508bdfa413e153e9",
        "bdb7400d739469757be2196113cb51d382c65c92bbfd2d760e5d9153ca22c118",
    ),
    (
        "e0f530bc8d06c0434b980b5a27981336fc10469781cd825270e3290e8a2d1f69",
        "1e849138f50b67ecbe767c117ea324b0d05ef6d2c7ba41bbeee3e46e5b6e551f",
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

    verify(&scratch, RUN);
    for (generation, _) in FAMILIES {
        verify(&scratch, generation);
    }
    verify(&scratch, SELECTION);
    for (generation, _) in FEATURES {
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
        "976c576c1087085825d10d7a13d53a793ab8bb84ef9a921a7d31c38a070ca932"
    );

    for (generation, identity) in FEATURES {
        let ready = manifest(&scratch.root, generation);
        assert_eq!(ready["schema_version"], 1);
        assert_eq!(ready["plan_identity"], identity);
        let plan = FeaturePlan::from_json(&object(&scratch.root, &ready, "plan.json")).unwrap();
        assert_eq!(plan.identity(), identity);
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

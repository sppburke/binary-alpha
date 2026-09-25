//! The build script must refresh its revision stamp when any computation input changes.

#[allow(dead_code)]
#[path = "../build.rs"]
mod build_script;

#[test]
fn revision_stamp_watches_workspace_computation_inputs() {
    let watched = build_script::REVISION_INPUTS;
    for path in [
        "src",
        "../engine/src",
        "../accelerator/src",
        "../accelerator/kernels",
        "../../Cargo.toml",
        "../../Cargo.lock",
        "Cargo.toml",
        "../engine/Cargo.toml",
        "../accelerator/Cargo.toml",
    ] {
        assert!(watched.contains(&path), "missing revision input {path}");
        assert!(
            std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                .join(path)
                .exists(),
            "missing revision input path {path}"
        );
    }
}

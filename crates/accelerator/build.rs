//! Compiles the preserved kernels only for the explicitly enabled device backend.

use std::{env, fs, path::PathBuf, process::Command};

fn quote(text: &str) -> String {
    let mut out = String::from("\"");
    for ch in text.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            ch if ch.is_control() => out.push_str(&format!("\\u{:04x}", ch as u32)),
            ch => out.push(ch),
        }
    }
    out.push('"');
    out
}

fn main() {
    println!("cargo:rerun-if-changed=kernels");
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-env-changed=BINARY_ALPHA_NVCC");
    println!("cargo:rerun-if-env-changed=BINARY_ALPHA_CUDA_ARCH");
    if env::var_os("CARGO_FEATURE_CUDA").is_none() {
        return;
    }
    let sources = [
        "score_bucket_plans_cap1",
        "score_bucket_plans_cap1_dual",
        "score_bucket_plans_cap1_basic",
        "score_bucket_plans_cap1_basic_dual",
        "score_bucket_plans_cap1_sparse",
        "score_bucket_plans_cap1_basic_sparse",
        "score_bucket_plans_cap1_sparse_dual",
        "score_bucket_plans_cap1_basic_sparse_dual",
        "reconstruct_signal_masks_cap1",
        "bootstrap_path_metrics",
        "replay_capacity",
        "path_drawdown",
        "replay_policies",
    ];
    let compiler = env::var_os("BINARY_ALPHA_NVCC").map(PathBuf::from).unwrap_or_else(|| {
        env::split_paths(&env::var_os("PATH").unwrap_or_default())
            .map(|path| path.join("nvcc"))
            .find(|path| path.is_file())
            .expect("CUDA build requires nvcc on PATH or an exact compiler path in BINARY_ALPHA_NVCC")
    });
    let version = Command::new(&compiler)
        .arg("--version")
        .output()
        .unwrap_or_else(|error| {
            panic!(
                "cannot execute BINARY_ALPHA_NVCC compiler {}: {error}",
                compiler.display()
            )
        });
    assert!(
        version.status.success(),
        "BINARY_ALPHA_NVCC compiler --version failed: {}",
        String::from_utf8_lossy(&version.stderr)
    );
    let version = String::from_utf8(version.stdout).expect("nvcc version is UTF-8");
    let lines: Vec<_> = version.lines().collect();
    let version = lines[lines.len().saturating_sub(2)..].join("\n");
    let out = PathBuf::from(env::var_os("OUT_DIR").expect("Cargo OUT_DIR"));
    let mut source = Vec::new();
    for symbol in sources {
        source.extend(fs::read(format!("kernels/{symbol}.cu")).expect("read kernel"));
    }
    let source_path = out.join("module.cu");
    let binary_path = out.join("module.cubin");
    fs::write(&source_path, source).expect("write concatenated source");
    let architecture = env::var("BINARY_ALPHA_CUDA_ARCH").unwrap_or_else(|_| "sm_120".into());
    let flags = [
        format!("-arch={architecture}"),
        "-cubin".into(),
        "--std=c++11".into(),
        "-o".into(),
        binary_path.display().to_string(),
        source_path.display().to_string(),
    ];
    let result = Command::new(&compiler)
        .args(&flags)
        .output()
        .expect("execute BINARY_ALPHA_NVCC compiler");
    assert!(
        result.status.success(),
        "BINARY_ALPHA_NVCC compiler {} failed: {}",
        compiler.display(),
        String::from_utf8_lossy(&result.stderr)
    );
    let metadata = format!(
        "{{\n  \"compiler_path\": {},\n  \"compiler_version\": {},\n  \"flags\": [{}],\n  \"architecture\": {},\n  \"source_order\": [{}]\n}}\n",
        quote(&compiler.display().to_string()),
        quote(&version),
        flags
            .iter()
            .map(|arg| quote(arg))
            .collect::<Vec<_>>()
            .join(", "),
        quote(&architecture),
        sources
            .iter()
            .map(|symbol| quote(symbol))
            .collect::<Vec<_>>()
            .join(", "),
    );
    fs::write(out.join("module.json"), metadata).expect("write module build metadata");
}

//! Records the producing Git revision so dataset manifests can name it without a package or a
//! command-line argument.

use std::process::Command;

fn git(args: &[&str]) -> Option<String> {
    let output = Command::new("git").args(args).output().ok()?;
    output
        .status
        .success()
        .then(|| String::from_utf8_lossy(&output.stdout).trim().to_string())
}

fn main() {
    let revision = match git(&["rev-parse", "HEAD"]) {
        Some(commit) if !commit.is_empty() => match git(&["status", "--porcelain"]) {
            Some(status) if status.is_empty() => commit,
            _ => format!("{commit}-dirty"),
        },
        _ => "unavailable".to_string(),
    };
    println!("cargo:rustc-env=BINARY_ALPHA_CODE_REVISION={revision}");
    // Any source edit changes the dirty state, and `--git-path` resolves HEAD, the checked-out
    // branch ref, and packed refs correctly for linked worktrees.
    println!("cargo:rerun-if-changed=src");
    println!("cargo:rerun-if-changed=../engine/src");
    let mut watched = vec![
        "HEAD".to_string(),
        "index".to_string(),
        "packed-refs".to_string(),
    ];
    watched.extend(git(&["symbolic-ref", "-q", "HEAD"]));
    for name in watched {
        if let Some(path) = git(&["rev-parse", "--git-path", &name]) {
            println!("cargo:rerun-if-changed={path}");
        }
    }
}

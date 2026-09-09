//! The `binary-alpha` executable: configuration validation, historical-data import and
//! verification, and the external adapters those commands need.

mod archive;
mod import;
mod parallel;
mod store;
mod verify;

use std::path::{Path, PathBuf};
use std::process::ExitCode;

use binary_alpha_engine::config::Config;
use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(
    name = "binary-alpha",
    about = "Binary-options research and execution system",
    disable_help_subcommand = true
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Inspect configuration documents.
    #[command(disable_help_subcommand = true)]
    Config {
        #[command(subcommand)]
        command: ConfigCommand,
    },
    /// Import and verify immutable historical datasets.
    #[command(disable_help_subcommand = true)]
    Data {
        #[command(subcommand)]
        command: DataCommand,
    },
}

#[derive(Subcommand)]
enum ConfigCommand {
    /// Validate a document and print its content hash and canonical form.
    Validate {
        /// Path of the TOML configuration document.
        #[arg(long)]
        config: PathBuf,
    },
}

#[derive(Subcommand)]
enum DataCommand {
    /// Retain, publish, and commit every dataset the configuration declares.
    Import {
        /// Path of the TOML configuration document.
        #[arg(long)]
        config: PathBuf,
    },
    /// Re-read one published generation from its ready manifest and objects alone.
    Verify {
        /// `file://` or `gs://` location ending in `manifests/GENERATION/ready.json`.
        #[arg(long)]
        manifest: String,
    },
}

fn main() -> ExitCode {
    let result = match Cli::parse().command {
        Command::Config {
            command: ConfigCommand::Validate { config },
        } => validate(&config).map(|report| print!("{report}")),
        Command::Data {
            command: DataCommand::Import { config },
        } => import::run(&config, &mut std::io::stdout().lock()),
        Command::Data {
            command: DataCommand::Verify { manifest },
        } => verify::run(&manifest).map(|line| println!("{line}")),
    };
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(message) => {
            eprintln!("{message}");
            ExitCode::FAILURE
        }
    }
}

fn validate(path: &Path) -> Result<String, String> {
    let source = std::fs::read_to_string(path)
        .map_err(|error| format!("cannot read {}: {error}", path.display()))?;
    let config = Config::parse(&source).map_err(|error| error.to_string())?;
    Ok(format!(
        "# content-hash: {}\n{}",
        config.content_hash(),
        config.canonical_toml()
    ))
}

//! The `binary-alpha` executable: configuration validation, historical-data import, instrument
//! audit, feature builds, outcome builds, verification, and the external adapters those commands
//! need.

mod archive;
mod audit;
mod features;
mod import;
mod outcomes;
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
    /// Import, audit, and verify immutable historical generations.
    #[command(disable_help_subcommand = true)]
    Data {
        #[command(subcommand)]
        command: DataCommand,
    },
    /// Build feature generations from published instrument streams.
    #[command(disable_help_subcommand = true)]
    Features {
        #[command(subcommand)]
        command: FeaturesCommand,
    },
    /// Build future-only binary-expiry outcomes from published feature generations.
    #[command(disable_help_subcommand = true)]
    Outcomes {
        #[command(subcommand)]
        command: OutcomesCommand,
    },
}

#[derive(Subcommand)]
enum OutcomesCommand {
    /// Label every decision row of the configured feature generation against its tick
    /// generation and publish the outcome generation.
    Build {
        /// Path of the TOML configuration document with the `outcomes` table.
        #[arg(long)]
        config: PathBuf,
    },
}

#[derive(Subcommand)]
enum FeaturesCommand {
    /// Resolve or apply one feature plan per configured instrument and publish the feature
    /// generation.
    Build {
        /// Path of the TOML configuration document naming the feature instruments.
        #[arg(long)]
        config: PathBuf,
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
    /// Feed one published dataset generation through its configured instrument stream and
    /// publish the profile, finalized candles, and stream manifest.
    Audit {
        /// Path of the TOML configuration document naming the instrument.
        #[arg(long)]
        config: PathBuf,
        /// `file://` or `gs://` location of the dataset ready manifest.
        #[arg(long)]
        manifest: String,
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
            command: DataCommand::Audit { config, manifest },
        } => audit::run(&config, &manifest, &mut std::io::stdout().lock()),
        Command::Data {
            command: DataCommand::Verify { manifest },
        } => verify::run(&manifest).map(|line| println!("{line}")),
        Command::Features {
            command: FeaturesCommand::Build { config },
        } => features::run(&config, &mut std::io::stdout().lock()),
        Command::Outcomes {
            command: OutcomesCommand::Build { config },
        } => outcomes::run(&config, &mut std::io::stdout().lock()),
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

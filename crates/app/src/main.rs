//! The `binary-alpha` executable: configuration loading now, external adapters in later phases.

use std::path::{Path, PathBuf};
use std::process::ExitCode;

use binary_alpha_engine::config::Config;
use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(
    name = "binary-alpha",
    version,
    about = "Binary-options research and execution system"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Inspect configuration documents.
    Config {
        #[command(subcommand)]
        command: ConfigCommand,
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

fn main() -> ExitCode {
    let Cli {
        command:
            Command::Config {
                command: ConfigCommand::Validate { config },
            },
    } = Cli::parse();
    match validate(&config) {
        Ok(report) => {
            print!("{report}");
            ExitCode::SUCCESS
        }
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

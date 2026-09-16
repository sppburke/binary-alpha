//! The `binary-alpha` executable: configuration validation, historical-data import, instrument
//! audit, feature builds, outcome builds, engine replay, candidate search, portfolio selection,
//! research certification, ordered live execution and replay, immutable entry authorization,
//! verification, and the external adapters those commands need.

use binary_alpha_app::{
    audit, data_pipeline, features, fetch, import, inspect, live, load_config, outcomes, portfolio,
    replay, research, search, verify,
};

use std::path::{Path, PathBuf};
use std::process::ExitCode;

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
    /// Run or replay the ordered broker runtime and manage entry authorization.
    Live {
        #[command(subcommand)]
        command: LiveCommand,
    },
    /// Inspect a configured broker without purchasing.
    Broker {
        #[command(subcommand)]
        command: BrokerCommand,
    },
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
    /// Replay governed historical inputs through the engine and publish its ledger.
    Replay {
        /// Path of the TOML configuration document with the `replay` table.
        #[arg(long)]
        config: PathBuf,
    },
    /// Enumerate, score, replay, evaluate, and resample one candidate family and publish it.
    Search {
        /// Path of the TOML configuration document with the `search` table.
        #[arg(long)]
        config: PathBuf,
    },
    /// Compare complete joint policies through the engine over development folds and publish
    /// one selection.
    #[command(disable_help_subcommand = true)]
    Portfolio {
        #[command(subcommand)]
        command: PortfolioCommand,
    },
    /// Prepare, search, select, assess, and, under a separate grant, certify one policy.
    #[command(disable_help_subcommand = true)]
    Research {
        #[command(subcommand)]
        command: ResearchCommand,
    },
    /// Operator-only holdout authorizations.
    #[command(disable_help_subcommand = true)]
    Holdout {
        #[command(subcommand)]
        command: HoldoutCommand,
    },
}

#[derive(Subcommand)]
enum LiveCommand {
    Run {
        #[arg(long)]
        config: PathBuf,
    },
    Replay {
        #[arg(long)]
        config: PathBuf,
    },
    Authorization {
        #[command(subcommand)]
        command: AuthorizationCommand,
    },
}

#[derive(Subcommand)]
enum AuthorizationCommand {
    Create {
        #[arg(long)]
        deployment_manifest: String,
        #[arg(long)]
        bundle_manifest: String,
        #[arg(long)]
        broker: String,
        #[arg(long)]
        account: String,
        #[arg(long)]
        reason: String,
    },
}

#[derive(Subcommand)]
enum ResearchCommand {
    /// Run the configured study from its declared historical generations to one immutable
    /// research state, resuming the same identity on every invocation.
    Run {
        /// Path of the TOML configuration document with the `research` table.
        #[arg(long)]
        config: PathBuf,
    },
}

#[derive(Subcommand)]
enum HoldoutCommand {
    /// Holdout grants.
    #[command(disable_help_subcommand = true)]
    Grant {
        #[command(subcommand)]
        command: GrantCommand,
    },
}

#[derive(Subcommand)]
enum GrantCommand {
    /// Create the one-use grant that authorizes exactly one frozen bundle to open exactly the
    /// declared holdout generations; never opens holdout data.
    Create {
        /// Path of the TOML configuration document with the `research.study` table.
        #[arg(long)]
        config: PathBuf,
        /// `file://` or `gs://` location of the research run ready manifest.
        #[arg(long)]
        bundle_manifest: String,
        /// `file://` or `gs://` location of one declared holdout ready manifest; repeat once per
        /// instrument in instrument order.
        #[arg(long, required = true)]
        holdout_manifest: Vec<String>,
        /// The recorded reason for the authorization.
        #[arg(long)]
        reason: String,
    },
}

#[derive(Subcommand)]
enum PortfolioCommand {
    /// Enumerate every declared complete policy, replay each one jointly per inner fold, select
    /// under the frozen objective, refit, optionally evaluate, and publish the selection.
    Optimize {
        /// Path of the TOML configuration document with the `portfolio` table.
        #[arg(long)]
        config: PathBuf,
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
enum BrokerCommand {
    Inspect {
        #[arg(long)]
        config: PathBuf,
    },
}

#[derive(Subcommand)]
enum DataCommand {
    /// Fetch and publish bounded broker tick history.
    Fetch {
        #[arg(long)]
        config: PathBuf,
    },
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
        /// Path of a TOML configuration document whose `research.study` declaration permits the
        /// target before it is opened.
        #[arg(long)]
        config: Option<PathBuf>,
    },
    /// Stage, import, extend, archive, list, and restore research market data through one
    /// managed local store and a private Drive archive.
    #[command(disable_help_subcommand = true)]
    Pipeline {
        #[command(subcommand)]
        command: PipelineCommand,
    },
}

#[derive(Subcommand)]
enum PipelineCommand {
    /// Extend every job's imported generation from its frontier to one pinned cutoff within
    /// its budget, then audit, verify, and archive the result.
    Update {
        /// Path of the TOML pipeline document.
        #[arg(long)]
        config: PathBuf,
        /// The pinned cutoff as `YYYY-MM-DDTHH:MM:SS[.ffffff]Z`; absent means now.
        #[arg(long)]
        end: Option<String>,
    },
    /// Select the newest archived catalog of one instrument and restore it unless it is already
    /// in this document's managed store; print the local ready-manifest locations.
    Pull {
        #[arg(long)]
        config: PathBuf,
        #[arg(long)]
        broker: String,
        #[arg(long)]
        symbol: String,
    },
    /// List every archived catalog of one instrument.
    List {
        #[arg(long)]
        config: PathBuf,
        #[arg(long)]
        broker: String,
        #[arg(long)]
        symbol: String,
    },
    /// Install exactly one catalog's dataset and stream closure into this document's managed
    /// store and print their local ready-manifest locations.
    Restore {
        #[arg(long)]
        config: PathBuf,
        /// The catalog's Drive file identifier.
        #[arg(long)]
        catalog: String,
        /// The catalog's expected SHA-256.
        #[arg(long)]
        sha256: String,
        #[arg(long)]
        broker: String,
        #[arg(long)]
        symbol: String,
    },
}

fn main() -> ExitCode {
    let result = match Cli::parse().command {
        Command::Live {
            command: LiveCommand::Run { config },
        } => live::run(&config, &mut std::io::stdout().lock()),
        Command::Live {
            command: LiveCommand::Replay { config },
        } => live::replay(&config, &mut std::io::stdout().lock()),
        Command::Live {
            command:
                LiveCommand::Authorization {
                    command:
                        AuthorizationCommand::Create {
                            deployment_manifest,
                            bundle_manifest,
                            broker,
                            account,
                            reason,
                        },
                },
        } => live::authorize(
            &deployment_manifest,
            &bundle_manifest,
            &broker,
            &account,
            &reason,
            &mut std::io::stdout().lock(),
        ),
        Command::Data {
            command: DataCommand::Fetch { config },
        } => fetch::run(&config, &mut std::io::stdout().lock()),
        Command::Broker {
            command: BrokerCommand::Inspect { config },
        } => inspect::run(&config, &mut std::io::stdout().lock()),
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
            command: DataCommand::Verify { manifest, config },
        } => verify::run_configured(config.as_deref(), &manifest).map(|line| println!("{line}")),
        Command::Data {
            command: DataCommand::Pipeline { command },
        } => match command {
            PipelineCommand::Update { config, end } => {
                data_pipeline::update(&config, end.as_deref(), &mut std::io::stdout().lock())
            }
            PipelineCommand::Pull {
                config,
                broker,
                symbol,
            } => data_pipeline::pull(&config, &broker, &symbol, &mut std::io::stdout().lock()),
            PipelineCommand::List {
                config,
                broker,
                symbol,
            } => data_pipeline::list(&config, &broker, &symbol, &mut std::io::stdout().lock()),
            PipelineCommand::Restore {
                config,
                catalog,
                sha256,
                broker,
                symbol,
            } => data_pipeline::restore(
                &config,
                &catalog,
                &sha256,
                &broker,
                &symbol,
                &mut std::io::stdout().lock(),
            ),
        },
        Command::Features {
            command: FeaturesCommand::Build { config },
        } => features::run(&config, &mut std::io::stdout().lock()),
        Command::Outcomes {
            command: OutcomesCommand::Build { config },
        } => outcomes::run(&config, &mut std::io::stdout().lock()),
        Command::Replay { config } => replay::run(&config, &mut std::io::stdout().lock()),
        Command::Search { config } => search::run(&config, &mut std::io::stdout().lock()),
        Command::Portfolio {
            command: PortfolioCommand::Optimize { config },
        } => portfolio::run(&config, &mut std::io::stdout().lock()),
        Command::Research {
            command: ResearchCommand::Run { config },
        } => research::run(&config, &mut std::io::stdout().lock()),
        Command::Holdout {
            command:
                HoldoutCommand::Grant {
                    command:
                        GrantCommand::Create {
                            config,
                            bundle_manifest,
                            holdout_manifest,
                            reason,
                        },
                },
        } => research::grant(
            &config,
            &bundle_manifest,
            &holdout_manifest,
            &reason,
            &mut std::io::stdout().lock(),
        ),
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
    let config = load_config(path)?;
    Ok(format!(
        "# content-hash: {}\n{}",
        config.content_hash(),
        config.canonical_toml()
    ))
}

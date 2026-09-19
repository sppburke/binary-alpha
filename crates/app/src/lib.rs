pub mod archive;
pub mod audit;
pub mod daily;
pub mod data_pipeline;
pub mod drive;
pub mod features;
pub mod import;
pub mod lineage;
pub mod live;
pub mod outcomes;
pub mod parallel;
pub mod portfolio;
pub mod registry;
pub mod replay;
pub mod research;
pub mod retire;
pub mod search;
mod session_audit;
mod session_candles;
mod session_migration;
pub mod store;
pub mod verify;

pub mod broker;
pub mod fetch;
pub mod inspect;

use binary_alpha_engine::config::Config;
use std::path::Path;

/// The schema, run mode, and storage of a configuration with every table cleared: the base of
/// a synthesized configuration whose hash and generations depend on nothing but the one table
/// its caller adds.
pub fn skeleton(config: &Config) -> Config {
    Config {
        import: None,
        instruments: Vec::new(),
        features: None,
        outcomes: None,
        replay: None,
        accelerator: None,
        search: None,
        portfolio: None,
        research: None,
        brokers: Vec::new(),
        history: None,
        inspect: None,
        live: None,
        ..config.clone()
    }
}

/// All configuration-path commands share parsing and build-capability checks.
pub fn load_config(path: &Path) -> Result<Config, String> {
    let source = std::fs::read_to_string(path)
        .map_err(|error| format!("cannot read {}: {error}", path.display()))?;
    let config = Config::parse(&source).map_err(|error| error.to_string())?;
    check_config_capabilities(&config)?;
    Ok(config)
}

pub(crate) fn check_config_capabilities(config: &Config) -> Result<(), String> {
    if !cfg!(feature = "cuda")
        && config
            .accelerator
            .as_ref()
            .is_some_and(|section| section.backend == binary_alpha_engine::config::Backend::Cuda)
    {
        return Err("accelerator.backend: `cuda` requested but this binary was built without the `cuda` feature".into());
    }
    Ok(())
}

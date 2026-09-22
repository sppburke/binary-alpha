//! Discovery-backed job registration. Broker and history policy come from a core template;
//! publication of the pipeline entry is the final, atomic registration step.
use super::*;
use binary_alpha_engine::market::{Currency, InstrumentId, PriceScale, ProviderSymbol};
use serde::Deserialize;

#[derive(Debug)]
pub struct Options<'a> {
    pub broker: &'a str,
    pub symbol: &'a str,
    pub template: &'a Path,
    pub quote_currency: Option<&'a str>,
    pub price_scale: Option<u8>,
    /// TOML document containing exactly the singular session table's fields.
    pub session: Option<&'a Path>,
}

fn quote(symbol: &str) -> Result<String, String> {
    let pair = symbol.strip_prefix("frx").unwrap_or(symbol);
    let pair = pair.strip_suffix("_otc").unwrap_or(pair);
    if pair.len() == 6 && pair.bytes().all(|c| c.is_ascii_uppercase()) {
        Ok(pair[3..].into())
    } else {
        Err("quote currency is ambiguous; supply --quote-currency".into())
    }
}

fn publish_file(path: &Path, bytes: &[u8]) -> Result<(), String> {
    // The record/store owner already supplies flushed create-once publication and byte
    // comparison on retry, including recovery from interrupted temporary writes.
    let parent = path.parent().ok_or("job file requires a parent")?;
    fs::create_dir_all(parent).map_err(|e| e.to_string())?;
    private(parent)?;
    let store = Store::filesystem(parent);
    let name = path
        .file_name()
        .and_then(|n| n.to_str())
        .ok_or("invalid job filename")?;
    research::publish_record(&store, &store, name, bytes).map(|_| ())
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Registration {
    request_hash: String,
    core: String,
    evidence: Vec<u8>,
}

fn install(
    config_path: &Path,
    layout: &Layout,
    job: &Job,
    registration: &Registration,
    out: &mut dyn Write,
) -> Result<(), String> {
    publish_file(&layout.base.join(&job.config), registration.core.as_bytes())?;
    publish_file(&layout.base.join(&job.evidence), &registration.evidence)?;
    // Append preserves comments and operator formatting. An empty serialized jobs = [] must
    // be removed before adding array tables; TOML cannot define jobs twice.
    let original = fs::read_to_string(config_path).map_err(|e| e.to_string())?;
    let mut doc: toml::Value = toml::from_str(&original).map_err(|e| e.to_string())?;
    let text = if doc
        .get("jobs")
        .and_then(toml::Value::as_array)
        .is_some_and(Vec::is_empty)
    {
        doc.as_table_mut().expect("pipeline").remove("jobs");
        toml::to_string(&doc).map_err(|e| e.to_string())?
    } else {
        original
    };
    let entry = toml::to_string(job).map_err(|e| e.to_string())?;
    let text = format!("{text}\n[[jobs]]\n{entry}");
    PipelineConfig::parse(&text)?;
    write_atomic(config_path, text.as_bytes())?;
    writeln!(
        out,
        "pipeline add-job {} config {}",
        job.id,
        job.config.display()
    )
    .map_err(|e| e.to_string())
}

pub fn run(config_path: &Path, options: &Options<'_>, out: &mut dyn Write) -> Result<(), String> {
    options.price_scale.map(PriceScale::try_from).transpose()?;
    let (mut pipeline, layout, _) = load(config_path)?;
    let _lock = writer_lock(&layout)?;
    let broker_id: BrokerId = options.broker.to_string().try_into()?;
    let symbol: ProviderSymbol = options.symbol.to_string().try_into()?;
    let currency: Currency = options
        .quote_currency
        .map(str::to_string)
        .map(Ok)
        .unwrap_or_else(|| quote(options.symbol))?
        .try_into()?;
    let template_text = fs::read_to_string(options.template).map_err(|e| e.to_string())?;
    let mut document: toml::Value = toml::from_str(&template_text).map_err(|e| e.to_string())?;
    let instruments = document
        .get_mut("instruments")
        .and_then(toml::Value::as_array_mut)
        .ok_or("add-job template must contain one instrument policy")?;
    if instruments.len() != 1 {
        return Err("add-job template must contain one instrument policy".into());
    }
    let instrument = instruments[0]
        .as_table_mut()
        .ok_or("invalid instrument policy")?;
    if let Some(path) = options.session {
        let session: toml::Value =
            toml::from_str(&fs::read_to_string(path).map_err(|e| e.to_string())?)
                .map_err(|e| e.to_string())?;
        instrument.insert("session".into(), session);
    }
    let mut core = Config::parse(&toml::to_string(&document).map_err(|e| e.to_string())?)
        .map_err(|e| e.to_string())?;
    crate::check_config_capabilities(&core)?;
    core.instruments[0]
        .session
        .as_ref()
        .ok_or("add-job requires an explicit [instruments.session] table or --session FILE")?
        .calendar()?;
    core.instruments[0].base_currency = None;
    core.instruments[0].broker = broker_id.clone();
    core.instruments[0].provider_symbol = symbol.clone();
    core.instruments[0].quote_currency = currency;
    if core.run_mode != RunMode::Research || core.research.is_some() || core.live.is_some() {
        return Err(
            "add-job requires a research-only template without research/live sections".into(),
        );
    }
    core.brokers.retain(|b| b.id() == &broker_id);
    let [settings] = core.brokers.as_slice() else {
        return Err("add-job broker must occur once in the template".into());
    };
    let source_identity = broker::source_identity(settings);
    let native = match settings.kind() {
        broker::BrokerKind::Deriv => NativeGranularity::Tick,
        broker::BrokerKind::PocketOption => NativeGranularity::Bar { period_seconds: 5 },
    };
    let history = core
        .history
        .as_mut()
        .ok_or("add-job template requires history")?;
    if history.role != DatasetRole::Development
        || history.refresh_interval_seconds.is_some()
        || history.overlap_seconds.is_none()
        || history.max_pages.is_none()
        || history.max_elapsed_seconds.is_none()
    {
        return Err(
            "add-job requires development history with overlap/page/time budgets and no refresh"
                .into(),
        );
    }
    history.broker = broker_id.clone();
    history.instruments = vec![symbol.clone()];
    history.native_granularity = native;
    history.seeds.clear();
    let sample_end = time(&history.end)?;
    core.instruments[0].native_granularity = native;
    core.import = None;
    core.inspect = None;
    core.storage.historical_data_dir = ConfigPath::try_from(layout.store.clone())?;
    core.storage.publication_uri = PublicationUri::Filesystem(layout.store.clone());
    core = Config::parse(&core.canonical_toml()).map_err(|e| e.to_string())?;
    let id = format!(
        "{}-{}-{}",
        options.broker,
        options
            .symbol
            .chars()
            .filter(|c| c.is_ascii_alphanumeric() || *c == '_')
            .collect::<String>(),
        &sha256_hex(options.symbol.as_bytes())[..8]
    );
    for job in &pipeline.jobs {
        let existing = crate::load_config(&layout.base.join(&job.config))?;
        if existing
            .history
            .as_ref()
            .is_some_and(|h| h.broker == broker_id && h.instruments.contains(&symbol))
        {
            return Err(format!(
                "add-job: instrument already registered as {}",
                job.id
            ));
        }
    }
    let job = Job {
        id: id.clone(),
        config: format!("jobs/{id}.toml").into(),
        evidence: format!("evidence/{id}.json").into(),
    };
    if crate::retire::retired_jobs(&layout.state)?.contains(&id) {
        return Err("add-job: this job identity has been retired; preserve its evidence".into());
    }
    pipeline.jobs.push(job.clone());
    pipeline.validate()?;
    let request_hash = sha256_hex(&json_bytes(&(core.canonical_toml(), options.price_scale))?);
    let checkpoint = layout
        .state
        .join("registrations")
        .join(format!("{id}.json"));
    if let Some(saved) = read_json::<Registration>(&checkpoint)? {
        if saved.request_hash != request_hash {
            return Err(
                "add-job: pending registration differs from the requested template/session/options"
                    .into(),
            );
        }
        return install(config_path, &layout, &job, &saved, out);
    }
    // All local policy checks precede credential resolution and discovery.
    let mut adapter = broker::connect(&core)?;
    let discovered = adapter
        .market()
        .discover()?
        .into_iter()
        .find(|i| i.symbol == options.symbol)
        .ok_or("add-job: instrument absent from broker discovery")?;
    let scale = match (discovered.precision, options.price_scale) {
        (Some(required), Some(declared)) if declared < required => {
            return Err(format!(
                "add-job: discovery requires {required} fraction digits; --price-scale {declared} is insufficient"
            ));
        }
        (_, Some(declared)) => declared,
        (Some(required), None) => required,
        (None, None) => {
            let instrument = InstrumentId {
                broker: broker_id,
                provider_symbol: symbol,
            };
            let page = adapter.market().history_page(
                &instrument,
                PriceScale::try_from(0)?,
                Some(sample_end),
                native,
            )?;
            let mut valid = None;
            for digits in 0..=18 {
                if adapter
                    .market()
                    .decode_history(&instrument, &page.raw, digits.try_into()?, native)
                    .is_ok_and(|(_, rows)| !rows.is_empty())
                {
                    valid = Some(digits);
                    break;
                }
            }
            valid.ok_or("add-job: discovery has no precision and no nonempty sample decodes; supply --price-scale after checking provider precision")?
        }
    };
    core.instruments[0].price_scale = scale.try_into()?;
    let output = core.canonical_toml();
    Config::parse(&output).map_err(|e| e.to_string())?;
    let evidence = json_bytes(&serde_json::json!({
        "source_identity": source_identity, "discovery": discovered, "price_scale": scale,
        "precision_basis": if options.price_scale.is_some() { "explicit" } else if discovered.precision.is_some() { "discovery" } else { "bounded_history_sample" }
    }))?;
    let registration = Registration {
        request_hash,
        core: output,
        evidence,
    };
    publish_file(&checkpoint, &json_bytes(&registration)?)?;
    install(config_path, &layout, &job, &registration, out)
}

/// The configuration change follows completion, so it cannot invalidate the sealed plan.
pub fn remove(config_path: &Path, job: &str, out: &mut dyn Write) -> Result<(), String> {
    let (_, layout, _) = load(config_path)?;
    let _lock = writer_lock(&layout)?;
    let (mut config, current_layout, _) = load(config_path)?;
    if current_layout.store != layout.store {
        return Err("remove-job: managed root changed while acquiring writer lock".into());
    }
    if !crate::retire::retired_jobs(&layout.state)?.contains(job) {
        return Err("remove-job requires a completed --whole-job retirement plan".into());
    }
    let old_len = config.jobs.len();
    config.jobs.retain(|j| j.id != job);
    if config.jobs.len() != old_len {
        write_atomic(
            config_path,
            toml::to_string_pretty(&config)
                .map_err(|e| e.to_string())?
                .as_bytes(),
        )?;
    }
    writeln!(
        out,
        "pipeline remove-job {job}; immutable configuration and evidence files retained"
    )
    .map_err(|e| e.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn examples_use_native_calendars_and_reject_malformed_declarations() {
        let weekly = include_str!("../../../configs/data-pipeline-deriv.example.toml");
        let pocket = include_str!("../../../configs/data-pipeline-pocket.example.toml");
        let always = include_str!("../../../configs/example.toml");
        for text in [weekly, pocket, always] {
            let core = Config::parse(text).unwrap();
            core.instruments[0]
                .session
                .as_ref()
                .unwrap()
                .calendar()
                .unwrap();
            assert_eq!(Config::parse(&core.canonical_toml()).unwrap(), core);
        }
        let malformed = weekly.replace("monday", "mon");
        assert!(Config::parse(&malformed).is_err());
        let unsupported_timezone =
            weekly.replace("timezone = \"UTC\"", "timezone = \"unsupported\"");
        assert!(Config::parse(&unsupported_timezone).is_err());
        let pipeline =
            PipelineConfig::parse(include_str!("../../../configs/data-pipeline.example.toml"))
                .unwrap();
        assert_eq!(pipeline.parallel_jobs, Some(4));
        assert_eq!(pipeline.parallel_transfers, Some(8));
        assert_eq!(pipeline.drive.request_timeout_seconds, 300);
        assert_eq!(pipeline.drive.chunk_bytes, 8 * 1024 * 1024);
    }

    #[test]
    fn canonical_session_changes_the_intent_binding() {
        let text = include_str!("../../../configs/data-pipeline-deriv.example.toml");
        let core = Config::parse(text).unwrap();
        let binding = binding_hash(&core);
        assert_eq!(
            binding,
            binding_hash(&Config::parse(&core.canonical_toml()).unwrap())
        );
        for (from, to) in [
            ("timezone = \"UTC\"", "timezone = \"America/New_York\""),
            ("monday", "sunday"),
            ("20:55:00", "20:50:00"),
            ("12-25", "12-26"),
            ("22:00:00", "21:00:00"),
        ] {
            let changed = Config::parse(&text.replace(from, to)).unwrap();
            assert_ne!(core.canonical_toml(), changed.canonical_toml(), "{from}");
            assert_ne!(binding, binding_hash(&changed), "{from}");
        }
        let mut changed = core.clone();
        changed.instruments[0].session = Some(binary_alpha_engine::session::Session::Always);
        assert_ne!(binding, binding_hash(&changed));
        changed.instruments[0].session = None;
        assert_ne!(binding, binding_hash(&changed));
        let mut budgets = core;
        budgets.history.as_mut().unwrap().max_pages = Some(1);
        budgets.history.as_mut().unwrap().max_elapsed_seconds = Some(1);
        assert_eq!(binding, binding_hash(&budgets));
    }
}

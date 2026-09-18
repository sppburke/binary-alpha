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

// WT-SESSIONS INTEGRATION POINT: these are registration syntax only. Once the session
// branch is merged Config::parse below owns validation and calendar semantics. Remove this
// shim and enable native_session_contract in new_instrument.rs. Never execute a weekly
// calendar after stripping it. Existing imported jobs may remain sessionless; add-job and
// empty-store bootstrap always require an explicit singular instruments.session.
#[derive(Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum SessionDeclaration {
    Always,
    Weekly {
        timezone: String,
        open: Boundary,
        close: Boundary,
        #[serde(default)]
        closed_dates: Vec<String>,
        #[serde(default)]
        early_closes: Vec<EarlyClose>,
    },
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Boundary {
    day: String,
    time: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct EarlyClose {
    date: String,
    time: String,
}

fn session_declaration(value: &toml::Value) -> Result<bool, String> {
    let session: SessionDeclaration = value
        .clone()
        .try_into()
        .map_err(|e| format!("session: {e}"))?;
    let SessionDeclaration::Weekly {
        timezone,
        open,
        close,
        closed_dates,
        early_closes,
    } = session
    else {
        return Ok(false);
    };
    if !["UTC", "America/New_York"].contains(&timezone.as_str()) {
        return Err("session.timezone: expected UTC or America/New_York".into());
    }
    let clock = |text: &str| -> Result<(), String> {
        if text.len() != 8 {
            return Err("session time must be HH:MM:SS".into());
        }
        time(&format!("2025-01-01T{text}Z")).map(|_| ())
    };
    for boundary in [&open, &close] {
        if ![
            "monday",
            "tuesday",
            "wednesday",
            "thursday",
            "friday",
            "saturday",
            "sunday",
        ]
        .contains(&boundary.day.as_str())
        {
            return Err("session day must be monday through sunday".into());
        }
        clock(&boundary.time)?;
    }
    if open.day == close.day && open.time == close.time {
        return Err("session open and close must differ".into());
    }
    let mut dates = std::collections::BTreeSet::new();
    for date in closed_dates
        .iter()
        .chain(early_closes.iter().map(|c| &c.date))
    {
        binary_alpha_engine::dataset::daily::day_bounds(date)?;
        if !dates.insert(date) || (timezone == "America/New_York" && date.as_str() < "2007-01-01") {
            return Err(
                "session dates must be distinct and New York overrides must be from 2007 onward"
                    .into(),
            );
        }
    }
    for close in early_closes {
        clock(&close.time)?;
    }
    Ok(true)
}

pub(crate) fn parse_core(text: &str, execution: bool) -> Result<Config, String> {
    if let Ok(config) = Config::parse(text) {
        crate::check_config_capabilities(&config)?;
        return Ok(config);
    }
    let mut document: toml::Value = toml::from_str(text).map_err(|e| format!("job config: {e}"))?;
    if let Some(instruments) = document
        .get_mut("instruments")
        .and_then(toml::Value::as_array_mut)
    {
        for instrument in instruments {
            if let Some(session) = instrument.as_table_mut().and_then(|t| t.remove("session")) {
                let weekly = session_declaration(&session)?;
                if execution && weekly {
                    return Err("session: weekly acquisition requires the wt-sessions calendar integration; registration is preserved".into());
                }
            }
        }
    }
    let config = Config::parse(&toml::to_string(&document).map_err(|e| e.to_string())?)
        .map_err(|e| e.to_string())?;
    crate::check_config_capabilities(&config)?;
    Ok(config)
}

pub(crate) fn load_core(path: &Path, execution: bool) -> Result<Config, String> {
    parse_core(
        &fs::read_to_string(path).map_err(|e| e.to_string())?,
        execution,
    )
}

pub(crate) fn session_binding(path: &Path, required: bool) -> Result<Option<String>, String> {
    let text = fs::read_to_string(path).map_err(|e| e.to_string())?;
    let document: toml::Value = toml::from_str(&text).map_err(|e| e.to_string())?;
    let sessions: Vec<_> = document
        .get("instruments")
        .and_then(toml::Value::as_array)
        .into_iter()
        .flatten()
        .map(|i| i.get("session"))
        .collect();
    if required && (sessions.len() != 1 || sessions[0].is_none()) {
        return Err(
            "job requires an explicit [instruments.session] table; no calendar is inferred".into(),
        );
    }
    if sessions.iter().all(|s| s.is_none()) {
        return Ok(None);
    }
    Ok(Some(sha256_hex(&json_bytes(&sessions)?)))
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
    let session = instrument
        .get("session")
        .cloned()
        .ok_or("add-job requires an explicit [instruments.session] table or --session FILE")?;
    session_declaration(&session)?;
    let mut core = parse_core(
        &toml::to_string(&document).map_err(|e| e.to_string())?,
        false,
    )?;
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
        let existing = load_core(&layout.base.join(&job.config), false)?;
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
    let request_hash = sha256_hex(&json_bytes(&(
        core.canonical_toml(),
        &session,
        options.price_scale,
    ))?);
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
    let mut output: toml::Value =
        toml::from_str(&core.canonical_toml()).map_err(|e| e.to_string())?;
    output["instruments"]
        .as_array_mut()
        .expect("core instruments")[0]
        .as_table_mut()
        .expect("instrument")
        .insert("session".into(), session);
    let output = toml::to_string_pretty(&output).map_err(|e| e.to_string())?;
    parse_core(&output, false)?;
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
    fn examples_use_explicit_calendar_shape_and_weekly_execution_waits_for_integration() {
        let weekly = include_str!("../../../configs/data-pipeline-deriv.example.toml");
        let always = include_str!("../../../configs/data-pipeline-pocket.example.toml");
        assert!(parse_core(weekly, false).is_ok());
        assert!(parse_core(always, true).is_ok());
        // Once the native calendar merges, the same example must parse through that owner.
        if Config::parse(weekly).is_err() {
            assert!(
                parse_core(weekly, true)
                    .unwrap_err()
                    .contains("wt-sessions")
            );
        }
        let malformed = weekly.replace("sunday", "sun");
        assert!(parse_core(&malformed, false).is_err());
        let missing_timezone = weekly.replace(
            "timezone = \"America/New_York\"",
            "timezone = \"unsupported\"",
        );
        assert!(parse_core(&missing_timezone, false).is_err());
        let pipeline =
            PipelineConfig::parse(include_str!("../../../configs/data-pipeline.example.toml"))
                .unwrap();
        assert_eq!(pipeline.parallel_jobs, Some(4));
        assert_eq!(pipeline.parallel_transfers, Some(8));
        assert_eq!(pipeline.drive.request_timeout_seconds, 300);
        assert_eq!(pipeline.drive.chunk_bytes, 8 * 1024 * 1024);
    }
}

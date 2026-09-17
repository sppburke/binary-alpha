//! Offline migration. Payloads and occurrence indexes spill to the job workspace; only one
//! UTC day is assembled for a codec. Immutable source objects are never modified.
use super::*;
use crate::daily::{
    self, LosslessRow, PageDisposition, PageOccurrence, PageOrderKind, ReceiptState,
};
use crate::lineage::receipt_time;
use crate::{archive, import, lineage};
use binary_alpha_engine::dataset::daily::{
    DAY_MICROS, DayFamily, DayInventoryEntry, DayState, Layout as DailyLayout, UnresolvedInterval,
    day_bounds,
};
use binary_alpha_engine::dataset::{
    ObjectRole, PriceRepresentation, generation_id_with_layout, object_key,
};
use binary_alpha_engine::market::InstrumentId;
use serde_json::{Value, json};
use std::collections::BTreeMap;
use std::io::{BufRead, BufReader, Read, Seek, SeekFrom};

fn err(e: impl std::fmt::Display) -> String {
    e.to_string()
}
fn date(t: i64) -> String {
    time_text(t)[..10].to_string()
}
fn save(path: &Path, v: &impl Serialize) -> Result<(), String> {
    write_atomic(path, &json_bytes(v)?)
}
fn get<T: for<'de> Deserialize<'de>>(path: &Path) -> Result<T, String> {
    read_json(path)?.ok_or_else(|| format!("missing {}", path.display()))
}
fn mkdir(path: &Path) -> Result<(), String> {
    fs::create_dir_all(path).map_err(err)
}
fn entries(path: &Path) -> Result<Vec<PathBuf>, String> {
    if !path.exists() {
        return Ok(vec![]);
    }
    let mut paths = fs::read_dir(path)
        .map_err(err)?
        .map(|e| e.map(|e| e.path()).map_err(err))
        .collect::<Result<Vec<_>, _>>()?;
    paths.sort();
    Ok(paths)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ByteRef {
    key: String,
    offset: u64,
    bytes: u64,
    sha256: String,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
struct Alias {
    label: String,
    acquisition_id: String,
    ordinal: u64,
    source: ByteRef,
    #[serde(default)]
    checkpoint: Option<ByteRef>,
    #[serde(default)]
    checkpoint_ordinal: Option<u64>,
    #[serde(default)]
    raw_suffix: Vec<u8>,
    #[serde(default)]
    checkpoint_suffix: Vec<u8>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
struct ImportFiles {
    acquisition_id: String,
    raw: ObjectRecord,
    checkpoint: ObjectRecord,
    lines: u64,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
struct State {
    phase: String,
    binding: String,
    dataset: String,
    stream: String,
    newest: String,
    old_stream: Option<String>,
    generations: Vec<String>,
    aliases: String,
    imports: Vec<ImportFiles>,
    occurrences: u64,
    peak_day_rows: u64,
    peak_day_payload_bytes: u64,
    record: Option<String>,
}
#[derive(Default)]
struct Sources {
    datasets: Vec<GenerationManifest>,
    streams: Vec<StreamManifest>,
}

/// No Drive session or broker adapter is constructed by this command.
pub fn migrate(
    config_path: &Path,
    selected: Option<&str>,
    out: &mut dyn Write,
) -> Result<(), String> {
    migrate_with(config_path, selected, &|_| Ok(()), out)
}

fn inventory(local: &Store, bound: &Bound, access: Access<'_>) -> Result<Sources, String> {
    let history = bound.core.history.as_ref().unwrap();
    if history.role != DatasetRole::Development {
        return Err("migration requires development role".into());
    }
    let instrument = format!("{}:{}", history.broker, bound.symbol);
    let mut result = Sources::default();
    for generation in local.list_manifests()? {
        access.lookup(&generation)?;
        // Manifests establish identity; permits precede every referenced object read.
        let mut bytes = Vec::new();
        local.read_to(&manifest_key(&generation), None, &mut bytes)?;
        if let Some(kind) = verify::manifest_kind(&bytes)? {
            if kind != binary_alpha_engine::stream::STREAM_MANIFEST_KIND {
                continue;
            }
            let m = StreamManifest::from_json(&bytes)?;
            if m.instrument == instrument && m.role == history.role && m.layout.is_none() {
                access.permit(Some(m.role), &m.source_generation)?;
                result.streams.push(m);
            }
        } else {
            let m = GenerationManifest::from_json(&bytes)?;
            if m.instrument == instrument && m.role == history.role && m.layout.is_none() {
                access.permit(Some(m.role), &m.generation)?;
                if !matches!(
                    m.source_kind,
                    SourceKind::BrokerHistory
                        | SourceKind::TickParquetDaily
                        | SourceKind::BarParquet
                ) {
                    return Err("migration: unsupported v1 source kind".into());
                }
                result.datasets.push(m);
            }
        }
    }
    if result.datasets.is_empty() {
        return Err(format!("migration: no v1 dataset for {instrument}"));
    }
    Ok(result)
}
fn object_path(layout: &Layout, object: &ObjectRecord) -> Result<PathBuf, String> {
    let path = layout.store.join(&object.key);
    let id = store::identify(&path)?;
    if id.bytes != object.bytes || id.sha256 != object.sha256 {
        return Err(format!(
            "{}: source object SHA-256/bytes mismatch",
            object.path
        ));
    }
    Ok(path)
}
fn slice(layout: &Layout, source: &ByteRef) -> Result<Vec<u8>, String> {
    let mut f = File::open(layout.store.join(&source.key)).map_err(err)?;
    f.seek(SeekFrom::Start(source.offset)).map_err(err)?;
    let mut bytes = vec![0; usize::try_from(source.bytes).map_err(err)?];
    f.read_exact(&mut bytes).map_err(err)?;
    if sha256_hex(&bytes) != source.sha256 {
        return Err(format!(
            "page bytes mismatch: {} offset {} expected {}",
            source.key, source.offset, source.sha256
        ));
    }
    Ok(bytes)
}
fn retain_file(
    local: &Store,
    path: &Path,
    logical: &str,
    role: ObjectRole,
) -> Result<ObjectRecord, String> {
    let id = store::identify(path)?;
    let object = import::record(role, logical, &id);
    local.put_new(&object.key, path, &id)?;
    Ok(object)
}
fn retain_json(
    local: &Store,
    work: &Path,
    logical: &str,
    value: &impl Serialize,
) -> Result<ObjectRecord, String> {
    let path = work.join("metadata.tmp.json");
    save(&path, value)?;
    retain_file(local, &path, logical, ObjectRole::Provenance)
}

// Deserialize the unbounded page/request arrays one entry at a time, retaining only the small
// enclosing document. Nested coverage objects use the same visitor.
fn document(
    path: &Path,
    sink: &mut dyn FnMut(&str, Value) -> Result<(), String>,
) -> Result<Value, String> {
    use serde::de::{DeserializeSeed, MapAccess, SeqAccess, Visitor};
    struct Doc<'a>(&'a mut dyn FnMut(&str, Value) -> Result<(), String>);
    struct Array<'a> {
        key: String,
        sink: &'a mut dyn FnMut(&str, Value) -> Result<(), String>,
    }
    impl<'de> DeserializeSeed<'de> for Array<'_> {
        type Value = ();
        fn deserialize<D: serde::Deserializer<'de>>(self, d: D) -> Result<(), D::Error> {
            d.deserialize_seq(self)
        }
    }
    impl<'de> Visitor<'de> for Array<'_> {
        type Value = ();
        fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
            f.write_str("page array")
        }
        fn visit_seq<A: SeqAccess<'de>>(self, mut a: A) -> Result<(), A::Error> {
            while let Some(v) = a.next_element::<Value>()? {
                (self.sink)(&self.key, v).map_err(serde::de::Error::custom)?;
            }
            Ok(())
        }
    }
    impl<'de> DeserializeSeed<'de> for Doc<'_> {
        type Value = Value;
        fn deserialize<D: serde::Deserializer<'de>>(self, d: D) -> Result<Value, D::Error> {
            d.deserialize_map(self)
        }
    }
    impl<'de> Visitor<'de> for Doc<'_> {
        type Value = Value;
        fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
            f.write_str("object")
        }
        fn visit_map<A: MapAccess<'de>>(self, mut a: A) -> Result<Value, A::Error> {
            let mut map = serde_json::Map::new();
            while let Some(key) = a.next_key::<String>()? {
                if matches!(key.as_str(), "pages" | "requests") {
                    a.next_value_seed(Array { key, sink: self.0 })?;
                } else if key == "coverage" || key == "progress" {
                    // Receipts can carry null coverage.
                    if key == "coverage" {
                        let v = a.next_value::<Option<CoverageDoc>>()?;
                        if let Some(v) = v {
                            map.insert(key, v.0);
                        }
                    } else {
                        map.insert(key, a.next_value_seed(Doc(self.0))?);
                    }
                } else {
                    map.insert(key, a.next_value()?);
                }
            }
            Ok(Value::Object(map))
        }
    }
    // Receipt coverage is duplicate provenance. Discard its page array with IgnoredAny,
    // rather than allocating it; published coverage is indexed independently.
    struct CoverageDoc(Value);
    impl<'de> Deserialize<'de> for CoverageDoc {
        fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
            struct V;
            impl<'de> Visitor<'de> for V {
                type Value = CoverageDoc;
                fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                    f.write_str("coverage")
                }
                fn visit_map<A: MapAccess<'de>>(self, mut a: A) -> Result<Self::Value, A::Error> {
                    let mut map = serde_json::Map::new();
                    while let Some(k) = a.next_key::<String>()? {
                        if k == "pages" {
                            a.next_value::<serde::de::IgnoredAny>()?;
                        } else {
                            map.insert(k, a.next_value()?);
                        }
                    }
                    Ok(CoverageDoc(Value::Object(map)))
                }
            }
            d.deserialize_map(V)
        }
    }
    let mut des =
        serde_json::Deserializer::from_reader(BufReader::new(File::open(path).map_err(err)?));
    let v = Doc(sink)
        .deserialize(&mut des)
        .map_err(|e| format!("{}: {e}", path.display()))?;
    des.end().map_err(err)?;
    Ok(v)
}
fn append(path: &Path, value: &impl Serialize) -> Result<(), String> {
    let mut f = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .map_err(err)?;
    serde_json::to_writer(&mut f, value).map_err(err)?;
    f.write_all(b"\n").map_err(err)
}
fn lines(
    path: &Path,
    mut sink: impl FnMut(u64, u64, Vec<u8>, Vec<u8>) -> Result<(), String>,
) -> Result<u64, String> {
    let mut input = BufReader::new(File::open(path).map_err(err)?);
    let (mut ordinal, mut offset) = (0, 0);
    loop {
        let mut bytes = Vec::new();
        let n = input.read_until(b'\n', &mut bytes).map_err(err)?;
        if n == 0 {
            break;
        }
        let mut suffix = Vec::new();
        if bytes.last() == Some(&b'\n') {
            bytes.pop();
            suffix.push(b'\n');
            if bytes.last() == Some(&b'\r') {
                bytes.pop();
                suffix.insert(0, b'\r');
            }
        }
        sink(ordinal, offset, bytes, suffix)?;
        ordinal += 1;
        offset += n as u64;
    }
    Ok(ordinal)
}
fn json_lines<T: for<'de> Deserialize<'de>>(
    path: &Path,
    mut sink: impl FnMut(T) -> Result<(), String>,
) -> Result<(), String> {
    if !path.exists() {
        return Ok(());
    }
    lines(path, |_, _, b, _| {
        sink(serde_json::from_slice(&b).map_err(err)?)
    })?;
    Ok(())
}

#[derive(Clone, Serialize, Deserialize)]
struct IndexedPage {
    ordinal: u64,
    /// Equal request fingerprints are counted within each receipt, independently of its
    /// absolute request position (a replayed subset can start at a different ordinal).
    #[serde(default)]
    request_occurrence: Option<u64>,
    legacy_identity: String,
    label: String,
    source: ByteRef,
    page: fetch::PageCoverage,
    acquisition: String,
}
struct Spool<'a> {
    layout: &'a Layout,
    work: PathBuf,
    aliases: PathBuf,
    offset_s: i64,
    occurrences: u64,
}
impl Spool<'_> {
    fn add(
        &mut self,
        mut page: PageOccurrence,
        alias: Alias,
        identity: Option<String>,
    ) -> Result<(), String> {
        let identity_path = identity.map(|i| self.work.join("identities").join(i));
        if let Some(path) = &identity_path
            && let Some(existing) = read_json::<(String, u64)>(path)?
        {
            if page.disposition == PageDisposition::Diagnostic {
                let path = self
                    .work
                    .join("days")
                    .join(date(page.partition_time()?))
                    .join(sha256_hex(
                        format!("{}\n{}", existing.0, existing.1).as_bytes(),
                    ));
                let mut original: PageOccurrence = get(&path)?;
                original.disposition = PageDisposition::Diagnostic;
                save(&path, &original)?;
            }
            let mut alias = alias;
            alias.acquisition_id = existing.0;
            alias.ordinal = existing.1;
            append(&self.aliases, &alias)?;
            return Ok(());
        }
        page.payload = slice(self.layout, &alias.source)?;
        let day = date(page.partition_time()?);
        let name = sha256_hex(format!("{}\n{}", page.acquisition_id, page.ordinal).as_bytes());
        let dir = self.work.join("days").join(day);
        mkdir(&dir)?;
        let path = dir.join(name);
        if path.exists() {
            return Err("migration: occurrence identifier reused".into());
        }
        save(&path, &page)?;
        if let Some(path) = identity_path {
            save(&path, &(page.acquisition_id.clone(), page.ordinal))?;
        }
        append(&self.aliases, &alias)?;
        self.occurrences += 1;
        Ok(())
    }
    fn history(
        &mut self,
        indexed: &IndexedPage,
        intent: Option<String>,
        ordinal: u64,
        diagnostic: bool,
    ) -> Result<(), String> {
        let p = &indexed.page;
        let (rows, first, last) =
            payload_bounds(&slice(self.layout, &indexed.source)?, self.offset_s)?;
        let first = first.as_deref().map(time).transpose()?;
        let last = last.as_deref().map(time).transpose()?;
        if (rows, first, last)
            != (
                p.rows,
                p.first.as_deref().map(time).transpose()?,
                p.last.as_deref().map(time).transpose()?,
            )
        {
            return Err(format!(
                "{}: payload rows/event bounds mismatch",
                indexed.label
            ));
        }
        let fingerprint = sha256_hex(&json_bytes(&(
            p.anchor.as_ref(),
            &p.sha256,
            p.receipt_time.as_ref(),
        ))?);
        let mut pending_match = None;
        if indexed.label.starts_with("pending:") && p.receipt_time.is_some() {
            let matches = entries(&self.work.join("requests-exact").join(&fingerprint))?;
            for path in matches {
                let candidate: String = get(&path)?;
                let existing: (String, u64) = get(&self.work.join("identities").join(&candidate))?;
                let partition = p
                    .last
                    .as_deref()
                    .map(time)
                    .transpose()?
                    .or(anchor(p.anchor.as_deref(), self.offset_s)?)
                    .or(p.receipt_time.as_deref().map(receipt_time).transpose()?)
                    .ok_or("unresolved pending day")?;
                let row: PageOccurrence =
                    get(&self
                        .work
                        .join("days")
                        .join(date(partition))
                        .join(sha256_hex(
                            format!("{}\n{}", existing.0, existing.1).as_bytes(),
                        )))?;
                if row.intent == intent {
                    if pending_match.is_some() {
                        return Err("pending log matches multiple receipt occurrences".into());
                    }
                    pending_match = Some(candidate);
                }
            }
        }
        let identity = if let Some(identity) = pending_match {
            identity
        } else if let Some(intent) = &intent {
            sha256_hex(&json_bytes(&(
                intent,
                indexed.request_occurrence.unwrap_or(ordinal),
                &fingerprint,
            ))?)
        } else if p.receipt_time.is_none() {
            // Equal anchor/payload alone is not evidence of the same request. Preserve the
            // ordered legacy prefix or bundle slice identity, including carried occurrences.
            sha256_hex(indexed.legacy_identity.as_bytes())
        } else {
            let folder = self.work.join("requests-exact").join(&fingerprint);
            let candidates = entries(&folder)?;
            if candidates.len() > 1 {
                return Err(format!(
                    "{}: unresolved occurrence identity: multiple requests match legacy coverage",
                    indexed.label
                ));
            }
            if let Some(path) = candidates.first() {
                get::<String>(path)?
            } else {
                sha256_hex(indexed.legacy_identity.as_bytes())
            }
        };
        let has_intent = intent.is_some();
        let receipt_time_utc = p.receipt_time.as_deref().map(receipt_time).transpose()?;
        let page = PageOccurrence {
            acquisition_id: indexed.acquisition.clone(),
            intent,
            ordinal,
            checkpoint_ordinal: None,
            order_kind: PageOrderKind::RequestOrder,
            payload_sha256: p.sha256.clone(),
            payload: vec![],
            request_token: p.anchor.clone(),
            request_anchor_utc: anchor(p.anchor.as_deref(), self.offset_s)?,
            receipt_time_utc,
            receipt_state: if receipt_time_utc.is_some() {
                ReceiptState::Recorded
            } else {
                ReceiptState::AbsentInLegacyRecord
            },
            first_event_time: first,
            last_event_time: last,
            rows: p.rows,
            checkpoint: None,
            disposition: if diagnostic {
                PageDisposition::Diagnostic
            } else {
                PageDisposition::Indexed
            },
        };
        self.add(
            page,
            Alias {
                label: indexed.label.clone(),
                acquisition_id: indexed.acquisition.clone(),
                ordinal,
                source: indexed.source.clone(),
                checkpoint: None,
                checkpoint_ordinal: None,
                raw_suffix: vec![],
                checkpoint_suffix: vec![],
            },
            Some(identity.clone()),
        )?;
        if has_intent {
            let dir = self.work.join("requests-exact").join(fingerprint);
            mkdir(&dir)?;
            save(&dir.join(&identity), &identity)?;
        }
        Ok(())
    }
}
fn token(value: &Value) -> Option<String> {
    match value {
        Value::String(s) => Some(s.clone()),
        Value::Number(n) => Some(n.to_string()),
        _ => None,
    }
}
fn anchor(value: Option<&str>, offset: i64) -> Result<Option<i64>, String> {
    use binary_alpha_engine::execution::Decimal;
    value
        .filter(|s| *s != "latest")
        .map(|s| {
            let d = Decimal::parse(s)?;
            if d.scale() > 6 {
                return Err("request anchor has sub-microsecond precision".into());
            }
            i64::try_from(
                d.checked_sub(Decimal::parse(&offset.to_string())?)?
                    .rescale(6)?
                    .coefficient(),
            )
            .map_err(err)
        })
        .transpose()
}
fn payload_bounds(
    raw: &[u8],
    offset: i64,
) -> Result<(u64, Option<String>, Option<String>), String> {
    let v: Value = serde_json::from_slice(raw).map_err(|e| format!("provider page JSON: {e}"))?;
    let times: Vec<i64> = if let Some(times) = v.pointer("/history/times").and_then(Value::as_array)
    {
        times
            .iter()
            .map(|v| anchor(token(v).as_deref(), 0)?.ok_or("missing tick time".into()))
            .collect::<Result<_, String>>()?
    } else if let Some(rows) = v.get("data").and_then(Value::as_array) {
        rows.iter()
            .map(|r| anchor(token(&r["time"]).as_deref(), offset)?.ok_or("missing bar time".into()))
            .collect::<Result<_, String>>()?
    } else {
        return Err("provider page lacks history.times or data".into());
    };
    Ok((
        times.len() as u64,
        times.iter().min().copied().map(time_text),
        times.iter().max().copied().map(time_text),
    ))
}

fn index_coverage(
    layout: &Layout,
    work: &Path,
    sources: &Sources,
    identity: &str,
) -> Result<Vec<(String, Value)>, String> {
    mkdir(&work.join("payloads"))?;
    let mut coverage = Vec::new();
    for m in &sources.datasets {
        if m.source_kind != SourceKind::BrokerHistory {
            continue;
        }
        let object = m
            .objects
            .iter()
            .find(|o| o.path == fetch::COVERAGE_PATH)
            .ok_or("missing v1 coverage")?;
        let path = object_path(layout, object)?;
        let mut ordinal = 0;
        let mut prefix = Sha256::new();
        let mut offsets = BTreeMap::<String, u64>::new();
        let header = document(&path, &mut |kind, p| {
            if kind != "pages" {
                return Ok(());
            }
            let page: fetch::PageCoverage = serde_json::from_value(p).map_err(err)?;
            let obj = m
                .objects
                .iter()
                .find(|o| o.path == page.path)
                .ok_or_else(|| format!("missing indexed source {}", page.path))?;
            if page.path.ends_with("/pages.bin") && page.offset.is_none() {
                return Err(format!("{}: bundle page lacks offset", page.path));
            }
            if let Some(offset) = page.offset {
                let next = offsets.entry(obj.path.clone()).or_default();
                if offset != *next {
                    return Err(format!(
                        "{}: bundle index gap/overlap at {offset}, expected {}",
                        obj.path, *next
                    ));
                }
                *next = offset
                    .checked_add(page.bytes)
                    .ok_or("bundle offset overflow")?;
            }
            let source = ByteRef {
                key: obj.key.clone(),
                offset: page.offset.unwrap_or(0),
                bytes: page.bytes,
                sha256: page.sha256.clone(),
            };
            slice(layout, &source)?;
            // Legacy single objects carry no acquisition ID. Descendants retain their
            // ordered coverage prefix verbatim. Bind the entire physical-source/metadata
            // prefix, not just this payload, so repeated requests within it stay distinct.
            prefix.update(json_bytes(&(
                &source,
                &page.anchor,
                &page.receipt_time,
                page.rows,
                &page.first,
                &page.last,
            ))?);
            let legacy_identity = if page.path.ends_with(".json") {
                format!(
                    "legacy-prefix-{}",
                    binary_alpha_engine::hex(&prefix.clone().finalize())
                )
            } else {
                format!(
                    "{}:{}:{}",
                    if page.path == fetch::BUNDLE_PATH {
                        m.generation.as_str()
                    } else {
                        page.path.split('/').nth(1).unwrap_or(&m.generation)
                    },
                    page.sha256,
                    page.offset.unwrap_or(ordinal)
                )
            };
            save(&work.join("payloads").join(&page.sha256), &source)?;
            append(
                &work.join("coverage-pages.jsonl"),
                &IndexedPage {
                    ordinal,
                    request_occurrence: None,
                    legacy_identity,
                    label: format!("coverage:{}/{}", m.generation, ordinal),
                    source,
                    page,
                    acquisition: format!("legacy-coverage-{}", object.sha256),
                },
            )?;
            ordinal += 1;
            Ok(())
        })?;
        if header["source_identity"] != identity
            || header["broker"] != m.broker.as_str()
            || header["provider_symbol"] != m.provider_symbol.as_str()
            || header["rows"].as_u64() != Some(m.row_count)
        {
            return Err(format!(
                "{}: coverage source identity/rows mismatch",
                m.generation
            ));
        }
        for obj in m
            .objects
            .iter()
            .filter(|o| o.path.starts_with("raw/") && o.path.ends_with("/pages.bin"))
        {
            let end = offsets.get(&obj.path).copied().unwrap_or(0);
            if end != obj.bytes {
                return Err(format!(
                    "{}: bundle has unindexed bytes: indexed {end}, stored {}",
                    obj.path, obj.bytes
                ));
            }
        }
        let mut header = header;
        header["page_count"] = json!(ordinal);
        coverage.push((m.generation.clone(), header));
    }
    Ok(coverage)
}
// Null coverage is normal for requests that produced no dataset. The immutable intent
// still binds those requests to a source; a filename or job name is not ownership evidence.
fn receipt_matches(
    layout: &Layout,
    header: &Value,
    source: &GenerationManifest,
    identity: &str,
    has_requests: bool,
) -> Result<bool, String> {
    // Migration retains Pending snapshots beside receipts. A valid job id can contain
    // `-receipt-`, so the filename alone cannot distinguish them. Never let a sibling's
    // newly published diagnostic snapshot become a schedule-dependent source receipt.
    if header.get("progress").is_some() {
        return Ok(false);
    }
    if let Some(cov) = header.get("coverage").filter(|v| !v.is_null()) {
        if cov["broker"] != source.broker.as_str()
            || cov["provider_symbol"] != source.provider_symbol.as_str()
        {
            return Ok(false);
        }
        if cov["source_identity"] != identity {
            return Err("receipt source identity mismatch".into());
        }
        return Ok(true);
    }
    let Some(intent) = header["intent"].as_str() else {
        return if has_requests {
            Err("unresolved receipt ownership: missing intent".into())
        } else {
            Ok(false)
        };
    };
    let intent: Intent = get(&layout.state.join("records").join(intent))
        .map_err(|e| format!("unresolved receipt ownership: {e}"))?;
    if intent.seeds.is_empty() {
        return if has_requests {
            Err("unresolved receipt ownership: intent has no source binding".into())
        } else {
            Ok(false)
        };
    }
    if intent.seeds.iter().any(|seed| {
        seed.provider_symbol == source.provider_symbol && seed.source_identity != identity
    }) {
        return Err("unresolved receipt source identity mismatch".into());
    }
    let matches = intent.seeds.iter().any(|seed| {
        seed.provider_symbol == source.provider_symbol && seed.source_identity == identity
    });
    if matches && intent.seeds.len() != 1 && has_requests {
        return Err("unresolved receipt ownership: intent binds multiple sources".into());
    }
    Ok(matches)
}
fn add_receipts(
    spool: &mut Spool<'_>,
    sources: &Sources,
    identity: &str,
    records: &mut Vec<Value>,
) -> Result<(), String> {
    let first = &sources.datasets[0];
    let mut receipts = Vec::new();
    for path in entries(&spool.layout.state.join("records"))? {
        let name = path.file_name().unwrap().to_string_lossy();
        if !name.contains("-receipt-") || !name.ends_with(".json") {
            continue;
        }
        let mut count = 0;
        document(&path, &mut |kind, _| {
            if kind == "requests" {
                count += 1;
            }
            Ok(())
        })?;
        receipts.push((count, path));
    }
    receipts.sort();
    for (count, path) in receipts {
        let name = path.file_name().unwrap().to_string_lossy().to_string();
        if !name.contains("-receipt-") || !name.ends_with(".json") {
            continue;
        }
        let header = document(&path, &mut |_, _| Ok(()))?;
        if !receipt_matches(spool.layout, &header, first, identity, count != 0)
            .map_err(|e| format!("{name}: {e}"))?
        {
            continue;
        }
        let record_id = store::identify(&path)?;
        records.push(json!({"name":name,"sha256":record_id.sha256,"bytes":record_id.bytes,"kind":"operation_receipt"}));
        let intent = header["intent"]
            .as_str()
            .ok_or("receipt has no intent")?
            .to_string();
        let intent_path = spool.layout.state.join("records").join(&intent);
        let intent_id = store::identify(&intent_path)?;
        if !records.iter().any(|r| r["name"] == intent) {
            records.push(json!({"kind":"intent","name":intent,"sha256":intent_id.sha256,"bytes":intent_id.bytes}));
        }
        if let Some(acquisition) = header["acquisition_id"].as_str() {
            lineage::record_name(&format!("records/{acquisition}"))?;
            let path = spool.layout.state.join("records").join(acquisition);
            let record: Value = get(&path)?;
            if record["intent"] != intent {
                return Err("receipt acquisition does not bind its intent".into());
            }
            let id = store::identify(&path)?;
            if !records.iter().any(|r| r["name"] == acquisition) {
                records.push(json!({"kind":"acquisition","name":acquisition,"sha256":id.sha256,"bytes":id.bytes}));
            }
        }
        let mut ordinal = 0;
        document(&path, &mut |kind, value| {
            if kind != "requests" {
                return Ok(());
            }
            let request: PageReceipt = serde_json::from_value(value).map_err(err)?;
            let fingerprint = sha256_hex(&json_bytes(&(
                &request.anchor,
                &request.sha256,
                &request.receipt_time,
            ))?);
            let request_occurrence =
                next_count(&spool.work.join("receipt-counts").join(&name), &fingerprint)?;
            let source = read_json::<ByteRef>(&spool.work.join("payloads").join(&request.sha256))?
                .unwrap_or(ByteRef {
                    key: object_key(&request.sha256),
                    offset: 0,
                    bytes: request.bytes,
                    sha256: request.sha256.clone(),
                });
            if source.bytes != request.bytes {
                return Err(format!("{name} request {ordinal}: byte count mismatch"));
            }
            let raw = slice(spool.layout, &source)?;
            let (rows, first, last) = payload_bounds(&raw, spool.offset_s)?;
            if rows != request.rows {
                return Err(format!("{name} request {ordinal}: rows mismatch"));
            }
            let page = fetch::PageCoverage {
                occurrence: None,
                path: String::new(),
                offset: None,
                sha256: request.sha256,
                bytes: request.bytes,
                anchor: request.anchor,
                rows,
                first,
                last,
                receipt_time: Some(request.receipt_time),
            };
            spool.history(
                &IndexedPage {
                    ordinal,
                    request_occurrence: Some(request_occurrence),
                    legacy_identity: String::new(),
                    label: format!("receipt:{name}/{ordinal}"),
                    source,
                    page,
                    acquisition: name.clone(),
                },
                Some(intent.clone()),
                ordinal,
                false,
            )?;
            ordinal += 1;
            Ok(())
        })?;
    }
    Ok(())
}
#[derive(Serialize, Deserialize)]
struct Checkpoint {
    ordinal: u64,
    source: ByteRef,
    suffix: Vec<u8>,
}
fn next_count(dir: &Path, hash: &str) -> Result<u64, String> {
    mkdir(dir)?;
    let path = dir.join(hash);
    let count = read_json::<u64>(&path)?.unwrap_or(0);
    save(&path, &(count + 1))?;
    Ok(count)
}
fn import_pair(m: &GenerationManifest) -> Result<Option<(&ObjectRecord, &ObjectRecord)>, String> {
    match (
        m.objects.iter().find(|o| o.path == "raw_pages.ndjson"),
        m.objects.iter().find(|o| o.path == "checkpoint.ndjson"),
    ) {
        (None, None) => Ok(None),
        (Some(raw), Some(checkpoint)) => Ok(Some((raw, checkpoint))),
        (Some(_), None) => {
            Err("raw_pages.ndjson has no checkpoint.ndjson; unresolved import".into())
        }
        (None, Some(_)) => {
            Err("checkpoint.ndjson has no raw_pages.ndjson; unresolved import".into())
        }
    }
}
fn add_import(
    spool: &mut Spool<'_>,
    m: &GenerationManifest,
) -> Result<Option<ImportFiles>, String> {
    let Some((raw, checkpoint)) = import_pair(m)? else {
        return Ok(None);
    };
    let raw_path = object_path(spool.layout, raw)?;
    let checkpoint_path = object_path(spool.layout, checkpoint)?;
    let recorded_offset = import_offset(spool.layout, m)?;
    let base = spool.work.join(format!("import-{}", raw.sha256));
    mkdir(&base.join("checkpoints"))?;
    let count = lines(&checkpoint_path, |ordinal, offset, bytes, suffix| {
        let v: Value = serde_json::from_slice(&bytes).map_err(err)?;
        let hash = v["payload_sha256"]
            .as_str()
            .ok_or("checkpoint line lacks payload_sha256")?;
        if hash.len() != 64 || !hash.bytes().all(|b| b.is_ascii_hexdigit()) {
            return Err("invalid checkpoint payload_sha256".into());
        }
        let n = next_count(&base.join("cp-counts"), hash)?;
        save(
            &base.join("checkpoints").join(format!("{hash}-{n}")),
            &Checkpoint {
                ordinal,
                source: ByteRef {
                    key: checkpoint.key.clone(),
                    offset,
                    bytes: bytes.len() as u64,
                    sha256: sha256_hex(&bytes),
                },
                suffix,
            },
        )
    })?;
    let raw_count = lines(&raw_path, |ordinal, offset, bytes, suffix| {
        let hash = sha256_hex(&bytes);
        let n = next_count(&base.join("raw-counts"), &hash)?;
        let cp: Checkpoint =
            get(&base.join("checkpoints").join(format!("{hash}-{n}"))).map_err(|e| {
                format!("raw_pages.ndjson line {ordinal}: checkpoint match failed: {e}")
            })?;
        let checkpoint_bytes = slice(spool.layout, &cp.source)?;
        let v: Value = serde_json::from_slice(&checkpoint_bytes).map_err(err)?;
        let page = lineage::import_page(
            bytes.clone(),
            &raw.sha256,
            ordinal,
            Some((cp.ordinal, checkpoint_bytes)),
            recorded_offset,
        )?;
        if v["rows"]
            .as_u64()
            .is_some_and(|expected| expected != page.rows)
        {
            return Err(format!("checkpoint line {}: rows mismatch", cp.ordinal));
        }
        spool.add(
            page,
            Alias {
                label: format!("import:{}/{}", m.generation, ordinal),
                acquisition_id: raw.sha256.clone(),
                ordinal,
                source: ByteRef {
                    key: raw.key.clone(),
                    offset,
                    bytes: bytes.len() as u64,
                    sha256: hash,
                },
                checkpoint: Some(cp.source),
                checkpoint_ordinal: Some(cp.ordinal),
                raw_suffix: suffix,
                checkpoint_suffix: cp.suffix,
            },
            None,
        )
    })?;
    if raw_count != count {
        return Err(format!(
            "checkpoint.ndjson occurrence count {count} differs from raw count {raw_count}"
        ));
    }
    Ok(Some(ImportFiles {
        acquisition_id: raw.sha256.clone(),
        raw: raw.clone(),
        checkpoint: checkpoint.clone(),
        lines: count,
    }))
}

fn add_pending(
    spool: &mut Spool<'_>,
    sources: &Sources,
    records: &mut Vec<Value>,
    identity: &str,
) -> Result<(), String> {
    for dir in entries(&spool.layout.state)? {
        let path = dir.join("progress.json");
        if !path.is_file() {
            continue;
        }
        let header = document(&path, &mut |_, _| Ok(()))?;
        let Some(baseline) = header.pointer("/progress/baseline").and_then(Value::as_str) else {
            continue;
        };
        if !sources.datasets.iter().any(|m| m.generation == baseline) {
            continue;
        }
        let id = store::identify(&path)?;
        let intent = header["intent"]
            .as_str()
            .ok_or("pending header lacks intent")?
            .to_string();
        let intent_path = spool.layout.state.join("records").join(&intent);
        let intent_record: Intent = get(&intent_path)?;
        if !intent_record.seeds.iter().any(|seed| {
            seed.provider_symbol == sources.datasets[0].provider_symbol
                && seed.source_identity == identity
        }) {
            return Err(format!(
                "pending intent {intent}: source identity/instrument mismatch"
            ));
        }
        let intent_id = store::identify(&intent_path)?;
        if !records.iter().any(|r| r["name"] == intent) {
            records.push(json!({"kind":"intent","name":intent,"sha256":intent_id.sha256,"bytes":intent_id.bytes}));
        }
        let acquisition = format!("{intent}-{}", id.sha256);
        records.push(json!({"kind":"pending","path":path.strip_prefix(&spool.layout.state).map_err(err)?,"sha256":id.sha256,"bytes":id.bytes,"intent":intent,"disposition":"diagnostic","abandonment":"retained; migration does not resume or discard broker work"}));
        let mut ordinal = 0;
        let mut accept = |value: Value| -> Result<(), String> {
            let p: fetch::PageCoverage = serde_json::from_value(value).map_err(err)?;
            let source = ByteRef {
                key: object_key(&p.sha256),
                offset: 0,
                bytes: p.bytes,
                sha256: p.sha256.clone(),
            };
            spool.history(
                &IndexedPage {
                    ordinal,
                    request_occurrence: None,
                    legacy_identity: String::new(),
                    label: format!("pending:{acquisition}/{ordinal}"),
                    source,
                    page: p,
                    acquisition: acquisition.clone(),
                },
                Some(intent.clone()),
                ordinal,
                true,
            )?;
            ordinal += 1;
            Ok(())
        };
        document(&path, &mut |kind, v| {
            if kind == "pages" { accept(v) } else { Ok(()) }
        })?;
        let log = dir.join("progress.pages.jsonl");
        if log.exists() {
            lines(&log, |_, _, bytes, suffix| {
                if suffix.is_empty() {
                    return Err("pending progress log has an incomplete final append; retained unchanged, unresolved".into());
                }
                accept(serde_json::from_slice(&bytes).map_err(err)?)
            })?;
            let id = store::identify(&log)?;
            records.push(json!({"kind":"pending_log","path":log.strip_prefix(&spool.layout.state).map_err(err)?,"sha256":id.sha256,"bytes":id.bytes}));
        }
    }
    Ok(())
}
fn new_day(d: &str, family: DayFamily) -> DayInventoryEntry {
    DayInventoryEntry {
        date: d.into(),
        family,
        duration: None,
        offset: None,
        object: None,
        rows: 0,
        first_time: None,
        last_time: None,
        state: DayState::Unknown,
        reason: Some("source has no whole-day completeness claim".into()),
        unresolved: vec![],
    }
}
fn set_data(day: &mut DayInventoryEntry, data: &archive::DataSummary, object: &ObjectRecord) {
    day.object = Some(object.key.clone());
    day.rows = data.rows;
    day.first_time = data.first_event_micros.map(time_text);
    day.last_time = data.last_event_micros.map(time_text);
}
fn select_newest<'a>(
    sources: &'a Sources,
    coverage: &[(String, Value)],
    with_stream: bool,
) -> Result<&'a GenerationManifest, String> {
    let mut selected = None;
    let mut rank = None;
    for m in &sources.datasets {
        if with_stream
            && !sources
                .streams
                .iter()
                .any(|s| s.source_generation == m.generation)
        {
            continue;
        }
        let c = coverage
            .iter()
            .find(|(g, _)| g == &m.generation)
            .map(|(_, c)| c);
        let value = if let Some(c) = c {
            (
                true,
                c.pointer("/verified/end")
                    .and_then(Value::as_str)
                    .map(time)
                    .transpose()?
                    .unwrap_or(i64::MIN),
                std::cmp::Reverse(
                    c.pointer("/verified/start")
                        .and_then(Value::as_str)
                        .map(time)
                        .transpose()?
                        .unwrap_or(i64::MAX),
                ),
                c.pointer("/shortfall/reason").and_then(Value::as_str)
                    != Some(fetch::BUDGET_SHORTFALL),
                time(&m.coverage.last_event_time)?,
                m.row_count,
            )
        } else {
            (
                false,
                time(&m.coverage.last_event_time)?,
                std::cmp::Reverse(time(&m.coverage.first_event_time)?),
                true,
                time(&m.coverage.last_event_time)?,
                m.row_count,
            )
        };
        let value = (
            value,
            c.and_then(|c| c["page_count"].as_u64()).unwrap_or(0),
            sources
                .streams
                .iter()
                .any(|s| s.source_generation == m.generation),
        );
        if rank.as_ref().is_none_or(|old| value > *old) {
            rank = Some(value);
            selected = Some(m);
        }
    }
    selected.ok_or("no migration source".into())
}
fn offset_seconds(bound: &Bound) -> i64 {
    match bound
        .core
        .brokers
        .iter()
        .find(|b| b.id() == &bound.core.history.as_ref().unwrap().broker)
        .unwrap()
    {
        binary_alpha_engine::config::Broker::PocketOption(s) => {
            i64::from(s.server_offset_minutes) * 60
        }
        binary_alpha_engine::config::Broker::Deriv(_) => 0,
    }
}
fn source_lineage(
    layout: &Layout,
    sources: &Sources,
) -> Result<(Vec<Value>, BTreeMap<String, Value>), String> {
    use parquet::file::reader::{FileReader, SerializedFileReader};
    let mut objects = Vec::new();
    let mut metas = BTreeMap::new();
    for (generation, list) in sources
        .datasets
        .iter()
        .map(|m| (&m.generation, &m.objects))
        .chain(sources.streams.iter().map(|m| (&m.generation, &m.objects)))
    {
        for o in list {
            let path = object_path(layout, o)?;
            let rows = if o.path.ends_with(".parquet") {
                Some(
                    SerializedFileReader::new(File::open(&path).map_err(err)?)
                        .map_err(err)?
                        .metadata()
                        .file_metadata()
                        .num_rows(),
                )
            } else {
                None
            };
            let embed = o.path.ends_with(".meta.json")
                || [
                    "download_manifest.json",
                    "dataset/manifest.json",
                    "dataset/hashes.sha256",
                    "dataset/reports/quality.json",
                ]
                .contains(&o.path.as_str());
            let verbatim = if embed {
                Some(fs::read_to_string(&path).map_err(err)?)
            } else {
                None
            };
            if o.path.ends_with(".meta.json") && !o.path.starts_with("seed/") {
                let meta: Value =
                    serde_json::from_str(verbatim.as_deref().unwrap()).map_err(err)?;
                if let Some(d) = meta["date"].as_str() {
                    day_bounds(d)?;
                    metas.insert(d.to_string(), meta);
                }
            }
            let collection = if o.path.contains("collection") && o.path.ends_with(".json") {
                let v: Value =
                    serde_json::from_reader(File::open(&path).map_err(err)?).map_err(err)?;
                let symbol = sources.datasets[0].provider_symbol.as_str();
                v.get("assets").and_then(|v| v.get(symbol)).cloned()
            } else {
                None
            };
            objects.push(json!({"generation":generation,"path":o.path,"key":o.key,"sha256":o.sha256,"bytes":o.bytes,"rows":rows,"verbatim":verbatim,"collection_entry":collection}));
        }
    }
    Ok((objects, metas))
}

fn observations(
    local: &Store,
    work: &Path,
    newest: &GenerationManifest,
    metas: &BTreeMap<String, Value>,
    coverage: Option<&Value>,
) -> Result<(Vec<ObjectRecord>, Vec<DayInventoryEntry>, u64), String> {
    let id = InstrumentId {
        broker: newest.broker.clone(),
        provider_symbol: newest.provider_symbol.clone(),
    };
    let mut objects = Vec::new();
    let mut days = BTreeMap::new();
    let mut rows = Vec::new();
    let mut current = String::new();
    let mut peak = 0;
    let mut flush = |d: &str, rows: &mut Vec<LosslessRow>| -> Result<(), String> {
        if d.is_empty() {
            return Ok(());
        }
        peak = peak.max(rows.len() as u64);
        let path = work.join("observations.parquet");
        let data = match newest.price_representation {
            PriceRepresentation::IntegerUnits { scale } => daily::write_ticks(
                &path,
                d,
                &id,
                scale,
                [rows.drain(..).map(|r| match r {
                    LosslessRow::Tick(t) => t,
                    _ => unreachable!(),
                })],
            )?,
            PriceRepresentation::BinaryFloat64 => daily::write_bars(
                &path,
                d,
                [rows.drain(..).map(|r| match r {
                    LosslessRow::Bar(b) => b,
                    _ => unreachable!(),
                })],
            )?,
        };
        let object = retain_file(
            local,
            &path,
            &format!("observations/{d}.parquet"),
            ObjectRole::Normalized,
        )?;
        let mut day = new_day(d, DayFamily::Observations);
        set_data(&mut day, &data, &object);
        objects.push(object);
        days.insert(d.to_string(), day);
        Ok(())
    };
    daily::read_generation_lossless(local, newest, |row| {
        let d = date(row.time()?);
        if d != current {
            flush(&current, &mut rows)?;
            current = d;
        }
        rows.push(row);
        Ok(())
    })?;
    flush(&current, &mut rows)?;
    let cutoff = coverage
        .and_then(|c| c.pointer("/requested/end"))
        .and_then(Value::as_str)
        .map(time)
        .transpose()?
        .unwrap_or(time(&newest.coverage.last_event_time)?);
    let first = time(&newest.coverage.first_event_time)?;
    let mut d = day_bounds(&date(first))?.0;
    while d <= cutoff {
        let key = date(d);
        days.entry(key.clone())
            .or_insert_with(|| new_day(&key, DayFamily::Observations));
        d = d.checked_add(DAY_MICROS).ok_or("inventory day overflow")?;
    }
    for d in metas.keys() {
        days.entry(d.clone())
            .or_insert_with(|| new_day(d, DayFamily::Observations));
    }
    for day in days.values_mut() {
        let (start, end) = day_bounds(&day.date)?;
        let meta = metas.get(&day.date);
        let clipped = meta.is_some_and(|m| {
            m["clipped"] == true
                || m["partial"] == true
                || m["clipped_by_retention"] == true
                || m["clipped_by_now"] == true
        });
        if let Some(m) = meta {
            if m["complete"] == false {
                day.reason = Some("Deriv historical gap: complete=false".into());
            } else if !clipped
                && (m["complete"] == true || m["market_closed"] == true)
                && m["gaps"].as_array().is_none_or(Vec::is_empty)
            {
                if m["market_closed"] == true && day.rows > 0 {
                    return Err(format!(
                        "{}: market_closed contradicts observations",
                        day.date
                    ));
                }
                day.state = if day.rows == 0 && m["market_closed"] == true {
                    DayState::EmptyKnown
                } else {
                    DayState::Complete
                };
                day.reason = None;
            }
        } else if let Some(c) = coverage
            && let Some(range) = c.get("verified").filter(|v| !v.is_null())
        {
            let (from, to) = (
                time(range["start"].as_str().ok_or("coverage start")?)?,
                time(range["end"].as_str().ok_or("coverage end")?)?,
            );
            if from <= start && to >= end {
                day.state = if day.rows == 0 {
                    DayState::EmptyKnown
                } else {
                    DayState::Complete
                };
                day.reason = None;
            } else if from < end && to > start {
                day.state = DayState::Partial;
                day.reason = Some("verified acquisition covers only part of this day".into());
                if from > start {
                    day.unresolved.push(UnresolvedInterval {
                        start: time_text(start),
                        end: time_text(from.min(end)),
                    });
                }
                if to < end {
                    day.unresolved.push(UnresolvedInterval {
                        start: time_text(to.max(start)),
                        end: time_text(end),
                    });
                }
            }
        }
        if newest.source_kind == SourceKind::BarParquet
            && day.rows == 17_280
            && day.first_time.as_deref().map(time).transpose()? == Some(start)
            && day.last_time.as_deref().map(time).transpose()? == Some(end - 5_000_000)
        {
            day.state = DayState::Complete;
            day.reason = None;
        }
        if day.state == DayState::Unknown {
            day.unresolved = vec![UnresolvedInterval {
                start: time_text(start),
                end: time_text(end),
            }];
        }
        if day.date == date(cutoff) {
            day.state = DayState::Partial;
            let cutoff_reason = "cutoff day may receive later input";
            day.reason = Some(day.reason.take().map_or_else(
                || cutoff_reason.into(),
                |reason| format!("{reason}; {cutoff_reason}"),
            ));
            // Preserve any earlier unresolved head/tail boundary established by coverage.
            if !day.unresolved.iter().any(|r| r.end == time_text(end)) {
                day.unresolved.push(UnresolvedInterval {
                    start: time_text(cutoff.max(start)),
                    end: time_text(end),
                });
            }
        }
        if day.state == DayState::EmptyKnown {
            if day.object.is_some() {
                objects.retain(|o| Some(&o.key) != day.object.as_ref());
                day.object = None;
            }
        } else if day.object.is_none() {
            let path = work.join("empty.parquet");
            let data = match newest.price_representation {
                PriceRepresentation::IntegerUnits { scale } => daily::write_ticks(
                    &path,
                    &day.date,
                    &id,
                    scale,
                    [Vec::<binary_alpha_engine::market::Tick>::new()],
                )?,
                PriceRepresentation::BinaryFloat64 => {
                    daily::write_bars(&path, &day.date, [Vec::<daily::DailyBar>::new()])?
                }
            };
            let o = retain_file(local, &path, &day.logical_path()?, ObjectRole::Normalized)?;
            set_data(day, &data, &o);
            objects.push(o);
        }
    }
    Ok((objects, days.into_values().collect(), peak))
}

fn convert(
    job: &Job,
    bound: &Bound,
    layout: &Layout,
    access: Access<'_>,
    binding: &str,
    work: &Path,
) -> Result<State, String> {
    let local = layout.store();
    let sources = inventory(&local, bound, access)?;
    let settings = bound
        .core
        .brokers
        .iter()
        .find(|b| b.id() == &bound.core.history.as_ref().unwrap().broker)
        .unwrap();
    let identity = broker::source_identity(settings);
    let coverage = index_coverage(layout, work, &sources, &identity)?;
    let newest = select_newest(&sources, &coverage, false)?;
    let streamed_source = if sources.streams.is_empty() {
        None
    } else {
        Some(select_newest(&sources, &coverage, true)?)
    };
    let old_stream = sources.streams.iter().find(|m| {
        streamed_source.is_some_and(|source| m.source_generation == source.generation)
            && bound
                .core
                .instrument(&m.definition.id(), newest.native_granularity)
                == Some(&m.definition)
    });
    if streamed_source.is_some() && old_stream.is_none() {
        return Err("newest v1 stream definition differs from migration configuration".into());
    }
    let (replaced, metas) = source_lineage(layout, &sources)?;
    let mut spool = Spool {
        layout,
        work: work.to_path_buf(),
        aliases: work.join("aliases.jsonl"),
        offset_s: offset_seconds(bound),
        occurrences: 0,
    };
    mkdir(&work.join("identities"))?;
    let mut records = Vec::new();
    add_receipts(&mut spool, &sources, &identity, &mut records)?;
    add_pending(&mut spool, &sources, &mut records, &identity)?;
    // Archive pending diagnostics as immutable evidence without turning a restore into
    // resumption of a broker acquisition. The original active progress remains protected.
    for record in &mut records {
        if let Some(relative) = record["path"].as_str() {
            let path = layout.state.join(relative);
            let id = store::identify(&path)?;
            let extension = path
                .extension()
                .and_then(|e| e.to_str())
                .ok_or("pending evidence extension")?;
            let name = format!("{}-migration-pending-{}.{}", job.id, id.sha256, extension);
            layout.records().put_new(&name, &path, &id)?;
            record["name"] = json!(name);
        }
    }
    json_lines::<IndexedPage>(&work.join("coverage-pages.jsonl"), |p| {
        spool.history(&p, None, p.ordinal, false)?;
        Ok(())
    })?;
    // Single objects carried beside bundles are storage aliases. No content-hash deduplication
    // is performed for response occurrences, which were established above by request identity.
    for m in &sources.datasets {
        for o in &m.objects {
            if o.path.starts_with("raw/") && o.path.ends_with(".json") {
                let label_prefix = format!("coverage:{}/", m.generation);
                let mut directly_indexed = false;
                json_lines::<IndexedPage>(&work.join("coverage-pages.jsonl"), |p| {
                    directly_indexed |= p.label.starts_with(&label_prefix) && p.page.path == o.path;
                    Ok(())
                })?;
                let mut matched: Option<(String, Option<String>, Option<String>)> = None;
                json_lines::<IndexedPage>(&work.join("coverage-pages.jsonl"), |p| {
                    if p.page.sha256 == o.sha256
                        && p.label.starts_with(&label_prefix)
                        && (!directly_indexed || p.page.path == o.path)
                    {
                        let signature = (
                            p.page.sha256.clone(),
                            p.page.anchor.clone(),
                            p.page.receipt_time.clone(),
                        );
                        if let Some(existing) = &matched {
                            if existing != &signature {
                                return Err(format!(
                                    "{}: single-page alias matches multiple requests",
                                    o.path
                                ));
                            }
                            return Ok(());
                        }
                        let mut p = p;
                        p.label = format!("single:{}/{}", m.generation, o.path);
                        p.source = ByteRef {
                            key: o.key.clone(),
                            offset: 0,
                            bytes: o.bytes,
                            sha256: o.sha256.clone(),
                        };
                        spool.history(&p, None, p.ordinal, false)?;
                        matched = Some(signature);
                    }
                    Ok(())
                })?;
                if matched.is_none() {
                    return Err(format!(
                        "unresolved single page {}/{}: no occurrence evidence",
                        m.generation, o.path
                    ));
                }
            }
        }
    }
    let mut imports: Vec<ImportFiles> = Vec::new();
    for m in &sources.datasets {
        if let Some(raw) = m.objects.iter().find(|o| o.path == "raw_pages.ndjson")
            && let Some(prior) = imports.iter().find(|i| i.acquisition_id == raw.sha256)
        {
            let cp = m
                .objects
                .iter()
                .find(|o| o.path == "checkpoint.ndjson")
                .ok_or("missing checkpoint")?;
            if cp.sha256 != prior.checkpoint.sha256 {
                return Err("same raw import acquisition has conflicting checkpoints".into());
            }
            continue;
        }
        if m.source_kind == SourceKind::BarParquet
            && let Some(i) = add_import(&mut spool, m)?
        {
            imports.push(i);
        }
    }
    let (mut objects, mut days, mut peak_rows) = observations(
        &local,
        work,
        newest,
        &metas,
        coverage
            .iter()
            .find(|(g, _)| g == &newest.generation)
            .map(|(_, c)| c),
    )?;
    let mut peak_bytes = 0;
    for dir in entries(&work.join("days"))? {
        let d = dir.file_name().unwrap().to_string_lossy().to_string();
        let mut pages = entries(&dir)?
            .iter()
            .map(|p| get::<PageOccurrence>(p))
            .collect::<Result<Vec<_>, _>>()?;
        pages.sort_by(|a, b| (&a.acquisition_id, a.ordinal).cmp(&(&b.acquisition_id, b.ordinal)));
        peak_rows = peak_rows.max(pages.len() as u64);
        peak_bytes = peak_bytes.max(
            pages
                .iter()
                .map(|p| (p.payload.len() + p.checkpoint.as_ref().map_or(0, Vec::len)) as u64)
                .sum(),
        );
        let path = work.join("pages.parquet");
        let data = daily::write_pages(&path, &d, [pages])?;
        let mut day = new_day(&d, DayFamily::Pages);
        let object = retain_file(&local, &path, &day.logical_path()?, ObjectRole::Source)?;
        set_data(&mut day, &data, &object);
        objects.push(object);
        days.push(day);
    }
    if !spool.aliases.exists() {
        File::create(&spool.aliases).map_err(err)?;
    }
    let aliases_id = store::identify(&spool.aliases)?;
    let aliases = format!("{}-migration-aliases-{}.jsonl", job.id, aliases_id.sha256);
    layout
        .records()
        .put_new(&aliases, &spool.aliases, &aliases_id)?;
    let generations = sources
        .datasets
        .iter()
        .map(|m| m.generation.clone())
        .chain(sources.streams.iter().map(|m| m.generation.clone()))
        .collect::<Vec<_>>();
    let manifest_bindings = generations
        .iter()
        .map(|g| {
            let id = store::identify(&layout.store.join(manifest_key(g)))?;
            Ok(json!({"generation":g,"sha256":id.sha256,"bytes":id.bytes}))
        })
        .collect::<Result<Vec<_>, String>>()?;
    let mapping = lineage::MigrationMapping {
        v1_generations: sources
            .datasets
            .iter()
            .map(|m| m.generation.clone())
            .collect(),
        v1_stream: old_stream.map(|m| m.generation.clone()),
        v1_streams: sources
            .streams
            .iter()
            .map(|m| m.generation.clone())
            .collect(),
    };
    let mut lineage = json!({"schema_version":1,"kind":"migration","layout":"daily-v2","source_identity":identity,"role":newest.role,"newest_v1_dataset":newest.generation,"objects":replaced,"manifests":manifest_bindings,"imports":imports,"identity_basis":{"recorded":"intent plus request fingerprint and per-receipt occurrence count; shortest receipt then lexical name owns acquisition/ordinal","legacy":"bundle origin and slice, or inherited canonical single-page coverage prefix; legacy-coverage-<coverage SHA-256> and original coverage ordinal"},"ndjson_framing":"per-line terminators, offsets and independent ordinals in immutable alias table","alias_table":{"record":aliases,"sha256":aliases_id.sha256,"bytes":aliases_id.bytes},"records":records});
    lineage.as_object_mut().unwrap().extend(
        serde_json::to_value(&mapping)
            .map_err(err)?
            .as_object()
            .unwrap()
            .clone(),
    );
    if newest.source_kind == SourceKind::BrokerHistory {
        lineage["continuation"] = json!({
            "acquisition_id": format!("v1-history:{}", newest.generation),
            "seed": null,
        });
    }
    objects.push(retain_json(
        &local,
        work,
        "provenance/lineage.json",
        &lineage,
    )?);
    let mut manifest = newest.clone();
    manifest.layout = Some(DailyLayout::DailyV2);
    manifest.day_inventory = days;
    manifest.objects = objects;
    let cov = lineage::migration_coverage(&mut manifest, &coverage, &identity)?;
    manifest
        .objects
        .push(retain_json(&local, work, fetch::COVERAGE_PATH, &cov)?);
    manifest.code_revision = import::CODE_REVISION.into();
    let scale = match manifest.price_representation {
        PriceRepresentation::IntegerUnits { scale } => Some(scale),
        _ => None,
    };
    manifest.generation = generation_id_with_layout(
        &InstrumentId {
            broker: manifest.broker.clone(),
            provider_symbol: manifest.provider_symbol.clone(),
        },
        manifest.source_kind,
        manifest.role,
        scale,
        &manifest.objects,
        manifest.layout,
    );
    GenerationManifest::from_json(&manifest.to_json())?;
    let path = work.join("ready.json");
    fs::write(&path, manifest.to_json()).map_err(err)?;
    local.put_new(&manifest.key(), &path, &store::identify(&path)?)?;
    let audited = audit::audit(
        &bound.core,
        &local.uri(&manifest.key()),
        &local,
        &local,
        access,
    )?;
    Ok(State {
        phase: "converted".into(),
        binding: binding.into(),
        dataset: manifest.generation,
        stream: audited.generation,
        newest: newest.generation.clone(),
        old_stream: old_stream.map(|m| m.generation.clone()),
        generations,
        aliases,
        imports,
        occurrences: spool.occurrences,
        peak_day_rows: peak_rows,
        peak_day_payload_bytes: peak_bytes,
        record: None,
    })
}

fn observation_proof(local: &Store, manifest: &GenerationManifest) -> Result<Value, String> {
    let mut hash = Sha256::new();
    let read = daily::read_generation_lossless(local, manifest, |row| {
        match row {
            LosslessRow::Tick(t) => {
                hash.update(b"tick");
                hash.update(t.event_time_micros.to_le_bytes());
                hash.update(t.price_units.to_le_bytes());
            }
            LosslessRow::Bar(b) => {
                hash.update(b"bar");
                hash.update(serde_json::to_vec(&b).map_err(err)?);
            }
        }
        Ok(())
    })?;
    Ok(
        json!({"rows":read.data.rows,"sha256":binary_alpha_engine::hex(&hash.finalize()),"first":read.data.first_event_micros,"last":read.data.last_event_micros}),
    )
}
fn candle_digest(layout: &Layout, m: &StreamManifest) -> Result<String, String> {
    use parquet::file::reader::{FileReader, SerializedFileReader};
    let mut hash = Sha256::new();
    for spec in &m.streams {
        hash.update(spec.duration_seconds.to_le_bytes());
        hash.update(spec.offset_seconds.to_le_bytes());
        let paths = if m.layout.is_none() {
            vec![StreamManifestPath::legacy(spec)]
        } else {
            m.day_inventory
                .iter()
                .filter(|d| {
                    d.duration == Some(spec.duration_seconds)
                        && d.offset == Some(spec.offset_seconds)
                        && d.object.is_some()
                })
                .map(|d| d.logical_path())
                .collect::<Result<Vec<_>, _>>()?
        };
        for path in paths {
            let object = m
                .objects
                .iter()
                .find(|o| o.path == path)
                .ok_or("missing candle object")?;
            let reader =
                SerializedFileReader::new(File::open(object_path(layout, object)?).map_err(err)?)
                    .map_err(err)?;
            for row in reader.get_row_iter(None).map_err(err)? {
                hash.update(format!("{:?}\n", row.map_err(err)?).as_bytes());
            }
        }
    }
    Ok(binary_alpha_engine::hex(&hash.finalize()))
}
struct StreamManifestPath;
impl StreamManifestPath {
    fn legacy(s: &binary_alpha_engine::stream::StreamSummary) -> String {
        binary_alpha_engine::stream::StreamSummary::object_path(
            s.duration_seconds,
            s.offset_seconds,
        )
    }
}
fn read_stream(layout: &Layout, generation: &str) -> Result<StreamManifest, String> {
    StreamManifest::from_json(&fs::read(layout.store.join(manifest_key(generation))).map_err(err)?)
}
fn verify_migration(
    layout: &Layout,
    state: &State,
    access: Access<'_>,
    work: &Path,
    offset_s: i64,
) -> Result<Value, String> {
    let local = layout.store();
    for generation in &state.generations {
        let bytes = fs::read(layout.store.join(manifest_key(generation))).map_err(err)?;
        if verify::manifest_kind(&bytes)?.is_none() {
            access.permit(Some(DatasetRole::Development), generation)?;
        } else {
            let m = StreamManifest::from_json(&bytes)?;
            access.permit(Some(m.role), &m.source_generation)?;
        }
    }
    let old = read_manifest(&local, &state.newest)?.0;
    let new = read_manifest(&local, &state.dataset)?.0;
    let old_rows = observation_proof(&local, &old)?;
    let new_rows = observation_proof(&local, &new)?;
    if old_rows != new_rows {
        return Err(
            "observation equality proof failed: v1/v2 rows differ in order or provider columns"
                .into(),
        );
    }
    let proof = work.join("proof");
    if proof.exists() {
        fs::remove_dir_all(&proof).map_err(err)?;
    }
    mkdir(&proof)?;
    let lineage_obj = new
        .objects
        .iter()
        .find(|o| o.path == "provenance/lineage.json")
        .ok_or("missing lineage")?;
    let lineage: Value =
        serde_json::from_reader(File::open(object_path(layout, lineage_obj)?).map_err(err)?)
            .map_err(err)?;
    let alias_path = layout.state.join("records").join(&state.aliases);
    let alias_id = store::identify(&alias_path)?;
    if lineage["alias_table"]["sha256"] != alias_id.sha256
        || lineage["alias_table"]["bytes"] != alias_id.bytes
    {
        return Err("migration alias table hash mismatch".into());
    }
    for record in lineage["records"]
        .as_array()
        .ok_or("missing source records")?
    {
        let mut paths = Vec::new();
        if let Some(name) = record["name"].as_str() {
            paths.push(layout.state.join("records").join(name));
        }
        if let Some(path) = record["path"].as_str() {
            paths.push(layout.state.join(path));
        }
        if paths.is_empty() {
            return Err("migration source record lacks a name or path".into());
        }
        for path in paths {
            let id = store::identify(&path)?;
            if record["sha256"] != id.sha256 || record["bytes"] != id.bytes {
                return Err(format!(
                    "migration source record changed: {}",
                    path.display()
                ));
            }
        }
    }
    let mut count = 0;
    for day in new
        .day_inventory
        .iter()
        .filter(|d| d.family == DayFamily::Pages)
    {
        let object = new
            .objects
            .iter()
            .find(|o| Some(&o.key) == day.object.as_ref() && o.path == day.logical_path().unwrap())
            .ok_or("missing page day")?;
        for page in daily::read_pages(&object_path(layout, object)?, &day.date)
            .map_err(|e| format!("page proof {}: {e}", day.date))?
        {
            let key = sha256_hex(&json_bytes(&(&page.acquisition_id, page.ordinal))?);
            let path = proof.join(key);
            if path.exists() {
                return Err("page proof: duplicated occurrence".into());
            }
            save(&path, &page)?;
            count += 1;
        }
    }
    if count != state.occurrences {
        return Err("page proof occurrence count differs".into());
    }
    let mut aliases = 0;
    let mut checkpoint_count = 0;
    for i in &state.imports {
        for name in ["raw", "checkpoint"] {
            File::create(proof.join(format!("{}-{name}", i.acquisition_id))).map_err(err)?;
        }
    }
    json_lines::<Alias>(&alias_path, |alias| {
        let key = sha256_hex(&json_bytes(&(&alias.acquisition_id, alias.ordinal))?);
        let page: PageOccurrence = get(&proof.join(&key))
            .map_err(|e| format!("{}: missing target occurrence: {e}", alias.label))?;
        let raw = slice(layout, &alias.source).map_err(|e| format!("{}: {e}", alias.label))?;
        if raw != page.payload {
            return Err(format!("{}: page equality proof failed", alias.label));
        }
        let label_path = proof.join(format!("alias-{}", sha256_hex(alias.label.as_bytes())));
        if label_path.exists() {
            return Err(format!("{}: alias mapped more than once", alias.label));
        }
        save(&label_path, &alias)?;
        File::create(proof.join(format!("seen-{key}"))).map_err(err)?;
        if let Some(cp) = &alias.checkpoint {
            let bytes =
                slice(layout, cp).map_err(|e| format!("{} checkpoint: {e}", alias.label))?;
            if page.checkpoint.as_ref() != Some(&bytes)
                || page.checkpoint_ordinal != alias.checkpoint_ordinal
            {
                return Err(format!("{}: checkpoint equality proof failed", alias.label));
            }
            let marker = proof.join(format!(
                "checkpoint-{}-{}",
                alias.acquisition_id,
                alias
                    .checkpoint_ordinal
                    .ok_or("missing checkpoint ordinal")?
            ));
            if marker.exists() {
                return Err("checkpoint ordinal mapped more than once".into());
            }
            save(&marker, &key)?;
            for (name, at, data, suffix) in [
                (
                    "raw",
                    alias.source.offset,
                    page.payload.as_slice(),
                    &alias.raw_suffix,
                ),
                (
                    "checkpoint",
                    cp.offset,
                    bytes.as_slice(),
                    &alias.checkpoint_suffix,
                ),
            ] {
                let mut f = fs::OpenOptions::new()
                    .write(true)
                    .open(proof.join(format!("{}-{name}", alias.acquisition_id)))
                    .map_err(err)?;
                f.seek(SeekFrom::Start(at)).map_err(err)?;
                f.write_all(data).map_err(err)?;
                f.write_all(suffix).map_err(err)?;
            }
            checkpoint_count += 1;
        }
        aliases += 1;
        Ok(())
    })?;
    let seen =
        fs::read_dir(&proof)
            .map_err(err)?
            .try_fold(0u64, |count, e| -> Result<u64, String> {
                Ok(count
                    + u64::from(
                        e.map_err(err)?
                            .file_name()
                            .to_string_lossy()
                            .starts_with("seen-"),
                    ))
            })?;
    if seen != state.occurrences {
        return Err("page proof: a v2 occurrence has no source alias".into());
    }
    let mut reconstructed = Vec::new();
    for i in &state.imports {
        for (name, object) in [("raw", &i.raw), ("checkpoint", &i.checkpoint)] {
            object_path(layout, object)?;
            let id = store::identify(&proof.join(format!("{}-{name}", i.acquisition_id)))?;
            if id.sha256 != object.sha256 || id.bytes != object.bytes {
                return Err(format!(
                    "{}.ndjson reconstruction SHA-256/bytes proof failed",
                    if name == "raw" { "raw_pages" } else { name }
                ));
            }
            reconstructed
                .push(json!({"path":object.path,"sha256":id.sha256,"bytes":id.bytes,"equal":true}));
        }
    }
    let source_census = census(layout, state, &proof, &lineage, offset_s)?;
    let stream = read_stream(layout, &state.stream)?;
    let candles = if let Some(old_stream) = &state.old_stream {
        let before = read_stream(layout, old_stream)?;
        if before.streams != stream.streams {
            return Err("candle summaries differ".into());
        }
        let a = candle_digest(layout, &before)?;
        let b = candle_digest(layout, &stream)?;
        if a != b {
            return Err("candle row equality proof failed".into());
        }
        let profile = |m: &StreamManifest| -> Result<Value, String> {
            let o = m
                .objects
                .iter()
                .find(|o| o.path == "profile.json")
                .ok_or("missing profile")?;
            serde_json::from_reader(File::open(object_path(layout, o)?).map_err(err)?).map_err(err)
        };
        let mut a_profile = profile(&before)?;
        let b_profile = profile(&stream)?;
        a_profile["source"]["generation"] = json!(state.dataset);
        for calculation in a_profile["calculations"]
            .as_array_mut()
            .ok_or("profile calculations missing")?
        {
            if let Some(reason) = calculation["reason"].as_str()
                && let Ok(mut reason) = serde_json::from_str::<Value>(reason)
                && reason["generation"] == before.source_generation
            {
                reason["generation"] = json!(state.dataset);
                calculation["reason"] = json!(serde_json::to_string(&reason).map_err(err)?);
            }
        }
        // Capability reasons are encoded JSON strings; compare their semantic JSON after
        // the same exact generation substitution (object key order is not semantic).
        let mut b_profile = b_profile;
        for profile in [&mut a_profile, &mut b_profile] {
            for calculation in profile["calculations"]
                .as_array_mut()
                .ok_or("profile calculations missing")?
            {
                if let Some(reason) = calculation["reason"].as_str()
                    && let Ok(reason) = serde_json::from_str::<Value>(reason)
                {
                    calculation["reason"] = reason;
                }
            }
        }
        if a_profile != b_profile {
            return Err(
                "profile equality proof failed after source.generation continuation substitution"
                    .into(),
            );
        }
        json!({"equal":true,"sha256":a,"profile_equal":true,"profile_substitution":{"from":before.source_generation,"to":state.dataset,"fields":["source.generation","calculations[*].reason.generation"]}})
    } else {
        json!({"equal":null,"reason":"no v1 stream exists"})
    };
    let dataset_verified = verify::run_with(&layout.manifest_uri(&state.dataset), access)?;
    let stream_verified = verify::run_with(&layout.manifest_uri(&state.stream), access)?;
    Ok(
        json!({"observations":{"equal":true,"v1":old_rows,"v2":new_rows},"pages":{"equal":true,"occurrences":count,"aliases":aliases,"checkpoint_lines":checkpoint_count,"source_census":source_census},"import_files":reconstructed,"candles":candles,"data_verify":{"dataset":dataset_verified,"stream":stream_verified}}),
    )
}
fn migrate_job_with(
    job: &Job,
    bound: &Bound,
    layout: &Layout,
    access: Access<'_>,
    after_converted: &(dyn Fn(&str) -> Result<(), String> + Sync),
) -> Result<(State, bool), String> {
    let dir = layout.job_state(&job.id)?;
    let path = dir.join("migration.json");
    let work = dir.join("migration-work");
    let binding = sha256_hex(&json_bytes(&(
        bound.core.content_hash(),
        &bound.evidence_sha256,
    ))?);
    let mut state = if let Some(state) = read_json::<State>(&path)? {
        if state.binding != binding {
            return Err("migration configuration/evidence changed since converted phase".into());
        }
        if state.phase == "verified" {
            return Ok((state, true));
        }
        if state.phase != "converted" {
            return Err("unknown migration phase".into());
        }
        state
    } else {
        if work.exists() {
            fs::remove_dir_all(&work).map_err(err)?;
        }
        mkdir(&work)?;
        let state = convert(job, bound, layout, access, &binding, &work)?;
        save(&path, &state)?;
        state
    };
    after_converted(&job.id)?;
    let proofs = verify_migration(layout, &state, access, &work, offset_seconds(bound))?;
    let root = read_manifest(&layout.store(), &state.dataset)?.0;
    let lineage = lineage::read_lineage(&layout.store(), &root)?;
    let mapping: lineage::MigrationMapping = serde_json::from_value(lineage).map_err(err)?;
    let mut evidence = BTreeMap::new();
    evidence.insert("command".into(), json!("migrate"));
    evidence.insert("binding".into(), json!(binding));
    evidence.insert("newest_v1_dataset".into(), json!(state.newest));
    evidence.insert("alias_table".into(), json!(state.aliases));
    evidence.insert("proofs".into(), proofs);
    evidence.insert("memory".into(), json!({"bound":"one UTC observation/page day plus one provider response and Parquet column buffers; audit holds one candle day per configured stream; manifests/lineage and verification occurrence IDs are additional metadata; conversion page/checkpoint indexes are on disk","peak_day_rows":state.peak_day_rows,"peak_day_payload_bytes":state.peak_day_payload_bytes}));
    let record = publish(
        &layout.records(),
        &format!("{}-migration", job.id),
        &lineage::MigrationRecord {
            schema_version: 1,
            job: job.id.clone(),
            phase: "verified".into(),
            mapping,
            v2_root: state.dataset.clone(),
            v2_stream: state.stream.clone(),
            equality: lineage::MigrationEquality {
                observations: true,
                pages: true,
                source_files: true,
                candles: state.old_stream.as_ref().map(|_| true),
            },
            evidence,
        },
    )?;
    state.record = Some(record);
    state.phase = "verified".into();
    save(&path, &state)?;
    fs::remove_dir_all(&work).map_err(err)?;
    Ok((state, false))
}

/// A deterministic interruption boundary for non-live orchestration and fixture recovery.
/// The hook can run concurrently for different jobs; a hook failure is reported with that
/// job while the remaining jobs continue, just like a conversion or verification failure.
pub fn migrate_with(
    config_path: &Path,
    selected: Option<&str>,
    after_converted: &(dyn Fn(&str) -> Result<(), String> + Sync),
    out: &mut dyn Write,
) -> Result<(), String> {
    let (mut config, layout, _) = load(config_path)?;
    let _lock = writer_lock(&layout)?;
    if config.jobs.is_empty() {
        return Err("migration: no jobs".into());
    }
    if selected.is_some_and(|id| !config.jobs.iter().any(|j| j.id == id)) {
        return Err("migration: unknown --job".into());
    }
    config.jobs.retain(|j| selected.is_none_or(|id| id == j.id));
    let declaration = declaration(&config)?;
    let access = Access {
        declaration: declaration.as_ref(),
        certification: None,
    };
    let failed = run_job_pool(&config, out, &|job, _out| {
        let bound = bind(job, &layout)?;
        let (state, reused) = migrate_job_with(job, &bound, &layout, access, after_converted)?;
        Ok(format!(
            "pipeline migrate {} status {} dataset {} stream {} record {} observations_equal true pages {} import_files_equal true candles_equal {} peak_day_rows {} peak_day_payload_bytes {}",
            job.id,
            if reused {
                "already_verified"
            } else {
                "verified"
            },
            state.dataset,
            state.stream,
            state.record.as_deref().unwrap_or("none"),
            state.occurrences,
            if state.old_stream.is_some() {
                "true"
            } else {
                "not_applicable"
            },
            state.peak_day_rows,
            state.peak_day_payload_bytes
        ))
    })?;
    job_result(&config, failed)
}

fn import_offset(layout: &Layout, m: &GenerationManifest) -> Result<i64, String> {
    let mut found = None;
    for o in &m.objects {
        if o.path == "download_manifest.json"
            || o.path.contains("collection") && o.path.ends_with(".json")
        {
            let v: Value =
                serde_json::from_reader(File::open(object_path(layout, o)?).map_err(err)?)
                    .map_err(err)?;
            for field in ["server_timestamp_offset_seconds", "server_offset_seconds"] {
                if let Some(offset) = v[field].as_i64() {
                    if found.is_some_and(|old| old != offset) {
                        return Err("import recorded server offsets disagree".into());
                    }
                    found = Some(offset);
                }
            }
        }
    }
    found.ok_or("import lacks recorded server offset; current broker configuration is not historical evidence".into())
}

fn check_history_occurrence(
    label: &str,
    p: &fetch::PageCoverage,
    row: &PageOccurrence,
    offset_s: i64,
) -> Result<(), String> {
    // Resume must validate retained payloads even if an earlier converter recorded bad bounds.
    let (rows, first, last) = payload_bounds(&row.payload, offset_s)?;
    if (
        rows,
        first.as_deref().map(time).transpose()?,
        last.as_deref().map(time).transpose()?,
    ) != (row.rows, row.first_event_time, row.last_event_time)
    {
        return Err(format!("{label}: payload rows/event bounds mismatch"));
    }
    if row.payload_sha256 != p.sha256
        || row.payload.len() as u64 != p.bytes
        || row.rows != p.rows
        || row.request_token != p.anchor
        || row.first_event_time != p.first.as_deref().map(time).transpose()?
        || row.last_event_time != p.last.as_deref().map(time).transpose()?
        || row.receipt_time_utc != p.receipt_time.as_deref().map(receipt_time).transpose()?
    {
        return Err(format!(
            "{label}: occurrence metadata equality proof failed"
        ));
    }
    Ok(())
}

// Independent source-side census. The generated alias table cannot certify its own
// completeness: every original index and receipt is traversed again and must have a target.
fn census(
    layout: &Layout,
    state: &State,
    proof: &Path,
    lineage: &Value,
    offset_s: i64,
) -> Result<Value, String> {
    let local = layout.store();
    let newest = read_manifest(&local, &state.newest)?.0;
    let require = |label: &str| -> Result<PageOccurrence, String> {
        let alias: Alias = get(&proof.join(format!("alias-{}", sha256_hex(label.as_bytes()))))
            .map_err(|_| format!("page accounting proof: missing source alias {label}"))?;
        get(&proof.join(sha256_hex(&json_bytes(&(
            &alias.acquisition_id,
            alias.ordinal,
        ))?)))
    };
    let check = |label: &str, p: &fetch::PageCoverage| -> Result<(), String> {
        check_history_occurrence(label, p, &require(label)?, offset_s)
    };
    for binding in lineage["manifests"]
        .as_array()
        .ok_or("lineage lacks manifest bindings")?
    {
        let g = binding["generation"]
            .as_str()
            .ok_or("lineage manifest generation")?;
        let id = store::identify(&layout.store.join(manifest_key(g)))?;
        if binding["sha256"] != id.sha256 || binding["bytes"] != id.bytes {
            return Err(format!("source manifest {g} changed after converted"));
        }
    }
    let (mut pages, mut requests, mut singles, mut pending) = (0u64, 0u64, 0u64, 0u64);
    let (mut raw_lines, mut checkpoint_lines) = (0u64, 0u64);
    for generation in local.list_manifests()? {
        let bytes = fs::read(layout.store.join(manifest_key(&generation))).map_err(err)?;
        if let Some(kind) = verify::manifest_kind(&bytes)? {
            if kind == binary_alpha_engine::stream::STREAM_MANIFEST_KIND {
                let m = StreamManifest::from_json(&bytes)?;
                if m.layout.is_none() && m.instrument == newest.instrument && m.role == newest.role
                {
                    if !state.generations.contains(&generation) {
                        return Err(format!(
                            "new v1 stream {generation} appeared after converted"
                        ));
                    }
                    for o in &m.objects {
                        object_path(layout, o)?;
                    }
                }
            }
            continue;
        }
        let m = GenerationManifest::from_json(&bytes)?;
        if m.layout.is_some() || m.instrument != newest.instrument || m.role != newest.role {
            continue;
        }
        if !state.generations.contains(&generation) {
            return Err(format!(
                "new v1 generation {generation} appeared after converted; migration snapshot is unresolved"
            ));
        }
        if let Some((raw, checkpoint)) = import_pair(&m)? {
            if !state.imports.iter().any(|i| {
                i.acquisition_id == raw.sha256
                    && i.raw.bytes == raw.bytes
                    && i.checkpoint.sha256 == checkpoint.sha256
                    && i.checkpoint.bytes == checkpoint.bytes
            }) {
                return Err(format!(
                    "{generation}: import files missing from migration census"
                ));
            }
            raw_lines += lines(&object_path(layout, raw)?, |ordinal, _, bytes, _| {
                let key = sha256_hex(&json_bytes(&(&raw.sha256, ordinal))?);
                let row: PageOccurrence = get(&proof.join(key))?;
                if row.payload != bytes {
                    return Err(format!(
                        "{generation}: raw line {ordinal} equality proof failed"
                    ));
                }
                Ok(())
            })?;
            checkpoint_lines +=
                lines(&object_path(layout, checkpoint)?, |ordinal, _, bytes, _| {
                    let key: String =
                        get(&proof.join(format!("checkpoint-{}-{ordinal}", raw.sha256)))?;
                    let row: PageOccurrence = get(&proof.join(key))?;
                    let cp: Value = serde_json::from_slice(&bytes).map_err(err)?;
                    if row.checkpoint_ordinal != Some(ordinal)
                        || row.checkpoint.as_ref() != Some(&bytes)
                        || cp["payload_sha256"] != row.payload_sha256
                    {
                        return Err(format!(
                            "{generation}: checkpoint line {ordinal} equality proof failed"
                        ));
                    }
                    Ok(())
                })?;
        }
        for o in &m.objects {
            if o.path == fetch::COVERAGE_PATH {
                let path = object_path(layout, o)?;
                let mut ordinal = 0;
                let mut legacy_targets = std::collections::BTreeSet::new();
                document(&path, &mut |kind, value| {
                    if kind == "pages" {
                        let p: fetch::PageCoverage = serde_json::from_value(value).map_err(err)?;
                        let label = format!("coverage:{generation}/{ordinal}");
                        check(&label, &p)?;
                        if p.receipt_time.is_none() {
                            let row = require(&label)?;
                            if !legacy_targets.insert((row.acquisition_id, row.ordinal)) {
                                return Err(format!(
                                    "{label}: distinct legacy requests share an occurrence"
                                ));
                            }
                        }
                        ordinal += 1;
                        pages += 1;
                    }
                    Ok(())
                })?;
            } else if o.path.starts_with("raw/") && o.path.ends_with(".json") {
                let page = require(&format!("single:{generation}/{}", o.path))?;
                if page.payload_sha256 != o.sha256 {
                    return Err("single page source digest differs".into());
                }
                singles += 1;
            }
            object_path(layout, o)?;
        }
    }
    for path in entries(&layout.state.join("records"))? {
        let name = path.file_name().unwrap().to_string_lossy().to_string();
        if !name.contains("-receipt-") || !name.ends_with(".json") {
            continue;
        }
        let mut count = 0;
        let header = document(&path, &mut |kind, _| {
            if kind == "requests" {
                count += 1;
            }
            Ok(())
        })?;
        if !receipt_matches(
            layout,
            &header,
            &newest,
            lineage["source_identity"]
                .as_str()
                .ok_or("lineage source identity")?,
            count != 0,
        )
        .map_err(|e| format!("{name}: {e}"))?
        {
            continue;
        }
        let mut ordinal = 0;
        document(&path, &mut |kind, value| {
            if kind == "requests" {
                let p: PageReceipt = serde_json::from_value(value).map_err(err)?;
                let label = format!("receipt:{name}/{ordinal}");
                let row = require(&label)?;
                if row.payload_sha256 != p.sha256
                    || row.payload.len() as u64 != p.bytes
                    || row.rows != p.rows
                    || row.request_token != p.anchor
                    || row.receipt_time_utc != Some(receipt_time(&p.receipt_time)?)
                    || row.intent.as_deref() != header["intent"].as_str()
                {
                    return Err(format!("{label}: receipt metadata equality proof failed"));
                }
                ordinal += 1;
                requests += 1;
            }
            Ok(())
        })?;
    }
    for dir in entries(&layout.state)? {
        let path = dir.join("progress.json");
        if !path.is_file() {
            continue;
        }
        let header = document(&path, &mut |_, _| Ok(()))?;
        if !header
            .pointer("/progress/baseline")
            .and_then(Value::as_str)
            .is_some_and(|g| state.generations.iter().any(|known| known == g))
        {
            continue;
        }
        let id = store::identify(&path)?;
        let rel = path
            .strip_prefix(&layout.state)
            .map_err(err)?
            .to_str()
            .ok_or("pending path encoding")?;
        if !lineage["records"]
            .as_array()
            .ok_or("missing records")?
            .iter()
            .any(|r| r["kind"] == "pending" && r["path"] == rel && r["sha256"] == id.sha256)
        {
            return Err(format!(
                "pending acquisition {rel} appeared or changed after converted"
            ));
        }
        let acquisition = format!(
            "{}-{}",
            header["intent"].as_str().ok_or("pending intent")?,
            id.sha256
        );
        let mut ordinal = 0;
        let mut accept = |value: Value| -> Result<(), String> {
            let p: fetch::PageCoverage = serde_json::from_value(value).map_err(err)?;
            let label = format!("pending:{acquisition}/{ordinal}");
            check(&label, &p)?;
            if require(&label)?.disposition != PageDisposition::Diagnostic {
                return Err(format!("{label}: pending occurrence is not diagnostic"));
            }
            ordinal += 1;
            pending += 1;
            Ok(())
        };
        document(&path, &mut |kind, v| {
            if kind == "pages" { accept(v) } else { Ok(()) }
        })?;
        json_lines::<Value>(&dir.join("progress.pages.jsonl"), accept)?;
    }
    Ok(
        json!({"coverage_entries":pages,"receipt_requests":requests,"single_objects":singles,"pending_pages":pending,"raw_lines":raw_lines,"checkpoint_lines":checkpoint_lines,"all_mapped_once":true}),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn converted_page() -> (fetch::PageCoverage, PageOccurrence) {
        let payload = br#"{"history":{"times":[1754956800,1754956802]}}"#.to_vec();
        let (rows, first, last) = payload_bounds(&payload, 0).unwrap();
        let coverage = fetch::PageCoverage {
            occurrence: None,
            path: "raw/fixture.json".into(),
            offset: None,
            sha256: sha256_hex(&payload),
            bytes: payload.len() as u64,
            anchor: Some("1754956810".into()),
            rows,
            first,
            last,
            receipt_time: None,
        };
        let row = PageOccurrence {
            acquisition_id: "legacy-coverage-fixture".into(),
            intent: None,
            ordinal: 0,
            checkpoint_ordinal: None,
            order_kind: PageOrderKind::RequestOrder,
            payload_sha256: coverage.sha256.clone(),
            payload,
            request_token: coverage.anchor.clone(),
            request_anchor_utc: Some(1_754_956_810_000_000),
            receipt_time_utc: None,
            receipt_state: ReceiptState::AbsentInLegacyRecord,
            first_event_time: Some(1_754_956_800_000_000),
            last_event_time: Some(1_754_956_802_000_000),
            rows,
            checkpoint: None,
            disposition: PageDisposition::Indexed,
        };
        (coverage, row)
    }

    #[test]
    fn census_rejects_converted_bounds_that_disagree_with_payload() {
        for corrupt_rows in [false, true] {
            let (mut coverage, mut row) = converted_page();
            assert_eq!(
                check_history_occurrence("coverage:fixture/0", &coverage, &row, 0),
                Ok(())
            );
            // Old converted artifacts and their source metadata can agree with each other
            // while disagreeing with the retained provider bytes.
            if corrupt_rows {
                coverage.rows += 1;
                row.rows += 1;
            } else {
                row.first_event_time = row.first_event_time.map(|t| t + DAY_MICROS);
                row.last_event_time = row.last_event_time.map(|t| t + DAY_MICROS);
                coverage.first = row.first_event_time.map(time_text);
                coverage.last = row.last_event_time.map(time_text);
            }
            assert_eq!(
                check_history_occurrence("coverage:fixture/0", &coverage, &row, 0),
                Err("coverage:fixture/0: payload rows/event bounds mismatch".into())
            );
        }
    }

    #[test]
    fn census_rejects_converted_legacy_occurrence_with_recorded_receipt() {
        let (coverage, mut row) = converted_page();
        assert_eq!(
            check_history_occurrence("coverage:fixture/0", &coverage, &row, 0),
            Ok(())
        );
        row.acquisition_id = "modern-receipt.json".into();
        row.receipt_state = ReceiptState::Recorded;
        row.receipt_time_utc = Some(1_754_956_900_000_000);
        assert_eq!(
            check_history_occurrence("coverage:fixture/0", &coverage, &row, 0),
            Err("coverage:fixture/0: occurrence metadata equality proof failed".into())
        );
    }
}

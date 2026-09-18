//! A proof upgrade retains the v1 equality gate, then rebuilds only root metadata around
//! the daily continuation. Partition reuse and preservation are checked independently.
use super::*;

pub(super) fn index_daily(
    layout: &Layout,
    work: &Path,
    family: &[GenerationManifest],
    access: Access<'_>,
) -> Result<(), String> {
    mkdir(&work.join("daily-occurrences"))?;
    for m in family {
        verify::run_with(&layout.manifest_uri(&m.generation), access)?;
        for day in m
            .day_inventory
            .iter()
            .filter(|d| d.family == DayFamily::Pages && d.object.is_some())
        {
            let o = day_object(m, day)?;
            for page in daily::read_pages(&object_path(layout, o)?, &day.date)? {
                let source = ByteRef {
                    key: o.key.clone(),
                    offset: 0,
                    bytes: page.payload.len() as u64,
                    sha256: page.payload_sha256.clone(),
                    daily: Some(DailyPageRef {
                        date: day.date.clone(),
                        acquisition_id: page.acquisition_id.clone(),
                        ordinal: page.ordinal,
                    }),
                };
                let path = work
                    .join("daily-occurrences")
                    .join(occurrence_key(&page.acquisition_id, page.ordinal)?);
                if let Some(old) = read_json::<ByteRef>(&path)? {
                    if daily_page(layout, &old)? != page {
                        return Err("proof upgrade: conflicting daily occurrence".into());
                    }
                } else {
                    save(&path, &source)?;
                }
                let payload = work.join("payloads").join(&page.payload_sha256);
                if !payload.exists() {
                    save(&payload, &source)?;
                }
            }
        }
    }
    Ok(())
}

pub(super) fn occurrence_key(acquisition: &str, ordinal: u64) -> Result<String, String> {
    Ok(sha256_hex(&json_bytes(&(acquisition, ordinal))?))
}

type PageCache = Option<(PathBuf, String, BTreeMap<(String, u64), PageOccurrence>)>;
thread_local! { static DAILY_CACHE: std::cell::RefCell<PageCache> = const { std::cell::RefCell::new(None) }; }

/// Each worker owns at most one authenticated page day. Clear it at the independent proof
/// boundary, including failed jobs, so a converted-phase hook cannot certify cached bytes.
pub(super) struct CacheScope;
impl CacheScope {
    pub(super) fn clear() {
        DAILY_CACHE.with(|cache| *cache.borrow_mut() = None);
    }
    pub(super) fn enter() -> Self {
        Self::clear();
        Self
    }
}
impl Drop for CacheScope {
    fn drop(&mut self) {
        Self::clear();
    }
}

pub(super) fn daily_page(layout: &Layout, source: &ByteRef) -> Result<PageOccurrence, String> {
    let selector = source.daily.as_ref().ok_or("missing daily page selector")?;
    let path = layout.store.join(&source.key).canonicalize().map_err(err)?;
    DAILY_CACHE.with(|cache| {
        let mut cache = cache.borrow_mut();
        if cache
            .as_ref()
            .is_none_or(|(p, d, _)| p != &path || d != &selector.date)
        {
            let id = store::identify(&path)?;
            if object_key(&id.sha256) != source.key {
                return Err("daily page object digest mismatch".into());
            }
            let mut pages = BTreeMap::new();
            for page in daily::read_pages(&path, &selector.date)? {
                if pages
                    .insert((page.acquisition_id.clone(), page.ordinal), page)
                    .is_some()
                {
                    return Err("duplicate daily receipt occurrence".into());
                }
            }
            *cache = Some((path, selector.date.clone(), pages));
        }
        let page = cache
            .as_ref()
            .expect("loaded page day")
            .2
            .get(&(selector.acquisition_id.clone(), selector.ordinal))
            .ok_or("daily receipt occurrence is absent")?;
        if page.payload_sha256 != source.sha256 || page.payload.len() as u64 != source.bytes {
            return Err("daily receipt occurrence identity mismatch".into());
        }
        Ok(page.clone())
    })
}

pub(super) fn reuse_observations(
    local: &Store,
    family: &[GenerationManifest],
    date: &str,
    newest: &GenerationManifest,
    rows: &[LosslessRow],
) -> Result<Option<ObjectRecord>, String> {
    for m in family
        .iter()
        .filter(|m| m.price_representation == newest.price_representation)
    {
        let Some(day) = m.day_inventory.iter().find(|d| {
            d.family == DayFamily::Observations
                && d.date == date
                && d.rows == rows.len() as u64
                && d.object.is_some()
        }) else {
            continue;
        };
        let object = day_object(m, day)?;
        let (_, file) = verify::fetch(local, object, true)?;
        let path = &file.as_ref().expect("daily observations").path;
        let old: Vec<_> = match newest.price_representation {
            PriceRepresentation::IntegerUnits { scale } => daily::read_ticks(
                path,
                date,
                &InstrumentId {
                    broker: newest.broker.clone(),
                    provider_symbol: newest.provider_symbol.clone(),
                },
                scale,
            )?
            .into_iter()
            .map(LosslessRow::Tick)
            .collect(),
            PriceRepresentation::BinaryFloat64 => daily::read_bars(path, date)?
                .into_iter()
                .map(LosslessRow::Bar)
                .collect(),
        };
        if old
            .iter()
            .map(|r| format!("{r:?}"))
            .eq(rows.iter().map(|r| format!("{r:?}")))
        {
            return Ok(Some(object.clone()));
        }
    }
    Ok(None)
}

pub(super) fn reuse_pages(
    layout: &Layout,
    family: &[GenerationManifest],
    date: &str,
    pages: &[PageOccurrence],
) -> Result<Option<(DayInventoryEntry, ObjectRecord)>, String> {
    for m in family {
        let Some(day) = m.day_inventory.iter().find(|d| {
            d.family == DayFamily::Pages
                && d.date == date
                && d.rows == pages.len() as u64
                && d.object.is_some()
        }) else {
            continue;
        };
        let object = day_object(m, day)?;
        if daily::read_pages(&object_path(layout, object)?, date)? == pages {
            return Ok(Some((day.clone(), object.clone())));
        }
    }
    Ok(None)
}

fn day_object<'a>(
    m: &'a GenerationManifest,
    day: &DayInventoryEntry,
) -> Result<&'a ObjectRecord, String> {
    let path = day.logical_path()?;
    m.objects
        .iter()
        .find(|o| Some(&o.key) == day.object.as_ref() && o.path == path)
        .ok_or("missing daily object".into())
}

// Compare bounded, decoded days as ordered subsequences. Advancing the cursor for every
// match preserves multiplicity; a set or row count cannot prove repeated observations.
fn contains_rows(before: &Path, after: &Path) -> Result<(), String> {
    use parquet::file::reader::{FileReader, SerializedFileReader};
    let old = SerializedFileReader::new(File::open(before).map_err(err)?).map_err(err)?;
    let new = SerializedFileReader::new(File::open(after).map_err(err)?).map_err(err)?;
    let mut rows = new.get_row_iter(None).map_err(err)?;
    for row in old.get_row_iter(None).map_err(err)? {
        // Parquet's typed row Debug includes all columns and exact round-trip float text
        // (including signed zero). No field, ordering, or duplicate occurrence is omitted.
        let wanted = format!("{:?}", row.map_err(err)?);
        loop {
            let next = rows
                .next()
                .ok_or("proof upgrade: prior daily row was lost")?
                .map_err(err)?;
            if format!("{next:?}") == wanted {
                break;
            }
        }
    }
    Ok(())
}

fn preserve_days(
    layout: &Layout,
    old_days: &[DayInventoryEntry],
    old_objects: &[ObjectRecord],
    new_days: &[DayInventoryEntry],
    new_objects: &[ObjectRecord],
    family: DayFamily,
) -> Result<(), String> {
    for day in old_days
        .iter()
        .filter(|d| d.family == family && d.object.is_some())
    {
        let logical = day.logical_path()?;
        let old = old_objects
            .iter()
            .find(|o| o.path == logical && Some(&o.key) == day.object.as_ref())
            .ok_or("missing old partition")?;
        let new_day = new_days
            .iter()
            .find(|d| d.logical_path().is_ok_and(|p| p == logical))
            .ok_or("proof upgrade: prior daily partition was lost")?;
        let new = new_objects
            .iter()
            .find(|o| o.path == logical && Some(&o.key) == new_day.object.as_ref())
            .ok_or("missing new partition")?;
        let old_path = object_path(layout, old)?;
        let new_path = object_path(layout, new)?;
        if old.key != new.key {
            contains_rows(&old_path, &new_path)?;
        }
    }
    Ok(())
}

pub(super) fn preserve(
    job: &Job,
    bound: &Bound,
    layout: &Layout,
    state: &mut State,
    access: Access<'_>,
    work: &Path,
) -> Result<Option<lineage::ContinuationProof>, String> {
    let Some(previous_name) = &state.supersedes else {
        return Ok(None);
    };
    let previous: lineage::MigrationRecord =
        get(&layout.state.join("records").join(previous_name))?;
    if !previous.baseline_verified() {
        return Err("proof upgrade requires verified predecessor".into());
    }
    let local = layout.store();
    let baseline = read_manifest(&local, &state.dataset)?.0;
    let family = lineage::upgrade_family(
        &local,
        &layout.state.join("records"),
        previous_name,
        &job.id,
        &baseline.instrument,
        baseline.role,
        access,
    )?;
    for m in &family {
        verify::run_with(&layout.manifest_uri(&m.generation), access)?;
    }
    let ids: BTreeSet<_> = family.iter().map(|m| m.generation.clone()).collect();
    if ids != state.continuations.iter().cloned().collect() || !ids.contains(&previous.v2_root) {
        return Err("proof upgrade: continuation family changed after converted".into());
    }
    let latest = match lineage::newest_from(&local, family.iter().collect()) {
        Ok(latest) => latest,
        Err(error) if error.starts_with("archive: ambiguous daily lineage at equal coverage") => {
            // Old proof executables could omit a page-only descendant from supersession.
            // Repair may choose equivalent observation history only after proving every
            // former day's ordered rows; ordinary lineage selection remains strict.
            let mut ranked = Vec::new();
            for m in &family {
                let requested = lineage::read_coverage(&local, m)?
                    .acquisitions
                    .into_iter()
                    .flat_map(|a| a.requested)
                    .map(|r| time(&r.end))
                    .collect::<Result<Vec<_>, _>>()?
                    .into_iter()
                    .max()
                    .unwrap_or(i64::MIN);
                ranked.push((
                    time(&m.coverage.last_event_time)?,
                    m.row_count,
                    requested,
                    lineage::read_lineage(&local, m)?
                        .get("continuation")
                        .is_some(),
                    &m.generation,
                    m,
                ));
            }
            ranked.sort_by(|a, b| {
                (&a.0, &a.1, &a.2, &a.3, &a.4).cmp(&(&b.0, &b.1, &b.2, &b.3, &b.4))
            });
            let candidate = ranked.last().ok_or("proof upgrade: no continuation")?.5;
            for old in &family {
                preserve_days(
                    layout,
                    &old.day_inventory,
                    &old.objects,
                    &candidate.day_inventory,
                    &candidate.objects,
                    DayFamily::Observations,
                )?;
            }
            candidate.generation.clone()
        }
        Err(error) => return Err(error),
    };
    let newest = family
        .iter()
        .find(|m| m.generation == latest)
        .expect("selected daily");
    // Keep the exact strict v1 proof on `baseline`; every row of that proved baseline must
    // also survive in the selected daily history before it can replace the continuation.
    preserve_days(
        layout,
        &baseline.day_inventory,
        &baseline.objects,
        &newest.day_inventory,
        &newest.objects,
        DayFamily::Observations,
    )?;
    let mut manifest = newest.clone();
    manifest
        .objects
        .retain(|o| o.path.starts_with("observations/"));
    manifest
        .day_inventory
        .retain(|d| d.family == DayFamily::Observations);
    let mut dates = BTreeSet::new();
    for m in family.iter().chain(std::iter::once(&baseline)) {
        dates.extend(
            m.day_inventory
                .iter()
                .filter(|d| d.family == DayFamily::Pages)
                .map(|d| d.date.clone()),
        );
    }
    // Union all page identities, including diagnostic-only sibling snapshots. Assemble at
    // most one UTC day; prefer any exact existing partition over invoking the encoder.
    for date in dates {
        let mut pages = BTreeMap::new();
        let mut reusable = Vec::new();
        for m in family.iter().chain(std::iter::once(&baseline)) {
            if let Some(d) = m
                .day_inventory
                .iter()
                .find(|d| d.family == DayFamily::Pages && d.date == date && d.object.is_some())
            {
                let o = day_object(m, d)?;
                let rows = daily::read_pages(&object_path(layout, o)?, &date)?;
                reusable.push((d, o, rows.len()));
                for p in rows {
                    let key = (p.acquisition_id.clone(), p.ordinal);
                    if let Some(old) = pages.insert(key, p.clone())
                        && old != p
                    {
                        return Err("proof upgrade: conflicting page occurrence metadata".into());
                    }
                }
            }
        }
        if let Some((d, o, _)) = reusable.iter().find(|(_, _, n)| *n == pages.len()) {
            manifest.day_inventory.push((*d).clone());
            manifest.objects.push((*o).clone());
        } else if !pages.is_empty() {
            let path = work.join("continuation-pages.parquet");
            let data = daily::write_pages(&path, &date, [pages.into_values()])?;
            let mut day = new_day(&date, DayFamily::Pages);
            let object = retain_file(&local, &path, &day.logical_path()?, ObjectRole::Source)?;
            set_data(&mut day, &data, &object);
            manifest.day_inventory.push(day);
            manifest.objects.push(object);
        }
    }
    let mut provenance = lineage::read_lineage(&local, &baseline)?;
    if let Some(continuation) = lineage::read_lineage(&local, newest)?.get("continuation") {
        provenance["continuation"] = continuation.clone();
    }
    provenance["parent_generation"] = json!(latest);
    let mut ancestors = ids;
    ancestors.insert(baseline.generation.clone());
    provenance["ancestors"] = json!(ancestors);
    provenance["supersedes_generation"] = json!(previous.v2_root);
    // The latest cumulative coverage includes requested/verified ranges and shortfalls.
    // Rebind only page inventory claims if the census adds a diagnostic occurrence.
    let mut coverage = lineage::read_coverage(&local, newest)?;
    for source in family.iter().chain(std::iter::once(&baseline)) {
        for claim in lineage::read_coverage(&local, source)?.acquisitions {
            if let Some(existing) = coverage
                .acquisitions
                .iter()
                .find(|a| a.acquisition_id == claim.acquisition_id)
            {
                if existing != &claim {
                    return Err("proof upgrade: conflicting acquisition coverage".into());
                }
            } else {
                coverage.acquisitions.push(claim);
            }
        }
    }
    lineage::upgrade_page_coverage(&mut coverage, &mut manifest)?;
    manifest
        .objects
        .push(retain_json(&local, work, fetch::COVERAGE_PATH, &coverage)?);
    manifest.objects.push(retain_json(
        &local,
        work,
        lineage::LINEAGE_PATH,
        &provenance,
    )?);
    lineage::identify(&mut manifest);
    coverage.check_manifest(&manifest)?;
    let path = work.join("continuation-ready.json");
    fs::write(&path, manifest.to_json()).map_err(err)?;
    local.put_new(&manifest.key(), &path, &store::identify(&path)?)?;
    let audited = audit::audit(
        &bound.core,
        &local.uri(&manifest.key()),
        &local,
        &local,
        access,
    )?;
    let stream = read_stream(layout, &audited.generation)?;
    verify::run_with(&layout.manifest_uri(&manifest.generation), access)?;
    verify::run_with(&layout.manifest_uri(&stream.generation), access)?;
    let mut closures = Vec::new();
    for old in family.iter().chain(std::iter::once(&baseline)) {
        preserve_days(
            layout,
            &old.day_inventory,
            &old.objects,
            &manifest.day_inventory,
            &manifest.objects,
            DayFamily::Observations,
        )?;
        // Read the published output independently of the union builder before claiming
        // preservation. Page order is canonical acquisition/ordinal order, so the same
        // ordered-row proof checks payloads, metadata, identity and multiplicity.
        preserve_days(
            layout,
            &old.day_inventory,
            &old.objects,
            &manifest.day_inventory,
            &manifest.objects,
            DayFamily::Pages,
        )?;
        let mut old_streams = Vec::new();
        for generation in local.list_manifests()? {
            access.lookup(&generation)?;
            let bytes = fs::read(layout.store.join(manifest_key(&generation))).map_err(err)?;
            if verify::manifest_kind(&bytes)?.as_deref()
                != Some(binary_alpha_engine::stream::STREAM_MANIFEST_KIND)
            {
                continue;
            }
            let prior = StreamManifest::from_json(&bytes)?;
            if prior.source_generation != old.generation {
                continue;
            }
            if prior.definition != stream.definition {
                return Err("proof upgrade: prior stream definition differs".into());
            }
            verify::run_with(&layout.manifest_uri(&generation), access)?;
            preserve_days(
                layout,
                &prior.day_inventory,
                &prior.objects,
                &stream.day_inventory,
                &stream.objects,
                DayFamily::Candles,
            )?;
            old_streams.push(generation);
        }
        closures.push(lineage::PreservedClosure {
            dataset: old.generation.clone(),
            streams: old_streams,
        });
    }
    if let Some(prior) = previous.continuation_proof() {
        closures.extend(prior.closures);
    }
    closures.sort_by(|a, b| a.dataset.cmp(&b.dataset));
    closures.dedup();
    let proof = lineage::ContinuationProof {
        from_root: previous.v2_root,
        to_root: manifest.generation.clone(),
        to_stream: stream.generation.clone(),
        observations: true,
        pages: true,
        candles: true,
        closures,
    };
    state.occurrences = manifest
        .day_inventory
        .iter()
        .filter(|d| d.family == DayFamily::Pages)
        .map(|d| d.rows)
        .sum();
    state.dataset = manifest.generation;
    state.stream = stream.generation;
    Ok(Some(proof))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn preservation_requires_ordered_row_multiplicity() {
        let dir = std::env::temp_dir().join(format!("retire-preservation-{}", std::process::id()));
        fs::create_dir(&dir).unwrap();
        let day = "2025-08-11";
        let id = InstrumentId {
            broker: "deriv".to_string().try_into().unwrap(),
            provider_symbol: "frxEURUSD".to_string().try_into().unwrap(),
        };
        let tick = binary_alpha_engine::market::Tick {
            event_time_micros: day_bounds(day).unwrap().0,
            price_units: 123,
        };
        let before = dir.join("before.parquet");
        let after = dir.join("after.parquet");
        daily::write_ticks(&before, day, &id, 5.try_into().unwrap(), [vec![tick, tick]]).unwrap();
        daily::write_ticks(&after, day, &id, 5.try_into().unwrap(), [vec![tick]]).unwrap();
        assert_eq!(
            contains_rows(&before, &after).unwrap_err(),
            "proof upgrade: prior daily row was lost"
        );
        assert!(contains_rows(&after, &before).is_ok());
        fs::remove_dir_all(dir).unwrap();
    }
}

//! `binary-alpha data import`: enumerate the declared inventory, retain every input once, validate
//! and normalize each dataset from its retained bytes, publish it, and commit one ready manifest
//! last.

use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::time::Instant;

use binary_alpha_engine::config::{PublicationUri, Source, relative_path};
use binary_alpha_engine::dataset::{
    Capability, Coverage, DatasetRole, GenerationManifest, Input, IntervalContract,
    MANIFEST_SCHEMA_VERSION, NativeGranularity, ObjectRecord, ObjectRole, PriceRepresentation,
    SourceKind, TimeUnit, generation_id, manifest_key, object_key,
};
use binary_alpha_engine::market::{
    BrokerId, InstrumentId, PriceScale, ProviderSymbol, TICK_HEADER, Tick, parse_event_time_micros,
    parse_tick_line,
};
use serde::Deserialize;
use sha2::Digest;

use crate::archive::{self, BarExpectation, DataSummary, TICK_OBJECT_PATH};
use crate::parallel;
use crate::store::{self, ObjectIdentity, Put, Store};

/// The producing code revision, captured by `build.rs`.
pub const CODE_REVISION: &str = env!("BINARY_ALPHA_CODE_REVISION");

/// The period of the one bar interval contract the engine admits.
const BAR_PERIOD_S: u16 = 5;

/// One input file of a dataset before it is retained.
struct PlannedFile {
    role: ObjectRole,
    /// Relative to the dataset root; collection-level files use the `collection/` prefix.
    path: String,
    absolute: PathBuf,
}

/// Source-specific validation inputs.
enum Data {
    Ticks {
        scale: PriceScale,
        rows: TickRows,
    },
    Bars {
        expectation: Box<BarExpectation>,
        /// Listed archive files in manifest order, with their recorded identities and rows.
        listed: Vec<ListedFile>,
        canonical_rows: u64,
    },
}

/// Where a tick dataset's rows come from.
enum TickRows {
    /// One native three-column file whose rows carry this symbol.
    Csv { source_symbol: ProviderSymbol },
    /// Daily archive files in date order, aligned with the leading source objects.
    Daily { days: Vec<Day> },
}

/// One daily archive file and what its metadata records about it.
struct Day {
    /// The Parquet file's object path.
    path: String,
    /// The first microsecond of the file's calendar day.
    start_micros: i64,
    /// The row count the metadata records.
    ticks: u64,
}

/// One dataset to publish; `files` lists source objects in declared order, then provenance.
struct Dataset {
    instrument: InstrumentId,
    role: DatasetRole,
    files: Vec<PlannedFile>,
    /// Object paths whose bytes were parsed while planning, with the SHA-256 of those bytes; the
    /// retained bytes must carry the same identity.
    parsed: Vec<(String, String)>,
    data: Data,
}

/// Runs the import described by the configuration at `config_path`, writing one report line to
/// `out` as each dataset is published.
pub fn run(config_path: &Path, out: &mut dyn Write) -> Result<(), String> {
    let config = crate::load_config(config_path)?;
    let base = config_path.parent().unwrap_or(Path::new("."));
    let sources = config
        .import
        .as_ref()
        .filter(|import| !import.sources.is_empty())
        .map(|import| &import.sources)
        .ok_or("import.sources must declare at least one entry")?;
    let historical_dir = base.join(config.storage.historical_data_dir.as_path());
    let mut protected = vec![resolve(&historical_dir)?];
    if let PublicationUri::Filesystem(path) = &config.storage.publication_uri {
        protected.push(resolve(path)?);
    }
    let mut roots: Vec<PathBuf> = Vec::new();
    let mut datasets = Vec::new();
    for source in sources {
        let root = canonical(&base.join(source.path()))?;
        if roots.contains(&root) {
            return Err(format!("source {} is declared twice", root.display()));
        }
        if protected.iter().any(|dir| root.starts_with(dir)) {
            return Err(format!(
                "source {} lies inside the historical-data folder or the file destination",
                root.display()
            ));
        }
        datasets.extend(plan(source, &root, &protected)?);
        roots.push(root);
    }
    for dir in &protected {
        fs::create_dir_all(dir)
            .map_err(|error| format!("cannot create {}: {error}", dir.display()))?;
    }
    let local = Store::filesystem(&historical_dir);
    let destination = Store::open(&config.storage.publication_uri)?;
    let config_hash = config.content_hash();
    for dataset in &datasets {
        let line = publish(dataset, &local, &destination, &config_hash)?;
        writeln!(out, "{line}")
            .and_then(|()| out.flush())
            .map_err(|error| format!("cannot write the report: {error}"))?;
    }
    Ok(())
}

fn canonical(path: &Path) -> Result<PathBuf, String> {
    path.canonicalize()
        .map_err(|error| format!("cannot resolve {}: {error}", path.display()))
}

/// The absolute form of a path that may not exist yet: its longest existing ancestor is
/// canonicalized and the remaining components are appended unchanged.
fn resolve(path: &Path) -> Result<PathBuf, String> {
    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()
            .map_err(|error| format!("cannot resolve the working directory: {error}"))?
            .join(path)
    };
    let mut remaining = Vec::new();
    let mut existing = absolute.as_path();
    while !existing.exists() {
        let name = existing
            .file_name()
            .ok_or_else(|| format!("cannot resolve {}", path.display()))?;
        remaining.push(name.to_owned());
        existing = existing
            .parent()
            .ok_or_else(|| format!("cannot resolve {}", path.display()))?;
    }
    let mut resolved = canonical(existing)?;
    resolved.extend(remaining.iter().rev());
    Ok(resolved)
}

/// A declared file must be a regular file, never a symbolic link into an undeclared location.
fn regular_file(path: &Path) -> Result<(), String> {
    let metadata = fs::symlink_metadata(path)
        .map_err(|error| format!("cannot inspect {}: {error}", path.display()))?;
    if metadata.is_symlink() {
        return Err(format!(
            "{} is a symbolic link; declared inventories contain only regular files",
            path.display()
        ));
    }
    if !metadata.is_file() {
        return Err(format!("{} is not a regular file", path.display()));
    }
    Ok(())
}

fn plan(source: &Source, root: &Path, protected: &[PathBuf]) -> Result<Vec<Dataset>, String> {
    match source {
        Source::TickCsv {
            broker,
            role,
            provider_symbol,
            source_symbol,
            price_scale,
            ..
        } => {
            regular_file(root)?;
            let name = object_path(
                &root
                    .file_name()
                    .ok_or_else(|| format!("{} has no file name", root.display()))?
                    .to_string_lossy(),
            )?;
            Ok(vec![Dataset {
                instrument: InstrumentId {
                    broker: broker.clone(),
                    provider_symbol: provider_symbol.clone(),
                },
                role: *role,
                files: vec![PlannedFile {
                    role: ObjectRole::Source,
                    path: name,
                    absolute: root.to_path_buf(),
                }],
                parsed: vec![],
                data: Data::Ticks {
                    scale: *price_scale,
                    rows: TickRows::Csv {
                        source_symbol: source_symbol.clone(),
                    },
                },
            }])
        }
        Source::BarParquetCollection {
            broker,
            role,
            manifest,
            provenance,
            ..
        } => plan_collection(
            root,
            broker,
            *role,
            manifest,
            provenance.as_deref().unwrap_or(&[]),
            protected,
        ),
        Source::TickParquetDaily {
            broker,
            role,
            price_scale,
            instruments,
            ..
        } => plan_daily_archive(root, broker, *role, *price_scale, instruments, protected),
    }
}

/// The fields of the observed collection manifest that the importer consumes.
#[derive(Deserialize)]
struct Collection {
    assets: BTreeMap<String, Asset>,
    server_timestamp_offset_seconds: i64,
}

#[derive(Deserialize)]
struct Asset {
    asset: String,
    asset_root: PathBuf,
    dataset_root: PathBuf,
    expected_symbol_id: Option<i32>,
    symbol_id: Option<i32>,
    canonical_rows: u64,
    parquet_files: Vec<ListedFile>,
    interval_contract: BTreeMap<String, serde_json::Value>,
    interval_contract_provenance: String,
}

#[derive(Deserialize, Clone)]
struct ListedFile {
    path: String,
    bytes: u64,
    rows: u64,
    sha256: String,
}

fn plan_collection(
    root: &Path,
    broker: &BrokerId,
    role: DatasetRole,
    manifest: &Path,
    provenance: &[PathBuf],
    protected: &[PathBuf],
) -> Result<Vec<Dataset>, String> {
    let manifest_path = contained_file(root, manifest, protected)?;
    let text = fs::read(&manifest_path)
        .map_err(|error| format!("cannot read {}: {error}", manifest_path.display()))?;
    let manifest_sha256 = binary_alpha_engine::hex(&sha2::Sha256::digest(&text));
    let collection: Collection = serde_json::from_slice(&text).map_err(|error| {
        format!(
            "{} is not a collection manifest: {error}",
            manifest_path.display()
        )
    })?;
    let mut collection_files = Vec::new();
    for relative in std::iter::once(manifest).chain(provenance.iter().map(PathBuf::as_path)) {
        let absolute = contained_file(root, relative, protected)?;
        collection_files.push(PlannedFile {
            role: ObjectRole::Provenance,
            path: object_path(&format!("collection/{}", relative.display()))?,
            absolute,
        });
    }
    let mut datasets = Vec::with_capacity(collection.assets.len());
    for asset in collection.assets.values() {
        let asset_root = contained_dir(root, &asset.asset_root, protected)?;
        let dataset_root = canonical(&root.join(&asset.dataset_root))?;
        let dataset_prefix = dataset_root.strip_prefix(&asset_root).map_err(|_| {
            format!(
                "dataset root {} is outside its asset root {}",
                dataset_root.display(),
                asset_root.display()
            )
        })?;
        let mut listed = Vec::with_capacity(asset.parquet_files.len());
        let mut files = Vec::new();
        for file in &asset.parquet_files {
            let relative = relative_path(&file.path)
                .map_err(|reason| format!("{}: {reason}", manifest_path.display()))?;
            let path = object_path(&dataset_prefix.join(relative).to_string_lossy())?;
            if listed.iter().any(|listed: &ListedFile| listed.path == path) {
                return Err(format!("{} lists {path} twice", manifest_path.display()));
            }
            let absolute = asset_root.join(&path);
            regular_file(&absolute)
                .map_err(|reason| format!("{} lists {path}: {reason}", manifest_path.display()))?;
            listed.push(ListedFile {
                path: path.clone(),
                ..file.clone()
            });
            files.push(PlannedFile {
                role: ObjectRole::Source,
                path,
                absolute,
            });
        }
        for absolute in walk(&asset_root)? {
            let path = object_path(
                &absolute
                    .strip_prefix(&asset_root)
                    .expect("walked beneath the asset root")
                    .to_string_lossy(),
            )?;
            if !listed.iter().any(|listed| listed.path == path) {
                files.push(PlannedFile {
                    role: ObjectRole::Provenance,
                    path,
                    absolute,
                });
            }
        }
        files.extend(collection_files.iter().map(|file| PlannedFile {
            role: file.role,
            path: file.path.clone(),
            absolute: file.absolute.clone(),
        }));
        for (index, file) in files.iter().enumerate() {
            if files[..index]
                .iter()
                .any(|earlier| earlier.path == file.path)
            {
                return Err(format!(
                    "{}: object path {} is inventoried twice",
                    asset.asset, file.path
                ));
            }
        }
        let interval = five_second_contract(asset)?;
        let symbol_id = asset
            .expected_symbol_id
            .or(asset.symbol_id)
            .ok_or_else(|| {
                format!(
                    "{}: the collection manifest records no symbol identifier for {}",
                    manifest_path.display(),
                    asset.asset
                )
            })?;
        datasets.push(Dataset {
            instrument: InstrumentId {
                broker: broker.clone(),
                provider_symbol: ProviderSymbol::try_from(asset.asset.clone())?,
            },
            role,
            files,
            parsed: vec![(collection_files[0].path.clone(), manifest_sha256.clone())],
            data: Data::Bars {
                expectation: Box::new(BarExpectation {
                    symbol: asset.asset.clone(),
                    symbol_id: Some(symbol_id),
                    period_s: BAR_PERIOD_S,
                    server_offset_s: Some(collection.server_timestamp_offset_seconds),
                    metadata_required: asset.interval_contract_provenance
                        == "manifest_and_parquet_metadata",
                    interval,
                }),
                listed,
                canonical_rows: asset.canonical_rows,
            },
        });
    }
    Ok(datasets)
}

/// The fields of a daily metadata file that the importer consumes.
#[derive(Deserialize)]
struct DailyMetadata {
    symbol: String,
    date: String,
    calendar: String,
    ticks: u64,
}

/// The Parquet file and the metadata file of one calendar day, as planned objects.
#[derive(Default)]
struct DayFiles {
    parquet: Option<PlannedFile>,
    metadata: Option<PlannedFile>,
}

/// Plans one generation per listed directory of a daily tick archive: the directory's Parquet
/// days are source objects and its metadata files are provenance, both in date order, and the
/// provider symbol is the one every metadata file records.
fn plan_daily_archive(
    root: &Path,
    broker: &BrokerId,
    role: DatasetRole,
    scale: PriceScale,
    instruments: &[String],
    protected: &[PathBuf],
) -> Result<Vec<Dataset>, String> {
    let mut datasets = Vec::with_capacity(instruments.len());
    for name in instruments {
        let dir = contained_dir(root, Path::new(name), protected)?;
        let listing = |error| format!("cannot list {}: {error}", dir.display());
        let mut days: BTreeMap<String, DayFiles> = BTreeMap::new();
        for entry in fs::read_dir(&dir).map_err(listing)? {
            let entry = entry.map_err(listing)?;
            let path = entry.path();
            regular_file(&path)?;
            let file_name = entry.file_name().to_string_lossy().into_owned();
            let Some((date, is_parquet)) = daily_file(name, &file_name) else {
                return Err(format!(
                    "{} is not `{name}_YYYY-MM-DD_ticks.parquet` or `{name}_YYYY-MM-DD_ticks.meta.json`",
                    path.display()
                ));
            };
            let day = days.entry(date.to_string()).or_default();
            let (slot, role) = if is_parquet {
                (&mut day.parquet, ObjectRole::Source)
            } else {
                (&mut day.metadata, ObjectRole::Provenance)
            };
            *slot = Some(PlannedFile {
                role,
                path: file_name,
                absolute: path,
            });
        }
        let mut symbol: Option<String> = None;
        let mut sources = Vec::new();
        let mut provenance = Vec::new();
        let mut parsed = Vec::new();
        let mut decode = Vec::new();
        for (date, DayFiles { parquet, metadata }) in days {
            let start_micros = parse_event_time_micros(&format!("{date}T00:00:00Z"))
                .map_err(|_| format!("{}: `{date}` is not a calendar date", dir.display()))?;
            let Some(metadata) = metadata else {
                return Err(format!(
                    "{} has no metadata file",
                    parquet
                        .expect("a day has at least one file")
                        .absolute
                        .display()
                ));
            };
            let text = fs::read(&metadata.absolute)
                .map_err(|error| format!("cannot read {}: {error}", metadata.absolute.display()))?;
            let record: DailyMetadata = serde_json::from_slice(&text).map_err(|error| {
                format!(
                    "{} is not a daily metadata file: {error}",
                    metadata.absolute.display()
                )
            })?;
            if record.calendar != "UTC" || record.date != date {
                return Err(format!(
                    "{} records calendar `{}` and date `{}`, expected `UTC` and `{date}`",
                    metadata.absolute.display(),
                    record.calendar,
                    record.date
                ));
            }
            match &symbol {
                Some(symbol) if *symbol != record.symbol => {
                    return Err(format!(
                        "{} records symbol `{}`, but the directory's earlier days record `{symbol}`",
                        metadata.absolute.display(),
                        record.symbol
                    ));
                }
                Some(_) => {}
                None => symbol = Some(record.symbol),
            }
            parsed.push((
                metadata.path.clone(),
                binary_alpha_engine::hex(&sha2::Sha256::digest(&text)),
            ));
            match parquet {
                Some(file) => {
                    decode.push(Day {
                        path: file.path.clone(),
                        start_micros,
                        ticks: record.ticks,
                    });
                    sources.push(file);
                }
                None if record.ticks != 0 => {
                    return Err(format!(
                        "{} records {} ticks but the day has no Parquet file",
                        metadata.absolute.display(),
                        record.ticks
                    ));
                }
                None => {}
            }
            provenance.push(metadata);
        }
        if sources.is_empty() {
            return Err(format!("{} holds no daily Parquet file", dir.display()));
        }
        let symbol = symbol.expect("every Parquet day has a metadata file");
        sources.extend(provenance);
        datasets.push(Dataset {
            instrument: InstrumentId {
                broker: broker.clone(),
                provider_symbol: ProviderSymbol::try_from(symbol)
                    .map_err(|reason| format!("{}: {reason}", dir.display()))?,
            },
            role,
            files: sources,
            parsed,
            data: Data::Ticks {
                scale,
                rows: TickRows::Daily { days: decode },
            },
        });
    }
    Ok(datasets)
}

/// The date field of `NAME_YYYY-MM-DD_ticks.parquet` (`true`) or `NAME_YYYY-MM-DD_ticks.meta.json`
/// (`false`), which the caller validates as a calendar date; any other name is `None`.
fn daily_file<'a>(name: &str, file_name: &'a str) -> Option<(&'a str, bool)> {
    let rest = file_name.strip_prefix(name)?.strip_prefix('_')?;
    let (date, suffix) = rest.split_at_checked(10)?;
    match suffix {
        "_ticks.parquet" => Some((date, true)),
        "_ticks.meta.json" => Some((date, false)),
        _ => None,
    }
}

/// A declared directory resolved through every symbolic link: it must still lie beneath `root`
/// and neither contain nor lie inside either destination.
fn contained_dir(root: &Path, relative: &Path, protected: &[PathBuf]) -> Result<PathBuf, String> {
    let absolute = canonical(&root.join(relative))?;
    if !absolute.starts_with(root) {
        return Err(format!(
            "{} resolves to {}, outside the root {}",
            relative.display(),
            absolute.display(),
            root.display()
        ));
    }
    if protected
        .iter()
        .any(|dir| dir.starts_with(&absolute) || absolute.starts_with(dir))
    {
        return Err(format!(
            "{} resolves to {}, which overlaps the historical-data folder or the file destination",
            relative.display(),
            absolute.display()
        ));
    }
    Ok(absolute)
}

/// A declared collection-level file resolved through every symbolic link: it must be a regular
/// file that still lies beneath the canonical collection root and outside both destinations.
fn contained_file(root: &Path, relative: &Path, protected: &[PathBuf]) -> Result<PathBuf, String> {
    let absolute = canonical(&root.join(relative))?;
    if !absolute.starts_with(root) {
        return Err(format!(
            "{} resolves to {}, outside the collection root {}",
            relative.display(),
            absolute.display(),
            root.display()
        ));
    }
    if protected.iter().any(|dir| absolute.starts_with(dir)) {
        return Err(format!(
            "{} resolves to {}, inside the historical-data folder or the file destination",
            relative.display(),
            absolute.display()
        ));
    }
    regular_file(&absolute)?;
    Ok(absolute)
}

/// The asset's declared interval contract, which the engine requires to be the approved one.
fn five_second_contract(asset: &Asset) -> Result<IntervalContract, String> {
    let declared = &asset.interval_contract;
    let text = |key: &str| {
        declared
            .get(key)
            .and_then(|value| value.as_str())
            .map(str::to_string)
            .ok_or_else(|| format!("{}: interval contract `{key}` is missing", asset.asset))
    };
    let contract = IntervalContract {
        closed: text("closed")?,
        frequency: text("frequency")?,
        interval: text("interval")?,
        label: text("label")?,
        offset_seconds: declared
            .get("offset_seconds")
            .and_then(|value| value.as_i64())
            .ok_or_else(|| {
                format!(
                    "{}: interval contract `offset_seconds` is missing",
                    asset.asset
                )
            })?,
        origin: text("origin")?,
        timestamp_semantics: text("timestamp_semantics")?,
        provenance: asset.interval_contract_provenance.clone(),
    };
    contract
        .validate()
        .map_err(|reason| format!("{}: {reason}", asset.asset))?;
    Ok(contract)
}

/// An object path free of control characters, so manifest lines stay unambiguous.
fn object_path(path: &str) -> Result<String, String> {
    if path.bytes().any(|byte| byte.is_ascii_control()) {
        return Err(format!(
            "object path `{}` contains a control character",
            path.escape_default()
        ));
    }
    Ok(path.to_string())
}

/// Every regular file beneath `root` in sorted order; symbolic links are rejected so nothing
/// outside the declared root is ever opened.
fn walk(root: &Path) -> Result<Vec<PathBuf>, String> {
    let mut files = Vec::new();
    let mut pending = vec![root.to_path_buf()];
    while let Some(dir) = pending.pop() {
        let entries: Vec<PathBuf> = fs::read_dir(&dir)
            .map_err(|error| format!("cannot list {}: {error}", dir.display()))?
            .map(|entry| entry.map(|entry| entry.path()))
            .collect::<Result<_, _>>()
            .map_err(|error| format!("cannot list {}: {error}", dir.display()))?;
        for path in entries {
            let metadata = fs::symlink_metadata(&path)
                .map_err(|error| format!("cannot inspect {}: {error}", path.display()))?;
            if metadata.is_symlink() {
                return Err(format!(
                    "{} is a symbolic link; declared inventories contain only regular files",
                    path.display()
                ));
            } else if metadata.is_dir() {
                pending.push(path);
            } else if metadata.is_file() {
                files.push(path);
            }
        }
    }
    files.sort();
    Ok(files)
}

/// Retains, validates, publishes, and commits one dataset generation, returning its report
/// line. Every run does the complete work from the retained copies; a ready manifest already at
/// the destination must describe exactly this result, and its committed bytes are mirrored.
fn publish(
    dataset: &Dataset,
    local: &Store,
    destination: &Store,
    config_hash: &str,
) -> Result<String, String> {
    let started = Instant::now();
    let mut identities: Vec<ObjectIdentity> =
        parallel::map(&dataset.files, |file| store::identify(&file.absolute))
            .into_iter()
            .collect::<Result<_, _>>()?;
    let mut objects: Vec<ObjectRecord> = dataset
        .files
        .iter()
        .zip(&identities)
        .map(|(file, identity)| record(file.role, &file.path, identity))
        .collect();
    for (path, sha256) in &dataset.parsed {
        if objects
            .iter()
            .any(|object| object.path == *path && object.sha256 != *sha256)
        {
            return Err(format!(
                "{path} changed after its expectations were read; nothing was published"
            ));
        }
    }
    let hashed = started.elapsed();
    let (source_kind, scale) = match &dataset.data {
        Data::Ticks {
            scale,
            rows: TickRows::Csv { .. },
        } => (SourceKind::TickCsv, Some(*scale)),
        Data::Ticks {
            scale,
            rows: TickRows::Daily { .. },
        } => (SourceKind::TickParquetDaily, Some(*scale)),
        Data::Bars { .. } => (SourceKind::BarParquet, None),
    };
    let generation = generation_id(
        &dataset.instrument,
        source_kind,
        dataset.role,
        scale,
        &objects,
    );
    let key = manifest_key(&generation);

    let retaining = Instant::now();
    for (file, identity) in dataset.files.iter().zip(&identities) {
        local.put_new(&object_key(&identity.sha256), &file.absolute, identity)?;
    }
    let retained = retaining.elapsed();
    let retained_path = |object: &ObjectRecord| {
        local
            .local_path(&object.key)
            .expect("the retained folder is a filesystem store")
    };

    let validating = Instant::now();
    let (summary, native_granularity, time_unit, price_representation, interval, capability) =
        match &dataset.data {
            Data::Ticks { scale, rows } => {
                let temporary = temporary_path(local, &generation)?;
                let summary = match rows {
                    TickRows::Csv { source_symbol } => archive::write_ticks(
                        &temporary,
                        &dataset.instrument,
                        *scale,
                        tick_rows(&retained_path(&objects[0]), source_symbol, *scale)?,
                    )?,
                    TickRows::Daily { days } => {
                        let paths: Vec<PathBuf> =
                            objects[..days.len()].iter().map(retained_path).collect();
                        archive::write_ticks(
                            &temporary,
                            &dataset.instrument,
                            *scale,
                            daily_tick_rows(days, &paths, *scale),
                        )?
                    }
                };
                let identity = store::identify(&temporary)?;
                local.put_new(&object_key(&identity.sha256), &temporary, &identity)?;
                fs::remove_file(&temporary)
                    .map_err(|error| format!("cannot remove {}: {error}", temporary.display()))?;
                objects.push(record(ObjectRole::Normalized, TICK_OBJECT_PATH, &identity));
                identities.push(identity);
                (
                    summary,
                    NativeGranularity::Tick,
                    TimeUnit::Microsecond,
                    PriceRepresentation::IntegerUnits { scale: *scale },
                    None,
                    Capability::Ticks,
                )
            }
            Data::Bars {
                expectation,
                listed,
                canonical_rows,
            } => {
                let paths: Vec<PathBuf> =
                    objects[..listed.len()].iter().map(retained_path).collect();
                let (summary, all_embedded) = validate_bars(
                    &dataset.instrument,
                    &objects[..listed.len()],
                    &paths,
                    expectation,
                    listed,
                    *canonical_rows,
                )?;
                let interval = IntervalContract {
                    provenance: if all_embedded {
                        "parquet_metadata".to_string()
                    } else {
                        expectation.interval.provenance.clone()
                    },
                    ..expectation.interval.clone()
                };
                (
                    summary,
                    NativeGranularity::Bar {
                        period_seconds: expectation.period_s,
                    },
                    TimeUnit::Second,
                    PriceRepresentation::BinaryFloat64,
                    Some(interval),
                    Capability::Bars,
                )
            }
        };
    let validated = validating.elapsed();

    let publishing = Instant::now();
    let mut reused = 0;
    for (object, identity) in objects.iter_mut().zip(&identities) {
        let put = destination.put_new(&object.key, &retained_path(object), identity)?;
        if let Put::Reused(_) = put {
            reused += 1;
        }
        object.crc32c = put.object().crc32c;
        object.generation = put.object().generation;
    }
    let (first_event_time, last_event_time) = archive::coverage(&summary)?;
    let manifest = GenerationManifest {
        schema_version: MANIFEST_SCHEMA_VERSION,
        generation: generation.clone(),
        broker: dataset.instrument.broker.clone(),
        provider_symbol: dataset.instrument.provider_symbol.clone(),
        instrument: dataset.instrument.to_string(),
        role: dataset.role,
        source_kind,
        native_granularity,
        time_unit,
        price_representation,
        coverage: Coverage {
            first_event_time,
            last_event_time,
        },
        row_count: summary.rows,
        capabilities: vec![capability],
        config_hash: config_hash.to_string(),
        code_revision: CODE_REVISION.to_string(),
        inputs: dataset
            .files
            .iter()
            .zip(&identities)
            .map(|(file, identity)| Input {
                path: file.absolute.to_string_lossy().into_owned(),
                bytes: identity.bytes,
                sha256: identity.sha256.clone(),
            })
            .collect(),
        interval,
        objects,
    };
    let report = format!(
        "published {} {} generation {generation} rows {} objects {} reused {reused}",
        dataset.instrument,
        dataset.role,
        manifest.row_count,
        manifest.objects.len()
    );
    let committed = match destination.head(&key)? {
        Some(_) => {
            let mut bytes = Vec::new();
            destination.read_to(&key, None, &mut bytes)?;
            let committed = GenerationManifest::from_json(&bytes)
                .map_err(|error| format!("{}: {error}", destination.uri(&key)))?;
            if !same_result(&committed, &manifest, &identities) {
                return Err(format!(
                    "{} records a different generation, object set, row count, or coverage than this import produced",
                    destination.uri(&key)
                ));
            }
            bytes
        }
        None => manifest.to_json(),
    };
    let temporary = temporary_path(local, &format!("manifest-{generation}"))?;
    fs::write(&temporary, &committed)
        .map_err(|error| format!("cannot write {}: {error}", temporary.display()))?;
    let identity = store::identify(&temporary)?;
    let put = destination.put_new(&key, &temporary, &identity)?;
    local.put_new(&key, &temporary, &identity)?;
    fs::remove_file(&temporary)
        .map_err(|error| format!("cannot remove {}: {error}", temporary.display()))?;
    let published = publishing.elapsed();
    Ok(match put {
        Put::Reused(_) => format!("{report} (already published)"),
        Put::Created(_) => format!(
            "{report} [hash {:.3}s retain {:.3}s validate {:.3}s publish {:.3}s]",
            hashed.as_secs_f64(),
            retained.as_secs_f64(),
            validated.as_secs_f64(),
            published.as_secs_f64()
        ),
    })
}

/// A committed manifest describes this import's result when it names the same generation, role,
/// rows, coverage, and interval, and the same objects by role, path, identity, and size, with
/// every recorded checksum matching the bytes, and every checksum or generation the destination
/// reports now matching the record (a store that reports neither leaves them as provenance).
fn same_result(
    committed: &GenerationManifest,
    fresh: &GenerationManifest,
    identities: &[ObjectIdentity],
) -> bool {
    committed.generation == fresh.generation
        && committed.role == fresh.role
        && committed.row_count == fresh.row_count
        && committed.coverage == fresh.coverage
        && committed.interval == fresh.interval
        && same_objects(&committed.objects, &fresh.objects, identities)
}

/// Committed objects describe fresh ones when they agree by role, path, identity, and size,
/// every recorded checksum matches the bytes, and every checksum or generation the destination
/// reports now matches the record.
pub(crate) fn same_objects(
    committed: &[ObjectRecord],
    fresh: &[ObjectRecord],
    identities: &[ObjectIdentity],
) -> bool {
    committed.len() == fresh.len()
        && committed
            .iter()
            .zip(fresh.iter().zip(identities))
            .all(|(a, (b, identity))| {
                a.role == b.role
                    && a.path == b.path
                    && a.sha256 == b.sha256
                    && a.bytes == b.bytes
                    && a.crc32c.is_none_or(|crc32c| crc32c == identity.crc32c)
                    && (b.crc32c.is_none() || a.crc32c == b.crc32c)
                    && (b.generation.is_none() || a.generation == b.generation)
            })
}

pub(crate) fn record(role: ObjectRole, path: &str, identity: &ObjectIdentity) -> ObjectRecord {
    ObjectRecord {
        role,
        path: path.to_string(),
        key: object_key(&identity.sha256),
        bytes: identity.bytes,
        sha256: identity.sha256.clone(),
        crc32c: None,
        generation: None,
    }
}

/// A process-specific scratch path inside the retained folder's object directory; a leftover
/// from an interrupted run is ignored by every reader and overwritten by the same process id.
pub(crate) fn temporary_path(local: &Store, name: &str) -> Result<PathBuf, String> {
    let path = local
        .local_path(&format!("objects/.tmp-{name}-{}", std::process::id()))
        .expect("the retained folder is a filesystem store");
    let parent = path.parent().expect("objects directory");
    fs::create_dir_all(parent)
        .map_err(|error| format!("cannot create {}: {error}", parent.display()))?;
    Ok(path)
}

/// The rows of a native tick file, checked against the declared symbol and scale.
fn tick_rows(
    path: &Path,
    source_symbol: &ProviderSymbol,
    scale: PriceScale,
) -> Result<impl Iterator<Item = Result<Tick, String>>, String> {
    let file =
        File::open(path).map_err(|error| format!("cannot open {}: {error}", path.display()))?;
    let mut lines = BufReader::with_capacity(1 << 20, file).lines();
    let header = lines
        .next()
        .transpose()
        .map_err(|error| format!("cannot read {}: {error}", path.display()))?
        .ok_or_else(|| format!("{} is empty", path.display()))?;
    if header != TICK_HEADER {
        return Err(format!(
            "{} header `{header}` is not `{TICK_HEADER}`",
            path.display()
        ));
    }
    let display = path.display().to_string();
    let source_symbol = source_symbol.clone();
    Ok(lines.enumerate().map(move |(index, line)| {
        let line = line.map_err(|error| format!("cannot read {display}: {error}"))?;
        parse_tick_line(&line, &source_symbol, scale)
            .map_err(|reason| format!("{display} row {}: {reason}", index + 1))
    }))
}

/// The rows of every daily archive file in date order, each decoded from its retained copy and
/// checked against the tick count its metadata records.
fn daily_tick_rows<'a>(
    days: &'a [Day],
    paths: &'a [PathBuf],
    scale: PriceScale,
) -> impl Iterator<Item = Result<Tick, String>> + 'a {
    days.iter().zip(paths).flat_map(move |(day, path)| {
        let decoded = archive::read_daily_ticks(path, scale, day.start_micros)
            .map_err(|reason| format!("{}: {reason}", day.path))
            .and_then(|ticks| {
                if ticks.len() as u64 == day.ticks {
                    Ok(ticks)
                } else {
                    Err(format!(
                        "{}: {} rows, metadata records {} ticks",
                        day.path,
                        ticks.len(),
                        day.ticks
                    ))
                }
            });
        let (ticks, error) = match decoded {
            Ok(ticks) => (ticks, None),
            Err(error) => (Vec::new(), Some(Err(error))),
        };
        ticks.into_iter().map(Ok).chain(error)
    })
}

/// Validates every listed archive file from its retained copy, in manifest order, and returns
/// the dataset summary and whether every file embedded the interval contract.
fn validate_bars(
    instrument: &InstrumentId,
    records: &[ObjectRecord],
    paths: &[PathBuf],
    expectation: &BarExpectation,
    listed: &[ListedFile],
    canonical_rows: u64,
) -> Result<(DataSummary, bool), String> {
    for (record, file) in records.iter().zip(listed) {
        if record.bytes != file.bytes || record.sha256 != file.sha256 {
            return Err(format!(
                "{}: {} bytes and SHA-256 {} do not match the listed {} bytes and {}",
                file.path, record.bytes, record.sha256, file.bytes, file.sha256
            ));
        }
    }
    let summaries = parallel::map(paths, |path| archive::validate_bar_file(path, expectation))
        .into_iter()
        .zip(listed)
        .map(|(summary, file)| summary.map_err(|reason| format!("{}: {reason}", file.path)))
        .collect::<Result<Vec<_>, _>>()?;
    let mut total = DataSummary::default();
    let mut all_embedded = true;
    for (file, summary) in listed.iter().zip(&summaries) {
        let (path, rows) = (&file.path, file.rows);
        if summary.data.rows != rows {
            return Err(format!("{path}: {} rows, listed {rows}", summary.data.rows));
        }
        if let (Some(last), Some(first)) =
            (total.last_event_micros, summary.data.first_event_micros)
            && first <= last
        {
            return Err(format!(
                "{path}: starts at or before the previous file's last bar"
            ));
        }
        all_embedded &= summary.embedded_interval;
        total.extend(&summary.data);
    }
    if total.rows != canonical_rows {
        return Err(format!(
            "{instrument}: {} rows across listed files, recorded {canonical_rows}",
            total.rows
        ));
    }
    Ok((total, all_embedded))
}

#[cfg(test)]
mod tests {
    use super::*;
    use binary_alpha_engine::dataset::Coverage;
    use binary_alpha_engine::market::{BrokerId, ProviderSymbol};

    /// A tick manifest with one source object carrying the given destination metadata.
    fn manifest(crc32c: Option<u32>, generation: Option<i64>) -> GenerationManifest {
        let sha256 = "a".repeat(64);
        GenerationManifest {
            schema_version: MANIFEST_SCHEMA_VERSION,
            generation: "g".repeat(64),
            broker: BrokerId::try_from("b".to_string()).unwrap(),
            provider_symbol: ProviderSymbol::try_from("s".to_string()).unwrap(),
            instrument: "b:s".to_string(),
            role: DatasetRole::Development,
            source_kind: SourceKind::TickCsv,
            native_granularity: NativeGranularity::Tick,
            time_unit: TimeUnit::Microsecond,
            price_representation: PriceRepresentation::IntegerUnits {
                scale: PriceScale::try_from(6).unwrap(),
            },
            coverage: Coverage {
                first_event_time: "1970-01-01T00:00:00.000000Z".to_string(),
                last_event_time: "1970-01-01T00:00:01.000000Z".to_string(),
            },
            row_count: 2,
            capabilities: vec![Capability::Ticks],
            config_hash: String::new(),
            code_revision: String::new(),
            inputs: vec![],
            interval: None,
            objects: vec![ObjectRecord {
                role: ObjectRole::Source,
                path: "ticks.csv".to_string(),
                key: object_key(&sha256),
                bytes: 1,
                sha256,
                crc32c,
                generation,
            }],
        }
    }

    #[test]
    fn committed_manifests_must_match_the_fresh_result_and_reported_metadata() {
        let identities = [ObjectIdentity {
            bytes: 1,
            sha256: "a".repeat(64),
            crc32c: 7,
        }];
        let filesystem = manifest(None, None);
        let google = manifest(Some(7), Some(3));
        assert!(same_result(&manifest(None, None), &filesystem, &identities));
        assert!(
            same_result(&manifest(Some(7), Some(3)), &filesystem, &identities),
            "Google provenance in a filesystem mirror is compared against the bytes only"
        );
        assert!(
            !same_result(&manifest(Some(8), None), &filesystem, &identities),
            "a recorded checksum must match the bytes"
        );
        assert!(same_result(
            &manifest(Some(7), Some(3)),
            &google,
            &identities
        ));
        assert!(
            !same_result(&manifest(None, Some(3)), &google, &identities),
            "a checksum the destination reports must be recorded"
        );
        assert!(
            !same_result(&manifest(Some(7), Some(4)), &google, &identities),
            "a reported generation must match the record"
        );
        let mut rows = manifest(None, None);
        rows.row_count += 1;
        assert!(!same_result(&rows, &filesystem, &identities));
        let mut role = manifest(None, None);
        role.role = DatasetRole::Evaluation;
        assert!(!same_result(&role, &filesystem, &identities));
    }
}

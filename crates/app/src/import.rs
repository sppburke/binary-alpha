//! `binary-alpha data import`: enumerate the declared inventory, retain every input once, validate
//! and normalize each dataset from its retained bytes, publish it, and commit one ready manifest
//! last.

use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::time::Instant;

use binary_alpha_engine::config::{Config, PublicationUri, Source, relative_path};
use binary_alpha_engine::dataset::{
    Capability, Coverage, DatasetRole, GenerationManifest, Input, IntervalContract,
    MANIFEST_SCHEMA_VERSION, NativeGranularity, ObjectRecord, ObjectRole, PriceRepresentation,
    SourceKind, TimeUnit, generation_id, manifest_key, object_key,
};
use binary_alpha_engine::market::{
    BrokerId, InstrumentId, PriceScale, ProviderSymbol, TICK_HEADER, parse_tick_line,
};
use serde::Deserialize;

use crate::archive::{self, BarExpectation, DataSummary, TICK_OBJECT_PATH};
use crate::parallel;
use crate::store::{self, ObjectIdentity, Put, Store};

/// The producing code revision, captured by `build.rs`.
pub const CODE_REVISION: &str = env!("BINARY_ALPHA_CODE_REVISION");

/// The one bar interval contract this checkout admits: left-closed five-second bars whose
/// timestamp is the bar start on the Unix epoch grid.
const FIVE_SECOND_TEXT: [(&str, &str); 6] = [
    ("closed", "left"),
    ("frequency", "5s"),
    ("interval", "[timestamp,timestamp+5s)"),
    ("label", "left"),
    ("origin", "unix_epoch_utc"),
    ("timestamp_semantics", "bar_start"),
];
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
        source_symbol: ProviderSymbol,
    },
    Bars {
        expectation: Box<BarExpectation>,
        /// Listed archive files in manifest order, with their recorded identities and rows.
        listed: Vec<ListedFile>,
        canonical_rows: u64,
    },
}

/// One dataset to publish; `files` lists source objects in declared order, then provenance.
struct Dataset {
    instrument: InstrumentId,
    role: DatasetRole,
    files: Vec<PlannedFile>,
    data: Data,
}

/// Runs the import described by the configuration at `config_path`, writing one report line to
/// `out` as each dataset is published.
pub fn run(config_path: &Path, out: &mut dyn Write) -> Result<(), String> {
    let text = fs::read_to_string(config_path)
        .map_err(|error| format!("cannot read {}: {error}", config_path.display()))?;
    let config = Config::parse(&text).map_err(|error| error.to_string())?;
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
                data: Data::Ticks {
                    scale: *price_scale,
                    source_symbol: source_symbol.clone(),
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
    let manifest_path = root.join(manifest);
    regular_file(&manifest_path)?;
    let text = fs::read(&manifest_path)
        .map_err(|error| format!("cannot read {}: {error}", manifest_path.display()))?;
    let collection: Collection = serde_json::from_slice(&text).map_err(|error| {
        format!(
            "{} is not a collection manifest: {error}",
            manifest_path.display()
        )
    })?;
    let mut collection_files = Vec::new();
    for relative in std::iter::once(manifest).chain(provenance.iter().map(PathBuf::as_path)) {
        let absolute = root.join(relative);
        regular_file(&absolute)?;
        collection_files.push(PlannedFile {
            role: ObjectRole::Provenance,
            path: object_path(&format!("collection/{}", relative.display()))?,
            absolute,
        });
    }
    let mut datasets = Vec::with_capacity(collection.assets.len());
    for asset in collection.assets.values() {
        let asset_root = canonical(&root.join(&asset.asset_root))?;
        if !asset_root.starts_with(root) {
            return Err(format!(
                "asset root {} is outside the collection root {}",
                asset_root.display(),
                root.display()
            ));
        }
        if protected.iter().any(|dir| dir.starts_with(&asset_root)) {
            return Err(format!(
                "the historical-data folder or the file destination lies inside the asset root {}",
                asset_root.display()
            ));
        }
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

/// The asset's declared interval contract, which must be exactly the approved five-second one.
fn five_second_contract(asset: &Asset) -> Result<IntervalContract, String> {
    let declared = &asset.interval_contract;
    let mismatch = |key: &str| {
        format!(
            "{}: interval contract `{key}` is {}, expected the approved five-second contract",
            asset.asset,
            declared
                .get(key)
                .map_or("absent".to_string(), |value| value.to_string())
        )
    };
    for (key, expected) in FIVE_SECOND_TEXT {
        if declared.get(key).and_then(|value| value.as_str()) != Some(expected) {
            return Err(mismatch(key));
        }
    }
    if declared
        .get("offset_seconds")
        .and_then(|value| value.as_i64())
        != Some(0)
    {
        return Err(mismatch("offset_seconds"));
    }
    let text = |key: &str| {
        FIVE_SECOND_TEXT
            .iter()
            .find(|(name, _)| *name == key)
            .expect("declared above")
            .1
            .to_string()
    };
    Ok(IntervalContract {
        closed: text("closed"),
        frequency: text("frequency"),
        interval: text("interval"),
        label: text("label"),
        offset_seconds: 0,
        origin: text("origin"),
        timestamp_semantics: text("timestamp_semantics"),
        provenance: asset.interval_contract_provenance.clone(),
    })
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

/// Retains, publishes, and commits one dataset generation, returning its report line.
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
    let hashed = started.elapsed();
    let (source_kind, scale) = match &dataset.data {
        Data::Ticks { scale, .. } => (SourceKind::TickCsv, Some(*scale)),
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

    if destination.head(&key)?.is_some() {
        let mut bytes = Vec::new();
        destination.read_to(&key, None, &mut bytes)?;
        let committed = GenerationManifest::from_json(&bytes)
            .map_err(|error| format!("{}: {error}", destination.uri(&key)))?;
        reuse(dataset, &committed, &objects, local, destination)?;
        mirror(local, &key, &bytes)?;
        return Ok(format!(
            "published {} {} generation {generation} rows {} objects {} reused {} (already published)",
            dataset.instrument,
            dataset.role,
            committed.row_count,
            committed.objects.len(),
            committed.objects.len()
        ));
    }

    let validating = Instant::now();
    let (summary, native_granularity, time_unit, price_representation, interval, capability) =
        match &dataset.data {
            Data::Ticks {
                scale,
                source_symbol,
            } => {
                let temporary = temporary_path(local, &generation)?;
                let summary = archive::write_ticks(
                    &temporary,
                    &dataset.instrument,
                    *scale,
                    tick_rows(&retained_path(&objects[0]), source_symbol, *scale)?,
                )?;
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
        let path = local
            .local_path(&object.key)
            .expect("the retained folder is a filesystem store");
        let put = destination.put_new(&object.key, &path, identity)?;
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
    let bytes = manifest.to_json();
    let temporary = temporary_path(local, &format!("manifest-{generation}"))?;
    fs::write(&temporary, &bytes)
        .map_err(|error| format!("cannot write {}: {error}", temporary.display()))?;
    let identity = store::identify(&temporary)?;
    destination.put_new(&key, &temporary, &identity)?;
    local.put_new(&key, &temporary, &identity)?;
    fs::remove_file(&temporary)
        .map_err(|error| format!("cannot remove {}: {error}", temporary.display()))?;
    let published = publishing.elapsed();
    Ok(format!(
        "published {} {} generation {generation} rows {} objects {} reused {reused} [hash {:.3}s retain {:.3}s validate {:.3}s publish {:.3}s]",
        dataset.instrument,
        dataset.role,
        manifest.row_count,
        manifest.objects.len(),
        hashed.as_secs_f64(),
        retained.as_secs_f64(),
        validated.as_secs_f64(),
        published.as_secs_f64()
    ))
}

fn record(role: ObjectRole, path: &str, identity: &ObjectIdentity) -> ObjectRecord {
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

/// A committed ready manifest is reused only when it records exactly the computed inputs, every
/// committed child still exists at the destination with its recorded size and checksum, and the
/// retained folder holds every child.
fn reuse(
    dataset: &Dataset,
    committed: &GenerationManifest,
    inputs: &[ObjectRecord],
    local: &Store,
    destination: &Store,
) -> Result<(), String> {
    let uri = destination.uri(&committed.key());
    let committed_inputs: Vec<&ObjectRecord> = committed
        .objects
        .iter()
        .filter(|object| object.role != ObjectRole::Normalized)
        .collect();
    let same = committed_inputs.len() == inputs.len()
        && inputs.iter().all(|input| {
            committed_inputs.iter().any(|object| {
                object.role == input.role
                    && object.path == input.path
                    && object.sha256 == input.sha256
                    && object.bytes == input.bytes
            })
        });
    if !same {
        return Err(format!(
            "{uri} records different inputs than the declared source"
        ));
    }
    for object in &committed.objects {
        let stored = destination
            .head(&object.key)?
            .ok_or_else(|| format!("{uri} names {}, which is missing", object.key))?;
        if stored.bytes != object.bytes
            || (stored.crc32c.is_some() && stored.crc32c != object.crc32c)
            || (stored.generation.is_some() && stored.generation != object.generation)
        {
            return Err(format!(
                "{uri} names {}, whose destination size, checksum, or generation differs",
                object.key
            ));
        }
        if object.role == ObjectRole::Normalized && local.head(&object.key)?.is_none() {
            let Data::Ticks {
                scale,
                source_symbol,
            } = &dataset.data
            else {
                return Err(format!(
                    "{uri} records a normalized object for a bar source"
                ));
            };
            let source = local
                .local_path(&inputs[0].key)
                .expect("the retained folder is a filesystem store");
            let temporary = temporary_path(local, &committed.generation)?;
            archive::write_ticks(
                &temporary,
                &dataset.instrument,
                *scale,
                tick_rows(&source, source_symbol, *scale)?,
            )?;
            let identity = store::identify(&temporary)?;
            if identity.sha256 != object.sha256 {
                return Err(format!(
                    "{uri} records normalized object {}, but normalizing the retained source yields {}",
                    object.sha256, identity.sha256
                ));
            }
            local.put_new(&object.key, &temporary, &identity)?;
            fs::remove_file(&temporary)
                .map_err(|error| format!("cannot remove {}: {error}", temporary.display()))?;
        }
    }
    Ok(())
}

/// A scratch path inside the retained folder's object directory; a leftover from an
/// interrupted run is simply overwritten.
fn temporary_path(local: &Store, name: &str) -> Result<PathBuf, String> {
    let path = local
        .local_path(&format!("objects/.tmp-{name}"))
        .expect("the retained folder is a filesystem store");
    let parent = path.parent().expect("objects directory");
    fs::create_dir_all(parent)
        .map_err(|error| format!("cannot create {}: {error}", parent.display()))?;
    Ok(path)
}

/// Writes the exact committed ready-manifest bytes into the retained folder through the same
/// create-once primitive as every other object.
fn mirror(local: &Store, key: &str, bytes: &[u8]) -> Result<(), String> {
    let temporary = temporary_path(local, "mirror")?;
    fs::write(&temporary, bytes)
        .map_err(|error| format!("cannot write {}: {error}", temporary.display()))?;
    let identity = store::identify(&temporary)?;
    local.put_new(key, &temporary, &identity)?;
    fs::remove_file(&temporary)
        .map_err(|error| format!("cannot remove {}: {error}", temporary.display()))
}

/// The rows of a native tick file, checked against the declared symbol and scale.
fn tick_rows(
    path: &Path,
    source_symbol: &ProviderSymbol,
    scale: PriceScale,
) -> Result<impl Iterator<Item = Result<binary_alpha_engine::market::Tick, String>>, String> {
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

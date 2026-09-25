//! The narrow artifact-store interface: content-addressed objects beneath one root, created once
//! and never rewritten.
//!
//! The filesystem implementation serves the retained historical-data folder and research
//! publication; the Google Cloud Storage implementation serves publication in every run mode.

use std::fs::{self, File};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use binary_alpha_engine::config::PublicationUri;
use google_cloud_storage::client::{Storage, StorageControl};
use sha2::{Digest, Sha256};

/// Size, SHA-256, and CRC32C of one closed local file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectIdentity {
    pub bytes: u64,
    pub sha256: String,
    pub crc32c: u32,
}

/// What a store reports about one object; the checksum and generation exist only where the
/// store computes them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredObject {
    pub bytes: u64,
    pub crc32c: Option<u32>,
    pub generation: Option<i64>,
}

/// The result of a create-once write.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Put {
    Created(StoredObject),
    Reused(StoredObject),
}

impl Put {
    pub fn object(&self) -> &StoredObject {
        match self {
            Self::Created(object) | Self::Reused(object) => object,
        }
    }
}

/// One object root.
pub enum Store {
    Filesystem { root: PathBuf },
    GoogleCloud(Box<GoogleCloud>),
}

/// The Google Cloud Storage implementation: clients plus the runtime that drives them.
pub struct GoogleCloud {
    runtime: tokio::runtime::Runtime,
    storage: Storage,
    control: StorageControl,
    /// `projects/_/buckets/NAME`, the resource name the client expects.
    bucket: String,
    /// Object-name prefix without a trailing slash, possibly empty.
    prefix: String,
    /// `gs://NAME/PREFIX`, for diagnostics.
    uri: String,
}

impl Store {
    pub fn filesystem(root: impl Into<PathBuf>) -> Self {
        Self::Filesystem { root: root.into() }
    }

    /// Opens the destination a configuration names; Google clients use Application Default
    /// Credentials resolved outside the configuration document.
    pub fn open(uri: &PublicationUri) -> Result<Self, String> {
        match uri {
            PublicationUri::Filesystem(path) => Ok(Self::filesystem(path)),
            PublicationUri::GoogleCloudStorage { bucket, prefix } => {
                let runtime = tokio::runtime::Runtime::new()
                    .map_err(|error| format!("cannot start the asynchronous runtime: {error}"))?;
                let (storage, control) = runtime
                    .block_on(async {
                        Ok::<_, String>((
                            Storage::builder()
                                .build()
                                .await
                                .map_err(|error| error.to_string())?,
                            StorageControl::builder()
                                .build()
                                .await
                                .map_err(|error| error.to_string())?,
                        ))
                    })
                    .map_err(|error| format!("cannot open Google Cloud Storage client: {error}"))?;
                Ok(Self::GoogleCloud(Box::new(GoogleCloud {
                    runtime,
                    storage,
                    control,
                    bucket: format!("projects/_/buckets/{bucket}"),
                    prefix: prefix.clone(),
                    uri: uri.to_string(),
                })))
            }
        }
    }

    /// The optional access log of the filesystem implementation: when `BINARY_ALPHA_STORE_LOG`
    /// names a file, every operation appends `OPERATION KEY` to it. Test instrumentation only;
    /// it changes no identity and the Google implementation never writes it.
    fn log(&self, operation: &str, key: &str) {
        if let Self::Filesystem { .. } = self
            && let Ok(path) = std::env::var("BINARY_ALPHA_STORE_LOG")
            && let Ok(mut file) = fs::OpenOptions::new().append(true).create(true).open(path)
        {
            let _ = writeln!(file, "{operation} {key}");
        }
    }

    /// The full location of a key, for diagnostics and manifest URIs.
    pub fn uri(&self, key: &str) -> String {
        match self {
            Self::Filesystem { root } => format!("file://{}", root.join(key).display()),
            Self::GoogleCloud(cloud) => format!("{}/{key}", cloud.uri),
        }
    }

    /// The generations whose ready manifests the filesystem implementation holds, in name order;
    /// the Google implementation never lists.
    pub fn list_manifests(&self) -> Result<Vec<String>, String> {
        self.log("list", "manifests");
        let Self::Filesystem { root } = self else {
            return Err(format!("{} cannot be listed", self.uri("manifests")));
        };
        let root = root.join("manifests");
        let entries = match fs::read_dir(&root) {
            Ok(entries) => entries,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(error) => return Err(format!("cannot inspect {}: {error}", root.display())),
        };
        let mut generations = Vec::new();
        for entry in entries {
            let entry =
                entry.map_err(|error| format!("cannot inspect {}: {error}", root.display()))?;
            let name = entry.file_name().to_string_lossy().into_owned();
            self.log("probe", &format!("manifests/{name}/ready.json"));
            if entry.path().join("ready.json").is_file() {
                generations.push(name);
            }
        }
        generations.sort();
        Ok(generations)
    }

    /// The local path of a key, only for the filesystem implementation.
    pub fn local_path(&self, key: &str) -> Option<PathBuf> {
        self.log("local_path", key);
        match self {
            Self::Filesystem { root } => Some(root.join(key)),
            Self::GoogleCloud(_) => None,
        }
    }

    fn object_name(prefix: &str, key: &str) -> String {
        if prefix.is_empty() {
            key.to_string()
        } else {
            format!("{prefix}/{key}")
        }
    }

    /// Metadata of an object, or `None` when the key does not exist.
    pub fn head(&self, key: &str) -> Result<Option<StoredObject>, String> {
        self.log("head", key);
        match self {
            Self::Filesystem { root } => match fs::metadata(root.join(key)) {
                Ok(metadata) if metadata.is_file() => Ok(Some(StoredObject {
                    bytes: metadata.len(),
                    crc32c: None,
                    generation: None,
                })),
                Ok(_) => Err(format!("{} is not a regular file", self.uri(key))),
                Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
                Err(error) => Err(format!("cannot inspect {}: {error}", self.uri(key))),
            },
            Self::GoogleCloud(cloud) => {
                let request = cloud
                    .control
                    .get_object()
                    .set_bucket(&cloud.bucket)
                    .set_object(Self::object_name(&cloud.prefix, key));
                match cloud.runtime.block_on(request.send()) {
                    Ok(object) => Ok(Some(stored(&object)?)),
                    Err(error) if error.http_status_code() == Some(404) => Ok(None),
                    Err(error) => Err(format!("cannot inspect {}: {error}", self.uri(key))),
                }
            }
        }
    }

    /// Creates `key` from the closed local file `local` whose identity is already known. An
    /// existing object with the same size and checksum is reused; any other existing content
    /// is an error and nothing is replaced.
    pub fn put_new(
        &self,
        key: &str,
        local: &Path,
        identity: &ObjectIdentity,
    ) -> Result<Put, String> {
        self.log("put_new", key);
        if let Some(existing) = self.head(key)? {
            return self.reuse(key, existing, identity);
        }
        match self {
            Self::Filesystem { root } => {
                let target = root.join(key);
                let parent = target
                    .parent()
                    .ok_or_else(|| format!("{key} has no parent"))?;
                fs::create_dir_all(parent)
                    .map_err(|error| format!("cannot create {}: {error}", parent.display()))?;
                // Identical content can be published by multiple jobs in this process.
                // Each copy needs its own inode until the create-once hard link below.
                static SEQUENCE: AtomicU64 = AtomicU64::new(0);
                let sequence = SEQUENCE.fetch_add(1, Ordering::Relaxed);
                let temporary = parent.join(format!(
                    ".tmp-{}-{}-{sequence}",
                    identity.sha256,
                    std::process::id()
                ));
                let copied = File::open(local)
                    .and_then(|mut source| {
                        let mut file = File::create(&temporary)?;
                        let mut hasher = Hasher::default();
                        io::copy(&mut source, &mut Tee(&mut hasher, &mut file))?;
                        file.sync_all()?;
                        Ok(hasher.finish())
                    })
                    .map_err(|error| {
                        format!(
                            "cannot copy {} to {}: {error}",
                            local.display(),
                            temporary.display()
                        )
                    })?;
                if copied != *identity {
                    let _ = fs::remove_file(&temporary);
                    return Err(format!(
                        "{} changed while it was being retained: copied {} bytes with SHA-256 {}, expected {} bytes and {}",
                        local.display(),
                        copied.bytes,
                        copied.sha256,
                        identity.bytes,
                        identity.sha256
                    ));
                }
                // A hard link never replaces an existing target, so a concurrent creator loses
                // the race and falls into the reuse-or-conflict comparison below.
                let linked = fs::hard_link(&temporary, &target);
                let _ = fs::remove_file(&temporary);
                match linked {
                    Ok(()) => Ok(Put::Created(StoredObject {
                        bytes: identity.bytes,
                        crc32c: None,
                        generation: None,
                    })),
                    Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                        let existing = self.head(key)?.ok_or_else(|| {
                            format!("{} vanished while being created", self.uri(key))
                        })?;
                        self.reuse(key, existing, identity)
                    }
                    Err(error) => Err(format!("cannot close {}: {error}", self.uri(key))),
                }
            }
            Self::GoogleCloud(cloud) => {
                let written = cloud.runtime.block_on(async {
                    let file = tokio::fs::File::open(local)
                        .await
                        .map_err(|error| (None, error.to_string()))?;
                    cloud
                        .storage
                        .write_object(&cloud.bucket, Self::object_name(&cloud.prefix, key), file)
                        .set_if_generation_match(0)
                        .with_known_crc32c(identity.crc32c)
                        .send_unbuffered()
                        .await
                        .map_err(|error| (error.http_status_code(), error.to_string()))
                });
                match written {
                    Ok(object) => {
                        let created = stored(&object)?;
                        if created.bytes != identity.bytes
                            || created.crc32c != Some(identity.crc32c)
                        {
                            return Err(format!(
                                "{} was created with {} bytes and checksum {:?}, expected {} bytes and {}",
                                self.uri(key),
                                created.bytes,
                                created.crc32c,
                                identity.bytes,
                                identity.crc32c
                            ));
                        }
                        Ok(Put::Created(created))
                    }
                    // Another writer created the object first; the precondition protected it.
                    Err((Some(412), _)) => {
                        let existing = self.head(key)?.ok_or_else(|| {
                            format!("{} vanished after its precondition failed", self.uri(key))
                        })?;
                        self.reuse(key, existing, identity)
                    }
                    Err((_, message)) => Err(format!("cannot write {}: {message}", self.uri(key))),
                }
            }
        }
    }

    /// An existing object is reused only when its content matches the identity exactly.
    fn reuse(
        &self,
        key: &str,
        existing: StoredObject,
        identity: &ObjectIdentity,
    ) -> Result<Put, String> {
        let same = existing.bytes == identity.bytes
            && match self {
                Self::Filesystem { root } => identify(&root.join(key))?.sha256 == identity.sha256,
                Self::GoogleCloud(_) => existing.crc32c == Some(identity.crc32c),
            };
        if same {
            Ok(Put::Reused(existing))
        } else {
            Err(format!(
                "{} already holds different content ({} bytes); nothing was replaced",
                self.uri(key),
                existing.bytes
            ))
        }
    }

    /// Streams an object into `sink`, returning the byte count. A recorded generation pins the
    /// exact Google object revision.
    pub fn read_to(
        &self,
        key: &str,
        generation: Option<i64>,
        sink: &mut dyn Write,
    ) -> Result<u64, String> {
        self.log("read_to", key);
        match self {
            Self::Filesystem { root } => {
                let mut file = File::open(root.join(key))
                    .map_err(|error| format!("cannot open {}: {error}", self.uri(key)))?;
                io::copy(&mut file, sink)
                    .map_err(|error| format!("cannot read {}: {error}", self.uri(key)))
            }
            Self::GoogleCloud(cloud) => cloud
                .runtime
                .block_on(async {
                    let mut request = cloud
                        .storage
                        .read_object(&cloud.bucket, Self::object_name(&cloud.prefix, key));
                    if let Some(generation) = generation {
                        request = request.set_generation(generation);
                    }
                    let mut response = request.send().await.map_err(io::Error::other)?;
                    let mut total = 0;
                    while let Some(chunk) = response.next().await {
                        let chunk = chunk.map_err(io::Error::other)?;
                        sink.write_all(&chunk)?;
                        total += chunk.len() as u64;
                    }
                    Ok::<u64, io::Error>(total)
                })
                .map_err(|error| format!("cannot read {}: {error}", self.uri(key))),
        }
    }
}

/// The metadata Google reports for a live object; a generation is always positive.
fn stored(object: &google_cloud_storage::model::Object) -> Result<StoredObject, String> {
    let bytes = u64::try_from(object.size)
        .map_err(|_| format!("{} reports a negative size {}", object.name, object.size))?;
    if object.generation <= 0 {
        return Err(format!(
            "{} reports generation {}, expected a positive value",
            object.name, object.generation
        ));
    }
    Ok(StoredObject {
        bytes,
        crc32c: object
            .checksums
            .as_ref()
            .and_then(|checksums| checksums.crc32c),
        generation: Some(object.generation),
    })
}

/// Streams a file once, computing its size, SHA-256, and CRC32C.
pub fn identify(path: &Path) -> Result<ObjectIdentity, String> {
    let mut file =
        File::open(path).map_err(|error| format!("cannot open {}: {error}", path.display()))?;
    let mut hasher = Hasher::default();
    io::copy(&mut file, &mut hasher)
        .map_err(|error| format!("cannot read {}: {error}", path.display()))?;
    Ok(hasher.finish())
}

/// A sink that computes the identity of everything written through it.
#[derive(Default)]
pub struct Hasher {
    bytes: u64,
    sha256: Sha256,
    crc32c: u32,
}

impl Hasher {
    pub fn finish(self) -> ObjectIdentity {
        ObjectIdentity {
            bytes: self.bytes,
            sha256: binary_alpha_engine::hex(&self.sha256.finalize()),
            crc32c: self.crc32c,
        }
    }
}

impl Write for Hasher {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        self.bytes += buffer.len() as u64;
        self.sha256.update(buffer);
        self.crc32c = crc32c_update(self.crc32c, buffer);
        Ok(buffer.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

/// Writes every byte to both sinks.
pub struct Tee<'a>(pub &'a mut Hasher, pub &'a mut File);

impl Write for Tee<'_> {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        self.0.write_all(buffer)?;
        self.1.write_all(buffer)?;
        Ok(buffer.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        self.1.flush()
    }
}

/// CRC32C (Castagnoli), the checksum Google Cloud Storage computes for every object.
pub fn crc32c_update(state: u32, bytes: &[u8]) -> u32 {
    static TABLE: std::sync::LazyLock<[u32; 256]> = std::sync::LazyLock::new(|| {
        let mut table = [0u32; 256];
        for (index, entry) in table.iter_mut().enumerate() {
            let mut value = index as u32;
            for _ in 0..8 {
                value = if value & 1 == 1 {
                    0x82F6_3B78 ^ (value >> 1)
                } else {
                    value >> 1
                };
            }
            *entry = value;
        }
        table
    });
    let mut crc = !state;
    for byte in bytes {
        crc = TABLE[((crc ^ u32::from(*byte)) & 0xFF) as usize] ^ (crc >> 8);
    }
    !crc
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn crc32c_matches_the_reference_vector() {
        assert_eq!(crc32c_update(0, b"123456789"), 0xE306_9283);
        assert_eq!(
            crc32c_update(crc32c_update(0, b"1234"), b"56789"),
            0xE306_9283
        );
        assert_eq!(crc32c_update(0, b""), 0);
    }

    #[test]
    fn concurrent_identical_publications_create_once_without_sharing_scratch_files() {
        let dir = std::env::temp_dir().join(format!(
            "binary-alpha-store-concurrent-{}",
            std::process::id()
        ));
        fs::create_dir_all(&dir).unwrap();
        let source = dir.join("source");
        let bytes = vec![42; 1 << 20];
        fs::write(&source, &bytes).unwrap();
        let identity = identify(&source).unwrap();
        let store = Store::filesystem(dir.join("root"));
        // Exercise shared object and record directories, including different keys with
        // identical bytes: all of these previously used the same per-process scratch name.
        for prefix in ["objects", "records"] {
            let barrier = std::sync::Barrier::new(8);
            let results = std::thread::scope(|scope| {
                let handles: Vec<_> = (0..8)
                    .map(|index| {
                        let (store, source, identity, barrier) =
                            (&store, &source, &identity, &barrier);
                        scope.spawn(move || {
                            barrier.wait();
                            store.put_new(&format!("{prefix}/{}", index % 2), source, identity)
                        })
                    })
                    .collect();
                handles
                    .into_iter()
                    .map(|handle| handle.join().unwrap().unwrap())
                    .collect::<Vec<_>>()
            });
            assert_eq!(
                results
                    .iter()
                    .filter(|put| matches!(put, Put::Created(_)))
                    .count(),
                2
            );
            for key in ["0", "1"] {
                assert_eq!(
                    fs::read(dir.join("root").join(prefix).join(key)).unwrap(),
                    bytes
                );
            }
            assert_eq!(
                fs::read_dir(dir.join("root").join(prefix)).unwrap().count(),
                2
            );
        }
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn filesystem_store_creates_once_reuses_identical_and_rejects_conflicts() {
        let dir = std::env::temp_dir().join(format!("binary-alpha-store-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        let store = Store::filesystem(dir.join("root"));
        let local = dir.join("local");
        fs::create_dir_all(&local).unwrap();
        let first = local.join("first");
        fs::write(&first, b"hello").unwrap();
        let identity = identify(&first).unwrap();
        assert_eq!(
            identity.sha256,
            "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"
        );
        assert_eq!(store.head("objects/x").unwrap(), None);
        assert!(matches!(
            store.put_new("objects/x", &first, &identity).unwrap(),
            Put::Created(_)
        ));
        assert!(matches!(
            store.put_new("objects/x", &first, &identity).unwrap(),
            Put::Reused(_)
        ));
        let other = local.join("other");
        fs::write(&other, b"world").unwrap();
        let error = store
            .put_new("objects/x", &other, &identify(&other).unwrap())
            .unwrap_err();
        assert!(error.contains("different content"));
        assert_eq!(fs::read(dir.join("root/objects/x")).unwrap(), b"hello");
        // A source that changes after it was identified never reaches its content-addressed key.
        fs::write(&other, b"hello world").unwrap();
        let stale = ObjectIdentity {
            bytes: 5,
            sha256: identity.sha256.clone(),
            crc32c: identity.crc32c,
        };
        assert!(
            store
                .put_new("objects/y", &other, &stale)
                .unwrap_err()
                .contains("changed while")
        );
        assert_eq!(store.head("objects/y").unwrap(), None);
        assert!(
            fs::read_dir(dir.join("root/objects"))
                .unwrap()
                .all(|entry| {
                    !entry
                        .unwrap()
                        .file_name()
                        .to_string_lossy()
                        .starts_with(".tmp")
                })
        );
        let mut sink = Vec::new();
        assert_eq!(store.read_to("objects/x", None, &mut sink).unwrap(), 5);
        assert_eq!(sink, b"hello");
        fs::remove_dir_all(&dir).unwrap();
    }
}

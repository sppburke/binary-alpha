//! The narrow private Google Drive transport of the research market-data archive: user OAuth
//! refresh, pre-generated file identities, resumable uploads that survive interruption and
//! session expiry, ranged downloads, metadata, and paginated listing. Nothing here decides what
//! is archived; `data_pipeline` owns the catalog and its closure.
//!
//! Official contracts: files/create with a pre-generated `id` (a reused identity answers 409
//! rather than creating a duplicate), resumable uploads in 256 KiB multiples with `308 Resume
//! Incomplete` status queries, `alt=media` byte-range downloads, and `files.list` pagination.

use crate::broker::resolve_secret;
use crate::store::{Hasher, ObjectIdentity};
use binary_alpha_engine::config::credential_name;
use serde::{Deserialize, Serialize};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::time::Duration;

/// Resumable upload chunks are multiples of 256 KiB.
/// The largest identifier batch `files.generateIds` accepts.
const GENERATE_IDS_LIMIT: usize = 1000;
pub const CHUNK_UNIT: u64 = 256 * 1024;
const GOOGLE_API: &str = "https://www.googleapis.com/drive/v3";
const GOOGLE_UPLOAD: &str = "https://www.googleapis.com/upload/drive/v3";
const GOOGLE_TOKEN: &str = "https://oauth2.googleapis.com/token";
/// The synthetic credential a loopback fixture accepts; operator references are never read in
/// fixture mode.
const FIXTURE_CREDENTIAL: &str =
    r#"{"client_id":"fixture","client_secret":"fixture","refresh_token":"fixture"}"#;

/// The archive root and transfer limits; credentials are environment names only.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DriveSettings {
    /// The Drive folder the application created or was explicitly granted; an identifier
    /// alone grants nothing.
    pub root_folder_id: String,
    /// Environment variable holding the user OAuth credential JSON (`client_id`,
    /// `client_secret`, `refresh_token`); required unless `loopback_endpoint` is set.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub credential: Option<String>,
    pub chunk_bytes: u64,
    pub request_timeout_seconds: u32,
    pub max_attempts: u32,
    /// A literal loopback `http://` base that replaces every Google endpoint and uses a
    /// synthetic credential: the integration-test fixture, never an operator setting.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub loopback_endpoint: Option<String>,
}

impl DriveSettings {
    pub fn validate(&self) -> Result<(), String> {
        if self.root_folder_id.is_empty()
            || self
                .root_folder_id
                .bytes()
                .any(|byte| byte.is_ascii_control() || byte == b'/' || byte == b'\'')
        {
            return Err("root_folder_id must be a Drive file identifier".into());
        }
        if self.chunk_bytes == 0 || !self.chunk_bytes.is_multiple_of(CHUNK_UNIT) {
            return Err(format!(
                "chunk_bytes must be a positive multiple of {CHUNK_UNIT}"
            ));
        }
        if self.request_timeout_seconds == 0 || self.max_attempts == 0 {
            return Err("request_timeout_seconds and max_attempts must be positive".into());
        }
        match (&self.loopback_endpoint, &self.credential) {
            (None, None) => return Err("credential is required without loopback_endpoint".into()),
            (None, Some(reference)) if !credential_name(reference) => {
                return Err("credential must be an environment variable name".into());
            }
            (Some(_), Some(_)) => {
                return Err("credential must be absent with loopback_endpoint".into());
            }
            (Some(endpoint), None)
                if !(endpoint.starts_with("http://127.0.0.1:")
                    || endpoint.starts_with("http://[::1]:"))
                    || endpoint.ends_with('/')
                    || endpoint.chars().any(char::is_whitespace) =>
            {
                return Err(
                    "loopback_endpoint must be a literal http://127.0.0.1:PORT or http://[::1]:PORT base"
                        .into(),
                );
            }
            _ => {}
        }
        Ok(())
    }
}

#[derive(Deserialize)]
struct Credential {
    client_id: String,
    client_secret: String,
    refresh_token: String,
}

#[derive(Deserialize)]
struct Token {
    access_token: String,
}

/// What Drive reports about one file.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct RemoteFile {
    pub id: String,
    #[serde(default)]
    pub name: String,
    #[serde(default, deserialize_with = "decimal_text")]
    pub size: Option<u64>,
    #[serde(default, rename = "sha256Checksum")]
    pub sha256: Option<String>,
    #[serde(default)]
    pub trashed: bool,
}

/// Drive renders `size` as decimal text.
fn decimal_text<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<u64>, D::Error> {
    let text: Option<String> = Option::deserialize(deserializer)?;
    text.map(|text| text.parse().map_err(serde::de::Error::custom))
        .transpose()
}

#[derive(Deserialize)]
struct Listing {
    #[serde(default, rename = "nextPageToken")]
    next_page_token: Option<String>,
    #[serde(default)]
    files: Vec<RemoteFile>,
}

#[derive(Deserialize)]
struct GeneratedIds {
    ids: Vec<String>,
}

/// One response with the parts the transport decides on.
struct Reply {
    status: u16,
    location: Option<String>,
    range_end: Option<u64>,
    body: Vec<u8>,
}

/// The connected transport; every method is synchronous over one runtime.
pub struct Drive {
    runtime: tokio::runtime::Runtime,
    client: reqwest::Client,
    api: String,
    upload: String,
    token_endpoint: String,
    credential: Credential,
    root: String,
    chunk_bytes: u64,
    max_attempts: u32,
    access_token: Option<String>,
}

impl Drive {
    /// Opens the transport; the credential is resolved here and only here.
    pub fn open(settings: &DriveSettings) -> Result<Self, String> {
        settings.validate()?;
        let (api, upload, token_endpoint, credential) = match &settings.loopback_endpoint {
            Some(base) => (
                format!("{base}/drive/v3"),
                format!("{base}/upload/drive/v3"),
                format!("{base}/token"),
                FIXTURE_CREDENTIAL.to_string(),
            ),
            None => (
                GOOGLE_API.into(),
                GOOGLE_UPLOAD.into(),
                GOOGLE_TOKEN.into(),
                resolve_secret(settings.credential.as_deref().expect("validated reference"))?,
            ),
        };
        let credential: Credential = serde_json::from_str(&credential).map_err(
            |_| "drive: credential must be JSON with client_id, client_secret, and refresh_token",
        )?;
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|_| "drive: cannot start the transport runtime")?;
        let timeout = Duration::from_secs(u64::from(settings.request_timeout_seconds));
        let client = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .connect_timeout(timeout)
            .read_timeout(timeout)
            .build()
            .map_err(|_| "drive: cannot initialize the transport")?;
        Ok(Self {
            runtime,
            client,
            api,
            upload,
            token_endpoint,
            credential,
            root: settings.root_folder_id.clone(),
            chunk_bytes: settings.chunk_bytes,
            max_attempts: settings.max_attempts,
            access_token: None,
        })
    }

    fn token(&mut self) -> Result<String, String> {
        if let Some(token) = &self.access_token {
            return Ok(token.clone());
        }
        let form = [
            ("client_id", self.credential.client_id.as_str()),
            ("client_secret", self.credential.client_secret.as_str()),
            ("refresh_token", self.credential.refresh_token.as_str()),
            ("grant_type", "refresh_token"),
        ];
        let request = self.client.post(&self.token_endpoint).form(&form);
        let reply = self.runtime.block_on(async {
            let response = request
                .send()
                .await
                .map_err(|_| "drive token: request failed".to_string())?;
            let status = response.status().as_u16();
            let body = response
                .bytes()
                .await
                .map_err(|_| "drive token: response read failed".to_string())?;
            Ok::<_, String>((status, body))
        })?;
        if reply.0 != 200 {
            // The body can echo the credential and is never included.
            return Err(format!("drive token: status {}", reply.0));
        }
        let token: Token =
            serde_json::from_slice(&reply.1).map_err(|_| "drive token: malformed response")?;
        self.access_token = Some(token.access_token.clone());
        Ok(token.access_token)
    }

    /// Sends one authenticated request, refreshing the token once on 401 and retrying
    /// transport failures, 429, and 5xx within `max_attempts`.
    fn send(
        &mut self,
        what: &str,
        build: &dyn Fn(&reqwest::Client) -> reqwest::RequestBuilder,
    ) -> Result<Reply, String> {
        let mut attempt = 0;
        loop {
            attempt += 1;
            let token = self.token()?;
            let request = build(&self.client).bearer_auth(&token);
            let sent = self.runtime.block_on(async {
                let response = request.send().await.map_err(|_| ())?;
                let status = response.status().as_u16();
                let header = |name: &str| {
                    response
                        .headers()
                        .get(name)
                        .and_then(|value| value.to_str().ok())
                        .map(str::to_string)
                };
                let location = header("location");
                let range_end = header("range").and_then(|range| {
                    range
                        .strip_prefix("bytes=0-")
                        .and_then(|end| end.parse::<u64>().ok())
                });
                let body = response.bytes().await.map_err(|_| ())?.to_vec();
                Ok::<_, ()>(Reply {
                    status,
                    location,
                    range_end,
                    body,
                })
            });
            match sent {
                Ok(reply) if reply.status == 401 && attempt < self.max_attempts => {
                    self.access_token = None;
                }
                Ok(reply)
                    if (reply.status == 429 || reply.status >= 500)
                        && attempt < self.max_attempts =>
                {
                    std::thread::sleep(Duration::from_millis(200 * u64::from(attempt)));
                }
                Ok(reply) => return Ok(reply),
                Err(()) if attempt < self.max_attempts => {
                    std::thread::sleep(Duration::from_millis(200 * u64::from(attempt)));
                }
                Err(()) => {
                    return Err(format!(
                        "drive {what}: request failed after {attempt} attempts"
                    ));
                }
            }
        }
    }

    /// Pre-generates `count` file identifiers so a creation can be reconciled by identity,
    /// in requests of at most `GENERATE_IDS_LIMIT` (the service rejects larger ones with 400;
    /// observed 2026-09-16 with a 1249-object closure).
    pub fn generate_ids(&mut self, count: usize) -> Result<Vec<String>, String> {
        let url = format!("{}/files/generateIds", self.api);
        let mut ids = Vec::with_capacity(count);
        while ids.len() < count {
            let batch = (count - ids.len()).min(GENERATE_IDS_LIMIT);
            let query = [("count", batch.to_string()), ("space", "drive".into())];
            let reply = self.send("generateIds", &|client| client.get(&url).query(&query))?;
            if reply.status != 200 {
                return Err(format!("drive generateIds: status {}", reply.status));
            }
            let generated: GeneratedIds = serde_json::from_slice(&reply.body)
                .map_err(|_| "drive generateIds: malformed response")?;
            if generated.ids.len() != batch {
                return Err("drive generateIds: wrong identifier count".into());
            }
            ids.extend(generated.ids);
        }
        Ok(ids)
    }

    /// The archive root this transport writes beneath.
    pub fn root(&self) -> &str {
        &self.root
    }

    /// Confirms that the existing, untrashed file `id` carries `identity`, hashing its bytes
    /// when Drive reports no checksum.
    pub fn verify(&mut self, id: &str, identity: &ObjectIdentity) -> Result<RemoteFile, String> {
        let remote = self
            .metadata(id)?
            .filter(|remote| !remote.trashed)
            .ok_or_else(|| format!("drive: archived file {id} is missing or trashed"))?;
        self.confirm(id, remote, identity)
    }

    /// The metadata of file `id`, or `None` when it does not exist.
    pub fn metadata(&mut self, id: &str) -> Result<Option<RemoteFile>, String> {
        let url = format!("{}/files/{id}", self.api);
        let query = [("fields", "id,name,size,sha256Checksum,trashed")];
        let reply = self.send("files.get", &|client| client.get(&url).query(&query))?;
        match reply.status {
            200 => serde_json::from_slice(&reply.body)
                .map(Some)
                .map_err(|_| "drive files.get: malformed response".into()),
            404 => Ok(None),
            status => Err(format!("drive files.get {id}: status {status}")),
        }
    }

    /// Every non-trashed file beneath the root whose name starts with `prefix`, following
    /// pagination to the end.
    pub fn list(&mut self, prefix: &str) -> Result<Vec<RemoteFile>, String> {
        let url = format!("{}/files", self.api);
        let filter = format!(
            "'{}' in parents and trashed = false and name contains '{}'",
            self.root,
            prefix.replace('\\', "\\\\").replace('\'', "\\'")
        );
        let mut files = Vec::new();
        let mut page_token: Option<String> = None;
        loop {
            let mut query = vec![
                ("q".to_string(), filter.clone()),
                (
                    "fields".to_string(),
                    "nextPageToken,files(id,name,size,sha256Checksum,trashed)".to_string(),
                ),
                ("pageSize".to_string(), "1000".to_string()),
            ];
            if let Some(token) = &page_token {
                query.push(("pageToken".to_string(), token.clone()));
            }
            let reply = self.send("files.list", &|client| client.get(&url).query(&query))?;
            if reply.status != 200 {
                return Err(format!("drive files.list: status {}", reply.status));
            }
            let listing: Listing = serde_json::from_slice(&reply.body)
                .map_err(|_| "drive files.list: malformed response")?;
            files.extend(
                listing
                    .files
                    .into_iter()
                    .filter(|file| file.name.starts_with(prefix)),
            );
            match listing.next_page_token {
                Some(token) => page_token = Some(token),
                None => return Ok(files),
            }
        }
    }

    /// Uploads `path` as file `id` named `name` beneath the root and returns the remote file
    /// once its size and SHA-256 are confirmed. `session` is the resumable session recorded by
    /// an earlier attempt; `checkpoint` receives every session change before bytes flow through
    /// it. A file already completed under `id` is reconciled by content, never duplicated.
    pub fn upload(
        &mut self,
        id: &str,
        name: &str,
        path: &Path,
        identity: &ObjectIdentity,
        mut session: Option<String>,
        checkpoint: &mut dyn FnMut(Option<&str>) -> Result<(), String>,
    ) -> Result<RemoteFile, String> {
        let total = identity.bytes;
        let mut file =
            File::open(path).map_err(|error| format!("cannot open {}: {error}", path.display()))?;
        let mut offset;
        let mut started = 0;
        loop {
            started += 1;
            if started > self.max_attempts {
                return Err(format!("drive upload {name}: session restarted too often"));
            }
            let uri = match session.take() {
                Some(uri) => match self.status(&uri, total)? {
                    Resume::At(next) => {
                        offset = next;
                        uri
                    }
                    Resume::Completed(remote) => return self.confirm(id, remote, identity),
                    Resume::Expired => {
                        checkpoint(None)?;
                        continue;
                    }
                },
                None => match self.begin(id, name, total)? {
                    Some(uri) => {
                        checkpoint(Some(&uri))?;
                        offset = 0;
                        uri
                    }
                    None => {
                        // The identity was already created: reconcile the existing file.
                        let remote = self.metadata(id)?.ok_or_else(|| {
                            format!("drive upload {name}: {id} was created but is unreadable")
                        })?;
                        return self.confirm(id, remote, identity);
                    }
                },
            };
            let mut chunk = vec![0u8; self.chunk_bytes as usize];
            let outcome = loop {
                if offset > total {
                    break Err(format!("drive upload {name}: acknowledged beyond the file"));
                }
                let length = (total - offset).min(self.chunk_bytes);
                file.seek(SeekFrom::Start(offset))
                    .map_err(|error| format!("cannot read {}: {error}", path.display()))?;
                file.read_exact(&mut chunk[..length as usize])
                    .map_err(|error| format!("cannot read {}: {error}", path.display()))?;
                // An empty file has nothing to send: the status form of the request finalizes it.
                let range = if length == 0 {
                    format!("bytes */{total}")
                } else {
                    format!("bytes {offset}-{}/{total}", offset + length - 1)
                };
                let body = chunk[..length as usize].to_vec();
                let reply = self.send("upload", &|client| {
                    client
                        .put(&uri)
                        .header("Content-Range", &range)
                        .body(body.clone())
                })?;
                match reply.status {
                    308 if length == 0 => {
                        break Err(format!(
                            "drive upload {name}: the session holds every byte but did not complete"
                        ));
                    }
                    308 => offset = reply.range_end.map_or(0, |end| end + 1),
                    200 | 201 => {
                        let remote: RemoteFile = serde_json::from_slice(&reply.body)
                            .map_err(|_| "drive upload: malformed completion")?;
                        break Ok(Some(remote));
                    }
                    404 | 410 => break Ok(None),
                    status => break Err(format!("drive upload {name}: status {status}")),
                }
            };
            match outcome? {
                Some(remote) => return self.confirm(id, remote, identity),
                None => {
                    // The session expired: the same identity continues under a new session.
                    checkpoint(None)?;
                }
            }
        }
    }

    /// Starts a resumable session for a new file `id`; `None` when that identity already exists.
    fn begin(&mut self, id: &str, name: &str, total: u64) -> Result<Option<String>, String> {
        let url = format!("{}/files", self.upload);
        let query = [
            ("uploadType", "resumable"),
            ("fields", "id,name,size,sha256Checksum,trashed"),
        ];
        let metadata =
            serde_json::json!({ "id": id, "name": name, "parents": [self.root.clone()] });
        let total = total.to_string();
        let reply = self.send("upload begin", &|client| {
            client
                .post(&url)
                .query(&query)
                .header("X-Upload-Content-Length", &total)
                .header("X-Upload-Content-Type", "application/octet-stream")
                .json(&metadata)
        })?;
        match reply.status {
            200 => reply
                .location
                .map(Some)
                .ok_or("drive upload begin: no session location".into()),
            409 => Ok(None),
            status => Err(format!("drive upload begin {name}: status {status}")),
        }
    }

    /// Queries a resumable session: the next byte to send, the completed file, or expiry.
    fn status(&mut self, uri: &str, total: u64) -> Result<Resume, String> {
        let range = format!("bytes */{total}");
        let reply = self.send("upload status", &|client| {
            client
                .put(uri)
                .header("Content-Range", &range)
                .header("Content-Length", "0")
        })?;
        match reply.status {
            308 => Ok(Resume::At(reply.range_end.map_or(0, |end| end + 1))),
            200 | 201 => serde_json::from_slice(&reply.body)
                .map(Resume::Completed)
                .map_err(|_| "drive upload status: malformed completion".into()),
            404 | 410 => Ok(Resume::Expired),
            status => Err(format!("drive upload status: status {status}")),
        }
    }

    /// The remote file is the uploaded bytes: equal size and SHA-256, read back and hashed when
    /// Drive reports no checksum. Different content is a conflict that replaces nothing.
    fn confirm(
        &mut self,
        id: &str,
        remote: RemoteFile,
        identity: &ObjectIdentity,
    ) -> Result<RemoteFile, String> {
        let remote = match (remote.size, &remote.sha256) {
            (Some(_), Some(_)) => remote,
            _ => self
                .metadata(id)?
                .ok_or_else(|| format!("drive: {id} vanished after completion"))?,
        };
        let sha256 = match &remote.sha256 {
            Some(sha256) => sha256.to_ascii_lowercase(),
            None => self.hash(id)?.sha256,
        };
        if remote.size != Some(identity.bytes) || sha256 != identity.sha256 {
            return Err(format!(
                "drive: {id} holds {} bytes with SHA-256 {sha256}, expected {} bytes and {}; nothing was replaced",
                remote
                    .size
                    .map_or("unknown".to_string(), |size| size.to_string()),
                identity.bytes,
                identity.sha256
            ));
        }
        Ok(RemoteFile {
            sha256: Some(sha256),
            ..remote
        })
    }

    /// Reads file `id` back completely and returns its identity without keeping the bytes.
    fn hash(&mut self, id: &str) -> Result<ObjectIdentity, String> {
        let mut hasher = Hasher::default();
        self.read(id, 0, &mut hasher)?;
        Ok(hasher.finish())
    }

    /// Streams the bytes of file `id` from `offset` into `sink`, returning the bytes written.
    fn read(&mut self, id: &str, offset: u64, sink: &mut dyn Write) -> Result<u64, String> {
        let url = format!("{}/files/{id}", self.api);
        let query = [("alt", "media")];
        let range = format!("bytes={offset}-");
        let mut attempt = 0;
        loop {
            attempt += 1;
            let token = self.token()?;
            let mut request = self.client.get(&url).query(&query).bearer_auth(&token);
            if offset > 0 {
                request = request.header("Range", &range);
            }
            let outcome = self.runtime.block_on(async {
                let mut response = request.send().await.map_err(|_| Failure::Transport)?;
                let status = response.status().as_u16();
                match (status, offset) {
                    (206, _) | (200, 0) => {}
                    (200, _) => return Err(Failure::RangeIgnored),
                    (401, _) => return Err(Failure::Unauthorized),
                    (404, _) => {
                        return Err(Failure::Fatal(format!("drive files.get {id}: missing")));
                    }
                    (429, _) | (500..=599, _) => return Err(Failure::Transport),
                    (status, _) => {
                        return Err(Failure::Fatal(format!(
                            "drive files.get {id}: status {status}"
                        )));
                    }
                }
                let mut written = 0;
                while let Some(chunk) = response.chunk().await.map_err(|_| Failure::Transport)? {
                    sink.write_all(&chunk)
                        .map_err(|error| Failure::Fatal(format!("cannot write: {error}")))?;
                    written += chunk.len() as u64;
                }
                Ok(written)
            });
            match outcome {
                Ok(written) => return Ok(written),
                Err(Failure::Unauthorized) if attempt < self.max_attempts => {
                    self.access_token = None
                }
                Err(Failure::Transport) if attempt < self.max_attempts => {
                    std::thread::sleep(Duration::from_millis(200 * u64::from(attempt)));
                }
                Err(Failure::RangeIgnored) => {
                    return Err(format!(
                        "drive files.get {id}: the byte range was not honoured"
                    ));
                }
                Err(Failure::Fatal(reason)) => return Err(reason),
                Err(_) => {
                    return Err(format!(
                        "drive files.get {id}: request failed after {attempt} attempts"
                    ));
                }
            }
        }
    }

    /// Downloads file `id` into `partial`, resuming its existing bytes by range, until the
    /// complete file carries exactly `expected`. A partial file that no longer matches is
    /// discarded once and fetched again from the start.
    pub fn download(
        &mut self,
        id: &str,
        partial: &Path,
        expected: &ObjectIdentity,
    ) -> Result<(), String> {
        if let Some(parent) = partial.parent() {
            fs::create_dir_all(parent)
                .map_err(|error| format!("cannot create {}: {error}", parent.display()))?;
        }
        for restart in 0..2 {
            let have = fs::metadata(partial)
                .map(|metadata| metadata.len())
                .unwrap_or(0);
            let have = if have > expected.bytes { 0 } else { have };
            let mut file = OpenOptions::new()
                .create(true)
                .append(true)
                .open(partial)
                .map_err(|error| format!("cannot open {}: {error}", partial.display()))?;
            if have == 0 {
                file.set_len(0)
                    .map_err(|error| format!("cannot truncate {}: {error}", partial.display()))?;
            }
            if have < expected.bytes {
                let written = self.read(id, have, &mut file)?;
                file.sync_all()
                    .map_err(|error| format!("cannot flush {}: {error}", partial.display()))?;
                if have + written != expected.bytes {
                    return Err(format!(
                        "drive files.get {id}: received {} bytes, expected {}",
                        have + written,
                        expected.bytes
                    ));
                }
            }
            drop(file);
            let identity = crate::store::identify(partial)?;
            if identity.bytes == expected.bytes && identity.sha256 == expected.sha256 {
                return Ok(());
            }
            if restart == 0 {
                fs::remove_file(partial)
                    .map_err(|error| format!("cannot remove {}: {error}", partial.display()))?;
                continue;
            }
            return Err(format!(
                "drive files.get {id}: bytes carry SHA-256 {}, expected {}",
                identity.sha256, expected.sha256
            ));
        }
        unreachable!("the download loop returns")
    }
}

enum Resume {
    At(u64),
    Completed(RemoteFile),
    Expired,
}

enum Failure {
    Transport,
    Unauthorized,
    RangeIgnored,
    Fatal(String),
}

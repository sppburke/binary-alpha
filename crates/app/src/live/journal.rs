//! Synchronous append-only records and immutable closed journal segments.
//!
//! The open tail's newest record is unanchored until a successor exists. Closed segments
//! are verified against their cloud identities by the caller before marking or restoring them.

use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};

use binary_alpha_engine::execution::{FinancialEvent, Proposal};
use binary_alpha_engine::research::digest;
use serde::{Deserialize, Serialize};

pub const RECORD_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Record {
    pub sequence: u64,
    pub previous_sha256: String,
    pub time_micros: i64,
    pub deployment: String,
    #[serde(flatten)]
    pub kind: RecordKind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LeaseState {
    Acquired,
    Renewed,
    Released,
    Lost,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
#[allow(clippy::large_enum_variant)]
pub enum RecordKind {
    Started {
        config_hash: String,
        definition: String,
        code_revision: String,
    },
    Ledger {
        event: FinancialEvent,
    },
    Refused {
        binding: String,
        proposal: Proposal,
        reason: String,
    },
    Claimed {
        command: String,
        claim: String,
        token: u64,
    },
    Written {
        command: String,
        claim: String,
    },
    Lease {
        state: LeaseState,
        token: u64,
    },
    Discontinuity {
        reason: String,
    },
    Segment {
        closed: u64,
    },
}

pub struct Journal {
    dir: PathBuf,
    deployment: String,
    segment_records: u64,
    next_sequence: u64,
    previous_sha256: String,
    first: u64,
    file: Option<File>,
}

impl Journal {
    /// Restores cleaned full segments before `open`; `fetch` verifies the cloud identity.
    #[allow(clippy::type_complexity)]
    pub fn restore(
        dir: &Path,
        deployment: &str,
        segment_records: u64,
        fetch: &mut dyn FnMut(&str) -> Result<Option<Vec<u8>>, String>,
    ) -> Result<u64, String> {
        if segment_records == 0 {
            return Err("journal segment_records must be positive".into());
        }
        fs::create_dir_all(dir).map_err(|error| error.to_string())?;
        let mut first = 1u64;
        let mut restored = 0;
        loop {
            let last = first
                .checked_add(segment_records - 1)
                .ok_or("journal sequence overflow")?;
            let name = segment_name(first, last);
            let uploaded = dir.join(format!("{name}.uploaded"));
            if !dir.join(&name).exists() && !uploaded.exists() {
                let Some(bytes) = fetch(&format!("live/{deployment}/journal/{name}"))? else {
                    break;
                };
                read_records(bytes.as_slice(), &name, deployment, first, None)?;
                let mut file = OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .open(uploaded)
                    .map_err(|error| error.to_string())?;
                file.write_all(&bytes)
                    .and_then(|()| file.sync_all())
                    .map_err(|error| error.to_string())?;
                File::open(dir)
                    .and_then(|file| file.sync_all())
                    .map_err(|error| error.to_string())?;
                restored += 1;
            }
            first = last.checked_add(1).ok_or("journal sequence overflow")?;
        }
        Ok(restored)
    }

    /// Replays the complete local chain, including uploaded segments awaiting cleanup.
    /// Removed cloud segments must be restored locally before reopening this journal.
    pub fn open(
        dir: &Path,
        deployment: &str,
        segment_records: u64,
    ) -> Result<(Self, Vec<Record>), String> {
        if segment_records == 0 {
            return Err("journal segment_records must be positive".into());
        }
        fs::create_dir_all(dir).map_err(|error| error.to_string())?;
        let mut files = segments(dir, true)?;
        files.push("open.jsonl".into());
        let mut records = Vec::new();
        let mut sequence = 1;
        let mut previous = "0".repeat(64);
        let mut first = 1;
        for name in files {
            let path = dir.join(&name);
            let file = match File::open(&path) {
                Ok(file) => file,
                Err(error)
                    if name == "open.jsonl" && error.kind() == std::io::ErrorKind::NotFound =>
                {
                    first = sequence;
                    continue;
                }
                Err(error) => return Err(format!("cannot open {}: {error}", path.display())),
            };
            if name == "open.jsonl" {
                first = sequence;
            }
            let (parsed, hash) = read_records(
                BufReader::new(file),
                &name,
                deployment,
                sequence,
                Some(&previous),
            )?;
            sequence = sequence
                .checked_add(parsed.len() as u64)
                .ok_or("journal sequence overflow")?;
            previous = hash;
            records.extend(parsed);
        }
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(dir.join("open.jsonl"))
            .map_err(|error| error.to_string())?;
        let mut journal = Self {
            dir: dir.into(),
            deployment: deployment.into(),
            segment_records,
            next_sequence: sequence,
            previous_sha256: previous,
            first,
            file: Some(file),
        };
        if sequence - first >= segment_records {
            journal.rotate()?;
        }
        Ok((journal, records))
    }

    pub fn next_sequence(&self) -> u64 {
        self.next_sequence
    }

    /// The previous hash covers the serialized JSON bytes, excluding the newline delimiter.
    pub fn append(&mut self, time_micros: i64, kind: RecordKind) -> Result<Record, String> {
        let next = self
            .next_sequence
            .checked_add(1)
            .ok_or("journal sequence overflow")?;
        let record = Record {
            sequence: self.next_sequence,
            previous_sha256: self.previous_sha256.clone(),
            time_micros,
            deployment: self.deployment.clone(),
            kind,
        };
        let bytes = serde_json::to_vec(&record).map_err(|error| error.to_string())?;
        let file = self
            .file
            .as_mut()
            .ok_or("journal open file is unavailable")?;
        file.write_all(&bytes)
            .and_then(|()| file.write_all(b"\n"))
            .and_then(|()| file.sync_data())
            .map_err(|error| error.to_string())?;
        self.previous_sha256 = digest(b"", &bytes);
        self.next_sequence = next;
        if self.next_sequence - self.first >= self.segment_records {
            self.rotate()?;
        }
        Ok(record)
    }

    /// Closes and renames a nonempty segment before opening the next one.
    pub fn rotate(&mut self) -> Result<Option<String>, String> {
        if self.first == self.next_sequence {
            return Ok(None);
        }
        let name = segment_name(self.first, self.next_sequence - 1);
        if self.dir.join(&name).exists() || self.dir.join(format!("{name}.uploaded")).exists() {
            return Err(format!("journal segment {name} already exists"));
        }
        let file = self.file.take().ok_or("journal open file is unavailable")?;
        file.sync_data().map_err(|error| error.to_string())?;
        drop(file);
        fs::rename(self.dir.join("open.jsonl"), self.dir.join(&name))
            .map_err(|error| error.to_string())?;
        self.first = self.next_sequence;
        self.file = Some(
            OpenOptions::new()
                .create_new(true)
                .append(true)
                .open(self.dir.join("open.jsonl"))
                .map_err(|error| error.to_string())?,
        );
        File::open(&self.dir)
            .and_then(|file| file.sync_all())
            .map_err(|error| error.to_string())?;
        Ok(Some(name))
    }

    pub fn closed(&self) -> Result<Vec<String>, String> {
        segments(&self.dir, false)
    }

    /// The caller has verified the corresponding immutable cloud object's identity.
    pub fn mark_uploaded(&mut self, name: &str) -> Result<(), String> {
        if !valid_segment(name) {
            return Err("invalid journal segment name".into());
        }
        let target = self.dir.join(format!("{name}.uploaded"));
        if target.exists() {
            if target.is_file() && !self.dir.join(name).exists() {
                return Ok(());
            }
            return Err(format!("uploaded journal segment {name} already exists"));
        }
        fs::rename(self.dir.join(name), target).map_err(|error| error.to_string())?;
        File::open(&self.dir)
            .and_then(|file| file.sync_all())
            .map_err(|error| error.to_string())
    }

    pub fn remove_uploaded(&mut self) -> Result<(), String> {
        for name in segments(&self.dir, true)? {
            if let Some(base) = name.strip_suffix(".uploaded") {
                let (first, last) = segment_range(base).ok_or("invalid journal segment name")?;
                if last - first + 1 != self.segment_records {
                    continue;
                }
                fs::remove_file(self.dir.join(name)).map_err(|error| error.to_string())?;
            }
        }
        File::open(&self.dir)
            .and_then(|file| file.sync_all())
            .map_err(|error| error.to_string())
    }

    pub fn spool_bytes(&self) -> Result<u64, String> {
        let mut names = self.closed()?;
        names.push("open.jsonl".into());
        names.into_iter().try_fold(0u64, |total, name| {
            let size = fs::metadata(self.dir.join(name))
                .map_err(|error| error.to_string())?
                .len();
            total
                .checked_add(size)
                .ok_or_else(|| "journal spool size overflow".into())
        })
    }

    pub fn key(&self, name: &str) -> Result<String, String> {
        if !valid_segment(name) {
            return Err("invalid journal segment name".into());
        }
        Ok(format!("live/{}/journal/{name}", self.deployment))
    }
}

fn segment_name(first: u64, last: u64) -> String {
    format!("{first:020}-{last:020}.jsonl")
}
fn segment_range(name: &str) -> Option<(u64, u64)> {
    let (first, last) = name.strip_suffix(".jsonl")?.split_once('-')?;
    if first.len() != 20
        || last.len() != 20
        || !first
            .bytes()
            .chain(last.bytes())
            .all(|b| b.is_ascii_digit())
    {
        return None;
    }
    let first = first.parse::<u64>().ok()?;
    let last = last.parse::<u64>().ok()?;
    (first > 0 && last >= first).then_some((first, last))
}
fn valid_segment(name: &str) -> bool {
    segment_range(name).is_some()
}

fn read_records(
    mut reader: impl BufRead,
    name: &str,
    deployment: &str,
    first: u64,
    previous: Option<&str>,
) -> Result<(Vec<Record>, String), String> {
    let mut sequence = first;
    let mut previous = previous.map(str::to_owned);
    let mut records = Vec::new();
    let mut bytes = Vec::new();
    loop {
        bytes.clear();
        if reader
            .read_until(b'\n', &mut bytes)
            .map_err(|error| error.to_string())?
            == 0
        {
            break;
        }
        let fail = |reason: String| format!("journal record {sequence} in {name}: {reason}");
        if bytes.pop() != Some(b'\n') {
            return Err(fail("incomplete line".into()));
        }
        let record: Record =
            serde_json::from_slice(&bytes).map_err(|error| fail(error.to_string()))?;
        if record.sequence != sequence {
            return Err(fail(format!(
                "sequence mismatch: found {}",
                record.sequence
            )));
        }
        if record.deployment != deployment {
            return Err(fail("deployment mismatch".into()));
        }
        if previous
            .as_ref()
            .is_some_and(|hash| record.previous_sha256 != *hash)
        {
            return Err(fail("previous hash mismatch".into()));
        }
        previous = Some(digest(b"", &bytes));
        sequence = sequence.checked_add(1).ok_or("journal sequence overflow")?;
        records.push(record);
    }
    if name != "open.jsonl"
        && (sequence == first
            || name.trim_end_matches(".uploaded") != segment_name(first, sequence - 1))
    {
        return Err(format!("journal segment {name}: record range mismatch"));
    }
    Ok((records, previous.unwrap_or_else(|| "0".repeat(64))))
}
fn segments(dir: &Path, uploaded: bool) -> Result<Vec<String>, String> {
    let mut names = Vec::new();
    for entry in fs::read_dir(dir).map_err(|error| error.to_string())? {
        let entry = entry.map_err(|error| error.to_string())?;
        let name = entry.file_name().to_string_lossy().into_owned();
        let base = if uploaded {
            name.strip_suffix(".uploaded").unwrap_or(&name)
        } else {
            &name
        };
        if valid_segment(base) {
            names.push(name);
        } else if base.ends_with(".jsonl") && base != "open.jsonl" {
            return Err(format!("invalid journal segment name {name}"));
        }
    }
    names.sort();
    Ok(names)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn root(name: &str) -> PathBuf {
        let root =
            Path::new(env!("CARGO_MANIFEST_DIR")).join(format!("../../target/live-journal-{name}"));
        let _ = fs::remove_dir_all(&root);
        root
    }
    fn kind() -> RecordKind {
        RecordKind::Discontinuity {
            reason: "restart".into(),
        }
    }

    #[test]
    fn append_rotate_replay_and_spool() {
        let dir = root("replay");
        let (mut journal, records) = Journal::open(&dir, "deployment", 2).unwrap();
        assert!(records.is_empty());
        assert_eq!(journal.spool_bytes().unwrap(), 0);
        let first = journal.append(1, kind()).unwrap();
        assert_eq!(first.sequence, 1);
        assert_eq!(first.previous_sha256, "0".repeat(64));
        let second = journal.append(2, kind()).unwrap();
        assert_eq!(
            second.previous_sha256,
            digest(b"", &serde_json::to_vec(&first).unwrap())
        );
        let third = journal.append(3, kind()).unwrap();
        let closed = journal.closed().unwrap();
        assert_eq!(closed, vec![segment_name(1, 2)]);
        assert_eq!(
            journal.key(&closed[0]).unwrap(),
            format!("live/deployment/journal/{}", closed[0])
        );
        let open_size = fs::metadata(dir.join("open.jsonl")).unwrap().len();
        assert_eq!(
            journal.spool_bytes().unwrap(),
            open_size + fs::metadata(dir.join(&closed[0])).unwrap().len()
        );
        journal.mark_uploaded(&closed[0]).unwrap();
        journal.mark_uploaded(&closed[0]).unwrap();
        assert!(journal.closed().unwrap().is_empty());
        assert_eq!(journal.spool_bytes().unwrap(), open_size);
        drop(journal);
        let (mut journal, records) = Journal::open(&dir, "deployment", 2).unwrap();
        assert_eq!(records, vec![first, second, third]);
        assert_eq!(journal.next_sequence(), 4);
        journal.remove_uploaded().unwrap();
        journal.remove_uploaded().unwrap();
        assert!(!dir.join(format!("{}.uploaded", closed[0])).exists());
        assert_eq!(journal.spool_bytes().unwrap(), open_size);
        assert_eq!(journal.rotate().unwrap(), Some(segment_name(3, 3)));
        assert_eq!(journal.rotate().unwrap(), None);
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn corrupt_line_and_deployment_are_rejected() {
        let dir = root("corruption");
        let (mut journal, _) = Journal::open(&dir, "deployment", 10).unwrap();
        journal.append(1, kind()).unwrap();
        journal.append(2, kind()).unwrap();
        drop(journal);
        let error = Journal::open(&dir, "other", 10).err().unwrap();
        assert!(
            error.contains("record 1") && error.contains("deployment mismatch"),
            "{error}"
        );
        let path = dir.join("open.jsonl");
        let mut bytes = fs::read(&path).unwrap();
        let offset = bytes
            .windows(7)
            .position(|value| value == b"restart")
            .unwrap();
        bytes[offset] = b'R';
        fs::write(&path, bytes).unwrap();
        let error = Journal::open(&dir, "deployment", 10).err().unwrap();
        assert!(
            error.contains("record 2") && error.contains("hash mismatch"),
            "{error}"
        );
        fs::remove_dir_all(dir).unwrap();
    }
    #[test]
    fn cleanup_restore_preserves_the_chain_and_keeps_partial_segments() {
        let dir = root("restore");
        let (mut journal, _) = Journal::open(&dir, "deployment", 2).unwrap();
        let records: Vec<_> = (1..=5)
            .map(|time| journal.append(time, kind()).unwrap())
            .collect();
        journal.rotate().unwrap();
        let mut cloud = std::collections::BTreeMap::new();
        for name in journal.closed().unwrap() {
            cloud.insert(
                journal.key(&name).unwrap(),
                fs::read(dir.join(&name)).unwrap(),
            );
            journal.mark_uploaded(&name).unwrap();
        }
        journal.remove_uploaded().unwrap();
        journal.remove_uploaded().unwrap();
        let partial = dir.join(format!("{}.uploaded", segment_name(5, 5)));
        assert!(partial.exists());
        for (first, last) in [(1, 2), (3, 4)] {
            assert!(
                !dir.join(format!("{}.uploaded", segment_name(first, last)))
                    .exists()
            );
        }
        drop(journal);
        let error = Journal::open(&dir, "deployment", 2).err().unwrap();
        assert!(error.contains("sequence mismatch: found 5"), "{error}");
        let mut requested = Vec::new();
        assert_eq!(
            Journal::restore(&dir, "deployment", 2, &mut |key| {
                requested.push(key.to_owned());
                Ok(cloud.get(key).cloned())
            })
            .unwrap(),
            2
        );
        assert_eq!(
            requested,
            [(1, 2), (3, 4), (5, 6)].map(|(first, last)| {
                format!("live/deployment/journal/{}", segment_name(first, last))
            })
        );
        let (journal, restored) = Journal::open(&dir, "deployment", 2).unwrap();
        assert_eq!(restored, records);
        assert_eq!(journal.next_sequence(), 6);
        drop(journal);
        // A present plain or uploaded segment does not terminate the restore walk.
        let first = segment_name(1, 2);
        fs::rename(dir.join(format!("{first}.uploaded")), dir.join(&first)).unwrap();
        fs::remove_file(dir.join(format!("{}.uploaded", segment_name(3, 4)))).unwrap();
        assert_eq!(
            Journal::restore(&dir, "deployment", 2, &mut |key| {
                assert!(!key.ends_with(&first));
                Ok(cloud.get(key).cloned())
            })
            .unwrap(),
            1
        );
        assert_eq!(Journal::open(&dir, "deployment", 2).unwrap().1, records);
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn restore_empty_spool_or_fresh_deployment_and_refuse_wrong_range() {
        let dir = root("restore-empty");
        assert_eq!(
            Journal::restore(&dir, "deployment", 2, &mut |_| Ok(None)).unwrap(),
            0
        );
        let (mut journal, records) = Journal::open(&dir, "deployment", 2).unwrap();
        assert!(records.is_empty());
        let records = vec![
            journal.append(1, kind()).unwrap(),
            journal.append(2, kind()).unwrap(),
        ];
        let name = segment_name(1, 2);
        let bytes = fs::read(dir.join(&name)).unwrap();
        journal.mark_uploaded(&name).unwrap();
        journal.remove_uploaded().unwrap();
        drop(journal);
        fs::remove_file(dir.join("open.jsonl")).unwrap();
        let partial = serde_json::to_string(&records[0]).unwrap() + "\n";
        let error = Journal::restore(&dir, "deployment", 2, &mut |_| {
            Ok(Some(partial.as_bytes().to_vec()))
        })
        .unwrap_err();
        assert!(error.contains("record range mismatch"), "{error}");
        assert!(!dir.join(format!("{name}.uploaded")).exists());
        let wrong_sequence = serde_json::to_string(&records[1]).unwrap() + "\n";
        let error = Journal::restore(&dir, "deployment", 2, &mut |_| {
            Ok(Some(wrong_sequence.as_bytes().to_vec()))
        })
        .unwrap_err();
        assert!(error.contains("sequence mismatch: found 2"), "{error}");
        assert!(!dir.join(format!("{name}.uploaded")).exists());
        assert_eq!(
            Journal::restore(&dir, "deployment", 2, &mut |key| {
                Ok(key.ends_with(&name).then(|| bytes.clone()))
            })
            .unwrap(),
            1
        );
        assert_eq!(Journal::open(&dir, "deployment", 2).unwrap().1, records);
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn incomplete_last_line_is_refused_and_retained() {
        let dir = root("incomplete");
        let (mut journal, _) = Journal::open(&dir, "deployment", 2).unwrap();
        journal.append(1, kind()).unwrap();
        drop(journal);
        let path = dir.join("open.jsonl");
        let mut bytes = fs::read(&path).unwrap();
        assert_eq!(bytes.pop(), Some(b'\n'));
        fs::write(&path, &bytes).unwrap();
        let error = Journal::open(&dir, "deployment", 2).err().unwrap();
        assert!(
            error.contains("record 1") && error.contains("incomplete line"),
            "{error}"
        );
        assert_eq!(fs::read(&path).unwrap(), bytes);
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn reopen_at_rotation_boundary_preserves_sequence_and_refuses_duplicate_tail() {
        let dir = root("boundary");
        let (mut journal, _) = Journal::open(&dir, "deployment", 2).unwrap();
        let records = vec![
            journal.append(1, kind()).unwrap(),
            journal.append(2, kind()).unwrap(),
        ];
        drop(journal);
        let (journal, replay) = Journal::open(&dir, "deployment", 2).unwrap();
        assert_eq!(replay, records);
        assert_eq!(journal.next_sequence(), 3);
        drop(journal);
        // Simulate a full open tail whose rotation has not yet renamed it.
        let name = segment_name(1, 2);
        fs::rename(dir.join(&name), dir.join("open.jsonl")).unwrap();
        let (mut journal, replay) = Journal::open(&dir, "deployment", 2).unwrap();
        assert_eq!(replay, records);
        assert_eq!(journal.closed().unwrap(), vec![name.clone()]);
        let third = journal.append(3, kind()).unwrap();
        assert_eq!(third.sequence, 3);
        assert_eq!(
            third.previous_sha256,
            digest(b"", &serde_json::to_vec(&records[1]).unwrap())
        );
        drop(journal);
        let bytes = fs::read(dir.join(&name)).unwrap();
        fs::write(dir.join("open.jsonl"), &bytes).unwrap();
        let error = Journal::open(&dir, "deployment", 2).err().unwrap();
        assert!(
            error.contains("record 3") && error.contains("sequence mismatch: found 1"),
            "{error}"
        );
        assert_eq!(fs::read(dir.join("open.jsonl")).unwrap(), bytes);
        fs::remove_dir_all(dir).unwrap();
    }
}

//! Shared daily-layout manifest vocabulary and owner/path validation.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use super::{ObjectRecord, ObjectRole};
use crate::market::parse_event_time_micros;

pub const ACQUISITION_PREFIX: &str = "provenance/acquisitions/";

pub const DAILY_PARQUET_PROFILE: &str = "daily-parquet-v1";
pub const DAY_MICROS: i64 = 86_400_000_000;

crate::string_enum! {
    /// An absent layout marker means legacy v1.
    Layout "layout" { DailyV2 => "daily-v2" }
}
crate::string_enum! {
    DayFamily "family" {
        Observations => "observations", Pages => "pages", Candles => "candles",
    }
}
crate::string_enum! {
    DayState "state" {
        Complete => "complete", Partial => "partial", Unknown => "unknown",
        EmptyKnown => "empty_known",
    }
}

/// A half-open unresolved range inside its inventory day.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct UnresolvedInterval {
    pub start: String,
    pub end: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DayInventoryEntry {
    pub date: String,
    pub family: DayFamily,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub duration: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub offset: Option<u32>,
    pub object: Option<String>,
    pub rows: u64,
    /// Inclusive bounds of partition timestamps, not candle closes or page first events.
    pub first_time: Option<String>,
    pub last_time: Option<String>,
    pub state: DayState,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub unresolved: Vec<UnresolvedInterval>,
}

/// A strictly spelled Gregorian UTC date and its half-open microsecond bounds.
pub fn day_bounds(date: &str) -> Result<(i64, i64), String> {
    if date.len() != 10 || !date.is_ascii() {
        return Err(format!("invalid daily date `{date}`"));
    }
    let start = parse_event_time_micros(&format!("{date}T00:00:00Z"))?;
    let end = start.checked_add(DAY_MICROS).ok_or("day end overflow")?;
    Ok((start, end))
}

fn timestamp(text: &str) -> Result<i64, String> {
    if !text.is_ascii() {
        return Err("daily timestamp must be ASCII".into());
    }
    parse_event_time_micros(text)
}

impl DayInventoryEntry {
    pub fn logical_path(&self) -> Result<String, String> {
        day_bounds(&self.date)?;
        match (self.family, self.duration, self.offset) {
            (DayFamily::Candles, Some(n), Some(o)) if n > 0 && o < n => {
                Ok(format!("candles/{n}s_{o}s/{}.parquet", self.date))
            }
            (DayFamily::Observations | DayFamily::Pages, None, None) => {
                Ok(format!("{}/{}.parquet", self.family, self.date))
            }
            _ => Err(
                "duration/offset must occur only on candles, with 0 <= offset < duration".into(),
            ),
        }
    }

    pub fn validate(&self) -> Result<(), String> {
        self.logical_path()?;
        let (start, end) = day_bounds(&self.date)?;
        if self.object.is_none() != (self.state == DayState::EmptyKnown) {
            return Err("object must be null exactly for empty_known".into());
        }
        if self.state == DayState::EmptyKnown && self.rows != 0 {
            return Err("empty_known must have zero rows".into());
        }
        match (&self.first_time, &self.last_time) {
            (None, None) if self.rows == 0 => {}
            (Some(first), Some(last)) if self.rows > 0 => {
                let (first, last) = (timestamp(first)?, timestamp(last)?);
                if first < start || last >= end || first > last {
                    return Err("inventory time bounds must be ordered inside the UTC day".into());
                }
            }
            _ => return Err("inventory time bounds must be null exactly for zero rows".into()),
        }
        if matches!(self.state, DayState::Partial | DayState::Unknown)
            && self
                .reason
                .as_ref()
                .is_none_or(|reason| reason.trim().is_empty())
        {
            return Err("partial/unknown requires a reason".into());
        }
        if self.state == DayState::Partial && self.unresolved.is_empty() {
            return Err("partial requires unresolved intervals".into());
        }
        if matches!(self.state, DayState::Complete | DayState::EmptyKnown)
            && !self.unresolved.is_empty()
        {
            return Err("complete/empty_known cannot have unresolved intervals".into());
        }
        let mut previous_end = start;
        for interval in &self.unresolved {
            let (from, to) = (timestamp(&interval.start)?, timestamp(&interval.end)?);
            if from < previous_end || from >= to || to > end {
                return Err(
                    "unresolved intervals must be ordered, nonoverlapping, and inside the day"
                        .into(),
                );
            }
            previous_end = to;
        }
        Ok(())
    }
}

/// The enclosing manifest owns which families and metadata may appear.
pub(crate) enum DailyOwner<'a> {
    Dataset,
    Stream(&'a [(u32, u32)]),
}

pub(crate) fn validate_inventory(
    entries: &[DayInventoryEntry],
    objects: &[ObjectRecord],
    owner: DailyOwner<'_>,
) -> Result<(), String> {
    let mut dates = BTreeMap::new();
    let mut expected = BTreeSet::new();
    for entry in entries {
        entry.validate()?;
        match (&owner, entry.family) {
            (DailyOwner::Dataset, DayFamily::Observations | DayFamily::Pages) => {}
            (DailyOwner::Stream(specs), DayFamily::Candles)
                if specs.contains(&(entry.duration.unwrap(), entry.offset.unwrap())) => {}
            _ => return Err("day family/candle spec does not belong to this manifest".into()),
        }
        let series = (entry.family, entry.duration, entry.offset);
        if dates
            .insert(series, entry.date.as_str())
            .is_some_and(|prior| prior >= entry.date.as_str())
        {
            return Err(
                "inventory dates must be strictly increasing per family/candle spec".into(),
            );
        }
        let path = entry.logical_path()?;
        if let Some(key) = &entry.object {
            let role = if entry.family == DayFamily::Pages {
                ObjectRole::Source
            } else {
                ObjectRole::Normalized
            };
            if !objects
                .iter()
                .any(|object| object.path == path && object.key == *key && object.role == role)
            {
                return Err(format!(
                    "inventory object `{path}` disagrees with the object list"
                ));
            }
            expected.insert(path);
        }
    }
    for object in objects {
        if expected.contains(&object.path) {
            continue;
        }
        let allowed = match owner {
            DailyOwner::Dataset => {
                object.role == ObjectRole::Provenance
                    && (matches!(
                        object.path.as_str(),
                        "provenance/coverage.json" | "provenance/lineage.json"
                    ) || object.path == format!("{ACQUISITION_PREFIX}{}.json", object.sha256))
            }
            DailyOwner::Stream(_) => {
                object.role == ObjectRole::Normalized && object.path == "profile.json"
            }
        };
        if !allowed {
            return Err(format!(
                "daily object `{}` is not allowed or has no inventory entry",
                object.path
            ));
        }
    }
    let required = match owner {
        DailyOwner::Dataset => "provenance/coverage.json",
        DailyOwner::Stream(_) => "profile.json",
    };
    if !objects.iter().any(|object| object.path == required) {
        return Err(format!("daily manifest requires `{required}`"));
    }
    Ok(())
}

pub(crate) fn inventory_rows<'a>(
    mut entries: impl Iterator<Item = &'a DayInventoryEntry>,
) -> Result<u64, String> {
    entries.try_fold(0u64, |total, day| {
        total
            .checked_add(day.rows)
            .ok_or_else(|| "daily row count overflow".into())
    })
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::dataset::{object_key, validate_objects};

    pub(crate) fn entry(family: DayFamily) -> DayInventoryEntry {
        DayInventoryEntry {
            date: "2026-09-17".into(),
            family,
            duration: (family == DayFamily::Candles).then_some(5),
            offset: (family == DayFamily::Candles).then_some(0),
            object: Some(object_key(&"a".repeat(64))),
            rows: 2,
            first_time: Some("2026-09-17T00:00:00.000000Z".into()),
            last_time: Some("2026-09-17T23:59:59.999999Z".into()),
            state: DayState::Complete,
            reason: None,
            unresolved: vec![],
        }
    }

    pub(crate) fn object(path: &str, role: ObjectRole) -> ObjectRecord {
        ObjectRecord {
            role,
            path: path.into(),
            key: object_key(&"a".repeat(64)),
            sha256: "a".repeat(64),
            bytes: 1,
            crc32c: None,
            generation: None,
        }
    }

    fn interval() -> UnresolvedInterval {
        UnresolvedInterval {
            start: "2026-09-17T12:00:00Z".into(),
            end: "2026-09-18T00:00:00Z".into(),
        }
    }

    #[test]
    fn dates_are_exact_gregorian_utc_days() {
        assert_eq!(day_bounds("1970-01-01").unwrap(), (0, DAY_MICROS));
        assert_eq!(day_bounds("1969-12-31").unwrap(), (-DAY_MICROS, 0));
        assert!(day_bounds("2000-02-29").is_ok());
        for date in [
            "1900-02-29",
            "2026-02-29",
            "2026-13-01",
            "2026-00-01",
            "2026-09-31",
            "2026-09-00",
            "2026-9-17",
            "2026/09/17",
            "2026-09-17/..",
            "💥-09-17",
        ] {
            assert!(day_bounds(date).is_err(), "{date}");
        }
    }

    #[test]
    fn states_round_trip_and_enforce_evidence() {
        let complete = entry(DayFamily::Observations);
        let partial = DayInventoryEntry {
            state: DayState::Partial,
            reason: Some("cutoff".into()),
            unresolved: vec![interval()],
            ..complete.clone()
        };
        let unknown = DayInventoryEntry {
            state: DayState::Unknown,
            reason: Some("historical gap".into()),
            ..complete.clone()
        };
        let empty = DayInventoryEntry {
            state: DayState::EmptyKnown,
            object: None,
            rows: 0,
            first_time: None,
            last_time: None,
            ..complete.clone()
        };
        for day in [&complete, &partial, &unknown, &empty] {
            day.validate().unwrap();
            assert_eq!(
                *day,
                serde_json::from_slice::<DayInventoryEntry>(&serde_json::to_vec(day).unwrap())
                    .unwrap()
            );
        }
        for mut day in [partial.clone(), unknown] {
            day.reason = None;
            assert!(day.validate().is_err());
            day.reason = Some("  ".into());
            assert!(day.validate().is_err());
        }
        let mut bad = partial;
        bad.unresolved.clear();
        assert!(bad.validate().unwrap_err().contains("intervals"));
        for mut day in [complete.clone(), empty.clone()] {
            day.unresolved.push(interval());
            assert!(day.validate().is_err());
        }
        let mut bad = empty;
        bad.rows = 1;
        assert!(bad.validate().is_err());
        let mut bad = complete;
        bad.object = None;
        assert!(bad.validate().is_err());
        bad.state = DayState::EmptyKnown;
        bad.rows = 0;
        bad.first_time = None;
        bad.last_time = None;
        bad.object = Some(object_key(&"a".repeat(64)));
        assert!(bad.validate().is_err());
    }

    #[test]
    fn bounds_and_unresolved_intervals_are_checked() {
        let base = entry(DayFamily::Pages);
        for (first, last) in [
            (None, base.last_time.clone()),
            (base.first_time.clone(), None),
            (Some("2026-09-16T23:59:59Z".into()), base.last_time.clone()),
            (base.first_time.clone(), Some("2026-09-18T00:00:00Z".into())),
            (base.last_time.clone(), base.first_time.clone()),
            (Some("💥-09-17T00:00:00Z".into()), base.last_time.clone()),
        ] {
            assert!(
                DayInventoryEntry {
                    first_time: first,
                    last_time: last,
                    ..base.clone()
                }
                .validate()
                .is_err()
            );
        }
        assert!(
            DayInventoryEntry {
                rows: 0,
                ..base.clone()
            }
            .validate()
            .is_err()
        );
        let partial = DayInventoryEntry {
            state: DayState::Partial,
            reason: Some("gap".into()),
            unresolved: vec![interval()],
            ..base
        };
        for (start, end) in [
            ("2026-09-16T23:00:00Z", "2026-09-17T01:00:00Z"),
            ("2026-09-17T12:00:00Z", "2026-09-18T00:00:00.000001Z"),
            ("2026-09-17T12:00:00Z", "2026-09-17T12:00:00Z"),
            ("2026-09-17T13:00:00Z", "2026-09-17T12:00:00Z"),
        ] {
            let mut bad = partial.clone();
            bad.unresolved[0] = UnresolvedInterval {
                start: start.into(),
                end: end.into(),
            };
            assert!(bad.validate().is_err());
        }
        let mut bad = partial;
        bad.unresolved.push(interval());
        assert!(bad.validate().is_err());
    }

    #[test]
    fn candle_parameters_are_exclusive_and_paths_are_canonical() {
        for family in [
            DayFamily::Observations,
            DayFamily::Pages,
            DayFamily::Candles,
        ] {
            let base = entry(family);
            base.validate().unwrap();
            let expected = if family == DayFamily::Candles {
                "candles/5s_0s".into()
            } else {
                family.to_string()
            };
            assert_eq!(
                base.logical_path().unwrap(),
                format!("{expected}/2026-09-17.parquet")
            );
            for (duration, offset) in [
                (Some(0), Some(0)),
                (Some(5), Some(5)),
                (Some(5), None),
                (None, Some(0)),
            ] {
                assert!(
                    DayInventoryEntry {
                        duration,
                        offset,
                        ..base.clone()
                    }
                    .validate()
                    .is_err()
                );
            }
        }
        let mut candle = entry(DayFamily::Candles);
        candle.duration = None;
        candle.offset = None;
        assert!(candle.validate().is_err());
    }

    #[test]
    fn inventory_and_objects_agree_in_both_directions_and_roles() {
        let day = entry(DayFamily::Observations);
        let objects = vec![
            object(&day.logical_path().unwrap(), ObjectRole::Normalized),
            object("provenance/coverage.json", ObjectRole::Provenance),
        ];
        let check = |days: &[DayInventoryEntry], objects: &[ObjectRecord]| {
            validate_objects(objects)?;
            validate_inventory(days, objects, DailyOwner::Dataset)
        };
        check(std::slice::from_ref(&day), &objects).unwrap();
        for path in [
            "observations/2026-9-17.parquet",
            "observations/2026-09-17.parquet/extra",
            "observations/../x",
            "normalized/ticks.parquet",
            "profile.json",
            "candles/5s_0s/2026-09-17.parquet",
            "provenance/other.json",
        ] {
            let mut bad = objects.clone();
            bad[0].path = path.into();
            assert!(check(std::slice::from_ref(&day), &bad).is_err(), "{path}");
        }
        assert!(check(&[], &objects).is_err());
        assert!(check(std::slice::from_ref(&day), &objects[1..]).is_err());
        assert!(check(std::slice::from_ref(&day), &objects[..1]).is_err());
        let mut bad = objects.clone();
        bad.push(objects[0].clone());
        assert!(check(std::slice::from_ref(&day), &bad).is_err());
        for role in [ObjectRole::Source, ObjectRole::Provenance] {
            let mut bad = objects.clone();
            bad[0].role = role;
            assert!(check(std::slice::from_ref(&day), &bad).is_err());
        }
        let mut bad = day.clone();
        bad.object = Some(object_key(&"b".repeat(64)));
        assert!(check(&[bad], &objects).is_err());
        let mut bad = objects.clone();
        bad[0].key = "untrusted".into();
        assert!(check(std::slice::from_ref(&day), &bad).is_err());
        let mut empty = day;
        empty.state = DayState::EmptyKnown;
        empty.object = None;
        empty.rows = 0;
        empty.first_time = None;
        empty.last_time = None;
        assert!(check(std::slice::from_ref(&empty), &objects).is_err());
        check(&[empty], &objects[1..]).unwrap();
    }

    #[test]
    fn nonempty_pages_require_source_role_and_observations_cannot_have_candle_parameters() {
        let day = entry(DayFamily::Pages);
        let mut objects = vec![
            object(&day.logical_path().unwrap(), ObjectRole::Source),
            object("provenance/coverage.json", ObjectRole::Provenance),
        ];
        validate_inventory(std::slice::from_ref(&day), &objects, DailyOwner::Dataset).unwrap();
        for role in [ObjectRole::Normalized, ObjectRole::Provenance] {
            objects[0].role = role;
            assert!(
                validate_inventory(std::slice::from_ref(&day), &objects, DailyOwner::Dataset)
                    .is_err()
            );
        }
        for family in [DayFamily::Observations, DayFamily::Pages] {
            let mut bad = entry(family);
            bad.duration = Some(5);
            bad.offset = Some(0);
            assert!(bad.validate().is_err());
        }
    }

    #[test]
    fn dates_increase_per_family_and_candle_spec_and_owners_are_exclusive() {
        let mut one = entry(DayFamily::Candles);
        one.state = DayState::EmptyKnown;
        one.object = None;
        one.rows = 0;
        one.first_time = None;
        one.last_time = None;
        let profile = vec![object("profile.json", ObjectRole::Normalized)];
        let specs = [(5, 0), (15, 5)];
        let check = |days: &[DayInventoryEntry]| {
            validate_inventory(days, &profile, DailyOwner::Stream(&specs))
        };
        let mut two = one.clone();
        two.duration = Some(15);
        two.offset = Some(5);
        check(&[one.clone(), two.clone()]).unwrap();
        assert!(check(&[one.clone(), one.clone()]).is_err());
        let mut earlier = one.clone();
        earlier.date = "2026-09-16".into();
        assert!(check(&[one.clone(), earlier]).is_err());
        two.duration = Some(20);
        assert!(check(&[two]).is_err());
        assert!(check(&[entry(DayFamily::Observations)]).is_err());
        assert!(validate_inventory(&[one], &profile, DailyOwner::Dataset).is_err());
        let mut pages = entry(DayFamily::Pages);
        pages.object = None;
        pages.state = DayState::EmptyKnown;
        pages.rows = 0;
        pages.first_time = None;
        pages.last_time = None;
        let mut obs = pages.clone();
        obs.family = DayFamily::Observations;
        let coverage = [object("provenance/coverage.json", ObjectRole::Provenance)];
        validate_inventory(&[pages, obs], &coverage, DailyOwner::Dataset).unwrap();
        assert!(validate_inventory(&[], &coverage, DailyOwner::Stream(&specs)).is_err());
    }

    #[test]
    fn serde_rejects_unknown_vocabulary_and_counts_cannot_overflow() {
        let base = serde_json::to_value(entry(DayFamily::Observations)).unwrap();
        for (field, value) in [("family", "ticks"), ("state", "missing")] {
            let mut bad = base.clone();
            bad[field] = value.into();
            assert!(serde_json::from_value::<DayInventoryEntry>(bad).is_err());
        }
        assert!(serde_json::from_str::<Layout>("\"daily-v3\"").is_err());
        let one = entry(DayFamily::Observations);
        let two = DayInventoryEntry {
            rows: u64::MAX,
            ..one.clone()
        };
        assert!(inventory_rows([one, two].iter()).is_err());
    }
}

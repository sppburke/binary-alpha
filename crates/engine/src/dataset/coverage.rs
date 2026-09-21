//! Acquisition evidence for `daily-v2`; page occurrences live only in daily Parquet.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use super::daily::{DayFamily, DayInventoryEntry, DayState, Layout, day_bounds};
use super::{DatasetRole, GenerationManifest, NativeGranularity};
use crate::market::{BrokerId, ProviderSymbol, format_event_time_micros, parse_event_time_micros};

/// Half-open event-time range, including for native bars (bar starts, not availability).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CoverageRange {
    pub start: String,
    pub end: String,
}

impl CoverageRange {
    pub fn new(start: i64, end: i64) -> Self {
        Self {
            start: format_event_time_micros(start),
            end: format_event_time_micros(end),
        }
    }

    pub fn bounds(&self) -> Result<(i64, i64), String> {
        if !self.start.is_ascii() || !self.end.is_ascii() {
            return Err("coverage timestamps must be ASCII".into());
        }
        let bounds = (
            parse_event_time_micros(&self.start)?,
            parse_event_time_micros(&self.end)?,
        );
        if bounds.0 >= bounds.1 {
            return Err("coverage range must be nonempty and increasing".into());
        }
        Ok(bounds)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CoverageShortfall {
    pub reason: String,
    pub unresolved: CoverageRange,
}

/// One retained import/acquisition claim. Cumulative verified ranges can precede requested.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AcquisitionCoverage {
    pub acquisition_id: String,
    pub source_identity: String,
    pub requested: Vec<CoverageRange>,
    pub verified: Vec<CoverageRange>,
    pub shortfalls: Vec<CoverageShortfall>,
    pub unresolved: Vec<CoverageRange>,
}

/// Independent day evidence; never inferred from observation endpoints or row counts alone. A
/// validated complete native-bar grid (every slot of the day occupied) is the one admitted
/// count-based observation evidence, on a first-time migration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DayCoverage {
    pub date: String,
    pub family: DayFamily,
    pub acquisition_ids: Vec<String>,
    /// Identifies the retained source claim, or explains the absence of such a claim.
    /// For pages this must justify occurrence completeness independently of market coverage.
    pub basis: String,
    pub verified: Vec<CoverageRange>,
    pub unresolved: Vec<CoverageRange>,
    pub reason: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DailyCoverage {
    pub schema_version: u32,
    pub broker: BrokerId,
    pub provider_symbol: ProviderSymbol,
    pub role: DatasetRole,
    pub native_granularity: NativeGranularity,
    pub acquisitions: Vec<AcquisitionCoverage>,
    pub days: Vec<DayCoverage>,
}

fn ranges(ranges: &[CoverageRange]) -> Result<Vec<(i64, i64)>, String> {
    let mut previous = None;
    ranges
        .iter()
        .map(|range| {
            let bounds = range.bounds()?;
            if previous.is_some_and(|end| end > bounds.0) {
                return Err("coverage ranges must be ordered and nonoverlapping".into());
            }
            previous = Some(bounds.1);
            Ok(bounds)
        })
        .collect()
}

fn covered(range: (i64, i64), spans: &[(i64, i64)]) -> bool {
    let mut cursor = range.0;
    for &(start, end) in spans {
        if start > cursor {
            break;
        }
        cursor = cursor.max(end);
        if cursor >= range.1 {
            return true;
        }
    }
    false
}

impl DayCoverage {
    pub fn state(&self, rows: u64) -> Result<DayState, String> {
        let (start, end) = day_bounds(&self.date)?;
        let verified = ranges(&self.verified)?;
        let unresolved = ranges(&self.unresolved)?;
        let mut complement = Vec::new();
        let mut cursor = start;
        for &(from, to) in &verified {
            if from < start || to > end {
                return Err("verified day coverage lies outside its day".into());
            }
            if cursor < from {
                complement.push((cursor, from));
            }
            cursor = to;
        }
        if cursor < end {
            complement.push((cursor, end));
        }
        if unresolved != complement {
            return Err("day unresolved ranges must exactly complement verified coverage".into());
        }
        let state = if verified.is_empty() {
            DayState::Unknown
        } else if !unresolved.is_empty() {
            DayState::Partial
        } else if rows == 0 {
            DayState::EmptyKnown
        } else {
            DayState::Complete
        };
        if matches!(state, DayState::Partial | DayState::Unknown) {
            if self.reason.as_ref().is_none_or(|s| s.trim().is_empty()) {
                return Err("incomplete day coverage requires a reason".into());
            }
        } else if self.reason.is_some() {
            return Err("complete day coverage cannot have an unresolved reason".into());
        }
        Ok(state)
    }

    fn check(&self, day: &DayInventoryEntry) -> Result<(), String> {
        let unresolved: Vec<_> = day
            .unresolved
            .iter()
            .map(|i| {
                CoverageRange {
                    start: i.start.clone(),
                    end: i.end.clone(),
                }
                .bounds()
            })
            .collect::<Result<_, _>>()?;
        if day.state != self.state(day.rows)?
            || day.reason != self.reason
            || unresolved != ranges(&self.unresolved)?
        {
            return Err(format!(
                "{}/{}: day inventory disagrees with acquisition coverage",
                day.family, day.date
            ));
        }
        Ok(())
    }
}

impl DailyCoverage {
    pub fn from_json(bytes: &[u8]) -> Result<Self, String> {
        let coverage: Self =
            serde_json::from_slice(bytes).map_err(|e| format!("daily coverage: {e}"))?;
        coverage.validate()?;
        Ok(coverage)
    }

    pub fn to_json(&self) -> Vec<u8> {
        let mut bytes = serde_json::to_vec_pretty(self).expect("coverage serializes");
        bytes.push(b'\n');
        bytes
    }

    pub fn validate(&self) -> Result<(), String> {
        if self.schema_version != 2 {
            return Err("daily coverage requires schema_version 2".into());
        }
        let mut acquisitions = BTreeMap::new();
        for acquisition in &self.acquisitions {
            if acquisition.acquisition_id.trim().is_empty()
                || acquisition.source_identity.trim().is_empty()
                || acquisitions
                    .insert(&acquisition.acquisition_id, acquisition)
                    .is_some()
            {
                return Err(
                    "coverage requires unique nonempty acquisition/source identities".into(),
                );
            }
            let requested = ranges(&acquisition.requested)?;
            let verified = ranges(&acquisition.verified)?;
            let unresolved = ranges(&acquisition.unresolved)?;
            if verified
                .iter()
                .any(|&(a, b)| unresolved.iter().any(|&(c, d)| a < d && c < b))
            {
                return Err("acquisition verified and unresolved ranges overlap".into());
            }
            let mut accounted = verified.clone();
            accounted.extend(&unresolved);
            accounted.sort_unstable();
            if requested.iter().any(|&range| !covered(range, &accounted)) {
                return Err("requested acquisition coverage is not accounted for".into());
            }
            for shortfall in &acquisition.shortfalls {
                if shortfall.reason.trim().is_empty()
                    || !covered(shortfall.unresolved.bounds()?, &unresolved)
                {
                    return Err(
                        "shortfall requires a reason and an unresolved acquisition range".into(),
                    );
                }
            }
        }
        let mut days = BTreeSet::new();
        for day in &self.days {
            if day.family == DayFamily::Candles || !days.insert((day.family, &day.date)) {
                return Err("coverage requires unique observation/page days".into());
            }
            if day.basis.trim().is_empty() || day.acquisition_ids.is_empty() {
                return Err("day coverage requires a basis and acquisition references".into());
            }
            day.state(0)?;
            let mut seen = BTreeSet::new();
            let mut verified = Vec::new();
            for id in &day.acquisition_ids {
                if !seen.insert(id) {
                    return Err("repeated day acquisition reference".into());
                }
                let acquisition = acquisitions
                    .get(id)
                    .ok_or("unknown day acquisition reference")?;
                verified.extend(ranges(&acquisition.verified)?);
            }
            verified.sort_unstable();
            if day.family == DayFamily::Observations
                && ranges(&day.verified)?
                    .iter()
                    .any(|&range| !covered(range, &verified))
            {
                return Err("verified observation day lacks acquisition coverage".into());
            }
        }
        Ok(())
    }

    pub fn check_manifest(&self, manifest: &GenerationManifest) -> Result<(), String> {
        self.validate()?;
        if manifest.layout != Some(Layout::DailyV2)
            || self.broker != manifest.broker
            || self.provider_symbol != manifest.provider_symbol
            || self.role != manifest.role
            || self.native_granularity != manifest.native_granularity
        {
            return Err("daily coverage does not describe its dataset".into());
        }
        let days: BTreeMap<_, _> = self
            .days
            .iter()
            .map(|day| ((day.family, &day.date), day))
            .collect();
        if days.len() != manifest.day_inventory.len() {
            return Err("day inventory and coverage must contain exactly the same days".into());
        }
        for day in &manifest.day_inventory {
            days.get(&(day.family, &day.date))
                .ok_or("day inventory has no acquisition coverage")?
                .check(day)?;
        }
        Ok(())
    }
}

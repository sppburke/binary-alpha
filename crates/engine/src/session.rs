//! Explicit trading calendars. Bucket opens include both session boundaries; dates and
//! clock times are local to the named zone. UTC and the post-2007 US New York rule are
//! explicit and independent of host zoneinfo. Membership never claims acquisition coverage.
use std::collections::{BTreeMap, BTreeSet};

use crate::market::{civil_from_days, days_from_civil};

const DAY: i64 = 86_400;
const SECOND_MICROS: i64 = 1_000_000;
type Date = i64; // Gregorian days since 1970-01-01, using the shared market arithmetic.

#[derive(Debug, Clone, Copy)]
enum Zone {
    Utc,
    NewYork,
}

fn weekday(day: Date) -> i64 {
    (day + 3).rem_euclid(7) // 1970-01-01 was Thursday; Monday is zero.
}
fn tomorrow(day: Date) -> Result<Date, String> {
    day.checked_add(1).ok_or_else(|| err("date overflow"))
}
fn wall_seconds(day: Date, seconds: i64) -> Result<i64, String> {
    day.checked_mul(DAY)
        .and_then(|start| start.checked_add(seconds))
        .ok_or_else(|| err("date overflow"))
}
/// The second Sunday in March and first Sunday in November, local dates.
fn transitions(year: i64) -> (Date, Date) {
    let march = days_from_civil(year, 3, 1);
    let november = days_from_civil(year, 11, 1);
    (
        march + (6 - weekday(march)).rem_euclid(7) + 7,
        november + (6 - weekday(november)).rem_euclid(7),
    )
}
impl Zone {
    fn check_date(self, day: Date) -> Result<(), String> {
        let (year, _, _) = civil_from_days(day);
        if matches!(self, Self::NewYork) && year < 2007 {
            return Err(err(
                "America/New_York dates before 2007 are unsupported; the US daylight-saving rule starts in 2007",
            ));
        }
        if !(0..=9999).contains(&year) {
            return Err(err("calendar year must be between 0000 and 9999"));
        }
        Ok(())
    }
    fn offset_at(self, at: i64) -> Result<i64, String> {
        if matches!(self, Self::Utc) {
            return Ok(0);
        }
        let seconds = at.div_euclid(SECOND_MICROS);
        let day = seconds.div_euclid(DAY);
        self.check_date(day)?;
        let (spring, fall) = transitions(civil_from_days(day).0);
        // Spring 02:00 EST = 07:00 UTC; fall 02:00 EDT = 06:00 UTC.
        let daylight =
            (wall_seconds(spring, 7 * 3600)?..wall_seconds(fall, 6 * 3600)?).contains(&seconds);
        Ok(if daylight { -4 * 3600 } else { -5 * 3600 })
    }
    fn local_date(self, at: i64) -> Result<Date, String> {
        let seconds = at
            .div_euclid(SECOND_MICROS)
            .checked_add(self.offset_at(at)?)
            .ok_or_else(|| err("date overflow"))?;
        let day = seconds.div_euclid(DAY);
        self.check_date(day)?;
        Ok(day)
    }
    fn instant(self, day: Date, seconds: i64) -> Result<i64, String> {
        if !(0..=DAY).contains(&seconds) {
            return Err(err("local seconds outside day"));
        }
        let (day, seconds) = if seconds == DAY {
            (tomorrow(day)?, 0)
        } else {
            (day, seconds)
        };
        self.check_date(day)?;
        let offset = match self {
            Self::Utc => 0,
            Self::NewYork => {
                let (spring, fall) = transitions(civil_from_days(day).0);
                // Refuse both folds and skips: never silently move a configured boundary.
                if day == spring && (2 * 3600..3 * 3600).contains(&seconds) {
                    return Err(err(
                        "America/New_York skipped local hour 02:00:00..03:00:00 at spring DST transition",
                    ));
                }
                if day == fall && (3600..2 * 3600).contains(&seconds) {
                    return Err(err(
                        "America/New_York ambiguous local hour 01:00:00..02:00:00 at fall DST transition",
                    ));
                }
                let daylight = (day > spring || day == spring && seconds >= 3 * 3600)
                    && (day < fall || day == fall && seconds < 3600);
                if daylight { -4 * 3600 } else { -5 * 3600 }
            }
        };
        wall_seconds(day, seconds)?
            .checked_sub(offset)
            .and_then(|s| s.checked_mul(SECOND_MICROS))
            .ok_or_else(|| err("instant overflow"))
    }
}
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum Session {
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Boundary {
    /// Exact English weekday: monday through sunday.
    pub day: String,
    /// HH:MM:SS, local to the configured timezone.
    pub time: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EarlyClose {
    pub date: String,
    pub time: String,
}

#[derive(Debug, Clone)]
pub struct Calendar {
    zone: Zone,
    weekly: Option<(i64, i64)>,
    closed: BTreeSet<Date>,
    early: BTreeMap<Date, i64>,
}

fn err(error: impl std::fmt::Display) -> String {
    format!("session: {error}")
}
fn date(text: &str) -> Result<Date, String> {
    // Enforce the repository's exact Gregorian date grammar first.
    let (start, _) = crate::dataset::daily::day_bounds(text).map_err(err)?;
    Ok(start / (DAY * SECOND_MICROS))
}
fn clock(text: &str) -> Result<i64, String> {
    if text.len() != 8 || !text.is_ascii() || &text[2..3] != ":" || &text[5..6] != ":" {
        return Err(err("time must be HH:MM:SS"));
    }
    let parts = [&text[..2], &text[3..5], &text[6..]];
    let mut numbers = [0i64; 3];
    for (n, p) in numbers.iter_mut().zip(parts) {
        if !p.bytes().all(|c| c.is_ascii_digit()) {
            return Err(err("invalid clock"));
        }
        *n = p.parse().map_err(err)?;
    }
    if numbers[0] > 23 || numbers[1] > 59 || numbers[2] > 59 {
        return Err(err("invalid clock"));
    }
    Ok(numbers[0] * 3600 + numbers[1] * 60 + numbers[2])
}
fn boundary(b: &Boundary) -> Result<i64, String> {
    let day = [
        "monday",
        "tuesday",
        "wednesday",
        "thursday",
        "friday",
        "saturday",
        "sunday",
    ]
    .iter()
    .position(|d| *d == b.day)
    .ok_or_else(|| err("day must be monday through sunday"))?;
    Ok(day as i64 * 86400 + clock(&b.time)?)
}
impl Session {
    pub fn calendar(&self) -> Result<Calendar, String> {
        let mut calendar = Calendar {
            zone: Zone::Utc,
            weekly: None,
            closed: BTreeSet::new(),
            early: BTreeMap::new(),
        };
        if let Self::Weekly {
            timezone,
            open,
            close,
            closed_dates,
            early_closes,
        } = self
        {
            calendar.zone = match timezone.as_str() {
                "UTC" => Zone::Utc,
                "America/New_York" => Zone::NewYork,
                _ => {
                    return Err(err(format!(
                        "unsupported timezone `{timezone}`; supported zones: UTC, America/New_York"
                    )));
                }
            };
            let (open, close) = (boundary(open)?, boundary(close)?);
            if open == close {
                return Err(err(
                    "weekly open and close must differ; use kind = always for 24/7",
                ));
            }
            calendar.weekly = Some((open, close));
            for text in closed_dates {
                let day = date(text)?;
                calendar.zone.check_date(day)?;
                if !calendar.closed.insert(day) {
                    return Err(err("duplicate closed date"));
                }
            }
            for early in early_closes {
                let day = date(&early.date)?;
                calendar.zone.check_date(day)?;
                // Dated overrides can be checked at config parse time, including DST folds.
                calendar.zone.instant(day, clock(&early.time)?)?;
                if calendar.closed.contains(&day)
                    || calendar.early.insert(day, clock(&early.time)?).is_some()
                {
                    return Err(err("duplicate or closed early-close date"));
                }
            }
        }
        Ok(calendar)
    }
}
impl Calendar {
    fn local_date(&self, at: i64) -> Result<Date, String> {
        self.zone.local_date(at)
    }
    fn instant(&self, day: Date, seconds: i64) -> Result<i64, String> {
        self.zone.instant(day, seconds)
    }
    fn intervals(&self, day: Date) -> Result<Vec<(i64, i64)>, String> {
        if self.closed.contains(&day) {
            return Ok(Vec::new());
        }
        let d = weekday(day) * 86400;
        let spans = match self.weekly {
            None => vec![(0, 604800)],
            Some((a, b)) if a < b => vec![(a, b)],
            Some((a, b)) => vec![(0, b), (a, 604800)],
        };
        let mut result = Vec::new();
        for (a, b) in spans {
            let from = (a - d).max(0);
            let to = (b - d).min(*self.early.get(&day).unwrap_or(&86400));
            // A close at local midnight contributes that instant even though this day's
            // intersection has zero length. Closed dates above still remove the whole day.
            if from <= to {
                result.push((self.instant(day, from)?, self.instant(day, to)?));
            }
        }
        Ok(result)
    }
    /// A valid bucket belongs when its open is inside a session, including the close instant.
    /// Its end may extend past session close; the epoch grid is never shifted or shortened.
    /// A bucket opening before session open remains excluded even if its end is in session.
    pub fn contains(&self, start: i64, end: i64) -> Result<bool, String> {
        if end <= start {
            return Err(err("bucket end must follow start"));
        }
        Ok(self
            .intervals(self.local_date(start)?)?
            .into_iter()
            .any(|(open, close)| open <= start && start <= close))
    }
    /// First grid bucket at/after `at` whose open belongs to a session, including its close.
    /// The bucket's own close must still be <= `limit` (coverage/pending bound). Advancing
    /// across closed time visits local days, not every absent five-second bucket.
    pub fn next_bucket(
        &self,
        at: i64,
        duration: i64,
        offset: i64,
        limit: i64,
    ) -> Result<Option<i64>, String> {
        if duration <= 0 || offset < 0 || offset >= duration {
            return Err(err("invalid bucket grid"));
        }
        if at >= limit {
            return Ok(None);
        }
        let mut day = self.local_date(at)?;
        loop {
            for (a, b) in self.intervals(day)? {
                let lower = at.max(a);
                let rem = (lower - offset).rem_euclid(duration);
                let open = lower
                    .checked_add(if rem == 0 { 0 } else { duration - rem })
                    .ok_or_else(|| err("bucket overflow"))?;
                let Some(close) = open.checked_add(duration) else {
                    return Err(err("bucket overflow"));
                };
                if open <= b && close <= limit && self.contains(open, close)? {
                    return Ok(Some(open));
                }
            }
            if self.instant(day, 86400)? >= limit {
                return Ok(None);
            }
            day = tomorrow(day)?;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::market::parse_event_time_micros as t;
    fn fx() -> Session {
        Session::Weekly {
            timezone: "America/New_York".into(),
            open: Boundary {
                day: "sunday".into(),
                time: "17:00:00".into(),
            },
            close: Boundary {
                day: "friday".into(),
                time: "17:00:00".into(),
            },
            closed_dates: vec![],
            early_closes: vec![],
        }
    }
    #[test]
    fn dst_sunday_open_and_friday_close() {
        let c = fx().calendar().unwrap();
        for (date, hour) in [
            ("2025-03-02", 22),
            ("2025-03-09", 21),
            ("2025-10-26", 21),
            ("2025-11-02", 22),
            ("2026-03-01", 22),
            ("2026-03-08", 21),
            ("2026-10-25", 21),
            ("2026-11-01", 22),
        ] {
            let open = t(&format!("{date}T{hour}:00:00Z")).unwrap();
            assert!(!c.contains(open - 5_000_000, open).unwrap());
            assert!(c.contains(open, open + 5_000_000).unwrap());
        }
        for (date, hour) in [
            ("2025-03-07", 22),
            ("2025-03-14", 21),
            ("2025-10-31", 21),
            ("2025-11-07", 22),
            ("2026-03-06", 22),
            ("2026-03-13", 21),
            ("2026-10-30", 21),
            ("2026-11-06", 22),
        ] {
            let close = t(&format!("{date}T{hour}:00:00Z")).unwrap();
            assert!(c.contains(close - 5_000_000, close).unwrap());
            // Inclusive open-instant membership retains the Friday closing bucket.
            assert!(c.contains(close, close + 5_000_000).unwrap());
            assert!(!c.contains(close + 1, close + 5_000_000).unwrap());
        }
    }
    #[test]
    fn closed_dates_early_close_and_cross_day_bucket() {
        let mut s = fx();
        if let Session::Weekly {
            closed_dates,
            early_closes,
            ..
        } = &mut s
        {
            closed_dates.push("2026-12-25".into());
            early_closes.push(EarlyClose {
                date: "2026-12-24".into(),
                time: "13:00:00".into(),
            });
        }
        let c = s.calendar().unwrap();
        for time in [
            // The early close itself is included; the next bucket is outside the session.
            "2026-12-24T18:00:05Z",
            "2026-12-25T06:00:00Z",
            "2026-09-05T12:00:00Z",
        ] {
            let start = t(time).unwrap();
            assert!(!c.contains(start, start + 5_000_000).unwrap());
        }
        let start = t("2026-09-09T03:59:55Z").unwrap();
        assert!(c.contains(start, start + 15_000_000).unwrap());
        let start = t("2026-12-24T17:59:55Z").unwrap();
        // Open-instant membership includes straddling buckets and the early close itself.
        assert!(c.contains(start, start + 15_000_000).unwrap());
        assert!(c.contains(start + 5_000_000, start + 20_000_000).unwrap());
    }
    fn deriv() -> Session {
        Session::Weekly {
            timezone: "UTC".into(),
            open: Boundary {
                day: "monday".into(),
                time: "00:00:00".into(),
            },
            close: Boundary {
                day: "friday".into(),
                time: "20:55:00".into(),
            },
            closed_dates: vec![],
            early_closes: vec![EarlyClose {
                date: "2025-12-24".into(),
                time: "22:00:00".into(),
            }],
        }
    }
    #[test]
    fn inclusive_weekly_and_early_closes_preserve_grid_and_coverage_limit() {
        let c = deriv().calendar().unwrap();
        for close in ["2026-09-04T20:55:00Z", "2025-12-24T22:00:00Z"] {
            let close = t(close).unwrap();
            for duration in [5_000_000, 60_000_000] {
                assert!(c.contains(close, close + duration).unwrap());
                assert_eq!(
                    c.next_bucket(close, duration, 0, close + duration).unwrap(),
                    Some(close)
                );
                // Session membership is inclusive, but verified coverage remains exclusive.
                assert_eq!(
                    c.next_bucket(close, duration, 0, close + duration - 1)
                        .unwrap(),
                    None
                );
                assert!(!c.contains(close + 1, close + duration).unwrap());
            }
            // The 15s/5s epoch grid opens ten seconds before this close and ends five after.
            let open = close - 10_000_000;
            assert!(c.contains(open, close + 5_000_000).unwrap());
            assert_eq!(
                c.next_bucket(open, 15_000_000, 5_000_000, close + 5_000_000)
                    .unwrap(),
                Some(open)
            );
        }
        let open = t("2026-09-07T00:00:00Z").unwrap();
        assert!(!c.contains(open - 10_000_000, open + 5_000_000).unwrap());
        assert_eq!(
            c.next_bucket(open - 10_000_000, 15_000_000, 5_000_000, open + 20_000_000)
                .unwrap(),
            Some(open + 5_000_000)
        );
    }
    #[test]
    fn midnight_closes_are_inclusive_without_leaking_into_closed_dates() {
        let mut s = deriv();
        if let Session::Weekly {
            close,
            early_closes,
            ..
        } = &mut s
        {
            close.time = "00:00:00".into();
            early_closes[0].time = "00:00:00".into();
        }
        let c = s.calendar().unwrap();
        for close in ["2026-09-04T00:00:00Z", "2025-12-24T00:00:00Z"] {
            let close = t(close).unwrap();
            assert!(c.contains(close, close + 5_000_000).unwrap());
            assert_eq!(
                c.next_bucket(close, 5_000_000, 0, close + 5_000_000)
                    .unwrap(),
                Some(close)
            );
            assert!(!c.contains(close + 1, close + 5_000_000).unwrap());
        }
        if let Session::Weekly { closed_dates, .. } = &mut s {
            closed_dates.push("2026-09-04".into());
        }
        let c = s.calendar().unwrap();
        let close = t("2026-09-04T00:00:00Z").unwrap();
        assert!(!c.contains(close, close + 5_000_000).unwrap());
        assert_eq!(
            c.next_bucket(close, 5_000_000, 0, close + 5_000_000)
                .unwrap(),
            None
        );
    }
    #[test]
    fn complete_week_counts_include_one_closing_bucket() {
        for (session, open, close, old_five_second_count) in [
            (
                deriv(),
                "2026-09-07T00:00:00Z",
                "2026-09-11T20:55:00Z",
                84_180,
            ),
            (fx(), "2026-03-08T21:00:00Z", "2026-03-13T21:00:00Z", 86_400),
            (fx(), "2026-11-01T22:00:00Z", "2026-11-06T22:00:00Z", 86_400),
        ] {
            let c = session.calendar().unwrap();
            let (open, close) = (t(open).unwrap(), t(close).unwrap());
            for duration in [5_000_000, 60_000_000] {
                let mut at = open;
                let mut count = 0;
                let mut last = None;
                while let Some(bucket) = c.next_bucket(at, duration, 0, close + duration).unwrap() {
                    assert_eq!(bucket, open + count * duration);
                    assert!(c.contains(bucket, bucket + duration).unwrap());
                    count += 1;
                    last = Some(bucket);
                    at = bucket + duration;
                }
                // The binding inclusive-close rule adds exactly one aligned closing bucket.
                assert_eq!(count, old_five_second_count * 5_000_000 / duration + 1);
                assert_eq!(last, Some(close));
            }
        }
    }
    #[test]
    fn explicit_dst_wall_conversion_rejects_both_skips_and_folds() {
        let zone = Zone::NewYork;
        for (spring, fall) in [("2025-03-09", "2025-11-02"), ("2026-03-08", "2026-11-01")] {
            let march = date(spring).unwrap();
            let november = date(fall).unwrap();
            for seconds in [7200, 8999, 10799] {
                assert!(
                    zone.instant(march, seconds)
                        .unwrap_err()
                        .contains("skipped local hour")
                );
            }
            for seconds in [3600, 5399, 7199] {
                assert!(
                    zone.instant(november, seconds)
                        .unwrap_err()
                        .contains("ambiguous local hour")
                );
            }
            for (day, text, local, utc) in [
                (march, spring, 7199, "06:59:59"),
                (march, spring, 10800, "07:00:00"),
                (november, fall, 3599, "04:59:59"),
                (november, fall, 7200, "07:00:00"),
            ] {
                assert_eq!(
                    zone.instant(day, local).unwrap(),
                    t(&format!("{text}T{utc}Z")).unwrap()
                );
            }
            for (text, utc, offset) in [
                (spring, "06:59:59", -18000),
                (spring, "07:00:00", -14400),
                (fall, "05:59:59", -14400),
                (fall, "06:00:00", -18000),
            ] {
                assert_eq!(
                    zone.offset_at(t(&format!("{text}T{utc}Z")).unwrap())
                        .unwrap(),
                    offset
                );
            }
            assert_eq!(
                zone.instant(march, DAY).unwrap() - zone.instant(march, 0).unwrap(),
                23 * 3600 * SECOND_MICROS
            );
            assert_eq!(
                zone.instant(november, DAY).unwrap() - zone.instant(november, 0).unwrap(),
                25 * 3600 * SECOND_MICROS
            );
        }
    }
    #[test]
    fn calendar_arithmetic_and_supported_years() {
        assert_eq!(weekday(date("1970-01-01").unwrap()), 3);
        for (last, next) in [
            ("2000-02-28", "2000-02-29"),
            ("2000-02-29", "2000-03-01"),
            ("1900-02-28", "1900-03-01"),
            ("2025-12-31", "2026-01-01"),
        ] {
            assert_eq!(tomorrow(date(last).unwrap()).unwrap(), date(next).unwrap());
        }
        let old = t("2006-12-31T23:59:59Z").unwrap();
        assert!(
            fx().calendar()
                .unwrap()
                .contains(old, old + SECOND_MICROS)
                .unwrap_err()
                .contains("dates before 2007")
        );
        assert!(
            Zone::NewYork
                .instant(date("2006-12-31").unwrap(), 12 * 3600)
                .unwrap_err()
                .contains("dates before 2007")
        );
        assert_eq!(
            Zone::Utc.instant(date("1970-01-01").unwrap(), 0).unwrap(),
            0
        );
        assert!(
            Session::Always
                .calendar()
                .unwrap()
                .contains(old, old + SECOND_MICROS)
                .unwrap()
        );
        let mut s = fx();
        if let Session::Weekly { closed_dates, .. } = &mut s {
            closed_dates.push("2006-12-25".into());
        }
        assert!(s.calendar().unwrap_err().contains("dates before 2007"));
    }
    #[test]
    fn unsupported_zones_refused_when_config_is_parsed() {
        let config = r#"schema_version=1
run_mode="research"
[storage]
historical_data_dir="unused"
publication_uri="file:///unused"
[[instruments]]
broker="fixture"
provider_symbol="FX"
quote_currency="USD"
price_scale=4
native_granularity={kind="tick"}
candles=[{duration_seconds=5,offset_seconds=0}]
session={kind="weekly",timezone="Europe/London",open={day="monday",time="00:00:00"},close={day="friday",time="20:55:00"}}
"#;
        let error = crate::config::Config::parse(config).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("supported zones: UTC, America/New_York"),
            "{error}"
        );
    }
    #[test]
    fn reject_bad_calendars_and_ambiguous_boundaries() {
        assert!(clock("25:00:00").is_err());
        assert!(clock("1:00:00").is_err());
        assert!(date("2026-02-29").is_err());
        let mut s = fx();
        if let Session::Weekly { timezone, .. } = &mut s {
            *timezone = "invalid".into();
        }
        assert!(s.calendar().is_err());
        let mut s = fx();
        if let Session::Weekly { open, .. } = &mut s {
            open.time = "01:30:00".into();
        }
        let c = s.calendar().unwrap();
        let start = t("2026-11-01T05:30:00Z").unwrap();
        assert!(c.contains(start, start + 5_000_000).is_err());
    }
}

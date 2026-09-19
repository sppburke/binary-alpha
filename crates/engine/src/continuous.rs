//! Pure bounded-state transformation of finalized feed candles into a session grid.
//! Feed diagnostics on actual candles are preserved. Synthetic rows cannot become evidence.
use crate::{
    config::CandleSpec,
    session::Calendar,
    stream::{Candle, Flags},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Fill {
    None,
    Source,
    Engine,
}
impl Fill {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Source => "source",
            Self::Engine => "engine",
        }
    }
}
/// The explicit daily column is a lossless classification of these facts, not a second
/// mutable authority. Ticks have no volume and are never classified as source fills.
pub fn fill(c: &Candle) -> Fill {
    if c.observations == 0 {
        Fill::Engine
    } else if c.volume == Some(0.0)
        && c.open_units == c.high_units
        && c.open_units == c.low_units
        && c.open_units == c.close_units
    {
        Fill::Source
    } else {
        Fill::None
    }
}

pub struct Continuous {
    calendar: Calendar,
    duration: i64,
    offset: i64,
    /// Exclusive event-time end of verified acquisition coverage, never a requested end.
    coverage_end: Option<i64>,
    verified: Vec<(i64, i64)>,
    previous_input: Option<i64>,
    last: Option<Candle>,
    has_volume: bool,
}
impl Continuous {
    pub fn new(
        calendar: Calendar,
        spec: &CandleSpec,
        verified: Vec<(i64, i64)>,
        has_volume: bool,
    ) -> Result<Self, String> {
        if spec.duration_seconds == 0 || spec.offset_seconds >= spec.duration_seconds {
            return Err("invalid continuous candle grid".into());
        }
        let mut merged: Vec<(i64, i64)> = Vec::new();
        for (start, end) in verified {
            if start >= end
                || merged
                    .last()
                    .is_some_and(|(_, prior_end)| start < *prior_end)
            {
                return Err(
                    "verified coverage must be ordered, nonoverlapping, nonempty ranges".into(),
                );
            }
            if let Some((_, prior_end)) = merged.last_mut()
                && *prior_end == start
            {
                *prior_end = end;
            } else {
                merged.push((start, end));
            }
        }
        let coverage_end = merged.last().map(|(_, end)| *end);
        Ok(Self {
            calendar,
            duration: i64::from(spec.duration_seconds) * 1_000_000,
            offset: i64::from(spec.offset_seconds) * 1_000_000,
            coverage_end,
            verified: merged,
            previous_input: None,
            last: None,
            has_volume,
        })
    }
    /// One ordered finalized candle in; zero or more ordered candles out, without buffering
    /// the gap or a day. `emit` errors stop the transformation immediately.
    pub fn push(
        &mut self,
        mut c: Candle,
        emit: &mut impl FnMut(Candle) -> Result<(), String>,
    ) -> Result<(), String> {
        if self.previous_input.is_some_and(|p| c.open_time_micros < p)
            || c.close_time_micros.checked_sub(c.open_time_micros) != Some(self.duration)
            || (c.open_time_micros - self.offset).rem_euclid(self.duration) != 0
        {
            return Err(
                "continuous candles require ordered, nonoverlapping finalized grid buckets".into(),
            );
        }
        self.previous_input = Some(c.close_time_micros);
        if !self
            .calendar
            .contains(c.open_time_micros, c.close_time_micros)?
        {
            return Ok(());
        }
        let kind = fill(&c);
        if kind == Fill::Engine {
            return Err("continuous input must be finalized feed candles, not engine fills".into());
        }
        if self.last.is_none() && kind == Fill::Source {
            return Ok(());
        }
        // A later finalized candle proves the intervening absence, even in an acquisition
        // with unresolved ranges. That absence is represented, never declared clean/complete.
        self.extend(c.open_time_micros, c.known_at_micros, emit)?;
        if kind == Fill::Source {
            c.flags.frozen = true;
        }
        emit(c.clone())?;
        self.last = Some(c);
        Ok(())
    }
    /// The unfinished feed candle is never replaced by a fill. Once input is exhausted,
    /// empty trailing buckets need coverage evidence and stop before its exclusive end.
    pub fn finish(
        &mut self,
        pending_open: Option<i64>,
        emit: &mut impl FnMut(Candle) -> Result<(), String>,
    ) -> Result<(), String> {
        if let Some(end) = self.coverage_end {
            let pending_open = match pending_open {
                Some(open)
                    if self.calendar.contains(
                        open,
                        open.checked_add(self.duration)
                            .ok_or("pending close overflow")?,
                    )? =>
                {
                    Some(open)
                }
                _ => None,
            };
            self.extend(pending_open.map_or(end, |p| p.min(end)), end, emit)?;
        }
        Ok(())
    }
    fn covered(&self, open: i64, close: i64) -> bool {
        let index = self.verified.partition_point(|(_, end)| *end <= open);
        self.verified
            .get(index)
            .is_some_and(|(start, end)| *start <= open && close <= *end)
    }
    fn extend(
        &mut self,
        limit: i64,
        proof_at: i64,
        emit: &mut impl FnMut(Candle) -> Result<(), String>,
    ) -> Result<(), String> {
        while let Some(prior) = &self.last {
            let Some(open) = self.calendar.next_bucket(
                prior.close_time_micros,
                self.duration,
                self.offset,
                limit,
            )?
            else {
                break;
            };
            let close = open
                .checked_add(self.duration)
                .ok_or("continuous candle close overflow")?;
            let c = Candle {
                open_time_micros: open,
                close_time_micros: close,
                // A fully covered interval's logical proof boundary is its close. It must
                // not change when a later acquisition adds input or extends coverage. In an
                // unresolved interval only later finalized input/snapshot bounds the absence.
                known_at_micros: if self.covered(open, close) {
                    close
                } else {
                    proof_at.max(close)
                },
                // These clocks identify the carried price's last real source record. With
                // observations=0 they do not assert an event inside this interval.
                first_event_micros: prior.last_event_micros,
                last_event_micros: prior.last_event_micros,
                active_span_micros: 0,
                open_units: prior.close_units,
                high_units: prior.close_units,
                low_units: prior.close_units,
                close_units: prior.close_units,
                observations: 0,
                duplicates: 0,
                volume: self.has_volume.then_some(0.0),
                gap_before_micros: None,
                max_gap_inside_micros: 0,
                missing_buckets_before: 0,
                frozen_observations: 0,
                frozen_micros: 0,
                max_jump_basis_points: 0,
                max_delayed_jump_basis_points: 0,
                max_reopen_jump_basis_points: 0,
                flags: Flags {
                    low_activity: true,
                    hard_low_activity: true,
                    frozen: true,
                    short_span: true,
                    ..Flags::default()
                },
            };
            emit(c.clone())?;
            self.last = Some(c);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        market::parse_event_time_micros as time,
        session::{Boundary, Session},
    };
    fn c(t: i64) -> Candle {
        Candle {
            open_time_micros: t,
            close_time_micros: t + 5_000_000,
            known_at_micros: t + 5_000_000,
            first_event_micros: t,
            last_event_micros: t,
            active_span_micros: 0,
            open_units: 100,
            high_units: 100,
            low_units: 100,
            close_units: 100,
            observations: 1,
            duplicates: 0,
            volume: None,
            gap_before_micros: None,
            max_gap_inside_micros: 0,
            missing_buckets_before: 0,
            frozen_observations: 0,
            frozen_micros: 0,
            max_jump_basis_points: 0,
            max_delayed_jump_basis_points: 0,
            max_reopen_jump_basis_points: 0,
            flags: Flags::default(),
        }
    }
    fn run(
        session: Session,
        inputs: Vec<Candle>,
        end: Option<i64>,
        pending: Option<i64>,
    ) -> Vec<Candle> {
        let spec = CandleSpec {
            duration_seconds: 5,
            offset_seconds: 0,
            min_observations: None,
            hard_min_observations: None,
        };
        let mut tr = Continuous::new(
            session.calendar().unwrap(),
            &spec,
            end.map(|end| vec![(i64::MIN, end)]).unwrap_or_default(),
            inputs.iter().any(|c| c.volume.is_some()),
        )
        .unwrap();
        let mut out = Vec::new();
        let mut emit = |c| {
            out.push(c);
            Ok(())
        };
        for c in inputs {
            tr.push(c, &mut emit).unwrap();
        }
        tr.finish(pending, &mut emit).unwrap();
        out
    }
    fn week() -> Session {
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
            early_closes: vec![],
        }
    }
    #[test]
    fn fills_gap_preserving_raw_evidence_and_no_leading_fill() {
        let mut b = c(20_000_000);
        b.missing_buckets_before = 2;
        b.flags.missing_before = true;
        b.flags.gap_before = true;
        b.gap_before_micros = Some(15_000_000);
        let out = run(
            Session::Always,
            vec![c(5_000_000), b.clone()],
            Some(31_000_000),
            None,
        );
        assert_eq!(
            out.iter().map(|c| c.open_time_micros).collect::<Vec<_>>(),
            vec![5, 10, 15, 20, 25]
                .into_iter()
                .map(|s| s * 1_000_000)
                .collect::<Vec<_>>()
        );
        assert_eq!(out[3], b);
        for row in [&out[1], &out[2], &out[4]] {
            assert_eq!(fill(row), Fill::Engine);
            assert!(!row.flags.clean());
            assert!(!row.flags.complete());
            assert_eq!(row.close_units, 100);
        }
    }
    #[test]
    fn weekend_skipped_and_next_session_filled_from_open() {
        let fri = time("2026-09-04T20:54:55Z").unwrap();
        let mon = time("2026-09-07T00:00:00Z").unwrap();
        let out = run(week(), vec![c(fri), c(mon + 10_000_000)], None, None);
        assert_eq!(
            out.iter().map(|c| c.open_time_micros).collect::<Vec<_>>(),
            // Inclusive session close adds the Friday 20:55:00 engine fill.
            vec![fri, fri + 5_000_000, mon, mon + 5_000_000, mon + 10_000_000]
        );
        assert_eq!(fill(&out[1]), Fill::Engine);
    }
    #[test]
    fn pocket_closed_source_and_leading_bars_removed_and_pending_not_filled() {
        let fri = time("2026-09-04T20:54:50Z").unwrap();
        let sat = time("2026-09-05T00:00:00Z").unwrap();
        let mon = time("2026-09-07T00:00:00Z").unwrap();
        let mut initial = c(fri - 5_000_000);
        initial.volume = Some(0.0);
        let mut a = c(fri);
        a.volume = Some(1.0);
        let mut source = c(fri + 5_000_000);
        source.volume = Some(0.0);
        let out = run(
            week(),
            vec![initial, a, source, c(sat)],
            Some(mon + 20_000_000),
            Some(mon + 10_000_000),
        );
        assert_eq!(
            out.iter().map(|c| c.open_time_micros).collect::<Vec<_>>(),
            // Open-instant membership includes the closing bucket even with no source bar.
            vec![fri, fri + 5_000_000, fri + 10_000_000, mon, mon + 5_000_000]
        );
        assert_eq!(fill(&out[1]), Fill::Source);
        assert!(!out[1].flags.clean());
        assert!(out[1].flags.complete());
        assert_eq!(fill(&out[2]), Fill::Engine);
        // The closing fill moves the first Monday fill from index 2 to index 3.
        assert_eq!(out[3].volume, Some(0.0));
        assert!(
            out.windows(2)
                .all(|p| p[0].open_time_micros < p[1].open_time_micros)
        );
    }

    #[test]
    fn closing_fills_require_coverage_and_never_replace_pending_quotes() {
        let mut session = week();
        if let Session::Weekly { early_closes, .. } = &mut session {
            early_closes.push(crate::session::EarlyClose {
                date: "2025-12-24".into(),
                time: "22:00:00".into(),
            });
        }
        for close in ["2026-09-04T20:55:00Z", "2025-12-24T22:00:00Z"] {
            let close = time(close).unwrap();
            let first = c(close - 5_000_000);
            for end in [None, Some(close), Some(close + 4_999_999)] {
                assert_eq!(
                    run(session.clone(), vec![first.clone()], end, None),
                    vec![first.clone()]
                );
            }
            let filled = run(
                session.clone(),
                vec![first.clone()],
                Some(close + 5_000_000),
                None,
            );
            assert_eq!(filled.len(), 2);
            assert_eq!(filled[1].open_time_micros, close);
            assert_eq!(filled[1].close_time_micros, close + 5_000_000);
            assert_eq!(filled[1].known_at_micros, close + 5_000_000);
            assert_eq!(fill(&filled[1]), Fill::Engine);
            assert!(!filled[1].flags.clean());
            assert!(!filled[1].flags.complete());
            assert_eq!(
                run(
                    session.clone(),
                    vec![first.clone()],
                    Some(close + 5_000_000),
                    Some(close)
                ),
                vec![first]
            );
        }
    }
    #[test]
    fn unknown_coverage_cannot_extend_tail_and_no_price_means_no_fills() {
        assert_eq!(run(Session::Always, vec![c(0)], None, None).len(), 1);
        assert!(run(Session::Always, vec![], Some(100_000_000), None).is_empty());
    }
    #[test]
    fn covered_fills_are_identical_when_later_input_extends_the_snapshot() {
        let old = run(Session::Always, vec![c(0)], Some(15_000_000), None);
        let new = run(
            Session::Always,
            vec![c(0), c(20_000_000)],
            Some(30_000_000),
            None,
        );
        assert_eq!(old, new[..old.len()]);
        assert_eq!(old[1].known_at_micros, old[1].close_time_micros);
    }
}

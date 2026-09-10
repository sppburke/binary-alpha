//! Neutral market records: instrument identity, exact tick prices, and bars.
//!
//! `Tick` and `Bar` are distinct records; nothing here converts one into the other. Times are
//! provider event times in Coordinated Universal Time with an explicit unit.

use std::fmt;

use serde::{Deserialize, Serialize};

/// Declares a non-empty string newtype used as a neutral identifier.
macro_rules! identifier {
    ($(#[$meta:meta])* $name:ident) => {
        $(#[$meta])*
        #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Deserialize, Serialize)]
        #[serde(try_from = "String", into = "String")]
        pub struct $name(String);

        impl $name {
            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl TryFrom<String> for $name {
            type Error = String;

            fn try_from(text: String) -> Result<Self, Self::Error> {
                if text.is_empty() {
                    return Err(format!("{} must not be empty", stringify!($name)));
                }
                if text.bytes().any(|byte| byte.is_ascii_control()) {
                    return Err(format!("{} `{}` contains a control character", stringify!($name), text.escape_default()));
                }
                Ok(Self(text))
            }
        }

        impl From<$name> for String {
            fn from(value: $name) -> Self {
                value.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(&self.0)
            }
        }
    };
}

identifier! {
    /// The neutral name of a broker, such as `pocket_option`.
    BrokerId
}

identifier! {
    /// The symbol text a provider uses for an instrument, such as `AEDCNY_otc`.
    ProviderSymbol
}

/// A neutral instrument identity: one broker and that broker's provider symbol.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Deserialize, Serialize)]
pub struct InstrumentId {
    pub broker: BrokerId,
    pub provider_symbol: ProviderSymbol,
}

impl fmt::Display for InstrumentId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.broker, self.provider_symbol)
    }
}

/// The number of decimal fraction digits carried by integer price units: `0` to `18`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Deserialize, Serialize)]
#[serde(try_from = "u8", into = "u8")]
pub struct PriceScale(u8);

impl PriceScale {
    pub const MAX: u8 = 18;

    pub const fn digits(self) -> u8 {
        self.0
    }

    /// Ten to the power of the scale.
    const fn unit(self) -> i64 {
        10_i64.pow(self.0 as u32)
    }
}

impl TryFrom<u8> for PriceScale {
    type Error = String;

    fn try_from(digits: u8) -> Result<Self, Self::Error> {
        if digits > Self::MAX {
            return Err(format!("price_scale {digits} exceeds {}", Self::MAX));
        }
        Ok(Self(digits))
    }
}

impl From<PriceScale> for u8 {
    fn from(scale: PriceScale) -> Self {
        scale.0
    }
}

/// One provider tick: the provider event time in Unix microseconds and the price in integer
/// units at the dataset's [`PriceScale`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Tick {
    pub event_time_micros: i64,
    pub price_units: i64,
}

/// The exact header of a native three-column tick file.
pub const TICK_HEADER: &str = "time_utc,symbol,price";

/// Parses one `time_utc,symbol,price` line whose symbol must equal `source_symbol`.
pub fn parse_tick_line(
    line: &str,
    source_symbol: &ProviderSymbol,
    scale: PriceScale,
) -> Result<Tick, String> {
    let mut fields = line.split(',');
    let (Some(time), Some(symbol), Some(price), None) =
        (fields.next(), fields.next(), fields.next(), fields.next())
    else {
        return Err(format!(
            "expected three comma-separated fields, found `{line}`"
        ));
    };
    if symbol != source_symbol.as_str() {
        return Err(format!(
            "symbol `{symbol}` is not the declared source symbol `{source_symbol}`"
        ));
    }
    Ok(Tick {
        event_time_micros: parse_event_time_micros(time)?,
        price_units: parse_price_units(price, scale)?,
    })
}

/// Parses `YYYY-MM-DDTHH:MM:SS[.fraction]Z` into Unix microseconds with integer arithmetic,
/// rejecting more than six fraction digits.
pub fn parse_event_time_micros(text: &str) -> Result<i64, String> {
    let invalid =
        || format!("invalid timestamp `{text}`, expected YYYY-MM-DDTHH:MM:SS[.fraction]Z");
    let body = text.strip_suffix('Z').ok_or_else(invalid)?;
    let (clock, fraction) = match body.split_once('.') {
        Some((_, "")) => {
            return Err(format!(
                "timestamp `{text}` fraction must be one to six digits"
            ));
        }
        Some(parts) => parts,
        None => (body, ""),
    };
    let bytes = clock.as_bytes();
    if bytes.len() != 19
        || bytes[4] != b'-'
        || bytes[7] != b'-'
        || bytes[10] != b'T'
        || bytes[13] != b':'
        || bytes[16] != b':'
    {
        return Err(invalid());
    }
    let number = |range: std::ops::Range<usize>| -> Result<i64, String> {
        let digits = &clock[range];
        if !digits.bytes().all(|byte| byte.is_ascii_digit()) {
            return Err(invalid());
        }
        digits.parse::<i64>().map_err(|_| invalid())
    };
    let (year, month, day) = (number(0..4)?, number(5..7)?, number(8..10)?);
    let (hour, minute, second) = (number(11..13)?, number(14..16)?, number(17..19)?);
    if !(1..=12).contains(&month)
        || day < 1
        || day > days_in_month(year, month)
        || hour > 23
        || minute > 59
        || second > 59
    {
        return Err(invalid());
    }
    if fraction.len() > 6 || !fraction.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(format!(
            "timestamp `{text}` fraction must be one to six digits"
        ));
    }
    let micros = if fraction.is_empty() {
        0
    } else {
        fraction.parse::<i64>().map_err(|_| invalid())? * 10_i64.pow(6 - fraction.len() as u32)
    };
    let seconds = days_from_civil(year, month, day) * 86_400 + hour * 3_600 + minute * 60 + second;
    seconds
        .checked_mul(1_000_000)
        .and_then(|value| value.checked_add(micros))
        .ok_or_else(|| format!("timestamp `{text}` overflows microseconds"))
}

/// Renders Unix microseconds as `YYYY-MM-DDTHH:MM:SS.ffffffZ`.
pub fn format_event_time_micros(micros: i64) -> String {
    let seconds = micros.div_euclid(1_000_000);
    let fraction = micros.rem_euclid(1_000_000);
    let (year, month, day) = civil_from_days(seconds.div_euclid(86_400));
    let clock = seconds.rem_euclid(86_400);
    format!(
        "{year:04}-{month:02}-{day:02}T{:02}:{:02}:{:02}.{fraction:06}Z",
        clock / 3_600,
        clock % 3_600 / 60,
        clock % 60
    )
}

fn days_in_month(year: i64, month: i64) -> i64 {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        _ if year % 4 == 0 && (year % 100 != 0 || year % 400 == 0) => 29,
        _ => 28,
    }
}

/// Days since 1970-01-01 of a proleptic Gregorian date (Howard Hinnant's algorithm).
fn days_from_civil(year: i64, month: i64, day: i64) -> i64 {
    let year = if month <= 2 { year - 1 } else { year };
    let era = year.div_euclid(400);
    let year_of_era = year - era * 400;
    let month_index = (month + 9) % 12;
    let day_of_year = (153 * month_index + 2) / 5 + day - 1;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    era * 146_097 + day_of_era - 719_468
}

fn civil_from_days(days: i64) -> (i64, i64, i64) {
    let days = days + 719_468;
    let era = days.div_euclid(146_097);
    let day_of_era = days - era * 146_097;
    let year_of_era =
        (day_of_era - day_of_era / 1_460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_index = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * month_index + 2) / 5 + 1;
    let month = if month_index < 10 {
        month_index + 3
    } else {
        month_index - 9
    };
    let year = year_of_era + era * 400 + i64::from(month <= 2);
    (year, month, day)
}

/// Parses decimal text such as `-1.80787` into checked integer units at `scale`, rejecting
/// text that is not a plain decimal number, needs rounding, or overflows.
pub fn parse_price_units(text: &str, scale: PriceScale) -> Result<i64, String> {
    let (negative, unsigned) = match text.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, text),
    };
    let (whole, fraction) = match unsigned.split_once('.') {
        Some((_, "")) | None if unsigned.is_empty() || unsigned.ends_with('.') => {
            return Err(format!("invalid decimal price `{text}`"));
        }
        Some(parts) => parts,
        None => (unsigned, ""),
    };
    let digits = |part: &str| part.bytes().all(|byte| byte.is_ascii_digit());
    if !digits(whole) || !digits(fraction) {
        return Err(format!("invalid decimal price `{text}`"));
    }
    if fraction.len() > usize::from(scale.digits()) {
        return Err(format!(
            "price `{text}` has {} fraction digits, more than the declared price_scale {}",
            fraction.len(),
            scale.digits()
        ));
    }
    let overflow = || {
        format!(
            "price `{text}` overflows signed 64-bit units at price_scale {}",
            scale.digits()
        )
    };
    let whole_units = if whole.is_empty() {
        0
    } else {
        whole.parse::<i128>().map_err(|_| overflow())?
    };
    let fraction_units = if fraction.is_empty() {
        0
    } else {
        fraction.parse::<i128>().map_err(|_| overflow())?
            * 10_i128.pow(u32::from(scale.digits()) - fraction.len() as u32)
    };
    let magnitude = whole_units
        .checked_mul(i128::from(scale.unit()))
        .and_then(|value| value.checked_add(fraction_units))
        .ok_or_else(overflow)?;
    i64::try_from(if negative { -magnitude } else { magnitude }).map_err(|_| overflow())
}

/// Converts a binary floating-point archive price into integer units at `scale` by parsing its
/// shortest round-trip decimal rendering, so a value whose rendering needs more fraction digits
/// than the scale, or is not a plain finite number, is rejected rather than rounded.
pub fn float_price_units(value: f64, scale: PriceScale) -> Result<i64, String> {
    parse_price_units(&value.to_string(), scale)
}

/// Rejects backwards time and conflicting prices at one provider event time.
#[derive(Debug, Default)]
pub struct TickSequence {
    last: Option<Tick>,
}

impl TickSequence {
    pub fn accept(&mut self, tick: Tick) -> Result<(), String> {
        if let Some(last) = self.last {
            if tick.event_time_micros < last.event_time_micros {
                return Err(format!(
                    "backwards time: {} follows {}",
                    format_event_time_micros(tick.event_time_micros),
                    format_event_time_micros(last.event_time_micros)
                ));
            }
            if tick.event_time_micros == last.event_time_micros
                && tick.price_units != last.price_units
            {
                return Err(format!(
                    "conflicting prices {} and {} at {}",
                    last.price_units,
                    tick.price_units,
                    format_event_time_micros(tick.event_time_micros)
                ));
            }
        }
        self.last = Some(tick);
        Ok(())
    }
}

/// One left-closed bar starting at `start_unix_s` and lasting `period_s` seconds. Prices are
/// the archive's binary floating point and never cross a money, order, accounting, or risk
/// boundary.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Bar {
    pub start_unix_s: i64,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: f64,
    pub period_s: u16,
}

impl Bar {
    /// Finite values, non-negative volume, consistent price relationships, the expected period,
    /// and a start on the period grid.
    pub fn validate(&self, period_s: u16) -> Result<(), String> {
        let values = [self.open, self.high, self.low, self.close, self.volume];
        if values.iter().any(|value| !value.is_finite()) {
            return Err(format!("non-finite value in bar at {}", self.start_unix_s));
        }
        if self.volume < 0.0 {
            return Err(format!("negative volume in bar at {}", self.start_unix_s));
        }
        if self.high < self.open.max(self.close)
            || self.low > self.open.min(self.close)
            || self.high < self.low
        {
            return Err(format!(
                "invalid high/low relationship in bar at {}",
                self.start_unix_s
            ));
        }
        if self.period_s != period_s {
            return Err(format!(
                "bar at {} has period {} seconds, expected {period_s}",
                self.start_unix_s, self.period_s
            ));
        }
        if self.start_unix_s.rem_euclid(i64::from(period_s)) != 0 {
            return Err(format!(
                "bar at {} is off the {period_s}-second grid",
                self.start_unix_s
            ));
        }
        Ok(())
    }
}

/// Requires strictly increasing bar start times across one dataset.
#[derive(Debug, Default)]
pub struct BarSequence {
    last_start: Option<i64>,
}

impl BarSequence {
    pub fn accept(&mut self, start_unix_s: i64) -> Result<(), String> {
        if let Some(last) = self.last_start
            && start_unix_s <= last
        {
            return Err(format!("bar at {start_unix_s} does not follow {last}"));
        }
        self.last_start = Some(start_unix_s);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn scale(digits: u8) -> PriceScale {
        PriceScale::try_from(digits).unwrap()
    }

    #[test]
    fn timestamps_round_trip_exactly() {
        for (text, micros) in [
            ("1970-01-01T00:00:00Z", 0),
            ("2026-03-22T06:02:39.312Z", 1_774_159_359_312_000),
            ("2000-02-29T23:59:59.999999Z", 951_868_799_999_999),
            ("1969-12-31T23:59:59.5Z", -500_000),
        ] {
            assert_eq!(parse_event_time_micros(text).unwrap(), micros, "{text}");
            let canonical = format_event_time_micros(micros);
            assert_eq!(
                parse_event_time_micros(&canonical).unwrap(),
                micros,
                "{canonical}"
            );
        }
        for text in [
            "2026-02-29T00:00:00Z",
            "2026-03-22T24:00:00Z",
            "2026-03-22T06:02:39.1234567Z",
            "2026-03-22 06:02:39Z",
            "2026-03-22T06:02:39.312",
            "2026-13-01T00:00:00Z",
            "1970-01-01T00:00:00.Z",
        ] {
            assert!(parse_event_time_micros(text).is_err(), "{text}");
        }
    }

    #[test]
    fn prices_convert_without_rounding() {
        assert_eq!(parse_price_units("1.80787", scale(6)).unwrap(), 1_807_870);
        assert_eq!(parse_price_units("1.914290", scale(6)).unwrap(), 1_914_290);
        assert_eq!(parse_price_units("-0.5", scale(1)).unwrap(), -5);
        assert_eq!(parse_price_units("7", scale(0)).unwrap(), 7);
        assert_eq!(parse_price_units(".25", scale(2)).unwrap(), 25);
        for text in [
            "1.2345678",
            "nan",
            "inf",
            "1e5",
            "",
            "-",
            ".",
            "1.",
            "+1",
            "9223372036854775808",
        ] {
            assert!(parse_price_units(text, scale(6)).is_err(), "{text}");
        }
        assert!(parse_price_units("9223372036854.775808", scale(6)).is_err());
        assert_eq!(
            parse_price_units("-9223372036854775808", scale(0)).unwrap(),
            i64::MIN
        );
        assert_eq!(
            parse_price_units("9223372036854775807", scale(0)).unwrap(),
            i64::MAX
        );
        assert!(parse_price_units("9223372036854775808", scale(0)).is_err());
        assert!(parse_price_units("-9223372036854775809", scale(0)).is_err());
    }

    #[test]
    fn float_prices_convert_through_their_shortest_decimal_form() {
        assert_eq!(float_price_units(0.65165, scale(5)).unwrap(), 65_165);
        assert_eq!(float_price_units(158.424, scale(5)).unwrap(), 15_842_400);
        assert_eq!(float_price_units(0.1, scale(1)).unwrap(), 1);
        assert_eq!(float_price_units(-0.0, scale(5)).unwrap(), 0);
        assert_eq!(float_price_units(1e-7, scale(7)).unwrap(), 1);
        assert_eq!(
            float_price_units(1e15, scale(3)).unwrap(),
            1_000_000_000_000_000_000
        );
        for value in [
            0.651_651,
            0.1 + 0.2,
            f64::NAN,
            f64::INFINITY,
            f64::NEG_INFINITY,
            1e300,
        ] {
            assert!(float_price_units(value, scale(5)).is_err(), "{value}");
        }
    }

    #[test]
    fn tick_lines_bind_to_the_declared_symbol() {
        let symbol = ProviderSymbol::try_from("AEDCNY".to_string()).unwrap();
        let tick =
            parse_tick_line("2026-03-22T06:02:39.312Z,AEDCNY,1.80787", &symbol, scale(6)).unwrap();
        assert_eq!(
            tick,
            Tick {
                event_time_micros: 1_774_159_359_312_000,
                price_units: 1_807_870
            }
        );
        assert!(
            parse_tick_line("2026-03-22T06:02:39.312Z,EURUSD,1.80787", &symbol, scale(6)).is_err()
        );
        assert!(parse_tick_line("2026-03-22T06:02:39.312Z,AEDCNY", &symbol, scale(6)).is_err());
        assert!(
            parse_tick_line(
                "2026-03-22T06:02:39.312Z,AEDCNY,1.80787,x",
                &symbol,
                scale(6)
            )
            .is_err()
        );
    }

    #[test]
    fn tick_sequences_reject_backwards_time_and_conflicts() {
        let mut sequence = TickSequence::default();
        let first = Tick {
            event_time_micros: 10,
            price_units: 1,
        };
        sequence.accept(first).unwrap();
        sequence.accept(first).unwrap();
        assert!(
            sequence
                .accept(Tick {
                    event_time_micros: 10,
                    price_units: 2
                })
                .is_err()
        );
        assert!(
            sequence
                .accept(Tick {
                    event_time_micros: 9,
                    price_units: 1
                })
                .is_err()
        );
        sequence
            .accept(Tick {
                event_time_micros: 11,
                price_units: 2,
            })
            .unwrap();
    }

    #[test]
    fn bars_validate_values_grid_and_order() {
        let bar = Bar {
            start_unix_s: 1_747_653_300,
            open: 181.05,
            high: 181.1,
            low: 181.0,
            close: 181.05,
            volume: 0.0,
            period_s: 5,
        };
        bar.validate(5).unwrap();
        assert!(Bar { high: 181.0, ..bar }.validate(5).is_err());
        assert!(Bar { low: 181.06, ..bar }.validate(5).is_err());
        assert!(
            Bar {
                close: f64::NAN,
                ..bar
            }
            .validate(5)
            .is_err()
        );
        assert!(
            Bar {
                volume: -1.0,
                ..bar
            }
            .validate(5)
            .is_err()
        );
        assert!(
            Bar {
                start_unix_s: 1_747_653_301,
                ..bar
            }
            .validate(5)
            .is_err()
        );
        assert!(
            Bar {
                period_s: 60,
                ..bar
            }
            .validate(5)
            .is_err()
        );
        let mut sequence = BarSequence::default();
        sequence.accept(5).unwrap();
        assert!(sequence.accept(5).is_err());
        assert!(sequence.accept(0).is_err());
        sequence.accept(10).unwrap();
    }
}

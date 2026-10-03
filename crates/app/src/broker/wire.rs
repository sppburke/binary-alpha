use binary_alpha_engine::execution::Decimal;
use binary_alpha_engine::market::{PriceScale, parse_price_units};
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;
use std::borrow::Cow;

/// Preserves a provider numeric token until the existing exact decimal boundary reads it.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(transparent)]
pub struct WireDecimal(pub Box<RawValue>);
impl WireDecimal {
    pub fn token(&self) -> Result<Cow<'_, str>, String> {
        let token = self.0.get();
        if token.starts_with('"') {
            serde_json::from_str::<String>(token)
                .map(Cow::Owned)
                .map_err(|_| "decimal: malformed string field".into())
        } else {
            Ok(Cow::Borrowed(token))
        }
    }
    pub fn decimal(&self) -> Result<Decimal, String> {
        let token = self.token()?;
        let plain = if self.0.get().starts_with('"') {
            Cow::Borrowed(token.as_ref())
        } else {
            expand_number(&token).map_err(|_| "decimal: invalid amount field")?
        };
        Decimal::parse(&plain).map_err(|_| "decimal: invalid amount field".into())
    }
    pub fn price_units(&self, scale: PriceScale) -> Result<i64, String> {
        let token = self.token()?;
        let plain = if self.0.get().starts_with('"') {
            Cow::Borrowed(token.as_ref())
        } else {
            expand_number(&token)
                .map_err(|reason| format!("decimal: invalid price field: {reason}"))?
        };
        parse_price_units(&plain, scale)
            .map_err(|reason| format!("decimal: invalid price field: {reason}"))
    }
    pub fn from_decimal(value: Decimal) -> Self {
        Self(RawValue::from_string(value.to_string()).expect("a Decimal is a JSON number"))
    }
    pub(crate) fn require_number(&self) -> Result<Decimal, String> {
        if self.0.get().starts_with('"') {
            return Err("expected a numeric token, found a string".into());
        }
        self.decimal()
    }
}

/// The exact plain spelling of an unquoted JSON exponent token, bounded before allocation: at
/// most 18 fraction digits and 39 significant whole digits. Other tokens are returned as they are.
fn expand_number(token: &str) -> Result<Cow<'_, str>, &'static str> {
    let Some((mantissa, exponent)) = token.split_once(['e', 'E']) else {
        return Ok(Cow::Borrowed(token));
    };
    if !matches!(token.as_bytes().first(), Some(b'-' | b'0'..=b'9')) {
        return Err("invalid numeric token");
    }
    let negative = mantissa.starts_with('-');
    let unsigned = mantissa.strip_prefix('-').unwrap_or(mantissa);
    let (whole, fraction) = unsigned.split_once('.').unwrap_or((unsigned, ""));
    let whole = whole.trim_start_matches('0');
    let leading_zeros = whole
        .bytes()
        .chain(fraction.bytes())
        .take_while(|digit| *digit == b'0')
        .count();
    let zero = leading_zeros == whole.len() + fraction.len();
    let backwards = exponent.starts_with('-');
    let exponent = exponent.strip_prefix(['-', '+']).unwrap_or(exponent);
    let limit = if backwards {
        18
    } else {
        fraction.len().saturating_add(39)
    };
    let shift = exponent.bytes().try_fold(0usize, |value, digit| {
        value
            .checked_mul(10)?
            .checked_add(usize::from(digit - b'0'))
            .filter(|value| *value <= limit)
    });
    let shift = match shift {
        Some(value) => value,
        None if zero && !backwards => fraction.len(),
        None => return Err("exponent exceeds exact decimal bounds"),
    };
    let shift = if backwards {
        -(shift as i128)
    } else {
        shift as i128
    };
    let fraction_len = (fraction.len() as i128 - shift).max(0);
    if fraction_len > 18 {
        return Err("more than 18 fraction digits");
    }
    let point = whole.len() as i128 + shift;
    let whole_len = if zero {
        0
    } else {
        (point - leading_zeros as i128).max(0)
    };
    if whole_len > 39 {
        return Err("more than 39 significant whole digits");
    }
    let (whole_len, fraction_len) = (whole_len as usize, fraction_len as usize);
    let digits = std::iter::repeat_n('0', (-point).max(0) as usize)
        .chain(whole.chars())
        .chain(fraction.chars())
        .chain(std::iter::repeat('0'));
    let mut plain = String::with_capacity(
        usize::from(negative) + whole_len.max(1) + usize::from(fraction_len > 0) + fraction_len,
    );
    if negative {
        plain.push('-');
    }
    if whole_len == 0 {
        plain.push('0');
    } else {
        plain.extend(digits.clone().skip(leading_zeros).take(whole_len));
    }
    if fraction_len > 0 {
        plain.push('.');
        plain.extend(digits.skip(point.max(0) as usize).take(fraction_len));
    }
    Ok(Cow::Owned(plain))
}

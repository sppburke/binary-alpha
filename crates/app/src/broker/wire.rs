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
    let digits = whole.bytes().chain(fraction.bytes());
    let leading_zeros = digits.clone().take_while(|digit| *digit == b'0').count();
    let zero = leading_zeros == whole.len() + fraction.len();
    let backwards = exponent.starts_with('-');
    let exponent = exponent
        .strip_prefix(['-', '+'])
        .unwrap_or(exponent)
        .trim_start_matches('0');
    let limit = if backwards {
        18
    } else if zero {
        fraction.len()
    } else {
        fraction.len().saturating_add(39)
    };
    // RawValue guarantees number grammar; bound the digit string before converting it.
    let shift = if exponent.is_empty() {
        Some(0)
    } else if exponent.len() > limit.checked_ilog10().unwrap_or(0) as usize + 1 {
        None
    } else {
        exponent
            .parse::<usize>()
            .ok()
            .filter(|value| *value <= limit)
    };
    let shift = match shift {
        Some(value) => value,
        None if zero && !backwards => fraction.len(),
        None => return Err("exponent exceeds exact decimal bounds"),
    };
    let fraction_len = if backwards {
        fraction.len().checked_add(shift)
    } else {
        Some(fraction.len().saturating_sub(shift))
    }
    .filter(|length| *length <= 18)
    .ok_or("more than 18 fraction digits")?;
    let point = if backwards {
        whole.len().saturating_sub(shift)
    } else {
        whole
            .len()
            .checked_add(shift)
            .ok_or("whole part overflow")?
    };
    let whole_len = if zero {
        0
    } else {
        point.saturating_sub(leading_zeros)
    };
    if whole_len > 39 {
        return Err("more than 39 significant whole digits");
    }
    let mut plain = String::with_capacity(
        usize::from(negative) + whole_len.max(1) + usize::from(fraction_len > 0) + fraction_len,
    );
    if negative {
        plain.push('-');
    }
    if whole_len == 0 {
        plain.push('0');
    } else {
        plain.extend(
            digits
                .clone()
                .skip(leading_zeros)
                .take(whole_len)
                .map(char::from),
        );
        plain.extend(std::iter::repeat_n(
            '0',
            point.saturating_sub(whole.len() + fraction.len()),
        ));
    }
    if fraction_len > 0 {
        plain.push('.');
        if backwards {
            plain.extend(std::iter::repeat_n('0', shift.saturating_sub(whole.len())));
        }
        plain.extend(digits.skip(point).map(char::from));
    }
    Ok(Cow::Owned(plain))
}

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
        Decimal::parse(&self.token()?).map_err(|_| "decimal: invalid amount field".into())
    }
    pub fn price_units(&self, scale: PriceScale) -> Result<i64, String> {
        parse_price_units(&self.token()?, scale)
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

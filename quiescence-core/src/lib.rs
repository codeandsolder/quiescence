#![forbid(unsafe_code)]

use core::fmt;

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct NodeId(u32);

impl NodeId {
    #[must_use]
    pub const fn from_index(index: u32) -> Self {
        Self(index)
    }
    #[must_use]
    pub const fn index(self) -> usize {
        self.0 as usize
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ReactionId(u32);

impl ReactionId {
    #[must_use]
    pub const fn from_index(index: u32) -> Self {
        Self(index)
    }
    #[must_use]
    pub const fn index(self) -> usize {
        self.0 as usize
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct SimTime(i64);

impl SimTime {
    pub const ZERO: Self = Self(0);
    #[must_use]
    pub const fn from_nanos(v: i64) -> Self {
        Self(v)
    }
    #[must_use]
    pub const fn from_millis(v: i64) -> Self {
        Self(v.saturating_mul(1_000_000))
    }
    #[must_use]
    pub const fn from_secs(v: i64) -> Self {
        Self(v.saturating_mul(1_000_000_000))
    }
    #[must_use]
    pub const fn as_nanos(self) -> i64 {
        self.0
    }
    #[must_use]
    pub const fn saturating_add(self, d: SimDuration) -> Self {
        Self(self.0.saturating_add(d.0))
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct SimDuration(i64);

impl SimDuration {
    pub const ZERO: Self = Self(0);
    #[must_use]
    pub const fn from_nanos(v: i64) -> Self {
        Self(if v < 0 { 0 } else { v })
    }
    #[must_use]
    pub const fn from_millis(v: i64) -> Self {
        Self::from_nanos(v.saturating_mul(1_000_000))
    }
    #[must_use]
    pub const fn from_secs(v: i64) -> Self {
        Self::from_nanos(v.saturating_mul(1_000_000_000))
    }
    #[must_use]
    pub const fn as_nanos(self) -> i64 {
        self.0
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Bucket(i64);
impl Bucket {
    #[must_use]
    pub const fn new(raw: i64) -> Self {
        Self(raw)
    }
    #[must_use]
    pub const fn raw(self) -> i64 {
        self.0
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Estimate {
    pub center: f64,
    pub radius: f64,
}
impl Estimate {
    #[must_use]
    pub const fn point(value: f64) -> Self {
        Self {
            center: value,
            radius: 0.0,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Quantization {
    decimal_places: u8,
    hysteresis_lsd: u32,
}

impl Quantization {
    pub const MAX_DECIMAL_PLACES: u8 = 15;

    pub fn new(decimal_places: u8, hysteresis_lsd: u32) -> Result<Self, QuantizeError> {
        if decimal_places > Self::MAX_DECIMAL_PLACES {
            return Err(QuantizeError::TooManyDecimalPlaces(decimal_places));
        }
        Ok(Self {
            decimal_places,
            hysteresis_lsd,
        })
    }

    #[must_use]
    pub const fn decimal_places(self) -> u8 {
        self.decimal_places
    }
    #[must_use]
    pub const fn hysteresis_lsd(self) -> u32 {
        self.hysteresis_lsd
    }
    #[must_use]
    pub fn scale(self) -> f64 {
        10_f64.powi(i32::from(self.decimal_places))
    }
    #[must_use]
    pub fn decode(self, bucket: Bucket) -> f64 {
        bucket.raw() as f64 / self.scale()
    }

    pub fn resolve(
        self,
        previous: Option<Bucket>,
        estimate: Estimate,
    ) -> Result<Bucket, QuantizeError> {
        if !estimate.center.is_finite() || !estimate.radius.is_finite() || estimate.radius < 0.0 {
            return Err(QuantizeError::NonFiniteEstimate);
        }
        let scale = self.scale();
        let lower_value = estimate.center - estimate.radius;
        let upper_value = estimate.center + estimate.radius;
        let lower = lower_value * scale;
        let upper = upper_value * scale;

        if let Some(previous) = previous {
            let raw = previous.raw() as f64;
            let h = f64::from(self.hysteresis_lsd);
            if lower > raw - 0.5 - h && upper < raw + 0.5 + h {
                return Ok(previous);
            }
        }

        let lo = round_i64(lower)?;
        let hi = round_i64(upper)?;
        if lo != hi {
            return Err(QuantizeError::Ambiguous {
                lower: lower_value,
                upper: upper_value,
            });
        }
        Ok(Bucket::new(lo))
    }
}

fn round_i64(value: f64) -> Result<i64, QuantizeError> {
    if !value.is_finite() {
        return Err(QuantizeError::NonFiniteEstimate);
    }
    let rounded = value.round();
    if rounded < i64::MIN as f64 || rounded > i64::MAX as f64 {
        return Err(QuantizeError::OutOfRange);
    }
    Ok(rounded as i64)
}

#[derive(Clone, Debug, PartialEq)]
pub enum QuantizeError {
    TooManyDecimalPlaces(u8),
    NonFiniteEstimate,
    OutOfRange,
    Ambiguous { lower: f64, upper: f64 },
}

impl fmt::Display for QuantizeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::TooManyDecimalPlaces(v) => {
                write!(f, "decimal precision {v} exceeds supported maximum")
            }
            Self::NonFiniteEstimate => f.write_str("estimate must be finite"),
            Self::OutOfRange => f.write_str("quantized value does not fit in i64"),
            Self::Ambiguous { lower, upper } => write!(
                f,
                "estimate interval [{lower}, {upper}] crosses an observable bucket boundary"
            ),
        }
    }
}
impl std::error::Error for QuantizeError {}

#[cfg(test)]
mod tests {
    use super::{Bucket, Estimate, Quantization};

    #[test]
    fn decimal_quantization() -> Result<(), Box<dyn std::error::Error>> {
        let q = Quantization::new(2, 0)?;
        assert_eq!(q.resolve(None, Estimate::point(12.344))?, Bucket::new(1234));
        assert_eq!(q.resolve(None, Estimate::point(12.345))?, Bucket::new(1235));
        Ok(())
    }

    #[test]
    fn hysteresis_holds_last_digit() -> Result<(), Box<dyn std::error::Error>> {
        let q = Quantization::new(1, 1)?;
        let old = Bucket::new(200);
        assert_eq!(q.resolve(Some(old), Estimate::point(20.14))?, old);
        assert_eq!(
            q.resolve(Some(old), Estimate::point(20.16))?,
            Bucket::new(202)
        );
        Ok(())
    }

    #[test]
    fn bounded_estimate_can_prove_unchanged() -> Result<(), Box<dyn std::error::Error>> {
        let q = Quantization::new(2, 0)?;
        let old = Bucket::new(1234);
        assert_eq!(
            q.resolve(
                Some(old),
                Estimate {
                    center: 12.341,
                    radius: 0.002
                }
            )?,
            old
        );
        Ok(())
    }
}

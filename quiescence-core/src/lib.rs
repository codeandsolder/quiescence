#![forbid(unsafe_code)]

use core::fmt;

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct NodeId(u32);

impl NodeId {
    #[must_use = "the constructed identifier must be used"]
    pub const fn from_index(index: u32) -> Self {
        Self(index)
    }
    #[must_use = "the converted index must be used"]
    pub const fn index(self) -> usize {
        self.0 as usize
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct HistoryId(u32);

impl HistoryId {
    #[must_use = "the constructed identifier must be used"]
    pub const fn from_index(index: u32) -> Self {
        Self(index)
    }
    #[must_use = "the converted index must be used"]
    pub const fn index(self) -> usize {
        self.0 as usize
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ReactionId(u32);

impl ReactionId {
    #[must_use = "the constructed identifier must be used"]
    pub const fn from_index(index: u32) -> Self {
        Self(index)
    }
    #[must_use = "the converted index must be used"]
    pub const fn index(self) -> usize {
        self.0 as usize
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct SimTime(i64);

impl SimTime {
    pub const ZERO: Self = Self(0);
    #[must_use = "the converted simulation time or duration must be used"]
    pub const fn from_nanos(v: i64) -> Self {
        Self(v)
    }
    #[must_use = "the converted simulation time or duration must be used"]
    pub const fn from_millis(v: i64) -> Self {
        Self(v.saturating_mul(1_000_000))
    }
    #[must_use = "the converted simulation time or duration must be used"]
    pub const fn from_secs(v: i64) -> Self {
        Self(v.saturating_mul(1_000_000_000))
    }
    #[must_use = "the converted nanosecond count must be used"]
    pub const fn as_nanos(self) -> i64 {
        self.0
    }
    #[must_use = "time arithmetic returns a new time without modifying the original"]
    pub const fn saturating_add(self, d: SimDuration) -> Self {
        Self(self.0.saturating_add(d.0))
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct SimDuration(i64);

impl SimDuration {
    pub const ZERO: Self = Self(0);
    #[must_use = "the converted simulation time or duration must be used"]
    pub const fn from_nanos(v: i64) -> Self {
        Self(if v < 0 { 0 } else { v })
    }
    #[must_use = "the converted simulation time or duration must be used"]
    pub const fn from_millis(v: i64) -> Self {
        Self::from_nanos(v.saturating_mul(1_000_000))
    }
    #[must_use = "the converted simulation time or duration must be used"]
    pub const fn from_secs(v: i64) -> Self {
        Self::from_nanos(v.saturating_mul(1_000_000_000))
    }
    #[must_use = "the converted nanosecond count must be used"]
    pub const fn as_nanos(self) -> i64 {
        self.0
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Bucket(i64);
impl Bucket {
    #[must_use = "the constructed value must be used"]
    pub const fn new(raw: i64) -> Self {
        Self(raw)
    }
    #[must_use = "the canonical integer bucket must be used"]
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
    #[must_use = "the constructed estimate must be used"]
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

    /// # Errors
    /// Returns [`QuantizeError::TooManyDecimalPlaces`] when the requested precision exceeds the supported bound.
    pub const fn new(decimal_places: u8, hysteresis_lsd: u32) -> Result<Self, QuantizeError> {
        if decimal_places > Self::MAX_DECIMAL_PLACES {
            return Err(QuantizeError::TooManyDecimalPlaces(decimal_places));
        }
        Ok(Self {
            decimal_places,
            hysteresis_lsd,
        })
    }

    #[must_use = "the configured precision must be used"]
    pub const fn decimal_places(self) -> u8 {
        self.decimal_places
    }
    #[must_use = "the configured hysteresis width must be used"]
    pub const fn hysteresis_lsd(self) -> u32 {
        self.hysteresis_lsd
    }
    #[must_use = "the calculated quantization scale must be used"]
    pub fn scale(self) -> f64 {
        10_f64.powi(i32::from(self.decimal_places))
    }
    #[must_use = "the decoded working value must be used"]
    #[expect(
        clippy::cast_precision_loss,
        reason = "decoding intentionally maps the exact integer bucket into the f64 estimate domain"
    )]
    pub fn decode(self, bucket: Bucket) -> f64 {
        bucket.raw() as f64 / self.scale()
    }

    /// # Errors
    /// Returns a [`QuantizeError`] for non-finite inputs, out-of-range rounded values, or intervals spanning multiple buckets.
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

        let lo = round_i64(lower)?;
        let hi = round_i64(upper)?;

        if let Some(previous) = previous
            && retains_bucket(previous, self.hysteresis_lsd, lower, upper)
        {
            return Ok(previous);
        }
        if lo != hi {
            return Err(QuantizeError::Ambiguous {
                lower: lower_value,
                upper: upper_value,
            });
        }
        Ok(Bucket::new(lo))
    }
}

// Both scaled endpoints have already passed round_i64. Multiplication by
// two is exact in that range, and the directed integer conversions preserve
// strict comparisons with the integer half-bucket boundaries. In particular,
// we never project a previous i64 bucket into a lossy floating-point value.
#[expect(
    clippy::cast_possible_truncation,
    reason = "the validated scaled endpoints are rounded directionally and fit in i128"
)]
fn retains_bucket(previous: Bucket, hysteresis: u32, lower: f64, upper: f64) -> bool {
    let twice_bucket = 2 * i128::from(previous.raw());
    let twice_width = 2 * i128::from(hysteresis) + 1;
    (2.0 * lower).ceil() as i128 > twice_bucket - twice_width
        && ((2.0 * upper).floor() as i128) < twice_bucket + twice_width
}

#[expect(
    clippy::cast_precision_loss,
    reason = "the i64 bounds are intentionally projected into f64 before validating the rounded value"
)]
#[expect(
    clippy::cast_possible_truncation,
    reason = "the value is finite, rounded to an integer, and range-checked before conversion"
)]
fn round_i64(value: f64) -> Result<i64, QuantizeError> {
    if !value.is_finite() {
        return Err(QuantizeError::NonFiniteEstimate);
    }
    let rounded = value.round();
    // i64::MAX rounds up to 2^63 in f64. The upper bound is exclusive,
    // otherwise the saturating float-to-integer cast silently accepts 2^63.
    if rounded < i64::MIN as f64 || rounded >= -(i64::MIN as f64) {
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
    fn quantization_rejects_unrepresentable_float_boundaries()
    -> Result<(), Box<dyn std::error::Error>> {
        let q = Quantization::new(0, 0)?;
        assert!(matches!(
            q.resolve(None, Estimate::point(9_223_372_036_854_775_808.0)),
            Err(super::QuantizeError::OutOfRange)
        ));
        assert_eq!(
            q.resolve(None, Estimate::point(-9_223_372_036_854_775_808.0))?,
            Bucket::new(i64::MIN)
        );
        Ok(())
    }

    #[test]
    fn hysteresis_preserves_exact_large_bucket_identity() -> Result<(), Box<dyn std::error::Error>>
    {
        let q = Quantization::new(0, 1)?;
        // Both previous buckets lie between consecutive representable f64
        // integers. The estimate is exactly one LSD from the previous
        // bucket, inside its open retention interval on either side.
        for (bucket, estimate) in [
            (9_007_199_254_740_993, 9_007_199_254_740_994.0),
            (-9_007_199_254_740_993, -9_007_199_254_740_994.0),
            (i64::MIN + 1, -9_223_372_036_854_775_808.0),
        ] {
            assert_eq!(
                q.resolve(Some(Bucket::new(bucket)), Estimate::point(estimate))?,
                Bucket::new(bucket)
            );
        }
        // A previous bucket cannot make a rounded out-of-range estimate valid.
        assert!(matches!(
            q.resolve(
                Some(Bucket::new(i64::MAX)),
                Estimate::point(9_223_372_036_854_775_808.0)
            ),
            Err(super::QuantizeError::OutOfRange)
        ));
        Ok(())
    }

    #[test]
    fn hysteresis_half_grid_matches_integer_oracle() -> Result<(), Box<dyn std::error::Error>> {
        for hysteresis in 0..=3_u32 {
            let q = Quantization::new(0, hysteresis)?;
            for bucket in -5_i64..=5 {
                for lower_half in -20_i32..=20 {
                    for upper_half in lower_half..=20 {
                        let center = f64::from(lower_half + upper_half) / 4.0;
                        let radius = f64::from(upper_half - lower_half) / 4.0;
                        let lower = i64::from(lower_half);
                        let upper = i64::from(upper_half);
                        let width = 2 * i64::from(hysteresis) + 1;
                        let expected = if lower > 2 * bucket - width && upper < 2 * bucket + width {
                            Some(Bucket::new(bucket))
                        } else {
                            // Independent exact half-grid rounding, ties away from zero.
                            let round_half = |n: i64| n / 2 + n % 2;
                            (round_half(lower) == round_half(upper))
                                .then(|| Bucket::new(round_half(lower)))
                        };
                        let actual =
                            q.resolve(Some(Bucket::new(bucket)), Estimate { center, radius });
                        match expected {
                            Some(value) => assert_eq!(actual?, value),
                            None => assert!(matches!(
                                actual,
                                Err(super::QuantizeError::Ambiguous { .. })
                            )),
                        }
                    }
                }
            }
        }
        Ok(())
    }

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

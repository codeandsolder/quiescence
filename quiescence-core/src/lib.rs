//! Canonical numeric buckets, graph-local identifiers, and simulation time.
//!
//! Decimal quantization uses the exact real values represented by binary64 inputs.
//! Nonzero uncertainty radii are never discarded through intermediate rounding.
//!
//! ```
//! use quiescence_core::{Bucket, Estimate, Quantization};
//!
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! let quantization = Quantization::new(2, 1)?;
//! let previous = Bucket::new(1234);
//! let estimate = Estimate { center: 12.341, radius: 0.002 };
//! assert_eq!(quantization.resolve(Some(previous), estimate)?, previous);
//! # Ok(())
//! # }
//! ```

#![forbid(unsafe_code)]
#![feature(portable_simd)]

use core::fmt;

mod point;
mod quantize;
pub use point::{PointRequest, PreparedPoint};
use quantize::HalfBounds;

macro_rules! graph_id {
    ($name:ident, $doc:literal) => {
        #[doc = $doc]
        #[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
        pub struct $name(u32);

        impl $name {
            /// Construct a graph-local identifier from its raw index.
            #[must_use = "the constructed identifier must be used"]
            pub const fn from_index(index: u32) -> Self {
                Self(index)
            }

            #[must_use = "the identifier value must be used"]
            pub const fn raw(self) -> u32 {
                self.0
            }

            #[must_use = "the converted index must be used"]
            pub const fn index(self) -> usize {
                self.0 as usize
            }
        }
    };
}

graph_id!(NodeId, "Graph-local state node identifier.");
graph_id!(HistoryId, "Graph-local event history identifier.");
graph_id!(ReactionId, "Graph-local reaction identifier.");

#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
/// Signed nanoseconds since the simulation epoch. Larger-unit constructors saturate.
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
    /// Add a nonnegative duration, returning None if the result is unrepresentable.
    #[must_use = "time arithmetic must check for overflow"]
    pub const fn checked_add(self, duration: SimDuration) -> Option<Self> {
        match self.0.checked_add(duration.0) {
            Some(value) => Some(Self(value)),
            None => None,
        }
    }

    #[must_use = "time arithmetic returns a new time without modifying the original"]
    pub const fn saturating_add(self, d: SimDuration) -> Self {
        Self(self.0.saturating_add(d.0))
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
/// Nonnegative nanoseconds. Constructors clamp negative values to zero and saturate overflow.
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
    /// Integer buckets without hysteresis.
    pub const INTEGER: Self = Self {
        decimal_places: 0,
        hysteresis_lsd: 0,
    };

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
    const INTEGER_SCALES: [u64; 16] = {
        let mut scales = [1; 16];
        let mut i = 1;
        while i < scales.len() {
            scales[i] = scales[i - 1] * 10;
            i += 1;
        }
        scales
    };

    #[expect(
        clippy::cast_precision_loss,
        reason = "all supported integer powers of ten are exactly representable in binary64"
    )]
    const FLOAT_SCALES: [f64; 16] = {
        let mut scales = [0.0; 16];
        let mut i = 0;
        while i < scales.len() {
            scales[i] = Self::INTEGER_SCALES[i] as f64;
            i += 1;
        }
        scales
    };

    #[must_use = "the calculated quantization scale must be used"]
    pub const fn scale(self) -> f64 {
        Self::FLOAT_SCALES[self.decimal_places as usize]
    }

    const fn scale_integer(self) -> u64 {
        Self::INTEGER_SCALES[self.decimal_places as usize]
    }

    #[must_use = "the decoded working value must be used"]
    #[expect(
        clippy::cast_precision_loss,
        reason = "decoding intentionally maps the exact integer bucket into the f64 estimate domain"
    )]
    #[inline]
    pub fn decode(self, bucket: Bucket) -> f64 {
        let value = bucket.raw() as f64;
        if self.decimal_places == 0 {
            value
        } else {
            value / self.scale()
        }
    }

    /// Quantize the exact real interval represented by the binary64 center and radius.
    /// Decimal scaling and half-bucket comparisons introduce no additional rounding.
    /// Halfway points round away from zero.
    ///
    /// # Errors
    /// Returns a [`QuantizeError`] for non-finite inputs, out-of-range rounded values, or intervals spanning multiple buckets.
    pub fn resolve(
        self,
        previous: Option<Bucket>,
        estimate: Estimate,
    ) -> Result<Bucket, QuantizeError> {
        if estimate.radius == 0.0 {
            return point::resolve_point(self, previous, estimate.center);
        }
        point::resolve_interval(self, previous, estimate)
            .unwrap_or_else(|| self.resolve_exact(previous, estimate))
    }

    #[cold]
    fn resolve_exact(
        self,
        previous: Option<Bucket>,
        estimate: Estimate,
    ) -> Result<Bucket, QuantizeError> {
        if !estimate.center.is_finite() || !estimate.radius.is_finite() || estimate.radius < 0.0 {
            return Err(QuantizeError::NonFiniteEstimate);
        }
        let (lower, upper) = HalfBounds::estimate(estimate, u128::from(self.scale_integer()))?;
        let lo = lower.round_bucket()?;
        let hi = upper.round_bucket()?;

        if let Some(previous) = previous {
            let twice_bucket = 2 * i128::from(previous.raw());
            let twice_width = 2 * i128::from(self.hysteresis_lsd) + 1;
            if lower.ceil > twice_bucket - twice_width && upper.floor < twice_bucket + twice_width {
                return Ok(previous);
            }
        }
        if lo != hi {
            return Err(QuantizeError::Ambiguous { estimate });
        }
        Ok(lo)
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum QuantizeError {
    TooManyDecimalPlaces(u8),
    BatchLengthMismatch,
    NonFiniteEstimate,
    OutOfRange,
    Ambiguous { estimate: Estimate },
}

impl fmt::Display for QuantizeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::BatchLengthMismatch => f.write_str("point request and output lengths differ"),
            Self::TooManyDecimalPlaces(v) => {
                write!(f, "decimal precision {v} exceeds supported maximum")
            }
            Self::NonFiniteEstimate => {
                f.write_str("estimate must be finite with a nonnegative radius")
            }
            Self::OutOfRange => f.write_str("quantized value does not fit in i64"),
            Self::Ambiguous { estimate } => write!(
                f,
                "estimate {} ± {} crosses an observable bucket boundary",
                estimate.center, estimate.radius
            ),
        }
    }
}
impl std::error::Error for QuantizeError {}

#[cfg(test)]
mod tests {
    use super::{Bucket, Estimate, Quantization};

    #[test]
    fn decode_preserves_original_binary64_rounding() -> Result<(), super::QuantizeError> {
        // The integer-to-binary64 conversion rounds these extreme and
        // halfway integers to the representable values below before scaling.
        let cases = [
            (i64::MIN, -9_223_372_036_854_775_808.0_f64),
            (i64::MIN + 1, -9_223_372_036_854_775_808.0),
            (-9_007_199_254_740_993, -9_007_199_254_740_992.0),
            (-1, -1.0),
            (0, 0.0),
            (1, 1.0),
            (9_007_199_254_740_993, 9_007_199_254_740_992.0),
            (i64::MAX - 1, 9_223_372_036_854_775_808.0),
            (i64::MAX, 9_223_372_036_854_775_808.0),
        ];
        for places in 0..=Quantization::MAX_DECIMAL_PLACES {
            let q = Quantization::new(places, 0)?;
            for (raw, rounded) in cases {
                assert_eq!(
                    q.decode(Bucket::new(raw)).to_bits(),
                    (rounded / q.scale()).to_bits()
                );
            }
        }
        Ok(())
    }

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

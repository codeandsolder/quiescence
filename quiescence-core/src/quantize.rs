//! Exact half-grid arithmetic for decimal quantization of binary64 estimates.
//!
//! Each finite f64 is an integer significand times a power of two. Multiplying
//! by twice the decimal scale needs at most 104 significand bits. Splitting the
//! result into integer and dyadic fractional parts avoids both double rounding
//! and arbitrary-precision arithmetic, even when exponents differ by 1,000 bits.

use super::{Bucket, Estimate, QuantizeError};
use core::cmp::Ordering;

#[derive(Clone, Copy)]
struct Fraction {
    numerator: u128,
    shift: u32,
}

impl Fraction {
    const ZERO: Self = Self {
        numerator: 0,
        shift: 0,
    };
    const HALF: Self = Self {
        numerator: 1,
        shift: 1,
    };

    fn compare(self, other: Self) -> Ordering {
        if self.numerator == 0 || other.numerator == 0 {
            return self.numerator.cmp(&other.numerator);
        }
        // Equalize leading exponents and significands separately. No shift
        // by the potentially enormous difference between binary exponents.
        let left_zeros = self.numerator.leading_zeros();
        let right_zeros = other.numerator.leading_zeros();
        (right_zeros + other.shift)
            .cmp(&(left_zeros + self.shift))
            .then_with(|| (self.numerator << left_zeros).cmp(&(other.numerator << right_zeros)))
    }

    // Only used on a fraction >= 1/2, so its denominator is at most 2^128.
    const fn complement(self) -> Self {
        let numerator = if self.shift == u128::BITS {
            self.numerator.wrapping_neg()
        } else {
            (1_u128 << self.shift) - self.numerator
        };
        Self {
            numerator,
            shift: self.shift,
        }
    }

    fn sum_cmp_one(self, other: Self) -> Ordering {
        if self.compare(Self::HALF) != Ordering::Less {
            other.compare(self.complement())
        } else if other.compare(Self::HALF) != Ordering::Less {
            self.compare(other.complement())
        } else {
            Ordering::Less
        }
    }
}

#[derive(Clone, Copy)]
struct Magnitude {
    whole: i128,
    fraction: Fraction,
}

impl Magnitude {
    // Twice the magnitude of the largest admissible negative bucket.
    const LIMIT: u128 = 1_u128 << 64;

    fn scaled(value: f64, scale: u128) -> Result<Self, QuantizeError> {
        const FRACTION_BITS: u32 = 52;
        const FRACTION_MASK: u64 = (1_u64 << FRACTION_BITS) - 1;
        let bits = value.to_bits();
        let exponent = i32::from(
            u16::try_from((bits >> FRACTION_BITS) & 0x7ff)
                .map_err(|_| QuantizeError::NonFiniteEstimate)?,
        );
        let (significand, power) = if exponent == 0 {
            (bits & FRACTION_MASK, -1074 + 1)
        } else {
            (
                (bits & FRACTION_MASK) | (1_u64 << FRACTION_BITS),
                exponent - 1023 - 52 + 1,
            )
        };
        let numerator = u128::from(significand) * scale;
        let shift = power.unsigned_abs();
        let (whole, fraction) = if power >= 0 {
            if numerator > Self::LIMIT.checked_shr(shift).unwrap_or(0) {
                return Err(QuantizeError::OutOfRange);
            }
            (numerator << shift, Fraction::ZERO)
        } else {
            let whole = numerator.checked_shr(shift).unwrap_or(0);
            let remainder = numerator - whole.checked_shl(shift).unwrap_or(0);
            (
                whole,
                Fraction {
                    numerator: remainder,
                    shift,
                },
            )
        };
        if whole > Self::LIMIT {
            return Err(QuantizeError::OutOfRange);
        }
        Ok(Self {
            whole: i128::try_from(whole).map_err(|_| QuantizeError::OutOfRange)?,
            fraction,
        })
    }

    const fn bounds(self) -> HalfBounds {
        HalfBounds::new(self.whole, self.fraction.numerator == 0)
    }

    fn add(self, other: Self) -> HalfBounds {
        let ordering = self.fraction.sum_cmp_one(other.fraction);
        let carry = i128::from(ordering != Ordering::Less);
        let integral = ordering == Ordering::Equal
            || (self.fraction.numerator == 0 && other.fraction.numerator == 0);
        HalfBounds::new(self.whole + other.whole + carry, integral)
    }

    fn subtract(self, other: Self) -> HalfBounds {
        let ordering = self.fraction.compare(other.fraction);
        let borrow = i128::from(ordering == Ordering::Less);
        HalfBounds::new(
            self.whole - other.whole - borrow,
            ordering == Ordering::Equal,
        )
    }
}

/// Floor and ceiling of an exact endpoint expressed in half-bucket units.
#[derive(Clone, Copy)]
pub struct HalfBounds {
    pub(super) floor: i128,
    pub(super) ceil: i128,
}

impl HalfBounds {
    const fn new(floor: i128, integral: bool) -> Self {
        Self {
            floor,
            ceil: if integral { floor } else { floor + 1 },
        }
    }

    const fn negated(self) -> Self {
        Self {
            floor: -self.ceil,
            ceil: -self.floor,
        }
    }

    pub(super) fn estimate(estimate: Estimate, scale: u128) -> Result<(Self, Self), QuantizeError> {
        let center = Magnitude::scaled(estimate.center, scale)?;
        if estimate.radius == 0.0 {
            let bounds = if estimate.center.is_sign_negative() {
                center.bounds().negated()
            } else {
                center.bounds()
            };
            return Ok((bounds, bounds));
        }
        let radius = Magnitude::scaled(estimate.radius, scale)?;
        let lower = center.subtract(radius);
        let upper = center.add(radius);
        if estimate.center.is_sign_negative() {
            Ok((upper.negated(), lower.negated()))
        } else {
            Ok((lower, upper))
        }
    }

    pub(super) fn round_bucket(self) -> Result<Bucket, QuantizeError> {
        // The sign determines which half-grid boundary rounds away from zero.
        let rounded = if self.floor >= 0 {
            (self.floor + 1) / 2
        } else {
            (self.ceil - 1) / 2
        };
        i64::try_from(rounded)
            .map(Bucket::new)
            .map_err(|_| QuantizeError::OutOfRange)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Quantization;

    #[test]
    fn sub_ulp_radius_cannot_disappear_at_half_bucket() -> Result<(), Box<dyn std::error::Error>> {
        let quantization = Quantization::new(0, 0)?;
        for center in [-0.5, 0.5] {
            for radius in [f64::from_bits(1), f64::EPSILON / 16.0] {
                assert!(matches!(
                    quantization.resolve(None, Estimate { center, radius }),
                    Err(QuantizeError::Ambiguous { .. })
                ));
            }
        }
        Ok(())
    }

    #[test]
    fn decimal_scaling_does_not_round_before_bucket_rounding()
    -> Result<(), Box<dyn std::error::Error>> {
        let quantization = Quantization::new(1, 0)?;
        // The nearest binary64 to 0.15 is strictly below the real decimal
        // midpoint, even though multiplying it by ten rounds to exactly 1.5.
        assert_eq!(
            quantization.resolve(None, Estimate::point(0.15))?,
            Bucket::new(1)
        );
        assert_eq!(
            quantization.resolve(None, Estimate::point(0.15_f64.next_up()))?,
            Bucket::new(2)
        );
        assert_eq!(
            quantization.resolve(None, Estimate::point(-0.15))?,
            Bucket::new(-1)
        );
        Ok(())
    }

    #[test]
    fn interval_arithmetic_matches_exact_rational_grid() -> Result<(), Box<dyn std::error::Error>> {
        for places in [0, 1, 2, 7, 15] {
            let quantization = Quantization::new(places, 1)?;
            let scale = 10_i128.pow(u32::from(places));
            for shift in [1, 4, 15, 30] {
                let denominator = 1_i128 << shift;
                let denominator_f64 = f64::from(1_u32 << shift);
                for center in -31_i32..=31 {
                    for radius in 0_i32..=13 {
                        let estimate = Estimate {
                            center: f64::from(center) / denominator_f64,
                            radius: f64::from(radius) / denominator_f64,
                        };
                        let lower = i128::from(center - radius) * scale;
                        let upper = i128::from(center + radius) * scale;
                        let round =
                            |n: i128| (n.abs() + denominator / 2) / denominator * n.signum();
                        let lo = round(lower);
                        let hi = round(upper);
                        let previous = i64::try_from(round(i128::from(center) * scale))?;
                        let lower_retention = (2 * i128::from(previous) - 3) * denominator;
                        let upper_retention = (2 * i128::from(previous) + 3) * denominator;
                        let expected = if 2 * lower > lower_retention && 2 * upper < upper_retention
                        {
                            Some(previous)
                        } else if lo == hi {
                            Some(i64::try_from(lo)?)
                        } else {
                            None
                        };
                        let actual = quantization.resolve(Some(Bucket::new(previous)), estimate);
                        match expected {
                            Some(value) => assert_eq!(
                                actual?,
                                Bucket::new(value),
                                "{places} {shift} {estimate:?}"
                            ),
                            None => assert!(
                                matches!(actual, Err(QuantizeError::Ambiguous { .. })),
                                "{places} {shift} {estimate:?}"
                            ),
                        }
                    }
                }
            }
        }
        Ok(())
    }
}

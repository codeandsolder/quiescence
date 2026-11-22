//! Certified fast paths for exact point quantization.
//!
//! For |scaled| < 2^51, every half-integer boundary is representable in
//! binary64. Correct rounding of the floating-point multiply cannot cross
//! such a boundary without landing exactly on it. If the product is not a
//! half-integer, nearest-integer rounding and open hysteresis retention are
//! therefore exact. At a boundary (or outside this range) use integer dyadic
//! arithmetic instead.

use super::{Bucket, Estimate, Quantization, QuantizeError};
use core::simd::{
    Select, Simd,
    cmp::SimdPartialOrd,
    num::{SimdFloat, SimdUint},
};

const MAGIC: f64 = 4_503_599_627_370_496.0; // 2^52: spacing = 1
const LIMIT: f64 = MAGIC / 2.0;
const MANTISSA: u64 = (1 << 52) - 1;
const LANES: usize = 8;

/// One independent point estimate, with its own decimal and hysteresis policy.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct PointRequest {
    pub quantization: Quantization,
    pub previous: Option<Bucket>,
    pub value: f64,
}

impl PointRequest {
    /// Resolve using exact point semantics, including at decimal midpoints.
    ///
    /// # Errors
    /// Rejects nonfinite or unrepresentable values.
    #[inline]
    pub fn resolve(self) -> Result<Bucket, QuantizeError> {
        let scaled = self.value * self.quantization.scale();
        let magnitude = scaled.abs();
        let encoded = magnitude + MAGIC;
        let rounded = encoded - MAGIC;
        if magnitude < LIMIT && (magnitude - rounded).abs() < 0.5 {
            let unsigned = encoded.to_bits() & MANTISSA;
            let bucket = signed_magnitude(unsigned, scaled.is_sign_negative());
            Ok(self.retain(bucket))
        } else {
            self.resolve_exact()
        }
    }

    /// Resolve independent requests into a caller-owned output buffer.
    /// Requests may have different quantization policies; no SIMD types escape.
    /// On error, earlier blocks may have been written; the whole batch is not atomic.
    ///
    /// # Errors
    /// Returns the first numeric error, or a length mismatch before writing.
    pub fn resolve_batch(requests: &[Self], output: &mut [Bucket]) -> Result<(), QuantizeError> {
        if requests.len() != output.len() {
            return Err(QuantizeError::BatchLengthMismatch);
        }
        let (blocks, remainder) = requests.as_chunks::<LANES>();
        let (outputs, tail) = output.as_chunks_mut::<LANES>();
        for (block, out) in blocks.iter().zip(outputs) {
            resolve_block(block, out)?;
        }
        for (&request, out) in remainder.iter().zip(tail) {
            *out = request.resolve()?;
        }
        Ok(())
    }

    #[inline]
    fn retain(self, rounded: i64) -> Bucket {
        if let Some(previous) = self.previous
            && rounded.abs_diff(previous.raw()) <= u64::from(self.quantization.hysteresis_lsd())
        {
            return previous;
        }
        Bucket::new(rounded)
    }

    #[cold]
    fn resolve_exact(self) -> Result<Bucket, QuantizeError> {
        self.quantization
            .resolve_exact(self.previous, Estimate::point(self.value))
    }
}

#[inline]
const fn signed_magnitude(magnitude: u64, negative: bool) -> i64 {
    // The mantissa is at most 2^52 - 1, always a representable i64.
    let value = i64::from_ne_bytes(magnitude.to_ne_bytes());
    if negative { -value } else { value }
}

#[inline]
fn resolve_block(
    requests: &[PointRequest; LANES],
    output: &mut [Bucket; LANES],
) -> Result<(), QuantizeError> {
    let values = Simd::from_array(requests.map(|request| request.value));
    let scales = Simd::from_array(requests.map(|request| request.quantization.scale()));
    let previous =
        Simd::from_array(requests.map(|request| request.previous.map_or(i64::MIN, Bucket::raw)));
    let widths =
        Simd::from_array(requests.map(|request| i64::from(request.quantization.hysteresis_lsd())));
    let (rounded, certified) = round_block(values * scales);
    // Compare rather than subtract: an arbitrary previous i64 cannot overflow.
    let retain = previous.simd_ge(rounded - widths) & previous.simd_le(rounded + widths);
    let mut buckets = retain.select(previous, rounded).to_array().map(Bucket::new);
    let failures = !certified;
    if failures.any() {
        let mut mask = failures.to_bitmask();
        while mask != 0 {
            let i = mask.trailing_zeros() as usize;
            buckets[i] = requests[i].resolve_exact()?;
            mask &= mask - 1;
        }
    }
    *output = buckets;
    Ok(())
}

#[inline]
fn round_block(scaled: Simd<f64, LANES>) -> (Simd<i64, LANES>, core::simd::Mask<i64, LANES>) {
    let magnitude = scaled.abs();
    let encoded = magnitude + Simd::splat(MAGIC);
    let rounded = encoded - Simd::splat(MAGIC);
    let certified = magnitude.simd_lt(Simd::splat(LIMIT))
        & (magnitude - rounded).abs().simd_lt(Simd::splat(0.5));
    let unsigned = encoded.to_bits() & Simd::splat(MANTISSA);
    let signed = unsigned.cast::<i64>();
    (scaled.is_sign_negative().select(-signed, signed), certified)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn next(random: &mut u64) -> u64 {
        *random ^= *random << 13;
        *random ^= *random >> 7;
        *random ^= *random << 17;
        *random
    }

    #[test]
    fn scalar_and_portable_simd_match_exact_dyadic_oracle() -> Result<(), Box<dyn std::error::Error>>
    {
        let mut random = 0x9e37_79b9_7f4a_7c15;
        for decimal_places in 0..=Quantization::MAX_DECIMAL_PLACES {
            let q = Quantization::new(decimal_places, 1)?;
            for round in 0..768_u32 {
                let mut requests = [PointRequest {
                    quantization: q,
                    previous: None,
                    value: 0.0,
                }; LANES + 3];
                for (i, request) in requests.iter_mut().enumerate() {
                    let r = next(&mut random);
                    let quantization =
                        Quantization::new(u8::try_from(r & 15)?, u32::try_from(r >> 32)?)?;
                    // Include both arbitrary binary64 data and dense
                    // neighborhoods of integer and half-integer boundaries.
                    let integer = f64::from(i32::try_from(r & 0x7fff_ffff)?)
                        * if r & 16 == 0 { 1.0 } else { -1.0 };
                    let boundary = (integer + 0.5) / q.scale();
                    let value = match (i + usize::try_from(round)?) % 9 {
                        0 => f64::from_bits(r),
                        1 => boundary,
                        2 => boundary.next_up(),
                        3 => boundary.next_down(),
                        4 => integer / q.scale(),
                        5 => f64::from_bits(1),
                        6 => LIMIT / q.scale(),
                        7 => -LIMIT / q.scale(),
                        _ => -0.0,
                    };
                    *request = PointRequest {
                        quantization: if i.is_multiple_of(3) { quantization } else { q },
                        previous: match i % 4 {
                            0 => None,
                            1 => Some(Bucket::new(i64::MIN)),
                            2 => Some(Bucket::new(i64::MAX)),
                            _ => Some(Bucket::new(i64::from(i32::try_from(r & 0x7fff_ffff)?))),
                        },
                        value,
                    };
                }
                let mut expected = [Bucket::new(0); LANES + 3];
                let mut error = None;
                for (request, out) in requests.iter().zip(&mut expected) {
                    let exact = request.resolve_exact();
                    assert_eq!(request.resolve(), exact, "{request:?}");
                    match exact {
                        Ok(bucket) => *out = bucket,
                        Err(e) => {
                            error.get_or_insert(e);
                            // Replace invalid lanes below to exercise all
                            // successful outputs including the tail.
                        }
                    }
                }
                let mut actual = [Bucket::new(0); LANES + 3];
                let result = PointRequest::resolve_batch(&requests, &mut actual);
                if let Some(error) = error {
                    assert_eq!(result, Err(error));
                    for (request, out) in requests.iter_mut().zip(&mut expected) {
                        if request.resolve_exact().is_err() {
                            request.value = 0.0;
                            *out = request.resolve_exact()?;
                        }
                    }
                    PointRequest::resolve_batch(&requests, &mut actual)?;
                } else {
                    result?;
                }
                assert_eq!(actual, expected, "{requests:?}");
            }
        }
        Ok(())
    }

    #[test]
    fn batch_length_mismatch_preserves_output() -> Result<(), Box<dyn std::error::Error>> {
        let request = PointRequest {
            quantization: Quantization::new(2, 0)?,
            previous: None,
            value: 1.0,
        };
        let mut buckets = [Bucket::new(99); 3];
        assert_eq!(
            PointRequest::resolve_batch(&[request; 2], &mut buckets),
            Err(QuantizeError::BatchLengthMismatch)
        );
        assert_eq!(buckets, [Bucket::new(99); 3]);
        PointRequest::resolve_batch(&[], &mut [])?;
        for len in 1..=3 {
            PointRequest::resolve_batch(&[request; 3][..len], &mut buckets[..len])?;
            assert_eq!(buckets[..len], [Bucket::new(100); 3][..len]);
        }
        Ok(())
    }
}

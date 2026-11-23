//! Certified floating-point fast paths for exact quantization.
//!
//! For |scaled| < 2^51, every half-integer boundary is representable in
//! binary64. Correct rounding of the floating-point multiply cannot cross
//! such a boundary without landing exactly on it. If the product is not a
//! half-integer, nearest-integer rounding and open hysteresis retention are
//! therefore exact. At a boundary (or outside this range) use integer dyadic
//! arithmetic instead. Nonzero-radius intervals use an explicit absolute
//! endpoint error bound and two-lane certification.

use super::{Bucket, Estimate, Quantization, QuantizeError, quantize::exact_point};
use core::simd::{
    Select, Simd,
    cmp::SimdPartialOrd,
    num::{SimdFloat, SimdInt, SimdUint},
};

const MAGIC: f64 = 4_503_599_627_370_496.0; // 2^52: spacing = 1
const LIMIT: f64 = MAGIC / 2.0;
const MANTISSA: u64 = (1 << 52) - 1;
// Four f64 lanes fit one AVX2 vector and outperform the former eight-lane
// gather/round path on the reference Acer Ryzen; the API stays portable.
const LANES: usize = 4;

/// Exact, state-independent rounding of a point. The midpoint class
/// preserves the open hysteresis boundary for any later previous bucket.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PreparedPoint {
    /// The exact point lies strictly inside its nearest bucket's half-grid.
    Interior(Bucket),
    /// The exact point is at a half-grid boundary, rounded away from zero.
    Midpoint(Bucket),
}

impl PreparedPoint {
    #[must_use = "the canonical bucket must be used"]
    #[inline]
    pub fn resolve(self, previous: Bucket, hysteresis_lsd: u32) -> Bucket {
        let (rounded, midpoint) = match self {
            Self::Interior(bucket) => (bucket, false),
            Self::Midpoint(bucket) => (bucket, true),
        };
        let width = u64::from(hysteresis_lsd);
        let distance = rounded.raw().abs_diff(previous.raw());
        // Positive ties lie half a unit below their rounded bucket; negative
        // ties lie half a unit above. The outer boundary on that far side
        // must be excluded (the hysteresis interval is open).
        let excluded_edge = midpoint
            && if rounded.raw() > 0 {
                previous.raw() >= rounded.raw()
            } else {
                previous.raw() <= rounded.raw()
            };
        if distance < width || (distance == width && !excluded_edge) {
            previous
        } else {
            rounded
        }
    }
}

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
        resolve_point(self.quantization, self.previous, self.value)
    }

    /// Compile point rounding independently of the current previous bucket.
    /// The result can be reused when delivering an already-admitted write.
    ///
    /// # Errors
    /// Rejects nonfinite or unrepresentable values.
    #[inline]
    pub fn prepare(self) -> Result<PreparedPoint, QuantizeError> {
        certified_bucket(self.quantization, self.value).map_or_else(
            || prepare_point_exact(self.quantization, self.value),
            |raw| Ok(PreparedPoint::Interior(Bucket::new(raw))),
        )
    }

    /// Resolve independent requests into a caller-owned output buffer.
    /// Requests may have different quantization policies; no SIMD types escape.
    /// On error, earlier blocks may have been written; the whole batch is not atomic.
    ///
    /// # Errors
    /// Returns the first numeric error, or a length mismatch before writing.
    #[inline]
    pub fn resolve_batch(requests: &[Self], output: &mut [Bucket]) -> Result<(), QuantizeError> {
        transform_batch(requests, output, resolve_block, Self::resolve)
    }

    /// Compile independent points without applying their previous buckets.
    /// On error, earlier requests may have been written.
    ///
    /// # Errors
    /// Returns the first numeric error, or a length mismatch before writing.
    #[inline]
    pub fn prepare_batch(
        requests: &[Self],
        output: &mut [PreparedPoint],
    ) -> Result<(), QuantizeError> {
        if requests.len() != output.len() {
            return Err(QuantizeError::BatchLengthMismatch);
        }
        // Independent preparation is faster without vector gather/scatter;
        // exact certification and midpoint fallback remain per request.
        for (&request, point) in requests.iter().zip(output) {
            *point = request.prepare()?;
        }
        Ok(())
    }
}

/// Scalar point entry shared by the quantization API and batch requests.
#[inline]
pub fn resolve_point(
    quantization: Quantization,
    previous: Option<Bucket>,
    value: f64,
) -> Result<Bucket, QuantizeError> {
    let Some(rounded) = certified_bucket(quantization, value) else {
        return resolve_point_exact(quantization, previous, value);
    };
    Ok(previous
        .filter(|bucket| rounded.abs_diff(bucket.raw()) <= u64::from(quantization.hysteresis_lsd()))
        .unwrap_or_else(|| Bucket::new(rounded)))
}

#[inline]
fn certified_bucket(quantization: Quantization, value: f64) -> Option<i64> {
    let scaled = value * quantization.scale();
    let magnitude = scaled.abs();
    let encoded = magnitude + MAGIC;
    let rounded = encoded - MAGIC;
    if magnitude < LIMIT && (magnitude - rounded).abs() < 0.5 {
        let unsigned = encoded.to_bits() & MANTISSA;
        Some(signed_magnitude(unsigned, scaled.is_sign_negative()))
    } else {
        None
    }
}

#[cold]
fn prepare_point_exact(
    quantization: Quantization,
    value: f64,
) -> Result<PreparedPoint, QuantizeError> {
    let (bucket, midpoint) = exact_point(value, u128::from(quantization.scale_integer()))?;
    Ok(if midpoint {
        PreparedPoint::Midpoint(bucket)
    } else {
        PreparedPoint::Interior(bucket)
    })
}

#[cold]
fn resolve_point_exact(
    quantization: Quantization,
    previous: Option<Bucket>,
    value: f64,
) -> Result<Bucket, QuantizeError> {
    let prepared = prepare_point_exact(quantization, value)?;
    Ok(previous.map_or(
        match prepared {
            PreparedPoint::Interior(bucket) | PreparedPoint::Midpoint(bucket) => bucket,
        },
        |previous| prepared.resolve(previous, quantization.hysteresis_lsd()),
    ))
}

#[inline]
fn transform_batch<T>(
    requests: &[PointRequest],
    output: &mut [T],
    block: impl Fn(&[PointRequest; LANES], &mut [T; LANES]) -> Result<(), QuantizeError>,
    scalar: impl Fn(PointRequest) -> Result<T, QuantizeError>,
) -> Result<(), QuantizeError> {
    if requests.len() != output.len() {
        return Err(QuantizeError::BatchLengthMismatch);
    }
    let (blocks, remainder) = requests.as_chunks::<LANES>();
    let (outputs, tail) = output.as_chunks_mut::<LANES>();
    for (requests, out) in blocks.iter().zip(outputs) {
        block(requests, out)?;
    }
    for (&request, out) in remainder.iter().zip(tail) {
        *out = scalar(request)?;
    }
    Ok(())
}

/// Certify the exact scaled center ± radius without constructing dyadic
/// fractions in the common case. Uncertain boundary decisions fall back.
#[inline]
pub fn resolve_interval(
    quantization: Quantization,
    previous: Option<Bucket>,
    estimate: Estimate,
) -> Option<Result<Bucket, QuantizeError>> {
    let scale = quantization.scale();
    let center = estimate.center * scale;
    let radius = estimate.radius * scale;
    let magnitude = center.abs() + radius;
    // This also excludes NaN/infinite input and overflow, and ensures both
    // exact rounded endpoints are well inside the representable i64 range.
    let certified_domain = estimate.radius > 0.0 && magnitude < LIMIT;
    if !certified_domain {
        return None;
    }

    // For a finite rounded result x, the absolute rounding error is at most
    // epsilon*|x| + min_subnormal. The two scaled products together have
    // error <= epsilon*(|center| + radius) + 2*min_subnormal. Their exact
    // sum of magnitudes differs from the rounded magnitude by at most
    // epsilon*magnitude + min_subnormal. The final addition/subtraction
    // has result magnitude <= magnitude (monotonic rounding). Thus total
    // endpoint error <= (2+epsilon)*epsilon*magnitude + (3+epsilon)*min_subnormal.
    // The factor 4 is a conservative power of two. Taking the maximum
    // with MIN_POSITIVE covers underflow and subnormals: above that floor
    // the margin from 2+epsilon to 4 dominates (3+epsilon)*min_subnormal;
    // below it, the total error is less than MIN_POSITIVE.
    let error = (magnitude * (4.0 * f64::EPSILON)).max(f64::MIN_POSITIVE);
    let endpoints = Simd::splat(center) + Simd::from_array([-radius, radius]);
    let (rounded, certified) = round_block(endpoints, Some(error));
    if !certified.all() {
        return None;
    }
    let [lo, hi] = rounded.to_array();
    // Certified endpoints never touch half-integer boundaries. Hence the
    // exact open hysteresis interval contains both iff their rounded
    // buckets are within its integer width of the previous bucket.
    if let Some(old) = previous {
        let width = u64::from(quantization.hysteresis_lsd());
        if lo.abs_diff(old.raw()) <= width && hi.abs_diff(old.raw()) <= width {
            return Some(Ok(old));
        }
    }
    Some(if lo == hi {
        Ok(Bucket::new(lo))
    } else {
        Err(QuantizeError::Ambiguous { estimate })
    })
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
    let previous =
        Simd::from_array(requests.map(|request| request.previous.map_or(i64::MIN, Bucket::raw)));
    let widths =
        Simd::from_array(requests.map(|request| i64::from(request.quantization.hysteresis_lsd())));
    let (rounded, certified) = round_requests(requests);
    // Translate the signed retention interval to an unsigned interval.
    // SIMD integer operations wrap. For certified lanes the rounded bucket
    // is bounded by 2^51, so an arbitrary i64 previous bucket cannot wrap
    // across the entire u64 domain into the (at most 2^33) retention range.
    let distance = (rounded - previous + widths).cast::<u64>();
    let retain = distance.simd_le((widths + widths).cast::<u64>());
    let mut buckets = retain.select(previous, rounded).to_array().map(Bucket::new);
    patch_uncertified(requests, certified, &mut buckets, |request| {
        resolve_point_exact(request.quantization, request.previous, request.value)
    })?;
    *output = buckets;
    Ok(())
}

#[inline]
fn round_requests(
    requests: &[PointRequest; LANES],
) -> (Simd<i64, LANES>, core::simd::Mask<i64, LANES>) {
    let values = Simd::from_array(requests.map(|request| request.value));
    let scales = Simd::from_array(requests.map(|request| request.quantization.scale()));
    round_block(values * scales, None)
}

#[inline]
fn patch_uncertified<T>(
    requests: &[PointRequest; LANES],
    certified: core::simd::Mask<i64, LANES>,
    out: &mut [T; LANES],
    exact: impl Fn(PointRequest) -> Result<T, QuantizeError>,
) -> Result<(), QuantizeError> {
    let failures = !certified;
    if failures.any() {
        let mut mask = failures.to_bitmask();
        while mask != 0 {
            let i = mask.trailing_zeros() as usize;
            out[i] = exact(requests[i])?;
            mask &= mask - 1;
        }
    }
    Ok(())
}

#[inline]
fn round_block<const N: usize>(
    scaled: Simd<f64, N>,
    error: Option<f64>,
) -> (Simd<i64, N>, core::simd::Mask<i64, N>) {
    let magnitude = scaled.abs();
    let encoded = magnitude + Simd::splat(MAGIC);
    let rounded = encoded - Simd::splat(MAGIC);
    let distance = (magnitude - rounded).abs();
    let bounded = error.map_or(distance, |error| distance + Simd::splat(error));
    let certified = magnitude.simd_lt(Simd::splat(LIMIT)) & bounded.simd_lt(Simd::splat(0.5));
    let unsigned = encoded.to_bits() & Simd::splat(MANTISSA);
    let signed = unsigned.cast::<i64>();
    (
        scaled.simd_lt(Simd::splat(0.0)).select(-signed, signed),
        certified,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn oracle(request: PointRequest) -> Result<Bucket, QuantizeError> {
        request
            .quantization
            .resolve_exact(request.previous, Estimate::point(request.value))
    }

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
                    let exact = oracle(*request);
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
                        if oracle(*request).is_err() {
                            request.value = 0.0;
                            *out = oracle(*request)?;
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
    fn compiled_points_preserve_exact_hysteresis_at_all_delivery_buckets()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut random = 0x18c3_569f_ab37_854d;
        for places in 0..=Quantization::MAX_DECIMAL_PLACES {
            for width in [0, 1, 2, 1024, u32::MAX] {
                let q = Quantization::new(places, width)?;
                for round in 0..256_u32 {
                    let r = next(&mut random);
                    let integer = i32::try_from(r & 0x7fff_ffff)?;
                    let center = f64::from(integer) * if round & 1 == 0 { 1.0 } else { -1.0 };
                    let boundary = (center + 0.5) / q.scale();
                    let values = [
                        0.5,
                        -0.5,
                        boundary,
                        boundary.next_up(),
                        boundary.next_down(),
                        f64::from_bits(r),
                        f64::from_bits(1),
                        LIMIT / q.scale(),
                        -9_223_372_036_854_775_808.0 / q.scale(),
                    ];
                    let mut requests = values.map(|value| PointRequest {
                        quantization: q,
                        previous: None,
                        value,
                    });
                    let mut first_error = None;
                    for request in &requests {
                        let exact = oracle(*request);
                        let prepared = request.prepare();
                        match exact {
                            Ok(bucket) => {
                                let prepared = prepared?;
                                let raw = bucket.raw();
                                let w = i64::from(width);
                                for previous in [
                                    i64::MIN,
                                    i64::MAX,
                                    raw,
                                    raw.saturating_sub(w),
                                    raw.saturating_add(w),
                                    raw.saturating_sub(w).saturating_sub(1),
                                    raw.saturating_add(w).saturating_add(1),
                                ]
                                .map(Bucket::new)
                                {
                                    let reference = q.resolve_exact(
                                        Some(previous),
                                        Estimate::point(request.value),
                                    )?;
                                    assert_eq!(
                                        prepared.resolve(previous, width),
                                        reference,
                                        "{q:?} {previous:?} {request:?}"
                                    );
                                }
                            }
                            Err(error) => {
                                assert_eq!(prepared, Err(error.clone()));
                                first_error.get_or_insert(error);
                            }
                        }
                    }
                    let mut output = [PreparedPoint::Interior(Bucket::new(0)); 9];
                    let result = PointRequest::prepare_batch(&requests, &mut output);
                    if let Some(error) = first_error {
                        assert_eq!(result, Err(error));
                        for request in &mut requests {
                            if request.prepare().is_err() {
                                request.value = 0.0;
                            }
                        }
                        PointRequest::prepare_batch(&requests, &mut output)?;
                    } else {
                        result?;
                    }
                    for (&request, prepared) in requests.iter().zip(output) {
                        assert_eq!(prepared, request.prepare()?);
                    }
                }
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
    #[test]
    fn certified_intervals_match_exact_dyadic_oracle() -> Result<(), Box<dyn std::error::Error>> {
        let mut random = 0x7d3c_28a1_9e37_79b9;
        let mut certified = 0_usize;
        for places in 0..=Quantization::MAX_DECIMAL_PLACES {
            let q = Quantization::new(places, 0)?;
            for round in 0..16_384_u32 {
                let r = next(&mut random);
                let q = Quantization::new(places, u32::try_from((r >> 32) & 15)?)?;
                let integer = f64::from(i32::try_from(r & 0x7fff_ffff)?)
                    * if r & 16 == 0 { 1.0 } else { -1.0 };
                let scale = q.scale();
                let boundary = (integer + 0.5) / scale;
                let center = match round % 9 {
                    0 => f64::from_bits(r),
                    1 => boundary,
                    2 => boundary.next_up(),
                    3 => boundary.next_down(),
                    4 => (integer + 0.25) / scale,
                    5 => (integer + 0.125) / scale,
                    6 => LIMIT / scale,
                    7 => -LIMIT / scale,
                    _ => f64::MIN_POSITIVE,
                };
                let radius = match (round / 9) % 10 {
                    0 => f64::from_bits(next(&mut random)),
                    1 => f64::from_bits(1),
                    2 => f64::EPSILON / scale,
                    3 => 0.25 / scale,
                    4 => 0.125 / scale,
                    5 => 1.0 / scale,
                    6 => f64::from(u32::try_from(r >> 32)?) / scale,
                    7 => LIMIT / scale,
                    8 => center.abs() / 2.0,
                    _ => -0.25 / scale,
                };
                let previous = match round % 5 {
                    0 => None,
                    1 => Some(Bucket::new(i64::MIN)),
                    2 => Some(Bucket::new(i64::MAX)),
                    3 => Some(Bucket::new(i64::from(i32::try_from(r & 0x7fff_ffff)?))),
                    _ => q.resolve_exact(None, Estimate::point(center)).ok(),
                };
                let estimate = Estimate { center, radius };
                let expected = q.resolve_exact(previous, estimate);
                if let Some(actual) = resolve_interval(q, previous, estimate) {
                    certified += 1;
                    assert_eq!(actual, expected, "{q:?} {previous:?} {estimate:?}");
                }
                assert_eq!(
                    q.resolve(previous, estimate),
                    expected,
                    "{q:?} {previous:?} {estimate:?}"
                );
            }
            // The fast path is exercised at every supported scale.
            assert!(
                resolve_interval(
                    q,
                    None,
                    Estimate {
                        center: 3.125 / q.scale(),
                        radius: 0.125 / q.scale()
                    }
                )
                .is_some()
            );
        }
        assert!(certified > 10_000, "only {certified} certified intervals");
        Ok(())
    }

    #[test]
    fn interval_certificate_rejects_uncertain_ties_and_invalid_values() {
        let q = Quantization::INTEGER;
        for center in [0.5, -0.5, LIMIT, -LIMIT, f64::INFINITY, f64::NAN] {
            let estimate = Estimate {
                center,
                radius: f64::from_bits(1),
            };
            assert!(resolve_interval(q, None, estimate).is_none());
        }
        for radius in [-1.0, f64::NAN, f64::INFINITY] {
            assert!(
                resolve_interval(
                    q,
                    None,
                    Estimate {
                        center: 0.0,
                        radius
                    }
                )
                .is_none()
            );
        }
    }
}

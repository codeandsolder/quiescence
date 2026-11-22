//! Bounded-estimate quantization throughput and fallback workloads.
#![forbid(unsafe_code)]
use quiescence_core::{Bucket, Estimate, Quantization, QuantizeError};
use std::{error::Error, hint::black_box, time::Instant};

const REQUESTS: usize = 4096;
const ROUNDS: usize = 2048;

fn main() -> Result<(), Box<dyn Error>> {
    let mode = std::env::args()
        .nth(1)
        .unwrap_or_else(|| String::from("interior"));
    let requests = (0..REQUESTS)
        .map(|i| {
            let q = Quantization::new(u8::try_from(i % 12)?, u32::try_from(i % 4)?)?;
            let integer = i32::try_from(i % 3000)? - 1500;
            let center = f64::from(integer);
            let (center, radius) = match mode.as_str() {
                "interior" => (center + 0.125, 0.125),
                "boundary" => (center + 0.5, 0.000_000_000_01),
                "fallback" => (center + 0.5, 1e-30),
                "ambiguous" => (center + 0.5, 0.75),
                "wide" => (center + 1.5, 2.25),
                "point" => (center + 0.125, 0.0),
                _ => {
                    return Err(
                        "expected interior, boundary, fallback, ambiguous, wide or point".into(),
                    );
                }
            };
            Ok((
                q,
                Some(Bucket::new(i64::from(integer))),
                Estimate {
                    center: center / q.scale(),
                    radius: radius / q.scale(),
                },
            ))
        })
        .collect::<Result<Vec<_>, Box<dyn Error>>>()?;
    let start = Instant::now();
    let mut successes = 0_u64;
    let mut ambiguous = 0_u64;
    let mut checksum = 0_i64;
    for _ in 0..ROUNDS {
        for &(q, previous, estimate) in black_box(&requests) {
            match black_box(q.resolve(previous, estimate)) {
                Ok(bucket) => {
                    successes += 1;
                    checksum = checksum.wrapping_add(bucket.raw());
                }
                Err(QuantizeError::Ambiguous { .. }) => ambiguous += 1,
                Err(error) => return Err(error.into()),
            }
        }
    }
    println!(
        "{mode}: {:.3} ns/estimate, successes {successes}, ambiguous {ambiguous}, checksum {checksum}",
        start.elapsed().as_secs_f64() * 1e9 / f64::from(u32::try_from(REQUESTS * ROUNDS)?)
    );
    Ok(())
}

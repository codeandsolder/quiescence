//! Exact point fallback and certified fast path workloads.
#![forbid(unsafe_code)]
use quiescence_core::{Bucket, PointRequest, PreparedPoint, Quantization};
use std::{error::Error, hint::black_box, time::Instant};

fn main() -> Result<(), Box<dyn Error>> {
    let mut args = std::env::args().skip(1);
    let mode = args.next().unwrap_or_else(|| "midpoint".to_owned());
    let path = args.next().unwrap_or_else(|| "resolve".to_owned());
    if !matches!(
        mode.as_str(),
        "midpoint" | "near" | "large" | "interior" | "mixed"
    ) || !matches!(path.as_str(), "resolve" | "prepare")
    {
        return Err("expected midpoint, near, large, interior or mixed; resolve or prepare".into());
    }
    let mut points = Vec::with_capacity(1024);
    for index in 0..1024_u32 {
        let places = u8::try_from(index % 16)?;
        let q = Quantization::new(places, index % 5)?;
        let center = f64::from(index) - 512.0;
        let value = match mode.as_str() {
            "midpoint" => (center + 0.5) / q.scale(),
            "near" => ((center + 0.5) / q.scale()).next_up(),
            "large" => (center * 8_796_093_022_208.0) / q.scale(),
            "mixed" if index % 8 == 0 => (center + 0.5) / q.scale(),
            _ => (center + 0.125) / q.scale(),
        };
        points.push(PointRequest {
            quantization: q,
            previous: Some(Bucket::new(i64::from(index) - 512)),
            value,
        });
    }
    let mut buckets = vec![Bucket::new(0); points.len()];
    let mut prepared = vec![PreparedPoint::Interior(Bucket::new(0)); points.len()];
    let start = Instant::now();
    for _ in 0..8192 {
        if path == "resolve" {
            PointRequest::resolve_batch(black_box(&points), black_box(&mut buckets))?;
        } else {
            PointRequest::prepare_batch(black_box(&points), black_box(&mut prepared))?;
        }
    }
    println!("{mode} {path}: {:?}", start.elapsed());
    black_box(buckets);
    black_box(prepared);
    Ok(())
}

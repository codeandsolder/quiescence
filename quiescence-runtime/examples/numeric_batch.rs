//! Repeatable smoke measurements for independent quantization and graph writes.
#![forbid(unsafe_code)]

use quiescence_core::{Bucket, PointRequest, PreparedPoint, Quantization};
use quiescence_runtime::{Graph, RuntimeConfig, StateWrite};
use std::{error::Error, hint::black_box, time::Instant};

const SAMPLES: usize = 8_388_608;

fn requests(size: usize) -> Result<Vec<PointRequest>, Box<dyn Error>> {
    (0..size)
        .map(|i| {
            let quantization = Quantization::new(u8::try_from(i % 6)?, u32::try_from(i % 4)?)?;
            let integer = i32::try_from(i % 2_000)? - 1_000;
            Ok(PointRequest {
                quantization,
                previous: Some(Bucket::new(i64::from(integer))),
                value: (f64::from(integer) + 0.37) / quantization.scale(),
            })
        })
        .collect()
}

fn measure(
    label: &str,
    size: usize,
    mut run: impl FnMut(usize) -> Result<(), Box<dyn Error>>,
) -> Result<(), Box<dyn Error>> {
    let rounds = SAMPLES / size;
    let start = Instant::now();
    for round in 0..rounds {
        run(round)?;
    }
    let ns = start.elapsed().as_secs_f64() * 1e9 / f64::from(u32::try_from(rounds * size)?);
    println!("{label:<24} {size:>5} {ns:>10.3} ns/item");
    Ok(())
}

fn quantization(size: usize, scalar: bool) -> Result<(), Box<dyn Error>> {
    let points = requests(size)?;
    let mut buckets = vec![Bucket::new(0); size];
    let label = if scalar { "scalar" } else { "portable SIMD" };
    measure(label, size, |_| {
        let points = black_box(&points);
        let buckets = black_box(&mut buckets);
        if scalar {
            for (&request, bucket) in points.iter().zip(buckets) {
                *bucket = request.resolve()?;
            }
        } else {
            PointRequest::resolve_batch(points, buckets)?;
        }
        Ok(())
    })?;
    black_box(buckets);
    Ok(())
}

fn point_preparation(size: usize, scalar: bool) -> Result<(), Box<dyn Error>> {
    let points = requests(size)?;
    let mut prepared = vec![PreparedPoint::Interior(Bucket::new(0)); size];
    let label = if scalar {
        "scalar preparation"
    } else {
        "batch preparation"
    };
    measure(label, size, |_| {
        let points = black_box(&points);
        let prepared = black_box(&mut prepared);
        if scalar {
            for (&request, point) in points.iter().zip(prepared) {
                *point = request.prepare()?;
            }
        } else {
            PointRequest::prepare_batch(points, prepared)?;
        }
        Ok(())
    })?;
    black_box(prepared);
    Ok(())
}

fn graph_writes(size: usize, mode: &str, stride: usize) -> Result<(), Box<dyn Error>> {
    let mut graph = Graph::new(RuntimeConfig::default());
    let mut first = Vec::with_capacity(size);
    let mut second = Vec::with_capacity(size);
    for (i, request) in requests(size)?.into_iter().enumerate() {
        if i != 0 {
            for _ in 1..stride {
                graph.add_atom_input("unused", 0)?;
            }
        }
        if mode.starts_with("mixed") && i.is_multiple_of(5) {
            let node = graph.add_atom_input("atom", 1)?;
            first.push(StateWrite::atom(node, 1));
            second.push(StateWrite::atom(node, 2));
        } else {
            let q = request.quantization;
            let node = graph.add_input("numeric", q, request.value)?;
            first.push(StateWrite::new(node, request.value));
            second.push(StateWrite::new(node, request.value + 16.0 / q.scale()));
        }
    }
    if mode.ends_with("unsorted") {
        // Deterministic permutation, independent of hash seeds.
        let mut random = 0x9e37_79b9_7f4a_7c15_u64;
        for i in (1..size).rev() {
            random ^= random << 13;
            random ^= random >> 7;
            random ^= random << 17;
            let index = usize::try_from(random % u64::try_from(i + 1)?)?;
            first.swap(i, index);
            second.swap(i, index);
        }
    }
    measure(mode, size, |round| {
        let writes = if mode.contains("change") && round % 2 == 1 {
            &second
        } else {
            &first
        };
        graph.set_state_now(black_box(writes))?;
        if mode.contains("change") {
            black_box(graph.take_transitions());
        }
        Ok(())
    })
}

fn main() -> Result<(), Box<dyn Error>> {
    let mut args = std::env::args().skip(1);
    let mode = args.next().unwrap_or_else(|| String::from("simd"));
    let size = args
        .next()
        .map_or(Ok(1_024), |value| value.parse::<usize>())?;
    let stride = args.next().map_or(Ok(1), |value| value.parse::<usize>())?;
    if size == 0 || size > SAMPLES || stride == 0 || args.next().is_some() {
        return Err("expected a nonzero batch size up to 8388608".into());
    }
    match mode.as_str() {
        "scalar" => quantization(size, true),
        "simd" => quantization(size, false),
        "prepare" => point_preparation(size, false),
        "prepare-scalar" => point_preparation(size, true),
        "quiet" | "change" | "mixed" | "mixed-change" | "quiet-unsorted"
        | "mixed-change-unsorted" => graph_writes(size, &mode, stride),
        _ => Err("mode must be scalar, simd, prepare, prepare-scalar, quiet, change, mixed, mixed-change, quiet-unsorted or mixed-change-unsorted".into()),
    }
}

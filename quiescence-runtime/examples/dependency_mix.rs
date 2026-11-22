//! Dependency-graph throughput under stable and changing subscriptions.
#![forbid(unsafe_code)]

use quiescence_core::{NodeId, Quantization};
use quiescence_runtime::{EvalOutput, Graph, RuntimeConfig};
use std::{
    error::Error,
    hint::black_box,
    sync::Arc,
    time::{Duration, Instant},
};

const ROUNDS: usize = 12;

fn median(samples: &mut [Duration]) -> Duration {
    samples.sort_unstable();
    samples[samples.len() / 2]
}

fn run(mode: &str, count: usize, width: usize, places: u8) -> Result<(), Box<dyn Error>> {
    let mut graph = Graph::new(RuntimeConfig {
        same_time_microstep_budget: count + 16,
    });
    let q = Quantization::new(places, 0)?;
    let shared = graph.add_input("shared", q, 0.0)?;
    let gate = graph.add_atom_input("gate", 0)?;
    let started = Instant::now();
    let mut last = shared;
    let shared_reads: Arc<[NodeId]> = (0..width)
        .map(|_| graph.add_input("shared read", q, 0.0))
        .collect::<Result<Vec<_>, _>>()?
        .into();
    for _ in 0..count {
        last = match mode {
            "chain" => {
                let upstream = last;
                graph.add_derived("chain", q, 0.0, move |ctx| {
                    Ok(EvalOutput::immediate(ctx.read_f64(upstream)?))
                })?
            }
            "fanout" => graph.add_derived("fanout", q, 0.0, move |ctx| {
                Ok(EvalOutput::immediate(ctx.read_f64(shared)?))
            })?,
            "wide" | "dynamic" => {
                let sources = Arc::clone(&shared_reads);
                let dynamic = mode == "dynamic";
                graph.add_derived("read set", q, 0.0, move |ctx| {
                    let mut sum = ctx.read_f64(shared)?;
                    let parity = if dynamic { ctx.read_atom(gate)? % 2 } else { 0 };
                    for (i, &source) in sources.iter().enumerate() {
                        if !dynamic || i as u64 % 2 == parity {
                            sum += ctx.read_f64(source)?;
                            // Repeated observations must not grow the read set.
                            sum += ctx.read_f64(source)?;
                        }
                    }
                    Ok(EvalOutput::immediate(sum))
                })?
            }
            _ => return Err("expected chain, fanout, wide or dynamic".into()),
        };
    }
    graph.settle()?;
    println!(
        "mode {mode}, nodes {count}, width {width}, build+settle {:?}",
        started.elapsed()
    );

    let mut samples = [Duration::ZERO; ROUNDS];
    for (round, sample) in samples.iter_mut().enumerate() {
        let started = Instant::now();
        if mode == "dynamic" {
            graph.set_atom_now(gate, u64::try_from(round + 1)?)?;
        } else {
            graph.set_input_now(shared, f64::from(u32::try_from(round + 1)?))?;
        }
        *sample = started.elapsed();
        black_box(graph.take_transitions());
    }
    println!(
        "median update {:?}, last {}",
        median(&mut samples),
        graph.value_f64(last)?
    );
    Ok(())
}

fn main() -> Result<(), Box<dyn Error>> {
    let mut args = std::env::args().skip(1);
    let mode = args.next().unwrap_or_else(|| String::from("chain"));
    let count = args
        .next()
        .map_or(Ok(100_000), |arg| arg.parse::<usize>())?;
    let width = args.next().map_or(Ok(4), |arg| arg.parse::<usize>())?;
    let places = args.next().map_or(Ok(0), |arg| arg.parse::<u8>())?;
    run(&mode, count, width, places)
}

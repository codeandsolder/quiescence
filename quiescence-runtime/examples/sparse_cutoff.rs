#![forbid(unsafe_code)]

use quiescence_core::Quantization;
use quiescence_runtime::{EvalOutput, Graph, RuntimeConfig};
use std::{error::Error, time::Instant};

const DERIVED_NODES: usize = 100_000;

fn main() -> Result<(), Box<dyn Error>> {
    let mut graph = Graph::new(RuntimeConfig {
        same_time_microstep_budget: DERIVED_NODES + 16,
    });
    let source = graph.add_input("source", Quantization::new(2, 0)?, 0.0)?;
    let whole = Quantization::new(0, 0)?;

    let build_started = Instant::now();
    let mut previous = source;
    for _ in 0..DERIVED_NODES {
        let upstream = previous;
        previous = graph.add_derived("chain", whole, 0.0, move |ctx| {
            Ok(EvalOutput::immediate(ctx.read_f64(upstream)?))
        })?;
    }
    graph.settle()?;
    let build_and_settle = build_started.elapsed();

    let cutoff_started = Instant::now();
    graph.set_input_now(source, 0.04)?;
    let cutoff = cutoff_started.elapsed();

    let propagate_started = Instant::now();
    graph.set_input_now(source, 1.0)?;
    let full_propagation = propagate_started.elapsed();

    println!("nodes:              {}", DERIVED_NODES + 1);
    println!("build + settle:     {build_and_settle:?}");
    println!("cut off at node #1: {cutoff:?}");
    println!("propagate all:      {full_propagation:?}");
    println!("last value:         {}", graph.value_f64(previous)?);
    Ok(())
}

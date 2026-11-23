//! Changing subscription cardinality with a fixed read-only source pool.
#![forbid(unsafe_code)]
use quiescence_core::Quantization;
use quiescence_runtime::{EvalOutput, Graph, RuntimeConfig};
use std::{error::Error, hint::black_box, sync::Arc, time::Instant};

fn main() -> Result<(), Box<dyn Error>> {
    let mut args = std::env::args().skip(1);
    let count = args
        .next()
        .map_or(Ok(10_000_usize), |arg| arg.parse::<usize>())?;
    let narrow = args
        .next()
        .map_or(Ok(16_usize), |arg| arg.parse::<usize>())?;
    let wide = args
        .next()
        .map_or(Ok(64_usize), |arg| arg.parse::<usize>())?;
    if count == 0 || narrow == 0 || narrow > wide || wide > 4096 {
        return Err("expected positive node count and 0 < narrow <= wide <= 4096".into());
    }
    let mut graph = Graph::new(RuntimeConfig {
        same_time_microstep_budget: count + 16,
    });
    let gate = graph.add_atom_input("gate", 0)?;
    let q = Quantization::INTEGER;
    let sources: Arc<[_]> = (0..wide)
        .map(|_| graph.add_input("source", q, 0.0))
        .collect::<Result<Vec<_>, _>>()?
        .into();
    let start = Instant::now();
    for _ in 0..count {
        let ids = Arc::clone(&sources);
        graph.add_derived("subscriber", q, 0.0, move |ctx| {
            let take = if ctx.read_atom(gate)? & 1 == 0 {
                wide
            } else {
                narrow
            };
            for &id in ids.iter().take(take) {
                black_box(ctx.read_f64(id)?);
            }
            Ok(EvalOutput::immediate(0.0))
        })?;
    }
    graph.settle()?;
    let built = start.elapsed();
    let start = Instant::now();
    for round in 0..16_u64 {
        graph.set_atom_now(gate, round + 1)?;
        black_box(graph.take_transitions());
    }
    println!(
        "{count} subscribers, cardinality {narrow}/{wide}: build {built:?}, 16 toggles {:?}",
        start.elapsed()
    );
    Ok(())
}

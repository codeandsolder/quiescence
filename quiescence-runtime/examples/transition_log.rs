//! Transition log throughput and retained publication footprint.
#![forbid(unsafe_code)]
use quiescence_core::{Quantization, SimTime};
use quiescence_runtime::{Graph, RuntimeConfig, StateWrite};
use std::{error::Error, hint::black_box, time::Instant};

fn main() -> Result<(), Box<dyn Error>> {
    let mut args = std::env::args().skip(1);
    let mode = args.next().unwrap_or_else(|| "atom".to_owned());
    let count = args
        .next()
        .map_or(Ok(1024_usize), |arg| arg.parse::<usize>())?;
    let rounds = args
        .next()
        .map_or(Ok(8192_usize), |arg| arg.parse::<usize>())?;
    let drain = args
        .next()
        .map_or(Ok(1_usize), |arg| arg.parse::<usize>())?;
    if count == 0
        || drain == 0
        || !matches!(mode.as_str(), "atom" | "numeric" | "mixed" | "growing")
    {
        return Err(
            "expected atom, numeric, mixed or growing; positive node count and drain interval"
                .into(),
        );
    }
    let mut graph = Graph::new(RuntimeConfig::default());
    let mut batches = [Vec::with_capacity(count), Vec::with_capacity(count)];
    for index in 0..count {
        let numeric = mode == "numeric" || (mode == "mixed" && index.is_multiple_of(2));
        let label = format!("node {index}");
        let node = if numeric {
            graph.add_input(label, Quantization::INTEGER, 0.0)?
        } else {
            graph.add_atom_input(label, 0)?
        };
        for (bit, batch) in batches.iter_mut().enumerate() {
            batch.push(if numeric {
                StateWrite::new(node, f64::from(u32::try_from(bit)?))
            } else {
                StateWrite::atom(node, u64::try_from(bit)?)
            });
        }
    }
    let start = Instant::now();
    let mut retained = Vec::new();
    let mut transitions = 0;
    for round in 0..rounds {
        graph.set_state_now(&batches[(round + 1) % 2])?;
        if (round + 1).is_multiple_of(drain) {
            let publication = graph.take_transitions();
            transitions += publication.len();
            if mode == "growing" {
                retained.push(publication);
                graph.add_atom_input(format!("extra {round}"), 0)?;
            } else {
                black_box(publication);
            }
        }
    }
    let remaining = graph.take_transitions();
    transitions += remaining.len();
    println!(
        "{mode}, {count} nodes, {rounds} rounds, {drain} drain interval: {transitions} transitions in {:?}",
        start.elapsed()
    );
    black_box(remaining);
    black_box(retained);
    black_box(graph.now() == SimTime::ZERO);
    Ok(())
}

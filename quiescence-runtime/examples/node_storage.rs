//! Graph storage footprint and locality for large mostly-input workloads.
#![forbid(unsafe_code)]
use quiescence_core::{NodeId, Quantization};
use quiescence_runtime::{EvalOutput, Graph, RuntimeConfig};
use std::{error::Error, hint::black_box, sync::Arc, time::Instant};

fn main() -> Result<(), Box<dyn Error>> {
    let mut args = std::env::args().skip(1);
    let count = args
        .next()
        .map_or(Ok(1_000_000), |arg| arg.parse::<usize>())?;
    let derived_percent = args.next().map_or(Ok(10), |arg| arg.parse::<usize>())?;
    if count == 0 || derived_percent > 100 {
        return Err("expected positive node count and derived percentage 0..=100".into());
    }
    let mut graph = Graph::new(RuntimeConfig {
        same_time_microstep_budget: count + 16,
    });
    let label: Arc<str> = Arc::from("shared-node-label");
    let q = Quantization::INTEGER;
    let source = graph.add_input(Arc::clone(&label), q, 0.0)?;
    let began = Instant::now();
    for i in 0..count {
        if i % 100 < derived_percent {
            graph.add_derived(Arc::clone(&label), q, 0.0, move |ctx| {
                Ok(EvalOutput::immediate(ctx.read_f64(source)?))
            })?;
        } else if i.is_multiple_of(2) {
            graph.add_input(Arc::clone(&label), q, 0.0)?;
        } else {
            graph.add_atom_input(Arc::clone(&label), 0)?;
        }
    }
    graph.settle()?;
    println!(
        "{count} nodes {derived_percent}% derived: build+settle {:?}",
        began.elapsed()
    );
    let began = Instant::now();
    for _ in 0..16 {
        let snapshot = graph.quiescent_snapshot()?;
        black_box(snapshot.value(NodeId::from_index(u32::try_from(count)?)));
        black_box(snapshot);
    }
    println!("16 snapshots {:?}", began.elapsed());
    Ok(())
}

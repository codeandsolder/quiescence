//! Distributed small fanouts, with all dependency groups changing together.
#![forbid(unsafe_code)]
use quiescence_core::Quantization;
use quiescence_runtime::{EvalOutput, Graph, RuntimeConfig, StateWrite};
use std::{error::Error, hint::black_box, sync::Arc, time::Instant};

fn main() -> Result<(), Box<dyn Error>> {
    let mut args = std::env::args().skip(1);
    let groups = args
        .next()
        .map_or(Ok(4096_usize), |arg| arg.parse::<usize>())?;
    let width = args
        .next()
        .map_or(Ok(8_usize), |arg| arg.parse::<usize>())?;
    let stride = args
        .next()
        .map_or(Ok(1_usize), |arg| arg.parse::<usize>())?;
    if groups == 0 || width == 0 || stride == 0 {
        return Err("expected positive group count, fanout and stride".into());
    }
    let mut graph = Graph::new(RuntimeConfig {
        same_time_microstep_budget: groups * width + 16,
    });
    let label: Arc<str> = Arc::from("node");
    let started = Instant::now();
    let mut batches = [Vec::with_capacity(groups), Vec::with_capacity(groups)];
    let q = Quantization::INTEGER;
    for _ in 0..groups {
        let source = graph.add_input(Arc::clone(&label), q, 0.0)?;
        for (bit, batch) in batches.iter_mut().enumerate() {
            batch.push(StateWrite::new(source, f64::from(u32::try_from(bit)?)));
        }
        for _ in 0..width {
            for _ in 1..stride {
                graph.add_atom_input(Arc::clone(&label), 0)?;
            }
            graph.add_derived(Arc::clone(&label), q, 0.0, move |ctx| {
                Ok(EvalOutput::immediate(ctx.read_f64(source)?))
            })?;
        }
    }
    graph.settle()?;
    println!(
        "{groups} groups, width {width}, stride {stride}: build+settle {:?}",
        started.elapsed()
    );
    let started = Instant::now();
    for round in 0..32 {
        graph.set_state_now(&batches[(round + 1) % 2])?;
        black_box(graph.take_transitions());
    }
    println!("32 updates: {:?}", started.elapsed());
    Ok(())
}

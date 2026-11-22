//! Compare a large live forecast set with repeated replacement of deadlines.
#![forbid(unsafe_code)]

use quiescence_core::{Quantization, SimDuration, SimTime};
use quiescence_runtime::{EvalOutput, Graph, RuntimeConfig};
use std::{error::Error, hint::black_box, time::Instant};

fn main() -> Result<(), Box<dyn Error>> {
    let mut args = std::env::args().skip(1);
    let owners = args.next().map_or(Ok(1_000), |arg| arg.parse::<usize>())?;
    let updates = args.next().map_or(Ok(100), |arg| arg.parse::<usize>())?;
    let mode = args.next().unwrap_or_else(|| String::from("equal"));
    let budget = owners
        .checked_mul(updates)
        .and_then(|n| n.checked_add(owners + 16))
        .ok_or("workload too large")?;
    let mut graph = Graph::new(RuntimeConfig {
        same_time_microstep_budget: budget,
    });
    let q = Quantization::INTEGER;
    let source = graph.add_input("source", q, 0.0)?;
    let mut last = source;
    for index in 0..owners {
        let delay = match mode.as_str() {
            "equal" => SimDuration::from_millis(10),
            "dispersed" => SimDuration::from_nanos(i64::try_from(index)? + 1),
            _ => return Err("expected equal or dispersed".into()),
        };
        last = graph.add_derived("forecast", q, 0.0, move |ctx| {
            Ok(EvalOutput::after(ctx.read_f64(source)?, delay))
        })?;
    }
    graph.settle()?;
    let start = Instant::now();
    for turn in 1..=updates {
        graph.set_input_now(source, f64::from(u32::try_from(turn)?))?;
        if turn.is_multiple_of(1024) {
            black_box(graph.take_transitions());
        }
    }
    let scheduled = start.elapsed();
    let start = Instant::now();
    graph.advance_to(SimTime::from_nanos(i64::try_from(owners)?.max(10_000_000)))?;
    let settled = start.elapsed();
    println!(
        "owners {owners}, updates {updates}, {mode}: schedule {scheduled:?}, settle {settled:?}, last {}",
        graph.value_f64(last)?
    );
    black_box(graph.take_transitions());
    Ok(())
}

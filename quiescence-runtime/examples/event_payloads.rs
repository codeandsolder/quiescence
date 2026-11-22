//! Queue footprint and throughput for ordinary callbacks and reaction forecasts.
#![forbid(unsafe_code)]
use quiescence_core::{SimDuration, SimTime};
use quiescence_runtime::{Graph, ReactionOutput, RuntimeConfig, StateWrite};
use std::{error::Error, hint::black_box, time::Instant};

fn main() -> Result<(), Box<dyn Error>> {
    let mut args = std::env::args().skip(1);
    let mode = args.next().unwrap_or_else(|| "equal".to_owned());
    let count = args
        .next()
        .map_or(Ok(1_000_000_u32), |arg| arg.parse::<u32>())?;
    let mut graph = Graph::new(RuntimeConfig {
        same_time_microstep_budget: usize::try_from(count)? * 4 + 16,
    });
    let start = Instant::now();
    match mode.as_str() {
        "equal" | "dispersed" => {
            let callback = graph.add_event_callback("callback", |ctx| {
                black_box(ctx.external_token());
                Ok(ReactionOutput::idle())
            })?;
            for index in 0..count {
                let at = if mode == "equal" {
                    4096
                } else {
                    i64::from(index.wrapping_mul(2_654_435_761))
                };
                graph.schedule_reaction_event_with_token(
                    callback,
                    SimTime::from_nanos(at),
                    0,
                    u64::from(index),
                )?;
            }
        }
        "empty" | "write" | "history" | "both" | "multiple" => {
            let history = graph.add_event_window("history", SimDuration::from_nanos(100_000))?;
            for index in 0..count {
                let value = graph.add_atom_input("value", 0)?;
                let other = graph.add_atom_input("other", 0)?;
                let delay = SimDuration::from_nanos(i64::from(index % 4096) + 1);
                let has_writes = matches!(mode.as_str(), "write" | "both" | "multiple");
                let has_records = matches!(mode.as_str(), "history" | "both" | "multiple");
                let multiple = mode == "multiple";
                graph.add_reaction("forecast", move |ctx| {
                    if ctx.now() != SimTime::ZERO {
                        return Ok(ReactionOutput::idle());
                    }
                    let mut writes = Vec::new();
                    if has_writes {
                        writes.push(StateWrite::atom(value, 1));
                        if multiple {
                            writes.push(StateWrite::atom(other, 2));
                        }
                    }
                    let mut result = ReactionOutput::after(delay, writes);
                    if has_records {
                        result = result.and_record_window(history, u64::from(index));
                        if multiple {
                            result = result.and_record_window(history, u64::from(index) + 1);
                        }
                    }
                    Ok(result)
                })?;
            }
            graph.settle()?;
        }
        _ => {
            return Err(
                "expected equal, dispersed, empty, write, history, both or multiple".into(),
            );
        }
    }
    println!("{mode} {count}: schedule {:?}", start.elapsed());
    let start = Instant::now();
    graph.advance_to(SimTime::from_nanos(i64::from(u32::MAX)))?;
    println!("delivery {:?}", start.elapsed());
    black_box(graph.take_transitions());
    black_box(graph.quiescent_snapshot()?);
    Ok(())
}

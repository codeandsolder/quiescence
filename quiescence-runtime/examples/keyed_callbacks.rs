//! Keyed callback admission, deduplication and delivery workloads.
#![forbid(unsafe_code)]
use quiescence_core::{SimDuration, SimTime};
use quiescence_runtime::{Graph, ReactionOutput, RuntimeConfig};
use std::{error::Error, hint::black_box, time::Instant};

fn main() -> Result<(), Box<dyn Error>> {
    let mut args = std::env::args().skip(1);
    let mode = args.next().unwrap_or_else(|| "batch".to_owned());
    let batch = args.next().map_or(Ok(64), |arg| arg.parse::<u32>())?;
    let total = args
        .next()
        .map_or(Ok(1_048_576), |arg| arg.parse::<u32>())?;
    if batch == 0 || total < batch {
        return Err("expected nonzero batch not exceeding total".into());
    }
    let mut graph = Graph::new(RuntimeConfig {
        same_time_microstep_budget: usize::try_from(total)? * 3 + 16,
    });
    let target = graph.add_event_callback("target", |ctx| {
        black_box(ctx.scheduled_key());
        Ok(ReactionOutput::idle())
    })?;
    let mode = mode.as_str();
    let start = Instant::now();
    if mode == "stream" {
        let callback = graph.add_event_callback("chain", move |ctx| {
            let index = ctx.scheduled_key().unwrap_or(0);
            black_box(index);
            if index + 1 < u64::from(total) {
                Ok(ReactionOutput::schedule_once_after(
                    SimDuration::from_nanos(1),
                    index + 1,
                    0,
                ))
            } else {
                Ok(ReactionOutput::idle())
            }
        })?;
        graph.schedule_reaction_event(callback, SimTime::ZERO, 0)?;
        graph.advance_to(SimTime::from_nanos(i64::from(total)))?;
    } else {
        let dispersed = mode == "dispersed";
        let overlap = mode == "overlap";
        let duplicate = mode == "duplicate";
        if !dispersed && !overlap && !duplicate && mode != "batch" {
            return Err("expected stream, batch, duplicate, overlap or dispersed".into());
        }
        let producer = graph.add_event_callback("producer", move |ctx| {
            let start = ctx.external_token().unwrap_or(0);
            let mut result = ReactionOutput::idle();
            for index in 0..u64::from(batch) {
                let key = start + index;
                let after = if dispersed {
                    i64::try_from(key % 4096).map_or(1, |value| value + 1)
                } else {
                    4096
                };
                let delay = SimDuration::from_nanos(after);
                result = result.and_schedule_reaction_once_after(target, delay, key, 0);
                if duplicate {
                    result = result.and_schedule_reaction_once_after(target, delay, key, 0);
                }
            }
            Ok(result)
        })?;
        for round in 0..total / batch {
            let token = if overlap {
                u64::from(round / 2) * u64::from(batch)
            } else {
                u64::from(round) * u64::from(batch)
            };
            graph.schedule_reaction_event_with_token(producer, SimTime::ZERO, 0, token)?;
        }
        graph.advance_to(SimTime::from_nanos(4096))?;
    }
    println!("{mode} {batch} {total}: {:?}", start.elapsed());
    black_box(graph.quiescent_snapshot()?);
    Ok(())
}

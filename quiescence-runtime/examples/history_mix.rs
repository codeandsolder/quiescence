//! Keyed temporal-history workload with configurable cardinality and expiry.
#![forbid(unsafe_code)]
use quiescence_core::{SimDuration, SimTime};
use quiescence_runtime::{Graph, ReactionOutput, RuntimeConfig};
use std::{error::Error, hint::black_box, time::Instant};

fn main() -> Result<(), Box<dyn Error>> {
    let mut args = std::env::args().skip(1);
    let count = args.next().map_or(Ok(100_000), |arg| arg.parse::<u64>())?;
    let keys = args.next().map_or(Ok(1024), |arg| arg.parse::<u64>())?;
    let duration = args.next().map_or(Ok(8192), |arg| arg.parse::<i64>())?;
    if keys == 0 || duration < 0 {
        return Err("keys must be positive and duration nonnegative".into());
    }
    let mut graph = Graph::new(RuntimeConfig::default());
    let history = graph.add_event_window("history", SimDuration::from_nanos(duration))?;
    let callback = graph.add_event_callback("record", move |ctx| {
        let key = ctx.external_token().unwrap_or_default();
        black_box(ctx.window_count(history, key)?);
        Ok(ReactionOutput::idle().and_record_window(history, key))
    })?;
    let start = Instant::now();
    for i in 0..count {
        let at = SimTime::from_nanos(i64::try_from(i)?);
        let key = i % keys;
        graph.schedule_reaction_event_with_token(callback, at, 0, key)?;
        graph.advance_to(at)?;
    }
    println!(
        "{count} events, {keys} keys, duration {duration}: {:?}, last count {}",
        start.elapsed(),
        graph.event_window_count(history, (count.saturating_sub(1)) % keys)?
    );
    Ok(())
}

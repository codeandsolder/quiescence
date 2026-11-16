#![forbid(unsafe_code)]

// Controlled smoke benchmark for equal-key, grouped, and dispersed causal events.
use quiescence_core::SimTime;
use quiescence_runtime::{Graph, ReactionOutput, RuntimeConfig};
use std::{
    error::Error,
    hint::black_box,
    time::{Duration, Instant},
};

const EVENTS: usize = 4_096;
const WARMUP: usize = 4;
const SAMPLES: usize = 15;

#[derive(Clone, Copy)]
enum Workload {
    Equal,
    Grouped,
    Dispersed,
}

impl Workload {
    const ALL: [Self; 3] = [Self::Equal, Self::Grouped, Self::Dispersed];

    const fn name(self) -> &'static str {
        match self {
            Self::Equal => "equal",
            Self::Grouped => "grouped",
            Self::Dispersed => "dispersed",
        }
    }

    fn key(self, index: usize) -> Result<(SimTime, i32), Box<dyn Error>> {
        match self {
            Self::Equal => Ok((SimTime::ZERO, 0)),
            Self::Grouped => Ok((
                SimTime::from_nanos(i64::try_from(index % 16)?),
                i32::try_from((index / 16) % 4)? - 2,
            )),
            Self::Dispersed => Ok((
                SimTime::from_nanos(i64::try_from((index * 179) % EVENTS)?),
                i32::try_from(index % 17)? - 8,
            )),
        }
    }
}

fn median(values: &mut [Duration]) -> Duration {
    values.sort_unstable();
    values[values.len() / 2]
}

fn main() -> Result<(), Box<dyn Error>> {
    for workload in Workload::ALL {
        let mut scheduling = Vec::with_capacity(SAMPLES);
        let mut settling = Vec::with_capacity(SAMPLES);
        for round in 0..WARMUP + SAMPLES {
            let mut graph = Graph::new(RuntimeConfig {
                same_time_microstep_budget: EVENTS * 2 + 16,
            });
            let callback = graph.add_event_callback("benchmark", |_| Ok(ReactionOutput::idle()))?;
            graph.quiesce()?;

            let started = Instant::now();
            for index in 0..EVENTS {
                let (at, priority) = workload.key(index)?;
                graph.schedule_reaction_event_with_token(
                    callback,
                    at,
                    priority,
                    u64::try_from(index)?,
                )?;
            }
            let scheduled = started.elapsed();

            let started = Instant::now();
            graph.advance_to(SimTime::from_nanos(i64::try_from(EVENTS)?))?;
            let settled = started.elapsed();

            black_box(graph.quiescent_snapshot()?);
            if round >= WARMUP {
                scheduling.push(scheduled);
                settling.push(settled);
            }
        }
        let name = workload.name();
        let scheduled = median(&mut scheduling);
        let settled = median(&mut settling);
        println!("{name}: events {EVENTS}, scheduling {scheduled:?}, settlement {settled:?}");
    }
    Ok(())
}

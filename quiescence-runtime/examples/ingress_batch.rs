#![forbid(unsafe_code)]

// Release-mode smoke benchmark for submission, admission, and same-time callback settlement.
// Not a substitute for statistically controlled microbenchmarks.
use quiescence_runtime::{Graph, ReactionOutput, RuntimeConfig, interaction_channel};
use std::{
    error::Error,
    hint::black_box,
    time::{Duration, Instant},
};

const CALLBACKS: usize = 8_192;
const WARMUP: usize = 5;
const SAMPLES: usize = 21;

fn median(values: &mut [Duration]) -> Duration {
    values.sort_unstable();
    values[values.len() / 2]
}

fn main() -> Result<(), Box<dyn Error>> {
    let mut submissions = Vec::with_capacity(SAMPLES);
    let mut imports = Vec::with_capacity(SAMPLES);
    let mut settlements = Vec::with_capacity(SAMPLES);
    for round in 0..WARMUP + SAMPLES {
        let mut graph = Graph::new(RuntimeConfig {
            same_time_microstep_budget: CALLBACKS * 2 + 16,
        });
        let callback =
            graph.add_event_callback("ingress benchmark", |_| Ok(ReactionOutput::idle()))?;
        graph.quiesce()?;
        let (sender, inbox) = interaction_channel();

        let started = Instant::now();
        for token in 0..CALLBACKS {
            sender.callback_now_with_token(callback, 0, u64::try_from(token)?)?;
        }
        let submission = started.elapsed();

        let started = Instant::now();
        black_box(graph.import_interactions(&inbox)?);
        let import = started.elapsed();

        let started = Instant::now();
        graph.quiesce()?;
        let settlement = started.elapsed();

        black_box(graph.quiescent_snapshot()?);
        if round >= WARMUP {
            submissions.push(submission);
            imports.push(import);
            settlements.push(settlement);
        }
    }
    println!("callbacks per sample: {CALLBACKS}, warmup: {WARMUP}, samples: {SAMPLES}");
    println!("median submission:   {:?}", median(&mut submissions));
    println!("median admission:    {:?}", median(&mut imports));
    println!("median settlement:   {:?}", median(&mut settlements));
    Ok(())
}

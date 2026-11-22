//! Admission and delivery of numeric and atom writes across causal boundaries.
#![forbid(unsafe_code)]
use quiescence_core::{Quantization, SimTime};
use quiescence_runtime::{Graph, RuntimeConfig, StateWrite, interaction_channel};
use std::{error::Error, hint::black_box, time::Instant};

fn main() -> Result<(), Box<dyn Error>> {
    let mut args = std::env::args().skip(1);
    let mode = args.next().unwrap_or_else(|| "numeric".to_owned());
    let size = args
        .next()
        .map_or(Ok(1_usize), |arg| arg.parse::<usize>())?;
    let count = args
        .next()
        .map_or(Ok(1_048_576_usize), |arg| arg.parse::<usize>())?;
    if size == 0
        || count < size
        || !matches!(mode.as_str(), "numeric" | "midpoint" | "mixed" | "atom")
    {
        return Err(
            "expected numeric, midpoint, mixed or atom, positive size and count >= size".into(),
        );
    }
    let mut graph = Graph::new(RuntimeConfig::default());
    let nodes = (0..size)
        .map(|i| {
            if mode == "atom" || (mode == "mixed" && i.is_multiple_of(5)) {
                graph.add_atom_input("atom", 0)
            } else {
                graph.add_input(
                    "numeric",
                    Quantization::new(u8::try_from(i % 6).unwrap_or(0), 1)?,
                    0.0,
                )
            }
        })
        .collect::<Result<Vec<_>, _>>()?;
    let make_write = |index: usize, round: usize| -> Result<StateWrite, Box<dyn Error>> {
        let bit = u32::try_from(round % 2)?;
        if mode == "atom" || (mode == "mixed" && index.is_multiple_of(5)) {
            Ok(StateWrite::atom(nodes[index], u64::from(bit)))
        } else {
            let q = Quantization::new(u8::try_from(index % 6)?, 1)?;
            let value = if mode == "midpoint" {
                f64::from(bit * 4) + 0.5
            } else {
                f64::from(bit * 4) + 0.125
            };
            Ok(StateWrite::new(nodes[index], value / q.scale()))
        }
    };
    let start = Instant::now();
    let (sender, inbox) = interaction_channel();
    let rounds = count / size;
    for round in 0..rounds {
        let at = SimTime::from_nanos(i64::try_from(round)? + 1);
        if size == 1 {
            graph.schedule_write(at, make_write(0, round)?)?;
        } else {
            let writes = (0..size)
                .map(|index| make_write(index, round))
                .collect::<Result<Vec<_>, _>>()?;
            sender.state_at(at, 0, writes)?;
        }
    }
    let admitted = graph.import_interactions(&inbox)?;
    if !admitted.rejected.is_empty() {
        return Err("benchmark had rejected interactions".into());
    }
    println!("{mode} {size} {count}: admission {:?}", start.elapsed());
    let start = Instant::now();
    graph.advance_to(SimTime::from_nanos(i64::try_from(rounds)?))?;
    let transitions = graph.take_transitions();
    println!(
        "delivery {:?}, changes {}",
        start.elapsed(),
        transitions.len()
    );
    black_box(transitions);
    Ok(())
}

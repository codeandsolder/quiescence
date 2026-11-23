//! Repeated publication of immutable snapshots with sparse or dense changes.
#![forbid(unsafe_code)]
use quiescence_runtime::{Graph, QuiescentSnapshot, RuntimeConfig, StateWrite};
use std::{error::Error, hint::black_box, sync::Arc, time::Instant};

fn main() -> Result<(), Box<dyn Error>> {
    let mut args = std::env::args().skip(1);
    let mode = args.next().unwrap_or_else(|| "static".to_owned());
    let count = args
        .next()
        .map_or(Ok(100_000_usize), |arg| arg.parse::<usize>())?;
    let rounds = args
        .next()
        .map_or(Ok(16_usize), |arg| arg.parse::<usize>())?;
    let retain = args.next().map_or(Ok(false), |arg| arg.parse::<bool>())?;
    if count == 0 || rounds == 0 || !matches!(mode.as_str(), "static" | "sparse" | "page" | "dense")
    {
        return Err(
            "expected static, sparse, page or dense; positive node count/rounds; boolean retention"
                .into(),
        );
    }
    let mut graph = Graph::new(RuntimeConfig::default());
    let label: Arc<str> = Arc::from("snapshot cell");
    let begun = Instant::now();
    let nodes = (0..count)
        .map(|_| graph.add_atom_input(Arc::clone(&label), 0))
        .collect::<Result<Vec<_>, _>>()?;
    graph.settle()?;
    let built = begun.elapsed();
    let dense: [Vec<_>; 2] = std::array::from_fn(|value| {
        nodes
            .iter()
            .map(|&node| StateWrite::atom(node, u64::from(value == 0)))
            .collect()
    });
    let begun = Instant::now();
    let mut held = Vec::<QuiescentSnapshot>::new();
    for round in 0..rounds {
        let tick = u64::try_from(round + 1)?;
        match mode.as_str() {
            "sparse" => {
                let index = (round * 9973) % count;
                graph.set_atom_now(nodes[index], tick)?;
            }
            "page" => {
                let index = (round * 257) % count;
                let writes = nodes[index..count.min(index + 256)]
                    .iter()
                    .map(|&node| StateWrite::atom(node, tick))
                    .collect::<Vec<_>>();
                graph.set_state_now(&writes)?;
            }
            "dense" => graph.set_state_now(&dense[round % 2])?,
            "static" => {}
            _ => return Err("invalid benchmark mode".into()),
        }
        let snapshot = graph.quiescent_snapshot()?;
        black_box(snapshot.value(nodes[round % count]));
        if retain {
            held.push(snapshot);
        } else {
            black_box(snapshot);
        }
        black_box(graph.take_transitions());
    }
    println!(
        "{mode} count {count} rounds {rounds} retain {retain}: built {built:?}, published {:?}",
        begun.elapsed()
    );
    black_box(held);
    Ok(())
}

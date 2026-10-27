use quiescence_core::{Quantization, SimDuration, SimTime};
use quiescence_runtime::{Graph, ReactionOutput, RuntimeConfig, StateWrite};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

const TICK_MS: i64 = 50;
const COOLDOWN_TICKS: i64 = 8;

fn transfer(source: u64, destination: u64) -> Option<(u64, u64)> {
    match (source, destination) {
        // Handles stand for immutable canonical inventory snapshots. The transaction logic that
        // creates new snapshots is deliberately outside Quiescence.
        (11, 21) => Some((12, 22)),
        (13, 23) => Some((14, 24)),
        _ => None,
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let q = Quantization::new(0, 0)?;
    let mut graph = Graph::new(RuntimeConfig::default());
    let ready_at_ms = graph.add_input("hopper.ready_at_ms", q, 400.0)?;
    let source_inventory = graph.add_atom_input("hopper.source_inventory", 10)?;
    let destination_inventory = graph.add_atom_input("hopper.destination_inventory", 20)?;
    let evaluations = Arc::new(AtomicUsize::new(0));
    let evaluation_counter = Arc::clone(&evaluations);

    let _hopper = graph.add_reaction("recovered vanilla hopper cooldown", move |context| {
        evaluation_counter.fetch_add(1, Ordering::Relaxed);
        let now_ms = context.now().as_nanos() / 1_000_000;
        let ready_at = context.read_f64(ready_at_ms)? as i64;

        // Recovered source shape: cooldown -= 1, attempt when <= 0, successful transfer resets
        // cooldown to 8. Once activation cadence is known to be 50 ms, the per-tick counter is an
        // absolute deadline. Inventory is intentionally not read while that deadline is pending.
        if now_ms < ready_at {
            return Ok(ReactionOutput::reevaluate_after(SimDuration::from_millis(
                ready_at - now_ms,
            )));
        }

        let source = context.read_atom(source_inventory)?;
        let destination = context.read_atom(destination_inventory)?;
        let Some((next_source, next_destination)) = transfer(source, destination) else {
            return Ok(ReactionOutput::idle());
        };
        Ok(ReactionOutput::immediate(vec![
            StateWrite::atom(source_inventory, next_source),
            StateWrite::atom(destination_inventory, next_destination),
            StateWrite::new(ready_at_ms, (now_ms + COOLDOWN_TICKS * TICK_MS) as f64),
        ]))
    })?;

    graph.settle()?;
    println!(
        "evaluations_after_initial_settle={}",
        evaluations.load(Ordering::Relaxed)
    );

    // These snapshots change while the cooldown is running. They are future transaction inputs,
    // but not current causal dependencies, so neither change wakes the hopper.
    graph.schedule_atom(source_inventory, SimTime::from_millis(100), 11)?;
    graph.schedule_atom(destination_inventory, SimTime::from_millis(200), 21)?;
    graph.advance_to(SimTime::from_millis(200))?;
    println!(
        "evaluations_during_cooldown={}",
        evaluations.load(Ordering::Relaxed)
    );

    graph.advance_to(SimTime::from_millis(400))?;
    println!(
        "first_transfer_source={}",
        graph.value_atom(source_inventory)?
    );
    println!(
        "first_transfer_destination={}",
        graph.value_atom(destination_inventory)?
    );
    println!(
        "first_next_ready_ms={}",
        graph.value_f64(ready_at_ms)? as i64
    );

    // After success the reaction immediately shrinks its dependency set back to the deadline.
    // A source change during the next cooldown again does not wake it.
    let after_first_transfer = evaluations.load(Ordering::Relaxed);
    graph.schedule_atom(source_inventory, SimTime::from_millis(500), 13)?;
    graph.advance_to(SimTime::from_millis(500))?;
    println!(
        "source_change_woke_during_cooldown={}",
        evaluations.load(Ordering::Relaxed) != after_first_transfer
    );

    // At expiry there is no valid transfer yet, so inventory snapshots become dependencies. A
    // later destination change then wakes the idle hopper immediately and completes transaction 2.
    graph.advance_to(SimTime::from_millis(800))?;
    let idle_evaluations = evaluations.load(Ordering::Relaxed);
    graph.schedule_atom(destination_inventory, SimTime::from_millis(850), 23)?;
    graph.advance_to(SimTime::from_millis(850))?;
    println!(
        "idle_inventory_change_woke={}",
        evaluations.load(Ordering::Relaxed) > idle_evaluations
    );
    println!(
        "second_transfer_source={}",
        graph.value_atom(source_inventory)?
    );
    println!(
        "second_transfer_destination={}",
        graph.value_atom(destination_inventory)?
    );
    println!(
        "second_next_ready_ms={}",
        graph.value_f64(ready_at_ms)? as i64
    );
    println!(
        "reaction_evaluations={}",
        evaluations.load(Ordering::Relaxed)
    );
    println!("legacy_tick_callbacks_to_850ms={}", 850 / TICK_MS);
    Ok(())
}

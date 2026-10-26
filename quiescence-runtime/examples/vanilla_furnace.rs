use quiescence_core::{Quantization, SimDuration, SimTime};
use quiescence_runtime::{Graph, ReactionOutput, RuntimeConfig, StateWrite};

const TICK_MS: i64 = 50;
const COOK_TICKS: i64 = 200;
const COAL_BURN_TICKS: i64 = 1_600;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let q = Quantization::new(0, 0)?;
    let mut graph = Graph::new(RuntimeConfig::default());

    let input_count = graph.add_input("furnace.input_count", q, 3.0)?;
    let fuel_count = graph.add_input("furnace.fuel_count", q, 1.0)?;
    let output_count = graph.add_input("furnace.output_count", q, 0.0)?;
    let burn_until_ms = graph.add_input("furnace.burn_until_ms", q, 0.0)?;
    let cook_until_ms = graph.add_input("furnace.cook_until_ms", q, 0.0)?;

    let _furnace = graph.add_reaction("vanilla furnace", move |context| {
        let now_ms = context.now().as_nanos() / 1_000_000;
        let inputs = context.read_f64(input_count)? as i64;
        let fuel = context.read_f64(fuel_count)? as i64;
        let outputs = context.read_f64(output_count)? as i64;
        let burn_until = context.read_f64(burn_until_ms)? as i64;
        let cook_until = context.read_f64(cook_until_ms)? as i64;
        let can_smelt = inputs > 0 && outputs < 64;

        if !can_smelt {
            if cook_until != 0 {
                return Ok(ReactionOutput::immediate(vec![StateWrite::new(
                    cook_until_ms,
                    0.0,
                )]));
            }
            if burn_until > now_ms {
                return Ok(ReactionOutput::after(
                    SimDuration::from_millis(burn_until - now_ms),
                    vec![StateWrite::new(burn_until_ms, 0.0)],
                ));
            }
            return Ok(ReactionOutput::idle());
        }

        if burn_until <= now_ms {
            if fuel > 0 {
                return Ok(ReactionOutput::immediate(vec![
                    StateWrite::new(fuel_count, (fuel - 1) as f64),
                    StateWrite::new(burn_until_ms, (now_ms + COAL_BURN_TICKS * TICK_MS) as f64),
                ]));
            }
            if cook_until != 0 {
                return Ok(ReactionOutput::immediate(vec![StateWrite::new(
                    cook_until_ms,
                    0.0,
                )]));
            }
            return Ok(ReactionOutput::idle());
        }

        if cook_until == 0 {
            return Ok(ReactionOutput::immediate(vec![StateWrite::new(
                cook_until_ms,
                (now_ms + COOK_TICKS * TICK_MS) as f64,
            )]));
        }

        if cook_until <= burn_until {
            return Ok(ReactionOutput::after(
                SimDuration::from_millis(cook_until - now_ms),
                vec![
                    StateWrite::new(input_count, (inputs - 1) as f64),
                    StateWrite::new(output_count, (outputs + 1) as f64),
                    StateWrite::new(cook_until_ms, 0.0),
                ],
            ));
        }

        Ok(ReactionOutput::after(
            SimDuration::from_millis(burn_until - now_ms),
            vec![StateWrite::new(burn_until_ms, 0.0)],
        ))
    })?;

    graph.settle()?;
    let horizon = SimTime::from_secs(80);
    let mut causal_events = 0_u64;
    while let Some(next) = graph.next_event_time() {
        if next > horizon {
            break;
        }
        graph.advance_to(next)?;
        causal_events = causal_events.saturating_add(1);
    }
    graph.advance_to(horizon)?;

    println!("input={}", graph.value_f64(input_count)? as i64);
    println!("fuel={}", graph.value_f64(fuel_count)? as i64);
    println!("output={}", graph.value_f64(output_count)? as i64);
    println!("causal_events={causal_events}");
    println!("legacy_tick_callbacks={}", horizon.as_nanos() / 50_000_000);
    Ok(())
}

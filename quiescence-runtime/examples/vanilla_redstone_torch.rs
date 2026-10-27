use quiescence_core::{Quantization, SimDuration, SimTime};
use quiescence_runtime::{Graph, ReactionOutput, RuntimeConfig, StateWrite};

const TICK_MS: i64 = 50;
const BURNOUT_WINDOW_TICKS: i64 = 60;
const BURNOUT_TOGGLES: usize = 8;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let q = Quantization::new(0, 0)?;
    let mut graph = Graph::new(RuntimeConfig::default());
    let history = graph.add_event_window(
        "redstone torch toggles",
        SimDuration::from_millis(BURNOUT_WINDOW_TICKS * TICK_MS),
    )?;
    let burned_out = graph.add_input("torch.burned_out", q, 0.0)?;

    // A real adapter interns the source key `(x, y, z)` to a stable canonical handle.
    let torch_position = 0x1234_u64;
    let callback = graph.add_reaction("redstone torch callback", move |context| {
        if context.now() == SimTime::ZERO {
            return Ok(ReactionOutput::idle());
        }
        let count_after_toggle = context
            .window_count(history, torch_position)?
            .saturating_add(1);
        let writes = if count_after_toggle >= BURNOUT_TOGGLES {
            vec![StateWrite::new(burned_out, 1.0)]
        } else {
            Vec::new()
        };
        Ok(ReactionOutput::immediate(writes).and_record_window(history, torch_position))
    })?;
    graph.settle()?;

    for tick in 1..=BURNOUT_TOGGLES {
        graph.schedule_reaction_event(
            callback,
            SimTime::from_millis(i64::try_from(tick)? * TICK_MS),
            0,
        )?;
    }
    graph.advance_to(SimTime::from_millis(
        i64::try_from(BURNOUT_TOGGLES)? * TICK_MS,
    ))?;
    println!(
        "toggle_count={}",
        graph.event_window_count(history, torch_position)?
    );
    println!("burned_out={}", graph.value_f64(burned_out)? as i64);
    println!("expiry_event={:?}", graph.next_event_time());

    graph.advance_to(SimTime::from_millis(
        (i64::try_from(BURNOUT_TOGGLES)? + BURNOUT_WINDOW_TICKS + 1) * TICK_MS,
    ))?;
    println!(
        "toggle_count_after_window={}",
        graph.event_window_count(history, torch_position)?
    );
    Ok(())
}

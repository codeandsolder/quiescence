#![forbid(unsafe_code)]

use quiescence_core::{Quantization, SimDuration, SimTime};
use quiescence_runtime::{EvalOutput, Graph, RuntimeConfig};
use std::error::Error;

fn main() -> Result<(), Box<dyn Error>> {
    let mut g = Graph::new(RuntimeConfig::default());
    let bit = Quantization::new(0, 0)?;
    let tenth = Quantization::new(1, 1)?;

    let charcoal = g.add_input("ae.charcoal_available", bit, 1.0)?;
    let water = g.add_input("boiler.water_present", bit, 1.0)?;
    let room = g.add_input("steam.output_has_room", bit, 1.0)?;
    let base_ms = g.add_input("controller.base_reaction_ms", tenth, 50.0)?;
    let damage = g.add_input("controller.damage_modifier", tenth, 1.0)?;

    let reaction = g.add_derived("controller.reaction_ms", tenth, 50.0, move |c| {
        Ok(EvalOutput::immediate(
            c.read_f64(base_ms)? * c.read_f64(damage)?,
        ))
    })?;
    let can_fire = g.add_derived("boiler.can_fire", bit, 1.0, move |c| {
        Ok(EvalOutput::immediate(f64::from(
            c.read_bool(charcoal)? && c.read_bool(water)? && c.read_bool(room)?,
        )))
    })?;
    let burner = g.add_derived("boiler.burner", bit, 1.0, move |c| {
        let delay = SimDuration::from_millis(c.read_f64(reaction)?.round() as i64);
        Ok(EvalOutput::after(f64::from(c.read_bool(can_fire)?), delay))
    })?;

    g.settle()?;
    let _ = g.take_transitions();
    g.schedule_input(water, SimTime::from_secs(5), 0.0)?;
    g.schedule_input(damage, SimTime::from_secs(7), 1.5)?;
    g.schedule_input(water, SimTime::from_secs(8), 1.0)?;

    for ms in [4_000, 5_000, 5_049, 5_050, 7_000, 8_000, 8_074, 8_075] {
        g.advance_to(SimTime::from_millis(ms))?;
        println!(
            "t={ms:>5} ms | water={} | reaction={:>4.1} ms | can_fire={} | burner={}",
            g.value_f64(water)?,
            g.value_f64(reaction)?,
            g.value_f64(can_fire)?,
            g.value_f64(burner)?
        );
        for t in g.take_transitions() {
            println!(
                "             @ {:>10} ns  {:<28} {} -> {}",
                t.at.as_nanos(),
                t.node_name,
                t.old.raw(),
                t.new.raw()
            );
        }
    }
    Ok(())
}

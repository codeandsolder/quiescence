use super::{
    CanonicalValue, EvalContext, EvalError, EvalOutput, Graph, InteractionIngressError,
    ReactionOutput, RuntimeConfig, RuntimeError, StateWrite, interaction_channel,
};
use quiescence_core::{Bucket, Quantization, QuantizeError, ReactionId, SimDuration, SimTime};
use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
    thread,
};

fn q0() -> Result<Quantization, Box<dyn std::error::Error>> {
    Ok(Quantization::new(0, 0)?)
}

fn recording_callback<T: Send + 'static>(
    graph: &mut Graph,
    name: &str,
    recorded: &Arc<Mutex<Vec<T>>>,
    observe: impl Fn(&EvalContext<'_>) -> T + Send + Sync + 'static,
) -> Result<ReactionId, RuntimeError> {
    let captured = Arc::clone(recorded);
    graph.add_event_callback(name, move |context| {
        captured
            .lock()
            .map_err(|_| EvalError::Message(String::from("capture lock poisoned")))?
            .push(observe(context));
        Ok(ReactionOutput::idle())
    })
}

#[test]
fn failed_node_evaluation_stays_dirty_until_retry() -> Result<(), Box<dyn std::error::Error>> {
    let mut g = Graph::new(RuntimeConfig::default());
    let source = g.add_input("source", q0()?, 1.0)?;
    let attempts = Arc::new(AtomicUsize::new(0));
    let count = Arc::clone(&attempts);
    let derived = g.add_derived("fallible", q0()?, 0.0, move |context| {
        let value = context.read_f64(source)?;
        if count.fetch_add(1, Ordering::Relaxed) == 0 {
            return Err(EvalError::Message(String::from(
                "temporary evaluation error",
            )));
        }
        Ok(EvalOutput::immediate(value))
    })?;
    assert!(matches!(g.settle(), Err(RuntimeError::Evaluation { .. })));
    assert!(!g.is_quiescent());
    assert_eq!(g.value_bucket(derived)?, Bucket::new(0));
    g.set_input_now(source, 2.0)?;
    assert_eq!(g.value_bucket(derived)?, Bucket::new(2));
    assert!(g.is_quiescent());
    assert_eq!(attempts.load(Ordering::Relaxed), 2);
    Ok(())
}

#[test]
fn failed_reaction_keeps_previous_subscriptions_until_success()
-> Result<(), Box<dyn std::error::Error>> {
    let mut g = Graph::new(RuntimeConfig::default());
    let gate = g.add_input("gate", q0()?, 0.0)?;
    let previous = g.add_input("previous", q0()?, 1.0)?;
    let replacement = g.add_input("replacement", q0()?, 2.0)?;
    let retry_allowed = Arc::new(AtomicUsize::new(0));
    let allow_retry = Arc::clone(&retry_allowed);
    let reaction = g.add_reaction("dynamic subscriptions", move |context| {
        if context.read_bool(gate)? {
            context.read_bucket(replacement)?;
            if allow_retry.load(Ordering::Relaxed) == 0 {
                return Ok(ReactionOutput::schedule_reaction_once_after(
                    quiescence_core::ReactionId::from_index(u32::MAX),
                    SimDuration::ZERO,
                    0,
                    0,
                ));
            }
        } else {
            context.read_bucket(previous)?;
        }
        Ok(ReactionOutput::idle())
    })?;
    g.settle()?;
    let before = &g.reactions[reaction.index()].dependencies;
    assert!(before.contains(gate));
    assert!(before.contains(previous));
    assert!(!before.contains(replacement));

    assert!(matches!(
        g.set_input_now(gate, 1.0),
        Err(RuntimeError::UnknownReaction(_))
    ));
    let still_previous = &g.reactions[reaction.index()].dependencies;
    assert!(still_previous.contains(previous));
    assert!(!still_previous.contains(replacement));
    assert!(!g.is_quiescent());

    retry_allowed.store(1, Ordering::Relaxed);
    g.settle()?;
    let after = &g.reactions[reaction.index()].dependencies;
    assert!(after.contains(gate));
    assert!(!after.contains(previous));
    assert!(after.contains(replacement));
    Ok(())
}

#[test]
fn invalid_reaction_output_is_retryable_without_partial_effects()
-> Result<(), Box<dyn std::error::Error>> {
    let mut g = Graph::new(RuntimeConfig::default());
    let sink = g.add_input("sink", q0()?, 0.0)?;
    let _reaction = g.add_reaction("bad callback target", move |_| {
        Ok(ReactionOutput::immediate(vec![StateWrite::new(sink, 8.0)])
            .and_schedule_reaction_once_after(
                quiescence_core::ReactionId::from_index(u32::MAX),
                SimDuration::ZERO,
                13,
                0,
            ))
    })?;
    for _ in 0..2 {
        assert!(matches!(g.settle(), Err(RuntimeError::UnknownReaction(_))));
        assert!(!g.is_quiescent());
        assert_eq!(g.value_bucket(sink)?, Bucket::new(0));
        assert_eq!(g.take_transitions().len(), 0);
    }
    Ok(())
}

#[test]
fn event_callback_retry_preserves_external_token() -> Result<(), Box<dyn std::error::Error>> {
    let mut g = Graph::new(RuntimeConfig::default());
    let tokens = Arc::new(Mutex::new(Vec::new()));
    let observed = Arc::clone(&tokens);
    let callback = g.add_event_callback("temporarily failing event", move |context| {
        let mut recorded = observed
            .lock()
            .map_err(|_| EvalError::Message(String::from("token lock poisoned")))?;
        recorded.push(context.external_token());
        if recorded.len() == 1 {
            return Err(EvalError::Message(String::from("temporary callback error")));
        }
        drop(recorded);
        Ok(ReactionOutput::idle())
    })?;
    let at = SimTime::from_millis(12);
    g.schedule_reaction_event_with_token(callback, at, 0, 0xdead)?;
    assert!(matches!(
        g.advance_to(at),
        Err(RuntimeError::ReactionEvaluation { .. })
    ));
    assert!(!g.is_quiescent());
    g.advance_to(at)?;
    assert_eq!(
        *tokens
            .lock()
            .map_err(|_| String::from("token lock poisoned"))?,
        vec![Some(0xdead), Some(0xdead)]
    );
    assert!(g.is_quiescent());
    Ok(())
}

#[test]
fn scheduled_callback_retry_preserves_key() -> Result<(), Box<dyn std::error::Error>> {
    let mut g = Graph::new(RuntimeConfig::default());
    let keys = Arc::new(Mutex::new(Vec::new()));
    let observed = Arc::clone(&keys);
    let callback = g.add_event_callback("temporarily failing scheduled callback", move |c| {
        let mut recorded = observed
            .lock()
            .map_err(|_| EvalError::Message(String::from("key lock poisoned")))?;
        recorded.push(c.scheduled_key());
        if recorded.len() == 1 {
            return Err(EvalError::Message(String::from(
                "temporary scheduled error",
            )));
        }
        drop(recorded);
        Ok(ReactionOutput::idle())
    })?;
    let _scheduler = g.add_reaction("schedule once", move |_| {
        Ok(ReactionOutput::schedule_reaction_once_after(
            callback,
            SimDuration::from_millis(5),
            71,
            0,
        ))
    })?;
    g.settle()?;
    let at = SimTime::from_millis(5);
    assert!(matches!(
        g.advance_to(at),
        Err(RuntimeError::ReactionEvaluation { .. })
    ));
    assert!(!g.is_quiescent());
    g.advance_to(at)?;
    assert_eq!(
        *keys.lock().map_err(|_| String::from("key lock poisoned"))?,
        vec![Some(71), Some(71)]
    );
    Ok(())
}

#[test]
fn newly_scheduled_higher_priority_callback_preempts_existing_event()
-> Result<(), Box<dyn std::error::Error>> {
    let mut graph = Graph::new(RuntimeConfig::default());
    let seen = Arc::new(Mutex::new(Vec::new()));
    let target = recording_callback(&mut graph, "target", &seen, |context| {
        (context.scheduled_key(), context.external_token())
    })?;
    let producer = graph.add_event_callback("producer", move |_| {
        Ok(ReactionOutput::schedule_reaction_once_after(
            target,
            SimDuration::ZERO,
            55,
            -5,
        ))
    })?;
    graph.schedule_reaction_event(producer, SimTime::ZERO, 0)?;
    graph.schedule_reaction_event_with_token(target, SimTime::ZERO, 10, 9)?;
    graph.advance_to(SimTime::ZERO)?;
    assert_eq!(
        *seen
            .lock()
            .map_err(|_| String::from("capture lock poisoned"))?,
        vec![(Some(55), None), (None, Some(9))]
    );
    Ok(())
}

#[test]
fn out_of_range_scheduled_write_is_rejected_before_admission()
-> Result<(), Box<dyn std::error::Error>> {
    let mut g = Graph::new(RuntimeConfig::default());
    let node = g.add_input("node", q0()?, 0.0)?;
    let at = SimTime::from_millis(8);
    assert!(matches!(
        g.schedule_input(node, at, 1e100),
        Err(RuntimeError::Quantize(QuantizeError::OutOfRange))
    ));
    assert_eq!(g.next_event_time(), None);
    g.schedule_input(node, at, 2.0)?;
    g.advance_to(at)?;
    assert_eq!(g.value_bucket(node)?, Bucket::new(2));
    assert!(g.is_quiescent());
    Ok(())
}

#[test]
fn out_of_range_ingress_batch_does_not_block_valid_work() -> Result<(), Box<dyn std::error::Error>>
{
    let mut g = Graph::new(RuntimeConfig::default());
    let node = g.add_input("node", q0()?, 0.0)?;
    let atom = g.add_atom_input("atom", 0)?;
    let (sender, inbox) = interaction_channel();
    sender.state_now(
        0,
        vec![StateWrite::atom(atom, 1), StateWrite::new(node, 1e100)],
    )?;
    sender.state_now(0, vec![StateWrite::new(node, 2.0)])?;
    let result = g.process_interactions(&inbox)?;
    assert_eq!(result.accepted, 1);
    assert_eq!(result.rejected.len(), 1);
    assert!(matches!(
        result.rejected[0].error,
        RuntimeError::Quantize(QuantizeError::OutOfRange)
    ));
    assert_eq!(g.value_atom(atom)?, 0);
    assert_eq!(g.value_bucket(node)?, Bucket::new(2));
    assert!(g.is_quiescent());
    Ok(())
}

#[test]
fn budget_exhaustion_preserves_pending_work_for_retry() -> Result<(), Box<dyn std::error::Error>> {
    let mut g = Graph::new(RuntimeConfig {
        same_time_microstep_budget: 1,
    });
    let first = g.add_derived("first", q0()?, 0.0, |_| Ok(EvalOutput::immediate(1.0)))?;
    let second = g.add_derived("second", q0()?, 0.0, |_| Ok(EvalOutput::immediate(2.0)))?;
    assert!(matches!(
        g.settle(),
        Err(RuntimeError::CalculationInstability { .. })
    ));
    assert!(!g.is_quiescent());
    g.set_microstep_budget(2);
    g.settle()?;
    assert!(g.is_quiescent());
    assert_eq!(g.value_bucket(first)?, Bucket::new(1));
    assert_eq!(g.value_bucket(second)?, Bucket::new(2));
    Ok(())
}

#[test]
fn zero_delay_scheduled_callbacks_respect_same_time_budget()
-> Result<(), Box<dyn std::error::Error>> {
    let mut g = Graph::new(RuntimeConfig {
        same_time_microstep_budget: 32,
    });
    let counter = Arc::new(AtomicU64::new(0));
    let callback_counter = Arc::clone(&counter);
    let callback = g.add_event_callback("self-reschedule", move |_| {
        let key = callback_counter.fetch_add(1, Ordering::Relaxed);
        Ok(ReactionOutput::schedule_once_after(
            SimDuration::ZERO,
            key,
            0,
        ))
    })?;
    g.schedule_reaction_event(callback, SimTime::ZERO, 0)?;
    assert!(matches!(
        g.advance_to(SimTime::ZERO),
        Err(RuntimeError::CalculationInstability { .. })
    ));
    assert!(!g.is_quiescent());
    assert!(counter.load(Ordering::Relaxed) <= 32);
    Ok(())
}

#[test]
fn scheduled_writes_validate_target_before_entering_queue() -> Result<(), Box<dyn std::error::Error>>
{
    let mut g = Graph::new(RuntimeConfig::default());
    let atom = g.add_atom_input("atom", 5)?;
    let numeric = g.add_input("numeric", q0()?, 1.0)?;
    let derived = g.add_derived("derived", q0()?, 1.0, |_| Ok(EvalOutput::immediate(1.0)))?;
    assert!(matches!(
        g.schedule_input(atom, SimTime::ZERO, 1.0),
        Err(RuntimeError::ExpectedNumeric(_))
    ));
    assert!(matches!(
        g.schedule_atom(numeric, SimTime::ZERO, 4),
        Err(RuntimeError::ExpectedAtom(_))
    ));
    assert!(matches!(
        g.schedule_input(derived, SimTime::ZERO, 1.0),
        Err(RuntimeError::NotAnInput(_))
    ));
    assert!(matches!(
        g.schedule_input(numeric, SimTime::ZERO, f64::NAN),
        Err(RuntimeError::Quantize(_))
    ));
    assert_eq!(g.next_event_time(), None);
    Ok(())
}

#[test]
fn invalid_callback_target_does_not_commit_reaction_writes()
-> Result<(), Box<dyn std::error::Error>> {
    let mut g = Graph::new(RuntimeConfig::default());
    let output = g.add_input("output", q0()?, 0.0)?;
    let _reaction = g.add_reaction("invalid callback target", move |_| {
        Ok(
            ReactionOutput::immediate(vec![StateWrite::new(output, 5.0)])
                .and_schedule_reaction_once_after(
                    quiescence_core::ReactionId::from_index(999),
                    SimDuration::ZERO,
                    0,
                    0,
                ),
        )
    })?;
    assert!(matches!(g.settle(), Err(RuntimeError::UnknownReaction(_))));
    assert_eq!(g.value_bucket(output)?, Bucket::new(0));
    Ok(())
}

#[test]
fn unchanged_bucket_cuts_off_propagation() -> Result<(), Box<dyn std::error::Error>> {
    let mut g = Graph::new(RuntimeConfig::default());
    let source = g.add_input("source", Quantization::new(2, 0)?, 1.00)?;
    let middle = g.add_derived("middle", q0()?, 1.0, move |c| {
        Ok(EvalOutput::immediate(c.read_f64(source)?))
    })?;
    let calls = Arc::new(AtomicUsize::new(0));
    let calls2 = Arc::clone(&calls);
    let _sink = g.add_derived("sink", q0()?, 1.0, move |c| {
        calls2.fetch_add(1, Ordering::Relaxed);
        Ok(EvalOutput::immediate(c.read_f64(middle)?))
    })?;
    g.settle()?;
    let before = calls.load(Ordering::Relaxed);
    g.set_input_now(source, 1.04)?;
    assert_eq!(g.value_f64(middle)?, 1.0);
    assert_eq!(calls.load(Ordering::Relaxed), before);
    Ok(())
}

#[test]
fn delay_is_just_another_node() -> Result<(), Box<dyn std::error::Error>> {
    let mut g = Graph::new(RuntimeConfig::default());
    let command = g.add_input("command", q0()?, 0.0)?;
    let reaction = g.add_input("reaction_ms", q0()?, 50.0)?;
    let out = g.add_derived("out", q0()?, 0.0, move |c| {
        Ok(EvalOutput::after(
            c.read_f64(command)?,
            SimDuration::from_millis(c.read_bucket(reaction)?.raw()),
        ))
    })?;
    g.settle()?;
    g.set_input_now(command, 1.0)?;
    g.advance_to(SimTime::from_millis(49))?;
    assert_eq!(g.value_f64(out)?, 0.0);
    g.advance_to(SimTime::from_millis(50))?;
    assert_eq!(g.value_f64(out)?, 1.0);
    Ok(())
}

#[test]
fn dynamic_dependencies_cull_irrelevant_branch() -> Result<(), Box<dyn std::error::Error>> {
    let mut g = Graph::new(RuntimeConfig::default());
    let enabled = g.add_input("enabled", q0()?, 0.0)?;
    let source = g.add_input("source", q0()?, 10.0)?;
    let calls = Arc::new(AtomicUsize::new(0));
    let calls2 = Arc::clone(&calls);
    let _gated = g.add_derived("gated", q0()?, 0.0, move |c| {
        calls2.fetch_add(1, Ordering::Relaxed);
        if c.read_bool(enabled)? {
            Ok(EvalOutput::immediate(c.read_f64(source)?))
        } else {
            Ok(EvalOutput::immediate(0.0))
        }
    })?;
    g.settle()?;
    let before = calls.load(Ordering::Relaxed);
    g.set_input_now(source, 11.0)?;
    assert_eq!(calls.load(Ordering::Relaxed), before);
    g.set_input_now(enabled, 1.0)?;
    assert!(calls.load(Ordering::Relaxed) > before);
    Ok(())
}

#[test]
fn reaction_forecast_is_invalidated_when_a_dependency_changes()
-> Result<(), Box<dyn std::error::Error>> {
    let mut g = Graph::new(RuntimeConfig::default());
    let enabled = g.add_input("enabled", q0()?, 0.0)?;
    let output = g.add_input("output", q0()?, 0.0)?;
    let reaction = g.add_reaction("delayed output", move |c| {
        if c.read_bool(enabled)? {
            Ok(ReactionOutput::after(
                SimDuration::from_millis(10),
                vec![StateWrite::new(output, 1.0)],
            ))
        } else {
            Ok(ReactionOutput::idle())
        }
    })?;
    g.settle()?;
    assert_eq!(g.reaction_name(reaction)?, "delayed output");

    g.set_input_now(enabled, 1.0)?;
    assert_eq!(g.next_event_time(), Some(SimTime::from_millis(10)));
    g.advance_to(SimTime::from_millis(5))?;
    g.set_input_now(enabled, 0.0)?;
    g.advance_to(SimTime::from_millis(10))?;
    assert_eq!(g.value_f64(output)?, 0.0);
    Ok(())
}

#[test]
fn reaction_transition_applies_multiple_state_writes_before_dependents_run()
-> Result<(), Box<dyn std::error::Error>> {
    let mut g = Graph::new(RuntimeConfig::default());
    let trigger = g.add_input("trigger", q0()?, 0.0)?;
    let left = g.add_input("left", q0()?, 0.0)?;
    let right = g.add_input("right", q0()?, 0.0)?;
    let mismatches = Arc::new(AtomicUsize::new(0));
    let seen = Arc::clone(&mismatches);
    let _observer = g.add_derived("observer", q0()?, 0.0, move |c| {
        let left_value = c.read_f64(left)?;
        let right_value = c.read_f64(right)?;
        if c.read_bucket(left)? != c.read_bucket(right)? {
            seen.fetch_add(1, Ordering::Relaxed);
        }
        Ok(EvalOutput::immediate(left_value + right_value))
    })?;
    let _reaction = g.add_reaction("paired write", move |c| {
        if c.read_bool(trigger)? {
            Ok(ReactionOutput::after(
                SimDuration::from_millis(1),
                vec![StateWrite::new(left, 1.0), StateWrite::new(right, 1.0)],
            ))
        } else {
            Ok(ReactionOutput::idle())
        }
    })?;
    g.settle()?;
    g.set_input_now(trigger, 1.0)?;
    g.advance_to(SimTime::from_millis(1))?;
    assert_eq!(g.value_f64(left)?, 1.0);
    assert_eq!(g.value_f64(right)?, 1.0);
    assert_eq!(mismatches.load(Ordering::Relaxed), 0);
    Ok(())
}

#[test]
fn reaction_dependencies_are_dynamic() -> Result<(), Box<dyn std::error::Error>> {
    let mut g = Graph::new(RuntimeConfig::default());
    let enabled = g.add_input("enabled", q0()?, 0.0)?;
    let source = g.add_input("source", q0()?, 1.0)?;
    let sink = g.add_input("sink", q0()?, 0.0)?;
    let evaluations = Arc::new(AtomicUsize::new(0));
    let calls = Arc::clone(&evaluations);
    let _reaction = g.add_reaction("gated copy", move |c| {
        calls.fetch_add(1, Ordering::Relaxed);
        if !c.read_bool(enabled)? {
            return Ok(ReactionOutput::idle());
        }
        Ok(ReactionOutput::immediate(vec![StateWrite::new(
            sink,
            c.read_f64(source)?,
        )]))
    })?;
    g.settle()?;
    let before = evaluations.load(Ordering::Relaxed);
    g.set_input_now(source, 2.0)?;
    assert_eq!(evaluations.load(Ordering::Relaxed), before);
    g.set_input_now(enabled, 1.0)?;
    assert_eq!(g.value_f64(sink)?, 2.0);
    Ok(())
}

#[test]
fn atom_state_uses_exact_equality_and_dynamic_dependencies()
-> Result<(), Box<dyn std::error::Error>> {
    let mut g = Graph::new(RuntimeConfig::default());
    let snapshot = g.add_atom_input("inventory_snapshot", 10)?;
    let sink = g.add_atom_input("sink", 0)?;
    let evaluations = Arc::new(AtomicUsize::new(0));
    let calls = Arc::clone(&evaluations);
    let _reaction = g.add_reaction("observe snapshot", move |c| {
        calls.fetch_add(1, Ordering::Relaxed);
        let snapshot_value = c.read_atom(snapshot)?;
        Ok(ReactionOutput::immediate(vec![StateWrite::atom(
            sink,
            snapshot_value,
        )]))
    })?;
    g.settle()?;
    assert_eq!(g.value_atom(snapshot)?, 10);
    assert_eq!(g.value_atom(sink)?, 10);
    let before = evaluations.load(Ordering::Relaxed);

    g.set_atom_now(snapshot, 10)?;
    assert_eq!(evaluations.load(Ordering::Relaxed), before);
    g.set_atom_now(snapshot, 11)?;
    assert!(evaluations.load(Ordering::Relaxed) > before);
    assert_eq!(g.value_atom(sink)?, 11);
    Ok(())
}

#[test]
fn scheduled_atom_change_invalidates_reaction_forecast() -> Result<(), Box<dyn std::error::Error>> {
    let mut g = Graph::new(RuntimeConfig::default());
    let snapshot = g.add_atom_input("snapshot", 1)?;
    let output = g.add_input("output", q0()?, 0.0)?;
    let _reaction = g.add_reaction("snapshot forecast", move |c| {
        if c.read_atom(snapshot)? == 1 {
            Ok(ReactionOutput::after(
                SimDuration::from_millis(10),
                vec![StateWrite::new(output, 1.0)],
            ))
        } else {
            Ok(ReactionOutput::idle())
        }
    })?;
    g.settle()?;
    assert_eq!(g.next_event_time(), Some(SimTime::from_millis(10)));
    g.schedule_atom(snapshot, SimTime::from_millis(5), 2)?;
    g.advance_to(SimTime::from_millis(10))?;
    assert_eq!(g.value_atom(snapshot)?, 2);
    assert_eq!(g.value_f64(output)?, 0.0);
    Ok(())
}

#[test]
fn mixed_numeric_and_atom_reaction_writes_are_atomic() -> Result<(), Box<dyn std::error::Error>> {
    let mut g = Graph::new(RuntimeConfig::default());
    let trigger = g.add_input("trigger", q0()?, 0.0)?;
    let count = g.add_input("count", q0()?, 0.0)?;
    let snapshot = g.add_atom_input("snapshot", 100)?;
    let mismatches = Arc::new(AtomicUsize::new(0));
    let seen = Arc::clone(&mismatches);
    let _observer = g.add_derived("observer", q0()?, 0.0, move |c| {
        let count_value = u64::try_from(c.read_bucket(count)?.raw())
            .map_err(|_| EvalError::Message(String::from("count bucket is negative")))?;
        let snapshot_value = c.read_atom(snapshot)?;
        if snapshot_value != count_value + 100 {
            seen.fetch_add(1, Ordering::Relaxed);
        }
        Ok(EvalOutput::immediate(c.read_f64(count)?))
    })?;
    let _reaction = g.add_reaction("mixed transaction", move |c| {
        if c.read_bool(trigger)? {
            Ok(ReactionOutput::after(
                SimDuration::from_millis(1),
                vec![StateWrite::new(count, 1.0), StateWrite::atom(snapshot, 101)],
            ))
        } else {
            Ok(ReactionOutput::idle())
        }
    })?;
    g.settle()?;
    g.set_input_now(trigger, 1.0)?;
    g.advance_to(SimTime::from_millis(1))?;
    assert_eq!(g.value_f64(count)?, 1.0);
    assert_eq!(g.value_atom(snapshot)?, 101);
    assert_eq!(mismatches.load(Ordering::Relaxed), 0);
    Ok(())
}

#[test]
fn node_kinds_are_checked_at_runtime() -> Result<(), Box<dyn std::error::Error>> {
    let mut g = Graph::new(RuntimeConfig::default());
    let numeric = g.add_input("numeric", q0()?, 1.0)?;
    let atom = g.add_atom_input("atom", 7)?;
    assert!(matches!(g.value_atom(numeric), Err(RuntimeError::ExpectedAtom(id)) if id == numeric));
    assert!(matches!(g.value_f64(atom), Err(RuntimeError::ExpectedNumeric(id)) if id == atom));
    assert!(matches!(
        g.set_input_now(atom, 1.0),
        Err(RuntimeError::ExpectedNumeric(id)) if id == atom
    ));
    assert!(matches!(
        g.set_atom_now(numeric, 8),
        Err(RuntimeError::ExpectedAtom(id)) if id == numeric
    ));
    Ok(())
}

#[test]
fn lazy_event_window_counts_keyed_history_without_expiry_wakeups()
-> Result<(), Box<dyn std::error::Error>> {
    let mut g = Graph::new(RuntimeConfig::default());
    let history = g.add_event_window("toggle history", SimDuration::from_millis(60))?;
    let burned_out = g.add_input("burned out", q0()?, 0.0)?;
    let key = 0x00ab_cdef_u64;
    let reaction = g.add_reaction("window threshold", move |c| {
        if c.now() == SimTime::ZERO {
            return Ok(ReactionOutput::idle());
        }
        let next_count = c.window_count(history, key)?.saturating_add(1);
        let writes = if next_count >= 8 {
            vec![StateWrite::new(burned_out, 1.0)]
        } else {
            Vec::new()
        };
        Ok(ReactionOutput::immediate(writes).and_record_window(history, key))
    })?;
    g.settle()?;

    for millisecond in 1..=7 {
        g.schedule_reaction_event(reaction, SimTime::from_millis(millisecond), 0)?;
    }
    g.advance_to(SimTime::from_millis(7))?;
    assert_eq!(g.event_window_count(history, key)?, 7);
    assert_eq!(g.value_f64(burned_out)?, 0.0);

    g.schedule_reaction_event(reaction, SimTime::from_millis(8), 0)?;
    g.advance_to(SimTime::from_millis(8))?;
    assert_eq!(g.event_window_count(history, key)?, 8);
    assert_eq!(g.value_f64(burned_out)?, 1.0);
    assert_eq!(g.next_event_time(), None);

    g.advance_to(SimTime::from_millis(61))?;
    assert_eq!(g.event_window_count(history, key)?, 8);
    g.advance_to(SimTime::from_millis(62))?;
    assert_eq!(g.event_window_count(history, key)?, 7);
    g.advance_to(SimTime::from_millis(69))?;
    assert_eq!(g.event_window_count(history, key)?, 0);
    Ok(())
}

#[test]
fn interaction_ingress_crosses_threads_and_returns_quiescent_snapshot()
-> Result<(), Box<dyn std::error::Error>> {
    let mut g = Graph::new(RuntimeConfig::default());
    let left = g.add_atom_input("left", 0)?;
    let right = g.add_atom_input("right", 0)?;
    let mismatches = Arc::new(AtomicUsize::new(0));
    let seen_mismatch = Arc::clone(&mismatches);
    let _observer = g.add_derived("paired observer", q0()?, 0.0, move |context| {
        let left_value = context.read_atom(left)?;
        let right_value = context.read_atom(right)?;
        if left_value != right_value {
            seen_mismatch.fetch_add(1, Ordering::Relaxed);
        }
        Ok(EvalOutput::immediate(0.0))
    })?;
    let callbacks = Arc::new(Mutex::new(Vec::new()));
    let callback = recording_callback(&mut g, "background interaction", &callbacks, |context| {
        (context.now(), context.external_token())
    })?;
    g.quiesce()?;
    g.advance_to(SimTime::from_millis(25))?;
    let _baseline = g.publish_quiescent()?;
    let (sender, inbox) = interaction_channel();
    let producer = thread::spawn(move || -> Result<(), InteractionIngressError> {
        sender.state_now(
            -1,
            vec![StateWrite::atom(left, 1), StateWrite::atom(right, 1)],
        )?;
        sender.callback_now_with_token(callback, 0, 77)?;
        sender.callback_now_with_token(callback, 0, 77)?;
        Ok(())
    });
    producer
        .join()
        .map_err(|_| String::from("interaction producer thread panicked"))??;
    assert_eq!(inbox.pending_count()?, 3);
    let (processed, publication) = g.process_interactions_and_publish(&inbox)?;
    assert_eq!(processed.accepted, 3);
    assert_eq!(processed.rejected.len(), 0);
    assert_eq!(inbox.pending_count()?, 0);
    assert!(g.is_quiescent());
    assert_eq!(mismatches.load(Ordering::Relaxed), 0);
    assert_eq!(
        *callbacks
            .lock()
            .map_err(|_| String::from("callback capture lock poisoned"))?,
        vec![
            (SimTime::from_millis(25), Some(77)),
            (SimTime::from_millis(25), Some(77)),
        ]
    );
    assert_eq!(publication.snapshot.at, SimTime::from_millis(25));
    assert_eq!(publication.snapshot.next_event_time, None);
    assert_eq!(
        publication.snapshot.value(left),
        Some(CanonicalValue::Atom(1))
    );
    assert_eq!(
        publication.snapshot.value(right),
        Some(CanonicalValue::Atom(1))
    );
    assert_eq!(publication.snapshot.node_count(), 3);
    assert!(
        publication
            .transitions
            .iter()
            .any(|record| record.node == left)
    );
    assert!(
        publication
            .transitions
            .iter()
            .any(|record| record.node == right)
    );
    assert_eq!(g.publish_quiescent()?.transitions.len(), 0);
    Ok(())
}

#[test]
fn interaction_inbox_waits_without_polling_and_wakes_on_submit()
-> Result<(), Box<dyn std::error::Error>> {
    let (sender, inbox) = interaction_channel();
    let waiter = thread::spawn(move || inbox.wait_for_pending());
    sender.state_now(0, Vec::new())?;
    let pending = waiter
        .join()
        .map_err(|_| String::from("interaction waiter thread panicked"))??;
    assert_eq!(pending, 1);
    Ok(())
}

#[test]
fn last_sender_drop_wakes_blocked_inbox() -> Result<(), Box<dyn std::error::Error>> {
    let (sender, inbox) = interaction_channel();
    let cloned = sender.clone();
    let waiter = thread::spawn(move || inbox.wait_for_pending());
    drop(sender);
    drop(cloned);
    let result = waiter
        .join()
        .map_err(|_| String::from("interaction waiter thread panicked"))?;
    assert_eq!(result, Err(InteractionIngressError::Disconnected));
    Ok(())
}

#[test]
fn closed_producer_side_drains_existing_queue_before_shutdown()
-> Result<(), Box<dyn std::error::Error>> {
    let (sender, inbox) = interaction_channel();
    sender.state_now(0, Vec::new())?;
    drop(sender);
    assert_eq!(inbox.wait_for_pending()?, 1);
    let mut graph = Graph::new(RuntimeConfig::default());
    let result = graph.process_interactions(&inbox)?;
    assert_eq!(result.accepted, 1);
    assert_eq!(result.rejected.len(), 0);
    assert_eq!(inbox.pending_count()?, 0);
    assert_eq!(
        inbox.wait_for_pending(),
        Err(InteractionIngressError::Disconnected)
    );
    Ok(())
}

#[test]
fn evaluation_failure_after_import_keeps_rejected_items_visible()
-> Result<(), Box<dyn std::error::Error>> {
    let mut graph = Graph::new(RuntimeConfig::default());
    let input = graph.add_input("input", q0()?, 0.0)?;
    let _derived = graph.add_derived("fallible", q0()?, 0.0, move |context| {
        let value = context.read_f64(input)?;
        if value > 0.0 {
            return Err(EvalError::Message(String::from("transient failure")));
        }
        Ok(EvalOutput::immediate(value))
    })?;
    graph.quiesce()?;
    let (sender, inbox) = interaction_channel();
    sender.state_now(
        0,
        vec![StateWrite::new(input, 1.0), StateWrite::new(input, 2.0)],
    )?;
    sender.state_now(0, vec![StateWrite::new(input, 3.0)])?;
    let (import, source) = match graph.process_interactions(&inbox) {
        Err(RuntimeError::InteractionProcessing { import, source }) => (import, source),
        other => return Err(format!("expected import processing error: {other:?}").into()),
    };
    assert_eq!(import.accepted, 1);
    assert_eq!(import.rejected.len(), 1);
    assert!(matches!(
        import.rejected[0].error,
        RuntimeError::DuplicateStateWrite(id) if id == input
    ));
    assert!(matches!(*source, RuntimeError::Evaluation { .. }));
    assert_eq!(inbox.pending_count()?, 0);
    assert_eq!(graph.value_bucket(input)?, Bucket::new(3));
    assert!(!graph.is_quiescent());
    Ok(())
}

#[test]
fn past_interaction_rejected_without_blocking_current_input()
-> Result<(), Box<dyn std::error::Error>> {
    let mut graph = Graph::new(RuntimeConfig::default());
    let node = graph.add_atom_input("atom", 0)?;
    let now = SimTime::from_millis(20);
    graph.advance_to(now)?;
    let (sender, inbox) = interaction_channel();
    sender.state_at(SimTime::ZERO, 0, vec![StateWrite::atom(node, 3)])?;
    sender.state_now(0, vec![StateWrite::atom(node, 4)])?;
    let result = graph.process_interactions(&inbox)?;
    assert_eq!(result.accepted, 1);
    assert_eq!(result.rejected.len(), 1);
    assert!(matches!(
        result.rejected[0].error,
        RuntimeError::EventInPast { now: reported, requested } if reported == now && requested == SimTime::ZERO
    ));
    assert_eq!(graph.value_atom(node)?, 4);
    assert!(graph.is_quiescent());
    Ok(())
}

#[test]
fn concurrent_producers_all_deliver_before_last_sender_shutdown()
-> Result<(), Box<dyn std::error::Error>> {
    const PRODUCERS: u64 = 4;
    const PER_PRODUCER: u64 = 64;
    let mut graph = Graph::new(RuntimeConfig::default());
    let seen = Arc::new(Mutex::new(Vec::new()));
    let callback = recording_callback(&mut graph, "concurrent ingress", &seen, |context| {
        context.external_token()
    })?;
    graph.quiesce()?;
    let (sender, inbox) = interaction_channel();
    let mut handles = Vec::new();
    for id in 0..PRODUCERS {
        let producer = sender.clone();
        handles.push(thread::spawn(
            move || -> Result<(), InteractionIngressError> {
                for index in 0..PER_PRODUCER {
                    producer.callback_now_with_token(callback, 0, id * PER_PRODUCER + index)?;
                }
                Ok(())
            },
        ));
    }
    drop(sender);
    let mut accepted = 0;
    loop {
        match inbox.wait_for_pending() {
            Ok(_) => {
                let result = graph.process_interactions(&inbox)?;
                accepted += result.accepted;
                assert_eq!(result.rejected.len(), 0);
            }
            Err(InteractionIngressError::Disconnected) => break,
            Err(other) => return Err(Box::new(other)),
        }
    }
    for handle in handles {
        handle
            .join()
            .map_err(|_| String::from("interaction producer panicked"))??;
    }
    assert_eq!(accepted, usize::try_from(PRODUCERS * PER_PRODUCER)?);
    let mut tokens = seen
        .lock()
        .map_err(|_| String::from("capture lock poisoned"))?
        .clone();
    tokens.sort_unstable();
    let expected: Vec<_> = (0..PRODUCERS * PER_PRODUCER).map(Some).collect();
    assert_eq!(tokens, expected);
    assert!(graph.is_quiescent());
    Ok(())
}

#[test]
fn imported_now_interaction_is_not_quiescent_until_processed()
-> Result<(), Box<dyn std::error::Error>> {
    let mut g = Graph::new(RuntimeConfig::default());
    let calls = Arc::new(AtomicUsize::new(0));
    let observed = Arc::clone(&calls);
    let callback = g.add_event_callback("explicit quiescence", move |_| {
        observed.fetch_add(1, Ordering::Relaxed);
        Ok(ReactionOutput::idle())
    })?;
    g.quiesce()?;
    let (sender, inbox) = interaction_channel();
    sender.callback_now(callback, 0)?;
    let imported = g.import_interactions(&inbox)?;
    assert_eq!(imported.accepted, 1);
    assert_eq!(imported.rejected.len(), 0);
    assert!(!g.is_quiescent());
    assert!(matches!(
        g.quiescent_snapshot(),
        Err(RuntimeError::NotQuiescent { .. })
    ));
    assert_eq!(calls.load(Ordering::Relaxed), 0);
    g.quiesce()?;
    assert!(g.is_quiescent());
    assert_eq!(calls.load(Ordering::Relaxed), 1);
    Ok(())
}

#[test]
fn future_interactions_import_without_running_early() -> Result<(), Box<dyn std::error::Error>> {
    let mut g = Graph::new(RuntimeConfig::default());
    let tokens = Arc::new(Mutex::new(Vec::new()));
    let callback = recording_callback(&mut g, "future ingress", &tokens, |context| {
        context.external_token()
    })?;
    g.quiesce()?;
    let (sender, inbox) = interaction_channel();
    sender.callback_at_with_token(callback, SimTime::from_millis(40), 0, 9)?;
    let processed = g.process_interactions(&inbox)?;
    assert_eq!(processed.accepted, 1);
    assert_eq!(processed.rejected.len(), 0);
    assert!(g.is_quiescent());
    let snapshot = g.quiescent_snapshot()?;
    assert_eq!(snapshot.next_event_time, Some(SimTime::from_millis(40)));
    assert!(
        tokens
            .lock()
            .map_err(|_| String::from("future capture lock poisoned"))?
            .is_empty()
    );
    let publication = g.advance_to_and_publish(SimTime::from_millis(40))?;
    assert_eq!(publication.snapshot.at, SimTime::from_millis(40));
    assert_eq!(
        *tokens
            .lock()
            .map_err(|_| String::from("future capture lock poisoned"))?,
        vec![Some(9)]
    );
    Ok(())
}

#[test]
fn invalid_interaction_is_reported_without_blocking_valid_work()
-> Result<(), Box<dyn std::error::Error>> {
    let mut g = Graph::new(RuntimeConfig::default());
    let atom = g.add_atom_input("atom", 0)?;
    let seen = Arc::new(Mutex::new(Vec::new()));
    let callback = recording_callback(&mut g, "ordered callback", &seen, |context| {
        context.external_token()
    })?;
    g.quiesce()?;
    let (sender, inbox) = interaction_channel();
    sender.callback_now_with_token(callback, 0, 11)?;
    sender.state_now(
        0,
        vec![StateWrite::atom(atom, 1), StateWrite::atom(atom, 2)],
    )?;
    sender.callback_now_with_token(callback, 0, 12)?;
    sender.callback_now(quiescence_core::ReactionId::from_index(u32::MAX), 0)?;
    let result = g.process_interactions(&inbox)?;
    assert_eq!(result.accepted, 2);
    assert_eq!(result.rejected.len(), 2);
    assert!(matches!(
        result.rejected[0].error,
        RuntimeError::DuplicateStateWrite(id) if id == atom
    ));
    assert!(matches!(
        result.rejected[1].error,
        RuntimeError::UnknownReaction(_)
    ));
    assert!(matches!(
        &result.rejected[0].interaction,
        super::ExternalInteraction::StateBatch { writes, .. } if writes.len() == 2
    ));
    assert_eq!(inbox.pending_count()?, 0);
    assert_eq!(g.value_atom(atom)?, 0);
    assert_eq!(
        *seen.lock().map_err(|_| String::from("capture poisoned"))?,
        vec![Some(11), Some(12)]
    );
    assert!(g.is_quiescent());
    Ok(())
}

#[test]
fn external_reaction_event_tokens_are_delivered_per_duplicate()
-> Result<(), Box<dyn std::error::Error>> {
    let mut g = Graph::new(RuntimeConfig::default());
    let tokens = Arc::new(Mutex::new(Vec::new()));
    let callback = recording_callback(&mut g, "tokenized external callback", &tokens, |context| {
        context.external_token()
    })?;
    g.schedule_reaction_event_with_token(callback, SimTime::from_millis(10), 0, 11)?;
    g.schedule_reaction_event_with_token(callback, SimTime::from_millis(10), 0, 12)?;
    g.advance_to(SimTime::from_millis(10))?;
    assert_eq!(
        *tokens
            .lock()
            .map_err(|_| String::from("token capture lock poisoned"))?,
        vec![Some(11), Some(12)]
    );
    Ok(())
}

#[test]
fn external_reaction_events_preserve_duplicates() -> Result<(), Box<dyn std::error::Error>> {
    let mut g = Graph::new(RuntimeConfig::default());
    let evaluations = Arc::new(AtomicUsize::new(0));
    let calls = Arc::clone(&evaluations);
    let reaction = g.add_reaction("external callback", move |_| {
        calls.fetch_add(1, Ordering::Relaxed);
        Ok(ReactionOutput::idle())
    })?;
    g.settle()?;
    assert_eq!(evaluations.load(Ordering::Relaxed), 1);

    g.schedule_reaction_event(reaction, SimTime::from_millis(10), 0)?;
    g.schedule_reaction_event(reaction, SimTime::from_millis(10), 0)?;
    g.advance_to(SimTime::from_millis(10))?;
    assert_eq!(evaluations.load(Ordering::Relaxed), 3);
    Ok(())
}

#[test]
fn external_reaction_events_honor_priority_without_dedup() -> Result<(), Box<dyn std::error::Error>>
{
    let mut g = Graph::new(RuntimeConfig::default());
    let marker = g.add_input("marker", q0()?, 0.0)?;
    let normal = g.add_reaction("normal external", move |c| {
        if c.now() == SimTime::ZERO {
            Ok(ReactionOutput::idle())
        } else {
            Ok(ReactionOutput::immediate(vec![StateWrite::new(
                marker, 2.0,
            )]))
        }
    })?;
    let high = g.add_reaction("high external", move |c| {
        if c.now() == SimTime::ZERO {
            Ok(ReactionOutput::idle())
        } else {
            Ok(ReactionOutput::immediate(vec![StateWrite::new(
                marker, 1.0,
            )]))
        }
    })?;
    g.settle()?;
    g.schedule_reaction_event(normal, SimTime::from_millis(10), 0)?;
    g.schedule_reaction_event(high, SimTime::from_millis(10), -1)?;
    g.advance_to(SimTime::from_millis(10))?;

    let marker_values: Vec<_> = g
        .take_transitions()
        .into_iter()
        .filter(|transition| transition.node == marker)
        .map(|transition| transition.new)
        .collect();
    assert_eq!(
        marker_values,
        vec![
            CanonicalValue::Numeric(Bucket::new(1)),
            CanonicalValue::Numeric(Bucket::new(2)),
        ]
    );
    Ok(())
}

#[test]
fn keyed_scheduled_reaction_preserves_first_pending_request()
-> Result<(), Box<dyn std::error::Error>> {
    let mut g = Graph::new(RuntimeConfig::default());
    let trigger = g.add_input("trigger", q0()?, 1.0)?;
    let evaluations = Arc::new(AtomicUsize::new(0));
    let calls = Arc::clone(&evaluations);
    let _reaction = g.add_reaction("schedule once", move |c| {
        calls.fetch_add(1, Ordering::Relaxed);
        if c.read_bool(trigger)? {
            Ok(ReactionOutput::schedule_once_after(
                SimDuration::from_millis(10),
                7,
                0,
            ))
        } else {
            Ok(ReactionOutput::idle())
        }
    })?;
    g.settle()?;
    assert_eq!(g.next_event_time(), Some(SimTime::from_millis(10)));

    g.schedule_input(trigger, SimTime::from_millis(5), 2.0)?;
    g.advance_to(SimTime::from_millis(5))?;
    assert_eq!(evaluations.load(Ordering::Relaxed), 2);
    assert_eq!(g.next_event_time(), Some(SimTime::from_millis(10)));

    g.advance_to(SimTime::from_millis(10))?;
    assert_eq!(evaluations.load(Ordering::Relaxed), 3);
    assert_eq!(g.next_event_time(), Some(SimTime::from_millis(20)));
    Ok(())
}

#[test]
fn keyed_schedule_can_target_a_different_reaction() -> Result<(), Box<dyn std::error::Error>> {
    let mut g = Graph::new(RuntimeConfig::default());
    let callbacks = Arc::new(AtomicUsize::new(0));
    let observed_callbacks = Arc::clone(&callbacks);
    let observed_keys = Arc::new(Mutex::new(Vec::new()));
    let callback_keys = Arc::clone(&observed_keys);
    let callback = g.add_event_callback("scheduled callback target", move |context| {
        observed_callbacks.fetch_add(1, Ordering::Relaxed);
        callback_keys
            .lock()
            .map_err(|_| EvalError::Message(String::from("callback-key lock poisoned")))?
            .push(context.scheduled_key());
        Ok(ReactionOutput::idle())
    })?;
    let producer_calls = Arc::new(AtomicUsize::new(0));
    let observed_producer = Arc::clone(&producer_calls);
    let producer = g.add_reaction("schedule producer", move |_| {
        let call = observed_producer.fetch_add(1, Ordering::Relaxed);
        if call < 2 {
            Ok(ReactionOutput::schedule_reaction_once_after(
                callback,
                SimDuration::from_millis(10),
                7,
                -1,
            ))
        } else {
            Ok(ReactionOutput::idle())
        }
    })?;
    g.settle()?;
    g.schedule_reaction_event(producer, SimTime::from_millis(5), 0)?;
    g.advance_to(SimTime::from_millis(5))?;
    assert_eq!(callbacks.load(Ordering::Relaxed), 0);
    assert_eq!(g.next_event_time(), Some(SimTime::from_millis(10)));
    g.advance_to(SimTime::from_millis(9))?;
    assert_eq!(callbacks.load(Ordering::Relaxed), 0);
    g.advance_to(SimTime::from_millis(10))?;
    assert_eq!(callbacks.load(Ordering::Relaxed), 1);
    assert_eq!(
        *observed_keys
            .lock()
            .map_err(|_| String::from("callback-key lock poisoned"))?,
        vec![Some(7)]
    );
    Ok(())
}

#[test]
fn event_callback_is_dormant_and_does_not_subscribe_to_reads()
-> Result<(), Box<dyn std::error::Error>> {
    let mut g = Graph::new(RuntimeConfig::default());
    let observed = g.add_input("observed", q0()?, 1.0)?;
    let calls = Arc::new(AtomicUsize::new(0));
    let callback_calls = Arc::clone(&calls);
    let callback = g.add_event_callback("event-only callback", move |context| {
        context.read_bool(observed)?;
        callback_calls.fetch_add(1, Ordering::Relaxed);
        Ok(ReactionOutput::idle())
    })?;
    g.settle()?;
    assert_eq!(calls.load(Ordering::Relaxed), 0);

    g.schedule_reaction_event(callback, SimTime::from_millis(10), 0)?;
    g.advance_to(SimTime::from_millis(10))?;
    assert_eq!(calls.load(Ordering::Relaxed), 1);

    g.schedule_input(observed, SimTime::from_millis(20), 0.0)?;
    g.advance_to(SimTime::from_millis(20))?;
    assert_eq!(calls.load(Ordering::Relaxed), 1);
    Ok(())
}

#[test]
fn scheduled_reaction_priority_orders_same_time_callbacks() -> Result<(), Box<dyn std::error::Error>>
{
    let mut g = Graph::new(RuntimeConfig::default());
    let marker = g.add_input("marker", q0()?, 0.0)?;
    let _normal = g.add_reaction("normal priority", move |c| {
        if c.now() == SimTime::ZERO {
            Ok(ReactionOutput::schedule_once_after(
                SimDuration::from_millis(10),
                1,
                0,
            ))
        } else {
            Ok(ReactionOutput::immediate(vec![StateWrite::new(
                marker, 2.0,
            )]))
        }
    })?;
    let _high = g.add_reaction("high priority", move |c| {
        if c.now() == SimTime::ZERO {
            Ok(ReactionOutput::schedule_once_after(
                SimDuration::from_millis(10),
                1,
                -1,
            ))
        } else {
            Ok(ReactionOutput::immediate(vec![StateWrite::new(
                marker, 1.0,
            )]))
        }
    })?;
    g.settle()?;
    g.advance_to(SimTime::from_millis(10))?;
    assert_eq!(g.value_f64(marker)?, 2.0);
    let marker_values: Vec<_> = g
        .take_transitions()
        .into_iter()
        .filter(|transition| transition.node == marker)
        .map(|transition| transition.new)
        .collect();
    assert_eq!(
        marker_values,
        vec![
            CanonicalValue::Numeric(Bucket::new(1)),
            CanonicalValue::Numeric(Bucket::new(2)),
        ]
    );
    Ok(())
}

#[test]
fn delayed_reevaluation_defers_dynamic_atom_dependencies() -> Result<(), Box<dyn std::error::Error>>
{
    let mut g = Graph::new(RuntimeConfig::default());
    let ready_at_ms = g.add_input("ready_at_ms", q0()?, 10.0)?;
    let snapshot = g.add_atom_input("snapshot", 1)?;
    let observed = g.add_atom_input("observed", 0)?;
    let evaluations = Arc::new(AtomicUsize::new(0));
    let calls = Arc::clone(&evaluations);
    let _reaction = g.add_reaction("deadline-gated snapshot", move |c| {
        calls.fetch_add(1, Ordering::Relaxed);
        let now_ms = c.now().as_nanos() / 1_000_000;
        let ready_at = c.read_bucket(ready_at_ms)?.raw();
        if now_ms < ready_at {
            return Ok(ReactionOutput::reevaluate_after(SimDuration::from_millis(
                ready_at - now_ms,
            )));
        }
        Ok(ReactionOutput::immediate(vec![StateWrite::atom(
            observed,
            c.read_atom(snapshot)?,
        )]))
    })?;
    g.settle()?;
    assert_eq!(evaluations.load(Ordering::Relaxed), 1);

    g.schedule_atom(snapshot, SimTime::from_millis(5), 2)?;
    g.advance_to(SimTime::from_millis(5))?;
    assert_eq!(evaluations.load(Ordering::Relaxed), 1);
    assert_eq!(g.value_atom(observed)?, 0);

    g.advance_to(SimTime::from_millis(10))?;
    assert_eq!(g.value_atom(observed)?, 2);
    assert_eq!(evaluations.load(Ordering::Relaxed), 2);
    Ok(())
}

#[test]
fn zero_time_oscillation_is_explicit_instability() -> Result<(), Box<dyn std::error::Error>> {
    let mut g = Graph::new(RuntimeConfig {
        same_time_microstep_budget: 32,
    });
    let a = g.declare_derived("a", q0()?, 0.0)?;
    let b = g.declare_derived("b", q0()?, 0.0)?;
    g.set_evaluator(a, move |c| {
        Ok(EvalOutput::immediate(if c.read_bool(b)? {
            0.0
        } else {
            1.0
        }))
    })?;
    g.set_evaluator(b, move |c| {
        Ok(EvalOutput::immediate(if c.read_bool(a)? {
            1.0
        } else {
            0.0
        }))
    })?;
    assert!(matches!(
        g.settle(),
        Err(RuntimeError::CalculationInstability { .. })
    ));
    Ok(())
}

#[test]
fn future_inputs_run_in_time_order() -> Result<(), Box<dyn std::error::Error>> {
    let mut g = Graph::new(RuntimeConfig::default());
    let n = g.add_input("n", q0()?, 0.0)?;
    g.schedule_input(n, SimTime::from_secs(2), 2.0)?;
    g.schedule_input(n, SimTime::from_secs(1), 1.0)?;
    g.advance_to(SimTime::from_secs(1))?;
    assert_eq!(g.value_f64(n)?, 1.0);
    g.advance_to(SimTime::from_secs(2))?;
    assert_eq!(g.value_f64(n)?, 2.0);
    Ok(())
}

#[test]
fn mixed_callback_destinations_preserve_output_order() -> Result<(), Box<dyn std::error::Error>> {
    let mut graph = Graph::new(RuntimeConfig::default());
    let seen = Arc::new(Mutex::new(Vec::new()));
    let target = recording_callback(&mut graph, "target", &seen, |context| {
        (context.invocation(), context.now())
    })?;
    let capture = Arc::clone(&seen);
    let producer =
        graph.add_event_callback("producer", move |context| match context.invocation() {
            super::Invocation::External { token: None } => Ok(
                ReactionOutput::schedule_reaction_once_after(target, SimDuration::ZERO, 1, 0)
                    .and_schedule_once_after(SimDuration::ZERO, 2, 0)
                    .and_schedule_reaction_once_after(target, SimDuration::ZERO, 3, 0),
            ),
            invocation => {
                capture
                    .lock()
                    .map_err(|_| EvalError::Message(String::from("capture poisoned")))?
                    .push((invocation, context.now()));
                Ok(ReactionOutput::idle())
            }
        })?;
    graph.schedule_reaction_event(producer, SimTime::ZERO, 0)?;
    graph.advance_to(SimTime::ZERO)?;
    assert_eq!(
        *seen.lock().map_err(|_| "capture poisoned")?,
        [
            (super::Invocation::Scheduled { key: 1 }, SimTime::ZERO),
            (super::Invocation::Scheduled { key: 2 }, SimTime::ZERO),
            (super::Invocation::Scheduled { key: 3 }, SimTime::ZERO),
        ]
    );
    Ok(())
}

#[test]
fn explicit_and_implicit_self_requests_share_first_wins_deduplication()
-> Result<(), Box<dyn std::error::Error>> {
    let mut graph = Graph::new(RuntimeConfig::default());
    // First reaction has graph-local index zero.
    let callback = quiescence_core::ReactionId::from_index(0);
    let actual = graph.add_event_callback("self", move |context| {
        if matches!(context.invocation(), super::Invocation::External { .. }) {
            Ok(ReactionOutput::schedule_reaction_once_after(
                callback,
                SimDuration::from_millis(5),
                9,
                0,
            )
            .and_schedule_once_after(SimDuration::from_millis(1), 9, 0))
        } else {
            Ok(ReactionOutput::idle())
        }
    })?;
    assert_eq!(actual, callback);
    graph.schedule_reaction_event(callback, SimTime::ZERO, 0)?;
    graph.advance_to(SimTime::ZERO)?;
    assert_eq!(graph.next_event_time(), Some(SimTime::from_millis(5)));
    graph.advance_to(SimTime::from_millis(5))?;
    assert_eq!(graph.next_event_time(), None);
    Ok(())
}

#[test]
fn history_only_transition_and_invalid_batch_are_atomic() -> Result<(), Box<dyn std::error::Error>>
{
    let mut graph = Graph::new(RuntimeConfig::default());
    let history = graph.add_event_window("history", SimDuration::from_secs(1))?;
    let node = graph.add_input("value", q0()?, 0.0)?;
    let callback = graph.add_event_callback("record", move |_| {
        Ok(ReactionOutput::idle().and_record_window(history, 7))
    })?;
    graph.schedule_reaction_event(callback, SimTime::ZERO, 0)?;
    graph.advance_to(SimTime::ZERO)?;
    assert_eq!(graph.event_window_count(history, 7)?, 1);
    let invalid = graph.add_event_callback("invalid", move |_| {
        Ok(
            ReactionOutput::immediate(vec![StateWrite::new(node, 13.0), StateWrite::atom(node, 0)])
                .and_record_window(history, 7),
        )
    })?;
    graph.schedule_reaction_event(invalid, SimTime::ZERO, 0)?;
    assert!(matches!(
        graph.advance_to(SimTime::ZERO),
        Err(RuntimeError::DuplicateStateWrite(_))
    ));
    assert_eq!(graph.value_bucket(node)?, Bucket::new(0));
    assert_eq!(graph.event_window_count(history, 7)?, 1);
    assert_eq!(graph.take_transitions(), []);
    Ok(())
}

#[test]
fn overflowing_callback_deadline_rejects_entire_reaction_output()
-> Result<(), Box<dyn std::error::Error>> {
    let mut graph = Graph::new(RuntimeConfig::default());
    let node = graph.add_atom_input("state", 0)?;
    let callback = graph.add_event_callback("overflow", move |_| {
        Ok(
            ReactionOutput::immediate(vec![StateWrite::atom(node, 1)]).and_schedule_once_after(
                SimDuration::from_nanos(1),
                7,
                0,
            ),
        )
    })?;
    let limit = SimTime::from_nanos(i64::MAX);
    graph.schedule_reaction_event(callback, limit, 0)?;
    assert!(matches!(
        graph.advance_to(limit),
        Err(RuntimeError::TimeOverflow { .. })
    ));
    assert_eq!(graph.value_atom(node)?, 0);
    assert_eq!(graph.take_transitions(), []);
    assert_eq!(graph.next_event_time(), None);
    assert!(!graph.is_quiescent());
    Ok(())
}

#[test]
fn overflowing_validity_timer_does_not_commit_derived_value()
-> Result<(), Box<dyn std::error::Error>> {
    let mut graph = Graph::new(RuntimeConfig::default());
    let limit = SimTime::from_nanos(i64::MAX);
    graph.advance_to(limit)?;
    let node = graph.add_derived("overflow validity", q0()?, 0.0, |_| {
        Ok(EvalOutput::immediate(1.0).with_valid_for(SimDuration::from_nanos(1)))
    })?;
    assert!(matches!(
        graph.settle(),
        Err(RuntimeError::TimeOverflow { .. })
    ));
    assert_eq!(graph.value_bucket(node)?, Bucket::new(0));
    assert_eq!(graph.next_event_time(), None);
    Ok(())
}

#[test]
fn duplicate_callback_ignores_later_unrepresentable_deadline()
-> Result<(), Box<dyn std::error::Error>> {
    let mut graph = Graph::new(RuntimeConfig::default());
    let limit = SimTime::from_nanos(i64::MAX);
    let callback = graph.add_event_callback("duplicate", |context| {
        if matches!(context.invocation(), super::Invocation::External { .. }) {
            Ok(ReactionOutput::schedule_once_after(SimDuration::ZERO, 7, 0)
                .and_schedule_once_after(SimDuration::from_nanos(1), 7, 0))
        } else {
            Ok(ReactionOutput::idle())
        }
    })?;
    graph.schedule_reaction_event(callback, limit, 0)?;
    graph.advance_to(limit)?;
    assert!(graph.is_quiescent());
    assert_eq!(graph.next_event_time(), None);
    Ok(())
}

#[test]
fn exhausted_generation_cannot_revalidate_a_stale_forecast()
-> Result<(), Box<dyn std::error::Error>> {
    let mut graph = Graph::new(RuntimeConfig::default());
    let node = graph.add_derived("generation", q0()?, 0.0, |_| Ok(EvalOutput::immediate(1.0)))?;
    let owner = graph.derived_owner(node)?;
    graph.derived[owner.index()].generation = u64::MAX;
    assert!(
        matches!(graph.settle(), Err(RuntimeError::GenerationExhausted(super::WorkId::Node(id))) if id == node)
    );
    assert_eq!(graph.value_bucket(node)?, Bucket::new(0));
    assert_eq!(graph.take_transitions(), []);
    Ok(())
}

#[test]
fn shared_reverse_edges_preserve_node_before_reaction_order()
-> Result<(), Box<dyn std::error::Error>> {
    let mut graph = Graph::new(RuntimeConfig::default());
    let source = graph.add_atom_input("source", 0)?;
    let q = q0()?;
    let derived = graph.add_derived("derived", q, 0.0, move |context| {
        Ok(EvalOutput::immediate(f64::from(
            context.read_atom(source)? != 0,
        )))
    })?;
    let observed = Arc::new(Mutex::new(Vec::new()));
    let capture = Arc::clone(&observed);
    graph.add_reaction("observer", move |context| {
        capture
            .lock()
            .map_err(|_| EvalError::Message(String::from("capture poisoned")))?
            .push((context.read_atom(source)?, context.read_bucket(derived)?));
        Ok(ReactionOutput::idle())
    })?;
    graph.quiesce()?;
    graph.set_atom_now(source, 1)?;
    assert_eq!(
        *observed.lock().map_err(|_| "capture poisoned")?,
        [(0, Bucket::new(0)), (1, Bucket::new(1)),]
    );
    Ok(())
}

#[test]
fn transition_records_share_stable_name_storage() -> Result<(), Box<dyn std::error::Error>> {
    let mut graph = Graph::new(RuntimeConfig::default());
    let label: Arc<str> = Arc::from("shared name");
    let node = graph.add_atom_input(Arc::clone(&label), 0)?;
    graph.set_atom_now(node, 1)?;
    graph.set_atom_now(node, 2)?;
    let records = graph.take_transitions();
    drop(graph);
    assert_eq!(records.len(), 2);
    for record in records {
        assert!(Arc::ptr_eq(&label, &record.node_name));
    }
    Ok(())
}

#[test]
fn dynamic_wide_subscriptions_replace_only_after_success() -> Result<(), Box<dyn std::error::Error>>
{
    let mut graph = Graph::new(RuntimeConfig::default());
    let gate = graph.add_atom_input("gate", 0)?;
    let sources = (0..24)
        .map(|_| graph.add_atom_input("source", 0))
        .collect::<Result<Vec<_>, _>>()?;
    let captured = sources.clone();
    let calls = Arc::new(AtomicUsize::new(0));
    let can_run = Arc::new(AtomicUsize::new(1));
    let observed = Arc::clone(&calls);
    let allowed = Arc::clone(&can_run);
    let derived = graph.add_derived("wide", Quantization::INTEGER, 0.0, move |context| {
        let group = context.read_atom(gate)? % 2;
        observed.fetch_add(1, Ordering::Relaxed);
        for (index, &source) in captured.iter().enumerate().rev() {
            if (index as u64) % 2 == group {
                context.read_atom(source)?;
                context.read_atom(source)?;
            }
        }
        if allowed.load(Ordering::Relaxed) == 0 {
            return Err(EvalError::Message(String::from("retry")));
        }
        Ok(EvalOutput::immediate(0.0))
    })?;
    graph.settle()?;
    let owner = graph.derived_owner(derived)?;
    let dependencies = &graph.derived[owner.index()].dependencies;
    assert!(dependencies.contains(sources[0]));
    assert!(!dependencies.contains(sources[1]));
    can_run.store(0, Ordering::Relaxed);
    assert!(matches!(
        graph.set_atom_now(gate, 1),
        Err(RuntimeError::Evaluation { .. })
    ));
    let dependencies = &graph.derived[owner.index()].dependencies;
    assert!(dependencies.contains(sources[0]));
    assert!(!dependencies.contains(sources[1]));
    can_run.store(1, Ordering::Relaxed);
    graph.settle()?;
    let before = calls.load(Ordering::Relaxed);
    for source in sources.iter().step_by(2) {
        graph.set_atom_now(*source, 1)?;
    }
    assert_eq!(calls.load(Ordering::Relaxed), before);
    for source in sources.iter().skip(1).step_by(2) {
        graph.set_atom_now(*source, 1)?;
    }
    assert_eq!(calls.load(Ordering::Relaxed), before + 12);
    Ok(())
}

#[test]
fn obsolete_forecasts_do_not_accumulate_or_consume_settlement_budget()
-> Result<(), Box<dyn std::error::Error>> {
    let mut graph = Graph::new(RuntimeConfig {
        same_time_microstep_budget: 4,
    });
    let source = graph.add_input("source", Quantization::INTEGER, 0.0)?;
    let derived = graph.add_derived("delayed", Quantization::INTEGER, 0.0, move |context| {
        Ok(EvalOutput::after(
            context.read_f64(source)?,
            SimDuration::from_millis(10),
        ))
    })?;
    graph.settle()?;
    for value in 1..=10_000 {
        graph.set_input_now(source, f64::from(value))?;
        assert_eq!(graph.next_event_time(), Some(SimTime::from_millis(10)));
    }
    graph.advance_to(SimTime::from_millis(10))?;
    assert_eq!(graph.value_bucket(derived)?, Bucket::new(10_000));
    assert_eq!(graph.next_event_time(), None);
    Ok(())
}

#[test]
fn failed_evaluation_retains_previous_forecasts_and_retry_removes_them()
-> Result<(), Box<dyn std::error::Error>> {
    let mut graph = Graph::new(RuntimeConfig::default());
    let source = graph.add_atom_input("mode", 0)?;
    let ready = Arc::new(AtomicUsize::new(0));
    let gate = Arc::clone(&ready);
    let node = graph.add_derived("forecast", Quantization::INTEGER, 0.0, move |context| {
        if context.read_atom(source)? == 0 {
            return Ok(EvalOutput::after(1.0, SimDuration::from_millis(20))
                .with_valid_for(SimDuration::from_millis(10)));
        }
        if gate.load(Ordering::Relaxed) == 0 {
            return Err(EvalError::Message(String::from("retry")));
        }
        Ok(EvalOutput::immediate(0.0))
    })?;
    graph.settle()?;
    assert_eq!(graph.next_event_time(), Some(SimTime::from_millis(10)));
    assert!(matches!(
        graph.set_atom_now(source, 1),
        Err(RuntimeError::Evaluation { .. })
    ));
    assert_eq!(graph.next_event_time(), Some(SimTime::from_millis(10)));
    ready.store(1, Ordering::Relaxed);
    graph.settle()?;
    assert_eq!(graph.next_event_time(), None);
    graph.advance_to(SimTime::from_millis(20))?;
    assert_eq!(graph.value_bucket(node)?, Bucket::new(0));
    Ok(())
}

#[test]
fn cancelling_validity_and_commit_preserves_explicit_callbacks_and_writes()
-> Result<(), Box<dyn std::error::Error>> {
    let mut graph = Graph::new(RuntimeConfig::default());
    let source = graph.add_atom_input("source", 0)?;
    let callback_calls = Arc::new(AtomicUsize::new(0));
    let captured = Arc::clone(&callback_calls);
    let callback = graph.add_event_callback("external", move |_| {
        captured.fetch_add(1, Ordering::Relaxed);
        Ok(ReactionOutput::idle())
    })?;
    let node = graph.add_derived("forecast", Quantization::INTEGER, 0.0, move |context| {
        if context.read_atom(source)? == 0 {
            Ok(EvalOutput::after(1.0, SimDuration::from_millis(30))
                .with_valid_for(SimDuration::from_millis(10)))
        } else {
            Ok(EvalOutput::immediate(9.0))
        }
    })?;
    graph.settle()?;
    graph.schedule_reaction_event(callback, SimTime::from_millis(20), 0)?;
    graph.schedule_atom(source, SimTime::from_millis(5), 1)?;
    graph.advance_to(SimTime::from_millis(5))?;
    assert_eq!(graph.next_event_time(), Some(SimTime::from_millis(20)));
    assert_eq!(graph.value_bucket(node)?, Bucket::new(9));
    graph.advance_to(SimTime::from_millis(30))?;
    assert_eq!(callback_calls.load(Ordering::Relaxed), 1);
    assert_eq!(graph.value_bucket(node)?, Bucket::new(9));
    assert_eq!(graph.next_event_time(), None);
    Ok(())
}

#[test]
fn buried_obsolete_forecasts_are_bounded_in_grouped_and_sparse_queues()
-> Result<(), Box<dyn std::error::Error>> {
    for ordinary in [1_i64, 160] {
        let mut graph = Graph::new(RuntimeConfig {
            same_time_microstep_budget: 8,
        });
        let source = graph.add_input("source", Quantization::INTEGER, 0.0)?;
        let marker = graph.add_atom_input("marker", 0)?;
        let derived = graph.add_derived("delayed", Quantization::INTEGER, 0.0, move |context| {
            Ok(EvalOutput::after(
                context.read_f64(source)?,
                SimDuration::from_millis(10),
            ))
        })?;
        graph.settle()?;
        for at in 1..=ordinary {
            graph.schedule_atom(marker, SimTime::from_nanos(at), at.unsigned_abs())?;
        }
        for value in 1..=5000 {
            graph.set_input_now(source, f64::from(value))?;
            assert!(graph.stale_forecasts < 1024);
            assert!(graph.events.len() <= usize::try_from(ordinary)? + 1024);
            assert_eq!(graph.next_event_time(), Some(SimTime::from_nanos(1)));
        }
        graph.advance_to(SimTime::from_millis(10))?;
        assert_eq!(graph.stale_forecasts, 0);
        assert_eq!(graph.events.len(), 0);
        assert_eq!(graph.value_bucket(derived)?, Bucket::new(5000));
        assert_eq!(graph.value_atom(marker)?, ordinary.unsigned_abs());
        let owner = graph.derived_owner(derived)?;
        assert_eq!(graph.derived[owner.index()].pending_forecasts, 0);
    }
    Ok(())
}

#[test]
fn scheduled_writable_inputs_cannot_become_derived() -> Result<(), Box<dyn std::error::Error>> {
    let mut graph = Graph::new(RuntimeConfig::default());
    let numeric = graph.add_input("numeric", Quantization::INTEGER, 0.0)?;
    let atom = graph.add_atom_input("atom", 0)?;
    let at = SimTime::from_millis(10);
    graph.schedule_input(numeric, at, 7.0)?;
    graph.schedule_atom(atom, at, 9)?;
    let reaction = graph.add_reaction("forecast", move |context| {
        if context.now() == SimTime::ZERO {
            Ok(ReactionOutput::after(
                SimDuration::from_millis(5),
                vec![StateWrite::new(numeric, 3.0), StateWrite::atom(atom, 4)],
            ))
        } else {
            Ok(ReactionOutput::idle())
        }
    })?;
    graph.settle()?;
    assert!(matches!(
        graph.set_evaluator(numeric, |_| Ok(EvalOutput::immediate(1.0))),
        Err(RuntimeError::NotDerived(id)) if id == numeric
    ));
    assert!(matches!(
        graph.set_evaluator(atom, |_| Ok(EvalOutput::immediate(1.0))),
        Err(RuntimeError::NotDerived(id)) if id == atom
    ));
    assert_eq!(graph.next_event_time(), Some(SimTime::from_millis(5)));
    // Admitted forecast and ordinary writes remain valid after rejection.
    graph.advance_to(SimTime::from_millis(5))?;
    assert_eq!(graph.value_bucket(numeric)?, Bucket::new(3));
    assert_eq!(graph.value_atom(atom)?, 4);
    assert_eq!(graph.reaction_name(reaction)?, "forecast");
    graph.advance_to(at)?;
    assert_eq!(graph.value_bucket(numeric)?, Bucket::new(7));
    assert_eq!(graph.value_atom(atom)?, 9);
    Ok(())
}

#[test]
fn declared_derived_nodes_remain_read_only_and_prevent_premature_settlement()
-> Result<(), Box<dyn std::error::Error>> {
    let mut graph = Graph::new(RuntimeConfig::default());
    let derived = graph.declare_derived("undefined", Quantization::INTEGER, 0.0)?;
    assert!(!graph.is_quiescent());
    assert!(matches!(
        graph.quiescent_snapshot(),
        Err(RuntimeError::NotQuiescent { .. })
    ));
    assert!(matches!(
        graph.set_input_now(derived, 5.0),
        Err(RuntimeError::NotAnInput(id)) if id == derived
    ));
    assert!(matches!(
        graph.schedule_input(derived, SimTime::from_millis(10), 5.0),
        Err(RuntimeError::NotAnInput(id)) if id == derived
    ));
    assert!(matches!(
        graph.settle(),
        Err(RuntimeError::MissingEvaluator(id)) if id == derived
    ));
    graph.set_evaluator(derived, |_| Ok(EvalOutput::immediate(2.0)))?;
    graph.quiesce()?;
    assert_eq!(graph.value_bucket(derived)?, Bucket::new(2));
    assert!(graph.is_quiescent());
    Ok(())
}

#[test]
fn replacing_derived_evaluator_preserves_retry_and_removes_old_forecasts_on_success()
-> Result<(), Box<dyn std::error::Error>> {
    let mut graph = Graph::new(RuntimeConfig::default());
    let source = graph.add_input("old source", Quantization::INTEGER, 3.0)?;
    let derived = graph.add_derived("forecast", Quantization::INTEGER, 0.0, move |context| {
        Ok(
            EvalOutput::after(context.read_f64(source)?, SimDuration::from_millis(30))
                .with_valid_for(SimDuration::from_millis(10)),
        )
    })?;
    graph.settle()?;
    assert_eq!(graph.next_event_time(), Some(SimTime::from_millis(10)));
    graph.set_evaluator(derived, |_| Err(EvalError::Message(String::from("retry"))))?;
    assert!(
        matches!(graph.settle(), Err(RuntimeError::Evaluation { node, .. }) if node == derived)
    );
    assert_eq!(graph.next_event_time(), Some(SimTime::from_millis(10)));
    graph.set_evaluator(derived, |_| Ok(EvalOutput::immediate(11.0)))?;
    graph.settle()?;
    assert_eq!(graph.next_event_time(), None);
    graph.set_input_now(source, 8.0)?;
    assert_eq!(graph.value_bucket(derived)?, Bucket::new(11));
    graph.advance_to(SimTime::from_millis(30))?;
    assert_eq!(graph.value_bucket(derived)?, Bucket::new(11));
    Ok(())
}

#[test]
fn reaction_write_validation_does_not_discard_its_read_dependencies()
-> Result<(), Box<dyn std::error::Error>> {
    let mut graph = Graph::new(RuntimeConfig::default());
    let input = graph.add_atom_input("input", 1)?;
    let output_a = graph.add_atom_input("a", 0)?;
    let output_b = graph.add_atom_input("b", 0)?;
    let fail = Arc::new(AtomicUsize::new(0));
    let flag = Arc::clone(&fail);
    let reaction = graph.add_reaction("copy", move |context| {
        let value = context.read_atom(input)?;
        let mut writes = vec![
            StateWrite::atom(output_a, value),
            StateWrite::atom(output_b, value),
        ];
        if flag.load(Ordering::Relaxed) != 0 {
            writes.push(StateWrite::atom(output_a, value));
        }
        Ok(ReactionOutput::immediate(writes))
    })?;
    graph.settle()?;
    assert!(
        graph.reactions[reaction.index()]
            .dependencies
            .contains(input)
    );
    assert_eq!(graph.value_atom(output_a)?, 1);
    assert_eq!(graph.value_atom(output_b)?, 1);
    fail.store(1, Ordering::Relaxed);
    assert!(
        matches!(graph.set_atom_now(input, 2), Err(RuntimeError::DuplicateStateWrite(id)) if id == output_a)
    );
    assert_eq!(graph.value_atom(output_a)?, 1);
    assert_eq!(graph.value_atom(output_b)?, 1);
    assert!(
        graph.reactions[reaction.index()]
            .dependencies
            .contains(input)
    );
    fail.store(0, Ordering::Relaxed);
    graph.settle()?;
    graph.set_atom_now(input, 3)?;
    assert_eq!(graph.value_atom(output_a)?, 3);
    assert_eq!(graph.value_atom(output_b)?, 3);
    assert!(
        graph.reactions[reaction.index()]
            .dependencies
            .contains(input)
    );
    Ok(())
}

#[test]
fn interleaved_node_roles_keep_distinct_forecast_owners() -> Result<(), Box<dyn std::error::Error>>
{
    let mut graph = Graph::new(RuntimeConfig::default());
    let source = graph.add_input("source", Quantization::INTEGER, 2.0)?;
    let first = graph.add_derived("first", Quantization::INTEGER, 0.0, move |ctx| {
        Ok(EvalOutput::after(
            ctx.read_f64(source)?,
            SimDuration::from_millis(5),
        ))
    })?;
    let atom = graph.add_atom_input("mode", 0)?;
    let second = graph.add_derived("second", Quantization::INTEGER, 0.0, move |ctx| {
        if ctx.read_atom(atom)? == 0 {
            Ok(
                EvalOutput::after(ctx.read_f64(first)?, SimDuration::from_millis(10))
                    .with_valid_for(SimDuration::from_millis(7)),
            )
        } else {
            Ok(EvalOutput::immediate(ctx.read_f64(first)? * 2.0))
        }
    })?;
    let tail = graph.add_input("tail", Quantization::INTEGER, 19.0)?;
    graph.settle()?;
    assert_eq!(graph.derived.len(), 2);
    graph.advance_to(SimTime::from_millis(5))?;
    assert_eq!(graph.value_bucket(first)?, Bucket::new(2));
    assert_eq!(graph.value_bucket(second)?, Bucket::new(0));
    graph.schedule_atom(atom, SimTime::from_millis(6), 1)?;
    graph.advance_to(SimTime::from_millis(20))?;
    let snapshot = graph.quiescent_snapshot()?;
    assert_eq!(
        snapshot.value(source),
        Some(CanonicalValue::Numeric(Bucket::new(2)))
    );
    assert_eq!(
        snapshot.value(first),
        Some(CanonicalValue::Numeric(Bucket::new(2)))
    );
    assert_eq!(snapshot.value(atom), Some(CanonicalValue::Atom(1)));
    assert_eq!(
        snapshot.value(second),
        Some(CanonicalValue::Numeric(Bucket::new(4)))
    );
    assert_eq!(
        snapshot.value(tail),
        Some(CanonicalValue::Numeric(Bucket::new(19)))
    );
    assert_eq!(snapshot.next_event_time, None);
    graph.set_input_now(source, 9.0)?;
    graph.advance_to(SimTime::from_millis(25))?;
    assert_eq!(graph.value_bucket(first)?, Bucket::new(9));
    assert_eq!(graph.value_bucket(second)?, Bucket::new(18));
    Ok(())
}

#[test]
fn callback_reservation_rollback_preserves_existing_keys_and_retries_new_ones()
-> Result<(), Box<dyn std::error::Error>> {
    let mut graph = Graph::new(RuntimeConfig::default());
    let seen = Arc::new(Mutex::new(Vec::new()));
    let target = recording_callback(&mut graph, "target", &seen, |ctx| {
        (ctx.scheduled_key(), ctx.now())
    })?;
    let state = graph.add_atom_input("state", 0)?;
    let producer = graph.add_event_callback("existing", move |_| {
        Ok(ReactionOutput::schedule_reaction_once_after(
            target,
            SimDuration::from_nanos(10),
            1,
            0,
        ))
    })?;
    graph.schedule_reaction_event(producer, SimTime::ZERO, 0)?;
    graph.quiesce()?;
    let phase = Arc::new(AtomicUsize::new(0));
    let control = Arc::clone(&phase);
    let failing = graph.add_event_callback("failing", move |_| {
        let mut result =
            ReactionOutput::schedule_reaction_once_after(target, SimDuration::from_nanos(3), 1, 0)
                .and_schedule_reaction_once_after(target, SimDuration::from_nanos(5), 2, 0)
                .and_schedule_once_after(SimDuration::from_nanos(20), 3, 0);
        match control.load(Ordering::Relaxed) {
            0 => {
                result = result.and_schedule_reaction_once_after(
                    ReactionId::from_index(u32::MAX),
                    SimDuration::ZERO,
                    4,
                    0,
                );
            }
            1 => {
                result = ReactionOutput::immediate(vec![StateWrite::new(state, 1.0)])
                    .and_schedule_reaction_once_after(target, SimDuration::from_nanos(5), 2, 0);
            }
            _ => {}
        }
        Ok(result)
    })?;
    graph.schedule_reaction_event(failing, SimTime::ZERO, 0)?;
    assert!(matches!(
        graph.quiesce(),
        Err(RuntimeError::UnknownReaction(_))
    ));
    assert_eq!(graph.next_event_time(), Some(SimTime::from_nanos(10)));
    assert!(graph.callback_scratch.is_empty());
    phase.store(1, Ordering::Relaxed);
    assert!(matches!(graph.quiesce(), Err(RuntimeError::ExpectedNumeric(id)) if id == state));
    assert_eq!(graph.next_event_time(), Some(SimTime::from_nanos(10)));
    assert!(graph.callback_scratch.is_empty());
    phase.store(2, Ordering::Relaxed);
    graph.quiesce()?;
    graph.advance_to(SimTime::from_nanos(10))?;
    assert_eq!(
        *seen.lock().map_err(|_| "capture poisoned")?,
        [
            (Some(2), SimTime::from_nanos(5)),
            (Some(1), SimTime::from_nanos(10))
        ]
    );
    Ok(())
}

#[test]
fn callback_generation_failure_releases_all_tentative_keys()
-> Result<(), Box<dyn std::error::Error>> {
    let mut graph = Graph::new(RuntimeConfig::default());
    let seen = Arc::new(Mutex::new(Vec::new()));
    let target = recording_callback(&mut graph, "target", &seen, |ctx| {
        (ctx.scheduled_key(), ctx.now())
    })?;
    let producer = graph.add_event_callback("producer", move |_| {
        Ok(
            ReactionOutput::schedule_reaction_once_after(target, SimDuration::ZERO, 1, 0)
                .and_schedule_reaction_once_after(target, SimDuration::ZERO, 2, 0),
        )
    })?;
    graph.reactions[producer.index()].generation = u64::MAX;
    graph.schedule_reaction_event(producer, SimTime::ZERO, 0)?;
    assert!(
        matches!(graph.quiesce(), Err(RuntimeError::GenerationExhausted(super::WorkId::Reaction(id))) if id == producer)
    );
    assert_eq!(graph.next_event_time(), None);
    graph.reactions[producer.index()].generation = 0;
    graph.quiesce()?;
    assert_eq!(
        *seen.lock().map_err(|_| "capture poisoned")?,
        [(Some(1), SimTime::ZERO), (Some(2), SimTime::ZERO)]
    );
    Ok(())
}

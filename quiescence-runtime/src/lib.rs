#![forbid(unsafe_code)]

use quiescence_core::{
    Bucket, Estimate, NodeId, Quantization, QuantizeError, SimDuration, SimTime,
};
use std::{
    cell::RefCell,
    cmp::Ordering,
    collections::{BTreeSet, BinaryHeap, VecDeque},
    fmt,
    sync::Arc,
};

type EvalFn =
    Arc<dyn for<'a> Fn(&EvalContext<'a>) -> Result<EvalOutput, EvalError> + Send + Sync + 'static>;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RuntimeConfig {
    pub same_time_microstep_budget: usize,
}
impl Default for RuntimeConfig {
    fn default() -> Self {
        Self {
            same_time_microstep_budget: 4_096,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct EvalOutput {
    pub estimate: Estimate,
    pub delay: SimDuration,
    pub valid_for: Option<SimDuration>,
}
impl EvalOutput {
    #[must_use]
    pub const fn immediate(value: f64) -> Self {
        Self {
            estimate: Estimate::point(value),
            delay: SimDuration::ZERO,
            valid_for: None,
        }
    }
    #[must_use]
    pub const fn after(value: f64, delay: SimDuration) -> Self {
        Self {
            estimate: Estimate::point(value),
            delay,
            valid_for: None,
        }
    }
    #[must_use]
    pub const fn estimate(estimate: Estimate) -> Self {
        Self {
            estimate,
            delay: SimDuration::ZERO,
            valid_for: None,
        }
    }
    #[must_use]
    pub const fn with_valid_for(mut self, duration: SimDuration) -> Self {
        self.valid_for = Some(duration);
        self
    }
}

pub struct EvalContext<'a> {
    now: SimTime,
    nodes: &'a [Node],
    reads: RefCell<BTreeSet<NodeId>>,
}
impl EvalContext<'_> {
    #[must_use]
    pub const fn now(&self) -> SimTime {
        self.now
    }
    pub fn read_bucket(&self, id: NodeId) -> Result<Bucket, EvalError> {
        let node = self
            .nodes
            .get(id.index())
            .ok_or(EvalError::UnknownNode(id))?;
        self.reads.borrow_mut().insert(id);
        Ok(node.bucket)
    }
    pub fn read_f64(&self, id: NodeId) -> Result<f64, EvalError> {
        let node = self
            .nodes
            .get(id.index())
            .ok_or(EvalError::UnknownNode(id))?;
        self.reads.borrow_mut().insert(id);
        Ok(node.quantization.decode(node.bucket))
    }
    pub fn read_bool(&self, id: NodeId) -> Result<bool, EvalError> {
        Ok(self.read_bucket(id)?.raw() != 0)
    }
    fn dependencies(&self) -> BTreeSet<NodeId> {
        self.reads.borrow().clone()
    }
}

struct Node {
    name: String,
    quantization: Quantization,
    bucket: Bucket,
    evaluator: Option<EvalFn>,
    dependencies: BTreeSet<NodeId>,
    dependents: BTreeSet<NodeId>,
    generation: u64,
}

pub struct Graph {
    config: RuntimeConfig,
    now: SimTime,
    nodes: Vec<Node>,
    dirty: VecDeque<NodeId>,
    is_dirty: Vec<bool>,
    events: BinaryHeap<ScheduledEvent>,
    next_sequence: u64,
    transitions: Vec<TransitionRecord>,
}

impl Graph {
    #[must_use]
    pub fn new(config: RuntimeConfig) -> Self {
        Self {
            config,
            now: SimTime::ZERO,
            nodes: Vec::new(),
            dirty: VecDeque::new(),
            is_dirty: Vec::new(),
            events: BinaryHeap::new(),
            next_sequence: 0,
            transitions: Vec::new(),
        }
    }
    #[must_use]
    pub const fn now(&self) -> SimTime {
        self.now
    }

    pub fn add_input(
        &mut self,
        name: impl Into<String>,
        q: Quantization,
        initial: f64,
    ) -> Result<NodeId, RuntimeError> {
        self.add_node(name, q, initial, None)
    }

    pub fn add_derived<F>(
        &mut self,
        name: impl Into<String>,
        q: Quantization,
        initial: f64,
        evaluator: F,
    ) -> Result<NodeId, RuntimeError>
    where
        F: for<'a> Fn(&EvalContext<'a>) -> Result<EvalOutput, EvalError> + Send + Sync + 'static,
    {
        self.add_node(name, q, initial, Some(Arc::new(evaluator)))
    }

    fn add_node(
        &mut self,
        name: impl Into<String>,
        q: Quantization,
        initial: f64,
        evaluator: Option<EvalFn>,
    ) -> Result<NodeId, RuntimeError> {
        let raw_id = u32::try_from(self.nodes.len()).map_err(|_| RuntimeError::TooManyNodes)?;
        let id = NodeId::from_index(raw_id);
        let bucket = q.resolve(None, Estimate::point(initial))?;
        let derived = evaluator.is_some();
        self.nodes.push(Node {
            name: name.into(),
            quantization: q,
            bucket,
            evaluator,
            dependencies: BTreeSet::new(),
            dependents: BTreeSet::new(),
            generation: 0,
        });
        self.is_dirty.push(false);
        if derived {
            self.mark_dirty(id);
        }
        Ok(id)
    }

    pub fn set_evaluator<F>(&mut self, id: NodeId, evaluator: F) -> Result<(), RuntimeError>
    where
        F: for<'a> Fn(&EvalContext<'a>) -> Result<EvalOutput, EvalError> + Send + Sync + 'static,
    {
        let node = self
            .nodes
            .get_mut(id.index())
            .ok_or(RuntimeError::UnknownNode(id))?;
        node.evaluator = Some(Arc::new(evaluator));
        self.mark_dirty(id);
        Ok(())
    }

    pub fn value_bucket(&self, id: NodeId) -> Result<Bucket, RuntimeError> {
        Ok(self.node(id)?.bucket)
    }
    pub fn value_f64(&self, id: NodeId) -> Result<f64, RuntimeError> {
        let n = self.node(id)?;
        Ok(n.quantization.decode(n.bucket))
    }

    pub fn set_input_now(&mut self, id: NodeId, value: f64) -> Result<(), RuntimeError> {
        self.apply_input(id, value)?;
        self.settle()
    }

    pub fn schedule_input(
        &mut self,
        id: NodeId,
        at: SimTime,
        value: f64,
    ) -> Result<(), RuntimeError> {
        self.node(id)?;
        if at < self.now {
            return Err(RuntimeError::EventInPast {
                now: self.now,
                requested: at,
            });
        }
        self.push_event(at, EventKind::SetInput { node: id, value });
        Ok(())
    }

    pub fn settle(&mut self) -> Result<(), RuntimeError> {
        let mut steps = 0_usize;
        while let Some(id) = self.dirty.pop_front() {
            self.is_dirty[id.index()] = false;
            if self.nodes[id.index()].evaluator.is_none() {
                continue;
            }
            steps = steps.saturating_add(1);
            if steps > self.config.same_time_microstep_budget {
                let active_nodes = self
                    .dirty
                    .iter()
                    .copied()
                    .chain(std::iter::once(id))
                    .collect();
                return Err(RuntimeError::CalculationInstability {
                    at: self.now,
                    microsteps: steps,
                    active_nodes,
                });
            }
            self.evaluate(id)?;
        }
        Ok(())
    }

    pub fn advance_to(&mut self, target: SimTime) -> Result<(), RuntimeError> {
        if target < self.now {
            return Err(RuntimeError::EventInPast {
                now: self.now,
                requested: target,
            });
        }
        self.settle()?;
        while self.events.peek().is_some_and(|e| e.at <= target) {
            let event = self.events.pop().ok_or(RuntimeError::CorruptEventQueue)?;
            self.now = event.at;
            self.apply_event(event.kind)?;
            self.settle()?;
        }
        self.now = target;
        Ok(())
    }

    #[must_use]
    pub fn take_transitions(&mut self) -> Vec<TransitionRecord> {
        std::mem::take(&mut self.transitions)
    }

    fn evaluate(&mut self, id: NodeId) -> Result<(), RuntimeError> {
        let evaluator = self.nodes[id.index()]
            .evaluator
            .as_ref()
            .cloned()
            .ok_or(RuntimeError::MissingEvaluator(id))?;
        let context = EvalContext {
            now: self.now,
            nodes: &self.nodes,
            reads: RefCell::new(BTreeSet::new()),
        };
        let output =
            evaluator(&context).map_err(|source| RuntimeError::Evaluation { node: id, source })?;
        let deps = context.dependencies();
        self.update_dependencies(id, deps)?;

        let current = self.nodes[id.index()].bucket;
        let next = self.nodes[id.index()]
            .quantization
            .resolve(Some(current), output.estimate)?;
        let generation = self.nodes[id.index()].generation.saturating_add(1);
        self.nodes[id.index()].generation = generation;

        if let Some(valid_for) = output.valid_for {
            self.push_event(
                self.now.saturating_add(valid_for),
                EventKind::Reevaluate {
                    node: id,
                    generation,
                },
            );
        }
        if next == current {
            return Ok(());
        }
        if output.delay == SimDuration::ZERO {
            self.commit(id, next);
        } else {
            self.push_event(
                self.now.saturating_add(output.delay),
                EventKind::Commit {
                    node: id,
                    generation,
                    bucket: next,
                },
            );
        }
        Ok(())
    }

    fn update_dependencies(
        &mut self,
        id: NodeId,
        new: BTreeSet<NodeId>,
    ) -> Result<(), RuntimeError> {
        for dep in &new {
            self.node(*dep)?;
        }
        let old = std::mem::replace(&mut self.nodes[id.index()].dependencies, new.clone());
        for dep in old.difference(&new) {
            self.nodes[dep.index()].dependents.remove(&id);
        }
        for dep in new.difference(&old) {
            self.nodes[dep.index()].dependents.insert(id);
        }
        Ok(())
    }

    fn apply_event(&mut self, event: EventKind) -> Result<(), RuntimeError> {
        match event {
            EventKind::SetInput { node, value } => self.apply_input(node, value),
            EventKind::Commit {
                node,
                generation,
                bucket,
            } => {
                self.node(node)?;
                if self.nodes[node.index()].generation == generation {
                    self.commit(node, bucket);
                }
                Ok(())
            }
            EventKind::Reevaluate { node, generation } => {
                self.node(node)?;
                if self.nodes[node.index()].generation == generation {
                    self.mark_dirty(node);
                }
                Ok(())
            }
        }
    }

    fn apply_input(&mut self, id: NodeId, value: f64) -> Result<(), RuntimeError> {
        self.node(id)?;
        if self.nodes[id.index()].evaluator.is_some() {
            return Err(RuntimeError::NotAnInput(id));
        }
        let current = self.nodes[id.index()].bucket;
        let next = self.nodes[id.index()]
            .quantization
            .resolve(Some(current), Estimate::point(value))?;
        if next != current {
            self.nodes[id.index()].generation = self.nodes[id.index()].generation.saturating_add(1);
            self.commit(id, next);
        }
        Ok(())
    }

    fn commit(&mut self, id: NodeId, bucket: Bucket) {
        let node = &mut self.nodes[id.index()];
        let old = node.bucket;
        if old == bucket {
            return;
        }
        node.bucket = bucket;
        self.transitions.push(TransitionRecord {
            at: self.now,
            node: id,
            node_name: node.name.clone(),
            old,
            new: bucket,
        });
        let dependents: Vec<_> = node.dependents.iter().copied().collect();
        for dependent in dependents {
            self.mark_dirty(dependent);
        }
    }

    fn mark_dirty(&mut self, id: NodeId) {
        if !self.is_dirty[id.index()] {
            self.is_dirty[id.index()] = true;
            self.dirty.push_back(id);
        }
    }

    fn push_event(&mut self, at: SimTime, kind: EventKind) {
        let sequence = self.next_sequence;
        self.next_sequence = self.next_sequence.saturating_add(1);
        self.events.push(ScheduledEvent { at, sequence, kind });
    }

    fn node(&self, id: NodeId) -> Result<&Node, RuntimeError> {
        self.nodes
            .get(id.index())
            .ok_or(RuntimeError::UnknownNode(id))
    }
}

#[derive(Clone, Debug, PartialEq)]
enum EventKind {
    SetInput {
        node: NodeId,
        value: f64,
    },
    Commit {
        node: NodeId,
        generation: u64,
        bucket: Bucket,
    },
    Reevaluate {
        node: NodeId,
        generation: u64,
    },
}
#[derive(Clone, Debug, PartialEq)]
struct ScheduledEvent {
    at: SimTime,
    sequence: u64,
    kind: EventKind,
}
impl Eq for ScheduledEvent {}
impl Ord for ScheduledEvent {
    fn cmp(&self, other: &Self) -> Ordering {
        other
            .at
            .cmp(&self.at)
            .then_with(|| other.sequence.cmp(&self.sequence))
    }
}
impl PartialOrd for ScheduledEvent {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TransitionRecord {
    pub at: SimTime,
    pub node: NodeId,
    pub node_name: String,
    pub old: Bucket,
    pub new: Bucket,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum EvalError {
    UnknownNode(NodeId),
    Message(String),
}
impl fmt::Display for EvalError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnknownNode(id) => write!(f, "evaluator read unknown node {id:?}"),
            Self::Message(m) => f.write_str(m),
        }
    }
}
impl std::error::Error for EvalError {}

#[derive(Debug)]
pub enum RuntimeError {
    Quantize(QuantizeError),
    UnknownNode(NodeId),
    MissingEvaluator(NodeId),
    NotAnInput(NodeId),
    TooManyNodes,
    CorruptEventQueue,
    EventInPast {
        now: SimTime,
        requested: SimTime,
    },
    Evaluation {
        node: NodeId,
        source: EvalError,
    },
    CalculationInstability {
        at: SimTime,
        microsteps: usize,
        active_nodes: Vec<NodeId>,
    },
}
impl fmt::Display for RuntimeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Quantize(e) => write!(f, "quantization failed: {e}"),
            Self::UnknownNode(id) => write!(f, "unknown node {id:?}"),
            Self::MissingEvaluator(id) => write!(f, "node {id:?} has no evaluator"),
            Self::NotAnInput(id) => write!(f, "node {id:?} is derived, not an input"),
            Self::TooManyNodes => f.write_str("graph contains too many nodes"),
            Self::CorruptEventQueue => f.write_str("event queue became inconsistent"),
            Self::EventInPast { now, requested } => write!(
                f,
                "cannot move backwards: now={now:?}, requested={requested:?}"
            ),
            Self::Evaluation { node, source } => {
                write!(f, "evaluation of {node:?} failed: {source}")
            }
            Self::CalculationInstability {
                at,
                microsteps,
                active_nodes,
            } => write!(
                f,
                "calculation instability at {at:?} after {microsteps} same-time microsteps; active nodes: {active_nodes:?}"
            ),
        }
    }
}
impl std::error::Error for RuntimeError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Quantize(e) => Some(e),
            Self::Evaluation { source, .. } => Some(source),
            _ => None,
        }
    }
}
impl From<QuantizeError> for RuntimeError {
    fn from(value: QuantizeError) -> Self {
        Self::Quantize(value)
    }
}

#[cfg(test)]
mod tests {
    use super::{EvalOutput, Graph, RuntimeConfig, RuntimeError};
    use quiescence_core::{Quantization, SimDuration, SimTime};
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    fn q0() -> Result<Quantization, Box<dyn std::error::Error>> {
        Ok(Quantization::new(0, 0)?)
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
                SimDuration::from_millis(c.read_f64(reaction)? as i64),
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
    fn zero_time_oscillation_is_explicit_instability() -> Result<(), Box<dyn std::error::Error>> {
        let mut g = Graph::new(RuntimeConfig {
            same_time_microstep_budget: 32,
        });
        let a = g.add_input("a", q0()?, 0.0)?;
        let b = g.add_input("b", q0()?, 0.0)?;
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
}

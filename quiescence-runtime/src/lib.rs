#![forbid(unsafe_code)]

use quiescence_core::{
    Bucket, Estimate, NodeId, Quantization, QuantizeError, ReactionId, SimDuration, SimTime,
};
use std::{
    cell::RefCell,
    cmp::Ordering,
    collections::{BTreeMap, BTreeSet, BinaryHeap, VecDeque},
    fmt,
    sync::Arc,
};

type EvalFn =
    Arc<dyn for<'a> Fn(&EvalContext<'a>) -> Result<EvalOutput, EvalError> + Send + Sync + 'static>;
type ReactionFn = Arc<
    dyn for<'a> Fn(&EvalContext<'a>) -> Result<ReactionOutput, EvalError> + Send + Sync + 'static,
>;

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
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CanonicalValue {
    Numeric(Bucket),
    Atom(u64),
}

impl fmt::Display for CanonicalValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Numeric(bucket) => write!(f, "{}", bucket.raw()),
            Self::Atom(value) => write!(f, "atom:{value}"),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum StateWrite {
    Numeric { node: NodeId, value: f64 },
    Atom { node: NodeId, value: u64 },
}
impl StateWrite {
    #[must_use]
    pub const fn new(node: NodeId, value: f64) -> Self {
        Self::Numeric { node, value }
    }

    #[must_use]
    pub const fn atom(node: NodeId, value: u64) -> Self {
        Self::Atom { node, value }
    }

    #[must_use]
    pub const fn node(self) -> NodeId {
        match self {
            Self::Numeric { node, .. } | Self::Atom { node, .. } => node,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ScheduledReevaluation {
    pub key: u64,
    pub after: SimDuration,
    pub priority: i32,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ReactionOutput {
    pub transition_after: Option<SimDuration>,
    pub writes: Vec<StateWrite>,
    pub scheduled_reevaluations: Vec<ScheduledReevaluation>,
}
impl ReactionOutput {
    #[must_use]
    pub const fn idle() -> Self {
        Self {
            transition_after: None,
            writes: Vec::new(),
            scheduled_reevaluations: Vec::new(),
        }
    }

    #[must_use]
    pub fn after(delay: SimDuration, writes: Vec<StateWrite>) -> Self {
        Self {
            transition_after: Some(delay),
            writes,
            scheduled_reevaluations: Vec::new(),
        }
    }

    #[must_use]
    pub fn immediate(writes: Vec<StateWrite>) -> Self {
        Self::after(SimDuration::ZERO, writes)
    }

    #[must_use]
    pub fn reevaluate_after(delay: SimDuration) -> Self {
        Self::after(delay, Vec::new())
    }

    #[must_use]
    pub fn schedule_once_after(delay: SimDuration, key: u64, priority: i32) -> Self {
        Self {
            transition_after: None,
            writes: Vec::new(),
            scheduled_reevaluations: vec![ScheduledReevaluation {
                key,
                after: delay,
                priority,
            }],
        }
    }

    #[must_use]
    pub fn and_schedule_once_after(mut self, delay: SimDuration, key: u64, priority: i32) -> Self {
        self.scheduled_reevaluations.push(ScheduledReevaluation {
            key,
            after: delay,
            priority,
        });
        self
    }
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
        let NodeState::Numeric { bucket, .. } = node.state else {
            return Err(EvalError::ExpectedNumeric(id));
        };
        Ok(bucket)
    }
    pub fn read_f64(&self, id: NodeId) -> Result<f64, EvalError> {
        let node = self
            .nodes
            .get(id.index())
            .ok_or(EvalError::UnknownNode(id))?;
        self.reads.borrow_mut().insert(id);
        let NodeState::Numeric {
            quantization,
            bucket,
        } = node.state
        else {
            return Err(EvalError::ExpectedNumeric(id));
        };
        Ok(quantization.decode(bucket))
    }
    pub fn read_bool(&self, id: NodeId) -> Result<bool, EvalError> {
        Ok(self.read_bucket(id)?.raw() != 0)
    }
    pub fn read_atom(&self, id: NodeId) -> Result<u64, EvalError> {
        let node = self
            .nodes
            .get(id.index())
            .ok_or(EvalError::UnknownNode(id))?;
        self.reads.borrow_mut().insert(id);
        let NodeState::Atom(value) = node.state else {
            return Err(EvalError::ExpectedAtom(id));
        };
        Ok(value)
    }
    fn dependencies(&self) -> BTreeSet<NodeId> {
        self.reads.borrow().clone()
    }
}

enum NodeState {
    Numeric {
        quantization: Quantization,
        bucket: Bucket,
    },
    Atom(u64),
}

struct Node {
    name: String,
    state: NodeState,
    evaluator: Option<EvalFn>,
    dependencies: BTreeSet<NodeId>,
    dependents: BTreeSet<NodeId>,
    generation: u64,
}

struct Reaction {
    name: String,
    evaluator: ReactionFn,
    dependencies: BTreeSet<NodeId>,
    generation: u64,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum WorkId {
    Node(NodeId),
    Reaction(ReactionId),
}

pub struct Graph {
    config: RuntimeConfig,
    now: SimTime,
    nodes: Vec<Node>,
    reactions: Vec<Reaction>,
    reaction_dependents: BTreeMap<NodeId, BTreeSet<ReactionId>>,
    dirty: VecDeque<WorkId>,
    node_is_dirty: Vec<bool>,
    reaction_is_dirty: Vec<bool>,
    events: BinaryHeap<ScheduledEvent>,
    pending_scheduled_reactions: BTreeSet<(ReactionId, u64)>,
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
            reactions: Vec::new(),
            reaction_dependents: BTreeMap::new(),
            dirty: VecDeque::new(),
            node_is_dirty: Vec::new(),
            reaction_is_dirty: Vec::new(),
            events: BinaryHeap::new(),
            pending_scheduled_reactions: BTreeSet::new(),
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

    pub fn add_atom_input(
        &mut self,
        name: impl Into<String>,
        initial: u64,
    ) -> Result<NodeId, RuntimeError> {
        let raw_id = u32::try_from(self.nodes.len()).map_err(|_| RuntimeError::TooManyNodes)?;
        let id = NodeId::from_index(raw_id);
        self.nodes.push(Node {
            name: name.into(),
            state: NodeState::Atom(initial),
            evaluator: None,
            dependencies: BTreeSet::new(),
            dependents: BTreeSet::new(),
            generation: 0,
        });
        self.node_is_dirty.push(false);
        Ok(id)
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
            state: NodeState::Numeric {
                quantization: q,
                bucket,
            },
            evaluator,
            dependencies: BTreeSet::new(),
            dependents: BTreeSet::new(),
            generation: 0,
        });
        self.node_is_dirty.push(false);
        if derived {
            self.mark_dirty(WorkId::Node(id));
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
        if !matches!(node.state, NodeState::Numeric { .. }) {
            return Err(RuntimeError::ExpectedNumeric(id));
        }
        node.evaluator = Some(Arc::new(evaluator));
        self.mark_dirty(WorkId::Node(id));
        Ok(())
    }

    pub fn add_reaction<F>(
        &mut self,
        name: impl Into<String>,
        evaluator: F,
    ) -> Result<ReactionId, RuntimeError>
    where
        F: for<'a> Fn(&EvalContext<'a>) -> Result<ReactionOutput, EvalError>
            + Send
            + Sync
            + 'static,
    {
        let raw_id =
            u32::try_from(self.reactions.len()).map_err(|_| RuntimeError::TooManyReactions)?;
        let id = ReactionId::from_index(raw_id);
        self.reactions.push(Reaction {
            name: name.into(),
            evaluator: Arc::new(evaluator),
            dependencies: BTreeSet::new(),
            generation: 0,
        });
        self.reaction_is_dirty.push(false);
        self.mark_dirty(WorkId::Reaction(id));
        Ok(id)
    }

    pub fn reaction_name(&self, id: ReactionId) -> Result<&str, RuntimeError> {
        Ok(&self
            .reactions
            .get(id.index())
            .ok_or(RuntimeError::UnknownReaction(id))?
            .name)
    }

    pub fn value_bucket(&self, id: NodeId) -> Result<Bucket, RuntimeError> {
        let NodeState::Numeric { bucket, .. } = self.node(id)?.state else {
            return Err(RuntimeError::ExpectedNumeric(id));
        };
        Ok(bucket)
    }
    pub fn value_f64(&self, id: NodeId) -> Result<f64, RuntimeError> {
        let NodeState::Numeric {
            quantization,
            bucket,
        } = self.node(id)?.state
        else {
            return Err(RuntimeError::ExpectedNumeric(id));
        };
        Ok(quantization.decode(bucket))
    }
    pub fn value_atom(&self, id: NodeId) -> Result<u64, RuntimeError> {
        let NodeState::Atom(value) = self.node(id)?.state else {
            return Err(RuntimeError::ExpectedAtom(id));
        };
        Ok(value)
    }

    pub fn set_input_now(&mut self, id: NodeId, value: f64) -> Result<(), RuntimeError> {
        self.apply_state_writes(&[StateWrite::new(id, value)])?;
        self.settle()
    }

    pub fn set_atom_now(&mut self, id: NodeId, value: u64) -> Result<(), RuntimeError> {
        self.apply_state_writes(&[StateWrite::atom(id, value)])?;
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
        self.push_event(
            at,
            EventKind::SetState {
                write: StateWrite::new(id, value),
            },
        );
        Ok(())
    }

    pub fn schedule_atom(
        &mut self,
        id: NodeId,
        at: SimTime,
        value: u64,
    ) -> Result<(), RuntimeError> {
        self.node(id)?;
        if at < self.now {
            return Err(RuntimeError::EventInPast {
                now: self.now,
                requested: at,
            });
        }
        self.push_event(
            at,
            EventKind::SetState {
                write: StateWrite::atom(id, value),
            },
        );
        Ok(())
    }

    pub fn settle(&mut self) -> Result<(), RuntimeError> {
        let mut steps = 0_usize;
        while let Some(work) = self.dirty.pop_front() {
            match work {
                WorkId::Node(id) => self.node_is_dirty[id.index()] = false,
                WorkId::Reaction(id) => self.reaction_is_dirty[id.index()] = false,
            }
            steps = steps.saturating_add(1);
            if steps > self.config.same_time_microstep_budget {
                let active_work = self
                    .dirty
                    .iter()
                    .copied()
                    .chain(std::iter::once(work))
                    .collect();
                return Err(RuntimeError::CalculationInstability {
                    at: self.now,
                    microsteps: steps,
                    active_work,
                });
            }
            match work {
                WorkId::Node(id) => {
                    if self.nodes[id.index()].evaluator.is_some() {
                        self.evaluate(id)?;
                    }
                }
                WorkId::Reaction(id) => self.evaluate_reaction(id)?,
            }
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
    pub fn next_event_time(&self) -> Option<SimTime> {
        self.events.peek().map(|event| event.at)
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
        self.update_dependencies(WorkId::Node(id), deps)?;

        let (quantization, current) = match self.nodes[id.index()].state {
            NodeState::Numeric {
                quantization,
                bucket,
            } => (quantization, bucket),
            NodeState::Atom(_) => return Err(RuntimeError::ExpectedNumeric(id)),
        };
        let next = quantization.resolve(Some(current), output.estimate)?;
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
            self.commit(id, CanonicalValue::Numeric(next))?;
        } else {
            self.push_event(
                self.now.saturating_add(output.delay),
                EventKind::Commit {
                    node: id,
                    generation,
                    value: CanonicalValue::Numeric(next),
                },
            );
        }
        Ok(())
    }

    fn evaluate_reaction(&mut self, id: ReactionId) -> Result<(), RuntimeError> {
        let evaluator = self
            .reactions
            .get(id.index())
            .ok_or(RuntimeError::UnknownReaction(id))?
            .evaluator
            .clone();
        let context = EvalContext {
            now: self.now,
            nodes: &self.nodes,
            reads: RefCell::new(BTreeSet::new()),
        };
        let output = evaluator(&context).map_err(|source| RuntimeError::ReactionEvaluation {
            reaction: id,
            source,
        })?;
        let deps = context.dependencies();
        self.update_dependencies(WorkId::Reaction(id), deps)?;
        let reaction = self
            .reactions
            .get_mut(id.index())
            .ok_or(RuntimeError::UnknownReaction(id))?;
        reaction.generation = reaction.generation.saturating_add(1);
        let generation = reaction.generation;

        let ReactionOutput {
            transition_after,
            writes,
            scheduled_reevaluations,
        } = output;
        if let Some(delay) = transition_after {
            if delay == SimDuration::ZERO {
                self.apply_state_writes(&writes)?;
            } else {
                self.push_event(
                    self.now.saturating_add(delay),
                    EventKind::ReactionTransition {
                        reaction: id,
                        generation,
                        writes,
                    },
                );
            }
        } else if !writes.is_empty() {
            return Err(RuntimeError::UnscheduledReactionWrites(id));
        }
        for request in scheduled_reevaluations {
            self.schedule_reaction_once(id, request);
        }
        Ok(())
    }

    fn update_dependencies(
        &mut self,
        dependent: WorkId,
        new: BTreeSet<NodeId>,
    ) -> Result<(), RuntimeError> {
        for dep in &new {
            self.node(*dep)?;
        }
        let old = match dependent {
            WorkId::Node(id) => {
                std::mem::replace(&mut self.nodes[id.index()].dependencies, new.clone())
            }
            WorkId::Reaction(id) => std::mem::replace(
                &mut self
                    .reactions
                    .get_mut(id.index())
                    .ok_or(RuntimeError::UnknownReaction(id))?
                    .dependencies,
                new.clone(),
            ),
        };
        for dep in old.difference(&new) {
            match dependent {
                WorkId::Node(id) => {
                    self.nodes[dep.index()].dependents.remove(&id);
                }
                WorkId::Reaction(id) => {
                    if let Some(reactions) = self.reaction_dependents.get_mut(dep) {
                        reactions.remove(&id);
                        if reactions.is_empty() {
                            self.reaction_dependents.remove(dep);
                        }
                    }
                }
            }
        }
        for dep in new.difference(&old) {
            match dependent {
                WorkId::Node(id) => {
                    self.nodes[dep.index()].dependents.insert(id);
                }
                WorkId::Reaction(id) => {
                    self.reaction_dependents.entry(*dep).or_default().insert(id);
                }
            }
        }
        Ok(())
    }

    fn apply_event(&mut self, event: EventKind) -> Result<(), RuntimeError> {
        match event {
            EventKind::SetState { write } => self.apply_state_writes(&[write]),
            EventKind::Commit {
                node,
                generation,
                value,
            } => {
                self.node(node)?;
                if self.nodes[node.index()].generation == generation {
                    self.commit(node, value)?;
                }
                Ok(())
            }
            EventKind::Reevaluate { node, generation } => {
                self.node(node)?;
                if self.nodes[node.index()].generation == generation {
                    self.mark_dirty(WorkId::Node(node));
                }
                Ok(())
            }
            EventKind::ScheduledReaction { reaction, key } => {
                self.pending_scheduled_reactions.remove(&(reaction, key));
                self.mark_dirty(WorkId::Reaction(reaction));
                Ok(())
            }
            EventKind::ReactionTransition {
                reaction,
                generation,
                writes,
            } => {
                let current_generation = self
                    .reactions
                    .get(reaction.index())
                    .ok_or(RuntimeError::UnknownReaction(reaction))?
                    .generation;
                if current_generation == generation {
                    self.apply_state_writes(&writes)?;
                    self.mark_dirty(WorkId::Reaction(reaction));
                }
                Ok(())
            }
        }
    }

    fn apply_state_writes(&mut self, writes: &[StateWrite]) -> Result<(), RuntimeError> {
        let mut seen = BTreeSet::new();
        let mut resolved = Vec::with_capacity(writes.len());
        for write in writes {
            let node_id = write.node();
            self.node(node_id)?;
            if !seen.insert(node_id) {
                return Err(RuntimeError::DuplicateStateWrite(node_id));
            }
            if self.nodes[node_id.index()].evaluator.is_some() {
                return Err(RuntimeError::NotAnInput(node_id));
            }
            let current = canonical_value(&self.nodes[node_id.index()]);
            let next = match (*write, &self.nodes[node_id.index()].state) {
                (
                    StateWrite::Numeric { value, .. },
                    NodeState::Numeric {
                        quantization,
                        bucket,
                    },
                ) => CanonicalValue::Numeric(
                    quantization.resolve(Some(*bucket), Estimate::point(value))?,
                ),
                (StateWrite::Atom { value, .. }, NodeState::Atom(_)) => CanonicalValue::Atom(value),
                (StateWrite::Numeric { .. }, NodeState::Atom(_)) => {
                    return Err(RuntimeError::ExpectedNumeric(node_id));
                }
                (StateWrite::Atom { .. }, NodeState::Numeric { .. }) => {
                    return Err(RuntimeError::ExpectedAtom(node_id));
                }
            };
            resolved.push((node_id, current, next));
        }
        for (node, current, next) in resolved {
            if next != current {
                self.nodes[node.index()].generation =
                    self.nodes[node.index()].generation.saturating_add(1);
                self.commit(node, next)?;
            }
        }
        Ok(())
    }

    fn commit(&mut self, id: NodeId, value: CanonicalValue) -> Result<(), RuntimeError> {
        let node = &mut self.nodes[id.index()];
        let old = canonical_value(node);
        if old == value {
            return Ok(());
        }
        match (&mut node.state, value) {
            (NodeState::Numeric { bucket, .. }, CanonicalValue::Numeric(next)) => *bucket = next,
            (NodeState::Atom(current), CanonicalValue::Atom(next)) => *current = next,
            (NodeState::Numeric { .. }, CanonicalValue::Atom(_)) => {
                return Err(RuntimeError::ExpectedNumeric(id));
            }
            (NodeState::Atom(_), CanonicalValue::Numeric(_)) => {
                return Err(RuntimeError::ExpectedAtom(id));
            }
        }
        self.transitions.push(TransitionRecord {
            at: self.now,
            node: id,
            node_name: node.name.clone(),
            old,
            new: value,
        });
        let node_dependents: Vec<_> = node.dependents.iter().copied().collect();
        for dependent in node_dependents {
            self.mark_dirty(WorkId::Node(dependent));
        }
        let reaction_dependents: Vec<_> = self
            .reaction_dependents
            .get(&id)
            .into_iter()
            .flat_map(|dependents| dependents.iter().copied())
            .collect();
        for dependent in reaction_dependents {
            self.mark_dirty(WorkId::Reaction(dependent));
        }
        Ok(())
    }

    fn mark_dirty(&mut self, work: WorkId) {
        let already_dirty = match work {
            WorkId::Node(id) => &mut self.node_is_dirty[id.index()],
            WorkId::Reaction(id) => &mut self.reaction_is_dirty[id.index()],
        };
        if !*already_dirty {
            *already_dirty = true;
            self.dirty.push_back(work);
        }
    }

    fn schedule_reaction_once(&mut self, reaction: ReactionId, request: ScheduledReevaluation) {
        if !self
            .pending_scheduled_reactions
            .insert((reaction, request.key))
        {
            return;
        }
        self.push_event_with_priority(
            self.now.saturating_add(request.after),
            request.priority,
            EventKind::ScheduledReaction {
                reaction,
                key: request.key,
            },
        );
    }

    fn push_event(&mut self, at: SimTime, kind: EventKind) {
        self.push_event_with_priority(at, 0, kind);
    }

    fn push_event_with_priority(&mut self, at: SimTime, priority: i32, kind: EventKind) {
        let sequence = self.next_sequence;
        self.next_sequence = self.next_sequence.saturating_add(1);
        self.events.push(ScheduledEvent {
            at,
            priority,
            sequence,
            kind,
        });
    }

    fn node(&self, id: NodeId) -> Result<&Node, RuntimeError> {
        self.nodes
            .get(id.index())
            .ok_or(RuntimeError::UnknownNode(id))
    }
}

fn canonical_value(node: &Node) -> CanonicalValue {
    match node.state {
        NodeState::Numeric { bucket, .. } => CanonicalValue::Numeric(bucket),
        NodeState::Atom(value) => CanonicalValue::Atom(value),
    }
}

#[derive(Clone, Debug, PartialEq)]
enum EventKind {
    SetState {
        write: StateWrite,
    },
    Commit {
        node: NodeId,
        generation: u64,
        value: CanonicalValue,
    },
    Reevaluate {
        node: NodeId,
        generation: u64,
    },
    ReactionTransition {
        reaction: ReactionId,
        generation: u64,
        writes: Vec<StateWrite>,
    },
    ScheduledReaction {
        reaction: ReactionId,
        key: u64,
    },
}
#[derive(Clone, Debug, PartialEq)]
struct ScheduledEvent {
    at: SimTime,
    priority: i32,
    sequence: u64,
    kind: EventKind,
}
impl Eq for ScheduledEvent {}
impl Ord for ScheduledEvent {
    fn cmp(&self, other: &Self) -> Ordering {
        other
            .at
            .cmp(&self.at)
            .then_with(|| other.priority.cmp(&self.priority))
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
    pub old: CanonicalValue,
    pub new: CanonicalValue,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum EvalError {
    UnknownNode(NodeId),
    ExpectedNumeric(NodeId),
    ExpectedAtom(NodeId),
    Message(String),
}
impl fmt::Display for EvalError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnknownNode(id) => write!(f, "evaluator read unknown node {id:?}"),
            Self::ExpectedNumeric(id) => write!(f, "evaluator expected numeric node {id:?}"),
            Self::ExpectedAtom(id) => write!(f, "evaluator expected atom node {id:?}"),
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
    ExpectedNumeric(NodeId),
    ExpectedAtom(NodeId),
    TooManyNodes,
    TooManyReactions,
    UnknownReaction(ReactionId),
    UnscheduledReactionWrites(ReactionId),
    DuplicateStateWrite(NodeId),
    CorruptEventQueue,
    EventInPast {
        now: SimTime,
        requested: SimTime,
    },
    Evaluation {
        node: NodeId,
        source: EvalError,
    },
    ReactionEvaluation {
        reaction: ReactionId,
        source: EvalError,
    },
    CalculationInstability {
        at: SimTime,
        microsteps: usize,
        active_work: Vec<WorkId>,
    },
}
impl fmt::Display for RuntimeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Quantize(e) => write!(f, "quantization failed: {e}"),
            Self::UnknownNode(id) => write!(f, "unknown node {id:?}"),
            Self::MissingEvaluator(id) => write!(f, "node {id:?} has no evaluator"),
            Self::NotAnInput(id) => write!(f, "node {id:?} is derived, not an input"),
            Self::ExpectedNumeric(id) => write!(f, "node {id:?} is not numeric"),
            Self::ExpectedAtom(id) => write!(f, "node {id:?} is not an atom"),
            Self::TooManyNodes => f.write_str("graph contains too many nodes"),
            Self::TooManyReactions => f.write_str("graph contains too many reactions"),
            Self::UnknownReaction(id) => write!(f, "unknown reaction {id:?}"),
            Self::UnscheduledReactionWrites(id) => {
                write!(
                    f,
                    "reaction {id:?} returned writes without a transition time"
                )
            }
            Self::DuplicateStateWrite(id) => {
                write!(
                    f,
                    "reaction transition writes state node {id:?} more than once"
                )
            }
            Self::CorruptEventQueue => f.write_str("event queue became inconsistent"),
            Self::EventInPast { now, requested } => write!(
                f,
                "cannot move backwards: now={now:?}, requested={requested:?}"
            ),
            Self::Evaluation { node, source } => {
                write!(f, "evaluation of {node:?} failed: {source}")
            }
            Self::ReactionEvaluation { reaction, source } => {
                write!(f, "evaluation of reaction {reaction:?} failed: {source}")
            }
            Self::CalculationInstability {
                at,
                microsteps,
                active_work,
            } => write!(
                f,
                "calculation instability at {at:?} after {microsteps} same-time microsteps; active work: {active_work:?}"
            ),
        }
    }
}
impl std::error::Error for RuntimeError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Quantize(e) => Some(e),
            Self::Evaluation { source, .. } | Self::ReactionEvaluation { source, .. } => {
                Some(source)
            }
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
    use super::{
        CanonicalValue, EvalOutput, Graph, ReactionOutput, RuntimeConfig, RuntimeError, StateWrite,
    };
    use quiescence_core::{Bucket, Quantization, SimDuration, SimTime};
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
            if left_value != right_value {
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
        let sink = g.add_input("sink", q0()?, 0.0)?;
        let evaluations = Arc::new(AtomicUsize::new(0));
        let calls = Arc::clone(&evaluations);
        let _reaction = g.add_reaction("observe snapshot", move |c| {
            calls.fetch_add(1, Ordering::Relaxed);
            let snapshot_value = c.read_atom(snapshot)?;
            Ok(ReactionOutput::immediate(vec![StateWrite::new(
                sink,
                snapshot_value as f64,
            )]))
        })?;
        g.settle()?;
        assert_eq!(g.value_atom(snapshot)?, 10);
        assert_eq!(g.value_f64(sink)?, 10.0);
        let before = evaluations.load(Ordering::Relaxed);

        g.set_atom_now(snapshot, 10)?;
        assert_eq!(evaluations.load(Ordering::Relaxed), before);
        g.set_atom_now(snapshot, 11)?;
        assert!(evaluations.load(Ordering::Relaxed) > before);
        assert_eq!(g.value_f64(sink)?, 11.0);
        Ok(())
    }

    #[test]
    fn scheduled_atom_change_invalidates_reaction_forecast()
    -> Result<(), Box<dyn std::error::Error>> {
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
    fn mixed_numeric_and_atom_reaction_writes_are_atomic() -> Result<(), Box<dyn std::error::Error>>
    {
        let mut g = Graph::new(RuntimeConfig::default());
        let trigger = g.add_input("trigger", q0()?, 0.0)?;
        let count = g.add_input("count", q0()?, 0.0)?;
        let snapshot = g.add_atom_input("snapshot", 100)?;
        let mismatches = Arc::new(AtomicUsize::new(0));
        let seen = Arc::clone(&mismatches);
        let _observer = g.add_derived("observer", q0()?, 0.0, move |c| {
            let count_value = c.read_f64(count)? as u64;
            let snapshot_value = c.read_atom(snapshot)?;
            if snapshot_value != count_value + 100 {
                seen.fetch_add(1, Ordering::Relaxed);
            }
            Ok(EvalOutput::immediate(count_value as f64))
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
        assert!(
            matches!(g.value_atom(numeric), Err(RuntimeError::ExpectedAtom(id)) if id == numeric)
        );
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
    fn scheduled_reaction_priority_orders_same_time_callbacks()
    -> Result<(), Box<dyn std::error::Error>> {
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
    fn delayed_reevaluation_defers_dynamic_atom_dependencies()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut g = Graph::new(RuntimeConfig::default());
        let ready_at_ms = g.add_input("ready_at_ms", q0()?, 10.0)?;
        let snapshot = g.add_atom_input("snapshot", 1)?;
        let observed = g.add_atom_input("observed", 0)?;
        let evaluations = Arc::new(AtomicUsize::new(0));
        let calls = Arc::clone(&evaluations);
        let _reaction = g.add_reaction("deadline-gated snapshot", move |c| {
            calls.fetch_add(1, Ordering::Relaxed);
            let now_ms = c.now().as_nanos() / 1_000_000;
            let ready_at = c.read_f64(ready_at_ms)? as i64;
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

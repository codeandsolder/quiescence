//! Single-owner temporal incremental computation with quiescent publications.
//!
//! Derived nodes subscribe to the state they actually read. Canonically unchanged
//! values stop propagation, while explicit callback events preserve causal order.
//! A thread-safe ingress allows producers to submit work without owning the graph.
//!
//! ```
//! use quiescence_core::{Bucket, Quantization};
//! use quiescence_runtime::{CanonicalValue, EvalOutput, Graph, RuntimeConfig, StateWrite, interaction_channel};
//!
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! let mut graph = Graph::new(RuntimeConfig::default());
//! let integer = Quantization::new(0, 0)?;
//! let source = graph.add_input("source", integer, 0.0)?;
//! let doubled = graph.add_derived("doubled", integer, 0.0, move |context| {
//!     Ok(EvalOutput::immediate(2.0 * context.read_f64(source)?))
//! })?;
//! let (sender, inbox) = interaction_channel();
//! sender.state_now(0, vec![StateWrite::new(source, 3.0)])?;
//! let (import, publication) = graph.process_interactions_and_publish(&inbox)?;
//! assert_eq!(import.accepted, 1);
//! assert_eq!(publication.snapshot.value(doubled),
//!     Some(CanonicalValue::Numeric(Bucket::new(6))));
//! # Ok(())
//! # }
//! ```

#![forbid(unsafe_code)]

use quiescence_core::{
    Bucket, Estimate, HistoryId, NodeId, Quantization, QuantizeError, ReactionId, SimDuration,
    SimTime,
};
use std::{
    cell::RefCell,
    collections::{BTreeSet, VecDeque},
    sync::Arc,
};

mod error;
mod event_queue;
mod history;
mod ingress;
mod output;

pub use error::{EvalError, InteractionIngressError, RuntimeError};
pub use ingress::{
    ExternalInteraction, InteractionImport, InteractionInbox, InteractionMoment, InteractionSender,
    RejectedInteraction, interaction_channel,
};
pub use output::{
    CallbackTarget, CanonicalValue, EvalOutput, QuiescentPublication, QuiescentSnapshot,
    ReactionOutput, ReactionTransition, ScheduledCallback, StateWrite, WindowRecord,
};

use event_queue::{EventKind, EventQueue, ScheduledEvent};
use history::EventWindow;

type EvalFn =
    Box<dyn for<'a> Fn(&EvalContext<'a>) -> Result<EvalOutput, EvalError> + Send + Sync + 'static>;
type ReactionFn = Box<
    dyn for<'a> Fn(&EvalContext<'a>) -> Result<ReactionOutput, EvalError> + Send + Sync + 'static,
>;
enum PreparedTransition {
    Immediate {
        writes: Vec<ResolvedWrite>,
        window_records: Vec<WindowRecord>,
    },
    Delayed {
        at: SimTime,
        writes: Vec<StateWrite>,
        window_records: Vec<WindowRecord>,
    },
}

struct PreparedCallback {
    reaction: ReactionId,
    at: SimTime,
    key: u64,
    priority: i32,
}

struct ResolvedWrite {
    node: NodeId,
    next: CanonicalValue,
}

enum ValidatedWrite {
    Numeric {
        quantization: Quantization,
        bucket: Bucket,
        value: f64,
    },
    Atom {
        value: u64,
    },
}

impl ValidatedWrite {
    fn resolve(self, node: NodeId) -> Result<ResolvedWrite, RuntimeError> {
        let next = match self {
            Self::Numeric {
                quantization,
                bucket,
                value,
            } => {
                CanonicalValue::Numeric(quantization.resolve(Some(bucket), Estimate::point(value))?)
            }
            Self::Atom { value } => CanonicalValue::Atom(value),
        };
        Ok(ResolvedWrite { node, next })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RuntimeConfig {
    /// Maximum event deliveries and evaluations per timestamp within one settlement call.
    pub same_time_microstep_budget: usize,
}
impl Default for RuntimeConfig {
    fn default() -> Self {
        Self {
            same_time_microstep_budget: 4_096,
        }
    }
}

/// Why the current evaluation was requested.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Invocation {
    Reactive,
    Forecast,
    Scheduled { key: u64 },
    External { token: Option<u64> },
}

pub struct EvalContext<'a> {
    now: SimTime,
    nodes: &'a [Node],
    windows: &'a [EventWindow],
    reads: RefCell<BTreeSet<NodeId>>,
    track_dependencies: bool,
    invocation: Invocation,
}
impl EvalContext<'_> {
    #[must_use = "the current simulation time must be used"]
    pub const fn now(&self) -> SimTime {
        self.now
    }

    /// Key of the keyed scheduled callback that caused this evaluation, when applicable.
    #[must_use = "the scheduled key must be considered by the caller"]
    pub const fn scheduled_key(&self) -> Option<u64> {
        match self.invocation {
            Invocation::Scheduled { key } => Some(key),
            _ => None,
        }
    }

    /// Source-defined token attached to an unkeyed external event, when applicable.
    #[must_use = "the external token must be considered by the caller"]
    pub const fn external_token(&self) -> Option<u64> {
        match self.invocation {
            Invocation::External { token } => token,
            _ => None,
        }
    }
    /// Distinguishes tokenless external events from reactive evaluation.
    #[must_use = "the invocation cause must be considered by the caller"]
    pub const fn invocation(&self) -> Invocation {
        self.invocation
    }

    /// # Errors
    /// Returns an error if the node does not exist or is not numeric.
    pub fn read_bucket(&self, id: NodeId) -> Result<Bucket, EvalError> {
        self.read_state(id)?
            .numeric()
            .map(|(_, bucket)| bucket)
            .ok_or(EvalError::ExpectedNumeric(id))
    }
    /// # Errors
    /// Returns an error if the node does not exist or is not numeric.
    pub fn read_f64(&self, id: NodeId) -> Result<f64, EvalError> {
        self.read_state(id)?
            .numeric()
            .map(|(quantization, bucket)| quantization.decode(bucket))
            .ok_or(EvalError::ExpectedNumeric(id))
    }
    /// # Errors
    /// Returns an error if the node does not exist or is not numeric.
    pub fn read_bool(&self, id: NodeId) -> Result<bool, EvalError> {
        Ok(self.read_bucket(id)?.raw() != 0)
    }
    /// # Errors
    /// Returns an error if the node does not exist or is not an atom.
    pub fn read_atom(&self, id: NodeId) -> Result<u64, EvalError> {
        self.read_state(id)?
            .atom()
            .ok_or(EvalError::ExpectedAtom(id))
    }
    /// # Errors
    /// Returns an error if the temporal history does not exist.
    pub fn window_count(&self, id: HistoryId, key: u64) -> Result<usize, EvalError> {
        let window = self
            .windows
            .get(id.index())
            .ok_or(EvalError::UnknownHistory(id))?;
        Ok(window.count_at(self.now, key))
    }

    fn read_state(&self, id: NodeId) -> Result<&NodeState, EvalError> {
        let node = self
            .nodes
            .get(id.index())
            .ok_or(EvalError::UnknownNode(id))?;
        if self.track_dependencies {
            self.reads.borrow_mut().insert(id);
        }
        Ok(&node.state)
    }

    fn into_dependencies(self) -> BTreeSet<NodeId> {
        self.reads.into_inner()
    }
}

enum NodeState {
    Numeric {
        quantization: Quantization,
        bucket: Bucket,
        evaluator: Option<EvalFn>,
    },
    Atom(u64),
}

impl NodeState {
    const fn numeric(&self) -> Option<(Quantization, Bucket)> {
        match self {
            Self::Numeric {
                quantization,
                bucket,
                ..
            } => Some((*quantization, *bucket)),
            Self::Atom(_) => None,
        }
    }

    const fn atom(&self) -> Option<u64> {
        match self {
            Self::Atom(value) => Some(*value),
            Self::Numeric { .. } => None,
        }
    }

    const fn is_derived(&self) -> bool {
        matches!(
            self,
            Self::Numeric {
                evaluator: Some(_),
                ..
            }
        )
    }

    const fn canonical(&self) -> CanonicalValue {
        match self {
            Self::Numeric { bucket, .. } => CanonicalValue::Numeric(*bucket),
            Self::Atom(value) => CanonicalValue::Atom(*value),
        }
    }
}

struct Node {
    name: Arc<str>,
    state: NodeState,
    dependencies: BTreeSet<NodeId>,
    dependents: BTreeSet<WorkId>,
    generation: u64,
}

struct Reaction {
    name: String,
    evaluator: ReactionFn,
    dependencies: BTreeSet<NodeId>,
    generation: u64,
    track_dependencies: bool,
    pending_invocation: Invocation,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum WorkId {
    Node(NodeId),
    Reaction(ReactionId),
}

/// FIFO work queue with one membership bit per registered graph item.
struct DirtyQueue {
    pending: VecDeque<WorkId>,
    nodes: Vec<bool>,
    reactions: Vec<bool>,
}

impl DirtyQueue {
    const fn new() -> Self {
        Self {
            pending: VecDeque::new(),
            nodes: Vec::new(),
            reactions: Vec::new(),
        }
    }

    fn membership(&mut self, work: WorkId) -> &mut bool {
        match work {
            WorkId::Node(id) => &mut self.nodes[id.index()],
            WorkId::Reaction(id) => &mut self.reactions[id.index()],
        }
    }

    fn mark(&mut self, work: WorkId) {
        if !std::mem::replace(self.membership(work), true) {
            self.pending.push_back(work);
        }
    }

    fn pop(&mut self) -> Option<WorkId> {
        let work = self.pending.pop_front()?;
        *self.membership(work) = false;
        Some(work)
    }

    fn retry(&mut self, work: WorkId) {
        if !std::mem::replace(self.membership(work), true) {
            self.pending.push_front(work);
        }
    }
}

pub struct Graph {
    config: RuntimeConfig,
    now: SimTime,
    nodes: Vec<Node>,
    reactions: Vec<Reaction>,
    windows: Vec<EventWindow>,
    dirty: DirtyQueue,
    events: EventQueue,
    pending_scheduled_reactions: BTreeSet<(ReactionId, u64)>,
    transitions: Vec<TransitionRecord>,
}

impl Graph {
    #[must_use = "the constructed value must be used"]
    pub const fn new(config: RuntimeConfig) -> Self {
        Self {
            config,
            now: SimTime::ZERO,
            nodes: Vec::new(),
            reactions: Vec::new(),
            windows: Vec::new(),
            dirty: DirtyQueue::new(),
            events: EventQueue::new(),
            pending_scheduled_reactions: BTreeSet::new(),
            transitions: Vec::new(),
        }
    }
    #[must_use = "the current simulation time must be used"]
    pub const fn now(&self) -> SimTime {
        self.now
    }

    /// Update the per-timestamp settlement budget, including when retrying an exhausted call.
    pub const fn set_microstep_budget(&mut self, budget: usize) {
        self.config.same_time_microstep_budget = budget;
    }

    /// True only at an externally observable safe point: no dirty computation and no event due at
    /// the current simulation time. Future events do not prevent quiescence.
    #[must_use = "the quiescence result must be checked"]
    pub fn is_quiescent(&self) -> bool {
        self.dirty.pending.is_empty() && self.events.peek().is_none_or(|event| event.at > self.now)
    }

    /// Process all work and events that are causally due at the current simulation time.
    ///
    /// # Errors
    /// Propagates evaluation, scheduling, quantization, or calculation-instability errors.
    pub fn quiesce(&mut self) -> Result<(), RuntimeError> {
        self.advance_to(self.now)
    }

    /// Capture immutable canonical state at a quiescent boundary.
    ///
    /// # Errors
    /// Returns an error if the graph is not currently quiescent.
    pub fn quiescent_snapshot(&self) -> Result<QuiescentSnapshot, RuntimeError> {
        if !self.is_quiescent() {
            return Err(self.not_quiescent_error());
        }
        Ok(QuiescentSnapshot {
            at: self.now,
            next_event_time: self.events.peek().map(|event| event.at),
            values: self
                .nodes
                .iter()
                .map(|node| node.state.canonical())
                .collect(),
        })
    }

    /// Import a finite FIFO prefix, admitting valid interactions and returning
    /// invalid ones to the caller with their errors. A single bad producer
    /// request cannot block later requests from other producers. No evaluator
    /// executes in this method; future events remain deferred.
    ///
    /// `QuiescentNow` is resolved to the same owner time for this entire prefix.
    /// The inbox mutex is released before validation, so producers can enqueue
    /// the next prefix concurrently.
    ///
    /// # Errors
    /// Returns an error if the graph is not quiescent or the ingress queue lock is poisoned.
    pub fn import_interactions(
        &mut self,
        inbox: &InteractionInbox,
    ) -> Result<InteractionImport, RuntimeError> {
        if !self.is_quiescent() {
            return Err(self.not_quiescent_error());
        }
        let interactions = inbox
            .drain_pending()
            .map_err(RuntimeError::InteractionIngress)?;

        let mut result = InteractionImport::default();
        for interaction in interactions {
            match self.validate_external_interaction(&interaction) {
                Ok(()) => {
                    self.enqueue_external_interaction(interaction);
                    result.accepted += 1;
                }
                Err(error) => result
                    .rejected
                    .push(RejectedInteraction { interaction, error }),
            }
        }
        Ok(result)
    }

    /// Quiesce, admit all valid currently queued external interactions, and
    /// process every interaction due at the current time. Invalid interactions
    /// are returned separately; future events remain in the causal event queue.
    ///
    /// # Errors
    /// Returns an `InteractionProcessing` error containing the full import
    /// report if settlement fails after admission; no rejected item is lost.
    /// Other errors cover pre-import quiescence and ingress-lock failures.
    pub fn process_interactions(
        &mut self,
        inbox: &InteractionInbox,
    ) -> Result<InteractionImport, RuntimeError> {
        self.quiesce()?;
        let result = self.import_interactions(inbox)?;
        match self.quiesce() {
            Ok(()) => Ok(result),
            Err(source) => Err(RuntimeError::InteractionProcessing {
                import: result,
                source: Box::new(source),
            }),
        }
    }

    /// Drain committed transition records together with a stable observable snapshot.
    ///
    /// # Errors
    /// Returns `RuntimeError::NotQuiescent` if causal work is still due at the current time.
    pub fn publish_quiescent(&mut self) -> Result<QuiescentPublication, RuntimeError> {
        let snapshot = self.quiescent_snapshot()?;
        Ok(QuiescentPublication {
            snapshot,
            transitions: std::mem::take(&mut self.transitions),
        })
    }

    /// Process currently visible interactions and publish the resulting stable state in one worker
    /// operation.
    ///
    /// # Errors
    /// Propagates quiescence, interaction validation/import, and evaluation errors.
    pub fn process_interactions_and_publish(
        &mut self,
        inbox: &InteractionInbox,
    ) -> Result<(InteractionImport, QuiescentPublication), RuntimeError> {
        let result = self.process_interactions(inbox)?;
        Ok((result, self.publish_quiescent()?))
    }

    /// Advance to a requested simulation time and publish the resulting quiescent state.
    ///
    /// # Errors
    /// Propagates temporal, evaluation, and quiescence errors.
    pub fn advance_to_and_publish(
        &mut self,
        target: SimTime,
    ) -> Result<QuiescentPublication, RuntimeError> {
        self.advance_to(target)?;
        self.publish_quiescent()
    }

    /// # Errors
    /// Returns an error if the graph cannot allocate another temporal history identifier.
    pub fn add_event_window(
        &mut self,
        name: impl Into<String>,
        duration: SimDuration,
    ) -> Result<HistoryId, RuntimeError> {
        let raw_id =
            u32::try_from(self.windows.len()).map_err(|_| RuntimeError::TooManyHistories)?;
        let id = HistoryId::from_index(raw_id);
        self.windows.push(EventWindow::new(name.into(), duration));
        Ok(id)
    }

    /// # Errors
    /// Returns an error if the graph cannot allocate another node or the initial value cannot be quantized.
    pub fn add_input(
        &mut self,
        name: impl Into<Arc<str>>,
        q: Quantization,
        initial: f64,
    ) -> Result<NodeId, RuntimeError> {
        self.add_numeric_node(name, q, initial, None)
    }

    /// # Errors
    /// Returns an error if the graph cannot allocate another node identifier.
    pub fn add_atom_input(
        &mut self,
        name: impl Into<Arc<str>>,
        initial: u64,
    ) -> Result<NodeId, RuntimeError> {
        self.add_node(name, NodeState::Atom(initial))
    }

    /// # Errors
    /// Returns an error if the graph cannot allocate another node or the initial value cannot be quantized.
    pub fn add_derived<F>(
        &mut self,
        name: impl Into<Arc<str>>,
        q: Quantization,
        initial: f64,
        evaluator: F,
    ) -> Result<NodeId, RuntimeError>
    where
        F: for<'a> Fn(&EvalContext<'a>) -> Result<EvalOutput, EvalError> + Send + Sync + 'static,
    {
        self.add_numeric_node(name, q, initial, Some(Box::new(evaluator)))
    }

    fn add_numeric_node(
        &mut self,
        name: impl Into<Arc<str>>,
        q: Quantization,
        initial: f64,
        evaluator: Option<EvalFn>,
    ) -> Result<NodeId, RuntimeError> {
        let bucket = q.resolve(None, Estimate::point(initial))?;
        self.add_node(
            name,
            NodeState::Numeric {
                quantization: q,
                bucket,
                evaluator,
            },
        )
    }

    fn add_node(
        &mut self,
        name: impl Into<Arc<str>>,
        state: NodeState,
    ) -> Result<NodeId, RuntimeError> {
        let raw_id = u32::try_from(self.nodes.len()).map_err(|_| RuntimeError::TooManyNodes)?;
        let id = NodeId::from_index(raw_id);
        let derived = state.is_derived();
        self.nodes.push(Node {
            name: name.into(),
            state,
            dependencies: BTreeSet::new(),
            dependents: BTreeSet::new(),
            generation: 0,
        });
        self.dirty.nodes.push(false);
        if derived {
            self.dirty.mark(WorkId::Node(id));
        }
        Ok(id)
    }

    /// # Errors
    /// Returns an error if the node does not exist or is not numeric.
    pub fn set_evaluator<F>(&mut self, id: NodeId, evaluator: F) -> Result<(), RuntimeError>
    where
        F: for<'a> Fn(&EvalContext<'a>) -> Result<EvalOutput, EvalError> + Send + Sync + 'static,
    {
        let node = self
            .nodes
            .get_mut(id.index())
            .ok_or(RuntimeError::UnknownNode(id))?;
        let NodeState::Numeric {
            evaluator: slot, ..
        } = &mut node.state
        else {
            return Err(RuntimeError::ExpectedNumeric(id));
        };
        *slot = Some(Box::new(evaluator));
        self.dirty.mark(WorkId::Node(id));
        Ok(())
    }

    /// # Errors
    /// Returns an error if the graph cannot allocate another reaction identifier.
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
        self.register_reaction(name.into(), Box::new(evaluator), true)
    }

    /// Add an event-only callback reaction.
    ///
    /// Event callbacks are dormant until explicitly scheduled. Reads performed while they run are
    /// intentionally not registered as reactive dependencies, so observing current state during a
    /// callback cannot cause later state changes to invoke that callback spontaneously.
    ///
    /// # Errors
    /// Returns an error if the graph cannot allocate another reaction identifier.
    pub fn add_event_callback<F>(
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
        self.register_reaction(name.into(), Box::new(evaluator), false)
    }

    fn register_reaction(
        &mut self,
        name: String,
        evaluator: ReactionFn,
        track_dependencies: bool,
    ) -> Result<ReactionId, RuntimeError> {
        let raw_id =
            u32::try_from(self.reactions.len()).map_err(|_| RuntimeError::TooManyReactions)?;
        let id = ReactionId::from_index(raw_id);
        self.reactions.push(Reaction {
            name,
            evaluator,
            dependencies: BTreeSet::new(),
            generation: 0,
            track_dependencies,
            pending_invocation: Invocation::Reactive,
        });
        self.dirty.reactions.push(false);
        if track_dependencies {
            self.dirty.mark(WorkId::Reaction(id));
        }
        Ok(id)
    }

    /// # Errors
    /// Returns an error if the reaction does not exist.
    pub fn reaction_name(&self, id: ReactionId) -> Result<&str, RuntimeError> {
        Ok(&self
            .reactions
            .get(id.index())
            .ok_or(RuntimeError::UnknownReaction(id))?
            .name)
    }

    /// # Errors
    /// Returns an error if the temporal history does not exist.
    pub fn event_window_name(&self, id: HistoryId) -> Result<&str, RuntimeError> {
        Ok(&self
            .windows
            .get(id.index())
            .ok_or(RuntimeError::UnknownHistory(id))?
            .name)
    }

    /// # Errors
    /// Returns an error if the temporal history does not exist.
    pub fn event_window_count(&self, id: HistoryId, key: u64) -> Result<usize, RuntimeError> {
        let window = self
            .windows
            .get(id.index())
            .ok_or(RuntimeError::UnknownHistory(id))?;
        Ok(window.count_at(self.now, key))
    }

    /// # Errors
    /// Returns an error if the node does not exist or is not numeric.
    pub fn value_bucket(&self, id: NodeId) -> Result<Bucket, RuntimeError> {
        self.node(id)?
            .state
            .numeric()
            .map(|(_, bucket)| bucket)
            .ok_or(RuntimeError::ExpectedNumeric(id))
    }
    /// # Errors
    /// Returns an error if the node does not exist or is not numeric.
    pub fn value_f64(&self, id: NodeId) -> Result<f64, RuntimeError> {
        self.node(id)?
            .state
            .numeric()
            .map(|(quantization, bucket)| quantization.decode(bucket))
            .ok_or(RuntimeError::ExpectedNumeric(id))
    }
    /// # Errors
    /// Returns an error if the node does not exist or is not an atom.
    pub fn value_atom(&self, id: NodeId) -> Result<u64, RuntimeError> {
        self.node(id)?
            .state
            .atom()
            .ok_or(RuntimeError::ExpectedAtom(id))
    }

    fn not_quiescent_error(&self) -> RuntimeError {
        RuntimeError::NotQuiescent {
            at: self.now,
            dirty_work: self.dirty.pending.len(),
            due_event: self
                .events
                .peek()
                .filter(|event| event.at <= self.now)
                .map(|event| event.at),
        }
    }

    fn validate_external_interaction(
        &self,
        interaction: &ExternalInteraction,
    ) -> Result<(), RuntimeError> {
        let moment = match interaction {
            ExternalInteraction::StateBatch { moment, .. }
            | ExternalInteraction::Callback { moment, .. } => *moment,
        };
        let at = moment.resolve(self.now);
        self.validate_time(at)?;
        match interaction {
            ExternalInteraction::StateBatch { writes, .. } => {
                self.validate_state_write_batch(writes)
            }
            ExternalInteraction::Callback { reaction, .. } => {
                self.reactions
                    .get(reaction.index())
                    .ok_or(RuntimeError::UnknownReaction(*reaction))?;
                Ok(())
            }
        }
    }

    fn validated_writes<'a>(
        &'a self,
        writes: &'a [StateWrite],
    ) -> impl Iterator<Item = Result<(NodeId, ValidatedWrite), RuntimeError>> + 'a {
        let mut seen = BTreeSet::new();
        writes.iter().map(move |&write| {
            let id = write.node();
            let node = self.node(id)?;
            if writes.len() > 1 && !seen.insert(id) {
                return Err(RuntimeError::DuplicateStateWrite(id));
            }
            Self::validate_write(node, write).map(|validated| (id, validated))
        })
    }

    fn validate_write(node: &Node, write: StateWrite) -> Result<ValidatedWrite, RuntimeError> {
        let id = write.node();
        if node.state.is_derived() {
            return Err(RuntimeError::NotAnInput(id));
        }
        let validated = match (write, &node.state) {
            (
                StateWrite::Numeric { value, .. },
                NodeState::Numeric {
                    quantization,
                    bucket,
                    ..
                },
            ) => {
                if !value.is_finite() {
                    return Err(QuantizeError::NonFiniteEstimate.into());
                }
                ValidatedWrite::Numeric {
                    quantization: *quantization,
                    bucket: *bucket,
                    value,
                }
            }
            (StateWrite::Atom { value, .. }, NodeState::Atom(_)) => ValidatedWrite::Atom { value },
            (StateWrite::Numeric { .. }, NodeState::Atom(_)) => {
                return Err(RuntimeError::ExpectedNumeric(id));
            }
            (StateWrite::Atom { .. }, NodeState::Numeric { .. }) => {
                return Err(RuntimeError::ExpectedAtom(id));
            }
        };
        Ok(validated)
    }

    fn validate_state_write_batch(&self, writes: &[StateWrite]) -> Result<(), RuntimeError> {
        // Quantization depends on the then-current hysteresis state and must
        // remain at delivery time.
        self.validated_writes(writes)
            .try_for_each(|write| write.map(|_| ()))
    }

    fn enqueue_external_interaction(&mut self, interaction: ExternalInteraction) {
        match interaction {
            ExternalInteraction::StateBatch {
                moment,
                priority,
                writes,
            } => self.push_event_with_priority(
                moment.resolve(self.now),
                priority,
                EventKind::SetStateBatch { writes },
            ),
            ExternalInteraction::Callback {
                moment,
                priority,
                reaction,
                token,
            } => self.push_event_with_priority(
                moment.resolve(self.now),
                priority,
                EventKind::ExternalReaction { reaction, token },
            ),
        }
    }

    /// # Errors
    /// Propagates node validation, quantization, evaluation, and calculation-instability errors.
    pub fn set_input_now(&mut self, id: NodeId, value: f64) -> Result<(), RuntimeError> {
        self.set_state_now(&[StateWrite::new(id, value)])
    }

    /// # Errors
    /// Propagates node validation, evaluation, and calculation-instability errors.
    pub fn set_atom_now(&mut self, id: NodeId, value: u64) -> Result<(), RuntimeError> {
        self.set_state_now(&[StateWrite::atom(id, value)])
    }

    /// Apply a batch atomically, then settle dirty computations without delivering queued events.
    ///
    /// # Errors
    /// Returns validation, quantization, evaluation, or calculation-instability errors.
    /// An evaluation error after the batch commits does not roll back the input batch.
    pub fn set_state_now(&mut self, writes: &[StateWrite]) -> Result<(), RuntimeError> {
        self.apply_state_writes(writes)?;
        self.settle()
    }

    /// Schedule a numeric input write.
    ///
    /// # Errors
    /// Rejects invalid input targets, non-finite values, and past timestamps.
    pub fn schedule_input(
        &mut self,
        id: NodeId,
        at: SimTime,
        value: f64,
    ) -> Result<(), RuntimeError> {
        self.schedule_write(at, StateWrite::new(id, value))
    }

    /// Schedule an atom input write.
    ///
    /// # Errors
    /// Rejects invalid input targets and past timestamps.
    pub fn schedule_atom(
        &mut self,
        id: NodeId,
        at: SimTime,
        value: u64,
    ) -> Result<(), RuntimeError> {
        self.schedule_write(at, StateWrite::atom(id, value))
    }

    /// Schedule a single typed input write without allocating a batch.
    ///
    /// # Errors
    /// Rejects invalid input targets, non-finite numeric values, and past timestamps.
    pub fn schedule_write(&mut self, at: SimTime, write: StateWrite) -> Result<(), RuntimeError> {
        self.validate_state_write_batch(&[write])?;
        self.validate_time(at)?;
        self.push_event(at, EventKind::SetState { write });
        Ok(())
    }

    /// Schedule one externally sourced reaction callback.
    ///
    /// External events are intentionally not deduplicated and are not tied to the reaction's
    /// dependency generation. Each accepted event causes one reevaluation when dequeued.
    ///
    /// # Errors
    ///
    /// Returns an error for an unknown reaction or an event in the past.
    pub fn schedule_reaction_event(
        &mut self,
        reaction: ReactionId,
        at: SimTime,
        priority: i32,
    ) -> Result<(), RuntimeError> {
        self.schedule_callback(reaction, at, priority, None)
    }

    /// Schedule one externally sourced reaction callback with an opaque source token.
    ///
    /// Tokens are delivered per event and do not participate in deduplication. Multiple events for
    /// the same reaction and token remain distinct callbacks.
    ///
    /// # Errors
    ///
    /// Returns an error for an unknown reaction or an event in the past.
    pub fn schedule_reaction_event_with_token(
        &mut self,
        reaction: ReactionId,
        at: SimTime,
        priority: i32,
        token: u64,
    ) -> Result<(), RuntimeError> {
        self.schedule_callback(reaction, at, priority, Some(token))
    }

    fn schedule_callback(
        &mut self,
        reaction: ReactionId,
        at: SimTime,
        priority: i32,
        token: Option<u64>,
    ) -> Result<(), RuntimeError> {
        self.reactions
            .get(reaction.index())
            .ok_or(RuntimeError::UnknownReaction(reaction))?;
        self.validate_time(at)?;
        self.push_event_with_priority(
            at,
            priority,
            EventKind::ExternalReaction { reaction, token },
        );
        Ok(())
    }

    fn validate_time(&self, at: SimTime) -> Result<(), RuntimeError> {
        if at < self.now {
            return Err(RuntimeError::EventInPast {
                now: self.now,
                requested: at,
            });
        }
        Ok(())
    }

    /// Process dirty computations without delivering queued events.
    ///
    /// # Errors
    /// Propagates evaluation, scheduling, quantization, and calculation-instability errors.
    pub fn settle(&mut self) -> Result<(), RuntimeError> {
        let mut steps = 0;
        self.settle_with_budget(&mut steps)
    }

    fn settle_with_budget(&mut self, steps: &mut usize) -> Result<(), RuntimeError> {
        while let Some(&work) = self.dirty.pending.front() {
            if *steps >= self.config.same_time_microstep_budget {
                return Err(RuntimeError::CalculationInstability {
                    at: self.now,
                    microsteps: *steps,
                    active_work: self.dirty.pending.iter().copied().collect(),
                });
            }
            *steps += 1;
            self.dirty.pop();
            let result = match work {
                WorkId::Node(id) => self.evaluate(id),
                WorkId::Reaction(id) => self.evaluate_reaction(id),
            };
            if let Err(error) = result {
                self.dirty.retry(work);
                return Err(error);
            }
        }
        Ok(())
    }

    /// # Errors
    /// Returns an error for backwards time travel or if processing due work fails.
    pub fn advance_to(&mut self, target: SimTime) -> Result<(), RuntimeError> {
        self.validate_time(target)?;
        let mut steps = 0;
        self.settle_with_budget(&mut steps)?;
        while self.events.peek().is_some_and(|e| e.at <= target) {
            let event_time = self
                .events
                .peek()
                .ok_or(RuntimeError::CorruptEventQueue)?
                .at;
            if event_time > self.now {
                steps = 0;
            }
            if steps >= self.config.same_time_microstep_budget {
                return Err(RuntimeError::CalculationInstability {
                    at: event_time,
                    microsteps: steps,
                    active_work: self.dirty.pending.iter().copied().collect(),
                });
            }
            steps += 1;
            let event = self.events.pop().ok_or(RuntimeError::CorruptEventQueue)?;
            self.now = event.at;
            if let Err(error) = self.apply_event(&event.kind) {
                self.events.restore_front(event);
                return Err(error);
            }
            self.settle_with_budget(&mut steps)?;
        }
        self.now = target;
        Ok(())
    }

    #[must_use = "the next event time must be considered before advancing"]
    pub fn next_event_time(&self) -> Option<SimTime> {
        self.events.peek().map(|event| event.at)
    }

    #[must_use = "discarding the returned records permanently drains the transition log"]
    pub fn take_transitions(&mut self) -> Vec<TransitionRecord> {
        std::mem::take(&mut self.transitions)
    }

    fn evaluate(&mut self, id: NodeId) -> Result<(), RuntimeError> {
        let NodeState::Numeric {
            quantization,
            bucket: current,
            evaluator,
        } = &self.nodes[id.index()].state
        else {
            return Err(RuntimeError::ExpectedNumeric(id));
        };
        let (quantization, current) = (*quantization, *current);
        let evaluator = evaluator
            .as_ref()
            .ok_or(RuntimeError::MissingEvaluator(id))?;
        let context = EvalContext {
            now: self.now,
            nodes: &self.nodes,
            windows: &self.windows,
            reads: RefCell::new(BTreeSet::new()),
            track_dependencies: true,
            invocation: Invocation::Reactive,
        };
        let output =
            evaluator(&context).map_err(|source| RuntimeError::Evaluation { node: id, source })?;

        let next = quantization.resolve(Some(current), output.estimate)?;
        let validity_at = output
            .valid_for
            .map(|duration| self.event_time(duration))
            .transpose()?;
        let commit_at = if next != current && output.delay != SimDuration::ZERO {
            Some(self.event_time(output.delay)?)
        } else {
            None
        };
        let generation = self.nodes[id.index()]
            .generation
            .checked_add(1)
            .ok_or(RuntimeError::GenerationExhausted(WorkId::Node(id)))?;
        let deps = context.into_dependencies();
        self.update_dependencies(WorkId::Node(id), deps)?;
        self.nodes[id.index()].generation = generation;

        if let Some(at) = validity_at {
            self.push_event(
                at,
                EventKind::Reevaluate {
                    node: id,
                    generation,
                },
            );
        }
        if next != current {
            if let Some(at) = commit_at {
                self.push_event(
                    at,
                    EventKind::Commit {
                        node: id,
                        generation,
                        value: CanonicalValue::Numeric(next),
                    },
                );
            } else {
                self.commit(id, CanonicalValue::Numeric(next))?;
            }
        }
        Ok(())
    }

    fn evaluate_reaction(&mut self, id: ReactionId) -> Result<(), RuntimeError> {
        let (evaluator, track_dependencies, invocation) = {
            let reaction = self
                .reactions
                .get(id.index())
                .ok_or(RuntimeError::UnknownReaction(id))?;
            (
                &reaction.evaluator,
                reaction.track_dependencies,
                reaction.pending_invocation,
            )
        };
        let context = EvalContext {
            now: self.now,
            nodes: &self.nodes,
            windows: &self.windows,
            reads: RefCell::new(BTreeSet::new()),
            track_dependencies,
            invocation,
        };
        let output = evaluator(&context).map_err(|source| RuntimeError::ReactionEvaluation {
            reaction: id,
            source,
        })?;
        let dependencies = if track_dependencies {
            Some(context.into_dependencies())
        } else {
            None
        };
        let ReactionOutput {
            transition,
            callbacks,
        } = output;

        // Prepare every effect before changing subscriptions or publishing state.
        let callbacks = self.prepare_callbacks(id, callbacks)?;
        let prepared = transition
            .map(|transition| self.prepare_transition(transition))
            .transpose()?;

        let generation = self.reactions[id.index()]
            .generation
            .checked_add(1)
            .ok_or(RuntimeError::GenerationExhausted(WorkId::Reaction(id)))?;
        if let Some(deps) = dependencies {
            self.update_dependencies(WorkId::Reaction(id), deps)?;
        }
        let reaction = self
            .reactions
            .get_mut(id.index())
            .ok_or(RuntimeError::UnknownReaction(id))?;
        reaction.pending_invocation = Invocation::Reactive;
        reaction.generation = generation;

        match prepared {
            Some(PreparedTransition::Immediate {
                writes,
                window_records,
            }) => {
                self.commit_resolved_state_writes(writes)?;
                self.record_windows(&window_records);
            }
            Some(PreparedTransition::Delayed {
                at,
                writes,
                window_records,
            }) => {
                self.push_event(
                    at,
                    EventKind::ReactionTransition {
                        reaction: id,
                        generation,
                        writes,
                        window_records,
                    },
                );
            }
            None => {}
        }
        for callback in &callbacks {
            self.schedule_reaction_once(callback);
        }
        Ok(())
    }

    fn prepare_callbacks(
        &self,
        current: ReactionId,
        requests: Vec<ScheduledCallback>,
    ) -> Result<Vec<PreparedCallback>, RuntimeError> {
        let mut callbacks = Vec::with_capacity(requests.len());
        let mut seen = BTreeSet::new();
        let check_duplicates = requests.len() > 1;
        for request in requests {
            let reaction = request.target.resolve(current);
            self.reactions
                .get(reaction.index())
                .ok_or(RuntimeError::UnknownReaction(reaction))?;
            let key = (reaction, request.key);
            if self.pending_scheduled_reactions.contains(&key)
                || (check_duplicates && !seen.insert(key))
            {
                continue;
            }
            callbacks.push(PreparedCallback {
                reaction,
                at: self.event_time(request.after)?,
                key: request.key,
                priority: request.priority,
            });
        }
        Ok(callbacks)
    }

    fn prepare_transition(
        &self,
        transition: ReactionTransition,
    ) -> Result<PreparedTransition, RuntimeError> {
        self.validate_window_records(&transition.window_records)?;
        if transition.after == SimDuration::ZERO {
            Ok(PreparedTransition::Immediate {
                writes: self.resolve_state_writes(&transition.writes)?,
                window_records: transition.window_records,
            })
        } else {
            // Delayed writes resolve against canonical state at delivery.
            self.validate_state_write_batch(&transition.writes)?;
            Ok(PreparedTransition::Delayed {
                at: self.event_time(transition.after)?,
                writes: transition.writes,
                window_records: transition.window_records,
            })
        }
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
            WorkId::Node(id) => std::mem::take(
                &mut self
                    .nodes
                    .get_mut(id.index())
                    .ok_or(RuntimeError::UnknownNode(id))?
                    .dependencies,
            ),
            WorkId::Reaction(id) => std::mem::take(
                &mut self
                    .reactions
                    .get_mut(id.index())
                    .ok_or(RuntimeError::UnknownReaction(id))?
                    .dependencies,
            ),
        };
        for dep in old.difference(&new).copied() {
            self.nodes[dep.index()].dependents.remove(&dependent);
        }
        for dep in new.difference(&old).copied() {
            self.nodes[dep.index()].dependents.insert(dependent);
        }
        match dependent {
            WorkId::Node(id) => self.nodes[id.index()].dependencies = new,
            WorkId::Reaction(id) => self.reactions[id.index()].dependencies = new,
        }
        Ok(())
    }

    fn apply_event(&mut self, event: &EventKind) -> Result<(), RuntimeError> {
        match event {
            EventKind::SetState { write } => self.apply_state_writes(&[*write]),
            EventKind::SetStateBatch { writes } => self.apply_state_writes(writes),
            EventKind::Commit {
                node,
                generation,
                value,
            } => {
                self.node(*node)?;
                if self.nodes[node.index()].generation == *generation {
                    self.commit(*node, *value)?;
                }
                Ok(())
            }
            EventKind::Reevaluate { node, generation } => {
                self.node(*node)?;
                if self.nodes[node.index()].generation == *generation {
                    self.dirty.mark(WorkId::Node(*node));
                }
                Ok(())
            }
            EventKind::ExternalReaction { reaction, token } => {
                self.wake_reaction(*reaction, Invocation::External { token: *token })
            }
            EventKind::ScheduledReaction { reaction, key } => {
                self.wake_reaction(*reaction, Invocation::Scheduled { key: *key })?;
                self.pending_scheduled_reactions.remove(&(*reaction, *key));
                Ok(())
            }
            EventKind::ReactionTransition {
                reaction,
                generation,
                writes,
                window_records,
            } => {
                let current_generation = self
                    .reactions
                    .get(reaction.index())
                    .ok_or(RuntimeError::UnknownReaction(*reaction))?
                    .generation;
                if current_generation == *generation {
                    self.apply_reaction_effects(writes, window_records)?;
                    self.wake_reaction(*reaction, Invocation::Forecast)?;
                }
                Ok(())
            }
        }
    }

    fn wake_reaction(
        &mut self,
        reaction: ReactionId,
        invocation: Invocation,
    ) -> Result<(), RuntimeError> {
        self.reactions
            .get_mut(reaction.index())
            .ok_or(RuntimeError::UnknownReaction(reaction))?
            .pending_invocation = invocation;
        self.dirty.mark(WorkId::Reaction(reaction));
        Ok(())
    }

    fn apply_reaction_effects(
        &mut self,
        writes: &[StateWrite],
        window_records: &[WindowRecord],
    ) -> Result<(), RuntimeError> {
        self.validate_window_records(window_records)?;
        self.apply_state_writes(writes)?;
        self.record_windows(window_records);
        Ok(())
    }

    fn validate_window_records(&self, records: &[WindowRecord]) -> Result<(), RuntimeError> {
        for record in records {
            if self.windows.get(record.history.index()).is_none() {
                return Err(RuntimeError::UnknownHistory(record.history));
            }
        }
        Ok(())
    }

    fn record_windows(&mut self, records: &[WindowRecord]) {
        for record in records {
            self.windows[record.history.index()].record(self.now, record.key);
        }
    }

    fn apply_state_writes(&mut self, writes: &[StateWrite]) -> Result<(), RuntimeError> {
        if let [write] = writes {
            let id = write.node();
            let resolved = Self::validate_write(self.node(id)?, *write)?.resolve(id)?;
            return self.commit(resolved.node, resolved.next);
        }
        let resolved = self.resolve_state_writes(writes)?;
        self.commit_resolved_state_writes(resolved)
    }

    fn resolve_state_writes(
        &self,
        writes: &[StateWrite],
    ) -> Result<Vec<ResolvedWrite>, RuntimeError> {
        self.validated_writes(writes)
            .map(|write| {
                let (node, validated) = write?;
                validated.resolve(node)
            })
            .collect()
    }

    fn commit_resolved_state_writes(
        &mut self,
        resolved: Vec<ResolvedWrite>,
    ) -> Result<(), RuntimeError> {
        for ResolvedWrite { node, next } in resolved {
            self.commit(node, next)?;
        }
        Ok(())
    }

    fn commit(&mut self, id: NodeId, value: CanonicalValue) -> Result<(), RuntimeError> {
        let node = &mut self.nodes[id.index()];
        let old = node.state.canonical();
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
            node_name: Arc::clone(&node.name),
            old,
            new: value,
        });
        for &dependent in &node.dependents {
            self.dirty.mark(dependent);
        }
        Ok(())
    }

    fn schedule_reaction_once(&mut self, callback: &PreparedCallback) {
        self.pending_scheduled_reactions
            .insert((callback.reaction, callback.key));
        self.push_event_with_priority(
            callback.at,
            callback.priority,
            EventKind::ScheduledReaction {
                reaction: callback.reaction,
                key: callback.key,
            },
        );
    }

    fn event_time(&self, duration: SimDuration) -> Result<SimTime, RuntimeError> {
        self.now
            .checked_add(duration)
            .ok_or(RuntimeError::TimeOverflow {
                now: self.now,
                duration,
            })
    }

    fn push_event(&mut self, at: SimTime, kind: EventKind) {
        self.push_event_with_priority(at, 0, kind);
    }

    fn push_event_with_priority(&mut self, at: SimTime, priority: i32, kind: EventKind) {
        self.events.push(ScheduledEvent {
            at,
            priority,
            sequence: 0,
            kind,
        });
    }

    fn node(&self, id: NodeId) -> Result<&Node, RuntimeError> {
        self.nodes
            .get(id.index())
            .ok_or(RuntimeError::UnknownNode(id))
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TransitionRecord {
    pub at: SimTime,
    pub node: NodeId,
    /// Shared immutable node label, retained after the graph is dropped.
    pub node_name: Arc<str>,
    pub old: CanonicalValue,
    pub new: CanonicalValue,
}

#[cfg(test)]
mod tests;

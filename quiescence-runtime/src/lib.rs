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
    Bucket, Estimate, HistoryId, NodeId, Quantization, ReactionId, SimDuration, SimTime,
};
use std::{
    cell::RefCell,
    collections::{BTreeSet, VecDeque},
    sync::Arc,
};

mod edge_set;
mod error;
mod event_queue;
mod history;
mod ingress;
mod node_storage;
mod output;
mod read_set;
mod write_batch;

pub use error::{EvalError, InteractionIngressError, RuntimeError};
pub use ingress::{
    ExternalInteraction, InteractionImport, InteractionInbox, InteractionMoment, InteractionSender,
    RejectedInteraction, interaction_channel,
};
pub use output::{
    CallbackTarget, CanonicalValue, EvalOutput, QuiescentPublication, QuiescentSnapshot,
    ReactionOutput, ReactionTransition, ScheduledCallback, StateWrite, WindowRecord,
};

use edge_set::ReverseEdges;
use event_queue::{DeferredEffects, EventKind, EventQueue, ForecastOwner, ScheduledEvent};
use history::EventWindow;
use node_storage::{DerivedId, DerivedNode, NodeLinks, NodeState, NumericCell};
use read_set::{Dependencies, ReadTracker};
use write_batch::ResolvedWrite;

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
        effects: DeferredEffects,
    },
}

struct PreparedCallback {
    reaction: ReactionId,
    at: SimTime,
    key: u64,
    priority: i32,
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
    values: &'a [NodeState],
    windows: &'a [EventWindow],
    reads: Option<RefCell<&'a mut ReadTracker>>,
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
            .bucket()
            .ok_or(EvalError::ExpectedNumeric(id))
    }
    /// # Errors
    /// Returns an error if the node does not exist or is not numeric.
    pub fn read_f64(&self, id: NodeId) -> Result<f64, EvalError> {
        self.read_state(id)?
            .decoded()
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
            .values
            .get(id.index())
            .ok_or(EvalError::UnknownNode(id))?;
        if let Some(reads) = &self.reads {
            reads.borrow_mut().record(id);
        }
        Ok(node)
    }
}

struct Reaction {
    name: String,
    evaluator: ReactionFn,
    dependencies: Dependencies,
    generation: u64,
    pending_forecasts: u8,
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
    values: Vec<NodeState>,
    links: Vec<NodeLinks>,
    derived: Vec<DerivedNode>,
    reactions: Vec<Reaction>,
    windows: Vec<EventWindow>,
    dirty: DirtyQueue,
    reads: ReadTracker,
    write_scratch: Vec<ResolvedWrite>,
    events: EventQueue,
    stale_forecasts: usize,
    pending_scheduled_reactions: BTreeSet<(ReactionId, u64)>,
    transitions: Vec<TransitionRecord>,
}

impl Graph {
    #[must_use = "the constructed value must be used"]
    pub const fn new(config: RuntimeConfig) -> Self {
        Self {
            config,
            now: SimTime::ZERO,
            values: Vec::new(),
            links: Vec::new(),
            derived: Vec::new(),
            reactions: Vec::new(),
            windows: Vec::new(),
            dirty: DirtyQueue::new(),
            reads: ReadTracker::new(),
            write_scratch: Vec::new(),
            events: EventQueue::new(),
            stale_forecasts: 0,
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
            values: self.values.iter().map(NodeState::canonical).collect(),
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
        let cell = Self::numeric_cell(q, initial)?;
        self.add_node(name, NodeState::Input(cell))
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
        self.add_derived_node(name, q, initial, Some(Box::new(evaluator)))
    }

    /// Reserve a derived node's identity before its evaluator is defined.
    ///
    /// This supports mutually recursive graphs without temporarily exposing
    /// derived state as externally writable input. The node is dirty from
    /// registration; settlement fails until its evaluator is installed.
    ///
    /// # Errors
    /// Returns an error if another node cannot be allocated or the initial value cannot be quantized.
    pub fn declare_derived(
        &mut self,
        name: impl Into<Arc<str>>,
        q: Quantization,
        initial: f64,
    ) -> Result<NodeId, RuntimeError> {
        self.add_derived_node(name, q, initial, None)
    }

    fn add_derived_node(
        &mut self,
        name: impl Into<Arc<str>>,
        q: Quantization,
        initial: f64,
        evaluator: Option<EvalFn>,
    ) -> Result<NodeId, RuntimeError> {
        let cell = Self::numeric_cell(q, initial)?;
        let raw = u32::try_from(self.derived.len()).map_err(|_| RuntimeError::TooManyNodes)?;
        let owner = DerivedId::new(raw);
        let node = self.add_node(name, NodeState::Derived { cell, owner })?;
        self.derived.push(DerivedNode {
            node,
            evaluator,
            dependencies: Dependencies::new(),
            generation: 0,
            pending_forecasts: 0,
        });
        Ok(node)
    }

    fn numeric_cell(q: Quantization, initial: f64) -> Result<NumericCell, RuntimeError> {
        let bucket = q.resolve(None, Estimate::point(initial))?;
        Ok(NumericCell {
            quantization: q,
            bucket,
        })
    }

    fn add_node(
        &mut self,
        name: impl Into<Arc<str>>,
        state: NodeState,
    ) -> Result<NodeId, RuntimeError> {
        let raw_id = u32::try_from(self.values.len()).map_err(|_| RuntimeError::TooManyNodes)?;
        let id = NodeId::from_index(raw_id);
        let derived = state.is_derived();
        self.values.push(state);
        self.links.push(NodeLinks {
            name: name.into(),
            dependents: ReverseEdges::new(),
        });
        self.dirty.nodes.push(false);
        self.reads.register_node();
        if derived {
            self.dirty.mark(WorkId::Node(id));
        }
        Ok(id)
    }

    /// Define or replace the evaluator of a derived node.
    ///
    /// Input nodes cannot change roles: accepted input writes must remain
    /// deliverable throughout the lifetime of their target. A replacement
    /// evaluator is scheduled for settlement and replaces its old forecasts
    /// only after successful evaluation.
    ///
    /// # Errors
    /// Returns an error if the node does not exist or is not derived.
    pub fn set_evaluator<F>(&mut self, id: NodeId, evaluator: F) -> Result<(), RuntimeError>
    where
        F: for<'a> Fn(&EvalContext<'a>) -> Result<EvalOutput, EvalError> + Send + Sync + 'static,
    {
        let owner = self.derived_owner(id)?;
        self.derived[owner.index()].evaluator = Some(Box::new(evaluator));
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
            dependencies: Dependencies::new(),
            generation: 0,
            pending_forecasts: 0,
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
        self.node_state(id)?
            .bucket()
            .ok_or(RuntimeError::ExpectedNumeric(id))
    }
    /// # Errors
    /// Returns an error if the node does not exist or is not numeric.
    pub fn value_f64(&self, id: NodeId) -> Result<f64, RuntimeError> {
        self.node_state(id)?
            .decoded()
            .ok_or(RuntimeError::ExpectedNumeric(id))
    }
    /// # Errors
    /// Returns an error if the node does not exist or is not an atom.
    pub fn value_atom(&self, id: NodeId) -> Result<u64, RuntimeError> {
        self.node_state(id)?
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
        &mut self,
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
        let result = self.settle_dirty(steps);
        self.prune_forecasts();
        result
    }

    fn settle_dirty(&mut self, steps: &mut usize) -> Result<(), RuntimeError> {
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
            if self.should_compact_forecasts() {
                self.prune_forecasts();
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
        while let Some(event_time) = self
            .events
            .peek()
            .map(|event| event.at)
            .filter(|&at| at <= target)
        {
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
            if let Some((work, _)) = event.kind.forecast() {
                *self.pending_forecasts(work) -= 1;
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
        let NodeState::Derived { cell, owner } = self.values[id.index()] else {
            return Err(RuntimeError::NotDerived(id));
        };
        let NumericCell {
            quantization,
            bucket: current,
        } = cell;
        let evaluator = self.derived[owner.index()]
            .evaluator
            .as_ref()
            .ok_or(RuntimeError::MissingEvaluator(id))?;
        self.reads.begin();
        let context = EvalContext {
            now: self.now,
            values: &self.values,
            windows: &self.windows,
            reads: Some(RefCell::new(&mut self.reads)),
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
        let computation = &mut self.derived[owner.index()];
        let generation = computation
            .generation
            .checked_add(1)
            .ok_or(RuntimeError::GenerationExhausted(WorkId::Node(id)))?;
        Self::update_dependencies(
            &mut self.links,
            &mut computation.dependencies,
            WorkId::Node(id),
            self.reads.finish(),
        );
        self.stale_forecasts += usize::from(computation.pending_forecasts);
        computation.pending_forecasts = 0;
        computation.generation = generation;

        if let Some(at) = validity_at {
            self.push_event(at, EventKind::Reevaluate { owner, generation });
        }
        if next != current {
            if let Some(at) = commit_at {
                self.push_event(
                    at,
                    EventKind::Commit {
                        owner,
                        generation,
                        bucket: next,
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
        let reads = track_dependencies.then(|| {
            self.reads.begin();
            RefCell::new(&mut self.reads)
        });
        let context = EvalContext {
            now: self.now,
            values: &self.values,
            windows: &self.windows,
            reads,
            invocation,
        };
        let output = evaluator(&context).map_err(|source| RuntimeError::ReactionEvaluation {
            reaction: id,
            source,
        })?;
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
        if track_dependencies {
            Self::update_dependencies(
                &mut self.links,
                &mut self.reactions[id.index()].dependencies,
                WorkId::Reaction(id),
                self.reads.finish(),
            );
        }
        let reaction = self
            .reactions
            .get_mut(id.index())
            .ok_or(RuntimeError::UnknownReaction(id))?;
        reaction.pending_invocation = Invocation::Reactive;
        self.stale_forecasts += usize::from(reaction.pending_forecasts);
        reaction.pending_forecasts = 0;
        reaction.generation = generation;

        match prepared {
            Some(PreparedTransition::Immediate {
                writes,
                window_records,
            }) => {
                self.commit_resolved_state_writes(writes)?;
                self.record_windows(&window_records);
            }
            Some(PreparedTransition::Delayed { at, effects }) => {
                self.push_event(
                    at,
                    EventKind::ReactionTransition {
                        reaction: id,
                        generation,
                        effects,
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
        &mut self,
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
                effects: DeferredEffects::new(transition.writes, transition.window_records),
            })
        }
    }

    fn update_dependencies(
        links: &mut [NodeLinks],
        stored: &mut Dependencies,
        dependent: WorkId,
        new: &[NodeId],
    ) {
        // Only existent IDs can be recorded; graph structure is immutable
        // throughout a single evaluator invocation.
        if stored.as_slice() == new {
            return;
        }
        let old = std::mem::replace(stored, Dependencies::from_sorted(new));
        read_set::changes(old.as_slice(), new, |id, added| {
            let reverse = &mut links[id.index()].dependents;
            if added {
                reverse.insert(dependent);
            } else {
                reverse.remove(dependent);
            }
        });
    }

    fn apply_event(&mut self, event: &EventKind) -> Result<(), RuntimeError> {
        match event {
            EventKind::SetState { write } => self.apply_state_writes(&[*write]),
            EventKind::SetStateBatch { writes } => self.apply_state_writes(writes),
            EventKind::Commit {
                owner,
                generation,
                bucket,
            } => {
                let computation = &self.derived[owner.index()];
                if computation.generation == *generation {
                    self.commit(computation.node, CanonicalValue::Numeric(*bucket))?;
                }
                Ok(())
            }
            EventKind::Reevaluate { owner, generation } => {
                let computation = &self.derived[owner.index()];
                if computation.generation == *generation {
                    self.dirty.mark(WorkId::Node(computation.node));
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
                effects,
            } => {
                let current_generation = self
                    .reactions
                    .get(reaction.index())
                    .ok_or(RuntimeError::UnknownReaction(*reaction))?
                    .generation;
                if current_generation == *generation {
                    let (writes, window_records) = effects.parts();
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

    fn commit(&mut self, id: NodeId, value: CanonicalValue) -> Result<(), RuntimeError> {
        let state = &mut self.values[id.index()];
        let old = state.canonical();
        if old == value {
            return Ok(());
        }
        match (state, value) {
            (
                NodeState::Input(cell) | NodeState::Derived { cell, .. },
                CanonicalValue::Numeric(next),
            ) => cell.bucket = next,
            (NodeState::Atom(current), CanonicalValue::Atom(next)) => *current = next,
            (NodeState::Input(_) | NodeState::Derived { .. }, CanonicalValue::Atom(_)) => {
                return Err(RuntimeError::ExpectedNumeric(id));
            }
            (NodeState::Atom(_), CanonicalValue::Numeric(_)) => {
                return Err(RuntimeError::ExpectedAtom(id));
            }
        }
        let node = &self.links[id.index()];
        self.transitions.push(TransitionRecord {
            at: self.now,
            node: id,
            node_name: Arc::clone(&node.name),
            old,
            new: value,
        });
        node.dependents
            .for_each(|dependent| self.dirty.mark(dependent));
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

    fn pending_forecasts(&mut self, owner: ForecastOwner) -> &mut u8 {
        match owner {
            ForecastOwner::Derived(id) => &mut self.derived[id.index()].pending_forecasts,
            ForecastOwner::Reaction(id) => &mut self.reactions[id.index()].pending_forecasts,
        }
    }

    /// Permit a bounded amount of obsolete work before a linear sweep. The
    /// threshold amortizes the scan against the invalidations it removes.
    const fn should_compact_forecasts(&self) -> bool {
        const MIN_STALE: usize = 1024;
        self.stale_forecasts >= MIN_STALE && self.stale_forecasts >= self.events.len() / 2
    }

    fn prune_forecasts(&mut self) {
        if self.stale_forecasts == 0 {
            return;
        }
        let compact = self.should_compact_forecasts();
        let derived = &self.derived;
        let reactions = &self.reactions;
        let live = |kind: &EventKind| {
            kind.forecast().is_none_or(|(work, generation)| {
                let current = match work {
                    ForecastOwner::Derived(id) => derived[id.index()].generation,
                    ForecastOwner::Reaction(id) => reactions[id.index()].generation,
                };
                current == generation
            })
        };
        if compact {
            // A linear retain is cheaper than individually popping a large
            // stale prefix, especially in dispersed heap mode.
            self.events.retain(live);
            self.stale_forecasts = 0;
            return;
        }
        // Prune at exposed causal boundaries, so obsolete events never
        // determine next_event_time or spend settlement microsteps.
        while self.events.peek().is_some_and(|event| !live(&event.kind)) {
            self.events.pop();
            self.stale_forecasts -= 1;
        }
    }

    fn push_event_with_priority(&mut self, at: SimTime, priority: i32, kind: EventKind) {
        if let Some((work, _)) = kind.forecast() {
            *self.pending_forecasts(work) += 1;
        }
        self.events.push(ScheduledEvent {
            at,
            priority,
            sequence: 0,
            kind,
        });
    }

    fn node_state(&self, id: NodeId) -> Result<&NodeState, RuntimeError> {
        self.values
            .get(id.index())
            .ok_or(RuntimeError::UnknownNode(id))
    }

    fn derived_owner(&self, id: NodeId) -> Result<DerivedId, RuntimeError> {
        match self.node_state(id)? {
            NodeState::Derived { owner, .. } => Ok(*owner),
            _ => Err(RuntimeError::NotDerived(id)),
        }
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

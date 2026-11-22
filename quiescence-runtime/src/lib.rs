#![forbid(unsafe_code)]

use quiescence_core::{
    Bucket, Estimate, HistoryId, NodeId, Quantization, QuantizeError, ReactionId, SimDuration,
    SimTime,
};
use std::{
    cell::RefCell,
    cmp::Ordering,
    collections::{BTreeMap, BTreeSet, BinaryHeap, VecDeque},
    fmt,
    sync::{Arc, Condvar, Mutex},
};

type EvalFn =
    Box<dyn for<'a> Fn(&EvalContext<'a>) -> Result<EvalOutput, EvalError> + Send + Sync + 'static>;
type ReactionFn = Box<
    dyn for<'a> Fn(&EvalContext<'a>) -> Result<ReactionOutput, EvalError> + Send + Sync + 'static,
>;
type ResolvedWrite = (NodeId, CanonicalValue, CanonicalValue);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RuntimeConfig {
    /// Maximum total event deliveries and evaluations at one simulation timestamp.
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
    #[must_use = "the constructed value must be used"]
    pub const fn new(node: NodeId, value: f64) -> Self {
        Self::Numeric { node, value }
    }

    #[must_use = "the constructed state write must be applied or scheduled"]
    pub const fn atom(node: NodeId, value: u64) -> Self {
        Self::Atom { node, value }
    }

    #[must_use = "the target node identifier must be used"]
    pub const fn node(self) -> NodeId {
        match self {
            Self::Numeric { node, .. } | Self::Atom { node, .. } => node,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum InteractionMoment {
    /// Resolve to the graph owner's simulation time when the interaction is imported.
    QuiescentNow,
    /// Preserve an explicit simulation timestamp supplied by the producer.
    At(SimTime),
}

impl InteractionMoment {
    const fn resolve(self, now: SimTime) -> SimTime {
        match self {
            Self::QuiescentNow => now,
            Self::At(at) => at,
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum ExternalInteraction {
    StateBatch {
        moment: InteractionMoment,
        priority: i32,
        writes: Vec<StateWrite>,
    },
    Callback {
        moment: InteractionMoment,
        priority: i32,
        reaction: ReactionId,
        token: Option<u64>,
    },
}

/// An ingress item that could not be admitted to this graph at its import boundary.
#[derive(Debug)]
pub struct RejectedInteraction {
    pub interaction: ExternalInteraction,
    pub error: RuntimeError,
}

/// Admission results for a finite prefix of the producer queue. Rejected items
/// remain owned by the caller for inspection, correction, or dead-letter handling.
#[derive(Debug, Default)]
pub struct InteractionImport {
    pub accepted: usize,
    pub rejected: Vec<RejectedInteraction>,
}

struct InteractionQueue {
    pending: VecDeque<ExternalInteraction>,
    senders: usize,
    receiver_alive: bool,
}

impl Default for InteractionQueue {
    fn default() -> Self {
        Self {
            pending: VecDeque::new(),
            senders: 1,
            receiver_alive: true,
        }
    }
}

#[derive(Default)]
struct InteractionShared {
    queue: Mutex<InteractionQueue>,
    ready: Condvar,
}

pub struct InteractionSender {
    shared: Arc<InteractionShared>,
}

pub struct InteractionInbox {
    shared: Arc<InteractionShared>,
}

#[must_use = "both endpoints are required to submit and consume interactions"]
pub fn interaction_channel() -> (InteractionSender, InteractionInbox) {
    let shared = Arc::new(InteractionShared::default());
    (
        InteractionSender {
            shared: Arc::clone(&shared),
        },
        InteractionInbox { shared },
    )
}

impl Clone for InteractionSender {
    fn clone(&self) -> Self {
        // Clone cannot fail; recover a poisoned mutex solely to keep the
        // sender count consistent. All fallible queue operations still report
        // QueuePoisoned to the caller.
        let mut queue = self
            .shared
            .queue
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        queue.senders += 1;
        drop(queue);
        Self {
            shared: Arc::clone(&self.shared),
        }
    }
}

impl Drop for InteractionSender {
    fn drop(&mut self) {
        let mut queue = self
            .shared
            .queue
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        queue.senders -= 1;
        let last_sender = queue.senders == 0;
        drop(queue);
        if last_sender {
            self.shared.ready.notify_all();
        }
    }
}

impl Drop for InteractionInbox {
    fn drop(&mut self) {
        let mut queue = self
            .shared
            .queue
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        queue.receiver_alive = false;
        queue.pending.clear();
    }
}

impl InteractionSender {
    /// # Errors
    /// Returns an error if the inbox has been dropped or the shared queue
    /// lock was poisoned.
    pub fn submit(&self, interaction: ExternalInteraction) -> Result<(), InteractionIngressError> {
        {
            let mut queue = self
                .shared
                .queue
                .lock()
                .map_err(|_| InteractionIngressError::QueuePoisoned)?;
            if !queue.receiver_alive {
                return Err(InteractionIngressError::Disconnected);
            }
            queue.pending.push_back(interaction);
        }
        self.shared.ready.notify_one();
        Ok(())
    }

    /// # Errors
    /// Returns an error if the shared ingress queue lock is poisoned.
    pub fn state_now(
        &self,
        priority: i32,
        writes: Vec<StateWrite>,
    ) -> Result<(), InteractionIngressError> {
        self.submit(ExternalInteraction::StateBatch {
            moment: InteractionMoment::QuiescentNow,
            priority,
            writes,
        })
    }

    /// # Errors
    /// Returns an error if the shared ingress queue lock is poisoned.
    pub fn state_at(
        &self,
        at: SimTime,
        priority: i32,
        writes: Vec<StateWrite>,
    ) -> Result<(), InteractionIngressError> {
        self.submit(ExternalInteraction::StateBatch {
            moment: InteractionMoment::At(at),
            priority,
            writes,
        })
    }

    /// # Errors
    /// Returns an error if the shared ingress queue lock is poisoned.
    pub fn callback_now(
        &self,
        reaction: ReactionId,
        priority: i32,
    ) -> Result<(), InteractionIngressError> {
        self.submit(ExternalInteraction::Callback {
            moment: InteractionMoment::QuiescentNow,
            priority,
            reaction,
            token: None,
        })
    }

    /// # Errors
    /// Returns an error if the shared ingress queue lock is poisoned.
    pub fn callback_now_with_token(
        &self,
        reaction: ReactionId,
        priority: i32,
        token: u64,
    ) -> Result<(), InteractionIngressError> {
        self.submit(ExternalInteraction::Callback {
            moment: InteractionMoment::QuiescentNow,
            priority,
            reaction,
            token: Some(token),
        })
    }

    /// # Errors
    /// Returns an error if the shared ingress queue lock is poisoned.
    pub fn callback_at(
        &self,
        reaction: ReactionId,
        at: SimTime,
        priority: i32,
    ) -> Result<(), InteractionIngressError> {
        self.submit(ExternalInteraction::Callback {
            moment: InteractionMoment::At(at),
            priority,
            reaction,
            token: None,
        })
    }

    /// # Errors
    /// Returns an error if the shared ingress queue lock is poisoned.
    pub fn callback_at_with_token(
        &self,
        reaction: ReactionId,
        at: SimTime,
        priority: i32,
        token: u64,
    ) -> Result<(), InteractionIngressError> {
        self.submit(ExternalInteraction::Callback {
            moment: InteractionMoment::At(at),
            priority,
            reaction,
            token: Some(token),
        })
    }
}

impl InteractionInbox {
    /// # Errors
    /// Returns an error if the shared ingress queue lock is poisoned.
    pub fn pending_count(&self) -> Result<usize, InteractionIngressError> {
        Ok(self
            .shared
            .queue
            .lock()
            .map_err(|_| InteractionIngressError::QueuePoisoned)?
            .pending
            .len())
    }

    /// Block until a queued interaction is available or the final producer
    /// is dropped. Already queued interactions are delivered before shutdown.
    /// The caller still owns the policy for advancing simulation time.
    ///
    /// # Errors
    /// Returns `Disconnected` when no senders remain and the queue is empty,
    /// or `QueuePoisoned` if the shared mutex was poisoned.
    pub fn wait_for_pending(&self) -> Result<usize, InteractionIngressError> {
        let queue = self
            .shared
            .queue
            .lock()
            .map_err(|_| InteractionIngressError::QueuePoisoned)?;
        let queue = self
            .shared
            .ready
            .wait_while(queue, |queue| {
                queue.pending.is_empty() && queue.senders != 0
            })
            .map_err(|_| InteractionIngressError::QueuePoisoned)?;
        if queue.pending.is_empty() {
            Err(InteractionIngressError::Disconnected)
        } else {
            Ok(queue.pending.len())
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QuiescentSnapshot {
    pub at: SimTime,
    pub next_event_time: Option<SimTime>,
    values: Vec<CanonicalValue>,
}

impl QuiescentSnapshot {
    #[must_use = "the observed canonical value must be used"]
    pub fn value(&self, node: NodeId) -> Option<CanonicalValue> {
        self.values.get(node.index()).copied()
    }

    #[must_use = "the snapshot node count must be used"]
    pub const fn node_count(&self) -> usize {
        self.values.len()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QuiescentPublication {
    pub snapshot: QuiescentSnapshot,
    pub transitions: Vec<TransitionRecord>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WindowRecord {
    pub history: HistoryId,
    pub key: u64,
}

impl WindowRecord {
    #[must_use = "the constructed value must be used"]
    pub const fn new(history: HistoryId, key: u64) -> Self {
        Self { history, key }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ScheduledReevaluation {
    pub key: u64,
    pub after: SimDuration,
    pub priority: i32,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ScheduledReactionCallback {
    pub reaction: ReactionId,
    pub key: u64,
    pub after: SimDuration,
    pub priority: i32,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ReactionOutput {
    pub transition_after: Option<SimDuration>,
    pub writes: Vec<StateWrite>,
    pub window_records: Vec<WindowRecord>,
    pub scheduled_reevaluations: Vec<ScheduledReevaluation>,
    pub scheduled_callbacks: Vec<ScheduledReactionCallback>,
}
impl ReactionOutput {
    #[must_use = "the reaction result must be returned to the scheduler"]
    pub const fn idle() -> Self {
        Self {
            transition_after: None,
            writes: Vec::new(),
            window_records: Vec::new(),
            scheduled_reevaluations: Vec::new(),
            scheduled_callbacks: Vec::new(),
        }
    }

    #[must_use = "the constructed evaluation or reaction result must be used"]
    pub const fn after(delay: SimDuration, writes: Vec<StateWrite>) -> Self {
        Self {
            transition_after: Some(delay),
            writes,
            window_records: Vec::new(),
            scheduled_reevaluations: Vec::new(),
            scheduled_callbacks: Vec::new(),
        }
    }

    #[must_use = "the constructed evaluation or reaction result must be used"]
    pub const fn immediate(writes: Vec<StateWrite>) -> Self {
        Self::after(SimDuration::ZERO, writes)
    }

    #[must_use = "the reevaluation request must be returned to the scheduler"]
    pub const fn reevaluate_after(delay: SimDuration) -> Self {
        Self::after(delay, Vec::new())
    }

    #[must_use = "the scheduling request must be returned to the scheduler"]
    pub fn schedule_once_after(delay: SimDuration, key: u64, priority: i32) -> Self {
        Self {
            transition_after: None,
            writes: Vec::new(),
            window_records: Vec::new(),
            scheduled_reevaluations: vec![ScheduledReevaluation {
                key,
                after: delay,
                priority,
            }],
            scheduled_callbacks: Vec::new(),
        }
    }

    #[must_use = "the updated reaction result contains the new scheduling request"]
    pub fn and_schedule_once_after(mut self, delay: SimDuration, key: u64, priority: i32) -> Self {
        self.scheduled_reevaluations.push(ScheduledReevaluation {
            key,
            after: delay,
            priority,
        });
        self
    }

    #[must_use = "the callback request must be returned to the scheduler"]
    pub fn schedule_reaction_once_after(
        reaction: ReactionId,
        delay: SimDuration,
        key: u64,
        priority: i32,
    ) -> Self {
        let mut output = Self::idle();
        output.scheduled_callbacks.push(ScheduledReactionCallback {
            reaction,
            key,
            after: delay,
            priority,
        });
        output
    }

    #[must_use = "the updated reaction result contains the callback request"]
    pub fn and_schedule_reaction_once_after(
        mut self,
        reaction: ReactionId,
        delay: SimDuration,
        key: u64,
        priority: i32,
    ) -> Self {
        self.scheduled_callbacks.push(ScheduledReactionCallback {
            reaction,
            key,
            after: delay,
            priority,
        });
        self
    }

    #[must_use = "the updated reaction result contains the history record"]
    pub fn and_record_window(mut self, history: HistoryId, key: u64) -> Self {
        self.window_records.push(WindowRecord::new(history, key));
        self
    }
}

impl EvalOutput {
    #[must_use = "the constructed evaluation or reaction result must be used"]
    pub const fn immediate(value: f64) -> Self {
        Self {
            estimate: Estimate::point(value),
            delay: SimDuration::ZERO,
            valid_for: None,
        }
    }
    #[must_use = "the constructed evaluation or reaction result must be used"]
    pub const fn after(value: f64, delay: SimDuration) -> Self {
        Self {
            estimate: Estimate::point(value),
            delay,
            valid_for: None,
        }
    }
    #[must_use = "the constructed evaluation result must be used"]
    pub const fn estimate(estimate: Estimate) -> Self {
        Self {
            estimate,
            delay: SimDuration::ZERO,
            valid_for: None,
        }
    }
    #[must_use = "the returned evaluation result contains the validity interval"]
    pub const fn with_valid_for(mut self, duration: SimDuration) -> Self {
        self.valid_for = Some(duration);
        self
    }
}

pub struct EvalContext<'a> {
    now: SimTime,
    nodes: &'a [Node],
    windows: &'a [EventWindow],
    reads: RefCell<BTreeSet<NodeId>>,
    scheduled_key: Option<u64>,
    external_token: Option<u64>,
}
impl EvalContext<'_> {
    #[must_use = "the current simulation time must be used"]
    pub const fn now(&self) -> SimTime {
        self.now
    }

    /// Key of the keyed scheduled callback that caused this evaluation, when applicable.
    #[must_use = "the scheduled key must be considered by the caller"]
    pub const fn scheduled_key(&self) -> Option<u64> {
        self.scheduled_key
    }

    /// Source-defined token attached to an unkeyed external event, when applicable.
    #[must_use = "the external token must be considered by the caller"]
    pub const fn external_token(&self) -> Option<u64> {
        self.external_token
    }
    /// # Errors
    /// Returns an error if the node does not exist or is not numeric.
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
    /// # Errors
    /// Returns an error if the node does not exist or is not numeric.
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
    /// # Errors
    /// Returns an error if the node does not exist or is not numeric.
    pub fn read_bool(&self, id: NodeId) -> Result<bool, EvalError> {
        Ok(self.read_bucket(id)?.raw() != 0)
    }
    /// # Errors
    /// Returns an error if the node does not exist or is not an atom.
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
    /// # Errors
    /// Returns an error if the temporal history does not exist.
    pub fn window_count(&self, id: HistoryId, key: u64) -> Result<usize, EvalError> {
        let window = self
            .windows
            .get(id.index())
            .ok_or(EvalError::UnknownHistory(id))?;
        Ok(window.count_at(self.now, key))
    }

    fn into_dependencies(self) -> BTreeSet<NodeId> {
        self.reads.into_inner()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct WindowEvent {
    at: SimTime,
    key: u64,
}

struct EventWindow {
    name: String,
    duration: SimDuration,
    events: VecDeque<WindowEvent>,
}

impl EventWindow {
    fn count_at(&self, now: SimTime, key: u64) -> usize {
        self.events
            .iter()
            .filter(|event| {
                let elapsed = i128::from(now.as_nanos()) - i128::from(event.at.as_nanos());
                event.key == key && (0..=i128::from(self.duration.as_nanos())).contains(&elapsed)
            })
            .count()
    }

    fn record(&mut self, now: SimTime, key: u64) {
        while self.events.front().is_some_and(|event| {
            i128::from(now.as_nanos()) - i128::from(event.at.as_nanos())
                > i128::from(self.duration.as_nanos())
        }) {
            self.events.pop_front();
        }
        self.events.push_back(WindowEvent { at: now, key });
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
    track_dependencies: bool,
    pending_scheduled_key: Option<u64>,
    pending_external_token: Option<u64>,
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
    windows: Vec<EventWindow>,
    reaction_dependents: BTreeMap<NodeId, BTreeSet<ReactionId>>,
    dirty: VecDeque<WorkId>,
    node_is_dirty: Vec<bool>,
    reaction_is_dirty: Vec<bool>,
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
            reaction_dependents: BTreeMap::new(),
            dirty: VecDeque::new(),
            node_is_dirty: Vec::new(),
            reaction_is_dirty: Vec::new(),
            events: EventQueue::new(),
            pending_scheduled_reactions: BTreeSet::new(),
            transitions: Vec::new(),
        }
    }
    #[must_use = "the current simulation time must be used"]
    pub const fn now(&self) -> SimTime {
        self.now
    }

    /// True only at an externally observable safe point: no dirty computation and no event due at
    /// the current simulation time. Future events do not prevent quiescence.
    #[must_use = "the quiescence result must be checked"]
    pub fn is_quiescent(&self) -> bool {
        self.dirty.is_empty() && self.events.peek().is_none_or(|event| event.at > self.now)
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
            values: self.nodes.iter().map(canonical_value).collect(),
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
        let mut queue = inbox.shared.queue.lock().map_err(|_| {
            RuntimeError::InteractionIngress(InteractionIngressError::QueuePoisoned)
        })?;
        let interactions = std::mem::take(&mut queue.pending);
        drop(queue);

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
        self.windows.push(EventWindow {
            name: name.into(),
            duration,
            events: VecDeque::new(),
        });
        Ok(id)
    }

    /// # Errors
    /// Returns an error if the graph cannot allocate another node or the initial value cannot be quantized.
    pub fn add_input(
        &mut self,
        name: impl Into<String>,
        q: Quantization,
        initial: f64,
    ) -> Result<NodeId, RuntimeError> {
        self.add_node(name, q, initial, None)
    }

    /// # Errors
    /// Returns an error if the graph cannot allocate another node identifier.
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

    /// # Errors
    /// Returns an error if the graph cannot allocate another node or the initial value cannot be quantized.
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
        self.add_node(name, q, initial, Some(Box::new(evaluator)))
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
        if !matches!(node.state, NodeState::Numeric { .. }) {
            return Err(RuntimeError::ExpectedNumeric(id));
        }
        node.evaluator = Some(Box::new(evaluator));
        self.mark_dirty(WorkId::Node(id));
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
        let raw_id =
            u32::try_from(self.reactions.len()).map_err(|_| RuntimeError::TooManyReactions)?;
        let id = ReactionId::from_index(raw_id);
        self.reactions.push(Reaction {
            name: name.into(),
            evaluator: Box::new(evaluator),
            dependencies: BTreeSet::new(),
            generation: 0,
            track_dependencies: true,
            pending_scheduled_key: None,
            pending_external_token: None,
        });
        self.reaction_is_dirty.push(false);
        self.mark_dirty(WorkId::Reaction(id));
        Ok(id)
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
        let raw_id =
            u32::try_from(self.reactions.len()).map_err(|_| RuntimeError::TooManyReactions)?;
        let id = ReactionId::from_index(raw_id);
        self.reactions.push(Reaction {
            name: name.into(),
            evaluator: Box::new(evaluator),
            dependencies: BTreeSet::new(),
            generation: 0,
            track_dependencies: false,
            pending_scheduled_key: None,
            pending_external_token: None,
        });
        self.reaction_is_dirty.push(false);
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
        let NodeState::Numeric { bucket, .. } = self.node(id)?.state else {
            return Err(RuntimeError::ExpectedNumeric(id));
        };
        Ok(bucket)
    }
    /// # Errors
    /// Returns an error if the node does not exist or is not numeric.
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
    /// # Errors
    /// Returns an error if the node does not exist or is not an atom.
    pub fn value_atom(&self, id: NodeId) -> Result<u64, RuntimeError> {
        let NodeState::Atom(value) = self.node(id)?.state else {
            return Err(RuntimeError::ExpectedAtom(id));
        };
        Ok(value)
    }

    fn not_quiescent_error(&self) -> RuntimeError {
        RuntimeError::NotQuiescent {
            at: self.now,
            dirty_work: self.dirty.len(),
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
        if at < self.now {
            return Err(RuntimeError::EventInPast {
                now: self.now,
                requested: at,
            });
        }
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

    fn validate_state_write_batch(&self, writes: &[StateWrite]) -> Result<(), RuntimeError> {
        let mut seen = BTreeSet::new();
        for write in writes {
            let node_id = write.node();
            let node = self.node(node_id)?;
            if !seen.insert(node_id) {
                return Err(RuntimeError::DuplicateStateWrite(node_id));
            }
            if node.evaluator.is_some() {
                return Err(RuntimeError::NotAnInput(node_id));
            }
            match (write, &node.state) {
                (StateWrite::Numeric { value, .. }, NodeState::Numeric { .. }) => {
                    // Finiteness is independent of the state at delivery time.
                    // Bucket resolution depends on the then-current hysteresis
                    // state, so it must remain in apply_state_writes.
                    if !value.is_finite() {
                        return Err(QuantizeError::NonFiniteEstimate.into());
                    }
                }
                (StateWrite::Atom { .. }, NodeState::Atom(_)) => {}
                (StateWrite::Numeric { .. }, NodeState::Atom(_)) => {
                    return Err(RuntimeError::ExpectedNumeric(node_id));
                }
                (StateWrite::Atom { .. }, NodeState::Numeric { .. }) => {
                    return Err(RuntimeError::ExpectedAtom(node_id));
                }
            }
        }
        Ok(())
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
        self.apply_state_writes(&[StateWrite::new(id, value)])?;
        self.settle()
    }

    /// # Errors
    /// Propagates node validation, evaluation, and calculation-instability errors.
    pub fn set_atom_now(&mut self, id: NodeId, value: u64) -> Result<(), RuntimeError> {
        self.apply_state_writes(&[StateWrite::atom(id, value)])?;
        self.settle()
    }

    /// # Errors
    /// Returns an error if the node does not exist or the requested time is in the past.
    pub fn schedule_input(
        &mut self,
        id: NodeId,
        at: SimTime,
        value: f64,
    ) -> Result<(), RuntimeError> {
        self.validate_state_write_batch(&[StateWrite::new(id, value)])?;
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

    /// # Errors
    /// Returns an error if the node does not exist or the requested time is in the past.
    pub fn schedule_atom(
        &mut self,
        id: NodeId,
        at: SimTime,
        value: u64,
    ) -> Result<(), RuntimeError> {
        self.validate_state_write_batch(&[StateWrite::atom(id, value)])?;
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
        self.reactions
            .get(reaction.index())
            .ok_or(RuntimeError::UnknownReaction(reaction))?;
        if at < self.now {
            return Err(RuntimeError::EventInPast {
                now: self.now,
                requested: at,
            });
        }
        self.push_event_with_priority(
            at,
            priority,
            EventKind::ExternalReaction {
                reaction,
                token: None,
            },
        );
        Ok(())
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
        self.reactions
            .get(reaction.index())
            .ok_or(RuntimeError::UnknownReaction(reaction))?;
        if at < self.now {
            return Err(RuntimeError::EventInPast {
                now: self.now,
                requested: at,
            });
        }
        self.push_event_with_priority(
            at,
            priority,
            EventKind::ExternalReaction {
                reaction,
                token: Some(token),
            },
        );
        Ok(())
    }

    /// # Errors
    /// Propagates evaluation, scheduling, quantization, and calculation-instability errors.
    pub fn settle(&mut self) -> Result<(), RuntimeError> {
        let mut steps = 0;
        self.settle_with_budget(&mut steps)
    }

    fn settle_with_budget(&mut self, steps: &mut usize) -> Result<(), RuntimeError> {
        while let Some(&work) = self.dirty.front() {
            if *steps >= self.config.same_time_microstep_budget {
                return Err(RuntimeError::CalculationInstability {
                    at: self.now,
                    microsteps: *steps,
                    active_work: self.dirty.iter().copied().collect(),
                });
            }
            *steps += 1;
            self.dirty.pop_front();
            match work {
                WorkId::Node(id) => self.node_is_dirty[id.index()] = false,
                WorkId::Reaction(id) => self.reaction_is_dirty[id.index()] = false,
            }
            let result = match work {
                WorkId::Node(id) => {
                    if self.nodes[id.index()].evaluator.is_some() {
                        self.evaluate(id)
                    } else {
                        Ok(())
                    }
                }
                WorkId::Reaction(id) => self.evaluate_reaction(id),
            };
            if let Err(error) = result {
                let already_dirty = match work {
                    WorkId::Node(id) => &mut self.node_is_dirty[id.index()],
                    WorkId::Reaction(id) => &mut self.reaction_is_dirty[id.index()],
                };
                if !*already_dirty {
                    *already_dirty = true;
                    self.dirty.push_front(work);
                }
                return Err(error);
            }
        }
        Ok(())
    }

    /// # Errors
    /// Returns an error for backwards time travel or if processing due work fails.
    pub fn advance_to(&mut self, target: SimTime) -> Result<(), RuntimeError> {
        if target < self.now {
            return Err(RuntimeError::EventInPast {
                now: self.now,
                requested: target,
            });
        }
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
                    active_work: self.dirty.iter().copied().collect(),
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
        let evaluator = self.nodes[id.index()]
            .evaluator
            .as_ref()
            .ok_or(RuntimeError::MissingEvaluator(id))?;
        let context = EvalContext {
            now: self.now,
            nodes: &self.nodes,
            windows: &self.windows,
            reads: RefCell::new(BTreeSet::new()),
            scheduled_key: None,
            external_token: None,
        };
        let output =
            evaluator(&context).map_err(|source| RuntimeError::Evaluation { node: id, source })?;

        let (quantization, current) = match self.nodes[id.index()].state {
            NodeState::Numeric {
                quantization,
                bucket,
            } => (quantization, bucket),
            NodeState::Atom(_) => return Err(RuntimeError::ExpectedNumeric(id)),
        };
        let next = quantization.resolve(Some(current), output.estimate)?;
        let deps = context.into_dependencies();
        self.update_dependencies(WorkId::Node(id), deps)?;
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
        let (evaluator, track_dependencies, scheduled_key, external_token) = {
            let reaction = self
                .reactions
                .get(id.index())
                .ok_or(RuntimeError::UnknownReaction(id))?;
            (
                &reaction.evaluator,
                reaction.track_dependencies,
                reaction.pending_scheduled_key,
                reaction.pending_external_token,
            )
        };
        let context = EvalContext {
            now: self.now,
            nodes: &self.nodes,
            windows: &self.windows,
            reads: RefCell::new(BTreeSet::new()),
            scheduled_key,
            external_token,
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
            transition_after,
            writes,
            window_records,
            scheduled_reevaluations,
            scheduled_callbacks,
        } = output;

        // Validate the entire proposed result before changing dependency
        // subscriptions, consuming callback metadata, or publishing effects.
        if transition_after.is_none() && (!writes.is_empty() || !window_records.is_empty()) {
            return Err(RuntimeError::UnscheduledReactionEffects(id));
        }
        for callback in &scheduled_callbacks {
            self.reactions
                .get(callback.reaction.index())
                .ok_or(RuntimeError::UnknownReaction(callback.reaction))?;
        }
        let prepared_writes = if let Some(delay) = transition_after {
            for record in &window_records {
                if self.windows.get(record.history.index()).is_none() {
                    return Err(RuntimeError::UnknownHistory(record.history));
                }
            }
            if delay == SimDuration::ZERO {
                Some(self.resolve_state_writes(&writes)?)
            } else {
                // Delayed writes will be resolved against the canonical state
                // when they fire, but their types and targets are fixed now.
                self.validate_state_write_batch(&writes)?;
                None
            }
        } else {
            None
        };

        if let Some(deps) = dependencies {
            self.update_dependencies(WorkId::Reaction(id), deps)?;
        }
        let reaction = self
            .reactions
            .get_mut(id.index())
            .ok_or(RuntimeError::UnknownReaction(id))?;
        reaction.pending_scheduled_key = None;
        reaction.pending_external_token = None;
        reaction.generation = reaction.generation.saturating_add(1);
        let generation = reaction.generation;

        if let Some(resolved) = prepared_writes {
            self.commit_resolved_state_writes(resolved)?;
            for record in window_records {
                self.windows[record.history.index()].record(self.now, record.key);
            }
        } else if let Some(delay) = transition_after {
            self.push_event(
                self.now.saturating_add(delay),
                EventKind::ReactionTransition {
                    reaction: id,
                    generation,
                    writes,
                    window_records,
                },
            );
        }
        for request in scheduled_reevaluations {
            self.schedule_reaction_once(id, request);
        }
        for callback in scheduled_callbacks {
            self.schedule_reaction_once(
                callback.reaction,
                ScheduledReevaluation {
                    key: callback.key,
                    after: callback.after,
                    priority: callback.priority,
                },
            );
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
            match dependent {
                WorkId::Node(id) => {
                    self.nodes[dep.index()].dependents.remove(&id);
                }
                WorkId::Reaction(id) => {
                    if let Some(reactions) = self.reaction_dependents.get_mut(&dep) {
                        reactions.remove(&id);
                        if reactions.is_empty() {
                            self.reaction_dependents.remove(&dep);
                        }
                    }
                }
            }
        }
        for dep in new.difference(&old).copied() {
            match dependent {
                WorkId::Node(id) => {
                    self.nodes[dep.index()].dependents.insert(id);
                }
                WorkId::Reaction(id) => {
                    self.reaction_dependents.entry(dep).or_default().insert(id);
                }
            }
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
                    self.mark_dirty(WorkId::Node(*node));
                }
                Ok(())
            }
            EventKind::ExternalReaction { reaction, token } => {
                let target = self
                    .reactions
                    .get_mut(reaction.index())
                    .ok_or(RuntimeError::UnknownReaction(*reaction))?;
                target.pending_scheduled_key = None;
                target.pending_external_token = *token;
                self.mark_dirty(WorkId::Reaction(*reaction));
                Ok(())
            }
            EventKind::ScheduledReaction { reaction, key } => {
                self.pending_scheduled_reactions.remove(&(*reaction, *key));
                let target = self
                    .reactions
                    .get_mut(reaction.index())
                    .ok_or(RuntimeError::UnknownReaction(*reaction))?;
                target.pending_scheduled_key = Some(*key);
                target.pending_external_token = None;
                self.mark_dirty(WorkId::Reaction(*reaction));
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
                    self.mark_dirty(WorkId::Reaction(*reaction));
                }
                Ok(())
            }
        }
    }

    fn apply_reaction_effects(
        &mut self,
        writes: &[StateWrite],
        window_records: &[WindowRecord],
    ) -> Result<(), RuntimeError> {
        for record in window_records {
            if self.windows.get(record.history.index()).is_none() {
                return Err(RuntimeError::UnknownHistory(record.history));
            }
        }
        self.apply_state_writes(writes)?;
        for record in window_records {
            self.windows[record.history.index()].record(self.now, record.key);
        }
        Ok(())
    }

    fn apply_state_writes(&mut self, writes: &[StateWrite]) -> Result<(), RuntimeError> {
        let resolved = self.resolve_state_writes(writes)?;
        self.commit_resolved_state_writes(resolved)
    }

    fn resolve_state_writes(
        &self,
        writes: &[StateWrite],
    ) -> Result<Vec<ResolvedWrite>, RuntimeError> {
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
        Ok(resolved)
    }

    fn commit_resolved_state_writes(
        &mut self,
        resolved: Vec<ResolvedWrite>,
    ) -> Result<(), RuntimeError> {
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

const fn canonical_value(node: &Node) -> CanonicalValue {
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
    SetStateBatch {
        writes: Vec<StateWrite>,
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
        window_records: Vec<WindowRecord>,
    },
    ExternalReaction {
        reaction: ReactionId,
        token: Option<u64>,
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
/// Ordering ignores payloads, including NaN numeric writes; equality
/// remains reflexive because heap identity is its assigned scheduling key.
#[derive(Debug)]
struct HeapEvent(ScheduledEvent);
impl PartialEq for HeapEvent {
    fn eq(&self, other: &Self) -> bool {
        self.0.at == other.0.at
            && self.0.priority == other.0.priority
            && self.0.sequence == other.0.sequence
    }
}
impl Eq for HeapEvent {}
impl Ord for HeapEvent {
    fn cmp(&self, other: &Self) -> Ordering {
        other
            .0
            .at
            .cmp(&self.0.at)
            .then_with(|| other.0.priority.cmp(&self.0.priority))
            .then_with(|| other.0.sequence.cmp(&self.0.sequence))
    }
}
impl PartialOrd for HeapEvent {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// A FIFO queue per event key while the workload has few distinct keys.
/// When events spread over many times/priorities, switch to a conventional
/// binary heap to avoid tree-map allocation overhead on sparse schedules.
const MAX_GROUPED_KEYS: usize = 128;

enum EventStorage {
    Grouped(BTreeMap<(SimTime, i32), VecDeque<ScheduledEvent>>),
    Sparse(BinaryHeap<HeapEvent>),
}

struct EventQueue {
    storage: EventStorage,
    next_sequence: u64,
}
impl EventQueue {
    const fn new() -> Self {
        Self {
            storage: EventStorage::Grouped(BTreeMap::new()),
            next_sequence: 0,
        }
    }

    fn peek(&self) -> Option<&ScheduledEvent> {
        match &self.storage {
            EventStorage::Grouped(groups) => groups.first_key_value()?.1.front(),
            EventStorage::Sparse(heap) => heap.peek().map(|event| &event.0),
        }
    }

    fn push(&mut self, mut event: ScheduledEvent) {
        if self.next_sequence == u64::MAX {
            self.resequence();
        }
        event.sequence = self.next_sequence;
        self.next_sequence += 1;
        match &mut self.storage {
            EventStorage::Grouped(groups) => {
                groups
                    .entry((event.at, event.priority))
                    .or_default()
                    .push_back(event);
                if groups.len() > MAX_GROUPED_KEYS {
                    // Conversion is linear in the pending event count, and the
                    // heap handles future mostly-distinct keys efficiently.
                    let old = std::mem::replace(
                        &mut self.storage,
                        EventStorage::Sparse(BinaryHeap::new()),
                    );
                    if let EventStorage::Grouped(groups) = old {
                        let heap = BinaryHeap::from(
                            groups
                                .into_values()
                                .flatten()
                                .map(HeapEvent)
                                .collect::<Vec<_>>(),
                        );
                        self.storage = EventStorage::Sparse(heap);
                    }
                }
            }
            EventStorage::Sparse(heap) => heap.push(HeapEvent(event)),
        }
    }

    fn restore_front(&mut self, event: ScheduledEvent) {
        match &mut self.storage {
            EventStorage::Grouped(groups) => groups
                .entry((event.at, event.priority))
                .or_default()
                .push_front(event),
            EventStorage::Sparse(heap) => heap.push(HeapEvent(event)),
        }
    }

    fn pop(&mut self) -> Option<ScheduledEvent> {
        match &mut self.storage {
            EventStorage::Grouped(groups) => {
                let mut first = groups.first_entry()?;
                let event = first.get_mut().pop_front();
                if first.get().is_empty() {
                    first.remove();
                }
                event
            }
            EventStorage::Sparse(heap) => {
                let event = heap.pop().map(|event| event.0);
                if heap.is_empty() {
                    self.storage = EventStorage::Grouped(BTreeMap::new());
                }
                event
            }
        }
    }

    fn resequence(&mut self) {
        let mut sequence = 0;
        match &mut self.storage {
            EventStorage::Grouped(groups) => {
                for group in groups.values_mut() {
                    for event in group {
                        event.sequence = sequence;
                        sequence += 1;
                    }
                }
            }
            EventStorage::Sparse(heap) => {
                let mut ordered = std::mem::take(heap).into_sorted_vec();
                // Our Ord is a max-heap reversal of causal order, so the
                // chronological sequence is the reverse of into_sorted_vec.
                for event in ordered.iter_mut().rev() {
                    event.0.sequence = sequence;
                    sequence += 1;
                }
                *heap = BinaryHeap::from(ordered);
            }
        }
        self.next_sequence = sequence;
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
    UnknownHistory(HistoryId),
    Message(String),
}
impl fmt::Display for EvalError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnknownNode(id) => write!(f, "evaluator read unknown node {id:?}"),
            Self::ExpectedNumeric(id) => write!(f, "evaluator expected numeric node {id:?}"),
            Self::ExpectedAtom(id) => write!(f, "evaluator expected atom node {id:?}"),
            Self::UnknownHistory(id) => write!(f, "evaluator read unknown history {id:?}"),
            Self::Message(m) => f.write_str(m),
        }
    }
}
impl std::error::Error for EvalError {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum InteractionIngressError {
    QueuePoisoned,
    Disconnected,
}
impl fmt::Display for InteractionIngressError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::QueuePoisoned => f.write_str("interaction ingress queue lock poisoned"),
            Self::Disconnected => f.write_str("interaction ingress channel disconnected"),
        }
    }
}
impl std::error::Error for InteractionIngressError {}

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
    TooManyHistories,
    UnknownReaction(ReactionId),
    UnknownHistory(HistoryId),
    UnscheduledReactionEffects(ReactionId),
    DuplicateStateWrite(NodeId),
    InteractionIngress(InteractionIngressError),
    InteractionProcessing {
        import: InteractionImport,
        source: Box<Self>,
    },
    NotQuiescent {
        at: SimTime,
        dirty_work: usize,
        due_event: Option<SimTime>,
    },
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
            Self::TooManyHistories => f.write_str("graph contains too many temporal histories"),
            Self::UnknownReaction(id) => write!(f, "unknown reaction {id:?}"),
            Self::UnknownHistory(id) => write!(f, "unknown temporal history {id:?}"),
            Self::UnscheduledReactionEffects(id) => {
                write!(
                    f,
                    "reaction {id:?} returned effects without a transition time"
                )
            }
            Self::DuplicateStateWrite(id) => {
                write!(
                    f,
                    "reaction transition writes state node {id:?} more than once"
                )
            }
            Self::InteractionIngress(source) => write!(f, "interaction ingress failed: {source}"),
            Self::InteractionProcessing { source, .. } => {
                write!(f, "interaction processing failed after import: {source}")
            }
            Self::NotQuiescent {
                at,
                dirty_work,
                due_event,
            } => write!(
                f,
                "graph is not quiescent at {at:?}: {dirty_work} dirty work items, due event {due_event:?}"
            ),
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
            Self::InteractionIngress(source) => Some(source),
            Self::InteractionProcessing { source, .. } => Some(source.as_ref()),
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
        CanonicalValue, EvalError, EvalOutput, Graph, InteractionIngressError, ReactionOutput,
        RuntimeConfig, RuntimeError, StateWrite, interaction_channel,
    };
    use quiescence_core::{Bucket, Quantization, SimDuration, SimTime};
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
        assert!(before.contains(&gate));
        assert!(before.contains(&previous));
        assert!(!before.contains(&replacement));

        assert!(matches!(
            g.set_input_now(gate, 1.0),
            Err(RuntimeError::UnknownReaction(_))
        ));
        let still_previous = &g.reactions[reaction.index()].dependencies;
        assert!(still_previous.contains(&previous));
        assert!(!still_previous.contains(&replacement));
        assert!(!g.is_quiescent());

        retry_allowed.store(1, Ordering::Relaxed);
        g.settle()?;
        let after = &g.reactions[reaction.index()].dependencies;
        assert!(after.contains(&gate));
        assert!(!after.contains(&previous));
        assert!(after.contains(&replacement));
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
        let callback =
            g.add_event_callback("temporarily failing scheduled callback", move |c| {
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
    fn grouped_event_queue_preserves_priority_fifo_and_failed_event_position()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut queue = super::EventQueue::new();
        let at = SimTime::from_millis(1);
        let reaction = quiescence_core::ReactionId::from_index(0);
        let event = |priority, token| super::ScheduledEvent {
            at,
            priority,
            sequence: 0,
            kind: super::EventKind::ExternalReaction {
                reaction,
                token: Some(token),
            },
        };
        queue.push(event(2, 1));
        queue.push(event(2, 2));
        queue.push(event(3, 3));
        assert_eq!(queue.peek().map(|e| &e.kind), Some(&event(2, 1).kind));
        let failed = queue.pop().ok_or("queue unexpectedly empty")?;
        assert_eq!(failed.kind, event(2, 1).kind);
        queue.restore_front(failed);
        queue.push(event(-1, 99));
        assert_eq!(queue.pop().map(|e| e.kind), Some(event(-1, 99).kind));
        assert_eq!(queue.pop().map(|e| e.kind), Some(event(2, 1).kind));
        assert_eq!(queue.pop().map(|e| e.kind), Some(event(2, 2).kind));
        assert_eq!(queue.pop().map(|e| e.kind), Some(event(3, 3).kind));
        assert_eq!(queue.pop(), None);
        Ok(())
    }

    #[test]
    fn sparse_fallback_keeps_fifo_and_retry_priority() -> Result<(), Box<dyn std::error::Error>> {
        let mut queue = super::EventQueue::new();
        let reaction = quiescence_core::ReactionId::from_index(0);
        let at = SimTime::from_nanos(8);
        let event = |priority, token| super::ScheduledEvent {
            at,
            priority,
            sequence: 0,
            kind: super::EventKind::ExternalReaction {
                reaction,
                token: Some(token),
            },
        };
        for index in 0..=super::MAX_GROUPED_KEYS {
            queue.push(event(i32::try_from(index)?, u64::try_from(index)?));
        }
        assert!(matches!(&queue.storage, super::EventStorage::Sparse(_)));
        queue.push(event(17, 999));
        let failed = queue.pop().ok_or("sparse queue unexpectedly empty")?;
        queue.restore_front(failed);
        for index in 0..=super::MAX_GROUPED_KEYS {
            assert_eq!(
                queue.pop().map(|e| e.kind),
                Some(event(i32::try_from(index)?, u64::try_from(index)?).kind)
            );
            if index == 17 {
                assert_eq!(queue.pop().map(|e| e.kind), Some(event(17, 999).kind));
            }
        }
        assert_eq!(queue.pop(), None);
        assert!(matches!(&queue.storage, super::EventStorage::Grouped(_)));
        Ok(())
    }

    #[test]
    fn sequence_rebase_preserves_fifo_in_both_storage_modes()
    -> Result<(), Box<dyn std::error::Error>> {
        let reaction = quiescence_core::ReactionId::from_index(0);
        let event = |priority, token| super::ScheduledEvent {
            at: SimTime::ZERO,
            priority,
            sequence: 0,
            kind: super::EventKind::ExternalReaction {
                reaction,
                token: Some(token),
            },
        };
        let mut grouped = super::EventQueue::new();
        grouped.push(event(0, 1));
        grouped.push(event(0, 2));
        grouped.next_sequence = u64::MAX;
        grouped.push(event(0, 3));
        assert_eq!(grouped.pop().map(|e| e.kind), Some(event(0, 1).kind));
        assert_eq!(grouped.pop().map(|e| e.kind), Some(event(0, 2).kind));
        assert_eq!(grouped.pop().map(|e| e.kind), Some(event(0, 3).kind));

        let mut sparse = super::EventQueue::new();
        for index in 0..=super::MAX_GROUPED_KEYS {
            sparse.push(event(i32::try_from(index)?, u64::try_from(index)?));
        }
        sparse.push(event(17, 999));
        assert!(matches!(&sparse.storage, super::EventStorage::Sparse(_)));
        sparse.next_sequence = u64::MAX;
        sparse.push(event(17, 1_000));
        for index in 0..=super::MAX_GROUPED_KEYS {
            assert_eq!(
                sparse.pop().map(|e| e.kind),
                Some(event(i32::try_from(index)?, u64::try_from(index)?).kind)
            );
            if index == 17 {
                assert_eq!(sparse.pop().map(|e| e.kind), Some(event(17, 999).kind));
                assert_eq!(sparse.pop().map(|e| e.kind), Some(event(17, 1_000).kind));
            }
        }
        assert_eq!(sparse.pop(), None);
        Ok(())
    }

    #[test]
    fn adaptive_queue_matches_reference_heap_under_interleaved_operations()
    -> Result<(), Box<dyn std::error::Error>> {
        // Deterministic mixes repeatedly cross the grouped/sparse threshold,
        // insert earlier priorities after later ones, and retry popped work.
        for distinct_keys in [1_u64, 64, 4_096] {
            let mut queue = super::EventQueue::new();
            let mut reference = std::collections::BinaryHeap::<super::HeapEvent>::new();
            let reaction = quiescence_core::ReactionId::from_index(0);
            let mut random = 0x9e37_79b9_7f4a_7c15_u64;
            let mut next_sequence = 0_u64;
            for turn in 0_u64..24_000 {
                random ^= random << 13;
                random ^= random >> 7;
                random ^= random << 17;
                if turn < 256 || random & 3 != 0 {
                    let key = (random >> 8) % distinct_keys;
                    let event = super::ScheduledEvent {
                        at: SimTime::from_nanos(i64::try_from(key / 4)?),
                        priority: i32::try_from(key % 4)? - 2,
                        sequence: next_sequence,
                        kind: super::EventKind::ExternalReaction {
                            reaction,
                            token: Some(turn),
                        },
                    };
                    next_sequence += 1;
                    queue.push(event.clone());
                    reference.push(super::HeapEvent(event));
                } else {
                    assert_eq!(queue.peek(), reference.peek().map(|entry| &entry.0));
                    let actual = queue.pop();
                    let expected = reference.pop().map(|entry| entry.0);
                    assert_eq!(actual, expected);
                    if random.trailing_zeros() >= 7
                        && let Some(event) = actual
                    {
                        queue.restore_front(event.clone());
                        reference.push(super::HeapEvent(event));
                    }
                }
            }
            while let Some(expected) = reference.pop() {
                assert_eq!(queue.pop(), Some(expected.0));
            }
            assert_eq!(queue.pop(), None);
        }
        Ok(())
    }

    #[test]
    fn sparse_heap_ordering_is_reflexive_for_nan_payloads() {
        let node = quiescence_core::NodeId::from_index(0);
        let event = super::HeapEvent(super::ScheduledEvent {
            at: SimTime::ZERO,
            priority: 0,
            sequence: 0,
            kind: super::EventKind::SetState {
                write: StateWrite::new(node, f64::NAN),
            },
        });
        assert_eq!(event, event);
    }

    #[test]
    fn newly_scheduled_higher_priority_callback_preempts_existing_event()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut graph = Graph::new(RuntimeConfig::default());
        let seen = Arc::new(Mutex::new(Vec::new()));
        let captured = Arc::clone(&seen);
        let target = graph.add_event_callback("target", move |context| {
            captured
                .lock()
                .map_err(|_| EvalError::Message(String::from("capture lock poisoned")))?
                .push((context.scheduled_key(), context.external_token()));
            Ok(ReactionOutput::idle())
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
    fn failed_scheduled_write_remains_in_causal_queue() -> Result<(), Box<dyn std::error::Error>> {
        let mut g = Graph::new(RuntimeConfig::default());
        let node = g.add_input("node", q0()?, 0.0)?;
        let at = SimTime::from_millis(8);
        g.schedule_input(node, at, 1e100)?;
        let original_event = g.events.peek().cloned();
        for _ in 0..2 {
            assert!(matches!(g.advance_to(at), Err(RuntimeError::Quantize(_))));
            assert_eq!(g.next_event_time(), Some(at));
            assert_eq!(g.events.peek().cloned(), original_event);
            assert!(!g.is_quiescent());
            assert_eq!(g.value_bucket(node)?, Bucket::new(0));
        }
        Ok(())
    }

    #[test]
    fn budget_exhaustion_preserves_pending_work_for_retry() -> Result<(), Box<dyn std::error::Error>>
    {
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
        g.config.same_time_microstep_budget = 2;
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
    fn scheduled_writes_validate_target_before_entering_queue()
    -> Result<(), Box<dyn std::error::Error>> {
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
    fn event_window_handles_full_i64_time_span() {
        let mut window = super::EventWindow {
            name: String::from("extreme time"),
            duration: SimDuration::from_nanos(i64::MAX),
            events: std::collections::VecDeque::new(),
        };
        window.record(SimTime::from_nanos(i64::MIN), 1);
        assert_eq!(window.count_at(SimTime::from_nanos(i64::MAX), 1), 0);
        window.record(SimTime::from_nanos(i64::MAX), 1);
        assert_eq!(window.events.len(), 1);
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
        let observed = Arc::clone(&callbacks);
        let callback = g.add_event_callback("background interaction", move |context| {
            observed
                .lock()
                .map_err(|_| EvalError::Message(String::from("callback capture lock poisoned")))?
                .push((context.now(), context.external_token()));
            Ok(ReactionOutput::idle())
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
    fn dropping_inbox_disconnects_all_sender_clones_and_releases_queue()
    -> Result<(), Box<dyn std::error::Error>> {
        let (sender, inbox) = interaction_channel();
        let clone = sender.clone();
        sender.state_now(0, Vec::new())?;
        drop(inbox);
        assert_eq!(
            sender.state_now(0, Vec::new()),
            Err(InteractionIngressError::Disconnected)
        );
        assert_eq!(
            clone.state_now(0, Vec::new()),
            Err(InteractionIngressError::Disconnected)
        );
        let pending = sender
            .shared
            .queue
            .lock()
            .map_err(|_| String::from("ingress lock poisoned"))?
            .pending
            .len();
        assert_eq!(pending, 0);
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
        let captured = Arc::clone(&seen);
        let callback = graph.add_event_callback("concurrent ingress", move |context| {
            captured
                .lock()
                .map_err(|_| EvalError::Message(String::from("capture lock poisoned")))?
                .push(context.external_token());
            Ok(ReactionOutput::idle())
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
    fn future_interactions_import_without_running_early() -> Result<(), Box<dyn std::error::Error>>
    {
        let mut g = Graph::new(RuntimeConfig::default());
        let tokens = Arc::new(Mutex::new(Vec::new()));
        let observed = Arc::clone(&tokens);
        let callback = g.add_event_callback("future ingress", move |context| {
            observed
                .lock()
                .map_err(|_| EvalError::Message(String::from("future capture lock poisoned")))?
                .push(context.external_token());
            Ok(ReactionOutput::idle())
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
        let observed = Arc::clone(&seen);
        let callback = g.add_event_callback("ordered callback", move |context| {
            observed
                .lock()
                .map_err(|_| EvalError::Message(String::from("capture poisoned")))?
                .push(context.external_token());
            Ok(ReactionOutput::idle())
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
        let observed = Arc::clone(&tokens);
        let callback = g.add_event_callback("tokenized external callback", move |context| {
            observed
                .lock()
                .map_err(|_| EvalError::Message(String::from("token capture lock poisoned")))?
                .push(context.external_token());
            Ok(ReactionOutput::idle())
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
    fn external_reaction_events_honor_priority_without_dedup()
    -> Result<(), Box<dyn std::error::Error>> {
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

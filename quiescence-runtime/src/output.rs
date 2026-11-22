use super::{NodeLabels, TransitionRecord};
use quiescence_core::{Bucket, Estimate, HistoryId, NodeId, ReactionId, SimDuration, SimTime};
use std::fmt;
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

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QuiescentSnapshot {
    pub at: SimTime,
    pub next_event_time: Option<SimTime>,
    pub(super) values: Vec<CanonicalValue>,
    pub labels: NodeLabels,
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

/// Destination of a keyed callback.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CallbackTarget {
    CurrentReaction,
    Reaction(ReactionId),
}

impl CallbackTarget {
    pub(super) const fn resolve(self, current: ReactionId) -> ReactionId {
        match self {
            Self::CurrentReaction => current,
            Self::Reaction(reaction) => reaction,
        }
    }
}

/// A keyed callback request. Requests are admitted in output order.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ScheduledCallback {
    pub target: CallbackTarget,
    pub key: u64,
    pub after: SimDuration,
    pub priority: i32,
}

/// Callback requests in emission order. A singleton needs no allocation.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub enum CallbackRequests {
    #[default]
    Empty,
    One(ScheduledCallback),
    Many(Vec<ScheduledCallback>),
}

impl CallbackRequests {
    /// Append to the ordered batch, allocating only when a second request arrives.
    pub fn push(&mut self, request: ScheduledCallback) {
        match self {
            Self::Empty => *self = Self::One(request),
            Self::One(first) => *self = Self::Many(vec![*first, request]),
            Self::Many(requests) => requests.push(request),
        }
    }

    #[must_use = "the callback requests must be considered"]
    pub fn as_slice(&self) -> &[ScheduledCallback] {
        match self {
            Self::Empty => &[],
            Self::One(request) => std::slice::from_ref(request),
            Self::Many(requests) => requests,
        }
    }
}

/// State and history effects applied atomically at a simulation time.
#[derive(Clone, Debug, PartialEq)]
pub struct ReactionTransition {
    pub after: SimDuration,
    pub writes: Vec<StateWrite>,
    pub window_records: Vec<WindowRecord>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ReactionOutput {
    pub transition: Option<ReactionTransition>,
    pub callbacks: CallbackRequests,
}

impl ReactionOutput {
    #[must_use = "the reaction result must be returned to the scheduler"]
    pub const fn idle() -> Self {
        Self {
            transition: None,
            callbacks: CallbackRequests::Empty,
        }
    }

    #[must_use = "the constructed evaluation or reaction result must be used"]
    pub const fn after(delay: SimDuration, writes: Vec<StateWrite>) -> Self {
        Self {
            transition: Some(ReactionTransition {
                after: delay,
                writes,
                window_records: Vec::new(),
            }),
            callbacks: CallbackRequests::Empty,
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
        Self::idle().and_schedule_once_after(delay, key, priority)
    }

    #[must_use = "the updated reaction result contains the new scheduling request"]
    pub fn and_schedule_once_after(self, delay: SimDuration, key: u64, priority: i32) -> Self {
        self.and_schedule(ScheduledCallback {
            target: CallbackTarget::CurrentReaction,
            key,
            after: delay,
            priority,
        })
    }

    #[must_use = "the callback request must be returned to the scheduler"]
    pub fn schedule_reaction_once_after(
        reaction: ReactionId,
        delay: SimDuration,
        key: u64,
        priority: i32,
    ) -> Self {
        Self::idle().and_schedule_reaction_once_after(reaction, delay, key, priority)
    }

    #[must_use = "the updated reaction result contains the callback request"]
    pub fn and_schedule_reaction_once_after(
        self,
        reaction: ReactionId,
        delay: SimDuration,
        key: u64,
        priority: i32,
    ) -> Self {
        self.and_schedule(ScheduledCallback {
            target: CallbackTarget::Reaction(reaction),
            key,
            after: delay,
            priority,
        })
    }

    #[must_use = "the updated reaction result contains the callback request"]
    pub fn and_schedule(mut self, callback: ScheduledCallback) -> Self {
        self.callbacks.push(callback);
        self
    }

    /// Record at the transition time, or immediately if no transition was requested.
    #[must_use = "the updated reaction result contains the history record"]
    pub fn and_record_window(mut self, history: HistoryId, key: u64) -> Self {
        self.transition
            .get_or_insert_with(|| ReactionTransition {
                after: SimDuration::ZERO,
                writes: Vec::new(),
                window_records: Vec::new(),
            })
            .window_records
            .push(WindowRecord::new(history, key));
        self
    }
}

impl EvalOutput {
    #[must_use = "the constructed evaluation or reaction result must be used"]
    pub const fn immediate(value: f64) -> Self {
        Self::after(value, SimDuration::ZERO)
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

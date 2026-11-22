use super::TransitionRecord;
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
        Self::idle().and_schedule_once_after(delay, key, priority)
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
        Self::idle().and_schedule_reaction_once_after(reaction, delay, key, priority)
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

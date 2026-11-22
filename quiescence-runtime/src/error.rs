use super::{InteractionImport, WorkId};
use quiescence_core::{HistoryId, NodeId, QuantizeError, ReactionId, SimDuration, SimTime};
use std::fmt;
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
    GenerationExhausted(WorkId),
    TimeOverflow {
        now: SimTime,
        duration: SimDuration,
    },
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
            Self::DuplicateStateWrite(id) => {
                write!(f, "state batch writes node {id:?} more than once")
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
            Self::GenerationExhausted(work) => {
                write!(f, "forecast generation exhausted for {work:?}")
            }
            Self::TimeOverflow { now, duration } => write!(
                f,
                "simulation time {now:?} plus {duration:?} is unrepresentable"
            ),
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

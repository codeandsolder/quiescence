//! Hot canonical values, reverse links, and derived-only computation state.
//!
//! All nodes have a stable public `NodeId` indexing values and links. Derived
//! owners have an independent dense index; inputs need no evaluator,
//! forward subscriptions or forecast generation storage.
use super::edge_set::ReverseEdges;
use super::read_set::Dependencies;
use super::{CanonicalValue, EvalFn};
use quiescence_core::{Bucket, NodeId, Quantization};
use std::sync::Arc;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DerivedId(u32);

impl DerivedId {
    pub(super) const fn new(raw: u32) -> Self {
        Self(raw)
    }

    pub(super) const fn index(self) -> usize {
        self.0 as usize
    }
}

#[derive(Clone, Copy)]
pub struct NumericCell {
    pub(super) quantization: Quantization,
    pub(super) bucket: Bucket,
}

/// A node's write/evaluation role is immutable from registration onward.
#[derive(Clone, Copy)]
pub enum NodeState {
    Input(NumericCell),
    Derived { cell: NumericCell, owner: DerivedId },
    Atom(u64),
}

impl NodeState {
    const fn numeric(&self) -> Option<NumericCell> {
        match self {
            Self::Input(cell) | Self::Derived { cell, .. } => Some(*cell),
            Self::Atom(_) => None,
        }
    }

    pub(super) fn bucket(&self) -> Option<Bucket> {
        self.numeric().map(|cell| cell.bucket)
    }

    pub(super) fn decoded(&self) -> Option<f64> {
        self.numeric()
            .map(|cell| cell.quantization.decode(cell.bucket))
    }

    pub(super) const fn atom(&self) -> Option<u64> {
        match self {
            Self::Atom(value) => Some(*value),
            Self::Input(_) | Self::Derived { .. } => None,
        }
    }

    pub(super) const fn is_derived(&self) -> bool {
        matches!(self, Self::Derived { .. })
    }

    pub(super) const fn canonical(&self) -> CanonicalValue {
        match self {
            Self::Input(cell) | Self::Derived { cell, .. } => CanonicalValue::Numeric(cell.bucket),
            Self::Atom(value) => CanonicalValue::Atom(*value),
        }
    }
}

pub struct NodeLinks {
    pub(super) name: Arc<str>,
    pub(super) dependents: ReverseEdges,
}

pub struct DerivedNode {
    pub(super) node: NodeId,
    pub(super) evaluator: Option<EvalFn>,
    pub(super) dependencies: Dependencies,
    pub(super) generation: u64,
    pub(super) pending_forecasts: u8,
}

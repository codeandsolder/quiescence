//! Validation and atomic staging of independent numeric and atom writes.
use super::{CanonicalValue, Graph, Node, NodeState, RuntimeError, StateWrite};
use quiescence_core::{Bucket, NodeId, PointRequest, Quantization};
use std::collections::BTreeSet;

/// IDs are dense graph-local indices. Use no storage for strictly ordered
/// batches; for unordered batches choose a bitmap when its span costs at
/// most one machine word per write, otherwise use sparse membership.
enum DuplicateTracker {
    Ordered,
    Bitmap { first: usize, words: Vec<u64> },
    Sparse(BTreeSet<NodeId>),
}

impl DuplicateTracker {
    fn new(writes: &[StateWrite], node_count: usize) -> Self {
        if writes
            .windows(2)
            .all(|pair| pair[0].node() < pair[1].node())
            || writes
                .windows(2)
                .all(|pair| pair[0].node() > pair[1].node())
        {
            return Self::Ordered;
        }
        // Unknown IDs are reported by target validation before insertion,
        // and must never be allowed to inflate the bitmap allocation.
        let (first, last) = writes
            .iter()
            .map(|write| write.node().index())
            .filter(|&index| index < node_count)
            .fold((usize::MAX, 0), |(lo, hi), index| {
                (lo.min(index), hi.max(index))
            });
        if first <= last {
            let count = (last - first) / 64 + 1;
            if count <= writes.len() {
                return Self::Bitmap {
                    first,
                    words: vec![0; count],
                };
            }
        }
        Self::Sparse(BTreeSet::new())
    }

    /// Returns false exactly when the validated node was already present.
    fn insert(&mut self, id: NodeId) -> bool {
        match self {
            Self::Ordered => true,
            Self::Sparse(seen) => seen.insert(id),
            Self::Bitmap { first, words } => {
                let offset = id.index() - *first;
                let word = &mut words[offset / 64];
                let mask = 1 << (offset % 64);
                let inserted = *word & mask == 0;
                *word |= mask;
                inserted
            }
        }
    }
}

pub struct ResolvedWrite {
    node: NodeId,
    next: CanonicalValue,
}

enum ValidatedWrite {
    Numeric(PointRequest),
    Atom { current: u64, next: u64 },
}

impl ValidatedWrite {
    fn resolve(self, node: NodeId) -> Result<Option<ResolvedWrite>, RuntimeError> {
        let next = match self {
            Self::Numeric(request) => {
                let bucket = request.resolve()?;
                if request.previous == Some(bucket) {
                    return Ok(None);
                }
                CanonicalValue::Numeric(bucket)
            }
            Self::Atom { current, next } => {
                if current == next {
                    return Ok(None);
                }
                CanonicalValue::Atom(next)
            }
        };
        Ok(Some(ResolvedWrite { node, next }))
    }
}

impl Graph {
    fn validated_writes<'a>(
        &'a self,
        writes: &'a [StateWrite],
    ) -> impl Iterator<Item = Result<(NodeId, ValidatedWrite), RuntimeError>> + 'a {
        let mut seen = DuplicateTracker::new(writes, self.nodes.len());
        writes.iter().map(move |&write| {
            let id = write.node();
            let node = self.node(id)?;
            if !seen.insert(id) {
                return Err(RuntimeError::DuplicateStateWrite(id));
            }
            Self::validate_write(node, write).map(|validated| (id, validated))
        })
    }

    const fn validate_write(
        node: &Node,
        write: StateWrite,
    ) -> Result<ValidatedWrite, RuntimeError> {
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
            ) => ValidatedWrite::Numeric(PointRequest {
                quantization: *quantization,
                previous: Some(*bucket),
                value,
            }),
            (StateWrite::Atom { value, .. }, NodeState::Atom(current)) => ValidatedWrite::Atom {
                current: *current,
                next: value,
            },
            (StateWrite::Numeric { .. }, NodeState::Atom(_)) => {
                return Err(RuntimeError::ExpectedNumeric(id));
            }
            (StateWrite::Atom { .. }, NodeState::Numeric { .. }) => {
                return Err(RuntimeError::ExpectedAtom(id));
            }
        };
        Ok(validated)
    }

    pub(super) fn validate_state_write_batch(
        &self,
        writes: &[StateWrite],
    ) -> Result<(), RuntimeError> {
        // Point estimates have state-independent representability. Reject
        // invalid values at admission, but resolve hysteresis again at delivery.
        self.visit_resolved_writes(writes, |_| {})
    }

    pub(super) fn apply_state_writes(&mut self, writes: &[StateWrite]) -> Result<(), RuntimeError> {
        if let [write] = writes {
            let id = write.node();
            if let Some(resolved) = Self::validate_write(self.node(id)?, *write)?.resolve(id)? {
                self.commit(resolved.node, resolved.next)?;
            }
            return Ok(());
        }
        let resolved = self.resolve_state_writes(writes)?;
        self.commit_resolved_state_writes(resolved)
    }

    pub(super) fn resolve_state_writes(
        &self,
        writes: &[StateWrite],
    ) -> Result<Vec<ResolvedWrite>, RuntimeError> {
        let mut resolved = Vec::with_capacity(writes.len());
        self.visit_resolved_writes(writes, |write| resolved.push(write))?;
        Ok(resolved)
    }

    // Validation and resolution share the same ordered traversal at admission
    // and delivery. Staging keeps graph commits atomic even with mixed writes.
    fn visit_resolved_writes(
        &self,
        writes: &[StateWrite],
        mut visit: impl FnMut(ResolvedWrite),
    ) -> Result<(), RuntimeError> {
        const BLOCK: usize = 8;
        let validated = self.validated_writes(writes);
        if writes.len() < BLOCK {
            for write in validated {
                let (node, write) = write?;
                if let Some(write) = write.resolve(node)? {
                    visit(write);
                }
            }
            return Ok(());
        }
        let mut points = [PointRequest {
            quantization: Quantization::INTEGER,
            previous: None,
            value: 0.0,
        }; BLOCK];
        let mut nodes = [NodeId::from_index(0); BLOCK];
        let mut count = 0;
        for write in validated {
            let (node, write) = match write {
                Ok(write) => write,
                Err(error) => {
                    Self::visit_point_run(&points[..count], &nodes[..count], &mut visit)?;
                    return Err(error);
                }
            };
            match write {
                ValidatedWrite::Numeric(request) => {
                    points[count] = request;
                    nodes[count] = node;
                    count += 1;
                    if count == BLOCK {
                        Self::visit_point_run(&points, &nodes, &mut visit)?;
                        count = 0;
                    }
                }
                atom @ ValidatedWrite::Atom { .. } => {
                    Self::visit_point_run(&points[..count], &nodes[..count], &mut visit)?;
                    count = 0;
                    if let Some(write) = atom.resolve(node)? {
                        visit(write);
                    }
                }
            }
        }
        Self::visit_point_run(&points[..count], &nodes[..count], &mut visit)
    }

    #[inline]
    fn visit_point_run(
        points: &[PointRequest],
        nodes: &[NodeId],
        visit: &mut impl FnMut(ResolvedWrite),
    ) -> Result<(), RuntimeError> {
        let mut buckets = [Bucket::new(0); 8];
        let output = &mut buckets[..points.len()];
        PointRequest::resolve_batch(points, output)?;
        for ((request, &bucket), &node) in points.iter().zip(output.iter()).zip(nodes) {
            if request.previous != Some(bucket) {
                visit(ResolvedWrite {
                    node,
                    next: CanonicalValue::Numeric(bucket),
                });
            }
        }
        Ok(())
    }

    pub(super) fn commit_resolved_state_writes(
        &mut self,
        resolved: Vec<ResolvedWrite>,
    ) -> Result<(), RuntimeError> {
        for ResolvedWrite { node, next } in resolved {
            self.commit(node, next)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn adaptive_duplicate_tracking_matches_reference_membership() {
        let cases: [(usize, &[u32]); 5] = [
            (10, &[0, 1, 2, 3]),
            (10, &[3, 2, 1, 0]),
            (1000, &[63, 3, 64, 127, 3, 64, 63]),
            (1_000_001, &[1_000_000, 0, 1, 1_000_000, 0]),
            (128, &[65, u32::MAX, 1, 65, 1, 64]),
        ];
        for (count, indices) in cases {
            let writes: Vec<_> = indices
                .iter()
                .map(|&index| StateWrite::atom(NodeId::from_index(index), 1))
                .collect();
            let mut tracker = DuplicateTracker::new(&writes, count);
            let mut reference = BTreeSet::new();
            for id in writes
                .iter()
                .map(|write| write.node())
                .filter(|id| id.index() < count)
            {
                assert_eq!(
                    tracker.insert(id),
                    reference.insert(id),
                    "{indices:?}, {id:?}"
                );
            }
        }
        let dense = [
            StateWrite::atom(NodeId::from_index(64), 1),
            StateWrite::atom(NodeId::from_index(1), 1),
            StateWrite::atom(NodeId::from_index(64), 1),
        ];
        assert!(matches!(
            DuplicateTracker::new(&dense, 65),
            DuplicateTracker::Bitmap { .. }
        ));
        let sparse = [
            StateWrite::atom(NodeId::from_index(100_000), 1),
            StateWrite::atom(NodeId::from_index(1), 1),
            StateWrite::atom(NodeId::from_index(100_000), 1),
        ];
        assert!(matches!(
            DuplicateTracker::new(&sparse, 100_001),
            DuplicateTracker::Sparse(_)
        ));
    }

    #[test]
    fn mixed_batches_preserve_order_and_skip_only_unchanged_values()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut graph = Graph::new(crate::RuntimeConfig::default());
        let q = Quantization::new(1, 1)?;
        let mut writes = Vec::new();
        let mut expected = Vec::new();
        for i in 0..43_u32 {
            let changed = i % 3 != 0;
            if i == 11 || i == 32 {
                let node = graph.add_atom_input("atom", 7)?;
                writes.push(StateWrite::atom(node, if changed { 8 } else { 7 }));
                if changed {
                    expected.push((node, CanonicalValue::Atom(8)));
                }
            } else {
                let node = graph.add_input("numeric", q, 1.0)?;
                writes.push(StateWrite::new(node, if changed { 2.25 } else { 1.04 }));
                if changed {
                    expected.push((node, CanonicalValue::Numeric(Bucket::new(23))));
                }
            }
        }
        // Descending ID order has the same semantics as ascending or arbitrary order.
        writes.reverse();
        expected.reverse();
        graph.set_state_now(&writes)?;
        let transitions: Vec<_> = graph
            .take_transitions()
            .into_iter()
            .map(|record| (record.node, record.new))
            .collect();
        assert_eq!(transitions, expected);
        graph.set_state_now(&writes)?;
        assert_eq!(graph.take_transitions(), []);
        Ok(())
    }

    #[test]
    fn failed_numeric_lane_precedes_later_validation_error_and_batch_is_atomic()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut graph = Graph::new(crate::RuntimeConfig::default());
        let q = Quantization::INTEGER;
        let mut writes = Vec::new();
        let mut ids = Vec::new();
        for _ in 0..19 {
            let node = graph.add_input("numeric", q, 0.0)?;
            writes.push(StateWrite::new(node, 4.0));
            ids.push(node);
        }
        // Earlier complete SIMD blocks have resolved, but nothing may commit.
        writes[17] = StateWrite::new(ids[17], f64::NAN);
        writes[18] = StateWrite::new(NodeId::from_index(u32::MAX), 1.0);
        assert!(matches!(
            graph.set_state_now(&writes),
            Err(RuntimeError::Quantize(
                quiescence_core::QuantizeError::NonFiniteEstimate
            ))
        ));
        assert_eq!(graph.take_transitions(), []);
        for id in ids {
            assert_eq!(graph.value_bucket(id)?, Bucket::new(0));
        }
        Ok(())
    }

    #[test]
    fn nonadjacent_duplicates_in_large_shuffled_batch_reject_every_write()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut graph = Graph::new(crate::RuntimeConfig::default());
        let mut writes = Vec::new();
        for _ in 0..73 {
            let node = graph.add_atom_input("atom", 0)?;
            writes.push(StateWrite::atom(node, 1));
        }
        writes.swap(0, 52);
        let duplicate = writes[21];
        writes.push(duplicate);
        assert!(
            matches!(graph.set_state_now(&writes), Err(RuntimeError::DuplicateStateWrite(id)) if id == duplicate.node())
        );
        assert_eq!(graph.take_transitions(), []);
        Ok(())
    }
}

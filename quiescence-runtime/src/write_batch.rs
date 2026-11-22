//! Validation and atomic staging of independent numeric and atom writes.
use super::read_set::{EpochMarks, ReadTracker};
use super::{CanonicalValue, Graph, NodeState, RuntimeError, StateWrite};
use quiescence_core::{Bucket, NodeId, PointRequest, PreparedPoint, Quantization};

#[derive(Clone, Copy)]
pub struct ResolvedWrite {
    node: NodeId,
    next: CanonicalValue,
}

/// A validated write whose point rounding is independent of delivery time.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AdmittedWrite {
    Interior { node: NodeId, bucket: Bucket },
    Midpoint { node: NodeId, bucket: Bucket },
    Atom { node: NodeId, next: u64 },
}

impl AdmittedWrite {
    const fn numeric(node: NodeId, point: PreparedPoint) -> Self {
        match point {
            PreparedPoint::Interior(bucket) => Self::Interior { node, bucket },
            PreparedPoint::Midpoint(bucket) => Self::Midpoint { node, bucket },
        }
    }

    fn deliver(self, graph: &mut Graph) -> Result<(), RuntimeError> {
        let (node, point) = match self {
            Self::Atom { node, next } => return graph.commit(node, CanonicalValue::Atom(next)),
            Self::Interior { node, bucket } => (node, PreparedPoint::Interior(bucket)),
            Self::Midpoint { node, bucket } => (node, PreparedPoint::Midpoint(bucket)),
        };
        // The target's existence, write role and policy are fixed since admission.
        let NodeState::Input(cell) = graph.values[node.index()] else {
            return Err(RuntimeError::NotAnInput(node));
        };
        let bucket = point.resolve(cell.bucket, cell.quantization.hysteresis_lsd());
        graph.commit(node, CanonicalValue::Numeric(bucket))
    }
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

enum WriteRun<'a> {
    Single(NodeId, ValidatedWrite),
    Points(&'a [NodeId], &'a [PointRequest]),
}

impl ValidatedWrite {
    fn admit(self, node: NodeId) -> Result<AdmittedWrite, RuntimeError> {
        Ok(match self {
            Self::Numeric(request) => AdmittedWrite::numeric(node, request.prepare()?),
            Self::Atom { next, .. } => AdmittedWrite::Atom { node, next },
        })
    }
}

impl Graph {
    fn validated_writes<'a>(
        nodes: &'a [NodeState],
        seen: &'a mut EpochMarks,
        writes: &'a [StateWrite],
    ) -> impl Iterator<Item = Result<(NodeId, ValidatedWrite), RuntimeError>> + 'a {
        writes.iter().map(move |&write| {
            let id = write.node();
            // Unknown targets must be reported before touching dense marks.
            let node = nodes.get(id.index()).ok_or(RuntimeError::UnknownNode(id))?;
            if !seen.insert(id) {
                return Err(RuntimeError::DuplicateStateWrite(id));
            }
            Self::validate_write(node, write).map(|validated| (id, validated))
        })
    }

    const fn validate_write(
        node: &NodeState,
        write: StateWrite,
    ) -> Result<ValidatedWrite, RuntimeError> {
        let id = write.node();
        let validated = match (write, node) {
            (_, NodeState::Derived { .. }) => return Err(RuntimeError::NotAnInput(id)),
            (StateWrite::Numeric { value, .. }, NodeState::Input(cell)) => {
                ValidatedWrite::Numeric(PointRequest {
                    quantization: cell.quantization,
                    previous: Some(cell.bucket),
                    value,
                })
            }
            (StateWrite::Atom { value, .. }, NodeState::Atom(current)) => ValidatedWrite::Atom {
                current: *current,
                next: value,
            },
            (StateWrite::Numeric { .. }, NodeState::Atom(_)) => {
                return Err(RuntimeError::ExpectedNumeric(id));
            }
            (StateWrite::Atom { .. }, NodeState::Input(_)) => {
                return Err(RuntimeError::ExpectedAtom(id));
            }
        };
        Ok(validated)
    }

    fn validate_single_write(
        nodes: &[NodeState],
        write: StateWrite,
    ) -> Result<(NodeId, ValidatedWrite), RuntimeError> {
        let id = write.node();
        let node = nodes.get(id.index()).ok_or(RuntimeError::UnknownNode(id))?;
        Ok((id, Self::validate_write(node, write)?))
    }

    fn resolve_single_write(
        nodes: &[NodeState],
        write: StateWrite,
    ) -> Result<Option<ResolvedWrite>, RuntimeError> {
        let (node, validated) = Self::validate_single_write(nodes, write)?;
        validated.resolve(node)
    }

    pub(super) fn admit_state_write(
        &self,
        write: StateWrite,
    ) -> Result<AdmittedWrite, RuntimeError> {
        let (node, validated) = Self::validate_single_write(&self.values, write)?;
        validated.admit(node)
    }

    pub(super) fn admit_state_batch(
        &mut self,
        writes: &[StateWrite],
    ) -> Result<Vec<AdmittedWrite>, RuntimeError> {
        let mut admitted = Vec::with_capacity(writes.len());
        Self::visit_validated_writes(&self.values, &mut self.reads, writes, |run| {
            match run {
                WriteRun::Single(node, write) => admitted.push(write.admit(node)?),
                WriteRun::Points(nodes, requests) => {
                    let mut points = [PreparedPoint::Interior(Bucket::new(0)); 8];
                    let output = &mut points[..requests.len()];
                    PointRequest::prepare_batch(requests, output)?;
                    admitted.extend(
                        nodes
                            .iter()
                            .copied()
                            .zip(output.iter().copied())
                            .map(|(node, point)| AdmittedWrite::numeric(node, point)),
                    );
                }
            }
            Ok(())
        })?;
        Ok(admitted)
    }

    pub(super) fn deliver_admitted_writes(
        &mut self,
        writes: &[AdmittedWrite],
    ) -> Result<(), RuntimeError> {
        // Admission proved every target and value valid, with no duplicates.
        // Apply independent writes in original order; evaluators run afterward.
        writes.iter().try_for_each(|write| write.deliver(self))
    }

    pub(super) fn apply_state_writes(&mut self, writes: &[StateWrite]) -> Result<(), RuntimeError> {
        if let [write] = writes {
            if let Some(resolved) = Self::resolve_single_write(&self.values, *write)? {
                self.commit(resolved.node, resolved.next)?;
            }
            return Ok(());
        }
        let resolved = self.resolve_state_writes(writes)?;
        self.commit_resolved_state_writes(resolved)
    }

    pub(super) fn resolve_state_writes(
        &mut self,
        writes: &[StateWrite],
    ) -> Result<Vec<ResolvedWrite>, RuntimeError> {
        if writes.is_empty() {
            return Ok(Vec::new());
        }
        let mut resolved = std::mem::take(&mut self.write_scratch);
        let result = Self::visit_resolved_writes(&self.values, &mut self.reads, writes, |write| {
            resolved.push(write);
        });
        if let Err(error) = result {
            resolved.clear();
            self.recycle_write_scratch(resolved);
            return Err(error);
        }
        Ok(resolved)
    }

    fn visit_resolved_writes(
        graph_nodes: &[NodeState],
        tracker: &mut ReadTracker,
        writes: &[StateWrite],
        mut visit: impl FnMut(ResolvedWrite),
    ) -> Result<(), RuntimeError> {
        Self::visit_validated_writes(graph_nodes, tracker, writes, |run| {
            match run {
                WriteRun::Single(node, write) => {
                    if let Some(resolved) = write.resolve(node)? {
                        visit(resolved);
                    }
                }
                WriteRun::Points(nodes, requests) => {
                    Self::visit_point_run(requests, nodes, &mut visit)?;
                }
            }
            Ok(())
        })
    }

    // Admission and immediate resolution share the same ordered validation.
    // A numeric run is flushed before a later validation error or an atom.
    fn visit_validated_writes(
        graph_nodes: &[NodeState],
        tracker: &mut ReadTracker,
        writes: &[StateWrite],
        mut visit: impl FnMut(WriteRun<'_>) -> Result<(), RuntimeError>,
    ) -> Result<(), RuntimeError> {
        const BLOCK: usize = 8;
        if writes.is_empty() {
            return Ok(());
        }
        if let [write] = writes {
            let (node, write) = Self::validate_single_write(graph_nodes, *write)?;
            return visit(WriteRun::Single(node, write));
        }
        let validated = Self::validated_writes(graph_nodes, tracker.begin_membership(), writes);
        if writes.len() < BLOCK {
            for write in validated {
                let (node, write) = write?;
                visit(WriteRun::Single(node, write))?;
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
                    visit(WriteRun::Points(&nodes[..count], &points[..count]))?;
                    return Err(error);
                }
            };
            match write {
                ValidatedWrite::Numeric(request) => {
                    points[count] = request;
                    nodes[count] = node;
                    count += 1;
                    if count == BLOCK {
                        visit(WriteRun::Points(&nodes, &points))?;
                        count = 0;
                    }
                }
                atom @ ValidatedWrite::Atom { .. } => {
                    visit(WriteRun::Points(&nodes[..count], &points[..count]))?;
                    count = 0;
                    visit(WriteRun::Single(node, atom))?;
                }
            }
        }
        visit(WriteRun::Points(&nodes[..count], &points[..count]))
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
        mut resolved: Vec<ResolvedWrite>,
    ) -> Result<(), RuntimeError> {
        let result = resolved
            .iter()
            .copied()
            .try_for_each(|ResolvedWrite { node, next }| self.commit(node, next));
        resolved.clear();
        self.recycle_write_scratch(resolved);
        result
    }

    fn recycle_write_scratch(&mut self, resolved: Vec<ResolvedWrite>) {
        if resolved.capacity() > self.write_scratch.capacity() {
            self.write_scratch = resolved;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
            .map(|record| (record.node, record.new()))
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
    #[test]
    fn write_staging_reuses_capacity_and_abandons_partial_failed_batches()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut graph = Graph::new(crate::RuntimeConfig::default());
        let writes = (0..73)
            .map(|_| {
                graph
                    .add_atom_input("atom", 0)
                    .map(|id| StateWrite::atom(id, 0))
            })
            .collect::<Result<Vec<_>, _>>()?;
        graph.set_state_now(&writes)?;
        assert_eq!(graph.write_scratch.capacity(), 0);
        let updates: Vec<_> = writes
            .iter()
            .map(|write| StateWrite::atom(write.node(), 1))
            .collect();
        graph.set_state_now(&updates)?;
        let capacity = graph.write_scratch.capacity();
        assert!(capacity >= updates.len());
        assert!(graph.write_scratch.is_empty());
        let allocation = graph.write_scratch.as_ptr();
        let mut bad = writes.clone();
        bad.push(writes[0]);
        assert!(matches!(
            graph.set_state_now(&bad),
            Err(RuntimeError::DuplicateStateWrite(_))
        ));
        assert_eq!(graph.write_scratch.capacity(), capacity);
        assert!(graph.write_scratch.is_empty());
        for write in &writes {
            assert_eq!(graph.value_atom(write.node())?, 1);
        }
        graph.set_state_now(&writes)?;
        assert_eq!(graph.write_scratch.as_ptr(), allocation);
        assert!(graph.write_scratch.is_empty());
        Ok(())
    }
}

//! Compact causal records and immutable, structurally shared node labels.
use crate::CanonicalValue;
use quiescence_core::{Bucket, NodeId, SimTime};
use std::sync::{Arc, OnceLock};

const LABEL_CHUNK: usize = 256;
type LabelChunk = Arc<Vec<Arc<str>>>;

/// An immutable snapshot of the graph's append-only node labels.
///
/// Cloning the catalog is constant time. Registration after publication
/// shares already-sealed chunks and copies only the directory and partial tail.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct NodeLabels {
    chunks: Option<Arc<Vec<LabelChunk>>>,
}

impl NodeLabels {
    /// Look up an immutable label by graph-local identifier.
    #[must_use = "the node label must be considered"]
    pub fn name(&self, node: NodeId) -> Option<&str> {
        let index = node.index();
        self.chunks
            .as_ref()?
            .get(index / LABEL_CHUNK)?
            .get(index % LABEL_CHUNK)
            .map(AsRef::as_ref)
    }
}

/// Owner-side append buffer. Only snapshot operations construct a shared
/// directory; node registration never copies a previous publication.
pub struct LabelStore {
    sealed: Vec<LabelChunk>,
    tail: Vec<Arc<str>>,
    snapshot: OnceLock<NodeLabels>,
}

impl LabelStore {
    pub(super) const fn new() -> Self {
        Self {
            sealed: Vec::new(),
            tail: Vec::new(),
            snapshot: OnceLock::new(),
        }
    }

    pub(super) fn push(&mut self, label: Arc<str>) {
        self.snapshot.take();
        self.tail.push(label);
        if self.tail.len() == LABEL_CHUNK {
            self.sealed.push(Arc::new(std::mem::take(&mut self.tail)));
        }
    }

    pub(super) fn name(&self, node: NodeId) -> Option<&str> {
        let index = node.index();
        let chunk = index / LABEL_CHUNK;
        let labels = if chunk == self.sealed.len() {
            &self.tail
        } else {
            self.sealed.get(chunk)?.as_ref()
        };
        labels.get(index % LABEL_CHUNK).map(AsRef::as_ref)
    }

    pub(super) fn snapshot(&self) -> NodeLabels {
        self.snapshot
            .get_or_init(|| {
                if self.sealed.is_empty() && self.tail.is_empty() {
                    return NodeLabels::default();
                }
                let mut chunks =
                    Vec::with_capacity(self.sealed.len() + usize::from(!self.tail.is_empty()));
                chunks.extend(self.sealed.iter().cloned());
                if !self.tail.is_empty() {
                    chunks.push(Arc::new(self.tail.clone()));
                }
                NodeLabels {
                    chunks: Some(Arc::new(chunks)),
                }
            })
            .clone()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TransitionKind {
    Numeric,
    Atom,
}

impl TransitionKind {
    const fn decode(self, raw: u64) -> CanonicalValue {
        match self {
            Self::Numeric => CanonicalValue::Numeric(Bucket::new(raw.cast_signed())),
            Self::Atom => CanonicalValue::Atom(raw),
        }
    }
}

/// One committed change. Both values share the immutable node kind.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TransitionRecord {
    pub at: SimTime,
    pub node: NodeId,
    pub(super) kind: TransitionKind,
    pub(super) before: u64,
    pub(super) after: u64,
}

impl TransitionRecord {
    #[must_use = "the previous canonical value must be considered"]
    pub const fn previous(&self) -> CanonicalValue {
        self.kind.decode(self.before)
    }

    #[must_use = "the committed canonical value must be considered"]
    pub const fn current(&self) -> CanonicalValue {
        self.kind.decode(self.after)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn catalog_snapshots_share_immutable_labels_across_chunk_boundaries()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut labels = LabelStore::new();
        let empty = labels.snapshot();
        for index in 0..255_u32 {
            labels.push(format!("label {index}").into());
        }
        let first = labels.snapshot();
        labels.push("boundary".into());
        let full = labels.snapshot();
        labels.push("next chunk".into());
        let next = labels.snapshot();
        labels.push("another".into());
        let id = NodeId::from_index(0);
        assert!(empty.name(id).is_none());
        for snapshot in [&first, &full, &next, &labels.snapshot()] {
            assert!(std::ptr::eq(
                snapshot.name(id).ok_or("missing first label")?,
                first.name(id).ok_or("missing first snapshot label")?
            ));
        }
        assert_eq!(first.name(NodeId::from_index(255)), None);
        assert_eq!(full.name(NodeId::from_index(255)), Some("boundary"));
        assert_eq!(full.name(NodeId::from_index(256)), None);
        assert_eq!(next.name(NodeId::from_index(256)), Some("next chunk"));
        assert_eq!(next.name(NodeId::from_index(257)), None);
        assert_eq!(labels.name(NodeId::from_index(u32::MAX)), None);
        drop(labels);
        assert_eq!(next.name(NodeId::from_index(256)), Some("next chunk"));
        Ok(())
    }

    #[test]
    fn compact_records_round_trip_full_integer_domains() {
        for kind in [TransitionKind::Numeric, TransitionKind::Atom] {
            for before in [
                0,
                1,
                i64::MAX.cast_unsigned(),
                i64::MIN.cast_unsigned(),
                u64::MAX,
            ] {
                for after in [
                    0,
                    1,
                    i64::MAX.cast_unsigned(),
                    i64::MIN.cast_unsigned(),
                    u64::MAX,
                ] {
                    let record = TransitionRecord {
                        at: SimTime::from_nanos(i64::MIN),
                        node: NodeId::from_index(u32::MAX),
                        kind,
                        before,
                        after,
                    };
                    let value = |raw: u64| match kind {
                        TransitionKind::Numeric => {
                            CanonicalValue::Numeric(Bucket::new(raw.cast_signed()))
                        }
                        TransitionKind::Atom => CanonicalValue::Atom(raw),
                    };
                    assert_eq!(record.previous(), value(before));
                    assert_eq!(record.current(), value(after));
                }
            }
        }
    }
}

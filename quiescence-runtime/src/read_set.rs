//! Reusable unique-read collection and compact immutable dependency lists.
use quiescence_core::NodeId;
use std::cmp::Ordering;

const INLINE: usize = 6;

/// Once an evaluator succeeds, its subscriptions only need ordered traversal
/// and equality. Large forward sets therefore need just one dense allocation.
pub enum Dependencies {
    Inline { len: u8, ids: [NodeId; INLINE] },
    Heap(Box<[NodeId]>),
}

impl Default for Dependencies {
    fn default() -> Self {
        Self::new()
    }
}

impl Dependencies {
    pub(super) const fn new() -> Self {
        Self::Inline {
            len: 0,
            ids: [NodeId::from_index(0); INLINE],
        }
    }

    pub(super) fn from_sorted(values: &[NodeId]) -> Self {
        if values.len() <= INLINE
            && let Ok(len) = u8::try_from(values.len())
        {
            let mut ids = [NodeId::from_index(0); INLINE];
            ids[..values.len()].copy_from_slice(values);
            Self::Inline { len, ids }
        } else {
            Self::Heap(values.into())
        }
    }

    pub(super) fn as_slice(&self) -> &[NodeId] {
        match self {
            Self::Inline { len, ids } => &ids[..usize::from(*len)],
            Self::Heap(ids) => ids,
        }
    }

    #[cfg(test)]
    pub(super) fn contains(&self, id: NodeId) -> bool {
        self.as_slice().binary_search(&id).is_ok()
    }
}

/// The owner runs exactly one evaluator at a time. A per-node epoch lets us
/// deduplicate reads in O(1) without allocating or clearing a set each call.
/// Only touched IDs are sorted, and already-ordered observations skip sorting.
pub struct ReadTracker {
    marks: Vec<u32>,
    epoch: u32,
    observed: Vec<NodeId>,
    ordered: bool,
}

impl ReadTracker {
    pub(super) const fn new() -> Self {
        Self {
            marks: Vec::new(),
            epoch: 0,
            observed: Vec::new(),
            ordered: true,
        }
    }

    pub(super) fn register_node(&mut self) {
        self.marks.push(0);
    }

    pub(super) fn begin(&mut self) {
        self.observed.clear();
        self.ordered = true;
        self.epoch = self.epoch.wrapping_add(1);
        if self.epoch == 0 {
            self.marks.fill(0);
            self.epoch = 1;
        }
    }

    pub(super) fn record(&mut self, id: NodeId) {
        let mark = &mut self.marks[id.index()];
        if *mark != self.epoch {
            *mark = self.epoch;
            self.ordered &= self.observed.last().is_none_or(|&previous| previous < id);
            self.observed.push(id);
        }
    }

    pub(super) fn finish(&mut self) -> &[NodeId] {
        if !self.ordered {
            self.observed.sort_unstable();
        }
        &self.observed
    }
}

pub fn changes(mut old: &[NodeId], mut new: &[NodeId], mut changed: impl FnMut(NodeId, bool)) {
    while let (Some((&a, tail_a)), Some((&b, tail_b))) = (old.split_first(), new.split_first()) {
        match a.cmp(&b) {
            Ordering::Less => {
                changed(a, false);
                old = tail_a;
            }
            Ordering::Greater => {
                changed(b, true);
                new = tail_b;
            }
            Ordering::Equal => {
                old = tail_a;
                new = tail_b;
            }
        }
    }
    for &id in old {
        changed(id, false);
    }
    for &id in new {
        changed(id, true);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;

    #[test]
    fn epoch_deduplication_matches_reference_across_wrap_and_abandoned_evaluations() {
        let mut tracker = ReadTracker::new();
        for _ in 0..128 {
            tracker.register_node();
        }
        tracker.epoch = u32::MAX - 4;
        let mut random = 0x9e37_79b9_u32;
        for turn in 0..4096_u32 {
            tracker.begin();
            let mut reference = BTreeSet::new();
            for _ in 0..turn % 143 {
                random ^= random << 13;
                random ^= random >> 17;
                random ^= random << 5;
                let id = NodeId::from_index(random % 128);
                tracker.record(id);
                reference.insert(id);
            }
            // Failure paths abandon the scratch without publishing. The next
            // evaluation must never inherit observations or epoch marks.
            if turn % 5 != 0 {
                let actual = tracker.finish();
                assert!(actual.iter().eq(reference.iter()));
                let stored = Dependencies::from_sorted(actual);
                assert_eq!(stored.as_slice(), actual);
            }
        }
    }

    #[test]
    fn dependency_differences_match_sets() {
        for old_size in 0..12_u32 {
            for new_size in 0..12_u32 {
                let a: BTreeSet<_> = (0..old_size).map(|i| NodeId::from_index(i * 2)).collect();
                let b: BTreeSet<_> = (0..new_size).map(|i| NodeId::from_index(i * 3)).collect();
                let old: Vec<_> = a.iter().copied().collect();
                let new: Vec<_> = b.iter().copied().collect();
                let mut actual = Vec::new();
                changes(&old, &new, |id, add| actual.push((id, add)));
                let mut expected: Vec<_> = a
                    .symmetric_difference(&b)
                    .map(|&id| (id, b.contains(&id)))
                    .collect();
                expected.sort_unstable();
                assert_eq!(actual, expected);
            }
        }
    }
}

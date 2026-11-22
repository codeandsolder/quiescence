//! Ordered reverse edges with allocation-free small fan-out.
//!
//! Large reverse sets retain logarithmic insertion/removal, independently
//! of evaluator read order. Small fan-out uses compact inline storage.
use super::WorkId;
use quiescence_core::NodeId;
use std::collections::BTreeSet;

const INLINE: usize = 3;
const PLACEHOLDER: WorkId = WorkId::Node(NodeId::from_index(0));

pub enum ReverseEdges {
    Inline { len: u8, items: [WorkId; INLINE] },
    Tree(BTreeSet<WorkId>),
}

impl ReverseEdges {
    pub(super) const fn new() -> Self {
        Self::Inline {
            len: 0,
            items: [PLACEHOLDER; INLINE],
        }
    }

    pub(super) fn insert(&mut self, work: WorkId) {
        match self {
            Self::Inline { len, items } => {
                let count = usize::from(*len);
                let Err(index) = items[..count].binary_search(&work) else {
                    return;
                };
                if count < INLINE {
                    items.copy_within(index..count, index + 1);
                    items[index] = work;
                    *len += 1;
                } else {
                    let mut tree: BTreeSet<_> = items.iter().copied().collect();
                    tree.insert(work);
                    *self = Self::Tree(tree);
                }
            }
            Self::Tree(tree) => {
                tree.insert(work);
            }
        }
    }

    pub(super) fn remove(&mut self, work: WorkId) {
        match self {
            Self::Inline { len, items } => {
                let count = usize::from(*len);
                if let Ok(index) = items[..count].binary_search(&work) {
                    items.copy_within(index + 1..count, index);
                    *len -= 1;
                }
            }
            Self::Tree(tree) => {
                tree.remove(&work);
                // Hysteresis avoids continual promotion/demotion around INLINE.
                if tree.len() <= INLINE / 2
                    && let Ok(len) = u8::try_from(tree.len())
                {
                    let mut items = [PLACEHOLDER; INLINE];
                    for (slot, &edge) in items.iter_mut().zip(tree.iter()) {
                        *slot = edge;
                    }
                    *self = Self::Inline { len, items };
                }
            }
        }
    }

    pub(super) fn for_each(&self, mut visit: impl FnMut(WorkId)) {
        match self {
            Self::Inline { len, items } => {
                for &work in &items[..usize::from(*len)] {
                    visit(work);
                }
            }
            Self::Tree(tree) => {
                for &work in tree {
                    visit(work);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use quiescence_core::ReactionId;

    #[test]
    fn adaptive_storage_matches_ordered_reference() {
        let mut set = ReverseEdges::new();
        let mut reference = BTreeSet::new();
        let mut random = 0x9e37_79b9_u32;
        let mut observed = Vec::new();
        for turn in 0..16_384 {
            random ^= random << 13;
            random ^= random >> 17;
            random ^= random << 5;
            let key = random % if turn % 128 < 64 { 8 } else { 64 };
            let work = if random & 0x200 == 0 {
                WorkId::Node(NodeId::from_index(key))
            } else {
                WorkId::Reaction(ReactionId::from_index(key))
            };
            if random & 0x100 == 0 {
                set.insert(work);
                reference.insert(work);
            } else {
                set.remove(work);
                reference.remove(&work);
            }
            observed.clear();
            set.for_each(|edge| observed.push(edge));
            assert!(observed.iter().eq(reference.iter()));
        }
        for work in reference {
            set.remove(work);
        }
        assert!(matches!(set, ReverseEdges::Inline { len: 0, .. }));
    }
}

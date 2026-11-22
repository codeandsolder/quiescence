//! Ordered reverse edges with inline and density-adaptive storage.
use super::WorkId;
use quiescence_core::{NodeId, ReactionId};
use std::collections::{BTreeMap, BTreeSet, btree_map::Entry};

const INLINE: usize = 3;
const PLACEHOLDER: WorkId = WorkId::Node(NodeId::from_index(0));
const REACTION_WORD: u32 = 1 << 26;

fn word_and_mask(work: WorkId) -> (u32, u64) {
    let (raw, kind) = match work {
        WorkId::Node(id) => (id.raw(), 0),
        WorkId::Reaction(id) => (id.raw(), REACTION_WORD),
    };
    (kind | (raw >> 6), 1 << (raw & 63))
}

fn word_members(word: u32, mut mask: u64, mut visit: impl FnMut(WorkId)) {
    let base = (word & (REACTION_WORD - 1)) << 6;
    while mask != 0 {
        let raw = base | mask.trailing_zeros();
        visit(if word & REACTION_WORD == 0 {
            WorkId::Node(NodeId::from_index(raw))
        } else {
            WorkId::Reaction(ReactionId::from_index(raw))
        });
        mask &= mask - 1;
    }
}

#[derive(Default)]
struct Chunks {
    masks: BTreeMap<u32, u64>,
    count: usize,
}

impl Chunks {
    fn insert(&mut self, work: WorkId) {
        let (word, mask) = word_and_mask(work);
        let bits = self.masks.entry(word).or_default();
        if *bits & mask == 0 {
            *bits |= mask;
            self.count += 1;
        }
    }

    fn remove(&mut self, work: WorkId) {
        let (word, mask) = word_and_mask(work);
        if let Entry::Occupied(mut entry) = self.masks.entry(word)
            && *entry.get() & mask != 0
        {
            *entry.get_mut() &= !mask;
            self.count -= 1;
            if *entry.get() == 0 {
                entry.remove();
            }
        }
    }

    fn for_each(&self, mut visit: impl FnMut(WorkId)) {
        for (&word, &mask) in &self.masks {
            word_members(word, mask, &mut visit);
        }
    }
}

enum LargeStorage {
    Tree {
        members: BTreeSet<WorkId>,
        probe_at: usize,
    },
    Chunks(Chunks),
}

pub struct LargeEdges(LargeStorage);

impl LargeEdges {
    fn from_members(members: BTreeSet<WorkId>) -> Self {
        let mut previous = None;
        let mut words = 0;
        for &work in &members {
            let (word, _) = word_and_mask(work);
            if previous != Some(word) {
                previous = Some(word);
                words += 1;
            }
            // No later member can make this candidate dense enough.
            if words > members.len() / 2 {
                let probe_at = members.len().saturating_mul(2);
                return Self(LargeStorage::Tree { members, probe_at });
            }
        }
        let mut chunks = Chunks::default();
        for work in members {
            chunks.insert(work);
        }
        Self(LargeStorage::Chunks(chunks))
    }

    fn len(&self) -> usize {
        match &self.0 {
            LargeStorage::Tree { members, .. } => members.len(),
            LargeStorage::Chunks(chunks) => chunks.count,
        }
    }

    fn insert(&mut self, work: WorkId) {
        match &mut self.0 {
            LargeStorage::Tree { members, probe_at } => {
                if members.insert(work) && members.len() >= *probe_at {
                    *self = Self::from_members(std::mem::take(members));
                }
            }
            LargeStorage::Chunks(chunks) => {
                chunks.insert(work);
                self.maybe_sparsify();
            }
        }
    }

    fn remove(&mut self, work: WorkId) {
        match &mut self.0 {
            LargeStorage::Tree { members, probe_at } => {
                if members.remove(&work) && members.len() <= *probe_at / 4 {
                    *self = Self::from_members(std::mem::take(members));
                }
            }
            LargeStorage::Chunks(chunks) => {
                chunks.remove(work);
                self.maybe_sparsify();
            }
        }
    }

    fn maybe_sparsify(&mut self) {
        if let LargeStorage::Chunks(chunks) = &self.0
            && chunks.count >= INLINE + 1
            && chunks.masks.len() > chunks.count - chunks.count / 4
        {
            let mut members = BTreeSet::new();
            chunks.for_each(|work| {
                members.insert(work);
            });
            let probe_at = members.len().saturating_mul(2);
            self.0 = LargeStorage::Tree { members, probe_at };
        }
    }

    fn for_each(&self, mut visit: impl FnMut(WorkId)) {
        match &self.0 {
            LargeStorage::Tree { members, .. } => {
                for &work in members {
                    visit(work);
                }
            }
            LargeStorage::Chunks(chunks) => chunks.for_each(visit),
        }
    }
}

pub enum ReverseEdges {
    Inline { len: u8, items: [WorkId; INLINE] },
    Large(Box<LargeEdges>),
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
                    let mut members: BTreeSet<_> = items.iter().copied().collect();
                    members.insert(work);
                    *self = Self::Large(Box::new(LargeEdges::from_members(members)));
                }
            }
            Self::Large(edges) => edges.insert(work),
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
            Self::Large(edges) => {
                edges.remove(work);
                if edges.len() <= INLINE / 2 {
                    let mut items = [PLACEHOLDER; INLINE];
                    let mut len = 0_u8;
                    edges.for_each(|edge| {
                        items[usize::from(len)] = edge;
                        len += 1;
                    });
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
            Self::Large(edges) => edges.for_each(visit),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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

    #[test]
    fn full_identifier_space_and_density_transitions_match_reference() {
        let mut edges = ReverseEdges::new();
        let mut reference = BTreeSet::new();
        let dense = (0..128_u32)
            .flat_map(|id| {
                [
                    WorkId::Node(NodeId::from_index(u32::MAX - id)),
                    WorkId::Reaction(ReactionId::from_index(id)),
                ]
            })
            .collect::<Vec<_>>();
        let sparse = (0..1024_u32)
            .flat_map(|id| {
                [
                    WorkId::Node(NodeId::from_index(id * 1_048_576)),
                    WorkId::Reaction(ReactionId::from_index(u32::MAX - id * 1_048_576)),
                ]
            })
            .collect::<Vec<_>>();
        let mut observed = Vec::new();
        for (values, insert) in [
            (&dense, true),
            (&sparse, true),
            (&dense, false),
            (&dense, true),
            (&sparse, false),
            (&dense, false),
        ] {
            for &work in values {
                if insert {
                    edges.insert(work);
                    edges.insert(work);
                    reference.insert(work);
                } else {
                    edges.remove(work);
                    edges.remove(work);
                    reference.remove(&work);
                }
                observed.clear();
                edges.for_each(|edge| observed.push(edge));
                assert!(observed.iter().eq(reference.iter()));
            }
        }
        assert!(matches!(edges, ReverseEdges::Inline { len: 0, .. }));
    }
}

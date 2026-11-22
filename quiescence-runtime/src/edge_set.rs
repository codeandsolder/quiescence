//! Ordered reverse edges with inline and density-adaptive storage.
use super::WorkId;
use quiescence_core::{NodeId, ReactionId};
use std::collections::{BTreeMap, BTreeSet, btree_map::Entry};

const INLINE: usize = 3;
const KIND_BIT: u64 = 1 << 32;

const fn low_word(raw: u64) -> u32 {
    let [a, b, c, d, ..] = raw.to_le_bytes();
    u32::from_le_bytes([a, b, c, d])
}

/// The natural integer order matches `WorkId`: all node IDs precede
/// reaction IDs. Preserve all 32 bits of both identifier domains.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct EdgeKey(u64);

impl EdgeKey {
    const fn from_work(work: WorkId) -> Self {
        Self(match work {
            WorkId::Node(id) => id.raw() as u64,
            WorkId::Reaction(id) => KIND_BIT | id.raw() as u64,
        })
    }

    const fn work(self) -> WorkId {
        let id = low_word(self.0);
        if self.0 & KIND_BIT == 0 {
            WorkId::Node(NodeId::from_index(id))
        } else {
            WorkId::Reaction(ReactionId::from_index(id))
        }
    }

    const fn word_and_mask(self) -> (u32, u64) {
        (low_word(self.0 >> 6), 1 << (self.0 & 63))
    }
}

fn word_members(word: u32, mut mask: u64, mut visit: impl FnMut(EdgeKey)) {
    let base = u64::from(word) << 6;
    while mask != 0 {
        visit(EdgeKey(base | u64::from(mask.trailing_zeros())));
        mask &= mask - 1;
    }
}

#[derive(Default)]
struct Chunks {
    masks: BTreeMap<u32, u64>,
    count: usize,
}

impl Chunks {
    fn insert(&mut self, key: EdgeKey) {
        let (block, mask) = key.word_and_mask();
        let bits = self.masks.entry(block).or_default();
        if *bits & mask == 0 {
            *bits |= mask;
            self.count += 1;
        }
    }

    fn remove(&mut self, key: EdgeKey) {
        let (block, mask) = key.word_and_mask();
        if let Entry::Occupied(mut entry) = self.masks.entry(block)
            && *entry.get() & mask != 0
        {
            *entry.get_mut() &= !mask;
            self.count -= 1;
            if *entry.get() == 0 {
                entry.remove();
            }
        }
    }

    fn for_each(&self, mut visit: impl FnMut(EdgeKey)) {
        for (&word, &mask) in &self.masks {
            word_members(word, mask, &mut visit);
        }
    }
}

enum LargeStorage {
    Tree {
        members: BTreeSet<EdgeKey>,
        probe_at: usize,
    },
    Chunks(Chunks),
}

pub struct LargeEdges(LargeStorage);

impl LargeEdges {
    fn from_members(members: BTreeSet<EdgeKey>) -> Self {
        let mut previous = None;
        let mut words = 0;
        for &key in &members {
            let (word, _) = key.word_and_mask();
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
        for key in members {
            chunks.insert(key);
        }
        Self(LargeStorage::Chunks(chunks))
    }

    fn len(&self) -> usize {
        match &self.0 {
            LargeStorage::Tree { members, .. } => members.len(),
            LargeStorage::Chunks(chunks) => chunks.count,
        }
    }

    fn insert(&mut self, key: EdgeKey) {
        match &mut self.0 {
            LargeStorage::Tree { members, probe_at } => {
                if members.insert(key) && members.len() >= *probe_at {
                    *self = Self::from_members(std::mem::take(members));
                }
            }
            LargeStorage::Chunks(chunks) => {
                chunks.insert(key);
                self.maybe_sparsify();
            }
        }
    }

    fn remove(&mut self, key: EdgeKey) {
        match &mut self.0 {
            LargeStorage::Tree { members, probe_at } => {
                if members.remove(&key) && members.len() <= *probe_at / 4 {
                    *self = Self::from_members(std::mem::take(members));
                }
            }
            LargeStorage::Chunks(chunks) => {
                chunks.remove(key);
                self.maybe_sparsify();
            }
        }
    }

    fn maybe_sparsify(&mut self) {
        if let LargeStorage::Chunks(chunks) = &self.0
            && chunks.count > INLINE
            && chunks.masks.len() > chunks.count - chunks.count / 4
        {
            let mut members = BTreeSet::new();
            chunks.for_each(|key| {
                members.insert(key);
            });
            let probe_at = members.len().saturating_mul(2);
            self.0 = LargeStorage::Tree { members, probe_at };
        }
    }

    fn for_each(&self, mut visit: impl FnMut(EdgeKey)) {
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

/// Inline keys store the three 32-bit IDs and a separate mask of kinds.
/// This avoids three copies of the enum padding on every graph node.
pub struct InlineEdges {
    items: [u32; INLINE],
    kinds: u8,
    len: u8,
}

impl InlineEdges {
    const fn new() -> Self {
        Self {
            items: [0; INLINE],
            kinds: 0,
            len: 0,
        }
    }

    fn key(&self, index: usize) -> EdgeKey {
        EdgeKey(u64::from(self.items[index]) | (u64::from((self.kinds >> index) & 1) << 32))
    }

    fn set(&mut self, index: usize, key: EdgeKey) {
        self.items[index] = low_word(key.0);
        let mask = 1 << index;
        self.kinds = (self.kinds & !mask) | (u8::from(key.0 & KIND_BIT != 0) << index);
    }

    fn iter(&self) -> impl Iterator<Item = EdgeKey> + '_ {
        (0..usize::from(self.len)).map(|index| self.key(index))
    }

    // Return false only when the key is new and storage must grow.
    fn insert(&mut self, key: EdgeKey) -> bool {
        let len = usize::from(self.len);
        let index = (0..len)
            .find(|&index| self.key(index) >= key)
            .unwrap_or(len);
        if index < len && self.key(index) == key {
            return true;
        }
        if len == INLINE {
            return false;
        }
        for source in (index..len).rev() {
            self.set(source + 1, self.key(source));
        }
        self.set(index, key);
        self.len += 1;
        true
    }

    fn remove(&mut self, key: EdgeKey) {
        let len = usize::from(self.len);
        if let Some(index) = (0..len).find(|&index| self.key(index) == key) {
            for source in index + 1..len {
                self.set(source - 1, self.key(source));
            }
            self.len -= 1;
        }
    }
}

pub enum ReverseEdges {
    Inline(InlineEdges),
    Large(Box<LargeEdges>),
}

impl ReverseEdges {
    pub(super) const fn new() -> Self {
        Self::Inline(InlineEdges::new())
    }

    pub(super) fn insert(&mut self, work: WorkId) {
        let key = EdgeKey::from_work(work);
        match self {
            Self::Inline(inline) => {
                if !inline.insert(key) {
                    let mut members: BTreeSet<_> = inline.iter().collect();
                    members.insert(key);
                    *self = Self::Large(Box::new(LargeEdges::from_members(members)));
                }
            }
            Self::Large(edges) => edges.insert(key),
        }
    }

    pub(super) fn remove(&mut self, work: WorkId) {
        let key = EdgeKey::from_work(work);
        match self {
            Self::Inline(inline) => inline.remove(key),
            Self::Large(edges) => {
                edges.remove(key);
                if edges.len() <= INLINE / 2 {
                    let mut inline = InlineEdges::new();
                    edges.for_each(|key| {
                        inline.insert(key);
                    });
                    *self = Self::Inline(inline);
                }
            }
        }
    }

    pub(super) fn for_each(&self, mut visit: impl FnMut(WorkId)) {
        match self {
            Self::Inline(inline) => {
                for key in inline.iter() {
                    visit(key.work());
                }
            }
            Self::Large(edges) => edges.for_each(|key| visit(key.work())),
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
        assert!(matches!(
            set,
            ReverseEdges::Inline(InlineEdges { len: 0, .. })
        ));
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
        assert!(matches!(
            edges,
            ReverseEdges::Inline(InlineEdges { len: 0, .. })
        ));
    }
}

#[cfg(test)]
mod packed_tests {
    use super::*;

    #[test]
    fn packed_keys_preserve_full_domains_and_inline_order() {
        let members = [
            WorkId::Reaction(ReactionId::from_index(u32::MAX)),
            WorkId::Node(NodeId::from_index(u32::MAX)),
            WorkId::Reaction(ReactionId::from_index(0)),
            WorkId::Node(NodeId::from_index(0)),
        ];
        for &work in &members {
            assert_eq!(EdgeKey::from_work(work).work(), work);
        }
        let mut inline = InlineEdges::new();
        for &work in &members[..3] {
            assert!(inline.insert(EdgeKey::from_work(work)));
        }
        assert!(inline.insert(EdgeKey::from_work(members[1])));
        assert!(!inline.insert(EdgeKey::from_work(members[3])));
        inline.remove(EdgeKey::from_work(members[2]));
        assert!(inline.insert(EdgeKey::from_work(members[3])));
        assert_eq!(
            inline.iter().map(EdgeKey::work).collect::<Vec<_>>(),
            [members[3], members[1], members[0]]
        );
    }
}

//! Ordered reverse edges with inline and density-adaptive storage.
use super::WorkId;
use quiescence_core::{NodeId, ReactionId};
use std::collections::{BTreeMap, BTreeSet, VecDeque, btree_map::Entry};

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

const MIN_SPAN: usize = 16;

const fn span_worthwhile(span: usize, count: usize) -> bool {
    span <= count / 2 || (count >= 32 && span <= count.saturating_mul(2))
}

pub struct SparseEdges {
    members: BTreeSet<EdgeKey>,
    probe: usize,
}

pub struct WordTree {
    masks: BTreeMap<u32, u64>,
    count: usize,
    probe: usize,
}

impl WordTree {
    fn new(masks: BTreeMap<u32, u64>, count: usize) -> Self {
        Self {
            probe: masks.len().saturating_mul(2).max(2),
            masks,
            count,
        }
    }

    fn insert(&mut self, word: u32, mask: u64) {
        let bits = self.masks.entry(word).or_default();
        self.count += usize::from(*bits & mask == 0);
        *bits |= mask;
    }

    fn remove(&mut self, word: u32, mask: u64) {
        if let Entry::Occupied(mut entry) = self.masks.entry(word)
            && *entry.get() & mask != 0
        {
            self.count -= 1;
            *entry.get_mut() &= !mask;
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

pub struct WordSpan {
    first: u32,
    masks: VecDeque<u64>,
    count: usize,
}

impl WordSpan {
    fn insert(&mut self, word: u32, mask: u64) -> bool {
        if self.masks.is_empty() {
            self.first = word;
            self.masks.push_back(mask);
            self.count = 1;
            return true;
        }
        if let Some(offset) = word.checked_sub(self.first)
            && let Some(bits) = self.masks.get_mut(offset as usize)
        {
            self.count += usize::from(*bits & mask == 0);
            *bits |= mask;
            return true;
        }
        let lower = self.first.min(word);
        let upper = (self.first as usize + self.masks.len() - 1).max(word as usize);
        let span = upper - lower as usize + 1;
        // Bound any extension before allocating, including distant outliers.
        if span > MIN_SPAN.max(self.count.saturating_add(1).saturating_mul(4)) {
            return false;
        }
        if lower != self.first {
            let extra = (self.first - lower) as usize;
            for _ in 0..extra {
                self.masks.push_front(0);
            }
            self.first = lower;
        }
        self.masks.resize(span, 0);
        self.masks[(word - self.first) as usize] = mask;
        self.count += 1;
        true
    }

    fn remove(&mut self, word: u32, mask: u64) {
        let Some(offset) = word.checked_sub(self.first) else {
            return;
        };
        let Some(bits) = self.masks.get_mut(offset as usize) else {
            return;
        };
        if *bits & mask == 0 {
            return;
        }
        *bits &= !mask;
        self.count -= 1;
        if *bits == 0 {
            while self.masks.front() == Some(&0) {
                self.masks.pop_front();
                self.first += 1;
            }
            while self.masks.back() == Some(&0) {
                self.masks.pop_back();
            }
            if self.masks.capacity() > self.masks.len().max(MIN_SPAN).saturating_mul(4) {
                self.masks.shrink_to_fit();
            }
        }
    }

    fn words(&self) -> impl Iterator<Item = (u32, u64)> + '_ {
        (self.first..)
            .zip(self.masks.iter().copied())
            .filter(|&(_, mask)| mask != 0)
    }

    fn for_each(&self, mut visit: impl FnMut(EdgeKey)) {
        for (word, mask) in self.words() {
            word_members(word, mask, &mut visit);
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
    SingleWord { word: u32, mask: u64 },
    Sparse(Box<SparseEdges>),
    Span(Box<WordSpan>),
    Words(Box<WordTree>),
}

impl ReverseEdges {
    pub(super) const fn new() -> Self {
        Self::Inline(InlineEdges::new())
    }

    #[cold]
    fn from_inline(inline: &InlineEdges, key: EdgeKey) -> Self {
        // Four adjacent members can go straight into one inline word; don't
        // allocate a temporary ordered set just to rediscover locality.
        let (word, mut mask) = key.word_and_mask();
        for member in inline.iter() {
            let (other, bit) = member.word_and_mask();
            if other != word {
                let mut members: BTreeSet<_> = inline.iter().collect();
                members.insert(key);
                return Self::from_members(members);
            }
            mask |= bit;
        }
        Self::SingleWord { word, mask }
    }

    /// Geometric probes bound repeated conversions; use a directly indexed
    /// span whenever its range is dense relative to actual membership.
    #[cold]
    fn from_members(members: BTreeSet<EdgeKey>) -> Self {
        let count = members.len();
        if count <= INLINE {
            let mut inline = InlineEdges::new();
            for key in members {
                inline.insert(key);
            }
            return Self::Inline(inline);
        }
        if let (Some(first), Some(last)) = (members.first(), members.last()) {
            let (first, _) = first.word_and_mask();
            let (last, _) = last.word_and_mask();
            let span = (last - first) as usize + 1;
            if span == 1 {
                let mask = members
                    .iter()
                    .fold(0, |bits, key| bits | key.word_and_mask().1);
                return Self::SingleWord { word: first, mask };
            }
            if span_worthwhile(span, count) {
                let mut masks = VecDeque::from(vec![0; span]);
                for key in members {
                    let (word, mask) = key.word_and_mask();
                    masks[(word - first) as usize] |= mask;
                }
                return Self::Span(Box::new(WordSpan {
                    first,
                    masks,
                    count,
                }));
            }
        }
        let mut previous = None;
        let mut words = 0;
        for &key in &members {
            let (word, _) = key.word_and_mask();
            if previous != Some(word) {
                previous = Some(word);
                words += 1;
            }
            if words > count / 2 {
                return Self::Sparse(Box::new(SparseEdges {
                    probe: count.saturating_mul(2),
                    members,
                }));
            }
        }
        let mut masks = BTreeMap::<u32, u64>::new();
        for key in members {
            let (word, mask) = key.word_and_mask();
            *masks.entry(word).or_default() |= mask;
        }
        Self::Words(Box::new(WordTree::new(masks, count)))
    }

    #[cold]
    fn from_words(masks: BTreeMap<u32, u64>, count: usize) -> Self {
        if count <= INLINE {
            let mut inline = InlineEdges::new();
            for (word, mask) in masks {
                word_members(word, mask, |key| {
                    inline.insert(key);
                });
            }
            return Self::Inline(inline);
        }
        if let (Some((&first, &first_mask)), Some((&last, _))) =
            (masks.first_key_value(), masks.last_key_value())
        {
            let span = (last - first) as usize + 1;
            if span == 1 {
                return Self::SingleWord {
                    word: first,
                    mask: first_mask,
                };
            }
            if span_worthwhile(span, count) {
                let mut dense = VecDeque::from(vec![0; span]);
                for (word, mask) in masks {
                    dense[(word - first) as usize] = mask;
                }
                return Self::Span(Box::new(WordSpan {
                    first,
                    masks: dense,
                    count,
                }));
            }
        }
        if masks.len() > count - count / 4 {
            let mut members = BTreeSet::new();
            for (word, mask) in masks {
                word_members(word, mask, |key| {
                    members.insert(key);
                });
            }
            return Self::Sparse(Box::new(SparseEdges {
                probe: count.saturating_mul(2),
                members,
            }));
        }
        Self::Words(Box::new(WordTree::new(masks, count)))
    }

    pub(super) fn insert(&mut self, edge: WorkId) {
        let key = EdgeKey::from_work(edge);
        let (word, mask) = key.word_and_mask();
        match self {
            Self::Inline(inline) => {
                if !inline.insert(key) {
                    *self = Self::from_inline(inline, key);
                }
            }
            Self::SingleWord {
                word: existing,
                mask: bits,
            } => {
                if *existing == word {
                    *bits |= mask;
                } else {
                    let count = bits.count_ones() as usize + 1;
                    *self =
                        Self::from_words(BTreeMap::from([(*existing, *bits), (word, mask)]), count);
                }
            }
            Self::Sparse(sparse) => {
                if sparse.members.insert(key) && sparse.members.len() >= sparse.probe {
                    *self = Self::from_members(std::mem::take(&mut sparse.members));
                }
            }
            Self::Span(span) => {
                if !span.insert(word, mask) {
                    let mut masks: BTreeMap<_, _> = span.words().collect();
                    masks.insert(word, mask);
                    *self = Self::from_words(masks, span.count + 1);
                }
            }
            Self::Words(tree) => {
                tree.insert(word, mask);
                if tree.masks.len() >= tree.probe || tree.masks.len() > tree.count - tree.count / 4
                {
                    *self = Self::from_words(std::mem::take(&mut tree.masks), tree.count);
                }
            }
        }
    }

    pub(super) fn remove(&mut self, edge: WorkId) {
        let key = EdgeKey::from_work(edge);
        let (word, mask) = key.word_and_mask();
        match self {
            Self::Inline(inline) => inline.remove(key),
            Self::SingleWord {
                word: existing,
                mask: bits,
            } => {
                if *existing == word {
                    *bits &= !mask;
                    if bits.count_ones() as usize <= INLINE / 2 {
                        let mut inline = InlineEdges::new();
                        word_members(*existing, *bits, |key| {
                            inline.insert(key);
                        });
                        *self = Self::Inline(inline);
                    }
                }
            }
            Self::Sparse(sparse) => {
                if sparse.members.remove(&key)
                    && (sparse.members.len() <= INLINE / 2
                        || sparse.members.len() <= sparse.probe / 4)
                {
                    *self = Self::from_members(std::mem::take(&mut sparse.members));
                }
            }
            Self::Span(span) => {
                span.remove(word, mask);
                if span.count <= INLINE / 2
                    || span.masks.len() > MIN_SPAN.max(span.count.saturating_mul(4))
                {
                    *self = Self::from_words(span.words().collect(), span.count);
                }
            }
            Self::Words(tree) => {
                tree.remove(word, mask);
                if tree.count <= INLINE / 2
                    || tree.masks.len() <= tree.probe / 4
                    || tree.masks.len() > tree.count - tree.count / 4
                {
                    *self = Self::from_words(std::mem::take(&mut tree.masks), tree.count);
                }
            }
        }
    }

    pub(super) fn for_each(&self, mut visit: impl FnMut(WorkId)) {
        let mut keys = |key: EdgeKey| visit(key.work());
        match self {
            Self::Inline(inline) => inline.iter().for_each(keys),
            Self::SingleWord { word, mask } => word_members(*word, *mask, keys),
            Self::Sparse(sparse) => sparse.members.iter().copied().for_each(keys),
            Self::Span(span) => span.for_each(keys),
            Self::Words(tree) => tree.for_each(&mut keys),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn single_word_and_contiguous_spans_adapt_without_losing_order() {
        let mut edges = ReverseEdges::new();
        let at = |id| WorkId::Node(NodeId::from_index(id));
        for id in 0..8 {
            edges.insert(at(id));
        }
        assert!(matches!(edges, ReverseEdges::SingleWord { .. }));
        for word in 1..128 {
            edges.insert(at(word * 64));
        }
        assert!(matches!(edges, ReverseEdges::Span(_)));
        let distant = WorkId::Reaction(ReactionId::from_index(u32::MAX));
        let mut reference: BTreeSet<_> = (0..8)
            .map(at)
            .chain((1..128).map(|word| at(word * 64)))
            .collect();
        for _ in 0..32 {
            edges.insert(distant);
            reference.insert(distant);
            let mut observed = Vec::new();
            edges.for_each(|edge| observed.push(edge));
            assert!(observed.iter().eq(reference.iter()));
            edges.remove(distant);
            reference.remove(&distant);
            observed.clear();
            edges.for_each(|edge| observed.push(edge));
            assert!(observed.iter().eq(reference.iter()));
        }
        for edge in reference {
            edges.remove(edge);
        }
        assert!(matches!(
            edges,
            ReverseEdges::Inline(InlineEdges { len: 0, .. })
        ));
    }

    #[test]
    fn large_stride_spans_release_storage_after_contraction() {
        let mut edges = ReverseEdges::new();
        let at = |word: u32| WorkId::Node(NodeId::from_index(word * 64));
        for word in 0..4096 {
            edges.insert(at(word));
        }
        assert!(matches!(edges, ReverseEdges::Span(_)));
        for word in 0..4032 {
            edges.remove(at(word));
        }
        if let ReverseEdges::Span(span) = &edges {
            assert!(span.masks.capacity() <= 4 * span.masks.len().max(MIN_SPAN));
        }
        let mut observed = Vec::new();
        edges.for_each(|edge| observed.push(edge));
        assert!(observed.into_iter().eq((4032..4096).map(at)));
    }

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

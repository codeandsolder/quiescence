//! Ordered dependency sets with allocation-free small degrees.
//!
//! Graph edges tend to be sparse. Store the first few sorted unique edges
//! inline, but retain logarithmic insertion/removal for arbitrary fan-out.

use std::collections::{BTreeSet, btree_set};

pub enum EdgeSet<T, const N: usize> {
    Empty,
    Inline { len: usize, items: [T; N] },
    Tree(BTreeSet<T>),
}

impl<T, const N: usize> Default for EdgeSet<T, N> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T, const N: usize> EdgeSet<T, N> {
    pub(super) const fn new() -> Self {
        Self::Empty
    }

    pub(super) fn iter(&self) -> Iter<'_, T> {
        match self {
            Self::Empty => Iter::Inline([].iter()),
            Self::Inline { len, items } => Iter::Inline(items[..*len].iter()),
            Self::Tree(tree) => Iter::Tree(tree.iter()),
        }
    }
}

impl<T: Copy + Ord, const N: usize> EdgeSet<T, N> {
    #[cfg(test)]
    pub(super) fn contains(&self, value: &T) -> bool {
        match self {
            Self::Empty => false,
            Self::Inline { len, items } => items[..*len].binary_search(value).is_ok(),
            Self::Tree(tree) => tree.contains(value),
        }
    }

    pub(super) fn insert(&mut self, value: T) {
        match self {
            Self::Empty if N > 0 => {
                *self = Self::Inline {
                    len: 1,
                    items: [value; N],
                };
            }
            Self::Inline { len, items } => {
                let Err(index) = items[..*len].binary_search(&value) else {
                    return;
                };
                if *len < N {
                    items.copy_within(index..*len, index + 1);
                    items[index] = value;
                    *len += 1;
                } else {
                    let mut tree: BTreeSet<_> = items.iter().copied().collect();
                    tree.insert(value);
                    *self = Self::Tree(tree);
                }
            }
            Self::Tree(tree) => {
                tree.insert(value);
            }
            Self::Empty => {
                *self = Self::Tree(BTreeSet::from([value]));
            }
        }
    }

    pub(super) fn remove(&mut self, value: &T) {
        match self {
            Self::Empty => {}
            Self::Inline { len, items } => {
                if let Ok(index) = items[..*len].binary_search(value) {
                    items.copy_within(index + 1..*len, index);
                    *len -= 1;
                    if *len == 0 {
                        *self = Self::Empty;
                    }
                }
            }
            Self::Tree(tree) => {
                tree.remove(value);
                // Hysteresis avoids continual promotion/demotion around N.
                if tree.is_empty() {
                    *self = Self::Empty;
                } else if tree.len() <= N / 2
                    && let Some(&first) = tree.first()
                {
                    let mut items = [first; N];
                    let len = tree.len();
                    for (slot, &edge) in items.iter_mut().zip(tree.iter()) {
                        *slot = edge;
                    }
                    *self = Self::Inline { len, items };
                }
            }
        }
    }

    // Report symmetric changes in sorted order without allocating or
    // repeatedly searching one set for each member of the other.
    pub(super) fn changes(&self, new: &Self, mut changed: impl FnMut(T, bool)) {
        use std::cmp::Ordering;
        let mut old = self.iter().peekable();
        let mut new = new.iter().peekable();
        loop {
            match (old.peek(), new.peek()) {
                (Some(&&a), Some(&&b)) => match a.cmp(&b) {
                    Ordering::Less => {
                        changed(a, false);
                        old.next();
                    }
                    Ordering::Greater => {
                        changed(b, true);
                        new.next();
                    }
                    Ordering::Equal => {
                        old.next();
                        new.next();
                    }
                },
                (None, _) => {
                    new.for_each(|&value| changed(value, true));
                    break;
                }
                (_, None) => {
                    old.for_each(|&value| changed(value, false));
                    break;
                }
            }
        }
    }

    pub(super) fn same_members(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Empty, Self::Empty) => true,
            (Self::Inline { len: a, items: x }, Self::Inline { len: b, items: y }) => {
                x[..*a] == y[..*b]
            }
            (Self::Tree(a), Self::Tree(b)) => a == b,
            _ => self.iter().eq(other.iter()),
        }
    }
}

pub enum Iter<'a, T> {
    Inline(std::slice::Iter<'a, T>),
    Tree(btree_set::Iter<'a, T>),
}

impl<'a, T> Iterator for Iter<'a, T> {
    type Item = &'a T;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Inline(iter) => iter.next(),
            Self::Tree(iter) => iter.next(),
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        match self {
            Self::Inline(iter) => iter.size_hint(),
            Self::Tree(iter) => iter.size_hint(),
        }
    }
}

impl<T> ExactSizeIterator for Iter<'_, T> {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn adaptive_storage_matches_ordered_reference() {
        let mut set = EdgeSet::<u32, 4>::new();
        let mut reference = BTreeSet::new();
        let mut random = 0x9e37_79b9_u32;
        for turn in 0..16_384 {
            random ^= random << 13;
            random ^= random >> 17;
            random ^= random << 5;
            let key = random % if turn % 128 < 64 { 8 } else { 64 };
            if random & 0x100 == 0 {
                set.insert(key);
                reference.insert(key);
            } else {
                set.remove(&key);
                reference.remove(&key);
            }
            assert!(set.iter().eq(reference.iter()));
            assert_eq!(set.contains(&key), reference.contains(&key));
        }
        let keys: Vec<_> = reference.iter().copied().collect();
        for key in keys {
            set.remove(&key);
        }
        assert!(matches!(set, EdgeSet::Empty));
    }

    #[test]
    fn differences_match_two_reference_sets() {
        for old_size in 0..12 {
            for new_size in 0..12 {
                let mut old = EdgeSet::<u32, 4>::new();
                let mut new = EdgeSet::<u32, 4>::new();
                let a: BTreeSet<_> = (0..old_size).map(|i| i * 2).collect();
                let b: BTreeSet<_> = (0..new_size).map(|i| i * 3).collect();
                for &value in &a {
                    old.insert(value);
                }
                for &value in &b {
                    new.insert(value);
                }
                let mut actual = Vec::new();
                old.changes(&new, |value, add| actual.push((value, add)));
                let mut expected: Vec<_> = a
                    .symmetric_difference(&b)
                    .map(|&value| (value, b.contains(&value)))
                    .collect();
                expected.sort_unstable();
                assert_eq!(actual, expected);
                assert_eq!(old.same_members(&new), a == b);
            }
        }
    }
}

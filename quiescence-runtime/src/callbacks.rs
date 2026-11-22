//! Membership of accepted keyed callbacks, independent of causal queue order.
use quiescence_core::ReactionId;
use std::collections::BTreeSet;

type Key = (ReactionId, u64);
const INLINE: usize = 4;
const EMPTY_KEY: Key = (ReactionId::from_index(0), 0);

pub enum PendingCallbacks {
    Inline { keys: [Key; INLINE], len: usize },
    Tree(BTreeSet<Key>),
}

impl PendingCallbacks {
    pub(super) const fn new() -> Self {
        Self::Inline {
            keys: [EMPTY_KEY; INLINE],
            len: 0,
        }
    }

    pub(super) fn insert(&mut self, key: Key) -> bool {
        match self {
            Self::Inline { keys, len } => {
                if keys[..*len].contains(&key) {
                    return false;
                }
                if *len < INLINE {
                    keys[*len] = key;
                    *len += 1;
                } else {
                    let mut tree: BTreeSet<_> = keys.iter().copied().collect();
                    tree.insert(key);
                    *self = Self::Tree(tree);
                }
                true
            }
            Self::Tree(keys) => keys.insert(key),
        }
    }

    pub(super) fn remove(&mut self, key: &Key) {
        match self {
            Self::Inline { keys, len } => {
                if let Some(index) = keys[..*len].iter().position(|candidate| candidate == key) {
                    *len -= 1;
                    keys[index] = keys[*len];
                }
            }
            Self::Tree(keys) => {
                keys.remove(key);
                if keys.len() <= INLINE / 2 {
                    let mut inline = [EMPTY_KEY; INLINE];
                    for (slot, key) in inline.iter_mut().zip(keys.iter()) {
                        *slot = *key;
                    }
                    *self = Self::Inline {
                        keys: inline,
                        len: keys.len(),
                    };
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn adaptive_membership_matches_tree_across_promotion_and_demotion() {
        let mut pending = PendingCallbacks::new();
        let mut expected = BTreeSet::new();
        let mut random = 0x56ae_72cc_9e37_79b9_u64;
        for turn in 0..50_000 {
            random ^= random << 13;
            random ^= random >> 7;
            random ^= random << 17;
            let range = if turn % 100 < 50 { 3 } else { 127 };
            let key = (ReactionId::from_index(0), random % range);
            if random & 128 == 0 {
                assert_eq!(pending.insert(key), expected.insert(key));
            } else {
                pending.remove(&key);
                expected.remove(&key);
            }
            if turn % 100 == 0 {
                for key in std::mem::take(&mut expected) {
                    pending.remove(&key);
                }
                assert!(matches!(pending, PendingCallbacks::Inline { len: 0, .. }));
            }
        }
    }
}

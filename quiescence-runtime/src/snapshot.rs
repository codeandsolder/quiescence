//! Structurally shared canonical snapshots with dirty-page publication.
use super::{CanonicalValue, NodeState, TransitionRecord};
use std::sync::Arc;

/// A 4 KiB canonical page on 64-bit targets (256 16-byte values).
const VALUES_PER_PAGE: usize = 256;

/// Immutable, independently owned canonical state for one quiescent instant.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SnapshotValues {
    pages: Arc<Vec<Arc<[CanonicalValue]>>>,
    len: usize,
}

impl SnapshotValues {
    pub(super) fn value(&self, index: usize) -> Option<CanonicalValue> {
        if index >= self.len {
            return None;
        }
        self.pages
            .get(index / VALUES_PER_PAGE)?
            .get(index % VALUES_PER_PAGE)
            .copied()
    }

    pub(super) const fn len(&self) -> usize {
        self.len
    }
}

/// The graph's authoritative values remain densely mutable. This cache owns
/// immutable pages shared by existing publications; only changed pages are
/// reconstructed at the next quiescent boundary.
pub struct SnapshotCache {
    pages: Option<Arc<Vec<Arc<[CanonicalValue]>>>>,
    dirty: Vec<bool>,
    dirty_indices: Vec<usize>,
    observed_transitions: usize,
    published_nodes: usize,
}

impl SnapshotCache {
    pub(super) const fn new() -> Self {
        Self {
            pages: None,
            dirty: Vec::new(),
            dirty_indices: Vec::new(),
            observed_transitions: 0,
            published_nodes: 0,
        }
    }

    fn mark(&mut self, index: usize) {
        let page = index / VALUES_PER_PAGE;
        if page >= self.dirty.len() {
            self.dirty.resize(page + 1, false);
        }
        if !self.dirty[page] {
            self.dirty[page] = true;
            self.dirty_indices.push(page);
        }
    }

    /// In dense batches, stop inspecting transitions once more than half
    /// the graph's pages are already known dirty. The coverage condition is
    /// essential: repeated changes to a single node must not rebuild the
    /// unrelated pages of a million-node graph.
    fn mark_transitions(&mut self, transitions: &[TransitionRecord], known_nodes: usize) {
        if transitions.len() <= known_nodes / 2 {
            for record in transitions {
                self.mark(record.node.index());
            }
            return;
        }
        let total_pages = known_nodes.div_ceil(VALUES_PER_PAGE);
        for record in transitions {
            self.mark(record.node.index());
            if total_pages > 1 && self.dirty_indices.len() > total_pages / 2 {
                for page in 0..total_pages {
                    self.mark(page * VALUES_PER_PAGE);
                }
                break;
            }
        }
    }

    pub(super) fn before_drain(&mut self, transitions: &[TransitionRecord]) {
        if self.pages.is_some() {
            self.mark_transitions(
                &transitions[self.observed_transitions..],
                self.published_nodes,
            );
        }
        self.observed_transitions = 0;
    }

    fn copy_page(values: &[NodeState]) -> Arc<[CanonicalValue]> {
        values
            .iter()
            .map(NodeState::canonical)
            .collect::<Vec<_>>()
            .into()
    }

    pub(super) fn capture(
        &mut self,
        values: &[NodeState],
        transitions: &[TransitionRecord],
    ) -> SnapshotValues {
        if self.pages.is_some() {
            self.mark_transitions(&transitions[self.observed_transitions..], values.len());
            if values.len() > self.published_nodes {
                for page in (self.published_nodes / VALUES_PER_PAGE)
                    ..=((values.len() - 1) / VALUES_PER_PAGE)
                {
                    self.mark(page * VALUES_PER_PAGE);
                }
            }
        }
        self.observed_transitions = transitions.len();
        self.published_nodes = values.len();
        let pages = self.pages.get_or_insert_with(|| {
            Arc::new(
                values
                    .chunks(VALUES_PER_PAGE)
                    .map(Self::copy_page)
                    .collect(),
            )
        });
        if !self.dirty_indices.is_empty() {
            // Registration is monotonic, but subsequent writes can dirty
            // older pages in any order. Process in index order so every
            // appended page has an existing predecessor.
            self.dirty_indices.sort_unstable();
            let owned = Arc::make_mut(pages);
            for page in self.dirty_indices.drain(..) {
                let start = page * VALUES_PER_PAGE;
                let end = values.len().min(start + VALUES_PER_PAGE);
                let canonical = Self::copy_page(&values[start..end]);
                if page == owned.len() {
                    owned.push(canonical);
                } else {
                    owned[page] = canonical;
                }
                self.dirty[page] = false;
            }
        }
        SnapshotValues {
            pages: Arc::clone(pages),
            len: values.len(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_dirty_pages_change_and_snapshots_own_old_values() {
        let mut cache = SnapshotCache::new();
        let mut values = (0..=VALUES_PER_PAGE * 2)
            .map(|_| NodeState::Atom(0))
            .collect::<Vec<_>>();
        for index in 0..values.len() {
            cache.mark(index);
        }
        let first = cache.capture(&values, &[]);
        let identical = cache.capture(&values, &[]);
        assert!(Arc::ptr_eq(&first.pages, &identical.pages));
        assert_eq!(first.len(), VALUES_PER_PAGE * 2 + 1);
        values[VALUES_PER_PAGE] = NodeState::Atom(u64::MAX);
        cache.mark(VALUES_PER_PAGE);
        let changed = cache.capture(&values, &[]);
        assert!(!Arc::ptr_eq(&first.pages, &changed.pages));
        assert!(Arc::ptr_eq(&first.pages[0], &changed.pages[0]));
        assert!(!Arc::ptr_eq(&first.pages[1], &changed.pages[1]));
        assert!(Arc::ptr_eq(&first.pages[2], &changed.pages[2]));
        assert_eq!(first.value(VALUES_PER_PAGE), Some(CanonicalValue::Atom(0)));
        assert_eq!(
            changed.value(VALUES_PER_PAGE),
            Some(CanonicalValue::Atom(u64::MAX))
        );

        values.push(NodeState::Atom(23));
        cache.mark(values.len() - 1);
        let grown = cache.capture(&values, &[]);
        assert_eq!(grown.len(), first.len() + 1);
        assert_eq!(first.value(first.len()), None);
        assert_eq!(grown.value(first.len()), Some(CanonicalValue::Atom(23)));
        drop(cache);
        drop(values);
        assert_eq!(
            changed.value(VALUES_PER_PAGE),
            Some(CanonicalValue::Atom(u64::MAX))
        );
        assert_eq!(grown.value(first.len()), Some(CanonicalValue::Atom(23)));
    }

    #[test]
    fn publishing_dirty_pages_in_reverse_order_preserves_all_indices()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut cache = SnapshotCache::new();
        let mut values = vec![NodeState::Atom(0); VALUES_PER_PAGE * 4];
        for index in (0..values.len()).rev() {
            cache.mark(index);
        }
        let first = cache.capture(&values, &[]);
        for index in (0..values.len()).step_by(VALUES_PER_PAGE) {
            values[index] = NodeState::Atom(u64::try_from(index)?);
            cache.mark(index);
        }
        let changed = cache.capture(&values, &[]);
        for (index, state) in values.iter().enumerate() {
            assert_eq!(changed.value(index), Some(state.canonical()));
            assert_eq!(first.value(index), Some(CanonicalValue::Atom(0)));
        }
        Ok(())
    }
}

#[cfg(test)]
mod skewed_transition_tests {
    use super::super::transition::TransitionKind;
    use super::*;
    use quiescence_core::{NodeId, SimTime};

    #[test]
    fn many_changes_to_one_node_keep_unrelated_pages_shared() {
        let mut cache = SnapshotCache::new();
        let mut values = vec![NodeState::Atom(0); VALUES_PER_PAGE * 16];
        let first = cache.capture(&values, &[]);
        let changes = vec![
            TransitionRecord {
                at: SimTime::ZERO,
                node: NodeId::from_index(17),
                kind: TransitionKind::Atom,
                before: 0,
                after: 42,
            };
            values.len()
        ];
        values[17] = NodeState::Atom(42);
        cache.mark_transitions(&changes, values.len());
        assert_eq!(cache.dirty_indices, [0]);
        let second = cache.capture(&values, &[]);
        assert!(!Arc::ptr_eq(&first.pages[0], &second.pages[0]));
        for page in 1..first.pages.len() {
            assert!(Arc::ptr_eq(&first.pages[page], &second.pages[page]));
        }
        assert_eq!(first.value(17), Some(CanonicalValue::Atom(0)));
        assert_eq!(second.value(17), Some(CanonicalValue::Atom(42)));
    }
}

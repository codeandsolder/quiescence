use super::{CanonicalValue, StateWrite, WindowRecord};
use quiescence_core::{NodeId, ReactionId, SimTime};
use std::{
    cmp::Ordering,
    collections::{BTreeMap, BinaryHeap, VecDeque},
};
#[derive(Clone, Debug, PartialEq)]
pub enum EventKind {
    SetState {
        write: StateWrite,
    },
    SetStateBatch {
        writes: Vec<StateWrite>,
    },
    Commit {
        node: NodeId,
        generation: u64,
        value: CanonicalValue,
    },
    Reevaluate {
        node: NodeId,
        generation: u64,
    },
    ReactionTransition {
        reaction: ReactionId,
        generation: u64,
        writes: Vec<StateWrite>,
        window_records: Vec<WindowRecord>,
    },
    ExternalReaction {
        reaction: ReactionId,
        token: Option<u64>,
    },
    ScheduledReaction {
        reaction: ReactionId,
        key: u64,
    },
}
#[derive(Clone, Debug, PartialEq)]
pub struct ScheduledEvent {
    pub(super) at: SimTime,
    pub(super) priority: i32,
    pub(super) sequence: u64,
    pub(super) kind: EventKind,
}
/// Ordering ignores payloads, including NaN numeric writes; equality
/// remains reflexive because heap identity is its assigned scheduling key.
#[derive(Debug)]
struct HeapEvent(ScheduledEvent);
impl PartialEq for HeapEvent {
    fn eq(&self, other: &Self) -> bool {
        self.0.at == other.0.at
            && self.0.priority == other.0.priority
            && self.0.sequence == other.0.sequence
    }
}
impl Eq for HeapEvent {}
impl Ord for HeapEvent {
    fn cmp(&self, other: &Self) -> Ordering {
        other
            .0
            .at
            .cmp(&self.0.at)
            .then_with(|| other.0.priority.cmp(&self.0.priority))
            .then_with(|| other.0.sequence.cmp(&self.0.sequence))
    }
}
impl PartialOrd for HeapEvent {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// A FIFO queue per event key while the workload has few distinct keys.
/// When events spread over many times/priorities, switch to a conventional
/// binary heap to avoid tree-map allocation overhead on sparse schedules.
const MAX_GROUPED_KEYS: usize = 128;

enum EventStorage {
    Grouped(BTreeMap<(SimTime, i32), VecDeque<ScheduledEvent>>),
    Sparse(BinaryHeap<HeapEvent>),
}

pub struct EventQueue {
    storage: EventStorage,
    next_sequence: u64,
}
impl EventQueue {
    pub(super) const fn new() -> Self {
        Self {
            storage: EventStorage::Grouped(BTreeMap::new()),
            next_sequence: 0,
        }
    }

    pub(super) fn peek(&self) -> Option<&ScheduledEvent> {
        match &self.storage {
            EventStorage::Grouped(groups) => groups.first_key_value()?.1.front(),
            EventStorage::Sparse(heap) => heap.peek().map(|event| &event.0),
        }
    }

    pub(super) fn push(&mut self, mut event: ScheduledEvent) {
        if self.next_sequence == u64::MAX {
            self.resequence();
        }
        event.sequence = self.next_sequence;
        self.next_sequence += 1;
        match &mut self.storage {
            EventStorage::Grouped(groups) => {
                groups
                    .entry((event.at, event.priority))
                    .or_default()
                    .push_back(event);
                if groups.len() > MAX_GROUPED_KEYS {
                    // Conversion is linear in the pending event count, and the
                    // heap handles future mostly-distinct keys efficiently.
                    let old = std::mem::replace(
                        &mut self.storage,
                        EventStorage::Sparse(BinaryHeap::new()),
                    );
                    if let EventStorage::Grouped(groups) = old {
                        let heap = BinaryHeap::from(
                            groups
                                .into_values()
                                .flatten()
                                .map(HeapEvent)
                                .collect::<Vec<_>>(),
                        );
                        self.storage = EventStorage::Sparse(heap);
                    }
                }
            }
            EventStorage::Sparse(heap) => heap.push(HeapEvent(event)),
        }
    }

    pub(super) fn restore_front(&mut self, event: ScheduledEvent) {
        match &mut self.storage {
            EventStorage::Grouped(groups) => groups
                .entry((event.at, event.priority))
                .or_default()
                .push_front(event),
            EventStorage::Sparse(heap) => heap.push(HeapEvent(event)),
        }
    }

    pub(super) fn pop(&mut self) -> Option<ScheduledEvent> {
        match &mut self.storage {
            EventStorage::Grouped(groups) => {
                let mut first = groups.first_entry()?;
                let event = first.get_mut().pop_front();
                if first.get().is_empty() {
                    first.remove();
                }
                event
            }
            EventStorage::Sparse(heap) => {
                let event = heap.pop().map(|event| event.0);
                if heap.is_empty() {
                    self.storage = EventStorage::Grouped(BTreeMap::new());
                }
                event
            }
        }
    }

    fn resequence(&mut self) {
        let mut sequence = 0;
        match &mut self.storage {
            EventStorage::Grouped(groups) => {
                for group in groups.values_mut() {
                    for event in group {
                        event.sequence = sequence;
                        sequence += 1;
                    }
                }
            }
            EventStorage::Sparse(heap) => {
                let mut ordered = std::mem::take(heap).into_sorted_vec();
                // Our Ord is a max-heap reversal of causal order, so the
                // chronological sequence is the reverse of into_sorted_vec.
                for event in ordered.iter_mut().rev() {
                    event.0.sequence = sequence;
                    sequence += 1;
                }
                *heap = BinaryHeap::from(ordered);
            }
        }
        self.next_sequence = sequence;
    }
}

#[cfg(test)]
mod tests {
    use crate::StateWrite;
    use quiescence_core::SimTime;
    #[test]
    fn grouped_event_queue_preserves_priority_fifo_and_failed_event_position()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut queue = super::EventQueue::new();
        let at = SimTime::from_millis(1);
        let reaction = quiescence_core::ReactionId::from_index(0);
        let event = |priority, token| super::ScheduledEvent {
            at,
            priority,
            sequence: 0,
            kind: super::EventKind::ExternalReaction {
                reaction,
                token: Some(token),
            },
        };
        queue.push(event(2, 1));
        queue.push(event(2, 2));
        queue.push(event(3, 3));
        assert_eq!(queue.peek().map(|e| &e.kind), Some(&event(2, 1).kind));
        let failed = queue.pop().ok_or("queue unexpectedly empty")?;
        assert_eq!(failed.kind, event(2, 1).kind);
        queue.restore_front(failed);
        queue.push(event(-1, 99));
        assert_eq!(queue.pop().map(|e| e.kind), Some(event(-1, 99).kind));
        assert_eq!(queue.pop().map(|e| e.kind), Some(event(2, 1).kind));
        assert_eq!(queue.pop().map(|e| e.kind), Some(event(2, 2).kind));
        assert_eq!(queue.pop().map(|e| e.kind), Some(event(3, 3).kind));
        assert_eq!(queue.pop(), None);
        Ok(())
    }

    #[test]
    fn sparse_fallback_keeps_fifo_and_retry_priority() -> Result<(), Box<dyn std::error::Error>> {
        let mut queue = super::EventQueue::new();
        let reaction = quiescence_core::ReactionId::from_index(0);
        let at = SimTime::from_nanos(8);
        let event = |priority, token| super::ScheduledEvent {
            at,
            priority,
            sequence: 0,
            kind: super::EventKind::ExternalReaction {
                reaction,
                token: Some(token),
            },
        };
        for index in 0..=super::MAX_GROUPED_KEYS {
            queue.push(event(i32::try_from(index)?, u64::try_from(index)?));
        }
        assert!(matches!(&queue.storage, super::EventStorage::Sparse(_)));
        queue.push(event(17, 999));
        let failed = queue.pop().ok_or("sparse queue unexpectedly empty")?;
        queue.restore_front(failed);
        for index in 0..=super::MAX_GROUPED_KEYS {
            assert_eq!(
                queue.pop().map(|e| e.kind),
                Some(event(i32::try_from(index)?, u64::try_from(index)?).kind)
            );
            if index == 17 {
                assert_eq!(queue.pop().map(|e| e.kind), Some(event(17, 999).kind));
            }
        }
        assert_eq!(queue.pop(), None);
        assert!(matches!(&queue.storage, super::EventStorage::Grouped(_)));
        Ok(())
    }

    #[test]
    fn sequence_rebase_preserves_fifo_in_both_storage_modes()
    -> Result<(), Box<dyn std::error::Error>> {
        let reaction = quiescence_core::ReactionId::from_index(0);
        let event = |priority, token| super::ScheduledEvent {
            at: SimTime::ZERO,
            priority,
            sequence: 0,
            kind: super::EventKind::ExternalReaction {
                reaction,
                token: Some(token),
            },
        };
        let mut grouped = super::EventQueue::new();
        grouped.push(event(0, 1));
        grouped.push(event(0, 2));
        grouped.next_sequence = u64::MAX;
        grouped.push(event(0, 3));
        assert_eq!(grouped.pop().map(|e| e.kind), Some(event(0, 1).kind));
        assert_eq!(grouped.pop().map(|e| e.kind), Some(event(0, 2).kind));
        assert_eq!(grouped.pop().map(|e| e.kind), Some(event(0, 3).kind));

        let mut sparse = super::EventQueue::new();
        for index in 0..=super::MAX_GROUPED_KEYS {
            sparse.push(event(i32::try_from(index)?, u64::try_from(index)?));
        }
        sparse.push(event(17, 999));
        assert!(matches!(&sparse.storage, super::EventStorage::Sparse(_)));
        sparse.next_sequence = u64::MAX;
        sparse.push(event(17, 1_000));
        for index in 0..=super::MAX_GROUPED_KEYS {
            assert_eq!(
                sparse.pop().map(|e| e.kind),
                Some(event(i32::try_from(index)?, u64::try_from(index)?).kind)
            );
            if index == 17 {
                assert_eq!(sparse.pop().map(|e| e.kind), Some(event(17, 999).kind));
                assert_eq!(sparse.pop().map(|e| e.kind), Some(event(17, 1_000).kind));
            }
        }
        assert_eq!(sparse.pop(), None);
        Ok(())
    }

    #[test]
    fn adaptive_queue_matches_reference_heap_under_interleaved_operations()
    -> Result<(), Box<dyn std::error::Error>> {
        // Deterministic mixes repeatedly cross the grouped/sparse threshold,
        // insert earlier priorities after later ones, and retry popped work.
        for distinct_keys in [1_u64, 64, 4_096] {
            let mut queue = super::EventQueue::new();
            let mut reference = std::collections::BinaryHeap::<super::HeapEvent>::new();
            let reaction = quiescence_core::ReactionId::from_index(0);
            let mut random = 0x9e37_79b9_7f4a_7c15_u64;
            let mut next_sequence = 0_u64;
            for turn in 0_u64..24_000 {
                random ^= random << 13;
                random ^= random >> 7;
                random ^= random << 17;
                if turn < 256 || random & 3 != 0 {
                    let key = (random >> 8) % distinct_keys;
                    let event = super::ScheduledEvent {
                        at: SimTime::from_nanos(i64::try_from(key / 4)?),
                        priority: i32::try_from(key % 4)? - 2,
                        sequence: next_sequence,
                        kind: super::EventKind::ExternalReaction {
                            reaction,
                            token: Some(turn),
                        },
                    };
                    next_sequence += 1;
                    queue.push(event.clone());
                    reference.push(super::HeapEvent(event));
                } else {
                    assert_eq!(queue.peek(), reference.peek().map(|entry| &entry.0));
                    let actual = queue.pop();
                    let expected = reference.pop().map(|entry| entry.0);
                    assert_eq!(actual, expected);
                    if random.trailing_zeros() >= 7
                        && let Some(event) = actual
                    {
                        queue.restore_front(event.clone());
                        reference.push(super::HeapEvent(event));
                    }
                }
            }
            while let Some(expected) = reference.pop() {
                assert_eq!(queue.pop(), Some(expected.0));
            }
            assert_eq!(queue.pop(), None);
        }
        Ok(())
    }

    #[test]
    fn sparse_heap_ordering_is_reflexive_for_nan_payloads() {
        let node = quiescence_core::NodeId::from_index(0);
        let event = super::HeapEvent(super::ScheduledEvent {
            at: SimTime::ZERO,
            priority: 0,
            sequence: 0,
            kind: super::EventKind::SetState {
                write: StateWrite::new(node, f64::NAN),
            },
        });
        assert_eq!(event, event);
    }
}

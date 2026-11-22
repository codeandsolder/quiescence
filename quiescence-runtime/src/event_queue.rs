use super::{CanonicalValue, StateWrite, WindowRecord, WorkId};
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
impl EventKind {
    pub(super) const fn forecast(&self) -> Option<(WorkId, u64)> {
        match self {
            Self::Commit {
                node, generation, ..
            }
            | Self::Reevaluate { node, generation } => Some((WorkId::Node(*node), *generation)),
            Self::ReactionTransition {
                reaction,
                generation,
                ..
            } => Some((WorkId::Reaction(*reaction), *generation)),
            _ => None,
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct ScheduledEvent {
    pub(super) at: SimTime,
    pub(super) priority: i32,
    pub(super) sequence: u64,
    pub(super) kind: EventKind,
}
impl ScheduledEvent {
    const fn group_key(&self) -> (SimTime, i32) {
        (self.at, self.priority)
    }
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
// Reuse small same-key buffers across quiescent cycles without retaining
// a potentially huge burst allocation indefinitely.
const MAX_RETAINED_FIFO: usize = 64;

enum EventStorage {
    SingleKey(VecDeque<ScheduledEvent>),
    Grouped(BTreeMap<(SimTime, i32), VecDeque<ScheduledEvent>>),
    Sparse(BinaryHeap<HeapEvent>),
}

pub struct EventQueue {
    storage: EventStorage,
    next_sequence: u64,
    len: usize,
}
impl EventQueue {
    pub(super) const fn new() -> Self {
        Self {
            storage: EventStorage::SingleKey(VecDeque::new()),
            next_sequence: 0,
            len: 0,
        }
    }

    pub(super) const fn len(&self) -> usize {
        self.len
    }

    pub(super) fn peek(&self) -> Option<&ScheduledEvent> {
        match &self.storage {
            EventStorage::SingleKey(queue) => queue.front(),
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
        self.insert_assigned(event, VecDeque::push_back);
    }

    pub(super) fn restore_front(&mut self, event: ScheduledEvent) {
        self.insert_assigned(event, VecDeque::push_front);
    }

    fn insert_assigned(
        &mut self,
        event: ScheduledEvent,
        insert: impl FnOnce(&mut VecDeque<ScheduledEvent>, ScheduledEvent),
    ) {
        self.len += 1;
        let key = event.group_key();
        match &mut self.storage {
            EventStorage::SingleKey(queue) => {
                if let Some(previous) = queue.front().map(ScheduledEvent::group_key)
                    && previous != key
                {
                    let mut groups = BTreeMap::from([(previous, std::mem::take(queue))]);
                    insert(groups.entry(key).or_default(), event);
                    self.storage = EventStorage::Grouped(groups);
                } else {
                    insert(queue, event);
                }
            }
            EventStorage::Grouped(groups) => {
                insert(groups.entry(key).or_default(), event);
                if groups.len() > MAX_GROUPED_KEYS {
                    // Conversion is linear in the number of pending events.
                    let heap = BinaryHeap::from(
                        std::mem::take(groups)
                            .into_values()
                            .flatten()
                            .map(HeapEvent)
                            .collect::<Vec<_>>(),
                    );
                    self.storage = EventStorage::Sparse(heap);
                }
            }
            EventStorage::Sparse(heap) => heap.push(HeapEvent(event)),
        }
    }

    fn trim_empty_fifo(queue: &mut VecDeque<ScheduledEvent>) {
        if queue.is_empty() && queue.capacity() > MAX_RETAINED_FIFO {
            queue.shrink_to_fit();
        }
    }

    pub(super) fn pop(&mut self) -> Option<ScheduledEvent> {
        let event = match &mut self.storage {
            EventStorage::SingleKey(queue) => {
                let event = queue.pop_front();
                Self::trim_empty_fifo(queue);
                event
            }
            EventStorage::Grouped(groups) => {
                let mut first = groups.first_entry()?;
                let event = first.get_mut().pop_front();
                if first.get().is_empty() {
                    let mut queue = first.remove();
                    if groups.is_empty() {
                        Self::trim_empty_fifo(&mut queue);
                        self.storage = EventStorage::SingleKey(queue);
                    }
                }
                event
            }
            EventStorage::Sparse(heap) => {
                let event = heap.pop().map(|event| event.0);
                if heap.is_empty() {
                    self.storage = EventStorage::SingleKey(VecDeque::new());
                }
                event
            }
        };
        if event.is_some() {
            self.len -= 1;
        }
        event
    }

    /// Remove invalidated entries without changing surviving causal keys.
    pub(super) fn retain(&mut self, mut keep: impl FnMut(&EventKind) -> bool) {
        match &mut self.storage {
            EventStorage::SingleKey(queue) => {
                queue.retain(|event| keep(&event.kind));
                self.len = queue.len();
                Self::trim_empty_fifo(queue);
            }
            EventStorage::Grouped(groups) => {
                groups.retain(|_, group| {
                    group.retain(|event| keep(&event.kind));
                    !group.is_empty()
                });
                self.len = groups.values().map(VecDeque::len).sum();
                if groups.is_empty() {
                    self.storage = EventStorage::SingleKey(VecDeque::new());
                }
            }
            EventStorage::Sparse(heap) => {
                heap.retain(|event| keep(&event.0.kind));
                self.len = heap.len();
                if heap.is_empty() {
                    self.storage = EventStorage::SingleKey(VecDeque::new());
                }
            }
        }
    }

    fn resequence(&mut self) {
        let mut sequence = 0;
        match &mut self.storage {
            EventStorage::SingleKey(queue) => {
                for event in queue {
                    event.sequence = sequence;
                    sequence += 1;
                }
            }
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
    fn token_event(at: SimTime, priority: i32, token: u64) -> super::ScheduledEvent {
        super::ScheduledEvent {
            at,
            priority,
            sequence: 0,
            kind: super::EventKind::ExternalReaction {
                reaction: quiescence_core::ReactionId::from_index(0),
                token: Some(token),
            },
        }
    }

    #[test]
    fn grouped_event_queue_preserves_priority_fifo_and_failed_event_position()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut queue = super::EventQueue::new();
        let at = SimTime::from_millis(1);
        let event = |priority, token| token_event(at, priority, token);
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
        let at = SimTime::from_nanos(8);
        let event = |priority, token| token_event(at, priority, token);
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
        assert!(matches!(&queue.storage, super::EventStorage::SingleKey(_)));
        Ok(())
    }

    #[test]
    fn sequence_rebase_preserves_fifo_in_both_storage_modes()
    -> Result<(), Box<dyn std::error::Error>> {
        let event = |priority, token| token_event(SimTime::ZERO, priority, token);
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
    fn adaptive_queue_matches_independent_ordered_map_under_interleaved_operations()
    -> Result<(), Box<dyn std::error::Error>> {
        // Deterministic mixes repeatedly cross the grouped/sparse threshold,
        // insert earlier priorities after later ones, and retry popped work.
        for distinct_keys in [1_u64, 64, 4_096] {
            let mut queue = super::EventQueue::new();
            let mut reference = std::collections::BTreeMap::new();
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
                    assert!(
                        reference
                            .insert((event.at, event.priority, event.sequence), event)
                            .is_none()
                    );
                } else {
                    assert_eq!(
                        queue.peek(),
                        reference.first_key_value().map(|(_, event)| event)
                    );
                    let actual = queue.pop();
                    let expected = reference.pop_first().map(|(_, event)| event);
                    assert_eq!(actual, expected);
                    if random.trailing_zeros() >= 7
                        && let Some(event) = actual
                    {
                        queue.restore_front(event.clone());
                        assert!(
                            reference
                                .insert((event.at, event.priority, event.sequence), event)
                                .is_none()
                        );
                    }
                }
            }
            while let Some((_, expected)) = reference.pop_first() {
                assert_eq!(queue.pop(), Some(expected));
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
    #[test]
    fn single_key_storage_reuses_small_buffers_and_releases_large_bursts()
    -> Result<(), Box<dyn std::error::Error>> {
        use super::{EventQueue, EventStorage, MAX_RETAINED_FIFO};
        let mut queue = EventQueue::new();
        for turn in 0..128_u64 {
            let event = token_event(SimTime::from_nanos(i64::try_from(turn)?), 0, turn);
            queue.push(event.clone());
            assert_eq!(queue.len(), 1);
            assert_eq!(queue.pop().map(|e| e.kind), Some(event.kind));
            assert_eq!(queue.len(), 0);
            assert!(matches!(&queue.storage, EventStorage::SingleKey(fifo)
                if fifo.capacity() > 0 && fifo.capacity() <= MAX_RETAINED_FIFO));
        }
        for token in 0..=MAX_RETAINED_FIFO {
            queue.push(token_event(SimTime::ZERO, 0, u64::try_from(token)?));
        }
        queue.retain(|_| false);
        assert_eq!(queue.len(), 0);
        assert!(matches!(&queue.storage, EventStorage::SingleKey(fifo) if fifo.capacity() == 0));
        Ok(())
    }

    #[test]
    fn single_key_promotion_and_compaction_preserve_causal_order()
    -> Result<(), Box<dyn std::error::Error>> {
        use super::{EventQueue, EventStorage};
        let mut queue = EventQueue::new();
        let late = token_event(SimTime::from_nanos(2), 0, 1);
        let early = token_event(SimTime::from_nanos(1), 0, 2);
        queue.push(late.clone());
        queue.push(early.clone());
        assert!(matches!(&queue.storage, EventStorage::Grouped(_)));
        let popped = queue.pop().ok_or("missing earlier event")?;
        queue.restore_front(popped);
        queue.retain(|kind| kind != &early.kind);
        assert_eq!(queue.len(), 1);
        assert_eq!(queue.pop().map(|e| e.kind), Some(late.kind));
        assert!(matches!(&queue.storage, EventStorage::SingleKey(_)));
        assert_eq!(queue.len(), 0);
        Ok(())
    }
}

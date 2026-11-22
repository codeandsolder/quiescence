//! Lazy keyed temporal history, with chronological expiry and adaptive indexing.
use quiescence_core::{SimDuration, SimTime};
use std::collections::{HashMap, VecDeque, hash_map::Entry};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct WindowEvent {
    at: SimTime,
    key: u64,
}

// Small windows are cheaper to scan than to index. Uniform windows need
// neither a scan nor a per-key allocation, even when very large.
const INDEX_THRESHOLD: usize = 32;

enum KeyHistory {
    One(SimTime),
    Many(VecDeque<SimTime>),
}

impl KeyHistory {
    fn push(&mut self, at: SimTime) {
        match self {
            Self::One(first) => *self = Self::Many(VecDeque::from([*first, at])),
            Self::Many(times) => times.push_back(at),
        }
    }

    // The chronological queue is authoritative for expiry order.
    fn expire_first(&mut self) -> bool {
        match self {
            Self::One(_) => true,
            Self::Many(times) => {
                times.pop_front();
                times.is_empty()
            }
        }
    }

    fn count(&self, cutoff: SimTime, now: SimTime) -> usize {
        match self {
            Self::One(at) => usize::from(cutoff <= *at && *at <= now),
            Self::Many(times) => {
                let start = lower_bound(times, cutoff, |&at| at);
                let end = upper_bound(times, now, |&at| at);
                end - start
            }
        }
    }
}

enum Index {
    Scan,
    Uniform(u64),
    Keyed(HashMap<u64, KeyHistory>),
}

impl Index {
    fn insert(index: &mut HashMap<u64, KeyHistory>, event: WindowEvent) {
        match index.entry(event.key) {
            Entry::Occupied(mut entry) => entry.get_mut().push(event.at),
            Entry::Vacant(entry) => {
                entry.insert(KeyHistory::One(event.at));
            }
        }
    }

    fn from_events(events: &VecDeque<WindowEvent>) -> Self {
        let Some(first) = events.front() else {
            return Self::Scan;
        };
        if events.iter().all(|event| event.key == first.key) {
            return Self::Uniform(first.key);
        }
        let mut index = HashMap::new();
        for &event in events {
            Self::insert(&mut index, event);
        }
        Self::Keyed(index)
    }
}

pub struct EventWindow {
    pub(super) name: String,
    duration: SimDuration,
    events: VecDeque<WindowEvent>,
    index: Index,
}

fn lower_bound<T>(events: &VecDeque<T>, cutoff: SimTime, time: impl Fn(&T) -> SimTime) -> usize {
    if events.front().is_none_or(|event| time(event) >= cutoff) {
        0
    } else {
        events.partition_point(|event| time(event) < cutoff)
    }
}

fn upper_bound<T>(events: &VecDeque<T>, now: SimTime, time: impl Fn(&T) -> SimTime) -> usize {
    if events.back().is_none_or(|event| time(event) <= now) {
        events.len()
    } else {
        events.partition_point(|event| time(event) <= now)
    }
}

impl EventWindow {
    pub(super) const fn new(name: String, duration: SimDuration) -> Self {
        Self {
            name,
            duration,
            events: VecDeque::new(),
            index: Index::Scan,
        }
    }

    const fn cutoff(&self, now: SimTime) -> SimTime {
        // Saturation is exact for comparison to representable timestamps:
        // any mathematical lower bound below i64::MIN includes every event.
        SimTime::from_nanos(now.as_nanos().saturating_sub(self.duration.as_nanos()))
    }

    pub(super) fn count_at(&self, now: SimTime, key: u64) -> usize {
        let cutoff = self.cutoff(now);
        match &self.index {
            Index::Keyed(index) => index
                .get(&key)
                .map_or(0, |history| history.count(cutoff, now)),
            Index::Uniform(only) if *only != key => 0,
            index => {
                let start = lower_bound(&self.events, cutoff, |event| event.at);
                let end = upper_bound(&self.events, now, |event| event.at);
                if matches!(index, Index::Uniform(_)) {
                    end - start
                } else {
                    self.events
                        .range(start..end)
                        .filter(|event| event.key == key)
                        .count()
                }
            }
        }
    }

    pub(super) fn record(&mut self, now: SimTime, key: u64) {
        let cutoff = self.cutoff(now);
        while self.events.front().is_some_and(|event| event.at < cutoff) {
            if let Some(event) = self.events.pop_front()
                && let Index::Keyed(index) = &mut self.index
                && let Entry::Occupied(mut entry) = index.entry(event.key)
                && entry.get_mut().expire_first()
            {
                entry.remove();
            }
        }
        let event = WindowEvent { at: now, key };
        self.events.push_back(event);
        match &mut self.index {
            Index::Scan if self.events.len() >= INDEX_THRESHOLD => {
                self.index = Index::from_events(&self.events);
            }
            Index::Uniform(only) if *only != key => {
                self.index = if self.events.len() >= INDEX_THRESHOLD {
                    Index::from_events(&self.events)
                } else {
                    Index::Scan
                };
            }
            Index::Keyed(index) => {
                // Hysteresis avoids rebuilding the index near the crossover.
                if self.events.len() <= INDEX_THRESHOLD / 2 {
                    self.index = Index::Scan;
                } else {
                    Index::insert(index, event);
                }
            }
            _ => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{EventWindow, SimDuration, SimTime};
    #[test]
    fn event_window_handles_full_i64_time_span() {
        let mut window = EventWindow::new(
            String::from("extreme time"),
            SimDuration::from_nanos(i64::MAX),
        );
        window.record(SimTime::from_nanos(i64::MIN), 1);
        assert_eq!(window.count_at(SimTime::from_nanos(i64::MAX), 1), 0);
        window.record(SimTime::from_nanos(i64::MAX), 1);
        assert_eq!(window.events.len(), 1);
    }

    #[test]
    fn binary_window_search_matches_linear_reference_across_expiry() {
        let duration = 31;
        let mut window =
            EventWindow::new(String::from("history"), SimDuration::from_nanos(duration));
        let mut reference = Vec::new();
        for at in 0..1024_i64 {
            let key = at.unsigned_abs() % 7;
            window.record(SimTime::from_nanos(at), key);
            reference.push((at, key));
            for query in [at - 10, at, at + 20, at + 100] {
                // Queries before the committed current time can only inspect
                // retained data; the owner never moves time backwards.
                let cutoff = at - duration;
                for key in 0..7 {
                    let expected = reference
                        .iter()
                        .filter(|&&(time, observed)| {
                            time >= cutoff
                                && time >= query - duration
                                && time <= query
                                && observed == key
                        })
                        .count();
                    assert_eq!(window.count_at(SimTime::from_nanos(query), key), expected);
                }
            }
        }
    }
}

#[cfg(test)]
mod index_tests {
    use super::*;

    #[test]
    fn histories_match_chronological_reference_across_index_changes() {
        let mut random = 0x9e37_79b9_7f4a_7c15_u64;
        for duration in [0, 1, 31, 512, i64::MAX] {
            let mut window = EventWindow::new(String::new(), SimDuration::from_nanos(duration));
            let mut reference = Vec::new();
            let mut now = i64::MIN;
            for step in 0..2048 {
                random ^= random << 13;
                random ^= random >> 7;
                random ^= random << 17;
                now = now.saturating_add(if step % 137 == 0 {
                    1024
                } else {
                    i64::from(random.to_le_bytes()[0] % 3)
                });
                let key = if step % 256 < 100 { 0 } else { random % 97 };
                let at = SimTime::from_nanos(now);
                window.record(at, key);
                reference.retain(|&(time, _)| {
                    i128::from(time) >= i128::from(now) - i128::from(duration)
                });
                reference.push((now, key));
                for query in [now, now.saturating_sub(4), now.saturating_add(7), i64::MAX] {
                    for sought in [0, key, random % 97, u64::MAX] {
                        let expected = reference
                            .iter()
                            .filter(|&&(time, observed)| {
                                observed == sought
                                    && time <= query
                                    && i128::from(time) >= i128::from(query) - i128::from(duration)
                            })
                            .count();
                        assert_eq!(
                            window.count_at(SimTime::from_nanos(query), sought),
                            expected
                        );
                    }
                }
            }
        }
    }

    #[test]
    fn large_mixed_history_releases_retired_keys() {
        let mut window = EventWindow::new(String::new(), SimDuration::from_nanos(63));
        for key in 0..128 {
            window.record(SimTime::from_nanos(key), key.unsigned_abs());
        }
        assert!(matches!(&window.index, Index::Keyed(index) if index.len() == 64));
        window.record(SimTime::from_nanos(1024), 7);
        assert!(matches!(window.index, Index::Scan));
        assert_eq!(window.count_at(SimTime::from_nanos(1024), 7), 1);
    }
}

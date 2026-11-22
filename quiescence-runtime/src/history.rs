use quiescence_core::{SimDuration, SimTime};
use std::collections::VecDeque;
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct WindowEvent {
    at: SimTime,
    key: u64,
}

pub struct EventWindow {
    pub(super) name: String,
    duration: SimDuration,
    events: VecDeque<WindowEvent>,
}

impl EventWindow {
    pub(super) const fn new(name: String, duration: SimDuration) -> Self {
        Self {
            name,
            duration,
            events: VecDeque::new(),
        }
    }

    fn first_unexpired(&self, now: SimTime) -> usize {
        let cutoff = i128::from(now.as_nanos()) - i128::from(self.duration.as_nanos());
        // Records are appended in causal order; expired entries are a prefix.
        self.events
            .partition_point(|event| i128::from(event.at.as_nanos()) < cutoff)
    }

    pub(super) fn count_at(&self, now: SimTime, key: u64) -> usize {
        let start = self.first_unexpired(now);
        let end = self.events.partition_point(|event| event.at <= now);
        self.events
            .range(start..end)
            .filter(|event| event.key == key)
            .count()
    }

    pub(super) fn record(&mut self, now: SimTime, key: u64) {
        let expired = self.first_unexpired(now);
        self.events.drain(..expired);
        self.events.push_back(WindowEvent { at: now, key });
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

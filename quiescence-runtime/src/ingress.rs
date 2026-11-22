use super::{InteractionIngressError, RuntimeError, StateWrite};
use quiescence_core::{ReactionId, SimTime};
use std::{
    collections::VecDeque,
    sync::{Arc, Condvar, Mutex},
};
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum InteractionMoment {
    /// Resolve to the graph owner's simulation time when the interaction is imported.
    QuiescentNow,
    /// Preserve an explicit simulation timestamp supplied by the producer.
    At(SimTime),
}

impl InteractionMoment {
    pub(super) const fn resolve(self, now: SimTime) -> SimTime {
        match self {
            Self::QuiescentNow => now,
            Self::At(at) => at,
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum ExternalInteraction {
    StateBatch {
        moment: InteractionMoment,
        priority: i32,
        writes: Vec<StateWrite>,
    },
    Callback {
        moment: InteractionMoment,
        priority: i32,
        reaction: ReactionId,
        token: Option<u64>,
    },
}

/// An ingress item that could not be admitted to this graph at its import boundary.
#[derive(Debug)]
pub struct RejectedInteraction {
    pub interaction: ExternalInteraction,
    pub error: RuntimeError,
}

/// Admission results for a finite prefix of the producer queue. Rejected items
/// remain owned by the caller for inspection, correction, or dead-letter handling.
#[derive(Debug, Default)]
pub struct InteractionImport {
    pub accepted: usize,
    pub rejected: Vec<RejectedInteraction>,
}

struct InteractionQueue {
    pending: VecDeque<ExternalInteraction>,
    senders: usize,
    receiver_alive: bool,
}

impl Default for InteractionQueue {
    fn default() -> Self {
        Self {
            pending: VecDeque::new(),
            senders: 1,
            receiver_alive: true,
        }
    }
}

#[derive(Default)]
struct InteractionShared {
    queue: Mutex<InteractionQueue>,
    ready: Condvar,
}

pub struct InteractionSender {
    shared: Arc<InteractionShared>,
}

pub struct InteractionInbox {
    shared: Arc<InteractionShared>,
}

#[must_use = "both endpoints are required to submit and consume interactions"]
pub fn interaction_channel() -> (InteractionSender, InteractionInbox) {
    let shared = Arc::new(InteractionShared::default());
    (
        InteractionSender {
            shared: Arc::clone(&shared),
        },
        InteractionInbox { shared },
    )
}

impl Clone for InteractionSender {
    fn clone(&self) -> Self {
        // Clone cannot fail; recover a poisoned mutex solely to keep the
        // sender count consistent. All fallible queue operations still report
        // QueuePoisoned to the caller.
        let mut queue = self
            .shared
            .queue
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        queue.senders += 1;
        drop(queue);
        Self {
            shared: Arc::clone(&self.shared),
        }
    }
}

impl Drop for InteractionSender {
    fn drop(&mut self) {
        let mut queue = self
            .shared
            .queue
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        queue.senders -= 1;
        let last_sender = queue.senders == 0;
        drop(queue);
        if last_sender {
            self.shared.ready.notify_all();
        }
    }
}

impl Drop for InteractionInbox {
    fn drop(&mut self) {
        let mut queue = self
            .shared
            .queue
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        queue.receiver_alive = false;
        queue.pending.clear();
    }
}

impl InteractionSender {
    /// # Errors
    /// Returns an error if the inbox has been dropped or the shared queue
    /// lock was poisoned.
    pub fn submit(&self, interaction: ExternalInteraction) -> Result<(), InteractionIngressError> {
        {
            let mut queue = self
                .shared
                .queue
                .lock()
                .map_err(|_| InteractionIngressError::QueuePoisoned)?;
            if !queue.receiver_alive {
                return Err(InteractionIngressError::Disconnected);
            }
            queue.pending.push_back(interaction);
        }
        self.shared.ready.notify_one();
        Ok(())
    }

    /// # Errors
    /// Returns an error if the shared ingress queue lock is poisoned.
    pub fn state_now(
        &self,
        priority: i32,
        writes: Vec<StateWrite>,
    ) -> Result<(), InteractionIngressError> {
        self.submit(ExternalInteraction::StateBatch {
            moment: InteractionMoment::QuiescentNow,
            priority,
            writes,
        })
    }

    /// # Errors
    /// Returns an error if the shared ingress queue lock is poisoned.
    pub fn state_at(
        &self,
        at: SimTime,
        priority: i32,
        writes: Vec<StateWrite>,
    ) -> Result<(), InteractionIngressError> {
        self.submit(ExternalInteraction::StateBatch {
            moment: InteractionMoment::At(at),
            priority,
            writes,
        })
    }

    /// # Errors
    /// Returns an error if the shared ingress queue lock is poisoned.
    pub fn callback_now(
        &self,
        reaction: ReactionId,
        priority: i32,
    ) -> Result<(), InteractionIngressError> {
        self.submit(ExternalInteraction::Callback {
            moment: InteractionMoment::QuiescentNow,
            priority,
            reaction,
            token: None,
        })
    }

    /// # Errors
    /// Returns an error if the shared ingress queue lock is poisoned.
    pub fn callback_now_with_token(
        &self,
        reaction: ReactionId,
        priority: i32,
        token: u64,
    ) -> Result<(), InteractionIngressError> {
        self.submit(ExternalInteraction::Callback {
            moment: InteractionMoment::QuiescentNow,
            priority,
            reaction,
            token: Some(token),
        })
    }

    /// # Errors
    /// Returns an error if the shared ingress queue lock is poisoned.
    pub fn callback_at(
        &self,
        reaction: ReactionId,
        at: SimTime,
        priority: i32,
    ) -> Result<(), InteractionIngressError> {
        self.submit(ExternalInteraction::Callback {
            moment: InteractionMoment::At(at),
            priority,
            reaction,
            token: None,
        })
    }

    /// # Errors
    /// Returns an error if the shared ingress queue lock is poisoned.
    pub fn callback_at_with_token(
        &self,
        reaction: ReactionId,
        at: SimTime,
        priority: i32,
        token: u64,
    ) -> Result<(), InteractionIngressError> {
        self.submit(ExternalInteraction::Callback {
            moment: InteractionMoment::At(at),
            priority,
            reaction,
            token: Some(token),
        })
    }
}

impl InteractionInbox {
    pub(super) fn drain_pending(
        &self,
    ) -> Result<VecDeque<ExternalInteraction>, InteractionIngressError> {
        let mut queue = self
            .shared
            .queue
            .lock()
            .map_err(|_| InteractionIngressError::QueuePoisoned)?;
        Ok(std::mem::take(&mut queue.pending))
    }

    /// # Errors
    /// Returns an error if the shared ingress queue lock is poisoned.
    pub fn pending_count(&self) -> Result<usize, InteractionIngressError> {
        Ok(self
            .shared
            .queue
            .lock()
            .map_err(|_| InteractionIngressError::QueuePoisoned)?
            .pending
            .len())
    }

    /// Block until a queued interaction is available or the final producer
    /// is dropped. Already queued interactions are delivered before shutdown.
    /// The caller still owns the policy for advancing simulation time.
    ///
    /// # Errors
    /// Returns `Disconnected` when no senders remain and the queue is empty,
    /// or `QueuePoisoned` if the shared mutex was poisoned.
    pub fn wait_for_pending(&self) -> Result<usize, InteractionIngressError> {
        let queue = self
            .shared
            .queue
            .lock()
            .map_err(|_| InteractionIngressError::QueuePoisoned)?;
        let queue = self
            .shared
            .ready
            .wait_while(queue, |queue| {
                queue.pending.is_empty() && queue.senders != 0
            })
            .map_err(|_| InteractionIngressError::QueuePoisoned)?;
        if queue.pending.is_empty() {
            Err(InteractionIngressError::Disconnected)
        } else {
            Ok(queue.pending.len())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{InteractionIngressError, interaction_channel};
    #[test]
    fn dropping_inbox_disconnects_all_sender_clones_and_releases_queue()
    -> Result<(), Box<dyn std::error::Error>> {
        let (sender, inbox) = interaction_channel();
        let clone = sender.clone();
        sender.state_now(0, Vec::new())?;
        drop(inbox);
        assert_eq!(
            sender.state_now(0, Vec::new()),
            Err(InteractionIngressError::Disconnected)
        );
        assert_eq!(
            clone.state_now(0, Vec::new()),
            Err(InteractionIngressError::Disconnected)
        );
        let pending = sender
            .shared
            .queue
            .lock()
            .map_err(|_| String::from("ingress lock poisoned"))?
            .pending
            .len();
        assert_eq!(pending, 0);
        Ok(())
    }
}

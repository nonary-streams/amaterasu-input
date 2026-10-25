//! Deferred, count-only facts about validated-size input batch attempts.
//! Injection never calls the broker to publish these optional observations.

use crate::model::MAX_EVENTS;
use amaterasu_module_sdk::{HookEvent, HookMode, HookPoint, HookSubject, HookVersion};
use serde_json::Value;
use std::collections::VecDeque;

pub(super) const TOPIC: &str = "amaterasu.input.lifecycle/1";
const MAX_PENDING: usize = 32;
// Publish one complete batch pair per tick. Broker calls are outside native
// injection, but still share the module's bounded control-plane tick budget.
const MAX_FLUSH_PER_TICK: usize = 2;

pub(super) struct InputObservations {
    active: bool,
    pending: VecDeque<HookEvent>,
    next_id: u64,
}

impl InputObservations {
    pub(super) fn new(enabled: bool) -> Self {
        Self {
            active: enabled,
            pending: VecDeque::new(),
            next_id: 0,
        }
    }

    /// Called after the controller decides the batch. BeforeInput is a
    /// retrospective requested count; AfterInput is the acknowledged prefix.
    /// A partial_event_possible failure may have physically delivered one
    /// additional event, so the latter is not a physical delivery count.
    pub(super) fn record_batch(&mut self, requested: usize, reply: &Value) {
        if !self.active || !(1..=MAX_EVENTS).contains(&requested) {
            return;
        }
        let Some(after_id) = self.next_id.checked_add(2) else {
            self.disable();
            return;
        };
        self.next_id = after_id;
        // Reserve both slots or drop the whole pair. Later IDs expose gaps.
        if self.pending.len() > MAX_PENDING - 2 {
            return;
        }
        let accepted = reply
            .get("accepted")
            .and_then(Value::as_u64)
            .unwrap_or(0)
            .min(requested as u64) as u16;
        self.pending.push_back(Self::event(
            after_id - 1,
            HookPoint::BeforeInput,
            requested as u16,
        ));
        self.pending
            .push_back(Self::event(after_id, HookPoint::AfterInput, accepted));
    }

    /// A denied publish or failed transport disables only the observer; input
    /// delivery and owner cleanup continue with no retry of old telemetry.
    pub(super) fn flush<E>(&mut self, mut publish: impl FnMut(&HookEvent) -> Result<(), E>) {
        for _ in 0..MAX_FLUSH_PER_TICK {
            let Some(event) = self.pending.pop_front() else {
                break;
            };
            if publish(&event).is_err() {
                self.disable();
                break;
            }
        }
    }

    fn event(event_id: u64, point: HookPoint, events: u16) -> HookEvent {
        HookEvent {
            version: HookVersion::CURRENT,
            event_id,
            point,
            mode: HookMode::Observe,
            subject: HookSubject::Input { events },
        }
    }

    fn disable(&mut self) {
        self.active = false;
        self.pending.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn delivered(queue: &mut InputObservations) -> Vec<HookEvent> {
        let mut events = Vec::new();
        while !queue.pending.is_empty() {
            queue.flush(|event| {
                events.push(event.clone());
                Ok::<(), ()>(())
            });
        }
        events
    }

    #[test]
    fn disabled_observer_is_inert() {
        let mut queue = InputObservations::new(false);
        queue.record_batch(2, &json!({"ok":true,"accepted":2,"lease":"secret"}));
        assert!(delivered(&mut queue).is_empty());
        assert_eq!(queue.next_id, 0);
    }

    #[test]
    fn success_denial_and_partial_cleanup_publish_only_ordered_counts() {
        let mut queue = InputObservations::new(true);
        queue.record_batch(3, &json!({"ok":true,"accepted":3,"lease":"secret"}));
        queue.record_batch(2, &json!({"ok":false,"code":"permission_denied"}));
        queue.record_batch(
            4,
            &json!({"ok":false,"accepted":1,
            "partial_event_possible":true,"cleanup_required":true,"client_id":"secret"}),
        );
        let events = delivered(&mut queue);
        assert!(events.iter().all(|event| event.validate().is_ok()));
        let points: Vec<_> = events
            .iter()
            .map(|event| {
                let HookSubject::Input { events } = &event.subject else {
                    panic!("wrong subject")
                };
                (event.event_id, event.point, *events)
            })
            .collect();
        assert_eq!(
            points,
            vec![
                (1, HookPoint::BeforeInput, 3),
                (2, HookPoint::AfterInput, 3),
                (3, HookPoint::BeforeInput, 2),
                (4, HookPoint::AfterInput, 0),
                (5, HookPoint::BeforeInput, 4),
                (6, HookPoint::AfterInput, 1),
            ]
        );
        let wire = serde_json::to_string(&events).unwrap();
        for secret in ["lease", "client_id", "secret", "permission_denied"] {
            assert!(!wire.contains(secret));
        }
    }

    #[test]
    fn empty_and_oversized_batches_are_not_lifecycle_events() {
        let mut queue = InputObservations::new(true);
        queue.record_batch(0, &json!({"ok":false}));
        queue.record_batch(MAX_EVENTS + 1, &json!({"ok":false}));
        assert!(queue.pending.is_empty());
        assert_eq!(queue.next_id, 0);
    }

    #[test]
    fn backlog_drops_whole_pairs_and_flushes_at_most_one_pair_per_tick() {
        let mut queue = InputObservations::new(true);
        for _ in 0..(MAX_PENDING / 2 + 1) {
            queue.record_batch(1, &json!({"ok":true,"accepted":1}));
        }
        assert_eq!(queue.pending.len(), MAX_PENDING);
        assert_eq!(queue.next_id, MAX_PENDING as u64 + 2);
        let mut emitted = Vec::new();
        queue.flush(|event| {
            emitted.push(event.clone());
            Ok::<(), ()>(())
        });
        assert_eq!(emitted.len(), MAX_FLUSH_PER_TICK);
        queue.record_batch(1, &json!({"ok":true,"accepted":1}));
        assert_eq!(
            queue.pending.back().unwrap().event_id,
            MAX_PENDING as u64 + 4
        );
    }

    #[test]
    fn failed_publication_disables_observation_without_replay() {
        let mut queue = InputObservations::new(true);
        let reply = json!({"ok":false,"accepted":1,"partial_event_possible":true,
            "cleanup_required":true,"lease":"secret"});
        queue.record_batch(2, &reply);
        queue.flush(|_| Err::<(), _>("permission_denied"));
        assert!(!queue.active);
        assert!(queue.pending.is_empty());
        assert!(crate::completion_requires_cleanup(true, &reply));
        assert_eq!(reply["cleanup_required"], true);
        queue.record_batch(1, &json!({"ok":true,"accepted":1}));
        assert!(delivered(&mut queue).is_empty());
    }
}

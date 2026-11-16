//! Input failures this provider reports first-hand to the launch owner's
//! lifecycle store.
//!
//! A stream session acquires input under an operation ID bound to its stream
//! (`ss-<hex>-input`). A refused acquisition is reported under that ID. Once
//! the acquisition succeeds its lease is remembered, so a controller that
//! stops accepting packets for that lease is reported once as a fallback: the
//! stream keeps keyboard and mouse input while gamepad packets are dropped.
use amaterasu_module_sdk::stream_lifecycle::{LifecycleLayer, StreamIssue, StreamIssueOutbox};
use amaterasu_module_sdk::{Command, ModuleResult, Response};
use serde_json::Value;
use std::collections::BTreeMap;

/// Requests and leases remembered at once; the oldest is forgotten first.
const MAX_TRACKED: usize = 32;
/// Code used when a failed reply carries no identifier-shaped code.
const UNNAMED_FAILURE: &str = "input_request_failed";

/// What a tracked request was doing for its stream.
enum Tracked {
    Acquire { operation: String },
    Batch { lease: String },
    Release { lease: String },
}

/// The stream operation a lease was acquired for.
struct LeaseStream {
    operation: String,
    controller_reported: bool,
    events_dropped_reported: bool,
    /// Batch rejection codes already reported for this lease.
    rejections_reported: Vec<String>,
}

#[derive(Default)]
pub(crate) struct StreamIssues {
    enabled: bool,
    requests: BTreeMap<u64, Tracked>,
    leases: BTreeMap<String, LeaseStream>,
    outbox: StreamIssueOutbox,
}

impl StreamIssues {
    pub(crate) fn enable(&mut self) {
        self.enabled = true;
    }

    /// Remember a queued acquisition that names its stream operation, or a
    /// queued batch on a lease.
    pub(crate) fn track(&mut self, request: u64, payload: &Value) {
        let text = |field: &str| {
            payload
                .get(field)
                .and_then(Value::as_str)
                .map(str::to_owned)
        };
        let tracked = match payload.get("action").and_then(Value::as_str) {
            Some("acquire") => text("operation_id").map(|operation| Tracked::Acquire { operation }),
            Some("batch") => text("lease").map(|lease| Tracked::Batch { lease }),
            Some("release") => text("lease").map(|lease| Tracked::Release { lease }),
            _ => None,
        };
        if let Some(tracked) = tracked {
            bounded_insert(&mut self.requests, request, tracked);
        }
    }

    /// Settle the reply to a tracked request.
    pub(crate) fn completed(&mut self, request: u64, reply: &Value) {
        match self.requests.remove(&request) {
            Some(Tracked::Acquire { operation }) => self.acquired(operation, reply),
            Some(Tracked::Batch { lease }) => self.batch_reply(&lease, reply),
            Some(Tracked::Release { lease }) => {
                self.leases.remove(&lease);
            }
            None => {}
        }
    }

    /// Without its input lease the session cannot accept client input.
    fn acquired(&mut self, operation: String, reply: &Value) {
        if reply["ok"] == true {
            if let Some(lease) = reply["lease"].as_str() {
                let stream = LeaseStream {
                    operation,
                    controller_reported: false,
                    events_dropped_reported: false,
                    rejections_reported: Vec::new(),
                };
                bounded_insert(&mut self.leases, lease.to_owned(), stream);
            }
            return;
        }
        let code = failure_code(reply);
        // A request its caller ended is the caller's to report.
        if code == "cancelled" {
            return;
        }
        self.push(StreamIssue::critical(
            operation,
            LifecycleLayer::Input,
            "acquire",
            code,
        ));
    }

    /// A batch on a stream's lease, served directly or from the queue. The
    /// stream continues in each case, so each is a demoted warning reported
    /// once per lease: gamepad packets dropped while no controller is
    /// available, single events skipped because they could not apply, and a
    /// batch refused as a whole (its events are dropped by the session).
    pub(crate) fn batch_reply(&mut self, lease: &str, reply: &Value) {
        let Some(stream) = self.leases.get_mut(lease) else {
            return;
        };
        let operation = stream.operation.clone();
        let issue = if reply["ok"] == true {
            let dropped = reply["dropped"].as_u64().unwrap_or(0);
            if dropped == 0 || stream.events_dropped_reported {
                return;
            }
            stream.events_dropped_reported = true;
            // The rule is a fixed message and never names the key.
            eprintln!(
                "input skipped events stream={} rule={}",
                stream.operation,
                reply["dropped_reason"].as_str().unwrap_or("unspecified")
            );
            let reason = reply["dropped_code"]
                .as_str()
                .filter(|code| identifier(code));
            let issue = StreamIssue::demoted(
                operation,
                LifecycleLayer::Input,
                "batch",
                "input_events_skipped",
                "skip_inapplicable_events",
            );
            match reason {
                Some(reason) => issue.because(reason),
                None => issue,
            }
        } else if reply["error"]["code"] == "controller_unavailable" {
            if stream.controller_reported {
                return;
            }
            stream.controller_reported = true;
            StreamIssue::demoted(
                operation,
                LifecycleLayer::Input,
                "batch",
                "controller_unavailable",
                "controller_input_dropped",
            )
        } else {
            let code = failure_code(reply).to_owned();
            if stream.rejections_reported.contains(&code) || stream.rejections_reported.len() >= 8 {
                return;
            }
            stream.rejections_reported.push(code.clone());
            StreamIssue::demoted(
                operation,
                LifecycleLayer::Input,
                "batch",
                "input_batch_refused",
                "drop_refused_batch",
            )
            .because(code)
        };
        self.push(issue);
    }

    fn push(&mut self, issue: StreamIssue) {
        eprintln!(
            "input stream issue operation={} phase={} code={}",
            issue.operation, issue.phase, issue.code
        );
        self.outbox.push(issue);
    }

    /// Publish queued issues from the tick; discard them when not enabled.
    pub(crate) fn publish(&mut self, send: &mut impl FnMut(Command) -> ModuleResult<Response>) {
        if !self.enabled {
            self.outbox.drain().for_each(drop);
            return;
        }
        if self.outbox.is_empty() {
            return;
        }
        if let Err(error) = self.outbox.flush_with(send) {
            eprintln!("input stream issue report failed error={error}");
        }
    }
}

fn bounded_insert<K: Ord, V>(map: &mut BTreeMap<K, V>, key: K, value: V) {
    if map.len() >= MAX_TRACKED {
        map.pop_first();
    }
    map.insert(key, value);
}

fn failure_code(reply: &Value) -> &str {
    reply["error"]["code"]
        .as_str()
        .filter(|code| identifier(code))
        .unwrap_or(UNNAMED_FAILURE)
}

fn identifier(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.' | b':'))
}

#[cfg(test)]
mod tests {
    use super::*;
    use amaterasu_module_sdk::stream_lifecycle::{IssueDisposition, StreamReport};
    use serde_json::json;

    fn publish(issues: &mut StreamIssues) -> Vec<StreamIssue> {
        let mut published = Vec::new();
        issues.publish(&mut |command| {
            let Command::Publish { topic, payload } = command else {
                panic!("only stream reports are sent")
            };
            assert_eq!(topic, "amaterasu.stream.report/1");
            assert_eq!(payload["kind"], "issue");
            if let StreamReport::Issue(issue) = serde_json::from_value(payload).unwrap() {
                published.push(issue);
            }
            Ok(Response::Ok)
        });
        published
    }

    fn failure(code: &str) -> Value {
        json!({"ok":false,"error":{"code":code,"message":"detail"}})
    }

    #[test]
    fn a_refused_acquisition_is_published_under_the_sessions_operation() {
        let mut issues = StreamIssues::default();
        issues.enable();
        issues.track(4, &json!({"action":"acquire","operation_id":"ss-3c-input"}));
        issues.completed(4, &failure("backend_unavailable"));
        let published = publish(&mut issues);
        assert_eq!(published.len(), 1);
        let issue = &published[0];
        assert_eq!(issue.operation, "ss-3c-input");
        assert_eq!(issue.layer, LifecycleLayer::Input);
        assert_eq!(issue.phase, "acquire");
        assert_eq!(issue.code, "backend_unavailable");
        assert_eq!(issue.disposition, IssueDisposition::CriticalFailure);
        assert!(issue.caused_by.is_none());
        assert!(issue.site.contains("stream_issues.rs:"), "{}", issue.site);
    }

    #[test]
    fn a_lost_controller_is_reported_once_per_stream_lease() {
        let mut issues = StreamIssues::default();
        issues.enable();
        issues.track(5, &json!({"action":"acquire","operation_id":"ss-3d-input"}));
        issues.completed(5, &json!({"ok":true,"lease":"lease-1"}));
        issues.batch_reply("lease-1", &failure("controller_unavailable"));
        issues.track(6, &json!({"action":"batch","lease":"lease-1"}));
        issues.completed(6, &failure("controller_unavailable"));
        issues.batch_reply("lease-2", &failure("controller_unavailable"));
        let published = publish(&mut issues);
        assert_eq!(published.len(), 1);
        assert_eq!(published[0].operation, "ss-3d-input");
        assert_eq!(published[0].disposition, IssueDisposition::DemotedWarning);
        assert_eq!(
            published[0].fallback.as_deref(),
            Some("controller_input_dropped")
        );
    }

    #[test]
    fn skipped_events_and_refused_batches_are_reported_once_per_lease() {
        let mut issues = StreamIssues::default();
        issues.enable();
        issues.track(5, &json!({"action":"acquire","operation_id":"ss-3d-input"}));
        issues.completed(5, &json!({"ok":true,"lease":"lease-1"}));
        let skipped = json!({"ok":true,"lease":"lease-1","accepted":3,"dropped":1,
            "dropped_code":"invalid_sequence"});
        issues.batch_reply("lease-1", &skipped);
        issues.batch_reply("lease-1", &skipped);
        issues.batch_reply("lease-1", &failure("shortcut_unavailable"));
        issues.batch_reply("lease-1", &failure("shortcut_unavailable"));
        issues.batch_reply(
            "lease-1",
            &json!({"ok":true,"lease":"lease-1","accepted":2}),
        );
        let published = publish(&mut issues);
        assert_eq!(published.len(), 2);
        assert_eq!(published[0].code, "input_events_skipped");
        assert_eq!(published[0].reason, "invalid_sequence");
        assert_eq!(published[0].disposition, IssueDisposition::DemotedWarning);
        assert_eq!(published[1].code, "input_batch_refused");
        assert_eq!(published[1].reason, "shortcut_unavailable");
        assert!(
            published
                .iter()
                .all(|issue| issue.operation == "ss-3d-input")
        );
    }

    #[test]
    fn nothing_is_published_until_enabled_or_without_an_operation() {
        let mut issues = StreamIssues::default();
        issues.track(7, &json!({"action":"acquire","operation_id":"ss-3e-input"}));
        issues.completed(7, &failure("input_busy"));
        assert!(publish(&mut issues).is_empty());
        issues.enable();
        assert!(publish(&mut issues).is_empty(), "discarded, not kept");

        issues.track(8, &json!({"action":"acquire"}));
        issues.completed(8, &failure("input_busy"));
        issues.track(9, &json!({"action":"acquire","operation_id":"ss-3f-input"}));
        issues.completed(9, &failure("cancelled"));
        assert!(publish(&mut issues).is_empty());
    }
}

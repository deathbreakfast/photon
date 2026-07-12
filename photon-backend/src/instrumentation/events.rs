//! UC3 event field builders for Photon self-telemetry.

use chrono::{DateTime, Utc};
use serde::Serialize;
use serde_json::{json, Value};

use super::labels::FailureReason;

const MAX_ERROR_LEN: usize = 512;

/// Truncate an error message to the ops-log field limit.
pub fn truncate_error(message: &str) -> String {
    if message.len() <= MAX_ERROR_LEN {
        message.to_string()
    } else {
        format!("{}…", &message[..MAX_ERROR_LEN.saturating_sub(1)])
    }
}

/// Build JSON fields for a `photon_dlq` ops-log event.
#[must_use]
pub fn dlq_fields(
    event_id: &str,
    topic: &str,
    topic_key: Option<&str>,
    seq: i64,
    subscription: Option<&str>,
    reason: FailureReason,
    error: &str,
) -> Value {
    json!({
        "event_id": event_id,
        "topic": topic,
        "topic_key": topic_key.unwrap_or(""),
        "seq": seq,
        "subscription": subscription.unwrap_or(""),
        "reason": reason.as_str(),
        "error": truncate_error(error),
    })
}

/// Row shape for DLQ / failure telemetry append (includes `ts`).
#[derive(Debug, Serialize)]
pub struct PhotonDlqRow {
    /// Failed event id.
    pub event_id: String,
    /// Topic name.
    pub topic: String,
    /// Partition key, or empty string when none.
    pub topic_key: String,
    /// Event sequence number.
    pub seq: i64,
    /// Subscription name, or empty string when none.
    pub subscription: String,
    /// Failure reason label.
    pub reason: String,
    /// Truncated error message.
    pub error: String,
    /// Row timestamp.
    pub ts: DateTime<Utc>,
}

impl PhotonDlqRow {
    /// Build a DLQ row from delivery failure parts.
    #[must_use]
    pub fn from_parts(
        event_id: &str,
        topic: &str,
        topic_key: Option<&str>,
        seq: i64,
        subscription: Option<&str>,
        reason: FailureReason,
        error: &str,
    ) -> Self {
        Self {
            event_id: event_id.to_string(),
            topic: topic.to_string(),
            topic_key: topic_key.unwrap_or("").to_string(),
            seq,
            subscription: subscription.unwrap_or("").to_string(),
            reason: reason.as_str().to_string(),
            error: truncate_error(error),
            ts: Utc::now(),
        }
    }
}

/// Build JSON for `photon_ops_log`.
#[must_use]
pub fn ops_log_fields(
    component: &str,
    operation: &str,
    message: &str,
    topic: &str,
    subscription: &str,
    error: &str,
) -> Value {
    json!({
        "component": component,
        "operation": operation,
        "message": message,
        "topic": topic,
        "subscription": subscription,
        "error": truncate_error(error),
    })
}

use photon_telemetry::ops_log;

/// Emit a `photon_ops_log` UC3 row via the installed [`photon_telemetry::OpsLog`].
pub fn log_ops(
    component: &str,
    operation: &str,
    message: &str,
    topic: &str,
    subscription: &str,
    error: &str,
) {
    ops_log().log_event(
        "photon_ops_log",
        &ops_log_fields(component, operation, message, topic, subscription, error),
    );
}

//! Serde DTOs for host ops introspection (JSON mapping in product packages).

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Topic catalog entry from the compile-time [`TopicRegistry`](photon_backend::TopicRegistry).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AdminTopicSummary {
    /// Stable topic name (e.g. `user.notifications`).
    pub topic_name: String,
    /// JSON payload field used as partition key, when keyed.
    pub keyed_by: Option<String>,
    /// Parsed topic schema JSON.
    pub schema_json: Value,
}

/// Inventory entry for a `#[photon::subscribe]` handler.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AdminHandlerSummary {
    /// Topic this handler listens on.
    pub topic_name: String,
    /// Durable subscription name (`None` for consumer-group handlers).
    pub subscription_name: Option<String>,
    /// Consumer group id when load-balanced (`None` for durable handlers).
    pub consumer_group: Option<String>,
    /// Stable registry key from inventory.
    pub registry_key: String,
    /// Delivery mode: `"durable"` or `"consumer_group"`.
    pub mode: String,
    /// Delivery health and pause state.
    pub health: AdminHandlerHealth,
}

/// How a handler is doing.
///
/// Delivery figures are counted in memory by the node that built the snapshot, since it
/// started; other nodes running the same handler report their own. `paused` is read from the
/// backend and is shared by every node on the same store.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct AdminHandlerHealth {
    /// When this node last handled an event successfully.
    pub last_handled_at: Option<DateTime<Utc>>,
    /// Sequence of that event.
    pub last_handled_seq: Option<i64>,
    /// Deliveries on this node that ended in the dead-letter queue over the last 24 hours.
    pub failures_24h: u32,
    /// Sanitized, truncated text of the last failure.
    pub last_error: Option<String>,
    /// When the last failure happened.
    pub last_error_at: Option<DateTime<Utc>>,
    /// The event waiting on a retry backoff, if any.
    pub retrying: Option<AdminRetrying>,
    /// Set while the subscription is paused (`None` too when the backend has no control).
    pub paused: Option<AdminPaused>,
}

/// An event waiting for its next delivery attempt.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AdminRetrying {
    /// Event being retried.
    pub event_id: String,
    /// Number of the next attempt (the first delivery is attempt 1).
    pub attempt: u32,
    /// When the next attempt runs unless retried sooner.
    pub next_retry_at: DateTime<Utc>,
}

/// Who paused a subscription, and when.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AdminPaused {
    /// Display label passed to [`Photon::pause`](crate::Photon::pause).
    pub by: String,
    /// When it was paused.
    pub at: DateTime<Utc>,
}

/// Last committed sequence for a subscription/topic partition.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AdminCheckpointSummary {
    /// Subscription or consumer-group id used for checkpoint storage.
    pub subscription_name: String,
    /// Topic name.
    pub topic_name: String,
    /// Optional partition or virtual-shard key.
    pub topic_key: Option<String>,
    /// Last committed sequence, if a checkpoint exists.
    pub last_seq: Option<i64>,
    /// Highest sequence assigned in this partition, if known.
    pub head_seq: Option<i64>,
    /// Creation time of the first event after `last_seq`, when one is waiting and the backend
    /// lists events.
    pub oldest_waiting_at: Option<DateTime<Utc>>,
}

/// Storage adapter capabilities surfaced for admin UIs.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AdminBackendSummary {
    /// Stable telemetry label (`mem`, `nats`, …).
    pub telemetry_label: String,
    /// Whether [`Photon::get_event`](crate::Photon::get_event) is supported.
    pub supports_get_event: bool,
    /// Whether list/browse APIs (`list_events_by_topic` / `list_recent_events`) are supported.
    pub supports_list_events: bool,
    /// Maximum replay window in seconds for bounded retention adapters.
    pub max_replay_window_secs: Option<u64>,
    /// Whether pause/resume and replay are supported ([`Photon::pause`](crate::Photon::pause)).
    pub supports_subscription_control: bool,
}

/// Point-in-time ops introspection snapshot (read-only).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AdminSnapshot {
    /// Installed backend capabilities.
    pub backend: AdminBackendSummary,
    /// Registered topics from `#[photon::topic]`.
    pub topics: Vec<AdminTopicSummary>,
    /// Registered handlers from `#[photon::subscribe]`.
    pub handlers: Vec<AdminHandlerSummary>,
    /// Checkpoint cursors for inventory handlers.
    pub checkpoints: Vec<AdminCheckpointSummary>,
}

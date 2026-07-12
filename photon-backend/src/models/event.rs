//! Event model and envelope.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Published event with payload and metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Event {
    /// Unique event ID (UUID).
    pub event_id: String,
    /// Topic name.
    pub topic_name: String,
    /// Key value if keyed topic.
    pub topic_key: Option<String>,
    /// Sequence number per topic/key.
    pub seq: i64,
    /// Captured identity (actor JSON).
    pub actor_json: serde_json::Value,
    /// Serialized payload.
    pub payload_json: serde_json::Value,
    /// When the event was published.
    pub created_at: DateTime<Utc>,
}

/// Event envelope with decoded payload.
#[derive(Debug, Clone)]
pub struct Envelope<T> {
    /// The raw event metadata.
    pub event: Event,
    /// Decoded payload.
    pub payload: T,
}

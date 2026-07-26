//! Event model and envelope.

use std::fmt;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Published event with payload and metadata.
///
/// [`Debug`] redacts `actor_json` and `payload_json` so accidental logging cannot leak
/// plaintext when transport crypto is enabled (or legacy plaintext rows in labs).
#[derive(Clone, Serialize, Deserialize)]
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

impl fmt::Debug for Event {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Event")
            .field("event_id", &self.event_id)
            .field("topic_name", &self.topic_name)
            .field("topic_key", &self.topic_key)
            .field("seq", &self.seq)
            .field("actor_json", &"<redacted>")
            .field("payload_json", &"<redacted>")
            .field("created_at", &self.created_at)
            .finish()
    }
}

/// Event envelope with decoded payload.
#[derive(Debug, Clone)]
pub struct Envelope<T> {
    /// The raw event metadata.
    pub event: Event,
    /// Decoded payload.
    pub payload: T,
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    #[test]
    fn debug_redacts_actor_and_payload() {
        let event = Event {
            event_id: "e1".into(),
            topic_name: "t".into(),
            topic_key: None,
            seq: 1,
            actor_json: serde_json::json!({"secret": "actor-secret"}),
            payload_json: serde_json::json!({"secret": "payload-secret"}),
            created_at: Utc.timestamp_opt(0, 0).unwrap(),
        };
        let dbg = format!("{event:?}");
        assert!(dbg.contains("<redacted>"));
        assert!(!dbg.contains("actor-secret"));
        assert!(!dbg.contains("payload-secret"));
        assert!(dbg.contains("e1"));
    }
}

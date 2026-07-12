//! Topic metadata model (for persistence).

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Topic metadata stored in the registry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopicMetadata {
    /// Unique topic ID (UUID).
    pub topic_id: String,
    /// Stable topic name (e.g., "user.notifications").
    pub topic_name: String,
    /// Optional key field name for keyed topics.
    pub keyed_by: Option<String>,
    /// JSON schema for payload.
    pub schema_json: serde_json::Value,
    /// When the topic was registered.
    pub created_at: DateTime<Utc>,
}

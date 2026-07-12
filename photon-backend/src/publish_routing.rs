//! Resolve storage keys for publishes based on topic delivery mode.

use serde_json::Value;
use uuid::Uuid;

use crate::delivery_mode::DeliveryMode;
use crate::registry::TopicRegistry;
use crate::shard_router::group_publish_storage_key;

/// Resolved publish target: pre-generated event id + storage topic key (if any).
pub struct PublishTarget {
    /// Pre-generated event id for the append.
    pub event_id: String,
    /// Storage topic key (shard key for group topics).
    pub topic_key: Option<String>,
}

/// Resolve storage key for append, honoring consumer-group shard routing from registry.
pub fn resolve_publish_target(
    registry: &TopicRegistry,
    topic_name: &str,
    publish_key: Option<&str>,
    payload: &Value,
) -> PublishTarget {
    let event_id = Uuid::new_v4().to_string();
    if let Some(desc) = registry.get(topic_name) {
        if desc.delivery == DeliveryMode::ConsumerGroup {
            let key = group_publish_storage_key(desc, publish_key, payload, &event_id);
            return PublishTarget {
                event_id,
                topic_key: Some(key),
            };
        }
    }
    PublishTarget {
        event_id,
        topic_key: publish_key.map(String::from),
    }
}

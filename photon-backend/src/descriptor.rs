//! Topic descriptor for auto-registration.

use serde_json::Value;

use crate::delivery_mode::{DeliveryMode, ShardConfig};

/// Descriptor for a registered topic (from `#[photon::topic]` macro).
///
/// Used for runtime lookup and schema metadata.
#[derive(Debug, Clone)]
pub struct TopicDescriptor {
    /// Stable topic name (e.g., "user.notifications").
    pub topic_name: &'static str,
    /// Optional key field name.
    pub keyed_by: Option<&'static str>,
    /// Reserved JSON schema metadata placeholder.
    ///
    /// Currently always `"{}"` from `#[photon::topic]`. **Not validated** at publish time;
    /// do not infer schema enforcement from this field.
    pub schema_json: &'static str,
    /// Publish routing mode (broadcast default).
    pub delivery: DeliveryMode,
    /// Virtual shard settings when [`DeliveryMode::ConsumerGroup`].
    pub shard_config: Option<ShardConfig>,
}

impl TopicDescriptor {
    /// Create a broadcast topic descriptor (default delivery mode).
    #[must_use]
    pub const fn new(
        topic_name: &'static str,
        keyed_by: Option<&'static str>,
        schema_json: &'static str,
    ) -> Self {
        Self {
            topic_name,
            keyed_by,
            schema_json,
            delivery: DeliveryMode::Broadcast,
            shard_config: None,
        }
    }

    /// Create a consumer-group topic with virtual shard routing.
    #[must_use]
    pub const fn group(
        topic_name: &'static str,
        shard_count: u32,
        shard_by: Option<&'static str>,
        schema_json: &'static str,
    ) -> Self {
        Self {
            topic_name,
            keyed_by: None,
            schema_json,
            delivery: DeliveryMode::ConsumerGroup,
            shard_config: Some(ShardConfig::new(shard_count, shard_by)),
        }
    }

    /// Whether publishes route to virtual shard streams.
    #[must_use]
    pub fn is_consumer_group(&self) -> bool {
        self.delivery == DeliveryMode::ConsumerGroup
    }

    /// Get schema as parsed JSON.
    #[must_use]
    pub fn schema_value(&self) -> Option<Value> {
        serde_json::from_str(self.schema_json).ok()
    }
}

// Register with inventory for auto-collection
crate::inventory::collect!(TopicDescriptor);

impl quark::Registrable for TopicDescriptor {
    fn registry_key(&self) -> &str {
        self.topic_name
    }
}

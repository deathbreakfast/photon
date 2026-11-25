//! Topic descriptor for auto-registration.

use std::borrow::Cow;
use std::collections::BTreeMap;

use serde::Deserialize;
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
    /// JSON schema metadata written by `#[photon::topic]`.
    ///
    /// For a struct with named fields: `{"type":"object","properties":{..}}` with one entry per
    /// serialized field name, and `"x-personal": true` on fields marked `#[photon(personal)]`
    /// (see [`Self::personal_fields`]). Other shapes get `"{}"`. **Not validated** at publish
    /// time; do not infer schema enforcement from this field.
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

    /// Serialized names of the payload fields marked `#[photon(personal)]`, in name order.
    ///
    /// Read from the `"x-personal": true` properties in [`Self::schema_json`]. Photon stores the
    /// mark only; admin UIs use it to mask those values until someone reveals them.
    #[must_use]
    pub fn personal_fields(&self) -> Vec<&'static str> {
        #[derive(Deserialize)]
        struct Schema<'a> {
            #[serde(borrow, default)]
            properties: BTreeMap<Name<'a>, Property>,
        }
        #[derive(Deserialize, PartialEq, Eq, PartialOrd, Ord)]
        struct Name<'a>(#[serde(borrow)] Cow<'a, str>);
        #[derive(Deserialize)]
        struct Property {
            #[serde(rename = "x-personal", default)]
            personal: bool,
        }

        let Ok(schema) = serde_json::from_str::<Schema<'static>>(self.schema_json) else {
            return Vec::new();
        };
        schema
            .properties
            .into_iter()
            .filter(|(_, property)| property.personal)
            // The macro rejects personal names that need JSON escapes, so these borrow.
            .filter_map(|(Name(name), _)| match name {
                Cow::Borrowed(name) => Some(name),
                Cow::Owned(_) => None,
            })
            .collect()
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn personal_fields_reads_marked_properties() {
        let descriptor = TopicDescriptor::new(
            "test.signup",
            None,
            r#"{"type":"object","properties":{"user_id":{},"we\"ird":{},"phone":{"x-personal":true},"email":{"x-personal":true}}}"#,
        );
        assert_eq!(descriptor.personal_fields(), vec!["email", "phone"]);
    }

    #[test]
    fn personal_fields_is_empty_without_marks() {
        let none: Vec<&str> = Vec::new();
        assert_eq!(
            TopicDescriptor::new("test.empty", None, "{}").personal_fields(),
            none
        );
        assert_eq!(
            TopicDescriptor::new("test.bad", None, "not json").personal_fields(),
            none
        );
    }
}

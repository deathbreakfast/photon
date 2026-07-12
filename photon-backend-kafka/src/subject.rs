//! Kafka topic naming and checkpoint key layout for Photon events.

/// Topic prefix for all Photon topics (single-shard mode).
pub const TOPIC_PREFIX: &str = "photon";

/// Sharded-mode topic prefix (must not overlap legacy single-shard layout).
pub const SHARDED_TOPIC_PREFIX: &str = "photon-s";

/// Header carrying the Photon event UUID.
pub const HEADER_EVENT_ID: &str = "Photon-Event-Id";

/// Header carrying the per-partition monotonic sequence.
pub const HEADER_SEQ: &str = "Photon-Seq";

/// Header carrying the optional topic partition key.
pub const HEADER_TOPIC_KEY: &str = "Photon-Topic-Key";

/// Build the Kafka topic for a logical topic (`photon.{topic_name}`).
#[must_use]
pub fn photon_topic(topic_name: &str) -> String {
    format!("{TOPIC_PREFIX}.{topic_name}")
}

/// Separator for checkpoint keys (Kafka message keys).
const KV_SEP: &str = "/";

/// Checkpoint key (logical `{sub}:{topic}:{key}`; wire uses `/` separators).
#[must_use]
pub fn checkpoint_key(sub: &str, topic: &str, topic_key: Option<&str>) -> String {
    format!(
        "{sub}{}{topic}{}{}",
        KV_SEP,
        KV_SEP,
        topic_key.unwrap_or("__null__")
    )
}

/// Per-shard checkpoint suffix for keyed subscriptions.
#[must_use]
pub fn checkpoint_key_sharded(
    sub: &str,
    topic: &str,
    topic_key: Option<&str>,
    topic_shard: u32,
) -> String {
    format!("{}/s{topic_shard}", checkpoint_key(sub, topic, topic_key))
}

/// Checkpoint key for unkeyed durable checkpoints across topic shards (JSON map).
#[must_use]
pub fn checkpoint_key_unkeyed_shards(sub: &str, topic: &str) -> String {
    format!("{sub}{KV_SEP}__shards__{topic}")
}

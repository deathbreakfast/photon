//! `JetStream` subject and header naming for Photon events.

/// NATS subject prefix for all Photon topics.
pub const SUBJECT_PREFIX: &str = "photon";

/// Sharded-mode subject prefix (must not match legacy `photon.>` wildcard).
pub const SHARDED_SUBJECT_PREFIX: &str = "photon-s";

/// `JetStream` stream wildcard covering all Photon subjects (single-stream mode).
pub const STREAM_SUBJECTS: &str = "photon.>";

/// Header carrying the Photon event UUID.
pub const HEADER_EVENT_ID: &str = "Photon-Event-Id";

/// Header carrying the per-partition monotonic sequence.
pub const HEADER_SEQ: &str = "Photon-Seq";

/// Header carrying the optional topic partition key.
pub const HEADER_TOPIC_KEY: &str = "Photon-Topic-Key";

/// Build the `JetStream` subject for a topic (`photon.{topic_name}`).
#[must_use]
pub fn photon_subject(topic_name: &str) -> String {
    format!("{SUBJECT_PREFIX}.{topic_name}")
}

/// Separator for NATS KV keys (`:` is not allowed in `JetStream` KV keys).
const KV_SEP: &str = "/";

/// Checkpoint KV key (logical `{sub}:{topic}:{key}`; wire uses `/` separators).
#[must_use]
pub fn checkpoint_key(sub: &str, topic: &str, topic_key: Option<&str>) -> String {
    format!(
        "{sub}{}{topic}{}{}",
        KV_SEP,
        KV_SEP,
        topic_key.unwrap_or("__null__")
    )
}

/// Per-stream-shard checkpoint suffix for keyed subscriptions.
#[must_use]
pub fn checkpoint_key_sharded(
    sub: &str,
    topic: &str,
    topic_key: Option<&str>,
    stream_shard: u32,
) -> String {
    format!("{}/s{stream_shard}", checkpoint_key(sub, topic, topic_key))
}

/// KV key for unkeyed durable checkpoints across stream shards (JSON map).
#[must_use]
pub fn checkpoint_key_unkeyed_shards(sub: &str, topic: &str) -> String {
    format!("{sub}{KV_SEP}__shards__{topic}")
}

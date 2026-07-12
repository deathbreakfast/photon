//! Fluvio topic naming and checkpoint key layout for Photon events.

use std::hash::{Hash, Hasher};

/// Topic prefix for all Photon topics (single-shard mode).
pub const TOPIC_PREFIX: &str = "photon";

/// Sharded-mode topic prefix (must not overlap legacy single-shard layout).
pub const SHARDED_TOPIC_PREFIX: &str = "photon-s";

/// Header carrying the Photon event UUID.
#[allow(dead_code)] // reserved for record metadata wiring
pub const HEADER_EVENT_ID: &str = "Photon-Event-Id";

/// Header carrying the per-partition monotonic sequence.
#[allow(dead_code)] // reserved for record metadata wiring
pub const HEADER_SEQ: &str = "Photon-Seq";

/// Header carrying the optional topic partition key.
#[allow(dead_code)] // reserved for record metadata wiring
pub const HEADER_TOPIC_KEY: &str = "Photon-Topic-Key";

/// Separator for checkpoint keys (Fluvio record keys).
const KV_SEP: &str = "/";

/// Max Fluvio topic name length (cluster limit).
const MAX_FLUVIO_TOPIC_LEN: usize = 63;

/// Sanitize a logical topic name for Fluvio topic naming rules.
#[must_use]
pub fn sanitize_fluvio_topic_name(topic_name: &str) -> String {
    topic_name
        .chars()
        .map(|c| match c {
            '.' | '/' | ':' => '-',
            c if c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-' => c,
            c if c.is_ascii_uppercase() => c.to_ascii_lowercase(),
            _ => '-',
        })
        .collect()
}

pub fn truncate_or_hash(prefix: &str, topic_name: &str) -> String {
    let sanitized = sanitize_fluvio_topic_name(topic_name);
    let candidate = format!("{prefix}-{sanitized}");
    if candidate.len() <= MAX_FLUVIO_TOPIC_LEN {
        return candidate;
    }
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    topic_name.hash(&mut hasher);
    format!("{prefix}-{:016x}", hasher.finish())
}

/// Build the Fluvio topic for a logical topic (`photon-{topic_name}`).
#[must_use]
pub fn photon_topic(topic_name: &str) -> String {
    truncate_or_hash(TOPIC_PREFIX, topic_name)
}

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

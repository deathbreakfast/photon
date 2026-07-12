//! `JetStream` stream shard routing (distinct from Photon virtual shard topic keys).

use photon_backend::shard_router::shard_id;

use crate::config::NatsConfig;

/// Stride for encoding `(stream_shard, local_seq)` into one `Event.seq`.
pub const SEQ_STRIDE: i64 = 1_000_000_000_000;

/// Max supported `JetStream` stream shards.
pub const MAX_STREAM_SHARDS: u32 = 256;

/// Environment variable for stream shard count (bench/scripts fallback).
pub const STREAM_SHARDS_ENV: &str = "PHOTON_NATS_STREAM_SHARDS";

/// Pick a stream shard index from routing key and configured shard count.
#[must_use]
pub fn pick_shard(routing_key: &str, shard_count: u32) -> u32 {
    shard_id(routing_key, shard_count.max(1))
}

/// Routing key for publish shard assignment.
#[must_use]
pub fn publish_routing_key(topic_key: Option<&str>, event_id: &str) -> String {
    topic_key.map_or_else(|| event_id.to_string(), str::to_string)
}

/// `JetStream` stream name for a shard (or base name when `shards == 1`).
#[must_use]
pub fn stream_name_for(config: &NatsConfig, shard: u32) -> String {
    if config.stream_shards <= 1 {
        config.stream_name.clone()
    } else {
        format!("{}-{}", config.stream_name, shard)
    }
}

/// Subject wildcard bound to one stream shard (`photon.>` or `photon-s.{shard}.>`).
#[must_use]
pub fn stream_subject_wildcard(shard: u32, shard_count: u32) -> String {
    if shard_count <= 1 {
        crate::subject::STREAM_SUBJECTS.into()
    } else {
        format!("{}.{shard}.>", crate::subject::SHARDED_SUBJECT_PREFIX)
    }
}

/// Publish/subscribe filter subject for a topic on a shard.
#[must_use]
pub fn photon_subject_for(shard: u32, shard_count: u32, topic_name: &str) -> String {
    if shard_count <= 1 {
        crate::subject::photon_subject(topic_name)
    } else {
        format!(
            "{}.{shard}.{topic_name}",
            crate::subject::SHARDED_SUBJECT_PREFIX
        )
    }
}

/// Encode per-stream local sequence and shard into one composite `Event.seq`.
#[must_use]
pub fn composite_seq(shard: u32, local_seq: u64) -> i64 {
    i64::from(shard)
        .saturating_mul(SEQ_STRIDE)
        .saturating_add(i64::try_from(local_seq).unwrap_or(i64::MAX))
}

/// Split composite sequence into `(shard, local_seq)`.
#[must_use]
pub fn decompose_seq(composite: i64) -> (u32, i64) {
    if composite <= 0 {
        return (0, 0);
    }
    let shard = u32::try_from(composite / SEQ_STRIDE).unwrap_or(0);
    let local = composite % SEQ_STRIDE;
    (shard, local)
}

/// Per-shard replay cursor for subscribe when caller passes one composite `after_seq`.
#[must_use]
pub fn local_after_seq_for_shard(shard: u32, after_seq: Option<i64>) -> Option<i64> {
    if after_seq == Some(0) {
        return Some(0);
    }
    let composite = after_seq.filter(|&s| s > 0)?;
    let (seq_shard, local) = decompose_seq(composite);
    match seq_shard.cmp(&shard) {
        std::cmp::Ordering::Equal => Some(local),
        std::cmp::Ordering::Greater => Some(0),
        std::cmp::Ordering::Less => None,
    }
}

/// Parse stream shard count from env (default 1).
#[must_use]
pub fn stream_shards_from_env() -> u32 {
    std::env::var(STREAM_SHARDS_ENV)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(1)
        .clamp(1, MAX_STREAM_SHARDS)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pick_shard_is_stable() {
        assert_eq!(pick_shard("user-1", 4), pick_shard("user-1", 4));
    }

    #[test]
    fn composite_roundtrip() {
        let c = composite_seq(3, 42);
        assert_eq!(decompose_seq(c), (3, 42));
    }

    #[test]
    fn local_after_zero_replays_from_start() {
        assert_eq!(local_after_seq_for_shard(2, Some(0)), Some(0));
    }

    #[test]
    fn legacy_subject_when_single_shard() {
        assert_eq!(photon_subject_for(0, 1, "orders"), "photon.orders");
    }

    #[test]
    fn sharded_subject_layout() {
        assert_eq!(photon_subject_for(2, 4, "orders"), "photon-s.2.orders");
        assert_eq!(stream_subject_wildcard(2, 4), "photon-s.2.>");
    }
}

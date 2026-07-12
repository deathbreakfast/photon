//! Static shard assignment from environment (lab / CI).

use std::env;

/// Parsed static assignment configuration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StaticAssignmentConfig {
    /// Member instance id.
    pub instance_id: String,
    /// Shards assigned to this instance.
    pub shard_ids: Vec<u32>,
}

/// `PHOTON_GROUP_SHARD_ASSIGNMENT` — comma list or inclusive range `0-15`.
#[must_use]
pub fn parse_shard_assignment(raw: &str, shard_count: u32) -> Vec<u32> {
    let raw = raw.trim();
    if raw.is_empty() {
        return Vec::new();
    }
    if let Some((start, end)) = raw.split_once('-') {
        if let (Ok(s), Ok(e)) = (start.trim().parse::<u32>(), end.trim().parse::<u32>()) {
            return (s..=e.min(shard_count.saturating_sub(1))).collect();
        }
    }
    raw.split(',')
        .filter_map(|p| p.trim().parse::<u32>().ok())
        .filter(|id| *id < shard_count)
        .collect()
}

/// `PHOTON_GROUP_SHARD_COUNT` when set and parseable.
#[must_use]
pub fn shard_count_from_env() -> Option<u32> {
    env::var("PHOTON_GROUP_SHARD_COUNT")
        .ok()
        .and_then(|v| v.parse().ok())
}

/// `PHOTON_GROUP_MEMBER_COUNT` when set and parseable.
#[must_use]
pub fn member_count_from_env() -> Option<u32> {
    env::var("PHOTON_GROUP_MEMBER_COUNT")
        .ok()
        .and_then(|v| v.parse().ok())
}

/// `PHOTON_GROUP_INSTANCE_ID` when set.
#[must_use]
pub fn instance_id_from_env() -> Option<String> {
    env::var("PHOTON_GROUP_INSTANCE_ID").ok()
}

/// Shards assigned to `instance_index` when splitting `shard_count` across `member_count`.
#[must_use]
pub fn round_robin_shards_for_member(
    shard_count: u32,
    member_count: u32,
    instance_index: u32,
) -> Vec<u32> {
    let member_count = member_count.max(1);
    let per = shard_count.div_ceil(member_count);
    let start = instance_index * per;
    let end = (start + per).min(shard_count);
    (start..end).collect()
}

/// Shards for this process: explicit env range, else round-robin by instance index.
#[must_use]
pub fn static_assigned_shards(shard_count: u32) -> Vec<u32> {
    if let Ok(raw) = env::var("PHOTON_GROUP_SHARD_ASSIGNMENT") {
        let ids = parse_shard_assignment(&raw, shard_count);
        if !ids.is_empty() {
            return ids;
        }
    }
    let member_count = member_count_from_env().unwrap_or(1).max(1);
    let instance_index = instance_id_from_env()
        .and_then(|id| id.parse::<u32>().ok())
        .unwrap_or(0);
    round_robin_shards_for_member(shard_count, member_count, instance_index)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_range() {
        assert_eq!(parse_shard_assignment("0-3", 8), vec![0, 1, 2, 3]);
    }

    #[test]
    fn parse_list() {
        assert_eq!(parse_shard_assignment("1,3,5", 8), vec![1, 3, 5]);
    }

    #[test]
    fn round_robin_two_members_covers_eight_shards() {
        assert_eq!(round_robin_shards_for_member(8, 2, 0), vec![0, 1, 2, 3]);
        assert_eq!(round_robin_shards_for_member(8, 2, 1), vec![4, 5, 6, 7]);
    }
}

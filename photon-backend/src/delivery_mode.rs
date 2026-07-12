//! Topic delivery mode and virtual shard configuration.

/// How publishes on a topic are routed to subscribers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum DeliveryMode {
    /// Every matching subscriber receives every event (default).
    #[default]
    Broadcast,
    /// Events routed to virtual shards; one consumer group member per shard.
    ConsumerGroup,
}

impl std::str::FromStr for DeliveryMode {
    type Err = ();

    /// Parse `"broadcast"`, `"group"`, or `"consumer_group"`.
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "broadcast" => Ok(Self::Broadcast),
            "group" | "consumer_group" => Ok(Self::ConsumerGroup),
            _ => Err(()),
        }
    }
}

/// Virtual shard settings for [`DeliveryMode::ConsumerGroup`] topics.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ShardConfig {
    /// Number of virtual shards (`hash(routing_key) % shard_count`).
    pub shard_count: u32,
    /// JSON payload field used for routing when publish has no explicit partition key.
    pub shard_by: Option<&'static str>,
}

impl ShardConfig {
    /// Default shard count when topic macro omits `shards`.
    pub const DEFAULT_SHARD_COUNT: u32 = 32;

    /// Create shard config; `0` count is replaced with [`Self::DEFAULT_SHARD_COUNT`].
    #[must_use]
    pub const fn new(shard_count: u32, shard_by: Option<&'static str>) -> Self {
        Self {
            shard_count: if shard_count == 0 {
                Self::DEFAULT_SHARD_COUNT
            } else {
                shard_count
            },
            shard_by,
        }
    }

    /// Default shard count with no `shard_by` field.
    #[must_use]
    pub const fn default_count() -> Self {
        Self::new(Self::DEFAULT_SHARD_COUNT, None)
    }
}

//! Optional host hooks for retention policy extensions.

use async_trait::async_trait;

use super::partition::{SubscriptionPartition, TopicPartition};
use crate::Result;

/// Optional host callback for subscriptions and legal-hold floors.
pub trait RetentionHook: Send + Sync {
    /// Extra durable subscriptions not in [`HandlerRegistry`](crate::HandlerRegistry).
    fn extra_subscriptions(&self) -> Vec<SubscriptionPartition> {
        Vec::new()
    }

    /// Lowest seq that must be retained (legal hold); acts as a floor pin.
    fn retain_floor_seq(&self, topic: &str, topic_key: Option<&str>) -> Option<i64> {
        let _ = (topic, topic_key);
        None
    }
}

/// Post-checkpoint opportunistic reclaim (implemented by [`RetentionReclaimer`](super::reclaimer::RetentionReclaimer)).
#[async_trait]
pub trait PartitionReclaim: Send + Sync {
    /// Reclaim transport rows for the given partitions after checkpoint flush.
    async fn sweep_partitions(&self, partitions: &[TopicPartition]) -> Result<()>;
}

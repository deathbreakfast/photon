//! Safe watermark computation for storage reclaim.

use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;

use super::config::{retain_seq_margin, RetentionPolicy};
use super::hook::RetentionHook;
use super::partition::{subscriptions_for_partition, SubscriptionPartition};
use crate::checkpoint::CheckpointCoalescer;
use crate::delivery::DlqSink;
use crate::error::Result;
use crate::storage::StoragePort;

/// Minimum seq pins combined into a checkpoint-style safe watermark.
pub fn min_seq_pins(pins: impl IntoIterator<Item = Option<i64>>) -> Option<i64> {
    pins.into_iter().flatten().filter(|&s| s > 0).min()
}

/// Apply retain margin to a checkpoint high-water seq.
#[must_use]
pub fn truncate_bound_from_checkpoint(safe_seq: i64) -> i64 {
    (safe_seq - retain_seq_margin()).max(0)
}

/// Merge checkpoint margin bound with TTL keep-from seq (most conservative wins).
#[must_use]
pub fn merge_truncate_bound(checkpoint_safe: Option<i64>, ttl_keep_from: Option<i64>) -> i64 {
    let mut bound = 0i64;
    if let Some(cp) = checkpoint_safe {
        bound = bound.max(truncate_bound_from_checkpoint(cp));
    }
    if let Some(ttl) = ttl_keep_from {
        bound = bound.max(ttl.max(1));
    }
    bound
}

/// Seq argument for [`StoragePort::truncate_before`] given a merged truncate floor.
#[must_use]
pub fn truncate_before_arg(truncate_floor: i64) -> i64 {
    truncate_floor.saturating_add(retain_seq_margin())
}

/// Inputs for watermark computation on one partition.
pub struct WatermarkContext<'a> {
    /// Storage port for durable checkpoint loads and truncate.
    pub port: &'a Arc<dyn StoragePort>,
    /// Pending coalesced checkpoint high-water marks.
    pub coalescer: &'a CheckpointCoalescer,
    /// DLQ sink for optional min-seq pins.
    pub dlq: &'a DlqSink,
    /// Retention policy (TTL, pin flags).
    pub policy: &'a RetentionPolicy,
    /// Optional host retention hook.
    pub hook: Option<&'a dyn RetentionHook>,
    /// Known durable subscriptions for this reclaim pass.
    pub subscriptions: &'a [SubscriptionPartition],
}

/// Compute checkpoint pin and optional TTL keep-from seq for one partition.
pub async fn compute_watermarks(
    ctx: &WatermarkContext<'_>,
    topic: &str,
    topic_key: Option<&str>,
) -> Result<(Option<i64>, Option<i64>)> {
    let mut pins: Vec<Option<i64>> = Vec::new();

    for sub in subscriptions_for_partition(ctx.subscriptions, topic, topic_key) {
        let cp = ctx
            .port
            .load_checkpoint(&sub.subscription_name, topic, topic_key)
            .await?;
        pins.push(cp.filter(|&s| s > 0));
    }

    pins.push(
        ctx.coalescer
            .pending_min_seq(topic, topic_key)
            .await
            .filter(|&s| s > 0),
    );
    pins.push(ctx.port.delivery_seq_pin(topic, topic_key).await);

    if ctx.policy.pin_dlq {
        pins.push(ctx.dlq.min_seq_for(topic, topic_key).filter(|&s| s > 0));
    }

    if let Some(h) = ctx.hook {
        pins.push(h.retain_floor_seq(topic, topic_key).filter(|&s| s > 0));
    }

    let checkpoint_safe = min_seq_pins(pins);

    let ttl_keep_from = ctx
        .policy
        .max_age_secs
        .and_then(|max_age| ttl_keep_from_seq(ctx.port.as_ref(), topic, topic_key, max_age));

    Ok((checkpoint_safe, ttl_keep_from))
}

fn ttl_keep_from_seq(
    port: &dyn StoragePort,
    topic: &str,
    topic_key: Option<&str>,
    max_age_secs: u64,
) -> Option<i64> {
    let _ = (port, topic, topic_key, max_age_secs);
    let cutoff = Utc::now() - Duration::from_secs(max_age_secs);
    let _ = cutoff;
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn min_seq_pins_ignores_none_and_zero() {
        assert_eq!(min_seq_pins([Some(10), None, Some(3)]), Some(3));
        assert_eq!(min_seq_pins([Some(0), Some(4)]), Some(4));
    }

    #[test]
    fn truncate_bound_from_checkpoint_applies_margin() {
        std::env::set_var("PHOTON_TRANSPORT_RETAIN_SEQ", "5");
        assert_eq!(truncate_bound_from_checkpoint(10), 5);
    }
}

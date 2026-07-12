//! Topic partition and subscription partition keys for reclaim.

use std::collections::HashSet;

use crate::handler_registry::HandlerRegistry;
use crate::retention::config::RetentionPolicy;
use crate::retention::hook::RetentionHook;

/// Storage partition `(topic, optional key)`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TopicPartition {
    /// Topic name.
    pub topic_name: String,
    /// Optional partition / shard key.
    pub topic_key: Option<String>,
}

impl TopicPartition {
    /// Create a partition key.
    pub fn new(topic_name: impl Into<String>, topic_key: Option<String>) -> Self {
        Self {
            topic_name: topic_name.into(),
            topic_key,
        }
    }
}

/// Durable subscription scoped to one transport partition.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SubscriptionPartition {
    /// Durable subscription name.
    pub subscription_name: String,
    /// Topic name.
    pub topic_name: String,
    /// Optional partition / shard key.
    pub topic_key: Option<String>,
}

/// Collect known durable subscriptions for watermark checkpoint loads.
pub fn known_subscriptions(
    policy: &RetentionPolicy,
    hook: Option<&dyn RetentionHook>,
) -> Vec<SubscriptionPartition> {
    let mut seen = HashSet::new();
    let mut out = Vec::new();

    let registry = HandlerRegistry::auto_discover();
    for handler in registry.iter() {
        let entry = SubscriptionPartition {
            subscription_name: handler.subscription_name.to_string(),
            topic_name: handler.topic_name.to_string(),
            topic_key: None,
        };
        if seen.insert(entry.clone()) {
            out.push(entry);
        }
    }

    for entry in &policy.extra_subscriptions {
        if seen.insert(entry.clone()) {
            out.push(entry.clone());
        }
    }

    if let Some(h) = hook {
        for entry in h.extra_subscriptions() {
            if seen.insert(entry.clone()) {
                out.push(entry);
            }
        }
    }

    out
}

/// Subscriptions whose checkpoint applies to the given transport partition.
pub fn subscriptions_for_partition<'a>(
    subs: &'a [SubscriptionPartition],
    topic: &str,
    topic_key: Option<&str>,
) -> Vec<&'a SubscriptionPartition> {
    subs.iter()
        .filter(|s| {
            s.topic_name == topic
                && match (&s.topic_key, topic_key) {
                    (None, _) => true,
                    (Some(sk), Some(tk)) => sk == tk,
                    (Some(_), None) => false,
                }
        })
        .collect()
}

/// Merge partition sets from multiple sources.
pub fn merge_partitions(sources: impl IntoIterator<Item = TopicPartition>) -> Vec<TopicPartition> {
    let mut seen = HashSet::new();
    let mut out = Vec::new();
    for part in sources {
        if seen.insert(part.clone()) {
            out.push(part);
        }
    }
    out
}

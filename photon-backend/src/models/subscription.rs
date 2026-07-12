//! Subscription model.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Subscription mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum SubscriptionMode {
    /// Durable: checkpoints maintained, survives restart.
    #[default]
    Durable,
    /// Ephemeral: no checkpoints, lost on restart.
    Ephemeral,
}

/// Subscription handler registration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Subscription {
    /// Unique subscription ID (UUID).
    pub subscription_id: String,
    /// Subscription name (unique per topic, required for durable).
    pub subscription_name: String,
    /// Topic name.
    pub topic_name: String,
    /// Optional key filter.
    pub topic_key_filter: Option<String>,
    /// Whether the subscription is enabled.
    pub enabled: bool,
    /// Durable or ephemeral.
    pub mode: SubscriptionMode,
    /// When the subscription was created.
    pub created_at: DateTime<Utc>,
}

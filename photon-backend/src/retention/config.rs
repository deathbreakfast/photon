//! Retention policy and environment configuration.

use super::partition::SubscriptionPartition;

/// Host-configurable retention policy (env defaults + optional overrides).
///
/// # Example
///
/// ```
/// use photon_backend::RetentionPolicy;
///
/// let policy = RetentionPolicy {
///     max_age_secs: Some(3600),
///     ..RetentionPolicy::default()
/// };
/// assert_eq!(policy.max_age_secs, Some(3600));
/// ```
#[derive(Debug, Clone)]
pub struct RetentionPolicy {
    /// Drop events older than this many seconds when set (`PHOTON_TRANSPORT_MAX_AGE_SECS`).
    pub max_age_secs: Option<u64>,
    /// Pin reclaim floor at min DLQ seq per partition (`PHOTON_RETENTION_PIN_DLQ`, default true).
    pub pin_dlq: bool,
    /// Background sweep interval in ms; `0` disables automatic sweeps (`PHOTON_RETENTION_SWEEP_MS`).
    pub sweep_interval_ms: u64,
    /// Durable subscriptions not discoverable via [`HandlerRegistry`](crate::HandlerRegistry).
    pub extra_subscriptions: Vec<SubscriptionPartition>,
}

impl Default for RetentionPolicy {
    fn default() -> Self {
        Self {
            max_age_secs: retention_max_age_secs(),
            pin_dlq: retention_pin_dlq(),
            sweep_interval_ms: retention_sweep_ms(),
            extra_subscriptions: Vec::new(),
        }
    }
}

/// Optional TTL from `PHOTON_TRANSPORT_MAX_AGE_SECS` (unset = seq-only retention).
///
/// See the retention section in [`photon::config`](https://docs.rs/uf-photon/latest/photon/config/index.html).
#[must_use]
pub fn retention_max_age_secs() -> Option<u64> {
    std::env::var("PHOTON_TRANSPORT_MAX_AGE_SECS")
        .ok()
        .and_then(|s| s.parse().ok())
        .filter(|&v| v > 0)
}

/// Whether DLQ min-seq pins reclaim (`PHOTON_RETENTION_PIN_DLQ`, default true).
///
/// See the retention section in [`photon::config`](https://docs.rs/uf-photon/latest/photon/config/index.html).
#[must_use]
pub fn retention_pin_dlq() -> bool {
    std::env::var("PHOTON_RETENTION_PIN_DLQ")
        .ok()
        .is_none_or(|s| !matches!(s.as_str(), "0" | "false" | "off"))
}

/// Default seq margin below checkpoint high-water for reclaim (`PHOTON_TRANSPORT_RETAIN_SEQ`, default 5).
///
/// See the transport section in [`photon::config`](https://docs.rs/uf-photon/latest/photon/config/index.html).
#[must_use]
pub fn retain_seq_margin() -> i64 {
    std::env::var("PHOTON_TRANSPORT_RETAIN_SEQ")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(5)
        .max(0)
}

/// Default 30s; `0` = manual [`RetentionReclaimer::sweep_all`](crate::RetentionReclaimer::sweep_all) only.
///
/// See the retention section in [`photon::config`](https://docs.rs/uf-photon/latest/photon/config/index.html).
#[must_use]
pub fn retention_sweep_ms() -> u64 {
    std::env::var("PHOTON_RETENTION_SWEEP_MS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(30_000)
}

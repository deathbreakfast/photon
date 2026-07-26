//! Kafka topic setup for Photon events.

use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use photon_backend::{PhotonError, Result};
use rskafka::client::partition::UnknownTopicHandling;
use tracing::warn;

use crate::config::KafkaConfig;
use crate::connect::SharedClient;

const TOPIC_TIMEOUT_MS: i32 = 30_000;

static WARNED_RETENTION_NOT_APPLIED: AtomicBool = AtomicBool::new(false);

/// Convert Photon retention duration to Kafka `retention.ms` (milliseconds).
///
/// `rskafka` 0.6 `ControllerClient::create_topic` does not accept topic configs, so Photon
/// cannot set `retention.ms` at create time. Use this value when pre-creating topics or
/// configuring broker defaults.
#[must_use]
pub fn retention_ms(retention: Duration) -> i64 {
    i64::try_from(retention.as_millis())
        .unwrap_or(i64::MAX)
        .max(1)
}

fn warn_retention_not_applied(config: &KafkaConfig) {
    if WARNED_RETENTION_NOT_APPLIED.swap(true, Ordering::Relaxed) {
        return;
    }
    warn!(
        retention_ms = retention_ms(config.retention),
        "PHOTON_KAFKA_RETENTION is not applied at topic create (rskafka create_topic has no \
         config API). Pre-create topics with retention.ms={ms} or set broker log.retention.ms; \
         see SECURITY.md production checklist",
        ms = retention_ms(config.retention)
    );
}

/// Ensure the compact checkpoint topic exists.
///
/// # Errors
///
/// Returns an error when topic creation fails.
pub async fn ensure_checkpoint_topic(client: &SharedClient, config: &KafkaConfig) -> Result<()> {
    warn_retention_not_applied(config);
    create_topic_if_missing(
        client,
        &config.checkpoint_topic(),
        1,
        config.effective_replicas(),
    )
    .await
}

/// Ensure a data topic exists before publish/subscribe.
///
/// # Errors
///
/// Returns an error when topic creation fails.
pub async fn ensure_data_topic(
    client: &SharedClient,
    config: &KafkaConfig,
    topic_name: &str,
) -> Result<()> {
    warn_retention_not_applied(config);
    create_topic_if_missing(client, topic_name, 1, config.effective_replicas()).await
}

async fn create_topic_if_missing(
    client: &SharedClient,
    name: &str,
    partitions: i32,
    replication: i32,
) -> Result<()> {
    if topic_exists(client, name).await? {
        return Ok(());
    }

    let controller = client
        .controller_client()
        .map_err(|e| PhotonError::caused(format!("kafka controller client {name}"), e))?;
    match controller
        .create_topic(
            name,
            partitions,
            i16::try_from(replication).unwrap_or(1),
            TOPIC_TIMEOUT_MS,
        )
        .await
    {
        Ok(()) => Ok(()),
        Err(e) if e.to_string().contains("TopicAlreadyExists") => Ok(()),
        Err(e) => Err(PhotonError::caused(format!("kafka create topic {name}"), e)),
    }
}

async fn topic_exists(client: &SharedClient, name: &str) -> Result<bool> {
    match client
        .partition_client(name.to_string(), 0, UnknownTopicHandling::Error)
        .await
    {
        Ok(_) => Ok(true),
        Err(e) if e.to_string().contains("UnknownTopicOrPartition") => Ok(false),
        Err(e) => Err(PhotonError::caused(format!("kafka metadata {name}"), e)),
    }
}

/// Warn when replication settings may limit ingress scaling.
pub fn warn_replication_settings(config: &KafkaConfig) {
    let replicas = config.effective_replicas();
    if config.topic_shards <= 1 && replicas > 1 {
        warn!(
            topic_shards = config.topic_shards,
            replicas,
            "PHOTON_KAFKA_REPLICAS>1 with topic_shards=1 causes sublinear publish ingress; \
             set topic_shards to broker count for write-heavy workloads"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retention_ms_from_fifteen_minutes() {
        assert_eq!(retention_ms(Duration::from_mins(15)), 900_000);
    }

    #[test]
    fn retention_ms_clamps_zero_to_one() {
        assert_eq!(retention_ms(Duration::from_millis(0)), 1);
    }
}

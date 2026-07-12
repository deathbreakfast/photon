//! Fluvio topic setup for Photon events.

use std::time::Duration;

use fluvio::metadata::topic::TopicSpec;
use photon_backend::{PhotonError, Result};
use tracing::warn;

use crate::config::FluvioConfig;
use crate::connect::SharedClient;

fn topic_spec(config: &FluvioConfig) -> TopicSpec {
    TopicSpec::new_computed(
        1,
        u32::try_from(config.effective_replicas()).unwrap_or(1),
        None,
    )
}

/// Ensure the compact checkpoint topic exists.
///
/// # Errors
///
/// Returns an error when topic creation fails.
pub async fn ensure_checkpoint_topic(client: &SharedClient, config: &FluvioConfig) -> Result<()> {
    create_topic_if_missing(client, config, &config.checkpoint_topic()).await
}

/// Ensure a data topic exists before publish/subscribe.
///
/// # Errors
///
/// Returns an error when topic creation fails.
pub async fn ensure_data_topic(
    client: &SharedClient,
    config: &FluvioConfig,
    topic_name: &str,
) -> Result<()> {
    create_topic_if_missing(client, config, topic_name).await
}

async fn create_topic_if_missing(
    client: &SharedClient,
    config: &FluvioConfig,
    name: &str,
) -> Result<()> {
    let admin = client.admin().await;
    let spec = topic_spec(config);
    match admin.create(name.to_string(), false, spec).await {
        Ok(()) => {}
        Err(e) => {
            let msg = e.to_string();
            if msg.contains("already exists")
                || msg.contains("AlreadyExists")
                || msg.contains("TopicAlreadyExists")
            {
                // Fall through to readiness wait — create ack can race producer lookup.
            } else {
                return Err(PhotonError::Internal(format!(
                    "fluvio create topic {name}: {e}"
                )));
            }
        }
    }
    wait_topic_visible(client, name).await
}

/// Wait until SC metadata lists the topic (create returns before SPU routing is ready).
async fn wait_topic_visible(client: &SharedClient, name: &str) -> Result<()> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    let admin = client.admin().await;
    while tokio::time::Instant::now() < deadline {
        match admin.list::<TopicSpec, String>(Vec::new()).await {
            Ok(topics) if topics.iter().any(|t| t.name == name) => {
                // Brief settle so topic_producer's spu_pool.topic_exists sees the same view.
                tokio::time::sleep(Duration::from_millis(150)).await;
                return Ok(());
            }
            Ok(_) => {}
            Err(e) => {
                warn!(topic = name, error = %e, "fluvio list topics while waiting for create");
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    Err(PhotonError::Internal(format!(
        "fluvio topic {name} not visible after create"
    )))
}

/// Warn when replication settings may limit ingress scaling.
pub fn warn_replication_settings(config: &FluvioConfig) {
    let replicas = config.effective_replicas();
    if config.topic_shards <= 1 && replicas > 1 {
        warn!(
            topic_shards = config.topic_shards,
            replicas,
            "PHOTON_FLUVIO_REPLICAS>1 with topic_shards=1 causes sublinear publish ingress; \
             set topic_shards to broker count for write-heavy workloads"
        );
    }
}

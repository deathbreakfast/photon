//! Compose [`AdminSnapshot`] from registries and storage checkpoint reads.
//!
//! Snapshots are metadata-only (topics, handlers, checkpoints). They never include
//! event `actor_json` / `payload_json` or raw DLQ error bodies.

use chrono::{DateTime, Utc};
use photon_backend::{
    redact_credentials_in_text, sanitize_error_message, BackendCapabilities, HandlerDescriptor,
    HandlerHealth, HandlerRegistry, PhotonError, TopicDescriptor,
};

use crate::executor::subscription_of;
use crate::Photon;

use super::types::{
    AdminBackendSummary, AdminCheckpointSummary, AdminHandlerHealth, AdminHandlerSummary,
    AdminPaused, AdminRetrying, AdminSnapshot, AdminTopicSummary,
};

/// Build a full admin snapshot for the given runtime (async checkpoint reads).
///
/// # Errors
///
/// Returns an error if a checkpoint load fails. Error text is sanitized before return.
pub async fn collect_admin_snapshot(photon: &Photon) -> photon_backend::Result<AdminSnapshot> {
    let topics = collect_topics(photon);
    let handlers = collect_handlers(photon)
        .await
        .map_err(|e| sanitize_admin_error(&e))?;
    let backend = collect_backend(photon.backend_capabilities());
    let checkpoints = collect_checkpoints(photon)
        .await
        .map_err(|e| sanitize_admin_error(&e))?;

    Ok(AdminSnapshot {
        backend,
        topics,
        handlers,
        checkpoints,
    })
}

fn sanitize_admin_error(err: &PhotonError) -> PhotonError {
    PhotonError::Internal(sanitize_error_message(&redact_credentials_in_text(
        &err.to_string(),
    )))
}

fn collect_topics(photon: &Photon) -> Vec<AdminTopicSummary> {
    photon
        .registry()
        .sorted_topic_names()
        .into_iter()
        .filter_map(|name| photon.registry().get(name).map(topic_from_descriptor))
        .collect()
}

fn topic_from_descriptor(descriptor: &TopicDescriptor) -> AdminTopicSummary {
    AdminTopicSummary {
        topic_name: descriptor.topic_name.to_string(),
        keyed_by: descriptor.keyed_by.map(str::to_string),
        schema_json: descriptor
            .schema_value()
            .unwrap_or_else(|| serde_json::json!({})),
    }
}

async fn collect_handlers(photon: &Photon) -> photon_backend::Result<Vec<AdminHandlerSummary>> {
    let registry = HandlerRegistry::auto_discover();
    let stats = &photon.runtime().executor_services.stats;
    let now = Utc::now();
    let mut out = Vec::new();
    for handler in registry.iter() {
        let paused = match photon
            .backend()
            .get_subscription_control(subscription_of(handler))
            .await
        {
            Ok(control) => control.map(|c| AdminPaused {
                by: c.paused_by,
                at: c.paused_at,
            }),
            Err(PhotonError::Unsupported(_)) => None,
            Err(e) => return Err(e),
        };
        let health = health_from_stats(stats.snapshot(handler.registry_key, now), paused);
        out.push(handler_from_descriptor(handler, health));
    }
    Ok(out)
}

fn health_from_stats(health: HandlerHealth, paused: Option<AdminPaused>) -> AdminHandlerHealth {
    AdminHandlerHealth {
        last_handled_at: health.last_handled_at,
        last_handled_seq: health.last_handled_seq,
        failures_24h: health.failures_24h,
        last_error: health.last_error,
        last_error_at: health.last_error_at,
        retrying: health.retrying.map(|r| AdminRetrying {
            event_id: r.event_id,
            attempt: r.attempt,
            next_retry_at: r.next_retry_at,
        }),
        paused,
    }
}

fn handler_from_descriptor(
    handler: &HandlerDescriptor,
    health: AdminHandlerHealth,
) -> AdminHandlerSummary {
    let is_group = handler.is_consumer_group();
    AdminHandlerSummary {
        topic_name: handler.topic_name.to_string(),
        subscription_name: if is_group || handler.subscription_name.is_empty() {
            None
        } else {
            Some(handler.subscription_name.to_string())
        },
        consumer_group: handler.consumer_group.map(str::to_string),
        registry_key: handler.registry_key.to_string(),
        mode: if is_group {
            "consumer_group".to_string()
        } else {
            "durable".to_string()
        },
        health,
    }
}

fn collect_backend(caps: BackendCapabilities) -> AdminBackendSummary {
    AdminBackendSummary {
        telemetry_label: caps.telemetry_label.to_string(),
        supports_get_event: caps.supports_get_event,
        supports_list_events: caps.supports_list_events,
        supports_count_events: caps.supports_count_events,
        max_replay_window_secs: caps.max_replay_window.map(|d| d.as_secs()),
        supports_subscription_control: caps.supports_subscription_control,
    }
}

async fn collect_checkpoints(
    photon: &Photon,
) -> photon_backend::Result<Vec<AdminCheckpointSummary>> {
    let registry = HandlerRegistry::auto_discover();
    let lists = photon.backend_capabilities().supports_list_events;
    let mut out = Vec::new();

    for handler in registry.iter() {
        let subscription = subscription_of(handler);
        for topic_key in photon.partitions(handler) {
            let key = topic_key.as_deref();
            let last_seq = photon
                .get_checkpoint_seq(subscription, handler.topic_name, key)
                .await?;
            let head_seq = photon.head_seq(handler.topic_name, key).await?;
            let oldest_waiting_at = if lists {
                oldest_waiting(photon, handler.topic_name, key, last_seq).await?
            } else {
                None
            };
            out.push(AdminCheckpointSummary {
                subscription_name: subscription.to_string(),
                topic_name: handler.topic_name.to_string(),
                topic_key,
                last_seq,
                head_seq,
                oldest_waiting_at,
            });
        }
    }

    Ok(out)
}

/// Creation time of the first event after `last_seq` in a partition.
async fn oldest_waiting(
    photon: &Photon,
    topic: &str,
    topic_key: Option<&str>,
    last_seq: Option<i64>,
) -> photon_backend::Result<Option<DateTime<Utc>>> {
    let next = photon
        .list_events_by_topic(topic, topic_key, Some(last_seq.unwrap_or(0)), 1)
        .await?;
    Ok(next.first().map(|e| e.created_at))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sanitize_admin_error_redacts_secrets_happy_path() {
        let err = PhotonError::Internal(
            "checkpoint load failed password=hunter2 nats://u:p@host:4222".into(),
        );
        let safe = sanitize_admin_error(&err).to_string();
        assert!(safe.contains("[redacted]"), "safe: {safe}");
        assert!(!safe.contains("hunter2"), "safe: {safe}");
        assert!(!safe.contains("u:p@"), "safe: {safe}");
    }

    #[test]
    fn sanitize_admin_error_preserves_benign_detail_sad_path() {
        let err = PhotonError::Internal("checkpoint missing".into());
        let safe = sanitize_admin_error(&err).to_string();
        assert!(safe.contains("checkpoint missing"), "safe: {safe}");
    }
}

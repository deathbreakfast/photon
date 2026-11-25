//! Integration test: `Photon::admin_snapshot` durable handler introspection.
#![allow(missing_docs)]
#![allow(clippy::unused_async)]

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use photon::{configure, subscribe, topic, JsonIdentityFactory, Photon};
use photon_core::Actor;

static HANDLER_INVOCATIONS: AtomicUsize = AtomicUsize::new(0);
static FAILING_INVOCATIONS: AtomicUsize = AtomicUsize::new(0);

#[topic(name = "test.admin.failing")]
pub struct TestAdminFailingEvent {
    pub value: u32,
}

#[subscribe(topic = "test.admin.failing", durable = "admin-failing-handler")]
async fn on_admin_failing(
    _actor: Box<dyn Actor>,
    event: TestAdminFailingEvent,
) -> photon::Result<()> {
    FAILING_INVOCATIONS.fetch_add(1, Ordering::SeqCst);
    Err(photon::PhotonError::Internal(format!(
        "rejected value {} password=hunter2",
        event.value
    )))
}

#[topic(name = "test.admin.snapshot")]
pub struct TestAdminSnapshotEvent {
    pub value: u32,
}

#[subscribe(topic = "test.admin.snapshot", durable = "admin-test-handler")]
async fn on_admin_snapshot_test(
    actor: Box<dyn Actor>,
    event: TestAdminSnapshotEvent,
) -> photon::Result<()> {
    assert_eq!(actor.label(), "photon_publish");
    assert_eq!(event.value, 7);
    HANDLER_INVOCATIONS.fetch_add(1, Ordering::SeqCst);
    Ok(())
}

#[tokio::test]
async fn admin_snapshot_reports_topics_handlers_and_checkpoint() {
    HANDLER_INVOCATIONS.store(0, Ordering::SeqCst);

    let photon = Photon::builder()
        .auto_registry()
        .build()
        .expect("build photon");

    photon
        .start_executor(Arc::new(JsonIdentityFactory))
        .expect("start executor");
    configure(photon.clone());

    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    TestAdminSnapshotEvent { value: 7 }
        .publish()
        .await
        .expect("publish");

    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    assert_eq!(HANDLER_INVOCATIONS.load(Ordering::SeqCst), 1);

    photon
        .runtime()
        .executor_services
        .checkpoint_coalescer
        .flush()
        .await
        .expect("flush checkpoints");

    let snapshot = photon.admin_snapshot().await.expect("admin snapshot");

    assert!(snapshot.backend.supports_get_event);
    assert!(snapshot.backend.supports_list_events);
    assert_eq!(snapshot.backend.telemetry_label, "mem");

    let topic = snapshot
        .topics
        .iter()
        .find(|t| t.topic_name == "test.admin.snapshot")
        .expect("topic in snapshot");
    assert!(topic.schema_json.is_object());

    let handler = snapshot
        .handlers
        .iter()
        .find(|h| h.registry_key == "test.admin.snapshot:admin-test-handler")
        .expect("handler in snapshot");
    assert_eq!(handler.mode, "durable");
    assert_eq!(
        handler.subscription_name.as_deref(),
        Some("admin-test-handler")
    );
    assert!(handler.consumer_group.is_none());

    let checkpoint = snapshot
        .checkpoints
        .iter()
        .find(|c| {
            c.subscription_name == "admin-test-handler"
                && c.topic_name == "test.admin.snapshot"
                && c.topic_key.is_none()
        })
        .expect("checkpoint in snapshot");
    assert!(checkpoint.last_seq.is_some());
    assert!(
        checkpoint.oldest_waiting_at.is_none(),
        "nothing waits after the handled event"
    );
    assert!(snapshot.backend.supports_subscription_control);

    // A handler that succeeds reports when it last handled an event.
    assert_eq!(handler.health.failures_24h, 0);
    assert!(handler.health.last_handled_at.is_some());
    assert_eq!(handler.health.last_handled_seq, checkpoint.last_seq);
    assert!(handler.health.paused.is_none());
}

#[tokio::test]
async fn admin_snapshot_reports_failures_last_error_and_dlq_records() {
    FAILING_INVOCATIONS.store(0, Ordering::SeqCst);

    let photon = Photon::builder()
        .auto_registry()
        .build()
        .expect("build photon");
    photon
        .start_executor(Arc::new(JsonIdentityFactory))
        .expect("start executor");
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let mut ids = Vec::new();
    for value in 1..=3 {
        ids.push(
            photon
                .publish(
                    "test.admin.failing",
                    None,
                    serde_json::json!({"label": "test"}),
                    serde_json::json!({ "value": value }),
                )
                .await
                .expect("publish"),
        );
    }

    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
    while tokio::time::Instant::now() < deadline
        && photon.dlq_records(Some("admin-failing-handler"), 10).len() < 3
    {
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    assert_eq!(FAILING_INVOCATIONS.load(Ordering::SeqCst), 3);

    let snapshot = photon.admin_snapshot().await.expect("admin snapshot");
    let handler = snapshot
        .handlers
        .iter()
        .find(|h| h.registry_key == "test.admin.failing:admin-failing-handler")
        .expect("failing handler in snapshot");
    assert_eq!(handler.health.failures_24h, 3);
    let last_error = handler.health.last_error.as_deref().expect("last error");
    assert!(last_error.contains("rejected value 3"), "{last_error}");
    assert!(!last_error.contains("hunter2"), "{last_error}");
    assert!(handler.health.last_error_at.is_some());
    assert!(handler.health.last_handled_at.is_none());

    let records = photon.dlq_records(Some("admin-failing-handler"), 10);
    assert_eq!(records.len(), 3);
    assert_eq!(records[0].event_id, ids[2], "newest first");
    let for_event = photon.dlq_for_event(&ids[0]);
    assert_eq!(for_event.len(), 1);
    assert_eq!(
        for_event[0].subscription_name.as_deref(),
        Some("admin-failing-handler")
    );

    // Dead-lettered events never advance the checkpoint, so the first one is still waiting.
    let checkpoint = snapshot
        .checkpoints
        .iter()
        .find(|c| c.subscription_name == "admin-failing-handler")
        .expect("failing checkpoint");
    let first = photon
        .get_event(&ids[0])
        .await
        .expect("get event")
        .expect("first event");
    assert_eq!(checkpoint.oldest_waiting_at, Some(first.created_at));

    photon.shutdown_executor();
    photon.join_executor().await;
}

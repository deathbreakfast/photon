//! Integration tests for the Photon runtime surface (mem tier): builder wiring,
//! publish/subscribe happy paths, executor lifecycle including failing-handler
//! DLQ routing, and admin snapshot composition.

#![cfg(feature = "mem")]
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use futures::stream::Stream;
use futures::StreamExt;
use photon_backend::{
    BackendCapabilities, BackendContext, Event, GenericPhotonBackend, HandlerDescriptor,
    InProcStoragePort, PhotonBackend, PhotonError, RetentionPolicy, StoragePort, TopicDescriptor,
    TopicRegistry, TransportCrypto,
};
use photon_core::JsonIdentityFactory;
use photon_runtime::{Photon, ReplayFrom};
use photon_telemetry::NoOpsLog;

const TOPIC: &str = "rt.orders";

static DELIVERED: AtomicUsize = AtomicUsize::new(0);

fn ok_invoke<'a>(
    _identity: &'a dyn photon_core::IdentityFactory,
    _event: &'a photon_backend::Event,
) -> Pin<Box<dyn Future<Output = photon_backend::Result<()>> + Send + 'a>> {
    Box::pin(async {
        DELIVERED.fetch_add(1, Ordering::SeqCst);
        Ok(())
    })
}

fn fail_invoke<'a>(
    _identity: &'a dyn photon_core::IdentityFactory,
    _event: &'a photon_backend::Event,
) -> Pin<Box<dyn Future<Output = photon_backend::Result<()>> + Send + 'a>> {
    Box::pin(async { Err(PhotonError::Internal("handler rejected event".into())) })
}

photon_backend::inventory::submit! {
    TopicDescriptor::new(TOPIC, None, "{}")
}

photon_backend::inventory::submit! {
    HandlerDescriptor::new(TOPIC, "rt-ok", "rt.orders:rt-ok", ok_invoke)
}

photon_backend::inventory::submit! {
    HandlerDescriptor::new(TOPIC, "rt-fail", "rt.orders:rt-fail", fail_invoke)
}

fn mem_port() -> Arc<dyn StoragePort> {
    Arc::new(InProcStoragePort::new(TransportCrypto::from_bytes(
        *b"photon-dev-transport-key-32bytes",
    )))
}

fn build_photon() -> Photon {
    Photon::builder()
        .storage_port(mem_port())
        .auto_registry()
        .build()
        .expect("build mem runtime")
}

async fn wait_until(mut condition: impl FnMut() -> bool) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while !condition() {
        assert!(
            tokio::time::Instant::now() < deadline,
            "condition not reached within timeout"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

#[tokio::test]
async fn executor_dispatches_ok_handler_and_dead_letters_failing_handler() {
    let photon = build_photon();
    let identity = Arc::new(JsonIdentityFactory);

    photon
        .start_executor(Arc::clone(&identity) as _)
        .expect("first start succeeds");

    // Sad path: a second start on the same runtime must be rejected.
    let err = photon
        .start_executor(identity as _)
        .expect_err("second start is rejected");
    assert!(matches!(err, PhotonError::Internal(_)));

    let before = DELIVERED.load(Ordering::SeqCst);
    photon
        .publish(
            TOPIC,
            None,
            serde_json::json!({}),
            serde_json::json!({"n": 1}),
        )
        .await
        .expect("publish");

    // Happy path: the ok handler runs; sad path: the failing handler lands in the DLQ.
    let dlq = Arc::clone(&photon.runtime().executor_services.dlq);
    wait_until(|| DELIVERED.load(Ordering::SeqCst) > before).await;
    wait_until(|| !dlq.is_empty()).await;
    assert_eq!(dlq.min_seq_for(TOPIC, None), Some(1));

    // DLQ reads and handler health.
    let records = photon.dlq_records(Some("rt-fail"), 10);
    assert_eq!(records.len(), 1);
    assert!(photon.dlq_records(Some("rt-ok"), 10).is_empty());
    assert_eq!(photon.dlq_for_event(&records[0].event_id).len(), 1);
    let snapshot = photon.admin_snapshot().await.expect("snapshot");
    let health = |key: &str| {
        snapshot
            .handlers
            .iter()
            .find(|h| h.registry_key == key)
            .map(|h| h.health.clone())
            .expect("handler")
    };
    let failing = health("rt.orders:rt-fail");
    assert_eq!(failing.failures_24h, 1);
    assert!(failing
        .last_error
        .is_some_and(|e| e.contains("handler rejected event")));
    wait_until(|| {
        photon
            .runtime()
            .executor_services
            .stats
            .snapshot("rt.orders:rt-ok", chrono_now())
            .last_handled_seq
            .is_some()
    })
    .await;

    // Redeliver runs the one handler again, outside the stream.
    let before = DELIVERED.load(Ordering::SeqCst);
    let event_id = records[0].event_id.clone();
    photon
        .redeliver(&event_id, "rt.orders:rt-ok", "test")
        .await
        .expect("redeliver ok handler");
    assert_eq!(DELIVERED.load(Ordering::SeqCst), before + 1);
    let err = photon
        .redeliver(&event_id, "rt.orders:rt-fail", "test")
        .await
        .expect_err("failing handler fails again");
    assert!(err.to_string().contains("handler rejected event"));
    assert_eq!(photon.dlq_records(Some("rt-fail"), 10).len(), 2);

    photon.shutdown_executor();
    photon.join_executor().await;
}

#[tokio::test]
async fn admin_snapshot_reports_backend_topics_handlers_and_checkpoints() {
    let photon = build_photon();
    let snapshot = photon.admin_snapshot().await.expect("snapshot");

    assert_eq!(snapshot.backend.telemetry_label, "mem");
    assert!(snapshot.backend.supports_get_event);
    assert!(snapshot.backend.supports_list_events);

    assert!(snapshot.topics.iter().any(|t| t.topic_name == TOPIC));

    let modes: Vec<&str> = snapshot
        .handlers
        .iter()
        .filter(|h| h.topic_name == TOPIC)
        .map(|h| h.mode.as_str())
        .collect();
    assert_eq!(modes.len(), 2);
    assert!(modes.iter().all(|m| *m == "durable"));

    // Fresh runtime: inventory handlers are listed with no committed checkpoints.
    let cursors: Vec<_> = snapshot
        .checkpoints
        .iter()
        .filter(|c| c.topic_name == TOPIC)
        .collect();
    assert_eq!(cursors.len(), 2);
    assert!(cursors.iter().all(|c| c.last_seq.is_none()));
}

#[tokio::test]
async fn get_event_and_checkpoints_roundtrip() {
    let photon = build_photon();

    let event_id = photon
        .publish(
            TOPIC,
            None,
            serde_json::json!({}),
            serde_json::json!({"n": 7}),
        )
        .await
        .expect("publish");

    let event = photon
        .get_event(&event_id)
        .await
        .expect("get_event")
        .expect("event stored");
    assert_eq!(event.topic_name, TOPIC);
    assert_eq!(event.payload_json["n"], 7);

    // Sad path: unknown event id resolves to None, not an error.
    assert!(photon
        .get_event("does-not-exist")
        .await
        .expect("get_event")
        .is_none());

    photon
        .set_checkpoint("sub-a", TOPIC, None, 41)
        .await
        .expect("set checkpoint");
    assert_eq!(
        photon
            .get_checkpoint_seq("sub-a", TOPIC, None)
            .await
            .expect("get checkpoint"),
        Some(41)
    );
    // Sad path: unknown subscription has no cursor.
    assert_eq!(
        photon
            .get_checkpoint_seq("sub-unknown", TOPIC, None)
            .await
            .expect("get checkpoint"),
        None
    );
}

#[tokio::test]
async fn subscribe_stream_replays_published_events_in_order() {
    let photon = build_photon();

    for n in 1..=3 {
        photon
            .publish(
                TOPIC,
                None,
                serde_json::json!({}),
                serde_json::json!({"n": n}),
            )
            .await
            .expect("publish");
    }

    let mut stream = photon.subscribe(TOPIC, None, Some(0));
    let mut seqs = Vec::new();
    for _ in 0..3 {
        let event = tokio::time::timeout(Duration::from_secs(5), stream.next())
            .await
            .expect("replay within timeout")
            .expect("stream open")
            .expect("event ok");
        seqs.push(event.seq);
    }
    assert!(seqs.windows(2).all(|w| w[0] < w[1]), "seqs: {seqs:?}");
}

#[tokio::test]
async fn subscribe_consumer_group_with_no_shards_ends_immediately() {
    let photon = build_photon();
    let mut stream = photon.subscribe_consumer_group(TOPIC, &[], std::collections::HashMap::new());
    assert!(stream.next().await.is_none());
}

#[tokio::test]
async fn reclaim_transport_succeeds_on_fresh_runtime() {
    let photon = build_photon();
    let reports = photon.reclaim_transport().await.expect("reclaim");
    // A fresh runtime has nothing past the safe watermark to remove.
    assert!(
        reports.iter().all(|r| r.removed == 0),
        "reports: {reports:?}"
    );
}

#[tokio::test]
async fn configure_sets_process_default() {
    let photon = build_photon();
    assert_eq!(photon.backend_label(), "mem");

    photon_runtime::configure(photon);
    let handle = photon_runtime::default().expect("default configured");
    assert_eq!(handle.backend_label(), "mem");
}

#[tokio::test]
async fn builder_accepts_prebuilt_backend_and_install_context() {
    // Pre-built backend path.
    let backend = GenericPhotonBackend::install_with_port(
        BackendContext {
            registry: TopicRegistry::new(),
        },
        mem_port(),
    )
    .expect("install backend");
    let photon = Photon::builder()
        .backend(backend)
        .storage_port(mem_port())
        .build()
        .expect("build with prebuilt backend");
    assert_eq!(photon.backend_label(), "mem");

    // Install-from-context path, plus ops log and retention policy wiring.
    let port = mem_port();
    let photon = Photon::builder()
        .backend_with_context(move |ctx| GenericPhotonBackend::install_with_port(ctx, port))
        .storage_port(mem_port())
        .ops_log(NoOpsLog)
        .ops_log_arc(Arc::new(NoOpsLog))
        .retention_policy(RetentionPolicy::default())
        .build()
        .expect("build with install fn");
    assert_eq!(photon.backend_label(), "mem");
}

fn chrono_now() -> chrono::DateTime<chrono::Utc> {
    chrono::Utc::now()
}

#[tokio::test]
async fn subscription_operations_reject_unknown_handlers() {
    let photon = build_photon();
    let key = "rt.orders:missing";
    let not_found = |r: Result<(), PhotonError>| {
        assert!(
            matches!(r, Err(PhotonError::SubscriptionNotFound(_))),
            "{r:?}"
        );
    };
    not_found(photon.pause(key, "t").await);
    not_found(photon.resume(key, "t").await);
    not_found(photon.retry_now(key, "t"));
    not_found(photon.redeliver("e", key, "t").await);
    not_found(photon.skip_to_latest(key, "t").await.map(|_| ()));
    not_found(
        photon
            .replay_preview(key, &ReplayFrom::Seq(1))
            .await
            .map(|_| ()),
    );
    not_found(
        photon
            .replay(key, &ReplayFrom::Seq(1), "t")
            .await
            .map(|_| ()),
    );
}

#[tokio::test]
async fn redeliver_needs_a_started_executor() {
    let photon = build_photon();
    let err = photon
        .redeliver("e", "rt.orders:rt-ok", "t")
        .await
        .expect_err("executor not started");
    assert!(matches!(err, PhotonError::Internal(_)), "{err:?}");
}

#[tokio::test]
async fn replay_preview_counts_from_seq_and_skip_moves_to_head() {
    let photon = build_photon();
    for n in 0..4 {
        photon
            .publish(
                TOPIC,
                None,
                serde_json::json!({}),
                serde_json::json!({ "n": n }),
            )
            .await
            .expect("publish");
    }
    photon
        .set_checkpoint("rt-ok", TOPIC, None, 2)
        .await
        .expect("checkpoint");
    let preview = photon
        .replay_preview("rt.orders:rt-ok", &ReplayFrom::Seq(2))
        .await
        .expect("preview");
    assert_eq!(preview.from_seq, 2);
    assert_eq!(preview.count, 3);
    assert_eq!(preview.already_handled, 1);

    let applied = photon
        .replay("rt.orders:rt-ok", &ReplayFrom::Seq(2), "t")
        .await
        .expect("replay");
    assert_eq!(applied, preview);
    assert_eq!(
        photon
            .get_checkpoint_seq("rt-ok", TOPIC, None)
            .await
            .expect("load"),
        Some(1)
    );

    let skipped = photon
        .skip_to_latest("rt.orders:rt-ok", "t")
        .await
        .expect("skip");
    assert_eq!(skipped, 3);
    assert_eq!(
        photon
            .get_checkpoint_seq("rt-ok", TOPIC, None)
            .await
            .expect("load"),
        Some(4)
    );
}

/// A custom backend that implements only the required methods, so every optional one keeps
/// its default.
struct MinimalBackend(GenericPhotonBackend);

#[async_trait]
impl PhotonBackend for MinimalBackend {
    fn capabilities(&self) -> BackendCapabilities {
        BackendCapabilities::broker("minimal")
    }

    async fn publish(
        &self,
        topic_name: &str,
        topic_key: Option<&str>,
        actor_json: serde_json::Value,
        payload_json: serde_json::Value,
    ) -> photon_backend::Result<String> {
        PhotonBackend::publish(&self.0, topic_name, topic_key, actor_json, payload_json).await
    }

    fn subscribe(
        &self,
        topic_name: String,
        topic_key_filter: Option<String>,
        after_seq: Option<i64>,
    ) -> Pin<Box<dyn Stream<Item = photon_backend::Result<Event>> + Send>> {
        PhotonBackend::subscribe(&self.0, topic_name, topic_key_filter, after_seq)
    }

    async fn get_event(&self, event_id: &str) -> photon_backend::Result<Option<Event>> {
        PhotonBackend::get_event(&self.0, event_id).await
    }

    fn registry(&self) -> &TopicRegistry {
        PhotonBackend::registry(&self.0)
    }

    async fn get_checkpoint_seq(
        &self,
        subscription_name: &str,
        topic_name: &str,
        topic_key: Option<&str>,
    ) -> photon_backend::Result<Option<i64>> {
        self.0
            .get_checkpoint_seq(subscription_name, topic_name, topic_key)
            .await
    }

    async fn set_checkpoint(
        &self,
        subscription_name: &str,
        topic_name: &str,
        topic_key: Option<&str>,
        last_seq: i64,
    ) -> photon_backend::Result<()> {
        self.0
            .set_checkpoint(subscription_name, topic_name, topic_key, last_seq)
            .await
    }
}

#[tokio::test]
async fn backends_without_control_report_unsupported() {
    let photon = Photon::builder()
        .backend(Arc::new(MinimalBackend(GenericPhotonBackend::new(
            mem_port(),
            TopicRegistry::new(),
        ))))
        .storage_port(mem_port())
        .auto_registry()
        .build()
        .expect("build minimal runtime");
    let key = "rt.orders:rt-ok";

    let snapshot = photon.admin_snapshot().await.expect("snapshot");
    assert!(!snapshot.backend.supports_subscription_control);
    assert!(snapshot.handlers.iter().all(|h| h.health.paused.is_none()));

    let unsupported = |r: Result<(), PhotonError>| {
        assert!(matches!(r, Err(PhotonError::Unsupported(_))), "{r:?}");
    };
    unsupported(photon.pause(key, "t").await);
    unsupported(photon.resume(key, "t").await);
    // A time needs event listing, which a broker-tier backend lacks.
    unsupported(
        photon
            .replay_preview(key, &ReplayFrom::Time(chrono_now()))
            .await
            .map(|_| ()),
    );
    // Rewinding needs reset_checkpoint.
    photon
        .set_checkpoint("rt-ok", TOPIC, None, 5)
        .await
        .expect("checkpoint");
    unsupported(
        photon
            .replay(key, &ReplayFrom::Seq(1), "t")
            .await
            .map(|_| ()),
    );
    // Older than the broker tier's 15 minute window.
    let old = chrono_now() - chrono::Duration::hours(1);
    assert!(matches!(
        photon.replay_preview(key, &ReplayFrom::Time(old)).await,
        Err(PhotonError::ReplayOutOfWindow { .. })
    ));
}

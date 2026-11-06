//! Handler executor: inventory dispatch loop for `#[photon::subscribe]` handlers.
//!
//! [`ExecutorController::start`] is invoked by [`Photon::start_executor`]. It auto-discovers
//! [`photon_backend::HandlerDescriptor`] entries submitted by the proc macro, subscribes to each
//! topic with durable checkpoint replay, and dispatches through [`photon_core::IdentityFactory`].
//!
//! Call [`ExecutorController::shutdown`] then [`ExecutorController::join`] for graceful teardown.

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use futures::StreamExt;
use photon_backend::{
    consumer_group::instance_id_from_env,
    consumer_group::static_assigned_shards,
    consumer_group::{ConsumerGroupCoordinator, GroupMember, StaticGroupCoordinator},
    delivery::DlqRecordParams,
    delivery_mode::ShardConfig,
    handler_descriptor::HandlerInvoker,
    instrumentation::FailureReason,
    Event, ExecutorServices, HandlerDescriptor, HandlerRegistry, PhotonError, Result,
};
use photon_core::IdentityFactory;
use tokio::task::{JoinHandle, JoinSet};
use tokio_util::sync::CancellationToken;

use crate::Photon;

type EventStream = Pin<Box<dyn futures::Stream<Item = Result<Event>> + Send>>;

/// Controls background handler dispatch tasks started by [`Photon::start_executor`].
pub struct ExecutorController {
    started: AtomicBool,
    cancel: CancellationToken,
    tasks: std::sync::Mutex<Vec<JoinHandle<()>>>,
}

impl Default for ExecutorController {
    fn default() -> Self {
        Self {
            started: AtomicBool::new(false),
            cancel: CancellationToken::new(),
            tasks: std::sync::Mutex::new(Vec::new()),
        }
    }
}

impl ExecutorController {
    /// Spawn subscription loops for every inventory-registered handler.
    ///
    /// # Errors
    ///
    /// Returns an error if the executor was already started on this controller.
    ///
    /// # Contract
    ///
    /// - Start is one-shot per controller; restart requires a new [`Photon`] build.
    /// - Outer loops respect [`Self::shutdown`]; in-flight handler tasks are awaited in
    ///   [`Self::join`].
    /// - A stream that ends or errors is resubscribed from the stored checkpoint with capped
    ///   backoff.
    /// - Handler errors are retried per `PHOTON_HANDLER_MAX_RETRIES` (default 0) before the DLQ.
    #[tracing::instrument(name = "photon.executor.start", skip_all)]
    pub fn start(&self, photon: &Photon, identity: &Arc<dyn IdentityFactory>) -> Result<()> {
        if self
            .started
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return Err(PhotonError::Internal(
                "Photon executor already started".into(),
            ));
        }

        let registry = HandlerRegistry::auto_discover();
        if registry.is_empty() {
            return Ok(());
        }

        let mut tasks = Vec::new();
        for handler in registry.iter() {
            let cancel = self.cancel.child_token();
            let task = if handler.is_consumer_group() {
                spawn_group_handler_loop(photon.clone(), Arc::clone(identity), handler, cancel)
            } else {
                spawn_handler_loop(photon.clone(), Arc::clone(identity), handler, cancel)
            };
            tasks.push(task);
        }

        *self
            .tasks
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = tasks;
        Ok(())
    }

    /// Signal all handler loops to stop accepting new events.
    ///
    /// # Contract
    ///
    /// - Idempotent: repeated calls are no-ops.
    /// - Does not wait for in-flight handlers; call [`Self::join`] afterward.
    pub fn shutdown(&self) {
        self.cancel.cancel();
    }

    /// Await outer subscription loops (and their in-flight handler tasks).
    ///
    /// # Contract
    ///
    /// - Call [`Self::shutdown`] first for prompt exit from stream loops.
    /// - Drains stored outer [`JoinHandle`]s; each loop drains its own in-flight [`JoinSet`]
    ///   before exiting.
    /// - Safe to call when no tasks were started (no-op).
    pub async fn join(&self) {
        let tasks = std::mem::take(
            &mut *self
                .tasks
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
        );
        for task in tasks {
            let _ = task.await;
        }
    }
}

const fn failure_reason(err: &PhotonError) -> FailureReason {
    match err {
        PhotonError::Identity(_) => FailureReason::IdentityBuild,
        _ => FailureReason::HandlerError,
    }
}

/// Reconnect delay bounds for a subscription loop whose stream ended or errored.
const RESUBSCRIBE_BASE: Duration = Duration::from_millis(100);
const RESUBSCRIBE_MAX: Duration = Duration::from_secs(10);

/// Capped exponential backoff with jitter.
struct Backoff {
    base: Duration,
    max: Duration,
    attempt: u32,
}

impl Backoff {
    const fn new(base: Duration, max: Duration) -> Self {
        Self {
            base,
            max,
            attempt: 0,
        }
    }

    /// Next delay: `base * 2^attempt` capped at `max`, then jittered into the upper half.
    fn next_delay(&mut self) -> Duration {
        let exp = self.base.saturating_mul(1u32 << self.attempt.min(16));
        let capped = exp.min(self.max);
        self.attempt = self.attempt.saturating_add(1);
        let half = capped / 2;
        let jitter_nanos = u64::from(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_or(0, |d| d.subsec_nanos()),
        ) % (u64::try_from(half.as_nanos()).unwrap_or(u64::MAX).max(1));
        half + Duration::from_nanos(jitter_nanos)
    }

    const fn reset(&mut self) {
        self.attempt = 0;
    }
}

/// Handler retry settings, read once per subscription loop.
///
/// `PHOTON_HANDLER_MAX_RETRIES` (default 0) extra attempts after the first failure, spaced by
/// `PHOTON_HANDLER_RETRY_BACKOFF_MS` (default 100) doubling per attempt.
#[derive(Clone, Copy)]
struct RetryPolicy {
    max_retries: u32,
    base_delay: Duration,
}

impl RetryPolicy {
    fn from_env() -> Self {
        let max_retries = std::env::var("PHOTON_HANDLER_MAX_RETRIES")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);
        let base_ms = std::env::var("PHOTON_HANDLER_RETRY_BACKOFF_MS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(100);
        Self {
            max_retries,
            base_delay: Duration::from_millis(base_ms),
        }
    }
}

/// Identity failures are deterministic, so only handler errors are retried.
const fn is_retryable(err: &PhotonError) -> bool {
    !matches!(err, PhotonError::Identity(_))
}

fn record_dlq(services: &ExecutorServices, params: &DlqRecordParams<'_>) {
    if let Err(e) = services.dlq.record(params) {
        tracing::error!(
            event_id = params.event_id,
            topic = params.topic_name,
            seq = params.seq,
            error = %e,
            "DLQ write failed; delivery failure is not recorded"
        );
    }
}

/// Run one handler delivery (with retries), then checkpoint or dead-letter it.
async fn dispatch_event(
    identity: Arc<dyn IdentityFactory>,
    services: Arc<ExecutorServices>,
    invoke: HandlerInvoker,
    event: Event,
    subscription: String,
    retry: RetryPolicy,
) {
    let event_id = event.event_id.clone();
    let topic_name = event.topic_name.clone();
    let topic_key = event.topic_key.clone();
    let seq = event.seq;

    let mut attempt = 0u32;
    let result = loop {
        match invoke(identity.as_ref(), &event).await {
            Ok(()) => break Ok(()),
            Err(e) if attempt < retry.max_retries && is_retryable(&e) => {
                tokio::time::sleep(retry.base_delay.saturating_mul(1u32 << attempt.min(16))).await;
                attempt += 1;
            }
            Err(e) => break Err(e),
        }
    };

    match result {
        Ok(()) => {
            if let Err(e) = services
                .checkpoint_coalescer
                .record(&subscription, &topic_name, topic_key.as_deref(), seq)
                .await
            {
                record_dlq(
                    &services,
                    &DlqRecordParams {
                        event_id: &event_id,
                        topic_name: &topic_name,
                        topic_key: topic_key.as_deref(),
                        seq,
                        subscription_name: Some(&subscription),
                        reason: FailureReason::CheckpointError,
                        error: e.to_string(),
                    },
                );
            }
        }
        Err(e) => record_dlq(
            &services,
            &DlqRecordParams {
                event_id: &event_id,
                topic_name: &topic_name,
                topic_key: topic_key.as_deref(),
                seq,
                subscription_name: Some(&subscription),
                reason: failure_reason(&e),
                error: e.to_string(),
            },
        ),
    }
}

fn shard_count_for_handler(photon: &Photon, handler: &'static HandlerDescriptor) -> u32 {
    handler
        .group_shard_count
        .or_else(|| {
            photon
                .registry()
                .get(handler.topic_name)
                .and_then(|d| d.shard_config)
                .map(|c| c.shard_count)
        })
        .unwrap_or(ShardConfig::DEFAULT_SHARD_COUNT)
}

/// Why a consume pass ended.
enum PassEnd {
    /// Shutdown requested, or the worker pool closed; do not resubscribe.
    Stop,
    /// Stream ended or errored; resubscribe after backoff.
    Reconnect,
}

/// Per-loop state shared by every consume pass.
struct LoopCtx {
    cancel: CancellationToken,
    services: Arc<ExecutorServices>,
    identity: Arc<dyn IdentityFactory>,
    handler: &'static HandlerDescriptor,
    /// Durable subscription name, or the consumer group id for group handlers.
    subscription: String,
    retry: RetryPolicy,
}

/// Drain `stream`, spawning one delivery task per event, until it ends, errors or is cancelled.
async fn consume_stream(
    mut stream: EventStream,
    ctx: &LoopCtx,
    inflight: &mut JoinSet<()>,
    backoff: &mut Backoff,
) -> PassEnd {
    let LoopCtx {
        cancel,
        services,
        identity,
        handler,
        subscription,
        retry,
    } = ctx;
    let subscription = subscription.as_str();
    let retry = *retry;
    let topic = handler.topic_name;
    loop {
        tokio::select! {
            biased;
            () = cancel.cancelled() => return PassEnd::Stop,
            event_result = stream.next() => {
                let Some(event_result) = event_result else {
                    tracing::warn!(topic = topic, subscription = subscription, "subscription stream ended; resubscribing");
                    return PassEnd::Reconnect;
                };
                match event_result {
                    Ok(event) => {
                        let permit = match services.worker_pool.acquire().await {
                            Ok(p) => p,
                            Err(e) => {
                                // A closed pool never recovers; skipping the event would let the
                                // checkpoint move past it.
                                tracing::error!(
                                    topic = topic,
                                    subscription = subscription,
                                    error = %e,
                                    "worker pool closed; stopping handler loop"
                                );
                                return PassEnd::Stop;
                            }
                        };
                        backoff.reset();
                        let task = dispatch_event(
                            Arc::clone(identity),
                            Arc::clone(services),
                            handler.invoke,
                            event,
                            subscription.to_string(),
                            retry,
                        );
                        inflight.spawn(async move {
                            let _permit = permit;
                            task.await;
                        });
                    }
                    Err(e) if e.is_config() => {
                        tracing::error!(
                            topic = topic,
                            subscription = subscription,
                            error = %e,
                            "subscription configuration error; stopping handler loop"
                        );
                        return PassEnd::Stop;
                    }
                    Err(PhotonError::StreamItem { context, event_id, seq, source }) => {
                        match (event_id, seq) {
                            (Some(event_id), Some(seq)) => {
                                tracing::error!(
                                    topic = topic,
                                    subscription = subscription,
                                    event_id = %event_id,
                                    seq = seq,
                                    error = %source,
                                    "{context} bad stream item; dead-lettering and continuing"
                                );
                                record_dlq(
                                    services,
                                    &DlqRecordParams {
                                        event_id: &event_id,
                                        topic_name: topic,
                                        topic_key: None,
                                        seq,
                                        subscription_name: Some(subscription),
                                        reason: FailureReason::StreamItem,
                                        error: format!("{context} {source}"),
                                    },
                                );
                            }
                            (_, seq) => tracing::error!(
                                topic = topic,
                                subscription = subscription,
                                seq = ?seq,
                                error = %source,
                                "{context} bad stream item; skipping"
                            ),
                        }
                    }
                    Err(e) => {
                        tracing::warn!(
                            topic = topic,
                            subscription = subscription,
                            error = %e,
                            "subscription stream error; resubscribing"
                        );
                        return PassEnd::Reconnect;
                    }
                }
            }
        }
    }
}

/// Sleep for the next backoff delay; returns `false` when shutdown arrived first.
async fn backoff_or_cancel(backoff: &mut Backoff, cancel: &CancellationToken) -> bool {
    tokio::select! {
        () = cancel.cancelled() => false,
        () = tokio::time::sleep(backoff.next_delay()) => true,
    }
}

fn spawn_group_handler_loop(
    photon: Photon,
    identity: Arc<dyn IdentityFactory>,
    handler: &'static HandlerDescriptor,
    cancel: CancellationToken,
) -> JoinHandle<()> {
    let topic = handler.topic_name;
    let group_id = handler.consumer_group.unwrap_or("");
    let services = Arc::clone(&photon.runtime().executor_services);
    let coordinator = StaticGroupCoordinator;
    let retry = RetryPolicy::from_env();

    tokio::spawn(async move {
        let shard_count = shard_count_for_handler(&photon, handler);
        let instance_id = instance_id_from_env().unwrap_or_else(|| "0".into());
        let assigned = match coordinator
            .register(GroupMember {
                group_id: group_id.to_string(),
                instance_id: instance_id.clone(),
                topic_name: topic.to_string(),
                shard_count,
            })
            .await
        {
            Ok(shards) => shards,
            Err(e) => {
                tracing::warn!(
                    topic = topic,
                    group = group_id,
                    error = %e,
                    "consumer group registration failed; using static shard assignment"
                );
                static_assigned_shards(shard_count)
            }
        };

        let ctx = LoopCtx {
            cancel,
            services,
            identity,
            handler,
            subscription: group_id.to_string(),
            retry,
        };
        let mut inflight = JoinSet::new();
        let mut backoff = Backoff::new(RESUBSCRIBE_BASE, RESUBSCRIBE_MAX);
        loop {
            let mut after_seq_by_shard = HashMap::new();
            for shard_id in &assigned {
                let shard_key = photon_backend::shard_storage_key(*shard_id);
                let seq = photon
                    .get_checkpoint_seq(group_id, topic, Some(&shard_key))
                    .await
                    .ok()
                    .flatten()
                    .unwrap_or(0);
                after_seq_by_shard.insert(*shard_id, Some(seq));
            }
            let stream = photon.subscribe_consumer_group(topic, &assigned, after_seq_by_shard);
            let end = consume_stream(stream, &ctx, &mut inflight, &mut backoff).await;
            if matches!(end, PassEnd::Stop) || !backoff_or_cancel(&mut backoff, &ctx.cancel).await {
                break;
            }
        }

        while inflight.join_next().await.is_some() {}
    })
}

fn spawn_handler_loop(
    photon: Photon,
    identity: Arc<dyn IdentityFactory>,
    handler: &'static HandlerDescriptor,
    cancel: CancellationToken,
) -> JoinHandle<()> {
    let topic = handler.topic_name;
    let subscription_name = handler.subscription_name;
    let services = Arc::clone(&photon.runtime().executor_services);
    let retry = RetryPolicy::from_env();

    tokio::spawn(async move {
        let ctx = LoopCtx {
            cancel,
            services,
            identity,
            handler,
            subscription: subscription_name.to_string(),
            retry,
        };
        let mut inflight = JoinSet::new();
        let mut backoff = Backoff::new(RESUBSCRIBE_BASE, RESUBSCRIBE_MAX);
        loop {
            // Durable handlers always replay from a cursor. Missing checkpoint → start at 0
            // (not live-only `None`), so publishes that land before the subscribe handshake
            // are still delivered from storage.
            let after_seq = Some(
                photon
                    .get_checkpoint_seq(subscription_name, topic, None)
                    .await
                    .ok()
                    .flatten()
                    .unwrap_or(0),
            );
            let stream = photon.subscribe(topic, None, after_seq);
            let end = consume_stream(stream, &ctx, &mut inflight, &mut backoff).await;
            if matches!(end, PassEnd::Stop) || !backoff_or_cancel(&mut backoff, &ctx.cancel).await {
                break;
            }
        }

        while inflight.join_next().await.is_some() {}
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_grows_caps_and_resets() {
        let mut b = Backoff::new(Duration::from_millis(100), Duration::from_millis(800));
        let first = b.next_delay();
        assert!(first >= Duration::from_millis(50) && first <= Duration::from_millis(100));
        for _ in 0..10 {
            let d = b.next_delay();
            assert!(d <= Duration::from_millis(800));
        }
        let capped = b.next_delay();
        assert!(capped >= Duration::from_millis(400));
        b.reset();
        assert!(b.next_delay() <= Duration::from_millis(100));
    }

    #[test]
    fn identity_errors_are_not_retried() {
        assert!(!is_retryable(&PhotonError::Identity("bad".into())));
        assert!(is_retryable(&PhotonError::Internal("boom".into())));
    }

    #[cfg(feature = "mem")]
    mod stream {
        use std::collections::HashMap;
        use std::sync::Mutex;

        use photon_backend::retention::RetentionPolicy;
        use photon_backend::{InProcStoragePort, StoragePort, TransportCrypto};
        use photon_core::JsonIdentityFactory;

        use super::super::*;

        static SEEN: Mutex<Vec<(String, i64)>> = Mutex::new(Vec::new());

        fn record_seen<'a>(
            _id: &'a dyn IdentityFactory,
            event: &'a Event,
        ) -> Pin<Box<dyn std::future::Future<Output = Result<()>> + Send + 'a>> {
            Box::pin(async move {
                SEEN.lock()
                    .unwrap()
                    .push((event.topic_name.clone(), event.seq));
                Ok(())
            })
        }

        fn event(topic: &str, seq: i64) -> Event {
            serde_json::from_value(serde_json::json!({
                "event_id": format!("e{seq}"),
                "topic_name": topic,
                "topic_key": null,
                "seq": seq,
                "actor_json": {},
                "payload_json": {},
                "created_at": "2026-01-01T00:00:00Z",
            }))
            .expect("event json")
        }

        fn ctx(topic: &'static str) -> LoopCtx {
            let port: Arc<dyn StoragePort> = Arc::new(InProcStoragePort::new(
                TransportCrypto::from_bytes(*b"photon-dev-transport-key-32bytes"),
            ));
            let handler: &'static HandlerDescriptor = Box::leak(Box::new(HandlerDescriptor::new(
                topic,
                "sub",
                "k",
                record_seen,
            )));
            LoopCtx {
                cancel: CancellationToken::new(),
                services: Arc::new(ExecutorServices::new(
                    port,
                    RetentionPolicy::default(),
                    None,
                )),
                identity: Arc::new(JsonIdentityFactory),
                handler,
                subscription: "sub".into(),
                retry: RetryPolicy {
                    max_retries: 0,
                    base_delay: Duration::from_millis(1),
                },
            }
        }

        async fn run(ctx: &LoopCtx, items: Vec<Result<Event>>) -> (PassEnd, Vec<i64>) {
            let mut inflight = JoinSet::new();
            let mut backoff = Backoff::new(RESUBSCRIBE_BASE, RESUBSCRIBE_MAX);
            let end = consume_stream(
                Box::pin(futures::stream::iter(items)),
                ctx,
                &mut inflight,
                &mut backoff,
            )
            .await;
            while inflight.join_next().await.is_some() {}
            let topic = ctx.handler.topic_name;
            let mut seen: HashMap<String, Vec<i64>> = HashMap::new();
            for (t, seq) in SEEN.lock().unwrap().iter() {
                seen.entry(t.clone()).or_default().push(*seq);
            }
            let mut got = seen.remove(topic).unwrap_or_default();
            got.sort_unstable();
            (end, got)
        }

        #[tokio::test]
        async fn item_error_mid_stream_is_dead_lettered_and_stream_continues() {
            let c = ctx("exec.unit.item");
            let bad = PhotonError::stream_item("open", Some("e2".into()), Some(2), "bad tag");
            let (end, got) = run(
                &c,
                vec![
                    Ok(event("exec.unit.item", 1)),
                    Err(bad),
                    Ok(event("exec.unit.item", 3)),
                ],
            )
            .await;
            assert_eq!(got, vec![1, 3]);
            assert!(
                matches!(end, PassEnd::Reconnect),
                "only the stream end reconnects"
            );
            assert_eq!(c.services.dlq.len(), 1);
        }

        #[tokio::test]
        async fn item_error_without_id_is_skipped_without_dlq() {
            let c = ctx("exec.unit.noid");
            let bad = PhotonError::stream_item("decode", None, None, "garbage");
            let (_, got) = run(&c, vec![Err(bad), Ok(event("exec.unit.noid", 1))]).await;
            assert_eq!(got, vec![1]);
            assert!(c.services.dlq.is_empty());
        }

        #[tokio::test]
        async fn fatal_error_ends_the_pass_for_resubscribe() {
            let c = ctx("exec.unit.fatal");
            let fatal = PhotonError::caused("nats subscribe recv:", "connection reset");
            let (end, got) = run(
                &c,
                vec![
                    Ok(event("exec.unit.fatal", 1)),
                    Err(fatal),
                    Ok(event("exec.unit.fatal", 3)),
                ],
            )
            .await;
            assert_eq!(got, vec![1]);
            assert!(matches!(end, PassEnd::Reconnect));
            assert!(c.services.dlq.is_empty());
        }

        #[tokio::test]
        async fn invalid_topic_stops_the_loop() {
            let c = ctx("exec.unit.badtopic");
            let (end, got) = run(&c, vec![Err(PhotonError::InvalidTopicName("bad*".into()))]).await;
            assert_eq!(got, Vec::<i64>::new());
            assert!(matches!(end, PassEnd::Stop));
        }
    }
}

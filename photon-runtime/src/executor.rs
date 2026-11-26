//! Handler executor: inventory dispatch loop for `#[photon::subscribe]` handlers.
//!
//! [`ExecutorController::start`] is invoked by [`Photon::start_executor`]. It auto-discovers
//! [`photon_backend::HandlerDescriptor`] entries submitted by the proc macro, subscribes to each
//! topic with durable checkpoint replay, and dispatches through [`photon_core::IdentityFactory`].
//!
//! Call [`ExecutorController::shutdown`] then [`ExecutorController::join`] for graceful teardown.

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use chrono::Utc;
use futures::StreamExt;
use photon_backend::{
    consumer_group::instance_id_from_env,
    consumer_group::static_assigned_shards,
    consumer_group::{ConsumerGroupCoordinator, GroupMember, StaticGroupCoordinator},
    delivery::DlqRecordParams,
    delivery_mode::ShardConfig,
    executor_services::HandlerSignal,
    instrumentation::FailureReason,
    Event, ExecutorServices, HandlerDescriptor, HandlerRegistry, PhotonBackend, PhotonError,
    Result, RetryState,
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
    identity: OnceLock<Arc<dyn IdentityFactory>>,
}

impl Default for ExecutorController {
    fn default() -> Self {
        Self {
            started: AtomicBool::new(false),
            cancel: CancellationToken::new(),
            tasks: std::sync::Mutex::new(Vec::new()),
            identity: OnceLock::new(),
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
        let _ = self.identity.set(Arc::clone(identity));

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

    /// Identity factory passed to [`Self::start`], once started.
    pub(crate) fn identity(&self) -> Option<Arc<dyn IdentityFactory>> {
        self.identity.get().cloned()
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
pub struct RetryPolicy {
    max_retries: u32,
    base_delay: Duration,
}

impl RetryPolicy {
    pub fn from_env() -> Self {
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

/// Checkpoint and stats key for a handler: durable subscription name, or consumer group id.
pub fn subscription_of(handler: &HandlerDescriptor) -> &'static str {
    handler.consumer_group.unwrap_or(handler.subscription_name)
}

/// Run one handler delivery (with retries), then checkpoint or dead-letter it.
///
/// Updates the handler's [`photon_backend::HandlerStats`] on success, retry and failure.
/// Returns the final error when the delivery was dead-lettered.
pub async fn dispatch_event(
    identity: Arc<dyn IdentityFactory>,
    services: Arc<ExecutorServices>,
    handler: &'static HandlerDescriptor,
    event: Event,
    subscription: String,
    retry: RetryPolicy,
) -> Result<()> {
    let key = handler.registry_key;
    let signal = services.signals.get(key);
    let event_id = event.event_id.clone();
    let topic_name = event.topic_name.clone();
    let topic_key = event.topic_key.clone();
    let seq = event.seq;

    let mut attempt = 0u32;
    let result = loop {
        match (handler.invoke)(identity.as_ref(), &event).await {
            Ok(()) => break Ok(()),
            Err(e) if attempt < retry.max_retries && is_retryable(&e) => {
                let delay = retry.base_delay.saturating_mul(1u32 << attempt.min(16));
                services.stats.record_retry(
                    key,
                    RetryState {
                        event_id: event_id.clone(),
                        attempt: attempt + 2,
                        next_retry_at: Utc::now()
                            + chrono::Duration::from_std(delay).unwrap_or(chrono::TimeDelta::MAX),
                    },
                );
                tokio::select! {
                    () = tokio::time::sleep(delay) => {}
                    () = signal.retry_now.notified() => {}
                }
                attempt += 1;
            }
            Err(e) => break Err(e),
        }
    };

    let (reason, err) = match result {
        Ok(()) => match services
            .checkpoint_coalescer
            .record(&subscription, &topic_name, topic_key.as_deref(), seq)
            .await
        {
            Ok(()) => {
                services.stats.record_success(key, seq, Utc::now());
                return Ok(());
            }
            Err(e) => (FailureReason::CheckpointError, e),
        },
        Err(e) => (failure_reason(&e), e),
    };
    let message = err.to_string();
    services.stats.record_failure(key, &message, Utc::now());
    record_dlq(
        &services,
        &DlqRecordParams {
            event_id: &event_id,
            topic_name: &topic_name,
            topic_key: topic_key.as_deref(),
            seq,
            subscription_name: Some(&subscription),
            reason,
            error: message,
        },
    );
    Err(err)
}

pub fn shard_count_for_handler(photon: &Photon, handler: &'static HandlerDescriptor) -> u32 {
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
    /// Paused, or an operation woke the loop; drain, then recheck before resubscribing.
    Control,
    /// Another node rewound the checkpoint; drop pending commits, then resubscribe.
    Rewound,
}

/// How often a loop re-reads its subscription's control state from the backend.
const CONTROL_REFRESH: Duration = Duration::from_secs(5);

/// What a loop should do after reading its subscription's control state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ControlState {
    Run,
    Paused,
    /// The stored rewind generation moved past the one this node last saw.
    Rewound,
}

/// Cached control state for one subscription, refreshed at most every [`CONTROL_REFRESH`].
///
/// Reading through the backend (not a local flag) lets a pause, replay or skip run on another
/// node sharing the store reach this loop.
struct ControlCache {
    /// `None` when the backend has no subscription control (never paused).
    backend: Option<Arc<dyn PhotonBackend>>,
    subscription: String,
    signal: Arc<HandlerSignal>,
    paused: bool,
    checked_at: Option<tokio::time::Instant>,
}

impl ControlCache {
    fn new(
        backend: Option<Arc<dyn PhotonBackend>>,
        subscription: &str,
        signal: Arc<HandlerSignal>,
    ) -> Self {
        let backend = backend.filter(|b| b.capabilities().supports_subscription_control);
        Self {
            backend,
            subscription: subscription.to_string(),
            signal,
            paused: false,
            checked_at: None,
        }
    }

    /// The subscription's control state; `refresh` forces a backend read.
    ///
    /// A cached answer is never [`ControlState::Rewound`]: a rewind is reported once, by the
    /// read that first sees its generation.
    async fn check(&mut self, refresh: bool) -> ControlState {
        let Some(backend) = &self.backend else {
            return ControlState::Run;
        };
        if !refresh
            && self
                .checked_at
                .is_some_and(|at| at.elapsed() < CONTROL_REFRESH)
        {
            return self.cached();
        }
        self.checked_at = Some(tokio::time::Instant::now());
        match backend.get_subscription_control(&self.subscription).await {
            Ok(control) => {
                self.paused = control.pause.is_some();
                if self.signal.see_generation(control.rewind_generation) {
                    return ControlState::Rewound;
                }
            }
            Err(PhotonError::Unsupported(_)) => {
                self.backend = None;
                self.paused = false;
            }
            Err(e) => tracing::warn!(
                subscription = %self.subscription,
                error = %e,
                "subscription control read failed; keeping the last known state"
            ),
        }
        self.cached()
    }

    /// Whether the subscription is paused, after a forced backend read.
    ///
    /// Used just before a pass subscribes: a rewind seen here is only recorded, since the new
    /// stream starts from the stored checkpoint anyway.
    async fn paused_now(&mut self) -> bool {
        self.check(true).await;
        self.paused
    }

    const fn cached(&self) -> ControlState {
        if self.paused {
            ControlState::Paused
        } else {
            ControlState::Run
        }
    }
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
    signal: Arc<HandlerSignal>,
}

/// Drain `stream`, spawning one delivery task per event, until it ends, errors or is cancelled.
async fn consume_stream(
    mut stream: EventStream,
    ctx: &LoopCtx,
    inflight: &mut JoinSet<()>,
    backoff: &mut Backoff,
    control: &mut ControlCache,
) -> PassEnd {
    let LoopCtx {
        cancel,
        services,
        identity,
        handler,
        subscription,
        retry,
        signal,
    } = ctx;
    let subscription = subscription.as_str();
    let retry = *retry;
    let topic = handler.topic_name;
    // Re-read control state while the stream is idle too, so a pause or rewind from another
    // node lands without waiting for the next event.
    let mut refresh = tokio::time::interval_at(
        tokio::time::Instant::now() + CONTROL_REFRESH,
        CONTROL_REFRESH,
    );
    refresh.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            biased;
            () = cancel.cancelled() => return PassEnd::Stop,
            () = signal.wake.notified() => return PassEnd::Control,
            _ = refresh.tick() => match control.check(false).await {
                ControlState::Run => {}
                ControlState::Paused => return PassEnd::Control,
                ControlState::Rewound => return PassEnd::Rewound,
            },
            event_result = stream.next() => {
                let Some(event_result) = event_result else {
                    tracing::warn!(topic = topic, subscription = subscription, "subscription stream ended; resubscribing");
                    return PassEnd::Reconnect;
                };
                match event_result {
                    Ok(event) => {
                        // The event is dropped unhandled; the resubscribe after resume or a
                        // rewind reads it again from the checkpoint.
                        match control.check(false).await {
                            ControlState::Run => {}
                            ControlState::Paused => return PassEnd::Control,
                            ControlState::Rewound => return PassEnd::Rewound,
                        }
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
                            handler,
                            event,
                            subscription.to_string(),
                            retry,
                        );
                        inflight.spawn(async move {
                            let _permit = permit;
                            let _ = task.await;
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

/// Wait out a pause or a held loop; returns `false` when shutdown arrived first.
async fn park(ctx: &LoopCtx) -> bool {
    tokio::select! {
        biased;
        () = ctx.cancel.cancelled() => false,
        () = ctx.signal.wake.notified() => true,
        () = tokio::time::sleep(CONTROL_REFRESH) => true,
    }
}

/// Finish in-flight deliveries and write their checkpoints, so an operation that moves the
/// checkpoint is not overwritten by them afterwards.
async fn drain(ctx: &LoopCtx, inflight: &mut JoinSet<()>) {
    while inflight.join_next().await.is_some() {}
    if let Err(e) = ctx.services.checkpoint_coalescer.flush().await {
        tracing::warn!(
            subscription = %ctx.subscription,
            error = %e,
            "checkpoint flush failed while parking the handler loop"
        );
    }
}

/// Drop the pass after another node rewound the checkpoint: discard this subscription's pending
/// commits (they would raise the rewound checkpoint again), let in-flight deliveries finish,
/// and discard what they recorded too.
async fn abandon_pass(ctx: &LoopCtx, inflight: &mut JoinSet<()>) {
    let coalescer = &ctx.services.checkpoint_coalescer;
    coalescer.discard(&ctx.subscription).await;
    while inflight.join_next().await.is_some() {}
    coalescer.discard(&ctx.subscription).await;
}

/// Run consume passes until shutdown.
///
/// Each pass holds the handler's `run_lock`. Pause/replay/skip wake the loop, which drains and
/// releases the lock; a paused subscription parks until resumed, re-reading its state every
/// [`CONTROL_REFRESH`]. A replay or skip on another node shows up as a new rewind generation on
/// that refresh; the loop then drops the pass without committing and resubscribes. `open` builds
/// a stream from the current stored checkpoint.
async fn run_passes<F, Fut>(ctx: LoopCtx, mut control: ControlCache, mut open: F)
where
    F: FnMut() -> Fut,
    Fut: Future<Output = EventStream>,
{
    ctx.signal.set_running(true);
    let mut inflight = JoinSet::new();
    let mut backoff = Backoff::new(RESUBSCRIBE_BASE, RESUBSCRIBE_MAX);
    loop {
        let guard = tokio::select! {
            biased;
            () = ctx.cancel.cancelled() => break,
            guard = ctx.signal.run_lock.lock() => guard,
        };
        if ctx.signal.held() || control.paused_now().await {
            drop(guard);
            if !park(&ctx).await {
                break;
            }
            continue;
        }
        let stream = open().await;
        let end = consume_stream(stream, &ctx, &mut inflight, &mut backoff, &mut control).await;
        match end {
            PassEnd::Stop => break,
            PassEnd::Control => {
                drain(&ctx, &mut inflight).await;
                drop(guard);
                // Let an operation waiting on `run_lock` take it before the next pass.
                tokio::task::yield_now().await;
            }
            PassEnd::Rewound => {
                abandon_pass(&ctx, &mut inflight).await;
                drop(guard);
            }
            PassEnd::Reconnect => {
                drop(guard);
                if !backoff_or_cancel(&mut backoff, &ctx.cancel).await {
                    break;
                }
            }
        }
    }

    while inflight.join_next().await.is_some() {}
    ctx.signal.set_running(false);
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
            signal: services.signals.get(handler.registry_key),
            services,
            identity,
            handler,
            subscription: group_id.to_string(),
            retry,
        };
        let control = ControlCache::new(
            Some(Arc::clone(photon.backend())),
            group_id,
            Arc::clone(&ctx.signal),
        );
        run_passes(ctx, control, || {
            let photon = photon.clone();
            let assigned = assigned.clone();
            async move {
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
                photon.subscribe_consumer_group(topic, &assigned, after_seq_by_shard)
            }
        })
        .await;
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
            signal: services.signals.get(handler.registry_key),
            services,
            identity,
            handler,
            subscription: subscription_name.to_string(),
            retry,
        };
        let control = ControlCache::new(
            Some(Arc::clone(photon.backend())),
            subscription_name,
            Arc::clone(&ctx.signal),
        );
        run_passes(ctx, control, || {
            let photon = photon.clone();
            async move {
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
                photon.subscribe(topic, None, after_seq)
            }
        })
        .await;
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

        use photon_backend::handler_descriptor::HandlerInvoker;
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
                signal: Arc::new(HandlerSignal::default()),
            }
        }

        async fn run(ctx: &LoopCtx, items: Vec<Result<Event>>) -> (PassEnd, Vec<i64>) {
            let mut inflight = JoinSet::new();
            let mut backoff = Backoff::new(RESUBSCRIBE_BASE, RESUBSCRIBE_MAX);
            let mut control = ControlCache::new(None, "sub", Arc::clone(&ctx.signal));
            let end = consume_stream(
                Box::pin(futures::stream::iter(items)),
                ctx,
                &mut inflight,
                &mut backoff,
                &mut control,
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

        fn control_cache(port: Arc<dyn StoragePort>, signal: Arc<HandlerSignal>) -> ControlCache {
            let backend: Arc<dyn PhotonBackend> =
                Arc::new(photon_backend::GenericPhotonBackend::new(
                    port,
                    photon_backend::TopicRegistry::auto_discover(),
                ));
            ControlCache::new(Some(backend), "sub", signal)
        }

        #[tokio::test]
        async fn control_cache_reports_a_new_rewind_generation_once() {
            let port: Arc<dyn StoragePort> = Arc::new(InProcStoragePort::new(
                TransportCrypto::from_bytes(*b"photon-dev-transport-key-32bytes"),
            ));
            // A rewind stored before the loop started is absorbed by the first read.
            port.bump_rewind_generation("sub").await.expect("bump");
            let mut control = control_cache(Arc::clone(&port), Arc::new(HandlerSignal::default()));
            assert!(!control.paused_now().await);
            assert_eq!(control.check(true).await, ControlState::Run);

            port.bump_rewind_generation("sub").await.expect("bump");
            assert_eq!(control.check(true).await, ControlState::Rewound);
            assert_eq!(control.check(true).await, ControlState::Run);
        }

        #[tokio::test]
        async fn control_cache_ignores_a_rewind_this_node_ran() {
            let port: Arc<dyn StoragePort> = Arc::new(InProcStoragePort::new(
                TransportCrypto::from_bytes(*b"photon-dev-transport-key-32bytes"),
            ));
            let signal = Arc::new(HandlerSignal::default());
            let mut control = control_cache(Arc::clone(&port), Arc::clone(&signal));
            assert!(!control.paused_now().await);

            let generation = port.bump_rewind_generation("sub").await.expect("bump");
            signal.see_generation(generation);
            assert_eq!(control.check(true).await, ControlState::Run);
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

        fn always_fail<'a>(
            _id: &'a dyn IdentityFactory,
            _event: &'a Event,
        ) -> Pin<Box<dyn std::future::Future<Output = Result<()>> + Send + 'a>> {
            Box::pin(async { Err(PhotonError::Internal("handler boom".into())) })
        }

        static FLAKY_CALLS: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);

        fn fail_once<'a>(
            _id: &'a dyn IdentityFactory,
            _event: &'a Event,
        ) -> Pin<Box<dyn std::future::Future<Output = Result<()>> + Send + 'a>> {
            Box::pin(async {
                if FLAKY_CALLS.fetch_add(1, Ordering::SeqCst) == 0 {
                    Err(PhotonError::Internal("first try fails".into()))
                } else {
                    Ok(())
                }
            })
        }

        fn leak(
            topic: &'static str,
            key: &'static str,
            invoke: HandlerInvoker,
        ) -> &'static HandlerDescriptor {
            Box::leak(Box::new(HandlerDescriptor::new(topic, "sub", key, invoke)))
        }

        #[tokio::test]
        async fn stats_record_success_and_failure_per_handler() {
            let c = ctx("exec.unit.stats");
            let (_, got) = run(&c, vec![Ok(event("exec.unit.stats", 4))]).await;
            assert_eq!(got, vec![4]);
            let ok = c.services.stats.snapshot("k", Utc::now());
            assert_eq!(ok.last_handled_seq, Some(4));
            assert!(ok.last_handled_at.is_some());

            let failing = leak("exec.unit.stats", "k-fail", always_fail);
            for seq in 1..=3 {
                let err = dispatch_event(
                    Arc::clone(&c.identity),
                    Arc::clone(&c.services),
                    failing,
                    event("exec.unit.stats", seq),
                    "sub".into(),
                    c.retry,
                )
                .await;
                assert!(err.is_err());
            }
            let health = c.services.stats.snapshot("k-fail", Utc::now());
            assert_eq!(health.failures_24h, 3);
            assert!(health
                .last_error
                .as_deref()
                .is_some_and(|e| e.contains("handler boom")));
            assert_eq!(c.services.dlq.records(Some("sub"), 10).len(), 3);
        }

        #[tokio::test]
        async fn retry_now_cuts_the_backoff_short() {
            let c = ctx("exec.unit.retry");
            let handler = leak("exec.unit.retry", "k-retry", fail_once);
            let task = tokio::spawn(dispatch_event(
                Arc::clone(&c.identity),
                Arc::clone(&c.services),
                handler,
                event("exec.unit.retry", 1),
                "sub".into(),
                RetryPolicy {
                    max_retries: 1,
                    base_delay: Duration::from_secs(600),
                },
            ));
            let retrying = loop {
                if let Some(r) = c.services.stats.snapshot("k-retry", Utc::now()).retrying {
                    break r;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            };
            assert_eq!(retrying.attempt, 2);
            assert_eq!(retrying.event_id, "e1");
            c.services.signals.get("k-retry").retry_now.notify_waiters();
            let outcome = tokio::time::timeout(Duration::from_secs(5), task)
                .await
                .expect("retry ran before the 600 s backoff")
                .expect("join");
            assert!(outcome.is_ok());
            let health = c.services.stats.snapshot("k-retry", Utc::now());
            assert_eq!(health.last_handled_seq, Some(1));
            assert!(health.retrying.is_none());
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

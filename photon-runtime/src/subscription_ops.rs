//! Subscription health and control operations on [`Photon`]: DLQ reads, pause and resume,
//! replay, skip, retry now and redeliver.

use std::sync::Arc;

use chrono::{DateTime, Utc};
use photon_backend::delivery::DlqRecord;
use photon_backend::executor_services::HandlerSignal;
use photon_backend::instrumentation::log_subscription_op;
use photon_backend::{
    sanitize_error_message, shard_storage_key, HandlerDescriptor, HandlerRegistry, PhotonError,
    Result, SubscriptionControl,
};
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::executor::{dispatch_event, shard_count_for_handler, subscription_of, RetryPolicy};
use crate::Photon;

/// Where [`Photon::replay`] starts.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReplayFrom {
    /// This sequence number, inclusive (durable handlers only; seqs are per shard for groups).
    Seq(i64),
    /// The first event created at or after this time.
    Time(DateTime<Utc>),
    /// This event, by id.
    Event(String),
}

/// What [`Photon::replay`] will resend, from [`Photon::replay_preview`].
///
/// For consumer-group handlers the counts add up every shard and `from_seq` is the lowest
/// shard start.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplayPreview {
    /// First sequence the replay delivers again.
    pub from_seq: i64,
    /// Events the subscription will receive after the replay (up to the current head).
    pub count: u64,
    /// How many of those it has already handled once (each runs again).
    pub already_handled: u64,
}

/// Events read per page while resolving a replay time.
const REPLAY_PAGE: usize = 500;

/// How long an operation waits for the loop to park before waking it again.
const PARK_RETRY: std::time::Duration = std::time::Duration::from_millis(250);

/// One checkpoint partition of a replay: durable `None`, or a consumer-group shard key.
struct PartitionPlan {
    topic_key: Option<String>,
    /// First seq to deliver again (`head + 1` when nothing in this partition qualifies).
    from_seq: i64,
    head: i64,
    checkpoint: i64,
}

impl PartitionPlan {
    /// Checkpoint the replay writes: one before `from_seq`, never forward of the current one.
    fn target(&self) -> i64 {
        (self.from_seq - 1).min(self.checkpoint)
    }
}

#[allow(clippy::cast_sign_loss)] // clamped at zero first
fn non_negative(n: i64) -> u64 {
    n.max(0) as u64
}

fn preview_of(plans: &[PartitionPlan]) -> ReplayPreview {
    ReplayPreview {
        from_seq: plans.iter().map(|p| p.from_seq).min().unwrap_or(1),
        count: plans
            .iter()
            .map(|p| non_negative(p.head - p.target()))
            .sum(),
        already_handled: plans
            .iter()
            .map(|p| non_negative(p.checkpoint.min(p.head) - p.from_seq + 1))
            .sum(),
    }
}

fn handler_for(registry_key: &str) -> Result<&'static HandlerDescriptor> {
    HandlerRegistry::auto_discover()
        .iter()
        .find(|h| h.registry_key == registry_key)
        .ok_or_else(|| PhotonError::SubscriptionNotFound(registry_key.to_string()))
}

/// Wake the handler's loop until it drains and releases `run_lock`, then hold the lock.
async fn park_loop(signal: &HandlerSignal) -> tokio::sync::MutexGuard<'_, ()> {
    loop {
        signal.set_held(true);
        signal.wake.notify_one();
        if let Ok(guard) = tokio::time::timeout(PARK_RETRY, signal.run_lock.lock()).await {
            return guard;
        }
    }
}

fn release_loop(signal: &HandlerSignal, guard: tokio::sync::MutexGuard<'_, ()>) {
    signal.set_held(false);
    drop(guard);
    signal.wake.notify_one();
}

/// Subscription health and control operations for host admin UIs.
///
/// Every method takes a handler's `registry_key` (see
/// [`AdminHandlerSummary::registry_key`](crate::AdminHandlerSummary::registry_key)). Mutating
/// methods take `by`, a display label recorded in the `photon_subscription_op` ops-log event;
/// Photon does not resolve identity or check permissions, so the host must authorize the caller
/// first. For consumer-group handlers each operation acts on the group's shared checkpoints and
/// pause state, so it affects every member.
impl Photon {
    /// Newest-first dead-letter records, optionally for one subscription (durable name or
    /// consumer group id).
    ///
    /// The DLQ is in memory: it holds this node's failures since start.
    #[must_use]
    pub fn dlq_records(&self, subscription: Option<&str>, limit: usize) -> Vec<DlqRecord> {
        self.runtime()
            .executor_services
            .dlq
            .records(subscription, limit)
    }

    /// Every dead-letter record for one event, newest first.
    #[must_use]
    pub fn dlq_for_event(&self, event_id: &str) -> Vec<DlqRecord> {
        self.runtime().executor_services.dlq.for_event(event_id)
    }

    /// Stop dispatching to a handler until [`Self::resume`].
    ///
    /// The pause is stored in the backend, so every node sharing the store stops within the
    /// 5 s control refresh; this node stops at once. In-flight deliveries finish.
    ///
    /// # Errors
    ///
    /// [`PhotonError::SubscriptionNotFound`] for an unknown key, [`PhotonError::Unsupported`]
    /// when the backend lacks subscription control, or a storage error.
    pub async fn pause(&self, registry_key: &str, by: &str) -> Result<()> {
        let handler = handler_for(registry_key)?;
        let subscription = subscription_of(handler);
        let control = SubscriptionControl {
            paused_by: by.to_string(),
            paused_at: Utc::now(),
        };
        self.backend()
            .set_subscription_control(subscription, Some(control))
            .await?;
        self.signal(registry_key).wake.notify_one();
        log_subscription_op("pause", registry_key, subscription, by, &json!({}));
        Ok(())
    }

    /// Clear a pause; the handler restarts from its stored checkpoint.
    ///
    /// # Errors
    ///
    /// Same as [`Self::pause`].
    pub async fn resume(&self, registry_key: &str, by: &str) -> Result<()> {
        let handler = handler_for(registry_key)?;
        let subscription = subscription_of(handler);
        self.backend()
            .set_subscription_control(subscription, None)
            .await?;
        self.signal(registry_key).wake.notify_one();
        log_subscription_op("resume", registry_key, subscription, by, &json!({}));
        Ok(())
    }

    /// What [`Self::replay`] would resend, without moving any checkpoint.
    ///
    /// Flushes pending checkpoint writes first so the counts reflect deliveries that already
    /// finished on this node.
    ///
    /// # Errors
    ///
    /// [`PhotonError::SubscriptionNotFound`], [`PhotonError::EventNotFound`],
    /// [`PhotonError::ReplayOutOfWindow`] when the point is older than the backend's replay
    /// window, [`PhotonError::Unsupported`] for a time on a backend without event listing or a
    /// seq on a consumer group, or a storage error.
    pub async fn replay_preview(
        &self,
        registry_key: &str,
        from: &ReplayFrom,
    ) -> Result<ReplayPreview> {
        let handler = handler_for(registry_key)?;
        self.runtime()
            .executor_services
            .checkpoint_coalescer
            .flush()
            .await?;
        Ok(preview_of(&self.replay_plan(handler, from).await?))
    }

    /// Move the handler's checkpoint back so it receives events again from `from`.
    ///
    /// Parks this node's loop for the handler (in-flight deliveries finish and checkpoint
    /// first), rewinds every partition's checkpoint to one before its start, then lets the loop
    /// resubscribe. Loops on other nodes pick the rewind up on their next resubscribe. Returns
    /// the preview it acted on.
    ///
    /// # Errors
    ///
    /// Same as [`Self::replay_preview`], plus [`PhotonError::Unsupported`] when the backend
    /// cannot rewind checkpoints.
    pub async fn replay(
        &self,
        registry_key: &str,
        from: &ReplayFrom,
        by: &str,
    ) -> Result<ReplayPreview> {
        let handler = handler_for(registry_key)?;
        let subscription = subscription_of(handler);
        let signal = self.signal(registry_key);
        let guard = park_loop(&signal).await;
        let result: Result<_> = async {
            self.runtime()
                .executor_services
                .checkpoint_coalescer
                .flush()
                .await?;
            let plans = self.replay_plan(handler, from).await?;
            for plan in &plans {
                if plan.target() < plan.checkpoint {
                    self.backend()
                        .reset_checkpoint(
                            subscription,
                            handler.topic_name,
                            plan.topic_key.as_deref(),
                            plan.target(),
                        )
                        .await?;
                }
            }
            Ok(preview_of(&plans))
        }
        .await;
        release_loop(&signal, guard);
        let preview = result?;
        log_subscription_op(
            "replay",
            registry_key,
            subscription,
            by,
            &json!({
                "from": from,
                "from_seq": preview.from_seq,
                "count": preview.count,
                "already_handled": preview.already_handled,
            }),
        );
        Ok(preview)
    }

    /// Move the handler's checkpoint to the newest event, skipping everything unhandled.
    ///
    /// Returns how many events were skipped.
    ///
    /// # Errors
    ///
    /// [`PhotonError::SubscriptionNotFound`] or a storage error.
    pub async fn skip_to_latest(&self, registry_key: &str, by: &str) -> Result<u64> {
        let handler = handler_for(registry_key)?;
        let subscription = subscription_of(handler);
        let signal = self.signal(registry_key);
        let guard = park_loop(&signal).await;
        let result: Result<_> = async {
            self.runtime()
                .executor_services
                .checkpoint_coalescer
                .flush()
                .await?;
            let mut skipped = 0u64;
            for key in self.partitions(handler) {
                let head = self
                    .head_seq(handler.topic_name, key.as_deref())
                    .await?
                    .unwrap_or(0);
                let checkpoint = self
                    .get_checkpoint_seq(subscription, handler.topic_name, key.as_deref())
                    .await?
                    .unwrap_or(0);
                if head > checkpoint {
                    self.set_checkpoint(subscription, handler.topic_name, key.as_deref(), head)
                        .await?;
                    skipped += non_negative(head - checkpoint);
                }
            }
            Ok(skipped)
        }
        .await;
        release_loop(&signal, guard);
        let skipped = result?;
        log_subscription_op(
            "skip_to_latest",
            registry_key,
            subscription,
            by,
            &json!({ "skipped": skipped }),
        );
        Ok(skipped)
    }

    /// Run a handler's waiting retry now instead of after its backoff.
    ///
    /// A no-op when nothing is waiting.
    ///
    /// # Errors
    ///
    /// [`PhotonError::SubscriptionNotFound`] for an unknown key.
    pub fn retry_now(&self, registry_key: &str, by: &str) -> Result<()> {
        let handler = handler_for(registry_key)?;
        self.signal(registry_key).retry_now.notify_waiters();
        log_subscription_op(
            "retry_now",
            registry_key,
            subscription_of(handler),
            by,
            &json!({}),
        );
        Ok(())
    }

    /// Deliver one stored event to one handler again, outside its stream.
    ///
    /// Runs the normal delivery path once (retries, DLQ and stats included). The checkpoint
    /// never moves backward, so redelivering an old event leaves it where it is.
    ///
    /// # Errors
    ///
    /// [`PhotonError::SubscriptionNotFound`], [`PhotonError::EventNotFound`] (unknown id, or
    /// an event on another topic), [`PhotonError::Internal`] when [`Self::start_executor`] has
    /// not run on this node, or the handler's error (sanitized) when the delivery failed and
    /// was dead-lettered.
    pub async fn redeliver(&self, event_id: &str, registry_key: &str, by: &str) -> Result<()> {
        let handler = handler_for(registry_key)?;
        let subscription = subscription_of(handler);
        let identity = self.runtime().executor.identity().ok_or_else(|| {
            PhotonError::Internal("redeliver needs start_executor on this node".into())
        })?;
        let event = self
            .get_event(event_id)
            .await?
            .filter(|e| e.topic_name == handler.topic_name)
            .ok_or_else(|| PhotonError::EventNotFound(event_id.to_string()))?;
        let seq = event.seq;
        let outcome = dispatch_event(
            identity,
            Arc::clone(&self.runtime().executor_services),
            handler,
            event,
            subscription.to_string(),
            RetryPolicy::from_env(),
        )
        .await;
        log_subscription_op(
            "redeliver",
            registry_key,
            subscription,
            by,
            &json!({ "event_id": event_id, "seq": seq, "ok": outcome.is_ok() }),
        );
        outcome.map_err(|e| PhotonError::Internal(sanitize_error_message(&e.to_string())))
    }

    fn signal(&self, registry_key: &str) -> Arc<HandlerSignal> {
        self.runtime().executor_services.signals.get(registry_key)
    }

    /// Checkpoint partitions of a handler: `None` for durable, every shard key for groups.
    pub(crate) fn partitions(&self, handler: &'static HandlerDescriptor) -> Vec<Option<String>> {
        if handler.is_consumer_group() {
            (0..shard_count_for_handler(self, handler))
                .map(|id| Some(shard_storage_key(id)))
                .collect()
        } else {
            vec![None]
        }
    }

    fn check_window(&self, at: DateTime<Utc>, requested: impl FnOnce() -> String) -> Result<()> {
        let Some(window) = self.backend_capabilities().max_replay_window else {
            return Ok(());
        };
        let oldest =
            Utc::now() - chrono::Duration::from_std(window).unwrap_or(chrono::TimeDelta::MAX);
        if at < oldest {
            return Err(PhotonError::ReplayOutOfWindow {
                requested: requested(),
                window_secs: window.as_secs(),
            });
        }
        Ok(())
    }

    /// Seq of the first event in a partition created at or after `at`, by paging the topic.
    async fn first_seq_at_or_after(
        &self,
        topic: &str,
        topic_key: Option<&str>,
        at: DateTime<Utc>,
    ) -> Result<Option<i64>> {
        let mut after = None;
        loop {
            let page = self
                .list_events_by_topic(topic, topic_key, after, REPLAY_PAGE)
                .await?;
            if let Some(found) = page.iter().find(|e| e.created_at >= at) {
                return Ok(Some(found.seq));
            }
            match page.last() {
                Some(last) if page.len() == REPLAY_PAGE => after = Some(last.seq),
                _ => return Ok(None),
            }
        }
    }

    async fn replay_plan(
        &self,
        handler: &'static HandlerDescriptor,
        from: &ReplayFrom,
    ) -> Result<Vec<PartitionPlan>> {
        let topic = handler.topic_name;
        let is_group = handler.is_consumer_group();
        // Resolve to a seq (durable) or a time (groups, and every time-based start).
        let (seq, time) = match from {
            ReplayFrom::Seq(_) if is_group => return Err(PhotonError::Unsupported(
                "replay from a seq on a consumer group; seqs are per shard, use a time or an event"
                    .into(),
            )),
            ReplayFrom::Seq(seq) => (Some((*seq).max(1)), None),
            ReplayFrom::Time(at) => {
                self.check_window(*at, || at.to_rfc3339())?;
                (None, Some(*at))
            }
            ReplayFrom::Event(event_id) => {
                let event = self
                    .get_event(event_id)
                    .await?
                    .filter(|e| e.topic_name == topic)
                    .ok_or_else(|| PhotonError::EventNotFound(event_id.clone()))?;
                self.check_window(event.created_at, || event_id.clone())?;
                if is_group {
                    (None, Some(event.created_at))
                } else {
                    (Some(event.seq), None)
                }
            }
        };
        if time.is_some() && !self.backend_capabilities().supports_list_events {
            return Err(PhotonError::Unsupported(
                "replay from a time needs a backend that lists events".into(),
            ));
        }

        let subscription = subscription_of(handler);
        let mut plans = Vec::new();
        for topic_key in self.partitions(handler) {
            let key = topic_key.as_deref();
            let head = self.head_seq(topic, key).await?.unwrap_or(0);
            let checkpoint = self
                .get_checkpoint_seq(subscription, topic, key)
                .await?
                .unwrap_or(0);
            let from_seq = match (seq, time) {
                (Some(seq), _) => seq,
                (None, Some(at)) => self
                    .first_seq_at_or_after(topic, key, at)
                    .await?
                    .unwrap_or(head + 1),
                (None, None) => head + 1,
            };
            plans.push(PartitionPlan {
                topic_key,
                from_seq,
                head,
                checkpoint,
            });
        }
        Ok(plans)
    }
}

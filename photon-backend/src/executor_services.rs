//! Shared runtime services for handler delivery (constructed at Photon build time).

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, PoisonError};

use chrono::{DateTime, Utc};
use tokio::sync::Notify;

use crate::checkpoint::CheckpointCoalescer;
use crate::delivery::{DlqSink, WorkerPool};
use crate::retention::{
    PartitionReclaim, RetentionDeps, RetentionHook, RetentionPolicy, RetentionReclaimer,
};
use crate::sanitize::sanitize_error_message;
use crate::storage::StoragePort;

/// Shared runtime services for handler delivery.
pub struct ExecutorServices {
    /// Bounded handler concurrency pool.
    pub worker_pool: Arc<WorkerPool>,
    /// Coalesced checkpoint writer.
    pub checkpoint_coalescer: Arc<CheckpointCoalescer>,
    /// Dead-letter sink for failed deliveries.
    pub dlq: Arc<DlqSink>,
    /// Storage retention reclaim coordinator.
    pub retention_reclaimer: Arc<RetentionReclaimer>,
    /// Per-handler delivery stats (in memory, this node only).
    pub stats: Arc<HandlerStats>,
    /// Per-handler control signals (retry now, pause/replay wake-ups).
    pub signals: Arc<SubscriptionSignals>,
}

impl ExecutorServices {
    /// Construct delivery services bound to the storage port.
    #[allow(clippy::needless_pass_by_value)] // Arc-by-value is the public ownership API
    pub fn new(
        port: Arc<dyn StoragePort>,
        policy: RetentionPolicy,
        hook: Option<Arc<dyn RetentionHook>>,
    ) -> Self {
        let coalescer = Arc::new(CheckpointCoalescer::new(Arc::clone(&port)));
        let dlq = Arc::new(DlqSink::new());
        let reclaimer = RetentionReclaimer::new(RetentionDeps {
            port: Arc::clone(&port),
            coalescer: Arc::clone(&coalescer),
            dlq: Arc::clone(&dlq),
            policy,
            hook,
        });
        coalescer.attach_reclaimer(Arc::clone(&reclaimer) as Arc<dyn PartitionReclaim>);
        Self {
            worker_pool: Arc::new(WorkerPool::from_env()),
            checkpoint_coalescer: coalescer,
            dlq,
            retention_reclaimer: reclaimer,
            stats: Arc::new(HandlerStats::default()),
            signals: Arc::new(SubscriptionSignals::default()),
        }
    }
}

/// Hours covered by [`HandlerHealth::failures_24h`] (one ring bucket each).
const FAILURE_WINDOW_HOURS: i64 = 24;

/// The event a handler is waiting to retry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetryState {
    /// Event being retried.
    pub event_id: String,
    /// Number of the attempt that runs next (the first delivery is attempt 1).
    pub attempt: u32,
    /// When the next attempt runs unless retried sooner.
    pub next_retry_at: DateTime<Utc>,
}

/// Point-in-time delivery health for one handler.
///
/// Counted in memory on this node since it started; other nodes report their own.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct HandlerHealth {
    /// When this node last handled an event successfully.
    pub last_handled_at: Option<DateTime<Utc>>,
    /// Sequence of that event.
    pub last_handled_seq: Option<i64>,
    /// Deliveries that ended in failure (dead-lettered) over the last 24 hours.
    pub failures_24h: u32,
    /// Sanitized, truncated text of the last failure.
    pub last_error: Option<String>,
    /// When the last failure happened.
    pub last_error_at: Option<DateTime<Utc>>,
    /// The event waiting on a retry backoff, if any.
    pub retrying: Option<RetryState>,
}

#[derive(Default)]
struct StatsEntry {
    health: HandlerHealth,
    /// Failure counts per hour, indexed by `hour % 24` and stamped with the hour they count.
    buckets: [(i64, u32); 24],
}

fn hour_slot(hour: i64) -> usize {
    usize::try_from(hour.rem_euclid(FAILURE_WINDOW_HOURS)).unwrap_or_default()
}

const fn hour_of(at: DateTime<Utc>) -> i64 {
    at.timestamp().div_euclid(3600)
}

/// Delivery stats per handler, keyed by the handler's `registry_key`.
///
/// In-process only: each node reports its own numbers since start, and nothing is persisted.
#[derive(Default)]
pub struct HandlerStats {
    entries: Mutex<HashMap<String, StatsEntry>>,
}

impl HandlerStats {
    fn with_entry(&self, registry_key: &str, f: impl FnOnce(&mut StatsEntry)) {
        let mut guard = self.entries.lock().unwrap_or_else(PoisonError::into_inner);
        f(guard.entry(registry_key.to_string()).or_default());
    }

    /// Record a successful delivery of `seq` at `at`.
    pub fn record_success(&self, registry_key: &str, seq: i64, at: DateTime<Utc>) {
        self.with_entry(registry_key, |e| {
            e.health.last_handled_at = Some(at);
            e.health.last_handled_seq = Some(seq);
            e.health.retrying = None;
        });
    }

    /// Record a delivery that ended in failure; `error` is sanitized and truncated like the DLQ.
    pub fn record_failure(&self, registry_key: &str, error: &str, at: DateTime<Utc>) {
        let hour = hour_of(at);
        self.with_entry(registry_key, |e| {
            let bucket = &mut e.buckets[hour_slot(hour)];
            if bucket.0 != hour {
                *bucket = (hour, 0);
            }
            bucket.1 = bucket.1.saturating_add(1);
            e.health.last_error = Some(sanitize_error_message(error));
            e.health.last_error_at = Some(at);
            e.health.retrying = None;
        });
    }

    /// Record that an event is waiting for its next attempt.
    pub fn record_retry(&self, registry_key: &str, retry: RetryState) {
        self.with_entry(registry_key, |e| e.health.retrying = Some(retry));
    }

    /// Health for one handler as of `now`; all-empty when it has not delivered anything yet.
    #[must_use]
    pub fn snapshot(&self, registry_key: &str, now: DateTime<Utc>) -> HandlerHealth {
        let guard = self.entries.lock().unwrap_or_else(PoisonError::into_inner);
        let Some(entry) = guard.get(registry_key) else {
            return HandlerHealth::default();
        };
        let now_hour = hour_of(now);
        let failures_24h = entry
            .buckets
            .iter()
            .filter(|(hour, _)| (0..FAILURE_WINDOW_HOURS).contains(&(now_hour - hour)))
            .map(|(_, n)| *n)
            .sum();
        HandlerHealth {
            failures_24h,
            ..entry.health.clone()
        }
    }
}

/// Wake-ups shared between `Photon` operations and one handler's subscription loop.
#[derive(Default)]
pub struct HandlerSignal {
    /// Cancels a handler's current retry backoff.
    pub retry_now: Notify,
    /// Tells the loop its pause state or checkpoint changed.
    pub wake: Notify,
    /// Held by the loop while it consumes a stream; operations take it to park the loop.
    pub run_lock: tokio::sync::Mutex<()>,
    hold: AtomicBool,
    running: AtomicBool,
}

impl HandlerSignal {
    /// Whether an operation asked the loop to stay parked.
    pub fn held(&self) -> bool {
        self.hold.load(Ordering::SeqCst)
    }

    /// Ask the loop to park (`true`) or let it go (`false`).
    pub fn set_held(&self, held: bool) {
        self.hold.store(held, Ordering::SeqCst);
    }

    /// Whether a subscription loop for this handler runs on this node.
    pub fn running(&self) -> bool {
        self.running.load(Ordering::SeqCst)
    }

    /// Mark the handler's loop as started or stopped.
    pub fn set_running(&self, running: bool) {
        self.running.store(running, Ordering::SeqCst);
    }
}

/// [`HandlerSignal`]s keyed by handler `registry_key`, created on first use.
#[derive(Default)]
pub struct SubscriptionSignals {
    map: Mutex<HashMap<String, Arc<HandlerSignal>>>,
}

impl SubscriptionSignals {
    /// The signal for `registry_key`.
    pub fn get(&self, registry_key: &str) -> Arc<HandlerSignal> {
        let mut guard = self.map.lock().unwrap_or_else(PoisonError::into_inner);
        Arc::clone(guard.entry(registry_key.to_string()).or_default())
    }
}

#[cfg(test)]
mod tests {
    use chrono::{Duration, TimeZone};

    use super::*;

    fn t0() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 11, 24, 10, 30, 0).unwrap()
    }

    #[test]
    fn failures_count_within_24_hours_and_expire_after() {
        let stats = HandlerStats::default();
        stats.record_failure("k", "boom", t0());
        stats.record_failure("k", "boom", t0() + Duration::minutes(10));
        stats.record_failure("k", "later", t0() + Duration::hours(5));
        let health = stats.snapshot("k", t0() + Duration::hours(6));
        assert_eq!(health.failures_24h, 3);
        assert_eq!(health.last_error.as_deref(), Some("later"));
        assert_eq!(health.last_error_at, Some(t0() + Duration::hours(5)));

        let later = stats.snapshot("k", t0() + Duration::hours(24));
        assert_eq!(
            later.failures_24h, 1,
            "the first two fall out of the window"
        );
        assert_eq!(
            stats.snapshot("k", t0() + Duration::hours(40)).failures_24h,
            0
        );
    }

    #[test]
    fn ring_slot_reuse_resets_old_counts() {
        let stats = HandlerStats::default();
        stats.record_failure("k", "a", t0());
        stats.record_failure("k", "b", t0() + Duration::hours(24));
        assert_eq!(
            stats.snapshot("k", t0() + Duration::hours(24)).failures_24h,
            1
        );
    }

    #[test]
    fn success_and_retry_are_tracked_and_errors_truncated() {
        let stats = HandlerStats::default();
        assert_eq!(stats.snapshot("k", t0()), HandlerHealth::default());
        stats.record_retry(
            "k",
            RetryState {
                event_id: "e1".into(),
                attempt: 2,
                next_retry_at: t0(),
            },
        );
        assert_eq!(
            stats.snapshot("k", t0()).retrying.map(|r| r.attempt),
            Some(2)
        );
        stats.record_success("k", 7, t0());
        let h = stats.snapshot("k", t0());
        assert_eq!(h.last_handled_seq, Some(7));
        assert_eq!(h.last_handled_at, Some(t0()));
        assert!(h.retrying.is_none());

        stats.record_failure("k", &"x".repeat(800), t0());
        let err = stats.snapshot("k", t0()).last_error.unwrap();
        assert!(err.ends_with('…'));
    }

    #[test]
    fn signals_are_shared_per_key() {
        let signals = SubscriptionSignals::default();
        let a = signals.get("k");
        a.set_held(true);
        assert!(signals.get("k").held());
        assert!(!signals.get("other").held());
    }
}

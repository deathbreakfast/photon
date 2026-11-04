//! Integration test: opt-in handler retry before the DLQ.
//!
//! Retry settings are read when the executor starts, so this binary sets them once and holds
//! both scenarios in one test (the registry is process-wide).

#![allow(missing_docs)]
#![allow(clippy::unused_async)]

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use photon::{configure, subscribe, topic, JsonIdentityFactory, Photon};
use photon_core::Actor;

static FLAKY_CALLS: AtomicUsize = AtomicUsize::new(0);
static BROKEN_CALLS: AtomicUsize = AtomicUsize::new(0);

#[topic(name = "test.retry.flaky")]
pub struct FlakyEvent {
    pub value: u32,
}

#[topic(name = "test.retry.broken")]
pub struct BrokenEvent {
    pub value: u32,
}

#[subscribe(topic = "test.retry.flaky", durable = "flaky-handler")]
async fn on_flaky(_actor: Box<dyn Actor>, _event: FlakyEvent) -> photon::Result<()> {
    if FLAKY_CALLS.fetch_add(1, Ordering::SeqCst) < 2 {
        return Err(photon::PhotonError::Internal("transient".into()));
    }
    Ok(())
}

#[subscribe(topic = "test.retry.broken", durable = "broken-handler")]
async fn on_broken(_actor: Box<dyn Actor>, _event: BrokenEvent) -> photon::Result<()> {
    BROKEN_CALLS.fetch_add(1, Ordering::SeqCst);
    Err(photon::PhotonError::Internal("permanent".into()))
}

#[tokio::test]
async fn transient_failures_retry_and_permanent_failures_dead_letter_after_budget() {
    std::env::set_var("PHOTON_HANDLER_MAX_RETRIES", "2");
    std::env::set_var("PHOTON_HANDLER_RETRY_BACKOFF_MS", "5");

    let photon = Photon::builder()
        .auto_registry()
        .build()
        .expect("build photon");
    photon
        .start_executor(Arc::new(JsonIdentityFactory))
        .expect("start executor");
    configure(photon.clone());
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    FlakyEvent { value: 1 }.publish().await.expect("publish");
    BrokenEvent { value: 1 }.publish().await.expect("publish");

    // Poll until the counters settle instead of sleeping a fixed time.
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
    while tokio::time::Instant::now() < deadline {
        if FLAKY_CALLS.load(Ordering::SeqCst) >= 3
            && BROKEN_CALLS.load(Ordering::SeqCst) >= 3
            && !photon.runtime().executor_services.dlq.is_empty()
        {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    // Grace period so an unwanted extra call or DLQ row would show up in the asserts below.
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    // Two failures then success: three calls, nothing dead-lettered for this handler.
    assert_eq!(FLAKY_CALLS.load(Ordering::SeqCst), 3);
    // First attempt plus two retries, then exactly one DLQ row.
    assert_eq!(BROKEN_CALLS.load(Ordering::SeqCst), 3);
    assert_eq!(photon.runtime().executor_services.dlq.len(), 1);
}

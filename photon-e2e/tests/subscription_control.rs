//! Integration test: pause/resume, replay, skip and redeliver on the mem and sqlite backends.
//!
//! The handler registry is process-wide, so each backend runs every scenario inside one test
//! and shuts its executor down before the next backend starts.

#![allow(missing_docs)]
#![allow(clippy::unused_async)]

use std::sync::{Arc, Mutex};
use std::time::Duration;

use photon::{subscribe, topic, JsonIdentityFactory, Photon, ReplayFrom};
use photon_core::Actor;
use photon_testkit::{BootstrapSession, MatrixSpec};

static PAUSE_SEEN: Mutex<Vec<i64>> = Mutex::new(Vec::new());
static REPLAY_SEEN: Mutex<Vec<i64>> = Mutex::new(Vec::new());
static SKIP_SEEN: Mutex<Vec<i64>> = Mutex::new(Vec::new());
static REDELIVER_SEEN: Mutex<Vec<i64>> = Mutex::new(Vec::new());

const PAUSE_KEY: &str = "test.ctl.pause:ctl-pause";
const REPLAY_KEY: &str = "test.ctl.replay:ctl-replay";
const SKIP_KEY: &str = "test.ctl.skip:ctl-skip";
const REDELIVER_KEY: &str = "test.ctl.redeliver:ctl-redeliver";

#[topic(name = "test.ctl.pause")]
pub struct PauseEvent {
    pub n: u32,
}

#[topic(name = "test.ctl.replay")]
pub struct ReplayEvent {
    pub n: u32,
}

#[topic(name = "test.ctl.skip")]
pub struct SkipEvent {
    pub n: u32,
}

#[topic(name = "test.ctl.redeliver")]
pub struct RedeliverEvent {
    pub n: u32,
}

#[subscribe(topic = "test.ctl.pause", durable = "ctl-pause")]
async fn on_pause(_actor: Box<dyn Actor>, event: PauseEvent) -> photon::Result<()> {
    PAUSE_SEEN.lock().unwrap().push(i64::from(event.n));
    Ok(())
}

#[subscribe(topic = "test.ctl.replay", durable = "ctl-replay")]
async fn on_replay(_actor: Box<dyn Actor>, event: ReplayEvent) -> photon::Result<()> {
    REPLAY_SEEN.lock().unwrap().push(i64::from(event.n));
    Ok(())
}

#[subscribe(topic = "test.ctl.skip", durable = "ctl-skip")]
async fn on_skip(_actor: Box<dyn Actor>, event: SkipEvent) -> photon::Result<()> {
    SKIP_SEEN.lock().unwrap().push(i64::from(event.n));
    Ok(())
}

#[subscribe(topic = "test.ctl.redeliver", durable = "ctl-redeliver")]
async fn on_redeliver(_actor: Box<dyn Actor>, event: RedeliverEvent) -> photon::Result<()> {
    REDELIVER_SEEN.lock().unwrap().push(i64::from(event.n));
    Ok(())
}

fn seen(log: &Mutex<Vec<i64>>) -> Vec<i64> {
    log.lock().unwrap().clone()
}

/// `log[from..]` sorted: the executor runs deliveries concurrently, so a batch may finish in
/// any order.
fn sorted_tail(log: &[i64], from: usize) -> Vec<i64> {
    let mut tail = log[from..].to_vec();
    tail.sort_unstable();
    tail
}

/// Poll until `log` holds `len` entries (or the deadline passes), then return it.
async fn wait_len(log: &Mutex<Vec<i64>>, len: usize, within: Duration) -> Vec<i64> {
    let deadline = tokio::time::Instant::now() + within;
    while tokio::time::Instant::now() < deadline && log.lock().unwrap().len() < len {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    // Grace period so an unwanted extra delivery shows up in the caller's assert.
    tokio::time::sleep(Duration::from_millis(150)).await;
    seen(log)
}

async fn publish(photon: &Photon, topic: &str, n: u32) -> String {
    photon
        .publish(
            topic,
            None,
            serde_json::json!({"label": "test"}),
            serde_json::json!({ "n": n }),
        )
        .await
        .expect("publish")
}

fn reset_logs() {
    for log in [&PAUSE_SEEN, &REPLAY_SEEN, &SKIP_SEEN, &REDELIVER_SEEN] {
        log.lock().unwrap().clear();
    }
}

async fn run_all(session: &BootstrapSession) {
    reset_logs();
    let photon = session.build_photon().expect("build photon");
    // A second handle on the same store stands in for another node: its pause reaches this
    // node only through the backend, within the 5 s control refresh.
    let other_node = session.build_photon().expect("build second photon");
    photon
        .start_executor(Arc::new(JsonIdentityFactory))
        .expect("start executor");
    tokio::time::sleep(Duration::from_millis(50)).await;

    pause_and_resume(&photon, &other_node).await;
    replay_from_seq_time_and_event(&photon).await;
    remote_replay(&photon, &other_node).await;
    skip_to_latest(&photon).await;
    redeliver_one(&photon).await;

    photon.shutdown_executor();
    photon.join_executor().await;
}

async fn pause_and_resume(photon: &Photon, other_node: &Photon) {
    let snapshot = photon.admin_snapshot().await.expect("snapshot");
    assert!(snapshot.backend.supports_subscription_control);

    publish(photon, "test.ctl.pause", 1).await;
    assert_eq!(
        wait_len(&PAUSE_SEEN, 1, Duration::from_secs(5)).await,
        vec![1]
    );

    // Local pause stops dispatch at once.
    photon.pause(PAUSE_KEY, "Kenji").await.expect("pause");
    publish(photon, "test.ctl.pause", 2).await;
    assert_eq!(
        wait_len(&PAUSE_SEEN, 2, Duration::from_millis(500)).await,
        vec![1]
    );
    let snapshot = photon.admin_snapshot().await.expect("snapshot");
    let handler = snapshot
        .handlers
        .iter()
        .find(|h| h.registry_key == PAUSE_KEY)
        .expect("pause handler");
    assert_eq!(
        handler.health.paused.as_ref().map(|p| p.by.as_str()),
        Some("Kenji")
    );

    // Resume restarts from the checkpoint and delivers what waited.
    photon.resume(PAUSE_KEY, "Kenji").await.expect("resume");
    assert_eq!(
        wait_len(&PAUSE_SEEN, 2, Duration::from_secs(5)).await,
        vec![1, 2]
    );

    // A pause stored by another node stops dispatch here within the 5 s refresh.
    other_node
        .pause(PAUSE_KEY, "Ines")
        .await
        .expect("remote pause");
    tokio::time::sleep(Duration::from_millis(5200)).await;
    publish(photon, "test.ctl.pause", 3).await;
    assert_eq!(
        wait_len(&PAUSE_SEEN, 3, Duration::from_millis(700)).await,
        vec![1, 2]
    );
    photon.resume(PAUSE_KEY, "Kenji").await.expect("resume");
    assert_eq!(
        wait_len(&PAUSE_SEEN, 3, Duration::from_secs(5)).await,
        vec![1, 2, 3]
    );
}

async fn replay_from_seq_time_and_event(photon: &Photon) {
    let mut ids = Vec::new();
    for n in 1..=5 {
        ids.push(publish(photon, "test.ctl.replay", n).await);
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let first = wait_len(&REPLAY_SEEN, 5, Duration::from_secs(5)).await;
    assert_eq!(sorted_tail(&first, 0), [1, 2, 3, 4, 5]);

    let from = ReplayFrom::Seq(3);
    let preview = photon
        .replay_preview(REPLAY_KEY, &from)
        .await
        .expect("preview seq");
    assert_eq!(preview.from_seq, 3);
    assert_eq!(preview.count, 3);
    assert_eq!(preview.already_handled, 3);
    let done = photon
        .replay(REPLAY_KEY, &from, "Kenji")
        .await
        .expect("replay seq");
    assert_eq!(done, preview);
    let got = wait_len(&REPLAY_SEEN, 8, Duration::from_secs(5)).await;
    assert_eq!(sorted_tail(&got, 5), [3, 4, 5]);

    let event4 = photon
        .get_event(&ids[3])
        .await
        .expect("get")
        .expect("event 4");
    let from = ReplayFrom::Time(event4.created_at);
    let preview = photon
        .replay_preview(REPLAY_KEY, &from)
        .await
        .expect("preview time");
    assert_eq!((preview.from_seq, preview.count), (4, 2));
    photon
        .replay(REPLAY_KEY, &from, "Kenji")
        .await
        .expect("replay time");
    let got = wait_len(&REPLAY_SEEN, 10, Duration::from_secs(5)).await;
    assert_eq!(sorted_tail(&got, 8), [4, 5]);

    let from = ReplayFrom::Event(ids[1].clone());
    let preview = photon
        .replay_preview(REPLAY_KEY, &from)
        .await
        .expect("preview event");
    assert_eq!((preview.from_seq, preview.count), (2, 4));
    photon
        .replay(REPLAY_KEY, &from, "Kenji")
        .await
        .expect("replay event");
    let got = wait_len(&REPLAY_SEEN, 14, Duration::from_secs(5)).await;
    assert_eq!(sorted_tail(&got, 10), [2, 3, 4, 5]);

    let missing = photon
        .replay_preview(REPLAY_KEY, &ReplayFrom::Event("no-such-event".into()))
        .await;
    assert!(matches!(
        missing,
        Err(photon::PhotonError::EventNotFound(_))
    ));
}

/// A replay run on another node reaches this node's idle loop through the stored rewind
/// generation: it resubscribes from the rewound checkpoint within the 5 s control refresh,
/// with no new publish to wake it.
async fn remote_replay(photon: &Photon, other_node: &Photon) {
    let before = seen(&REPLAY_SEEN).len();
    let preview = other_node
        .replay(REPLAY_KEY, &ReplayFrom::Seq(4), "Ines")
        .await
        .expect("remote replay");
    assert_eq!((preview.from_seq, preview.count), (4, 2));
    let got = wait_len(&REPLAY_SEEN, before + 2, Duration::from_secs(8)).await;
    assert_eq!(sorted_tail(&got, before), [4, 5]);

    // Once it has caught up again, this node's commits bring the checkpoint back to the head.
    let head = photon
        .head_seq("test.ctl.replay", None)
        .await
        .expect("head");
    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    loop {
        let checkpoint = photon
            .get_checkpoint_seq("ctl-replay", "test.ctl.replay", None)
            .await
            .expect("checkpoint");
        if checkpoint == head {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "checkpoint {checkpoint:?} never returned to head {head:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn skip_to_latest(photon: &Photon) {
    photon.pause(SKIP_KEY, "Ada").await.expect("pause");
    for n in 1..=3 {
        publish(photon, "test.ctl.skip", n).await;
    }
    let skipped = photon.skip_to_latest(SKIP_KEY, "Ada").await.expect("skip");
    assert_eq!(skipped, 3);
    let checkpoint = photon
        .get_checkpoint_seq("ctl-skip", "test.ctl.skip", None)
        .await
        .expect("checkpoint");
    let head = photon.head_seq("test.ctl.skip", None).await.expect("head");
    assert_eq!(checkpoint, head);
    photon.resume(SKIP_KEY, "Ada").await.expect("resume");
    let after_skip = wait_len(&SKIP_SEEN, 1, Duration::from_millis(700)).await;
    assert_eq!(after_skip, Vec::<i64>::new());
}

async fn redeliver_one(photon: &Photon) {
    let id = publish(photon, "test.ctl.redeliver", 9).await;
    assert_eq!(
        wait_len(&REDELIVER_SEEN, 1, Duration::from_secs(5)).await,
        vec![9]
    );
    photon
        .redeliver(&id, REDELIVER_KEY, "Ada")
        .await
        .expect("redeliver");
    assert_eq!(
        wait_len(&REDELIVER_SEEN, 2, Duration::from_secs(2)).await,
        vec![9, 9]
    );
    let missing = photon
        .redeliver("no-such-event", REDELIVER_KEY, "Ada")
        .await;
    assert!(matches!(
        missing,
        Err(photon::PhotonError::EventNotFound(_))
    ));
    let unknown = photon.redeliver(&id, "no.such:handler", "Ada").await;
    assert!(matches!(
        unknown,
        Err(photon::PhotonError::SubscriptionNotFound(_))
    ));
}

#[tokio::test(flavor = "multi_thread")]
async fn subscription_control_on_mem() {
    let mut session = BootstrapSession::new(MatrixSpec::ci_mem_embedded());
    session.install().expect("mem bootstrap");
    run_all(&session).await;
}

#[cfg(feature = "sqlite")]
#[tokio::test(flavor = "multi_thread")]
async fn subscription_control_on_sqlite() {
    if std::env::var("PHOTON_TRANSPORT_KEY").is_err() {
        std::env::set_var(
            "PHOTON_TRANSPORT_KEY",
            "cGhvdG9uLWRldi10cmFuc3BvcnQta2V5LTMyYnl0ZXM=",
        );
    }
    let path = std::env::temp_dir().join(format!(
        "photon-subscription-control-{}.db",
        std::process::id()
    ));
    let _ = std::fs::remove_file(&path);
    std::env::set_var("PHOTON_SQLITE_PATH", &path);
    let mut session = BootstrapSession::new(MatrixSpec::ci_sqlite_embedded());
    session.install_async().await.expect("sqlite bootstrap");
    run_all(&session).await;
    let _ = std::fs::remove_file(&path);
}

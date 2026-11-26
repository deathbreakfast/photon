//! `SQLite` storage port contract tests (no external broker).

#![allow(clippy::unwrap_used, clippy::expect_used)]
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures::StreamExt;
use photon_backend::{ListEventsQuery, StoragePort};
use photon_backend_sqlite::SqliteStoragePort;
use sqlx::Row;
use tempfile::NamedTempFile;
use tokio::task::JoinSet;

fn ensure_dev_transport_key() {
    if std::env::var("PHOTON_TRANSPORT_KEY").is_err() {
        std::env::set_var(
            "PHOTON_TRANSPORT_KEY",
            "cGhvdG9uLWRldi10cmFuc3BvcnQta2V5LTMyYnl0ZXM=",
        );
    }
}

async fn open_temp_port() -> (SqliteStoragePort, NamedTempFile) {
    ensure_dev_transport_key();
    let file = NamedTempFile::new().expect("temp db");
    let path = file.path().to_string_lossy().into_owned();
    let port = SqliteStoragePort::open(&path).await.expect("open sqlite");
    (port, file)
}

#[tokio::test]
async fn sqlite_append_subscribe_checkpoint_roundtrip() {
    let (port, _file) = open_temp_port().await;
    let topic = format!("testkit.contract.{}", uuid::Uuid::new_v4());
    let actor = serde_json::json!({"test": "actor"});
    let payload = serde_json::json!({"contract": true});

    let published = port
        .append(&topic, None, actor, payload)
        .await
        .expect("append");
    assert!(published.seq > 0);

    let mut stream = port.subscribe(topic.clone(), None, Some(0));
    let received = tokio::time::timeout(Duration::from_secs(5), stream.next())
        .await
        .expect("subscribe timeout")
        .expect("stream ended")
        .expect("event");

    assert_eq!(received.event_id, published.event_id);

    port.commit_checkpoint("sub-a", &topic, None, published.seq)
        .await
        .expect("commit");
    let loaded = port
        .load_checkpoint("sub-a", &topic, None)
        .await
        .expect("load");
    assert_eq!(loaded, Some(published.seq));
}

#[tokio::test]
async fn sqlite_head_seq_tracks_appended_events() {
    let (port, _file) = open_temp_port().await;
    let topic = format!("testkit.contract.{}", uuid::Uuid::new_v4());

    let before = port.head_seq(&topic, None).await.expect("read head seq");
    assert_eq!(before, None);

    for _ in 0..3 {
        port.append(&topic, None, serde_json::json!({}), serde_json::json!({}))
            .await
            .expect("append");
    }

    let after = port.head_seq(&topic, None).await.expect("read head seq");
    assert_eq!(after, Some(3));
}

#[tokio::test]
async fn sqlite_get_event_and_keyed_filter() {
    let (port, _file) = open_temp_port().await;
    let topic = format!("testkit.keyed.{}", uuid::Uuid::new_v4());
    let key = "shard-a";

    let published = port
        .append(
            &topic,
            Some(key),
            serde_json::json!({}),
            serde_json::json!({"k": key}),
        )
        .await
        .expect("append");

    let fetched = port
        .get_event(&published.event_id)
        .await
        .expect("get_event")
        .expect("found");
    assert_eq!(fetched.seq, published.seq);

    let mut stream = port.subscribe(topic.clone(), Some(key.to_string()), Some(0));
    let received = tokio::time::timeout(Duration::from_secs(5), stream.next())
        .await
        .expect("timeout")
        .expect("stream")
        .expect("event");
    assert_eq!(received.topic_key.as_deref(), Some(key));
}

#[tokio::test]
async fn sqlite_persists_ciphertext_and_returns_plaintext() {
    ensure_dev_transport_key();
    let file = NamedTempFile::new().expect("temp db");
    let path = file.path().to_string_lossy().into_owned();
    let port = SqliteStoragePort::open(&path).await.expect("open sqlite");
    let marker = "SECRET_PLAINTEXT_MARKER_xyz";
    let published = port
        .append(
            "testkit.ciphertext",
            None,
            serde_json::json!({"actor": "test"}),
            serde_json::json!({"secret": marker}),
        )
        .await
        .expect("append");

    let pool = sqlx::SqlitePool::connect(&format!("sqlite://{path}"))
        .await
        .expect("open raw sqlite");
    let row = sqlx::query("SELECT payload_json FROM events WHERE event_id = ?")
        .bind(&published.event_id)
        .fetch_one(&pool)
        .await
        .expect("stored row");
    let stored_payload: String = row.get("payload_json");
    assert!(!stored_payload.contains(marker));

    let fetched = port
        .get_event(&published.event_id)
        .await
        .expect("get event")
        .expect("event");
    assert_eq!(fetched.payload_json, published.payload_json);

    let mut stream = port.subscribe("testkit.ciphertext".into(), None, Some(0));
    let received = tokio::time::timeout(Duration::from_secs(5), stream.next())
        .await
        .expect("subscribe timeout")
        .expect("stream ended")
        .expect("event");
    assert_eq!(received.payload_json, published.payload_json);
}

#[tokio::test]
async fn sqlite_replay_survives_reopen() {
    ensure_dev_transport_key();
    let file = NamedTempFile::new().expect("temp db");
    let path = file.path().to_string_lossy().into_owned();
    let topic = format!("testkit.reopen.{}", uuid::Uuid::new_v4());

    let published = {
        let port = SqliteStoragePort::open(&path).await.expect("open");
        port.append(
            &topic,
            None,
            serde_json::json!({}),
            serde_json::json!({"persist": true}),
        )
        .await
        .expect("append")
    };

    let reopened = SqliteStoragePort::open(&path).await.expect("reopen");
    let mut stream = reopened.subscribe(topic, None, Some(0));
    let received = tokio::time::timeout(Duration::from_secs(5), stream.next())
        .await
        .expect("timeout")
        .expect("stream")
        .expect("event");
    assert_eq!(received.event_id, published.event_id);
}

#[tokio::test]
async fn sqlite_list_by_topic_and_list_recent_honor_limit() {
    let (port, _file) = open_temp_port().await;
    let topic = format!("testkit.list.{}", uuid::Uuid::new_v4());
    let mut ids = Vec::new();
    for i in 0..4 {
        let ev = port
            .append(
                &topic,
                None,
                serde_json::json!({}),
                serde_json::json!({"n": i}),
            )
            .await
            .expect("append");
        ids.push(ev.event_id);
    }
    let _other = port
        .append(
            &format!("testkit.other.{}", uuid::Uuid::new_v4()),
            None,
            serde_json::json!({}),
            serde_json::json!({"n": 99}),
        )
        .await
        .expect("append other");

    let page = port
        .list_by_topic(&topic, None, None, 2)
        .await
        .expect("list_by_topic");
    assert_eq!(page.len(), 2);
    assert_eq!(page[0].event_id, ids[0]);
    assert_eq!(page[1].event_id, ids[1]);
    assert!(page.windows(2).all(|w| w[0].seq < w[1].seq));

    let after = port
        .list_by_topic(&topic, None, Some(1), 10)
        .await
        .expect("after_seq");
    assert!(after.iter().all(|e| e.seq > 1));
    assert_eq!(after.len(), 3);

    let recent = port.list_recent(2).await.expect("list_recent");
    assert_eq!(recent.len(), 2);
    assert!(recent[0].created_at >= recent[1].created_at);

    assert!(port
        .list_by_topic(&topic, None, None, 0)
        .await
        .expect("zero")
        .is_empty());
}

#[tokio::test]
async fn sqlite_file_uses_wal_journal() {
    let (port, file) = open_temp_port().await;
    drop(port);
    let path = file.path().to_string_lossy().into_owned();
    let pool = sqlx::SqlitePool::connect(&format!("sqlite://{path}"))
        .await
        .expect("reopen");
    let row = sqlx::query("PRAGMA journal_mode")
        .fetch_one(&pool)
        .await
        .expect("pragma");
    let mode: String = row.get(0);
    assert_eq!(mode.to_ascii_lowercase(), "wal");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sqlite_sustained_checkpoint_commits_at_pd2_floor() {
    let (port, _file) = open_temp_port().await;
    let topic = format!("testkit.wal-chk.{}", uuid::Uuid::new_v4());
    let port = Arc::new(port);
    let start = Instant::now();
    let duration = Duration::from_secs(30);
    let mut joins = JoinSet::new();
    for sub in 0..4u32 {
        let port = Arc::clone(&port);
        let topic = topic.clone();
        joins.spawn(async move {
            let mut seq = 0i64;
            while start.elapsed() < duration {
                seq += 1;
                port.commit_checkpoint(&format!("sub-{sub}"), &topic, None, seq)
                    .await
                    .expect("checkpoint");
                tokio::time::sleep(Duration::from_millis(15)).await;
            }
            seq
        });
    }
    let mut total = 0i64;
    while let Some(res) = joins.join_next().await {
        total += res.expect("join");
    }
    assert!(
        total >= 1_400,
        "expected ~50 checkpoints/s for 30s, got {total}"
    );
    let loaded = port
        .load_checkpoint("sub-0", &topic, None)
        .await
        .expect("load");
    assert!(loaded.is_some_and(|seq| seq > 0));
}

#[tokio::test]
async fn sqlite_open_rejects_directory_path() {
    let dir = tempfile::tempdir().expect("tempdir");
    let result = SqliteStoragePort::open(dir.path().to_str().expect("utf8")).await;
    assert!(result.is_err(), "directory is not a sqlite file");
}

async fn append_n(port: &SqliteStoragePort, topic: &str, key: Option<&str>, n: usize) {
    for i in 0..n {
        port.append(
            topic,
            key,
            serde_json::json!({}),
            serde_json::json!({"n": i}),
        )
        .await
        .expect("append");
    }
}

/// Pin every stored event of `topic` to `base + seq` seconds, so ranges need no clock gaps.
///
/// Writes `created_at` in the same `to_rfc3339()` text form the port uses.
async fn stamp(file: &NamedTempFile, topic: &str, base: chrono::DateTime<chrono::Utc>) {
    let path = file.path().to_string_lossy().into_owned();
    let pool = sqlx::SqlitePool::connect(&format!("sqlite://{path}"))
        .await
        .expect("open raw sqlite");
    let rows = sqlx::query("SELECT event_id, seq FROM events WHERE topic_name = ?")
        .bind(topic)
        .fetch_all(&pool)
        .await
        .expect("select events");
    for row in rows {
        let event_id: String = row.get("event_id");
        let seq: i64 = row.get("seq");
        sqlx::query("UPDATE events SET created_at = ? WHERE event_id = ?")
            .bind((base + chrono::Duration::seconds(seq)).to_rfc3339())
            .bind(event_id)
            .execute(&pool)
            .await
            .expect("stamp created_at");
    }
    pool.close().await;
}

async fn seqs(port: &SqliteStoragePort, query: &ListEventsQuery) -> Vec<i64> {
    port.list_events(query)
        .await
        .expect("list_events")
        .iter()
        .map(|e| e.seq)
        .collect()
}

#[tokio::test]
async fn sqlite_count_events_counts_a_day_by_topic_key_and_since() {
    let (port, file) = open_temp_port().await;
    assert!(port.capabilities().supports_count_events);
    append_n(&port, "test.count", None, 412).await;
    append_n(&port, "test.count.keyed", Some("alice"), 3).await;
    append_n(&port, "test.count.keyed", Some("bob"), 2).await;

    let day_ago = chrono::Utc::now() - chrono::Duration::hours(24);
    let count = |topic, key, since| port.count_events(topic, key, since);
    assert_eq!(
        count(Some("test.count"), None, Some(day_ago))
            .await
            .unwrap(),
        Some(412)
    );
    assert_eq!(
        count(Some("test.count.keyed"), Some("alice"), None)
            .await
            .unwrap(),
        Some(3)
    );
    assert_eq!(count(None, None, Some(day_ago)).await.unwrap(), Some(417));

    let hour_ago = chrono::Utc::now() - chrono::Duration::hours(1);
    for topic in ["test.count", "test.count.keyed"] {
        stamp(&file, topic, hour_ago).await;
    }
    let later = hour_ago + chrono::Duration::minutes(30);
    assert_eq!(count(None, None, Some(later)).await.unwrap(), Some(0));
    append_n(&port, "test.count", None, 1).await;
    assert_eq!(
        count(Some("test.count"), None, Some(later)).await.unwrap(),
        Some(1)
    );
}

#[tokio::test]
async fn sqlite_list_events_returns_a_time_range_newest_first() {
    let (port, file) = open_temp_port().await;
    let topic = "test.range";
    append_n(&port, topic, None, 9).await;
    append_n(&port, "test.range.other", None, 1).await;
    let base = chrono::Utc::now() - chrono::Duration::hours(1);
    stamp(&file, topic, base).await;
    let start = base + chrono::Duration::seconds(4);
    let end = base + chrono::Duration::seconds(8);

    let query = ListEventsQuery {
        topic: Some(topic.into()),
        since: Some(start),
        until: Some(end),
        newest_first: true,
        limit: 100,
        ..ListEventsQuery::default()
    };
    assert_eq!(seqs(&port, &query).await, vec![7, 6, 5, 4]);

    let oldest_first = ListEventsQuery {
        newest_first: false,
        ..query.clone()
    };
    assert_eq!(seqs(&port, &oldest_first).await, vec![4, 5, 6, 7]);

    let cross_topic = port
        .list_events(&ListEventsQuery {
            topic: None,
            ..query
        })
        .await
        .expect("cross-topic");
    assert_eq!(cross_topic.len(), 4);
    assert!(cross_topic
        .windows(2)
        .all(|w| w[0].created_at >= w[1].created_at));
}

#[tokio::test]
async fn sqlite_list_events_pages_with_seq_cursors_both_ways() {
    let (port, _file) = open_temp_port().await;
    let topic = "test.paging";
    append_n(&port, topic, None, 10).await;
    let page = |after_seq, before_seq| ListEventsQuery {
        topic: Some(topic.into()),
        after_seq,
        before_seq,
        newest_first: true,
        limit: 3,
        ..ListEventsQuery::default()
    };

    assert_eq!(seqs(&port, &page(None, None)).await, vec![10, 9, 8]);
    assert_eq!(seqs(&port, &page(None, Some(8))).await, vec![7, 6, 5]);
    assert_eq!(seqs(&port, &page(Some(7), None)).await, vec![10, 9, 8]);
    assert_eq!(seqs(&port, &page(Some(4), None)).await, vec![7, 6, 5]);
    assert_eq!(seqs(&port, &page(Some(2), Some(5))).await, vec![4, 3]);
}

#[tokio::test]
async fn sqlite_rewind_generation_survives_pause_and_reopen() {
    let (port, file) = open_temp_port().await;
    assert_eq!(port.bump_rewind_generation("s").await.unwrap(), 1);
    assert_eq!(port.bump_rewind_generation("s").await.unwrap(), 2);
    port.set_subscription_pause(
        "s",
        Some(photon_backend::SubscriptionPause {
            by: "Ines".into(),
            at: chrono::Utc::now(),
        }),
    )
    .await
    .unwrap();
    let control = port.get_subscription_control("s").await.unwrap();
    assert_eq!(control.pause.map(|p| p.by).as_deref(), Some("Ines"));
    assert_eq!(control.rewind_generation, 2);
    port.set_subscription_pause("s", None).await.unwrap();
    drop(port);

    let path = file.path().to_string_lossy().into_owned();
    let reopened = SqliteStoragePort::open(&path).await.unwrap();
    let control = reopened.get_subscription_control("s").await.unwrap();
    assert_eq!(control.pause, None);
    assert_eq!(control.rewind_generation, 2);
    assert_eq!(
        reopened
            .get_subscription_control("other")
            .await
            .unwrap()
            .rewind_generation,
        0
    );
}

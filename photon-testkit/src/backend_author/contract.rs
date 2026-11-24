//! Minimal publish/subscribe contract for custom backends.

use std::time::Duration;

use chrono::{DateTime, Utc};
use futures::StreamExt;
use photon_backend::{ListEventsQuery, PhotonError, Result};
use photon_runtime::Photon;

use crate::fixtures::smoke_actor_json;

/// Publish → subscribe → optional `get_event` roundtrip on a fresh topic.
///
/// # Errors
///
/// Returns an error if the operation fails.
pub async fn run_backend_contract(photon: &Photon, topic: &str) -> Result<()> {
    let payload = serde_json::json!({"contract": true});
    let event_id = photon
        .publish(topic, None, smoke_actor_json(), payload)
        .await?;

    let mut stream = photon.subscribe(topic, None, None);
    let received = stream.next().await.ok_or_else(|| {
        photon_backend::PhotonError::Internal("subscribe stream ended empty".into())
    })??;

    if received.event_id != event_id {
        return Err(photon_backend::PhotonError::Internal(format!(
            "expected event_id {event_id}, got {}",
            received.event_id
        )));
    }

    if photon.backend_label() == "mem" || photon_capabilities_support_get_event(photon) {
        let loaded = photon.get_event(&event_id).await?.ok_or_else(|| {
            photon_backend::PhotonError::Internal("get_event returned None".into())
        })?;

        if loaded.event_id != event_id {
            return Err(photon_backend::PhotonError::Internal(
                "get_event id mismatch".into(),
            ));
        }
    }

    Ok(())
}

/// Count and time-range listing contract on a fresh topic, for backends that report support.
///
/// Publishes three batches (2, 3 and 2 events) separated by recorded timestamps, then checks:
///
/// - with `supports_count_events`: `count_events_by_topic` since a day ago returns 7, since the
///   middle batch returns 5, and `count_recent_events` counts at least 7;
/// - with `supports_list_events`: `list_events` over the middle range returns exactly the middle
///   batch newest first, and seq cursors page older (`before_seq`) and newer (`after_seq`).
///
/// Backends that report neither capability pass without checks.
///
/// # Errors
///
/// Returns an error describing the first broken expectation, or a backend error.
pub async fn run_count_and_list_contract(photon: &Photon, topic: &str) -> Result<()> {
    let caps = photon.admin_snapshot().await?.backend;
    let day_ago = Utc::now() - chrono::Duration::hours(24);

    publish_batch(photon, topic, 2).await?;
    let start = time_mark().await;
    publish_batch(photon, topic, 3).await?;
    let end = time_mark().await;
    publish_batch(photon, topic, 2).await?;

    if caps.supports_count_events {
        expect_eq(
            "count since a day ago",
            &photon
                .count_events_by_topic(topic, None, Some(day_ago))
                .await?,
            &Some(7),
        )?;
        expect_eq(
            "count since the middle batch",
            &photon
                .count_events_by_topic(topic, None, Some(start))
                .await?,
            &Some(5),
        )?;
        let recent = photon.count_recent_events(Some(day_ago)).await?;
        if recent.is_none_or(|n| n < 7) {
            return Err(PhotonError::Internal(format!(
                "count_recent_events: expected at least 7, got {recent:?}"
            )));
        }
    }

    if caps.supports_list_events {
        let range = ListEventsQuery {
            topic: Some(topic.to_owned()),
            since: Some(start),
            until: Some(end),
            newest_first: true,
            limit: 100,
            ..ListEventsQuery::default()
        };
        expect_eq(
            "time range, newest first",
            &seqs(photon, &range).await?,
            &vec![5, 4, 3],
        )?;

        let page = |after_seq, before_seq| ListEventsQuery {
            topic: Some(topic.to_owned()),
            after_seq,
            before_seq,
            newest_first: true,
            limit: 3,
            ..ListEventsQuery::default()
        };
        expect_eq(
            "newest page",
            &seqs(photon, &page(None, None)).await?,
            &vec![7, 6, 5],
        )?;
        expect_eq(
            "older page",
            &seqs(photon, &page(None, Some(5))).await?,
            &vec![4, 3, 2],
        )?;
        expect_eq(
            "newer page",
            &seqs(photon, &page(Some(3), None)).await?,
            &vec![6, 5, 4],
        )?;
    }

    Ok(())
}

async fn publish_batch(photon: &Photon, topic: &str, n: usize) -> Result<()> {
    for i in 0..n {
        photon
            .publish(topic, None, smoke_actor_json(), serde_json::json!({"n": i}))
            .await?;
    }
    Ok(())
}

/// A timestamp strictly between the events published before and after it.
async fn time_mark() -> DateTime<Utc> {
    tokio::time::sleep(Duration::from_millis(5)).await;
    let mark = Utc::now();
    tokio::time::sleep(Duration::from_millis(5)).await;
    mark
}

async fn seqs(photon: &Photon, query: &ListEventsQuery) -> Result<Vec<i64>> {
    Ok(photon
        .list_events(query)
        .await?
        .iter()
        .map(|e| e.seq)
        .collect())
}

fn expect_eq<T: PartialEq + std::fmt::Debug>(what: &str, got: &T, want: &T) -> Result<()> {
    if got == want {
        Ok(())
    } else {
        Err(PhotonError::Internal(format!(
            "{what}: expected {want:?}, got {got:?}"
        )))
    }
}

const fn photon_capabilities_support_get_event(_photon: &Photon) -> bool {
    // Broker tiers omit get_event until optional cache lands.
    false
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use photon_backend::{InProcStoragePort, TransportCrypto};

    use super::*;

    #[tokio::test]
    async fn count_and_list_contract_passes_on_mem() {
        let port = Arc::new(InProcStoragePort::new(TransportCrypto::from_bytes([1; 32])));
        let photon = Photon::builder()
            .storage_port(port)
            .build()
            .expect("mem photon");
        run_count_and_list_contract(&photon, "testkit.contract.count_list")
            .await
            .expect("contract");
    }
}

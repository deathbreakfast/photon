//! Minimal publish/subscribe contract for custom backends.

use chrono::Utc;
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
/// Publishes seven events, then checks:
///
/// - with `supports_count_events`: `count_events_by_topic` since a day ago returns 7 and
///   `count_recent_events` counts at least 7;
/// - with `supports_list_events`: `list_events` over a range cut at stored `created_at` values
///   returns exactly the events inside it, newest first, and seq cursors page older
///   (`before_seq`) and newer (`after_seq`);
/// - with both: the count since a stored `created_at` matches the events at or after it.
///
/// Expected range results are derived from the timestamps the backend stored, so the contract
/// needs no clock gaps between publishes. Backends that report neither capability pass without
/// checks.
///
/// # Errors
///
/// Returns an error describing the first broken expectation, or a backend error.
pub async fn run_count_and_list_contract(photon: &Photon, topic: &str) -> Result<()> {
    let caps = photon.admin_snapshot().await?.backend;
    let day_ago = Utc::now() - chrono::Duration::hours(24);

    for i in 0..7 {
        photon
            .publish(topic, None, smoke_actor_json(), serde_json::json!({"n": i}))
            .await?;
    }

    if caps.supports_count_events {
        expect_eq(
            "count since a day ago",
            &photon
                .count_events_by_topic(topic, None, Some(day_ago))
                .await?,
            &Some(7),
        )?;
        let recent = photon.count_recent_events(Some(day_ago)).await?;
        if recent.is_none_or(|n| n < 7) {
            return Err(PhotonError::Internal(format!(
                "count_recent_events: expected at least 7, got {recent:?}"
            )));
        }
    }

    if !caps.supports_list_events {
        return Ok(());
    }

    let all = photon
        .list_events(&ListEventsQuery {
            topic: Some(topic.to_owned()),
            limit: 100,
            ..ListEventsQuery::default()
        })
        .await?;
    expect_eq(
        "all events, oldest first",
        &all.iter().map(|e| e.seq).collect::<Vec<_>>(),
        &(1..=7).collect::<Vec<_>>(),
    )?;

    let (start, end) = (all[2].created_at, all[5].created_at);
    let in_range: Vec<i64> = all
        .iter()
        .rev()
        .filter(|e| e.created_at >= start && e.created_at < end)
        .map(|e| e.seq)
        .collect();
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
        &in_range,
    )?;

    if caps.supports_count_events {
        let since_start = all.iter().filter(|e| e.created_at >= start).count();
        expect_eq(
            "count since a stored created_at",
            &photon
                .count_events_by_topic(topic, None, Some(start))
                .await?,
            &Some(u64::try_from(since_start).unwrap_or(u64::MAX)),
        )?;
    }

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

    Ok(())
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

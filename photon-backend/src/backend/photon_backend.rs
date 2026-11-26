//! Object-safe backend trait for Photon publish/subscribe runtimes.
//!
//! Prefer wiring a [`crate::storage::StoragePort`] through
//! [`PhotonBuilder::storage_port`](https://docs.rs/uf-photon/latest/photon/struct.PhotonBuilder.html#method.storage_port).
//! Implement this trait only for custom delivery stacks, then install via
//! [`PhotonBuilder::backend_with_context`](https://docs.rs/uf-photon/latest/photon/struct.PhotonBuilder.html#method.backend_with_context).
//!
//! See also: [`crate::storage::StoragePort`], [`crate::GenericPhotonBackend`].

use std::pin::Pin;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use futures::stream::Stream;
use serde_json::Value;

use crate::backend::BackendCapabilities;
use crate::error::{PhotonError, Result};
use crate::models::Event;
use crate::registry::TopicRegistry;
use crate::storage::ListEventsQuery;

/// Stored control state for one subscription (durable name or consumer group id).
///
/// Stored by backends that report `supports_subscription_control`. The executor stops
/// dispatching while `pause` is set, and drops its stream and resubscribes from the stored
/// checkpoint when `rewind_generation` moves past the value it last saw.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SubscriptionControl {
    /// Set while the subscription is paused.
    pub pause: Option<SubscriptionPause>,
    /// Bumped by every replay or skip that moved the subscription's checkpoints; `0` when none
    /// has run.
    pub rewind_generation: u64,
}

/// Who paused a subscription, and when.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubscriptionPause {
    /// Display label of whoever paused it (Photon does not resolve identity).
    pub by: String,
    /// When it was paused.
    pub at: DateTime<Utc>,
}

/// Backend for publish/subscribe delivery and checkpoint persistence.
///
/// Most hosts never implement this — use a storage adapter and the default
/// [`GenericPhotonBackend`](crate::GenericPhotonBackend). Custom delivery example:
///
/// ```rust,ignore
/// use std::sync::Arc;
/// use photon_backend::{BackendContext, PhotonBackend};
/// use photon_runtime::Photon;
///
/// let _photon = Photon::builder()
///     .backend_with_context(|ctx: BackendContext| {
///         // return Ok(Arc::new(MyBackend::new(ctx)) as Arc<dyn PhotonBackend>)
///         photon_backend::EmbeddedBackend::install_mem(ctx)
///     })
///     .auto_registry()
///     .build()?;
/// ```
#[async_trait]
pub trait PhotonBackend: Send + Sync {
    /// Stable telemetry label for ops metrics (e.g. `"mem"`, `"nats"`).
    fn telemetry_label(&self) -> &'static str {
        "custom"
    }

    /// Adapter capabilities (replay window, get-by-id support, …).
    fn capabilities(&self) -> BackendCapabilities {
        BackendCapabilities::mem()
    }

    /// Append an event and return its event id.
    ///
    /// # Contract
    ///
    /// - Returns the stable `event_id` assigned by the underlying storage port.
    /// - Ordering and dedupe are per `(topic_name, topic_key)` partition, not global.
    async fn publish(
        &self,
        topic_name: &str,
        topic_key: Option<&str>,
        actor_json: Value,
        payload_json: Value,
    ) -> Result<String>;

    /// Stream events for a topic partition, optionally replaying after `after_seq`.
    ///
    /// # Contract
    ///
    /// - Same semantics as [`crate::storage::StoragePort::subscribe`].
    fn subscribe(
        &self,
        topic_name: String,
        topic_key_filter: Option<String>,
        after_seq: Option<i64>,
    ) -> Pin<Box<dyn Stream<Item = Result<Event>> + Send>>;

    /// Load a single event by id.
    ///
    /// # Contract
    ///
    /// - Returns `None` when unknown or truncated; see backend [`BackendCapabilities`].
    async fn get_event(&self, event_id: &str) -> Result<Option<Event>>;

    /// Bounded, filtered page of events (ops browse).
    ///
    /// Same semantics as [`crate::storage::StoragePort::list_events`]; see
    /// [`ListEventsQuery`] for filters, order and seq paging.
    ///
    /// Default returns empty when the adapter does not support listing.
    async fn list_events(&self, _query: &ListEventsQuery) -> Result<Vec<Event>> {
        Ok(Vec::new())
    }

    /// Bounded page of events for one topic (ops browse), oldest first.
    ///
    /// Default calls [`Self::list_events`].
    async fn list_by_topic(
        &self,
        topic_name: &str,
        topic_key: Option<&str>,
        after_seq: Option<i64>,
        limit: usize,
    ) -> Result<Vec<Event>> {
        self.list_events(&ListEventsQuery {
            topic: Some(topic_name.to_owned()),
            key: topic_key.map(str::to_owned),
            after_seq,
            limit,
            ..ListEventsQuery::default()
        })
        .await
    }

    /// Bounded cross-topic page of newest events (ops browse).
    ///
    /// Default calls [`Self::list_events`].
    async fn list_recent(&self, limit: usize) -> Result<Vec<Event>> {
        self.list_events(&ListEventsQuery {
            newest_first: true,
            limit,
            ..ListEventsQuery::default()
        })
        .await
    }

    /// Count a topic's events, optionally for one partition key and since a time.
    ///
    /// Default returns `Ok(None)`: the adapter cannot count (see
    /// [`BackendCapabilities::supports_count_events`]).
    async fn count_by_topic(
        &self,
        _topic_name: &str,
        _topic_key: Option<&str>,
        _since: Option<DateTime<Utc>>,
    ) -> Result<Option<u64>> {
        Ok(None)
    }

    /// Count events across all topics, optionally since a time.
    ///
    /// Default returns `Ok(None)`: the adapter cannot count.
    async fn count_recent(&self, _since: Option<DateTime<Utc>>) -> Result<Option<u64>> {
        Ok(None)
    }

    /// Inventory-discovered topic descriptors.
    fn registry(&self) -> &TopicRegistry;

    /// Load the last committed checkpoint seq for a subscription partition.
    ///
    /// # Contract
    ///
    /// - Returns `None` when no checkpoint exists.
    async fn get_checkpoint_seq(
        &self,
        subscription_name: &str,
        topic_name: &str,
        topic_key: Option<&str>,
    ) -> Result<Option<i64>>;

    /// Persist the high-water checkpoint seq for a subscription partition.
    ///
    /// # Contract
    ///
    /// - `last_seq` must not regress; coalesced writes may batch behind the scenes.
    async fn set_checkpoint(
        &self,
        subscription_name: &str,
        topic_name: &str,
        topic_key: Option<&str>,
        last_seq: i64,
    ) -> Result<()>;

    /// Highest `seq` ever assigned in a topic partition (ops/health introspection).
    ///
    /// Default returns `None` when the adapter does not support it.
    async fn head_seq(&self, _topic_name: &str, _topic_key: Option<&str>) -> Result<Option<i64>> {
        Ok(None)
    }

    /// Overwrite a checkpoint, moving it backward if needed (replay).
    ///
    /// # Contract
    ///
    /// - Unlike [`Self::set_checkpoint`], the stored value becomes exactly `last_seq`.
    /// - Default returns [`PhotonError::Unsupported`].
    async fn reset_checkpoint(
        &self,
        _subscription_name: &str,
        _topic_name: &str,
        _topic_key: Option<&str>,
        _last_seq: i64,
    ) -> Result<()> {
        Err(PhotonError::Unsupported("reset_checkpoint".into()))
    }

    /// Load the control state for a subscription; the default value when nothing is stored.
    ///
    /// Default returns [`PhotonError::Unsupported`]; see `supports_subscription_control`.
    async fn get_subscription_control(&self, _subscription: &str) -> Result<SubscriptionControl> {
        Err(PhotonError::Unsupported("subscription control".into()))
    }

    /// Store (`Some`) or clear (`None`) the pause for a subscription, keeping its rewind
    /// generation.
    ///
    /// Default returns [`PhotonError::Unsupported`]; see `supports_subscription_control`.
    async fn set_subscription_pause(
        &self,
        _subscription: &str,
        _pause: Option<SubscriptionPause>,
    ) -> Result<()> {
        Err(PhotonError::Unsupported("subscription control".into()))
    }

    /// Atomically increment a subscription's rewind generation and return the new value.
    ///
    /// Default returns [`PhotonError::Unsupported`]; see `supports_subscription_control`.
    async fn bump_rewind_generation(&self, _subscription: &str) -> Result<u64> {
        Err(PhotonError::Unsupported("subscription control".into()))
    }
}

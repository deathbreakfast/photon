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

/// Pause state for one subscription (durable name or consumer group id).
///
/// Stored by backends that report `supports_subscription_control`; the executor stops
/// dispatching for a subscription while one is set.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubscriptionControl {
    /// Display label of whoever paused it (Photon does not resolve identity).
    pub paused_by: String,
    /// When it was paused.
    pub paused_at: DateTime<Utc>,
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

    /// Bounded page of events for one topic (ops browse).
    ///
    /// Default returns empty when the adapter does not support listing.
    async fn list_by_topic(
        &self,
        _topic_name: &str,
        _topic_key: Option<&str>,
        _after_seq: Option<i64>,
        _limit: usize,
    ) -> Result<Vec<Event>> {
        Ok(Vec::new())
    }

    /// Bounded cross-topic page of newest events (ops browse).
    ///
    /// Default returns empty when the adapter does not support listing.
    async fn list_recent(&self, _limit: usize) -> Result<Vec<Event>> {
        Ok(Vec::new())
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

    /// Load the pause state for a subscription; `None` when it is running.
    ///
    /// Default returns [`PhotonError::Unsupported`]; see `supports_subscription_control`.
    async fn get_subscription_control(
        &self,
        _subscription: &str,
    ) -> Result<Option<SubscriptionControl>> {
        Err(PhotonError::Unsupported("subscription control".into()))
    }

    /// Store (`Some`) or clear (`None`) the pause state for a subscription.
    ///
    /// Default returns [`PhotonError::Unsupported`]; see `supports_subscription_control`.
    async fn set_subscription_control(
        &self,
        _subscription: &str,
        _control: Option<SubscriptionControl>,
    ) -> Result<()> {
        Err(PhotonError::Unsupported("subscription control".into()))
    }
}

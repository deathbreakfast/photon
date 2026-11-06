//! Backend wrapper that injects one stream fault into the first `subscribe` call for a topic.

use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use futures::{Stream, StreamExt};
use photon_backend::{
    BackendCapabilities, Event, GenericPhotonBackend, InProcStoragePort, PhotonBackend,
    PhotonError, Result, StoragePort, TopicRegistry, TransportCrypto,
};
use photon_runtime::Photon;

/// Fault injected into the first subscription stream of the faulty topic.
///
/// Later `subscribe` calls (resubscribes) and other topics pass through untouched.
#[derive(Clone, Copy, Debug)]
pub enum StreamFault {
    /// After `after` events, yield one [`PhotonError::StreamItem`] and keep the stream alive.
    BadItemAfter {
        /// Events delivered before the bad item.
        after: usize,
    },
    /// End the stream after `after` events, as a dropped connection would.
    EndAfter {
        /// Events delivered before the stream ends.
        after: usize,
    },
}

/// Event id carried by the injected bad item.
pub const BAD_ITEM_EVENT_ID: &str = "injected-bad-item";
/// Sequence carried by the injected bad item (outside the published range).
pub const BAD_ITEM_SEQ: i64 = 1_000;

/// Mem-backed [`PhotonBackend`] that applies a [`StreamFault`] to its first subscription.
pub struct FaultyBackend {
    inner: GenericPhotonBackend,
    topic: &'static str,
    fault: StreamFault,
    subscribes: AtomicUsize,
}

impl FaultyBackend {
    /// Build a [`Photon`] over an in-process store with the fault installed.
    ///
    /// # Errors
    ///
    /// Returns an error if the Photon build fails.
    pub fn photon(topic: &'static str, fault: StreamFault) -> Result<(Photon, Arc<Self>)> {
        let port: Arc<dyn StoragePort> = Arc::new(InProcStoragePort::new(
            TransportCrypto::from_bytes(*b"photon-dev-transport-key-32bytes"),
        ));
        let backend = Arc::new(Self {
            inner: GenericPhotonBackend::new(Arc::clone(&port), TopicRegistry::auto_discover()),
            topic,
            fault,
            subscribes: AtomicUsize::new(0),
        });
        let photon = Photon::builder()
            .storage_port(port)
            .backend(Arc::clone(&backend) as Arc<dyn PhotonBackend>)
            .auto_registry()
            .build()?;
        Ok((photon, backend))
    }

    /// Number of `subscribe` calls for the faulty topic so far (1 means the executor never resubscribed).
    #[must_use]
    pub fn subscribe_calls(&self) -> usize {
        self.subscribes.load(Ordering::SeqCst)
    }
}

#[async_trait::async_trait]
impl PhotonBackend for FaultyBackend {
    fn telemetry_label(&self) -> &'static str {
        self.inner.telemetry_label()
    }

    fn capabilities(&self) -> BackendCapabilities {
        self.inner.capabilities()
    }

    async fn publish(
        &self,
        topic_name: &str,
        topic_key: Option<&str>,
        actor_json: serde_json::Value,
        payload_json: serde_json::Value,
    ) -> Result<String> {
        self.inner
            .publish(topic_name, topic_key, actor_json, payload_json)
            .await
    }

    fn subscribe(
        &self,
        topic_name: String,
        topic_key_filter: Option<String>,
        after_seq: Option<i64>,
    ) -> Pin<Box<dyn Stream<Item = Result<Event>> + Send>> {
        let faulty_topic = topic_name == self.topic;
        let inner = self
            .inner
            .subscribe(topic_name, topic_key_filter, after_seq);
        if !faulty_topic || self.subscribes.fetch_add(1, Ordering::SeqCst) > 0 {
            return inner;
        }
        match self.fault {
            StreamFault::EndAfter { after } => Box::pin(inner.take(after)),
            StreamFault::BadItemAfter { after } => {
                Box::pin(inner.enumerate().flat_map(move |(i, item)| {
                    let mut items = vec![item];
                    if i + 1 == after {
                        items.push(Err(PhotonError::stream_item(
                            "injected bad item:",
                            Some(BAD_ITEM_EVENT_ID.into()),
                            Some(BAD_ITEM_SEQ),
                            "corrupt payload",
                        )));
                    }
                    futures::stream::iter(items)
                }))
            }
        }
    }

    async fn get_event(&self, event_id: &str) -> Result<Option<Event>> {
        self.inner.get_event(event_id).await
    }

    fn registry(&self) -> &TopicRegistry {
        self.inner.registry()
    }

    async fn get_checkpoint_seq(
        &self,
        subscription_name: &str,
        topic_name: &str,
        topic_key: Option<&str>,
    ) -> Result<Option<i64>> {
        self.inner
            .get_checkpoint_seq(subscription_name, topic_name, topic_key)
            .await
    }

    async fn set_checkpoint(
        &self,
        subscription_name: &str,
        topic_name: &str,
        topic_key: Option<&str>,
        seq: i64,
    ) -> Result<()> {
        self.inner
            .set_checkpoint(subscription_name, topic_name, topic_key, seq)
            .await
    }

    async fn head_seq(&self, topic_name: &str, topic_key: Option<&str>) -> Result<Option<i64>> {
        self.inner.head_seq(topic_name, topic_key).await
    }
}

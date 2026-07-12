//! Error types for Photon.

use thiserror::Error;

/// Result type alias for Photon operations.
pub type Result<T> = std::result::Result<T, PhotonError>;

/// Errors that can occur in Photon operations.
#[derive(Debug, Clone, Error)]
pub enum PhotonError {
    /// Topic not found in registry.
    #[error("topic not found: {0}")]
    TopicNotFound(String),

    /// Subscription not found.
    #[error("subscription not found: {0}")]
    SubscriptionNotFound(String),

    /// Event not found.
    #[error("event not found: {0}")]
    EventNotFound(String),

    /// Invalid topic name.
    #[error("invalid topic name: {0}")]
    InvalidTopicName(String),

    /// Payload serialization/deserialization error.
    #[error("payload error: {0}")]
    PayloadError(String),

    /// Schema mismatch at publish time.
    #[error("schema mismatch: {0}")]
    SchemaMismatch(String),

    /// Topic already registered with different schema.
    #[error("topic already exists: {0}")]
    TopicAlreadyExists(String),

    /// Subscription name required for durable subscriptions.
    #[error("subscription name required for durable subscriptions")]
    SubscriptionNameRequired,

    /// Persistence / store error (ops metadata adapters).
    #[error("persistence error: {0}")]
    PersistenceError(String),

    /// Identity reconstruction failed at the handler boundary.
    ///
    /// Produced when [`photon_core::IdentityFactory::reconstruct`] rejects actor JSON
    /// (or a typed-actor downcast fails). Executor maps this to
    /// [`crate::instrumentation::FailureReason::IdentityBuild`].
    #[error("identity error: {0}")]
    Identity(String),

    /// Internal error.
    #[error("internal error: {0}")]
    Internal(String),
}

impl From<serde_json::Error> for PhotonError {
    fn from(err: serde_json::Error) -> Self {
        Self::PayloadError(err.to_string())
    }
}

impl From<anyhow::Error> for PhotonError {
    fn from(err: anyhow::Error) -> Self {
        Self::Internal(err.to_string())
    }
}

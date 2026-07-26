//! Transport log, storage adapters, and delivery for Photon.
//!
//! Pluggable [`StoragePort`] implementations, unified [`GenericPhotonBackend`],
//! subscriptions, checkpoints, retention, and consumer groups.
//!
//! ## Goals
//!
//! - Append-only sequenced transport with swappable storage adapters (`mem`, `sqlite`, broker tiers)
//! - Durable subscriptions with coalesced checkpoints and optional retention reclaim
//! - Consumer-group shard routing for load-balanced handlers
//!
//! ## Non-goals
//!
//! - Canonical system-of-record datastore (transport log is encrypted and transient)
//! - Ops/admin UI (hosts provide their own)
//!
//! Public crate documentation map: `cargo doc -p uf-photon --features runtime,mem --open`.

#![cfg(feature = "runtime")]
#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used))]

pub mod backend;
pub mod broker_security;
pub mod checkpoint;
pub mod consumer_group;
pub mod delivery;
pub mod delivery_mode;
pub mod descriptor;
pub mod error;
pub mod event;
pub mod executor_services;
pub mod group_subscribe;
pub mod handler_ctx;
pub mod handler_descriptor;
pub mod handler_registry;
pub mod input;
pub mod instrumentation;
pub mod models;
pub mod publish_routing;
pub mod registry;
pub mod retention;
pub mod sanitize;
pub mod shard_router;
pub mod storage;

pub use backend::{
    BackendCapabilities, BackendContext, EmbeddedBackend, GenericPhotonBackend, PhotonBackend,
};
pub use broker_security::{BrokerTransportSecurity, ALLOW_INSECURE_BROKER_ENV};
pub use consumer_group::{
    ConsumerGroupCoordinator, ConsumerLease, FleetGroupCoordinator, GroupMember, LeaseStore,
    MemoryLeaseStore, StaticGroupCoordinator,
};
pub use delivery_mode::{DeliveryMode, ShardConfig};
pub use descriptor::TopicDescriptor;
pub use error::{PhotonError, Result, SharedError};
pub use event::{open_stored_event, seal_event_for_storage, TransportCrypto, ENVELOPE_JSON_KEY};
pub use executor_services::ExecutorServices;
pub use group_subscribe::{merge_shard_streams, AbortOnDrop};
pub use handler_ctx::HandlerCtx;
pub use handler_descriptor::HandlerDescriptor;
pub use handler_registry::HandlerRegistry;
pub use input::{
    map_broker_connect_err, redact_credentials_in_text, redact_endpoint, validate_payload_size,
    validate_topic_name, MAX_PAYLOAD_JSON_BYTES, MAX_TOPIC_NAME_BYTES,
};
pub use models::{
    Envelope, Event, GroupOpts, SubscribeOpts, Subscription, SubscriptionHandle, SubscriptionMode,
    TopicMetadata,
};
pub use publish_routing::{resolve_publish_target, PublishTarget};
pub use registry::TopicRegistry;
pub use retention::{
    ReclaimReport, RetentionDeps, RetentionHook, RetentionPolicy, RetentionReclaimer,
    SubscriptionPartition, TopicPartition,
};
pub use sanitize::{sanitize_error_message, MAX_ERROR_MESSAGE_CHARS};
pub use shard_router::{
    group_publish_storage_key, is_shard_storage_key, parse_shard_storage_key, routing_key,
    shard_id, shard_storage_key, SHARD_KEY_PREFIX,
};
pub use storage::{topic_filter_matches, InProcStoragePort, StorageCapabilities, StoragePort};

pub use quark::inventory;

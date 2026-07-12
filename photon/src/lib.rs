//! Pub/sub event pipeline facade.
//!
//! Typed topics, durable subscriptions with checkpoints, and the same API in single-process and
//! multi-node deployments. Enable the `runtime` feature for the full stack (`Photon`, backends,
//! executor). Photon uses pluggable **storage adapters**
//! (`mem`, `sqlite`, `nats`, `fluvio`, `kafka`) behind [`StoragePort`] and Quark inventory for topic/handler
//! discovery. Payloads are encrypted before append; storage stays opaque. Canonical business data
//! remains in your datastore — the transport log is for event delivery, not system of record.
//!
//! ## Stack
//!
//! ```text
//! Application  →  Photon (macros + Photon runtime)  →  storage port  →  delivery backend
//! ```
//!
//! ## Architecture
//!
//! Persistence and cross-process delivery are pluggable via [`photon_backend::storage::StoragePort`].
//! [`photon_backend::backend::GenericPhotonBackend`] implements [`PhotonBackend`] for every adapter.
//!
//! ```text
//!   Typed API (#[topic], publish_on / publish, #[subscribe])
//!              │
//!              v
//!   Photon runtime + GenericPhotonBackend
//!              │
//!              v
//!         StoragePort  ──►  mem (InProcStoragePort)
//!                      ──►  sqlite (SqliteStoragePort)
//!                      ──►  nats / fluvio / kafka (broker crates)
//!                      ──►  custom implementation
//! ```
//!
//! Built-in adapters: `mem`, `sqlite`, `nats`, `fluvio`, `kafka`. Configuration and broker wiring:
//! [`config`]. Macro expansion: repository `docs/macro-expansion.md`. Bench methodology:
//! `photon-bench/PERFORMANCE_STUDY.md`.
//!
// Maintainer lane rules (not rendered in public docs):
// - Creating topics: #[topic], #[subscribe], publish_on/subscribe_on, start_executor — no boot APIs.
// - Integrating the host: PhotonBuilder, optional configure, config/env, EmbeddedBackend — once per process.
// - Developing the backend: PhotonBackend trait, custom storage adapters.
// If content sits on a topic-creator entry point, link to Integrating the host instead of duplicating boot steps.
//!
//! ## Documentation map
//!
//! Photon splits **boot** (wire the runtime once) from **topics** (add handlers and publish events).
//! Each section lists only what belongs in that workflow. Linked items include `# Example`
//! sections; runnable binaries are cited below.
//!
//! ### Creating topics
//!
//! Assumes the host process is already running. To stand up Photon for the first time, see
//! [Integrating the host](#integrating-the-host) below.
//!
//! - [`topic`] / [`subscribe`] — typed publish/subscribe macros and inventory registration
//! - [`prelude`] — common imports (`Event`, `SubscribeOpts`, `Photon`, macros)
//! - [`Photon::start_executor`] — dispatch inventory-registered `#[subscribe]` handlers
//! - Enqueue: `<EventType>::publish_on(&photon)` (preferred) or `.publish()` after [`configure`]
//!
//! Manual streams without the macro: [`Photon::subscribe`], [`SubscribeOpts`].
//!
//! Runnable: `cargo run -p uf-photon --example manual_subscribe --features runtime,mem`.
//!
//! ### Integrating the host
//!
//! Do this **once** when standing up or changing how the process runs.
//!
//! **Boot** — wire storage port, delivery backend, and discovery:
//!
//! - [`PhotonBuilder`] — [`storage_port`](PhotonBuilder::storage_port),
//!   [`backend_with_context`](PhotonBuilder::backend_with_context), [`mem_backend`](PhotonBuilder::mem_backend),
//!   [`auto_registry`](PhotonBuilder::auto_registry)
//! - Keep the [`Photon`] handle and pass it to `publish_on` / `subscribe_on` (preferred)
//! - Optional: [`configure`] — process-wide default for macro `.publish()` / `.subscribe()`
//! - Optional: [`config`] (env vars, retention, macro attributes), [`OpsLog`] via
//!   [`PhotonBuilder::ops_log`]
//!
//! **Run** — execute handlers and reclaim transport:
//!
//! - [`Photon::start_executor`] — durable / group handler dispatch
//! - [`Photon::reclaim_transport`] — retention sweep entry point
//!
//! Runnable: `cargo run -p uf-photon --example embedded_mem --features runtime,mem`
//! (requires `PHOTON_TRANSPORT_KEY`; see example).
//!
//! ### Developing the backend
//!
//! **Storage adapter (typical):** implement or select a [`photon_backend::storage::StoragePort`]
//! and pass it to [`PhotonBuilder::storage_port`]. Built-in ports: `mem` / `sqlite` for single-process;
//! broker adapters document options on their `*StoragePortBuilder` types (see [`config`] index).
//!
//! **Advanced:** implement [`PhotonBackend`] directly for custom delivery semantics.
//! Reference in-process stack: [`EmbeddedBackend`]. Install via [`PhotonBuilder::backend_with_context`]
//! or [`EmbeddedBackend::install_mem`].
//!
//! ## First boot
//!
//! ```rust,no_run
//! use std::sync::Arc;
//!
//! use photon::{JsonIdentityFactory, Photon};
//!
//! # fn main() -> photon::Result<()> {
//! // PhotonBuilder defaults to InProcStoragePort, which loads PHOTON_TRANSPORT_KEY via from_env().
//! let photon = Photon::builder().auto_registry().build()?;
//! photon.start_executor(Arc::new(JsonIdentityFactory))?;
//! // Prefer publish_on(&photon) / Photon::publish. Optional: configure(photon) for .publish() sugar.
//! # let _ = photon;
//! # Ok(())
//! # }
//! ```
//!
//! Full option reference: [`config`]. Macro expansion: repository `docs/macro-expansion.md`.

/// Register a typed topic struct (`#[photon::topic]`).
pub use photon_macros::topic;
/// Register an async handler (`#[photon::subscribe]`).
pub use photon_macros::subscribe;
/// Re-export identity port traits and JSON stubs from [`photon_core`].
pub use photon_core::{
    actor_downcast_methods, Actor, IdentityError, IdentityFactory, JsonActor, JsonIdentityFactory,
};
/// Quark inventory for compile-time topic/handler registration.
pub use quark::inventory;

#[cfg(feature = "runtime")]
mod runtime;

#[cfg(feature = "runtime")]
pub use runtime::*;

#[cfg(feature = "runtime")]
pub mod config;

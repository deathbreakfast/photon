//! Read-only admin introspection for host ops UIs.
//!
//! Product hosts call [`Photon::admin_snapshot`](crate::Photon::admin_snapshot) to obtain a
//! serde-friendly [`AdminSnapshot`] without mirroring bus state into a secondary store.
//! HTTP routes and authorization are host-owned; this module exposes the Rust API only.

mod snapshot;
mod types;

pub use snapshot::collect_admin_snapshot;
pub use types::{
    AdminBackendSummary, AdminCheckpointSummary, AdminHandlerSummary, AdminSnapshot,
    AdminTopicSummary,
};

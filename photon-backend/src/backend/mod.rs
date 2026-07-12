//! Backend implementations and assembly context for Photon.

mod capabilities;
mod context;
mod generic;
mod photon_backend;

pub use capabilities::BackendCapabilities;
pub use context::BackendContext;
pub use generic::GenericPhotonBackend;
pub use photon_backend::PhotonBackend;

/// Back-compat alias for the default in-process `mem` tier [`GenericPhotonBackend`].
pub type EmbeddedBackend = GenericPhotonBackend;

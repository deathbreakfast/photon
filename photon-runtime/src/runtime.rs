//! Host-facing runtime wiring helpers (**Integrating the host**).

use std::sync::Arc;

use crate::{configure, Photon};

/// Core runtime parts after builder (handler executor wired by integration layer).
pub struct PhotonRuntimeParts {
    /// Configured Photon handle.
    pub photon: Arc<Photon>,
}

/// Build [`Photon`] with the default in-process `mem` storage adapter.
///
/// # Errors
///
/// Returns an error if the operation fails.
pub fn build_photon_parts() -> anyhow::Result<PhotonRuntimeParts> {
    let photon = Photon::builder()
        .auto_registry()
        .build()
        .map_err(|e| anyhow::anyhow!("Photon build failed: {e}"))?;

    let photon = Arc::new(photon);
    configure((*photon).clone());

    Ok(PhotonRuntimeParts { photon })
}

//! Backend-author test harness (custom [`photon_backend::PhotonBackend`] crates).

mod contract;
mod harness;

pub use contract::run_backend_contract;
pub use harness::BackendAuthorHarness;

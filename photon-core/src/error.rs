//! Identity and factory errors.

use thiserror::Error;

/// Errors from [`crate::IdentityFactory::reconstruct`].
#[derive(Debug, Error)]
pub enum IdentityError {
    /// JSON or policy rejected the actor payload.
    #[error("invalid actor: {0}")]
    InvalidActor(String),
    /// Factory/backend failure (typed; no `anyhow` in the library API).
    #[error("identity factory: {0}")]
    Factory(String),
}

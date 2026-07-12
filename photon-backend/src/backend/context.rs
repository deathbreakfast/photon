//! Wiring inputs passed to backend `install` functions.
use crate::registry::TopicRegistry;

/// Shared builder context for storage adapter install fns.
pub struct BackendContext {
    /// Inventory-discovered topic descriptors.
    pub registry: TopicRegistry,
}

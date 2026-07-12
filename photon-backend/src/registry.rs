//! Topic registry for discovering and looking up registered topics.

#![allow(missing_docs)]

use crate::descriptor::TopicDescriptor;
use crate::error::{PhotonError, Result};

quark::define_registry! {
    /// Registry of all topics discovered via `#[photon::topic]`.
    pub struct TopicRegistry for TopicDescriptor;
}

impl TopicRegistry {
    /// Look up a topic by name, returning an error if not found.
    ///
    /// # Errors
    ///
    /// Returns an error if the operation fails.
    pub fn get_or_err(&self, topic_name: &str) -> Result<&'static TopicDescriptor> {
        self.get(topic_name)
            .ok_or_else(|| PhotonError::TopicNotFound(topic_name.to_string()))
    }

    /// Topic names in sorted order for deterministic registration across platforms.
    #[must_use]
    pub fn sorted_topic_names(&self) -> Vec<&str> {
        let mut names = self.list();
        names.sort_unstable();
        names
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_empty_registry() {
        let registry = TopicRegistry::new();
        assert!(registry.is_empty());
        assert_eq!(registry.len(), 0);
        assert!(registry.get("nonexistent").is_none());
    }

    #[test]
    fn test_auto_discover() {
        let registry = TopicRegistry::auto_discover();
        let _ = registry.list();
    }

    #[test]
    fn test_get_or_err_not_found() {
        let registry = TopicRegistry::new();
        let err = registry.get_or_err("nonexistent").unwrap_err();
        assert!(matches!(err, PhotonError::TopicNotFound(_)));
    }
}

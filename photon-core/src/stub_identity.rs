//! Minimal JSON identity factory for tests, docs, and dev and test runs.

use std::any::Any;

use serde_json::Value;

use crate::error::IdentityError;
use crate::identity::{Actor, IdentityFactory};

/// Actor label derived from publish-time `actor_json` (`System.operation` when present).
#[derive(Debug, Clone)]
pub struct JsonActor {
    label: String,
}

impl Actor for JsonActor {
    fn label(&self) -> &str {
        &self.label
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn into_any(self: Box<Self>) -> Box<dyn Any> {
        self
    }
}

/// Reconstructs [`JsonActor`] from JSON captured at publish time.
///
/// Sufficient for README examples, integration tests, and hosts that only need
/// a debug label at the handler boundary until a custom [`IdentityFactory`] is wired.
#[derive(Debug, Default, Clone, Copy)]
pub struct JsonIdentityFactory;

impl IdentityFactory for JsonIdentityFactory {
    fn reconstruct(&self, actor_json: &str) -> Result<Box<dyn Actor>, IdentityError> {
        let value: Value = serde_json::from_str(actor_json)
            .map_err(|e| IdentityError::InvalidActor(e.to_string()))?;
        let label = value
            .get("System")
            .and_then(|s| s.get("operation"))
            .and_then(|v| v.as_str())
            .unwrap_or("json-actor")
            .to_string();
        Ok(Box::new(JsonActor { label }))
    }
}

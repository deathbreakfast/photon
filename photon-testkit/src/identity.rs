//! Stub identity factory for tests.

use std::any::Any;

use photon_core::{Actor, IdentityError, IdentityFactory};
use serde_json::Value;

/// Parses actor JSON and returns a test label — sufficient for matrix smoke tests.
#[derive(Debug, Default, Clone, Copy)]
pub struct StubIdentityFactory;

struct StubActor {
    label: String,
}

impl Actor for StubActor {
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

impl IdentityFactory for StubIdentityFactory {
    fn reconstruct(&self, actor_json: &str) -> Result<Box<dyn Actor>, IdentityError> {
        let value: Value = serde_json::from_str(actor_json)
            .map_err(|e| IdentityError::InvalidActor(e.to_string()))?;
        let label = value
            .get("System")
            .and_then(|s| s.get("operation"))
            .and_then(|v| v.as_str())
            .unwrap_or("stub")
            .to_string();
        Ok(Box::new(StubActor { label }))
    }
}

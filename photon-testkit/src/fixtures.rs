//! Synthetic topics and payloads for tests.

/// Default topic for matrix smoke scenarios.
pub const SMOKE_TOPIC: &str = "testkit.smoke";

/// Sample actor JSON for stub identity.
#[must_use]
pub fn smoke_actor_json() -> serde_json::Value {
    serde_json::json!({"System": {"operation": "testkit"}})
}

/// Sample publish payload.
#[must_use]
pub fn smoke_payload() -> serde_json::Value {
    serde_json::json!({"n": 1})
}

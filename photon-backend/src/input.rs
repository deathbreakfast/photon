//! Boundary validation and safe endpoint formatting.

use serde_json::Value;

use crate::{PhotonError, Result};

/// Maximum accepted topic name length in bytes.
pub const MAX_TOPIC_NAME_BYTES: usize = 256;

/// Maximum serialized JSON payload size in bytes.
pub const MAX_PAYLOAD_JSON_BYTES: usize = 1024 * 1024;

/// Validate a Photon topic name before it reaches a storage adapter.
///
/// NATS wildcard tokens are rejected so a Photon topic always denotes one concrete topic.
///
/// # Errors
///
/// Returns [`PhotonError::InvalidTopicName`] for empty, wildcard-containing, or oversized names.
pub fn validate_topic_name(topic_name: &str) -> Result<()> {
    if topic_name.is_empty() {
        return Err(PhotonError::InvalidTopicName(
            "topic name must not be empty".into(),
        ));
    }
    if topic_name.len() > MAX_TOPIC_NAME_BYTES {
        return Err(PhotonError::InvalidTopicName(format!(
            "topic name exceeds {MAX_TOPIC_NAME_BYTES} bytes"
        )));
    }
    if topic_name.contains('*') || topic_name.contains('>') {
        return Err(PhotonError::InvalidTopicName(
            "topic name must not contain wildcard tokens ('*' or '>')".into(),
        ));
    }
    Ok(())
}

/// Reject payloads whose serialized JSON representation exceeds the transport limit.
///
/// # Errors
///
/// Returns [`PhotonError::PayloadError`] when serialization fails or exceeds the limit.
pub fn validate_payload_size(payload: &Value) -> Result<()> {
    let size = serde_json::to_vec(payload)?.len();
    if size > MAX_PAYLOAD_JSON_BYTES {
        return Err(PhotonError::PayloadError(format!(
            "serialized payload exceeds {MAX_PAYLOAD_JSON_BYTES} bytes"
        )));
    }
    Ok(())
}

/// Remove URL userinfo before placing an endpoint in an error or log message.
#[must_use]
pub fn redact_endpoint(endpoint: &str) -> String {
    endpoint
        .split(',')
        .map(redact_single_endpoint)
        .collect::<Vec<_>>()
        .join(",")
}

fn redact_single_endpoint(endpoint: &str) -> String {
    let Some(scheme_end) = endpoint.find("://") else {
        return endpoint.to_string();
    };
    let authority_start = scheme_end + 3;
    let authority = &endpoint[authority_start..];
    let Some(userinfo_end) = authority.find('@') else {
        return endpoint.to_string();
    };
    let userinfo_end = authority_start + userinfo_end;
    let host_start = userinfo_end + 1;
    if endpoint[authority_start..userinfo_end].contains(['/', '?', '#']) {
        return endpoint.to_string();
    }
    format!(
        "{}***@{}",
        &endpoint[..authority_start],
        &endpoint[host_start..]
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_concrete_topic_name() {
        assert!(validate_topic_name("orders.created").is_ok());
    }

    #[test]
    fn rejects_nats_wildcard_topic_names() {
        assert!(matches!(
            validate_topic_name("foo.>"),
            Err(PhotonError::InvalidTopicName(_))
        ));
        assert!(matches!(
            validate_topic_name("*"),
            Err(PhotonError::InvalidTopicName(_))
        ));
    }

    #[test]
    fn rejects_empty_and_oversized_topic_names() {
        assert!(matches!(
            validate_topic_name(""),
            Err(PhotonError::InvalidTopicName(_))
        ));
        assert!(matches!(
            validate_topic_name(&"x".repeat(MAX_TOPIC_NAME_BYTES + 1)),
            Err(PhotonError::InvalidTopicName(_))
        ));
    }

    #[test]
    fn rejects_oversized_payload() {
        let payload = Value::String("x".repeat(MAX_PAYLOAD_JSON_BYTES));
        assert!(matches!(
            validate_payload_size(&payload),
            Err(PhotonError::PayloadError(_))
        ));
    }

    #[test]
    fn redacts_url_userinfo() {
        let redacted = redact_endpoint("nats://user:secret@host:4222");
        assert_eq!(redacted, "nats://***@host:4222");
        assert!(!redacted.contains("secret"));
    }
}

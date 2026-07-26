//! Boundary validation and safe endpoint formatting.

use std::fmt;

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

/// Length of a URL-like prefix starting at `s` (scheme through host/path until whitespace).
fn consume_endpoint_prefix(s: &str) -> usize {
    let Some(scheme_end) = s.find("://") else {
        return s.len();
    };
    let after_scheme = scheme_end + 3;
    let rest = &s[after_scheme..];
    let end_rel = rest
        .find(|c: char| c.is_whitespace() || c == '"' || c == '\'' || c == ')' || c == ']')
        .unwrap_or(rest.len());
    after_scheme + end_rel
}

/// Redact `scheme://userinfo@host` substrings embedded in free-form error text.
#[must_use]
pub fn redact_credentials_in_text(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let mut i = 0;
    while i < text.len() {
        if let Some(rel) = text[i..].find("://") {
            let abs = i + rel;
            let scheme_start = text[..abs]
                .rfind(|c: char| !(c.is_ascii_alphanumeric() || c == '+' || c == '.' || c == '-'))
                .map_or(i, |j| j + 1);
            out.push_str(&text[i..scheme_start]);
            let consumed = consume_endpoint_prefix(&text[scheme_start..]);
            let endpoint = &text[scheme_start..scheme_start + consumed];
            out.push_str(&redact_endpoint(endpoint));
            i = scheme_start + consumed;
        } else {
            out.push_str(&text[i..]);
            break;
        }
    }
    out
}

/// Broker connect failure labeled with a redacted endpoint and redacted source text.
///
/// `label` is a short prefix such as `"nats connect"` or `"nats credentials file"`.
/// Use for all adapter connect paths so URL userinfo never lands in error labels or sources.
#[must_use]
pub fn map_broker_connect_err(label: &str, endpoint: &str, err: impl fmt::Display) -> PhotonError {
    let detail = redact_credentials_in_text(&err.to_string());
    PhotonError::caused(format!("{label} {}", redact_endpoint(endpoint)), detail)
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

    #[test]
    fn leaves_urls_without_userinfo() {
        assert_eq!(
            redact_endpoint("nats://127.0.0.1:4222"),
            "nats://127.0.0.1:4222"
        );
    }

    #[test]
    fn redacts_embedded_url_in_error_text() {
        let raw = "connection failed: nats://u:p@localhost:4222 refused";
        let redacted = redact_credentials_in_text(raw);
        assert!(redacted.contains("nats://***@localhost:4222"));
        assert!(!redacted.contains("u:p@"));
    }

    #[test]
    fn map_broker_connect_err_redacts_label_and_source() {
        let err = map_broker_connect_err(
            "nats connect",
            "nats://user:secret@host:4222",
            "dial nats://user:secret@host:4222 timed out",
        );
        let msg = err.to_string();
        assert!(
            msg.contains("nats connect nats://***@host:4222"),
            "msg: {msg}"
        );
        assert!(!msg.contains("secret"), "msg: {msg}");
        let source = std::error::Error::source(&err)
            .expect("caused keeps source")
            .to_string();
        assert!(!source.contains("secret"), "source: {source}");
        assert!(source.contains("nats://***@host:4222"), "source: {source}");
    }

    #[test]
    fn map_broker_connect_err_sad_path_still_surfaces_failure() {
        let err = map_broker_connect_err("kafka connect", "plain-host:9092", "broker down");
        assert!(err.to_string().contains("kafka connect plain-host:9092"));
        let source = std::error::Error::source(&err)
            .expect("caused keeps source")
            .to_string();
        assert!(source.contains("broker down"));
    }
}

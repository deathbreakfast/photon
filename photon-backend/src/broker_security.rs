//! Broker transport security policy for adapter connect paths.

use crate::{PhotonError, Result};

/// Environment opt-in for plaintext broker endpoints (development/CI only).
pub const ALLOW_INSECURE_BROKER_ENV: &str = "PHOTON_ALLOW_INSECURE_BROKER";

/// How Photon may connect to an external broker.
///
/// Production hosts should use [`Self::RequireTls`]. Plaintext endpoints require an explicit
/// [`Self::AllowInsecurePlaintext`] opt-in (builder method or [`ALLOW_INSECURE_BROKER_ENV`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum BrokerTransportSecurity {
    /// Reject plaintext broker URLs/endpoints; prefer TLS (`tls://`, SDK TLS options).
    #[default]
    RequireTls,
    /// Development/CI only: allow `nats://` and other plaintext broker endpoints.
    AllowInsecurePlaintext,
}

impl BrokerTransportSecurity {
    /// Load from [`ALLOW_INSECURE_BROKER_ENV`] (`1`/`true` → insecure; otherwise require TLS).
    #[must_use]
    pub fn from_env() -> Self {
        match std::env::var(ALLOW_INSECURE_BROKER_ENV).as_deref() {
            Ok("1" | "true" | "TRUE" | "yes" | "YES") => Self::AllowInsecurePlaintext,
            _ => Self::RequireTls,
        }
    }

    /// Returns true when plaintext broker endpoints are permitted.
    #[must_use]
    pub const fn allows_plaintext(self) -> bool {
        matches!(self, Self::AllowInsecurePlaintext)
    }

    /// Fail closed when `endpoint` looks like plaintext and insecure opt-in is absent.
    ///
    /// Recognizes common plaintext schemes: `nats://`, `kafka://`, `ftp://`, bare `host:port`
    /// without a `tls` / `ssl` / `https` marker. `tls://`, `nats+tls://`, and URLs containing
    /// `ssl`/`tls` as the scheme are treated as TLS-oriented.
    ///
    /// # Errors
    ///
    /// Returns [`PhotonError::Internal`] when plaintext would be used without opt-in.
    pub fn check_endpoint(self, endpoint: &str) -> Result<()> {
        if self.allows_plaintext() || endpoint_looks_tls(endpoint) {
            return Ok(());
        }
        Err(PhotonError::Internal(format!(
            "plaintext broker endpoint rejected under BrokerTransportSecurity::RequireTls \
             (endpoint looks non-TLS). Opt in explicitly with .allow_insecure_plaintext() \
             or {ALLOW_INSECURE_BROKER_ENV}=1 for development/CI only"
        )))
    }
}

fn endpoint_looks_tls(endpoint: &str) -> bool {
    let trimmed = endpoint.trim();
    let lower = trimmed.to_ascii_lowercase();
    if lower.starts_with("tls://")
        || lower.starts_with("ssl://")
        || lower.starts_with("https://")
        || lower.starts_with("nats+tls://")
        || lower.starts_with("rediss://")
    {
        return true;
    }
    // Multi-URL lists: every entry must look TLS-capable when requiring TLS.
    if trimmed.contains(',') {
        return trimmed
            .split(',')
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .all(endpoint_looks_tls);
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn require_tls_rejects_nats_plaintext() {
        let err = BrokerTransportSecurity::RequireTls
            .check_endpoint("nats://127.0.0.1:4222")
            .expect_err("plaintext");
        assert!(err.to_string().contains("plaintext"));
    }

    #[test]
    fn require_tls_accepts_tls_scheme() {
        BrokerTransportSecurity::RequireTls
            .check_endpoint("tls://broker.example:4222")
            .expect("tls ok");
    }

    #[test]
    fn insecure_allows_nats_plaintext() {
        BrokerTransportSecurity::AllowInsecurePlaintext
            .check_endpoint("nats://127.0.0.1:4222")
            .expect("insecure ok");
    }
}

//! NATS client connection helpers.

use photon_backend::{redact_endpoint, BrokerTransportSecurity, PhotonError, Result};

/// Connect to one or more NATS servers (`PHOTON_NATS_URL` may be comma-separated).
///
/// # Errors
///
/// Returns an error when the security policy rejects the endpoint or connection fails.
pub async fn connect_nats(
    urls: &str,
    security: BrokerTransportSecurity,
) -> Result<async_nats::Client> {
    security.check_endpoint(urls)?;
    let servers: Vec<&str> = urls
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .collect();
    if servers.is_empty() {
        return Err(PhotonError::Internal(
            "PHOTON_NATS_URL empty after parsing".into(),
        ));
    }
    let require_tls = matches!(security, BrokerTransportSecurity::RequireTls);
    if servers.len() == 1 && !require_tls {
        async_nats::connect(servers[0]).await.map_err(|e| {
            PhotonError::caused(format!("nats connect {}", redact_endpoint(servers[0])), e)
        })
    } else {
        let mut opts = async_nats::ConnectOptions::new().retry_on_initial_connect();
        if require_tls {
            opts = opts.require_tls(true);
        }
        opts.connect(servers)
            .await
            .map_err(|e| PhotonError::caused(format!("nats connect {}", redact_endpoint(urls)), e))
    }
}

//! NATS client connection helpers.

use photon_backend::{map_broker_connect_err, BrokerTransportSecurity, PhotonError, Result};

/// Connect to one or more NATS servers (`PHOTON_NATS_URL` may be comma-separated).
///
/// When `credentials_file` is set, loads a NATS `.creds` file (JWT + `NKey`) instead of URL userinfo.
///
/// # Errors
///
/// Returns an error when the security policy rejects the endpoint, credentials cannot be loaded,
/// or connection fails. Error labels redact URL userinfo via [`map_broker_connect_err`].
pub async fn connect_nats(
    urls: &str,
    security: BrokerTransportSecurity,
    credentials_file: Option<&str>,
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
    let mut opts = async_nats::ConnectOptions::new().retry_on_initial_connect();
    if require_tls {
        opts = opts.require_tls(true);
    }
    if let Some(path) = credentials_file {
        opts = opts
            .credentials_file(path)
            .await
            .map_err(|e| map_broker_connect_err("nats credentials file", path, e))?;
    }
    opts.connect(servers)
        .await
        .map_err(|e| map_broker_connect_err("nats connect", urls, e))
}

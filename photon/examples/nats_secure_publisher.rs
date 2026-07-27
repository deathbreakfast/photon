//! Secure brokered publisher — NATS `JetStream` over TLS with credentials-file auth.
//!
//! Production hosts **must** use TLS (`tls://…`) and a NATS `.creds` file — this example
//! never sets `PHOTON_ALLOW_INSECURE_BROKER`. It demonstrates the `.require_tls()` +
//! `.credentials_file` / `PHOTON_NATS_CREDS` builder pattern from
//! [`NatsStoragePortBuilder`](https://docs.rs/photon-backend-nats/latest/photon_backend_nats/struct.NatsStoragePortBuilder.html).
//! For the local plaintext dev lab, see `nats_publisher` / `nats_worker` instead.
//!
//! Interdependent with `nats_secure_worker`. Start the worker first so subscriptions are ready.
//!
//! ```bash
//! export PHOTON_TRANSPORT_KEY=cGhvdG9uLWRldi10cmFuc3BvcnQta2V5LTMyYnl0ZXM=
//! export PHOTON_NATS_URL=tls://nats.example.internal:4222
//! export PHOTON_NATS_STREAM=photon
//! export PHOTON_NATS_CREDS=/run/secrets/photon-nats.creds
//! cargo run -p uf-photon --example nats_secure_publisher --features runtime,nats
//! ```
//!
//! No TLS broker on hand? This example checks `PHOTON_NATS_URL` up front and — instead of
//! falling back to plaintext — prints the runbook above and exits cleanly. See
//! `photon/README.md` § How to run examples for the full production checklist.
#![allow(missing_docs)]
#![allow(clippy::unused_async, clippy::used_underscore_binding)]
#![allow(clippy::unwrap_used, clippy::expect_used)]
use std::sync::Arc;

use photon::{topic, NatsStoragePort, Photon, ReplayCursor};

#[topic(name = "examples.brokered.secure.greeting", keyed_by = "name")]
pub struct GreetingSent {
    pub name: String,
    pub message: String,
}

/// Require a `tls://` URL up front so this example never silently connects over plaintext.
fn require_tls_url() -> Option<String> {
    let url = std::env::var("PHOTON_NATS_URL").ok()?;
    url.trim()
        .to_ascii_lowercase()
        .starts_with("tls://")
        .then_some(url)
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt().with_env_filter("info").init();

    let Some(url) = require_tls_url() else {
        tracing::warn!(
            "nats_secure_publisher: set PHOTON_NATS_URL to a tls:// endpoint and PHOTON_NATS_CREDS \
             to a valid .creds file to run this example against a real TLS broker. Never set \
             PHOTON_ALLOW_INSECURE_BROKER here — see photon/README.md § How to run examples. \
             Skipping connect."
        );
        return Ok(());
    };

    let port = Arc::new(
        NatsStoragePort::builder()
            .from_env_defaults()
            .url(url)
            .require_tls()
            .replay_cursor(ReplayCursor::StreamSeq)
            .sync_ack(true)
            .build()
            .await?,
    );
    let photon = Photon::builder()
        .storage_port(port)
        .auto_registry()
        .build()?;

    let event_id = GreetingSent {
        name: "world".into(),
        message: "hello from nats_secure_publisher example".into(),
    }
    .publish_on(&photon)
    .await?;

    tracing::info!(event_id = %event_id, "nats_secure_publisher: published over TLS");
    Ok(())
}

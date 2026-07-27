//! Secure brokered worker — `#[subscribe]` + `start_executor` over TLS with credentials-file auth.
//!
//! Production hosts **must** use TLS (`tls://…`) and a NATS `.creds` file — this example never
//! sets `PHOTON_ALLOW_INSECURE_BROKER`. It demonstrates the `.require_tls()` +
//! `.credentials_file` / `PHOTON_NATS_CREDS` builder pattern. For the local plaintext dev lab,
//! see `nats_worker` / `nats_publisher` instead.
//!
//! Interdependent with `nats_secure_publisher`. Start the broker and this worker **before** the
//! publisher.
//!
//! ```bash
//! export PHOTON_TRANSPORT_KEY=cGhvdG9uLWRldi10cmFuc3BvcnQta2V5LTMyYnl0ZXM=
//! export PHOTON_NATS_URL=tls://nats.example.internal:4222
//! export PHOTON_NATS_STREAM=photon
//! export PHOTON_NATS_CREDS=/run/secrets/photon-nats.creds
//! cargo run -p uf-photon --example nats_secure_worker --features runtime,nats
//! ```
//!
//! No TLS broker on hand? This example checks `PHOTON_NATS_URL` up front and — instead of
//! falling back to plaintext — prints the runbook above and exits cleanly. See
//! `photon/README.md` § How to run examples for the full production checklist.
//!
//! Stop with Ctrl-C.
#![allow(missing_docs)]
#![allow(clippy::unused_async, clippy::used_underscore_binding)]
#![allow(clippy::unwrap_used, clippy::expect_used)]
use std::sync::Arc;

use photon::{subscribe, topic, Actor, JsonIdentityFactory, NatsStoragePort, Photon, ReplayCursor};

#[topic(name = "examples.brokered.secure.greeting", keyed_by = "name")]
pub struct GreetingSent {
    pub name: String,
    pub message: String,
}

#[subscribe(
    topic = "examples.brokered.secure.greeting",
    durable = "nats-secure-logger"
)]
async fn on_greeting(_actor: Box<dyn Actor>, event: GreetingSent) -> photon::Result<()> {
    tracing::info!(name = %event.name, message = %event.message, "worker received greeting");
    Ok(())
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
            "nats_secure_worker: set PHOTON_NATS_URL to a tls:// endpoint and PHOTON_NATS_CREDS \
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
    photon.start_executor(Arc::new(JsonIdentityFactory))?;

    tracing::info!("nats_secure_worker: executor running over TLS; waiting for Ctrl-C");
    tokio::signal::ctrl_c().await?;
    tracing::info!("nats_secure_worker: shutting down");
    Ok(())
}

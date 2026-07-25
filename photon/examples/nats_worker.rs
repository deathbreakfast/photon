//! Brokered worker — `#[subscribe]` + `start_executor` against shared NATS `JetStream`.
//!
//! Interdependent with `nats_publisher`. Start the broker and this worker **before** the publisher.
//!
//! ```bash
//! export PHOTON_TRANSPORT_KEY=cGhvdG9uLWRldi10cmFuc3BvcnQta2V5LTMyYnl0ZXM=
//! export PHOTON_NATS_URL=nats://127.0.0.1:4222
//! export PHOTON_NATS_STREAM=photon
//! export PHOTON_ALLOW_INSECURE_BROKER=1   # local plaintext only
//! cargo run -p uf-photon --example nats_worker --features runtime,nats
//! ```
//!
//! Stop with Ctrl-C. Production hosts must use TLS + credentials (never
//! `PHOTON_ALLOW_INSECURE_BROKER`). See `photon/README.md` § How to run examples.
#![allow(missing_docs)]
#![allow(clippy::unused_async, clippy::used_underscore_binding)]
#![allow(clippy::unwrap_used, clippy::expect_used)]
use std::sync::Arc;

use photon::{subscribe, topic, Actor, JsonIdentityFactory, NatsStoragePort, Photon, ReplayCursor};

#[topic(name = "examples.brokered.greeting", keyed_by = "name")]
pub struct GreetingSent {
    pub name: String,
    pub message: String,
}

#[subscribe(topic = "examples.brokered.greeting", durable = "nats-logger")]
async fn on_greeting(_actor: Box<dyn Actor>, event: GreetingSent) -> photon::Result<()> {
    tracing::info!(name = %event.name, message = %event.message, "worker received greeting");
    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt().with_env_filter("info").init();

    let port = Arc::new(
        NatsStoragePort::builder()
            .from_env_defaults()
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

    tracing::info!("nats_worker: executor running; waiting for Ctrl-C");
    tokio::signal::ctrl_c().await?;
    tracing::info!("nats_worker: shutting down");
    Ok(())
}

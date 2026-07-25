//! Brokered publisher — publish only against shared NATS `JetStream` (no executor).
//!
//! Interdependent with `nats_worker`. Start the worker first so subscriptions are ready.
//!
//! ```bash
//! export PHOTON_TRANSPORT_KEY=cGhvdG9uLWRldi10cmFuc3BvcnQta2V5LTMyYnl0ZXM=
//! export PHOTON_NATS_URL=nats://127.0.0.1:4222
//! export PHOTON_NATS_STREAM=photon
//! export PHOTON_ALLOW_INSECURE_BROKER=1   # local plaintext only
//! cargo run -p uf-photon --example nats_publisher --features runtime,nats
//! ```
//!
//! See `photon/README.md` § How to run examples.
#![allow(missing_docs)]
#![allow(clippy::unused_async, clippy::used_underscore_binding)]
#![allow(clippy::unwrap_used, clippy::expect_used)]
use std::sync::Arc;

use photon::{topic, NatsStoragePort, Photon, ReplayCursor};

#[topic(name = "examples.brokered.greeting", keyed_by = "name")]
pub struct GreetingSent {
    pub name: String,
    pub message: String,
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

    let event_id = GreetingSent {
        name: "world".into(),
        message: "hello from nats_publisher example".into(),
    }
    .publish_on(&photon)
    .await?;

    tracing::info!(event_id = %event_id, "nats_publisher: published");
    Ok(())
}

//! Brokered publisher — publish only against shared Fluvio (no executor).
//!
//! Interdependent with `fluvio_worker`. Start the worker first so subscriptions are ready.
//! Same brokered topology as `nats_publisher`; swap the storage port builder for Fluvio.
//!
//! ```bash
//! export PHOTON_TRANSPORT_KEY=cGhvdG9uLWRldi10cmFuc3BvcnQta2V5LTMyYnl0ZXM=
//! export PHOTON_FLUVIO_ENDPOINT=127.0.0.1:9103
//! export PHOTON_ALLOW_INSECURE_BROKER=1   # local plaintext only
//! cargo run -p uf-photon --example fluvio_publisher --features runtime,fluvio
//! ```
//!
//! Local single-node Fluvio lab: `infra/broker/scripts/fluvio-single.sh` +
//! `infra/broker/scripts/export-fluvio-env.sh`. See `photon/README.md` § How to run examples.
#![allow(missing_docs)]
#![allow(clippy::unused_async, clippy::used_underscore_binding)]
#![allow(clippy::unwrap_used, clippy::expect_used)]
use std::sync::Arc;

use photon::{topic, FluvioReplayCursor, FluvioStoragePort, Photon};

#[topic(name = "examples.brokered.fluvio.greeting", keyed_by = "name")]
pub struct GreetingSent {
    pub name: String,
    pub message: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt().with_env_filter("info").init();

    let port = Arc::new(
        FluvioStoragePort::builder()
            .from_env_defaults()
            .replay_cursor(FluvioReplayCursor::StreamSeq)
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
        message: "hello from fluvio_publisher example".into(),
    }
    .publish_on(&photon)
    .await?;

    tracing::info!(event_id = %event_id, "fluvio_publisher: published");
    Ok(())
}

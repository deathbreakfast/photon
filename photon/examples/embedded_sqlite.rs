//! Embedded durable Photon — same host path as `embedded_mem`, file-backed `SQLite`.
//!
//! Run: `cargo run -p uf-photon --example embedded_sqlite --features runtime,sqlite`
//!
//! Requires `PHOTON_TRANSPORT_KEY` (base64-encoded 32-byte key). Smoke / CI use
//! `cGhvdG9uLWRldi10cmFuc3BvcnQta2V5LTMyYnl0ZXM=` (dev key). See `docs/configuration.md`.
//!
//! Optional: `PHOTON_SQLITE_PATH` (default `/tmp/photon-example.db`).
#![allow(missing_docs)]
#![allow(clippy::unused_async, clippy::used_underscore_binding)]
#![allow(clippy::unwrap_used, clippy::expect_used)]
use std::sync::Arc;

use photon::{subscribe, topic, Actor, JsonIdentityFactory, Photon, SqliteStoragePort};

#[topic(name = "examples.greeting", keyed_by = "name")]
pub struct GreetingSent {
    pub name: String,
    pub message: String,
}

#[subscribe(topic = "examples.greeting", durable = "logger")]
async fn on_greeting(_actor: Box<dyn Actor>, event: GreetingSent) -> photon::Result<()> {
    tracing::info!(name = %event.name, message = %event.message, "received greeting");
    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt().with_env_filter("info").init();

    let path =
        std::env::var("PHOTON_SQLITE_PATH").unwrap_or_else(|_| "/tmp/photon-example.db".into());
    let port = Arc::new(SqliteStoragePort::open(&path).await?);
    let photon = Photon::builder()
        .storage_port(port)
        .auto_registry()
        .build()?;
    photon.start_executor(Arc::new(JsonIdentityFactory))?;

    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let event_id = GreetingSent {
        name: "world".into(),
        message: "hello from embedded_sqlite example".into(),
    }
    .publish_on(&photon)
    .await?;

    tracing::info!(event_id = %event_id, path = %path, "published");
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    Ok(())
}

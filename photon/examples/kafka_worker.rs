//! Brokered worker — `#[subscribe]` + `start_executor` against shared Kafka.
//!
//! Interdependent with `kafka_publisher`. Start the broker and this worker **before** the
//! publisher. Same brokered topology as `nats_worker`; swap the storage port builder for Kafka.
//!
//! ```bash
//! export PHOTON_TRANSPORT_KEY=cGhvdG9uLWRldi10cmFuc3BvcnQta2V5LTMyYnl0ZXM=
//! export PHOTON_KAFKA_BROKERS=127.0.0.1:9092
//! export PHOTON_ALLOW_INSECURE_BROKER=1   # local plaintext only
//! cargo run -p uf-photon --example kafka_worker --features runtime,kafka
//! ```
//!
//! Local single-node Kafka lab: `infra/broker/scripts/kafka-single.sh` +
//! `infra/broker/scripts/export-kafka-env.sh`.
//!
//! Stop with Ctrl-C. Production hosts must use TLS + credentials (never
//! `PHOTON_ALLOW_INSECURE_BROKER`). See `photon/README.md` § How to run examples.
#![allow(missing_docs)]
#![allow(clippy::unused_async, clippy::used_underscore_binding)]
#![allow(clippy::unwrap_used, clippy::expect_used)]
use std::sync::Arc;

use photon::{
    subscribe, topic, Actor, JsonIdentityFactory, KafkaReplayCursor, KafkaStoragePort, Photon,
};

#[topic(name = "examples.brokered.kafka.greeting", keyed_by = "name")]
pub struct GreetingSent {
    pub name: String,
    pub message: String,
}

#[subscribe(topic = "examples.brokered.kafka.greeting", durable = "kafka-logger")]
async fn on_greeting(_actor: Box<dyn Actor>, event: GreetingSent) -> photon::Result<()> {
    tracing::info!(name = %event.name, message = %event.message, "worker received greeting");
    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt().with_env_filter("info").init();

    let port = Arc::new(
        KafkaStoragePort::builder()
            .from_env_defaults()
            .replay_cursor(KafkaReplayCursor::StreamSeq)
            .sync_ack(true)
            .build()
            .await?,
    );
    let photon = Photon::builder()
        .storage_port(port)
        .auto_registry()
        .build()?;
    photon.start_executor(Arc::new(JsonIdentityFactory))?;

    tracing::info!("kafka_worker: executor running; waiting for Ctrl-C");
    tokio::signal::ctrl_c().await?;
    tracing::info!("kafka_worker: shutting down");
    Ok(())
}

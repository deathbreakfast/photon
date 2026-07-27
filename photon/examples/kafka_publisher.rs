//! Brokered publisher — publish only against shared Kafka (no executor).
//!
//! Interdependent with `kafka_worker`. Start the worker first so subscriptions are ready.
//! Same brokered topology as `nats_publisher`; swap the storage port builder for Kafka.
//!
//! ```bash
//! export PHOTON_TRANSPORT_KEY=cGhvdG9uLWRldi10cmFuc3BvcnQta2V5LTMyYnl0ZXM=
//! export PHOTON_KAFKA_BROKERS=127.0.0.1:9092
//! export PHOTON_ALLOW_INSECURE_BROKER=1   # local plaintext only
//! cargo run -p uf-photon --example kafka_publisher --features runtime,kafka
//! ```
//!
//! Local single-node Kafka lab: `infra/broker/scripts/kafka-single.sh` +
//! `infra/broker/scripts/export-kafka-env.sh`. See `photon/README.md` § How to run examples.
#![allow(missing_docs)]
#![allow(clippy::unused_async, clippy::used_underscore_binding)]
#![allow(clippy::unwrap_used, clippy::expect_used)]
use std::sync::Arc;

use photon::{topic, KafkaReplayCursor, KafkaStoragePort, Photon};

#[topic(name = "examples.brokered.kafka.greeting", keyed_by = "name")]
pub struct GreetingSent {
    pub name: String,
    pub message: String,
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

    let event_id = GreetingSent {
        name: "world".into(),
        message: "hello from kafka_publisher example".into(),
    }
    .publish_on(&photon)
    .await?;

    tracing::info!(event_id = %event_id, "kafka_publisher: published");
    Ok(())
}

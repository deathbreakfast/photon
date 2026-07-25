//! Integration tests: checkpoint commits never move backward.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::Arc;

use photon_backend::{InProcStoragePort, StoragePort, TransportCrypto};

const TOPIC: &str = "test.checkpoint.regression";

#[tokio::test]
async fn checkpoint_commit_advances_to_higher_sequence_on_mem() {
    let port: Arc<dyn StoragePort> = Arc::new(InProcStoragePort::new(TransportCrypto::from_bytes(
        *b"photon-dev-transport-key-32bytes",
    )));

    port.commit_checkpoint("sub-a", TOPIC, None, 5)
        .await
        .expect("commit first checkpoint");
    port.commit_checkpoint("sub-a", TOPIC, None, 10)
        .await
        .expect("advance checkpoint");

    let loaded = port
        .load_checkpoint("sub-a", TOPIC, None)
        .await
        .expect("load checkpoint");
    assert_eq!(loaded, Some(10));
}

#[tokio::test]
async fn checkpoint_commit_does_not_regress_to_lower_sequence_on_mem() {
    let port: Arc<dyn StoragePort> = Arc::new(InProcStoragePort::new(TransportCrypto::from_bytes(
        *b"photon-dev-transport-key-32bytes",
    )));

    port.commit_checkpoint("sub-a", TOPIC, None, 10)
        .await
        .expect("commit high watermark");
    port.commit_checkpoint("sub-a", TOPIC, None, 5)
        .await
        .expect("commit regressive checkpoint");

    let loaded = port
        .load_checkpoint("sub-a", TOPIC, None)
        .await
        .expect("load checkpoint");
    assert_eq!(loaded, Some(10));
}

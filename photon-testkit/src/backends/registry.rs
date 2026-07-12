//! Harness storage port install fns (bench/e2e matrix).

use std::sync::Arc;

use anyhow::{bail, Result};
use photon_backend::{InProcStoragePort, StoragePort, TransportCrypto};

use crate::matrix::StorageAdapter;

/// Install a sync storage port (`mem` only).
///
/// # Errors
///
/// Returns an error if the adapter is unsupported synchronously.
pub fn install_storage_port(adapter: StorageAdapter) -> Result<Arc<dyn StoragePort>> {
    match adapter {
        StorageAdapter::Mem => Ok(Arc::new(InProcStoragePort::new(
            TransportCrypto::from_bytes(*b"photon-dev-transport-key-32bytes"),
        ))),
        StorageAdapter::Nats | StorageAdapter::Fluvio | StorageAdapter::Kafka => {
            bail!("{adapter:?} requires install_storage_port_async")
        }
        StorageAdapter::Sqlite => {
            bail!("sqlite requires install_storage_port_async")
        }
    }
}

/// Install a storage port, connecting to external brokers when required.
///
/// # Errors
///
/// Returns an error if env or connection fails.
pub async fn install_storage_port_async(adapter: StorageAdapter) -> Result<Arc<dyn StoragePort>> {
    match adapter {
        StorageAdapter::Mem => install_storage_port(adapter),
        StorageAdapter::Nats => {
            #[cfg(feature = "nats")]
            {
                Ok(Arc::new(
                    photon_backend_nats::NatsStoragePort::from_env().await?,
                ))
            }
            #[cfg(not(feature = "nats"))]
            {
                let _ = adapter;
                bail!("nats storage requires photon-testkit/nats feature")
            }
        }
        StorageAdapter::Fluvio => {
            #[cfg(feature = "fluvio")]
            {
                Ok(Arc::new(
                    photon_backend_fluvio::FluvioStoragePort::from_env().await?,
                ))
            }
            #[cfg(not(feature = "fluvio"))]
            {
                let _ = adapter;
                bail!("fluvio storage requires photon-testkit/fluvio feature")
            }
        }
        StorageAdapter::Kafka => {
            #[cfg(feature = "kafka")]
            {
                Ok(Arc::new(
                    photon_backend_kafka::KafkaStoragePort::from_env().await?,
                ))
            }
            #[cfg(not(feature = "kafka"))]
            {
                let _ = adapter;
                bail!("kafka storage requires photon-testkit/kafka feature")
            }
        }
        StorageAdapter::Sqlite => {
            #[cfg(feature = "sqlite")]
            {
                let path = std::env::var(photon_backend_sqlite::PATH_ENV).unwrap_or_else(|_| {
                    std::env::temp_dir()
                        .join(format!("photon-testkit-{}.db", std::process::id()))
                        .to_string_lossy()
                        .into_owned()
                });
                Ok(Arc::new(
                    photon_backend_sqlite::SqliteStoragePort::open(&path).await?,
                ))
            }
            #[cfg(not(feature = "sqlite"))]
            {
                let _ = adapter;
                bail!("sqlite storage requires photon-testkit/sqlite feature")
            }
        }
    }
}

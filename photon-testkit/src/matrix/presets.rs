//! CI and campaign matrix presets.

use super::{MatrixSpec, StorageAdapter, TelemetryAdapter, Topology};

impl MatrixSpec {
    /// CI default slice (fast, in-process mem).
    #[must_use]
    pub fn ci_mem_embedded() -> Self {
        Self::default()
    }

    /// Embedded `SQLite` durable storage preset.
    #[must_use]
    pub fn ci_sqlite_embedded() -> Self {
        Self {
            storage: StorageAdapter::Sqlite,
            ..Self::default()
        }
    }

    /// Alias for [`Self::ci_mem_embedded`].
    #[must_use]
    pub fn ci_mem() -> Self {
        Self::default()
    }

    /// Broker cluster topology preset (NATS lab).
    #[must_use]
    pub fn ci_nats_broker() -> Self {
        Self {
            storage: StorageAdapter::Nats,
            topology: Topology::BrokerCluster,
            ..Self::default()
        }
    }

    /// Broker cluster topology preset (Kafka lab).
    #[must_use]
    pub fn ci_kafka_broker() -> Self {
        Self {
            storage: StorageAdapter::Kafka,
            topology: Topology::BrokerCluster,
            ..Self::default()
        }
    }

    /// Broker cluster topology preset (Fluvio lab).
    #[must_use]
    pub fn ci_fluvio_broker() -> Self {
        Self {
            storage: StorageAdapter::Fluvio,
            topology: Topology::BrokerCluster,
            ..Self::default()
        }
    }

    /// Override topology while keeping other dimensions.
    #[must_use]
    pub const fn with_topology(mut self, topology: Topology) -> Self {
        self.topology = topology;
        self
    }

    /// Override telemetry while keeping other dimensions.
    #[must_use]
    pub const fn with_telemetry(mut self, telemetry: TelemetryAdapter) -> Self {
        self.telemetry = telemetry;
        self
    }

    /// Override storage adapter.
    #[must_use]
    pub const fn with_storage(mut self, storage: StorageAdapter) -> Self {
        self.storage = storage;
        self
    }
}

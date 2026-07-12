# photon-backend-kafka

Apache Kafka [`StoragePort`](../photon-backend/src/storage/port.rs) adapter.

Enable on the facade with `features = ["runtime", "kafka"]`. Wiring example: [`photon/README.md`](../photon/README.md).

Configuration: [`KafkaStoragePortBuilder`](https://docs.rs/photon-backend-kafka/latest/photon_backend_kafka/struct.KafkaStoragePortBuilder.html) (options + example). Index: [docs.rs `photon::config`](https://docs.rs/uf-photon/latest/photon/config/#storage-adapter-builders).

Fleet runbook: [`infra/aws/kafka-fleet/README.md`](../infra/aws/kafka-fleet/README.md).

# photon-backend-nats

NATS JetStream [`StoragePort`](../photon-backend/src/storage/port.rs) adapter.

Enable on the facade with `features = ["runtime", "nats"]`. Wiring example: [`photon/README.md`](../photon/README.md) (NATS section).

Configuration: [`NatsStoragePortBuilder`](https://docs.rs/photon-backend-nats/latest/photon_backend_nats/struct.NatsStoragePortBuilder.html) (options + example). Index: [docs.rs `photon::config`](https://docs.rs/photon/latest/photon/config/#storage-adapter-builders).

Local broker: [`infra/broker/README.md`](../infra/broker/README.md).

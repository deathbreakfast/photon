# photon-backend-nats

NATS JetStream [`StoragePort`](../photon-backend/src/storage/port.rs) adapter.

Enable on the public crate with `features = ["runtime", "nats"]`. Topology:
[Brokered](https://docs.rs/uf-photon/latest/photon/#brokered-publisher--worker-binaries).

Runnable (multi-terminal): `cargo run -p uf-photon --example nats_worker --features runtime,nats` then
`nats_publisher` — runbook in [`photon/README.md`](../photon/README.md#how-to-run-examples).

Configuration: [`NatsStoragePortBuilder`](https://docs.rs/photon-backend-nats/latest/photon_backend_nats/struct.NatsStoragePortBuilder.html) (options + example). Index: [docs.rs `photon::config`](https://docs.rs/uf-photon/latest/photon/config/#storage-adapter-builders).

Local broker: [`infra/broker/README.md`](../infra/broker/README.md).

# photon-backend

Transport log, [`PhotonBackend`](src/backend/photon_backend.rs) trait, and delivery glue.

## Exports

- `PhotonBackend`, [`EmbeddedBackend`](src/backend/embedded.rs), [`BackendContext`](src/backend/context.rs)
- `StoragePort`, `GenericPhotonBackend`, `InProcStoragePort`
- `transport`, `delivery`, `checkpoint`, `handler_registry`

Multi-node delivery: broker-native via [`photon-backend-nats`](../photon-backend-nats/), [`photon-backend-fluvio`](../photon-backend-fluvio/), [`photon-backend-kafka`](../photon-backend-kafka/).
Durable single-process: [`photon-backend-sqlite`](../photon-backend-sqlite/).

## Tasks

- **Implement `StoragePort` / `PhotonBackend`** — custom adapters; install via `BackendContext`
- **Wire a storage port** — pass `Arc<dyn StoragePort>` to [`PhotonBuilder::storage_port`](../photon-runtime/src/builder.rs) (see [`photon-runtime`](../photon-runtime/))

API reference: `cargo doc -p photon --features runtime,mem --open` → [Developing the backend](https://docs.rs/photon/latest/photon/index.html#developing-the-backend).

# photon-backend-fluvio

Fluvio [`StoragePort`](../photon-backend/src/storage/port.rs) adapter.

Enable on the facade with `features = ["runtime", "fluvio"]`. Wiring example: [`photon/README.md`](../photon/README.md).

Configuration: [`FluvioStoragePortBuilder`](https://docs.rs/photon-backend-fluvio/latest/photon_backend_fluvio/struct.FluvioStoragePortBuilder.html) (options + example). Index: [docs.rs `photon::config`](https://docs.rs/uf-photon/latest/photon/config/#storage-adapter-builders).

Fleet runbook: [`infra/aws/fluvio-fleet/README.md`](../infra/aws/fluvio-fleet/README.md).

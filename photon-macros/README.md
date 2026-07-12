# photon-macros

Proc macros for typed Photon topics and handlers.

## Entry points

| Macro | Purpose |
|-------|---------|
| [`topic`](src/lib.rs) | Register a typed topic struct; generates `publish_on` / `publish`, `subscribe_on` / `subscribe` |
| [`subscribe`](src/lib.rs) | Register an async handler for executor dispatch |

Prefer **handle-first** APIs (`publish_on(&photon)`, `subscribe_on(&photon, opts)`). `configure` + `.publish()` / `.subscribe()` remain optional sugar for a process-wide default.

Requires `#[photon::topic]` and `Photon::start_executor` at boot for inventory handlers. Attribute reference: [docs.rs `photon::config` — proc macros](https://docs.rs/photon/latest/photon/config/#proc-macro-attributes). Expansion behavior: [`docs/macro-expansion.md`](../docs/macro-expansion.md).

Runnable examples: `cargo run -p photon --example embedded_mem --features runtime,mem` (set `PHOTON_TRANSPORT_KEY`; see example comments).

API reference: `cargo doc -p photon-macros --open`.

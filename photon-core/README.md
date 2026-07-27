# photon-core

Identity port and shared types — **no delivery topology**. Handler signatures use [`IdentityFactory`](src/identity.rs) and [`Actor`](src/identity.rs); hosts inject identity at `start_executor`.

crates.io package: **`uf-photon-core`** (Rust crate name remains `photon_core`):

```toml
photon-core = { package = "uf-photon-core", version = "0.1.1" }
```

## Exports

- [`IdentityFactory`](src/identity.rs), [`Actor`](src/identity.rs), [`IdentityError`](src/error.rs)
- [`JsonIdentityFactory`](src/stub_identity.rs) / [`JsonActor`](src/stub_identity.rs) — test/dev stubs

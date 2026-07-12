# Contributing to Photon

## Documentation

When you change public API behavior, configuration defaults, or wiring steps:

1. Update rustdoc on the affected symbols (workspace enforces `missing_docs = "deny"`).
2. Update [`docs/configuration.md`](docs/configuration.md) when env vars, macro attributes, or builder options change (renders on [docs.rs `photon::config`](https://docs.rs/uf-photon/latest/photon/config/)).
3. Add or update a runnable example under [`photon/examples/`](photon/examples/) when introducing a new user-facing workflow.
4. Run the verification block in [`docs/VERIFICATION.md`](docs/VERIFICATION.md) before opening a PR.

### Style

- Organize facade docs by **task** (boot, topics, backend development), not reader personas.
- Put full code snippets on the item that owns the API; the facade documentation map links without duplicating.
- Use `# Contract` subsections on trait methods for semantics (empty input, monotonicity, no-op defaults).
- Backend adapter crate docs should link to their `*StoragePortBuilder` rustdoc; cross-cutting env vars stay in [`photon::config`](https://docs.rs/uf-photon/latest/photon/config/index.html).

## Verification

```bash
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps --all-features
cargo test -p uf-photon --doc --features runtime,mem
cargo test -p photon-runtime --doc --features runtime,mem
cargo test -p photon-macros --doc
cargo test -p photon-backend --doc --features runtime
cargo test -p photon-e2e
```

See [`.github/workflows/ci.yml`](.github/workflows/ci.yml) for the full CI matrix.

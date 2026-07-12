# Supply chain policy

Photon pins Quark via a Git revision in the workspace `Cargo.toml` (`quark` → `unified-field-dev/quark`). That pin is intentional until Quark publishes a crates.io release that Photon can depend on.

## Rules

1. **Prefer crates.io** for all other dependencies.
2. **New Git dependencies** require:
   - An entry in [`deny.toml`](../deny.toml) `[sources].allow-git`
   - A short note in this file (why Git, which rev, migration plan)
3. **CI** runs `cargo deny check` on every PR (advisories, licenses, sources).
4. **Ignored advisories** in [`deny.toml`](../deny.toml) must cite the transitive crate and a removal trigger (e.g. bump `async-nats` off `rustls-webpki` 0.102).

## Security-sensitive configuration

Transport encryption keys and development opt-ins are documented under
[configuration.md](configuration.md#security-sensitive-variables).

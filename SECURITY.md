# Security Policy

## Supported versions

Security fixes are accepted against the latest published release line of this repository's crates (`uf-photon`, `photon-backend`, `photon-runtime`, and related workspace members).

## Reporting a vulnerability

Please **do not** open a public GitHub issue for security-sensitive reports.

Prefer one of the following:

1. **GitHub Security Advisories** — use the repository's private vulnerability reporting flow when available.
2. Contact the maintainers privately via the repository owner.

Include:

- a description of the issue and its impact
- steps to reproduce or a proof of concept when possible
- affected crate names and versions

We will acknowledge receipt as soon as practical and coordinate a fix and disclosure timeline with you.

## Threat model (library)

Photon is an **embeddable trusted-host** messaging library. Holding a `Photon` handle is fully privileged: the core does **not** enforce session authn, topic ACLs, or multi-tenant isolation.

### Host responsibilities

Before exposing any user-reachable surface (HTTP, WebSocket, RPC, admin, jobs):

1. Authenticate callers and authorize publish, subscribe, replay, `get_event`, checkpoint, retention, and admin independently.
2. Do not map untrusted client topic/key/payload inputs directly onto raw `Photon` APIs without a policy boundary.
3. Do not use `JsonIdentityFactory` in production; supply an `IdentityFactory` that reconstructs identity from trusted server-side context.
4. Configure broker TLS and credentials explicitly; treat plaintext broker URLs as development-only.
5. Keep `PHOTON_TRANSPORT_KEY` in a secret manager; never set `PHOTON_ALLOW_DEV_TRANSPORT_KEY` in production.
6. Protect admin snapshot and ops-log sinks; avoid console telemetry for sensitive workloads.

### What Photon provides

- Transport envelope cryptography for actor/payload at rest and on the wire (when adapters seal correctly).
- Durable subscriptions, checkpoints, and pluggable storage adapters.
- Integration points for host identity and telemetry.

### Related

Browser WebSocket Origin policy and cookie-authenticated routes are documented in the **photon-leptos** / **photon-axum** crates (host must override Origin allowlisting for production).

## Scope

In scope: vulnerabilities in this repository's published crates and documentation that could cause unsafe production defaults, plus CI/supply-chain issues in this repository.

Out of scope: product-layer linking/authz (implemented by host applications such as UF Photon), and vulnerabilities solely in third-party dependencies unless this project mishandles them in a security-relevant way.

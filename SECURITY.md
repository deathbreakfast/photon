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

## Production checklist for implementers

Photon is an **embeddable trusted-host** messaging library. Holding a `Photon` handle is fully privileged: the core does **not** enforce session authn, topic ACLs, or multi-tenant isolation. Hosts supply session/JWT auth, topic ACLs, tenant binding, CSRF, CORS/security headers, TLS at the HTTP edge, broker ACLs, and product authorization.

Browser WebSocket Origin policy lives in **photon-leptos** / **photon-axum** (default rejects all Origins; hosts allowlist).

### Secrets and crypto

| Check | How |
|-------|-----|
| Set a real transport key | `PHOTON_TRANSPORT_KEY` = base64 encoding of **32 random bytes**, from a secret manager |
| Never enable the dev key | Do **not** set `PHOTON_ALLOW_DEV_TRANSPORT_KEY` |
| Rotate by redeploy | Key rotation is host-owned; plan dual-running cutovers outside Photon core |
| Protect SQLite files | Explicit durable path (`PHOTON_SQLITE_PATH` / builder); encrypt or ACL the volume |

Example (fail-closed boot):

```bash
export PHOTON_TRANSPORT_KEY="$(openssl rand -base64 32)"
# unset PHOTON_ALLOW_DEV_TRANSPORT_KEY
```

### Identity and authorization (host edge)

| Check | How |
|-------|-----|
| Authenticate every user-reachable surface | HTTP, WebSocket, RPC, admin, jobs |
| Authorize each Photon capability independently | publish, subscribe, replay, `get_event`, checkpoint, retention, admin |
| Do not map raw client input onto Photon APIs | Validate/allowlist topic, key, and payload at the host boundary |
| Production identity factory | Do **not** use `JsonIdentityFactory`; reconstruct identity from trusted server-side session/JWT context |
| Admin / DLQ / ops-log | Authz admin snapshot routes; avoid `ConsoleOpsLog` for sensitive workloads |
| Errors | DLQ/ops-log/admin error strings are sanitized/truncated; connect errors redact URL userinfo. Do not embed actor/payload JSON in handler errors |

Anti-pattern: accepting `?topic=` from the browser and calling `Photon::publish` / subscribe without a policy boundary.

### Broker transport (Brokered)

| Check | How |
|-------|-----|
| Require TLS | Default `BrokerTransportSecurity::RequireTls`; use `tls://` / SDK TLS endpoints |
| Never allow insecure in production | Do **not** set `PHOTON_ALLOW_INSECURE_BROKER`; do not call `.allow_insecure_plaintext()` |
| NATS credentials | Prefer `.credentials_file(...)` / `PHOTON_NATS_CREDS` over URL userinfo |
| Prefer tokens over URL passwords | Connect errors redact userinfo, but secrets in URLs still risk process listings and ops scrapers |
| Broker ACLs | Restrict publish/subscribe subjects to least privilege for each binary's identity |
| NATS retention | Applied as JetStream `max_age` when Photon creates streams |
| Fluvio retention | Applied as segment cleanup policy when Photon creates topics |
| Kafka retention | **Host must** pre-create topics with `retention.ms` (or set broker `log.retention.ms`). Photon exposes `photon_backend_kafka::retention_ms` and warns once at create; `rskafka` 0.6 cannot set topic configs |

NATS TLS + credentials sketch:

```rust,ignore
NatsStoragePort::builder()
    .url("tls://nats.example:4222")
    .credentials_file("/run/secrets/nats.creds")
    .require_tls()
    .build()
    .await?;
```

### Related HTTP / WebSocket hosts

If the process also serves browsers via **photon-axum** / **photon-leptos**:

1. Override `HasPhoton::allow_ws_origin` with your production Origin allowlist (crate default rejects all).
2. Set cookie flags (`Secure`, `HttpOnly`, `SameSite`) and a CSRF strategy for mutating server functions.
3. Do not deploy the E2E demo or bench server as a public app (they refuse non-loopback bind unless explicitly opted in).

See the photon-leptos `SECURITY.md` for Origin and demo/bench detail.

### Supply chain

| Check | How |
|-------|-----|
| Keep `cargo deny` green | CI runs advisories/licenses; do not re-add ignored Critical TLS advisories |
| Pin adapter versions deliberately | Broker SDK bumps can change TLS defaults |

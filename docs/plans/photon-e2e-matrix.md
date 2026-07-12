# Photon E2E Matrix Plan

Embedded SQLite storage adapter, full e2e scenario coverage, topology/telemetry in CI, and unified AWS validation.

## Hard constraint

**The dev laptop must never run `cargo build` / `cargo test`.** All validation runs on AWS EC2 via `infra/aws/*/scripts/`.

## Phases

1. **`photon-backend-sqlite`** — write-through SQLite + in-memory broadcast fanout
2. **Matrix wiring** — `StorageAdapter::Sqlite` through testkit, photon-e2e, photon-bench
3. **E2e scenarios** — 13 sqlite scenarios + topology/telemetry smokes promoted from `#[ignore]`
4. **`infra/aws/sqlite-smoke`** — t3.medium provision/bootstrap/remote smoke
5. **`infra/aws/scripts/run-all-e2e-aws.sh`** — orchestrate sqlite + kafka + fluvio + nats gates
6. **Docs** — STORAGE-ADAPTERS-DESIGN, configuration, ROADMAP, photon-e2e README

## AWS validation

```bash
cd infra/aws/sqlite-smoke
export AWS_KEY_NAME=your-key
export SSH_KEY_PATH=~/.ssh/your-key.pem
chmod +x provision.sh bootstrap.sh scripts/*.sh
./provision.sh && ./bootstrap.sh && ./scripts/run-remote-smoke.sh
./scripts/teardown.sh

# All backends (sqlite auto-provisions; others need instances.env)
./infra/aws/scripts/run-all-e2e-aws.sh
```

## Out of scope (v1)

Multi-process SQLite WAL, Postgres/Surreal, `shard_strategy` matrix wiring.

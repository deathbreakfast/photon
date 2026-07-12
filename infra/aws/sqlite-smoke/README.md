# SQLite smoke validation on AWS (t3.medium)

Minimal single-EC2 smoke gate for the SQLite storage adapter and E2E matrix. No external broker — only Rust toolchain on a `t3.medium`.

**All builds and tests run on EC2.** Do not run `cargo` on the dev laptop.

## Prerequisites

- AWS CLI configured (`aws sts get-caller-identity`)
- EC2 key pair: `export AWS_KEY_NAME=your-key`
- SSH key: `export SSH_KEY_PATH=~/.ssh/your-key.pem`

## Provision and run

```bash
cd infra/aws/sqlite-smoke
export AWS_KEY_NAME=your-key-name
export SSH_KEY_PATH=~/.ssh/your-key-name.pem

chmod +x provision.sh bootstrap.sh scripts/*.sh
./provision.sh
./bootstrap.sh
# Compile / clippy / doc / focused backend tests
./scripts/run-remote-check.sh
# Full E2E + sqlite contract smoke
./scripts/run-remote-smoke.sh
./scripts/teardown.sh
```

## Scripts

| Script | Purpose |
|--------|---------|
| `run-remote-check.sh` | rsync + `cargo check` / full-workspace clippy / doc / backend tests |
| `run-remote-ci.sh` | CI subset without live brokers (check, deny, clippy, testkit, e2e mem/sqlite, bench, examples, docs) |
| `run-remote-smoke.sh` | rsync + full mem/sqlite E2E + sqlite contract |
## Test matrix

| Layer | Count | Command (on EC2) |
|-------|-------|------------------|
| Mem E2E + topology/telemetry | 18 | `cargo test -p photon-e2e` + topology/telemetry filter |
| Sqlite E2E | 13 | `cargo test -p photon-e2e --features sqlite` |
| Sqlite contract | 3 | `cargo test -p photon-backend-sqlite --test sqlite_contract` |

## Unified orchestrator

Run all backend e2e gates (sqlite, kafka, fluvio, nats) via:

```bash
./infra/aws/scripts/run-all-e2e-aws.sh
```

See each smoke README for per-backend provisioning.

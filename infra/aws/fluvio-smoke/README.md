# Fluvio smoke validation on AWS (t3.medium)

Minimal single-EC2 smoke gate for the Fluvio storage adapter and E2E matrix. One `t3.medium` runs Fluvio (Docker) and executes the full ignored test suite co-located.

## Prerequisites

- AWS CLI configured (`aws sts get-caller-identity`)
- EC2 key pair: `export AWS_KEY_NAME=your-key`
- SSH key: `export SSH_KEY_PATH=~/.ssh/your-key.pem`

## Provision and run

```bash
cd infra/aws/fluvio-smoke
export AWS_KEY_NAME=your-key-name
export SSH_KEY_PATH=~/.ssh/your-key-name.pem

./provision.sh
./bootstrap.sh          # SSH: Docker + Fluvio + Rust toolchain
~/aws/photon-upstream/fluvio-smoke/run-remote-smoke.sh
./scripts/teardown.sh
```

## AWS MCP agent workflow

Agents can drive smoke via `plugin-aws-core-aws-mcp`:

1. `aws ec2 run-instances` — `t3.medium`, Ubuntu 22.04, SG with SSH + 9103
2. SSH bootstrap (`bootstrap.sh` user-data or remote)
3. Rsync repo to EC2
4. Run `scripts/run-e2e-smoke-aws.sh` on the instance
5. `aws ec2 terminate-instances` on pass/fail

## Test matrix (21 ignored live tests)

| Layer | Count | Command |
|-------|-------|---------|
| Mem E2E sanity | 13 | `cargo test -p photon-e2e` |
| Fluvio E2E broker | 12 | `cargo test -p photon-e2e --features fluvio -- --ignored` |
| Fluvio contract | 9 | `cargo test -p photon-backend-fluvio --test fluvio_contract -- --ignored` |

Replay/checkpoint scenarios use `PHOTON_FLUVIO_TOPIC_SHARDS=1` (NATS parity).

## Local alternative

```bash
cd infra/broker && ./scripts/fluvio-single.sh
source scripts/export-fluvio-env.sh
../../infra/aws/fluvio-smoke/scripts/run-e2e-smoke-aws.sh
```

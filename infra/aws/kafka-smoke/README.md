# Kafka smoke validation on AWS (t3.medium)

Minimal single-EC2 smoke gate for the Kafka storage adapter and E2E matrix. One `t3.medium` runs Kafka (Docker) and executes the full ignored test suite co-located.

## Prerequisites

- AWS CLI configured (`aws sts get-caller-identity`)
- EC2 key pair: `export AWS_KEY_NAME=your-key`
- SSH key: `export SSH_KEY_PATH=~/.ssh/your-key.pem`

## Provision and run

```bash
cd infra/aws/kafka-smoke
export AWS_KEY_NAME=your-key-name
export SSH_KEY_PATH=~/.ssh/your-key-name.pem

./provision.sh
./bootstrap.sh          # SSH: Docker + Kafka + Rust toolchain
~/aws/photon-upstream/kafka-smoke/run-remote-smoke.sh
./scripts/teardown.sh
```

## AWS MCP agent workflow

Agents can drive smoke via `plugin-aws-core-aws-mcp`:

1. `aws ec2 run-instances` — `t3.medium`, Ubuntu 22.04, SG with SSH + 9092
2. SSH bootstrap (`bootstrap.sh` user-data or remote)
3. Rsync repo to EC2
4. Run `scripts/run-e2e-smoke-aws.sh` on the instance
5. `aws ec2 terminate-instances` on pass/fail

## Test matrix (21 ignored live tests)

| Layer | Count | Command |
|-------|-------|---------|
| Mem E2E sanity | 13 | `cargo test -p photon-e2e` |
| Kafka E2E broker | 12 | `cargo test -p photon-e2e --features kafka -- --ignored` |
| Kafka contract | 9 | `cargo test -p photon-backend-kafka --test kafka_contract -- --ignored` |

Replay/checkpoint scenarios use `PHOTON_KAFKA_TOPIC_SHARDS=1` (NATS parity).

## Local alternative

```bash
cd infra/broker && ./scripts/kafka-single.sh
source scripts/export-kafka-env.sh
../../infra/aws/kafka-smoke/scripts/run-e2e-smoke-aws.sh
```

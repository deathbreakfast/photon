# AWS Multi-EC2 Kafka KRaft Fleet (PFH campaigns)

Authoritative BM-PFH ingress validation for the Kafka storage adapter. Mirrors the NATS [`broker-fleet`](../broker-fleet/README.md) PFH ladder.

## Topology

| Role | Count | Default type | Notes |
|------|-------|--------------|-------|
| `photon-kafka-single` | 1 | `t3.medium` | N=1 baseline |
| `photon-kafka-{1,2,3}` | 3 | `t3.medium` | KRaft cluster nodes |
| `photon-kafka-bench-{1..4}` | 4 | `c6i.large` | Release builds + orchestration |

Brokers use **private IPs** for KRaft quorum (port 9093) and client bootstrap (port 9092).

## Quick start

```bash
cd infra/aws/kafka-fleet
chmod +x *.sh scripts/*.sh

export AWS_KEY_NAME=your-key-name
export SSH_KEY_PATH=~/.ssh/your-key-name.pem

# 1. Launch instances
./provision.sh

# 2. Bootstrap Kafka brokers
./deploy-brokers.sh

# 3. Build bench on primary host
./deploy-bench.sh

# 4. PFH campaigns (in order)
PFH_SWEEP_MODE=baseline ./scripts/run-pfh-campaign-aws.sh
PFH_SWEEP_MODE=sharded ./scripts/run-pfh-phase3-campaign-aws.sh
BENCH_COUNT=4 ./scripts/run-pfh-multibench-campaign-aws.sh

# 5. Verify + teardown brokers (keep bench for Fluvio)
TEARDOWN_BENCH=0 ./scripts/teardown.sh
```

## PFH sweep modes

| Mode | `PFH_SWEEP_MODE` | Artifact |
|------|------------------|----------|
| Baseline ladder | `baseline` | `scaling-curve-aws-c6i-large-kafka-firehose.json` |
| Sharded ladder | `sharded` | `scaling-curve-aws-c6i-large-kafka-firehose-sharded.json` |
| Multibench | (multibench script) | `scaling-curve-aws-c6i-large-kafka-firehose-multibench.json` |

Set `PFH_PRIMARY_ONLY=1` for a fast validation pass (primary row only per N).

## Local smoke (pre-AWS)

```bash
cd infra/broker
./scripts/run-pfh-sweep-kafka.sh
```

## Apples-to-apples primary row

`stream_seq`, `sync_ack=1`, `publishers=256`, `target=100000`, `PHOTON_KAFKA_MAX_INFLIGHT=256`, `PHOTON_BENCH_CRYPTO=0`, in-VPC on `aws-c6i-large`.

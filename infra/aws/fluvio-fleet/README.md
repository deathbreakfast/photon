# AWS Multi-EC2 Fluvio SC+SPU Fleet (PFH campaigns)

Authoritative BM-PFH ingress validation for the Fluvio storage adapter. Mirrors the NATS/Kafka PFH ladder.

## Topology

| Role | Count | Default type | Notes |
|------|-------|--------------|-------|
| `photon-fluvio-single` | 1 | `t3.medium` | SC host (+ SPU for N=1/N=4) |
| `photon-fluvio-{1,2,3}` | 3 | `t3.medium` | Remote SPUs |
| `photon-fluvio-bench-{1..4}` | 4 | `c6i.large` | Reuse from Kafka fleet when possible |

Clients connect to **SC** at `PHOTON_FLUVIO_ENDPOINT={BROKER_SINGLE_IP}:9103`.

## Quick start

```bash
cd infra/aws/fluvio-fleet
chmod +x *.sh scripts/*.sh

export AWS_KEY_NAME=your-key-name
export SSH_KEY_PATH=~/.ssh/your-key-name.pem

# Option A: fresh provision (brokers + bench)
./provision.sh

# Option B: reuse Kafka bench hosts (brokers only)
REUSE_BENCH=1 ./provision.sh
# Then copy BENCH_* vars from kafka-fleet/instances.env into fluvio-fleet/instances.env

./deploy-brokers.sh
./deploy-bench.sh

PFH_SWEEP_MODE=baseline ./scripts/run-pfh-campaign-aws.sh
PFH_SWEEP_MODE=sharded ./scripts/run-pfh-phase3-campaign-aws.sh
BENCH_COUNT=4 ./scripts/run-pfh-multibench-campaign-aws.sh

./scripts/verify-authoritative-reports.sh
./scripts/teardown.sh
```

## Local smoke

```bash
cd infra/broker
./scripts/run-pfh-sweep-fluvio.sh
```

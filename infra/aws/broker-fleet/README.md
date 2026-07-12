# AWS Multi-EC2 NATS Broker Fleet

Authoritative validation for BM-PF0–PF4, BM-PB4 (linear scaling), and BM-PB5 (failover). Local WSL compose is for smoke; this runbook uses **separate EC2 hosts** per NATS node.

## Topology

| Role | Count | Default type | Notes |
|------|-------|--------------|-------|
| `photon-broker-single` | 1 | `t3.medium` | PB4 n=1 baseline |
| `photon-broker-{1,2,3}` | 3 | `t3.medium` | JetStream RAFT cluster |
| `photon-bench` | 1 | `c6i.large` | Release builds + orchestration |

Brokers use **private IPs** for cluster routes (port 6222) and client URLs (port 4222). Bench host connects via private IPs within the VPC.

## Prerequisites

- AWS CLI configured (`aws sts get-caller-identity`)
- EC2 key pair in target region (default `us-west-2`)
- Operator SSH access from current IP

```bash
export AWS_KEY_NAME=your-key-name
export SSH_KEY_PATH=~/.ssh/your-key-name.pem
export AWS_REGION=us-west-2
```

## Quick start

```bash
cd infra/aws/broker-fleet
chmod +x *.sh scripts/*.sh

# 1. Launch 5 instances → instances.env
./provision.sh

# 2. Bootstrap NATS on broker hosts (SSH via public IPs)
./deploy-brokers.sh

# 3. Copy repo to bench host and build release bench
export PHOTON_REPO_URL=git@github.com:your-org/photon.git   # or omit if using rsync from local clone
./deploy-bench.sh

# 4. SSH to bench and run full campaign
ssh -i "$SSH_KEY_PATH" ubuntu@"$(grep BENCH_PUBLIC_IP instances.env | cut -d= -f2)"
cd ~/photon
export INSTANCES_ENV=$PWD/infra/aws/broker-fleet/instances.env
export CARGO_TARGET_DIR=/tmp/photon-target
./infra/aws/broker-fleet/scripts/run-full-validation.sh

# 5. Teardown when done
./scripts/teardown.sh
```

## Environment files

After `provision.sh`, `instances.env` contains instance IDs and private/public IPs. Source it before orchestration:

```bash
export INSTANCES_ENV=$PWD/instances.env
source scripts/export-env-aws.sh cluster   # or single
```

See [`instances.env.example`](instances.env.example) for the schema.

## Scripts

| Script | Purpose |
|--------|---------|
| `provision.sh` | Create security group + 5 EC2 instances |
| `deploy-brokers.sh` | Remote bootstrap all NATS brokers |
| `deploy-bench.sh` | Rsync repo to bench EC2 + `bootstrap-bench.sh` |
| `bootstrap-broker.sh` | On broker: Docker + NATS from config template |
| `bootstrap-bench.sh` | On bench: Rust + `cargo build --release` |
| `scripts/export-env-aws.sh` | `PHOTON_NATS_URL` from private IPs |
| `scripts/wait-cluster.sh` | Health poll all brokers |
| `scripts/run-full-validation.sh` | PF0–PF4, P6, PG0–PG2, PFS, PFE + PB4 sweep + PB5 + e2e |
| `scripts/run-pf-stale-aws.sh` | Re-run PF0/PF1/PF3 only (replace WSL `*-aws.json`) |
| `scripts/verify-authoritative-reports.sh` | Fail if broker-fleet AWS reports have WSL `hardware_detail` |
| `scripts/run-pb4-sweep-aws.sh` | PB4 n=1 vs n=3 sweep (informational ratio; per-run error gate) |
| `scripts/run-pb5-aws.sh` | Remote kill broker-2 at t=22s |
| `scripts/kill-broker-remote.sh` | SSH `docker stop photon-nats` |
| `scripts/teardown.sh` | Terminate instances + security group |

## PF2 parallel publishers

BM-PF2 uses **4 keyed parallel publishers** (250/s each, aggregate 1000/s) to spread load across partition keys.

## Cost estimate

~5 instances × ~$0.01–0.04/hr ≈ **$0.05–0.15/hr**. Run `teardown.sh` promptly after collecting reports.

## Disk safety on bench host

```bash
export CARGO_TARGET_DIR=/tmp/photon-target
export CARGO_INCREMENTAL=0
export CARGO_BUILD_JOBS=2
```

## Success criteria

**NATS ingress gate (blocking):** PFH campaign + BM-PF2/PF4 (≥900 ops/s) + BM-P6 + BM-PG0–PG2 + BM-PFS/PFE + BM-PB5. Run [`scripts/run-nats-gate-aws.sh`](scripts/run-nats-gate-aws.sh) for a fast subset; [`scripts/verify-authoritative-reports.sh`](scripts/verify-authoritative-reports.sh) before check-in.

| Check | Criterion |
|-------|-----------|
| PF0, PF1, PF3, P6, PG0–PG2, PFS, PFE | `pass: true`, authoritative `hardware_detail` (EC2, not WSL) |
| PF2 | `achieved_ops_per_sec >= 900` |
| PF4 | `achieved_ops_per_sec >= 900` (4×250/s streams) |
| PFH | existing campaign unchanged |
| PB4 sweep | **Informational only** — both legs `pass: true`, `error_rate < 0.001`; n3/n1 ratio logged, not gating |
| PB5 | `error_rate < 0.01` |
| broker-spike | 4/4 pass on single broker |
| mem e2e | 13/13 pass |

Set `PHOTON_BENCH_PB4_REQUIRE_LINEAR=1` to restore the legacy ratio gate (≥2.4 on AWS).

## Tuning

- PB4 ratio flat on 4-shard fleet: expected — use PFH/PF2/PF4 for ingress sizing, not PB4 ratio
- PF2 marginal: set `BENCH_INSTANCE_TYPE=c6i.xlarge`

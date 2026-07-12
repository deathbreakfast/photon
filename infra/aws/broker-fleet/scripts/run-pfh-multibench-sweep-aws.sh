#!/usr/bin/env bash
# Phase 4 multi-bench PFH sweep: bc ∈ {1,2,4} clients on fixed 4-broker / 4-shard NATS.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REPO="$(cd "$ROOT/../../.." && pwd)"
REPORTS="${REPO}/profiling/photon-bench/reports"
mkdir -p "$REPORTS"

# shellcheck disable=SC1091
source "${INSTANCES_ENV:-$ROOT/instances.env}"
# shellcheck disable=SC1091
source "$ROOT/scripts/bench-fleet.sh"

export PHOTON_AWS_USE_PUBLIC_IPS="${PHOTON_AWS_USE_PUBLIC_IPS:-0}"
export PHOTON_BENCH_CRYPTO=0
export PHOTON_BENCH_RESOURCE_PROFILE=1
export PHOTON_NATS_MAX_INFLIGHT="${PHOTON_NATS_MAX_INFLIGHT:-256}"
export PHOTON_NATS_STREAM="${PHOTON_NATS_STREAM:-photon}"
export PHOTON_NATS_RETENTION="${PHOTON_NATS_RETENTION:-15m}"
export PHOTON_NATS_REPLAY_CURSOR=stream_seq
export PHOTON_NATS_SYNC_ACK=1
export PHOTON_BENCH_PUBLISHERS=256
export PHOTON_BENCH_NODES=4
export PHOTON_NATS_STREAM_SHARDS=4

BENCH="${PHOTON_BENCH_CMD:-cargo run --release -p photon-bench --features nats --}"
HW="--hardware ${HARDWARE:-aws-c6i-large}"
TOPO="--topology broker-cluster --storage nats --telemetry off"
SSH_OPTS=(-o StrictHostKeyChecking=accept-new -i "$SSH_KEY_PATH")

bootstrap_cluster() {
  unset PHOTON_NATS_REPLICAS PHOTON_NATS_URL || true
  "$ROOT/scripts/bootstrap-n4-cluster.sh"
  # shellcheck disable=SC1091
  source "$ROOT/scripts/export-env-aws.sh" cluster4
  export PHOTON_NATS_REPLICAS=1
}

sample_broker_resource() {
  local host
  host="$(resolve_host BROKER_1)"
  local stats varz
  stats="$(ssh "${SSH_OPTS[@]}" "${SSH_USER}@${host}" \
    "sudo docker stats photon-nats --no-stream --format '{{.CPUPerc}},{{.MemUsage}}'" 2>/dev/null || true)"
  varz="$(ssh "${SSH_OPTS[@]}" "${SSH_USER}@${host}" \
    "curl -sf http://127.0.0.1:8222/varz" 2>/dev/null || true)"
  export PHOTON_BENCH_BROKER_RESOURCE_JSON="$(
    STATS="$stats" VARZ="$varz" python3 - <<'PY'
import json, os
stats = os.environ.get("STATS", "")
varz = os.environ.get("VARZ", "")
cpu, mem = ("", "")
if stats and "," in stats:
    cpu, mem = stats.split(",", 1)
print(json.dumps({
    "docker_cpu_percent": cpu.strip(),
    "docker_mem_usage": mem.strip(),
    "varz_present": bool(varz),
}))
PY
  )"
}

resolve_host() {
  local base="$1"
  if [[ "${PHOTON_AWS_USE_PUBLIC_IPS:-0}" == "1" ]]; then
    local key="${base}_PUBLIC_IP"
    echo "${!key}"
  else
    local key="${base}_IP"
    echo "${!key}"
  fi
}

run_cell() {
  local count="$1"
  echo "=== Multi-bench PFH bc=${count} (4 brokers, 4 shards) ==="
  sample_broker_resource || true

  local start_epoch
  start_epoch=$(( $(date +%s) + 90 ))
  local tag_base="bm-pfh-nats-n4-sh4-bc${count}"
  local pids=()

  for i in $(seq 1 "$count"); do
    local host client_idx report
    host="$(resolve_bench_ip "$i")"
    client_idx=$((i - 1))
    report="${REPORTS}/${tag_base}-i${client_idx}-stream_seq-ack1-p256-r100000-aws.json"
    ssh "${SSH_OPTS[@]}" "${SSH_USER}@${host}" \
      "export START_EPOCH=${start_epoch} && \
       while [[ \$(date +%s) -lt \$START_EPOCH ]]; do sleep 1; done && \
       export PHOTON_BENCH_CLIENT_INDEX=${client_idx} && \
       export PHOTON_BENCH_CLIENT_COUNT=${count} && \
       export PHOTON_BENCH_NODES=4 && \
       export PHOTON_NATS_STREAM_SHARDS=4 && \
       export PHOTON_NATS_REPLAY_CURSOR=stream_seq && \
       export PHOTON_NATS_SYNC_ACK=1 && \
       export PHOTON_BENCH_PUBLISHERS=256 && \
       export PHOTON_BENCH_CRYPTO=0 && \
       export PHOTON_BENCH_RESOURCE_PROFILE=1 && \
       export PHOTON_NATS_MAX_INFLIGHT=${PHOTON_NATS_MAX_INFLIGHT} && \
       export PHOTON_NATS_URL='${PHOTON_NATS_URL}' && \
       export PHOTON_NATS_REPLICAS=1 && \
       export PHOTON_BENCH_HARDWARE='${HARDWARE:-aws-c6i-large}' && \
       cd ${REPO} && \
       ${BENCH} run --experiment bm-pfh ${TOPO} ${HW} --nodes 4 --publishers 256 \
         --ops 100000 --report ${report}" &
    pids+=($!)
  done

  local fail=0
  for pid in "${pids[@]}"; do
    wait "$pid" || fail=1
  done
  if [[ "$fail" -ne 0 ]]; then
    echo "One or more bench clients failed for bc=${count}" >&2
    exit 1
  fi

  for i in $(seq 2 "$count"); do
    local host
    host="$(resolve_bench_ip "$i")"
    for client_idx in $(seq 0 $((count - 1))); do
      if [[ "$client_idx" -eq $((i - 1)) ]]; then
        local remote_report local_report
        remote_report="${REPORTS}/${tag_base}-i${client_idx}-stream_seq-ack1-p256-r100000-aws.json"
        local_report="${REPORTS}/${tag_base}-i${client_idx}-stream_seq-ack1-p256-r100000-aws.json"
        scp "${SSH_OPTS[@]}" "${SSH_USER}@${host}:${remote_report}" "${local_report}"
      fi
    done
  done

  cd "$REPO"
  $BENCH aggregate-pfh --reports-dir "$REPORTS" --hardware "${HARDWARE:-aws-c6i-large}" --storage nats \
    --cell-prefix "${tag_base}"
}

cd "$REPO"
bootstrap_cluster

for bc in 1 2 4; do
  if [[ "$bc" -gt "${BENCH_COUNT:-1}" ]]; then
    echo "skip bc=${bc}: BENCH_COUNT=${BENCH_COUNT:-1}" >&2
    continue
  fi
  run_cell "$bc"
done

$BENCH scaling-curve --hardware "${HARDWARE:-aws-c6i-large}" --storage nats \
  --reports-dir "$REPORTS" --multibench-ladder \
  --out "$REPORTS/scaling-curve-${HARDWARE:-aws-c6i-large}-nats-firehose-multibench.json"

echo "Multi-bench PFH sweep complete."

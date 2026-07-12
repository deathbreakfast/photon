#!/usr/bin/env bash
# Phase 4 multi-bench PFH sweep for Kafka: bc ∈ {1,2,4} on fixed 4-broker / 4-shard cluster.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REPO="$(cd "$ROOT/../../.." && pwd)"
REPORTS="${REPO}/profiling/photon-bench/reports"
mkdir -p "$REPORTS"

STORAGE="${STORAGE:-kafka}"
FEATURES="${PHOTON_BENCH_FEATURES:-kafka}"

# shellcheck disable=SC1091
source "${INSTANCES_ENV:-$ROOT/instances.env}"
# shellcheck disable=SC1091
source "$ROOT/scripts/bench-fleet.sh"

export PHOTON_AWS_USE_PUBLIC_IPS="${PHOTON_AWS_USE_PUBLIC_IPS:-0}"
export PHOTON_BENCH_CRYPTO=0
export PHOTON_BENCH_RESOURCE_PROFILE=1
export PHOTON_KAFKA_MAX_INFLIGHT="${PHOTON_KAFKA_MAX_INFLIGHT:-256}"
export PHOTON_KAFKA_RETENTION="${PHOTON_KAFKA_RETENTION:-15m}"
export PHOTON_KAFKA_REPLAY_CURSOR=stream_seq
export PHOTON_KAFKA_SYNC_ACK=1
export PHOTON_BENCH_PUBLISHERS=256
export PHOTON_BENCH_NODES=4
export PHOTON_KAFKA_TOPIC_SHARDS=4
export PHOTON_BENCH_TOPIC_SHARDS=4

BENCH="${PHOTON_BENCH_CMD:-cargo run --release -p photon-bench --features ${FEATURES} --}"
HW="--hardware ${HARDWARE:-aws-c6i-large}"
TOPO="--topology broker-cluster --storage ${STORAGE} --telemetry off"
SSH_OPTS=(-o StrictHostKeyChecking=accept-new -i "$SSH_KEY_PATH")

bootstrap_cluster() {
  unset PHOTON_KAFKA_REPLICAS PHOTON_KAFKA_BROKERS || true
  "$ROOT/scripts/bootstrap-n4-cluster.sh"
  # shellcheck disable=SC1091
  source "$ROOT/scripts/export-env-aws.sh" cluster4
  export PHOTON_KAFKA_REPLICAS=1
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
  echo "=== Multi-bench PFH ${STORAGE} bc=${count} (4 brokers, 4 shards) ==="

  local start_epoch
  start_epoch=$(( $(date +%s) + 90 ))
  local tag_base="bm-pfh-${STORAGE}-n4-sh4-bc${count}"
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
       export PHOTON_KAFKA_TOPIC_SHARDS=4 && \
       export PHOTON_BENCH_TOPIC_SHARDS=4 && \
       export PHOTON_KAFKA_REPLAY_CURSOR=stream_seq && \
       export PHOTON_KAFKA_SYNC_ACK=1 && \
       export PHOTON_BENCH_PUBLISHERS=256 && \
       export PHOTON_BENCH_CRYPTO=0 && \
       export PHOTON_BENCH_RESOURCE_PROFILE=1 && \
       export PHOTON_KAFKA_MAX_INFLIGHT=${PHOTON_KAFKA_MAX_INFLIGHT} && \
       export PHOTON_KAFKA_BROKERS='${PHOTON_KAFKA_BROKERS}' && \
       export PHOTON_KAFKA_REPLICAS=1 && \
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
  $BENCH aggregate-pfh --reports-dir "$REPORTS" --hardware "${HARDWARE:-aws-c6i-large}" --storage "${STORAGE}" \
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

$BENCH scaling-curve --hardware "${HARDWARE:-aws-c6i-large}" --storage "${STORAGE}" \
  --reports-dir "$REPORTS" --multibench-ladder \
  --out "$REPORTS/scaling-curve-${HARDWARE:-aws-c6i-large}-${STORAGE}-firehose-multibench.json"

echo "Multi-bench PFH sweep complete (${STORAGE})."

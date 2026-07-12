#!/usr/bin/env bash
# Phase 4 multi-bench PFH sweep for Fluvio: bc ∈ {1,2,4} on fixed 4-SPU / 4-shard cluster.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REPO="$(cd "$ROOT/../../.." && pwd)"
REPORTS="${REPO}/profiling/photon-bench/reports"
mkdir -p "$REPORTS"

STORAGE="${STORAGE:-fluvio}"
FEATURES="${PHOTON_BENCH_FEATURES:-fluvio}"

# shellcheck disable=SC1091
source "${INSTANCES_ENV:-$ROOT/instances.env}"
# shellcheck disable=SC1091
source "$ROOT/scripts/bench-fleet.sh"

export PHOTON_AWS_USE_PUBLIC_IPS="${PHOTON_AWS_USE_PUBLIC_IPS:-0}"
export PHOTON_BENCH_CRYPTO=0
export PHOTON_BENCH_RESOURCE_PROFILE=1
export PHOTON_FLUVIO_MAX_INFLIGHT="${PHOTON_FLUVIO_MAX_INFLIGHT:-256}"
export PHOTON_FLUVIO_RETENTION="${PHOTON_FLUVIO_RETENTION:-15m}"
export PHOTON_FLUVIO_REPLAY_CURSOR=stream_seq
export PHOTON_FLUVIO_SYNC_ACK=1
export PHOTON_BENCH_PUBLISHERS=256
export PHOTON_BENCH_NODES=4
export PHOTON_FLUVIO_TOPIC_SHARDS=4
export PHOTON_BENCH_TOPIC_SHARDS=4

BENCH="${PHOTON_BENCH_CMD:-cargo run --release -p photon-bench --features ${FEATURES} --}"
HW="--hardware ${HARDWARE:-aws-c6i-large}"
TOPO="--topology broker-cluster --storage ${STORAGE} --telemetry off"
SSH_OPTS=(-o StrictHostKeyChecking=accept-new -i "$SSH_KEY_PATH")

bootstrap_cluster() {
  unset PHOTON_FLUVIO_REPLICAS PHOTON_FLUVIO_ENDPOINT || true
  "$ROOT/scripts/bootstrap-n4-cluster.sh"
  load_cluster_env
}

load_cluster_env() {
  # shellcheck disable=SC1091
  source "$ROOT/scripts/export-env-aws.sh" cluster4
  export PHOTON_FLUVIO_REPLICAS=1
}

run_cell() {
  local count="$1"
  echo "=== Multi-bench PFH ${STORAGE} bc=${count} (4 SPUs, 4 shards) ==="

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
       export PHOTON_FLUVIO_TOPIC_SHARDS=4 && \
       export PHOTON_BENCH_TOPIC_SHARDS=4 && \
       export PHOTON_FLUVIO_REPLAY_CURSOR=stream_seq && \
       export PHOTON_FLUVIO_SYNC_ACK=1 && \
       export PHOTON_BENCH_PUBLISHERS=256 && \
       export PHOTON_BENCH_CRYPTO=0 && \
       export PHOTON_BENCH_RESOURCE_PROFILE=1 && \
       export PHOTON_FLUVIO_MAX_INFLIGHT=${PHOTON_FLUVIO_MAX_INFLIGHT} && \
       export PHOTON_FLUVIO_ENDPOINT='${PHOTON_FLUVIO_ENDPOINT}' && \
       export PHOTON_FLUVIO_REPLICAS=1 && \
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
if [[ "${PHOTON_SKIP_CLUSTER_BOOTSTRAP:-0}" != "1" ]]; then
  bootstrap_cluster
else
  load_cluster_env
fi

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

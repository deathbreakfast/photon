#!/usr/bin/env bash
# Authoritative PFH firehose sweep on AWS in-VPC (N=1,2,4 broker nodes).
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REPO="$(cd "$ROOT/../../.." && pwd)"
REPORTS="${REPO}/profiling/photon-bench/reports"
NATS_BENCH="${REPO}/profiling/nats-bench"
mkdir -p "$REPORTS" "$NATS_BENCH"

# shellcheck disable=SC1091
source "${INSTANCES_ENV:-$ROOT/instances.env}"
export PHOTON_AWS_USE_PUBLIC_IPS="${PHOTON_AWS_USE_PUBLIC_IPS:-0}"
export PHOTON_BENCH_CRYPTO=0
export PHOTON_BENCH_RESOURCE_PROFILE=1
export PHOTON_NATS_MAX_INFLIGHT="${PHOTON_NATS_MAX_INFLIGHT:-256}"
export PHOTON_NATS_STREAM="${PHOTON_NATS_STREAM:-photon}"
export PHOTON_NATS_RETENTION="${PHOTON_NATS_RETENTION:-15m}"
export PFH_SWEEP_MODE="${PFH_SWEEP_MODE:-all}"

HW="--hardware ${HARDWARE:-aws-c6i-large}"
TOPO="--topology broker-cluster --storage nats --telemetry off"
BENCH="${PHOTON_BENCH_CMD:-cargo run --release -p photon-bench --features nats --}"

SSH_OPTS=(-o StrictHostKeyChecking=accept-new -i "$SSH_KEY_PATH")
REMOTE_DIR="/tmp/photon-broker-fleet"

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

bootstrap_single() {
  local host
  host="$(resolve_host BROKER_SINGLE)"
  ssh "${SSH_OPTS[@]}" "${SSH_USER}@${host}" "mkdir -p ${REMOTE_DIR}/config"
  scp "${SSH_OPTS[@]}" -r "$ROOT/config" "$ROOT/bootstrap-broker.sh" \
    "${SSH_USER}@${host}:${REMOTE_DIR}/"
  ssh "${SSH_OPTS[@]}" "${SSH_USER}@${host}" \
    "chmod +x ${REMOTE_DIR}/bootstrap-broker.sh && ${REMOTE_DIR}/bootstrap-broker.sh standalone nats-single"
  # shellcheck disable=SC1091
  source "$ROOT/scripts/export-env-aws.sh" single
  export PHOTON_NATS_REPLICAS=1
}

bootstrap_for_nodes() {
  local n="$1"
  unset PHOTON_NATS_REPLICAS PHOTON_NATS_URL || true
  case "$n" in
    1) bootstrap_single ;;
    2) "$ROOT/scripts/bootstrap-n2-cluster.sh"
       # shellcheck disable=SC1091
       source "$ROOT/scripts/export-env-aws.sh" n2
       export PHOTON_NATS_REPLICAS=2 ;;
    4) "$ROOT/scripts/bootstrap-n4-cluster.sh"
       # shellcheck disable=SC1091
       source "$ROOT/scripts/export-env-aws.sh" cluster4
       export PHOTON_NATS_REPLICAS=4 ;;
    *) echo "unsupported broker node count: $n" >&2; exit 1 ;;
  esac
}

run_nats_bench_baseline() {
  local n="$1"
  local host
  if [[ "$n" == "1" ]]; then
    host="$(resolve_host BROKER_SINGLE)"
  else
    host="$(resolve_host BROKER_1)"
  fi
  if ! ssh "${SSH_OPTS[@]}" "${SSH_USER}@${host}" "command -v nats" >/dev/null 2>&1; then
    echo "nats CLI missing on ${host}; skip raw bench baseline for N=${n}"
    unset PHOTON_BENCH_NATS_BENCH_PEAK || true
    return 0
  fi
  local out
  out="$(ssh "${SSH_OPTS[@]}" "${SSH_USER}@${host}" \
    "nats bench pub photon.firehose.raw --server nats://127.0.0.1:4222 --size 512 --msgs 100000 --pub 64" 2>&1 || true)"
  echo "$out" | tee "${NATS_BENCH}/n${n}-raw-pub-aws.txt"
  if echo "$out" | grep -qE '[0-9,]+ msgs/sec'; then
    local peak
    peak="$(echo "$out" | grep -oE '[0-9,]+ msgs/sec' | tail -1 | tr -d ', msgs/sec')"
    echo "{\"broker_nodes\":${n},\"peak_ops_per_sec\":${peak}}" > "${NATS_BENCH}/n${n}-raw-pub-aws.json"
    export PHOTON_BENCH_NATS_BENCH_PEAK="$peak"
  else
    unset PHOTON_BENCH_NATS_BENCH_PEAK || true
  fi
}

sample_broker_resource() {
  local n="$1"
  local host
  if [[ "$n" == "1" ]]; then
    host="$(resolve_host BROKER_SINGLE)"
  else
    host="$(resolve_host BROKER_1)"
  fi
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

run_pfh() {
  local cursor="$1"
  local ack="$2"
  local pubs="$3"
  local rate="$4"
  local nodes="$5"
  local peak_cell="${6:-0}"
  local stream_shards="${7:-1}"
  export PHOTON_NATS_REPLAY_CURSOR="$cursor"
  export PHOTON_NATS_SYNC_ACK="$ack"
  export PHOTON_BENCH_PUBLISHERS="$pubs"
  export PHOTON_BENCH_NODES="$nodes"
  export PHOTON_NATS_STREAM_SHARDS="$stream_shards"
  if [[ "$peak_cell" == "1" ]]; then
    sample_broker_resource "$nodes" || true
  else
    unset PHOTON_BENCH_BROKER_RESOURCE_JSON || true
  fi
  local tag="bm-pfh-nats-n${nodes}-sh${stream_shards}-${cursor}-ack${ack}-p${pubs}-r${rate}-aws"
  cd "$REPO"
  $BENCH run --experiment bm-pfh $TOPO $HW --nodes "$nodes" --publishers "$pubs" \
    --ops "$rate" --report "$REPORTS/${tag}.json"
}

sweep_nodes() {
  local nodes="$1"
  local stream_shards="$2"
  echo "=== PFH sweep N=${nodes} stream_shards=${stream_shards} ==="
  bootstrap_for_nodes "$nodes"
  run_nats_bench_baseline "$nodes"

  if [[ "${PFH_PRIMARY_ONLY:-0}" == "1" || "${PFH_SWEEP_MODE:-all}" == "phase3" ]]; then
    # Apples-to-apples primary row only (stream_seq / ack=1 / 256 pubs / 100k target).
    run_pfh stream_seq 1 256 100000 "$nodes" 1 "$stream_shards"
    return
  fi

  for cursor in stream_seq tail_only; do
    for ack in 0 1; do
      if [[ "$cursor" == "stream_seq" && "$ack" == "0" ]]; then
        continue
      fi
      for pubs in 32 128; do
        for rate in 10000 50000; do
          run_pfh "$cursor" "$ack" "$pubs" "$rate" "$nodes" 0 "$stream_shards"
        done
      done
    done
  done

  # Peak finder cells
  for cursor in stream_seq tail_only; do
    run_pfh "$cursor" 1 256 100000 "$nodes" 1 "$stream_shards"
  done
}

cd "$REPO"
case "${PFH_SWEEP_MODE:-all}" in
  baseline)
    for n in 1 2 4; do
      sweep_nodes "$n" 1
    done
    CURVE_ARGS=(--stream-shards 1)
    OUT_SUFFIX=""
    ;;
  sharded|phase3)
    for n in 1 2 4; do
      sweep_nodes "$n" "$n"
    done
    CURVE_ARGS=(--match-broker-nodes)
    OUT_SUFFIX="-sharded"
    ;;
  all)
    for n in 1 2 4; do
      sweep_nodes "$n" 1
    done
    for n in 1 2 4; do
      sweep_nodes "$n" "$n"
    done
    CURVE_ARGS=(--stream-shards 1)
    OUT_SUFFIX=""
    ;;
  *)
    echo "unknown PFH_SWEEP_MODE=${PFH_SWEEP_MODE}" >&2
    exit 1
    ;;
esac

$BENCH scaling-curve --hardware "${HARDWARE:-aws-c6i-large}" --storage nats \
  --reports-dir "$REPORTS" \
  "${CURVE_ARGS[@]}" \
  --out "$REPORTS/scaling-curve-${HARDWARE:-aws-c6i-large}-nats-firehose${OUT_SUFFIX}.json"

echo "PFH AWS sweep complete."

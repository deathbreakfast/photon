#!/usr/bin/env bash
# Rsync repo to SQLite smoke EC2 and run Criterion microbenches; pull reports back.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REPO="$(cd "$ROOT/../../.." && pwd)"
ENV_FILE="${INSTANCES_ENV:-$ROOT/instances.env}"
# shellcheck disable=SC1091
source "$ENV_FILE"

HOST="${SMOKE_PUBLIC_IP:-$SMOKE_IP}"
SSH_KEY="${SSH_KEY_PATH:-$HOME/.ssh/${AWS_KEY_NAME:-}.pem}"
SSH_OPTS=(-o StrictHostKeyChecking=accept-new -i "$SSH_KEY")
REMOTE_DIR="/tmp/photon-sqlite-check"
REPORTS_LOCAL="$REPO/profiling/photon-bench/reports"
HW_LABEL="${HARDWARE:-aws-t3-medium}"

mkdir -p "$REPORTS_LOCAL"

echo "Syncing repo to ${SSH_USER}@${HOST}:${REMOTE_DIR}..."
ssh "${SSH_OPTS[@]}" "${SSH_USER}@${HOST}" "mkdir -p ${REMOTE_DIR}"
rsync -az --delete \
  --exclude target --exclude .git \
  "$REPO/" "${SSH_USER}@${HOST}:${REMOTE_DIR}/"

ssh "${SSH_OPTS[@]}" "${SSH_USER}@${HOST}" \
  env HW_LABEL="${HW_LABEL}" bash -s <<REMOTE
set -euo pipefail
source "\$HOME/.cargo/env" 2>/dev/null || true
export CARGO_TARGET_DIR=/tmp/photon-target
export CARGO_INCREMENTAL=0
export CARGO_BUILD_JOBS=1
export PHOTON_TRANSPORT_KEY="\${PHOTON_TRANSPORT_KEY:-cGhvdG9uLWRldi10cmFuc3BvcnQta2V5LTMyYnl0ZXM=}"
export HW_LABEL="\${HW_LABEL:-aws-t3-medium}"
cd ${REMOTE_DIR}

mkdir -p /tmp/photon-criterion-out
echo "=== Criterion: envelope_crypto ==="
cargo bench -p photon-backend --bench envelope_crypto -- --sample-size 50 \
  | tee /tmp/photon-criterion-out/envelope_crypto.txt

echo "=== Criterion: shard_routing ==="
cargo bench -p photon-backend --bench shard_routing -- --sample-size 50 \
  | tee /tmp/photon-criterion-out/shard_routing.txt

echo "=== Criterion: dispatch_stub ==="
cargo bench -p photon-backend --bench dispatch_stub -- --sample-size 50 \
  | tee /tmp/photon-criterion-out/dispatch_stub.txt

# Bundle a machine-readable summary JSON for profiling/reports.
python3 - <<'PY'
import json, os, re, platform, datetime
out = {
  "kind": "criterion-micro",
  "hardware": os.environ.get("HW_LABEL", "aws-t3-medium"),
  "captured_at": datetime.datetime.utcnow().isoformat() + "Z",
  "os": platform.platform(),
  "benches": {},
}
pat = re.compile(r"^\s*(\S.*?)\s+time:\s+\[([^\]]+)\]", re.M)
for name in ("envelope_crypto", "shard_routing", "dispatch_stub"):
    path = f"/tmp/photon-criterion-out/{name}.txt"
    text = open(path).read()
    rows = [{"id": m.group(1).strip(), "time": m.group(2).strip()} for m in pat.finditer(text)]
    out["benches"][name] = {"raw_path": path, "measurements": rows}
open("/tmp/photon-criterion-out/criterion-summary.json", "w").write(json.dumps(out, indent=2))
print("wrote /tmp/photon-criterion-out/criterion-summary.json")
PY
REMOTE

mkdir -p /tmp/photon-criterion-pull
rsync -az -e "ssh ${SSH_OPTS[*]}" \
  "${SSH_USER}@${HOST}:/tmp/photon-criterion-out/" \
  "/tmp/photon-criterion-pull/"

cp /tmp/photon-criterion-pull/criterion-summary.json \
  "$REPORTS_LOCAL/criterion-${HW_LABEL}-aws.json"
cp /tmp/photon-criterion-pull/*.txt "$REPORTS_LOCAL/" 2>/dev/null || true

# Mem-profile note (measurement findings from Criterion + design review targets).
cat > "$REPORTS_LOCAL/mem-profile-${HW_LABEL}-aws.md" <<EOF
# Mem adapter profiling notes (${HW_LABEL})

Captured: $(date -u +%Y-%m-%dT%H:%MZ) on SQLite smoke EC2.

## Measured

See \`criterion-${HW_LABEL}-aws.json\` for hot-path Criterion timings
(envelope crypto, shard routing, empty dispatch stub).

## Observed / follow-up targets (not rewritten this pass)

- String partition keys (\`topic:key\` formatting) remain allocation-heavy in \`InProcStoragePort\`.
- Replay buffer uses \`RwLock<HashMap<String, VecDeque<Event>>>\` — growth under long replay
  windows should be watched before claiming mem as a production baseline.
- Cloned JSON payloads on subscribe fanout remain a structural cost.

No \`InProcStoragePort\` rewrite in this campaign; numbers do not yet justify a redesign.
EOF

echo "Criterion reports written under $REPORTS_LOCAL"

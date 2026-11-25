#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

echo "==> go test ./... -count=1"
go test ./... -count=1

echo "==> go build"
mkdir -p artifacts/bin
go build -o artifacts/bin/converge ./cmd/converge
BIN="$ROOT/artifacts/bin/converge"

WORK="$ROOT/artifacts/verify-sh"
rm -rf "$WORK"
mkdir -p "$WORK"

"$BIN" gen-samples --dir "$ROOT/samples" >/dev/null

echo "==> start deterministic loopback target"
"$BIN" target --addr 127.0.0.1:0 \
  --snapshot "$ROOT/samples/snapshot-v1.json" \
  --addr-file "$WORK/target.addr" >"$WORK/target.log" 2>&1 &
TARGET_PID=$!
cleanup() {
  kill "$TARGET_PID" 2>/dev/null || true
}
trap cleanup EXIT

for _ in $(seq 1 100); do
  if [[ -s "$WORK/target.addr" ]]; then break; fi
  sleep 0.05
done
ENDPOINT="$(tr -d '\r\n' < "$WORK/target.addr")"
if [[ ! "$ENDPOINT" =~ ^http://127\.0\.0\.1: ]]; then
  echo "target endpoint was not discovered: $ENDPOINT" >&2
  cat "$WORK/target.log" >&2 || true
  exit 1
fi
echo "endpoint=$ENDPOINT"

echo "==> full snapshot and apply"
"$BIN" commit-full --db "$WORK/convergence.db" \
  --file "$ROOT/samples/snapshot-v1.json" \
  --out "$WORK/full-commit.json" >/dev/null
"$BIN" apply --db "$WORK/convergence.db" --endpoint "$ENDPOINT" --all \
  --out "$WORK/full-apply.json" >/dev/null

echo "==> delta cascade and apply"
"$BIN" commit-delta --db "$WORK/convergence.db" \
  --file "$ROOT/samples/delta-v2.json" \
  --out "$WORK/delta-commit.json" >/dev/null
"$BIN" apply --db "$WORK/convergence.db" --endpoint "$ENDPOINT" --all \
  --out "$WORK/delta-apply.json" >/dev/null

echo "==> stale base must return E_STALE_BASE"
set +e
"$BIN" commit-delta --db "$WORK/convergence.db" \
  --file "$ROOT/samples/delta-stale-v99.json" \
  --out "$WORK/stale.json" >"$WORK/stale.stdout" 2>"$WORK/stale.stderr"
STALE_CODE=$?
set -e
if [[ $STALE_CODE -eq 0 ]] || ! grep -q E_STALE_BASE "$WORK/stale.stderr"; then
  echo "stale delta did not fail correctly (exit=$STALE_CODE)" >&2
  cat "$WORK/stale.stderr" >&2
  exit 1
fi

echo "==> invalid spec must return E_SPEC_INVALID"
set +e
"$BIN" commit-full --db "$WORK/invalid.db" \
  --file "$ROOT/samples/invalid-v1.json" \
  --out "$WORK/invalid.json" >"$WORK/invalid.stdout" 2>"$WORK/invalid.stderr"
INVALID_CODE=$?
set -e
if [[ $INVALID_CODE -eq 0 ]] || ! grep -q E_SPEC_INVALID "$WORK/invalid.stderr"; then
  echo "invalid snapshot did not fail correctly (exit=$INVALID_CODE)" >&2
  cat "$WORK/invalid.stderr" >&2
  exit 1
fi

echo "==> dependency cycle must return E_DEPENDENCY_CYCLE"
set +e
"$BIN" commit-full --db "$WORK/cycle.db" \
  --file "$ROOT/samples/cycle-v1.json" \
  --out "$WORK/cycle.json" >"$WORK/cycle.stdout" 2>"$WORK/cycle.stderr"
CYCLE_CODE=$?
set -e
if [[ $CYCLE_CODE -ne 3 ]] || ! grep -q E_DEPENDENCY_CYCLE "$WORK/cycle.stderr"; then
  echo "cycle snapshot did not fail correctly (exit=$CYCLE_CODE)" >&2
  cat "$WORK/cycle.stderr" >&2
  exit 1
fi

echo "==> SIGKILL after durable write intent"
"$BIN" commit-full --db "$WORK/crash.db" \
  --file "$ROOT/samples/snapshot-v1.json" \
  --out "$WORK/crash-commit.json" >/dev/null
"$BIN" apply --db "$WORK/crash.db" --endpoint "$ENDPOINT" \
  --batch B001-S1 --crash-after-write-intent \
  --ready-file "$WORK/crash-ready.json" >"$WORK/crash-apply.log" 2>&1 &
CRASH_PID=$!
for _ in $(seq 1 100); do
  if [[ -s "$WORK/crash-ready.json" ]]; then break; fi
  sleep 0.05
done
if [[ ! -s "$WORK/crash-ready.json" ]]; then
  echo "crash process did not persist intent" >&2
  cat "$WORK/crash-apply.log" >&2 || true
  exit 1
fi
kill -9 "$CRASH_PID"
wait "$CRASH_PID" 2>/dev/null || true

"$BIN" recover --db "$WORK/crash.db" --out "$WORK/recovery.json" >/dev/null
"$BIN" apply --db "$WORK/crash.db" --endpoint "$ENDPOINT" --batch B001-S1 \
  --out "$WORK/reapply.json" >/dev/null

python3 - "$WORK" <<'PY'
import json
import pathlib
import sys

work = pathlib.Path(sys.argv[1])
full_apply = json.loads((work / "full-apply.json").read_text())
delta = json.loads((work / "delta-commit.json").read_text())
delta_apply = json.loads((work / "delta-apply.json").read_text())
recovery = json.loads((work / "recovery.json").read_text())["recovery"]
reapply = json.loads((work / "reapply.json").read_text())

assert full_apply["ok"] is True, full_apply
assert full_apply["round"]["status"] == "succeeded"
assert full_apply["view"]["batches_failed"] == 0
assert delta["ok"] is True and delta["version"] == 2
assert "golf-api" in delta["cascade"]["invalidated_drift_ids"]
assert delta_apply["ok"] is True and delta_apply["round"]["status"] == "succeeded"
assert len(recovery["rolled_back_write_batches"]) == 1
assert recovery["rolled_back_write_batches"][0]["new_status"] == "pending"
assert recovery["exit_report_promise"] is False
assert reapply["ok"] is True and reapply["result"]["status"] == "succeeded"

report = {
    "ok": True,
    "full_batches": len(full_apply["round"]["batch_results"]),
    "delta_batches": len(delta_apply["round"]["batch_results"]),
    "cascade": delta["cascade"],
    "recovery": recovery,
}
(work / "verification-summary.json").write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n")
print(json.dumps({k: report[k] for k in ("ok", "full_batches", "delta_batches")}, ensure_ascii=False))
PY

echo "VERIFY_SH_OK artifacts/verify-sh/verification-summary.json"

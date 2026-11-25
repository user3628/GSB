#!/usr/bin/env bash
# Deterministic end-to-end verification for the qaudit workbench.
# No Docker, no GPU, no network dataset: all inputs are generated locally.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT
echo "workdir: $WORK"

echo "==> unit + race tests"
go test ./... -race

echo "==> generate two independent deterministic datasets"
go run ./cmd/qaudit generate -data "$WORK/a" >/dev/null
go run ./cmd/qaudit generate -data "$WORK/b" >/dev/null

echo "==> continuous run A and B, compare byte-level digests"
go run ./cmd/qaudit run    -data "$WORK/a" >"$WORK/run_a.json"
go run ./cmd/qaudit export -data "$WORK/a" >"$WORK/exp_a.json"
go run ./cmd/qaudit run    -data "$WORK/b" >"$WORK/run_b.json"
go run ./cmd/qaudit export -data "$WORK/b" >"$WORK/exp_b.json"
python3 scripts/compare_digests.py "$WORK/run_a.json" "$WORK/run_b.json" \
  "$WORK/exp_a.json" "$WORK/exp_b.json"

echo "==> forced termination after layer 2, then recovery"
go run ./cmd/qaudit generate -data "$WORK/c" >/dev/null
set +e
go run ./cmd/qaudit run -data "$WORK/c" -crash-after-layer 2 >/dev/null 2>&1
set -e
echo "committed after crash:"
sqlite3 "$WORK/c/snapshots.db" "SELECT seq, layer_name FROM snapshots ORDER BY seq;"
go run ./cmd/qaudit recover -data "$WORK/c" >/dev/null
go run ./cmd/qaudit export  -data "$WORK/c" >"$WORK/exp_c.json"
python3 scripts/compare_recovery.py "$WORK/exp_a.json" "$WORK/exp_c.json"

echo "==> fault injection business codes"
for spec in \
  "zero_channel -layer fc2 -channel 5" \
  "nan_weight -layer fc1 -channel 0" \
  "bad_bitwidth -layer fc3 -bitwidth 3" \
  "calib_ratio -ratio 0" \
  "calib_ratio -ratio 9"; do
  set +e
  out="$(go run ./cmd/qaudit inject -data "$WORK/a" -kind $spec 2>&1)"
  set -e
  code="$(printf '%s' "$out" | grep -oE 'E_[A-Z_]+' | head -1)"
  printf 'injection %-28s -> %s\n' "$spec" "$code"
  if [ -z "$code" ]; then
    echo "missing business code: $out" >&2
    exit 1
  fi
done

echo
echo "ALL VERIFICATION CHECKS PASSED"

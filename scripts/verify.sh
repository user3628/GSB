#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$root"

work="${VPNBENCH_WORK_DIR:-$(mktemp -d)}"
mkdir -p "$work" validation

CGO_ENABLED=0 go build -trimpath -o "$work/vpnbench" ./cmd/vpnbench
"$work/vpnbench" generate --out samples
"$work/vpnbench" run \
  --config samples/configs/workbench.json \
  --db "$work/workbench.db" \
  --report validation/workbench-report.json
"$work/vpnbench" verify \
  --samples samples \
  --work "$work/verify" \
  --out validation/acceptance-report.json

echo "reports: validation/workbench-report.json validation/acceptance-report.json"

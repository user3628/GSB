#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
WORKDIR="$(mktemp -d /tmp/ledger-workbench-demo.XXXXXX)"
mkdir -p "$WORKDIR/bin"

echo "workdir=$WORKDIR"
cd "$ROOT"

echo "== build =="
go build -o "$WORKDIR/bin/ledger-workbench" ./cmd/ledger-workbench
go build -o "$WORKDIR/bin/probe-target" ./cmd/probe-target
go build -o "$WORKDIR/bin/gen-samples" ./cmd/gen-samples
"$WORKDIR/bin/gen-samples" --dir "$WORKDIR/samples" --seed 20261125 >/dev/null

"$WORKDIR/bin/probe-target" --addr 127.0.0.1:0 --port-file "$WORKDIR/probe-url" &
PROBE_PID=$!
"$WORKDIR/bin/ledger-workbench" \
  --db "$WORKDIR/ledger.db" \
  --addr 127.0.0.1:0 \
  --port-file "$WORKDIR/wb-url" \
  --probe-concurrency 1 &
WB_PID=$!

cleanup() {
  kill "$PROBE_PID" "$WB_PID" 2>/dev/null || true
}
trap cleanup EXIT

wait_for_file_url() {
  local file=$1
  for _ in $(seq 1 100); do
    if [[ -s "$file" ]]; then
      tr -d '\n' <"$file"
      return 0
    fi
    sleep 0.05
  done
  echo "timed out waiting for $file" >&2
  return 1
}

PROBE_URL="$(wait_for_file_url "$WORKDIR/probe-url")"
WB_URL="$(wait_for_file_url "$WORKDIR/wb-url")"
for _ in $(seq 1 100); do
  if curl -fsS "$WB_URL/healthz" >/dev/null 2>&1; then break; fi
  sleep 0.05
done

BASE_MS="$(sed -n 's/.*"base_time_ms": \([0-9][0-9]*\).*/\1/p' "$WORKDIR/samples/manifest.json" | head -1)"
DURING_MS=$((BASE_MS + 80000))
AFTER_MS=$((BASE_MS + 180000))
REFUSED_URL="$("$WORKDIR/bin/probe-target" --print-refused)"

section() { printf '\n==== %s ====\n' "$1"; }
post_file() {
  local path=$1 file=$2
  curl -sS -H 'Content-Type: application/json' -X POST --data-binary @"$file" "$WB_URL$path"
  echo
}
post_json() {
  local path=$1 json=$2
  curl -sS -H 'Content-Type: application/json' -X POST --data "$json" "$WB_URL$path"
  echo
}
get_json() { curl -sS "$WB_URL$1"; echo; }
extract_json() {
  local file=$1 key=$2 out=$3
  python3 -c 'import json,sys; key=sys.argv[2]; data=json.load(open(sys.argv[1])); open(sys.argv[3],"w").write(json.dumps(data[key], ensure_ascii=False))' \
    "$file" "$key" "$out"
}

section "health"
get_json /healthz

section "submit fixed weight snapshot v1"
post_file /v1/snapshots "$WORKDIR/samples/snapshot.json"

section "submit valid migration base_version=1"
extract_json "$WORKDIR/samples/migrations.json" valid "$WORKDIR/valid-migration.json"
post_file /v1/migrations "$WORKDIR/valid-migration.json"

section "submit fixed ledger entries"
post_file /v1/entries "$WORKDIR/samples/entries.json"

section "strict conservation: route-balanced exact 0.700000/0.300000"
get_json "/v1/conservation/route-balanced?observe_version=3&as_of_ms=$((BASE_MS+30000))&strict=1"

section "migration transition: v1/v2 are 500/500 and history stays original"
get_json "/v1/conservation/route-migrate?observe_version=3&as_of_ms=$DURING_MS"
get_json "/v1/ledger/route-migrate?observe_version=3"
section "E_WEIGHT_IMBALANCE after cutover when strict current weights are checked"
get_json "/v1/conservation/route-migrate?observe_version=3&as_of_ms=$AFTER_MS&strict=1"

section "idempotent snapshot replay"
post_file /v1/snapshots "$WORKDIR/samples/snapshot.json"

section "E_CONFLICTING_LEDGER: same commit_id, different content"
python3 -c 'import json,sys; p=sys.argv[1]; d=json.load(open(p)); d["generated_at_ms"]+=1; d["digest"]=""; json.dump(d,open(sys.argv[2],"w"))' \
  "$WORKDIR/samples/snapshot.json" "$WORKDIR/conflicting-snapshot.json"
post_file /v1/snapshots "$WORKDIR/conflicting-snapshot.json"

section "E_STALE_BASE: sample fragment still carries base_version=1"
extract_json "$WORKDIR/samples/migrations.json" stale_base "$WORKDIR/stale-base.json"
post_file /v1/migrations "$WORKDIR/stale-base.json"

section "E_STALE_READ: observed_version=999 is newer than view_version=3"
get_json "/v1/conservation/route-balanced?observe_version=999"

section "E_MIGRATION_RACE: overlapping ratios 600+300 != 1000"
cat >"$WORKDIR/race-a.json" <<JSON
{"commit_id":"demo-race-a","migration_id":"demo-race-a","base_version":3,"route_key":"route-race","from_version":"v1","to_version":"v2","ratio_permille":600,"start_at_ms":$((BASE_MS+60000)),"transition_ms":100000}
JSON
cat >"$WORKDIR/race-b.json" <<JSON
{"commit_id":"demo-race-b","migration_id":"demo-race-b","base_version":4,"route_key":"route-race","from_version":"v1","to_version":"v2","ratio_permille":300,"start_at_ms":$((BASE_MS+70000)),"transition_ms":100000}
JSON
post_file /v1/migrations "$WORKDIR/race-a.json"
post_file /v1/migrations "$WORKDIR/race-b.json"

section "probe OK against random loopback test service"
post_json /v1/probes/single "{\"batch_id\":\"demo-ok\",\"route_key\":\"route-probe\",\"base_url\":\"$PROBE_URL\",\"expected_owner_version\":\"owner\",\"timeout_ms\":1000}"

section "E_PROBE_TIMEOUT with connection_close and slot_released facts"
post_json /v1/probes/single "{\"batch_id\":\"demo-timeout\",\"route_key\":\"route-probe\",\"base_url\":\"$PROBE_URL\",\"expected_owner_version\":\"owner\",\"timeout_ms\":40,\"injection\":{\"mode\":\"timeout\"}}"

section "E_PROBE_REFUSED"
post_json /v1/probes/single "{\"batch_id\":\"demo-refused\",\"route_key\":\"route-probe\",\"endpoint\":\"$REFUSED_URL\",\"timeout_ms\":300}"

section "E_PROBE_MISMATCH"
post_json /v1/probes/single "{\"batch_id\":\"demo-mismatch\",\"route_key\":\"route-probe\",\"base_url\":\"$PROBE_URL\",\"expected_owner_version\":\"owner\",\"timeout_ms\":1000,\"injection\":{\"mode\":\"mismatch\"}}"

section "E_PROBE_LATE_RESPONSE"
post_json /v1/probes/single "{\"batch_id\":\"demo-late\",\"route_key\":\"route-probe\",\"base_url\":\"$PROBE_URL\",\"expected_owner_version\":\"owner\",\"timeout_ms\":1000,\"late_threshold_ms\":40,\"injection\":{\"mode\":\"late_response\",\"delay_ms\":120}}"

section "parameter-only ledger injections: invalid sum / ratio range / version gap"
extract_json "$WORKDIR/samples/injection_parameters.json" ledger_invalid_sum "$WORKDIR/invalid-sum.json"
post_file /v1/entries "$WORKDIR/invalid-sum.json"
extract_json "$WORKDIR/samples/injection_parameters.json" migration_ratio_out_of_range "$WORKDIR/invalid-ratio.json"
post_file /v1/migrations "$WORKDIR/invalid-ratio.json"
extract_json "$WORKDIR/samples/injection_parameters.json" version_gap_snapshot "$WORKDIR/version-gap.json"
post_file /v1/snapshots "$WORKDIR/version-gap.json"

section "sqlite persistent facts"
if command -v sqlite3 >/dev/null 2>&1; then
  sqlite3 -header -column "$WORKDIR/ledger.db" \
    "SELECT key,value FROM meta WHERE key IN ('view_version','snapshot_version');
     SELECT route_key,view_version,snapshot_version,valid,conservation_hash FROM derived_lineage ORDER BY route_key LIMIT 8;
     SELECT batch_id,route_key,outcome_code,timed_out,conn_closed,slot_released FROM probe_results ORDER BY rowid DESC LIMIT 5;"
else
  echo "sqlite3 CLI not found; database remains at $WORKDIR/ledger.db"
fi

echo
echo "demo complete. artifacts preserved in $WORKDIR"

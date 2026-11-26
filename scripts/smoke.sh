#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
"$ROOT/scripts/build.sh"

AUTH_PORT="${AUTH_PORT:-25354}"
DNS_PORT="${DNS_PORT:-25353}"
ADMIN_PORT="${ADMIN_PORT:-28080}"
TMP="$(mktemp -d "${TMPDIR:-/tmp}/dnscache-smoke.XXXXXX")"
DB="$TMP/dnscache.db"
PIDS=()
cleanup() {
  for pid in "${PIDS[@]:-}"; do kill "$pid" 2>/dev/null || true; done
  wait 2>/dev/null || true
  rm -rf "$TMP"
}
trap cleanup EXIT

wait_health() {
  for _ in $(seq 1 120); do
    if curl -fsS "http://127.0.0.1:$ADMIN_PORT/healthz" >/dev/null 2>&1; then return 0; fi
    sleep 0.05
  done
  echo 'admin health check failed' >&2
  cat "$TMP/cache.log" >&2 || true
  return 1
}

start_auth_v1() {
  "$ROOT/bin/authserver" -zone "$ROOT/samples/zone-v1.json" \
    -udp "127.0.0.1:$AUTH_PORT" -tcp "127.0.0.1:$AUTH_PORT" \
    >>"$TMP/auth.log" 2>&1 &
  AUTH_PID=$!
  PIDS+=("$AUTH_PID")
  sleep 0.15
}

stop_auth() {
  if [[ -n "${AUTH_PID:-}" ]]; then
    kill "$AUTH_PID" 2>/dev/null || true
    wait "$AUTH_PID" 2>/dev/null || true
    AUTH_PID=''
  fi
}

start_auth_v1
"$ROOT/bin/dnscache" \
  -udp "127.0.0.1:$DNS_PORT" -tcp "127.0.0.1:$DNS_PORT" \
  -admin "127.0.0.1:$ADMIN_PORT" -db "$DB" \
  -upstream "127.0.0.1:$AUTH_PORT" -snapshot "$ROOT/samples/zone-v1.json" \
  -upstream-timeout 300ms -round-budget 2s \
  >"$TMP/cache.log" 2>&1 &
CACHE_PID=$!
PIDS+=("$CACHE_PID")
wait_health

probe() { "$ROOT/bin/dnsprobe" -server "127.0.0.1:$DNS_PORT" -timeout 3s "$@"; }

echo '== A / AAAA / CNAME / MX / TXT / NS / SOA =='
probe -name www.example.test -type A | tee "$TMP/a.json"
grep -q '192.0.2.10' "$TMP/a.json"
probe -name www.example.test -type AAAA | grep -q '2001:db8::10'
probe -name alias.example.test -type A | grep -q '192.0.2.10'
probe -name example.test -type MX | grep -q 'mail.example.test.'
probe -name txt.example.test -type TXT | grep -q 'local deterministic sample'
probe -name example.test -type NS | grep -q 'ns1.example.test.'
probe -name example.test -type SOA | grep -q 'hostmaster.example.test.'
probe -network tcp -name www.example.test -type A | grep -q '192.0.2.10'

echo '== cached TTL must be non-increasing =='
probe -name www.example.test -type A -repeat 2 -interval 1100ms -require-noninc-ttl | tee "$TMP/ttl.json"

echo '== NXDOMAIN / NODATA =='
probe -name absent.example.test -type A -expect-rcode NXDOMAIN | grep -q 'NXDOMAIN'
probe -name txt.example.test -type A -expect-rcode NOERROR -expect-answer-count 0 | tee "$TMP/nodata.json"

echo '== malformed packet => FORMERR + EDE E_MESSAGE_MALFORMED =='
probe -raw-hex '000901000001000000000000c00c00010001' \
  -expect-rcode FORMERR -expect-extended-code E_MESSAGE_MALFORMED | tee "$TMP/formerr.json"

echo '== zone v2 authority and delta invalidation =='
stop_auth
"$ROOT/bin/authserver" -zone "$ROOT/samples/zone-v2.json" \
  -udp "127.0.0.1:$AUTH_PORT" -tcp "127.0.0.1:$AUTH_PORT" \
  >>"$TMP/auth.log" 2>&1 &
AUTH_PID=$!
PIDS+=("$AUTH_PID")
sleep 0.15
status=$(curl -sS -o "$TMP/delta.out" -w '%{http_code}' \
  -H 'Content-Type: application/json' -H 'X-Request-ID: smoke-delta-v1-v2' \
  --data-binary @"$ROOT/samples/delta-v1-to-v2.json" \
  "http://127.0.0.1:$ADMIN_PORT/api/zones/delta")
test "$status" = '200'
probe -name www.example.test -type A | tee "$TMP/a-v2.json"
grep -q '192.0.2.11' "$TMP/a-v2.json"

echo '== stale delta must be HTTP 409 and leave version unchanged =='
status=$(curl -sS -o "$TMP/stale.out" -w '%{http_code}' \
  -H 'Content-Type: application/json' \
  --data-binary @"$ROOT/samples/delta-stale.json" \
  "http://127.0.0.1:$ADMIN_PORT/api/zones/delta")
if [[ "$status" != '409' ]]; then
  echo "expected 409 for stale delta, got $status" >&2
  cat "$TMP/stale.out" >&2
  exit 1
fi
if ! grep -q 'E_STALE_BASE' "$TMP/stale.out"; then
  echo 'stale delta response did not contain E_STALE_BASE' >&2
  cat "$TMP/stale.out" >&2
  exit 1
fi
curl -fsS "http://127.0.0.1:$ADMIN_PORT/api/zones/example.test." | grep -Eq '"version"[[:space:]]*:[[:space:]]*2'

echo '== upstream timeout => SERVFAIL + E_UPSTREAM_TIMEOUT =='
stop_auth
"$ROOT/bin/authserver" -zone "$ROOT/samples/zone-v2.json" -behavior drop \
  -udp "127.0.0.1:$AUTH_PORT" -tcp "127.0.0.1:$AUTH_PORT" \
  >>"$TMP/auth.log" 2>&1 &
AUTH_PID=$!
PIDS+=("$AUTH_PID")
sleep 0.15
probe -name "timeout-$$.example.test" -type A -expect-rcode SERVFAIL \
  -expect-extended-code E_UPSTREAM_TIMEOUT | tee "$TMP/timeout.json"

echo '== recovery report and SQLite facts =='
curl -fsS "http://127.0.0.1:$ADMIN_PORT/api/recovery" | tee "$TMP/recovery.json"
echo
sqlite3 "$DB" <<'SQL'
.mode list
SELECT 'zones=' || COUNT(*) FROM zones;
SELECT 'cache_entries=' || COUNT(*) FROM cache_entries;
SELECT 'pending_intents=' || COUNT(*) FROM intention_log WHERE status='pending';
SELECT 'committed_batches=' || COUNT(*) FROM zone_batches WHERE status='committed';
SQL

curl -fsS "http://127.0.0.1:$ADMIN_PORT/api/metrics" | tee "$TMP/metrics.json" >/dev/null
echo
echo 'smoke: PASS'

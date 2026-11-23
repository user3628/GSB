#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

export GOFLAGS=-mod=vendor
export GOPROXY=off
export GOSUMDB=off
export CGO_ENABLED=0

TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

echo "[1/8] offline unit and integration tests"
go test ./...

echo "[2/8] offline Linux build and Windows cross-build"
go build -o "$TMP/sandboxctl" ./cmd/sandboxctl
GOOS=windows GOARCH=amd64 go build -o "$TMP/sandboxctl.exe" ./cmd/sandboxctl
test -s "$TMP/sandboxctl.exe"

BIN="$TMP/sandboxctl"
DB="$TMP/state.db"

echo "[3/8] baseline and four incremental snapshots"
"$BIN" init --db "$DB" --policy samples/policies/baseline.json > "$TMP/init.json"
for change in 01-add-cache.json 02-delete-directory.json 03-tighten-process.json 04-delete-registry.json; do
  "$BIN" apply --db "$DB" --changeset "samples/changes/$change" > "$TMP/$change.out"
done
"$BIN" current --db "$DB" > "$TMP/current.json"
grep -q '"version": 5' "$TMP/current.json"

echo "[4/8] historical closure and dependency trace"
"$BIN" closure --db "$DB" --version 3 > "$TMP/closure3.json"
grep -q 'rule.file.data.read' "$TMP/closure3.json"
grep -q 'rule.file.audit.read' "$TMP/closure3.json"
grep -q 'E_DEP_INVALIDATED' "$TMP/closure3.json"
"$BIN" closure --db "$DB" --version 4 > "$TMP/closure4.json"
grep -q 'rule.proc.trusted' "$TMP/closure4.json"
grep -q 'rule.proc.audit' "$TMP/closure4.json"
grep -q 'E_DEP_RESTRICTED' "$TMP/closure4.json"
"$BIN" trace --db "$DB" --version 3 --rule rule.file.audit.read > "$TMP/trace.json"
grep -q 'dir.root' "$TMP/trace.json"

echo "[5/8] real file/process probes and byte-stable report"
"$BIN" report --db "$DB" --version 2 --out "$TMP/report-a.json" > "$TMP/report-a-summary.json"
"$BIN" report --db "$DB" --version 2 --out "$TMP/report-b.json" > "$TMP/report-b-summary.json"
cmp "$TMP/report-a.json" "$TMP/report-b.json"
grep -q '"matched": 10' "$TMP/report-a.json"
grep -q '"failed": 0' "$TMP/report-a.json"
grep -q '"mismatched": 0' "$TMP/report-a.json"

echo "[6/8] four malformed/conflict input injections"
for spec in \
  'missing-reference.json:2' \
  'dependency-cycle.json:2' \
  'cross-namespace-reference.json:7' \
  'delete-then-add-conflict.json:6'; do
  file="${spec%%:*}"
  expected="${spec##*:}"
  idb="$TMP/inject-${file%.json}.db"
  "$BIN" init --db "$idb" --policy samples/policies/baseline.json > /dev/null
  before="$("$BIN" current --db "$idb" | tr -d '\n ')"
  set +e
  "$BIN" apply --db "$idb" --changeset "samples/invalid/$file" > /dev/null 2> "$TMP/$file.err"
  rc=$?
  set -e
  test "$rc" -eq "$expected"
  after="$("$BIN" current --db "$idb" | tr -d '\n ')"
  test "$before" = "$after"
done

echo "[7/8] version-gap fallback and digest replacement"
GAPDB="$TMP/gap.db"
"$BIN" init --db "$GAPDB" --policy samples/policies/baseline.json > /dev/null
"$BIN" apply --db "$GAPDB" --changeset samples/changes/01-add-cache.json > /dev/null
"$BIN" inject version-gap --db "$GAPDB" --namespace prod/win-sandbox --version 4 > /dev/null
"$BIN" current --db "$GAPDB" > "$TMP/gap-current.json"
grep -q '"version": 2' "$TMP/gap-current.json"
grep -q 'E_SNAPSHOT_VERSION' "$TMP/gap-current.json"
set +e
"$BIN" apply --db "$GAPDB" --changeset samples/changes/02-delete-directory.json > /dev/null 2> "$TMP/gap-apply.err"
rc=$?
set -e
test "$rc" -eq 3
grep -q 'E_SNAPSHOT_VERSION' "$TMP/gap-apply.err"

DIGESTDB="$TMP/digest.db"
"$BIN" init --db "$DIGESTDB" --policy samples/policies/baseline.json > /dev/null
"$BIN" apply --db "$DIGESTDB" --changeset samples/changes/01-add-cache.json > /dev/null
"$BIN" inject digest-mismatch --db "$DIGESTDB" --namespace prod/win-sandbox --version 2 > /dev/null
"$BIN" current --db "$DIGESTDB" > "$TMP/digest-current.json"
grep -q 'E_SNAPSHOT_VERSION' "$TMP/digest-current.json"

echo "[8/8] interactive shell smoke test"
printf 'current\nquit\n' | "$BIN" shell --db "$DB" > "$TMP/shell.out"
grep -q '"version": 5' "$TMP/shell.out"

printf 'verification OK\nreport-sha256=%s\n' "$(sha256sum "$TMP/report-a.json" | awk '{print $1}')"


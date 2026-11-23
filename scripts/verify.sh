#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "$0")/.." && pwd)
BASE=$(mktemp -d "${TMPDIR:-/tmp}/ntfsacl-verify.XXXXXX")
trap 'rm -rf "$BASE"' EXIT
BIN="$BASE/ntfsacl"
( cd "$ROOT" && go build -o "$BIN" ./cmd/ntfsacl )

pass() { printf 'PASS  %s\n' "$1"; }

W1="$BASE/w1"
W2="$BASE/w2"
"$BIN" init --workdir "$W1" --reset >/dev/null
"$BIN" init --workdir "$W2" --reset >/dev/null
"$BIN" audit --workdir "$W1" --output audit-1.json
"$BIN" audit --workdir "$W2" --output audit-1.json
cmp "$W1/audit-1.json" "$W2/audit-1.json"
pass "fresh-workspace audit JSON is byte-identical"

python3 - "$W1/audit-1.json" <<'PY'
import json,sys
p=json.load(open(sys.argv[1]))
codes=[x['code'] for x in p['summary']['conflicts']]
want=['E_CONFLICT_DENY_OVERRIDE','E_INHERIT_BROKEN','E_FLAG_INCOMPATIBLE','E_ORPHAN_SID']
assert codes==want, (codes,want)
assert p['summary']['node_count']==9
print('PASS  fixture classified all four conflict codes')
PY

"$BIN" replay --workdir "$W1" --version 1 --output replay.json
cmp "$W1/audit-1.json" "$W1/replay.json"
pass "replay reproduces exact committed bytes"

set +e
"$BIN" replay --workdir "$W1" --version 99 --output fallback.json 2> "$BASE/fallback.err"
FALLBACK_RC=$?
set -e
[ "$FALLBACK_RC" -eq 3 ]
cmp "$W1/audit-1.json" "$W1/fallback.json"
pass "missing version returns E_SNAPSHOT_VERSION and exact latest bytes"

"$BIN" writeback --workdir "$W1" --output writeback.json
python3 - "$W1/writeback.json" <<'PY'
import json,sys
p=json.load(open(sys.argv[1]))
def walk(x):
    assert x is not None
    if isinstance(x,dict):
        for v in x.values(): walk(v)
    elif isinstance(x,list):
        for v in x: walk(v)
walk(p)
assert p['status']=='COMPLETED', p
assert p['before_snapshot_version']==2 and p['after_snapshot_version']==3, p
assert len(p['nodes'])==3 and all(n['status']=='COMMITTED' for n in p['nodes']), p
assert [(x['version'],x['kind']) for x in p['snapshot_chain']]==[(1,'audit'),(2,'writeback_before'),(3,'writeback_after')]
print('PASS  controlled writeback committed 3 conflict nodes with no null arrays')
PY
"$BIN" conflicts --workdir "$W1" --output after-conflicts.json
python3 - "$W1/after-conflicts.json" <<'PY'
import json,sys
p=json.load(open(sys.argv[1]))
assert p['conflicts']==[], p['conflicts']
assert [(x['version'],x['kind'],x['change_kind']) for x in p['snapshot_chain']]==[
 (1,'audit','append'),(2,'writeback_before','unchanged'),(3,'writeback_after','incremental')]
print('PASS  post-writeback audit is conflict-free and snapshots compare incrementally')
PY
"$BIN" report --workdir "$W1" --before 2 --after 3 --output compare.json
python3 - "$W1/compare.json" <<'PY'
import json,sys
p=json.load(open(sys.argv[1]))
assert len(p['before_nodes'])==9 and len(p['after_nodes'])==9 and p['after_conflicts']==[], p
changed=[n['rel_path'] for n in p['nodes'] if n['status']=='CHANGED']
assert changed==['docs/plan.txt','finance/budget.txt','legacy/orphan.txt'], changed
assert p['business_codes']==['E_CONFLICT_DENY_OVERRIDE','E_INHERIT_BROKEN','E_FLAG_INCOMPATIBLE','E_ORPHAN_SID'], p['business_codes']
print('PASS  comparison includes full node/source data, conflicts, and business codes')
PY

# Gap fallback: copy a healthy workspace, remove v2, request v3, compare fallback to v1.
W3="$BASE/w3"
cp -a "$W1" "$W3"
python3 - "$W3/.ntfsacl/engine.db" <<'PY'
import sqlite3,sys
c=sqlite3.connect(sys.argv[1]); c.execute('DELETE FROM snapshots WHERE version=2'); c.commit(); c.close()
PY
set +e
"$BIN" replay --workdir "$W3" --version 3 --output gap-fallback.json 2> "$BASE/gap.err"
GAP_RC=$?
set -e
[ "$GAP_RC" -eq 3 ]
grep -q 'E_SNAPSHOT_VERSION' "$BASE/gap.err"
cmp "$W3/gap-fallback.json" "$W3/audit-1.json"
pass "version gap falls back to nearest contiguous snapshot with exact bytes"

# Injected writeback failure must restore the pre-write tree.
W4="$BASE/w4"
"$BIN" init --workdir "$W4" --reset >/dev/null
"$BIN" audit --workdir "$W4" --output audit.json
set +e
"$BIN" writeback --workdir "$W4" --fail-node finance/budget.txt --output rollback.json 2> "$BASE/rollback.err"
ROLLBACK_RC=$?
set -e
[ "$ROLLBACK_RC" -eq 1 ]
python3 - "$W4/audit.json" "$W4/rollback.json" <<'PY'
import json,sys
a=json.load(open(sys.argv[1])); w=json.load(open(sys.argv[2]))
assert w['status']=='ROLLED_BACK' and w['code']=='E_WRITEBACK_ROLLBACK', w
assert w['before_tree_hash']==a['tree_hash']==w['after_tree_hash'], (a['tree_hash'],w)
assert 'docs/plan.txt' in w['rollback']['restored'], w['rollback']
print('PASS  failure rolled back prior committed node and restored tree hash')
PY

# Forced termination at after-write, then restart and recover.
W5="$BASE/w5"
"$BIN" init --workdir "$W5" --reset >/dev/null
"$BIN" audit --workdir "$W5" --output audit.json
ENC=$(python3 -c 'import base64;print(base64.urlsafe_b64encode(b"docs/plan.txt").decode().rstrip("="))')
ACL="$W5/.ntfsacl/acl/$ENC.json"
BEFORE=$(sha256sum "$ACL" | cut -d' ' -f1)
set +e
"$BIN" writeback --workdir "$W5" --crash-at 1 --crash-point after-write --output crashed.json >/dev/null 2>"$BASE/crash.err"
CRASH_RC=$?
set -e
[ "$CRASH_RC" -eq 137 ]
MID=$(sha256sum "$ACL" | cut -d' ' -f1)
[ "$BEFORE" != "$MID" ]
"$BIN" recover --workdir "$W5" --output recovery.json
RESTORED=$(sha256sum "$ACL" | cut -d' ' -f1)
[ "$BEFORE" = "$RESTORED" ]
python3 - "$W5/recovery.json" <<'PY'
import json,sys
p=json.load(open(sys.argv[1]))
assert p['exit_report_committed'] is False
s=p['sessions'][0]
assert s['status']=='RECOVERED' and s['requeue']==['docs/plan.txt'] and s['residual']==[], s
print('PASS  forced termination recovered pending write and requeued node')
PY

# Malformed ACL must not create a snapshot DB.
W6="$BASE/w6"
"$BIN" init --workdir "$W6" --reset --malformed mask >/dev/null
set +e
"$BIN" audit --workdir "$W6" --output malformed.json >/dev/null 2>&1
MALFORMED_RC=$?
set -e
[ "$MALFORMED_RC" -eq 2 ]
[ ! -e "$W6/.ntfsacl/engine.db" ]
python3 - "$W6/malformed.json" <<'PY'
import json,sys
p=json.load(open(sys.argv[1]))
assert p['code']=='E_ACL_MALFORMED' and p['node_path']=='finance/budget.txt' and p['ace_index']==0, p
print('PASS  malformed ACL rejected before inheritance and snapshot creation')
PY

printf '\nAll end-to-end verification checks passed.\n'

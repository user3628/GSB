#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

GATE_PORT="${GATE_PORT:-}"
PEER_PORT="${PEER_PORT:-}"
REFUSED_PORT="${REFUSED_PORT:-}"
STAY=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --gate-port) GATE_PORT="$2"; shift 2 ;;
    --peer-port) PEER_PORT="$2"; shift 2 ;;
    --refused-port) REFUSED_PORT="$2"; shift 2 ;;
    --stay) STAY=1; shift ;;
    -h|--help)
      echo "usage: $0 [--gate-port N] [--peer-port N] [--refused-port N] [--stay]"
      exit 0 ;;
    *) echo "unknown argument: $1" >&2; exit 2 ;;
  esac
done

pick_port() {
  python3 - <<'PY'
import socket
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.bind(("127.0.0.1", 0))
print(s.getsockname()[1])
s.close()
PY
}

[[ -n "$GATE_PORT" ]] || GATE_PORT="$(pick_port)"
[[ -n "$PEER_PORT" ]] || PEER_PORT="$(pick_port)"
[[ -n "$REFUSED_PORT" ]] || REFUSED_PORT="$(pick_port)"

BASE="http://127.0.0.1:${GATE_PORT}"
PEER_ADDR="127.0.0.1:${PEER_PORT}"
REF="1795507200000"
WORK="$(mktemp -d /tmp/nonceguard-demo-XXXXXX)"
DB="${WORK}/nonceguard.db"
GATE_BIN="${WORK}/nonceguard"
PEER_BIN="${WORK}/loopback-peer"

GATE_PID=""
PEER_PID=""
CURL_PID=""
cleanup() {
  set +e
  [[ -n "$CURL_PID" ]] && kill "$CURL_PID" 2>/dev/null
  [[ -n "$GATE_PID" ]] && kill "$GATE_PID" 2>/dev/null
  [[ -n "$PEER_PID" ]] && kill "$PEER_PID" 2>/dev/null
}
trap cleanup EXIT

log() { printf '\n\033[1;36m== %s ==\033[0m\n' "$*"; }
ok() { printf '\033[1;32m✓ %s\033[0m\n' "$*"; }

log "构建纯 Go/SQLite 二进制"
go build -o "$GATE_BIN" ./cmd/gate
go build -o "$PEER_BIN" ./cmd/peer
ok "构建完成：$WORK"

log "启动本地回环代理对端 :$PEER_PORT"
"$PEER_BIN" -addr "$PEER_ADDR" -instance demo-loopback-peer >"${WORK}/peer.log" 2>&1 &
PEER_PID=$!

log "启动 NonceGuard :$GATE_PORT"
"$GATE_BIN" -addr "127.0.0.1:${GATE_PORT}" -db "$DB" -peer-addr "$PEER_ADDR" -fixed-reference-ms "$REF" >"${WORK}/gate.log" 2>&1 &
GATE_PID=$!

for _ in $(seq 1 80); do
  if curl -sf --max-time 1 "$BASE/healthz" >/dev/null 2>&1; then break; fi
  sleep 0.25
done
curl -sf "$BASE/healthz" >/dev/null
ok "服务与 SQLite 已就绪"

log "1/7 固定样本：完整裁决 + 快速预检对比"
curl -fsS "$BASE/v1/sample?scenario=canonical&reference_ms=$REF" -o "${WORK}/sample-envelope.json"
python3 - <<PY
import json
env=json.load(open('${WORK}/sample-envelope.json'))
assert env['ok']
json.dump(env['data']['batch'], open('${WORK}/canonical.json','w'))
PY
curl -sS -X POST "$BASE/v1/batches?mode=compare&precheck_limit=6" \
  -H 'Content-Type: application/json' --data-binary @"${WORK}/canonical.json" \
  -o "${WORK}/compare.json"
python3 - <<PY
import json
e=json.load(open('${WORK}/compare.json'))
assert e['ok'], e
d=e['data']; full=d['full']; cmp=d['comparison']
codes=[r['code'] for r in full['results']]
want=['OK','OK','E_SIGNATURE_MISMATCH','E_TIMESTAMP_EARLY','E_TIMESTAMP_LATE',
      'E_NONCE_REPLAYED','OK','OK','E_CONFLICTING_REQUEST','OK']
assert codes == want, codes
assert full['config_version'] == 1
assert cmp['comparable'] == 6 and cmp['differences'] == 0, cmp
assert cmp['not_prechecked'] == 4
replay=full['results'][5]
assert replay['first_seen_ms'] == $REF and replay['remaining_window_ms'] == 600000
early=full['results'][3]; late=full['results'][4]
assert early['offset_ms'] == -300001 and early['boundary_ms'] == -300000
assert late['offset_ms'] == 300001 and late['boundary_ms'] == 300000
print('codes:', ', '.join(codes))
print('comparison: selected=%d match=%d not_prechecked=%d' % (cmp['comparable'], cmp['matching'], cmp['not_prechecked']))
PY
ok "签名、偏移边界、Nonce 重放、幂等复用/冲突和快速采样对比全部符合预期"

log "2/7 查询配置版本 v1 的结论与占用"
curl -fsS "$BASE/v1/decisions?config_version=1" -o "${WORK}/decisions.json"
curl -fsS "$BASE/v1/nonces?config_version=1" -o "${WORK}/nonces.json"
python3 - <<PY
import json
d=json.load(open('${WORK}/decisions.json'))['data']
n=json.load(open('${WORK}/nonces.json'))['data']
assert d['count'] == 8, d['count']
assert n['count'] == 4, n['count']
print('decisions=%d occupancy=%d' % (d['count'], n['count']))
PY
ok "查询只看到完整提交版本"

log "3/7 非法批次整批拒绝"
curl -sS "$BASE/v1/sample?scenario=invalid" -o "${WORK}/invalid.json"
curl -sS -X POST "$BASE/v1/batches?mode=full" -H 'Content-Type: application/json' \
  --data-binary @"${WORK}/invalid.json" -o "${WORK}/invalid-response.json"
python3 - <<PY
import json
e=json.load(open('${WORK}/invalid-response.json'))
assert (not e['ok']) and e['code'] == 'E_REQUEST_INVALID', e
assert len(e['context']['issues']) >= 2
print('issues:', len(e['context']['issues']))
PY
ok "Nonce 字符集/长度与权重溢出返回 E_REQUEST_INVALID"

log "4/7 配置整体替换、旧版本拒绝与跳号区分"
curl -fsS -X POST "$BASE/v1/config/snapshots?template=v2" -o "${WORK}/v2.json"
python3 - <<PY
import json
e=json.load(open('${WORK}/v2.json'))
assert e['ok'] and e['data']['active'] == 2
PY
curl -sS -X POST "$BASE/v1/config/snapshots?template=v2" -o "${WORK}/stale.json"
curl -sS -X POST "$BASE/v1/config/snapshots?fault=version_gap" -o "${WORK}/gap.json"
python3 - <<PY
import json
stale=json.load(open('${WORK}/stale.json'))
gap=json.load(open('${WORK}/gap.json'))
assert stale['code'] == 'E_STALE_SNAPSHOT', stale
assert gap['code'] == 'E_CONFIG_VERSION_GAP', gap
print(stale['code'], '/', gap['code'])
PY
ok "v2 已原子生效；陈旧版本和跳号返回不同业务码"

log "5/7 回环对端超时、拒绝、迟到确认"
curl -sS -X POST "$BASE/v1/batches?fault=peer_timeout&peer_check=signature&peer_addr=$PEER_ADDR" \
  --data-binary @"${WORK}/canonical.json" -o "${WORK}/timeout.json"
curl -sS -X POST "$BASE/v1/batches?fault=peer_refused&peer_addr=127.0.0.1:${REFUSED_PORT}" \
  --data-binary @"${WORK}/canonical.json" -o "${WORK}/refused.json"
curl -sS -X POST "$BASE/v1/batches?fault=peer_late_ack&peer_check=nonce&peer_addr=$PEER_ADDR&peer_timeout_ms=3000" \
  --data-binary @"${WORK}/canonical.json" -o "${WORK}/late.json"
python3 - <<PY
import json
for name, code in [('timeout','E_PEER_TIMEOUT'), ('refused','E_PEER_REFUSED'), ('late','E_PEER_LATE_ACK')]:
    e=json.load(open('${WORK}/%s.json' % name))
    assert e['code'] == code, (name, e)
    assert e['context'], name
    print('%-8s %s elapsed=%s' % (name, code, e['context'].get('elapsed_ms')))
PY
ok "三种真实连接行为产生互不相同的 JSON 错误码"

log "6/7 批次中断业务码与在线恢复"
curl -sS -X POST "$BASE/v1/batches?fault=batch_interrupt" \
  -H 'Content-Type: application/json' --data-binary @"${WORK}/canonical.json" \
  -o "${WORK}/interrupt.json"
python3 - <<PY
import json
e=json.load(open('${WORK}/interrupt.json'))
assert e['code'] == 'E_BATCH_INTERRUPTED', e
assert e['context']['open_intents'] == 10
print(e['code'], 'open_intents=', e['context']['open_intents'])
PY
curl -fsS -X POST "$BASE/v1/recovery/run" -o "${WORK}/recovery-live.json"
python3 - <<PY
import json
r=json.load(open('${WORK}/recovery-live.json'))['data']
assert r['open_intents_found'] == 10 and r['aborted_intents'] == 10, r
PY
ok "中断可区分，未完成意图可扫描并标记中止"

log "7/7 SIGKILL 强制终止、重启恢复与续跑"
curl -fsS "$BASE/v1/sample?scenario=valid&count=120&batch_id=kill-demo&reference_ms=$REF" -o "${WORK}/kill-sample.json"
python3 - <<PY
import json
e=json.load(open('${WORK}/kill-sample.json'))
json.dump(e['data']['batch'], open('${WORK}/kill-batch.json','w'))
PY
curl -sS -X POST "$BASE/v1/batches?mode=full&record_delay_ms=80" \
  -H 'Content-Type: application/json' --data-binary @"${WORK}/kill-batch.json" \
  -o "${WORK}/kill-curl-response.json" &
CURL_PID=$!
sleep 0.45
kill -9 "$GATE_PID"
wait "$GATE_PID" 2>/dev/null || true
kill "$CURL_PID" 2>/dev/null || true
CURL_PID=""
ok "已对服务进程执行 kill -9"

"$GATE_BIN" -addr "127.0.0.1:${GATE_PORT}" -db "$DB" -peer-addr "$PEER_ADDR" -fixed-reference-ms "$REF" >>"${WORK}/gate-restart.log" 2>&1 &
GATE_PID=$!
for _ in $(seq 1 80); do
  if curl -sf --max-time 1 "$BASE/healthz" >/dev/null 2>&1; then break; fi
  sleep 0.25
done
curl -sf "$BASE/healthz" >/dev/null
curl -fsS "$BASE/v1/recovery" -o "${WORK}/recovery-after-kill.json"
python3 - <<PY
import json
d=json.load(open('${WORK}/recovery-after-kill.json'))['data']
r=d['latest']
assert r['open_intents_found'] == 120, r
assert 'kill-demo' in r['interrupted_batches'], r
print('run_id=%s open_intents=%d aborted_batches=%d' % (r['run_id'], r['open_intents_found'], r['aborted_batches']))
for fact in r['facts']: print(' -', fact)
PY
curl -fsS -X POST "$BASE/v1/batches/kill-demo/resume" -o "${WORK}/resume.json"
python3 - <<PY
import json
d=json.load(open('${WORK}/resume.json'))['data']
assert d['total'] == 120 and len(d['results']) == 120, d
assert all(r['code'] == 'OK' for r in d['results']), [r['code'] for r in d['results'] if r['code']!='OK'][:5]
print('resumed decisions=%d accepted=%d' % (d['total'], d['accepted']))
PY
ok "重启恢复报告基于落盘事实，同一批次可续跑并全部裁决成功"

printf '\n\033[1;32mDemo passed.\033[0m\n'
echo "UI/API: $BASE"
echo "Work directory: $WORK"
echo "Logs: ${WORK}/gate.log ${WORK}/peer.log"

if [[ "$STAY" -eq 1 ]]; then
  echo "按 Ctrl-C 停止演示服务。"
  trap - EXIT
  wait "$GATE_PID"
fi

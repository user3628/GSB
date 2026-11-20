#!/usr/bin/env bash
# 可重复执行的 HTTP 接口演示（仅监听 127.0.0.1）。
set -euo pipefail
cd "$(dirname "$0")/.."
export PATH="$PWD/.venv/bin:$PATH"
PORT=8765
WS=demo_output/http_demo_ws
[ -d "$WS" ] && find "$WS" -mindepth 1 -delete

mmpipe-serve -w "$WS" --port "$PORT" >demo_output/http_server.log 2>&1 &
SERVER_PID=$!
trap 'kill $SERVER_PID 2>/dev/null || true' EXIT

python3 - "$PORT" <<'PY'
import json, sys, time, urllib.request
port = sys.argv[1]
base = f"http://127.0.0.1:{port}"

def call(method, path, payload=None):
    data = json.dumps(payload).encode() if payload is not None else None
    req = urllib.request.Request(base + path, data=data, method=method,
                                 headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=10) as resp:
        return json.loads(resp.read())

for _ in range(100):
    try:
        print("health:", call("GET", "/health")["ok"])
        break
    except Exception:
        time.sleep(0.1)

call("POST", "/workspace/init")
call("POST", "/import", {"texts": "samples/texts.jsonl",
                         "images": "samples/images.json",
                         "config": "samples/pipeline_config.json"})
call("POST", "/run", {})
for _ in range(100):
    body = call("GET", "/run/status")
    if not body["running"]:
        break
    time.sleep(0.1)
status = call("GET", "/status")
print("head_seq:", status["head_seq"])
print("stages:", {k: v["status"] for k, v in status["stages"].items()})
agg = call("GET", "/aggregate")
print("conflicts retained:", agg["aggregate"]["summary"]["conflict_sample_ids"])
print("stop:", call("POST", "/stop")["stop_requested"])
PY

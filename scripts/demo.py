"""Repeatable local demo of the fault injection workbench.

Covers four drills against the deterministic samples in ./samples:
  1. normal round execution (scheduling, budget, observations)
  2. unsupported fault (E_FAULT_UNSUPPORTED with a mismatch reason)
  3. real connection timeout (E_INJECTION_TIMEOUT, socket released)
  4. hard kill mid-round + recovery from persisted intents

Usage: .venv/bin/python scripts/demo.py
"""

import json
import os
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from test_endpoints.servers import (  # noqa: E402
    BlackholeServer,
    HealthyServer,
    port_is_free,
)

API_PORT = int(os.environ.get("FIW_DEMO_PORT", "18090"))
API = f"http://127.0.0.1:{API_PORT}"
DB_PATH = ROOT / "data" / "fiw.db"
SAMPLE_ENDPOINTS = ROOT / "samples" / "endpoints.json"
SAMPLE_PLAN = ROOT / "samples" / "plan.json"

PORTS = {"ep-web-1": 18101, "ep-web-2": 18102, "ep-api-1": 18103,
         "ep-db-1 (blackhole)": 18201, "ep-cache-1 (unlistened)": 18301,
         "workbench-api": API_PORT}


def banner(title):
    print("\n" + "=" * 72)
    print(f"  {title}")
    print("=" * 72)


def http(method, path, body=None):
    req = urllib.request.Request(
        API + path, method=method,
        data=json.dumps(body).encode() if body is not None else None,
        headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read())


def wait_for_api(timeout=15):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            http("GET", "/health")
            return True
        except OSError:
            time.sleep(0.2)
    return False


def start_api(extra_env=None):
    env = dict(os.environ)
    env["FIW_DB"] = str(DB_PATH)
    env["FIW_PORT"] = str(API_PORT)
    env.update(extra_env or {})
    proc = subprocess.Popen(
        [sys.executable, "-m", "fiw.app"], cwd=ROOT, env=env,
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    if not wait_for_api():
        proc.kill()
        raise RuntimeError("workbench API did not come up")
    return proc


def stop_api(proc):
    proc.terminate()
    try:
        proc.wait(timeout=10)
    except subprocess.TimeoutExpired:
        proc.kill()


def show_round(rnd):
    print(f"  round={rnd['round_no']} committed_at={rnd['committed_at']}")
    budget = rnd["budget"]
    print(f"  budget: limit={budget['limit_ms']}ms used={budget['used_ms']}ms "
          f"remaining={budget['remaining_ms']}ms quota={budget['injection_quota']} "
          f"concurrency={budget['concurrency_limit']}")
    print("  trajectory:")
    for entry in rnd["trajectory"]:
        code = f" [{entry['code']}]" if entry["code"] else ""
        reason = f" -- {entry['reason']}" if entry["reason"] else ""
        print(f"    {entry['item_id']:>5} {entry['fault_type']:<18} "
              f"-> {entry['target']:<10} {entry['outcome']}{code} "
              f"({entry['elapsed_ms']}ms){reason}")
    for entry in rnd["skipped"]:
        print(f"    {entry['item_id']:>5} {entry['fault_type']:<18} "
              f"-> {entry['target']:<10} SKIPPED ({entry['reason']})")
    print("  observations:")
    for eid, obs in rnd["observations"].items():
        prop = ""
        if obs["propagation"]:
            ups = ", ".join(p["upstream"] for p in obs["propagation"])
            prop = f" (propagated from {ups})"
        print(f"    {eid:<10} {obs['conclusion']}{prop}")


def main():
    banner("0. preflight: loopback ports must be bindable/free")
    for name, port in PORTS.items():
        deadline = time.time() + 60
        while not port_is_free(port) and time.time() < deadline:
            time.sleep(0.5)  # wait out TIME_WAIT from a previous run
        if not port_is_free(port):
            raise SystemExit(f"port {port} ({name}) is occupied; aborting demo")
        print(f"  port {port:<6} free  ({name})")

    for suffix in ("", "-wal", "-shm"):
        (DB_PATH.parent / (DB_PATH.name + suffix)).unlink(missing_ok=True)

    servers = [
        HealthyServer(PORTS["ep-web-1"], "ep-web-1").start(),
        HealthyServer(PORTS["ep-web-2"], "ep-web-2").start(),
        HealthyServer(PORTS["ep-api-1"], "ep-api-1").start(),
        BlackholeServer(PORTS["ep-db-1 (blackhole)"]).start(),
    ]
    api = None
    try:
        api = start_api()

        banner("1. drill: normal round (load plan, RUNNING, round 1)")
        print(" ", http("POST", "/plans/load", {
            "endpoints_path": str(SAMPLE_ENDPOINTS),
            "plan_path": str(SAMPLE_PLAN)}))
        print(" ", http("POST", "/plans/plan-demo/transitions",
                        {"target": "RUNNING"}))
        rnd = http("POST", "/plans/plan-demo/rounds/1/run")
        show_round(rnd)
        again = http("POST", "/plans/plan-demo/rounds/1/run")
        print(f"  idempotent re-trigger of round 1 -> reused={again['reused']}")

        banner("2. drill: unsupported fault (refused on a listening port)")
        it4 = [t for t in rnd["trajectory"] if t["item_id"] == "it-4"][0]
        print(f"  it-4 outcome={it4['outcome']} code={it4['code']}")
        print(f"  reason: {it4['reason']}")

        banner("3. drill: real timeout against a non-responding socket")
        it2 = [t for t in rnd["trajectory"] if t["item_id"] == "it-2"][0]
        print(f"  it-2 outcome={it2['outcome']} code={it2['code']} "
              f"elapsed={it2['elapsed_ms']}ms evidence={it2['evidence']}")

        banner("4. drill: hard kill mid-round, then recovery")
        stop_api(api)
        api = start_api({"FIW_CRASH_AFTER_INTENT": "plan-demo:2"})
        try:
            http("POST", "/plans/plan-demo/rounds/2/run")
            print("  ERROR: expected the server to die mid-round")
        except (urllib.error.URLError, ConnectionError, OSError) as exc:
            print(f"  round 2 request failed as the process was killed: {exc}")
        api.wait(timeout=10)
        print(f"  server exit code: {api.returncode} (hard kill)")

        api = start_api()
        report = http("GET", "/recovery/startup")["report"]
        print(f"  pending intents found at restart: "
              f"{report['scanned_pending_intents']}")
        for action in report["actions"]:
            print(f"  recovery action: {json.dumps(action)}")
        plan = http("GET", "/plans/plan-demo")
        print(f"  plan now: status={plan['status']} "
              f"last_committed_round={plan['last_committed_round']}")
        rnd1 = http("GET", "/plans/plan-demo/rounds/1")
        print(f"  round 1 still committed ({len(rnd1['trajectory'])} "
              f"trajectory entries); round 2 was rolled back")

        banner("all four drills completed")
    finally:
        if api is not None and api.poll() is None:
            stop_api(api)
        for srv in servers:
            srv.stop()


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""Deterministic end-to-end demo of the feature workbench.

Covers: sample generation, full run with a fixed summary digest, byte-identical
reproduction, incremental recompute after partial config changes, latency
budget errors, graceful stop + resume, hard kill + resume, publish protection
error codes, and the local HTTP interface. Exits non-zero if any check fails.
"""

import json
import os
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PY = sys.executable
DEMO = os.path.join(ROOT, "demo_workspace")

FAILURES = []


def check(name, cond, detail=""):
    mark = "PASS" if cond else "FAIL"
    print("  [%s] %s%s" % (mark, name, (" -- " + str(detail)) if detail else ""))
    if not cond:
        FAILURES.append(name)


def section(title):
    print("\n=== %s ===" % title)


def cli(state, *args):
    return subprocess.run(
        [PY, "-m", "workbench", "--state", state] + list(args),
        cwd=ROOT, capture_output=True, text=True,
    )


def cli_json(state, *args):
    p = cli(state, *args)
    if p.returncode != 0:
        raise RuntimeError("cli failed: %s\n%s" % (args, p.stderr))
    return json.loads(p.stdout)


def fresh_state(name):
    state = os.path.join(DEMO, name)
    os.makedirs(state, exist_ok=True)
    return state


def bootstrap(state):
    cli_json(state, "config-init", "--from", os.path.join(DEMO, "config.json"))
    cli_json(state, "import-sample", "--path", os.path.join(DEMO, "sample.csv"))


def main():
    shutil.rmtree(DEMO, ignore_errors=True)
    os.makedirs(DEMO)
    state = fresh_state("state")

    section("1. deterministic sample + config")
    gen = cli_json(state, "gen-sample", "--out", os.path.join(DEMO, "sample.csv"))
    print("  sample: rows=%d categorical=%s" % (gen["generated"]["rows"], gen["generated"]["categorical"]))
    cli_json(state, "gen-config", "--out", os.path.join(DEMO, "config.json"))
    bootstrap(state)
    print("  config initialized at version 1")

    section("2. full run (fixed summary digest)")
    r1 = cli_json(state, "run")
    d1 = r1["summary_digest"]
    print("  digest:", d1)
    print("  stages:", [(s["stage"], s["source"]) for s in r1["stages"]])
    print("  encoded=%d selected=%d" % (r1["summary"]["encoded_features"],
                                        r1["summary"]["selected_features"]))
    enc = cli_json(state, "stage", "encode")
    te = enc["target_encoding"]["loyalty"]
    cat0 = sorted(te["categories"])[0]
    d0 = te["categories"][cat0]
    print("  target encoding diagnostics (loyalty, category %s):" % cat0)
    print("    fold_values=%s" % ["%.6f" % v for v in d0["fold_values"]])
    print("    spread=%.6f unseen_folds=%d" % (d0["spread"], d0["unseen_folds"]))
    check("te diagnostics expose per-fold spread", d0["spread"] > 0)

    section("3. byte-identical reproduction in a fresh state")
    state_b = fresh_state("state-repro")
    bootstrap(state_b)
    r_b = cli_json(state_b, "run")
    check("fresh state reproduces digest", r_b["summary_digest"] == d1)
    check("summaries identical", r_b["summary"] == r1["summary"])

    section("4. idempotent rerun (journal reuse, no recompute)")
    r2 = cli_json(state, "run")
    check("digest unchanged", r2["summary_digest"] == d1)
    check("all stages from journal", all(s["source"] == "journal" for s in r2["stages"]))

    section("5. incremental recompute after partial config change")
    out = cli_json(state, "config-set", "--set", "selection.mutual_info.threshold=0.002")
    print("  changed_sections:", out["changed_sections"])
    r3 = cli_json(state, "run")
    src = {s["stage"]: s["source"] for s in r3["stages"]}
    print("  sources:", src)
    check("infer/encode reused", src["infer"] == "journal" and src["encode"] == "journal")
    check("select/summarize recomputed",
          src["select"] == "computed" and src["summarize"] == "computed")
    v = cli_json(state, "verify")
    check("incremental == full rebuild (verify)", v["consistent"])
    out = cli_json(state, "config-set", "--set", "encoding.hash.n_buckets=12")
    r4 = cli_json(state, "run")
    src = {s["stage"]: s["source"] for s in r4["stages"]}
    print("  after encoding change:", src)
    check("only infer reused after encoding change",
          src["infer"] == "journal" and src["encode"] == "computed")
    check("verify still consistent", cli_json(state, "verify")["consistent"])

    section("6. latency budget -> E_LATENCY_BUDGET")
    p = cli(state, "run", "--budget-ms", "0")
    err = json.loads(p.stderr)
    print("  exit=%d code=%s" % (p.returncode, err["error"]["code"]))
    check("exit code 2", p.returncode == 2)
    check("E_LATENCY_BUDGET", err["error"]["code"] == "E_LATENCY_BUDGET")

    section("7. graceful stop (SIGTERM) + resume consistency")
    cli_json(state, "config-set", "--set", "encoding.hash.n_buckets=16")
    proc = subprocess.Popen(
        [PY, "-m", "workbench", "--state", state, "run", "--stage-sleep-ms", "900"],
        cwd=ROOT, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    time.sleep(1.4)
    proc.send_signal(signal.SIGTERM)
    out, err = proc.communicate(timeout=60)
    rep = json.loads(out)
    print("  exit=%d status=%s completed=%s" % (proc.returncode, rep["status"],
                                                rep.get("completed_stages")))
    check("graceful stop exits 0", proc.returncode == 0)
    check("status stopped", rep["status"] == "stopped")
    check("partial journal", 1 <= len(rep["completed_stages"]) < 4)
    r5 = cli_json(state, "resume")
    state_ref = fresh_state("state-ref-stop")
    bootstrap(state_ref)
    cli_json(state_ref, "config-set", "--set", "selection.mutual_info.threshold=0.002")
    cli_json(state_ref, "config-set", "--set", "encoding.hash.n_buckets=16")
    ref = cli_json(state_ref, "run")
    check("resume digest == uninterrupted digest", r5["summary_digest"] == ref["summary_digest"])

    section("8. hard kill (SIGKILL) + resume from last consistent record")
    cli_json(state, "config-set", "--set", "encoding.hash.n_buckets=20")
    head_before = cli_json(state, "status")["journal"]["head_seq"]
    proc = subprocess.Popen(
        [PY, "-m", "workbench", "--state", state, "run", "--stage-sleep-ms", "900"],
        cwd=ROOT, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    time.sleep(1.4)
    proc.send_signal(signal.SIGKILL)
    proc.communicate(timeout=60)
    print("  killed with exit=%d" % proc.returncode)
    check("SIGKILL exit != 0", proc.returncode != 0)
    head = cli_json(state, "status")["journal"]["head_seq"]
    print("  journal head: before=%d after_kill=%d" % (head_before, head))
    check("journal truncated at last consistent record",
          head_before <= head < head_before + 3)
    r6 = cli_json(state, "resume")
    head_final = cli_json(state, "status")["journal"]["head_seq"]
    check("resume published remaining stages", head_final == head_before + 3)
    cli_json(state_ref, "config-set", "--set", "encoding.hash.n_buckets=20")
    ref2 = cli_json(state_ref, "run")
    check("resume-after-kill digest == reference", r6["summary_digest"] == ref2["summary_digest"])
    check("verify consistent after kill+resume", cli_json(state, "verify")["consistent"])

    section("9. publish protection error codes")
    sys.path.insert(0, ROOT)
    from workbench.errors import InvalidStageError, StaleCheckpointError
    from workbench.store import Store
    from workbench.util import sha256_bytes

    st = Store(fresh_state("state-lock"))
    st.ensure()
    st.write_artifact("k1", b"m1")
    st.write_artifact("k2", b"m2")

    def rec(seq, stage, upstream, key):
        return {"seq": seq, "stage": stage, "sample_fp": "s", "config_version": 1,
                "stage_fp": "fp-%d" % seq, "cache_key": key, "upstream_fp": upstream,
                "artifact_digest": sha256_bytes(b"m%d" % (1 if key == "k1" else 2))}

    st.publish(rec(1, "infer", "genesis:s", "k1"))
    try:
        st.publish(rec(1, "infer", "genesis:s", "k1"))
        check("duplicate publish rejected", False)
    except StaleCheckpointError as e:
        check("duplicate publish -> E_STALE_CHECKPOINT", e.code == "E_STALE_CHECKPOINT")
    try:
        st.publish(rec(2, "select", "fp-1", "k2"))
        check("missing predecessor rejected", False)
    except InvalidStageError as e:
        check("missing predecessor -> E_INVALID_STAGE", e.code == "E_INVALID_STAGE")

    section("10. local HTTP interface")
    http_state = fresh_state("state-http")
    bootstrap(http_state)
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    sock.close()
    srv = subprocess.Popen(
        [PY, "-m", "workbench", "--state", http_state, "serve",
         "--host", "127.0.0.1", "--port", str(port)],
        cwd=ROOT, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    base = "http://127.0.0.1:%d" % port
    try:
        for _ in range(100):
            try:
                urllib.request.urlopen(base + "/health", timeout=2)
                break
            except OSError:
                time.sleep(0.05)
        req = urllib.request.Request(base + "/run", data=b"{}", method="POST",
                                     headers={"Content-Type": "application/json"})
        body = json.loads(urllib.request.urlopen(req, timeout=30).read())
        check("HTTP /run completes", body["status"] == "completed")
        sel = json.loads(urllib.request.urlopen(base + "/selection", timeout=10).read())
        check("HTTP /selection lists features",
              sel["kept_count"] > 0 and sel["dropped_count"] > 0)
        req = urllib.request.Request(
            base + "/config",
            data=json.dumps({"set": {"selection.method": "variance"}}).encode(),
            method="PATCH", headers={"Content-Type": "application/json"})
        json.loads(urllib.request.urlopen(req, timeout=10).read())
        req = urllib.request.Request(base + "/run", data=b"{}", method="POST",
                                     headers={"Content-Type": "application/json"})
        body = json.loads(urllib.request.urlopen(req, timeout=30).read())
        src = {s["stage"]: s["source"] for s in body["stages"]}
        check("HTTP incremental reuses upstream", src["infer"] == "journal")
        try:
            req = urllib.request.Request(base + "/run",
                                         data=json.dumps({"budget_ms": 0}).encode(),
                                         method="POST",
                                         headers={"Content-Type": "application/json"})
            urllib.request.urlopen(req, timeout=10)
            check("HTTP latency budget error", False)
        except urllib.error.HTTPError as e:
            err = json.loads(e.read())
            check("HTTP E_LATENCY_BUDGET", err["error"]["code"] == "E_LATENCY_BUDGET")
        stat = json.loads(urllib.request.urlopen(base + "/status", timeout=10).read())
        check("HTTP /status shows versions", stat["config"]["version"] == 2)
    finally:
        srv.send_signal(signal.SIGTERM)
        srv.wait(timeout=15)
    check("HTTP server graceful shutdown exit 0", srv.returncode == 0)

    section("fixed summary")
    print("  default-config full-run digest: %s" % d1)
    if FAILURES:
        print("\nDEMO FAILED: %s" % ", ".join(FAILURES))
        return 1
    print("\nDEMO OK: all checks passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

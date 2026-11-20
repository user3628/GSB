"""End-to-end deterministic demo of the LLM data pipeline.

Scenarios (each uses its own workspace under demo_runs/):
  S1  full run, records artifact digests
  S2  injected stop after 'normalize' (env PIPELINE_STOP_AFTER), then
      resume; final mix must be byte-identical to S1
  S3  version changes: mix-weight change invalidates only 'mix';
      a shard change invalidates everything downstream
  S4  business error codes: E_INVALID_STAGE and E_STALE_CHECKPOINT

Run: python demo.py   (Windows: py -3.11 demo.py)
"""

import json
import os
import shutil
import subprocess
import sys

ROOT = os.path.dirname(os.path.abspath(__file__))
RUNS = os.path.join(ROOT, "demo_runs")
APP = [sys.executable, "-m", "pipeline.app"]

FAILURES = []


def check(name, condition, detail=""):
    mark = "PASS" if condition else "FAIL"
    print("  [%s] %s%s" % (mark, name, (" -- " + detail) if detail else ""))
    if not condition:
        FAILURES.append(name)


def cli(workspace, *args, env_extra=None, expect=0):
    env = dict(os.environ)
    if env_extra:
        env.update(env_extra)
    proc = subprocess.run(
        APP + ["--workspace", workspace] + list(args),
        capture_output=True, text=True, cwd=ROOT, env=env)
    if expect is not None and proc.returncode != expect:
        raise AssertionError(
            "cli %s exited %d (expected %d)\nstdout:\n%s\nstderr:\n%s"
            % (args, proc.returncode, expect, proc.stdout, proc.stderr))
    return proc


def fresh_workspace(name):
    path = os.path.join(RUNS, name)
    if os.path.isdir(path):
        shutil.rmtree(path)
    os.makedirs(path)
    shutil.copytree(os.path.join(ROOT, "data"), os.path.join(path, "data"))
    return path


def file_bytes(workspace, *parts):
    with open(os.path.join(workspace, *parts), "rb") as fh:
        return fh.read()


def sha256_file(workspace, *parts):
    import hashlib
    return hashlib.sha256(file_bytes(workspace, *parts)).hexdigest()


def status_map(workspace):
    proc = cli(workspace, "status", "--json")
    return {s["stage"]: s for s in json.loads(proc.stdout)}


def scenario_full_run():
    print("\n=== S1: full run ===")
    ws = fresh_workspace("full")
    out = cli(ws, "run").stdout
    print("  " + "\n  ".join(out.strip().splitlines()))
    digests = {
        stage: sha256_file(ws, "artifacts", name)
        for stage, name in [("normalize", "normalize.jsonl"),
                            ("dedup", "dedup.jsonl"),
                            ("mix", "mix.jsonl")]
    }
    print("  artifact digests: %s"
          % {k: v[:12] for k, v in digests.items()})
    states = status_map(ws)
    check("S1 all stages completed",
          all(s["state"] == "completed" for s in states.values()))
    check("S1 checkpoint seqs are 1,2,3",
          [states[s]["seq"] for s in ("normalize", "dedup", "mix")]
          == [1, 2, 3])
    return ws, digests


def scenario_stop_resume(reference):
    print("\n=== S2: injected stop after 'normalize', then resume ===")
    ws = fresh_workspace("resume")
    proc = cli(ws, "run", env_extra={"PIPELINE_STOP_AFTER": "normalize"},
               expect=75)
    print("  stopped run output: " + " | ".join(proc.stdout.strip().splitlines()[-2:]))
    states = status_map(ws)
    check("S2 normalize completed before stop",
          states["normalize"]["state"] == "completed")
    check("S2 dedup/mix still pending after stop",
          states["dedup"]["state"] == "pending"
          and states["mix"]["state"] == "pending")
    out = cli(ws, "resume").stdout
    print("  resume output: " + " | ".join(out.strip().splitlines()))
    check("S2 resume reuses published normalize (no recompute)",
          "[reuse] stage 'normalize'" in out)
    for stage, name in [("normalize", "normalize.jsonl"),
                        ("dedup", "dedup.jsonl"),
                        ("mix", "mix.jsonl")]:
        same = sha256_file(ws, "artifacts", name) == reference[stage]
        check("S2 %s artifact byte-identical to full run" % stage, same)
    states = status_map(ws)
    check("S2 checkpoint seqs after resume are 1,2,3",
          [states[s]["seq"] for s in ("normalize", "dedup", "mix")]
          == [1, 2, 3])
    return ws


def scenario_invalidation(ws):
    print("\n=== S3: version-change detection and invalidation ===")
    before = status_map(ws)
    dedup_digest_before = before["dedup"]["artifact_digest"]
    mix_digest_before = before["mix"]["artifact_digest"]

    config_path = os.path.join(ws, "data", "mix_config.json")
    with open(config_path, encoding="utf-8") as fh:
        config = json.load(fh)
    config["mix"]["total_samples"] = 8
    with open(config_path, "w", encoding="utf-8") as fh:
        json.dump(config, fh, ensure_ascii=False, indent=2)
    print("  changed mix config: total_samples 10 -> 8")
    states = status_map(ws)
    check("S3 mix invalidated after weight change",
          states["mix"]["state"] == "invalidated",
          states["mix"]["reason"] or "")
    check("S3 normalize/dedup stay completed",
          states["normalize"]["state"] == "completed"
          and states["dedup"]["state"] == "completed")
    out = cli(ws, "resume").stdout
    print("  resume output: " + " | ".join(out.strip().splitlines()))
    after = status_map(ws)
    check("S3 dedup artifact reused (digest unchanged)",
          after["dedup"]["artifact_digest"] == dedup_digest_before)
    check("S3 mix rebuilt (digest changed)",
          after["mix"]["artifact_digest"] != mix_digest_before)
    check("S3 mix checkpoint seq advanced to 4",
          after["mix"]["seq"] == 4)

    with open(os.path.join(ws, "data", "shards", "shard_c.jsonl"),
              "a", encoding="utf-8") as fh:
        fh.write(json.dumps({"id": "c7", "source": "forum", "lang": "en",
                             "text": "Checkpointing enables crash recovery"},
                            ensure_ascii=False) + "\n")
    print("  appended sample c7 to shard_c.jsonl")
    states = status_map(ws)
    check("S3 all stages invalidated after shard change",
          all(states[s]["state"] == "invalidated" for s in
              ("normalize", "dedup", "mix")))
    out = cli(ws, "resume").stdout
    print("  resume output: " + " | ".join(out.strip().splitlines()))
    after = status_map(ws)
    check("S3 all stages rebuilt and completed",
          all(after[s]["state"] == "completed" for s in
              ("normalize", "dedup", "mix")))
    check("S3 dedup digest changed after shard change",
          after["dedup"]["artifact_digest"] != dedup_digest_before)
    stats = cli(ws, "stats").stdout
    print("  stats after rebuild:\n   " + "\n   ".join(stats.strip().splitlines()))


def scenario_error_codes():
    print("\n=== S4: business error codes ===")
    ws = fresh_workspace("errors")
    proc = cli(ws, "run-stage", "mix", expect=2)
    check("S4 run-stage mix without upstream -> E_INVALID_STAGE",
          "E_INVALID_STAGE" in proc.stdout, proc.stdout.strip())
    proc = cli(ws, "run", "--target", "bogus", expect=2)
    check("S4 run --target bogus -> E_INVALID_STAGE",
          "E_INVALID_STAGE" in proc.stdout, proc.stdout.strip())
    proc = cli(ws, "stats", expect=2)
    check("S4 stats without mix artifact -> E_INVALID_STAGE",
          "E_INVALID_STAGE" in proc.stdout, proc.stdout.strip())

    sys.path.insert(0, ROOT)
    from pipeline.errors import E_STALE_CHECKPOINT, PipelineError
    from pipeline.store import CheckpointStore
    store = CheckpointStore(ws)
    store.publish("normalize", {"shards": "v1"}, "sha256:" + "0" * 64,
                  "artifacts/normalize.jsonl", expected_seq=0)
    try:
        store.publish("normalize", {"shards": "v1"}, "sha256:" + "1" * 64,
                      "artifacts/normalize.jsonl", expected_seq=0)
        check("S4 stale publish rejected", False, "no error raised")
    except PipelineError as exc:
        check("S4 stale publish rejected with E_STALE_CHECKPOINT",
              exc.code == E_STALE_CHECKPOINT, exc.message)
    check("S4 store seq unchanged after rejection",
          store.current_seq() == 1)


def main():
    os.makedirs(RUNS, exist_ok=True)
    _, reference = scenario_full_run()
    ws = scenario_stop_resume(reference)
    scenario_invalidation(ws)
    scenario_error_codes()
    print("\n=== summary ===")
    if FAILURES:
        print("FAILED: %s" % ", ".join(FAILURES))
        return 1
    print("all demo checks passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())

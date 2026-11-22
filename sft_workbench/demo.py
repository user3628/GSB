"""End-to-end local demo of the instruction-data workbench.

Steps:
  1. generate deterministic shards (fixed seed)
  2. fresh full run; record the fixed run digest
  3. rerun from scratch; assert byte-identical artifacts (determinism)
  4. graceful stop mid-import, then resume; assert byte-identical outputs
  5. incremental invalidation: change the dedup threshold, watch downstream
     stages get invalidated and re-executed while import stays cached
  6. schema interop: v1 -> internal -> v2 -> internal keeps content and
     fingerprint; legacy v1 quality upgrade annotations are shown
  7. snapshot replacement detection: normal / tamper / rollback
  8. concurrent publish protection via a real file lock
  9. HTTP API smoke run through the FastAPI test client

All state lives under the demo root directory; nothing external is used.
"""
from __future__ import annotations

import json
import multiprocessing
import shutil
import sys
from pathlib import Path

from .checkpoint import STAGES, CheckpointRecord, CheckpointStore, compute_version_fingerprint
from .codec import decode_sample, encode_v1, encode_v2
from .errors import E_STALE_CHECKPOINT, WorkbenchError
from .generator import generate_shards
from .pipeline import Pipeline, PipelineConfig
from .util import digest_obj, sha256_hex


def _tree_digest(root: Path) -> str:
    entries = {}
    for path in sorted(root.rglob("*")):
        if path.is_file():
            entries[str(path.relative_to(root))] = sha256_hex(path.read_bytes())
    return digest_obj(entries)


def _fresh_dir(path: Path) -> Path:
    if path.exists():
        shutil.rmtree(path)
    path.mkdir(parents=True)
    return path


def _publish_attempt(workspace: str, seq: int, stage: str, prev: str, queue) -> None:
    store = CheckpointStore(workspace)
    record = CheckpointRecord(
        stage=stage,
        seq=seq,
        input_fingerprint="in",
        config_fingerprint="cfg",
        artifacts={},
        artifact_digest=digest_obj({}),
        version_fingerprint=compute_version_fingerprint({stage: digest_obj({})}, "cfg"),
        prev_fingerprint=prev,
    )
    try:
        store.publish(record)
        queue.put(("ok", seq))
    except WorkbenchError as exc:
        queue.put((exc.code, seq))


def run_demo(demo_root: str | Path = "demo_run") -> dict[str, object]:
    root = Path(demo_root).resolve()
    _fresh_dir(root)
    summary: dict[str, object] = {"demo_root": str(root)}
    log = print

    log("== 1. generate deterministic shards ==")
    data_dir = root / "data"
    shards = generate_shards(data_dir)
    shard_digests = {p.name: sha256_hex(p.read_bytes()) for p in shards}
    summary["shard_digests"] = shard_digests
    for name, digest in shard_digests.items():
        log(f"  {name}: {digest[:16]}...")

    log("== 2. fresh full run ==")
    ws1 = root / "ws-full"
    pipe = Pipeline(ws1)
    result = pipe.run(shard_paths=[str(p) for p in shards])
    assert result["status"] == "completed", result
    run_digest = _tree_digest(ws1 / "artifacts")
    summary["run_digest"] = run_digest
    summary["version_fingerprint"] = result["version_fingerprint"]
    log(f"  stages: {[s['status'] for s in result['stages']]}")
    log(f"  version fingerprint: {result['version_fingerprint'][:16]}...")
    log(f"  artifact tree digest: {run_digest[:16]}...")

    dedup_summary = json.loads((ws1 / "artifacts/dedup_summary.json").read_text())
    import_errors = json.loads((ws1 / "artifacts/import_errors.json").read_text())
    summary["dedup"] = {
        "kept": len(dedup_summary["kept"]),
        "removed": len(dedup_summary["removed"]),
        "empty_samples": len(dedup_summary["errors"]),
    }
    summary["import_errors"] = len(import_errors["errors"])
    log(f"  kept={summary['dedup']['kept']} removed={summary['dedup']['removed']} "
        f"empty={summary['dedup']['empty_samples']} import_errors={summary['import_errors']}")

    log("== 3. rerun from scratch; byte-identical check ==")
    ws2 = root / "ws-rerun"
    pipe2 = Pipeline(ws2)
    result2 = pipe2.run(shard_paths=[str(p) for p in shards])
    digest2 = _tree_digest(ws2 / "artifacts")
    identical = digest2 == run_digest and result2["version_fingerprint"] == result["version_fingerprint"]
    summary["rerun_identical"] = identical
    log(f"  identical: {identical}")
    assert identical

    log("== 4. graceful stop + resume; byte-identical check ==")
    ws3 = root / "ws-resume"
    calls = {"n": 0}

    def stop_after_20() -> bool:
        calls["n"] += 1
        return calls["n"] > 20

    pipe3 = Pipeline(ws3, stop_checker=stop_after_20)
    stopped = pipe3.run(shard_paths=[str(p) for p in shards])
    log(f"  first pass status: {stopped['status']} (units seen: {calls['n']})")
    assert stopped["status"] == "stopped"
    pipe4 = Pipeline(ws3)
    resumed = pipe4.resume()
    log(f"  resume status: {resumed['status']}")
    digest3 = _tree_digest(ws3 / "artifacts")
    seqs = [r.seq for r in pipe4.store.list_records()]
    resume_identical = digest3 == run_digest
    summary["resume_identical"] = resume_identical
    summary["resume_seqs"] = seqs
    log(f"  identical to full run: {resume_identical}; checkpoint seqs: {seqs}")
    assert resume_identical and seqs == [1, 2, 3, 4, 5]

    log("== 5. incremental invalidation (dedup threshold 3 -> 2) ==")
    config = PipelineConfig()
    config.dedup.hamming_threshold = 2
    pipe5 = Pipeline(ws1, config=config)
    rerun = pipe5.run()
    statuses = {s["stage"]: s["status"] for s in rerun["stages"]}
    summary["incremental_statuses"] = statuses
    log(f"  statuses: {statuses}")
    assert statuses["import"] == "cached" and statuses["dedup"] == "executed"
    assert rerun["version_fingerprint"] != result["version_fingerprint"]

    log("== 6. schema interop: v1 -> internal -> v2 -> internal ==")
    lines0 = shards[0].read_text(encoding="utf-8").splitlines()
    raw_v1 = json.loads(lines0[0])
    internal = decode_sample(raw_v1)
    as_v2 = encode_v2(internal)
    reimported = decode_sample(as_v2)
    interop_ok = (
        reimported.fingerprint == internal.fingerprint
        and reimported.quality.score == internal.quality.score
        and reimported.schema_version == 2
    )
    summary["interop"] = {
        "fingerprint_stable": interop_ok,
        "fingerprint": internal.fingerprint,
        "downgraded": internal.quality.downgraded,
        "basis": internal.quality.basis,
    }
    log(f"  fingerprint stable: {interop_ok} ({internal.fingerprint[:16]}...)")
    log(f"  v1 legacy upgrade: downgraded={internal.quality.downgraded}")
    log(f"  basis: {internal.quality.basis}")
    assert interop_ok
    hint_internal = decode_sample(json.loads(lines0[5]))
    hint_v1 = encode_v1(hint_internal)
    hint_roundtrip = decode_sample(hint_v1)
    v1_roundtrip_ok = hint_roundtrip.fingerprint == hint_internal.fingerprint
    summary["interop"]["v1_roundtrip"] = v1_roundtrip_ok
    log(f"  v1 format-hint round trip: {v1_roundtrip_ok}")
    assert v1_roundtrip_ok

    log("== 7. snapshot replacement detection ==")
    detect_ws = root / "ws-detect"
    pipe_part = Pipeline(detect_ws)
    pipe_part.run(shard_paths=[str(p) for p in shards], until="dedup")
    snapshot_dir = root / "snapshot-ckpt"
    shutil.copytree(detect_ws / "checkpoints", snapshot_dir)
    shutil.copy2(detect_ws / "HEAD.json", root / "HEAD.snapshot")
    pipe_full = Pipeline(detect_ws)
    pipe_full.run()
    normal = pipe_full.store.verify()
    log(f"  normal chain: kind={normal.kind}")
    assert normal.kind == "normal"

    quality_detect = detect_ws / "artifacts/quality.json"
    original = quality_detect.read_bytes()
    quality_detect.write_bytes(original + b" ")
    tampered = pipe_full.store.verify()
    log(f"  after tampering quality.json: kind={tampered.kind}")
    quality_detect.write_bytes(original)
    assert tampered.kind == "tamper"

    shutil.rmtree(detect_ws / "checkpoints")
    shutil.copytree(snapshot_dir, detect_ws / "checkpoints")
    shutil.copy2(root / "HEAD.snapshot", detect_ws / "HEAD.json")
    rolled_back = pipe_full.store.verify()
    log(f"  after wholesale rollback: kind={rolled_back.kind}")
    summary["replacement_detection"] = {
        "normal": normal.kind,
        "tamper": tampered.kind,
        "rollback": rolled_back.kind,
    }
    assert rolled_back.kind == "rollback"
    try:
        pipe_full.store.verify_or_raise()
        raise AssertionError("expected E_STALE_CHECKPOINT")
    except WorkbenchError as exc:
        assert exc.code == E_STALE_CHECKPOINT
        expected = exc.details["expected_fingerprint"][:12]
        actual = exc.details["actual_fingerprint"][:12]
        log(f"  verify_or_raise -> {exc.code} expected={expected}... actual={actual}...")

    log("== 8. concurrent publish protection ==")
    ws8 = root / "ws-lock"
    store8 = CheckpointStore(ws8)
    head8 = store8.head()
    next_seq = int(head8["seq"]) + 1 if head8 else 1
    ctx = multiprocessing.get_context("fork")
    queue = ctx.Queue()
    procs = [
        ctx.Process(
            target=_publish_attempt,
            args=(str(ws8), next_seq, STAGES[next_seq - 1], "", queue),
        )
        for _ in range(2)
    ]
    for proc in procs:
        proc.start()
    for proc in procs:
        proc.join()
    outcomes = sorted(queue.get() for _ in procs)
    summary["concurrent_publish"] = outcomes
    log(f"  outcomes: {outcomes}")
    assert ("ok", next_seq) in outcomes and (E_STALE_CHECKPOINT, next_seq) in outcomes

    log("== 9. HTTP API smoke run ==")
    from fastapi.testclient import TestClient

    from .server import create_app

    ws9 = root / "ws-http"
    client = TestClient(create_app(ws9))
    bad = client.post("/stages/run", json={"stage": "score"})
    log(f"  run score before import -> {bad.status_code} {bad.json()['error']['code']}")
    assert bad.status_code == 409 and bad.json()["error"]["code"] == "E_INVALID_STAGE"
    resp = client.post("/samples/import", json={"shards": [str(p) for p in shards]})
    assert resp.status_code == 200, resp.text
    resp = client.post("/stages/run", json={"stage": "all"})
    assert resp.status_code == 200, resp.text
    fp = client.get("/fingerprint").json()["version_fingerprint"]
    quality = client.get("/quality").json()
    mixture = client.get("/mixture").json()
    exported = client.get("/export/mixture")
    checks = client.get("/checkpoints").json()["verification"]
    summary["http"] = {
        "version_fingerprint": fp,
        "scores": len(quality["scores"]),
        "mixture": len(mixture["mixture"]),
        "export_lines": len(exported.text.strip().splitlines()),
        "verification": checks["kind"],
    }
    log(f"  fingerprint: {fp[:16]}... scores={len(quality['scores'])} "
        f"mixture={len(mixture['mixture'])} export_lines={len(exported.text.strip().splitlines())}")
    assert checks["kind"] == "normal"

    summary_path = root / "demo_summary.json"
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True) + "\n")
    log(f"demo summary written to {summary_path}")
    return summary


if __name__ == "__main__":
    run_demo(sys.argv[1] if len(sys.argv) > 1 else "demo_run")

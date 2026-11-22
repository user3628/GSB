#!/usr/bin/env python3
"""多进程文件锁发布保护演示：两个进程竞争发布同一查询的检查点。"""
import multiprocessing
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from qrwb.checkpoints import CheckpointStore  # noqa: E402
from qrwb.config import load_config, write_default_config  # noqa: E402
from qrwb.sampling import generate_workspace_inputs  # noqa: E402
from qrwb.workspace import load_workspace  # noqa: E402
from qrwb.engine import build_version_bundle  # noqa: E402
import json  # noqa: E402


def _worker(workspace: str, query_id: str, stage: str, fps: dict,
            barrier) -> None:
    store = CheckpointStore(workspace)
    barrier.wait()
    try:
        store.publish(query_id, stage, input_fingerprint="in",
                      config_version="cfg", product={"stage": stage},
                      stage_fingerprints=fps)
    except Exception as exc:  # 子进程回传稳定错误码
        with open(os.path.join(workspace, f"mp-{os.getpid()}.err"),
                  "w", encoding="utf-8") as handle:
            handle.write(getattr(exc, "code", type(exc).__name__))


def main() -> int:
    workspace = sys.argv[1]
    os.makedirs(workspace, exist_ok=True)
    cfg_path = os.path.join(workspace, "config.yaml")
    write_default_config(cfg_path)
    cfg = load_config(cfg_path)
    generate_workspace_inputs(cfg, os.path.join(workspace, "inputs"),
                              shard_count=1, rows_per_shard=2)
    resources, records, _ = load_workspace(workspace, cfg)
    store = CheckpointStore(workspace)
    record = records[0]
    bundle = build_version_bundle(record, cfg, resources)
    qid = record["query_id"]

    # 主进程先发布 dispatch(seq=1)
    store.publish(qid, "dispatch", input_fingerprint=bundle.input_fingerprint,
                  config_version=bundle.config_version, product={"v": 1},
                  stage_fingerprints={
                      "input_fingerprint": bundle.input_fingerprint,
                      "normalization_fingerprint":
                          bundle.normalization_fingerprint})

    ctx = multiprocessing.get_context("fork")
    barrier = ctx.Barrier(2)
    fps_dup = {"input_fingerprint": bundle.input_fingerprint,
               "normalization_fingerprint": bundle.normalization_fingerprint}
    fps_next = {
        "input_fingerprint": bundle.input_fingerprint,
        "normalization_fingerprint": bundle.normalization_fingerprint,
        "synonym_fingerprint": bundle.synonym_fingerprint,
        "glyph_fingerprint": bundle.glyph_fingerprint,
        "bridge_fingerprint": bundle.bridge_fingerprint,
        "routes_fingerprint": bundle.routes_fingerprint}
    procs = [
        ctx.Process(target=_worker, args=(workspace, qid, "dispatch",
                                          fps_dup, barrier)),
        ctx.Process(target=_worker, args=(workspace, qid, "candidate",
                                          fps_next, barrier)),
    ]
    for proc in procs:
        proc.start()
    for proc in procs:
        proc.join(timeout=30)
    codes = set()
    for name in os.listdir(workspace):
        if name.startswith("mp-") and name.endswith(".err"):
            with open(os.path.join(workspace, name), encoding="utf-8") as h:
                codes.add(h.read())
    reloaded = CheckpointStore(workspace)
    result = {
        "stale_rejected": "E_STALE_CHECKPOINT" in codes,
        "sequential_publish": reloaded.latest_seq(qid) == 2,
        "observed_codes": sorted(codes),
    }
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

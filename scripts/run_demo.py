"""Deterministic acceptance demo.

Runs the full four-stage build on at least three shards, performs one sample
edit, demonstrates incremental invalidation, graceful stop/resume, concurrent
publish protection and a byte-level comparison against a from-scratch rebuild.

Usage:
    .venv/bin/python scripts/run_demo.py
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from prefalign.core.checkpoint import (  # noqa: E402
    FileLock, checkpoint_record, publish_checkpoint,
)
from prefalign.core.errors import StaleCheckpointError  # noqa: E402
from prefalign.core.workspace import (  # noqa: E402
    build_workspace, edit_sample, full_rebuild_digests, import_demo_shards,
)


def main() -> int:
    cfg, store, pipeline = build_workspace(ROOT)
    import_demo_shards(store, ROOT)

    print("== 1. full four-stage build ==")
    first = pipeline.run(reason="demo_full_build")
    print(json.dumps(first, ensure_ascii=False))
    baseline = pipeline.stage_digests()

    print("\n== 2. single-sample edit -> earliest invalid stage ==")
    samples, _ = store.effective_samples()
    target = next(sample for sample in samples if sample.prompt)
    payload = target.to_dict()
    payload.pop("shard", None)
    payload["chosen"] = target.chosen + " 【增量编辑】"
    edit_sample(store, payload)
    second = pipeline.run(reason=f"edit:{target.pair_id}")
    print(json.dumps(second, ensure_ascii=False))
    assert second["rebuilt"] == ["normalize", "quality", "features", "sampling"]

    print("\n== 3. graceful stop and resume ==")
    pipeline.invalidate_chain("quality", "demo_stop",
                              store.effective_samples()[1])
    pipeline.request_stop()
    stopped = pipeline.run_with_stop()
    print(json.dumps(stopped, ensure_ascii=False))
    pipeline.clear_stop()
    resumed = pipeline.resume()
    print(json.dumps({key: resumed[key] for key in
                      ("status", "reused", "rebuilt", "resumed_from")},
                     ensure_ascii=False))

    print("\n== 4. concurrent publish protection ==")
    with FileLock(pipeline.publish_lock):
        try:
            publish_checkpoint(pipeline.checkpoints_dir,
                               checkpoint_record("normalize", 9, {}, 1, "c", "a", "f"))
        except StaleCheckpointError as exc:
            print(json.dumps(exc.to_dict(), ensure_ascii=False))

    print("\n== 5. incremental vs from-scratch byte comparison ==")
    digests, count = full_rebuild_digests(
        ROOT, ROOT / "workspace" / "verify_full",
        ROOT / "config" / "default.yaml")
    report = pipeline.verify_incremental(digests, count)
    print(json.dumps(report, ensure_ascii=False, indent=2))

    print("\n== 6. export mixture ==")
    target_path = pipeline.export_mixture()
    print(f"exported: {target_path}")
    print("\nDEMO OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

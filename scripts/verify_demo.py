#!/usr/bin/env python3
"""Fresh-build the bundled demo and compare against demo/expected.json."""

from __future__ import annotations

import json
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from llm_pipeline.engine import PipelineEngine  # noqa: E402
from llm_pipeline.workspace import Workspace  # noqa: E402


def main() -> int:
    expected = json.loads((ROOT / "demo" / "expected.json").read_text(encoding="utf-8"))
    with tempfile.TemporaryDirectory(prefix="llm-pipeline-demo-verify-") as temp:
        workspace = Workspace(Path(temp) / "workspace")
        workspace.initialize_demo(ROOT / "demo")
        engine = PipelineEngine(workspace)
        run = engine.run(force=True)
        verification = engine.verify_incremental(run_if_needed=False)
        report = engine.report()
        state = engine.store.load()
        duplicate_ids = [
            item["id"] for item in engine.store.artifact_payload(state.item("normalize"))["duplicates"]
        ]
        checkpoint_sequence = [record["seq"] for record in state.records]

    stage_digests = {item["stage"]: item["artifact_digest"] for item in run["rebuilt_stages"]}
    product_digests = {
        name: details["incremental"]
        for name, details in verification["products"].items()
    }
    product_bytes = {
        name: details["incremental_bytes"]
        for name, details in verification["products"].items()
    }
    checks = {
        "counts": report["counts"] == expected["counts"],
        "duplicate_ids": duplicate_ids == expected["duplicate_ids"],
        "empty_layers": report["empty_layers"] == expected["empty_layers"],
        "stage_digests": stage_digests == expected["stage_digests"],
        "product_digests": product_digests == expected["product_digests"],
        "product_bytes": product_bytes == expected["product_bytes"],
        "checkpoint_sequence": checkpoint_sequence == expected["checkpoint_sequence"],
        "incremental_equals_full": verification["status"] == "consistent",
    }
    ok = all(checks.values())
    print(
        json.dumps(
            {
                "ok": ok,
                "checks": checks,
                "stage_digests": stage_digests,
                "product_digests": product_digests,
                "product_bytes": product_bytes,
                "counts": report["counts"],
            },
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        )
    )
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())

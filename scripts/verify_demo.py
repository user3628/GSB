#!/usr/bin/env python3
"""Reproduce the core acceptance checks and print a machine-readable summary."""

from __future__ import annotations

import hashlib
import json
import shutil
import sys
import tempfile
from pathlib import Path
from typing import Any

import numpy as np

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from motion_engine.runner import StopController, report_bytes  # noqa: E402
from motion_engine.sample_gen import generate_samples  # noqa: E402
from motion_engine.workspace import Workspace  # noqa: E402


def _sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def main() -> int:
    if not (ROOT / "data" / "manifest.json").is_file():
        generate_samples(ROOT / "data", seed=20261121)
    with tempfile.TemporaryDirectory(prefix="motion-acceptance-") as temporary:
        root = Path(temporary)
        source = root / "data"
        shutil.copytree(ROOT / "data", source)
        manifest = source / "manifest.json"

        complete = Workspace(root / "complete")
        resumed = Workspace(root / "resumed")
        complete.import_manifest(manifest)
        resumed.import_manifest(manifest)
        full_report = complete.run("beta_deform")
        controller = StopController()

        def stop_after_five(record: dict[str, Any]) -> None:
            if record["step_index"] == 5:
                controller.request()

        partial_report = resumed.run(
            "beta_deform", stop_controller=controller, on_step=stop_after_five
        )
        final_report = resumed.run("beta_deform")
        full_bytes = report_bytes(full_report)
        resumed_bytes = report_bytes(final_report)
        checkpoint_a = complete.root / "checkpoints" / "beta_deform"
        checkpoint_b = resumed.root / "checkpoints" / "beta_deform"
        names = sorted(path.name for path in checkpoint_a.glob("*.json"))
        record_mismatches = sum(
            (checkpoint_a / name).read_bytes() != (checkpoint_b / name).read_bytes()
            for name in names
        )

        invalidation_workspace = Workspace(root / "invalidation")
        invalidation_workspace.import_manifest(manifest)
        invalidation_workspace.run("alpha_translate")
        frames_path = source / "alpha_translate.npy"
        frames = np.load(frames_path, allow_pickle=False)
        frames[5, 0, 0] = (int(frames[5, 0, 0]) + 23) % 256
        np.save(frames_path, frames, allow_pickle=False)
        invalidated = invalidation_workspace.inspect_series("alpha_translate")
        rebuilt = invalidation_workspace.run("alpha_translate")

        result = {
            "ok": (
                full_bytes == resumed_bytes
                and record_mismatches == 0
                and invalidated["rebuild_from_step"] == 5
                and rebuilt["complete"]
            ),
            "samples": json.loads((ROOT / "data" / "fingerprints.json").read_text()),
            "stop_resume": {
                "stopped_after_step": partial_report["checkpoint"]["last_sequence"],
                "partial_complete": partial_report["complete"],
                "resumed_started_at_step": final_report["run"]["started_at_step"],
                "resumed_performed_steps": final_report["run"]["performed_steps"],
                "final_complete": final_report["complete"],
                "report_sha256_complete": _sha(full_bytes),
                "report_sha256_resumed": _sha(resumed_bytes),
                "report_bytes_equal": full_bytes == resumed_bytes,
                "report_fingerprint": final_report["report_fingerprint"],
                "checkpoint_file_count": len(names),
                "checkpoint_record_mismatches": record_mismatches,
            },
            "invalidation": {
                "changed_truth_frame_index": 5,
                "rebuild_from_step": invalidated["rebuild_from_step"],
                "reasons": invalidated["invalidation_reasons"],
                "current_version_fingerprint": invalidated["version_fingerprints"]["current"],
                "previous_version_fingerprint": invalidated["version_fingerprints"]["checkpoint"],
                "rebuilt_report_fingerprint": rebuilt["report_fingerprint"],
            },
        }
        print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
        return 0 if result["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())

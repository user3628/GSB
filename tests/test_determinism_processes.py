from __future__ import annotations

import dataclasses
import os
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from detdiff.config import load_config
from detdiff.pipeline import build_workspace, compute_shard_from_scratch, load_all_references, published_manifest

from helpers import make_workspace


class CrossProcessDeterminismTests(unittest.TestCase):
    def test_full_build_identical_across_worker_processes_and_thread_counts(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = make_workspace(Path(temp) / "one")
            second = Path(temp) / "two"
            shutil.copytree(root, second)
            first_result = _run_build(root, "1")
            second_result = _run_build(second, "4")
            self.assertEqual(first_result.returncode, 0, first_result.stderr)
            self.assertEqual(second_result.returncode, 0, second_result.stderr)
            first_manifest = published_manifest(load_config(root / "config.yaml"))
            second_manifest = published_manifest(load_config(second / "config.yaml"))
            self.assertEqual(first_manifest, second_manifest)

            repeated = build_workspace(root)
            self.assertEqual(repeated.rebuilt, {})
            self.assertEqual(published_manifest(load_config(root / "config.yaml")), first_manifest)

    def test_absent_reference_and_zero_strength_reference_branch_are_byte_identical(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = make_workspace(Path(temp) / "workspace")
            config = load_config(root / "config.yaml")
            beta = config.shard("beta")
            references = load_all_references(config)
            without = compute_shard_from_scratch(beta, config, None)
            zero_strength = compute_shard_from_scratch(dataclasses.replace(beta, blend_strength=0.0), config, references["beta"])
            for stage in ("encode", "schedule", "sample", "quantize"):
                self.assertEqual(without.stages[stage].artifact, zero_strength.stages[stage].artifact)
            self.assertEqual(without.png_bytes, zero_strength.png_bytes)


def _run_build(root: Path, threads: str) -> subprocess.CompletedProcess[str]:
    env = dict(os.environ)
    for name in ("OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS", "NUMEXPR_NUM_THREADS"):
        env[name] = threads
    return subprocess.run(
        [sys.executable, "-m", "detdiff.cli", "build", "--workspace", str(root), "--json"],
        text=True,
        capture_output=True,
        env=env,
        timeout=60,
        check=False,
    )


if __name__ == "__main__":
    unittest.main()


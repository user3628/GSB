from __future__ import annotations

import json
import io
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path

import numpy as np

from flashblock.cli import main as cli_main
from flashblock.config import AttentionConfig
from flashblock.engine import AttentionEngine
from flashblock.errors import FlashBlockError
from flashblock.scheduler import plan_row_block


class ConfigBudgetAndCliTests(unittest.TestCase):
    def test_yaml_config_and_shape_manifest_are_deterministic(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            with redirect_stdout(io.StringIO()):
                result = cli_main(["generate", "--root", str(root), "--seed", "1234"])
            self.assertEqual(result, 0)
            manifest = json.loads((root / "data" / "manifest.json").read_text(encoding="utf-8"))
            config = AttentionConfig.from_yaml(root / "configs" / "operator.yaml")
            self.assertEqual(len(manifest["shards"]), 3)
            self.assertTrue(config.causal)
            tags = {tag for shard in manifest["shards"] for tag in shard["tags"]}
            self.assertIn("short_sequence", tags)
            self.assertIn("long_sequence", tags)
            self.assertIn("non_divisible_head_dim", tags)
            self.assertIn("batch_gt_1", tags)

    def test_memory_budget_split_and_error(self) -> None:
        config = AttentionConfig(
            row_block_size=16,
            col_block_size=16,
            causal=True,
            memory_budget_bytes=1_000_000,
        )
        plan = plan_row_block(
            config,
            shard_id="budget",
            batch=16,
            heads=2,
            head_dim=32,
            key_length=64,
            q_start=0,
            q_end=16,
            row_block=0,
            column_blocks=4,
        )
        self.assertEqual(plan.decision, "batch_split_required")
        self.assertGreater(len(plan.batch_chunks), 1)
        self.assertIn("budget", plan.split_reason)

        tiny = config.with_overrides(memory_budget_bytes=64)
        rng = np.random.default_rng(1)
        q = rng.normal(size=(2, 1, 16, 32))
        k = rng.normal(size=(2, 1, 16, 32))
        v = rng.normal(size=(2, 1, 16, 32))
        with tempfile.TemporaryDirectory() as temporary:
            engine = AttentionEngine(temporary, "budget", q, k, v, tiny)
            with self.assertRaises(FlashBlockError) as caught:
                engine.run()
        error = caught.exception
        self.assertEqual(error.code, "E_MEMORY_BUDGET")
        self.assertIn("breakdown", error.context)
        self.assertIn("required_bytes", error.context)

    def test_state_and_budget_cli_queries(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            with redirect_stdout(io.StringIO()):
                cli_main(["generate", "--root", str(root), "--seed", "99"])
            manifest = json.loads((root / "data" / "manifest.json").read_text(encoding="utf-8"))
            shard = manifest["shards"][0]
            workspace = root / "runtime"
            args = [
                "attention",
                "--input",
                str(root / shard["path"]),
                "--shard-id",
                shard["shard_id"],
                "--config",
                str(root / "configs" / "operator.yaml"),
                "--workspace",
                str(workspace),
                "--max-new-row-blocks",
                "1",
            ]
            with redirect_stdout(io.StringIO()):
                self.assertEqual(cli_main(args), 0)
                self.assertEqual(
                    cli_main(
                        [
                            "state",
                            "--workspace",
                            str(workspace),
                            "--shard-id",
                            shard["shard_id"],
                        ]
                    ),
                    0,
                )
                self.assertEqual(
                    cli_main(
                        [
                            "budget",
                            "--input",
                            str(root / shard["path"]),
                            "--shard-id",
                            shard["shard_id"],
                            "--config",
                            str(root / "configs" / "operator.yaml"),
                            "--workspace",
                            str(workspace),
                        ]
                    ),
                    0,
                )


if __name__ == "__main__":
    unittest.main()


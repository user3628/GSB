from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

import numpy as np

from flashblock.checkpoint import CheckpointStore
from flashblock.config import AttentionConfig
from flashblock.core import arrays_byte_equal, build_fingerprints, compute_row_block, full_attention, needed_column_blocks
from flashblock.engine import AttentionEngine
from flashblock.errors import FlashBlockError
from flashblock.scheduler import plan_row_block


def _state_for_row(q: np.ndarray, k: np.ndarray, v: np.ndarray, config: AttentionConfig, row: int):
    batch, heads, q_length, head_dim = q.shape
    q_start = row * config.row_block_size
    q_end = min(q_start + config.row_block_size, q_length)
    columns = needed_column_blocks(config, k.shape[2], q_end)
    plan = plan_row_block(
        config,
        shard_id="manual",
        batch=batch,
        heads=heads,
        head_dim=head_dim,
        key_length=k.shape[2],
        q_start=q_start,
        q_end=q_end,
        row_block=row,
        column_blocks=columns,
    )
    state, _ = compute_row_block(
        q,
        k,
        v,
        config=config,
        shard_id="manual",
        row_block=row,
        plan=plan,
        fingerprints=build_fingerprints(q, k, v, config),
    )
    return state


class CheckpointRecoveryTests(unittest.TestCase):
    def setUp(self) -> None:
        rng = np.random.default_rng(5)
        self.q = rng.normal(size=(1, 1, 40, 8))
        self.k = rng.normal(size=(1, 1, 40, 8))
        self.v = rng.normal(size=(1, 1, 40, 8))
        self.config = AttentionConfig(row_block_size=8, col_block_size=8, causal=True)
        self.full = full_attention(self.q, self.k, self.v, self.config, shard_id="recover")

    def test_resume_after_truncated_record_matches_full_bytes(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            paused = AttentionEngine(temporary, "recover", self.q, self.k, self.v, self.config).run(
                max_new_row_blocks=2
            )
            self.assertEqual(paused.status, "paused")
            store = CheckpointStore(temporary, "recover")
            header = store.latest_header()
            assert header is not None
            run_dir = store._run_directory(header.run_id)
            records = sorted(run_dir.glob("*.ckpt.npz"))
            self.assertEqual(len(records), 2)
            records[-1].write_bytes(b"truncated-checkpoint")

            resumed = AttentionEngine(temporary, "recover", self.q, self.k, self.v, self.config).run()
            self.assertTrue(resumed.complete)
            self.assertTrue(arrays_byte_equal(resumed.output, self.full.output))
            self.assertTrue(arrays_byte_equal(resumed.row_max, self.full.row_max))
            self.assertTrue(arrays_byte_equal(resumed.row_norm, self.full.row_norm))

    def test_duplicate_sequence_is_quarantined_and_recomputed(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            AttentionEngine(temporary, "recover", self.q, self.k, self.v, self.config).run()
            store = CheckpointStore(temporary, "recover")
            header = store.latest_header()
            assert header is not None
            first = sorted(store._run_directory(header.run_id).glob("*.ckpt.npz"))[0]
            duplicate = first.with_name("duplicate-" + first.name)
            duplicate.write_bytes(first.read_bytes())

            resumed = AttentionEngine(temporary, "recover", self.q, self.k, self.v, self.config).run()
            self.assertTrue(resumed.complete)
            self.assertTrue(arrays_byte_equal(resumed.output, self.full.output))
            self.assertTrue(any(item["type"] == "duplicate_sequence" for item in resumed.anomalies))

    def test_record_contains_required_versioned_fields(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            AttentionEngine(temporary, "recover", self.q, self.k, self.v, self.config).run(max_new_row_blocks=1)
            store = CheckpointStore(temporary, "recover")
            header = store.latest_header()
            assert header is not None
            record = sorted(store._run_directory(header.run_id).glob("*.ckpt.npz"))[0]
            with np.load(record, allow_pickle=False) as payload:
                metadata = json.loads(str(payload["meta_json"].item()))
            for field in (
                "shard_id",
                "run_id",
                "row_block",
                "sequence",
                "key_start",
                "key_end",
                "row_m_sha256",
                "row_l_sha256",
                "row_o_sha256",
                "input_fingerprint",
                "payload_sha256",
            ):
                self.assertIn(field, metadata)
            self.assertEqual(metadata["key_end"], 8)

    def test_stale_and_misaligned_publications_are_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            store = CheckpointStore(temporary, "manual")
            config = self.config
            fingerprints = {
                "config_fingerprint": config.semantic_fingerprint,
                "q_fingerprint": build_fingerprints(self.q, self.k, self.v, config).q,
                "kv_fingerprint": build_fingerprints(self.q, self.k, self.v, config).kv,
                "input_fingerprint": build_fingerprints(self.q, self.k, self.v, config).input,
            }
            header = store.create_run(
                config=config,
                q_shape=self.q.shape,
                k_shape=self.k.shape,
                v_shape=self.v.shape,
                fingerprints=fingerprints,
                mode="full",
            )
            row_one = _state_for_row(self.q, self.k, self.v, config, 1)
            with self.assertRaises(FlashBlockError) as crossed:
                store.publish_next(row_one, run_id=header.run_id)
            self.assertEqual(crossed.exception.code, "E_STALE_CHECKPOINT")

            row_zero = _state_for_row(self.q, self.k, self.v, config, 0)
            store.publish_next(row_zero, run_id=header.run_id)
            with self.assertRaises(FlashBlockError) as duplicate:
                store.publish_next(row_zero, run_id=header.run_id)
            self.assertEqual(duplicate.exception.code, "E_STALE_CHECKPOINT")

    def test_invalid_resume_stage_reports_available_sequence(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            engine = AttentionEngine(temporary, "stage", self.q, self.k, self.v, self.config)
            with self.assertRaises(FlashBlockError) as missing:
                engine.resume()
            self.assertEqual(missing.exception.code, "E_INVALID_STAGE")
            self.assertEqual(missing.exception.context["available_sequence"], 0)

            paused = engine.run(max_new_row_blocks=1)
            self.assertEqual(paused.committed_row_blocks, 1)
            with self.assertRaises(FlashBlockError) as jump:
                engine.resume(start_row_block=4)
            self.assertEqual(jump.exception.code, "E_INVALID_STAGE")
            self.assertEqual(jump.exception.context["available_sequence"], 1)

    def test_crash_leftover_run_directory_does_not_block_new_header(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            store = CheckpointStore(temporary, "header-crash")
            leftover = store.runs_root / "run-0001"
            leftover.mkdir(parents=True)
            config = self.config
            fingerprints = build_fingerprints(self.q, self.k, self.v, config)
            header = store.create_run(
                config=config,
                q_shape=self.q.shape,
                k_shape=self.k.shape,
                v_shape=self.v.shape,
                fingerprints={
                    "config_fingerprint": fingerprints.config,
                    "q_fingerprint": fingerprints.q,
                    "kv_fingerprint": fingerprints.kv,
                    "input_fingerprint": fingerprints.input,
                },
                mode="full",
            )
            self.assertEqual(header.run_id, "run-0002")


if __name__ == "__main__":
    unittest.main()


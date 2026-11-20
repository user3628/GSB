from __future__ import annotations

import math
from pathlib import Path

import pytest

from stream_tts.acoustic import Prosody
from stream_tts.checkpoint import StageStore, record_key
from stream_tts.config import load_config
from stream_tts.errors import TTSError
from stream_tts.frontend import PhonemeTable, front_end
from stream_tts.inputs import load_samples
from stream_tts.scheduler import plan_blocks


def test_budget_degradation_is_proportional_and_reproducible(workspace: Path):
    config = load_config(workspace / "config/default.yaml", workspace=workspace)
    table = PhonemeTable.load(config.resolved_path("phoneme_table"))
    utt = [item for item in load_samples(config.resolved_path("samples")) if item.utt_id == "utt_budget_stress_03"][0]
    frontend = front_end(
        utt_id=utt.utt_id,
        text=utt.text,
        language=utt.language,
        split=config.split,
        phoneme_table=table,
    )
    calibration = __import__("json").loads(config.resolved_path("calibration").read_text())
    plan1 = plan_blocks(
        frontend=frontend,
        config=config,
        calibration=calibration,
        prosody=Prosody.from_dict(utt.prosody),
    )
    plan2 = plan_blocks(
        frontend=frontend,
        config=config,
        calibration=calibration,
        prosody=Prosody.from_dict(utt.prosody),
    )
    assert plan1.trace_records(utt_id=utt.utt_id, calibration=calibration) == plan2.trace_records(
        utt_id=utt.utt_id, calibration=calibration
    )
    assert plan1.degradation_events
    for event in plan1.degradation_events:
        expected = max(2, math.ceil(event["estimated_ms"] / event["budget_ms"]))
        assert event["split_factor"] == min(expected, len(frontend.phonemes))
        assert event["rejected_blocks"] == event["split_factor"] - 1
        assert event["overflow_ratio"] > 1.0
    degraded = [record for record in plan1.trace_records(utt_id=utt.utt_id, calibration=calibration) if record["degraded"]]
    assert degraded
    assert all(record["timeout_cancellation_used"] is False for record in plan1.trace_records(utt_id=utt.utt_id, calibration=calibration))
    assert any(record["over_budget"] for record in degraded)
    assert plan1.worker_count >= 1


def test_checkpoint_atomicity_sequence_and_prerequisites(workspace: Path):
    store = StageStore(workspace=workspace, run_id="unit")
    store.ensure_utt_order(["a", "b"])
    r0 = store.publish_bytes(
        utt_id="a",
        stage="frontend",
        chunk_index=0,
        stage_total_chunks=1,
        fingerprint="fp-a-front",
        product=b"front-a",
        suffix="bin",
    )
    assert r0["seq"] == 0
    with pytest.raises(TTSError) as duplicate:
        store.publish_bytes(
            utt_id="a",
            stage="frontend",
            chunk_index=0,
            stage_total_chunks=1,
            fingerprint="fp-a-front",
            product=b"front-a",
            suffix="bin",
        )
    assert duplicate.value.code == "E_STALE_CHECKPOINT"

    with pytest.raises(TTSError) as stale_seq:
        store.publish_bytes(
            utt_id="a",
            stage="acoustic",
            chunk_index=0,
            stage_total_chunks=1,
            fingerprint="fp-a-acoustic",
            product=b"acoustic-a",
            suffix="bin",
            prerequisites=[record_key("a", "frontend", 0)],
            expected_seq=0,
        )
    assert stale_seq.value.code == "E_STALE_CHECKPOINT"

    with pytest.raises(TTSError) as missing_stage:
        store.publish_bytes(
            utt_id="b",
            stage="acoustic",
            chunk_index=0,
            stage_total_chunks=1,
            fingerprint="fp-b-acoustic",
            product=b"acoustic-b",
            suffix="bin",
        )
    assert missing_stage.value.code == "E_INVALID_STAGE"

    with pytest.raises(TTSError) as cross_statement:
        store.publish_bytes(
            utt_id="b",
            stage="frontend",
            chunk_index=0,
            stage_total_chunks=1,
            fingerprint="fp-b-front",
            product=b"front-b",
            suffix="bin",
        )
    assert cross_statement.value.code == "E_STALE_CHECKPOINT"

    store.publish_bytes(
        utt_id="a",
        stage="acoustic",
        chunk_index=0,
        stage_total_chunks=1,
        fingerprint="fp-a-acoustic",
        product=b"acoustic-a",
        suffix="bin",
        prerequisites=[record_key("a", "frontend", 0)],
    )
    store.publish_bytes(
        utt_id="a",
        stage="waveform",
        chunk_index=0,
        stage_total_chunks=1,
        fingerprint="fp-a-wave",
        product=b"wave-a",
        suffix="bin",
        prerequisites=[record_key("a", "acoustic", 0)],
    )
    store.publish_bytes(
        utt_id="a",
        stage="validation",
        chunk_index=0,
        stage_total_chunks=1,
        fingerprint="fp-a-validation",
        product=b"validation-a",
        suffix="bin",
        prerequisites=[record_key("a", "waveform", 0)],
    )
    # Only after the earlier utterance is validated may b start.
    store.publish_bytes(
        utt_id="b",
        stage="frontend",
        chunk_index=0,
        stage_total_chunks=1,
        fingerprint="fp-b-front",
        product=b"front-b",
        suffix="bin",
    )
    ledger = store.ledger()
    assert ledger["next_seq"] == 5
    assert not list(store.checkpoint_dir.rglob("*.tmp"))
    # Every persisted record remains fully parseable JSON.
    for path in store.records_dir.glob("*.json"):
        __import__("json").loads(path.read_text(encoding="utf-8"))


def test_invalid_prerequisite_product(workspace: Path):
    store = StageStore(workspace=workspace, run_id="invalid-product")
    store.ensure_utt_order(["a"])
    rec = store.publish_bytes(
        utt_id="a",
        stage="frontend",
        chunk_index=0,
        stage_total_chunks=1,
        fingerprint="fp",
        product=b"ok",
        suffix="bin",
    )
    path = workspace / rec["product_path"]
    path.write_bytes(b"tampered")
    with pytest.raises(TTSError) as exc:
        store.read_product(rec)
    assert exc.value.code == "E_INVALID_STAGE"


def test_concurrent_duplicate_publish_is_serialized(workspace: Path):
    import concurrent.futures

    store = StageStore(workspace=workspace, run_id="concurrent")
    store.ensure_utt_order(["z"])

    def publish_once():
        try:
            record = store.publish_bytes(
                utt_id="z",
                stage="frontend",
                chunk_index=0,
                stage_total_chunks=1,
                fingerprint="same-fp",
                product=b"same-product",
                suffix="bin",
                expected_seq=0,
            )
            return ("ok", record["seq"])
        except TTSError as exc:
            return ("error", exc.code)

    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(lambda _: publish_once(), range(2)))
    assert sorted(results) == [("error", "E_STALE_CHECKPOINT"), ("ok", 0)]
    ledger = store.ledger()
    assert ledger["next_seq"] == 1
    assert ledger["records"] == {"z:frontend:000000": 0}

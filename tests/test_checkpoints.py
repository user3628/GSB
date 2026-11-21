import json
import os

import pytest

from iqa.checkpoints import CheckpointStore, STAGE_ORDER, atomic_write_json
from iqa.errors import BusinessError, E_STALE_CHECKPOINT, E_INVALID_STAGE


def _record(image_id, stage, tag=1):
    return {
        "stage": stage,
        "image_id": image_id,
        "input_fingerprint": f"in-{image_id}-{stage}-{tag}",
        "product": {"tag": tag},
        "product_digest": f"prod-{image_id}-{stage}-{tag}",
        "fingerprint": f"fp-{image_id}-{stage}-{tag}",
    }


def test_atomic_write_no_half_files(tmp_path):
    target = tmp_path / "a.json"
    atomic_write_json(str(target), {"x": 1})
    assert json.loads(target.read_text(encoding="utf-8")) == {"x": 1}
    assert not list(tmp_path.glob(".tmp-*"))


def test_monotonic_sequences_and_predecessor_guard(tmp_path):
    store = CheckpointStore(str(tmp_path))
    seq1 = store.publish("img", STAGE_ORDER[0], _record("img", STAGE_ORDER[0]))
    seq2 = store.publish("img", STAGE_ORDER[1], _record("img", STAGE_ORDER[1]))
    assert seq2 == seq1 + 1
    with pytest.raises(BusinessError) as exc:
        store.publish("orphan", STAGE_ORDER[1], _record("orphan", STAGE_ORDER[1]))
    assert exc.value.code == E_INVALID_STAGE


def test_duplicate_and_stale_rejected(tmp_path):
    store = CheckpointStore(str(tmp_path))
    record = _record("img", STAGE_ORDER[0])
    store.publish("img", STAGE_ORDER[0], record)
    with pytest.raises(BusinessError) as exc:
        store.publish("img", STAGE_ORDER[0], record)
    assert exc.value.code == E_STALE_CHECKPOINT

    stale = _record("img", STAGE_ORDER[0], tag=2)
    with pytest.raises(BusinessError) as exc2:
        store.publish("img", STAGE_ORDER[0], stale, force_sequence=1, allow_replace=False)
    assert exc2.value.code == E_STALE_CHECKPOINT


def test_replace_keeps_sequence_and_rebuild_allowed(tmp_path):
    store = CheckpointStore(str(tmp_path))
    original = store.publish("img", STAGE_ORDER[0], _record("img", STAGE_ORDER[0]))
    rebuilt = _record("img", STAGE_ORDER[0], tag=99)
    seq = store.publish("img", STAGE_ORDER[0], rebuilt, force_sequence=original, allow_replace=True)
    assert seq == original
    stored = store.load("img", STAGE_ORDER[0])
    assert stored["fingerprint"] == rebuilt["fingerprint"]


def test_stop_flag_lifecycle(tmp_path):
    store = CheckpointStore(str(tmp_path))
    assert store.stop_requested() is False
    store.request_stop()
    assert store.stop_requested() is True
    store.clear_stop()
    assert store.stop_requested() is False

import os

import pytest

from legal_cite import checkpoints as cp
from legal_cite.config import default_config
from legal_cite.errors import E_STALE_CHECKPOINT, LegalCiteError
from legal_cite.pipeline import run_pipeline
from legal_cite.serde import write_json_atomic


def test_crash_during_atomic_write_leaves_no_half_record(tmp_path, monkeypatch):
    target = tmp_path / "ckpt_0001_parse.json"
    real_replace = os.replace

    def boom(src, dst):
        if str(dst).endswith("ckpt_0001_parse.json"):
            raise OSError("simulated crash before rename")
        return real_replace(src, dst)

    monkeypatch.setattr(os, "replace", boom)
    with pytest.raises(OSError):
        cp.publish_checkpoint(tmp_path, "parse", "in", 3, "art", "fp")
    # No committed record, and no temp file left behind by a real crash that
    # happens before rename; the index is untouched.
    assert not target.exists()
    index = cp.load_index(tmp_path)
    assert index["records"] == []
    assert index["next_seq"] == 1


def test_partial_temp_file_never_visible_as_checkpoint(tmp_path):
    target = tmp_path / "doc.json"
    tmp = target.with_name(target.name + ".tmp")
    tmp.write_text('{"broken":', encoding="utf-8")
    # A leftover temp file is not read as a valid artifact.
    write_json_atomic(target, {"ok": True})
    from legal_cite.serde import read_json
    assert read_json(target) == {"ok": True}


def test_full_pipeline_after_simulated_stop_is_consistent(all_records, tmp_path):
    cfg = default_config()
    ckpt = tmp_path / "ckpt"
    state_a = run_pipeline(all_records, cfg=cfg, ckpt_dir=ckpt)
    # simulate an operator deleting only the downstream index tail: rebuild
    import json
    idx_path = ckpt / "checkpoint_index.json"
    index = json.loads(idx_path.read_text(encoding="utf-8"))
    index["records"] = index["records"][:2]
    idx_path.write_text(json.dumps(index, sort_keys=True), encoding="utf-8")
    state_b = run_pipeline(all_records, cfg=cfg, ckpt_dir=ckpt)
    # fingerprints must still match because inputs/config are unchanged
    assert state_b["pipeline_fingerprint"] == state_a["pipeline_fingerprint"]

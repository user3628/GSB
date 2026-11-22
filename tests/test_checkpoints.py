import pytest

from legal_cite.checkpoints import (STAGE_ORDER, load_index,
                                    publish_checkpoint, reusable_checkpoints)
from legal_cite.config import default_config
from legal_cite.errors import E_INVALID_STAGE, E_STALE_CHECKPOINT, LegalCiteError
from legal_cite.pipeline import StopController, run_pipeline
from legal_cite.serde import canonical_text, fingerprint


def test_checkpoint_sequence_and_atomic_files(all_records, tmp_path):
    ckpt = tmp_path / "ckpt"
    state = run_pipeline(all_records, cfg=default_config(), ckpt_dir=ckpt)
    index = load_index(ckpt)
    assert [r["stage"] for r in index["records"]] == list(STAGE_ORDER)
    assert [r["seq"] for r in index["records"]] == [1, 2, 3, 4]
    # second identical run reuses every checkpoint without new sequence entries
    again = run_pipeline(all_records, cfg=default_config(), ckpt_dir=ckpt)
    assert again["checkpoint"]["reused"] == list(STAGE_ORDER)
    assert load_index(ckpt)["next_seq"] == 5
    # no temp/half-written files remain
    assert not list(ckpt.glob("*.tmp"))


def test_stale_and_invalid_stage_publish(tmp_path):
    ckpt = tmp_path / "ckpt"
    publish_checkpoint(ckpt, "parse", "in", 3, "art", "fp-parse")
    # skipping normalize -> invalid stage
    with pytest.raises(LegalCiteError) as ei:
        publish_checkpoint(ckpt, "evidence", "in", 3, "art", "fp-x")
    assert ei.value.code == E_INVALID_STAGE
    # duplicate parse -> stale
    with pytest.raises(LegalCiteError) as ei:
        publish_checkpoint(ckpt, "parse", "in", 3, "art", "fp-parse")
    assert ei.value.code == E_STALE_CHECKPOINT


def test_graceful_stop_then_resume_is_byte_identical(all_records, tmp_path):
    cfg = default_config()
    full = run_pipeline(all_records, cfg=cfg)
    ckpt = tmp_path / "ckpt"
    controller = StopController()
    controller.request_stop()
    partial = run_pipeline(all_records, cfg=cfg, ckpt_dir=ckpt,
                           stop_controller=controller)
    assert partial["stop_requested"] is True
    assert partial["manifest"] is None
    resumed = run_pipeline(all_records, cfg=cfg, ckpt_dir=ckpt)
    assert canonical_text(resumed["manifest"]) == canonical_text(full["manifest"])
    assert resumed["pipeline_fingerprint"] == full["pipeline_fingerprint"]


def test_incremental_invalidation_rebuilds_downstream(all_records, tmp_path):
    cfg = default_config()
    ckpt = tmp_path / "ckpt"
    first = run_pipeline(all_records, cfg=cfg, ckpt_dir=ckpt)
    fps = first["stage_fingerprints"]
    assert reusable_checkpoints(load_index(ckpt), fps) == list(STAGE_ORDER)
    # change language weights: earliest invalidation is the resolve stage
    changed = default_config()
    changed["language_weights"]["zh"] = 0.8
    second = run_pipeline(all_records, cfg=changed, ckpt_dir=ckpt)
    assert second["pipeline_fingerprint"] != first["pipeline_fingerprint"]
    rebuilt = second["checkpoint"]["rebuilt"]
    assert rebuilt == ["parse", "normalize", "resolve", "evidence"]


def test_shard_change_invalidates_all(all_records, tmp_path):
    cfg = default_config()
    ckpt = tmp_path / "ckpt"
    run_pipeline(all_records, cfg=cfg, ckpt_dir=ckpt)
    mutated = [dict(r) for r in all_records]
    mutated[0]["raw_text"] = mutated[0]["raw_text"] + " "
    # keep span valid; re-run must rebuild from parse
    state = run_pipeline(mutated, cfg=cfg, ckpt_dir=ckpt)
    assert state["checkpoint"]["rebuilt"] == list(STAGE_ORDER)

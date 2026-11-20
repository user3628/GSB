from __future__ import annotations

import json
import shutil
from pathlib import Path

import yaml

from stream_tts.engine import StreamingTTSEngine
from tests.conftest import copy_workspace


def _trace_bytes(path: Path) -> bytes:
    return path.read_bytes()


def test_full_run_consistency_degradation_and_resume(workspace: Path):
    full = StreamingTTSEngine(
        workspace=workspace,
        config_path=workspace / "config/default.yaml",
        run_id="full",
    )
    outcome = full.run()
    assert outcome.status == "complete"
    assert outcome.aggregate_sha256
    summary = json.loads(full.summary_path.read_text(encoding="utf-8"))
    assert summary["utterance_count"] == 3
    assert summary["degraded_utterances"] == ["utt_budget_stress_03"]
    assert all(item["reference_match"] is True for item in summary["utterances"])
    first_trace = _trace_bytes(full.trace_path)
    first_summary = full.summary_path.read_bytes()
    first_wavs = {
        path.name: path.read_bytes() for path in sorted(full.output_root.glob("*.wav"))
    }
    assert len(first_wavs) == 3

    # A complete rerun reproduces the aggregate digest and every byte.
    rerun = full.run()
    assert rerun.aggregate_sha256 == outcome.aggregate_sha256
    assert full.summary_path.read_bytes() == first_summary
    assert full.trace_path.read_bytes() == first_trace
    assert {path.name: path.read_bytes() for path in sorted(full.output_root.glob("*.wav"))} == first_wavs

    resumed = StreamingTTSEngine(
        workspace=workspace,
        config_path=workspace / "config/default.yaml",
        run_id="resumed",
    )
    stopped = resumed.run(stop_after_chunks=3)
    assert stopped.status == "stopped"
    assert stopped.stopped_after_blocks == 3
    assert not resumed.trace_path.exists()
    assert resumed.status()["no_exit_report"] is True
    recovered = resumed.run(resume=True)
    assert recovered.status == "complete"
    assert resumed.trace_path.read_bytes() == first_trace
    recovered_wavs = {
        path.name: path.read_bytes() for path in sorted(resumed.output_root.glob("*.wav"))
    }
    assert recovered_wavs == first_wavs

    trace = [json.loads(line) for line in first_trace.decode("utf-8").splitlines()]
    assert trace
    assert sum(1 for item in trace if item["budget_kind"] == "first_packet") == 3
    assert any(item["degraded"] for item in trace if item["utt_id"] == "utt_budget_stress_03")
    assert all(item["timeout_cancellation_used"] is False for item in trace)
    assert all("start_time_ms" in item and "end_time_ms" in item for item in trace)
    assert all(item["sample_end"] >= item["sample_start"] for item in trace)

    # Same config in a fresh clone yields the same trace and fixed aggregate summary.
    clone = copy_workspace(workspace.parent / "clone")
    clone_engine = StreamingTTSEngine(
        workspace=clone,
        config_path=clone / "config/default.yaml",
        run_id="full",
    )
    clone_outcome = clone_engine.run()
    assert clone_outcome.aggregate_sha256 == outcome.aggregate_sha256
    assert clone_engine.trace_path.read_bytes() == first_trace


def test_version_invalidation_reuses_unaffected_upstream(workspace: Path):
    engine = StreamingTTSEngine(
        workspace=workspace,
        config_path=workspace / "config/default.yaml",
        run_id="versions",
    )
    engine.run()
    before = engine.store.ledger()["records"]
    front_before = {key: seq for key, seq in before.items() if ":frontend:" in key}
    acoustic_before = {key: seq for key, seq in before.items() if ":acoustic:" in key}
    waveform_before = {key: seq for key, seq in before.items() if ":waveform:" in key}

    config_path = workspace / "config/default.yaml"
    raw = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    raw["synthesis"]["noise_level"] = round(float(raw["synthesis"]["noise_level"]) + 0.001, 6)
    config_path.write_text(yaml.safe_dump(raw, allow_unicode=True, sort_keys=False), encoding="utf-8")
    changed = StreamingTTSEngine(
        workspace=workspace,
        config_path=config_path,
        run_id="versions",
    )
    changed.run()
    after = changed.store.ledger()["records"]
    front_after = {key: seq for key, seq in after.items() if ":frontend:" in key}
    acoustic_after = {key: seq for key, seq in after.items() if ":acoustic:" in key}
    waveform_after = {key: seq for key, seq in after.items() if ":waveform:" in key}
    assert front_after == front_before
    assert acoustic_after == acoustic_before
    assert {key: seq for key, seq in waveform_after.items()} != waveform_before


def test_engine_invalid_text_error_context(workspace: Path):
    import pytest
    from stream_tts.errors import TTSError
    from stream_tts.inputs import samples_to_jsonl, Utterance

    bad_config = workspace / "config/bad-split.yaml"
    raw = yaml.safe_load((workspace / "config/default.yaml").read_text(encoding="utf-8"))
    raw["split"]["first_chunk_min_chars"] = -3
    bad_config.write_text(yaml.safe_dump(raw, allow_unicode=True, sort_keys=False), encoding="utf-8")
    engine = StreamingTTSEngine(workspace=workspace, config_path=bad_config, run_id="bad-split")
    with pytest.raises(TTSError) as invalid:
        engine.run()
    assert invalid.value.code == "E_INVALID_TEXT"
    assert invalid.value.context["utt_id"] == "utt_mix_01"
    assert invalid.value.context["chunk_index"] == 0
    assert invalid.value.context["char_position"] == 0
    assert not list(engine.output_root.glob("*.wav"))

    empty_file = workspace / "data/empty.jsonl"
    empty_file.write_text(
        samples_to_jsonl(
            [
                Utterance(
                    utt_id="empty_utt",
                    text="   ",
                    language="zh",
                    speaker=1,
                    prosody={},
                )
            ]
        ),
        encoding="utf-8",
    )
    engine2 = StreamingTTSEngine(
        workspace=workspace,
        config_path=workspace / "config/default.yaml",
        run_id="empty",
    )
    with pytest.raises(TTSError) as empty:
        engine2.run(samples_path=empty_file)
    assert empty.value.code == "E_INVALID_TEXT"
    assert empty.value.context["utt_id"] == "empty_utt"


def test_engine_numeric_and_mismatch_errors_do_not_publish_wav(workspace: Path, monkeypatch):
    import numpy as np
    import pytest
    from stream_tts.errors import TTSError

    numeric_engine = StreamingTTSEngine(
        workspace=workspace,
        config_path=workspace / "config/default.yaml",
        run_id="numeric",
    )
    original_predict = numeric_engine.predictor.predict_phones

    def bad_predict(*args, **kwargs):
        result = original_predict(*args, **kwargs)
        result.values[0, 0] = np.inf
        return result

    monkeypatch.setattr(numeric_engine.predictor, "predict_phones", bad_predict)
    with pytest.raises(TTSError) as numeric:
        numeric_engine.run()
    assert numeric.value.code == "E_NUMERIC"
    assert numeric.value.context["frame_index"] == 0
    assert numeric.value.context["parameter_name"] == "band_0"
    assert not list((workspace / "output/numeric").glob("*.wav"))

    monkeypatch.undo()
    mismatch_engine = StreamingTTSEngine(
        workspace=workspace,
        config_path=workspace / "config/default.yaml",
        run_id="mismatch",
    )
    original_all = mismatch_engine.predictor.predict_all

    def bad_all(*args, **kwargs):
        result = original_all(*args, **kwargs)
        result.values[0, 0] += 0.125
        return result

    monkeypatch.setattr(mismatch_engine.predictor, "predict_all", bad_all)
    with pytest.raises(TTSError) as mismatch:
        mismatch_engine.run()
    assert mismatch.value.code == "E_STREAM_MISMATCH"
    assert mismatch.value.context["first_deviating_block"] == 0
    assert mismatch.value.context["frame_index"] == 0
    assert mismatch.value.context["sample_index"] == 0
    assert "chunked_summary" in mismatch.value.context
    assert "full_summary" in mismatch.value.context
    assert not list((workspace / "output/mismatch").glob("*.wav"))


def test_budget_and_text_changes_identify_earliest_stage(workspace: Path):
    engine = StreamingTTSEngine(
        workspace=workspace,
        config_path=workspace / "config/default.yaml",
        run_id="earliest",
    )
    engine.run()
    before = engine.store.ledger()["records"]
    front_before = {key: seq for key, seq in before.items() if ":frontend:" in key}
    acoustic_before = {key: seq for key, seq in before.items() if ":acoustic:" in key}

    config_path = workspace / "config/default.yaml"
    raw = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    raw["budget"]["continuation_ms"] = 23.5
    config_path.write_text(yaml.safe_dump(raw, allow_unicode=True, sort_keys=False), encoding="utf-8")
    budget_engine = StreamingTTSEngine(
        workspace=workspace,
        config_path=config_path,
        run_id="earliest",
    )
    budget_engine.run()
    after_budget = budget_engine.store.ledger()["records"]
    assert {key: seq for key, seq in after_budget.items() if ":frontend:" in key} == front_before
    assert {key: seq for key, seq in after_budget.items() if ":acoustic:" in key} != acoustic_before

    # A text change invalidates the earliest frontend stage and all downstream work.
    samples_path = workspace / "data/samples.jsonl"
    lines = [json.loads(line) for line in samples_path.read_text(encoding="utf-8").splitlines() if line]
    lines[0]["text"] = lines[0]["text"] + "。"
    samples_path.write_text(
        "".join(json.dumps(item, ensure_ascii=False) + "\n" for item in lines),
        encoding="utf-8",
    )
    pre_text = budget_engine.store.ledger()["records"]
    pre_front = {key: seq for key, seq in pre_text.items() if ":frontend:" in key}
    text_engine = StreamingTTSEngine(
        workspace=workspace,
        config_path=config_path,
        run_id="earliest",
    )
    text_engine.run()
    after_text = text_engine.store.ledger()["records"]
    assert {key: seq for key, seq in after_text.items() if ":frontend:" in key} != pre_front
    assert any(":validation:" in key for key in after_text)

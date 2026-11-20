from __future__ import annotations

from pathlib import Path

import numpy as np
import pytest

from sampling_engine.config import EngineConfig
from sampling_engine.errors import EngineError, E_INVALID_PARAM, E_NUMERIC
from sampling_engine.engine import SamplingEngine
from sampling_engine.models import SamplingRequest
from sampling_engine.stages import (
    run_decode,
    run_normalize,
    run_postprocess,
    run_summarize,
)

from conftest import SMALL_VOCAB, write_logits, write_v2_manifest


def _request(logits_path, **overrides):
    values = {
        "request_id": "sampling",
        "logits_path": str(logits_path),
        "temperature": 1.0,
        "top_k": 5,
        "top_p": 0.9,
        "repetition_penalty": 1.0,
        "seed": 42,
        "steps": 3,
        "prompt_tokens": (),
        "manifest_version": 2,
        "vocab": tuple(SMALL_VOCAB),
        "base_dir": str(Path(logits_path).parent),
    }
    values.update(overrides)
    return SamplingRequest(**values)


def _run_all_stages(request, tmp_path, *, max_temperature=5.0):
    tmp_path.mkdir(parents=True, exist_ok=True)
    post = run_postprocess(
        request,
        request.resolved_logits_path(),
        tmp_path / "post.npy",
        max_temperature=max_temperature,
    )
    norm = run_normalize(
        request,
        tmp_path / "post.npy",
        post.metadata,
        post.artifact_digest,
        tmp_path / "norm.npz",
    )
    dec = run_decode(
        request,
        tmp_path / "norm.npz",
        norm.metadata,
        norm.artifact_digest,
        tmp_path / "decode.json",
    )
    summary = run_summarize(
        request,
        tmp_path / "post.npy",
        post.metadata,
        post.artifact_digest,
        tmp_path / "norm.npz",
        norm.metadata,
        norm.artifact_digest,
        tmp_path / "decode.json",
        dec.artifact_digest,
        tmp_path / "summary.json",
        version_fingerprint="version-test",
        config_version="config-test",
    )
    return post, norm, dec, summary


def test_tied_maxima_are_deterministic_and_token_id_sorted(tmp_path):
    matrix = np.zeros((3, 8), dtype=np.float64)
    matrix[:, 2] = 4.0
    matrix[:, 5] = 4.0
    logits = write_logits(tmp_path / "tie.npy", matrix)
    request = _request(logits)
    first = run_postprocess(
        request, logits, tmp_path / "first.npy", max_temperature=5.0
    )
    second = run_postprocess(
        request, logits, tmp_path / "second.npy", max_temperature=5.0
    )
    assert first.artifact_digest == second.artifact_digest
    assert first.metadata["tie_max_indices"] == [[2, 5], [2, 5], [2, 5]]
    assert (tmp_path / "first.npy").read_bytes() == (tmp_path / "second.npy").read_bytes()


def test_repetition_penalty_and_temperature_are_applied_in_postprocess(tmp_path):
    matrix = np.array([[2.0, -2.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]])
    logits = write_logits(tmp_path / "penalty.npy", matrix)
    request = _request(
        logits,
        steps=1,
        temperature=0.5,
        repetition_penalty=2.0,
        prompt_tokens=(0, 1),
    )
    run_postprocess(request, logits, tmp_path / "post.npy", max_temperature=5.0)
    result = np.load(tmp_path / "post.npy")
    assert result[0, 0] == pytest.approx(2.0)
    assert result[0, 1] == pytest.approx(-8.0)


def test_equal_probability_top_p_crossing_is_deterministic(tmp_path):
    matrix = np.zeros((2, 8), dtype=np.float64)
    logits = write_logits(tmp_path / "equal.npy", matrix)
    request = _request(logits, steps=2, top_k=None, top_p=0.3, temperature=1.0)
    post = run_postprocess(request, logits, tmp_path / "post.npy", max_temperature=5.0)
    norm = run_normalize(
        request,
        tmp_path / "post.npy",
        post.metadata,
        post.artifact_digest,
        tmp_path / "norm.npz",
    )
    with np.load(tmp_path / "norm.npz") as arrays:
        candidates = arrays["candidate_indices"]
        probs = arrays["probs"]
    # Each probability is 1/8. The crossing token is included, so the first
    # three ascending token IDs are selected (0.125 + 0.125 + 0.125 >= .3).
    assert candidates[0].tolist() == [0, 1, 2, -1, -1, -1, -1, -1]
    assert np.count_nonzero(probs[0]) == 3
    # Re-running produces the same logical artifact digest.
    post2 = run_postprocess(
        request, logits, tmp_path / "post2.npy", max_temperature=5.0
    )
    norm2 = run_normalize(
        request,
        tmp_path / "post2.npy",
        post2.metadata,
        post2.artifact_digest,
        tmp_path / "norm2.npz",
    )
    assert norm.artifact_digest == norm2.artifact_digest


def test_log_softmax_avoids_overflow(tmp_path):
    matrix = np.array([[1000.0, 1001.0, 999.0, -1000.0, 0.0, 1.0, 2.0, 3.0]])
    logits = write_logits(tmp_path / "huge.npy", matrix)
    request = _request(logits, steps=1, top_k=None, top_p=1.0)
    post = run_postprocess(request, logits, tmp_path / "post.npy", max_temperature=5.0)
    norm = run_normalize(
        request,
        tmp_path / "post.npy",
        post.metadata,
        post.artifact_digest,
        tmp_path / "norm.npz",
    )
    with np.load(tmp_path / "norm.npz") as arrays:
        probs = arrays["probs"]
    assert np.isfinite(probs).all()
    assert probs.sum() == pytest.approx(1.0, abs=1e-12)


def test_same_seed_is_byte_reproducible(tmp_path):
    matrix = np.random.default_rng(9).normal(size=(5, 8))
    logits = write_logits(tmp_path / "seed.npy", matrix)
    request = _request(logits, steps=5, seed=123456)
    _, _, decode_a, summary_a = _run_all_stages(request, tmp_path / "a")
    _, _, decode_b, summary_b = _run_all_stages(request, tmp_path / "b")
    assert decode_a.artifact_digest == decode_b.artifact_digest
    assert summary_a.artifact_digest == summary_b.artifact_digest
    assert decode_a.metadata["tokens"] == decode_b.metadata["tokens"]
    assert (tmp_path / "a" / "decode.json").read_bytes() == (
        tmp_path / "b" / "decode.json"
    ).read_bytes()


@pytest.mark.parametrize(
    "bad_value,step,reason",
    [
        (np.nan, 1, "nan"),
        (np.inf, 2, "positive_infinity"),
    ],
)
def test_nan_and_inf_return_numeric_error_with_step(tmp_path, bad_value, step, reason):
    matrix = np.zeros((3, 8), dtype=np.float64)
    matrix[step, 3] = bad_value
    logits = write_logits(tmp_path / "bad.npy", matrix)
    request = _request(logits)
    with pytest.raises(EngineError) as info:
        run_postprocess(request, logits, tmp_path / "post.npy", max_temperature=5.0)
    assert info.value.code == E_NUMERIC
    assert info.value.context["request_id"] == "sampling"
    assert info.value.context["step_index"] == step
    assert info.value.context["reason"] == reason
    assert info.value.context["context_digest"]


def test_all_negative_infinity_is_numeric_error(tmp_path):
    matrix = np.full((2, 8), -np.inf)
    logits = write_logits(tmp_path / "neginf.npy", matrix)
    request = _request(logits, steps=2)
    with pytest.raises(EngineError) as info:
        run_postprocess(request, logits, tmp_path / "post.npy", max_temperature=5.0)
    assert info.value.code == E_NUMERIC
    assert info.value.context["step_index"] == 0
    assert info.value.context["reason"] == "all_negative_infinity"


@pytest.mark.parametrize(
    "temperature,parameter", [(0.0, "temperature"), (-1.0, "temperature"), (6.0, "temperature")]
)
def test_invalid_parameter_has_request_and_name(tmp_path, temperature, parameter):
    matrix = np.zeros((1, 8), dtype=np.float64)
    logits = write_logits(tmp_path / "temp.npy", matrix)
    request = _request(logits, steps=1, temperature=temperature)
    with pytest.raises(EngineError) as info:
        run_postprocess(request, logits, tmp_path / "post.npy", max_temperature=5.0)
    assert info.value.code == E_INVALID_PARAM
    assert info.value.context["request_id"] == "sampling"
    assert info.value.context["parameter"] == parameter
    assert info.value.context["value"] == temperature


def test_engine_emits_nan_error_and_preserves_diagnostics(tmp_path):
    matrix = np.zeros((2, 8), dtype=np.float64)
    matrix[1, 4] = np.nan
    logits = write_logits(tmp_path / "nan.npy", matrix)
    manifest = write_v2_manifest(
        tmp_path / "manifest.json",
        logits,
        requests=[{"request_id": "req_a", "logits_path": str(logits), "temperature": 0.8, "top_k": 4, "top_p": 0.9, "repetition_penalty": 1.1, "seed": 123, "steps": 2, "prompt_tokens": []}],
    )
    engine = SamplingEngine(tmp_path / "workspace", EngineConfig(max_temperature=5.0))
    engine.import_manifest_path(manifest)
    result = engine.run()
    assert result["processed"][0]["status"] == "error"
    error = result["processed"][0]["error"]
    assert error["code"] == E_NUMERIC
    assert error["context"]["step_index"] == 1
    status = engine.request_status("req_a")
    assert status["status"] == "error"
    assert status["error"]["error"]["code"] == E_NUMERIC


def test_large_vocab_and_mixed_language_sample_runs(tmp_path):
    engine = SamplingEngine(
        tmp_path / "workspace",
        EngineConfig(max_temperature=5.0),
    )
    # Use the repository's deterministic large-vocabulary manifest.
    engine.import_manifest_path(Path("data/manifest_v2_large.json"))
    result = engine.run(request_ids=["r2_large"])
    processed = result["processed"][0]
    assert processed["status"] == "ok"
    assert len(processed["tokens"]) == 10
    status = engine.request_status("r2_large")
    conversion = engine.request_temperature_detail("r2_large")
    assert status["status"] == "complete"
    assert conversion["top_p"] == 0.92

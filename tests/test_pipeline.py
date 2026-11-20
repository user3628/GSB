from __future__ import annotations

import copy
import json
from dataclasses import replace
from pathlib import Path

import pytest

from qworkbench.config import WorkbenchConfig
from qworkbench.errors import BusinessError
from qworkbench.pipeline import Pipeline
from qworkbench.samples import SampleDataset
from qworkbench.util import sha256_json


def _run(config, dataset, workdir: Path, **kwargs):
    return Pipeline(config, dataset, workdir, **kwargs).run()


def _modified_config(config: WorkbenchConfig, mutate) -> WorkbenchConfig:
    raw = copy.deepcopy(dict(config.raw))
    mutate(raw)
    return WorkbenchConfig.from_dict(raw)


def _sample_fingerprint(samples) -> str:
    return sha256_json(
        [
            {
                key: sample.get(key)
                for key in ("id", "instruction", "input", "output", "source", "lang")
            }
            for sample in samples
        ]
    )


def _annotation_fingerprint(annotations) -> str:
    return sha256_json(
        [{"id": key, "score": annotations[key]} for key in sorted(annotations)]
    )


def test_complete_run_is_byte_deterministic(config, dataset, tmp_path) -> None:
    first_dir = tmp_path / "first"
    second_dir = tmp_path / "second"
    first = _run(config, dataset, first_dir)
    second = _run(config, dataset, second_dir)
    assert first["status"] == second["status"] == "COMPLETE"
    first_mixture = (first_dir / "artifacts/mix/mixture.jsonl").read_bytes()
    second_mixture = (second_dir / "artifacts/mix/mixture.jsonl").read_bytes()
    assert first_mixture == second_mixture
    assert first["final_fingerprint"] == second["final_fingerprint"]
    assert first["mixture_sha256"] == second["mixture_sha256"]
    checkpoint_files = sorted((first_dir / "checkpoints").glob("*"))
    assert len(checkpoint_files) == 4
    assert not list((first_dir / "checkpoints").glob("*.tmp"))
    for path in checkpoint_files:
        record = json.loads(path.read_text(encoding="utf-8"))
        assert {"stage", "seq", "input_fingerprint", "config_version", "summary"} <= set(record)


def test_canonical_demo_fingerprint(config, dataset, tmp_path) -> None:
    result = _run(config, dataset, tmp_path / "canonical")
    assert result["final_fingerprint"] == (
        "2867c079628097d3613eb63e0548b0665d4853e9096c25be134964a08173ce17"
    )
    assert result["mixture_sha256"] == (
        "c205b9a582ff771278a8375561a79bce86562aca12c6f3478be0562f3ef18ad6"
    )


def test_graceful_stop_then_resume_matches_complete_run(config, dataset, tmp_path) -> None:
    complete_dir = tmp_path / "complete"
    stopped_dir = tmp_path / "stopped"
    complete = _run(config, dataset, complete_dir)
    stopped = _run(config, dataset, stopped_dir, stop_after_stage="normalize")
    assert stopped["status"] == "STOPPED"
    assert stopped["completed_stages"] == ["normalize"]
    status = Pipeline(config, dataset, stopped_dir).status()
    assert status["stages"]["normalize"]["valid"] is True
    assert status["stages"]["score"]["valid"] is False
    normalize_seq = status["stages"]["normalize"]["seq"]
    resumed = Pipeline(config, dataset, stopped_dir).resume()
    assert resumed["status"] == "COMPLETE"
    assert "normalize" in resumed["reused_stages"]
    resumed_status = Pipeline(config, dataset, stopped_dir).status()
    assert resumed_status["stages"]["normalize"]["seq"] == normalize_seq
    assert (complete_dir / "artifacts/mix/mixture.jsonl").read_bytes() == (
        stopped_dir / "artifacts/mix/mixture.jsonl"
    ).read_bytes()
    assert complete["mixture_sha256"] == resumed["mixture_sha256"]


def test_invalidation_propagates_from_earliest_changed_stage(config, dataset, tmp_path) -> None:
    workdir = tmp_path / "incremental"
    baseline = _run(config, dataset, workdir)
    baseline_status = Pipeline(config, dataset, workdir).status()
    assert baseline_status["stages"]["normalize"]["seq"] == 1

    quant_config = _modified_config(
        config, lambda raw: raw["quantization"].__setitem__("fraction_bits", 18)
    )
    quant_run = _run(quant_config, dataset, workdir)
    assert "normalize" in quant_run["reused_stages"]
    quant_status = Pipeline(quant_config, dataset, workdir).status()
    assert quant_status["last_invalidation"]["earliest_stage"] == "score"
    assert quant_status["last_invalidation"]["affected_stages"] == ["score", "allocate", "mix"]
    assert quant_status["stages"]["normalize"]["seq"] == 1
    assert quant_status["stages"]["score"]["seq"] == 5

    allocation_config = _modified_config(
        quant_config, lambda raw: raw["allocation"].__setitem__("target_size", 29)
    )
    allocation_run = _run(allocation_config, dataset, workdir)
    assert allocation_run["reused_stages"] == ["normalize", "score"]
    allocation_status = Pipeline(allocation_config, dataset, workdir).status()
    assert allocation_status["last_invalidation"]["earliest_stage"] == "allocate"
    assert allocation_status["stages"]["score"]["seq"] == 5

    annotations = dict(dataset.annotations)
    annotations[dataset.samples[0]["id"]] = 1.0
    annotation_dataset = replace(
        dataset,
        annotations=annotations,
        annotation_fingerprint=_annotation_fingerprint(annotations),
    )
    annotation_run = _run(allocation_config, annotation_dataset, workdir)
    assert annotation_run["reused_stages"] == ["normalize", "score", "allocate"]
    annotation_status = Pipeline(allocation_config, annotation_dataset, workdir).status()
    assert annotation_status["last_invalidation"]["earliest_stage"] == "mix"
    assert annotation_status["stages"]["allocate"]["seq"] == 8

    changed_samples = copy.deepcopy(dataset.samples)
    changed_samples[0]["output"] += " content-change-probe"
    content_dataset = replace(
        dataset,
        samples=changed_samples,
        sample_fingerprint=_sample_fingerprint(changed_samples),
    )
    content_run = _run(allocation_config, content_dataset, workdir)
    assert content_run["status"] == "COMPLETE"
    content_status = Pipeline(allocation_config, content_dataset, workdir).status()
    assert content_status["last_invalidation"]["earliest_stage"] == "normalize"
    assert content_status["last_invalidation"]["affected_stages"] == [
        "normalize",
        "score",
        "allocate",
        "mix",
    ]
    assert baseline["final_fingerprint"] != content_run["final_fingerprint"]


def test_starting_without_predecessor_returns_invalid_stage(config, dataset, tmp_path) -> None:
    pipeline = Pipeline(config, dataset, tmp_path / "missing")
    with pytest.raises(BusinessError) as caught:
        pipeline.run(start_stage="score")
    assert caught.value.code == "E_INVALID_STAGE"
    assert caught.value.details["required_predecessor"] == "normalize"


def test_empty_output_fallback_keeps_sample_in_mixture(config, dataset, tmp_path) -> None:
    workdir = tmp_path / "fallback"
    _run(config, dataset, workdir)
    pipeline = Pipeline(config, dataset, workdir)
    diagnostics = pipeline.diagnostics()
    empty = next(sample for sample in diagnostics["samples"] if sample["sample_id"] == "anomaly-empty-output")
    assert empty["fallback_reasons"]
    assert empty["quality_score"] > 0.0

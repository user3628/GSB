"""Deterministic offline demonstration of the complete query understanding chain."""

from __future__ import annotations

import json
import shutil
import tempfile
from pathlib import Path
from typing import Any

from .engine import QueryEngine
from .errors import QueryError
from .experiment import experiment_bytes, run_experiment
from .io import canonical_json, pretty_json, sha256_bytes, write_json


def run_demo(
    *,
    config_path: Path,
    training_path: Path,
    dictionary_path: Path,
    eval_path: Path,
    output_dir: Path,
    seed: int,
) -> bytes:
    output_dir.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="qiu-demo-") as temporary:
        root = Path(temporary)
        local_config = root / "config.json"
        local_training = root / "training.jsonl"
        local_dictionary = root / "dictionary.json"
        shutil.copyfile(config_path, local_config)
        shutil.copyfile(training_path, local_training)
        shutil.copyfile(dictionary_path, local_dictionary)

        baseline = _engine(local_config, local_training, local_dictionary)
        independent = _engine(local_config, local_training, local_dictionary)
        sample = {
            "channel": "map",
            "history_categories": [],
            "locale": "zh-CN",
            "query_id": "demo-determinism",
            "raw_query": "  Ｈｅｌｌｏ　北京咖啡店!!! ",
        }
        first = baseline.parse(sample)
        second = independent.parse(sample)
        first_bytes = canonical_json(first).encode("utf-8")
        second_bytes = canonical_json(second).encode("utf-8")

        overlap_result = baseline.parse(
            {
                "channel": "shop",
                "history_categories": [],
                "locale": "zh-CN",
                "query_id": "demo-overlap",
                "raw_query": "买苹果手机",
            },
            {
                "overlap_slot": {
                    "end": 5,
                    "start": 1,
                    "text": "苹果手机",
                    "type": "brand",
                }
            },
        )

        fallback_examples = []
        for variant in (
            {"channel": "web", "history_categories": ["餐厅"], "label": "history_prior"},
            {"channel": "app", "history_categories": [], "label": "channel_default"},
            {"channel": "unknown", "history_categories": [], "label": "final_default"},
        ):
            result = baseline.parse(
                {
                    "channel": variant["channel"],
                    "history_categories": variant["history_categories"],
                    "locale": "zh-CN",
                    "query_id": f"demo-fallback-{variant['label']}",
                    "raw_query": "你好",
                }
            )
            fallback_examples.append(
                {
                    "case": variant["label"],
                    "intent": result["intent"]["label"],
                    "source": result["intent"]["source"],
                    "trace": result["intent"]["fallback_trace"],
                    "used_fallback_level": result["intent"]["used_fallback_level"],
                }
            )

        error_cases = _error_cases(baseline)
        cache_check = baseline.parse(sample)

        invalidation = _invalidation_demo(
            config_path=local_config,
            training_path=local_training,
            dictionary_path=local_dictionary,
        )

        experiment_engine = _engine(config_path, training_path, dictionary_path)
        summary_a = run_experiment(experiment_engine, eval_path=eval_path, seed=seed, bootstrap_samples=200)
        summary_b = run_experiment(experiment_engine, eval_path=eval_path, seed=seed, bootstrap_samples=200)
        summary_other = run_experiment(experiment_engine, eval_path=eval_path, seed=seed + 1, bootstrap_samples=200)
        bytes_a = experiment_bytes(summary_a)
        bytes_b = experiment_bytes(summary_b)
        bytes_other = experiment_bytes(summary_other)

        (output_dir / f"experiment_seed{seed}_a.json").write_bytes(bytes_a)
        (output_dir / f"experiment_seed{seed}_b.json").write_bytes(bytes_b)
        (output_dir / f"experiment_seed{seed + 1}.json").write_bytes(bytes_other)

        report = {
            "cache_reuse_check": {
                "second_parse_status": cache_check["cache"]["status"],
                "second_parse_reused_stages": cache_check["cache"]["reused_stages"],
            },
            "deterministic_parse": {
                "byte_identical_across_independent_engines": first_bytes == second_bytes,
                "intent_components": first["intent"]["candidates"],
                "intent_label": first["intent"]["label"],
                "normalized_query": first["normalization"]["normalized_query"],
                "result_sha256": sha256_bytes(first_bytes),
                "slot_count": len(first["slots"]),
                "tokens": first["normalization"]["tokens"],
            },
            "error_injection": error_cases,
            "experiment": {
                "different_seed_fingerprint": summary_other["run_fingerprint"],
                "different_seed_summary_differs": bytes_a != bytes_other,
                "metrics": summary_a["metrics"],
                "same_seed_byte_identical": bytes_a == bytes_b,
                "same_seed_run_fingerprint": summary_a["run_fingerprint"],
            },
            "fallback_chain": fallback_examples,
            "invalidation_rebuild": invalidation,
            "schema_version": "qiu-demo-v1",
            "slot_overlap": {
                "accepted": overlap_result["slots"],
                "suppressed": overlap_result["slot_diagnostics"]["suppressed"],
            },
        }
        payload = (canonical_json(report) + "\n").encode("utf-8")
        (output_dir / "demo_report.json").write_bytes(payload)
        return payload


def _error_cases(engine: QueryEngine) -> list[dict[str, Any]]:
    base = {
        "channel": "web",
        "history_categories": [],
        "locale": "zh-CN",
        "query_id": "demo-error",
        "raw_query": "北京咖啡店",
    }
    cases = [
        ("invalid_locale", base, {"invalid_locale": True}),
        ("empty_query", {**base, "raw_query": "！！？。"}, {}),
        ("query_too_long", base, {"force_query_length": 161}),
        ("dict_version", base, {"dict_version": "slot-dict-v999"}),
        ("numeric", base, {"nan_confidence": True}),
        ("dict_missing", base, {"missing_dictionary": True}),
    ]
    results: list[dict[str, Any]] = []
    for name, sample, faults in cases:
        errors: list[bytes] = []
        for _ in range(2):
            try:
                engine.parse(sample, faults)
                raise AssertionError(f"injection {name} did not fail")
            except QueryError as exc:
                errors.append(canonical_json(exc.to_dict()).encode("utf-8"))
        assert errors[0] == errors[1]
        error = json.loads(errors[0])["error"]
        results.append(
            {
                "code": error["code"],
                "details": error["details"],
                "name": name,
                "query_id": error["query_id"],
                "reproduced_byte_identically": errors[0] == errors[1],
                "stage": error["stage"],
            }
        )
    return results


def _invalidation_demo(*, config_path: Path, training_path: Path, dictionary_path: Path) -> list[dict[str, Any]]:
    engine = _engine(config_path, training_path, dictionary_path)
    sample = {
        "channel": "map",
        "history_categories": [],
        "locale": "zh-CN",
        "query_id": "demo-invalidation",
        "raw_query": "北京咖啡店",
    }
    engine.parse(sample)
    second = engine.parse(sample)
    events: list[dict[str, Any]] = [
        {
            "change": "none",
            "invalidated_stages": second["cache"]["invalidated_stages"],
            "reused_stages": second["cache"]["reused_stages"],
            "status": second["cache"]["status"],
        }
    ]

    config = json.loads(config_path.read_text(encoding="utf-8"))
    config["policy"]["confidence_threshold"] = 0.63
    config_path.write_text(pretty_json(config), encoding="utf-8")
    result = engine.parse(sample)
    events.append(_cache_event("policy.threshold", result))

    dictionary = json.loads(dictionary_path.read_text(encoding="utf-8"))
    dictionary["entries"].append({"canonical": "演示词", "term": "演示词", "type": "brand"})
    dictionary_path.write_text(pretty_json(dictionary), encoding="utf-8")
    result = engine.parse(sample)
    events.append(_cache_event("dictionary.entries", result))

    with training_path.open("a", encoding="utf-8") as handle:
        handle.write(
            canonical_json(
                {
                    "channel": "app",
                    "history_categories": [],
                    "intent": "navigation",
                    "locale": "zh-CN",
                    "query_id": "train-demo-extra",
                    "raw_query": "华为官网",
                }
            )
            + "\n"
        )
    result = engine.parse(sample)
    events.append(_cache_event("training.samples", result))
    return events


def _cache_event(change: str, result: dict[str, Any]) -> dict[str, Any]:
    cache = result["cache"]
    return {
        "change": change,
        "changed_components": {
            stage: cache["stage_status"][stage].get("changed_components", [])
            for stage in cache["invalidated_stages"]
        },
        "invalidated_stages": cache["invalidated_stages"],
        "reused_stages": cache["reused_stages"],
        "status": cache["status"],
        "version_fingerprints": result["version_fingerprints"],
    }


def _engine(config_path: Path, training_path: Path, dictionary_path: Path) -> QueryEngine:
    return QueryEngine(
        config_path=config_path,
        training_path=training_path,
        dictionary_path=dictionary_path,
    )

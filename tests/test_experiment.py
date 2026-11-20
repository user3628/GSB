from __future__ import annotations

from qiu.engine import QueryEngine
from qiu.experiment import experiment_bytes, run_experiment


def test_experiment_summary_is_byte_stable_and_seed_sensitive(engine: QueryEngine, data_paths: dict) -> None:
    first = run_experiment(engine, eval_path=data_paths["eval"], seed=17, bootstrap_samples=40)
    second = run_experiment(engine, eval_path=data_paths["eval"], seed=17, bootstrap_samples=40)
    other = run_experiment(engine, eval_path=data_paths["eval"], seed=18, bootstrap_samples=40)
    assert experiment_bytes(first) == experiment_bytes(second)
    assert experiment_bytes(first) != experiment_bytes(other)
    assert first["run_fingerprint"] == second["run_fingerprint"]
    assert first["run_fingerprint"] != other["run_fingerprint"]
    assert first["config_fingerprint"] == second["config_fingerprint"] == other["config_fingerprint"]
    metrics = first["metrics"]
    assert metrics["sample_count"] >= 30
    assert 0.0 <= metrics["intent_accuracy"] <= 1.0
    assert 0.0 <= metrics["slot_precision"] <= 1.0
    assert 0.0 <= metrics["slot_recall"] <= 1.0
    assert 0.0 <= metrics["fallback_trigger_rate"] <= 1.0

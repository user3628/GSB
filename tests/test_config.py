"""Requirement 6: configuration validation error codes."""
from __future__ import annotations

import json

import pytest

from depworkbench.config import parse_config
from depworkbench.errors import E_INVALID_CONFIG, WorkbenchError

BASE = {
    "model": {
        "weights": {
            "shift": [1, 0, 0, 0, 0, 0],
            "left": [0, 1, 0, 0, 0, 0],
            "right": [0, 0, 1, 0, 0, 0],
        },
        "max_dependency_distance": 5, "unique_root": True,
    },
    "parallel": {"worker_threads": 2},
    "budget": {"per_sentence_max_tokens": 10,
               "global_total_tokens": 100, "worker_token_quota": 50},
}


def test_valid_config_loads():
    cfg = parse_config(BASE)
    assert cfg.parallel.worker_threads == 2
    assert cfg.config_fingerprint


def test_distance_out_of_range():
    raw = json_clone(BASE)
    raw["model"]["max_dependency_distance"] = 100000
    with pytest.raises(WorkbenchError) as exc:
        parse_config(raw)
    assert exc.value.code == E_INVALID_CONFIG


def test_zero_distance_rejected():
    raw = json_clone(BASE)
    raw["model"]["max_dependency_distance"] = 0
    with pytest.raises(WorkbenchError) as exc:
        parse_config(raw)
    assert exc.value.code == E_INVALID_CONFIG


def test_non_numeric_weight_rejected():
    raw = json_clone(BASE)
    raw["model"]["weights"]["right"][1] = "NaN"
    with pytest.raises(WorkbenchError) as exc:
        parse_config(raw)
    assert exc.value.code == E_INVALID_CONFIG


def test_infinite_weight_rejected():
    raw = json_clone(BASE)
    raw["model"]["weights"]["left"][0] = float("inf")
    with pytest.raises(WorkbenchError) as exc:
        parse_config(raw)
    assert exc.value.code == E_INVALID_CONFIG


def test_wrong_weight_vector_length():
    raw = json_clone(BASE)
    raw["model"]["weights"]["shift"] = [1, 2, 3]
    with pytest.raises(WorkbenchError) as exc:
        parse_config(raw)
    assert exc.value.code == E_INVALID_CONFIG


def test_quota_larger_than_global_rejected():
    raw = json_clone(BASE)
    raw["budget"]["worker_token_quota"] = 500
    with pytest.raises(WorkbenchError) as exc:
        parse_config(raw)
    assert exc.value.code == E_INVALID_CONFIG


def test_fingerprint_changes_with_weight_change():
    first = parse_config(BASE).config_fingerprint
    raw = json_clone(BASE)
    raw["model"]["weights"]["shift"][0] = 2.5
    assert parse_config(raw).config_fingerprint != first


def json_clone(value):
    return json.loads(json.dumps(value))

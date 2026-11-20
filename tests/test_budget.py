"""Requirement 2: budget admission planning."""
from __future__ import annotations

import pytest

from depworkbench.config import parse_config
from depworkbench.errors import (E_BUDGET_GLOBAL, E_BUDGET_SENTENCE,
                                 E_BUDGET_WORKER)
from depworkbench.budget import plan_admission
from depworkbench.shards import load_shards


def _config(workers, per_sentence, global_total, worker_quota):
    return parse_config({
        "model": {
            "weights": {
                "shift": [1, 0, 0, 0, 0, 0],
                "left": [0, 1, 0, 0, 0, 0],
                "right": [0, 0, 1, 0, 0, 0],
            },
            "max_dependency_distance": 20, "unique_root": True,
        },
        "parallel": {"worker_threads": workers},
        "budget": {"per_sentence_max_tokens": per_sentence,
                   "global_total_tokens": global_total,
                   "worker_token_quota": worker_quota},
    })


def test_default_plan_admits_all_within_budget(ensure_samples, config):
    shards = load_shards(ensure_samples, ensure_samples / "manifest.json")
    plan = plan_admission(shards, config.budget, config.parallel.worker_threads)
    assert all(d.admitted for d in plan.decisions)
    assert plan.global_consumed == sum(s.token_count for s in shards)
    assert plan.global_consumed <= plan.global_budget
    for worker, consumed in plan.worker_consumed.items():
        assert consumed <= plan.worker_quota


def test_global_budget_rejection(ensure_samples):
    shards = load_shards(ensure_samples, ensure_samples / "manifest.json")
    cfg = _config(3, 40, 100, 90)
    plan = plan_admission(shards, cfg.budget, cfg.parallel.worker_threads)
    assert plan.decisions[0].admitted
    assert plan.decisions[1].reason_code == E_BUDGET_GLOBAL
    assert plan.global_consumed <= 100


def test_worker_quota_rejection(ensure_samples):
    shards = load_shards(ensure_samples, ensure_samples / "manifest.json")
    cfg = _config(3, 40, 1000, 30)
    plan = plan_admission(shards, cfg.budget, cfg.parallel.worker_threads)
    rejected = [d for d in plan.decisions if not d.admitted]
    assert all(d.reason_code == E_BUDGET_WORKER for d in rejected)


def test_per_sentence_limit_rejection(ensure_samples):
    shards = load_shards(ensure_samples, ensure_samples / "manifest.json")
    cfg = _config(3, 1, 1000, 1000)
    plan = plan_admission(shards, cfg.budget, cfg.parallel.worker_threads)
    assert plan.sentence_skips
    assert plan.sentence_skips[0].code == E_BUDGET_SENTENCE
    assert all(not d.admitted for d in plan.decisions)


def test_cumulative_consumption_never_exceeds_global(ensure_samples):
    shards = load_shards(ensure_samples, ensure_samples / "manifest.json")
    for global_total in (50, 100, 150, 250):
        cfg = _config(3, 40, global_total, global_total)
        plan = plan_admission(shards, cfg.budget, cfg.parallel.worker_threads)
        assert plan.global_consumed <= global_total

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from obs_sampler.policy import Policy, policy_digest
from obs_sampler.service import SamplerService

ROOT = Path(__file__).resolve().parents[1]
SAMPLES = ROOT / "samples"


def load_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def load_jsonl(path: Path) -> list[dict[str, Any]]:
    return [
        json.loads(line)
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]


def policy_document(
    *,
    version: int = 1,
    seed: int = 7,
    rates: dict[str, dict[str, float]] | None = None,
    dedup_window_ms: int = 500,
    dedup_scope: str = "trace_per_kind",
    min_samples: int = 1,
    error_bound: float = 0.5,
    tail_threshold_us: int = 100_000,
    tail_base_rate: float = 0.5,
    error_multiplier: float = 2.0,
) -> dict[str, Any]:
    document: dict[str, Any] = {
        "schema_version": 1,
        "version": version,
        "generated_at": f"2026-11-20T00:00:{version:02d}.000Z",
        "seed": seed,
        "dedup_window_ms": dedup_window_ms,
        "dedup_scope": dedup_scope,
        "min_samples": min_samples,
        "error_bound": error_bound,
        "bias_correction": True,
        "strata": rates or {"checkout": {"INFO": 0.5, "ERROR": 1.0}},
        "tail_sampling": {
            "latency_threshold_us": tail_threshold_us,
            "base_rate": tail_base_rate,
            "error_multiplier": error_multiplier,
        },
    }
    document["digest"] = policy_digest(document)
    return document


def make_log(
    index: int,
    *,
    timestamp_ms: int | None = None,
    trace_id: str | None = None,
    span_id: str | None = None,
    service: str = "checkout",
    level: str = "INFO",
    sample_weight: float = 1.0,
) -> dict[str, Any]:
    return {
        "timestamp_ms": 1_000 + index if timestamp_ms is None else timestamp_ms,
        "service": service,
        "level": level,
        "message_fingerprint": f"fp-{index}",
        "trace_id": trace_id or f"trace-{index}",
        "span_id": span_id or f"log-span-{index}",
        "parent_span_id": f"root-{index}",
        "sample_weight": sample_weight,
    }


def make_span(
    index: int,
    *,
    timestamp_ms: int | None = None,
    trace_id: str | None = None,
    span_id: str | None = None,
    service: str = "checkout",
    level: str = "INFO",
    latency_us: int = 10_000,
    error: bool = False,
    sample_weight: float = 1.0,
) -> dict[str, Any]:
    return {
        "timestamp_ms": 1_000 + index if timestamp_ms is None else timestamp_ms,
        "service": service,
        "level": level,
        "message_fingerprint": f"span-op-{index}",
        "trace_id": trace_id or f"trace-{index}",
        "span_id": span_id or f"span-{index}",
        "parent_span_id": f"root-{index}",
        "latency_us": latency_us,
        "error": error,
        "sample_weight": sample_weight,
    }


@pytest.fixture
def sample_policy_v1() -> dict[str, Any]:
    return load_json(SAMPLES / "policy_v1.json")


@pytest.fixture
def sample_policy_v2() -> dict[str, Any]:
    return load_json(SAMPLES / "policy_v2.json")


@pytest.fixture
def sample_logs() -> list[dict[str, Any]]:
    return load_jsonl(SAMPLES / "logs.jsonl")


@pytest.fixture
def sample_spans() -> list[dict[str, Any]]:
    return load_jsonl(SAMPLES / "spans.jsonl")


@pytest.fixture
def naive_expected() -> dict[str, Any]:
    return load_json(SAMPLES / "naive_expected.json")


@pytest.fixture
def service(tmp_path: Path) -> SamplerService:
    return SamplerService(tmp_path / "sampler.sqlite3")


@pytest.fixture
def seeded_service(
    tmp_path: Path,
    sample_policy_v1: dict[str, Any],
    sample_logs: list[dict[str, Any]],
    sample_spans: list[dict[str, Any]],
) -> SamplerService:
    instance = SamplerService(tmp_path / "seeded.sqlite3")
    instance.submit_policy(sample_policy_v1)
    from obs_sampler.models import BatchRequest

    instance.submit_batch(
        BatchRequest(batch_id="fixed-sample-batch-1", logs=sample_logs, spans=sample_spans)
    )
    return instance


def sample_policy_object(
    *,
    version: int = 1,
    seed: int = 7,
    rates: dict[str, dict[str, float]] | None = None,
    dedup_window_ms: int = 500,
    dedup_scope: str = "trace_per_kind",
    min_samples: int = 1,
    tail_threshold_us: int = 100_000,
    tail_base_rate: float = 0.5,
    error_multiplier: float = 2.0,
) -> Policy:
    return Policy.from_mapping(
        policy_document(
            version=version,
            seed=seed,
            rates=rates,
            dedup_window_ms=dedup_window_ms,
            dedup_scope=dedup_scope,
            min_samples=min_samples,
            tail_threshold_us=tail_threshold_us,
            tail_base_rate=tail_base_rate,
            error_multiplier=error_multiplier,
        )
    )

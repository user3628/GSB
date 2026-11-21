"""Deadline-aware diagnostic queries with verifiable cache payloads."""

from __future__ import annotations

import math
import time
import uuid
from pathlib import Path
from typing import Any, Mapping

import numpy as np

from .artifacts import ArtifactRef, ArtifactStore
from .cache import QueryCache
from .config import RopeConfig
from .data import ShardData
from .digest import canonical_json_bytes, sha256_bytes
from .errors import RopeError


class Deadline:
    def __init__(self, budget_ms: float, query_id: str) -> None:
        self.budget_ms = float(budget_ms)
        self.query_id = query_id
        self.started = time.perf_counter()
        self.deadline = self.started + self.budget_ms / 1000.0

    def elapsed_ms(self) -> float:
        return (time.perf_counter() - self.started) * 1000.0

    def check(self) -> None:
        if time.perf_counter() > self.deadline:
            raise RopeError(
                "E_LATENCY_BUDGET",
                "查询超出延迟预算",
                {
                    "query_id": self.query_id,
                    "elapsed_ms": round(self.elapsed_ms(), 6),
                    "budget_ms": self.budget_ms,
                },
            )

    def finish(self) -> None:
        if self.elapsed_ms() > self.budget_ms:
            self.check()


class DiagnosticQueries:
    def __init__(self, workspace: str | Path) -> None:
        self.workspace = Path(workspace).resolve()
        self.artifacts = ArtifactStore(self.workspace)
        self.cache = QueryCache(self.workspace)

    def query(
        self,
        *,
        shard: ShardData,
        config: RopeConfig,
        stage_refs: Mapping[str, ArtifactRef],
        query_type: str,
        options: Mapping[str, Any] | None = None,
        latency_budget_ms: float | None = None,
        bypass_cache: bool = False,
    ) -> dict[str, Any]:
        options = dict(options or {})
        if query_type not in {"angles", "score_summary", "weight_stats"}:
            raise RopeError(
                "E_INVALID_INPUT",
                "未知查询类型",
                {
                    "query_type": query_type,
                    "valid_types": ["angles", "score_summary", "weight_stats"],
                },
            )
        required_stage = {
            "angles": "frequency_generation",
            "score_summary": "scoring_normalization",
            "weight_stats": "scoring_normalization",
        }[query_type]
        if required_stage not in stage_refs:
            raise RopeError(
                "E_INVALID_STAGE",
                "查询所需阶段尚未完成",
                {"shard_id": shard.shard_id, "missing_stage": required_stage},
            )
        if "frequency_generation" not in stage_refs:
            raise RopeError(
                "E_INVALID_STAGE",
                "查询缺少旋转角度表指纹",
                {"shard_id": shard.shard_id, "missing_stage": "frequency_generation"},
            )

        query_id = f"qry-{uuid.uuid4().hex}"
        budget = config.latency_budget_ms if latency_budget_ms is None else latency_budget_ms
        if isinstance(budget, bool) or not isinstance(budget, (int, float)) or budget < 0:
            raise RopeError(
                "E_INVALID_CONFIG",
                "查询延迟预算必须是非负数值",
                {"latency_budget_ms": budget},
            )
        deadline = Deadline(float(budget), query_id)
        deadline.check()

        angle_ref = stage_refs["frequency_generation"]
        components = self.cache.key_components(
            shard_fingerprint=shard.content_fingerprint,
            config_version=config.version,
            angle_table_fingerprint=angle_ref.artifact_digest,
            query_type=query_type,
            options=options,
        )
        cache_key = self.cache.key(components)
        payload_bytes: bytes | None = None
        cache_hit = False
        if not bypass_cache:
            payload_bytes = self.cache.get(cache_key)
            deadline.check()
            cache_hit = payload_bytes is not None

        if payload_bytes is None:
            payload = self._build_payload(
                shard=shard,
                config=config,
                stage_refs=stage_refs,
                query_type=query_type,
                options=options,
                deadline=deadline,
            )
            payload_bytes = canonical_json_bytes(payload)
            deadline.finish()
            if not bypass_cache:
                self.cache.put(cache_key, payload_bytes)

        payload_digest = sha256_bytes(payload_bytes)
        import json

        payload = json.loads(payload_bytes.decode("utf-8"))
        deadline.finish()
        return {
            "query_id": query_id,
            "query_type": query_type,
            "shard_id": shard.shard_id,
            "cache_key": cache_key,
            "cache_components": components,
            "cache_hit": cache_hit,
            "bypass_cache": bool(bypass_cache),
            "elapsed_ms": round(deadline.elapsed_ms(), 6),
            "latency_budget_ms": float(budget),
            "payload_digest": payload_digest,
            "payload": payload,
        }

    def _build_payload(
        self,
        *,
        shard: ShardData,
        config: RopeConfig,
        stage_refs: Mapping[str, ArtifactRef],
        query_type: str,
        options: Mapping[str, Any],
        deadline: Deadline,
    ) -> dict[str, Any]:
        if query_type == "angles":
            return self._angles_payload(shard, config, stage_refs, options, deadline)
        if query_type == "score_summary":
            return self._score_payload(shard, config, stage_refs, options, deadline)
        return self._weight_payload(shard, config, stage_refs, options, deadline)

    def _angles_payload(
        self,
        shard: ShardData,
        config: RopeConfig,
        stage_refs: Mapping[str, ArtifactRef],
        options: Mapping[str, Any],
        deadline: Deadline,
    ) -> dict[str, Any]:
        frequency_ref = stage_refs["frequency_generation"]
        arrays = self.artifacts.load(frequency_ref)
        metadata = self.artifacts.metadata(frequency_ref)
        angle_table = arrays["angle_table"]
        positions = _selected_indexes(
            options.get("position"), shard.sequence_length, "position"
        )
        channels = _selected_indexes(options.get("channel"), shard.head_dim, "channel")
        rows = []
        for position in positions:
            deadline.check()
            rows.append(
                {
                    "position": int(position),
                    "angles": [float(angle_table[position, channel]) for channel in channels],
                }
            )
        return {
            "query_type": "angles",
            "shard_id": shard.shard_id,
            "shape": [int(shard.sequence_length), int(shard.head_dim)],
            "positions": [int(value) for value in positions],
            "channels": [int(value) for value in channels],
            "rows": rows,
            "unrotated_channels": [int(value) for value in metadata.get("unrotated_channels", [])],
            "rotation_block_size": int(config.rotation_block_size),
            "scaling_mode": config.scaling_mode,
            "scaling_factor": float(config.scaling_factor),
            "inverse_frequencies": [float(value) for value in arrays["inverse_frequencies"]],
            "angle_table_fingerprint": frequency_ref.artifact_digest,
        }

    def _score_payload(
        self,
        shard: ShardData,
        config: RopeConfig,
        stage_refs: Mapping[str, ArtifactRef],
        options: Mapping[str, Any],
        deadline: Deadline,
    ) -> dict[str, Any]:
        arrays = self.artifacts.load(stage_refs["scoring_normalization"])
        scores = arrays["scores"]
        deadline.check()
        finite = scores[np.isfinite(scores)]
        batch = _index_option(options.get("batch"), shard.batch, "batch", default=0)
        head = _index_option(options.get("head"), shard.heads, "head", default=0)
        position = _index_option(
            options.get("query_position"),
            shard.sequence_length,
            "query_position",
            default=shard.sequence_length - 1,
        )
        top_k = _positive_int(options.get("top_k", 8), "top_k")
        row = scores[batch, head, position, : shard.sequence_length]
        finite_positions = np.flatnonzero(np.isfinite(row))
        ordered = sorted(
            finite_positions.tolist(), key=lambda index: (-float(row[index]), index)
        )[:top_k]
        detail_scores = [
            float(value) if math.isfinite(float(value)) else None for value in row
        ]
        return {
            "query_type": "score_summary",
            "shard_id": shard.shard_id,
            "shape": [int(value) for value in scores.shape],
            "global": {
                "finite_count": int(finite.size),
                "min": float(np.min(finite)),
                "max": float(np.max(finite)),
                "mean": float(np.mean(finite)),
            },
            "detail": {
                "batch": batch,
                "head": head,
                "query_position": position,
                "causal_mask": config.causal_mask,
                "float_precision": 64,
                "scores": detail_scores,
                "top_k": [
                    {"key_position": int(index), "score": float(row[index])}
                    for index in ordered
                ],
            },
        }

    def _weight_payload(
        self,
        shard: ShardData,
        config: RopeConfig,
        stage_refs: Mapping[str, ArtifactRef],
        options: Mapping[str, Any],
        deadline: Deadline,
    ) -> dict[str, Any]:
        arrays = self.artifacts.load(stage_refs["scoring_normalization"])
        weights = arrays["attention_weights"]
        norm = arrays["normalization_factors"]
        deadline.check()
        batch = _index_option(options.get("batch"), shard.batch, "batch", default=0)
        head = _index_option(options.get("head"), shard.heads, "head", default=0)
        position = _index_option(
            options.get("query_position"),
            shard.sequence_length,
            "query_position",
            default=shard.sequence_length - 1,
        )
        top_k = _positive_int(options.get("top_k", 8), "top_k")
        row = weights[batch, head, position, : shard.sequence_length]
        ordered = sorted(
            range(shard.sequence_length), key=lambda index: (-float(row[index]), index)
        )[:top_k]
        row_sums = np.sum(weights, axis=-1, dtype=np.float64)
        deadline.check()
        positive = weights > 0.0
        log_weights = np.zeros_like(weights)
        log_weights[positive] = np.log(weights[positive])
        entropy = -np.sum(weights * log_weights, axis=-1, dtype=np.float64)
        row_sum_error = float(np.max(np.abs(row_sums - 1.0)))
        return {
            "query_type": "weight_stats",
            "shard_id": shard.shard_id,
            "shape": [int(value) for value in weights.shape],
            "global": {
                "min": float(np.min(weights)),
                "max": float(np.max(weights)),
                "mean": float(np.mean(weights)),
                "row_sum_max_error": row_sum_error,
                "within_numeric_tolerance": bool(
                    row_sum_error <= config.numeric_tolerance
                ),
                "entropy_min": float(np.min(entropy)),
                "entropy_max": float(np.max(entropy)),
                "entropy_mean": float(np.mean(entropy)),
                "normalization_min": float(np.min(norm)),
                "normalization_max": float(np.max(norm)),
                "normalization_mean": float(np.mean(norm)),
            },
            "detail": {
                "batch": batch,
                "head": head,
                "query_position": position,
                "row_sum": float(row_sums[batch, head, position]),
                "normalization_factor": float(norm[batch, head, position]),
                "weights": [float(value) for value in row],
                "top_k": [
                    {"key_position": int(index), "weight": float(row[index])}
                    for index in ordered
                ],
            },
        }


def _selected_indexes(value: Any, size: int, name: str) -> list[int]:
    if value is None:
        return list(range(size))
    if isinstance(value, int) and not isinstance(value, bool):
        values = [value]
    elif isinstance(value, list) and all(
        isinstance(item, int) and not isinstance(item, bool) for item in value
    ):
        values = value
    else:
        raise RopeError(
            "E_INVALID_INPUT",
            f"{name} 必须是整数或整数数组",
            {"field": name, "value": value},
        )
    for item in values:
        if item < 0 or item >= size:
            raise RopeError(
                "E_INVALID_INPUT",
                f"{name} 超出范围",
                {"field": name, "value": item, "size": size},
            )
    return values


def _index_option(value: Any, size: int, name: str, *, default: int) -> int:
    if value is None:
        return default
    if not isinstance(value, int) or isinstance(value, bool):
        raise RopeError(
            "E_INVALID_INPUT", f"{name} 必须是整数", {"field": name, "value": value}
        )
    if value < 0 or value >= size:
        raise RopeError(
            "E_INVALID_INPUT",
            f"{name} 超出范围",
            {"field": name, "value": value, "size": size},
        )
    return value


def _positive_int(value: Any, name: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
        raise RopeError(
            "E_INVALID_INPUT", f"{name} 必须是正整数", {"field": name, "value": value}
        )
    return value

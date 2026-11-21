"""交互查询：延迟预算与缓存一致性。

* 每次查询携带可配置预算（秒）与查询标识；处理前与各检查点之间比对
  已耗时，超预算立即返回带 ``query_id`` 与 ``elapsed_ms`` 的
  E_LATENCY_BUDGET，而不是阻塞等待；
* 缓存键 = 样本指纹 + 位姿配置版本 + 阶段产物摘要 + 查询类型/参数，
  阶段指纹变化后键自然改变，旧缓存不可能被命中；
* 缓存值直接保存冷计算的规范化响应字节，命中即原样返回，保证与冷
  计算逐字节一致。
"""

from __future__ import annotations

import os
import time

from .canonical import atomic_write, dumps, file_lock, fingerprint, read_json
from .errors import LatencyBudgetError, NotFoundError

QUERY_TYPES = ("cloud_summary", "rejection_reasons", "error_stats", "stage_status")


class QueryService:
    def __init__(self, run_dir: str, sample_fp: str, pose_config_version: str, default_budget: float = 1.0):
        self.run_dir = run_dir
        self.sample_fp = sample_fp
        self.pose_config_version = pose_config_version
        self.default_budget = default_budget
        self.cache_dir = os.path.join(run_dir, "cache")
        self.lock_path = os.path.join(self.cache_dir, "cache.lock")
        os.makedirs(self.cache_dir, exist_ok=True)
        # 阶段产物摘要注册表，由管线在发布阶段产物时更新
        self.stage_digests: dict[str, str] = {}

    def _cache_key(self, query_type: str, params: dict) -> str:
        return fingerprint(
            {
                "sample_fingerprint": self.sample_fp,
                "pose_config_version": self.pose_config_version,
                "stage_digests": dict(sorted(self.stage_digests.items())),
                "query_type": query_type,
                "params": params,
            }
        )

    def _cache_path(self, key: str) -> str:
        return os.path.join(self.cache_dir, f"{key}.json")

    def _check_budget(self, started: float, budget: float, query_id: str) -> None:
        elapsed_ms = (time.perf_counter() - started) * 1000.0
        if elapsed_ms > budget * 1000.0:
            raise LatencyBudgetError(
                "query exceeded latency budget",
                query_id=query_id,
                elapsed_ms=round(elapsed_ms, 3),
                budget_ms=round(budget * 1000.0, 3),
            )

    def query(
        self,
        query_type: str,
        params: dict | None = None,
        budget: float | None = None,
        query_id: str | None = None,
        force_cold: bool = False,
    ) -> dict:
        if query_type not in QUERY_TYPES:
            raise NotFoundError("unknown query type", query_type=query_type)
        params = params or {}
        budget = self.default_budget if budget is None else budget
        query_id = query_id or f"q-{query_type}-{abs(hash((query_type, fingerprint(params)))) & 0xffff:04x}"
        started = time.perf_counter()
        key = self._cache_key(query_type, params)
        path = self._cache_path(key)

        if not force_cold and os.path.exists(path):
            self._check_budget(started, budget, query_id)
            cached = read_json(path)
            cached["cache"] = {"hit": True, "key": key}
            cached["query_id"] = query_id
            return cached

        self._check_budget(started, budget, query_id)
        result = self._compute(query_type, params)
        self._check_budget(started, budget, query_id)
        result["query_id"] = query_id
        result["cache"] = {"hit": False, "key": key}
        with file_lock(self.lock_path):
            atomic_write(path, dumps(result))
        self._check_budget(started, budget, query_id)
        return result

    def _compute(self, query_type: str, params: dict) -> dict:
        store_root = os.path.join(self.run_dir, "artifacts")
        if query_type == "cloud_summary":
            geometry = read_json(os.path.join(store_root, "geometry", "global.json"))
            return {
                "query_type": query_type,
                "point_count": geometry["counts"]["after_downsample"],
                "counts": geometry["counts"],
                "duplicate_voxel_keys": geometry["duplicate_voxel_keys"],
            }
        if query_type == "rejection_reasons":
            geometry = read_json(os.path.join(store_root, "geometry", "global.json"))
            return {
                "query_type": query_type,
                "rejected": geometry["rejected"],
                "merge_log": geometry["merge_log"],
            }
        if query_type == "error_stats":
            triangulation = read_json(os.path.join(store_root, "triangulation", "global.json"))
            errors = [track["max_reprojection_error"] for track in triangulation["tracks"]]
            return {
                "query_type": query_type,
                "track_count": len(errors),
                "max_error": max(errors) if errors else None,
                "mean_error": round_float(sum(errors) / len(errors)) if errors else None,
                "errors": errors,
            }
        status = read_json(os.path.join(self.run_dir, "checkpoints", "status.json"))
        return {"query_type": query_type, "stages": status}


def round_float(value, digits=10):
    return float(f"{value:.{digits}g}")

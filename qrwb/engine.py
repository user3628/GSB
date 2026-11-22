"""单查询四阶段处理引擎：派发 → 候选 → 编排裁决 → 验证汇总。

引擎只负责计算与通过 CheckpointStore 原子发布；预算预占、波次调度、
停止控制由 :mod:`qrwb.pipeline` 统一编排。
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional

from .adjudication import adjudicate, score_candidates
from .candidates import generate_candidates
from .checkpoints import CheckpointStore
from .config import Config
from .errors import E_WORKER_EXCEPTION, make_error
from .normalize import build_dispatch_plan
from .util import fingerprint


@dataclass
class VersionBundle:
    input_fingerprint: str
    config_version: str
    synonym_fingerprint: str
    glyph_fingerprint: str
    bridge_fingerprint: str
    routes_fingerprint: str
    budget_fingerprint: str
    adjudication_fingerprint: str
    normalization_fingerprint: str

    def as_dict(self) -> Dict[str, str]:
        return {
            "input_fingerprint": self.input_fingerprint,
            "config_version": self.config_version,
            "synonym_fingerprint": self.synonym_fingerprint,
            "glyph_fingerprint": self.glyph_fingerprint,
            "bridge_fingerprint": self.bridge_fingerprint,
            "routes_fingerprint": self.routes_fingerprint,
            "budget_fingerprint": self.budget_fingerprint,
            "adjudication_fingerprint": self.adjudication_fingerprint,
            "normalization_fingerprint": self.normalization_fingerprint,
        }


def build_version_bundle(record: Dict[str, Any], cfg: Config,
                         resources: Dict[str, Any]) -> VersionBundle:
    return VersionBundle(
        input_fingerprint=fingerprint(record),
        config_version=cfg.config_version,
        synonym_fingerprint=resources["fingerprints"]["synonym"],
        glyph_fingerprint=resources["fingerprints"]["glyph"],
        bridge_fingerprint=resources["fingerprints"]["bridge"],
        routes_fingerprint=fingerprint(cfg.route_cfg),
        budget_fingerprint=fingerprint(cfg.budget),
        adjudication_fingerprint=fingerprint(cfg.adjudication),
        normalization_fingerprint=fingerprint({
            "version": cfg.raw.get("version"),
            "simulation": cfg.raw.get("simulation", {}),
        }),
    )


class QueryEngine:
    def __init__(self, cfg: Config, resources: Dict[str, Any],
                 store: CheckpointStore) -> None:
        self.cfg = cfg
        self.resources = resources
        self.store = store

    def _stage_fingerprints(self, bundle: VersionBundle, stage: str
                            ) -> Dict[str, str]:
        data = bundle.as_dict()
        if stage == "dispatch":
            keys = ("input_fingerprint", "normalization_fingerprint")
        elif stage == "candidate":
            keys = ("input_fingerprint", "normalization_fingerprint",
                    "synonym_fingerprint", "glyph_fingerprint",
                    "bridge_fingerprint", "routes_fingerprint")
        elif stage == "orchestration":
            keys = ("input_fingerprint", "budget_fingerprint")
        else:
            keys = ("input_fingerprint", "adjudication_fingerprint")
        return {key: data[key] for key in keys}

    def ensure_dispatch(self, record: Dict[str, Any],
                        bundle: VersionBundle) -> Dict[str, Any]:
        existing = self.store.get(record["query_id"], "dispatch")
        if existing is not None:
            return existing["product"]
        plan = build_dispatch_plan(record, self.cfg)
        self.store.publish(
            record["query_id"], "dispatch",
            input_fingerprint=bundle.input_fingerprint,
            config_version=bundle.config_version,
            product=plan,
            stage_fingerprints=self._stage_fingerprints(bundle, "dispatch"))
        return plan

    def ensure_candidates(self, record: Dict[str, Any], plan: Dict[str, Any],
                          bundle: VersionBundle) -> Optional[Dict[str, Any]]:
        if plan["status"] == "error":
            existing = self.store.get(record["query_id"], "candidate")
            if existing is not None:
                return existing["product"]
            # 空查询无候选可生成，但仍写阶段标记，保证四阶段序号连续
            marker = {
                "query_id": record["query_id"],
                "status": "skipped_due_to_dispatch_error",
                "candidates": [],
                "route_counts": {"synonym": 0, "glyph": 0, "bridge": 0},
                "candidates_fingerprint": fingerprint(
                    {"skipped": record["query_id"]}),
            }
            self.store.publish(
                record["query_id"], "candidate",
                input_fingerprint=bundle.input_fingerprint,
                config_version=bundle.config_version,
                product=marker,
                stage_fingerprints=self._stage_fingerprints(
                    bundle, "candidate"))
            return marker
        existing = self.store.get(record["query_id"], "candidate")
        if existing is not None:
            return existing["product"]
        bundle_dict = bundle.as_dict()
        candidate_bundle = generate_candidates(plan, self.cfg, self.resources)
        self.store.publish(
            record["query_id"], "candidate",
            input_fingerprint=bundle.input_fingerprint,
            config_version=bundle.config_version,
            product=candidate_bundle,
            stage_fingerprints=self._stage_fingerprints(bundle, "candidate"),
            expect_fingerprints={
                "synonym_fingerprint": bundle_dict["synonym_fingerprint"],
                "bridge_fingerprint": bundle_dict["bridge_fingerprint"]})
        return candidate_bundle

    def ensure_orchestration(self, record: Dict[str, Any],
                             plan: Dict[str, Any],
                             candidate_bundle: Optional[Dict[str, Any]],
                             bundle: VersionBundle,
                             orchestration_product: Dict[str, Any]
                             ) -> Dict[str, Any]:
        existing = self.store.get(record["query_id"], "orchestration")
        if existing is not None:
            return existing["product"]
        self.store.publish(
            record["query_id"], "orchestration",
            input_fingerprint=bundle.input_fingerprint,
            config_version=bundle.config_version,
            product=orchestration_product,
            stage_fingerprints=self._stage_fingerprints(
                bundle, "orchestration"))
        return orchestration_product

    def ensure_validation(self, record: Dict[str, Any], plan: Dict[str, Any],
                          candidate_bundle: Optional[Dict[str, Any]],
                          orchestration: Dict[str, Any],
                          bundle: VersionBundle) -> Dict[str, Any]:
        existing = self.store.get(record["query_id"], "validation")
        if existing is not None:
            return existing["product"]
        if plan["status"] == "error" or \
                orchestration["status"] != "orchestrated":
            product = {
                "query_id": record["query_id"],
                "status": "skipped",
                "reason": orchestration.get("status"),
                "error": orchestration.get("error"),
                "adopted_queries": [],
                "scored_candidates": [],
                "adoptions": [],
                "rejected": [],
                "validation_fingerprint": fingerprint(
                    {"skipped": record["query_id"],
                     "reason": orchestration.get("status")}),
            }
        else:
            scored = score_candidates(candidate_bundle, plan, self.cfg)
            decision = adjudicate(candidate_bundle, plan, self.cfg)
            product = {
                "query_id": record["query_id"],
                "status": "validated",
                "scored_candidates": scored,
                **decision,
            }
        self.store.publish(
            record["query_id"], "validation",
            input_fingerprint=bundle.input_fingerprint,
            config_version=bundle.config_version,
            product=product,
            stage_fingerprints=self._stage_fingerprints(
                bundle, "validation"))
        return product


def structured_worker_error(query_id: str, stage: str,
                            exc: BaseException) -> Dict[str, Any]:
    """工作线程异常 → 结构化错误对象（保留阶段，不伪装成线程超时）。"""
    return make_error(
        E_WORKER_EXCEPTION,
        f"{type(exc).__name__}: {exc}",
        query_id=query_id, stage=stage,
        exception_type=type(exc).__name__).to_dict()

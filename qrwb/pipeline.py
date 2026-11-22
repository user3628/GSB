"""全量流水线：导入、版本失效、预算编排、并行/串行确定性归约与恢复。"""

from __future__ import annotations

import json
import os
import threading
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Dict, List, Optional

from .adjudication import build_summary
from .checkpoints import CheckpointStore
from .config import Config
from .engine import (QueryEngine, VersionBundle, build_version_bundle,
                     structured_worker_error)
from .errors import E_LATENCY_BUDGET
from .scheduler import (Admission, BudgetLedger, LatencyGuard, admit_queries,
                        build_waves, estimate_cost)
from .util import (atomic_write_json, fingerprint, full_fingerprint)

class Pipeline:
    def __init__(self, workspace: str, cfg: Config,
                 resources: Dict[str, Any],
                 records: List[Dict[str, Any]],
                 manifest: Dict[str, Any]) -> None:
        self.workspace = workspace
        self.cfg = cfg
        self.resources = resources
        self.records = records
        self.manifest = manifest
        self.store = CheckpointStore(workspace)
        self.budget_fp = fingerprint(cfg.budget)
        self.ledger = BudgetLedger(workspace, cfg.budget, self.budget_fp)
        sim = cfg.simulation
        self.latency = LatencyGuard(
            cfg.budget["per_request_latency_ms"],
            sim.get("work_ms", 1), sim.get("jitter_ms", 2),
            sim.get("seed", 77))
        self._stop = threading.Event()
        self._stop_lock_path = os.path.join(workspace, "locks", "stop.lock")
        self._stop_flag_path = os.path.join(workspace, "state", "stop.flag")
        if os.path.exists(self._stop_flag_path):
            self._stop.set()

    # ---- 停止控制 -------------------------------------------------------
    def request_stop(self) -> None:
        from .util import FileLock
        with FileLock(self._stop_lock_path):
            atomic_write_json(self._stop_flag_path,
                              {"stop_requested": True})
        self._stop.set()

    def clear_stop(self) -> None:
        from .util import FileLock
        with FileLock(self._stop_lock_path):
            if os.path.exists(self._stop_flag_path):
                os.remove(self._stop_flag_path)
        self._stop.clear()

    @property
    def stop_requested(self) -> bool:
        return self._stop.is_set()

    # ---- 版本失效 -------------------------------------------------------
    def _invalidate_if_needed(self, record: Dict[str, Any],
                              bundle: VersionBundle) -> Dict[str, Any]:
        if self.store.latest_seq(record["query_id"]) == 0:
            return {"query_id": record["query_id"], "reused": False,
                    "new": True}
        invalid = self.store.first_invalid_stage(
            record["query_id"], bundle.as_dict())
        if invalid is None:
            return {"query_id": record["query_id"], "reused": True}
        stage, reason = invalid
        removed = self.store.invalidate_from(
            record["query_id"], stage,
            {"expected_fingerprints": reason,
             "version_bundle": bundle.as_dict()})
        return {"query_id": record["query_id"], "reused": False,
                "invalidated_from": stage, "removed_stages": removed,
                "reason": reason}

    # ---- 单查询完整处理（工作线程入口） ---------------------------------
    def _process_query(self, record: Dict[str, Any]) -> Dict[str, Any]:
        query_id = record["query_id"]
        engine = QueryEngine(self.cfg, self.resources, self.store)
        bundle = build_version_bundle(record, self.cfg, self.resources)
        try:
            invalidation = self._invalidate_if_needed(record, bundle)
            plan = engine.ensure_dispatch(record, bundle)

            candidate_bundle = engine.ensure_candidates(record, plan, bundle)
            if plan["status"] == "error":
                orchestration = {
                    "query_id": query_id,
                    "status": "rejected",
                    "error": plan["error"],
                    "estimated_cost": 0,
                    "elapsed_ms": 0.0,
                    "orchestration_fingerprint": "",
                }
                engine.ensure_orchestration(
                    record, plan, candidate_bundle, bundle, orchestration)
                validation = engine.ensure_validation(
                    record, plan, candidate_bundle, orchestration, bundle)
                return {"query_id": query_id, "plan": plan,
                        "candidate_bundle": candidate_bundle,
                        "orchestration": orchestration,
                        "validation": validation,
                        "invalidation": invalidation}

            return {"query_id": query_id, "plan": plan,
                    "candidate_bundle": candidate_bundle,
                    "invalidation": invalidation,
                    "_need_orchestration": True}
        except Exception as exc:  # 结构化记录，保留已完成阶段产物
            error = structured_worker_error(query_id, "worker", exc)
            return {"query_id": query_id, "fatal_error": error}

    # ---- 主执行入口 -----------------------------------------------------
    def run(self, parallel: bool = True,
            stop_after: Optional[int] = None) -> Dict[str, Any]:
        """执行流水线。

        stop_after: 处理完指定数量查询单元后触发优雅停止（用于确定性
        演示与测试）；运行中收到外部 stop 请求同样生效。
        """
        plans: Dict[str, Dict[str, Any]] = {}
        candidate_bundles: Dict[str, Any] = {}
        fatal_errors: List[Dict[str, Any]] = []
        invalidations: List[Dict[str, Any]] = []
        unprocessed: List[Dict[str, Any]] = []
        stop_happened = False

        # 阶段 1+2：逐查询派发与候选生成（检查点复用/失效重建）
        processed_units = 0
        for record in sorted(self.records, key=lambda r: r["query_id"]):
            if self._stop.is_set() or (stop_after is not None
                                       and processed_units >= stop_after):
                stop_happened = True
                break
            outcome = self._process_query(record)
            if outcome.get("fatal_error"):
                fatal_errors.append(outcome["fatal_error"])
                continue
            processed_units += 1
            invalidations.append(outcome["invalidation"])
            plans[outcome["query_id"]] = outcome["plan"]
            if outcome.get("candidate_bundle") is not None:
                candidate_bundles[outcome["query_id"]] = \
                    outcome["candidate_bundle"]

        # 优雅停止时，尚未进入派发循环的查询显式登记为未处理
        if stop_happened:
            for record in sorted(self.records, key=lambda r: r["query_id"]):
                if record["query_id"] not in plans:
                    unprocessed.append({
                        "query_id": record["query_id"],
                        "reason": "STOP_REQUESTED",
                        "detail": {"phase": "dispatch_candidate"}})

        atomic_write_json(os.path.join(self.workspace, "state", "plans.json"),
                          plans)
        atomic_write_json(os.path.join(self.workspace, "state",
                                       "candidates.json"),
                          candidate_bundles)

        # 阶段 3：预算编排（恢复时先清空瞬时预占，已提交查询跳过重接纳）
        self.ledger.reset_consumption()
        already_committed = {
            qid for qid in plans
            if (cp := self.store.get(qid, "orchestration")) is not None
            and cp["product"]["status"] == "orchestrated"}
        ordered_plans = [plans[qid] for qid in sorted(plans)]
        admissions = admit_queries(ordered_plans, self.ledger, self.manifest,
                                   already_committed=already_committed)
        waves = build_waves(admissions,
                            int(self.cfg.budget["max_concurrent_channels"]))

        orchestrations: Dict[str, Dict[str, Any]] = {}
        for admission in admissions:
            if not admission.accepted:
                orchestrations[admission.query_id] = {
                    "query_id": admission.query_id,
                    "status": "unprocessed",
                    "error": admission.error,
                    "estimated_cost": admission.estimated_cost,
                    "elapsed_ms": 0.0,
                    "orchestration_fingerprint": "",
                }
                unprocessed.append({
                    "query_id": admission.query_id,
                    "reason": admission.error["code"],
                    "detail": admission.error["context"]})

        def run_one(admission: Admission) -> Dict[str, Any]:
            query_id = admission.query_id
            plan = plans[query_id]
            existing_cp = self.store.get(query_id, "orchestration")
            if existing_cp is not None and                     existing_cp["product"]["status"] == "orchestrated":
                self.ledger.commit(query_id)
                return existing_cp["product"]
            record = next(r for r in self.records if r["query_id"] == query_id)
            bundle = build_version_bundle(record, self.cfg, self.resources)
            engine = QueryEngine(self.cfg, self.resources, self.store)
            existing = self.store.get(query_id, "orchestration")
            if existing is not None:
                return existing["product"]
            latency = self.latency.simulate(query_id)
            if not latency["ok"]:
                self.ledger.release(query_id)
                product = {
                    "query_id": query_id,
                    "status": "latency_aborted",
                    "error": latency["error"],
                    "estimated_cost": admission.estimated_cost,
                    "elapsed_ms": latency["elapsed_ms"],
                    "orchestration_fingerprint": "",
                }
                engine.ensure_orchestration(record, plan,
                                            candidate_bundles.get(query_id),
                                            bundle, product)
                unprocessed.append({
                    "query_id": query_id,
                    "reason": E_LATENCY_BUDGET,
                    "detail": latency["error"]["context"]})
                return product
            product = {
                "query_id": query_id,
                "status": "orchestrated",
                "channel": admission.channel,
                "estimated_cost": admission.estimated_cost,
                "elapsed_ms": latency["elapsed_ms"],
                "wave_routes": sorted(candidate_bundles[query_id]["route_counts"]),
                "orchestration_fingerprint": fingerprint({
                    "query_id": query_id,
                    "route_counts": candidate_bundles[query_id]["route_counts"],
                    "estimated_cost": admission.estimated_cost}),
            }
            engine.ensure_orchestration(record, plan,
                                        candidate_bundles[query_id],
                                        bundle, product)
            self.ledger.commit(query_id)
            return product

        for wave in waves:
            if self._stop.is_set():
                stop_happened = True
                break
            if parallel:
                with ThreadPoolExecutor(max_workers=max(len(wave), 1)) as pool:
                    products = list(pool.map(run_one, wave))
            else:
                products = [run_one(admission) for admission in wave]
            for product in products:
                orchestrations[product["query_id"]] = product

        atomic_write_json(os.path.join(self.workspace, "state",
                                       "orchestration.json"), orchestrations)

        # 停止时尚未编排的查询：显式标记（恢复时重新编排）
        for query_id in sorted(plans):
            if query_id not in orchestrations:
                orchestrations[query_id] = {
                    "query_id": query_id,
                    "status": "stopped_pending",
                    "estimated_cost": estimate_cost(
                        plans[query_id], self.manifest["total_rows"]),
                    "elapsed_ms": 0.0,
                    "orchestration_fingerprint": "",
                }
                unprocessed.append({
                    "query_id": query_id,
                    "reason": "STOP_REQUESTED",
                    "detail": {}})

        # 阶段 4：裁决与验证汇总（跳过未成功编排的查询）
        validations: Dict[str, Dict[str, Any]] = {}
        for query_id in sorted(plans):
            if self._stop.is_set() and \
                    orchestrations[query_id]["status"] == "stopped_pending":
                stop_happened = True
                continue
            record = next(r for r in self.records
                          if r["query_id"] == query_id)
            bundle = build_version_bundle(record, self.cfg, self.resources)
            engine = QueryEngine(self.cfg, self.resources, self.store)
            validation = engine.ensure_validation(
                record, plans[query_id], candidate_bundles.get(query_id),
                orchestrations[query_id], bundle)
            validations[query_id] = validation

        validation_list = [validations[qid] for qid in sorted(validations)]
        summary = build_summary(validation_list, plans)
        atomic_write_json(os.path.join(self.workspace, "state",
                                       "validation.json"), validations)
        atomic_write_json(os.path.join(self.workspace, "state",
                                       "summary.json"), summary)

        report = {
            "status": "stopped" if stop_happened else "completed",
            "parallel": parallel,
            "total_records": len(self.records),
            "dispatched": len(plans),
            "orchestrated": sum(1 for o in orchestrations.values()
                                 if o["status"] == "orchestrated"),
            "validated": len(validations),
            "unprocessed": unprocessed,
            "fatal_errors": fatal_errors,
            "invalidations": invalidations,
            "budget": self.ledger.snapshot(),
            "waves": [[a.query_id for a in wave] for wave in waves],
            "summary_fingerprint": summary["summary_fingerprint"],
            "run_fingerprint": self.run_fingerprint(
                plans, candidate_bundles, orchestrations, validations),
        }
        atomic_write_json(os.path.join(self.workspace, "state",
                                       "run_report.json"), report)
        return report

    # ---- 确定性归约指纹 -------------------------------------------------
    @staticmethod
    def run_fingerprint(plans: Dict[str, Any], candidates: Dict[str, Any],
                        orchestrations: Dict[str, Any],
                        validations: Dict[str, Any]) -> str:
        """跨并行/串行模式一致的整轮归约指纹（按 query_id 与路径排序）。"""
        reduced = {
            "plans": [plans[qid] for qid in sorted(plans)],
            "candidates": [candidates[qid] for qid in sorted(candidates)],
            "orchestrations": [orchestrations[qid]
                               for qid in sorted(orchestrations)],
            "validations": [validations[qid] for qid in sorted(validations)],
        }
        return full_fingerprint(reduced)

    # ---- 查询接口数据 ---------------------------------------------------
    def query_status(self, query_id: str) -> Dict[str, Any]:
        record = next((r for r in self.records
                       if r["query_id"] == query_id), None)
        out: Dict[str, Any] = {"query_id": query_id, "found": record is not None}
        if record is None:
            return out
        bundle = build_version_bundle(record, self.cfg, self.resources)
        out["input_fingerprint"] = bundle.input_fingerprint
        out["config_version"] = bundle.config_version
        out["checkpoints"] = self.store.stage_versions(query_id)
        invalid = self.store.first_invalid_stage(query_id, bundle.as_dict())
        out["version_state"] = ("valid" if invalid is None
                                else {"invalidated_from": invalid[0],
                                      "reason": invalid[1]})
        for key, filename in (("plan", "plans.json"),
                              ("candidate", "candidates.json"),
                              ("orchestration", "orchestration.json"),
                              ("validation", "validation.json")):
            path = os.path.join(self.workspace, "state", filename)
            if os.path.exists(path):
                with open(path, "r", encoding="utf-8") as handle:
                    data = json.load(handle)
                if query_id in data:
                    out[key] = data[query_id]
        return out

    def adjudication_trace(self, query_id: str) -> Dict[str, Any]:
        path = os.path.join(self.workspace, "state", "validation.json")
        if not os.path.exists(path):
            return {"query_id": query_id, "trace": None}
        with open(path, "r", encoding="utf-8") as handle:
            data = json.load(handle)
        item = data.get(query_id)
        if item is None:
            return {"query_id": query_id, "trace": None}
        return {
            "query_id": query_id,
            "adoptions": item.get("adoptions", []),
            "rejected": item.get("rejected", []),
            "scored_candidates": item.get("scored_candidates", []),
            "validation_fingerprint": item.get("validation_fingerprint"),
        }

    def budget_view(self) -> Dict[str, Any]:
        return self.ledger.snapshot()

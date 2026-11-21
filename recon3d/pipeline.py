"""管线编排：四阶段执行、优雅停止、检查点恢复与增量一致性。

执行单元为“分片 × 阶段”的不可拆分单元；收到停止请求时，完成当前单元
后在阶段边界退出。恢复时依据检查点 journal 的连续序号前缀，从最近一致
检查点继续。增量模式下，对比各阶段输入指纹，定位最早变化阶段，令其与
全部下游失效并重建，未变化上游直接复用既有产物。
"""

from __future__ import annotations

import os

import numpy as np

from . import (
    GLOBAL_SCOPE,
    PIPELINE_STAGES,
    STAGE_FEATURES,
    STAGE_GEOMETRY,
    STAGE_PACKAGING,
    STAGE_TRIANGULATION,
)
from .canonical import dumps, fingerprint, fingerprint_bytes, read_json, read_png, write_json
from .checkpoint import CheckpointStore
from .errors import IncrementalMismatchError, InvalidStageError, StoppedError
from .features import extract_features
from .fingerprint import (
    features_input_fingerprint,
    geometry_input_fingerprint,
    packaging_input_fingerprint,
    sample_fingerprint,
    triangulation_input_fingerprint,
)
from .geometry import cull_and_downsample
from .packaging import validate_and_package
from .triangulation import triangulate_tracks

STOP_FILENAME = "STOP"


class Pipeline:
    def __init__(self, sample_dir: str, run_dir: str):
        self.sample_dir = sample_dir
        self.run_dir = run_dir
        self.store = CheckpointStore(run_dir)
        self.manifest_path = os.path.join(sample_dir, "manifest.json")
        self.manifest = read_json(self.manifest_path)
        self.sample_fp = sample_fingerprint(self.manifest, sample_dir)
        self.stop_path = os.path.join(run_dir, STOP_FILENAME)

    # ------------------------------------------------------------------ #
    def request_stop(self) -> None:
        os.makedirs(self.run_dir, exist_ok=True)
        with open(self.stop_path, "wb") as handle:
            handle.write(b"stop\n")

    def _stop_requested(self) -> bool:
        return os.path.exists(self.stop_path)

    def clear_stop(self) -> None:
        if os.path.exists(self.stop_path):
            os.remove(self.stop_path)

    # ------------------------------------------------------------------ #
    def _image_hash(self, tile) -> str:
        with open(os.path.join(self.sample_dir, tile["image"]), "rb") as handle:
            return fingerprint_bytes(handle.read())

    def _wide_tiles(self):
        # 三角化使用边长最大的前三个宽视角，保证多视覆盖与可分性
        ordered = sorted(self.manifest["tiles"], key=lambda t: (-t["width"], t["tile_id"]))
        wide_ids = [t["tile_id"] for t in ordered[:3]]
        return [t for t in self.manifest["tiles"] if t["tile_id"] in wide_ids]

    # ------------------------------------------------------------------ #
    def run(self, stop_after: str | None = None, force: bool = False) -> dict:
        """全量/恢复运行。``stop_after`` 为 ``stage:tile_id``，到达后优雅停止。"""
        self.clear_stop()
        if force:
            self._reset_run_dir()
        plan = self._plan(force=force)
        executed = []
        for unit in plan:
            if unit["stage"] != STAGE_FEATURES:
                self._refresh_global_unit(unit, force)
            if self._stop_requested():
                self._write_status(executed, stopped=True)
                raise StoppedError(
                    "stop requested; finished current unit before exiting",
                    stage=unit["stage"],
                    tile_id=unit["tile_id"],
                    completed=executed,
                )
            self._execute_unit(unit)
            executed.append(f"{unit['stage']}:{unit['tile_id']}")
            if stop_after and f"{unit['stage']}:{unit['tile_id']}" == stop_after:
                self.request_stop()
                self._write_status(executed, stopped=True)
                raise StoppedError(
                    "reached stop_after checkpoint",
                    stage=unit["stage"],
                    tile_id=unit["tile_id"],
                    completed=executed,
                )
        self._write_status(executed, stopped=False)
        return {"executed": executed, "sample_fingerprint": self.sample_fp}

    # ------------------------------------------------------------------ #
    def _reset_run_dir(self) -> None:
        """全量重建：清空旧 journal/产物/缓存，保证序号从 1 单调开始。"""
        import shutil

        for name in ("artifacts", "checkpoints", "cache", "exports"):
            target = os.path.join(self.run_dir, name)
            if os.path.exists(target):
                shutil.rmtree(target)
        for name in ("artifacts", "checkpoints", "cache", "exports"):
            os.makedirs(os.path.join(self.run_dir, name), exist_ok=True)
        self.store = CheckpointStore(self.run_dir)

    # ------------------------------------------------------------------ #
    def _plan(self, force: bool) -> list[dict]:
        journal = self.store.load_journal()
        done = {(r["stage"], r["tile_id"]): r for r in journal}
        plan = []
        # 阶段一：逐分片特征
        for tile in self.manifest["tiles"]:
            tid = tile["tile_id"]
            image_fp = self._image_hash(tile)
            input_fp = features_input_fingerprint(image_fp, self.manifest)
            plan.append(
                {
                    "stage": STAGE_FEATURES,
                    "tile_id": tid,
                    "input_fp": input_fp,
                    "skip": (not force) and self._reusable(STAGE_FEATURES, tid, input_fp, done),
                }
            )
        # 全局阶段的输入指纹依赖刚生成的上游产物，占位后在执行前重算，
        # 避免一次性预算时读到尚未生成的摘要（否则发布值与恢复值不一致）
        for stage in (STAGE_TRIANGULATION, STAGE_GEOMETRY, STAGE_PACKAGING):
            plan.append({"stage": stage, "tile_id": GLOBAL_SCOPE, "input_fp": None, "skip": False})
        return plan

    def _refresh_global_unit(self, unit: dict, force: bool) -> None:
        """执行全局阶段前，基于最新 journal 重算输入指纹与复用决策。"""
        done = {(r["stage"], r["tile_id"]): r for r in self.store.load_journal()}
        stage = unit["stage"]
        if stage == STAGE_TRIANGULATION:
            input_fp = triangulation_input_fingerprint(self._feature_digests(done), self.manifest)
        elif stage == STAGE_GEOMETRY:
            input_fp = geometry_input_fingerprint(
                self._stage_digest(STAGE_TRIANGULATION, GLOBAL_SCOPE, done), self.manifest
            )
        else:
            input_fp = packaging_input_fingerprint(
                self._stage_digest(STAGE_GEOMETRY, GLOBAL_SCOPE, done), self.manifest
            )
        unit["input_fp"] = input_fp
        unit["skip"] = (not force) and self._reusable(stage, GLOBAL_SCOPE, input_fp, done)

    def _reusable(self, stage: str, tile_id: str, input_fp: str, done: dict) -> bool:
        record = done.get((stage, tile_id))
        return bool(record) and record["input_fingerprint"] == input_fp and self.store.has_artifact(stage, tile_id)

    def _feature_digests(self, done: dict) -> dict:
        digests = {}
        for tile in self.manifest["tiles"]:
            record = done.get((STAGE_FEATURES, tile["tile_id"]))
            if record:
                digests[tile["tile_id"]] = record["artifact_digest"]
            elif self.store.has_artifact(STAGE_FEATURES, tile["tile_id"]):
                digests[tile["tile_id"]] = fingerprint_bytes(
                    dumps(self.store.read_artifact(STAGE_FEATURES, tile["tile_id"]))
                )
        return digests

    def _stage_digest(self, stage: str, tile_id: str, done: dict) -> str:
        record = done.get((stage, tile_id))
        if record:
            return record["artifact_digest"]
        if self.store.has_artifact(stage, tile_id):
            return fingerprint_bytes(dumps(self.store.read_artifact(stage, tile_id)))
        return "unavailable"

    # ------------------------------------------------------------------ #
    def _execute_unit(self, unit: dict) -> None:
        stage, tile_id = unit["stage"], unit["tile_id"]
        if unit["skip"] and self.store.has_artifact(stage, tile_id):
            return
        if stage == STAGE_FEATURES:
            tile = next(t for t in self.manifest["tiles"] if t["tile_id"] == tile_id)
            image = read_png(os.path.join(self.sample_dir, tile["image"]))
            payload = extract_features(image, tile_id)
        elif stage == STAGE_TRIANGULATION:
            self._require_stage(STAGE_FEATURES)
            feature_sets = {
                t["tile_id"]: self.store.read_artifact(STAGE_FEATURES, t["tile_id"])
                for t in self.manifest["tiles"]
            }
            payload = triangulate_tracks(self._wide_tiles(), feature_sets)
        elif stage == STAGE_GEOMETRY:
            self._require_stage(STAGE_TRIANGULATION)
            triangulation = self.store.read_artifact(STAGE_TRIANGULATION, GLOBAL_SCOPE)
            payload = cull_and_downsample(
                triangulation,
                self.manifest["reprojection_threshold"],
                self.manifest["min_points"],
                self.manifest["sample_density"],
            )
        elif stage == STAGE_PACKAGING:
            self._require_stage(STAGE_GEOMETRY)
            geometry = self.store.read_artifact(STAGE_GEOMETRY, GLOBAL_SCOPE)
            payload = validate_and_package(geometry, self.manifest, self.store.export_dir)
        else:
            raise InvalidStageError("unknown stage", stage=stage, tile_id=tile_id)

        digest = self.store.write_artifact(stage, tile_id, payload)
        self.store.publish(stage, tile_id, unit["input_fp"], self.manifest["config_version"], digest)

    def _require_stage(self, stage: str) -> None:
        if stage == STAGE_FEATURES:
            missing = [
                t["tile_id"]
                for t in self.manifest["tiles"]
                if not self.store.has_artifact(STAGE_FEATURES, t["tile_id"])
            ]
            if missing:
                raise InvalidStageError(
                    "missing prerequisite feature artifact",
                    stage=stage,
                    tile_id=missing[0],
                    missing_tiles=missing,
                )
            return
        tile_id = GLOBAL_SCOPE
        if not self.store.has_artifact(stage, tile_id):
            raise InvalidStageError(
                "missing prerequisite stage artifact", stage=stage, tile_id=tile_id
            )

    # ------------------------------------------------------------------ #
    def incremental_rebuild(self, changed_manifest: dict, changed_sample_dir: str, baseline_run_dir: str) -> dict:
        """在变更样本上增量重建，并与同输入的全量重建逐字节比对。"""
        # 1) 用变更样本在当前 run_dir 做增量（复用未变化上游）
        self.manifest = changed_manifest
        self.manifest_path = os.path.join(changed_sample_dir, "manifest.json")
        self.sample_dir = changed_sample_dir
        self.sample_fp = sample_fingerprint(changed_manifest, changed_sample_dir)
        incremental = self.run(force=False)

        # 2) 在独立目录对相同变更样本做从零全量重建作为对照
        fresh_dir = os.path.join(self.run_dir, "fresh_rebuild")
        fresh = Pipeline(changed_sample_dir, fresh_dir)
        fresh.run(force=True)

        # 3) 逐字节比对所有阶段产物与导出
        mismatches = []
        for stage in PIPELINE_STAGES:
            scopes = (
                [t["tile_id"] for t in changed_manifest["tiles"]]
                if stage == STAGE_FEATURES
                else [GLOBAL_SCOPE]
            )
            for scope in scopes:
                inc_bytes = dumps(self.store.read_artifact(stage, scope))
                fresh_bytes = dumps(fresh.store.read_artifact(stage, scope))
                if inc_bytes != fresh_bytes:
                    mismatches.append(f"{stage}:{scope}")
        for name in ("point_cloud.ply", "validation_summary.json"):
            inc_path = os.path.join(self.store.export_dir, name)
            fresh_path = os.path.join(fresh.store.export_dir, name)
            if os.path.exists(inc_path) and os.path.exists(fresh_path):
                with open(inc_path, "rb") as a, open(fresh_path, "rb") as b:
                    export_differs = a.read() != b.read()
                if export_differs:
                    mismatches.append(f"export:{name}")
        if mismatches:
            raise IncrementalMismatchError(
                "incremental rebuild differs from full rebuild",
                diff_count=len(mismatches),
                mismatches=mismatches,
            )
        return {"executed": incremental["executed"], "mismatches": 0, "fresh_dir": fresh_dir}

    # ------------------------------------------------------------------ #
    def stage_fingerprints(self) -> list[dict]:
        plan = self._plan(force=False)
        for unit in plan:
            if unit["stage"] != STAGE_FEATURES:
                self._refresh_global_unit(unit, force=False)
        journal = {(r["stage"], r["tile_id"]): r for r in self.store.load_journal()}
        rows = []
        for unit in plan:
            record = journal.get((unit["stage"], unit["tile_id"]))
            rows.append(
                {
                    "stage": unit["stage"],
                    "tile_id": unit["tile_id"],
                    "input_fingerprint": unit["input_fp"],
                    "artifact_digest": record["artifact_digest"] if record else None,
                    "reusable": unit["skip"],
                    "status": "cached" if unit["skip"] else "rebuild",
                }
            )
        return rows

    def invalidation_reason(self, previous_fingerprints: list[dict]) -> list[dict]:
        current = {(r["stage"], r["tile_id"]): r["input_fingerprint"] for r in self.stage_fingerprints()}
        reasons = []
        for row in previous_fingerprints:
            key = (row["stage"], row["tile_id"])
            if current.get(key) != row["input_fingerprint"]:
                reasons.append(
                    {
                        "stage": row["stage"],
                        "tile_id": row["tile_id"],
                        "reason": "input fingerprint changed",
                        "previous": row["input_fingerprint"],
                        "current": current.get(key),
                    }
                )
        return reasons

    def _write_status(self, executed: list[str], stopped: bool) -> None:
        status = {
            "sample_fingerprint": self.sample_fp,
            "config_version": self.manifest["config_version"],
            "stopped": stopped,
            "journal": self.store.stage_status(),
            "last_executed": executed,
        }
        write_json(os.path.join(self.store.checkpoint_dir, "status.json"), status)

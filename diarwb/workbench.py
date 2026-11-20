"""Application facade tying storage, pipeline and editing together."""
from __future__ import annotations

import os
import time
import uuid
from typing import Any, Callable, Dict, List, Optional

from .config import PipelineConfig, default_config
from .errors import (ELatencyBudget, EInvalidConfig, EInvalidStage,
                     ENotFound, EStaleVersion, WorkbenchError)
from .pipeline import (STAGE_RECOGNITION, STAGE_SEPARATION, Pipeline,
                       stage_fingerprint)
from .timeline import Segment, Timeline
from .workspace import (SNAPSHOT_REASON_CONFIG, SNAPSHOT_REASON_EDIT,
                        Workspace, atomic_write_json, digest_json,
                        read_json)


class Workbench:
    def __init__(self, root: str, config: Optional[PipelineConfig] = None):
        self.ws = Workspace(root)
        self.cfg = config or default_config()
        self.cfg.validate()
        self.pipeline: Optional[Pipeline] = None
        if os.path.exists(self.ws.state_path):
            self.pipeline = Pipeline(self.ws, self.cfg)

    # ---------- lifecycle ----------
    def import_workspace(self, manifest_path: Optional[str] = None
                         ) -> List[Dict[str, Any]]:
        infos = self.ws.import_manifest(manifest_path)
        self.pipeline = Pipeline(self.ws, self.cfg)
        return [{"shard_id": i.shard_id, "order": i.order,
                 "n_samples": i.n_samples, "duration_ms": i.duration_ms,
                 "n_speakers": i.n_speakers,
                 "content_fingerprint": i.content_fingerprint} for i in infos]

    def require_pipeline(self) -> Pipeline:
        if self.pipeline is None:
            raise EInvalidStage("workspace is not imported yet")
        return self.pipeline

    def update_config(self, **changes: Any) -> Dict[str, Any]:
        try:
            new_cfg = self.cfg.clone(**changes)
        except EInvalidConfig:
            raise
        old_fp = self.cfg.fingerprint()
        self.cfg = new_cfg
        if self.pipeline is not None:
            self.pipeline.cfg = new_cfg
            self.pipeline.cache.clear()
        return {"old_config_version": old_fp,
                "new_config_version": new_cfg.fingerprint(),
                "reason": SNAPSHOT_REASON_CONFIG}

    # ---------- stage execution ----------
    def run_stage(self, shard_id: str, stage: str) -> Dict[str, Any]:
        pipe = self.require_pipeline()
        if stage == STAGE_SEPARATION:
            return pipe.run_separation(shard_id)
        if stage == STAGE_RECOGNITION:
            return pipe.run_recognition(shard_id)
        raise EInvalidStage(f"unknown stage {stage!r}", {"stage": stage})

    def run_all(self) -> List[Dict[str, Any]]:
        return self.require_pipeline().run_all()

    def request_stop(self) -> Dict[str, str]:
        self.ws.request_stop()
        return {"status": "stop_requested"}

    def recover(self) -> Dict[str, Any]:
        return self.require_pipeline().recover()

    def verify_consistency(self) -> Dict[str, Any]:
        return self.require_pipeline().consistency_report()

    def stage_status(self) -> List[Dict[str, Any]]:
        pipe = self.require_pipeline()
        status = []
        for shard_id in sorted(pipe.infos):
            entry = {"shard_id": shard_id, "stages": {}}
            for stage in (STAGE_SEPARATION, STAGE_RECOGNITION):
                if self.ws.has_stage(shard_id, stage):
                    result = self.ws.read_stage_result(shard_id, stage)
                    snap = self.ws.read_snapshot(shard_id, stage)
                    entry["stages"][stage] = {
                        "stage_version": result["stage_version"],
                        "config_version": result["config_version"],
                        "edit_version": result["edit_version"],
                        "snapshot_reason": snap["reason"] if snap else None,
                    }
                else:
                    entry["stages"][stage] = None
            status.append(entry)
        return status

    # ---------- editing ----------
    def get_timeline(self, shard_id: str) -> Dict[str, Any]:
        return self.ws.load_timeline(shard_id).to_dict()

    def _apply_edit(self, timeline: Timeline, operation: str,
                    params: Dict[str, Any], origin: str) -> Timeline:
        if operation == "split":
            return timeline.split(params["seg_id"], int(params["at_ms"]), origin)
        if operation == "merge_adjacent":
            return timeline.merge_adjacent(list(params["seg_ids"]), origin)
        if operation == "rename_speaker":
            return timeline.rename_speaker(params["old"], params["new"], origin)
        if operation == "merge_speakers":
            return timeline.merge_speakers(params["source"], params["target"],
                                           origin)
        if operation == "upsert_segment":
            seg = Segment(**params["segment"])
            return timeline.upsert_segment(seg, origin)
        if operation == "delete_segment":
            return timeline.delete_segment(params["seg_id"], origin)
        raise EInvalidStage(f"unknown edit {operation!r}",
                            {"operation": operation})

    def save_draft(self, shard_id: str, operation: str,
                   params: Dict[str, Any], origin: str = "draft") -> Dict[str, Any]:
        """Edit a private draft; published versions stay untouched."""
        current = self.ws.load_timeline(shard_id)
        path = self.ws.draft_path(shard_id)
        if os.path.exists(path):
            current = self.ws.load_draft(shard_id)
        draft = self._apply_edit(current, operation, params, origin)
        self.ws.save_draft(shard_id, draft)
        return {"draft_version": draft.version, "shard_id": shard_id}

    def discard_draft(self, shard_id: str) -> Dict[str, str]:
        self.ws.discard_draft(shard_id)
        return {"status": "draft_discarded", "shard_id": shard_id}

    def publish_draft(self, shard_id: str, expected_version: int) -> Dict[str, Any]:
        return self._publish(shard_id, expected_version, draft=True)

    def publish_edit(self, shard_id: str, operation: str,
                     params: Dict[str, Any], expected_version: int,
                     origin: str = "api") -> Dict[str, Any]:
        return self._publish(shard_id, expected_version,
                             edit=(operation, params, origin))

    def _publish(self, shard_id: str, expected_version: int,
                 draft: bool = False,
                 edit: Optional[tuple] = None) -> Dict[str, Any]:
        if shard_id not in (self.pipeline.infos if self.pipeline else {}):
            raise ENotFound("unknown shard", {"shard_id": shard_id})
        with self.ws.publish_lock:
            current = self.ws.load_timeline(shard_id)
            if expected_version != current.version:
                raise EStaleVersion(
                    "version conflict; expected current monotonic version",
                    {"shard_id": shard_id,
                     "expected_version": expected_version,
                     "current_version": current.version})
            if draft:
                if not os.path.exists(self.ws.draft_path(shard_id)):
                    raise ENotFound("no draft to publish",
                                    {"shard_id": shard_id})
                new_timeline = self.ws.load_draft(shard_id)
                if new_timeline.version != current.version + 1:
                    raise EStaleVersion(
                        "draft version is not contiguous",
                        {"shard_id": shard_id,
                         "draft_version": new_timeline.version,
                         "current_version": current.version})
            else:
                operation, params, origin = edit
                new_timeline = self._apply_edit(current, operation, params,
                                                 origin)
            self.ws.save_timeline(new_timeline)
            self.ws.discard_draft(shard_id)
            if self.pipeline is not None:
                self.pipeline.cache.clear()
                # Recognition depends on the timeline; refresh it immediately
                # while separation is reused untouched (incremental path).
                if self.ws.has_stage(shard_id, STAGE_SEPARATION):
                    if self.ws.has_stage(shard_id, STAGE_RECOGNITION):
                        self.pipeline.run_recognition(
                            shard_id, reason=SNAPSHOT_REASON_EDIT)
        return {"shard_id": shard_id, "version": new_timeline.version,
                "operation": new_timeline.history[-1].operation,
                "affected_ms": list(new_timeline.history[-1].affected_ms)}

    def unify_identity(self, mappings: Dict[str, str], origin: str = "unify"
                       ) -> Dict[str, Any]:
        """Apply a global speaker rename map across every shard atomically."""
        pipe = self.require_pipeline()
        touched = []
        with self.ws.publish_lock:
            for shard_id in sorted(pipe.infos):
                timeline = self.ws.load_timeline(shard_id)
                next_tl = timeline
                for old, new in sorted(mappings.items()):
                    if old in next_tl.speakers():
                        next_tl = next_tl.rename_speaker(old, new, origin)
                if next_tl is not timeline:
                    self.ws.save_timeline(next_tl)
                    touched.append({"shard_id": shard_id,
                                    "version": next_tl.version})
                    if self.ws.has_stage(shard_id, STAGE_RECOGNITION):
                        pipe.run_recognition(shard_id,
                                             reason=SNAPSHOT_REASON_EDIT)
            pipe.cache.clear()
        return {"mappings": mappings, "touched": touched}

    # ---------- latency-budgeted queries ----------
    def query(self, shard_id: str, kind: str,
              budget_ms: Optional[int] = None) -> Dict[str, Any]:
        pipe = self.require_pipeline()
        if shard_id not in pipe.infos:
            raise ENotFound("unknown shard", {"shard_id": shard_id})
        if kind not in ("segments", "similarity", "overlaps"):
            raise EInvalidStage("unknown query kind", {"kind": kind})
        budget = int(budget_ms if budget_ms is not None
                     else self.cfg.latency_budget_ms)
        query_id = uuid.uuid4().hex[:12]
        started = time.monotonic()
        info = pipe.infos[shard_id]
        timeline = self.ws.load_timeline(shard_id)
        key = pipe.cache.key(shard_id, kind, info.content_fingerprint,
                             self.cfg.fingerprint(), timeline.version)
        cached = pipe.cache.get(key)
        if cached is not None:
            return {"query_id": query_id, "cached": True, "data": cached,
                    "elapsed_ms": round((time.monotonic() - started) * 1000, 3)}

        def produce() -> Any:
            if kind == "segments":
                return timeline.to_dict()
            result = self.ws.read_stage_result(shard_id, STAGE_RECOGNITION)
            if kind == "similarity":
                return {"diagnostics": result["diagnostics"],
                        "centroids": result["centroids"]}
            return {"overlaps": result["overlaps"]}

        data = self._run_budgeted(produce, query_id, started, budget)
        pipe.cache.put(key, data)
        return {"query_id": query_id, "cached": False, "data": data,
                "elapsed_ms": round((time.monotonic() - started) * 1000, 3)}

    @staticmethod
    def _run_budgeted(fn: Callable[[], Any], query_id: str,
                      started: float, budget_ms: int) -> Any:
        try:
            return fn()
        except WorkbenchError:
            raise
        except Exception as exc:  # pragma: no cover - defensive
            raise WorkbenchError(str(exc)) from exc
        finally:
            elapsed_ms = round((time.monotonic() - started) * 1000, 3)
            if elapsed_ms > budget_ms:
                raise ELatencyBudget(
                    "query exceeded latency budget",
                    {"query_id": query_id, "elapsed_ms": elapsed_ms,
                     "budget_ms": budget_ms})

    # ---------- export ----------
    def export_results(self) -> str:
        pipe = self.require_pipeline()
        payload = {"config_version": self.cfg.fingerprint(), "shards": []}
        for shard_id in sorted(pipe.infos):
            timeline = self.ws.load_timeline(shard_id)
            rec = self.ws.read_stage_result(shard_id, STAGE_RECOGNITION)
            payload["shards"].append({
                "shard_id": shard_id,
                "timeline_version": timeline.version,
                "segments": timeline.fingerprint_payload(),
                "overlaps": rec["overlaps"],
                "stats": rec["stats"],
                "stage_version": rec["stage_version"],
            })
        payload["export_digest"] = digest_json(
            {k: v for k, v in payload.items() if k != "export_digest"})
        path = os.path.join(self.ws.exports_dir, "export.json")
        atomic_write_json(path, payload)
        return path

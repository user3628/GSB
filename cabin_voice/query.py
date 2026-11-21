from __future__ import annotations

import time
import uuid
from typing import Any

from .canonical import digest_obj
from .config import ZONE_ORDER, config_from_dict
from .engine import PipelineEngine
from .errors import CabinError, invalid_argument, invalid_stage
from .projection import build_activity_projection, effective_activity_map
from .workspace import Workspace


class QueryService:
    def __init__(self, workspace: Workspace, engine: PipelineEngine) -> None:
        self.workspace = workspace
        self.engine = engine

    def _validate_shard(self, state: dict[str, Any], shard_id: str) -> None:
        if shard_id not in {shard["shard_id"] for shard in state["shards"]}:
            raise invalid_argument("unknown shard_id", shard_id=shard_id)

    def _ensure_vad_fresh(self, state: dict[str, Any], config: Any) -> dict[str, Any]:
        vad_stage = state.get("stages", {}).get("vad")
        if not vad_stage:
            raise invalid_stage(
                "query requires the VAD stage",
                required_stage="vad",
                available_stages=sorted(state.get("stages", {})),
            )
        desired = self.engine.vad_input_fingerprint(state, config)
        if vad_stage.get("input_fingerprint") != desired:
            raise invalid_stage(
                "VAD stage is stale; run the incremental pipeline first",
                required_stage="vad",
                reason="stage_fingerprint_mismatch",
                stage_input_fingerprint=desired,
                recorded_input_fingerprint=vad_stage.get("input_fingerprint"),
            )
        return vad_stage

    def _ensure_arbitration_fresh(
        self,
        state: dict[str, Any],
        config: Any,
        vad_stage: dict[str, Any],
    ) -> dict[str, Any]:
        arbitration_stage = state.get("stages", {}).get("arbitrate")
        if not arbitration_stage:
            raise invalid_stage(
                "query requires the arbitration stage",
                required_stage="arbitrate",
                available_stages=sorted(state.get("stages", {})),
            )
        desired = self.engine.arbitration_input_fingerprint(
            state,
            vad_stage["artifact"],
            config,
        )
        if arbitration_stage.get("input_fingerprint") != desired:
            raise invalid_stage(
                "arbitration stage is stale; run the incremental pipeline first",
                required_stage="arbitrate",
                reason="stage_fingerprint_mismatch",
                stage_input_fingerprint=desired,
                recorded_input_fingerprint=arbitration_stage.get("input_fingerprint"),
            )
        return arbitration_stage

    def _active_result(
        self,
        state: dict[str, Any],
        *,
        shard_id: str,
        time_ms: int,
    ) -> dict[str, Any]:
        activity = effective_activity_map(state)
        projection = build_activity_projection(state, activity)[shard_id]
        active_zones: list[str] = []
        active_intervals: list[dict[str, Any]] = []
        for zone_id in ZONE_ORDER:
            for interval in projection["zones"][zone_id]:
                if interval["start_ms"] <= time_ms < interval["end_ms"]:
                    active_zones.append(zone_id)
                    active_intervals.append(interval)
        overlap_zones = sorted(
            {
                other
                for interval in active_intervals
                for other in interval["overlap_zones"]
                if other in active_zones
            },
            key=ZONE_ORDER.index,
        )
        return {
            "type": "active",
            "shard_id": shard_id,
            "time_ms": time_ms,
            "active_zones": active_zones,
            "active_count": len(active_zones),
            "active_intervals": active_intervals,
            "overlap": len(active_zones) > 1,
            "overlap_zones": overlap_zones,
            "activity_digest": projection["activity_digest"],
        }

    def _event_result(
        self,
        state: dict[str, Any],
        *,
        kind: str,
        shard_id: str,
        time_ms: int,
    ) -> dict[str, Any]:
        arbitration_stage = state["stages"]["arbitrate"]
        source_assignments = arbitration_stage["artifact"]["shards"][shard_id]["assignments"]
        events = [
            assignment
            for assignment in source_assignments
            if assignment["cluster_start_ms"] <= time_ms <= assignment["cluster_end_ms"]
        ]
        if kind == "tdoa":
            return {
                "type": "tdoa",
                "shard_id": shard_id,
                "time_ms": time_ms,
                "events": [
                    {
                        "assignment_id": event["assignment_id"],
                        "word": event["word"],
                        "cluster_start_ms": event["cluster_start_ms"],
                        "cluster_end_ms": event["cluster_end_ms"],
                        "participating_zones": event["participating_zones"],
                        "observed_onset_by_zone": event["observed_onset_by_zone"],
                        "pair_delay_details": event["pair_delay_details"],
                    }
                    for event in events
                ],
                "event_count": len(events),
            }
        return {
            "type": "assignment",
            "shard_id": shard_id,
            "time_ms": time_ms,
            "events": events,
            "event_count": len(events),
        }

    def query(
        self,
        kind: str,
        *,
        shard_id: str,
        time_ms: int,
        budget_ms: int | None = None,
    ) -> dict[str, Any]:
        started_ns = time.perf_counter_ns()
        query_id = "qry-" + uuid.uuid4().hex
        if kind not in {"active", "tdoa", "assignment"}:
            raise invalid_argument(
                "unsupported query kind",
                kind=kind,
                allowed=["active", "tdoa", "assignment"],
            )
        if isinstance(time_ms, bool) or not isinstance(time_ms, int) or time_ms < 0:
            raise invalid_argument("time_ms must be a non-negative integer")
        state = self.workspace.load_state()
        self._validate_shard(state, shard_id)
        config = config_from_dict(state["config"])
        selected_budget = config.query_budget_ms if budget_ms is None else budget_ms
        if isinstance(selected_budget, bool) or not isinstance(selected_budget, int):
            raise invalid_argument("budget_ms must be an integer")
        if selected_budget < 0:
            raise invalid_argument("budget_ms must be non-negative")
        vad_stage = self._ensure_vad_fresh(state, config)
        if kind in {"tdoa", "assignment"}:
            self._ensure_arbitration_fresh(state, config, vad_stage)
        cache_material = {
            "kind": kind,
            "shard_id": shard_id,
            "time_ms": time_ms,
            "content_fingerprint": digest_obj(
                [shard["content_fingerprint"] for shard in state["shards"]]
            ),
            "manifest_digest": state["manifest_digest"],
            "edit_version": state["edit"]["version"],
            "config_fingerprint": digest_obj(
                {
                    "vad": config.phase_digest("vad"),
                    "arbitrate": config.phase_digest("arbitrate"),
                }
            ),
            "stage_artifacts": {
                "vad": vad_stage.get("artifact_digest"),
                "arbitrate": (
                    state["stages"]["arbitrate"].get("artifact_digest")
                    if "arbitrate" in state["stages"]
                    else None
                ),
            },
        }
        cache_key = digest_obj(cache_material)
        cached = self.workspace.cache.get(cache_key)
        cache_hit = cached is not None
        if cached is not None:
            result = cached["result"]
        else:
            if kind == "active":
                result = self._active_result(state, shard_id=shard_id, time_ms=time_ms)
            else:
                result = self._event_result(
                    state,
                    kind=kind,
                    shard_id=shard_id,
                    time_ms=time_ms,
                )
            cached = self.workspace.cache.put(cache_key, result)
        result_digest = cached["result_digest"]
        elapsed_ms = (time.perf_counter_ns() - started_ns) / 1_000_000.0
        if elapsed_ms > selected_budget:
            raise CabinError(
                "E_LATENCY_BUDGET",
                "query exceeded its latency budget",
                details={
                    "query_id": query_id,
                    "query_kind": kind,
                    "elapsed_ms": round(elapsed_ms, 6),
                    "budget_ms": selected_budget,
                    "cache_hit": cache_hit,
                },
                http_status=504,
            )
        return {
            "ok": True,
            "query_id": query_id,
            "query_kind": kind,
            "cache_hit": cache_hit,
            "elapsed_ms": round(elapsed_ms, 6),
            "budget_ms": selected_budget,
            "result_digest": result_digest,
            "result": result,
        }

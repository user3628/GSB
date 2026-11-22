"""Four-stage deterministic build pipeline with cache invalidation and recovery."""

from __future__ import annotations

import json
import shutil
import tempfile
from pathlib import Path
from typing import Any

from .attribution import arbitrate_commands, estimate_delays
from .audio import detect_chunk
from .config import config_version as compute_config_version
from .config import validate_config
from .errors import CabinVoiceError
from .manifest import (
    chunk_content_fingerprint,
    validate_manifest,
    zone_ids as manifest_zone_ids,
)
from .storage import STAGES, CheckpointLog, StageCache, Workspace
from .timeline import EditModel, normalize_intervals
from .util import (
    atomic_write_json,
    first_difference,
    read_json,
    sha256_bytes,
    sha256_file,
    sha256_json,
)


STAGE_INDEX = {stage: index + 1 for index, stage in enumerate(STAGES)}


def _artifact_summary(artifact: dict[str, Any]) -> str:
    return sha256_json(artifact)


def _stage_fingerprint(stage: str, chunk_id: str, components: dict[str, str]) -> str:
    return sha256_json({"stage": stage, "chunk_id": chunk_id, "components": components})


def _effective_chunk_hits(
    chunk: dict[str, Any], edits: EditModel
) -> tuple[dict[str, list[dict[str, Any]]], dict[str, Any]]:
    hits_by_zone: dict[str, list[dict[str, Any]]] = {}
    for segment in chunk["segments"]:
        zone_id = str(segment["zone_id"])
        override = edits.hits_for(str(chunk["chunk_id"]), zone_id)
        hits = override if override is not None else segment.get("lexicon_hits", [])
        hits_by_zone[zone_id] = [
            {
                "term": str(hit["term"]),
                "strength": float(hit["strength"]),
                **({"at_ms": int(hit["at_ms"])} if "at_ms" in hit else {}),
            }
            for hit in hits
        ]
    component = {
        str(chunk["chunk_id"]): {
            zone_id: sorted(
                hits,
                key=lambda item: (
                    item["term"],
                    item.get("at_ms", -1),
                    -item["strength"],
                ),
            )
            for zone_id, hits in sorted(hits_by_zone.items())
        }
    }
    return hits_by_zone, component


class Engine:
    def __init__(self, workspace: Workspace | str | Path):
        self.workspace = workspace if isinstance(workspace, Workspace) else Workspace(workspace)
        self.workspace.ensure()
        self.checkpoints = CheckpointLog(self.workspace)
        self.cache = StageCache(self.workspace)

    # ------------------------------------------------------------------ context
    def load_context(self) -> dict[str, Any]:
        manifest = self.workspace.require_manifest()
        validate_manifest(manifest, self.workspace.root)
        config = self.workspace.load_config()
        zone_ids = manifest_zone_ids(manifest)
        validate_config(config, zone_ids)
        edits = EditModel(self.workspace.edits_path, manifest, persist=False)
        chunks = list(manifest["chunks"])
        chunk_samples: dict[str, Path] = {}
        content_fingerprints: dict[str, str] = {}
        for chunk in chunks:
            wav_path = self.workspace.chunk_wav_path(chunk)
            if not wav_path.exists():
                raise CabinVoiceError(
                    "E_NOT_FOUND",
                    "chunk WAV is missing",
                    {"chunk_id": chunk["chunk_id"], "path": str(wav_path)},
                )
            chunk_samples[str(chunk["chunk_id"])] = wav_path
            content_fingerprints[str(chunk["chunk_id"])] = chunk_content_fingerprint(
                wav_path, chunk["segments"]
            )
        return {
            "manifest": manifest,
            "config": config,
            "config_version": compute_config_version(config),
            "edits": edits,
            "edit_version": edits.edit_version,
            "priorities": edits.priorities(),
            "chunks": chunks,
            "chunk_samples": chunk_samples,
            "content_fingerprints": content_fingerprints,
            "manifest_digest": sha256_file(self.workspace.manifest_path),
            "state_fingerprint": self.workspace.state_fingerprint(),
        }

    def _plan(self, chunks: list[dict[str, Any]]) -> list[dict[str, Any]]:
        plan: list[dict[str, Any]] = []
        for chunk_index, chunk in enumerate(chunks, start=1):
            for stage in STAGES:
                plan.append(
                    {
                        "position": len(plan) + 1,
                        "chunk_id": str(chunk["chunk_id"]),
                        "chunk_index": chunk_index,
                        "stage": stage,
                        "stage_index": STAGE_INDEX[stage],
                    }
                )
        return plan

    def _geometry_component(self, manifest: dict[str, Any]) -> str:
        return sha256_json(
            {
                "microphone": manifest.get("microphone", {"x_m": 0.0, "y_m": 0.0}),
                "zones": [
                    {
                        "zone_id": zone["zone_id"],
                        "x_m": zone["x_m"],
                        "y_m": zone["y_m"],
                    }
                    for zone in sorted(manifest["zones"], key=lambda item: str(item["zone_id"]))
                ],
            }
        )

    # --------------------------------------------------------------- stage work
    def _detection_components(
        self,
        context: dict[str, Any],
        chunk: dict[str, Any],
        detection_config: dict[str, Any],
    ) -> dict[str, str]:
        chunk_id = str(chunk["chunk_id"])
        return {
            "sample": context["content_fingerprints"][chunk_id],
            "segments": sha256_json(
                {
                    "sample_rate": context["manifest"]["sample_rate"],
                    "segments": [
                        {
                            "zone_id": segment["zone_id"],
                            "offset_samples": segment["offset_samples"],
                            "sample_count": segment["sample_count"],
                        }
                        for segment in chunk["segments"]
                    ],
                }
            ),
            "detection_config": sha256_json(detection_config),
            "activity_ops": context["edits"].activity_component(str(chunk["chunk_id"])),
        }

    def _delay_components(
        self,
        detection_artifact: dict[str, Any],
        detection_fingerprint: str,
        manifest: dict[str, Any],
        arrival_config: dict[str, Any],
    ) -> dict[str, str]:
        return {
            "detection_fingerprint": detection_fingerprint,
            "detection_output": sha256_json(detection_artifact),
            "geometry": self._geometry_component(manifest),
            "arrival_config": sha256_json(arrival_config),
        }

    def _attribution_components(
        self,
        delay_artifact: dict[str, Any],
        delay_fingerprint: str,
        context: dict[str, Any],
        hits_component: dict[str, Any],
    ) -> dict[str, str]:
        return {
            "delay_fingerprint": delay_fingerprint,
            "delay_output": sha256_json(delay_artifact),
            "priorities": context["edits"].priority_component(),
            "lexicon_hits": sha256_json(hits_component),
            "attribution_config": sha256_json(context["config"]["attribution"]),
        }

    def _summary_components(
        self,
        attribution_artifact: dict[str, Any],
        attribution_fingerprint: str,
        previous_cumulative: dict[str, Any] | None,
    ) -> dict[str, str]:
        return {
            "attribution_fingerprint": attribution_fingerprint,
            "attribution_output": sha256_json(attribution_artifact),
            "prior_summary": sha256_json(previous_cumulative or {}),
        }

    def _compute_stage(
        self,
        stage: str,
        *,
        context: dict[str, Any],
        chunk: dict[str, Any],
        components: dict[str, str],
        previous_cumulative: dict[str, Any] | None,
        hits_component: dict[str, Any],
    ) -> dict[str, Any]:
        chunk_id = str(chunk["chunk_id"])
        config = context["config"]
        if stage == "detection":
            artifact = detect_chunk(
                context["chunk_samples"][chunk_id],
                chunk,
                config["detection"],
                sample_rate=int(context["manifest"]["sample_rate"]),
                activity_ops=context["edits"].activity_ops(chunk_id),
            )
        elif stage == "delay":
            detection_artifact = self._result_for(context, chunk_id, "detection")["artifact"]
            artifact = estimate_delays(detection_artifact, context["manifest"], config["arrival"])
        elif stage == "attribution":
            detection_artifact = self._result_for(context, chunk_id, "detection")["artifact"]
            delay_artifact = self._result_for(context, chunk_id, "delay")["artifact"]
            hits_by_zone = context["hits_by_zone"][chunk_id]
            artifact = arbitrate_commands(
                chunk,
                detection_artifact,
                delay_artifact,
                hits_by_zone,
                context["priorities"],
                config["attribution"],
            )
        elif stage == "summary":
            attribution_artifact = self._result_for(context, chunk_id, "attribution")["artifact"]
            detection_artifact = self._result_for(context, chunk_id, "detection")["artifact"]
            delay_artifact = self._result_for(context, chunk_id, "delay")["artifact"]
            artifact = self._summary_artifact(
                context,
                chunk,
                detection_artifact,
                delay_artifact,
                attribution_artifact,
                previous_cumulative,
            )
        else:
            raise AssertionError(stage)
        digest = _artifact_summary(artifact)
        fingerprint = _stage_fingerprint(stage, chunk_id, components)
        return {
            "artifact": artifact,
            "artifact_summary": digest,
            "input_fingerprint": fingerprint,
            "components": components,
        }

    def _result_for(self, context: dict[str, Any], chunk_id: str, stage: str) -> dict[str, Any]:
        return context["stage_results"][chunk_id][stage]

    def _summary_artifact(
        self,
        context: dict[str, Any],
        chunk: dict[str, Any],
        detection_artifact: dict[str, Any],
        delay_artifact: dict[str, Any],
        attribution_artifact: dict[str, Any],
        previous_cumulative: dict[str, Any] | None,
    ) -> dict[str, Any]:
        chunk_id = str(chunk["chunk_id"])
        prior_entries = list((previous_cumulative or {}).get("chunks", []))
        zones = {
            str(zone["zone_id"]): normalize_intervals(zone["effective_intervals"])
            for zone in detection_artifact["zones"]
        }
        entry = {
            "chunk_id": chunk_id,
            "content_fingerprint": context["content_fingerprints"][chunk_id],
            "zones": zones,
            "overlap_markers": detection_artifact["overlap_markers"],
            "pair_delays": delay_artifact["pair_delays"],
            "commands": attribution_artifact["events"],
        }
        entries = prior_entries + [entry]
        assigned = sum(
            1
            for command_entry in entries
            for event in command_entry["commands"]
            if event["status"] == "assigned"
        )
        review = sum(
            1
            for command_entry in entries
            for event in command_entry["commands"]
            if event["status"] == "needs_review"
        )
        overlap_count = sum(len(command_entry["overlap_markers"]) for command_entry in entries)
        counts = {
            "chunks": len(entries),
            "assigned_commands": assigned,
            "review_commands": review,
            "overlap_regions": overlap_count,
        }
        session_digest = sha256_json(
            {
                "session_id": context["manifest"]["session_id"],
                "chunks": entries,
                "counts": counts,
            }
        )
        cumulative = {
            "session_id": context["manifest"]["session_id"],
            "chunks": entries,
            "counts": counts,
            "session_digest": session_digest,
        }
        return {
            "chunk_id": chunk_id,
            "content_fingerprint": context["content_fingerprints"][chunk_id],
            "activity_intervals": zones,
            "overlap_markers": detection_artifact["overlap_markers"],
            "pair_delays": delay_artifact["pair_delays"],
            "command_attributions": attribution_artifact["events"],
            "cumulative": cumulative,
        }

    # ---------------------------------------------------------------- build loop
    def build(
        self,
        *,
        mode: str = "incremental",
        resume: bool = False,
        verify: bool = False,
        stop_after_units: int | None = None,
    ) -> dict[str, Any]:
        if mode not in {"incremental", "full"}:
            raise CabinVoiceError("E_BAD_REQUEST", "mode must be incremental or full", {"mode": mode})
        with self.workspace.build_lock():
            return self._build_locked(
                mode=mode,
                resume=resume,
                verify=verify,
                stop_after_units=stop_after_units,
            )

    def _build_locked(
        self,
        *,
        mode: str,
        resume: bool,
        verify: bool,
        stop_after_units: int | None,
    ) -> dict[str, Any]:
        context = self.load_context()
        chunks = context["chunks"]
        plan = self._plan(chunks)
        if resume:
            run = self.checkpoints.load_run()
            if run is None:
                raise CabinVoiceError(
                    "E_NO_CHECKPOINT", "no persisted run to recover", {"workspace": str(self.workspace.root)}
                )
            if run.get("plan") != plan:
                raise CabinVoiceError(
                    "E_RUN_STATE_CHANGED",
                    "current manifest plan differs from the persisted run",
                    {"workspace": str(self.workspace.root)},
                )
            if run.get("state_fingerprint") != context["state_fingerprint"]:
                raise CabinVoiceError(
                    "E_RUN_STATE_CHANGED",
                    "inputs changed after the checkpoint run started",
                    {
                        "persisted_state_fingerprint": run.get("state_fingerprint"),
                        "current_state_fingerprint": context["state_fingerprint"],
                    },
                )
            self.workspace.clear_stop()
            scan = self.checkpoints.recover_prefix(prune=True)
        else:
            build_id = sha256_json(
                {
                    "session_id": context["manifest"]["session_id"],
                    "state_fingerprint": context["state_fingerprint"],
                    "manifest_digest": context["manifest_digest"],
                    "plan": plan,
                }
            )[:32]
            run = self.checkpoints.begin_run(
                plan=plan,
                metadata={
                    "build_id": build_id,
                    "session_id": context["manifest"]["session_id"],
                    "state_fingerprint": context["state_fingerprint"],
                    "manifest_digest": context["manifest_digest"],
                    "config_version": context["config_version"],
                    "edit_version": context["edit_version"],
                    "mode": mode,
                },
                reset=True,
                clear_cache=(mode == "full"),
            )
            self.workspace.final_path.unlink(missing_ok=True)
            scan = self.checkpoints.load_consistent()
        prefix = scan["prefix"]
        old_snapshot = self._load_snapshot() if not resume else None
        context["hits_by_zone"] = {}
        context["hits_component"] = {}
        context["stage_results"] = {str(chunk["chunk_id"]): {} for chunk in chunks}
        for chunk in chunks:
            hits_by_zone, hits_component = _effective_chunk_hits(chunk, context["edits"])
            chunk_id = str(chunk["chunk_id"])
            context["hits_by_zone"][chunk_id] = hits_by_zone
            context["hits_component"][chunk_id] = hits_component
        sources: dict[str, dict[str, str]] = {
            str(chunk["chunk_id"]): {} for chunk in chunks
        }

        # Restore the contiguous checkpoint prefix without recomputing it.
        for record in prefix:
            chunk_id = str(record["chunk_id"])
            stage = str(record["stage"])
            context["stage_results"][chunk_id][stage] = {
                "artifact": record["artifact"],
                "artifact_summary": record["artifact_summary"],
                "input_fingerprint": record["input_fingerprint"],
                "components": record["fingerprint_components"],
            }
            sources[chunk_id][stage] = "checkpoint"

        chunk_map = {str(chunk["chunk_id"]): chunk for chunk in chunks}
        stopped = False
        units_done = 0
        for unit in plan:
            chunk_id = str(unit["chunk_id"])
            stage = str(unit["stage"])
            chunk = chunk_map[chunk_id]
            if stage in context["stage_results"][chunk_id]:
                continue
            if stop_after_units is not None and units_done >= stop_after_units:
                stopped = True
                break
            components = self._components_for(
                stage, context, chunk, previous_cumulative_for_chunk=None
            )
            if stage == "summary":
                # The summary of chunk N depends on summaries of all preceding chunks.
                prior = self._previous_cumulative_for(chunk_id, chunks, context)
                components = self._summary_components(
                    context["stage_results"][chunk_id]["attribution"]["artifact"],
                    context["stage_results"][chunk_id]["attribution"]["input_fingerprint"],
                    prior,
                )
            fingerprint = _stage_fingerprint(stage, chunk_id, components)
            cached = self.cache.get(stage, chunk_id, fingerprint)
            if cached is not None:
                result = {
                    "artifact": cached["artifact"],
                    "artifact_summary": cached["artifact_summary"],
                    "input_fingerprint": fingerprint,
                    "components": components,
                }
                sources[chunk_id][stage] = "cache"
            else:
                result = self._compute_stage(
                    stage,
                    context=context,
                    chunk=chunk,
                    components=components,
                    previous_cumulative=(
                        self._previous_cumulative_for(chunk_id, chunks, context)
                        if stage == "summary"
                        else None
                    ),
                    hits_component=context["hits_component"][chunk_id],
                )
                self.cache.put(
                    stage,
                    chunk_id,
                    fingerprint,
                    artifact=result["artifact"],
                    artifact_summary=result["artifact_summary"],
                    components=components,
                )
                sources[chunk_id][stage] = "computed"
            context["stage_results"][chunk_id][stage] = result
            position = int(unit["position"])
            body = {
                "schema_version": 1,
                "build_id": run["build_id"],
                "seq": position,
                "position": position,
                "chunk_id": chunk_id,
                "chunk_index": int(unit["chunk_index"]),
                "stage": stage,
                "stage_index": int(unit["stage_index"]),
                "content_fingerprint": context["content_fingerprints"][chunk_id],
                "input_fingerprint": result["input_fingerprint"],
                "config_version": context["config_version"],
                "edit_version": context["edit_version"],
                "fingerprint_components": components,
                "artifact_summary": result["artifact_summary"],
                "artifact": result["artifact"],
            }
            self.checkpoints.publish(body, expected_position=position)
            units_done += 1
            if self.workspace.stop_requested():
                stopped = True
                break

        scan_after = self.checkpoints.load_consistent()
        complete = len(scan_after["prefix"]) == len(plan)
        invalidation = self._build_invalidation_report(context, chunks, old_snapshot, sources)
        snapshot = self._build_snapshot(context, chunks)
        atomic_write_json(self.workspace.snapshot_path, snapshot)
        atomic_write_json(self.workspace.invalidation_path, invalidation)
        if not complete:
            return {
                "ok": True,
                "status": "stopped" if (stopped or self.workspace.stop_requested()) else "incomplete",
                "mode": mode,
                "checkpoints": len(scan_after["prefix"]),
                "plan_units": len(plan),
                "next_position": len(scan_after["prefix"]) + 1,
                "last_seq": scan_after["prefix"][-1]["seq"] if scan_after["prefix"] else 0,
                "invalidation": invalidation,
                "final_path": None,
            }

        final = self._final_artifact(context, chunks, plan, scan_after["prefix"])
        final_bytes = self.workspace.write_final(final)
        verification: dict[str, Any] | None = None
        if verify:
            verification = self.verify_full_consistency()
        return {
            "ok": True,
            "status": "complete",
            "mode": mode,
            "checkpoints": len(scan_after["prefix"]),
            "plan_units": len(plan),
            "last_seq": scan_after["prefix"][-1]["seq"],
            "final_path": str(self.workspace.final_path),
            "final_digest": sha256_bytes(final_bytes),
            "summary_digest": final["summary"]["session_digest"],
            "invalidation": invalidation,
            "verification": verification,
        }

    def _components_for(
        self,
        stage: str,
        context: dict[str, Any],
        chunk: dict[str, Any],
        *,
        previous_cumulative_for_chunk: dict[str, Any] | None,
    ) -> dict[str, str]:
        chunk_id = str(chunk["chunk_id"])
        if stage == "detection":
            return self._detection_components(context, chunk, context["config"]["detection"])
        if stage == "delay":
            detection_artifact = context["stage_results"][chunk_id]["detection"]["artifact"]
            return self._delay_components(
                detection_artifact,
                context["stage_results"][chunk_id]["detection"]["input_fingerprint"],
                context["manifest"],
                context["config"]["arrival"],
            )
        if stage == "attribution":
            delay_artifact = context["stage_results"][chunk_id]["delay"]["artifact"]
            return self._attribution_components(
                delay_artifact,
                context["stage_results"][chunk_id]["delay"]["input_fingerprint"],
                context,
                context["hits_component"][chunk_id],
            )
        if stage == "summary":
            attribution_artifact = context["stage_results"][chunk_id]["attribution"]["artifact"]
            return self._summary_components(
                attribution_artifact,
                context["stage_results"][chunk_id]["attribution"]["input_fingerprint"],
                previous_cumulative_for_chunk,
            )
        raise AssertionError(stage)

    def _previous_cumulative_for(
        self,
        chunk_id: str,
        chunks: list[dict[str, Any]],
        context: dict[str, Any],
    ) -> dict[str, Any] | None:
        index = next(index for index, chunk in enumerate(chunks) if str(chunk["chunk_id"]) == chunk_id)
        if index == 0:
            return None
        previous_chunk_id = str(chunks[index - 1]["chunk_id"])
        result = context["stage_results"][previous_chunk_id].get("summary")
        if result is None:
            raise CabinVoiceError(
                "E_STALE_CHECKPOINT",
                "summary predecessor is missing",
                {"chunk_id": chunk_id, "previous_chunk_id": previous_chunk_id},
            )
        return result["artifact"]["cumulative"]

    # ---------------------------------------------------------------- artifacts
    def _build_snapshot(
        self, context: dict[str, Any], chunks: list[dict[str, Any]]
    ) -> dict[str, Any]:
        return {
            "schema_version": 1,
            "config_version": context["config_version"],
            "edit_version": context["edit_version"],
            "state_fingerprint": context["state_fingerprint"],
            "chunks": {
                str(chunk["chunk_id"]): {
                    stage: {
                        "input_fingerprint": context["stage_results"][str(chunk["chunk_id"])][stage][
                            "input_fingerprint"
                        ],
                        "components": context["stage_results"][str(chunk["chunk_id"])][stage][
                            "components"
                        ],
                        "artifact_summary": context["stage_results"][str(chunk["chunk_id"])][stage][
                            "artifact_summary"
                        ],
                    }
                    for stage in STAGES
                    if stage in context["stage_results"][str(chunk["chunk_id"])]
                }
                for chunk in chunks
            },
        }

    def _load_snapshot(self) -> dict[str, Any] | None:
        if not self.workspace.snapshot_path.exists():
            return None
        try:
            return read_json(self.workspace.snapshot_path)
        except (json.JSONDecodeError, UnicodeDecodeError):
            return None

    def _build_invalidation_report(
        self,
        context: dict[str, Any],
        chunks: list[dict[str, Any]],
        old_snapshot: dict[str, Any] | None,
        sources: dict[str, dict[str, str]],
    ) -> dict[str, Any]:
        reason_by_component = {
            "sample": "SAMPLE_CONTENT_CHANGED",
            "segments": "ZONE_LAYOUT_CHANGED",
            "detection_config": "FRAME_OR_HYSTERESIS_CONFIG_CHANGED",
            "activity_ops": "ACTIVITY_EDIT_CHANGED",
            "detection_fingerprint": "UPSTREAM_DETECTION_CHANGED",
            "detection_output": "UPSTREAM_DETECTION_CHANGED",
            "geometry": "GEOMETRY_CHANGED",
            "arrival_config": "ARRIVAL_CONFIG_CHANGED",
            "delay_fingerprint": "UPSTREAM_DELAY_CHANGED",
            "delay_output": "UPSTREAM_DELAY_CHANGED",
            "priorities": "ZONE_PRIORITY_CHANGED",
            "lexicon_hits": "LEXICON_HITS_CHANGED",
            "attribution_config": "ATTRIBUTION_CONFIG_CHANGED",
            "attribution_fingerprint": "UPSTREAM_ATTRIBUTION_CHANGED",
            "attribution_output": "UPSTREAM_ATTRIBUTION_CHANGED",
            "prior_summary": "UPSTREAM_SUMMARY_CHANGED",
        }
        chunk_reports: dict[str, Any] = {}
        rebuilt: list[str] = []
        reused: list[str] = []
        for chunk in chunks:
            chunk_id = str(chunk["chunk_id"])
            old_chunk = ((old_snapshot or {}).get("chunks", {})).get(chunk_id, {})
            earliest_index = len(STAGES)
            changed_components: list[dict[str, Any]] = []
            for stage_index, stage in enumerate(STAGES):
                if stage not in context["stage_results"][chunk_id]:
                    continue
                current = context["stage_results"][chunk_id][stage]
                old = old_chunk.get(stage)
                if old is None:
                    earliest_index = min(earliest_index, stage_index)
                    changed_components.append(
                        {
                            "stage": stage,
                            "component": "<stage>",
                            "reason": "INITIAL_OR_MISSING_STAGE",
                            "before": None,
                            "after": current["input_fingerprint"],
                        }
                    )
                    break
                differing = [
                    component
                    for component in sorted(current["components"])
                    if current["components"][component] != old.get("components", {}).get(component)
                ]
                if differing:
                    earliest_index = min(earliest_index, stage_index)
                    for component in differing:
                        changed_components.append(
                            {
                                "stage": stage,
                                "component": component,
                                "reason": reason_by_component.get(component, "CONTENT_CHANGED"),
                                "before": old.get("components", {}).get(component),
                                "after": current["components"][component],
                            }
                        )
                    break
            invalidated = [STAGES[index] for index in range(earliest_index, len(STAGES))]
            earliest_stage = STAGES[earliest_index] if earliest_index < len(STAGES) else None
            for stage in invalidated:
                rebuilt.append(f"{chunk_id}:{stage}")
            for stage in STAGES:
                if stage not in invalidated and stage in context["stage_results"][chunk_id]:
                    reused.append(f"{chunk_id}:{stage}")
            chunk_reports[chunk_id] = {
                "earliest_stage": earliest_stage,
                "invalidated_stages": invalidated,
                "reused_stages": [stage for stage in STAGES if stage not in invalidated],
                "reason_codes": sorted(
                    {item["reason"] for item in changed_components}
                )
                or ([ "NO_CHANGE"] if earliest_stage is None else []),
                "changed_components": changed_components,
                "sources": sources[chunk_id],
            }
        return {
            "mode": "initial" if old_snapshot is None else "incremental",
            "config_version": context["config_version"],
            "edit_version": context["edit_version"],
            "chunks": chunk_reports,
            "rebuilt_stages": sorted(rebuilt, key=lambda item: (item.split(":", 1)[0], STAGES.index(item.split(":", 1)[1]))),
            "reused_stages": sorted(reused, key=lambda item: (item.split(":", 1)[0], STAGES.index(item.split(":", 1)[1]))),
        }

    def _final_artifact(
        self,
        context: dict[str, Any],
        chunks: list[dict[str, Any]],
        plan: list[dict[str, Any]],
        records: list[dict[str, Any]],
    ) -> dict[str, Any]:
        last_chunk_id = str(chunks[-1]["chunk_id"])
        cumulative = context["stage_results"][last_chunk_id]["summary"]["artifact"]["cumulative"]
        activity_intervals = {
            entry["chunk_id"]: entry["zones"] for entry in cumulative["chunks"]
        }
        overlap_markers = [
            {"chunk_id": entry["chunk_id"], **marker}
            for entry in cumulative["chunks"]
            for marker in entry["overlap_markers"]
        ]
        command_attributions = [
            command
            for entry in cumulative["chunks"]
            for command in entry["commands"]
        ]
        fingerprints = {
            str(chunk["chunk_id"]): {
                stage: {
                    "input_fingerprint": context["stage_results"][str(chunk["chunk_id"])][stage][
                        "input_fingerprint"
                    ],
                    "components": context["stage_results"][str(chunk["chunk_id"])][stage][
                        "components"
                    ],
                    "artifact_summary": context["stage_results"][str(chunk["chunk_id"])][stage][
                        "artifact_summary"
                    ],
                }
                for stage in STAGES
            }
            for chunk in chunks
        }
        final_without_digest = {
            "schema_version": 1,
            "session_id": context["manifest"]["session_id"],
            "manifest_digest": context["manifest_digest"],
            "config_version": context["config_version"],
            "edit_version": context["edit_version"],
            "activity_intervals": activity_intervals,
            "overlap_markers": overlap_markers,
            "command_attributions": command_attributions,
            "summary": cumulative,
            "fingerprints": fingerprints,
            "checkpoint": {
                "count": len(records),
                "last_seq": records[-1]["seq"] if records else 0,
                "plan_units": len(plan),
            },
        }
        return {**final_without_digest, "final_digest": sha256_json(final_without_digest)}

    # ------------------------------------------------------------------ queries
    def status(self) -> dict[str, Any]:
        has_manifest = self.workspace.has_manifest()
        if not has_manifest:
            return {
                "ok": True,
                "workspace": str(self.workspace.root),
                "has_manifest": False,
                "status": "empty",
            }
        run = self.checkpoints.load_run()
        scan = self.checkpoints.load_consistent()
        final_exists = self.workspace.final_path.exists()
        prefix = scan["prefix"]
        plan = run.get("plan", []) if run else []
        stop_requested = self.workspace.stop_requested()
        if not run:
            state = "not_started"
        elif len(prefix) == len(plan) and final_exists:
            state = "complete"
        elif stop_requested:
            state = "stopped"
        else:
            state = "incomplete"
        return {
            "ok": True,
            "workspace": str(self.workspace.root),
            "has_manifest": True,
            "session_id": run.get("session_id") if run else None,
            "status": state,
            "run": (
                {
                    key: value
                    for key, value in run.items()
                    if key not in {"plan"}
                }
                if run
                else None
            ),
            "plan_units": len(plan),
            "checkpoints": len(prefix),
            "last_seq": prefix[-1]["seq"] if prefix else 0,
            "next_position": len(prefix) + 1,
            "stop_requested": stop_requested,
            "invalid_records": scan["invalid"],
            "final_exists": final_exists,
            "final_digest": sha256_file(self.workspace.final_path) if final_exists else None,
        }

    def fingerprints(self) -> dict[str, Any]:
        run = self.checkpoints.load_run()
        if run is None:
            return {"ok": True, "run": None, "chunks": {}, "invalidation": None}
        scan = self.checkpoints.load_consistent()
        records_by_unit = {
            (str(record["chunk_id"]), str(record["stage"])): record
            for record in scan["prefix"]
        }
        chunks: dict[str, Any] = {}
        for unit in run.get("plan", []):
            chunk_id = str(unit["chunk_id"])
            stage = str(unit["stage"])
            record = records_by_unit.get((chunk_id, stage))
            chunks.setdefault(chunk_id, {})[stage] = (
                {
                    "status": "published",
                    "input_fingerprint": record["input_fingerprint"],
                    "components": record["fingerprint_components"],
                    "artifact_summary": record["artifact_summary"],
                    "seq": record["seq"],
                }
                if record
                else {"status": "pending"}
            )
        invalidation = None
        if self.workspace.invalidation_path.exists():
            try:
                invalidation = read_json(self.workspace.invalidation_path)
            except (json.JSONDecodeError, UnicodeDecodeError):
                invalidation = None
        return {
            "ok": True,
            "run": {key: value for key, value in run.items() if key != "plan"},
            "chunks": chunks,
            "invalidation": invalidation,
        }

    def result(self) -> dict[str, Any]:
        data = self.workspace.read_final_bytes()
        if data is None:
            raise CabinVoiceError(
                "E_NOT_FOUND",
                "workspace has no complete final artifact",
                {"workspace": str(self.workspace.root)},
            )
        return json.loads(data.decode("utf-8"))

    # --------------------------------------------------------------- consistency
    def verify_full_consistency(self) -> dict[str, Any]:
        current = self.workspace.read_final_bytes()
        if current is None:
            raise CabinVoiceError(
                "E_NO_CHECKPOINT", "an incremental final artifact is required before verification", {}
            )
        temporary_root = tempfile.mkdtemp(prefix="cabin_voice_reference_")
        try:
            reference_workspace = self.workspace.clone_inputs_to(temporary_root)
            reference_engine = Engine(reference_workspace)
            reference_engine.build(mode="full", resume=False, verify=False)
            reference = reference_workspace.read_final_bytes()
            if reference is None:
                raise CabinVoiceError(
                    "E_INCREMENTAL_MISMATCH",
                    "full reference run did not produce a final artifact",
                    {"workspace": temporary_root},
                )
            if current != reference:
                difference = first_difference(current, reference)
                raise CabinVoiceError(
                    "E_INCREMENTAL_MISMATCH",
                    "incremental result differs byte-for-byte from the full build",
                    {
                        "first_difference": difference,
                        "incremental_sha256": sha256_bytes(current),
                        "full_sha256": sha256_bytes(reference),
                    },
                )
            return {
                "ok": True,
                "equal": True,
                "incremental_sha256": sha256_bytes(current),
                "full_sha256": sha256_bytes(reference),
                "bytes": len(current),
            }
        finally:
            shutil.rmtree(temporary_root, ignore_errors=True)

    def compare_with_full(self) -> dict[str, Any]:
        return self.verify_full_consistency()

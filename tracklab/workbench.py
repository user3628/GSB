"""Workbench orchestration: staged execution, resumability, diagnostics, reports."""

from __future__ import annotations

import copy
import json
import os
import shutil
from pathlib import Path
from typing import Any, Callable, Iterable

from .appearance import (
    appearance_artifact_path,
    build_appearance_stage,
    load_appearance_stage,
)
from .canonical import atomic_write_json, digest
from .checkpoints import CheckpointStore
from .config import Settings
from .diagnostics import record_error
from .errors import E_INVALID_STAGE, E_MANIFEST, E_NOT_FOUND, E_STALE_CHECKPOINT, WorkbenchError, raise_error
from .geometry import validate_box
from .manifest import ManifestView, validate_manifest
from .scoring import build_candidate_grid, score_candidates
from .tracker import evolve_frame, initialize_state_and_template
from .versions import (
    appearance_config_fingerprint,
    list_invalidation_events,
    prepare_sequence,
    scoring_config_fingerprint,
    sequence_version_fingerprint,
    stage_fingerprint_summary,
    trajectory_config_fingerprint,
)


VALID_STAGES = {"appearance", "candidates", "trajectory", "tracking", "report", "all"}


def _stop_request_payload(sequence_id: str, reason: str) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "sequence_id": sequence_id,
        "requested": True,
        "reason": reason,
    }


class Workbench:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.store = CheckpointStore(settings)
        self._manifest: ManifestView | None = None

    @property
    def manifest(self) -> ManifestView:
        if self._manifest is None:
            self._manifest = validate_manifest(self.settings, verify_frame_files=True)
        return self._manifest

    def reload_manifest(self) -> ManifestView:
        self._manifest = validate_manifest(self.settings, verify_frame_files=True)
        return self._manifest

    def sequence(self, sequence_id: str) -> dict[str, Any]:
        return self.manifest.by_id(sequence_id)

    def import_manifest(self, source_path: str | Path, *, source_data_dir: str | Path | None = None) -> dict[str, Any]:
        source = Path(source_path).resolve()
        if not source.exists():
            raise_error(E_NOT_FOUND, "source manifest was not found", context={"path": str(source)})
        try:
            raw = json.loads(source.read_text(encoding="utf-8"))
        except (OSError, ValueError) as exc:
            raise_error(E_MANIFEST, "source manifest is not valid JSON", context={"path": str(source), "detail": str(exc)})
        if not isinstance(raw, dict):
            raise_error(E_MANIFEST, "source manifest root must be an object", context={"path": str(source)})
        source_data = Path(source_data_dir).resolve() if source_data_dir is not None else source.parent.resolve()
        copied = 0
        for sequence in raw.get("sequences", []):
            if not isinstance(sequence, dict):
                continue
            for entry in sequence.get("frames", []):
                if not isinstance(entry, dict) or not isinstance(entry.get("path"), str):
                    continue
                relative = Path(entry["path"])
                if relative.is_absolute() or ".." in relative.parts:
                    raise_error(E_MANIFEST, "source frame path is unsafe", context={"path": entry["path"]})
                source_frame = (source_data / relative).resolve()
                try:
                    source_frame.relative_to(source_data)
                except ValueError:
                    raise_error(E_MANIFEST, "source frame path escapes the source data directory", context={"path": entry["path"]})
                if not source_frame.exists():
                    raise_error(E_MANIFEST, "source frame file is missing", context={"path": str(source_frame)})
                target = self.settings.data_dir / relative
                if source_frame != target.resolve():
                    target.parent.mkdir(parents=True, exist_ok=True)
                    payload = source_frame.read_bytes()
                    temp = target.with_name(f".{target.name}.import.{os.getpid()}.tmp")
                    temp.write_bytes(payload)
                    os.replace(temp, target)
                    copied += 1
        atomic_write_json(self.settings.manifest_path, raw, pretty=True)
        self.reload_manifest()
        return {
            "ok": True,
            "source_manifest": str(source),
            "source_data_dir": str(source_data),
            "copied_frame_files": copied,
            "manifest_path": str(self.settings.manifest_path),
            "manifest_fingerprint": self.manifest.fingerprint,
        }

    def request_stop(self, sequence_id: str, reason: str = "external request") -> dict[str, Any]:
        self.sequence(sequence_id)
        path = self.settings.stop_request_path(sequence_id)
        payload = _stop_request_payload(sequence_id, reason)
        atomic_write_json(path, payload, pretty=True)
        return {"ok": True, "sequence_id": sequence_id, "stop_request_path": str(path), "request": payload}

    def clear_stop(self, sequence_id: str) -> None:
        path = self.settings.stop_request_path(sequence_id)
        try:
            path.unlink()
        except FileNotFoundError:
            pass

    def stop_requested(self, sequence_id: str) -> bool:
        return self.settings.stop_request_path(sequence_id).exists()

    def _appearance_stage_ready(self, sequence: dict[str, Any]) -> tuple[bool, dict[str, Any] | None]:
        artifact = load_appearance_stage(self.settings, str(sequence["sequence_id"]))
        if artifact is None:
            return False, None
        records = artifact.get("records", [])
        if len(records) != len(sequence["frames"]):
            return False, artifact
        from .canonical import sha256_file

        from .appearance import appearance_source_fingerprint

        for index, entry in enumerate(sequence["frames"]):
            try:
                actual = sha256_file(self.manifest.frame_path(sequence, index))
            except OSError:
                return False, artifact
            expected_source = appearance_source_fingerprint(self.settings, sequence, index, actual)
            if records[index].get("frame_sha256") != actual:
                return False, artifact
            if records[index].get("source_fingerprint") != expected_source:
                return False, artifact
            if records[index].get("reference_truth_box_xywh_norm") != entry["box_xywh_norm"]:
                return False, artifact
        return True, artifact

    def _candidate_artifact_path(self, sequence_id: str) -> Path:
        return self.settings.sequence_artifact_dir(sequence_id) / "candidates.json"

    def run_candidate_stage(self, sequence: dict[str, Any]) -> dict[str, Any]:
        """Execute/audit stage 2 without publishing trajectory states.

        Published checkpoints are audited by candidate digest. If the trajectory has
        not reached the end, one next-frame candidate set is scored from the persisted
        state; the result is kept as a stage artifact and is not a trajectory publish.
        """
        sequence_id = str(sequence["sequence_id"])
        ready, appearance = self._appearance_stage_ready(sequence)
        if not ready or appearance is None:
            raise_error(
                E_INVALID_STAGE,
                "candidate stage requires a complete, current appearance stage",
                context={"sequence_id": sequence_id, "required_stage": "appearance"},
            )
        scan = self.store.scan(sequence_id)
        checked = [
            {
                "frame_index": record["frame_index"],
                "candidate_record_digest": record["candidate_record"]["candidate_record_digest"],
                "response_score": record["candidate_record"]["response_score"],
            }
            for record in scan.records
        ]
        preview: dict[str, Any] | None = None
        if scan.count < len(sequence["frames"]):
            frame_index = scan.count
            frame = self.manifest.load_frame(sequence, frame_index)
            if scan.last_state is None:
                state = initialize_state_and_template(frame, sequence, self.settings)
            else:
                state = scan.last_state
            expansion = float(sequence["search_window_expansion"])
            if state["occluded"] and state["occlusion_anchor"] is not None:
                predicted = [float(v) for v in state["occlusion_anchor"]["box_xywh_norm"]]
                expansion *= float(self.settings.tracking["recovery_search_expansion_multiplier"])
            else:
                predicted = [
                    float(state["box_xywh_norm"][0]) + float(state["velocity_per_frame"][0]),
                    float(state["box_xywh_norm"][1]) + float(state["velocity_per_frame"][1]),
                    max(0.01, float(state["box_xywh_norm"][2]) + float(state["size_velocity_per_frame"][0])),
                    max(0.01, float(state["box_xywh_norm"][3]) + float(state["size_velocity_per_frame"][1])),
                ]
            sequence_view = dict(sequence)
            sequence_view["_current_frame_index"] = frame_index
            candidates, grid = build_candidate_grid(
                predicted, sequence_view, self.settings, expansion_override=expansion
            )
            record = score_candidates(
                frame,
                state["template"]["model"],
                candidates,
                predicted,
                grid,
                sequence_view,
                self.settings,
                template_source_frame=int(state["template"]["source_frame"]),
            )
            preview = {
                "frame_index": frame_index,
                "candidate_record_digest": record["candidate_record_digest"],
                "candidate_count": record["candidate_count"],
                "response_score": record["response_score"],
                "best_candidate": record["best_candidate"],
                "sampling_grid": record["sampling_grid"],
            }
        source = {
            "appearance_fingerprint": appearance["appearance_fingerprint"],
            "scoring_config_fingerprint": scoring_config_fingerprint(self.settings, sequence),
            "checked_frame_digests": checked,
            "preview": preview,
        }
        artifact = {
            "schema_version": 1,
            "stage": "candidates",
            "sequence_id": sequence_id,
            "source": source,
            "source_fingerprint": digest(source),
            "audited_frames": len(checked),
            "preview_frame": preview["frame_index"] if preview else None,
            "candidate_stage_fingerprint": digest(
                {
                    "kind": "candidate-stage-v1",
                    "sequence_id": sequence_id,
                    "source_fingerprint": digest(source),
                }
            ),
        }
        artifact["stage_digest"] = digest(artifact)
        atomic_write_json(self._candidate_artifact_path(sequence_id), artifact)
        return {
            "ok": True,
            "sequence_id": sequence_id,
            "stage": "candidates",
            "audited_frames": len(checked),
            "preview_frame": artifact["preview_frame"],
            "candidate_stage_fingerprint": artifact["candidate_stage_fingerprint"],
            "stage_digest": artifact["stage_digest"],
        }

    def _candidate_stage_ready(self, sequence: dict[str, Any], appearance: dict[str, Any]) -> bool:
        sequence_id = str(sequence["sequence_id"])
        path = self._candidate_artifact_path(sequence_id)
        if not path.exists():
            return False
        try:
            artifact = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            return False
        if (
            artifact.get("schema_version") != 1
            or artifact.get("stage") != "candidates"
            or artifact.get("sequence_id") != sequence_id
        ):
            return False
        payload = dict(artifact)
        claimed_stage_digest = payload.pop("stage_digest", None)
        if claimed_stage_digest != digest(payload):
            return False
        source = artifact.get("source", {})
        if artifact.get("source_fingerprint") != digest(source):
            return False
        expected_stage_fingerprint = digest(
            {
                "kind": "candidate-stage-v1",
                "sequence_id": sequence_id,
                "source_fingerprint": artifact["source_fingerprint"],
            }
        )
        if artifact.get("candidate_stage_fingerprint") != expected_stage_fingerprint:
            return False
        if source.get("appearance_fingerprint") != appearance.get("appearance_fingerprint"):
            return False
        if source.get("scoring_config_fingerprint") != scoring_config_fingerprint(self.settings, sequence):
            return False
        scan = self.store.scan(sequence_id)
        expected_checked = [
            {
                "frame_index": record["frame_index"],
                "candidate_record_digest": record["candidate_record"]["candidate_record_digest"],
                "response_score": record["candidate_record"]["response_score"],
            }
            for record in scan.records
        ]
        if source.get("checked_frame_digests") != expected_checked:
            return False
        preview = source.get("preview")
        if scan.count < len(sequence["frames"]):
            if not isinstance(preview, dict) or preview.get("frame_index") != scan.count:
                return False
        elif preview is not None:
            return False
        return True

    def run_stage(
        self,
        sequence_id: str,
        stage: str = "all",
        *,
        stop_after: int | None = None,
        stop_flag: Callable[[], bool] | None = None,
    ) -> dict[str, Any]:
        stage = str(stage)
        try:
            if stage not in VALID_STAGES:
                raise_error(
                    E_INVALID_STAGE,
                    "unsupported stage transition",
                    context={"sequence_id": sequence_id, "requested_stage": stage, "allowed_stages": sorted(VALID_STAGES)},
                )
            sequence = self.sequence(sequence_id)
            if stage in {"appearance", "all"}:
                appearance_result = build_appearance_stage(self.settings, self.manifest, sequence)
            else:
                ready, artifact = self._appearance_stage_ready(sequence)
                if not ready or artifact is None:
                    raise_error(
                        E_INVALID_STAGE,
                        "this stage requires a complete, current appearance stage",
                        context={
                            "sequence_id": sequence_id,
                            "required_stage": "appearance",
                            "appearance_artifact": str(appearance_artifact_path(self.settings, sequence_id)),
                            "artifact_frames": len((artifact or {}).get("records", [])),
                        },
                    )
                appearance_result = {
                    "ok": True,
                    "sequence_id": sequence_id,
                    "stage": "appearance",
                    "appearance_fingerprint": artifact.get("appearance_fingerprint"),
                    "frame_count": len(artifact.get("records", [])),
                    "reused": True,
                }
            if stage == "appearance":
                return {
                    "ok": True,
                    "sequence_id": sequence_id,
                    "stage_run": "appearance",
                    "appearance": appearance_result,
                }
            if stage == "report":
                return self.build_report([sequence_id])
            if stage in {"candidates", "all"}:
                candidate_result = self.run_candidate_stage(sequence)
            else:
                _ready, artifact = self._appearance_stage_ready(sequence)
                assert artifact is not None
                if not self._candidate_stage_ready(sequence, artifact):
                    raise_error(
                        E_INVALID_STAGE,
                        "trajectory stage requires a current candidate-stage artifact",
                        context={
                            "sequence_id": sequence_id,
                            "required_stage": "candidates",
                            "candidate_artifact": str(self._candidate_artifact_path(sequence_id)),
                        },
                    )
                candidate_result = {
                    "ok": True,
                    "sequence_id": sequence_id,
                    "stage": "candidates",
                    "reused": True,
                }
            if stage == "candidates":
                return {
                    "ok": True,
                    "sequence_id": sequence_id,
                    "stage_run": "candidates",
                    "appearance": appearance_result,
                    "candidates": candidate_result,
                }
            result = self._run_tracking(
                sequence,
                stop_after=stop_after,
                stop_flag=stop_flag,
                appearance_result=appearance_result,
            )
            if result.get("status") == "complete":
                candidate_result = self.run_candidate_stage(self.sequence(sequence_id))
            result["candidates"] = candidate_result
            return result
        except WorkbenchError as exc:
            record_error(
                self.settings,
                exc,
                operation=f"run_stage:{stage}",
                sequence_id=sequence_id,
            )
            raise

    def _run_tracking(
        self,
        sequence: dict[str, Any],
        *,
        stop_after: int | None,
        stop_flag: Callable[[], bool] | None,
        appearance_result: dict[str, Any],
    ) -> dict[str, Any]:
        sequence_id = str(sequence["sequence_id"])
        frame_count = len(sequence["frames"])
        ready, artifact = self._appearance_stage_ready(sequence)
        if not ready or artifact is None:
            raise_error(
                E_INVALID_STAGE,
                "tracking requires a complete, current appearance stage",
                context={"sequence_id": sequence_id, "required_stage": "appearance"},
            )
        appearance_records = artifact["records"]
        prepared = prepare_sequence(self.settings, self.manifest, sequence, self.store)
        invalidations = prepared.invalidation_events
        processed: list[int] = []
        stopped = False
        stop_reason: str | None = None
        while prepared.scan.count < frame_count:
            if stop_flag is not None and stop_flag():
                stopped = True
                stop_reason = "process signal or local stop flag"
                break
            if self.stop_requested(sequence_id):
                stopped = True
                stop_reason = "stop request file"
                break
            if stop_after is not None and prepared.scan.count >= int(stop_after):
                stopped = True
                stop_reason = "configured frame budget" if prepared.scan.count > int(stop_after) else "stop request delivered at frame budget"
                self.request_stop(sequence_id, reason=stop_reason)
                break
            frame_index = prepared.scan.count
            prior_record = prepared.scan.last_record
            prior_state = prior_record["state"] if prior_record is not None else None
            frame = self.manifest.load_frame(sequence, frame_index)
            if prior_state is None:
                prior_state = initialize_state_and_template(frame, sequence, self.settings)
            state, candidate_record, tracking_record = evolve_frame(
                frame,
                sequence,
                frame_index,
                prior_state,
                self.settings,
            )
            state_digest = digest(state)
            appearance_record = appearance_records[frame_index]
            appearance_stage_fp = digest(
                {
                    "source_fingerprint": appearance_record["source_fingerprint"],
                    "model_digest": appearance_record["model_digest"],
                }
            )
            sources = {
                "appearance_fingerprint": appearance_record["source_fingerprint"],
                "appearance_model_digest": appearance_record["model_digest"],
                "appearance_config_fingerprint": appearance_config_fingerprint(self.settings),
                "scoring_config_fingerprint": prepared.scoring_config_fingerprint,
                "trajectory_config_fingerprint": prepared.trajectory_config_fingerprint,
                "frame_sha256": prepared.frame_hashes[frame_index],
                "initial_box": sequence["initial_box"],
                "reference_truth_box": sequence["frames"][frame_index]["box_xywh_norm"],
            }
            record = {
                "schema_version": 1,
                "kind": "tracklab.checkpoint",
                "sequence_id": sequence_id,
                "frame_index": frame_index,
                "ordinal": frame_index + 1,
                "logical_time_ms": int(round(frame_index * 1000.0 / float(sequence["frame_rate"]))),
                "input_frame_content_fingerprint": prepared.frame_hashes[frame_index],
                "config_fingerprint": self.settings.config_fingerprint,
                "sequence_version_fingerprint": prepared.sequence_version_fingerprint,
                "sources": sources,
                "stage_fingerprints": {
                    "appearance": appearance_stage_fp,
                    "candidates": candidate_record["candidate_record_digest"],
                    "trajectory": tracking_record["tracking_record_digest"],
                    "composite": digest([appearance_stage_fp, candidate_record["candidate_record_digest"], tracking_record["tracking_record_digest"]]),
                },
                "appearance_record_digest": appearance_record["record_digest"],
                "candidate_record": candidate_record,
                "tracking_record": tracking_record,
                "state": state,
                "state_digest": state_digest,
                "previous_state_digest": prior_record["state_digest"] if prior_record else None,
            }
            try:
                published = self.store.publish(sequence_id, record)
            except WorkbenchError as exc:
                record_error(
                    self.settings,
                    exc,
                    operation="publish_checkpoint",
                    sequence_id=sequence_id,
                    frame_index=frame_index,
                )
                raise
            processed.append(frame_index)
            # Re-read the published record so a fresh run and a resumed run consume the
            # exact same serialized state on the next frame boundary.
            prepared.scan = self.store.scan(sequence_id)
            if prepared.scan.count - 1 < frame_index:
                raise_error(
                    E_STALE_CHECKPOINT,
                    "published checkpoint is not visible in the contiguous prefix",
                    context={"sequence_id": sequence_id, "frame_index": frame_index, "valid_frames": prepared.scan.count},
                )
        scan = self.store.scan(sequence_id)
        status = "complete" if scan.count == frame_count else "stopped" if stopped else "incomplete"
        summary = self.sequence_status(sequence_id, include_stage_fingerprints=False)
        result = {
            "ok": status in {"complete", "stopped"},
            "sequence_id": sequence_id,
            "status": status,
            "stage_run": "tracking",
            "stop_reason": stop_reason,
            "processed_frames": processed,
            "valid_frames": scan.count,
            "total_frames": frame_count,
            "next_frame_index": scan.next_frame_index,
            "checkpoint_chain_digest": scan.chain_digest,
            "invalidation_events": invalidations,
            "appearance": appearance_result,
            "stages": summary.get("stages"),
        }
        atomic_write_json(self.settings.sequence_artifact_dir(sequence_id) / "last_run.json", result, pretty=True)
        if status == "complete":
            self.write_sequence_outputs(sequence_id)
        return result

    def resume_sequence(
        self,
        sequence_id: str,
        *,
        stop_after: int | None = None,
        stop_flag: Callable[[], bool] | None = None,
    ) -> dict[str, Any]:
        self.clear_stop(sequence_id)
        return self.run_stage(sequence_id, "all", stop_after=stop_after, stop_flag=stop_flag)

    def sequence_status(self, sequence_id: str, *, include_stage_fingerprints: bool = True) -> dict[str, Any]:
        sequence = self.sequence(sequence_id)
        scan = self.store.scan(sequence_id)
        frame_count = len(sequence["frames"])
        if include_stage_fingerprints:
            versions = stage_fingerprint_summary(self.settings, self.manifest, sequence, self.store)
        else:
            versions = stage_fingerprint_summary(
                self.settings,
                self.manifest,
                sequence,
                self.store,
                frame_hashes=self.sequence_frame_hashes(sequence),
            )
        occlusions, recoveries, updates = self._summarize_events(scan.records)
        return {
            "ok": True,
            "sequence_id": sequence_id,
            "motion_model": sequence["motion_model"],
            "frame_count": frame_count,
            "visible_frames": sequence["visible_frames"],
            "sample_count_upper_bound": sequence["sample_count_upper_bound"],
            "frame_rate": sequence["frame_rate"],
            "initial_box": sequence["initial_box"],
            "search_window_expansion": sequence["search_window_expansion"],
            "appearance_update_threshold": sequence["appearance_update_threshold"],
            "trace_confidence_floor": sequence["trace_confidence_floor"],
            "valid_frames": scan.count,
            "complete": scan.count == frame_count,
            "next_frame_index": scan.next_frame_index,
            "checkpoint_issues": scan.issues,
            "occlusion_intervals": occlusions,
            "recoveries": recoveries,
            "template_update_count": len(updates),
            "stages": versions["stages"],
            "versions": versions,
        }

    def sequence_frame_hashes(self, sequence: dict[str, Any]) -> list[str]:
        from .versions import current_frame_hashes

        return current_frame_hashes(self.manifest, sequence)

    def list_sequences(self) -> dict[str, Any]:
        sequences = []
        for sequence in self.manifest.sequences:
            status = self.sequence_status(sequence["sequence_id"], include_stage_fingerprints=False)
            sequences.append(
                {
                    "sequence_id": sequence["sequence_id"],
                    "motion_type": sequence["motion_model"]["type"],
                    "frame_count": status["frame_count"],
                    "visible_frames": status["visible_frames"],
                    "sample_count_upper_bound": status["sample_count_upper_bound"],
                    "valid_frames": status["valid_frames"],
                    "complete": status["complete"],
                    "occlusion_intervals": status["occlusion_intervals"],
                    "stages": status["stages"],
                }
            )
        return {
            "ok": True,
            "manifest_fingerprint": self.manifest.fingerprint,
            "sequence_count": len(sequences),
            "sequences": sequences,
        }

    def _summarize_events(
        self, records: list[dict[str, Any]]
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
        occlusions: list[dict[str, Any]] = []
        recoveries: list[dict[str, Any]] = []
        updates: list[dict[str, Any]] = []
        open_occlusion: dict[str, Any] | None = None
        for record in records:
            tracking = record["tracking_record"]
            for event in tracking.get("events", []):
                event_type = event.get("type")
                if event_type == "occlusion_started" and open_occlusion is None:
                    open_occlusion = {
                        "start_frame": event["start_frame"],
                        "anchor_frame": event["anchor_frame"],
                        "anchor_box_xywh_norm": event["anchor_box_xywh_norm"],
                        "enter_frame": event["frame_index"],
                    }
                elif event_type == "occlusion_closed" and open_occlusion is not None:
                    open_occlusion.update(
                        {
                            "end_frame": event["end_frame"],
                            "recovery_frame": event["recovery_frame"],
                            "recovered": True,
                        }
                    )
                    occlusions.append(open_occlusion)
                    open_occlusion = None
                elif event_type == "occlusion_recovered":
                    recoveries.append(copy.deepcopy(event))
                elif event_type == "template_updated":
                    updates.append(copy.deepcopy(event))
        if open_occlusion is not None and records:
            open_occlusion.update(
                {
                    "end_frame": records[-1]["frame_index"],
                    "recovery_frame": None,
                    "recovered": False,
                }
            )
            occlusions.append(open_occlusion)
        return occlusions, recoveries, updates

    def build_sequence_bundle(self, sequence_id: str) -> dict[str, Any]:
        sequence = self.sequence(sequence_id)
        scan = self.store.scan(sequence_id)
        if scan.count != len(sequence["frames"]):
            raise_error(
                E_INVALID_STAGE,
                "report requires all trajectory frames to be checkpointed",
                context={
                    "sequence_id": sequence_id,
                    "valid_frames": scan.count,
                    "required_frames": len(sequence["frames"]),
                    "next_frame_index": scan.next_frame_index,
                },
            )
        occlusions, recoveries, updates = self._summarize_events(scan.records)
        frame_errors = []
        tracking_frames = []
        for record in scan.records:
            tracking = record["tracking_record"]
            tracking_frames.append(
                {
                    "frame_index": tracking["frame_index"],
                    "logical_time_ms": tracking["logical_time_ms"],
                    "box_xywh_norm": tracking["box_xywh_norm"],
                    "reference_truth_box_xywh_norm": tracking["reference_truth_box_xywh_norm"],
                    "response_score": tracking["response_score"],
                    "state_action": tracking["state_action"],
                    "occluded": tracking["occluded"],
                    "template_updated": tracking["template_updated"],
                }
            )
            frame_errors.append(
                {
                    "frame_index": tracking["frame_index"],
                    "logical_time_ms": tracking["logical_time_ms"],
                    "iou": tracking["iou"],
                    "center_error_normalized": tracking["center_error_normalized"],
                    "response_score": tracking["response_score"],
                }
            )
        artifact = load_appearance_stage(self.settings, sequence_id)
        versions = stage_fingerprint_summary(self.settings, self.manifest, sequence, self.store)
        if frame_errors:
            mean_iou = sum(item["iou"] for item in frame_errors) / len(frame_errors)
            max_center_error = max(item["center_error_normalized"] for item in frame_errors)
        else:
            mean_iou = 0.0
            max_center_error = 0.0
        bundle = {
            "schema_version": 1,
            "sequence_id": sequence_id,
            "manifest_fingerprint": self.manifest.fingerprint,
            "sequence_version_fingerprint": scan.records[-1]["sequence_version_fingerprint"],
            "config_fingerprint": self.settings.config_fingerprint,
            "appearance_fingerprint": artifact.get("appearance_fingerprint") if artifact else None,
            "stage_fingerprints": versions["stages"],
            "checkpoint_chain_digest": scan.chain_digest,
            "tracking_frames": tracking_frames,
            "frame_errors": frame_errors,
            "occlusion_intervals": occlusions,
            "recoveries": recoveries,
            "template_updates": updates,
            "mean_iou": round(mean_iou, 12),
            "max_center_error_normalized": round(max_center_error, 12),
            "final_box_xywh_norm": tracking_frames[-1]["box_xywh_norm"],
        }
        bundle["bundle_digest"] = digest(bundle)
        return bundle

    def write_sequence_outputs(self, sequence_id: str) -> Path:
        bundle = self.build_sequence_bundle(sequence_id)
        path = self.settings.reports_dir / "sequences" / f"{sequence_id}.json"
        atomic_write_json(path, bundle, pretty=True)
        return path

    def build_report(self, sequence_ids: Iterable[str] | None = None) -> dict[str, Any]:
        if sequence_ids is None:
            sequence_ids = [seq["sequence_id"] for seq in self.manifest.sequences]
        ids = list(sequence_ids)
        bundles = [self.build_sequence_bundle(sequence_id) for sequence_id in ids]
        package = {
            "schema_version": 1,
            "kind": "tracklab.run_bundle",
            "manifest_fingerprint": self.manifest.fingerprint,
            "config_fingerprint": self.settings.config_fingerprint,
            "sequences": bundles,
        }
        package["summary_sha256"] = digest(package)
        bytes_written = atomic_write_json(self.settings.reports_dir / "run_bundle.json", package)
        summary = {
            "schema_version": 1,
            "summary_sha256": package["summary_sha256"],
            "manifest_fingerprint": self.manifest.fingerprint,
            "config_fingerprint": self.settings.config_fingerprint,
            "run_bundle_sha256": __import__("hashlib").sha256(bytes_written).hexdigest(),
            "sequence_count": len(bundles),
            "total_frames": sum(len(bundle["tracking_frames"]) for bundle in bundles),
            "mean_iou": round(
                sum(bundle["mean_iou"] for bundle in bundles) / max(1, len(bundles)), 12
            ),
            "occlusion_interval_count": sum(len(bundle["occlusion_intervals"]) for bundle in bundles),
            "recovered_occlusion_count": sum(
                1
                for bundle in bundles
                for interval in bundle["occlusion_intervals"]
                if interval.get("recovered")
            ),
            "template_update_count": sum(len(bundle["template_updates"]) for bundle in bundles),
            "sequences": [
                {
                    "sequence_id": bundle["sequence_id"],
                    "frames": len(bundle["tracking_frames"]),
                    "mean_iou": bundle["mean_iou"],
                    "max_center_error_normalized": bundle["max_center_error_normalized"],
                    "occlusion_intervals": bundle["occlusion_intervals"],
                    "checkpoint_chain_digest": bundle["checkpoint_chain_digest"],
                    "bundle_digest": bundle["bundle_digest"],
                }
                for bundle in bundles
            ],
        }
        atomic_write_json(self.settings.reports_dir / "summary.json", summary, pretty=True)
        return {
            "ok": True,
            "summary": summary,
            "run_bundle_path": str(self.settings.reports_dir / "run_bundle.json"),
            "summary_path": str(self.settings.reports_dir / "summary.json"),
            "summary_sha256": package["summary_sha256"],
        }

    def diagnose_frame(self, sequence_id: str, frame_index: int) -> dict[str, Any]:
        sequence = self.sequence(sequence_id)
        frame_index = int(frame_index)
        if frame_index < 0 or frame_index >= len(sequence["frames"]):
            raise_error(
                E_INVALID_STAGE,
                "diagnostic frame is outside the sequence",
                context={"sequence_id": sequence_id, "frame_index": frame_index, "frame_count": len(sequence["frames"])},
            )
        ready, artifact = self._appearance_stage_ready(sequence)
        if not ready or artifact is None:
            raise_error(
                E_INVALID_STAGE,
                "frame diagnostics require a current appearance stage",
                context={"sequence_id": sequence_id, "frame_index": frame_index, "required_stage": "appearance"},
            )
        scan = self.store.scan(sequence_id)
        if frame_index > scan.count - 1:
            raise_error(
                E_INVALID_STAGE,
                "diagnostic request skipped prerequisite frame states",
                context={
                    "sequence_id": sequence_id,
                    "frame_index": frame_index,
                    "valid_frames": scan.count,
                    "required_previous_frame": frame_index - 1 if frame_index else None,
                    "next_frame_index": scan.next_frame_index,
                },
            )
        record = scan.records[frame_index]
        if frame_index > 0 and scan.records[frame_index - 1]["state_digest"] != record["previous_state_digest"]:
            raise_error(
                E_INVALID_STAGE,
                "diagnostic prerequisite state chain is not contiguous",
                context={"sequence_id": sequence_id, "frame_index": frame_index},
            )
        frame = self.manifest.load_frame(sequence, frame_index)
        height, width = frame.shape
        return {
            "ok": True,
            "sequence_id": sequence_id,
            "frame_index": frame_index,
            "image_shape": [int(height), int(width)],
            "reference_truth_box_xywh_norm": sequence["frames"][frame_index]["box_xywh_norm"],
            "initial_box_xywh_norm": sequence["initial_box"],
            "appearance": artifact["records"][frame_index],
            "candidate": record["candidate_record"],
            "trajectory": record["tracking_record"],
            "state": record["state"],
            "sources": record["sources"],
            "stage_fingerprints": record["stage_fingerprints"],
            "checkpoint_ordinal": record["ordinal"],
            "checkpoint_digest": record["checkpoint_digest"],
            "previous_state_digest": record["previous_state_digest"],
        }

    def frame_payload(self, sequence_id: str, frame_index: int) -> dict[str, Any]:
        import base64

        sequence = self.sequence(sequence_id)
        frame_index = int(frame_index)
        if frame_index < 0 or frame_index >= len(sequence["frames"]):
            raise_error(
                E_NOT_FOUND,
                "frame is outside the sequence",
                context={"sequence_id": sequence_id, "frame_index": frame_index},
            )
        frame = self.manifest.load_frame(sequence, frame_index)
        scan = self.store.scan(sequence_id)
        record = scan.records[frame_index] if frame_index < scan.count else None
        return {
            "ok": True,
            "sequence_id": sequence_id,
            "frame_index": frame_index,
            "shape": [int(v) for v in frame.shape],
            "dtype": "uint8",
            "pixels_base64": base64.b64encode(frame.tobytes(order="C")).decode("ascii"),
            "reference_truth_box_xywh_norm": sequence["frames"][frame_index]["box_xywh_norm"],
            "tracked_box_xywh_norm": record["tracking_record"]["box_xywh_norm"] if record else None,
            "occluded": record["tracking_record"]["occluded"] if record else None,
        }

    def invalidations(self, sequence_id: str | None = None) -> dict[str, Any]:
        if sequence_id is not None:
            self.sequence(sequence_id)
        return {
            "ok": True,
            "invalidations": list_invalidation_events(self.settings, sequence_id),
            "diagnostics": __import__("tracklab.diagnostics", fromlist=["list_error_diagnostics"]).list_error_diagnostics(self.settings),
        }

"""Rolling execution, stop control, invalidation assessment, and reports."""

from __future__ import annotations

import threading
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

from .checkpoints import CheckpointScan, CheckpointStore, finalize_record
from .errors import invalid_state
from .inputs import SeriesData
from .model import initial_state_record, transition_state
from .render import mean_absolute_error, render_frame, summarize_frame
from .util import (
    atomic_write_bytes,
    digest_json,
    pretty_json_bytes,
    safe_component,
    state_digest,
)


class StopController:
    """Cooperative stop flag; callers poll it between indivisible steps."""

    def __init__(self) -> None:
        self._event = threading.Event()

    def request(self) -> None:
        self._event.set()

    def clear(self) -> None:
        self._event.clear()

    def is_requested(self) -> bool:
        return self._event.is_set()


@dataclass
class PrefixAssessment:
    valid_records: list[dict[str, Any]]
    rebuild_from_step: int | None
    reasons: list[dict[str, Any]]

    @property
    def last_valid_step(self) -> int:
        return int(self.valid_records[-1]["step_index"]) if self.valid_records else 0


def _stage_reason(stage: str, previous: Any, current: Any, step: int) -> dict[str, Any]:
    labels = {
        "base_frame": "canonical base frame content changed",
        "truth_frame": "truth frame content changed",
        "truth_frame_index": "timeline alignment or selected truth frame changed",
        "initial_state": "initial state changed",
        "dynamics": "velocity update or direction event changed",
        "deformation": "deformation evolution changed",
        "timeline": "timeline origin or stride changed",
        "render": "render parameters changed",
        "expected_ranges": "expected state range for this step changed",
    }
    return {
        "stage": stage,
        "message": labels.get(stage, f"step input {stage} changed"),
        "step": step,
        "previous_fingerprint": previous,
        "current_fingerprint": current,
    }


def assess_prefix(series_data: SeriesData, scan: CheckpointScan) -> PrefixAssessment:
    valid: list[dict[str, Any]] = []
    reasons: list[dict[str, Any]] = []
    rebuild_from: int | None = None
    prediction_steps = series_data.prediction_steps

    for step in range(1, prediction_steps + 1):
        record = scan.records.get(step)
        if record is None:
            if (
                scan.max_valid_step >= step
                or scan.duplicate_sequences
                or scan.ignored_files
            ):
                rebuild_from = step
                reasons.append(
                    {
                        "stage": "checkpoint_integrity",
                        "message": "checkpoint is missing, incomplete, corrupt, or has a duplicate sequence",
                        "step": step,
                        "previous_fingerprint": None,
                        "current_fingerprint": series_data.step_plans[step - 1].step_fingerprint,
                        "ignored_files": scan.ignored_files,
                        "duplicate_sequences": scan.duplicate_sequences,
                    }
                )
            break
        plan = series_data.step_plans[step - 1]
        if record.get("step_input_fingerprint") != plan.step_fingerprint:
            rebuild_from = step
            previous_inputs = record.get("input_fingerprints", {})
            for key in sorted(set(previous_inputs) | set(plan.step_inputs)):
                previous = previous_inputs.get(key)
                current = plan.step_inputs.get(key)
                if previous != current:
                    reasons.append(_stage_reason(key, previous, current, step))
            if not reasons:
                reasons.append(
                    _stage_reason(
                        "step_input",
                        record.get("step_input_fingerprint"),
                        plan.step_fingerprint,
                        step,
                    )
                )
            break
        valid.append(record)

    if rebuild_from is None and scan.max_valid_step > prediction_steps:
        rebuild_from = prediction_steps + 1
        reasons.append(
            {
                "stage": "prediction_steps",
                "message": "prediction step count decreased; downstream checkpoints are stale",
                "step": prediction_steps + 1,
                "previous_fingerprint": scan.records.get(scan.max_valid_step, {}).get(
                    "step_input_fingerprint"
                ),
                "current_fingerprint": None,
            }
        )

    if rebuild_from is None and (scan.duplicate_sequences or scan.ignored_files):
        # Damage only in the already-unpublished suffix still invalidates that suffix.
        rebuild_from = len(valid) + 1
        reasons.append(
            {
                "stage": "checkpoint_integrity",
                "message": "untrusted checkpoint files were ignored",
                "step": rebuild_from,
                "previous_fingerprint": None,
                "current_fingerprint": None,
                "ignored_files": scan.ignored_files,
                "duplicate_sequences": scan.duplicate_sequences,
            }
        )

    return PrefixAssessment(
        valid_records=valid,
        rebuild_from_step=rebuild_from,
        reasons=reasons,
    )


def _write_invalidation(
    workspace_root: Path,
    series_data: SeriesData,
    assessment: PrefixAssessment,
    scan: CheckpointScan,
) -> dict[str, Any]:
    previous_version = None
    if assessment.valid_records:
        previous_version = assessment.valid_records[-1].get("version_fingerprint")
    elif scan.records:
        previous_version = scan.records[max(scan.records)].get("version_fingerprint")
    payload = {
        "schema_version": 1,
        "series_id": series_data.series_id,
        "rebuild_from_step": assessment.rebuild_from_step,
        "previous_valid_step": assessment.last_valid_step,
        "previous_version_fingerprint": previous_version,
        "current_version_fingerprint": series_data.version_fingerprint,
        "reasons": assessment.reasons,
        "checkpoint_issues": scan.issues,
    }
    path = (
        workspace_root
        / "invalidations"
        / f"{safe_component(series_data.series_id)}.json"
    )
    atomic_write_bytes(path, pretty_json_bytes(payload))
    return payload


def _build_record(
    series_data: SeriesData,
    step: int,
    state: dict[str, Any],
    frame_summary: dict[str, Any],
    error_mae: float,
    clamp_events: list[dict[str, Any]],
) -> dict[str, Any]:
    plan = series_data.step_plans[step - 1]
    summary_digest = state_digest(state)
    payload = {
        "schema_version": 1,
        "record_type": "motion-step",
        "series_id": series_data.series_id,
        "step_index": step,
        "sequence": step,
        "truth_frame_index": plan.truth_frame_index,
        "manifest_config_fingerprint": series_data.manifest_config_fingerprint,
        "input_frame_file_fingerprint": series_data.frame_file_sha256,
        "input_frame_content_fingerprint": series_data.frame_content_fingerprint,
        "input_truth_frame_fingerprint": series_data.frame_hashes[plan.truth_frame_index],
        "input_fingerprints": plan.step_inputs,
        "step_input_fingerprint": plan.step_fingerprint,
        "version_fingerprint": series_data.version_fingerprint,
        "state": state,
        "state_digest": summary_digest,
        "state_summary": {
            "digest": summary_digest,
            "step": step,
            "components": {
                key: state[key]
                for key in ("x", "y", "vx", "vy", "deformation")
            },
        },
        "frame_summary": frame_summary,
        "error": {"mae": error_mae},
        "clamp_events": clamp_events,
    }
    return finalize_record(payload)


def build_report(series_data: SeriesData, records: list[dict[str, Any]]) -> dict[str, Any]:
    steps = [
        {
            "step": record["step_index"],
            "sequence": record["sequence"],
            "truth_frame_index": record["truth_frame_index"],
            "state": record["state"],
            "state_digest": record["state_digest"],
            "frame_summary": record["frame_summary"],
            "error": record["error"],
            "clamp_events": record["clamp_events"],
            "step_input_fingerprint": record["step_input_fingerprint"],
        }
        for record in records
    ]
    state_trajectory = [record["state"] for record in records]
    error_curve = [float(record["error"]["mae"]) for record in records]
    frame_summaries = [record["frame_summary"] for record in records]
    # Use the language's ordered accumulator so the published aggregate is exactly
    # reproducible by consumers summing the emitted per-step curve.
    sum_mae = sum(error_curve)
    clamp_events = [
        event for record in records for event in record.get("clamp_events", [])
    ]
    complete = len(records) == series_data.prediction_steps and all(
        record["step_index"] == step
        for step, record in enumerate(records, start=1)
    )
    core = {
        "schema_version": 1,
        "series_id": series_data.series_id,
        "model_version": series_data.stage_fingerprints["model_version"],
        "prediction_steps": series_data.prediction_steps,
        "timeline": series_data.series["timeline"],
        "complete": complete,
        "checkpoint": {
            "count": len(records),
            "first_sequence": records[0]["sequence"] if records else None,
            "last_sequence": records[-1]["sequence"] if records else None,
            "sequences": [record["sequence"] for record in records],
        },
        "version_fingerprints": {
            "current": series_data.version_fingerprint,
            "stages": series_data.stage_fingerprints,
            "checkpoint": records[-1].get("version_fingerprint") if records else None,
        },
        "input_fingerprints": {
            "frame_file": series_data.frame_file_sha256,
            "frame_content": series_data.frame_content_fingerprint,
            "manifest_config": series_data.manifest_config_fingerprint,
            "base_frame": series_data.base_frame_fingerprint,
        },
        "steps": steps,
        "aggregate": {
            "step_count": len(records),
            "sum_mae": sum_mae,
            "mean_mae": (sum_mae / len(records)) if records else None,
            "clamp_count": len(clamp_events),
            "final_state": records[-1]["state"] if records else initial_state_record(series_data.series),
            "state_trajectory_digest": digest_json(state_trajectory),
            "frame_summary_digest": digest_json(frame_summaries),
            "error_curve_digest": digest_json(error_curve),
        },
        "error_curve": error_curve,
        "clamp_events": clamp_events,
    }
    return {**core, "report_fingerprint": digest_json(core)}


def report_bytes(report: dict[str, Any]) -> bytes:
    # Workflow envelope fields (performed steps, stop flag) are deliberately not
    # part of the byte-comparable domain report.
    payload = {key: value for key, value in report.items() if key != "run"}
    return pretty_json_bytes(payload)


def run_series(
    workspace_root: str | Path,
    series_data: SeriesData,
    *,
    max_steps: int | None = None,
    stop_controller: StopController | None = None,
    from_step: int | None = None,
    on_step: Callable[[dict[str, Any]], None] | None = None,
) -> dict[str, Any]:
    if max_steps is not None and max_steps < 0:
        raise invalid_state(
            "max_steps must be non-negative",
            series_id=series_data.series_id,
            position="max_steps",
        )
    store = CheckpointStore(workspace_root)
    scan = store.scan(series_data.series_id)
    if from_step is not None:
        store.assert_resume_step(series_data.series_id, from_step)

    assessment = assess_prefix(series_data, scan)
    if assessment.rebuild_from_step is not None:
        _write_invalidation(Path(workspace_root), series_data, assessment, scan)
        store.invalidate_from(series_data.series_id, assessment.rebuild_from_step)
        scan = store.scan(series_data.series_id)
        assessment = assess_prefix(series_data, scan)

    previous_state = (
        assessment.valid_records[-1]["state"]
        if assessment.valid_records
        else initial_state_record(series_data.series)
    )
    start_step = assessment.last_valid_step + 1
    performed = 0
    stopped = False
    controller = stop_controller or StopController()

    for step in range(start_step, series_data.prediction_steps + 1):
        if controller.is_requested():
            stopped = True
            break
        if max_steps is not None and performed >= max_steps:
            stopped = True
            break

        state, clamp_events = transition_state(series_data.series, previous_state, step)
        prediction = render_frame(
            series_data.frames[0],
            series_data.initial_state,
            state,
            series_data.render,
        )
        truth = series_data.truth_frame(step)
        mae = mean_absolute_error(prediction, truth)
        frame_summary = summarize_frame(prediction)
        record = _build_record(
            series_data, step, state, frame_summary, mae, clamp_events
        )
        store.append(record)
        previous_state = state
        performed += 1
        if on_step is not None:
            on_step(record)

    final_scan = store.scan(series_data.series_id)
    valid_records = final_scan.prefix[: series_data.prediction_steps]
    report = build_report(series_data, valid_records)
    report["run"] = {
        "performed_steps": performed,
        "started_at_step": start_step,
        "stopped": stopped,
        "stop_requested": controller.is_requested(),
        "complete": performed + start_step - 1 == series_data.prediction_steps,
    }
    # The run section is intentionally excluded from report_fingerprint; it is the
    # only non-deterministic-by-workflow observation in the returned envelope.
    return report


def load_invalidation(workspace_root: str | Path, series_id: str) -> dict[str, Any] | None:
    path = (
        Path(workspace_root)
        / "invalidations"
        / f"{safe_component(series_id)}.json"
    )
    if not path.is_file():
        return None
    import json

    try:
        with path.open("rb") as handle:
            value = json.load(handle)
    except (OSError, ValueError, UnicodeDecodeError):
        return None
    return value if isinstance(value, dict) else None

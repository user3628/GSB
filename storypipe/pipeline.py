from __future__ import annotations

import fcntl
import hashlib
import os
import shutil
import tempfile
from collections import OrderedDict
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Callable, Iterator

import numpy as np
from PIL import Image

from .cache_policy import frame_nbytes, select_cache_policy
from .canonical import (
    atomic_write_bytes,
    atomic_write_json,
    canonical_bytes,
    digest_file,
    digest_value,
    read_json,
)
from .checkpoints import STAGES, CheckpointStore
from .errors import PipelineError, StopRequested
from .fingerprints import STAGE_VERSIONS, build_fingerprints, changed_components
from .frame_provider import ComposedFrameProvider, load_rgb_image
from .models import Shot
from .motion import blend_frames, transform_matrix
from .schedule import build_schedule
from .stage_io import (
    build_tree_digest,
    float_frame_bytes,
    float_frame_sha256,
    frame_stats,
    make_build_dir,
    publish_directory,
    quantize,
    read_float_frame,
    safe_slug,
    stage_directory,
    validate_artifact,
    write_jsonl,
    write_manifest,
)
from .transition import build_transition_plan, frame_blocks
from .workspace import InputBundle, load_bundle


UnitObserver = Callable[[str, int, str], None]


class PipelineRunner:
    def __init__(
        self,
        workspace: str | Path,
        *,
        unit_observer: UnitObserver | None = None,
    ) -> None:
        self.workspace = Path(workspace).resolve()
        self.checkpoints = CheckpointStore(self.workspace)
        self.unit_observer = unit_observer
        self.stop_path = self.workspace / "control" / "stop.json"

    def request_stop(self, reason: str = "operator request") -> dict[str, Any]:
        payload = {"requested": True, "reason": reason}
        atomic_write_json(self.stop_path, payload)
        return payload

    def resume(self) -> dict[str, Any]:
        if self.stop_path.exists():
            self.stop_path.unlink()
            directory_fd = os.open(self.stop_path.parent, os.O_RDONLY)
            try:
                os.fsync(directory_fd)
            finally:
                os.close(directory_fd)
        return {"requested": False}

    def _stop_requested(self) -> bool:
        return self.stop_path.exists()

    def _check_stop(self, stage: str, completed_units: int) -> None:
        if self._stop_requested():
            raise StopRequested(stage, completed_units)

    def _after_unit(self, stage: str, unit_index: int, shot_id: str) -> None:
        if self.unit_observer is not None:
            self.unit_observer(stage, unit_index, shot_id)
        self._check_stop(stage, unit_index + 1)

    @contextmanager
    def _run_lock(self) -> Iterator[None]:
        self.workspace.mkdir(parents=True, exist_ok=True)
        lock_path = self.workspace / ".pipeline.lock"
        with lock_path.open("a+b") as handle:
            try:
                fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError as exc:
                raise PipelineError(
                    "E_PIPELINE_BUSY", "another pipeline run owns this workspace"
                ) from exc
            try:
                yield
            finally:
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)

    def _reset_artifacts(self) -> None:
        for path in (
            self.workspace / "checkpoints",
            self.workspace / "stages",
            self.workspace / "state",
            self.workspace / "algorithm_plan.json",
        ):
            if path.is_dir():
                shutil.rmtree(path)
            elif path.exists():
                path.unlink()
        self.resume()

    def _image_dimensions(self, bundle: InputBundle, stage: str = "compose") -> tuple[int, int]:
        dimensions: tuple[int, int] | None = None
        for shot in bundle.shots:
            path = bundle.image_dir / shot.image
            if not path.exists() or not path.is_file():
                raise PipelineError(
                    "E_MISSING_ASSET",
                    f"image not found: {shot.image}",
                    stage=stage,
                    shot_id=shot.shot_id,
                    details={"path": str(path)},
                )
            try:
                with Image.open(path) as image:
                    image.verify()
                with Image.open(path) as image:
                    current = (int(image.width), int(image.height))
            except Exception as exc:
                raise PipelineError(
                    "E_MISSING_ASSET",
                    f"image cannot be decoded: {shot.image}",
                    stage=stage,
                    shot_id=shot.shot_id,
                    details={"error": str(exc)},
                ) from exc
            if dimensions is None:
                dimensions = current
            elif current != dimensions:
                raise PipelineError(
                    "E_INVALID_ASSET",
                    "all images must share identical dimensions",
                    stage=stage,
                    shot_id=shot.shot_id,
                    details={"expected": list(dimensions), "actual": list(current)},
                )
        if dimensions is None:
            raise PipelineError("E_INVALID_SHOT", "no shots found", stage=stage)
        return dimensions

    def _prepare(self, *, strict_assets: bool) -> dict[str, Any]:
        bundle = load_bundle(self.workspace)
        schedule = build_schedule(bundle.shots, bundle.config)
        input_error: PipelineError | None = None
        try:
            width, height = self._image_dimensions(bundle, stage="compose")
        except PipelineError as exc:
            if strict_assets:
                raise
            input_error = exc
            width, height = 1, 1
        cache_plan = select_cache_policy(
            shot_count=len(bundle.shots),
            total_frames=len(schedule["frames"]),
            width=width,
            height=height,
            max_shot_frames=max(shot.duration_frames for shot in bundle.shots),
            config=bundle.config,
        )
        fingerprints = build_fingerprints(bundle, schedule, cache_plan)
        return {
            "bundle": bundle,
            "schedule": schedule,
            "cache_plan": cache_plan,
            "fingerprints": fingerprints,
            "input_error": input_error,
            "dimensions": (width, height),
        }

    def _statuses(
        self, prepared: dict[str, Any], records: dict[str, dict[str, Any] | None]
    ) -> dict[str, Any]:
        stage_status: dict[str, Any] = {}
        invalid_seen = False
        earliest_invalid: str | None = None
        current_stages = prepared["fingerprints"]["stages"]
        components = prepared["fingerprints"]["components"]
        for stage in STAGES:
            record = records[stage]
            current_fp = current_stages[stage]
            artifact_ok = bool(
                record
                and validate_artifact(
                    self.workspace,
                    record.get("artifact_path"),
                    record.get("artifact_digest"),
                )
            )
            if record is None:
                status = "pending"
                reason = "no checkpoint has been published"
                changed = changed_components(None, components[stage])
            elif record.get("input_fingerprint") != current_fp:
                status = "stale"
                changed = changed_components(record.get("components"), components[stage])
                reason = "stage input fingerprint changed"
            elif not artifact_ok:
                status = "stale"
                changed = ["artifact"]
                reason = "checkpoint artifact is missing or its digest differs"
            elif invalid_seen:
                status = "blocked"
                changed = []
                reason = "an upstream stage is invalid"
            else:
                status = "complete"
                changed = []
                reason = None
            if status != "complete" and earliest_invalid is None:
                earliest_invalid = stage
            if status != "complete":
                invalid_seen = True
            info = {
                "status": status,
                "input_fingerprint": current_fp,
                "changed_components": changed,
                "reason": reason,
                "artifact_valid": artifact_ok,
            }
            if record is not None:
                info.update(
                    {
                        "checkpoint_sequence": record.get("sequence"),
                        "checkpoint_fingerprint": record.get("input_fingerprint"),
                        "artifact_digest": record.get("artifact_digest"),
                        "artifact_path": record.get("artifact_path"),
                    }
                )
            stage_status[stage] = info
        downstream = (
            list(STAGES[STAGES.index(earliest_invalid) + 1 :])
            if earliest_invalid is not None
            else []
        )
        return {
            "stages": stage_status,
            "earliest_invalid_stage": earliest_invalid,
            "invalidated_stages": downstream,
        }

    def inspect(self) -> dict[str, Any]:
        try:
            prepared = self._prepare(strict_assets=False)
        except PipelineError as exc:
            return {
                "ok": False,
                "workspace": str(self.workspace),
                "error": exc.to_dict(),
                "stop_requested": self._stop_requested(),
            }
        records = self.checkpoints.read_all()
        status = self._statuses(prepared, records)
        return {
            "ok": True,
            "workspace": str(self.workspace),
            "stop_requested": self._stop_requested(),
            "overall_fingerprint": prepared["fingerprints"]["overall_fingerprint"],
            "stage_fingerprints": prepared["fingerprints"]["stages"],
            "timeline_digest": prepared["fingerprints"]["timeline_digest"],
            "cache_strategy": prepared["cache_plan"]["strategy"],
            "cache_plan": prepared["cache_plan"],
            "stages": status["stages"],
            "earliest_invalid_stage": status["earliest_invalid_stage"],
            "invalidated_stages": status["invalidated_stages"],
            "input_error": prepared["input_error"].to_dict()
            if prepared["input_error"] is not None
            else None,
        }

    def run(
        self,
        *,
        fresh: bool = False,
        stop_after_units: int | None = None,
    ) -> dict[str, Any]:
        with self._run_lock():
            if fresh:
                self._reset_artifacts()
            prepared = self._try_prepare_for_run()
            bundle: InputBundle = prepared["bundle"]
            schedule: dict[str, Any] = prepared["schedule"]
            cache_plan: dict[str, Any] = prepared["cache_plan"]
            fingerprints: dict[str, Any] = prepared["fingerprints"]
            self._write_algorithm_plan(cache_plan)
            completed_units: dict[str, int] = {stage: 0 for stage in STAGES}
            try:
                for stage in STAGES:
                    self._check_stop(stage, completed_units[stage])
                    current_fp = fingerprints["stages"][stage]
                    record = self.checkpoints.read(stage)
                    if record is not None and record.get("input_fingerprint") == current_fp:
                        if validate_artifact(
                            self.workspace,
                            record.get("artifact_path"),
                            record.get("artifact_digest"),
                        ):
                            continue
                    if stage == "schedule":
                        self._run_schedule(bundle, schedule, current_fp, fingerprints, cache_plan)
                    elif stage == "compose":
                        if prepared["input_error"] is not None:
                            raise prepared["input_error"]
                        completed_units[stage] = self._run_compose(
                            bundle,
                            schedule,
                            cache_plan,
                            fingerprints,
                            current_fp,
                            stop_after_units,
                        )
                    elif stage == "transition":
                        completed_units[stage] = self._run_transition(
                            bundle,
                            schedule,
                            cache_plan,
                            fingerprints,
                            current_fp,
                            stop_after_units,
                        )
                    elif stage == "encode":
                        completed_units[stage] = self._run_encode(
                            bundle,
                            schedule,
                            cache_plan,
                            fingerprints,
                            current_fp,
                            stop_after_units,
                        )
            except StopRequested as stopped:
                return self._stopped_result(stopped, fingerprints)
            return self.inspect()

    def _try_prepare_for_run(self) -> dict[str, Any]:
        bundle = load_bundle(self.workspace)
        schedule = build_schedule(bundle.shots, bundle.config)
        input_error: PipelineError | None = None
        try:
            width, height = self._image_dimensions(bundle, stage="compose")
        except PipelineError as exc:
            input_error = exc
            width, height = 1, 1
        cache_plan = select_cache_policy(
            shot_count=len(bundle.shots),
            total_frames=len(schedule["frames"]),
            width=width,
            height=height,
            max_shot_frames=max(shot.duration_frames for shot in bundle.shots),
            config=bundle.config,
        )
        return {
            "bundle": bundle,
            "schedule": schedule,
            "cache_plan": cache_plan,
            "fingerprints": build_fingerprints(bundle, schedule, cache_plan),
            "input_error": input_error,
            "dimensions": (width, height),
        }

    def _stopped_result(
        self, stopped: StopRequested, fingerprints: dict[str, Any]
    ) -> dict[str, Any]:
        status = self.inspect()
        return {
            "ok": True,
            "status": "stopped",
            "workspace": str(self.workspace),
            "stopped_at_stage": stopped.stage,
            "completed_units": stopped.completed_units,
            "resume_required": True,
            "overall_fingerprint": fingerprints["overall_fingerprint"],
            "stage_fingerprints": fingerprints["stages"],
            "stage_status": status.get("stages", {}),
            "earliest_invalid_stage": status.get("earliest_invalid_stage"),
        }

    def _write_algorithm_plan(self, cache_plan: dict[str, Any]) -> None:
        atomic_write_json(self.workspace / "algorithm_plan.json", cache_plan)

    def _publish(
        self,
        *,
        stage: str,
        current_fp: str,
        artifact_path: Path,
        components: dict[str, Any],
        bundle: InputBundle,
        extra: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        artifact_digest = digest_file(artifact_path)
        existing = self.checkpoints.read(stage)
        if (
            existing is not None
            and existing.get("input_fingerprint") == current_fp
            and existing.get("artifact_digest") == artifact_digest
        ):
            return existing
        return self.checkpoints.publish(
            stage=stage,
            input_fingerprint=current_fp,
            config_version=bundle.config["config_version"],
            artifact_digest=artifact_digest,
            artifact_path=artifact_path.relative_to(self.workspace).as_posix(),
            components=components,
            extra={"stage_version": STAGE_VERSIONS[stage], **(extra or {})},
        )

    def _run_schedule(
        self,
        bundle: InputBundle,
        schedule: dict[str, Any],
        current_fp: str,
        fingerprints: dict[str, Any],
        cache_plan: dict[str, Any],
    ) -> None:
        directory = stage_directory(self.workspace, "schedule")
        path = directory / "schedule.json"
        directory.mkdir(parents=True, exist_ok=True)
        atomic_write_json(path, schedule)
        self._publish(
            stage="schedule",
            current_fp=current_fp,
            artifact_path=path,
            components=fingerprints["components"]["schedule"],
            bundle=bundle,
            extra={"timeline_digest": schedule["timeline_digest"]},
        )

    def _unit_path(self, stage: str, index: int, shot_id: str) -> Path:
        return stage_directory(self.workspace, stage) / "units" / f"{index:04d}_{safe_slug(shot_id)}.json"

    def _load_unit(
        self,
        path: Path,
        *,
        current_fp: str,
        shot_id: str,
        raw_path: Path | None = None,
    ) -> dict[str, Any] | None:
        if not path.exists():
            return None
        try:
            unit = read_json(path)
        except Exception:
            return None
        if not isinstance(unit, dict):
            return None
        if unit.get("input_fingerprint") != current_fp or unit.get("shot_id") != shot_id:
            return None
        if raw_path is not None:
            if not raw_path.exists():
                return None
            if unit.get("file_digest") != digest_file(raw_path):
                return None
        return unit

    def _run_compose(
        self,
        bundle: InputBundle,
        schedule: dict[str, Any],
        cache_plan: dict[str, Any],
        fingerprints: dict[str, Any],
        current_fp: str,
        stop_after_units: int | None,
    ) -> int:
        directory = stage_directory(self.workspace, "compose")
        directory.mkdir(parents=True, exist_ok=True)
        provider = ComposedFrameProvider(
            shots=bundle.shots_by_id,
            image_dir=bundle.image_dir,
            config=bundle.config,
            cache_plan=cache_plan,
            stage="compose",
        )
        width, height = self._image_dimensions(bundle, stage="compose")
        schedule_frames = {
            (frame["shot_id"], frame["local_frame"]): frame for frame in schedule["frames"]
        }
        unit_records: list[dict[str, Any]] = []
        reused = 0
        for index, shot in enumerate(bundle.shots):
            self._check_stop("compose", index)
            path = self._unit_path("compose", index, shot.shot_id)
            unit = self._load_unit(path, current_fp=current_fp, shot_id=shot.shot_id)
            if unit is None:
                frames = []
                for local_frame in range(shot.duration_frames):
                    frame = provider.get(shot.shot_id, local_frame)
                    matrix = transform_matrix(shot, local_frame, width, height, bundle.config)
                    global_record = schedule_frames[(shot.shot_id, local_frame)]
                    frames.append(
                        {
                            "global_frame": global_record["global_frame"],
                            "shot_id": shot.shot_id,
                            "local_frame": local_frame,
                            "matrix": matrix,
                            "matrix_digest": digest_value(matrix),
                            "float_sha256": float_frame_sha256(frame),
                        }
                    )
                unit = {
                    "format": "compose-unit-v1",
                    "input_fingerprint": current_fp,
                    "source_index": index,
                    "shot_id": shot.shot_id,
                    "image": shot.image,
                    "image_digest": digest_file(bundle.image_dir / shot.image),
                    "camera_move": shot.camera_move,
                    "duration_frames": shot.duration_frames,
                    "frames": frames,
                }
                atomic_write_json(path, unit)
            else:
                reused += 1
            unit_records.append(unit)
            self._after_unit("compose", index, shot.shot_id)
            if stop_after_units is not None and index + 1 >= stop_after_units:
                self.request_stop("stop_after_units")
        manifest = {
            "format": "compose-manifest-v1",
            "stage": "compose",
            "input_fingerprint": current_fp,
            "timeline_digest": schedule["timeline_digest"],
            "cache_plan": cache_plan,
            "shots": [
                {
                    "source_index": unit["source_index"],
                    "shot_id": unit["shot_id"],
                    "image": unit["image"],
                    "image_digest": unit["image_digest"],
                    "camera_move": unit["camera_move"],
                    "duration_frames": unit["duration_frames"],
                    "frame_count": len(unit["frames"]),
                }
                for unit in unit_records
            ],
            "frames": [frame for unit in unit_records for frame in unit["frames"]],
        }
        path = directory / "compose.json"
        write_manifest(path, manifest)
        self._write_execution_stats(
            "compose", {**provider.stats(), "units_reused": reused}
        )
        self._publish(
            stage="compose",
            current_fp=current_fp,
            artifact_path=path,
            components=fingerprints["components"]["compose"],
            bundle=bundle,
        )
        return len(bundle.shots)

    def _run_transition(
        self,
        bundle: InputBundle,
        schedule: dict[str, Any],
        cache_plan: dict[str, Any],
        fingerprints: dict[str, Any],
        current_fp: str,
        stop_after_units: int | None,
    ) -> int:
        directory = stage_directory(self.workspace, "transition")
        directory.mkdir(parents=True, exist_ok=True)
        frames_dir = directory / "frames"
        frames_dir.mkdir(parents=True, exist_ok=True)
        plan = build_transition_plan(bundle.shots, schedule, bundle.config)
        blocks = frame_blocks(plan, schedule)
        provider = ComposedFrameProvider(
            shots=bundle.shots_by_id,
            image_dir=bundle.image_dir,
            config=bundle.config,
            cache_plan=cache_plan,
            stage="transition",
        )
        reused = 0
        block_records: list[dict[str, Any]] = []
        for index, block in enumerate(blocks):
            self._check_stop("transition", index)
            filename = f"block_{index:04d}_{safe_slug(block['shot_id'])}.f32"
            raw_path = frames_dir / filename
            unit_path = self._unit_path("transition", index, block["shot_id"])
            unit = self._load_unit(
                unit_path, current_fp=current_fp, shot_id=block["shot_id"], raw_path=raw_path
            )
            if unit is None:
                unit = self._write_transition_block(
                    block, raw_path, filename, provider, current_fp
                )
                atomic_write_json(unit_path, unit)
            else:
                reused += 1
            block_records.append(unit)
            self._after_unit("transition", index, block["shot_id"])
            if stop_after_units is not None and index + 1 >= stop_after_units:
                self.request_stop("stop_after_units")
        manifest = {
            "format": "transition-manifest-v1",
            "stage": "transition",
            "input_fingerprint": current_fp,
            "timeline_digest": schedule["timeline_digest"],
            "transition_plan_digest": plan["transition_plan_digest"],
            "cache_plan": cache_plan,
            "boundaries": plan["boundaries"],
            "total_frames": plan["total_frames"],
            "blocks": [
                {
                    "block_index": unit["block_index"],
                    "shot_id": unit["shot_id"],
                    "global_frame_start": unit["global_frame_start"],
                    "global_frame_end": unit["global_frame_end"],
                    "raw_file": unit["raw_file"],
                    "file_digest": unit["file_digest"],
                    "frame_count": len(unit["frames"]),
                }
                for unit in block_records
            ],
            "frames": [frame for unit in block_records for frame in unit["frames"]],
        }
        path = directory / "transition.json"
        write_manifest(path, manifest)
        self._write_execution_stats(
            "transition", {**provider.stats(), "units_reused": reused}
        )
        self._publish(
            stage="transition",
            current_fp=current_fp,
            artifact_path=path,
            components=fingerprints["components"]["transition"],
            bundle=bundle,
        )
        return len(blocks)

    def _write_transition_block(
        self,
        block: dict[str, Any],
        raw_path: Path,
        filename: str,
        provider: ComposedFrameProvider,
        current_fp: str,
    ) -> dict[str, Any]:
        temp_fd, temp_name = tempfile.mkstemp(
            prefix=f".{raw_path.name}.", suffix=".tmp", dir=raw_path.parent
        )
        temp_path = Path(temp_name)
        frame_records: list[dict[str, Any]] = []
        frame_bytes = 0
        try:
            with os.fdopen(temp_fd, "wb") as handle:
                offset = 0
                for record in block["frames"]:
                    sources = []
                    for source in record["blend_sources"]:
                        sources.append(
                            (provider.get(source["shot_id"], source["local_frame"]), source["weight"])
                        )
                    frame = blend_frames(sources)
                    data = float_frame_bytes(frame)
                    if frame_bytes == 0:
                        frame_bytes = len(data)
                    handle.write(data)
                    output_record = {
                        "global_frame": record["global_frame"],
                        "source_shot": record["source_shot"],
                        "source_index": record["source_index"],
                        "local_frame": record["local_frame"],
                        "start_ms": record["start_ms"],
                        "end_ms": record["end_ms"],
                        "blend_sources": record["blend_sources"],
                        "transition": record["transition"],
                        "byte_offset": offset,
                        "float_sha256": float_frame_sha256(frame),
                    }
                    frame_records.append(output_record)
                    offset += len(data)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temp_path, raw_path)
            directory_fd = os.open(raw_path.parent, os.O_RDONLY)
            try:
                os.fsync(directory_fd)
            finally:
                os.close(directory_fd)
        except BaseException:
            try:
                temp_path.unlink()
            except FileNotFoundError:
                pass
            raise
        return {
            "format": "transition-unit-v1",
            "input_fingerprint": current_fp,
            "block_index": block["block_index"],
            "shot_id": block["shot_id"],
            "global_frame_start": block["global_frame_start"],
            "global_frame_end": block["global_frame_end"],
            "raw_file": f"frames/{filename}",
            "file_digest": digest_file(raw_path),
            "frame_bytes": frame_bytes,
            "frames": frame_records,
        }

    def _run_encode(
        self,
        bundle: InputBundle,
        schedule: dict[str, Any],
        cache_plan: dict[str, Any],
        fingerprints: dict[str, Any],
        current_fp: str,
        stop_after_units: int | None,
    ) -> int:
        transition_record = self.checkpoints.read("transition")
        if transition_record is None:
            raise PipelineError(
                "E_INVALID_STAGE", "transition must complete before encode", stage="encode"
            )
        transition_dir = stage_directory(self.workspace, "transition")
        transition_manifest_path = self.workspace / transition_record["artifact_path"]
        transition_manifest = read_json(transition_manifest_path)
        directory = stage_directory(self.workspace, "encode")
        self._recover_encode_directory(directory)
        build_dir = make_build_dir(directory, ".build-")
        frames_dir = build_dir / "frames"
        frames_dir.mkdir(parents=True, exist_ok=True)
        width, height = self._image_dimensions(bundle, stage="encode")
        frame_bytes = frame_nbytes(width, height)
        loaded_blocks: OrderedDict[int, np.ndarray] = OrderedDict()
        block_arrays: dict[int, np.ndarray]
        transition_bytes = 0
        peak_transition_bytes = 0
        try:
            if cache_plan["strategy"] == "full_cache":
                block_arrays = {}
                for block in transition_manifest["blocks"]:
                    array = self._read_transition_block(transition_dir, block, width, height)
                    transition_bytes += int(array.nbytes)
                    peak_transition_bytes = max(peak_transition_bytes, transition_bytes)
                    block_arrays[block["block_index"]] = array
            else:
                block_arrays = {}
            frame_summaries: list[dict[str, Any]] = []
            for block_index, block in enumerate(transition_manifest["blocks"]):
                self._check_stop("encode", block_index)
                raw_path = transition_dir / block["raw_file"]
                if digest_file(raw_path) != block["file_digest"]:
                    raise PipelineError(
                        "E_ARTIFACT_CORRUPT",
                        f"transition block changed: {block['raw_file']}",
                        stage="encode",
                        details={"block_index": block_index},
                    )
                if cache_plan["strategy"] == "full_cache":
                    array = block_arrays[block["block_index"]]
                elif cache_plan["strategy"] == "segment_cache":
                    array = self._read_transition_block(transition_dir, block, width, height)
                    loaded_blocks[block["block_index"]] = array
                    transition_bytes += int(array.nbytes)
                    peak_transition_bytes = max(peak_transition_bytes, transition_bytes)
                    while len(loaded_blocks) > 1:
                        _, evicted = loaded_blocks.popitem(last=False)
                        transition_bytes -= int(evicted.nbytes)
                else:
                    array = None
                block_frames = self._transition_frame_records(
                    transition_manifest, block["block_index"]
                )
                for local_index, frame_record in enumerate(block_frames):
                    if array is None:
                        frame = read_float_frame(
                            raw_path,
                            frame_record["byte_offset"],
                            frame_bytes,
                            width,
                            height,
                        )
                        peak_transition_bytes = max(peak_transition_bytes, int(frame.nbytes))
                    else:
                        frame = array[local_index]
                    quantized = quantize(frame)
                    if quantized.shape[:2] != (height, width):
                        raise PipelineError(
                            "E_ARTIFACT_CORRUPT",
                            "transition frame dimensions do not match input images",
                            stage="encode",
                        )
                    filename = f"frame_{frame_record['global_frame']:08d}.png"
                    png_path = frames_dir / filename
                    image = Image.fromarray(quantized)
                    image.save(
                        png_path,
                        format="PNG",
                        compress_level=int(bundle.config["encode"]["compression"]),
                        optimize=False,
                    )
                    pixel = frame_stats(quantized)
                    frame_summaries.append(
                        {
                            "global_frame": frame_record["global_frame"],
                            "source_shot": frame_record["source_shot"],
                            "local_frame": frame_record["local_frame"],
                            "start_ms": frame_record["start_ms"],
                            "end_ms": frame_record["end_ms"],
                            "transition": frame_record["transition"],
                            "blend_sources": frame_record["blend_sources"],
                            "file": f"frames/{filename}",
                            "width": width,
                            "height": height,
                            "pixel_sha256": pixel["pixel_sha256"],
                            "mean_rgb": pixel["mean_rgb"],
                            "png_sha256": digest_file(png_path),
                        }
                    )
                self._after_unit("encode", block_index, block["shot_id"])
                if stop_after_units is not None and block_index + 1 >= stop_after_units:
                    self.request_stop("stop_after_units")
            summaries_path = build_dir / "frame_summaries.jsonl"
            write_jsonl(summaries_path, frame_summaries)
            output_tree_digest = build_tree_digest(
                list(frames_dir.glob("*.png")) + [summaries_path], build_dir
            )
            stats = self._output_statistics(schedule, frame_summaries, transition_manifest)
            deterministic_summary = digest_value(
                {
                    "timeline_digest": schedule["timeline_digest"],
                    "transition_plan_digest": transition_manifest["transition_plan_digest"],
                    "frames": frame_summaries,
                    "statistics": stats,
                }
            )
            manifest = {
                "format": "encoded-sequence-manifest-v1",
                "stage": "encode",
                "input_fingerprint": current_fp,
                "overall_fingerprint": fingerprints["overall_fingerprint"],
                "timeline_digest": schedule["timeline_digest"],
                "transition_plan_digest": transition_manifest["transition_plan_digest"],
                "cache_strategy": cache_plan["strategy"],
                "width": width,
                "height": height,
                "frame_count": len(frame_summaries),
                "frames": frame_summaries,
                "statistics": stats,
                "output_tree_digest": output_tree_digest,
                "deterministic_summary_sha256": deterministic_summary,
            }
            manifest_path = build_dir / "manifest.json"
            atomic_write_json(manifest_path, manifest)
            output_dir = directory / "output"
            publish_directory(build_dir, output_dir)
        except BaseException:
            if build_dir.exists():
                shutil.rmtree(build_dir)
            raise
        final_manifest = directory / "output" / "manifest.json"
        self._write_execution_stats(
            "encode",
            {
                "strategy": cache_plan["strategy"],
                "peak_transition_buffer_bytes": peak_transition_bytes,
                "memory_budget_bytes": cache_plan["inputs"]["memory_budget_bytes"],
                "budget_respected": peak_transition_bytes
                <= cache_plan["inputs"]["memory_budget_bytes"],
            },
        )
        self._publish(
            stage="encode",
            current_fp=current_fp,
            artifact_path=final_manifest,
            components=fingerprints["components"]["encode"],
            bundle=bundle,
            extra={"deterministic_summary_sha256": deterministic_summary},
        )
        return len(transition_manifest["blocks"])

    def _recover_encode_directory(self, directory: Path) -> None:
        output_dir = directory / "output"
        backups = sorted(directory.glob(".output.old-*"))
        if not output_dir.exists() and backups:
            backups[-1].replace(output_dir)
            backups = backups[:-1]
        for backup in backups:
            if backup.exists():
                shutil.rmtree(backup)
        for stale_build in directory.glob(".build-*"):
            if stale_build.is_dir():
                shutil.rmtree(stale_build)

    def _read_transition_block(
        self,
        transition_dir: Path,
        block: dict[str, Any],
        width: int,
        height: int,
    ) -> np.ndarray:
        path = transition_dir / block["raw_file"]
        expected = block["frame_count"] * frame_nbytes(width, height)
        if path.stat().st_size != expected:
            raise PipelineError(
                "E_ARTIFACT_CORRUPT",
                f"transition block has wrong size: {block['raw_file']}",
                stage="encode",
            )
        return np.fromfile(path, dtype="<f4").reshape(
            block["frame_count"], height, width, 3
        )

    def _transition_frame_records(
        self, manifest: dict[str, Any], block_index: int
    ) -> list[dict[str, Any]]:
        block = manifest["blocks"][block_index]
        start = block["global_frame_start"]
        end = block["global_frame_end"]
        return [frame for frame in manifest["frames"] if start <= frame["global_frame"] < end]

    def _output_statistics(
        self,
        schedule: dict[str, Any],
        frames: list[dict[str, Any]],
        transition_manifest: dict[str, Any],
    ) -> dict[str, Any]:
        camera_counts = {"static": 0, "pan": 0, "zoom": 0}
        for shot in schedule["shots"]:
            camera_counts[shot["camera_move"]] += shot["duration_frames"]
        transition_counts = {"hard_cut": 0, "crossfade": 0}
        for frame in frames:
            transition = frame.get("transition")
            if transition is not None:
                transition_counts[transition["type"]] += 1
        boundary_counts = {"hard_cut": 0, "crossfade": 0}
        for boundary in transition_manifest["boundaries"]:
            boundary_counts[boundary["type"]] += 1
        return {
            "total_frames": len(frames),
            "camera_move_frame_counts": camera_counts,
            "transition_frame_counts": transition_counts,
            "transition_boundary_counts": boundary_counts,
        }

    def _write_execution_stats(self, stage: str, stats: dict[str, Any]) -> None:
        atomic_write_json(self.workspace / "state" / "execution" / f"{stage}.json", stats)

    def export_manifest(self) -> dict[str, Any]:
        record = self.checkpoints.read("encode")
        if record is None:
            raise PipelineError(
                "E_INVALID_STAGE", "encode checkpoint is not available", stage="encode"
            )
        path = self.workspace / record["artifact_path"]
        if not path.exists():
            raise PipelineError(
                "E_MISSING_ASSET", "encoded manifest is missing", stage="encode"
            )
        return read_json(path)

    def verify_encoded_output(self) -> dict[str, Any]:
        manifest = self.export_manifest()
        output_dir = stage_directory(self.workspace, "encode") / "output"
        paths = list((output_dir / "frames").glob("*.png")) + [output_dir / "frame_summaries.jsonl"]
        actual = build_tree_digest(paths, output_dir)
        return {
            "ok": actual == manifest["output_tree_digest"],
            "expected_tree_digest": manifest["output_tree_digest"],
            "actual_tree_digest": actual,
            "frame_count": manifest["frame_count"],
            "deterministic_summary_sha256": manifest["deterministic_summary_sha256"],
        }

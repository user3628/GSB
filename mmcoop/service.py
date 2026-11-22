from __future__ import annotations

import shutil
from pathlib import Path
from typing import Any

from .canonical import digest_json
from .checkpoints import CheckpointStore
from .config import Config, load_config
from .dataio import (
    Dataset,
    import_from_paths,
    import_records,
    load_dataset,
    replace_image_record,
    replace_text_record,
)
from .errors import AppError, BuildStopped, business_error
from .generator import generate_demo
from .injection import Injection, parse_injections
from .pipeline import Computation, PipelineEngine
from .stages import STAGES


class WorkspaceService:
    def __init__(self, config_path: str | Path, data_dir: str | Path, workspace_dir: str | Path) -> None:
        self.config_path = Path(config_path)
        self.data_dir = Path(data_dir)
        self.workspace_dir = Path(workspace_dir)
        self.store = CheckpointStore(self.workspace_dir)

    def _load(self) -> tuple[Config, Dataset]:
        return load_config(self.config_path), load_dataset(self.data_dir)

    def ensure_directories(self) -> None:
        self.store.ensure()

    def reset_workspace_state(self) -> None:
        for path in (
            self.store.checkpoint_dir,
            self.store.artifact_dir,
            self.store.state_dir,
            self.store.quarantine_dir,
            self.store.lock_dir,
        ):
            if path.exists():
                shutil.rmtree(path)
        try:
            self.store.stop_file.unlink()
        except FileNotFoundError:
            pass
        self.store.ensure()

    def generate_demo(self, *, seed: int = 20261123, fresh_workspace: bool = False) -> dict[str, Any]:
        if not self.config_path.exists():
            raise business_error("E_INVALID_INPUT", "data_import", "configuration not found", http_status=404)
        config = load_config(self.config_path)
        dataset = generate_demo(self.data_dir, seed, config)
        if fresh_workspace:
            self.reset_workspace_state()
        else:
            self.store.ensure()
        return {
            "seed": seed,
            "text_count": len(dataset.texts),
            "image_count": len(dataset.images),
            "missing_link_text_ids": sorted(
                set(dataset.text_map()) - {str(image.get("text_id")) for image in dataset.images if image.get("text_id") is not None}
            ),
            "duplicate_content_groups": list(dataset.duplicate_content_groups),
            "data_fingerprint": dataset.content_fingerprint(self.data_dir),
        }

    def import_dataset(self, source_texts: str | Path, source_images: str | Path) -> dict[str, Any]:
        dataset = import_from_paths(source_texts, source_images, self.data_dir)
        return {
            "text_count": len(dataset.texts),
            "image_count": len(dataset.images),
            "data_fingerprint": dataset.content_fingerprint(self.data_dir),
        }

    def import_payload(self, payload: dict[str, Any]) -> dict[str, Any]:
        if "texts_path" in payload or "images_path" in payload:
            if not payload.get("texts_path") or not payload.get("images_path"):
                raise business_error(
                    "E_INVALID_INPUT",
                    "data_import",
                    "texts_path and images_path are both required",
                    http_status=400,
                )
            return self.import_dataset(str(payload["texts_path"]), str(payload["images_path"]))
        texts = payload.get("texts")
        images = payload.get("images")
        if not isinstance(texts, list) or not isinstance(images, list):
            raise business_error(
                "E_INVALID_INPUT",
                "data_import",
                "payload must contain texts and images arrays, or both path fields",
                http_status=400,
            )
        dataset = import_records(self.data_dir, texts, images)
        return {
            "text_count": len(dataset.texts),
            "image_count": len(dataset.images),
            "data_fingerprint": dataset.content_fingerprint(self.data_dir),
        }

    def edit_text(self, text_id: str, changes: dict[str, Any]) -> dict[str, Any]:
        record = replace_text_record(self.data_dir, text_id, changes)
        config, dataset = self._load()
        return {"record": record, "data_fingerprint": dataset.content_fingerprint(self.data_dir), "config_version": config.digest}

    def edit_image(self, image_id: str, changes: dict[str, Any]) -> dict[str, Any]:
        record = replace_image_record(self.data_dir, image_id, changes)
        config, dataset = self._load()
        return {"record": record, "data_fingerprint": dataset.content_fingerprint(self.data_dir), "config_version": config.digest}

    def request_stop(self) -> dict[str, Any]:
        value = self.store.request_stop()
        return {"status": "stop_requested", **value}

    def resume(self, *, verify_incremental: bool = True, through_stage: str = "aggregation") -> dict[str, Any]:
        self.store.clear_stop()
        return self.build(verify_incremental=verify_incremental, through_stage=through_stage)

    def build(
        self,
        *,
        injections: Any = None,
        verify_incremental: bool = True,
        through_stage: str = "aggregation",
    ) -> dict[str, Any]:
        parsed_injections = parse_injections(injections)
        if len(parsed_injections) > 1:
            raise business_error(
                "E_INVALID_INPUT",
                "injection",
                "exactly one injection may be applied per build",
                field="injections",
                http_status=400,
            )
        injection: Injection | None = parsed_injections[0] if parsed_injections else None
        if through_stage not in STAGES:
            raise business_error(
                "E_INVALID_STAGE",
                "build",
                "unknown target stage",
                diagnostics={"through_stage": through_stage, "valid_stages": list(STAGES)},
                http_status=409,
            )
        if injection is not None and STAGES.index(injection.stage) > STAGES.index(through_stage):
            raise business_error(
                "E_INVALID_STAGE",
                "build",
                "injection targets a stage after the requested build boundary",
                diagnostics={"injection_stage": injection.stage, "through_stage": through_stage},
                http_status=409,
            )
        config, dataset = self._load()
        self.store.ensure()
        if self.store.stop_requested():
            result = {
                "status": "stopped",
                "stage": None,
                "reason": "stop_requested_before_build",
                "reused_stages": [],
                "rebuilt_stages": [],
            }
            self.store.write_last_run(result)
            return result
        integrity = self.store.inspect_integrity()
        if integrity["verdict"] in {"rollback", "tamper"}:
            raise business_error(
                "E_STALE_CHECKPOINT",
                "publish",
                "workspace snapshot integrity failure blocks build",
                diagnostics={"integrity": integrity},
                http_status=409,
            )
        if integrity["verdict"] == "normal_advance":
            integrity = self.store.reconcile_marker()
        reusable = self.store.current_stages()
        engine = PipelineEngine(config, dataset, self.data_dir)
        try:
            computation = engine.compute(
                reusable=reusable,
                injection=injection,
                should_stop=self.store.stop_requested,
                through_stage=through_stage,
            )
            if verify_incremental and computation.reused_stages:
                engine.verify_against_full(computation)
        except BuildStopped as stopped:
            result = {
                "status": "stopped",
                "stage": stopped.stage,
                "reason": "stop_requested_after_current_unit",
                "completed_in_memory_stages": stopped.completed_stages,
                "reused_stages": [],
                "rebuilt_stages": [],
            }
            self.store.write_last_run(result)
            return result
        except AppError as error:
            self.store.write_last_run(
                {
                    "status": "failed",
                    "injection": injection.to_dict() if injection else None,
                    "error": error.to_dict(),
                }
            )
            raise

        published: list[dict[str, Any]] = []
        try:
            for stage in computation.rebuilt_stages:
                published.append(
                    self.store.publish(
                        computation.artifacts[stage],
                        input_fingerprint=computation.input_fingerprints[stage],
                        components=computation.components[stage],
                        config_version=config.digest,
                    )
                )
        except AppError as error:
            self.store.write_last_run(
                {
                    "status": "failed",
                    "injection": injection.to_dict() if injection else None,
                    "rebuilt_stages": computation.rebuilt_stages,
                    "published": published,
                    "error": error.to_dict(),
                }
            )
            raise
        versions = computation.public_version()
        result = {
            "status": "completed" if through_stage == "aggregation" else "partial_completed",
            "through_stage": through_stage,
            "injection": injection.to_dict() if injection else None,
            "reused_stages": computation.reused_stages,
            "rebuilt_stages": computation.rebuilt_stages,
            "invalidations": computation.invalidations,
            "published": published,
            "data_fingerprint": engine.data_fingerprint(),
            "config_version": config.digest,
            **versions,
        }
        self.store.write_last_run(result)
        return result

    def _status_from_computation(
        self,
        config: Config,
        dataset: Dataset,
        engine: PipelineEngine,
        computation: Computation,
        stored: dict[str, dict[str, Any]],
        integrity: dict[str, Any],
    ) -> dict[str, Any]:
        invalidation_by_stage = {item["stage"]: item for item in computation.invalidations}
        stage_rows: list[dict[str, Any]] = []
        for stage in STAGES:
            expected_fp = computation.input_fingerprints[stage]
            expected_digest = computation.digests[stage]
            existing = stored.get(stage)
            valid = bool(
                existing
                and existing.get("input_fingerprint") == expected_fp
                and existing.get("artifact_sha256") == expected_digest
            )
            if valid:
                state = "valid"
            elif existing:
                state = "stale"
            else:
                state = "missing"
            invalidation = invalidation_by_stage.get(stage)
            stage_rows.append(
                {
                    "stage": stage,
                    "state": state,
                    "reused": stage in computation.reused_stages,
                    "checkpoint_sequence": existing.get("sequence") if existing else None,
                    "artifact_sha256": expected_digest,
                    "published_artifact_sha256": existing.get("artifact_sha256") if existing else None,
                    "input_fingerprint": expected_fp,
                    "published_input_fingerprint": existing.get("input_fingerprint") if existing else None,
                    "reason_codes": invalidation.get("reason_codes", []) if invalidation else [],
                }
            )
        return {
            "status": "ready" if all(row["state"] == "valid" for row in stage_rows) else "stale",
            "config": config.public_summary(),
            "data_fingerprint": engine.data_fingerprint(),
            "checkpoint": {
                "latest_sequence": max((row["checkpoint_sequence"] or 0) for row in stage_rows),
                "stages": {
                    stage: {
                        "sequence": value.get("sequence"),
                        "input_fingerprint": value.get("input_fingerprint"),
                        "artifact_sha256": value.get("artifact_sha256"),
                    }
                    for stage, value in sorted(stored.items())
                },
            },
            "version": computation.public_version(),
            "stages": stage_rows,
            "earliest_affected_stage": computation.rebuilt_stages[0] if computation.rebuilt_stages else None,
            "invalidations": computation.invalidations,
            "integrity": integrity,
            "stop_requested": self.store.stop_requested(),
            "last_run": self.store.read_last_run(),
        }

    def status(self) -> dict[str, Any]:
        config, dataset = self._load()
        self.store.ensure()
        integrity = self.store.inspect_integrity()
        stored = self.store.current_stages()
        engine = PipelineEngine(config, dataset, self.data_dir)
        try:
            computation = engine.compute(reusable=stored, injection=None, should_stop=None)
            return self._status_from_computation(config, dataset, engine, computation, stored, integrity)
        except AppError as error:
            version = {
                "version_fingerprint": digest_json(
                    {
                        stage: {
                            "input_fingerprint": value.get("input_fingerprint"),
                            "artifact_sha256": value.get("artifact_sha256"),
                        }
                        for stage, value in sorted(stored.items())
                    }
                ),
                "pipeline_fingerprint": stored.get("aggregation", {}).get("artifact_sha256"),
                "stage_input_fingerprints": {stage: value.get("input_fingerprint") for stage, value in sorted(stored.items())},
                "stage_artifact_sha256": {stage: value.get("artifact_sha256") for stage, value in sorted(stored.items())},
            }
            return {
                "status": "error",
                "config": config.public_summary(),
                "data_fingerprint": engine.data_fingerprint(),
                "checkpoint": {
                    "latest_sequence": max((value.get("sequence", 0) for value in stored.values()), default=0),
                    "stages": {
                        stage: {
                            "sequence": value.get("sequence"),
                            "input_fingerprint": value.get("input_fingerprint"),
                            "artifact_sha256": value.get("artifact_sha256"),
                        }
                        for stage, value in sorted(stored.items())
                    },
                },
                "version": version,
                "stages": [
                    {
                        "stage": stage,
                        "state": "valid" if stage in stored else "missing",
                        "reused": False,
                        "checkpoint_sequence": stored.get(stage, {}).get("sequence"),
                        "artifact_sha256": stored.get(stage, {}).get("artifact_sha256"),
                        "input_fingerprint": stored.get(stage, {}).get("input_fingerprint"),
                    }
                    for stage in STAGES
                ],
                "earliest_affected_stage": error.stage,
                "invalidations": [],
                "pipeline_error": error.to_dict(),
                "integrity": integrity,
                "stop_requested": self.store.stop_requested(),
                "last_run": self.store.read_last_run(),
            }

    def results(self) -> dict[str, Any]:
        status = self.status()
        stored = self.store.current_stages()
        aggregation = stored.get("aggregation")
        if aggregation is None:
            raise business_error(
                "E_INVALID_STAGE",
                "aggregation",
                "aggregation result is not available until the pipeline is built",
                diagnostics={"status": status["status"]},
                http_status=409,
            )
        current = any(row["stage"] == "aggregation" and row["state"] == "valid" for row in status["stages"]) and status["status"] == "ready"
        return {
            "current": current,
            "status": status["status"],
            "version": status["version"],
            "invalidations": status.get("invalidations", []),
            "result": aggregation["artifact"],
        }

    def invalidations(self) -> dict[str, Any]:
        status = self.status()
        return {
            "status": status["status"],
            "version": status["version"],
            "earliest_affected_stage": status.get("earliest_affected_stage"),
            "invalidations": status.get("invalidations", []),
        }

    def integrity(self) -> dict[str, Any]:
        self.store.ensure()
        return self.store.inspect_integrity()

    def checkpoints(self) -> dict[str, Any]:
        self.store.ensure()
        return {
            "records": self.store.public_checkpoint_listing(),
            "scan": self.store.scan().public(),
            "integrity": self.store.inspect_integrity(),
        }

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any, Callable, Iterable

import jsonlines

from .aggregate import aggregate_records
from .canonical import (
    SCHEMA_VERSION,
    GracefulStop,
    PipelineError,
    artifact_digest,
    atomic_write_json,
    canonical_bytes,
    deep_merge,
    exclusive_file_lock,
    read_bytes,
    read_json,
    sha256_bytes,
    sha256_obj,
)
from .checkpoints import CheckpointStore
from .config import Config, apply_assignments
from .normalize import validate_and_normalize_record
from .scoring import judge_record
from .validation import validate_annotations


STAGE_NAMES = {1: "normalize", 2: "judge", 3: "aggregate", 4: "validate"}


class Pipeline:
    def __init__(self, run_dir: Path | str) -> None:
        self.run_dir = Path(run_dir)
        self.config_path = self.run_dir / "config.json"
        self.state_path = self.run_dir / "state.json"
        self.input_path = self.run_dir / "input" / "records.json"
        self.stages_dir = self.run_dir / "stages"
        self.control_dir = self.run_dir / "control"
        self.stop_path = self.control_dir / "stop.requested"
        self.run_lock_path = self.run_dir / ".run.lock"
        self.checkpoints = CheckpointStore(self.run_dir)

    # ------------------------------------------------------------------ setup
    def _ensure_dirs(self) -> None:
        for directory in (
            self.run_dir,
            self.input_path.parent,
            self.stages_dir,
            self.control_dir,
            self.run_dir / "cache",
        ):
            directory.mkdir(parents=True, exist_ok=True)

    def initialize(
        self,
        shard_paths: Iterable[Path | str],
        config_overrides: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        self._ensure_dirs()
        config = Config.with_overrides(config_overrides)
        records, shard_names = self._read_shards(shard_paths)
        with exclusive_file_lock(self.run_lock_path, blocking=False):
            atomic_write_json(self.config_path, config.to_dict())
            input_fingerprint = self._write_input(records)
            state = self._initial_state(records, input_fingerprint, shard_names, config)
            atomic_write_json(self.state_path, state)
        return {
            "code": "OK",
            "record_count": len(records),
            "shard_count": len(shard_names),
            "shards": shard_names,
            "input_fingerprint": input_fingerprint,
            "config_version": config.version,
        }

    def import_shards(
        self,
        shard_paths: Iterable[Path | str],
        config_overrides: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        self._ensure_dirs()
        records, shard_names = self._read_shards(shard_paths)
        with exclusive_file_lock(self.run_lock_path, blocking=False):
            existing = read_json(self.state_path)
            if existing is None:
                config = Config.with_overrides(config_overrides)
            else:
                current = read_json(self.config_path)
                config = Config(
                    deep_merge(current or Config.defaults().to_dict(), config_overrides or {})
                )
            input_fingerprint = self._write_input(records)
            state = self._initial_state(records, input_fingerprint, shard_names, config)
            atomic_write_json(self.config_path, config.to_dict())
            atomic_write_json(self.state_path, state)
        return {
            "code": "OK",
            "record_count": len(records),
            "shard_count": len(shard_names),
            "shards": shard_names,
            "input_fingerprint": input_fingerprint,
            "config_version": config.version,
            "invalidated_stage": 1,
        }

    def _read_shards(
        self, shard_paths: Iterable[Path | str]
    ) -> tuple[list[dict[str, Any]], list[str]]:
        paths = [Path(item).resolve() for item in shard_paths]
        if not paths:
            raise PipelineError("E_EMPTY_INPUT", "至少需要提供一个 JSONL 分片。")
        records: list[dict[str, Any]] = []
        seen: dict[str, int] = {}
        shard_names: list[str] = []
        for path in sorted(paths, key=lambda item: str(item)):
            if not path.exists():
                raise PipelineError(
                    "E_SHARD_NOT_FOUND", "分片文件不存在。", {"path": str(path)}
                )
            shard_names.append(path.name)
            try:
                with jsonlines.open(path, mode="r") as reader:
                    for record in reader:
                        validate_and_normalize_record(record)
                        sample_id = record["sample_id"]
                        if sample_id in seen:
                            raise PipelineError(
                                "E_DUPLICATE_SAMPLE_ID",
                                "输入分片中存在重复 sample_id。",
                                {"sample_id": sample_id, "paths": [path.name, seen[sample_id]]},
                            )
                        seen[sample_id] = path.name
                        records.append(record)
            except PipelineError:
                raise
            except Exception as exc:
                raise PipelineError(
                    "E_INVALID_JSONL",
                    "无法解析 JSONL 分片。",
                    {"path": str(path), "error": f"{type(exc).__name__}: {exc}"},
                ) from exc
        if not records:
            raise PipelineError("E_EMPTY_INPUT", "分片中没有样本。")
        records.sort(key=lambda item: item["sample_id"])
        return records, sorted(shard_names)

    def _write_input(self, records: list[dict[str, Any]]) -> str:
        payload = {"schema_version": SCHEMA_VERSION, "records": records}
        atomic_write_json(self.input_path, payload)
        return sha256_obj(payload)

    def _initial_state(
        self,
        records: list[dict[str, Any]],
        input_fingerprint: str,
        shard_names: list[str],
        config: Config,
    ) -> dict[str, Any]:
        return {
            "schema_version": SCHEMA_VERSION,
            "input_fingerprint": input_fingerprint,
            "record_count": len(records),
            "shards": shard_names,
            "config": config.to_dict(),
            "config_version": config.version,
            "stage_config_fingerprints": {
                str(stage): config.stage_config_fingerprint(stage) for stage in range(1, 5)
            },
            "stages": {},
        }

    def load_input(self) -> tuple[list[dict[str, Any]], str]:
        payload = read_json(self.input_path)
        if not isinstance(payload, dict) or not isinstance(payload.get("records"), list):
            raise PipelineError("E_NOT_INITIALIZED", "尚未导入任何评测分片。")
        return payload["records"], sha256_obj(payload)

    def load_config(self) -> Config:
        payload = read_json(self.config_path)
        if payload is None:
            raise PipelineError("E_NOT_INITIALIZED", "管线尚未初始化。")
        return Config(payload)

    def load_state(self) -> dict[str, Any]:
        state = read_json(self.state_path)
        if state is None:
            raise PipelineError("E_NOT_INITIALIZED", "管线尚未初始化。")
        return state

    def stage_path(self, stage: int) -> Path:
        return self.stages_dir / f"stage{stage}.json"

    # --------------------------------------------------------------- controls
    def request_stop(self) -> dict[str, Any]:
        self.control_dir.mkdir(parents=True, exist_ok=True)
        atomic_write_json(
            self.stop_path,
            {"requested": True, "scope": "complete_current_indivisible_unit"},
        )
        return {"code": "OK", "stop_requested": True}

    def resume(self) -> dict[str, Any]:
        if self.stop_path.exists():
            self.stop_path.unlink()
        return {"code": "OK", "stop_requested": False, "resume": True}

    def stop_requested(self) -> bool:
        return self.stop_path.exists()

    def _raise_if_stopped(self, stage: str, completed_units: int) -> None:
        if self.stop_requested():
            raise GracefulStop(stage=stage, completed_units=completed_units)

    # ----------------------------------------------------------------- config
    def update_config(self, assignments: list[str] | None = None, updates: dict[str, Any] | None = None) -> dict[str, Any]:
        with exclusive_file_lock(self.run_lock_path, blocking=False):
            current = self.load_config()
            state = self.load_state()
            values = current.to_dict()
            if assignments:
                values = apply_assignments(values, assignments)
            if updates:
                values = deep_merge(values, updates)
            new_config = Config(values)
            earliest = new_config.changed_stage(current)
            atomic_write_json(self.config_path, new_config.to_dict())
        return {
            "code": "OK",
            "old_config_version": current.version,
            "new_config_version": new_config.version,
            "earliest_affected_stage": earliest,
            "affected_stages": list(range(earliest, 5)) if earliest else [],
            "unaffected_upstream_stages": list(range(1, earliest)) if earliest else [1, 2, 3, 4],
            "state_config_version": state.get("config_version"),
        }

    # ------------------------------------------------------------------ status
    def _expected_stage_input(
        self,
        stage: int,
        input_fingerprint: str,
        config: Config,
        prior_stage_digest: str | None,
    ) -> str:
        upstream = input_fingerprint if stage == 1 else prior_stage_digest
        return sha256_obj(
            {
                "schema_version": SCHEMA_VERSION,
                "stage": stage,
                "upstream_fingerprint": upstream,
                "config_fingerprint": config.stage_config_fingerprint(stage),
            }
        )

    def _artifact_is_valid(self, stage: int, state: dict[str, Any]) -> bool:
        metadata = state.get("stages", {}).get(str(stage))
        if not isinstance(metadata, dict) or metadata.get("status") != "complete":
            return False
        path = self.stage_path(stage)
        if not path.exists():
            return False
        return sha256_bytes(read_bytes(path)) == metadata.get("artifact_digest")

    def determine_invalidation(self, state: dict[str, Any], input_fingerprint: str, config: Config) -> int:
        previous_config = Config(state["config"]) if state.get("config") else None
        config_stage = config.changed_stage(previous_config)
        content_changed = input_fingerprint != state.get("input_fingerprint")
        earliest = 1 if content_changed else config_stage

        if earliest == 0:
            for stage in range(1, 5):
                if not self._artifact_is_valid(stage, state):
                    earliest = stage
                    break
        elif earliest > 1:
            if not self._artifact_is_valid(1, state):
                earliest = 1
        return earliest

    def status(self) -> dict[str, Any]:
        config = self.load_config()
        state = self.load_state()
        _, input_fingerprint = self.load_input()
        earliest = self.determine_invalidation(state, input_fingerprint, config)
        stages: list[dict[str, Any]] = []
        for stage in range(1, 5):
            metadata = state.get("stages", {}).get(str(stage), {})
            valid = self._artifact_is_valid(stage, state)
            stages.append(
                {
                    "stage": stage,
                    "name": STAGE_NAMES[stage],
                    "status": metadata.get("status", "pending"),
                    "artifact_valid": valid,
                    "artifact_digest": metadata.get("artifact_digest"),
                    "input_fingerprint": metadata.get("input_fingerprint"),
                    "config_fingerprint": state.get("stage_config_fingerprints", {}).get(str(stage)),
                    "stale": bool(earliest and stage >= earliest),
                    "invalidated": bool(earliest and stage >= earliest),
                }
            )
        return {
            "code": "OK",
            "input_fingerprint": input_fingerprint,
            "record_count": state.get("record_count", 0),
            "shards": state.get("shards", []),
            "config_version": config.version,
            "state_config_version": state.get("config_version"),
            "stage_config_fingerprints": {
                str(stage): config.stage_config_fingerprint(stage) for stage in range(1, 5)
            },
            "earliest_invalidated_stage": earliest,
            "invalidation_reason": self._invalidation_reason(
                state, input_fingerprint, config, earliest
            ),
            "stages": stages,
            "checkpoint_last_sequence": self.checkpoints.last_sequence(),
            "stop_requested": self.stop_requested(),
        }

    def _invalidation_reason(
        self,
        state: dict[str, Any],
        input_fingerprint: str,
        config: Config,
        earliest: int,
    ) -> str | None:
        if earliest == 0:
            return None
        if input_fingerprint != state.get("input_fingerprint"):
            return "INPUT_CONTENT_CHANGED"
        previous = Config(state["config"]) if state.get("config") else None
        if previous is not None:
            for stage in range(1, 5):
                if config.stage_config_fingerprint(stage) != previous.stage_config_fingerprint(stage):
                    return f"STAGE_{stage}_CONFIG_CHANGED"
        return "ARTIFACT_MISSING_OR_CORRUPT"

    # --------------------------------------------------------------- execution
    def _map_units(
        self,
        items: list[dict[str, Any]],
        function: Callable[[dict[str, Any]], dict[str, Any]],
        workers: int,
        stage_name: str,
    ) -> list[dict[str, Any]]:
        if workers <= 1:
            results: list[dict[str, Any]] = []
            for index, item in enumerate(items):
                results.append(function(item))
                if self.stop_requested() and index + 1 < len(items):
                    raise GracefulStop(stage=stage_name, completed_units=index + 1)
            return results

        results: list[dict[str, Any]] = [None] * len(items)  # type: ignore[list-item]
        executor = ThreadPoolExecutor(max_workers=workers)
        futures = [executor.submit(function, item) for item in items]
        try:
            for index, future in enumerate(futures):
                results[index] = future.result()
                if self.stop_requested() and index + 1 < len(items):
                    for pending in futures[index + 1 :]:
                        pending.cancel()
                    raise GracefulStop(stage=stage_name, completed_units=index + 1)
        finally:
            executor.shutdown(wait=True, cancel_futures=True)
        return results

    def _compute_stage1(
        self, records: list[dict[str, Any]], config: Config, input_fingerprint: str, workers: int
    ) -> dict[str, Any]:
        ordered = sorted(records, key=lambda item: item["sample_id"])
        normalized = self._map_units(
            ordered, validate_and_normalize_record, workers, STAGE_NAMES[1]
        )
        normalized.sort(key=lambda item: item["sample_id"])
        return {
            "stage": "normalize",
            "schema_version": SCHEMA_VERSION,
            "normalization_version": config.values["normalization_version"],
            "input_content_fingerprint": input_fingerprint,
            "records": normalized,
        }

    def _compute_stage2(
        self, stage1_payload: dict[str, Any], config: Config, workers: int
    ) -> dict[str, Any]:
        ordered = sorted(stage1_payload["records"], key=lambda item: item["sample_id"])
        results = self._map_units(
            ordered,
            lambda record: judge_record(record, config),
            workers,
            STAGE_NAMES[2],
        )
        results.sort(key=lambda item: item["sample_id"])
        return {
            "stage": "judge",
            "scoring_version": config.values["scoring_version"],
            "scoring_config": config.stage_subset(2),
            "tie_break_rule": "score_micro_desc,evidence_index_asc,evidence_sentence_index_asc",
            "records": results,
        }

    def _compute_all(
        self,
        records: list[dict[str, Any]],
        config: Config,
        input_fingerprint: str,
        workers: int,
        stop_sensitive: bool = False,
    ) -> dict[int, dict[str, Any]]:
        stage1 = self._compute_stage1(records, config, input_fingerprint, workers)
        if stop_sensitive:
            self._raise_if_stopped(STAGE_NAMES[1], len(records))
        stage2 = self._compute_stage2(stage1, config, workers)
        if stop_sensitive:
            self._raise_if_stopped(STAGE_NAMES[2], len(records))
        stage3 = aggregate_records(stage2, config)
        if stop_sensitive:
            self._raise_if_stopped(STAGE_NAMES[3], 1)
        stage4 = validate_annotations(stage2, config)
        return {1: stage1, 2: stage2, 3: stage3, 4: stage4}

    def _publish_stage(
        self,
        stage: int,
        payload: dict[str, Any],
        input_fingerprint: str,
        config: Config,
        state: dict[str, Any],
    ) -> dict[str, Any]:
        path = self.stage_path(stage)
        digest = atomic_write_json(path, payload)
        checkpoint = self.checkpoints.publish(
            stage=STAGE_NAMES[stage],
            input_fingerprint=input_fingerprint,
            config_version=config.version,
            artifact_digest=digest,
        )
        metadata = {
            "status": "complete",
            "stage": STAGE_NAMES[stage],
            "input_fingerprint": input_fingerprint,
            "config_fingerprint": config.stage_config_fingerprint(stage),
            "config_version": config.version,
            "artifact_digest": digest,
            "checkpoint_sequence": checkpoint["sequence"],
        }
        state.setdefault("stages", {})[str(stage)] = metadata
        state.setdefault("stage_config_fingerprints", {})[str(stage)] = metadata[
            "config_fingerprint"
        ]
        atomic_write_json(self.state_path, state)
        return {"stage": stage, "name": STAGE_NAMES[stage], "artifact_digest": digest, "checkpoint_sequence": checkpoint["sequence"]}

    def _load_stage_payload(self, stage: int) -> dict[str, Any]:
        path = self.stage_path(stage)
        if not path.exists():
            raise PipelineError(
                "E_ARTIFACT_MISSING", "阶段产物不存在。", {"stage": stage, "path": str(path)}
            )
        payload = read_json(path)
        if not isinstance(payload, dict):
            raise PipelineError(
                "E_ARTIFACT_CORRUPT", "阶段产物不是 JSON 对象。", {"stage": stage}
            )
        return payload

    def load_stage_payload(self, stage: int) -> dict[str, Any]:
        return self._load_stage_payload(stage)

    def run(
        self, verify_incremental: bool = True, until_stage: int = 4
    ) -> dict[str, Any]:
        if isinstance(until_stage, bool) or until_stage not in (1, 2, 3, 4):
            raise PipelineError(
                "E_INVALID_CONFIG",
                "until_stage 必须是 1 到 4 的整数。",
                {"until_stage": until_stage},
            )
        with exclusive_file_lock(self.run_lock_path, blocking=False):
            config = self.load_config()
            records, input_fingerprint = self.load_input()
            state = self.load_state()
            if self.stop_requested():
                raise GracefulStop(stage="before_stage_1", completed_units=0)

            earliest = self.determine_invalidation(state, input_fingerprint, config)
            recomputed: list[int] = []
            reused: list[int] = []
            payloads: dict[int, dict[str, Any]] = {}
            workers = int(config.values["parallel_workers"])

            for stage in range(1, until_stage + 1):
                if stage < earliest and self._artifact_is_valid(stage, state):
                    payloads[stage] = self._load_stage_payload(stage)
                    reused.append(stage)
                    continue
                if self.stop_requested():
                    raise GracefulStop(stage=f"before_stage_{stage}", completed_units=0)
                try:
                    if stage == 1:
                        payload = self._compute_stage1(
                            records, config, input_fingerprint, workers
                        )
                    elif stage == 2:
                        payload = self._compute_stage2(payloads[1], config, workers)
                    elif stage == 3:
                        payload = aggregate_records(payloads[2], config)
                    else:
                        payload = validate_annotations(payloads[2], config)
                except GracefulStop as exc:
                    state.setdefault("stages", {})[str(stage)] = {
                        "status": "stopped",
                        "stage": STAGE_NAMES[stage],
                        "completed_units": exc.details.get("completed_units", 0),
                        "config_version": config.version,
                    }
                    atomic_write_json(self.state_path, state)
                    raise

                prior_digest = (
                    None
                    if stage == 1
                    else sha256_bytes(read_bytes(self.stage_path(stage - 1)))
                )
                stage_input_fingerprint = self._expected_stage_input(
                    stage, input_fingerprint, config, prior_digest
                )
                payloads[stage] = payload
                self._publish_stage(
                    stage, payload, stage_input_fingerprint, config, state
                )
                recomputed.append(stage)
                if self.stop_requested() and stage < until_stage:
                    raise GracefulStop(stage=STAGE_NAMES[stage], completed_units=1)

            state["config"] = config.to_dict()
            state["config_version"] = config.version
            state["input_fingerprint"] = input_fingerprint
            atomic_write_json(self.state_path, state)

            verification = "SKIPPED"
            if verify_incremental:
                self._verify_against_full_run(
                    records, input_fingerprint, config, payloads, until_stage
                )
                verification = "PASS"

            return {
                "code": "OK",
                "input_fingerprint": input_fingerprint,
                "record_count": len(records),
                "config_version": config.version,
                "earliest_affected_stage": earliest,
                "recomputed_stages": recomputed,
                "reused_stages": reused,
                "stage_fingerprints": {
                    str(stage): artifact_digest(payloads[stage])
                    for stage in range(1, until_stage + 1)
                },
                "until_stage": until_stage,
                "verification": verification,
            }

    def _verify_against_full_run(
        self,
        records: list[dict[str, Any]],
        input_fingerprint: str,
        config: Config,
        payloads: dict[int, dict[str, Any]],
        until_stage: int,
    ) -> None:
        fresh = self._compute_all(
            records, config, input_fingerprint, workers=1, stop_sensitive=False
        )
        different_stages: list[int] = []
        for stage in range(1, until_stage + 1):
            if canonical_bytes(fresh[stage]) != canonical_bytes(payloads[stage]):
                different_stages.append(stage)
        if not different_stages:
            return
        diff_samples = self._diff_samples(payloads.get(2, {}), fresh.get(2, {}))
        raise PipelineError(
            "E_INCREMENTAL_MISMATCH",
            "增量结果与从零全量顺序运行不一致，已拒绝发布。",
            {
                "different_stages": different_stages,
                "different_sample_ids": diff_samples,
            },
        )

    @staticmethod
    def _diff_samples(left: dict[str, Any], right: dict[str, Any]) -> list[str]:
        left_records = {item["sample_id"]: item for item in left.get("records", [])}
        right_records = {item["sample_id"]: item for item in right.get("records", [])}
        sample_ids = sorted(set(left_records) | set(right_records))
        different: list[str] = []
        for sample_id in sample_ids:
            left_record = left_records.get(sample_id)
            right_record = right_records.get(sample_id)
            if left_record is None or right_record is None:
                different.append(sample_id)
            elif canonical_bytes(left_record) != canonical_bytes(right_record):
                different.append(sample_id)
        return different

    # ------------------------------------------------------------------ export
    def export_report(self) -> dict[str, Any]:
        state = self.load_state()
        config = self.load_config()
        _, input_fingerprint = self.load_input()
        report: dict[str, Any] = {
            "code": "OK",
            "schema_version": SCHEMA_VERSION,
            "input_fingerprint": input_fingerprint,
            "record_count": state.get("record_count", 0),
            "config": config.to_dict(),
            "config_version": config.version,
            "status": self.status(),
        }
        for stage in range(1, 5):
            if self._artifact_is_valid(stage, state):
                report[f"stage{stage}"] = self._load_stage_payload(stage)
        return report

    def write_report(self, output_path: Path | str) -> str:
        report = self.export_report()
        digest = atomic_write_json(output_path, report)
        return digest

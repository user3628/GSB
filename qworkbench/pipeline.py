from __future__ import annotations

from pathlib import Path
from typing import Any, Callable, Mapping

from . import ALGORITHM_VERSION, __version__
from .allocation import allocate_and_sample
from .config import WorkbenchConfig
from .consistency import validate_consistency
from .dedup import deduplicate
from .errors import BusinessError, StopRequested
from .fingerprints import STAGES, compute_stage_fingerprints
from .locking import FileLockTimeout
from .samples import SampleDataset
from .scoring import score_records
from .store import CheckpointStore
from .util import (
    atomic_write_json,
    digest_file,
    read_json,
    read_jsonl,
    sha256_json,
    write_jsonl_atomic,
)


STAGE_ARTIFACT_FILES = {
    "normalize": {
        "records": "01_normalized.jsonl",
        "groups": "01_duplicate_groups.jsonl",
        "matches": "01_simhash_matches.jsonl",
        "summary": "01_summary.json",
    },
    "score": {
        "records": "02_scores.jsonl",
        "diagnostics": "02_diagnostics.json",
        "summary": "02_summary.json",
    },
    "allocate": {
        "records": "03_selected.jsonl",
        "allocation": "03_allocation.json",
        "summary": "03_summary.json",
    },
    "mix": {
        "records": "mixture.jsonl",
        "consistency": "04_consistency.json",
        "summary": "04_summary.json",
        "manifest": "manifest.json",
    },
}


class Pipeline:
    def __init__(
        self,
        config: WorkbenchConfig,
        dataset: SampleDataset,
        workdir: Path,
        *,
        stop_checker: Callable[[], None] | None = None,
        stop_after_units: int | None = None,
        stop_after_stage: str | None = None,
        lock_timeout: float = 10.0,
    ) -> None:
        self.config = config
        self.dataset = dataset
        self.workdir = Path(workdir).resolve()
        self.store = CheckpointStore(self.workdir, lock_timeout=lock_timeout)
        self.external_stop_checker = stop_checker
        self.stop_after_units = stop_after_units
        self.stop_after_stage = stop_after_stage
        self._unit_count = 0
        self.expected = compute_stage_fingerprints(dataset, config)

    def _check_stop(self) -> None:
        self._unit_count += 1
        if self.store.stop_requested():
            raise StopRequested("stop request file is present")
        if self.external_stop_checker is not None:
            self.external_stop_checker()
        if self.stop_after_units is not None and self._unit_count >= self.stop_after_units:
            raise StopRequested(f"stop_after_units={self.stop_after_units} reached")

    def _stage_dir(self, stage: str) -> Path:
        return self.store.artifact_dir / stage

    def _write_artifacts(
        self,
        stage: str,
        *,
        jsonl: Mapping[str, list[Mapping[str, Any]]],
        json_files: Mapping[str, Mapping[str, Any]],
    ) -> tuple[dict[str, str], dict[str, str]]:
        stage_dir = self._stage_dir(stage)
        stage_dir.mkdir(parents=True, exist_ok=True)
        artifacts: dict[str, str] = {}
        names = STAGE_ARTIFACT_FILES[stage]
        for key, relative_name in names.items():
            path = stage_dir / relative_name
            if key in jsonl:
                write_jsonl_atomic(path, jsonl[key])
            elif key in json_files:
                atomic_write_json(path, json_files[key])
            else:
                raise BusinessError(
                    "E_INVALID_STAGE", "stage artifact payload is missing", {"stage": stage, "artifact": key}
                )
            artifacts[str(path.relative_to(self.workdir))] = digest_file(path)
        return artifacts, dict(names)

    def _output_fingerprint(self, artifacts: Mapping[str, str]) -> str:
        return sha256_json(dict(sorted(artifacts.items())))

    def _read_stage_records(self, stage: str, key: str = "records") -> list[dict[str, Any]]:
        relative = f"artifacts/{stage}/{STAGE_ARTIFACT_FILES[stage][key]}"
        path = self.workdir / relative
        if not path.exists():
            raise BusinessError(
                "E_INVALID_STAGE",
                "required predecessor artifact is missing",
                {"stage": stage, "artifact": relative},
            )
        return read_jsonl(path)

    def _checkpoint_valid(
        self,
        stage: str,
        state: Mapping[str, Any],
    ) -> tuple[bool, dict[str, Any] | None, list[dict[str, Any]]]:
        checkpoint = state.get("checkpoints", {}).get(stage)
        if not checkpoint:
            return False, None, [{"reason": "missing_checkpoint"}]
        if checkpoint.get("fingerprint") != self.expected[stage]["fingerprint"]:
            return False, checkpoint, [{"reason": "fingerprint_mismatch"}]
        valid, problems = self.store.checkpoint_artifact_validation(checkpoint)
        return valid, checkpoint, problems

    def _stage_reasons(
        self,
        stage: str,
        checkpoint: Mapping[str, Any] | None,
        artifact_problems: list[dict[str, Any]],
    ) -> list[str]:
        if checkpoint is None:
            return ["no checkpoint exists for this stage"]
        expected = self.expected[stage]
        reasons: list[str] = []
        if checkpoint.get("fingerprint") == expected["fingerprint"] and artifact_problems:
            return ["artifact missing or digest mismatch"]
        if checkpoint.get("config_fingerprint") != expected["config_fingerprint"]:
            reason_map = {
                "normalize": "dedup parameters changed",
                "score": "scoring weights, source priors, length parameters, or quantization changed",
                "allocate": "allocation weights, target size, or default weight changed",
                "mix": "consistency thresholds, annotation range, or bucket parameters changed",
            }
            reasons.append(reason_map[stage])
        if checkpoint.get("input_fingerprint") != expected["input_fingerprint"]:
            if stage == "normalize":
                reasons.append("sample content changed")
            elif stage == "mix":
                reasons.append("annotation data or predecessor fingerprint changed")
            else:
                reasons.append("upstream stage fingerprint changed")
        if artifact_problems:
            reasons.append("artifact missing or digest mismatch")
        return reasons or ["stage fingerprint changed"]

    def _invalidation_event(
        self,
        state: Mapping[str, Any],
        earliest_stage: str,
        artifact_problems_by_stage: Mapping[str, list[dict[str, Any]]],
    ) -> dict[str, Any]:
        start = STAGES.index(earliest_stage)
        affected = list(STAGES[start:])
        reasons = []
        old_fingerprints: dict[str, Any] = {}
        for stage in affected:
            checkpoint = state.get("checkpoints", {}).get(stage)
            old_fingerprints[stage] = {
                "fingerprint": checkpoint.get("fingerprint") if checkpoint else None,
                "config_fingerprint": checkpoint.get("config_fingerprint") if checkpoint else None,
                "input_fingerprint": checkpoint.get("input_fingerprint") if checkpoint else None,
            }
            reasons.extend(
                {
                    "stage": stage,
                    "reason": reason,
                }
                for reason in self._stage_reasons(
                    stage, checkpoint, artifact_problems_by_stage.get(stage, [])
                )
            )
        return self.store.record_invalidation(
            earliest_stage=earliest_stage,
            affected_stages=affected,
            reasons=reasons,
            old_fingerprints=old_fingerprints,
            expected_fingerprints={
                stage: self.expected[stage]["fingerprint"] for stage in affected
            },
        )

    def _commit_stage(
        self,
        stage: str,
        *,
        artifacts: Mapping[str, str],
        summary: Mapping[str, Any],
        state: Mapping[str, Any],
    ) -> dict[str, Any]:
        expected = self.expected[stage]
        record = {
            "stage": stage,
            "input_fingerprint": expected["input_fingerprint"],
            "config_version": self.config.version,
            "config_fingerprint": expected["config_fingerprint"],
            "upstream_fingerprint": expected["upstream_fingerprint"],
            "fingerprint": expected["fingerprint"],
            "output_fingerprint": self._output_fingerprint(artifacts),
            "artifacts": dict(artifacts),
            "summary": dict(summary),
            "components": {
                "algorithm_version": ALGORITHM_VERSION,
                "workbench_version": __version__,
                "dataset_fingerprint": self.dataset.sample_fingerprint,
                "annotation_fingerprint": self.dataset.annotation_fingerprint,
                "config_fingerprint": self.config.fingerprint,
            },
        }
        seq = int(state.get("last_seq", 0)) + 1
        return self.store.publish_stage(record, seq=seq)

    def run(self, start_stage: str = "normalize") -> dict[str, Any]:
        if start_stage not in STAGES:
            raise BusinessError(
                "E_INVALID_STAGE",
                "unknown start stage",
                {"stage": start_stage, "known": list(STAGES)},
            )
        start_index = STAGES.index(start_stage)
        try:
            with self.store.run_lock():
                state = self.store.load_state()
                self._unit_count = 0
                if start_index > 0:
                    for predecessor in STAGES[:start_index]:
                        valid, checkpoint, problems = self._checkpoint_valid(predecessor, state)
                        if not valid:
                            raise BusinessError(
                                "E_INVALID_STAGE",
                                "cannot start from requested stage because a predecessor is invalid",
                                {
                                    "requested_stage": start_stage,
                                    "required_predecessor": predecessor,
                                    "checkpoint_fingerprint": checkpoint.get("fingerprint")
                                    if checkpoint
                                    else None,
                                    "expected_fingerprint": self.expected[predecessor]["fingerprint"],
                                    "problems": problems,
                                },
                            )
                validity: dict[str, bool] = {}
                checkpoints: dict[str, dict[str, Any] | None] = {}
                problems_by_stage: dict[str, list[dict[str, Any]]] = {}
                for stage in STAGES:
                    valid, checkpoint, problems = self._checkpoint_valid(stage, state)
                    validity[stage] = valid
                    checkpoints[stage] = checkpoint
                    problems_by_stage[stage] = problems
                invalidated: set[str] = set()
                earliest = next(
                    (stage for stage in STAGES if not validity[stage]),
                    None,
                )
                if earliest is not None and STAGES.index(earliest) >= start_index:
                    affected_index = STAGES.index(earliest)
                    invalidated = set(STAGES[affected_index:])
                    self._invalidation_event(state, earliest, problems_by_stage)
                completed: list[str] = []
                reused: list[str] = []
                records_by_stage: dict[str, list[dict[str, Any]]] = {}
                for stage in STAGES:
                    stage_index = STAGES.index(stage)
                    if stage_index < start_index:
                        completed.append(stage)
                        reused.append(stage)
                        continue
                    self._check_stop()
                    if validity[stage] and stage not in invalidated:
                        completed.append(stage)
                        reused.append(stage)
                        continue
                    if stage == "normalize":
                        names = STAGE_ARTIFACT_FILES[stage]
                        result = deduplicate(
                            self.dataset.samples,
                            self.config.dedup,
                            stop_checker=self._check_stop,
                        )
                        artifacts, _ = self._write_artifacts(
                            stage,
                            jsonl={
                                "records": result["records"],
                                "groups": result["groups"],
                                "matches": result["matches"],
                            },
                            json_files={"summary": result["summary"]},
                        )
                        checkpoint = self._commit_stage(
                            stage, artifacts=artifacts, summary=result["summary"], state=state
                        )
                        records_by_stage[stage] = result["records"]
                    elif stage == "score":
                        names = STAGE_ARTIFACT_FILES[stage]
                        normalized = records_by_stage.get("normalize")
                        if normalized is None:
                            normalized = self._read_stage_records("normalize")
                        result = score_records(
                            normalized,
                            self.config,
                            stop_checker=self._check_stop,
                        )
                        artifacts, _ = self._write_artifacts(
                            stage,
                            jsonl={"records": result["records"]},
                            json_files={
                                "diagnostics": result["diagnostics"],
                                "summary": result["summary"],
                            },
                        )
                        checkpoint = self._commit_stage(
                            stage, artifacts=artifacts, summary=result["summary"], state=state
                        )
                        records_by_stage[stage] = result["records"]
                    elif stage == "allocate":
                        names = STAGE_ARTIFACT_FILES[stage]
                        scored = records_by_stage.get("score")
                        if scored is None:
                            scored = self._read_stage_records("score")
                        result = allocate_and_sample(
                            scored,
                            self.config.allocation,
                            stop_checker=self._check_stop,
                        )
                        allocation_payload = {
                            "summary": result["summary"],
                            **result["allocation"],
                        }
                        artifacts, _ = self._write_artifacts(
                            stage,
                            jsonl={"records": result["records"]},
                            json_files={
                                "allocation": allocation_payload,
                                "summary": result["summary"],
                            },
                        )
                        checkpoint = self._commit_stage(
                            stage, artifacts=artifacts, summary=result["summary"], state=state
                        )
                        records_by_stage[stage] = result["records"]
                    elif stage == "mix":
                        names = STAGE_ARTIFACT_FILES[stage]
                        selected = records_by_stage.get("allocate")
                        if selected is None:
                            selected = self._read_stage_records("allocate")
                        result = validate_consistency(
                            selected,
                            self.dataset.annotations,
                            self.config.consistency,
                            self.config.allocation.source_weights,
                            stop_checker=self._check_stop,
                        )
                        mixture_records = result["records"]
                        mixture_path = self._stage_dir(stage) / STAGE_ARTIFACT_FILES[stage]["records"]
                        stage_dir = self._stage_dir(stage)
                        stage_dir.mkdir(parents=True, exist_ok=True)
                        write_jsonl_atomic(mixture_path, mixture_records)
                        mixture_sha = digest_file(mixture_path)
                        final_fingerprint = sha256_json(
                            {
                                "dataset_fingerprint": self.dataset.sample_fingerprint,
                                "annotation_fingerprint": self.dataset.annotation_fingerprint,
                                "config_fingerprint": self.config.fingerprint,
                                "stage_fingerprints": {
                                    name: self.expected[name]["fingerprint"] for name in STAGES
                                },
                                "mixture_sha256": mixture_sha,
                            }
                        )
                        manifest = {
                            "schema_version": 1,
                            "workbench_version": __version__,
                            "algorithm_version": ALGORITHM_VERSION,
                            "config_version": self.config.version,
                            "config_fingerprint": self.config.fingerprint,
                            "dataset_fingerprint": self.dataset.sample_fingerprint,
                            "annotation_fingerprint": self.dataset.annotation_fingerprint,
                            "stage_fingerprints": {
                                name: self.expected[name]["fingerprint"] for name in STAGES
                            },
                            "final_fingerprint": final_fingerprint,
                            "mixture_sha256": mixture_sha,
                            "mixture_samples": len(mixture_records),
                            "mixture_stats": result["summary"],
                            "source_review": result["summary"]["source_review"],
                        }
                        summary = {
                            **result["summary"],
                            "final_fingerprint": final_fingerprint,
                            "mixture_sha256": mixture_sha,
                            "source_review": result["summary"]["source_review"],
                        }
                        artifacts, _ = self._write_artifacts(
                            stage,
                            jsonl={"records": mixture_records},
                            json_files={
                                "consistency": result["report"],
                                "summary": summary,
                                "manifest": manifest,
                            },
                        )
                        checkpoint = self._commit_stage(
                            stage, artifacts=artifacts, summary=summary, state=state
                        )
                        records_by_stage[stage] = mixture_records
                    state = self.store.load_state()
                    completed.append(stage)
                    if self.stop_after_stage == stage:
                        raise StopRequested(f"stop_after_stage={stage} reached")
                final_checkpoint = self.store.get_checkpoint("mix", state)
                return {
                    "status": "COMPLETE",
                    "completed_stages": completed,
                    "reused_stages": reused,
                    "rebuilt_stages": [stage for stage in completed if stage not in reused],
                    "last_seq": state.get("last_seq"),
                    "final_fingerprint": final_checkpoint["summary"].get("final_fingerprint")
                    if final_checkpoint
                    else None,
                    "mixture_sha256": final_checkpoint["summary"].get("mixture_sha256")
                    if final_checkpoint
                    else None,
                }
        except StopRequested as exc:
            state = self.store.load_state()
            return {
                "status": "STOPPED",
                "reason": str(exc),
                "completed_stages": completed if "completed" in locals() else [],
                "last_seq": state.get("last_seq", 0),
                "stop_requested": self.store.stop_requested(),
            }
        except FileLockTimeout as exc:
            raise BusinessError("E_LOCK_TIMEOUT", "unable to acquire run lock", {"error": str(exc)}) from exc

    def status(self) -> dict[str, Any]:
        state = self.store.load_state()
        stages: dict[str, Any] = {}
        earliest: str | None = None
        for stage in STAGES:
            valid, checkpoint, problems = self._checkpoint_valid(stage, state)
            if not valid and earliest is None:
                earliest = stage
            expected = self.expected[stage]
            stages[stage] = {
                "valid": valid,
                "checkpoint_fingerprint": checkpoint.get("fingerprint") if checkpoint else None,
                "expected_fingerprint": expected["fingerprint"],
                "input_fingerprint": expected["input_fingerprint"],
                "config_fingerprint": expected["config_fingerprint"],
                "seq": checkpoint.get("seq") if checkpoint else None,
                "output_fingerprint": checkpoint.get("output_fingerprint") if checkpoint else None,
                "artifact_problems": problems,
                "summary": checkpoint.get("summary") if checkpoint else None,
                "invalid_reasons": self._stage_reasons(stage, checkpoint, problems)
                if not valid
                else [],
            }
        return {
            "ok": True,
            "state": "COMPLETE" if earliest is None else "INCOMPLETE",
            "config_version": self.config.version,
            "config_fingerprint": self.config.fingerprint,
            "algorithm_version": ALGORITHM_VERSION,
            "dataset": self.dataset.summary(),
            "last_seq": state.get("last_seq", 0),
            "earliest_invalid_stage": earliest,
            "stages": stages,
            "last_invalidation": state.get("last_invalidation"),
            "invalidation_history": state.get("invalidation_history", []),
            "stop_requested": self.store.stop_requested(),
        }

    def _valid_artifact_json(self, stage: str, artifact_key: str) -> dict[str, Any]:
        state = self.store.load_state()
        valid, checkpoint, problems = self._checkpoint_valid(stage, state)
        if not valid:
            raise BusinessError(
                "E_INVALID_STAGE",
                "stage artifact is unavailable until its prerequisites are valid",
                {
                    "stage": stage,
                    "checkpoint_fingerprint": checkpoint.get("fingerprint") if checkpoint else None,
                    "expected_fingerprint": self.expected[stage]["fingerprint"],
                    "problems": problems,
                },
            )
        relative = f"artifacts/{stage}/{STAGE_ARTIFACT_FILES[stage][artifact_key]}"
        return read_json(self.workdir / relative)

    def diagnostics(self) -> dict[str, Any]:
        state = self.store.load_state()
        valid, checkpoint, problems = self._checkpoint_valid("score", state)
        if not valid:
            raise BusinessError(
                "E_INVALID_STAGE",
                "numeric diagnostics require a valid score stage",
                {
                    "stage": "score",
                    "problems": problems,
                    "expected_fingerprint": self.expected["score"]["fingerprint"],
                },
            )
        payload = dict(self._valid_artifact_json("score", "diagnostics"))
        payload["stage_fingerprint"] = self.expected["score"]["fingerprint"]
        payload["checkpoint_seq"] = checkpoint.get("seq") if checkpoint else None
        return payload

    def allocation(self) -> dict[str, Any]:
        payload = dict(self._valid_artifact_json("allocate", "allocation"))
        payload["stage_fingerprint"] = self.expected["allocate"]["fingerprint"]
        return payload

    def consistency(self) -> dict[str, Any]:
        payload = dict(self._valid_artifact_json("mix", "consistency"))
        payload["stage_fingerprint"] = self.expected["mix"]["fingerprint"]
        return payload

    def final_manifest(self) -> dict[str, Any]:
        return self._valid_artifact_json("mix", "manifest")

    def export_path(self) -> Path:
        state = self.store.load_state()
        valid, checkpoint, problems = self._checkpoint_valid("mix", state)
        if not valid or checkpoint is None:
            raise BusinessError(
                "E_INVALID_STAGE",
                "mixture export requires a valid mix stage",
                {"stage": "mix", "problems": problems},
            )
        path = self.workdir / "artifacts/mix/mixture.jsonl"
        if not path.exists():
            raise BusinessError("E_INVALID_STAGE", "mixture artifact is missing", {"path": str(path)})
        return path

    def request_stop(self, reason: str = "operator_request") -> dict[str, Any]:
        return self.store.request_stop(reason)

    def resume(self) -> dict[str, Any]:
        self.store.clear_stop()
        return self.run()

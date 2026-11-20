"""Four-stage pipeline orchestration with incremental invalidation."""
from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

from . import stage_features, stage_normalize, stage_quality, stage_sampling
from .checkpoint import (
    STAGE_ORDER,
    FileLock,
    checkpoint_record,
    clear_marker,
    latest_consistent,
    load_checkpoint,
    marker_present,
    publish_checkpoint,
    truncate_from,
    write_marker,
)
from .config import STAGES, Config
from .errors import IncrementalMismatchError
from .hashing import canonical_json, stable_hash64
from .paths import ensure_dir
from .store import WorkspaceStore, atomic_write_json

STAGE_MODULES = {
    "normalize": stage_normalize,
    "quality": stage_quality,
    "features": stage_features,
    "sampling": stage_sampling,
}


class StopRequested(Exception):
    """Raised at a sample-unit boundary when a stop request is observed."""


@dataclass
class StageVersion:
    stage: str
    input_fingerprint: str
    config_fingerprint: str
    upstream_fingerprint: str
    stage_fingerprint: str
    artifact_digest: str


class Pipeline:
    def __init__(self, cfg: Config):
        self.cfg = cfg
        self.root = cfg.project_root
        self.store = WorkspaceStore(
            self.root, cfg.path("shards_dir"), cfg.path("state_dir"))
        self.artifacts_dir = ensure_dir(cfg.path("artifacts_dir"))
        self.checkpoints_dir = ensure_dir(cfg.path("checkpoints_dir"))
        self.locks_dir = ensure_dir(cfg.path("locks_dir"))
        self.stop_marker = self.locks_dir / "STOP_REQUESTED"
        self.invalidation_log_path = cfg.path("state_dir") / "invalidation_log.json"
        self.publish_lock = self.locks_dir / "publish.lock"

    # ---- stop handling --------------------------------------------------
    def request_stop(self) -> None:
        write_marker(self.stop_marker)

    def stop_requested(self) -> bool:
        return marker_present(self.stop_marker)

    def clear_stop(self) -> None:
        clear_marker(self.stop_marker)

    def _checkpoint(self) -> None:
        if self.stop_requested():
            raise StopRequested("stop requested at sample-unit boundary")

    # ---- fingerprints ---------------------------------------------------
    def _stage_versions(self, input_summary: dict) -> dict[str, StageVersion]:
        versions: dict[str, StageVersion] = {}
        upstream = ""
        for stage in STAGES:
            input_fp = stable_hash64(canonical_json(input_summary))
            config_fp = self.cfg.subtree_fingerprint(stage)
            if stage == "normalize":
                stage_fp = stable_hash64(canonical_json(
                    {"input": input_fp, "config": config_fp}))
            else:
                prev = versions[STAGES[STAGES.index(stage) - 1]]
                upstream = prev.stage_fingerprint
                stage_fp = stable_hash64(canonical_json(
                    {"input": input_fp, "config": config_fp, "upstream": upstream}))
            versions[stage] = StageVersion(stage, input_fp, config_fp, upstream, stage_fp, "")
        return versions

    def _artifact_path(self, stage: str) -> Path:
        return self.artifacts_dir / f"{stage}.json"

    def _save_artifact(self, stage: str, result: dict) -> str:
        digest = STAGE_MODULES[stage].artifact_digest(result)
        atomic_write_json(self._artifact_path(stage), result)
        return digest

    def load_artifact(self, stage: str) -> dict:
        return json.loads(self._artifact_path(stage).read_text(encoding="utf-8"))

    # ---- invalidation ---------------------------------------------------
    def _log_invalidation(self, entry: dict) -> None:
        log = []
        if self.invalidation_log_path.exists():
            log = json.loads(self.invalidation_log_path.read_text(encoding="utf-8"))
        log.append(entry)
        atomic_write_json(self.invalidation_log_path, log)

    def detect_changes(self, input_summary: dict) -> list[dict]:
        """Compare current input/config fingerprints with checkpoints."""
        versions = self._stage_versions(input_summary)
        changes: list[dict] = []
        for stage in STAGES:
            record = load_checkpoint(self.checkpoints_dir, stage)
            if record is None:
                changes.append({"stage": stage, "reason": "missing_checkpoint",
                                "new_fingerprint": versions[stage].stage_fingerprint})
                continue
            mismatch = []
            if record["input_summary"] != input_summary:
                mismatch.append("input")
            if record["config_fingerprint"] != versions[stage].config_fingerprint:
                mismatch.append("config")
            if record["stage_fingerprint"] != versions[stage].stage_fingerprint:
                mismatch.append("upstream")
            if mismatch:
                changes.append({"stage": stage, "reason": "+".join(mismatch),
                                "new_fingerprint": versions[stage].stage_fingerprint})
        return changes

    def earliest_invalid_stage(self, input_summary: dict) -> tuple[str | None, list[dict]]:
        changes = self.detect_changes(input_summary)
        if not changes:
            return None, []
        earliest = min(STAGE_ORDER.index(change["stage"]) for change in changes)
        return STAGE_ORDER[earliest], changes

    def invalidate_chain(self, earliest: str, reason: str,
                         input_summary: dict) -> list[str]:
        chain = STAGE_ORDER[STAGE_ORDER.index(earliest):]
        truncate_from(self.checkpoints_dir, earliest)
        for stage in chain:
            with _suppress_missing():
                self._artifact_path(stage).unlink()
        versions = self._stage_versions(input_summary)
        entry = {
            "reason": reason,
            "earliest_stage": earliest,
            "invalidated_chain": chain,
            "new_fingerprints": {stage: versions[stage].stage_fingerprint for stage in chain},
        }
        self._log_invalidation(entry)
        return chain

    # ---- run ------------------------------------------------------------
    def run(self, reason: str = "full_run", clear_stop: bool = True) -> dict:
        if clear_stop:
            self.clear_stop()
        samples, input_summary = self.store.effective_samples()
        versions = self._stage_versions(input_summary)

        earliest, changes = self.earliest_invalid_stage(input_summary)
        reused: list[str] = []
        rebuilt: list[str] = []
        if earliest is None:
            return {"status": "up_to_date", "reused": list(STAGE_ORDER), "rebuilt": [],
                    "stop": False, "input_summary": input_summary}

        with FileLock(self.publish_lock):
            chain = self.invalidate_chain(earliest, reason, input_summary)
            results: dict[str, dict] = {}
            for index, stage in enumerate(STAGE_ORDER):
                if stage in chain:
                    self._checkpoint()  # honor a pending stop before any new unit
                    result = self._execute_stage(stage, samples, results)
                    digest = self._save_artifact(stage, result)
                    versions[stage].artifact_digest = digest
                    record = checkpoint_record(
                        stage=stage,
                        seq=index + 1,
                        input_summary=input_summary,
                        config_version=self.cfg.config_version,
                        config_fingerprint=versions[stage].config_fingerprint,
                        artifact_digest=digest,
                        stage_fingerprint=versions[stage].stage_fingerprint,
                    )
                    publish_checkpoint(self.checkpoints_dir, record)
                    results[stage] = result
                    rebuilt.append(stage)
                else:
                    reused.append(stage)
                    results[stage] = self.load_artifact(stage)
        return {"status": "ok", "reused": reused, "rebuilt": rebuilt, "stop": False,
                "input_summary": input_summary, "invalidation": changes}

    def _execute_stage(self, stage: str, samples, results: dict) -> dict:
        if stage == "normalize":
            return stage_normalize.run(samples)
        if stage == "quality":
            return stage_quality.run(results["normalize"], self.cfg.raw)
        if stage == "features":
            return stage_features.run(results["normalize"], results["quality"], self.cfg.raw)
        if stage == "sampling":
            return stage_sampling.run(results["features"], results["normalize"], self.cfg.raw)
        raise ValueError(f"unknown stage {stage}")

    # ---- graceful stop / resume ----------------------------------------
    def run_with_stop(self, reason: str = "run_with_stop") -> dict:
        """Run until completion or until the stop marker is observed.

        A pre-existing stop marker is honored (not cleared), which models a
        stop request arriving while the worker was idle.
        """
        try:
            return self.run(reason=reason, clear_stop=False)
        except StopRequested:
            last_stage, last_seq = latest_consistent(self.checkpoints_dir)
            return {"status": "stopped", "stop": True,
                    "last_completed_stage": last_stage, "last_seq": last_seq}

    def resume(self) -> dict:
        """Resume from the latest intact checkpoint, ignoring corrupt records."""
        last_stage, last_seq = latest_consistent(self.checkpoints_dir)
        return self.run(reason="resume") | {"resumed_from": last_stage, "resumed_seq": last_seq}

    # ---- verification ---------------------------------------------------
    def stage_digests(self) -> dict[str, str]:
        out = {}
        for stage in STAGE_ORDER:
            record = load_checkpoint(self.checkpoints_dir, stage)
            out[stage] = record["artifact_digest"] if record else None
        return out

    def verify_incremental(self, baseline_digests: dict[str, str],
                           sample_count: int) -> dict:
        current = self.stage_digests()
        for stage in STAGE_ORDER:
            if current[stage] != baseline_digests[stage]:
                raise IncrementalMismatchError(
                    "incremental artifact differs from full rebuild",
                    details={"first_diff_stage": stage,
                             "position": f"artifact_digest:{stage}",
                             "incremental": current[stage], "baseline": baseline_digests[stage]},
                )
        return {"status": "matched", "stages_covered": list(STAGE_ORDER),
                "sample_count": sample_count}

    # ---- status ---------------------------------------------------------
    def status(self) -> dict:
        samples, input_summary = self.store.effective_samples()
        versions = self._stage_versions(input_summary)
        checkpoints = {}
        for stage in STAGE_ORDER:
            record = load_checkpoint(self.checkpoints_dir, stage)
            checkpoints[stage] = {
                "present": record is not None,
                "seq": record["seq"] if record else None,
                "stage_fingerprint": record["stage_fingerprint"] if record else None,
                "expected_fingerprint": versions[stage].stage_fingerprint,
                "artifact_digest": record["artifact_digest"] if record else None,
                "config_fingerprint": record["config_fingerprint"] if record else None,
            }
        earliest, changes = self.earliest_invalid_stage(input_summary)
        log = []
        if self.invalidation_log_path.exists():
            log = json.loads(self.invalidation_log_path.read_text(encoding="utf-8"))
        return {
            "config_version": self.cfg.config_version,
            "config_fingerprint": self.cfg.full_fingerprint(),
            "input_summary": input_summary,
            "checkpoints": checkpoints,
            "earliest_invalid_stage": earliest,
            "pending_changes": changes,
            "invalidation_log": log,
            "stop_requested": self.stop_requested(),
        }

    def export_mixture(self) -> Path:
        result = self.load_artifact("sampling")
        target = self.cfg.path("mixture")
        atomic_write_json(target, result)
        return target


class _suppress_missing:
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return exc_type is FileNotFoundError

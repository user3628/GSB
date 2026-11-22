"""Pipeline orchestrator: staged execution, incremental recompute, recovery.

Stage fingerprints chain the sample fingerprint, the config version, the
stage-scoped config digest and the upstream stage fingerprint, so a config
change invalidates exactly the earliest affected stage and everything
downstream.  After an incremental run the artefacts are verified against a
from-scratch full rebuild; any divergence raises E_INCREMENTAL_MISMATCH
with the first differing byte position.
"""
from __future__ import annotations

import os
import shutil
import tempfile

from .atomic import atomic_write_bytes
from .checkpoint import CheckpointStore
from .config import STAGE_ORDER, config_version, stage_config_digest
from .crossing import build_crosses
from .errors import (
    E_INCREMENTAL_MISMATCH,
    E_INVALID_STAGE,
    E_STOPPED,
    PipelineError,
)
from .fingerprint import canonical_json_bytes, hash_file, hash_obj
from .inference import infer_columns, load_csv
from .reduce import reduce_features
from .summary import build_summary

GENESIS = "0" * 64


class Pipeline:
    def __init__(self, workdir: str, sample_path: str, config: dict) -> None:
        self.workdir = os.path.abspath(workdir)
        self.sample_path = os.path.abspath(sample_path)
        self.config = config
        self.derived_dir = os.path.join(self.workdir, "derived")
        self.artifacts_dir = os.path.join(self.derived_dir, "artifacts")
        os.makedirs(self.artifacts_dir, exist_ok=True)
        self.checkpoints = CheckpointStore(
            os.path.join(self.derived_dir, "checkpoints"), self.artifacts_dir
        )
        self.stop_flag_path = os.path.join(self.derived_dir, "STOP")

    # --------------------------------------------------------- fingerprints
    @property
    def sample_fingerprint(self) -> str:
        return hash_file(self.sample_path)

    @property
    def config_version(self) -> str:
        return config_version(self.config)

    def stage_fingerprints(self) -> dict[str, str]:
        fps: dict[str, str] = {}
        upstream = GENESIS
        for stage in STAGE_ORDER:
            fps[stage] = hash_obj({
                "stage": stage,
                "sample_fingerprint": self.sample_fingerprint,
                "stage_config_digest": stage_config_digest(self.config, stage),
                "upstream": upstream,
            })
            upstream = fps[stage]
        return fps

    # ------------------------------------------------------------ artefacts
    def _artifact_path(self, stage: str) -> str:
        return os.path.join(self.artifacts_dir, f"{stage}.json")

    def _write_artifact(self, stage: str, artifact: dict, stage_fp: str) -> bytes:
        envelope = {
            "meta": {
                "stage": stage,
                "sample_fingerprint": self.sample_fingerprint,
                "config_version": self.config_version,
                "stage_fingerprint": stage_fp,
                "artifact_digest": hash_obj(artifact),
            },
            "artifact": artifact,
        }
        data = canonical_json_bytes(envelope)
        atomic_write_bytes(self._artifact_path(stage), data)
        return data

    def load_artifact(self, stage: str) -> dict:
        import json

        path = self._artifact_path(stage)
        if not os.path.exists(path):
            raise PipelineError(
                E_INVALID_STAGE,
                f"artefact for stage {stage!r} is missing; run upstream stages first",
                stage=stage,
            )
        with open(path, "rb") as fh:
            return json.loads(fh.read().decode("utf-8"))

    def _reusable(self, stage: str, stage_fp: str) -> bool:
        try:
            envelope = self.load_artifact(stage)
        except PipelineError:
            return False
        meta = envelope.get("meta", {})
        return (
            meta.get("stage_fingerprint") == stage_fp
            and meta.get("sample_fingerprint") == self.sample_fingerprint
        )

    # ------------------------------------------------------------- execution
    def _compute_stage(self, stage: str, artifacts: dict[str, dict]) -> dict:
        if stage == "infer":
            header, rows = load_csv(self.sample_path)
            return infer_columns(header, rows, self.config)
        if stage == "cross":
            return build_crosses(artifacts["infer"], self.config)
        if stage == "reduce":
            return reduce_features(artifacts["cross"], self.config)
        if stage == "summary":
            return build_summary(
                artifacts["infer"], artifacts["cross"], artifacts["reduce"],
                self.stage_fingerprints(),
            )
        raise PipelineError(E_INVALID_STAGE, f"unknown stage {stage!r}", stage=stage)

    def request_stop(self) -> None:
        os.makedirs(self.derived_dir, exist_ok=True)
        atomic_write_bytes(self.stop_flag_path, b"stop\n")

    def clear_stop(self) -> None:
        if os.path.exists(self.stop_flag_path):
            os.unlink(self.stop_flag_path)

    def stop_requested(self) -> bool:
        return os.path.exists(self.stop_flag_path)

    def run(self, stop_after_stage: str | None = None) -> dict:
        """Run all four stages, recomputing only affected stages.

        ``stop_after_stage`` simulates a stop request arriving mid-run for
        the graceful-stop demonstration.
        """
        journal = self.checkpoints.verify_or_raise()
        fps = self.stage_fingerprints()
        artifacts: dict[str, dict] = {}
        reused: list[str] = []
        recomputed: list[str] = []
        completed: list[str] = []

        for stage in STAGE_ORDER:
            if self.stop_requested():
                return self._report("stopped", completed, reused, recomputed, journal)
            if self._reusable(stage, fps[stage]):
                artifacts[stage] = self.load_artifact(stage)["artifact"]
                reused.append(stage)
            else:
                for upstream in STAGE_ORDER[: STAGE_ORDER.index(stage)]:
                    if upstream not in artifacts:
                        artifacts[upstream] = self.load_artifact(upstream)["artifact"]
                artifact = self._compute_stage(stage, artifacts)
                artifacts[stage] = artifact
                self._write_artifact(stage, artifact, fps[stage])
                self.checkpoints.publish(
                    stage=stage,
                    sample_fingerprint=self.sample_fingerprint,
                    config_version=self.config_version,
                    artifact_digest=hash_obj(artifact),
                    expected_seq=self.checkpoints.next_seq(),
                )
                recomputed.append(stage)
            completed.append(stage)
            if stop_after_stage == stage:
                return self._report("stopped", completed, reused, recomputed, journal)

        report = self._report("completed", completed, reused, recomputed, journal)
        if reused and recomputed:
            report["incremental_verification"] = self.verify_against_full_rebuild()
        return report

    def _report(self, status, completed, reused, recomputed, journal) -> dict:
        return {
            "status": status,
            "completed_stages": completed,
            "reused_stages": reused,
            "recomputed_stages": recomputed,
            "stage_fingerprints": self.stage_fingerprints(),
            "journal_records_before": journal.get("records", 0),
            "sample_fingerprint": self.sample_fingerprint,
            "config_version": self.config_version,
        }

    def recover(self) -> dict:
        """Resume after a stop: verify the journal, reuse valid artefacts."""
        snapshot = self.checkpoints.verify_or_raise()
        report = self.run()
        report["recovered_from"] = snapshot
        return report

    # -------------------------------------------------------- verification
    def verify_against_full_rebuild(self) -> dict:
        """Rebuild from scratch in a temp dir and compare byte-for-byte."""
        tmp = tempfile.mkdtemp(prefix="xfpipe-full-", dir=self.derived_dir)
        try:
            fresh = Pipeline(tmp, self.sample_path, self.config)
            fresh.run()
            for stage in STAGE_ORDER:
                current = canonical_json_bytes(self.load_artifact(stage)["artifact"])
                reference = canonical_json_bytes(fresh.load_artifact(stage)["artifact"])
                if current != reference:
                    offset = next(
                        (i for i, (a, b) in enumerate(zip(current, reference)) if a != b),
                        min(len(current), len(reference)),
                    )
                    raise PipelineError(
                        E_INCREMENTAL_MISMATCH,
                        f"incremental result diverges from full rebuild at stage {stage!r}",
                        stage=stage,
                        first_diff_offset=offset,
                        current_length=len(current),
                        reference_length=len(reference),
                    )
            return {
                "status": "match",
                "stages_compared": STAGE_ORDER,
                "pipeline_digest": hash_obj(
                    [hash_file(self._artifact_path(s)) for s in STAGE_ORDER]
                ),
            }
        finally:
            shutil.rmtree(tmp, ignore_errors=True)

    def status(self) -> dict:
        fps = self.stage_fingerprints()
        stages = {}
        for stage in STAGE_ORDER:
            try:
                envelope = self.load_artifact(stage)
                meta = envelope["meta"]
                stages[stage] = {
                    "present": True,
                    "reusable": self._reusable(stage, fps[stage]),
                    "artifact_digest": meta["artifact_digest"],
                    "stage_fingerprint": meta["stage_fingerprint"],
                }
            except PipelineError:
                stages[stage] = {"present": False, "stage_fingerprint": fps[stage]}
        return {
            "sample_fingerprint": self.sample_fingerprint,
            "config_version": self.config_version,
            "stages": stages,
            "journal": self.checkpoints.classify_snapshot(),
            "stop_requested": self.stop_requested(),
        }

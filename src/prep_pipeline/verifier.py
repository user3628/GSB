from __future__ import annotations

import json
import shutil
import uuid
from pathlib import Path
from typing import Any

import numpy as np

from .canonical import digest_bytes, digest_object
from .config import Settings
from .errors import PipelineError
from .models import STAGE_ORDER
from .pipeline import PipelineComputer
from .storage import Database, utc_now


STAGE_SUGGESTIONS: dict[str, list[str]] = {
    "parse_clean": [
        "check the persisted source snapshot byte-for-byte and its UTF-8 decoding",
        "check line field count, timestamp/length parsing, sample_id rules and duplicate ordering",
        "check cleaning.trim_whitespace, cleaning.lowercase_labels and allowed_labels",
    ],
    "filter": [
        "check filtering timestamp and text-length bounds",
        "check filtering.drop_labels and the persisted policy version",
        "check whether input records reached this stage in the recorded order",
    ],
    "shard": [
        "check sharding.shard_count, sharding.salt and sha256_mod_v1 implementation",
        "check shard-key normalization and the no-overlap/no-missing invariant",
        "check whether a skew warning was mistaken for an assignment change",
    ],
    "sample": [
        "check policy.seed, sample_ratio and the persisted sample-stage input digest",
        "check HMAC-SHA256 stage-seed derivation and per-shard rank tie-breaking",
        "check that no global random source or wall-clock value entered selection",
    ],
    "tensorize": [
        "check NumPy version, normalization bounds/clipping and float32 casting",
        "check shape/dtype/null_mask contract and manifest canonical JSON",
        "check write-then-readback file digests and published shard paths",
    ],
}


class ResultVerifier:
    def __init__(self, settings: Settings, db: Database):
        self.settings = settings
        self.db = db

    def verify(
        self,
        batch_id: str,
        *,
        policy_version: int | None = None,
        baseline_batch_id: str | None = None,
    ) -> dict[str, Any]:
        batch = self.db.get_committed_batch(batch_id)

        baseline: dict[str, Any] | None
        comparison_mode: str
        if baseline_batch_id is not None:
            baseline = self.db.get_committed_batch(baseline_batch_id)
            if baseline["source_sha256"] != batch["source_sha256"]:
                raise PipelineError(
                    "E_REPRO_MISMATCH",
                    "baseline batch was produced from a different source digest",
                    {
                        "batch_id": batch_id,
                        "baseline_batch_id": baseline_batch_id,
                        "batch_source_sha256": batch["source_sha256"],
                        "baseline_source_sha256": baseline["source_sha256"],
                        "first_divergence_stage": "parse_clean",
                    },
                )
            selected_version = policy_version or baseline["policy_version"]
            policy = self.db.get_policy(selected_version)
            if baseline["policy_version"] != selected_version:
                raise PipelineError(
                    "E_REQUEST_INVALID",
                    "selected policy_version must match the explicit baseline batch",
                    {
                        "selected_policy_version": selected_version,
                        "baseline_policy_version": baseline["policy_version"],
                    },
                )
            comparison_mode = "explicit_baseline"
        else:
            selected_version = policy_version or batch["policy_version"]
            policy = self.db.get_policy(selected_version)
        if baseline_batch_id is not None:
            pass
        elif selected_version == batch["policy_version"]:
            baseline = batch
            comparison_mode = "self_recompute"
        else:
            auto = self.db.find_baseline_batch(batch["source_sha256"], selected_version)
            baseline = self.db.get_committed_batch(auto["batch_id"]) if auto is not None else None
            comparison_mode = "historical_policy_baseline" if baseline is not None else "replay_only"

        source_relative = "source/input.txt"
        source_root_batch = baseline["batch_id"] if baseline is not None else batch_id
        source_path = self.settings.published_dir / source_root_batch / source_relative
        if not source_path.is_file():
            report = self._mismatch_report(
                batch=batch,
                baseline=baseline,
                selected_version=selected_version,
                comparison_mode=comparison_mode,
                first_stage="parse_clean",
                stored_digest=None,
                recomputed_digest=None,
                message="persisted source snapshot is missing; replay cannot be verified",
                extra={"source_snapshot_path": str(source_path)},
            )
            self.db.record_verification(report)
            raise self._mismatch_error(report)
        source_bytes = source_path.read_bytes()
        source_sha256 = digest_bytes(source_bytes)
        if source_sha256 != batch["source_sha256"]:
            report = self._mismatch_report(
                batch=batch,
                baseline=baseline,
                selected_version=selected_version,
                comparison_mode=comparison_mode,
                first_stage="parse_clean",
                stored_digest=batch["source_sha256"],
                recomputed_digest=source_sha256,
                message="persisted source snapshot no longer matches the batch source digest",
                extra={"source_snapshot_path": str(source_path)},
            )
            self.db.record_verification(report)
            raise self._mismatch_error(report)
        if baseline is not None and source_sha256 != baseline["source_sha256"]:
            report = self._mismatch_report(
                batch=batch,
                baseline=baseline,
                selected_version=selected_version,
                comparison_mode=comparison_mode,
                first_stage="parse_clean",
                stored_digest=baseline["source_sha256"],
                recomputed_digest=source_sha256,
                message="persisted source snapshot no longer matches the baseline source digest",
                extra={"source_snapshot_path": str(source_path)},
            )
            self.db.record_verification(report)
            raise self._mismatch_error(report)

        verification_id = f"verify_{uuid.uuid4().hex}"
        recompute_root = self.settings.verification_dir / verification_id
        recompute_root.mkdir(parents=True, exist_ok=False)
        try:
            execution = PipelineComputer(source_bytes, policy, recompute_root).execute()
            report = self._compare(
                batch=batch,
                baseline=baseline,
                comparison_mode=comparison_mode,
                selected_version=selected_version,
                verification_id=verification_id,
                recomputed=execution,
            )
        finally:
            shutil.rmtree(recompute_root, ignore_errors=True)

        self.db.record_verification(report)
        if report["status"] == "mismatch":
            raise self._mismatch_error(report)
        return report

    def _compare(
        self,
        *,
        batch: dict[str, Any],
        baseline: dict[str, Any] | None,
        comparison_mode: str,
        selected_version: int,
        verification_id: str,
        recomputed,
    ) -> dict[str, Any]:
        recomputed_stages = {stage.name: stage for stage in recomputed.stages}
        stage_results = []
        first_divergence: dict[str, Any] | None = None
        baseline_stages = baseline["stages"] if baseline is not None else []
        baseline_by_name = {stage["stage"]: stage for stage in baseline_stages}

        if baseline is not None:
            for sequence, stage_name in enumerate(STAGE_ORDER, start=1):
                actual = recomputed_stages.get(stage_name)
                expected = baseline_by_name.get(stage_name)
                result = {
                    "stage": stage_name,
                    "sequence": sequence,
                    "stored_input_digest": expected["input_digest"] if expected else None,
                    "stored_output_digest": expected["output_digest"] if expected else None,
                    "recomputed_input_digest": actual.input_digest if actual else None,
                    "recomputed_output_digest": actual.output_digest if actual else None,
                    "matches": bool(
                        expected is not None
                        and actual is not None
                        and expected["input_digest"] == actual.input_digest
                        and expected["output_digest"] == actual.output_digest
                    ),
                }
                stage_results.append(result)
                if not result["matches"] and first_divergence is None:
                    divergence_kind = []
                    if expected is None:
                        divergence_kind.append("missing_stored_stage")
                    if actual is None:
                        divergence_kind.append("missing_recomputed_stage")
                    if expected is not None and actual is not None:
                        if expected["input_digest"] != actual.input_digest:
                            divergence_kind.append("input_digest")
                        if expected["output_digest"] != actual.output_digest:
                            divergence_kind.append("output_digest")
                    first_divergence = {
                        "stage": stage_name,
                        "sequence": sequence,
                        "kind": divergence_kind,
                        "stored_digest": result["stored_output_digest"],
                        "recomputed_digest": result["recomputed_output_digest"],
                        "stored_input_digest": result["stored_input_digest"],
                        "recomputed_input_digest": result["recomputed_input_digest"],
                    }

            if first_divergence is None:
                tensor_issues = self._verify_tensors(baseline, recomputed)
                if tensor_issues:
                    issue = tensor_issues[0]
                    first_divergence = {
                        "stage": "tensorize",
                        "sequence": 5,
                        "kind": ["physical_tensor_readback"],
                        "stored_digest": issue.get("stored"),
                        "recomputed_digest": issue.get("actual"),
                        "stored_input_digest": baseline_by_name.get("tensorize", {}).get(
                            "input_digest"
                        ),
                        "recomputed_input_digest": recomputed_stages["tensorize"].input_digest,
                        "physical_issues": tensor_issues,
                    }
                    stage_results[-1]["matches"] = False
                    stage_results[-1]["physical_issues"] = tensor_issues

        report: dict[str, Any] = {
            "verification_id": verification_id,
            "generated_at": utc_now(),
            "batch_id": batch["batch_id"],
            "baseline_batch_id": baseline["batch_id"] if baseline is not None else None,
            "selected_policy_version": selected_version,
            "comparison_mode": comparison_mode,
            "comparable": baseline is not None,
            "status": "mismatch" if first_divergence is not None else (
                "match" if baseline is not None else "replayed_unpaired"
            ),
            "first_divergence_stage": first_divergence["stage"] if first_divergence else None,
            "first_divergence": first_divergence,
            "stored_digest": first_divergence["stored_digest"] if first_divergence else None,
            "recomputed_digest": (
                first_divergence["recomputed_digest"] if first_divergence else None
            ),
            "stage_results": stage_results,
            "recomputed_stage_digests": {
                stage.name: {
                    "input_digest": stage.input_digest,
                    "output_digest": stage.output_digest,
                    "stats": stage.stats,
                }
                for stage in recomputed.stages
            },
            "suggestion_scope": self._suggestions(
                first_divergence["stage"] if first_divergence else None,
                batch["batch_id"],
                baseline["batch_id"] if baseline is not None else None,
                selected_version,
            ),
            "artifact_scope": {
                "source_snapshot": str(
                    self.settings.published_dir
                    / (baseline["batch_id"] if baseline is not None else batch["batch_id"])
                    / "source/input.txt"
                ),
                "baseline_shard_root": (
                    str(self.settings.published_dir / baseline["batch_id"])
                    if baseline is not None
                    else None
                ),
                "sqlite_stage_table": "stage_facts",
                "recompute_artifacts_retained": False,
            },
        }
        if first_divergence is not None:
            report["message"] = (
                f"recomputed stage summary diverged first at {first_divergence['stage']}"
            )
        elif baseline is None:
            report["message"] = (
                "historical policy replay completed, but no committed baseline batch with the "
                "same source digest and policy version was available for comparison"
            )
        else:
            report["message"] = "all persisted stage summaries and tensor readbacks match"
        return report

    def _verify_tensors(self, baseline: dict[str, Any], recomputed) -> list[dict[str, Any]]:
        issues: list[dict[str, Any]] = []
        baseline_shards = {shard["shard_id"]: shard for shard in baseline["shards"]}
        for artifact in recomputed.shards:
            stored = baseline_shards.get(artifact.shard_id)
            if stored is None:
                issues.append(
                    {
                        "shard_id": artifact.shard_id,
                        "kind": "missing_stored_shard",
                        "stored": None,
                        "actual": artifact.tensor_digest,
                    }
                )
                continue
            if (
                stored["sample_count"] != artifact.sample_count
                or stored["tensor_digest"] != artifact.tensor_digest
                or stored["manifest_sha256"] != artifact.manifest_sha256
            ):
                issues.append(
                    {
                        "shard_id": artifact.shard_id,
                        "kind": "stored_shard_summary_mismatch",
                        "stored": {
                            "sample_count": stored["sample_count"],
                            "tensor_digest": stored["tensor_digest"],
                            "manifest_sha256": stored["manifest_sha256"],
                        },
                        "actual": {
                            "sample_count": artifact.sample_count,
                            "tensor_digest": artifact.tensor_digest,
                            "manifest_sha256": artifact.manifest_sha256,
                        },
                    }
                )
            shard_root = self.settings.artifact_dir / stored["relative_path"]
            manifest_path = shard_root / "manifest.json"
            if not manifest_path.is_file():
                issues.append(
                    {
                        "shard_id": artifact.shard_id,
                        "kind": "missing_manifest",
                        "stored": stored["manifest_sha256"],
                        "actual": None,
                    }
                )
                continue
            manifest_bytes = manifest_path.read_bytes()
            if digest_bytes(manifest_bytes) != stored["manifest_sha256"]:
                issues.append(
                    {
                        "shard_id": artifact.shard_id,
                        "kind": "manifest_file_digest_mismatch",
                        "stored": stored["manifest_sha256"],
                        "actual": digest_bytes(manifest_bytes),
                    }
                )
                continue
            manifest = json.loads(manifest_bytes)
            checksum_target = dict(manifest)
            declared_checksum = checksum_target.pop("manifest_checksum", None)
            if declared_checksum != digest_object(checksum_target):
                issues.append(
                    {
                        "shard_id": artifact.shard_id,
                        "kind": "manifest_checksum_mismatch",
                        "stored": declared_checksum,
                        "actual": digest_object(checksum_target),
                    }
                )
            for array_name, metadata in manifest.get("arrays", {}).items():
                array_path = shard_root / metadata["path"]
                if not array_path.is_file():
                    issues.append(
                        {
                            "shard_id": artifact.shard_id,
                            "array": array_name,
                            "kind": "missing_array",
                            "stored": metadata["sha256"],
                            "actual": None,
                        }
                    )
                    continue
                actual_digest = digest_bytes(array_path.read_bytes())
                if actual_digest != metadata["sha256"]:
                    issues.append(
                        {
                            "shard_id": artifact.shard_id,
                            "array": array_name,
                            "kind": "array_file_digest_mismatch",
                            "stored": metadata["sha256"],
                            "actual": actual_digest,
                        }
                    )
                    continue
                loaded = np.load(array_path, allow_pickle=False)
                if loaded.dtype.name != metadata["dtype"] or list(loaded.shape) != metadata["shape"]:
                    issues.append(
                        {
                            "shard_id": artifact.shard_id,
                            "array": array_name,
                            "kind": "array_dtype_or_shape_mismatch",
                            "stored": {
                                "dtype": metadata["dtype"],
                                "shape": metadata["shape"],
                            },
                            "actual": {
                                "dtype": loaded.dtype.name,
                                "shape": list(loaded.shape),
                            },
                        }
                    )
            null_mask_meta = manifest.get("null_mask", {})
            mask_path = shard_root / null_mask_meta.get("path", "null_mask.npy")
            if mask_path.is_file():
                loaded_mask = np.load(mask_path, allow_pickle=False)
                if int(loaded_mask.sum()) != int(null_mask_meta.get("null_count", -1)):
                    issues.append(
                        {
                            "shard_id": artifact.shard_id,
                            "array": "null_mask",
                            "kind": "null_mask_count_mismatch",
                            "stored": int(null_mask_meta.get("null_count", -1)),
                            "actual": int(loaded_mask.sum()),
                        }
                    )
            features_meta = manifest.get("arrays", {}).get("features")
            if features_meta is not None and features_meta["shape"][0] != manifest["sample_count"]:
                issues.append(
                    {
                        "shard_id": artifact.shard_id,
                        "kind": "sample_count_shape_mismatch",
                        "stored": manifest["sample_count"],
                        "actual": features_meta["shape"][0],
                    }
                )
        recomputed_ids = {artifact.shard_id for artifact in recomputed.shards}
        for shard_id in sorted(set(baseline_shards) - recomputed_ids):
            issues.append(
                {
                    "shard_id": shard_id,
                    "kind": "unexpected_stored_shard",
                    "stored": baseline_shards[shard_id]["tensor_digest"],
                    "actual": None,
                }
            )
        return issues

    def _suggestions(
        self,
        stage: str | None,
        batch_id: str,
        baseline_batch_id: str | None,
        policy_version: int,
    ) -> dict[str, Any]:
        return {
            "first_stage": stage,
            "checks": STAGE_SUGGESTIONS.get(stage, ["run a full replay and inspect all stage facts"]),
            "sqlite_queries": [
                f"SELECT * FROM stage_facts WHERE batch_id='{batch_id}' ORDER BY stage_seq",
                f"SELECT * FROM policies WHERE version={policy_version}",
                "SELECT * FROM verification_runs ORDER BY created_at DESC LIMIT 5",
            ],
            "artifact_paths": [
                str(self.settings.published_dir / (baseline_batch_id or batch_id)),
                str(self.settings.published_dir / batch_id / "source/input.txt"),
            ],
        }

    def _mismatch_report(
        self,
        *,
        batch: dict[str, Any],
        baseline: dict[str, Any] | None,
        selected_version: int,
        comparison_mode: str,
        first_stage: str,
        stored_digest: str | None,
        recomputed_digest: str | None,
        message: str,
        extra: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        return {
            "verification_id": f"verify_{uuid.uuid4().hex}",
            "generated_at": utc_now(),
            "batch_id": batch["batch_id"],
            "baseline_batch_id": baseline["batch_id"] if baseline is not None else None,
            "selected_policy_version": selected_version,
            "comparison_mode": comparison_mode,
            "comparable": baseline is not None,
            "status": "mismatch",
            "message": message,
            "first_divergence_stage": first_stage,
            "stored_digest": stored_digest,
            "recomputed_digest": recomputed_digest,
            "stage_results": [],
            "suggestion_scope": self._suggestions(
                first_stage,
                batch["batch_id"],
                baseline["batch_id"] if baseline is not None else None,
                selected_version,
            ),
            "extra": extra or {},
        }

    def _mismatch_error(self, report: dict[str, Any]) -> PipelineError:
        return PipelineError(
            "E_REPRO_MISMATCH",
            report["message"],
            {
                "first_divergence_stage": report["first_divergence_stage"],
                "stored_digest": report["stored_digest"],
                "recomputed_digest": report["recomputed_digest"],
                "verification": report,
            },
        )

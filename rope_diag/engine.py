"""Three-stage deterministic execution engine with checkpoint and reuse support."""

from __future__ import annotations

import time
from pathlib import Path
from typing import Any, Callable, Mapping

import numpy as np

from .artifacts import ArtifactRef, ArtifactStore
from .attention import compute_scores_and_weights, scoring_statistics
from .checkpoints import CheckpointStore
from .config import RopeConfig, STAGE_ORDER
from .data import ShardData
from .digest import digest_arrays, digest_json
from .errors import PipelineStopped, RopeError
from .memory import MemoryDecision, plan_frequency, plan_rotation, plan_scoring
from .rope import build_frequency_artifact, rotate_tensors, rotation_block_layout


class RopeEngine:
    """Executes frequency, rotation, and scoring stages for local shards."""

    def __init__(
        self,
        workspace: str | Path,
        shards: list[ShardData],
        config: RopeConfig,
        *,
        external_stop: Callable[[], bool] | None = None,
    ) -> None:
        self.workspace = Path(workspace).resolve()
        self.shards = shards
        self.config = config
        self.artifacts = ArtifactStore(self.workspace)
        self.checkpoints = CheckpointStore(self.workspace)
        self.external_stop = external_stop or (lambda: False)

    def _should_stop(self) -> bool:
        return bool(self.external_stop())

    def run(
        self,
        *,
        reuse_refs: Mapping[str, Mapping[str, ArtifactRef]] | None = None,
        stop_after_stage: str | None = None,
    ) -> dict[str, Any]:
        if stop_after_stage is not None and stop_after_stage not in STAGE_ORDER:
            raise RopeError(
                "E_INVALID_STAGE",
                "停止条件引用了未知阶段",
                {"stage": stop_after_stage, "valid_stages": list(STAGE_ORDER)},
            )
        reuse_refs = reuse_refs or {}
        started = time.perf_counter()
        report: dict[str, Any] = {
            "config_version": self.config.version,
            "shards": {},
            "stopped": False,
            "stopped_at": None,
            "elapsed_ms": 0.0,
        }
        for shard in self.shards:
            shard_report = self._run_shard(
                shard,
                dict(reuse_refs.get(shard.shard_id, {})),
                stop_after_stage=stop_after_stage,
            )
            report["shards"][shard.shard_id] = shard_report
            if shard_report["stopped"]:
                report["stopped"] = True
                report["stopped_at"] = shard_report["stopped_at"]
                report["elapsed_ms"] = round(
                    (time.perf_counter() - started) * 1000.0, 6
                )
                return report
        report["elapsed_ms"] = round((time.perf_counter() - started) * 1000.0, 6)
        return report

    def _run_shard(
        self,
        shard: ShardData,
        reuse_refs: dict[str, ArtifactRef],
        *,
        stop_after_stage: str | None,
    ) -> dict[str, Any]:
        records = self.checkpoints.progress(
            shard.shard_id, self.config.version, shard.content_fingerprint
        )
        refs: dict[str, ArtifactRef] = {
            record.stage: record.artifact for record in records
        }
        stage_reports: dict[str, Any] = {
            record.stage: {
                "status": "checkpointed",
                "sequence": record.sequence,
                "artifact": record.artifact.to_dict(),
                "scheduler": {
                    "mode": "checkpoint_reuse",
                    "checkpoint_sequence": record.sequence,
                },
                "publication_status": record.publication_status,
            }
            for record in records
        }

        for stage in STAGE_ORDER:
            if stage in refs:
                continue
            if self._should_stop():
                return {
                    "stages": stage_reports,
                    "refs": refs,
                    "complete": False,
                    "stopped": True,
                    "stopped_at": stage,
                }

            seed_ref = reuse_refs.get(stage)
            if seed_ref is not None:
                status = "reused"
                publication_status = "reused"
                scheduler: dict[str, Any] = {
                    "mode": "reused_upstream",
                    "reason": "配置变更未影响该阶段",
                }
                ref = seed_ref
                metadata = self.artifacts.metadata(ref)
            else:
                status = "computed"
                publication_status = "computed"
                try:
                    arrays, metadata, decision = self._compute_stage(shard, stage, refs)
                except PipelineStopped:
                    return {
                        "stages": stage_reports,
                        "refs": refs,
                        "complete": False,
                        "stopped": True,
                        "stopped_at": stage,
                    }
                ref = self.artifacts.put(
                    stage,
                    shard.shard_id,
                    arrays,
                    metadata={
                        **metadata,
                        "source_config_version": self.config.version,
                        "source_shard_fingerprint": shard.content_fingerprint,
                    },
                )
                scheduler = decision.to_dict()

            sequence = len(refs) + 1
            record = self.checkpoints.publish(
                shard_id=shard.shard_id,
                shard_fingerprint=shard.content_fingerprint,
                config_version=self.config.version,
                stage=stage,
                sequence=sequence,
                artifact=ref,
                publication_status=publication_status,
            )
            refs[stage] = ref
            stage_reports[stage] = {
                "status": status,
                "sequence": sequence,
                "artifact": ref.to_dict(),
                "scheduler": scheduler,
                "publication_status": record.publication_status,
                "metadata": metadata,
            }

            if stop_after_stage == stage:
                return {
                    "stages": stage_reports,
                    "refs": refs,
                    "complete": False,
                    "stopped": True,
                    "stopped_at": stage,
                }
            if self._should_stop():
                return {
                    "stages": stage_reports,
                    "refs": refs,
                    "complete": False,
                    "stopped": True,
                    "stopped_at": stage,
                }

        return {
            "stages": stage_reports,
            "refs": refs,
            "complete": len(refs) == len(STAGE_ORDER),
            "stopped": False,
            "stopped_at": None,
        }

    def _compute_stage(
        self,
        shard: ShardData,
        stage: str,
        refs: Mapping[str, ArtifactRef],
    ) -> tuple[dict[str, np.ndarray], dict[str, Any], MemoryDecision]:
        if stage == "frequency_generation":
            decision = plan_frequency(shard, self.config)
            if self._should_stop():
                raise PipelineStopped(stage, "pre_frequency")
            artifact = build_frequency_artifact(
                shard.sequence_length, shard.head_dim, self.config
            )
            return artifact["arrays"], artifact["metadata"], decision

        if stage == "block_rotation":
            if "frequency_generation" not in refs:
                raise RopeError(
                    "E_INVALID_STAGE",
                    "分块旋转缺少频率生成前置产物",
                    {"shard_id": shard.shard_id, "missing_stage": "frequency_generation"},
                )
            decision = plan_rotation(shard, self.config)
            frequency = self.artifacts.load(refs["frequency_generation"])
            rotated = rotate_tensors(
                shard.query,
                shard.key,
                frequency["angle_table"],
                self.config,
                batch_chunk_size=decision.batch_chunk_size,
                should_stop=self._should_stop,
            )
            metadata = {
                "shard_id": shard.shard_id,
                "frequency_artifact_digest": refs[
                    "frequency_generation"
                ].artifact_digest,
                "rotation_block_size": int(self.config.rotation_block_size),
                "block_layout": rotation_block_layout(
                    shard.head_dim, self.config.rotation_block_size
                ),
                "unrotated_channels": np.flatnonzero(
                    ~frequency["rotated_mask"]
                ).astype(int).tolist(),
                "odd_head_dim_recorded": bool(shard.head_dim % 2 == 1),
            }
            return rotated, metadata, decision

        if stage == "scoring_normalization":
            if "block_rotation" not in refs:
                raise RopeError(
                    "E_INVALID_STAGE",
                    "打分归一化缺少分块旋转前置产物",
                    {"shard_id": shard.shard_id, "missing_stage": "block_rotation"},
                )
            decision = plan_scoring(shard, self.config)
            rotated = self.artifacts.load(refs["block_rotation"])
            arrays = compute_scores_and_weights(
                rotated["query_rotated"],
                rotated["key_rotated"],
                self.config,
                shard_id=shard.shard_id,
                row_chunk_size=decision.row_chunk_size,
                should_stop=self._should_stop,
            )
            statistics = scoring_statistics(arrays, shard_id=shard.shard_id)
            metadata = {
                "shard_id": shard.shard_id,
                "rotation_artifact_digest": refs["block_rotation"].artifact_digest,
                "causal_mask": bool(self.config.causal_mask),
                "numeric_tolerance": float(self.config.numeric_tolerance),
                "statistics": statistics,
                "statistics_digest": digest_json(statistics),
            }
            return arrays, metadata, decision

        raise RopeError(
            "E_INVALID_STAGE", "未知计算阶段", {"stage": stage, "valid_stages": list(STAGE_ORDER)}
        )

    def verify_full(
        self, stage_refs: Mapping[str, Mapping[str, ArtifactRef]]
    ) -> dict[str, Any]:
        """Recompute every stage from zero and compare byte-level digests."""

        checked: dict[str, dict[str, str]] = {
            stage: {} for stage in STAGE_ORDER
        }
        for shard in self.shards:
            refs = stage_refs.get(shard.shard_id, {})
            for stage in STAGE_ORDER:
                if stage not in refs:
                    raise RopeError(
                        "E_INVALID_STAGE",
                        "全量一致性校验缺少阶段产物",
                        {"shard_id": shard.shard_id, "missing_stage": stage},
                    )

            frequency_artifact = build_frequency_artifact(
                shard.sequence_length, shard.head_dim, self.config
            )
            fresh_frequency = frequency_artifact["arrays"]
            self._compare_stage_digest(
                shard, "frequency_generation", refs["frequency_generation"], fresh_frequency
            )
            checked["frequency_generation"][shard.shard_id] = refs[
                "frequency_generation"
            ].artifact_digest

            rotation_decision = plan_rotation(shard, self.config)
            fresh_rotation = rotate_tensors(
                shard.query,
                shard.key,
                fresh_frequency["angle_table"],
                self.config,
                batch_chunk_size=rotation_decision.batch_chunk_size,
            )
            self._compare_stage_digest(
                shard, "block_rotation", refs["block_rotation"], fresh_rotation
            )
            checked["block_rotation"][shard.shard_id] = refs[
                "block_rotation"
            ].artifact_digest

            scoring_decision = plan_scoring(shard, self.config)
            fresh_scoring = compute_scores_and_weights(
                fresh_rotation["query_rotated"],
                fresh_rotation["key_rotated"],
                self.config,
                shard_id=shard.shard_id,
                row_chunk_size=scoring_decision.row_chunk_size,
            )
            self._compare_stage_digest(
                shard, "scoring_normalization", refs["scoring_normalization"], fresh_scoring
            )
            checked["scoring_normalization"][shard.shard_id] = refs[
                "scoring_normalization"
            ].artifact_digest

            fresh_stats = scoring_statistics(fresh_scoring, shard_id=shard.shard_id)
            current_metadata = self.artifacts.metadata(refs["scoring_normalization"])
            if current_metadata.get("statistics") != fresh_stats:
                raise RopeError(
                    "E_INCREMENTAL_MISMATCH",
                    "增量统计摘要与从零全量构建不一致",
                    {
                        "differing_stage": "scoring_normalization",
                        "shard_id": shard.shard_id,
                        "incremental_statistics_digest": current_metadata.get(
                            "statistics_digest"
                        ),
                        "full_statistics_digest": digest_json(fresh_stats),
                    },
                )

        return {
            "verified": True,
            "all_byte_identical": True,
            "reference": "fresh-recompute-from-zero",
            "stage_artifact_digests": checked,
        }

    def _compare_stage_digest(
        self,
        shard: ShardData,
        stage: str,
        current_ref: ArtifactRef,
        fresh_arrays: Mapping[str, np.ndarray],
    ) -> None:
        fresh_digest = digest_arrays(dict(fresh_arrays))
        if fresh_digest != current_ref.artifact_digest:
            raise RopeError(
                "E_INCREMENTAL_MISMATCH",
                "增量产物与从零全量构建不一致",
                {
                    "differing_stage": stage,
                    "shard_id": shard.shard_id,
                    "incremental_digest": current_ref.artifact_digest,
                    "full_digest": fresh_digest,
                },
            )


def refs_from_report(
    report: Mapping[str, Any], *, shard_ids: list[str]
) -> dict[str, dict[str, ArtifactRef]]:
    refs: dict[str, dict[str, ArtifactRef]] = {}
    for shard_id in shard_ids:
        shard_report = dict(report.get("shards", {}).get(shard_id, {}))
        stage_refs: dict[str, ArtifactRef] = {}
        for stage, stage_report in dict(shard_report.get("stages", {})).items():
            stage_refs[str(stage)] = ArtifactRef.from_dict(stage_report["artifact"])
        refs[shard_id] = stage_refs
    return refs

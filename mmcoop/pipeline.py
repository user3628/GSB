from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable

from .canonical import canonical_bytes, digest_json, first_difference, sha256_bytes
from .config import Config
from .dataio import Dataset, png_digests
from .errors import BuildStopped, business_error
from .injection import Injection
from .stages import (
    STAGES,
    compute_aggregation,
    compute_fusion_scoring,
    compute_projection_alignment,
    compute_text_encoding,
)


REASON_LABELS = {
    "texts": "text_content_changed",
    "encoding": "encoding_configuration_changed",
    "structure_version": "structure_version_changed",
    "upstream": "upstream_stage_changed",
    "images": "visual_features_or_links_changed",
    "projection": "projection_matrix_changed",
    "text_metadata": "language_or_source_metadata_changed",
    "image_operators": "source_operator_changed",
    "weights": "fusion_weights_changed",
    "thresholds": "fusion_thresholds_changed",
    "confidence": "operator_confidence_configuration_changed",
    "language": "language_configuration_changed",
    "text_ids": "text_structure_changed",
    "aggregation": "aggregation_configuration_changed",
    "missing": "missing_checkpoint",
}


@dataclass
class Computation:
    artifacts: dict[str, dict[str, Any]]
    digests: dict[str, str]
    input_fingerprints: dict[str, str]
    components: dict[str, dict[str, Any]]
    reused_stages: list[str] = field(default_factory=list)
    rebuilt_stages: list[str] = field(default_factory=list)
    invalidations: list[dict[str, Any]] = field(default_factory=list)
    terminal_stage: str = "aggregation"

    @property
    def version_fingerprint(self) -> str:
        return digest_json(
            {
                "input_fingerprints": self.input_fingerprints,
                "artifact_sha256": self.digests,
            }
        )

    @property
    def pipeline_fingerprint(self) -> str:
        return self.digests[self.terminal_stage]

    def public_version(self) -> dict[str, Any]:
        return {
            "version_fingerprint": self.version_fingerprint,
            "pipeline_fingerprint": self.pipeline_fingerprint,
            "stage_input_fingerprints": dict(sorted(self.input_fingerprints.items())),
            "stage_artifact_sha256": dict(sorted(self.digests.items())),
        }


class PipelineEngine:
    def __init__(self, config: Config, dataset: Dataset, data_dir: str | Path) -> None:
        self.config = config
        self.dataset = dataset
        self.data_dir = Path(data_dir)

    def _stage1_components(self) -> dict[str, Any]:
        texts = [
            {"text_id": record["text_id"], "text": record["text"]}
            for record in sorted(self.dataset.texts, key=lambda item: item["text_id"])
        ]
        return {
            "structure_version": self.config.structure_version,
            "texts": digest_json(texts),
            "encoding": digest_json(
                {
                    "embedding_dim": self.config.embedding_dim,
                    "hash_salt": self.config.hash_salt,
                    "max_chars": self.config.max_chars,
                    "normalization": self.config.text.get("normalization", ""),
                }
            ),
        }

    def _stage2_components(self, upstream_digest: str) -> dict[str, Any]:
        png_hashes = png_digests(self.dataset.images, self.data_dir)
        images = [
            {
                "image_id": record["image_id"],
                "text_id": record["text_id"],
                "feature": record["feature"],
                "png": record["png"],
                "png_sha256": png_hashes.get(record["image_id"]),
            }
            for record in sorted(self.dataset.images, key=lambda item: item["image_id"])
        ]
        return {
            "structure_version": self.config.structure_version,
            "upstream": upstream_digest,
            "images": digest_json(images),
            "projection": digest_json(
                {
                    "input_dim": self.config.projection_input_dim,
                    "output_dim": self.config.projection_output_dim,
                    "matrix": self.config.matrix,
                }
            ),
        }

    def _stage3_components(self, upstream_digest: str) -> dict[str, Any]:
        text_metadata = [
            {"text_id": record["text_id"], "lang": record["lang"], "source_channel": record["source_channel"]}
            for record in sorted(self.dataset.texts, key=lambda item: item["text_id"])
        ]
        image_operators = [
            {"image_id": record["image_id"], "source_operator": record["source_operator"]}
            for record in sorted(self.dataset.images, key=lambda item: item["image_id"])
        ]
        return {
            "structure_version": self.config.structure_version,
            "upstream": upstream_digest,
            "text_metadata": digest_json(text_metadata),
            "image_operators": digest_json(image_operators),
            "weights": digest_json(self.config.weights),
            "thresholds": digest_json(self.config.thresholds),
            "confidence": digest_json(self.config.operator_confidence),
            "language": digest_json(self.config.language_config),
        }

    def _stage4_components(self, upstream_digest: str) -> dict[str, Any]:
        return {
            "structure_version": self.config.structure_version,
            "upstream": upstream_digest,
            "text_ids": digest_json(sorted(record["text_id"] for record in self.dataset.texts)),
            "aggregation": digest_json(self.config.aggregation),
        }

    @staticmethod
    def _fingerprint(stage: str, components: dict[str, Any]) -> str:
        return digest_json({"stage": stage, "components": components})

    def _reuse(
        self,
        stage: str,
        fingerprint: str,
        reusable: dict[str, dict[str, Any]],
        *,
        forbidden: bool,
    ) -> dict[str, Any] | None:
        if forbidden:
            return None
        candidate = reusable.get(stage)
        if candidate is None or candidate.get("input_fingerprint") != fingerprint:
            return None
        artifact = candidate.get("artifact")
        if not isinstance(artifact, dict):
            return None
        if sha256_bytes(canonical_bytes(artifact)) != candidate.get("artifact_sha256"):
            return None
        return candidate

    def compute(
        self,
        *,
        reusable: dict[str, dict[str, Any]] | None = None,
        injection: Injection | None = None,
        should_stop: Callable[[], bool] | None = None,
        check_stop_between_stages: bool = True,
        through_stage: str = "aggregation",
    ) -> Computation:
        if through_stage not in STAGES:
            raise business_error(
                "E_INVALID_STAGE",
                "build",
                "unknown target stage",
                diagnostics={"through_stage": through_stage, "valid_stages": list(STAGES)},
                http_status=409,
            )
        target_index = STAGES.index(through_stage) + 1
        reusable = reusable or {}
        forced_index = STAGES.index(injection.stage) + 1 if injection is not None else 999
        artifacts: dict[str, dict[str, Any]] = {}
        digests: dict[str, str] = {}
        fingerprints: dict[str, str] = {}
        components_by_stage: dict[str, dict[str, Any]] = {}
        reused: list[str] = []
        rebuilt: list[str] = []
        invalidations: list[dict[str, Any]] = []
        upstream: str | None = None
        invalidated = False

        for stage_index, stage in enumerate(STAGES[:target_index], start=1):
            if check_stop_between_stages and should_stop and should_stop():
                raise BuildStopped(stage, list(artifacts))
            if stage_index == 1:
                components = self._stage1_components()
            elif stage_index == 2:
                assert upstream is not None
                components = self._stage2_components(upstream)
            elif stage_index == 3:
                assert upstream is not None
                components = self._stage3_components(upstream)
            else:
                assert upstream is not None
                components = self._stage4_components(upstream)
            fingerprint = self._fingerprint(stage, components)
            components_by_stage[stage] = components
            fingerprints[stage] = fingerprint
            forbidden = invalidated or stage_index >= forced_index
            candidate = self._reuse(stage, fingerprint, reusable, forbidden=forbidden)
            if candidate is not None:
                artifact = candidate["artifact"]
                digest = candidate["artifact_sha256"]
                reused.append(stage)
            else:
                artifact = self._compute_stage(stage, artifacts, injection, should_stop)
                digest = sha256_bytes(canonical_bytes(artifact))
                rebuilt.append(stage)
                if not invalidated:
                    invalidated = True
                    invalidations.append(self._invalidation(stage, reusable.get(stage), components))
                elif stage_index > 1:
                    invalidations.append(
                        {
                            "stage": stage,
                            "cause": "upstream_invalidated",
                            "changed_components": ["upstream"],
                            "reason_codes": ["upstream_stage_changed"],
                            "earliest_affected_stage": rebuilt[0],
                        }
                    )
            if artifact["stage"] != stage:
                raise business_error(
                    "E_INCREMENTAL_MISMATCH",
                    "aggregation",
                    "stage function returned an artifact for the wrong stage",
                    sample_id=stage,
                    diagnostics={"expected_stage": stage, "actual_stage": artifact.get("stage")},
                    http_status=409,
                )
            if stage_index > 1:
                previous_stage = STAGES[stage_index - 2]
                if artifact.get("upstream", {}).get(previous_stage) != digests[previous_stage]:
                    raise business_error(
                        "E_INCREMENTAL_MISMATCH",
                        stage,
                        "artifact upstream digest does not match the produced upstream artifact",
                        sample_id=stage,
                        diagnostics={"required_upstream": previous_stage},
                        http_status=409,
                    )
            artifacts[stage] = artifact
            digests[stage] = digest
            upstream = digest

        return Computation(
            artifacts=artifacts,
            digests=digests,
            input_fingerprints=fingerprints,
            components=components_by_stage,
            reused_stages=reused,
            rebuilt_stages=rebuilt,
            invalidations=invalidations,
            terminal_stage=through_stage,
        )

    def _compute_stage(
        self,
        stage: str,
        artifacts: dict[str, dict[str, Any]],
        injection: Injection | None,
        should_stop: Callable[[], bool] | None,
    ) -> dict[str, Any]:
        if stage == "text_encoding":
            return compute_text_encoding(self.dataset, self.config, injection=injection, should_stop=should_stop)
        if stage == "projection_alignment":
            return compute_projection_alignment(
                artifacts["text_encoding"], self.dataset, self.config, injection=injection, should_stop=should_stop
            )
        if stage == "fusion_scoring":
            return compute_fusion_scoring(
                artifacts["projection_alignment"], self.dataset, self.config, injection=injection, should_stop=should_stop
            )
        if stage == "aggregation":
            return compute_aggregation(artifacts["fusion_scoring"], self.dataset, self.config, should_stop=should_stop)
        raise AssertionError(f"unknown stage {stage}")

    def _invalidation(
        self,
        stage: str,
        previous: dict[str, Any] | None,
        current_components: dict[str, Any],
    ) -> dict[str, Any]:
        if previous is None:
            return {
                "stage": stage,
                "cause": "missing_checkpoint",
                "changed_components": ["missing"],
                "reason_codes": ["missing_checkpoint"],
                "earliest_affected_stage": stage,
            }
        old_components = previous.get("components", {})
        changed = [
            key
            for key in sorted(set(old_components) | set(current_components))
            if old_components.get(key) != current_components.get(key)
        ]
        reason_codes = [REASON_LABELS.get(key, f"{key}_changed") for key in changed]
        direct = [key for key in changed if key != "upstream"]
        if stage != "text_encoding" and direct:
            cause = reason_codes[changed.index(direct[0])] if direct[0] in changed else "direct_input_changed"
        elif "upstream" in changed:
            cause = "upstream_stage_changed"
        elif changed:
            cause = reason_codes[0]
        else:
            cause = "artifact_regeneration_required"
            reason_codes = [cause]
        return {
            "stage": stage,
            "cause": cause,
            "changed_components": changed,
            "reason_codes": reason_codes,
            "earliest_affected_stage": stage,
        }

    def verify_against_full(self, incremental: Computation) -> None:
        full = self.compute(
            reusable={},
            injection=None,
            should_stop=None,
            check_stop_between_stages=False,
            through_stage=incremental.terminal_stage,
        )
        for stage in STAGES[: STAGES.index(incremental.terminal_stage) + 1]:
            incremental_bytes = canonical_bytes(incremental.artifacts[stage])
            full_bytes = canonical_bytes(full.artifacts[stage])
            if incremental_bytes != full_bytes:
                difference = first_difference(incremental.artifacts[stage], full.artifacts[stage])
                raise business_error(
                    "E_INCREMENTAL_MISMATCH",
                    stage,
                    "incremental artifact differs from a from-scratch full run",
                    sample_id=stage,
                    diagnostics={
                        "stage": stage,
                        "difference": difference,
                        "incremental_sha256": sha256_bytes(incremental_bytes),
                        "full_sha256": sha256_bytes(full_bytes),
                    },
                    http_status=409,
                )

    def data_fingerprint(self) -> str:
        return self.dataset.content_fingerprint(self.data_dir)

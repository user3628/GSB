"""Library-level orchestration for deterministic cross-modal retrieval."""

from __future__ import annotations

import copy
import math
import shutil
import tempfile
from pathlib import Path
from typing import Any, Iterable

import numpy as np

from .alignment import (
    build_projection_matrices,
    project_vector,
    projection_matrix_digest,
)
from .canonical import (
    atomic_write_json,
    digest_file,
    digest_obj,
    file_lock,
    read_json,
    try_read_json,
)
from .checkpoints import STAGES, CheckpointStore
from .configs import (
    FEATURE_CONFIG_VERSION,
    PROJECTION_CONFIG_VERSION,
    QUANT_CONFIG_VERSION,
    RANKING_CONFIG_VERSION,
    RECALL_CONFIG_VERSION,
    AlignmentConfig,
    QuantizationConfig,
    default_config,
    merge_config_patch,
)
from .dataset import Dataset, DatasetItem
from .descriptors import extract_text, extract_visual, normalize_caption
from .errors import (
    CMIRError,
    E_INCREMENTAL_MISMATCH,
    E_INVALID_QUERY,
    E_INVALID_STAGE,
    E_NUMERIC,
    invalid_query,
    numeric_error,
)
from .index import build_shard_index, inverted_hits, vector_index_keys
from .quantization import (
    Calibration,
    cosine_similarity,
    conservative_step_cosine_error_bound,
    effective_digits,
    fit_calibration,
    normalized_cosine_error_bound,
    quantize_vector,
)


STATE_SCHEMA = "cmir.state.v1"
FEATURE_SCHEMA = "cmir.feature-artifact.v1"
PROJECTED_SCHEMA = "cmir.projected-quantized.v1"
INDEX_SCHEMA = "cmir.shard-index.v1"
RECALL_SCHEMA = "cmir.recall-validation.v1"
SNAPSHOT_STAGES = ("feature", "projection_quant", "index", "recall")


def _signed_log1p(value: float) -> float:
    if value == 0.0:
        return 0.0
    return math.copysign(math.log1p(abs(value)), value)


def _first_difference(left: Any, right: Any, path: str = "$") -> dict[str, Any] | None:
    if type(left) is not type(right):
        return {"path": path, "left_type": type(left).__name__, "right_type": type(right).__name__}
    if isinstance(left, dict):
        if set(left) != set(right):
            return {
                "path": path,
                "left_keys": sorted(left),
                "right_keys": sorted(right),
            }
        for key in sorted(left):
            diff = _first_difference(left[key], right[key], f"{path}.{key}")
            if diff is not None:
                return diff
        return None
    if isinstance(left, list):
        if len(left) != len(right):
            return {"path": path, "left_length": len(left), "right_length": len(right)}
        for index, (left_item, right_item) in enumerate(zip(left, right)):
            diff = _first_difference(left_item, right_item, f"{path}[{index}]")
            if diff is not None:
                return diff
        return None
    if left != right:
        return {"path": path, "left": left, "right": right}
    return None


class RetrievalEngine:
    """Library entry point; all state is local and deterministic."""

    def __init__(
        self,
        workspace: str | Path,
        *,
        dataset_dir: str | Path | None = None,
        config: AlignmentConfig | None = None,
    ) -> None:
        self.workspace = Path(workspace).resolve()
        self.workspace.mkdir(parents=True, exist_ok=True)
        self.meta_path = self.workspace / "workspace.json"
        self.config_path = self.workspace / "config.json"
        self.state_path = self.workspace / "state.json"
        self.run_context_path = self.workspace / "checkpoints" / "run_context.json"
        self.stop_path = self.workspace / "STOP"
        self.build_lock_path = self.workspace / "locks" / "build.lock"
        self.diagnostics_dir = self.workspace / "diagnostics"
        self.artifacts_dir = self.workspace / "artifacts"
        for path in (self.artifacts_dir, self.diagnostics_dir, self.workspace / "locks"):
            path.mkdir(parents=True, exist_ok=True)
        if dataset_dir is not None:
            self._write_meta({"dataset_dir": str(Path(dataset_dir).resolve())})
        if config is not None:
            self._write_config(config)
        self.checkpoints = CheckpointStore(self.workspace)
        if self.state_path.is_file():
            # Load once so syntactically invalid state cannot masquerade as a healthy build.
            try_read_json(self.state_path)

    # ------------------------------------------------------------------ workspace
    def _meta(self) -> dict[str, Any]:
        value, error = try_read_json(self.meta_path)
        if error is not None or not isinstance(value, dict):
            return {}
        return value

    def _write_meta(self, value: dict[str, Any]) -> None:
        atomic_write_json(
            self.meta_path,
            {"schema": "cmir.workspace.v1", **value},
        )

    def _write_config(self, config: AlignmentConfig) -> None:
        atomic_write_json(self.config_path, config.to_dict())

    def load_config(self) -> AlignmentConfig:
        value, error = try_read_json(self.config_path)
        if error is not None:
            return default_config(8)
        return AlignmentConfig.from_dict(value)

    def _load_dataset(self) -> Dataset:
        meta = self._meta()
        dataset_dir = meta.get("dataset_dir")
        if not dataset_dir:
            raise CMIRError(
                E_INVALID_STAGE,
                "dataset has not been imported",
                stage="dataset_import",
            )
        return Dataset.load(dataset_dir)

    def _state(self) -> dict[str, Any] | None:
        value, error = try_read_json(self.state_path)
        if error is not None or not isinstance(value, dict):
            return None
        return value

    def _write_state(self, state: dict[str, Any]) -> None:
        atomic_write_json(self.state_path, state)

    def _write_run_context(self, context: dict[str, Any]) -> None:
        atomic_write_json(self.run_context_path, context)

    def _read_run_context(self) -> dict[str, Any] | None:
        value, error = try_read_json(self.run_context_path)
        if error is not None or not isinstance(value, dict):
            return None
        return value

    def _record_error(self, error: CMIRError) -> None:
        atomic_write_json(
            self.diagnostics_dir / "last_error.json",
            error.to_dict(),
        )

    def import_dataset(self, dataset_dir: str | Path) -> dict[str, Any]:
        try:
            active = self.checkpoints.active_plan()
            if active is not None:
                raise CMIRError(
                    E_INVALID_STAGE,
                    "cannot import a dataset while a checkpoint plan is incomplete",
                    active_plan_id=active["plan"].get("plan_id"),
                )
            dataset = Dataset.load(dataset_dir)
            dataset.assert_valid()
            resolved = str(dataset.root)
            self._write_meta({"dataset_dir": resolved})
            if not self.config_path.is_file():
                self._write_config(default_config(8))
            return {
                "ok": True,
                "dataset_dir": resolved,
                "dataset_digest": dataset.dataset_digest(),
                "shards": [
                    {"shard_id": shard.shard_id, "item_count": len(shard.items)}
                    for shard in dataset.shards
                ],
                "diagnostics": dataset.diagnostics,
            }
        except CMIRError as exc:
            self._record_error(exc)
            raise

    def set_config(
        self,
        config: AlignmentConfig | None = None,
        *,
        patch: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        active = self.checkpoints.active_plan()
        if active is not None:
            raise CMIRError(
                E_INVALID_STAGE,
                "cannot change configuration while a checkpoint plan is incomplete",
                active_plan_id=active["plan"].get("plan_id"),
                next_unit_index=active["next_unit_index"],
            )
        current = self.load_config()
        if patch is not None:
            config = merge_config_patch(current, patch)
        if config is None:
            config = current
        # Validate explicit matrices eagerly so a bad config never becomes current.
        build_projection_matrices(config.projection, config.feature)
        projection_matrix_digest(config.projection, config.feature)
        self._write_config(config)
        state = self._state() or {}
        pending = None
        if state:
            pending = self._pending_stages(state, config)
        return {
            "ok": True,
            "config_digest": config.digest,
            "stage_digests": config.stage_digests(),
            "pending_rebuild": pending,
        }

    def request_stop(self) -> dict[str, Any]:
        atomic_write_json(
            self.stop_path,
            {"schema": "cmir.stop-request.v1", "requested": True},
        )
        return {
            "ok": True,
            "stop_requested": True,
            "semantics": "finish the current atomic stage-shard unit; no force-kill exit report",
        }

    def clear_stop(self) -> None:
        try:
            self.stop_path.unlink()
        except FileNotFoundError:
            pass

    def stop_requested(self) -> bool:
        return self.stop_path.is_file()

    def resume(self, *, stop_after_units: int | None = None) -> dict[str, Any]:
        self.clear_stop()
        return self.build(stop_after_units=stop_after_units)

    # ---------------------------------------------------------------- diagnostics
    def status(self) -> dict[str, Any]:
        state = self._state()
        config = self.load_config()
        dataset_summary: dict[str, Any]
        try:
            dataset = self._load_dataset()
            dataset_summary = {
                "ok": not dataset.diagnostics,
                "dataset_dir": str(dataset.root),
                "dataset_digest": dataset.dataset_digest(),
                "shards": [
                    {
                        "shard_id": shard.shard_id,
                        "item_count": len(shard.items),
                        "content_digest": shard.content_digest(),
                    }
                    for shard in dataset.shards
                ],
                "diagnostics": dataset.diagnostics,
            }
        except CMIRError as exc:
            dataset_summary = {"ok": False, "error": exc.to_dict()["error"]}
        audit = self.checkpoints.audit()
        active = self.checkpoints.active_plan()
        return {
            "ok": True,
            "workspace": str(self.workspace),
            "dataset": dataset_summary,
            "config_digest": config.digest,
            "stage_digests": config.stage_digests(),
            "state": self._state_summary(state),
            "checkpoints": audit,
            "active_plan": active,
            "stop_requested": self.stop_requested(),
        }

    def _state_summary(self, state: dict[str, Any] | None) -> dict[str, Any] | None:
        if state is None:
            return None
        return {
            "schema": state.get("schema"),
            "dataset_digest": state.get("dataset_digest"),
            "config_digest": state.get("config_digest"),
            "run_digest": state.get("run_digest"),
            "invalidations": state.get("invalidations", []),
            "shards": {
                shard_id: {
                    "content_digest": row.get("content_digest"),
                    "cycle": row.get("cycle"),
                    "feature_digest": row["feature"]["digest"],
                    "projection_quant_digest": row["projection_quant"]["digest"],
                    "index_digest": row["index"]["digest"],
                    "recall_digest": row["recall"]["digest"],
                }
                for shard_id, row in sorted(state.get("shards", {}).items())
            },
            "checkpoint_last_seq": state.get("checkpoint_last_seq"),
        }

    # ------------------------------------------------------------- invalidation
    def _pending_stages(self, state: dict[str, Any], config: AlignmentConfig) -> dict[str, Any]:
        old = state.get("stage_digests", {})
        changed = []
        if old.get("feature") != config.feature_digest:
            changed.append({"stage": "feature", "reason": "feature_config_changed"})
        if old.get("projection") != config.projection_digest:
            changed.append({"stage": "projection_quant", "reason": "projection_changed"})
        if old.get("quantization") != config.quantization_digest:
            changed.append({"stage": "projection_quant", "reason": "quantization_changed"})
        if old.get("index") != config.ranking_digest:
            changed.append({"stage": "index", "reason": "ranking_changed"})
        if old.get("recall") != config.recall_digest:
            changed.append({"stage": "recall_validation", "reason": "recall_config_changed"})
        return {"changed_components": changed}

    def _determine_invalidation(
        self,
        dataset: Dataset,
        config: AlignmentConfig,
        state: dict[str, Any] | None,
        *,
        changed_shard: str | None,
    ) -> dict[str, Any]:
        all_shards = [shard.shard_id for shard in dataset.shards]
        content_now = dataset.content_digests()
        if state is None:
            return {
                "earliest_stage": "feature",
                "reason": "initial_build",
                "reasons": ["initial_build"],
                "affected_shards": all_shards,
                "feature_rebuild": all_shards,
                "quant_rebuild": all_shards,
                "index_rebuild": all_shards,
                "recall_rebuild": all_shards,
                "config_changes": [],
            }
        old_stage = state.get("stage_digests", {})
        old_content = state.get("content_digests", {})
        config_changes: list[str] = []
        if old_stage.get("feature") != config.feature_digest:
            config_changes.append("feature_config_changed")
        if old_stage.get("projection") != config.projection_digest:
            config_changes.append("projection_changed")
        if old_stage.get("quantization") != config.quantization_digest:
            config_changes.append("quantization_changed")
        if old_stage.get("index") != config.ranking_digest:
            config_changes.append("ranking_changed")
        if old_stage.get("recall") != config.recall_digest:
            config_changes.append("recall_config_changed")

        if changed_shard is None:
            considered = all_shards
        else:
            if changed_shard not in all_shards:
                raise CMIRError(
                    E_INVALID_STAGE,
                    "changed_shard is not present in the dataset",
                    shard_id=changed_shard,
                    available_shards=all_shards,
                )
            considered = [changed_shard]
        content_changed = [
            shard_id
            for shard_id in considered
            if shard_id not in old_content or old_content[shard_id] != content_now.get(shard_id)
        ]
        missing_shards = [shard_id for shard_id in all_shards if shard_id not in state.get("shards", {})]

        if "feature_config_changed" in config_changes or missing_shards:
            earliest = "feature"
            reason = "feature_config_changed" if "feature_config_changed" in config_changes else "new_shard"
            feature_rebuild = all_shards
            reasons = [reason]
        elif content_changed:
            earliest = "feature"
            reason = "content_changed"
            feature_rebuild = content_changed
            reasons = [f"content_changed:{shard_id}" for shard_id in content_changed]
        elif "projection_changed" in config_changes or "quantization_changed" in config_changes:
            earliest = "projection_quant"
            reason = (
                "projection_changed"
                if "projection_changed" in config_changes
                else "quantization_changed"
            )
            feature_rebuild = []
            reasons = [name for name in config_changes if name in ("projection_changed", "quantization_changed")]
        elif "ranking_changed" in config_changes:
            earliest = "index"
            reason = "ranking_changed"
            feature_rebuild = []
            reasons = ["ranking_changed"]
        elif "recall_config_changed" in config_changes:
            earliest = "recall_validation"
            reason = "recall_config_changed"
            feature_rebuild = []
            reasons = ["recall_config_changed"]
        else:
            return {
                "earliest_stage": None,
                "reason": "no_change",
                "reasons": [],
                "affected_shards": [],
                "feature_rebuild": [],
                "quant_rebuild": [],
                "index_rebuild": [],
                "recall_rebuild": [],
                "config_changes": [],
            }

        if earliest == "feature":
            if config.quantization.granularity == "per_dim":
                quant_rebuild = all_shards
            else:
                quant_rebuild = list(feature_rebuild)
        elif earliest == "projection_quant":
            quant_rebuild = all_shards
        else:
            quant_rebuild = []

        if earliest == "index":
            index_rebuild = all_shards
        else:
            index_rebuild = list(quant_rebuild)

        if earliest == "recall_validation":
            recall_rebuild = all_shards
        else:
            recall_rebuild = list(index_rebuild)

        affected = sorted(
            set(feature_rebuild) | set(quant_rebuild) | set(index_rebuild) | set(recall_rebuild)
        )
        if not affected:
            return {
                "earliest_stage": None,
                "reason": "no_change",
                "reasons": [],
                "affected_shards": [],
                "feature_rebuild": [],
                "quant_rebuild": [],
                "index_rebuild": [],
                "recall_rebuild": [],
                "config_changes": [],
            }
        return {
            "earliest_stage": earliest,
            "reason": reason,
            "reasons": reasons,
            "affected_shards": affected,
            "feature_rebuild": sorted(feature_rebuild),
            "quant_rebuild": sorted(quant_rebuild),
            "index_rebuild": sorted(index_rebuild),
            "recall_rebuild": sorted(recall_rebuild),
            "config_changes": config_changes,
        }

    # -------------------------------------------------------------- feature stage
    def _build_feature_artifact(
        self, dataset: Dataset, shard_id: str, config: AlignmentConfig
    ) -> tuple[dict[str, Any], str, str]:
        shard = dataset.shard(shard_id)
        items = []
        for item in shard.items:
            visual = extract_visual(item.image_path, config.feature, item_id=item.item_id)
            text = extract_text(item.caption, config.feature, item_id=item.item_id)
            input_digest = digest_obj(
                {
                    "item_id": item.item_id,
                    "image_file": item.image_file,
                    "image_digest": visual["image_digest"],
                    "caption": item.caption,
                    "lang": item.lang,
                    "source": item.source,
                    "feature_config_digest": config.feature_digest,
                }
            )
            items.append(
                {
                    "item_id": item.item_id,
                    "image_file": item.image_file,
                    "caption": item.caption,
                    "lang": item.lang,
                    "source": item.source,
                    "image_digest": visual["image_digest"],
                    "image_size": visual["image_size"],
                    "visual": {
                        "vector": visual["vector"].tolist(),
                        "blocks": visual["blocks"],
                        "summary_digest": visual["summary_digest"],
                    },
                    "text": {
                        "normalized_caption": text["normalized_caption"],
                        "vector": text["vector"].tolist(),
                        "features": text["features"],
                        "summary_digest": text["summary_digest"],
                    },
                    "item_input_digest": input_digest,
                }
            )
        artifact = {
            "schema": FEATURE_SCHEMA,
            "shard_id": shard_id,
            "content_digest": shard.content_digest(),
            "feature_config_digest": config.feature_digest,
            "items": items,
        }
        artifact_digest = self.checkpoints.put_object(artifact)
        self._write_current_artifact("features", shard_id, artifact)
        return artifact, artifact_digest, shard.content_digest()

    def _write_current_artifact(self, kind: str, shard_id: str, artifact: dict[str, Any]) -> str:
        path = self.artifacts_dir / kind / f"{shard_id}.json"
        atomic_write_json(path, artifact)
        return str(path.relative_to(self.workspace))

    # --------------------------------------------------- projection/quant stage
    def _build_projected_artifact(
        self,
        shard_id: str,
        feature_artifact: dict[str, Any],
        feature_artifact_digest: str,
        config: AlignmentConfig,
        visual_matrix: np.ndarray,
        text_matrix: np.ndarray,
        visual_calibration: Calibration,
        text_calibration: Calibration,
        row_map: dict[str, int],
    ) -> tuple[dict[str, Any], str]:
        items = []
        for item in feature_artifact["items"]:
            item_id = item["item_id"]
            row_index = row_map[item_id]
            visual_reference = project_vector(
                np.asarray(item["visual"]["vector"], dtype=np.float64),
                visual_matrix,
                item_id=item_id,
                component="visual",
            )
            text_reference = project_vector(
                np.asarray(item["text"]["vector"], dtype=np.float64),
                text_matrix,
                item_id=item_id,
                component="text",
            )
            visual_quantized = quantize_vector(
                visual_reference,
                visual_calibration,
                row_index=row_index,
                item_id=item_id,
                component="visual",
            )
            text_quantized = quantize_vector(
                text_reference,
                text_calibration,
                row_index=row_index,
                item_id=item_id,
                component="text",
            )
            items.append(
                {
                    "item_id": item_id,
                    "image_file": item["image_file"],
                    "caption": item["caption"],
                    "lang": item["lang"],
                    "source": item["source"],
                    "item_input_digest": item["item_input_digest"],
                    "visual": visual_quantized,
                    "text": text_quantized,
                }
            )
        quant_param_digest = digest_obj(
            {
                "schema": "cmir.quant-parameters.v1",
                "visual": visual_calibration.to_dict(),
                "text": text_calibration.to_dict(),
            }
        )
        artifact = {
            "schema": PROJECTED_SCHEMA,
            "shard_id": shard_id,
            "feature_artifact_digest": feature_artifact_digest,
            "feature_config_digest": config.feature_digest,
            "projection_config_digest": config.projection_digest,
            "projection_matrix_digest": projection_matrix_digest(
                config.projection, config.feature
            ),
            "quant_config": config.quantization.to_dict(),
            "quant_param_digest": quant_param_digest,
            "calibrations": {
                "visual": visual_calibration.to_dict(),
                "text": text_calibration.to_dict(),
            },
            "items": items,
        }
        digest = self.checkpoints.put_object(artifact)
        self._write_current_artifact("projected", shard_id, artifact)
        return artifact, digest

    def _build_projection_and_quantization(
        self,
        *,
        dataset: Dataset,
        config: AlignmentConfig,
        feature_artifacts: dict[str, tuple[dict[str, Any], str]],
        quant_shards: list[str],
    ) -> dict[str, tuple[dict[str, Any], str]]:
        visual_matrix, text_matrix = build_projection_matrices(
            config.projection, config.feature
        )
        results: dict[str, tuple[dict[str, Any], str]] = {}

        def projected_vectors(feature_artifact: dict[str, Any]) -> list[tuple[str, np.ndarray, np.ndarray]]:
            vectors = []
            for item in feature_artifact["items"]:
                visual_reference = project_vector(
                    np.asarray(item["visual"]["vector"], dtype=np.float64),
                    visual_matrix,
                    item_id=item["item_id"],
                    component="visual",
                )
                text_reference = project_vector(
                    np.asarray(item["text"]["vector"], dtype=np.float64),
                    text_matrix,
                    item_id=item["item_id"],
                    component="text",
                )
                vectors.append((item["item_id"], visual_reference, text_reference))
            return vectors

        if config.quantization.granularity == "per_dim":
            # Per-dimension scales are a corpus property. Every corpus-wide quantized
            # shard must share exactly the same calibration for full/incremental parity.
            all_item_vectors: list[tuple[str, np.ndarray, np.ndarray]] = []
            for shard_id in sorted(quant_shards):
                all_item_vectors.extend(projected_vectors(feature_artifacts[shard_id][0]))
            if not all_item_vectors:
                raise CMIRError(E_INVALID_STAGE, "no vectors available for quantization")
            visual_calibration = fit_calibration(
                [(item_id, visual) for item_id, visual, _ in all_item_vectors],
                config.quantization,
                modality="visual",
            )
            text_calibration = fit_calibration(
                [(item_id, text) for item_id, _, text in all_item_vectors],
                config.quantization,
                modality="text",
            )
            row_map = {
                item_id: index
                for index, (item_id, _, _) in enumerate(all_item_vectors)
            }
            for shard_id in sorted(quant_shards):
                feature_artifact, feature_digest = feature_artifacts[shard_id]
                artifact, digest = self._build_projected_artifact(
                    shard_id,
                    feature_artifact,
                    feature_digest,
                    config,
                    visual_matrix,
                    text_matrix,
                    visual_calibration,
                    text_calibration,
                    row_map,
                )
                results[shard_id] = (artifact, digest)
        else:
            # Per-vector scales depend only on that vector. Keeping calibration
            # metadata shard-local makes single-shard rebuilds byte-identical to a
            # full rebuild of the same shard.
            for shard_id in sorted(quant_shards):
                feature_artifact, feature_digest = feature_artifacts[shard_id]
                shard_vectors = projected_vectors(feature_artifact)
                if not shard_vectors:
                    raise CMIRError(E_INVALID_STAGE, "no vectors available for quantization", shard_id=shard_id)
                visual_calibration = fit_calibration(
                    [(item_id, visual) for item_id, visual, _ in shard_vectors],
                    config.quantization,
                    modality="visual",
                )
                text_calibration = fit_calibration(
                    [(item_id, text) for item_id, _, text in shard_vectors],
                    config.quantization,
                    modality="text",
                )
                row_map = {
                    item_id: index
                    for index, (item_id, _, _) in enumerate(shard_vectors)
                }
                artifact, digest = self._build_projected_artifact(
                    shard_id,
                    feature_artifact,
                    feature_digest,
                    config,
                    visual_matrix,
                    text_matrix,
                    visual_calibration,
                    text_calibration,
                    row_map,
                )
                results[shard_id] = (artifact, digest)
        return results

    # ------------------------------------------------------------------ index stage
    def _build_index_artifact(
        self,
        shard_id: str,
        projected_artifact: dict[str, Any],
        projected_digest: str,
        config: AlignmentConfig,
    ) -> tuple[dict[str, Any], str]:
        artifact = build_shard_index(
            projected_artifact,
            index_dims=config.ranking.index_dims,
            projected_digest=projected_digest,
        )
        artifact["ranking_digest"] = config.ranking_digest
        artifact["quant_param_digest"] = projected_artifact["quant_param_digest"]
        digest = self.checkpoints.put_object(artifact)
        self._write_current_artifact("indexes", shard_id, artifact)
        return artifact, digest

    # ----------------------------------------------------------- scoring helpers
    def _score_item(
        self,
        *,
        query_record: dict[str, Any],
        query_reference: list[float] | np.ndarray,
        item: dict[str, Any],
        mode: str,
        hit_count: int,
        query_key_count: int,
        config: AlignmentConfig,
        enforce_error_limit: bool,
    ) -> dict[str, Any]:
        visual_weight, text_weight = config.ranking.route_weights(mode)
        visual_record = item["visual"]
        text_record = item["text"]
        visual_quant_cos = cosine_similarity(query_record["dequantized"], visual_record["dequantized"])
        text_quant_cos = cosine_similarity(query_record["dequantized"], text_record["dequantized"])
        visual_ref_cos = cosine_similarity(query_reference, visual_record["reference"])
        text_ref_cos = cosine_similarity(query_reference, text_record["reference"])
        visual_bound = normalized_cosine_error_bound(query_record, visual_record)
        text_bound = normalized_cosine_error_bound(query_record, text_record)
        visual_step_bound = conservative_step_cosine_error_bound(query_record, visual_record)
        text_step_bound = conservative_step_cosine_error_bound(query_record, text_record)
        blended_quant = visual_weight * visual_quant_cos + text_weight * text_quant_cos
        blended_ref = visual_weight * visual_ref_cos + text_weight * text_ref_cos
        observed_error = abs(blended_quant - blended_ref)
        weighted_bound = visual_weight * visual_bound + text_weight * text_bound
        weighted_step_bound = visual_weight * visual_step_bound + text_weight * text_step_bound
        route_error_bound = max(weighted_bound, observed_error)
        final_error_bound = config.ranking.cosine_weight * route_error_bound
        hit_denominator = max(1, 2 * query_key_count)
        hit_ratio = min(1.0, max(0.0, hit_count / hit_denominator))
        cosine_component = config.ranking.cosine_weight * blended_quant
        hit_component = config.ranking.inverted_hit_weight * hit_ratio
        score = cosine_component + hit_component
        reference_score = config.ranking.cosine_weight * blended_ref + hit_component
        values = (
            visual_quant_cos,
            text_quant_cos,
            visual_ref_cos,
            text_ref_cos,
            visual_bound,
            text_bound,
            blended_quant,
            blended_ref,
            final_error_bound,
            score,
            reference_score,
        )
        if not all(math.isfinite(float(value)) for value in values):
            raise numeric_error(
                item["item_id"],
                "similarity",
                "retrieval similarity or error bound contains NaN/Inf",
                context={
                    "visual_quantized_cosine": visual_quant_cos,
                    "text_quantized_cosine": text_quant_cos,
                    "visual_reference_cosine": visual_ref_cos,
                    "text_reference_cosine": text_ref_cos,
                    "visual_error_bound": visual_bound,
                    "text_error_bound": text_bound,
                },
            )
        if enforce_error_limit and final_error_bound > config.recall.max_error_bound:
            raise numeric_error(
                item["item_id"],
                "similarity",
                "quantization error bound exceeds configured maximum",
                error_bound=final_error_bound,
                max_error_bound=config.recall.max_error_bound,
                context={
                    "visual_error_bound": visual_bound,
                    "text_error_bound": text_bound,
                    "observed_error": observed_error,
                },
            )
        visual_contribution = config.ranking.cosine_weight * visual_weight * visual_quant_cos
        text_contribution = config.ranking.cosine_weight * text_weight * text_quant_cos
        return {
            "item_id": item["item_id"],
            "image_file": item["image_file"],
            "caption": item["caption"],
            "lang": item["lang"],
            "source": item["source"],
            "source_shard": item.get("source_shard"),
            "score": float(score),
            "similarity": {
                "score": float(score),
                "quantized_blended_cosine": float(blended_quant),
                "reference_blended_cosine": float(blended_ref),
                "cosine_component": float(cosine_component),
                "inverted_hit_ratio": float(hit_ratio),
                "inverted_hit_component": float(hit_component),
                "visual_quantized_cosine": float(visual_quant_cos),
                "text_quantized_cosine": float(text_quant_cos),
                "visual_reference_cosine": float(visual_ref_cos),
                "text_reference_cosine": float(text_ref_cos),
                "route_weights": {
                    "visual": visual_weight,
                    "text": text_weight,
                },
            },
            "error_propagation": {
                "visual_route_bound": float(visual_bound),
                "text_route_bound": float(text_bound),
                "weighted_route_bound": float(weighted_bound),
                "worst_case_step_route_bound": float(
                    config.ranking.cosine_weight * weighted_step_bound
                ),
                "similarity_error_bound": float(final_error_bound),
                "observed_error_vs_reference": float(observed_error),
                "effective_digits": effective_digits(final_error_bound),
                "reference_source": "unquantized_local_high_precision_projection",
                "bound_method": "unit-reference-checked perturbation sum; explicit step-only bound reported separately",
            },
            "log_contributions": {
                "visual": _signed_log1p(visual_contribution),
                "text": _signed_log1p(text_contribution),
                "inverted_hit": _signed_log1p(hit_component),
            },
            "hit_count": int(hit_count),
        }

    def _rank_items(
        self,
        *,
        query_record: dict[str, Any],
        query_reference: np.ndarray,
        query_keys: list[str],
        items: list[dict[str, Any]],
        indexes: list[dict[str, Any]],
        mode: str,
        config: AlignmentConfig,
        enforce_error_limit: bool,
    ) -> list[dict[str, Any]]:
        hits = inverted_hits(indexes, query_keys)
        results = []
        for item in items:
            result = self._score_item(
                query_record=query_record,
                query_reference=query_reference,
                item=item,
                mode=mode,
                hit_count=hits.get(item["item_id"], 0),
                query_key_count=len(query_keys),
                config=config,
                enforce_error_limit=enforce_error_limit,
            )
            results.append(result)
        results.sort(key=lambda row: (-row["score"], row["item_id"]))
        return results

    def _build_recall_artifact(
        self,
        shard_id: str,
        projected_artifact: dict[str, Any],
        projected_digest: str,
        index_artifact: dict[str, Any],
        index_digest: str,
        config: AlignmentConfig,
    ) -> tuple[dict[str, Any], str]:
        items = projected_artifact["items"]
        probes = items[: min(config.recall.probe_count, len(items))]
        checks = []
        for item in probes:
            for mode, component in (("text2image", "text"), ("image2text", "visual")):
                query_record = item[component]
                query_reference = np.asarray(query_record["reference"], dtype=np.float64)
                query_keys = vector_index_keys(query_record, config.ranking.index_dims)
                ranked = self._rank_items(
                    query_record=query_record,
                    query_reference=query_reference,
                    query_keys=query_keys,
                    items=items,
                    indexes=[index_artifact],
                    mode=mode,
                    config=config,
                    enforce_error_limit=True,
                )
                checks.append(
                    {
                        "mode": mode,
                        "query_item_id": item["item_id"],
                        "top": [
                            {
                                "item_id": row["item_id"],
                                "score": row["score"],
                                "effective_digits": row["error_propagation"]["effective_digits"],
                            }
                            for row in ranked[: config.recall.probe_k]
                        ],
                    }
                )
        artifact = {
            "schema": RECALL_SCHEMA,
            "shard_id": shard_id,
            "projected_digest": projected_digest,
            "index_digest": index_digest,
            "ranking_digest": config.ranking_digest,
            "recall_config_digest": config.recall_digest,
            "checks": checks,
        }
        digest = self.checkpoints.put_object(artifact)
        self._write_current_artifact("recalls", shard_id, artifact)
        return artifact, digest

    # ------------------------------------------------------------------- planning
    def _artifact_pointer(self, state: dict[str, Any], shard_id: str, stage: str) -> dict[str, Any]:
        try:
            return copy.deepcopy(state["shards"][shard_id][stage])
        except (KeyError, TypeError) as exc:
            raise CMIRError(
                E_INVALID_STAGE,
                "required upstream artifact pointer is missing",
                shard_id=shard_id,
                stage=stage,
            ) from exc

    def _build_run_digest(
        self,
        dataset: Dataset,
        config: AlignmentConfig,
        snapshots: dict[str, Any],
    ) -> str:
        matrix_digest = projection_matrix_digest(config.projection, config.feature)
        return digest_obj(
            {
                "schema": "cmir.run-digest.v1",
                "dataset_digest": dataset.dataset_digest(),
                "config": config.to_dict(),
                "projection_matrix_digest": matrix_digest,
                "content_digests": dataset.content_digests(),
                "shards": {
                    shard_id: {
                        stage: snapshots[shard_id][stage]["digest"]
                        for stage in SNAPSHOT_STAGES
                    }
                    for shard_id in sorted(snapshots)
                },
            }
        )

    def _prepare_build(
        self,
        dataset: Dataset,
        config: AlignmentConfig,
        state: dict[str, Any] | None,
        invalidation: dict[str, Any],
    ) -> dict[str, Any]:
        all_shards = [shard.shard_id for shard in dataset.shards]
        participants = invalidation["affected_shards"]
        feature_rebuild = set(invalidation["feature_rebuild"])
        quant_rebuild = set(invalidation["quant_rebuild"])
        index_rebuild = set(invalidation["index_rebuild"])
        recall_rebuild = set(invalidation["recall_rebuild"])
        feature_artifacts: dict[str, tuple[dict[str, Any], str]] = {}
        for shard_id in sorted(quant_rebuild):
            if shard_id in feature_rebuild or state is None:
                artifact, digest, _ = self._build_feature_artifact(dataset, shard_id, config)
            else:
                pointer = self._artifact_pointer(state, shard_id, "feature")
                artifact = self.checkpoints.get_object(pointer["object"])
                if artifact.get("feature_config_digest") != config.feature_digest:
                    raise CMIRError(
                        E_INVALID_STAGE,
                        "reused feature artifact has an incompatible feature configuration",
                        shard_id=shard_id,
                    )
                digest = pointer["digest"]
            feature_artifacts[shard_id] = (artifact, digest)

        projected_new: dict[str, tuple[dict[str, Any], str]] = {}
        if quant_rebuild:
            projected_new = self._build_projection_and_quantization(
                dataset=dataset,
                config=config,
                feature_artifacts=feature_artifacts,
                quant_shards=sorted(quant_rebuild),
            )

        index_new: dict[str, tuple[dict[str, Any], str]] = {}
        for shard_id in sorted(index_rebuild):
            if shard_id in projected_new:
                projected_artifact, projected_digest = projected_new[shard_id]
            else:
                pointer = self._artifact_pointer(state, shard_id, "projection_quant")
                projected_artifact = self.checkpoints.get_object(pointer["object"])
                projected_digest = pointer["digest"]
            artifact, digest = self._build_index_artifact(
                shard_id, projected_artifact, projected_digest, config
            )
            index_new[shard_id] = (artifact, digest)

        recall_new: dict[str, tuple[dict[str, Any], str]] = {}
        for shard_id in sorted(recall_rebuild):
            if shard_id in projected_new:
                projected_artifact, projected_digest = projected_new[shard_id]
            else:
                pointer = self._artifact_pointer(state, shard_id, "projection_quant")
                projected_artifact = self.checkpoints.get_object(pointer["object"])
                projected_digest = pointer["digest"]
            if shard_id in index_new:
                index_artifact, index_digest = index_new[shard_id]
            else:
                pointer = self._artifact_pointer(state, shard_id, "index")
                index_artifact = self.checkpoints.get_object(pointer["object"])
                index_digest = pointer["digest"]
            artifact, digest = self._build_recall_artifact(
                shard_id,
                projected_artifact,
                projected_digest,
                index_artifact,
                index_digest,
                config,
            )
            recall_new[shard_id] = (artifact, digest)

        old_snapshots = copy.deepcopy(state.get("shards", {})) if state else {}
        snapshots: dict[str, Any] = {}
        cycles: dict[str, int] = {}
        old_cycles = self.checkpoints.latest_cycles()
        for shard_id in all_shards:
            if shard_id not in participants:
                if shard_id not in old_snapshots:
                    raise CMIRError(
                        E_INVALID_STAGE,
                        "unaffected shard has no reusable artifact snapshot",
                        shard_id=shard_id,
                    )
                snapshots[shard_id] = copy.deepcopy(old_snapshots[shard_id])
                cycles[shard_id] = int(old_cycles.get(shard_id, old_snapshots[shard_id].get("cycle", 1)))
                continue
            cycle = int(old_cycles.get(shard_id, old_snapshots.get(shard_id, {}).get("cycle", 0))) + 1
            cycles[shard_id] = cycle
            content_digest = dataset.shard(shard_id).content_digest()
            row: dict[str, Any] = {
                "content_digest": content_digest,
                "cycle": cycle,
            }
            if shard_id in feature_rebuild:
                artifact = feature_artifacts[shard_id][0]
                digest = feature_artifacts[shard_id][1]
                row["feature"] = {
                    "digest": digest,
                    "object": digest,
                    "path": f"artifacts/features/{shard_id}.json",
                    "input_digest": content_digest,
                    "reused": False,
                }
            else:
                row["feature"] = self._artifact_pointer(state, shard_id, "feature")
                row["feature"]["reused"] = True
            if shard_id in quant_rebuild:
                artifact = projected_new[shard_id][0]
                digest = projected_new[shard_id][1]
                row["projection_quant"] = {
                    "digest": digest,
                    "object": digest,
                    "path": f"artifacts/projected/{shard_id}.json",
                    "input_digest": digest_obj(
                        {
                            "feature_digest": row["feature"]["digest"],
                            "projection_digest": config.projection_digest,
                            "quantization_digest": config.quantization_digest,
                        }
                    ),
                    "quant_param_digest": artifact["quant_param_digest"],
                    "reused": False,
                }
            else:
                row["projection_quant"] = self._artifact_pointer(state, shard_id, "projection_quant")
                row["projection_quant"]["reused"] = True
            if shard_id in index_rebuild:
                artifact = index_new[shard_id][0]
                digest = index_new[shard_id][1]
                row["index"] = {
                    "digest": digest,
                    "object": digest,
                    "path": f"artifacts/indexes/{shard_id}.json",
                    "input_digest": digest_obj(
                        {
                            "projected_digest": row["projection_quant"]["digest"],
                            "ranking_digest": config.ranking_digest,
                        }
                    ),
                    "quant_param_digest": row["projection_quant"]["quant_param_digest"],
                    "reused": False,
                }
            else:
                row["index"] = self._artifact_pointer(state, shard_id, "index")
                row["index"]["reused"] = True
            if shard_id in recall_rebuild:
                artifact = recall_new[shard_id][0]
                digest = recall_new[shard_id][1]
                row["recall"] = {
                    "digest": digest,
                    "object": digest,
                    "path": f"artifacts/recalls/{shard_id}.json",
                    "input_digest": digest_obj(
                        {
                            "index_digest": row["index"]["digest"],
                            "recall_digest": config.recall_digest,
                        }
                    ),
                    "quant_param_digest": row["index"]["quant_param_digest"],
                    "reused": False,
                }
            else:
                row["recall"] = self._artifact_pointer(state, shard_id, "recall")
                row["recall"]["reused"] = True
            snapshots[shard_id] = row

        # A reused chain must remain self-consistent even when another stage was rebuilt.
        for shard_id, row in snapshots.items():
            if row["projection_quant"]["reused"] and row["index"]["reused"]:
                index_artifact = self.checkpoints.get_object(row["index"]["object"])
                if index_artifact.get("projected_digest") not in (None, row["projection_quant"]["digest"]):
                    raise CMIRError(
                        E_INVALID_STAGE,
                        "reused index does not match reused projection artifact",
                        shard_id=shard_id,
                    )

        run_digest = self._build_run_digest(dataset, config, snapshots)
        invalidation_record = dict(invalidation)
        invalidation_record["reused_stages"] = {
            stage: sorted(
                set(all_shards)
                - set(
                    invalidation[
                        {
                            "feature": "feature_rebuild",
                            "projection_quant": "quant_rebuild",
                            "index": "index_rebuild",
                            "recall_validation": "recall_rebuild",
                        }[stage]
                    ]
                )
            )
            for stage in STAGES
        }
        history = copy.deepcopy(state.get("invalidations", [])) if state else []
        history.append(invalidation_record)
        matrix_digest = projection_matrix_digest(config.projection, config.feature)
        units: list[dict[str, Any]] = []
        for shard_id in sorted(participants):
            row = snapshots[shard_id]
            cycle = cycles[shard_id]
            feature_stage = row["feature"]
            units.append(
                {
                    "shard_id": shard_id,
                    "stage": "feature",
                    "shard_seq": 1,
                    "cycle": cycle,
                    "input_digest": row["content_digest"],
                    "config_version": FEATURE_CONFIG_VERSION,
                    "config_digest": config.feature_digest,
                    "quant_param_digest": config.quantization_digest,
                    "artifact_digest": feature_stage["digest"],
                    "artifact_object": feature_stage["object"],
                    "artifact_path": feature_stage["path"],
                    "reused": bool(feature_stage.get("reused", False)),
                }
            )
            quant_stage = row["projection_quant"]
            units.append(
                {
                    "shard_id": shard_id,
                    "stage": "projection_quant",
                    "shard_seq": 2,
                    "cycle": cycle,
                    "input_digest": quant_stage["input_digest"],
                    "config_version": f"{PROJECTION_CONFIG_VERSION}+{QUANT_CONFIG_VERSION}",
                    "config_digest": config.projection_digest,
                    "quant_param_digest": quant_stage["quant_param_digest"],
                    "artifact_digest": quant_stage["digest"],
                    "artifact_object": quant_stage["object"],
                    "artifact_path": quant_stage["path"],
                    "reused": bool(quant_stage.get("reused", False)),
                    "projection_matrix_digest": matrix_digest,
                }
            )
            index_stage = row["index"]
            units.append(
                {
                    "shard_id": shard_id,
                    "stage": "index",
                    "shard_seq": 3,
                    "cycle": cycle,
                    "input_digest": index_stage["input_digest"],
                    "config_version": RANKING_CONFIG_VERSION,
                    "config_digest": config.ranking_digest,
                    "quant_param_digest": index_stage["quant_param_digest"],
                    "artifact_digest": index_stage["digest"],
                    "artifact_object": index_stage["object"],
                    "artifact_path": index_stage["path"],
                    "reused": bool(index_stage.get("reused", False)),
                }
            )
            recall_stage = row["recall"]
            units.append(
                {
                    "shard_id": shard_id,
                    "stage": "recall_validation",
                    "shard_seq": 4,
                    "cycle": cycle,
                    "input_digest": recall_stage["input_digest"],
                    "config_version": RECALL_CONFIG_VERSION,
                    "config_digest": config.recall_digest,
                    "quant_param_digest": recall_stage["quant_param_digest"],
                    "artifact_digest": recall_stage["digest"],
                    "artifact_object": recall_stage["object"],
                    "artifact_path": recall_stage["path"],
                    "reused": bool(recall_stage.get("reused", False)),
                }
            )
        plan_id = digest_obj(
            {
                "units": units,
                "dataset_digest": dataset.dataset_digest(),
                "config_digest": config.digest,
                "run_digest": run_digest,
            }
        )
        state_payload = {
            "schema": STATE_SCHEMA,
            "dataset_digest": dataset.dataset_digest(),
            "config": config.to_dict(),
            "config_digest": config.digest,
            "stage_digests": config.stage_digests(),
            "projection_matrix_digest": matrix_digest,
            "content_digests": dataset.content_digests(),
            "cycles": cycles,
            "shards": snapshots,
            "run_digest": run_digest,
            "invalidations": history,
            "last_plan_id": plan_id,
        }
        context = {
            "schema": "cmir.run-context.v1",
            "plan_id": plan_id,
            "dataset_digest": dataset.dataset_digest(),
            "config": config.to_dict(),
            "config_digest": config.digest,
            "stage_digests": config.stage_digests(),
            "run_digest": run_digest,
            "invalidations": history,
            "units": units,
            "state": state_payload,
        }
        return context

    # -------------------------------------------------------------------- build
    def _finalize_state(self, context: dict[str, Any]) -> dict[str, Any]:
        recovery = self.checkpoints.recover()
        state = copy.deepcopy(context["state"])
        state["checkpoint_last_seq"] = recovery.last_seq
        state["last_plan_id"] = context["plan_id"]
        self._write_state(state)
        context = copy.deepcopy(context)
        context["status"] = "complete"
        context["checkpoint_last_seq"] = recovery.last_seq
        self._write_run_context(context)
        return {
            "ok": True,
            "status": "complete",
            "plan_id": context["plan_id"],
            "run_digest": state["run_digest"],
            "state_digest": state["run_digest"],
            "invalidations": state["invalidations"],
            "checkpoint_last_seq": recovery.last_seq,
            "shards": self._state_summary(state)["shards"],
        }

    def _execute_plan(
        self,
        context: dict[str, Any],
        *,
        stop_after_units: int | None,
    ) -> dict[str, Any]:
        units = context["units"]
        active = self.checkpoints.active_plan()
        if active is None:
            recovery = self.checkpoints.recover()
            if recovery.plan and recovery.plan_complete and recovery.plan.get("plan_id") == context["plan_id"]:
                return self._finalize_state(context)
            raise CMIRError(
                E_INVALID_STAGE,
                "run context has no matching active checkpoint plan",
                plan_id=context.get("plan_id"),
            )
        if active["plan"].get("plan_id") != context.get("plan_id"):
            raise CMIRError(
                E_INVALID_STAGE,
                "run context does not match active checkpoint plan",
                expected_plan_id=active["plan"].get("plan_id"),
                context_plan_id=context.get("plan_id"),
            )
        start = int(active["next_unit_index"])
        if self.stop_requested():
            return {
                "ok": True,
                "status": "stopped",
                "plan_id": context["plan_id"],
                "next_unit_index": start,
                "checkpoint_last_seq": active["last_seq"],
                "reason": "stop_requested_before_next_unit",
            }
        completed_this_call = 0
        for unit_index in range(start, len(units)):
            record = self.checkpoints.publish(unit_index)
            completed_this_call += 1
            if (stop_after_units is not None and completed_this_call >= stop_after_units) or self.stop_requested():
                return {
                    "ok": True,
                    "status": "stopped",
                    "plan_id": context["plan_id"],
                    "published_units": completed_this_call,
                    "last_published": {
                        "seq": record["seq"],
                        "shard_id": record["shard_id"],
                        "stage": record["stage"],
                        "shard_seq": record["shard_seq"],
                    },
                    "next_unit_index": unit_index + 1,
                    "checkpoint_last_seq": record["seq"],
                    "reason": "graceful_stop_after_atomic_unit",
                }
        return self._finalize_state(context)

    def build(
        self,
        *,
        stop_after_units: int | None = None,
        changed_shard: str | None = None,
    ) -> dict[str, Any]:
        try:
            with file_lock(self.build_lock_path):
                return self._build_locked(
                    stop_after_units=stop_after_units,
                    changed_shard=changed_shard,
                )
        except CMIRError as exc:
            self._record_error(exc)
            raise

    def _build_locked(
        self,
        *,
        stop_after_units: int | None,
        changed_shard: str | None,
    ) -> dict[str, Any]:
        recovery = self.checkpoints.recover()
        active = self.checkpoints.active_plan()
        context = self._read_run_context()
        if active is not None:
            if context is None or context.get("plan_id") != active["plan"].get("plan_id"):
                raise CMIRError(
                    E_INVALID_STAGE,
                    "active checkpoint plan has no matching run context",
                    active_plan_id=active["plan"].get("plan_id"),
                )
            return self._execute_plan(context, stop_after_units=stop_after_units)
        if (
            recovery.plan is not None
            and recovery.plan_complete
            and context is not None
            and context.get("plan_id") == recovery.plan.get("plan_id")
            and context.get("status") == "pending"
        ):
            return self._finalize_state(context)
        if self.stop_requested():
            return {
                "ok": True,
                "status": "stopped",
                "reason": "stop_requested_before_build",
                "checkpoint_last_seq": recovery.last_seq,
            }
        dataset = self._load_dataset()
        dataset.assert_valid()
        config = self.load_config()
        state = self._state()
        invalidation = self._determine_invalidation(
            dataset, config, state, changed_shard=changed_shard
        )
        if invalidation["earliest_stage"] is None:
            if state is None:
                raise CMIRError(E_INVALID_STAGE, "no build state and no invalidated stage")
            return {
                "ok": True,
                "status": "no_op",
                "earliest_stage": None,
                "run_digest": state.get("run_digest"),
                "checkpoint_last_seq": recovery.last_seq,
                "invalidations": state.get("invalidations", []),
            }
        prepared = self._prepare_build(dataset, config, state, invalidation)
        self._write_run_context({**prepared, "status": "pending"})
        self.checkpoints.create_plan(
            prepared["units"],
            plan_id=prepared["plan_id"],
            context=prepared,
        )
        return self._execute_plan(prepared, stop_after_units=stop_after_units)

    def update_shard(self, shard_id: str, *, stop_after_units: int | None = None) -> dict[str, Any]:
        return self.build(stop_after_units=stop_after_units, changed_shard=shard_id)

    # ------------------------------------------------------------------- queries
    def _load_query_artifacts(
        self,
    ) -> tuple[dict[str, Any], AlignmentConfig, list[dict[str, Any]], list[dict[str, Any]]]:
        state = self._state()
        if state is None:
            raise CMIRError(E_INVALID_STAGE, "no completed build state is available", stage="query")
        active = self.checkpoints.active_plan()
        if active is not None:
            raise CMIRError(
                E_INVALID_STAGE,
                "an incomplete checkpoint plan is active; resume or finish it first",
                active_plan_id=active["plan"].get("plan_id"),
            )
        config = self.load_config()
        if state.get("config_digest") != config.digest:
            raise CMIRError(
                E_INVALID_STAGE,
                "configuration changed after the last completed build",
                state_config_digest=state.get("config_digest"),
                current_config_digest=config.digest,
            )
        items: list[dict[str, Any]] = []
        indexes: list[dict[str, Any]] = []
        for shard_id in sorted(state["shards"]):
            row = state["shards"][shard_id]
            projected = self.checkpoints.get_object(row["projection_quant"]["object"])
            index = self.checkpoints.get_object(row["index"]["object"])
            for item in projected["items"]:
                item = copy.deepcopy(item)
                item["source_shard"] = shard_id
                items.append(item)
            indexes.append(index)
        items.sort(key=lambda item: item["item_id"])
        return state, config, items, indexes

    def _query_text_record(
        self,
        query: str,
        *,
        config: AlignmentConfig,
        items: list[dict[str, Any]],
    ) -> tuple[dict[str, Any], np.ndarray]:
        text_features = extract_text(query, config.feature, item_id="__query__")
        _, text_matrix = build_projection_matrices(config.projection, config.feature)
        reference = project_vector(
            text_features["vector"],
            text_matrix,
            item_id="__query__",
            component="text",
        )
        projected_artifact = items[0]
        # Calibration objects are stored verbatim on each projected artifact.
        first_projected = self._first_projected_artifact_for_calibration()
        if config.quantization.granularity == "per_dim":
            calibration = Calibration.from_dict(first_projected["calibrations"]["text"])
            row_index = 0
        else:
            calibration = fit_calibration(
                [("__query__", reference)],
                config.quantization,
                modality="text",
            )
            row_index = 0
        record = quantize_vector(
            reference,
            calibration,
            row_index=row_index,
            item_id="__query__",
            component="text_query",
        )
        return record, reference

    def _query_visual_record(
        self,
        image_path: str | Path,
        *,
        item_id: str,
        config: AlignmentConfig,
    ) -> tuple[dict[str, Any], np.ndarray]:
        visual_features = extract_visual(image_path, config.feature, item_id=item_id)
        visual_matrix, _ = build_projection_matrices(config.projection, config.feature)
        reference = project_vector(
            visual_features["vector"],
            visual_matrix,
            item_id=item_id,
            component="visual",
        )
        first_projected = self._first_projected_artifact_for_calibration()
        if config.quantization.granularity == "per_dim":
            calibration = Calibration.from_dict(first_projected["calibrations"]["visual"])
            row_index = 0
        else:
            calibration = fit_calibration(
                [(item_id, reference)],
                config.quantization,
                modality="visual",
            )
            row_index = 0
        record = quantize_vector(
            reference,
            calibration,
            row_index=row_index,
            item_id=item_id,
            component="visual_query",
        )
        return record, reference

    def _first_projected_artifact_for_calibration(self) -> dict[str, Any]:
        state = self._state()
        if not state:
            raise CMIRError(E_INVALID_STAGE, "no state known while loading query calibration")
        first_shard = sorted(state["shards"])[0]
        pointer = state["shards"][first_shard]["projection_quant"]
        return self.checkpoints.get_object(pointer["object"])

    def _corpus_error_profile(
        self, ranked: list[dict[str, Any]]
    ) -> dict[str, Any]:
        by_shard: dict[str, list[float]] = {}
        for row in ranked:
            shard_id = row["source_shard"]
            by_shard.setdefault(shard_id, []).append(
                float(row["error_propagation"]["similarity_error_bound"])
            )
        cumulative_squared = 0.0
        profile = []
        for shard_id in sorted(by_shard):
            bounds = by_shard[shard_id]
            shard_bound = math.sqrt(sum(value * value for value in bounds))
            cumulative_squared += shard_bound * shard_bound
            profile.append(
                {
                    "shard_id": shard_id,
                    "result_count": len(bounds),
                    "shard_rss_bound": shard_bound,
                    "max_result_bound": max(bounds),
                    "cumulative_shard_count": len(profile) + 1,
                    "cumulative_rss_bound": math.sqrt(cumulative_squared),
                }
            )
        return {
            "method": "root_sum_square_of_explicit_result_bounds_by_shard",
            "total_result_count": len(ranked),
            "corpus_rss_bound": math.sqrt(cumulative_squared),
            "max_result_bound": max(
                (float(row["error_propagation"]["similarity_error_bound"]) for row in ranked),
                default=0.0,
            ),
            "by_shard_prefix": profile,
        }

    def search(
        self,
        *,
        mode: str,
        query: str | None = None,
        query_item_id: str | None = None,
        query_image_file: str | None = None,
        query_vector: list[float] | tuple[float, ...] | None = None,
        k: int | None = None,
    ) -> dict[str, Any]:
        if mode not in ("text2image", "image2text"):
            raise invalid_query(
                "mode must be text2image or image2text",
                mode=mode,
            )
        state, config, items, indexes = self._load_query_artifacts()
        if not items:
            raise invalid_query("index is empty", index_size=0)
        if k is None:
            k = config.ranking.default_k
        if isinstance(k, bool) or not isinstance(k, int) or k <= 0:
            raise invalid_query("k must be a positive integer", k=k)
        if k > len(items):
            raise invalid_query(
                "k exceeds index size",
                k=k,
                index_size=len(items),
            )
        dataset = self._load_dataset()
        query_description: dict[str, Any]
        if query_vector is not None:
            try:
                reference = np.asarray(query_vector, dtype=np.float64)
            except (TypeError, ValueError) as exc:
                raise invalid_query(
                    "query_vector must be numeric",
                    dimensions={"actual": "non-numeric"},
                ) from exc
            if reference.ndim != 1 or reference.size != config.projection.alignment_dim:
                raise invalid_query(
                    "query_vector dimension does not match the 64-D alignment space",
                    dimensions={
                        "expected": config.projection.alignment_dim,
                        "actual": list(reference.shape),
                    },
                    field="query_vector",
                )
            if not np.all(np.isfinite(reference)):
                raise invalid_query(
                    "query_vector contains NaN or Inf",
                    field="query_vector",
                    dimensions=list(reference.shape),
                )
            norm = float(np.linalg.norm(reference))
            if norm <= 0.0:
                raise invalid_query(
                    "query_vector has zero norm",
                    field="query_vector",
                )
            reference = reference / norm
            first_projected = self._first_projected_artifact_for_calibration()
            component = "text" if mode == "text2image" else "visual"
            if config.quantization.granularity == "per_dim":
                calibration = Calibration.from_dict(
                    first_projected["calibrations"][component]
                )
            else:
                calibration = fit_calibration(
                    [("__query__", reference)],
                    config.quantization,
                    modality=component,
                )
            query_record = quantize_vector(
                reference,
                calibration,
                row_index=0,
                item_id="__query_vector__",
                component=f"{component}_query",
            )
            query_description = {
                "mode": mode,
                "query_vector_dim": int(reference.size),
            }
        elif mode == "text2image":
            if query is None or not normalize_caption(query):
                raise invalid_query("query text is empty", mode=mode, field="query")
            query_record, query_reference = self._query_text_record(
                query, config=config, items=items
            )
            query_description = {"mode": mode, "query": query}
        else:
            if query_item_id is not None:
                matches = [item for item in dataset.items if item.item_id == query_item_id]
                if not matches:
                    raise invalid_query(
                        "query_item_id is not present in the dataset",
                        item_id=query_item_id,
                    )
                query_path = matches[0].image_path
                query_id = query_item_id
            elif query_image_file is not None:
                query_path = Path(query_image_file).resolve()
                query_id = "__query_image__"
            else:
                raise invalid_query(
                    "image2text requires query_item_id or query_image_file",
                    mode=mode,
                )
            query_record, query_reference = self._query_visual_record(
                query_path, item_id=query_id, config=config
            )
            query_description = {
                "mode": mode,
                "query_item_id": query_item_id,
                "query_image_file": query_image_file,
            }
        query_keys = vector_index_keys(query_record, config.ranking.index_dims)
        ranked = self._rank_items(
            query_record=query_record,
            query_reference=query_reference,
            query_keys=query_keys,
            items=items,
            indexes=indexes,
            mode=mode,
            config=config,
            enforce_error_limit=True,
        )
        top = ranked[:k]
        for rank, row in enumerate(top, start=1):
            row["rank"] = rank
        query_calibration_summary = {
            "bits": query_record["bits"],
            "granularity": query_record["granularity"],
            "step_error_l2": query_record["step_error_l2"],
            "checked_error_l2": query_record["checked_error_l2"],
            "actual_error_l2": query_record["actual_error_l2"],
            "clipped_dims": query_record["clipped_dims"],
            "calibration_digest": query_record["calibration_digest"],
        }
        return {
            "ok": True,
            "mode": mode,
            "query": query_description,
            "k": k,
            "index_size": len(items),
            "config_digest": config.digest,
            "run_digest": state["run_digest"],
            "ranking": config.ranking.to_dict(),
            "top_k": top,
            "diagnostics": {
                "query_quantization": query_calibration_summary,
                "query_index_keys": query_keys,
                "corpus_error": self._corpus_error_profile(ranked),
                "invalidations": state.get("invalidations", []),
                "tie_breaker": "item_id_ascending",
                "score_formula": "cosine_weight*(visual_weight*visual_cosine+text_weight*text_cosine)+inverted_hit_weight*hit_ratio",
                "effective_digits_definition": "floor(-log10(similarity_error_bound))",
            },
        }

    # ------------------------------------------------------ full consistency check
    def _probe_specs(self, dataset: Dataset, config: AlignmentConfig) -> list[dict[str, Any]]:
        items = sorted(dataset.items, key=lambda item: item.item_id)
        k = min(config.ranking.default_k, len(items))
        specs: list[dict[str, Any]] = []
        for item in items[:2]:
            specs.append(
                {
                    "label": f"text2image:{item.item_id}",
                    "mode": "text2image",
                    "query": item.caption,
                    "k": k,
                }
            )
        for item in items[:2]:
            specs.append(
                {
                    "label": f"image2text:{item.item_id}",
                    "mode": "image2text",
                    "query_item_id": item.item_id,
                    "k": k,
                }
            )
        return specs

    def comparison_signature(self, dataset: Dataset | None = None) -> dict[str, Any]:
        if dataset is None:
            dataset = self._load_dataset()
        config = self.load_config()
        state = self._state()
        if state is None:
            raise CMIRError(E_INVALID_STAGE, "cannot sign an unpublished state")
        probes: dict[str, Any] = {}
        for spec in self._probe_specs(dataset, config):
            label = spec.pop("label")
            result = self.search(**spec)
            probes[label] = {
                "top_k": result["top_k"],
                "corpus_error": result["diagnostics"]["corpus_error"],
            }
        return {
            "run_digest": state["run_digest"],
            "feature_artifacts": {
                shard_id: row["feature"]["digest"]
                for shard_id, row in sorted(state["shards"].items())
            },
            "projection_quant_artifacts": {
                shard_id: row["projection_quant"]["digest"]
                for shard_id, row in sorted(state["shards"].items())
            },
            "index_artifacts": {
                shard_id: row["index"]["digest"]
                for shard_id, row in sorted(state["shards"].items())
            },
            "recall_artifacts": {
                shard_id: row["recall"]["digest"]
                for shard_id, row in sorted(state["shards"].items())
            },
            "probes": probes,
        }

    def verify_full_consistency(
        self,
        *,
        probe_dataset: Dataset | None = None,
    ) -> dict[str, Any]:
        dataset = probe_dataset or self._load_dataset()
        config = self.load_config()
        incremental_signature = self.comparison_signature(dataset)
        temp_dir = Path(tempfile.mkdtemp(prefix="cmir-full-verify-"))
        try:
            temp_engine = RetrievalEngine(
                temp_dir,
                dataset_dir=dataset.root,
                config=AlignmentConfig.from_dict(config.to_dict()),
            )
            temp_engine.build()
            full_signature = temp_engine.comparison_signature(dataset)
            difference = _first_difference(full_signature, incremental_signature)
            if difference is not None:
                raise CMIRError(
                    E_INCREMENTAL_MISMATCH,
                    "incremental artifacts or recall results differ from a full rebuild",
                    difference=difference,
                    full_run_digest=full_signature["run_digest"],
                    incremental_run_digest=incremental_signature["run_digest"],
                )
            return {
                "ok": True,
                "consistent": True,
                "full_run_digest": full_signature["run_digest"],
                "incremental_run_digest": incremental_signature["run_digest"],
                "probe_count": len(full_signature["probes"]),
                "feature_digests": full_signature["feature_artifacts"],
                "projection_quant_digests": full_signature["projection_quant_artifacts"],
                "index_digests": full_signature["index_artifacts"],
                "recall_digests": full_signature["recall_artifacts"],
                "probe_result_digests": {
                    label: digest_obj(value)
                    for label, value in sorted(full_signature["probes"].items())
                },
            }
        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)

    # -------------------------------------------------------------------- export
    def export_report(
        self,
        output_dir: str | Path,
        *,
        queries: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        output = Path(output_dir).resolve()
        output.mkdir(parents=True, exist_ok=True)
        dataset = self._load_dataset()
        config = self.load_config()
        if queries is None:
            queries = []
            for item in sorted(dataset.items, key=lambda row: row.item_id)[:2]:
                queries.append(
                    {"mode": "text2image", "query": item.caption, "k": min(5, dataset.item_count)}
                )
                queries.append(
                    {
                        "mode": "image2text",
                        "query_item_id": item.item_id,
                        "k": min(5, dataset.item_count),
                    }
                )
        results = []
        for query in queries:
            result = self.search(**query)
            results.append({"query": query, "result": result})
        state = self._state() or {}
        report = {
            "schema": "cmir.export.v1",
            "run_digest": state.get("run_digest"),
            "config_digest": config.digest,
            "dataset_digest": dataset.dataset_digest(),
            "query_results_digest": digest_obj(results),
            "queries": results,
            "error_formula": {
                "route": "max(weighted explicit scale/reference checked bound, observed high-precision error)",
                "corpus": "sqrt(sum(result_bound^2)) grouped by shard prefix",
            },
            "checkpoints": self.checkpoints.audit(),
            "invalidations": state.get("invalidations", []),
        }
        atomic_write_json(output / "search-results.json", results)
        atomic_write_json(output / "diagnostics.json", report)
        atomic_write_json(output / "state.json", self.status())
        atomic_write_json(output / "report.json", report)
        return {
            "ok": True,
            "output_dir": str(output),
            "files": [
                "search-results.json",
                "diagnostics.json",
                "state.json",
                "report.json",
            ],
            "query_results_digest": report["query_results_digest"],
            "run_digest": report["run_digest"],
        }

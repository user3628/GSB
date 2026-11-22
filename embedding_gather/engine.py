from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence

import numpy as np

from .batches import IndexBatch, load_index_batch
from .checkpoint import CheckpointInspection, CheckpointStore, STAGE_ORDER
from .codec import read_manifest_file, decode_manifest
from .digest import (
    atomic_write_json,
    canonical_json_bytes,
    deterministic_npz_write,
    json_scalar,
    load_npz,
    sha256_file,
    sha256_json,
)
from .errors import (
    BusinessError,
    E_INVALID_STAGE,
    E_MANIFEST_INVALID,
    E_NUMERIC,
    E_STALE_CHECKPOINT,
    fail,
)
from .kernel import (
    BatchValidation,
    GatherComputation,
    GatherConfig,
    QuantizedResult,
    gather,
    quantize,
    validate_batch,
)
from .model import BatchLink, IndexProject, V1UpgradeConfig
from .weights import WeightTable, load_manifest_facts, load_weight_table


@dataclass(frozen=True)
class BatchRunResult:
    batch_id: str
    status: str
    artifact_path: str
    artifact_digest: str
    output_shape: tuple[int, ...]
    hit_count: int
    pad_hit_count: int
    stage_sequences: Mapping[str, int]
    reused_stages: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "batch_id": self.batch_id,
            "status": self.status,
            "artifact_path": self.artifact_path,
            "artifact_digest": self.artifact_digest,
            "output_shape": list(self.output_shape),
            "hit_count": self.hit_count,
            "pad_hit_count": self.pad_hit_count,
            "stage_sequences": dict(self.stage_sequences),
            "reused_stages": list(self.reused_stages),
        }


@dataclass(frozen=True)
class RunReport:
    status: str
    manifest_fingerprint: str
    config_version: str
    selected_batches: tuple[str, ...]
    completed_batches: tuple[str, ...]
    results: tuple[BatchRunResult, ...]
    bundle_path: str | None
    bundle_digest: str | None

    def to_dict(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "manifest_fingerprint": self.manifest_fingerprint,
            "config_version": self.config_version,
            "selected_batches": list(self.selected_batches),
            "completed_batches": list(self.completed_batches),
            "results": [result.to_dict() for result in self.results],
            "bundle_path": self.bundle_path,
            "bundle_digest": self.bundle_digest,
        }


class EmbeddingEngine:
    def __init__(
        self,
        project: IndexProject,
        table: WeightTable,
        workspace: str | Path,
        data_root: str | Path | None = None,
        *,
        gather_config: GatherConfig | None = None,
        verify_tolerance_scale: float = 0.0,
        strict_weight_metadata: bool = False,
        fault_hook: Callable[[str, str], None] | None = None,
    ) -> None:
        self.project = project
        self.table = table
        self.workspace = Path(workspace)
        self.data_root = None if data_root is None else Path(data_root)
        self.gather_config = gather_config or GatherConfig()
        self.verify_tolerance_scale = float(verify_tolerance_scale)
        self.strict_weight_metadata = strict_weight_metadata
        self.fault_hook = fault_hook
        self.manifest_fingerprint = sha256_json(self._manifest_semantics())
        self.config_version = sha256_json(
            {
                "gather": self.gather_config.fingerprint_components(),
                "verify_tolerance_scale": float(self.verify_tolerance_scale).hex(),
                "strict_weight_metadata": self.strict_weight_metadata,
            }
        )
        self._last_inspections: list[dict[str, Any]] = []

    def _manifest_semantics(self) -> dict[str, Any]:
        return {
            "structure_version": self.project.structure_version,
            "table_id": self.project.table_id,
            "embedding_dim": self.project.embedding_dim,
            "shards": [
                {
                    "shard_id": shard.shard_id,
                    "row_start": shard.row_start,
                    "row_end": shard.row_end,
                    "row_offset": shard.row_offset,
                    "embedding_dim": shard.embedding_dim,
                    "pad_ranges": [list(item) for item in shard.pad_ranges],
                    "pad_marker": shard.pad_marker,
                }
                for shard in sorted(self.project.shards, key=lambda item: item.shard_id)
            ],
            "batches": [
                {
                    "batch_id": batch.batch_id,
                    "table_id": batch.table_id,
                    "file": batch.file,
                    "batch_number": batch.batch_number,
                }
                for batch in sorted(self.project.index_batches, key=lambda item: item.batch_id)
            ],
        }

    def _store(self, link: BatchLink) -> CheckpointStore:
        return CheckpointStore(self.workspace, link.batch_id, partition_id=link.batch_id)

    def _artifact_root(self, store: CheckpointStore) -> Path:
        return self.workspace / "artifacts" / store.safe_name

    def _artifact_path(self, store: CheckpointStore, stage: str) -> Path:
        suffix = "json" if stage == "validate" else "npz"
        name = "output" if stage == "verify" else stage
        return self._artifact_root(store) / f"{name}.{suffix}"

    def _effective_output_scale(self) -> float:
        if self.gather_config.output_scale is not None:
            return float(self.gather_config.output_scale)
        scales = [shard.scale for shard in self.table.shards.values() if np.isfinite(shard.scale) and shard.scale > 0]
        if not scales:
            return float(next(iter(self.table.shards.values())).scale)
        return float(min(scales))

    def _expected_components(
        self,
        link: BatchLink,
        batch: IndexBatch,
    ) -> tuple[dict[str, dict[str, Any]], Mapping[str, Any] | None]:
        validate_components = {
            "manifest_structure_version": self.project.structure_version,
            "manifest_semantics": self.manifest_fingerprint,
            "ids_digest": batch.ids_digest,
            "ids_dtype": batch.ids.dtype.str,
            "ids_shape": list(batch.ids.shape),
            "batch_number": batch.batch_number,
            "batch_table_id": batch.table_id,
            "negative_index_policy": self.gather_config.negative_index_policy,
        }
        validation: BatchValidation | None = None
        dependency_error: str | None = None
        try:
            validation = validate_batch(self.table, batch, self.gather_config)
        except BusinessError as exc:
            dependency_error = exc.code
        if validation is None:
            dependency: dict[str, str] = {"unresolved": dependency_error or "invalid"}
            gather_axis = batch.gather_axis
        else:
            dependency = {
                shard_id: self.table.shards[shard_id].content_digest for shard_id in validation.shard_ids
            }
            gather_axis = validation.gather_axis
        validate_fp = sha256_json(validate_components)
        gather_components = {
            "upstream_validate": validate_fp,
            "weight_content": dependency,
            "weights": batch.weights_digest,
            "gather_axis": gather_axis,
            "output_scale": self._effective_output_scale().hex(),
        }
        gather_fp = sha256_json(gather_components)
        quantize_components = {
            "upstream_gather": gather_fp,
            "quantization_bits": self.project.quantization_bits,
            "rounding_mode": "half_even",
        }
        quantize_fp = sha256_json(quantize_components)
        verify_components = {
            "upstream_quantize": quantize_fp,
            "verify_tolerance_scale": float(self.verify_tolerance_scale).hex(),
            "verification_scheme": "fixed-point-bound-v1",
        }
        return (
            {
                "validate": validate_components,
                "gather": gather_components,
                "quantize": quantize_components,
                "verify": verify_components,
            },
            validation,
        )

    def _fingerprints(self, components: Mapping[str, Mapping[str, Any]]) -> dict[str, str]:
        return {stage: sha256_json(value) for stage, value in components.items()}

    def inspect(self, batch_ids: Sequence[str] | None = None) -> list[dict[str, Any]]:
        selected = set(batch_ids) if batch_ids is not None else None
        reports: list[dict[str, Any]] = []
        for link in self.project.index_batches:
            if selected is not None and link.batch_id not in selected:
                continue
            try:
                batch = load_index_batch(self.project, link, self._data_root())
                components, _ = self._expected_components(link, batch)
                store = self._store(link)
                inspection = store.inspect(components)
                record_by_stage = {stage: status.record for stage in STAGE_ORDER for status in inspection.stages if status.stage == stage}
                stage_details = []
                for status in inspection.stages:
                    record = record_by_stage.get(status.stage)
                    stage_details.append(
                        {
                            **status.to_dict(),
                            "expected_input_fingerprint": sha256_json(components[status.stage]),
                            "recorded_input_fingerprint": None if record is None else record.get("input_fingerprint"),
                            "expected_components": components[status.stage],
                            "recorded_components": None if record is None else record.get("components"),
                        }
                    )
                reports.append(
                    {
                        **inspection.to_dict(),
                        "expected_fingerprints": self._fingerprints(components),
                        "stage_details": stage_details,
                    }
                )
            except BusinessError as exc:
                reports.append(
                    {
                        "batch_id": link.batch_id,
                        "all_valid": False,
                        "error": exc.to_dict(),
                    }
                )
        self._last_inspections = reports
        return reports

    def _data_root(self) -> Path:
        if self.data_root is None:
            raise fail(E_MANIFEST_INVALID, "engine data_root is not configured")
        return self.data_root

    def process_batch(self, link: BatchLink) -> BatchRunResult:
        try:
            batch = load_index_batch(self.project, link, self._data_root())
        except BusinessError as exc:
            exc.context.setdefault("batch_id", link.batch_id)
            exc.context.setdefault("batch_file", link.file)
            exc.context.setdefault("manifest_fingerprint", self.manifest_fingerprint)
            exc.context.setdefault("manifest_structure_version", self.project.structure_version)
            exc.context.setdefault("config_version", self.config_version)
            exc.context.setdefault("ids_digest", None)
            raise
        try:
            return self._process_batch_loaded(link, batch)
        except BusinessError as exc:
            exc.context.setdefault("batch_id", link.batch_id)
            exc.context.setdefault("batch_file", link.file)
            exc.context.setdefault("manifest_fingerprint", self.manifest_fingerprint)
            exc.context.setdefault("manifest_structure_version", self.project.structure_version)
            exc.context.setdefault("config_version", self.config_version)
            exc.context.setdefault("ids_digest", batch.ids_digest)
            exc.context.setdefault("weights_digest", batch.weights_digest)
            raise

    def _process_batch_loaded(self, link: BatchLink, batch: IndexBatch) -> BatchRunResult:
        components, validation = self._expected_components(link, batch)
        store = self._store(link)
        inspection = store.inspect(components)
        if inspection.all_valid:
            return self._reuse_batch(link, batch, store, inspection)

        if validation is None:
            validation = validate_batch(self.table, batch, self.gather_config)
        earliest = inspection.earliest_invalid_stage or "validate"
        invalidation = store.invalidate_from(
            target_stage=earliest,
            reasons=inspection.reasons or ("missing",),
            components=components[earliest],
            current_input_fingerprint=sha256_json(components[earliest]),
            manifest_fingerprint=self.manifest_fingerprint,
            manifest_version=self.project.structure_version,
            config_version=self.config_version,
        )
        if invalidation:
            inspection = store.inspect(components)

        reused = tuple(inspection.reused_stages)
        gather_computation: GatherComputation | None = None
        quantized: QuantizedResult | None = None
        final_digest: str | None = None

        for stage in STAGE_ORDER:
            status = next(item for item in inspection.stages if item.stage == stage)
            if status.valid:
                if stage == "validate":
                    self._verify_artifact_digest(self._artifact_path(store, "validate"), status.record)
                elif stage == "gather":
                    gather_computation = self._load_gather_computation(store, status.record, batch, validation)
                elif stage == "quantize":
                    self._verify_artifact_digest(self._artifact_path(store, "quantize"), status.record)
                elif stage == "verify":
                    final_digest = str(status.record["product_digest"])
                continue

            if stage == "validate":
                artifact = self._build_validate_artifact(link, batch, components["validate"], validation)
                product_digest = atomic_write_json(self._artifact_path(store, "validate"), artifact)
            elif stage == "gather":
                if gather_computation is None:
                    gather_computation = gather(self.table, batch, self.gather_config, validation)
                arrays = self._gather_arrays(gather_computation)
                product_digest = deterministic_npz_write(self._artifact_path(store, "gather"), arrays)
            elif stage == "quantize":
                if gather_computation is None:
                    record = store.latest_effective().get("gather")
                    if record is None:
                        raise fail(E_INVALID_STAGE, "gather checkpoint is missing", batch_id=link.batch_id, stage=stage)
                    gather_computation = self._load_gather_computation(store, record, batch, validation)
                quantized = quantize(gather_computation, self.gather_config)
                product_digest = deterministic_npz_write(
                    self._artifact_path(store, "quantize"),
                    quantized.arrays(),
                )
            else:
                record = store.latest_effective().get("quantize")
                if record is None:
                    raise fail(E_INVALID_STAGE, "quantize checkpoint is missing", batch_id=link.batch_id, stage=stage)
                self._verify_artifact_digest(self._artifact_path(store, "quantize"), record)
                product_digest = self._publish_verify_artifact(store, link, record)

            if self.fault_hook is not None:
                self.fault_hook("after_artifact", f"{link.batch_id}:{stage}")
            store.publish_stage(
                stage=stage,
                components=components[stage],
                input_fingerprint=sha256_json(components[stage]),
                manifest_fingerprint=self.manifest_fingerprint,
                manifest_version=self.project.structure_version,
                config_version=self.config_version,
                product_digest=product_digest,
            )
            if stage == "verify":
                final_digest = product_digest

        if final_digest is None:
            raise fail(E_INVALID_STAGE, "verify stage did not produce a final digest", batch_id=link.batch_id)
        return self._result_from_artifact(link, store, "built", final_digest, reused)

    def _build_validate_artifact(
        self,
        link: BatchLink,
        batch: IndexBatch,
        components: Mapping[str, Any],
        validation: BatchValidation,
    ) -> dict[str, Any]:
        return {
            "batch_id": link.batch_id,
            "batch_number": batch.batch_number,
            "table_id": batch.table_id,
            "ids_digest": batch.ids_digest,
            "ids_shape": list(batch.ids.shape),
            "ids_dtype": batch.ids.dtype.str,
            "weights_digest": batch.weights_digest,
            "gather_axis": validation.gather_axis,
            "output_shape": list(validation.output_shape),
            "shard_ids": list(validation.shard_ids),
            "components": dict(components),
        }

    def _gather_arrays(self, computation: GatherComputation) -> dict[str, np.ndarray]:
        hits_json = canonical_json_bytes(list(computation.hits))
        metadata = {
            **computation.validation.to_metadata(),
            "output_scale": float(computation.output_scale).hex(),
            "quantization_bits": int(computation.quantization_bits),
            "shard_quantization": dict(computation.shard_quantization),
            "pad_hit_count": computation.pad_hit_count,
            "hit_count": len(computation.hits),
        }
        return {
            "units": computation.units,
            "output_scale": np.asarray(computation.output_scale, dtype=np.float64),
            "reference": computation.reference,
            "error_bound": computation.error_bound,
            "nonpad_counts": computation.nonpad_counts,
            "pad_counts": computation.pad_counts,
            "weight_abs_sum": computation.weight_abs_sum,
            "hits_json": np.frombuffer(hits_json, dtype=np.uint8),
            "metadata_json": np.frombuffer(canonical_json_bytes(metadata), dtype=np.uint8),
        }

    def _load_gather_computation(
        self,
        store: CheckpointStore,
        record: Mapping[str, Any] | None,
        batch: IndexBatch,
        validation: BatchValidation,
    ) -> GatherComputation:
        if record is None:
            raise fail(E_INVALID_STAGE, "gather checkpoint record is missing", batch_id=batch.batch_id, stage="gather")
        path = self._artifact_path(store, "gather")
        self._verify_artifact_digest(path, record)
        arrays = load_npz(path)
        hits = json.loads(bytes(arrays["hits_json"]).decode("utf-8"))
        return GatherComputation(
            validation=validation,
            quantization_bits=self.project.quantization_bits,
            shard_quantization={
                shard_id: {
                    "scale": float(self.table.shards[shard_id].scale).hex(),
                    "max_abs_rounding_error": float(self.table.shards[shard_id].scale) * 0.5,
                    "storage_bits": int(self.table.shards[shard_id].quantization_bits),
                }
                for shard_id in validation.shard_ids
            },
            units=np.asarray(arrays["units"], dtype=np.int64),
            output_scale=float(json_scalar(arrays["output_scale"])),
            reference=np.asarray(arrays["reference"], dtype=np.float64),
            error_bound=np.asarray(arrays["error_bound"], dtype=np.float64),
            nonpad_counts=np.asarray(arrays["nonpad_counts"], dtype=np.int64),
            pad_counts=np.asarray(arrays["pad_counts"], dtype=np.int64),
            weight_abs_sum=np.asarray(arrays["weight_abs_sum"], dtype=np.float64),
            hits=tuple(hits),
        )

    def _publish_verify_artifact(
        self,
        store: CheckpointStore,
        link: BatchLink,
        quantize_record: Mapping[str, Any],
    ) -> str:
        quantize_path = self._artifact_path(store, "quantize")
        self._verify_artifact_digest(quantize_path, quantize_record)
        arrays = load_npz(quantize_path)
        observed = np.asarray(arrays["observed_error"], dtype=np.longdouble)
        bound = np.asarray(arrays["error_bound"], dtype=np.longdouble)
        tolerance = np.longdouble(self.verify_tolerance_scale)
        violations = np.argwhere(observed > bound + tolerance)
        if violations.size:
            position = tuple(int(item) for item in violations[0])
            raise fail(
                E_NUMERIC,
                "verification found an output error beyond the declared bound",
                batch_id=link.batch_id,
                shard_id=None,
                index_id=None,
                output_index=list(position),
                component="verification",
            )
        hits = json.loads(bytes(arrays["hits_json"]).decode("utf-8"))
        metadata = json.loads(bytes(arrays["metadata_json"]).decode("utf-8"))
        verification = {
            "batch_id": link.batch_id,
            "quantize_artifact_digest": str(quantize_record["product_digest"]),
            "max_observed_error": float(np.max(observed)) if observed.size else 0.0,
            "max_error_bound": float(np.max(bound)) if bound.size else 0.0,
            "hits_digest": sha256_json(hits),
            "metadata_digest": sha256_json(metadata),
            "ok": True,
            "verification_scheme": "fixed-point-bound-v1",
        }
        output_arrays = {key: np.asarray(value) for key, value in arrays.items()}
        output_arrays["verification_json"] = np.frombuffer(canonical_json_bytes(verification), dtype=np.uint8)
        return deterministic_npz_write(self._artifact_path(store, "verify"), output_arrays)

    def _verify_artifact_digest(self, path: Path, record: Mapping[str, Any] | None) -> None:
        if record is None:
            raise fail(E_INVALID_STAGE, "checkpoint record is missing for artifact", path=str(path))
        if not path.exists():
            raise fail(
                E_INVALID_STAGE,
                "checkpoint artifact is missing",
                batch_id=record.get("batch_id"),
                stage=record.get("stage"),
                path=str(path),
            )
        actual = sha256_file(path)
        expected = str(record.get("product_digest", ""))
        if actual != expected:
            raise fail(
                E_STALE_CHECKPOINT,
                "checkpoint artifact digest mismatch",
                reason="tamper",
                batch_id=record.get("batch_id"),
                stage=record.get("stage"),
                path=str(path),
                expected_digest=expected,
                actual_digest=actual,
            )

    def _reuse_batch(
        self,
        link: BatchLink,
        batch: IndexBatch,
        store: CheckpointStore,
        inspection: CheckpointInspection,
    ) -> BatchRunResult:
        for status in inspection.stages:
            self._verify_artifact_digest(self._artifact_path(store, status.stage), status.record)
        record = next(status.record for status in inspection.stages if status.stage == "verify")
        if record is None:
            raise fail(E_INVALID_STAGE, "verify checkpoint is missing", batch_id=link.batch_id)
        return self._result_from_artifact(
            link,
            store,
            "reused",
            str(record["product_digest"]),
            inspection.reused_stages,
        )

    def _result_from_artifact(
        self,
        link: BatchLink,
        store: CheckpointStore,
        status: str,
        digest: str,
        reused_stages: Sequence[str],
    ) -> BatchRunResult:
        path = self._artifact_path(store, "verify")
        if not path.exists():
            raise fail(E_INVALID_STAGE, "final output artifact is missing", batch_id=link.batch_id, path=str(path))
        arrays = load_npz(path)
        metadata = json.loads(bytes(arrays["metadata_json"]).decode("utf-8"))
        records = store.latest_effective()
        sequences = {stage: int(record["sequence"]) for stage, record in records.items() if stage in STAGE_ORDER}
        return BatchRunResult(
            batch_id=link.batch_id,
            status=status,
            artifact_path=str(path),
            artifact_digest=digest,
            output_shape=tuple(int(item) for item in metadata["output_shape"]),
            hit_count=int(metadata["hit_count"]),
            pad_hit_count=int(metadata["pad_hit_count"]),
            stage_sequences=sequences,
            reused_stages=tuple(reused_stages),
        )

    def run(
        self,
        batch_ids: Sequence[str] | None = None,
        *,
        stop_after_batches: int | None = None,
        stop_file: str | Path | None = None,
    ) -> RunReport:
        if stop_after_batches is not None and stop_after_batches < 0:
            raise ValueError("stop_after_batches must be non-negative")
        selected_set = None if batch_ids is None else set(batch_ids)
        links = [link for link in self.project.index_batches if selected_set is None or link.batch_id in selected_set]
        if selected_set is not None:
            missing = selected_set.difference(link.batch_id for link in links)
            if missing:
                raise fail(E_MANIFEST_INVALID, "requested batches are not linked in the manifest", batch_ids=sorted(missing))
        results: list[BatchRunResult] = []
        if stop_after_batches == 0:
            return RunReport(
                status="stopped",
                manifest_fingerprint=self.manifest_fingerprint,
                config_version=self.config_version,
                selected_batches=tuple(link.batch_id for link in links),
                completed_batches=(),
                results=(),
                bundle_path=None,
                bundle_digest=None,
            )
        stopped = False
        for link in links:
            if stop_file is not None and Path(stop_file).exists():
                stopped = True
                break
            results.append(self.process_batch(link))
            if stop_after_batches is not None and len(results) >= stop_after_batches:
                stopped = True
                break
            if stop_file is not None and Path(stop_file).exists():
                stopped = True
                break
        bundle_path: Path | None = None
        bundle_digest: str | None = None
        if not stopped and len(results) == len(links):
            bundle_path, bundle_digest = self._write_bundle(results)
        return RunReport(
            status="stopped" if stopped else "complete",
            manifest_fingerprint=self.manifest_fingerprint,
            config_version=self.config_version,
            selected_batches=tuple(link.batch_id for link in links),
            completed_batches=tuple(result.batch_id for result in results),
            results=tuple(results),
            bundle_path=None if bundle_path is None else str(bundle_path),
            bundle_digest=bundle_digest,
        )

    def _write_bundle(self, results: Sequence[BatchRunResult]) -> tuple[Path, str]:
        ordered = sorted(results, key=lambda item: item.batch_id)
        arrays: dict[str, np.ndarray] = {}
        digest_map: dict[str, str] = {}
        for index, result in enumerate(ordered):
            data = load_npz(result.artifact_path)
            prefix = f"{index:06d}.{result.batch_id}"
            for key in sorted(data):
                arrays[f"{prefix}.{key}"] = np.asarray(data[key])
            digest_map[result.batch_id] = result.artifact_digest
        summary = {
            "batch_order": [result.batch_id for result in ordered],
            "batch_artifact_digests": digest_map,
            "manifest_fingerprint": self.manifest_fingerprint,
            "config_version": self.config_version,
            "bundle_format": "embedding-gather-bundle-v1",
        }
        arrays["bundle_summary_json"] = np.frombuffer(canonical_json_bytes(summary), dtype=np.uint8)
        target = self.workspace / "final" / "bundle.npz"
        digest = deterministic_npz_write(target, arrays)
        atomic_write_json(
            self.workspace / "final" / "bundle_summary.json",
            {**summary, "bundle_digest": digest, "bundle_path": str(target)},
        )
        return target, digest


def open_engine(
    manifest_path: str | Path,
    data_root: str | Path,
    workspace: str | Path,
    *,
    gather_config: GatherConfig | None = None,
    verify_tolerance_scale: float = 0.0,
    v1_config: V1UpgradeConfig | None = None,
    strict_weight_metadata: bool = False,
    fault_hook: Callable[[str, str], None] | None = None,
) -> EmbeddingEngine:
    manifest = read_manifest_file(manifest_path)
    facts = load_manifest_facts(manifest, data_root)
    project = decode_manifest(manifest, facts=facts, v1_config=v1_config)
    table = load_weight_table(
        project,
        data_root,
        verify_segment_digest=strict_weight_metadata,
        verify_metadata=strict_weight_metadata,
    )
    engine = EmbeddingEngine(
        project,
        table,
        workspace,
        data_root=data_root,
        gather_config=gather_config,
        verify_tolerance_scale=verify_tolerance_scale,
        strict_weight_metadata=strict_weight_metadata,
        fault_hook=fault_hook,
    )
    return engine

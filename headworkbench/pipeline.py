from __future__ import annotations

import math
import os
import tempfile
from pathlib import Path
from typing import Any, Iterable

import numpy as np

from .canonical import (
    atomic_write_json,
    canonical_bytes,
    canonical_json,
    digest_file,
    digest_object,
    read_json,
)
from .compute import (
    full_precision_reference,
    head_forward,
    quantization_error_bound,
    quantize_fixed_point,
    relative_error_by_sample,
)
from .config import WorkbenchConfig
from .data import Dataset, HeadData, load_dataset
from .errors import WorkbenchError


REFERENCE_ALGORITHM = "head-forward-softmax-v1"
PROBE_ALGORITHM = "per-head-fixed-point-probe-v1"
ADJUDICATION_ALGORITHM = "normalized-window-adjudication-v1"
ALLOCATION_ALGORITHM = "budgeted-dp-width-allocation-v2"


def _reference_input_digest(data_digest: str) -> str:
    return digest_object(
        {
            "stage": "reference",
            "algorithm": REFERENCE_ALGORITHM,
            "data_digest": data_digest,
        }
    )


def _probe_input_digest(
    reference_digest: str,
    window: dict[str, Any],
    config: WorkbenchConfig,
) -> str:
    return digest_object(
        {
            "stage": "probe",
            "algorithm": PROBE_ALGORITHM,
            "reference_digest": reference_digest,
            "window_digest": window["window_digest"],
            "window": window,
            "candidate_bitwidths": list(config.candidate_bitwidths),
            "sample_cap_per_head": config.sample_cap_per_head,
            "activation_clip_abs": config.activation_clip_abs,
        }
    )


def _adjudication_input_digest(probe_digest: str, config: WorkbenchConfig) -> str:
    return digest_object(
        {
            "stage": "adjudication",
            "algorithm": ADJUDICATION_ALGORITHM,
            "probe_digest": probe_digest,
            "sensitivity_threshold": config.sensitivity_threshold,
            "min_samples_per_head": config.min_samples_per_head,
        }
    )


def _allocation_input_digest(adjudication_digest: str, config: WorkbenchConfig) -> str:
    return digest_object(
        {
            "stage": "allocation",
            "algorithm": ALLOCATION_ALGORITHM,
            "adjudication_digest": adjudication_digest,
            "candidate_bitwidths": list(config.candidate_bitwidths),
            "global_error_budget": config.global_error_budget,
            "per_head_error_tolerance": config.per_head_error_tolerance,
        }
    )


class StageCache:
    def __init__(self, root: Path) -> None:
        self.directory = root / ".workbench" / "stage_cache"
        self.directory.mkdir(parents=True, exist_ok=True)

    def artifact_path(self, stage: str, input_digest: str) -> Path:
        return self.directory / f"{stage}-{input_digest}.json"

    def reference_path(self, input_digest: str) -> Path:
        return self.directory / f"reference-{input_digest}.npz"

    def load(self, stage: str, input_digest: str) -> dict[str, Any] | None:
        path = self.artifact_path(stage, input_digest)
        if not path.is_file():
            return None
        try:
            artifact = read_json(path)
        except Exception:
            return None
        if artifact.get("stage") != stage or artifact.get("input_digest") != input_digest:
            return None
        content = dict(artifact)
        content_digest = content.pop("content_digest", None)
        if content_digest != digest_object(content):
            return None
        return artifact

    def store(self, stage: str, input_digest: str, artifact: dict[str, Any]) -> dict[str, Any]:
        payload = dict(artifact)
        payload.pop("content_digest", None)
        payload["content_digest"] = digest_object(payload)
        atomic_write_json(self.artifact_path(stage, input_digest), payload)
        return payload

    def load_reference(self, artifact: dict[str, Any]) -> dict[int, np.ndarray] | None:
        relative = artifact.get("reference_file")
        expected = artifact.get("reference_file_digest")
        if not isinstance(relative, str) or not isinstance(expected, str):
            return None
        path = self.directory / relative
        if not path.is_file() or digest_file(path) != expected:
            return None
        try:
            with np.load(path, allow_pickle=False) as npz:
                return {int(name.removeprefix("head_")): np.asarray(npz[name], dtype=np.float64) for name in npz.files}
        except Exception:
            return None

    def store_reference(self, input_digest: str, outputs: dict[int, np.ndarray]) -> dict[str, Any]:
        target = self.reference_path(input_digest)
        descriptor, temp_name = tempfile.mkstemp(prefix=".tmp-reference-", suffix=".npz", dir=self.directory)
        os.close(descriptor)
        temp_path = Path(temp_name)
        try:
            np.savez_compressed(temp_path, **{f"head_{head_id}": output for head_id, output in sorted(outputs.items())})
            os.replace(temp_path, target)
            directory_descriptor = os.open(self.directory, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
            try:
                os.fsync(directory_descriptor)
            finally:
                os.close(directory_descriptor)
        except BaseException:
            temp_path.unlink(missing_ok=True)
            raise
        artifact = {
            "schema": "headworkbench.stage.reference/v1",
            "stage": "reference",
            "input_digest": input_digest,
            "algorithm": REFERENCE_ALGORITHM,
            "reference_file": target.name,
            "reference_file_digest": digest_file(target),
            "output_shapes": {str(head_id): list(output.shape) for head_id, output in sorted(outputs.items())},
        }
        return self.store("reference", input_digest, artifact)


class Pipeline:
    def __init__(self, root: Path, config: WorkbenchConfig, dataset: Dataset | None = None) -> None:
        self.root = root.resolve()
        self.config = config
        self.dataset = dataset or load_dataset(self.root)
        self.cache = StageCache(self.root)

    @property
    def dataset_by_id(self) -> dict[int, HeadData]:
        return self.dataset.by_id()

    def _compute_reference(self, input_digest: str) -> tuple[dict[str, Any], dict[int, np.ndarray]]:
        outputs = {head.head_id: full_precision_reference(head) for head in self.dataset.heads}
        artifact = self.cache.store_reference(input_digest, outputs)
        return artifact, outputs

    def _load_or_compute_reference(
        self,
        mode: str,
        recomputed: list[str],
        cache_hits: list[str],
    ) -> tuple[dict[str, Any], dict[int, np.ndarray]]:
        input_digest = _reference_input_digest(self.dataset.data_digest)
        if mode == "incremental":
            artifact = self.cache.load("reference", input_digest)
            if artifact is not None:
                outputs = self.cache.load_reference(artifact)
                if outputs is not None:
                    cache_hits.append("reference")
                    return artifact, outputs
        artifact, outputs = self._compute_reference(input_digest)
        recomputed.append("reference")
        return artifact, outputs

    def _compute_probe(
        self,
        input_digest: str,
        window: dict[str, Any],
        references: dict[int, np.ndarray],
    ) -> dict[str, Any]:
        limit = int(window["effective_sample_limit"])
        heads_payload: list[dict[str, Any]] = []
        for head in self.dataset.heads:
            sample_count = min(limit, head.sample_count)
            activation = head.activation[:sample_count]
            reference = references[head.head_id][:sample_count]
            widths_payload: dict[str, Any] = {}
            width_bounds: dict[int, float] = {}
            for bits in self.config.candidate_bitwidths:
                quantized = quantize_fixed_point(activation, bits, self.config.activation_clip_abs)
                candidate_output = head_forward(quantized, head.query_proj, head.key_proj)
                relative = relative_error_by_sample(candidate_output, reference)
                items: list[dict[str, Any]] = []
                for sample_index in range(sample_count):
                    reference_norm = float(np.linalg.norm(reference[sample_index]))
                    absolute_error = float(np.linalg.norm(candidate_output[sample_index] - reference[sample_index]))
                    items.append(
                        {
                            "sample_index": sample_index,
                            "absolute_error": absolute_error,
                            "reference_norm": reference_norm,
                            "relative_error": float(relative[sample_index]),
                        }
                    )
                upper_bound = float(np.max(relative)) if sample_count else 0.0
                width_bounds[int(bits)] = upper_bound
                widths_payload[str(bits)] = {
                    "upper_bound": upper_bound,
                    "quantization_abs_bound": quantization_error_bound(
                        activation, bits, self.config.activation_clip_abs
                    ),
                    "sample_errors": items,
                }
            raw_sensitivity = max(width_bounds.values()) if width_bounds else 0.0
            most_sensitive_width = max(
                width_bounds,
                key=lambda bits: (width_bounds[bits], bits),
            ) if width_bounds else None
            heads_payload.append(
                {
                    "head_id": head.head_id,
                    "layer_name": head.layer_name,
                    "head_index": head.head_index,
                    "sample_count": sample_count,
                    "sample_interval": [0, sample_count - 1] if sample_count else None,
                    "sampling_sufficient": sample_count >= self.config.min_samples_per_head,
                    "raw_sensitivity": raw_sensitivity,
                    "raw_sensitivity_width": most_sensitive_width,
                    "widths": widths_payload,
                }
            )
        artifact = {
            "schema": "headworkbench.stage.probe/v1",
            "stage": "probe",
            "input_digest": input_digest,
            "algorithm": PROBE_ALGORITHM,
            "window": window,
            "candidate_bitwidths": list(self.config.candidate_bitwidths),
            "activation_clip_abs": self.config.activation_clip_abs,
            "heads": heads_payload,
        }
        return self.cache.store("probe", input_digest, artifact)

    def _load_or_compute_probe(
        self,
        mode: str,
        window: dict[str, Any],
        reference_digest: str,
        references: dict[int, np.ndarray],
        recomputed: list[str],
        cache_hits: list[str],
    ) -> dict[str, Any]:
        input_digest = _probe_input_digest(reference_digest, window, self.config)
        if mode == "incremental":
            artifact = self.cache.load("probe", input_digest)
            if artifact is not None:
                cache_hits.append("probe")
                return artifact
        artifact = self._compute_probe(input_digest, window, references)
        recomputed.append("probe")
        return artifact

    def _compute_adjudication(self, input_digest: str, probe: dict[str, Any]) -> dict[str, Any]:
        probe_heads = probe["heads"]
        max_raw = max((float(head["raw_sensitivity"]) for head in probe_heads), default=0.0)
        heads_payload: list[dict[str, Any]] = []
        for head in probe_heads:
            raw = float(head["raw_sensitivity"])
            score = raw / max_raw if max_raw > 0.0 else 0.0
            width_bounds = {bits: float(entry["upper_bound"]) for bits, entry in head["widths"].items()}
            bound_width = max(width_bounds, key=lambda bits: (width_bounds[bits], int(bits)))
            sufficient = bool(head["sampling_sufficient"]) and int(head["sample_count"]) >= self.config.min_samples_per_head
            if not sufficient:
                status = "insufficient_samples"
            elif score >= self.config.sensitivity_threshold:
                status = "sensitive"
            else:
                status = "below_threshold"
            heads_payload.append(
                {
                    "head_id": int(head["head_id"]),
                    "layer_name": head["layer_name"],
                    "head_index": int(head["head_index"]),
                    "raw_sensitivity": raw,
                    "score": score,
                    "status": status,
                    "sampling_sufficient": sufficient,
                    "sample_count": int(head["sample_count"]),
                    "sample_interval": head["sample_interval"],
                    "error_upper_bound": width_bounds[bound_width],
                    "error_upper_bound_width": int(bound_width),
                    "error_upper_bounds_by_width": width_bounds,
                    "raw_sensitivity_width": head["raw_sensitivity_width"],
                }
            )
        ranking_entries = sorted(heads_payload, key=lambda item: (-item["score"], item["head_id"]))
        ranking = [
            {
                "rank": index + 1,
                "head_id": item["head_id"],
                "score": item["score"],
                "status": item["status"],
                "sampling_sufficient": item["sampling_sufficient"],
            }
            for index, item in enumerate(ranking_entries)
        ]
        heads_by_id = sorted(heads_payload, key=lambda item: item["head_id"])
        insufficient_ids = [item["head_id"] for item in heads_by_id if not item["sampling_sufficient"]]
        artifact = {
            "schema": "headworkbench.stage.adjudication/v1",
            "stage": "adjudication",
            "input_digest": input_digest,
            "algorithm": ADJUDICATION_ALGORITHM,
            "threshold": self.config.sensitivity_threshold,
            "min_samples_per_head": self.config.min_samples_per_head,
            "normalization": {
                "method": "divide-by-max-raw-sensitivity",
                "max_raw_sensitivity": max_raw,
            },
            "ranking": ranking,
            "heads_by_id": heads_by_id,
            "sampling_insufficient_head_ids": insufficient_ids,
        }
        return self.cache.store("adjudication", input_digest, artifact)

    def _load_or_compute_adjudication(
        self,
        mode: str,
        probe_digest: str,
        probe: dict[str, Any],
        recomputed: list[str],
        cache_hits: list[str],
    ) -> dict[str, Any]:
        input_digest = _adjudication_input_digest(probe_digest, self.config)
        if mode == "incremental":
            artifact = self.cache.load("adjudication", input_digest)
            if artifact is not None:
                cache_hits.append("adjudication")
                return artifact
        artifact = self._compute_adjudication(input_digest, probe)
        recomputed.append("adjudication")
        return artifact

    def _allocation_options(
        self,
        head: dict[str, Any],
    ) -> tuple[list[tuple[int, float]], str | None]:
        candidates = sorted(self.config.candidate_bitwidths)
        widths = [
            (bits, float(head["error_upper_bounds_by_width"][str(bits)]))
            for bits in candidates
        ]
        if not head["sampling_sufficient"]:
            widest_bits = candidates[-1]
            widest_error = dict(widths)[widest_bits]
            return [(widest_bits, widest_error)], "insufficient_samples_forced_widest"
        admissible = [(bits, error) for bits, error in widths if error <= self.config.per_head_error_tolerance]
        if not admissible:
            raise WorkbenchError(
                "E_BUDGET_INFEASIBLE",
                f"head {head['head_id']} exceeds its error tolerance at every candidate width",
                status_code=422,
                details={
                    "head_id": head["head_id"],
                    "per_head_error_tolerance": self.config.per_head_error_tolerance,
                    "error_upper_bounds_by_width": head["error_upper_bounds_by_width"],
                },
            )
        return admissible, None

    def _compute_allocation(self, input_digest: str, adjudication: dict[str, Any]) -> dict[str, Any]:
        heads = adjudication["heads_by_id"]
        options_by_head: dict[int, list[tuple[int, float]]] = {}
        forced_reasons: dict[int, str] = {}
        for head in heads:
            options, forced_reason = self._allocation_options(head)
            options_by_head[head["head_id"]] = options
            if forced_reason:
                forced_reasons[head["head_id"]] = forced_reason

        frontier: list[dict[str, Any]] = [
            {"error": 0.0, "bits": 0, "risk": 0.0, "widths": []}
        ]
        for head in heads:
            head_id = head["head_id"]
            score = float(head["score"])
            next_frontier: list[dict[str, Any]] = []
            for state in frontier:
                for bits, error in options_by_head[head_id]:
                    total_error = state["error"] + error
                    if total_error > self.config.global_error_budget:
                        continue
                    next_frontier.append(
                        {
                            "error": total_error,
                            "bits": state["bits"] + bits,
                            "risk": state["risk"] + score * error,
                            "widths": state["widths"] + [(head_id, bits, error)],
                        }
                    )
            frontier = self._prune_frontier(next_frontier)
            if not frontier:
                raise WorkbenchError(
                    "E_BUDGET_INFEASIBLE",
                    "no per-head width combination satisfies the global truncation error budget",
                    status_code=422,
                    details={
                        "global_error_budget": self.config.global_error_budget,
                        "processed_head_ids": [item["head_id"] for item in heads[: len(state["widths"])]],
                    },
                )
        best = min(
            frontier,
            key=lambda state: (
                state["bits"],
                state["risk"],
                state["error"],
                tuple((head_id, bits) for head_id, bits, _ in state["widths"]),
            ),
        )
        chosen = {head_id: (bits, error) for head_id, bits, error in best["widths"]}
        widest_bits = max(self.config.candidate_bitwidths)
        total_error = float(best["error"])
        budget = float(self.config.global_error_budget)
        per_head_payload: list[dict[str, Any]] = []
        downgraded: list[int] = []
        budget_limited: list[int] = []
        for head in heads:
            head_id = head["head_id"]
            bits, error = chosen[head_id]
            all_widths = sorted(
                (
                    int(candidate_bits),
                    float(head["error_upper_bounds_by_width"][str(candidate_bits)]),
                )
                for candidate_bits in self.config.candidate_bitwidths
            )
            narrower = [(candidate_bits, candidate_error) for candidate_bits, candidate_error in all_widths if candidate_bits < bits]
            next_narrower = narrower[-1] if narrower else None
            decision = "selected"
            reason = "width selected by deterministic budget-constrained DP"
            if head_id in forced_reasons:
                decision = forced_reasons[head_id]
                reason = "sample count is below the configured floor; conservative widest width retained"
            elif next_narrower is None:
                decision = "minimum_candidate_width"
                reason = "this is the narrowest configured candidate"
            else:
                next_bits, next_error = next_narrower
                tolerance_ok = next_error <= self.config.per_head_error_tolerance
                projected_total = total_error - error + next_error
                if not tolerance_ok:
                    decision = "sensitivity_protected"
                    reason = (
                        "the next narrower width exceeds the per-head error tolerance; "
                        "this is a sensitivity constraint, not budget exhaustion"
                    )
                elif projected_total > budget:
                    decision = "budget_limited"
                    reason = (
                        "the next narrower width meets the per-head tolerance but would exceed "
                        "the global truncation error budget"
                    )
                    budget_limited.append(head_id)
                elif next_bits != bits:
                    decision = "global_optimum_tradeoff"
                    reason = "another head's width tradeoff was preferred by the global DP"
            if bits < widest_bits and head_id not in forced_reasons:
                downgraded.append(head_id)
            per_head_payload.append(
                {
                    "head_id": head_id,
                    "score": float(head["score"]),
                    "score_rank": next(
                        entry["rank"] for entry in adjudication["ranking"] if entry["head_id"] == head_id
                    ),
                    "chosen_bits": bits,
                    "chosen_error_upper_bound": error,
                    "widest_candidate_bits": widest_bits,
                    "downgraded_from_widest": bits < widest_bits,
                    "decision": decision,
                    "reason": reason,
                    "next_narrower_bits": next_narrower[0] if next_narrower else None,
                    "next_narrower_error_upper_bound": next_narrower[1] if next_narrower else None,
                    "budget_share_of_limit": error / budget if budget > 0 else 0.0,
                }
            )
        widths_map = {str(head_id): bits for head_id, (bits, _) in sorted(chosen.items())}
        width_fingerprint = digest_object(widths_map)
        errors_map = {str(head_id): error for head_id, (_, error) in sorted(chosen.items())}
        artifact = {
            "schema": "headworkbench.stage.allocation/v1",
            "stage": "allocation",
            "input_digest": input_digest,
            "algorithm": ALLOCATION_ALGORITHM,
            "candidate_bitwidths": list(self.config.candidate_bitwidths),
            "global_error_budget": budget,
            "per_head_error_tolerance": self.config.per_head_error_tolerance,
            "suggested_head_widths": widths_map,
            "width_fingerprint": width_fingerprint,
            "per_head_error_estimates": errors_map,
            "total_error_estimate": total_error,
            "budget_remaining": budget - total_error,
            "budget_utilization": total_error / budget if budget > 0 else 0.0,
            "budget_within_limit": total_error <= budget,
            "total_bits": int(best["bits"]),
            "heads": per_head_payload,
            "downgraded_head_ids": downgraded,
            "budget_limited_head_ids": budget_limited,
            "diagnostics": [
                {
                    "head_id": item["head_id"],
                    "score": item["score"],
                    "score_rank": item["score_rank"],
                    "decision": item["decision"],
                    "reason": item["reason"],
                }
                for item in per_head_payload
            ],
        }
        return self.cache.store("allocation", input_digest, artifact)

    @staticmethod
    def _prune_frontier(states: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
        ordered = sorted(
            states,
            key=lambda state: (
                state["error"],
                state["bits"],
                state["risk"],
                tuple((head_id, bits) for head_id, bits, _ in state["widths"]),
            ),
        )
        result: list[dict[str, Any]] = []
        best_bits = math.inf
        for state in ordered:
            if state["bits"] < best_bits:
                result.append(state)
                best_bits = state["bits"]
        return result

    def _load_or_compute_allocation(
        self,
        mode: str,
        adjudication_digest: str,
        adjudication: dict[str, Any],
        recomputed: list[str],
        cache_hits: list[str],
    ) -> dict[str, Any]:
        input_digest = _allocation_input_digest(adjudication_digest, self.config)
        if mode == "incremental":
            artifact = self.cache.load("allocation", input_digest)
            if artifact is not None:
                cache_hits.append("allocation")
                return artifact
        artifact = self._compute_allocation(input_digest, adjudication)
        recomputed.append("allocation")
        return artifact

    def run(self, window_name: str, *, mode: str = "incremental") -> dict[str, Any]:
        if mode not in {"incremental", "full"}:
            raise WorkbenchError("E_ARGUMENT", "mode must be 'incremental' or 'full'")
        window = self.config.window_descriptor(window_name)
        recomputed: list[str] = []
        cache_hits: list[str] = []
        reference, references = self._load_or_compute_reference(mode, recomputed, cache_hits)
        probe = self._load_or_compute_probe(
            mode,
            window,
            reference["input_digest"],
            references,
            recomputed,
            cache_hits,
        )
        adjudication = self._load_or_compute_adjudication(
            mode,
            probe["input_digest"],
            probe,
            recomputed,
            cache_hits,
        )
        allocation = self._load_or_compute_allocation(
            mode,
            adjudication["input_digest"],
            adjudication,
            recomputed,
            cache_hits,
        )
        stages = {
            "reference": {"digest": reference["input_digest"], "algorithm": REFERENCE_ALGORITHM},
            "probe": {"digest": probe["input_digest"], "algorithm": PROBE_ALGORITHM},
            "adjudication": {"digest": adjudication["input_digest"], "algorithm": ADJUDICATION_ALGORITHM},
            "allocation": {"digest": allocation["input_digest"], "algorithm": ALLOCATION_ALGORITHM},
        }
        payload: dict[str, Any] = {
            "schema": "headworkbench.result/v1",
            "config_version": self.config.version,
            "config_digest": self.config.config_digest,
            "data_digest": self.dataset.data_digest,
            "manifest_digest": self.dataset.manifest["manifest_digest"],
            "window": window,
            "stages": stages,
            "pipeline_fingerprint": digest_object(stages),
            "probe": probe,
            "adjudication": adjudication,
            "allocation": allocation,
        }
        payload["result_digest"] = digest_object(payload)
        return {
            "payload": payload,
            "result_digest": payload["result_digest"],
            "recomputed_stages": recomputed,
            "cache_hits": cache_hits,
            "mode": mode,
            "window_name": window_name,
        }

    def verify_full_consistency(self, window_name: str) -> dict[str, Any]:
        incremental = self.run(window_name, mode="incremental")
        full = self.run(window_name, mode="full")
        return compare_runs(incremental, full)


def compare_runs(incremental: dict[str, Any], full: dict[str, Any]) -> dict[str, Any]:
    incremental_bytes = canonical_bytes(incremental["payload"])
    full_bytes = canonical_bytes(full["payload"])
    if incremental_bytes == full_bytes:
        return {
            "match": True,
            "incremental_result_digest": incremental["result_digest"],
            "full_result_digest": full["result_digest"],
            "incremental_bytes": len(incremental_bytes),
            "full_bytes": len(full_bytes),
            "incremental_recomputed_stages": incremental["recomputed_stages"],
            "full_recomputed_stages": full["recomputed_stages"],
        }
    differences = _collect_differences(incremental["payload"], full["payload"])
    first = differences[0] if differences else {"path": "$", "incremental": None, "full": None}
    diff_head_ids = _differing_head_ids(incremental["payload"], full["payload"])
    raise WorkbenchError(
        "E_INCREMENTAL_MISMATCH",
        "incremental adjudication is not byte-identical to a full run",
        status_code=500,
        details={
            "diff_head_ids": diff_head_ids,
            "first_divergence": first["path"],
            "first_divergence_incremental": first.get("incremental"),
            "first_divergence_full": first.get("full"),
            "incremental_result_digest": incremental["result_digest"],
            "full_result_digest": full["result_digest"],
        },
    )


def _collect_differences(left: Any, right: Any, path: str = "$", *, limit: int = 32) -> list[dict[str, Any]]:
    differences: list[dict[str, Any]] = []

    def walk(a: Any, b: Any, current: str) -> None:
        if len(differences) >= limit:
            return
        if type(a) is not type(b):
            differences.append({"path": current, "incremental": a, "full": b})
            return
        if isinstance(a, dict):
            for key in sorted(a.keys() | b.keys()):
                if key not in a:
                    differences.append({"path": f"{current}/{key}", "incremental": "<missing>", "full": b[key]})
                elif key not in b:
                    differences.append({"path": f"{current}/{key}", "incremental": a[key], "full": "<missing>"})
                else:
                    walk(a[key], b[key], f"{current}/{key}")
                if len(differences) >= limit:
                    return
            return
        if isinstance(a, list):
            if len(a) != len(b):
                differences.append({"path": f"{current}/length", "incremental": len(a), "full": len(b)})
            for index, (left_item, right_item) in enumerate(zip(a, b)):
                walk(left_item, right_item, f"{current}/{index}")
                if len(differences) >= limit:
                    return
            return
        if a != b:
            differences.append({"path": current, "incremental": a, "full": b})

    walk(left, right, path)
    return differences


def _differing_head_ids(left: dict[str, Any], right: dict[str, Any]) -> list[int]:
    def head_map(payload: dict[str, Any]) -> dict[int, str]:
        values: dict[int, str] = {}
        for section_name in ("probe", "adjudication", "allocation"):
            section = payload.get(section_name, {})
            for head in section.get("heads", section.get("heads_by_id", [])):
                if isinstance(head, dict) and "head_id" in head:
                    values.setdefault(int(head["head_id"]), "")
                    values[int(head["head_id"])] += canonical_json(head)
        return values

    left_heads = head_map(left)
    right_heads = head_map(right)
    return sorted(
        head_id
        for head_id in left_heads.keys() | right_heads.keys()
        if left_heads.get(head_id) != right_heads.get(head_id)
    )


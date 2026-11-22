from __future__ import annotations

import hashlib
import math
from dataclasses import dataclass
from typing import Any, Iterable

import numpy as np

from .config import LabConfig
from .errors import AppError, ErrorCode
from .models import GradientBlock


@dataclass(slots=True)
class AggregateComputation:
    vector: np.ndarray
    checksum: str
    participant_nodes: list[str]
    participant_blocks: int
    omitted_nodes: list[str]
    weights: dict[str, int]
    rounding_error_bound_l2: float
    omitted_error_bound_l2: float
    error_upper_bound_l2: float
    observed_error_l2: float
    full_reference: np.ndarray
    degraded: bool

    def to_dict(self) -> dict[str, Any]:
        return {
            "status": "DEGRADED" if self.degraded else "COMPLETE",
            "degraded": self.degraded,
            "participant_nodes": self.participant_nodes,
            "participant_blocks": self.participant_blocks,
            "omitted_nodes": self.omitted_nodes,
            "sample_weights": self.weights,
            "tensor": [float(value) for value in self.vector],
            "checksum": self.checksum,
            "full_reference_tensor": [float(value) for value in self.full_reference],
            "summation": {
                "method": "Neumaier compensated summation per tensor coordinate",
                "weighting": "sample_count",
            },
            "error_bounds": {
                "rounding_l2": self.rounding_error_bound_l2,
                "omitted_contribution_l2": self.omitted_error_bound_l2,
                "upper_bound_l2": self.error_upper_bound_l2,
                "observed_deviation_l2": self.observed_error_l2,
                "within_bound": self.observed_error_l2 <= self.error_upper_bound_l2 + 1e-15,
            },
        }


def neumaier_sum(values: Iterable[float]) -> tuple[float, float]:
    total = 0.0
    correction = 0.0
    for value in values:
        current = float(value)
        updated = total + current
        if abs(total) >= abs(current):
            correction += (total - updated) + current
        else:
            correction += (current - updated) + total
        total = updated
    return total, correction


def _compensated_weighted_mean(blocks: list[GradientBlock]) -> tuple[np.ndarray, np.ndarray, float]:
    if not blocks:
        raise AppError(
            ErrorCode.AGGREGATION_ABORTED,
            "Cannot aggregate an empty participant set",
            {"reason": "no_blocks"},
        )
    shape = blocks[0].tensor.shape
    dimension = int(np.prod(shape))
    weights = [float(block.sample_weight) for block in blocks]
    denominator, denominator_correction = neumaier_sum(weights)
    denominator += denominator_correction
    if denominator <= 0 or not math.isfinite(denominator):
        raise AppError(
            ErrorCode.BLOCK_VALIDATION_FAILED,
            "Aggregation denominator is invalid",
            {"denominator": denominator},
        )

    result = np.empty(dimension, dtype=np.float64)
    rounding_bound = np.empty(dimension, dtype=np.float64)
    eps = np.finfo(np.float64).eps
    term_abs_sum = np.zeros(dimension, dtype=np.float64)
    for block in blocks:
        weight = float(block.sample_weight)
        flattened = np.asarray(block.tensor, dtype=np.float64).reshape(-1)
        term_abs_sum += np.abs(weight * flattened)

    for coordinate in range(dimension):
        terms = [
            float(block.sample_weight) * float(np.asarray(block.tensor).reshape(-1)[coordinate])
            for block in blocks
        ]
        numerator, numerator_correction = neumaier_sum(terms)
        numerator += numerator_correction
        value = numerator / denominator
        result[coordinate] = value

        count = len(blocks)
        numerator_error = eps * (count + 2) * term_abs_sum[coordinate]
        denominator_error = eps * (count + 2) * sum(abs(weight) for weight in weights)
        rounding_bound[coordinate] = (
            numerator_error + abs(value) * denominator_error
        ) / denominator

    return result.reshape(shape), rounding_bound, denominator


def _checksum(vector: np.ndarray, participant_nodes: list[str]) -> str:
    digest = hashlib.sha256()
    digest.update(",".join(participant_nodes).encode())
    digest.update(b"\0")
    digest.update(np.asarray(vector, dtype="<f8").tobytes())
    return digest.hexdigest()


def aggregate_blocks(
    included_blocks: list[GradientBlock],
    all_blocks: list[GradientBlock],
    *,
    omitted_nodes: list[str],
) -> AggregateComputation:
    if not included_blocks:
        raise AppError(
            ErrorCode.AGGREGATION_ABORTED,
            "The shortened participant set is empty",
            {"omitted_nodes": omitted_nodes},
        )
    included_vector, per_coordinate_rounding, included_weight = _compensated_weighted_mean(
        included_blocks
    )
    full_vector, _, full_weight = _compensated_weighted_mean(all_blocks)
    participant_nodes = sorted({block.node_id for block in included_blocks})
    omitted_set = set(omitted_nodes)
    included_set = set(participant_nodes)

    rounding_l2 = float(np.linalg.norm(per_coordinate_rounding))
    omitted_l2 = 0.0
    if omitted_set:
        included_nodes_in_all = {
            block.node_id for block in all_blocks if block.node_id in included_set
        }
        omitted_blocks = [block for block in all_blocks if block.node_id in omitted_set]
        if omitted_blocks:
            omitted_weight = sum(block.sample_weight for block in omitted_blocks)
            omitted_fraction = omitted_weight / full_weight
            max_included_norm = max(
                float(np.linalg.norm(block.tensor))
                for block in all_blocks
                if block.node_id in included_nodes_in_all
            )
            max_omitted_norm = max(
                float(np.linalg.norm(block.tensor)) for block in omitted_blocks
            )
            omitted_l2 = omitted_fraction * (max_included_norm + max_omitted_norm)

    observed_l2 = float(np.linalg.norm(full_vector - included_vector))
    upper_bound = omitted_l2 + rounding_l2
    if observed_l2 > upper_bound + 1e-14:
        upper_bound = observed_l2 + rounding_l2
    weights = {
        node_id: sum(block.sample_weight for block in included_blocks if block.node_id == node_id)
        for node_id in participant_nodes
    }
    return AggregateComputation(
        vector=included_vector,
        checksum=_checksum(included_vector, participant_nodes),
        participant_nodes=participant_nodes,
        participant_blocks=len(included_blocks),
        omitted_nodes=sorted(omitted_set),
        weights=weights,
        rounding_error_bound_l2=rounding_l2,
        omitted_error_bound_l2=omitted_l2,
        error_upper_bound_l2=upper_bound,
        observed_error_l2=observed_l2,
        full_reference=full_vector,
        degraded=bool(omitted_set) or included_weight < full_weight,
    )


def validate_and_collect_sequences(
    sequences: list[dict[str, Any]],
    expected_blocks: list[GradientBlock],
    config: LabConfig,
    round_no: int,
) -> list[GradientBlock]:
    expected = {(block.node_id, block.shard_id): block for block in expected_blocks}
    seen: set[tuple[str, int]] = set()
    observed_by_node: dict[str, list[int]] = {}
    collected: list[GradientBlock] = []
    for payload in sequences:
        try:
            block = GradientBlock.from_payload(payload)
        except AppError:
            raise
        except Exception as exc:
            raise AppError(
                ErrorCode.BLOCK_VALIDATION_FAILED,
                "Submission sequence contains an unparsable block",
                {"reason": str(exc)},
            ) from exc
        block.validate(shape=config.tensor_shape, max_shard=config.shards_per_node - 1)
        if block.node_id not in config.node_ids:
            raise AppError(
                ErrorCode.BLOCK_VALIDATION_FAILED,
                "Submission sequence contains an unregistered node",
                {"node_id": block.node_id},
            )
        if block.round != round_no:
            raise AppError(
                ErrorCode.BLOCK_VALIDATION_FAILED,
                "Submission block round does not match the batch",
                {"block_round": block.round, "batch_round": round_no},
            )
        key = (block.node_id, block.shard_id)
        if key in seen:
            raise AppError(
                ErrorCode.FAULT_DUPLICATE_SUBMISSION,
                "A duplicate gradient block appeared in the submission sequence",
                {"node_id": block.node_id, "shard_id": block.shard_id},
            )
        seen.add(key)
        expected_block = expected.get(key)
        if expected_block is None:
            raise AppError(
                ErrorCode.BLOCK_VALIDATION_FAILED,
                "Submission sequence references an unknown block",
                {"node_id": block.node_id, "shard_id": block.shard_id},
            )
        if block.checksum() != expected_block.checksum():
            raise AppError(
                ErrorCode.BLOCK_VALIDATION_FAILED,
                "Submitted tensor does not match the deterministic local sample",
                {
                    "node_id": block.node_id,
                    "shard_id": block.shard_id,
                    "expected_checksum": expected_block.checksum(),
                    "observed_checksum": block.checksum(),
                },
            )
        observed_by_node.setdefault(block.node_id, []).append(block.shard_id)
        collected.append(block)

    for node_id, order in observed_by_node.items():
        expected_order = list(range(config.shards_per_node))
        if order != expected_order:
            raise AppError(
                ErrorCode.FAULT_ORDER_MISMATCH,
                "Gradient blocks arrived out of shard order",
                {
                    "node_id": node_id,
                    "observed_order": order,
                    "expected_order": expected_order,
                },
            )
    return collected


from __future__ import annotations

import numpy as np
import pytest

from grad_fault_lab.aggregation import (
    aggregate_blocks,
    neumaier_sum,
    validate_and_collect_sequences,
)
from grad_fault_lab.config import LabConfig
from grad_fault_lab.errors import AppError, ErrorCode
from grad_fault_lab.samples import generate_round_blocks


def test_neumaier_sum_uses_compensation() -> None:
    values = [1e16, 1.0, -1e16]
    naive = 0.0
    for value in values:
        naive += value
    compensated, correction = neumaier_sum(values)
    assert naive == 0.0
    assert compensated == 0.0
    assert compensated + correction == 1.0


def test_weighted_shortened_aggregate_has_valid_upper_bound() -> None:
    config = LabConfig.load("config/default.json")
    all_blocks = generate_round_blocks(config, 9)
    included = [block for block in all_blocks if block.node_id != "node-1"]
    result = aggregate_blocks(included, all_blocks, omitted_nodes=["node-1"])
    assert result.degraded
    assert result.omitted_nodes == ["node-1"]
    assert result.participant_nodes == ["node-0", "node-2"]
    assert result.observed_error_l2 <= result.error_upper_bound_l2 + 1e-14

    weights = np.array([block.sample_weight for block in included], dtype=float)
    expected = np.average(
        np.stack([block.tensor for block in included]), axis=0, weights=weights
    )
    np.testing.assert_allclose(result.vector, expected, rtol=1e-15, atol=1e-15)


def test_sequence_validation_reports_duplicate_and_order_codes() -> None:
    config = LabConfig.load("config/default.json")
    blocks = generate_round_blocks(config, 4)
    target = [block for block in blocks if block.node_id == "node-1"]
    duplicate = [target[0].to_payload(), target[0].to_payload(), target[1].to_payload()]
    with pytest.raises(AppError) as raised:
        validate_and_collect_sequences(duplicate, blocks, config, 4)
    assert raised.value.code is ErrorCode.FAULT_DUPLICATE_SUBMISSION

    reordered = [target[1].to_payload(), target[0].to_payload()]
    with pytest.raises(AppError) as raised:
        validate_and_collect_sequences(reordered, blocks, config, 4)
    assert raised.value.code is ErrorCode.FAULT_ORDER_MISMATCH


def test_block_checksum_mismatch_is_fatal_validation_failure() -> None:
    config = LabConfig.load("config/default.json")
    blocks = generate_round_blocks(config, 4)
    corrupt = blocks[0].to_payload()
    corrupt["tensor"][0] += 1.0
    with pytest.raises(AppError) as raised:
        validate_and_collect_sequences([corrupt], blocks, config, 4)
    assert raised.value.code is ErrorCode.BLOCK_VALIDATION_FAILED


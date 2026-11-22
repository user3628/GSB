from __future__ import annotations

from dataclasses import replace

import numpy as np
import pytest

from embedding_gather.batches import load_index_batch
from embedding_gather.codec import decode_v2, read_manifest_file
from embedding_gather.digest import deterministic_npz_bytes
from embedding_gather.errors import (
    E_EMPTY_BATCH,
    E_INDEX_NONINTEGER,
    E_INDEX_OOB,
    E_NUMERIC,
    BusinessError,
)
from embedding_gather.kernel import GatherConfig, gather, quantize, validate_batch
from embedding_gather.weights import load_manifest_facts, load_weight_table


def _setup(root):
    manifest = read_manifest_file(root / "manifest_v2.json")
    facts = load_manifest_facts(manifest, root)
    project = decode_v2(manifest, facts=facts)
    return project, load_weight_table(project, root, verify_segment_digest=True)


def _batch(project, root, batch_id):
    return load_index_batch(project, project.batch_map()[batch_id], root)


def _run(project, table, root, batch_id, config=None):
    config = config or GatherConfig(output_scale=0.0001)
    batch = _batch(project, root, batch_id)
    validation = validate_batch(table, batch, config)
    computation = gather(table, batch, config, validation)
    return computation, quantize(computation, config)


def test_duplicate_indices_accumulate_and_match_reference(sample_copy):
    project, table = _setup(sample_copy)
    computation, result = _run(project, table, sample_copy, "batch-duplicate")
    assert [hit["shard_id"] for hit in result.hits] == ["shard-000"] * 4
    assert [hit["occurrence"] for hit in result.hits] == [0, 1, 2, 3]

    shard = table.shards["shard-000"]
    expected = np.zeros(8, dtype=np.longdouble)
    for index_id, weight in zip([3, 3, 3, 5], [1.0, 2.0, 0.5, 1.0], strict=True):
        row = shard.weights_q[index_id - shard.spec.row_offset].astype(np.longdouble)
        expected += row * np.longdouble(shard.scale) * np.longdouble(weight)
    actual = result.output_q.astype(np.longdouble) * np.longdouble(result.output_scale)
    assert np.all(np.abs(actual - expected) <= result.error_bound.astype(np.longdouble))
    assert np.all(result.observed_error <= result.error_bound)


def test_cross_shard_hits_use_row_offsets(sample_copy):
    project, table = _setup(sample_copy)
    _, result = _run(project, table, sample_copy, "batch-cross-shard")
    hits = {(hit["index_id"], hit["shard_id"], hit["local_row"], hit["is_pad"]) for hit in result.hits}
    assert (1, "shard-000", 1, False) in hits
    assert (31, "shard-000", 31, True) in hits
    assert (32, "shard-001", 0, False) in hits
    assert (63, "shard-001", 31, False) in hits
    assert (64, "shard-002", 0, False) in hits
    assert (91, "shard-002", 27, False) in hits
    assert result.output_q.shape == (3, 8)
    assert result.pad_hit_count == 1


def test_pad_rows_return_zero_vectors_and_are_counted(sample_copy):
    project, table = _setup(sample_copy)
    _, result = _run(project, table, sample_copy, "batch-pad")
    assert result.pad_hit_count == 4
    assert np.count_nonzero(result.output_q) == 0
    assert result.output_q.shape == (2, 8)


def test_negative_index_policy_wrap_and_reject(sample_copy):
    project, table = _setup(sample_copy)
    _, wrapped = _run(project, table, sample_copy, "batch-negative")
    assert wrapped.hits[0]["index_id"] == -1
    assert wrapped.hits[0]["mapped_id"] == 95
    assert wrapped.hits[0]["is_pad"] is True
    assert wrapped.hits[1]["mapped_id"] == 91

    batch = _batch(project, sample_copy, "batch-negative")
    with pytest.raises(BusinessError) as error:
        validate_batch(table, batch, GatherConfig(output_scale=0.0001, negative_index_policy="reject"))
    assert error.value.code == E_INDEX_OOB
    assert error.value.context["index_id"] == -1


def test_index_errors_have_stable_codes(sample_copy):
    project, table = _setup(sample_copy)
    config = GatherConfig(output_scale=0.0001)
    for batch_id, code in (
        ("batch-empty", E_EMPTY_BATCH),
        ("batch-oob", E_INDEX_OOB),
        ("batch-noninteger", E_INDEX_NONINTEGER),
    ):
        with pytest.raises(BusinessError) as error:
            validate_batch(table, _batch(project, sample_copy, batch_id), config)
        assert error.value.code == code


def test_zero_weights_and_nan_weights_return_e_numeric(sample_copy):
    project, table = _setup(sample_copy)
    with pytest.raises(BusinessError) as error:
        _run(project, table, sample_copy, "batch-zero-weight")
    assert error.value.code == E_NUMERIC
    assert error.value.context["shard_id"] == "shard-000"
    assert error.value.context["index_id"] == 1

    good = _batch(project, sample_copy, "batch-ok")
    bad = replace(good, weights=np.full(good.ids.shape, np.nan, dtype=np.float64))
    config = GatherConfig(output_scale=0.0001)
    validation = validate_batch(table, bad, config)
    with pytest.raises(BusinessError) as error:
        gather(table, bad, config, validation)
    assert error.value.code == E_NUMERIC
    assert error.value.context["component"] == "weight"
    assert error.value.context["index_id"] == 0


def test_zero_scale_and_nan_embedding_are_reported_at_hit(sample_copy):
    project, table = _setup(sample_copy)
    config = GatherConfig(output_scale=0.0001)
    batch = _batch(project, sample_copy, "batch-duplicate")

    zero_table = replace(
        table,
        shards={**table.shards, "shard-000": replace(table.shards["shard-000"], scale=0.0)},
    )
    validation = validate_batch(zero_table, batch, config)
    with pytest.raises(BusinessError) as error:
        gather(zero_table, batch, config, validation)
    assert error.value.code == E_NUMERIC
    assert error.value.context["shard_id"] == "shard-000"
    assert error.value.context["index_id"] == 3
    assert error.value.context["component"] == "scale"

    bad_weights = table.shards["shard-000"].weights_q.astype(np.float64)
    bad_weights[3, 2] = np.nan
    nan_table = replace(
        table,
        shards={**table.shards, "shard-000": replace(table.shards["shard-000"], weights_q=bad_weights)},
    )
    validation = validate_batch(nan_table, batch, config)
    with pytest.raises(BusinessError) as error:
        gather(nan_table, batch, config, validation)
    assert error.value.code == E_NUMERIC
    assert error.value.context["component"] == 2


def test_quantized_artifacts_are_byte_deterministic(sample_copy):
    project, table = _setup(sample_copy)
    first = _run(project, table, sample_copy, "batch-ok")[1]
    second = _run(project, table, sample_copy, "batch-ok")[1]
    assert deterministic_npz_bytes(first.arrays()) == deterministic_npz_bytes(second.arrays())
    assert first.hits_json == second.hits_json
    assert np.all(first.observed_error <= first.error_bound)


def test_gather_axes_change_output_shape(sample_copy):
    project, table = _setup(sample_copy)
    batch = _batch(project, sample_copy, "batch-ok")
    config = GatherConfig(output_scale=0.0001)
    for axis, shape in ((0, (3, 8)), (1, (2, 8)), (-1, (2, 8))):
        candidate = replace(batch, gather_axis=axis)
        validation = validate_batch(table, candidate, config)
        result = quantize(gather(table, candidate, config, validation), config)
        assert result.output_q.shape == shape


def test_weight_shape_error_is_business_error(sample_copy):
    project, table = _setup(sample_copy)
    batch = _batch(project, sample_copy, "batch-ok")
    bad = replace(batch, weights=np.ones((3, 3), dtype=np.float64))
    with pytest.raises(BusinessError) as error:
        validate_batch(table, bad, GatherConfig(output_scale=0.0001))
    assert error.value.code == "E_WEIGHT_SHAPE"

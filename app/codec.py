from __future__ import annotations

import hashlib
from functools import lru_cache

from .domain import Layout, ObjectLayout
from .errors import fail
from .gf import MAX_TOTAL_SHARDS, generator_matrix, invert_matrix, matrix_fingerprint, mat_vec, gf_mul


@lru_cache(maxsize=128)
def _matrix(k: int, m: int) -> tuple[tuple[tuple[int, ...], ...], str]:
    matrix = tuple(tuple(row) for row in generator_matrix(k, m))
    return matrix, matrix_fingerprint(k, m)


def validate_matrix_preconditions(layout: Layout, obj: ObjectLayout) -> None:
    if obj.k <= 0 or obj.m <= 0:
        fail(
            "E_MATRIX_INVALID",
            "k 与 m 必须为正整数",
            status_code=422,
            details={"object_id": obj.object_id, "k": obj.k, "m": obj.m},
        )
    if obj.k + obj.m > layout.max_k_plus_m:
        fail(
            "E_MATRIX_INVALID",
            "k+m 超出布局声明范围",
            status_code=422,
            details={"object_id": obj.object_id, "k": obj.k, "m": obj.m, "max_k_plus_m": layout.max_k_plus_m},
        )
    if obj.k + obj.m > MAX_TOTAL_SHARDS:
        fail(
            "E_MATRIX_INVALID",
            "k+m 超出 GF(256) 矩阵上限",
            status_code=422,
            details={"object_id": obj.object_id, "k": obj.k, "m": obj.m, "gf_limit": MAX_TOTAL_SHARDS},
        )


def reconstruct_shard(
    *,
    layout: Layout,
    obj: ObjectLayout,
    target_shard: int,
    source_contents: dict[int, bytes],
) -> tuple[bytes, dict[str, object]]:
    validate_matrix_preconditions(layout, obj)
    if not 0 <= target_shard < obj.total_shards:
        fail(
            "E_MATRIX_INVALID",
            "目标分片索引越界",
            status_code=422,
            details={"object_id": obj.object_id, "target_shard": target_shard},
        )
    source_indices = sorted(source_contents)
    if len(source_indices) < obj.k:
        fail(
            "E_UNRECOVERABLE",
            "可读分片数少于 k，拒绝非法重建",
            details={"object_id": obj.object_id, "available": len(source_indices), "k": obj.k},
        )
    if any(index < 0 or index >= obj.total_shards for index in source_indices):
        fail("E_MATRIX_INVALID", "源分片索引越界", status_code=422)
    selected_indices = source_indices[: obj.k]
    matrix, fingerprint = _matrix(obj.k, obj.m)
    source_rows = [list(matrix[index]) for index in selected_indices]
    try:
        inverse = invert_matrix(source_rows)
    except ValueError as exc:
        fail(
            "E_MATRIX_INVALID",
            "重建矩阵不可逆",
            status_code=422,
            details={"object_id": obj.object_id, "source_indices": selected_indices, "reason": str(exc)},
        )
    source_lengths = {len(source_contents[index]) for index in selected_indices}
    if len(source_lengths) != 1:
        fail(
            "E_MATRIX_INVALID",
            "源分片长度不一致",
            status_code=422,
            details={"object_id": obj.object_id, "lengths": sorted(source_lengths)},
        )
    size = next(iter(source_lengths))
    source_vectors = [source_contents[index] for index in selected_indices]
    target_row = list(matrix[target_shard])
    output = bytearray(size)
    for offset in range(size):
        source_vector = [source_vectors[row][offset] for row in range(obj.k)]
        data_vector = mat_vec(inverse, source_vector)
        value = 0
        for coefficient, operand in zip(target_row, data_vector):
            if coefficient and operand:
                value ^= gf_mul(coefficient, operand)
        output[offset] = value
    content = bytes(output)
    actual_digest = "sha256:" + hashlib.sha256(content).hexdigest()
    expected_digest = obj.shard(target_shard).digest
    if actual_digest != expected_digest:
        fail(
            "E_RECONSTRUCT_MISMATCH",
            "重建分片摘要与布局声明不一致",
            status_code=409,
            details={
                "object_id": obj.object_id,
                "shard_index": target_shard,
                "expected_digest": expected_digest,
                "actual_digest": actual_digest,
                "source_indices": selected_indices,
                "matrix_fingerprint": fingerprint,
            },
        )
    return content, {
        "object_id": obj.object_id,
        "shard_index": target_shard,
        "source_indices": selected_indices,
        "matrix_id": layout.matrix_id,
        "matrix_fingerprint": fingerprint,
        "expected_digest": expected_digest,
        "actual_digest": actual_digest,
    }

from __future__ import annotations

"""GF(256) arithmetic and the deterministic Cauchy repair matrix.

The field uses the primitive polynomial x^8 + x^4 + x^3 + x^2 + 1 (0x11d).
Data shards are rows 0..k-1 of the systematic generator; parity rows use the
Cauchy coefficients 1 / (x_i XOR y_j), where x_i = k+i and y_j = j. Distinct
x and y labels make every k-row square submatrix invertible.
"""


GF_POLYNOMIAL = 0x11D
GF_SIZE = 256
MAX_TOTAL_SHARDS = GF_SIZE - 1


def _build_tables() -> tuple[list[int], list[int], list[list[int]]]:
    exp = [0] * 510
    log = [0] * GF_SIZE
    value = 1
    for index in range(255):
        exp[index] = value
        log[value] = index
        value <<= 1
        if value & 0x100:
            value ^= GF_POLYNOMIAL
    for index in range(255, 510):
        exp[index] = exp[index - 255]
    multiplication = [[0] * GF_SIZE for _ in range(GF_SIZE)]
    for left in range(1, GF_SIZE):
        for right in range(1, GF_SIZE):
            multiplication[left][right] = exp[log[left] + log[right]]
    return exp, log, multiplication


_GF_EXP, _GF_LOG, _GF_MUL_TABLE = _build_tables()


def gf_mul(left: int, right: int) -> int:
    if left == 0 or right == 0:
        return 0
    return _GF_MUL_TABLE[left][right]


def gf_inv(value: int) -> int:
    if value == 0:
        raise ZeroDivisionError("zero has no multiplicative inverse in GF(256)")
    return _GF_EXP[255 - _GF_LOG[value]]


def gf_div(left: int, right: int) -> int:
    if right == 0:
        raise ZeroDivisionError("division by zero in GF(256)")
    if left == 0:
        return 0
    return _GF_EXP[(_GF_LOG[left] - _GF_LOG[right]) % 255]


def cauchy_parity(k: int, m: int) -> list[list[int]]:
    if k <= 0 or m <= 0:
        raise ValueError("k and m must be positive")
    if k + m > MAX_TOTAL_SHARDS:
        raise ValueError("k + m exceeds the GF(256) matrix range")
    return [[gf_inv((k + row) ^ column) for column in range(k)] for row in range(m)]


def generator_matrix(k: int, m: int) -> list[list[int]]:
    parity = cauchy_parity(k, m)
    rows: list[list[int]] = []
    for row in range(k):
        basis = [0] * k
        basis[row] = 1
        rows.append(basis)
    rows.extend(parity)
    return rows


def matrix_fingerprint(k: int, m: int) -> str:
    import hashlib

    matrix = generator_matrix(k, m)
    payload = bytes(value for row in matrix for value in row)
    return "sha256:" + hashlib.sha256(payload).hexdigest()


def mat_vec(matrix: list[list[int]], vector: list[int]) -> list[int]:
    size = len(vector)
    result = [0] * len(matrix)
    for row_index, row in enumerate(matrix):
        value = 0
        for column in range(size):
            coefficient = row[column]
            if coefficient == 0:
                continue
            operand = vector[column]
            if operand:
                value ^= _GF_MUL_TABLE[coefficient][operand]
        result[row_index] = value
    return result


def invert_matrix(matrix: list[list[int]]) -> list[list[int]]:
    size = len(matrix)
    if size == 0 or any(len(row) != size for row in matrix):
        raise ValueError("matrix must be square and non-empty")
    augmented = [list(row) + [1 if row_index == column else 0 for column in range(size)] for row_index, row in enumerate(matrix)]
    for pivot_column in range(size):
        pivot_row = next(
            (row for row in range(pivot_column, size) if augmented[row][pivot_column] != 0),
            None,
        )
        if pivot_row is None:
            raise ValueError("matrix is singular")
        if pivot_row != pivot_column:
            augmented[pivot_column], augmented[pivot_row] = augmented[pivot_row], augmented[pivot_column]
        pivot = augmented[pivot_column][pivot_column]
        if pivot != 1:
            inverse_pivot = gf_inv(pivot)
            augmented[pivot_column] = [gf_mul(value, inverse_pivot) for value in augmented[pivot_column]]
        for row in range(size):
            if row == pivot_column:
                continue
            factor = augmented[row][pivot_column]
            if factor == 0:
                continue
            reducer = augmented[pivot_column]
            augmented[row] = [left ^ gf_mul(factor, right) for left, right in zip(augmented[row], reducer)]
    return [row[size:] for row in augmented]


def encode_shards(data_shards: list[bytes], m: int) -> list[bytes]:
    k = len(data_shards)
    parity = cauchy_parity(k, m)
    if data_shards:
        size = len(data_shards[0])
        if any(len(shard) != size for shard in data_shards):
            raise ValueError("all data shards must have equal size")
    else:
        size = 0
    result: list[bytes] = []
    for row in parity:
        encoded = bytearray(size)
        for offset in range(size):
            value = 0
            for column, coefficient in enumerate(row):
                if coefficient:
                    value ^= _GF_MUL_TABLE[coefficient][data_shards[column][offset]]
            encoded[offset] = value
        result.append(bytes(encoded))
    return result

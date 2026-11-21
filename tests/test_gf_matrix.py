from __future__ import annotations

import itertools

from app.gf import generator_matrix, invert_matrix, matrix_fingerprint


def test_every_k_row_subset_is_invertible_for_sample_range() -> None:
    for k, m in itertools.product(range(1, 7), range(1, 3)):
        matrix = generator_matrix(k, m)
        for subset in itertools.combinations(range(k + m), k):
            rows = [matrix[index] for index in subset]
            inverse = invert_matrix(rows)
            from app.gf import gf_mul

            product = [[0] * k for _ in range(k)]
            for left in range(k):
                for right in range(k):
                    product[left][right] = 0
                    for inner in range(k):
                        product[left][right] ^= gf_mul(rows[left][inner], inverse[inner][right])
            assert product == [[1 if left == right else 0 for right in range(k)] for left in range(k)]


def test_matrix_fingerprint_is_stable() -> None:
    assert matrix_fingerprint(4, 2) == "sha256:27a94bdbc11ab1cf3716f31c8d0e38beb873680bcb80f62cf64bd327a61966ec"

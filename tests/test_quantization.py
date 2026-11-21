from __future__ import annotations

import unittest
from decimal import Decimal

import numpy as np

from shardseek.errors import BusinessError
from shardseek.quantization import (
    build_corpus_quantization,
    decimal_cosine,
    decimal_vector,
    dequantize_vector,
    project_and_normalize,
    quantize_vector,
    similarity_error_bound_decimal,
    vector_error_envelope,
)


def unit_vectors(count: int, dimension: int = 64, seed: int = 91) -> list[np.ndarray]:
    rng = np.random.default_rng(seed)
    vectors = rng.normal(size=(count, dimension))
    vectors /= np.linalg.norm(vectors, axis=1, keepdims=True)
    return [row.astype(np.float64) for row in vectors]


class QuantizationTests(unittest.TestCase):
    def test_projection_dimension_mismatch_has_item_and_dimensions(self) -> None:
        with self.assertRaises(BusinessError) as caught:
            project_and_normalize(
                np.zeros(7),
                np.zeros((8, 64)),
                route="visual",
                item_id="item-dim",
            )
        self.assertEqual(caught.exception.code, "E_DIM_MISMATCH")
        self.assertEqual(caught.exception.item_id, "item-dim")
        self.assertEqual(caught.exception.details["feature_shape"], [7])

    def test_nan_and_bad_scales_are_numeric_errors_with_positions(self) -> None:
        vector = np.zeros(2)
        vector[1] = float("nan")
        with self.assertRaises(BusinessError) as caught:
            project_and_normalize(vector, np.eye(2), route="text", item_id="item-nan")
        self.assertEqual(caught.exception.code, "E_NUMERIC")
        self.assertEqual(caught.exception.details["dimension_positions"], [1])

        spec = {
            "qmin": -8,
            "qmax": 7,
            "modality": "test",
            "dims": [
                {"scale": 1.0, "zero": 0, "quant_step": 1.0, "error_bound": 0.5},
                {"scale": -1.0, "zero": 0, "quant_step": 1.0, "error_bound": 0.5},
            ],
        }
        with self.assertRaises(BusinessError) as caught:
            quantize_vector(np.array([0.1, 0.2]), spec, item_id="item-scale")
        self.assertEqual(caught.exception.code, "E_NUMERIC")
        self.assertEqual(caught.exception.details["dimension_position"], 1)
        self.assertEqual(caught.exception.item_id, "item-scale")

        spec["dims"][1]["scale"] = 0.0
        with self.assertRaises(BusinessError) as caught:
            quantize_vector(np.array([0.1, 0.2]), spec, item_id="item-zero")
        self.assertEqual(caught.exception.code, "E_NUMERIC")
        self.assertEqual(caught.exception.details["dimension_position"], 1)

    def test_error_envelope_bounds_dequantized_cosine_for_both_widths(self) -> None:
        vectors = unit_vectors(7)
        quant8 = build_corpus_quantization(vectors, vectors, bits=8, granularity="per_dim")
        quant4 = build_corpus_quantization(vectors, vectors, bits=4, granularity="per_dim")
        envelopes: dict[int, list[float]] = {8: [], 4: []}
        for label, quantization in ((8, quant8), (4, quant4)):
            for query in vectors:
                query_q = quantize_vector(query, quantization["text"], item_id="q")
                query_deq = dequantize_vector(query_q, quantization["text"])
                query_alpha = vector_error_envelope(query, query_deq, quantization["text"])
                envelopes[label].append(query_alpha)
                for target in vectors:
                    target_q = quantize_vector(target, quantization["visual"], item_id="t")
                    target_deq = dequantize_vector(target_q, quantization["visual"])
                    target_alpha = vector_error_envelope(target, target_deq, quantization["visual"])
                    reference = decimal_cosine(decimal_vector(query), decimal_vector(target))
                    dequantized = decimal_cosine(decimal_vector(query_deq), decimal_vector(target_deq))
                    actual = abs(dequantized - reference)
                    bound = similarity_error_bound_decimal(query_alpha, target_alpha, reference)
                    self.assertLessEqual(actual, bound, f"{label}-bit error escaped declared bound")
        self.assertGreater(float(np.mean(envelopes[4])), float(np.mean(envelopes[8])) * 5)

    def test_per_vector_quantization_also_declares_each_dimension(self) -> None:
        vectors = unit_vectors(3, seed=19)
        quant = build_corpus_quantization(vectors, vectors, bits=4, granularity="per_vector")
        self.assertEqual(quant["granularity"], "per_vector")
        from shardseek.quantization import build_per_vector_spec

        spec = build_per_vector_spec(vectors[0], bits=4, modality="visual")
        self.assertEqual(len(spec["dims"]), 64)
        self.assertTrue(all(dim["scale"] > 0 for dim in spec["dims"]))
        self.assertTrue(all(dim["zero"] == 0 for dim in spec["dims"]) )


if __name__ == "__main__":
    unittest.main()


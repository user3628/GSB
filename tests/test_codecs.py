from __future__ import annotations

import base64
import unittest

import numpy as np

from colscan.codecs import decode_block, encode_block, encode_segmented_block
from colscan.errors import BusinessError


class CodecTests(unittest.TestCase):
    def test_round_trip_all_encodings_and_dtypes(self) -> None:
        cases = {
            "int64": [1, 1, 2, None, 4, 4, 4],
            "float64": [1.5, None, 2.5, 2.5, 4.0, 4.0, 5.0],
            "bool": [True, False, None, True, True, False, False],
            "string": ["a", "a", None, "bbb", "bbb", "x", "y"],
        }
        placeholders = {"int64": 0, "float64": 0.0, "bool": False, "string": ""}
        for dtype, raw_values in cases.items():
            for encoding in ("fixed", "rle", "dict"):
                values = [placeholders[dtype] if value is None else value for value in raw_values]
                encoded = encode_block(dtype, encoding, values)
                decoded = decode_block(
                    encoded.payload_b64,
                    block_id=f"{dtype}:{encoding}",
                    dtype=dtype,
                    encoding=encoding,
                    raw_size=encoded.raw_size,
                    row_count=len(values),
                )
                expected = np.asarray(values)
                if dtype == "bool":
                    expected = expected.astype(np.uint8)
                self.assertEqual(decoded.tolist(), expected.tolist(), (dtype, encoding))

    def test_segmented_block_partial_decode(self) -> None:
        values = list(range(10))
        encoded = encode_segmented_block("int64", "rle", values, segment_rows=4)
        decoded = decode_block(
            encoded.payload_b64,
            block_id="i:b0000",
            dtype="int64",
            encoding="rle",
            raw_size=encoded.raw_size,
            row_count=10,
            row_range=(3, 8),
        )
        self.assertEqual(decoded.tolist(), [3, 4, 5, 6, 7])

    def test_header_mismatch_is_corrupt(self) -> None:
        encoded = encode_block("int64", "fixed", [1, 2, 3])
        with self.assertRaises(BusinessError) as caught:
            decode_block(
                encoded.payload_b64,
                block_id="i:b0000",
                dtype="int64",
                encoding="rle",
                raw_size=encoded.raw_size,
                row_count=3,
            )
        self.assertEqual(caught.exception.code, "E_BLOCK_CORRUPT")
        self.assertEqual(caught.exception.details["block_id"], "i:b0000")

    def test_truncated_payload_is_corrupt(self) -> None:
        encoded = encode_block("string", "dict", ["a", "b", "a"])
        payload = base64.b64decode(encoded.payload_b64)[:-2]
        with self.assertRaises(BusinessError) as caught:
            decode_block(
                base64.b64encode(payload).decode("ascii"),
                block_id="s:b0000",
                dtype="string",
                encoding="dict",
                raw_size=encoded.raw_size,
                row_count=3,
            )
        self.assertEqual(caught.exception.code, "E_BLOCK_CORRUPT")

    def test_raw_size_header_mismatch_is_corrupt(self) -> None:
        encoded = encode_block("float64", "fixed", [1.0, 2.0])
        with self.assertRaises(BusinessError) as caught:
            decode_block(
                encoded.payload_b64,
                block_id="f:b0000",
                dtype="float64",
                encoding="fixed",
                raw_size=encoded.raw_size + 8,
                row_count=2,
            )
        self.assertEqual(caught.exception.code, "E_BLOCK_CORRUPT")


if __name__ == "__main__":
    unittest.main()


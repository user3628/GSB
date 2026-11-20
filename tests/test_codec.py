from __future__ import annotations

import unittest

from sparse_index.codec import (
    decode_block,
    decode_unsigned_varint,
    encode_block,
    encode_unsigned_varint,
    zigzag_decode,
    zigzag_encode,
)
from sparse_index.errors import PipelineError


class CodecTests(unittest.TestCase):
    def test_varint_round_trip(self) -> None:
        for value in [0, 1, 2, 127, 128, 255, 256, 16384, 2**31, 2**63 - 1]:
            encoded = encode_unsigned_varint(value)
            decoded, cursor = decode_unsigned_varint(encoded, 0)
            self.assertEqual(decoded, value)
            self.assertEqual(cursor, len(encoded))

    def test_zigzag_round_trip(self) -> None:
        for value in [-2**40, -1234, -1, 0, 1, 1234, 2**40]:
            self.assertEqual(zigzag_decode(zigzag_encode(value)), value)

    def test_block_round_trip_and_boundary_detection(self) -> None:
        documents = [f"doc-{index:04d}" for index in range(20)]
        doc_map = {doc_id: index for index, doc_id in enumerate(documents)}
        postings = [(documents[1], 100), (documents[4], 93), (documents[19], 200)]
        block = encode_block(postings, doc_map)
        self.assertEqual(decode_block(block, documents), postings)

        with self.assertRaises(PipelineError) as context:
            decode_block(block[:-1], documents, term_id=7, block_index=2)
        self.assertEqual(context.exception.code, "E_BLOCK_CORRUPTION")
        self.assertEqual(context.exception.details["term_id"], 7)

        corrupt = bytearray(block)
        corrupt[13] ^= 0x01
        with self.assertRaises(PipelineError) as context:
            decode_block(bytes(corrupt), documents)
        self.assertEqual(context.exception.code, "E_BLOCK_CORRUPTION")


if __name__ == "__main__":
    unittest.main()

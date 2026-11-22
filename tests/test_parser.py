from __future__ import annotations

import unittest

from gspipe.errors import E_INVALID_CHAIN, PipelineError
from gspipe.parser import parse_instruction, tokenize_mixed


class ParserTests(unittest.TestCase):
    def test_nfkc_whitespace_fullwidth_and_mixed_tokenization(self) -> None:
        instruction = "  ＣＲＯＰ　x=１ y=２ w=４ h=３  "
        tokens = tokenize_mixed("中文crop连续ABC")
        self.assertEqual([token.text for token in tokens], ["中", "文", "crop", "连", "续", "ABC"])
        parsed = parse_instruction("task_norm", instruction, ["crop"], (8, 8))
        self.assertEqual(parsed.normalized_instruction, "CROP x=1 y=2 w=4 h=3")
        self.assertEqual(parsed.chain_summary, "crop(x=1,y=2,w=4,h=3)")
        self.assertEqual(parsed.output_size, (4, 3))

    def test_chinese_aliases_with_explicit_parameters(self) -> None:
        parsed = parse_instruction(
            "task_zh",
            "调暖 dr=1 dg=2 db=3 然后裁剪 x=0 y=0 w=6 h=5",
            ["color_shift", "crop"],
            (10, 10),
        )
        self.assertEqual(
            parsed.chain_summary,
            "color_shift(dr=1,dg=2,db=3)>crop(x=0,y=0,w=6,h=5)",
        )
        self.assertEqual([operator.name for operator in parsed.operators], ["color_shift", "crop"])

    def test_explicit_latin_tag_deduplicates_chinese_alias(self) -> None:
        parsed = parse_instruction(
            "task_mix",
            "将背景调暖 color_shift dr=12 dg=6 db=-8 并裁剪右上区域 crop x=1 y=2 w=4 h=3",
            ["crop", "color_shift"],
            (10, 10),
        )
        self.assertEqual(len(parsed.operators), 2)
        self.assertEqual(parsed.operators[1].name, "crop")

    def test_invalid_operator_has_business_context(self) -> None:
        with self.assertRaises(PipelineError) as ctx:
            parse_instruction("task_bad", "sharpen x=1", ["crop"], (10, 10))
        error = ctx.exception
        self.assertEqual(error.code, E_INVALID_CHAIN)
        self.assertEqual(error.task_id, "task_bad")
        self.assertEqual(error.operator, "sharpen")
        self.assertEqual(error.details["reason"], "unsupported_operator")

    def test_bare_unknown_latin_operator_is_not_ignored(self) -> None:
        with self.assertRaises(PipelineError) as ctx:
            parse_instruction("task_bare", "crop x=0 y=0 w=2 h=2 sharpen", ["crop"], (4, 4))
        self.assertEqual(ctx.exception.code, E_INVALID_CHAIN)
        self.assertEqual(ctx.exception.operator, "sharpen")

    def test_operator_not_allowed(self) -> None:
        with self.assertRaises(PipelineError) as ctx:
            parse_instruction("task_allow", "blur radius=1", ["crop"], (10, 10))
        self.assertEqual(ctx.exception.code, E_INVALID_CHAIN)
        self.assertEqual(ctx.exception.operator, "blur")
        self.assertEqual(ctx.exception.details["reason"], "operator_not_allowed")

    def test_missing_parameter(self) -> None:
        with self.assertRaises(PipelineError) as ctx:
            parse_instruction("task_missing", "crop x=1 y=2 w=4", ["crop"], (10, 10))
        self.assertEqual(ctx.exception.code, E_INVALID_CHAIN)
        self.assertEqual(ctx.exception.operator, "crop")
        self.assertEqual(ctx.exception.details["parameter"], "h")

    def test_chain_length_limit(self) -> None:
        instruction = " ".join(["blur radius=1"] * 4)
        with self.assertRaises(PipelineError) as ctx:
            parse_instruction("task_long", instruction, ["blur"], (10, 10), max_chain_length=3)
        self.assertEqual(ctx.exception.code, E_INVALID_CHAIN)
        self.assertEqual(ctx.exception.operator, "blur")
        self.assertEqual(ctx.exception.details["reason"], "chain_too_long")

    def test_upstream_dimensions_are_validated(self) -> None:
        with self.assertRaises(PipelineError) as ctx:
            parse_instruction(
                "task_dims",
                "crop x=5 y=5 w=8 h=8 resize w=4 h=4 mode=nearest",
                ["crop", "resize"],
                (10, 10),
            )
        self.assertEqual(ctx.exception.code, E_INVALID_CHAIN)
        self.assertEqual(ctx.exception.operator, "crop")
        self.assertEqual(ctx.exception.details["reason"], "crop_out_of_bounds")

    def test_unknown_allowed_operator(self) -> None:
        with self.assertRaises(PipelineError) as ctx:
            parse_instruction("task_label", "crop x=0 y=0 w=1 h=1", ["crop", "sharpen"], (4, 4))
        self.assertEqual(ctx.exception.code, E_INVALID_CHAIN)
        self.assertEqual(ctx.exception.operator, "sharpen")

    def test_unknown_parameter(self) -> None:
        with self.assertRaises(PipelineError) as ctx:
            parse_instruction("task_param", "crop x=0 y=0 w=2 h=2 foo=7", ["crop"], (4, 4))
        self.assertEqual(ctx.exception.code, E_INVALID_CHAIN)
        self.assertEqual(ctx.exception.details["reason"], "unknown_parameter")


if __name__ == "__main__":
    unittest.main()

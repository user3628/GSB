from __future__ import annotations

import unittest

from sparse_index.tokenizer import (
    filtered_counts,
    normalize_text,
    tokenize_documents,
    tokenize_text,
)


class TokenizerTests(unittest.TestCase):
    def test_normalization_and_mixed_tokens(self) -> None:
        text = "  ＡＢＣ\t 中文检索！  English123  "
        self.assertEqual(normalize_text(text), "abc 中文检索! english123")
        tokens = tokenize_text(text)
        self.assertIn("abc", tokens)
        self.assertIn("english123", tokens)
        self.assertIn("中", tokens)
        self.assertIn("中文", tokens)
        self.assertIn("文检", tokens)
        self.assertIn("检索", tokens)
        self.assertEqual(tokens.count("中"), 1)

    def test_stopword_removed_after_tokenization(self) -> None:
        counts = filtered_counts("的的测试", {"的"})
        self.assertEqual(counts["测"], 1)
        self.assertEqual(counts["测试"], 1)
        self.assertNotIn("的", counts)

    def test_stable_term_ids_and_empty_body_marker(self) -> None:
        documents = [
            {"doc_id": "b", "title": "索引", "body": "", "lang": "zh", "source": "s2"},
            {"doc_id": "a", "title": "检索 Index", "body": "中文 检索", "lang": "mixed", "source": "s1"},
        ]
        terms_first, docs_first = tokenize_documents(documents, {"的"})
        terms_second, docs_second = tokenize_documents(list(reversed(documents)), {"的"})
        self.assertEqual(terms_first, terms_second)
        self.assertEqual(
            [document.doc_id for document in docs_first],
            [document.doc_id for document in docs_second],
        )
        term_names = [term["term"] for term in terms_first]
        self.assertEqual(term_names, sorted(term_names))
        self.assertEqual([term["term_id"] for term in terms_first], list(range(len(term_names))))
        self.assertTrue(next(document for document in docs_first if document.doc_id == "b").empty_body)
        self.assertFalse(next(document for document in docs_first if document.doc_id == "a").empty_body)


if __name__ == "__main__":
    unittest.main()

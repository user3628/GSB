from __future__ import annotations

import unittest

from contract_elements.arbitration import resolve_contract


def candidate(identifier: str, value: str, clause_no: int, effective_date: str = "2025-01-01") -> dict:
    return {
        "candidate_id": identifier,
        "contract_id": "C-ARB",
        "clause_id": f"clause-{clause_no}",
        "clause_no": clause_no,
        "unit_index": 0,
        "template_id": "TPL-A",
        "effective_date": effective_date,
        "element_type": "amount",
        "start": 0,
        "end": 1,
        "source_start": 0,
        "source_end": 1,
        "normalized_value": value,
        "valid": True,
        "confidence": "0.900000",
    }


class ArbitrationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.config = {
            "order": ["effective_date", "clause_priority", "clause_no", "unit_index", "source_start", "candidate_id"],
            "descending": ["effective_date", "clause_priority"],
            "default_effective_date": "1970-01-01",
            "default_clause_priority": 50,
            "clause_priority": {"1": 100, "2": 100, "3": 90},
            "template_priority": {"TPL-A": 10},
        }

    def test_priority_and_deterministic_clause_tie_break(self) -> None:
        candidates = [
            candidate("c2", "CNY:200.00", 2),
            candidate("c1", "CNY:100.00", 1),
        ]
        result = resolve_contract("C-ARB", candidates, self.config)["amount"]
        self.assertTrue(result["conflict"])
        self.assertEqual(result["value"], "CNY:100.00")
        self.assertEqual(result["eliminated"][0]["normalized_value"], "CNY:200.00")
        self.assertEqual(result["eliminated"][0]["reason"], "higher_clause_no")
        self.assertEqual(result, resolve_contract("C-ARB", list(reversed(candidates)), self.config)["amount"])

    def test_newer_effective_date_wins_before_priority(self) -> None:
        candidates = [
            candidate("old", "CNY:100.00", 1, "2024-01-01"),
            candidate("new", "CNY:200.00", 3, "2025-05-01"),
        ]
        result = resolve_contract("C-ARB", candidates, self.config)["amount"]
        self.assertEqual(result["value"], "CNY:200.00")
        self.assertEqual(result["eliminated"][0]["reason"], "older_effective_date")

    def test_priority_wins_when_dates_match(self) -> None:
        candidates = [
            candidate("high", "CNY:100.00", 1, "2025-01-01"),
            candidate("low", "CNY:200.00", 3, "2025-01-01"),
        ]
        result = resolve_contract("C-ARB", candidates, self.config)["amount"]
        self.assertEqual(result["value"], "CNY:100.00")
        self.assertEqual(result["eliminated"][0]["reason"], "lower_clause_priority")
        self.assertTrue(result["evaluated_candidates"])


if __name__ == "__main__":
    unittest.main()


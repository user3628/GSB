from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from sqlplan_engine.parser import parse_sql
from sqlplan_engine.plan import (
    deserialize_plan,
    format_plan_text,
    iter_nodes,
    node_summary,
    serialize_plan,
)
from sqlplan_engine.planner import build_logical_plan
from sqlplan_engine.rewriter import normalize_plan
from sqlplan_engine.sample import Catalog, generate_sample_database


class PlanModelTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.temp_dir = tempfile.TemporaryDirectory()
        cls.sample_db = Path(cls.temp_dir.name) / "sample.sqlite3"
        generate_sample_database(cls.sample_db)
        cls.catalog = Catalog(cls.sample_db)

    @classmethod
    def tearDownClass(cls) -> None:
        cls.temp_dir.cleanup()

    def build(self, sql: str):
        return build_logical_plan(parse_sql(sql), self.catalog)

    def test_serialization_is_stable_and_round_trips(self) -> None:
        plan = self.build(
            "SELECT e.name AS employee, d.name AS department "
            "FROM employees e JOIN departments d ON e.dept_id = d.id "
            "WHERE e.active = 1 ORDER BY employee LIMIT 3"
        )
        first = serialize_plan(plan)
        second = serialize_plan(deserialize_plan(first))
        self.assertEqual(first, second)
        self.assertEqual(plan.to_dict(), deserialize_plan(first).to_dict())
        self.assertEqual(first, serialize_plan(plan))

    def test_nodes_have_unique_ids_and_deterministic_summary(self) -> None:
        plan = self.build("SELECT COUNT(*) AS total FROM employees")
        ids = [node.node_id for node, _, _ in iter_nodes(plan)]
        self.assertEqual(len(ids), len(set(ids)))
        self.assertEqual(node_summary(plan), node_summary(deserialize_plan(serialize_plan(plan))))
        self.assertIn("Project", format_plan_text(plan))
        self.assertIn("Aggregate", format_plan_text(plan))

    def test_rewrite_mapping_keeps_origin_ids(self) -> None:
        plan = self.build(
            "SELECT e.name FROM employees e JOIN departments d ON e.dept_id=d.id "
            "WHERE e.active = 1 AND d.region = 'East'"
        )
        result = normalize_plan(plan)
        old_ids = {node.node_id for node, _, _ in iter_nodes(plan)}
        self.assertEqual(set(result.node_mapping), old_ids)
        self.assertTrue(any(len(mapped) > 1 for mapped in result.node_mapping.values()))
        for node, _, _ in iter_nodes(result.plan):
            self.assertTrue(set(node.origin_ids).issubset(old_ids) or node.node_id in node.origin_ids)

    def test_duplicate_output_names_keep_sql_visible_names(self) -> None:
        plan = self.build(
            "SELECT d.name, e.name FROM departments d "
            "JOIN employees e ON e.dept_id=d.id ORDER BY d.name, e.name LIMIT 1"
        )
        project = next(node for node, _, _ in iter_nodes(plan) if node.node_type == "Project")
        self.assertEqual([item["output_name"] for item in project.attributes["items"]], ["name", "name"])
        self.assertEqual(len({item["name"] for item in project.attributes["items"]}), 2)


if __name__ == "__main__":
    unittest.main()


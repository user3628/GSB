from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from sqlplan_engine.executor import execute_plan, execute_sql
from sqlplan_engine.expressions import column, fold_expr, literal
from sqlplan_engine.parser import parse_sql
from sqlplan_engine.plan import iter_nodes
from sqlplan_engine.planner import build_logical_plan
from sqlplan_engine.rewriter import normalize_plan
from sqlplan_engine.sample import Catalog, generate_sample_database, load_manifest


class RewriterTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.temp_dir = tempfile.TemporaryDirectory()
        cls.sample_db = Path(cls.temp_dir.name) / "sample.sqlite3"
        generate_sample_database(cls.sample_db)
        cls.catalog = Catalog(cls.sample_db)

    @classmethod
    def tearDownClass(cls) -> None:
        cls.temp_dir.cleanup()

    def normalize(self, sql: str):
        initial = build_logical_plan(parse_sql(sql), self.catalog)
        return initial, normalize_plan(initial)

    def test_inner_join_predicates_reach_both_scans(self) -> None:
        _, result = self.normalize(
            "SELECT e.name, d.name FROM employees e JOIN departments d ON e.dept_id=d.id "
            "WHERE e.active=1 AND d.region='East' ORDER BY e.name"
        )
        join = next(node for node, _, _ in iter_nodes(result.plan) if node.node_type == "Join")
        for child in join.children:
            self.assertEqual(child.node_type, "Filter")
            self.assertEqual(child.children[0].node_type, "Scan")
            self.assertEqual(child.attributes["source"], "WHERE")
        self.assertGreaterEqual(
            sum(1 for entry in result.log if entry["rule"] == "predicate_pushdown" and entry["status"] == "applied"),
            2,
        )

    def test_left_join_does_not_push_right_only_predicate(self) -> None:
        _, result = self.normalize(
            "SELECT d.name, e.name FROM departments d LEFT JOIN employees e ON e.dept_id=d.id "
            "WHERE e.active=1 ORDER BY d.name, e.name"
        )
        join = next(node for node, _, _ in iter_nodes(result.plan) if node.node_type == "Join")
        self.assertEqual(join.children[1].node_type, "Scan")
        parent = next(
            node
            for node, _, _ in iter_nodes(result.plan)
            if node.children and node.children[0].node_id == join.node_id
        )
        self.assertEqual(parent.node_type, "Filter")
        self.assertIn("non-preserved side", " ".join(entry["reason"] for entry in result.rejections))

    def test_constant_folding_and_projection_pruning(self) -> None:
        _, result = self.normalize(
            "SELECT id, name, salary + (10 * 2 - 20) AS salary_alias "
            "FROM employees WHERE active=1 ORDER BY id LIMIT 2"
        )
        scan = next(node for node, _, _ in iter_nodes(result.plan) if node.node_type == "Scan")
        self.assertNotIn("employees.manager_id", scan.attributes["columns"])
        self.assertNotIn("employees.dept_id", scan.attributes["columns"])
        self.assertTrue(any(entry["rule"] == "constant_fold" for entry in result.log))
        project = next(
            node
            for node, _, _ in iter_nodes(result.plan)
            if node.node_type == "Project" and any(item["name"] == "salary_alias" for item in node.attributes["items"])
        )
        salary_item = next(item for item in project.attributes["items"] if item["name"] == "salary_alias")
        self.assertEqual(salary_item["expr"]["kind"], "binary")
        self.assertEqual(salary_item["expr"]["right"], {"kind": "literal", "value": 0, "value_type": "number"})

    def test_constant_fold_does_not_apply_unsafe_text_identity(self) -> None:
        folded, changed = fold_expr(
            {"kind": "binary", "op": "+", "left": column("employees.name"), "right": literal(0)}
        )
        self.assertFalse(changed)
        self.assertEqual(folded["kind"], "binary")
        folded_literals, changed_literals = fold_expr(
            {"kind": "binary", "op": "+", "left": literal(2), "right": literal(3)}
        )
        self.assertTrue(changed_literals)
        self.assertEqual(folded_literals["value"], 5)

    def test_having_dependency_is_protected(self) -> None:
        _, result = self.normalize(
            "SELECT d.name AS department, COUNT(*) AS n, SUM(e.salary) AS payroll "
            "FROM employees e JOIN departments d ON e.dept_id=d.id "
            "GROUP BY d.name HAVING SUM(e.salary) > 100000 ORDER BY payroll DESC"
        )
        having_rejections = [
            entry for entry in result.rejections if "HAVING" in entry["reason"]
        ]
        self.assertTrue(having_rejections)
        self.assertTrue(any(entry["conflict_node"] for entry in having_rejections))
        self.assertTrue(all(entry["rule"] == "predicate_pushdown" or entry["rule"] == "projection_pruning" for entry in having_rejections))

    def test_sort_key_dependency_is_protected(self) -> None:
        _, result = self.normalize(
            "SELECT name FROM employees ORDER BY id LIMIT 2"
        )
        sort_rejections = [
            entry for entry in result.rejections if "sort key" in entry["reason"]
        ]
        self.assertTrue(sort_rejections)
        self.assertTrue(all(entry["rule"] == "projection_pruning" for entry in sort_rejections))
        self.assertTrue(all(entry["conflict_node"] for entry in sort_rejections))

    def test_all_fixed_statements_are_semantically_equivalent(self) -> None:
        for entry in load_manifest():
            if entry["expected_code"] != "OK":
                continue
            with self.subTest(statement=entry["id"]):
                initial, result = self.normalize(entry["sql"])
                before = execute_plan(initial, self.sample_db)
                after = execute_plan(result.plan, self.sample_db)
                sqlite_result = execute_sql(entry["sql"], self.sample_db)
                self.assertEqual(before, after)
                self.assertEqual(sqlite_result, after)

    def test_rewrite_budget_is_bounded(self) -> None:
        _, result = self.normalize(
            "SELECT id, name, salary, active, dept_id FROM employees "
            "WHERE active=1 AND salary > 1 AND salary < 999999 ORDER BY salary"
        )
        self.assertLessEqual(len(result.log), 500)
        self.assertGreaterEqual(result.rounds, 1)


if __name__ == "__main__":
    unittest.main()


from __future__ import annotations

import unittest

from sqlplan_engine.ast import BinaryExpr, FunctionCall, InListExpr, JoinRef
from sqlplan_engine.business import ParseError
from sqlplan_engine.parser import parse_sql


class ParserTests(unittest.TestCase):
    def test_full_supported_subset(self) -> None:
        sql = (
            "SELECT d.name AS department, COUNT(*) AS n, SUM(e.salary) AS payroll "
            "FROM departments AS d LEFT JOIN employees AS e ON e.dept_id = d.id "
            "WHERE NOT (e.active = 0 OR e.name IS NULL) "
            "AND e.salary IN (72000, 98000) "
            "GROUP BY d.name HAVING SUM(e.salary) > 1000 "
            "ORDER BY payroll DESC, department ASC NULLS FIRST LIMIT 4"
        )
        statement = parse_sql(sql)
        self.assertEqual(len(statement.items), 3)
        self.assertIsInstance(statement.from_clause, JoinRef)
        self.assertEqual(statement.from_clause.join_type, "LEFT")
        self.assertIsInstance(statement.where, BinaryExpr)
        self.assertEqual(len(statement.group_by), 1)
        self.assertIsNotNone(statement.having)
        self.assertEqual(statement.limit, 4)
        self.assertIsInstance(statement.items[1].expr, FunctionCall)
        self.assertEqual(statement.items[1].alias, "n")
        where_text = repr(statement.where)
        self.assertIn("InListExpr", where_text)

    def test_comma_join_and_in_list(self) -> None:
        statement = parse_sql(
            "SELECT e.name FROM employees e, departments d "
            "WHERE e.dept_id = d.id AND d.region IN ('East', 'West') LIMIT 0"
        )
        self.assertIsInstance(statement.from_clause, JoinRef)
        self.assertEqual(statement.from_clause.join_type, "CROSS")
        self.assertIn("InListExpr", repr(statement.where))
        self.assertEqual(statement.limit, 0)

    def test_parse_error_has_position_and_business_code(self) -> None:
        with self.assertRaises(ParseError) as raised:
            parse_sql("SELECT id FROM WHERE")
        error = raised.exception
        self.assertEqual(error.business_code, "E_PARSE")
        self.assertIn("line", error.context)
        self.assertIn("column", error.context)
        self.assertIn("offset", error.context)
        self.assertGreaterEqual(error.context["line"], 1)
        self.assertGreaterEqual(error.context["column"], 1)

    def test_lexical_error_is_parse_error(self) -> None:
        with self.assertRaises(ParseError) as raised:
            parse_sql("SELECT name FROM employees WHERE name = '@")
        self.assertEqual(raised.exception.business_code, "E_PARSE")
        self.assertIn("line 1", raised.exception.message)
        self.assertIn("column", raised.exception.message)

    def test_empty_statement_is_parse_error(self) -> None:
        with self.assertRaises(ParseError) as raised:
            parse_sql("   ")
        self.assertEqual(raised.exception.context["line"], 1)
        self.assertEqual(raised.exception.context["column"], 1)

    def test_malformed_numeric_literal_is_parse_error(self) -> None:
        with self.assertRaises(ParseError) as raised:
            parse_sql("SELECT name FROM employees WHERE salary > 1e+")
        self.assertEqual(raised.exception.business_code, "E_PARSE")
        self.assertIn("line", raised.exception.context)


if __name__ == "__main__":
    unittest.main()


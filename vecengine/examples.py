from __future__ import annotations

import copy
from typing import Any


def filter_expression() -> dict[str, Any]:
    return {
        "op": "compare",
        "fn": "ge",
        "left": {"op": "column", "name": "id"},
        "right": {"op": "literal", "dtype": "int64", "value": 0},
    }


def projection_plan(dataset: str = "small") -> dict[str, Any]:
    return {
        "version": 1,
        "dataset": dataset,
        "operators": [
            {"op": "filter", "predicate": filter_expression()},
            {
                "op": "project",
                "name": "neg_i",
                "expr": {"op": "unary", "fn": "neg", "arg": {"op": "column", "name": "i64_a"}},
            },
            {
                "op": "project",
                "name": "add_i",
                "expr": {
                    "op": "binary",
                    "fn": "add",
                    "left": {"op": "column", "name": "i64_a"},
                    "right": {"op": "column", "name": "i64_b"},
                },
            },
            {
                "op": "project",
                "name": "mul_i",
                "expr": {
                    "op": "binary",
                    "fn": "mul",
                    "left": {"op": "column", "name": "i64_a"},
                    "right": {"op": "column", "name": "i64_b"},
                },
            },
            {
                "op": "project",
                "name": "div_i",
                "expr": {
                    "op": "binary",
                    "fn": "div",
                    "left": {"op": "column", "name": "i64_a"},
                    "right": {"op": "column", "name": "i64_b"},
                },
            },
            {
                "op": "project",
                "name": "div_f",
                "expr": {
                    "op": "binary",
                    "fn": "div",
                    "left": {"op": "column", "name": "f64_a"},
                    "right": {"op": "column", "name": "f64_b"},
                },
            },
            {
                "op": "project",
                "name": "cmp_f",
                "expr": {
                    "op": "compare",
                    "fn": "gt",
                    "left": {"op": "column", "name": "f64_a"},
                    "right": {"op": "column", "name": "f64_b"},
                },
            },
            {
                "op": "project",
                "name": "logic",
                "expr": {
                    "op": "logical",
                    "fn": "and",
                    "left": {"op": "column", "name": "flag"},
                    "right": {
                        "op": "compare",
                        "fn": "lt",
                        "left": {"op": "column", "name": "i64_a"},
                        "right": {"op": "column", "name": "i64_b"},
                    },
                },
            },
            {
                "op": "project",
                "name": "cond",
                "expr": {
                    "op": "conditional",
                    "condition": {"op": "column", "name": "flag"},
                    "then": {"op": "column", "name": "f64_a"},
                    "else": {"op": "literal", "dtype": "float64", "value": 42.5},
                },
            },
        ],
    }


def aggregate_plan(dataset: str = "small") -> dict[str, Any]:
    return {
        "version": 1,
        "dataset": dataset,
        "operators": [
            {"op": "filter", "predicate": filter_expression()},
            {
                "op": "aggregate",
                "name": "sum_i",
                "fn": "sum",
                "expr": {"op": "column", "name": "i64_a"},
            },
            {
                "op": "aggregate",
                "name": "sum_f",
                "fn": "sum",
                "expr": {
                    "op": "binary",
                    "fn": "div",
                    "left": {"op": "column", "name": "f64_a"},
                    "right": {"op": "column", "name": "f64_b"},
                },
            },
            {
                "op": "aggregate",
                "name": "avg_f",
                "fn": "avg",
                "expr": {"op": "column", "name": "f64_a"},
            },
            {
                "op": "aggregate",
                "name": "min_i",
                "fn": "min",
                "expr": {"op": "column", "name": "i64_a"},
            },
            {
                "op": "aggregate",
                "name": "max_f",
                "fn": "max",
                "expr": {"op": "column", "name": "f64_a"},
            },
            {
                "op": "aggregate",
                "name": "count_flag",
                "fn": "count",
                "expr": {"op": "column", "name": "flag"},
            },
            {"op": "aggregate", "name": "rows", "fn": "count_star"},
        ],
    }


def clone_plan(plan: dict[str, Any]) -> dict[str, Any]:
    return copy.deepcopy(plan)

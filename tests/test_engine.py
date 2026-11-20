from __future__ import annotations

import json
import shutil
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import numpy as np

from vecengine.engine import ExecutionEngine
from vecengine.errors import (
    DuplicateRequestError,
    MemoryBudgetError,
    PlanError,
    SessionAbortedError,
)
from vecengine.evaluator import execute_plan
from vecengine.model import BOOL, FLOAT64, INT64, Column, Dataset, DatasetSpec, FieldSpec
from vecengine.plan import parse_plan
from vecengine.samples import generate_samples
from vecengine.selection import EngineConfig, choose_execution, normalize_execution
from vecengine.evaluator import wrap_int64


FILTER = {
    "op": "compare",
    "fn": "ge",
    "left": {"op": "column", "name": "id"},
    "right": {"op": "literal", "dtype": "int64", "value": 0},
}


PROJECTION_PLAN = {
    "version": 1,
    "dataset": "small",
    "operators": [
        {"op": "filter", "predicate": FILTER},
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


AGGREGATE_PLAN = {
    "version": 1,
    "dataset": "small",
    "operators": [
        {"op": "filter", "predicate": FILTER},
        {"op": "aggregate", "name": "sum_i", "fn": "sum", "expr": {"op": "column", "name": "i64_a"}},
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
        {"op": "aggregate", "name": "avg_f", "fn": "avg", "expr": {"op": "column", "name": "f64_a"}},
        {"op": "aggregate", "name": "min_i", "fn": "min", "expr": {"op": "column", "name": "i64_a"}},
        {"op": "aggregate", "name": "max_f", "fn": "max", "expr": {"op": "column", "name": "f64_a"}},
        {"op": "aggregate", "name": "count_flag", "fn": "count", "expr": {"op": "column", "name": "flag"}},
        {"op": "aggregate", "name": "rows", "fn": "count_star"},
    ],
}


class EngineTestBase(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.root = Path(tempfile.mkdtemp(prefix="vecengine-tests-"))
        cls.data_dir = cls.root / "samples"
        generate_samples(cls.data_dir, sizes={"small": 200}, force=True)

    @classmethod
    def tearDownClass(cls) -> None:
        shutil.rmtree(cls.root, ignore_errors=True)

    def make_engine(self, name: str, *, config: EngineConfig | None = None) -> ExecutionEngine:
        base = self.root / name
        base.mkdir(parents=True, exist_ok=True)
        return ExecutionEngine(
            db_path=base / "engine.db",
            data_dir=self.data_dir,
            results_dir=base / "results",
            config=config or EngineConfig(batch_min=16, batch_max=64),
        )


class PlanValidationTests(EngineTestBase):
    def test_missing_column_is_rejected_before_any_session(self) -> None:
        engine = self.make_engine("invalid_column")
        plan = {
            "dataset": "small",
            "operators": [
                {"op": "project", "name": "x", "expr": {"op": "column", "name": "missing"}}
            ],
        }
        with self.assertRaises(PlanError) as raised:
            engine.submit("bad-column", plan, {})
        self.assertEqual(raised.exception.code, "E_PLAN_INVALID")
        self.assertIsNone(engine.store.find_by_request_id("bad-column"))

    def test_type_mismatch_and_implicit_promotion_are_rejected(self) -> None:
        engine = self.make_engine("invalid_type")
        plan = {
            "dataset": "small",
            "operators": [
                {
                    "op": "project",
                    "name": "x",
                    "expr": {
                        "op": "binary",
                        "fn": "add",
                        "left": {"op": "column", "name": "i64_a"},
                        "right": {"op": "column", "name": "f64_a"},
                    },
                }
            ],
        }
        with self.assertRaises(PlanError) as raised:
            engine.submit("bad-type", plan, {})
        self.assertEqual(raised.exception.code, "E_PLAN_INVALID")
        self.assertIsNone(engine.store.find_by_request_id("bad-type"))


class ExecutionParityTests(EngineTestBase):
    def _run_paths(self, engine: ExecutionEngine, plan: dict, overflow: str) -> tuple[dict, dict]:
        outputs = []
        for path in ("scalar", "vector"):
            result = engine.submit(
                f"parity-{plan['operators'][-1].get('name', plan['operators'][-1]['op'])}-{path}-{overflow}",
                plan,
                {
                    "path": path,
                    "batch_size": 16,
                    "integer_overflow": overflow,
                    "memory_budget_bytes": 64 * 1024 * 1024,
                },
            )
            path_on_disk, _ = engine.get_result_file(result["session_id"])
            with np.load(path_on_disk, allow_pickle=False) as archive:
                metadata = json.loads(str(archive["__meta__"].item()))
                outputs.append(
                    {
                        column["name"]: (
                            np.array(archive[f"{column['name']}__values"], copy=True),
                            np.array(archive[f"{column['name']}__valid"], copy=True),
                        )
                        for column in metadata["columns"]
                    }
                )
        return outputs[0], outputs[1]

    def test_projection_paths_are_exactly_equal(self) -> None:
        engine = self.make_engine("projection_parity")
        for overflow in ("wrap", "saturate"):
            scalar, vector = self._run_paths(engine, PROJECTION_PLAN, overflow)
            self.assertEqual(set(scalar), set(vector))
            for name in scalar:
                np.testing.assert_array_equal(scalar[name][1], vector[name][1], err_msg=name)
                np.testing.assert_array_equal(scalar[name][0], vector[name][0], err_msg=name)

    def test_aggregate_paths_are_exactly_equal(self) -> None:
        engine = self.make_engine("aggregate_parity")
        for overflow in ("wrap", "saturate"):
            scalar, vector = self._run_paths(engine, AGGREGATE_PLAN, overflow)
            for name in scalar:
                np.testing.assert_array_equal(scalar[name][1], vector[name][1], err_msg=name)
                np.testing.assert_array_equal(scalar[name][0], vector[name][0], err_msg=name)
            self.assertEqual(int(scalar["count_flag"][0][0]), 199 - 1)

    def test_overflow_div_zero_and_condition_null_contract(self) -> None:
        engine = self.make_engine("edge_contract")
        wrapped = engine.submit("edge-wrap", PROJECTION_PLAN, {"path": "vector", "integer_overflow": "wrap"})
        saturated = engine.submit(
            "edge-sat", PROJECTION_PLAN, {"path": "vector", "integer_overflow": "saturate"}
        )
        wrapped_npz, _ = engine.get_result_file(wrapped["session_id"])
        saturated_npz, _ = engine.get_result_file(saturated["session_id"])
        with np.load(wrapped_npz, allow_pickle=False) as wrapped_archive, np.load(
            saturated_npz, allow_pickle=False
        ) as saturated_archive:
            self.assertEqual(int(wrapped_archive["add_i__values"][0]), -(1 << 63))
            self.assertEqual(int(saturated_archive["add_i__values"][0]), (1 << 63) - 1)
            self.assertEqual(
                int(wrapped_archive["mul_i__values"][4]),
                wrap_int64(3_037_000_500 * 3_037_000_500),
            )
            self.assertEqual(int(saturated_archive["mul_i__values"][4]), (1 << 63) - 1)
            self.assertFalse(bool(wrapped_archive["div_i__valid"][6]))
            # Row 11 has a null flag; null condition selects ELSE (42.5).
            self.assertTrue(bool(wrapped_archive["cond__valid"][11]))
            self.assertEqual(float(wrapped_archive["cond__values"][11]), 42.5)

    def test_compensated_sum_cancellation(self) -> None:
        spec = DatasetSpec(
            "cancel",
            3,
            (FieldSpec("x", FLOAT64),),
            "unused.npz",
        )
        dataset = Dataset(
            spec,
            {
                "x": Column(
                    "x",
                    FLOAT64,
                    np.array([1e16, 1.0, -1e16], dtype=np.float64),
                    np.ones(3, dtype=np.bool_),
                )
            },
        )
        plan = parse_plan(
            {
                "dataset": "cancel",
                "operators": [
                    {"op": "aggregate", "name": "total", "fn": "sum", "expr": {"op": "column", "name": "x"}}
                ],
            },
            lambda name: spec,
        )
        config = EngineConfig(batch_min=1, batch_max=8)
        for path in ("scalar", "vector"):
            options = normalize_execution(
                {"path": path, "batch_size": 1, "memory_budget_bytes": 16 * 1024 * 1024},
                config,
            )
            selection = choose_execution(plan, options, config)
            result = execute_plan(plan, dataset, options, selection)
            self.assertEqual(float(result.values["total"][0]), 1.0)

    def test_empty_batch_and_all_null_aggregate_contract(self) -> None:
        config = EngineConfig(batch_min=1, batch_max=8)
        cases = [
            (
                DatasetSpec("empty", 0, (FieldSpec("x", FLOAT64),), "unused.npz"),
                np.array([], dtype=np.float64),
                np.array([], dtype=np.bool_),
            ),
            (
                DatasetSpec("all_null", 3, (FieldSpec("x", FLOAT64),), "unused.npz"),
                np.array([1.0, 2.0, 3.0], dtype=np.float64),
                np.zeros(3, dtype=np.bool_),
            ),
        ]
        for spec, values, valid in cases:
            dataset = Dataset(spec, {"x": Column("x", FLOAT64, values, valid)})
            plan = parse_plan(
                {
                    "dataset": spec.name,
                    "operators": [
                        {
                            "op": "aggregate",
                            "name": "total",
                            "fn": "sum",
                            "expr": {"op": "column", "name": "x"},
                        },
                        {
                            "op": "aggregate",
                            "name": "counted",
                            "fn": "count",
                            "expr": {"op": "column", "name": "x"},
                        },
                        {"op": "aggregate", "name": "rows", "fn": "count_star"},
                    ],
                },
                lambda name, spec=spec: spec,
            )
            for path in ("scalar", "vector"):
                options = normalize_execution(
                    {"path": path, "batch_size": 1, "memory_budget_bytes": 16 * 1024 * 1024},
                    config,
                )
                result = execute_plan(plan, dataset, options, choose_execution(plan, options, config))
                self.assertFalse(bool(result.valid["total"][0]))
                self.assertEqual(int(result.values["counted"][0]), 0)
                self.assertEqual(int(result.values["rows"][0]), spec.row_count)


class SchedulingAndRecoveryTests(EngineTestBase):
    def test_auto_path_switches_at_configured_threshold(self) -> None:
        scalar_engine = self.make_engine(
            "auto-scalar", config=EngineConfig(scalar_max_rows=500, batch_min=16, batch_max=64)
        )
        result = scalar_engine.submit("auto-small", PROJECTION_PLAN, {"path": "auto"})
        self.assertEqual(result["path"], "scalar")
        vector_engine = self.make_engine(
            "auto-vector", config=EngineConfig(scalar_max_rows=10, batch_min=16, batch_max=64)
        )
        result = vector_engine.submit("auto-vector", PROJECTION_PLAN, {"path": "auto"})
        self.assertEqual(result["path"], "vector")
        self.assertTrue(result["selection"]["reasons"])

    def test_memory_budget_fails_without_session(self) -> None:
        engine = self.make_engine("memory")
        with self.assertRaises(MemoryBudgetError) as raised:
            engine.submit(
                "too-large",
                PROJECTION_PLAN,
                {"memory_budget_bytes": 128 * 1024, "batch_size": 64},
            )
        self.assertEqual(raised.exception.code, "E_MEMORY_BUDGET")
        self.assertIsNone(engine.store.find_by_request_id("too-large"))

    def test_duplicate_submission_runs_once_and_reuses_result(self) -> None:
        engine = self.make_engine("dedup")

        def submit_one(index: int) -> dict:
            return engine.submit("same-request", PROJECTION_PLAN, {"path": "vector", "batch_size": 16})

        with ThreadPoolExecutor(max_workers=6) as pool:
            results = list(pool.map(submit_one, range(6)))
        session_ids = {result["session_id"] for result in results}
        self.assertEqual(len(session_ids), 1)
        self.assertEqual(sum(1 for result in results if not result["reused"]), 1)
        session_id = session_ids.pop()
        self.assertEqual(len(engine.get_batches(session_id)["batches"]), 13)
        with self.assertRaises(DuplicateRequestError):
            engine.submit(
                "same-request",
                PROJECTION_PLAN,
                {"path": "scalar", "batch_size": 16},
            )

    def test_concurrent_distinct_plans_do_not_cross_contaminate(self) -> None:
        engine = self.make_engine("distinct")
        second_plan = json.loads(json.dumps(PROJECTION_PLAN))
        second_plan["operators"] = second_plan["operators"][:3]

        def submit_pair(pair: tuple[str, dict]) -> dict:
            request_id, plan = pair
            return engine.submit(request_id, plan, {"path": "vector", "batch_size": 16})

        with ThreadPoolExecutor(max_workers=4) as pool:
            results = list(
                pool.map(
                    submit_pair,
                    [("distinct-a", PROJECTION_PLAN), ("distinct-b", second_plan)],
                )
            )
        first, second = sorted(results, key=lambda item: item["session_id"])
        self.assertNotEqual(first["session_id"], second["session_id"])
        for result in results:
            batches = engine.get_batches(result["session_id"])["batches"]
            self.assertTrue(batches)
            self.assertTrue(all(batch["session_id"] == result["session_id"] for batch in batches))
            self.assertEqual(result["completed_batches"], len(batches))

    def test_startup_aborts_incomplete_session_and_reports_persisted_facts(self) -> None:
        engine = self.make_engine("recovery")
        session_id = "interrupted-session"
        submit_intent = engine.store.create_intent(
            session_id=session_id,
            request_id="interrupted-request",
            kind="submit_session",
        )
        summary = {"dataset": "small", "mode": "project", "output_columns": [{"name": "x", "dtype": "int64"}]}
        engine.store.create_session(
            session_id=session_id,
            request_id="interrupted-request",
            payload_hash="deadbeef",
            plan=PROJECTION_PLAN,
            plan_summary=summary,
            execution={"path": "vector", "batch_size": 16},
            dataset="small",
            total_rows=200,
            submit_intent_id=submit_intent,
        )
        run_intent = engine.store.create_intent(
            session_id=session_id, request_id=None, kind="execute_session"
        )
        engine.store.mark_running(
            session_id,
            run_intent,
            {"path": "vector", "batch_size": 16},
        )
        batch_intent = engine.store.create_intent(
            session_id=session_id,
            request_id=None,
            kind="execute_batch",
            batch_index=0,
        )
        engine.store.record_batch_and_checkpoint(
            session_id=session_id,
            stat={
                "batch_index": 0,
                "row_start": 0,
                "row_end": 16,
                "rows_in": 16,
                "rows_out": 15,
                "path": "vector",
                "batch_size": 16,
                "elapsed_ms": 1.0,
                "memory_estimate_bytes": 10_000,
                "expression_nodes": 3,
            },
            processed_rows=16,
            intent_id=batch_intent,
        )
        pending_intent = engine.store.create_intent(
            session_id=session_id,
            request_id=None,
            kind="execute_batch",
            batch_index=1,
        )
        restarted = ExecutionEngine(
            db_path=Path(engine.store.db_path),
            data_dir=self.data_dir,
            results_dir=engine.results_dir,
            config=EngineConfig(batch_min=16, batch_max=64),
        )
        session = restarted.store.get_session(session_id)
        self.assertEqual(session["status"], "aborted")
        report = restarted.recovery_report(session_id)
        self.assertEqual(report["last_completed_batch"], 0)
        self.assertEqual(report["last_persisted_row"], 16)
        self.assertFalse(report["result_available"])
        self.assertEqual(report["pending_intent_count"], 1)
        intents = restarted.store.list_intents(session_id)
        self.assertEqual(next(item for item in intents if item["intent_id"] == pending_intent)["status"], "recovered")
        with self.assertRaises(SessionAbortedError):
            restarted.get_result_page(session_id, 0, 10)

    def test_startup_recovers_stale_intent_without_invalidating_ready_result(self) -> None:
        engine = self.make_engine("stale_intent")
        ready = engine.submit("ready-request", PROJECTION_PLAN, {"path": "vector", "batch_size": 16})
        stale_intent = engine.store.create_intent(
            session_id=ready["session_id"],
            request_id="ready-request",
            kind="duplicate_reuse",
            detail={"simulated": "process died after intent commit"},
        )
        restarted = ExecutionEngine(
            db_path=Path(engine.store.db_path),
            data_dir=self.data_dir,
            results_dir=engine.results_dir,
            config=EngineConfig(batch_min=16, batch_max=64),
        )
        session = restarted.store.get_session(ready["session_id"])
        self.assertEqual(session["status"], "ready")
        stale = next(
            item
            for item in restarted.store.list_intents(ready["session_id"])
            if item["intent_id"] == stale_intent
        )
        self.assertEqual(stale["status"], "recovered")
        page = restarted.get_result_page(ready["session_id"], 0, 1)
        self.assertEqual(page["row_count"], 199)


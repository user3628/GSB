from __future__ import annotations

import argparse
import json
import math
import shutil
import subprocess
import sys
import threading
import time
from pathlib import Path
from typing import Any

import numpy as np
import requests

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from vecengine.api import create_server
from vecengine.engine import ExecutionEngine
from vecengine.examples import aggregate_plan, projection_plan
from vecengine.plan import parse_plan
from vecengine.samples import DEFAULT_SEED, generate_samples, parse_size_overrides
from vecengine.selection import choose_execution, normalize_execution
from vecengine.server import config_from_env


RTOL = 1e-12
ATOL = 1e-12


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run the repeatable three-tier engine experiment")
    parser.add_argument("--data-dir", default="data/samples")
    parser.add_argument("--work-dir", default="artifacts/experiment")
    parser.add_argument("--seed", type=int, default=DEFAULT_SEED)
    parser.add_argument("--rows", action="append", metavar="TIER=ROWS")
    parser.add_argument("--regenerate-samples", action="store_true")
    parser.add_argument("--keep-work-dir", action="store_true")
    parser.add_argument("--skip-recovery", action="store_true")
    args = parser.parse_args(argv)
    root = Path(__file__).resolve().parents[1]
    work_dir = (root / args.work_dir).resolve()
    data_dir = (root / args.data_dir).resolve()
    if work_dir.exists() and not args.keep_work_dir:
        shutil.rmtree(work_dir)
    work_dir.mkdir(parents=True, exist_ok=True)
    downloads = work_dir / "downloads"
    downloads.mkdir(parents=True, exist_ok=True)

    sizes = parse_size_overrides(args.rows)
    manifest = generate_samples(
        data_dir,
        seed=args.seed,
        sizes=sizes,
        force=args.regenerate_samples or bool(args.rows),
    )
    engine = ExecutionEngine(
        db_path=work_dir / "experiment.db",
        data_dir=data_dir,
        results_dir=work_dir / "results",
        config=config_from_env(),
    )
    server = create_server(engine, "127.0.0.1", 0)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    base_url = f"http://127.0.0.1:{server.server_address[1]}"
    report: dict[str, Any] = {
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
        "seed": args.seed,
        "manifest": manifest,
        "tolerance": {"rtol": RTOL, "atol": ATOL, "float_compare": "explicit tolerance for path audit"},
        "datasets": {},
        "recovery_drill": None,
    }
    try:
        print(f"API: {base_url}")
        for tier in sorted(manifest["datasets"]):
            dataset_report = run_dataset(
                engine,
                base_url,
                downloads,
                tier,
                manifest["datasets"][tier]["rows"],
            )
            report["datasets"][tier] = dataset_report
            print_dataset_summary(tier, dataset_report)

        if not args.skip_recovery:
            print("recovery drill: starting forced-termination subprocess...", flush=True)
            recovery_work = work_dir / "recovery-drill"
            recovery_file = recovery_work / "recovery_report.json"
            command = [
                sys.executable,
                str(root / "scripts" / "recovery_drill.py"),
                "--data-dir",
                str(data_dir),
                "--work-dir",
                str(recovery_work),
            ]
            completed = subprocess.run(
                command,
                cwd=root,
                text=True,
                capture_output=True,
                timeout=240,
                check=False,
            )
            print(completed.stdout.strip())
            if completed.stderr.strip():
                print(completed.stderr.strip(), file=sys.stderr)
            if recovery_file.exists():
                report["recovery_drill"] = json.loads(recovery_file.read_text(encoding="utf-8"))
            else:
                report["recovery_drill"] = {
                    "passed": False,
                    "error": "recovery drill did not produce a report",
                    "returncode": completed.returncode,
                }
            if completed.returncode != 0:
                report["recovery_drill"]["passed"] = False
        report_path = work_dir / "experiment_report.json"
        report_path.write_text(
            json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        print(f"report: {report_path}")
        all_consistent = all(
            comparison["consistent"]
            for dataset in report["datasets"].values()
            for comparison in dataset["comparisons"]
        )
        recovery_ok = report["recovery_drill"] is None or report["recovery_drill"].get("passed", False)
        return 0 if all_consistent and recovery_ok else 1
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=10)


def run_dataset(
    engine: ExecutionEngine,
    base_url: str,
    downloads: Path,
    tier: str,
    rows: int,
) -> dict[str, Any]:
    kind_reports = []
    auto_preview: dict[str, Any] = {}
    for kind, plan_factory, overflow in (
        ("projection", projection_plan, "wrap"),
        ("aggregate", aggregate_plan, "saturate"),
    ):
        plan = plan_factory(tier)
        auto_preview[kind] = preview_auto_selection(engine, plan)
        executions: dict[str, Any] = {}
        downloaded: dict[str, Path] = {}
        print(f"{tier}/{kind}: running scalar and vector paths", flush=True)
        for path in ("scalar", "vector"):
            request_id = f"exp-{tier}-{kind}-{path}"
            execution = {
                "path": path,
                "integer_overflow": overflow,
                "memory_budget_bytes": 512 * 1024 * 1024,
            }
            started = time.perf_counter()
            response = requests.post(
                f"{base_url}/sessions",
                json={"request_id": request_id, "plan": plan, "execution": execution},
                timeout=600,
            )
            submit_ms = round((time.perf_counter() - started) * 1000.0, 3)
            payload = response.json()
            if response.status_code not in (200, 201):
                raise RuntimeError(f"{request_id} failed: {json.dumps(payload, ensure_ascii=False)}")
            data = payload["data"]
            download = requests.get(
                f"{base_url}{data['result_download_url']}", timeout=600
            )
            download.raise_for_status()
            local_path = downloads / f"{tier}-{kind}-{path}.npz"
            local_path.write_bytes(download.content)
            downloaded[path] = local_path
            batches = requests.get(
                f"{base_url}/sessions/{data['session_id']}/batches", timeout=60
            ).json()["data"]["batches"]
            executions[path] = {
                "session_id": data["session_id"],
                "request_id": request_id,
                "path": data["path"],
                "batch_size": data["batch_size"],
                "selection": data["selection"],
                "submit_elapsed_ms": submit_ms,
                "batch_count": len(batches),
                "batches_total_elapsed_ms": round(
                    sum(float(batch["elapsed_ms"]) for batch in batches), 3
                ),
                "output_rows": data["output_rows"],
                "output_columns": data["output_columns"],
                "result_sha256": data["result_sha256"],
            }
        comparison = compare_archives(downloaded["scalar"], downloaded["vector"])
        kind_reports.append(
            {
                "kind": kind,
                "integer_overflow_policy": overflow,
                "executions": executions,
                "comparison": comparison,
                "consistent": comparison["consistent"],
            }
        )
    return {
        "rows": rows,
        "auto_selection_preview": auto_preview,
        "comparisons": kind_reports,
    }


def preview_auto_selection(engine: ExecutionEngine, plan: dict[str, Any]) -> dict[str, Any]:
    parsed = parse_plan(plan, engine.registry.get_spec)
    options = normalize_execution({"path": "auto"}, engine.config)
    selection = choose_execution(parsed, options, engine.config)
    return selection.to_dict()


def compare_archives(scalar_path: Path, vector_path: Path) -> dict[str, Any]:
    result: dict[str, Any] = {
        "consistent": True,
        "row_count_equal": False,
        "columns": [],
        "max_absolute_deviation": 0.0,
        "max_relative_deviation": 0.0,
    }
    with np.load(scalar_path, allow_pickle=False) as scalar, np.load(
        vector_path, allow_pickle=False
    ) as vector:
        scalar_meta = json.loads(str(scalar["__meta__"].item()))
        vector_meta = json.loads(str(vector["__meta__"].item()))
        result["row_count_equal"] = scalar_meta["row_count"] == vector_meta["row_count"]
        if not result["row_count_equal"]:
            result["consistent"] = False
        scalar_columns = {column["name"]: column["dtype"] for column in scalar_meta["columns"]}
        vector_columns = {column["name"]: column["dtype"] for column in vector_meta["columns"]}
        if scalar_columns != vector_columns:
            result["consistent"] = False
            result["column_schema_equal"] = False
        else:
            result["column_schema_equal"] = True
        for name, dtype in scalar_columns.items():
            valid_a = np.asarray(scalar[f"{name}__valid"], dtype=np.bool_)
            valid_b = np.asarray(vector[f"{name}__valid"], dtype=np.bool_)
            values_a = np.asarray(scalar[f"{name}__values"])
            values_b = np.asarray(vector[f"{name}__values"])
            mask_equal = bool(np.array_equal(valid_a, valid_b))
            column_report: dict[str, Any] = {
                "name": name,
                "dtype": dtype,
                "null_bitmap_equal": mask_equal,
                "value_equal": False,
                "max_absolute_deviation": 0.0,
                "max_relative_deviation": 0.0,
            }
            if dtype == "float64":
                a = values_a[valid_a]
                b = values_b[valid_b]
                if mask_equal and a.shape == b.shape:
                    equal = _float_arrays_equal(a, b, RTOL, ATOL)
                    absolute = _absolute_deviation(a, b)
                    relative = _relative_deviation(a, b)
                    column_report["value_equal"] = equal
                    column_report["max_absolute_deviation"] = _json_number(absolute)
                    column_report["max_relative_deviation"] = _json_number(relative)
                    result["max_absolute_deviation"] = max(
                        float(result["max_absolute_deviation"]), absolute
                        if math.isfinite(absolute) else math.inf
                    )
                    result["max_relative_deviation"] = max(
                        float(result["max_relative_deviation"]), relative
                        if math.isfinite(relative) else math.inf
                    )
            else:
                column_report["value_equal"] = bool(np.array_equal(values_a, values_b))
            if not mask_equal or not column_report["value_equal"]:
                result["consistent"] = False
            result["columns"].append(column_report)
    result["max_absolute_deviation"] = _json_number(float(result["max_absolute_deviation"]))
    result["max_relative_deviation"] = _json_number(float(result["max_relative_deviation"]))
    return result


def _float_arrays_equal(a: np.ndarray, b: np.ndarray, rtol: float, atol: float) -> bool:
    if a.shape != b.shape:
        return False
    both_nan = np.logical_and(np.isnan(a), np.isnan(b))
    both_inf = np.logical_and(np.isinf(a), np.isinf(b))
    same_inf = np.logical_and(both_inf, np.signbit(a) == np.signbit(b))
    finite = np.logical_and(np.isfinite(a), np.isfinite(b))
    close = np.zeros(a.shape, dtype=np.bool_)
    close[finite] = np.isclose(a[finite], b[finite], rtol=rtol, atol=atol, equal_nan=False)
    return bool(np.all(np.logical_or(close, np.logical_or(both_nan, same_inf))))


def _absolute_deviation(a: np.ndarray, b: np.ndarray) -> float:
    if a.shape != b.shape or a.size == 0:
        return 0.0
    finite = np.logical_and(np.isfinite(a), np.isfinite(b))
    maximum = float(np.max(np.abs(a[finite] - b[finite]))) if np.any(finite) else 0.0
    if np.any(np.logical_xor(np.isinf(a), np.isinf(b))):
        maximum = math.inf
    return maximum


def _relative_deviation(a: np.ndarray, b: np.ndarray) -> float:
    if a.shape != b.shape or a.size == 0:
        return 0.0
    finite = np.logical_and(np.isfinite(a), np.isfinite(b))
    if not np.any(finite):
        return 0.0
    left = a[finite]
    right = b[finite]
    denominator = np.maximum(np.abs(left), np.abs(right))
    delta = np.abs(left - right)
    relative = np.zeros(delta.shape, dtype=np.float64)
    nonzero = denominator != 0
    relative[nonzero] = delta[nonzero] / denominator[nonzero]
    return float(np.max(relative))


def _json_number(value: float) -> float | str:
    if math.isnan(value):
        return "NaN"
    if math.isinf(value):
        return "Infinity" if value > 0 else "-Infinity"
    return value


def print_dataset_summary(tier: str, dataset_report: dict[str, Any]) -> None:
    auto = dataset_report["auto_selection_preview"]
    print(
        f"  auto preview: projection={auto['projection']['path']}/"
        f"{auto['projection']['batch_size']} aggregate={auto['aggregate']['path']}/"
        f"{auto['aggregate']['batch_size']}"
    )
    for comparison in dataset_report["comparisons"]:
        scalar = comparison["executions"]["scalar"]
        vector = comparison["executions"]["vector"]
        audit = comparison["comparison"]
        print(
            f"  {comparison['kind']}: scalar {scalar['submit_elapsed_ms']:.1f}ms "
            f"{scalar['batch_count']} batches; vector {vector['submit_elapsed_ms']:.1f}ms "
            f"{vector['batch_count']} batches; consistent={comparison['consistent']} "
            f"max_abs={audit['max_absolute_deviation']} max_rel={audit['max_relative_deviation']}"
        )


if __name__ == "__main__":
    raise SystemExit(main())

from __future__ import annotations

import argparse
import json
import sys
import tempfile
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from app.engine import RepairEngine  # noqa: E402
from app.store import Store  # noqa: E402


DATA = ROOT / "data"


def load(name: str) -> dict[str, Any]:
    return json.loads((DATA / name).read_text(encoding="utf-8"))


def print_decisions(result: dict[str, Any]) -> None:
    print(
        f"round={result['round_id']} seq={result['round_seq']} "
        f"digest={result['decision_digest']}"
    )
    print(f"{'request':30} {'strategy':13} {'outcome':8} {'latency_us':>10}  error")
    print("-" * 88)
    for decision in result["decisions"]:
        code = decision["error"]["code"] if decision["error"] else "-"
        print(
            f"{decision['request_id'][:30]:30} {decision['strategy']:13} "
            f"{decision['outcome']:8} {decision['estimated_latency_us']:>10}  {code}"
        )
    budget = result["budget"]
    print(
        f"bytes={budget['consumed_bytes']}/{budget['byte_budget']} "
        f"makespan_us={budget['estimated_makespan_us']}/{budget['time_budget_us']} "
        f"tasks={budget['task_count']} shed={len(budget['shed_requests'])}"
    )


def run(db_path: Path) -> dict[str, Any]:
    engine = RepairEngine(Store(db_path))
    startup = engine.recover()
    print("startup reconciliation:", json.dumps(startup["summary"], ensure_ascii=False))
    layout = load("sample_layout.json")
    loaded = engine.load_layout(layout)
    print(f"layout={loaded['layout_version']} objects={loaded['object_count']} matrix={loaded['matrix_id']}")

    print("\n[1] MAIN TRACE · budget shedding / replica fill / unrecoverable")
    main_trace = load("sample_trace.json")
    main_result = engine.submit_trace(main_trace)
    print_decisions(main_result)
    assert main_result["summary"]["success_count"] == 5
    assert main_result["summary"]["error_counts"] == {
        "E_LATENCY_GUARD": 1,
        "E_REBUILD_SHED": 2,
        "E_UNRECOVERABLE": 1,
    }

    print("\n[2] REPLAY · duplicate request IDs are reused, digest must match bit-for-bit")
    replay = engine.submit_trace(main_trace)
    assert replay["decision_digest"] == main_result["decision_digest"]
    assert replay["budget"]["consumed_bytes"] == 0
    print(f"decision digest identical: {replay['decision_digest']}")
    print(f"reused request IDs: {len(replay['reused_request_ids'])}")

    print("\n[3] BOUNDED QUEUE · concurrency=1, queue_capacity=0")
    queue_result = engine.submit_trace(load("queue_trace.json"))
    print_decisions(queue_result)
    assert queue_result["summary"]["error_counts"] == {"E_REBUILD_QUEUE_FULL": 2}

    print("\n[4] RECONSTRUCTION MISMATCH · declared digest is intentionally broken")
    mismatch_result = engine.submit_trace(load("mismatch_trace.json"))
    print_decisions(mismatch_result)
    mismatch_error = mismatch_result["decisions"][0]["error"]
    assert mismatch_error["code"] == "E_RECONSTRUCT_MISMATCH"
    assert mismatch_error["details"]["expected_digest"] != mismatch_error["details"]["actual_digest"]

    return {
        "layout": loaded,
        "main_round": main_result,
        "replay_round": replay,
        "queue_round": queue_result,
        "mismatch_round": mismatch_result,
        "startup_recovery": startup,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Repeatable local engine demonstration")
    parser.add_argument("--db", type=Path, default=None, help="Optional fixed SQLite path; default uses a temporary DB")
    parser.add_argument("--output", type=Path, default=None, help="Write the complete demonstration JSON here")
    args = parser.parse_args()
    if args.db is None:
        with tempfile.TemporaryDirectory(prefix="read-repair-demo-") as temp_dir:
            report = run(Path(temp_dir) / "demo.db")
    else:
        args.db.parent.mkdir(parents=True, exist_ok=True)
        report = run(args.db)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(report, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
        print(f"\nwrote demonstration evidence to {args.output}")
    print("\nDEMO PASSED")


if __name__ == "__main__":
    main()

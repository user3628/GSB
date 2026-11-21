from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from .storage import Storage


def _check(
    checks: list[dict[str, Any]], name: str, ok: bool, detail: Any | None = None
) -> None:
    checks.append({"name": name, "ok": bool(ok), "detail": detail})


def validate_round(
    db_path: str | Path, round_id: str, retry_backoff_base_ms: int = 200
) -> dict[str, Any]:
    storage = Storage(db_path)
    storage.initialize()
    row = storage.get_round(round_id)
    checks: list[dict[str, Any]] = []
    if row is None:
        return {
            "ok": False,
            "round_id": round_id,
            "checks": [{"name": "round_exists", "ok": False, "detail": None}],
            "summary": {"failed_checks": 1},
        }

    _check(checks, "round_exists", True)
    attempts = storage.get_attempts_for_round(round_id)
    config = (row.get("requested_config") or {}) | (row.get("effective_config") or {})
    max_concurrency = int(
        config.get("effective_max_concurrency", config.get("max_concurrency", 0))
    )
    retry_backoff_base_ms = int(
        config.get("retry_backoff_base_ms", retry_backoff_base_ms)
    )
    result = row.get("result") or {}
    has_normal_dispatched_list = "dispatched" in result
    result_dispatched = result.get("dispatched") or []

    _check(
        checks,
        "dispatch_count_within_concurrency",
        len(attempts) <= max_concurrency,
        {"attempts": len(attempts), "max_concurrency": max_concurrency},
    )
    attempt_task_ids = [attempt["task_id"] for attempt in attempts]
    _check(
        checks,
        "no_duplicate_task_in_round",
        len(attempt_task_ids) == len(set(attempt_task_ids)),
        {"task_ids": attempt_task_ids},
    )
    if has_normal_dispatched_list:
        expected_result_count = len(result_dispatched)
        result_attempt_ids = {item.get("attempt_id") for item in result_dispatched}
    elif row["status"] == "INTERRUPTED":
        expected_result_count = len(attempts)
        result_attempt_ids = {attempt["attempt_id"] for attempt in attempts}
    else:
        expected_result_count = 0
        result_attempt_ids = set()
    _check(
        checks,
        "result_dispatch_records_match",
        expected_result_count == len(attempts),
        {"result_dispatched": expected_result_count, "attempts": len(attempts)},
    )
    _check(
        checks,
        "result_attempt_ids_match_records",
        result_attempt_ids == {attempt["attempt_id"] for attempt in attempts},
        {
            "result_attempt_ids": sorted(x for x in result_attempt_ids if x),
            "record_attempt_ids": sorted(attempt["attempt_id"] for attempt in attempts),
        },
    )

    for attempt in attempts:
        attempt_id = attempt["attempt_id"]
        outcome = attempt["outcome"]
        transitions = storage.get_transitions_for_attempt(attempt_id)
        to_dispatching = [
            tr for tr in transitions if tr["to_state"] == "DISPATCHED"
        ]
        _check(
            checks,
            f"attempt:{attempt_id}:dispatch_transition_persisted",
            len(to_dispatching) == 1,
            {"count": len(to_dispatching)},
        )
        if to_dispatching:
            _check(
                checks,
                f"attempt:{attempt_id}:dispatch_transition_not_after_record_start",
                to_dispatching[0]["occurred_at_ms"] <= attempt["started_at_ms"],
                {
                    "transition_at_ms": to_dispatching[0]["occurred_at_ms"],
                    "attempt_started_at_ms": attempt["started_at_ms"],
                },
            )

        if outcome == "SUCCEEDED":
            _check(
                checks,
                f"attempt:{attempt_id}:http_success_status",
                attempt["http_status"] is not None
                and 200 <= int(attempt["http_status"]) < 300,
                {"http_status": attempt["http_status"]},
            )
            terminal = [tr for tr in transitions if tr["to_state"] == "COMPLETED"]
            _check(
                checks,
                f"attempt:{attempt_id}:completed_transition",
                len(terminal) == 1,
                {"count": len(terminal)},
            )
        elif outcome in {"FAILED", "TIMEOUT"}:
            terminal = [
                tr
                for tr in transitions
                if tr["to_state"] in {"RETRY_WAITING", "DEAD_LETTERED"}
            ]
            _check(
                checks,
                f"attempt:{attempt_id}:failure_terminal_transition",
                len(terminal) == 1,
                {"count": len(terminal)},
            )
            if outcome == "TIMEOUT":
                _check(
                    checks,
                    f"attempt:{attempt_id}:timeout_business_code",
                    attempt["error_code"] == "E_DISPATCH_TIMEOUT",
                    {"error_code": attempt["error_code"]},
                )
            if terminal and terminal[0]["to_state"] == "RETRY_WAITING":
                expected_backoff = retry_backoff_base_ms * (
                    2 ** max(0, int(attempt["attempt_no"]) - 1)
                )
                meta = terminal[0]["metadata"]
                _check(
                    checks,
                    f"attempt:{attempt_id}:deterministic_backoff",
                    meta.get("backoff_ms") == expected_backoff,
                    {
                        "actual": meta.get("backoff_ms"),
                        "expected": expected_backoff,
                    },
                )
                _check(
                    checks,
                    f"attempt:{attempt_id}:retry_scheduled_after_result",
                    meta.get("next_attempt_at_ms", 0) >= attempt["finished_at_ms"],
                    {
                        "next_attempt_at_ms": meta.get("next_attempt_at_ms"),
                        "finished_at_ms": attempt["finished_at_ms"],
                    },
                )
        elif outcome == "INTERRUPTED":
            _check(
                checks,
                f"attempt:{attempt_id}:interrupted_has_no_claimed_wire_result",
                attempt["http_status"] is None and attempt["duration_ms"] is None,
                {
                    "http_status": attempt["http_status"],
                    "duration_ms": attempt["duration_ms"],
                },
            )
            _check(
                checks,
                f"attempt:{attempt_id}:interrupted_business_code",
                attempt["error_code"] == "E_PROCESS_INTERRUPTED",
                {"error_code": attempt["error_code"]},
            )
        elif outcome == "ABORTED":
            _check(
                checks,
                f"attempt:{attempt_id}:aborted_before_wire",
                attempt["http_status"] is None and attempt["duration_ms"] is None,
                None,
            )
        else:
            _check(
                checks,
                f"attempt:{attempt_id}:outcome_is_terminal",
                outcome != "IN_FLIGHT",
                {"outcome": outcome, "round_status": row["status"]},
            )

        intent = storage.get_intent(attempt["intent_id"])
        _check(
            checks,
            f"attempt:{attempt_id}:intent_exists",
            intent is not None,
            {"intent_id": attempt["intent_id"]},
        )
        if intent is not None:
            _check(
                checks,
                f"attempt:{attempt_id}:intent_closed",
                intent["state"] == "CLOSED",
                {"state": intent["state"], "resolution": intent["resolution"]},
            )

    skipped = result.get("skipped") or []
    skipped_keys = [
        (entry.get("priority", -1), entry.get("due_at_ms", -1), entry.get("task_id", ""))
        for entry in skipped
    ]
    _check(
        checks,
        "skipped_order_is_deterministic",
        skipped_keys == sorted(skipped_keys),
        {"skipped": skipped_keys},
    )
    overlap = set(attempt_task_ids) & {
        entry.get("task_id") for entry in skipped if entry.get("task_id")
    }
    _check(
        checks,
        "no_task_both_dispatched_and_skipped",
        not overlap,
        {"overlap": sorted(overlap)},
    )

    consistency = storage.verify_consistency()
    _check(
        checks,
        "persisted_state_consistency",
        consistency["ok"],
        consistency["violations"],
    )
    failed = [check for check in checks if not check["ok"]]
    return {
        "ok": not failed,
        "round_id": round_id,
        "queue": row["queue_name"],
        "round_status": row["status"],
        "checks": checks,
        "summary": {
            "total_checks": len(checks),
            "passed_checks": len(checks) - len(failed),
            "failed_checks": len(failed),
            "attempts": len(attempts),
            "skipped": len(skipped),
        },
    }


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Verify persisted dispatch/retry facts for one round"
    )
    parser.add_argument("--db", required=True, help="SQLite database path")
    parser.add_argument("--round-id", "--round", dest="round_id", required=True)
    parser.add_argument("--retry-base-ms", type=int, default=200)
    parser.add_argument("--pretty", action="store_true")
    args = parser.parse_args()
    report = validate_round(args.db, args.round_id, args.retry_base_ms)
    print(
        json.dumps(
            report, ensure_ascii=False, indent=2 if args.pretty else None, sort_keys=True
        )
    )
    raise SystemExit(0 if report["ok"] else 1)


if __name__ == "__main__":
    main()


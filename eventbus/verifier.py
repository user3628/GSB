from __future__ import annotations

import math
from pathlib import Path
from typing import Any

from .aggregation import NeumaierSum
from .service import EVENT_STATES, LEGAL_TRANSITIONS, utc_now
from .storage import Database


class StateVerifier:
    def __init__(self, db_path: str | Path):
        self.db = Database(db_path)

    def run(self, *, topic_id: str | None = None) -> dict[str, Any]:
        checks: list[dict[str, Any]] = []
        with self.db.read_transaction() as connection:
            topics = connection.execute(
                "SELECT * FROM topics ORDER BY topic_id"
            ).fetchall()
            if topic_id is not None:
                topics = [row for row in topics if row["topic_id"] == topic_id]
                if not topics:
                    checks.append(
                        self._check(
                            "topic_exists",
                            False,
                            {"topic": topic_id, "reason": "not_found"},
                        )
                    )

            for topic in topics:
                name = topic["topic_id"]
                sequence_details = self._sequence_check(connection, topic)
                checks.append(
                    self._check(
                        "production_sequence_contiguous",
                        sequence_details["ok"],
                        {"topic": name, **sequence_details},
                    )
                )
                trace_details = self._trace_check(connection, topic)
                checks.append(
                    self._check(
                        "state_transitions_legal",
                        trace_details["ok"],
                        {"topic": name, **trace_details},
                    )
                )
                aggregate_details = self._aggregate_check(connection, topic)
                checks.append(
                    self._check(
                        "confirmation_aggregate_consistent",
                        aggregate_details["ok"],
                        {"topic": name, **aggregate_details},
                    )
                )
                offset_details = self._offset_check(connection, topic)
                checks.append(
                    self._check(
                        "offset_history_and_boundary_consistent",
                        offset_details["ok"],
                        {"topic": name, **offset_details},
                    )
                )
                lease_details = self._lease_check(connection, topic)
                checks.append(
                    self._check(
                        "lease_invariants",
                        lease_details["ok"],
                        {"topic": name, **lease_details},
                    )
                )

            pending_count = int(
                connection.execute(
                    "SELECT COUNT(*) FROM intents WHERE status = 'PENDING'"
                ).fetchone()[0]
            )
            checks.append(
                self._check(
                    "no_pending_intents",
                    pending_count == 0,
                    {"pending_intents": pending_count},
                )
            )

            invalid_states = [
                dict(row)
                for row in connection.execute(
                    "SELECT event_id, state FROM events "
                    f"WHERE state NOT IN ({','.join('?' for _ in EVENT_STATES)})",
                    EVENT_STATES,
                ).fetchall()
            ]
            checks.append(
                self._check(
                    "event_states_declared",
                    not invalid_states,
                    {"invalid": invalid_states},
                )
            )

        failures = [
            {"name": item["name"], "details": item["details"]}
            for item in checks
            if not item["ok"]
        ]
        return {
            "ok": not failures,
            "generated_at": utc_now(),
            "source": "sqlite_read_snapshot",
            "topic_filter": topic_id,
            "summary": {
                "checks": len(checks),
                "passed": len(checks) - len(failures),
                "failed": len(failures),
                "failure_names": [failure["name"] for failure in failures],
            },
            "checks": checks,
        }

    @staticmethod
    def _check(name: str, ok: bool, details: dict[str, Any]) -> dict[str, Any]:
        return {"name": name, "ok": bool(ok), "details": details}

    def _sequence_check(self, connection, topic) -> dict[str, Any]:
        problems: list[dict[str, Any]] = []
        partitions = connection.execute(
            "SELECT * FROM partitions WHERE topic_id = ? ORDER BY partition",
            (topic["topic_id"],),
        ).fetchall()
        for partition in partitions:
            rows = connection.execute(
                "SELECT production_seq FROM events WHERE topic_id = ? AND partition = ? "
                "ORDER BY production_seq",
                (topic["topic_id"], partition["partition"]),
            ).fetchall()
            sequences = [int(row["production_seq"]) for row in rows]
            expected = int(partition["min_retained_seq"])
            if not sequences and int(partition["next_seq"]) != expected:
                problems.append(
                    {
                        "partition": int(partition["partition"]),
                        "next_seq": int(partition["next_seq"]),
                        "expected_next_seq": expected,
                    }
                )
            for actual in sequences:
                if actual != expected:
                    problems.append(
                        {
                            "partition": int(partition["partition"]),
                            "expected_seq": expected,
                            "actual_seq": actual,
                        }
                    )
                    break
                expected += 1
            if sequences and int(partition["next_seq"]) != sequences[-1] + 1:
                problems.append(
                    {
                        "partition": int(partition["partition"]),
                        "next_seq": int(partition["next_seq"]),
                        "expected_next_seq": sequences[-1] + 1,
                    }
                )
        return {"ok": not problems, "problems": problems}

    def _trace_check(self, connection, topic) -> dict[str, Any]:
        problems: list[dict[str, Any]] = []
        events = connection.execute(
            "SELECT event_id, state, partition, production_seq FROM events "
            "WHERE topic_id = ? ORDER BY partition, production_seq",
            (topic["topic_id"],),
        ).fetchall()
        for event in events:
            traces = connection.execute(
                "SELECT from_state, to_state, reason FROM event_state_trace "
                "WHERE event_id = ? ORDER BY id",
                (event["event_id"],),
            ).fetchall()
            if not traces:
                problems.append(
                    {"event_id": event["event_id"], "reason": "missing_trace"}
                )
                continue
            if traces[0]["from_state"] is not None or traces[0]["to_state"] != "ACCEPTED":
                problems.append(
                    {"event_id": event["event_id"], "reason": "invalid_initial_trace"}
                )
                continue
            previous = None
            legal = True
            for index, trace in enumerate(traces):
                if trace["from_state"] != previous:
                    problems.append(
                        {
                            "event_id": event["event_id"],
                            "trace_index": index,
                            "reason": "broken_trace_chain",
                            "expected_from": previous,
                            "actual_from": trace["from_state"],
                        }
                    )
                    legal = False
                    break
                if trace["to_state"] not in LEGAL_TRANSITIONS.get(previous, frozenset()):
                    problems.append(
                        {
                            "event_id": event["event_id"],
                            "trace_index": index,
                            "reason": "illegal_transition",
                            "from_state": previous,
                            "to_state": trace["to_state"],
                            "transition_reason": trace["reason"],
                        }
                    )
                    legal = False
                    break
                previous = trace["to_state"]
            if legal and previous != event["state"]:
                problems.append(
                    {
                        "event_id": event["event_id"],
                        "reason": "final_state_mismatch",
                        "trace_state": previous,
                        "event_state": event["state"],
                    }
                )
        return {"ok": not problems, "problems": problems[:100]}

    def _offset_check(self, connection, topic) -> dict[str, Any]:
        problems: list[dict[str, Any]] = []
        partition_rows = connection.execute(
            "SELECT * FROM partitions WHERE topic_id = ? ORDER BY partition",
            (topic["topic_id"],),
        ).fetchall()
        for partition in partition_rows:
            partition_id = int(partition["partition"])
            offset = connection.execute(
                "SELECT * FROM consumer_offsets WHERE topic_id = ? AND group_id = ? "
                "AND partition = ?",
                (topic["topic_id"], topic["group_id"], partition_id),
            ).fetchone()
            if offset is None:
                problems.append(
                    {"partition": partition_id, "reason": "missing_offset"}
                )
                continue
            history = connection.execute(
                "SELECT * FROM offset_history WHERE topic_id = ? AND group_id = ? "
                "AND partition = ? ORDER BY id",
                (topic["topic_id"], topic["group_id"], partition_id),
            ).fetchall()
            if not history:
                problems.append(
                    {"partition": partition_id, "reason": "missing_offset_history"}
                )
            else:
                for index, item in enumerate(history):
                    old = int(item["old_seq"])
                    new = int(item["new_seq"])
                    kind = item["kind"]
                    if kind in {"ADVANCE", "INITIAL"} and new < old:
                        problems.append(
                            {
                                "partition": partition_id,
                                "offset_history_id": int(item["id"]),
                                "reason": "advance_went_backwards",
                                "old": old,
                                "new": new,
                            }
                        )
                    if kind in {"ROLLBACK", "RECOVERY_ROLLBACK"} and new > old:
                        problems.append(
                            {
                                "partition": partition_id,
                                "offset_history_id": int(item["id"]),
                                "reason": "rollback_went_forward",
                                "old": old,
                                "new": new,
                            }
                        )
                    if index and int(item["old_seq"]) != int(history[index - 1]["new_seq"]):
                        problems.append(
                            {
                                "partition": partition_id,
                                "offset_history_id": int(item["id"]),
                                "reason": "history_chain_broken",
                                "expected_old": int(history[index - 1]["new_seq"]),
                                "actual_old": int(item["old_seq"]),
                            }
                        )
                if int(history[-1]["new_seq"]) != int(offset["next_seq"]):
                    problems.append(
                        {
                            "partition": partition_id,
                            "reason": "offset_history_tail_mismatch",
                            "history_tail": int(history[-1]["new_seq"]),
                            "offset": int(offset["next_seq"]),
                        }
                    )
            maximum = connection.execute(
                "SELECT MAX(production_seq) FROM events WHERE topic_id = ? AND partition = ?",
                (topic["topic_id"], partition_id),
            ).fetchone()[0]
            offset_value = int(offset["next_seq"])
            if maximum is None:
                if offset_value != int(partition["min_retained_seq"]):
                    problems.append(
                        {
                            "partition": partition_id,
                            "reason": "empty_partition_offset",
                            "offset": offset_value,
                        }
                    )
            elif not (
                int(partition["min_retained_seq"])
                <= offset_value
                <= int(maximum) + 1
            ):
                problems.append(
                    {
                        "partition": partition_id,
                        "reason": "offset_out_of_range",
                        "offset": offset_value,
                        "max_seq": int(maximum),
                    }
                )
            if bool(offset["replaying"]):
                target = offset["replay_target"]
                end = offset["replay_end"]
                if target is None or end is None or not (
                    int(target) <= offset_value < int(end)
                ):
                    problems.append(
                        {
                            "partition": partition_id,
                            "reason": "replay_window_invalid",
                            "offset": offset_value,
                            "target": target,
                            "end": end,
                        }
                    )

            active_confirmation_seqs = [
                int(row["production_seq"])
                for row in connection.execute(
                    "SELECT production_seq FROM confirmations WHERE topic_id = ? "
                    "AND group_id = ? AND partition = ? AND status = 'ACTIVE'",
                    (topic["topic_id"], topic["group_id"], partition_id),
                ).fetchall()
            ]
            if any(seq >= offset_value for seq in active_confirmation_seqs):
                problems.append(
                    {
                        "partition": partition_id,
                        "reason": "confirmation_at_or_after_offset",
                        "offset": offset_value,
                        "confirmation_seqs": active_confirmation_seqs,
                    }
                )

            events_below = connection.execute(
                "SELECT * FROM events WHERE topic_id = ? AND partition = ? "
                "AND production_seq < ? ORDER BY production_seq",
                (topic["topic_id"], partition_id, offset_value),
            ).fetchall()
            for event in events_below:
                valid = event["state"] == "DEAD_LETTERED" or (
                    event["state"] == "ACKED"
                    and connection.execute(
                        "SELECT 1 FROM confirmations WHERE event_id = ? AND status = 'ACTIVE'",
                        (event["event_id"],),
                    ).fetchone()
                    is not None
                )
                if not valid:
                    problems.append(
                        {
                            "partition": partition_id,
                            "reason": "offset_skips_uncommitted_event",
                            "event_id": event["event_id"],
                            "production_seq": int(event["production_seq"]),
                            "state": event["state"],
                        }
                    )
                    break
        return {"ok": not problems, "problems": problems[:100]}

    def _aggregate_check(self, connection, topic) -> dict[str, Any]:
        problems: list[dict[str, Any]] = []
        confirmations = connection.execute(
            "SELECT c.event_id, c.production_seq, c.status, e.event_key, e.measure, "
            "e.partition, e.state "
            "FROM confirmations c JOIN events e ON e.event_id = c.event_id "
            "WHERE c.topic_id = ? AND c.group_id = ? ORDER BY c.id",
            (topic["topic_id"], topic["group_id"]),
        ).fetchall()
        for confirmation in confirmations:
            if confirmation["status"] != "ACTIVE":
                continue
            if confirmation["state"] != "ACKED":
                problems.append(
                    {
                        "event_id": confirmation["event_id"],
                        "reason": "active_confirmation_event_not_acked",
                        "state": confirmation["state"],
                    }
                )

        acked = connection.execute(
            "SELECT event_id, production_seq FROM events WHERE topic_id = ? AND state = 'ACKED' "
            "ORDER BY partition, production_seq",
            (topic["topic_id"],),
        ).fetchall()
        for event in acked:
            active = connection.execute(
                "SELECT 1 FROM confirmations WHERE event_id = ? AND status = 'ACTIVE'",
                (event["event_id"],),
            ).fetchone()
            if active is None:
                problems.append(
                    {
                        "event_id": event["event_id"],
                        "reason": "acked_without_active_confirmation",
                    }
                )

        canonical_rows = connection.execute(
            "SELECT e.event_key, e.measure, e.partition, e.production_seq "
            "FROM confirmations c JOIN events e ON e.event_id = c.event_id "
            "WHERE c.topic_id = ? AND c.group_id = ? AND c.status = 'ACTIVE' "
            "ORDER BY e.event_key, e.partition, e.production_seq",
            (topic["topic_id"], topic["group_id"]),
        ).fetchall()
        rebuilt: dict[str, dict[str, float | int]] = {}
        for row in canonical_rows:
            key = row["event_key"]
            if key not in rebuilt:
                rebuilt[key] = {"count": 0, "acc": NeumaierSum()}
            rebuilt[key]["count"] = int(rebuilt[key]["count"]) + 1
            acc = rebuilt[key]["acc"]
            assert isinstance(acc, NeumaierSum)
            acc.add(row["measure"])

        stored_rows = connection.execute(
            "SELECT * FROM aggregate_state WHERE topic_id = ? AND group_id = ? "
            "ORDER BY event_key",
            (topic["topic_id"], topic["group_id"]),
        ).fetchall()
        stored = {row["event_key"]: row for row in stored_rows}
        if set(stored) != set(rebuilt):
            problems.append(
                {
                    "reason": "aggregate_key_set_mismatch",
                    "stored_keys": sorted(stored),
                    "expected_keys": sorted(rebuilt),
                }
            )
        for key in sorted(set(stored) & set(rebuilt)):
            row = stored[key]
            expected = rebuilt[key]
            acc = expected["acc"]
            assert isinstance(acc, NeumaierSum)
            count = int(expected["count"])
            if int(row["event_count"]) != count:
                problems.append(
                    {
                        "key": key,
                        "reason": "aggregate_count_mismatch",
                        "stored": int(row["event_count"]),
                        "expected": count,
                    }
                )
            if not self._float_equal(float(row["measure_sum"]), acc.total):
                problems.append(
                    {
                        "key": key,
                        "reason": "aggregate_sum_mismatch",
                        "stored": float(row["measure_sum"]),
                        "expected": acc.total,
                    }
                )
            if not self._float_equal(float(row["compensation"]), acc.compensation):
                problems.append(
                    {
                        "key": key,
                        "reason": "aggregate_compensation_mismatch",
                        "stored": float(row["compensation"]),
                        "expected": acc.compensation,
                    }
                )
        if [row["event_key"] for row in stored_rows] != sorted(stored):
            problems.append({"reason": "aggregate_output_not_stably_sorted"})
        return {
            "ok": not problems,
            "active_confirmations": sum(
                1 for item in confirmations if item["status"] == "ACTIVE"
            ),
            "aggregate_keys": len(rebuilt),
            "problems": problems[:100],
        }

    def _lease_check(self, connection, topic) -> dict[str, Any]:
        problems: list[dict[str, Any]] = []
        rows = connection.execute(
            "SELECT d.*, e.state AS event_state, e.production_seq, o.next_seq AS offset "
            "FROM deliveries d JOIN events e ON e.event_id = d.event_id "
            "JOIN consumer_offsets o ON o.topic_id = d.topic_id "
            "AND o.group_id = d.group_id AND o.partition = d.partition "
            "WHERE d.topic_id = ? AND d.status = 'ACTIVE'",
            (topic["topic_id"],),
        ).fetchall()
        seen: set[int] = set()
        for row in rows:
            partition = int(row["partition"])
            if partition in seen:
                problems.append(
                    {"partition": partition, "reason": "multiple_active_leases"}
                )
            seen.add(partition)
            if row["event_state"] != "DISPATCHED":
                problems.append(
                    {
                        "delivery_id": int(row["id"]),
                        "reason": "active_lease_event_not_dispatched",
                        "state": row["event_state"],
                    }
                )
            if int(row["production_seq"]) < int(row["offset"]):
                problems.append(
                    {
                        "delivery_id": int(row["id"]),
                        "reason": "active_lease_before_offset",
                    }
                )
        return {"ok": not problems, "problems": problems}

    @staticmethod
    def _float_equal(left: float, right: float) -> bool:
        if math.isnan(left) or math.isnan(right):
            return False
        return math.isclose(left, right, rel_tol=1e-14, abs_tol=1e-15)

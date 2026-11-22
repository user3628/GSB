"""Parallel, budgeted dispatcher with real HTTP call timeouts."""

from __future__ import annotations

import concurrent.futures
import threading
import time
from collections import defaultdict
from typing import Any

import requests

from .constants import (
    DEAD_LETTERED,
    DELIVERY_ERROR,
    DISPATCHED,
    DELIVERY_MODES,
    DELIVERY_REFUSED,
    DELIVERY_TIMED_OUT,
    PENDING,
    REDELIVERED,
)
from .errors import (
    E_INVALID_ARGUMENT,
    E_NOT_FOUND,
    E_QUEUE_FULL,
    E_ROUND_IN_PROGRESS,
    MWError,
)
from .store import SQLiteStore
from .utils import dumps, loads, monotonic_ms, new_id, now_ms, require_int

E_WAIT_QUEUE_FULL = "E_WAIT_QUEUE_FULL"
E_TIME_BUDGET = "E_TIME_BUDGET_EXHAUSTED"
E_PER_QUEUE_LIMIT = "E_PER_QUEUE_INFLIGHT_LIMIT"
E_NO_CONSUMER = "E_NO_CONSUMER"


class Scheduler:
    def __init__(self, store: SQLiteStore) -> None:
        self.store = store
        self._round_guard = threading.Lock()

    def run_round(
        self,
        mode: str,
        config: dict[str, Any] | None,
        *,
        request_id: str,
        ack_base_url: str,
        consumer_id: str | None = None,
    ) -> dict[str, Any]:
        if not self._round_guard.acquire(blocking=False):
            raise MWError(
                E_ROUND_IN_PROGRESS,
                "another round is currently executing; use a distinct process for true parallel rounds",
                http_status=409,
            )
        try:
            return self._run_round_locked(
                mode,
                config or {},
                request_id=request_id,
                ack_base_url=ack_base_url,
                consumer_id=consumer_id,
            )
        finally:
            self._round_guard.release()

    @staticmethod
    def _validated_config(config: dict[str, Any]) -> dict[str, Any]:
        global_concurrency = require_int(
            config.get("global_concurrency", 4), "global_concurrency", minimum=1
        )
        per_queue_inflight = require_int(
            config.get("per_queue_inflight", 1), "per_queue_inflight", minimum=1
        )
        time_budget_ms = require_int(
            config.get("time_budget_ms", 2_000), "time_budget_ms", minimum=1
        )
        dispatch_timeout_ms = require_int(
            config.get("dispatch_timeout_ms", 1_000), "dispatch_timeout_ms", minimum=1
        )
        wait_queue_capacity = require_int(
            config.get("wait_queue_capacity", 256), "wait_queue_capacity", minimum=0
        )
        max_scan_messages = require_int(
            config.get("max_scan_messages", 5_000), "max_scan_messages", minimum=1
        )
        strict_wait_queue = bool(config.get("strict_wait_queue", False))
        retry_delay_ms = require_int(
            config.get("retry_delay_ms", 0), "retry_delay_ms", minimum=0
        )
        return {
            "global_concurrency": min(global_concurrency, 256),
            "per_queue_inflight": min(per_queue_inflight, 256),
            "time_budget_ms": time_budget_ms,
            "dispatch_timeout_ms": dispatch_timeout_ms,
            "wait_queue_capacity": min(wait_queue_capacity, 100_000),
            "max_scan_messages": min(max_scan_messages, 100_000),
            "strict_wait_queue": strict_wait_queue,
            "retry_delay_ms": retry_delay_ms,
        }

    def _plan(
        self,
        *,
        consumer_id: str | None,
        admission_limit: int,
        max_scan_messages: int,
    ) -> dict[str, Any]:
        with self.store.db.read_tx() as conn:
            queues = [dict(r) for r in conn.execute("SELECT * FROM queues").fetchall()]
            enabled_for_queue: dict[str, set[str]] = defaultdict(set)
            for row in conn.execute(
                """
                SELECT cq.queue_name, c.consumer_id
                  FROM consumer_queues cq JOIN consumers c ON c.consumer_id = cq.consumer_id
                 WHERE c.enabled = 1
                """
            ).fetchall():
                enabled_for_queue[row["queue_name"]].add(row["consumer_id"])
            active_counts = {
                row["queue_name"]: int(row["n"])
                for row in conn.execute(
                    "SELECT queue_name, COUNT(*) AS n FROM deliveries WHERE state = 'DISPATCHED' GROUP BY queue_name"
                ).fetchall()
            }
            eligible_counts = {
                row["queue_name"]: int(row["n"])
                for row in conn.execute(
                    """
                    SELECT queue_name, COUNT(*) AS n FROM messages
                     WHERE status IN (?, ?) AND available_at_ms <= ?
                       AND active_delivery_id IS NULL
                     GROUP BY queue_name
                    """,
                    (PENDING, REDELIVERED, now_ms()),
                ).fetchall()
            }
            per_queue: dict[str, list[dict[str, Any]]] = {}
            total_scanned = 0
            scan_cap_hit = False
            for queue in queues:
                qname = queue["queue_name"]
                if consumer_id:
                    if consumer_id not in enabled_for_queue.get(qname, set()):
                        per_queue[qname] = []
                        continue
                elif not enabled_for_queue.get(qname):
                    per_queue[qname] = []
                    continue
                remaining = max_scan_messages - total_scanned
                if remaining <= 0:
                    per_queue[qname] = []
                    scan_cap_hit = True
                    continue
                rows = conn.execute(
                    """
                    SELECT * FROM messages
                     WHERE queue_name = ? AND status IN (?, ?)
                       AND available_at_ms <= ? AND active_delivery_id IS NULL
                     ORDER BY seq ASC LIMIT ?
                    """,
                    (qname, PENDING, REDELIVERED, now_ms(), remaining),
                ).fetchall()
                total_scanned += len(rows)
                per_queue[qname] = [
                    {
                        "queue_name": qname,
                        "message_id": row["message_id"],
                        "seq": int(row["seq"]),
                        "priority": int(queue["priority"]),
                        "queue_rotation": int(queue["rotation"]),
                        "body": loads(row["body_json"], None),
                        "body_digest": row["body_digest"],
                        "visibility_timeout_ms": int(row["visibility_timeout_ms"]),
                        "max_retries": int(row["max_retries"]),
                        "attempt_count": int(row["attempt_count"]),
                    }
                    for row in rows
                ]
        queues_by_name = {q["queue_name"]: q for q in queues}
        priority_groups: dict[int, list[str]] = defaultdict(list)
        for queue in queues:
            priority_groups[int(queue["priority"])].append(queue["queue_name"])

        planned: list[dict[str, Any]] = []
        skipped: list[dict[str, Any]] = []
        rotate_queues: list[str] = []
        queue_order: list[dict[str, Any]] = []
        delivery_eligible_total = 0
        for qname, count in eligible_counts.items():
            if consumer_id:
                has_subscriber = consumer_id in enabled_for_queue.get(qname, set())
            else:
                has_subscriber = bool(enabled_for_queue.get(qname))
            if has_subscriber:
                delivery_eligible_total += count
        for priority in sorted(priority_groups):
            names = sorted(
                (
                    q
                    for q in priority_groups[priority]
                    if per_queue.get(q)
                ),
                key=lambda q: (int(queues_by_name[q]["rotation"]), q),
            )
            if not names:
                continue
            rotate_queues.append(names[0])
            queue_order.append(
                {
                    "priority": priority,
                    "rotation_order": [
                        {
                            "rank": rank,
                            "queue_name": qname,
                            "rotation": int(queues_by_name[qname]["rotation"]),
                        }
                        for rank, qname in enumerate(names)
                    ],
                }
            )
            cursors = {q: 0 for q in names}
            while len(planned) < admission_limit and any(
                cursors[q] < len(per_queue.get(q, [])) for q in names
            ):
                for qname in names:
                    if len(planned) >= admission_limit:
                        break
                    q_rows = per_queue.get(qname, [])
                    index = cursors[qname]
                    if index < len(q_rows):
                        candidate = q_rows[index]
                        candidate["planned_order"] = len(planned)
                        candidate["rotation_rank"] = names.index(qname)
                        planned.append(candidate)
                        cursors[qname] = index + 1
            for qname in names:
                for row in per_queue.get(qname, [])[cursors[qname] :]:
                    skipped.append(
                        {
                            "queue_name": qname,
                            "message_id": row["message_id"],
                            "seq": row["seq"],
                            "priority": row["priority"],
                            "queue_rotation": int(queues_by_name[qname]["rotation"]),
                            "rotation_rank": names.index(qname),
                            "reason": E_QUEUE_FULL,
                            "detail": "bounded admission window (global_concurrency + wait_queue_capacity) is full",
                            "order": len(planned) + len(skipped),
                        }
                    )

        # Queues with no eligible subscriber are reported when no consumer
        # filter was requested. When a specific consumer is selected, queues
        # outside that consumer's subscriptions are simply out of scope.
        for queue in queues:
            qname = queue["queue_name"]
            if consumer_id:
                continue
            if eligible_counts.get(qname, 0) and not per_queue.get(qname):
                with self.store.db.read_tx() as conn:
                    rows = conn.execute(
                        """
                        SELECT m.message_id, m.seq, q.priority FROM messages m
                          JOIN queues q ON q.queue_name = m.queue_name
                         WHERE m.queue_name = ? AND m.status IN (?, ?)
                           AND m.available_at_ms <= ? AND m.active_delivery_id IS NULL
                         ORDER BY m.seq LIMIT ?
                        """,
                        (qname, PENDING, REDELIVERED, now_ms(), max_scan_messages),
                    ).fetchall()
                for row in rows:
                    skipped.append(
                        {
                            "queue_name": qname,
                            "message_id": row["message_id"],
                            "seq": int(row["seq"]),
                            "priority": int(row["priority"]),
                            "queue_rotation": int(queue["rotation"]),
                            "rotation_rank": None,
                            "reason": E_NO_CONSUMER,
                            "detail": "no enabled consumer is subscribed to this queue",
                            "order": len(planned) + len(skipped),
                        }
                    )

        # If there are still lower-priority eligible messages not represented
        # because of the window, they are queue-full degradations too.
        represented = {c["queue_name"] + "\x1f" + c["message_id"] for c in planned}
        represented.update(s["queue_name"] + "\x1f" + s["message_id"] for s in skipped)
        truncated = 0
        for queue in queues:
            qname = queue["queue_name"]
            if consumer_id:
                has_subscriber = consumer_id in enabled_for_queue.get(qname, set())
            else:
                has_subscriber = bool(enabled_for_queue.get(qname))
            if not has_subscriber:
                continue
            missing = int(eligible_counts.get(qname, 0))
            seen = sum(
                1
                for c in planned
                if c["queue_name"] == qname
            ) + sum(1 for s in skipped if s["queue_name"] == qname)
            if missing > seen:
                truncated += missing - seen
        if truncated:
            skipped.append(
                {
                    "queue_name": None,
                    "message_id": None,
                    "seq": None,
                    "priority": None,
                    "reason": E_QUEUE_FULL,
                    "detail": f"{truncated} additional eligible messages were outside max_scan_messages",
                    "order": len(planned) + len(skipped),
                    "aggregate_count": truncated,
                }
            )

        skipped.sort(key=lambda s: (s.get("order", 0), s.get("queue_name") or "", s.get("seq") or 0))
        return {
            "candidates": planned,
            "skipped": skipped,
            "rotate_queues": rotate_queues,
            "queue_order": queue_order,
            "eligible_total": delivery_eligible_total,
            "active_counts": active_counts,
            "scan_truncated": scan_cap_hit,
        }

    def _run_round_locked(
        self,
        mode: str,
        config: dict[str, Any],
        *,
        request_id: str,
        ack_base_url: str,
        consumer_id: str | None,
    ) -> dict[str, Any]:
        if mode not in DELIVERY_MODES:
            raise MWError(E_INVALID_ARGUMENT, f"mode must be one of {sorted(DELIVERY_MODES)}")
        cfg = self._validated_config(config)
        round_id = new_id("round_")
        started_wall = now_ms()
        started_mono = monotonic_ms()
        admission_limit = cfg["global_concurrency"] + cfg["wait_queue_capacity"]
        persisted_config = {
            **cfg,
            "consumer_id": consumer_id,
            "ack_base_url": ack_base_url,
        }
        self.store.create_round(round_id, request_id, mode, persisted_config)
        try:
            reap_result = self.store.reap_expired()
            plan = self._plan(
                consumer_id=consumer_id,
                admission_limit=admission_limit,
                max_scan_messages=cfg["max_scan_messages"],
            )
            if plan["eligible_total"] > admission_limit and cfg["strict_wait_queue"]:
                result = {
                    "ok": False,
                    "status": "QUEUE_FULL",
                    "round_id": round_id,
                    "request_id": request_id,
                    "mode": mode,
                    "config": persisted_config,
                    "error": {
                        "code": E_QUEUE_FULL,
                        "message": "eligible messages exceed the bounded admission window",
                        "details": {
                            "eligible_total": plan["eligible_total"],
                            "admission_limit": admission_limit,
                        },
                    },
                    "error_codes": [E_QUEUE_FULL],
                    "dispatch_attempts": 0,
                    "redelivery_transitions": 0,
                    "skipped": plan["skipped"],
                    "degradation_order": plan["skipped"],
                    "queue_order": plan["queue_order"],
                    "outcomes": [],
                    "recovery_before_round": reap_result,
                    "http_status": 429,
                }
                self.store.finish_round(
                    round_id,
                    "QUEUE_FULL",
                    result,
                    extra_traces=[
                        {
                            "event": "ROUND_DEGRADED",
                            "details": {"code": E_QUEUE_FULL, "strict": True},
                        }
                    ],
                )
                return result

            result, extra_traces = self._admit_and_dispatch(
                round_id=round_id,
                request_id=request_id,
                mode=mode,
                cfg=cfg,
                plan=plan,
                ack_base_url=ack_base_url,
                consumer_id=consumer_id,
                started_mono=started_mono,
                reap_result=reap_result,
            )
            status = "PARTIAL" if result.get("skipped") else "OK"
            if result.get("error_codes"):
                status = "PARTIAL" if result["dispatch_attempts"] else "QUEUE_FULL"
            self.store.finish_round(
                round_id,
                status,
                result,
                extra_traces=extra_traces,
                rotate_queues=plan["rotate_queues"],
            )
            loaded = self.store.get_round(round_id, include_traces=False)
            result["status"] = loaded["status"]
            result["persisted_round"] = {
                "round_id": loaded["round_id"],
                "status": loaded["status"],
                "started_at_ms": loaded["started_at_ms"],
                "finished_at_ms": loaded["finished_at_ms"],
            }
            return result
        except MWError:
            raise
        except BaseException as exc:
            failure = {
                "ok": False,
                "status": "FAILED",
                "round_id": round_id,
                "request_id": request_id,
                "mode": mode,
                "config": persisted_config,
                "error": {"code": "E_INTERNAL", "message": str(exc)},
                "started_at_ms": started_wall,
                "finished_at_ms": now_ms(),
            }
            self.store.finish_round(round_id, "FAILED", failure)
            raise

    def _admit_and_dispatch(
        self,
        *,
        round_id: str,
        request_id: str,
        mode: str,
        cfg: dict[str, Any],
        plan: dict[str, Any],
        ack_base_url: str,
        consumer_id: str | None,
        started_mono: float,
        reap_result: dict[str, Any],
    ) -> tuple[dict[str, Any], list[dict[str, Any]]]:
        candidates: list[dict[str, Any]] = plan["candidates"]
        skipped: list[dict[str, Any]] = plan["skipped"]
        extra_traces: list[dict[str, Any]] = []
        deadline = started_mono + cfg["time_budget_ms"]
        outcomes: list[dict[str, Any]] = []
        claimed_ids: list[int] = []
        claimed_deliveries: list[dict[str, Any]] = []
        queue_claimed: dict[str, int] = defaultdict(int)
        per_queue_inflight = cfg["per_queue_inflight"]

        def mark_skipped(candidate: dict[str, Any], reason: str, detail: str) -> None:
            item = {
                "queue_name": candidate["queue_name"],
                "message_id": candidate["message_id"],
                "seq": candidate["seq"],
                "priority": candidate["priority"],
                "queue_rotation": candidate.get("queue_rotation"),
                "rotation_rank": candidate.get("rotation_rank"),
                "reason": reason,
                "detail": detail,
                "order": candidate.get("planned_order", 0),
                "elapsed_ms_at_decision": int(monotonic_ms() - started_mono),
            }
            skipped.append(item)
            extra_traces.append(
                {
                    "event": "CANDIDATE_DEGRADED",
                    "queue_name": item["queue_name"],
                    "message_id": item["message_id"],
                    "seq": item["seq"],
                    "details": {
                        "reason": reason,
                        "detail": detail,
                        "order": item["order"],
                        "elapsed_ms_at_decision": item["elapsed_ms_at_decision"],
                    },
                }
            )

        with concurrent.futures.ThreadPoolExecutor(
            max_workers=cfg["global_concurrency"], thread_name_prefix="mqmw-dispatch"
        ) as pool:
            inflight: dict[concurrent.futures.Future, dict[str, Any]] = {}

            def collect(done: set[concurrent.futures.Future]) -> None:
                for future in done:
                    delivery = inflight.pop(future)
                    try:
                        outcome = future.result(timeout=1.0)
                    except Exception as exc:  # pragma: no cover - defensive
                        outcome = {
                            "transport": "ERROR",
                            "outcome": DELIVERY_ERROR,
                            "error_code": "E_INTERNAL",
                            "message": str(exc),
                            "delivery_id": delivery["delivery_id"],
                            "queue_name": delivery["queue_name"],
                            "message_id": delivery["message_id"],
                            "attempt": delivery["attempt"],
                            "message_state": DISPATCHED,
                            "connection_closed": True,
                        }
                    outcomes.append(outcome)

            # Candidates blocked by a per-queue lease limit go back into the
            # bounded waiting pool. They become claimable as soon as an earlier
            # in-flight call settles; they are never discarded merely because
            # the queue was momentarily full.
            waiting: list[dict[str, Any]] = list(candidates)
            while waiting:
                if monotonic_ms() >= deadline:
                    for candidate in waiting:
                        mark_skipped(
                            candidate,
                            E_TIME_BUDGET,
                            "round admission deadline reached before this message could be claimed",
                        )
                    waiting = []
                    break

                blocked: list[dict[str, Any]] = []
                while (
                    waiting
                    and len(inflight) < cfg["global_concurrency"]
                    and monotonic_ms() < deadline
                ):
                    candidate = waiting.pop(0)
                    qname = candidate["queue_name"]
                    active_now = self.store.active_delivery_count(qname)
                    if active_now >= per_queue_inflight:
                        blocked.append(candidate)
                        continue
                    try:
                        delivery = self.store.claim(
                            qname,
                            consumer_id,
                            mode,
                            round_id=round_id,
                            defer_commit=True,
                        )
                    except MWError as exc:
                        if exc.code == E_NOT_FOUND:
                            mark_skipped(candidate, E_NO_CONSUMER, exc.message)
                        else:
                            mark_skipped(candidate, exc.code, exc.message)
                        continue
                    except Exception as exc:  # pragma: no cover - defensive
                        mark_skipped(candidate, "E_INTERNAL", str(exc))
                        continue
                    if delivery is None:
                        mark_skipped(
                            candidate,
                            "E_CLAIM_RACE",
                            "message was claimed by another writer before this round",
                        )
                        continue
                    claimed_ids.append(int(delivery["delivery_id"]))
                    claimed_deliveries.append(delivery)
                    queue_claimed[qname] += 1
                    remaining_budget_ms = max(1, int(deadline - monotonic_ms()))
                    effective_timeout_ms = min(
                        cfg["dispatch_timeout_ms"], remaining_budget_ms
                    )
                    future = pool.submit(
                        self._dispatch_http,
                        delivery,
                        mode,
                        effective_timeout_ms,
                        ack_base_url,
                        round_id,
                    )
                    inflight[future] = delivery

                waiting.extend(blocked)
                waiting.sort(key=lambda c: c.get("planned_order", 0))
                if not waiting:
                    break
                if monotonic_ms() >= deadline:
                    continue
                if not inflight:
                    # No in-flight call can release these queues. The active
                    # leases belong to earlier work, so keep the messages
                    # pending and report the queue-limit degradation.
                    for candidate in waiting:
                        mark_skipped(
                            candidate,
                            E_PER_QUEUE_LIMIT,
                            "queue lease limit remained full with no in-round dispatch able to release it",
                        )
                    waiting = []
                    break
                now = monotonic_ms()
                remaining = max(0.001, (deadline - now) / 1000.0)
                done, _ = concurrent.futures.wait(
                    inflight.keys(),
                    timeout=remaining,
                    return_when=concurrent.futures.FIRST_COMPLETED,
                )
                if done:
                    collect(done)

            # Deadline means no new admissions. Already-dispatched HTTP calls
            # are allowed to settle, but they are never interrupted by the
            # admission budget.
            for future, delivery in list(inflight.items()):
                try:
                    outcome = future.result(timeout=cfg["dispatch_timeout_ms"] / 1000.0 + 5.0)
                except Exception as exc:  # pragma: no cover - defensive
                    outcome = {
                        "transport": "ERROR",
                        "outcome": DELIVERY_ERROR,
                        "error_code": "E_INTERNAL",
                        "message": str(exc),
                        "delivery_id": delivery["delivery_id"],
                        "queue_name": delivery["queue_name"],
                        "message_id": delivery["message_id"],
                        "attempt": delivery["attempt"],
                        "message_state": DISPATCHED,
                        "connection_closed": True,
                    }
                outcomes.append(outcome)

        finished_mono = monotonic_ms()
        finished_wall = now_ms()
        skipped.sort(
            key=lambda s: (
                int(s.get("order") or 0),
                s.get("queue_name") or "",
                int(s.get("seq") or 0),
                s.get("message_id") or "",
            )
        )

        accepted = [o for o in outcomes if o.get("outcome") == "ACCEPTED"]
        timeouts = [o for o in outcomes if o.get("outcome") == DELIVERY_TIMED_OUT]
        refused = [o for o in outcomes if o.get("outcome") == DELIVERY_REFUSED]
        errors = [o for o in outcomes if o.get("outcome") == DELIVERY_ERROR]
        redelivery_transitions = [
            o for o in outcomes if o.get("message_state") == REDELIVERED
        ]
        dead_lettered = [o for o in outcomes if o.get("message_state") == DEAD_LETTERED]

        with self.store.db.read_tx() as conn:
            if claimed_ids:
                placeholders = ",".join("?" for _ in claimed_ids)
                acked_rows = conn.execute(
                    f"""
                    SELECT delivery_id FROM acks
                     WHERE state = 'COMMITTED' AND delivery_id IN ({placeholders})
                    """,
                    tuple(claimed_ids),
                ).fetchall()
            else:
                acked_rows = []
            acked_ids = {int(r["delivery_id"]) for r in acked_rows}
            round_trace_count = int(
                conn.execute(
                    "SELECT COUNT(*) AS n FROM round_traces WHERE round_id = ?", (round_id,)
                ).fetchone()["n"]
            )
            revision = int(
                conn.execute("SELECT value FROM revision WHERE id = 1").fetchone()["value"]
            )

        per_queue: dict[str, dict[str, Any]] = {}
        involved = sorted(
            {c["queue_name"] for c in candidates}
            | {o["queue_name"] for o in outcomes}
            | set(queue_claimed)
        )
        for qname in involved:
            per_queue[qname] = {
                "planned": sum(1 for c in candidates if c["queue_name"] == qname),
                "claimed": queue_claimed.get(qname, 0),
                "accepted": sum(1 for o in accepted if o["queue_name"] == qname),
                "redelivered_attempts": sum(
                    1
                    for o in outcomes
                    if o["queue_name"] == qname and int(o.get("attempt", 0)) > 1
                ),
                "timeouts": sum(1 for o in timeouts if o["queue_name"] == qname),
                "refused": sum(1 for o in refused if o["queue_name"] == qname),
                "errors": sum(1 for o in errors if o["queue_name"] == qname),
                "redelivery_transitions": sum(
                    1 for o in redelivery_transitions if o["queue_name"] == qname
                ),
                "dead_lettered": sum(1 for o in dead_lettered if o["queue_name"] == qname),
                "acked": sum(
                    1
                    for o in outcomes
                    if o["queue_name"] == qname and int(o["delivery_id"]) in acked_ids
                ),
                "skipped": sum(1 for s in skipped if s["queue_name"] == qname),
                "inflight_at_end": self.store.active_delivery_count(qname),
            }

        error_codes: list[str] = []
        if any(s["reason"] == E_QUEUE_FULL for s in skipped):
            error_codes.append(E_QUEUE_FULL)
        if any(s["reason"] == E_TIME_BUDGET for s in skipped):
            error_codes.append(E_TIME_BUDGET)
        if timeouts:
            error_codes.append("E_DISPATCH_TIMEOUT")
        if refused:
            error_codes.append("E_DISPATCH_REFUSED")

        budget_stop_ms = None
        if any(s["reason"] == E_TIME_BUDGET for s in skipped):
            budget_stop_ms = int(deadline - started_mono)
        result: dict[str, Any] = {
            "ok": True,
            "round_id": round_id,
            "request_id": request_id,
            "mode": mode,
            "config": cfg,
            "started_at_ms": round_start_wall(round_id, self.store),
            "finished_at_ms": finished_wall,
            "elapsed_ms": int(finished_mono - started_mono),
            "admission_deadline_ms": cfg["time_budget_ms"],
            "admission_stopped_at_ms": budget_stop_ms,
            "budget_exhausted": bool(budget_stop_ms is not None),
            "eligible_total": plan["eligible_total"],
            "admission_limit": cfg["global_concurrency"] + cfg["wait_queue_capacity"],
            "planned": len(candidates),
            "claimed": len(claimed_ids),
            "dispatch_attempts": len(claimed_ids),
            "redelivered_attempts": sum(1 for d in claimed_deliveries if int(d["attempt"]) > 1),
            "deliveries": len(claimed_ids),
            "redeliveries": sum(1 for d in claimed_deliveries if int(d["attempt"]) > 1),
            "redelivery_transitions": len(redelivery_transitions),
            "http_accepted": len(accepted),
            "dispatch_timeouts": len(timeouts),
            "budget_limited_dispatches": sum(
                1
                for o in outcomes
                if int(o.get("effective_timeout_ms", cfg["dispatch_timeout_ms"]))
                < cfg["dispatch_timeout_ms"]
            ),
            "dispatch_refusals": len(refused),
            "dispatch_errors": len(errors),
            "dead_lettered": len(dead_lettered),
            "acks_observed": len(acked_ids),
            "skipped_count": len(skipped),
            "wait_queue_overflow": sum(
                1
                for s in skipped
                if s["reason"] == E_QUEUE_FULL
            ),
            "error_codes": error_codes,
            "queue_order": plan["queue_order"],
            "degradation_order": [
                {
                    "order": s.get("order"),
                    "queue_name": s.get("queue_name"),
                    "message_id": s.get("message_id"),
                    "seq": s.get("seq"),
                    "priority": s.get("priority"),
                    "queue_rotation": s.get("queue_rotation"),
                    "rotation_rank": s.get("rotation_rank"),
                    "reason": s.get("reason"),
                    "detail": s.get("detail"),
                    "elapsed_ms_at_decision": s.get("elapsed_ms_at_decision"),
                }
                for s in skipped
            ],
            "outcomes": outcomes,
            "skipped": skipped,
            "per_queue": per_queue,
            "recovery_before_round": reap_result,
            "round_trace_count": round_trace_count,
            "revision_at_finish": revision,
        }
        return result, extra_traces

    def _dispatch_http(
        self,
        delivery: dict[str, Any],
        mode: str,
        dispatch_timeout_ms: int,
        ack_base_url: str,
        round_id: str,
    ) -> dict[str, Any]:
        # The caller passes min(configured dispatch timeout, remaining round
        # budget). This is still an independent per-request timeout, while also
        # ensuring an in-flight call cannot extend the round past its deadline.
        timeout_s = max(0.001, dispatch_timeout_ms / 1000.0)
        connect_timeout_s = min(timeout_s, 2.0)
        ack_url = ack_base_url.rstrip("/") + "/v1/acks"
        payload = {
            "protocol": "mqmw/1",
            "round_id": round_id,
            "mode": mode,
            "delivery_id": delivery["delivery_id"],
            "lease_token": delivery["lease_token"],
            "consumer_id": delivery["consumer_id"],
            "consumer_group": delivery["consumer_group"],
            "ack_url": ack_url,
            "visibility_timeout_ms": delivery["visibility_timeout_ms"],
            "deadline_ms": delivery["deadline_ms"],
            "message": {
                "queue_name": delivery["queue_name"],
                "message_id": delivery["message_id"],
                "seq": delivery["seq"],
                "body": delivery["body"],
                "body_digest": delivery["body_digest"],
                "attempt": delivery["attempt"],
                "max_retries": delivery["max_retries"],
            },
        }
        session = requests.Session()
        started = monotonic_ms()
        connection_closed = False
        effective_timeout_ms = dispatch_timeout_ms
        try:
            response = session.post(
                delivery["consumer_endpoint"],
                json=payload,
                headers={
                    "X-MQMW-Delivery-ID": str(delivery["delivery_id"]),
                    "X-MQMW-Round-ID": round_id,
                },
                timeout=(connect_timeout_s, timeout_s),
                allow_redirects=False,
            )
            elapsed = int(monotonic_ms() - started)
            if 200 <= response.status_code < 300:
                self.store.complete_dispatch(
                    delivery["queue_name"],
                    delivery["intent_id"],
                    int(delivery["delivery_id"]),
                    http_status=response.status_code,
                    elapsed_ms=elapsed,
                )
                result = {
                    "transport": "ACCEPTED",
                    "outcome": "ACCEPTED",
                    "http_status": response.status_code,
                    "elapsed_ms": elapsed,
                    "connection_closed": False,
                    "effective_timeout_ms": effective_timeout_ms,
                    "response_excerpt": response.text[:512],
                    "delivery_id": delivery["delivery_id"],
                    "queue_name": delivery["queue_name"],
                    "message_id": delivery["message_id"],
                    "attempt": delivery["attempt"],
                    "message_state": DISPATCHED,
                }
            else:
                failure = self.store.fail_dispatch(
                    delivery["queue_name"],
                    delivery["intent_id"],
                    int(delivery["delivery_id"]),
                    outcome=DELIVERY_REFUSED,
                    error_code="E_DISPATCH_REFUSED",
                    http_status=response.status_code,
                    elapsed_ms=elapsed,
                )
                result = {
                    "transport": "REFUSED",
                    "outcome": DELIVERY_REFUSED,
                    "http_status": response.status_code,
                    "elapsed_ms": elapsed,
                    "connection_closed": False,
                    "effective_timeout_ms": effective_timeout_ms,
                    "response_excerpt": response.text[:512],
                    "delivery_id": delivery["delivery_id"],
                    "queue_name": delivery["queue_name"],
                    "message_id": delivery["message_id"],
                    "attempt": delivery["attempt"],
                    "message_state": failure["message_state"],
                }
            response.close()
            return result
        except requests.exceptions.Timeout as exc:
            elapsed = int(monotonic_ms() - started)
            # Closing the session is deliberate and observable: requests tears
            # down its adapter connections, and the worker immediately frees
            # its global concurrency slot. The consumer may still be sleeping,
            # but no middleware socket remains attached.
            session.close()
            connection_closed = True
            failure = self.store.fail_dispatch(
                delivery["queue_name"],
                delivery["intent_id"],
                int(delivery["delivery_id"]),
                outcome=DELIVERY_TIMED_OUT,
                error_code="E_DISPATCH_TIMEOUT",
                http_status=None,
                elapsed_ms=elapsed,
            )
            return {
                "transport": "TIMEOUT",
                "outcome": DELIVERY_TIMED_OUT,
                "error_code": "E_DISPATCH_TIMEOUT",
                "error": str(exc),
                "http_status": None,
                "elapsed_ms": elapsed,
                "connection_closed": connection_closed,
                "effective_timeout_ms": effective_timeout_ms,
                "delivery_id": delivery["delivery_id"],
                "queue_name": delivery["queue_name"],
                "message_id": delivery["message_id"],
                "attempt": delivery["attempt"],
                "message_state": failure["message_state"],
            }
        except requests.exceptions.RequestException as exc:
            elapsed = int(monotonic_ms() - started)
            session.close()
            connection_closed = True
            failure = self.store.fail_dispatch(
                delivery["queue_name"],
                delivery["intent_id"],
                int(delivery["delivery_id"]),
                outcome=DELIVERY_ERROR,
                error_code="E_DISPATCH_ERROR",
                http_status=None,
                elapsed_ms=elapsed,
            )
            return {
                "transport": "ERROR",
                "outcome": DELIVERY_ERROR,
                "error_code": "E_DISPATCH_ERROR",
                "error": str(exc),
                "http_status": None,
                "elapsed_ms": elapsed,
                "connection_closed": connection_closed,
                "effective_timeout_ms": effective_timeout_ms,
                "delivery_id": delivery["delivery_id"],
                "queue_name": delivery["queue_name"],
                "message_id": delivery["message_id"],
                "attempt": delivery["attempt"],
                "message_state": failure["message_state"],
            }
        finally:
            session.close()


def round_start_wall(round_id: str, store: SQLiteStore) -> int:
    try:
        return int(store.get_round(round_id, include_traces=False)["started_at_ms"])
    except Exception:
        return now_ms()

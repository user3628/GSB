"""Parallel round scheduling under a global concurrency limit, a bounded
waiting queue and a per-round time budget.

Admission order is deterministic: priority first, then endpoint rotation
(the rotation offset advances with the round number), then item id.
"""

from __future__ import annotations

import threading
import time
from concurrent.futures import ThreadPoolExecutor

from .errors import E_INJECTION_QUEUE_FULL
from .faults import execute_fault
from .models import (
    OUTCOME_QUEUE_FULL,
    OUTCOME_SKIPPED,
)

REASON_FAULT_DISABLED = "fault_type_disabled"
REASON_DUPLICATE_ENDPOINT = "duplicate_endpoint_in_round"
REASON_BUDGET_EXHAUSTED = "round_time_budget_exhausted"


def _trajectory_entry(item, outcome, code=None, reason=None, elapsed_ms=0.0, evidence=None):
    return {
        "item_id": item.item_id,
        "target": item.target,
        "fault_type": item.fault_type,
        "priority": item.priority,
        "outcome": outcome,
        "code": code,
        "reason": reason,
        "elapsed_ms": round(elapsed_ms, 3),
        "evidence": evidence or {},
    }


class RoundScheduler:
    def __init__(self, config):
        self.config = config

    def admission_order(self, items, catalog, round_no):
        endpoints = catalog.endpoints
        index = {e.endpoint_id: pos for pos, e in enumerate(endpoints)}
        n = max(len(endpoints), 1)

        def key(item):
            rotation = (index[item.target] + round_no) % n
            return (-item.priority, rotation, item.item_id)

        return sorted(items, key=key)

    def admit(self, items, catalog, round_no, fault_switches):
        """Split items into admitted ones and deterministic skips."""
        admitted = []
        skipped = []
        remaining_ms = self.config.round_time_budget_ms
        used_targets = set()
        for item in self.admission_order(items, catalog, round_no):
            if not fault_switches.get(item.fault_type, True):
                skipped.append(_trajectory_entry(
                    item, OUTCOME_SKIPPED, reason=REASON_FAULT_DISABLED))
                continue
            if item.target in used_targets:
                skipped.append(_trajectory_entry(
                    item, OUTCOME_SKIPPED, reason=REASON_DUPLICATE_ENDPOINT))
                continue
            estimate = self.config.estimate(item.fault_type)
            if estimate > remaining_ms:
                skipped.append(_trajectory_entry(
                    item, OUTCOME_SKIPPED, reason=REASON_BUDGET_EXHAUSTED,
                    evidence={"estimate_ms": estimate,
                              "remaining_budget_ms": round(remaining_ms, 3)}))
                continue
            remaining_ms -= estimate
            used_targets.add(item.target)
            admitted.append(item)
        return admitted, skipped, remaining_ms

    def execute(self, admitted, catalog):
        """Run admitted items concurrently; overflow gets E_INJECTION_QUEUE_FULL."""
        by_id = catalog.by_id
        limit = self.config.global_concurrency
        gate = threading.BoundedSemaphore(limit + self.config.queue_capacity)
        results = {}
        started = time.monotonic()

        def run(item):
            try:
                verdict = execute_fault(by_id[item.target], item.fault_type,
                                        self.config.connect_timeout_ms)
                results[item.item_id] = _trajectory_entry(
                    item, verdict.outcome, code=verdict.code, reason=verdict.reason,
                    elapsed_ms=verdict.elapsed_ms, evidence=verdict.evidence)
            finally:
                gate.release()

        with ThreadPoolExecutor(max_workers=limit) as pool:
            futures = []
            queue_full = []
            for item in admitted:
                if gate.acquire(blocking=False):
                    futures.append(pool.submit(run, item))
                else:
                    queue_full.append(_trajectory_entry(
                        item, OUTCOME_QUEUE_FULL, code=E_INJECTION_QUEUE_FULL,
                        reason="global concurrency limit and bounded waiting queue "
                               "are both full"))
            for fut in futures:
                fut.result()
        wall_ms = (time.monotonic() - started) * 1000.0
        executed = [results[item.item_id] for item in admitted if item.item_id in results]
        return executed + queue_full, wall_ms

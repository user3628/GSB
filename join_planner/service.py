from __future__ import annotations

import hashlib
import json
import threading
import time
from dataclasses import dataclass, field
from typing import Any

from .config import PlannerConfig
from .errors import PlannerError, invalid_query
from .journal import IntentJournal
from .optimizer import PlanOptimizer
from .query import QueryCatalog
from .snapshot import SnapshotStore


@dataclass
class _BuildEntry:
    query_id: str
    signature: str
    event: threading.Event = field(default_factory=threading.Event)
    state: str = "building"
    result: dict[str, Any] | None = None
    error: PlannerError | None = None


class PlanService:
    """Serial leader builds with concurrent idempotent result reuse."""

    def __init__(
        self,
        snapshots: SnapshotStore,
        catalog: QueryCatalog,
        journal: IntentJournal,
        optimizer: PlanOptimizer,
        config: PlannerConfig,
    ) -> None:
        self.snapshots = snapshots
        self.catalog = catalog
        self.journal = journal
        self.optimizer = optimizer
        self.config = config
        self._registry_lock = threading.Lock()
        self._build_lock = threading.Lock()
        self._entries: dict[tuple[str, int, str], _BuildEntry] = {}
        self._latest_by_query: dict[str, tuple[str, int, str]] = {}

    @staticmethod
    def _signature(query_dict: dict[str, Any], mode: str, snapshot_checksum: str) -> str:
        payload = {
            "query": query_dict,
            "mode": mode,
            "snapshot_checksum": snapshot_checksum,
        }
        return json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)

    def submit(self, query_id: str, snapshot_version: int, mode: str) -> tuple[dict[str, Any], str]:
        if mode not in {"fast", "exact"}:
            raise invalid_query("mode must be 'fast' or 'exact'", mode=mode)
        query = self.catalog.get(query_id)
        snapshot = self.snapshots.for_submission(snapshot_version)
        self.optimizer.validate(snapshot, query)
        signature = self._signature(query.as_dict(), mode, snapshot.checksum)

        request_key = (query_id, snapshot_version, mode)
        with self._registry_lock:
            existing = self._entries.get(request_key)
            if existing is not None:
                if existing.state == "completed":
                    assert existing.result is not None
                    return existing.result, "hit"
                leader = False
                entry = existing
            else:
                entry = _BuildEntry(query_id=query_id, signature=signature)
                self._entries[request_key] = entry
                leader = True

        if not leader:
            entry.event.wait()
            if entry.error is not None:
                raise entry.error
            assert entry.result is not None
            return entry.result, "hit"

        intent: dict[str, Any] | None = None
        try:
            with self._build_lock:
                intent = self.journal.begin(query_id, snapshot_version, signature)
                if self.config.build_delay_seconds > 0:
                    time.sleep(self.config.build_delay_seconds)
                plan = self.optimizer.plan(snapshot, query, mode)
                plan["snapshot_binding"] = {
                    "captured_version": snapshot.version,
                    "captured_checksum": snapshot.checksum,
                    "retired_during_build": not self.snapshots.is_still_current(snapshot),
                    "isolation_rule": "The build used the immutable snapshot object captured at submission.",
                }
                plan["plan_digest"] = self._reseal_plan(plan)
                self.journal.write_plan(intent, plan)
                self.journal.complete(intent, plan["plan_digest"])
            entry.result = plan
            entry.state = "completed"
            with self._registry_lock:
                self._latest_by_query[query_id] = request_key
            return plan, "miss"
        except PlannerError as exc:
            if intent is not None:
                try:
                    self.journal.abort(intent, exc.code, exc.message)
                except Exception:
                    pass
            entry.error = exc
            entry.state = "failed"
            raise
        except Exception as exc:
            if intent is not None:
                try:
                    self.journal.abort(intent, "E_INTERNAL", str(exc))
                except Exception:
                    pass
            entry.error = PlannerError("E_INTERNAL", "Plan build failed.", {"error": str(exc)})
            entry.state = "failed"
            raise entry.error from exc
        finally:
            entry.event.set()

    @staticmethod
    def _reseal_plan(plan: dict[str, Any]) -> str:
        digest_source = {key: value for key, value in plan.items() if key != "plan_digest"}
        return hashlib.sha256(
            json.dumps(
                digest_source,
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=False,
                allow_nan=False,
            ).encode("utf-8")
        ).hexdigest()

    def get_materialized(
        self,
        query_id: str,
        mode: str | None = None,
        snapshot_version: int | None = None,
    ) -> dict[str, Any]:
        with self._registry_lock:
            if mode is not None and snapshot_version is not None:
                request_key = (query_id, snapshot_version, mode)
            elif mode is not None:
                candidates = [
                    key for key, entry in self._entries.items() if key[0] == query_id and key[2] == mode
                ]
                request_key = max(candidates, key=lambda key: key[1]) if candidates else None
            elif snapshot_version is not None:
                candidates = [
                    key
                    for key, entry in self._entries.items()
                    if key[0] == query_id and key[1] == snapshot_version
                ]
                request_key = max(candidates, key=lambda key: key[2]) if candidates else None
            else:
                request_key = self._latest_by_query.get(query_id)
            entry = self._entries.get(request_key) if request_key is not None else None
            if entry is None or entry.state != "completed" or entry.result is None:
                raise PlannerError(
                    "E_PLAN_NOT_FOUND",
                    "No completed materialized plan exists for this query_id.",
                    {
                        "query_id": query_id,
                        "mode": mode,
                        "snapshot_version": snapshot_version,
                    },
                )
            return entry.result


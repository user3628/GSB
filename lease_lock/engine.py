from __future__ import annotations

import json
import os
import sqlite3
import threading
import uuid
from contextlib import closing
from dataclasses import replace
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping

from .clock import LogicalClock
from .errors import ApiError
from .model import (
    DependencyMode,
    Lease,
    LeaseState,
    Snapshot,
    canonical_json,
    find_dependency_cycle,
    normalize_dependencies,
    RELEASE_DEPENDENCY_EXPIRED,
    RELEASE_EXPLICIT,
    RELEASE_LEASE_EXPIRED,
)
from .storage import Database


PENDING_ABORTED_CODE = "E_RECOVERED_ABORTED_INTENT"


class LeaseEngine:
    def __init__(
        self,
        db_path: str | Path,
        *,
        clock_offset_ms: int = 0,
        enable_faults: bool = False,
        fault_hook: Callable[[str, dict[str, Any]], None] | None = None,
    ) -> None:
        self.db = Database(db_path)
        self.db.initialize()
        self._serial = threading.RLock()
        self.enable_faults = enable_faults
        self._fault_hook = fault_hook

        with closing(self.db.connect()) as conn:
            high_water = int(self.db.get_meta(conn, "clock_high_water_ms", "0"))
        self.clock = LogicalClock(high_water, offset_ms=clock_offset_ms)
        self._snapshot: Snapshot = Snapshot.empty()
        self._recovery_report: dict[str, Any] = {}
        self._recover()

    @property
    def snapshot_version(self) -> int:
        with self._serial:
            return self._snapshot.version

    def close(self) -> None:
        # Connections are intentionally short-lived; there is no open handle to release.
        return None

    # ------------------------------------------------------------------
    # Public mutations
    # ------------------------------------------------------------------
    def acquire(
        self,
        lock_key: str,
        holder: str,
        ttl_ms: int,
        dependencies: Iterable[Any] | None = None,
        *,
        debug_fault: str | None = None,
    ) -> dict[str, Any]:
        lock_key = _require_text(lock_key, "lock_key")
        holder = _require_text(holder, "holder")
        ttl_ms = _positive_int(ttl_ms, "ttl_ms")
        deps = _parse_dependencies(dependencies)
        payload = {
            "lock_key": lock_key,
            "holder": holder,
            "ttl_ms": ttl_ms,
            "dependencies": [dependency.to_dict() for dependency in deps],
        }

        def operation(conn: sqlite3.Connection, current: Snapshot, now: int):
            expiry_patches = _expiry_patches(current, now)
            effective = current.leases.get(lock_key)
            if effective is not None and _lease_blocks_acquisition(effective, now):
                raise ApiError(
                    "E_LOCK_HELD",
                    f"lock {lock_key!r} is held",
                    snapshot_version=current.version,
                    details={
                        "lock_key": lock_key,
                        "holder": effective.holder,
                        "lease_version": effective.lease_version,
                        "remaining_ttl_ms": max(0, effective.expires_at_ms - now),
                        "expires_at_ms": effective.expires_at_ms,
                    },
                )

            cycle = find_dependency_cycle(_leases_with_patch(current.leases, lock_key, deps, None))
            if cycle:
                raise ApiError(
                    "E_DEPENDENCY_CYCLE",
                    "dependency declaration would create a cycle",
                    snapshot_version=current.version,
                    details={"cycle": cycle},
                )

            lease_version = max(
                _next_lease_version(current, lock_key),
                _persisted_last_lease_version(conn, lock_key) + 1,
            )
            lease = Lease(
                lock_key=lock_key,
                holder=holder,
                lease_version=lease_version,
                acquired_at_ms=now,
                ttl_ms=ttl_ms,
                expires_at_ms=now + ttl_ms,
                state=LeaseState.ACTIVE,
                dependencies=deps,
            )
            patches = dict(expiry_patches)
            patches[lock_key] = lease
            snapshot = _finalize_snapshot(
                current,
                patches=patches,
                kind="acquire",
                now=now,
                seed_keys=set(patches),
            )
            granted = snapshot.leases[lock_key]
            if granted.state == LeaseState.RELEASED and granted.release_reason == RELEASE_DEPENDENCY_EXPIRED:
                raise ApiError(
                    "E_DEPENDENCY_EXPIRED",
                    "a strong parent dependency is not currently active",
                    snapshot_version=current.version,
                    details={
                        "lock_key": lock_key,
                        "dependencies": [d.to_dict() for d in deps],
                    },
                )
            _ensure_resource(conn, lock_key, {}, now)
            return _mutation_result(
                "acquired",
                snapshot,
                current.version,
                extra={"lease": granted.to_dict()},
            ), snapshot

        return self._run_mutation("acquire", None, payload, operation, debug_fault)

    def renew(
        self,
        lock_key: str,
        holder: str,
        lease_version: int,
        ttl_ms: int | None = None,
        *,
        debug_fault: str | None = None,
    ) -> dict[str, Any]:
        lock_key = _require_text(lock_key, "lock_key")
        holder = _require_text(holder, "holder")
        lease_version = _positive_int(lease_version, "lease_version")
        if ttl_ms is not None:
            ttl_ms = _positive_int(ttl_ms, "ttl_ms")
        payload = {
            "lock_key": lock_key,
            "holder": holder,
            "lease_version": lease_version,
            "ttl_ms": ttl_ms,
        }

        def operation(conn: sqlite3.Connection, current: Snapshot, now: int):
            expiry_patches = _expiry_patches(current, now)
            lease = current.leases.get(lock_key)
            if lease is None:
                raise ApiError(
                    "E_LEASE_NOT_FOUND",
                    f"lease {lock_key!r} was not found",
                    snapshot_version=current.version,
                    details={"lock_key": lock_key},
                )
            if _effective_release_reason(lease, now) == RELEASE_DEPENDENCY_EXPIRED:
                raise ApiError(
                    "E_DEPENDENCY_EXPIRED",
                    "the lease was released because a strong dependency expired",
                    snapshot_version=current.version,
                    details={"lock_key": lock_key},
                )
            if not _lease_usable(lease, now) or lease.holder != holder or lease.lease_version != lease_version:
                raise ApiError(
                    "E_STALE_LEASE",
                    "holder and lease version must match the current active lease",
                    snapshot_version=current.version,
                    details={
                        "lock_key": lock_key,
                        "expected_holder": lease.holder,
                        "expected_lease_version": lease.lease_version,
                        "provided_holder": holder,
                        "provided_lease_version": lease_version,
                    },
                )
            effective_ttl = ttl_ms if ttl_ms is not None else lease.ttl_ms
            renewed = replace(
                lease,
                lease_version=lease.lease_version + 1,
                ttl_ms=effective_ttl,
                expires_at_ms=now + effective_ttl,
                state=LeaseState.ACTIVE,
                released_at_ms=None,
                release_reason=None,
            )
            patches = dict(expiry_patches)
            patches[lock_key] = renewed
            snapshot = _finalize_snapshot(
                current,
                patches=patches,
                kind="renew",
                now=now,
                seed_keys=set(patches),
            )
            return _mutation_result(
                "renewed",
                snapshot,
                current.version,
                extra={"lease": snapshot.leases[lock_key].to_dict()},
            ), snapshot

        return self._run_mutation("renew", None, payload, operation, debug_fault)

    def release(
        self,
        lock_key: str,
        holder: str,
        lease_version: int,
        *,
        debug_fault: str | None = None,
    ) -> dict[str, Any]:
        lock_key = _require_text(lock_key, "lock_key")
        holder = _require_text(holder, "holder")
        lease_version = _positive_int(lease_version, "lease_version")
        payload = {
            "lock_key": lock_key,
            "holder": holder,
            "lease_version": lease_version,
        }

        def operation(conn: sqlite3.Connection, current: Snapshot, now: int):
            expiry_patches = _expiry_patches(current, now)
            lease = current.leases.get(lock_key)
            if lease is None:
                raise ApiError(
                    "E_LEASE_NOT_FOUND",
                    f"lease {lock_key!r} was not found",
                    snapshot_version=current.version,
                    details={"lock_key": lock_key},
                )
            release_reason = _effective_release_reason(lease, now)
            if release_reason == RELEASE_DEPENDENCY_EXPIRED:
                raise ApiError(
                    "E_DEPENDENCY_EXPIRED",
                    "the lease was released because a strong dependency expired",
                    snapshot_version=current.version,
                    details={"lock_key": lock_key},
                )
            if not _lease_usable(lease, now) or lease.holder != holder or lease.lease_version != lease_version:
                raise ApiError(
                    "E_STALE_LEASE",
                    "holder and lease version must match the current active lease",
                    snapshot_version=current.version,
                    details={
                        "lock_key": lock_key,
                        "expected_holder": lease.holder,
                        "expected_lease_version": lease.lease_version,
                    },
                )
            released = replace(
                lease,
                lease_version=lease.lease_version + 1,
                state=LeaseState.RELEASED,
                released_at_ms=now,
                release_reason=RELEASE_EXPLICIT,
            )
            patches = dict(expiry_patches)
            patches[lock_key] = released
            snapshot = _finalize_snapshot(
                current,
                patches=patches,
                kind="release",
                now=now,
                seed_keys=set(patches),
            )
            return _mutation_result(
                "released",
                snapshot,
                current.version,
                extra={"lease": snapshot.leases[lock_key].to_dict()},
            ), snapshot

        return self._run_mutation("release", None, payload, operation, debug_fault)

    def commit_increment(
        self,
        base_version: int,
        changes: Iterable[Mapping[str, Any]],
        *,
        debug_fault: str | None = None,
    ) -> dict[str, Any]:
        base_version = _non_negative_int(base_version, "base_version")
        change_list = list(changes)
        if not change_list:
            raise ApiError("E_INVALID_REQUEST", "changes must not be empty")
        for change in change_list:
            if not isinstance(change, Mapping):
                raise ApiError("E_INVALID_REQUEST", "each change must be an object")
        payload = {"base_version": base_version, "changes": change_list}

        def operation(conn: sqlite3.Connection, current: Snapshot, now: int):
            if current.version != base_version:
                raise ApiError(
                    "E_STALE_BASE",
                    "base_version does not match the current snapshot version",
                    snapshot_version=current.version,
                    details={"base_version": base_version, "current_version": current.version},
                )

            expiry_patches = _expiry_patches(current, now)
            patches: dict[str, Lease] = {}
            for raw_change in change_list:
                key = _require_text(
                    raw_change.get("lock_key") or raw_change.get("lease_key") or "",
                    "lock_key",
                )
                old = current.leases.get(key)
                if old is None:
                    raise ApiError(
                        "E_LEASE_NOT_FOUND",
                        f"lease {key!r} was not found",
                        snapshot_version=current.version,
                        details={"lock_key": key},
                    )
                if not _lease_usable(old, now):
                    raise ApiError(
                        "E_STALE_LEASE" if key not in expiry_patches else "E_STALE_LEASE",
                        "cannot incrementally update an expired or released lease",
                        snapshot_version=current.version,
                        details={"lock_key": key, "state": old.state.value},
                    )
                expected_version = raw_change.get("expected_lease_version")
                if expected_version is not None and int(expected_version) != old.lease_version:
                    raise ApiError(
                        "E_STALE_LEASE",
                        "expected_lease_version does not match the current lease version",
                        snapshot_version=current.version,
                        details={
                            "lock_key": key,
                            "expected_lease_version": int(expected_version),
                            "current_lease_version": old.lease_version,
                        },
                    )
                holder = (
                    _require_text(raw_change["holder"], "holder")
                    if "holder" in raw_change
                    else old.holder
                )
                ttl = (
                    _positive_int(raw_change["ttl_ms"], "ttl_ms")
                    if "ttl_ms" in raw_change
                    else old.ttl_ms
                )
                dependencies = (
                    _parse_dependencies(raw_change.get("dependencies"))
                    if "dependencies" in raw_change
                    else old.dependencies
                )
                expires_at = now + ttl if "ttl_ms" in raw_change else old.expires_at_ms
                patches[key] = replace(
                    old,
                    holder=holder,
                    ttl_ms=ttl,
                    dependencies=dependencies,
                    expires_at_ms=expires_at,
                    lease_version=old.lease_version + 1,
                    state=LeaseState.ACTIVE,
                    released_at_ms=None,
                    release_reason=None,
                )

            graph = _leases_with_patches(current.leases, patches)
            cycle = find_dependency_cycle(graph)
            if cycle:
                raise ApiError(
                    "E_DEPENDENCY_CYCLE",
                    "incremental dependency update would create a cycle",
                    snapshot_version=current.version,
                    details={"cycle": cycle, "base_version": base_version},
                )

            combined = dict(expiry_patches)
            combined.update(patches)
            snapshot = _finalize_snapshot(
                current,
                patches=combined,
                kind="increment",
                now=now,
                seed_keys=set(combined),
            )
            changed_keys = sorted(
                key
                for key, lease in snapshot.leases.items()
                if current.leases.get(key) != lease
            )
            return _mutation_result(
                "increment_committed",
                snapshot,
                current.version,
                extra={
                    "base_version": base_version,
                    "changed_leases": changed_keys,
                    "affected_leases": list(snapshot.affected),
                },
            ), snapshot

        return self._run_mutation("increment", base_version, payload, operation, debug_fault)

    def replace_snapshot(
        self,
        base_version: int,
        raw_leases: Iterable[Mapping[str, Any]],
        *,
        debug_fault: str | None = None,
    ) -> dict[str, Any]:
        base_version = _non_negative_int(base_version, "base_version")
        raw_list = list(raw_leases)
        payload = {"base_version": base_version, "leases": raw_list}

        def operation(conn: sqlite3.Connection, current: Snapshot, now: int):
            if current.version != base_version:
                raise ApiError(
                    "E_STALE_BASE",
                    "base_version does not match the current snapshot version",
                    snapshot_version=current.version,
                    details={"base_version": base_version, "current_version": current.version},
                )
            next_leases: dict[str, Lease] = {}
            for raw in raw_list:
                if not isinstance(raw, Mapping):
                    raise ApiError("E_INVALID_REQUEST", "each lease must be an object")
                key = _require_text(raw.get("lock_key") or raw.get("lease_key") or "", "lock_key")
                if key in next_leases:
                    raise ApiError(
                        "E_INVALID_REQUEST",
                        f"duplicate lock_key {key!r} in full snapshot",
                    )
                holder = _require_text(raw.get("holder", ""), "holder")
                ttl_ms = _positive_int(raw.get("ttl_ms", 0), "ttl_ms")
                dependencies = _parse_dependencies(raw.get("dependencies", []))
                requested_version = _positive_int(raw.get("lease_version", 1), "lease_version")
                lease_version = max(
                    requested_version,
                    _persisted_last_lease_version(conn, key) + 1,
                )
                state = str(raw.get("state", "active")).lower()
                if state == LeaseState.RELEASED.value:
                    lease = Lease(
                        lock_key=key,
                        holder=holder,
                        lease_version=lease_version,
                        acquired_at_ms=int(raw.get("acquired_at_ms", now)),
                        ttl_ms=ttl_ms,
                        expires_at_ms=int(raw.get("expires_at_ms", now + ttl_ms)),
                        state=LeaseState.RELEASED,
                        released_at_ms=int(raw.get("released_at_ms", now)),
                        release_reason=str(raw.get("release_reason", RELEASE_EXPLICIT)),
                        dependency_expired=bool(raw.get("dependency_expired", False)),
                        dependencies=dependencies,
                    )
                elif state == LeaseState.ACTIVE.value:
                    lease = Lease(
                        lock_key=key,
                        holder=holder,
                        lease_version=lease_version,
                        acquired_at_ms=now,
                        ttl_ms=ttl_ms,
                        expires_at_ms=now + ttl_ms,
                        dependencies=dependencies,
                    )
                else:
                    raise ApiError("E_INVALID_REQUEST", f"invalid lease state {state!r}")
                next_leases[key] = lease

            cycle = find_dependency_cycle(next_leases)
            if cycle:
                raise ApiError(
                    "E_DEPENDENCY_CYCLE",
                    "full snapshot contains a dependency cycle",
                    snapshot_version=current.version,
                    details={"cycle": cycle},
                )

            next_version = current.version + 1
            snapshot = Snapshot.build(
                version=next_version,
                parent_version=current.version,
                kind="full_replace",
                created_at_ms=now,
                leases=next_leases,
                affected=set(current.leases) | set(next_leases),
            )
            snapshot = _normalize_dependencies(snapshot, now)
            removed = sorted(set(current.leases) - set(next_leases))
            for key in next_leases:
                _ensure_resource(conn, key, {}, now)
            return _mutation_result(
                "snapshot_replaced",
                snapshot,
                current.version,
                extra={
                    "base_version": base_version,
                    "loaded_leases": len(next_leases),
                    "removed_leases": removed,
                    "affected_leases": list(snapshot.affected),
                },
            ), snapshot

        return self._run_mutation("full_replace", base_version, payload, operation, debug_fault)

    # ------------------------------------------------------------------
    # Public reads and control
    # ------------------------------------------------------------------
    def get_lease(self, lock_key: str) -> dict[str, Any]:
        lock_key = _require_text(lock_key, "lock_key")
        self._sweep_if_due()
        snapshot = self._snapshot_ref()
        lease = snapshot.leases.get(lock_key)
        if lease is None:
            raise ApiError(
                "E_LEASE_NOT_FOUND",
                f"lease {lock_key!r} was not found",
                snapshot_version=snapshot.version,
                details={"lock_key": lock_key},
            )
        parent_links = [dependency.to_dict() for dependency in snapshot.parents.get(lock_key, ())]
        children = [link.to_dict() for link in snapshot.direct_children(lock_key)]
        descendants = sorted(snapshot.descendant_keys([lock_key]))
        return {
            "ok": True,
            "snapshot_version": snapshot.version,
            "lease": lease.to_dict(),
            "dependency_chain": {
                "parents": parent_links,
                "ancestors": list(snapshot.ancestors.get(lock_key, ())),
                "children": children,
                "descendants": descendants,
            },
            "derived_index_version": snapshot.derived_version.get(lock_key),
        }

    def get_current_snapshot(self) -> dict[str, Any]:
        self._sweep_if_due()
        snapshot = self._snapshot_ref()
        return {
            "ok": True,
            "snapshot_version": snapshot.version,
            "snapshot": snapshot.to_payload(),
        }

    def get_snapshot_version(self, version: int) -> dict[str, Any]:
        version = _non_negative_int(version, "version")
        with closing(self.db.connect()) as conn:
            payload = self.db.get_snapshot_payload(conn, version)
        if payload is None:
            if version == 0:
                payload = Snapshot.empty(0).to_payload()
            else:
                raise ApiError(
                    "E_SNAPSHOT_NOT_FOUND",
                    f"snapshot version {version} was not found",
                    snapshot_version=self.snapshot_version,
                )
        return {"ok": True, "snapshot_version": version, "snapshot": payload}

    def get_recovery_report(self) -> dict[str, Any]:
        with closing(self.db.connect()) as conn:
            row = conn.execute(
                "SELECT report_json FROM recovery_reports ORDER BY report_id DESC LIMIT 1"
            ).fetchone()
        report = json.loads(row["report_json"]) if row is not None else dict(self._recovery_report)
        return {
            "ok": True,
            "snapshot_version": self.snapshot_version,
            "report": report,
        }

    def get_resource(self, lock_key: str) -> dict[str, Any]:
        lock_key = _require_text(lock_key, "lock_key")
        with closing(self.db.connect()) as conn:
            row = conn.execute(
                "SELECT lock_key, payload_json, revision, updated_at_ms "
                "FROM resources WHERE lock_key = ?",
                (lock_key,),
            ).fetchone()
        if row is None:
            raise ApiError(
                "E_LEASE_NOT_FOUND",
                f"resource {lock_key!r} was not found",
                snapshot_version=self.snapshot_version,
            )
        return {
            "ok": True,
            "snapshot_version": self.snapshot_version,
            "resource": {
                "lock_key": row["lock_key"],
                "payload": json.loads(row["payload_json"]),
                "revision": int(row["revision"]),
                "updated_at_ms": int(row["updated_at_ms"]),
            },
        }

    def advance_clock(self, amount_ms: int) -> dict[str, Any]:
        amount_ms = _non_negative_int(amount_ms, "advance_ms")
        with self._serial:
            now = self.clock.advance_ms(amount_ms)
            conn = self.db.connect()
            try:
                conn.execute("BEGIN IMMEDIATE")
                self.db.set_meta(conn, "clock_high_water_ms", now)
                conn.commit()
            except Exception:
                conn.rollback()
                raise
            finally:
                conn.close()
        return {
            "ok": True,
            "snapshot_version": self.snapshot_version,
            "advanced_ms": amount_ms,
            "now_ms": now,
        }

    def sweep(self) -> dict[str, Any]:
        self._refresh_if_database_newer()
        now = self.clock.now_ms()
        if not _has_due_expiry(self._snapshot_ref(), now):
            return {
                "ok": True,
                "snapshot_version": self.snapshot_version,
                "affected_leases": [],
                "expired_leases": [],
            }

        def operation(conn: sqlite3.Connection, current: Snapshot, tx_now: int):
            patches = _expiry_patches(current, tx_now)
            if not patches:
                return {
                    "ok": True,
                    "snapshot_version": current.version,
                    "affected_leases": [],
                    "expired_leases": [],
                }, None
            snapshot = _finalize_snapshot(
                current,
                patches=patches,
                kind="expire",
                now=tx_now,
                seed_keys=set(patches),
            )
            return _mutation_result(
                "expired",
                snapshot,
                current.version,
                extra={
                    "expired_leases": sorted(patches),
                    "affected_leases": list(snapshot.affected),
                },
            ), snapshot

        return self._run_mutation("expire", None, {"trigger": "explicit"}, operation)

    # ------------------------------------------------------------------
    # Serialized transaction machinery
    # ------------------------------------------------------------------
    def _run_mutation(
        self,
        kind: str,
        base_version: int | None,
        payload: dict[str, Any],
        operation: Callable[[sqlite3.Connection, Snapshot, int], tuple[dict[str, Any], Snapshot | None]],
        debug_fault: str | None = None,
    ) -> dict[str, Any]:
        if debug_fault and not self.enable_faults:
            raise ApiError("E_INVALID_REQUEST", "fault injection is disabled")
        intent_id = uuid.uuid4().hex
        created_at_ms = self.clock.now_ms()
        intent_conn = self.db.connect()
        try:
            intent_conn.execute("BEGIN IMMEDIATE")
            self.db.insert_intent(
                intent_conn,
                intent_id=intent_id,
                kind=kind,
                base_version=base_version,
                target_version=None,
                payload=payload,
                created_at_ms=created_at_ms,
                pid=os.getpid(),
            )
            persisted_high_water = int(
                self.db.get_meta(intent_conn, "clock_high_water_ms", "0")
            )
            self.db.set_meta(
                intent_conn,
                "clock_high_water_ms",
                max(created_at_ms, persisted_high_water),
            )
            intent_conn.commit()
        except BaseException:
            intent_conn.rollback()
            raise
        finally:
            intent_conn.close()

        if debug_fault == "after_intent":
            self._fire_fault(
                debug_fault,
                {"intent_id": intent_id, "kind": kind, "base_version": base_version},
            )

        try:
            with self._serial:
                conn = self.db.connect()
                new_snapshot: Snapshot | None = None
                try:
                    conn.execute("BEGIN IMMEDIATE")
                    self._refresh_locked(conn)
                    now = self.clock.now_ms()
                    result, new_snapshot = operation(conn, self._snapshot, now)
                    if new_snapshot is not None:
                        self._persist_snapshot(conn, self._snapshot, new_snapshot)
                    if debug_fault == "before_commit":
                        self._fire_fault(
                            debug_fault,
                            {
                                "intent_id": intent_id,
                                "kind": kind,
                                "snapshot_version": new_snapshot.version if new_snapshot else self._snapshot.version,
                            },
                        )
                    self.db.complete_intent(
                        conn,
                        intent_id,
                        new_snapshot.version if new_snapshot is not None else self._snapshot.version,
                        now,
                    )
                    self.db.set_meta(conn, "clock_high_water_ms", max(now, self.clock.high_water_ms))
                    conn.commit()
                    if new_snapshot is not None:
                        self._snapshot = new_snapshot
                    if debug_fault == "after_commit":
                        self._fire_fault(
                            debug_fault,
                            {
                                "intent_id": intent_id,
                                "kind": kind,
                                "snapshot_version": self._snapshot.version,
                            },
                        )
                    return result
                except BaseException:
                    try:
                        conn.rollback()
                    except Exception:
                        pass
                    raise
                finally:
                    conn.close()
        except ApiError as exc:
            self._abort_intent(intent_id, exc.code, exc.message)
            if exc.snapshot_version is None:
                exc.snapshot_version = self.snapshot_version
            raise
        except Exception as exc:
            self._abort_intent(intent_id, "E_INTERNAL", str(exc))
            raise ApiError(
                "E_INTERNAL",
                f"internal mutation failure: {exc}",
                snapshot_version=self.snapshot_version,
            ) from exc

    def _abort_intent(self, intent_id: str, error_code: str, message: str) -> None:
        conn = self.db.connect()
        try:
            self.db.abort_intent(
                conn,
                intent_id,
                completed_at_ms=self.clock.now_ms(),
                error_code=error_code,
                message=message,
            )
        finally:
            conn.close()

    def _fire_fault(self, point: str, context: dict[str, Any]) -> None:
        if self._fault_hook is not None:
            self._fault_hook(point, context)
            return
        os._exit(86)

    def _persist_snapshot(self, conn: sqlite3.Connection, old: Snapshot, new: Snapshot) -> None:
        removed = sorted(set(old.leases) - set(new.leases))
        for key in removed:
            conn.execute("DELETE FROM lease_dependencies WHERE lock_key = ?", (key,))
            conn.execute("DELETE FROM leases WHERE lock_key = ?", (key,))

        dirty = sorted(
            key
            for key, lease in new.leases.items()
            if old.leases.get(key) != lease
        )
        for key in dirty:
            lease = new.leases[key]
            _upsert_lease(conn, lease)
            old_dependencies = old.leases.get(key).dependencies if key in old.leases else ()
            if key not in old.leases or old_dependencies != lease.dependencies:
                conn.execute("DELETE FROM lease_dependencies WHERE lock_key = ?", (key,))
                for dependency in lease.dependencies:
                    conn.execute(
                        "INSERT INTO lease_dependencies(lock_key, parent_key, mode) VALUES (?, ?, ?)",
                        (key, dependency.parent_key, dependency.mode.value),
                    )
            conn.execute(
                "INSERT INTO lock_versions(lock_key, last_version) VALUES (?, ?) "
                "ON CONFLICT(lock_key) DO UPDATE SET last_version = "
                "MAX(lock_versions.last_version, excluded.last_version)",
                (key, lease.lease_version),
            )
            _ensure_resource(conn, key, {}, new.created_at_ms)

        payload = new.to_payload()
        self.db.insert_snapshot(conn, payload)
        self.db.set_meta(conn, "snapshot_version", new.version)

    def _snapshot_ref(self) -> Snapshot:
        with self._serial:
            return self._snapshot

    def _refresh_if_database_newer(self) -> None:
        with self._serial:
            conn = self.db.connect()
            try:
                self._refresh_locked(conn)
            finally:
                conn.close()

    def _refresh_locked(self, conn: sqlite3.Connection) -> Snapshot:
        db_version = int(self.db.get_meta(conn, "snapshot_version", "0"))
        if db_version <= self._snapshot.version:
            return self._snapshot
        payload = self.db.get_snapshot_payload(conn, db_version)
        if payload is None:
            payload = self.db.get_latest_snapshot_payload(conn)
        if payload is not None:
            self._snapshot = Snapshot.from_payload(payload)
        return self._snapshot

    def _sweep_if_due(self) -> None:
        self._refresh_if_database_newer()
        now = self.clock.now_ms()
        if _has_due_expiry(self._snapshot_ref(), now):
            self.sweep()

    # ------------------------------------------------------------------
    # Recovery
    # ------------------------------------------------------------------
    def _recover(self) -> None:
        with self._serial:
            conn = self.db.connect()
            try:
                conn.execute("BEGIN IMMEDIATE")
                now = self.clock.now_ms()
                started_rows = conn.execute(
                    "SELECT intent_id, kind, base_version, target_version, created_at_ms, pid "
                    "FROM intents WHERE status = 'STARTED' ORDER BY created_at_ms, intent_id"
                ).fetchall()
                aborted: list[dict[str, Any]] = []
                for row in started_rows:
                    self.db.abort_intent(
                        conn,
                        str(row["intent_id"]),
                        completed_at_ms=now,
                        error_code=PENDING_ABORTED_CODE,
                        message="process terminated before the intent transaction committed",
                    )
                    aborted.append(
                        {
                            "intent_id": row["intent_id"],
                            "kind": row["kind"],
                            "base_version": row["base_version"],
                            "target_version": row["target_version"],
                            "created_at_ms": row["created_at_ms"],
                            "pid": row["pid"],
                        }
                    )

                meta_version = int(self.db.get_meta(conn, "snapshot_version", "0"))
                checkpoint = self.db.get_snapshot_payload(conn, meta_version)
                if checkpoint is None:
                    checkpoint = self.db.get_latest_snapshot_payload(conn)
                    if checkpoint is not None:
                        meta_version = int(checkpoint["version"])
                        self.db.set_meta(conn, "snapshot_version", meta_version)
                if checkpoint is None:
                    snapshot = Snapshot.empty(0)
                else:
                    snapshot = Snapshot.from_payload(checkpoint)

                materialized_payload = _load_materialized_leases(conn)
                checkpoint_leases = [
                    Lease.from_dict(raw).to_dict()
                    for raw in sorted(checkpoint.get("leases", []), key=lambda item: item["lock_key"])
                ] if checkpoint is not None else []
                repaired = canonical_json(materialized_payload) != canonical_json(checkpoint_leases)
                if repaired:
                    _rewrite_materialized(conn, snapshot)

                high_water = max(
                    now,
                    int(self.db.get_meta(conn, "clock_high_water_ms", "0")),
                )
                self.db.set_meta(conn, "clock_high_water_ms", high_water)

                status_counts = {
                    str(row["status"]): int(row["count"])
                    for row in conn.execute(
                        "SELECT status, COUNT(*) AS count FROM intents GROUP BY status"
                    ).fetchall()
                }
                generated_at_ms = max(now, high_water)
                report = {
                    "report_type": "restart_recovery",
                    "generated_at_ms": generated_at_ms,
                    "pid": os.getpid(),
                    "complete_exit_report": False,
                    "notice": (
                        "This report was reconstructed from persisted SQLite facts after a restart. "
                        "A forced process termination cannot produce a graceful exit report."
                    ),
                    "recovered_snapshot_version": snapshot.version,
                    "checkpoint_checksum_verified": checkpoint is not None,
                    "snapshot_restored_from_checkpoint": repaired,
                    "aborted_intents": aborted,
                    "aborted_intent_count": len(aborted),
                    "intent_status_counts": status_counts,
                }
                conn.execute(
                    "INSERT INTO recovery_reports(generated_at_ms, report_json) VALUES (?, ?)",
                    (generated_at_ms, canonical_json(report)),
                )
                conn.commit()
                self._snapshot = snapshot
                self._recovery_report = report
            except BaseException:
                conn.rollback()
                raise
            finally:
                conn.close()


def _require_text(value: Any, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ApiError("E_INVALID_REQUEST", f"{field} must be a non-empty string")
    return value.strip()


def _positive_int(value: Any, field: str) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise ApiError("E_INVALID_REQUEST", f"{field} must be an integer") from exc
    if parsed <= 0:
        raise ApiError("E_INVALID_REQUEST", f"{field} must be greater than zero")
    return parsed


def _non_negative_int(value: Any, field: str) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise ApiError("E_INVALID_REQUEST", f"{field} must be an integer") from exc
    if parsed < 0:
        raise ApiError("E_INVALID_REQUEST", f"{field} must be non-negative")
    return parsed


def _parse_dependencies(raw: Iterable[Any] | None):
    try:
        return normalize_dependencies(raw)
    except ValueError as exc:
        raise ApiError("E_INVALID_REQUEST", str(exc)) from exc


def _lease_usable(lease: Lease, now: int) -> bool:
    return lease.state == LeaseState.ACTIVE and lease.expires_at_ms > now


def _lease_blocks_acquisition(lease: Lease, now: int) -> bool:
    return lease.state == LeaseState.ACTIVE and lease.expires_at_ms > now


def _effective_release_reason(lease: Lease, now: int) -> str | None:
    if lease.state == LeaseState.RELEASED:
        return lease.release_reason
    if lease.expires_at_ms <= now:
        return RELEASE_LEASE_EXPIRED
    return None


def _expiry_patches(snapshot: Snapshot, now: int) -> dict[str, Lease]:
    patches: dict[str, Lease] = {}
    for key, lease in snapshot.leases.items():
        if lease.state == LeaseState.ACTIVE and lease.expires_at_ms <= now:
            patches[key] = replace(
                lease,
                lease_version=lease.lease_version + 1,
                state=LeaseState.RELEASED,
                released_at_ms=now,
                release_reason=RELEASE_LEASE_EXPIRED,
                dependency_expired=lease.dependency_expired,
            )
    return patches


def _has_due_expiry(snapshot: Snapshot, now: int) -> bool:
    return any(
        lease.state == LeaseState.ACTIVE and lease.expires_at_ms <= now
        for lease in snapshot.leases.values()
    )


def _finalize_snapshot(
    current: Snapshot,
    *,
    patches: Mapping[str, Lease],
    kind: str,
    now: int,
    seed_keys: Iterable[str],
) -> Snapshot:
    if not patches:
        return current
    snapshot = current.evolve(
        version=current.version + 1,
        patches=patches,
        kind=kind,
        created_at_ms=now,
        seed_keys=set(seed_keys),
    )
    return _normalize_dependencies(snapshot, now)


def _normalize_dependencies(snapshot: Snapshot, now: int) -> Snapshot:
    updates: dict[str, Lease] = {}
    working = dict(snapshot.leases)
    order = snapshot.topological_affected()
    for key in order:
        lease = working.get(key)
        if lease is None or lease.state != LeaseState.ACTIVE:
            continue
        strong_bad = False
        any_bad = False
        for dependency in lease.dependencies:
            parent = working.get(dependency.parent_key)
            parent_bad = (
                parent is None
                or parent.state != LeaseState.ACTIVE
                or parent.expires_at_ms <= now
            )
            if parent_bad:
                any_bad = True
                if dependency.mode == DependencyMode.STRONG:
                    strong_bad = True
        if strong_bad:
            updated = replace(
                lease,
                lease_version=lease.lease_version + 1,
                state=LeaseState.RELEASED,
                released_at_ms=now,
                release_reason=RELEASE_DEPENDENCY_EXPIRED,
                dependency_expired=True,
            )
            working[key] = updated
            updates[key] = updated
        elif lease.dependency_expired != any_bad:
            updated = replace(lease, dependency_expired=any_bad)
            working[key] = updated
            updates[key] = updated
    return snapshot.replace_leases(updates)


def _leases_with_patch(
    leases: Mapping[str, Lease],
    key: str,
    dependencies: Iterable[Any],
    lease: Lease | None,
) -> dict[str, Lease]:
    result = dict(leases)
    normalized_dependencies = tuple(
        sorted(dependencies, key=lambda dependency: dependency.parent_key)
    )
    if lease is None:
        old = leases.get(key)
        if old is None:
            result[key] = Lease(
                lock_key=key,
                holder="__probe__",
                lease_version=1,
                acquired_at_ms=0,
                ttl_ms=1,
                expires_at_ms=1,
                dependencies=normalized_dependencies,
            )
        else:
            result[key] = replace(old, dependencies=normalized_dependencies)
    else:
        result[key] = lease
    return result


def _leases_with_patches(
    leases: Mapping[str, Lease], patches: Mapping[str, Lease]
) -> dict[str, Lease]:
    result = dict(leases)
    result.update(patches)
    return result


def _next_lease_version(snapshot: Snapshot, key: str) -> int:
    lease = snapshot.leases.get(key)
    return (lease.lease_version if lease is not None else 0) + 1


def _persisted_last_lease_version(conn: sqlite3.Connection, key: str) -> int:
    row = conn.execute(
        "SELECT last_version FROM lock_versions WHERE lock_key = ?", (key,)
    ).fetchone()
    return int(row["last_version"]) if row is not None else 0


def _mutation_result(
    status: str,
    snapshot: Snapshot,
    base_version: int,
    *,
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    result: dict[str, Any] = {
        "ok": True,
        "status": status,
        "snapshot_version": snapshot.version,
        "previous_snapshot_version": base_version,
        "affected_leases": list(snapshot.affected),
    }
    if extra:
        result.update(extra)
    return result


def _upsert_lease(conn: sqlite3.Connection, lease: Lease) -> None:
    conn.execute(
        "INSERT INTO leases(lock_key, holder, lease_version, acquired_at_ms, ttl_ms, "
        "expires_at_ms, state, released_at_ms, release_reason, dependency_expired) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?) "
        "ON CONFLICT(lock_key) DO UPDATE SET holder = excluded.holder, "
        "lease_version = excluded.lease_version, acquired_at_ms = excluded.acquired_at_ms, "
        "ttl_ms = excluded.ttl_ms, expires_at_ms = excluded.expires_at_ms, "
        "state = excluded.state, released_at_ms = excluded.released_at_ms, "
        "release_reason = excluded.release_reason, "
        "dependency_expired = excluded.dependency_expired",
        (
            lease.lock_key,
            lease.holder,
            lease.lease_version,
            lease.acquired_at_ms,
            lease.ttl_ms,
            lease.expires_at_ms,
            lease.state.value,
            lease.released_at_ms,
            lease.release_reason,
            1 if lease.dependency_expired else 0,
        ),
    )


def _ensure_resource(
    conn: sqlite3.Connection, lock_key: str, payload: dict[str, Any], now: int
) -> None:
    conn.execute(
        "INSERT OR IGNORE INTO resources(lock_key, payload_json, revision, updated_at_ms) "
        "VALUES (?, ?, 0, ?)",
        (lock_key, canonical_json(payload), now),
    )


def _load_materialized_leases(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    dependency_rows = conn.execute(
        "SELECT lock_key, parent_key, mode FROM lease_dependencies "
        "ORDER BY lock_key, parent_key"
    ).fetchall()
    deps_by_key: dict[str, list[dict[str, str]]] = {}
    for row in dependency_rows:
        deps_by_key.setdefault(str(row["lock_key"]), []).append(
            {"lock_key": row["parent_key"], "mode": row["mode"]}
        )
    rows = conn.execute("SELECT * FROM leases ORDER BY lock_key").fetchall()
    result: list[dict[str, Any]] = []
    for row in rows:
        lease = Lease(
            lock_key=str(row["lock_key"]),
            holder=str(row["holder"]),
            lease_version=int(row["lease_version"]),
            acquired_at_ms=int(row["acquired_at_ms"]),
            ttl_ms=int(row["ttl_ms"]),
            expires_at_ms=int(row["expires_at_ms"]),
            state=LeaseState(str(row["state"])),
            released_at_ms=(
                int(row["released_at_ms"]) if row["released_at_ms"] is not None else None
            ),
            release_reason=row["release_reason"],
            dependency_expired=bool(row["dependency_expired"]),
            dependencies=normalize_dependencies(deps_by_key.get(str(row["lock_key"]), [])),
        )
        result.append(lease.to_dict())
    return result


def _rewrite_materialized(conn: sqlite3.Connection, snapshot: Snapshot) -> None:
    conn.execute("DELETE FROM lease_dependencies")
    conn.execute("DELETE FROM leases")
    for key in sorted(snapshot.leases):
        lease = snapshot.leases[key]
        _upsert_lease(conn, lease)
        for dependency in lease.dependencies:
            conn.execute(
                "INSERT INTO lease_dependencies(lock_key, parent_key, mode) VALUES (?, ?, ?)",
                (key, dependency.parent_key, dependency.mode.value),
            )
        conn.execute(
            "INSERT INTO lock_versions(lock_key, last_version) VALUES (?, ?) "
            "ON CONFLICT(lock_key) DO UPDATE SET last_version = "
            "MAX(lock_versions.last_version, excluded.last_version)",
            (key, lease.lease_version),
        )
        _ensure_resource(conn, key, {}, snapshot.created_at_ms)


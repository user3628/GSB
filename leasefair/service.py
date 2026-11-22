"""Lease-lock application service with durable intent and recovery semantics."""

from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import uuid
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Callable, Sequence

from .clock import Clock
from .errors import (
    E_CONFLICTING_REQUEST,
    E_INVALID_REQUEST,
    E_LEASE_EXPIRED,
    E_LOCK_HELD,
    E_LOCK_NOT_HELD,
    E_NOT_HOLDER,
    E_STALE_LEASE,
    OK,
    LeaseError,
)
from .fixedpoint import decimal_to_q, q_to_string
from .metrics import (
    DEFAULT_FAIRNESS_RELATIVE_TOLERANCE,
    DEFAULT_REBALANCE_RELATIVE_TOLERANCE,
    Candidate,
    build_arbitration,
    build_metrics,
    fixed_metadata,
)
from .storage import Store
from .time_contract import expiry_from_grant, ttl_precision_report

MAX_TTL_MS = 31_536_000_000
MAX_TEXT_LENGTH = 512
MAX_REQUEST_ID_LENGTH = 256


def _pid_is_alive(pid: int) -> bool:
    """Best-effort cross-platform process liveness check for recovery fencing."""

    if pid <= 0:
        return False
    if os.name == "nt":
        try:
            import ctypes

            process_query_limited_information = 0x1000
            kernel32 = ctypes.windll.kernel32  # type: ignore[attr-defined]
            handle = kernel32.OpenProcess(process_query_limited_information, False, pid)
            if handle:
                kernel32.CloseHandle(handle)
                return True
            return False
        except Exception:
            return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except OSError:
        return False
    return True


@dataclass(frozen=True, slots=True)
class PreparedResult:
    intent_id: int
    cached_response: dict[str, Any] | None


@dataclass(frozen=True, slots=True)
class HandlerResult:
    response: dict[str, Any]
    mutated: bool
    intended_state: dict[str, Any]


def _canonical_decimal(text: str | Decimal | int | float) -> tuple[str, int]:
    try:
        value = text if isinstance(text, Decimal) else Decimal(str(text))
    except (InvalidOperation, ValueError) as exc:
        raise LeaseError(E_INVALID_REQUEST, "weight must be a decimal number") from exc
    if not value.is_finite() or value <= 0:
        raise LeaseError(E_INVALID_REQUEST, "weight must be finite and greater than zero")
    q, _ = decimal_to_q(value)
    if q <= 0:
        raise LeaseError(E_INVALID_REQUEST, "weight is smaller than 1e-9 and quantizes to zero")
    rendered = format(value, "f")
    if "." in rendered:
        rendered = rendered.rstrip("0").rstrip(".")
    if rendered in {"", "-0"}:
        rendered = "0"
    return rendered, q


def _text(value: str, name: str, max_length: int = MAX_TEXT_LENGTH) -> str:
    if not isinstance(value, str) or not value:
        raise LeaseError(E_INVALID_REQUEST, f"{name} must be a non-empty string")
    if len(value) > max_length:
        raise LeaseError(E_INVALID_REQUEST, f"{name} exceeds {max_length} characters")
    if any(ord(char) < 32 for char in value):
        raise LeaseError(E_INVALID_REQUEST, f"{name} must not contain control characters")
    return value


def _ttl(value: int, *, name: str = "ttl_ms") -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise LeaseError(E_INVALID_REQUEST, f"{name} must be an integer")
    if value <= 0 or value > MAX_TTL_MS:
        raise LeaseError(E_INVALID_REQUEST, f"{name} must be in [1, {MAX_TTL_MS}]")
    return value


def _version(value: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise LeaseError(E_INVALID_REQUEST, "version must be a positive integer")
    return value


def _payload_hash(payload: dict[str, Any]) -> str:
    canonical = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


class LeaseService:
    def __init__(
        self,
        db_path: str | os.PathLike[str],
        *,
        clock: Clock | Callable[[], int] | None = None,
        rebalance_relative_tolerance: Decimal | None = None,
        fairness_relative_tolerance: Decimal | None = None,
        auto_recover: bool = True,
    ) -> None:
        self.store = Store(db_path)
        self.store.initialize()
        if clock is None:
            self.clock = Clock(offset_ms=int(os.environ.get("LEASE_CLOCK_OFFSET_MS", "0")))
        elif callable(clock):
            self.clock = clock
        else:
            self.clock = clock
        rebalance_tolerance = rebalance_relative_tolerance
        if rebalance_tolerance is None:
            rebalance_tolerance = Decimal(
                os.environ.get(
                    "LEASE_REBALANCE_RELATIVE_TOLERANCE",
                    str(DEFAULT_REBALANCE_RELATIVE_TOLERANCE),
                )
            )
        fairness_tolerance = fairness_relative_tolerance
        if fairness_tolerance is None:
            fairness_tolerance = Decimal(
                os.environ.get(
                    "LEASE_FAIRNESS_RELATIVE_TOLERANCE",
                    str(DEFAULT_FAIRNESS_RELATIVE_TOLERANCE),
                )
            )
        self.rebalance_tolerance_q, _ = decimal_to_q(rebalance_tolerance)
        self.fairness_tolerance_q, _ = decimal_to_q(fairness_tolerance)
        self.last_recovery: dict[str, Any] | None = None
        if auto_recover:
            self.last_recovery = self.recover_pending_intents()

    def now_ms(self) -> int:
        method = getattr(self.clock, "now_ms", None)
        value = method() if callable(method) else self.clock()  # type: ignore[operator]
        return int(value)

    @property
    def clock_offset_ms(self) -> int:
        return int(getattr(self.clock, "offset_ms", 0))

    def register_key(self, lock_key: str, *, now_ms: int | None = None) -> dict[str, Any]:
        lock_key = _text(lock_key, "lock_key")
        now = self.now_ms() if now_ms is None else int(now_ms)
        with self.store.write() as con:
            before = self.store.registered_key_count(con)
            self.store.ensure_registered(con, lock_key, now)
            self.store.event(
                con,
                lock_key=lock_key,
                holder_id=None,
                version=None,
                event_type="registered",
                event_at_ms=now,
                request_id=None,
                details={},
            )
            after = self.store.registered_key_count(con)
            changed = after != before
            if changed:
                revision, _ = self._refresh_metric_cache(con, now)
            else:
                revision = self.store.revision(con)
        return {
            "ok": True,
            "code": OK,
            "lock_key": lock_key,
            "registered_keys": after,
            "changed": changed,
            "revision": revision,
            "now_ms": now,
        }

    def acquire(
        self,
        lock_key: str,
        *,
        request_id: str,
        holder_id: str,
        ttl_ms: int,
        weight: str | Decimal | int | float,
    ) -> dict[str, Any]:
        lock_key = _text(lock_key, "lock_key")
        request_id = _text(request_id, "request_id", MAX_REQUEST_ID_LENGTH)
        holder_id = _text(holder_id, "holder_id")
        ttl_ms = _ttl(ttl_ms)
        weight_text, weight_q = _canonical_decimal(weight)
        payload = {
            "operation": "acquire",
            "lock_key": lock_key,
            "request_id": request_id,
            "holder_id": holder_id,
            "ttl_ms": ttl_ms,
            "weight": weight_text,
            "weight_q": weight_q,
        }
        return self._execute(
            operation="acquire",
            lock_key=lock_key,
            request_id=request_id,
            payload=payload,
            handler=self._handle_acquire,
        )

    def renew(
        self,
        lock_key: str,
        *,
        request_id: str,
        holder_id: str,
        version: int,
        ttl_ms: int | None = None,
    ) -> dict[str, Any]:
        lock_key = _text(lock_key, "lock_key")
        request_id = _text(request_id, "request_id", MAX_REQUEST_ID_LENGTH)
        holder_id = _text(holder_id, "holder_id")
        version = _version(version)
        if ttl_ms is not None:
            ttl_ms = _ttl(ttl_ms)
        payload = {
            "operation": "renew",
            "lock_key": lock_key,
            "request_id": request_id,
            "holder_id": holder_id,
            "version": version,
            "ttl_ms": ttl_ms,
        }
        return self._execute(
            operation="renew",
            lock_key=lock_key,
            request_id=request_id,
            payload=payload,
            handler=self._handle_renew,
        )

    def release(
        self,
        lock_key: str,
        *,
        request_id: str,
        holder_id: str,
        version: int,
    ) -> dict[str, Any]:
        lock_key = _text(lock_key, "lock_key")
        request_id = _text(request_id, "request_id", MAX_REQUEST_ID_LENGTH)
        holder_id = _text(holder_id, "holder_id")
        version = _version(version)
        payload = {
            "operation": "release",
            "lock_key": lock_key,
            "request_id": request_id,
            "holder_id": holder_id,
            "version": version,
        }
        return self._execute(
            operation="release",
            lock_key=lock_key,
            request_id=request_id,
            payload=payload,
            handler=self._handle_release,
        )

    def acquire_best(
        self,
        lock_key: str,
        *,
        request_id: str,
        candidates: Sequence[dict[str, Any]],
    ) -> dict[str, Any]:
        lock_key = _text(lock_key, "lock_key")
        request_id = _text(request_id, "request_id", MAX_REQUEST_ID_LENGTH)
        normalized_candidates = self._normalize_candidates(candidates)
        payload = {
            "operation": "acquire_best",
            "lock_key": lock_key,
            "request_id": request_id,
            "candidates": normalized_candidates,
        }
        return self._execute(
            operation="acquire_best",
            lock_key=lock_key,
            request_id=request_id,
            payload=payload,
            handler=self._handle_acquire_best,
        )

    def arbitrate(
        self,
        lock_key: str,
        *,
        candidates: Sequence[dict[str, Any]],
        now_ms: int | None = None,
    ) -> dict[str, Any]:
        lock_key = _text(lock_key, "lock_key")
        normalized = self._normalize_candidates(candidates)
        now = self.now_ms() if now_ms is None else int(now_ms)
        candidate_models = [
            Candidate(
                holder_id=item["holder_id"],
                weight_text=item["weight"],
                weight_q=item["weight_q"],
                ttl_ms=item["ttl_ms"],
            )
            for item in normalized
        ]
        with self.store.read() as con:
            held = self._held_lease(con, lock_key, now)
            if held is not None:
                return self._held_response(lock_key, held, now)
            active = self.store.active_leases(con, now)
            holders = self.store.holders(con)
            arbitration = build_arbitration(
                active,
                holders,
                candidate_models,
                now_ms=now,
                relative_tolerance_q=self.fairness_tolerance_q,
            )
        return {
            "ok": True,
            "code": OK,
            "mode": "dry_run",
            "lock_key": lock_key,
            "now_ms": now,
            "arbitration": arbitration,
        }

    def get_lock(self, lock_key: str, *, now_ms: int | None = None) -> dict[str, Any]:
        lock_key = _text(lock_key, "lock_key")
        now = self.now_ms() if now_ms is None else int(now_ms)
        with self.store.read() as con:
            snapshot = self.store.snapshot_lock(con, lock_key)
        lease = snapshot.get("lease")
        active = bool(
            lease
            and lease.get("status") == "active"
            and int(lease.get("expires_at_ms", 0)) > now
        )
        response: dict[str, Any] = {
            "ok": True,
            "code": OK,
            "lock_key": lock_key,
            "now_ms": now,
            "version": snapshot["version"],
            "state": "held" if active else "available",
            "registered": snapshot["registered"],
        }
        if lease is not None:
            response["lease"] = self._lease_response(lease, now)
            if active:
                response["remaining_ttl_ms"] = max(0, int(lease["expires_at_ms"]) - now)
        return response

    def metrics(self, *, now_ms: int | None = None) -> dict[str, Any]:
        now = self.now_ms() if now_ms is None else int(now_ms)
        with self.store.read() as con:
            return self._metrics_in_transaction(con, now)

    def recompute_metrics(self) -> dict[str, Any]:
        now = self.now_ms()
        request_id = f"metrics-recompute:{uuid.uuid4()}"
        payload = {"operation": "metrics_recompute", "request_id": request_id}
        payload_hash = _payload_hash(payload)
        with self.store.write() as con:
            intent_id = self.store.create_prepared_intent(
                con,
                request_id=request_id,
                operation="metrics_recompute",
                lock_key="*",
                payload_hash=payload_hash,
                payload=payload,
                prior_state={"basis": "active lease facts"},
                now_ms=now,
            )
        self._maybe_crash("metrics_recompute")
        with self.store.write() as con:
            expired = self.store.expire_due(con, now)
            revision, metrics = self._refresh_metric_cache(con, now)
            self.store.finish_intent(
                con,
                intent_id,
                phase="committed",
                intended_state={"revision": revision, "expired_keys": expired},
                now_ms=now,
            )
        return {
            **metrics,
            "revision": revision,
            "expired_keys": expired,
            "intent_id": intent_id,
        }

    def recovery_report(self) -> dict[str, Any] | None:
        with self.store.read() as con:
            return self.store.latest_recovery(con)

    def events(
        self, *, lock_key: str | None = None, limit: int = 200
    ) -> dict[str, Any]:
        limit = max(1, min(int(limit), 2000))
        with self.store.read() as con:
            if lock_key is None:
                rows = con.execute(
                    "SELECT * FROM lease_events ORDER BY id DESC LIMIT ?", (limit,)
                ).fetchall()
            else:
                rows = con.execute(
                    "SELECT * FROM lease_events WHERE lock_key=? ORDER BY id DESC LIMIT ?",
                    (lock_key, limit),
                ).fetchall()
        events = []
        for row in rows:
            event = dict(row)
            event["details"] = json.loads(event.pop("details_json"))
            events.append(event)
        events.reverse()
        return {
            "ok": True,
            "code": OK,
            "lock_key": lock_key,
            "events": events,
        }

    def intentions(
        self, *, phase: str | None = None, limit: int = 200
    ) -> dict[str, Any]:
        limit = max(1, min(int(limit), 2000))
        with self.store.read() as con:
            if phase is None:
                rows = con.execute(
                    "SELECT * FROM intentions ORDER BY id DESC LIMIT ?", (limit,)
                ).fetchall()
            else:
                rows = con.execute(
                    "SELECT * FROM intentions WHERE phase=? ORDER BY id DESC LIMIT ?",
                    (phase, limit),
                ).fetchall()
        records = []
        for row in rows:
            record = dict(row)
            for field in ("payload_json", "prior_state_json", "intended_state_json"):
                record[field.removesuffix("_json")] = json.loads(record.pop(field))
            records.append(record)
        records.reverse()
        return {"ok": True, "code": OK, "phase": phase, "intentions": records}

    def diagnostics(self, *, now_ms: int | None = None) -> dict[str, Any]:
        now = self.now_ms() if now_ms is None else int(now_ms)
        with self.store.read() as con:
            cache = self.store.get_metric_cache(con)
            recovery = self.store.latest_recovery(con)
            revision = self.store.revision(con)
            registered = self.store.registered_key_count(con)
            intent_rows = con.execute(
                "SELECT * FROM intentions ORDER BY id DESC LIMIT 20"
            ).fetchall()
        intent_log = []
        for row in reversed(intent_rows):
            record = dict(row)
            for field in ("payload_json", "prior_state_json", "intended_state_json"):
                record[field.removesuffix("_json")] = json.loads(record.pop(field))
            intent_log.append(record)
        return {
            "ok": True,
            "code": OK,
            "now_ms": now,
            "revision": revision,
            "fixed_point": fixed_metadata(),
            "metric_definitions": {
                "actual_lease_share": "per-holder sum of active lease weight_q / sum of all active lease weight_q; largest-remainder Q normalization",
                "target_weight_share": "holder declared weight normalized over holders that currently have active leases; largest-remainder Q normalization",
                "weighted_fairness": "1 - sum(target_share * abs(actual_share-target_share)), Q9 with compensated summation",
                "renewal_margin_ratio": "min(remaining_ms, ttl_ms) / ttl_ms, Q9",
                "cluster_occupancy": "active occupied lock keys / registered lock keys, Q9",
                "rebalance_relative_deviation": "abs(actual_share-target_share) / target_share; trigger iff > configured relative tolerance",
                "arbitration_priority": "projected weighted fairness after hypothetically granting the candidate; higher wins, relative-tolerance tie to holder_id ascending",
            },
            "tolerances": {
                "rebalance_relative": {
                    "q": self.rebalance_tolerance_q,
                    "decimal": q_to_string(self.rebalance_tolerance_q),
                },
                "fairness_relative": {
                    "q": self.fairness_tolerance_q,
                    "decimal": q_to_string(self.fairness_tolerance_q),
                },
            },
            "clock": {
                "offset_ms": self.clock_offset_ms,
                "injection": "Clock.offset_ms or LEASE_CLOCK_OFFSET_MS; added to system now only",
                "effective_now_ms": now,
            },
            "time_contract": ttl_precision_report(now_ms=now),
            "database": {
                "path": str(self.store.db_path),
                "sqlite_journal_mode": "WAL",
                "sqlite_synchronous": "FULL",
                "write_transaction": "BEGIN IMMEDIATE",
                "registered_keys": registered,
            },
            "metric_cache": cache,
            "recovery": recovery,
            "intent_log": intent_log,
            "forced_termination": {
                "scan": "all intentions with phase='prepared' at startup",
                "grant_rollback": "delete uncommitted lease, restore prior lock version and expiry index",
                "renew_rollback": "restore prior expires_at_ms, ttl_ms, renew_count and expiry index",
                "release_rollback": "restore prior active status and expiry index",
                "source_of_truth": "SQLite durable rows only",
                "graceful_shutdown_report_guaranteed": False,
                "caveat": "A SIGKILL/power loss can remove an in-flight response; only post-restart facts are reported.",
            },
            "error_codes": {
                E_LOCK_HELD: "lock remains actively held by a different/granted lease",
                E_STALE_LEASE: "request version is not the current persisted version",
                E_LEASE_EXPIRED: "the version existed but its deadline has passed",
                E_NOT_HOLDER: "current holder does not match",
                E_LOCK_NOT_HELD: "no active current lease exists",
                E_CONFLICTING_REQUEST: "request_id was reused with a different canonical payload",
                E_INVALID_REQUEST: "validation failure",
            },
        }

    def _execute(
        self,
        *,
        operation: str,
        lock_key: str,
        request_id: str,
        payload: dict[str, Any],
        handler: Callable[[sqlite3.Connection, int, dict[str, Any], int], HandlerResult],
    ) -> dict[str, Any]:
        prepared = self._prepare(operation, lock_key, request_id, payload)
        if prepared.cached_response is not None:
            return prepared.cached_response
        self._maybe_crash(operation)
        return self._apply(operation, lock_key, request_id, payload, prepared.intent_id, handler)

    def _prepare(
        self,
        operation: str,
        lock_key: str,
        request_id: str,
        payload: dict[str, Any],
    ) -> PreparedResult:
        payload_hash = _payload_hash(payload)
        now = self.now_ms()
        with self.store.write() as con:
            row = self.store.get_request(con, request_id)
            if row is not None:
                if str(row["payload_hash"]) != payload_hash:
                    return PreparedResult(
                        intent_id=-1,
                        cached_response={
                            "ok": False,
                            "code": E_CONFLICTING_REQUEST,
                            "message": "request_id was already used with a different canonical payload",
                            "request_id": request_id,
                            "existing_operation": str(row["operation"]),
                            "existing_lock_key": str(row["lock_key"]),
                            "now_ms": now,
                        },
                    )
                if str(row["status"]) == "completed":
                    return PreparedResult(
                        intent_id=-1, cached_response=self.store.request_response(row)
                    )
            prior = self.store.snapshot_lock(con, lock_key)
            if row is None:
                con.execute(
                    "INSERT INTO requests(request_id, operation, lock_key, payload_hash, payload_json, "
                    "status, response_json, created_at_ms, completed_at_ms) "
                    "VALUES(?, ?, ?, ?, ?, 'prepared', NULL, ?, NULL)",
                    (request_id, operation, lock_key, payload_hash, self.store.json(payload), now),
                )
            else:
                con.execute(
                    "UPDATE requests SET status='prepared', response_json=NULL, completed_at_ms=NULL "
                    "WHERE request_id=?",
                    (request_id,),
                )
            intent = self.store.latest_prepared_intent(con, request_id)
            if intent is None:
                intent_id = self.store.create_prepared_intent(
                    con,
                    request_id=request_id,
                    operation=operation,
                    lock_key=lock_key,
                    payload_hash=payload_hash,
                    payload=payload,
                    prior_state=prior,
                    now_ms=now,
                )
            else:
                intent_id = int(intent["id"])
        return PreparedResult(intent_id=intent_id, cached_response=None)

    def _apply(
        self,
        operation: str,
        lock_key: str,
        request_id: str,
        payload: dict[str, Any],
        intent_id: int,
        handler: Callable[[sqlite3.Connection, int, dict[str, Any], int], HandlerResult],
    ) -> dict[str, Any]:
        payload_hash = _payload_hash(payload)
        with self.store.write() as con:
            row = self.store.get_request(con, request_id)
            if row is None:
                raise RuntimeError("prepared request disappeared")
            if str(row["payload_hash"]) != payload_hash:
                return {
                    "ok": False,
                    "code": E_CONFLICTING_REQUEST,
                    "message": "request_id was already used with a different canonical payload",
                    "request_id": request_id,
                    "now_ms": self.now_ms(),
                }
            if str(row["status"]) == "completed":
                return self.store.request_response(row)
            now = self.now_ms()
            due_before_handler = con.execute(
                "SELECT 1 FROM leases WHERE status='active' AND expires_at_ms <= ? LIMIT 1",
                (now,),
            ).fetchone() is not None
            try:
                result = handler(con, now, payload, intent_id)
            except LeaseError as exc:
                snapshot = self.store.snapshot_lock(con, lock_key)
                response = {**exc.payload(), "lock_key": lock_key, "now_ms": now}
                if due_before_handler:
                    revision, _ = self._refresh_metric_cache(con, now)
                else:
                    revision = self.store.revision(con)
                response["revision"] = revision
                self.store.finish_intent(
                    con,
                    intent_id,
                    phase="committed",
                    intended_state={"business_error": exc.code, "observed": snapshot},
                    now_ms=now,
                )
                self.store.complete_request(con, request_id, response, now)
                return response
            if result.mutated:
                revision, _ = self._refresh_metric_cache(con, now)
            else:
                revision = self.store.revision(con)
            response = {**result.response, "revision": revision, "now_ms": now}
            self.store.finish_intent(
                con,
                intent_id,
                phase="committed",
                intended_state=result.intended_state,
                now_ms=now,
            )
            self.store.complete_request(con, request_id, response, now)
        return response

    def _handle_acquire(
        self,
        con: sqlite3.Connection,
        now: int,
        payload: dict[str, Any],
        intent_id: int,
    ) -> HandlerResult:
        lock_key = payload["lock_key"]
        holder_id = payload["holder_id"]
        ttl_ms = int(payload["ttl_ms"])
        weight_text = payload["weight"]
        weight_q = int(payload["weight_q"])
        expired = self.store.expire_due(con, now)
        self.store.ensure_registered(con, lock_key, now)
        held = self._held_lease(con, lock_key, now)
        if held is not None:
            response = self._held_response(lock_key, held, now)
            return HandlerResult(
                response=response,
                mutated=bool(expired),
                intended_state={"result": E_LOCK_HELD, "expired_keys": expired},
            )
        snapshot = self.store.snapshot_lock(con, lock_key)
        prior_version = int(snapshot["version"])
        version = prior_version + 1
        expires_at_ms = expiry_from_grant(now, ttl_ms)
        self.store.upsert_holder(con, holder_id, weight_text, weight_q, now)
        cursor = con.execute(
            "INSERT INTO leases(lock_key, holder_id, version, granted_at_ms, expires_at_ms, ttl_ms, "
            "renew_count, weight_text, weight_q, status, last_renewed_at_ms, ended_at_ms, acquire_request_id) "
            "VALUES(?, ?, ?, ?, ?, ?, 0, ?, ?, 'active', NULL, NULL, ?)",
            (
                lock_key,
                holder_id,
                version,
                now,
                expires_at_ms,
                ttl_ms,
                weight_text,
                weight_q,
                payload["request_id"],
            ),
        )
        lease_id = int(cursor.lastrowid)
        con.execute(
            "UPDATE lock_state SET version=?, updated_at_ms=? WHERE lock_key=?",
            (version, now, lock_key),
        )
        con.execute(
            "INSERT INTO expiry_index(lock_key, lease_id, expires_at_ms) VALUES(?, ?, ?) "
            "ON CONFLICT(lock_key) DO UPDATE SET lease_id=excluded.lease_id, expires_at_ms=excluded.expires_at_ms",
            (lock_key, lease_id, expires_at_ms),
        )
        self.store.event(
            con,
            lock_key=lock_key,
            holder_id=holder_id,
            version=version,
            event_type="granted",
            event_at_ms=now,
            request_id=payload["request_id"],
            details={"ttl_ms": ttl_ms, "expires_at_ms": expires_at_ms, "weight": weight_text},
        )
        response = {
            "ok": True,
            "code": OK,
            "lock_key": lock_key,
            "lease": {
                "lock_key": lock_key,
                "holder_id": holder_id,
                "version": version,
                "granted_at_ms": now,
                "expires_at_ms": expires_at_ms,
                "remaining_ttl_ms": ttl_ms,
                "ttl_ms": ttl_ms,
                "renew_count": 0,
                "weight": {"q": weight_q, "decimal": q_to_string(weight_q)},
                "weight_input": weight_text,
            },
            "previous_version": prior_version,
            "expired_keys_swept": expired,
        }
        return HandlerResult(
            response=response,
            mutated=True,
            intended_state={
                "operation": "grant",
                "lease_id": lease_id,
                "version": version,
                "holder_id": holder_id,
                "granted_at_ms": now,
                "expires_at_ms": expires_at_ms,
                "ttl_ms": ttl_ms,
                "renew_count": 0,
                "request_id": payload["request_id"],
            },
        )

    def _handle_renew(
        self,
        con: sqlite3.Connection,
        now: int,
        payload: dict[str, Any],
        intent_id: int,
    ) -> HandlerResult:
        lock_key = payload["lock_key"]
        holder_id = payload["holder_id"]
        requested_version = int(payload["version"])
        expired = self.store.expire_due(con, now)
        snapshot = self.store.snapshot_lock(con, lock_key)
        current = snapshot.get("lease")
        current_version = int(snapshot["version"])
        if current is None or int(current["version"]) != requested_version or current_version != requested_version:
            raise LeaseError(
                E_STALE_LEASE,
                "renew requires the current lease version",
                requested_version=requested_version,
                current_version=current_version,
                current_holder_id=None if current is None else str(current["holder_id"]),
            )
        if str(current["status"]) != "active" or int(current["expires_at_ms"]) <= now:
            raise LeaseError(
                E_LEASE_EXPIRED,
                "lease has expired",
                version=requested_version,
                expires_at_ms=int(current["expires_at_ms"]),
                remaining_ttl_ms=0,
            )
        if str(current["holder_id"]) != holder_id:
            raise LeaseError(
                E_NOT_HOLDER,
                "holder does not own the current lease",
                current_holder_id=str(current["holder_id"]),
                version=requested_version,
            )
        prior = dict(current)
        ttl_ms = int(payload["ttl_ms"] or current["ttl_ms"])
        new_expires_at_ms = now + ttl_ms
        new_renew_count = int(current["renew_count"]) + 1
        con.execute(
            "UPDATE leases SET expires_at_ms=?, ttl_ms=?, renew_count=?, last_renewed_at_ms=? WHERE id=?",
            (new_expires_at_ms, ttl_ms, new_renew_count, now, int(current["id"])),
        )
        con.execute(
            "UPDATE expiry_index SET expires_at_ms=? WHERE lock_key=? AND lease_id=?",
            (new_expires_at_ms, lock_key, int(current["id"])),
        )
        self.store.event(
            con,
            lock_key=lock_key,
            holder_id=holder_id,
            version=requested_version,
            event_type="renewed",
            event_at_ms=now,
            request_id=payload["request_id"],
            details={
                "prior_expires_at_ms": int(current["expires_at_ms"]),
                "expires_at_ms": new_expires_at_ms,
                "ttl_ms": ttl_ms,
                "renew_count": new_renew_count,
            },
        )
        response = {
            "ok": True,
            "code": OK,
            "lock_key": lock_key,
            "lease": {
                "lock_key": lock_key,
                "holder_id": holder_id,
                "version": requested_version,
                "granted_at_ms": int(current["granted_at_ms"]),
                "expires_at_ms": new_expires_at_ms,
                "remaining_ttl_ms": max(0, new_expires_at_ms - now),
                "ttl_ms": ttl_ms,
                "renew_count": new_renew_count,
                "weight": {"q": int(current["weight_q"]), "decimal": q_to_string(int(current["weight_q"]))},
                "weight_input": str(current["weight_text"]),
            },
            "prior_expires_at_ms": int(current["expires_at_ms"]),
            "expired_keys_swept": expired,
        }
        return HandlerResult(
            response=response,
            mutated=True,
            intended_state={
                "operation": "renew",
                "lease_id": int(current["id"]),
                "version": requested_version,
                "prior_expires_at_ms": int(current["expires_at_ms"]),
                "expires_at_ms": new_expires_at_ms,
                "prior_ttl_ms": int(current["ttl_ms"]),
                "ttl_ms": ttl_ms,
                "prior_renew_count": int(current["renew_count"]),
                "renew_count": new_renew_count,
                "renewed_at_ms": now,
                "request_id": payload["request_id"],
            },
        )

    def _handle_release(
        self,
        con: sqlite3.Connection,
        now: int,
        payload: dict[str, Any],
        intent_id: int,
    ) -> HandlerResult:
        lock_key = payload["lock_key"]
        holder_id = payload["holder_id"]
        requested_version = int(payload["version"])
        expired = self.store.expire_due(con, now)
        snapshot = self.store.snapshot_lock(con, lock_key)
        current = snapshot.get("lease")
        current_version = int(snapshot["version"])
        if current is None or int(current["version"]) != requested_version or current_version != requested_version:
            raise LeaseError(
                E_STALE_LEASE,
                "release requires the current lease version",
                requested_version=requested_version,
                current_version=current_version,
                current_holder_id=None if current is None else str(current["holder_id"]),
            )
        if str(current["status"]) != "active" or int(current["expires_at_ms"]) <= now:
            raise LeaseError(
                E_LEASE_EXPIRED,
                "lease has expired and is already unavailable",
                version=requested_version,
                expires_at_ms=int(current["expires_at_ms"]),
            )
        if str(current["holder_id"]) != holder_id:
            raise LeaseError(
                E_NOT_HOLDER,
                "holder does not own the current lease",
                current_holder_id=str(current["holder_id"]),
                version=requested_version,
            )
        con.execute(
            "UPDATE leases SET status='released', ended_at_ms=? WHERE id=?",
            (now, int(current["id"])),
        )
        con.execute("DELETE FROM expiry_index WHERE lock_key=? AND lease_id=?", (lock_key, int(current["id"])))
        self.store.event(
            con,
            lock_key=lock_key,
            holder_id=holder_id,
            version=requested_version,
            event_type="released",
            event_at_ms=now,
            request_id=payload["request_id"],
            details={"prior_expires_at_ms": int(current["expires_at_ms"])},
        )
        response = {
            "ok": True,
            "code": OK,
            "lock_key": lock_key,
            "holder_id": holder_id,
            "version": requested_version,
            "released_at_ms": now,
            "state": "available",
            "expired_keys_swept": expired,
        }
        return HandlerResult(
            response=response,
            mutated=True,
            intended_state={
                "operation": "release",
                "lease_id": int(current["id"]),
                "version": requested_version,
                "prior_status": "active",
                "prior_expires_at_ms": int(current["expires_at_ms"]),
                "status": "released",
                "released_at_ms": now,
                "request_id": payload["request_id"],
            },
        )

    def _handle_acquire_best(
        self,
        con: sqlite3.Connection,
        now: int,
        payload: dict[str, Any],
        intent_id: int,
    ) -> HandlerResult:
        lock_key = payload["lock_key"]
        expired = self.store.expire_due(con, now)
        self.store.ensure_registered(con, lock_key, now)
        held = self._held_lease(con, lock_key, now)
        if held is not None:
            response = self._held_response(lock_key, held, now)
            return HandlerResult(
                response=response,
                mutated=bool(expired),
                intended_state={"result": E_LOCK_HELD, "expired_keys": expired},
            )
        candidate_models = [
            Candidate(
                holder_id=item["holder_id"],
                weight_text=item["weight"],
                weight_q=item["weight_q"],
                ttl_ms=item["ttl_ms"],
            )
            for item in payload["candidates"]
        ]
        active = self.store.active_leases(con, now)
        holders = self.store.holders(con)
        arbitration = build_arbitration(
            active,
            holders,
            candidate_models,
            now_ms=now,
            relative_tolerance_q=self.fairness_tolerance_q,
        )
        winner = arbitration["winner"]
        holder_id = str(winner["holder_id"])
        weight_q = int(winner["weight"]["q"])  # type: ignore[index]
        ttl_ms = int(winner["ttl_ms"])
        winner_item = next(item for item in payload["candidates"] if item["holder_id"] == holder_id)
        weight_text = str(winner_item["weight"])
        snapshot = self.store.snapshot_lock(con, lock_key)
        prior_version = int(snapshot["version"])
        version = prior_version + 1
        expires_at_ms = now + ttl_ms
        self.store.upsert_holder(con, holder_id, weight_text, weight_q, now)
        cursor = con.execute(
            "INSERT INTO leases(lock_key, holder_id, version, granted_at_ms, expires_at_ms, ttl_ms, "
            "renew_count, weight_text, weight_q, status, last_renewed_at_ms, ended_at_ms, acquire_request_id) "
            "VALUES(?, ?, ?, ?, ?, ?, 0, ?, ?, 'active', NULL, NULL, ?)",
            (
                lock_key,
                holder_id,
                version,
                now,
                expires_at_ms,
                ttl_ms,
                weight_text,
                weight_q,
                payload["request_id"],
            ),
        )
        lease_id = int(cursor.lastrowid)
        con.execute(
            "UPDATE lock_state SET version=?, updated_at_ms=? WHERE lock_key=?",
            (version, now, lock_key),
        )
        con.execute(
            "INSERT INTO expiry_index(lock_key, lease_id, expires_at_ms) VALUES(?, ?, ?) "
            "ON CONFLICT(lock_key) DO UPDATE SET lease_id=excluded.lease_id, expires_at_ms=excluded.expires_at_ms",
            (lock_key, lease_id, expires_at_ms),
        )
        self.store.event(
            con,
            lock_key=lock_key,
            holder_id=holder_id,
            version=version,
            event_type="granted",
            event_at_ms=now,
            request_id=payload["request_id"],
            details={
                "ttl_ms": ttl_ms,
                "expires_at_ms": expires_at_ms,
                "weight": weight_text,
                "arbitration_winner": holder_id,
            },
        )
        response = {
            "ok": True,
            "code": OK,
            "lock_key": lock_key,
            "lease": {
                "lock_key": lock_key,
                "holder_id": holder_id,
                "version": version,
                "granted_at_ms": now,
                "expires_at_ms": expires_at_ms,
                "remaining_ttl_ms": ttl_ms,
                "ttl_ms": ttl_ms,
                "renew_count": 0,
                "weight": {"q": weight_q, "decimal": q_to_string(weight_q)},
                "weight_input": weight_text,
            },
            "arbitration": arbitration,
            "previous_version": prior_version,
            "expired_keys_swept": expired,
        }
        return HandlerResult(
            response=response,
            mutated=True,
            intended_state={
                "operation": "grant",
                "lease_id": lease_id,
                "version": version,
                "holder_id": holder_id,
                "granted_at_ms": now,
                "expires_at_ms": expires_at_ms,
                "ttl_ms": ttl_ms,
                "renew_count": 0,
                "request_id": payload["request_id"],
            },
        )

    def _held_lease(
        self, con: sqlite3.Connection, lock_key: str, now: int
    ) -> sqlite3.Row | None:
        snapshot = self.store.snapshot_lock(con, lock_key)
        lease = snapshot.get("lease")
        if (
            lease is not None
            and str(lease["status"]) == "active"
            and int(lease["expires_at_ms"]) > now
        ):
            return con.execute("SELECT * FROM leases WHERE id=?", (int(lease["id"]),)).fetchone()
        return None

    @staticmethod
    def _held_response(lock_key: str, lease: sqlite3.Row, now: int) -> dict[str, Any]:
        return {
            "ok": False,
            "code": E_LOCK_HELD,
            "message": "lock is held by an active lease",
            "lock_key": lock_key,
            "current": {
                "holder_id": str(lease["holder_id"]),
                "version": int(lease["version"]),
                "granted_at_ms": int(lease["granted_at_ms"]),
                "expires_at_ms": int(lease["expires_at_ms"]),
                "remaining_ttl_ms": max(0, int(lease["expires_at_ms"]) - now),
                "weight": {
                    "q": int(lease["weight_q"]),
                    "decimal": q_to_string(int(lease["weight_q"])),
                },
            },
            "remaining_ttl_ms": max(0, int(lease["expires_at_ms"]) - now),
            "now_ms": now,
        }

    @staticmethod
    def _lease_response(lease: dict[str, Any], now: int) -> dict[str, Any]:
        expires = int(lease["expires_at_ms"])
        return {
            "id": int(lease["id"]),
            "lock_key": str(lease["lock_key"]),
            "holder_id": str(lease["holder_id"]),
            "version": int(lease["version"]),
            "granted_at_ms": int(lease["granted_at_ms"]),
            "expires_at_ms": expires,
            "remaining_ttl_ms": max(0, expires - now),
            "ttl_ms": int(lease["ttl_ms"]),
            "renew_count": int(lease["renew_count"]),
            "weight": {"q": int(lease["weight_q"]), "decimal": q_to_string(int(lease["weight_q"]))},
            "weight_input": str(lease["weight_text"]),
            "status": str(lease["status"]),
            "last_renewed_at_ms": lease["last_renewed_at_ms"],
            "ended_at_ms": lease["ended_at_ms"],
        }

    def _metrics_in_transaction(self, con: sqlite3.Connection, now: int) -> dict[str, Any]:
        active = self.store.active_leases(con, now)
        holders = self.store.holders(con)
        registered = self.store.registered_key_count(con)
        revision = self.store.revision(con)
        metrics = build_metrics(
            active,
            holders,
            registered_keys=registered,
            now_ms=now,
            revision=revision,
            rebalance_tolerance_q=self.rebalance_tolerance_q,
        )
        cache = self.store.get_metric_cache(con)
        metrics["cache"] = {
            "revision": revision,
            "persisted_snapshot": cache,
            "query_basis": "single SQLite read transaction over committed rows",
            "stale_cache_used_for_query": False,
        }
        return metrics

    def _refresh_metric_cache(
        self, con: sqlite3.Connection, now: int
    ) -> tuple[int, dict[str, Any]]:
        revision = self.store.increment_revision(con)
        metrics = build_metrics(
            self.store.active_leases(con, now),
            self.store.holders(con),
            registered_keys=self.store.registered_key_count(con),
            now_ms=now,
            revision=revision,
            rebalance_tolerance_q=self.rebalance_tolerance_q,
        )
        self.store.upsert_metric_cache(con, revision, now, metrics)
        return revision, metrics

    def _normalize_candidates(
        self, candidates: Sequence[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        if not candidates:
            raise LeaseError(E_INVALID_REQUEST, "at least one candidate is required")
        normalized: list[dict[str, Any]] = []
        seen: set[str] = set()
        for candidate in candidates:
            holder_id = _text(str(candidate.get("holder_id", "")), "candidate.holder_id")
            if holder_id in seen:
                raise LeaseError(E_INVALID_REQUEST, "candidate holder IDs must be unique")
            seen.add(holder_id)
            ttl_ms = _ttl(candidate.get("ttl_ms"), name="candidate.ttl_ms")
            weight_text, weight_q = _canonical_decimal(candidate.get("weight"))
            normalized.append(
                {
                    "holder_id": holder_id,
                    "ttl_ms": ttl_ms,
                    "weight": weight_text,
                    "weight_q": weight_q,
                }
            )
        return sorted(normalized, key=lambda item: item["holder_id"])

    def _maybe_crash(self, operation: str) -> None:
        target = os.environ.get("LEASE_FAULT_AFTER_PREPARE")
        if target and target == operation:
            os._exit(int(os.environ.get("LEASE_FAULT_EXIT_CODE", "91")))

    def recover_pending_intents(self) -> dict[str, Any]:
        started = self.now_ms()
        actions: list[dict[str, Any]] = []
        compensated = False
        with self.store.write() as con:
            intents = self.store.pending_prepared_intents(con)
            for intent in intents:
                owner_pid = int(intent["owner_pid"])
                if _pid_is_alive(owner_pid):
                    actions.append(
                        {
                            "intent_id": int(intent["id"]),
                            "request_id": str(intent["request_id"]),
                            "operation": str(intent["operation"]),
                            "lock_key": str(intent["lock_key"]),
                            "action": "skipped_owner_alive",
                            "basis": "prepared intent belongs to a live process; recovery did not reclaim it",
                            "observed_before": None,
                            "observed_after": None,
                            "changed": None,
                            "lock_available_after": None,
                        }
                    )
                    continue
                prior = json.loads(str(intent["prior_state_json"]))
                intended = json.loads(str(intent["intended_state_json"]))
                operation = str(intent["operation"])
                lock_key = str(intent["lock_key"])
                observed_before = self.store.snapshot_lock(con, lock_key) if lock_key != "*" else {}
                action = "no_op"
                basis = "mutation transaction did not commit; intended row is absent"
                if operation in {"acquire", "acquire_best"}:
                    deleted = (
                        self._rollback_grant(con, intent, intended, prior, started)
                        if intended
                        else False
                    )
                    if deleted:
                        action = "rolled_back_grant"
                        basis = "durable lease matched the prepared grant; lease deleted and prior version restored"
                        compensated = True
                    else:
                        action = "rolled_back_grant"
                        basis = (
                            "no durable lease matched the prepared grant; SQLite atomically rolled back the "
                            "uncommitted mutation, so the prior lock state remains effective"
                        )
                elif operation == "renew":
                    restored = (
                        self._rollback_renew(con, intent, intended, prior, started)
                        if intended
                        else False
                    )
                    if restored:
                        action = "rolled_back_renew"
                        basis = "durable lease matched prepared renewal; prior expiry and counter restored"
                        compensated = True
                    else:
                        action = "rolled_back_renew"
                        basis = (
                            "no durable renewal matched; SQLite atomically rolled back the uncommitted mutation, "
                            "so the previous expiry remains effective"
                        )
                elif operation == "release":
                    restored = (
                        self._rollback_release(con, intent, intended, prior, started)
                        if intended
                        else False
                    )
                    if restored:
                        action = "rolled_back_release"
                        basis = "durable lease matched prepared release; active status and expiry restored"
                        compensated = True
                    else:
                        action = "rolled_back_release"
                        basis = (
                            "no durable release matched; SQLite atomically rolled back the uncommitted mutation, "
                            "so the prior active status remains effective"
                        )
                observed_after = self.store.snapshot_lock(con, lock_key) if lock_key != "*" else {}
                self.store.finish_intent(
                    con,
                    int(intent["id"]),
                    phase="recovered",
                    intended_state=intended,
                    now_ms=started,
                    note=f"{action}: {basis}",
                )
                con.execute(
                    "UPDATE requests SET status='rolled_back', response_json=NULL, completed_at_ms=NULL "
                    "WHERE request_id=? AND status='prepared'",
                    (str(intent["request_id"]),),
                )
                actions.append(
                    {
                        "intent_id": int(intent["id"]),
                        "request_id": str(intent["request_id"]),
                        "operation": operation,
                        "lock_key": lock_key,
                        "action": action,
                        "basis": basis,
                        "observed_before": observed_before,
                        "observed_after": observed_after,
                        "changed": observed_before != observed_after,
                        "lock_available_after": self._lock_available_after(con, lock_key, started)
                        if lock_key != "*"
                        else None,
                    }
                )
            expired = self.store.expire_due(con, started)
            if compensated or expired or self.store.get_metric_cache(con) is None:
                revision, _ = self._refresh_metric_cache(con, started)
            else:
                revision = self.store.revision(con)
            completed = self.now_ms()
            summary = {
                "scanned": len(intents),
                "rolled_back_grants": sum(1 for item in actions if item["action"] == "rolled_back_grant"),
                "rolled_back_renews": sum(1 for item in actions if item["action"] == "rolled_back_renew"),
                "rolled_back_releases": sum(1 for item in actions if item["action"] == "rolled_back_release"),
                "no_op": sum(1 for item in actions if item["action"] == "no_op"),
                "skipped_owner_alive": sum(
                    1 for item in actions if item["action"] == "skipped_owner_alive"
                ),
                "changed_states": sum(1 for item in actions if item.get("changed") is True),
                "expired_keys_swept": expired,
            }
            report = {
                "scope": "startup scan of durable SQLite intentions and committed lease tables",
                "graceful_exit_report_guaranteed": False,
                "caveat": "Forced termination can discard the in-flight response. This report makes no claim about a complete graceful shutdown report.",
                "started_at_ms": started,
                "completed_at_ms": completed,
                "scanned_intents": len(intents),
                "summary": summary,
                "actions": actions,
                "revision_after_recovery": revision,
                "source_of_truth": "committed rows only; no process memory or implied shutdown state",
            }
            run_id = self.store.insert_recovery(con, started, completed, len(intents), report)
            report_with_id = {**report, "run_id": run_id}
            con.execute(
                "UPDATE recovery_runs SET report_json=? WHERE id=?",
                (self.store.json(report_with_id), run_id),
            )
        self.last_recovery = report_with_id
        return report_with_id

    def _rollback_grant(
        self,
        con: sqlite3.Connection,
        intent: sqlite3.Row,
        intended: dict[str, Any],
        prior: dict[str, Any],
        now: int,
    ) -> bool:
        lease = con.execute(
            "SELECT * FROM leases WHERE lock_key=? AND version=?",
            (str(intent["lock_key"]), int(intended.get("version", -1))),
        ).fetchone()
        if lease is None or str(lease["acquire_request_id"]) != str(intent["request_id"]):
            return False
        con.execute("DELETE FROM expiry_index WHERE lock_key=? AND lease_id=?", (str(intent["lock_key"]), int(lease["id"])))
        con.execute("DELETE FROM leases WHERE id=?", (int(lease["id"]),))
        prior_version = int(prior.get("version", 0))
        con.execute(
            "UPDATE lock_state SET version=?, updated_at_ms=? WHERE lock_key=?",
            (prior_version, now, str(intent["lock_key"])),
        )
        self.store.event(
            con,
            lock_key=str(intent["lock_key"]),
            holder_id=str(lease["holder_id"]),
            version=int(lease["version"]),
            event_type="recovery_rolled_back_grant",
            event_at_ms=now,
            request_id=str(intent["request_id"]),
            details={"restored_version": prior_version},
        )
        return True

    def _rollback_renew(
        self,
        con: sqlite3.Connection,
        intent: sqlite3.Row,
        intended: dict[str, Any],
        prior: dict[str, Any],
        now: int,
    ) -> bool:
        lease = con.execute(
            "SELECT * FROM leases WHERE id=?", (int(intended.get("lease_id", -1)),)
        ).fetchone()
        if lease is None:
            return False
        if int(lease["renew_count"]) != int(intended.get("renew_count", -1)):
            return False
        if int(lease["expires_at_ms"]) != int(intended.get("expires_at_ms", -1)):
            return False
        prior_lease = prior.get("lease") or {}
        prior_expires = int(intended.get("prior_expires_at_ms", prior_lease.get("expires_at_ms", lease["expires_at_ms"])))
        prior_ttl = int(intended.get("prior_ttl_ms", prior_lease.get("ttl_ms", lease["ttl_ms"])))
        prior_count = int(intended.get("prior_renew_count", prior_lease.get("renew_count", 0)))
        con.execute(
            "UPDATE leases SET expires_at_ms=?, ttl_ms=?, renew_count=?, last_renewed_at_ms=NULL WHERE id=?",
            (prior_expires, prior_ttl, prior_count, int(lease["id"])),
        )
        con.execute(
            "UPDATE expiry_index SET expires_at_ms=? WHERE lock_key=? AND lease_id=?",
            (prior_expires, str(lease["lock_key"]), int(lease["id"])),
        )
        self.store.event(
            con,
            lock_key=str(lease["lock_key"]),
            holder_id=str(lease["holder_id"]),
            version=int(lease["version"]),
            event_type="recovery_rolled_back_renew",
            event_at_ms=now,
            request_id=str(intent["request_id"]),
            details={"restored_expires_at_ms": prior_expires, "restored_renew_count": prior_count},
        )
        return True

    def _rollback_release(
        self,
        con: sqlite3.Connection,
        intent: sqlite3.Row,
        intended: dict[str, Any],
        prior: dict[str, Any],
        now: int,
    ) -> bool:
        lease = con.execute(
            "SELECT * FROM leases WHERE id=?", (int(intended.get("lease_id", -1)),)
        ).fetchone()
        if lease is None or str(lease["status"]) != "released":
            return False
        if int(lease["ended_at_ms"] or 0) != int(intended.get("released_at_ms", -1)):
            return False
        prior_expires = int(intended.get("prior_expires_at_ms", lease["expires_at_ms"]))
        con.execute(
            "UPDATE leases SET status='active', ended_at_ms=NULL WHERE id=?",
            (int(lease["id"]),),
        )
        con.execute(
            "INSERT INTO expiry_index(lock_key, lease_id, expires_at_ms) VALUES(?, ?, ?) "
            "ON CONFLICT(lock_key) DO UPDATE SET lease_id=excluded.lease_id, expires_at_ms=excluded.expires_at_ms",
            (str(lease["lock_key"]), int(lease["id"]), prior_expires),
        )
        self.store.event(
            con,
            lock_key=str(lease["lock_key"]),
            holder_id=str(lease["holder_id"]),
            version=int(lease["version"]),
            event_type="recovery_rolled_back_release",
            event_at_ms=now,
            request_id=str(intent["request_id"]),
            details={"restored_expires_at_ms": prior_expires},
        )
        return True

    def _lock_available_after(self, con: sqlite3.Connection, lock_key: str, now: int) -> bool:
        return self._held_lease(con, lock_key, now) is None


def load_fixed_samples(path: str | os.PathLike[str]) -> dict[str, Any]:
    with Path(path).open("r", encoding="utf-8") as handle:
        return json.load(handle)


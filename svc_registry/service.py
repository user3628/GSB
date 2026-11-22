from __future__ import annotations

import asyncio
import json
import time
from dataclasses import dataclass
from typing import Any

from .db import Database, InstanceRow, canonical_json, instance_from_row, now_ms
from .errors import E_NOT_FOUND, E_STALE_LEASE, E_VALIDATION, BusinessError
from .faults import maybe_crash
from .models import NormalizedRegistration
from .validation import validate_endpoint, validate_probe_method, validate_probe_params


@dataclass(frozen=True)
class RenewalResult:
    instance: dict[str, Any]
    reused_concurrent_result: bool = False


class RegistryService:
    def __init__(self, database: Database | str) -> None:
        self.db = database if isinstance(database, Database) else Database(database)
        self.db.initialize()
        self._locks: dict[tuple[str, str], asyncio.Lock] = {}
        self._renewal_cache: dict[
            tuple[str, str, int], tuple[float, dict[str, Any]]
        ] = {}

    async def register(self, record: NormalizedRegistration) -> dict[str, Any]:
        validate_probe_method(record.probe_method)
        validate_endpoint(record.endpoint, record.probe_method)
        params = validate_probe_params(record.probe_method, record.probe_params)
        if record.weight_permille < 1 or record.weight_permille > 1000:
            raise BusinessError(
                E_VALIDATION,
                "weight_permille must be within 1..1000",
                details={"weight_permille": record.weight_permille},
            )
        if record.lease_ttl_ms <= 0:
            raise BusinessError(
                E_VALIDATION,
                "lease_ttl_ms must be positive",
                details={"lease_ttl_ms": record.lease_ttl_ms},
            )
        key = record.key
        lock = self._locks.setdefault(key, asyncio.Lock())
        async with lock:
            intent_id = self.db.start_intent(
                "REGISTER",
                f"{record.service_name}/{record.instance_id}",
                {"service_name": record.service_name, "instance_id": record.instance_id},
            )
            maybe_crash("register_after_intent")
            registered_at = now_ms()
            try:
                with self.db.transaction() as connection:
                    existing = self.db.get_instance(
                        record.service_name,
                        record.instance_id,
                        connection=connection,
                    )
                    lease_version = 1 if existing is None else existing.lease_version + 1
                    record_version = 1 if existing is None else existing.record_version + 1
                    stored = NormalizedRegistration(
                        service_name=record.service_name,
                        instance_id=record.instance_id,
                        endpoint=record.endpoint,
                        weight_permille=record.weight_permille,
                        lease_ttl_ms=record.lease_ttl_ms,
                        probe_method=record.probe_method,
                        probe_params=params,
                        registered_at_ms=registered_at,
                        lease_version=lease_version,
                        probe_window_ms=record.probe_window_ms,
                        last_heartbeat_at_ms=registered_at,
                    )
                    self.db.write_instance(
                        connection,
                        stored,
                        granted_at_ms=registered_at,
                        health_status="UNKNOWN",
                        health_reason="registered; awaiting probe",
                        record_version=record_version,
                        source="register",
                    )
                    self.db.finish_intent(
                        connection,
                        intent_id,
                        "COMMITTED",
                        {"lease_version": lease_version, "record_version": record_version},
                    )
            except BaseException as exc:
                self.db.abort_intent(intent_id, {"error": str(exc)})
                raise
            return {
                "ok": True,
                "action": "created" if existing is None else "updated",
                "lease": {
                    "service_name": stored.service_name,
                    "instance_id": stored.instance_id,
                    "lease_version": lease_version,
                    "granted_at_ms": registered_at,
                    "expires_at_ms": registered_at + stored.lease_ttl_ms,
                    "record_version": record_version,
                },
                "instance": self.db.get_instance(stored.service_name, stored.instance_id).to_dict(),
            }

    async def renew_lease(
        self,
        service_name: str,
        instance_id: str,
        expected_lease_version: int,
    ) -> RenewalResult:
        if (
            isinstance(expected_lease_version, bool)
            or not isinstance(expected_lease_version, int)
            or expected_lease_version < 1
        ):
            raise BusinessError(
                E_VALIDATION,
                "expected_lease_version must be a positive integer",
                details={"expected_lease_version": expected_lease_version},
            )
        key = (service_name, instance_id)
        lock = self._locks.setdefault(key, asyncio.Lock())
        observed_contention = lock.locked()
        async with lock:
            # Give already-scheduled concurrent callers a chance to queue on the
            # per-instance lock.  They will then share the single renewal result.
            await asyncio.sleep(0)
            cache_key = (service_name, instance_id, expected_lease_version)
            cached = self._renewal_cache.get(cache_key)
            if observed_contention and cached is not None and cached[0] >= time.monotonic():
                return RenewalResult(instance=cached[1], reused_concurrent_result=True)

            intent_id = self.db.start_intent(
                "RENEW_LEASE",
                f"{service_name}/{instance_id}",
                {"expected_lease_version": expected_lease_version},
            )
            maybe_crash("renew_after_intent")
            renewed_at = now_ms()
            try:
                with self.db.transaction() as connection:
                    row = self.db.get_instance(service_name, instance_id, connection=connection)
                    if row is None:
                        raise BusinessError(
                            E_NOT_FOUND,
                            "instance lease was not found",
                            details={"service_name": service_name, "instance_id": instance_id},
                            http_status=404,
                        )
                    if expected_lease_version != row.lease_version:
                        raise BusinessError(
                            E_STALE_LEASE,
                            "lease version is stale; lease was not extended",
                            details={
                                "service_name": service_name,
                                "instance_id": instance_id,
                                "expected_lease_version": expected_lease_version,
                                "current_lease_version": row.lease_version,
                                "lease_expires_at_ms": row.lease_expires_at_ms,
                            },
                            http_status=409,
                        )
                    new_version = row.lease_version + 1
                    new_record_version = row.record_version + 1
                    expires_at = renewed_at + row.lease_ttl_ms
                    expired = row.lease_expires_at_ms <= renewed_at
                    health = "UNKNOWN" if expired else row.health_status
                    reason = "lease renewed after expiry" if expired else row.health_reason
                    connection.execute(
                        """
                        UPDATE instances SET
                            lease_version = ?, lease_granted_at_ms = ?, lease_expires_at_ms = ?,
                            last_heartbeat_at_ms = ?, health_status = ?, health_reason = ?,
                            record_version = ?, updated_at_ms = ?
                        WHERE service_name = ? AND instance_id = ? AND lease_version = ?
                        """,
                        (
                            new_version,
                            renewed_at,
                            expires_at,
                            renewed_at,
                            health,
                            reason,
                            new_record_version,
                            renewed_at,
                            service_name,
                            instance_id,
                            expected_lease_version,
                        ),
                    )
                    if connection.execute("SELECT changes()").fetchone()[0] != 1:
                        raise BusinessError(
                            E_STALE_LEASE,
                            "lease changed concurrently; lease was not extended",
                            details={"current_lease_version": None},
                            http_status=409,
                        )
                    connection.execute(
                        """
                        INSERT INTO lease_events (
                            service_name, instance_id, event_type, lease_version_before,
                            lease_version_after, granted_at_ms, expires_at_ms, created_at_ms
                        ) VALUES (?, ?, 'RENEW', ?, ?, ?, ?, ?)
                        """,
                        (
                            service_name,
                            instance_id,
                            expected_lease_version,
                            new_version,
                            renewed_at,
                            expires_at,
                            renewed_at,
                        ),
                    )
                    self.db.finish_intent(
                        connection,
                        intent_id,
                        "COMMITTED",
                        {"lease_version": new_version, "record_version": new_record_version},
                    )
            except BaseException as exc:
                self.db.abort_intent(intent_id, {"error": str(exc)})
                raise
            updated = self.db.get_instance(service_name, instance_id)
            assert updated is not None
            payload = updated.to_dict()
            self._renewal_cache[cache_key] = (time.monotonic() + 2.0, payload)
            return RenewalResult(instance=payload)

    def get_instance(self, service_name: str, instance_id: str) -> dict[str, Any]:
        row = self.db.get_instance(service_name, instance_id)
        if row is None:
            raise BusinessError(
                E_NOT_FOUND,
                "instance was not found",
                details={"service_name": service_name, "instance_id": instance_id},
                http_status=404,
            )
        return row.to_dict()

    def list_instances(self, service_name: str | None = None) -> list[dict[str, Any]]:
        observed_at = now_ms()
        return [
            row.to_dict(observed_at_ms=observed_at)
            for row in self.db.list_instances(service_name=service_name)
        ]

    def intent_log(self, *, status: str | None = None) -> list[dict[str, Any]]:
        query = "SELECT * FROM intent_log"
        params: tuple[Any, ...] = ()
        if status is not None:
            query += " WHERE status = ?"
            params = (status,)
        query += " ORDER BY id"
        with self.db.read() as connection:
            rows = connection.execute(query, params).fetchall()
            return [
                {
                    **dict(row),
                    "detail": json.loads(row["detail_json"]),
                }
                for row in rows
            ]

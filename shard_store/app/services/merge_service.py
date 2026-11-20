"""Merge orchestration: validation, two-phase intent commit, dual strategies."""
from __future__ import annotations

import time

from app.db.connection import connect, transaction
from app.errors import BusinessError
from app.repositories import (
    IntentRepository,
    ObjectRepository,
    PartRepository,
    SessionRepository,
)
from app.services.locking import registry
from app.utils import object_etag, sha256_hex


class SimulatedCrash(Exception):
    """Raised after the intent transaction commits but before metadata commit.

    It mimics a SIGKILL: the process exits with durable 'pending' intent and a
    session stuck in 'merging', so recovery logic can be exercised
    deterministically without actually killing the OS process.
    """


def _contiguous_missing(part_numbers: list[int]) -> list[int]:
    if not part_numbers:
        return [1]
    present = set(part_numbers)
    expected = set(range(1, max(part_numbers) + 1))
    return sorted(expected - present)


def _digest_from_persisted_part_digests(part_rows) -> str:
    # Object digest is a deterministic function of the per-part SHA-256
    # digests in part-number order. Arrival order never participates.
    material = b"".join(row["sha256"].encode("ascii") for row in part_rows)
    return sha256_hex(material)


class MergeService:
    def __init__(self, settings):
        self.settings = settings

    # ------------------------------------------------------------------ #
    # Read-only preflight validation (no writes, no partial metadata).
    # ------------------------------------------------------------------ #
    def _validate(self, conn, session: dict) -> list:
        part_repo = PartRepository(conn)
        parts = part_repo.list_for_session(session["session_id"])
        if not parts:
            raise BusinessError(
                "E_PART_MISSING",
                "no parts have been committed to this session",
                409,
                details={"session_id": session["session_id"], "missing_part_numbers": [1]},
            )
        numbers = [r["part_number"] for r in parts]
        missing = _contiguous_missing(numbers)
        if missing:
            raise BusinessError(
                "E_PART_MISSING",
                "part numbers must be contiguous starting at 1",
                409,
                details={"session_id": session["session_id"],
                         "missing_part_numbers": missing,
                         "present_part_numbers": numbers},
            )

        summed_size = sum(r["size_bytes"] for r in parts)
        # Session counters are recomputed on every submission inside the same
        # commit; verify that invariant here as an extra guard.
        if summed_size != session["total_size"] or len(parts) != session["part_count"]:
            raise BusinessError(
                "E_SIZE_MISMATCH",
                "session counters are inconsistent with the part manifest",
                409,
                details={"manifest_size": summed_size,
                         "session_size": session["total_size"],
                         "manifest_count": len(parts),
                         "session_count": session["part_count"]},
            )
        if session["expected_total_size"] is not None and \
                summed_size != session["expected_total_size"]:
            raise BusinessError(
                "E_SIZE_MISMATCH",
                "sum of part sizes does not equal expected_total_size",
                409,
                details={"manifest_size": summed_size,
                         "expected_total_size": session["expected_total_size"]},
            )
        return parts

    # ------------------------------------------------------------------ #
    # Strategy implementations. Both yield the SAME object digest.
    # ------------------------------------------------------------------ #
    def _fast_path(self, conn, session_id: str, parts) -> tuple[str, dict]:
        evidence_parts = [
            {
                "part_number": r["part_number"],
                "size_bytes": r["size_bytes"],
                "sha256": r["sha256"],
                "uploaded_at": r["uploaded_at"],
            }
            for r in parts
        ]
        digest = _digest_from_persisted_part_digests(parts)
        evidence = {
            "strategy": "fast",
            "description": "object digest derived from per-part SHA-256 values "
                           "persisted in the parts table at upload time; no gap "
                           "exists and append order equals part-number order",
            "source_table": "parts",
            "parts": evidence_parts,
            "formula": "sha256(concat(parts.sha256 ordered by part_number))",
        }
        return digest, evidence

    def _full_path(self, conn, session_id: str, parts) -> tuple[str, dict]:
        part_repo = PartRepository(conn)
        per_part = []
        total_start = time.perf_counter_ns()
        for row in parts:
            start = time.perf_counter_ns()
            body = part_repo.get_body(session_id, row["part_number"])
            recomputed = sha256_hex(body or b"")
            elapsed_ms = (time.perf_counter_ns() - start) / 1_000_000.0
            match = recomputed == row["sha256"]
            per_part.append({
                "part_number": row["part_number"],
                "stored_sha256": row["sha256"],
                "recomputed_sha256": recomputed,
                "match": match,
                "verify_elapsed_ms": round(elapsed_ms, 6),
            })
            if not match:
                raise BusinessError(
                    "E_DIGEST_MISMATCH",
                    f"recomputed digest for part {row['part_number']} differs from stored record",
                    409,
                    details={"part_number": row["part_number"],
                             "stored_sha256": row["sha256"],
                             "recomputed_sha256": recomputed},
                )
        total_elapsed_ms = (time.perf_counter_ns() - total_start) / 1_000_000.0
        digest = _digest_from_persisted_part_digests(parts)
        evidence = {
            "strategy": "full",
            "description": "every part body re-read from part_bodies and its "
                           "SHA-256 recomputed before deriving the object digest",
            "per_part_verification": per_part,
            "total_verify_elapsed_ms": round(total_elapsed_ms, 6),
            "formula": "sha256(concat(recomputed part sha256 ordered by part_number))",
        }
        return digest, evidence

    # ------------------------------------------------------------------ #
    # Public merge entry point.
    # ------------------------------------------------------------------ #
    def merge(self, session_id: str, strategy: str | None = None,
              simulate_crash_after_intent: bool = False) -> dict:
        if strategy is not None and strategy not in ("fast", "full"):
            raise BusinessError("E_PART_INVALID", "strategy must be 'fast' or 'full'", 400)

        with registry.session_lock(session_id):
            with connect(self.settings) as conn:
                session_row = SessionRepository(conn).get(session_id)
                if session_row is None:
                    raise BusinessError(
                        "E_SESSION_NOT_FOUND", f"session '{session_id}' not found", 404
                    )
                session = dict(session_row)
                status = session["status"]
                if status == "completed":
                    obj = ObjectRepository(conn).get_current(session["object_key"])
                    raise BusinessError(
                        "E_SESSION_COMPLETED",
                        "session already merged; repeat merge is rejected",
                        409,
                        details={"existing_result": dict(obj) if obj else None},
                    )
                if status == "cancelled":
                    raise BusinessError("E_SESSION_CANCELLED",
                                        "cancelled session cannot be merged", 409)
                if status == "aborted":
                    raise BusinessError("E_SESSION_ABORTED",
                                        "aborted session cannot be merged", 409)
                if status == "merging":
                    raise BusinessError(
                        "E_SESSION_CONFLICT",
                        "session is mid-merge (possible interrupted process); "
                        "run crash recovery before retrying",
                        409,
                    )
                parts = self._validate(conn, session)
                chosen_strategy = strategy or session["merge_strategy"]

            # Phase 1: durable intent + session -> merging (own transaction,
            # committed before any object metadata is touched).
            with connect(self.settings) as conn, transaction(conn, immediate=True):
                intent_repo = IntentRepository(conn)
                session_repo = SessionRepository(conn)
                intent_id = intent_repo.add_pending(
                    session_id, session["object_key"], chosen_strategy,
                    detail={"part_count": len(parts),
                            "total_size": sum(r["size_bytes"] for r in parts)},
                )
                session_repo.update_status(session_id, "merging", intent_id=intent_id)

            if simulate_crash_after_intent:
                # Simulated SIGKILL boundary: intent is durable, metadata is not.
                raise SimulatedCrash(intent_id)

            try:
                return self._commit_merge(session_id, chosen_strategy, intent_id, parts)
            except Exception:
                # Business/validation failure during the metadata transaction:
                # roll the session back to 'open'. The pending intent stays
                # durable and is resolved by the next recovery scan, exactly
                # like an interrupted process, while the client can retry.
                self._rollback_after_failed_commit(session_id, intent_id)
                raise


    def _rollback_after_failed_commit(self, session_id: str, intent_id: str) -> None:
        try:
            with connect(self.settings) as conn, transaction(conn, immediate=True):
                SessionRepository(conn).update_status(session_id, "open")
                # Keep the intent as 'pending'; recovery reports it as a fact.
        except Exception:
            # Best-effort rollback; a real crash leaves recovery to do this.
            pass

    def _commit_merge(self, session_id: str, strategy: str,
                      intent_id: str, validated_parts) -> dict:
        # Object-key lock serialises version allocation across concurrent
        # sessions of the same key; SQLite BEGIN IMMEDIATE is the durable backstop.
        object_key = None
        with connect(self.settings) as conn:
            object_key = SessionRepository(conn).get(session_id)["object_key"]

        with registry.object_lock(object_key):
            with connect(self.settings) as conn, transaction(conn, immediate=True):
                session_repo = SessionRepository(conn)
                part_repo = PartRepository(conn)
                object_repo = ObjectRepository(conn)
                intent_repo = IntentRepository(conn)

                session = session_repo.get(session_id)
                if session is None or session["status"] != "merging":
                    raise BusinessError(
                        "E_SESSION_CONFLICT",
                        "session state changed before commit; recovery may be required",
                        409,
                    )
                # Re-read manifest inside the write transaction so the commit
                # rests on one serialised snapshot.
                parts = part_repo.list_for_session(session_id)
                if strategy == "full":
                    digest, strategy_report = self._full_path(conn, session_id, parts)
                else:
                    digest, strategy_report = self._fast_path(conn, session_id, parts)

                total_size = sum(r["size_bytes"] for r in parts)
                part_count = len(parts)
                version = object_repo.next_version(object_key)
                etag = object_etag(digest, part_count)
                object_repo.put_version(
                    object_key, version, total_size, part_count,
                    digest, etag, session_id,
                )
                intent_repo.mark(
                    intent_id, "committed",
                    detail={"version": version, "object_sha256": digest},
                )
                session_repo.update_status(session_id, "completed")

                return {
                    "session_id": session_id,
                    "object_key": object_key,
                    "version": version,
                    "total_size": total_size,
                    "part_count": part_count,
                    "object_sha256": digest,
                    "etag": etag,
                    "intent_id": intent_id,
                    "strategy_report": strategy_report,
                }

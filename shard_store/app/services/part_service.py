"""Part submission: validation, idempotency, out-of-order tolerance."""
from __future__ import annotations

from app.config import MAX_PART_NUMBER, MIN_PART_NUMBER, Settings
from app.db.connection import connect, transaction
from app.errors import BusinessError
from app.repositories import PartRepository, SessionRepository
from app.services.locking import registry
from app.utils import sha256_hex


class PartService:
    def __init__(self, settings: Settings):
        self.settings = settings

    def submit_part(
        self,
        session_id: str,
        part_number: int,
        content: bytes,
        client_sha256: str | None = None,
    ) -> dict:
        # Structural validation happens before any lock/DB work and must never
        # mutate session state (requirement 1: E_PART_INVALID leaves it untouched).
        if not isinstance(part_number, int) or isinstance(part_number, bool):
            raise BusinessError("E_PART_INVALID", "part_number must be an integer", 400)
        if part_number < MIN_PART_NUMBER or part_number > MAX_PART_NUMBER:
            raise BusinessError(
                "E_PART_INVALID",
                f"part_number must be within [{MIN_PART_NUMBER}, {MAX_PART_NUMBER}]",
                400,
                details={"part_number": part_number},
            )
        if not isinstance(content, bytes) or len(content) == 0:
            raise BusinessError("E_PART_INVALID", "part content must be non-empty bytes", 400)

        actual_sha = sha256_hex(content)
        if client_sha256 is not None and client_sha256.lower() != actual_sha:
            raise BusinessError(
                "E_DIGEST_MISMATCH",
                "client-supplied sha256 does not match the uploaded content",
                409,
                details={"part_number": part_number,
                         "client_sha256": client_sha256.lower(),
                         "actual_sha256": actual_sha},
            )

        # Single-writer critical section for this session.
        with registry.session_lock(session_id):
            with connect(self.settings) as conn, transaction(conn, immediate=True):
                session_repo = SessionRepository(conn)
                part_repo = PartRepository(conn)
                session = session_repo.get(session_id)
                if session is None:
                    raise BusinessError(
                        "E_SESSION_NOT_FOUND", f"session '{session_id}' not found", 404
                    )
                if session["status"] != "open":
                    raise BusinessError(
                        f"E_SESSION_{session['status'].upper()}",
                        f"session is '{session['status']}'; part submission rejected",
                        409,
                        details={"session_id": session_id, "status": session["status"]},
                    )

                size = len(content)
                min_ok = size >= session["min_part_size"]
                max_ok = size <= session["max_part_size"]
                # Last-part exemption: an undersized part is legal when it
                # completes the declared expected total size.
                expected = session["expected_total_size"]
                is_completing_last = (
                    expected is not None
                    and session["total_size"] + size == expected
                )
                if not max_ok or not (min_ok or is_completing_last):
                    raise BusinessError(
                        "E_PART_INVALID",
                        "part size violates the session size bounds",
                        400,
                        details={
                            "part_number": part_number,
                            "size_bytes": size,
                            "min_part_size": session["min_part_size"],
                            "max_part_size": session["max_part_size"],
                            "last_part_exemption": is_completing_last,
                        },
                    )

                existing = part_repo.get(session_id, part_number)
                if existing is not None:
                    if existing["sha256"] == actual_sha and existing["size_bytes"] == size:
                        # Idempotent re-submission: nothing is written twice.
                        return {
                            "session_id": session_id,
                            "part_number": part_number,
                            "status": "idempotent",
                            "size_bytes": size,
                            "sha256": actual_sha,
                            "uploaded_at": existing["uploaded_at"],
                            "part_count": session["part_count"],
                            "total_size": session["total_size"],
                        }
                    # Digest/size conflict: first submission always wins.
                    raise BusinessError(
                        "E_PART_MISMATCH",
                        "part_number already committed with different content",
                        409,
                        details={
                            "part_number": part_number,
                            "stored_sha256": existing["sha256"],
                            "incoming_sha256": actual_sha,
                            "rule": "first submission is retained",
                        },
                    )

                part_repo.add(session_id, part_number, size, actual_sha, content)
                session_repo.refresh_counters(session_id)
                refreshed = session_repo.get(session_id)
                return {
                    "session_id": session_id,
                    "part_number": part_number,
                    "status": "stored",
                    "size_bytes": size,
                    "sha256": actual_sha,
                    "part_count": refreshed["part_count"],
                    "total_size": refreshed["total_size"],
                }

    def list_parts(self, session_id: str) -> dict:
        # Read-only: WAL snapshot, no process lock needed.
        with connect(self.settings) as conn:
            session = SessionRepository(conn).get(session_id)
            if session is None:
                raise BusinessError(
                    "E_SESSION_NOT_FOUND", f"session '{session_id}' not found", 404
                )
            rows = PartRepository(conn).list_for_session(session_id)
            return {
                "session_id": session_id,
                "object_key": session["object_key"],
                "status": session["status"],
                "part_count": session["part_count"],
                "total_size": session["total_size"],
                "parts": [
                    {
                        "part_number": r["part_number"],
                        "size_bytes": r["size_bytes"],
                        "sha256": r["sha256"],
                        "uploaded_at": r["uploaded_at"],
                    }
                    for r in rows
                ],
            }

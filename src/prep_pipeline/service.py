from __future__ import annotations

import hashlib
import json
import os
import shutil
import time
import uuid
from pathlib import Path
from typing import Any, Callable

from .canonical import atomic_write_bytes, digest_object
from .config import Settings
from .errors import PipelineError
from .models import BatchSubmitRequest, InjectionOptions
from .pipeline import PipelineComputer, StageArtifact
from .policy import PolicySnapshot
from .recovery import RecoveryManager
from .storage import Database


class PipelineService:
    def __init__(self, settings: Settings, *, db: Database | None = None):
        self.settings = settings
        self.settings.ensure_directories()
        self.db = db or Database(settings.db_path)
        self.db.initialize()
        self.recovery = RecoveryManager(settings, self.db)

    def initialize(self, recover: bool = True) -> list[dict[str, Any]]:
        return self.recovery.scan() if recover else []

    def submit_policy(self, snapshot: PolicySnapshot) -> dict[str, Any]:
        result = self.db.submit_policy(snapshot)
        result["policy_head"] = self.db.policy_head()
        return result

    def bootstrap_policies(self, paths: list[Path]) -> list[dict[str, Any]]:
        results: list[dict[str, Any]] = []
        for path in sorted(paths, key=lambda item: item.name):
            value = json.loads(path.read_text(encoding="utf-8"))
            from .policy import parse_policy_snapshot

            snapshot = parse_policy_snapshot(value)
            try:
                results.append(self.submit_policy(snapshot))
            except PipelineError as exc:
                if exc.code in {"E_STALE_SNAPSHOT", "E_STALE_BASE"}:
                    results.append(
                        {
                            "version": snapshot.version,
                            "status": "already_present_or_stale",
                            "code": exc.code,
                        }
                    )
                    continue
                raise
        return results

    def _resolve_source(self, input_path: str) -> tuple[Path, bytes]:
        requested = Path(input_path)
        if requested.is_absolute():
            candidate = requested.resolve()
        else:
            candidate = (self.settings.data_dir / requested).resolve()
        try:
            candidate.relative_to(self.settings.data_dir.resolve())
        except ValueError as exc:
            raise PipelineError(
                "E_INPUT_INVALID",
                "input_path must resolve inside the configured data directory",
                {"input_path": input_path, "data_dir": str(self.settings.data_dir)},
            ) from exc
        if not candidate.is_file():
            raise PipelineError(
                "E_INPUT_NOT_FOUND",
                "local input file was not found",
                {"input_path": input_path, "resolved_path": str(candidate)},
            )
        return candidate, candidate.read_bytes()

    def _request_fingerprint(self, request: BatchSubmitRequest, source_sha256: str) -> str:
        return digest_object(
            {
                "request": request.model_dump(mode="json"),
                "source_sha256": source_sha256,
            }
        )

    def submit_batch(
        self,
        request: BatchSubmitRequest,
        *,
        hard_crash_after_stage: str | None = None,
    ) -> dict[str, Any]:
        policy = self.db.get_policy(request.policy_version)
        source_path, source_bytes = self._resolve_source(request.input_path)
        source_sha256 = "sha256:" + hashlib.sha256(source_bytes).hexdigest()
        fingerprint = self._request_fingerprint(request, source_sha256)
        intent_id = f"intent_{uuid.uuid4().hex}"
        attempt_id = f"attempt_{uuid.uuid4().hex}"
        created, batch_row = self.db.claim_batch(
            batch_id=request.batch_id,
            request_fingerprint=fingerprint,
            policy_version=request.policy_version,
            input_path=str(source_path),
            source_sha256=source_sha256,
            intent_id=intent_id,
            attempt_id=attempt_id,
            request_json=json.dumps(
                request.model_dump(mode="json"),
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            ),
        )
        if not created:
            return self._reuse_or_wait(batch_row)

        attempt_root = self.settings.staging_dir / attempt_id
        source_copy = attempt_root / "source" / "input.txt"
        lock_acquired = False
        try:
            atomic_write_bytes(source_copy, source_bytes)
            self.db.acquire_policy_lock(
                request.policy_version,
                request.batch_id,
                attempt_id,
                self.settings.policy_lock_timeout_seconds,
                self.settings.poll_interval_seconds,
            )
            lock_acquired = True
            self.db.mark_batch_running(request.batch_id, attempt_id)
            execution = self._execute(
                request=request,
                policy=policy,
                source_bytes=source_bytes,
                attempt_root=attempt_root,
                attempt_id=attempt_id,
                hard_crash_after_stage=hard_crash_after_stage,
            )

            final_root = self.settings.published_dir / request.batch_id
            if final_root.exists():
                raise PipelineError(
                    "E_INTERNAL",
                    "published directory already exists for an uncommitted batch",
                    {"batch_id": request.batch_id, "path": str(final_root)},
                )
            attempt_root.replace(final_root)
            published_relative_root = f"batches/{request.batch_id}"
            summary = dict(execution.summary)
            summary.update(
                {
                    "batch_id": request.batch_id,
                    "attempt_id": attempt_id,
                    "artifact_relative_root": published_relative_root,
                    "source_snapshot_relative_path": "source/input.txt",
                    "staging_visibility": "not query-visible until committed",
                }
            )
            shard_rows = [
                {
                    "shard_id": shard.shard_id,
                    "sample_count": shard.sample_count,
                    "byte_count": shard.byte_count,
                    "relative_path": f"{published_relative_root}/{shard.relative_path}",
                    "manifest_sha256": shard.manifest_sha256,
                    "tensor_digest": shard.tensor_digest,
                    "dtypes": shard.dtypes,
                    "null_count": shard.null_count,
                    "warnings": shard.warnings,
                }
                for shard in execution.shards
            ]
            self.db.commit_batch(request.batch_id, attempt_id, summary, shard_rows)
            lock_acquired = False
            return self.db.get_committed_batch(request.batch_id)
        except PipelineError as exc:
            self._record_failure(request.batch_id, attempt_id, exc)
            raise
        except Exception as exc:
            wrapped = PipelineError(
                "E_INTERNAL",
                "unhandled pipeline failure",
                {"batch_id": request.batch_id, "exception_type": type(exc).__name__, "detail": str(exc)},
            )
            self._record_failure(request.batch_id, attempt_id, wrapped)
            raise wrapped from exc
        finally:
            if lock_acquired:
                try:
                    self.db.release_policy_lock(request.policy_version, attempt_id)
                except Exception:
                    pass
            if not self._batch_is_committed(request.batch_id):
                shutil.rmtree(attempt_root, ignore_errors=True)
                shutil.rmtree(self.settings.published_dir / request.batch_id, ignore_errors=True)

    def _execute(
        self,
        *,
        request: BatchSubmitRequest,
        policy: PolicySnapshot,
        source_bytes: bytes,
        attempt_root: Path,
        attempt_id: str,
        hard_crash_after_stage: str | None,
    ):
        computer = PipelineComputer(source_bytes, policy, attempt_root)

        def on_stage_start(sequence: int, name: str, input_digest: str) -> None:
            self.db.start_stage(
                request.batch_id, attempt_id, sequence, name, input_digest
            )

        def on_stage_complete(stage: StageArtifact) -> None:
            self.db.complete_stage(
                request.batch_id,
                stage.name,
                stage.output_digest,
                stage.stats,
                stage.details,
            )

        def on_stage_boundary(stage: StageArtifact) -> None:
            if self.settings.test_stage_delay_seconds > 0:
                time.sleep(self.settings.test_stage_delay_seconds)
            if hard_crash_after_stage == stage.name:
                os._exit(71)

        return computer.execute(
            request.injections,
            on_stage_start=on_stage_start,
            on_stage_complete=on_stage_complete,
            on_stage_boundary=on_stage_boundary,
        )

    def _record_failure(self, batch_id: str, attempt_id: str, exc: PipelineError) -> None:
        try:
            self.db.fail_incomplete_stage(batch_id, exc.code, exc.context)
            self.db.fail_batch(batch_id, attempt_id, exc.code, exc.message, exc.context)
        except Exception:
            pass

    def _batch_is_committed(self, batch_id: str) -> bool:
        row = self.db.get_batch_optional(batch_id)
        return bool(row is not None and row["status"] == "committed")

    def _reuse_or_wait(self, batch_row: dict[str, Any]) -> dict[str, Any]:
        if batch_row["status"] == "committed":
            return self.db.get_committed_batch(batch_row["batch_id"])
        if batch_row["status"] in {"failed", "aborted"}:
            raise self.db.stored_failure(batch_row)
        terminal = self.db.wait_for_terminal(
            batch_row["batch_id"],
            self.settings.batch_wait_timeout_seconds,
            self.settings.poll_interval_seconds,
        )
        if terminal["status"] == "committed":
            return self.db.get_committed_batch(batch_row["batch_id"])
        raise self.db.stored_failure(terminal)

    def get_batch(self, batch_id: str) -> dict[str, Any]:
        return self.db.get_committed_batch(batch_id)

    def recover(self) -> list[dict[str, Any]]:
        return self.recovery.scan()

    def recovery_reports(self, batch_id: str | None = None) -> list[dict[str, Any]]:
        return self.recovery.reports(batch_id)

from __future__ import annotations

import os
import re
import threading
import time
import uuid
from concurrent.futures import Future, ThreadPoolExecutor
from pathlib import Path
from typing import Any

from .engine import ExecutionResult, execute_join, execute_shuffle
from .errors import EngineError
from .snapshot import Snapshot, load_snapshot
from .storage import ActiveDataset, Repository


_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}$")
_TERMINAL = {"committed", "failed", "aborted"}


def _validate_id(value: Any, field: str) -> str:
    if not isinstance(value, str) or not _ID_RE.fullmatch(value):
        raise EngineError(
            "E_INVALID_REQUEST",
            f"{field} must match {_ID_RE.pattern}",
            {"field": field, "value": value},
        )
    return value


def _number(value: Any, field: str, *, allow_none: bool = False) -> float | None:
    if value is None and allow_none:
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise EngineError("E_INVALID_REQUEST", f"{field} must be numeric", {"field": field})
    return float(value)


def _integer(value: Any, field: str, *, allow_none: bool = False) -> int | None:
    if value is None and allow_none:
        return None
    if isinstance(value, bool) or not isinstance(value, int):
        raise EngineError("E_INVALID_REQUEST", f"{field} must be an integer", {"field": field})
    return value


class EngineService:
    """Single-writer request service backed by a durable SQLite state machine."""

    def __init__(
        self,
        db_path: str | Path,
        *,
        snapshot_loader=load_snapshot,
        recover_on_start: bool = True,
    ):
        self.repo = Repository(db_path)
        self._snapshot_loader = snapshot_loader
        self.recovery_on_start = self.repo.recover_incomplete() if recover_on_start else []
        self._admission_lock = threading.RLock()
        self._futures: dict[str, Future[None]] = {}
        self._executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="join-writer")
        self.crash_after_partition = int(
            os.environ.get("JOIN_ENGINE_CRASH_AFTER_PARTITION", "0") or "0"
        )
        self.crash_at_partition_start = int(
            os.environ.get("JOIN_ENGINE_CRASH_AT_PARTITION_START", "0") or "0"
        )
        self.crash_before_commit = os.environ.get("JOIN_ENGINE_CRASH_BEFORE_COMMIT", "") == "1"

    def close(self, wait: bool = True) -> None:
        self._executor.shutdown(wait=wait, cancel_futures=False)

    def load_snapshots(
        self,
        *,
        dataset_id: str,
        left_path: str | Path,
        right_path: str | Path,
    ) -> dict[str, Any]:
        dataset_id = _validate_id(dataset_id, "dataset_id")
        left = self._load_path(left_path)
        right = self._load_path(right_path)
        if left.side != "left":
            raise EngineError(
                "E_SNAPSHOT_INVALID",
                "left_path does not contain a left-side snapshot",
                {"side": left.side, "path": str(left.path)},
            )
        if right.side != "right":
            raise EngineError(
                "E_SNAPSHOT_INVALID",
                "right_path does not contain a right-side snapshot",
                {"side": right.side, "path": str(right.path)},
            )
        dataset = self.repo.activate_dataset(dataset_id, left, right)
        return {"dataset": dataset.to_dict(), "activation": "atomic-replacement"}

    def _load_path(self, path: str | Path) -> Snapshot:
        if not isinstance(path, (str, Path)) or not str(path):
            raise EngineError("E_INVALID_REQUEST", "snapshot path must be a non-empty string")
        return self._snapshot_loader(Path(path).expanduser().resolve())

    def submit_shuffle(
        self,
        request_id: str,
        dataset_id: str,
        *,
        partitions: int,
        skew_limit: float = 0.35,
        skew_action: str = "report",
        wait: bool = True,
        timeout: float | None = 60.0,
    ) -> dict[str, Any]:
        request_id = _validate_id(request_id, "request_id")
        dataset_id = _validate_id(dataset_id, "dataset_id")
        payload = {
            "request_id": request_id,
            "dataset_id": dataset_id,
            "partitions": _integer(partitions, "partitions"),
            "skew_limit": _number(skew_limit, "skew_limit"),
            "skew_action": skew_action,
        }
        return self._submit(request_id, "shuffle", dataset_id, payload, wait, timeout)

    def submit_join(
        self,
        request_id: str,
        dataset_id: str,
        *,
        partitions: int,
        join_type: str = "inner",
        measure: str = "left",
        skew_limit: float = 0.35,
        skew_action: str = "report",
        precision_mode: str = "auto",
        target_abs_error: float | None = None,
        max_operations: int | None = None,
        wait: bool = True,
        timeout: float | None = 60.0,
    ) -> dict[str, Any]:
        request_id = _validate_id(request_id, "request_id")
        dataset_id = _validate_id(dataset_id, "dataset_id")
        payload = {
            "request_id": request_id,
            "dataset_id": dataset_id,
            "partitions": _integer(partitions, "partitions"),
            "join_type": join_type,
            "measure": measure,
            "skew_limit": _number(skew_limit, "skew_limit"),
            "skew_action": skew_action,
            "precision": {
                "mode": precision_mode,
                "target_abs_error": _number(target_abs_error, "target_abs_error", allow_none=True),
                "max_operations": _integer(max_operations, "max_operations", allow_none=True),
            },
        }
        return self._submit(request_id, "join", dataset_id, payload, wait, timeout)

    def _submit(
        self,
        request_id: str,
        kind: str,
        dataset_id: str,
        payload: dict[str, Any],
        wait: bool,
        timeout: float | None,
    ) -> dict[str, Any]:
        with self._admission_lock:
            dataset = self.repo.get_active_dataset(dataset_id)
            if dataset is None:
                raise EngineError(
                    "E_DATASET_NOT_FOUND",
                    "active dataset not found",
                    {"dataset_id": dataset_id},
                )
            request, created = self.repo.admit_request(
                request_id=request_id,
                kind=kind,
                payload=payload,
                dataset_id=dataset_id,
                pair_version=dataset.pair_version,
            )
            if created:
                future = self._executor.submit(
                    self._run_request, request_id, kind, payload
                )
                self._futures[request_id] = future
            queue_position = self.repo.queue_position(request_id)
        response = {
            "request_id": request_id,
            "kind": kind,
            "reused": not created,
            "queue_position_at_submit": queue_position,
            "pinned_pair_version": request["pair_version"],
        }
        if not wait:
            response["request"] = self.repo.get_request_status(request_id)
            return response
        status = self.wait_for_request(request_id, timeout=timeout, raise_terminal_error=True)
        response["request"] = status
        if status["status"] == "committed":
            response["result"] = self.get_aggregates(request_id)
            response["partitions"] = self.get_partitions(request_id)
            response["precision"] = self.get_precision_contract(request_id)
        return response

    def _run_request(
        self, request_id: str, kind: str, payload: dict[str, Any]
    ) -> None:
        execution_id: str | None = None
        try:
            request = self.repo.get_request(request_id)
            if request is None:
                return
            dataset = self.repo.get_dataset_pair(
                request["dataset_id"], request["pair_version"]
            )
            if dataset is None:
                raise EngineError(
                    "E_SNAPSHOT_INVALID",
                    "pinned snapshot pair no longer exists in snapshot history",
                    {
                        "dataset_id": request["dataset_id"],
                        "pair_version": request["pair_version"],
                    },
                )
            execution_id = str(uuid.uuid4())
            self.repo.start_execution(
                request_id=request_id,
                execution_id=execution_id,
                dataset=dataset,
                partitions=int(payload["partitions"]),
                kind=kind,
            )
            left, right = self._load_pinned(dataset)

            def on_partition_start(partition_id: int) -> None:
                self.repo.begin_partition(execution_id, request_id, partition_id)
                if (
                    self.crash_at_partition_start > 0
                    and self.crash_at_partition_start == partition_id
                ):
                    os._exit(96)

            def on_partition_complete(partition_id, stats, partials) -> None:
                self.repo.complete_partition(
                    execution_id=execution_id,
                    request_id=request_id,
                    partition_id=partition_id,
                    plan=stats.to_dict(),
                    partials=[item.to_dict() for item in partials],
                )
                if self.crash_after_partition > 0 and self.crash_after_partition == partition_id:
                    os._exit(97)

            if kind == "shuffle":
                result = execute_shuffle(
                    left,
                    right,
                    partitions=int(payload["partitions"]),
                    skew_limit=float(payload["skew_limit"]),
                    skew_action=str(payload["skew_action"]),
                    on_partition_start=on_partition_start,
                    on_partition_complete=on_partition_complete,
                )
            else:
                precision = payload["precision"]
                result = execute_join(
                    left,
                    right,
                    partitions=int(payload["partitions"]),
                    join_type=str(payload["join_type"]),
                    measure=str(payload["measure"]),
                    skew_limit=float(payload["skew_limit"]),
                    skew_action=str(payload["skew_action"]),
                    precision_mode=str(precision["mode"]),
                    target_abs_error=precision.get("target_abs_error"),
                    max_operations=precision.get("max_operations"),
                    on_partition_start=on_partition_start,
                    on_partition_complete=on_partition_complete,
                )
            merge_intent = self.repo.begin_merge(execution_id, request_id, result.selected_path)
            if self.crash_before_commit:
                os._exit(98)
            self.repo.commit_execution(
                execution_id=execution_id,
                request_id=request_id,
                merge_intent_id=merge_intent,
                result=result,
                kind=kind,
            )
        except EngineError as error:
            self.repo.fail_request(
                request_id=request_id, execution_id=execution_id, error=error
            )
        except BaseException as error:
            wrapped = EngineError(
                "E_INTERNAL",
                "unexpected execution failure",
                {"error_type": type(error).__name__, "error": str(error)},
            )
            try:
                self.repo.fail_request(
                    request_id=request_id, execution_id=execution_id, error=wrapped
                )
            except BaseException:
                pass

    def _load_pinned(self, dataset: ActiveDataset) -> tuple[Snapshot, Snapshot]:
        left = self._snapshot_loader(Path(dataset.left_path))
        right = self._snapshot_loader(Path(dataset.right_path))
        checks = (
            (
                left,
                dataset.left_snapshot_id,
                dataset.left_version,
                dataset.left_digest,
                "left",
            ),
            (
                right,
                dataset.right_snapshot_id,
                dataset.right_version,
                dataset.right_digest,
                "right",
            ),
        )
        for snapshot, snapshot_id, version, digest, side in checks:
            if snapshot.side != side:
                raise EngineError(
                    "E_SNAPSHOT_INVALID",
                    "pinned snapshot side changed",
                    {"side": side, "actual": snapshot.side},
                )
            if (
                snapshot.snapshot_id != snapshot_id
                or snapshot.version != version
                or snapshot.content_sha256 != digest
            ):
                raise EngineError(
                    "E_SNAPSHOT_INVALID",
                    "pinned snapshot content no longer matches the active version",
                    {
                        "side": side,
                        "expected_snapshot_id": snapshot_id,
                        "expected_version": version,
                        "expected_digest": digest,
                        "actual_snapshot_id": snapshot.snapshot_id,
                        "actual_version": snapshot.version,
                        "actual_digest": snapshot.content_sha256,
                    },
                )
        return left, right

    def wait_for_request(
        self, request_id: str, *, timeout: float | None = 60.0, raise_terminal_error: bool = True
    ) -> dict[str, Any]:
        deadline = None if timeout is None else time.monotonic() + max(0.0, float(timeout))
        future = self._futures.get(request_id)
        if future is not None:
            remaining = None if deadline is None else max(0.0, deadline - time.monotonic())
            try:
                future.result(timeout=remaining)
            except TimeoutError as exc:
                raise EngineError(
                    "E_REQUEST_NOT_COMMITTED",
                    "timed out waiting for request completion",
                    {"request_id": request_id, "timeout_seconds": timeout},
                ) from exc
        while True:
            status = self.repo.get_request_status(request_id)
            if status is None:
                raise EngineError(
                    "E_REQUEST_NOT_FOUND", "request not found", {"request_id": request_id}
                )
            if status["status"] in _TERMINAL:
                if (
                    raise_terminal_error
                    and status["status"] in {"failed", "aborted"}
                    and status.get("error")
                ):
                    raise EngineError.from_stored(status["error"])
                return status
            if deadline is not None and time.monotonic() >= deadline:
                raise EngineError(
                    "E_REQUEST_NOT_COMMITTED",
                    "timed out waiting for request completion",
                    {"request_id": request_id, "timeout_seconds": timeout},
                )
            time.sleep(0.01)

    def get_request_status(self, request_id: str) -> dict[str, Any]:
        status = self.repo.get_request_status(request_id)
        if status is None:
            raise EngineError(
                "E_REQUEST_NOT_FOUND", "request not found", {"request_id": request_id}
            )
        return status

    def get_partitions(self, request_id: str) -> dict[str, Any]:
        result = self.repo.get_partitions(request_id)
        if result is None:
            raise EngineError(
                "E_REQUEST_NOT_FOUND", "request not found", {"request_id": request_id}
            )
        return result

    def get_aggregates(self, request_id: str) -> dict[str, Any]:
        result = self.repo.get_aggregates(request_id)
        if result is None:
            raise EngineError(
                "E_REQUEST_NOT_FOUND", "request not found", {"request_id": request_id}
            )
        return result

    def get_precision_contract(self, request_id: str) -> dict[str, Any]:
        result = self.repo.get_precision_contract(request_id)
        if result is None:
            raise EngineError(
                "E_REQUEST_NOT_FOUND", "request not found", {"request_id": request_id}
            )
        return result

    def get_partials(self, request_id: str) -> dict[str, Any]:
        result = self.repo.get_partials(request_id)
        if result is None:
            raise EngineError(
                "E_REQUEST_NOT_FOUND", "request not found", {"request_id": request_id}
            )
        return result

    def recovery_report(self, limit: int = 100) -> dict[str, Any]:
        reports = self.repo.get_recovery_reports(limit=limit)
        return {
            "reports": reports,
            "report_count": len(reports),
            "claim_scope": (
                "only persisted SQLite facts; forced termination does not produce a complete "
                "exit report"
            ),
        }

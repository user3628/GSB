"""Workspace/session orchestration, invalidation planning and lifecycle control."""

from __future__ import annotations

import fcntl
import re
import threading
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator

from .cache import ArtifactCache
from .canonical import atomic_write_json, digest, read_json
from .checkpoints import CheckpointStore
from .config import config_version, default_config, deep_merge, validate_config
from .data import DatasetRegistry
from .engine import RunEngine
from .errors import (
    Busy,
    InvalidData,
    InvalidStage,
    NotFound,
    StopRequested,
    WorkbenchError,
)
from .folds import folds_fingerprint
from .preprocess import preprocessing_fingerprint

_SESSION_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,63}$")
_STAGE_ORDER = ["preprocess", "folds", "solve", "aggregate"]


class InternalError(WorkbenchError):
    code = "E_INTERNAL"
    http_status = 500


def _diff_paths(old: Any, new: Any, prefix: str = "") -> list[str]:
    if isinstance(old, dict) and isinstance(new, dict):
        result: list[str] = []
        for key in sorted(set(old) | set(new)):
            path = f"{prefix}.{key}" if prefix else key
            if key not in old or key not in new:
                result.append(path)
            else:
                result.extend(_diff_paths(old[key], new[key], path))
        return result
    if old != new:
        return [prefix]
    return []


def _stage_for_config_path(path: str) -> str:
    if path.startswith("preprocessing") or path in {"dataset_id", "schema_version"}:
        return "preprocess"
    if path.startswith("cv.folds") or path.startswith("cv.seed") or path.startswith("cv.stratify"):
        return "folds"
    if path.startswith("regularization") or path.startswith("solver"):
        return "solve"
    if path.startswith("cv.metric") or path.startswith("selection"):
        return "aggregate"
    return "preprocess"


def _earliest(stages: list[str]) -> str | None:
    if not stages:
        return None
    return min(stages, key=_STAGE_ORDER.index)


class Workbench:
    def __init__(
        self,
        root: str | Path,
        *,
        data_dir: str | Path | None = None,
    ) -> None:
        self.root = Path(root).resolve()
        if data_dir is None:
            data_dir = Path(__file__).resolve().parent.parent / "data"
        self.data_dir = Path(data_dir).resolve()
        self.runs_dir = self.root / "runs"
        self.sessions_dir = self.root / "sessions"
        self.cache_dir = self.root / "cache"
        self.imports_dir = self.root / "imports"
        for directory in (
            self.root,
            self.runs_dir,
            self.sessions_dir,
            self.cache_dir,
            self.imports_dir,
        ):
            directory.mkdir(parents=True, exist_ok=True)
        self.registry = DatasetRegistry(self.data_dir, self.root)
        self.cache = ArtifactCache(self.cache_dir)
        self.lock_path = self.root / ".workspace.lock"
        self.counter_path = self.root / "run-counter.json"
        self._worker_lock = threading.Lock()
        self._worker_thread: threading.Thread | None = None
        self._worker_run_id: str | None = None
        self._ensure_default_session()

    @contextmanager
    def locked(self) -> Iterator[None]:
        with open(self.lock_path, "a+b") as handle:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
            try:
                yield
            finally:
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)

    def _session_path(self, session_id: str) -> Path:
        if not isinstance(session_id, str) or not _SESSION_RE.match(session_id):
            raise InvalidData("Invalid session id", details={"session_id": session_id})
        return self.sessions_dir / f"{session_id}.json"

    def _ensure_default_session(self) -> None:
        path = self._session_path("default")
        if path.exists():
            return
        with self.locked():
            if path.exists():
                return
            config = validate_config(default_config("small"))
            session = {
                "schema_version": 1,
                "session_id": "default",
                "config": config,
                "status": "idle",
                "active_run_id": None,
                "previous_run_id": None,
                "pending_invalidation": None,
                "last_error": None,
                "stop_requested": False,
            }
            atomic_write_json(path, session)

    def _load_session(self, session_id: str) -> dict[str, Any]:
        path = self._session_path(session_id)
        if not path.exists():
            raise NotFound("Session does not exist", details={"session_id": session_id})
        session = read_json(path)
        return session

    def _save_session(self, session: dict[str, Any]) -> None:
        atomic_write_json(self._session_path(session["session_id"]), session)

    def _run_dir(self, run_id: str) -> Path:
        return self.runs_dir / run_id

    def _load_run(self, run_id: str) -> dict[str, Any]:
        path = self._run_dir(run_id) / "run.json"
        if not path.exists():
            raise NotFound("Run does not exist", details={"run_id": run_id})
        return read_json(path)

    def _save_run(self, run: dict[str, Any]) -> None:
        atomic_write_json(self._run_dir(run["run_id"]) / "run.json", run)

    def _next_run_id(self, dataset_id: str, version_fingerprint: str) -> str:
        counter = 1
        if self.counter_path.exists():
            try:
                counter = int(read_json(self.counter_path).get("next_counter", 1))
            except (OSError, ValueError, TypeError):
                counter = 1
        next_counter = counter + 1
        atomic_write_json(self.counter_path, {"next_counter": next_counter})
        return f"{dataset_id}-{version_fingerprint[:10]}-{counter:06d}"

    def datasets(self) -> list[dict[str, Any]]:
        return self.registry.list_public()

    def create_session(
        self,
        session_id: str,
        config: dict[str, Any] | None = None,
        *,
        overwrite: bool = False,
    ) -> dict[str, Any]:
        """Create or explicitly reset a session for scripts and API clients."""

        path = self._session_path(session_id)
        with self._worker_lock:
            if self._worker_alive() and self._worker_run_id:
                active_session = self._load_session("default")
                if active_session.get("active_run_id") == self._worker_run_id:
                    raise Busy(
                        "Cannot reset a session while this process is training",
                        details={"run_id": self._worker_run_id},
                    )
            with self.locked():
                if path.exists() and not overwrite:
                    return self.get_session(session_id, include_run=False)
                if path.exists():
                    existing = self._load_session(session_id)
                    run_id = existing.get("active_run_id")
                    if run_id:
                        try:
                            if self._load_run(run_id).get("status") == "running":
                                raise Busy(
                                    "Cannot reset a running session",
                                    details={"run_id": run_id},
                                )
                        except NotFound:
                            pass
                resolved = validate_config(config or default_config("small"))
                self.registry.get(resolved["dataset_id"])
                session = {
                    "schema_version": 1,
                    "session_id": session_id,
                    "config": resolved,
                    "status": "idle",
                    "active_run_id": None,
                    "previous_run_id": None,
                    "pending_invalidation": None,
                    "last_error": None,
                    "stop_requested": False,
                }
                atomic_write_json(path, session)
                return self.get_session(session_id, include_run=False)

    def import_dataset(
        self,
        csv_path: str | Path,
        dataset_id: str,
        *,
        target: str | None = None,
        features: list[dict[str, Any]] | None = None,
        override: bool = False,
    ) -> dict[str, Any]:
        with self.locked():
            spec = self.registry.import_dataset(
                csv_path,
                dataset_id,
                target=target,
                features=features,
                override=override,
            )
        return spec.public_summary()

    def get_session(self, session_id: str = "default", *, include_run: bool = True) -> dict[str, Any]:
        session = self._load_session(session_id)
        if include_run and session.get("active_run_id"):
            try:
                session["active_run"] = self._load_run(session["active_run_id"])
            except NotFound:
                session["active_run"] = None
        if session.get("previous_run_id"):
            try:
                previous = self._load_run(session["previous_run_id"])
                session["previous_run"] = {
                    "run_id": previous["run_id"],
                    "status": previous["status"],
                    "stage_fingerprints": previous.get("stage_fingerprints", {}),
                    "result_digest": previous.get("result_digest"),
                }
            except NotFound:
                session["previous_run"] = None
        return session

    def update_config(
        self, session_id: str, patch: dict[str, Any]
    ) -> dict[str, Any]:
        if not self._session_path(session_id).exists():
            dataset_id = patch.get("dataset_id", "small") if isinstance(patch, dict) else "small"
            self.create_session(session_id, default_config(str(dataset_id)))
        with self._worker_lock:
            if self._worker_thread is not None and self._worker_thread.is_alive():
                raise Busy(
                    "A training run is active in this process",
                    details={"run_id": self._worker_run_id},
                )
            with self.locked():
                session = self._load_session(session_id)
                if session.get("status") == "running":
                    raise Busy(
                        "A training run is marked active",
                        details={"run_id": session.get("active_run_id")},
                    )
                merged = deep_merge(session["config"], patch)
                config = validate_config(merged)
                self.registry.get(config["dataset_id"])
                previous_run_id = session.get("active_run_id") or session.get("previous_run_id")
                invalidation = self.plan_invalidation(config, previous_run_id)
                session["config"] = config
                session["pending_invalidation"] = invalidation
                session["previous_run_id"] = previous_run_id
                session["active_run_id"] = None
                session["status"] = "idle"
                session["stop_requested"] = False
                session["last_error"] = None
                self._save_session(session)
                return {
                    "session": self.get_session(session_id, include_run=False),
                    "invalidation": invalidation,
                }

    def plan_invalidation(
        self, new_config: dict[str, Any], previous_run_id: str | None
    ) -> dict[str, Any]:
        new_spec = self.registry.get(new_config["dataset_id"])
        new_pre = preprocessing_fingerprint(new_spec, new_config)
        new_folds = folds_fingerprint(new_pre, new_config)
        new_version = digest(
            {
                "kind": "run-version-v1",
                "data_sha256": new_spec.sha256,
                "dataset_semantics": new_spec.semantics_digest,
                "config": new_config,
            }
        )
        if not previous_run_id:
            return {
                "changed": True,
                "earliest_stage": "preprocess",
                "affected_stages": list(_STAGE_ORDER),
                "reusable_stages": [],
                "reasons": ["no previous run exists in this session"],
                "changed_config_paths": [],
                "data_changed": True,
                "previous_run_id": None,
                "previous_stage_fingerprints": {},
                "planned_stage_fingerprints": {
                    "preprocess": new_pre,
                    "folds": new_folds,
                    "solve": None,
                    "aggregate": None,
                },
                "new_version_fingerprint": new_version,
            }
        try:
            previous = self._load_run(previous_run_id)
        except NotFound:
            return {
                "changed": True,
                "earliest_stage": "preprocess",
                "affected_stages": list(_STAGE_ORDER),
                "reusable_stages": [],
                "reasons": ["previous run is no longer available"],
                "changed_config_paths": [],
                "data_changed": True,
                "previous_run_id": previous_run_id,
                "previous_stage_fingerprints": {},
                "planned_stage_fingerprints": {
                    "preprocess": new_pre,
                    "folds": new_folds,
                    "solve": None,
                    "aggregate": None,
                },
                "new_version_fingerprint": new_version,
            }
        old_config = previous["config"]
        changed_paths = _diff_paths(old_config, new_config)
        old_spec = self.registry.get(previous["dataset_id"])
        old_sha = previous.get("data_sha256", old_spec.sha256)
        old_semantics = previous.get(
            "dataset_semantics_digest", old_spec.semantics_digest
        )
        data_changed = (
            old_sha != new_spec.sha256
            or old_semantics != new_spec.semantics_digest
            or previous["dataset_id"] != new_config["dataset_id"]
        )
        stages: list[str] = []
        reasons: list[str] = []
        if data_changed:
            stages.append("preprocess")
            reasons.append("dataset content or declared semantics changed")
        for path in changed_paths:
            stage = _stage_for_config_path(path)
            stages.append(stage)
            reasons.append(f"configuration changed: {path}")
        earliest = _earliest(stages)
        if earliest is None:
            return {
                "changed": False,
                "earliest_stage": None,
                "affected_stages": [],
                "reusable_stages": list(_STAGE_ORDER),
                "reasons": [],
                "changed_config_paths": [],
                "data_changed": False,
                "previous_run_id": previous_run_id,
                "previous_stage_fingerprints": previous.get("stage_fingerprints", {}),
                "planned_stage_fingerprints": {
                    "preprocess": new_pre,
                    "folds": new_folds,
                    "solve": previous.get("stage_fingerprints", {}).get("solve"),
                    "aggregate": previous.get("stage_fingerprints", {}).get("aggregate"),
                },
                "new_version_fingerprint": new_version,
            }
        affected = _STAGE_ORDER[_STAGE_ORDER.index(earliest) :]
        reusable = _STAGE_ORDER[: _STAGE_ORDER.index(earliest)]
        planned = {
            "preprocess": new_pre,
            "folds": new_folds,
            "solve": None,
            "aggregate": None,
        }
        if earliest in {"aggregate"}:
            planned["solve"] = previous.get("stage_fingerprints", {}).get("solve")
        return {
            "changed": True,
            "earliest_stage": earliest,
            "affected_stages": affected,
            "reusable_stages": reusable,
            "reasons": reasons,
            "changed_config_paths": changed_paths,
            "data_changed": data_changed,
            "previous_run_id": previous_run_id,
            "previous_stage_fingerprints": previous.get("stage_fingerprints", {}),
            "planned_stage_fingerprints": planned,
            "new_version_fingerprint": new_version,
        }

    def _create_run(self, session: dict[str, Any]) -> dict[str, Any]:
        config = session["config"]
        spec = self.registry.get(config["dataset_id"])
        version_fp = digest(
            {
                "kind": "run-version-v1",
                "data_sha256": spec.sha256,
                "dataset_semantics": spec.semantics_digest,
                "config": config,
            }
        )
        previous_run_id = session.get("active_run_id") or session.get("previous_run_id")
        invalidation = session.get("pending_invalidation")
        if invalidation is None or (
            previous_run_id
            and invalidation.get("previous_run_id") not in {None, previous_run_id}
        ):
            invalidation = self.plan_invalidation(config, previous_run_id)
        run_id = self._next_run_id(config["dataset_id"], version_fp)
        run_dir = self._run_dir(run_id)
        run_dir.mkdir(parents=True, exist_ok=False)
        (run_dir / "artifacts").mkdir()
        run = {
            "schema_version": 1,
            "run_id": run_id,
            "dataset_id": config["dataset_id"],
            "data_sha256": spec.sha256,
            "dataset_semantics_digest": spec.semantics_digest,
            "config": config,
            "config_version": config_version(config),
            "version_fingerprint": version_fp,
            "status": "created",
            "stage": "created",
            "previous_run_id": previous_run_id,
            "invalidation": invalidation,
            "stage_fingerprints": {
                "preprocess": None,
                "folds": None,
                "solve": None,
                "aggregate": None,
            },
            "reuse": [],
            "completed_preprocess": False,
            "completed_folds": False,
            "completed_solve_tasks": 0,
            "total_solve_tasks": 0,
            "completed_cv_tasks": 0,
            "total_cv_tasks": 0,
            "folds_count": None,
            "fold_status": [],
            "solve_task_metadata": [],
            "current_fold": None,
            "current_task": None,
            "current_role": None,
            "current_hyperparameters": None,
            "selection": None,
            "result_path": None,
            "result_digest": None,
            "error": None,
            "stop_requested": False,
            "empty_feature_errors": [],
        }
        atomic_write_json(run_dir / "run.json", run)
        session["previous_run_id"] = previous_run_id
        session["active_run_id"] = run_id
        session["pending_invalidation"] = None
        session["status"] = "running"
        session["last_error"] = None
        session["stop_requested"] = False
        self._save_session(session)
        return run

    def _worker_alive(self) -> bool:
        return self._worker_thread is not None and self._worker_thread.is_alive()

    def _execute_run(
        self,
        run_id: str,
        stop_after_tasks: int | None = None,
        session_id: str = "default",
    ) -> dict[str, Any]:
        run = self._load_run(run_id)
        engine = RunEngine(
            run_dir=self._run_dir(run_id),
            run=run,
            registry=self.registry,
            cache=self.cache,
            stop_after_tasks=stop_after_tasks,
        )
        session = self._load_session(session_id)
        try:
            result = engine.execute()
            with self.locked():
                session = self._load_session(session["session_id"])
                persisted_run = self._load_run(run_id)
                session["status"] = persisted_run.get("status", "complete")
                session["stop_requested"] = False
                session["last_error"] = persisted_run.get("error")
                self._save_session(session)
            return {"status": "complete", "result": result}
        except StopRequested:
            run = self._load_run(run_id)
            run["status"] = "stopped"
            run["stop_requested"] = True
            run["stage"] = run.get("stage") or "stopped"
            self._save_run(run)
            with self.locked():
                session = self._load_session(session["session_id"])
                session["status"] = "stopped"
                session["stop_requested"] = True
                self._save_session(session)
            return {"status": "stopped", "run_id": run_id}
        except WorkbenchError as exc:
            run = self._load_run(run_id)
            run["status"] = "error"
            run["error"] = exc.to_dict()
            self._save_run(run)
            with self.locked():
                session = self._load_session(session["session_id"])
                session["status"] = "error"
                session["last_error"] = exc.to_dict()
                self._save_session(session)
            raise
        except Exception as exc:  # pragma: no cover - defensive boundary
            error = InternalError(
                "Unhandled training failure",
                details={"exception_type": type(exc).__name__, "reason": str(exc)},
            )
            run = self._load_run(run_id)
            run["status"] = "error"
            run["error"] = error.to_dict()
            self._save_run(run)
            with self.locked():
                session = self._load_session(session["session_id"])
                session["status"] = "error"
                session["last_error"] = error.to_dict()
                self._save_session(session)
            raise error from exc

    def _start_worker(
        self, run_id: str, stop_after_tasks: int | None, session_id: str
    ) -> None:
        def target() -> None:
            try:
                self._execute_run(run_id, stop_after_tasks, session_id)
            except WorkbenchError:
                pass
            finally:
                with self._worker_lock:
                    if self._worker_run_id == run_id:
                        self._worker_run_id = None
                        self._worker_thread = None

        thread = threading.Thread(
            target=target, name=f"training-{run_id}", daemon=True
        )
        self._worker_thread = thread
        self._worker_run_id = run_id
        thread.start()

    def start_training(
        self,
        session_id: str = "default",
        *,
        config_patch: dict[str, Any] | None = None,
        stop_after_tasks: int | None = None,
        background: bool = True,
    ) -> dict[str, Any]:
        if stop_after_tasks is not None and (
            isinstance(stop_after_tasks, bool)
            or not isinstance(stop_after_tasks, int)
            or stop_after_tasks < 1
        ):
            raise InvalidData("stop_after_tasks must be a positive integer")
        if config_patch:
            self.update_config(session_id, config_patch)
        elif not self._session_path(session_id).exists():
            self.create_session(session_id, default_config())
        with self._worker_lock:
            if self._worker_alive():
                raise Busy(
                    "A training run is already active",
                    details={"run_id": self._worker_run_id},
                )
            with self.locked():
                session = self._load_session(session_id)
                if session.get("status") == "running":
                    raise Busy(
                        "Session is already running",
                        details={"run_id": session.get("active_run_id")},
                    )
                run = self._create_run(session)
        if background:
            self._start_worker(run["run_id"], stop_after_tasks, session_id)
            return {
                "accepted": True,
                "run_id": run["run_id"],
                "status": "running",
                "invalidation": run["invalidation"],
            }
        outcome = self._execute_run(run["run_id"], stop_after_tasks, session_id)
        return {
            "accepted": True,
            "run_id": run["run_id"],
            **outcome,
        }

    def resume(
        self,
        session_id: str = "default",
        *,
        stop_after_tasks: int | None = None,
        background: bool = True,
    ) -> dict[str, Any]:
        with self._worker_lock:
            if self._worker_alive():
                raise Busy(
                    "A training run is already active",
                    details={"run_id": self._worker_run_id},
                )
            with self.locked():
                session = self._load_session(session_id)
                run_id = session.get("active_run_id")
                if not run_id:
                    raise InvalidStage(
                        "No resumable run is active", details={"session_id": session_id}
                    )
                run = self._load_run(run_id)
                if digest(run["config"]) != digest(session["config"]):
                    raise InvalidStage(
                        "Session configuration changed; start a new run instead",
                        details={"run_id": run_id},
                    )
                if run.get("status") == "complete":
                    return {
                        "accepted": False,
                        "run_id": run_id,
                        "status": "complete",
                        "result_digest": run.get("result_digest"),
                    }
                stop_path = self._run_dir(run_id) / "STOP"
                if stop_path.exists():
                    stop_path.unlink()
                run["status"] = "running"
                run["stop_requested"] = False
                run["error"] = None
                self._save_run(run)
                session["status"] = "running"
                session["stop_requested"] = False
                session["last_error"] = None
                self._save_session(session)
        if background:
            self._start_worker(run_id, stop_after_tasks, session_id)
            return {"accepted": True, "run_id": run_id, "status": "running"}
        outcome = self._execute_run(run_id, stop_after_tasks, session_id)
        return {"accepted": True, "run_id": run_id, **outcome}

    def request_stop(self, session_id: str = "default") -> dict[str, Any]:
        with self.locked():
            session = self._load_session(session_id)
            run_id = session.get("active_run_id")
            if not run_id:
                raise InvalidStage(
                    "No active run to stop", details={"session_id": session_id}
                )
            run = self._load_run(run_id)
            if run.get("status") == "complete":
                return {"accepted": False, "run_id": run_id, "status": "complete"}
            store = CheckpointStore(self._run_dir(run_id), run_id)
            store.append_stop_marker({"requested": True, "source": "api_or_cli"})
            run["stop_requested"] = True
            self._save_run(run)
            session["status"] = "stopping"
            session["stop_requested"] = True
            self._save_session(session)
            return {"accepted": True, "run_id": run_id, "status": "stopping"}

    def status(self, session_id: str = "default") -> dict[str, Any]:
        session = self._load_session(session_id)
        run_id = session.get("active_run_id")
        if not run_id:
            return {
                "session_id": session_id,
                "status": session.get("status", "idle"),
                "config": session["config"],
                "active_run_id": None,
                "pending_invalidation": session.get("pending_invalidation"),
                "last_error": session.get("last_error"),
                "stages": self._empty_stages(),
                "folds": [],
            }
        try:
            run = self._load_run(run_id)
        except NotFound:
            return {
                "session_id": session_id,
                "status": "error",
                "config": session["config"],
                "active_run_id": run_id,
                "error": {
                    "code": "E_NOT_FOUND",
                    "message": "Run directory is missing",
                    "details": {"run_id": run_id},
                },
                "stages": self._empty_stages(),
                "folds": [],
            }
        store = CheckpointStore(self._run_dir(run_id), run_id)
        checkpoint_state = store.inspect()
        checkpoint_sequence = {}
        for record in checkpoint_state["records"]:
            if record["stage"] == "solve" and record["task_index"] is not None:
                checkpoint_sequence[(record["role"], record["task_index"])] = record["sequence"]
        stages = self._stage_statuses(run)
        result_summary = None
        if run.get("status") == "complete":
            summary_path = self._run_dir(run_id) / "artifacts" / "result-summary.json"
            if summary_path.exists():
                result_summary = read_json(summary_path)
        return {
            "session_id": session_id,
            "status": run.get("status"),
            "stage": run.get("stage"),
            "config": run["config"],
            "active_run_id": run_id,
            "previous_run_id": run.get("previous_run_id"),
            "version_fingerprint": run.get("version_fingerprint"),
            "data_sha256": run.get("data_sha256"),
            "config_version": run.get("config_version"),
            "stage_fingerprints": run.get("stage_fingerprints"),
            "invalidation": run.get("invalidation"),
            "pending_invalidation": session.get("pending_invalidation"),
            "stages": stages,
            "progress": {
                "preprocess": bool(run.get("completed_preprocess")),
                "folds": bool(run.get("completed_folds")),
                "solve_completed": int(run.get("completed_solve_tasks", 0)),
                "solve_total": int(run.get("total_solve_tasks", 0)),
                "cv_completed": int(run.get("completed_cv_tasks", 0)),
                "cv_total": int(run.get("total_cv_tasks", 0)),
            },
            "current": {
                "stage": run.get("stage"),
                "fold": run.get("current_fold"),
                "task_index": run.get("current_task"),
                "role": run.get("current_role"),
                "hyperparameters": run.get("current_hyperparameters"),
            },
            "selection": run.get("selection"),
            "folds": [
                {
                    **item,
                    "checkpoint_sequence": checkpoint_sequence.get(
                        (item.get("role"), item.get("task_index"))
                    ),
                }
                for item in run.get("fold_status", [])
            ],
            "reuse": run.get("reuse", []),
            "result_digest": run.get("result_digest"),
            "summary": result_summary,
            "error": run.get("error") or (
                session.get("last_error") if run.get("status") == "error" else None
            ),
            "stop_requested": bool(run.get("stop_requested")),
            "checkpoints": {
                "last_sequence": checkpoint_state["last_sequence"],
                "next_sequence": checkpoint_state["next_sequence"],
                "valid_count": len(checkpoint_state["records"]),
                "ignored": checkpoint_state["ignored"],
            },
            "empty_feature_errors": run.get("empty_feature_errors", []),
        }

    def _empty_stages(self) -> list[dict[str, Any]]:
        return [{"name": stage, "status": "pending"} for stage in _STAGE_ORDER]

    def _stage_statuses(self, run: dict[str, Any]) -> list[dict[str, Any]]:
        current = run.get("stage")
        statuses = []
        solve_done = int(run.get("completed_solve_tasks", 0))
        solve_total = int(run.get("total_solve_tasks", 0))
        done = {
            "preprocess": bool(run.get("completed_preprocess")),
            "folds": bool(run.get("completed_folds")),
            "solve": bool(solve_total and solve_done >= solve_total),
            "aggregate": run.get("status") == "complete",
        }
        for stage in _STAGE_ORDER:
            if done[stage]:
                status = "complete"
            elif current == stage:
                status = "running"
            elif stage == "solve" and solve_done:
                status = "running"
            else:
                status = "pending"
            item: dict[str, Any] = {"name": stage, "status": status}
            if stage == "solve" and solve_total:
                item["completed"] = solve_done
                item["total"] = solve_total
            statuses.append(item)
        return statuses

    def folds(self, session_id: str = "default") -> dict[str, Any]:
        status = self.status(session_id)
        return {
            "run_id": status.get("active_run_id"),
            "status": status.get("status"),
            "folds": status.get("folds", []),
            "progress": status.get("progress"),
        }

    def fingerprints(self, session_id: str = "default") -> dict[str, Any]:
        session = self._load_session(session_id)
        run_id = session.get("active_run_id")
        if run_id:
            run = self._load_run(run_id)
            return {
                "run_id": run_id,
                "version_fingerprint": run.get("version_fingerprint"),
                "config_version": run.get("config_version"),
                "stage_fingerprints": run.get("stage_fingerprints"),
                "tasks": run.get("solve_task_metadata", []),
                "pending_invalidation": session.get("pending_invalidation"),
            }
        return {
            "run_id": None,
            "version_fingerprint": None,
            "config_version": config_version(session["config"]),
            "stage_fingerprints": {},
            "tasks": [],
            "pending_invalidation": session.get("pending_invalidation"),
        }

    def invalidations(self, session_id: str = "default") -> dict[str, Any]:
        session = self._load_session(session_id)
        result = session.get("pending_invalidation")
        if result is None and session.get("active_run_id"):
            try:
                result = self._load_run(session["active_run_id"]).get("invalidation")
            except NotFound:
                result = None
        return {"invalidation": result}

    def checkpoints(self, session_id: str = "default") -> dict[str, Any]:
        session = self._load_session(session_id)
        run_id = session.get("active_run_id")
        if not run_id:
            return {"run_id": None, "records": [], "ignored": [], "next_sequence": 1}
        store = CheckpointStore(self._run_dir(run_id), run_id)
        state = store.inspect()
        return {
            "run_id": run_id,
            **state,
        }

    def result(self, session_id: str = "default") -> dict[str, Any]:
        session = self._load_session(session_id)
        run_id = session.get("active_run_id")
        if not run_id:
            raise InvalidStage("No active run result is available")
        run = self._load_run(run_id)
        if run.get("status") != "complete":
            raise InvalidStage(
                "Run has not completed successfully",
                details={"run_id": run_id, "status": run.get("status")},
            )
        path = self._run_dir(run_id) / "artifacts" / "result.json"
        if not path.exists():
            raise InvalidStage("Completed result artifact is missing")
        return read_json(path)

    def trajectory(
        self,
        session_id: str = "default",
        *,
        task_index: int | None = None,
    ) -> dict[str, Any]:
        session = self._load_session(session_id)
        run_id = session.get("active_run_id")
        if not run_id:
            raise InvalidStage("No active run trajectory is available")
        run = self._load_run(run_id)
        artifacts = self._run_dir(run_id) / "artifacts"
        candidates = sorted(artifacts.glob("final-*.json"))
        if task_index is not None and task_index > 0:
            candidates = sorted(artifacts.glob(f"solve-{task_index:04d}.json"))
            candidates += sorted(artifacts.glob(f"final-{task_index:04d}.json"))
        if not candidates:
            raise InvalidStage(
                "No matching solve trajectory artifact exists",
                details={"run_id": run_id, "task_index": task_index},
            )
        artifact = read_json(candidates[-1])
        return {
            "run_id": run_id,
            "task_index": artifact.get("task_index"),
            "role": artifact.get("role"),
            "fold": artifact.get("fold"),
            "strength": artifact.get("strength"),
            "l1_ratio": artifact.get("l1_ratio"),
            "feature_names": artifact.get("feature_names"),
            "coefficients": artifact.get("coefficients"),
            "intercept": artifact.get("intercept"),
            "trajectory": artifact.get("trajectory"),
            "trajectory_summary": artifact.get("trajectory_summary"),
            "validation": artifact.get("validation"),
        }

    def summary(self, session_id: str = "default") -> dict[str, Any]:
        status = self.status(session_id)
        if not status.get("summary"):
            raise InvalidStage(
                "Summary is available only after a complete run",
                details={"status": status.get("status")},
            )
        return status["summary"]

    def wait(self, timeout: float | None = None) -> bool:
        thread = self._worker_thread
        if thread is None:
            return True
        thread.join(timeout)
        return not thread.is_alive()

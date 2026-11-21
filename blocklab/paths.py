"""Workspace path layout and lock helpers."""

from __future__ import annotations

import fcntl
import os
import re
from pathlib import Path

from .errors import E_PATH, BusinessError
from .util import resolve_inside

JOB_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,79}$")
STAGES = ("activation", "reorder", "reduce")
STAGE_INDEX = {stage: index for index, stage in enumerate(STAGES)}
STAGE_DIRS = {
    "activation": "activation",
    "reorder": "reorder",
    "reduce": "reduce",
}


class FileLock:
    """Advisory flock around a real file; safe across local processes."""

    def __init__(self, path: Path, *, blocking: bool = True) -> None:
        self.path = path
        self.blocking = blocking
        self._handle = None

    def __enter__(self) -> "FileLock":
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._handle = self.path.open("a+b")
        flags = fcntl.LOCK_EX
        if not self.blocking:
            flags |= fcntl.LOCK_NB
        try:
            fcntl.flock(self._handle.fileno(), flags)
        except BlockingIOError:
            self._handle.close()
            self._handle = None
            raise
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        if self._handle is not None:
            try:
                fcntl.flock(self._handle.fileno(), fcntl.LOCK_UN)
            finally:
                self._handle.close()
                self._handle = None


class JobPaths:
    def __init__(self, workdir: Path, job_id: str) -> None:
        if not JOB_ID_RE.fullmatch(job_id):
            raise BusinessError(E_PATH, "invalid job id", job_id=job_id)
        self.workdir = workdir
        self.job_id = job_id
        self.job_dir = workdir / "jobs" / job_id
        self.job_file = self.job_dir / "job.json"
        self.state_file = self.job_dir / "state.json"
        self.invalidations_file = self.job_dir / "invalidations.jsonl"
        self.run_state_file = self.job_dir / "run_state.json"
        self.last_error_file = self.job_dir / "last_error.json"
        self.stop_file = self.job_dir / "STOP"
        self.locks_dir = self.job_dir / "locks"
        self.meta_lock = self.locks_dir / "meta.lock"
        self.run_lock = self.locks_dir / "run.lock"
        self.checkpoints_dir = self.job_dir / "checkpoints"
        self.archive_dir = self.job_dir / "archive"
        self.quarantine_dir = self.job_dir / "quarantine"
        self.outputs_dir = self.job_dir / "outputs"

    def stage_lock(self, stage: str) -> Path:
        return self.locks_dir / f"{stage}.lock"

    def stage_dir(self, stage: str, epoch: int) -> Path:
        return self.checkpoints_dir / stage / f"e{epoch:06d}"

    def stage_root(self, stage: str) -> Path:
        return self.checkpoints_dir / stage

    def output_path(self, name: str) -> Path:
        return self.outputs_dir / name


class WorkspacePaths:
    def __init__(self, root: Path, workdir: Path | str = "work") -> None:
        self.root = root.resolve()
        try:
            self.workdir = resolve_inside(self.root, workdir)
        except ValueError as exc:
            raise BusinessError(E_PATH, str(exc)) from exc
        self.catalog_file = self.workdir / "catalog.json"
        self.jobs_dir = self.workdir / "jobs"
        self.exports_dir = self.workdir / "exports"
        self.workdir_lock = self.workdir / "locks" / "workspace.lock"

    def job(self, job_id: str) -> JobPaths:
        return JobPaths(self.workdir, job_id)

    def ensure(self) -> None:
        for path in (
            self.workdir,
            self.jobs_dir,
            self.exports_dir,
            self.workdir / "locks",
        ):
            path.mkdir(parents=True, exist_ok=True)

    def resolve_root_path(self, candidate: str | Path) -> Path:
        try:
            return resolve_inside(self.root, candidate)
        except ValueError as exc:
            raise BusinessError(E_PATH, str(exc)) from exc

    def relative(self, candidate: str | Path) -> str:
        path = self.resolve_root_path(candidate)
        return path.relative_to(self.root).as_posix()


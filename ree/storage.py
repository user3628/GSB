from __future__ import annotations

import contextlib
import fcntl
import hashlib
import os
import tempfile
from pathlib import Path
from typing import Any, Iterator

from pydantic import BaseModel, ConfigDict

from .errors import REEError
from .models import canonical_bytes, canonical_json, sha256_bytes, sha256_value

STAGES = ("parse", "estimate", "extract", "validate")


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def resolve_workspace_path(root: Path, path: str | Path) -> Path:
    candidate = Path(path)
    if not candidate.is_absolute():
        candidate = root / candidate
    resolved = candidate.resolve()
    try:
        resolved.relative_to(root.resolve())
    except ValueError as exc:
        raise REEError(
            "E_INVALID_PATH",
            "input",
            f"path escapes the workspace: {path}",
            details={"workspace": root.as_posix()},
        ) from exc
    return resolved


class Workspace:
    def __init__(self, root: str | Path, runtime_dir: str = ".ree") -> None:
        self.root = Path(root).resolve()
        self.runtime = resolve_workspace_path(self.root, runtime_dir)
        self.runtime.mkdir(parents=True, exist_ok=True)
        (self.runtime / "artifacts").mkdir(exist_ok=True)
        (self.runtime / "checkpoints").mkdir(exist_ok=True)
        (self.runtime / "state" / "invalidations").mkdir(parents=True, exist_ok=True)

    def relative(self, path: Path) -> str:
        return path.resolve().relative_to(self.root).as_posix()

    def atomic_write_bytes(self, path: Path, payload: bytes) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        descriptor, temp_name = tempfile.mkstemp(
            prefix=f".{path.name}.tmp-", dir=path.parent
        )
        temp_path = Path(temp_name)
        try:
            with os.fdopen(descriptor, "wb") as handle:
                handle.write(payload)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temp_path, path)
            _fsync_directory(path.parent)
        finally:
            if temp_path.exists():
                temp_path.unlink(missing_ok=True)

    def atomic_write_json(self, path: Path, payload: Any) -> None:
        self.atomic_write_bytes(path, canonical_bytes(payload))

    def read_json(self, path: Path) -> Any:
        import json

        return json.loads(path.read_text(encoding="utf-8"))

    @contextlib.contextmanager
    def lock(self) -> Iterator[None]:
        lock_path = self.runtime / "workspace.lock"
        with lock_path.open("a+b") as handle:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
            try:
                yield
            finally:
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)

    def stop_requested(self) -> bool:
        return (self.runtime / "stop.request").exists()

    def request_stop(self) -> dict[str, Any]:
        payload = {"requested": True, "scope": "finish_current_document"}
        self.atomic_write_json(self.runtime / "stop.request", payload)
        return payload

    def clear_stop(self) -> None:
        (self.runtime / "stop.request").unlink(missing_ok=True)

    def write_run_state(self, payload: dict[str, Any]) -> None:
        self.atomic_write_json(self.runtime / "state" / "last_run.json", payload)

    def read_run_state(self) -> dict[str, Any] | None:
        path = self.runtime / "state" / "last_run.json"
        return self.read_json(path) if path.exists() else None


class ArtifactStore:
    def __init__(self, workspace: Workspace) -> None:
        self.workspace = workspace

    def save(self, stage: str, payload: Any) -> tuple[str, str, Path]:
        data = canonical_bytes(payload)
        digest = sha256_bytes(data)
        path = self.workspace.runtime / "artifacts" / stage / f"{digest}.json"
        if not path.exists():
            self.workspace.atomic_write_bytes(path, data)
        else:
            actual = hashlib.sha256(path.read_bytes()).hexdigest()
            if actual != digest:
                raise REEError(
                    "E_ARTIFACT_CORRUPT",
                    stage,
                    "content-addressed artifact digest mismatch",
                    details={"path": self.workspace.relative(path)},
                )
        return digest, self.workspace.relative(path), path

    def load(self, relative_path: str, expected_digest: str | None = None) -> Any:
        path = resolve_workspace_path(self.workspace.root, relative_path)
        if not path.exists():
            raise FileNotFoundError(path)
        data = path.read_bytes()
        if expected_digest is not None:
            actual = hashlib.sha256(data).hexdigest()
            if actual != expected_digest:
                raise REEError(
                    "E_ARTIFACT_CORRUPT",
                    "storage",
                    "artifact digest mismatch",
                    details={
                        "path": relative_path,
                        "expected": expected_digest,
                        "actual": actual,
                    },
                )
        import json

        return json.loads(data.decode("utf-8"))


class CheckpointRecord(BaseModel):
    model_config = ConfigDict(extra="forbid")

    seq: int
    stage: str
    input_digest: str
    config_version: str
    artifact_digest: str
    artifact_path: str
    input_components: dict[str, Any]


class CheckpointManager:
    def __init__(self, workspace: Workspace, artifact_store: ArtifactStore) -> None:
        self.workspace = workspace
        self.artifact_store = artifact_store
        self.directory = workspace.runtime / "checkpoints"

    def records(self) -> list[CheckpointRecord]:
        records: list[CheckpointRecord] = []
        for path in sorted(self.directory.glob("*.json")):
            try:
                records.append(CheckpointRecord.model_validate(self.workspace.read_json(path)))
            except Exception as exc:
                raise REEError(
                    "E_STALE_CHECKPOINT",
                    "checkpoint",
                    f"invalid checkpoint record {path.name}: {exc}",
                ) from exc
        records.sort(key=lambda record: record.seq)
        for expected, record in enumerate(records, start=1):
            if record.seq != expected:
                raise REEError(
                    "E_STALE_CHECKPOINT",
                    record.stage,
                    "checkpoint sequence is not contiguous",
                    details={"expected_seq": expected, "actual_seq": record.seq},
                )
        return records

    def latest(self) -> CheckpointRecord | None:
        records = self.records()
        return records[-1] if records else None

    def latest_for_stage(self, stage: str) -> CheckpointRecord | None:
        matching = [record for record in self.records() if record.stage == stage]
        return matching[-1] if matching else None

    def latest_by_stage(self) -> dict[str, CheckpointRecord]:
        result: dict[str, CheckpointRecord] = {}
        for record in self.records():
            result[record.stage] = record
        return result

    def next_seq(self) -> int:
        records = self.records()
        return (records[-1].seq + 1) if records else 1

    def publish(
        self,
        *,
        stage: str,
        expected_seq: int,
        input_digest: str,
        config_version: str,
        artifact_digest: str,
        artifact_path: str,
        input_components: dict[str, Any],
    ) -> CheckpointRecord:
        if stage not in STAGES:
            raise REEError(
                "E_STALE_CHECKPOINT",
                stage,
                "cannot publish a checkpoint for an unknown stage",
            )
        with self.workspace.lock():
            records = self.records()
            last_seq = records[-1].seq if records else 0
            if expected_seq <= last_seq:
                raise REEError(
                    "E_STALE_CHECKPOINT",
                    stage,
                    "checkpoint publish is stale, duplicate, or out of order",
                    details={"expected_seq": expected_seq, "last_seq": last_seq},
                )
            if expected_seq != last_seq + 1:
                raise REEError(
                    "E_STALE_CHECKPOINT",
                    stage,
                    "checkpoint sequence must advance by exactly one",
                    details={"expected_seq": expected_seq, "last_seq": last_seq},
                )
            if list(self.directory.glob(f"{expected_seq:08d}-*.json")):
                raise REEError(
                    "E_STALE_CHECKPOINT",
                    stage,
                    "checkpoint sequence already exists",
                    details={"seq": expected_seq},
                )
            stage_index = STAGES.index(stage)
            if stage_index > 0:
                prerequisite_stage = STAGES[stage_index - 1]
                prerequisite = next(
                    (
                        record
                        for record in reversed(records)
                        if record.stage == prerequisite_stage
                    ),
                    None,
                )
                if prerequisite is None:
                    raise REEError(
                        "E_STALE_CHECKPOINT",
                        stage,
                        f"missing prerequisite checkpoint for stage {prerequisite_stage}",
                        details={"prerequisite_stage": prerequisite_stage},
                    )
                prerequisite_path = resolve_workspace_path(
                    self.workspace.root, prerequisite.artifact_path
                )
                if not prerequisite_path.exists():
                    raise REEError(
                        "E_STALE_CHECKPOINT",
                        stage,
                        f"missing prerequisite artifact for stage {prerequisite_stage}",
                        details={"prerequisite_stage": prerequisite_stage},
                    )
                if prerequisite.seq >= expected_seq:
                    raise REEError(
                        "E_STALE_CHECKPOINT",
                        stage,
                        "prerequisite checkpoint is not older than the new checkpoint",
                        details={
                            "prerequisite_stage": prerequisite_stage,
                            "prerequisite_seq": prerequisite.seq,
                            "expected_seq": expected_seq,
                        },
                    )
            artifact_full_path = resolve_workspace_path(self.workspace.root, artifact_path)
            if not artifact_full_path.exists():
                raise REEError(
                    "E_STALE_CHECKPOINT",
                    stage,
                    "checkpoint references a missing artifact",
                    details={"artifact_path": artifact_path},
                )
            actual_digest = hashlib.sha256(artifact_full_path.read_bytes()).hexdigest()
            if actual_digest != artifact_digest:
                raise REEError(
                    "E_STALE_CHECKPOINT",
                    stage,
                    "checkpoint artifact digest is stale",
                    details={
                        "artifact_path": artifact_path,
                        "expected": artifact_digest,
                        "actual": actual_digest,
                    },
                )
            record = CheckpointRecord(
                seq=expected_seq,
                stage=stage,
                input_digest=input_digest,
                config_version=config_version,
                artifact_digest=artifact_digest,
                artifact_path=artifact_path,
                input_components=input_components,
            )
            final_path = self.directory / f"{expected_seq:08d}-{stage}.json"
            self.workspace.atomic_write_json(final_path, record.model_dump(mode="json"))
            return record


class InvalidationStore:
    def __init__(self, workspace: Workspace) -> None:
        self.workspace = workspace
        self.directory = workspace.runtime / "state" / "invalidations"

    def record(self, payload: dict[str, Any]) -> dict[str, Any]:
        digest = sha256_value(payload)
        path = self.directory / f"{digest}.json"
        if not path.exists():
            self.workspace.atomic_write_json(path, payload)
        return payload

    def list(self) -> list[dict[str, Any]]:
        return [self.workspace.read_json(path) for path in sorted(self.directory.glob("*.json"))]


def hash_files(root: Path, paths: list[str]) -> str:
    entries: list[dict[str, str]] = []
    for relative in sorted(set(paths)):
        path = resolve_workspace_path(root, relative)
        if not path.exists() or not path.is_file():
            raise REEError(
                "E_MISSING_INPUT",
                "parse",
                f"input file does not exist: {relative}",
            )
        entries.append(
            {
                "path": path.relative_to(root).as_posix(),
                "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
            }
        )
    return sha256_value(entries)


def fingerprint(components: dict[str, Any]) -> str:
    return sha256_value({"components": components})


def pretty_json(payload: Any) -> str:
    import json

    return json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True)


def canonical_text(payload: Any) -> str:
    return canonical_json(payload)

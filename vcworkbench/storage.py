from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .canonical import (
    FileLock,
    atomic_write_json,
    canonical_json_bytes,
    digest_obj,
    read_json,
    safe_component,
    sha256_bytes,
    sha256_file,
)
from .errors import WorkbenchError, fail
from .version import ARTIFACT_SCHEMA, CHECKPOINT_SCHEMA, CONFIG_VERSION, LEDGER_SCHEMA


STAGES = ("analysis", "mapping", "reconstruction", "validation")
STAGE_INDEX = {name: index for index, name in enumerate(STAGES)}


@dataclass(frozen=True)
class Artifact:
    shard_id: str
    stage: str
    input_fingerprint: str
    artifact_digest: str
    relpath: str
    directory: Path
    files: dict[str, str]

    def path(self, name: str) -> Path:
        return self.directory / name


class ArtifactStore:
    def __init__(self, state_dir: Path) -> None:
        self.state_dir = state_dir
        self.root = state_dir / "artifacts"

    def _shard_root(self, shard_id: str) -> Path:
        return self.root / safe_component(shard_id)

    def directory(self, shard_id: str, stage: str, input_fingerprint: str) -> Path:
        return self._shard_root(shard_id) / stage / input_fingerprint

    def relpath(self, directory: Path) -> str:
        return str(directory.relative_to(self.state_dir))

    def put(
        self,
        *,
        shard_id: str,
        stage: str,
        input_fingerprint: str,
        files: dict[str, bytes],
    ) -> Artifact:
        if stage not in STAGE_INDEX:
            fail("E_INVALID_STAGE", f"unknown stage {stage}", shard_id=shard_id, stage=stage)
        directory = self.directory(shard_id, stage, input_fingerprint)
        existing = self.load_if_valid(directory, strict=False)
        if existing is not None and existing.input_fingerprint == input_fingerprint:
            return existing
        file_hashes: dict[str, str] = {}
        for name in sorted(files):
            if "/" in name or "\\" in name or name in {"", ".", ".."}:
                fail(
                    "E_INVALID_PARAM",
                    f"unsafe artifact filename: {name}",
                    shard_id=shard_id,
                    stage=stage,
                )
            from .canonical import atomic_write_bytes

            atomic_write_bytes(directory / name, files[name])
            file_hashes[name] = sha256_bytes(files[name])
        manifest_base = {
            "schema": ARTIFACT_SCHEMA,
            "shard_id": shard_id,
            "stage": stage,
            "input_fingerprint": input_fingerprint,
            "config_version": CONFIG_VERSION,
            "files": file_hashes,
        }
        artifact_digest = digest_obj(manifest_base)
        manifest = dict(manifest_base)
        manifest["artifact_digest"] = artifact_digest
        atomic_write_json(directory / "manifest.json", manifest)
        return Artifact(
            shard_id=shard_id,
            stage=stage,
            input_fingerprint=input_fingerprint,
            artifact_digest=artifact_digest,
            relpath=self.relpath(directory),
            directory=directory,
            files=file_hashes,
        )

    def load(self, directory: Path, *, expected_shard: str | None = None, strict: bool = True) -> Artifact:
        manifest_path = directory / "manifest.json"
        try:
            manifest = read_json(manifest_path)
        except FileNotFoundError:
            if strict:
                fail(
                    "E_STALE_CHECKPOINT",
                    "artifact manifest is missing",
                    shard_id=expected_shard,
                    stage=None,
                    path=str(directory),
                )
            raise
        except Exception as exc:
            raise WorkbenchError(
                "E_STALE_CHECKPOINT",
                f"artifact manifest is unreadable: {exc}",
                shard_id=expected_shard,
                details={"path": str(directory)},
            ) from exc
        shard_id = manifest.get("shard_id")
        stage = manifest.get("stage")
        if manifest.get("config_version") != CONFIG_VERSION:
            fail(
                "E_STALE_CHECKPOINT",
                "artifact configuration version is stale",
                shard_id=shard_id if isinstance(shard_id, str) else expected_shard,
                stage=stage if isinstance(stage, str) else None,
                expected=CONFIG_VERSION,
                actual=manifest.get("config_version"),
            )
        if expected_shard is not None and shard_id != expected_shard:
            fail(
                "E_STALE_CHECKPOINT",
                "artifact shard id does not match checkpoint shard id",
                shard_id=expected_shard,
                stage=stage,
                artifact_shard_id=shard_id,
            )
        if stage not in STAGE_INDEX:
            fail(
                "E_INVALID_STAGE",
                "artifact has an unknown stage",
                shard_id=shard_id,
                stage=stage,
            )
        file_hashes = manifest.get("files")
        if not isinstance(file_hashes, dict):
            fail(
                "E_STALE_CHECKPOINT",
                "artifact file summary is missing",
                shard_id=shard_id,
                stage=stage,
            )
        base = {
            "schema": manifest.get("schema"),
            "shard_id": shard_id,
            "stage": stage,
            "input_fingerprint": manifest.get("input_fingerprint"),
            "config_version": manifest.get("config_version"),
            "files": file_hashes,
        }
        actual_digest = digest_obj(base)
        if actual_digest != manifest.get("artifact_digest"):
            fail(
                "E_STALE_CHECKPOINT",
                "artifact digest does not match its manifest",
                shard_id=shard_id,
                stage=stage,
                expected=manifest.get("artifact_digest"),
                actual=actual_digest,
            )
        for name, expected_hash in sorted(file_hashes.items()):
            file_path = directory / name
            if not file_path.is_file():
                fail(
                    "E_STALE_CHECKPOINT",
                    f"artifact payload is missing: {name}",
                    shard_id=shard_id,
                    stage=stage,
                    file=name,
                )
            actual_hash = sha256_file(file_path)
            if actual_hash != expected_hash:
                fail(
                    "E_STALE_CHECKPOINT",
                    f"artifact payload digest mismatch: {name}",
                    shard_id=shard_id,
                    stage=stage,
                    file=name,
                    expected=expected_hash,
                    actual=actual_hash,
                )
        return Artifact(
            shard_id=shard_id,
            stage=stage,
            input_fingerprint=str(manifest.get("input_fingerprint")),
            artifact_digest=str(manifest.get("artifact_digest")),
            relpath=str(directory.relative_to(self.state_dir)),
            directory=directory,
            files={str(k): str(v) for k, v in file_hashes.items()},
        )

    def load_if_valid(self, directory: Path, *, strict: bool = False) -> Artifact | None:
        if not (directory / "manifest.json").is_file():
            return None
        try:
            return self.load(directory, strict=True)
        except WorkbenchError:
            if strict:
                raise
            return None


class CheckpointStore:
    """Per-shard atomic ledger with a chained checkpoint history."""

    def __init__(self, state_dir: Path) -> None:
        self.state_dir = state_dir
        self.root = state_dir / "shards"
        self.artifacts = ArtifactStore(state_dir)

    def _shard_dir(self, shard_id: str) -> Path:
        return self.root / safe_component(shard_id)

    def _lock_path(self, shard_id: str) -> Path:
        return self._shard_dir(shard_id) / "publish.lock"

    def _ledger_path(self, shard_id: str) -> Path:
        return self._shard_dir(shard_id) / "ledger.json"

    def _watermark_path(self, shard_id: str) -> Path:
        return self._shard_dir(shard_id) / "highwater.json"

    def lock(self, shard_id: str, *, shared: bool = False) -> FileLock:
        return FileLock(self._lock_path(shard_id), shared=shared)

    def _initial_ledger(self, shard_id: str) -> dict[str, Any]:
        return {
            "schema": LEDGER_SCHEMA,
            "shard_id": shard_id,
            "seq": 0,
            "epoch": 0,
            "commits": [],
            "current": {},
            "last_record_digest": "",
        }

    @staticmethod
    def _record_base(record: dict[str, Any]) -> dict[str, Any]:
        return {
            "schema": record["schema"],
            "shard_id": record["shard_id"],
            "seq": record["seq"],
            "epoch": record["epoch"],
            "stage": record["stage"],
            "stage_index": record["stage_index"],
            "input_fingerprint": record["input_fingerprint"],
            "config_version": record["config_version"],
            "artifact_digest": record["artifact_digest"],
            "artifact_relpath": record["artifact_relpath"],
            "prerequisite_digest": record.get("prerequisite_digest"),
            "prev_record_digest": record["prev_record_digest"],
        }

    def _validate_ledger(self, ledger: dict[str, Any], shard_id: str) -> None:
        if ledger.get("schema") != LEDGER_SCHEMA or ledger.get("shard_id") != shard_id:
            fail(
                "E_STALE_CHECKPOINT",
                "checkpoint ledger identity or schema mismatch",
                shard_id=shard_id,
                stage="checkpoint",
            )
        seq = ledger.get("seq")
        commits = ledger.get("commits")
        if not isinstance(seq, int) or not isinstance(commits, list) or seq != len(commits):
            fail(
                "E_STALE_CHECKPOINT",
                "checkpoint sequence is not continuous",
                shard_id=shard_id,
                stage="checkpoint",
                seq=seq,
                commit_count=len(commits) if isinstance(commits, list) else None,
            )
        previous = ""
        latest_by_stage: dict[str, dict[str, Any]] = {}
        for index, record in enumerate(commits, start=1):
            if not isinstance(record, dict):
                fail(
                    "E_STALE_CHECKPOINT",
                    "checkpoint record is not an object",
                    shard_id=shard_id,
                    stage="checkpoint",
                )
            if record.get("seq") != index:
                fail(
                    "E_STALE_CHECKPOINT",
                    "checkpoint record sequence is not contiguous",
                    shard_id=shard_id,
                    stage="checkpoint",
                    expected=index,
                    actual=record.get("seq"),
                )
            if record.get("shard_id") != shard_id:
                fail(
                    "E_STALE_CHECKPOINT",
                    "checkpoint record belongs to another shard",
                    shard_id=shard_id,
                    stage=record.get("stage"),
                    record_shard_id=record.get("shard_id"),
                )
            if record.get("prev_record_digest") != previous:
                fail(
                    "E_STALE_CHECKPOINT",
                    "checkpoint digest chain is broken",
                    shard_id=shard_id,
                    stage=record.get("stage"),
                    seq=index,
                )
            expected_digest = sha256_bytes(canonical_json_bytes(self._record_base(record)))
            if record.get("record_digest") != expected_digest:
                fail(
                    "E_STALE_CHECKPOINT",
                    "checkpoint record digest mismatch",
                    shard_id=shard_id,
                    stage=record.get("stage"),
                    seq=index,
                    expected=expected_digest,
                    actual=record.get("record_digest"),
                )
            previous = expected_digest
            latest_by_stage[str(record.get("stage"))] = record
        if ledger.get("last_record_digest", "") != previous:
            fail(
                "E_STALE_CHECKPOINT",
                "checkpoint ledger tail digest mismatch",
                shard_id=shard_id,
                stage="checkpoint",
            )
        current = ledger.get("current")
        if not isinstance(current, dict):
            fail(
                "E_STALE_CHECKPOINT",
                "checkpoint current-stage map is missing",
                shard_id=shard_id,
                stage="checkpoint",
            )
        for stage, record in current.items():
            if stage not in STAGE_INDEX or not isinstance(record, dict):
                fail(
                    "E_STALE_CHECKPOINT",
                    "checkpoint current-stage map is invalid",
                    shard_id=shard_id,
                    stage=stage,
                )
            expected_digest = sha256_bytes(canonical_json_bytes(self._record_base(record)))
            if record.get("record_digest") != expected_digest:
                fail(
                    "E_STALE_CHECKPOINT",
                    "current-stage checkpoint digest mismatch",
                    shard_id=shard_id,
                    stage=stage,
                )
            latest = latest_by_stage.get(stage)
            if latest is None or latest.get("record_digest") != record.get("record_digest"):
                fail(
                    "E_STALE_CHECKPOINT",
                    "current-stage checkpoint does not point at the latest stage commit",
                    shard_id=shard_id,
                    stage=stage,
                    current_seq=record.get("seq"),
                    latest_seq=None if latest is None else latest.get("seq"),
                )

    def _read_ledger_locked(self, shard_id: str) -> dict[str, Any] | None:
        path = self._ledger_path(shard_id)
        highwater_path = self._watermark_path(shard_id)
        if not path.is_file():
            if highwater_path.is_file():
                fail(
                    "E_STALE_CHECKPOINT",
                    "checkpoint ledger was removed after being committed",
                    shard_id=shard_id,
                    stage="checkpoint",
                )
            return None
        try:
            ledger = read_json(path)
        except Exception as exc:
            raise WorkbenchError(
                "E_STALE_CHECKPOINT",
                f"checkpoint ledger is unreadable: {exc}",
                shard_id=shard_id,
                stage="checkpoint",
                details={"path": str(path)},
            ) from exc
        self._validate_ledger(ledger, shard_id)
        if highwater_path.is_file():
            try:
                highwater = read_json(highwater_path)
            except Exception as exc:
                raise WorkbenchError(
                    "E_STALE_CHECKPOINT",
                    f"checkpoint high-water mark is unreadable: {exc}",
                    shard_id=shard_id,
                    stage="checkpoint",
                ) from exc
            high_seq = highwater.get("seq")
            if not isinstance(high_seq, int) or ledger["seq"] < high_seq:
                fail(
                    "E_STALE_CHECKPOINT",
                    "checkpoint sequence rolled back below the high-water mark",
                    shard_id=shard_id,
                    stage="checkpoint",
                    ledger_seq=ledger["seq"],
                    highwater_seq=high_seq,
                )
            if ledger["seq"] == high_seq and highwater.get("last_record_digest") != ledger.get(
                "last_record_digest"
            ):
                fail(
                    "E_STALE_CHECKPOINT",
                    "checkpoint digest differs from the high-water mark",
                    shard_id=shard_id,
                    stage="checkpoint",
                    seq=ledger["seq"],
                )
            if ledger["seq"] > high_seq:
                self._write_highwater(shard_id, ledger)
        else:
            self._write_highwater(shard_id, ledger)
        return ledger

    def _write_ledger(self, shard_id: str, ledger: dict[str, Any]) -> None:
        atomic_write_json(self._ledger_path(shard_id), ledger)

    def _write_highwater(self, shard_id: str, ledger: dict[str, Any]) -> None:
        atomic_write_json(
            self._watermark_path(shard_id),
            {
                "schema": "vc-highwater-v1",
                "shard_id": shard_id,
                "seq": ledger["seq"],
                "last_record_digest": ledger.get("last_record_digest", ""),
            },
        )

    def begin_rebuild(self, shard_id: str, earliest_stage: str) -> dict[str, int]:
        if earliest_stage not in STAGE_INDEX:
            fail("E_INVALID_STAGE", f"unknown stage {earliest_stage}", shard_id=shard_id, stage=earliest_stage)
        with self.lock(shard_id):
            ledger = self._read_ledger_locked(shard_id)
            if ledger is None:
                ledger = self._initial_ledger(shard_id)
            ledger["epoch"] = int(ledger.get("epoch", 0)) + 1
            start_index = STAGE_INDEX[earliest_stage]
            for stage in STAGES[start_index:]:
                ledger["current"].pop(stage, None)
            self._write_ledger(shard_id, ledger)
            self._write_highwater(shard_id, ledger)
            return {"epoch": ledger["epoch"], "next_seq": ledger["seq"] + 1}

    def publish(
        self,
        *,
        shard_id: str,
        stage: str,
        expected_seq: int,
        expected_epoch: int,
        input_fingerprint: str,
        artifact: Artifact,
        prerequisite_digest: str | None,
    ) -> dict[str, Any]:
        if stage not in STAGE_INDEX:
            fail("E_INVALID_STAGE", f"unknown stage {stage}", shard_id=shard_id, stage=stage)
        with self.lock(shard_id):
            ledger = self._read_ledger_locked(shard_id)
            if ledger is None:
                fail(
                    "E_STALE_CHECKPOINT",
                    "checkpoint ledger is missing; a rebuild must be opened first",
                    shard_id=shard_id,
                    stage=stage,
                )
            if expected_epoch != ledger["epoch"]:
                fail(
                    "E_STALE_CHECKPOINT",
                    "checkpoint epoch is stale",
                    shard_id=shard_id,
                    stage=stage,
                    expected_epoch=expected_epoch,
                    actual_epoch=ledger["epoch"],
                )
            if expected_seq != ledger["seq"] + 1:
                fail(
                    "E_STALE_CHECKPOINT",
                    "checkpoint sequence is not the next contiguous value",
                    shard_id=shard_id,
                    stage=stage,
                    expected_seq=expected_seq,
                    actual_next_seq=ledger["seq"] + 1,
                )
            stage_index = STAGE_INDEX[stage]
            if stage_index > 0:
                previous_stage = STAGES[stage_index - 1]
                previous_record = ledger["current"].get(previous_stage)
                if previous_record is None:
                    fail(
                        "E_INVALID_STAGE",
                        f"prerequisite stage is missing: {previous_stage}",
                        shard_id=shard_id,
                        stage=stage,
                        prerequisite_stage=previous_stage,
                    )
                if prerequisite_digest != previous_record.get("artifact_digest"):
                    fail(
                        "E_INVALID_STAGE",
                        "prerequisite stage digest does not match",
                        shard_id=shard_id,
                        stage=stage,
                        prerequisite_stage=previous_stage,
                        expected=previous_record.get("artifact_digest"),
                        actual=prerequisite_digest,
                    )
            if stage in ledger["current"]:
                fail(
                    "E_STALE_CHECKPOINT",
                    "duplicate checkpoint publication for the current stage",
                    shard_id=shard_id,
                    stage=stage,
                )
            for downstream in STAGES[stage_index + 1 :]:
                if downstream in ledger["current"]:
                    fail(
                        "E_STALE_CHECKPOINT",
                        "downstream checkpoint must be invalidated before rebuilding",
                        shard_id=shard_id,
                        stage=stage,
                        downstream_stage=downstream,
                    )
            verified_artifact = self.artifacts.load(artifact.directory, expected_shard=shard_id, strict=True)
            if verified_artifact.artifact_digest != artifact.artifact_digest:
                fail(
                    "E_STALE_CHECKPOINT",
                    "artifact digest changed before checkpoint publication",
                    shard_id=shard_id,
                    stage=stage,
                )
            if artifact.input_fingerprint != input_fingerprint:
                fail(
                    "E_INVALID_STAGE",
                    "artifact input fingerprint does not match checkpoint input",
                    shard_id=shard_id,
                    stage=stage,
                )
            seq = ledger["seq"] + 1
            base = {
                "schema": CHECKPOINT_SCHEMA,
                "shard_id": shard_id,
                "seq": seq,
                "epoch": ledger["epoch"],
                "stage": stage,
                "stage_index": stage_index,
                "input_fingerprint": input_fingerprint,
                "config_version": CONFIG_VERSION,
                "artifact_digest": artifact.artifact_digest,
                "artifact_relpath": artifact.relpath,
                "prerequisite_digest": prerequisite_digest,
                "prev_record_digest": ledger.get("last_record_digest", ""),
            }
            record = dict(base)
            record["record_digest"] = sha256_bytes(canonical_json_bytes(base))
            ledger["commits"].append(record)
            ledger["seq"] = seq
            ledger["current"][stage] = record
            ledger["last_record_digest"] = record["record_digest"]
            self._write_ledger(shard_id, ledger)
            self._write_highwater(shard_id, ledger)
            return record

    def read_ledger(self, shard_id: str) -> dict[str, Any] | None:
        # Reads may advance a lagging high-water mark after a crash, so use the
        # same exclusive lock as publishers.
        with self.lock(shard_id):
            return self._read_ledger_locked(shard_id)

    def current(self, shard_id: str, stage: str, *, verify_artifact: bool = True) -> dict[str, Any] | None:
        ledger = self.read_ledger(shard_id)
        if ledger is None:
            return None
        record = ledger["current"].get(stage)
        if record is None:
            return None
        if verify_artifact:
            self.artifacts.load(
                self.state_dir / record["artifact_relpath"],
                expected_shard=shard_id,
                strict=True,
            )
        return record

    def load_current_artifact(self, shard_id: str, stage: str) -> Artifact | None:
        record = self.current(shard_id, stage, verify_artifact=False)
        if record is None:
            return None
        artifact = self.artifacts.load(
            self.state_dir / record["artifact_relpath"],
            expected_shard=shard_id,
            strict=True,
        )
        if artifact.artifact_digest != record["artifact_digest"]:
            fail(
                "E_STALE_CHECKPOINT",
                "checkpoint artifact digest does not match current artifact",
                shard_id=shard_id,
                stage=stage,
                checkpoint_digest=record["artifact_digest"],
                artifact_digest=artifact.artifact_digest,
            )
        return artifact

    def status(self, shard_id: str, *, verify_artifacts: bool = True) -> dict[str, Any]:
        try:
            ledger = self.read_ledger(shard_id)
            if ledger is None:
                return {
                    "shard_id": shard_id,
                    "seq": 0,
                    "epoch": 0,
                    "current": {},
                    "stage_status": {stage: "pending" for stage in STAGES},
                }
            result: dict[str, Any] = {
                "shard_id": shard_id,
                "seq": ledger["seq"],
                "epoch": ledger["epoch"],
                "current": ledger["current"],
                "stage_status": {},
            }
            for stage in STAGES:
                record = ledger["current"].get(stage)
                if record is None:
                    result["stage_status"][stage] = "pending"
                    continue
                if verify_artifacts:
                    try:
                        self.artifacts.load(
                            self.state_dir / record["artifact_relpath"],
                            expected_shard=shard_id,
                            strict=True,
                        )
                        result["stage_status"][stage] = "complete"
                    except WorkbenchError as exc:
                        result["stage_status"][stage] = "stale"
                        result.setdefault("stage_errors", {})[stage] = exc.to_dict()["error"]
                else:
                    result["stage_status"][stage] = "complete"
            return result
        except WorkbenchError as exc:
            return {
                "shard_id": shard_id,
                "error": exc.to_dict()["error"],
                "stage_status": {stage: "stale" for stage in STAGES},
            }

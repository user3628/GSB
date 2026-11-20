from __future__ import annotations

from pathlib import Path
from typing import Any, Mapping

from .errors import BusinessError
from .fingerprints import STAGES
from .locking import FileLock, FileLockTimeout
from .util import (
    atomic_write_json,
    digest_file,
    read_json,
    sha256_json,
)


STATE_SCHEMA_VERSION = 1


def _default_state() -> dict[str, Any]:
    return {
        "schema_version": STATE_SCHEMA_VERSION,
        "last_seq": 0,
        "checkpoints": {},
        "stage_fingerprints": {},
        "last_invalidation": None,
        "invalidation_history": [],
    }


class CheckpointStore:
    def __init__(self, workdir: Path, lock_timeout: float = 10.0) -> None:
        self.workdir = Path(workdir).resolve()
        self.workdir.mkdir(parents=True, exist_ok=True)
        self.checkpoint_dir = self.workdir / "checkpoints"
        self.artifact_dir = self.workdir / "artifacts"
        self.lock_dir = self.workdir / "locks"
        self.temp_dir = self.workdir / "tmp"
        for path in (self.checkpoint_dir, self.artifact_dir, self.lock_dir, self.temp_dir):
            path.mkdir(parents=True, exist_ok=True)
        self.state_path = self.workdir / "state.json"
        self.stop_path = self.workdir / "STOP_REQUESTED"
        self.run_lock_path = self.lock_dir / "run.lock"
        self.publish_lock_path = self.lock_dir / "publish.lock"
        self.lock_timeout = lock_timeout

    def load_state(self) -> dict[str, Any]:
        state = read_json(self.state_path, _default_state())
        if not isinstance(state, dict):
            raise BusinessError("E_INVALID_STATE", "state.json root must be an object")
        default = _default_state()
        for key, value in default.items():
            state.setdefault(key, value)
        return state

    def save_state(self, state: dict[str, Any]) -> None:
        atomic_write_json(self.state_path, state)

    def run_lock(self) -> FileLock:
        return FileLock(self.run_lock_path, timeout=self.lock_timeout)

    def publish_lock(self) -> FileLock:
        return FileLock(self.publish_lock_path, timeout=self.lock_timeout)

    def checkpoint_path(self, seq: int, stage: str) -> Path:
        if stage not in STAGES:
            raise BusinessError("E_INVALID_STAGE", "unknown stage", {"stage": stage, "known": list(STAGES)})
        return self.checkpoint_dir / f"{seq:012d}-{stage}.json"

    def artifact_path(self, relative: str) -> Path:
        path = (self.workdir / relative).resolve()
        try:
            path.relative_to(self.workdir)
        except ValueError as exc:
            raise BusinessError("E_INVALID_STAGE", "artifact path escapes workdir", {"path": relative}) from exc
        return path

    def artifact_validation(self, artifacts: Mapping[str, str] | None) -> tuple[bool, list[dict[str, Any]]]:
        if not artifacts:
            return False, [{"reason": "no_artifacts"}]
        problems: list[dict[str, Any]] = []
        for relative, expected in artifacts.items():
            path = self.artifact_path(relative)
            if not path.exists():
                problems.append({"path": relative, "reason": "missing"})
                continue
            actual = digest_file(path)
            if actual != expected:
                problems.append(
                    {"path": relative, "reason": "sha256_mismatch", "expected": expected, "actual": actual}
                )
        return not problems, problems

    def checkpoint_artifact_validation(
        self, checkpoint: Mapping[str, Any] | None
    ) -> tuple[bool, list[dict[str, Any]]]:
        if not checkpoint:
            return False, [{"reason": "missing_checkpoint"}]
        return self.artifact_validation(checkpoint.get("artifacts"))

    def get_checkpoint(self, stage: str, state: dict[str, Any] | None = None) -> dict[str, Any] | None:
        state = state if state is not None else self.load_state()
        checkpoint = state.get("checkpoints", {}).get(stage)
        return checkpoint if isinstance(checkpoint, dict) else None

    def publish_stage(
        self,
        record: Mapping[str, Any],
        *,
        seq: int | None = None,
    ) -> dict[str, Any]:
        stage = str(record.get("stage", ""))
        if stage not in STAGES:
            raise BusinessError(
                "E_INVALID_STAGE", "unknown stage", {"stage": stage, "known": list(STAGES)}
            )
        required = (
            "fingerprint",
            "input_fingerprint",
            "config_fingerprint",
            "upstream_fingerprint",
            "output_fingerprint",
            "artifacts",
            "summary",
        )
        missing = [key for key in required if key not in record]
        if missing:
            raise BusinessError(
                "E_INVALID_STAGE", "checkpoint record is incomplete", {"stage": stage, "missing": missing}
            )
        try:
            with self.publish_lock():
                state = self.load_state()
                last_seq = int(state.get("last_seq", 0))
                stage_index = STAGES.index(stage)
                if stage_index > 0:
                    predecessor = STAGES[stage_index - 1]
                    predecessor_checkpoint = state.get("checkpoints", {}).get(predecessor)
                    if not predecessor_checkpoint:
                        raise BusinessError(
                            "E_INVALID_STAGE",
                            "predecessor checkpoint is missing",
                            {"stage": stage, "required_predecessor": predecessor},
                        )
                    valid, problems = self.checkpoint_artifact_validation(predecessor_checkpoint)
                    if not valid:
                        raise BusinessError(
                            "E_INVALID_STAGE",
                            "predecessor artifacts are unavailable or corrupt",
                            {"stage": stage, "required_predecessor": predecessor, "problems": problems},
                        )
                    if predecessor_checkpoint.get("fingerprint") != record.get("upstream_fingerprint"):
                        raise BusinessError(
                            "E_INVALID_STAGE",
                            "checkpoint upstream fingerprint does not match predecessor",
                            {
                                "stage": stage,
                                "required_predecessor": predecessor,
                                "expected_upstream": predecessor_checkpoint.get("fingerprint"),
                                "actual_upstream": record.get("upstream_fingerprint"),
                            },
                        )
                proposed_seq = last_seq + 1 if seq is None else int(seq)
                if proposed_seq <= last_seq or proposed_seq != last_seq + 1:
                    raise BusinessError(
                        "E_STALE_CHECKPOINT",
                        "checkpoint sequence is stale, duplicate, or non-monotonic",
                        {
                            "stage": stage,
                            "proposed_seq": proposed_seq,
                            "last_seq": last_seq,
                            "expected_seq": last_seq + 1,
                        },
                    )
                existing = state.get("checkpoints", {}).get(stage)
                if existing and existing.get("fingerprint") == record["fingerprint"]:
                    existing_valid, _ = self.checkpoint_artifact_validation(existing)
                    if existing_valid:
                        raise BusinessError(
                            "E_STALE_CHECKPOINT",
                            "duplicate stage fingerprint is already published",
                            {
                                "stage": stage,
                                "existing_seq": existing.get("seq"),
                                "proposed_seq": proposed_seq,
                                "fingerprint": record["fingerprint"],
                            },
                        )
                output_valid, output_problems = self.artifact_validation(record["artifacts"])
                if not output_valid:
                    raise BusinessError(
                        "E_INVALID_STAGE",
                        "checkpoint artifacts are unavailable or corrupt",
                        {"stage": stage, "problems": output_problems},
                    )
                checkpoint = {
                    "schema_version": STATE_SCHEMA_VERSION,
                    "stage": stage,
                    "seq": proposed_seq,
                    "input_fingerprint": record["input_fingerprint"],
                    "config_version": record.get("config_version"),
                    "config_fingerprint": record["config_fingerprint"],
                    "upstream_fingerprint": record["upstream_fingerprint"],
                    "fingerprint": record["fingerprint"],
                    "output_fingerprint": record["output_fingerprint"],
                    "artifacts": dict(record["artifacts"]),
                    "summary": dict(record["summary"]),
                    "components": dict(record.get("components", {})),
                    "record_path": str(self.checkpoint_path(proposed_seq, stage).relative_to(self.workdir)),
                }
                record_path = self.workdir / checkpoint["record_path"]
                atomic_write_json(record_path, checkpoint)
                state.setdefault("checkpoints", {})[stage] = checkpoint
                state.setdefault("stage_fingerprints", {})[stage] = checkpoint["fingerprint"]
                state["last_seq"] = proposed_seq
                state["schema_version"] = STATE_SCHEMA_VERSION
                self.save_state(state)
                return checkpoint
        except FileLockTimeout as exc:
            raise BusinessError("E_LOCK_TIMEOUT", "unable to acquire publish lock", {"error": str(exc)}) from exc

    def record_invalidation(
        self,
        *,
        earliest_stage: str,
        affected_stages: list[str],
        reasons: list[dict[str, Any]],
        old_fingerprints: Mapping[str, Any],
        expected_fingerprints: Mapping[str, Any],
    ) -> dict[str, Any]:
        with self.publish_lock():
            state = self.load_state()
            event = {
                "event_id": f"inv-{int(state.get('last_seq', 0)):012d}-{earliest_stage}",
                "earliest_stage": earliest_stage,
                "affected_stages": affected_stages,
                "reasons": reasons,
                "old_fingerprints": dict(old_fingerprints),
                "expected_fingerprints": dict(expected_fingerprints),
            }
            state["last_invalidation"] = event
            history = list(state.get("invalidation_history", []))
            history.append(event)
            state["invalidation_history"] = history[-20:]
            self.save_state(state)
            return event

    def stop_requested(self) -> bool:
        return self.stop_path.exists()

    def request_stop(self, reason: str = "operator_request") -> dict[str, Any]:
        payload = {"reason": reason}
        atomic_write_json(self.stop_path, payload)
        return {"stop_requested": True, **payload, "path": str(self.stop_path)}

    def clear_stop(self) -> None:
        try:
            self.stop_path.unlink()
        except FileNotFoundError:
            pass

    def state_fingerprint(self, state: dict[str, Any] | None = None) -> str:
        state = state if state is not None else self.load_state()
        stable = {
            "last_seq": state.get("last_seq", 0),
            "checkpoints": {
                stage: {
                    "seq": checkpoint.get("seq"),
                    "fingerprint": checkpoint.get("fingerprint"),
                    "output_fingerprint": checkpoint.get("output_fingerprint"),
                }
                for stage, checkpoint in sorted(state.get("checkpoints", {}).items())
            },
        }
        return sha256_json(stable)

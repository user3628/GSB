from __future__ import annotations

import json
import re
import shutil
from pathlib import Path
from typing import Any, Iterable

from .errors import (
    digest_mismatch,
    invalid_stage,
    sequence_regression,
    stale_checkpoint,
    version_replaced,
)
from .util import (
    FileLock,
    artifact_digest,
    atomic_write_bytes,
    atomic_write_json,
    canonical_json_bytes,
    ensure_workspace,
    read_json,
    sha256_bytes,
    sha256_file,
)

RECORD_SCHEMA = "state-record-v1"
LEDGER_SCHEMA = "state-ledger-v1"
STAGE_ORDER = ("preprocess", "reduce", "cluster", "diagnose", "report")


def _safe_name(value: str) -> str:
    if value in {"*", "all"}:
        return "all"
    cleaned = re.sub(r"[^A-Za-z0-9_.-]+", "_", value)
    return cleaned or "unknown"


def _record_digest(record_without_digest: dict[str, Any]) -> str:
    return sha256_bytes(canonical_json_bytes(record_without_digest))


class StateStore:
    def __init__(self, state_dir: Path, root: Path | None = None) -> None:
        self.state_dir = state_dir.resolve()
        self.root = root.resolve() if root is not None else self.state_dir.parent.resolve()
        self.paths = ensure_workspace(self.root)
        self.checkpoints_dir = self.state_dir / "checkpoints"
        self.artifacts_dir = self.state_dir / "artifacts"
        self.ledger_dir = self.state_dir / "ledger"
        self.tmp_dir = self.state_dir / "tmp"
        for path in (
            self.checkpoints_dir,
            self.artifacts_dir,
            self.ledger_dir,
            self.tmp_dir,
        ):
            path.mkdir(parents=True, exist_ok=True)
        self.lock_path = self.state_dir / "store.lock"

    def _checkpoint_path(self, stage: str, tier: str) -> Path:
        return self.checkpoints_dir / f"{_safe_name(tier)}__{_safe_name(stage)}.json"

    def _checkpoint_files(self) -> list[Path]:
        return sorted(self.checkpoints_dir.glob("*.json"))

    def _ledger_files(self) -> list[Path]:
        return sorted(self.ledger_dir.glob("*.json"))

    def _read_checkpoint_file(self, path: Path) -> dict[str, Any]:
        try:
            record = read_json(path)
        except (OSError, json.JSONDecodeError) as exc:
            raise digest_mismatch(
                "Checkpoint is unreadable or corrupt", {"path": str(path)}
            ) from exc
        stored_digest = record.get("record_digest")
        base = {key: value for key, value in record.items() if key != "record_digest"}
        actual = _record_digest(base)
        if stored_digest != actual:
            raise digest_mismatch(
                "Checkpoint record digest mismatch",
                {"path": str(path), "stored": stored_digest, "actual": actual},
            )
        return record

    def _read_ledger_entries(self) -> list[dict[str, Any]]:
        entries: list[dict[str, Any]] = []
        for path in self._ledger_files():
            try:
                entry = read_json(path)
            except (OSError, json.JSONDecodeError):
                continue
            if entry.get("schema_version") == LEDGER_SCHEMA:
                entries.append(entry)
        return entries

    def _latest_ledger_map(
        self, entries: Iterable[dict[str, Any]]
    ) -> dict[tuple[str, str], dict[str, Any]]:
        latest: dict[tuple[str, str], dict[str, Any]] = {}
        for entry in entries:
            key = (str(entry["stage"]), str(entry["tier"]))
            current = latest.get(key)
            if current is None or int(entry["sequence"]) > int(current["sequence"]):
                latest[key] = entry
        return latest

    def _max_sequence(self) -> int:
        maximum = 0
        for path in self._checkpoint_files():
            try:
                maximum = max(maximum, int(read_json(path).get("sequence", 0)))
            except (OSError, json.JSONDecodeError, TypeError, ValueError):
                continue
        for entry in self._read_ledger_entries():
            try:
                maximum = max(maximum, int(entry.get("sequence", 0)))
            except (TypeError, ValueError):
                continue
        return maximum

    def _write_ledger(self, record: dict[str, Any]) -> Path:
        entry = {
            "schema_version": LEDGER_SCHEMA,
            "sequence": int(record["sequence"]),
            "stage": record["stage"],
            "tier": record["tier"],
            "record_digest": record["record_digest"],
            "input_fingerprint": record["input_fingerprint"],
            "config_fingerprint": record["config_fingerprint"],
            "artifact_digest": record["artifact_digest"],
        }
        filename = (
            f"{int(record['sequence']):020d}--{_safe_name(record['tier'])}--"
            f"{_safe_name(record['stage'])}--{record['record_digest'][:16]}.json"
        )
        path = self.ledger_dir / filename
        atomic_write_json(path, entry)
        return path

    def _verify_against_ledger(
        self,
        record: dict[str, Any],
        latest_ledger: dict[tuple[str, str], dict[str, Any]],
        *,
        allow_unledgered: bool,
    ) -> None:
        key = (str(record["stage"]), str(record["tier"]))
        entry = latest_ledger.get(key)
        if entry is None:
            if allow_unledgered:
                return
            raise invalid_stage(
                "Checkpoint has no corresponding ledger entry",
                {"stage": record["stage"], "tier": record["tier"]},
            )
        record_sequence = int(record["sequence"])
        ledger_sequence = int(entry["sequence"])
        if record_sequence < ledger_sequence:
            raise sequence_regression(
                "Checkpoint sequence is older than the ledger head",
                {
                    "stage": record["stage"],
                    "tier": record["tier"],
                    "checkpoint_sequence": record_sequence,
                    "ledger_sequence": ledger_sequence,
                },
            )
        if record_sequence > ledger_sequence:
            if allow_unledgered:
                return
            raise invalid_stage(
                "Ledger is missing the current checkpoint sequence",
                {
                    "stage": record["stage"],
                    "tier": record["tier"],
                    "checkpoint_sequence": record_sequence,
                    "ledger_sequence": ledger_sequence,
                },
            )
        if record["record_digest"] != entry["record_digest"]:
            raise version_replaced(
                "Checkpoint was replaced after publication",
                {
                    "stage": record["stage"],
                    "tier": record["tier"],
                    "checkpoint_record_digest": record["record_digest"],
                    "ledger_record_digest": entry["record_digest"],
                },
            )

    def _reconcile_ledger(self) -> None:
        records: list[dict[str, Any]] = []
        for path in self._checkpoint_files():
            records.append(self._read_checkpoint_file(path))
        ledger_entries = self._read_ledger_entries()
        latest = self._latest_ledger_map(ledger_entries)
        by_sequence = {int(entry["sequence"]): entry for entry in ledger_entries}
        for record in records:
            self._verify_against_ledger(
                record, latest, allow_unledgered=True
            )
            existing = by_sequence.get(int(record["sequence"]))
            if existing is None:
                self._write_ledger(record)
            elif existing["record_digest"] != record["record_digest"]:
                raise version_replaced(
                    "Sequence is already occupied by a different record",
                    {
                        "sequence": record["sequence"],
                        "stage": record["stage"],
                        "tier": record["tier"],
                    },
                )

    def _artifact_destination(self, stage: str, tier: str, digest: str) -> Path:
        return (
            self.artifacts_dir
            / _safe_name(tier)
            / _safe_name(stage)
            / digest[:24]
        )

    def _materialize_artifacts(
        self,
        stage: str,
        tier: str,
        files: dict[str, bytes],
    ) -> tuple[str, dict[str, dict[str, Any]]]:
        file_digests = {
            name: sha256_bytes(data) for name, data in sorted(files.items())
        }
        digest = artifact_digest(file_digests)
        destination = self._artifact_destination(stage, tier, digest)
        destination.mkdir(parents=True, exist_ok=True)
        artifact_files: dict[str, dict[str, Any]] = {}
        for name in sorted(files):
            path = destination / name
            data = files[name]
            if path.exists():
                if sha256_file(path) != file_digests[name]:
                    raise digest_mismatch(
                        "Content-addressed artifact file was modified",
                        {"path": str(path), "artifact": name},
                    )
            else:
                atomic_write_bytes(path, data)
            artifact_files[name] = {
                "path": str(path.relative_to(self.state_dir)),
                "sha256": file_digests[name],
                "bytes": len(data),
            }
        return digest, artifact_files

    def publish(
        self,
        *,
        stage: str,
        tier: str,
        input_fingerprint: str,
        config_fingerprint: str,
        config_version: str,
        files: dict[str, bytes],
        predecessors: list[dict[str, str]] | None = None,
        expected_current_digest: str | None = None,
        expected_current_sequence: int | None = None,
    ) -> dict[str, Any]:
        if stage not in STAGE_ORDER:
            raise invalid_stage("Unknown pipeline stage", {"stage": stage})
        predecessors = predecessors or []
        with FileLock(self.lock_path):
            self._reconcile_ledger()
            ledger_entries = self._read_ledger_entries()
            latest_ledger = self._latest_ledger_map(ledger_entries)
            for predecessor in predecessors:
                key = (predecessor["stage"], predecessor["tier"])
                entry = latest_ledger.get(key)
                if entry is None:
                    raise invalid_stage(
                        "Missing prerequisite stage artifact",
                        {"required_stage": predecessor["stage"], "required_tier": predecessor["tier"]},
                    )
                if predecessor.get("artifact_digest") and (
                    entry["artifact_digest"] != predecessor["artifact_digest"]
                ):
                    raise invalid_stage(
                        "Prerequisite artifact changed before publication",
                        {
                            "required_stage": predecessor["stage"],
                            "required_tier": predecessor["tier"],
                            "expected": predecessor["artifact_digest"],
                            "actual": entry["artifact_digest"],
                        },
                    )
            checkpoint_path = self._checkpoint_path(stage, tier)
            current: dict[str, Any] | None = None
            if checkpoint_path.exists():
                current = self._read_checkpoint_file(checkpoint_path)
                self._verify_against_ledger(current, latest_ledger, allow_unledgered=True)
            if expected_current_sequence is not None and current is not None:
                if int(current["sequence"]) != int(expected_current_sequence):
                    raise stale_checkpoint(
                        "Checkpoint sequence advanced after the caller read it",
                        {
                            "stage": stage,
                            "tier": tier,
                            "expected_sequence": expected_current_sequence,
                            "actual_sequence": current["sequence"],
                        },
                    )

            artifact_digest_value, artifact_files = self._materialize_artifacts(
                stage, tier, files
            )
            if current is not None:
                if (
                    current["input_fingerprint"] == input_fingerprint
                    and current["config_fingerprint"] == config_fingerprint
                    and current["artifact_digest"] == artifact_digest_value
                ):
                    raise stale_checkpoint(
                        "This stage version is already published",
                        {
                            "stage": stage,
                            "tier": tier,
                            "sequence": current["sequence"],
                            "artifact_digest": artifact_digest_value,
                        },
                    )
                if expected_current_digest is None:
                    raise version_replaced(
                        "An existing checkpoint would be replaced without a read guard",
                        {
                            "stage": stage,
                            "tier": tier,
                            "existing_digest": current["artifact_digest"],
                        },
                    )
                if current["artifact_digest"] != expected_current_digest:
                    raise version_replaced(
                        "Checkpoint changed after it was read for recomputation",
                        {
                            "stage": stage,
                            "tier": tier,
                            "expected_current_digest": expected_current_digest,
                            "actual_current_digest": current["artifact_digest"],
                        },
                    )
                if (
                    current["input_fingerprint"] == input_fingerprint
                    and current["config_fingerprint"] == config_fingerprint
                ):
                    raise digest_mismatch(
                        "Same input/config produced a different artifact digest",
                        {
                            "stage": stage,
                            "tier": tier,
                            "existing_digest": current["artifact_digest"],
                            "new_digest": artifact_digest_value,
                        },
                    )
            elif expected_current_digest is not None:
                raise version_replaced(
                    "Expected checkpoint disappeared before publication",
                    {
                        "stage": stage,
                        "tier": tier,
                        "expected_current_digest": expected_current_digest,
                    },
                )

            sequence = self._max_sequence() + 1
            record_base: dict[str, Any] = {
                "schema_version": RECORD_SCHEMA,
                "sequence": sequence,
                "stage": stage,
                "tier": tier,
                "state": "complete",
                "input_fingerprint": input_fingerprint,
                "config_fingerprint": config_fingerprint,
                "config_version": config_version,
                "artifact_digest": artifact_digest_value,
                "artifacts": artifact_files,
                "predecessors": predecessors,
            }
            record = dict(record_base)
            record["record_digest"] = _record_digest(record_base)
            atomic_write_json(checkpoint_path, record)
            self._write_ledger(record)
            return record

    def load_latest(
        self,
        stage: str,
        tier: str,
        *,
        verify_artifacts: bool = True,
    ) -> dict[str, Any] | None:
        path = self._checkpoint_path(stage, tier)
        if not path.exists():
            return None
        record = self._read_checkpoint_file(path)
        ledger_entries = self._read_ledger_entries()
        latest_ledger = self._latest_ledger_map(ledger_entries)
        self._verify_against_ledger(record, latest_ledger, allow_unledgered=True)
        resolved: dict[str, Path] = {}
        if verify_artifacts:
            for name, info in record["artifacts"].items():
                artifact_path = self.state_dir / info["path"]
                if not artifact_path.exists():
                    raise invalid_stage(
                        "Checkpoint artifact file is missing",
                        {"stage": stage, "tier": tier, "artifact": name, "path": str(artifact_path)},
                    )
                actual = sha256_file(artifact_path)
                if actual != info["sha256"]:
                    raise digest_mismatch(
                        "Checkpoint artifact digest mismatch",
                        {
                            "stage": stage,
                            "tier": tier,
                            "artifact": name,
                            "expected": info["sha256"],
                            "actual": actual,
                        },
                    )
                resolved[name] = artifact_path
        result = dict(record)
        result["artifact_paths"] = resolved
        return result

    def list_checkpoints(self, verify: bool = False) -> list[dict[str, Any]]:
        records: list[dict[str, Any]] = []
        for path in self._checkpoint_files():
            try:
                record = self._read_checkpoint_file(path)
            except Exception as exc:
                records.append(
                    {
                        "file": path.name,
                        "ok": False,
                        "error": (
                            exc.to_dict()
                            if hasattr(exc, "to_dict")
                            else {"message": str(exc)}
                        ),
                    }
                )
                continue
            item = {
                "stage": record["stage"],
                "tier": record["tier"],
                "sequence": record["sequence"],
                "record_digest": record["record_digest"],
                "artifact_digest": record["artifact_digest"],
                "input_fingerprint": record["input_fingerprint"],
                "config_fingerprint": record["config_fingerprint"],
                "config_version": record["config_version"],
                "ok": True,
            }
            if verify:
                try:
                    self.load_latest(record["stage"], record["tier"], verify_artifacts=True)
                except Exception as exc:
                    item["ok"] = False
                    item["error"] = (
                        exc.to_dict()
                        if hasattr(exc, "to_dict")
                        else {"message": str(exc)}
                    )
            records.append(item)
        return sorted(records, key=lambda item: int(item.get("sequence", -1)))

    def status(self) -> dict[str, Any]:
        records = self.list_checkpoints(verify=False)
        by_tier: dict[str, dict[str, Any]] = {}
        for record in records:
            if not record.get("ok", False):
                by_tier.setdefault("unknown", {})["error"] = record.get("error")
                continue
            tier = record["tier"]
            by_tier.setdefault(tier, {})[record["stage"]] = {
                "sequence": record["sequence"],
                "artifact_digest": record["artifact_digest"],
                "input_fingerprint": record["input_fingerprint"],
                "config_fingerprint": record["config_fingerprint"],
                "config_version": record["config_version"],
            }
        head = max(
            [int(record["sequence"]) for record in records if record.get("ok")],
            default=0,
        )
        return {
            "ok": True,
            "state_dir": str(self.state_dir),
            "stop_requested": self.stop_requested(),
            "sequence_head": head,
            "checkpoint_count": len([record for record in records if record.get("ok")]),
            "tiers": by_tier,
        }

    def verify_integrity(self, raise_on_error: bool = False) -> dict[str, Any]:
        issues: list[dict[str, Any]] = []
        ledger_entries = self._read_ledger_entries()
        latest_ledger = self._latest_ledger_map(ledger_entries)
        seen_keys: set[tuple[str, str]] = set()
        for path in self._checkpoint_files():
            try:
                record = self._read_checkpoint_file(path)
                seen_keys.add((str(record["stage"]), str(record["tier"])))
                entry = latest_ledger.get((str(record["stage"]), str(record["tier"])))
                if entry is None:
                    issues.append(
                        {
                            "code": "E_INVALID_STAGE",
                            "message": "checkpoint has no ledger entry",
                            "stage": record["stage"],
                            "tier": record["tier"],
                        }
                    )
                elif int(record["sequence"]) < int(entry["sequence"]):
                    issues.append(
                        {
                            "code": "E_SEQUENCE_REGRESSION",
                            "message": "checkpoint sequence is behind ledger head",
                            "stage": record["stage"],
                            "tier": record["tier"],
                            "checkpoint_sequence": record["sequence"],
                            "ledger_sequence": entry["sequence"],
                        }
                    )
                elif record["record_digest"] != entry["record_digest"]:
                    issues.append(
                        {
                            "code": "E_VERSION_REPLACED",
                            "message": "checkpoint record digest differs from ledger",
                            "stage": record["stage"],
                            "tier": record["tier"],
                        }
                    )
                for name, info in record["artifacts"].items():
                    artifact_path = self.state_dir / info["path"]
                    if not artifact_path.exists():
                        issues.append(
                            {
                                "code": "E_DIGEST_MISMATCH",
                                "message": "artifact is missing",
                                "stage": record["stage"],
                                "tier": record["tier"],
                                "artifact": name,
                            }
                        )
                    elif sha256_file(artifact_path) != info["sha256"]:
                        issues.append(
                            {
                                "code": "E_DIGEST_MISMATCH",
                                "message": "artifact digest mismatch",
                                "stage": record["stage"],
                                "tier": record["tier"],
                                "artifact": name,
                            }
                        )
            except Exception as exc:
                issues.append(
                    {
                        "code": getattr(exc, "code", "E_DIGEST_MISMATCH"),
                        "message": str(exc),
                        "file": path.name,
                    }
                )
        observed_sequences: dict[int, dict[str, Any]] = {}
        for entry in ledger_entries:
            key = (str(entry["stage"]), str(entry["tier"]))
            if int(entry["sequence"]) == int(latest_ledger[key]["sequence"]) and key not in seen_keys:
                issues.append(
                    {
                        "code": "E_INVALID_STAGE",
                        "message": "ledger head has no checkpoint record",
                        "stage": entry["stage"],
                        "tier": entry["tier"],
                    }
                )
            sequence = int(entry["sequence"])
            prior = observed_sequences.get(sequence)
            if prior is None:
                observed_sequences[sequence] = entry
            elif (
                prior["record_digest"] != entry["record_digest"]
                or prior["stage"] != entry["stage"]
                or prior["tier"] != entry["tier"]
            ):
                issues.append(
                    {
                        "code": "E_VERSION_REPLACED",
                        "message": "duplicate sequence has conflicting records",
                        "sequence": sequence,
                    }
                )
        result = {
            "ok": not issues,
            "issues": issues,
            "sequence_head": max(
                [int(entry["sequence"]) for entry in ledger_entries], default=0
            ),
            "checkpoint_count": len(self._checkpoint_files()),
            "ledger_count": len(ledger_entries),
        }
        if issues and raise_on_error:
            first = issues[0]
            code = first["code"]
            message = first["message"]
            if code == "E_SEQUENCE_REGRESSION":
                raise sequence_regression(message, first)
            if code == "E_VERSION_REPLACED":
                raise version_replaced(message, first)
            if code == "E_DIGEST_MISMATCH":
                raise digest_mismatch(message, first)
            raise invalid_stage(message, first)
        return result

    def reset(self) -> None:
        with FileLock(self.lock_path):
            for directory in (
                self.checkpoints_dir,
                self.artifacts_dir,
                self.ledger_dir,
                self.tmp_dir,
            ):
                if directory.exists():
                    shutil.rmtree(directory)
                directory.mkdir(parents=True, exist_ok=True)

    def reconcile(self) -> None:
        """Repair the small crash window between checkpoint and ledger publication."""
        with FileLock(self.lock_path):
            self._reconcile_ledger()

    def save_active_config(self, config: dict[str, Any]) -> None:
        atomic_write_json(self.state_dir / "active_config.json", config)

    def load_active_config(self) -> dict[str, Any] | None:
        path = self.state_dir / "active_config.json"
        if not path.exists():
            return None
        value = read_json(path)
        return value if isinstance(value, dict) else None

    def request_stop(self, reason: str = "requested") -> dict[str, Any]:
        path = self.state_dir / "stop.flag"
        payload = {"requested": True, "reason": reason}
        atomic_write_json(path, payload)
        return {"ok": True, "stop_requested": True, "reason": reason}

    def clear_stop(self) -> None:
        path = self.state_dir / "stop.flag"
        try:
            path.unlink()
        except FileNotFoundError:
            pass

    def stop_requested(self) -> bool:
        return (self.state_dir / "stop.flag").exists()

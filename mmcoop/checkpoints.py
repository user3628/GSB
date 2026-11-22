from __future__ import annotations

import json
import os
import re
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from .canonical import atomic_write_bytes, atomic_write_json, canonical_bytes, digest_json, read_json, sha256_bytes
from .errors import AppError, business_error
from .locking import file_lock
from .stages import STAGE_INDEX, STAGES


CHECKPOINT_NAME = re.compile(r"^(?P<sequence>\d{8})-(?P<stage>[a-z_]+)\.json$")


@dataclass
class ScanReport:
    groups: dict[int, list[dict[str, Any]]] = field(default_factory=dict)
    invalid: list[dict[str, Any]] = field(default_factory=list)
    duplicate_sequences: list[int] = field(default_factory=list)
    chain: dict[int, dict[str, Any]] = field(default_factory=dict)
    state_after_sequence: dict[int, dict[str, dict[str, Any]]] = field(default_factory=dict)
    stages: dict[str, dict[str, Any]] = field(default_factory=dict)
    latest_sequence: int = 0
    max_filename_sequence: int = 0

    def public(self) -> dict[str, Any]:
        return {
            "latest_sequence": self.latest_sequence,
            "max_filename_sequence": self.max_filename_sequence,
            "stages": {
                stage: {
                    "sequence": value["sequence"],
                    "input_fingerprint": value["input_fingerprint"],
                    "artifact_sha256": value["artifact_sha256"],
                    "config_version": value["config_version"],
                }
                for stage, value in sorted(self.stages.items())
            },
            "duplicate_sequences": sorted(self.duplicate_sequences),
            "invalid_records": list(self.invalid),
        }


class CheckpointStore:
    def __init__(self, workspace: str | Path) -> None:
        self.workspace = Path(workspace).resolve()
        self.checkpoint_dir = self.workspace / "checkpoints"
        self.artifact_dir = self.workspace / "artifacts"
        self.lock_dir = self.workspace / "locks"
        self.state_dir = self.workspace / "state"
        self.quarantine_dir = self.workspace / "quarantine" / "checkpoints"
        self.publish_lock = self.lock_dir / "publish.lock"
        self.stop_file = self.workspace / "stop.request"
        self.marker_path = self.state_dir / "published.json"
        self.last_run_path = self.state_dir / "last_run.json"

    def ensure(self) -> None:
        for directory in (
            self.workspace,
            self.checkpoint_dir,
            self.artifact_dir,
            self.lock_dir,
            self.state_dir,
            self.quarantine_dir,
        ):
            directory.mkdir(parents=True, exist_ok=True)

    def artifact_relative_path(self, stage: str, digest: str) -> str:
        return f"artifacts/{stage}/{digest}.json"

    def artifact_path(self, stage: str, digest: str) -> Path:
        return self.workspace / self.artifact_relative_path(stage, digest)

    def _read_marker(self) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
        if not self.marker_path.exists():
            return None, None
        try:
            marker = read_json(self.marker_path)
        except (OSError, ValueError, json.JSONDecodeError) as error:
            return None, {"type": "marker_unreadable", "error": str(error)}
        if not isinstance(marker, dict):
            return None, {"type": "marker_not_object"}
        body = dict(marker)
        checksum = body.pop("marker_sha256", None)
        if checksum != digest_json(body):
            return marker, {"type": "marker_checksum_mismatch", "expected": digest_json(body), "actual": checksum}
        return marker, None

    def _candidate_from_path(self, path: Path) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
        match = CHECKPOINT_NAME.match(path.name)
        if not match:
            return None, {"file": path.name, "type": "invalid_filename"}
        filename_sequence = int(match.group("sequence"))
        filename_stage = match.group("stage")
        try:
            raw = read_json(path)
        except (OSError, ValueError, json.JSONDecodeError) as error:
            return None, {"file": path.name, "sequence": filename_sequence, "type": "incomplete_or_invalid_json", "error": str(error)}
        if not isinstance(raw, dict):
            return None, {"file": path.name, "sequence": filename_sequence, "type": "record_not_object"}
        record = dict(raw)
        checksum = record.pop("record_sha256", None)
        if checksum != digest_json(record):
            return None, {
                "file": path.name,
                "sequence": filename_sequence,
                "type": "record_checksum_mismatch",
                "expected": digest_json(record),
                "actual": checksum,
            }
        sequence = record.get("sequence")
        stage = record.get("stage")
        if sequence != filename_sequence or stage != filename_stage:
            return None, {"file": path.name, "sequence": filename_sequence, "type": "filename_record_mismatch"}
        if stage not in STAGE_INDEX or record.get("stage_index") != STAGE_INDEX[stage]:
            return None, {"file": path.name, "sequence": filename_sequence, "type": "invalid_stage"}
        artifact_relative = record.get("artifact_path")
        if not isinstance(artifact_relative, str):
            return None, {"file": path.name, "sequence": filename_sequence, "type": "missing_artifact_path"}
        artifact_path = (self.workspace / artifact_relative).resolve()
        artifact_root = self.artifact_dir.resolve()
        if artifact_path != artifact_root and artifact_root not in artifact_path.parents:
            return None, {"file": path.name, "sequence": filename_sequence, "type": "artifact_path_escape"}
        if not artifact_path.exists():
            return None, {"file": path.name, "sequence": filename_sequence, "type": "artifact_missing", "artifact_path": artifact_relative}
        try:
            artifact_bytes = artifact_path.read_bytes()
            artifact = json.loads(artifact_bytes)
        except (OSError, ValueError, json.JSONDecodeError) as error:
            return None, {"file": path.name, "sequence": filename_sequence, "type": "artifact_unreadable", "error": str(error)}
        artifact_digest = sha256_bytes(artifact_bytes)
        if artifact_digest != record.get("artifact_sha256"):
            return None, {
                "file": path.name,
                "sequence": filename_sequence,
                "type": "artifact_digest_mismatch",
                "expected": record.get("artifact_sha256"),
                "actual": artifact_digest,
            }
        if not isinstance(artifact, dict) or artifact.get("stage") != stage or artifact.get("stage_index") != STAGE_INDEX[stage]:
            return None, {"file": path.name, "sequence": filename_sequence, "type": "artifact_stage_mismatch"}
        upstream = record.get("upstream")
        if not isinstance(upstream, dict) or upstream != artifact.get("upstream"):
            return None, {"file": path.name, "sequence": filename_sequence, "type": "upstream_record_mismatch"}
        return {
            "sequence": sequence,
            "stage": stage,
            "stage_index": STAGE_INDEX[stage],
            "input_fingerprint": record.get("input_fingerprint"),
            "config_version": record.get("config_version"),
            "components": record.get("components", {}),
            "artifact_sha256": record.get("artifact_sha256"),
            "artifact_path": artifact_relative,
            "upstream": upstream,
            "record_sha256": digest_json(record),
            "record": {**record, "record_sha256": checksum},
            "artifact": artifact,
            "path": path,
        }, None

    def scan(self) -> ScanReport:
        report = ScanReport()
        if not self.checkpoint_dir.exists():
            return report
        paths = sorted(self.checkpoint_dir.glob("*.json"))
        for path in paths:
            match = CHECKPOINT_NAME.match(path.name)
            if match:
                report.max_filename_sequence = max(report.max_filename_sequence, int(match.group("sequence")))
            candidate, error = self._candidate_from_path(path)
            if error is not None:
                report.invalid.append(error)
                continue
            assert candidate is not None
            report.groups.setdefault(int(candidate["sequence"]), []).append(candidate)

        for sequence, candidates in report.groups.items():
            if len(candidates) > 1:
                report.duplicate_sequences.append(sequence)

        expected_sequence = 1
        states: dict[str, dict[str, Any]] = {}
        for sequence in sorted(report.groups):
            if sequence != expected_sequence:
                break
            candidates = report.groups[sequence]
            if len(candidates) != 1:
                break
            candidate = candidates[0]
            stage = str(candidate["stage"])
            stage_index = int(candidate["stage_index"])
            if stage_index > 1:
                previous_stage = STAGES[stage_index - 2]
                previous = states.get(previous_stage)
                if previous is None or candidate["upstream"].get(previous_stage) != previous["artifact_sha256"]:
                    break
            if stage_index == 1:
                states = {}
            elif stage_index == 2:
                states.pop("fusion_scoring", None)
                states.pop("aggregation", None)
            elif stage_index == 3:
                states.pop("aggregation", None)
            states[stage] = candidate
            report.chain[sequence] = candidate
            report.state_after_sequence[sequence] = {
                name: {
                    "sequence": item["sequence"],
                    "input_fingerprint": item["input_fingerprint"],
                    "artifact_sha256": item["artifact_sha256"],
                    "config_version": item["config_version"],
                }
                for name, item in sorted(states.items())
            }
            report.latest_sequence = sequence
            expected_sequence += 1
        report.stages = states
        return report

    def _workspace_id(self, marker: dict[str, Any] | None) -> str:
        if marker and isinstance(marker.get("workspace_id"), str):
            return str(marker["workspace_id"])
        return uuid.uuid4().hex

    def _marker_from_scan(self, scan: ScanReport, workspace_id: str) -> dict[str, Any]:
        stages = {
            stage: {
                "sequence": value["sequence"],
                "input_fingerprint": value["input_fingerprint"],
                "artifact_sha256": value["artifact_sha256"],
                "config_version": value["config_version"],
            }
            for stage, value in sorted(scan.stages.items())
        }
        latest_record = scan.chain.get(scan.latest_sequence)
        body = {
            "schema_version": "mmcoop.published.v1",
            "workspace_id": workspace_id,
            "latest_sequence": scan.latest_sequence,
            "stages": stages,
            "chain_tip_record_sha256": latest_record["record_sha256"] if latest_record else None,
        }
        return {**body, "marker_sha256": digest_json(body)}

    def write_marker(self, marker: dict[str, Any]) -> None:
        atomic_write_json(self.marker_path, marker, indent=2)

    def inspect_integrity(self, *, scan: ScanReport | None = None) -> dict[str, Any]:
        scan = scan or self.scan()
        marker, marker_error = self._read_marker()
        warnings = list(scan.invalid)
        if marker_error is not None:
            return {
                "verdict": "tamper",
                "reason": "marker_integrity_failure",
                "marker_error": marker_error,
                "scan": scan.public(),
                "warnings": warnings,
            }
        if marker is None:
            if scan.latest_sequence == 0 and scan.max_filename_sequence == 0:
                return {"verdict": "normal", "reason": "pristine_workspace", "scan": scan.public(), "warnings": warnings}
            if scan.latest_sequence > 0:
                return {
                    "verdict": "normal_advance",
                    "reason": "checkpoint_ahead_of_missing_marker",
                    "scan": scan.public(),
                    "warnings": warnings,
                }
            return {
                "verdict": "tamper",
                "reason": "checkpoint_files_without_valid_chain_or_marker",
                "scan": scan.public(),
                "warnings": warnings,
            }

        marker_latest = int(marker.get("latest_sequence", 0))
        if marker_latest == 0:
            if scan.latest_sequence == 0:
                return {"verdict": "normal", "reason": "empty_workspace", "scan": scan.public(), "warnings": warnings}
            return {"verdict": "normal_advance", "reason": "first_checkpoint_after_marker", "scan": scan.public(), "warnings": warnings}

        marker_candidates = scan.groups.get(marker_latest, [])
        marker_invalid = [item for item in scan.invalid if item.get("sequence") == marker_latest]
        if len(marker_candidates) > 1 or marker_invalid:
            return {
                "verdict": "tamper",
                "reason": "marker_tip_record_duplicate_or_corrupt",
                "marker_latest_sequence": marker_latest,
                "scan": scan.public(),
                "warnings": warnings,
            }
        if not marker_candidates:
            return {
                "verdict": "rollback",
                "reason": "marker_tip_checkpoint_missing",
                "marker_latest_sequence": marker_latest,
                "observed_latest_sequence": scan.max_filename_sequence,
                "scan": scan.public(),
                "warnings": warnings,
            }

        expected_view = scan.state_after_sequence.get(marker_latest)
        if expected_view is None:
            return {
                "verdict": "tamper",
                "reason": "marker_tip_not_on_valid_chain",
                "marker_latest_sequence": marker_latest,
                "scan": scan.public(),
                "warnings": warnings,
            }
        marker_stages = marker.get("stages", {})
        if marker_stages != expected_view:
            return {
                "verdict": "tamper",
                "reason": "stage_fingerprint_or_digest_mismatch",
                "marker_stages": marker_stages,
                "observed_stages": expected_view,
                "scan": scan.public(),
                "warnings": warnings,
            }
        marker_tip = scan.chain.get(marker_latest)
        if marker.get("chain_tip_record_sha256") != (marker_tip["record_sha256"] if marker_tip else None):
            return {
                "verdict": "tamper",
                "reason": "chain_tip_checksum_mismatch",
                "scan": scan.public(),
                "warnings": warnings,
            }
        if scan.latest_sequence < marker_latest:
            return {
                "verdict": "rollback",
                "reason": "sequence_moved_backwards",
                "marker_latest_sequence": marker_latest,
                "observed_latest_sequence": scan.latest_sequence,
                "scan": scan.public(),
                "warnings": warnings,
            }
        if scan.latest_sequence > marker_latest:
            return {
                "verdict": "normal_advance",
                "reason": "valid_checkpoint_chain_ahead_of_marker",
                "marker_latest_sequence": marker_latest,
                "observed_latest_sequence": scan.latest_sequence,
                "scan": scan.public(),
                "warnings": warnings,
            }
        return {"verdict": "normal", "reason": "marker_matches_checkpoint_chain", "scan": scan.public(), "warnings": warnings}

    def _quarantine_sequence_files(self, sequence: int) -> None:
        for path in sorted(self.checkpoint_dir.glob(f"{sequence:08d}-*.json")):
            self.quarantine_dir.mkdir(parents=True, exist_ok=True)
            target = self.quarantine_dir / f"{sequence:08d}-{uuid.uuid4().hex}-{path.name}"
            os.replace(path, target)

    def publish(
        self,
        artifact: dict[str, Any],
        *,
        input_fingerprint: str,
        components: dict[str, Any],
        config_version: str,
        expected_sequence: int | None = None,
    ) -> dict[str, Any]:
        self.ensure()
        stage = str(artifact.get("stage"))
        if stage not in STAGE_INDEX:
            raise business_error("E_INVALID_STAGE", "publish", "unknown stage", diagnostics={"stage": stage}, http_status=409)
        stage_index = STAGE_INDEX[stage]
        artifact_bytes = canonical_bytes(artifact)
        artifact_digest = sha256_bytes(artifact_bytes)
        relative = self.artifact_relative_path(stage, artifact_digest)
        with file_lock(self.publish_lock):
            scan = self.scan()
            integrity = self.inspect_integrity(scan=scan)
            if integrity["verdict"] in {"rollback", "tamper"}:
                raise business_error(
                    "E_STALE_CHECKPOINT",
                    "publish",
                    "workspace snapshot integrity failure blocks publication",
                    sample_id=stage,
                    diagnostics={"integrity": integrity},
                    http_status=409,
                )
            marker, _ = self._read_marker()
            if integrity["verdict"] == "normal_advance":
                self.write_marker(self._marker_from_scan(scan, self._workspace_id(marker)))
                marker, _ = self._read_marker()

            current = scan.stages.get(stage)
            if current is not None and current["input_fingerprint"] == input_fingerprint and current["artifact_sha256"] == artifact_digest:
                upstream_is_newer = False
                if stage_index > 1:
                    previous_for_duplicate = scan.stages.get(STAGES[stage_index - 2])
                    upstream_is_newer = bool(
                        previous_for_duplicate
                        and previous_for_duplicate["sequence"] > current["sequence"]
                    )
                if not upstream_is_newer:
                    raise business_error(
                        "E_STALE_CHECKPOINT",
                        "publish",
                        "duplicate checkpoint publication",
                        sample_id=stage,
                        diagnostics={"stage": stage, "sequence": current["sequence"]},
                        http_status=409,
                    )
            if stage_index > 1:
                previous_stage = STAGES[stage_index - 2]
                previous = scan.stages.get(previous_stage)
                if previous is None:
                    raise business_error(
                        "E_INVALID_STAGE",
                        "publish",
                        "required upstream stage artifact is missing",
                        sample_id=stage,
                        diagnostics={"required_stage": previous_stage, "present_stages": sorted(scan.stages)},
                        http_status=409,
                    )
                if artifact.get("upstream", {}).get(previous_stage) != previous["artifact_sha256"]:
                    raise business_error(
                        "E_STALE_CHECKPOINT",
                        "publish",
                        "publication crosses stage order or uses stale upstream artifact",
                        sample_id=stage,
                        diagnostics={
                            "required_upstream": previous_stage,
                            "artifact_upstream_digest": artifact.get("upstream", {}).get(previous_stage),
                            "current_upstream_digest": previous["artifact_sha256"],
                        },
                        http_status=409,
                    )

            sequence = scan.latest_sequence + 1
            if expected_sequence is not None and expected_sequence != sequence:
                raise business_error(
                    "E_STALE_CHECKPOINT",
                    "publish",
                    "publication sequence is stale or non-contiguous",
                    sample_id=stage,
                    diagnostics={"expected_sequence": expected_sequence, "current_next_sequence": sequence},
                    http_status=409,
                )
            self._quarantine_sequence_files(sequence)
            artifact_path = self.artifact_path(stage, artifact_digest)
            if artifact_path.exists():
                existing = artifact_path.read_bytes()
                if sha256_bytes(existing) != artifact_digest:
                    raise business_error(
                        "E_STALE_CHECKPOINT",
                        "publish",
                        "artifact digest collision or tampering",
                        sample_id=stage,
                        diagnostics={"artifact_path": relative},
                        http_status=409,
                    )
            else:
                atomic_write_bytes(artifact_path, artifact_bytes)
            record_body = {
                "schema_version": "mmcoop.checkpoint.v1",
                "sequence": sequence,
                "stage": stage,
                "stage_index": stage_index,
                "input_fingerprint": input_fingerprint,
                "config_version": config_version,
                "artifact_sha256": artifact_digest,
                "artifact_path": relative,
                "upstream": artifact.get("upstream", {}),
                "components": components,
            }
            record = {**record_body, "record_sha256": digest_json(record_body)}
            checkpoint_path = self.checkpoint_dir / f"{sequence:08d}-{stage}.json"
            atomic_write_json(checkpoint_path, record, indent=2)
            new_scan = self.scan()
            self.write_marker(self._marker_from_scan(new_scan, self._workspace_id(marker)))
            return {
                "sequence": sequence,
                "stage": stage,
                "artifact_sha256": artifact_digest,
                "artifact_path": relative,
                "checkpoint_path": str(checkpoint_path.relative_to(self.workspace)),
                "input_fingerprint": input_fingerprint,
            }

    def current_stages(self) -> dict[str, dict[str, Any]]:
        return self.scan().stages

    def reconcile_marker(self) -> dict[str, Any]:
        self.ensure()
        with file_lock(self.publish_lock):
            scan = self.scan()
            integrity = self.inspect_integrity(scan=scan)
            if integrity["verdict"] == "normal_advance":
                marker, _ = self._read_marker()
                self.write_marker(self._marker_from_scan(scan, self._workspace_id(marker)))
                integrity = self.inspect_integrity(scan=self.scan())
            return integrity

    def read_last_run(self) -> dict[str, Any] | None:
        if not self.last_run_path.exists():
            return None
        try:
            value = read_json(self.last_run_path)
        except (OSError, ValueError, json.JSONDecodeError):
            return None
        return value if isinstance(value, dict) else None

    def write_last_run(self, value: dict[str, Any]) -> None:
        self.ensure()
        atomic_write_json(self.last_run_path, value, indent=2)

    def request_stop(self) -> dict[str, Any]:
        self.ensure()
        value = {"schema_version": "mmcoop.stop.v1", "requested": True}
        atomic_write_json(self.stop_file, value, indent=2)
        return value

    def clear_stop(self) -> None:
        try:
            self.stop_file.unlink()
        except FileNotFoundError:
            pass

    def stop_requested(self) -> bool:
        return self.stop_file.exists()

    def public_checkpoint_listing(self) -> list[dict[str, Any]]:
        scan = self.scan()
        return [
            {
                "sequence": sequence,
                "stage": candidate["stage"],
                "input_fingerprint": candidate["input_fingerprint"],
                "artifact_sha256": candidate["artifact_sha256"],
                "artifact_path": candidate["artifact_path"],
            }
            for sequence, candidate in sorted(scan.chain.items())
        ]

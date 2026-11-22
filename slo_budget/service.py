"""High-level application service used by CLI, HTTP and demo scripts."""

from __future__ import annotations

import dataclasses
import uuid
from pathlib import Path
from typing import Any

from . import codec
from .calculator import calculate
from .compat import compatibility_document
from .errors import SloError
from .events import aggregate_events_to_windows, load_request_events
from .policy import BoundaryPolicy, CalculationPolicy, parse_decimal
from .storage import Storage


class SloService:
    def __init__(
        self,
        db_path: str | Path,
        *,
        clock_ms=None,
        boundaries: BoundaryPolicy | None = None,
        policy: CalculationPolicy | None = None,
    ) -> None:
        self.boundaries = boundaries or BoundaryPolicy()
        self.policy = policy or CalculationPolicy()
        self.boundaries.validate()
        self.policy.validate()
        self.storage = Storage(db_path, clock_ms=clock_ms)

    def close(self) -> None:
        self.storage.close()

    def __enter__(self) -> "SloService":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def compatibility(self) -> dict[str, Any]:
        return {
            "ok": True,
            **compatibility_document(),
            "boundary_policy": self.boundaries.to_dict(),
            "calculation_policy": self.policy.to_dict(),
        }

    def preflight(self, path: str | Path, *, expected_format: str = "auto") -> dict[str, Any]:
        negotiation = codec.preflight(
            path,
            expected_format=expected_format,
            boundaries=self.boundaries,
        )
        return {
            "ok": True,
            "path": str(path),
            "verdict": negotiation.verdict,
            "business_code": negotiation.code,
            "negotiation": negotiation.to_dict(),
            "active_snapshot": self.storage.active_snapshot(),
        }

    def import_file(
        self,
        path: str | Path,
        *,
        expected_format: str = "auto",
        truncation_rule: str = "floor",
        batch_id: str | None = None,
    ) -> dict[str, Any]:
        batch_id = batch_id or uuid.uuid4().hex
        source_path = str(path)
        intent_id = self.storage.begin_write_intent(
            "import",
            batch_id=batch_id,
            payload={
                "source_path": source_path,
                "format": expected_format,
                "truncation_rule": truncation_rule,
            },
        )
        try:
            negotiation = codec.preflight(
                path,
                expected_format=expected_format,
                boundaries=self.boundaries,
            )
            if negotiation.verdict == "REJECT":
                raise SloError(
                    negotiation.code or "E_FORMAT_UNSUPPORTED",
                    "input was rejected by the compatibility matrix; no record imported",
                    path=source_path,
                    details={
                        "batch_id": batch_id,
                        "negotiation": negotiation.to_dict(),
                    },
                )
            parsed = codec.decode(
                path,
                expected_format=expected_format,
                truncation_rule=truncation_rule,
                boundaries=self.boundaries,
            )
        except SloError as error:
            abort = self.storage.abort_write_intent(intent_id, error, source_path=source_path)
            error.details.setdefault("batch_id", batch_id)
            if abort.get("report") is not None:
                error.details.setdefault("report", abort["report"])
            raise

        try:
            report = self.storage.apply_import(intent_id, parsed, source_path=source_path)
        except SloError as error:
            abort = self.storage.abort_write_intent(intent_id, error, source_path=source_path)
            error.details.setdefault("batch_id", batch_id)
            if abort.get("report") is not None:
                error.details.setdefault("report", abort["report"])
            raise

        if report["status"] == "ROLLED_BACK":
            raise SloError(
                "E_IMPORT_CONFLICT",
                "import batch was rolled back because a conflict was not resolvable",
                path=source_path,
                details={
                    "batch_id": batch_id,
                    "report": report,
                    "active_snapshot": self.storage.active_snapshot(),
                },
            )
        return {
            "ok": True,
            "batch_id": batch_id,
            "report": report,
            "active_snapshot": self.storage.active_snapshot(),
            "rounding": {
                "rule": truncation_rule,
                "cumulative_error_us": parsed.cumulative_rounding_error_us,
                "records": [item.to_dict() for item in parsed.rounding],
            },
        }

    def replace_snapshot(
        self,
        path: str | Path,
        *,
        expected_format: str = "auto",
        truncation_rule: str = "floor",
        batch_id: str | None = None,
    ) -> dict[str, Any]:
        batch_id = batch_id or uuid.uuid4().hex
        source_path = str(path)
        intent_id = self.storage.begin_write_intent(
            "snapshot_replace",
            batch_id=batch_id,
            payload={
                "source_path": source_path,
                "format": expected_format,
                "truncation_rule": truncation_rule,
            },
        )
        try:
            negotiation = codec.preflight(
                path,
                expected_format=expected_format,
                boundaries=self.boundaries,
            )
            if negotiation.verdict == "REJECT":
                raise SloError(
                    negotiation.code or "E_FORMAT_UNSUPPORTED",
                    "snapshot rejected by compatibility matrix; active snapshot preserved",
                    path=source_path,
                    details={
                        "batch_id": batch_id,
                        "negotiation": negotiation.to_dict(),
                        "active_snapshot": self.storage.active_snapshot(),
                    },
                )
            parsed = codec.decode(
                path,
                expected_format=expected_format,
                truncation_rule=truncation_rule,
                boundaries=self.boundaries,
            )
        except SloError as error:
            abort = self.storage.abort_write_intent(intent_id, error, source_path=source_path)
            error.details.setdefault("batch_id", batch_id)
            if abort.get("report") is not None:
                error.details.setdefault("report", abort["report"])
            raise

        try:
            report = self.storage.apply_snapshot_replace(
                intent_id,
                parsed,
                source_path=source_path,
            )
        except SloError as error:
            # Storage may already have aborted a failing monotonicity check.
            abort = self.storage.abort_write_intent(intent_id, error, source_path=source_path)
            error.details.setdefault("batch_id", batch_id)
            if abort.get("report") is not None:
                error.details.setdefault("report", abort["report"])
            error.details.setdefault("active_snapshot", self.storage.active_snapshot())
            raise
        return {
            "ok": True,
            "batch_id": batch_id,
            "report": report,
            "active_snapshot": self.storage.active_snapshot(),
            "rounding": {
                "cumulative_error_us": parsed.cumulative_rounding_error_us,
                "records": [item.to_dict() for item in parsed.rounding],
            },
        }

    def export_snapshot(
        self,
        path: str | Path,
        *,
        file_format: str,
        endian: str = "little",
    ) -> dict[str, Any]:
        records, manifest = self.storage.export_material(file_format)
        checksum = codec.export_snapshot(
            path,
            records,
            manifest,
            file_format=file_format,
            endian=endian,
        )
        return {
            "ok": True,
            "path": str(path),
            "format": codec.normalize_format(file_format),
            "endian": endian if codec.normalize_format(file_format) == "binary" else None,
            "sha256": checksum,
            "manifest": manifest.to_dict(),
            "record_count": len(records),
            "active_snapshot": self.storage.active_snapshot(),
        }

    def _query_policy(
        self,
        *,
        slo_target: Any | None = None,
        min_samples: Any | None = None,
    ) -> CalculationPolicy:
        changes: dict[str, Any] = {}
        if slo_target is not None:
            changes["slo_target"] = parse_decimal(slo_target, "slo_target")
        if min_samples is not None:
            try:
                parsed = int(min_samples)
            except (TypeError, ValueError) as exc:
                raise SloError(
                    "E_REQUEST_INVALID",
                    "min_samples must be an integer",
                    details={"min_samples": min_samples},
                ) from exc
            changes["min_samples"] = parsed
        policy = dataclasses.replace(self.policy, **changes) if changes else self.policy
        policy.validate()
        return policy

    def query(
        self,
        *,
        service: str,
        sli_type: str,
        as_of_ms: int,
        events_path: str | Path | None = None,
        slo_target: Any | None = None,
        min_samples: Any | None = None,
        persist_alert: bool = False,
    ) -> dict[str, Any]:
        policy = self._query_policy(slo_target=slo_target, min_samples=min_samples)
        records = self.storage.windows_for(service, sli_type)
        events = None
        events_checksum = None
        if events_path is not None:
            events, events_checksum = load_request_events(events_path, boundaries=self.boundaries)
        result = calculate(
            records,
            service=service,
            sli_type=sli_type,
            as_of_ms=as_of_ms,
            policy=policy,
            boundaries=self.boundaries,
            events=events,
        )
        active = self.storage.active_snapshot()
        result["snapshot"] = active
        result["sources"] = {
            "snapshot_records": len(records),
            "events_path": str(events_path) if events_path is not None else None,
            "events_checksum_sha256": events_checksum,
        }
        if result["business_code"]:
            raise SloError(
                result["business_code"],
                "evaluation did not satisfy the declared sample gate",
                details={
                    "evaluation": result,
                    "deterministic_values_preserved": True,
                    "active_snapshot": active,
                },
            )
        if persist_alert:
            intent_id = self.storage.begin_write_intent(
                "alert",
                payload={
                    "service": service,
                    "sli_type": sli_type,
                    "as_of_ms": as_of_ms,
                    "grade": result["alert"]["grade"],
                    "snapshot_sequence": active["snapshot_sequence"],
                },
            )
            try:
                persisted = self.storage.persist_alert(
                    intent_id,
                    snapshot_sequence=active["snapshot_sequence"],
                    service=service,
                    sli_type=sli_type,
                    as_of_ms=as_of_ms,
                    grade=result["alert"]["grade"],
                    result=result,
                )
            except SloError as error:
                self.storage.abort_write_intent(intent_id, error)
                raise
            result["alert_persistence"] = persisted
        return result

    def analyze_events_file(
        self,
        *,
        events_path: str | Path,
        service: str,
        sli_type: str,
        as_of_ms: int,
        slo_target: Any | None = None,
        min_samples: Any | None = None,
        persist_alert: bool = False,
    ) -> dict[str, Any]:
        policy = self._query_policy(slo_target=slo_target, min_samples=min_samples)
        events, checksum = load_request_events(events_path, boundaries=self.boundaries)
        bucket_ms = min(policy.window_lengths_ms)
        event_records = aggregate_events_to_windows(
            events,
            bucket_ms=bucket_ms,
            generated_at_ms=as_of_ms,
            boundaries=self.boundaries,
        )
        result = calculate(
            event_records,
            service=service,
            sli_type=sli_type,
            as_of_ms=as_of_ms,
            policy=policy,
            boundaries=self.boundaries,
            events=events,
        )
        result["sources"] = {
            "events_path": str(events_path),
            "events_checksum_sha256": checksum,
            "bucket_ms": bucket_ms,
            "snapshot_records": 0,
        }
        result["snapshot"] = self.storage.active_snapshot()
        if result["business_code"]:
            raise SloError(
                result["business_code"],
                "event evaluation did not satisfy the declared sample gate",
                details={
                    "evaluation": result,
                    "deterministic_values_preserved": True,
                },
            )
        if persist_alert:
            intent_id = self.storage.begin_write_intent(
                "alert",
                payload={
                    "source": "events",
                    "service": service,
                    "sli_type": sli_type,
                    "as_of_ms": as_of_ms,
                    "grade": result["alert"]["grade"],
                },
            )
            try:
                persisted = self.storage.persist_alert(
                    intent_id,
                    snapshot_sequence=result["snapshot"]["snapshot_sequence"],
                    service=service,
                    sli_type=sli_type,
                    as_of_ms=as_of_ms,
                    grade=result["alert"]["grade"],
                    result=result,
                )
            except SloError as error:
                self.storage.abort_write_intent(intent_id, error)
                raise
            result["alert_persistence"] = persisted
        return result

    def state(self) -> dict[str, Any]:
        return {"ok": True, **self.storage.state_summary()}

    def windows(self, *, service: str | None = None, sli_type: str | None = None) -> dict[str, Any]:
        records = self.storage.window_records()
        selected = [
            record
            for record in records
            if (service is None or record.service == service)
            and (sli_type is None or record.sli_type == sli_type)
        ]
        return {
            "ok": True,
            "count": len(selected),
            "records": [record.persisted_dict() for record in selected],
            "active_snapshot": self.storage.active_snapshot(),
        }

    def imports(self, *, limit: int = 100) -> dict[str, Any]:
        return {"ok": True, "reports": self.storage.list_import_reports(limit=limit)}

    def alerts(self, *, limit: int = 100) -> dict[str, Any]:
        return {"ok": True, "alerts": self.storage.list_alerts(limit=limit)}

    def recovery(self) -> dict[str, Any]:
        return {
            "ok": True,
            "active_snapshot": self.storage.active_snapshot(),
            "recovered_intents_at_startup": self.storage._recovered_intents,
            "recovery_reports": self.storage.list_recovery_reports(),
            "intents": self.storage.list_intents(),
            "aborted_import_reports": [
                report
                for report in self.storage.list_import_reports(limit=100)
                if report.get("status") == "ABORTED"
            ],
        }

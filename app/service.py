from __future__ import annotations

import hashlib
import json
import re
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pydantic import ValidationError

from app.engine import schedule_batch
from app.errors import BusinessError
from app.models import BatchSpecInput, normalize_spec, spec_to_jsonable
from app.storage import Storage, canonical_json

_SAMPLE_NAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_-]*$")


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


class SchedulerService:
    def __init__(self, storage: Storage, samples_dir: str | Path) -> None:
        self.storage = storage
        self.samples_dir = Path(samples_dir)
        self._lock = threading.RLock()

    def list_samples(self) -> list[dict[str, Any]]:
        samples = []
        for path in sorted(self.samples_dir.glob("*.json")):
            if not _SAMPLE_NAME.fullmatch(path.stem):
                continue
            samples.append({"name": path.stem, "file": path.name})
        return samples

    def load_sample(self, name: str) -> dict[str, Any]:
        if not _SAMPLE_NAME.fullmatch(name):
            raise BusinessError(
                "E_SAMPLE_NOT_FOUND",
                f"sample not found: {name}",
                status_code=404,
                details={"sample": name},
            )
        path = self.samples_dir / f"{name}.json"
        if not path.is_file():
            raise BusinessError(
                "E_SAMPLE_NOT_FOUND",
                f"sample not found: {name}",
                status_code=404,
                details={"sample": name},
            )
        try:
            with path.open("r", encoding="utf-8") as handle:
                return json.load(handle)
        except (OSError, json.JSONDecodeError) as exc:
            raise BusinessError(
                "E_SAMPLE_INVALID",
                f"sample could not be read: {name}",
                status_code=422,
                details={"sample": name, "reason": str(exc)},
            ) from exc

    def submit_sample(self, *, batch_id: str, sample_name: str) -> dict[str, Any]:
        raw = self.load_sample(sample_name)
        try:
            parsed = BatchSpecInput.model_validate(raw)
        except ValidationError as exc:
            raise BusinessError(
                "E_INVALID_SPEC",
                "sample does not conform to the scheduling specification",
                details={"errors": json.loads(exc.json())},
            ) from exc
        return self.submit_parsed(batch_id=batch_id, parsed=parsed, source_name=sample_name)

    def submit_raw_spec(self, *, batch_id: str, raw_spec: dict[str, Any], source_name: str = "inline") -> dict[str, Any]:
        try:
            parsed = BatchSpecInput.model_validate(raw_spec)
        except ValidationError as exc:
            raise BusinessError(
                "E_INVALID_SPEC",
                "request spec does not conform to the scheduling specification",
                details={"errors": json.loads(exc.json())},
            ) from exc
        return self.submit_parsed(batch_id=batch_id, parsed=parsed, source_name=source_name)

    def submit_parsed(self, *, batch_id: str, parsed: BatchSpecInput, source_name: str) -> dict[str, Any]:
        # Full normalization (including every quantity) happens before the intent row is written.
        normalized = normalize_spec(parsed)
        raw_json = canonical_json(spec_to_jsonable(parsed))
        with self._lock:
            timestamp = utc_now()
            self.storage.begin_intent(
                batch_id=batch_id,
                spec_name=source_name,
                spec_json=raw_json,
                timestamp=timestamp,
            )
            try:
                result = schedule_batch(normalized, batch_id=batch_id)
            except Exception as exc:  # The intent is deliberately retained as an aborted persisted fact.
                error = {"code": "E_SCHEDULING_FAILED", "message": str(exc)}
                self.storage.abort(batch_id=batch_id, error=error, timestamp=utc_now())
                if isinstance(exc, BusinessError):
                    raise
                raise BusinessError(
                    "E_SCHEDULING_FAILED",
                    "scheduling failed after the intent was persisted",
                    status_code=500,
                    details={"batchId": batch_id},
                ) from exc
            self.storage.finalize(batch_id=batch_id, result=result, timestamp=utc_now())
        record = self.get_batch(batch_id)
        assert record is not None
        return record

    def get_batch(self, batch_id: str) -> dict[str, Any]:
        record = self.storage.get_batch(batch_id)
        if record is None:
            raise BusinessError(
                "E_BATCH_NOT_FOUND",
                f"batch not found: {batch_id}",
                status_code=404,
                details={"batchId": batch_id},
            )
        result = json.loads(record.pop("result_json")) if record.get("result_json") else None
        error = json.loads(record.pop("error_json")) if record.get("error_json") else None
        record.pop("spec_json", None)
        record["recovered"] = bool(record.get("recovered"))
        record["result"] = result
        record["error"] = error
        return record

    def node_detail(self, *, batch_id: str, node_name: str) -> dict[str, Any]:
        batch = self.get_batch(batch_id)
        result = batch["result"]
        if result is None:
            raise BusinessError(
                "E_BATCH_NOT_COMPLETE",
                "batch has no available scheduling conclusion",
                status_code=409,
                details={"batchId": batch_id, "status": batch["status"]},
            )
        node = next((item for item in result["nodes"] if item["name"] == node_name), None)
        if node is None:
            raise BusinessError(
                "E_NODE_NOT_FOUND",
                f"node not found in batch: {node_name}",
                status_code=404,
                details={"batchId": batch_id, "node": node_name},
            )
        filter_traces = []
        score_traces = []
        rejections = []
        placements = []
        for step in result["steps"]:
            trace = next(item for item in step["filterTrace"] if item["node"] == node_name)
            filter_traces.append({"pod": step["pod"], "decision": step["decision"], **trace})
            if step["decision"] == "placed" and step["selectedNode"] == node_name:
                placements.append(
                    {
                        "pod": step["pod"],
                        "score": step["selectedScore"],
                        "ledger": step.get("placementLedgerEntry"),
                    }
                )
            candidate = next((item for item in step["scoreCandidates"] if item["node"] == node_name), None)
            if candidate is not None:
                score_traces.append({"pod": step["pod"], **candidate})
            if not trace["passed"]:
                failure = trace["firstRejection"]
                rejections.append(
                    {
                        "pod": step["pod"],
                        "code": failure.get("code"),
                        "message": failure.get("message"),
                        "details": failure.get("details", {}),
                    }
                )
        return {
            "batchId": batch_id,
            "node": node,
            "podFilterTraces": filter_traces,
            "scoreTraces": score_traces,
            "rejections": rejections,
            "placements": placements,
        }

    def recover_incomplete(self, *, trigger: str) -> dict[str, Any]:
        with self._lock:
            run_id = self.storage.begin_recovery_run(trigger=trigger, timestamp=utc_now())
            incomplete = self.storage.list_incomplete()
            recovered = 0
            aborted = 0
            for row in incomplete:
                batch_id = row["batch_id"]
                if not self.storage.mark_recovering(batch_id=batch_id, timestamp=utc_now()):
                    continue
                spec_hash = hashlib.sha256(row["spec_json"].encode("utf-8")).hexdigest()
                detected = utc_now()
                self.storage.add_recovery_event(
                    run_id=run_id,
                    batch_id=batch_id,
                    timestamp=detected,
                    action="recovery_started",
                    details={
                        "persistedIntentAt": row["intent_at"],
                        "specName": row["spec_name"],
                        "specSha256": spec_hash,
                        "priorStatus": row["status"],
                    },
                )
                try:
                    raw_spec = json.loads(row["spec_json"])
                    parsed = BatchSpecInput.model_validate(raw_spec)
                    normalized = normalize_spec(parsed)
                    result = schedule_batch(normalized, batch_id=batch_id)
                    self.storage.finalize(batch_id=batch_id, result=result, timestamp=utc_now(), recovered=True)
                    recovered += 1
                    self.storage.add_recovery_event(
                        run_id=run_id,
                        batch_id=batch_id,
                        timestamp=utc_now(),
                        action="recomputed_from_persisted_spec",
                        details={
                            "resultStatus": "completed",
                            "summary": result["summary"],
                            "specSha256": spec_hash,
                        },
                    )
                except Exception as exc:
                    error = {
                        "code": exc.code if isinstance(exc, BusinessError) else "E_RECOVERY_FAILED",
                        "message": str(exc),
                    }
                    self.storage.abort(batch_id=batch_id, error=error, timestamp=utc_now())
                    aborted += 1
                    self.storage.add_recovery_event(
                        run_id=run_id,
                        batch_id=batch_id,
                        timestamp=utc_now(),
                        action="marked_aborted",
                        details={"error": error, "specSha256": spec_hash},
                    )
            self.storage.finish_recovery_run(
                run_id=run_id,
                timestamp=utc_now(),
                scanned_count=len(incomplete),
                recovered_count=recovered,
                aborted_count=aborted,
                notes="Recovery is derived only from persisted intent/spec rows and writes deterministic recomputation results.",
            )
        return self.recovery_report()

    def recovery_report(self) -> dict[str, Any]:
        rows = self.storage.recovery_report_rows()
        events = []
        for event in rows["events"]:
            event = dict(event)
            event["details"] = json.loads(event.pop("details_json"))
            events.append(event)
        current_incomplete = [
            {
                "batchId": row["batch_id"],
                "status": row["status"],
                "specName": row["spec_name"],
                "intentAt": row["intent_at"],
                "updatedAt": row["updated_at"],
            }
            for row in self.storage.list_incomplete()
        ]
        return {
            "generatedAt": utc_now(),
            "scope": "persisted-facts-only",
            "runs": rows["runs"],
            "events": events,
            "currentIncomplete": current_incomplete,
            "limitations": [
                "Forced termination can interrupt computation before the conclusion row is written.",
                "This report is generated after restart and does not claim that a graceful shutdown report was produced.",
                "Only SQLite intent, specification, event, and conclusion rows are treated as authoritative facts.",
            ],
        }

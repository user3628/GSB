from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from .errors import BusinessError
from .service import WorkbenchService


def _load(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def seed_workbench(service: WorkbenchService, samples_dir: str | Path) -> dict[str, Any]:
    directory = Path(samples_dir)
    actions: list[dict[str, Any]] = []
    for filename in ("policy-v1.json", "policy-v2.json"):
        path = directory / filename
        if not path.exists():
            continue
        policy = _load(path)
        try:
            response = service.submit_policy(policy)
            actions.append(
                {
                    "action": "SUBMIT_POLICY",
                    "policy_version": response["version"],
                    "result": "CREATED",
                    "digest": response["digest"],
                }
            )
        except BusinessError as error:
            if error.code != "E_STALE_POLICY":
                raise
            actions.append(
                {
                    "action": "SUBMIT_POLICY",
                    "policy_version": policy["version"],
                    "result": "SKIPPED_STALE",
                    "code": error.code,
                }
            )

    batches = _load(directory / "batches.json")
    for batch in batches:
        try:
            response = service.submit_batch(batch)
            actions.append(
                {
                    "action": "SUBMIT_BATCH",
                    "batch_id": batch["batch_id"],
                    "result": "CREATED",
                    "status": response["batch"]["status"],
                }
            )
        except BusinessError as error:
            if error.code != "E_BATCH_EXISTS":
                raise
            actions.append(
                {
                    "action": "SUBMIT_BATCH",
                    "batch_id": batch["batch_id"],
                    "result": "SKIPPED_EXISTS",
                    "code": error.code,
                }
            )

    observation_bundle = _load(directory / "observations.json")
    observations = observation_bundle.get("seed_observations", {})
    watermarks = observation_bundle.get("seed_watermarks", {})
    for batch in batches:
        batch_id = batch["batch_id"]
        payload = observations.get(batch_id, [])
        try:
            ingest = service.ingest_observations(
                batch_id, {"observations": payload}
            )
            actions.append(
                {
                    "action": "INGEST_OBSERVATIONS",
                    "batch_id": batch_id,
                    "result": "COMMITTED",
                    "accepted_count": ingest["accepted_count"],
                    "duplicate_count": ingest["duplicate_count"],
                }
            )
        except BusinessError as error:
            actions.append(
                {
                    "action": "INGEST_OBSERVATIONS",
                    "batch_id": batch_id,
                    "result": "SKIPPED",
                    "code": error.code,
                    "message": error.message,
                }
            )
        watermark = watermarks.get(batch_id)
        if watermark is not None:
            close = service.ingest_observations(
                batch_id, {"observations": [], "watermark_ms": watermark}
            )
            actions.append(
                {
                    "action": "CLOSE_WINDOWS",
                    "batch_id": batch_id,
                    "result": "COMMITTED",
                    "watermark_ms": watermark,
                    "window_epoch": close["window_epoch"],
                }
            )

    return {
        "samples_dir": str(directory),
        "actions": actions,
        "batch_count": len(batches),
        "policy_count": len(list(directory.glob("policy-v*.json"))),
    }

from __future__ import annotations

import json
from pathlib import Path

from app.models import PolicySnapshotIn
from app.service import GatewayService


def seed_if_empty(service: GatewayService, samples_dir: Path) -> dict[str, object]:
    if service.store.current_version() > 0:
        return {"seeded": False, "reason": "policy already present"}
    policy_path = samples_dir / "policy_snapshot.json"
    payload = PolicySnapshotIn.model_validate(json.loads(policy_path.read_text(encoding="utf-8")))
    result = service.submit_policy(payload)
    return {
        "seeded": True,
        "policy_version": result["policy"]["version"],
        "checksum": result["policy"]["checksum"],
        "intent_id": result["intent_id"],
    }

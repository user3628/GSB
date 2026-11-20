from __future__ import annotations

import json
import sys
from pathlib import Path
from tempfile import TemporaryDirectory

from fastapi.testclient import TestClient

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from replication_guard.api import create_app


def main() -> None:
    with TemporaryDirectory() as temp_dir:
        app = create_app(Path(temp_dir) / "smoke.sqlite3")
        with TestClient(app) as client:
            policy_text = (ROOT / "samples" / "policies.jsonl").read_text("utf-8")
            imported = client.post(
                "/api/policies/import?format=jsonl&batch_id=smoke-import",
                content=policy_text,
                headers={"Content-Type": "application/x-ndjson"},
            )
            print("import", imported.status_code, imported.json()["status"], imported.json()["datasetVersion"])

            observations = json.loads((ROOT / "samples" / "observations.json").read_text("utf-8"))
            ingested = client.post("/api/observations", json=observations)
            print("observations", ingested.status_code, ingested.json()["observationCount"])

            recalculated = client.post(
                "/api/recalculate",
                json={
                    "batchId": "smoke-recalc",
                    "observationBatchId": observations["batchId"],
                    "windowSize": 2,
                },
            )
            print("recalculate", recalculated.status_code, recalculated.json()["counts"])
            recalc_id = recalculated.json()["recalcId"]
            replayed = client.post(f"/api/verdicts/{recalc_id}/replay")
            print("replay-deterministic", replayed.json()["deterministicMatch"])


if __name__ == "__main__":
    main()


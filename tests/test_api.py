from __future__ import annotations

import json
from pathlib import Path

from fastapi.testclient import TestClient

from stream_tts.service import create_app


def test_http_run_query_stop_resume_and_settings(workspace: Path):
    app = create_app(workspace=workspace, config_path=workspace / "config/default.yaml")
    client = TestClient(app)

    health = client.get("/health")
    assert health.status_code == 200
    assert health.json()["ok"] is True
    assert health.json()["calibration_unit_ms"] > 0

    version = client.get("/version")
    assert version.status_code == 200
    assert len(version.json()["utterances"]) == 3

    # A pre-start stop request must produce a graceful stop; resume then completes.
    stopped = client.post("/runs/http/stop")
    assert stopped.status_code == 200
    run = client.post("/runs", json={"run_id": "http", "wait": True})
    assert run.status_code == 200
    assert run.json()["status"] == "stopped"

    resumed = client.post("/runs/http/resume", json={"run_id": "http", "wait": True})
    assert resumed.status_code == 200
    assert resumed.json()["status"] == "complete"
    aggregate = resumed.json()["aggregate_sha256"]
    assert aggregate

    status = client.get("/runs/http")
    assert status.status_code == 200
    assert status.json()["status"] == "complete"
    assert status.json()["process_active"] is False

    stages = client.get("/runs/http/stages")
    assert stages.status_code == 200
    stage_payload = stages.json()
    assert stage_payload["next_seq"] >= 12
    assert set(stage_payload["stages"]) == {
        "utt_mix_01",
        "utt_mix_02",
        "utt_budget_stress_03",
    }
    assert all(
        stage["complete"]
        for utt in stage_payload["stages"].values()
        for stage in utt.values()
    )

    summary = client.get("/runs/http/summary")
    assert summary.status_code == 200
    assert summary.json()["aggregate_sha256"] == aggregate
    assert summary.json()["degraded_utterances"] == ["utt_budget_stress_03"]

    trace = client.get("/runs/http/trace")
    assert trace.status_code == 200
    records = [json.loads(line) for line in trace.text.splitlines() if line.strip()]
    assert len(records) == summary.json()["trace_records"]
    assert any(item["degraded"] for item in records)

    timing = client.get("/runs/http/timing")
    assert timing.status_code == 200
    assert all("elapsed_ms" in json.loads(line) for line in timing.text.splitlines() if line.strip())

    degrade = client.get("/runs/http/degrade")
    assert degrade.status_code == 200
    assert degrade.json()["degraded_block_count"] > 0
    assert all(item["degrade_reason"] for item in degrade.json()["events"])

    exports = client.get("/runs/http/exports")
    assert exports.status_code == 200
    wavs = exports.json()["wavs"]
    assert len(wavs) == 3
    wav = client.get("/runs/http/exports/utt_mix_01.wav")
    assert wav.status_code == 200
    assert wav.headers["content-type"].startswith("audio/wav")
    assert wav.content.startswith(b"RIFF")

    split = client.put(
        "/settings/split",
        json={"first_chunk_min_chars": 10, "subsequent_chunk_chars": 16, "punctuation_forced": True},
    )
    assert split.status_code == 200
    assert split.json()["split"]["first_chunk_min_chars"] == 10
    budget = client.put(
        "/settings/budget",
        json={"first_packet_ms": 40.0, "continuation_ms": 28.0, "max_workers": 3},
    )
    assert budget.status_code == 200
    assert budget.json()["budget"]["first_packet_ms"] == 40.0

    imported = client.post(
        "/samples/import",
        json={
            "jsonl": json.dumps(
                {
                    "utt_id": "imported",
                    "text": "你好，English。",
                    "language": "zh",
                    "speaker": 2,
                    "prosody": {"rate": 1.0},
                },
                ensure_ascii=False,
            ),
            "filename": "imported.jsonl",
        },
    )
    assert imported.status_code == 200
    assert imported.json()["utterance_count"] == 1

    invalid_split = client.put("/settings/split", json={"first_chunk_min_chars": -1})
    assert invalid_split.status_code == 200
    invalid_run = client.post("/runs", json={"run_id": "bad-split", "wait": True})
    assert invalid_run.status_code == 400
    error = invalid_run.json()["error"]
    assert error["code"] == "E_INVALID_TEXT"
    assert error["context"]["utt_id"] == "utt_mix_01"
    assert error["context"]["chunk_index"] == 0
    assert error["context"]["char_position"] == 0

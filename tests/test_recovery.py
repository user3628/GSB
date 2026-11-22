from __future__ import annotations

import asyncio
import json
import sqlite3
from pathlib import Path

import pytest

from dns_workbench.models import QuerySpec, now_ms
from dns_workbench.storage import Storage, cache_entry_from_outcome


@pytest.mark.asyncio
async def test_startup_recovery_rolls_back_pending_intent_and_aborts_session(tmp_path: Path) -> None:
    db_path = tmp_path / "recovery.sqlite3"
    storage = Storage(db_path)
    startup_report = await storage.initialize()
    assert startup_report["first_startup"] is True
    assert startup_report["forced_interruption_detected"] is False
    spec = QuerySpec(name="a.lab.test.", qtype=1, query_id="interrupted-query")
    await storage.start_round("round-interrupted", 500, now_ms())
    await storage.start_session(spec, "round-interrupted")
    await storage.mark_session_resolving(spec.query_id, "round-interrupted")
    intent_id = await storage.write_intent(
        query_id=spec.query_id,
        cache_key=spec.cache_key,
        kind="positive",
        payload={"simulated": "crash after intent, before cache commit"},
    )
    # Simulate an uncommitted cache row being present despite a still-pending intent.
    with sqlite3.connect(db_path) as connection:
        connection.execute(
            "INSERT INTO cache_entries(cache_key, qname, qtype, qclass, kind, negative_kind, rcode, "
            "source_type, written_at_ms, original_ttl_ms, expires_at_ms, records_json, authorities_json, "
            "query_id, updated_at_ms) VALUES(?,?,?,?,?,NULL,0,1,?,?,?,?,?,?,?)",
            (
                spec.cache_key,
                spec.name,
                1,
                1,
                "positive",
                now_ms(),
                60000,
                now_ms() + 60000,
                json.dumps([]),
                json.dumps([]),
                spec.query_id,
                now_ms(),
            ),
        )
        connection.commit()
    await storage.close()

    reopened = Storage(db_path)
    report = await reopened.initialize()
    assert report["forced_interruption_detected"] is True
    assert report["pending_intents_found"] == 1
    assert report["rolled_back_intents"][0]["intent_id"] == intent_id
    assert report["rolled_back_cache_keys"] == [spec.cache_key]
    assert [item["query_id"] for item in report["aborted_sessions"]] == [spec.query_id]
    assert [item["round_id"] for item in report["interrupted_rounds"]] == ["round-interrupted"]
    assert await reopened.get_cache(spec.cache_key) is None
    session = await reopened.get_session(spec.query_id)
    assert session is not None and session["state"] == "aborted"
    assert session["business_code"] == "E_PROCESS_INTERRUPTED"
    with sqlite3.connect(db_path) as connection:
        status = connection.execute(
            "SELECT status FROM intent_log WHERE intent_id=?", (intent_id,)
        ).fetchone()[0]
    assert status == "rolled_back"
    await reopened.close()


@pytest.mark.asyncio
async def test_completed_cache_and_session_survive_unclean_reopen(tmp_path: Path) -> None:
    db_path = tmp_path / "durable.sqlite3"
    storage = Storage(db_path)
    await storage.initialize()
    spec = QuerySpec(name="mx.lab.test.", qtype=15, query_id="durable-query")
    await storage.start_round("durable-round", 500, now_ms())
    await storage.start_session(spec, "durable-round")
    entry = cache_entry_from_outcome(
        spec=spec,
        written_at_ms=now_ms(),
        ttl_ms=120000,
        rcode=0,
        source_type=15,
        kind="positive",
        negative_kind=None,
        answers=[
            {
                "name": "mx.lab.test.",
                "type": 15,
                "qclass": 1,
                "ttl_ms": 120000,
                "rdata": {"preference": 10, "target": "mail.lab.test."},
            }
        ],
        authorities=[],
    )
    response = {
        "rcode": 0,
        "source_record_type": 15,
        "answers": entry.records,
        "authorities": [],
        "ttl_base_ms": entry.written_at_ms,
    }
    intent_id = await storage.write_intent(
        query_id=spec.query_id, cache_key=spec.cache_key, kind="positive", payload={}
    )
    await storage.commit_cache_and_session(
        intent_id=intent_id,
        entry=entry,
        query_id=spec.query_id,
        response=response,
        rcode=0,
        source_type=15,
    )
    await storage.finish_round("durable-round", {"dispatch": []})
    # Deliberately do not write clean_shutdown.
    await storage.close()

    reopened = Storage(db_path)
    report = await reopened.initialize()
    assert report["forced_interruption_detected"] is True
    assert report["pending_intents_found"] == 0
    assert report["aborted_sessions"] == []
    cached = await reopened.get_cache(spec.cache_key)
    assert cached is not None
    assert cached.records[0]["rdata"]["target"] == "mail.lab.test."
    session = await reopened.get_session(spec.query_id)
    assert session is not None and session["state"] == "complete"
    round_data = await reopened.get_round("durable-round")
    assert round_data is not None and round_data["status"] == "complete"
    await reopened.close()


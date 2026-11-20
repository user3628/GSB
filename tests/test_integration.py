from __future__ import annotations

import json
import sqlite3
import threading
import time
from pathlib import Path

import httpx
import pytest

ROOT = Path(__file__).resolve().parents[1]
SAMPLES = ROOT / "samples"


def policy_payload(name: str, *, version: int | None = None, **overrides) -> dict:
    payload = json.loads((SAMPLES / name).read_text(encoding="utf-8"))
    payload.pop("digest", None)
    if version is not None:
        payload["version"] = version
    payload.update(overrides)
    return payload


def test_cache_key_hit_vary_and_invalid_key(server) -> None:
    server.submit_policy(policy_payload("policy_v1.json"))
    first = server.request(
        "edge-a",
        {
            "request_id": "cold-canonical",
            "path": "//assets/./app.js",
            "query": "v=1&lang=zh-CN",
            "headers": {"Accept-Language": "zh-CN"},
        },
    )
    assert first.status_code == 200
    first_data = first.json()
    assert first_data["cache_status"] == "MISS"
    assert first_data["origin_fetches"] == 1

    second = server.request(
        "edge-a",
        {
            "request_id": "hit-canonical",
            "path": "/assets/app.js",
            "query": "lang=zh-CN&v=1",
            "headers": {"accept-language": "zh-CN"},
        },
    ).json()
    assert second["cache_status"] == "FRESH"
    assert second["origin_fetches"] == 0
    assert second["cache_key"] == first_data["cache_key"]
    assert int(second["headers"]["Age"]) >= 0
    assert "max-age" in second["headers"]["Cache-Control"]

    vary = server.request(
        "edge-a",
        {
            "request_id": "vary-en",
            "path": "/assets/app.js",
            "query": "v=1&lang=zh-CN",
            "headers": {"Accept-Language": "en-US"},
        },
    ).json()
    assert vary["cache_status"] == "MISS"
    assert vary["origin_fetches"] == 1
    assert vary["cache_key"] != first_data["cache_key"]

    before = server.client().get("/api/edges/edge-a/entries").json()["entries"]
    invalid = server.request("edge-a", {"request_id": "invalid-key", "path": "/assets/%ZZ"})
    assert invalid.status_code == 400
    assert invalid.json()["business_code"] == "E_KEY_INVALID"
    after = server.client().get("/api/edges/edge-a/entries").json()["entries"]
    assert len(after) == len(before) == 2

    raw = server.client().get(
        "/edge/edge-a//assets/./app.js?v=1&lang=zh-CN",
        headers={"Accept-Language": "zh-CN", "X-Request-ID": "raw-get"},
    )
    assert raw.status_code == 200
    assert raw.headers["x-cache-status"] in {"FRESH", "MISS"}

    with server.client() as client:
        reserved_id = client.get(
            "/edge/edge-a/assets/theme.css?__sim_request_id=reserved-request-id",
            headers={"Accept-Language": "zh-CN"},
        )
        assert reserved_id.status_code == 200
        record = client.get("/api/requests/reserved-request-id")
        assert record.status_code == 200
        events = client.get(f"/api/edges/edge-a/entries/{reserved_id.headers['x-cache-key']}/events")
        assert events.status_code == 200
        assert any(event["to_state"] == "FRESH" for event in events.json()["events"])
        missing = client.get("/api/this-route-does-not-exist")
        assert missing.status_code == 404
        assert missing.json()["business_code"] == "E_NOT_FOUND"


def test_concurrent_batch_only_one_origin_fetch_per_cache_key(server) -> None:
    server.submit_policy(policy_payload("policy_v1.json"))
    requests = [
        {
            "request_id": f"coalesce-{index}",
            "path": "/api/catalog.json",
            "injections": [{"type": "origin_delay", "delay_ms": 450, "timeout_ms": 2500}],
        }
        for index in range(8)
    ]
    response = server.client().post("/api/edges/edge-c/requests/batch", json={"requests": requests})
    assert response.status_code == 200
    results = response.json()["results"]
    assert len(results) == 8
    assert all(item["ok"] for item in results)
    assert sum(item["origin_fetches"] for item in results) == 1
    assert sum(1 for item in results if item["cache_status"] == "MISS") == 1
    assert {item["etag"] for item in results} == {results[0]["etag"]}
    with server.sqlite() as connection:
        attempts = connection.execute(
            "SELECT COUNT(*) FROM origin_fetches WHERE request_id LIKE 'coalesce-%'"
        ).fetchone()[0]
    assert attempts == 1


def test_origin_version_change_replaces_whole_entry_and_body_length_changes_version(server) -> None:
    server.submit_policy(policy_payload("policy_v1.json"))
    old = server.request(
        "edge-a",
        {"request_id": "replace-old", "path": "/assets/theme.css", "headers": {"Accept-Language": "zh-CN"}},
    ).json()
    assert old["cache_status"] == "MISS"

    with server.client() as client:
        update = client.post(
            "/api/origin/content",
            json={
                "path": "/assets/theme.css",
                "body": ':root { --demo-version: "theme-v2-replaced"; }\n',
                "content_type": "text/css",
                "tags": ["static", "theme", "css", "release-v2"],
            },
        )
    assert update.status_code == 200
    new_etag = update.json()["item"]["etag"]
    assert new_etag != old["etag"]

    replaced = server.request(
        "edge-a",
        {
            "request_id": "replace-new",
            "path": "/assets/theme.css",
            "headers": {"Accept-Language": "zh-CN"},
            "force_revalidate": True,
        },
    ).json()
    assert replaced["cache_status"] == "REVALIDATED"
    assert replaced["etag"] == new_etag
    assert replaced["generation"] == 2
    commit = next(event for event in replaced["trace"]["events"] if event["phase"] == "ENTRY_COMMIT")
    assert commit["version_changed"] is True
    assert commit["old_etag"] == old["etag"]
    assert commit["new_etag"] == new_etag

    padded = server.request(
        "edge-a",
        {
            "request_id": "replace-padded",
            "path": "/assets/theme.css",
            "headers": {"Accept-Language": "zh-CN"},
            "force_revalidate": True,
            "injections": [{"type": "body_length", "pad_bytes": 64}],
        },
    ).json()
    assert padded["body_length"] == len(replaced["body"].encode("utf-8")) + 64
    assert padded["etag"] != replaced["etag"]
    trace_commit = next(event for event in padded["trace"]["events"] if event["phase"] == "ENTRY_COMMIT")
    assert trace_commit["version_changed"] is True

    entries = server.client().get("/api/edges/edge-a/entries?include_purged=false").json()["entries"]
    theme_entries = [entry for entry in entries if entry["canonical_path"] == "/assets/theme.css"]
    assert len(theme_entries) == 1
    assert theme_entries[0]["etag"] == padded["etag"]
    assert "release-v2" not in theme_entries[0]["tags"] or theme_entries[0]["generation"] == 3


def test_stale_while_revalidate_is_decoupled_and_expired_is_never_fresh(server) -> None:
    policy = policy_payload(
        "policy_v2.json",
        version=10,
        ttl_seconds=0.25,
        stale_while_revalidate_seconds=2.0,
        cache_control="public, max-age=0.25, stale-while-revalidate=2",
    )
    server.submit_policy(policy)
    cold = server.request(
        "edge-b",
        {"request_id": "swr-cold", "path": "/assets/app.js", "headers": {"Accept-Language": "zh", "X-Tenant": "t1"}},
    ).json()
    assert cold["cache_status"] == "MISS"
    time.sleep(0.38)

    started = time.perf_counter()
    stale_response = server.request(
        "edge-b",
        {
            "request_id": "swr-stale",
            "path": "/assets/app.js",
            "headers": {"Accept-Language": "zh", "X-Tenant": "t1"},
            "injections": [{"type": "origin_delay", "delay_ms": 700, "timeout_ms": 2500}],
        },
    )
    elapsed = time.perf_counter() - started
    stale = stale_response.json()
    assert stale_response.status_code == 200
    assert stale["cache_status"] == "STALE"
    assert stale["headers"]["X-Stale"] == "true"
    assert stale["headers"]["Warning"].startswith("110")
    assert elapsed < 0.6, f"stale request waited for background origin fetch: {elapsed}"
    assert stale["body"] == cold["body"]
    task = stale["background_revalidation"]
    assert task["decoupled"] is True

    with server.client(timeout=5) as client:
        background = client.get(f"/api/background/{task['task_id']}?wait=true&timeout=4")
    assert background.status_code == 200
    assert background.json()["background"]["status"] == "COMPLETED"
    after = server.request(
        "edge-b",
        {"request_id": "swr-after", "path": "/assets/app.js", "headers": {"Accept-Language": "zh", "X-Tenant": "t1"}},
    ).json()
    assert after["cache_status"] == "FRESH"
    assert after["origin_fetches"] == 0
    assert after["generation"] == 2


def test_purge_failure_rolls_back_then_success_is_atomic_across_nodes(server) -> None:
    server.submit_policy(policy_payload("policy_v1.json"))
    for node in ("edge-a", "edge-b", "edge-c"):
        response = server.request(
            node,
            {
                "request_id": f"purge-cold-{node}",
                "path": "//assets/./app.js",
                "query": "v=1",
                "headers": {"Accept-Language": "zh-CN"},
            },
        ).json()
        assert response["cache_status"] == "MISS"

    with server.client() as client:
        failed = client.post(
            "/api/purges",
            json={
                "request_id": "purge-injected-fail",
                "tag": "app",
                "injections": [{"type": "purge_broadcast_interrupt", "target_node": "edge-a"}],
            },
        )
    assert failed.status_code == 503
    assert failed.json()["business_code"] == "E_PURGE_BROADCAST_FAIL"
    for node in ("edge-a", "edge-b", "edge-c"):
        with server.client() as client:
            entries = client.get(f"/api/edges/{node}/entries?include_purged=false").json()["entries"]
        assert len(entries) == 1
        assert entries[0]["state"] in {"FRESH", "STALE"}

    with server.client() as client:
        success = client.post("/api/purges", json={"request_id": "purge-ok", "tag": "app"})
    assert success.status_code == 200
    purge = success.json()["purge"]
    assert purge["atomic"] is True
    assert set(purge["affected"]) == {"edge-a", "edge-b", "edge-c"}
    assert all(len(keys) == 1 for keys in purge["affected"].values())
    for node in ("edge-a", "edge-b", "edge-c"):
        with server.client() as client:
            item = client.get(f"/api/edges/{node}/entries").json()["entries"][0]
            state = client.get("/api/diagnostics/state").json()["state"]
        assert item["state"] == "PURGED"
        assert state["nodes"][node]["route_index_count"] == 0
        assert state["nodes"][node]["tag_index_count"] == 0


def test_injection_error_codes_and_records(server) -> None:
    server.submit_policy(policy_payload("policy_v1.json"))
    common = {"path": "/api/catalog.json"}
    invalid = server.request(
        "edge-a", {**common, "request_id": "inject-invalid", "injections": [{"type": "unknown"}]}
    )
    assert invalid.status_code == 400
    assert invalid.json()["business_code"] == "E_INJECT_INVALID"
    assert invalid.json()["origin_fetches"] == 0

    five_xx = server.request(
        "edge-a",
        {**common, "request_id": "inject-5xx", "injections": [{"type": "origin_status", "status": 503}]},
    )
    assert five_xx.status_code == 502
    assert five_xx.json()["business_code"] == "E_ORIGIN_5XX"
    assert five_xx.json()["origin_fetches"] == 1

    refused = server.request(
        "edge-a", {**common, "request_id": "inject-refused", "injections": [{"type": "connection_refused"}]}
    )
    assert refused.status_code == 502
    assert refused.json()["business_code"] == "E_ORIGIN_CONNECT"
    assert refused.json()["origin_fetches"] == 1

    started = time.perf_counter()
    timeout = server.request(
        "edge-a",
        {
            **common,
            "request_id": "inject-timeout",
            "injections": [{"type": "origin_delay", "delay_ms": 500, "timeout_ms": 80}],
        },
    )
    elapsed = time.perf_counter() - started
    assert timeout.status_code == 504
    assert timeout.json()["business_code"] == "E_ORIGIN_TIMEOUT"
    assert timeout.json()["origin_fetches"] == 1
    assert 0.07 <= elapsed < 0.8

    with server.client() as client:
        records = client.get("/api/injections?node_id=edge-a").json()["injections"]
    types = {record["type"] for record in records}
    assert {"unknown", "origin_status", "connection_refused", "origin_delay"} <= types
    assert any(record["status"] == "REJECTED" and record["business_code"] == "E_INJECT_INVALID" for record in records)


def test_snapshot_version_and_invalid_digest_are_rejected_without_mixing(server) -> None:
    server.submit_policy(policy_payload("policy_v1.json"))
    cold = server.request(
        "edge-a",
        {"request_id": "policy-cold", "path": "/assets/app.js", "headers": {"Accept-Language": "zh"}},
    )
    assert cold.status_code == 200

    stale = server.submit_policy(policy_payload("policy_v1.json"), expected_status=409)
    assert stale["business_code"] == "E_STALE_SNAPSHOT"
    invalid = policy_payload("policy_v2.json")
    invalid["digest"] = "0" * 64
    bad = server.submit_policy(invalid, expected_status=422)
    assert bad["business_code"] == "E_SNAPSHOT_INVALID"
    with server.client() as client:
        assert client.get("/api/policies/current").json()["policy"]["version"] == 1
        old_entries = client.get("/api/edges/edge-a/entries").json()["entries"]
    assert any(entry["state"] == "FRESH" for entry in old_entries if entry["canonical_path"] == "/assets/app.js")

    committed = server.submit_policy(policy_payload("policy_v2.json"))
    assert committed["commit"]["invalidated_entries"] >= 1
    with server.client() as client:
        current = client.get("/api/policies/current").json()["policy"]
        entries = client.get("/api/edges/edge-a/entries").json()["entries"]
    assert current["version"] == 2
    assert current["vary"] == ["accept-language", "x-tenant"]
    assert all(entry["state"] == "PURGED" for entry in entries if entry["canonical_path"] == "/assets/app.js")


def test_request_id_dedupe_and_transition_error(server) -> None:
    server.submit_policy(policy_payload("policy_v1.json"))
    payload = {"request_id": "dedupe-once", "path": "/assets/app.js", "headers": {"Accept-Language": "zh"}}
    first = server.request("edge-a", payload).json()
    second = server.request("edge-a", payload).json()
    assert first["origin_fetches"] == 1
    assert second["replayed"] is True
    assert second["origin_fetches"] == 0
    assert second["original_origin_fetches"] == 1
    conflict = server.request("edge-a", {**payload, "path": "/api/catalog.json"})
    assert conflict.status_code == 409
    assert conflict.json()["business_code"] == "E_REQUEST_CONFLICT"

    concurrent_payload = {
        "request_id": "dedupe-concurrent",
        "path": "/assets/theme.css",
        "headers": {"Accept-Language": "zh"},
        "injections": [{"type": "origin_delay", "delay_ms": 350, "timeout_ms": 2000}],
    }
    with server.client() as client:
        batch = client.post(
            "/api/edges/edge-a/requests/batch",
            json={"requests": [dict(concurrent_payload) for _ in range(6)]},
        )
    assert batch.status_code == 200
    batch_results = batch.json()["results"]
    assert sum(item["origin_fetches"] for item in batch_results) == 1
    assert sum(1 for item in batch_results if item.get("replayed")) == 5
    with server.client() as client:
        stored = client.get("/api/requests/dedupe-concurrent").json()["request"]
    assert [trace["decision"] for trace in stored["traces"]].count("DEDUPLICATED_IN_FLIGHT") == 5

    with server.client() as client:
        replay = client.post(
            "/api/replay",
            json={
                "requests": [
                    {
                        "node_id": "edge-a",
                        "request_id": "generic-replay-one",
                        "path": "/assets/app.js",
                        "headers": {"Accept-Language": "zh"},
                    }
                ]
            },
        )
    assert replay.status_code == 200
    assert replay.json()["results"][0]["ok"] is True

    cache_key = first["cache_key"]
    with server.client() as client:
        invalid = client.post(
            f"/api/edges/edge-a/entries/{cache_key}/transition",
            json={"from_state": "FRESH", "to_state": "MISS"},
        )
        assert invalid.status_code == 409
        assert invalid.json()["business_code"] == "E_INVALID_TRANSITION"
        unchanged = client.get(f"/api/edges/edge-a/entries/{cache_key}").json()["entry"]
    assert unchanged["state"] == "FRESH"


def test_force_termination_recovery_uses_persisted_facts_only(server) -> None:
    server.submit_policy(policy_payload("policy_v1.json"))
    stable = server.request(
        "edge-a",
        {"request_id": "recovery-stable", "path": "/assets/theme.css", "headers": {"Accept-Language": "zh"}},
    ).json()
    assert stable["cache_status"] == "MISS"

    result_holder: dict[str, object] = {}

    def in_flight_request() -> None:
        try:
            with httpx.Client(base_url=server.base_url, timeout=10, trust_env=False) as client:
                result_holder["response"] = client.post(
                    "/api/edges/edge-c/requests",
                    json={
                        "request_id": "recovery-in-flight",
                        "path": "/api/catalog.json",
                        "injections": [{"type": "origin_delay", "delay_ms": 30000, "timeout_ms": 60000}],
                    },
                )
        except BaseException as exc:  # Expected because the process is killed.
            result_holder["error"] = type(exc).__name__

    thread = threading.Thread(target=in_flight_request, daemon=True)
    thread.start()
    deadline = time.time() + 8
    saw_intent = False
    while time.time() < deadline:
        try:
            with server.sqlite() as connection:
                row = connection.execute(
                    "SELECT status FROM intents WHERE request_id = 'recovery-in-flight' ORDER BY created_at DESC LIMIT 1"
                ).fetchone()
            if row and row["status"] == "IN_PROGRESS":
                saw_intent = True
                break
        except sqlite3.Error:
            pass
        time.sleep(0.05)
    assert saw_intent, "in-flight origin intent was not persisted before termination"
    server.stop(force=True)
    thread.join(timeout=5)
    assert not thread.is_alive()

    server.restart()
    with server.client() as client:
        report = client.get("/api/recovery/report").json()["report"]
        stable_after = client.get("/api/edges/edge-a/entries?include_purged=false").json()["entries"]
        duplicate = client.post(
            "/api/edges/edge-c/requests",
            json={
                "request_id": "recovery-in-flight",
                "path": "/api/catalog.json",
                "injections": [{"type": "origin_delay", "delay_ms": 30000, "timeout_ms": 60000}],
            },
        )
    assert report["intents_rolled_back"] >= 1
    assert report["clean_startup"] is False
    assert report["exit_report_available_at_termination"] is False
    assert report["interrupted_origin_fetches"] >= 1
    assert any(intent["request_id"] == "recovery-in-flight" for intent in report["recovered_intents"])
    theme = [entry for entry in stable_after if entry["canonical_path"] == "/assets/theme.css"]
    assert len(theme) == 1
    assert theme[0]["etag"] == stable["etag"]
    assert theme[0]["generation"] == stable["generation"]
    assert duplicate.status_code == 409, duplicate.text
    assert duplicate.json()["business_code"] == "E_REQUEST_INCOMPLETE"
    assert report["facts"]["post_recovery"]["complete_cache_entries"]


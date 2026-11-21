import os
import threading

import pytest
from fastapi.testclient import TestClient

from pkq.app import create_app
from pkq.config import Config


@pytest.fixture()
def client(sample_root, tmp_path):
    app = create_app(Config(first_page_deadline_sec=10.0),
                     db_path=str(tmp_path / "pkq.db"),
                     snapshot_root=os.path.join(sample_root, "snapshot_v1"))
    with TestClient(app) as c:
        yield c


def drain(client, handle, first_page):
    pages = [first_page] if first_page else []
    seq = len(pages)
    while True:
        r = client.post("/pull", json={"handle": handle, "page_seq": seq})
        body = r.json()
        if body["status"] == "OK":
            pages.append(body)
            seq += 1
        elif body["status"] == "NOT_READY":
            import time; time.sleep(0.02)
        elif body["status"] == "EXHAUSTED":
            return pages
        else:
            raise AssertionError(body)


def test_submit_pull_exhaust(client):
    r = client.post("/query", json={"query_id": "q1", "keys": [0, 7, 35, 1005], "page_size": 2})
    assert r.status_code == 200
    body = r.json()
    assert body["handle"] and body["total_estimate"] == 4
    pages = drain(client, body["handle"], body["first_page"])
    rows = [row for p in pages for row in p["rows"]]
    assert [row["pk"] for row in rows] == sorted(row["pk"] for row in rows)
    # 耗尽后继续拉取 -> E_PAGE_OUT_OF_RANGE
    r = client.post("/pull", json={"handle": body["handle"], "page_seq": len(pages)})
    assert r.status_code == 416 and r.json()["error"]["code"] == "E_PAGE_OUT_OF_RANGE"


def test_empty_keys_empty_handle(client):
    r = client.post("/query", json={"query_id": "q-empty", "keys": []})
    assert r.status_code == 200 and r.json()["status"] == "EXHAUSTED"


def test_query_too_large(client):
    r = client.post("/query", json={"query_id": "q-big", "keys": list(range(1000))})
    assert r.status_code == 413
    err = r.json()["error"]
    assert err["code"] == "E_QUERY_TOO_LARGE" and err["details"]["limit"] == 64


def test_conflicting_query(client):
    client.post("/query", json={"query_id": "q-dup", "keys": [1]})
    r = client.post("/query", json={"query_id": "q-dup", "keys": [2]})
    assert r.status_code == 409 and r.json()["error"]["code"] == "E_CONFLICTING_QUERY"
    r2 = client.post("/query", json={"query_id": "q-dup", "keys": [1]})
    assert r2.status_code == 200 and r2.json()["reused"] is True


def test_concurrent_pull_single_consumer(client):
    r = client.post("/query", json={"query_id": "q-conc", "keys": [0, 7, 14, 21],
                                    "page_size": 4})
    handle = r.json()["handle"]
    drain(client, handle, r.json()["first_page"])  # 页 0 已消费，物化完成
    # 重新建一个会话用于并发拉取同一页
    r = client.post("/query", json={"query_id": "q-conc2", "keys": [0, 7, 14, 21],
                                    "page_size": 4})
    handle2 = r.json()["handle"]
    results = []

    def pull():
        resp = client.post("/pull", json={"handle": handle2, "page_seq": 0})
        results.append(resp.status_code)

    threads = [threading.Thread(target=pull) for _ in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    # 页 0 已在提交时消费，并发拉取全部得到 409 E_PAGE_CONSUMED
    assert all(s == 409 for s in results)
    r = client.post("/pull", json={"handle": handle2, "page_seq": 0})
    assert r.json()["error"]["code"] == "E_PAGE_CONSUMED"


def test_session_status_and_trace(client):
    r = client.post("/query", json={"query_id": "q-st", "keys": [0, 5005], "page_size": 8})
    handle = r.json()["handle"]
    drain(client, handle, r.json()["first_page"])
    st = client.get(f"/sessions/{handle}").json()
    assert st["status"] == "DONE" and st["tasks"].get("DONE", 0) >= 1
    trace = client.get(f"/sessions/{handle}/trace").json()["trace"]
    assert trace and all("decision" in rec and "task_id" in rec for rec in trace)


def test_snapshot_reload_errors(client, sample_root):
    r = client.post("/snapshots/load", json={"root": os.path.join(sample_root, "snapshot_v1")})
    assert r.status_code == 409 and r.json()["error"]["code"] == "E_SNAPSHOT_INVALID"
    r = client.post("/snapshots/load", json={"root": os.path.join(sample_root, "snapshot_v2")})
    assert r.status_code == 200 and r.json()["version"] == 2


def test_block_corrupt_surfaces(client, sample_root):
    client.post("/snapshots/load", json={"root": os.path.join(sample_root, "snapshot_corrupt")})
    r = client.post("/query", json={"query_id": "q-corrupt", "keys": [5005]})
    handle = r.json()["handle"]
    import time
    for _ in range(200):
        st = client.get(f"/sessions/{handle}").json()
        if st["status"] == "FAILED":
            break
        time.sleep(0.02)
    r = client.post("/pull", json={"handle": handle, "page_seq": 0})
    assert r.status_code == 422
    err = r.json()["error"]
    assert err["code"] == "E_BLOCK_CORRUPT" and err["details"]["block_id"] == 5

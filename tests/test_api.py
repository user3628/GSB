"""HTTP 测试：真实 uvicorn 绑定 127.0.0.1 随机端口，仅访问本机回环。"""

import json
import socket
import threading
import time
import urllib.error
import urllib.request

import pytest
import uvicorn

from gpworkbench.api import app
from gpworkbench.database import Database
from gpworkbench.scheduler import SchedulerService
from gpworkbench.spec import parse_workload

from .conftest import SIMPLE_WORKLOAD


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


class HttpClient:
    def __init__(self, base_url: str):
        self.base_url = base_url

    def request(self, method: str, path: str, payload=None):
        data = None
        headers = {}
        if payload is not None:
            data = json.dumps(payload).encode("utf-8")
            headers["Content-Type"] = "application/json"
        req = urllib.request.Request(
            self.base_url + path, data=data, headers=headers, method=method
        )
        try:
            with urllib.request.urlopen(req, timeout=10) as resp:
                return resp.status, json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            return exc.code, json.loads(exc.read().decode("utf-8"))

    def get(self, path):
        return self.request("GET", path)

    def post(self, path, payload=None):
        return self.request("POST", path, payload)


@pytest.fixture()
def server(tmp_path, monkeypatch):
    db_path = tmp_path / "api.sqlite3"
    monkeypatch.setenv("GPWB_DB_PATH", str(db_path))
    import gpworkbench.api as api_mod

    api_mod.DEFAULT_DB_PATH = str(db_path)
    api_mod._SERVICE = None
    port = _free_port()
    config = uvicorn.Config(app, host="127.0.0.1", port=port, log_level="warning")
    server = uvicorn.Server(config)
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    deadline = time.time() + 10
    while time.time() < deadline:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.5):
                break
        except OSError:
            time.sleep(0.1)
    service = api_mod.get_service()
    service.load_spec(parse_workload(SIMPLE_WORKLOAD))
    try:
        yield HttpClient(f"http://127.0.0.1:{port}"), service
    finally:
        server.should_exit = True
        thread.join(timeout=10)


def test_health_and_quota_endpoints(server):
    http, _ = server
    status, body = http.get("/health")
    assert status == 200 and body["status"] == "ok"
    status, quota = http.get("/quota/groups")
    assert status == 200 and len(quota) == 2
    assert all("remaining_memory" in row for row in quota)


def test_admit_and_batch_flow_over_http(server):
    http, _ = server
    for rid in ("r1", "r2", "r3"):
        status, body = http.post(f"/requests/{rid}/admit")
        assert status == 200 and body["state"] == "ADMITTED"
    status, body = http.post("/batching/run", {"group_id": "g1"})
    assert status == 200 and body["batches_created"] >= 1

    status, batches = http.get("/batches?state=PLACED")
    batch_id = batches[0]["batch_id"]
    status, detail = http.get(f"/batches/{batch_id}")
    assert status == 200 and detail["items"]

    assert http.post(f"/batches/{batch_id}/start")[0] == 200
    assert http.post(f"/batches/{batch_id}/complete")[0] == 200


def test_not_found_business_code(server):
    http, _ = server
    status, body = http.post("/requests/ghost/admit")
    assert status == 404 and body["detail"]["code"] == "E_NOT_FOUND"


def test_invalid_transition_returns_business_code(server):
    http, _ = server
    http.post("/requests/r1/admit")
    status, body = http.post(
        "/requests/r1/transition", {"to_state": "COMPLETED", "reason": "illegal"}
    )
    assert status == 409 and body["detail"]["code"] == "E_INVALID_TRANSITION"


def test_inline_invalid_spec_returns_e_spec_invalid(server):
    http, _ = server
    bad = {
        "quota_groups": [
            {"group_id": "g", "memory_limit": "10Gi", "compute_limit": "100m"}
        ],
        "nodes": [
            {
                "node_id": "n",
                "total_memory": "10Gi",
                "total_compute": "100m",
                "used_memory": "999Gi",
                "group_id": "g",
                "health": "HEALTHY",
            }
        ],
        "models": [{"model_id": "m", "cost_per_sample": 1, "max_batch_samples": 4}],
        "requests": [],
    }
    status, body = http.post("/spec/load-inline", bad)
    assert status == 400 and body["error"]["code"] == "E_SPEC_INVALID"


def test_recovery_endpoint_and_intents(server):
    http, service = server
    service.repo.crash_next_intent_type = "ADMIT"
    from gpworkbench.errors import CrashSimulated

    with pytest.raises(CrashSimulated):
        service.admit_request("r1")
    status, report = http.post("/recovery/run")
    assert status == 200
    assert report["actions"][0]["action"] == "ROLL_FORWARD"
    status, events = http.get("/recovery/events")
    assert status == 200 and events


def test_concurrent_readers_never_observe_torn_state(server):
    _, service = server
    for rid in ("r1", "r2", "r3"):
        service.admit_request(rid)

    anomalies = []
    stop = threading.Event()

    def reader():
        while not stop.is_set():
            for request in service.repo.list_requests():
                if request["state"] == "BATCHED":
                    if request["reserved_memory"] <= 0 or not request["batch_id"]:
                        anomalies.append(request["request_id"])

    threads = [threading.Thread(target=reader) for _ in range(4)]
    for thread in threads:
        thread.start()
    for _ in range(5):
        service.run_batching("g1")
        for batch in service.repo.list_batches(state="PLACED"):
            service.start_batch(batch["batch_id"])
            service.complete_batch(batch["batch_id"])
        service.load_spec(parse_workload(SIMPLE_WORKLOAD))
        for rid in ("r1", "r2", "r3"):
            service.admit_request(rid)
    stop.set()
    for thread in threads:
        thread.join()
    assert anomalies == []


def test_group_writer_serialization(server):
    _, service = server
    errors = []

    def admit_many(ids):
        for rid in ids:
            try:
                service.admit_request(rid)
            except Exception as exc:  # 配额拒绝是预期业务结果
                errors.append(str(exc))

    t1 = threading.Thread(target=admit_many, args=(["r1", "r2"],))
    t2 = threading.Thread(target=admit_many, args=(["r3", "r4"],))
    t1.start()
    t2.start()
    t1.join()
    t2.join()
    usage = service.quota_snapshot("g1")[0]
    assert usage["used_memory"] <= usage["memory_limit"]
    assert usage["used_compute"] <= usage["compute_limit"]

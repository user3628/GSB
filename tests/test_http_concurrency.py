from __future__ import annotations

import json
import threading
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor

from alert_sieve.http_api import make_server

from conftest import alert_row, load_baseline, make_alert_increment


class LocalClient:
    def __init__(self, port: int) -> None:
        self.base = f"http://127.0.0.1:{port}"

    def request(self, method: str, path: str, body=None, headers=None):
        data = None
        hdrs = dict(headers or {})
        if body is not None:
            data = json.dumps(body).encode("utf-8")
            hdrs["Content-Type"] = "application/json"
        req = urllib.request.Request(self.base + path, data=data, headers=hdrs, method=method)
        try:
            with urllib.request.urlopen(req, timeout=10) as response:
                return response.status, json.loads(response.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            return exc.code, json.loads(exc.read().decode("utf-8"))

    def get(self, path: str):
        return self.request("GET", path)

    def post(self, path: str, body, headers=None):
        return self.request("POST", path, body, headers)


def _server(store, *, fault_injection: bool = False):
    server = make_server(store, "127.0.0.1", 0, enable_fault_injection=fault_injection)
    thread = threading.Thread(target=server.serve_forever, kwargs={"poll_interval": 0.02}, daemon=True)
    thread.start()
    return server, LocalClient(int(server.server_address[1]))


def _stop(server) -> None:
    server.shutdown()
    server.server_close()


def test_http_error_body_and_concurrent_stale_base(store):
    load_baseline(store)
    server, client = _server(store)
    try:
        status, body = client.get("/v1/batches/missing")
        assert status == 404
        assert body["ok"] is False
        assert body["error"]["code"] == "E_ORDER_NOT_FOUND"
        assert "request_id" in body

        payload_a = make_alert_increment("http-a", 1, 2, [alert_row(1000, "http-fp-a", "ledger")])
        payload_b = make_alert_increment("http-b", 1, 2, [alert_row(1000, "http-fp-b", "ledger")])
        barrier = threading.Barrier(2)

        def submit(payload):
            barrier.wait()
            return client.post("/v1/alerts/increments", payload)

        with ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(submit, [payload_a, payload_b]))
        statuses = sorted(status for status, _ in results)
        assert statuses == [200, 409]
        conflict = next(body for status, body in results if status == 409)
        assert conflict["error"]["code"] == "E_STALE_BASE"
        status, state_body = client.get("/v1/state")
        assert status == 200
        assert state_body["data"]["alert_version"] == 2
        assert state_body["data"]["counts"]["alerts"] == 11
    finally:
        _stop(server)


def test_reader_never_sees_partial_cascade(store):
    load_baseline(store)
    server, client = _server(store, fault_injection=True)
    try:
        payload = json.loads(open("samples/topology_increment_v2.json", encoding="utf-8").read())
        observed = []
        result_holder = {}

        def writer():
            result_holder["result"] = client.post(
                "/v1/topology/increments",
                payload,
                headers={"X-Debug-Sleep-After-Products-Ms": "1200"},
            )

        thread = threading.Thread(target=writer)
        thread.start()
        time.sleep(0.25)
        while thread.is_alive():
            status, body = client.get("/v1/root-causes?service=edge-gateway")
            assert status == 200, body
            data = body["data"]
            version = data["state"]["topology_version"]
            services = {c["service"] for c in data["alerts"][0]["candidates"]}
            if version == 1:
                assert "payment" in services
            elif version == 2:
                assert "payment" not in services
                assert "checkout" in services
            else:
                raise AssertionError(f"unexpected topology version {version}")
            observed.append(version)
        thread.join(timeout=10)
        assert not thread.is_alive()
        status, body = result_holder["result"]
        assert status == 200, body
        status, final_body = client.get("/v1/root-causes?service=edge-gateway")
        assert final_body["data"]["state"]["topology_version"] == 2
        assert observed
        assert set(observed) <= {1, 2}
    finally:
        _stop(server)

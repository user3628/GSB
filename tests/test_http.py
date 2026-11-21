import json
import threading
import urllib.request

import pytest

from iqa.http_app import serve
from iqa.pipeline import Pipeline


@pytest.fixture()
def server(work_dir):
    from http.server import ThreadingHTTPServer
    from iqa.http_app import _make_handler

    httpd = ThreadingHTTPServer(("127.0.0.1", 0), _make_handler(work_dir))
    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()
    host, port = httpd.server_address
    yield f"http://{host}:{port}", work_dir
    httpd.shutdown()
    httpd.server_close()
    thread.join(timeout=2)


def _get(url):
    with urllib.request.urlopen(url, timeout=5) as response:
        return json.loads(response.read().decode("utf-8"))


def _post(url):
    with urllib.request.urlopen(url, data=b"", timeout=10) as response:
        return json.loads(response.read().decode("utf-8"))


def test_http_health_run_status_scores_stop(server):
    base, work_dir = server
    assert _get(base + "/health") == {"status": "ok"}
    run_summary = _post(base + "/run")
    assert run_summary["image_count"] == 6
    status = _get(base + "/status")
    assert len(status["images"]) == 6
    fingerprints = _get(base + "/fingerprints")
    assert "img_001_blocks" in fingerprints
    scores = _get(base + "/scores")
    assert any(item["image_id"] == "img_001_blocks" for item in scores)
    diagnostics = _get(base + "/diagnostics")
    assert "diagnostics" in diagnostics
    stopped = _post(base + "/stop")
    assert stopped["stop_requested"] is True
    assert Pipeline(work_dir).store.stop_requested() is True


def test_http_injection_endpoint(server):
    base, _ = server
    summary = _post(base + "/run?injection=nan_pixels&image_id=img_001_blocks")
    codes = [item["code"] for item in summary["diagnostics"]]
    assert "E_NUMERIC" in codes

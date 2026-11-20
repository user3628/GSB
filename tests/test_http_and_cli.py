import json
import os
import subprocess
import sys
import threading
import time
import urllib.request
import urllib.error

import pytest

from layout_order.generator import generate_documents
from layout_order.http_app import create_server


@pytest.fixture()
def server(tmp_path):
    sample_dir = tmp_path / "samples"
    sample_dir.mkdir()
    sample_path = sample_dir / "documents.json"
    from layout_order.hashing import canonical_json
    sample_path.write_text(
        canonical_json({"seed": 20261121, "documents": generate_documents()}) + "\n",
        encoding="utf-8",
    )
    httpd = create_server("127.0.0.1", 0, str(tmp_path), str(sample_path))
    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()
    host, port = httpd.server_address
    base = f"http://{host}:{port}"
    yield base, tmp_path
    httpd.shutdown()
    httpd.server_close()
    thread.join(timeout=5)


def _request(base: str, method: str = "GET", payload=None):
    data = None
    headers = {}
    if payload is not None:
        data = json.dumps(payload).encode("utf-8")
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(base, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            return resp.status, json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        return exc.code, json.loads(exc.read().decode("utf-8"))


def test_http_full_flow_and_stop_resume(server):
    base, workspace = server
    status, payload = _request(f"{base}/api/health")
    assert status == 200 and payload["status"] == "ok"

    status, payload = _request(f"{base}/api/samples/import", "POST", {})
    assert status == 200 and len(payload["imported"]) == 4

    status, payload = _request(f"{base}/api/analyze", "POST", {})
    assert status == 200
    assert all(item["status"] == "completed" for item in payload["reports"])

    status, payload = _request(f"{base}/api/status?doc_id=doc_double_en")
    assert status == 200
    assert all(payload["stages"][stage]["present"] for stage in payload["stages"])
    assert len(payload["stage_fingerprints"]) == 4

    status, payload = _request(f"{base}/api/documents/doc_double_mixed/spanning")
    assert status == 200
    assert payload["spanning_block_count"] >= 2

    status, payload = _request(f"{base}/api/documents/doc_single_zh/result")
    assert status == 200
    assert len(payload["result_digest"]) == 64

    status, payload = _request(f"{base}/api/documents/doc_single_zh/update", "POST")
    assert status == 200 and payload["verified_against_full_build"] is True


def test_http_error_codes(server):
    base, _ = server
    _request(f"{base}/api/samples/import", "POST", {})
    status, payload = _request(f"{base}/api/status?doc_id=missing_doc")
    assert status == 404 and payload["code"] == "E_DOC_NOT_FOUND"


def test_cli_end_to_end(tmp_path):
    workspace = tmp_path / "ws"
    root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

    def run_cli(*args):
        proc = subprocess.run(
            [sys.executable, "-m", "layout_order.cli", "--workspace", str(workspace), *args],
            cwd=root, capture_output=True, text=True, timeout=60,
        )
        return proc

    proc = run_cli("generate")
    assert proc.returncode == 0, proc.stderr
    sample = workspace / "samples" / "documents.json"
    assert sample.exists()

    proc = run_cli("analyze", "--stop-after", "column", "--doc-id", "doc_double_en")
    assert proc.returncode == 0, proc.stderr
    report = json.loads(proc.stdout)
    assert report["reports"][0]["status"] == "stopped"

    proc = run_cli("status", "--doc-id", "doc_double_en")
    status = json.loads(proc.stdout)
    assert status["stages"]["column"]["present"] is True
    assert status["stages"]["summary"]["present"] is False

    proc = run_cli("resume")
    assert proc.returncode == 0, proc.stderr

    proc = run_cli("verify")
    assert proc.returncode == 0, proc.stderr
    verification = json.loads(proc.stdout)["verification"]
    assert all(item["verified"] for item in verification)

    proc = run_cli("export", "--doc-id", "doc_single_zh")
    assert proc.returncode == 0, proc.stderr
    exported = json.loads(proc.stdout)
    assert (workspace / "exports" / "doc_single_zh.result.json").exists()

    proc = run_cli("fingerprints", "--doc-id", "doc_single_zh")
    assert proc.returncode == 0 and len(json.loads(proc.stdout)["stage_fingerprints"]) == 4

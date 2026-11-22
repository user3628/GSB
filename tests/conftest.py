from __future__ import annotations

import threading
from pathlib import Path

import pytest
from werkzeug.serving import make_server

from mqmw.api import create_app
from tools.test_consumer import create_consumer_app


def _serve(app):
    server = make_server("127.0.0.1", 0, app, threaded=True)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server, thread


@pytest.fixture
def consumer_server():
    app = create_consumer_app()
    server, thread = _serve(app)
    base = f"http://127.0.0.1:{server.server_port}"
    try:
        yield {"base": base, "app": app, "state": app.config["CONSUMER_STATE"]}
    finally:
        server.shutdown()
        thread.join(timeout=3)


@pytest.fixture
def mw_server(tmp_path: Path):
    db_path = tmp_path / "middleware.db"
    app = create_app(str(db_path), ack_base_url="http://127.0.0.1:8080")
    server, thread = _serve(app)
    base = f"http://127.0.0.1:{server.server_port}"
    app.config["MQMW_ACK_BASE_URL"] = base
    try:
        yield {
            "base": base,
            "db": str(db_path),
            "app": app,
            "store": app.extensions["mqmw"]["store"],
        }
    finally:
        server.shutdown()
        thread.join(timeout=3)

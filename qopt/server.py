"""Local loopback HTTP API for the query planning component.

Endpoints
  GET  /v1/health            liveness probe
  GET  /v1/snapshot          active snapshot version / fingerprint
  POST /v1/snapshot/load     {"path": ...} validate + atomically replace
  POST /v1/plan              {"query": {...}, "snapshot_version": optional}

All errors are returned as {"ok": false, "error": {"code", "message",
"details"}} with the business codes from qopt.errors.
"""

import argparse
import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from .errors import BadJsonError, NotFoundError, QoptError
from .planner import plan_query
from .stats import SnapshotRegistry

DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 8787
MAX_BODY_BYTES = 1 << 20


def _snapshot_info(snapshot):
    return {
        "version": snapshot.version,
        "generated_at": snapshot.generated_at,
        "fingerprint": snapshot.fingerprint,
        "tables": sorted(snapshot.tables),
        "indexes": sorted(idx.name for idx in snapshot.indexes),
    }


def _make_handler(registry):
    class Handler(BaseHTTPRequestHandler):
        server_version = "qopt/1.0"
        protocol_version = "HTTP/1.1"

        def log_message(self, *args):
            pass

        def _send(self, obj, status=200):
            body = json.dumps(obj, ensure_ascii=False, sort_keys=True).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def _send_error(self, err):
            self._send(err.to_dict(), status=err.http_status)

        def _read_json(self):
            length = self.headers.get("Content-Length")
            if length is None:
                raise BadJsonError("missing Content-Length header")
            try:
                n = int(length)
            except ValueError:
                raise BadJsonError("invalid Content-Length header")
            if n < 0 or n > MAX_BODY_BYTES:
                raise BadJsonError("request body too large", max_bytes=MAX_BODY_BYTES)
            raw = self.rfile.read(n)
            try:
                return json.loads(raw.decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise BadJsonError("request body is not valid JSON", error=str(exc))

        def do_GET(self):
            try:
                if self.path == "/v1/health":
                    self._send({"ok": True, "status": "up"})
                elif self.path == "/v1/snapshot":
                    snap = registry.require()
                    self._send({"ok": True, "snapshot": _snapshot_info(snap)})
                else:
                    raise NotFoundError("unknown route", path=self.path)
            except QoptError as err:
                self._send_error(err)
            except Exception as exc:
                self._send_error(QoptError("internal error: %s" % exc))

        def do_POST(self):
            try:
                if self.path == "/v1/snapshot/load":
                    body = self._read_json()
                    path = body.get("path") if isinstance(body, dict) else None
                    if not isinstance(path, str) or not path:
                        raise BadJsonError("snapshot/load requires a 'path' string")
                    snap = registry.load(path)
                    self._send({"ok": True, "snapshot": _snapshot_info(snap)})
                elif self.path == "/v1/plan":
                    body = self._read_json()
                    if not isinstance(body, dict):
                        raise BadJsonError("plan request must be a JSON object")
                    snap = registry.require(body.get("snapshot_version"))
                    plan = plan_query(body.get("query"), snap)
                    self._send({"ok": True, "plan": plan})
                else:
                    raise NotFoundError("unknown route", path=self.path)
            except QoptError as err:
                self._send_error(err)
            except Exception as exc:
                self._send_error(QoptError("internal error: %s" % exc))

    return Handler


def create_server(registry=None, host=DEFAULT_HOST, port=DEFAULT_PORT):
    registry = registry or SnapshotRegistry()
    return ThreadingHTTPServer((host, port), _make_handler(registry)), registry


def main(argv=None):
    parser = argparse.ArgumentParser(description="qopt local planning API")
    parser.add_argument("--host", default=DEFAULT_HOST)
    parser.add_argument("--port", type=int, default=DEFAULT_PORT)
    parser.add_argument("--snapshot", help="snapshot JSON to load at startup")
    args = parser.parse_args(argv)
    server, registry = create_server(host=args.host, port=args.port)
    if args.snapshot:
        snap = registry.load(args.snapshot)
        print("loaded snapshot %s (%s)" % (snap.version, snap.fingerprint[:12]))
    print("qopt API listening on http://%s:%d" % (args.host, args.port))
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == "__main__":
    main()

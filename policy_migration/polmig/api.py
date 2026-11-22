"""Loopback HTTP API built only on the Python standard library."""

import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

from .errors import HTTP_STATUS, MigrationError
from .service import MigrationService


def _make_handler(service):
    class Handler(BaseHTTPRequestHandler):
        server_version = "PolicyMigration/1.0"

        def log_message(self, fmt, *args):
            return

        def _send_json(self, status, payload):
            body = json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def _send_error(self, exc):
            status = HTTP_STATUS.get(exc.code, 500)
            self._send_json(status, {"error": exc.to_dict()})

        def _read_body(self):
            length = int(self.headers.get("Content-Length", "0"))
            return self.rfile.read(length) if length else b""

        def do_GET(self):
            try:
                parsed = urlparse(self.path)
                query = parse_qs(parsed.query)
                parts = [p for p in parsed.path.split("/") if p]
                if parts == ["api", "health"]:
                    self._send_json(200, {"status": "ok"})
                elif len(parts) == 4 and parts[:3] == ["api", "migration", "batches"] \
                        and query.get("detail") == ["decisions"]:
                    self._send_json(200, {
                        "batch_id": parts[3],
                        "decisions": service.get_decisions(parts[3]),
                    })
                elif len(parts) == 4 and parts[:3] == ["api", "migration", "batches"]:
                    row = service.store.get_batch(parts[3])
                    if row is None:
                        from .errors import BatchNotFound
                        raise BatchNotFound("unknown batch %s" % parts[3],
                                            {"batch_id": parts[3]})
                    self._send_json(200, {
                        "batch_id": row["batch_id"],
                        "status": row["status"],
                        "base_version": row["base_version"],
                        "committed_version": row["committed_version"],
                        "verdict": row["verdict"],
                        "error_code": row["error_code"],
                        "error_message": row["error_message"],
                    })
                elif len(parts) == 5 and parts[:3] == ["api", "migration", "batches"] \
                        and parts[4] == "diff":
                    self._send_json(200, {
                        "batch_id": parts[3],
                        "plan_diff": service.plan_diff(parts[3]),
                    })
                elif parts[:2] == ["api", "parent-chain"] and len(parts) == 3:
                    version = query.get("version", [None])[0]
                    view = service.parent_chain_view(
                        parts[2], int(version) if version is not None else None
                    )
                    self._send_json(200, view)
                elif parts == ["api", "recovery"]:
                    self._send_json(200, service.recovery_report_view())
                elif parts == ["api", "versions"]:
                    self._send_json(200, {
                        "versions": [dict(r) for r in service.store.list_versions()],
                        "current_version": service.store.current_version(),
                    })
                else:
                    self._send_json(404, {"error": {
                        "code": "E_INTERNAL", "message": "no such route",
                        "details": {"path": parsed.path}}})
            except MigrationError as exc:
                self._send_error(exc)
            except Exception as exc:  # pragma: no cover - defensive envelope
                self._send_json(500, {"error": {
                    "code": "E_INTERNAL", "message": str(exc), "details": {}}})

        def do_POST(self):
            try:
                parsed = urlparse(self.path)
                parts = [p for p in parsed.path.split("/") if p]
                if parts != ["api", "migration", "batches"]:
                    self._send_json(404, {"error": {
                        "code": "E_INTERNAL", "message": "no such route",
                        "details": {"path": parsed.path}}})
                    return
                raw = self._read_body()
                content_type = self.headers.get("Content-Type", "application/json")
                if "application/octet-stream" in content_type:
                    request = {
                        "batch_id": self.headers["X-Batch-Id"],
                        "base_version": int(self.headers["X-Base-Version"]),
                        "format_version": int(self.headers.get("X-Format-Version", "1")),
                    }
                    payload = raw
                else:
                    envelope = json.loads(raw.decode("utf-8"))
                    request = {
                        "batch_id": envelope["batch_id"],
                        "base_version": int(envelope["base_version"]),
                        "format_version": envelope.get("format_version", 1),
                    }
                    payload = envelope["payload"].encode("utf-8") \
                        if isinstance(envelope["payload"], str) else envelope["payload"]
                result = service.submit_batch(request, payload)
                self._send_json(200, result)
            except MigrationError as exc:
                self._send_error(exc)
            except (KeyError, ValueError, json.JSONDecodeError) as exc:
                self._send_json(400, {"error": {
                    "code": "E_INTERNAL", "message": "malformed request: %s" % exc,
                    "details": {}}})
            except Exception as exc:  # pragma: no cover - defensive envelope
                self._send_json(500, {"error": {
                    "code": "E_INTERNAL", "message": str(exc), "details": {}}})

    return Handler


def build_server(db_path, host="127.0.0.1", port=0, max_parent_depth=4):
    service = MigrationService(db_path, max_parent_depth=max_parent_depth)
    server = ThreadingHTTPServer((host, port), _make_handler(service))
    server.service = service
    return server


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Policy migration HTTP service")
    parser.add_argument("--db", default="policy_migration.db")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8080)
    parser.add_argument("--max-parent-depth", type=int, default=4)
    args = parser.parse_args()
    server = build_server(args.db, args.host, args.port, args.max_parent_depth)
    print("policy migration service listening on http://%s:%d"
          % (args.host, server.server_address[1]))
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == "__main__":
    main()

"""Local loopback HTTP service using only the Python standard library."""

from __future__ import annotations

import json
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, unquote, urlparse

from . import __version__
from .config import EngineConfig
from .engine import SamplingEngine
from .errors import EngineError, E_NOT_FOUND, invalid_manifest


MAX_BODY_BYTES = 32 * 1024 * 1024


class LocalSamplingHTTPServer(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True

    def __init__(self, address: tuple[str, int], engine: SamplingEngine) -> None:
        self.engine = engine
        super().__init__(address, SamplingRequestHandler)


class SamplingRequestHandler(BaseHTTPRequestHandler):
    server: LocalSamplingHTTPServer
    protocol_version = "HTTP/1.1"

    def log_message(self, format: str, *args: Any) -> None:
        # Keep the demo output deterministic by logging only to stderr in a
        # single stable line format.
        print(
            f"[http] {self.address_string()} {format % args}",
            flush=True,
        )

    def _send_json(self, status: int, payload: Any) -> None:
        body = (
            json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2) + "\n"
        ).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def _send_error(self, error: EngineError) -> None:
        self._send_json(
            error.http_status,
            {
                "ok": False,
                "code": error.code,
                "message": error.message,
                "context": error.context,
            },
        )

    def _read_body(self) -> dict[str, Any]:
        length_raw = self.headers.get("Content-Length")
        if length_raw is None:
            return {}
        try:
            length = int(length_raw)
        except ValueError:
            raise invalid_manifest("invalid Content-Length")
        if length < 0 or length > MAX_BODY_BYTES:
            raise invalid_manifest(
                "request body is too large", length=length, maximum=MAX_BODY_BYTES
            )
        raw = self.rfile.read(length)
        if not raw:
            return {}
        try:
            payload = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise invalid_manifest("request body is not valid JSON", detail=str(exc))
        if not isinstance(payload, dict):
            raise invalid_manifest("request body must be a JSON object")
        return payload

    def do_GET(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        try:
            self._route_get(parsed.path, parse_qs(parsed.query))
        except EngineError as exc:
            self._send_error(exc)
        except Exception as exc:  # pragma: no cover - defensive boundary
            self._send_json(
                HTTPStatus.INTERNAL_SERVER_ERROR,
                {"ok": False, "code": "E_INTERNAL", "message": str(exc)},
            )

    def do_POST(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        try:
            body = self._read_body()
            self._route_post(parsed.path, parse_qs(parsed.query), body)
        except EngineError as exc:
            self._send_error(exc)
        except Exception as exc:  # pragma: no cover - defensive boundary
            self._send_json(
                HTTPStatus.INTERNAL_SERVER_ERROR,
                {"ok": False, "code": "E_INTERNAL", "message": str(exc)},
            )

    def _route_get(self, path: str, query: dict[str, list[str]]) -> None:
        engine = self.server.engine
        if path in {"/health", "/api/health"}:
            self._send_json(
                HTTPStatus.OK,
                {
                    "ok": True,
                    "version": __version__,
                    "binding": f"{self.server.server_address[0]}:{self.server.server_address[1]}",
                    "workspace": engine.workspace_metadata(),
                    "request_count": len(engine.list_requests()),
                },
            )
            return
        if path in {"/api/requests", "/requests"}:
            self._send_json(HTTPStatus.OK, {"ok": True, "requests": engine.list_requests()})
            return
        if path in {"/api/manifests", "/manifests"}:
            self._send_json(
                HTTPStatus.OK, {"ok": True, "manifests": engine.manifest_groups()}
            )
            return
        if path in {"/api/result", "/result"}:
            self._send_json(HTTPStatus.OK, {"ok": True, "result": engine.export_result()})
            return
        if path in {"/api/corpus/summary", "/corpus/summary"}:
            self._send_json(
                HTTPStatus.OK, {"ok": True, "corpus": engine.corpus_summary()}
            )
            return
        parts = [unquote(part) for part in path.strip("/").split("/") if part]
        if len(parts) >= 3 and parts[0] == "api" and parts[1] == "requests":
            request_id = parts[2]
            action = parts[3] if len(parts) > 3 else "status"
            if action == "status":
                payload = engine.request_status(request_id)
            elif action == "fingerprints":
                payload = engine.request_fingerprints(request_id)
            elif action == "tokens":
                payload = engine.request_tokens(request_id)
            elif action == "temperature":
                payload = engine.request_temperature_detail(request_id)
            elif action == "summary":
                payload = engine.request_summary(request_id)
            else:
                raise EngineError(
                    E_NOT_FOUND,
                    "unknown request endpoint",
                    {"path": path, "action": action},
                )
            self._send_json(HTTPStatus.OK, {"ok": True, **payload})
            return
        if len(parts) >= 4 and parts[0] == "api" and parts[1] == "manifests" and parts[3] == "export":
            target_raw = query.get("target_version", ["2"])[0]
            try:
                target_version = int(target_raw)
            except ValueError:
                raise invalid_manifest(
                    "target_version must be 1 or 2", value=target_raw
                )
            payload = engine.export_manifest(parts[2], target_version)
            self._send_json(
                HTTPStatus.OK,
                {
                    "ok": True,
                    "manifest_id": parts[2],
                    "target_version": target_version,
                    "manifest": payload,
                },
            )
            return
        raise EngineError(E_NOT_FOUND, "endpoint not found", {"path": path})

    def _route_post(
        self,
        path: str,
        query: dict[str, list[str]],
        body: dict[str, Any],
    ) -> None:
        engine = self.server.engine
        if path in {"/api/import", "/import"}:
            if "path" in body:
                payload = engine.import_manifest_path(str(body["path"]))
            elif "manifest" in body:
                manifest = body["manifest"]
                if not isinstance(manifest, dict):
                    raise invalid_manifest("manifest must be an object")
                payload = engine.import_manifest_document(
                    manifest,
                    base_dir=str(body.get("base_dir", ".")),
                    manifest_id_source=body.get("manifest_id_source"),
                )
            else:
                raise invalid_manifest("import requires path or manifest")
            self._send_json(HTTPStatus.OK, payload)
            return
        if path in {"/api/import-logits", "/import-logits"}:
            if "path" not in body:
                raise invalid_manifest("import-logits requires path")
            self._send_json(HTTPStatus.OK, engine.import_logits(str(body["path"])))
            return
        if path in {"/api/run", "/run"}:
            request_ids = body.get("request_ids")
            if request_ids is not None and not isinstance(request_ids, list):
                raise invalid_manifest("request_ids must be a list")
            stop_after = body.get("stop_after_requests")
            if stop_after is not None and (
                isinstance(stop_after, bool) or not isinstance(stop_after, int)
            ):
                raise invalid_manifest("stop_after_requests must be an integer")
            payload = engine.run(
                request_ids=request_ids,
                stop_after_requests=stop_after,
            )
            self._send_json(HTTPStatus.OK, payload)
            return
        if path in {"/api/stop", "/stop"}:
            self._send_json(HTTPStatus.OK, engine.stop())
            return
        if path in {"/api/resume", "/resume"}:
            request_ids = body.get("request_ids")
            if request_ids is not None and not isinstance(request_ids, list):
                raise invalid_manifest("request_ids must be a list")
            self._send_json(
                HTTPStatus.OK, engine.resume(request_ids=request_ids)
            )
            return
        if path in {"/api/snapshot/export", "/snapshot/export"}:
            if "path" not in body:
                raise invalid_manifest("snapshot export requires path")
            snapshot = engine.export_workspace_snapshot(str(body["path"]))
            self._send_json(
                HTTPStatus.OK,
                {"ok": True, "path": str(Path(body["path"]).resolve()), "snapshot": snapshot},
            )
            return
        if path in {"/api/snapshot/validate", "/snapshot/validate"}:
            if "path" not in body:
                raise invalid_manifest("snapshot validation requires path")
            snapshot = engine.validate_workspace_snapshot(
                str(body["path"]),
                allow_rollback=bool(body.get("allow_rollback", False)),
            )
            self._send_json(HTTPStatus.OK, {"ok": True, "snapshot": snapshot})
            return
        raise EngineError(E_NOT_FOUND, "endpoint not found", {"path": path})


def create_server(
    engine: SamplingEngine,
    config: EngineConfig,
    *,
    host: str | None = None,
    port: int | None = None,
) -> LocalSamplingHTTPServer:
    bind_host = host if host is not None else config.host
    if bind_host not in {"127.0.0.1", "localhost"}:
        raise invalid_manifest(
            "HTTP service may only bind to 127.0.0.1",
            field="host",
            value=bind_host,
        )
    bind_port = config.port if port is None else int(port)
    return LocalSamplingHTTPServer((bind_host, bind_port), engine)


def serve(engine: SamplingEngine, config: EngineConfig) -> None:
    server = create_server(engine, config)
    host, port = server.server_address[:2]
    print(
        json.dumps(
            {
                "ok": True,
                "message": "local sampling engine listening",
                "binding": f"http://{host}:{port}",
                "workspace": str(engine.workspace),
            },
            ensure_ascii=False,
        ),
        flush=True,
    )
    try:
        server.serve_forever(poll_interval=0.1)
    except KeyboardInterrupt:
        pass
    finally:
        server.shutdown()
        server.server_close()

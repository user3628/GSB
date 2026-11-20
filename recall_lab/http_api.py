from __future__ import annotations

import json
import re
import threading
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

from .engine import Lab
from .errors import BusinessError, ErrorCode
from .generate import generate_dataset
from .workspace import init_workspace, read_meta


SCALE_RE = re.compile(r"^/scales/([^/]+)(?:/(.*))?$")


class LabService:
    def __init__(self, *, root: str | Path, config_path: str | Path):
        self.root = Path(root).resolve()
        self.config_path = Path(config_path).resolve()
        self._lock = threading.RLock()
        self._lab: Lab | None = None

    def get(self) -> Lab:
        with self._lock:
            if self._lab is None:
                self._lab = Lab(self.root, config_path=self.config_path)
            return self._lab

    def import_inputs(
        self,
        *,
        manifest_path: str | Path,
        config_path: str | Path,
        force: bool,
    ) -> Lab:
        with self._lock:
            init_workspace(self.root, manifest_path, config_path, force=force)
            self.config_path = Path(config_path).resolve()
            self._lab = Lab(self.root, config_path=self.config_path)
            return self._lab

    def reload(self, *, config_path: str | Path | None = None) -> Lab:
        with self._lock:
            if config_path is not None:
                self.config_path = Path(config_path).resolve()
            self._lab = Lab(self.root, config_path=self.config_path)
            return self._lab


class LabHTTPServer(ThreadingHTTPServer):
    daemon_threads = True

    def __init__(self, address: tuple[str, int], service: LabService):
        self.service = service
        super().__init__(address, LabRequestHandler)


class LabRequestHandler(BaseHTTPRequestHandler):
    server: LabHTTPServer
    protocol_version = "HTTP/1.1"

    def log_message(self, format: str, *args: Any) -> None:  # noqa: A003
        # Keep the deterministic demo quiet; errors are returned in JSON bodies.
        return

    def _send_json(self, status: int, payload: Any) -> None:
        data = json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(data)

    def _send_error(self, exc: BusinessError) -> None:
        self._send_json(exc.http_status, {"ok": False, "error": exc.as_dict()})

    def _read_body(self) -> dict[str, Any]:
        length_header = self.headers.get("Content-Length", "0")
        try:
            length = int(length_header)
        except ValueError as exc:
            raise BusinessError(ErrorCode.INVALID_INPUT, "Content-Length is invalid") from exc
        if length > 2 * 1024 * 1024:
            raise BusinessError(ErrorCode.INVALID_INPUT, "request body is too large")
        if length == 0:
            return {}
        data = self.rfile.read(length)
        try:
            value = json.loads(data.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise BusinessError(ErrorCode.INVALID_INPUT, "request body is not valid JSON", {"reason": str(exc)}) from exc
        if not isinstance(value, dict):
            raise BusinessError(ErrorCode.INVALID_INPUT, "request body must be a JSON object")
        return value

    def _scales_from(self, value: Any) -> list[str] | None:
        if value is None or value == "all" or value == []:
            return None
        if isinstance(value, str):
            return [item.strip() for item in value.split(",") if item.strip()] or None
        if isinstance(value, list) and all(isinstance(item, str) for item in value):
            return value
        raise BusinessError(ErrorCode.INVALID_INPUT, "scales must be a string list")

    def do_GET(self) -> None:  # noqa: N802
        try:
            parsed = urlparse(self.path)
            query = parse_qs(parsed.query)
            if parsed.path == "/health":
                self._send_json(HTTPStatus.OK, {"ok": True, "status": "healthy"})
                return
            if parsed.path == "/ready":
                lab = self.server.service.get()
                self._send_json(HTTPStatus.OK, {"ok": True, "workspace_id": lab.workspace_id})
                return
            if parsed.path == "/status":
                lab = self.server.service.get()
                scales = self._scales_from(query.get("scale", [None])[0])
                self._send_json(HTTPStatus.OK, {"ok": True, "data": lab.status(scales=scales)})
                return
            if parsed.path == "/summary":
                lab = self.server.service.get()
                path = lab.root / "exports" / "experiment-summary.json"
                if not path.exists():
                    raise BusinessError(ErrorCode.INVALID_STAGE, "summary has not been exported")
                self._send_json(HTTPStatus.OK, {"ok": True, "data": json.loads(path.read_text(encoding="utf-8"))})
                return
            if parsed.path == "/export":
                lab = self.server.service.get()
                scales = self._scales_from(query.get("scales", [None])[0])
                result = lab.export_summary(scales=scales)
                self._send_json(HTTPStatus.OK, {"ok": True, "data": result})
                return
            match = SCALE_RE.match(parsed.path)
            if match:
                scale, resource = match.groups()
                lab = self.server.service.get()
                if resource in (None, "", "status"):
                    self._send_json(HTTPStatus.OK, {"ok": True, "data": lab.status(scales=[scale])["scales"][0]})
                    return
                if resource == "decision":
                    self._send_json(HTTPStatus.OK, {"ok": True, "data": lab.decision(scale)})
                    return
                if resource == "version":
                    self._send_json(HTTPStatus.OK, {"ok": True, "data": lab.version_info(scale)})
                    return
                if resource == "fusion":
                    query_id = query.get("query_id", [None])[0]
                    self._send_json(HTTPStatus.OK, {"ok": True, "data": lab.fusion_detail(scale, query_id)})
                    return
                if resource.startswith("artifact/"):
                    stage = resource.split("/", 1)[1]
                    self._send_json(HTTPStatus.OK, {"ok": True, "data": lab.artifact(scale, stage)})
                    return
            raise BusinessError(ErrorCode.NOT_FOUND, "route not found", {"path": parsed.path})
        except BusinessError as exc:
            self._send_error(exc)
        except Exception as exc:  # pragma: no cover - defensive HTTP boundary
            self._send_json(
                HTTPStatus.INTERNAL_SERVER_ERROR,
                {"ok": False, "error": {"code": "E_INTERNAL", "message": str(exc), "details": {}}},
            )

    def do_POST(self) -> None:  # noqa: N802
        try:
            parsed = urlparse(self.path)
            body = self._read_body()
            if parsed.path == "/import":
                manifest = body.get("manifest_path")
                config = body.get("config_path")
                if not isinstance(manifest, str) or not isinstance(config, str):
                    raise BusinessError(
                        ErrorCode.INVALID_INPUT,
                        "manifest_path and config_path are required",
                    )
                lab = self.server.service.import_inputs(
                    manifest_path=manifest,
                    config_path=config,
                    force=bool(body.get("force", False)),
                )
                self._send_json(
                    HTTPStatus.OK,
                    {
                        "ok": True,
                        "data": {
                            "workspace_id": lab.workspace_id,
                            "config_version": lab.config.fingerprint,
                            "scales": [entry.scale for entry in lab.manifest.entries],
                        },
                    },
                )
                return
            if parsed.path == "/generate":
                out_dir = body.get("out_dir", "data")
                seed = body.get("seed", 20261120)
                if not isinstance(out_dir, str) or isinstance(seed, bool) or not isinstance(seed, int):
                    raise BusinessError(ErrorCode.INVALID_INPUT, "out_dir/seed are invalid")
                manifest = generate_dataset(out_dir, seed=seed)
                self._send_json(
                    HTTPStatus.OK,
                    {"ok": True, "data": {"manifest": str(manifest), "seed": seed}},
                )
                return
            if parsed.path == "/run":
                lab = self.server.service.get()
                result = lab.run(
                    scales=self._scales_from(body.get("scales")),
                    until=body.get("until", "summary"),
                    stop_after=body.get("stop_after"),
                    export=bool(body.get("export", True)),
                )
                self._send_json(HTTPStatus.OK, {"ok": True, "data": result})
                return
            if parsed.path == "/stage":
                scale = body.get("scale")
                stage = body.get("stage")
                if not isinstance(scale, str) or not isinstance(stage, str):
                    raise BusinessError(ErrorCode.INVALID_INPUT, "scale and stage are required")
                lab = self.server.service.get()
                self._send_json(HTTPStatus.OK, {"ok": True, "data": lab.run_stage(scale, stage)})
                return
            if parsed.path == "/stop":
                lab = self.server.service.get()
                self._send_json(
                    HTTPStatus.OK,
                    {"ok": True, "data": lab.request_stop(reason=str(body.get("reason", "http_stop")))},
                )
                return
            if parsed.path == "/resume":
                lab = self.server.service.get()
                result = lab.resume(
                    scales=self._scales_from(body.get("scales")),
                    until=body.get("until", "summary"),
                    export=bool(body.get("export", True)),
                )
                self._send_json(HTTPStatus.OK, {"ok": True, "data": result})
                return
            if parsed.path == "/config":
                config_path = body.get("config_path")
                if not isinstance(config_path, str):
                    raise BusinessError(ErrorCode.INVALID_INPUT, "config_path is required")
                lab = self.server.service.reload(config_path=config_path)
                self._send_json(
                    HTTPStatus.OK,
                    {"ok": True, "data": {"config_path": str(lab.config_path), "config_version": lab.config.fingerprint}},
                )
                return
            raise BusinessError(ErrorCode.NOT_FOUND, "route not found", {"path": parsed.path})
        except BusinessError as exc:
            self._send_error(exc)
        except Exception as exc:  # pragma: no cover - defensive HTTP boundary
            self._send_json(
                HTTPStatus.INTERNAL_SERVER_ERROR,
                {"ok": False, "error": {"code": "E_INTERNAL", "message": str(exc), "details": {}}},
            )


def serve(
    *,
    root: str | Path,
    config_path: str | Path,
    manifest_path: str | Path,
    host: str = "127.0.0.1",
    port: int = 8765,
) -> None:
    root_path = Path(root).resolve()
    if not (root_path / "meta.json").exists():
        init_workspace(root_path, manifest_path, config_path, force=False)
    else:
        read_meta(root_path)
    service = LabService(root=root_path, config_path=config_path)
    httpd = LabHTTPServer((host, port), service)
    print(json.dumps({"status": "serving", "host": host, "port": port}, ensure_ascii=False), flush=True)
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        httpd.server_close()

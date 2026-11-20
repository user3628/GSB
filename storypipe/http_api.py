from __future__ import annotations

import json
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

from .checkpoints import CheckpointStore
from .errors import PipelineError
from .pipeline import PipelineRunner
from .verify import compare_scales, validate_workspace
from .workspace import import_sample


ERROR_HTTP_STATUS = {
    "E_PIPELINE_BUSY": HTTPStatus.CONFLICT,
    "E_STALE_CHECKPOINT": HTTPStatus.CONFLICT,
    "E_INVALID_STAGE": HTTPStatus.CONFLICT,
    "E_MISSING_ASSET": HTTPStatus.NOT_FOUND,
    "E_TRANSITION_OVERFLOW": HTTPStatus.UNPROCESSABLE_ENTITY,
    "E_INVALID_TIMELINE": HTTPStatus.UNPROCESSABLE_ENTITY,
    "E_INVALID_SHOT": HTTPStatus.UNPROCESSABLE_ENTITY,
    "E_INVALID_CONFIG": HTTPStatus.UNPROCESSABLE_ENTITY,
    "E_INVALID_ASSET": HTTPStatus.UNPROCESSABLE_ENTITY,
    "E_IMPORT_CONFLICT": HTTPStatus.CONFLICT,
    "E_NOT_FOUND": HTTPStatus.NOT_FOUND,
    "E_ARTIFACT_CORRUPT": HTTPStatus.CONFLICT,
}


class ApiHandler(BaseHTTPRequestHandler):
    server_version = "StorypipeHTTP/1.0"

    def log_message(self, format_string: str, *args: Any) -> None:
        return

    def _workspace(self, query: dict[str, list[str]]) -> Path:
        value = query.get("workspace", [None])[0]
        if not value:
            raise PipelineError("E_INVALID_CONFIG", "workspace query parameter is required")
        return Path(value).resolve()

    def _body(self) -> dict[str, Any]:
        length = int(self.headers.get("Content-Length", "0"))
        if length <= 0:
            return {}
        data = self.rfile.read(length)
        try:
            parsed = json.loads(data.decode("utf-8"))
        except Exception as exc:
            raise PipelineError("E_INVALID_CONFIG", f"invalid JSON body: {exc}") from exc
        if not isinstance(parsed, dict):
            raise PipelineError("E_INVALID_CONFIG", "JSON body must be an object")
        return parsed

    def _send(self, status: int, payload: Any) -> None:
        data = json.dumps(
            payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")
        ).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _error(self, error: PipelineError) -> None:
        status = ERROR_HTTP_STATUS.get(error.code, HTTPStatus.INTERNAL_SERVER_ERROR)
        self._send(status, {"ok": False, "error": error.to_dict()})

    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        query = parse_qs(parsed.query)
        try:
            if parsed.path == "/health":
                self._send(HTTPStatus.OK, {"ok": True, "service": "storypipe"})
                return
            workspace = self._workspace(query)
            runner = PipelineRunner(workspace)
            if parsed.path == "/v1/status":
                self._send(HTTPStatus.OK, runner.inspect())
            elif parsed.path == "/v1/fingerprints":
                inspection = runner.inspect()
                self._send(
                    HTTPStatus.OK,
                    {
                        "ok": inspection.get("ok", False),
                        "overall_fingerprint": inspection.get("overall_fingerprint"),
                        "stage_fingerprints": inspection.get("stage_fingerprints"),
                        "earliest_invalid_stage": inspection.get("earliest_invalid_stage"),
                        "invalidated_stages": inspection.get("invalidated_stages"),
                        "stages": inspection.get("stages"),
                        "error": inspection.get("error"),
                    },
                )
            elif parsed.path == "/v1/cache-plan":
                inspection = runner.inspect()
                self._send(
                    HTTPStatus.OK,
                    {
                        "ok": inspection.get("ok", False),
                        "cache_plan": inspection.get("cache_plan"),
                        "error": inspection.get("error"),
                    },
                )
            elif parsed.path == "/v1/export/manifest":
                self._send(HTTPStatus.OK, {"ok": True, "manifest": runner.export_manifest()})
            elif parsed.path == "/v1/verify":
                self._send(HTTPStatus.OK, {"ok": True, "verification": validate_workspace(workspace)})
            elif parsed.path == "/v1/checkpoints":
                store = CheckpointStore(workspace)
                self._send(HTTPStatus.OK, {"ok": True, "records": store.read_all()})
            else:
                raise PipelineError("E_NOT_FOUND", f"unknown path {parsed.path}")
        except PipelineError as exc:
            self._error(exc)
        except Exception as exc:
            self._error(PipelineError("E_INTERNAL", str(exc)))

    def do_POST(self) -> None:
        parsed = urlparse(self.path)
        try:
            body = self._body()
            if parsed.path == "/v1/import":
                sample_dir = body.get("sample_dir")
                workspace = body.get("workspace")
                if not sample_dir or not workspace:
                    raise PipelineError(
                        "E_INVALID_CONFIG", "sample_dir and workspace are required"
                    )
                result = import_sample(
                    Path(sample_dir), Path(workspace), replace=bool(body.get("replace", True))
                )
                self._send(HTTPStatus.OK, {"ok": True, "import": result})
                return
            if parsed.path == "/v1/compare":
                result = compare_scales(
                    body["small_workspace"], body["large_workspace"], body.get("output")
                )
                self._send(HTTPStatus.OK, result)
                return
            workspace = body.get("workspace")
            if not workspace:
                raise PipelineError("E_INVALID_CONFIG", "workspace is required")
            runner = PipelineRunner(Path(workspace))
            if parsed.path == "/v1/stop":
                payload = runner.request_stop(str(body.get("reason", "operator request")))
                self._send(HTTPStatus.OK, {"ok": True, "stop": payload})
            elif parsed.path == "/v1/resume":
                payload = runner.resume()
                self._send(HTTPStatus.OK, {"ok": True, "resume": payload})
            elif parsed.path == "/v1/run":
                result = runner.run(
                    fresh=bool(body.get("fresh", False)),
                    stop_after_units=body.get("stop_after_units"),
                )
                self._send(HTTPStatus.OK, result)
            elif parsed.path == "/v1/checkpoints/publish":
                store = CheckpointStore(Path(workspace))
                record = store.publish(
                    stage=body["stage"],
                    input_fingerprint=body["input_fingerprint"],
                    config_version=str(body.get("config_version", "1")),
                    artifact_digest=body["artifact_digest"],
                    artifact_path=body.get("artifact_path", ""),
                    components=body.get("components", {}),
                    sequence=body.get("sequence"),
                    extra=body.get("extra"),
                )
                self._send(HTTPStatus.OK, {"ok": True, "record": record})
            elif parsed.path == "/v1/verify":
                self._send(HTTPStatus.OK, {"ok": True, "verification": validate_workspace(Path(workspace))})
            else:
                raise PipelineError("E_NOT_FOUND", f"unknown path {parsed.path}")
        except PipelineError as exc:
            self._error(exc)
        except KeyError as exc:
            self._error(PipelineError("E_INVALID_CONFIG", f"missing field {exc.args[0]}"))
        except Exception as exc:
            self._error(PipelineError("E_INTERNAL", str(exc)))


def serve(host: str = "127.0.0.1", port: int = 8765) -> None:
    server = ThreadingHTTPServer((host, port), ApiHandler)
    print(json.dumps({"ok": True, "listen": f"http://{host}:{server.server_port}"}, ensure_ascii=False))
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()

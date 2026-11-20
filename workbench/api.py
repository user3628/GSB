from __future__ import annotations

import json
import mimetypes
import os
import threading
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

from .demo import reset_outputs
from .generator import DEFAULT_SEED, generate_demo
from .importer import import_dataset
from .injection import append_injection, clear_injections
from .pipeline import (
    read_conflicts,
    read_failures,
    read_report,
    read_run_state,
    request_stop,
    run_pipeline,
    sample_detail,
    workspace_status,
)
from .store import CheckpointStore
from .util import WorkbenchError, atomic_write_bytes, pretty_json, resolve_workspace_path


class JobManager:
    def __init__(self, root: Path):
        self.root = root.resolve()
        self._lock = threading.Lock()
        self._thread: threading.Thread | None = None
        self._last_result: dict[str, Any] | None = None
        self._last_error: dict[str, Any] | None = None

    def start(
        self,
        *,
        mode: str,
        config_path: str,
        stop_after: int | None,
        clear_outputs: bool = False,
    ) -> dict[str, Any]:
        with self._lock:
            if self._thread is not None and self._thread.is_alive():
                raise WorkbenchError("E_RUN_IN_PROGRESS", "a workbench run is already active")
            if clear_outputs:
                reset_outputs(self.root)
            self._last_result = None
            self._last_error = None
            thread = threading.Thread(
                target=self._worker,
                name="workbench-pipeline",
                args=(mode, config_path, stop_after),
                daemon=True,
            )
            self._thread = thread
            thread.start()
            return {"accepted": True, "mode": mode, "stop_after": stop_after}

    def _worker(self, mode: str, config_path: str, stop_after: int | None) -> None:
        try:
            result = run_pipeline(
                self.root,
                config_path=config_path,
                mode=mode,
                clear_stop_flag=True,
                stop_after_checkpoints=stop_after,
            )
            self._last_result = result.as_dict()
        except Exception as exc:
            if isinstance(exc, WorkbenchError):
                self._last_error = exc.as_dict()
            else:
                self._last_error = {"code": "E_INTERNAL", "message": str(exc)}

    def state(self) -> dict[str, Any]:
        alive = self._thread is not None and self._thread.is_alive()
        return {
            "thread_alive": alive,
            "last_result": self._last_result,
            "last_error": self._last_error,
            "run_state": read_run_state(self.root),
        }


class WorkbenchHTTPServer(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True

    def __init__(self, address: tuple[str, int], root: Path):
        self.root = root.resolve()
        self.jobs = JobManager(self.root)
        super().__init__(address, WorkbenchHandler)


class WorkbenchHandler(BaseHTTPRequestHandler):
    server: WorkbenchHTTPServer

    def do_GET(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        query = parse_qs(parsed.query)
        try:
            if parsed.path == "/":
                self._serve_static("index.html")
            elif parsed.path == "/api/health":
                self._json({"ok": True, "service": "local-cv-validation-workbench"})
            elif parsed.path == "/api/status":
                config_path = query.get("config", ["configs/default.json"])[0]
                payload = workspace_status(self.server.root, config_path=config_path)
                payload["job"] = self.server.jobs.state()
                self._json(payload)
            elif parsed.path == "/api/stages":
                self._json({"stages": CheckpointStore(self.server.root).verify_all(), "job": self.server.jobs.state()})
            elif parsed.path == "/api/report":
                report = read_report(self.server.root)
                if report is None:
                    raise WorkbenchError("E_NOT_FOUND", "no report has been generated yet")
                self._json(report)
            elif parsed.path == "/api/report.sha256":
                path = self.server.root / "reports" / "report.sha256"
                if not path.exists():
                    raise WorkbenchError("E_NOT_FOUND", "no report digest has been generated yet")
                self._text(path.read_text(encoding="ascii").strip(), HTTPStatus.OK, "text/plain; charset=utf-8")
            elif parsed.path == "/api/failures":
                self._json({"failures": read_failures(self.server.root)})
            elif parsed.path == "/api/conflicts":
                self._json({"conflicts": read_conflicts(self.server.root)})
            elif parsed.path == "/api/invalidations":
                limit = int(query.get("limit", ["50"])[0])
                self._json({"invalidations": CheckpointStore(self.server.root).invalidations(limit)})
            elif parsed.path == "/api/samples":
                status = workspace_status(self.server.root)
                if status.get("integrity") != "ok":
                    raise WorkbenchError("E_STATUS_UNAVAILABLE", "sample status cannot be built", status.get("error"))
                self._json({"samples": status.get("samples", []), "dataset": status.get("dataset")})
            elif parsed.path == "/api/sample":
                key = query.get("key", [""])[0]
                if not key:
                    raise WorkbenchError("E_MISSING_FIELD", "sample key is required")
                self._json(sample_detail(self.server.root, key))
            elif parsed.path == "/api/sample-image":
                key = query.get("key", [""])[0]
                detail = sample_detail(self.server.root, key)
                path_value = detail["source"].get("image_path")
                if not path_value:
                    raise WorkbenchError("E_NOT_FOUND", "sample has no image path")
                image_path = (self.server.root / path_value).resolve()
                image_path.relative_to(self.server.root)
                if not image_path.exists():
                    raise WorkbenchError("E_NOT_FOUND", "sample image file does not exist")
                content_type = mimetypes.guess_type(image_path.name)[0] or "application/octet-stream"
                data = image_path.read_bytes()
                self._bytes(data, HTTPStatus.OK, content_type)
            elif parsed.path == "/api/export":
                path = self.server.root / "reports" / "report.json"
                if not path.exists():
                    raise WorkbenchError("E_NOT_FOUND", "no report has been generated yet")
                self._bytes(
                    path.read_bytes(),
                    HTTPStatus.OK,
                    "application/json; charset=utf-8",
                    extra_headers={"Content-Disposition": 'attachment; filename="report.json"'},
                )
            else:
                self._json_error(WorkbenchError("E_NOT_FOUND", "route not found", {"path": parsed.path}), HTTPStatus.NOT_FOUND)
        except WorkbenchError as exc:
            self._json_error(exc, _status_for(exc.code))
        except Exception as exc:
            self._json_error(WorkbenchError("E_INTERNAL", str(exc)), HTTPStatus.INTERNAL_SERVER_ERROR)

    def do_POST(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        try:
            body = self._read_body()
            if parsed.path in {"/api/run", "/api/resume"}:
                mode = "resume" if parsed.path == "/api/resume" else body.get("mode", "run")
                if mode not in {"run", "resume"}:
                    raise WorkbenchError("E_REQUEST_INVALID", "mode must be run or resume")
                stop_after = body.get("stop_after")
                if stop_after is not None:
                    stop_after = int(stop_after)
                    if stop_after < 0:
                        raise WorkbenchError("E_REQUEST_INVALID", "stop_after must be non-negative")
                config_path = str(body.get("config", "configs/default.json"))
                accepted = self.server.jobs.start(
                    mode=mode,
                    config_path=config_path,
                    stop_after=stop_after,
                    clear_outputs=bool(body.get("clear_outputs", False)),
                )
                self._json(accepted, HTTPStatus.ACCEPTED)
            elif parsed.path == "/api/stop":
                self._json(request_stop(self.server.root, str(body.get("reason", "http_stop_request"))), HTTPStatus.ACCEPTED)
            elif parsed.path == "/api/inject":
                operation = body.get("operation", body)
                result = append_injection(self.server.root, operation)
                self._json(result, HTTPStatus.CREATED)
            elif parsed.path == "/api/clear-injection":
                self._json(clear_injections(self.server.root))
            elif parsed.path == "/api/generate":
                seed = int(body.get("seed", DEFAULT_SEED))
                clean = bool(body.get("clean", True))
                self._json(generate_demo(self.server.root, seed=seed, clean=clean), HTTPStatus.CREATED)
            elif parsed.path == "/api/import":
                manifest = body.get("manifest")
                name = body.get("name")
                if not manifest or not name:
                    raise WorkbenchError("E_MISSING_FIELD", "import requires manifest and name")
                self._json(
                    import_dataset(
                        self.server.root,
                        str(manifest),
                        name=str(name),
                        overwrite=bool(body.get("overwrite", False)),
                    ),
                    HTTPStatus.CREATED,
                )
            elif parsed.path == "/api/export":
                source = self.server.root / "reports" / "report.json"
                if not source.exists():
                    raise WorkbenchError("E_NOT_FOUND", "no report has been generated yet")
                output = str(body.get("output", "reports/exported_report.json"))
                target = resolve_workspace_path(self.server.root, output)
                atomic_write_bytes(target, source.read_bytes())
                self._json({"output": output, "bytes": target.stat().st_size}, HTTPStatus.CREATED)
            else:
                self._json_error(WorkbenchError("E_NOT_FOUND", "route not found", {"path": parsed.path}), HTTPStatus.NOT_FOUND)
        except WorkbenchError as exc:
            self._json_error(exc, _status_for(exc.code))
        except (ValueError, TypeError) as exc:
            self._json_error(WorkbenchError("E_REQUEST_INVALID", str(exc)), HTTPStatus.BAD_REQUEST)
        except Exception as exc:
            self._json_error(WorkbenchError("E_INTERNAL", str(exc)), HTTPStatus.INTERNAL_SERVER_ERROR)

    def log_message(self, format_string: str, *args: Any) -> None:
        if os.environ.get("WORKBENCH_HTTP_LOG", "1") != "0":
            print(f"http {self.address_string()} {format_string % args}")

    def _read_body(self) -> dict[str, Any]:
        length = int(self.headers.get("Content-Length", "0"))
        if length > 2 * 1024 * 1024:
            raise WorkbenchError("E_REQUEST_INVALID", "request body is too large")
        if length == 0:
            return {}
        raw = self.rfile.read(length)
        try:
            data = json.loads(raw.decode("utf-8"))
        except Exception as exc:
            raise WorkbenchError("E_REQUEST_INVALID", "request body must be valid JSON") from exc
        if not isinstance(data, dict):
            raise WorkbenchError("E_REQUEST_INVALID", "request body must be a JSON object")
        return data

    def _serve_static(self, name: str) -> None:
        path = (Path(__file__).parent / "static" / name).resolve()
        if path.parent != (Path(__file__).parent / "static").resolve() or not path.exists():
            raise WorkbenchError("E_NOT_FOUND", "static asset not found")
        self._bytes(path.read_bytes(), HTTPStatus.OK, "text/html; charset=utf-8")

    def _json(self, payload: Any, status: HTTPStatus = HTTPStatus.OK) -> None:
        self._text(pretty_json(payload), status, "application/json; charset=utf-8")

    def _json_error(self, exc: WorkbenchError, status: HTTPStatus) -> None:
        self._json({"ok": False, "error": exc.as_dict()}, status)

    def _text(self, text: str, status: HTTPStatus, content_type: str) -> None:
        self._bytes(text.encode("utf-8"), status, content_type)

    def _bytes(
        self,
        data: bytes,
        status: HTTPStatus,
        content_type: str,
        *,
        extra_headers: dict[str, str] | None = None,
    ) -> None:
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Cache-Control", "no-store")
        for key, value in (extra_headers or {}).items():
            self.send_header(key, value)
        self.end_headers()
        self.wfile.write(data)


def _status_for(code: str) -> HTTPStatus:
    if code == "E_NOT_FOUND":
        return HTTPStatus.NOT_FOUND
    if code in {"E_RUN_IN_PROGRESS", "E_STALE_CHECKPOINT", "E_IMPORT_EXISTS"}:
        return HTTPStatus.CONFLICT
    return HTTPStatus.BAD_REQUEST


def serve(root: Path, *, host: str = "127.0.0.1", port: int = 8765) -> None:
    server = WorkbenchHTTPServer((host, port), root)
    print(f"workbench HTTP listening on http://{host}:{port} (root={server.root})")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


__all__ = ["JobManager", "WorkbenchHTTPServer", "serve"]

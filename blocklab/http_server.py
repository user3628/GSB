"""Local-only threaded HTTP API and static application server."""

from __future__ import annotations

import json
import mimetypes
import re
import threading
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

from . import __version__
from .engine import ExecutionEngine
from .errors import (
    E_BUSY,
    E_CONFIG,
    E_INPUT,
    E_INVALID_STAGE,
    E_NOT_FOUND,
    E_NUMERIC,
    E_PATH,
    E_SHAPE_MISMATCH,
    E_STALE_CHECKPOINT,
    BusinessError,
)
from .paths import STAGES, JOB_ID_RE
from .util import atomic_write_bytes, canonical_json, digest_bytes, read_json
from .workbench import Workbench

STATIC_DIR = Path(__file__).resolve().parent / "static"
JOB_ROUTE = re.compile(r"^/api/jobs/([^/]+)/([A-Za-z0-9_-]+)$")


ERROR_STATUS = {
    E_NOT_FOUND: HTTPStatus.NOT_FOUND,
    E_INPUT: HTTPStatus.BAD_REQUEST,
    E_CONFIG: HTTPStatus.BAD_REQUEST,
    E_PATH: HTTPStatus.BAD_REQUEST,
    E_SHAPE_MISMATCH: HTTPStatus.UNPROCESSABLE_ENTITY,
    E_NUMERIC: HTTPStatus.UNPROCESSABLE_ENTITY,
    E_STALE_CHECKPOINT: HTTPStatus.CONFLICT,
    E_INVALID_STAGE: HTTPStatus.CONFLICT,
    E_BUSY: HTTPStatus.CONFLICT,
}


class LocalApplication:
    def __init__(self, root: Path, workdir: str) -> None:
        self.root = root.resolve()
        self.wb = Workbench(self.root, workdir)
        self.engine = ExecutionEngine(self.wb)
        self.running: dict[str, threading.Thread] = {}
        self.errors: dict[str, dict[str, Any]] = {}
        self._lock = threading.Lock()

    def start_run(self, job_id: str, options: dict[str, Any]) -> dict[str, Any]:
        with self._lock:
            thread = self.running.get(job_id)
            if thread is not None and thread.is_alive():
                raise BusinessError(E_BUSY, "job already running", job_id=job_id)

            def worker() -> None:
                try:
                    self.engine.run_job(
                        job_id,
                        stages=options.get("stages"),
                        stop_after_stage=options.get("stop_after_stage"),
                        stop_after_blocks=options.get("stop_after_blocks"),
                        block_delay_ms=int(options.get("block_delay_ms", 0) or 0),
                        clear_stop=bool(options.get("clear_stop", True)),
                    )
                    self.errors.pop(job_id, None)
                except BusinessError as error:
                    self.errors[job_id] = error.to_dict()
                except Exception as error:  # pragma: no cover - defensive API boundary
                    self.errors[job_id] = {
                        "ok": False,
                        "error": {"code": "E_INTERNAL", "message": str(error), "details": {}},
                    }
                finally:
                    with self._lock:
                        self.running.pop(job_id, None)

            thread = threading.Thread(target=worker, name=f"blocklab-{job_id}", daemon=True)
            self.running[job_id] = thread
            thread.start()
        return {"ok": True, "job_id": job_id, "status": "running", "background": True}

    def cleanup(self) -> None:
        with self._lock:
            threads = list(self.running.values())
        for thread in threads:
            thread.join(timeout=2.0)


class Handler(BaseHTTPRequestHandler):
    server_version = f"BlockLab/{__version__}"

    @property
    def app(self) -> LocalApplication:
        return self.server.app  # type: ignore[attr-defined]

    def log_message(self, fmt: str, *args: Any) -> None:
        print(f"[http] {self.address_string()} {fmt % args}")

    def _send_json(self, status: int, value: Any) -> None:
        data = (json.dumps(value, ensure_ascii=False, sort_keys=True, allow_nan=False) + "\n").encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.end_headers()
        self.wfile.write(data)

    def _send_error(self, error: BusinessError) -> None:
        self._send_json(ERROR_STATUS.get(error.code, HTTPStatus.BAD_REQUEST), error.to_dict())

    def _read_json(self) -> dict[str, Any]:
        length = int(self.headers.get("Content-Length", "0") or 0)
        if length > 2 * 1024 * 1024:
            raise BusinessError(E_CONFIG, "request body is too large")
        raw = self.rfile.read(length) if length else b"{}"
        try:
            value = json.loads(raw.decode("utf-8"))
        except Exception as exc:
            raise BusinessError(E_CONFIG, f"invalid JSON body: {exc}") from exc
        if not isinstance(value, dict):
            raise BusinessError(E_CONFIG, "JSON body must be an object")
        return value

    def do_GET(self) -> None:  # noqa: N802
        try:
            parsed = urlparse(self.path)
            path = parsed.path
            query = parse_qs(parsed.query)
            if path in ("/", "/index.html"):
                self._send_static("index.html")
                return
            if path.startswith("/static/"):
                self._send_static(path.removeprefix("/static/"))
                return
            if path == "/api/health":
                self._send_json(
                    HTTPStatus.OK,
                    {"ok": True, "service": "blocklab", "version": __version__, "local_only": True},
                )
                return
            if path == "/api/samples":
                self._send_json(HTTPStatus.OK, self._samples())
                return
            if path == "/api/jobs":
                jobs = []
                for item in self.app.wb.list_jobs():
                    try:
                        status = self.app.wb.job_status(item["job_id"])
                        jobs.append(
                            {
                                **item,
                                "complete": status["complete"],
                                "alerts": status["alerts"],
                                "version_fingerprint": status["version_fingerprint"],
                            }
                        )
                    except BusinessError as error:
                        jobs.append({**item, "error": error.to_dict()["error"]})
                self._send_json(HTTPStatus.OK, {"ok": True, "jobs": jobs})
                return
            match = JOB_ROUTE.match(path)
            if match:
                job_id, action = match.groups()
                self._get_job_action(job_id, action, query)
                return
            raise BusinessError(E_NOT_FOUND, "route not found", path=path)
        except BusinessError as error:
            self._send_error(error)
        except Exception as exc:  # pragma: no cover - defensive API boundary
            self._send_json(
                HTTPStatus.INTERNAL_SERVER_ERROR,
                {"ok": False, "error": {"code": "E_INTERNAL", "message": str(exc), "details": {}}},
            )

    def _get_job_action(self, job_id: str, action: str, query: dict[str, list[str]]) -> None:
        if not JOB_ID_RE.fullmatch(job_id):
            raise BusinessError(E_PATH, "invalid job id", job_id=job_id)
        if action == "status":
            self._send_json(HTTPStatus.OK, self.app.wb.job_status(job_id))
            return
        if action == "fingerprints":
            self._send_json(HTTPStatus.OK, self.app.wb.fingerprints(job_id))
            return
        if action == "diagnostics":
            stage = query.get("stage", [None])[0]
            self._send_json(HTTPStatus.OK, self.app.wb.diagnostics(job_id, stage=stage))
            return
        if action == "invalidations":
            self._send_json(HTTPStatus.OK, self.app.wb.invalidations(job_id))
            return
        if action == "blocks":
            stage = query.get("stage", [None])[0]
            if stage is not None and stage not in STAGES:
                raise BusinessError(E_INVALID_STAGE, "unknown stage", requested_stage=stage)
            bundle = self.app.wb.scan_all_stages(job_id)
            blocks = []
            for current_stage in STAGES:
                if stage is not None and current_stage != stage:
                    continue
                scan = bundle["scans"][current_stage]
                for record in scan["records"]:
                    blocks.append(
                        {
                            "stage": current_stage,
                            "sequence": record["sequence"],
                            "epoch": record["epoch"],
                            "block": record["block"],
                            "status": record["status"],
                            "product_digest": record["product_digest"],
                            "artifact": record["artifact"],
                            "diagnostics": record.get("metadata", {}).get("diagnostics"),
                            "metadata": record.get("metadata", {}),
                        }
                    )
            self._send_json(HTTPStatus.OK, {"ok": True, "job_id": job_id, "blocks": blocks})
            return
        if action == "export":
            self.app.engine.ensure_outputs(job_id)
            paths = self.app.wb.paths.job(job_id)
            summary_path = paths.output_path("summary.json")
            if not summary_path.exists():
                raise BusinessError(
                    E_INVALID_STAGE,
                    "summary is unavailable until all stages complete",
                    requested_stage="reduce",
                )
            data = summary_path.read_bytes()
            destination = self.app.wb.paths.exports_dir / f"{job_id}-summary.json"
            atomic_write_bytes(destination, data)
            if query.get("download", ["0"])[0] in ("1", "true"):
                self.send_response(HTTPStatus.OK)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Content-Disposition", f'attachment; filename="{job_id}-summary.json"')
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)
                return
            self._send_json(
                HTTPStatus.OK,
                {
                    "ok": True,
                    "job_id": job_id,
                    "saved_path": destination.relative_to(self.app.root).as_posix(),
                    "sha256": digest_bytes(data),
                    "summary": json.loads(data.decode("utf-8")),
                },
            )
            return
        raise BusinessError(E_NOT_FOUND, "job route not found", job_id=job_id, action=action)

    def do_POST(self) -> None:  # noqa: N802
        try:
            parsed = urlparse(self.path)
            body = self._read_json()
            if parsed.path == "/api/import":
                self._post_import(body)
                return
            match = JOB_ROUTE.match(parsed.path)
            if not match:
                raise BusinessError(E_NOT_FOUND, "route not found", path=parsed.path)
            job_id, action = match.groups()
            if not JOB_ID_RE.fullmatch(job_id):
                raise BusinessError(E_PATH, "invalid job id", job_id=job_id)
            if action in ("run", "resume"):
                stage = body.get("stage", "all")
                stages = list(STAGES) if stage in (None, "all") else [stage]
                if any(item not in STAGES for item in stages):
                    raise BusinessError(E_INVALID_STAGE, "unknown stage", requested_stage=stage)
                options = {
                    "stages": stages,
                    "stop_after_stage": body.get("stop_after_stage"),
                    "stop_after_blocks": body.get("stop_after_blocks"),
                    "block_delay_ms": body.get("block_delay_ms", 0),
                    "clear_stop": True,
                }
                if action == "resume":
                    self.app.wb.clear_stop(job_id)
                if bool(body.get("background", True)):
                    result = self.app.start_run(job_id, options)
                    result["running"] = self.app.running.get(job_id) is not None
                    self._send_json(HTTPStatus.ACCEPTED, result)
                    return
                result = self.app.engine.run_job(job_id, **options)
                self._send_json(HTTPStatus.OK, result)
                return
            if action == "stop":
                self._send_json(HTTPStatus.OK, self.app.wb.request_stop(job_id))
                return
            raise BusinessError(E_NOT_FOUND, "job route not found", job_id=job_id, action=action)
        except BusinessError as error:
            self._send_error(error)
        except Exception as exc:  # pragma: no cover - defensive API boundary
            self._send_json(
                HTTPStatus.INTERNAL_SERVER_ERROR,
                {"ok": False, "error": {"code": "E_INTERNAL", "message": str(exc), "details": {}}},
            )

    def _post_import(self, body: dict[str, Any]) -> None:
        sample_id = body.get("sample_id")
        shard = body.get("shard_path")
        config = body.get("config_path")
        if sample_id:
            samples = self._samples()
            entry = next((item for item in samples["shards"] if item["id"] == sample_id), None)
            if entry is None:
                raise BusinessError(E_NOT_FOUND, "sample not found", sample_id=sample_id)
            shard = entry["path"]
            config = config or entry["config"]
        if not shard or not config:
            raise BusinessError(E_CONFIG, "shard_path and config_path are required")
        value = self.app.wb.import_job(
            source_path=shard,
            config_path=config,
            job_id=body.get("job_id"),
            manifest_path=body.get("manifest_path"),
            shard_id=body.get("shard_id") or sample_id,
        )
        self._send_json(HTTPStatus.OK, value)

    def _samples(self) -> dict[str, Any]:
        manifest_path = self.app.root / "samples" / "shape_manifest.json"
        manifest = read_json(manifest_path) if manifest_path.exists() else {"shards": []}
        configs = sorted(
            path.relative_to(self.app.root).as_posix()
            for path in (self.app.root / "configs").glob("*.yaml")
        )
        return {"ok": True, "manifest": "samples/shape_manifest.json", "shards": manifest.get("shards", []), "configs": configs}

    def _send_static(self, relative: str) -> None:
        candidate = (STATIC_DIR / relative).resolve()
        try:
            candidate.relative_to(STATIC_DIR.resolve())
        except ValueError as exc:
            raise BusinessError(E_PATH, "static path escapes application directory") from exc
        if not candidate.is_file():
            raise BusinessError(E_NOT_FOUND, "static asset not found", path=relative)
        data = candidate.read_bytes()
        mime, _encoding = mimetypes.guess_type(candidate.name)
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", mime or "application/octet-stream")
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Cache-Control", "no-cache" if candidate.name == "index.html" else "public, max-age=300")
        self.end_headers()
        self.wfile.write(data)


class LocalHTTPServer(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True

    def __init__(self, address: tuple[str, int], app: LocalApplication) -> None:
        self.app = app
        super().__init__(address, Handler)


def run_server(*, root: Path, workdir: str, host: str, port: int) -> None:
    if host not in ("127.0.0.1", "localhost"):
        raise BusinessError(E_CONFIG, "HTTP server must bind to a loopback address", host=host)
    app = LocalApplication(root, workdir)
    server = LocalHTTPServer((host, port), app)
    print(f"BlockLab listening on http://{host}:{server.server_port}/ (workdir={workdir})")
    try:
        server.serve_forever(poll_interval=0.25)
    except KeyboardInterrupt:
        print("\nBlockLab stopping")
    finally:
        app.cleanup()
        server.server_close()


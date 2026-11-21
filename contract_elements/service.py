from __future__ import annotations

import json
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import unquote, urlparse

from .canonical import atomic_write_bytes, canonical_json, read_json
from .config import config_fingerprint
from .errors import BusinessError
from .interop import V1Interop, load_v1
from .normalization import ClauseNormalizer
from .pipeline import BatchEngine, DatasetPaths


class ContractHTTPServer(ThreadingHTTPServer):
    daemon_threads = True

    def __init__(
        self,
        server_address: tuple[str, int],
        config: dict[str, Any],
        paths: DatasetPaths,
        injection_path: str | Path | None = None,
    ) -> None:
        self.config = config
        self.paths = paths
        self.injection_path = injection_path
        super().__init__(server_address, ContractRequestHandler)


class ContractRequestHandler(BaseHTTPRequestHandler):
    server: ContractHTTPServer
    server_version = "ContractElements/1.0"

    def _send(self, status: int, value: Any) -> None:
        body = (canonical_json(value) + "\n").encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _engine(self) -> BatchEngine:
        return BatchEngine(self.server.config, self.server.paths, injection_path=self.server.injection_path)

    def _read_body(self) -> dict[str, Any]:
        length = int(self.headers.get("Content-Length", "0") or "0")
        if length == 0:
            return {}
        raw = self.rfile.read(length)
        if not raw.strip():
            return {}
        value = json.loads(raw.decode("utf-8"))
        if not isinstance(value, dict):
            raise ValueError("request body must be a JSON object")
        return value

    def _version_payload(self) -> dict[str, Any]:
        state_path = self.server.paths.runtime_dir / "version_state.json"
        progress_path = self.server.paths.runtime_dir / "progress.json"
        return {
            "config_fingerprint": config_fingerprint(self.server.config),
            "runtime": str(self.server.paths.runtime_dir),
            "version_state": read_json(state_path) if state_path.exists() else None,
            "progress": read_json(progress_path) if progress_path.exists() else None,
        }

    def do_GET(self) -> None:
        try:
            path = urlparse(self.path).path
            if path == "/health":
                self._send(HTTPStatus.OK, {"status": "ok", "service": "contract-elements"})
                return
            if path == "/v1/version":
                self._send(HTTPStatus.OK, self._version_payload())
                return
            if path.startswith("/v1/contracts/"):
                remainder = unquote(path[len("/v1/contracts/"):])
                view = "all"
                for suffix in ("/candidates", "/conflicts"):
                    if remainder.endswith(suffix):
                        view = suffix[1:]
                        remainder = remainder[: -len(suffix)]
                        break
                result = self._engine().parse_contract(remainder)
                if view == "candidates":
                    self._send(HTTPStatus.OK, {"contract_id": remainder, "candidates": result["candidates"], "errors": result["errors"]})
                elif view == "conflicts":
                    self._send(HTTPStatus.OK, {"contract_id": remainder, "conflicts": result["conflicts"], "resolved_elements": result["resolved_elements"], "errors": result["errors"]})
                else:
                    self._send(HTTPStatus.OK, result)
                return
            self._send(HTTPStatus.NOT_FOUND, {"error": {"code": "E_INTEROP", "message": "not found", "path": path}})
        except BusinessError as error:
            status = HTTPStatus.NOT_FOUND if error.code == "E_INTEROP" else HTTPStatus.BAD_REQUEST
            self._send(status, {"status": "error", "error": error.to_dict()})
        except Exception as error:
            self._send(HTTPStatus.INTERNAL_SERVER_ERROR, {"status": "error", "error": {"code": "E_INTEROP", "message": str(error)}})

    def do_POST(self) -> None:
        try:
            path = urlparse(self.path).path
            body = self._read_body()
            if path == "/v1/stop":
                self.server.paths.runtime_dir.mkdir(parents=True, exist_ok=True)
                atomic_write_bytes(self.server.paths.runtime_dir / "STOP", b"stop\n")
                self._send(HTTPStatus.OK, {"status": "stop_requested", "runtime": str(self.server.paths.runtime_dir)})
                return
            if path in {"/v1/experiments", "/v1/resume"}:
                engine = self._engine()
                if path == "/v1/experiments":
                    stop_after_units = body.get("stop_after_units")
                    if stop_after_units is not None:
                        stop_after_units = int(stop_after_units)
                    result = engine.run(
                        stop_after_units=stop_after_units,
                        clear_stop=False,
                    )
                else:
                    result = engine.run(resume=True, clear_stop=True)
                self._send(HTTPStatus.OK, result)
                return
            if path == "/v1/interop/import":
                interop = V1Interop(self.server.config, ClauseNormalizer(self.server.config))
                rows = load_v1(body.get("source", self.server.paths.v1_path))
                structure = interop.import_rows(rows)
                compatibility = interop.compatibility(rows)
                self._send(HTTPStatus.OK, {"status": "ok", "compatibility": compatibility, "structure": structure})
                return
            self._send(HTTPStatus.NOT_FOUND, {"error": {"code": "E_INTEROP", "message": "not found", "path": path}})
        except BusinessError as error:
            self._send(HTTPStatus.BAD_REQUEST, {"status": "error", "error": error.to_dict()})
        except (ValueError, OSError, KeyError, json.JSONDecodeError) as error:
            self._send(HTTPStatus.BAD_REQUEST, {"status": "error", "error": {"code": "E_INTEROP", "message": str(error)}})
        except Exception as error:
            self._send(HTTPStatus.INTERNAL_SERVER_ERROR, {"status": "error", "error": {"code": "E_INTEROP", "message": str(error)}})

    def log_message(self, format: str, *args: Any) -> None:
        super().log_message(format, *args)


def run_server(
    config: dict[str, Any],
    paths: DatasetPaths,
    *,
    host: str | None = None,
    port: int | None = None,
    injection_path: str | Path | None = None,
) -> None:
    server = ContractHTTPServer(
        (host or config["service"]["host"], int(port or config["service"]["port"])),
        config,
        paths,
        injection_path,
    )
    try:
        server.serve_forever()
    finally:
        server.server_close()


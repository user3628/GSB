from __future__ import annotations

import argparse
import json
import os
import signal
import sys
import threading
import time
import traceback
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import unquote, urlparse

from .errors import BusinessError
from .service import RepairService


MAX_REQUEST_BODY = 4 * 1024 * 1024
LOOPBACK_HOSTS = {"127.0.0.1", "::1", "localhost"}


def clock_from_environment() -> Any:
    raw = os.environ.get("REPAIR_CLOCK_BASE_ISO")
    if not raw:
        return None
    text = raw[:-1] + "+00:00" if raw.endswith("Z") else raw
    base = datetime.fromisoformat(text)
    if base.tzinfo is None:
        raise ValueError("REPAIR_CLOCK_BASE_ISO 必须包含时区")
    base = base.astimezone(timezone.utc)
    started = time.monotonic()

    def logical_now() -> datetime:
        return base + timedelta(seconds=time.monotonic() - started)

    return logical_now


class RepairHttpHandler(BaseHTTPRequestHandler):
    service: RepairService
    server_version = "ReplicaRepair/1.0"

    def log_message(self, format: str, *args: Any) -> None:
        print(
            f"{self.address_string()} [{self.log_date_time_string()}] "
            + format % args,
            file=sys.stderr,
            flush=True,
        )

    def _send(self, status: int, payload: dict[str, Any]) -> None:
        body = json.dumps(
            payload,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def _read_json(self) -> dict[str, Any]:
        raw_length = self.headers.get("Content-Length")
        if raw_length is None:
            raise BusinessError(
                "E_REQUEST_INVALID",
                "请求缺少 Content-Length",
                http_status=400,
            )
        try:
            length = int(raw_length)
        except ValueError as exc:
            raise BusinessError(
                "E_REQUEST_INVALID",
                "Content-Length 非法",
                http_status=400,
            ) from exc
        if length <= 0 or length > MAX_REQUEST_BODY:
            raise BusinessError(
                "E_REQUEST_INVALID",
                "请求体长度非法",
                http_status=413,
                details={"length": length, "maximum": MAX_REQUEST_BODY},
            )
        raw = self.rfile.read(length)
        try:
            payload = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise BusinessError(
                "E_REQUEST_INVALID",
                "请求体不是合法 UTF-8 JSON",
                http_status=400,
            ) from exc
        if not isinstance(payload, dict):
            raise BusinessError(
                "E_REQUEST_INVALID",
                "JSON 请求体必须是对象",
                http_status=400,
            )
        return payload

    def _dispatch(self, method: str) -> None:
        try:
            parsed = urlparse(self.path)
            path = unquote(parsed.path)
            if method == "GET":
                self._dispatch_get(path)
            else:
                self._dispatch_post(path)
        except BusinessError as exc:
            self._send(exc.http_status, exc.as_dict())
        except BrokenPipeError:
            return
        except Exception as exc:
            traceback.print_exc(file=sys.stderr)
            self._send(
                500,
                {
                    "ok": False,
                    "code": "E_INTERNAL",
                    "message": f"内部异常: {type(exc).__name__}: {exc}",
                },
            )

    def _dispatch_get(self, path: str) -> None:
        if path == "/health":
            self._send(200, self.service.health())
            return
        if path == "/v1/recovery/report/latest":
            self._send(200, self.service.latest_recovery_report())
            return
        if path.startswith("/v1/groups/") and path.count("/") == 3:
            group_id = path.rsplit("/", 1)[1]
            self._send(200, self.service.get_group(group_id))
            return
        if path.startswith("/v1/rounds/") and path.endswith("/conclusion"):
            round_id = path[len("/v1/rounds/") : -len("/conclusion")].strip("/")
            self._send(200, self.service.get_conclusion(round_id))
            return
        if path.startswith("/v1/rounds/") and path.count("/") == 3:
            round_id = path.rsplit("/", 1)[1]
            self._send(200, self.service.get_round_status(round_id))
            return
        raise BusinessError(
            "E_ROUTE_NOT_FOUND",
            f"未知 GET 路由: {path}",
            http_status=404,
        )

    def _dispatch_post(self, path: str) -> None:
        if path == "/v1/groups":
            self._send(200, self.service.create_group(self._read_json()))
            return
        if path == "/v1/rounds":
            self._send(200, self.service.create_round(self._read_json()))
            return
        if path.startswith("/v1/rounds/") and path.endswith("/reports"):
            round_id = path[len("/v1/rounds/") : -len("/reports")].strip("/")
            result = self.service.submit_reports(round_id, self._read_json())
            status = int(result.pop("http_status", 200))
            self._send(status, result)
            return
        if path.startswith("/v1/rounds/") and path.endswith("/aggregate"):
            round_id = path[len("/v1/rounds/") : -len("/aggregate")].strip("/")
            self._send(200, self.service.aggregate(round_id, self._read_json()))
            return
        raise BusinessError(
            "E_ROUTE_NOT_FOUND",
            f"未知 POST 路由: {path}",
            http_status=404,
        )

    def do_GET(self) -> None:  # noqa: N802 - BaseHTTPRequestHandler API
        self._dispatch("GET")

    def do_POST(self) -> None:  # noqa: N802 - BaseHTTPRequestHandler API
        self._dispatch("POST")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Local deterministic replica-repair HTTP service"
    )
    parser.add_argument("--db", default="data/repair.sqlite3")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=18765)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.host not in LOOPBACK_HOSTS:
        print(
            "拒绝绑定非回环地址；该示例只允许本地 HTTP 服务",
            file=sys.stderr,
        )
        return 2
    delay_ms = int(os.environ.get("REPAIR_AGGREGATION_DELAY_MS", "0"))
    service_kwargs: dict[str, Any] = {
        "aggregation_delay_ms": delay_ms,
        "auto_recover": True,
    }
    logical_clock = clock_from_environment()
    if logical_clock is not None:
        service_kwargs["now_fn"] = logical_clock
    service = RepairService(Path(args.db), **service_kwargs)
    RepairHttpHandler.service = service
    server = ThreadingHTTPServer((args.host, args.port), RepairHttpHandler)
    server.daemon_threads = True

    def stop(_signum: int, _frame: Any) -> None:
        # shutdown() waits for serve_forever(), so it must run off the main thread.
        threading.Thread(target=server.shutdown, daemon=True).start()

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    startup = {
        "service": "replica-repair-http",
        "host": args.host,
        "port": args.port,
        "db_path": str(Path(args.db)),
        "aggregation_delay_ms": delay_ms,
        "logical_clock_base": os.environ.get("REPAIR_CLOCK_BASE_ISO"),
        "startup_recovery": (
            service.startup_recovery_report
            if service.startup_recovery_report is not None
            else {"incomplete_intent_count": 0}
        ),
    }
    print(json.dumps(startup, ensure_ascii=False, sort_keys=True), flush=True)
    try:
        server.serve_forever(poll_interval=0.1)
    finally:
        server.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


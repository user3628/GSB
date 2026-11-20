"""Async local proxy server, admin API, and self-contained dashboard."""

from __future__ import annotations

import asyncio
import html
import json
import re
from dataclasses import dataclass
from http import HTTPStatus
from typing import Any, Iterable
from urllib.parse import urlsplit

from .errors import (
    E_CONFIG_CHANGED,
    E_CONFIG_INVALID,
    E_REQUEST_INVALID,
    E_REQUEST_TOO_LARGE,
    E_ROUTE_NOT_FOUND,
    E_SHUTTING_DOWN,
    E_UNSUPPORTED_TARGET,
    ProxyError,
)
from .models import is_loopback_host
from .runtime import Runtime
from .transport import filter_hop_by_hop


MAX_REQUEST_BODY = 16 * 1024 * 1024
MAX_REQUEST_HEADER = 128 * 1024
READ_HEAD_TIMEOUT = 15.0
READ_BODY_TIMEOUT = 30.0
_HEADER_NAME = re.compile(r"^[!#$%&'*+\-.^_`|~0-9A-Za-z]+$")
_METHOD_NAME = re.compile(r"^[A-Za-z][A-Za-z0-9*_-]{0,31}$")


@dataclass(frozen=True)
class RequestHead:
    method: str
    target: str
    http_version: str
    headers: tuple[tuple[str, str], ...]

    def header(self, name: str) -> str | None:
        lower = name.lower()
        for key, value in self.headers:
            if key.lower() == lower:
                return value
        return None

    def header_values(self, name: str) -> list[str]:
        lower = name.lower()
        return [value for key, value in self.headers if key.lower() == lower]


def _target_path_query(target: str) -> str:
    if target.lower().startswith(("http://", "https://")):
        parts = urlsplit(target)
        return (parts.path or "/") + (f"?{parts.query}" if parts.query else "")
    return target


def _request_path(target: str) -> str:
    if target.lower().startswith(("http://", "https://")):
        return urlsplit(target).path or "/"
    return urlsplit(target).path or "/"


async def _read_head(reader: asyncio.StreamReader) -> RequestHead:
    try:
        raw = await asyncio.wait_for(reader.readuntil(b"\r\n\r\n"), timeout=READ_HEAD_TIMEOUT)
    except asyncio.TimeoutError as exc:
        raise ProxyError(E_REQUEST_INVALID, "request header read timed out", 408) from exc
    except asyncio.IncompleteReadError as exc:
        raise ProxyError(E_REQUEST_INVALID, "client closed before request headers completed", 400) from exc
    except asyncio.LimitOverrunError as exc:
        raise ProxyError(E_REQUEST_TOO_LARGE, "request headers exceed limit", 431) from exc
    if len(raw) > MAX_REQUEST_HEADER:
        raise ProxyError(E_REQUEST_TOO_LARGE, "request headers exceed limit", 431)
    try:
        lines = raw.decode("latin-1").split("\r\n")
    except UnicodeDecodeError as exc:  # latin-1 cannot normally fail
        raise ProxyError(E_REQUEST_INVALID, "request headers are not latin-1", 400) from exc
    request_line = lines[0]
    try:
        method, target, http_version = request_line.split(" ", 2)
    except ValueError as exc:
        raise ProxyError(E_REQUEST_INVALID, "malformed HTTP request line", 400) from exc
    if not _METHOD_NAME.match(method) or not target or not http_version.startswith("HTTP/1."):
        raise ProxyError(E_REQUEST_INVALID, "unsupported HTTP request line", 400, {"request_line": request_line[:128]})
    headers: list[tuple[str, str]] = []
    for line in lines[1:]:
        if not line:
            continue
        if line.startswith((" ", "\t")):
            raise ProxyError(E_REQUEST_INVALID, "obsolete header folding is not accepted", 400)
        name, separator, value = line.partition(":")
        if not separator or not _HEADER_NAME.match(name):
            raise ProxyError(E_REQUEST_INVALID, "malformed request header", 400)
        headers.append((name, value.strip()))
    return RequestHead(
        method=method.upper(),
        target=target,
        http_version=http_version,
        headers=tuple(headers),
    )


async def _read_chunked_request(reader: asyncio.StreamReader) -> bytes:
    chunks: list[bytes] = []
    total = 0
    while True:
        try:
            line = await asyncio.wait_for(reader.readline(), timeout=READ_BODY_TIMEOUT)
        except asyncio.TimeoutError as exc:
            raise ProxyError(E_REQUEST_INVALID, "request body read timed out", 408) from exc
        if not line.endswith(b"\r\n"):
            raise ProxyError(E_REQUEST_INVALID, "malformed chunked request body", 400)
        try:
            size = int(line[:-2].split(b";", 1)[0].strip(), 16)
        except ValueError as exc:
            raise ProxyError(E_REQUEST_INVALID, "invalid chunk size", 400) from exc
        if size == 0:
            while True:
                try:
                    trailer = await asyncio.wait_for(reader.readline(), timeout=READ_BODY_TIMEOUT)
                except asyncio.TimeoutError as exc:
                    raise ProxyError(E_REQUEST_INVALID, "request trailer read timed out", 408) from exc
                if trailer in (b"\r\n", b"\n", b""):
                    break
            return b"".join(chunks)
        total += size
        if total > MAX_REQUEST_BODY:
            raise ProxyError(E_REQUEST_TOO_LARGE, "request body exceeds limit", 413)
        try:
            chunks.append(await asyncio.wait_for(reader.readexactly(size), timeout=READ_BODY_TIMEOUT))
            ending = await asyncio.wait_for(reader.readexactly(2), timeout=READ_BODY_TIMEOUT)
        except asyncio.IncompleteReadError as exc:
            raise ProxyError(E_REQUEST_INVALID, "chunked request body ended early", 400) from exc
        except asyncio.TimeoutError as exc:
            raise ProxyError(E_REQUEST_INVALID, "request body read timed out", 408) from exc
        if ending != b"\r\n":
            raise ProxyError(E_REQUEST_INVALID, "malformed chunk terminator", 400)


async def _read_body(reader: asyncio.StreamReader, head: RequestHead, writer: asyncio.StreamWriter) -> bytes:
    transfer_encodings = [value.lower() for value in head.header_values("transfer-encoding")]
    content_lengths = head.header_values("content-length")
    if transfer_encodings and content_lengths:
        raise ProxyError(E_REQUEST_INVALID, "both Transfer-Encoding and Content-Length are not allowed", 400)
    if transfer_encodings:
        if len(transfer_encodings) != 1 or transfer_encodings[0].strip() != "chunked":
            raise ProxyError(E_REQUEST_INVALID, "only chunked request transfer encoding is supported", 400)
        if head.header("expect") and head.header("expect").lower() == "100-continue":
            writer.write(b"HTTP/1.1 100 Continue\r\n\r\n")
            await writer.drain()
        return await _read_chunked_request(reader)
    if not content_lengths:
        return b""
    if len(set(content_lengths)) != 1:
        raise ProxyError(E_REQUEST_INVALID, "conflicting Content-Length headers", 400)
    try:
        length = int(content_lengths[0])
    except ValueError as exc:
        raise ProxyError(E_REQUEST_INVALID, "invalid Content-Length", 400) from exc
    if length < 0 or length > MAX_REQUEST_BODY:
        raise ProxyError(E_REQUEST_TOO_LARGE, "request body exceeds limit", 413)
    if head.header("expect") and head.header("expect").lower() == "100-continue":
        writer.write(b"HTTP/1.1 100 Continue\r\n\r\n")
        await writer.drain()
    if length == 0:
        return b""
    try:
        return await asyncio.wait_for(reader.readexactly(length), timeout=READ_BODY_TIMEOUT)
    except asyncio.IncompleteReadError as exc:
        raise ProxyError(E_REQUEST_INVALID, "request body ended early", 400) from exc
    except asyncio.TimeoutError as exc:
        raise ProxyError(E_REQUEST_INVALID, "request body read timed out", 408) from exc


def _reason(status: int) -> str:
    try:
        return HTTPStatus(status).phrase
    except ValueError:
        return "Unknown"


async def _write_response(
    writer: asyncio.StreamWriter,
    *,
    status: int,
    headers: Iterable[tuple[str, str]],
    body: bytes,
    extra_headers: Iterable[tuple[str, str]] = (),
    omit_body: bool = False,
) -> None:
    filtered = filter_hop_by_hop(headers, extra_remove={"content-length"})
    response_headers = list(filtered)
    response_headers.append(("Content-Length", str(len(body))))
    response_headers.append(("Connection", "close"))
    response_headers.extend(extra_headers)
    lines = [f"HTTP/1.1 {status} {_reason(status)}"]
    lines.extend(f"{name}: {value}" for name, value in response_headers)
    writer.write(("\r\n".join(lines) + "\r\n\r\n").encode("latin-1") + (b"" if omit_body else body))
    await writer.drain()


async def _write_json(
    writer: asyncio.StreamWriter,
    *,
    status: int,
    payload: dict[str, Any],
    extra_headers: Iterable[tuple[str, str]] = (),
) -> None:
    body = json.dumps(payload, ensure_ascii=False, sort_keys=True).encode("utf-8")
    await _write_response(
        writer,
        status=status,
        headers=(("Content-Type", "application/json; charset=utf-8"),),
        body=body,
        extra_headers=extra_headers,
    )


def _error_payload(error: ProxyError, version: int | None) -> dict[str, Any]:
    payload = error.payload()
    payload["config_version"] = version if version is not None else error.details.get("config_version")
    return payload


def _artifact_badge(artifact: dict[str, Any]) -> str:
    status = html.escape(str(artifact.get("status", "unknown")))
    built = html.escape(str(artifact.get("built_version", "?")))
    digest = html.escape(str(artifact.get("content_hash", ""))[:12])
    return f'<span class="badge fresh">{status}</span> v{built} <code>{digest}</code>'


def _route_match(route: dict[str, Any]) -> str:
    match = route.get("match", {})
    methods = ",".join(match.get("methods", []))
    if route.get("kind") == "forward":
        return html.escape(
            f"{methods}  ->  {match.get('target_host')}:{match.get('target_port')}{match.get('path_prefix')}"
        )
    return html.escape(f"{methods}  {match.get('path_prefix')}")


def render_dashboard(diagnostics: dict[str, Any]) -> bytes:
    version = diagnostics.get("version")
    state = diagnostics.get("state") or {}
    routes = diagnostics.get("routes") or []
    aggregate = diagnostics.get("aggregate_indexes") or {}
    errors = diagnostics.get("recent_errors") or []
    traces = diagnostics.get("invalidation_trace") or []
    health = diagnostics.get("health") or {}
    recovery = diagnostics.get("recovery") or {}
    shutdown = diagnostics.get("shutdown")

    route_rows: list[str] = []
    for route in routes:
        health_items = "".join(
            f'<div class="health {html.escape(item.get("status", "unknown"))}">'
            f'{html.escape(item.get("upstream", ""))} · {html.escape(item.get("status", ""))}'
            f'</div>'
            for item in route.get("upstream_health", [])
        )
        route_rows.append(
            "<tr>"
            f'<td><strong>{html.escape(route.get("id", ""))}</strong><br><span class="muted">{html.escape(route.get("kind", ""))}</span></td>'
            f"<td>{_route_match(route)}</td>"
            f'<td><code>{html.escape(route.get("upstream", ""))}</code><br><span class="muted">pool {html.escape(route.get("failure_policy", {}).get("group", ""))}</span></td>'
            f'<td><div class="artifact-grid">'
            + "".join(
                f'<div><span class="muted">{html.escape(kind)}</span><br>{_artifact_badge(artifact)}</div>'
                for kind, artifact in route.get("artifacts", {}).items()
            )
            + "</div></td>"
            f'<td><div class="health-list">{health_items}</div></td>'
            "</tr>"
        )

    trace_rows: list[str] = []
    for trace in traces[:12]:
        changed = trace.get("changed_route_ids", [])
        invalidated = trace.get("invalidated_artifacts", [])
        rebuilt = trace.get("rebuilt_artifacts", [])
        fields = trace.get("changed_fields", {})
        trace_rows.append(
            "<tr>"
            f'<td>{html.escape(str(trace.get("from_version")))} → <strong>{html.escape(str(trace.get("to_version")))}</strong><br>'
            f'<span class="muted">{html.escape(str(trace.get("at", "")))}</span></td>'
            f'<td>{html.escape(str(trace.get("event", "update")))}</td>'
            f'<td>{html.escape(", ".join(changed))}<br><span class="muted">{html.escape(json.dumps(fields, ensure_ascii=False))}</span></td>'
            f'<td><strong>{len(invalidated)}</strong> 失效<br><span class="muted">{html.escape(", ".join(invalidated))}</span></td>'
            f'<td><strong>{len(rebuilt)}</strong> 重建<br><span class="muted">{html.escape(", ".join(rebuilt))}</span></td>'
            "</tr>"
        )

    error_rows = "".join(
        "<tr>"
        f'<td><code>{html.escape(error.get("code", ""))}</code></td>'
        f'<td>{html.escape(str(error.get("route_id") or "-"))}</td>'
        f'<td>{html.escape(str(error.get("config_version") or "-"))}</td>'
        f'<td>{html.escape(error.get("message", ""))}</td>'
        f'<td><span class="muted">{html.escape(error.get("at", ""))}</span></td>'
        "</tr>"
        for error in errors[:25]
    )
    aggregate_rows = "".join(
        "<tr>"
        f'<td><code>{html.escape(name)}</code></td>'
        f'<td>{_artifact_badge(artifact)}</td>'
        f'<td><span class="muted">{html.escape(", ".join(artifact.get("depends_on", [])[:6]))}</span></td>'
        "</tr>"
        for name, artifact in aggregate.items()
    )
    health_json = json.dumps(health, ensure_ascii=False, indent=2, sort_keys=True)
    recovery_json = json.dumps(recovery, ensure_ascii=False, indent=2, sort_keys=True)
    shutdown_json = json.dumps(shutdown, ensure_ascii=False, indent=2, sort_keys=True)
    raw_json = html.escape(json.dumps(diagnostics, ensure_ascii=False, indent=2, sort_keys=True))

    document = f"""<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <meta http-equiv="refresh" content="3">
  <title>Loopback Proxy Workbench · v{html.escape(str(version))}</title>
  <style>
    :root {{ color-scheme: light; --ink:#17212b; --muted:#667085; --line:#d8dee8; --panel:#fff; --bg:#f3f6fa; --accent:#0f766e; --bad:#b42318; --warn:#b54708; }}
    * {{ box-sizing:border-box; }}
    body {{ margin:0; font:14px/1.45 ui-monospace, SFMono-Regular, Consolas, monospace; color:var(--ink); background:var(--bg); }}
    header {{ padding:24px 32px 18px; background:linear-gradient(120deg,#0f172a,#123b3a); color:white; }}
    header h1 {{ margin:0 0 8px; font-size:23px; letter-spacing:.2px; }}
    header p {{ margin:0; opacity:.78; }}
    main {{ padding:22px 32px 40px; max-width:1600px; margin:auto; }}
    .cards {{ display:grid; grid-template-columns:repeat(auto-fit,minmax(180px,1fr)); gap:12px; margin-bottom:18px; }}
    .card {{ background:var(--panel); border:1px solid var(--line); border-radius:12px; padding:14px 16px; box-shadow:0 4px 18px rgba(15,23,42,.04); }}
    .card .label {{ color:var(--muted); font-size:12px; text-transform:uppercase; letter-spacing:.08em; }}
    .card .value {{ font-size:21px; margin-top:5px; font-weight:700; }}
    section {{ background:var(--panel); border:1px solid var(--line); border-radius:14px; margin:14px 0; overflow:hidden; }}
    section h2 {{ margin:0; padding:14px 18px; font-size:15px; border-bottom:1px solid var(--line); background:#fbfcfe; }}
    table {{ width:100%; border-collapse:collapse; }}
    th,td {{ text-align:left; vertical-align:top; padding:11px 14px; border-bottom:1px solid #edf0f5; }}
    th {{ color:var(--muted); font-size:12px; text-transform:uppercase; letter-spacing:.06em; background:#fafbfd; }}
    tr:last-child td {{ border-bottom:0; }}
    code {{ font:inherit; background:#eef2f6; padding:1px 5px; border-radius:5px; }}
    .muted {{ color:var(--muted); font-size:12px; }}
    .badge {{ display:inline-block; border-radius:999px; padding:1px 7px; font-size:11px; font-weight:700; }}
    .badge.fresh {{ color:#067647; background:#dcfae6; }}
    .artifact-grid {{ display:grid; grid-template-columns:1fr 1fr; gap:7px 14px; min-width:300px; }}
    .health-list {{ display:flex; flex-direction:column; gap:5px; }}
    .health {{ border-radius:7px; padding:4px 7px; background:#f2f4f7; }}
    .health.healthy {{ color:#067647; background:#dcfae6; }}
    .health.unhealthy {{ color:var(--bad); background:#fee4e2; }}
    .health.unknown {{ color:var(--warn); background:#fef0c7; }}
    pre {{ margin:0; padding:16px 18px; overflow:auto; max-height:420px; background:#0b1220; color:#dbe7ff; font-size:12px; }}
    details summary {{ cursor:pointer; padding:13px 18px; font-weight:700; }}
  </style>
</head>
<body>
<header>
  <h1>Loopback Proxy Workbench</h1>
  <p>配置版本 v{html.escape(str(version))} · generation {html.escape(str(state.get("generation", "-")))} · 每次刷新读取单一不可变快照 · 自动刷新 3s</p>
</header>
<main>
  <div class="cards">
    <div class="card"><div class="label">Config version</div><div class="value">v{html.escape(str(version))}</div></div>
    <div class="card"><div class="label">Routes</div><div class="value">{len(routes)}</div></div>
    <div class="card"><div class="label">Artifacts</div><div class="value">{html.escape(str(state.get("artifact_count", 0)))}</div></div>
    <div class="card"><div class="label">Process</div><div class="value" style="font-size:16px">{html.escape(str(diagnostics.get("process_state")))}</div></div>
    <div class="card"><div class="label">Recovered version</div><div class="value" style="font-size:16px">v{html.escape(str(recovery.get("last_complete_version")))}</div></div>
  </div>

  <section>
    <h2>路由 / 编译产物 / 上游健康</h2>
    <table><thead><tr><th>Route</th><th>Match</th><th>Upstream</th><th>Fresh artifacts</th><th>Health</th></tr></thead>
    <tbody>{''.join(route_rows) or '<tr><td colspan="5">尚无路由</td></tr>'}</tbody></table>
  </section>

  <section>
    <h2>级联失效轨迹（最近 12 次）</h2>
    <table><thead><tr><th>Version</th><th>Event</th><th>Changed route / fields</th><th>Invalidated</th><th>Rebuilt</th></tr></thead>
    <tbody>{''.join(trace_rows) or '<tr><td colspan="5">尚无更新</td></tr>'}</tbody></table>
  </section>

  <section>
    <h2>聚合索引</h2>
    <table><thead><tr><th>Artifact</th><th>Status</th><th>Depends on</th></tr></thead><tbody>{aggregate_rows}</tbody></table>
  </section>

  <section>
    <h2>最近转发 / 配置错误</h2>
    <table><thead><tr><th>Code</th><th>Route</th><th>Version</th><th>Message</th><th>At</th></tr></thead>
    <tbody>{error_rows or '<tr><td colspan="5">无错误</td></tr>'}</tbody></table>
  </section>

  <section>
    <h2>持久化恢复报告（只含 SQLite 已持久化事实）</h2>
    <pre>{html.escape(recovery_json)}</pre>
  </section>
  <section>
    <h2>上次安全退出记录</h2>
    <pre>{html.escape(shutdown_json)}</pre>
  </section>
  <section>
    <h2>上游健康状态机</h2>
    <pre>{html.escape(health_json)}</pre>
  </section>
  <details>
    <summary>完整诊断 JSON</summary>
    <pre>{raw_json}</pre>
  </details>
</main>
</body>
</html>"""
    return document.encode("utf-8")


class WorkbenchServer:
    def __init__(
        self,
        *,
        runtime: Runtime,
        host: str = "127.0.0.1",
        port: int = 18080,
        graceful_timeout: float = 8.0,
    ):
        if not is_loopback_host(host):
            raise ValueError("WorkbenchServer host must be a loopback address")
        self.runtime = runtime
        self.host = host
        self.requested_port = port
        self.port = port
        self.graceful_timeout = graceful_timeout
        self._server: asyncio.AbstractServer | None = None
        self._tasks: set[asyncio.Task[Any]] = set()
        self._accepting = True
        self._shutdown_event = asyncio.Event()
        self._waiting: asyncio.Task[Any] | None = None
        self._shutdown_result: dict[str, Any] | None = None

    async def start(self) -> "WorkbenchServer":
        self._server = await asyncio.start_server(
            self._handle_connection,
            host=self.host,
            port=self.requested_port,
            limit=MAX_REQUEST_HEADER,
        )
        sockets = self._server.sockets or []
        if sockets:
            self.port = int(sockets[0].getsockname()[1])
        return self

    def request_shutdown(self) -> None:
        if not self._shutdown_event.is_set():
            self._shutdown_event.set()

    async def wait_closed(self) -> dict[str, Any]:
        if self._waiting is not None:
            return await self._waiting
        self._waiting = asyncio.create_task(self._drain_and_stop(), name="workbench-drain")
        return await self._waiting

    async def _drain_and_stop(self) -> dict[str, Any]:
        await self._shutdown_event.wait()
        self._accepting = False
        if self._server is not None:
            self._server.close()
            await self._server.wait_closed()
        await asyncio.sleep(0)
        current = asyncio.current_task()
        pending = {task for task in self._tasks if task is not current and not task.done()}
        completed = len(pending)
        cancelled = 0
        detail = "all admitted requests drained before listener close"
        if pending:
            done, still_pending = await asyncio.wait(pending, timeout=self.graceful_timeout)
            completed = len(done)
            if still_pending:
                cancelled = len(still_pending)
                detail = f"grace timeout {self.graceful_timeout}s reached; cancelled remaining requests"
                for task in still_pending:
                    task.cancel()
                await asyncio.gather(*still_pending, return_exceptions=True)
        self._shutdown_result = await self.runtime.shutdown(
            mode="graceful",
            completed=completed,
            cancelled=cancelled,
            detail=detail,
        )
        return self._shutdown_result

    async def _handle_connection(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        task = asyncio.current_task()
        if task is not None:
            self._tasks.add(task)
        try:
            if not self._accepting:
                await _write_json(
                    writer,
                    status=503,
                    payload={
                        "ok": False,
                        "code": E_SHUTTING_DOWN,
                        "message": "server is draining and is not accepting new requests",
                    },
                )
                return
            await self._serve_request(reader, writer)
        except asyncio.CancelledError:
            raise
        except (ConnectionError, BrokenPipeError, asyncio.IncompleteReadError):
            pass
        except ProxyError as error:
            try:
                await _write_json(
                    writer,
                    status=error.http_status,
                    payload=_error_payload(error, None),
                    extra_headers=(("X-Workbench-Error", error.code),),
                )
            except (ConnectionError, BrokenPipeError, OSError):
                pass
        except Exception as error:  # keep failures local and machine-readable
            self.runtime.record_error(
                ProxyError("E_INTERNAL", "unhandled server error", 500, {"error": type(error).__name__}),
                source="server",
            )
            try:
                await _write_json(
                    writer,
                    status=500,
                    payload={
                        "ok": False,
                        "code": "E_INTERNAL",
                        "message": "unhandled server error",
                        "details": {"error": type(error).__name__},
                    },
                )
            except (ConnectionError, BrokenPipeError, OSError):
                pass
        finally:
            writer.close()
            try:
                await asyncio.wait_for(writer.wait_closed(), timeout=0.5)
            except (asyncio.TimeoutError, ConnectionError, OSError):
                pass
            if task is not None:
                self._tasks.discard(task)

    async def _serve_request(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        head = await _read_head(reader)
        path = _request_path(head.target)
        is_internal = (
            path == "/"
            or path == "/dashboard"
            or path.startswith("/__workbench/")
        )
        if is_internal and not head.target.lower().startswith(("http://", "https://")):
            await self._handle_internal(head, reader, writer)
            return

        state = self.runtime.current
        if state is None:
            body = await _read_body(reader, head, writer)
            del body
            error = ProxyError(
                E_ROUTE_NOT_FOUND,
                "no configuration snapshot is loaded",
                503,
                {"target": head.target[:256]},
            )
            self.runtime.record_error(error, source="routing")
            raise error
        compiled = self.runtime.match_target(
            state,
            method=head.method,
            target=head.target,
            host=head.header("host"),
        )
        if compiled is None:
            body = await _read_body(reader, head, writer)
            del body
            error = ProxyError(
                E_ROUTE_NOT_FOUND,
                "no route matched the request",
                404,
                {"method": head.method, "target": head.target[:256], "config_version": state.version},
            )
            self.runtime.record_error(
                error,
                version=state.version,
                source="routing",
            )
            raise error

        body = await _read_body(reader, head, writer)
        forwarded_headers = list(head.headers)
        peer = writer.get_extra_info("peername")
        if peer:
            forwarded_headers.append(("X-Forwarded-For", str(peer[0])))
        forwarded_headers.append(("X-Forwarded-Proto", "http"))
        try:
            outcome = await self.runtime.forward(
                bound_state=state,
                compiled=compiled,
                method=head.method,
                path_query=_target_path_query(head.target),
                headers=forwarded_headers,
                body=body,
            )
        except ProxyError as error:
            if error.code == E_CONFIG_CHANGED:
                self.runtime.record_error(
                    error,
                    route_id=compiled.route.id,
                    version=state.version,
                    source="forward",
                )
            raise

        version = str(state.version)
        extra_headers = (
            ("X-Workbench-Version", version),
            ("X-Workbench-Route", compiled.route.id),
            ("X-Workbench-Upstream", outcome.upstream),
            ("X-Workbench-Attempts", str(outcome.attempts)),
        )
        await _write_response(
            writer,
            status=outcome.response.status,
            headers=outcome.response.headers,
            body=outcome.response.body,
            extra_headers=extra_headers,
            omit_body=head.method == "HEAD",
        )

    async def _handle_internal(
        self,
        head: RequestHead,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        body = await _read_body(reader, head, writer)
        path = _request_path(head.target)
        if head.method == "GET" and path == "/":
            await _write_response(
                writer,
                status=302,
                headers=(("Location", "/dashboard"), ("Content-Type", "text/plain; charset=utf-8")),
                body=b"dashboard: /dashboard\n",
            )
            return
        if head.method == "GET" and path == "/dashboard":
            state = self.runtime.current
            diagnostics = self.runtime.diagnostics(state)
            await _write_response(
                writer,
                status=200,
                headers=(("Content-Type", "text/html; charset=utf-8"), ("Cache-Control", "no-store")),
                body=render_dashboard(diagnostics),
            )
            return
        if head.method == "GET" and path == "/__workbench/health":
            await _write_json(
                writer,
                status=200,
                payload={
                    "ok": True,
                    "code": "OK",
                    "process_state": self.runtime.store.current_process_state(),
                    "version": self.runtime.current.version if self.runtime.current else None,
                },
            )
            return
        if head.method == "GET" and path in (
            "/__workbench/state",
            "/__workbench/diagnostics",
            "/__workbench/artifacts",
            "/__workbench/errors",
        ):
            state = self.runtime.current
            diagnostics = self.runtime.diagnostics(state)
            if path == "/__workbench/state":
                payload = {
                    "ok": True,
                    "code": "OK",
                    "version": diagnostics["version"],
                    "state": diagnostics["state"],
                    "config": diagnostics["config"],
                    "routes": diagnostics["routes"],
                    "aggregate_indexes": diagnostics["aggregate_indexes"],
                    "health": diagnostics["health"],
                    "recovery": diagnostics["recovery"],
                    "shutdown": diagnostics["shutdown"],
                }
            elif path == "/__workbench/artifacts":
                payload = {
                    "ok": True,
                    "code": "OK",
                    "version": diagnostics["version"],
                    "artifacts": diagnostics["artifacts"],
                    "aggregate_indexes": diagnostics["aggregate_indexes"],
                    "invalidation_trace": diagnostics["invalidation_trace"],
                }
            elif path == "/__workbench/errors":
                payload = {
                    "ok": True,
                    "code": "OK",
                    "version": diagnostics["version"],
                    "errors": diagnostics["recent_errors"],
                    "invalidation_trace": diagnostics["invalidation_trace"],
                }
            else:
                payload = diagnostics
            await _write_json(writer, status=200, payload=payload)
            return
        if head.method == "POST" and path == "/__workbench/config":
            try:
                payload = json.loads(body.decode("utf-8")) if body else None
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise ProxyError(E_CONFIG_INVALID, "request body is not valid UTF-8 JSON", 400) from exc
            if payload is None:
                raise ProxyError(E_CONFIG_INVALID, "configuration payload is required", 400)
            outcome = await self.runtime.submit(payload)
            await _write_json(writer, status=200, payload=outcome.public())
            return
        if head.method == "POST" and path == "/__workbench/shutdown":
            await _write_json(
                writer,
                status=200,
                payload={"ok": True, "code": "OK", "message": "graceful shutdown requested"},
            )
            asyncio.get_running_loop().call_soon(self.request_shutdown)
            return
        raise ProxyError(E_ROUTE_NOT_FOUND, "unknown workbench endpoint", 404, {"path": path})


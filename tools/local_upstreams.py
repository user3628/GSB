from __future__ import annotations

import argparse
import asyncio
import base64
import hashlib
from contextlib import suppress
from typing import Any
from urllib.parse import urlsplit

import uvicorn
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, Response
from starlette.requests import ClientDisconnect

from app.models import is_loopback_host


ALL_METHODS = ["GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS", "TRACE"]


def _body_payload(body: bytes) -> dict[str, Any]:
    try:
        text = body.decode("utf-8")
        return {"body_text": text}
    except UnicodeDecodeError:
        return {"body_base64": base64.b64encode(body).decode("ascii")}


def create_backend_app(instance: str, port: int) -> FastAPI:
    app = FastAPI(title=f"Loopback backend {instance}", docs_url=None, redoc_url=None)
    stats = {"forwarded_requests": 0}

    @app.middleware("http")
    async def count_requests(request: Request, call_next):
        if not request.url.path.startswith("/__"):
            stats["forwarded_requests"] += 1
        return await call_next(request)

    @app.get("/__stats")
    async def request_stats() -> dict[str, Any]:
        return {"instance": instance, "port": port, **stats}

    @app.get("/health")
    async def health() -> dict[str, Any]:
        return {"ok": True, "instance": instance, "port": port}

    @app.api_route("/status/{status_code}", methods=ALL_METHODS)
    async def fixed_status(status_code: int, request: Request) -> Response:
        payload = {
            "ok": status_code < 500,
            "instance": instance,
            "port": port,
            "status": status_code,
            "path": request.url.path,
            "query": request.url.query,
        }
        return JSONResponse(status_code=status_code, content=payload)

    @app.api_route("/slow/{delay_ms}", methods=ALL_METHODS)
    async def slow_named(delay_ms: int, request: Request) -> Response:
        await asyncio.sleep(max(0, delay_ms) / 1000.0)
        return JSONResponse(
            content={
                "ok": True,
                "instance": instance,
                "port": port,
                "delayed_ms": delay_ms,
                "path": request.url.path,
                "query": request.url.query,
            }
        )

    @app.api_route("/{path:path}", methods=ALL_METHODS)
    async def catch_all(path: str, request: Request) -> Response:
        if path == "slow":
            await asyncio.sleep(0.35)
        elif path == "hang":
            await asyncio.sleep(60.0)
        elif path == "echo" or path.startswith(("svc/", "echo/")):
            pass
        elif path:
            # Unknown paths are still deterministic and useful for prefix tests.
            pass

        try:
            body = await request.body()
        except ClientDisconnect:
            return Response(status_code=499)
        payload = {
            "ok": True,
            "instance": instance,
            "port": port,
            "method": request.method,
            "path": request.url.path,
            "query": request.url.query,
            "headers": {key.lower(): value for key, value in request.headers.items()},
            "body_sha256": hashlib.sha256(body).hexdigest(),
            **_body_payload(body),
        }
        return JSONResponse(
            content=payload,
            headers={
                "x-test-backend": instance,
                "x-test-backend-port": str(port),
            },
        )

    return app


async def _read_headers(reader: asyncio.StreamReader) -> bytes:
    return await reader.readuntil(b"\r\n\r\n")


async def _egress_connection(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
    *,
    egress_label: str,
) -> None:
    upstream_writer: asyncio.StreamWriter | None = None
    try:
        raw_headers = await asyncio.wait_for(_read_headers(reader), timeout=5.0)
        lines = raw_headers.decode("latin-1").split("\r\n")
        if not lines or not lines[0]:
            return
        method, target, version = lines[0].split(" ", 2)
        if not target.startswith(("http://", "https://")):
            writer.write(b"HTTP/1.1 400 Bad Request\r\nConnection: close\r\nContent-Length: 0\r\n\r\n")
            await writer.drain()
            return
        parsed = urlsplit(target)
        if parsed.scheme != "http" or not parsed.hostname or not is_loopback_host(parsed.hostname):
            body = b'{"ok":false,"business_code":"E_BAD_REQUEST","message":"egress is loopback-http only"}'
            writer.write(
                b"HTTP/1.1 400 Bad Request\r\nConnection: close\r\nContent-Type: application/json\r\n"
                + f"Content-Length: {len(body)}\r\n\r\n".encode("ascii")
                + body
            )
            await writer.drain()
            return

        headers: list[tuple[str, str]] = []
        for line in lines[1:]:
            if not line:
                continue
            name, value = line.split(":", 1)
            lowered = name.strip().lower()
            if lowered in {
                "connection",
                "proxy-connection",
                "proxy-authorization",
                "keep-alive",
                "te",
                "trailer",
                "transfer-encoding",
                "upgrade",
                "host",
            }:
                continue
            headers.append((name.strip(), value.strip()))

        content_length = 0
        for name, value in headers:
            if name.lower() == "content-length":
                content_length = int(value)
                break
        body = await reader.readexactly(content_length) if content_length else b""

        host = parsed.hostname
        port = parsed.port or 80
        upstream_reader, upstream_writer = await asyncio.wait_for(
            asyncio.open_connection(host, port), timeout=2.0
        )
        path = parsed.path or "/"
        if parsed.query:
            path += "?" + parsed.query
        request_lines = [f"{method} {path} HTTP/1.1", f"Host: {host}:{port}"]
        request_lines.extend(f"{name}: {value}" for name, value in headers)
        request_lines.extend(
            [
                f"X-Local-Egress: {egress_label}",
                "Connection: close",
            ]
        )
        serialized = ("\r\n".join(request_lines) + "\r\n\r\n").encode("latin-1") + body
        upstream_writer.write(serialized)
        await upstream_writer.drain()

        while True:
            chunk = await upstream_reader.read(65536)
            if not chunk:
                break
            writer.write(chunk)
            await writer.drain()
    except (asyncio.IncompleteReadError, asyncio.LimitOverrunError, ValueError, OSError):
        with suppress(Exception):
            body = b'{"ok":false,"business_code":"E_UPSTREAM_UNAVAILABLE"}'
            writer.write(
                b"HTTP/1.1 502 Bad Gateway\r\nConnection: close\r\n"
                b"Content-Type: application/json\r\n"
                + f"Content-Length: {len(body)}\r\n\r\n".encode("ascii")
                + body
            )
            await writer.drain()
    finally:
        if upstream_writer is not None:
            upstream_writer.close()
            with suppress(Exception):
                await upstream_writer.wait_closed()
        writer.close()
        with suppress(Exception):
            await writer.wait_closed()


async def start_egress(host: str, port: int) -> asyncio.AbstractServer:
    return await asyncio.start_server(
        lambda reader, writer: _egress_connection(
            reader, writer, egress_label=f"{host}:{port}"
        ),
        host=host,
        port=port,
    )


async def run_servers(args: argparse.Namespace) -> None:
    servers: list[uvicorn.Server] = []
    if args.mode in {"all", "backends"}:
        primary = uvicorn.Server(
            uvicorn.Config(
                create_backend_app("primary", args.backend_port),
                host=args.host,
                port=args.backend_port,
                log_level=args.log_level,
                access_log=False,
            )
        )
        secondary = uvicorn.Server(
            uvicorn.Config(
                create_backend_app("secondary", args.alt_backend_port),
                host=args.host,
                port=args.alt_backend_port,
                log_level=args.log_level,
                access_log=False,
            )
        )
        servers.extend([primary, secondary])

    tasks = [asyncio.create_task(server.serve()) for server in servers]
    egress_server: asyncio.AbstractServer | None = None
    if args.mode in {"all", "egress"}:
        egress_server = await start_egress(args.host, args.egress_port)

    try:
        if tasks:
            await asyncio.gather(*tasks)
        elif egress_server is not None:
            await asyncio.Event().wait()
        else:
            raise RuntimeError("no service mode selected")
    finally:
        if egress_server is not None:
            egress_server.close()
            await egress_server.wait_closed()
        for task in tasks:
            task.cancel()
        for task in tasks:
            with suppress(asyncio.CancelledError):
                await task


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Start loopback-only deterministic test services")
    parser.add_argument("--mode", choices=["all", "backends", "egress"], default="all")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--backend-port", type=int, default=18081)
    parser.add_argument("--alt-backend-port", type=int, default=18083)
    parser.add_argument("--egress-port", type=int, default=18082)
    parser.add_argument("--log-level", default="warning")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    try:
        asyncio.run(run_servers(args))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()

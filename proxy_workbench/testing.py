"""Deterministic loopback HTTP backend used by tests and the demo script."""

from __future__ import annotations

import asyncio
import json
import socket
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import parse_qs, urlsplit


MAX_TEST_BODY = 8 * 1024 * 1024


def port_is_free(port: int, host: str = "127.0.0.1") -> bool:
    probe = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        probe.bind((host, port))
        return True
    except OSError:
        return False
    finally:
        probe.close()


def reserve_unused_port(host: str = "127.0.0.1") -> int:
    probe = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        probe.bind((host, 0))
        return int(probe.getsockname()[1])
    finally:
        probe.close()


@dataclass
class LoopbackBackend:
    host: str = "127.0.0.1"
    port: int = 0
    health_status: int = 200
    slow_delay: float = 0.5
    label: str | None = None
    requests: list[dict[str, Any]] = field(default_factory=list)
    request_started: asyncio.Event = field(default_factory=asyncio.Event)
    _server: asyncio.AbstractServer | None = None
    _connections: set[asyncio.Task[Any]] = field(default_factory=set)

    @property
    def upstream(self) -> str:
        return f"http://{self.host}:{self.port}"

    async def start(self) -> "LoopbackBackend":
        self._server = await asyncio.start_server(self._handle, self.host, self.port, limit=128 * 1024)
        sockets = self._server.sockets or []
        if sockets:
            self.port = int(sockets[0].getsockname()[1])
        return self

    async def stop(self) -> None:
        if self._server is not None:
            self._server.close()
            await self._server.wait_closed()
            self._server = None
        tasks = [task for task in self._connections if not task.done()]
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    def matching_requests(self, prefix: str) -> list[dict[str, Any]]:
        return [item for item in self.requests if item["path"].startswith(prefix)]

    async def _handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        task = asyncio.current_task()
        if task is not None:
            self._connections.add(task)
        try:
            head = await asyncio.wait_for(reader.readuntil(b"\r\n\r\n"), timeout=5)
            lines = head.decode("latin-1").split("\r\n")
            method, target, _version = lines[0].split(" ", 2)
            headers: list[tuple[str, str]] = []
            for line in lines[1:]:
                if not line:
                    continue
                name, _, value = line.partition(":")
                headers.append((name, value.strip()))
            header_map: dict[str, list[str]] = {}
            for name, value in headers:
                header_map.setdefault(name.lower(), []).append(value)
            length = int(header_map.get("content-length", ["0"])[0])
            if length > MAX_TEST_BODY:
                raise ValueError("test body too large")
            body = await reader.readexactly(length) if length else b""
            parts = urlsplit(target)
            path = parts.path or "/"
            query = parse_qs(parts.query, keep_blank_values=True)
            record = {
                "method": method,
                "target": target,
                "path": path,
                "query": parts.query,
                "headers": header_map,
                "body": body.decode("utf-8", errors="replace"),
                "backend": self.port,
                "label": self.label,
            }
            self.requests.append(record)
            self.request_started.set()

            status = 200
            response: dict[str, Any] = record
            if path == "/health":
                status = self.health_status
                response = {"ok": 200 <= status < 400, "backend": self.port, "health_status": status}
            elif path.startswith("/status/"):
                status = int(path.split("/", 3)[2])
                response = {
                    "error": "scripted upstream status",
                    "backend": self.port,
                    "status": status,
                    "path": path,
                }
            elif path.startswith("/slow"):
                delay = self.slow_delay
                if "ms" in query:
                    delay = int(query["ms"][0]) / 1000.0
                await asyncio.sleep(delay)
                response = {**record, "delayed_ms": int(delay * 1000)}
            payload = json.dumps(response, ensure_ascii=False, sort_keys=True).encode("utf-8")
            reason = "OK" if status == 200 else "Scripted"
            response_head = (
                f"HTTP/1.1 {status} {reason}\r\n"
                "Content-Type: application/json; charset=utf-8\r\n"
                f"Content-Length: {len(payload)}\r\n"
                "Connection: close\r\n\r\n"
            ).encode("latin-1")
            writer.write(response_head + payload)
            await writer.drain()
        except (asyncio.IncompleteReadError, asyncio.TimeoutError, ConnectionError, ValueError):
            pass
        finally:
            writer.close()
            try:
                await asyncio.wait_for(writer.wait_closed(), timeout=0.5)
            except (asyncio.TimeoutError, ConnectionError, OSError):
                pass
            if task is not None:
                self._connections.discard(task)


"""UDP/TCP DNS middleware service and command-line entry point."""

from __future__ import annotations

import argparse
import json
import logging
import socket
import socketserver
import struct
import threading
import time
from pathlib import Path
from typing import Any

from dnslib import DNSHeader, DNSRecord

from .admin import AdminHttpServer
from .cache import CacheStore
from .codec import (
    ParsedRequest,
    RCODE_FORMERR,
    parse_request,
    render_response,
)
from .errors import BusinessError, E_MALFORMED_MESSAGE, rcode_for_error
from .resolver import RecursiveResolver, ResolverConfig
from .stats import Stats
from .upstream import UpstreamClient


LOGGER = logging.getLogger("dnsmw")


def _log_event(event: str, **fields: Any) -> None:
    LOGGER.info(json.dumps({"event": event, **fields}, ensure_ascii=False, sort_keys=True))


class DnsMiddleware:
    def __init__(self, cache: CacheStore, resolver: RecursiveResolver, stats: Stats) -> None:
        self.cache = cache
        self.resolver = resolver
        self.stats = stats

    def handle(self, packet: bytes, transport: str, peer: tuple[Any, ...] | None = None) -> bytes | None:
        packet = bytes(packet)
        self.stats.inc(f"{transport}_requests")
        self.stats.inc("request_bytes", len(packet))
        try:
            request = parse_request(packet)
        except BusinessError as exc:
            self.stats.error(exc.code)
            _log_event(
                "request_error",
                code=exc.code,
                detail=exc.detail,
                transport=transport,
                peer=list(peer) if peer else None,
            )
            return self._error_packet(packet, exc.code)
        try:
            resolution = self.resolver.resolve(
                request.key, request.labels, request.qtype, request.qclass
            )
            wire, truncated = render_response(request, resolution, transport=transport)
            if truncated:
                self.stats.inc("udp_truncated")
            self.stats.inc("response_bytes", len(wire))
            _log_event(
                "response",
                transport=transport,
                key=request.key.name,
                qtype=request.qtype,
                response_code=resolution.business_code,
                rcode=resolution.rcode,
                cached=resolution.cached,
                tc=truncated,
                size=len(wire),
            )
            return wire
        except BusinessError as exc:
            self.stats.error(exc.code)
            _log_event("render_error", code=exc.code, detail=exc.detail)
            return self._error_packet(packet, exc.code)
        except Exception as exc:
            self.stats.error("E_INTERNAL")
            LOGGER.exception("unhandled middleware error: %s", exc)
            return self._error_packet(packet, "E_INTERNAL")

    @staticmethod
    def _error_packet(packet: bytes, code: str) -> bytes | None:
        if len(packet) < 2:
            return None
        message_id = struct.unpack_from("!H", packet, 0)[0]
        rd = (packet[2] >> 0) & 1 if len(packet) >= 3 else 1
        response = DNSRecord(
            header=DNSHeader(
                id=message_id,
                qr=1,
                aa=0,
                rd=rd,
                ra=1,
                rcode=rcode_for_error(code) & 0x0F,
            )
        )
        return response.pack()


class _ThreadingUDPServer(socketserver.ThreadingMixIn, socketserver.UDPServer):
    daemon_threads = True
    allow_reuse_address = True
    max_packet_size = 65535

    def __init__(self, address: tuple[str, int], middleware: DnsMiddleware) -> None:
        self.middleware = middleware
        super().__init__(address, _UDPHandler)


class _UDPHandler(socketserver.BaseRequestHandler):
    def handle(self) -> None:
        data, sock = self.request
        response = self.server.middleware.handle(bytes(data), "udp", self.client_address)  # type: ignore[attr-defined]
        if response is not None:
            sock.sendto(response, self.client_address)


class _ThreadingTCPServer(socketserver.ThreadingMixIn, socketserver.TCPServer):
    daemon_threads = True
    allow_reuse_address = True

    def __init__(self, address: tuple[str, int], middleware: DnsMiddleware) -> None:
        self.middleware = middleware
        super().__init__(address, _TCPHandler)


class _TCPHandler(socketserver.BaseRequestHandler):
    def handle(self) -> None:
        sock: socket.socket = self.request
        sock.settimeout(5.0)
        try:
            prefix = _recv_exact(sock, 2)
            if prefix is None:
                return
            length = struct.unpack("!H", prefix)[0]
            if length == 0:
                return
            data = _recv_exact(sock, length)
            if data is None:
                return
            response = self.server.middleware.handle(data, "tcp", self.client_address)  # type: ignore[attr-defined]
            if response is not None:
                sock.sendall(struct.pack("!H", len(response)) + response)
        except (OSError, ValueError):
            return


def _recv_exact(sock: socket.socket, length: int) -> bytes | None:
    chunks: list[bytes] = []
    remaining = length
    while remaining:
        chunk = sock.recv(remaining)
        if not chunk:
            return None
        chunks.append(chunk)
        remaining -= len(chunk)
    return b"".join(chunks)


class MiddlewareService:
    def __init__(
        self,
        *,
        listen_host: str = "127.0.0.1",
        udp_port: int = 15354,
        tcp_port: int = 15354,
        upstream_host: str = "127.0.0.1",
        upstream_port: int = 15353,
        upstream_timeout: float = 0.75,
        upstream_transport: str = "udp",
        max_cname_hops: int = 4,
        db_path: str | Path = "runtime/middleware.sqlite3",
        admin_host: str = "127.0.0.1",
        admin_port: int = 18080,
        purge_interval: float = 0.5,
    ) -> None:
        self.stats = Stats()
        self.cache = CacheStore(db_path)
        self.upstream = UpstreamClient(
            upstream_host,
            upstream_port,
            timeout=upstream_timeout,
            transport=upstream_transport,
        )
        self.resolver = RecursiveResolver(
            self.cache,
            self.upstream,
            self.stats,
            ResolverConfig(max_cname_hops=max_cname_hops),
        )
        self.middleware = DnsMiddleware(self.cache, self.resolver, self.stats)
        self.listen_host = listen_host
        self.requested_udp_port = int(udp_port)
        self.requested_tcp_port = int(tcp_port)
        self.purge_interval = max(0.05, float(purge_interval))
        self.udp: _ThreadingUDPServer | None = None
        self.tcp: _ThreadingTCPServer | None = None
        self.admin: AdminHttpServer | None = None
        self._threads: list[threading.Thread] = []
        self._stop = threading.Event()
        self._purger: threading.Thread | None = None
        self._started_at = time.time()
        self.admin_host = admin_host
        self.admin_port = int(admin_port)

    def start(self) -> "MiddlewareService":
        self.udp = _ThreadingUDPServer((self.listen_host, self.requested_udp_port), self.middleware)
        udp_port = int(self.udp.server_address[1])
        tcp_port = udp_port if self.requested_udp_port == 0 and self.requested_tcp_port == 0 else self.requested_tcp_port
        try:
            self.tcp = _ThreadingTCPServer((self.listen_host, tcp_port), self.middleware)
        except OSError:
            self.udp.server_close()
            raise
        if self.admin_port >= 0:
            self.admin = AdminHttpServer(self, self.admin_host, self.admin_port).start()
        for server in (self.udp, self.tcp):
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            self._threads.append(thread)
        self._purger = threading.Thread(target=self._purge_loop, daemon=True)
        self._purger.start()
        _log_event(
            "startup",
            udp_port=int(self.udp.server_address[1]),
            tcp_port=int(self.tcp.server_address[1]),
            admin_port=self.admin.port if self.admin else None,
            db=str(self.cache.path),
            recovery=self.cache.recovery_report,
        )
        return self

    def _purge_loop(self) -> None:
        while not self._stop.wait(self.purge_interval):
            try:
                self.cache.purge_expired()
            except Exception:
                LOGGER.exception("cache purge failed")

    def health(self) -> dict[str, Any]:
        return {
            "status": "ok",
            "uptime_seconds": max(0.0, time.time() - self._started_at),
            "udp_port": int(self.udp.server_address[1]) if self.udp else None,
            "tcp_port": int(self.tcp.server_address[1]) if self.tcp else None,
            "admin_port": self.admin.port if self.admin else None,
            "db_path": str(self.cache.path),
            "max_cname_hops": self.resolver.config.max_cname_hops,
            "upstream": f"{self.upstream.host}:{self.upstream.port}",
        }

    def admin_snapshot(self) -> dict[str, Any]:
        return {
            "health": self.health(),
            "stats": self.stats.snapshot(),
            "cache": self.cache.stats(),
            "upstream": self.upstream.stats(),
            "inflight": self.resolver.inflight_count(),
            "recovery": self.cache.latest_recovery_report(),
        }

    def stop(self) -> None:
        self._stop.set()
        if self.admin is not None:
            self.admin.stop()
        for server in (self.udp, self.tcp):
            if server is not None:
                server.shutdown()
                server.server_close()
        for thread in self._threads:
            thread.join(timeout=2.0)
        if self._purger is not None:
            self._purger.join(timeout=2.0)
        self.cache.close()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Loopback recursive DNS middleware")
    parser.add_argument("--listen-host", default="127.0.0.1")
    parser.add_argument("--udp-port", type=int, default=15354)
    parser.add_argument("--tcp-port", type=int, default=15354)
    parser.add_argument("--upstream-host", default="127.0.0.1")
    parser.add_argument("--upstream-port", type=int, default=15353)
    parser.add_argument("--upstream-timeout", type=float, default=0.75)
    parser.add_argument("--upstream-transport", choices=("udp", "tcp"), default="udp")
    parser.add_argument("--max-cname-hops", type=int, default=4)
    parser.add_argument("--db", default="runtime/middleware.sqlite3")
    parser.add_argument("--admin-host", default="127.0.0.1")
    parser.add_argument("--admin-port", type=int, default=18080)
    parser.add_argument("--purge-interval", type=float, default=0.5)
    parser.add_argument("--log-level", default="INFO")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    logging.basicConfig(
        level=getattr(logging, str(args.log_level).upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(message)s",
    )
    service = MiddlewareService(
        listen_host=args.listen_host,
        udp_port=args.udp_port,
        tcp_port=args.tcp_port,
        upstream_host=args.upstream_host,
        upstream_port=args.upstream_port,
        upstream_timeout=args.upstream_timeout,
        upstream_transport=args.upstream_transport,
        max_cname_hops=args.max_cname_hops,
        db_path=args.db,
        admin_host=args.admin_host,
        admin_port=args.admin_port,
        purge_interval=args.purge_interval,
    ).start()
    try:
        while True:
            time.sleep(1.0)
    except KeyboardInterrupt:
        pass
    finally:
        service.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

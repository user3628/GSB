from __future__ import annotations

import asyncio
import logging
from pathlib import Path

from .authority import AuthorityServer, ZoneData
from .config import AppConfig
from .listener import DnsListener
from .service import DiagnosticService
from .storage import Storage


class Runtime:
    def __init__(self, config: AppConfig) -> None:
        self.config = config
        self.storage = Storage(config.db_file)
        self.service: DiagnosticService | None = None
        self.listener: DnsListener | None = None
        self.authorities: list[AuthorityServer] = []
        self._started = False
        self._start_lock = asyncio.Lock()
        self.logger = logging.getLogger("dns_workbench")

    async def start(self, *, start_dns: bool | None = None, start_authorities: bool | None = None) -> None:
        async with self._start_lock:
            if self._started:
                return
            self.config.log_path.mkdir(parents=True, exist_ok=True)
            recovery = await self.storage.initialize()
            self.logger.info(
                "startup recovery: forced=%s aborted_sessions=%d pending_intents=%d",
                recovery.get("forced_interruption_detected"),
                len(recovery.get("aborted_sessions", [])),
                recovery.get("pending_intents_found", 0),
            )
            self.service = DiagnosticService(self.config, self.storage)
            should_start_authorities = self.config.enable_authorities if start_authorities is None else start_authorities
            if should_start_authorities:
                for authority_config in self.config.authorities:
                    if not authority_config.zone_file:
                        raise ValueError(f"authority {authority_config.id} has no zone_file")
                    zone = ZoneData.load(authority_config.zone_file)
                    server = AuthorityServer(authority_config, zone)
                    await server.start()
                    self.authorities.append(server)
            should_start_dns = self.config.enable_dns_listener if start_dns is None else start_dns
            if should_start_dns:
                self.listener = DnsListener(self.config, self.service)
                await self.listener.start()
            self._started = True

    async def stop(self, *, clean: bool = True) -> None:
        async with self._start_lock:
            if not self._started:
                return
            if self.listener is not None:
                await self.listener.stop()
                self.listener = None
            for server in reversed(self.authorities):
                await server.stop()
            self.authorities.clear()
            if clean:
                try:
                    await self.storage.mark_clean_shutdown(True)
                except Exception:
                    self.logger.exception("failed to persist clean shutdown marker")
            await self.storage.close()
            self._started = False

    async def health(self) -> dict[str, object]:
        if self.service is None:
            return {"status": "starting", "database": str(self.config.db_file)}
        result = await self.service.health()
        if self.listener is not None:
            result["dns_listener"] = self.listener.stats.snapshot()
        result["authorities"] = [
            {"id": server.config.id, "stats": server.stats.snapshot()} for server in self.authorities
        ]
        return result


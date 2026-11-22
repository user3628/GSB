from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import Any, AsyncIterator

from .models import now_ms


class GateTimeout(asyncio.TimeoutError):
    def __init__(self, context: dict[str, Any]) -> None:
        self._context = context
        super().__init__("timed out waiting for upstream concurrency capacity")

    def context(self) -> dict[str, Any]:
        return dict(self._context)


class GateQueueFull(asyncio.QueueFull):
    def __init__(self, context: dict[str, Any]) -> None:
        self._context = context
        super().__init__("upstream waiting queue is full")

    def context(self) -> dict[str, Any]:
        return dict(self._context)


class ConcurrencyGate:
    """Atomic global + per-authority capacity with deadline-aware waiting."""

    def __init__(
        self,
        global_limit: int,
        per_authority_limit: int,
        queue_limit: int | None = None,
    ) -> None:
        self.global_limit = global_limit
        self.per_authority_limit = per_authority_limit
        self.queue_limit = queue_limit
        self._condition = asyncio.Condition()
        self._active_global = 0
        self._active_by_authority: dict[str, int] = {}
        self._waiters = 0
        self._waiters_by_authority: dict[str, int] = {}
        self._dispatched = 0
        self._peak_global = 0
        self._peak_by_authority: dict[str, int] = {}

    def _has_capacity(self, key: str) -> bool:
        return (
            self._active_global < self.global_limit
            and self._active_by_authority.get(key, 0) < self.per_authority_limit
        )

    async def acquire(self, key: str, *, deadline_ms: int | None = None) -> None:
        started = now_ms()
        async with self._condition:
            if self._has_capacity(key):
                self._acquire_now(key)
                return
            if self.queue_limit is not None and self._waiters >= self.queue_limit:
                raise GateQueueFull(
                    {
                        "reason": "upstream_waiting_queue_full",
                        "authority_id": key,
                        "queue_limit": self.queue_limit,
                        "waiters": self._waiters,
                        "global_active": self._active_global,
                        "global_limit": self.global_limit,
                        "authority_active": self._active_by_authority.get(key, 0),
                        "authority_limit": self.per_authority_limit,
                    }
                )
            self._waiters += 1
            self._waiters_by_authority[key] = self._waiters_by_authority.get(key, 0) + 1
            try:
                while not self._has_capacity(key):
                    if deadline_ms is None:
                        await self._condition.wait()
                        continue
                    remaining_ms = deadline_ms - now_ms()
                    if remaining_ms <= 0:
                        raise GateTimeout(
                            {
                                "reason": "round_budget_exhausted_while_waiting",
                                "authority_id": key,
                                "waited_ms": max(0, now_ms() - started),
                                "remaining_budget_ms": 0,
                                "global_active": self._active_global,
                                "global_limit": self.global_limit,
                                "authority_active": self._active_by_authority.get(key, 0),
                                "authority_limit": self.per_authority_limit,
                                "waiters": self._waiters,
                            }
                        )
                    try:
                        await asyncio.wait_for(self._condition.wait(), timeout=remaining_ms / 1000.0)
                    except asyncio.TimeoutError as exc:
                        raise GateTimeout(
                            {
                                "reason": "round_budget_exhausted_while_waiting",
                                "authority_id": key,
                                "waited_ms": max(0, now_ms() - started),
                                "remaining_budget_ms": 0,
                                "global_active": self._active_global,
                                "global_limit": self.global_limit,
                                "authority_active": self._active_by_authority.get(key, 0),
                                "authority_limit": self.per_authority_limit,
                                "waiters": self._waiters,
                            }
                        ) from exc
                self._acquire_now(key)
            finally:
                self._waiters -= 1
                self._waiters_by_authority[key] -= 1
                if self._waiters_by_authority[key] <= 0:
                    del self._waiters_by_authority[key]

    def _acquire_now(self, key: str) -> None:
        self._active_global += 1
        active_for_authority = self._active_by_authority.get(key, 0) + 1
        self._active_by_authority[key] = active_for_authority
        self._dispatched += 1
        self._peak_global = max(self._peak_global, self._active_global)
        self._peak_by_authority[key] = max(self._peak_by_authority.get(key, 0), active_for_authority)

    async def release(self, key: str) -> None:
        async with self._condition:
            if self._active_by_authority.get(key, 0) > 0:
                self._active_by_authority[key] -= 1
                if self._active_by_authority[key] == 0:
                    del self._active_by_authority[key]
            self._active_global = max(0, self._active_global - 1)
            self._condition.notify_all()

    @asynccontextmanager
    async def lease(self, key: str, *, deadline_ms: int | None = None) -> AsyncIterator[None]:
        await self.acquire(key, deadline_ms=deadline_ms)
        try:
            yield
        finally:
            await self.release(key)

    async def snapshot(self) -> dict[str, Any]:
        async with self._condition:
            return {
                "global_active": self._active_global,
                "global_limit": self.global_limit,
                "global_peak": self._peak_global,
                "per_authority_limit": self.per_authority_limit,
                "queue_limit": self.queue_limit,
                "authority_active": dict(self._active_by_authority),
                "authority_peak": dict(self._peak_by_authority),
                "waiters": self._waiters,
                "waiters_by_authority": dict(self._waiters_by_authority),
                "dispatched_upstream_queries": self._dispatched,
            }


@dataclass(slots=True)
class Flight:
    cache_key: str
    leader_query_id: str
    created_ms: int
    timeout_ms: int
    priority: int = 0
    arrival_seq: int = 0
    started_ms: int | None = None
    future: asyncio.Future[Any] = field(default_factory=lambda: asyncio.get_running_loop().create_future())
    split_claimed: bool = False
    split_count: int = 0
    dispatch_count: int = 0
    cache_expires_at_ms: int | None = None

    @property
    def done(self) -> bool:
        return self.future.done()


class FlightRegistry:
    """Global in-flight key registry used for merge / single-flight behavior."""

    def __init__(self, max_splits_per_flight: int = 1) -> None:
        self._lock = asyncio.Lock()
        self._flights: dict[str, Flight] = {}
        self.max_splits_per_flight = max_splits_per_flight
        self._retention_tasks: set[asyncio.Task[Any]] = set()

    async def peek(self, cache_key: str) -> Flight | None:
        async with self._lock:
            return self._flights.get(cache_key)

    async def get_or_create(
        self,
        cache_key: str,
        leader_query_id: str,
        timeout_ms: int,
        *,
        priority: int = 0,
        arrival_seq: int = 0,
    ) -> tuple[Flight, bool]:
        async with self._lock:
            existing = self._flights.get(cache_key)
            if existing is not None and (
                existing.cache_expires_at_ms is None or existing.cache_expires_at_ms > now_ms()
            ):
                return existing, False
            if existing is not None:
                del self._flights[cache_key]
            flight = Flight(
                cache_key=cache_key,
                leader_query_id=leader_query_id,
                created_ms=now_ms(),
                timeout_ms=timeout_ms,
                priority=priority,
                arrival_seq=arrival_seq,
            )
            self._flights[cache_key] = flight
            return flight, True

    async def remove(self, flight: Flight) -> None:
        async with self._lock:
            if self._flights.get(flight.cache_key) is flight:
                del self._flights[flight.cache_key]

    def schedule_remove(self, flight: Flight, delay_ms: int = 250) -> None:
        """Keep a just-completed flight briefly so a late cache-miss racer can still merge."""

        async def delayed_remove() -> None:
            await asyncio.sleep(max(0, delay_ms) / 1000.0)
            await self.remove(flight)

        task = asyncio.create_task(delayed_remove())
        self._retention_tasks.add(task)
        task.add_done_callback(self._retention_tasks.discard)

    async def claim_split(self, flight: Flight) -> bool:
        async with self._lock:
            if flight.split_count >= self.max_splits_per_flight:
                return False
            flight.split_count += 1
            flight.split_claimed = True
            return True

    async def snapshot(self) -> dict[str, Any]:
        async with self._lock:
            return {
                "in_flight": len(self._flights),
                "keys": [
                    {
                        "cache_key": flight.cache_key,
                        "leader_query_id": flight.leader_query_id,
                        "age_ms": max(0, now_ms() - flight.created_ms),
                        "started": flight.started_ms is not None,
                        "split_count": flight.split_count,
                    }
                    for flight in self._flights.values()
                ],
            }


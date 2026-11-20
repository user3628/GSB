"""Health dependency state shared by probes and real forwarding results."""

from __future__ import annotations

import asyncio
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from .compiler import CompiledRoute, CompiledState
from .errors import ProxyError
from .transport import perform_request


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


@dataclass
class HealthState:
    upstream: str
    status: str = "unknown"
    consecutive_failures: int = 0
    consecutive_successes: int = 0
    last_checked_at: str | None = None
    last_error: str | None = None
    last_source: str = "initial"
    probe_plan_ids: set[str] = field(default_factory=set)

    def public(self, route_health: dict[str, Any] | None = None) -> dict[str, Any]:
        value = {
            "upstream": self.upstream,
            "status": self.status,
            "consecutive_failures": self.consecutive_failures,
            "consecutive_successes": self.consecutive_successes,
            "last_checked_at": self.last_checked_at,
            "last_error": self.last_error,
            "last_source": self.last_source,
        }
        if route_health is not None:
            value["plan"] = route_health
        return value


class HealthManager:
    def __init__(self) -> None:
        self.states: dict[str, HealthState] = {}
        self.transitions: deque[dict[str, Any]] = deque(maxlen=200)
        self.generation = 0
        self._tasks: list[asyncio.Task[Any]] = []
        self._state: CompiledState | None = None

    async def prepare_update(self) -> int:
        """Invalidate old plan tasks before a new immutable config is published."""
        self.generation += 1
        generation = self.generation
        tasks, self._tasks = self._tasks, []
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        return generation

    def start_plans(self, state: CompiledState, generation: int) -> None:
        if generation != self.generation:
            raise RuntimeError("health plan generation changed while publishing")
        self._state = state
        active_upstreams = {
            upstream for compiled in state.routes.values() for upstream in compiled.route.upstream_candidates
        }
        for upstream in list(self.states):
            if upstream not in active_upstreams:
                del self.states[upstream]
        for compiled in state.routes.values():
            if not compiled.route.health.enabled:
                continue
            task = asyncio.create_task(
                self._probe_loop(compiled, generation),
                name=f"health:{compiled.route.id}:{generation}",
            )
            self._tasks.append(task)

    async def stop(self) -> None:
        self.generation += 1
        tasks, self._tasks = self._tasks, []
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    def _get(self, upstream: str) -> HealthState:
        state = self.states.get(upstream)
        if state is None:
            state = HealthState(upstream=upstream)
            self.states[upstream] = state
        return state

    def _transition(self, state: HealthState, before: str, source: str, detail: dict[str, Any]) -> None:
        if before == state.status:
            return
        self.transitions.appendleft(
            {
                "at": _now(),
                "upstream": state.upstream,
                "from": before,
                "to": state.status,
                "source": source,
                "detail": detail,
            }
        )

    def record_probe_success(
        self, upstream: str, plan_id: str, generation: int, *, success_threshold: int
    ) -> None:
        if generation != self.generation:
            return
        state = self._get(upstream)
        before = state.status
        state.consecutive_failures = 0
        state.consecutive_successes += 1
        state.last_checked_at = _now()
        state.last_error = None
        state.last_source = "probe"
        state.probe_plan_ids.add(plan_id)
        if state.consecutive_successes >= success_threshold:
            state.status = "healthy"
        self._transition(
            state,
            before,
            "probe",
            {"plan_id": plan_id, "generation": generation, "successes": state.consecutive_successes},
        )

    def record_probe_failure(
        self,
        upstream: str,
        plan_id: str,
        generation: int,
        *,
        error: str,
        fail_threshold: int,
        success_threshold: int,
    ) -> None:
        if generation != self.generation:
            return
        state = self._get(upstream)
        before = state.status
        state.consecutive_successes = 0
        state.consecutive_failures += 1
        state.last_checked_at = _now()
        state.last_error = error
        state.last_source = "probe"
        state.probe_plan_ids.add(plan_id)
        if state.consecutive_failures >= fail_threshold:
            state.status = "unhealthy"
        self._transition(
            state,
            before,
            "probe",
            {
                "plan_id": plan_id,
                "generation": generation,
                "failures": state.consecutive_failures,
                "error": error,
            },
        )

    def record_forward_success(self, upstream: str, *, route_id: str) -> None:
        state = self._get(upstream)
        before = state.status
        state.consecutive_failures = 0
        state.consecutive_successes += 1
        state.last_checked_at = _now()
        state.last_error = None
        state.last_source = "forward"
        state.status = "healthy"
        self._transition(
            state,
            before,
            "forward",
            {"route_id": route_id, "successes": state.consecutive_successes},
        )

    def record_forward_failure(
        self,
        upstream: str,
        *,
        route_id: str,
        error: str,
        fail_threshold: int,
    ) -> None:
        state = self._get(upstream)
        before = state.status
        state.consecutive_successes = 0
        state.consecutive_failures += 1
        state.last_checked_at = _now()
        state.last_error = error
        state.last_source = "forward"
        if state.consecutive_failures >= fail_threshold:
            state.status = "unhealthy"
        self._transition(
            state,
            before,
            "forward",
            {"route_id": route_id, "failures": state.consecutive_failures, "error": error},
        )

    def choose_candidate(self, compiled: CompiledRoute) -> str | None:
        route = compiled.route
        primary = route.upstream
        primary_state = self.states.get(primary)
        if route.failure_policy.mode == "fail":
            if primary_state is not None and primary_state.status == "unhealthy":
                return None
            return primary
        for candidate in route.upstream_candidates:
            state = self.states.get(candidate)
            if state is None or state.status != "unhealthy":
                return candidate
        return None

    def candidate_public(self, compiled: CompiledRoute) -> list[dict[str, Any]]:
        result = []
        for upstream in compiled.route.upstream_candidates:
            status = self.states.get(upstream)
            if status is None:
                result.append(
                    HealthState(upstream=upstream).public(
                        {
                            "artifact_id": compiled.artifact("health_plan").artifact_id,
                            "enabled": compiled.route.health.enabled,
                        }
                    )
                )
            else:
                result.append(
                    status.public(
                        {
                            "artifact_id": compiled.artifact("health_plan").artifact_id,
                            "enabled": compiled.route.health.enabled,
                        }
                    )
                )
        return result

    def public(self, state: CompiledState | None = None) -> dict[str, Any]:
        result: dict[str, Any] = {
            "generation": self.generation,
            "upstreams": {
                upstream: health.public()
                for upstream, health in sorted(self.states.items())
            },
            "transitions": list(self.transitions),
        }
        if state is not None:
            result["routes"] = {
                route_id: self.candidate_public(compiled)
                for route_id, compiled in sorted(state.routes.items())
            }
        return result

    async def _probe_loop(self, compiled: CompiledRoute, generation: int) -> None:
        route = compiled.route
        plan_id = compiled.artifact("health_plan").artifact_id
        try:
            while generation == self.generation:
                for upstream in route.upstream_candidates:
                    if generation != self.generation:
                        return
                    path_query = route.health.path
                    try:
                        response = await perform_request(
                            upstream=upstream,
                            method="GET",
                            path_query=path_query,
                            headers=(("User-Agent", "local-proxy-workbench-health/1"),),
                            body=b"",
                            connect_timeout=route.health.timeout,
                            read_timeout=route.health.timeout,
                        )
                        if 200 <= response.status < 400:
                            self.record_probe_success(
                                upstream,
                                plan_id,
                                generation,
                                success_threshold=route.health.success_threshold,
                            )
                        else:
                            self.record_probe_failure(
                                upstream,
                                plan_id,
                                generation,
                                error=f"health status {response.status}",
                                fail_threshold=route.health.fail_threshold,
                                success_threshold=route.health.success_threshold,
                            )
                    except ProxyError as exc:
                        self.record_probe_failure(
                            upstream,
                            plan_id,
                            generation,
                            error=exc.code,
                            fail_threshold=route.health.fail_threshold,
                            success_threshold=route.health.success_threshold,
                        )
                    except asyncio.CancelledError:
                        raise
                    except BaseException as exc:
                        self.record_probe_failure(
                            upstream,
                            plan_id,
                            generation,
                            error=type(exc).__name__,
                            fail_threshold=route.health.fail_threshold,
                            success_threshold=route.health.success_threshold,
                        )
                await asyncio.sleep(route.health.interval)
        except asyncio.CancelledError:
            return


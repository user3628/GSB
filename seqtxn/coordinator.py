"""Configurable local HTTP transaction-coordinator test service."""

from __future__ import annotations

import asyncio
import copy
import json
import threading
import time
from pathlib import Path
from typing import Any

from fastapi import FastAPI, Header, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel


class ConfigureRequest(BaseModel):
    transaction_id: str
    responses: list[dict[str, Any]] | None = None
    reset: bool = False


class _State:
    def __init__(self, config: dict) -> None:
        self.lock = threading.RLock()
        self.default_config = copy.deepcopy(config)
        self.config: dict = {}
        self.index: dict[str, int] = {}
        self.cache: dict[tuple[str, str], dict] = {}
        self.observations: list[dict] = []
        self.observation_seq = 0
        self.reset()

    def reset(self) -> None:
        with self.lock:
            self.config = copy.deepcopy(self.default_config)
            self.index = {}
            self.cache = {}
            self.observations = []
            self.observation_seq = 0

    def configure(self, txn_id: str, responses: list[dict] | None, reset: bool) -> None:
        with self.lock:
            if responses is not None:
                self.config.setdefault("transactions", {})[txn_id] = {"responses": responses}
            if reset:
                self.index.pop(txn_id, None)
                for key in [key for key in self.cache if key[0] == txn_id]:
                    self.cache.pop(key, None)

    def next_script(self, txn_id: str) -> dict:
        with self.lock:
            transactions = self.config.get("transactions", {})
            entry = transactions.get(txn_id)
            if entry is None:
                default = self.config.get("default_response", {"outcome": "UNKNOWN"})
                return copy.deepcopy(default)
            responses = entry.get("responses") or []
            if not responses:
                return {"outcome": "UNKNOWN"}
            index = self.index.get(txn_id, 0)
            response = copy.deepcopy(responses[min(index, len(responses) - 1)])
            self.index[txn_id] = index + 1
            return response

    def cache_get(self, txn_id: str, key: str) -> dict | None:
        with self.lock:
            value = self.cache.get((txn_id, key))
            return copy.deepcopy(value) if value else None

    def cache_put(self, txn_id: str, key: str, response: dict) -> None:
        with self.lock:
            self.cache.setdefault((txn_id, key), copy.deepcopy(response))

    def observe(self, observation: dict) -> None:
        with self.lock:
            self.observation_seq += 1
            self.observations.append(
                {"observation_id": self.observation_seq, **observation}
            )


def create_coordinator_app(config_path: str | Path) -> FastAPI:
    config = json.loads(Path(config_path).read_text(encoding="utf-8"))
    state = _State(config)
    app = FastAPI(title="seqtxn local transaction coordinator", version="1.0.0")
    app.state.coordinator_state = state

    @app.get("/health")
    def health():
        return {"ok": True, "code": "OK", "data": {"status": "ok"}}

    @app.get("/txn/{txn_id}")
    async def query_transaction(
        txn_id: str,
        request: Request,
        idempotency_key: str | None = Header(default=None, alias="Idempotency-Key"),
        fault: str | None = Header(default=None, alias="X-Fault-Injection"),
        force: str | None = Header(default=None, alias="X-Force-Query"),
    ):
        key = idempotency_key or f"txn:{txn_id}"
        forced = force == "1"
        started_monotonic = time.monotonic()
        observation = {
            "transaction_id": txn_id,
            "idempotency_key": key,
            "injection": fault,
            "force": forced,
        }

        if not forced:
            cached = state.cache_get(txn_id, key)
            if cached is not None:
                observation.update(
                    {
                        "outcome": cached.get("outcome"),
                        "cached": True,
                        "source": "idempotency_cache",
                        "client_disconnected": False,
                    }
                )
                state.observe(observation)
                return {**cached, "source": "idempotency_cache", "replayed": True}

        injected = (fault or "").upper()
        if injected == "HANG":
            disconnected = False
            max_wait = float(state.config.get("hang_seconds", 2.0))
            while time.monotonic() - started_monotonic < max_wait:
                await asyncio.sleep(0.02)
                if await request.is_disconnected():
                    disconnected = True
                    break
            observation.update(
                {
                    "outcome": None,
                    "cached": False,
                    "source": "injected_hang",
                    "client_disconnected": disconnected,
                    "elapsed_ms": int((time.monotonic() - started_monotonic) * 1000),
                }
            )
            state.observe(observation)
            return JSONResponse(
                status_code=504,
                content={
                    "outcome": "UNKNOWN",
                    "source": "injected_hang",
                    "client_disconnected": disconnected,
                },
            )

        if injected in {"COMMIT", "ROLLBACK", "UNKNOWN"}:
            response = {"outcome": injected, "source": "injected_response"}
        else:
            response = state.next_script(txn_id)

        behavior = str(response.get("behavior", "")).upper()
        if behavior in {"HANG", "SLEEP"}:
            disconnected = False
            max_wait = float(response.get("sleep_ms", 2_000)) / 1000.0
            while time.monotonic() - started_monotonic < max_wait:
                await asyncio.sleep(0.02)
                if await request.is_disconnected():
                    disconnected = True
                    break
            observation.update(
                {
                    "outcome": None,
                    "cached": False,
                    "source": "script_hang",
                    "client_disconnected": disconnected,
                    "elapsed_ms": int((time.monotonic() - started_monotonic) * 1000),
                }
            )
            state.observe(observation)
            return JSONResponse(
                status_code=504,
                content={
                    "outcome": "UNKNOWN",
                    "source": "script_hang",
                    "client_disconnected": disconnected,
                },
            )

        status_code = int(response.get("http_status", 200))
        outcome = str(response.get("outcome", "UNKNOWN")).upper()
        payload = {
            "transaction_id": txn_id,
            "idempotency_key": key,
            "outcome": outcome,
            "source": response.get("source", "script"),
            "script_index": state.index.get(txn_id, 0) - 1,
        }
        if status_code == 200 and outcome in {"COMMIT", "ROLLBACK"}:
            # A forced conflicting response is deliberately not cached. This is
            # how the test endpoint simulates an inconsistent re-query.
            if not forced and injected not in {"COMMIT", "ROLLBACK"}:
                state.cache_put(txn_id, key, payload)
        observation.update(
            {
                "outcome": outcome,
                "cached": False,
                "source": payload["source"],
                "client_disconnected": False,
            }
        )
        state.observe(observation)
        return JSONResponse(status_code=status_code, content=payload)

    @app.get("/admin/observations")
    def observations():
        with state.lock:
            return {
                "ok": True,
                "code": "OK",
                "data": copy.deepcopy(state.observations),
            }

    @app.post("/admin/configure")
    def configure(request: ConfigureRequest):
        state.configure(request.transaction_id, request.responses, request.reset)
        return {"ok": True, "code": "OK", "data": {"transaction_id": request.transaction_id}}

    @app.post("/admin/reset")
    def reset():
        state.reset()
        return {"ok": True, "code": "OK", "data": {"reset": True}}

    return app


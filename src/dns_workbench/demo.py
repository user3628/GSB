from __future__ import annotations

import asyncio
import json
import os
import shutil
import socket
import subprocess
import sys
import time
import uuid
from pathlib import Path
from typing import Any

import httpx

from .authority import AuthorityServer, ZoneData
from .config import AppConfig, AuthorityConfig, SchedulerConfig


def _free_port() -> int:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])
    finally:
        sock.close()


def _free_ports(count: int) -> list[int]:
    ports: list[int] = []
    while len(ports) < count:
        candidate = _free_port()
        if candidate not in ports:
            ports.append(candidate)
    return ports


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _write_config(path: Path, *, db_path: Path, log_dir: Path, http_port: int, dns_port: int, auth_a: int, auth_b: int, zone_file: Path) -> AppConfig:
    data = {
        "db_path": str(db_path),
        "log_dir": str(log_dir),
        "http_host": "127.0.0.1",
        "http_port": http_port,
        "dns_host": "127.0.0.1",
        "dns_port": dns_port,
        "enable_dns_listener": True,
        "enable_authorities": False,
        "hot_names": ["hot.lab.test."],
        "max_cname_hops": 4,
        "max_udp_response_bytes": 1232,
        "scheduler": {
            "global_upstream_limit": 1,
            "per_authority_limit": 1,
            "queue_limit": 8,
            "round_budget_ms": 120,
            "upstream_timeout_ms": 400,
            "merge_split_enabled": True,
            "merge_split_after_ms": 20,
            "merge_split_reserve_ms": 20,
            "min_split_budget_ms": 40,
            "max_splits_per_flight": 1,
        },
        "authorities": [
            {
                "id": "auth-a",
                "host": "127.0.0.1",
                "port": auth_a,
                "zones": ["lab.test."],
                "transport": "udp",
                "zone_file": str(zone_file),
            },
            {
                "id": "auth-b",
                "host": "127.0.0.1",
                "port": auth_b,
                "zones": ["long.lab.test."],
                "transport": "udp",
                "zone_file": str(zone_file),
            },
        ],
    }
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    return AppConfig.from_dict(data)


class ServiceProcess:
    def __init__(self, config_path: Path, log_path: Path) -> None:
        self.config_path = config_path
        self.log_path = log_path
        self.process: subprocess.Popen[bytes] | None = None
        self._log_handle: Any = None

    async def start(self, http_port: int) -> None:
        env = dict(os.environ)
        env["PYTHONPATH"] = str(_repo_root() / "src") + os.pathsep + env.get("PYTHONPATH", "")
        self._log_handle = self.log_path.open("ab")
        self.process = subprocess.Popen(
            [sys.executable, "-m", "dns_workbench", "serve", "--config", str(self.config_path)],
            cwd=str(_repo_root()),
            env=env,
            stdout=self._log_handle,
            stderr=subprocess.STDOUT,
        )
        async with httpx.AsyncClient(timeout=0.5) as client:
            for _ in range(160):
                if self.process.poll() is not None:
                    raise RuntimeError(
                        f"service process exited with code {self.process.returncode}; see {self.log_path}"
                    )
                try:
                    response = await client.get(f"http://127.0.0.1:{http_port}/health")
                    if response.status_code == 200:
                        return
                except httpx.HTTPError:
                    pass
                await asyncio.sleep(0.05)
        raise RuntimeError(f"service did not become healthy; see {self.log_path}")

    async def kill(self) -> None:
        if self.process is not None and self.process.poll() is None:
            self.process.kill()
            await asyncio.to_thread(self.process.wait, 10)
        self._close_log()

    async def terminate(self) -> None:
        if self.process is not None and self.process.poll() is None:
            self.process.terminate()
            try:
                await asyncio.to_thread(self.process.wait, 10)
            except subprocess.TimeoutExpired:
                self.process.kill()
                await asyncio.to_thread(self.process.wait, 10)
        self._close_log()

    def _close_log(self) -> None:
        if self._log_handle is not None:
            self._log_handle.close()
            self._log_handle = None


async def run_demo(workdir: Path, *, keep_running: bool = False) -> int:
    root = _repo_root()
    zone_file = root / "samples" / "zone_lab.json"
    workdir = workdir.resolve()
    if workdir.exists():
        shutil.rmtree(workdir)
    log_dir = workdir / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    db_path = workdir / "workbench.sqlite3"
    config_path = workdir / "config.json"

    http_port, dns_port, auth_a_port, auth_b_port = _free_ports(4)
    config = _write_config(
        config_path,
        db_path=db_path,
        log_dir=log_dir,
        http_port=http_port,
        dns_port=dns_port,
        auth_a=auth_a_port,
        auth_b=auth_b_port,
        zone_file=zone_file,
    )
    report: dict[str, Any] = {
        "generated_at_ms": int(time.time() * 1000),
        "config": config.redacted(),
        "phases": {},
    }

    zone = ZoneData.load(zone_file)
    authority_a = AuthorityServer(config.authorities[0], zone)
    authority_b = AuthorityServer(config.authorities[1], zone)
    await authority_a.start()
    await authority_b.start()
    service = ServiceProcess(config_path, log_dir / "service.log")
    restarted: ServiceProcess | None = None
    try:
        await service.start(http_port)
        base = f"http://127.0.0.1:{http_port}"
        async with httpx.AsyncClient(base_url=base, timeout=10.0) as client:
            cold = await client.post("/api/v1/queries", json={"name": "a.lab.test.", "type": "A"})
            cold_payload = cold.json()
            if cold.status_code != 200:
                raise RuntimeError(f"cold query failed: {cold.status_code} {cold.text}")
            cold_result = cold_payload["result"]
            await asyncio.sleep(0.25)
            cache_lookup = await client.get(
                "/api/v1/cache", params={"name": "A.LAB.TEST.", "type": "A"}
            )
            cache_entries = cache_lookup.json()["entries"]
            case_hit = await client.post(
                "/api/v1/queries", json={"name": "A.LaB.TeSt.", "type": "A"}
            )
            report["phases"]["cold_and_ttl"] = {
                "cold_round_id": cold_payload["round_id"],
                "cold_outcome": cold_result["outcome"],
                "answers": cold_result["answers"],
                "cache_count_after_case_mixed_lookup": len(cache_entries),
                "remaining_ttl_ms_after_250ms": cache_entries[0]["remaining_ttl_ms"] if cache_entries else None,
                "case_mixed_query_outcome": case_hit.json().get("result", {}).get("outcome"),
                "dispatch": cold_payload["trace"]["dispatch"],
            }
            print(json.dumps(report["phases"]["cold_and_ttl"], ensure_ascii=False, indent=2))

            before_hot = authority_a.stats.by_name.get("hot.lab.test.", 0)
            merge_requests = [
                {"name": "hot.lab.test.", "type": "A", "query_id": f"merge-{index:02d}"}
                for index in range(12)
            ]
            merge = await client.post(
                "/api/v1/queries/batch",
                json={"requests": merge_requests, "round_budget_ms": 1000},
            )
            merge_payload = merge.json()
            hot_after = authority_a.stats.by_name.get("hot.lab.test.", 0)
            merge_ttls = [item["answers"][0]["ttl_ms"] for item in merge_payload["items"] if item.get("answers")]
            report["phases"]["hot_merge"] = {
                "requests": len(merge_requests),
                "upstream_queries_for_hot_name": hot_after - before_hot,
                "outcomes": [item["outcome"] for item in merge_payload["items"]],
                "identical_ttl_ms": sorted(set(merge_ttls)),
                "dispatch": merge_payload["trace"]["dispatch"],
                "waiting": merge_payload["trace"]["waiting"],
                "merge_decisions": merge_payload["trace"]["merge_decisions"],
            }
            if hot_after - before_hot != 1:
                raise RuntimeError("merge invariant failed: expected exactly one upstream query")
            if len(set(merge_ttls)) != 1:
                raise RuntimeError("merge invariant failed: waiters observed different TTLs")
            print(json.dumps(report["phases"]["hot_merge"], ensure_ascii=False, indent=2))

            budget = await client.post(
                "/api/v1/queries/batch",
                json={
                    "requests": [
                        {"name": "slow.lab.test.", "type": "A", "query_id": "budget-slow"},
                        {"name": "tail.long.lab.test.", "type": "A", "query_id": "budget-tail"},
                    ],
                    "round_budget_ms": 80,
                },
            )
            budget_payload = budget.json()
            report["phases"]["budget_exhaustion"] = {
                "elapsed_ms": budget_payload["elapsed_ms"],
                "items": [
                    {
                        "query_id": item["query_id"],
                        "name": item["name"],
                        "error": item.get("error"),
                    }
                    for item in budget_payload["items"]
                ],
                "dispatch": budget_payload["trace"]["dispatch"],
                "waiting": budget_payload["trace"]["waiting"],
                "skipped": budget_payload["trace"]["skipped"],
            }
            if not budget_payload["trace"]["skipped"]:
                raise RuntimeError("budget phase did not produce a skipped list")
            print(json.dumps(report["phases"]["budget_exhaustion"], ensure_ascii=False, indent=2))

            timeout_response = await client.post(
                "/api/v1/queries",
                json={
                    "name": "timeout.lab.test.",
                    "type": "A",
                    "query_id": "timeout-demo",
                    "round_budget_ms": 1000,
                },
            )
            timeout_payload = timeout_response.json()
            health = (await client.get("/health")).json()
            timeout_code = timeout_payload.get("error", {}).get("code")
            report["phases"]["upstream_timeout"] = {
                "http_status": timeout_response.status_code,
                "business_code": timeout_code,
                "error": timeout_payload.get("error"),
                "socket_balance": health["upstream"]["socket_balance"],
                "active_sockets": health["upstream"]["active_sockets"],
                "closed_sockets": health["upstream"]["closed_sockets"],
            }
            if timeout_code != "E_UPSTREAM_TIMEOUT":
                raise RuntimeError(f"timeout phase returned unexpected code: {timeout_code}")
            if health["upstream"]["active_sockets"] != 0 or health["upstream"]["socket_balance"] != 0:
                raise RuntimeError("timeout phase leaked an upstream socket")
            print(json.dumps(report["phases"]["upstream_timeout"], ensure_ascii=False, indent=2))

            mx_before = await client.post(
                "/api/v1/queries", json={"name": "mx.lab.test.", "type": "MX", "query_id": "persist-mx"}
            )
            mx_payload = mx_before.json()
            mx_result = mx_payload["result"]
            mx_answer = mx_result["answers"][0]["rdata"]["target"]

            killed_query_id = f"kill-{uuid.uuid4().hex}"
            hanging = asyncio.create_task(
                client.post(
                    "/api/v1/queries",
                    json={
                        "name": "timeout.lab.test.",
                        "type": "A",
                        "query_id": killed_query_id,
                        "round_budget_ms": 5000,
                    },
                )
            )
            resolving_seen = False
            for _ in range(80):
                session_response = await client.get(f"/api/v1/sessions/{killed_query_id}")
                if session_response.status_code == 200 and session_response.json().get("state") == "resolving":
                    resolving_seen = True
                    break
                await asyncio.sleep(0.025)
            if not resolving_seen:
                hanging.cancel()
                await asyncio.gather(hanging, return_exceptions=True)
                raise RuntimeError("could not observe a resolving session before forced termination")
            await service.kill()
            try:
                await asyncio.wait_for(hanging, timeout=2)
            except (asyncio.TimeoutError, httpx.HTTPError):
                hanging.cancel()
                await asyncio.gather(hanging, return_exceptions=True)

        restarted = ServiceProcess(config_path, log_dir / "service-restarted.log")
        await restarted.start(http_port)
        async with httpx.AsyncClient(base_url=base, timeout=10.0) as client:
            recovery = (await client.get("/api/v1/recovery")).json()
            mx_after = await client.post(
                "/api/v1/queries", json={"name": "mx.lab.test.", "type": "MX", "query_id": "persist-mx-after"}
            )
            mx_after_payload = mx_after.json()
            report["phases"]["force_kill_recovery"] = {
                "clean_shutdown_before_startup": recovery.get("clean_shutdown_before_startup"),
                "forced_interruption_detected": recovery.get("forced_interruption_detected"),
                "pending_intents_found": recovery.get("pending_intents_found"),
                "aborted_sessions": recovery.get("aborted_sessions"),
                "killed_query_id": killed_query_id,
                "restart_query_outcome": mx_after_payload.get("result", {}).get("outcome"),
                "restart_mx_target": mx_after_payload.get("result", {}).get("answers", [{}])[0]
                .get("rdata", {})
                .get("target"),
                "pre_kill_mx_target": mx_answer,
                "restart_remaining_ttl_ms": mx_after_payload.get("result", {}).get("cache", {}).get(
                    "remaining_ttl_ms"
                ),
            }
            if not recovery.get("forced_interruption_detected"):
                raise RuntimeError("recovery report did not identify forced interruption")
            if killed_query_id not in {
                item.get("query_id") for item in recovery.get("aborted_sessions", [])
            }:
                raise RuntimeError("recovery report did not include the killed resolving session")
            if mx_after_payload.get("result", {}).get("outcome") != "cache_hit":
                raise RuntimeError("persisted MX entry was not a cache hit after restart")
            if (
                mx_after_payload.get("result", {}).get("answers", [{}])[0]
                .get("rdata", {})
                .get("target")
                != mx_answer
            ):
                raise RuntimeError("post-recovery MX answer differed from pre-kill committed answer")
            print(json.dumps(report["phases"]["force_kill_recovery"], ensure_ascii=False, indent=2))

        report["result"] = "PASS"
        (workdir / "report.json").write_text(
            json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        print("\nDEMO PASS")
        print(f"report: {workdir / 'report.json'}")
        return 0
    finally:
        if restarted is not None and not keep_running:
            await restarted.terminate()
        if service.process is not None and service.process.poll() is None:
            await service.terminate()
        await authority_a.stop()
        await authority_b.stop()


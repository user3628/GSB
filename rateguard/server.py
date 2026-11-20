"""Command-line entry point for the loopback-only HTTP service."""

from __future__ import annotations

import argparse
import ipaddress
import os
import sys

from .config import GuardConfig
from .engine import Engine
from .http_api import RateGuardHTTPServer


def _is_loopback(host: str) -> bool:
    if host == "localhost":
        return True
    try:
        return ipaddress.ip_address(host).is_loopback
    except ValueError:
        return False


def _fault_hook_from_environment() -> tuple[object | None, str | None]:
    fault = os.environ.get("RATEGUARD_FAULT", "").strip()
    if not fault:
        return None, None
    stages = {
        "exit_after_intent": "after_intent",
        "exit_after_window_version": "after_window_version",
        "exit_after_breaker_version": "after_breaker_version",
        "exit_transition_after_intent": "transition_after_intent",
        "exit_decision_after_intent": "decision_after_intent",
    }
    if fault not in stages:
        raise ValueError(
            "RATEGUARD_FAULT must be one of: " + ", ".join(sorted(stages))
        )
    target_stage = stages[fault]

    def hook(stage: str, context: dict[str, object]) -> None:
        if stage == target_stage:
            print(f"fault_injected stage={stage} exit_code=87 context={context}", flush=True)
            sys.stdout.flush()
            sys.stderr.flush()
            os._exit(87)

    return hook, target_stage


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run the RateGuard loopback HTTP service")
    parser.add_argument("--db", default="artifacts/rateguard.sqlite3", help="SQLite database path")
    parser.add_argument("--host", default="127.0.0.1", help="loopback host only")
    parser.add_argument("--port", type=int, default=8765, help="TCP port; 0 selects a free port")
    parser.add_argument("--window-length-ms", type=int, default=8_000)
    parser.add_argument("--step-ms", type=int, default=4_000)
    parser.add_argument("--max-quota", type=int, default=200)
    parser.add_argument("--target-latency-us", type=int, default=50_000)
    parser.add_argument("--target-concurrency", type=int, default=64)
    parser.add_argument("--open-duration-ms", type=int, default=4_000)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if not _is_loopback(args.host):
        print("refusing non-loopback bind; use 127.0.0.1, ::1, or localhost", file=sys.stderr)
        return 2
    if args.port < 0 or args.port > 65_535:
        print("port must be within 0..65535", file=sys.stderr)
        return 2

    config = GuardConfig(
        window_length_ms=args.window_length_ms,
        step_ms=args.step_ms,
        max_quota=args.max_quota,
        target_latency_us=args.target_latency_us,
        target_concurrency=args.target_concurrency,
        open_duration_ms=args.open_duration_ms,
    )
    try:
        fault_hook, fault_stage = _fault_hook_from_environment()
        engine = Engine(args.db, config, fault_hook=fault_hook)  # type: ignore[arg-type]
    except Exception as exc:  # noqa: BLE001 - startup failure should be concise
        print(f"startup_failed type={type(exc).__name__} message={exc}", file=sys.stderr)
        return 2

    host = args.host
    if host == "localhost":
        host = "127.0.0.1"
    server = RateGuardHTTPServer((host, args.port), engine)
    actual_port = server.server_address[1]
    recovery = engine.startup_recovery or {}
    print(
        "startup_recovery "
        f"incomplete_intents={recovery.get('incomplete_intents_found', 0)} "
        f"database={engine.storage.database}",
        flush=True,
    )
    if fault_stage:
        print(f"fault_armed stage={fault_stage}", flush=True)
    print(f"rateguard listening http://{host}:{actual_port}", flush=True)
    try:
        server.serve_forever(poll_interval=0.1)
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


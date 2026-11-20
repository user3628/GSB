from __future__ import annotations

import argparse
import json
import logging
import os
import shutil
import sys
from pathlib import Path

from .api import create_server
from .engine import ExecutionEngine
from .errors import BusinessError
from .selection import DEFAULT_MEMORY_BUDGET_BYTES, EngineConfig


def config_from_env() -> EngineConfig:
    return EngineConfig(
        scalar_max_rows=_env_int("VECENGINE_SCALAR_MAX_ROWS", 50_000),
        scalar_max_cells=_env_int("VECENGINE_SCALAR_MAX_CELLS", 400_000),
        scalar_max_nodes=_env_int("VECENGINE_SCALAR_MAX_NODES", 48),
        batch_min=_env_int("VECENGINE_BATCH_MIN", 512),
        batch_max=_env_int("VECENGINE_BATCH_MAX", 65_536),
        default_memory_budget_bytes=_env_int(
            "VECENGINE_MEMORY_BUDGET_BYTES", DEFAULT_MEMORY_BUDGET_BYTES
        ),
        test_hooks_enabled=os.environ.get("VECENGINE_TEST_HOOKS") == "1",
    )


def reset_local_state(db_path: Path, results_dir: Path) -> None:
    for suffix in ("", "-wal", "-shm"):
        path = Path(str(db_path) + suffix)
        if path.exists():
            path.unlink()
    if results_dir.exists():
        shutil.rmtree(results_dir)
    results_dir.mkdir(parents=True, exist_ok=True)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run the local vectorized expression engine API")
    parser.add_argument("--host", default=os.environ.get("VECENGINE_HOST", "127.0.0.1"))
    parser.add_argument("--port", type=int, default=os.environ.get("VECENGINE_PORT", "8765"))
    parser.add_argument("--db", default=os.environ.get("VECENGINE_DB", "artifacts/vecengine.db"))
    parser.add_argument(
        "--data-dir", default=os.environ.get("VECENGINE_DATA_DIR", "data/samples")
    )
    parser.add_argument(
        "--results-dir", default=os.environ.get("VECENGINE_RESULTS_DIR", "artifacts/results")
    )
    parser.add_argument("--reset", action="store_true", help="delete the DB/WAL and result files first")
    parser.add_argument("--log-level", default=os.environ.get("VECENGINE_LOG_LEVEL", "INFO"))
    args = parser.parse_args(argv)
    logging.basicConfig(
        level=getattr(logging, str(args.log_level).upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    db_path = Path(args.db).resolve()
    results_dir = Path(args.results_dir).resolve()
    if args.reset:
        reset_local_state(db_path, results_dir)
    try:
        engine = ExecutionEngine(
            db_path=db_path,
            data_dir=args.data_dir,
            results_dir=results_dir,
            config=config_from_env(),
            duplicate_wait_seconds=_env_float("VECENGINE_DUPLICATE_WAIT_SECONDS", 600.0),
        )
    except BusinessError as exc:
        print(json.dumps({"ok": False, "error": exc.to_dict()}, ensure_ascii=False), file=sys.stderr)
        return 2
    server = create_server(engine, args.host, args.port)
    actual_port = int(server.server_address[1])
    boot = {
        "status": "listening",
        "url": f"http://{args.host}:{actual_port}",
        "db": str(db_path),
        "data_dir": str(Path(args.data_dir).resolve()),
        "results_dir": str(results_dir),
        "startup_recovery_reports": len(engine.startup_recovery_reports),
    }
    print(json.dumps(boot, ensure_ascii=False), flush=True)
    if engine.startup_recovery_reports:
        print(
            json.dumps(
                {"startup_recovery_reports": engine.startup_recovery_reports},
                ensure_ascii=False,
            ),
            flush=True,
        )
    try:
        server.serve_forever(poll_interval=0.2)
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
    return 0


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None:
        return default
    try:
        value = int(raw)
    except ValueError:
        raise SystemExit(f"{name} must be an integer") from None
    if value <= 0:
        raise SystemExit(f"{name} must be positive")
    return value


def _env_float(name: str, default: float) -> float:
    raw = os.environ.get(name)
    if raw is None:
        return default
    try:
        value = float(raw)
    except ValueError:
        raise SystemExit(f"{name} must be a number") from None
    if value <= 0:
        raise SystemExit(f"{name} must be positive")
    return value


if __name__ == "__main__":
    raise SystemExit(main())

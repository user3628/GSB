from __future__ import annotations

import json
import os
import subprocess
import sys
import warnings
from pathlib import Path
from typing import Any

warnings.filterwarnings(
    "ignore", message="Using `httpx` with `starlette.testclient` is deprecated"
)

from fastapi.testclient import TestClient

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from pdb_engine.api import create_app  # noqa: E402

SAMPLE = PROJECT_ROOT / "samples" / "scenario.json"
OUTPUT_DIR = PROJECT_ROOT / "demo_output"


def _print_json(title: str, value: Any) -> None:
    print(f"\n=== {title} ===")
    print(json.dumps(value, ensure_ascii=False, indent=2))


def _remove_sqlite_files(path: Path) -> None:
    for suffix in ("", "-wal", "-shm"):
        candidate = Path(f"{path}{suffix}")
        if candidate.exists():
            candidate.unlink()


def run_main_demo() -> dict[str, Any]:
    output: dict[str, Any] = {}
    db_path = OUTPUT_DIR / "demo.sqlite3"
    _remove_sqlite_files(db_path)
    application = create_app(db_path)
    with TestClient(application) as client:
        load = client.post(
            "/api/v1/samples/load", json={"sample_path": str(SAMPLE)}
        )
        output["sample_load"] = {"status": load.status_code, "body": load.json()}
        _print_json("SAMPLE LOAD", load.json())

        blocked = client.post(
            "/api/v1/evictions/check",
            json={"pod": "ledger-0", "request_id": "single-ledger-blocked"},
        )
        output["single_blocked"] = {
            "status": blocked.status_code,
            "body": blocked.json(),
        }
        _print_json("SINGLE POD BLOCKED", blocked.json())

        no_node = client.post(
            "/api/v1/evictions/check",
            json={"pod": "web-b-offsite", "request_id": "single-no-node"},
        )
        output["single_no_feasible_node"] = {
            "status": no_node.status_code,
            "body": no_node.json(),
        }
        _print_json("SINGLE POD PLACEMENT BLOCKED", no_node.json())

        drain = client.post(
            "/api/v1/drains",
            json={"node": "worker-a", "request_id": "drain-worker-a-demo"},
        )
        drain_body = drain.json()
        output["drain"] = {"status": drain.status_code, "body": drain_body}
        _print_json(
            "NODE DRAIN PARTIAL RELEASE",
            {
                "status": drain.status_code,
                "release_summary": drain_body.get("release_summary"),
                "optimization": drain_body.get("optimization"),
                "allowed": drain_body.get("allowed"),
                "blocked": drain_body.get("blocked"),
                "actual_sequence": drain_body.get("actual_sequence"),
            },
        )

        detail = client.get("/api/v1/drains/drain-worker-a-demo")
        output["drain_detail"] = {
            "status": detail.status_code,
            "body": detail.json(),
        }
        replay = client.post(
            "/api/v1/drains",
            json={"node": "worker-a", "request_id": "drain-worker-a-demo"},
        )
        output["idempotent_replay"] = {
            "status": replay.status_code,
            "body": replay.json(),
        }
        _print_json(
            "IDEMPOTENT REPLAY",
            {
                "status": replay.status_code,
                "replayed": replay.json().get("replayed"),
                "release_summary": replay.json().get("release_summary"),
            },
        )
        missing = client.get("/api/v1/drains/does-not-exist")
        output["drain_not_found"] = {
            "status": missing.status_code,
            "body": missing.json(),
        }
        _print_json("DRAIN NOT FOUND", missing.json())
    return output


def run_crash_recovery_demo() -> dict[str, Any]:
    output: dict[str, Any] = {}
    db_path = OUTPUT_DIR / "crash-demo.sqlite3"
    _remove_sqlite_files(db_path)
    command = [
        sys.executable,
        "-m",
        "scripts.crash_worker",
        "--db",
        str(db_path),
        "--sample",
        str(SAMPLE),
        "--node",
        "worker-a",
        "--request-id",
        "crash-drain-demo",
        "--after-steps",
        "1",
    ]
    environment = dict(os.environ)
    environment["PYTHONPATH"] = str(PROJECT_ROOT)
    child = subprocess.run(
        command,
        cwd=PROJECT_ROOT,
        env=environment,
        capture_output=True,
        text=True,
        check=False,
    )
    output["crash_worker"] = {
        "command": command,
        "exit_code": child.returncode,
        "stdout": child.stdout,
        "stderr": child.stderr,
    }
    _print_json(
        "FORCED TERMINATION",
        {
            "exit_code": child.returncode,
            "stdout": child.stdout.strip(),
            "stderr": child.stderr.strip(),
        },
    )
    if child.returncode != 97:
        raise RuntimeError("crash worker did not terminate with the expected exit code 97")

    application = create_app(db_path)
    with TestClient(application) as client:
        recovery = client.get("/api/v1/recovery/report")
        output["recovery_report"] = {
            "status": recovery.status_code,
            "body": recovery.json(),
        }
        _print_json("RECOVERY REPORT", recovery.json())
        drain = client.get("/api/v1/drains/crash-drain-demo")
        output["aborted_drain"] = {
            "status": drain.status_code,
            "body": drain.json(),
        }
        _print_json(
            "ABORTED DRAIN FACTS",
            {
                "status": drain.status_code,
                "drain_status": drain.json().get("status"),
                "committed_steps": drain.json().get("steps"),
                "recovery_hint": drain.json().get("recovery_hint"),
            },
        )
    return output


def main() -> int:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    result: dict[str, Any] = {
        "environment": {
            "python": sys.version,
            "sqlite3": __import__("sqlite3").sqlite_version,
        }
    }
    result["main"] = run_main_demo()
    result["crash_recovery"] = run_crash_recovery_demo()

    drain_summary = result["main"]["drain"]["body"]["release_summary"]
    report = result["crash_recovery"]["recovery_report"]["body"]
    assertions = {
        "single_blocked_code": result["main"]["single_blocked"]["body"]["error"]["code"]
        == "E_PDB_BLOCKED",
        "no_feasible_node_code": result["main"]["single_no_feasible_node"]["body"]["error"][
            "code"
        ]
        == "E_NO_FEASIBLE_NODE",
        "naive_allowed": drain_summary["naive_allowed"] == 1,
        "optimized_not_less_than_naive": drain_summary["optimized_not_less_than_naive"],
        "actual_allowed": drain_summary["actual_allowed"] == 2,
        "recovery_available": report["available"] is True,
        "recovery_aborted": (
            report["report"]["aborted_drains"][0]["status_after"] == "ABORTED"
        ),
    }
    result["assertions"] = assertions
    _print_json("DEMO ASSERTIONS", assertions)
    output_path = OUTPUT_DIR / "demo_result.json"
    output_path.write_text(
        json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    print(f"\nFull demo output written to {output_path}")
    if not all(assertions.values()):
        print("DEMO FAILED", file=sys.stderr)
        return 1
    print("DEMO PASSED")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


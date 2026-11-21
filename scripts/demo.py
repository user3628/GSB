"""End-to-end local HTTP demo for the DeployLens workbench."""

from __future__ import annotations

import argparse
import json
import os
import socket
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

import httpx


ROOT = Path(__file__).resolve().parent.parent


def free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def load_sample(name: str) -> dict[str, Any]:
    return json.loads((ROOT / "samples" / f"{name}.json").read_text(encoding="utf-8"))


def pretty(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True)


def wait_ready(client: httpx.Client, process: subprocess.Popen[str]) -> None:
    deadline = time.monotonic() + 20
    while time.monotonic() < deadline:
        if process.poll() is not None:
            stdout, stderr = process.communicate(timeout=2)
            raise RuntimeError(f"uvicorn exited early\nstdout:\n{stdout}\nstderr:\n{stderr}")
        try:
            response = client.get("/api/health", timeout=0.8)
            if response.status_code == 200:
                return
        except httpx.HTTPError:
            pass
        time.sleep(0.15)
    raise RuntimeError("local uvicorn server did not become ready")


def must_ok(response: httpx.Response) -> dict[str, Any]:
    try:
        body = response.json()
    except ValueError as exc:
        raise RuntimeError(f"non-JSON response HTTP {response.status_code}: {response.text}") from exc
    if response.status_code >= 400 or not body.get("ok"):
        raise RuntimeError(f"HTTP {response.status_code}: {pretty(body)}")
    return body["data"]


def main() -> None:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")
    parser = argparse.ArgumentParser(description="Run the local DeployLens demo")
    parser.add_argument("--port", type=int, default=0, help="0 chooses a free loopback port")
    parser.add_argument("--db", default=str(ROOT / "data" / "demo.db"))
    args = parser.parse_args()

    db_path = Path(args.db).resolve()
    for suffix in ("", "-wal", "-shm"):
        Path(str(db_path) + suffix).unlink(missing_ok=True)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    port = args.port or free_port()
    env = os.environ.copy()
    env["DEPLOYLENS_DB"] = str(db_path)
    env.setdefault("PYTHONUTF8", "1")
    process = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "uvicorn",
            "app.main:app",
            "--host",
            "127.0.0.1",
            "--port",
            str(port),
            "--log-level",
            "warning",
        ],
        cwd=ROOT,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    try:
        with httpx.Client(base_url=f"http://127.0.0.1:{port}", timeout=10.0) as client:
            wait_ready(client, process)
            print(f"EVENT local_http_ready http://127.0.0.1:{port} db={db_path}")

            print("\n=== 1. 全量快照 v1 ===")
            v1 = must_ok(client.post("/api/commits/full", json=load_sample("snapshot_v1")))
            print(
                pretty(
                    {
                        "snapshot_version": v1["snapshot_version"],
                        "scores": v1["scores"],
                        "artifacts_rebuilt": v1["artifacts_rebuilt"],
                        "references_written": v1["references_written"],
                    }
                )
            )

            print("\n=== 2. 全量快照 v2（整体替换） ===")
            v2 = must_ok(client.post("/api/commits/full", json=load_sample("snapshot_v2")))
            print(
                pretty(
                    {
                        "snapshot_version": v2["snapshot_version"],
                        "environments": v2["environments"],
                    }
                )
            )

            print("\n=== 3. 单服务镜像增量（prod/inventory） ===")
            increment = must_ok(
                client.post(
                    "/api/commits/incremental", json=load_sample("increment_image")
                )
            )
            print(
                pretty(
                    {
                        "snapshot_version": increment["snapshot_version"],
                        "changed_services": increment["affected"]["changed_services"],
                        "dependent_services": increment["affected"]["dependent_services"],
                        "affected_services": increment["affected"]["affected_services"],
                        "invalidated_artifacts": increment["affected"][
                            "invalidated_artifacts"
                        ],
                    }
                )
            )
            print("CASCADE TRACE")
            for event in increment["cascade_trace"]:
                print(
                    f"  #{event['seq']:02d} {event['environment']} "
                    f"{event['subject_type']}:{event['subject_key']} -> "
                    f"{event['artifact_type']}:{event['artifact_key']} "
                    f"[{event['reason']}]"
                )

            print("\n=== 4. 跨环境差异清单 ===")
            diagnostics = must_ok(client.get("/api/diagnostics", params={"version": 3}))
            print(pretty({"counts": diagnostics["counts"], "lag": diagnostics["environment_lag_markers"]}))
            for item in diagnostics["items"]:
                print(
                    f"  [{item['severity']:8}] {item['environment']:7} "
                    f"{item['service'] or '<environment>':14} {item['diff_type']:26} "
                    f"{item['left']['value']!r} != {item['right']['value']!r}"
                )

            print("\n=== 5. 注入依赖环（预期拒绝且不污染状态） ===")
            cycle_response = client.post(
                "/api/commits/incremental", json=load_sample("cycle_increment")
            )
            cycle_body = cycle_response.json()
            print(pretty(cycle_body))
            if cycle_response.status_code != 409 or cycle_body["error"]["code"] != "E_DEPENDENCY_CYCLE":
                raise RuntimeError("cycle injection did not return E_DEPENDENCY_CYCLE")

            print("\n=== 6. 最终状态摘要 ===")
            environments = must_ok(client.get("/api/environments"))
            bundles = {
                environment["environment"]: must_ok(
                    client.get(f"/api/environments/{environment['environment']}/bundle")
                )
                for environment in environments["environments"]
            }
            print(
                pretty(
                    {
                        "snapshot_version": environments["snapshot_version"],
                        "environments": [
                            {
                                **environment,
                                "layers": bundles[environment["environment"]]["topology"]["layers"],
                                "state_hash": bundles[environment["environment"]]["state_hash"],
                            }
                            for environment in environments["environments"]
                        ],
                    }
                )
            )

            print("\n=== 7. 恢复报告（本次正常退出，无待恢复意图） ===")
            recovery = must_ok(client.get("/api/recovery"))
            print(
                pretty(
                    {
                        "current_version": recovery["current_version"],
                        "pending_intents": recovery["pending_intents"],
                        "exit_report_guaranteed": recovery["exit_report_guaranteed"],
                        "statement": recovery["statement"],
                    }
                )
            )
            print("\nDEMO OK")
    finally:
        process.terminate()
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=5)


if __name__ == "__main__":
    main()

"""独立工作进程入口：用于强制终止恢复演示。

由演示脚本以子进程方式启动，执行指定快照版本的分发；父进程在分发途中
发送 SIGKILL/SIGTERM，模拟进程被强制终止（Windows 下对应 terminate()）。
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

from .coordinator import Coordinator
from .endpoint_server import EndpointCluster, load_endpoint_script
from .push_client import PushClient
from .repository import Repository


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="分发工作进程")
    parser.add_argument("--db", required=True)
    parser.add_argument("--components", required=True)
    parser.add_argument("--endpoint-script", required=True)
    parser.add_argument("--version", required=True, type=int)
    parser.add_argument("--policy", default="rollback", choices=["continue", "rollback"])
    parser.add_argument("--timeout", default=2.0, type=float)
    args = parser.parse_args(argv)

    declarations_raw = json.loads(Path(args.components).read_text(encoding="utf-8"))
    declarations = {item["component_id"]: item for item in declarations_raw}
    overrides = load_endpoint_script(args.endpoint_script)
    effective = {}
    for component_id, raw in declarations.items():
        item = dict(raw)
        override = overrides.get(component_id, {})
        if "endpoint_override" in override:
            item["endpoint"] = override["endpoint_override"]
        effective[component_id] = item

    repo = Repository(args.db)
    coordinator = Coordinator(repo, PushClient(timeout_seconds=args.timeout))
    cluster = EndpointCluster(effective, overrides)
    cluster.start()
    try:
        # 让父进程有时间观察到在途状态
        time.sleep(0.3)
        run = coordinator.dispatch(args.version, failure_policy=args.policy)
        print(json.dumps({"run": run}, ensure_ascii=False), flush=True)
    finally:
        cluster.stop()
        repo.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

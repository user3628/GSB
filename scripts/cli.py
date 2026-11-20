"""命令行诊断入口：不经过 HTTP 直接驱动调度服务。"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from gpworkbench.database import Database  # noqa: E402
from gpworkbench.errors import BusinessError  # noqa: E402
from gpworkbench.scheduler import SchedulerService  # noqa: E402
from gpworkbench.spec import load_workload_file  # noqa: E402


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="GPU 配额调度诊断工作台 CLI")
    parser.add_argument("--db", default="data/gpu_workbench.sqlite3")
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("init-db")

    load = sub.add_parser("load")
    load.add_argument("sample_path")

    sub.add_parser("quota")
    sub.add_parser("nodes")

    reqs = sub.add_parser("requests")
    reqs.add_argument("--state")
    reqs.add_argument("--group")

    admit = sub.add_parser("admit")
    admit.add_argument("request_id")

    reject = sub.add_parser("reject")
    reject.add_argument("request_id")

    batch = sub.add_parser("batch")
    batch.add_argument("--group")
    batch.add_argument("--strategy", choices=["GREEDY_FF", "MEM_DESC_BF"])

    reorg = sub.add_parser("reorg")
    reorg.add_argument("group_id")

    start = sub.add_parser("start-batch")
    start.add_argument("batch_id")

    complete = sub.add_parser("complete-batch")
    complete.add_argument("batch_id")

    preempt = sub.add_parser("preempt")
    preempt.add_argument("request_id")

    transition = sub.add_parser("transition")
    transition.add_argument("request_id")
    transition.add_argument("to_state")
    transition.add_argument("reason")

    sub.add_parser("events")
    sub.add_parser("intents")
    sub.add_parser("recover")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    service = SchedulerService(Database(args.db))
    try:
        if args.command == "init-db":
            result = {"database": args.db, "initialized": True}
        elif args.command == "load":
            spec = load_workload_file(args.sample_path)
            result = service.load_spec(spec)
        elif args.command == "quota":
            result = service.quota_snapshot()
        elif args.command == "nodes":
            result = service.node_snapshot()
        elif args.command == "requests":
            result = service.repo.list_requests(state=args.state, group_id=args.group)
        elif args.command == "admit":
            result = service.admit_request(args.request_id)
        elif args.command == "reject":
            result = service.reject_request(args.request_id)
        elif args.command == "batch":
            result = service.run_batching(args.group, strategy=args.strategy)
        elif args.command == "reorg":
            result = service.reorganize_batches(args.group_id)
        elif args.command == "start-batch":
            result = service.start_batch(args.batch_id)
        elif args.command == "complete-batch":
            result = service.complete_batch(args.batch_id)
        elif args.command == "preempt":
            result = service.preempt_for(args.request_id)
        elif args.command == "transition":
            result = service.transition(args.request_id, args.to_state, args.reason)
        elif args.command == "events":
            result = service.repo.list_events()
        elif args.command == "intents":
            result = service.repo.list_pending_intents()
        elif args.command == "recover":
            result = service.recover()
        else:  # pragma: no cover
            return 2
    except BusinessError as exc:
        print(json.dumps({"error": exc.to_dict()}, ensure_ascii=False, indent=2))
        return 1
    print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

"""命令行入口：python -m graphstore --db data/graph.db --port 8088。"""

from __future__ import annotations

import argparse
import time

from .api import serve
from .engine import GraphEngine


def main():
    parser = argparse.ArgumentParser(description="本地图存储与查询组件")
    parser.add_argument("--db", default="data/graphstore.db")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8088)
    parser.add_argument("--snapshot", help="启动时加载的快照文件路径")
    args = parser.parse_args()

    engine = GraphEngine(args.db)
    report = engine.start()
    print("恢复报告:", report)
    if args.snapshot and engine.current_version is None:
        print("加载初始快照:", engine.load_snapshot(args.snapshot))
    server = serve(engine, args.host, args.port)
    print(f"HTTP API 已启动: http://{args.host}:{args.port}")
    try:
        while True:
            time.sleep(3600)
    except KeyboardInterrupt:
        server.shutdown()
        engine.close()


if __name__ == "__main__":
    main()

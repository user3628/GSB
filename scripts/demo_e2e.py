"""端到端演示：HTTP API + 双模式 + 版本绑定 + 增量更新 + 强制终止恢复。

用法::

    python -m scripts.demo_e2e            # 使用临时数据库跑完整流程
    python -m scripts.demo_e2e --port 8090 --db data/demo.db

脚本只监听 127.0.0.1，样本全部来自本地 samples/ 目录。
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import tempfile
import urllib.error
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from graphstore.analytics import degree_stats  # noqa: E402
from graphstore.api import serve  # noqa: E402
from graphstore.engine import GraphEngine  # noqa: E402


def call(base_url: str, path: str, payload: dict | None = None):
    data = json.dumps(payload or {}).encode("utf-8")
    request = urllib.request.Request(
        base_url + path, data=data,
        headers={"Content-Type": "application/json"}, method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=10) as response:
            return response.status, json.loads(response.read())
    except urllib.error.HTTPError as exc:
        return exc.code, json.loads(exc.read())


def heading(title: str):
    print("\n" + "=" * 72)
    print(title)
    print("=" * 72)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=8090)
    parser.add_argument("--db", default=None)
    args = parser.parse_args()

    workdir = Path(args.db).parent if args.db else Path(tempfile.mkdtemp(prefix="graph-demo-"))
    workdir.mkdir(parents=True, exist_ok=True)
    db_path = args.db or str(workdir / "demo.db")

    engine = GraphEngine(db_path)
    startup_report = engine.start()
    server = serve(engine, "127.0.0.1", args.port)
    base_url = f"http://127.0.0.1:{args.port}"
    print(f"服务已启动: {base_url}  数据库: {db_path}")
    print("启动恢复报告:", json.dumps(startup_report, ensure_ascii=False))

    try:
        heading("1. 加载快照 v1（含版本号与 SHA-256 摘要校验）")
        status, body = call(base_url, "/snapshots/load",
                            {"path": str(ROOT / "samples/snapshot_v1.json")})
        print(status, json.dumps(body, ensure_ascii=False))

        heading("2. 加载坏摘要快照，预期 E_SNAPSHOT_INVALID 且旧版本保持可用")
        status, body = call(base_url, "/snapshots/load",
                            {"path": str(ROOT / "samples/snapshot_bad_checksum.json")})
        print(status, json.dumps(body, ensure_ascii=False))
        status, health = call(base_url, "/health") if False else (None, None)
        with urllib.request.urlopen(base_url + "/health") as response:
            print("当前版本仍为:", json.loads(response.read())["version"])

        heading("3. 多跳遍历：精确模式 vs 快速模式（逐项差异核对）")
        status, audit = call(base_url, "/query/audit", {
            "query": "multi_hop", "version": 1,
            "fast_budget": {"node_budget": 5, "prefetch_budget": 3},
            "params": {"start": "p1", "max_hops": 2},
        })
        diff = audit["data"]["diff"]
        print("快速是精确的子集:", diff["fast_subset_of_exact"])
        print("快速缺失节点:", diff["missing_in_fast"])
        print("快速虚构节点（应为空）:", diff["unexpected_in_fast"])
        print("被截断分支数:", diff["fast_truncated_branches"])

        heading("4. 最短路径（确定性 Dijkstra）")
        status, shortest = call(base_url, "/query", {
            "request_id": "demo-shortest", "query": "shortest_path",
            "version": 1, "mode": "exact",
            "params": {"source": "p1", "target": "p4"},
        })
        print(json.dumps(shortest["data"], ensure_ascii=False, indent=2))

        heading("5. 模式匹配：Person -KNOWS-> Person -LIVES_IN-> City")
        status, pattern = call(base_url, "/query", {
            "query": "pattern_match", "version": 1, "mode": "exact",
            "params": {"start": "p1", "pattern": [
                {"edge_type": "KNOWS"},
                {"edge_type": "LIVES_IN", "target_label": "City"}]},
        })
        for item in pattern["data"]["matched_paths"]:
            print("路径:", " -> ".join(item["nodes"]))

        heading("6. 版本绑定：v1 查询在版本切换期间固定在 v1，退役版本显式报错")
        status, old = call(base_url, "/query", {
            "query": "multi_hop", "version": 1, "mode": "exact",
            "params": {"start": "p1", "max_hops": 1},
        })
        print("绑定 v1 的查询返回版本:", old["data"]["version"])
        status, retired = call(base_url, "/query", {
            "query": "multi_hop", "version": 99, "mode": "exact",
            "params": {"start": "p1", "max_hops": 1},
        })
        print("退役版本错误码:", retired["error"]["code"])

        heading("7. 增量更新：新增节点 p5 与邻接边（只触碰受影响索引键）")
        status, delta = call(base_url, "/updates", {
            "add_nodes": [{"id": "p5", "label": "Person",
                           "properties": {"name": "Eve"}}],
            "add_edges": [
                {"source": "p5", "target": "p1", "type": "KNOWS", "weight": 1.0},
                {"source": "p5", "target": "x1", "type": "PURCHASED", "weight": 2.0},
            ],
            "generated_at": "2026-11-23T14:00:00+08:00",
        })
        print(json.dumps(delta["data"], ensure_ascii=False, indent=2))
        with urllib.request.urlopen(base_url + "/version") as response:
            print("当前版本:", json.loads(response.read()))

        heading("8. 同请求标识去重（重复提交复用持久化结果）")
        payload = {"request_id": "demo-dup", "query": "multi_hop", "version": 2,
                   "mode": "exact", "params": {"start": "p5", "max_hops": 1}}
        call(base_url, "/query", payload)
        status, replay = call(base_url, "/query", payload)
        print("复用方式:", replay["data"]["dedup"]["reused"])

        heading("9. NumPy 图统计")
        view = engine.registry.acquire(engine.current_version)
        try:
            print(json.dumps(degree_stats(view.snapshot), ensure_ascii=False, indent=2))
        finally:
            engine.registry.release(view)

        heading("10. 完整性交叉校验（图数据 vs 持久化索引）")
        status, verify = call(base_url, "/verify", {})
        print(json.dumps(verify["data"], ensure_ascii=False))
    finally:
        server.shutdown()
        engine.close()

    heading("11. 强制终止恢复：子进程在意图写入后 os._exit(137)")
    worker = workdir / "crash_worker.py"
    worker.write_text(
        "import sys, os\n"
        f"sys.path.insert(0, {str(ROOT)!r})\n"
        f"from graphstore.engine import GraphEngine\n"
        f"eng = GraphEngine({db_path!r})\n"
        "eng.start()\n"
        "eng.crash_hook = lambda point: os._exit(137) if point == 'after_delta_intent' else None\n"
        "eng.apply_delta({'add_nodes': [{'id': 'ghost', 'label': 'Person', "
        "'properties': {}}], 'generated_at': 't'})\n"
    )
    killed = subprocess.run([sys.executable, str(worker)], capture_output=True)
    print("被强杀子进程退出码:", killed.returncode)

    restarted = GraphEngine(db_path)
    report = restarted.start()
    print(json.dumps(report, ensure_ascii=False, indent=2))
    integrity = restarted.verify_integrity()
    print("恢复后完整性:", integrity)
    view = restarted.registry.acquire(restarted.current_version)
    try:
        print("ghost 节点是否残留:", "ghost" in view.snapshot.nodes)
    finally:
        restarted.registry.release(view)
    restarted.close()
    print("\n演示完成。")


if __name__ == "__main__":
    main()

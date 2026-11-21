"""本地结果验证工具。

同一固定样本上执行：
  A) 不间断分页拉取（uvicorn 子进程全程在线）
  B) 中途强制终止（SIGKILL）后重启恢复，继续拉取
比对两次的页序列逐页逐行一致，并与不分页一次取尽的真值逐行一致；
同时核验统一错误码与预算裁决轨迹。

用法: .venv/bin/python tools/verify.py [--keep]
"""
from __future__ import annotations

import argparse
import json
import os
import shutil
import signal
import socket
import subprocess
import sys
import time

import httpx

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
PY = os.path.join(ROOT, ".venv", "bin", "python")
SAMPLE = os.path.join(ROOT, "artifacts", "sample")
WORK = os.path.join(ROOT, "artifacts", "verify_run")

# 固定点查请求：跨多块、含同主键多行、含不存在主键
QUERY_KEYS = [0, 7, 35, 71, 1005, 1012, 2000, 3500, 5005, 7000, 7007,
              9000, 11000, 11007, 99999]
PAGE_SIZE = 4


def free_port() -> int:
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


def start_server(db: str, extra_config: dict | None = None) -> tuple[subprocess.Popen, int]:
    port = free_port()
    config = {"first_page_deadline_sec": 10.0}
    if extra_config:
        config.update(extra_config)
    cmd = [PY, "-m", "pkq.app", "--db", db, "--snapshot",
           os.path.join(SAMPLE, "snapshot_v1"), "--port", str(port),
           "--config-json", json.dumps(config)]
    proc = subprocess.Popen(cmd, cwd=ROOT, stdout=subprocess.PIPE,
                            stderr=subprocess.STDOUT, text=True)
    for _ in range(200):
        try:
            httpx.get(f"http://127.0.0.1:{port}/config", timeout=0.3)
            return proc, port
        except Exception:
            time.sleep(0.05)
    raise RuntimeError("服务启动失败")


def pull_all(port: int, handle: str, start_seq: int, first_page: dict | None,
             sink: list[dict]) -> int:
    """从 start_seq 续拉直到 EXHAUSTED，页追加到 sink。返回页数。"""
    if first_page:
        sink.append(first_page)
    seq = start_seq
    while True:
        r = httpx.post(f"http://127.0.0.1:{port}/pull",
                       json={"handle": handle, "page_seq": seq}, timeout=10)
        body = r.json()
        if r.status_code == 200 and body["status"] == "OK":
            sink.append({"page_seq": body["page_seq"], "rows": body["rows"]})
            seq += 1
            continue
        if r.status_code == 200 and body["status"] == "NOT_READY":
            time.sleep(0.05)
            continue
        if r.status_code == 200 and body["status"] == "EXHAUSTED":
            return seq
        raise RuntimeError(f"拉取失败: {r.status_code} {body}")


def ground_truth() -> list[dict]:
    """不分页一次取尽的真值：直接解码快照、按键过滤、主键升序稳定归并。"""
    from pkq.columnar import SnapshotStore
    snaps = SnapshotStore()
    snap = snaps.load(os.path.join(SAMPLE, "snapshot_v1"))
    keyset = set(QUERY_KEYS)
    rows = []
    for entry in snap.blocks:
        blk = snap.decode_block(entry)
        for i, pk in enumerate(blk.pks):
            if pk in keyset:
                rows.append({"pk": pk, "val_a": blk.columns["val_a"][i],
                             "val_b": blk.columns["val_b"][i],
                             "name": blk.columns["name"][i]})
    rows.sort(key=lambda r: r["pk"])
    return rows


def scenario_uninterrupted(db: str) -> tuple[list[dict], list[dict]]:
    proc, port = start_server(db)
    try:
        r = httpx.post(f"http://127.0.0.1:{port}/query",
                       json={"query_id": "run-a", "keys": QUERY_KEYS,
                             "page_size": PAGE_SIZE}, timeout=15)
        assert r.status_code == 200, r.text
        body = r.json()
        pages: list[dict] = []
        pull_all(port, body["handle"], 1, body.get("first_page"), pages)
        trace = httpx.get(f"http://127.0.0.1:{port}/sessions/{body['handle']}/trace",
                          timeout=10).json()["trace"]
        return pages, trace
    finally:
        proc.terminate()
        proc.wait(timeout=10)


def scenario_kill_recover(db: str, kill_after_pages: int = 2) -> tuple[list[dict], dict, list[dict]]:
    proc, port = start_server(db, extra_config={"task_commit_delay_sec": 0.4})
    pages: list[dict] = []
    r = httpx.post(f"http://127.0.0.1:{port}/query",
                   json={"query_id": "run-b", "keys": QUERY_KEYS,
                         "page_size": PAGE_SIZE}, timeout=15)
    assert r.status_code == 200, r.text
    body = r.json()
    handle = body["handle"]
    if body.get("first_page"):
        pages.append(body["first_page"])
    # 拉到 kill_after_pages 页后强制终止（物化因延迟钩子仍在进行）
    seq = len(pages)
    while len(pages) < kill_after_pages:
        r = httpx.post(f"http://127.0.0.1:{port}/pull",
                       json={"handle": handle, "page_seq": seq}, timeout=10)
        b = r.json()
        if b["status"] == "OK":
            pages.append({"page_seq": b["page_seq"], "rows": b["rows"]})
            seq += 1
        elif b["status"] == "NOT_READY":
            time.sleep(0.05)
        else:
            raise RuntimeError(f"意外状态: {b}")
    proc.send_signal(signal.SIGKILL)  # 强制终止
    proc.wait(timeout=10)

    # 重启：同一 SQLite 目录与快照，恢复后继续拉取
    proc2, port2 = start_server(db)
    try:
        report = httpx.get(f"http://127.0.0.1:{port2}/recovery-report", timeout=10).json()
        pull_all(port2, handle, seq, None, pages)
        trace = httpx.get(f"http://127.0.0.1:{port2}/sessions/{handle}/trace",
                          timeout=10).json()["trace"]
        return pages, report, trace
    finally:
        proc2.terminate()
        proc2.wait(timeout=10)


def check_error_codes(db: str) -> dict:
    """核验统一错误码 JSON 响应。"""
    results: dict[str, str] = {}
    proc, port = start_server(db)
    try:
        base = f"http://127.0.0.1:{port}"
        # E_QUERY_TOO_LARGE
        r = httpx.post(f"{base}/query", json={"query_id": "too-large",
                                              "keys": list(range(200))}, timeout=10)
        results["E_QUERY_TOO_LARGE"] = r.json()["error"]["code"] if r.status_code == 413 else f"FAIL {r.text}"
        # E_CONFLICTING_QUERY
        httpx.post(f"{base}/query", json={"query_id": "dup", "keys": [1, 2]}, timeout=10)
        r = httpx.post(f"{base}/query", json={"query_id": "dup", "keys": [3, 4]}, timeout=10)
        results["E_CONFLICTING_QUERY"] = r.json()["error"]["code"] if r.status_code == 409 else f"FAIL {r.text}"
        # 重复提交同内容 -> 复用同一句柄
        r1 = httpx.post(f"{base}/query", json={"query_id": "dup", "keys": [1, 2]}, timeout=10)
        results["idempotent_reuse"] = "OK" if r1.status_code == 200 and r1.json()["reused"] else f"FAIL {r1.text}"
        # E_PAGE_OUT_OF_RANGE
        r = httpx.post(f"{base}/query", json={"query_id": "empty-q", "keys": []}, timeout=10)
        h = r.json()["handle"]
        results["empty_keys_handle"] = "OK" if r.json()["status"] == "EXHAUSTED" else "FAIL"
        r = httpx.post(f"{base}/pull", json={"handle": h, "page_seq": 5}, timeout=10)
        results["E_PAGE_OUT_OF_RANGE"] = r.json()["error"]["code"] if r.status_code == 416 else f"FAIL {r.text}"
        # E_SNAPSHOT_INVALID（版本回退与摘要篡改）
        r = httpx.post(f"{base}/snapshots/load",
                       json={"root": os.path.join(SAMPLE, "snapshot_v1")}, timeout=10)
        results["E_SNAPSHOT_INVALID(version)"] = r.json()["error"]["code"] if r.status_code == 409 else f"FAIL {r.text}"
        r = httpx.post(f"{base}/snapshots/load",
                       json={"root": os.path.join(SAMPLE, "snapshot_badsum")}, timeout=10)
        results["E_SNAPSHOT_INVALID(checksum)"] = r.json()["error"]["code"] if r.status_code == 409 else f"FAIL {r.text}"
        cur = httpx.get(f"{base}/snapshots/current", timeout=10).json()
        results["old_snapshot_kept"] = "OK" if cur["version"] == 1 else f"FAIL {cur}"
        # E_BLOCK_CORRUPT
        r = httpx.post(f"{base}/snapshots/load",
                       json={"root": os.path.join(SAMPLE, "snapshot_corrupt")}, timeout=10)
        assert r.status_code == 200, r.text
        r = httpx.post(f"{base}/query", json={"query_id": "corrupt-q",
                                              "keys": [5005, 5006]}, timeout=10)
        h = r.json()["handle"]
        for _ in range(300):
            st = httpx.get(f"{base}/sessions/{h}", timeout=10).json()
            if st["status"] == "FAILED":
                break
            time.sleep(0.05)
        r = httpx.post(f"{base}/pull", json={"handle": h, "page_seq": 0}, timeout=10)
        body = r.json()
        ok = r.status_code == 422 and body["error"]["code"] == "E_BLOCK_CORRUPT" \
            and body["error"]["details"]["block_id"] == 5
        results["E_BLOCK_CORRUPT"] = "OK(block_id=5)" if ok else f"FAIL {r.status_code} {body}"
        # E_SCAN_QUEUE_FULL：小队列容量配置下的大切分查询
    finally:
        proc.terminate()
        proc.wait(timeout=10)
    proc, port = start_server(db + ".q", extra_config={"scan_queue_capacity": 1,
                                                       "max_concurrent_tasks": 1})
    try:
        base = f"http://127.0.0.1:{port}"
        keys = [b * 1000 + 5 for b in range(0, 12, 2)]  # 命中 6 个不相邻块 -> 6 任务
        r = httpx.post(f"{base}/query", json={"query_id": "big-scan", "keys": keys}, timeout=10)
        results["E_SCAN_QUEUE_FULL"] = r.json()["error"]["code"] if r.status_code == 429 else f"FAIL {r.text}"
    finally:
        proc.terminate()
        proc.wait(timeout=10)
    return results


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--keep", action="store_true")
    args = parser.parse_args()

    if os.path.exists(WORK):
        shutil.rmtree(WORK)
    os.makedirs(WORK)
    if not os.path.exists(os.path.join(SAMPLE, "snapshot_v1")):
        subprocess.check_call([PY, os.path.join(ROOT, "tools", "gen_sample.py"), SAMPLE])

    print("== 真值计算（不分页一次取尽） ==")
    truth = ground_truth()
    print(f"真值行数: {len(truth)}")

    print("== 场景 A：不间断分页拉取 ==")
    pages_a, trace_a = scenario_uninterrupted(os.path.join(WORK, "a", "pkq.db"))
    rows_a = [r for p in pages_a for r in p["rows"]]
    print(f"页数: {len(pages_a)}, 行数: {len(rows_a)}")

    print("== 场景 B：强制终止后恢复续拉 ==")
    pages_b, report, trace_b = scenario_kill_recover(os.path.join(WORK, "b", "pkq.db"))
    rows_b = [r for p in pages_b for r in p["rows"]]
    print(f"页数: {len(pages_b)}, 行数: {len(rows_b)}")
    print("恢复报告:", json.dumps(report, ensure_ascii=False))

    print("== 逐页逐行比对 ==")
    assert len(pages_a) == len(pages_b), f"页数不一致 {len(pages_a)} vs {len(pages_b)}"
    for pa, pb in zip(pages_a, pages_b):
        assert pa["page_seq"] == pb["page_seq"], (pa["page_seq"], pb["page_seq"])
        assert pa["rows"] == pb["rows"], f"页 {pa['page_seq']} 行不一致"
    print(f"场景 A/B 页序列逐页逐行一致: {len(pages_a)} 页")
    assert rows_a == truth, "分页拼接结果与不分页真值不一致"
    print(f"分页拼接结果与不分页一次取尽逐行一致: {len(rows_a)} 行，不重不漏")

    print("== 统一错误码核验 ==")
    codes = check_error_codes(os.path.join(WORK, "c", "pkq.db"))
    for k, v in codes.items():
        print(f"  {k}: {v}")
    bad = [k for k, v in codes.items() if str(v).startswith("FAIL")]
    assert not bad, f"错误码核验失败: {bad}"

    out = {
        "query_keys": QUERY_KEYS, "page_size": PAGE_SIZE,
        "pages_scenario_a": pages_a, "pages_scenario_b": pages_b,
        "rows_total": len(rows_a),
        "decision_trace_uninterrupted": trace_a,
        "decision_trace_recovered": trace_b,
        "recovery_report": report,
        "error_code_checks": codes,
    }
    out_path = os.path.join(WORK, "verify_result.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)
    print(f"裁决轨迹与明细已写入 {out_path}")
    print("VERIFY OK")
    if not args.keep:
        shutil.rmtree(WORK, ignore_errors=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())

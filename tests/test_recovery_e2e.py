"""进程级强制终止恢复：物化中途 kill -> 重启 -> 续拉结果逐行一致。"""
import os
import signal
import socket
import subprocess
import sys
import time

import httpx
import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PY = sys.executable


def free_port():
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    p = s.getsockname()[1]
    s.close()
    return p


def start(db, snapshot, port, extra=None):
    cfg = {"first_page_deadline_sec": 10.0, "task_commit_delay_sec": 0.3}
    if extra:
        cfg.update(extra)
    import json
    proc = subprocess.Popen(
        [PY, "-m", "pkq.app", "--db", db, "--snapshot", snapshot,
         "--port", str(port), "--config-json", json.dumps(cfg)],
        cwd=ROOT, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    for _ in range(200):
        try:
            httpx.get(f"http://127.0.0.1:{port}/config", timeout=0.3)
            return proc
        except Exception:
            time.sleep(0.05)
    proc.kill()
    raise RuntimeError("启动失败")


def pull_loop(port, handle, seq, pages):
    while True:
        r = httpx.post(f"http://127.0.0.1:{port}/pull",
                       json={"handle": handle, "page_seq": seq}, timeout=10)
        b = r.json()
        if b["status"] == "OK":
            pages.append(b)
            seq += 1
        elif b["status"] == "NOT_READY":
            time.sleep(0.05)
        elif b["status"] == "EXHAUSTED":
            return seq


@pytest.mark.slow
def test_kill_and_recover(sample_root, tmp_path):
    db = str(tmp_path / "pkq.db")
    snap = os.path.join(sample_root, "snapshot_v1")
    keys = [0, 7, 35, 1005, 2000, 3500, 5005, 7000, 9000, 11000]
    port = free_port()
    proc = start(db, snap, port)
    base = f"http://127.0.0.1:{port}"
    r = httpx.post(f"{base}/query", json={"query_id": "k1", "keys": keys, "page_size": 3},
                   timeout=15)
    body = r.json()
    handle = body["handle"]
    pages = [body["first_page"]] if body.get("first_page") else []
    seq = len(pages)
    while len(pages) < 2:  # 拉两页后强制终止
        r = httpx.post(f"{base}/pull", json={"handle": handle, "page_seq": seq}, timeout=10)
        b = r.json()
        if b["status"] == "OK":
            pages.append(b)
            seq += 1
        else:
            time.sleep(0.05)
    proc.send_signal(signal.SIGKILL)
    proc.wait(timeout=10)

    port2 = free_port()
    proc2 = start(db, snap, port2)
    try:
        base2 = f"http://127.0.0.1:{port2}"
        report = httpx.get(f"{base2}/recovery-report", timeout=10).json()
        sess = report["sessions"][0]
        assert sess["reused_tasks"]  # 已提交任务复用不重算
        pull_loop(port2, handle, seq, pages)
    finally:
        proc2.terminate()
        proc2.wait(timeout=10)

    # 与不分页真值比对
    from pkq.columnar import SnapshotStore
    snaps = SnapshotStore()
    snapshot = snaps.load(snap)
    keyset = set(keys)
    truth = []
    for entry in snapshot.blocks:
        blk = snapshot.decode_block(entry)
        for i, pk in enumerate(blk.pks):
            if pk in keyset:
                truth.append({"pk": pk, "val_a": blk.columns["val_a"][i],
                              "val_b": blk.columns["val_b"][i],
                              "name": blk.columns["name"][i]})
    truth.sort(key=lambda r: r["pk"])
    rows = [row for p in pages for row in p["rows"]]
    assert rows == truth

"""端到端验收：格式互转 / 校准诊断一致性 / 异常码 / 停止恢复逐字节一致。"""
import json, os, signal, subprocess, sys, tempfile, time
from pathlib import Path

ROOT = Path("/home/zsl/feedback/uid37fecb20-target-mwnkf3ge-94")
os.chdir(ROOT)
ok = []

# 0. 数据再生成确定性
h1 = subprocess.run(["sha256sum"] + sorted(str(p) for p in Path("data/shards").glob("*.jsonl")),
                    capture_output=True, text=True).stdout
subprocess.run([sys.executable, "scripts/gen_data.py", "--out", "data"],
               check=True, capture_output=True)
h2 = subprocess.run(["sha256sum"] + sorted(str(p) for p in Path("data/shards").glob("*.jsonl")),
                    capture_output=True, text=True).stdout
ok.append(("固定种子再生成确定性", h1 == h2))

with tempfile.TemporaryDirectory() as d:
    # 1. 格式互转（两种版本往返链）
    r = subprocess.run([sys.executable, "-m", "evalbench", "roundtrip",
                        "--shard", "data/shards/shard_v1_a.jsonl",
                        "--config", "data/configs/config_v2.json"],
                       capture_output=True, text=True)
    rep = json.loads(r.stdout)
    t1 = r.returncode == 0 and rep["ok"] and rep["chains_verified"] == 60
    r = subprocess.run([sys.executable, "-m", "evalbench", "roundtrip",
                        "--shard", "data/shards/shard_v2_a.jsonl",
                        "--config", "data/configs/config_v2.json"],
                       capture_output=True, text=True)
    rep = json.loads(r.stdout)
    t1 = t1 and r.returncode == 0 and rep["ok"] and rep["chains_verified"] == 56
    ok.append(("v1/v2 往返链格式互转", t1))

    # 2. 校准与诊断一致性（v1 升级配置 vs 原生 v2 配置）
    ws1, ws2 = f"{d}/w1", f"{d}/w2"
    for ws, cfg in ((ws1, "data/configs/config_v1.json"), (ws2, "data/configs/config_v2.json")):
        subprocess.run([sys.executable, "-m", "evalbench", "run", "--data", "data/shards",
                        "--config", cfg, "--workspace", ws], check=True, capture_output=True)
    r1 = json.loads(Path(ws1, "exports/reliability_report.json").read_text())
    r2 = json.loads(Path(ws2, "exports/reliability_report.json").read_text())
    r1.pop("config_upgrade_notes"); r2.pop("config_upgrade_notes")
    ok.append(("v1 升级配置与 v2 配置诊断报告一致", r1 == r2))

    # 3. 异常码（注入样本，纯输入文件）
    def cli_code(data, cfg):
        r = subprocess.run([sys.executable, "-m", "evalbench", "run", "--data", data,
                            "--config", cfg, "--workspace", f"{d}/wx"],
                           capture_output=True, text=True)
        return r.returncode, json.loads(r.stderr)["error"]["code"]
    c1 = cli_code("data/inject", "data/configs/config_v2.json")
    c2 = cli_code("data/shards", "data/inject/config_out_of_range.json")
    c3 = cli_code("data/shards", "data/inject/config_unknown_alias.json")
    ok.append(("E_VERSION_MISMATCH", c1 == (2, "E_VERSION_MISMATCH")))
    ok.append(("E_CONFIG_INVALID(越界)", c2 == (2, "E_CONFIG_INVALID")))
    ok.append(("E_CONFIG_INVALID(未知别名)", c3 == (2, "E_CONFIG_INVALID")))

    # 4. 优雅停止恢复 == 完整运行（逐字节）
    ws_full, ws_stop = f"{d}/full", f"{d}/stop"
    subprocess.run([sys.executable, "-m", "evalbench", "run", "--data", "data/shards",
                    "--config", "data/configs/config_v2.json", "--workspace", ws_full],
                   check=True, capture_output=True)
    p = subprocess.Popen([sys.executable, "-m", "evalbench", "run", "--data", "data/shards",
                          "--config", "data/configs/config_v2.json", "--workspace", ws_stop,
                          "--unit-delay-ms", "200"],
                         stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    time.sleep(1.0)
    p.send_signal(signal.SIGTERM)
    out, _ = p.communicate(timeout=60)
    stopped = json.loads(out)["status"] == "stopped"
    subprocess.run([sys.executable, "-m", "evalbench", "run", "--data", "data/shards",
                    "--config", "data/configs/config_v2.json", "--workspace", ws_stop],
                   check=True, capture_output=True)
    same = Path(ws_full, "exports/reliability_report.json").read_bytes() == \
           Path(ws_stop, "exports/reliability_report.json").read_bytes()
    for path in Path(ws_full, "checkpoints").rglob("*.json"):
        twin = Path(str(path).replace(ws_full, ws_stop))
        same = same and twin.exists() and path.read_bytes() == twin.read_bytes()
    ok.append(("优雅停止后恢复与完整运行逐字节一致", stopped and same))

print("=" * 60)
allpass = True
for name, passed in ok:
    print(f"[{'PASS' if passed else 'FAIL'}] {name}")
    allpass = allpass and passed
print("=" * 60)
print("验收结论:", "全部通过" if allpass else "存在失败项")
sys.exit(0 if allpass else 1)

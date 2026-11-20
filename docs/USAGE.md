# 使用与复现指南

## 环境准备（Windows / Linux / macOS）

要求 Python 3.10+。Windows 命令行确认 `python --version` 与 `pip --version`
可用后：

```bat
python -m venv .venv
.venv\Scripts\activate
python -m pip install -r requirements.txt
```

Linux/macOS：

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
```

依赖仅 `numpy`、`PyYAML`、`pytest`，无 GPU / 网络 / 容器。

## 1. 生成确定性数据

```bash
python -m scripts.generate_data --data-dir data
```

固定种子 `20261121`，产出 4 个 `.npz` 分片与三份清单：

- `manifest.yaml`：正常预算，4 分片全接纳。
- `manifest_tight.yaml`：紧张计算/配额预算，演示计算与配额拒绝。
- `manifest_tiny_memory.yaml`：极小内存预算，演示内存拒绝。

分片覆盖：8bit 对称逐张量（含离群值）、4bit 非对称逐通道、8bit 非对称逐通道
（含全零通道）、4bit 对称（含 NaN 与 ±Inf）。重复生成逐字节一致。

## 2. 常用命令

```bash
# 查看调度决策（接纳/拒绝、进程数、泳道、预算明细）
python -m qoplib.cli schedule --manifest data/manifest.yaml

# 串行 / 并行运行
python -m qoplib.cli run --manifest data/manifest.yaml --data-dir data \
    --run-dir runs/demo1 --serial
python -m qoplib.cli run --manifest data/manifest.yaml --data-dir data \
    --run-dir runs/demo2 --workers 4

# 优雅停止与恢复
python -m qoplib.cli stop --run-dir runs/demo1
python -m qoplib.cli resume --manifest data/manifest.yaml --data-dir data \
    --run-dir runs/demo1 --serial

# 状态、阶段指纹、审计报告
python -m qoplib.cli status --run-dir runs/demo1
python -m qoplib.cli fingerprints --run-dir runs/demo1
python -m qoplib.cli audit --run-dir runs/demo1
python -m qoplib.cli manifest-fingerprint --manifest data/manifest.yaml
```

## 3. 一键端到端演示

```bash
python -m scripts.run_demo
```

覆盖串行/并行逐字节一致、停止恢复、版本失效传播、三类预算拒绝与五类错误码
注入，结果写入 `artifacts/demo/demo_report.json`，全部检查通过时退出码为 0。

## 4. 作为库集成

```python
from qoplib import QuantConfig, quantize, dequantize, load_manifest, run

result = quantize(array, QuantConfig(bitwidth=4, mode="asymmetric",
                                     granularity="channel", channel_axis=1))
back = dequantize(result.qvalues, result.scale, result.zero_point, config)
print(result.clamp_count, result.clamp_positions)

outcome = run("data/manifest.yaml", "data", "runs/integration", workers=2)
print(outcome.summary["totals"])
```

## 5. 测试

```bash
python -m pytest
```

## 运行目录布局

```
runs/<run>/
├── counter.json            # 单调序号与已提交登记
├── reservation.json        # 最近一次预留
├── schedule.json           # 调度决策（含拒绝原因与预算明细）
├── summary.json            # 确定性归约摘要
├── audit_report.json       # 审计报告
├── STOP                    # 停止请求标志（存在即请求停止）
└── shards/<shard_id>/<validate|quantize|linear|verify>/
    ├── record.json         # 原子、版本化检查点记录
    └── *.npz               # 确定性产物
```

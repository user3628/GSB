# 依存句法分析并行调度与资源预算工作台

本地、确定性、零外部服务的转移式依存句法分析工作台：在全局/线程 token
预算约束下并行分析句子分片，支持分片级原子检查点、优雅停止与恢复、
版本变化增量失效、跨进程并发发布保护，以及 CLI 与本地 HTTP 双界面。

仅使用 Python 3.11 标准库 + numpy + PyYAML + pytest，不使用 Docker、
GPU、预训练模型或网络服务。

## 环境准备

```bash
# 本机已通过 uv 安装 Python 3.11.18 并创建 .venv
uv venv --python 3.11 .venv
uv pip install -r requirements.txt
```

## 一分钟上手

```bash
.venv/bin/python scripts/generate_samples.py     # 固定种子生成 samples/
.venv/bin/python scripts/demo.py                 # 端到端确定性演示
.venv/bin/python -m pytest tests/ -q             # 全部测试

# CLI
.venv/bin/python -m depworkbench.cli --workspace workspaces/demo import \
    --shards-dir samples --manifest samples/manifest.json --config samples/config.yaml
.venv/bin/python -m depworkbench.cli --workspace workspaces/demo run
.venv/bin/python -m depworkbench.cli --workspace workspaces/demo status
.venv/bin/python -m depworkbench.cli --workspace workspaces/demo stats

# HTTP（默认 127.0.0.1:18765）
./scripts/run_http.sh
curl -s http://127.0.0.1:18765/health
```

## 目录结构

- `depworkbench/`：核心包（解析器、预算、检查点、引擎、CLI、HTTP）
- `scripts/`：样本生成器、演示、CLI/HTTP 启动脚本
- `tests/`：pytest 测试套件（解析、预算、检查点、引擎、失效、并发、HTTP、CLI）
- `samples/`：3 个中英混排分片（含跨分片重复句）、清单、YAML 配置与错误注入配置
- `docs/ARCHITECTURE.md`：完整技术说明与错误码表
- `artifacts/`：演示产出的固定摘要与导出文件

## 关键特性对照

| 业务要求 | 实现位置 |
| --- | --- |
| 确定性转移式解码、单根/无环/距离校验 | `depworkbench/parser.py` |
| 全局+线程+单句预算接纳规划 | `depworkbench/budget.py` |
| 线程池并行与有序确定性归约 | `depworkbench/engine.py` |
| 原子检查点、优雅停止、损坏恢复 | `depworkbench/checkpoint.py` |
| 输入/模型/预算指纹与增量失效 | `depworkbench/config.py`, `depworkbench/engine.py` |
| 文件锁发布保护与业务错误码 | `depworkbench/locking.py`, `depworkbench/errors.py` |
| CLI 与 HTTP 交互界面 | `depworkbench/cli.py`, `depworkbench/http_app.py` |

## 验证结果

- `pytest`：42 passed（含跨进程文件锁、独立运行逐字节一致性）。
- `scripts/demo.py`：10 个场景全部通过，固定摘要见
  `artifacts/demo_summary.json`。

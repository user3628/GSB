# qoplib — 低精度量化算子库与预算约束并行调度

本地可运行、确定性可复现的量化算子库。核心能力：4/8 位量化（对称/非对称、
逐张量/逐通道）、量化域线性算子、三重预算约束下的分片调度、进程池并行与确定性
归约、版本化原子检查点、优雅停止恢复、增量失效传播与稳定业务错误码。

仅依赖 `numpy`、`PyYAML`、`pytest`，无 GPU、无网络服务、无容器。

## 快速开始

```bash
python3 -m venv .venv && source .venv/bin/activate   # Windows: .venv\Scripts\activate
python -m pip install -r requirements.txt

python -m scripts.generate_data --data-dir data     # 固定种子生成 4 个分片
python -m pytest                                     # 42 个测试
python -m scripts.run_demo                           # 端到端演示（15 项检查）
```

## 常用 CLI

```bash
python -m qoplib.cli schedule   --manifest data/manifest.yaml
python -m qoplib.cli run        --manifest data/manifest.yaml --data-dir data --run-dir runs/r1 --workers 4
python -m qoplib.cli stop       --run-dir runs/r1
python -m qoplib.cli resume     --manifest data/manifest.yaml --data-dir data --run-dir runs/r1
python -m qoplib.cli status     --run-dir runs/r1
python -m qoplib.cli fingerprints --run-dir runs/r1
python -m qoplib.cli audit      --run-dir runs/r1
```

## 目录

- `qoplib/`：算子库、调度器、检查点、流水线、运行器、审计与 CLI
- `scripts/generate_data.py`：固定种子数据与算子清单生成
- `scripts/run_demo.py`：可复现端到端演示
- `tests/`：量化、调度、检查点、流水线、并行归约、停止恢复、错误码、并发测试
- `docs/ARCHITECTURE.md`：架构与量化/调度/一致性设计
- `docs/USAGE.md`：环境准备、复现步骤与集成示例
- `data/`：生成的 `.npz` 分片与 YAML 算子清单
- `runs/`：运行工作区（检查点、摘要、审计报告）
- `artifacts/demo/`：演示报告 `demo_report.json`

## 业务错误码

`E_NUMERIC_MISMATCH`、`E_NUMERIC_INPUT`、`E_STALE_CHECKPOINT`、
`E_INVALID_STAGE`、`E_INVALID_BITWIDTH`、`E_MEMORY_BUDGET`、
`E_COMPUTE_BUDGET`、`E_WORKER_QUOTA`、`E_WORKER_FAILED`，详见
`docs/ARCHITECTURE.md`。

## 确定性保证

- 数据生成使用固定种子与确定性 `.npz` 写入（固定 ZIP 时间戳/排序）。
- 全局检查点序号是 `(分片序号, 阶段)` 的纯函数，发布按序连续提交。
- 摘要与审计经规范化 JSON 写入；并行与串行运行逐字节一致（测试与演示均校验）。

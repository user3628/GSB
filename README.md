# 时序预测滚动回测与并行调度工作台

本地、确定性、无外部服务依赖的时序预测滚动回测工作台。仅依赖 `numpy` 与
`PyYAML`，支持移动平均、带衰减指数平滑与线性自回归三种候选方法，提供预算约束
下的折级并行调度、原子检查点、优雅停止恢复、版本失效传播、稳定业务错误码与
本地 CLI / 回环 HTTP 诊断工作台。

## 环境准备

- Windows 10/11 + Python 3.11（亦在 WSL2 Linux + Python 3.14 验证）
- 不使用 Docker 或任何外部服务 / GPU

```bash
python -m venv .venv
# Windows: .venv\Scripts\activate    Linux/macOS: source .venv/bin/activate
pip install -r requirements.txt
```

## 快速开始

```bash
# 1. 生成固定种子样本：3 个 CSV 分片 + manifest.json
python scripts/generate_samples.py

# 2. 串行或并行执行滚动回测并导出报告
python -m src.cli --workspace workbench run --export
python -m src.cli --workspace workbench run --serial

# 3. 查询状态、指纹、未处理折与检查点
python -m src.cli --workspace workbench status
python -m src.cli --workspace workbench fingerprints
python -m src.cli --workspace workbench unprocessed
python -m src.cli --workspace workbench checkpoints

# 4. 优雅停止后恢复（结果与一次完整运行逐字节一致）
python -m src.cli --workspace workbench run --stop-after-folds 5
python -m src.cli --workspace workbench stop
python -m src.cli --workspace workbench resume --export

# 5. 紧预算配置：观察 E_BUDGET_EXCEEDED / E_WORKER_QUOTA 裁决
python -m src.cli --workspace workbench-budget run \
    --config config/backtest_tight_budget.yaml

# 6. 清空工作区产物
python -m src.cli --workspace workbench reset
```

## 本地 HTTP 接口（仅回环）

```bash
# 若默认端口被占用，使用 --port 0 由系统分配空闲端口
python -m src.http_app --host 127.0.0.1 --port 8765
```

```bash
curl http://127.0.0.1:8765/health
curl -X POST http://127.0.0.1:8765/api/run \
    -H "Content-Type: application/json" -d "{\"parallel\":true}"
curl http://127.0.0.1:8765/api/status
curl -X POST http://127.0.0.1:8765/api/stop
curl -X POST http://127.0.0.1:8765/api/resume -d "{}"
curl http://127.0.0.1:8765/api/fingerprints
curl http://127.0.0.1:8765/api/unprocessed
curl -X POST http://127.0.0.1:8765/api/export -d "{}"
```

## 样本与配置

- `samples/shard-0x.csv`：列 `timestamp_ms,series_id,target,exo_0,exo_1`，
  含日周期、趋势、缺失时间戳、缺失目标值与突变点。
- `samples/manifest.json`：显式声明每分片行数、时间戳范围、步长、序列集合、
  可选的每序列网格长度与内容指纹，是调度预估与真值观测的来源。
- `config/backtest.yaml`：方法、窗口长度、步长、预测步数、并发上限、
  全局预算、单进程配额、单折时间预算与数据质量阈值。
- `config/backtest_tight_budget.yaml`：预算收紧的演示配置。

## 稳定业务错误码

| 错误码 | 含义 |
| --- | --- |
| `E_MISSING_RATIO` | 序列缺失比例超过配置阈值 |
| `E_SERIES_TOO_SHORT` | 序列短于一个窗口或无完整折 |
| `E_BUDGET_EXCEEDED` | 全局拟合预算耗尽 |
| `E_WORKER_QUOTA` | 单工作进程拟合配额耗尽 |
| `E_TIME_BUDGET` | 单折拟合超过时间预算 |
| `E_WORKER_FAILED` | 工作进程异常（结构化捕获，保留其他结果） |
| `E_STALE_CHECKPOINT` | 落后/重复/跳跃/错位或指纹漂移的发布 |
| `E_INVALID_STAGE` | 非法阶段跳转或前置阶段缺失 |
| `E_CONFIG_INVALID` | 配置越界或未知键 |
| `E_MANIFEST_MISMATCH` | 分片清单与文件内容不一致 |

## 报告产物

运行 `run --export` 或 `export` 后在 `<workspace>/reports/` 生成：

- `report.json`：完整状态、聚合/分组指标、调度与失效信息
- `error_curves.csv`：逐折逐步的实际值、预测、误差与覆盖率
- `budget_details.csv`：接纳、失败、拒绝与未启动折的预算明细
- `dashboard.html`：自包含诊断工作台（无外部资源，可直接双击打开）

## 测试与验收

```bash
python scripts/run_tests.py     # 42 个 unittest
python scripts/demo.py          # 端到端验收：一致性/预算/停止恢复/导出
```

验收要点：串行与并行逐字节一致；停止后恢复与完整运行逐字节一致；任何时刻
累计拟合次数不超过全局预算；分片检查点原子写入且仅允许序号连续推进。

## 深入文档

完整设计、确定性保证、失效传播与 Windows 注意事项见 `docs/DESIGN.md`。

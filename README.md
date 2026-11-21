# ChunkOps：分块 Softmax / LayerNorm 算子库与阶段化验证管线

本项目从零实现一个仅依赖 Python 3.10+ 与 NumPy 的本地算子库，覆盖：

- 行方向 Softmax（`axis=1`）与列方向 LayerNorm（`axis=0`），同时支持两种算子的 `axis=0/1`。
- 行分块、列分块与在线归一化：Softmax 使用 running-max + log-sum-exp 更新，LayerNorm 使用分块 Welford 合并。
- `sequential`、`pairwise`、`compensated`（Neumaier）三种显式求和顺序。
- 基于规模、缓存预算和异常比例的自动分块评分，保留理由、分数明细与淘汰原因。
- 四阶段持久化管线：`plan -> reduce -> validate -> summary`。
- 真实 `fcntl.flock`、原子写入、外部版本守卫、单调版本链和内容寻址结果缓存。
- 静态峰值内存预算与行/列块拆分决策。
- 固定种子三档 `.npz` 样本和异常样本、形状/范围清单。
- 本地 HTTP、CLI、可集成 Python API、确定性演示与验收报告。

## 快速开始

```bash
python3 --version                  # >= 3.10
python3 -m pip install -r requirements.txt

python3 -m chunkops init-data --root data --overwrite
python3 -m chunkops import \
  --workspace runs/small-softmax \
  --fragment data/fragments/small_base.npz \
  --fragment-id small_base \
  --op softmax --axis 1

python3 -m chunkops run --workspace runs/small-softmax
python3 -m chunkops status --workspace runs/small-softmax
python3 -m chunkops plan --workspace runs/small-softmax
python3 -m chunkops errors --workspace runs/small-softmax --output errors.json
python3 -m chunkops summary --workspace runs/small-softmax --output summary.json
```

停止与恢复：

```bash
python3 -m chunkops run --workspace runs/stop-demo --stop-after plan
python3 -m chunkops status --workspace runs/stop-demo
python3 -m chunkops resume --workspace runs/stop-demo
python3 -m chunkops compare \
  --left-workspace runs/full-demo \
  --right-workspace runs/stop-demo
```

本地 HTTP：

```bash
python3 -m chunkops serve --host 127.0.0.1 --port 8765

curl http://127.0.0.1:8765/health
curl -X POST http://127.0.0.1:8765/api/fragments/import \
  -H 'Content-Type: application/json' \
  -d '{"workspace":"runs/http","fragment":"data/fragments/small_base.npz","fragment_id":"small_base","op":"softmax","axis":1}'
curl -X POST http://127.0.0.1:8765/api/runs/stage \
  -H 'Content-Type: application/json' \
  -d '{"workspace":"runs/http","stage":"plan"}'
curl 'http://127.0.0.1:8765/api/runs/status?workspace=runs/http'
curl -X POST http://127.0.0.1:8765/api/runs/stop \
  -H 'Content-Type: application/json' -d '{"workspace":"runs/http","reason":"manual"}'
curl -X POST http://127.0.0.1:8765/api/runs/resume \
  -H 'Content-Type: application/json' -d '{"workspace":"runs/http"}'
curl 'http://127.0.0.1:8765/api/runs/errors?workspace=runs/http'
curl 'http://127.0.0.1:8765/api/runs/summary?workspace=runs/http'
```

服务只允许绑定回环地址。

## 数据集

`python -m chunkops init-data --root data` 生成：

| 分片 | 形状 | 内容 |
|---|---:|---|
| `small_base` | 128 x 512 | 固定种子基础样本 |
| `medium_base` | 2048 x 1024 | 固定种子基础样本 |
| `large_base` | 8192 x 4096 | 固定种子基础样本 |
| `small_anomaly` | 128 x 512 | ±1e4 混合、全零行、NaN、±Inf |
| `medium_anomaly` | 2048 x 1024 | 同上，含稀疏异常 |
| `large_anomaly` | 8192 x 4096 | 同上，含稀疏异常 |

清单位于 `data/manifest.json`，逐分片记录：

- shape、dtype、文件大小、文件 SHA256。
- shape / value / numeric-range 指纹。
- 有限值 min/max/mean/abs-max、NaN/±Inf 数量、异常比例。
- NaN、+Inf、-Inf、任意非有限值的首次出现行列位置。
- 全零行索引和固定种子异常位置。

固定种子重复生成的分片与清单数值逐字节一致（清单仅 `generated_at_utc` 为生成时刻）。

## Python API

```python
from chunkops import softmax, layernorm, build_auto_plan, execute_blocked_op

out1 = softmax(x, axis=1)                       # 自动规划 + 分块执行
out2 = layernorm(x, axis=0, gamma=g, beta=b)   # 列方向 LayerNorm

plan = build_auto_plan(
    op="softmax", axis=1, rows=x.shape[0], cols=x.shape[1],
    cache_budget_bytes=4 * 1024 * 1024,
    anomaly_ratio=0.001,
)
result = execute_blocked_op(
    x, plan, memory_budget_bytes=512 * 1024 * 1024
)
print(result.plan.selection_reason)
print(result.plan.score_details["rejected"])
print(result.nonfinite["first_nan"])
```

`validate_output(x, output, plan)` 返回逐抽样块高精度比较结果和每个输出块的误差界；超过容差时抛出 `ChunkOpsError(code="E_NUMERIC_MISMATCH")`。

## 业务码

| 业务码 | 场景 |
|---|---|
| `E_INVALID_SHAPE` | 非二维、空维度、非法 gamma/beta 形状等 |
| `E_INVALID_AXIS` | 归约轴与张量维度不匹配 |
| `E_INVALID_CONFIG` | 非法算子、分块参数或求和策略 |
| `E_MISSING_FRAGMENT` | 分片文件或 `x` 键缺失 |
| `E_NUMERIC_MISMATCH` | 抽样块超出容差或输出逐字节比较失败 |
| `E_STALE_VERSION` | 序号回退、摘要篡改、形状/数值指纹变化、输出被替换 |
| `E_STALE_PUBLISH` | 并发发布落后、重复、跨算子/轴方案错位 |
| `E_MEMORY_BUDGET` | 静态峰值预算无法容纳输入、输出和最小工作区 |
| `E_ZERO_ROW` | 严格模式下检测到全零行 |
| `E_NONFINITE` | 严格模式下检测到 NaN/Inf |
| `E_INVALID_STATE` | 检查点链损坏、阶段顺序非法、运行尚未导入 |
| `E_NOT_FOUND` | HTTP 路径不存在 |

默认异常策略按 IEEE 传播 NaN/Inf，不替换为零，并记录首位置；`reject_zero_rows` / `reject_nonfinite` 可启用严格业务码。`import` CLI 对应参数为 `--reject-zero-rows`、`--reject-nonfinite`。

## 验证

```bash
python -m pytest
python -m chunkops demo --root . --overwrite
```

本次实际验证结果：

- `python -m pytest`：**60 passed**。
- 三档规模 + 两轴 + 异常分片：**8 个完整管线运行、15 项演示检查全部通过**。
- 自动方案随规模从 `small: 32x64/sequential` 变为
  `medium: 128x256/pairwise` 与 `large: 64x1024/pairwise`；
  异常输入切换为 `compensated`。
- 小/中/大三档抽样校验全部 `ok`；所有抽样块的最坏绝对偏差均被对应误差界覆盖。
- 阶段间停止后恢复与全量构建的输出 `.npy` 逐字节一致。
- HTTP 导入、分阶段运行、停止、恢复、误差报告与摘要均通过，HTTP 恢复输出与全量基线一致。
- 并发版本发布测试：6 个进程仅 1 个连续推进成功，其余返回 `E_STALE_PUBLISH`。

详细报告见：

- `artifacts/verification.json`
- `artifacts/verification.md`

更完整的设计、公式、版本协议与磁盘布局见 `TECHNICAL.md`。

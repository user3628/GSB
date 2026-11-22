# density-trace

本地可运行的滚动核密度估计、分布漂移裁决与可恢复状态组件。项目使用 Python 3.11、NumPy 和标准库 HTTP 服务，不依赖容器、数据库或外部服务。

核心约束已经实现并由测试覆盖：

- 批内高斯 KDE 在缩放定点域分块累加，支持 Kahan/Neumaier 补偿求和。
- 批次状态用整数密度单位精确合并，逐批合并与一次性合并逐字节一致。
- 每次漂移裁决绑定对应的密度状态版本指纹。
- 每批检查点原子发布，优雅停止后恢复的记录与连续运行逐字节一致。
- 样本内容、带宽/分块/求和配置、漂移阈值或参考表变化时，从最早受影响批次失效并按序重建。
- POSIX `flock` / Windows `msvcrt.locking` 真实文件锁保证并发发布只接受连续序号。
- 提供库接口、CLI、本地 HTTP API、确定性演示和异常演示。

## 快速开始

当前仓库已包含一次生成好的演示数据和完整运行产物：

- `demo_data/`：4 个确定性 NPZ 批次、`manifest.json`、`config.json`、`reference_quantiles.json`。
- `artifacts/demo/summary.json`：连续运行、停止恢复、非法参考和版本失效演示的固定摘要。
- `artifacts/demo/continuous/checkpoints/`：4 个原子检查点。

重新创建环境：

```bash
uv python install 3.11
uv venv --python 3.11 .venv
uv pip install --python .venv/bin/python -e '.[dev]'
source .venv/bin/activate
python --version
python -c 'import numpy; print(numpy.__version__)'
```

Windows PowerShell 可改用：

```powershell
py -3.11 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -e ".[dev]"
```

生成并运行演示：

```bash
density-trace generate-demo --out demo_data
density-trace demo --out artifacts/demo
```

也可以直接使用脚本：

```bash
python scripts/generate_demo.py --out demo_data
python scripts/run_demo.py --out artifacts/demo
```

## 实际验证结果

验证环境：Python 3.11.18、NumPy 2.4.6、Linux/WSL2。Windows 文件锁分支使用 `msvcrt.locking`，当前环境未执行原生 Windows 运行。

```text
pytest: 16 passed
ruff check --ignore BLE001: All checks passed
```

演示批次：

| 批次 | 类型 | 原始行数 | 有效样本 | 丢弃缺失行 |
| --- | --- | ---: | ---: | ---: |
| 0 | 单峰 | 160 | 160 | 0 |
| 1 | 双峰 | 160 | 160 | 0 |
| 2 | 单峰 + 离群点 | 160 | 160 | 0 |
| 3 | 双峰 + 缺失值 | 160 | 136 | 24 |

连续运行固定结果：

```text
final state_version      = 4
effective_samples        = 616
verdicts                 = warning, drift, drift, drift
final curve sha256       = sha256:04b89ab4eeb67338a0e31855585befa2bde77f5f5ac04164d01c33b3378b41e3
final version fingerprint= sha256:d94b3c75f2e1e8ebf3b5cad004ab4851ccb3d61a758d148844fc6dafd31df588
record chain sha256      = sha256:d709582ac194c34f50ea15cf0378764475f57a85e62b7e586ea1f931df5f4799
max error bound          = 3.5056251299571713e-12
min significant digits   = 9
```

逐批裁决：

| 批次 | score | 裁决 | 置信度 |
| --- | ---: | --- | ---: |
| 0 | 0.04048402340726714 | warning | 0.90 |
| 1 | 0.1628120848016835 | drift | 0.99 |
| 2 | 0.13057771170586188 | drift | 0.99 |
| 3 | 0.16145819529870056 | drift | 0.99 |

其它实际结果：

- 同配置重复评估的密度字节完全一致。
- 分块大小 `1/7/31/64/1000` 的曲线最大差约 `1.1e-11`，低于配置容差 `1e-9`。
- 一次性合并与逐批增量合并的整数状态、曲线字节和状态摘要完全一致。
- 批次 1 后停止：第一次 `resume` 返回 `stopped` 且不发布新批次；第二次 `resume` 发布批次 2、3。恢复后的 4 个检查点与连续运行逐字节相同。
- 注入缺失分位点 7 的参考表返回 `E_REFERENCE_INVALID`，`batch_id=0`、`quantile_index=7`，没有写入检查点。
- 修改批次 2 内容后，最早受影响批次为 2，批次 0、1 检查点字节未变化；事件记录新版本指纹后再按序重建批次 2、3。
- 两个真实进程同时发布同一后继批次：一个成功，另一个返回 `E_STALE_CHECKPOINT`，已有有效检查点未被改写。

## 数据契约

`manifest.json` 声明每批行数、列数、有效列和 NPZ 键名：

```json
{
  "schema_version": 1,
  "batches": [
    {
      "batch_id": 0,
      "file": "batches/batch-000.npz",
      "rows": 160,
      "cols": 2,
      "valid_columns": [0, 1],
      "source_label": "demo-unimodal",
      "feature_key": "features",
      "label_key": "source_batch"
    }
  ]
}
```

NPZ 包含二维 `features` 和逐行来源标签 `source_batch`。`valid_columns` 是缺失处理的显式来源：先选列，再丢弃有效列中含 NaN 的行；Inf 视为数值异常并返回 `E_NUMERIC`。全部行或全部列无效返回 `E_EMPTY_BATCH`。

参考表包含同一查询网格上的参考曲线和 x/y 边际分位数：

```json
{
  "schema_version": 1,
  "name": "demo-reference-v1",
  "grid": {"x_min": -8.0, "x_max": 10.0, "y_min": -6.0, "y_max": 8.0, "nx": 22, "ny": 20},
  "curve": [0.0, 0.001],
  "quantiles": [
    {"index": 0, "probability": 0.05, "x_value": -2.21, "y_value": -2.16}
  ]
}
```

## 设计摘要

单批密度先按样本块计算。每个块内的概率密度贡献先乘以显式 `density_scale`，再用 Kahan 或 Neumaier 补偿求和；块结果量化为整数单位并记录块序号、样本区间和部分和摘要。状态保存整数密度贡献，而不是未经缩放的概率密度：

```text
U_i = sum_b n_b * q_i^(b)
W_i = sum_b n_b
curve_i = U_i / (density_scale * W_i)
```

`U_i` 和 `W_i` 的加法满足结合律，因此任意中间状态可独立导出曲线，逐批合并与一次性合并也会得到相同字节。误差报告包含缩放因子、块顺序、量化上界、浮点补偿上界、高精度参考误差和有效位数。

漂移裁决从当前二维密度曲线计算 x/y 边际分布，在配置的分位索引范围内与参考表的边际分位点比较：

```text
score = rms_x,y( (q_current - q_reference) / (q_reference[last] - q_reference[first]) )
```

`warning_threshold` 与 `drift_threshold` 对应两档置信水平。缺失配置范围内的分位点、重复索引、概率或 x/y 分位值非严格单调时，均返回带 `batch_id` 和 `quantile_index` 的 `E_REFERENCE_INVALID`，不会跳过。

完整公式和状态/检查点字段见 `docs/design.md`。

## 命令行

导入并分阶段执行：

```bash
density-trace import \
  --workspace .demo-workspace \
  --manifest demo_data/manifest.json \
  --config demo_data/config.json \
  --reference demo_data/reference_quantiles.json

density-trace run --workspace .demo-workspace --batch 0
density-trace run --workspace .demo-workspace --batch 1 --expected-version 1
density-trace resume --workspace .demo-workspace
```

停止与恢复：

```bash
density-trace run --workspace .demo-workspace --max-batches 2
density-trace request-stop --workspace .demo-workspace --reason maintenance
density-trace resume --workspace .demo-workspace
density-trace status --workspace .demo-workspace
```

查询状态、裁决、误差和版本：

```bash
density-trace state --workspace .demo-workspace --batch 2
density-trace curve --workspace .demo-workspace --batch 2
density-trace verdict --workspace .demo-workspace --batch 2
density-trace errors --workspace .demo-workspace --batch 2
density-trace version --workspace .demo-workspace
density-trace reconcile --workspace .demo-workspace
```

## 本地 HTTP

```bash
density-trace serve --workspace .demo-workspace --host 127.0.0.1 --port 8765
```

端点：

| 方法 | 路径 | 用途 |
| --- | --- | --- |
| GET | `/health` | 健康检查 |
| POST | `/api/import` | 导入 manifest/config/reference 路径 |
| GET | `/api/batches` | 列出批次与输入指纹 |
| POST | `/api/run` | 执行指定批次或 `max_batches` 个后继批次 |
| POST | `/api/resume` | 从最新有效检查点恢复 |
| POST | `/api/reconcile` | 检测版本变化并应用失效 |
| POST | `/api/stop` | 请求批次边界优雅停止 |
| GET | `/api/state?batch=N` | 导出中间状态和完整曲线 |
| GET | `/api/curve?batch=N` | 导出曲线与摘要 |
| GET | `/api/verdict?batch=N` | 导出漂移裁决 |
| GET | `/api/errors?batch=N` | 导出误差报告和逐块部分和 |
| GET | `/api/version` | 查询阶段指纹、generation 和失效事件 |
| GET | `/api/checkpoints` | 查询连续检查点序号与摘要 |

示例：

```bash
curl -s -X POST http://127.0.0.1:8765/api/import \
  -H 'Content-Type: application/json' \
  -d '{"manifest_path":"demo_data/manifest.json","config_path":"demo_data/config.json","reference_path":"demo_data/reference_quantiles.json"}'

curl -s -X POST http://127.0.0.1:8765/api/run \
  -H 'Content-Type: application/json' -d '{"max_batches":2}'

curl -s 'http://127.0.0.1:8765/api/verdict?batch=1&expected_version=2'
```

业务错误统一返回 `{"ok":false,"error":{"code":...,"message":...,...}}`。状态冲突返回 HTTP 409，数值/参考/空批返回 422，输入契约返回 400。

## 库集成

```python
from density_trace import Engine, import_inputs

import_inputs(
    ".pipeline-workspace",
    "demo_data/manifest.json",
    "demo_data/config.json",
    "demo_data/reference_quantiles.json",
)
engine = Engine.from_workspace(".pipeline-workspace")
engine.run(batch_id=0, expected_version=0)
engine.resume()

state = engine.get_state(3)
verdict = engine.get_verdict(3)
errors = engine.get_error_report(3)
version = engine.version_info()
```

低层函数也已导出：`load_manifest`、`load_batch`、`evaluate_batch_density`、`merge_batch_state`、`merge_many_batch_states`、`load_reference`、`adjudicate_drift`。

## 错误码

| 错误码 | 触发条件 |
| --- | --- |
| `E_EMPTY_BATCH` | 空批、有效列为空、有效列中没有有限样本 |
| `E_REFERENCE_INVALID` | 参考表缺失/重复分位索引、非严格单调、网格或曲线不合法 |
| `E_NUMERIC` | NaN/Inf、缩放因子为零、整数溢出或误差界失败 |
| `E_STALE_CHECKPOINT` | 发布序号落后、重复、跳跃或批次错位，或 generation 已失效 |
| `E_INVALID_STAGE` | 非法批次跳转、请求缺失前置检查点的状态/裁决 |
| `E_STALE_VERSION` | 断言版本与当前有效版本不符，或查询记录已被当前版本失效 |

## 测试

```bash
.venv/bin/pytest -q
uvx ruff check src tests --ignore BLE001
```

测试覆盖：KDE 确定性、分块容差、空批、一次性/增量状态、中间导出、漂移双阈值、参考缺失与非单调、定点误差报告、零缩放/NaN/Inf、检查点恢复、原子临时文件忽略、内容/配置失效传播、前置复用、真实多进程锁竞争、六类业务错误、CLI 与 HTTP。

## 边界

- 当前密度网格固定为二维；`valid_columns` 必须恰好包含两个列。
- 分块单位是批内样本块；维度和网格在单个配置中固定。
- 停止在批次边界生效，不承诺强杀进程时生成退出报告。
- 文件锁使用本地文件系统语义；网络文件系统未作为验收目标。
- 字节一致性验证目标是同一 Python/NumPy 构建与同一配置和输入。

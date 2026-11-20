# LinearLab：确定性线性模型正则化训练工作台

从零实现的本地训练工作台，聚焦可复现性、分阶段持久化与增量重建。应用以 NumPy 实现二分类逻辑回归，支持 L1、L2、弹性网、坐标下降与近端梯度，并把训练固定划分为：

1. `preprocess`：列识别、缺失填充、标准化/编码
2. `folds`：固定种子的分层折划分
3. `solve`：逐折网格求解与最终全量拟合
4. `aggregate`：CV 指标汇总、最优参数选择与固定摘要

界面与 API 均只依赖 Python 标准库、NumPy，不访问外部网络服务。

## 快速开始

环境：Python 3.10+、NumPy、pytest。当前实际验收环境为 Python 3.14.4、NumPy 2.3.5、pytest 9.0.2。

```bash
python3 -m pip install numpy pytest
python3 scripts/generate_samples.py
python3 -m pytest
python3 -m linreg_workbench serve --workspace runtime/web --port 8765
# 打开 http://127.0.0.1:8765
```

固定演示与基线比对：

```bash
python3 scripts/run_demo.py --workspace runtime/demo
python3 scripts/verify_demo.py
```

也可以使用 `make test`、`make demo`、`make verify`、`make serve`。

## 样本与声明

`data/dataset_declaration.json` 是机器可读声明；`data/DATASETS.md` 是同一内容的人读版本。

| 数据集 | 行 × 列 | 连续 | 类别 | 目标 | 特点 |
| --- | ---: | ---: | ---: | --- | --- |
| `small` | 600 × 12 | 7 | 4 | `label` | 快速交互与网格搜索 |
| `medium` | 6000 × 40 | 29 | 10 | `label` | 5 个缺失列、9 个相关/冗余列 |

- 固定种子：`20261120`。
- 两档都含连续列、类别列和二分类标签。
- 中档含精确复制、线性派生、交互、复合与带噪冗余列。
- 声明记录文件名、SHA-256、行数、列数、目标、每列语义、类别集合、缺失列和冗余关系。
- `scripts/generate_samples.py` 可逐字节重建 CSV、声明和 Markdown 说明。

## CLI

```bash
# 列表与导入
python3 -m linreg_workbench datasets --workspace runtime/cli
python3 -m linreg_workbench import-data --workspace runtime/cli \
  --csv /abs/path/data.csv --dataset-id custom --target label

# 新建训练：网格搜索 λ × L1 ratio
python3 -m linreg_workbench train --workspace runtime/cli --session demo \
  --dataset small --folds 4 --seed 20261120 \
  --mode elasticnet --strength 0.005,0.02,0.08 --l1-ratio 0.2,0.7 \
  --solver coordinate_descent --tolerance 1e-5 --max-iter 250

# 查询
python3 -m linreg_workbench status --workspace runtime/cli --session demo
python3 -m linreg_workbench folds --workspace runtime/cli --session demo
python3 -m linreg_workbench fingerprints --workspace runtime/cli --session demo
python3 -m linreg_workbench invalidations --workspace runtime/cli --session demo
python3 -m linreg_workbench checkpoints --workspace runtime/cli --session demo
python3 -m linreg_workbench summary --workspace runtime/cli --session demo

# 导出
python3 -m linreg_workbench export --workspace runtime/cli --session demo \
  --kind coefficients --output /tmp/coefficients.json
python3 -m linreg_workbench export --workspace runtime/cli --session demo \
  --kind trajectory --task-index 6 --output /tmp/trajectory.json

# 确定性演示钩子：完成指定数量的求解任务后触发优雅停止
python3 -m linreg_workbench train --workspace runtime/cli --session stop-demo --dataset small \
  --mode l1 --strength 0.02 --folds 4 --stop-after-task 3
python3 -m linreg_workbench resume --workspace runtime/cli --session stop-demo
```

真实进程可通过 `SIGINT`/`SIGTERM` 请求优雅停止：当前不可拆分的求解轮次结束后退出。强制 `SIGKILL` 不承诺生成退出报告，但原子检查点仍保证下次一致性恢复。

## HTTP API

启动：`python3 -m linreg_workbench serve --workspace runtime/web --port 8765`。

| 方法 | 路径 | 作用 |
| --- | --- | --- |
| `GET` | `/` | 交互式单页应用 |
| `GET` | `/api/health` | 服务与工作区信息 |
| `GET` | `/api/datasets` | 数据集声明摘要 |
| `GET` | `/api/session` | 当前会话与配置 |
| `POST`/`PUT` | `/api/session/config` | 修改部分配置并返回失效规划 |
| `POST` | `/api/import` | 导入本地 CSV，body：`csv_path`、`dataset_id`、可选 `target`、`features` |
| `POST` | `/api/train` | 启动后台训练；可带 `config_patch`、`stop_after_tasks` |
| `POST` | `/api/stop` | 写停止标记，当前求解轮次完成后退出 |
| `POST` | `/api/resume` | 清除停止标记并从最近一致检查点继续 |
| `GET` | `/api/status` | 阶段、进度、当前任务、错误、检查点摘要 |
| `GET` | `/api/folds` | 各 CV/最终任务状态与验证值 |
| `GET` | `/api/fingerprints` | 版本、阶段与求解任务指纹 |
| `GET` | `/api/invalidations` | 最早失效阶段、原因、可复用阶段 |
| `GET` | `/api/checkpoints` | 连续有效记录与被忽略记录 |
| `GET` | `/api/export/coefficients` | 系数向量、截距与完整结果 |
| `GET` | `/api/export/trajectory?task_index=N` | 指定任务逐轮轨迹；缺省为最终拟合 |
| `GET` | `/api/summary` | 固定摘要 |

失败响应统一为：

```json
{
  "ok": false,
  "error": {
    "code": "E_NUMERIC",
    "message": "Numerical failure during fold solve",
    "details": {"fold": 1, "iteration": 0, "strength": 0.01, "l1_ratio": 0.0}
  }
}
```

## 配置模型

```json
{
  "dataset_id": "small",
  "preprocessing": {
    "numeric_normalization": "standard",
    "numeric_imputation": "median",
    "categorical_encoding": "onehot",
    "categorical_imputation": "mode",
    "target_encoding_smoothing": 10.0
  },
  "cv": {"folds": 4, "seed": 20261120, "stratify": true, "metric": "log_loss"},
  "regularization": {
    "mode": "elasticnet",
    "strengths": [0.005, 0.02, 0.08],
    "l1_ratios": [0.2, 0.7]
  },
  "solver": {
    "name": "coordinate_descent",
    "tolerance": 1e-5,
    "max_iter": 250,
    "objective_limit": 1000000.0
  }
}
```

- `mode=l1` 固定有效 `l1_ratio=1`；`mode=l2` 固定为 `0`；弹性网使用配置列表。
- 强度为 0 时目标退化为普通逻辑损失，代码不会除以正则系数。
- 选择指标支持 `log_loss`（最小）与 `accuracy`（最大）。同分时依次偏向较小 λ、较大 L1 ratio；accuracy 同分再比较 log loss。
- 标准差使用总体标准差（`ddof=0`），并在文档与输出中显式固定。

## 确定性与指纹

- CSV 生成、CSV 读取、分层折划分、类别顺序、网格顺序均显式排序或使用固定种子。
- 所有持久 JSON 使用 `sort_keys=True`、紧凑分隔符、UTF-8 和有限浮点数。
- 原子写入模式：同目录临时文件 → `write` → `fsync` → `os.replace` → 目录 `fsync`。
- 阶段指纹：
  - 预处理：数据 SHA + 声明语义 + 预处理配置
  - 分折：预处理指纹 + 折数 + 种子 + 分层策略
  - 求解：分折指纹 + 折索引 + 正则化参数 + 求解器配置 + 训练/验证索引
  - 汇总：全部分类求解任务指纹 + 指标 + 选择策略
- 运行级版本指纹：数据 SHA + 声明语义 + 完整配置。
- 新配置创建新运行，但会从内容寻址缓存复用指纹相同的前序阶段；失效规划显式列出最早失效阶段、受影响阶段、可复用阶段与变更字段。
- 结果文件 `result.json` 和 `result-summary.json` 不含运行 ID、时间戳或路径，因而同输入同配置的独立工作区会产生逐字节相同文件。

## 检查点、恢复与并发

每个运行目录：

```text
runtime/<...>/runs/<run-id>/
├── run.json
├── STOP
├── artifacts/
│   ├── preprocess.json
│   ├── folds.json
│   ├── solve-0001.json ...
│   ├── final-0013.json
│   ├── aggregate.json
│   ├── result.json
│   └── result-summary.json
└── checkpoints/
    ├── 00000001.json ...
    └── quarantine/
```

每条检查点包含：序号、运行 ID、阶段、折号、任务位次、角色、输入指纹、配置版本、系数摘要、产物路径/哈希、复用来源与记录校验和。

- `fcntl.flock` 对同一运行串行化发布。
- 序号只接受 `last + 1`；落后、重复、跳号或跨折错位返回 `E_STALE_CHECKPOINT`。
- 缺少前置阶段/最终求解返回 `E_INVALID_STAGE`。
- 恢复时只接受从 1 开始的连续有效前缀。损坏 JSON、校验和不符、重复序号、半写临时文件和缺口后的记录均被忽略并隔离。
- 已完成的 CV 任务从产物恢复，不重复求解；最终拟合的参数只有在 CV 选择完成后才确定。
- 优雅停止只发生在完整求解轮次边界；未完成的求解任务不写完成检查点。

## 错误码

| 错误码 | 触发条件 |
| --- | --- |
| `E_EMPTY_FEATURE` | 空列或全缺失列；作为阶段内逐列诊断，其他列继续处理 |
| `E_INVALID_HYPERPARAM` | 非法 λ、L1 ratio、求解器、容差、最大轮次等 |
| `E_INVALID_CV` | 折数 < 2、空折、训练/验证折缺少某一类别 |
| `E_NUMERIC` | NaN、Inf、目标函数超过上界或验证指标非有限；含折号、轮次与超参数 |
| `E_STALE_CHECKPOINT` | 序号落后/重复/跳号、跨折错位、重复阶段发布 |
| `E_INVALID_STAGE` | 非法阶段跳转、缺失前置产物、无可恢复运行 |
| `E_INVALID_DATA` | CSV/声明不一致、目标类别非法、行宽错误等 |
| `E_NOT_FOUND` | 数据集、会话或运行不存在 |
| `E_BUSY` | 已有训练运行占用当前会话 |

## 演示与固定摘要

`scripts/run_demo.py` 覆盖：

1. `small` 与 `medium` 两档完整运行。
2. 首次运行与独立空白工作区二次运行的逐字节摘要比对。
3. 第 7 个求解任务后停止、恢复、检查点前缀不变与结果一致。
4. 正则化强度变化：最早失效 `solve`，复用 `preprocess`/`folds`，不产生旧系数结果。
5. 非法 L1 ratio、单折 CV、数值发散、空特征诊断。
6. 两个进程同时发布同一阶段：一个成功，一个 `E_STALE_CHECKPOINT`。
7. 跳过前置阶段的发布返回 `E_INVALID_STAGE`。

固定基线位于 `expected/small.summary.json` 与 `expected/medium.summary.json`。实际验证结果记录于 `expected/demo_report.json`。

完整验证命令、输出和哈希见 [`VALIDATION.md`](VALIDATION.md)。

### 实际验收记录（2026-11-20）

环境：Python `3.14.4`、NumPy `2.3.5`、pytest `9.0.2`。

```text
$ pytest -o addopts='' -q
30 passed in 12.59s

$ python3 scripts/verify_demo.py --data-dir data
ok = true
small  result_digest = e430a3c2533f8f6df1ee73258af5d0a9130f573125955f8941898ce11ba85e57
medium result_digest = 7ca7cee23831f8145f06ee1611f7866d0ab74bdf91a329453d2d7ed38cd2212b
stop/resume         = 9 checkpoints -> 28 checkpoints, prefix preserved, result equal to full run
invalidation        = earliest solve; preprocess + folds reused; result digest changed
concurrent publish  = one OK, one E_STALE_CHECKPOINT
invalid stage       = E_INVALID_STAGE
invalid L1 ratio    = E_INVALID_HYPERPARAM
single-fold CV      = E_INVALID_CV
numeric divergence  = E_NUMERIC (fold=1, iteration=0, strength=0.01, l1_ratio=0.0)
empty feature       = E_EMPTY_FEATURE (other columns continue)
demo report digest  = 24b0394d0a4d1dd815aad258d90d6d33d66eb1a3d5e5a4a960f01a8c9745e4ee
```

基线文件 SHA-256：

```text
7d89b92e3efc6235ff53ff69f3a391a6e09e6c496a273cd5892a33f1e4f294b4  expected/small.summary.json
6c525c3457600f41f24f4d4e831f1672caa34fbf3df60a6078a06f650eed035e  expected/medium.summary.json
de1de34145777c2f4d2098b1aacf63a8837e3a1f06a229182547da74b986487a  expected/demo_report.json
```

## 源码结构

```text
linreg_workbench/
├── samples.py       固定种子样本生成
├── data.py          声明注册、校验与本地导入
├── preprocess.py    预处理与指纹
├── folds.py         分层折划分
├── solver.py        坐标下降/近端梯度求解核
├── engine.py        四阶段执行、缓存复用与恢复
├── checkpoints.py   原子检查点、文件锁与序号协议
├── workbench.py     会话、失效规划、生命周期
├── api.py           标准库 HTTP 服务
├── cli.py           命令行
├── demo.py          固定演示与验收矩阵
└── static/          交互式单页应用
```

## 已知边界

- 目标编码为预处理阶段在全量训练集上拟合的平滑编码；这是“预处理先于分折”的固定阶段契约下的确定性实现。
- 正则化网格逐点独立求解，不做跨 λ 热启动，因此每个任务结果不依赖候选遍历历史，缓存复用更直接。
- 强制终止进程不承诺状态报告或完整任务；只承诺原子记录不会形成半写完成项。
- 当前界面以桌面工作台为主，最小宽度 1120px；HTTP API 不依赖界面。

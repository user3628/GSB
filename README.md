# 确定性无监督异常检测与增量更新诊断工作台

一个完全本地运行、无容器依赖的异常检测流水线。输入为固定种子生成的 NPZ 批次分片，输出稳健协方差/孤立森林分数、阈值判定、检测质量诊断、阈值敏感度、可恢复检查点和增量失效报告。

核心保证：

- 同一数据与配置产生逐字节一致的预处理矩阵、模型参数、分数向量与诊断报告。
- 分片或配置变化后，只重建最早失效阶段及其下游；上游产物继续复用。
- 增量结果会与临时工作区的从零全量构建逐字节比较。
- 检查点使用原子重命名、摘要自校验、连续序号校验和 `flock` 发布锁。
- 参考分数由独立的从零执行路径生成，作为本地数值真值。

## 快速开始

要求 Python 3.10+、写权限、支持原子重命名与文件锁的本地文件系统。

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -U pip
pip install -e .
```

生成数据并执行完整验收演示：

```bash
python -m anomaly_workbench.cli demo \
  --root .workbench \
  --force \
  --report artifacts/validation-report.json
```

启动本地 HTTP 诊断工作台：

```bash
python -m anomaly_workbench.cli serve --root .workbench --port 8765
# 浏览器打开 http://127.0.0.1:8765/
```

运行测试：

```bash
python -m pytest -q
```

当前仓库内的实际验收记录：

- `artifacts/validation-report.json`：两档数据、停止恢复、参考真值、增量和全量比对结果。
- `artifacts/validation-run.json`：演示命令的终端摘要。
- `artifacts/test-results.txt`：测试输出。
- `artifacts/http-smoke.json`：真实本地 HTTP 进程的健康检查和汇总接口结果。

## 工作区结构

```text
.workbench/
  config.json                    # 规范化配置
  manifest.json                  # 分档、声明文件、批次分片清单
  data/<tier>/
    declaration.json             # 行数、列数、缺失列、重尾列、统计口径
    batch-*.npz                  # 批次分片
  checkpoints/
    records/00000001.json        # 单调、连续的检查点记录
    artifacts/<sha256>.json      # 内容寻址的阶段产物
    quarantine/                  # 损坏、重复或断链记录
  control/stop.json              # 优雅停止请求
  control/execution.json         # 最近执行状态
  state/invalidations.json       # 配置/数据变化导致的失效事件
  reference/reference.json       # 独立本地数值真值
  exports/<tier>/                # 分数与诊断导出
  locks/                         # 流水线、数据、配置、检查点锁
```

## 演示数据

固定种子默认为 `20261122`。生成前即确定两档规模：

| 分档 | 行数 | 列数 | 批次分片 | 缺失列 | 重尾列 |
|---|---:|---:|---|---|---|
| small | 800 | 10 | 4 × 200 | 2, 7 | 3, 8 |
| medium | 6000 | 30 | 5 × 1200 | 4, 13, 22 | 5, 11, 24 |

数据包含连续列、Student-t 重尾列、线性相关冗余列、少量 NaN 与确定性注入的异常偏移。每个 NPZ 包含：

```text
features    float64 [rows, columns]
source_ids  <U16    [rows]
batch_ids   int64   [rows]
```

声明文件显式记录 `rows`、`columns`、`missing_column_indices`、`heavy_tail_column_indices`、冗余列组和统计口径。加载时会验证实际缺失列全部被声明；未声明的缺失结构会返回 `E_DATA`。

## 确定性算法

### 预处理

1. 按配置使用 `median`、`mean` 或 `zero` 填充缺失。
2. 默认使用稳健标准化：`(x - median) / (IQR / 1.349)`；可选择 `zscore`。
3. 使用声明与列统计的并集识别重尾列；默认阈值为 `|skew| >= 2` 或 `excess_kurtosis >= 7`。
4. 使用声明与相关系数并集识别冗余列对；默认阈值为 `|corr| >= 0.98`。
5. 标准化值按配置截断到 `[-12, 12]`，避免极端重尾值放大后续计算。

### 稳健协方差

- 对标准化矩阵再次计算逐列中位数与 MAD 尺度。
- 将稳健标准化值 winsorize 到 `[-5, 5]`。
- 计算 winsorized 协方差，并加入固定收缩：`(1-s)Σ + s·trace(Σ)/p·I`。
- 马氏距离为 `sqrt(xᵀ Σ⁻¹ x)`。
- 产物记录协方差、精度矩阵、特征值范围、条件数、对数行列式。

### 确定性孤立森林

- 自研本地森林实现，不读取外部随机源。
- 第 `i` 棵树的种子由 `SHA-256("isolation-forest:<seed>:<i>")` 派生。
- 每个节点用树内 RNG 选择特征与区间切分点。
- 记录每棵树派生种子、节点数、最大深度、叶节点数、叶样本数最小值/最大值/均值、平均叶深度。
- 路径长度分数为 `2^(-平均路径长度 / c(sample_size))`。

### 合成分数与阈值

1. 协方差分数在 `log1p` 域变换，森林分数在 `logit` 域变换。
2. 每个检测器分别使用训练分数的中位数与 IQR 做稳健标准化，并截断。
3. 权重非负校验后归一化，在标准化域加权合成，避免重尾分数主导。
4. 阈值由 `threshold_quantile` 的显式经验分位数确定，记录分位点、阈值、比较方式和线性分位算法。
5. 任意 NaN/Inf、负权重或全零权重返回 `E_NUMERIC`，包含阶段、首个异常样本标识和上下文化数值，不静默给出默认判定。

### 诊断口径

- 合成分数的数量、极值、均值、标准差和各分位点。
- 总体、按 `source_id`、按 `batch_id` 的检出数与检出率。
- 多个阈值分位点下的阈值、检出率、经验误报估计和 Wilson 误差带。
- 无监督场景没有真实标签，因此误报率使用经验上尾估计 `1 - q`，并明确标注为估计，不伪装成真实标签误报率。
- 所有指标只使用显式模型分数、判定向量和声明文件，不从不完整信息推断。

## 版本指纹与增量失效

阶段产物只包含本阶段相关内容，不嵌入全局配置或时间戳：

```text
preprocess  -> data fingerprint + preprocessing config
fit         -> preprocess artifact digest + detector config
score       -> preprocess/fit artifact digests + scoring config
diagnostics -> score artifact digest + diagnostics config
```

因此无关配置变化不会改变上游产物的字节内容。

| 变化 | 最早失效阶段 | 下游重建 |
|---|---|---|
| NPZ 内容、声明、缺失结构 | `preprocess` | 全部 |
| 缺失策略、标准化、重尾/冗余阈值 | `preprocess` | 全部 |
| 协方差或森林参数 | `fit` | `fit`、`score`、`diagnostics` |
| 权重、归一化、阈值分位点 | `score` | `score`、`diagnostics` |
| 诊断分位点、敏感度曲线、置信系数 | `diagnostics` | `diagnostics` |

每次变化都会写入 `state/invalidations.json`，包含事件序号、变化路径、最早失效阶段、受影响/可复用阶段、旧/新配置版本、旧/新阶段配置指纹和检查点快照。查询接口可读取这些信息。

## 检查点、停止与并发

每个检查点记录包含：

```text
seq, stage, tier, input_fingerprint, config_version,
artifact_digest, artifact_path, artifact_size, record_digest
```

- 产物先原子写入内容寻址目录，再原子写入检查点记录。
- 临时文件写满并 `fsync` 后通过 `os.replace` 发布；目录也执行 `fsync`。
- 扫描时只接受从序号 1 开始连续、记录摘要和产物摘要全部一致的前缀。
- 损坏、缺失产物、重复序号、断链尾部均被忽略；下一次发布会隔离尾部并继续连续序号。
- 阶段发布在 `flock` 下检查下一序号和前置阶段。
- 落后序号、重复发布、上游版本错位返回 `E_STALE_CHECKPOINT`。
- 未知阶段、非法跳阶段或缺失前置阶段返回 `E_INVALID_STAGE`。
- 停止请求只在当前预处理/拟合/打分/诊断阶段完成后生效；拟合轮次不可拆分。
- 恢复会清除停止标记，只复用当前指纹有效的检查点。最终产物与从零构建逐字节一致。

## 错误码

| 错误码 | 含义 |
|---|---|
| `E_NUMERIC` | 分数出现 NaN/Inf、负权重或归一化无效 |
| `E_INVALID_CONFIG` | 配置类型、范围或分位点非法 |
| `E_DATA` | NPZ/声明/分片结构或内容非法 |
| `E_NOT_FOUND` | 分档、文件或接口不存在 |
| `E_INVALID_STAGE` | 非法阶段跳转、缺失前置阶段或当前产物无效 |
| `E_STALE_CHECKPOINT` | 序号落后/重复或上游版本错位 |
| `E_INCREMENTAL_MISMATCH` | 增量产物与从零全量构建不逐字节一致 |
| `E_REFERENCE_MISMATCH` | 产物与独立参考真值不一致 |
| `E_BUSY` | 流水线正在执行，数据/配置变更或第二次执行被拒绝 |

## CLI

```bash
# 初始化 / 生成数据
python -m anomaly_workbench.cli init --root .workbench
python -m anomaly_workbench.cli generate --root .workbench --force

# 导入声明与本地分片（可替换已有批次）
python -m anomaly_workbench.cli import --root .workbench --tier small \
  --declaration .workbench/data/small/declaration.json \
  --shard .workbench/data/small/batch-000.npz

# 四阶段执行、停止、恢复
python -m anomaly_workbench.cli run --root .workbench --tiers all --verify-reference
python -m anomaly_workbench.cli run --root .workbench --tiers small --through-stage preprocess
python -m anomaly_workbench.cli stop --root .workbench
python -m anomaly_workbench.cli resume --root .workbench

# 修改配置并只重建失效链路
python -m anomaly_workbench.cli config --root .workbench \
  --set 'scoring.weights.covariance=0.8' \
  --set 'scoring.weights.forest=0.2' \
  --execute --verify-full

# 单分片替换、重建、影子全量比对
python -m anomaly_workbench.cli replace-shard --root .workbench --tier small \
  --shard .workbench/exports/replacement-small-batch-001.npz --execute --verify-full

# 查询与导出
python -m anomaly_workbench.cli status --root .workbench
python -m anomaly_workbench.cli summary --root .workbench
python -m anomaly_workbench.cli sensitivity --root .workbench --tier medium
python -m anomaly_workbench.cli invalidations --root .workbench
python -m anomaly_workbench.cli reference --root .workbench
python -m anomaly_workbench.cli verify-reference --root .workbench
python -m anomaly_workbench.cli export --root .workbench --tier all
```

`--stop-after tier:stage` 可用于确定性演示或测试在阶段边界触发停止；HTTP 的 `/api/stop` 使用同一停止标记。

## HTTP API

默认监听 `127.0.0.1:8765`，仅面向本地使用。导入接口接受本地文件路径，不接受远程资源。

| 方法 | 路径 | 作用 |
|---|---|---|
| GET | `/` | 简易本地诊断面板 |
| GET | `/api/health` | 健康检查 |
| GET | `/api/status` | 各阶段状态、指纹、失效与执行信息 |
| GET | `/api/summary` | 两档规模汇总 |
| GET | `/api/fingerprints?tier=small` | 版本指纹链 |
| GET | `/api/checkpoints` | 连续检查点账本 |
| GET | `/api/invalidations?tier=small` | 失效原因与受影响阶段 |
| GET | `/api/sensitivity?tier=medium` | 阈值敏感度曲线与来源检出率 |
| GET | `/api/scores?tier=small` | 当前分数产物 |
| GET | `/api/diagnostics?tier=small` | 当前诊断产物 |
| GET | `/api/reference` | 参考真值比对 |
| GET | `/api/execution` | 后台执行状态 |
| POST | `/api/execute` | 启动执行；可设 `through_stage` 与 `wait` |
| POST | `/api/stop` | 请求优雅停止 |
| POST | `/api/resume` | 清除停止标记并恢复 |
| POST | `/api/config` | 单配置变更入口；可附带执行 |
| POST | `/api/import` | 导入本地声明/分片 |
| POST | `/api/replace-shard` | 单分片替换、重建和全量比对 |
| POST | `/api/verify-full` | 对指定分档执行影子全量比对 |
| POST | `/api/export` | 导出分数、模型摘要与诊断报告 |

示例：

```bash
curl -s http://127.0.0.1:8765/api/status
curl -s http://127.0.0.1:8765/api/sensitivity?tier=small
curl -s -X POST http://127.0.0.1:8765/api/execute \
  -H 'content-type: application/json' \
  -d '{"tiers":["small","medium"],"through_stage":"fit","wait":true}'
curl -s -X POST http://127.0.0.1:8765/api/config \
  -H 'content-type: application/json' \
  -d '{"patch":{"scoring":{"weights":{"covariance":0.8,"forest":0.2}}},"execute":true}'
curl -s -X POST http://127.0.0.1:8765/api/stop -d '{}'
curl -s -X POST http://127.0.0.1:8765/api/replace-shard \
  -H 'content-type: application/json' \
  -d '{"tier":"small","shard_path":".workbench/exports/replacement-small-batch-001.npz","execute":true,"verify_full":true}'
```

## 本次实际验收结果

在本机 Python 3.14.4、NumPy 2.2.1 环境中执行。

测试：

```text
...........                                                              [100%]
11 passed
```

完整演示摘要：

- 参考真值逐阶段核对：`verified = true`。
- 停止：`small.fit` 完成后停止，检查点序号 `2`。
- 恢复：完成两档，检查点总数 `8`，序号 `1..8` 连续，`reused=2`、`rebuilt=6`。
- 恢复后两档影子全量构建：`small=true`、`medium=true`，所有四阶段逐字节一致。
- 单分片替换：最早失效阶段 `preprocess`，四阶段全部重建；影子全量比对 `true`。
- 权重变更：最早失效阶段 `score`，两档均复用 `preprocess`、`fit`，仅重建 `score`、`diagnostics`；影子全量比对两档均为 `true`。
- 最终检查点序号 `1..16` 连续，无重复、无跳跃、无忽略记录。
- 非法阈值 `1.5` 返回 `E_INVALID_CONFIG`；负数权重路径由测试覆盖并返回 `E_NUMERIC`。
- 受控影子构建差异测试返回 `E_INCREMENTAL_MISMATCH`，并报告差异阶段 `score`。
- 固定种子重新生成两档数据的内容指纹、声明摘要和矩阵完全一致。
- 两档数据观测：`small = 800×10`，缺失列 `[2,7]`，重尾列 `[3,8]`；`medium = 6000×30`，缺失列 `[4,13,22]`，重尾列 `[5,11,24]`。

参考生成时的配置版本：`14d0c4a21eea3adbc83df6d932d4adf7c6d1512a8d0afac10b7d3b54a0647290`。之后演示修改了权重，因此最终工作台配置版本会不同；报告分别记录了基线参考与最终产物摘要。

完整摘要、每阶段 SHA-256、导出路径与失效事件见 `artifacts/validation-report.json`。

## 实现说明

- 使用自研孤立森林而非直接序列化第三方树对象，以保证跨执行路径的节点、切分种子、叶摘要和分数口径完全可控。
- 参考生成器绕过检查点，从原始 NPZ 重新执行同一数学口径；它不是读取工作台旧产物后复制。
- 全量一致性校验会在临时目录复制当前数据与配置，运行一次真正从零的四阶段构建，再对每个阶段比较原始字节和 SHA-256。
- 所有阶段产物均为规范 JSON（键排序、紧凑分隔、禁止 NaN/Inf），不写入时间戳、绝对路径或进程信息。
- “误报估计”是无标签场景的上尾经验估计，不是伪装成真实标签的准确率。

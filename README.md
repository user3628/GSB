# evalbench — 模型评估与校准格式互操作工作台

本地可运行的评估数据编辑与操作应用：把两种结构版本的评估记录统一为内部模型，
完成分桶校准、可靠性诊断与指标汇总，支持配置/分片变更后的增量失效重建，
并保证优雅停止后恢复结果与完整运行逐字节一致。

## 环境

- Python 3.11+（实际验证于 Python 3.14.4），NumPy、FastAPI、uvicorn，全部本地包
- 不使用 Docker/容器，不访问网络服务

## 目录约定

```
data/shards/*.jsonl        评估记录分片（固定种子生成，v1/v2 混合）
data/configs/*.json        校准配置（config_v1.json 旧版 / config_v2.json 新版）
data/inject/*              注入故障样本（版本不符记录、越界配置、未知别名）
workspaces/<name>/         工作区：checkpoints/<shard>/NNN_<stage>.json + exports/
exports/                   预留导出目录
scripts/gen_data.py        固定种子生成器（seed=20261122）
scripts/run_local.sh       本地运行脚本：data | run | status | test | serve | all
```

## 快速开始

```bash
python3 scripts/gen_data.py --out data                 # 生成分片/配置/注入样本
python3 -m evalbench run --data data/shards \
    --config data/configs/config_v2.json --workspace workspaces/ws1
python3 -m evalbench status --data data/shards \
    --config data/configs/config_v2.json --workspace workspaces/ws1
python3 -m evalbench roundtrip --shard data/shards/shard_v1_a.jsonl \
    --config data/configs/config_v2.json
python3 -m evalbench upgrade-config --config data/configs/config_v1.json
python3 -m evalbench serve --port 8077                 # HTTP 接口
python3 -m pytest tests/ -q                            # 51 项测试
```

HTTP 接口（与 CLI 错误码一致）：`POST /run`、`POST /stop`、`GET /status`、
`GET /exports/reliability_report`、`POST /publish`、`POST /validate/roundtrip`、
`POST /configs/upgrade`。

## 记录结构与内部模型

内部模型：`{model_id, sample_id, score(float), label, lang, batch, bucket}`。

- **v2**：`schema_version=2`，浮点 `score`、显式 `bucket`、稳定 `sample_id`
- **v1**：`schema_version=1`，整数百分比 `score_pct`、无 `bucket`、
  `sample_uid = "<model>:<batch>:<seq>"` 拼接形态

v1 导入规则（全部写入解析产物 `provenance` 可回看）：

- 分数换算 `score = score_pct / 100`（精确）；导出反向换算按可配置舍入规则
  （`half_up` 默认，另有 `half_even`/`floor`/`ceil`，Decimal 实现，确定性）
- 分桶补齐：等宽 `n_bins`，半开区间 `[lo, hi)`，边界值归入高编号桶，
  `1.0` 归入末桶（确定性裁决）
- 稳定标识切分：按可配置分隔符（默认 `:`）从右切最后一段，
  前缀 `<model><delim><batch>` 必须匹配，保证可逆

往返链校验（`evalbench/codec.py`）：

- `v1→内部→v2→内部→v1`：分数/标签/分桶/层级严格相等
- `v2→内部→v1→内部`：分数在 v1 百分比精度内保持，标签/层级严格相等；
  分桶校验为"当前 `n_bins` 下重算一致性"。若 v2 声明的 bucket 属于旧分桶体系
  （配置 `n_bins` 已变化），差异记为 `bucket_rebased` 信息而非违规——
  这使配置演进可重建（需求 7），同时仍能捕获编解码缺陷

声明版本与内容形态不符（如声明 v2 缺浮点 `score`）→ `E_VERSION_MISMATCH`，
错误详情携带 `shard_id` 与 `record_position`（行号），不按猜测版本解析。

## 分桶与校准器

- 等宽分桶：上述半开区间规则；等频分桶：按 `(score, sample_id)` 排序取秩，
  同分值记录不可拆分，整组归入该组最低秩次确定的桶（确定性裁决）
- **Platt 缩放**：`p = sigmoid(A·logit(score) + B)`，对数域阻尼牛顿迭代，
  目标为 Platt 平滑频率；产物记录迭代轮数、收敛状态、最终负对数似然；
  单类标签/Hessian 奇异/不收敛 → `E_FIT_FAILED`
- **等距回归**：唯一分值加权 PAVA 保序 + 节点间线性插值（端点截断），
  单调性写入产物；退化输入 → `E_FIT_FAILED`

## 可靠性诊断

按分桶统计预测概率与真实频率偏差，输出可靠性表、ECE、对数损失、Brier
分数（校准后 + 原始对照），并按语言标签与来源批次分层。每个指标携带
`n_samples` 与 `composition`（lang/batch 构成）。分层有效样本数低于
`reliability_min_samples` 时标记 `insufficient_samples` 并单列于
`insufficient_strata`，不并入有效层。总体正例率对照
`expected_positive_rate` 区间给出 `within_interval` 判定。

## 配置与 v1 升级

v2 配置字段：`n_bins`、`binning`、`calibration.method`、`reliability_min_samples`、
`expected_positive_rate`、`rounding`、`id_delimiter`。

v1 旧配置（`method` 字符串别名、`bins_pct` 整数百分比、缺区间字段）导入时
显式升级并逐条记录审计依据（`upgrade_notes`，同步进入诊断导出）：

- 别名经可配置映射解析为方法枚举（如 `platt_scaling → platt`）
- `n_bins = 100 / bins_pct`（须整除，否则 `E_CONFIG_INVALID`）
- 缺失区间按可配置默认值补全（默认 `[0.0, 1.0]`）

升级后配置可重新导出为 v2 结构；生成器产出的 config_v1/config_v2 语义等价，
实测二者完整诊断报告逐字段一致（`tests/test_pipeline.py::test_upgraded_v1_config_matches_native_v2`）。
越界配置（如 `n_bins=0`）→ `E_CONFIG_INVALID`。

## 检查点、优雅停止与失效传播

- 每分片三阶段 `parse(1) → calibrate(2) → diagnose(3)`，完成即原子写入
  （tmp + fsync + os.replace + 目录 fsync），中断不留半写记录（`.tmp` 残留
  会被读取方忽略）
- 检查点记录：阶段名、记录内容指纹、配置版本指纹、产物摘要、单调序号、
  该阶段依赖的配置视图
- 优雅停止：CLI 捕获 SIGINT/SIGTERM（HTTP `POST /stop`），当前不可拆分单元
  （某分片某阶段）完成后退出；恢复运行复用有效检查点，最终可靠性表、校准
  参数与汇总摘要与完整运行**逐字节一致**（产物不含时间戳等不确定字段，
  测试 `test_graceful_stop_resume_byte_identical` 验证导出与全部检查点字节相等）
- 失效传播：各阶段指纹 = f(上游产物摘要, 本阶段依赖的配置字段)。分片内容、
  校准方法、分桶数、可靠性阈值、期望正例率区间任一变化时，定位受影响最早
  阶段，锁内撤销该阶段及下游产物后重建；未受影响分片的上游解析结果复用。
  失效原因与指纹经 `GET /status` / `evalbench status` 可观测，旧校准参数
  不会被沿用（指纹不匹配即重建）

## 并发发布保护与错误码

发布经真实文件锁（`fcntl.flock`）串行化，仅允许分片内序号连续推进：

| 场景 | 错误码 | HTTP |
| --- | --- | --- |
| 重复/落后序号、跨分片错位发布 | `E_STALE_CHECKPOINT` | 409 |
| 非法阶段跳转、缺失前置产物 | `E_INVALID_STAGE` | 409 |
| 声明版本与内容形态不符 | `E_VERSION_MISMATCH` | 400 |
| 校准器拟合失败 | `E_FIT_FAILED` | 422 |
| 配置越界/未知别名 | `E_CONFIG_INVALID` | 400 |
| 往返链语义破坏 | `E_ROUNDTRIP_VIOLATION` | 422 |

CLI 以退出码 2 + 相同 JSON 错误体输出，与 HTTP 响应一致。

## 验证结果（2026-11-22，Python 3.14.4）

- `python3 -m pytest tests/ -q`：**51 passed**
  （编解码/往返、配置升级、校准器、诊断分层、检查点原子性与序号、
  失效传播、优雅停止恢复逐字节一致、多进程文件锁、HTTP/CLI 错误码一致）
- 固定种子数据：4 分片（v1×2 + v2×2，zh/en 混排，批次 b2026_01/b2026_02），
  共 208 条；两份配置升级后诊断报告完全一致
- 注入样本（`data/inject/`，纯输入文件，未改源码）：版本不符记录 →
  `E_VERSION_MISMATCH`（含分片标识与行号）；`n_bins=0` → `E_CONFIG_INVALID`；
  未知别名 → `E_CONFIG_INVALID`；非法阶段发布 → `E_INVALID_STAGE`；
  重复/跨分片发布 → `E_STALE_CHECKPOINT`
- 优雅停止：`--unit-delay-ms` 运行中 SIGTERM，6 个单元完成后退出；
  恢复后导出与全部检查点同完整运行逐字节一致

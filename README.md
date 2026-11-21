# 本地 CTR / CVR 可复现实验工作台

从零实现的本地确定性 CTR/CVR 管线：JSONL 分片、显式清单观测、特征哈希、对数域双目标逻辑回归、ESMM 风格联合约束、曝光→转化漏斗校验、分片预算调度、进程池确定性归约、原子检查点与增量失效、CLI 和本地 HTTP 工作台。

不依赖 Docker、GPU、真实账号、线上广告平台或网络下载。运行依赖只有 Python 3.10+、`numpy` 与 `pyyaml`；样本生成器只依赖标准库与 `numpy`。

## 环境与兼容性

本次实际验证环境：

- WSL2 / Linux `6.18.58.1-microsoft-standard-WSL2`，`x86_64`
- Python `3.14.4`
- numpy `2.5.5`
- PyYAML `6.0.3`

任务描述要求“确认当前操作系统为 Windows”，但本执行容器实际是 WSL2/Linux，无法伪报为 Windows。代码已按 Windows 目标约束编写：

- 配置和清单使用相对路径，`pathlib` 处理路径。
- 进程池显式使用 `spawn`，兼容 Windows。
- 文件锁使用目录原子创建，不依赖 `fcntl`。
- HTTP 服务使用标准库，无 POSIX 外部命令。
- JSONL 与 JSON 均以 UTF-8 二进制定长写入，换行固定为 `\n`。
- 原子替换使用 `os.replace`，目录 `fsync` 在 Windows 上自动跳过。

在 Windows 上将示例中的 `python3` 替换为 `python` 或 `py -3` 即可。

## 快速开始

```bash
python3 scripts/preflight.py
python3 -m ctr_cvr_workbench.generator --out data/shards --seed 20261122 --shards 6 --rows-per-shard 220
python3 -m ctr_cvr_workbench.cli register --config configs/default.yaml
python3 -m ctr_cvr_workbench.cli run --config configs/default.yaml --execution-mode parallel
python3 -m ctr_cvr_workbench.cli status --config configs/default.yaml
python3 -m ctr_cvr_workbench.cli losses --config configs/default.yaml
python3 -m ctr_cvr_workbench.cli funnel --config configs/default.yaml
python3 -m ctr_cvr_workbench.cli summary --config configs/default.yaml
```

启动本地 HTTP 工作台：

```bash
python3 -m ctr_cvr_workbench.cli serve --config configs/default.yaml
# 浏览器打开 http://127.0.0.1:8765
```

运行测试与完整验收演示：

```bash
python3 -m unittest discover -v
python3 scripts/verify.py
python3 -m ctr_cvr_workbench.cli demo --out artifacts/demo
```

`scripts/verify.py` 会执行测试和演示，并写入 `artifacts/verification_report.json`；演示明细在 `artifacts/demo/report.json`。

## 输入数据契约

样本分片为 UTF-8 JSONL，每行至少包含：

```json
{
  "impression_id": "imp-00-000001",
  "user_features": {
    "age": 31,
    "activity_score": 0.57,
    "city": "  ＳＨＡＮＧＨＡＩ ",
    "device": "iOS",
    "member_tier": "gold"
  },
  "item_features": {
    "price": 128.5,
    "category": "Electronics",
    "brand_tier": "premium"
  },
  "context_features": {"hour": 20, "network": "5G"},
  "ctr_label": 1,
  "cvr_label": 0,
  "source": "feed",
  "lang": "zh"
}
```

`cvr_label` 只在 `ctr_label=1` 时允许出现；负数曝光上出现任意 CVR 标签都会触发 `E_FUNNEL_VIOLATION`。曝光为正时若 CVR 标签缺失，该样本不参与 CVR 损失和联合损失，但参与 CTR 损失。

清单 `manifest.json` 声明每个分片的行数、CTR/CVR 正样本比例、缺失字段计数与原始 SHA256：

```json
{
  "format": "ctr-cvr-shards-v1",
  "seed": 20261122,
  "shards": [
    {
      "shard_id": "shard-00000",
      "index": 0,
      "path": "shard-00000.jsonl",
      "rows": 220,
      "ctr_positive_count": 114,
      "ctr_positive_ratio": 0.51818,
      "cvr_positive_count": 7,
      "cvr_positive_ratio": 0.03181,
      "conversion_rate_on_exposure": 0.06140,
      "missing_fields": {"user_features.city": 9},
      "sha256": "..."
    }
  ]
}
```

管线运行前会把清单声明以 `0444` 只读快照登记到 `state/observations/manifest-<sha>.json`，并将实际行数、正样本计数/比例、SHA256 与真值核对。任何不一致都返回 `E_MANIFEST_MISMATCH`。

## 四阶段管线

1. `features`：对类别值做 Unicode NFKC、空白折叠、小写化；数值特征按边界分箱；类别/分箱 token 经固定 BLAKE2b-64 哈希映射到特征维度。缺失字段总是生成显式 `__MISSING__|<field>` token，不静默丢弃。每行、每分片和整个特征产物都有 SHA256 指纹。
2. `train`：全批量对数域梯度下降，同时学习 CTR 与 CVR；CVR 损失只覆盖“曝光为正且 CVR 有观测”的样本；联合项约束 `p_conversion = p_ctr * p_cvr`。每轮记录 CTR/CVR/组合损失、两类参数变化量、分片遍历顺序与遍历摘要。
3. `funnel`：检查标签关系、`log(CVR) <= log(CTR)`、组合对数概率合法且不超过 CTR、阈值分类下“转化命中必蕴含曝光命中”。不修正标签、不截断概率；违规保留完整诊断并以 `E_FUNNEL_VIOLATION` 返回。
4. `infer`：导出样本级 `log_ctr`、`log_cvr`、`log_conversion`，以及由对数域结果派生的展示概率；计算对数损失、AUC、阈值命中率等目标摘要。

详细公式、确定性归约和检查点语义见 `docs/architecture.md`。

## 预算调度

调度严格按清单 `index` 升序进行，接纳前只使用清单声明的行数：

```text
shard_cost = min(manifest_rows, per_shard_sample_quota) * epochs
```

若 `consumed + shard_cost > max_global_sample_rounds`，该分片记为 `unprocessed` 并写出原因；累计消耗不会越界。工作进程数为 `min(max_workers, accepted_shards)`。默认配置实际接纳前 3 个分片、消耗 2640 sample-rounds，后 3 个因 3500 预算未接纳。

被接纳分片的训练与推理样本为 JSONL 文件顺序的前 `min(rows, per_shard_sample_quota)` 行；训练在每个 epoch 内再用确定性哈希顺序打乱。推理沿用同一接纳集合与配额，保证训练与打分口径可审计。

## 确定性并行

- 每轮在中心进程生成基于 `SHA256(seed|epoch|shard_id|impression_id)` 的确定性遍历顺序。
- 各分片 worker 独立计算局部损失和梯度。
- 中心按 `shard_index` 全局排序后逐项归约，再更新参数。
- 参数在每轮更新后进行对称定点量化，保证停止/恢复与不间断训练一致。
- 顺序和并行模式使用同一 worker 计算逻辑；本次验收中模型、损失曲线、推理分数和实验摘要逐字节一致。

## 检查点与增量失效

每个阶段完成后写入 `checkpoints/<8位单调序号>.json`，内容包含阶段、签名、配置版本、输入指纹、产物摘要和单调序号。产物先原子写，检查点最后原子替换；中断不会留下半写最终记录。

阶段签名粒度：

- 样本内容 / 清单版本 / 特征维度 / 分箱 / 类别字段变化 → 最早失效 `features`。
- 学习率 / 迭代轮数 / 组合权重 / 预算 / 配额 / seed 变化 → 最早失效 `train`。
- 置信阈值变化 → 最早失效 `funnel`。
- 推理格式变化 → 最早失效 `infer`。

失效会向所有下游传播，并写入 `kind=invalidation` 的原子记录；上游已完成阶段继续复用。停止训练会持久化进度记录，恢复时从最后完成的 epoch 继续。

## 稳定错误码

| 错误码 | 场景 |
|---|---|
| `E_INVALID_PARAM` | 特征维度为零、负学习率、负轮数、非法配置/注入参数 |
| `E_MANIFEST_MISMATCH` | 清单行数、比例、计数或 SHA256 与实际分片不符 |
| `E_LABEL_INVALID` | CTR/CVR 标签不是整数 0/1 |
| `E_FUNNEL_VIOLATION` | CVR 标签出现在负曝光、CVR>CTR、组合概率越界、阈值漏斗不一致 |
| `E_WORKER_FAILED` | 进程 worker 失败；已完成分片结果会保留在结构化错误产物中 |
| `E_CHECKPOINT_CORRUPT` | 检查点不可读、序号非法或产物摘要不匹配 |
| `E_SCHEDULE_EMPTY` | 没有分片能落入预算 |
| `E_JOB_ACTIVE` | HTTP 工作台已有活动作业 |
| `E_DATA_INVALID` | JSONL 非法、缺必需字段或 `impression_id` 非法 |
| `E_NOT_FOUND` / `E_IO` | 本地资源不存在或 I/O 失败 |

所有错误对象统一包含 `code`、`message`、`stage`、`shard_id`、`row_number`、`impression_id`、`details`。全局参数错误也会带上首个分片和行 1 作为定位上下文。

## CLI

```text
ctr-cvr preflight
ctr-cvr generate --out data/shards --seed 20261122 --shards 6 --rows-per-shard 220
ctr-cvr register --config configs/default.yaml
ctr-cvr run --config configs/default.yaml [--inject NAME] [--workers N] [--execution-mode sequential|parallel] [--stop-after-epochs N]
ctr-cvr status --config configs/default.yaml
ctr-cvr losses --config configs/default.yaml [--include-traversal]
ctr-cvr funnel --config configs/default.yaml
ctr-cvr summary --config configs/default.yaml
ctr-cvr export-model --config configs/default.yaml --out outputs/export/model.json
ctr-cvr export-scores --config configs/default.yaml --out outputs/export/scores.jsonl
ctr-cvr serve --config configs/default.yaml
ctr-cvr demo --out artifacts/demo
```

支持的注入：`zero_feature_dim`、`manifest_mismatch`、`invalid_ctr_label`、`cvr_without_ctr`、`negative_lr`、`negative_epochs`。

## HTTP API

服务只绑定 `127.0.0.1`，请求体上限 1 MiB，无外部资源。

| 方法与路径 | 功能 |
|---|---|
| `GET /` | 本地工作台页面 |
| `GET /health` | 健康检查 |
| `POST /api/import` | 导入并校验配置/分片，写只读清单观测 |
| `POST /api/run` | 启动四阶段后台作业，返回 202 和 job id |
| `GET /api/jobs` / `GET /api/jobs/{id}` | 作业状态 |
| `POST /api/jobs/{id}/stop` | 协作式停止请求 |
| `POST /api/jobs/{id}/resume` | 从已持久化 epoch 恢复 |
| `GET /api/status?config=...` | 阶段状态、版本、调度与失效记录 |
| `GET /api/versions?config=...` | 配置/样本/特征/阶段版本摘要 |
| `GET /api/losses?config=...` | 损失曲线 |
| `GET /api/funnel?config=...` | 漏斗校验明细 |
| `GET /api/summary?config=...` | 可复现实验摘要 |
| `GET /api/model?config=...` | 定点模型参数 |
| `GET /api/scores?config=...&limit=...` | 推理打分 |

示例：

```bash
curl -s http://127.0.0.1:8765/api/status?config=configs/default.yaml
curl -s -X POST http://127.0.0.1:8765/api/run \
  -H 'Content-Type: application/json' \
  -d '{"config_path":"configs/default.yaml","execution_mode":"parallel"}'
```

停止是协作式的：不会强杀计算中的 worker，也不会用线程超时把未完成计算描述成已终止；它会在下一个 epoch 边界持久化进度并返回 `stopped`。

## 本次实际验收结果

`python3 scripts/verify.py` 最近一次结果（详细见 `artifacts/verification_report.json`）：

- 自动化测试：13 项，0 failure / 0 error。
- 两次相同配置顺序运行：摘要、模型、损失、分数逐字节一致。
- 并行 vs 顺序：摘要、模型、损失、分数逐字节一致。
- 停止 2 轮后恢复 vs 不间断运行：摘要、模型、损失、分数逐字节一致。
- 演示摘要 SHA256：`929e720ceb910e5004d317723446816e267f07ef361e9812c9bc69e9ebfffddc`。
- 演示模型指标：CTR AUC `0.7300`，CVR AUC `0.8893`，CTR logloss `0.6861`，CVR logloss `0.2512`，漏斗违规 `0`。
- 注入错误码：`E_MANIFEST_MISMATCH`、`E_FUNNEL_VIOLATION`、`E_LABEL_INVALID`、`E_INVALID_PARAM`（负学习率与零维度）。
- 将学习率从 `0.15` 改为 `0.20`：最早失效阶段为 `train`，`features` 检查点序号保持不变，重跑后四阶段恢复 valid。
- 检查点序号严格单调且无临时半写文件。

## 目录

```text
ctr_cvr_workbench/   核心实现、生成器、CLI、HTTP 工作台与页面
configs/default.yaml 默认相对路径配置
data/shards/         确定性本地样本与清单
checkpoints/         原子检查点记录
outputs/             特征、模型、损失、漏斗、推理与实验摘要
state/               只读清单观测与最近运行状态
artifacts/demo/      可复现验收演示及报告
scripts/             环境检查、演示与一键验收
tests/               单元与集成测试
docs/architecture.md 算法与系统技术说明
```

## 已知边界

- 未使用真实广告数据；样本由固定种子生成器确定性产出。
- HTTP 工作台面向本机单用户，默认无鉴权，且强制只绑定 `127.0.0.1`。
- 当前实际验证平台是 WSL2/Linux。代码按 Windows 兼容方式实现，但本环境无法提供原生 Windows 执行证据。

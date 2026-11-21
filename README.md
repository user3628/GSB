# 注意力头激活截断裁决与版本轨迹工作台

这是一个完全本地、确定性的注意力头截断敏感性裁决工作台。它不加载真实大模型，不依赖 GPU、容器、外部站点或在线运行时资源；输入数据由固定种子生成器产出，裁决、预算分配、增量缓存与版本轨迹均由本地代码完成。

## 环境与快速运行

实际验收环境：

- Python `3.11.9`：`/home/zsl/.local/python3.11/bin/python3.11`
- 虚拟环境：`.venv311/`
- NumPy `2.4.6`
- FastAPI `0.165.0`
- uvicorn `0.54.0`
- PyYAML `6.0.4`
- pytest `9.2.0`

项目声明 `requires-python >= 3.11`，并在 Python 3.11.9 与 3.14.4 上均通过测试。依赖安装、测试和运行命令：

```bash
uv venv --python /home/zsl/.local/python3.11/bin/python3.11 .venv311
uv pip install --python .venv311/bin/python numpy fastapi uvicorn PyYAML pytest httpx
uv pip install --python .venv311/bin/python -e .

.venv311/bin/python -m pytest -q
.venv311/bin/python -m headworkbench --workspace . demo
.venv311/bin/python -m headworkbench --workspace . serve --host 127.0.0.1 --port 59409
```

`demo` 会重新生成数据与配置、清空阶段缓存和版本历史，然后执行两档采样窗口、增量/全量一致性校验、有效提交、超预算自动回滚、旧版本提交拒绝、头维度不匹配注入及完整历史检查。结果写入 `artifacts/demo_report.json`。

## 目录

```text
headworkbench/
  generator.py      固定种子数据生成器
  data.py           清单、分片、投影维度校验与原子导入
  compute.py        定点截断、单头注意力前向、相对误差
  pipeline.py       四阶段指纹缓存、裁决、预算 DP、增量/全量比较
  versioning.py     文件锁、原子版本记录、自动/显式回滚、恢复
  service.py        FastAPI HTTP 查询、延迟预算、导入与控制端点
  cli.py            本地诊断命令行
  demo.py           端到端验收演示
config/             YAML 裁决配置与当前指针
data/               manifest、3 个 .npz 分片、当前指针
.workbench/         文件锁、阶段缓存、checkpoint、不可变版本记录与 HEAD
artifacts/          固定生成摘要、演示报告、验证报告
  tests/              18 项自动化测试
scripts/            可复现验证脚本
```

## 确定性数据与配置

种子为 `20261122`，生成 3 个分片、6 个注意力头。每个分片包含：

- `activations`: `[64, 2, 16, 8]`，即 `[样本, 分片内头数, 序列长度, 头维度]`
- `query_proj` / `key_proj`: 每个头 `[8, 8]`
- `head_ids`、`layer_names`、`head_indices`、`sample_ids`

清单记录每个分片的 SHA-256、数组形状和逐头元数据；配置包含：

```yaml
candidate_bitwidths: [6, 8, 10, 12, 14]
sampling_windows: {compact: 12, full: 64}
sample_cap_per_head: 64
min_samples_per_head: 8
sensitivity_threshold: 0.90
global_error_budget: 0.030
per_head_error_tolerance: 0.040
activation_clip_abs: 8.0
latency_budget_ms: 250.0
```

## 逐头截断探测

单头前向定义为：

```text
q = x @ Wq^T
k = x @ Wk^T
s = q @ k^T / sqrt(d)
O = softmax(s) @ x
```

候选位宽 `b` 使用有符号定点、向零截断：

```text
qmax = 2^(b-1) - 1
step = 2 * clip_abs / 2^b
Q(x) = clip(trunc(x / step), -qmax, qmax) * step
```

每个头、每个位宽只对当前头的激活做 `Q(x)`，查询/键投影权重及参考前向保持 `float64`。逐样本相对误差为：

```text
rel_i = ||O_i(Q(x)) - O_i(x)||_2 / max(||O_i(x)||_2, 1e-15)
```

探测结果保存每个头、每个位宽的逐样本 `absolute_error`、`reference_norm`、`relative_error`，窗口内误差上界取这些逐项误差的最大值；窗口区间以 `[0, sample_count-1]` 明确给出。

如果激活头维度、查询/键投影形状或头号清单不一致，返回：

```json
{"error":{"code":"E_HEAD_MISMATCH","head_id":0,"activation_head_dim":8,"projection_shape":[2,7,8]}}
```

## 裁决与预算分配

原始敏感度为所有候选位宽窗口上界的最大值。裁决分数按所有头的最大原始敏感度归一化：

```text
score_h = raw_h / max(raw)
ranking = sort by (-score, head_id)
```

采样数低于 `min_samples_per_head` 的头保留在裁决表中，标记 `insufficient_samples`，并进入后续分配占位；不会被静默删除。

位宽分配使用确定性的多选背包 Pareto 动态规划：

- 每个头只能选择 `error_upper_bound <= per_head_error_tolerance` 的候选位宽。
- 所有头误差上界之和必须 `<= global_error_budget`。
- 目标最小化总位宽；同一位宽和时优先较低的 `score * error` 风险，再按头号/位宽字典序稳定决胜。
- 对每个头记录 `sensitivity_protected`、`budget_limited`、`downgraded` 等决策和理由。
- 高裁决分头若下一档更窄位宽满足单头容差但会击穿全局预算，明确标为 `budget_limited`，不是“敏感度不足”。

演示数据全量窗口的分配为：

```json
{"0":14,"1":12,"2":12,"3":14,"4":14,"5":12}
```

误差估计和 `0.028529596080594292`，低于全局预算 `0.030`。

## 增量阶段与逐字节一致性

流水线由四个可缓存阶段组成：

```text
reference_digest    = H(data_digest, reference_algorithm)
probe_digest        = H(reference_digest, window_digest, candidate_bitwidths, cap, clip)
adjudication_digest = H(probe_digest, sensitivity_threshold, min_samples_per_head)
allocation_digest   = H(adjudication_digest, widths, budget, per_head_tolerance)
pipeline_fingerprint= H(all four stage digests)
```

阶段产物写入 `.workbench/stage_cache/`，并使用原子临时文件替换。`incremental` 模式按阶段摘要复用缓存；`full` 模式忽略缓存读取，从零重算。窗口从 `compact` 扩大到 `full` 时，参考前向复用，只重算 `probe -> adjudication -> allocation`；敏感度阈值变化从 `adjudication` 开始；位宽档变化从 `probe` 开始。

增量结果与从零全量结果按规范 JSON 字节比较。不一致时返回 `E_INCREMENTAL_MISMATCH`，并给出 `diff_head_ids`、`first_divergence` 以及两侧首次差异值。

## 版本、锁与回滚

版本目录：

```text
.workbench/version.lock
.workbench/HEAD.json
.workbench/versions/000000000001.json
.workbench/versions/000000000002.json
...
.workbench/checkpoints/
```

每次提交在 `fcntl.flock` 独占锁内读取 HEAD、比较 `expected_base_version`、生成连续版本号。记录包含：

- 版本号、父版本与父记录摘要
- 尝试的逐头位宽与实际生效位宽、两者独立指纹
- 采样窗口名/摘要、配置版本/摘要、数据摘要
- 裁决摘要、分配摘要、流水线指纹、结果摘要
- 逐头误差、预算、验证错误、回滚来源与理由

记录和 HEAD 都通过同目录临时文件 `fsync + os.replace + 目录 fsync` 原子写入；中断只可能留下被忽略的 `.tmp-*` 文件，恢复命令会清理。HEAD 落后于最新不可变记录、记录摘要不匹配或位宽指纹变化均返回 `E_STALE_VERSION`，并以 `cause` 区分 `version_rollback` 与 `record_tampering`。

无效组合不会成为生效组合。若超预算、超单头容差或客户端给出的裁决摘要不一致，提交仍生成新版本记录，但状态为 `rolled_back`，生效位宽恢复到当前可用版本；尝试组合与失败详情保留在历史中。回滚后的查询读取该版本生效位宽，并将记录结果摘要与实时流水线结果摘要比较，确认与提交时全量校验一致。

## HTTP API

| 方法 | 路径 | 用途 |
|---|---|---|
| GET | `/health` | 服务、配置、当前版本状态 |
| GET | `/api/config` | 配置、数据摘要、头清单 |
| POST | `/api/import` | 原子导入工作区内分片/清单/配置 |
| POST | `/api/run` | 触发指定窗口裁决；增量模式默认附全量一致性校验 |
| POST | `/api/verify` | 增量与从零全量逐字节比较 |
| GET | `/api/adjudication` | 裁决排序、误差上界、采样区间 |
| GET | `/api/widths` | 生效位宽组合、预算与逐头理由 |
| GET | `/api/versions` | 完整版本轨迹与回滚记录 |
| GET | `/api/versions/{version}` | 单个不可变版本记录 |
| GET | `/api/rollbacks` | 回滚记录 |
| POST | `/api/versions/commit` | 带基版本号的原子提交 |
| POST | `/api/versions/rollback` | 显式恢复到历史可用版本 |
| POST | `/api/recover` | 从不可变记录恢复 HEAD、清理临时文件 |
| POST | `/api/control/stop` | 优雅停止本地服务 |
| GET | `/api/diagnostics` | 工作区、锁、数据维度诊断 |

查询响应的 `query` 字段包含 `query_id`、`elapsed_ms`、`budget_ms`、包含窗口摘要/配置版本的 `cache_key` 及 `cache_hit`。超过延迟预算返回 HTTP 408：

```json
{"error":{"code":"E_LATENCY_BUDGET","query_id":"...","elapsed_ms":251.7,"budget_ms":250.0}}
```

## CLI

```bash
.venv311/bin/headworkbench --workspace . diagnostics
.venv311/bin/headworkbench --workspace . run --window full --mode incremental
.venv311/bin/headworkbench --workspace . verify --window full
.venv311/bin/headworkbench --workspace . history
.venv311/bin/headworkbench --workspace . rollbacks
.venv311/bin/headworkbench --workspace . commit-suggested --window full
.venv311/bin/headworkbench --workspace . recover
.venv311/bin/headworkbench --workspace . make-mismatch
.venv311/bin/headworkbench --workspace . stop --url http://127.0.0.1:59409
```

## 实际验证结果

验证命令与结果：

```text
.venv311/bin/python -m pytest -q
.................                                                        [100%]
18 passed
```

固定摘要：

```text
manifest_digest       3a1f8ae176e9eb49fa3d1968461868a7456afc6e02ae3b99e3c736cc80714c81
config_digest         97247cf8c8450049806867011ebf211c842604dd2b85a88825ff00722f5424f8
compact_result_digest ba7645c50670c8cd066a2fa513b41d2f573dd01292e442dd10b3a8f2bd98a455
full_result_digest    e7ab9e015a2e9b60b5c1555b393f0fa006e9816ce5c6aec066c96ccf14950cfb
fixed_summary_digest  801b35574e5016cc1b9634be4aa99b0a4573f8df56207d36b5c9a2665717774e
```

两档窗口：

- `compact` 首次运行重算 `reference, probe, adjudication, allocation`。
- `full` 增量运行复用 `reference`，重算 `probe, adjudication, allocation`。
- `full` 增量结果与从零 `full` 全量结果的规范 JSON 字节完全一致。
- `compact` 同样通过增量/全量一致性。

回滚与错误码：

- 版本 1：有效组合，状态 `active`。
- 版本 2：提交全头 6-bit 组合，返回 `global_budget_exceeded` 与 `per_head_tolerance_exceeded`；状态 `rolled_back`，生效组合恢复版本 1 的 `14/12/12/14/14/12`。
- 版本 2 的尝试组合 `6/6/6/6/6/6` 保留在 `/api/rollbacks` 和完整版本轨迹中。
- 使用过期基版本提交返回 `E_STALE_VERSION`，`cause=concurrent_or_duplicate_commit`。
- 4 个独立进程使用同一基版本并发提交，只有 1 个获得版本 1，其余 3 个返回 `E_STALE_VERSION`。
- 注入 `query_proj` 形状 `[2,7,8]` 对激活头维度 `8`，HTTP 导入返回 400 `E_HEAD_MISMATCH`，头号 `0`。
- HEAD 回退一个版本返回 `cause=version_rollback`；记录摘要/位宽指纹被替换返回 `cause=record_tampering`。
- 将延迟预算设为 0ms 的 HTTP 查询返回 408 `E_LATENCY_BUDGET`，含查询标识、耗时与预算。

真实 uvicorn 冒烟结果：

- `/health`：Python `3.11.9`，当前版本 3，窗口 `compact/full`，位宽档 `6/8/10/12/14`。
- `/api/adjudication?window=full`：来源 `version_record`，结果摘要 `e7ab9e01…50cfb`，与实时结果一致；缓存命中，多次实测约 5–32ms。
- `/api/widths?window=full`：生效位宽 `14/12/12/14/14/12`，总误差 `0.028529596080594292`，6 个头均带取舍理由；多次实测约 5–32ms。
- `/api/verify`：`match=true`，两侧均为 `e7ab9e01…50cfb`。
- `/api/control/stop` 返回 `graceful=true`；随后端口关闭，服务进程正常退出。

更多原始结果见 `artifacts/demo_report.json`、`artifacts/verification.json` 与 `artifacts/VERIFICATION.md`。

## 说明

窗口误差“上界”是当前采样窗口内基于本地全精度参考逐项计算的确定性最大值，不是对未采样总体的概率置信界。扩大窗口后增量结果与从零全量结果逐字节比较，是其可审计替代保证。


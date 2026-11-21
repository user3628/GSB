# 本地确定性训练数据预处理与验证管线

这是一个从零实现的本地工具，覆盖：

- 原始 TSV 记录解析、清洗、过滤、分片、确定性采样和 NumPy 张量落盘。
- 带校验摘要、基础版本 CAS 和单调版本号的策略快照。
- SQLite 阶段摘要、分片索引、意图日志、恢复报告和验证记录。
- 单写者执行、同批次等待复用、多读者完整快照查询。
- 五类异常注入、逐阶段重算验证、历史策略回放。
- 进程强制终止后的持久化事实恢复。

项目不使用 Docker，不访问外部数据源，也不需要 GPU。固定样本和两版策略位于 `data/`。

## 快速开始

### Windows PowerShell

```powershell
.\scripts\setup.ps1
.\scripts\run_tests.ps1
.\scripts\run_server.ps1
```

默认绑定 `127.0.0.1:8767`。OpenAPI 页面为 `http://127.0.0.1:8767/docs`。

`setup.ps1` 要求已安装 Python 3.11，并通过 `py -3.11` 创建 `.venv` 后安装 FastAPI、NumPy、Uvicorn、SQLite 驱动（Python 标准库）和测试依赖。

### Linux / WSL

```bash
./scripts/setup.sh
.venv/bin/python -m pytest -q
.venv/bin/python -m prep_pipeline.cli serve --host 127.0.0.1 --port 8767
```

当前工作区的实际验收环境为 WSL 上的 Python `3.11.18`，使用 `uv` 创建了独立 `.venv`。

### 初始化与本地数据

```bash
.venv/bin/python -m prep_pipeline.cli init
.venv/bin/python -m prep_pipeline.cli bootstrap
```

`bootstrap` 按文件名顺序提交 `data/policy_v1.json`、`data/policy_v2.json`。策略历史只追加不覆盖，因此即使当前 head 已经是 v2，仍可读取 v1 做回放。

## 固定输入

记录文件为 UTF-8 TSV，每行严格五列：

```text
timestamp_ms<TAB>sample_id<TAB>text_length<TAB>label<TAB>shard_key
```

字段规则：

- `timestamp_ms`：非负十进制整数。
- `sample_id`：`[A-Za-z0-9_.:-]{1,128}`。
- `text_length`：非负十进制整数。
- `label`：非空；清洗后必须属于策略声明的 `allowed_labels`。
- `shard_key`：非空，分片时做稳定哈希。

固定文件：

- `data/records_valid.tsv`：21 行，20 个唯一样本，`s004` 有一条内容相同的重复；预期保留首条并统计 1 条重复。
- `data/records_mixed.tsv`：合法行与空行、缺列、非法时间戳、负长度、空标识、空标签等畸形行混合。
- `data/records_conflict.tsv`：同一 `sample_id` 内容冲突。
- `data/policy_v1.json`：3 分片、采样比 0.75、seed 1337。
- `data/policy_v2.json`：4 分片、采样比 0.5、seed 2027，用于版本回放和偏斜告警。

修改策略后可用以下命令重新计算整体校验摘要：

```bash
.venv/bin/python -m prep_pipeline.cli policy-checksum --file data/policy_v1.json
```

校验摘要覆盖除 `checksum` 外的全部规范化 JSON 字段。

## 管线阶段

阶段固定按以下顺序执行：

1. `parse_clean`
   - 严格解析五列。
   - 应用清洗规则、标签合法性和分片键存在性检查。
   - 同批次重复标识保留首条；完全相同只计重复数，内容不同立即返回 `E_CONFLICTING_RECORD`。
   - 任一行非法返回 `E_RECORD_INVALID`，整批不提交。
2. `filter`
   - 按时间戳、文本长度和标签过滤。
   - 统计各类丢弃原因。
3. `shard`
   - 使用 `sha256_mod_v1` 将 `shard_key` 稳定映射到声明分片数。
   - 校验分片间不重、不遗漏。
   - 样本数或字节量相对均衡值偏斜超过阈值时输出 `W_SHARD_SAMPLE_SKEW` / `W_SHARD_BYTE_SKEW`；只告警，不改变归属。
4. `sample`
   - 每个分片按采样比例确定目标数量。
   - 记录按 HMAC-SHA256 排名选择，随后恢复原顺序。
5. `tensorize`
   - 每个分片写 `features.npy`、`timestamps.npy`、`text_lengths.npy`、`label_ids.npy`、`shard_ids.npy`、`null_mask.npy` 和 `manifest.json`。
   - `features` 为 `float32[n,2]`，列分别是归一化长度和标签比例。
   - `null_mask` 为 `uint8[n,2]`，当前合法样本的空值计数为 0。
   - 写出后立即重新读取文件，核对文件摘要、dtype、shape 和空值掩码计数。

## 确定性契约

阶段输入摘要由规范化 JSON 计算：

- 解析阶段摘要绑定原始源文件字节摘要与清洗规则。
- 后续阶段摘要绑定前一阶段输出摘要和本阶段策略规则。
- 采样阶段种子：

```text
stage_seed = HMAC-SHA256(
  key = decimal(policy.seed),
  message = "prep-pipeline/v1" + NUL + "sample" + NUL + sample_input_digest
)
```

- 每条记录的选择排名：

```text
rank = HMAC-SHA256(stage_seed, "shard:<id>" + NUL + record_content_digest)
```

所有阶段都不读取全局随机源或当前时间。当前时间只写入 SQLite 元数据，例如提交时间和恢复时间，不进入阶段摘要。

NumPy 数组使用固定 dtype、固定写出方式；manifest 使用排序键、无多余空白的规范 JSON。测试会逐文件比较两次同版本运行的 `.npy` 与 `manifest.json` 字节。

## SQLite 持久化与可见性边界

主要表：

- `policies`：不可变历史策略快照，主键为版本号。
- `policy_head`：当前 head 版本，用于基础版本 CAS。
- `intents`：执行前写入的持久化意图。
- `batches`：批次状态、错误、请求指纹和最终摘要。
- `stage_facts`：每个阶段的输入摘要、输出摘要、统计和完成时间。
- `batch_shards`：已提交分片索引、路径、manifest 摘要、dtype、空值数和告警。
- `policy_locks`：同一策略版本的跨进程单写者锁。
- `recovery_reports`：强制终止后的恢复事实报告。
- `verification_runs`：验证报告历史。

可见性边界：

- 运行中会逐阶段提交 `stage_facts`，用于崩溃恢复和验证。
- 普通查询只在 `batches.status = 'committed'` 时返回数据。
- 分片文件先写 staging，成功后整体移动到发布目录，再在一个 SQLite 事务内写入分片索引并将批次置为 `committed`。
- 查询在一个 `BEGIN ... COMMIT` 读事务内同时读取批次状态、阶段摘要和分片索引。WAL 快照保证读者只会看到完整提交版本，不会看到“部分分片已写、批统计仍旧”的混合状态。

## 并发语义

- SQLite 开启 WAL、`synchronous=FULL` 和外键约束。
- 写事务使用 `BEGIN IMMEDIATE`。
- 同一策略版本通过 `policy_locks` 串行执行；不同策略版本可以排队写入但各自保持执行一致性。
- 同一 `batch_id` 使用唯一主键和请求指纹：
  - 首次请求写意图并执行。
  - 相同请求的并发调用等待已执行批次结束，然后复用相同结果或相同业务错误。
  - 相同批次号但请求内容不同返回 `E_BATCH_ID_CONFLICT`。
- 多读者使用独立连接和 WAL 快照并发读取已提交版本。

## 强制终止与恢复

执行前先提交 `intents` 记录。每个完成阶段都独立落盘到 `stage_facts`。最终 `committed` 是唯一发布事实。

进程被强制终止后：

1. 重启时扫描 `reserved/running/committing` 批次。
2. 读取 SQLite 中已存在的意图与阶段事实。
3. 将批次标为 `aborted`，错误码 `E_RECOVERY_ABORTED`。
4. 清理未发布 staging 或未提交发布目录，并记录删除前后的实际文件探测结果。
5. 生成恢复报告，包含最后完整阶段、其输出摘要、未完成阶段（若有）、已落盘阶段列表和 `committed_shards: []`。

恢复报告明确包含：

```json
{
  "exit_report_available": false,
  "clean_shutdown_report_available": false,
  "final_commit_observed": false,
  "uncommitted_artifacts_recoverable": false
}
```

因此不会声称强制终止后生成过完整退出报告，也不会承诺恢复未提交产物。

## HTTP 接口

所有失败响应统一为：

```json
{
  "error": {
    "code": "E_SAMPLE_RATIO_INVALID",
    "message": "sample_ratio must be in (0, 1]",
    "context": {"sample_ratio": 1.25}
  }
}
```

| 方法 | 路径 | 说明 |
|---|---|---|
| `GET` | `/health` | 健康状态与当前策略 head |
| `POST` | `/v1/policies` | 提交完整带校验摘要的策略快照 |
| `GET` | `/v1/policies/head` | 当前策略版本 |
| `GET` | `/v1/policies/{version}` | 读取任意历史策略 |
| `POST` | `/v1/batches` | 提交样本批次并同步执行 |
| `GET` | `/v1/batches/{batch_id}` | 查询完整提交批次、阶段与分片 |
| `GET` | `/v1/batches/{batch_id}/stages` | 查询阶段摘要 |
| `GET` | `/v1/batches/{batch_id}/shards` | 查询分片统计 |
| `GET` | `/v1/recovery` | 查询全部恢复报告 |
| `GET` | `/v1/recovery/{batch_id}` | 查询指定批次恢复报告 |
| `POST` | `/v1/recovery/scan` | 主动扫描未完成意图 |
| `POST` | `/v1/verify/{batch_id}` | 重算并验证，可指定历史策略或基线批次 |

批次请求示例：

```json
{
  "batch_id": "demo-v1",
  "policy_version": 1,
  "input_path": "records_valid.tsv",
  "injections": {
    "invalid_label": false,
    "missing_shard_key": false,
    "sample_ratio": null,
    "duplicate_conflict": false,
    "interrupt_at_stage": null,
    "interrupt_after_records": 1,
    "target_record_index": 0
  }
}
```

验证请求示例：

```json
{
  "policy_version": 2,
  "baseline_batch_id": "demo-v2"
}
```

## 命令行

```bash
.venv/bin/python -m prep_pipeline.cli init
.venv/bin/python -m prep_pipeline.cli bootstrap
.venv/bin/python -m prep_pipeline.cli serve --port 8767
.venv/bin/python -m prep_pipeline.cli run-batch --batch-id b1 --policy-version 1 --input records_valid.tsv
.venv/bin/python -m prep_pipeline.cli query --batch-id b1
.venv/bin/python -m prep_pipeline.cli verify --batch-id b1
.venv/bin/python -m prep_pipeline.cli verify --batch-id b1 --policy-version 2 --baseline-batch-id b2
.venv/bin/python -m prep_pipeline.cli recover
.venv/bin/python -m prep_pipeline.cli policy-checksum --file data/policy_v1.json
```

异常注入参数：

```text
--inject-invalid-label
--inject-missing-shard-key
--inject-sample-ratio 1.25
--inject-duplicate-conflict
--inject-interrupt-stage shard --interrupt-after-records 2
```

`--hard-crash-after-stage <stage>` 是仅用于验收的强制终止辅助参数，通过 `os._exit(71)` 模拟进程突然消失。

## 业务错误码

| 错误码 | 场景 |
|---|---|
| `E_RECORD_INVALID` | 行结构、UTF-8、整数或字段语法非法；整批不提交 |
| `E_LABEL_INVALID` | 标签不在策略允许集合 |
| `E_SHARD_KEY_MISSING` | 分片键缺失或注入清空 |
| `E_SAMPLE_RATIO_INVALID` | 采样比例不在 `(0, 1]` |
| `E_CONFLICTING_RECORD` | 重复标识内容冲突 |
| `E_INJECTED_INTERRUPT` | 阶段中途注入中断标记 |
| `E_STALE_SNAPSHOT` | 提交版本陈旧或同内容重复提交 |
| `E_STALE_BASE` | 同一基础版本的并发增量 CAS 失败 |
| `E_SNAPSHOT_INVALID` | 策略 schema、版本关系或整体摘要非法 |
| `E_REPRO_MISMATCH` | 验证发现首个阶段摘要或物理张量偏离 |
| `E_BATCH_NOT_COMMITTED` | 普通查询试图读取未完整提交批次 |
| `E_BATCH_ID_CONFLICT` | 同一批次标识对应不同请求 |
| `E_RECOVERY_ABORTED` | 未完成意图在重启恢复时被中止 |

其余输入、路由和内部错误也统一返回 JSON 业务码。

## 验证工具

验证器从已提交批次中读取：

- 持久化源快照 `source/input.txt` 及其 SHA-256。
- 原始策略版本对应的不可变快照。
- `stage_facts` 中每个阶段的输入摘要和输出摘要。
- 已发布分片 manifest 与张量文件。

随后在临时目录完整重算五阶段，逐阶段比较。若不一致，返回 `E_REPRO_MISMATCH`，并给出：

- `first_divergence_stage`：首个偏离阶段。
- `stored_digest` / `recomputed_digest`：两侧摘要。
- 两侧输入摘要和具体差异类型。
- 该阶段的建议排查范围、SQLite 查询和产物路径。

若指定了不同的历史策略版本，可使用显式 `baseline_batch_id`，或自动寻找相同源摘要和策略版本的已提交基线。找不到基线时返回 `replayed_unpaired`，明确标记“仅回放，未做成对比较”。

## 实际验收

本工作区已执行：

```text
20 passed
scripts/acceptance.py: 18/18 checks passed
真实 Uvicorn 127.0.0.1:8767: /health、阶段查询、验证接口均返回 200
```

机器可核对报告：`artifacts/acceptance-report.json`。详细结果与命令见 `VALIDATION.md`。

## 已知边界

- 这是一个本地单主机工具。启动时的恢复扫描假定旧执行进程已经退出；不要同时启动多个独立服务进程对同一个未完成数据库做恢复扫描。
- SQLite 提供单写者串行和 WAL 多读者快照，但不提供跨主机复制。
- 张量字段来自题目给定的时间戳、长度、标签和分片键，不包含原始文本 token；文本片段只以声明长度参与张量化。
- 偏斜告警是分布质量信号，不会改写分片归属。

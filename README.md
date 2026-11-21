# 本地分块列存点查计算引擎

这是一个从零构建、可在 Windows Python 3.11+ 运行的分块列存点查引擎。它读取带版本号的不可变列式快照，对多主键集合执行稳定排序的等值点查，在逐块流式解码与批量多块解码之间做预算裁决，并通过本机回环 HTTP 服务交付可拉取分页结果。批次、块任务、内存预算、页消费进度和恢复事实持久化在 SQLite 中。

当前工作区已经包含确定性的 v1 小样本、v2 大样本、v3 故意损坏块样本、查询键集合、测试与实验报告。默认实验在本机实测全部通过。

## 实际验证摘要

本次运行环境为当前工作区的 WSL Ubuntu 26.04、Python 3.14.4、numpy 2.2.1、FastAPI 0.115.6、uvicorn 0.34.0、pytest 8.3.4。代码仅使用跨平台 Python、`pathlib`、`sqlite3` 和 FastAPI；Windows 上使用 `py -3.11` / `python` 即可运行。Windows 强杀 fallback 使用 `taskkill /F /T`，本次实际进程级强杀演练在 WSL 上用 `os._exit(97)` 完成。

`pytest -q` 实际结果：`22 passed`。测试输出中的少量 warning 来自 Python 3.14 下 Starlette TestClient 的上游弃用提示，不影响引擎行为。

`python scripts/run_experiment.py` 实际输出：

```text
small: path=stream peak=59071 rows=3 pages=2
large: path=batch peak=2645360 rows=136 pages=3 splits=40
compare: bitwise_equal=True max_abs=0.0 max_rel=0.0 digest_equal=True
forced kill: committed_before=2 reused_after=2 reclaimed_budget=1 resumed_keys=[1, 20000, 40000, 60000]
corrupt block: http=400 code=E_BLOCK_CORRUPT block=1
verdict: {"small_query_used_stream": true, "large_query_used_batch": true, "both_queries_pagination_equal": true, "paths_bitwise_equal": true, "paths_within_tolerance": true, "stream_and_batch_digest_equal": true, "forced_kill_persisted_two_blocks": true, "restart_reused_committed_blocks": true, "restart_resumed_pages_in_order": true, "corrupt_block_rejected": true}
```

完整机器可读报告位于 `data/experiment_report.json`。报告明确记录：强杀前 SQLite 中已有 2 个 committed 块、1 个未完成意图和 328750 字节预算占用；重启恢复复用了这 2 个块、回收 1 条预算预留，并从第 0 页继续取完 4 行。恢复报告注明不推断完整退出报告。

## Windows 环境准备

在 PowerShell 中：

```powershell
py -3.11 --version
py -3.11 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

如果系统命令是 `python` 且版本不小于 3.11，也可直接使用 `python -m pip install -r requirements.txt`。

不使用 Docker 或其他容器。服务只允许绑定 `127.0.0.1`、`localhost` 或 `::1`，传入其他地址会直接拒绝启动。

## 快速开始

### 1. 生成固定样本

```powershell
python -m chunk_engine.samples --data-dir data
```

默认生成：

- v1：5000 行，512 行/块，用于小查询流式路径。
- v2：150000 行，16384 行/块，用于大查询批量路径和子块拆分。
- v3：5000 行，512 行/块，`label` 列第 1 块的块体被翻转 1 字节；清单仍合法，解码时会触发 `E_BLOCK_CORRUPT`。
- `data/queries/`：小键集、大键集和命中损坏块的键集。

生成是确定性的，随机种子、数据内容、块边界、校验和与版本时间均固定。

### 2. 运行测试

```powershell
pytest -q
```

### 3. 运行完整可重复实验

```powershell
python scripts/run_experiment.py
```

脚本会自行启动和终止回环 HTTP 服务，不需要另开终端，也不需要外部网络或服务。它会依次完成：

1. 小规模查询并确认自动选择 `stream`。
2. 大规模查询并确认自动选择 `batch`。
3. 对同一大键集强制两条路径，比较逐位结果、摘要和相对/绝对偏差。
4. 用真实 HTTP 请求触发物化，在提交 2 个块后通过 `os._exit(97)` 强杀服务进程。
5. 用同一 SQLite 与数据目录重启，扫描未完成意图、复用已提交块并向客户端续取剩余页。
6. 加载 v3 快照，查询损坏块并验证返回 `E_BLOCK_CORRUPT` 与块号。

### 4. 手动启动服务（可选）

```powershell
python -m chunk_engine.server `
  --data-dir data `
  --snapshot data\snapshots\v0001 `
  --host 127.0.0.1 `
  --port 8765 `
  --memory-budget 4194304 `
  --single-task-budget 65536 `
  --max-parallel-blocks 8 `
  --max-page-size 200
```

`--allow-debug-kill` 或环境变量 `CHUNK_ENGINE_ALLOW_DEBUG_KILL=1` 仅在实验时开启；默认关闭。

## 项目结构

```text
chunk_engine/
  codec.py         三种编码、块头校验、块体校验、受限区间解码
  snapshot.py      清单校验、版本单调发布、不可过快照对象
  planning.py      命块定位、任务合并、预算估计、路径裁决、拆分记录
  persistence.py   SQLite WAL、意图日志、预算台账、页消费、恢复扫描
  engine.py        单写者物化、流式/批量路径、稳定归并、分页、路径比较
  api.py           回环 HTTP 接口与统一业务错误 JSON
  server.py        uvicorn 启动与回环地址强制
  samples.py       确定性快照、查询键与损坏块注入
scripts/
  run_experiment.py 端到端可重复实验
tests/
  test_snapshot_codec.py
  test_planning.py
  test_engine.py
  test_recovery.py
  test_api.py
data/               已生成样本、查询文件与实验报告
```

## 快照与块格式

### 快照清单

每个快照目录包含 `manifest.json` 和 `blocks/`。清单包含：

- `format`、严格递增的 `version`、`generated_at`。
- `row_count`、`primary_key`、`block_size`。
- 每列的 `name`、`dtype`、`nullable`、`encoding` 和空值位图定义。
- 每列按主键升序排列的块索引：块号、行偏移、行数、主键区间、文件、编码、解码大小估计、块体长度和 SHA-256。
- 基于规范化清单 JSON（除 `digest` 字段）的 `digest`。

清单摘要覆盖列定义和全部块索引/块校验和。加载时还会逐块读取头部并核对索引；块体只在取数解码时校验，因此“清单合法、块体被故意篡改”的样本能在加载后准确返回 `E_BLOCK_CORRUPT`，而不是被误分类为快照整体损坏。

`SnapshotManager` 先完整验证候选快照，再在单次引用赋值中发布。候选版本必须严格大于当前版本；任何加载失败都不会替换旧快照。查询批次还持久化快照根目录、版本和摘要，恢复时按批次绑定身份重新加载，不会把新旧列定义或块索引拼在一起。

### 块格式

```text
magic "CHUNKBLK1\n"
4-byte big-endian header length
canonical JSON header
null bitmap
encoded payload
```

头部校验先于解码，覆盖：

- 块号、列名、数据类型、编码类型。
- 主键 `key_min`/`key_max`、原始行数、行偏移。
- 空值位图长度、块体长度、块体 SHA-256。

空值位图为 LSB-first，`1` 表示 null。解码结果 `DecodedSlice` 同时携带列类型、块号、主键区间、`values`、`null_mask`、原始空值位图和每行对应的全局 `row_offsets`。

### 三种编码

- `fixed`：int64/float64 定长小端字节；null 位置写占位值，最终以空值位图覆盖，不把 null 当作谓词值。
- `run`：连续等值（包括连续 null）压缩为 `(run_length, is_null, typed_value)`；解码时逐 run 行走，并核对 run null 标志与空值位图一致。
- `dict`：首次出现顺序建立确定性字典，再写每行 uint32 索引；解码前校验字典索引范围。全 null 块仍保留一个未引用的占位字典项，避免空字典索引歧义。

float64 在游程合并时按 IEEE 位模式区分 `-0.0` 与 `0.0`，避免解压后数值位发生变化。

## 查询规划与内存裁决

### 命块与任务

1. 对主键集合做类型/范围校验、去重和升序排序；超过 `max_keys` 直接返回 `E_KEY_SET_TOO_LARGE`，不截断。
2. 用主键列块索引二分定位每个键所在块；空键集直接形成空结果。
3. 按块号升序合并连续命中块为取数任务。任务记录块区间和目标主键区间，并断言任务间主键区间不重叠、块集合完整覆盖所有命中块。

### 解码前预算

规划只使用清单中的 `decompressed_bytes`、行数和目标键数，不读取或解压块体。对每个命中块和投影列：

- 若块估计不超过 `min(single_task_budget, remaining_budget)`，按整块处理。
- 若超过，按 `bytes_per_row = ceil(decompressed_bytes / row_count)` 计算子块行数，形成确定性的 split record，包含块号、列名、原估计、有效预算、每行字节、子块行数、子块数量与子块峰值。
- 如果连一行都不能装入有效预算，在任何块解码之前返回 `E_MEMORY_BUDGET_EXCEEDED`。

### 路径选择

每个块任务估算包含各列子块上限、命中键行开销和小型固定开销。令 `max_block_estimate` 为最大块任务估计：

- 可并行 lane 数 `fit_lanes = min(max_parallel_blocks, hit_blocks, remaining // max_block_estimate)`。
- `fit_lanes >= 2` 时自动选择 `batch`，峰值估计为 `fit_lanes * max_block_estimate`。
- 只够一条 lane 或强制 `stream` 时选择 `stream`，峰值估计为 `max_block_estimate`。
- 强制 `batch` 时至少使用一条 lane，并在同一预算公式下执行。

SQLite 预算台账在解码前写入 `reserved`，完成或失败后变为 `released`。若进程崩溃，重启恢复将遗留 `reserved` 标记为 `reclaimed`，恢复续取时再按持久化计划原子地重新预留。响应和批次状态均输出所选路径、并行块数、峰值估计、剩余预算、原因与全部拆分记录。

## 两条路径的逐位一致

- 两条路径共享同一块解码、同一空值处理、同一目标行定位和同一全局稳定归并代码。
- `stream` 逐块处理，`batch` 按已裁决的并行 lane 解码块组；并行任务只读文件，SQLite 写入仍由单一写者按组提交。
- 内部结果先写 `result_rows.jsonl`，保留 Python 可往返的精确 float 二进制值。随后以主键严格升序生成固定大小页文件。
- JSON 响应中的 float64 使用固定 6 位小数字符串，例如 `"123.456789"`，并在 `rounding` 中说明 `fixed_decimal_round_half_even`、小数位和受影响的列。内部比较在展示舍入前完成。
- `/compare` 强制同一键集分别走 `stream` 与 `batch`，逐行比较主键顺序、null mask、整数/字符串精确值和 float 位模式；同时记录最大绝对偏差、最大相对偏差、容差参数和前 20 个差异。实际样本两者结果摘要完全相同。

## 分页与并发消费

提交查询返回批次状态、结果句柄和第 0 页。有效页大小为：

```text
min(client_page_size, client_max_page_size, server_max_page_size)
```

页进度持久化在 SQLite。拉取同一页时使用 `BEGIN IMMEDIATE` 执行 compare-and-set：只有把 `ready` 原子改成 `consumed` 的请求获得数据，其他请求返回 `E_PAGE_CONSUMED`。页文件本身按主键稳定归并，分页不改变行顺序。

`GET /results/{handle}/verify-pagination` 只读校验页文件拼接结果与内部 JSONL 全量结果的键、值、null mask、页行数和总行数是否逐行一致，不会消费页。

## SQLite、单写者与崩溃恢复

SQLite 使用 WAL、`synchronous=FULL`、外键和 30 秒 busy timeout。HTTP 多读者可并发读取已完成结果；物化由进程内单写者锁串行化，SQLite 写操作另由写事务锁串行化。

核心表：

- `batches`：查询批次、快照身份、计划、状态、结果摘要和执行信息。
- `block_tasks`：每个命中块的状态、尝试次数、产物路径/校验和、`reusable` 标记。
- `memory_ledger`：每次预算预留的字节、路径、原因和释放/回收事实。
- `page_progress`：每页行范围、ready/consumed、消费者和时间。
- `intents`：批次运行、块物化、最终合并和恢复期预算预留的意图日志。
- `recovery_reports`：每次重启扫描事实。

块任务流程：

1. 事务写入 `task_materialize` pending 意图，并把任务置为 `running`，提交。
2. 在事务外执行只读解码，原子写块产物。
3. 事务写块产物校验和、任务 `committed`、意图 `committed`，提交。
4. 全部任务后写 `batch_finalize` pending 意图，再组装 JSONL 与页文件，最后用一个事务提交批次、页进度和预算释放。

重启扫描只依据 SQLite 已提交行和磁盘产物 SHA-256：

- committed 且产物校验通过的块保持 committed，置 `reusable=1` 并写入恢复报告。
- 有 pending 意图但无已提交产物的任务恢复为 `ready`。
- 遗留 `running` 任务恢复为 `ready`。
- 未提交的结果元数据不视为事实；批次回到 `ready`，后续页请求触发续做。
- 遗留预算预留标记为 `reclaimed`。

恢复报告明确写着 `No clean-shutdown or complete-exit report is inferred after forced termination.`，不虚构进程退出时的完整报告。

## HTTP 接口

```text
GET  /health
GET  /snapshots/active
POST /snapshots/load                 {"path": "..."}
POST /batches                        提交查询并消费第 0 页
GET  /batches                        批次列表
GET  /batches/{id_or_handle}         批次/任务/计划状态
POST /batches/{id}/resume            显式续做未完成批次
GET  /results/{handle}/pages/{n}?consumer_id=...   拉取并消费一页
GET  /results/{handle}/verify-pagination           校验分页拼接
POST /compare                        强制两路径比较
GET  /memory                         预算总量与台账
GET  /admin/recovery                 最近一次重启恢复报告
POST /admin/recover                  再执行一次持久化事实扫描
```

提交示例：

```json
{
  "keys": [1, 89, 97, 113],
  "columns": ["score", "region", "label"],
  "page_size": 2,
  "max_page_size": 20,
  "execution_mode": "auto",
  "consumer_id": "client-a"
}
```

主键列始终作为第一投影列返回，以保证稳定顺序。`execution_mode` 可取 `auto`、`stream`、`batch`。

## 统一业务错误

失败响应统一为：

```json
{
  "ok": false,
  "error": {
    "code": "E_BLOCK_CORRUPT",
    "message": "block failed validation before decode",
    "context": {"block_no": 1, "column": "label", "reason": "body checksum mismatch"}
  }
}
```

已实现并测试：

| 业务码 | HTTP | 场景 |
| --- | ---: | --- |
| `E_SNAPSHOT_INVALID` | 400 | 清单缺失/摘要错误/版本不递增/索引或头部不合法 |
| `E_BLOCK_CORRUPT` | 400 | 块头不匹配、块体摘要错误、主键/null 一致性错误 |
| `E_KEY_SET_TOO_LARGE` | 413 | 主键数量超过配置阈值 |
| `E_MEMORY_BUDGET_EXCEEDED` | 507 | 拆分后单行仍超预算或选中路径峰值超剩余预算 |
| `E_PAGE_CONSUMED` | 409 | 同一页被第二个消费者拉取 |
| `E_BATCH_NOT_FOUND` | 404 | 批次/结果句柄不存在 |

另有 `E_INVALID_REQUEST`、`E_PAGE_OUT_OF_RANGE`、`E_BATCH_FAILED`、`E_INTERNAL_ERROR` 覆盖参数、页范围、失败批次和未预期内部错误。

## 已验证行为

自动化测试覆盖：

- 定长、游程、字典解码，空值位图与 float/int/string 类型一致性，行偏移。
- 故意损坏块返回 `E_BLOCK_CORRUPT` 并携带块号/列名。
- 清单摘要篡改返回 `E_SNAPSHOT_INVALID`，旧快照仍可查询。
- 版本非单调拒绝，失败加载不发布候选快照。
- 空键集空结果，超阈值错误，不相邻块分任务，任务区间不重叠。
- 解码前拆分记录与预算不足错误。
- 小查询流式、大查询批量，两路径 bitwise 相同、最大绝对/相对偏差为 0。
- 分页拼接、无重无漏、页并发只允许一个消费者。
- 固定 6 位 float 字符串与舍入说明。
- 模拟崩溃后的 committed 块复用、ready 任务续做、预算回收、页续取。
- 真实 HTTP 业务错误 JSON、快照加载失败保持旧版本、损坏块错误。
- 完整实验脚本中的真实进程 `os._exit(97)` 强杀与重启续取。

## 设计边界与说明

- 服务固定单 uvicorn worker。多进程共享 SQLite 可扩展为外部协调，但本任务要求的是单写者物化与多读者查询。
- 预算针对解码峰值；编码块文件仍会整体读入以完成块体摘要校验。若单块编码体本身巨大，可进一步改成流式哈希与 mmap，但本样本规模下无需额外复杂度。
- 分页结果采用 at-most-once 消费：页被标记 consumed 后即使客户端未收到响应，也不会再次交付；这是并发一页一消费者的严格语义。
- 结果 JSON 的 float 是固定小数位字符串，不是 JSON number，以保证展示稳定和尾随零不被序列化器吞掉；内部比较始终使用原始 float。
- 生成大样本可调整 `--large-rows`、`--large-block-size`；算法本身不依赖默认 150000 行，但实验应在本地确定性数据上运行。

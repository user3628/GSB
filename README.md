# 服务网格旁路流量镜像采样与回放偏差裁决组件

这是一个可在 Windows 本地独立运行的领域组件。它把固定的镜像流量样本提交为不可变版本，按带版本的策略做确定性采样和回放分批，向仅绑定回环地址的测试端点执行回放，并把**采样偏差**与**回放失败/超时偏差**分开归因。

组件不依赖 Docker、外部服务或远端目标。SQLite、固定 JSON 样本和本地 HTTP 测试端点即可完成全流程。

## 主要能力

- 样本批次解析失败时返回 `E_RECORD_INVALID`，整批原子拒绝；`(batch_id, line_no)` 是记录身份，重复提交完全相同的内容幂等，不同内容返回 `E_CONFLICTING_RECORD` 并保留首次记录。
- 样本、策略都采用带基础版本校验的增量提交；整份快照替换使用新的 `snapshot_version`，新旧样本与策略不会混用。
- 采样由策略 `seed` 与记录内容派生的稳定 `sampling_sequence` 共同决定。相同记录集合无论被拆成多少个提交批次，逐条抽中结论一致。
- 排除规则优先于包含规则，包含/排除规则优先于采样率；同一连接在窗口内最多选中一次。
- 回放操作按放大倍数、批并发、样本数、估计字节量和估计耗时预算确定性地分批；空间不足时降倍或按连接顺序拆分，并输出原因与影响样本数。
- 每次回放使用独立连接和独立超时；超时会立即关闭底层 socket，并计为回放失败，不会用纯等待冒充超时。
- 每个提交版本以单事务写入策略快照、样本映射、采样集合、回放批次、偏差分解和依赖关系。策略规则变化会级联失效旧产物并原子重建新版本。
- 写入先落持久化意图记录。强杀进程后，启动扫描会把未完成提交标记为中止、把运行中的回放批次标记为中止，当前可见版本仍停留在最后一个完整版本。

## 目录

```text
mirror_judge/          领域组件与 FastAPI 服务
scripts/run_server.py  仅回环地址的启动入口
scripts/demo.py        端到端演示（自动启动/停止本地服务）
scripts/build_fixtures.py  固定样本与策略生成器
data/                  固定确定性样本和策略片段
tests/                 pytest 测试
runtime/               运行时 SQLite 与演示输出（已被 gitignore）
```

## Windows 环境准备

要求 Python 3.11 或更高版本。固定依赖见 `requirements.txt`：

```powershell
cd <工作区目录>
py -3.11 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
pip install -r requirements.txt
```

确认 Python 和依赖：

```powershell
python --version
python -c "import fastapi,uvicorn,numpy,pytest; print(fastapi.__version__, uvicorn.__version__, numpy.__version__, pytest.__version__)"
```

工作区需要允许创建 SQLite 文件和临时目录。默认数据库为 `runtime\mirror_judge.db`，服务会自动创建父目录。

## 启动本地服务

必须使用启动入口绑定回环地址；服务同时带有请求侧回环检查和回放目标回环校验：

```powershell
python scripts\run_server.py --host 127.0.0.1 --port 8765 --db runtime\mirror_judge.db
```

健康检查与 OpenAPI：

```text
http://127.0.0.1:8765/health
http://127.0.0.1:8765/docs
```

服务默认把回放目标设为 `http://127.0.0.1:8765/_test/replay`。测试端点支持：

- `mirror`：响应原始记录中的状态码，用于正常回放；
- `ok`：固定成功；
- `status`：返回指定错误状态码；
- `hang`：故意挂起指定毫秒数，用于验证真实超时与连接关闭。

## 运行端到端演示

演示脚本会在空闲回环端口自动启动独立服务，完成后停止，最终报告写入 `runtime\demo\demo_report.json`：

```powershell
python scripts\demo.py
```

保留已有演示数据库时可附加 `--keep-runtime`；默认会清理 `runtime\demo` 后重新运行，保证可复现。

演示依次完成：

1. 导入 `data\mirror_records.jsonl` 的 188 条全量样本与 `data\policy_baseline.json`；
2. 追加 `data\mirror_records_increment.jsonl` 的 12 条样本增量；
3. 只提交 `checkout-service` 分层权重变化，观察采样集合、批次和偏差分解三类产物级联重建；
4. 执行正常回放并查询不可变裁决版本；
5. 使用 `hang` 测试端点触发真实超时，取得 `E_REPLAY_TIMEOUT` 与独立超时归因；
6. 停止服务并启动带测试钩子的服务，在级联重建提交前 `os._exit(97)` 强杀；重启后输出只基于 SQLite 已持久化事实的恢复报告；
7. 在恢复后的当前版本上再次提交策略增量，证明系统仍可继续演进。

测试钩子仅在 `MIRROR_ENABLE_TEST_HOOKS=1` 时启用，正常启动不会触发。

## 固定样本与策略

固定生成种子：`20261122`。主样本 188 条，样本增量 12 条，均由 `scripts\build_fixtures.py` 固定生成；文件已随仓库提供，无需运行时生成。

样本 JSONL 每行字段：

```json
{
  "timestamp_ms": 1764000001000,
  "connection_id": "conn-0001",
  "source_service": "edge-gateway",
  "destination_service": "catalog-service",
  "method": "GET",
  "path_template": "/api/catalog/items/{item_id}",
  "status_code": 200,
  "response_bytes": 647,
  "duration_us": 7121
}
```

基线策略的关键规则：

```json
{
  "policy_version": "p-baseline-1",
  "seed": 20261122,
  "sampling_rate": 0.55,
  "service_weights": {"checkout-service": 1.2},
  "include_path_templates": ["/api/*"],
  "exclude_path_templates": ["/api/payment/*", "/internal/*"],
  "dedup_window_ms": 90,
  "replay_multiplier": 2,
  "max_batch_samples": 8,
  "max_batch_concurrency": 4,
  "batch_max_bytes": 200000,
  "batch_max_duration_us": 350000,
  "replay_timeout_ms": 600
}
```

实际权重表还包含 `catalog-service`、`payment-service`、`search-service` 和 `observability-service`。未声明的目的服务默认权重为 `1.0`。

## HTTP API

所有写接口都返回完整的新版本摘要（或幂等结果），所有查询接口都可附加 `?version=N` 固定读取某个已提交版本；不传时读取当前版本。

| 方法 | 路径 | 说明 |
|---|---|---|
| GET | `/health` | 健康状态、当前版本、数据库和回放目标 |
| GET | `/api/v1/commits` | 提交版本列表 |
| GET | `/api/v1/state?version=N` | 指定完整版本的摘要和三类产物 |
| GET | `/api/v1/records?version=N` | 指定版本的完整样本集合 |
| GET | `/api/v1/selection?version=N` | 采样集合、逐条决策、拒绝原因 |
| GET | `/api/v1/batches?version=N` | 回放批次、操作、预算、降倍与批次状态 |
| GET | `/api/v1/bias?version=N` | 线上、采样、回放统计与偏差归因 |
| POST | `/api/v1/snapshots` | 初次全量导入或带基版本校验的整份快照替换 |
| POST | `/api/v1/samples/batches` | 追加样本增量批次 |
| POST | `/api/v1/policies/increments` | 提交策略增量片段 |
| POST | `/api/v1/replays` | 执行批次回放并生成新裁决版本 |
| GET | `/api/v1/replays/{run_id}` | 查询回放运行事实 |
| GET | `/api/v1/recovery/latest` | 最近一次启动恢复报告 |
| POST | `/api/v1/recovery/scan` | 显式再次扫描未完成意图 |
| POST | `/_test/replay` | 本地回放测试端点，四种故障模式 |

初次全量导入示例：

```json
{
  "base_version": null,
  "snapshot_version": "snapshot-1",
  "batch_id": "baseline-records",
  "records": [{"timestamp_ms": 1764000001000, "...": "..."}],
  "policy": {"policy_version": "p1", "...": "..."}
}
```

样本增量：

```json
{"base_version": 1, "batch_id": "mirror-batch-2", "records": [{"...": "..."}]}
```

策略增量（片段只包含受影响规则）：

```json
{
  "base_version": 2,
  "base_policy_version": "p1",
  "policy_version": "p2",
  "patch": {"service_weights": {"checkout-service": 0.2}}
}
```

回放：

```json
{
  "source_version": 3,
  "batch_id": null,
  "timeout_ms": 500,
  "fault_mode": "mirror",
  "raise_on_timeout": false
}
```

使用 `hang` 并设置 `raise_on_timeout=true` 时，超时请求仍会持久化并生成完整裁决版本，接口随后返回 HTTP 504 与 `E_REPLAY_TIMEOUT`，错误详情包含 `run_id` 和 `result_version`。

## 一致性、版本与恢复语义

- **不可变版本**：每次成功写入都会产生新的 `commit_version`。样本映射、策略快照、三类派生产物和当前版本指针在同一个 `BEGIN IMMEDIATE` 事务中提交。读者使用单个只读事务读取当前版本，因此不会读到“采样已更新、偏差仍为旧值”的中间状态。
- **依赖关系**：`artifact_dependencies` 记录 `selection`、`batches`、`bias` 分别依赖哪些策略规则。只改 `service_weights.checkout-service` 时，三类产物都会失效并在新版本内重建；旧版本保持原样可查。
- **单写多读**：领域服务使用进程内写锁，SQLite 使用 WAL、`BEGIN IMMEDIATE` 和 busy timeout。多个读者可并发读取已提交版本。
- **意图日志**：每个成功变更在构建派生产物前先写入 `intents`。最终版本、派生产物和意图完成状态在同一事务提交。若进程在构建或提交中死亡，事务不会出现半提交版本。
- **回放恢复**：回放开始前写入 `replay_runs=running` 和 `batch_states=running`。进程若在回放中死亡，重启扫描把运行中和孤立批次改为 `aborted`，不会把半次回放当成完整版本。
- **恢复报告**：报告只列出 SQLite 中确实存在的 pending/aborted 意图、运行、批次和最后完整版本，不使用推测数据。

## 业务错误码

错误响应统一为：

```json
{"code": "E_RECORD_INVALID", "message": "...", "details": {}}
```

| 错误码 | HTTP | 含义 |
|---|---:|---|
| `E_RECORD_INVALID` | 422 | 任一记录字段类型、范围或结构非法；整批不入库 |
| `E_STALE_BASE` | 409 | 提交或回放的基础版本不是当前版本；不改变状态 |
| `E_CONFLICTING_RECORD` | 409 | 相同批次和行号出现不同内容；首次提交保留 |
| `E_SNAPSHOT_INVALID` | 422 | 策略/快照结构非法、快照版本未变化或回放参数非法 |
| `E_REPLAY_TIMEOUT` | 504 | 至少一次回放超时；连接已关闭并计为失败 |
| `E_BATCH_BUDGET_EXCEEDED` | 422 | 单条样本连最小倍数也无法满足声明的批预算 |
| `E_VERSION_NOT_FOUND` | 404 | 查询的提交版本不存在 |
| `E_REPLAY_NOT_FOUND` | 404 | 回放运行不存在 |

## 验证

运行全部自动测试：

```powershell
pytest -q
```

本机实际验证结果（Python 3.14.4、SQLite 3.46.1）：

```text
13 passed, 436 warnings in 8.78s
```

覆盖内容：

- 一次性全量提交与拆成两个提交批次的最终采样集合逐条一致；
- 排除规则优先、包含规则和连接去重窗口生效；
- 批预算导致的确定性降倍、连接顺序拆分和不可满足预算的原子失败；
- 批次幂等、冲突保留首次记录、陈旧基础版本零状态变化；
- 单服务权重增量的三类产物级联失效与旧版本不可变；
- 快照替换不混合旧样本/旧策略；
- 真实 Uvicorn 下正常回放、真实挂起超时、`E_REPLAY_TIMEOUT` 与分离归因；
- 原生 TCP 探针确认超时后客户端发送 FIN/EOF，底层连接确实关闭；
- 6 个并发读者与 1 个串行写者只观察到完整版本；
- 运行中回放由恢复扫描标记为 `aborted`；
- 真实子进程 `os._exit(97)` 强杀后重启，当前版本仍为最后完整版本，pending 意图被标记中止，随后可继续提交。

端到端演示的实测摘要：

| 步骤 | 结果 |
|---|---|
| v1 全量导入 | 188 条；选中 29；4 批；58 个回放操作 |
| v2 样本增量 | 200 条；选中 31；4 批；62 个操作 |
| v3 仅改 checkout 权重 | `1.2 -> 0.2`；选中 31 -> 26；selection/batches/bias 三个 artifact id 全部变化，旧版本仍可查 |
| v4 正常回放 | 52 次请求；成功率 0.884615；采样成功率差 `-0.070385`；回放失败差 `0.0`；超时 0 |
| v5 挂起超时 | 首批 16 次请求全部超时；HTTP 504 `E_REPLAY_TIMEOUT`；超时归因 16 次，与采样偏差 `-0.070385` 分列 |
| 强杀恢复 | 退出码 97；最后完整版本仍为 v5；1 个 pending 意图中止；恢复后成功提交 v6 |

完整机器可读报告在 `runtime\demo\demo_report.json`。

## 设计说明与边界

- 本组件只访问固定本地样本、SQLite 和回环 HTTP 测试端点，不包含外部服务发现、真实 sidecar 接入或云环境依赖。
- 回放统计中的成功率按 HTTP 状态码 `< 400` 计算；超时、传输错误、非成功状态均计为失败。
- 回放批次按 `batch_id` 和操作序号固定；执行时批次串行、批内不同连接并发、同一连接内部串行，保证连接内顺序。
- 演示中的状态码结果确定，但回放耗时和分位会受本机调度影响；耗时类数据不作为跨机器逐微秒复现承诺。
- `seed`、样本内容序列、策略快照和记录身份共同决定采样结论；运行时不使用全局随机数状态。

# 本地版本化倒排索引组件

这是一个面向本地文档集合的 SQLite 倒排索引存储组件。它用 FastAPI 暴露本地 HTTP API，支持嵌套对象、数组展开、增量批次、历史版本查询、短语/合取/析取检索、版本回收、倒排段合并以及合并强杀后的启动恢复。

所有数据和进程都在本机运行，不依赖 Docker、外部服务、账号或网络资源。

## 1. 快速开始（Windows）

要求 Python 3.11 或更高版本，且 `python`、`pip` 在命令行可用。

```powershell
cd <项目目录>
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
python scripts\generate_samples.py
```

若 PowerShell 禁止激活脚本，也可以直接使用虚拟环境解释器：

```powershell
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
.\.venv\Scripts\python.exe scripts\generate_samples.py
```

启动服务：

```powershell
$env:INDEX_DB_PATH = "data\index.db"
$env:INDEX_CONFIG_PATH = "samples\index_config.json"
python -m uvicorn inverted_index.api:app --host 127.0.0.1 --port 8000
```

本实现使用进程内查询租约管理，因此本地服务应保持单 uvicorn 进程，不要设置 `--workers` 大于 1。SQLite 自身使用 WAL 模式处理并发读写。

运行测试与完整演示：

```powershell
python -m pytest -q
python scripts\verify_demo.py
```

演示脚本会启动和重启本地 uvicorn，通过操作系统进程 API 强杀合并中的服务，并生成 `artifacts\verification_report.json`。它不会承诺被强杀进程生成退出报告；恢复报告来自重启后读取的 SQLite 持久化事实。

## 2. 项目结构

```text
inverted_index/
  api.py          FastAPI 应用与统一错误响应
  errors.py       业务错误码
  paths.py        点分隔字段路径校验
  text.py         NFKC/大小写/分隔符规范化、数组位置展开
  schemas.py      HTTP 请求模型
  storage.py      SQLite 文档、MVCC 倒排、版本、段、恢复
samples/
  index_config.json
  documents/*.json
  batches/001_initial.json
  batches/002_incremental.json
scripts/
  generate_samples.py
  verify_demo.py
tests/            19 个核心/API/并发/恢复测试
artifacts/
  verification_report.json
  pytest.txt
  demo_run.txt
```

## 3. 字段路径与词项语义

索引字段在启动配置 `samples/index_config.json` 中声明：

```json
{
  "index_fields": [
    "title", "body", "tags", "author.name",
    "sections.title", "sections.paragraphs"
  ]
}
```

路径语法为点分隔标识符，例如 `author.name`。数组不需要方括号，组件自动展开：

- `tags` 表示字符串数组中的每个元素；
- `sections.paragraphs` 表示每个 section 对象中的 paragraphs 数组；
- 嵌套数组按数组下标递归遍历。

业务错误：

| 代码 | 场景 |
|---|---|
| `E_BAD_PATH` | 路径语法非法，例如 `body[0]`、`a..b` |
| `E_UNKNOWN_PATH` | 路径语法合法但未在索引字段配置中注册 |
| `E_DUPLICATE_ID` | 同一批次重复 `_id`，或 insert 已存在的 `_id` |
| `E_DOC_NOT_FOUND` | update/delete 的文档不存在 |
| `E_BAD_TERM` | 查询词项规范化后不是单一 token |
| `E_VERSION_NOT_FOUND` | 版本不存在 |
| `E_VERSION_RETIRED` | 版本已被回收 |
| `E_INVALID_REQUEST` | HTTP 请求模型校验失败 |
| `E_NOT_FOUND` | HTTP 路由不存在 |

写入批次在事务内先验证全部文档；任何错误都会回滚，不产生版本，也不会留下部分倒排修改。

### 词项规范化

统一规则为：

1. Unicode NFKC；
2. `casefold()`；
3. 所有非字母/数字字符（空白、连字符、下划线、点号等）作为分隔符；
4. 连续字母/数字序列形成一个词项。

例如 `Hello--WORLD_alpha.beta` 产生 `hello`、`world`、`alpha`、`beta`。

### 位置与短语

每个字段路径的位置编号从 0 开始，按文档顺序递增。数组元素之间插入一个位置空档，因此：

- 位置列表严格升序且不重复；
- 同一数组元素内的短语可连续匹配；
- 短语不会错误跨越两个数组元素；
- 多值字段与嵌套数组的位置顺序是确定性的。

短语匹配要求位置严格相邻，即 `p, p+1, ..., p+n-1`。

## 4. 版本化倒排模型

倒排条目按 `(字段路径, 词项)` 建立，每条条目包含：

- 文档标识 `doc_id`；
- 词频 `term_frequency`；
- 升序、无重复的位置列表；
- 可见区间 `[start_version, end_version)`；
- 所属索引段 `segment_id`。

同一文档同一字段路径发生更新时，旧条目的 `end_version` 被设为新版本，新条目从新版本开始，绝不会把词频和位置追加到旧条目。文档本身也使用 `[start_version, end_version)` 保存历史快照。

查询始终带一个解析后的版本 `V`：

```sql
start_version <= V AND (end_version IS NULL OR V < end_version)
```

因此新批次提交后，旧版本查询仍只能读取旧区间。查询在提交进行中启动时，版本发布锁会让它捕获提交前的版本；SQLite WAL 保证它读取不到未提交事务。

### 活跃查询与版本回收

每个查询开始时在进程内注册版本租约，结束时释放。`POST /v1/versions/retire` 使用确定规则：

```text
cutoff = current_version - retain_versions
候选 = version <= cutoff 且无活跃查询租约的版本
```

活跃查询引用的版本不会被回收。被回收版本再次查询返回 `E_VERSION_RETIRED`。物理清理边界还会进一步取“首个保留版本”和“最早活跃版本”的较小值，避免活跃旧查询的数据被删。

## 5. 索引段与合并恢复

每个提交批次新建一个 active 段。`POST /v1/index/merge`：

1. 在独立自动提交事务中写入 `operations(status='pending')` 意图；
2. 在 SQLite 事务内创建目标段；
3. 复制源段倒排区间；仅当相邻区间的词频和位置完全相同时才合并区间；
4. 删除源段条目，把源段标为 `merged`；
5. 将操作标为 `completed` 并提交。

批次提交也采用相同模式：意图先持久化，版本、文档、倒排条目和 `completed` 状态在同一 SQLite 事务中提交。

启动时 `recover()` 扫描所有 pending 意图：

- pending 批次若没有已提交版本，则标为 `aborted-uncommitted-batch`；事务内容已被 SQLite 回滚；
- pending 合并若无已提交目标段，则删除目标产物、恢复源段为 active，并标为 `discarded-uncommitted-merge`；
- 所有判断只使用 SQLite 中已持久化的版本、段、操作状态和意图内容。

恢复报告持久化在 `recovery_reports` 表中，并通过 `GET /v1/recovery/report` 返回。报告包含扫描数、最后完整版本、段状态和每个操作的处理决定。

## 6. HTTP API

### 写入批次

`POST /v1/batches`

```json
{
  "batch_id": "sample-initial",
  "inserts": [{"_id": "doc-001", "title": "Local ...", "tags": ["local"]}],
  "updates": [],
  "deletes": []
}
```

- `inserts` 中已存在的 `_id` 返回 `E_DUPLICATE_ID`；
- `updates` 整体替换当前文档，只对实际变化的字段路径关闭旧倒排并写新倒排；
- `deletes` 关闭文档及其全部当前倒排区间；
- 成功返回严格递增版本、受影响字段路径和新段 ID。

### 查询

`POST /v1/search`

```json
{
  "version": 1,
  "operator": "AND",
  "clauses": [
    {"kind": "term", "field": "tags", "term": "local"},
    {"kind": "phrase", "field": "body", "terms": ["local", "search", "index"]}
  ],
  "limit": 20,
  "offset": 0
}
```

`version` 可省略；省略时在请求开始时解析当前完整提交版本。`operator` 支持 `AND`、`OR`。结果按 `_id` 升序，`limit/offset` 是稳定的全序窗口。每个命中返回该版本下的文档快照。

### 倒排条目

```text
GET /v1/postings?path=tags&term=LOCAL&version=2
```

返回规范化词项、文档标识、词频和位置列表。

### 其他端点

| 方法与路径 | 用途 |
|---|---|
| `GET /health` | 健康状态、当前版本、活跃查询 |
| `GET /v1/versions/current` | 当前完整版本 |
| `GET /v1/versions` | 版本及回收状态 |
| `GET /v1/documents/{id}?version=` | 指定版本文档快照 |
| `GET /v1/index/segments` | 段状态、条目数和活跃查询 |
| `GET /v1/index/fields` | 已注册索引字段路径 |
| `POST /v1/index/merge` | 合并 active 段 |
| `POST /v1/versions/retire` | 回收旧版本 |
| `GET /v1/recovery/report` | 最近一次启动恢复报告 |
| `POST /v1/recovery/run` | 主动再执行一次 pending 意图扫描 |

所有错误响应统一为：

```json
{
  "error": {
    "code": "E_VERSION_RETIRED",
    "message": "Version has been retired",
    "details": {"version": 1}
  }
}
```

## 7. 固定样本

`scripts/generate_samples.py` 使用固定种子 `20261120`，重复运行会生成相同内容：

- 初始 12 个文档，含 `author` 嵌套对象、`tags` 数组、`sections` 对象数组、`paragraphs` 字符串数组；
- `001_initial.json` 全量写入 12 个文档；
- `002_incremental.json` 更新 `doc-002`、`doc-007`，删除 `doc-004`，新增 `doc-013`；
- `doc-002` 与 `doc-013` 在版本 2 引入 `quantum`；`doc-007` 引入 `migrated`。

## 8. 实际验证结果

本次实际执行环境为 WSL Ubuntu 26.04、Python 3.14.4。代码与命令保持 Windows 兼容；在原生 Windows 上请按上面的 PowerShell 命令使用 `python` 与 `pip`。

### 自动测试

```text
19 passed, 1 warning
```

测试覆盖：路径错误码、规范化和数组位置、短语边界、增量版本隔离、重复提交整体替换、失败批次原子性、分页、AND/OR、提交中查询、活跃查询保护、版本回收、段合并、批次强杀、合并强杀、HTTP 全流程与统一错误响应。

### 强杀恢复演示

`python scripts/verify_demo.py` 实际结果（摘要）：

```text
status: PASS
seed: 20261120
versions: initial=1, incremental=2
old version quantum ids: []
new version quantum ids: [doc-002, doc-013]
merge marker: merge:after_copy, pid captured
process_returncode: -9              # POSIX SIGKILL；Windows 下为非零 TerminateProcess 返回码
recovery pending_operations_found: 1
recovery decision: discarded-uncommitted-merge
recovery last_committed_version: 2
active segments after restart: 2 个源段
old/new version queries after restart: 与强杀前一致
```

完整机器可读报告在 `artifacts/verification_report.json`，其中包含被强杀进程的故障点标记、返回码、SQLite 恢复报告、强杀前后查询 ID 以及每个通过的检查项。

## 9. 边界与设计取舍

- 组件面向本地单服务进程；进程内查询租约保证同一 API 服务中的并发查询一致性。
- 未实现用户认证、分布式复制或远程网络访问；服务默认建议只监听 `127.0.0.1`。
- 字段路径语法只允许标识符和点号；数组通过文档结构自动展开。
- 合并不会改变任何版本区间，只重写段归属并压缩相邻且内容完全相同的区间。
- 恢复报告不推断或虚构强制终止时未落盘的信息。

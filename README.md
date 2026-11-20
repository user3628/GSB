# Alert Sieve

面向本地可观测性样本的告警降噪与根因关联领域组件。项目只使用本地确定性样本、Python 标准库 `sqlite3` 与 NumPy，不依赖 Docker、外部数据库或线上服务。

核心能力：

- 告警 JSONL/JSON 快照与带基础版本的增量批次入库。
- 批次标识 + 行内序号幂等；内容冲突保留首次提交。
- 告警、拓扑、抑制规则的版本化快照与增量。
- 三类派生索引：服务/级别聚合去重、指纹抑制规则索引、告警根因候选评分。
- 输入变化后的服务闭包级联失效与同事务原子重建。
- SQLite WAL 单写者/多读者并发；只读查询读取完整提交快照。
- 写前意图日志、强制终止恢复扫描与基于持久化事实的恢复报告。
- 统一业务错误码和 JSON 错误体。
- 可重复的本地 HTTP 端到端示例。

## 目录

```text
src/alert_sieve/
  domain.py       输入校验、规范 JSON、校验摘要、规则/拓扑/告警模型
  scoring.py      定点跳数衰减、时间邻近度、容差排序
  store.py        SQLite 事务、增量入库、级联重建、查询、恢复
  http_api.py     本地 HTTP JSON 接口
  server.py       服务启动器
  cli.py          alert-sieve CLI
  samples.py      告警 JSONL 加载器
samples/          固定本地样本与各版本校验摘要
examples/         HTTP 端到端示例
tests/            自动化验收测试
scripts/          样本构建与故障注入辅助脚本
artifacts/        实际运行生成的报告
```

## 环境与安装

要求 Python 3.11+、标准库 `sqlite3`、NumPy。

Windows PowerShell 示例：

```powershell
py -3.11 --version
py -3.11 -m pip install --upgrade pip
py -3.11 -m pip install numpy
py -3.11 -c "import sqlite3, numpy; print(sqlite3.sqlite_version, numpy.__version__)"
py -3.11 -m pip install -e .
```

Linux/macOS：

```bash
python3 --version
python3 -m pip install numpy
python3 -m pip install -e .
```

也可以只设置 `PYTHONPATH`，不安装：

```powershell
$env:PYTHONPATH = "$PWD\src"
```

## 快速启动

启动本地 HTTP 服务：

```powershell
python -m alert_sieve serve --db artifacts/alert_sieve.db --host 127.0.0.1 --port 8765
```

仅本地恢复演示需要故障注入：

```powershell
python -m alert_sieve serve --db artifacts/alert_sieve.db --port 8765 --enable-fault-injection
```

`--enable-fault-injection` 只开放受控的 `X-Debug-Crash-After-Products: 1` 与 `X-Debug-Sleep-After-Products-Ms` 请求头。正常生产验证不要开启。

健康检查：

```powershell
Invoke-RestMethod http://127.0.0.1:8765/v1/health
```

运行完整端到端示例：

```powershell
python examples/e2e_demo.py
```

示例会自行启动本地回环 HTTP 服务、加载样本、提交增量、触发级联、强制终止服务进程、重启恢复并比较恢复前后的产物摘要。完整结果写入 `artifacts/e2e_result.json`。

运行测试：

```powershell
$env:PYTHONPATH = "$PWD\src"
python -m pytest -q
```

重新生成固定样本与校验摘要：

```powershell
python scripts/build_samples.py
```

## HTTP 接口

成功响应统一为：

```json
{"ok": true, "request_id": "...", "data": {}}
```

错误响应统一为：

```json
{
  "ok": false,
  "request_id": "...",
  "error": {
    "code": "E_STALE_BASE",
    "message": "base_version does not match the current committed version",
    "details": {"base_version": 1, "current_version": 2}
  }
}
```

### 写入

| 方法 | 路径 | 用途 |
|---|---|---|
| POST | `/v1/alerts/snapshot` | 整份告警快照替换 |
| POST | `/v1/alerts/increments` | 告警增量批次 |
| POST | `/v1/alerts/batch` | `/v1/alerts/increments` 别名 |
| POST | `/v1/topology/snapshot` | 整份拓扑快照替换 |
| POST | `/v1/topology/increments` | 拓扑增量片段 |
| POST | `/v1/rules/snapshot` | 整份抑制规则快照替换 |
| POST | `/v1/rules/increments` | 抑制规则增量片段 |

### 查询

| 方法 | 路径 | 用途 |
|---|---|---|
| GET | `/v1/health` | 版本、SQLite、NumPy 与状态 |
| GET | `/v1/state` | 当前提交版本、校验摘要与表计数 |
| GET | `/v1/reduction?include_alerts=true` | 聚合去重结果与逐告警去重状态 |
| GET | `/v1/suppression-index?fingerprint=...` | 按指纹的抑制规则索引 |
| GET | `/v1/root-causes?service=...&alert_uid=...&limit=200` | 根因候选评分 |
| GET | `/v1/invalidations?limit=100` | 级联失效事件列表 |
| GET | `/v1/invalidations/{event_id}` | 单个失效事件的完整 target 轨迹 |
| GET | `/v1/recovery/report` | 恢复报告 |
| GET | `/v1/batches/{batch_id}` | 已提交增量批次结果 |

## 输入契约

### 告警

单个告警：

```json
{
  "timestamp_ms": 1735689600000,
  "fingerprint": "ledger-db-connection",
  "service": "ledger",
  "resource": "ledger-primary",
  "severity": "CRITICAL",
  "labels": {"env": "prod", "kind": "database", "region": "cn-east"}
}
```

- `timestamp_ms`：整数 Unix epoch 毫秒，范围 `0..2^63-1`。所有窗口运算均使用整数毫秒，不使用浮点时钟。
- `severity`：允许 `INFO/WARNING/ERROR/CRITICAL/FATAL/DEBUG/P0..P4`，内部统一大写；`WARN` 规范为 `WARNING`。
- 相同指纹必须使用相同服务与同一标签集合。标签变更必须在增量中声明为受影响的指纹。
- 增量中的 `line_no` 由数组顺序自动分配，从 1 开始。

增量：

```json
{
  "kind": "alert_increment",
  "batch_id": "inc-002",
  "base_version": 1,
  "version": 2,
  "checksum": "sha256:...",
  "alerts": [{"timestamp_ms": 1735689600000, "fingerprint": "fp", "service": "svc", "resource": "obj", "severity": "ERROR", "labels": {"env": "prod"}}],
  "affected_fingerprints": [
    {"fingerprint": "fp", "service": "svc", "labels": {"env": "prod"}}
  ]
}
```

整份快照：

```json
{
  "kind": "alert_snapshot",
  "version": 3,
  "batch_id": "snap-v3",
  "checksum": "sha256:...",
  "alerts": [{"line_no": 1, "timestamp_ms": 1735689600000, "fingerprint": "fp", "service": "svc", "resource": "obj", "severity": "ERROR", "labels": {"env": "prod"}}]
}
```

`alerts_v1.jsonl` 是 header + alert 记录格式：第一行是 `record_type=header`，后续每行是 `record_type=alert`。`alert_sieve.samples.load_alerts_jsonl()` 会校验 header 中的校验摘要并转换成 HTTP 快照请求体。

### 拓扑

快照：

```json
{
  "kind": "topology_snapshot",
  "version": 1,
  "checksum": "sha256:...",
  "nodes": ["edge-gateway", "checkout", "payment"],
  "edges": [{"source": "edge-gateway", "target": "checkout"}]
}
```

增量：

```json
{
  "kind": "topology_increment",
  "base_version": 1,
  "version": 2,
  "checksum": "sha256:...",
  "upsert_edges": [{"source": "checkout", "target": "notifications"}],
  "remove_edges": [{"source": "checkout", "target": "payment"}],
  "upsert_nodes": [],
  "remove_nodes": []
}
```

边表示 `source -> target`，即 source 依赖 target。禁止自环和重复边。

### 抑制规则

```json
{
  "kind": "rules_snapshot",
  "version": 1,
  "default_window_ms": 60000,
  "checksum": "sha256:...",
  "rules": [
    {
      "rule_id": "r-checkout-5xx",
      "scope": {"service": "checkout", "severity": "CRITICAL"},
      "match_labels": {"env": "prod"},
      "window_ms": 30000,
      "enabled": true
    }
  ]
}
```

增量使用 `upsert_rules`、`remove_rules`，并可选覆盖 `default_window_ms`。`scope.service`、`scope.severity` 支持 `"*"`；`match_labels` 为“所有键值均相等”的标签子集匹配。

### 校验摘要

所有写入均校验 SHA-256 摘要，支持 `sha256:<hex>` 或裸 64 位 hex。摘要由规范 JSON（键排序、无多余空白、UTF-8）计算。具体字段由 `domain.py` 中的以下函数定义：

- `alert_snapshot_checksum`
- `alert_increment_checksum`
- `topology_snapshot_checksum`
- `topology_increment_checksum`
- `rules_snapshot_checksum`
- `rules_increment_checksum`

`samples/` 中的样本摘要均由 `scripts/build_samples.py` 使用同一套函数生成。

## 派生语义

### 聚合去重与抑制索引

- 按 `(service, severity)` 聚合。
- 同一指纹的告警按 `(timestamp_ms, alert_uid)` 稳定排序。
- 有效抑制窗口：对该告警，若命中一个或多个启用规则，取命中规则的 `window_ms` 最大值；未命中任何规则时使用 `default_window_ms`。
- 窗口为闭区间：若当前告警与当前 canonical 告警的时间差 `<= effective_window_ms`，则当前告警被去重，并以当前告警的有效窗口判定。时间差恰好等于窗口边界时仍去重。
- 每个指纹、每个连续 canonical 会话只保留第一条。被去重告警的 `canonical_alert_uid` 指向前一条 canonical。
- 抑制索引保存指纹命中的规则集合、有效窗口、原始/保留/去重数量。
- 聚合索引保存原始数、保留数、去重数、指纹数。

### 根因候选评分

- 从源告警服务出发，在拓扑上执行 BFS，使用最短下游跳数；跳数必须 `>= 1`。
- 候选告警必须来自下游服务，时间戳不晚于源告警，并位于闭区间 `[source_ts - 300000, source_ts]` 毫秒内。
- 只有 canonical（未被去重）告警参与根因评分。
- 定点常数：

```text
SCORE_SCALE   = 1_000_000
HOP_BASE      =   600_000
TIME_WINDOW   =   300_000 ms
TOLERANCE     =     1_000 score units
```

计算公式：

```text
hop_component  = floor(HOP_BASE / 2^hops)
time_component = round((window - delta) * SCORE_SCALE / window)
score_fixed    = round(hop_component * time_component / SCORE_SCALE)
```

每一步均检查整数范围并保证 `0 <= component/score <= SCORE_SCALE`。越界会中止事务并返回 `E_REBUILD_CONFLICT`，不会提交部分结果。

排序先按精确分数降序；与当前 cluster 首位分差 `<= TOLERANCE` 的候选视为平局，再按 `(candidate_service, candidate_fingerprint, candidate_alert_uid)` 稳定排序并分配连续 rank。原始定点分数不会被容差改写。

## 级联失效与原子性

受影响服务集合会沿反向下游依赖扩展到所有上游依赖者。例如 `checkout -> payment` 变化会影响 `checkout` 自身以及 `edge-gateway`。

以下写入均按服务闭包重建三类产物：

- 告警增量/快照。
- 拓扑增量/快照。
- 抑制规则增量/快照。
- 指纹标签变更。

每次重建只删除并重算受影响服务/指纹的范围，而不是全量重建；整份快照替换是明确的全局替换例外。

一次提交内的版本元数据、三类产物、artifact 引用、失效事件和 intent 状态全部在同一 SQLite 写事务中提交。只读查询使用独立读事务；因此不会看到“抑制索引已更新而根因评分仍是旧值”的中间状态。`invalidation_targets` 中每个 target 的 `generation` 等于该次提交的全局 `commit_seq`。

## 并发模型

- SQLite 使用 WAL、`FULL` 同步与 30 秒 busy timeout。
- 进程内写操作由同一写者锁串行化；数据库事务使用 `BEGIN IMMEDIATE`。
- 读操作使用独立连接与显式读事务。
- 两个相同 base version 的并发增量：先提交者成功，后提交者在事务内重新校验后返回 `E_STALE_BASE`。
- 相同批次重复提交：后到请求等待写者锁，然后直接复用已持久化结果，不重复插入，不增加 `commit_seq`。
- 同一批次同一行内容不同：返回 `E_CONFLICTING_ALERT`，首次内容保留。

单数据库建议只运行一个服务进程。SQLite 本身会串行化跨进程写入并阻止混合事务，但自动恢复扫描假定数据库的所有者是本地单一服务进程。

## 持久化与恢复

持久化表包括：

- `alerts`、`fingerprint_meta`、`batches`
- `topology_nodes`、`topology_edges`
- `suppression_rules`
- `aggregate_index`、`suppression_index`、`alert_dedup`、`rootcause_scores`
- `artifact_refs`、`invalidation_events`、`invalidation_targets`
- `commits`、`intent_log`、`recovery_events`
- 版本元数据和全局 `runtime_state`

写流程：

1. 完整校验输入与校验摘要。
2. 先以独立事务持久化 `intent_log.status=pending`。
3. 开启主事务，写入本次提交的全部状态与产物。
4. 同一主事务提交三产物、版本、失效轨迹、commit 记录和 intent 完成状态。
5. 强制终止发生在第 3 步中时，SQLite 会丢弃未提交事务；`pending` intent 保留。
6. 重启时扫描 `pending` intent，确认没有对应 commit 后标记为 `aborted`，并写 `recovery_events`。

恢复报告只包含已提交数据库事实、剩余 `pending` 计数和恢复事件。强制终止不会承诺生成正常退出报告，但保证未完成级联事务不可见。

## 业务错误码

| 错误码 | HTTP | 含义 |
|---|---:|---|
| `E_ALERT_INVALID` | 400 | 告警字段、标签、指纹元数据或增量行号非法；整批不入库 |
| `E_STALE_BASE` | 409 | 增量 base version 不是当前已提交版本，或目标版本不前进 |
| `E_CONFLICTING_ALERT` | 409 | 相同批次/行号内容冲突，或批次内容与原提交不同 |
| `E_SNAPSHOT_INVALID` | 400 | 快照/增量结构、边、规则或校验摘要非法 |
| `E_ORDER_NOT_FOUND` | 404 | 查询对象不存在或路由/方法不存在 |
| `E_REBUILD_CONFLICT` | 409 | 同版本不同内容，或重建数值契约/完整性冲突 |

## 固定样本场景

`samples/` 包含：

- `alerts_v1.jsonl` / `alerts_v1_snapshot.json`：10 条初始告警。
- `alerts_increment_v2.json`：新增 1 条同指纹告警。
- `topology_v1.json`：`edge-gateway -> checkout -> {payment, inventory}`，`payment -> ledger`，`inventory -> warehouse`。
- `rules_v1.json`：5 条规则与 60 秒默认窗口。
- `topology_increment_v2.json`：只改 `checkout` 的出边，移除到 `payment`，增加 `notifications`。
- `rules_increment_v2.json`：只把 `checkout-5xx` 窗口从 30 秒改为 1 秒。
- `topology_increment_v3_crash.json`：用于在写事务中途强制终止。

## 实际验证结果

验证环境：

```text
Python 3.14.4
SQLite 3.46.1
NumPy 2.5.5
```

命令：

```text
PYTHONPATH=src python3 -m pytest -q
.................... [100%]
20 passed

python3 examples/e2e_demo.py
```

端到端结果：

- 初始样本：`raw_count=10`，`retained_count=6`，`deduped_count=4`。
- 告警增量 + 规则变化后：`raw_count=11`，`retained_count=7`，`deduped_count=4`。
- 拓扑 v2 后的 `edge-gateway` 首要根因候选为 `checkout`：1 跳、时间差 5000 ms、定点分 `295000/1000000`；第二候选为 `inventory`：2 跳、时间差 8000 ms、定点分 `146000/1000000`。`payment` 已因拓扑增量不再可达。
- 最新失效事件同时包含 `aggregate`、`suppression`、`rootcause` 三类 target，generation 均等于提交版本。
- 故障注入子进程以退出码 `91` 终止；重启后恢复报告记录 1 个 `aborted_after_uncommitted_rebuild` intent。
- 恢复后版本保持 `alert_version=2`、`topology_version=2`、`rules_version=2`，恢复前后降噪/抑制/根因产物摘要完全一致。
- 示例核对：`E_ORDER_NOT_FOUND`、`E_STALE_BASE`；自动化测试另覆盖 `E_ALERT_INVALID`、`E_CONFLICTING_ALERT`、`E_SNAPSHOT_INVALID`、`E_REBUILD_CONFLICT`。

完整报告：`artifacts/e2e_result.json`。

# 本地 SQLite 租约锁中间件

这是一个只依赖 Python 标准库的本地租约锁中间件，用于协调多个本地进程对命名共享资源（SQLite 资源行）的独占访问。锁元数据、依赖关系和带版本号的完整快照检查点持久化到 SQLite；单进程 HTTP 服务在内存中维护不可变快照及依赖索引，所有写入按提交串行化。

实现不依赖 Docker、外部数据库或网络服务。HTTP 只绑定回环地址，样本和测试均为确定性输入。

## 快速开始

要求：Python 3.11+，`python` 在 `PATH` 中。

Windows PowerShell / CMD：

```bat
python -m lease_lock.server --db runtime\leases.db --host 127.0.0.1 --port 8765
```

或直接运行：

```bat
run_server.cmd
```

完整演示（会清理并重建 `runtime/demo.db`，实际强杀一次服务进程再恢复）：

```bat
python scripts\demo.py
```

或：

```bat
run_demo.cmd
```

运行全部测试：

```bat
python -m unittest discover -s tests -v
```

递归清理本地运行状态：

```bat
rmdir /s /q runtime
```

Linux/macOS 上把 `python` 换成 `python3`，路径分隔符换成 `/` 即可；实现只使用跨平台标准库。

## 核心语义

- 锁键由独占租约表示。获取成功后返回新的 `lease_version` 和全局 `snapshot_version`。
- 租约到期时间在获取/续约事务内按 `now_ms + ttl_ms` 计算，所有比较使用服务端逻辑时钟的毫秒整数。
- 当前租约仍有效时，其他持有者获取返回 `E_LOCK_HELD`，错误详情含持有者、租约版本和剩余 TTL。
- 续约必须同时匹配持有者与 `lease_version`；任一不匹配返回 `E_STALE_LEASE`，且不改变快照版本。
- 强依赖父租约失效时，下游锁被释放并记录 `release_reason=DEPENDENCY_EXPIRED`；继续续约/释放该锁返回 `E_DEPENDENCY_EXPIRED`。
- 弱依赖父租约失效时，下游锁保持 `active`，但 `dependency_expired=true`。
- 增量提交携带 `base_version`，仅接受“基础版本等于当前快照版本”的提交。两个并发同基版本提交只有一个成功，另一个返回 `E_STALE_BASE`。
- 依赖环检测覆盖现有图和本次增量中的全部变更；发现环时整个增量回滚并返回 `E_DEPENDENCY_CYCLE`。
- 整份快照替换也要求严格 `base_version`，在单个 SQLite 事务中删除旧图并写入新图，提交后一次性切换内存快照，不混合新旧条目。

## 一致性设计

### 写入路径

每次写操作按以下顺序执行：

1. 在独立 SQLite 事务中写入 `intents` 的 `STARTED` 记录，并推进持久化时钟高水位。
2. 获取进程内写锁，执行 `BEGIN IMMEDIATE`，获得 SQLite 跨进程写事务。
3. 在同一事务内完成过期扫描、锁判定、增量校验、依赖卷重建以及受影响租约的持久化。
4. 将完整新快照写入 `snapshots` 检查点，更新 `snapshot_version`，同时把意图标记为 `COMMITTED`。
5. `COMMIT` 成功后才替换内存中的不可变快照，并在释放写锁前完成指针切换。

如果进程在步骤 1 后、步骤 5 前被强杀，未提交的 SQLite 事务自动回滚；重启扫描到 `STARTED` 意图后将其标记为 `ABORTED`。若已提交数据与最新完整检查点不一致，恢复程序会从检查点重写 `leases`/`lease_dependencies`，并生成重启恢复报告。

### 快照与增量索引

内存快照使用不可变 `Snapshot` 对象，包含：

- `leases`：锁键到租约记录的映射；
- `parents`：每条租约的直接父依赖；
- `children`：下游引用索引；
- `ancestors`：父链闭包索引；
- `derived_version`：每个锁键最近一次派生索引重建所基于的快照版本；
- `affected`：本次变更及所有传递下游。

增量提交只接收受影响租约条目。`Snapshot.evolve()` 浅复制状态容器，但只重新计算变更键及其传递下游的父子索引、祖先闭包和派生版本。持久化层也只 upsert 内容变化的租约和依赖行。`snapshots` 表保存完整检查点用于崩溃恢复，它不是应用层增量输入。

### 原子失效传播

过期扫描先标记所有自身到期的租约，再按受影响子图的拓扑顺序重建依赖状态。父租约一旦在本次事务内被释放，后续子节点立即观察到新状态：

- 直接强依赖不可用：子锁释放，`release_reason=DEPENDENCY_EXPIRED`；
- 直接弱依赖不可用：子锁保持活动，`dependency_expired=true`；
- 释放可能继续沿下游传播。

整个重建只形成一个新的全局快照版本，不会暴露部分节点已更新、部分节点仍旧的中间视图。`affected_leases` 随响应返回，满足失效范围可查询要求。

### 逻辑时钟

进程存活时，时间为：

```text
启动时 max(墙钟, 已持久化高水位) + monotonic 经过时间 + 本地注入偏移
```

时钟在进程内只增不减。演示和测试调用 `POST /v1/admin/clock` 注入偏移，不等待真实 TTL。意图写入、时钟推进和每次提交都会把高水位写入 SQLite，重启后不会回退到更早的逻辑时间。部署模型是一个中间件服务进程对应一个数据库；客户端可以是任意多个本地进程。额外的数据库版本探测允许短生命周期审计/查询实例刷新到最新完整检查点，但不应并发启动多个服务实例抢占同一数据库的意图恢复流程。

## HTTP API

所有响应为 JSON。成功体包含 `ok=true` 和 `snapshot_version`。错误体至少包含：

```json
{
  "ok": false,
  "code": "E_STALE_BASE",
  "message": "base_version does not match the current snapshot version",
  "snapshot_version": 7,
  "details": {"base_version": 6, "current_version": 7}
}
```

| 方法 | 路径 | 作用 |
|---|---|---|
| `GET` | `/healthz` | 健康检查 |
| `POST` | `/v1/locks/acquire` | 获取租约 |
| `POST` | `/v1/locks/renew` | 续约 |
| `POST` | `/v1/locks/release` | 主动释放 |
| `GET` | `/v1/locks/{lock_key}` | 查询租约、父链、祖先和下游 |
| `GET` | `/v1/snapshot` | 读取当前完整快照（触发到期扫描） |
| `GET` | `/v1/snapshot?version=N` | 读取指定版本的完整历史快照 |
| `POST` | `/v1/snapshot/commit` | 增量提交 |
| `PUT` / `POST` | `/v1/snapshot/replace` | 整份快照替换 |
| `GET` | `/v1/recovery/report` | 最近一次重启恢复报告 |
| `POST` | `/v1/admin/clock` | 注入 `advance_ms` 本地时钟偏移 |
| `POST` | `/v1/admin/sweep` | 显式执行到期扫描 |
| `GET` | `/v1/resources/{lock_key}` | 查询命名资源行 |

### 获取、续约、释放

```json
POST /v1/locks/acquire
{
  "lock_key": "exclusive",
  "holder": "worker-a",
  "ttl_ms": 5000,
  "dependencies": [{"lock_key": "root", "mode": "strong"}]
}
```

```json
POST /v1/locks/renew
{
  "lock_key": "exclusive",
  "holder": "worker-a",
  "lease_version": 1,
  "ttl_ms": 5000
}
```

```json
POST /v1/locks/release
{
  "lock_key": "exclusive",
  "holder": "worker-a",
  "lease_version": 2
}
```

### 增量提交

```json
POST /v1/snapshot/commit
{
  "base_version": 5,
  "changes": [
    {
      "lock_key": "db",
      "ttl_ms": 45000,
      "expected_lease_version": 1
    }
  ]
}
```

`changes` 只包含需要更新的租约。可更新 `holder`、`ttl_ms`、`dependencies`；`expected_lease_version` 是可选的额外乐观校验。`base_version` 必须是当前版本。

### 整份快照替换

```json
PUT /v1/snapshot/replace
{
  "base_version": 0,
  "leases": [
    {
      "lock_key": "root",
      "holder": "svc-root",
      "ttl_ms": 2000,
      "lease_version": 1,
      "dependencies": []
    }
  ]
}
```

事件时间按本事务的 `now_ms` 计算到期点。若同名锁历史上已有更高租约版本，系统会推进版本，避免整份替换后旧令牌重新有效。

## 错误码

| 错误码 | HTTP | 含义 |
|---|---:|---|
| `E_LOCK_HELD` | 409 | 锁仍被有效租约持有 |
| `E_STALE_LEASE` | 409 | 持有者/租约版本不匹配，或租约已过期/释放 |
| `E_STALE_BASE` | 409 | 增量基础版本不是当前快照版本 |
| `E_LEASE_NOT_FOUND` | 404 | 锁键/资源不存在 |
| `E_DEPENDENCY_EXPIRED` | 409 | 强依赖已失效，下游锁已被释放 |
| `E_DEPENDENCY_CYCLE` | 409 | 新增依赖形成环，整个提交被拒绝 |
| `E_SNAPSHOT_NOT_FOUND` | 404 | 请求的历史快照版本不存在 |
| `E_INVALID_REQUEST` | 400 | 输入格式、参数或路由不合法 |

## SQLite 持久化

数据库位于 `--db` 指定路径，默认 `runtime/leases.db`。主要表：

- `leases`：锁键、持有者、租约版本、获取时间、TTL、到期时间、状态、释放原因；
- `lease_dependencies`：父键与 `strong`/`weak` 模式；
- `lock_versions`：每个锁键的租约版本高水位；
- `snapshots`：每次提交的完整版本检查点和 SHA-256 校验；
- `intents`：`STARTED`/`COMMITTED`/`ABORTED` 意图审计；
- `recovery_reports`：启动恢复报告；
- `resources`：命名共享资源行；
- `metadata`：当前快照版本和逻辑时钟高水位。

SQLite 使用 WAL、`synchronous=FULL`、`BEGIN IMMEDIATE` 和 15 秒 busy timeout。正常单元测试运行时可看到 `runtime/*.db-wal` 和 `*.db-shm`，均为 SQLite 正常运行文件。

## 强制终止与恢复边界

恢复报告明确包含：

```json
{
  "report_type": "restart_recovery",
  "complete_exit_report": false,
  "notice": "This report was reconstructed from persisted SQLite facts after a restart. A forced process termination cannot produce a graceful exit report."
}
```

强制终止后只能根据持久化事实重建“重启恢复报告”，不会伪称生成了完整退出报告。只有在演示/测试显式启用 `--enable-faults` 时，`_debug_fault=after_intent` 才会在意图提交后立即调用 `os._exit(86)`。普通业务请求无法触发故障注入。

## 确定性样本与演示步骤

样本文件：`samples/deterministic_leases.json`。依赖图：

```text
root
└── strong: db
    ├── weak: cache
    └── strong: reports
            └── weak: cache (额外依赖)

unrelated
```

`scripts/demo.py` 每次清理 `runtime/demo.db` 后启动真实回环 HTTP 服务，依次验证：

1. 从 `base_version=0` 加载整份样本快照；
2. 获取新锁、同锁二次获取返回 `E_LOCK_HELD`、错误租约版本续约返回 `E_STALE_LEASE`、正确续约和释放；
3. 仅修改 `db` 的 TTL，确认受影响租约为 `cache/db/reports`，版本从 4 到 5；
4. 提交 `root -> reports` 形成环，返回 `E_DEPENDENCY_CYCLE`，快照仍为 5；
5. 注入 2500ms 时钟偏移并扫描，`root` 过期后 `db/reports` 强依赖释放，`cache` 弱依赖保持活动且标记过期，快照变为 6；
6. 在增量意图写入后强杀服务，重启后报告 1 条被中止意图，恢复版本仍为 6。

完整机器可读结果写入 `artifacts/demo-report.json`。

## 实际验证记录

在交付工作区执行：

```text
python3 -m unittest discover -s tests -v
Ran 14 tests in 1.263s
OK
```

覆盖获取/占用/严格续约、过期后重获、增量 TTL 级联、依赖改挂载后的父子索引重建、同基版本并发提交、依赖环拒绝、强/弱依赖过期、整份替换与历史快照、锁版本单调性、数据库检查点刷新、待处理意图恢复、物化状态修复以及 HTTP 错误体。

执行 `python3 scripts/demo.py` 成功，关键结果为：

```text
full snapshot load: snapshot_version=1
incremental db TTL: snapshot_version=5, affected=[cache, db, reports]
cycle commit: E_DEPENDENCY_CYCLE, snapshot_version stays 5
parent expiry sweep: snapshot_version=6, affected=[cache, db, reports, root]
strong child db: released / DEPENDENCY_EXPIRED
weak child cache: active / dependency_expired=true
forced termination: connection lost, server exit code 86
restart recovery: aborted_intent_count=1, recovered_snapshot_version=6,
                  complete_exit_report=false
```

实际执行环境为任务提供的 Linux 容器和 Python 3.14.4。代码仅使用跨平台标准库，并提供了 Windows `run_server.cmd`、`run_demo.cmd`；尚未在真实 Windows 主机上重复上述命令，因此未声称完成 Windows 本机实测。

# 本地 HTTP 接口手册

所有端点仅在回环地址监听（默认 `127.0.0.1:18080`），请求/响应均为 JSON。
成功信封：`{"ok": true, "data": ...}`；失败信封：

```json
{"ok": false, "error": {"code": "E_...", "message": "...", "details": {...}}}
```

## 启动

```bash
go build -o bin/arbiter ./cmd/arbiter
./bin/arbiter -addr 127.0.0.1:18080 -db data/arbiter.db -seed sample/seed/rows.json
```

| 参数 | 说明 |
| --- | --- |
| `-addr` | 必须为回环地址，非回环绑定直接拒绝 |
| `-db` | 本地 SQLite 文件路径 |
| `-seed` | 数据库为空时播种确定性样本 |
| `-reaper-ms` | 后台租约到期扫描周期 |
| `-recovery-report` | 启动恢复报告 JSON 输出路径 |
| `-enable-fault-injection` | 挂载 `/debug/fault/*`（仅演示强制终止） |

## 端点

### `POST /rows` 版本化读

```json
{"row_id": "doc-100/body", "snapshot_ver": 3, "holder": "alice", "register_snapshot": true}
```

`snapshot_ver > 当前 commit_ver` → `E_SNAPSHOT_AHEAD`；命中已回收版本 →
`E_VERSION_RETIRED`；`register_snapshot=true` 同时登记活跃读快照。

### `POST /rows/open` 编辑面板打开

返回快照处行内容 + 当前租约状态 + `editable`，并预热快速预检缓存。
`snapshot_ver=0` 表示读取最新已提交版本。

### `POST /leases/acquire` 申请写权

```json
{"row_id": "r1", "holder": "alice", "ttl_ms": 5000, "wait_ms": 0, "request_id": "req-1"}
```

冲突且 `wait_ms=0` 立即返回 `E_WRITE_LEASE_HELD`（附 `holder`、`expires_at_ms`）；
`wait_ms>0` 进入有界可中断等待，超时返回 `E_LEASE_TIMEOUT`。

### `POST /leases/renew` 续约

携带 `lease_version`；版本落后 → `E_LEASE_STALE`；已过期/被抢占 → `E_LEASE_LOST`。

### `POST /leases/release` 主动释放

携带 `lease_version`，成功后唤醒同一行的等待队列。

### `POST /edits/precheck` 快速预检

```json
{"row_id": "r1", "holder": "alice", "lease_version": 2, "snapshot_ver": 0,
 "budget_ms": 200, "allow_lease_refresh": true}
```

返回 `editable`、`completed`、`mode`、`basis` 与缓存的行版本/租约事实；
`budget_ms=0` 或预算内未完成返回 `E_PRECHECK_INCOMPLETE`。

### `POST /edits/reconcile` 完整路径只读复核

返回 `pass`、`failure_code`、`reasons` 与逐项 `diff`（实际行版本、实际持有者、
实际租约号、快照号等），用于解释“预判可编辑但提交失败”。

### `POST /edits/commit` 提交修改

```json
{"row_id": "r1", "holder": "alice", "lease_version": 2,
 "base_row_version": 1, "content": "new text", "request_id": "req-2"}
```

租约校验与行版本校验在同一事务完成；成功返回新 `row_version` 与 `commit_ver`，
租约随之 `released`。支持 `"deleted": true` 提交删除标记版本。

### `POST /recycle` 旧版本回收

```json
{"row_id": "r1", "min_retain_snapshots": 5, "min_retain_versions": 2}
```

返回保留与回收的行版本号；活跃读快照保护下限内的版本不会被回收。

### `GET/POST /snapshots` 读快照注册

`GET` 列出注册项；`POST` 登记/`action=release` 释放/`action=prune_orphans`
清理崩溃遗留的 orphan 注册。

### `GET /recovery/report` 恢复审计

返回启动恢复产生的全部事实事件。

## 调试端点（需显式开关）

- `POST /debug/fault/acquire`：写入未完成的授予意图与 granted 租约。
- `POST /debug/fault/commit`：写入未完成的提交意图、孤儿行版本并推进版本号。
- `POST /debug/fault/recycle`：写入未完成回收意图。

调用后对进程 `kill -9`（Windows 下用 `taskkill /F /PID`），重启即可观察恢复。

# walgroup

`walgroup` 是一个可嵌入本地存储引擎的 WAL 段组提交与检查点协调组件。它负责：

- WAL 段文件、记录校验、跨段连续 LSN 与原子轮转；
- 单写者事务组提交、共享 fsync 屏障、事务偏移与幂等/冲突判定；
- 检查点整体替换、页版本摘要、可服务 LSN 与陈旧读裁决；
- `SEALED -> RECLAIMABLE -> RECLAIMED` 回收顺序与真实文件删除；
- SQLite 持久化意图日志、段目录、事务状态、检查点元数据；
- 强杀后的意图扫描、未完成事务回滚、段头修复与事实型恢复报告；
- 本地 HTTP 接口、确定性样本生成器与可重复崩溃演示。

核心仅依赖 Go 标准库和纯 Go SQLite 驱动 `modernc.org/sqlite`。测试、样本和演示不访问网络，也没有 Docker 依赖。

## 快速开始

要求 Go 1.22 或更高版本：

```sh
go version
go env
go build ./...
go test ./...
go vet ./...
```

`demo` 会在回环地址启动真实的临时 HTTP 服务，并通过该 HTTP 接口执行全部固定种子动作；随后关闭、重开引擎并输出恢复报告。生成固定种子样本并运行完整演示：

```bash
./scripts/demo.sh
```

Windows PowerShell：

```powershell
.\scripts\demo.ps1
```

也可以手动运行：

```sh
go run ./cmd/walgroup generate -seed 42 -out samples/workload-42.json
go run ./cmd/walgroup demo -workload samples/workload-42.json -data var/demo -clean -report var/demo/report.json
go run ./cmd/walgroup crash-demo -data var/crash -report var/crash/report.json
```

启动本地 HTTP 服务：

```sh
go run ./cmd/walgroup serve -data var/engine -addr 127.0.0.1:8080
```

服务默认只监听回环地址。所有业务错误均返回稳定错误码，例如：

```json
{
  "error": {
    "code": "E_RECLAIM_UNSAFE",
    "message": "segment 3 record LSN 17 is not covered by checkpoint replay start 16",
    "segment_no": 3,
    "earliest_lsn": 15,
    "earliest_uncovered_lsn": 17
  }
}
```

## 关键语义

### 事务状态

合法持久化路径：

```text
APPENDING -> GROUPED -> DURABLE
APPENDING -> ABORTED
GROUPED   -> ABORTED
```

`DURABLE` 只能由组提交的 fsync 屏障路径写入。公共 `TransitionTransaction` 只允许管理性 `ABORTED` 终态，不允许绕过 WAL 直接宣称持久化。非法迁移返回 `E_INVALID_TRANSITION`，且不更新 SQLite 行。

### 段状态

唯一合法路径：

```text
OPEN -> SEALED -> RECLAIMABLE -> RECLAIMED
```

- 仅 `OPEN` 段可追加。
- 组提交整体位于一个段内，事务不会被拆段；组总大小或事务数超过单段上限时返回 `E_ROTATE_FAILED`，且不写入该组 WAL 字节。
- 轮转先写意图，再创建并 fsync 新段头，最后用一个 SQLite 事务同时封存旧段、登记新段并移动 current 指针。失败时删除未登记文件，旧段保持 `OPEN`。
- `RECLAIMABLE` 只能由检查点覆盖校验通过后写入；只有文件真实删除成功后才写入 `RECLAIMED`。

### 组提交与屏障

组提交流程：

1. 校验事务指纹、幂等性和显式页版本；
2. 写 `GROUP_COMMIT` 意图，并把事务置为 `GROUPED`；
3. 写记录、更新段头记录数/事务数/LastLSN；
4. 对段文件执行一次 `fsync`（组内共享屏障）；
5. 在单个 SQLite 事务中把全部事务置为 `DURABLE`、写记录目录/页版本/段目录/`next_lsn`，并完成意图；
6. 返回每个事务的 `[start_offset,end_offset)` 与 `[start_lsn,end_lsn]`。

任一步失败都会截断到意图中的旧文件大小、恢复旧段头计数，并把整组事务置为 `ABORTED`。不会出现组内部分事务 `DURABLE`。若物理或元数据回滚本身未能落盘，则保留 `PENDING` 意图并返回 `E_RECOVERY_REQUIRED` 封锁后续写读，重启后由恢复流程完成幂等回滚。

### 检查点与读裁决

检查点由引擎依据 `replay_start_lsn` 时的持久化页版本集合生成，并计算：

- 排序后的页版本集合摘要；
- 包含序号、回放起点和页集合摘要的整体摘要。

序号必须严格递增，回放起点不得后退；`ExpectedPages` 或 `ExpectedDigest` 不符返回 `E_CHECKPOINT_INVALID`。旧检查点只用同一个 SQLite 事务整体替换。

`ReadPage(page, lsn)` 总是选择 `record.lsn <= lsn` 的最新页版本，绝不返回比请求快照更新的内容：

- `lsn <` 当前最小可服务 LSN：`E_SNAPSHOT_RETIRED`；
- 页最新版本未被活动检查点覆盖：`E_CHECKPOINT_LAG`；
- 无检查点时，仅在所有 WAL 段仍未回收的情况下从持久化页版本读取。

### 回收

一次回收先校验全部目标段：

- 段必须为 `OPEN` 已封存后的 `SEALED`；
- 段内全部记录 LSN 必须 `<= checkpoint.replay_start_lsn`。

否则返回 `E_RECLAIM_UNSAFE`，并在可能时附带最早未覆盖 LSN。校验通过后写 `RECLAIM` 意图，原子标记 `RECLAIMABLE`，删除文件，再标记 `RECLAIMED`。若在标记后、删除完成前退出，重启会继续删除并完成状态迁移。

## HTTP API

| Method | Path | 说明 |
|---|---|---|
| GET | `/healthz` | 健康与最近恢复状态 |
| GET | `/v1/recovery` | 最近一次启动的恢复报告 |
| GET | `/v1/segments` | 段目录与当前检查点 |
| GET | `/v1/segments/{no}/parse` | 严格解析并返回段头/记录 |
| POST | `/v1/transactions` | 暂存 `APPENDING` 事务 |
| GET | `/v1/transactions/{id}` | 查询事务持久状态 |
| POST | `/v1/commit-groups` | 提交完整事务组或 staged IDs |
| POST | `/v1/checkpoints` | 原子推进检查点 |
| GET | `/v1/checkpoints/current` | 当前活动检查点 |
| POST | `/v1/reclaim` | 校验并回收段 |
| POST | `/v1/segments/{no}/seal` | 显式封存当前段 |
| GET | `/v1/pages/{page}?lsn=N` | 按快照 LSN 读取页 |
| POST | `/v1/replay` | 从检查点起点做一致性重放 |
| GET | `/v1/min-serviceable-lsn` | 当前最小可服务 LSN |

提交组示例：

```json
{
  "transactions": [
    {
      "id": "txn-1001",
      "records": [
        {"page_no": 7, "value": "post-image bytes"}
      ]
    }
  ]
}
```

`PageUpdate.data` 使用 JSON base64 字节；`value` 是 UTF-8 文本便捷字段。两者同时出现时必须一致。

检查点示例：

```json
{"sequence": 2, "replay_start_lsn": 16}
```

## 恢复报告

每次 `Open` 都只依据以下持久化事实重建报告：

- 已落盘段文件及严格校验后的段头/记录；
- SQLite 中已提交的段目录、事务状态、页版本、检查点；
- 未完成 `PENDING`/`MARKED` 意图。

报告同时写入 `<data>/recovery-report.json`，HTTP `GET /v1/recovery` 返回同一结构。`status=RECOVERED` 表示本次启动处理过未完成事实；终态意图不会污染后续干净重启，后续启动为 `CLEAN`。

## 错误码

| Code | 含义 |
|---|---|
| `E_SEGMENT_CORRUPT` | 段头/记录/LSN/计数校验失败，包含段序号与期望起始 LSN |
| `E_ROTATE_FAILED` | 轮转无法原子完成；旧段保持可用 |
| `E_INVALID_TRANSITION` | 非法状态迁移，持久化状态不变 |
| `E_CONFLICTING_TXN` | 同一事务 ID 内容不同 |
| `E_GROUP_COMMIT_FAILED` | 组提交写/同步/元数据失败，整组回滚 |
| `E_CHECKPOINT_INVALID` | 序号、回放起点、摘要或页集合无效 |
| `E_RECLAIM_UNSAFE` | 检查点未覆盖目标段，附最早未覆盖 LSN |
| `E_RECLAIM_FAILED` | 回收标记/删除失败；重启可续作 |
| `E_SNAPSHOT_RETIRED` | 请求快照早于最小可服务 LSN |
| `E_CHECKPOINT_LAG` | 页最新版本尚未被检查点覆盖 |
| `E_RECOVERY_REQUIRED` | 回滚未能完整落盘，必须重启以执行意图恢复 |

更多实现细节见 [docs/DESIGN.md](docs/DESIGN.md)，实际验证记录见 [docs/RESULTS.md](docs/RESULTS.md)。

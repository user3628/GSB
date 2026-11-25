# Shard Route Ledger

面向本地多逻辑库的分库分表路由、领域数据编辑与运维验证应用。应用使用 Go + 纯 Go SQLite，提供本地 HTTP API 与浏览器控制台，不依赖 Docker、外部数据库或真实集群。

## 已实现能力

- 带版本、生成时间、分片键、算法参数、物理表清单、区间权重和 SHA-256 摘要的整份路由快照。
- 快照语义校验：逻辑表唯一、物理表存在、区间上下界合法、区间不重叠、聚合依赖存在；失败保持旧快照完全可用。
- 增量片段：单表 upsert、区间重划、逻辑表删除、物理表新增、聚合视图更新；严格校验 `base_version`。
- 单写者事务提交、整批原子生效、`commit_id` 幂等复用与内容冲突裁决。
- 等值、IN、范围路由；`mod`、固定种子 `hash`、`range` 三种分片算法；稳定去重排序。
- 版本绑定读：旧观察版本、缓存传播滞后与观察版本超前分别裁决，不返回冒充新版本的旧路由。
- 按逻辑表维护物理表清单、边界索引、路由版本和引用关系；受影响表及下游聚合视图同事务级联重建。
- SQLite 持久化快照、逻辑表、区间、路由缓存、聚合缓存、缓存引用、提交回执、意图日志和恢复报告。
- 意图先落盘、缓存事务后提交；进程被 SIGKILL/强退后启动扫描 pending 意图，按已落盘目标版本决定提交或中止，并校验/重建缓存。
- 五类可复现异常注入，返回五个不同业务码和上下文 JSON。
- 内嵌浏览器控制台与固定种子样本、Bash 演示脚本、纯 Go 跨平台演示命令。

## 快速启动

要求 Go 1.22 或以上。当前 `go.mod` 固定使用 `modernc.org/sqlite v1.34.5`。

```bash
go version
go env GOMODCACHE
# 如默认缓存不可写：go env -w GOMODCACHE=/path/to/writable/modcache
go mod download

# 程序内置生成器产出固定种子样本
go run ./cmd/routerd samples -out samples

# 端口、数据库路径从 config.json 读取
go run ./cmd/routerd serve -config config.json
```

打开 <http://127.0.0.1:18190>。默认配置位于 `config.json`，端口为 `127.0.0.1:18190`，SQLite 文件为 `var/router.db`。如端口冲突，修改 `listen` 即可；Bash 演示可额外使用 `PORT=18191`。

Windows PowerShell 示例：

```powershell
go build -o routerd.exe ./cmd/routerd
.\routerd.exe samples -out .\samples
.\routerd.exe serve -config .\config.json
```

## 演示与验收

纯 Go 跨平台演示（推荐，Windows/macOS/Linux 均可）：

```bash
go run ./cmd/routerdemo
```

它会自动构建 `routerd`、生成样本、启动本地 HTTP 服务并完成：

1. 整份快照 v1 加载；
2. 仅修改 `orders` 分片区间的 v2 增量；
3. v1 观察版本被替换后的 `E_STALE_READ` 裁决；
4. 意图落盘后子进程被平台强制终止、重启恢复、v2 继续可读；
5. 五类异常码矩阵。

Bash 版本：

```bash
NO_COLOR=1 ./scripts/demo.sh
```

本次实际输出见 [docs/VALIDATION.md](docs/VALIDATION.md)，原始日志位于 `artifacts/validation/`。

## HTTP API

成功响应统一为 `{"ok":true,"data":...}`；失败响应统一为：

```json
{
  "ok": false,
  "error": {
    "code": "E_STALE_READ",
    "message": "version-bound read rejected",
    "context": {
      "table_id": "orders",
      "cache_version": 2,
      "observed_version": 1,
      "current_version": 2,
      "reason": "observed_version_replaced"
    }
  }
}
```

主要接口：

| 方法 | 路径 | 说明 |
|---|---|---|
| `GET` | `/` | 内嵌控制台 |
| `GET` | `/api/health` | 健康与当前版本 |
| `GET` | `/api/state` | 当前快照、表状态、聚合缓存摘要 |
| `GET` | `/api/snapshots` | 快照版本历史 |
| `GET` | `/api/snapshots/{version}` | 指定版本完整快照 |
| `POST` | `/api/snapshots` | 提交整份快照 |
| `POST` | `/api/increments` | 提交增量片段 |
| `POST` | `/api/query` | 版本绑定路由查询 |
| `GET` | `/api/caches` | 路由缓存、边界索引、聚合缓存与引用 |
| `GET` | `/api/intents` | 意图日志 |
| `GET` | `/api/commits` | 提交回执 |
| `GET` | `/api/commits/{id}` | 指定提交回执 |
| `GET` | `/api/recovery` | 最近一次持久化恢复报告 |
| `GET` | `/api/samples` | 全部内置样本 |
| `GET` | `/api/samples/{name}` | 指定内置样本 |

提交整份快照的请求体为：

```json
{
  "commit_id": "optional-idempotency-key",
  "snapshot": { "...": "model.Snapshot" },
  "seal_checksum": true,
  "inject": ""
}
```

`seal_checksum=true` 时服务端按规范化内容重算摘要，便于控制台编辑；直接使用样本时样本已带正确摘要。

为便于脚本调用，`POST /api/snapshots` 也接受不带包装的 `model.Snapshot` JSON，也就是可以直接 `--data-binary @samples/snapshot-v1.json`；此时 `commit_id` 由内容摘要自动生成。

增量请求体直接使用 `model.Increment`：

```json
{
  "commit_id": "inc-orders-repartition-v2",
  "base_version": 1,
  "generated_at": "2026-01-02T00:00:00Z",
  "changes": [
    {
      "op": "replace_ranges",
      "table_id": "orders",
      "ranges": [
        {
          "id": "slot-00-01",
          "lower": 0,
          "upper": 1,
          "lower_inclusive": true,
          "upper_inclusive": true,
          "physical_table": "db0.orders_00",
          "weight": 2
        }
      ]
    }
  ]
}
```

支持的 `changes[].op`：`upsert_table`、`replace_ranges`、`delete_table`。仅新增物理表时可使用顶层 `add_physical_tables`。

## 查询与分片语义

等值：

```json
{"table_id":"orders","observed_version":2,"kind":"eq","value":"1"}
```

IN：

```json
{"table_id":"orders","observed_version":2,"kind":"in","values":["0","1","5"]}
```

范围（端点包含关系必须显式给出）：

```json
{
  "table_id":"ledger",
  "observed_version":2,
  "kind":"range",
  "lower":"999",
  "upper":"1000",
  "include_lower":true,
  "include_upper":false
}
```

- `mod`：槽位为 `key mod modulus`（负数按非负余数规范化），区间在槽位空间匹配。
- `hash`：固定种子加 FNV-1a 64 后取模；相同输入跨进程、跨请求结果一致。
- `range`：键值直接与区间比较，使用显式开闭端点判断区间交集。
- `mod` 范围查询会精确计算连续键区间可能覆盖的余数区间，包括跨模边界；空键区间返回空物理表集合。
- `hash` 无法从数值范围推导有限原像，因此范围查询显式标记 `conservative=true` 并返回全部映射物理表，不静默漏表。
- 物理表集合去重后稳定排序；同一版本、同一请求重复计算 `physical_tables` 和 `hits` 逐项一致。
- 多个逻辑表可映射到同一物理表（样本中的 `audit_eu` / `audit_us`）。

## 版本绑定裁决

令 `observed` 为请求观察版本，`current` 为当前完整提交版本，`cache` 为逻辑表路由缓存版本，`expected` 为该表定义最近一次变化版本。

| 条件 | 行为 |
|---|---|
| `observed > current` | `E_OBSERVED_AHEAD` |
| `observed < current` | `E_STALE_READ`，旧版本已被替换 |
| `observed == current` 且 `expected > cache` | `E_STALE_READ`，传播未完成 |
| `observed == current` 且 `expected <= cache <= current` | 返回路由，标注 `cache_version` / `used_version` |

未受影响逻辑表可保留旧缓存版本。例如 v2 只改 `orders`，对 `events` 带 `observed_version=2` 查询可以成功，并明确标注 `used_version=1`；这不会把旧路由冒充成 v2。

## 异常注入与业务码

五类注入均返回 JSON 错误体，且顶层业务码互不相同：

| 注入 | 触发方式 | 业务码 | HTTP |
|---|---|---|---|
| 物理表映射缺失 | 快照/增量 `inject:"mapping_missing"` | `E_PHYSICAL_TABLE_MISSING` | 400 |
| 分片区间重叠 | 快照/增量 `inject:"range_overlap"` | `E_RANGE_OVERLAP` | 400 |
| 基础版本回退 | 增量 `inject:"base_regression"` | `E_STALE_BASE` | 409 |
| 观察版本超前 | 查询 `inject:"observed_ahead"` | `E_OBSERVED_AHEAD` | 409 |
| 重建中断标记 | 增量 `inject:"rebuild_interrupt"` | `E_REBUILD_INTERRUPTED` | 409 |

前两类错误同时带 `category:"E_SNAPSHOT_INVALID"`，既有明确原因码，又保留快照非法大类。摘要不一致、重复逻辑表等通用校验失败返回 `E_SNAPSHOT_INVALID`。

整份快照请求的 JSON 语法错误同样返回 `E_SNAPSHOT_INVALID`；增量/查询请求的通用 JSON 语法错误返回 `E_INVALID_JSON`。带故障注入的增量不会因为原摘要字段被改写而遮蔽目标故障码。

其他重要业务码：`E_STALE_READ`、`E_CONFLICTING_REQUEST`、`E_VERSION_NOT_MONOTONIC`、`E_TABLE_NOT_FOUND`、`E_QUERY_INVALID`、`E_INVALID_JSON`、`E_CACHE_INCONSISTENT`。

## 持久化与恢复

每个提交先独立事务写入 `intents` 并落盘，再在单写者锁内执行主事务：

1. 写新版本快照及规范化逻辑表、区间行；
2. 原子替换路由缓存、边界索引、缓存引用和下游聚合缓存；
3. 写提交回执；
4. 在同一主事务内把意图标为 committed；
5. 提交 SQLite 事务后切换内存只读快照。

`rebuild_interrupt` 会在意图落盘后、主事务之前返回 `E_REBUILD_INTERRUPTED` 并暂停约 350ms 让 HTTP 响应送达，然后强杀进程。重启后：

- pending 意图的目标版本已存在：判为 `reconciled_committed`；
- 目标版本不存在：判为 `aborted_incomplete`；
- 校验当前完整快照与全部缓存内容；缺失或漂移时在单事务内从最后完整快照版本重建；
- 恢复报告只记录从 SQLite 已落盘事实推导出的结果。

## 样本

`samples/` 由程序内置生成器产出，固定种子 `20261125`：

- `snapshot-v1.json`：3 个逻辑库、6 个逻辑表、16 个物理表；包含 mod、hash、range 和共享物理表。
- `increment-v2.json`：只重划 `orders` 区间，`base_version=1`。
- `increment-crash-v3.json`：`base_version=2`，用于中断/恢复演示。
- `snapshot-invalid-mapping.json`、`snapshot-invalid-overlap.json`、`increment-stale-base.json`：异常样本。

重新生成：

```bash
go run ./cmd/routerd samples -out samples
```

## 测试

```bash
go test ./... -count=1
go test -race ./... -count=1
go vet ./...
GOOS=windows GOARCH=amd64 go build -o .tmp/routerd.exe ./cmd/routerd
GOOS=windows GOARCH=amd64 go build -o .tmp/routerdemo.exe ./cmd/routerdemo
```

测试覆盖模型校验、摘要、路由边界与确定性、增量基础版本竞争、并发同 commit 幂等、冲突检测、整批回滚、版本陈旧读、缓存级联、传播滞后、快照持久化、意图中止恢复与缓存漂移重建，以及五类 HTTP 异常码。

更细的实现说明见 [docs/DESIGN.md](docs/DESIGN.md)。

## 目录

```text
cmd/routerd/          HTTP 服务与样本生成 CLI
cmd/routerdemo/       纯 Go 跨平台端到端演示
internal/model/       快照/增量模型、校验、摘要
internal/routing/     mod/hash/range 路由计算
internal/store/       SQLite、事务、缓存、幂等、恢复
internal/httpapi/     HTTP API 与内嵌控制台
internal/sample/      固定种子样本生成器
samples/              已生成样本
scripts/demo.sh       Bash 演示脚本
docs/                 设计与实际验收记录
```

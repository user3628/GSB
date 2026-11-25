# Prompt Prefix Cache

一个面向本地推理服务的提示词**前缀缓存** Go 组件与可运行示例。它关注的不是采样，而是同一模板在改写、系统消息增删、追加对话轮次之后，哪些前缀还能复用，哪些必须失效重建。

实现包含：

- Unicode 规范化、分块、逐块 SHA-256 指纹和显式不可缓存标记。
- 最短前缀过滤、单请求前缀上限、占用预算和有限槽位分配。
- 精确命中、归并命中、不可复用三种裁决及完整数值依据。
- 由四个阶段内容摘要构成的版本指纹和最早失效阶段传播。
- SQLite 事务化不可变版本：策略、槽位、请求状态、失效、挤出同事务提交。
- 观测版本陈旧拒绝、整体替换识别、版本回退拒绝。
- 跨进程文件锁、`BEGIN IMMEDIATE`、SQLite 条件更新与连续版本推进。
- 本地 HTTP API、CLI、固定种子演示、优雅停止和重开恢复。
- 覆盖确定性、并发、陈旧读、中断回滚、槽位冲突和历史快照的测试。

不使用 Docker、GPU、外部模型或外部网络。运行所需数据全部由内置固定种子生成器产生。

## 快速开始

要求 Go 1.22 或更高版本。本项目只使用纯 Go 的 `modernc.org/sqlite`，不依赖 CGO。

```bash
go mod tidy
go test ./...
go test -race ./...
go vet ./...
go build ./...
```

Windows 本机可执行：

```powershell
go env
go test ./...
go build -o prefixcache.exe ./cmd/prefixcache
.\prefixcache.exe demo --db .\demo.db --seed 20261125
```

Linux/macOS：

```bash
go run ./cmd/prefixcache demo --db ./demo.db --seed 20261125
```

演示会真实监听 `127.0.0.1` 的临时端口，执行健康检查和 `/v1/shutdown`，然后关闭并重新打开数据库验证恢复一致性。

## 组件结构

```text
.
├── doc.go                 包说明
├── types.go               公开数据模型
├── normalize.go           NFKC、宽度折叠、小写、空白折叠
├── planner.go             分块、偏移、逐块指纹、显式不可缓存
├── policy.go              阶段摘要、版本指纹、增量字段与最早失效阶段
├── slots.go               最短过滤、前缀上限、预算、槽位冲突裁决
├── hit.go                 精确/归并/不可复用裁决与数值证据
├── engine.go              初始化、增量、整体策略、请求导入、事务提交
├── store.go               SQLite schema、事务、条件更新、版本快照读取
├── query.go               状态、快照、命中、失效与挤出查询
├── lock_unix.go           Unix 真实文件锁
├── lock_windows.go        Windows `LockFileEx` 文件锁
├── httpapi.go             本地 HTTP JSON API
├── generator.go           固定种子确定性样本
├── demo.go                完整演示和中断恢复场景
├── cmd/prefixcache/       CLI
└── *_test.go              单元、集成、HTTP、并发、中断恢复测试
```

## 数据模型

```go
type Message struct {
    Turn     int
    Role     string
    Language string
    Text     string
}

type Request struct {
    ID         string
    TemplateID string
    Messages   []Message
}

type Policy struct {
    Version               int64
    Granularity           int     // 每个前缀块的规范化字符数
    MinPrefixLen          int     // 最短可缓存前缀字符数
    MergeThreshold        float64 // 归并相似度阈值，范围 [0,1]
    MaxPrefixesPerRequest int     // 单请求进入槽位裁决的最长前缀个数
    SlotCount             int     // 有限直接映射槽位数
    OccupancyBudget       int     // 单版本槽位占用预算
}
```

`Version` 是缓存的有效修订号。策略推进、槽位重分配和请求集导入都会产生连续的新版本。策略内容摘要是四个字段加槽位参数的内容摘要，不包含版本号；版本指纹再叠加修订号，因此同内容可以有序推进但不会混淆版本。

## 阶段算法

### 1. 规范化与分块指纹

固定顺序：

1. Unicode NFKC。
2. 兼容宽度折叠（全角/半角统一）。
3. Unicode 小写化。
4. 将连续 Unicode 空白折叠为一个 ASCII 空格并裁剪两端。

规范化后按 `Granularity` 个 rune 切块。每个块记录：

- 全局块序号；
- 来源消息下标、轮次、角色和语言；
- 源消息内字符起止偏移；
- 拼接提示词内前缀字符起止偏移；
- 规范化文本；
- SHA-256 指纹；
- `Cacheable` 与 `InvalidReason`。

块指纹使用带长度前缀的规范化编码，包含消息下标、轮次、角色、语言、块下标和块文本，不包含请求 ID，因此相同内容可跨请求复用。前缀键是累计块指纹序列的 SHA-256。

显式不可缓存规则：

- 规范化后为空的消息产生一个 0 字符块，`InvalidReason=empty_message`，不会被静默丢弃。
- 字符跨度小于 `Granularity` 的尾部块标记为 `short_block`。
- 包含空块或短块的累计前缀标记为不可缓存；其下游前缀同样不能静默跳过该块。

同一请求与策略重复生成计划会得到逐字节一致的规范 JSON 和 `PlanDigest`。

### 2. 阶段摘要与最早失效

```text
NormalizationDigest
  └─ FingerprintDigest (granularity)
       └─ SlottingDigest (min_prefix_len, max_prefixes, slot_count, occupancy_budget)
            └─ HitDigest (merge_threshold)
                 └─ VersionFingerprint (version + policy content digest + above)
```

增量片段只允许一个字段：

| 字段 | 最早失效阶段 | 下游失效 |
|---|---|---|
| `granularity` | fingerprint | 槽位、命中 |
| `min_prefix_len` | slotting | 槽位、命中 |
| `max_prefixes_per_request` | slotting | 槽位、命中 |
| `merge_threshold` | hit | 仅命中 |

最早阶段通过直接比较四段内容摘要确定，而不是只看最终布尔值。阈值变化时，槽位摘要保持不变，所有槽位标记复用；切分粒度变化时，下游槽位不会被复用。每条失效记录包含新版本、旧版本、字段、阶段、请求 ID、旧值和新值。

### 3. 槽位分配

1. 丢弃不可缓存前缀和 `PrefixChars < MinPrefixLen` 的前缀。
2. 每个请求只保留最长的 `MaxPrefixesPerRequest` 个候选，短候选记录为 `truncated_max_prefixes`。
3. 去重相同前缀键；同一前缀只能映射到一个槽位。
4. 若唯一候选数超过 `OccupancyBudget`，按 `PrefixChars` 降序、块数降序、前缀键升序保留，其余记录为 `occupancy_budget` 挤出。
5. 槽位号为 `SHA-256(prefix_key) mod SlotCount` 的直接映射。
6. 槽位冲突时按以下顺序裁决：
   - 可复用字符数更长者保留；
   - 等长且块级相似度达到阈值时保留原占用项，因为新项仍可通过归并命中复用；
   - 更早的确定性占用时间保留；
   - 前缀键字典序作最终平局裁决。

每次挤出保存双方前缀键、请求 ID、块级相似度、阈值、阈值余量、占用时间、胜者和理由。

以下情况返回 `E_SLOT_CONFLICT`，并携带请求 ID 与槽位号：

- 有候选但 `SlotCount == 0`；
- 最终占用超过预算；
- 同一前缀映射到多个槽位；
- 分配表与槽位表不一致。

### 4. 命中裁决

命中查询首先比较 `ObservedVersion` 与当前有效版本。任何不等于当前版本的观察值都会返回 `E_STALE_VERSION`，其中包含 `current_version` 与 `observed_version`，不会返回陈旧命中。

版本一致后，从最长可缓存查询前缀开始：

- 累计块指纹序列完全相等：`exact_hit`。
- 否则计算前导块相似度：

```text
common = 两个指纹序列的公共前导块数
similarity = common / max(query_blocks, cached_blocks)
margin = similarity - merge_threshold

common > 0 且 similarity >= merge_threshold  => merged_hit
```

归并结果保留 `common_prefix_blocks`、`query_blocks`、`cached_blocks`、`similarity`、`threshold` 和 `threshold_margin`，不会只返回一个布尔值。未达阈值时返回 `not_reusable`，同时保留最佳候选的数值依据。

## 版本与事务

SQLite 使用：

- WAL 日志；
- `synchronous=FULL`；
- `_txlock=immediate`；
- `busy_timeout`；
- 每版本策略行、槽位、请求状态、失效和挤出在一个事务内写入。

元数据表的条件更新是最终提交闸门：

```sql
UPDATE cache_meta
   SET current_version = ?, current_policy_digest = ?, allocation_sequence = ?
 WHERE key = 'head'
   AND current_version = ?
   AND current_policy_digest = ?
   AND allocation_sequence = ?;
```

只有影响一行才提交。写入前还会获取独立的真实文件锁（Unix `flock`，Windows `LockFileEx`），因此多个本地工作进程也只能连续推进版本。落后、重复或跨请求错位提交返回 `E_STALE_VERSION`。

不可变性保证：

- `policy_versions.version` 主键唯一；
- `(version, slot_no)` 唯一；
- `(version, prefix_key)` 唯一；
- `(version, request_id)` 唯一，保证同一版本同一请求只有一条有效状态；
- 读取旧版本使用同一只读事务读取全部关联行，不会看到半提交状态。

### 正常推进、整体替换、回退

- 回退或重复版本：提交版本小于等于当前版本，返回 `E_STALE_VERSION`，不覆盖历史。
- 正常推进：版本必须为当前版本加一；完整策略提交时 `base_digest` 等于当前内容摘要。
- 整体替换：版本为当前版本加一，但 `base_digest` 缺失或不匹配（`mode=auto`），或显式 `mode=replace`。替换会使全部槽位和命中结论失效并重建，`ReusedSlotCount=0`。
- 请求集导入产生版本 `+1`；未变化请求的相同前缀键继续复用旧槽位和占用时间。

## HTTP API

启动服务：

```bash
go run ./cmd/prefixcache serve --db ./cache.db --addr 127.0.0.1:8080
```

所有响应为 JSON：

```json
{"ok": true, "result": {}}
{"ok": false, "error": {"code": "E_STALE_VERSION", "current_version": 3, "observed_version": 2}}
```

| 方法 | 路径 | 作用 |
|---|---|---|
| `GET` | `/v1/healthz` | 健康检查 |
| `GET` | `/v1/status` | 当前版本、阶段摘要、槽位占用 |
| `GET` | `/v1/snapshot?version=` | 读取不可变版本；0 表示当前 |
| `GET` | `/v1/invalidations?version=&request_id=` | 失效台账 |
| `GET` | `/v1/evictions?version=&request_id=` | 挤出理由 |
| `GET` | `/v1/requests` | 当前请求集 |
| `GET` | `/v1/request-state?version=&request_id=` | 单请求计划、分配与命中状态 |
| `GET` | `/v1/demo-data?seed=` | 固定种子样本 |
| `POST` | `/v1/bootstrap` | 初始化版本 1 与请求集 |
| `POST` | `/v1/policy` | 完整策略推进或整体替换 |
| `POST` | `/v1/policy/patch` | 单字段增量 |
| `POST` | `/v1/requests` | 导入或整体替换请求集 |
| `POST` | `/v1/hits` | 携带观测版本的命中查询 |
| `POST` | `/v1/shutdown` | 优雅停止 |

初始化：

```bash
curl -s http://127.0.0.1:8080/v1/bootstrap \
  -H 'Content-Type: application/json' \
  -d '{"policy":{"version":1,"granularity":4,"min_prefix_len":8,"merge_threshold":0.72,"max_prefixes_per_request":4,"slot_count":31,"occupancy_budget":22},"requests":[]}'
```

增量与命中：

```bash
curl -s http://127.0.0.1:8080/v1/policy/patch \
  -H 'Content-Type: application/json' \
  -d '{"expected_version":1,"patch":{"merge_threshold":0.83}}'

curl -s http://127.0.0.1:8080/v1/hits \
  -H 'Content-Type: application/json' \
  -d '{"observed_version":2,"request_id":"req-base"}'
```

陈旧观察会返回 HTTP 409：

```json
{
  "ok": false,
  "error": {
    "code": "E_STALE_VERSION",
    "message": "hit query observation is not the current effective version",
    "current_version": 2,
    "observed_version": 1,
    "slot_no": 0
  }
}
```

停止：

```bash
go run ./cmd/prefixcache stop --url http://127.0.0.1:8080
```

## CLI

```text
prefixcache demo          # 完整固定种子演示
prefixcache serve         # 本地 HTTP API
prefixcache init          # 生成数据或 JSON 文件初始化版本 1
prefixcache status        # 当前状态与阶段摘要
prefixcache hit           # 带观察版本的命中查询
prefixcache patch         # 单字段增量
prefixcache policy        # 完整策略提交
prefixcache requests      # 请求导入
prefixcache invalidations # 失效台账
prefixcache evictions     # 挤出台账
prefixcache snapshot      # 不可变版本快照
prefixcache demo-data     # 输出样本 JSON
prefixcache stop          # 请求 HTTP 服务优雅停止
prefixcache recover       # 重开数据库并校验 head 与快照一致
```

`init` 支持外部 JSON：

```bash
prefixcache init --db ./cache.db --policy policy.json --requests requests.json
```

完整策略提交文件示例：

```json
{
  "policy": {
    "version": 2,
    "granularity": 4,
    "min_prefix_len": 12,
    "merge_threshold": 0.8,
    "max_prefixes_per_request": 4,
    "slot_count": 31,
    "occupancy_budget": 22
  },
  "mode": "advance",
  "base_digest": "<当前策略内容摘要>"
}
```

## 集成示例

```go
engine, err := prefixcache.Open(ctx, "/path/to/cache.db")
if err != nil { /* handle */ }
defer engine.Close()

commit, err := engine.Initialize(ctx, policy, requests)
if err != nil { /* handle */ }

hit, err := engine.Hit(ctx, prefixcache.HitQuery{
    ObservedVersion: commit.Version,
    RequestID:       "req-base",
})
if err != nil {
    var cacheErr *prefixcache.Error
    if errors.As(err, &cacheErr) && cacheErr.Code == prefixcache.CodeStaleVersion {
        // 重新获取状态后再查询，不能使用旧结论
    }
}

threshold := 0.83
next, err := engine.ApplyPatch(ctx, commit.Version, prefixcache.PolicyPatch{
    MergeThreshold: &threshold,
})
```

## 确定性演示与本次验证

内置种子 `20261125`，9 条请求，覆盖：

- 模板改写；
- 系统消息新增；
- 系统消息删除；
- 追加对话轮次；
- 相似但不相等的提示词；
- 完全无关提示词；
- 空消息段；
- 全角/Unicode 与空白规范化。

本次实际运行 `go run ./cmd/prefixcache demo --db .demo-out-20261125/demo.db --seed 20261125` 的关键结果：

- 基础请求：`exact_hit`，槽位 2。
- 相似但不等前缀：`merged_hit`，`similarity=0.8125`，`threshold=0.72`，`margin=0.0925`。
- 无关请求：`not_reusable`。
- 初始槽位：15；预算挤出：7。
- 观测版本落后：`E_STALE_VERSION`。
- 模拟提交中断：`E_INTERRUPTED`，版本仍为 1；重试成功推进到 2。
- 阈值增量：`hit` 阶段失效，15/15 槽位复用。
- 最短前缀增量：`slotting` 阶段失效，字段 `min_prefix_len`。
- 前缀上限增量：`slotting` 阶段失效并产生截断/预算挤出台账。
- 切分粒度增量：`fingerprint` 阶段失效。
- 内容摘要不匹配的完整策略提交：`replacement`，全部 9 条请求的槽位/命中失效。
- 版本回退：`E_STALE_VERSION`，附当前版本 6、观察版本 5。
- HTTP：`healthz=healthy`，`shutdown=stopping`。
- 关闭并重开数据库：版本 6，版本指纹一致，`fingerprint_stable=true`。

实际验证命令与结果：

```text
go test ./...          PASS
go test -race ./...    PASS
go vet ./...           PASS
GOOS=windows GOARCH=amd64 CGO_ENABLED=0 go build ./...  PASS
```

测试覆盖：

- 规范化、全半角、空白折叠、逐字节计划确定性；
- 空消息与短块显式不可缓存；
- 精确、归并、不可复用及阈值余量；
- 陈旧读拒绝；
- 增量阶段摘要变化、槽位复用与失效字段；
- 整体替换、回退、重复版本；
- 槽位冲突确定性与完整证据；
- 预算占用不超限；
- 多引擎并发提交只有一个连续版本成功；
- 两个独立工作进程同时提交同一期望版本时，仅一个成功，另一个返回 `E_STALE_VERSION`；
- 提交中途错误后事务回滚并可恢复；
- 读取方在提交暂停时看不到半提交版本；
- 历史快照在新版本提交后保持不变；
- HTTP 全流程与真实回环端口优雅停止；
- 固定种子演示两次运行 JSON 完全一致。

## 设计边界

- 槽位采用直接映射，哈希碰撞由确定的长度/相似度/占用时间规则裁决，不隐式扩容。
- `short_block` 定义为字符跨度小于切分粒度；包含此类块的累计前缀及其下游都不可缓存。
- 完整策略提交若未提供与当前版本一致的 `base_digest`，会按整体替换处理。希望正常推进时应使用 `mode=advance` 并提供当前摘要。
- 槽位历史按版本保留，便于审计和旧版本只读快照；生产环境可按保留策略另行归档。
- 本组件不调用模型，只决定提示词前缀是否可以安全复用。

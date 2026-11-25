# 租约时序裁决组件

这是一个面向上层任务调度系统的单机租约时序裁决组件。它以租约剩余寿命为核心观测对象，接受经本机 HTTP 端点到达的心跳/续约记录，处理排队、重复投递和乱序到达，并给出可复核的重建、漂移、自愈、预算与恢复结论。

项目不依赖真实集群、Docker 或外部服务。样本由固定种子生成，自愈动作通过真实的本机回环 HTTP 端点下发；持久化使用纯 Go SQLite 驱动 `modernc.org/sqlite`，无需 CGO。

## 环境

- Linux / WSL Ubuntu
- Go 1.22 或以上
- `sqlite3` 命令行工具（仅用于检查持久化结果）

本机实测环境：Go 1.27.4、sqlite3 3.46.1。

## 快速验收

```bash
go test ./... -count=1
go test -race ./... -count=1
./scripts/verify.sh
```

`scripts/verify.sh` 会执行格式检查、`go vet`、测试、确定性演示、真实回环自愈下发，以及 `SIGKILL` 中途终止后的重启恢复，并写入：

- `artifacts/verification/demo-report.json`
- `artifacts/verification/demo-sqlite-facts.txt`
- `artifacts/verification/crash/recovery-report.json`
- `artifacts/verification/crash/sqlite-facts.txt`

本次实际结果见 `docs/VERIFICATION.md`，设计细节见 `docs/DESIGN.md`。

## 目录

```text
cmd/lease-arbiter/      CLI: demo / endpoint / recover / crash-mid-batch
arbiter/                公开集成门面，外部模块只依赖此包
internal/core/          模型、校验、去重、时序重建、漂移与自愈选择
internal/sample/        固定种子样本生成器
internal/loopback/      回环 HTTP 端点与独立超时的自愈下发器
internal/orchestrator/  全局并发、有限队列与轮次预算
internal/store/         SQLite 意图、不可变版本、恢复报告
internal/engine/        单写者编排与快照/读取语义
scripts/                强制终止与完整验收脚本
docs/                   设计与实测报告
```

## 作为库集成

```go
svc, err := arbiter.Open(arbiter.Config{
    DBPath: "lease.db",
    Coordinator: arbiter.CoordinatorConfig{
        Concurrency: 8, QueueCapacity: 32, Budget: 2 * time.Second,
    },
    HealTimeout: 300 * time.Millisecond,
})
if err != nil { /* ... */ }
defer svc.Close()

_, bizErr := svc.SubmitSnapshot(ctx, snapshot)
report, bizErr := svc.Ingest(ctx, dataset, arbiter.ProcessOptions{RunHeal: true})
read, bizErr := svc.Read(ctx, report.StateVersion)
recovery, bizErr := svc.Recover(ctx)
```

未提供 `EndpointURL` 时，`arbiter.Open` 会启动一个组件自有的真实回环端点；生产集成也可传入本机 `EndpointURL`。`Open` 默认先扫描并中止遗留的 `PREPARED` 意图，结果通过 `StartupRecovery()` 提供。公开包重导出了快照、数据集、观测、策略、错误码和报告类型。

## 关键语义

安全续约点定义为：

```text
safe_point = round(lease_duration_ms * safety_ratio)
on_time    = [safe_point - tolerance_ms, safe_point + tolerance_ms]
```

重建按持有者分组，并按以下完整键排序：

1. 客户端时间戳升序
2. 服务端到达时间戳升序
3. 上报序号升序

因此重建结果只由记录自身字段决定，与批次提交顺序和 Goroutine 完成顺序无关。第一条观测作为租约基线；有效续约会把计时锚点推进到该续约的客户端时间戳。迟于容差上界的续约累计连续迟到次数；连续迟到达到阈值、迟到且间隔超过声明阈值、或租约已过期时，标记时序漂移。漂移幅度为最大超出租约容差上界的毫秒数。容差内不漂移。

自愈按持有者声明的策略选择：

- `SHORTEN_LEASE`：缩短租约，并给出新的毫秒时长
- `REQUIRE_EARLY_RENEWAL`：要求提前续约，并给出新的续约点
- `REVOKE_LEASE`：撤销租约
- `AUTO`：按漂移严重度、续约次数与租约过期状态确定性选择上述三项之一

每个自愈动作都通过真实 HTTP POST 下发。单次下发有独立超时；超时会主动关闭该次请求的底层连接并返回 `E_HEAL_TIMEOUT`。审计状态仍会随已提交版本保存。

## 一致性与版本

- `snapshot_version` 是外部观测快照的单调版本；提交不大于当前值的快照返回 `E_STALE_SNAPSHOT`。
- `state_version` 是内部不可变提交状态的单调版本；每次快照切换、批次重建或重算都会推进一格。
- 读请求携带观察到的 `state_version`；如果当前重建状态落后，返回 `E_STALE_READ`，并同时给出双方版本。
- 写入由引擎互斥串行化，并用数据库旁的进程级排他锁保证同一 SQLite 文件只有一个活跃写者；SQLite WAL 下该进程内多个读者可并发读取同一个完整不可变版本。

## 去重与乱序

- 同一 `holder_id + report_seq` 只保留一条规范记录；规范首条按服务端到达时间、客户端时间戳、摘要与类型确定性选择，因此原始批次乱序不会改变结论。
- 重复摘要相同：幂等，仅累计 `duplicate_delivery_count`。
- 重复摘要不同：返回 `E_REPORT_MISMATCH`，保留首次摘要，不提交该批。
- 到达时间乱序不影响重建序号；重算始终纳入全部已归并记录，并逐条输出重建行。

## 预算与队列

协调器全局限制同时处理的持有者数量，并维护有界等待队列。若整批无法获得准入，则不启动任何任务，返回 `E_QUEUE_FULL` 并中止已预写的意图。轮次预算耗尽时，按漂移幅度降序、持有者标识升序生成确定性延后清单；已归并观测和全部漂移结论仍写入提交状态，下一轮可直接重算。

## CLI

```bash
# 完整确定性样本：快照、重建、自愈、陈旧读取/快照、重复重放、摘要冲突
go run ./cmd/lease-arbiter demo --db artifacts/demo.db

# 独立回环端点，可注入 delay / reject / late / block
go run ./cmd/lease-arbiter endpoint --addr 127.0.0.1:18080

# 重启恢复：扫描并中止 PREPARED 意图，输出版本差异
go run ./cmd/lease-arbiter recover --db artifacts/verification/crash/crash.db

# 强制终止脚本
./scripts/kill-mid-batch.sh
```

演示默认使用固定种子 `20261126`、SQLite 路径 `artifacts/demo.db`、并发 4、等待队列 4、轮次预算 3 秒、单次自愈超时 400 毫秒。

检查 SQLite：

```bash
sqlite3 -header -column artifacts/verification/demo.db \
  "SELECT version, snapshot_version FROM committed_states ORDER BY version;
   SELECT kind, state, base_version, target_version FROM intents ORDER BY created_at_ms;"
```

## 业务码

| 业务码 | 含义 |
| --- | --- |
| `E_OBSERVATION_INVALID` | 持有者/观测字段、序号或摘要非法，整批不入库 |
| `E_REPORT_MISMATCH` | 同序号重复投递摘要冲突，首次记录保留 |
| `E_HEAL_TIMEOUT` | 单次自愈超时，连接已关闭 |
| `E_HEAL_REJECTED` | 端点拒绝或不可达 |
| `E_HEAL_FAILED` | 自愈响应或本地下发失败 |
| `E_QUEUE_FULL` | 全局并发与等待队列已满，整批未启动 |
| `E_STALE_SNAPSHOT` | 快照版本不大于当前快照版本 |
| `E_SNAPSHOT_INVALID` | 快照时间、摘要或摘要校验失败 |
| `E_STALE_READ` | 当前重建版本落后于读请求观察版本 |
| `E_STORE_FAILED` | SQLite 状态机或事务失败 |

## 明确的输入约定

- 采集顺序中的 `report_seq` 必须非递减；相等表示重复投递。真实到达乱序由 `server_arrival_ms` 与客户端时间戳体现，而不是让报表序号倒序。
- 第一条观测是基线，因此第一条重建行的 `elapsed_since_renewal_ms` 为 0。
- 心跳不推进续约锚点；有效续约才推进锚点。
- 快照/批次校验失败不会写状态；摘要冲突也不会覆盖首次记录。
- 外部 HTTP 自愈动作不能与 SQLite 组成分布式事务。因此每次下发前都会先落盘完整动作计划；若在动作发出后、状态提交前被强制终止，恢复报告会把该意图标为 `ABORTED`、保留最后完整版本并展示此事实。

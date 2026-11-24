# 票据校验引擎设计说明（CAS 风格 SSO 一次性 ST）

## 1. 目标与范围

引擎在单台开发机内、不依赖任何外部数据库或网络账号的前提下，模拟中心认证服务（CAS）
签发一次性服务票据（ST），并由校验端在并发、异常注入与强制终止重启等场景下保证：

- 一张 ST 全局只能成功兑换一次，其余并发请求拿到明确业务码；
- 每次状态迁移都在同一个 SQLite 事务内附带一个带版本号的租户快照；
- 延迟以单调时钟差值采样，按租户 × 结果码聚合，可重复演示时报告字节级一致；
- 强制终止后只恢复已提交事务，未提交事务整体丢弃且不影响票据可用性。

仅引入一个第三方依赖：纯 Go 的 SQLite 驱动 `modernc.org/sqlite`（免 CGO，可直接交叉
编译到 Windows / macOS）。

## 2. 模块结构

| 包 | 职责 |
| --- | --- |
| `cmd/casengine` | 可执行入口，分发子命令与隐藏的崩溃演示阶段 |
| `internal/codes` | 稳定业务码（`E_*`）与业务错误类型 |
| `internal/token` | ST 载荷、HMAC-SHA256 签名/验签、签名篡改 |
| `internal/clock` | 时钟抽象：真实单调时钟 + 确定性脚本时钟 |
| `internal/sample` | 固定种子派生租户/SP/用户/密钥/并发场景 |
| `internal/store` | SQLite schema、事务化状态迁移、快照链与摘要 |
| `internal/metrics` | 异步延迟采集与确定性聚合（P50/P95 nearest-rank） |
| `internal/engine` | 校验主流程、信号量、票据互斥、清理、回放、恢复 |
| `internal/server` | 仅用标准库 `net/http` 的 `/issue`、`/validate`、`/healthz` |
| `internal/report` | 稳定排序、固定采样标记的 JSON 报告 |
| `internal/cli` | 子命令装配、确定性 demo、故障注入与崩溃演示 |

## 3. 票据与签名

票据线格式为：

```
base64url(JSON(Payload)) + "." + base64url(HMAC-SHA256(tenantSecret, body))
```

`Payload` 含全局唯一票据 ID、租户、目标 SP、用户、签发与过期 Unix 秒。校验端先用未验签
载荷定位租户密钥，再做 HMAC 校验，判定顺序固定为：签名 → 受众 → 过期 → 一次性兑换。
任一前置失败都直接返回，不触碰票据状态，因此失败请求不会消耗一次性语义。

业务码映射：

| 场景 | 业务码 |
| --- | --- |
| 签名不合法/密钥不符 | `E_TICKET_SIGNATURE` |
| 目标 SP 不匹配 | `E_TICKET_AUDIENCE` |
| 已过期 | `E_TICKET_EXPIRED` |
| 已被兑换 | `E_TICKET_CONSUMED` |
| 并发等待超阈值 | `E_TICKET_CONTENTION` |
| 快照链跳号/内容不符 | `E_SNAPSHOT_VERSION` |
| 成功兑换 | `E_OK` |

## 4. 一次性兑换与并发控制

- 数据库以 `_txlock=immediate` 打开，写事务使用 `BEGIN IMMEDIATE`，写者在 SQLite 层串行。
- 兑换在**单个事务**内完成：条件更新 `tickets.status VALID→REDEEMED`、插入
  `redemptions`、追加下一版 `snapshots`，任一失败整体回滚，票据不被消耗。
- 引擎层叠加每票据 `sync.Mutex`（票据粒度互斥）与全局加权信号量（资源预算）。
- 信号量获取带等待阈值，超时返回 `E_TICKET_CONTENTION`，不会无限阻塞。
- 后台清理与校验共享同一信号量预算，单批清理最多占用 `CleanupQuota` 个槽位；清理在票据
  锁内二次确认状态与过期时间，正在校验的有效票据不会被误删。
- 所有冲突（含输掉竞争、签名/受众/过期失败、争抢超时）都写入 `conflicts`，含票据 ID、
  请求序号与业务码。

## 5. 单调时钟延迟采样

- `Clock` 抽象同时服务线上与回放：
  - `RealClock` 用 `time.Since(mark)`，读取的是 Go runtime 嵌入 `time.Time` 的单调读数，
    不受墙钟跳变影响；
  - `ScriptedClock` 用虚拟单调纳秒，`LatencyFor(seq)=80µs+(seq*73) mod 901µs`，恒定落在
    `[80µs, 980µs]` 的亚毫秒区间。
- 采样通过带缓冲通道交给独立 collector goroutine，记录延迟不阻塞、不串行化校验主路径；
  通道极端打满时退化为带锁追加，保证样本不丢。
- 聚合按 `(租户, 业务码)` 输出样本数、平均、P50、P95（nearest-rank）、最大值，全部为整数
  纳秒，输出固定排序；报告里只出现 `sample#N` 这类固定采样标记，不写墙钟时间戳。

## 6. 版本化快照与一致性

- 每个租户维护从 v1 递增的 `snapshots` 链；签发、兑换、过期清理、回退都在同一事务内追加
  新版本。
- 快照摘要是对该租户“票据集 + 兑换集”规范化 JSON 的 SHA-256，因此即使版本号连续递增，
  只要内容被整体替换，头部摘要就与实时状态不符，会被识别。
- `VerifyChain` 同时检查：版本是否连续（无跳号）、头部摘要是否等于实时状态。
  - 任一不满足 → 该租户熔断，校验直接返回 `E_SNAPSHOT_VERSION`；
  - `RollbackTenant` 截断到最近连续版本（头部内容不符时再前移一位），再基于实时状态追加
    一个新快照，恢复连续链并解除熔断。
- 票据与兑换行永不因回退而删除，审计轨迹完整保留。

## 7. 确定性与可重复演示

可重复演示需要同时固定“输入、并发裁决、时间”：

1. 输入：固定种子 `20261124` 派生全部租户/SP/用户/密钥与场景脚本；
2. 并发裁决：回放为每张竞争票据登记确定性赢家（最小请求序号），同票请求按序号串行，跨票
   按最小序号的全局顺序提交，赢家与每个快照版本都与 goroutine 调度无关；
3. 时间：脚本时钟的当前时间只随显式 `Advance` 推进，事件时间戳是 `(时间线, seq)` 的确定
   函数，延迟是 `seq` 的确定函数；
4. 输出：报告所有切片按稳定键排序，JSON 缩进固定，无墙钟字段。

因此同输入、同并发度重复运行，报告字节级一致（见 `docs/VERIFICATION.md`）。线上 `serve`
不登记赢家、不使用脚本时钟，保持真实先到先得语义与真实单调延迟。

## 8. 强制终止恢复

`crash-recovery` 用父子进程演示：

1. 子进程 A 提交“票据 1 已兑换 + 票据 2 已签发未兑换”后，在写标记文件、未正常退出前被
   `Process.Kill` 强杀；
2. 子进程 B 打开一个**显式不提交**的事务（把票据 2 改成 REDEEMED），随后被强杀；
3. 父进程重新打开数据库，借助 SQLite 的 WAL + `synchronous=FULL`：
   - 已提交的票据 1 仍为 REDEEMED，再次校验返回 `E_TICKET_CONSUMED`；
   - 未提交事务整体丢弃，票据 2 仍为 VALID，可正常兑换一次；
   - 启动链校验发现被丢弃事务导致的头部摘要不符时，自动回退并重建连续快照链。

## 9. 子命令一览

```
casengine init             生成确定性参数（租户/SP/用户/密钥/场景脚本）
casengine demo             全流程确定性演示并导出报告
casengine replay <name>    回放 happy-path | concurrent-one-time | failure-mix
casengine inject           五类异常注入断言
casengine snapshot-gap     快照跳号/内容不符 → E_SNAPSHOT_VERSION → 回退恢复
casengine status [tenant]  查看票据、兑换与快照链
casengine stats            展示最近 demo 报告的延迟聚合与结果分布
casengine serve [addr]     启动 HTTP 校验服务（真实单调时钟）
casengine crash-recovery   强制终止后恢复一致性演示
```

所有命令都接受 `-workdir DIR`（标志在位置参数前后均可），数据库与报告均落在该工作区。

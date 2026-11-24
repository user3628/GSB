# 实际验证结果

环境：Linux amd64，Go 1.27.4。SQLite 驱动为纯 Go 的 `modernc.org/sqlite`，无 CGO、
无外部数据库、无网络依赖。Windows 可执行文件通过
`GOOS=windows GOARCH=amd64 CGO_ENABLED=0 go build` 交叉编译得到 PE32+ 控制台程序。

说明：`go.mod` 的 `go` 指令为 `1.26.0`，这是 `modernc.org/sqlite` 及其传递依赖
（如 `modernc.org/libc`、`golang.org/x/sys`）在所选版本上声明的最低语言版本；因此构建
需要 Go 1.26 及以上工具链（本机 1.27.4 满足）。代码本身未使用任何 1.22 之后才出现的
语言特性，若必须在 Go 1.22–1.25 工具链上构建，可把 `modernc.org/sqlite` 降到对应年代的
版本（例如 v1.29.x），再 `go mod tidy` 让指令回落到匹配值。

## 1. 自动化测试

```
go vet ./...          # 通过
go test ./... -race -count=1
# ok  casengine/internal/clock
# ok  casengine/internal/engine   # 含 64 goroutine 真实并发一次性兑换、竞争/争抢/快照链
# ok  casengine/internal/metrics  # 确定性聚合、nearest-rank P50/P95
# ok  casengine/internal/store    # 事务原子性、版本跳号检测与回退
# ok  casengine/internal/token    # HMAC 验签、密钥不符、签名篡改
```

`TestConcurrentOneTimeRedemption` 用 64 个真实 goroutine 同时兑换同一张票据，断言：
恰好 1 个 `E_OK`、63 个 `E_TICKET_CONSUMED`、兑换表只有 1 行、票据终态 `REDEEMED`，
且每个请求延迟为正。`-race` 下无数据竞争。

## 2. 五类异常注入（`casengine inject`）

```
[PASS] signature-tamper     want=E_TICKET_SIGNATURE got=E_TICKET_SIGNATURE
[PASS] audience-mismatch    want=E_TICKET_AUDIENCE  got=E_TICKET_AUDIENCE
[PASS] expired-ticket       want=E_TICKET_EXPIRED   got=E_TICKET_EXPIRED
[PASS] concurrent-replay    want=E_OKx1             got=E_OKx1
[PASS] healthy-control      want=E_OK               got=E_OK
```

签名/受众/过期失败后票据仍为 `VALID`（失败不消耗一次性语义）；同票 4 并发仅 1 次成功；
故障租户之外的健康票据照常兑换，注入不影响其他票据。

## 3. 并发一次性兑换（`casengine replay concurrent-one-time`）

两张票据各被 4 个并发请求兑换，输出稳定为每张票据 1 个 `E_OK` + 3 个
`E_TICKET_CONSUMED`，并在 `conflicts` 表中记录票据 ID、请求序号与业务码。

## 4. 延迟采样与统计

- 线上 `serve` 用真实单调时钟，实测 `/validate` 延迟约 1–3 微秒（亚毫秒）。
- 确定性回放延迟区间 P50 最小 274000ns、全样本最大 956000ns，均 < 1ms。
- 聚合按 `(租户, 业务码)` 输出 count/avg/P50/P95/max，P95 采用 nearest-rank。
- 报告内仅有 `sample#N` 固定采样标记，无墙钟时间戳。

## 5. 快照链一致性与回退（`casengine snapshot-gap`）

```
before gap: contiguous=true headVersion=2
injected gap by deleting version 2
after gap : contiguous=true gapAt=0 fault=true
validation while faulted -> E_SNAPSHOT_VERSION (want E_SNAPSHOT_VERSION)
rollback  : recovered=true contiguous=true headVersion=2
validation after rollback -> E_OK (want E_OK)
```

删除头部快照后版本可能仍连续，但头部内容摘要与实时状态不符，仍被识别并熔断；熔断期间
校验返回 `E_SNAPSHOT_VERSION`；回退截断并追加 `ROLLBACK` 快照后链恢复、服务继续。
存储层单测另覆盖真正的版本跳号（删除中间版本产生 1,3 链，`GapAt=2`）。

## 6. 强制终止恢复（`casengine crash-recovery`）

通过父进程拉起两个子进程并 `Process.Kill` 强杀：

```
[PASS] committed ticket stays REDEEMED        status=REDEEMED
[PASS] uncommitted ticket stays VALID         status=VALID
[PASS] uncommitted ticket redeems after restart code=E_OK
[PASS] committed ticket cannot be redeemed twice code=E_TICKET_CONSUMED
[PASS] snapshot chain contiguous for tenant-acme contiguous=true headMatches=true
[PASS] snapshot chain contiguous for tenant-globex contiguous=true headMatches=true
```

已提交事务在重启后保持；显式不提交的事务被 SQLite 整体丢弃，票据仍可正常兑换一次；
未兑换票据不会被错误标记为已兑换。

## 7. 报告字节级一致性

连续多次在全新工作区执行 `casengine demo`，`demo-report.json` 的 SHA-256 完全相同：

```
c1171cb7bb1daa7458bf87f67ea32510a97aec8fa2166279cd51e6740c15f7bf
c1171cb7bb1daa7458bf87f67ea32510a97aec8fa2166279cd51e6740c15f7bf
```

报告包含票据摘要、兑换结果分布、延迟统计、冲突记录、每租户快照版本链与业务码，所有列表
按票据 ID 与请求序号稳定排序。一次 demo 的结果分布：

```
E_OK=11  E_TICKET_CONSUMED=9  E_TICKET_SIGNATURE=1
E_TICKET_AUDIENCE=1  E_TICKET_EXPIRED=1
```

快照版本链连续：tenant-acme 为 v1..v14，tenant-globex 为 v1..v12。

## 8. Windows 运行

```
# 在安装 Go 1.22+ 的 Windows 终端
go build -o casengine.exe .\cmd\casengine
casengine.exe init -workdir .\work
casengine.exe demo -workdir .\work
casengine.exe serve :8080
```

驱动为纯 Go，`CGO_ENABLED=0` 即可生成单文件 exe，无需 gcc、容器或虚拟机。所有数据库与
报告都写入 `-workdir` 指定的可写本地目录。

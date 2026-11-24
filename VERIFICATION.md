# 实际验证记录

验证时间：2026-11-25，Asia/Shanghai。

## 环境

```text
$ go version
go version go1.27.4 linux/amd64

$ sqlite3 --version
3.46.1 2024-08-13 09:16:08
```

`go.mod` 声明 `go 1.22`，纯 Go SQLite 驱动固定为 `modernc.org/sqlite v1.29.10`。

## 构建与测试

以下命令均在项目根目录实际执行：

```text
$ go mod tidy
成功，go directive 保持为 go 1.22

$ CGO_ENABLED=0 go build ./...
CGO0_BUILD_OK

$ go vet ./...
VET_OK

$ go test ./... -count=1
ok  	ledgerworkbench	0.916s
?   	ledgerworkbench/cmd/gen-samples	[no test files]
?   	ledgerworkbench/cmd/ledger-workbench	[no test files]
?   	ledgerworkbench/cmd/probe-target	[no test files]
?   	ledgerworkbench/probetarget	[no test files]
ok  	ledgerworkbench/sample	0.008s

$ go test -race ./... -count=1
ok  	ledgerworkbench	3.539s
ok  	ledgerworkbench/sample	1.046s
```

当前共有 16 个显式测试函数，覆盖归一化、守恒、迁移、竞态、陈旧读、幂等并发、探测、回滚、样本复现和强杀恢复。

## 固定样本与逐位复现

固定种子为 `20261125`，基准时间为 `1795564800000`。`TestFixedSampleGeneratorIsByteReproducible` 在两个临时目录生成样本并逐文件比较 SHA-256 与字节内容，通过。

`TestConservationHashIsBitwiseReproducible` 新建两个 SQLite 库并执行相同固定提交流程，得到相同哈希：

```text
route-balanced view_hash=1d0861bb85dab283a92e6c4a842849c364849a554f33774681f7b7acbae31d31
```

本地 HTTP 演示中固定 `as_of_ms=1795564830000` 的响应也输出同一哈希。

过渡期视图的固定结果：

```text
route_key=route-migrate
as_of_ms=1795564880000
migration_phase=transitioning
effective_weights_permille={"v1":500,"v2":500}
actual/expected ratio v1=0.500000, v2=0.500000
abs_deviation=0.000000
view_hash=c449af2591a61d9d8c5b2e7282188b35296b8c9ef1791bd06f07a0b00e4261d8
```

`GetLedger("route-migrate")` 仍返回 v1 50 条/5000 权重、v2 50 条/5000 权重，证明过渡期没有重写历史归属。

## HTTP 演示

```text
$ bash scripts/demo.sh
DEMO_EXIT=0
```

实际观察到的业务码：

- `E_LEDGER_INVALID`
- `E_WEIGHT_IMBALANCE`（脚本演示过渡期后严格校验，自动化测试也覆盖）
- `E_STALE_BASE`，上下文含 `base_version=1`、`current_version=3`
- `E_STALE_READ`，上下文含 `view_version=3`、`observed_version=999`
- `E_MIGRATION_RACE`，上下文含 `demo-race-a`、`demo-race-b` 和重叠区间
- `E_VERSION_GAP`
- `E_MIGRATION_RATIO_INVALID`
- `E_PROBE_OK`
- `E_PROBE_TIMEOUT`
- `E_PROBE_REFUSED`
- `E_PROBE_MISMATCH`
- `E_PROBE_LATE_RESPONSE`
- `E_CONFLICTING_LEDGER`（脚本演示同 ID 不同内容，并发幂等测试也覆盖）

演示的 SQLite 查询中，探测事实为：

```text
batch_id       outcome_code           timed_out  conn_closed  slot_released
demo-timeout   E_PROBE_TIMEOUT        1          1            1
demo-refused   E_PROBE_REFUSED        0          0            1
demo-mismatch  E_PROBE_MISMATCH       0          0            1
demo-late      E_PROBE_LATE_RESPONSE  0          0            1
```

## 超时连接与并发名额

`TestRealProbeBehaviorsAndTimeoutSlotRelease` 使用并发名额 1 的 Workbench 和随机端口 `httptest.Server`：

1. `mode=timeout` 在 40ms 后返回 `E_PROBE_TIMEOUT`；
2. 结果中 `timed_out=true`、`connection_closed=true`、`slot_released=true`；
3. `WaitProbeConnectionsClosed(..., 2*time.Second)` 观察到被跟踪的底层连接关闭；
4. 随后立即发起正常探测并成功，证明名额已释放。

## 强杀与恢复

自动化测试 `TestForcedKillRecoveryDependsOnlyOnSQLiteFacts` 实际构建 `cmd/ledger-workbench` 二进制：

1. 启动独立进程；
2. 调用 `/v1/admin/crash` 写入 2 条 staged entries 和 1 个未完成 migration intent；
3. 服务在落盘意图后对自身发送 `SIGKILL`；
4. 测试等待进程真实退出；
5. 用同一 SQLite 文件重启第二个进程；
6. `/v1/recovery` 返回 `discarded_staged_entries=2`、两个 unfinished intents 和 `mig-crash-e2e-crash`。

Shell 演示也实际通过：

```text
$ bash scripts/recovery-demo.sh
RECOVERY_EXIT=0
"discarded_staged_entries":3
"recovered_migration_ids":["mig-crash-demo-crash"]

state      count
discarded  3

migration_id          status
mig-crash-demo-crash  pending
```

这些计数和 ID 均来自重启后对 SQLite 的扫描，不依赖被杀进程内存。

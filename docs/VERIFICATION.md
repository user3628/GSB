# 实际验证记录

验证日期：2026-11-24（Asia/Shanghai）

## 1. 构建环境

```text
Go: go1.27.4 linux/amd64
SQLite driver: modernc.org/sqlite v1.29.10（纯 Go，无 CGO）
目标代码兼容 go 1.21
```

离线构建验证：

```bash
GOPROXY=off go build -o /tmp/meshpolicy-offline ./cmd/meshpolicy
```

结果：退出码 0，产物约 17 MB，证明依赖已完整进入本地模块缓存。

Windows 交叉编译：

```bash
GOOS=windows GOARCH=amd64 go build -o /tmp/meshpolicy-windows-amd64.exe ./cmd/meshpolicy
file /tmp/meshpolicy-windows-amd64.exe
```

实际结果：

```text
PE32+ executable for MS Windows 10.00 (console), x86-64, 16 sections
```

说明：本执行环境为 Linux，因此实际强杀测试运行的是与 PowerShell 脚本等价的
`scripts/kill-test.sh`；Windows 侧提供 `scripts/kill-test.ps1` 并已完成 Windows
目标编译。所有端点均只监听 `127.0.0.1`。

## 2. Go 测试

命令：

```bash
go test ./...
```

结果：

```text
ok  meshmigrate/internal/codec
ok  meshmigrate/internal/compat
ok  meshmigrate/internal/engine
ok  meshmigrate/internal/normalize
ok  meshmigrate/internal/verify
```

并发竞态检查：

```bash
go test -race ./internal/engine ./internal/verify
```

结果：

```text
ok  meshmigrate/internal/engine  1.759s
ok  meshmigrate/internal/verify  1.220s
```

静态检查：

```bash
go vet ./...
```

结果：无输出、退出码 0。

测试覆盖重点：

- 二进制格式小端/大端往返。
- 记录魔数、长度前缀、CRC32、全局头部摘要篡改的错误码与精确偏移。
- JSONL 头部+策略行摘要、元数据篡改、可选字段标记一致性、未知必填字段名。
- 兼容矩阵六种已声明版本与未声明版本。
- kbps -> bps、us -> ms、floor/ceil/nearest、累计误差、默认值与溢出。
- 并发同请求 ID 只执行一次；不同内容冲突。
- 重建事务暂停时读事务不跨版本。
- 恢复扫描将 pending 意图和请求标记中止，活动版本不变。

## 3. 固定样本端到端回放

命令：

```bash
bash scripts/run-e2e.sh
```

实际输出：

```text
PASS health endpoint
PASS versions endpoint
PASS fresh sample database
PASS legacy negotiation HTTP
PASS repeated negotiation is byte-identical
PASS legacy decision downgrade_accept
PASS legacy detected as binary v1
PASS legacy policy count
PASS legacy default details present
PASS legacy normalized content digest present
PASS current negotiation HTTP
PASS current decision accept
PASS current detected as JSONL v2
PASS microsecond rounding remainder 456
PASS cumulative rounding error recorded
PASS unsupported negotiation HTTP
PASS unsupported decision reject
PASS unsupported unknown required field names
PASS corrupt CRC offset
PASS corrupt length offset
PASS corrupt record magic offset
PASS legacy import committed
PASS legacy active version 1
PASS import report endpoint
PASS legacy import report committed with 4 items
PASS legacy report contains downgrade detail
PASS policies endpoint
PASS policy query returns complete version 1
PASS legacy defaults normalized
PASS current import committed
PASS active version advanced to 2
PASS cascade affected services reported
PASS import report endpoint
PASS conflict outcome legacy-report
PASS conflict outcome cart-circuit
PASS conflict outcome catalog-timeout
PASS conflict outcome checkout-rate
PASS policies endpoint
PASS active policy query bound to version 2
PASS JSON units normalized to bps/ms
PASS per-record rounding audit
PASS lower revision conflict skipped and old policy retained
PASS absent snapshot policy removed
PASS policies endpoint
PASS explicit old version remains bound and readable
PASS old version does not mix new policy values
PASS derived endpoint
PASS derived structures rebuilt for version 2
PASS derived affected service set
PASS derived endpoint
PASS old derived version remains independently readable
PASS non-monotonic snapshot version rejected
PASS versions endpoint
PASS non-monotonic rejection leaves active version 2
PASS unsupported import rejected with unknown field name
PASS versions endpoint
PASS unsupported import leaves active version 2
PASS duplicate batch rejected atomically
PASS import report endpoint
PASS reject report lists per-policy outcomes
PASS versions endpoint
PASS rejected batch leaves active version 2
PASS versions endpoint
PASS rejected batches did not persist snapshot versions 3/4
PASS same request id reuses exact stored response
PASS same request id different content is conflicting
PASS unknown/retired version returns E_VERSION_RETIRED
{
  "checks": 67,
  "failures": null,
  "status": "pass"
}
```

关键可核对值：

- 旧二进制 `checkout-rate` 缺字段后补齐：`rateLimitBps=1000000`、`circuitWindowMs=30000`、`rateWindowMs=60000`、`windowAlignment=up`。
- JSONL `checkout-rate` 的 `250 kbps` 归一化为 `250000 bps`。
- `capturedAtUs=1760000100123456` 按 floor 得到 `1760000100123 ms`，`remainderUs=456`，`roundingBit=1`，`errorUs=-456`。
- v2 冲突报告：`checkout-rate=overwrite`、`catalog-timeout=overwrite`、`legacy-report=skip`、`cart-circuit=overwrite`。
- v2 活动快照保留 `legacy-report` 的旧 revision 3，移除未出现在输入中的 `remove-me`。
- v2 派生结构：4 条路由、4 个限流窗口、4 个熔断状态、4 个服务汇总，全部版本为 2。
- v3 未知必填字段拒绝：`E_FORMAT_UNSUPPORTED`，`unknownFields=["geoAffinity"]`。
- v4 批内重复策略拒绝：`E_BATCH_REJECTED`，活动版本仍为 2，数据库中没有版本 3/4 快照行。

## 4. 强制终止恢复验收

命令：

```bash
bash scripts/kill-test.sh
```

流程：

1. 导入固定二进制快照为版本 1。
2. 请求导入版本 2，并在“策略/派生行已写入、活动指针尚未切换”的事务内暂停。
3. 等待测试标记后，用 `kill -9` 强制终止服务进程。
4. 用同一 SQLite 文件重启服务，启动扫描自动生成恢复报告。
5. 查询版本、恢复报告、派生结构和原请求 ID 重放。

实际结果：

```text
mid-rebuild reached; kill -9 server pid 422297
PASS forced-termination recovery
activeVersion=1, versions=1, abortedBatch=1, derivedVersion=1, replay=E_IMPORT_ABORTED
```

恢复报告关键字段：

```json
{
  "activeSnapshotVersion": 1,
  "factsSource": "sqlite_committed_rows_only",
  "pendingIntentCount": 1,
  "activeSnapshotIntegrity": {
    "complete": true,
    "snapshotRowPresent": true,
    "expectedPolicyCount": 4,
    "storedPolicyCount": 4,
    "expectedRouteCount": 4,
    "storedRouteCount": 4,
    "expectedRateWindowCount": 4,
    "storedRateWindowCount": 4,
    "expectedCircuitCount": 4,
    "storedCircuitCount": 4,
    "expectedSummaryCount": 4,
    "storedSummaryCount": 4
  },
  "abortedBatches": [
    {
      "requestId": "kill-v2",
      "snapshotVersion": 2,
      "action": "mark_aborted",
      "reason": "pending intent has no committed snapshot row and is not the active snapshot version"
    }
  ],
  "pendingIntentsObserved": [
    {
      "snapshotRowPresent": false,
      "snapshotVersionIsActive": false
    }
  ]
}
```

重放原请求返回：

```json
{"error":{"code":"E_IMPORT_ABORTED","message":"import was interrupted before commit; recovery marked the batch aborted","reportId":"recovery-..."}}
```

这证明未提交事务被 SQLite 回滚、活动版本仍为最后一个完整版本，且恢复报告只引用已落盘的 SQLite 行。

## 5. 固定样本校验和

`samples/checksums.json` 由生成器写入，包含每个样本文件的 SHA-256。重新生成：

```bash
bin/meshpolicy samples -out samples
```

生成器不读取当前时间或系统状态，输出固定。

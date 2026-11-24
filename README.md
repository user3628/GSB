# TokenForge：多租户令牌刷新、吊销与定点配额组件

这是一个从零实现的 Go 领域组件和可直接运行的确定性示例，覆盖：

- HMAC-SHA256 访问令牌/刷新令牌签发与轮换
- 单令牌吊销、整族吊销、轮换后重放检测
- scale=1e9 的 int64 定点配额扣减、溢出保护和截断误差账本
- 小租户逐族精算、大租户批量流式扫描的自适应族谱查询
- SQLite 单事务状态变更、递增版本快照、内容哈希/前序哈希链与回放
- 同令牌并发竞争、同租户串行化、在途事务信号量及竞争超时
- 强制终止恢复测试和五类外部故障注入
- 字节级可重复的 JSON 验收报告

所有测试与示例均在本地生成密钥、租户、用户、令牌和配额脚本，不需要 Docker、外部账号或网络服务。

## Windows 一键运行

前置条件：安装 Go 1.22 或更高版本，确认 `go version` 可用。驱动为纯 Go 的 `modernc.org/sqlite`，不需要 CGO。

```bat
go mod download
go mod tidy
run-demo.cmd
```

也可以显式指定参数：

```bat
go run ./cmd/tokenforge-demo ^
  -workdir .\tmp ^
  -out .\tokenforge-report.json ^
  -threshold 4 ^
  -inject all
```

或从 PowerShell 运行：

```powershell
go run ./cmd/tokenforge-demo -workdir .\tmp -out .\tokenforge-report.json -threshold 4 -inject all
```

运行结束时 stdout 会输出报告 SHA-256。重复运行且参数相同时，报告必须逐字节一致：

```powershell
go run ./cmd/tokenforge-demo -workdir .\tmp-a -out .\report-a.json
go run ./cmd/tokenforge-demo -workdir .\tmp-b -out .\report-b.json
Get-FileHash .\report-a.json, .\report-b.json -Algorithm SHA256
```

`-inject` 可用值为 `all`、`none` 或以下逗号列表：

| 参数 | 注入行为 | 期望业务码 |
|---|---|---|
| `signature` | 篡改刷新令牌签名 | `E_TOKEN_INVALID` |
| `replay` | 轮换后再次提交旧刷新令牌 | `E_TOKEN_REUSE`，并整族吊销 |
| `quota_overflow` | 乘加溢出前拒绝扣减 | `E_QUOTA_OVERFLOW`，账本不变 |
| `snapshot_gap` | 下一快照版本跳一号 | `E_SNAPSHOT_VERSION`，回退最近连续版本 |
| `contention` | 在途上限 1，等待阈值 40ms，并发占用事务 | `E_QUOTA_CONTENTION` |

## Go 包接口

核心包导入路径为 `example.com/tokenforge`。最小接入示例：

```go
func integrate(ctx context.Context, localHMACKey []byte) error {
svc, err := tokenforge.NewService(ctx, tokenforge.Config{
    DatabasePath:  `C:\data\tokenforge.db`,
    SigningKey:    localHMACKey, // 由宿主密钥系统注入，长度至少 16 字节
    Clock:         time.Now,
    MaxInFlight:   16,
    AdmissionWait: 2 * time.Second,
})
if err != nil { return err }
defer svc.Close()

if err := svc.CreateTenant(ctx, tokenforge.TenantSpec{
    TenantID:          "tenant-a",
    DefaultUserID:     "user-a",
    InitialQuotaFixed: 100 * tokenforge.Scale,
    UnitCostFixed:     333_333_333,
    UsageFixed:        1_500_000_000,
}); err != nil { return err }

issued, err := svc.Issue(ctx, tokenforge.IssueRequest{TenantID: "tenant-a"})
if err != nil { return err }

rotated, err := svc.Refresh(ctx, tokenforge.RefreshRequest{
    RefreshToken: issued.Pair.Refresh.Value,
})
if err != nil {
    // 使用 tokenforge.CodeOf(err) 获取稳定业务码。
    return err
}

_, _ = svc.Revoke(ctx, tokenforge.RevokeRequest{
    Token: rotated.Pair.Access.Value,
    Scope: tokenforge.RevokeFamily,
})

if _, err := svc.ReplaySnapshots(ctx, "tenant-a"); err != nil {
    return err
}
if _, err := svc.ExportTenantReport(ctx, "tenant-a", tokenforge.FamilyQueryOptions{Threshold: 8}); err != nil {
    return err
}
return nil
}
```

主要导出接口：

- `NewService` / `Close`
- `CreateTenant` / `GetTenant`
- `Issue`
- `Refresh`
- `Revoke`
- `ValidateAccess`
- `AuditFamilies`
- `ReplaySnapshots` / `VerifySnapshotChain`
- `ExportTenantReport` / `WriteDeterministicJSON` / `WriteJSONFile`
- `DefaultSampleSpec` / `NewSequentialIDSource`
- `Metrics` / `InjectNextSnapshotGap`
- `CompareSnapshots` / `SummarizeSnapshotChain`

所有业务失败都通过 `*DomainError` 携带稳定 `Code`；宿主可直接使用 `tokenforge.CodeOf(err)`，无需解析错误文本。

## 核心设计

### 令牌与刷新轮换

令牌格式为 `tf1.<base64url(payload)>.<base64url(HMAC-SHA256)>`。载荷绑定：

- `tenant_id`
- `user_id`
- `family_id`
- `sequence`
- `kind`（access/refresh）
- `issued_at` / `expires_at`（Unix 纳秒）
- `jti`

刷新顺序为：签名校验 → 过期校验 → 族状态/序号校验 → 旧令牌状态校验 → 配额检查 → 轮换 → 快照 → 提交。因此：

- 签名不符：`E_TOKEN_INVALID`
- 过期：`E_TOKEN_EXPIRED`
- 序号超前：`E_TOKEN_SEQUENCE`
- 旧刷新令牌非并发重放：`E_TOKEN_REUSE`，在同一事务中整族吊销并提交快照
- 单令牌已吊销：`E_TOKEN_REVOKED`
- 同一刷新令牌并发落败者：`E_TOKEN_CONSUMED`
- 新访问令牌可用、旧访问令牌在轮换后为 `consumed`

数据库只保存令牌摘要，不保存可直接使用的原文。原文只在签发/轮换结果中返回一次。

### SQLite 事务与状态

每个租户的写入由 `tenantGate` 串行化；SQLite 单写者由进程内 `sqliteGate` 在 `BEGIN` 前串行化，避免不同租户并发时的事务升级 `SQLITE_BUSY`；可配置的全局在途信号量负责资源上限。刷新事务同时更新：

- 旧令牌状态和 `consumed_at`
- 新访问/刷新令牌
- 令牌族 `current_sequence`
- 租户余额、累计消费、误差聚合
- 本次扣减/误差账本
- 新的递增版本快照和当前快照指针

任一 SQL 操作或 `BeforeCommit` 钩子失败都会回滚整个事务，令牌、族谱、配额、账本和快照指针一起恢复。进程在提交前被强杀时，SQLite 会丢弃未提交事务。

### 定点配额与误差

金额/配额统一为 `int64`，比例固定为 `Scale = 1_000_000_000`。单次刷新成本：

```text
product   = unit_cost_fixed * usage_fixed       // CheckedMul
charged   = floor(product / Scale)
remainder = product % Scale
```

任何乘法、余额扣减、累计消费、误差聚合的溢出都会在写账本前返回 `E_QUOTA_OVERFLOW`。每次刷新都会写一条账本记录，包括 `remainder_units`。聚合误差上界为：

```text
ceil(sum(remainder_units) / Scale)
```

实现使用整数加法和向上取整，不使用 `float32`/`float64`。

### 双路径族谱查询

`FamilyQueryOptions{Threshold: n, ForcePath: ...}` 支持自动和强制路径：

- `families < threshold`：`precise_per_family`，逐族查询成员与状态
- `families >= threshold`：`batch_stream`，确保族谱索引存在，再按租户批量流式扫描

路径、阈值、索引名和成员摘要都会进入结果。示例在隔离数据库中分别强制 `precise` 与 `stream` 执行完全相同的签发、轮换、单令牌吊销和重放流程，然后比较：

- 族成员 JSON / `MembersDigest`
- 完整业务码序列
- 每次配额扣减和余数
- 快照版本链

比较结果写入报告中的 `path_parity`，`conclusion_bytes_identical` 必须为 `true`。

### 版本快照

每个成功写事务都持有：

- `tenant_state.version`
- `tenant_state.content_hash`
- 快照自身 `version`
- `prev_hash`
- `content_hash`
- 完整 JSON 状态：租户配额、所有族、所有令牌摘要、扣减/误差账本

加载时逐版本重算内容哈希并验证前序哈希链。若版本从 1、2、3 跳到 5：

- 返回 `E_SNAPSHOT_VERSION`
- `Snapshots` 只包含连续的 1、2、3
- `LatestContiguousVersion = 3`
- 调用方可用最近连续快照状态回放，不采用跳号后的状态

`CompareSnapshots` 比较内容而不只是版本号，会区分：

- `content_changed`
- `version_only_change`
- `whole_replacement`：同版本正文被整体替换，或不同版本之间没有任何共享族/令牌身份

### 并发与资源控制

- 同一刷新令牌的并发请求只有一个成功，其余在重叠请求窗口内返回 `E_TOKEN_CONSUMED`
- 真正的后续重放返回 `E_TOKEN_REUSE` 并整族吊销
- 同一租户的配额扣减通过租户门串行化
- 全局在途写事务受 `MaxInFlight` 信号量约束
- 等待超过 `AdmissionWait` 返回 `E_QUOTA_CONTENTION`，不会无限阻塞
- `Service.Metrics()` 和单次结果提供在途数、排队纳秒数、峰值和竞争计数

为了保持报告字节稳定，示例 JSON 只写入稳定的布尔观测值（是否竞争、是否观察到排队、峰值是否未超限）；真实运行时的排队纳秒数通过 Go API 可观测，但不写入验收报告。

## 确定性报告内容

`tokenforge-report.json` 包含：

- 内置样本规格：租户、用户、密钥摘要、固定时钟、配额脚本
- 主流程业务码序列与计数
- 每个租户的令牌摘要、族谱轨迹、扣减/误差账本、配额和快照链
- 双路径一致性与规范化结论 SHA-256
- 同令牌并发竞争结果
- 五类故障注入结果
- 快照跳号检测、连续前缀和回退版本
- 提交前异常导致回滚后可继续刷新的结果

报告不包含临时目录、数据库路径、当前墙钟时间、随机 ID 或原始令牌，因此同参数的重复运行应得到相同 SHA-256。

## 验证

常用命令：

```bat
go test ./... -count=1
go test -race ./... -count=1
go test ./... -run TestCrashRecovery -v
go vet ./...
set CGO_ENABLED=0
set GOOS=windows
set GOARCH=amd64
go build ./...
```

其中 `TestCrashRecovery` 会启动真实子进程，在刷新事务写入令牌、配额、账本和快照之后、`COMMIT` 之前写入标记文件；父进程确认标记后执行 `Process.Kill()`。重启后验证：

- 已提交的签发快照仍为版本 1
- 未提交刷新整体丢弃
- 原刷新令牌仍可成功刷新
- 人为制造恢复后的版本跳号时返回 `E_SNAPSHOT_VERSION` 并回退到版本 2

本轮实际执行结果（2026-11-24，当前 WSL/Linux 环境；同时完成 Windows 交叉编译）：

- `go version`：go1.27.4 linux/amd64；`go.mod` 指令为 `go 1.22`
- `go test ./... -count=1`：通过
- `go test -race ./... -count=1`：通过
- `go vet ./...`：通过
- `GOOS=windows GOARCH=amd64 CGO_ENABLED=0 go build ./...`：通过
- 源码金额实现无 `float32`、`float64`、`ParseFloat`、`math.Pow`（扫描仅命中 `fixed.go` 中说明“不使用浮点”的注释）
- 示例报告连续两次运行逐字节相同，SHA-256 为 `063e64e599b65b7cb75e6730b76fb6b9dc6fc3c49ad82180ec4cc18a552bf2d5`
- 双路径：`business_codes_equal=true`、`quota_deductions_equal=true`、`family_conclusions_equal=true`、`conclusion_bytes_identical=true`
- 五类注入码分别为 `E_TOKEN_INVALID`、`E_TOKEN_REUSE`、`E_QUOTA_OVERFLOW`、`E_SNAPSHOT_VERSION`、`E_QUOTA_CONTENTION`

由于当前执行环境不是原生 Windows 桌面，Windows 部分验证为“纯 Go 交叉编译通过 + 无平台相关系统调用 + 提供 `run-demo.cmd`”。原生 Windows 上按本文命令执行即可完成最终本地样本验收。

## 目录

```text
.
├── cmd/tokenforge-demo/       一键示例 CLI
├── internal/demo/             确定性场景与报告生成
├── access.go                  访问令牌校验
├── refresh.go                 刷新轮换/重放检测
├── revoke.go                  单令牌/整族吊销
├── quota.go                   定点扣减与误差账本
├── family.go                  双路径族谱查询
├── snapshot.go                版本快照、链校验、内容比较
├── store.go                   SQLite 打开、schema、服务资源
├── token.go                   Claims、HMAC 签名、ID 源
├── fixed.go                   scale=1e9 定点运算
├── sample.go                  内置确定性样本
├── export.go                  报告/租户导出
├── tokenforge_test.go         领域与恢复测试
└── run-demo.cmd               Windows 一键入口
```

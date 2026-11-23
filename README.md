# 多租户 SaaS 开放平台：OAuth 2.0 + 定点配额账本

这是一个可本地独立运行的 Go 参考实现，覆盖：

- OAuth 2.0 授权码 + PKCE S256 兑换。
- 一次性授权码 nonce、client/redirect 绑定、校验失败不消费授权码。
- 刷新令牌轮换、家族谱系、重放检测与整族撤销。
- `int64` 定点金额（scale=`1e9`）计费、截断余数与误差上界。
- 令牌、配额、账本、幂等结果和会话快照在同一 SQLite 事务中提交。
- 租户串行化、全局在途事务上限、排队超阈值稳定返回 `E_QUOTA_CONTENTION`。
- 连续版本快照、跳号检测、最近一致版本只读恢复。
- 六类参数化故障注入，以及真实子进程强制终止后的 SQLite 恢复验证。
- 可重复的确定性样本、字节级一致导出和 Windows PowerShell 操作方式。

运行时不访问外部 OAuth 服务、真实账号或网络；只使用标准库和
`modernc.org/sqlite` 纯 Go 驱动，不需要 CGO。

## 目录

| 路径 | 内容 |
| --- | --- |
| `authquota/` | 可导入的领域包：引擎、定点计算、快照、错误码、HTTP 中间件 |
| `cmd/openplatform/` | 本地 CLI 与强杀恢复集成测试 |
| `internal/sample/` | 确定性样本加载器 |
| `testdata/sample.json` | 固定租户、客户端、PKCE、nonce、计费脚本 |
| `docs/DESIGN.md` | 事务顺序、快照恢复、并发与数据模型说明 |
| `docs/VALIDATION.md` | 本次实际执行命令和结果 |

## Windows 环境准备

要求 Go 1.22 或以上。以下 PowerShell 命令把 GOPATH 与模块缓存放在工作区，
避免污染全局目录：

```powershell
# 任选其一：
# winget install GoLang.Go
# 或解压 go1.24.x.windows-amd64.zip 后把 go\bin 加入 PATH。

$env:GOPATH = "$PWD\.gopath"
$env:GOMODCACHE = "$PWD\.gomodcache"
go version
go env GOPATH GOMODCACHE

# 只在首次准备时下载依赖；运行与测试不访问网络。
go mod download

# 编译与全量测试
go test ./... -count=1
go build -o .\openplatform.exe .\cmd\openplatform
```

`go.mod` 声明 `go 1.22.0`，唯一直接依赖是 `modernc.org/sqlite v1.34.5`。

## 快速验收

### 1. 确定性端到端 demo

```powershell
.\openplatform.exe demo --db .\demo-a.db --reset
.\openplatform.exe demo --db .\demo-b.db --reset
Get-FileHash .\demo-a-output.json -Algorithm SHA256
Get-FileHash .\demo-b-output.json -Algorithm SHA256
```

如需保存输出：

```powershell
.\openplatform.exe demo --db .\demo-a.db --reset | Set-Content -NoNewline .\demo-a-output.json
.\openplatform.exe demo --db .\demo-b.db --reset | Set-Content -NoNewline .\demo-b-output.json
Compare-Object (Get-Content .\demo-a-output.json) (Get-Content .\demo-b-output.json)
```

两个输出应字节级一致。输出同时包含租户账本、误差聚合、令牌状态和健康快照。

### 2. 六类故障

```powershell
$faults = @(
  "auth_code_expired",
  "pkce_mismatch",
  "refresh_token_replay",
  "precision_overflow",
  "snapshot_version_jump",
  "quota_exhausted"
)
foreach ($fault in $faults) {
  .\openplatform.exe fault-demo --db ".\fault-$fault.db" --reset --fault $fault |
    Set-Content -NoNewline ".\fault-$fault.json"
}
```

`fault-demo` 会输出稳定业务码、终态账本版本、快照版本和配额余量。详细断言见
`authquota/integration_test.go` 与 `docs/VALIDATION.md`。

也可以一次执行完整 PowerShell 验收：

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\validate.ps1
```

### 3. 分步 CLI 工作流

```powershell
$env:OAUTH_NOW = "2026-01-02T03:04:05Z"
$env:OAUTH_TOKEN_SECRET = "local-deterministic-openplatform-sample-secret-v1"

.\openplatform.exe seed --db .\workflow.db --sample .\testdata\sample.json
$issued = .\openplatform.exe issue --db .\workflow.db --nonce nonce-cli-0001 |
  ConvertFrom-Json
$tokens = .\openplatform.exe exchange --db .\workflow.db `
  --code $issued.data.code `
  --verifier dBjftJeZ4CVP-mB92K27uhbUJU1p1r_wW1gFWFOEjXk `
  --idempotency-key exchange-cli-0001 | ConvertFrom-Json
$rotated = .\openplatform.exe refresh --db .\workflow.db `
  --token $tokens.data.refresh_token `
  --idempotency-key refresh-cli-0001 | ConvertFrom-Json

.\openplatform.exe status --db .\workflow.db --tenant tenant-acme
.\openplatform.exe export --db .\workflow.db --all
.\openplatform.exe errors --db .\workflow.db --tenant tenant-acme
```

所有命令输出 JSON envelope：成功为 `{"ok":true,"data":...}`，失败为
`{"ok":false,"error":{"code":"...","message":"..."}}`。`status` 和
`fault-demo` 在只读恢复场景会同时输出 `data` 与错误对象。

## CLI 与环境变量

| 环境变量 | 默认值 | 作用 |
| --- | --- | --- |
| `OAUTH_DB` | `openplatform.db` | SQLite 文件路径 |
| `OAUTH_RECOVERY_STRATEGY` | `strict` | `strict` 拒绝权威读取；`consistent` 从最近一致快照恢复只读导出 |
| `OAUTH_MAX_IN_FLIGHT` | `4` | 全局在途计费/令牌事务上限 |
| `OAUTH_MAX_QUEUE` | `16` | 单租户排队请求上限 |
| `OAUTH_QUEUE_TIMEOUT_MS` | `250` | 全局/租户等待阈值，超时返回 `E_QUOTA_CONTENTION` |
| `FAULT_INJECTION` | `none` | 六类故障注入模式 |
| `OAUTH_NOW` | 空（真实时钟） | 固定 RFC3339 时间，便于可重复运行 |
| `OAUTH_TOKEN_SECRET` | 内置样本值 | 本地确定性令牌 HMAC 密钥 |
| `OAUTH_SAMPLE` | `testdata/sample.json` | 样本路径 |
| `OAUTH_CRASH_POINT` | 空 | `mid_transaction` 或 `after_commit`，仅供强杀恢复测试 |
| `OAUTH_CRASH_MARKER` | 空 | 到达崩溃钩子时写出的标记文件 |

各命令也接受对应的 `--db`、`--recovery`、`--fault`、`--max-in-flight`、
`--max-queue`、`--queue-timeout-ms`、`--now` 等标志。

## Go 组件接入

```go
engine, err := authquota.Open(ctx, authquota.Config{
    DBPath:           "openplatform.db",
    RecoveryStrategy: authquota.RecoveryStrict,
    MaxInFlight:      4,
    MaxQueue:         16,
    QueueTimeout:     250 * time.Millisecond,
    TokenSecret:      []byte(os.Getenv("OAUTH_TOKEN_SECRET")),
})
if err != nil { return err }
defer engine.Close()

if err := engine.Seed(ctx, tenantSeed); err != nil { return err }

// 宿主 HTTP 服务可直接接入 Bearer 鉴权中间件。
http.Handle("/api/", engine.Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
    principal, _ := authquota.PrincipalFromContext(r.Context())
    _ = principal.TenantID
    w.WriteHeader(http.StatusNoContent)
})))
```

领域 API：

- `Open` / `Close` / `Seed`
- `IssueAuthorizationCode`
- `ExchangeAuthorizationCode`
- `Refresh`
- `Revoke`
- `Authenticate` / `Middleware`
- `Status`
- `Ledger` / `ExportAll`
- `Errors`

样本中的 HMAC 密钥与固定时钟仅用于可重复性。生产接入应使用安全随机密钥和真实时钟；
授权码/访问令牌/刷新令牌查询列只保存 SHA-256 哈希，幂等响应使用由 secret 派生的
AES-GCM 确定性封装保存，便于安全回放而不明文落盘。重启后的幂等回放必须使用同一个
`OAUTH_TOKEN_SECRET`。

## 定点计费

所有金额都是 `int64` 纳单位（`Scale=1_000_000_000`）。代码中不存在
`float32`、`float64`、`ParseFloat` 或 `FormatFloat` 金额路径。

单次刷新扣减公式：

```text
product = requests × unit_cost_nano
exact   = product × rate_numerator
charged = floor(exact / rate_denominator)
residual = exact mod rate_denominator   // 以 1/rate_denominator 纳单位表示
```

每一步 `int64` 乘积都做溢出检查；溢出返回 `E_PRECISION_OVERFLOW`，不改变令牌、配额或账本。
`rate_numerator/rate_denominator` 支持分数定价，默认 `1/1`。`residual=0` 时无截断。

每个扣减的截断余数同时写入 `billing_entries` 和独立的 `truncation_entries` 误差账本。
租户聚合报告提供：

- `truncated_entries`：发生截断的次数。
- `upper_bound_nano = truncated_entries`：每次向下取整损失严格小于 1 纳单位。
- `exact_residual_nano_rational`：所有余数的精确有理数和，例如 `4/3`。

因此累计误差有确定上界，不会因浮点或多次舍入扩散。

## 快照、恢复与并发

- 版本 1 由 `Seed` 创建；令牌签发、轮换、整族撤销、显式撤销、配额触底都会追加下一版本。
- 快照保存规范化会话 JSON 与 SHA-256；读取时同时校验版本连续性和哈希。
- 跳号时 `Status` 返回 `E_SNAPSHOT_VERSION`，同时返回最近连续版本、`read_only=true` 的视图。
- `strict` 策略下权威账本导出拒绝服务；`consistent` 策略下只导出
  `billing_entry.snapshot_version <= 最近一致版本` 的数据，避免使用跳号后的不可信状态。
- 同租户请求由内存 lane 串行化；全局 `MaxInFlight` 信号量与 `MaxQueue`/超时共同限制排队。
- 所有事务使用 SQLite WAL、`synchronous=FULL`、`_txlock=immediate` 和 busy timeout。
- 竞争冲突在 `error_events` 中记录租户、请求 ID、账本版本和快照版本。

事务与崩溃点的详细顺序见 `docs/DESIGN.md`。

## 业务码

| 业务码 | 场景 |
| --- | --- |
| `E_INVALID_GRANT` | 授权码未知/过期/已用、client 或 redirect 不匹配、刷新令牌无效 |
| `E_PKCE_MISMATCH` | S256 verifier 与 challenge 不匹配 |
| `E_PRECISION_OVERFLOW` | 定点乘法或聚合金额溢出 |
| `E_TOKEN_REUSE` | 已轮换/已撤销刷新令牌被再次提交，整族撤销 |
| `E_SNAPSHOT_VERSION` | 快照版本跳号或哈希/结构不可信 |
| `E_QUOTA_CONTENTION` | 排队超阈值或全局在途已满 |
| `E_QUOTA_EXHAUSTED` | 余额不足，刷新不扣费、不轮换 |
| `E_IDEMPOTENCY_CONFLICT` | 同一幂等键对应不同请求载荷 |
| `E_INVALID_REQUEST` | 参数或 fixed-point 输入非法 |
| `E_NOT_FOUND` | 租户或可撤销令牌不存在 |
| `E_INTERNAL` | 未分类内部错误 |

## 确定性来源

- 固定时钟来自 `testdata/sample.json` 的 `now`。
- 授权码、访问令牌、刷新令牌和 family ID 均由固定 secret 的 HMAC-SHA256 派生，不使用随机源。
- 所有快照/账本查询显式排序；JSON 使用结构体字段顺序和有序切片。
- SQLite 自增与事务提交顺序在相同输入下稳定；两个全新数据库的 demo 输出已通过 SHA-256 对比。

## 本次实际验证

环境：Go `1.24.6`，Linux/amd64（WSL2）；模块声明 `go 1.22.0`。

```text
go test ./... -count=1                         PASS
go test ./... -count=5                         PASS
CGO_ENABLED=0 go test ./... -count=1           PASS
go test -race ./authquota ./cmd/openplatform   PASS
go vet ./...                                   PASS
GOOS=windows GOARCH=amd64 go build ./...       PASS
GOOS=windows GOARCH=amd64 go test -c ./authquota  PASS
GOOS=windows GOARCH=amd64 go test -c ./cmd/openplatform  PASS
demo 两次输出 SHA-256
  cd3c63256b0263a9c31ce7e4763a745e641eb3365bcaada018314d772128b96b  PASS（byte-identical）
```

CLI 子进程强杀测试验证：`after_commit` 的轮换、扣减、快照与幂等记录恢复；
`mid_transaction` 的新令牌、账本、配额和幂等记录全部丢弃。完整记录见
`docs/VALIDATION.md`。

## 边界说明

本项目是本地确定性验收实现，不是完整生产 OAuth 部署：没有外部 IdP、客户端认证密钥、
网络 TLS、分布式限流或多进程共享租户锁。SQLite 单写者约束由 `BEGIN IMMEDIATE` 与
busy timeout 处理；跨进程部署时应将租户闸门与账本迁移到支持分布式事务/行锁的后端。

# authlab：多租户认证与会话隔离验证引擎

`authlab` 是一个可直接运行的 Go 1.22+ 本地实验工程。它从零提供登录、HMAC 令牌签发/校验、SQLite 会话快照、租户隔离、刷新令牌轮换、权限边界校验、HTTP 中间件，以及五类可参数化、可复现的异常注入。

工程不依赖外部身份提供商、Docker、CGO 或网络服务。SQLite 使用纯 Go 驱动 `modernc.org/sqlite`。认证样本、签名密钥、ID 序列和时钟全部由程序自身确定性生成。

## 环境准备

```bash
go version                 # go1.22 或更高
go env -w GOPATH="$HOME/go"
go env -w GOPROXY="https://proxy.golang.org,direct"   # 网络受限时可改用可用模块代理
go mod download
```

Windows 同样只需可写工作目录和 Go 工具链；SQLite 驱动为纯 Go，无需 CGO、管理员权限或数据库服务。

## 快速开始

```bash
go version                 # 需要 Go 1.22 或更高版本
go test ./...
go run ./cmd/authlab experiment --out artifacts/run
```

也可以使用一键脚本：

```bash
make experiment            # 构建、执行全部注入、写 artifacts/run/report.json
make verify                # 连续运行两次并逐字节比较报告和状态导出
./scripts/run-experiment.sh --injection refresh-race
```

Windows PowerShell：

```powershell
.\scripts\run-experiment.ps1
.\scripts\verify-determinism.ps1
```

首次构建需要模块代理可访问以取得依赖；运行实验本身不访问网络。CGO 可关闭：

```bash
CGO_ENABLED=0 go build -trimpath -o bin/authlab ./cmd/authlab
```

## 交付内容

- `cmd/authlab`：实验、HTTP 服务、样本导出、状态导出命令。
- 根包 `example.com/authlab`：认证引擎、令牌编解码、快照存储接口、SQLite 实现、HTTP 中间件。
- `sample`：固定租户、用户、口令哈希、盐、本地签名密钥和 ID 种子。
- `experiment`：五类异常注入、独立性断言、快照审计和字节稳定报告。
- `scripts` / `Makefile`：Linux/macOS 与 Windows 一键实验和确定性校验。
- `artifacts/run`：最近一次实际验证报告、报告 SHA-256 和租户状态导出。

## 命令

### 异常实验

```bash
go run ./cmd/authlab experiment --out artifacts/run
go run ./cmd/authlab experiment --injection expired --out /tmp/authlab-expired
go run ./cmd/authlab experiment --injection cross-tenant --out /tmp/authlab-cross
go run ./cmd/authlab experiment --injection session-fixation --out /tmp/authlab-fixation
go run ./cmd/authlab experiment --injection privilege-escalation --out /tmp/authlab-privilege
go run ./cmd/authlab experiment --injection refresh-race --out /tmp/authlab-race
```

`--injection` 可独立选择任一案例；下划线写法也可用。`--concurrency` 控制并发刷新请求数，`--contention-wait` 控制等待信号量/租户锁的超时阈值。

每次实验为每个案例建立独立 SQLite 文件，输入不变时报告、状态导出和 `report.sha256` 均逐字节一致。报告的版本号和摘要均来自实际持久化快照，不使用预估值。

### HTTP 本地服务

```bash
go run ./cmd/authlab serve --addr 127.0.0.1:8080 --db authlab.sqlite
```

端点：

- `POST /v1/login`
- `POST /v1/refresh`
- `GET /v1/session`（Bearer 令牌 + `X-Tenant-ID`）
- `POST /v1/logout`（Bearer 令牌 + `X-Tenant-ID`）
- `GET /v1/healthz`

示例：

```bash
curl -s http://127.0.0.1:8080/v1/login \
  -H 'Content-Type: application/json' \
  -d '{"tenant_id":"tenant-a","username":"alice","password":"alice-demo-password-v1"}'

curl -s http://127.0.0.1:8080/v1/session \
  -H "Authorization: Bearer $ACCESS_TOKEN" \
  -H 'X-Tenant-ID: tenant-a'
```

`/v1/refresh` 若带 `X-Tenant-ID`，会使用 `RefreshForTenant`；令牌签名中的租户与端点租户不一致时返回 `E_CROSS_TENANT`，且不读取或修改任何租户会话。

### 样本与状态导出

```bash
go run ./cmd/authlab sample
go run ./cmd/authlab sample --out artifacts/fixture.json
go run ./cmd/authlab export --db authlab.sqlite --tenant tenant-a
```

## 确定性本地样本

| 租户 | 用户 | 口令 | 角色 | 权限 |
|---|---|---|---|---|
| `tenant-a` | `alice` | `alice-demo-password-v1` | `member` | `doc:read`, `doc:write`, `profile:read` |
| `tenant-a` | `bob` | `bob-demo-password-v1` | `auditor` | `audit:read` |
| `tenant-b` | `carol` | `carol-demo-password-v1` | `member` | `doc:read` |

- 口令使用 PBKDF2-HMAC-SHA256 和每个用户固定盐派生；样本默认迭代 20,000 次。
- 签名密钥和 ID 种子由固定字符串的 SHA-256 派生，仅用于本地确定性实验，不可用于生产。
- 确定性时钟固定为 `2035-01-01T00:00:00Z`。
- 样本不下载真实账号，不连接外部 IdP。

## 安全与状态模型

### 令牌

访问令牌和刷新令牌为 `v1.<base64url(payload)>.<base64url(HMAC-SHA256)>`。签名覆盖头段和载荷段。验证顺序为：格式、签名、必填声明、类型、过期时间，然后才读取租户分区。以下情况统一返回 `E_TOKEN_INVALID`：

- 签名不符或被替换；
- 已过期；
- 缺少租户、用户、会话、令牌 ID 或过期时间；
- 令牌类型不符；
- 会话/令牌族不存在或已撤销。

登录失败统一返回 `E_AUTH_INVALID`，未知租户、未知用户和错误口令不会得到可区分的响应；未知用户路径也执行等量口令派生工作，降低枚举信号。

### 租户隔离

所有会话状态位于 `snapshots(tenant_id, version, ...)`。每一条读取、变更、历史和修复查询都带 `tenant_id`。令牌中的租户来自已签名声明，状态查询永远使用该声明中的租户，不依赖调用方传入的租户。调用方租户只用于边界比较：

- `ValidateAccessForTenant` / `RefreshForTenant`：边界不一致返回 `E_CROSS_TENANT`。
- `AuthorizeResource`：资源租户不一致属于授权越界，返回 `E_PRIVILEGE_ESCALATION`。

### 版本化快照

- 版本 0 是 `GENESIS` 空状态，不写快照行。
- 登录、成功刷新、撤销/复用封族都会在单个 SQLite 事务中追加下一版本。
- 内容为字段顺序固定、集合排序固定的 JSON；摘要为内容 SHA-256。
- 每版记录 `previous_digest`，读取时校验版本从 1 连续递增、上一摘要链接、内容摘要、内容租户/版本，以及独立 `snapshot_heads` 头摘要。
- 缺失版本返回 `E_SNAPSHOT_VERSION`，并返回最近连续有效快照，而不是损坏版本。
- 同版本整体替换后即使重算该行内容摘要，只要未同步可信头摘要，也会被 `head_digest_mismatch` 拒绝。
- `RepairSnapshots` 截断首个断点之后的行并恢复头版本，随后可继续产生连续快照。

会话状态只存在于快照，不存在另一张可漂移的可变 session 表。

### 刷新令牌族谱

- 每次刷新成功：旧刷新令牌原子标记为 `used`，当前序号递增，签发新访问/刷新令牌。
- 旧刷新令牌再次使用：整个令牌族、会话及族内令牌被撤销，并返回 `E_TOKEN_REUSE`。
- 刷新族的 `current_serial` 与令牌记录序号保证同一时刻最多一个活动刷新令牌。
- 登录不接受客户端提供的 `prior_session_id`；该输入返回 `E_SESSION_FIXATION`。每次成功登录均生成全新会话 ID。

### 并发与资源控制

- 全局通道信号量限制在途认证事务，默认上限 32，可由 `Options.MaxInFlight` 配置。
- 每个租户有独立串行化门，登录、刷新、撤销和授权变更按租户串行执行。
- 信号量或租户门等待超过 `Options.ContentionWait` 返回 `E_AUTH_CONTENTION`，不会无限阻塞。
- 同一刷新令牌并发提交时，只有一个请求可提交轮换；其余请求得到 `E_TOKEN_REUSE` 或 `E_AUTH_CONTENTION`。
- 实验读取每个历史版本，断言活动刷新令牌峰值不超过 1，终态为 0。

`Options.TransactionHook` 在取得在途槽后、读取租户状态前运行，用于指标或确定性竞争编排；它不得递归调用同一引擎操作。

## 五类注入与验收断言

| 注入 | 触发方式 | 稳定业务码 | 终态/版本断言 |
|---|---|---|---|
| `expired` | 固定时钟前进 6 分钟和 2 小时 | `E_TOKEN_INVALID` | 会话仍 active，快照保持 v1 |
| `cross-tenant` | 用 `tenant-a` 的有效令牌访问 `tenant-b` 校验与刷新端点 | `E_CROSS_TENANT` | 两租户 v1 摘要均不变 |
| `session-fixation` | 登录时提交已有 `prior_session_id` | `E_SESSION_FIXATION` | 攻击不增版本；随后干净登录 v2 且会话 ID 不同 |
| `privilege-escalation` | 本地重签加入 `admin`/`admin:all`；访问其他租户资源；撤销 `doc:write` 后使用旧令牌 | `E_PRIVILEGE_ESCALATION` | 会话快照保持 v1，其他租户会话仍有效 |
| `refresh-race` | 一令牌多请求竞争：受限信号量与同令牌并发两组场景 | `E_AUTH_CONTENTION`、`E_TOKEN_REUSE`、`E_TOKEN_INVALID` | 仅 1 个成功；重放后族撤销，版本 v1→v2→v3，活动刷新令牌峰值 1、终值 0 |

报告同时记录每个动作前后版本、摘要、业务码、版本差和状态是否不变；每个租户还包含完整状态、版本历史和链审计。

## 可集成包接口

```go
fixture := sample.Fixture()
store, err := authlab.OpenSQLiteStore(ctx, dbPath, clock.Now)
if err != nil { /* ... */ }
if err := store.Init(ctx, fixture); err != nil { /* ... */ }
engine, err := authlab.NewEngine(store, fixture, authlab.Options{
    Clock:          clock,
    IDs:            authlab.NewDeterministicIDSource(fixture.IDSeed),
    MaxInFlight:    16,
    ContentionWait: 100 * time.Millisecond,
})
if err != nil { /* ... */ }

login, err := engine.Login(ctx, authlab.LoginRequest{
    TenantID: "tenant-a", Username: "alice", Password: "alice-demo-password-v1",
})
claims, err := engine.ValidateAccessForTenant(ctx, "tenant-a", login.AccessToken)

http.Handle("/session", engine.AuthenticationMiddleware(
    authlab.RequirePermission("doc:read", sessionHandler),
))
```

主要导出接口：

- `SessionStore`：租户作用域用户查询、权限更新、快照加载/历史/审计/修复、事务化快照变更。
- `Engine`：`Login`、`Refresh`、`RefreshForTenant`、`ValidateAccess`、`ValidateAccessForTenant`、`AuthorizeResource`、`RevokeSession`、`UpdateAuthorization`。
- `AuthenticationMiddleware` / `RequirePermission`：Bearer 头与 `X-Tenant-ID` 边界集成。
- `TokenCodec`：本地样本实验可使用同一密钥构造用于注入的签名样本；生产宿主不应暴露签发能力给不可信输入。

## 错误码

| 业务码 | 含义 |
|---|---|
| `E_TOKEN_INVALID` | 令牌格式、签名、租户、类型、过期或会话无效 |
| `E_AUTH_INVALID` | 登录凭据无效，不区分主体是否存在 |
| `E_CROSS_TENANT` | 签名租户与端点租户不一致，未读取/修改状态 |
| `E_SNAPSHOT_VERSION` | 快照缺版、断链、摘要/头不一致，已回退最近连续版本 |
| `E_TOKEN_REUSE` | 已使用/已撤销刷新令牌重放，令牌族撤销 |
| `E_SESSION_FIXATION` | 登录试图沿用客户端预置会话 ID |
| `E_AUTH_CONTENTION` | 在途事务或租户串行等待超阈值 |
| `E_PRIVILEGE_ESCALATION` | 令牌角色/权限不匹配当前授权，或越租户资源授权 |

## 实际验证结果

本机（Linux/amd64，Go 1.27.4）执行：

```text
cases=5 steps=25 failed_steps=0 passed=true
report SHA-256: 3b5933244229fe9841106489e5c8c1d2979bd9da16c883f248bd2247386c4db8
```

报告与状态导出连续运行两次逐字节一致；SHA-256 记录在 `artifacts/run/report.sha256`。实验还实际启动了 HTTP 服务，确认同租户会话读取成功、跨租户读取返回 HTTP 403/`E_CROSS_TENANT`、刷新返回实际快照递增版本。

已执行验证：

```text
go test ./...                  PASS
go test -race ./...            PASS
CGO_ENABLED=0 go build         PASS
GOOS=windows GOARCH=amd64 ...  PASS
scripts/verify-determinism.sh  PASS
```

测试覆盖：

- `go test ./...`：令牌篡改/过期、跨租户零改动、刷新重放封族、并发单活、权限提升/撤销、会话固定、快照同版本替换、缺版回退与修复、中间件边界。
- `experiment` 测试：全部报告连续两次逐字节相等，五类注入可分别独立运行。

## 说明

这是确定性与安全边界实验工程，不是生产身份系统。生产部署应使用经审计的 JWT/JOSE 或 PASETO 实现、KMS/HSM、密钥轮换、速率限制、审计日志和更强口令策略。当前固定密钥、可注入签名样本和 SQLite 单进程存储均服务于本地可复现验证。

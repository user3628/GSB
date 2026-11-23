# 多租户 MFA 因子注册与认证可用性裁决引擎

这是一个从空代码库开始构建的 Go 实现，使用纯 Go SQLite 驱动
`modernc.org/sqlite` 持久化租户、用户、因子、策略、认证轨迹、重绑定轨迹和版本化快照。
所有挑战与伪应答均由内置确定性生成器产生，不依赖网络、外部服务或真实账号。

## 1. 交付内容

- 多租户注册表与策略规范化，非法输入原子失败并返回 `E_POLICY_MALFORMED`。
- TOTP、一次性恢复码、WebAuthn 公钥凭证模拟件三类因子的确定性挑战-应答裁决。
- 因子成功/失败计数、连续失败降级、恢复轨迹和 `E_NO_VIABLE_FACTOR` 裁决。
- 用户 + 因子类型粒度非阻塞重绑定互斥、原子替换、竞争返回 `E_REBIND_CONTENTION`。
- SQLite 持久化的哈希链快照、旧快照回放、跳号回退、同版本整体替换识别。
- 全局并发信号量等待队列、单用户待处理预算和 `E_AUTH_BUDGET`。
- 外部参数驱动的五类异常注入、真实子进程强制中断恢复和稳定 JSON 诊断报告。
- 13 个确定性业务场景、2 个快照故障场景及完整 Go 测试。

## 2. 环境与构建

要求 Go 1.22 或更高版本。依赖已通过 `go.mod` / `go.sum` 固定；运行时不需要网络。

```bash
go version
go mod download
go test ./...
go build -o artifacts/mfa-engine ./cmd/mfa-engine
```

Windows PowerShell 示例：

```powershell
go version
go mod download
go test ./...
go build -o artifacts\mfa-engine.exe .\cmd\mfa-engine
.\artifacts\mfa-engine.exe suite --workdir artifacts\run --out artifacts\report.json
```

程序只访问命令行指定的本地工作目录和 SQLite 文件。`modernc.org/sqlite` 为纯 Go 实现，
不要求 CGO 或 C 编译器。

## 3. 目录结构

| 路径 | 说明 |
| --- | --- |
| `types.go` | 领域模型、配置、请求/结果、报告类型 |
| `config.go` | JSON 配置加载、严格字段校验、规范化入口 |
| `registry.go` | 注册、激活、恢复、过期处理及只读查询 |
| `auth.go` | 并发预算、策略选因子、挑战生成、认证裁决、降级 |
| `rebind.go` | 因子重绑定互斥和原子替换 |
| `snapshot.go` | 快照写入、哈希链、封印校验、回放、故障注入 |
| `report.go` | 稳定排序的 SQLite 诊断查询与 JSON 报告 |
| `store.go` | SQLite 打开、PRAGMA、DDL、持久化限制与提交钩子 |
| `engine_test.go` | 核心引擎端到端测试 |
| `internal/scenario/` | 内置确定性验收套件与报告确定性测试 |
| `cmd/mfa-engine/` | CLI、退出码、外部注入命令和崩溃子进程入口 |
| `artifacts/report.json` | 最近一次验收报告 |
| `artifacts/sample-config.json` | 与套件一致的示例配置 |

## 4. 快速验收

运行全部 Go 测试：

```bash
go test ./...
```

运行内置验收套件并生成报告：

```bash
go run ./cmd/mfa-engine suite --workdir artifacts/run --out artifacts/report.json
```

成功时命令输出类似：

```text
report=artifacts/report.json scenarios=13/13 faults=2/2
```

要将报告直接写到标准输出，使用 `--out -`。`artifacts/run*` 是可清理的确定性
SQLite 工作目录，不参与源码构建。

## 5. CLI

```text
mfa-engine [suite] [--workdir DIR] [--out FILE|-]
mfa-engine validate --config FILE
mfa-engine register --db FILE --config FILE
mfa-engine auth --db FILE --tenant ID --user ID --attempt ID [--offset N]
               [--mode valid|invalid] [--recovery-index N] [--hints ID,ID]
mfa-engine rebind --db FILE --tenant ID --user ID --type totp --new-id ID
                 --attempt ID [--sensitivity high] [--hold-ms N]
mfa-engine expire --db FILE --tenant ID --user ID --attempt ID
mfa-engine inject --db FILE --kind snapshot_version_gap|snapshot_wholesale_replacement
mfa-engine example-config [--out FILE|-]
mfa-engine recover --db FILE
mfa-engine crash-auth --db FILE --point before-commit|after-commit
                      --tenant ID --user ID --attempt ID
mfa-engine crash-rebind --db FILE --point before-commit|after-commit
                        --tenant ID --user ID --new-id ID --attempt ID
```

典型的独立启动流程：

```bash
go run ./cmd/mfa-engine example-config --out /tmp/mfa-config.json
go run ./cmd/mfa-engine register --db /tmp/mfa.db --config /tmp/mfa-config.json
go run ./cmd/mfa-engine auth --db /tmp/mfa.db --tenant acme --user alice --attempt demo-1
go run ./cmd/mfa-engine recover --db /tmp/mfa.db
```

## 6. 业务码

| 业务码 | 触发条件 |
| --- | --- |
| `OK` | 请求完成且认证/操作成功 |
| `E_POLICY_MALFORMED` | 标识重复、类型不支持、策略引用不可解析、字段非法等；不发生状态写入 |
| `E_CHALLENGE_FAILED` | 伪应答不匹配、TOTP 步长越窗、恢复码索引越界 |
| `E_CODE_CONSUMED` | 指定恢复码索引已被一次性消费，或恢复码已耗尽 |
| `E_NO_VIABLE_FACTOR` | 过期、降级、缺失、强度不足或提示因子不可用导致策略无法满足 |
| `E_REBIND_CONTENTION` | 同用户同类型已有重绑定在途，非胜者立即返回 |
| `E_SNAPSHOT_VERSION` | 版本跳号、封印缺失/不匹配、头指针不一致、同版本内容整体替换 |
| `E_AUTH_BUDGET` | 单用户待处理认证请求数超过上限；不进入数据库事务 |

认证业务失败会写入本次裁决快照并返回对应业务码；预算拒绝、缺失因子预检失败和
非法配置不会污染因子状态。`E_NO_VIABLE_FACTOR` 因没有选中的因子，只返回当前快照版本。

## 7. 核心设计

### 7.1 注册与规范化

`ValidateConfig` 在任何 SQLite 写入前完成：

- 租户、用户、因子标识唯一性；
- 用户所属租户、策略所属租户、因子所属用户的可解析性；
- 因子类型限于 `totp`、`recovery`、`webauthn`；
- 敏感度标签映射为强度：`low=1`、`medium=2`、`high=3`、`critical=4`；
- 激活标记、状态、恢复码数量、重绑定标记等字段一致性；
- 每用户每类型至多一个活跃因子；
- 策略要求的类型组合无重复且均受支持。

`Register` 将全部实体与首个快照放在同一个事务中；任一步失败都回滚。

### 7.2 认证裁决

一次认证请求拥有独立 `SessionID`/`AttemptID` 和局部结果，读取租户策略后：

1. 只从 `status=active`、`last_activated=1`、未过期且强度达标的因子中选择；
2. 对策略中的每种类型选取确定性的最高强度/最小标识因子；
3. TOTP 使用逻辑时钟生成时间步，仅接受 `abs(offset) <= totp_window`；
4. 恢复码按指定索引或最小未消费索引生成挑战，成功后仅在整组认证成功时消费；
5. WebAuthn 使用 `HMAC(secret_seed, attempt|factor|nonce)` 模拟签名；
6. 所有伪应答由本地确定性源给出，也可通过输入覆盖为失败应答。

该事务同时更新成功/失败计数、可能写入降级轨迹、消费恢复码、写认证使用轨迹，
最后提交递增版本快照。

### 7.3 降级与恢复

失败更新 `consecutive_failures`。当 `consecutive_failures > failure_threshold`：

- 因子切换为 `degraded`；
- 写入带 `attempt_id`、原因、连续失败数和快照版本的 `risk_traces`；
- 后续策略满足判定不再使用该因子。

`RestoreFactor` 允许风险审核后恢复为 `active`，清零连续失败并写 `restored` 轨迹和快照。
`ExpireFactors` 将到期活跃因子标为 `expired`，写 `expired` 轨迹和快照。

### 7.4 重绑定互斥

锁键为 `(tenant_id, user_id, factor_type)`，使用 `sync.Mutex.TryLock`：

- 胜者可在事务提交前保持 `hold_ms`，期间旧因子仍可通过认证；
- 竞争者在因子粒度立即得到 `E_REBIND_CONTENTION`，只记录竞争轨迹，不提交快照；
- 胜者在单个事务中插入新活跃因子、删除旧活跃因子、提交快照并记录成功轨迹；
- 活跃因子计数和用户因子总行数均保持不变；
- 旧标识保留在历史快照和 `rebind_traces` 中，可审计、可回放。

### 7.5 版本化快照

每次注册、认证裁决、因子激活、恢复、过期和重绑定都提交新快照：

```text
content_hash = SHA256(canonical_json_payload)
chain_hash   = SHA256(parent_chain_hash || version || content_hash)
```

快照正文、`snapshot_seals` 封印、`snapshot_meta` 头指针三处交叉校验。加载时：

- 版本必须从 1 开始连续递增；
- 父版本和父链哈希必须匹配；
- 正文哈希、封印哈希、链哈希必须一致；
- 头指针必须正好指向最后一个连续有效版本；
- 任一检查失败返回 `E_SNAPSHOT_VERSION`，并给出最近连续有效版本作为 fallback；
- 即使版本号不变但内容被整体替换，正文哈希/封印不一致会使该版本失效；
- `ReadSnapshot(version)` 支持旧快照回放，并再次检查父链。

### 7.6 并发与中断

- `MaxConcurrency` 由带缓冲信号量限制，超过并发上限的认证阻塞等待而非拒绝；
- `MaxPendingPerUser` 在进入队列前递增，超限立即 `E_AUTH_BUDGET` 并释放计数；
- 认证、重绑定、激活等写入由单个 SQLite 写锁串行化，读取使用独立事务；
- 数据库使用 WAL、`synchronous=FULL` 和外键约束；
- `BeforeCommit` / `AfterCommit` 钩子可被 CLI 用于执行 `os.Exit(86)` 真实中断；
- SQLite 的事务规则保证未提交变更整体丢弃、已提交状态和快照重启后恢复。

### 7.7 稳定诊断报告

`artifacts/report.json` 包含：

- 场景预期码、实际码、通过状态、租户、用户、因子、前后快照版本；
- 快照版本链、父子哈希、有效性和 fallback；
- 因子终态、累计成功/失败、连续失败、恢复码消费数量；
- 认证使用轨迹、降级/恢复/过期轨迹、重绑定成功/竞争轨迹与业务码；
- 跳号与整体替换故障的失效版本、回退版本和原因；
- 真实子进程中断恢复结果。

所有列表按 `tenant_id`、`user_id`、`factor_id`、`attempt_id`、序号稳定排序。
报告不含实际时间、随机数、临时路径或 map 迭代顺序，因此重复运行可字节级一致。

## 8. 五类异常注入复现

所有注入只通过外部参数或 CLI 命令进入，不修改源码。

### 8.1 因子标识缺失

```bash
mfa-engine auth --db /tmp/mfa.db --tenant epsilon --user erin \
  --attempt missing-1 --hints f-does-not-exist
```

预期：`E_NO_VIABLE_FACTOR`，快照版本不变。

### 8.2 恢复码重复消费

```bash
mfa-engine auth --db /tmp/mfa.db --tenant acme --user alice \
  --attempt replay-1 --recovery-index 0
```

先执行一次不使用 `--recovery-index` 的成功认证，再以上述命令指定已消费索引。
预期：`E_CODE_CONSUMED`；恢复码消费总数不增加。

### 8.3 重绑定竞争

终端 A 保持重绑定锁：

```bash
mfa-engine rebind --db /tmp/mfa.db --tenant delta --user dave \
  --type totp --new-id f-dave-v2 --attempt rb-hold --hold-ms 1000
```

在锁释放前于终端 B 执行：

```bash
mfa-engine rebind --db /tmp/mfa.db --tenant delta --user dave \
  --type totp --new-id f-dave-v2 --attempt rb-loser
```

预期：B 返回 `E_REBIND_CONTENTION`；A 提交后旧因子失效、新因子激活、因子总数不变。

### 8.4 快照版本跳号

```bash
mfa-engine inject --db /tmp/mfa.db --kind snapshot_version_gap
mfa-engine recover --db /tmp/mfa.db
```

预期：`E_SNAPSHOT_VERSION`，fallback 指向跳号前的连续版本。

### 8.5 同版本整体替换

```bash
mfa-engine inject --db /tmp/mfa.db --kind snapshot_wholesale_replacement
mfa-engine recover --db /tmp/mfa.db
```

预期：版本号未改变但内容封印不一致，返回 `E_SNAPSHOT_VERSION`，fallback 为前一有效版本。

## 9. SQLite 持久化模型

| 表 | 用途 |
| --- | --- |
| `tenants`, `users`, `policies` | 租户、用户与规范化策略 |
| `factors` | 活跃/待激活/降级/吊销/过期因子及计数 |
| `recovery_codes` | 一次性恢复码状态与消费尝试 |
| `snapshots`, `snapshot_seals`, `snapshot_meta` | 快照正文、封印和链头 |
| `usage_traces` | 每次因子挑战的确定性使用轨迹 |
| `risk_traces` | 降级、恢复、过期轨迹 |
| `rebind_traces` | 重绑定成功和竞争结果 |
| `engine_meta`, `operation_meta` | 逻辑时钟、限制和确定性序号 |

## 10. 实际验证结果

最近一次本地验证环境：Go 1.27.4（`go.mod` 目标 1.22），Linux amd64，纯 Go SQLite。

```text
go test ./...                  PASS
suite scenarios                13/13 PASS
snapshot fault injections       2/2 PASS
auth crash recovery             PASS
rebind crash recovery           PASS (before-commit old factor retained; after-commit new factor retained)
snapshot chain                  Code=OK
repeat-run bytes               identical
SHA-256                       76a4745b207a5349953414b91b127e9b7ed07691f5b25b451ed9c3a1bc7c0e7a
```

上述清单以实际生成的 `artifacts/report.json` 为准。CLI suite 退出码为 0，报告中的
`summary.failed=0`，两个故障注入 `passed=true`，`crash_recovery.passed=true`。

## 11. 确定性与范围说明

- 模拟 WebAuthn 不验证真实公钥、attestation 或浏览器来源；它验证确定性的凭证挑战/应答与状态机。
- TOTP 使用内置种子和时间步，不依赖系统真实网络时间；逻辑时钟来自配置。
- 敏感度标签到强度的映射固定为 1–4，便于策略比较和报告复现。
- 快照保存因子注册域状态；认证/风险/重绑定轨迹单独持久化并进入诊断报告。
- 当前实现聚焦单机确定性验收，不是生产身份提供方；生产化还需密钥保护、真实密码学验证、
  审计签名、速率限制和跨进程分布式锁。

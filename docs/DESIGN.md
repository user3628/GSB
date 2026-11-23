# 技术设计

## 1. 模块边界

`authquota.Engine` 是宿主可直接嵌入的领域组件。HTTP 只负责传输，核心规则均在包内：

- `IssueAuthorizationCode`：签发绑定 tenant/client/redirect/PKCE/nonce 的代码。
- `ExchangeAuthorizationCode`：严格验证并一次性消费代码，签发首代令牌。
- `Refresh`：校验刷新令牌、检测重放、轮换令牌、扣减配额、写账本和快照。
- `Revoke`：撤销单个访问令牌，或整族撤销刷新令牌家族。
- `Authenticate`：读取访问令牌、家族和租户快照健康状态。
- `Status` / `Ledger` / `ExportAll`：验证快照并输出确定性状态。
- `Errors`：读取带租户、请求 ID、账本/快照版本的审计事件。

## 2. 授权码兑换事务

1. 校验输入非空。
2. 由代码哈希查出租户并取得租户闸门，防止同租户快照并发。
3. 校验该租户快照链健康；跳号直接返回 `E_SNAPSHOT_VERSION`。
4. `BEGIN IMMEDIATE`，查询授权码和幂等记录。
5. 按顺序校验 client、redirect 全等、未消费、未过期、S256 challenge。
6. 任一业务校验失败：回滚，不写 `consumed_at`；另起审计事务记录错误码。
7. 条件更新 `consumed_at IS NULL`，确保只有一个兑换者消费成功。
8. 插入 token family、首代 refresh token 与 access token。
9. 追加 `token_issued` 快照，保存幂等响应，提交。

错误优先级保证 PKCE 不匹配返回 `E_PKCE_MISMATCH`；过期、已用、client/redirect
不匹配返回 `E_INVALID_GRANT`。

## 3. 刷新事务与重放

同一事务内按以下顺序执行：

1. 租户闸门 + 排队阈值检查。
2. 快照链健康检查。
3. 幂等键查询。命中且请求指纹一致时直接回放首次 JSON 响应，不扣费、不写新快照。
4. 读取刷新令牌和 family。
5. 若令牌或其家族已撤销：整族撤销 refresh/access token，追加
   `token_reuse_revocation` 快照，写 `E_TOKEN_REUSE` 审计并提交，账本不变。
6. 校验 client 与过期时间。
7. 定点计算本次扣减；溢出则回滚并写审计。
8. 余额不足：追加 `quota_exhausted` 快照并写审计，令牌与账本不变。
9. 失效旧 refresh token，插入同 family 的下一个序号 refresh/access token。
10. 更新租户配额和账本版本。
11. 追加 `token_rotation` 或 `quota_floor` 快照。
12. 插入账本行和幂等记录。
13. 提交；只有提交后才返回成功。

因此令牌轮换与扣减要么同时可见，要么同时不可见。重放分支虽然返回错误，但其
“整族撤销”安全动作会提交并产生快照，便于审计。

## 4. 定点算法与误差

```text
charged  = floor(requests * unit_cost * rate_num / rate_den)
truncNum = (requests * unit_cost * rate_num) mod rate_den
truncDen = rate_den
```

`requests`、`unit_cost`、`rate_num`、`rate_den` 和 `charged` 均为 `int64`。
`math/big` 只用于在写入前判断中间乘积是否越过 `int64`，最终金额仍是 `int64`。

每个余数 `< rate_den`，因此单次截断损失严格小于 1 纳单位。租户报告使用
`truncated_entries` 作为保守整数上界，并用精确有理数保存余数和，便于审计。

## 5. 快照与恢复

`tenant_snapshots` 主键是 `(tenant_id, version)`，每行含规范化 JSON 和 SHA-256。
读取从版本 1 开始逐号检查，且验证：

- 行版本等于期望版本。
- 存储哈希等于 JSON 哈希。
- 状态 schema 版本、租户 ID、内嵌版本一致。

一旦发现断点，停止信任后续版本，返回最近连续状态作为只读视图。`consistent`
恢复导出的账本按 `snapshot_version <= last_consistent_version` 截断，防止把断点后
的扣减混入只读视图。

`FaultSnapshotJump` 只会在成功轮换的 `token_rotation` 快照上跳过一号，以稳定构造
“扣减已提交但快照跳号”的恢复场景。

## 6. 并发控制

- `Gate.slots`：全局在途事务数上限。
- 每租户 lane：同一租户的签发/兑换/刷新/撤销串行。
- `maxQueue`：单租户最大等待数；超限立即拒绝。
- `QueueTimeout`：等待超时后返回 `E_QUOTA_CONTENTION`。
- 拒绝时写 `error_events`，包含 tenant、request_id、ledger_version、snapshot_version。
- SQLite 打开 WAL、`synchronous=FULL`、`busy_timeout(5000)`、`_txlock=immediate`。

`BEGIN IMMEDIATE` 避免“先读事务后升级写锁”在 WAL 中的 `SQLITE_BUSY_SNAPSHOT`。

## 7. 崩溃恢复

`Refresh` 提供两个测试钩子点：

- `mid_transaction`：令牌、配额、快照、账本和幂等记录已写入，但尚未提交。
- `after_commit`：事务已提交，钩子阻塞。

CLI 子进程在标记文件出现后被 `Process.Kill()` 强制终止。重启后 SQLite 自动回滚
未提交事务并保留已提交 WAL 记录。测试分别断言：

- `mid_transaction`：账本版本 0、配额未减、仅原 refresh token、原 access token 有效。
- `after_commit`：账本版本 1、配额已减、两个 refresh token、快照连续，且同一幂等键
  重启后仍返回首次结果而不二次扣费。

## 8. 数据表

| 表 | 用途 |
| --- | --- |
| `tenants` | 配额余量、账本版本、快照版本、定价规则 |
| `clients` | 租户下的客户端与注册 redirect URI |
| `auth_codes` | 代码哈希、绑定字段、nonce、过期与消费时间 |
| `token_families` | 家族、撤销时间与原因 |
| `refresh_tokens` | 家族序号、轮换链、撤销状态 |
| `access_tokens` | 访问令牌哈希、有效期、撤销状态 |
| `billing_entries` | 每个账本版本的整数金额与截断余数 |
| `truncation_entries` | 逐次截断误差账本，供租户聚合与上界计算 |
| `error_events` | 稳定业务码与租户/请求/版本审计 |
| `tenant_snapshots` | 连续版本 JSON 快照与哈希 |
| `idempotency_records` | AES-GCM 加密的首次响应、请求指纹、账本/快照版本 |

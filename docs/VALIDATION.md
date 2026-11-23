# 实际验证记录

执行日期：2026-11-24（Asia/Shanghai）

## 环境

```text
Go:    go1.24.6 linux/amd64
OS:    Linux 6.18.58.1-microsoft-standard-WSL2
Module: openplatform, go 1.22.0
SQLite driver: modernc.org/sqlite v1.34.5（pure Go, CGO disabled path）
```

## 命令与结果

### 全量测试

```text
$ go test ./... -count=1 -v
PASS openplatform/authquota
PASS openplatform/cmd/openplatform
PASS openplatform/internal/sample (no test files)

$ go test ./... -count=5
ok  openplatform/authquota
ok  openplatform/cmd/openplatform
?   openplatform/internal/sample [no test files]

$ CGO_ENABLED=0 go test ./... -count=1
ok  openplatform/authquota
ok  openplatform/cmd/openplatform
?   openplatform/internal/sample [no test files]
```

主要断言：

- 授权码 PKCE/redirect/client 失败后仍可正确兑换，第二次兑换返回 `E_INVALID_GRANT`。
- 刷新轮换扣减 `2 × 100000000 / 3 = 66666666`，余数 `2/3` 入账。
- 旧 refresh token 重放返回 `E_TOKEN_REUSE`，整族 access/refresh token 失效，账本不变，快照 +1。
- 同一幂等键重复刷新返回完全相同的响应、账本版本和快照版本，只产生一条账本。
- 授权码过期与强制 PKCE 不匹配分别返回稳定码，`consumed_at` 保持 NULL。
- 定点溢出返回 `E_PRECISION_OVERFLOW`，令牌、配额、账本和快照版本保持不变。
- 配额耗尽返回 `E_QUOTA_EXHAUSTED`，账本不变，快照 +1，原会话仍有效。
- 快照跳号返回 `E_SNAPSHOT_VERSION`；strict 拒绝导出，consistent 返回版本 2 只读视图。
- 并发排队超阈值返回 `E_QUOTA_CONTENTION`，审计记录 tenant、request_id、ledger_version。
- 提交钩子失败时轮换与扣减一起回滚，旧 token 可重试。
- 多租户配额隔离、显式整族撤销和 HTTP Bearer 中间件通过。
- 两套全新数据库的确定性导出字节级一致。

### Race 与静态检查

```text
$ go test -race ./authquota ./cmd/openplatform -count=1
ok  openplatform/authquota
ok  openplatform/cmd/openplatform

$ go vet ./...
（无输出，退出码 0）
```

### 子进程强杀恢复

```text
$ go test ./cmd/openplatform -run TestCLICrashRecovery -count=1 -v
--- PASS: TestCLICrashRecovery/after_commit_survives
--- PASS: TestCLICrashRecovery/mid_transaction_is_discarded
PASS
```

测试构建真实 CLI 二进制，使用标记文件确认到达事务内/提交后钩子，再执行
`Process.Kill()`。重启后：

- `mid_transaction`：账本 0 行、配额原值、refresh token 1 个、原 access token 有效。
- `after_commit`：账本 1 行、配额已扣、refresh token 2 个、快照连续；
  相同幂等键在重启后仍返回版本 1，不新增扣减。

### Windows 目标编译

```text
$ GOOS=windows GOARCH=amd64 go build ./...
WINDOWS_BUILD_OK

$ GOOS=windows GOARCH=amd64 go test -c ./authquota -o /tmp/authquota.test.exe
WINDOWS_TEST_COMPILE_OK

$ GOOS=windows GOARCH=amd64 go test -c ./cmd/openplatform -o /tmp/openplatform.test.exe
WINDOWS_BUILD_AND_TEST_COMPILE_OK
```

本环境为 WSL2，未直接运行 Windows 二进制；已提供原生 Windows PowerShell 验收脚本
`scripts/validate.ps1`，并完成 Windows/amd64 编译与测试二进制交叉编译。测试中的强制终止使用
`os.Process.Kill()`，Windows 与类 Unix 均由标准库支持。

### 字节级确定性

```text
$ go run ./cmd/openplatform demo --db /tmp/oauth-final-a.db > /tmp/final-a.json
$ go run ./cmd/openplatform demo --db /tmp/oauth-final-b.db > /tmp/final-b.json
$ sha256sum /tmp/final-a.json /tmp/final-b.json
cd3c63256b0263a9c31ce7e4763a745e641eb3365bcaada018314d772128b96b  /tmp/final-a.json
cd3c63256b0263a9c31ce7e4763a745e641eb3365bcaada018314d772128b96b  /tmp/final-b.json
BYTE_IDENTICAL
```

## 故障终态矩阵

| 注入参数 | 稳定业务码 | 账本 | 快照 | 观察 |
| --- | --- | --- | --- | --- |
| `auth_code_expired` | `E_INVALID_GRANT` | 不变 | 不变 | 代码未消费，审计保留 |
| `pkce_mismatch` | `E_PKCE_MISMATCH` | 不变 | 不变 | 代码未消费，审计保留 |
| `refresh_token_replay` | `E_TOKEN_REUSE` | 不变 | +1 | 整族撤销 |
| `precision_overflow` | `E_PRECISION_OVERFLOW` | 不变 | 不变 | 原会话仍有效 |
| `snapshot_version_jump` | `E_SNAPSHOT_VERSION` | 已提交行不丢 | 跳号 | strict 拒绝；consistent 返回最近一致只读视图 |
| `quota_exhausted` | `E_QUOTA_EXHAUSTED` | 不变 | +1 | 配额不扣、令牌不轮换 |

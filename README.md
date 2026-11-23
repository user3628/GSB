# leasearbiter — 行级写权租约与版本化读快照仲裁引擎

面向本地多进程协同编辑客户端的并发控制组件。写侧用带 TTL 的行级排他租约串行化
同行修改，读侧用单调递增的已提交版本号提供无锁读快照；两者在单事务提交点收敛，
并通过写前意图保证强制终止后的一致恢复。

## 快速开始

```bash
go version            # 需要 Go 1.22+
go build -o bin/arbiter ./cmd/arbiter
./bin/arbiter -addr 127.0.0.1:18080 -db data/arbiter.db -seed sample/seed/rows.json

# 另一个终端
curl -s 127.0.0.1:18080/healthz
curl -s -X POST 127.0.0.1:18080/leases/acquire \
  -H 'Content-Type: application/json' \
  -d '{"row_id":"doc-100/body","holder":"alice","ttl_ms":5000}'
```

## 一键验证

```bash
go test -race ./...
./sample/scripts/run-all.sh
```

## 目录

- `cmd/arbiter`：本地 HTTP 服务入口（仅回环绑定）。
- `internal/store`：SQLite schema、事务与意图/版本/租约持久化。
- `internal/arbiter`：租约、快照、回收、幂等、预检与崩溃恢复策略。
- `internal/httpapi`：HTTP 路由与统一错误信封。
- `sample/seed`：确定性种子与场景说明；`sample/scripts`：本地演示脚本。
- `docs/ARCHITECTURE.md`：一致性设计论证。
- `docs/HTTP_API.md`：接口手册与错误码。
- `docs/VALIDATION.md`：实际验证结果。

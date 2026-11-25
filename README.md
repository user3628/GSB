# gpusched — 单机 GPU 显存配额调度与批推理裁决组件

一个可集成的 Go 领域数据编辑与操作应用：在单机 Linux 上用 SQLite 持久化逻辑 GPU 节点、
配额组、推理请求、策略快照与批记录，通过本地 HTTP 接口完成规格装载、配额准入、批组织、
抢占回收与崩溃恢复，保证节点占用、组预留、批组成与请求状态在同一提交边界内共同生效。

## 快速开始

```bash
# 1. 生成固定种子确定性样本（可重复，字节级一致）
go run ./cmd/gpusched generate -out samples -seed 20261126

# 2. 启动服务（-auto-sample 首次自动装载样本；启动时自动执行恢复）
go run ./cmd/gpusched serve -db data/gpusched.db -addr 127.0.0.1:8080 -auto-sample

# 3. 用本地客户端操作
BASE_URL=http://127.0.0.1:8080 scripts/client.sh quota
BASE_URL=http://127.0.0.1:8080 scripts/client.sh admit req-001
BASE_URL=http://127.0.0.1:8080 scripts/client.sh round
BASE_URL=http://127.0.0.1:8080 scripts/client.sh batches
```

## 强制终止恢复演练

```bash
scripts/crash_recovery.sh        # 构建 -> 装载 -> 武装崩溃点 -> kill -9 风格退出 -> 重启恢复 -> sqlite3 核对
```

## 测试

```bash
go test -race ./...
go vet ./...
```

## 目录

- `cmd/gpusched`：CLI（`generate` / `serve` / `recover`）
- `internal/domain`：量纲解析、状态机、归一化校验、摘要
- `internal/gen`：固定种子确定性样本生成器
- `internal/store`：SQLite schema 与事务/访问器
- `internal/scheduler`：延迟估算与批组织算法
- `internal/service`：单写者裁决、准入、抢占、轮次、迁移、恢复
- `internal/httpapi`：HTTP 路由与统一错误码
- `docs/DESIGN.md`：架构与一致性设计
- `docs/VERIFICATION.md`：实际验证结果

## 错误码

`E_SPEC_INVALID`、`E_QUOTA_EXCEEDED`、`E_INVALID_TRANSITION`、`E_EXEC_QUEUE_FULL`、
`E_QUOTA_LEAK`、`E_CONFLICTING_REQUEST`、`E_NOT_FOUND`、`E_POLICY_VERSION`。

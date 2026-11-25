# configtree — 配置订阅树收敛与陈旧读裁决管线

本地多组件平台读路径的配置收敛管线：键路径树前缀匹配、组件依赖展开、
快照/增量版本提交、有效视图与受影响集合重算、陈旧读拒绝、级联原子重建、
单写者/多读者并发与硬杀恢复。持久化使用纯 Go 的 `modernc.org/sqlite`，
依赖已 vendor，运行期不需要网络、容器或外部服务。

## 快速开始

```bash
go version                 # 需要 Go 1.22+（本机实测 1.27.4）
go build ./...             # 或 go build -mod=vendor ./... 完全离线构建
go test ./...              # 单元 + 引擎集成测试

# 生成确定性样本到 samples/
go run ./cmd/configtree -gen-seed -samples samples

# 一条命令完成固定样本端到端验证（含真实硬杀恢复）
./scripts/validate.sh

# 启动本地 HTTP 服务
mkdir -p data
go run ./cmd/configtree -db data/pipeline.db -addr 127.0.0.1:8080

# 真实 os.Exit(137) 硬杀 + 重启恢复演示
go run ./cmd/crashdemo -db data/crashdemo.db
```

## 提交信封与错误响应

```json
// POST /v1/commits/snapshots | /v1/commits/deltas
{"commit_id": "snap-001", "payload": { /* 快照或增量样本 */ }}

// 统一错误响应
{"code": "E_STALE_BASE", "message": "...", "context": {"base_version": 1, "current_version": 2}}
```

业务码：`E_SPEC_INVALID`、`E_STALE_SNAPSHOT`、`E_STALE_BASE`、
`E_STALE_READ`、`E_CONFLICTING_REQUEST`（另有 `E_NOT_FOUND`）。

## 文档

完整设计、事务边界、裁决规则与验证结果见 [DESIGN.md](DESIGN.md)。

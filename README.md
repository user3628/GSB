# EdgeConfig Distributor

纯 Go、无 CGO 的边缘节点配置分发组件与本地验收工具。它管理配置键、边缘节点、订阅和依赖，提交单调版本快照/增量，推导影响面与节点期望摘要，裁决陈旧观察版本，发起真实 loopback HTTP 探测，计算定点一致度，并用 SQLite + 意图日志支持幂等和崩溃恢复。

## Windows 快速验证

环境：Windows 10/11，Go 1.22+；不需要 Docker。

```powershell
git clone <this-repo>
cd <repo>
powershell -ExecutionPolicy Bypass -File .\scripts\verify.ps1
```

脚本会执行：

- `go version`
- `go test ./... -count=1`
- `go build -o bin\edge-dist.exe .\cmd\edge-dist`
- 用 `samples\` 固定样本启动真实 loopback 模拟端点
- 验证增量收敛、`E_STALE_READ`、`E_STALE_SNAPSHOT`、`E_STALE_BASE`
- 验证真实探测成功、HTTP 423 业务拒绝、`E_PROBE_TIMEOUT`
- 两次强制 `TerminateProcess` 风格的强杀与重启恢复
- 写出机器可读报告：`artifacts\verification-report.json`

也可以手动运行：

```powershell
go test ./... -count=1
go run .\cmd\edge-dist verify --samples samples --db .runtime\verify.db
```

Linux/macOS 对应入口是 `./scripts/verify.sh`。

## 启动可集成 HTTP 服务

```powershell
go run .\cmd\edge-dist serve --addr 127.0.0.1:18080 --db .runtime\edgeconfig.db --samples samples
```

主要接口：

- `POST /v1/topology`：整批校验并保存键、节点、订阅、依赖和策略。
- `POST /v1/snapshots`：提交带版本和摘要的全量快照。
- `POST /v1/increments`：基于当前版本提交局部键和订阅变更。
- `POST /v1/affected/recompute`：按版本重算影响面。
- `GET  /v1/affected/{version}`：读取影响面报告。
- `POST /v1/reports`：节点上报摘要、生效时刻和观察版本。
- `POST /v1/probe-rounds`：发起真实 HTTP 探测轮次。
- `GET  /v1/convergence`：读取节点收敛视图。
- `GET  /v1/consistency?scope=global|region:<region>`：读取定点一致度。
- `GET  /v1/intents?status=PENDING`：查看意图。
- `POST /v1/recovery`：扫描未完成意图并恢复。

错误响应统一为：

```json
{"error":{"code":"E_STALE_READ","message":"...","details":{"expected_version":2,"current_view_version":2}}}
```

## 模拟节点端点

```powershell
go run .\cmd\edge-dist node-endpoint `
  --node-id edge-c `
  --addr 127.0.0.1:19093 `
  --digest sha256:... `
  --version 2

# 超时场景
go run .\cmd\edge-dist node-endpoint --node-id edge-c --addr 127.0.0.1:19093 --hang 5s

# 业务拒绝场景
go run .\cmd\edge-dist node-endpoint --node-id edge-b --addr 127.0.0.1:19092 --reject-status 423 --reject-code E_NODE_AGENT_BUSY
```

## 固定样本

- `samples/topology.json`：6 个配置键、6 个节点、跨 3 个区域、两级下游依赖。
- `samples/tolerance.json`：影响面策略、探测超时/并发、定点 scale、阈值、相对容差和权重。
- `samples/snapshot-v1.json`：版本 1 全量快照和真实 SHA-256 摘要。
- `samples/increment-v2.json`：一次改 3 键并新增一个订阅。
- `samples/observations.json`：新/旧观察版本、探测成功/拒绝/超时和恢复场景。
- `samples/invalid-topology.json`：非法路径、重复节点 ID、未声明订阅。

## 关键契约

- 版本：快照版本必须大于当前版本；增量 `base_version` 必须等于当前版本。
- 陈旧读：`observed_version < expected_version` 时返回 `E_STALE_READ`，错误详情含 `expected_version` 和 `current_view_version`，不会把旧摘要判定为收敛。
- 影响面：输出直接节点、下游 BFS 级联、`hop`、`trigger_keys`、`selected_path`、`selection_basis` 和扫描指标；达到声明键数阈值时走区域分桶，避免逐键全节点扫描。
- 探测：每个探测在取得并发名额后建立独立 `context.WithTimeout`；超时会取消请求、关闭跟踪到的底层连接和空闲连接，并通过 `defer` 释放名额。
- 原子性：探测记录、节点状态、一致度、幂等记录和意图完成标记在同一个 SQLite 事务提交。
- 数值：权重和全部比较使用 `int64`/`math/big` 整数；默认 scale 为 `1_000_000`，采用底账 + 最大余数/稳定字典序补齐，输出每步余数、舍入值和累计误差。
- 并发：进程内单写者互斥串行化提交；读请求直接走 SQLite/WAL 已提交快照。相同请求 ID 后到者等待并复用首次结果，内容哈希不同返回 `E_CONFLICTING_REQUEST`。
- 恢复：所有写请求先持久化 `PENDING` 意图；强杀后重启执行 `Recover`，中止未完成增量/探测/重算意图，并从 `expected_views` 最后完整版本重建收敛视图和一致度。

更详细的映射见 `docs/IMPLEMENTATION.md`，实际验证记录见 `docs/VERIFICATION.md`。

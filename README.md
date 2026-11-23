# Aegis · 变更风险分级与自动回滚裁决台

面向本地微服务测试环境的单机工作台。系统以版本化策略快照、确定性观测样本和纯 Go SQLite 为基础，对变更批次计算可审计风险分，裁决继续观察、放行、自动回滚或升级人工处置。所有网络目标均绑定回环地址，不依赖容器、账号或外部服务。

## 快速开始

要求 Go 1.22+、可访问模块代理的 Go 模块缓存。SQLite 使用纯 Go 驱动 `modernc.org/sqlite`，不需要 CGO 或本机 SQLite 开发库。

```bash
go mod download
go test ./...
go run ./cmd/workbench
```

打开 `http://127.0.0.1:8080`。首次进入为空工作台，点击“初始化本地工作台”即可载入固定样本；也可以执行完整自动验收：

```bash
./scripts/demo.sh          # Linux / macOS / WSL
.\scripts\demo.ps1         # Windows PowerShell
```

演示驱动会编译工作台、启动本机回滚 HTTP 端点、完成放行、自动回滚、冷却期拦截，并用真实 `exit(137)` 强杀风险重算进程后重启核对恢复报告。结果写入 `artifacts/demo-report.json`。

## 已实现能力

- 策略快照单调版本提交、SHA-256 整体摘要、陈旧版本 `E_STALE_SNAPSHOT`，校验失败不触碰旧快照。
- 批次与观测模型：毫秒时间戳、指标类型、样本数、成功数、延迟样本和版本化时间桶。
- 观测批次原子摄入：序号幂等、内容冲突 `E_CONFLICTING_REPORT`、时间戳回退/非法指标/负样本整批拒绝 `E_OBSERVATION_INVALID`。
- 整数风险计算：PPM 指标、微单位分项、10,000bp 风险分、`round_half_up`、精确余数和累计舍入误差审计。
- 确定性最近秩 P90：`ceil(0.9 × n)`，输出样本数和秩；样本不足返回 `E_INSUFFICIENT_SAMPLES`。
- 四档风险等级与五态批次状态机，含容差内下档、固定平局顺序和 `E_INVALID_TRANSITION`。
- 快速/完整裁决：同一 SQLite 读快照、同一策略版本和观测修订；快速模式逐项输出采样率、方法、排除序号与置信说明，未采样时禁止放行。
- 自动回滚：策略开关、批次状态、服务冷却期共同校验；状态、冷却占用和风险裁决同事务提交；回滚触达失败升级人工。
- 意图优先持久化：策略、批次、观测、风险重算和回滚裁决均先写 `intents`；进程强杀后启动扫描未完成意图。
- 恢复报告只读取 SQLite 已提交事实，输出中止意图、恢复回滚、批次状态与最新提交序号。
- 原生离线 Web 工作台，支持批次筛选、快速/完整对照、数值审计、增量观测、手动回滚、冷却与恢复查看。

## 主要接口

| 方法 | 路径 | 说明 |
|---|---|---|
| `POST` | `/api/policies` | 提交版本化策略快照 |
| `GET` | `/api/policies/current` | 查询当前策略 |
| `POST` | `/api/batches` | 创建变更批次 |
| `GET` | `/api/batches` | 批次列表 |
| `POST` | `/api/batches/{id}/observations` | 原子摄入一份或多份观测 |
| `GET` | `/api/batches/{id}/risk?mode=fast\|full\|compare` | 从单一读快照预览风险与裁决 |
| `POST` | `/api/batches/{id}/evaluate?mode=full\|fast` | 持久化风险裁决并按状态机执行 |
| `POST` | `/api/batches/{id}/rollback` | 同事务占用冷却期并触达回滚端点 |
| `GET` | `/api/batches/{id}/cooldown` | 查询服务冷却状态与剩余时长 |
| `GET` | `/api/cooldowns` | 查询全部冷却 |
| `POST` | `/api/batches/{id}/transition` | 显式状态迁移，非法迁移返回业务码 |
| `GET` | `/api/recovery` | 最近一次启动恢复报告 |
| `GET` | `/api/snapshot` | 工作台单一快照 |
| `GET` | `/api/intents` | 最近意图记录 |
| `POST` | `/api/demo/seed` | 重置并载入固定演示样本 |

失败响应统一为：

```json
{"ok":false,"error":{"code":"E_ROLLBACK_COOLDOWN","message":"...","context":{"remaining_ms":119995}},"meta":{"elapsed_ms":2}}
```

## 项目结构

```text
cmd/workbench       工作台 HTTP 进程
cmd/webhook         独立本地回滚触达端点（演示驱动内也使用回环 HTTP 端点）
cmd/demo            跨平台端到端验证驱动
internal/domain     模型、校验、业务码、状态机
internal/risk       整数风险计算、P90、采样与审计
internal/store      SQLite/WAL、提交序号、意图与恢复表
internal/engine     事务编排、回滚触达、启动恢复
internal/httpapi    统一 JSON API
internal/web        原生离线 UI（嵌入二进制）
samples             固定策略、批次、观测
scripts             可重复演示入口
docs                演示与技术说明
```

更完整的事务与数值设计见 `docs/design.md`，逐步演示说明见 `docs/demo.md`。

## 最终验证

- `go test -race ./...` 与 `go vet ./...`：全部通过，覆盖策略陈旧/无效、观测幂等/冲突/整批拒绝、快速安全守护、放行、自动回滚、冷却拦截、自动回滚关闭升级、非法迁移、风险/观测/回滚中途强杀恢复和 HTTP 错误封装。
- 本地验证环境为 Go `1.27.4 linux/amd64`；另以 `GOOS=windows GOARCH=amd64 CGO_ENABLED=0` 成功构建工作台与演示驱动。
- `./scripts/demo.sh`：10 项端到端断言通过；放行风险分 `389bp`，回滚批次 `10000bp`，冷却剩余约 `120s`，恢复后批次保持 `OBSERVING`、最新提交未前进，重新裁决可完成 `ROLLED_BACK`。
- 实际报告：`artifacts/demo-report.json`。

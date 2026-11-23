# Gateway Freshness Workbench

面向网关请求路径的本地请求签名、时间戳时效窗口、nonce 去重与回放诊断工作台。项目从零实现，只使用 Go 标准库与 `modernc.org/sqlite`，不访问外部账号、外部服务或线上数据。

## 核心结论

- 签名：固定本地密钥上的 HMAC-SHA256，规范化字段采用长度前缀编码。
- 时间：`offset_ms = timestamp_ms - gateway_reference_time_ms`，接受区间为 **双边闭区间** `[-tolerance_ms, +tolerance_ms]`。
- 去重：同一 `(client_id, nonce)` 在有效去重窗口内仅首次裁决可成功；窗口到期后先回收、再参与去重。
- 一致性：签名、时间、nonce、裁决写入同一个 SQLite 写事务，并共享同一个 `window_version` 与 `tolerance_config_version`。
- 幂等：`batch_id` 是提交级幂等键。同内容重复提交等待并复用已提交结果；内容变化返回 `E_CONFLICTING_REQUEST`。
- 快速模式：仅检查确定性前缀，未预检项明确返回 `not_prechecked / E_PRECHECK_INCOMPLETE`，绝不计为通过。
- 诊断：查询返回单一已提交版本；预算耗尽时返回同一版本下已完成区段，并明确列出未完成区段。
- 恢复：意图记录先于裁决事务提交；进程在写事务中途被终止后，重启扫描 pending 意图，删除孤儿事实并标记中止。

详细设计见 `docs/DESIGN.md`，接口说明见 `docs/API.md`，实测记录见 `docs/VERIFICATION.md`。

## Windows 快速开始

要求：Windows 10/11、PowerShell 5.1+、Go 1.22+。项目使用纯 Go SQLite 驱动，因此不要求 CGO；如果希望安装 gcc，可由 `verify.ps1` 检测并报告。

```powershell
cd <repo-root>
go version
go mod download
.\scripts\verify.cmd
```

启动本地服务（默认只监听 `127.0.0.1:8787`）：

```powershell
.\scripts\run-workbench.ps1
```

然后打开 `http://127.0.0.1:8787/`。工作台可载入并提交内嵌样本、查看逐条裁决、执行一致性快照、比较 fast/full 结论并查看客户端版本不一致明细。

一键演示：

```powershell
.\scripts\demo.cmd
.\scripts\crash-recovery-demo.cmd
```

`demo.ps1` 使用临时 SQLite 文件重复验收签名、时间边界、重放、回收、快速模式、幂等与冲突。`crash-recovery-demo.ps1` 在裁决事务提交前强制终止进程，再重启读取恢复报告并重试同一批次。

## 本地样本

样本位于 `samples/batches/`，固定参考时刻为 `1800000000000`。按名称顺序提交时的主要预期如下：

| 样本 | 预期 |
| --- | --- |
| `01-sample-accept-001.json` | 完整模式接受 2 条；正负 500ms 边界均包含 |
| `02-sample-boundary-002.json` | `+501ms` 返回 `E_TIMESTAMP_AHEAD`；`-501ms` 返回 `E_TIMESTAMP_TOO_OLD` |
| `03-sample-signature-003.json` | `E_SIGNATURE_MISMATCH` |
| `04-sample-replay-004.json` | `E_NONCE_REPLAYED`，携带首次占用时刻与剩余窗口 |
| `05-sample-fast-005.json` | 前 2 条预检，后 2 条显式 `not_prechecked`，批次为 `partial` |
| `06`–`08` | 窗口到期回收后，同一 nonce 可重新占用；形成跨版本不一致场景 |
| `10-sample-mode-full-010.json` | 完整模式中 4 条均成功；诊断投影显示 fast 后 2 条未预检 |
| `11-sample-invalid-nonce.json` | `E_REQUEST_INVALID`，整批不裁决且不消耗版本 |
| `invalid-weight-overflow.json` | JSON 整数超出 int64，`E_REQUEST_INVALID` |
| `conflict-same-id-001.json` | 对已提交 `01` 的同 ID 不同内容返回 `E_CONFLICTING_REQUEST` |

重建样本：

```powershell
go run .\cmd\samplegen -config .\samples\config.json -out .\samples\batches
```

`samples/config.json` 中的密钥仅用于本地确定性演示，不能用于生产。

## 本地构建与运行

```powershell
go test ./...
go vet ./...
$env:CGO_ENABLED = "0"; go build -trimpath -o .build\workbench.exe .\cmd\workbench
.\.build\workbench.exe -config samples\config.json -db data\workbench.db -addr 127.0.0.1:8787
```

服务启动后会先执行恢复扫描，并把只基于落盘事实的恢复报告写入 SQLite 和日志。所有 HTTP 地址由配置校验为回环地址；`0.0.0.0`、局域网地址和缺失端口的地址会被拒绝。

## 测试覆盖

```powershell
go test ./... -count=1
go test -race ./... -count=1   # 可选，需当前平台支持 race detector
```

自动化测试包含：

- nonce 长度/字符集、负数时间戳、非正权重、int64 权重溢出与整批原子拒绝。
- HMAC 规范化确定性、闭区间上下边界、旧/未来时间偏移及触发边界。
- 同客户端重放、不同客户端 nonce 独立、窗口到期回收与复用。
- 在途意图引用阻止 nonce 回收，引用结束后才允许回收。
- 20 路并发幂等复用、冲突不改变已持久化结论。
- 快速模式采样率、未预检项不被静默通过、fast/full 逐条差异。
- 单一版本快照、零预算部分结果、历史去重表重建、无跨版本混合。
- 客户端拾取版本差异明细，包含客户端标识、版本和差异字段。
- 真实子进程在 `after_intent` 与 `after_writes` 两处强制退出后的恢复。
- HTTP 状态码、错误码、静态工作台页面和内嵌样本 API。

## HTTP API 摘要

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| `POST` | `/api/v1/adjudications` | 提交事件批次并返回裁决 |
| `GET` | `/api/v1/adjudications/{batch_id}` | 读取已提交裁决 |
| `GET` | `/api/v1/diagnostics/snapshot` | 同版本读取占用、去重表、轨迹 |
| `GET` | `/api/v1/diagnostics/occupancy` | 时效窗口占用 |
| `GET` | `/api/v1/diagnostics/nonces` | nonce 去重表（含已回收历史状态） |
| `GET` | `/api/v1/diagnostics/trails` | 裁决轨迹 |
| `GET` | `/api/v1/diagnostics/mode-differences` | fast/full 逐条差异 |
| `GET/POST` | `/api/v1/diagnostics/inconsistencies` | 多客户端版本视图差异 |
| `GET` | `/api/v1/diagnostics/recovery` | 最近一次恢复报告 |
| `GET` | `/api/v1/diagnostics/recovery/history` | 恢复历史 |
| `GET` | `/api/v1/samples` | 列出本地嵌入样本 |
| `GET` | `/api/v1/samples/{name}` | 读取本地嵌入样本 |

## 约束与边界

- 本实现是本地单进程单写者模型；SQLite WAL 支持并发只读。它不是分布式锁或跨主机去重服务。
- 随机数窗口按批次提供的网关参考时刻推进；客户端签名字段包含其声明的 tolerance/window，服务端还会检查配置上限。
- 确定性参考时刻覆盖仅在 `allow_reference_override=true` 的本地样本模式启用；生产配置应关闭。
- `batch_id` 是幂等键。批次内 `request_id` 必须唯一，用于轨迹与差异定位。
- 幂等摘要不包含服务端派生的网关参考时刻；同一请求内容在稍后重试会复用首次裁决。
- 快速模式是安全保守的：未预检即未通过，批次结论为 `partial`，后续可重新提交完整模式审计。

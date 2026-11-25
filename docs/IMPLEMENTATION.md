# GrayRouter 组件与诊断工作台说明

一套无外部服务依赖的 Go 组件：维护模型版本算子图、生成确定性样本、计算签名/差异、驱动四类派生产物迁移或重建、执行确定性算子图、稳定灰度裁决、绑定在途请求，并用纯 Go SQLite 持久化意图与恢复状态。

## 1. 快速开始

Windows 10+ PowerShell：

```powershell
go version                 # 需要 Go 1.22+
go env GOOS                # 在 Windows 本机应输出 windows
go mod tidy                # 固定并下载 modernc.org/sqlite v1.35.0
.\scripts\verify.ps1       # 构建、测试、端到端演练、强杀恢复、生成 exe
```

也可以手动执行：

```powershell
go build ./...
go test ./...
go run ./cmd/verify
go run ./cmd/server --addr 127.0.0.1:18080 --db .\data\grayrouter.db --samples-dir .\samples
```

浏览器打开 <http://127.0.0.1:18080/>。服务会生成 `samples/*.json`；空库首次启动会自动提交基线 v1 和增量 v2，便于直接查看迁移清单。

如默认 Go 代理超时，可设置国内代理：

```powershell
go env -w GOPROXY=https://goproxy.cn,direct
```

当前仓库附带 `vendor/`，依赖下载完成后可使用 `GOPROXY=off go test ./...` 离线验证。

## 2. 可集成 Go API

核心 module 为 `grayrouter`，直接 import 根包：

```go
ctx := context.Background()
h, err := grayrouter.Open(ctx, grayrouter.Config{DBPath: "data/grayrouter.db"})
if err != nil { panic(err) }
defer h.Close()

bundle := grayrouter.SampleBundleFixed()
report, err := h.SubmitSnapshot(ctx, grayrouter.CommitSnapshotInput{
    RequestID: "req-commit-0001",
    Snapshot:  bundle.Baseline,
})
```

主要方法：

- `ValidateGraph` / `ParseGraph`：结构校验与业务错误。
- `GraphSignature` / `StructuralSignature` / `DiffModelVersions`：签名和差异类别。
- `PlanMigration`：差异类别到四类产物动作的矩阵。
- `Host.SubmitSnapshot` / `Host.SubmitDelta`：单写者提交、幂等和版本冲突控制。
- `Host.BindInference` / `Host.CompleteBound` / `Host.Infer`：在途版本绑定与推理。
- `Host.DecideGray`：定点灰度和稳定裁决。
- `Host.Recover`：启动/手动扫描未完成意图。
- `NewHTTPServer`：嵌入诊断工作台和 JSON API。

## 3. 图描述与校验

每个 `ModelVersion` 包含 `name`、`version`、`graph.nodes`、路由与灰度配置。节点字段：

- `id`、`op`
- `inputs[]`、`outputs[]`：`name`、正整数 `shape`、允许集合内 `dtype`
- `quant_bits`：允许 `2/4/8/16/32`
- `pre_steps[]`、`post_steps[]`
- 可选 `attrs`

允许 dtype：`float32`、`float64`、`int8`、`int32`、`uint8`。校验失败返回：

```json
{
  "ok": false,
  "error": {
    "code": "E_GRAPH_INVALID",
    "message": "operator graph validation failed",
    "context": { "errors": [{"node_id":"n1","field":"outputs[0].shape[1]","reason":"..."}] }
  }
}
```

非法图在任何 SQLite 写入前返回，因此不会入库。

## 4. 签名、差异与迁移矩阵

- `structural_signature = SHA256("grayrouter-structural-graph-v1" + canonical(nodes(order,id,op,inputs,outputs,quant_bits)) + outputs)`
- `signature = SHA256("grayrouter-full-graph-v1" + canonical(含 pre/post/attrs 的节点))`

差异类别：`shape_change`、`dtype_change`、`quant_bits_change`、`operator_change`、`prepost_change`。

| 差异类别 | 路由表 | 前后处理缓存 | 在途绑定索引 | 灰度计划 |
|---|---|---|---|---|
| 形状变化 | rebuild | rebuild | migrate/pin old | migrate |
| dtype 变化 | rebuild | rebuild | migrate/pin old | migrate |
| 量化位宽变化 | migrate | rebuild | migrate/pin old | migrate |
| 算子增删/类型/顺序 | rebuild | rebuild | migrate/pin old | rebuild |
| 前后处理变化 | migrate | rebuild | migrate/pin old | migrate |

`migrate` 表示保留稳定标识、百分比或旧版本绑定；`rebuild` 表示从新图重新生成。最终提升在同一个 SQLite 事务中更新 head、当前模型指针、路由、缓存、灰度计划和意图状态。

## 5. 增量、并发与幂等

- 快照版本必须单调递增；整份快照在最终事务中替换 `current_models`。
- 增量携带 `base_version`。base 不等于当前 head 时返回 `E_STALE_BASE`。
- 同一 base 的两个并发增量由进程内 base gate 串行裁决：一个成功，另一个得到 `E_CONFLICTING_UPDATE`。
- `Idempotency-Key`、`X-Request-ID` 或 JSON `request_id` 用于幂等。同 key 的并发调用等待同一个执行结果；完成后从 SQLite 复用同一 JSON。
- 所有提交和推理推进通过单写者 token 串行；状态查询和 SQLite WAL 读可并发。

## 6. 推理、量化与摘要

内置算子：`input`、`linear`、`relu`、`sigmoid`、`add`、`scale`、`output`。输入由固定种子 `20261125` 结合 model/node/tensor 上下文派生，不访问外部模型或 GPU。

每个输出包含：

- `shape`、`dtype`、`element_count`
- 按声明 dtype 的大端类型位计算的 `sha256` 内容摘要
- min/max/mean 和前 8 个值

`quant_bits < 32` 时执行 symmetric fake-quant：

1. `round(value / scale)`，舍入方式为 `round-half-away-from-zero`
2. clamp 到量化整数范围
3. `q * scale` 反量化
4. float32/int dtype 转换使用 IEEE/就近偶数舍入

响应的 `rounding_steps` 给出每节点/每阶段舍入方式、单步误差、最大误差、累计绝对误差和 Kahan 补偿值。

## 7. 灰度稳定裁决

内部定点精度为 `1,000,000`。权重先通过 Kahan 补偿求和归一化，乘以 Scale 后向下取整，再按最大余数法分配剩余单位，保证总和恰好为 `1,000,000`；`raw_fixed` 另保留 `round-half-away-from-zero` 的原始四舍五入值。响应包含每个目标的舍入误差、最大误差和误差补偿。

裁决桶：

```text
key    = grayrouter-gray-v1|snapshot=<v>|route=<r>|salt=<s>|request=<id>
digest = SHA256(key)
bucket = first_8_bytes_big_endian(digest) mod 1,000,000
```

同一 request id + snapshot version + route/salt 逐位一致。请求在绑定阶段记录 model key 和 snapshot；即使 head 已推进，`CompleteBound` 仍读取旧 model 图并回传旧版本标识。

## 8. SQLite 意图与强杀恢复

主要表：`head_state`、`intents`、`idempotent_requests`、`snapshots`、`model_versions`、`current_models`、`migration_plans`、`derived_artifacts`、`routing_entries`、`prepost_cache`、`inflight_bindings`、`gray_plans`。

提交流程：

1. 校验全部图，非法则不写库。
2. 插入并提交 `prepared` 意图。
3. 一个事务写入 `staging` 快照、模型、迁移计划和派生产物。
4. 最终事务原子提升：head、current pointer、产物 `ready`、意图 `completed`、幂等响应。

重启时 `Recover` 扫描 `prepared/running` 意图和孤儿 staging/bound 行：

- 提交类： staging 快照和产物标记 `aborted`，head 保持最后完整版本。
- 推理类：未完成 binding 标记 `aborted`。
- 报告只列数据库中扫描到的意图、绑定、快照和影响行数。

`go run ./cmd/verify` 会启动真实子进程，分别在 `after_stage` 和 `after_binding` 检查点后调用 `Process.Kill`（Unix 为 SIGKILL，Windows 为 TerminateProcess），再重开 SQLite 执行恢复并断言。

## 9. HTTP API

| Method/Path | 用途 |
|---|---|
| `GET /` | 交互式诊断工作台 |
| `GET /api/health` | 健康检查 |
| `GET /api/state` | head、模型、表计数、推理执行计数 |
| `GET /api/samples/bundle` | 固定样本包 |
| `POST /api/samples/write` | 写入 `samples/` |
| `POST /api/samples/load-baseline` | 提交样本基线 |
| `POST /api/samples/load-delta` | 提交样本增量 v2 |
| `POST /api/graphs/validate` | 校验图并返回签名 |
| `POST /api/snapshots` | 整份快照提交/整体切换 |
| `POST /api/deltas` | 增量提交 |
| `GET /api/migration?version=2` | 差异和迁移/重建清单 |
| `GET /api/artifacts?model_key=` | 四类派生产物状态 |
| `POST /api/infer` | 绑定并推理；同 request id 幂等 |
| `POST /api/gray/decide` | 灰度定点裁决 |
| `POST /api/trail/decisions` | 固定请求轨迹裁决 |
| `GET /api/recovery?scan=1` / `POST /api/recovery/run` | 恢复报告 |

失败响应统一为 `{ "ok": false, "error": { "code", "message", "context" } }`。

## 10. 实际验证入口

```bash
go run ./cmd/verify
go test ./...
go test -race ./...
GOOS=windows GOARCH=amd64 CGO_ENABLED=0 go build ./...
bash scripts/check-sqlite.sh artifacts/verify-work/main.db
```

最新机器生成报告：`artifacts/verification-report.md` / `.json`。根目录 `VERIFICATION.md` 保存本次交付时的实际命令摘要。

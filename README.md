# AEGIS Recovery Coordinator

面向多组件平台的本地故障恢复协调引擎。系统从组件依赖图推导故障传播影响面、恢复批次和跨组件健康一致度，并以 SQLite 意图日志、单写者版本提交和原子派生重建保证增量更新、进程强杀与重放的一致性。

交付包含：

- Go HTTP 服务与控制台：`cmd/server`、`internal/httpapi`、`internal/webui`
- 本地真实探测/恢复端点：`cmd/testendpoint`
- 跨平台端到端演示器：`cmd/demo`
- Windows 一键演示脚本：`scripts/demo.ps1`
- 固定确定性样本：`samples/`
- 单元、并发、崩溃恢复和 HTTP 契约测试：各包 `*_test.go`
- 架构与实测记录：`docs/ARCHITECTURE.md`、`docs/VERIFICATION.md`

## 环境要求

- Windows 10/11 或同等本地环境，Go 1.22 或更高版本
- `go` 在 `PATH` 中；无需安装 SQLite C 库
- 仅使用回环网络：`127.0.0.1:19090` 为测试端点，`127.0.0.1:19180/19181` 为协调器
- 临时目录可写。默认运行数据位于 `.tmp/`，运行数据库位于 `data/recovery.db`
- 不使用 Docker 或其他容器

固定样本中的探测 URL 使用 `127.0.0.1:19090`，执行恢复演练时必须先启动 `cmd/testendpoint`。

## Windows 快速开始

在仓库根目录执行：

```powershell
go version
go mod download
go test ./...
.\scripts\demo.ps1
```

`scripts/demo.ps1` 会自动构建并启动测试端点与协调器，完成全部验收场景，然后在 `.tmp\demo\<run-id>\evidence.json` 写入机器可读证据，并为每个子进程保留日志。默认使用新的运行目录，因此可以重复执行。

只启动应用：

```powershell
# 终端 1：真实健康探测/恢复测试端点
go run .\cmd\testendpoint -addr 127.0.0.1:19090

# 终端 2：协调器与控制台
go run .\cmd\server -addr 127.0.0.1:19180 -db .\data\recovery.db -test-hooks=true
```

浏览器打开 `http://127.0.0.1:19180/`。空库时点击“载入固定样本”，或调用：

```powershell
Invoke-RestMethod -Method Post -Uri http://127.0.0.1:19180/api/v1/samples/load `
  -ContentType 'application/json' `
  -Body '{"name":"full-snapshot.json","idempotency_key":"bootstrap-001"}'
```

`-test-hooks=true` 仅用于演示/测试强杀窗口，会启用请求头 `X-Test-Hold-After-Intent-Ms`。生产式本地运行时可以省略。

## 演示覆盖

`cmd/demo` 使用固定样本和真实回环 HTTP 请求验证：

| 场景 | 断言 |
|---|---|
| 全量提交 | v1 快照入库，9 个组件、8 条依赖边、固定校验摘要 |
| 非法规范 | 负数、溢出、缺失依赖均返回 `E_SPEC_INVALID`，版本不变 |
| 影响面/批次 | `gateway` 影响 4 个组件、最大 3 跳，批次严格按依赖顺序 |
| 依赖环 | 返回 `E_DEPENDENCY_CYCLE` 和部分推导结果，版本不变 |
| 局部增量 | v1→v2，报告失效与重建的派生视图集合 |
| 级联重建 | 同一事务内全量重建 66 个派生视图，失效 13 个视图 |
| 陈旧基线/并发 | 两个基于 v2 的并发增量一成功一 `E_STALE_BASE` |
| 真实超时 | 端点实际收到上下文取消；区分超时、连接拒绝、业务拒绝 |
| 资源释放 | 演练后活动预留为 0，已释放预留为 4 |
| 强杀恢复 | 意图落盘后强杀；重启保持最后完整版本，未完成意图标记中止 |
| 精确重放 | 恢复后重试与未中断基线逐项比较版本、摘要、一致度与级联集合 |
| 整份替换 | v4→v5 整快照替换，组件/边与派生结构整体切换 |

实测输出见 `docs/VERIFICATION.md`。

## 数据与规范

固定样本位于 `samples/`：

- `full-snapshot.json`：9 组件完整快照，覆盖正常、零内存边界和非法探测模式
- `components.json`、`edges.json`：组件与依赖边清单
- `increments/resource-adjust.json`：单组件资源调整
- `increments/probe-adjust.json`：探测/恢复参数调整
- `increments/edge-add.json`：依赖边新增
- `increments/cycle-add.json`：闭合依赖环
- `increments/stale-base.json`：陈旧版本基线
- `boundary-weight-snapshot.json`：资源为零、健康/资源权重为 0/1000 的合法边界
- `invalid-*.json`：负数、溢出、缺失依赖、权重和不为 1000
- `drill-request.json`：正常、拒绝、超时混合演练

快照校验规则：

- `version >= 1`，`generated_at` 为 RFC3339
- 组件 ID 唯一；同批重复 ID 按输入顺序保留首条并记录 `first_index`/`duplicate_index`
- CPU 毫核、内存字节为非负 `int64`，解码和执行均做溢出检查
- 依赖引用必须存在；边会在规范化后写回组件的 `depends_on`
- 权重、探测类型、URL、超时、观测状态和一致度配置均严格校验
- 校验摘要为规范化内容（不含 `checksum_sha256` 字段本身）的 SHA-256
- 任何解析或引用错误返回 `E_SPEC_INVALID`，整批不入库

## HTTP API

成功响应统一为：

```json
{"ok": true, "data": {}}
```

失败响应统一为：

```json
{
  "ok": false,
  "error": {"code": "E_STALE_BASE", "message": "...", "context": {}},
  "partial_result": {}
}
```

`partial_result` 仅用于依赖环等可保留部分推导结果的失败。

| 方法 | 路径 | 说明 |
|---|---|---|
| GET | `/api/v1/healthz` | 健康状态、当前版本、派生摘要 |
| GET | `/api/v1/meta` | API/业务码/定点参数 |
| GET | `/api/v1/state` | 当前完整提交版本与不可变派生视图 |
| GET | `/api/v1/versions?limit=50` | 版本账本 |
| GET | `/api/v1/impact/{component}` | 单组件影响面与恢复批次 |
| GET | `/api/v1/impact?component=a,b` | 多组件影响面 |
| GET | `/api/v1/consistency?repeat=3` | 定点一致度重算与摘要复核 |
| POST | `/api/v1/snapshots` | 整份快照提交/替换 |
| POST | `/api/v1/increments` | 带 `base_version` 的幂等增量提交 |
| POST | `/api/v1/drills` | 真实 HTTP 探测/恢复演练 |
| GET | `/api/v1/drills`、`/api/v1/drills/{id}` | 演练记录 |
| GET | `/api/v1/intents?state=` | 意图日志 |
| GET | `/api/v1/recovery-reports` | 启动恢复报告 |
| GET | `/api/v1/lineage?version=` | 派生引用关系 |
| GET | `/api/v1/lineage/rebuilds?version=` | 某版本的级联失效/重建清单 |
| GET | `/api/v1/runtime/reservations` | 活动预留与累计释放数 |
| GET | `/api/v1/samples`、`/api/v1/samples/raw` | 固定样本目录/原文 |
| POST | `/api/v1/samples/validate` | 按样本类型做真实解析/增量/图校验 |
| POST | `/api/v1/samples/load` | 将完整快照样本提交为版本 |

`POST /api/v1/snapshots` 支持两种形式：

1. `{"idempotency_key":"...","snapshot":{...}}`
2. 请求体直接为快照，ID 放在 `Idempotency-Key` 请求头

业务码：

`E_SPEC_INVALID`、`E_DEPENDENCY_CYCLE`、`E_STALE_BASE`、`E_STALE_VERSION`、`E_CONFLICTING_REQUEST`、`E_VERSION_MISMATCH`、`E_PROBE_TIMEOUT`、`E_RECOVER_TIMEOUT`、`E_PROBE_CONNECTION_REFUSED`、`E_RECOVER_CONNECTION_REFUSED`、`E_PROBE_REJECTED`、`E_RECOVER_REJECTED`、`E_PROCESS_INTERRUPTED`。

## 定点数值契约

一致度全流程不使用浮点数：

- 定点比例为 `1_000_000`；所有结果以微单位整数输出
- 组件健康分：`healthy=1_000_000`、`degraded=500_000`、`unhealthy=0`、未知 `250_000`
- 资源余量取 CPU 与内存余量比例的较小值，并截断为微单位
- 组件本地精确分子：`probe_micro * health_weight + resource_micro * resource_weight`
- 聚合使用补偿求和，保留每项精确分子、舍入余数、补偿量、累计误差
- 所有除法舍入方式为 `half_even`
- 阈值比较：先精确比较；未直接达标时计算
  `tolerance_micro = round_half_even(threshold_micro * relative_tolerance_ppm / 1_000_000)`，
  在 `score_micro + tolerance_micro >= threshold_micro` 时标记为 `met_within_tolerance`
- 阈值附近的并列组件按组件 ID 字典序稳定输出
- 同一版本重复计算结果和 SHA-256 摘要逐位一致

## 恢复与持久化语义

- 所有写操作在 `writeMu` 下串行；读者只切换到完整不可变状态指针
- 每次写先落 `intents`，再在一个 SQLite 事务内写快照、组件、边、派生结构、引用、重建清单、幂等回执和 `meta.current_version`
- 增量成功后执行全量派生重建，但用旧引用闭包计算失效集合；读者不会看到部分派生结构更新
- 相同幂等键且内容相同：只执行一次，后到请求复用已落盘响应；内容不同返回 `E_CONFLICTING_REQUEST`
- 依赖环候选不提交；响应仍包含未污染组件批次、环内和下游受阻诊断
- 强制终止后启动扫描 `pending` 意图与提交：原子事务保留最后完整版本，未完成意图标记 `aborted/recovered_on_startup`
- 恢复报告只引用 SQLite 中已落盘意图和已提交快照
- 恢复演练的资源预留与结果同事务发布，预留通过 `defer` 释放；查询不暴露“恢复成功但预留未释放”的中间状态
- 单次探测/恢复使用独立超时和禁用连接复用的 Transport；超时会取消请求上下文并关闭底层连接，返回不同业务码

详细设计见 `docs/ARCHITECTURE.md`。

## 验证命令

```powershell
gofmt -w .\cmd .\internal .\samples
go vet ./...
go test ./...
go test -race ./...
go build ./...
.\scripts\demo.ps1
```

本次交付的实际版本、测试结论、演示数值和证据路径记录在 `docs/VERIFICATION.md`。

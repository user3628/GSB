# DriftLoom

面向 Kubernetes 风格集群的声明式配置漂移收敛裁决器。项目从零实现，使用 Go 1.22+ 标准库与纯 Go SQLite 驱动（`modernc.org/sqlite`），不依赖 Docker、CGO 或外部服务。

核心能力：

- 严格解析并校验集群对象、依赖边与带版本号的策略快照。
- 判定 `field_missing`、`field_extra`、`value_offset`、`source_conflict` 四类漂移，来源冲突自动提升为首要漂移。
- 浮点按十进制有理数和相对 `ppm` 容差比较，不使用裸 `float64` 判定。
- 精确逐对象路径与类别分桶路径并行核算，逐对象及批次 SHA-256 双重核对；出现分歧返回 `E_PATH_DIVERGENCE`。
- 基于依赖 SCC/分层生成稳定批次，支持稳定拆分、环检测、无漂移对象排除。
- 写回计划全程使用整数微秒与 1e-6 定点比例，输出每步舍入方式与累计误差。
- 单轮预算、最大并行批数、有界写回队列、确定性跳过清单。
- 向本地回环端点发送真实 HTTP 写回，再重新读取实际字段并用同一漂移内核复核收敛。
- SQLite 持久化快照、漂移、批次、写回记录与意图日志；启动时自动扫描未完成意图，支持真实进程中断恢复。
- 同一轮次 ID 单飞执行，其他调用等待并复用结果。
- 内置 Web 控制台、命令行演示、故障注入和可重复验收命令。

## 目录

```text
cmd/driftctl/                  CLI 与演示/验收入口
internal/model/                样本模型、归一化、校验、策略摘要
internal/detect/               四类漂移、精确路径、类别桶路径、一致性核对
internal/batch/                Tarjan 环检测、依赖分层、稳定批次、稳定签名
internal/fixed/                整数/有理数、定点舍入、相对容差比较
internal/plan/                 写回计划、代价、预算占用、舍入审计
internal/engine/               调度、HTTP 写回、读回复核、轮次单飞
internal/store/                SQLite 模式、两阶段意图、恢复
internal/simulator/            本地回环写回/读回端点与异常模式
internal/web/                  嵌入式交互式控制台与 JSON API
samples/                       固定确定性样本
scripts/                       Windows PowerShell / POSIX 构建脚本
artifacts/                     本次实际运行产生的 JSON 与日志
docs/                          设计与验证说明
```

## Windows 本地构建

前提：Windows 10/11，已安装 Go 1.22+，并按照任务要求预先准备本地模块缓存。项目 `go.mod` 固定 `modernc.org/sqlite v1.34.5`，`go.sum` 已完整落盘。

PowerShell：

```powershell
go version
cd <项目目录>
$env:GOPROXY = "off"
$env:GOSUMDB = "off"

go test ./...
go build -trimpath -ldflags "-s -w" -o bin\driftctl.exe .\cmd\driftctl
```

也可运行：

```powershell
.\scripts\build.ps1
```

本工作区已实际验证：

- `GOPROXY=off go test ./...` 通过。
- `GOOS=windows GOARCH=amd64 go build` 通过，生成纯 Go Windows 可执行文件。
- 当前执行环境为 Linux，Windows 交叉编译成功；Windows 实机运行命令与 Linux 一致。

## 快速演示

一条命令完成：加载样本、提交快照、双路径判定、生成批次与计划、启动本地模拟端点、逐对象写回与读回复核、同 ID 幂等重放、未监听端口拒绝连接演示。

```powershell
.\bin\driftctl.exe demo --out artifacts\demo
```

或：

```powershell
go run .\cmd\driftctl demo --out artifacts\demo
```

交互式控制台：

```powershell
.\bin\driftctl.exe web --addr 127.0.0.1:8787
# 浏览器打开 http://127.0.0.1:8787
```

控制台提供：路径一致性指标、四类分桶、逐对象漂移、依赖批次、定点成本、执行轮次、写回/复核结果、恢复扫描和原始 JSON。

## 样本

| 文件 | 用途 |
|---|---|
| `samples/cluster-objects.json` | 10 个对象、3 个无漂移对象、四类漂移、稀疏依赖与浮点边界 |
| `samples/policy-snapshot.json` | 版本 7 策略、SHA-256 摘要、精确阈值、批次上限、预算、并行与队列参数 |
| `samples/drift-field-values.json` | 字符串/整数/浮点严格相等与相对容差边界用例 |
| `samples/simulator-config.json` | 默认回环端点 `127.0.0.1:19090` 与正常模式 |
| `samples/cluster-cycle.json` | `cycle/a -> b -> c -> a` 依赖环及环外 DAG |

字段值使用显式类型：

```json
{
  "replicas": {"type": "int", "value": "3"},
  "ratio": {"type": "float", "value": "0.75"},
  "image": {"type": "string", "value": "registry.local/web:v2"}
}
```

来源标记：

```json
{"expected": "gitops", "actual": "kubectl"}
```

两者不一致且字段同时存在时产生 `source_conflict`。同一对象可以同时命中多类漂移，类别全部输出，`source_conflict` 始终排在最前。

策略摘要由规范化结构计算。修改策略后必须重算：

```powershell
.\bin\driftctl.exe checksum --policy samples\policy-snapshot.json --write
```

## 常用命令

```text
driftctl validate   校验样本并运行精确/分桶两路判定
driftctl analyze    校验并持久化快照、漂移、批次与计划
driftctl run        提交流程后向指定本地端点执行一轮写回
driftctl simulate   启动本地确定性写回/读回端点
driftctl recover    扫描未完成意图并输出恢复报告
driftctl demo       正常收敛 + 幂等重放 + 端口拒绝演示
driftctl verify     9 组端到端验收场景
driftctl stress     大规模确定性 DAG 压力演示
driftctl web        启动交互式控制台
driftctl checksum   计算/写回策略摘要
```

每个命令支持 `--help`。主要命令支持 `--json` 或输出结构化报告。

### 分离启动模拟端点和执行端

终端 A：

```powershell
go run .\cmd\driftctl simulate --addr 127.0.0.1:19090 --mode normal --config ""
```

终端 B（每次都使用新的数据库路径，重复提交相同版本会按契约返回 `E_STALE_SNAPSHOT`）：

```powershell
go run .\cmd\driftctl run --db artifacts\run-1.db --endpoint http://127.0.0.1:19090 --round-id round-7
```

模拟器只接受回环地址和回环客户端：`127.0.0.1`、`::1`、`localhost`。

## 异常注入（不改源码）

### 复核未消除

```powershell
# 终端 A
go run .\cmd\driftctl simulate --addr 127.0.0.1:19090 --mode unresolved

# 终端 B
go run .\cmd\driftctl run --db artifacts\unresolved.db --endpoint http://127.0.0.1:19090 --round-id unresolved-7
```

HTTP 写回返回成功，但实际字段不变；读回后仍检测到漂移，对象计入 `unresolvedObjects`，保留残留类别，轮次返回 `E_CONVERGENCE_INCOMPLETE`。

### 写回超时

```powershell
# 终端 A：延迟 900ms；样本策略 writeTimeoutMs=500ms
go run .\cmd\driftctl simulate --addr 127.0.0.1:19091 --mode timeout --write-delay-ms 900

# 终端 B
go run .\cmd\driftctl run --db artifacts\timeout.db --endpoint http://127.0.0.1:19091 --round-id timeout-7
```

每次请求独立 `context` 超时；超时后取消请求、关闭底层连接、禁用 keep-alive 并释放批次并行名额，返回 `E_WRITE_TIMEOUT`。

### 连接拒绝

使用未监听端口：

```powershell
go run .\cmd\driftctl run --db artifacts\refused.db --endpoint http://127.0.0.1:19099 --round-id refused-7
```

返回 `E_ENDPOINT_REFUSED`，并逐对象记录失败写回意图与记录。

### 队列满、预算跳过、中断恢复

`driftctl verify` 会在隔离数据库中自动构造这些条件，包括：

- `maxParallelBatches=1`、`writeQueueCapacity=0` -> `E_WRITE_QUEUE_FULL`；
- `roundTimeBudgetMs=1` -> 确定性预算跳过清单与 `E_CONVERGENCE_INCOMPLETE`；
- 子进程写入待提交意图后以退出码 91 强制终止，再重启扫描 -> 写回回滚为 `pending_writeback`、批次标记 `aborted`、运行中轮次标记 `aborted`。

## 算法与数值契约

### 漂移

字段集合按稳定顺序扫描：

- 期望有、实际无：`field_missing`。
- 实际有、期望无：`field_extra`。
- 两侧都有但类型/值不等：`value_offset`。
- 两侧来源标记不等：`source_conflict`。

整数先做 int64 规范化和严格相等；字符串严格相等；浮点使用 `math/big.Rat` 比较：

```text
abs(expected - actual) <= max(abs(expected), abs(actual)) * relativePpm / 1,000,000
```

每条结论包含字段名、双方值、容差、允许差、实际差、来源和原因。重复判定不考虑耗时字段，结论逐字段稳定。

### 自适应双路径

- 对象数 `<= exactObjectLimit`：主账本使用逐对象精确路径，输出每个对象耗时与依据。
- 对象数 `> exactObjectLimit`：主账本使用类别成员集合分桶，输出分桶方式、桶内数量与抽样对象。

因为验收要求两路径必须逐对象一致，实现会同时运行精确影子路径并对全部对象、全部字段分类、类别列表、首要漂移进行核对。随后分别用两路结论组织批次并比较 SHA-256 签名；报告直接输出 `exactBatchSignature`、`bucketedBatchSignature` 与 `batchPathReconciled`。任一对象或批次划分不同即返回 `E_PATH_DIVERGENCE` 并将整批标记失败。大样本下两路径都是 O(对象数 × 字段并集)，确定性排序保证可重现。

### 批次

1. Tarjan SCC 找出所有依赖环；有环返回 `E_DEPENDENCY_CYCLE` 并附环上对象。
2. 环外无漂移对象不计入批次，但计入 `cleanObjects`。
3. 对无环依赖图求最长依赖深度，依赖对象层级严格小于依赖者。
4. 每层内按对象 ID 稳定排序，按 `batchSizeLimit` 切块。
5. 重新计算批次间依赖索引；批次索引严格递增，且通过 SHA-256 `stableSignature` 可重建。

### 定点写回代价

所有金额单位是整数微秒（`1ms = 1,000,000us`），定点比例保留 6 位：

```text
batch_cost = 750,000us
           + Σ(20,000us)
           + Σ(10,000us × affected_drift_fields)
           + Σ(round_half_away_from_zero(resource_budget × 3,000 / 1,000,000))
```

`3,000ppm = 0.3%`。每个资源预算加权步骤输出分子、分母、舍入模式、结果、余数与该步误差；批次和整体输出累计误差。预算占用为 `cost / resourceBudgetTotal`，固定 6 位小数。

批次预算判断使用：

```text
candidate = acceptedEstimatedCost + batchEstimatedCost
allowed   = roundTimeBudget + roundHalfAway(roundTimeBudget × tolerancePpm / 1,000,000)
```

候选值大于 `allowed` 时跳过。处于容差内时接受，并记录双方微秒值、容差、差异和说明；就绪批次按 `severity_rank_desc -> batch_index_asc` 稳定排序。

### 调度与写回

- 只调度依赖已完成的就绪批次。
- 同时运行批次数不超过 `maxParallelBatches`。
- 额外就绪批次进入长度 `writeQueueCapacity` 的有界队列；溢出返回 `E_WRITE_QUEUE_FULL`。
- 预算决定先于执行确定，跳过批次及其依赖者进入稳定跳过清单；已开始批次的结果保留，不回退、不重算。
- 每批内对象写回后，立即 `GET /v1/read` 读取实际字段和来源，重新执行同一漂移判定内核。只有逐字段无漂移才记为 `converged`。
- 每个 HTTP 尝试有独立超时和独立传输连接；禁用 keep-alive，取消后关闭空闲连接。

## SQLite 与恢复

主要表：

```text
policy_snapshots       快照版本、生成时间、摘要、策略载荷
cluster_objects        对象期望/实际/来源事实与预算
drift_results          每对象漂移结论
batch_versions         批次版本状态 staging|complete|aborted
batch_entries          批次与写回计划
write_intents          先落盘的 pending 意图
write_records          每次 HTTP 尝试及终态
intent_log             意图事件审计日志
rounds                 轮次状态与可复用结果
```

两阶段规则：

1. 先提交 `pending` 意图。
2. 再做数据事务。
3. 最后将意图标记 `committed`。

恢复扫描只读取已落盘事实：

- 快照数据存在 -> 意图标记 committed；不存在 -> rolled_back。
- 批次版本 complete -> committed；staging -> 标记 aborted 并回滚意图。
- 写回记录已有稳态 -> committed；仍为 prepared -> 写回记录标记 `pending_writeback`，意图 rolled_back。
- 仍为 running 的轮次 -> `aborted`，错误码 `E_RECOVERED_INTERRUPTION`。
- 报告同时给出最后一个完整批次版本号、恢复动作与观察到的持久化状态。

单写者由进程内互斥锁组织；SQLite 使用 WAL、busy timeout 和事务，多读者可并发查询一致性快照。轮次 ID 首先检查持久化终态，再用进程内单飞表让重复调用等待并复用同一份报告。

`analyze`、`run`、`demo`、`web` 启动时都会先执行同一恢复扫描；`recover` 命令和 Web 控制台按钮可单独重放扫描。这样进程在批次组织或写回中途被终止后，重启不会依赖操作员先记得手工恢复。

## 错误码

| 错误码 | 含义 |
|---|---|
| `E_SPEC_INVALID` | 样本/策略/字段/依赖/预算/摘要非法 |
| `E_STALE_SNAPSHOT` | 提交版本不大于当前版本 |
| `E_DEPENDENCY_CYCLE` | 存在依赖环，附环上对象 |
| `E_PATH_DIVERGENCE` | 精确路径与类别桶路径逐对象不一致 |
| `E_WRITE_QUEUE_FULL` | 有界等待队列已满 |
| `E_WRITE_TIMEOUT` | 独立 HTTP 超时/连接关闭 |
| `E_ENDPOINT_REFUSED` | 本地端点拒绝连接 |
| `E_READBACK_FAILED` | 写回后读回复核失败 |
| `E_CONVERGENCE_INCOMPLETE` | 存在未消除漂移或确定性跳过批次 |
| `E_ROUND_BUDGET_EXHAUSTED` | 实际轮次墙钟预算耗尽 |
| `E_ENDPOINT_INVALID` | 非回环/非法端点 |

## 验证命令

```powershell
go test ./...
go test -race ./...
go vet ./...
go run .\cmd\driftctl verify --out artifacts\verification.json
go run .\cmd\driftctl stress --count 20000 --fanout 6 --exact-limit 5000
```

本次实际结果见 `docs/verification.md` 与 `artifacts/`。摘要：

- 单元/集成测试：全部包通过，`-race` 无数据竞争，`go vet` 无报告。
- 9 组验收场景、33 项检查全部 PASS。
- 10 对象正常演示：7 个漂移对象全部写回并复核收敛；精确/分桶批次签名均为 `sha256:4449c732...f025af0`；定点代价 `3.436037ms`，累计舍入误差 `-0.244000`；同轮次 ID 重放不新增写记录；未监听端口返回 `E_ENDPOINT_REFUSED`。
- 20,000 对象、6 路稠密依赖：双路径核对 20,000 对象一致，三条批次签名均为 `sha256:495173ad...f394477`，选中类别桶路径，25 个批次，累计舍入误差 `-203.826000`。
- Windows amd64 无 CGO 交叉构建成功；产物为 PE32+，约 11.9 MiB。本会话未在 Windows 实机启动该 `.exe`。

## 范围说明

- 模拟器不是通用 Kubernetes API Server；它按本地确定性规则保存对象实际字段，用于可重复验收。
- 所有服务仅绑定回环地址；不包含 TLS、外部认证或授权。禁止把端点指向未授权目标。
- 对同一 SQLite 数据库的写者由单进程互斥锁 + SQLite 事务共同串行化；跨进程并发写交由 SQLite busy timeout 与事务约束。
- 为保证两路径逐对象一致，当前实现对全部对象同时计算精确路径和类别桶路径，因此判定成本约为单路径的 2 倍；这是验收期显式的一致性成本。

更细的契约与状态机见 `docs/design.md`。

# Protocol FuzzLab

面向自定义二进制请求/响应协议的本地确定性模糊测试工作台。项目使用 Go 1.22+ 与纯 Go SQLite 驱动 `modernc.org/sqlite`，不依赖 CGO、Docker、外部 SQLite 服务或未授权网络目标。

工作台覆盖以下闭环：

- 五状态协议解析、校验和裁决与迁移轨迹记录。
- 种子模板、变异算子、路径断言和用例集的 SQLite 增量版本化。
- 删除/修改后的反向依赖传播、失效闭包、循环与缺失引用诊断。
- 仅绑定 `127.0.0.1` 临时端口的确定性协议服务与并发回放。
- 单个 SQLite 事务内的批次快照提交、版本链连续性与回退。
- 并发队列、单会话预算、在途/排队峰值观测。
- 强制终止后的已提交快照复用与未提交批次重放。
- CLI 诊断视图和字节级可重复 JSON 报告。

## 环境与构建

已验证环境：`go version go1.27.4 linux/amd64`。`go.mod` 的语言目标固定为 Go 1.22，SQLite 驱动固定为支持 Go 1.21+ 的 `modernc.org/sqlite v1.34.5`。

```bash
go version
go mod download

# 完全禁用 CGO，产物为静态 Go 二进制
CGO_ENABLED=0 go build -trimpath -o fuzzlab ./cmd/fuzzlab
```

若默认 Go 代理不可达，可显式使用可访问镜像：

```bash
GOPROXY=https://goproxy.cn,direct go mod download
GOPROXY=https://goproxy.cn,direct go build -trimpath -o fuzzlab ./cmd/fuzzlab
```

Windows 本机 PowerShell：

```powershell
go mod download
$env:CGO_ENABLED = "0"
go build -trimpath -o fuzzlab.exe .\cmd\fuzzlab
.\fuzzlab.exe version
```

Linux 交叉编译 Windows 控制台产物：

```bash
GOOS=windows GOARCH=amd64 CGO_ENABLED=0 go build -trimpath -o fuzzlab-windows-amd64.exe ./cmd/fuzzlab
file fuzzlab-windows-amd64.exe
# PE32+ executable for MS Windows, x86-64
```

项目预留：

- `data/`：内置生成器产出的确定性变更集、注入样本、验证 JSON 与本地 SQLite 数据库。
- `report/`：确定性诊断报告。

## 快速演示

以下流程在全新数据库上执行。若复用已有数据库，可指定新的 `--db` 路径。

```bash
# 1. 初始化基线用例集与全部内置样本
./fuzzlab init

# 2. 回放基线并提交批次快照
./fuzzlab replay --batch-size 2 --concurrency 2

# 3. 增量加入新种子、两个算子、用例和路径断言
./fuzzlab apply --file data/change-01-incremental.json
./fuzzlab replay --batch-size 2 --concurrency 2

# 4. 删除 ping 种子，触发算子、用例与断言的传递失效
./fuzzlab apply --file data/change-02-break-dependency.json

# 5. 回放当前版本。case-ping 以 E_DEP_INVALIDATED 终止，
#    READY->READY、READY->CLOSED 覆盖从上一版本丢失。
./fuzzlab replay --batch-size 2 --concurrency 2

# 6. 领域诊断视图
./fuzzlab status
./fuzzlab closure
./fuzzlab matrix
./fuzzlab trace --case case-ping

# 7. 导出确定性 JSON 报告
./fuzzlab report --batch-size 2 --concurrency 2 \
  --out report/diagnostic-report.json

# 8. 五类异常注入加真实强制终止恢复验证
./fuzzlab verify --out data/verification.json
```

报告重复导出时，输入与数据库状态不变则字节完全一致。当前样例可通过以下命令复核：

```bash
./fuzzlab report --batch-size 2 --concurrency 2
sha256sum report/diagnostic-report.json
./fuzzlab report --batch-size 2 --concurrency 2
sha256sum report/diagnostic-report.json
```

## 命令

```text
fuzzlab init    [--db data/fuzzlab.db] [--samples data]
fuzzlab samples [--dir data]
fuzzlab apply   [--db data/fuzzlab.db] --file CHANGE.json
fuzzlab status  [--db data/fuzzlab.db]
fuzzlab closure [--db data/fuzzlab.db] [--version N]
fuzzlab matrix  [--db data/fuzzlab.db] [--version N]
fuzzlab trace   [--db data/fuzzlab.db] [--version N] --case CASE_ID
fuzzlab replay  [--db data/fuzzlab.db] [--version N]
                [--batch-size 16] [--concurrency 4] [--case-budget 1000]
                [--tamper-case ID] [--tamper-step 0] [--no-commit]
                [--crash-after-batches N]
fuzzlab report  [--db data/fuzzlab.db] [--version N]
                [--out report/diagnostic-report.json]
fuzzlab verify  [--inject SCENARIO] [--out data/verification.json]
fuzzlab serve
fuzzlab version
```

`verify --inject` 可重复或逗号分隔，支持：

- `missing-seed`
- `operator-cycle`
- `bad-checksum`
- `snapshot-gap`
- `concurrent-race`
- `forced-restart`

`verify` 默认在操作系统临时目录建立隔离数据库，不污染 `data/fuzzlab.db`。

## 协议定义

### 帧格式

```text
+--------+------------------+------------------+----------+
| type:1 | payload_len:2 BE | payload:N bytes  | crc16:2  |
+--------+------------------+------------------+----------+
```

- 校验字段为 CRC16-CCITT（多项式 `0x1021`，初值 `0xFFFF`），输入为 `type || payload_len BE || payload`。
- 线路长度字段为 `uint16`；实验策略上限为 4096 字节，超出返回 `E_PAYLOAD_OVERFLOW`。
- 解析器在长度越界时排空该有界帧，使服务端能返回错误响应而不是因未读数据触发 TCP reset。
- 服务端响应帧类型为 `0xF1`，负载为确定性 JSON：`{"status":N,"from":"...","to":"..."}`。

### 状态与迁移表

```text
HANDSHAKE --HANDSHAKE--> AUTH
AUTH      --AUTH-------> READY
READY     --DATA-------> TRANSFER
READY     --PING-------> READY
READY     --CLOSE------> CLOSED
TRANSFER  --DATA-------> TRANSFER
TRANSFER  --PING-------> TRANSFER
TRANSFER  --CLOSE------> CLOSED
```

其余状态/报文组合返回 `E_ILLEGAL_TRANSITION`，状态保持不变。校验失败或负载越界同样不改变状态。服务器为每条 TCP 连接创建独立 `Machine`，因此连接间状态完全隔离。回放默认每个用例新建连接，批次并发不会共享协议状态。

## 用例集与依赖模型

版本化用例集包含四类实体：

- `Seed`：报文类型、变长负载、模板说明。
- `MutationOperator`：引用一个种子，并可按 `DependsOn` 引用其他算子。
- `PathAssertion`：引用用例与种子模板，声明应覆盖的 `from -> to` 迁移。
- `TestCase`：有序步骤列表，每步引用种子和零个或多个算子。

反向依赖传播方向：

- `operator -> seed`
- `operator -> operator`（`DependsOn`）
- `case -> seed/operator`
- `assertion -> case/seed`
- 失效节点继续传播到其反向依赖节点

变更集支持 `add`、`modify`、`delete`。新增/修改若引入缺失引用，以 `E_DEP_INVALIDATED` 拒绝；算子依赖成环以 `E_DEP_CYCLE` 拒绝。两类拒绝都不会推进用例集或快照版本。

删除/修改种子或算子时，允许形成可诊断的失效闭包。诊断会持久化到用例集版本中，因此重启或按旧版本查看时不会丢失。失效诊断包含实体、业务码、完整根因链和用例集版本。显式重新修改用例/算子可作为重新验证动作；断言编辑不会单独阻断用例执行。

## SQLite 与版本

数据库启用 WAL、`synchronous=FULL`、5 秒 busy timeout，并通过 `database/sql` 串行化单进程写入。

主要表：

- `caseset_versions`：每版完整用例集 JSON、摘要、父版本和摘要哈希；旧版本整体保留。
- `caseset_head`：当前用例集版本。
- `execution_snapshots`：快照版本、父版本、用例集版本、批次、逻辑批次槽、回放键、内容摘要、整体替换/内容变更标记。
- `batch_commits`：批次完成记录，与快照在同一事务提交。

写入语义：

- 应用变更集：读取当前版本、应用全部操作、检查引用/环、计算闭包、写入下一版本并更新 head，全部在一个事务内完成。
- 提交快照：检查版本链、插入快照和批次完成记录，在一个事务内完成。
- `replay_key` 由用例集版本/摘要、批次、用例 ID、预算和注入参数确定；同键重复回放直接复用已提交快照，不重复推进版本。
- 快照链从版本 1、父版本 0 开始校验。跳号或断链返回 `E_SNAPSHOT_VERSION`，并返回断点前最近连续版本作为回退视图。
- 整体替换识别基于内容身份，而不是版本号：同一逻辑批次的前后用例 ID 集合均非空且完全不交时标记 `whole_replacement=true`。

## 并发与预算

- `--concurrency`：最大并发批次数。
- `--batch-size`：单批用例数。
- `--case-budget`：单会话最多执行的用例数。

超过并发上限的批次进入 FIFO 队列，不会直接拒绝。调度器公开当前在途数、排队数、在途峰值、排队峰值和完成/失败计数；`replay` 会输出这些值。

批次数超过预算时，已执行用例及轨迹仍保留，剩余用例 ID 写入 `unprocessed`，批次业务码为 `E_CASE_BUDGET`。该部分结论会按正常事务提交。

为保证并发情况下的报告确定性：网络回放可并发，快照按稳定批次顺序提交。这样快照版本号与分析排序不受 goroutine 完成先后影响。

## 强制终止与恢复

`--crash-after-batches N` 在第 N 个新快照提交后执行 `os.Exit(99)`，用于模拟任务管理器结束进程或 PowerShell `Stop-Process`。验证流程：

1. 批次 1 已提交快照。
2. 子进程强制退出；批次 2/3 没有提交记录。
3. 重启同一回放命令。
4. 批次 1 按 `replay_key` 复用，不重复写入；批次 2/3 重新执行并依次提交。
5. 最终快照链连续、无重复、无跳号，用例集版本不回退。

SQLite 的提交原子性保证中断中的事务不会留下半条快照或半条批次记录。

## 业务码

| Code | 含义 |
| --- | --- |
| `E_OK` | 成功 |
| `E_ILLEGAL_TRANSITION` | 当前状态不接受该报文类型 |
| `E_BAD_CHECKSUM` | CRC16 校验失败 |
| `E_PAYLOAD_OVERFLOW` | 负载超过 4096 字节策略上限 |
| `E_DEP_INVALIDATED` | 种子/算子/用例/断言依赖缺失或已失效 |
| `E_DEP_CYCLE` | 算子依赖成环 |
| `E_SNAPSHOT_VERSION` | 快照版本链跳号或父链断裂 |
| `E_CASE_BUDGET` | 单会话用例预算耗尽，保留已执行结果 |
| `E_ASSERTION_MISMATCH` | 回放未覆盖期望迁移 |
| `E_TRUNCATED_FRAME` | 帧被截断 |
| `E_CONNECTION` | 本地回环连接失败 |
| `E_CASESET_VERSION` | 请求的用例集版本不存在 |
| `E_BAD_REQUEST` | 参数、变更集或结构校验失败 |

## 确定性报告

`report` 生成 `protocol-fuzzlab.diagnostic-report/v1`，包含：

- 快照版本链及整体替换/内容变更标记。
- 当前用例集版本、摘要、实体计数和失效用例数。
- 每个用例的业务码、最终状态、迁移轨迹、覆盖集合和断言结果。
- 迁移覆盖矩阵、相对上一用例集版本丢失/新增的迁移。
- 受影响用例与失效根因链。
- 稳定排序的代码计数。

确定性约束：

- 时间字段固定为 `FIXED_SAMPLE_2026-01-01T00:00:00Z`。
- 不写入临时监听端口、进程 ID、绝对数据库路径或 goroutine 调度峰值。
- 用例按 `case_id`，业务码计数按 `code`，快照按 `version`，迁移按状态序稳定排序。
- 相同数据库输入和相同 CLI 参数重复运行会复用快照并输出字节一致的 JSON。

## 测试

```bash
go test ./...
go vet ./...
go test -race ./internal/protocol ./internal/graph ./internal/store \
  ./internal/replay ./internal/report ./internal/scheduler \
  ./internal/target ./internal/verify
```

测试覆盖：

- 帧往返、校验篡改、越界排空、五状态裁决与响应。
- 删除/修改失效闭包、缺失引用、算子环、依赖溯源。
- 增量版本、历史保留、拒绝变更不推进版本。
- 并发上限/排队、预算部分结果、幂等快照。
- 快照跳号检测与最近连续版本回退。
- 用例标识整体替换识别。
- 报告覆盖丢失、排序与重复运行字节一致性。
- 五类隔离注入场景。

实际验证记录见 [`docs/VALIDATION.md`](docs/VALIDATION.md)。

## 目录

```text
cmd/fuzzlab/          命令行入口
internal/protocol/    帧解析、CRC16、五状态机
internal/model/       版本化实体、变更集、诊断
internal/graph/       反向依赖、失效闭包、循环、溯源
internal/store/       用例集与快照 SQLite 事务
internal/target/      127.0.0.1 临时端口确定性服务
internal/replay/      报文物化、回放、覆盖、快照编排
internal/scheduler/   并发限制、等待队列、统计
internal/report/      确定性 JSON 报告
internal/workbench/   status/closure/matrix/trace 视图
internal/demo/        内置确定性样本生成器
internal/verify/      异常注入与强制恢复验证
```

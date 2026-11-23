# capver — Windows 进程隔离与能力限制可复现验证组件

## 概述

`capver` 以本地确定性样本描述一组受测进程（启动命令、能力令牌、限制目录、子进程白名单），
在 Windows 上基于**作业对象（Job Object）+ 受限令牌（Restricted Token）**真实创建受限进程，
验证越权操作被内核或组件在真实边界上拒绝，并以 SQLite 维护**带递增版本号的能力策略快照**，
支持五类异常注入与强制终止后的恢复。全部样本、受限令牌与临时工作目录由组件在运行时于
系统临时目录下生成，不依赖外部账号、网络或真实凭据，不使用 Docker 等容器环境。

## 结构

```
cmd/capver/main.go        命令入口（gen-samples / run / replay / recover / probe）
capver/policy.go          能力策略模型、规范化与内容哈希（整体替换语义）
capver/sample.go          内置确定性样本生成器 + 五类异常注入定义
capver/store.go           SQLite 快照版本化存储（单事务提交、版本链校验、恢复）
capver/runner.go          运行器接口、父进程中介协议、判定逻辑（平台无关）
capver/runner_windows.go  Windows 隔离核心：Job Object + 受限令牌 + 显式句柄列表
capver/runner_other.go    非 Windows 平台：明确返回 E_UNSUPPORTED，不做软判定
capver/probe.go           探针子进程（受测进程载体，真实执行探测操作）
capver/probe_windows.go   注册表写 / 继承句柄探测（Windows）
capver/session.go         并发会话资源控制（并发上限、等待队列、超时）
capver/report.go          确定性 JSON 报告（稳定排序、路径规范化）
capver/verifier.go        领域组件编排：策略加载 -> 快照 -> 进程验证 -> 报告
```

## 业务码

| 业务码 | 含义 | 触发 |
|---|---|---|
| `OK` | 判定符合声明 | 基线 |
| `E_CAP_DENIED` | 越权访问/白名单外子进程被拒 | `unauthorized-write`、`child-escape` 注入 |
| `E_CAP_LEAK` | 实际获得未声明能力 | `cap-leak` 注入 |
| `E_HANDLE_ESCAPE` | 句柄继承逃逸尝试（被真实句柄表拦截） | `handle-escape` 注入 |
| `E_SNAPSHOT_VERSION` | 快照版本链跳号 | `snapshot-gap` 注入 |
| `E_PROCESS_TIMEOUT` | 单进程超时，作业对象终止进程树 | `-slow-test` |
| `E_UNSUPPORTED` | 非 Windows 平台无真实隔离 | 非 Windows 运行 |
| `E_POLICY_INVALID` / `E_SESSION_LIMIT` / `E_STORE` | 策略非法 / 会话资源限制 / 存储错误 | — |

## 关键设计

- **真实隔离边界（Windows）**：每个受测进程关联独立 Job Object
  （`JOB_OBJECT_LIMIT_ACTIVE_PROCESS`、`KILL_ON_JOB_CLOSE`、UI 限制全集），
  以 `CreateRestrictedToken(DISABLE_MAX_PRIVILEGE)` 生成的受限令牌通过
  `CreateProcessWithTokenW` 在独立临时工作目录中启动；超时通过
  `TerminateJobObject` 终止整棵进程树。
- **组件主动拦截**：探针对文件读写与子进程创建先经 stdout/stdin 中介协议
  向父进程请求判定，父进程按限制目录与白名单在真实边界上拒绝
  （`DeniedBy=component`）；注册表写与继承句柄由探针直接尝试，记录内核原始错误
  （`DeniedBy=kernel`）。
- **句柄继承逃逸检测**：父进程故意持有指向限制目录之外文件的可继承句柄，
  启动子进程时通过 `PROC_THREAD_ATTRIBUTE_HANDLE_LIST` 仅放行管道句柄，
  子进程使用该句柄值得到 `ERROR_INVALID_HANDLE`（真实句柄表状态，非模拟标志）。
- **快照版本化**：每次策略加载/更新在单事务内提交 `version = max+1` 的新快照并
  整体保留旧快照；读取时校验版本链从 1 连续，跳号返回 `E_SNAPSHOT_VERSION`
  并回退到最近连续版本；内容比较基于规范化 JSON 的 SHA-256（整体替换语义）。
- **强制终止恢复**：恢复仅依赖 SQLite 已提交事务（WAL）；已提交快照不回滚、
  未提交变更整体丢弃；不承诺强制终止后生成退出报告。
- **确定性报告**：报告不含时间戳与随机路径（临时目录映射为 `<SESSION>`、
  `<WORK:proc>`、`<OUTSIDE:proc>`、`<SELF>` 占位符），进程按 `(procId, code)`、
  探针按 `(token, target)`、事件按 `(sessionId, kind, reason)` 稳定排序，
  重复运行且输入不变时字节级一致。

## 依赖说明

- `modernc.org/sqlite v1.34.5`（纯 Go SQLite，go.mod 锁定）：与 `go-sqlite3` 等效，
  但无需 CGO 即可交叉编译 Windows/amd64，保证可复现构建；Windows API 调用经
  `golang.org/x/sys/windows v0.29.0`（syscall，非 CGO）。如需替换为 go-sqlite3，
  仅需更换 `store.go` 的驱动导入并启用 mingw-w64 工具链。

## 使用

```sh
# 环境：Go >= 1.22，Windows/amd64（真实隔离）；go version / go mod tidy
go build -o capver.exe ./cmd/capver

# 1. 生成确定性样本（内置生成器，本地产出）
capver.exe gen-samples -out samples.json

# 2. 一键基线验证（真实隔离 + 快照提交 + 报告导出）
capver.exe run -db capver.db -report report.json

# 3. 五类异常注入（仅以输入参数触发，不改源码）
capver.exe run -db capver.db -inject unauthorized-write   # -> E_CAP_DENIED   (exit 11)
capver.exe run -db capver.db -inject child-escape         # -> E_CAP_DENIED   (exit 11)
capver.exe run -db capver.db -inject handle-escape        # -> E_HANDLE_ESCAPE(exit 13)
capver.exe run -db capver.db -inject cap-leak             # -> E_CAP_LEAK     (exit 12)
capver.exe run -db capver.db -inject snapshot-gap         # -> E_SNAPSHOT_VERSION (exit 14)

# 4. 超时与并发控制
capver.exe run -db capver.db -slow-test -timeout 1s -max-sessions 2 -max-procs 8

# 5. 强制终止后恢复与快照回放
capver.exe recover -db capver.db
capver.exe replay  -db capver.db -version 1
```

宿主安全服务集成（Go 包接口）：

```go
store, _ := capver.OpenStore("capver.db")
v := capver.NewVerifier(store, capver.NewRunner())
report, err := v.Verify(ctx, capver.GenerateSamplePolicy(), capver.VerifyOptions{
    SessionID: "sess-1", Inject: capver.InjectUnauthorizedWrite,
    Limits: capver.DefaultSessionLimits(),
})
data := capver.ExportReport(report) // 确定性 JSON 字节
snap, rerr := v.Replay(1)           // 快照回放
```

## 实际验证结果（记录于 2026-11-24，Linux 开发环境 + Windows 交叉编译）

- `go test ./... -count=1`：**28 个测试全部通过**，覆盖：
  - 快照版本化：递增提交、旧快照整体保留、跳号回退（`E_SNAPSHOT_VERSION`）、
    崩溃后已提交快照不回滚、未提交事务整体丢弃、整体替换式内容比较；
  - 判定逻辑：五类异常到稳定业务码的映射、超时 `E_PROCESS_TIMEOUT`；
  - 探针中介（真实子进程）：越权写被组件拦截且目标文件未生成、
    白名单内子进程真实创建成功/白名单外被拒、声明内文件读写真实落盘；
  - 报告确定性：打乱输入顺序后字节级一致、路径规范化；
  - 会话控制：超并发上限进入等待队列（queued 事件）、单会话进程数上限
    （`E_SESSION_LIMIT`）、等待/终止事件携带会话标识与原因。
- `GOOS=windows GOARCH=amd64 go build ./... && go vet ./...`：通过
  （作业对象、受限令牌、句柄列表、注册表探测均真实编译）。
- CLI 冒烟（Linux，进程隔离返回 `E_UNSUPPORTED`，快照/报告链路完整）：
  - 连续两次 `run` 报告 `cmp` 字节级一致（`REPORT_BYTE_IDENTICAL`）；
  - `-inject snapshot-gap` 退出码 14，`recover` 输出
    `fallback-to-continuous` 且回退快照哈希与基线一致（已提交快照未污染）；
  - `replay -version 1` 正确回放基线快照。
- Windows 目标机上的真实隔离验证（作业对象/受限令牌/句柄逃逸/注册表拒绝）
  需在 Windows/amd64 主机执行上述「使用」小节命令；本组件不以软判定代替
  真实隔离，非 Windows 平台一律返回 `E_UNSUPPORTED`。

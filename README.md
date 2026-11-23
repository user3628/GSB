# Sandbox Policy Diagnostic Workbench

面向 Windows 沙箱的本地权限策略诊断工作台。项目提供分层策略解析、SQLite 版本快照、原子增量应用、依赖失效闭包、真实文件/进程探测、并发资源控制和确定性 JSON 报告。

所有样本均为本地静态 JSON，不访问账号、注册表或线上资源。模块内置纯 Go SQLite 驱动依赖，并已生成 `vendor/`，可在断网环境重复构建。

## 环境与构建

- Go 1.22.0 或更高版本
- Windows 10/11，或用于交叉编译与功能验证的 Linux/WSL
- SQLite 使用 `modernc.org/sqlite`，无需 CGO 或系统 SQLite

Windows PowerShell 直接构建与运行：

```powershell
$env:GOFLAGS = "-mod=vendor"
$env:CGO_ENABLED = "0"
go build -o sandboxctl.exe ./cmd/sandboxctl
go test ./...
.\sandboxctl.exe current --db state.db --namespace prod/win-sandbox
```

Linux/WSL 与交叉编译：

```bash
# 推荐：完全离线、纯 Go、无 CGO
export GOFLAGS=-mod=vendor
export CGO_ENABLED=0
go build -o sandboxctl.exe ./cmd/sandboxctl
go test ./...

# 在 Linux/WSL 上交叉编译 Windows 可执行文件
GOOS=windows GOARCH=amd64 go build -o sandboxctl.exe ./cmd/sandboxctl
```

依赖或 `go.mod` 发生变化时，联网环境执行一次 `go mod tidy && go mod vendor`，之后仍可离线构建。

## 快速验收

```bash
./scripts/verify.sh
```

脚本会离线运行测试、构建 Windows 可执行文件，并在临时目录中执行完整样本链、闭包、溯源、真实探测、稳定报告与异常注入检查。实际结果记录在 [`docs/VERIFICATION.md`](docs/VERIFICATION.md)。

手工流程：

```bash
go build -mod=vendor -o sandboxctl.exe ./cmd/sandboxctl

./sandboxctl.exe init  --db state.db --policy samples/policies/baseline.json
./sandboxctl.exe apply --db state.db --changeset samples/changes/01-add-cache.json
./sandboxctl.exe apply --db state.db --changeset samples/changes/02-delete-directory.json
./sandboxctl.exe apply --db state.db --changeset samples/changes/03-tighten-process.json
./sandboxctl.exe apply --db state.db --changeset samples/changes/04-delete-registry.json

./sandboxctl.exe current  --db state.db
./sandboxctl.exe closure  --db state.db --version 3
./sandboxctl.exe trace    --db state.db --version 3 --rule rule.file.audit.read
./sandboxctl.exe report   --db state.db --version 2 --out report.json
./sandboxctl.exe report   --db state.db --version 2 --out report-again.json
cmp report.json report-again.json
```

## 样本版本链

| 版本 | 输入 | 结果 |
| --- | --- | --- |
| 1 | `samples/policies/baseline.json` | 严格校验通过的基线 |
| 2 | `01-add-cache.json` | 新增缓存目录与读写规则，无失效 |
| 3 | `02-delete-directory.json` | `dir.data` 删除；直接失效 `rule.file.data.read/write`，传递失效 `rule.file.audit.read`、`rule.file.cache.read`，业务码 `E_DEP_INVALIDATED` |
| 4 | `03-tighten-process.json` | `pg.trusted` 移除 `tool`；直接失效 `rule.proc.trusted`，经规则依赖传递至 `rule.proc.audit`，业务码 `E_DEP_RESTRICTED` |
| 5 | `04-delete-registry.json` | `reg.legacy` 删除；`rule.registry.deny` 为 `E_DEP_INVALIDATED` |

历史快照整体保留，可从任一连续版本回放闭包与探测。版本链被破坏时只暴露最近连续前缀。

## 策略模型

```json
{
  "schema_version": 1,
  "namespace": "prod/win-sandbox",
  "directories": [
    {"id": "dir.root", "path": "work\\sandbox", "access": ["read", "write"]},
    {"id": "dir.data", "path": "work\\sandbox\\data", "parent_id": "dir.root", "access": ["read", "write"]}
  ],
  "process_groups": [
    {"id": "pg.root", "executables": ["sandboxctl"]},
    {"id": "pg.trusted", "parent_id": "pg.root", "executables": ["sandboxctl", "tool"]}
  ],
  "registry_paths": [
    {"id": "reg.root", "path": "HKLM\\Software", "access": ["read"]}
  ],
  "rules": [
    {"id": "rule.file.data.read", "effect": "allow", "action": "file.read", "ref": "dir.data"},
    {"id": "rule.proc.trusted", "effect": "allow", "action": "process.start", "ref": "pg.trusted"}
  ]
}
```

上游实体分三层：`directory`、`process_group`、`registry_path`。规则动作必须与引用层匹配：

| 动作 | 上游实体 |
| --- | --- |
| `file.read` / `file.write` | `directory` |
| `process.start` | `process_group` |
| `registry.write` | `registry_path` |

校验包括全局 ID 唯一性、父引用与规则引用可解析、同层父子关系、目录/注册表路径必须位于父路径之下、规则动作层匹配、实体层级与规则依赖无环。引用可写作 `other-ns::id`；引用其他命名空间返回 `E_NAMESPACE_SCOPE`。

规范化固定为：实体按 `id` 排序、规则按 `id` 排序、权限与可执行文件集合排序去重、父/规则依赖去重、效果与动作小写、Windows 路径分隔符统一。摘要为规范化 JSON 的 SHA-256。输入数组顺序变化不会改变规范形或摘要。

## 增量变更集

```json
{
  "schema_version": 1,
  "namespace": "prod/win-sandbox",
  "base_version": 2,
  "operations": [
    {"op": "delete", "kind": "directory", "id": "dir.data"},
    {
      "op": "modify",
      "kind": "process_group",
      "entity": {"id": "pg.trusted", "parent_id": "pg.root", "executables": ["sandboxctl"]}
    },
    {
      "op": "add",
      "kind": "rule",
      "entity": {"id": "rule.new", "effect": "allow", "action": "file.read", "ref": "dir.root"}
    }
  ]
}
```

- `add` / `modify` / `delete` 在一个 SQLite `BEGIN IMMEDIATE` 事务内执行。
- 新版本号只从已提交快照推导，结果为 `base + 1`；旧快照和对应闭包不覆盖。
- 失败会回滚快照与事务内事件，不保留部分写入；冲突/失败事件在回滚后独立记录。
- 同一变更集内重复触碰同一标识（如先删后加）返回 `E_CHANGE_CONFLICT`。
- 新加或修改的引用必须解析；删除上游可让既有规则悬空，悬空规则保留在快照中并进入失效闭包。
- `base_version` 可选；填写时启用乐观并发检查。

## 依赖闭包

对每次删除或收紧：

1. 以变更前的实体树计算被删除/收紧实体及其后代。
2. 反向查找引用这些实体的规则：直接引用为直接失效，后代或规则依赖链为传递失效。
3. 沿规则 `depends_on` 继续反向传播。
4. 目录/注册表删除或后代失效使用 `E_DEP_INVALIDATED`；进程组或其他上游收紧使用 `E_DEP_RESTRICTED`。
5. 闭包与目标快照版本一起持久化，`closure` / `replay` / `report` 可按历史版本读取。

`report` 同时包含 `change_closure`、当前快照 `state_issues` 和按规则 ID 合并排序的 `affected_rules`。

## 真实探测

`report` 默认执行探测，`diagnose --probe` 与 `probe` 也可显式触发。

- 每次探测创建独立临时根目录，所有夹具、文件和子进程副本均位于该目录。
- 不触碰策略声明的真实绝对路径；声明的相对目录映射到隔离根内。
- 文件读写逐规则创建夹具；拒绝规则通过权限位或 Windows deny ACL 施加限制。
- 进程启动使用当前可执行文件副本，`Start` 成功即视为放行，失败视为拒绝；子进程立即回收。
- 探测结束前恢复权限并删除整个临时根目录。
- 探测环境无法实施拒绝（例如以 root 运行 Unix 权限探测）返回 `E_PROBE_FAILED`，不伪造策略结论；允许动作被主机意外拒绝为 `E_PROBE_MISMATCH`。
- Unix 使用 `chmod`；Windows 使用系统自带 `icacls` 为当前用户添加/移除显式 deny ACE，只作用于隔离临时目录。
- 注册表规则不进行真实写入，探测仅覆盖需求规定的文件读写与进程启动。

## 快照一致性与并发

SQLite 表 `snapshots` 以 `(namespace, version)` 为主键，记录 `parent_version`、内容摘要、完整策略 JSON 和诊断 JSON。读取时：

- 从版本 1 开始要求版本连续且 `parent_version = version - 1`。
- 重算完整内容摘要，摘要不符视为同版本内容被替换。
- 跳号、父链断裂、内容摘要不符或诊断与版本不绑定都返回 `E_SNAPSHOT_VERSION`，并仅暴露最近连续前缀。
- 读取当前版本时自动回退；显式读取断裂版本直接拒绝；任何增量应用均不得跨过断裂链。

并发应用由两层控制：

- 每个命名空间一把进程内串行门，避免同命名空间版本交错。
- 全局可配置在途事务信号量（`--max-inflight`），超过 `--queue-timeout` 返回 `E_CHANGE_CONTENTION`。
- `change_events` 记录 `queued`、`contention`、`conflict`、`failed`、`applied` 事件，包括命名空间、版本号、操作与业务码。
- SQLite 单写者、WAL、短 busy timeout 与受控重试共同保证跨连接一致性。

## CLI

```text
sandboxctl init       --db DB --policy FILE
sandboxctl apply      --db DB --changeset FILE [--max-inflight N] [--queue-timeout DURATION]
sandboxctl current    --db DB [--namespace NS]
sandboxctl chain      --db DB [--namespace NS]
sandboxctl snapshot   --db DB [--namespace NS] [--version N]
sandboxctl diagnose   --db DB [--namespace NS] [--version N] [--probe] [--out PATH]
sandboxctl replay     --db DB [--namespace NS] --version N [--probe]
sandboxctl closure    --db DB [--namespace NS] [--version N]
sandboxctl trace      --db DB [--namespace NS] [--version N] --rule RULE_ID
sandboxctl report     --db DB [--namespace NS] [--version N] [--out -|PATH] [--no-probe]
sandboxctl probe      --db DB [--namespace NS] [--version N]
sandboxctl events     --db DB [--namespace NS] [--limit N]
sandboxctl namespaces --db DB
sandboxctl shell      --db DB [--namespace NS]
```

省略 `--namespace` 且本地仅有一个命名空间时自动推断。普通输出为 JSON；错误以稳定的 `{code,message,entry,reason,version}` 结构写入 stderr。

交互 shell 支持 `current`、`chain`、`snapshot [v]`、`closure [v]`、`trace <rule> [v]`、`diagnose [v]`、`probe [v]`、`report <path|-> [v]`、`help` 和 `quit`。

## 业务码与退出码

| 业务码 | 含义 | CLI 退出码 |
| --- | --- | --- |
| `E_POLICY_MALFORMED` | 策略/变更集结构、引用、层级或循环非法 | 2 |
| `E_SNAPSHOT_VERSION` | 版本跳号、父链断裂、同版本内容摘要不符 | 3 |
| `E_PROBE_FAILED` | 探测环境/夹具/权限实施失败 | 4 |
| `E_PROBE_MISMATCH` | 允许边界被真实环境拒绝 | 4 |
| `E_CHANGE_CONTENTION` | 命名空间/在途槽/写锁等待超阈值 | 5 |
| `E_CHANGE_CONFLICT` | 删除后新增、重复新增、缺失修改、过期基线 | 6 |
| `E_NAMESPACE_SCOPE` | 命名空间不匹配或越权引用 | 7 |
| `E_BASELINE_MISSING` | 命名空间尚无已提交基线 | 8 |
| `E_DEP_INVALIDATED` | 上游删除导致失效（诊断结果，不是命令失败） | 0 |
| `E_DEP_RESTRICTED` | 上游收紧导致失效（诊断结果，不是命令失败） | 0 |

## 异常注入

外部输入位于 `samples/invalid/`，测试与 CLI 不修改源码：

| 类型 | 输入 | 期望 |
| --- | --- | --- |
| 引用缺失 | `missing-reference.json` | `E_POLICY_MALFORMED` / `reference_missing` |
| 畸形策略 | `policy-duplicate-id.json` | `E_POLICY_MALFORMED` / `duplicate_identifier` |
| 循环依赖 | `dependency-cycle.json` | `E_POLICY_MALFORMED` / `dependency_cycle` |
| 命名空间越权 | `cross-namespace-reference.json` | `E_NAMESPACE_SCOPE` |
| 删除后新增冲突 | `delete-then-add-conflict.json` | `E_CHANGE_CONFLICT` |
| 版本跳号 | `inject version-gap --version N` | 当前版本回退，后续应用 `E_SNAPSHOT_VERSION` |

另有 `inject digest-mismatch --version N` 用于同版本内容替换检查。注入命令仅用于本地验收，不属于正常策略发布路径。

## 确定性报告

`sandboxctl report --out report.json` 输出：

- `snapshot_chain`：截至目标版本的连续摘要链
- `change_closure`：该版本变更产生的直接/传递失效闭包
- `state_issues`：当前快照仍存在的悬空引用
- `affected_rules`：按规则 ID 合并排序的失效规则
- `probe`：逐规则期望/观测/匹配/业务码与汇总
- `business_codes`：排序去重的业务码

报告不含时间戳、随机临时路径或主机绝对路径；探测仅使用逻辑夹具和规则 ID 表达结论。规则、探测结果、链和业务码均稳定排序，同一状态库与同一版本重复导出字节一致。

## 代码结构

```text
business/   业务码与统一错误
policy/     模型、严格解析、规范化、变更应用、闭包、溯源
snapshot/   SQLite schema、版本链、摘要验证、事件、异常注入
engine/     命名空间串行门、在途信号量、等待阈值与原子提交
probe/      隔离文件/进程探测与 Unix/Windows 权限适配
report/     确定性诊断报告
cli/        子命令与交互 shell
samples/    基线、增量、无效/冲突输入
tests/      五类外部异常注入验收
vendor/     离线构建依赖
```

## 验证限制

当前执行环境为 WSL2，可运行 Unix 权限位真实探测并交叉编译 Windows 二进制，但未启用 Windows 互操作，因此无法在本机启动 `.exe` 验证 Windows `icacls` 路径。Windows 适配代码已用 `GOOS=windows CGO_ENABLED=0` 完整编译；详细执行记录见 [`docs/VERIFICATION.md`](docs/VERIFICATION.md)。


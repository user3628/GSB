# secverify

`secverify` 是一个完全本地、无容器、无管理员的 Windows 主机安全配置变更验证实验引擎。它把账户策略、审计策略、服务启动类型和注册表键权限映射到真实临时文件系统对象，在独立子进程中执行受控探测，并用纯 Go SQLite 保存可回放、带版本号和内容哈希的配置快照。

项目只直接依赖 `modernc.org/sqlite`（纯 Go，无 CGO），其余实现均来自 Go 标准库。样本、探测目录、数据库和报告均在运行时由命令创建，不预置二进制或外部样本文件。

## 环境与构建

```bash
go version                 # 需要 Go 1.22+
go test ./...
go build -o secverify ./cmd/secverify
```

`go.mod` 使用 Go 1.22 语法和 `modernc.org/sqlite v1.35.0`。当前实现已在 Linux/WSL 上完成本地验证，并通过 `GOOS=windows GOARCH=amd64 go build ./...` 交叉编译检查。

## 快速开始

```bash
# 1) 完整演示：4 轮变更、依赖闭包、真实子进程探测、SQLite 快照、确定性报告
./secverify demo --report demo.json

# 2) 五类异常注入：依赖缺失、依赖环、探测越界、版本跳号、并发竞争
./secverify inject --report injections.json

# 3) 强制终止恢复：探测中断 + 提交前中断
./secverify crash-demo --report crash.json

# 4) 使用外部基线和长度前缀变更文件
./secverify run \
  --baseline samples/baseline.json \
  --changeset samples/round-01-tighten-account.changes \
  --report run.json
```

自动创建的临时工作目录默认在命令退出时清理；使用 `--workdir <empty-dir>` 可保留现场，或使用 `--keep`。报告默认写到标准输出，`--report FILE` 写到文件，`--report -` 强制标准输出。

常用资源参数：

```text
--max-tx N           在途变更事务上限（默认 4）
--max-probes N       在途探测子进程上限（默认 2）
--wait-timeout DUR   获取命名空间/事务/探测槽位的等待阈值（默认 2s）
--probe-timeout DUR  单个探测子进程超时（默认 10s）
--probe-delay DUR    持有探测槽位时的固定延迟，用于并发实验
```

`inject` 还支持 `--scenario all|missing-dependency|dependency-cycle|probe-escape|snapshot-version-gap|concurrent-contention`，便于从外部参数单独触发一类异常。

## 目录

```text
cmd/secverify/          命令行入口与 demo/inject/crash/run 编排
internal/model/         配置类型、业务错误、稳定哈希与排序
internal/config/        JSON 基线、长度前缀变更集、规范化与差异
internal/deps/          有向依赖图、最小环、反向失效闭包
internal/probe/         沙箱物化、路径守卫、受限探测子进程协议
internal/snapshot/      SQLite 快照、版本链、整体替换比较
internal/engine/        应用/探测/提交事务主流程、资源门控、恢复
internal/report/        确定性 JSON 报告
internal/sample/        内置确定性样本与异常样本生成器
docs/                   设计与实际验证记录
```

## 基线格式

基线是一个 JSON 对象。每个配置项有唯一 `id`、`kind`、可选 `refs`，并恰好带一个与 `kind` 对应的载荷。依赖语义为“当前项依赖 `refs` 中的项”。

```json
{
  "namespace": "lab-a",
  "items": [
    {
      "id": "acct.base.min_length",
      "kind": "account_policy",
      "account": {
        "name": "minimum_password_length",
        "value": 12,
        "unit": "characters",
        "min": 8,
        "max": 64,
        "objective": "at_least"
      }
    },
    {
      "id": "acct.child.min_length",
      "kind": "account_policy",
      "account": {
        "name": "inherited_password_length",
        "unit": "characters",
        "min": 10,
        "max": 64,
        "inherit_from": "acct.base.min_length",
        "objective": "at_least"
      }
    },
    {
      "id": "audit.logon",
      "kind": "audit_policy",
      "refs": ["acct.child.min_length"],
      "audit": {
        "name": "logon-audit",
        "category": "logon",
        "path": "audit/logon",
        "mode": "0750",
        "enabled": true,
        "retention_value": 30,
        "retention_unit": "days"
      }
    },
    {
      "id": "svc.telemetry",
      "kind": "service_start_type",
      "service": {
        "name": "TelemetrySvc",
        "start_type": "automatic",
        "image_path": "C:\\Lab\\Telemetry.exe"
      }
    },
    {
      "id": "reg.hklm.software.lab",
      "kind": "registry_key_permissions",
      "registry": {
        "key": "HKLM\\SOFTWARE\\Lab",
        "path": "registry/hklm/software/lab",
        "mode": "0750",
        "acl": [
          {"principal": "SYSTEM", "access": "full_control"},
          {"principal": "Users", "access": "read"}
        ]
      }
    }
  ]
}
```

字段约束：

- 项标识必须唯一，引用必须可解析；显式 `refs` 与 `inherit_from` 都会形成依赖边。
- 账户策略单位支持 `seconds/minutes/hours/days` 与 `characters/count/attempts/entries`；时间统一换算为秒，计数统一为 `count`。`objective` 为 `at_least`、`at_most` 或 `exact`。
- 账户策略可省略 `value` 并继承 `inherit_from` 的有效值；自身 `value` 表示继承覆盖。继承值仍会按当前项的 `min/max` 校验。
- 审计保留期支持时间、计数和字节单位；路径为沙箱内相对路径。
- 服务启动类型规范化为 `automatic`、`manual`、`disabled`。
- 注册表权限模式为八进制权限，ACL 访问值为 `read`、`write` 或 `full_control`。

解析器使用严格字段校验。重复 ID、非法取值域、未知字段、错误 JSON 等都返回稳定 `E_CONFIG_PARSE`，并携带 `item_id` 和原始输入字节偏移。解析和候选状态构建不会修改基线，失败时不会发生部分应用。

## 变更集格式

变更集由头部和若干长度前缀文本块组成，每块是完整 JSON 操作：

```text
SECCHANGE/1\n
BLOCK <payload-byte-length>\n
<exactly N bytes of JSON>\n
```

操作示例：

```json
{"op":"add","id":"acct.history.length","item":{...}}
{"op":"modify","id":"acct.base.min_length","item":{...}}
{"op":"delete","id":"svc.defender"}
```

- `add` 目标必须不存在，`modify/delete` 目标必须存在。
- 同一变更集不能对同一 ID 操作多次。
- 修改采用完整项替换语义，报告会给出发生变化的字段。
- 所有块先完整解析和校验，然后一次性构建候选状态；任何失败均不提交快照。

## 依赖、失效闭包与业务码

依赖图边方向为 `dependent -> dependency`。删除或收紧某项时，引擎沿反向边做 BFS，得到直接及传递受影响项集合。每个受影响项都带有 `trigger`、`depth` 和受影响项到触发项的 `chain`；最终边集合和项集合稳定排序。

环检测先做 Tarjan 强连通分量，再在含环分量内用 BFS 选全局最短环，长度相同时按节点序列字典序决胜，因此输出的是最小参与项集合和确定性的边序列。

| 业务码 | 含义 |
|---|---|
| `E_CONFIG_PARSE` | JSON/块格式、重复 ID、字段域或操作语义非法 |
| `E_DEP_MISSING` | 引用目标缺失 |
| `E_DEP_CYCLE` | 依赖或继承形成环 |
| `E_PROBE_ESCAPE` | 绝对路径、`..` 或符号链接导致探测越界 |
| `E_PROBE_MISMATCH` | 探测观测值与预期不一致 |
| `E_PROBE_EXEC` | 探测子进程无法执行或超时 |
| `E_SNAPSHOT_VERSION` | 快照版本链跳号，拒绝继续并返回最近连续版本 |
| `E_SNAPSHOT_CORRUPT` | 快照内容哈希、父子链或载荷身份不一致 |
| `E_CHANGE_CONTENTION` | 命名空间、事务或探测槽位等待超阈值 |
| `E_STORE` | SQLite 或事务提交错误 |

## 真实边界探测

每轮先为命名空间创建独立沙箱（命名空间经哈希隔离），再物化候选状态：

- 账户策略：规范化 JSON 声明文件。
- 审计策略：真实日志目录权限 + 目录内状态声明。
- 服务启动类型：声明文件。
- 注册表键权限：真实目录权限 + ACL 声明文件。

父进程先逐项执行 `SecureJoin` 守卫，拒绝绝对路径、卷切换、`..` 和解析到沙箱外的符号链接；确认全部安全后才写入。随后父进程重新执行自身，通过环境标记进入受限 worker 模式，经 stdin/stdout JSON 协议观察真实文件系统。worker 再次执行路径守卫，工作目录被设为沙箱，环境仅包含最小变量和沙箱临时目录。

每条观测都附带固定同步标记 `SECVERIFY_PROBE_SYNC_V1`、观测方式和真实权限位。默认报告不包含真实时间或绝对路径。

在 Linux/WSL 实验环境中，Windows ACL/启动类型/审计目录使用非管理员可创建的本地等价声明和 POSIX 权限位表达；引擎验证声明未被篡改、权限位实际一致且探测不能越过进程/目录边界。它不会调用管理员级 LSA、SCM 或真实注册表 DACL API。

## 快照、恢复与整体替换

SQLite 表 `snapshots(namespace, version)` 与 `round_reports(namespace, version)` 在同一事务内写入：

- 版本 0 是基线，之后每轮成功探测后递增 1。
- 快照载荷包含命名空间、版本、父内容哈希、变更集哈希、规范项列表和固定采样标记。
- 行内容有 SHA-256 内容哈希；每版还校验父哈希，形成不可静默替换的链。
- 旧版本整体保留，`RawChain`/`History` 可按版本回放。
- 加载发现跳号时返回 `E_SNAPSHOT_VERSION`，报告使用最近连续前缀作为回退版本，且引擎拒绝推进新版本。
- `CompareSnapshots` 同时比较版本、内容哈希和项集合：同版本内容不同必定判定为不同快照，并识别整体替换。

强制终止恢复依赖 SQLite 事务语义。`crash-demo` 会在探测执行中或快照/报告 `INSERT` 后、`COMMIT` 前发送强制终止信号；重启后已提交版本与父链保留，未提交事务被丢弃，版本号不推进，然后可正常继续下一轮。

## 并发控制

- 同一命名空间的变更使用独立命名空间令牌串行化，保证版本链单调。
- 不同命名空间可使用不同沙箱并行变更和探测。
- 全局事务槽位与探测进程槽位分别受 `--max-tx`、`--max-probes` 约束。
- 所有等待都有阈值；超时返回 `E_CHANGE_CONTENTION`，不会无限阻塞。
- 每轮报告包含事务/探测上限、在途数、排队数、峰值、命名空间排队峰值、竞争超时累计和等待阈值。
- SQLite 写事务只在最终提交段内串行化，探测仍可跨命名空间并行。

## 确定性报告

报告包含：

- 每轮配置差异、变更字段、收紧标记和前后哈希；
- 失效触发项、受影响项、边序列及逐项来源链；
- 每个探测的预期/观测值、权限位、观测方式和同步标记；
- 稳定业务码、项标识和来源偏移；
- 快照版本链、父哈希、内容哈希、状态哈希和变更哈希；
- 并发在途/排队统计；
- 注入与强制终止断言。

所有数组按项标识或业务码稳定排序；时间字段只使用固定标记 `T0_FIXED_SAMPLE`。JSON 编码固定缩进、禁用 HTML 转义并保留末尾换行。相同输入重复运行应字节级一致。

实际验证结果与报告哈希见 [docs/VERIFICATION.md](docs/VERIFICATION.md)，实现细节见 [docs/DESIGN.md](docs/DESIGN.md)。

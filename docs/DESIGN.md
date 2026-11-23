# 设计与实现说明

## 1. 目标边界

本引擎不连接真实域控、SCM、审计服务或注册表 DACL，也不要求管理员权限。它把四类 Windows 安全配置映射为真实临时文件系统对象，用独立子进程读取内核可见状态，并通过 SQLite 事务保存可回放快照。真实风险仍包括文件权限、进程边界、路径遍历、符号链接、SQLite 回滚和并发竞争。

## 2. 模块

| 包 | 职责 |
|---|---|
| `internal/model` | 配置项、边、差异、闭包、业务错误、稳定排序/哈希 |
| `internal/config` | 严格 JSON、单位与继承、长度前缀块、候选状态 |
| `internal/deps` | 缺失引用、Tarjan SCC + 最短环、反向闭包 |
| `internal/probe` | 沙箱物化、路径守卫、worker 协议、文件观测 |
| `internal/snapshot` | SQLite schema、内容/父哈希、版本链、替换比较 |
| `internal/engine` | 端到端编排、资源门控、崩溃注入、结果结构 |
| `internal/report` | 排序、结果断言、确定性 JSON |
| `internal/sample` | 运行时样本和五类异常生成器 |

## 3. 解析与规范化

`config.ParseBaseline` 用 `json.RawMessage` 保留每项原始字节，并记录起始 `SourceOffset`。字段校验包括唯一 ID、恰好一个载荷、引用语法、账户范围/单位、审计模式/保留期、服务枚举、注册表模式/ACL。账户时间单位统一换算为秒，计数统一为 `count`；`inherit_from` 在完整图上递归解析，自身 `value` 表示覆盖。

引用存在性和环依赖完整候选状态，因此放在图阶段：语法/字段域错误为 `E_CONFIG_PARSE`，缺失引用为 `E_DEP_MISSING`，环为 `E_DEP_CYCLE`。解析和候选构建均不修改基线。

变更帧格式：

```text
SECCHANGE/1\n
BLOCK <decimal N>\n
<exactly N bytes JSON>\n
```

解析器按字节计数，payload 必须是完整 add/modify/delete 操作。错误偏移使用 payload 原始字节位置。`BuildCandidate` 在克隆 map 上执行，拒绝重复目标、目标不存在和操作语义冲突。

## 4. 依赖与闭包

边方向为 `dependent -> dependency`，`refs` 和 `inherit_from` 都生成边。缺失引用聚合排序后返回涉及项和边。环检测使用 Tarjan SCC，对含环节点做 BFS 最短环，并以节点序列字典序打破平局，因此输出最小参与集合和确定性边序列。

删除使用变更前图，修改/收紧使用候选图。每个触发项沿反向边 BFS，受影响项记录 trigger、depth 和从该项到触发项的边链。同一项只保留最短、字典序最优来源链。收紧规则覆盖账户目标/值方向、审计权限/启用/保留期、服务严格度排序、注册表权限位/ACL 变化。

## 5. 探测边界

每个命名空间使用独立沙箱 `<workdir>/sandbox/<sha256(namespace)[:8]>`。`Materialize` 先清空当前命名空间沙箱，对全部路径预检后再写入，避免部分越界写入。

`SecureJoin` 拒绝空路径、NUL、绝对路径、卷名、`..`、外部符号链接和解析后位于 root 外的路径。父进程预检防止越界写入，worker 再检查一次。

worker 是当前可执行文件的受限重执行模式：工作目录为沙箱，环境只保留最小变量，stdin 请求上限 8 MiB，文件读取上限 1 MiB，父进程有超时并会终止子进程。每条观测带固定 `SECVERIFY_PROBE_SYNC_V1` 和观测方法。

在 Linux/WSL 上，审计目录和注册表键使用真实目录权限位，ACL/服务/账户使用规范化声明文件；Windows 上可编译运行，但 POSIX 权限位表达力有限，实验重点是声明完整性、边界隔离和提交一致性，而非管理员级 LSA/SCM/registry API。

## 6. SQLite 快照

`snapshots(namespace, version)` 和 `round_reports(namespace, version)` 在同一事务内写入。版本 0 是基线，之后成功探测一轮递增 1。payload 包含版本、父哈希、变更哈希、状态哈希、规范项和固定采样标记；行内容另有 SHA-256。

加载按版本升序检查：版本从 0 连续、内容哈希匹配、payload 身份匹配、父哈希匹配。跳号返回 `E_SNAPSHOT_VERSION` 并保留最近连续前缀；内容/父链损坏返回 `E_SNAPSHOT_CORRUPT`。`CompareSnapshots` 同时比较版本、内容哈希和项集合，同版本内容变化必定判定不同并识别整体替换。

Store 对写事务段加互斥，避免两个延迟事务同时升级写锁导致 `SQLITE_BUSY`；探测不受该互斥影响。WAL + busy timeout + 完整同步用于恢复。

## 7. 并发资源

资源管理器维护每命名空间容量 1 的令牌、全局事务槽、全局探测槽以及原子在途/排队/峰值计数。槽位先非阻塞获取，只有真正等待才增加队列。超时返回 `E_CHANGE_CONTENTION`。

应用顺序是命名空间令牌 -> 事务槽 -> 快照加载/解析/闭包 -> 沙箱物化 -> 探测槽/worker -> 比较 -> 单事务提交。因此同命名空间严格串行，不同命名空间可并行。

## 8. 强制终止恢复

`crash-demo` 先提交版本 1，再启动隐藏的 `__apply-crash` 子进程。`probe` 崩溃点在探测前阻塞；`before-commit` 崩溃点在快照和报告 INSERT 后、COMMIT 前阻塞。父进程收到 ready 文件后强制终止子进程。重启后已提交版本和内容哈希保留，未提交 INSERT 不可见，版本号不推进，随后可正常提交版本 2。

## 9. 确定性

- 样本由固定结构体和 JSON 编码生成；
- 项、边、差异、探测、检查和版本链均显式排序；
- 快照/报告不含真实时间、PID、绝对路径或随机值；
- 报告固定 `T0_FIXED_SAMPLE`、固定缩进、禁用 HTML 转义、末尾换行；
- 并发实验通过显式 gate 和固定等待阈值控制。

这些约束使相同输入重复运行产生字节级一致报告。

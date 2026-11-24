# 本地提权检测领域可视化与诊断工作台 — 技术说明

## 1. 范围与约束

- 纯本地、确定性运行：样本由内置生成器产出，不访问网络、账号或外部目标。
- 整数路径：全部评分以 `scale = 1e-6` 的 `int64` 定点数计算，代码中不出现浮点类型。
- 纯 Go SQLite 驱动 `modernc.org/sqlite`，无 CGO 依赖，可直接交叉编译到 Windows。
- 时间字段只输出固定采样标记 `T0+SAMPLE_MARK`；耗时仅以单调时钟 `time.Since` 差值采样。

## 2. 分层结构

```
cmd/privesc            子命令入口（generate/detect/trace/replay/ledger/report/inject）
internal/domain        稳定业务码、内部模型、虚拟根、固定采样标记
internal/fixedpoint    定点乘加、纳秒换算、截断误差账本（草稿提交语义）
internal/sample        JSONL 清单与长度前缀事件流的解析 + 确定性生成器/注入样本
internal/scoring       权重表、时间衰减、区间裁决（提权/不确定/正常）
internal/graph         父子边、句柄边、虚拟根挂载、顺序无关溯源
internal/snapshot      SQLite 版本化快照、版本链校验、整批替换识别、进度标记
internal/session       并发度信号量、等待队列、进程/事件预算、会话隔离
internal/detector      单轮检测流水线编排
internal/inject        五类异常注入构造与业务码断言
internal/report        稳定排序的确定性 JSON 报告
internal/recovery      强制终止恢复一致性集成测试
```

## 3. 定点换算与误差记账

- 存储单位为微（1 单位 = 1e-6）。权限位掩码第 `i` 位（0 起）权重为 `(i+1)` 个整微；
  路径可写性分级与账户等级均直接以整微常量给出，避免引入小数换算误差。
- 定点乘法 `Mul(a,b)` 返回商（微）与截断余数。余数以更细的 `1e-12` 账本单位记账，
  恒等式 `q*Scale + rem == a*b` 由测试固定。
- 纳秒偏移 `NanosToMicro` 丢弃的子微余数按 `1ns = 1000` 账本单位记账。
- 误差账本为草稿对象：单轮内所有 `Record` 先缓冲，任一步溢出立即返回
  `E_SCORE_OVERFLOW` 且丢弃草稿，已提交账本不会被部分写入污染；`Commit` 时把
  账本余数向上取整折叠为微级有界误差上界。
- 评分区间：`[score-bound, score+bound]`。下界严格高于阈值判 `E_PRIV_ESCALATION`；
  区间跨越阈值判 `E_INDETERMINATE_RANGE`；否则 `E_OK`。

## 4. 样本格式与校验

- 清单为 JSON 行，每行一条进程/服务声明，解析时记录该行起始字节偏移。
- 事件流为长度前缀文本块：`LEN <n>\n` + 恰好 `n` 字节 JSON + `\n`。
- 校验项：PID 唯一且为正、账户必须在五级层级内、服务写权 0..3、
  继承/事件掩码不超过 `0xFFFF`、事件序号唯一、时间偏移非负。
- 任一非法输入返回 `E_SAMPLE_PARSE`，错误体携带源字节偏移。

## 5. 溯源图

- 声明父子边、声明/事件推断的句柄边统一进集合去重后规范化排序，
  因此进程与事件的归并顺序不影响结论（有专门的乱序对照测试）。
- 父进程或句柄提供方缺失的进程标记 `E_ORPHAN_REF` 并挂到虚拟根 PID `-1`。
- 溯源优先沿句柄边（越权写归因到提供可继承句柄的祖先），其次父子边，
  环通过访问集打断并回落虚拟根；`trace` 子命令逐跳展示。

## 6. 快照与版本链

- 每轮检测在单个 SQLite 事务内写入 `snapshot_meta`、`proc_conclusion`、
  `ledger_entry` 三张表，版本号为 `MAX(version)+1`。
- 整批替换识别基于样本内容 SHA-256（进程+事件+版本），而非只比对版本号。
- `VerifyChain` 校验版本 1..N 连续；跳号或被标记非连续返回
  `E_SNAPSHOT_VERSION`，并给出最近连续版本作为回退点。
- `round_progress` 记录在途批次：强制终止后重开库，已提交版本不回退、
  未提交事务由 SQLite 原子性整体丢弃，在途批次可重新检测。

## 7. 并发会话

- `MaxConcurrent` 个信号量槽位；超额 `Submit` 阻塞排队而非拒绝。
- 每会话独立进程状态与误差账本；`MaxProcesses/MaxEvents` 超限以
  `E_SESSION_BUDGET` 仅终止该会话，此前已提交快照保留，其他会话不受影响。

## 8. 异常注入（仅经输入与参数）

| 场景 | 构造方式 | 期望业务码 | 快照是否推进 |
| --- | --- | --- | --- |
| 掩码越界 | 事件掩码置 `0x10000` | `E_SAMPLE_PARSE` | 否 |
| 定点乘法溢出 | 乘数超过 `MaxMicro` | `E_SCORE_OVERFLOW` | 否 |
| 可继承句柄越权 | 子进程持高权句柄注入/写 DAC | `E_PRIV_ESCALATION` | 是 |
| 孤儿子进程 | 父/句柄提供方缺失 | `E_ORPHAN_REF` | 是 |
| 快照版本跳号 | 物理删除中间版本后校验 | `E_SNAPSHOT_VERSION` | 否 |

## 9. 子命令

```
privesc generate --dir DIR --scenario S [--seed N]
privesc detect   --dir DIR [--db PATH] [--threshold-micro N] [--no-snapshot]
privesc trace    --dir DIR --pid PID
privesc replay   --db PATH --version N
privesc ledger   --db PATH --version N
privesc report   --dir DIR [--db PATH] [--out PATH] [--normalize]
privesc inject   --db PATH
```

`report --normalize` 把易变的快照版本与实测耗时归一为固定标记，
保证跨进程/跨运行导出字节一致的可重复 JSON 报告；行按 PID、业务码稳定排序。

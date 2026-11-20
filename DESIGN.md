# 配置分发与状态协调管线 — 技术说明

面向本地多组件协同平台的配置分发管线。全部组件运行在本地回环地址，仅使用
Python 3.11+ 标准库（`sqlite3`、`http.server`、`threading`、`http.client`），
无外部账号、无容器、无线上依赖。代码对路径与信号做了跨平台处理，可在
Windows / Linux / macOS 运行（当前验证环境为 Linux + Python 3.14）。

## 1. 项目结构

```
config_dispatch/
  models.py            快照/组件声明模型、SHA-256 校验摘要
  errors.py            业务错误码（每种失败情形一个稳定码）
  state_machine.py     五状态有限状态机与合法迁移表
  planner.py           影响分析、依赖环检测、Kahn 拓扑排序计划
  repository.py        SQLite 持久化（意图、计划、状态、审计、只读视图）
  push_client.py       推送客户端（拒绝/连接失败/超时三码分立，超时真关连接）
  endpoint_server.py   本地回环 HTTP 测试端点集群（成功/拒绝/延时/失效端口）
  coordinator.py       协调器：单写者串行、同版本单飞、失败策略与回滚收敛
  recovery.py          重启恢复：扫描未完成意图，按持久化事实收敛
  verifier.py          结果验证工具（结构化 JSON 结论，含 CLI）
  worker.py            独立工作进程入口，用于强制终止恢复演示
samples/               固定确定性样本（快照 x4、组件声明、端点脚本 x3）
tests/                 37 个 pytest 用例
tools/gen_samples.py   样本生成器（校验摘要自动计算，可重复生成）
demo.py                端到端演示（四个场景）
```

## 2. 数据模型与快照提交（需求 1）

- 快照字段：`version`（正整数）、`generated_at`、`config_items`（对象）、
  `checksum`。摘要是对配置项规范化 JSON（键排序、无空白）取 SHA-256，
  见 `models.compute_checksum`，样本与实现共用同一算法。
- 提交规则：
  - 缺必填字段 / 版本非正整数 / 摘要不匹配 → `E_INVALID_SNAPSHOT`，不落任何状态。
  - 版本号不大于库内最新版本 → `E_STALE_SNAPSHOT`，不落任何状态。
- 组件声明字段：`component_id`、`depends_on_keys`、`expected_version`、
  `endpoint`、`depends_on_components`；重复标识或未知组件依赖在加载期报错。

## 3. 影响分析与分发计划（需求 2）

- 新旧快照逐键比对得到变化键集合；声明中任一依赖键命中变化集合的组件为
  直接受影响组件，再沿 `depends_on_components` 做下游闭包，得到连带重分发集合。
- 计划用 Kahn 算法做拓扑排序（被依赖者先于依赖者），同层按组件标识字典序，
  因此相同输入恒产生相同计划；计划与快照版本一对一持久化，可重复生成。
- DFS 检测依赖环，命中即返回 `E_DEPENDENCY_CYCLE` 并在 `details.components_in_cycle`
  给出环上组件。

## 4. 组件状态机（需求 3）

| 当前状态 | 合法目标 |
| --- | --- |
| PENDING | DISPATCHED、ROLLED_BACK |
| DISPATCHED | APPLIED、FAILED、ROLLED_BACK、PENDING（仅恢复重排队） |
| APPLIED | ROLLED_BACK、FAILED（回滚指令无法确认时） |
| FAILED | ROLLED_BACK、PENDING（恢复重排队） |
| ROLLED_BACK | （终态，无迁出） |

- 非法迁移抛 `E_INVALID_TRANSITION`；状态机校验发生在写事务内、写入语句之前，
  非法路径不产生任何 UPDATE/INSERT，持久化状态保持不变（有测试断言）。
- 组件状态表主键为 `(version, component_id)`，每条记录都强制携带其依据的快照
  版本，外键指向 `snapshots(version)`，因此不存在“状态与版本不匹配”的可见记录。
- 每次迁移都追加一条只增审计记录（from/to、原因、错误码、时间戳），验证工具
  据此回放检查全部迁移合法性。

## 5. 推送与三类失败码（需求 4）

POST `/apply` 请求体携带 `version`、`config_items`、`idempotency_key`；
POST `/rollback` 携带版本与幂等键。端点按幂等键缓存成功结果，重复请求返回
同一响应。三类互斥失败码：

- 端点可达但返回 422 业务拒绝 → `E_PUSH_REJECTED`
- 连接被拒绝 / DNS / 其它 OS 层网络错误 → `E_PUSH_CONNECTION_FAILED`
- 超过客户端超时窗口 → `E_PUSH_TIMEOUT`

超时分支显式 `connection.close()` 真实关闭底层 socket 释放资源，而非继续
等待；测试断言超时在 1s 窗口返回（端点延时 5s）且关闭后可立即重建连接。
失效端口通过把组件指向未监听端口模拟，集群启动时对该组件跳过监听。

## 6. 失败处置与整体收敛（需求 5）

- `continue` 策略：失败组件落 `FAILED`（带具体错误码），其余组件继续；
  运行终态为 `PARTIAL`，错误摘要列出每个失败组件与错误码。
- `rollback` 策略：首个失败立即停止后续推送，对已 `APPLIED` 的组件按拓扑
  逆序调用 `/rollback`，成功者 `APPLIED -> ROLLED_BACK`；回滚指令本身无法
  确认时落 `APPLIED -> FAILED` 并继续其余回滚，运行终态为 `ROLLED_BACK`。
- 一次分发只产出一个结论标签（`COMPLETED/PARTIAL/ROLLED_BACK/ABORTED`），
  验证器强制：`PARTIAL` 运行不得出现 `ROLLED_BACK`；`ROLLED_BACK` 运行不得
  残留 `APPLIED/DISPATCHED`，从持久化层面禁止两种结论混用。

## 7. 并发控制与可见性（需求 6）

- 协调器持有全局单写者闸门（可重入锁），任意时刻仅一个分发执行，跨快照
  版本同样串行——分发中提交的新快照其分发自然排在当前分发之后。
- 同版本分发采用条件变量单飞：已有 PENDING/RUNNING 意图时，后来线程阻塞等待
  并复用同一运行结论；演示中 5 个并发线程只产生计划组件数次 `/apply` 调用，
  无重复推送。已终结运行的重复触发直接返回持久化结论。
- 只读查询走独立短连接（WAL 模式），与写事务并发执行；状态记录按
  `(版本, 组件)` 存储且迁移带版本断言，查询不会看到“组件 APPLIED 却挂在旧
  版本”的中间态。

## 8. 持久化与强制终止恢复（需求 7）

写操作遵循意图先行：`dispatch_runs` 先插入 PENDING 意图并提交，随后才进入
执行并置 RUNNING；每个组件先确保 PENDING 记录，再做 DISPATCHED/终态迁移，
全部在事务内提交。

恢复服务（`RecoveryService.recover`）扫描所有非终态意图：

1. 先把运行标记为 `ABORTED`（`E_DISPATCH_ABORTED`），记录中止事实。
2. rollback 策略或存在已 APPLIED 事实：在途 DISPATCHED 记录（请求已发出但
   无应用确认）按未应用事实置 ROLLED_BACK；对持久化的 APPLIED 组件逐个调用
   回滚并收敛，运行终态 `ROLLED_BACK`。
3. continue 策略且无 APPLIED 事实：在途记录回归 PENDING 重新排队，可通过
   `requeue_and_run` 重新触发，已 APPLIED 者凭幂等跳过、不重复推送。

恢复报告只包含数据库中已持久化的事实与据此采取的动作，并显式声明：强制
终止发生在进程内部，未提交的退出报告无法重建，不承诺生成“完整退出报告”。

## 9. 验证工具（需求 8）

`python3 -m config_dispatch.verifier --db <path> [--output report.json]`
输出结构化结论，七项检查：

1. 快照版本单调不重复
2. 全部快照校验摘要有效
3. 组件依赖无环
4. 状态记录与快照版本严格绑定且状态值合法
5. 审计日志中每一步迁移都符合状态机
6. 每个终结运行的结论一致（PARTIAL/ROLLBACK 不混用、无残留在途）
7. 全量一致运行（COMPLETED/ROLLED_BACK）满足 APPLIED 依赖闭合

退出码：全部通过 0，存在违例 1。

## 10. 实际验证结果

- `python3 -m pytest tests/ -q` → **37 passed**（约 20s，含真实超时用例）。
- `python3 demo.py` → 退出码 0，四个场景全部符合预期：
  1. v1 全量分发：8 组件按拓扑顺序全部 APPLIED，轨迹
     `PENDING -> DISPATCHED -> APPLIED`。
  2. v2 continue：PARTIAL，同时出现 `E_PUSH_REJECTED`、
     `E_PUSH_CONNECTION_FAILED`、`E_PUSH_TIMEOUT` 三个互斥码；
     v3 rollback：超时触发，已应用组件全部 ROLLED_BACK，运行 ROLLED_BACK。
  3. 5 线程并发重复触发 v4：单一实际执行，端点 `/apply` 调用数等于计划
     组件数，全部线程复用同一 COMPLETED 结论。
  4. 工作进程在途被 SIGKILL（退出码 -9），重启后恢复扫描到 1 个未完成
     意图，按持久化事实回滚，验证结论 `ok=True`。
- 验证工具对主库与恢复库均输出 `ok=True`，结构化报告见
  `runs/verification_report.json`。

## 11. 复现步骤

```bash
python3 tools/gen_samples.py     # 重新生成确定性样本（可选，样本已入库）
python3 -m pytest tests/         # 测试套件
python3 demo.py                  # 端到端四场景演示
python3 -m config_dispatch.verifier --db runs/demo.db
```

Windows 环境下把 `python3` 替换为安装时注册的 `python` 命令即可；演示中的
强制终止使用 `Popen.kill()`（Windows 上等价于 `TerminateProcess`）。

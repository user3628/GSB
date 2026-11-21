# 实际验证记录

## 1. 执行环境

- Python：3.14.4（代码要求 3.11+）
- 依赖：FastAPI 0.165.0、uvicorn 0.54.0、httpx 0.28.1、pytest 9.2.0、requests 2.34.2；完整锁定见 `requirements-lock.txt`
- 网络：协调器、组件、故障关闭端口均只使用 `127.0.0.1`
- 容器：未使用 Docker 或其他容器
- 外部依赖：无

当前执行发生在 Windows 本机的 WSL（Ubuntu）回环环境中，未使用 Docker；实现避免 Unix 专有信号、文件锁和路径，Windows PowerShell 启动命令见 README。强制终止统一使用跨平台 `os._exit(97)`。

## 2. 自动化测试

命令：

```bash
.venv/bin/python -m pytest -q
```

结果：

```text
.........                                                                [100%]
9 passed
```

覆盖内容：

- 显式/哈希路由、重复生产幂等、非法分区业务码、分区序号无缺口。
- 单租约、并发领取单赢家、租约超时原序号第 2 次投递、迟到确认、重复确认。
- 待决头阻塞、跨分区事务提交、回滚占位跳过、结论来源持久化。
- 协调器挂起超时/客户端断开、超时重试、未知、真实连接拒绝、冲突保留首次结论、重复结论码。
- 进程在租约效果后终止、在协调器应答后终止，以及两类重启恢复。

## 3. 可重复实验

命令：

```bash
.venv/bin/python scripts/run_experiment.py --seed 42
```

固定样本投递序列：

```text
orders/0: [1, 2, 3, 4, 6, 7, 8, 9]
orders/1: [1, 2, 3]
orders/2: [1, 2, 3, 4, 5]
events/0..3: []   # 用于验证哈希路由，本实验不消费事件主题
```

`orders/0` 的序号 5 属于回滚事务，故意不投递但保留序号占位。序号 1 先投递；随后待决头 2 阻塞，确认、回滚、超时重试等步骤完成后才继续 2、3、4，再跳过 5 投递 6 等。

事务结论轨迹：

| 事务 | 结论 | 观测 |
| --- | --- | --- |
| `txn-commit-001` | `COMMITTED` | 协调器脚本提交；跨 `orders/0,1` 原子更新 |
| `txn-rollback-001` | `ROLLED_BACK` | 回滚注入；序号 5 保留但不投递 |
| `txn-timeout-001` | `COMMITTED` | 首次 `E_TXN_TIMEOUT`，协调器观察到断开；重试提交 |
| `txn-conflict-001` | `COMMITTED` | 首次提交保留；冲突再次得到 ROLLBACK，返回 `E_TXN_CONFLICT` |
| `txn-crash-001` | `COMMITTED` | 协调器应答后进程终止；重启重新协商 |
| `txn-unknown-001` | `COMMITTED` | 首次 `E_TXN_UNKNOWN` 保持待决；显式提交重试 |
| `txn-unreachable-001` | `COMMITTED` | 关闭端口真实连接拒绝 `E_TXN_UNREACHABLE`；重试提交 |
| `txn-duplicate-001` | `COMMITTED` | 重复相同结论 `E_DUPLICATE_ACK`，状态不变 |

实验实际出现的业务码：

```text
E_DUPLICATE_ACK
E_LEASE_EXPIRED
E_LEASE_HELD
E_PARTITION_INVALID
E_TXN_CONFLICT
E_TXN_TIMEOUT
E_TXN_UNKNOWN
E_TXN_UNREACHABLE
```

超时观测确认协调器看到客户端断开：

```text
request_code = E_TXN_TIMEOUT
coordinator.client_disconnected = true
retry_state = COMMITTED
```

## 4. 强制终止前后差异

### 事务裁决窗口

在协调器已返回、组件尚未提交最终裁决时调用 `os._exit(97)`。重启前直接读取 SQLite：

```text
started_intents = [{kind: TXN_RESOLVE, scope_key: txn:txn-crash-001}]
transaction txn-crash-001 = PENDING
pending_messages = 1
```

重启后：

```text
txn-crash-001 = COMMITTED
pending_count = 0
recovery actions = KEEP_TXN_PENDING_AND_RENEGOTIATE, RENEGOTIATED_FROM_PENDING
renegotiated = 1
```

### 租约领取窗口

在租约已落库、意图尚未标记完成时 `os._exit(97)`。重启前：

```text
ACTIVE lease = orders/2 sequence 3 consumer crash-consumer
STARTED intent = LEASE_ACQUIRE partition:orders:2
```

重启后：

```text
active_lease = null
recovery action = RELEASE_UNCOMMITTED_LEASE
leases_released = 1
redelivery sequence = 3, attempt = 2
```

两份恢复报告均标记：

```text
report_basis = persisted_sqlite_facts
complete_exit_report_possible_after_kill = false
```

这表示报告只还原 SQLite 中已提交的事实，不承诺为被强制终止的进程生成完整退出报告。

## 5. 可重复性

相同输入连续运行两次，归一化后的实验 JSON 文件 SHA-256 完全一致：

```text
d99579b732f1091cc3ae45545f0bf13c5ce9f5d665a7b5eca7381fd105b0c63a
```

报告内容自校验哈希：

```text
eab873ce28810645c4ab933dcf66a4bccb0eebe78b7945b98e5f3cf36b6f72b6
```

报告移除了端口、UUID、耗时等运行噪声，保留固定种子、样本、业务码、投递序列、事务轨迹和恢复前后事实。完整结果位于 `artifacts/experiment/experiment_result.json`。


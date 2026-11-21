# seqtxn：单机顺序消息与事务消息中间件

这是一个面向本地微服务测试环境的 SQLite 消息中间件。它同时提供：

- 主题/分区、哈希或显式分区路由、分区内无缺口生产序号与生产者幂等去重。
- 分区单领取者租约、严格顺序投递、可见性超时后原序号重投。
- 事务消息待决表、本地 HTTP 事务协调器裁决、提交/回滚/超时/未知/冲突处理。
- 持久化意图记录、进程强制终止后的重启扫描与基于 SQLite 事实的恢复报告。
- 可配置的协调器异常注入与固定种子、可重复的一键实验。

所有服务只绑定 `127.0.0.1`，不需要 Docker、外部消息中间件、真实账号或网络资源。

## 环境与安装

要求 Python 3.11+。依赖版本已锁定；完整传递依赖见 `requirements-lock.txt`。

Windows PowerShell：

```powershell
py -3.11 -m venv .venv
.\.venv\Scripts\python.exe -m pip install --upgrade pip
.\.venv\Scripts\python.exe -m pip install -r requirements-lock.txt
.\.venv\Scripts\python.exe -m pip install -e .
```

Linux/macOS：

```bash
python3 -m venv .venv
.venv/bin/python -m pip install --upgrade pip
.venv/bin/python -m pip install -r requirements-lock.txt
.venv/bin/python -m pip install -e .
```

## 一键验证

运行全部自动化测试：

```powershell
.\.venv\Scripts\python.exe -m pytest -q
```

运行固定种子实验（会重建 `artifacts/experiment/`）：

```powershell
.\.venv\Scripts\python.exe scripts\run_experiment.py --seed 42
```

实验会启动协调器和组件两个真实本地 HTTP 子进程，完成：

1. 同分区顺序生产/消费、无缺口序号、重复生产幂等、非法分区。
2. 待决事务提交、回滚占位、待决头阻塞、跨分区原子提交。
3. 协调器挂起超时、真实 TCP 连接关闭、重试提交。
4. 未知结论、连接拒绝、重复结论冲突与保留首次结论。
5. 同分区并发领取冲突、租约超时后原序号重投、重复确认。
6. 在租约落库后和协调器应答后两个窗口用 `os._exit` 强制终止，再重启恢复。

报告写入 `artifacts/experiment/experiment_result.json`；相同输入与种子重复运行会产生相同文件哈希。

## 启动服务

终端一，事务协调测试端点：

```powershell
.\.venv\Scripts\python.exe -m seqtxn.coordinator_cli `
  --config samples\coordinator_responses.json --host 127.0.0.1 --port 18081
```

终端二，中间件组件：

```powershell
.\.venv\Scripts\python.exe -m seqtxn.cli `
  --db runtime\middleware.db `
  --coordinator-url http://127.0.0.1:18081 `
  --unreachable-url http://127.0.0.1:1 `
  --host 127.0.0.1 --port 18080
```

FastAPI 文档位于 `http://127.0.0.1:18080/docs`。协调器配置与异常观测分别位于
`/admin/configure`、`/admin/reset` 和 `/admin/observations`。

## 最小 API 示例

以下 Python 片段使用固定样本中的显式分区主题：

```python
import requests

base = "http://127.0.0.1:18080"
requests.post(f"{base}/topics", json={
    "name": "orders", "partition_count": 3, "routing_mode": "explicit"
}).raise_for_status()

requests.post(f"{base}/topics/orders/messages", json={
    "producer_id": "p1", "client_sequence": "1", "partition": 0,
    "body": {"id": 1}
}).raise_for_status()

claim = requests.post(f"{base}/topics/orders/partitions/0/claim", json={
    "consumer_id": "c1", "visibility_timeout_ms": 5000
}).json()["data"]

requests.post(f"{base}/acks", json={
    "lease_token": claim["lease_token"], "sequence": claim["message"]["sequence"]
}).raise_for_status()
```

事务消息生产时增加 `"txn_id": "txn-1"`，然后调用
`POST /transactions/txn-1/resolve`。可传入：

```json
{"injection": "commit | rollback | refuse | hang | unknown | duplicate_same | duplicate_conflict", "timeout_ms": 150, "force": false}
```

## 核心语义

- 普通消息生产即提交；事务消息先写 `pending_messages`，消费者永远看不到 `PENDING` 消息。
- 分区投递头是“最小未确认且未回滚”的序号。头为 `PENDING` 时不发租约，并由它阻止后继消息越序投递。
- 提交后消息变为 `COMMITTED`；回滚后消息变为 `ROLLED_BACK`，保留序号占位但不投递。
- `visibility_sequence` 是已取得最终结论的最大序号；`head_sequence` 是当前投递头；只有头已提交时才写入 `delivery_sequence` 并令 `delivery_ready=true`。
- 分区租约由 SQLite 部分唯一索引保证至多一个 `ACTIVE` 行。超时后领取或迟到确认都会把旧租约真实落为 `EXPIRED`。
- 事务结论、消息状态和所有受影响分区的水位在同一个 SQLite 写事务中提交；读者只能看到提交前或提交后的完整版本。
- 同一事务的重复协调查询不覆盖首次结论；冲突写入 `txn_conflicts` 并返回 `E_TXN_CONFLICT`。
- 领取、确认、发布和裁决都先持久化 `STARTED` 意图。恢复只扫描这些事实，不依赖进程退出时生成的内存报告。

业务码与 HTTP 状态、数据表、状态机和故障注入映射详见 `docs/DESIGN.md`；本次实际运行结果见 `docs/VERIFICATION.md` 与 JSON 报告。

## 目录

```text
seqtxn/                     中间件、协调器、CLI 与 HTTP 客户端
samples/                    固定主题、消息批次、协调响应脚本
scripts/run_experiment.py   可重复端到端实验与强制终止恢复演示
tests/                      顺序、租约、事务、失败注入、恢复测试
docs/DESIGN.md              算法与一致性设计说明
docs/VERIFICATION.md        实际验证记录
artifacts/experiment/       固定种子实验输出
```

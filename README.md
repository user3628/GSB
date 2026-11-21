# Loopback Tunnel Gateway

面向 Windows 本地回环环境的 VPN 隧道网关核心组件。项目包含 FastAPI 控制面、真实 UDP
两段承载链路、版本化隧道/路由快照、固定通道调度器、SQLite 意图与租约持久化、原子增量
重建、强制终止恢复，以及可交互控制台和可重复演示。

不使用 Docker、真实 VPN 账号、外部目标或未授权网络。所有端点均被限制在
`localhost` / `127.0.0.1` / `::1`。

## 快速开始（Windows）

要求 Python 3.11 或更新版本。

```bat
cd /d <project-root>
py -3.11 -m venv .venv
.venv\Scripts\activate.bat
python -m pip install -r requirements.txt
python scripts\check_ports.py --http-port 18765
python -m vgw serve --host 127.0.0.1 --port 18765 --db artifacts\vgw.db
```

打开 `http://127.0.0.1:18765` 使用控制台，`/docs` 查看 OpenAPI。

也可以直接运行：

```bat
scripts\start.bat
```

固定样本会占用以下回环端口：

| 用途 | 地址 |
|---|---|
| HTTP 控制面 | `127.0.0.1:18765` |
| carrier-east | `127.0.0.1:18811` |
| decap-east | `127.0.0.1:18911` |
| carrier-west | `127.0.0.1:18812` |
| decap-west | `127.0.0.1:18912` |

## 可重复演示

```bat
scripts\demo.bat
```

或：

```bat
python -m vgw demo --output artifacts\demo\current
```

演示使用固定样本真实执行：

1. 全量加载 v1 隧道/路由快照。
2. `alpha`、`beta`、`gamma` 三会话并行转发两轮连续帧。
3. 提交仅修改 `beta` 出口的 v2 增量片段，验证级联失效、统计重建和新出口探测。
4. 占满三个通道并在容量为 2 的等待队列中制造 `E_CHANNEL_QUEUE_FULL`。
5. 启动子进程写入 pending intent、dispatched frame 与 active lease 后强制终止，再由新进程扫描 SQLite 事实恢复。

输出：

- `artifacts/demo/current/report.md`：可读验收报告、通道占用轨迹、重建范围、恢复差异。
- `artifacts/demo/current/summary.json`：完整机器可读结果。
- `artifacts/demo/current/traces.json`：持久化协议轨迹。
- `artifacts/demo/current/recovery_diff.json`：强制终止前后事实与恢复动作。

本次实际运行结果见 `VERIFICATION.md`。

## 测试

```bat
python -m pytest -q
```

当前回归集覆盖快照摘要/版本失败无副作用、二进制帧校验、并发通道、同会话有序、优先级、
队满、真实 UDP 超时与通道复用、坏响应、重复帧幂等/冲突、增量原子重建、在途帧排空、
payload 上限和 pending intent 恢复。

## 目录

```text
vgw/
  app.py          FastAPI、统一错误信封、控制台 API、交互实验室场景
  codec.py        VGW1/VGR1 二进制帧封装、解封装与摘要校验
  gateway.py      单写者状态机、调度器、幂等派发、事务重建
  models.py       快照、增量、HTTP 请求模型与 loopback 校验
  snapshot.py     规范化摘要、版本校验、增量应用
  store.py        SQLite WAL、intents、frames、leases、stats、recovery
  transport.py    UDP carrier relay、egress decapsulator、真实超时
  static/         控制台 HTML/CSS/JS
samples/          固定快照、增量、帧序、期望结果、拓扑
tests/            25 项自动化回归
artifacts/demo/   实际演示输出
docs/ARCHITECTURE.md
```

## 设计要点

### 快照与增量

快照字段包括单调递增 `version`、`generated_at`、`channel_count`、
`round_budget_ms`、`queue_capacity`、会话 `bindings`、`egress_endpoints` 和
`sha256:` 整体摘要。摘要对去除自身后的规范化 JSON 计算。

加载失败（模型错误、摘要失败、版本不递增）在写入任何状态前返回
`E_SNAPSHOT_INVALID`。增量片段必须携带 `base_version`；非当前版本返回
`E_STALE_BASE`。合法增量会：

- 阻止受影响会话的新提交；
- 等待其在旧绑定下的活动帧和排队帧排空；
- 在一个 SQLite 事务边界内写新快照并从已确认帧事实重建统计；
- 一次性发布新快照和新统计，之后解除阻塞。

因此在途帧不会中途改绑，查询也不会看到“部分会话已切换、统计仍是旧端点”的中间状态。

### 帧协议

`VGW1` 请求帧包含 magic、协议版本、通道、uint64 序号、载荷字节数、会话标识字节数、
会话标识、载荷和 SHA-256 摘要。`VGR1` 响应帧带状态字节和同样的身份字段与摘要。
载荷超限、长度不一致、magic/版本错误或摘要不一致均返回 `E_FRAME_INVALID`，不会进入
发送路径。同一会话只接受连续序号。

### 调度与超时

每个快照固定通道数、每轮时间预算、有限等待队列。候选帧按会话优先级降序、到达顺序升序
选择，且只使用其绑定通道；同一会话最多一帧占用通道。每帧一个独立 connected UDP socket，
`asyncio.wait_for` 到期后立即在 `finally` 关闭该 socket 并释放通道。错误上下文明确标注
`wait_cancelled`、`socket_closed_after_timeout` 与 `remote_may_still_process`，不声称已经
终止远端处理。

### 持久化与恢复

SQLite 使用 WAL 与 `synchronous=FULL`。派发前先在同一个事务中写入 pending intent，再更新
frame 为 dispatched、lease 为 active（含 channel 与 `sent_unacked_seq`）。确认后另一事务
提交 ack、释放 lease、更新统计和会话最后确认序号。

重启扫描 pending intents、queued/dispatched frames 和 leases：

- 当前绑定仍匹配的 pending dispatch → frame 标记 `retransmit_required`；
- 绑定已被新快照取代 → `aborted`；
- 尚未派发的排队帧 → `aborted`；
- 所有崩溃前活跃 lease 清空通道占用，保留最近活动时刻并记录差异。

报告只使用已提交 SQLite 事实，并明确 `graceful_exit_report_available: false`。

### 并发一致与幂等

租约授予/释放、帧派发/完成、快照加载和增量重建均处于单一 writer lock。只读查询读取一次
提交后替换的不可变发布对象，无需阻塞 writer。`frame_id` 首次提交创建唯一操作；相同内容
的并发重复请求等待并复用同一结果，内容不同返回 `E_CONFLICTING_FRAME`。

## HTTP 接口

| 方法 | 路径 | 说明 |
|---|---|---|
| GET | `/healthz` | 存活、快照版本、调度器状态 |
| GET | `/api/v1/overview` | 完整查询状态、拓扑、恢复摘要 |
| POST | `/api/v1/snapshots` | 全量快照加载 |
| GET | `/api/v1/snapshots/current` | 当前快照 |
| POST | `/api/v1/routes/patches` | 原子增量路由提交 |
| POST | `/api/v1/frames` | 入站帧提交并等待响应 |
| POST | `/api/v1/frames/batch` | 并发批量提交 |
| POST | `/api/v1/frames/decode` | 原始 `base64` 帧校验/解封装 |
| GET | `/api/v1/frames` | 持久化帧事实 |
| GET | `/api/v1/traces` | 通道/帧/恢复轨迹 |
| GET | `/api/v1/intents` | 意图日志 |
| GET | `/api/v1/recovery` | 最近恢复报告 |
| POST | `/api/v1/lab/scenarios/*` | 控制台可重复场景 |

失败统一为：

```json
{
  "ok": false,
  "error": {
    "code": "E_CHANNEL_QUEUE_FULL",
    "message": "the bounded waiting queue is full",
    "context": {
      "queue_depth": 2,
      "queue_capacity": 2,
      "active_sessions": ["alpha", "beta", "gamma"]
    }
  }
}
```

| 业务码 | HTTP | 含义 |
|---|---:|---|
| `E_SNAPSHOT_INVALID` | 400 | 快照/增量结构、摘要或版本非法 |
| `E_FRAME_INVALID` | 400 | 序号、载荷、帧结构、摘要或响应身份非法 |
| `E_STALE_BASE` | 409 | 增量基础版本不是当前版本 |
| `E_CONFLICTING_FRAME` | 409 | `frame_id` 已存在但内容不同 |
| `E_CHANNEL_QUEUE_FULL` | 429 | 有界等待队列已满 |
| `E_TUNNEL_TIMEOUT` | 504 | 单帧 UDP 等待超时，socket 已关闭、通道释放 |

## 固定样本

- `samples/snapshot_v1.json`：3 通道、队列容量 2、6 个会话、两个出口。
- `samples/patch_v2.json`：仅将 `beta` 从 `west` 切到 `east`，基础版本 1。
- `samples/frames.json`：并行轮次、增量后探测、通道耗尽帧组。
- `samples/expected.json`：每会话期望有序序号与溢出业务码。
- `samples/topology.json`：carrier → decap 的本地 UDP 地址映射。

重新生成固定样本：

```bat
python scripts\generate_samples.py
```

## 边界说明

- 这是一个本地测试承载实现，不是生产 VPN 加密协议；协议中没有真实加密/认证密钥。
- SQLite 单写者协调以单网关进程为前提；多个网关进程同时调度同一数据库不在范围内。
- 强制终止后不会尝试猜测远端是否收到某一 UDP 数据报；只按已提交的 intent/frame/lease
  事实标记待重传或已中止。
- 控制台和 API 仅绑定回环地址。

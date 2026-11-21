# Verification Record

本文件记录当前工作区的实际验证结果。验证环境为 WSL2 Ubuntu 中的 Python
`3.14.4`（满足项目要求的 `>=3.11`），未使用 Docker 或容器运行时。

## Automated tests

命令：

```bash
python -m pytest -q
```

结果：

```text
25 passed, 1 warning in 1.83s
```

覆盖点：

- `VGW1`/`VGR1` 二进制帧 round-trip、截断、长度不一致、摘要篡改、载荷上限；
- 序号解析失败与 uint64 边界；
- 快照摘要/版本失败时旧快照、会话进度和统计完全不变；
- 三通道并行、同会话严格串行、优先级与到达顺序；
- 单通道 + 队列容量 2 时第四个等待帧返回 `E_CHANNEL_QUEUE_FULL`；
- 80 ms 真实 UDP 超时返回 `E_TUNNEL_TIMEOUT`，上下文标记 socket 已关闭，随后同会话重试成功；
- 坏响应摘要返回 `E_FRAME_INVALID` 并释放通道；
- 相同 `frame_id` 并发提交只派发一次，不同内容返回 `E_CONFLICTING_FRAME`；
- `base_version` 过期返回 `E_STALE_BASE` 且状态不变；
- 增量只影响 `beta`，同一事务重建统计，新帧走新出口；
- 增量提交等待旧绑定在途帧完成后才切换；
- pending dispatch intent + active lease 在重启后标记 `retransmit_required` 并清空租约；
- FastAPI 控制台、健康检查、统一错误信封与 `/docs` 挂载路径可加载。

## Deterministic demo

命令：

```bash
python -m vgw demo --output artifacts/demo/current
```

实际结果：

```text
snapshot        v1  sha256:cc5d8cf12710a30dcf733acf0ec4bb9ca6b35c38536bb952a2cd34a3dedd6c37
parallel         all_ack=True  max_channels=3
  alpha      expected=[1, 2] observed=[1, 2] ok=True
  beta       expected=[1, 2] observed=[1, 2] ok=True
  gamma      expected=[1, 2] observed=[1, 2] ok=True
route rebuild   v1 -> v2 scope=['beta']
  beta egress   127.0.0.1:18812 -> 127.0.0.1:18811  endpoint=decap-east
queue pressure  overflow=E_CHANNEL_QUEUE_FULL expected=E_CHANNEL_QUEUE_FULL capacity=2
crash recovery  frame=retransmit_required retransmit=True lease_released=True
  egress seen   True  observation=decap-east alpha/seq=1
  facts_only    True  graceful_exit_report=False
verification    PASS
```

完整输出：

- `artifacts/demo/current/report.md`
- `artifacts/demo/current/summary.json`
- `artifacts/demo/current/traces.json`
- `artifacts/demo/current/recovery_diff.json`

强制终止恢复事实：

- 子进程退出码：`-9`（Linux `SIGKILL`；Windows 等价使用 `TerminateProcess`）。
- 恢复前持久化事实：pending dispatch intent、frame `dispatched`、lease
  `channel_id=0`、`sent_unacked_seq=1`、`status=active`。
- 杀死子进程前，本地出口 decap 实际观察到 `alpha/seq=1` 数据报（`egress_observed_before_kill=true`）。
- 恢复后：frame `retransmit_required`，lease `channel_id=null`、
  `sent_unacked_seq=null`、`status=recovered_idle`。
- 恢复报告 `facts_only=true`，`graceful_exit_report_available=false`。

最终连续序号核验：

| Session | Acked sequences |
|---|---|
| alpha | `[1, 2, 3]` |
| beta | `[1, 2, 3, 4]` |
| gamma | `[1, 2, 3]` |
| delta | `[1]` |
| epsilon | `[1]` |

## Live FastAPI verification

实际启动命令：

```bash
python -m vgw serve --host 127.0.0.1 --port 18767 --db artifacts/live_verify2.db
```

验证结果：

- `GET /healthz` → `200`，`snapshot_version=1`，`scheduler_running=true`。
- `GET /` → `200`，控制台 HTML 正常返回。
- `GET /api/v1/overview` → 3 个固定通道、两个 carrier/decap 映射。
- `POST /api/v1/lab/scenarios/parallel` 连续执行两次 → 每轮 6/6 帧确认，证明交互实验室
  场景无需重置数据库即可重复。
- `POST /api/v1/lab/scenarios/rebuild` → v1 → v2，影响范围仅 `beta`。
- `POST /api/v1/lab/scenarios/pressure` → 三个活动通道、两个排队帧、一个
  `E_CHANNEL_QUEUE_FULL`。
- 场景结束后 `queue_depth=0`、`active=0`，各会话序号连续。

## Constraints observed

- 所有 UDP/TCP 地址均为回环地址。
- 无容器、无外部目标、无真实账号、无云端依赖。
- 未访问未授权网络。


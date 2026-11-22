# Verification Record

验证日期：2026-11-23  
工作区：`/home/zsl/feedback/uid37fecb20-target-mwnkf3ge-130`  
实际解释器：Python 3.14.4  
依赖：FastAPI 0.115.6、Uvicorn 0.34.0、NumPy 2.2.1、HTTPX 0.28.1、pytest 8.3.4

本实现按 Windows Python 3.11+ 可运行方式编写；本次实际执行环境为该 Linux 工作区，未使用容器或外部网络。

## 自动化测试

命令：

```bash
python3 -m pytest -q
```

结果：

```text
....................                                                     [100%]
20 passed
```

覆盖内容：

- 全量快照、整份替换、增量 stale 基础版本拒绝。
- 同基础版本两个并发增量仅一个成功。
- 损坏编译路径不改变当前已提交版本，并留下 `FAILED` 意图。
- 级联事件只指向变化路由及其依赖聚合；未变化路由产物摘要保持逐位一致。
- 同一上游集合按不同提交顺序与批次得到相同的路由 ID 排序、Q6 总等待预算和惩罚权重。
- 同请求 ID 5 路并发仅真实转发 1 次；不同请求复用同 ID 返回 `E_CONFLICTING_REQUEST`。
- 反向/正向转发、查询串、请求体、业务头与逐跳头过滤。
- 上游 503 透传、180ms 真实读取超时、连接拒绝业务码。
- 在途请求使用旧版本，更新完成后新请求使用新版本。
- 独立工作台进程在意图 `PREPARED` 后被 `kill()`，重启恢复到最后完整快照。

## 端到端演示

命令：

```bash
python3 scripts/run_demo.py
```

实际输出摘要：

```text
[1] 全量快照 v1 原子提交
version=1 routes=5 artifacts=23 events=26

[2] 反向转发
status=200 backend=primary path=/svc/echo query=demo=reverse body=reverse-body

[3] 正向转发（经本地出口）
status=200 backend=primary egress=127.0.0.1:18082

[4] Q6 定点预算与误差
total_wait=1692.482046 ms cumulative_error=-0.138415245737 Q6
normalize_timeout=0.000000000000
connection_budget=-0.060180541625
total_wait_budget=-0.138415245737
health_penalty_weight=-0.138415245737

[5] 混合量级求和对比
naive_error=-10.0 Q6
kahan_error=0.0 Q6

[6] 真实读取超时
status=504 elapsed=0.257s stage=read_timeout code=E_UPSTREAM_TIMEOUT

[7] 请求标识去重
4 concurrent results=[200, 200, 200, 200]
upstream_delta=1
conflicting_reuse=409/E_CONFLICTING_REQUEST

[8] 单路由上游增量提交 v2
version=2 events=14
reverse-main backend=secondary
stale=409/E_STALE_BASE

[9] 级联失效/重建轨迹（摘要）
forward_cache      INVALIDATED count=1 / REBUILT count=1
health_plan        INVALIDATED count=1 / REBUILT count=1
match_entry        INVALIDATED count=1 / REBUILT count=1
match_index        INVALIDATED count=1 / REBUILT count=1
pool_group         INVALIDATED count=1 / REBUILT count=1
upstream_budget    INVALIDATED count=2 / REBUILT count=2

[10] 强制终止
killed pid while intent v3 was PREPARED;
configuration transaction never committed

[11] 重启扫描未完成意图并恢复
current_version=2
last_complete_version=2
orphans_rolled_back=1

[12] 版本轨迹
v1 -> v2 (incomplete v3 rolled back)
```

持久化摘要写入 `runtime/demo_report.json`。该文件由演示实际生成，包含提交摘要、预算误差、基准结果、幂等统计、孤儿意图与恢复事实。

## 关键实际观测

| 观测项 | 结果 |
|---|---|
| 挂起路由超时 | 约 0.257 秒返回 504，`stage=read_timeout` |
| 4 路同 ID 并发请求 | 上游计数仅 +1，4 个响应均为 200 |
| 相同 ID 不同请求 | 409 `E_CONFLICTING_REQUEST`，上游计数不变 |
| 增量切换上游 | `reverse-main` 从 primary 切到 secondary |
| 重复旧基础版本 | 409 `E_STALE_BASE`，版本保持 v2 |
| 强制终止 | 意图 v3 为 `PREPARED`，无 v3 完整快照 |
| 重启恢复 | 当前与最后完整版本均为 v2，孤儿意图数 1，回滚数 1 |

## 复现命令

```bash
python3 scripts/check_ports.py
python3 -m pytest -q
python3 scripts/run_demo.py
```

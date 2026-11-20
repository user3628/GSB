# 实际验证记录

## 执行环境

- 工作区：`/home/zsl/feedback/uid37fecb20-target-mwnkf3ge-16`
- 实际执行环境：Linux，Python 3.14.4（代码要求 Python 3.11+）
- 已安装/验证：FastAPI、Uvicorn、NumPy、pytest、httpx
- 强杀方式：`subprocess.Popen.kill()`，在 POSIX 上发送 SIGKILL；在 Windows 上对应
  `TerminateProcess`，也可按任务管理器或 `Stop-Process -Id <pid> -Force` 操作
- 已提供 Windows PowerShell 启动脚本；当前执行器不是 Windows，故未在本机实跑 PowerShell

## 自动测试

命令：

```text
python3 -m pytest -q
```

结果：

```text
13 passed, 1 warning in 1.22s
```

覆盖：最小样本、精确与采样窗口、确定性重复查询、CI 包含精确 SLI、关闭桶拒绝、
三窗口预算平铺一致、多窗口告警、幂等批次、single-flight、恢复意图扫描、HTTP 统一错误、
采样响应逐字节一致。

## 固定样本

```text
samples/events_small.jsonl  6000 events  bad=30   SLI=0.995
samples/events_large.jsonl 60000 events  bad=600  SLI=0.990
```

清单文件：`samples/manifest.json`。两者时间范围均为 2026-01-15T10:00:00Z 至
2026-01-15T12:00:00Z。

## HTTP 端到端结果

命令：

```text
python3 scripts/e2e_demo.py
```

服务配置：SLO 99.9%，最小样本 100，精确上限 10,000，迟到容忍 600 秒，WARNING 2.0，
CRITICAL 6.0。

小样本：

```text
version=24 total_events=6000
5m  SLI=0.996000 burn=4.00x exact
30m SLI=0.995333 burn=4.67x exact
1h  SLI=0.995000 burn=5.00x exact
alert=WARNING
bad=30; 5m/30m/1h tumbling partitions each sum bad=30 and equal service total
```

大样本：

```text
version=48 total_events=60000
5m  SLI=0.990000 burn=10.00x exact, sample_rate=1.0
30m SLI=0.989500 burn=10.50x sampled, sample_rate=0.666666667,
    CI=[0.987305798, 0.991318268]
1h  SLI=0.990500 burn=9.50x sampled, sample_rate=0.333333333,
    CI=[0.988401464, 0.992221833]
alert=CRITICAL
bad=600; 5m/30m/1h tumbling partitions each sum bad=600 and equal service total
```

两个场景的全部 `consistency_checks` 为真，包括账本事件数、坏事件数和预算单位与服务总账
相等，以及所有平铺窗口与总账相等。

迟到检查：向已推进到 12:00 的 `checkout-large` 提交 10:00 的事件，服务返回 HTTP 409、
`E_LATE_EVENT`，并在同一提交版本下保持桶计数不变；`/v1/rejections` 记录 1 条拒绝原因。

## 强制终止恢复

1. 以 15,000 ms 意图后暂停启动服务；
2. 提交 `forced-crash` 批次；
3. 从 SQLite 观察到持久化 `RUNNING` intent，目标版本 49；
4. 强杀服务进程；
5. 强杀后核对：版本仍为 48，事件仍为 66,000，RUNNING intent 仍为 1；
6. 正常重启；
7. `GET /v1/recovery` 得到：

```text
last_complete_version=48
intents_scanned=1
intents_abandoned=1
target_version=49
action=ROLLED_BACK_TO_LAST_COMPLETE_VERSION
restart_version=48
restart_event_count=66000
```

恢复报告 `consistency.requires_window_recompute=false`，因为未提交批次没有产生任何业务行；
最后完整版本中的窗口、预算和告警保持可用。报告明确写明强杀无法产生优雅退出报告，也不
承诺未提交工作。

完整机器可读报告：`artifacts/e2e_report.json`。

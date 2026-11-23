# 可重复演示手册

## 1. 前置条件

```text
Go 1.22+
可访问 Go 模块代理的模块缓存
无需 Docker / CGO / 外部数据库 / 外部账号
```

验证版本：

```bash
go version
go mod download
```

## 2. 一键验收

Linux、macOS、WSL：

```bash
./scripts/demo.sh
```

Windows PowerShell：

```powershell
.\scripts\demo.ps1
```

脚本执行内容：

1. 将 `cmd/workbench` 编译到临时目录。
2. 在本机临时回环端口启动回滚 HTTP 端点。
3. 启动工作台，提交固定策略、4 个批次和 36 份观测。
4. 从同一提交快照比较快速/完整结论。
5. 完整裁决放行 `chg-pass-001`。
6. 完整裁决对 `chg-risk-002` 触发自动回滚并接收 HTTP 触达。
7. 对同服务 `chg-cooldown-003` 再次裁决，校验 `E_ROLLBACK_COOLDOWN`。
8. 以 `risk_eval_before_commit` 注入点启动工作台，远程序列在风险事务提交前执行 `exit(137)`。
9. 重启工作台，读取启动恢复报告，确认中止意图、批次 `OBSERVING`、修订号未前进、最新提交序号未前进。
10. 恢复后重新裁决，确认可正常完成 `ROLLED_BACK`。

最近一次实际运行的输出：

```text
[1/7] 启动本地工作台与回环回滚端点
[2/7] 通过 HTTP 提交策略、批次和固定观测
  policy=v1 commit=1 batches=4 observations=36
[3/7] 核对快速/完整同快照结论
  fast action=CONTINUE_OBSERVING score=389 rate=0.667; full action=PASS score=389
[4/7] 完整裁决放行低风险批次
  chg-pass-001 score=389 level=LOW status=PASSED
[5/7] 完整裁决触发自动回滚
  chg-risk-002 score=10000 status=ROLLED_BACK webhook_hits=1
[6/7] 同服务冷却期拦截
  E_ROLLBACK_COOLDOWN remaining=119996ms service=payments-api
[7/7] 强制终止风险重算并验证启动恢复
  recovery pending=1 commit=13 batch_status=OBSERVING batch_revision=1
  chg-recover-004 recovered score=10000 status=ROLLED_BACK webhook_hits=2

DEMO VERIFIED · 10 checks
```

机器可读结果写入 `artifacts/demo-report.json`，包含每项断言的事实、耗时、回滚触达结果和完整恢复报告。

## 3. 手动交互演示

启动工作台：

```bash
go run ./cmd/workbench -addr 127.0.0.1:8080 -db data/workbench.db
```

打开 `http://127.0.0.1:8080`：

1. 首次进入点击“初始化本地工作台”。
2. 在批次列表选择任意批次。
3. 切换“快速 / 完整”，观察风险分、采样率、结论和差异表。
4. 点击“提交完整裁决”；低风险批次进入 `PASSED`，高风险批次执行回滚。
5. 选择同服务的第二个高风险批次并裁决，会显示冷却期拦截与剩余时长。
6. 点击“增量观测”可追加一份观测；时间戳必须不小于最后观测，序号冲突会被拒绝。
7. 页面底部的意图和恢复面板显示持久化事实。

独立启动回滚端点（可选）：

```bash
go run ./cmd/webhook -addr 127.0.0.1:18090 -out artifacts/rollback-events.jsonl
go run ./cmd/workbench -rollback-url http://127.0.0.1:18090/rollback
```

工作台不指定 `-rollback-url` 时默认触达同进程回环端点 `/api/local-rollback-target`。

## 4. 固定样本

| 文件 | 内容 |
|---|---|
| `samples/policy-v1.json` | 策略 v1，含 SHA-256 摘要 |
| `samples/batches.json` | 放行、回滚、冷却、恢复四个批次 |
| `samples/observations.json` | 每批次 9 份报告，共 36 份；时间单调 |

快速模式每指标取最近 2 份报告。放行批次快速采样 30/45 个样本（`666,667ppm`），完整窗口 45 个样本；因此快速模式风险同为 `389bp`，但存在未采样观测，安全守护禁止放行，结论为“继续观察”。

## 5. 验收对照

| 验收项 | 预期事实 | 实际结果 |
|---|---|---|
| 放行 | `chg-pass-001`，`389bp`，`LOW`，`PASSED` | 通过 |
| 自动回滚 | `chg-risk-002`，`10000bp`，回滚端点命中，`ROLLED_BACK` | 通过 |
| 冷却拦截 | `E_ROLLBACK_COOLDOWN`，剩余约 `120s`，端点不重复命中 | 通过 |
| 强制终止恢复 | 中止 1 个 `PENDING` 意图；提交序号保持 13；批次仍 `OBSERVING` | 通过 |
| 恢复后继续 | 重新裁决成功，状态进入 `ROLLED_BACK` | 通过 |
| 快速/完整一致边界 | 同策略 v1、同观测修订、同提交序号 | 通过 |

## 6. 清理

自动化演示使用操作系统临时目录，默认结束后删除。手工运行产生的 `data/workbench.db` 可直接删除以重置；也可在界面点击“重置演示样本”。整个过程不会写入任何外部服务。

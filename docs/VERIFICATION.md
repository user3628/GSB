# 实际验证记录

## 环境

- 日期：2026-11-23
- 平台：WSL2 Linux，代码与脚本按 Windows 本地路径和启动方式设计
- Python：3.14.4（满足 >=3.11）
- FastAPI 0.115.6、Uvicorn 0.34.0、dnslib 0.9.26、pydantic 2.14.4、pytest 8.3.4、httpx 0.28.1
- 虚拟环境：`.venv`，使用 `--system-site-packages` 和本地已安装/缓存依赖，未访问公网服务

## 自动化测试

命令：

```bash
.venv/bin/python -m pytest -q
```

结果：

```text
..................................                                       [100%]
37 passed
```

覆盖：

- 严格 DNS 名称、压缩指针、截断、超长标签和尾部垃圾字节拒绝；
- A/AAAA/CNAME/MX/TXT/NS/SOA 正解析，CNAME 环和超长链；
- 大小写混写与转义标签缓存键归一；
- 正负缓存 TTL、毫秒递减、过期清理；
- 热点合并、同轮与跨轮单飞、相同 TTL 快照；
- 全局并发、单权威并发、优先级、批内与跨轮队列满、预算跳过；
- 合并等待预算冲突的显式拆分数值；
- 真实 UDP 超时、套接字关闭、名额释放；
- UDP/TCP DNS 监听器、畸形报文不建会话；
- HTTP 成功与 `E_MESSAGE_INVALID`、`E_CNAME_LOOP`、`E_QUEUE_FULL`、`E_UPSTREAM_TIMEOUT`、`E_CONFLICTING_QUERY`；
- query_id 幂等复用和冲突检测；
- 未完成意图回滚、未完成会话中止、已完成缓存跨非正常关闭保留。

## 端到端演示

命令：

```bash
.venv/bin/python -m dns_workbench demo --workdir artifacts/demo
```

结果：`DEMO PASS`，报告位于 `artifacts/demo/report.json`。

关键实测值：

- 冷缓存 `a.lab.test. A`：解析成功，返回 `192.0.2.10`。
- 250ms 后按大小写混写查询：`cache_hit`；同一域名只有一个缓存条目，本次运行剩余 TTL 约 `119.7s`。
- 12 个热点并发查询：热点名上游查询数增加 `1`，结果为一个 `resolved` 加 11 个 `merged`，所有项目 TTL 均为 `60000ms`。
- 80ms 轮次预算：轮次耗时约 `86ms`，等待中的长尾进入 `waiting` 与 `skipped` 清单，活动慢查询也被预算取消；活动查询上下文含 `socket_closed=true`。
- 上游丢包：HTTP `504` / `E_UPSTREAM_TIMEOUT`；活动套接字 `0`，套接字余额 `0`。
- 强制终止：重启恢复报告 `forced_interruption_detected=true`，列出被中止的 `resolving` 会话；已完成 MX 缓存重启后仍为 `cache_hit`，目标与中断前一致为 `mail.lab.test.`。

## 演示产物

- `artifacts/demo/report.json`：机器可读报告。
- `artifacts/demo/logs/service.log`：首次服务进程日志。
- `artifacts/demo/logs/service-restarted.log`：强制终止后的重启日志。
- `artifacts/demo/config.json`：本次随机高位端口配置。

报告中的端口是每次演示动态分配的回环高位端口；查询目标和期望行为固定。


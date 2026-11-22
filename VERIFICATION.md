# 实际验证记录

验证日期：2026-11-23（Asia/Shanghai）

运行环境：

- WSL2 / Ubuntu，项目位于空白工作区
- Python `3.14.4`（要求 Python ≥3.11）
- FastAPI `0.115.6`
- Uvicorn `0.34.0`
- NumPy `2.2.1`
- pytest `8.3.4`
- httpx `0.28.1`（FastAPI TestClient 依赖）
- SQLite 使用 Python 标准库 `sqlite3`
- 实际 SQLite 库版本 `3.46.1`

依赖安装：

```text
.venv/bin/python -m pip install -r requirements.txt
Requirement already satisfied: fastapi==0.115.6, uvicorn==0.34.0,
numpy==2.2.1, pytest==8.3.4, httpx==0.28.1
```

另以全新、无 system-site-packages 的临时虚拟环境执行了
`python -m pip install -r requirements.txt`，依赖版本检查通过，并在该环境重跑全量测试：

```text
clean venv OK 0.115.6 0.34.0 2.2.1 8.3.4 0.28.1
33 passed, 196 warnings in 3.36s
```

## 自动化测试

命令：

```bash
./scripts/test.sh
```

结果：

```text
.................................                                        [100%]
33 passed, 196 warnings in 2.99s
```

警告均来自 Python 3.14 下 Starlette/FastAPI 对 `asyncio.iscoroutinefunction` 的上游弃用提示，不影响测试结果。

覆盖项包括：

- 二进制 little/big endian 往返、跨格式往返、v1 缺省补齐。
- CRC 损坏、非法长度前缀、未知必填标志、高版本、JSONL 单位越界。
- 四种时间戳舍入策略、逐记录舍入位、累计整数误差。
- 最近秩分位、丢包率固定小数、补偿求和误差上界、重复计算摘要一致。
- 样本不足返回 `E_INSUFFICIENT_SAMPLES` 且无零值伪分位。
- 真实 echo 成功、真实拒绝、真实 hang 超时、套接字关闭。
- 探测记录写入 SQLite 后导出两种格式再导入，核心字段无漂移。
- 并发重复批次只提交一次、内容冲突、快照版本一致性。
- 子进程持有未提交事务后被 `SIGKILL`，重启恢复到最后完整版本。
- HTTP 预检、导入、幂等复用、拒绝不改版本、导出再导入、页面可访问。

## 端到端演示

命令：

```bash
./scripts/demo.sh
```

实际输出摘要：

```text
[PASS] binary v2 preflight accepted · decision=accept records=21
[PASS] cycle 1 import committed · version=1 records=21
[PASS] cycle 1 exported both formats · binary=DXPK; jsonl=21 lines
[PASS] cycle 2 JSONL re-import committed · version=2 records=21
[PASS] export/re-import normalized core fields are identical · records 42 = 2 × 21
[PASS] binary v1 degraded acceptance · missing_rtt=2 missing_payload=2
[PASS] corrupt CRC returns E_FRAME_CORRUPT · HTTP 422 E_FRAME_CORRUPT
[PASS] bad length prefix returns E_FRAME_CORRUPT · HTTP 422 E_FRAME_CORRUPT
[PASS] high version returns E_FORMAT_UNSUPPORTED · HTTP 415 E_FORMAT_UNSUPPORTED
[PASS] rejected imports do not change committed version · version=3
[PASS] real TCP echo probe succeeds · rtt=395us closed=True
[PASS] real connection refusal is classified · HTTP 502 E_CONNECTION_REFUSED closed=True
[PASS] real hang probe returns E_PROBE_TIMEOUT and closes socket · HTTP 504 E_PROBE_TIMEOUT closed=True
[PASS] local test service throughput/error counters persisted · completed=1 errors=0 throughput=157.248B/s
[PASS] loss/jitter statistics are deterministic · loss=16.67% p95=4500us jitter=554.64us
[PASS] root-cause candidates expose numeric ranking evidence · top=local-edge-b score=0.346944
[PASS] two-path comparison uses declared relative tolerance · loss_equal=False
[PASS] forced-termination marker published
force-killed pid=1240522 after intent a15f1e701a364a72bf10749b6df465d2 with uncommitted v8
[PASS] restart rolls back to last complete version and reports differences · v7->v7 records=49 diff=-3
[PASS] interrupted batch is durably marked aborted · status=aborted code=E_BATCH_ABORTED
```

演示最终结论：

- 格式协商：`compact-binary v2` → `accept`。
- 旧版裁决：`degrade_accept`，缺 RTT 2 条、缺载荷 2 条均有明细。
- 两次导入/导出：源 21 条 → 导入导出 → JSONL 再导入；归一化核心字段多重集合严格等于原始集合的两倍。
- 真实探测：`echo`、`refuse`、`hang` 三个本地测试服务模式分别产生成功、拒绝、超时，均走实际套接字路径且 `socket_closed=true`。
- 统计：`local-edge-a` 丢包率 `16.67%`，P95 `4500µs`，抖动均值 `554.64µs`；两次查询摘要一致。
- 根因：`local-edge-b` 排名第一，分数 `0.346944`，失败类型计数与丢包/抖动数值可核验。
- 恢复：强制终止时观察到未提交 `v8`；重启后仍为最后完整 `v7`，记录数不变；该批次 `planned=3, committed=0, difference=-3`，批次状态 `aborted`。

完整机器可读报告：`artifacts/demo/e2e_report.json`。演示还会留下两次导出的二进制/JSONL 文件与恢复 marker，便于复核。

## 页面启动检查

`GET /` 返回 `200` 且包含 `Loopback Margin Lab`；静态 CSS/JS 由 FastAPI 本地提供，无 CDN。端到端演示通过真实 Uvicorn 进程访问所有关键 API。

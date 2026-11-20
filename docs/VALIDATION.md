# 实际验证记录

验证日期：2026-11-21（Asia/Shanghai）  
宿主：WSL2 Linux，Python 3.14.4，SQLite WAL，本地回环 uvicorn  
不使用 Docker、外部推理服务或外部样本数据。

## 1. 自动化测试

命令：

```bash
.venv/bin/python -m pytest -q
```

结果：

```text
.................................                                        [100%]
33 passed
```

覆盖内容：

- `512Mi`、`2Gi`、`1500us`、`3ms`、小数单位与 ns 整数微秒归一化；
- 负数、未知后缀、非整数微秒、int64 溢出；
- 未知模型、量纲异常、延迟上限小于单样本代价、到达时间非单调；
- 模型/优先级隔离、样本上限、延迟切批、确定性摘要；
- nearest-rank 秩和空样本 `E_INSUFFICIENT_SAMPLES`；
- 补偿求和、半入舍入位与累计误差；
- 正常、碎片、漂移样本的 HTTP 统一错误与持久化；
- 强杀检查点后重启扫描、运行/批次中止、恢复报告与禁止伪分位。

测试过程中仅有 Starlette `TestClient` 对 `httpx` 的上游弃用提示，不影响结果。

## 2. 真实 uvicorn 强杀与重启

命令：

```bash
.venv/bin/python scripts/run_demo.py
```

脚本实际执行结果：

```text
normal_state: COMMITTED
normal_batches: 5
fragment_events: 2
mixed_rejections: 2
drift_code: E_PRECISION_DRIFT
invalid_code: E_SPEC_INVALID
crash_exit_code: 86
recovered_pending: 1
aborted_state: ABORTED
aborted_quantiles_code: E_INSUFFICIENT_SAMPLES
```

完整机器可读证据：`artifacts/demo_results.json`。

关键事实：

- 正常运行：5 批，运行级 P50 `40.00ms`、P95/P99 `48.00ms`，`sample_count=5`。
- 第一个正常批：成员级 P50 `12.00ms`、P95/P99 `13.50ms`，`sample_count=4`。
- 碎片样本：6 批、2 次显存碎片降批；降批前后 bitmap 不同。
- 混批候选内存 `1342177280` 字节可装入 `1363148800` 字节区域，但合计延迟 `72500µs` 超过 `21000µs` 上限，记录 `MIXED_BATCH_REJECTED_LATENCY_CAP`。
- 漂移样本：HTTP `409`，运行及批次持久化为 `VALIDATION_FAILED`，具体失败字段为批延迟与 P50/P95/P99。
- 非法量纲：HTTP `422`，`E_SPEC_INVALID`，`written=false`。
- 强制终止：在检查点 `BATCHES_PERSISTED_QUANTILES_PENDING` 后退出码 `86`。
- 重启恢复：发现 1 个未完成意图；运行 `ABORTED`，5 个批次 `ABORTED`，运行分位查询返回 `E_INSUFFICIENT_SAMPLES`。

## 3. 页面

使用本地 Playwright Chromium，以 `1600x1000` 视口和全页模式访问回环地址：

```bash
npx playwright screenshot --browser chromium --color-scheme dark \
  --viewport-size "1600,1000" --full-page \
  --wait-for-timeout 2200 --wait-for-selector "#batchRows tr[data-batch]" \
  http://127.0.0.1:8877/ artifacts/diagnostic-page.png
```

产物：

- `artifacts/diagnostic-normal.png`：正常轨迹，展示时间轴、显存水位、5 批组成、P50/P95/P99 与真值核对。
- `artifacts/diagnostic-page.png`：碎片轨迹，展示降批前后 bitmap、混批延迟拒绝、误差传播链与意图日志。

页面目检确认：

- 批次色条按持久化 `timeline_start_us/timeline_end_us` 正确渲染；
- 显存位图区分 free/reserved/fragment/allocated；
- 分位卡片显示 `n`、nearest-rank 秩与舍入位；
- 真值表逐成员、逐批、逐分位显示绝对/相对偏差和 PASS；
- 恢复区明确展示事实边界，不宣称强杀后存在优雅退出报告。

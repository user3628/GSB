# 实际验证记录

验证日期：2026-11-23（Asia/Shanghai）

## 环境

```text
Python 3.11.18
SQLite 3.53.1（Python 标准库 sqlite3）
fastapi 0.165.0
uvicorn 0.54.0
pydantic 2.14.4
numpy 2.4.6
pytest 9.2.0
```

本地虚拟环境为 `.venv`，依赖由 pip/uv 从可用 wheel 安装，项目通过 `pip install -e . --no-deps` 安装。未使用 Docker 或外部服务。

## 自动化测试

命令：

```bash
.venv/bin/python -m pytest -q
```

结果：`19 passed`。

覆盖项：

- Q9 最大余数归一化精确求和且插入顺序无关。
- Q 乘除的最终 half-even 舍入与有理数误差。
- 300ms 到 7,200,000ms 到期时刻、余量比例和 `+123ms` 时钟偏移。
- 混合大数小数样本：朴素浮点误差 86Q，Kahan 对照误差 0，并输出上界公式。
- 获取、竞争、版本单调、陈旧续约不延长、到期再获取、释放、权重/占用率、再平衡容差边界。
- 固定样本 4/5 占用、3 条再平衡建议、公平度 `0.872758620`、占比和精确为 `1.000000000`。
- 候选正序/逆序仲裁对象逐字段一致；容差内按 `holder_id` 字典序。
- HTTP 含 `/` 锁键、业务码、409 冲突、422 校验、指标/诊断/样本接口。
- 3 个独立进程同时竞争同一锁，只有 1 个 OK，2 个 E_LOCK_HELD。
- 两个线程使用同一 request_id，响应完全相同。
- `os._exit(91)` 在意图持久化后终止：恢复报告扫描 1 条、记录授予回滚、锁可用。
- 人工构造“持久化续约行 + prepared 意图”：恢复把到期时刻与续约次数恢复旧值。
- 人工构造“持久化授予行 + prepared 意图”：恢复删除授予行、版本回退、锁可用。

## 固定样本演示

命令：

```bash
.venv/bin/python scripts/demo.py
```

输出：`artifacts/demo_report.json`。

关键实测值：

```json
{
  "arbitration_order_independent": true,
  "arbitration_winner": "holder-b",
  "granted_holder": "holder-b",
  "fixed_sample_matches_persisted_rows": true,
  "metrics_after_occupancy": "1.000000000",
  "occupancy": "0.800000000",
  "occupied_keys": 4,
  "rebalance_suggestion_count": 3,
  "registered_keys": 5,
  "repeat_metrics_bit_identical": true
}
```

真实强制终止：

- 授予故障子进程退出码 91；恢复后 `crash-lock` 状态为 `available`，恢复动作报告 `lock_available_after=true`。
- 续约故障子进程退出码 91；恢复前后到期时刻均为 `1700000030000`，未延长期限。
- 两个恢复报告均声明 `graceful_exit_report_guaranteed=false`，报告范围仅限重启后 SQLite 已提交事实。

## 数值对照

固定压力样本为 1 个大项加 100 个 1Q 小项：

```text
canonical = 10000000.000000100
naive sum = 10000000.000000186  (偏差 86Q)
Kahan     = 10000000.000000100  (偏差 0Q)
naive bound = 1.11022302462518e-07
```

TTL 样本跨度 `7,200,000 / 300 = 24,000` 倍，满足跨三个数量级要求。


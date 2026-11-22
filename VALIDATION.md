# 实际验证记录

验证环境：Linux 开发容器，Python 3.14.4（满足 Python 3.10+）。实现仅使用标准库，
所有服务绑定 `127.0.0.1`，未访问外部网络或真实数据库。

## 自动测试

命令：

```bash
python3 -m unittest discover -s tests -v
```

结果：`Ran 41 tests`，`OK`。完整输出保存在
[`artifacts/test_output.txt`](artifacts/test_output.txt)。

覆盖范围：

- 二进制小端/大端、v1 降级、v2 双向互操作、v3 拒绝。
- CRC、头部长度、记录长度、截断、JSONL 记录数/记录 CRC 异常。
- 负数与 `2^63` 行数溢出，失败后旧快照保持。
- 等值/范围/自连接选择率边界、相关系数 0/1、无笛卡尔、搜索超限、对数封顶。
- 定点代价分解一致、容差字典序平局、同请求逐位一致。
- 二进制/JSONL 计划核心一致、快照整体替换、旧计划仍绑定旧版本。
- 请求冲突、并发重复提交只复用结果、构建期间版本替换完成在绑定版本。
- 未完成计划意图、未完成统计意图、悬空当前指针恢复。
- HTTP 回环、诊断页、统一错误 JSON、404/409/410/422。

## 固定样本互操作

`samples/manifest.json` 实际结果：

- `interoperable_v2: true`
- v2 二进制与 JSONL 语义摘要：
  `2f0468d626ae4fbf094cffb6381ba5a196889e628a8fbea695f7b074382fa635`
- v1 二进制：`DEGRADE_ACCEPT`，7 条降级明细。
- v1 JSONL：`DEGRADE_ACCEPT`，7 条降级明细。
- v3 JSONL：预期 `E_FORMAT_UNSUPPORTED` + `REJECT` 导入报告。

## 端到端演示

命令：

```bash
python3 scripts/demo.py
```

结果：`ok: true`。完整 JSON 保存在 [`artifacts/demo_report.json`](artifacts/demo_report.json)。

同一 v1001 统计快照的最优顺序：

```text
order_items -> orders -> customers -> products -> returns -> shipments
```

定点代价分解：

```text
scan                = 6860000.000000
join                = 1019848.800000
intermediate_write  = 5.000000
total               = 7879853.800000
```

演示核验结果：

- 二进制和 JSONL 导入均为 `ACCEPT`，语义 digest 相同，计划核心逐位相同。
- 注入 CRC 损坏后 HTTP `422`、`E_FORMAT_CORRUPT`，当前版本仍为 1001。
- 导入 v3 后 HTTP `422`、`E_FORMAT_UNSUPPORTED`，SQLite 中记录 `REJECT` 报告。
- 导入 v1002 替换后当前版本为 1002；v1001 的旧计划仍可读取且绑定 1001。
- 构建过程中真实 `kill` 服务进程；重启后扫描 1 条未完成意图，`crash-request`
  变为 `ABORTED`，再次查询返回 HTTP `410` / `E_REQUEST_ABORTED`。
- 恢复前后当前快照均为 1001，统计快照未被半提交覆盖。

## 复现方式

```bash
python3 samples/generate_samples.py
python3 -m unittest discover -s tests -v
python3 scripts/demo.py
python3 scripts/joincost.py serve --db artifacts/joincost.db --port 8765
```


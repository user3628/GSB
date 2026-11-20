# 实际验证记录

验证日期：2026-11-21（Asia/Shanghai）

环境：Python 3.14.4，项目本地 `.venv`（`--system-site-packages`，`pip --no-index`），numpy 2.5.5、PyYAML 6.0.3、Flask 3.1.3、pytest 9.2.0。

## 1. 自动化测试

执行：

```bash
.venv/bin/python -m pytest -q
```

结果：

```text
...........................                                              [100%]
29 passed
```

覆盖点：

- NFKC、空白折叠、全半角、小写、字符偏移、中文单字/二字组合、拉丁连续串。
- 词项编号和序列的重复运行字节一致性。
- 空查询/空文档 `E_EMPTY_TEXT` 诊断保留。
- 桥接映射、字符 n-gram、BM25 参数、定点缩放、舍入与误差元数据。
- `E_NUMERIC` 的负权重、NaN、Inf、零缩放因子四种路径，错误包含 pair、term、stage。
- 分数截断、lexical/bridge 对数贡献守恒。
- Decimal 高精度参考路径与解析误差界交叉核对。
- 四档、混淆矩阵、分桶偏移、边界偏移、越界清单。
- 文本、桥接词典、量化位宽、平滑、截断、阈值的最早失效阶段。
- 影子全量逐字节比较与 `E_INCREMENTAL_MISMATCH`。
- 停止/恢复后 `build_id` 与从零全量相同、检查点连续。
- 检查点重复、落后、跨阶段错位、缺失前置阶段。
- POSIX/Windows 双分支文件锁实现；Windows 分支用标准库 `msvcrt.locking` 模拟验证。
- 4 个独立进程同时发布同一检查点：仅 1 个成功，其余 3 个均返回 `E_STALE_CHECKPOINT`。
- HTTP 状态、分档、误差报告、导出、编辑重算、停止/恢复、`E_NUMERIC`。
- 未使用桥接条目的修改只重建聚合摘要且仍与全量一致。

## 2. 固定种子数据重建

```bash
.venv/bin/python scripts/generate_demo.py --out data --seed 20261121 --force
```

结果：

| 分片 | pair 数 | SHA-256 |
|---|---:|---|
| `part-001` | 8 | `6c5aa42a06a90756efa3583a7f457cfe44e19145e550610635668c734b519f76` |
| `part-002` | 8 | `b9cb94e16dd4be3463ebadc775afb362e7dc8e29db93fe42d0f79db99103186c` |
| `part-003` | 8 | `fc2e543f5e38d0e6193e909bff40055e8bfd752d6f600373d108669cc909026a` |

桥接词典：28 条，摘要 `b24273d95684ec86e694e04b20f0177b53e0adef9540f24ee8d1961532a9963f`。

## 3. 全量基线

```bash
.venv/bin/python -m xrel_workbench demo --seed 20261121
```

初始全量构建：

- `build_id = d4ece83229f613bfca9f2b26110c71a0ea5792843487d79980b042342f9e138d`
- 3 分片、24 pair、24 个有档位结果。
- 检查点序号 `1..13` 连续。
- 档位计数 `[3, 2, 3, 16]`。
- `review_required = true`：由分布偏斜、边界样本、空文本与刻意人工偏移触发。

## 4. 停止/恢复

在第 4 个不可拆分分片单元后停止：

- 停止时检查点序号：`4`。
- 清空停止请求后恢复。
- 最终 `build_id = d4ece83229f613bfca9f2b26110c71a0ea5792843487d79980b042342f9e138d`，与从零全量完全相同。
- 检查点 `1..13` 连续，无重复/缺口。
- 影子全量对比：`difference_count = 0`。

## 5. 词典编辑后的增量重建

编辑桥接条目 `br-003`（“检索”）：权重 `0.92 -> 0.71`。

- 最早失效阶段：`part-001`、`part-002`、`part-003` 均为 `stage2`。
- 复用单元：3（全部 `stage1`）。
- 重建单元：10（3×stage2 + 3×stage3 + 3×stage4 + 1×聚合）。
- 增量最终 `build_id = 312fe77bebd629cbd5eb8b7367412c0d16de083c998485c4958a35a8feadeb99`。
- 检查点 `1..23` 连续。
- 与同输入从零全量构建逐字节比较：`difference_count = 0`、`match = true`。
- 比较对象：3 个分档 JSONL、误差报告、校验报告、汇总摘要、导出 JSONL。

## 6. 误差与分档数值

- 24/24 pair 的高精度参考交叉核对通过。
- 24/24 pair 的解析误差界路径核对通过。
- `error_crosscheck_review_pairs = []`。
- 最大误差上界：`0.01953125`。
- 分数有效位数：最小 `2`，最大 `12`（零分数/零误差路径取配置上限）。
- 语料误差趋势：累计最大误差界斜率约 `-4.95e-19`；累计平均误差界斜率约 `-7.12e-5`，估计 24 pair 平均上界 `0.006659613715277776`。
- 截断验证：`p0012` 分数为 `6.0`，等于配置上限。

待复核样本：

- `p0005`：分数接近档位边界误差带。
- `p0008`、`p0023`：空查询/空文档，显式保留诊断。
- `p0022`：人工等级与模型档位偏移超过配置阈值。
- `p0024`：分数接近档位边界误差带。

## 7. 输出位置

- 初始全量摘要：`artifacts/demo/full_baseline_summary.json`
- 最终增量摘要：`artifacts/demo/final_incremental_summary.json`
- 最终误差报告：`artifacts/demo/final_error_report.json`
- 最终校验报告：`artifacts/demo/final_validation.json`
- 最终导出：`artifacts/demo/final_scores.jsonl`
- 机器可读验收报告：`artifacts/demo/verification_report.json`
- 当前增量工作区：`artifacts/demo/workspace`
- 停止/恢复工作区：`artifacts/demo/stop-resume-workspace`

## 8. 本地 HTTP 探测

实际启动：

```bash
.venv/bin/python -m xrel_workbench serve \
  --workspace artifacts/demo/workspace --host 127.0.0.1 --port 8765
```

探测结果：

- `GET /api/health`：HTTP 200。
- `GET /api/grades`：24 条，首条 `p0001 = 强相关`。
- `GET /api/errors`：24 条误差记录，交叉核对待复核列表为空。
- `GET /`：工作台 HTML 页面 HTTP 200。
- `GET /api/export/scores`：24 行 JSONL。
- `POST /api/verify`：`match = true`、`difference_count = 0`、比较 7 个正式输出文件。

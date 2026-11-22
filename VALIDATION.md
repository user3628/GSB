# 实际验证记录

- 日期：2026-11-23（Asia/Shanghai）
- 环境：WSL2 Linux `6.18.58.1-microsoft-standard-WSL2`、CPython `3.14.4`、NumPy `2.2.1`
- 命令：`python3 -m unittest discover -v` 与 `python3 scripts/run_validation.py`
- 结论：全部通过；机器可读报告为 `VALIDATION_RESULTS.json`

## 单元测试

`27` 项测试通过，覆盖多边形面积/包络框/自交/凹多边形交集/越界策略、两种确定性匹配仲裁、N/A 口径、全局预算与单 worker 配额、串并行一致性、停止恢复、增量失效、增量-全量不一致错误、旧 worker 发布保护、worker 异常保留、HTTP 回环与非法阈值。

## 串行 vs 并行

实际候选评估数均为 `145`。三个导出文件逐字节一致：

| 导出 | SHA-256 |
|---|---|
| `matches.json` | `06bf5e9b2d1b1a4eba2c23a0b4885718585a150e39bb9d65d21b053a12a84b4e` |
| `aggregates.json` | `7c77ad5cc5af02d38bc88c8ec022dedd5a1ca1bf5654c2f7b1d8057f1d5034bd` |
| `schedule.json` | `3262426ad58ee7569dc4bcc2b79c29713066415d65dc7050ae2e2cfda1595e6a` |

## 停止与恢复

以 `workers=2`、`stop_after_samples=1` 运行后状态为 `stopped`，停止原因为 `stop_after_samples_reached`。当时部分分片分别停留在 `decode`、`match`、`aggregate` 的未完成边界，没有发布半阶段结果。执行 `resume` 后：

- `matches.json` 与全量构建相同：`06bf5e9b2d1b1a4eba2c23a0b4885718585a150e39bb9d65d21b053a12a84b4e`
- `aggregates.json` 与全量构建相同：`7c77ad5cc5af02d38bc88c8ec022dedd5a1ca1bf5654c2f7b1d8057f1d5034bd`

## 增量更新

修改 `shard-0003` 中一个预测置信度后，查询到最早失效阶段为 `decode`，原因 `SHARD_CONTENT_CHANGED`，影响 `decode -> match -> aggregate`。`update-shard` 实际只重建这三个阶段，其他分片检查点复用。增量与临时全新全量构建：

- matches：两者均为 `23095867f9d92ec0c2aa29a135bb97273124ec469c21bd8dcb6dedc976e7d3d9`
- aggregates：两者均为 `0807d1904f32b3421016b7808619f3bdb9c96a78fbd5ddd492c9f22291b5cc90`

显式注入不一致后返回 `E_INCREMENTAL_MISMATCH`，首个差异为 `matches` JSON 的字节偏移 `112`（第 1 行、第 113 列），并给出左右上下文。

## 预算裁决

配置：`max_evaluations=180`、`per_worker_quota=100`、`max_workers=2`。

- 接纳：`shard-0001` 预估 `10`（lane 0）、`shard-0002` 预估 `47`（lane 1）。
- 未处理：`shard-0003` 预估 `203`、`shard-0004` 预估 `154`，均为 `E_GLOBAL_BUDGET_EXHAUSTED`。
- 预留预算 `57 <= 180`；实际候选评估 `28 <= 180`。

## 检查点与错误码

- 4 个并发 worker 发布检查点，获得唯一且连续的序号 `1,2,3,4`，不可变 journal 与当前快照均为完整 JSON。
- 缺失前置阶段：`E_INVALID_STAGE`。
- 重复发布：`E_STALE_CHECKPOINT`。
- 非法 IoU 阈值 `1.7`：`E_INVALID_IOU_THRESHOLD`。
- 负预算 `-1`：`E_INVALID_BUDGET`。
- 单 worker 异常：其他 worker 已完成结果保留，异常以 `E_WORKER_CRASH` 结构化记录。

## 聚合实例

4 个分片共 34 个有效真值、93 个有效预测：31 命中、3 漏检、22 重复预测、40 背景误报、3 个无效预测；平均 IoU `0.887768584`，命中率 `0.911764706`。

## 环境与回环

`doctor` 检查 Python、NumPy、文件锁、`spawn` 进程池、同目录原子替换与 `127.0.0.1` 绑定全部通过。HTTP 入口页与 `/api/status` 返回 200；通过 HTTP 注入非法阈值返回 `E_INVALID_IOU_THRESHOLD`。实际验证环境为 WSL2/Linux；Windows 的 `msvcrt` 文件锁与 `spawn` 路径已实现，但当前容器无法在真实 Windows 内核上复验。


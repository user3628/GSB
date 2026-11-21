# 时序预测滚动回测与并行调度工作台 — 技术说明

## 1. 目标与边界

工作台在单机本地运行，仅依赖 `numpy` 与 `PyYAML`（标准库承担 CSV、HTTP、
多进程、文件锁与 JSON 持久化），不访问外部服务、不使用 GPU 或容器。
所有随机数据由固定种子的 `numpy.random.RandomState` 生成，因此样本、预测、
指标、检查点与报告均可复现，串行与并行执行逐字节一致。

## 2. 目录结构

```
src/
  errors.py        稳定业务错误码 E_* 与结构化错误对象
  config.py        YAML 配置、校验、规范 JSON 与配置指纹
  models.py        三种确定性预测方法与预测区间
  data.py          CSV 分片加载、清单校验、内容指纹
  windows.py       滚动折切分、缺失/突变打标、序列级业务错误
  scheduler.py     全局预算/单进程配额/并发上限三重裁决
  engine.py        spawn 进程池、协作停止、确定性归约
  checkpoints.py   原子写、跨平台文件锁、单调序号、停止标记
  pipeline.py      阶段门控、指纹失效传播、检查点复用与恢复
  metrics.py       MAE/RMSE/MAPE/覆盖率与逐折结果
  report.py        JSON/CSV/自包含 HTML 诊断工作台
  cli.py           命令行入口
  http_app.py      仅绑定 127.0.0.1 的 HTTP 接口
scripts/
  generate_samples.py  固定种子样本生成器
  demo.py              端到端验收演示
  run_tests.py         测试启动器
tests/               unittest 套件（42 个用例）
samples/             3 个 CSV 分片 + manifest.json
config/              回测 YAML（常规 + 紧预算）
```

## 3. 三种预测方法（确定性求解）

- `moving_average`：窗口均值作为水平参数，多步预测为同一常数，残差标准差
  采用总体方差（含零均值），无随机分支。
- `exponential_smoothing`：带阻尼的 Holt 线性指数平滑，显式递推
  `level/trend`，阻尼预测 `level + trend * Σ φ^k`，参数 `alpha/phi` 来自配置。
- `linear_ar`：以滞后矩阵一次性最小二乘 `np.linalg.lstsq` 求解截距与系数，
  多步预测递归使用自身预测值；秩亏窗口由 `lstsq` 确定性处理。

所有计算强制 `float64`，预测区间为 `point ± z(confidence) * σ`，其中标准正态
分位数使用 Acklam 有理逼近（无第三方统计依赖）。

## 4. 缺失点与突变点

- 清单声明每分片时间戳范围与步长，加载时还原规则网格。网格上无 CSV 行记为
  “缺失时间戳”，有行但目标为空记为“缺失目标值”。
- 序列缺失率 `(缺失时间戳 + 缺失目标)/网格长度` 超过 `max_missing_ratio`
  返回 `E_MISSING_RATIO`；网格短于一个窗口或无任何完整折返回
  `E_SERIES_TOO_SHORT`，两者均带 `series_id` 与上下文，绝不静默产生空预测。
- 折 = 连续 `window_length` 训练槽 + `horizon` 测试槽，按 `step` 重叠推进。
  任一槽缺失则该折被剔除并记录 `MISSING_TRAIN_SLOT/MISSING_TEST_SLOT`。
- 突变点按相邻有效值相对变化 `|Δ|/max(|prev|,1e-12) ≥ changepoint_threshold`
  标记，折记录 `has_changepoint` 供诊断，不静默丢弃数据。

## 5. 预算约束调度

调度单元为“序列折”，按 `(shard_id, series_id, fold_index)` 规范序接纳。
接纳前依据清单行数/时间范围与窗口参数估算折数，再依次施加：

1. `global_fit_budget`：累计拟合永不超过上限，超出折记 `E_BUDGET_EXCEEDED`；
2. `worker_fit_quota`：每个工作进程配额，耗尽记 `E_WORKER_QUOTA`；
3. `max_workers`：进程池大小与并发度。

工作进程选择为“最少负载、再按最小 worker id”，因此同样输入下 worker 分配
完全确定。任何拒绝都发生在拟合启动之前。

## 6. 并行执行与确定性归约

- 使用 `multiprocessing.get_context("spawn")`（Windows 默认方式），worker
  为顶层纯函数，载荷可 pickle，Windows/Linux 行为一致。
  注意：使用 spawn 时入口脚本必须包含 `if __name__ == "__main__":` 保护。
- 停止为协作式：观察到停止标记或折数预算后，仅排空在途折、不再提交新折；
  未启动折以 `E_STOPPED` 记录。
- 单折时间预算在拟合后以显式截止检查，超时返回结构化 `E_TIME_BUDGET`，
  不把进程描述为失控终止；worker 异常捕获为 `E_WORKER_FAILED`，保留其余结果。
- 结果始终按 `(shard_id, series_id, fold_index)` 排序后归约。墙钟计时等
  非确定字段不进入检查点与指纹，因此并行/串行输出逐字节一致。

## 7. 检查点、停止恢复与发布保护

- 分片为原子检查点单元：临时文件 + `flush/fsync` + `os.replace`，任意中断
  都不会留下半写记录（测试用 `crash_after_temp` 钩子验证）。
- 记录含分片标识、单调序号、输入内容指纹、配置版本/指纹、完成折数、
  产物摘要指纹与逐折结果。
- 工作区锁位于 `state/.workspace.lock`，Windows 走 `msvcrt`、其他平台走
  `fcntl`。发布在锁内比较序号：仅允许 `+1` 连续推进，落后/重复/跳跃/
  指纹漂移均返回 `E_STALE_CHECKPOINT`，并校验前置分片存在以防跨分片错位。
- 恢复时仅当分片指纹匹配且已完成折数等于接纳折数才复用；部分分片的陈旧
  检查点被删除重建。最终清理瞬态停止字段，保证与一次完整运行逐字节一致。

## 8. 版本指纹与增量失效

- 配置指纹 = 规范 JSON 的 SHA-256，覆盖方法、窗口、步长、horizon 与预算等。
- 分片输入指纹 = `SHA256(分片内容指纹, 配置指纹)`；清单另有整体清单指纹。
- 配置或任一分片内容变化时，定位最早受影响分片，将其及全部下游分片检查点
  与聚合产物失效重建，未受影响分片继续复用；失效原因、前后指纹写入
  `invalidation` 并可通过 CLI/HTTP 查询。

## 9. 阶段门控与错误码

阶段线性推进：`ingest -> plan -> schedule -> execute -> checkpoint ->
aggregate`。未知阶段、跳过缺失前置阶段或在未完成时请求聚合，返回
`E_INVALID_STAGE`；配置越界返回 `E_CONFIG_INVALID`。

| 错误码 | 触发场景 |
| --- | --- |
| `E_MISSING_RATIO` | 序列缺失比例超阈值 |
| `E_SERIES_TOO_SHORT` | 序列短于窗口或无完整折 |
| `E_BUDGET_EXCEEDED` | 全局拟合预算耗尽 |
| `E_WORKER_QUOTA` | 单工作进程配额耗尽 |
| `E_TIME_BUDGET` | 单折拟合超过时间预算 |
| `E_WORKER_FAILED` | worker 内异常被结构化捕获 |
| `E_STALE_CHECKPOINT` | 落后/重复/跳跃/错位/指纹漂移发布 |
| `E_INVALID_STAGE` | 非法阶段跳转或前置阶段缺失 |
| `E_CONFIG_INVALID` | 配置取值越界或未知键 |
| `E_MANIFEST_MISMATCH` | 清单行数/指纹/序列集合与文件不符 |

## 10. 指标与诊断工作台

- 聚合与按序列/按分片分组：MAE、RMSE、MAPE（零分母显式跳过并计数，
  全部为零时 MAPE 为 `null`）、区间覆盖率。
- 导出 `report.json`、`error_curves.csv`（逐折逐步误差曲线）、
  `budget_details.csv`（接纳/拒绝/未启动明细）、自包含 `dashboard.html`
  （无外部资源，含指标表、预算、失效、业务错误与误差曲线画布）。

## 11. HTTP 接口（仅回环）

服务只允许绑定 `127.0.0.1/localhost/::1`，绑定非回环地址直接抛错。
长任务在守护线程执行，停止/状态接口保持可用。若本机常用端口被占用，
用 `--port 0` 让系统分配空闲端口。

`GET /health /api/status /api/fingerprints /api/unprocessed
/api/checkpoints /api/metrics /api/budget`；
`POST /api/run /api/resume /api/stop /api/reset /api/export`。

## 12. Windows 10/11 + Python 3.11 运行方式

```powershell
py -3.11 -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
python scripts\generate_samples.py
python -m src.cli --workspace workbench run --export
python -m src.cli --workspace workbench status
python -m src.http_app --port 8765
python scripts\run_tests.py
```

本仓库实际验证环境为 WSL2 Linux + Python 3.14（无容器），代码仅使用
跨平台标准库 API 与 spawn 多进程，兼容 Windows 10/11 + Python 3.11。

## 13. 实际验证结果

- `python scripts/run_tests.py`：42 个 unittest 全部通过。
- `python scripts/demo.py`：串并行逐字节一致；停止后恢复与完整运行逐字节
  一致；紧预算下接纳数不超过全局上限且拒绝折带原因码；四类报告成功导出。
- 三种方法均完成端到端回测，样本覆盖 4 条健康序列（含日周期、趋势、
  缺失时间戳/目标、突变点）以及 2 条触发业务错误的异常序列。

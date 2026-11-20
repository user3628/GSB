# qoplib 架构与技术说明

低精度量化算子库，面向“预算约束下的分片并行执行”。纯 Python 标准库 +
NumPy/PyYAML，无 GPU、无网络服务、无容器依赖。

## 1. 模块结构

| 模块 | 职责 |
| --- | --- |
| `qoplib/errors.py` | 稳定业务错误码与可序列化 `StructuredError` |
| `qoplib/fingerprints.py` | SHA-256 内容指纹、确定性 JSON 编码 |
| `qoplib/io_util.py` | 原子写入（临时文件 + fsync + rename）、确定性 `.npz` |
| `qoplib/filelock.py` | 跨平台文件锁（Windows `msvcrt` / POSIX `fcntl`） |
| `qoplib/quantization.py` | 量化/反量化、缩放因子与零点、钳制审计、量化域线性算子 |
| `qoplib/ops.py` | 量化线性算子链与高精度参考实现、误差诊断 |
| `qoplib/manifest.py` | YAML 算子清单加载、校验、元素/工作集估算 |
| `qoplib/scheduler.py` | 三重预算准入与并发度/泳道调度 |
| `qoplib/checkpoint.py` | 版本化原子检查点、单调序号、并发发布保护 |
| `qoplib/pipeline.py` | 分片四阶段流水线与增量失效传播 |
| `qoplib/worker.py` | 进程池顶层可 pickle 工作入口（spawn 兼容） |
| `qoplib/runner.py` | 并行/串行运行、优雅停止、确定性归约、发布队列 |
| `qoplib/audit.py` | 状态/指纹查询与审计报告导出 |
| `qoplib/cli.py` | 命令行接口 |

## 2. 量化语义

- 位宽：4 bit、8 bit；模式：对称 / 非对称；粒度：逐张量 / 逐通道。
- 对称模式使用带符号整数区间（8bit `[-127,127]`、4bit `[-7,7]`），零点固定
  为 0；非对称模式使用无符号区间（`[0,255]` / `[0,15]`），零点由最小值推导。
- 权重形状为 `(out_features, in_features)`，逐输出通道沿 `in_features` 轴
  （2D 即 axis=1）缩减，得到每个输出通道一个 scale。
- 量化域矩阵乘先做整数累加，再按缩放因子还原浮点；逐通道激活采用逐项缩放
  加权求和，避免假设统一 scale。
- 钳制统计记录次数、上下溢计数与完整坐标位置；极值自适应 scale 下不钳制，
  外部约束 scale 时的钳制路径有专门测试覆盖。

## 3. 四阶段流水线与版本指纹

每个分片依次经过 `validate -> quantize -> linear -> verify`：

1. `validate`：输入内容指纹、形状、NaN/Inf 检测（仅依赖输入与形状声明）。
2. `quantize`：权重/激活量化、钳制审计（依赖量化位宽/模式/粒度）。
3. `linear`：量化域算子链与输出指纹（依赖量化阶段指纹）。
4. `verify`：与 float64 参考实现比对、逐层误差与最紧余量（依赖容差配置）。

各阶段指纹由其输入内容与配置的规范化 JSON 哈希构成。任一上游变化只使最早受
影响阶段及其下游失效，未受影响上游继续复用；失效原因、新旧指纹在流水线返回
值与检查点记录中可观测。

## 4. 三重预算调度

- `total_element_budget`：累计计算元素（MAC 数）硬上限，任何时刻不超。
- `worker_element_quota`：单分片不得超过单工作线程配额。
- `peak_memory_bytes`：并发工作集峰值上限，同时决定最大进程数。

准入在调度期与派发前各检查一次（`enforce_runtime_admission` 是第二道防线）。
超预算分片进入 `rejected` 并携带结构化原因码与预算明细，不静默降级。

## 5. 确定性、并行与归约

- 全局序号是 `(shard_index, stage)` 的纯函数：`index*4 + stage_order`，
  因此串行与并行产生完全相同的序号。
- 工作进程只计算并返回待发布记录；父进程的 `DeterministicPublisher` 按全局
  序号 0,1,2,... 连续提交，配合文件锁实现并发发布保护。
- 归约严格按分片序号排序，摘要经规范化 JSON（排序键、固定分隔符、无时间戳）
  写入，保证并行与串行逐字节一致。
- 进程池使用 `mp.get_context("spawn")`，Windows/POSIX 行为一致。

## 6. 检查点、停止与恢复

- 所有记录/产物通过“同目录临时文件 + fsync + `os.replace`”原子落盘，崩溃
  只可能留下旧文件或新文件，不会出现半写记录。
- 停止请求以 `STOP` 标志文件轮询；正在执行的不可拆分分片会跑完，之后不再接纳
  新分片。恢复时复用有效阶段检查点，只补做缺失部分，最终产物与从零运行一致。

## 7. 业务错误码

| 错误码 | 触发场景 |
| --- | --- |
| `E_NUMERIC_MISMATCH` | 量化误差超容差（含分片、层名、最大偏差） |
| `E_NUMERIC_INPUT` | 输入含 NaN/Inf（IEEE 语义，显式报告并阻断下游） |
| `E_STALE_CHECKPOINT` | 落后/重复/错位/非连续发布 |
| `E_INVALID_STAGE` | 非法阶段跳转、缺失前置阶段产物 |
| `E_INVALID_BITWIDTH` | 位宽不在 {4,8} |
| `E_MEMORY_BUDGET` | 工作集超峰值内存（附预算明细） |
| `E_COMPUTE_BUDGET` | 累计计算元素超总预算 |
| `E_WORKER_QUOTA` | 单分片超单工作线程配额 |
| `E_WORKER_FAILED` | 工作进程其它异常（结构化记录分片与阶段） |

任务描述中“内存超预算”统一使用 `E_MEMORY_BUDGET`；计算/配额分别使用
`E_COMPUTE_BUDGET`、`E_WORKER_QUOTA`，三者均附预算明细。

## 8. Windows 适配要点

- 文件锁：`sys.platform == "win32"` 时走 `msvcrt.locking(LK_LOCK/LK_UNLCK)`，
  其余平台走 `fcntl.flock`。
- 多进程：统一 spawn 上下文，工作入口为模块级函数，任务载荷仅含原生类型。
- 原子替换：`os.replace` 在 Windows 与 POSIX 均为原子操作。
- 路径：全部使用 `pathlib.Path`，不依赖 POSIX 专属语义。

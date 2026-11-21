# 技术说明：误差传播、数值契约与设计决策

## 1. 为什么不用二进制 float

版本分流比例、镜像采样率、灰度上限都是“比例”。JSON 的 `0.1` 进入 Python 后是
无法精确表示的二进制浮点，多级乘除与累计求和会让误差沿计算路径传播，并因运算
顺序不同产生不同结论。GraySwitch 在边界上就拒绝 `float`：

* `Fixed(0.1)` 抛 `ImplicitFloatError`；
* 比例必须是十进制字符串（`"0.30"`）或整数，经 `Fixed.from_str` 精确构造；
* 内部表示为整数分子 `n`，实际值 `n / 10^9`（`DECIMALS=9`）。

## 2. 定点运算与误差上界

每次乘 / 除 / 定点化都必须显式声明舍入方式（`half_even` / `half_up` /
`truncate` / `down`），返回结果与**精确误差**（相对 `fractions.Fraction` 真值）。

| 运算 | 误差来源 | 严格上界 |
| --- | --- | --- |
| 定点加 / 减 / 整数倍 | 无（整数运算） | 0 |
| half_even 乘 / 除 / 定点化 | 一次舍入 | 0.5 ulp |
| 归一化最大余数法 | 残差以整数单位再分配 | 1 ulp / 版本 |

ulp = `10^-9`。`TraceBook` 逐运算记录表达式、舍入方式、精确值、本次绝对误差与
累计绝对误差，落库后由 `GET /contracts/errors` 输出；`bound_respected` 字段
断言累计误差不超过声明上界。

### 归一化为何逐位等于 1

先把每版本比例定点化得到 provisional 分子，再计算 `SCALE - sum(provisional)`
残差，按“精确份额小数部分最大者优先、版本字典序兜底”把残差以整数单位分配完，
最终 `sum(normalized_numerator) == SCALE` 由 `assert` 强制保证。

## 3. 分流算法：赤字加权路由

事件只声明 `target_version`（期望版本），实际命中由策略比例统一裁决，避免客户端
直接绑架分流。算法对每个路由键维护：

* `T`：该路由键累计权重（Kahan 补偿求和，整数路径精确）；
* `allocated_v`：各版本已承接权重；
* `target_v = largest_remainder(T * share_v)`：当前目标配额，配额总和逐位等于 `T`。

选择 `target_v - allocated_v`（赤字）最大的版本承接当前事件，平局按版本字典序。
该算法是确定性的（无随机数、无浮点），长期实际占比严格收敛到声明比例，并且
`replay` 重放与在线裁决逐位一致（测试 `test_replay_bit_identical` 固化）。

## 4. 灰度上限的容差契约

裸浮点比较 `usage > limit` 在边界附近不稳定。GraySwitch：

1. `usage = gray_weight / total_weight` 定点化（half_even，误差 <= 0.5 ulp）；
2. `tol = limit * rel_tol` 定点化（一次显式舍入）；
3. 阈值 `threshold = limit + tol`（定点精确加法）；
4. 仅做整数分子比较 `usage.n > threshold.n`，含等号判未越界。

越界事件返回 `E_GRAY_LIMIT_EXCEEDED`，响应内附 `usage / limit /
relative_tolerance / tolerance_band / threshold_inclusive / signed_gap`，全部 9
位小数，可逐项核对。越界事件**不写入任何累计、不参与镜像、不回落基线版本**。

## 5. 镜像采样与去重窗口

* 分位：`u = fixed(sha256(seed|seq) 的前 8 字节 / 2^64)`，half_even 定点化；
* 采样：左闭右开 `u < rate`，`rate=0` 全不采、`rate=1` 全采；
* 同策略版本 + 同累计状态：采样在事件通过灰度检查之后才进行，杜绝“按新版本采样
  率镜像、按旧版本比例分流”的跨版本读；
* 去重：锚点为客户端最近一次**采样命中**时间戳 `a`，窗口两端包含
  `a <= t <= a + window_ms`，窗口内命中被抑制；超过窗口后重新开放（样本
  `req-00112` 固化该行为）。

## 6. 并发与事务

* 一把 `RLock` 串行化策略提交 / 批次裁决 / 统计重建（单写者），写事务用
  `BEGIN IMMEDIATE` 抢占写权；
* SQLite 运行在 WAL + `synchronous=FULL`；读路径不获取写锁，每个读者线程通过
  `threading.local` 持有自己的只读连接（`mode=ro`，懒加载），多读者之间、
  读者与写者之间都不互斥，且避免多线程共享单连接的 `InterfaceError`；
* 业务数据与意图完成状态在同一事务提交，读者只能看到完整提交版本；
* 同 `request_id` 并发提交在写锁下串行：先到者裁决落库，后到者比对内容哈希，
  一致则复用、不一致返回 `E_CONFLICTING_REQUEST`；整批已存在时纯幂等返回，
  不产生新意图或批次行。

## 7. 崩溃恢复

意图表 `intents` 状态机：`PENDING -> COMMITTED | ABORTED`。

* 写意图先单独提交（崩溃可被观测）；
* 业务写 + 意图置 `COMMITTED` 在同一事务（要么全可见要么全不可见）；
* 重启 `recover()` 扫描 `PENDING`：批次 / 重建 / 策略意图一律 `ABORTED`，并把
  判定依据（哪些行存在 / 不存在）写入恢复报告；
* 报告只基于持久化事实，明确声明不承诺强制终止后仍有完整退出报告。

## 8. float64 对照实验的注意点

NumPy 的 `np.add.reduce` 默认使用成对（pairwise）求和，精度高于朴素左结合累加，
不能直接用来复现“大数吞小数”。对照实验显式实现左结合
`((x0+x1)+x2)+...` 的朴素 float64 求和，误差计算也避免把整数真值先转 float
（否则误差会被“减没”），而是用 `Fraction(float_value) - Fraction(exact_int)`。
在混合量级样本上，朴素 float64 把 27 个单位的小量整体吞掉，而定点 Kahan 路径
绝对误差为 0。

## 9. 实测结果（工作区 WSL / Python 3.14 虚拟环境）

* `pytest -q`：35 passed；
* 四场景实验 `data/experiment_report.json`：
  * 全量裁决 40 条：routed 38 / rejected 2 / mirrored 6，定点占比 9 位小数；
  * 镜像去重 13 条：镜像 2 条，窗口抑制 9 条，跨窗口重新开放 1 条；
  * 灰度上限：19 条越界，首条 `usage=0.500000000 > threshold=0.200000200`；
  * 强制终止退出码 137，重启后 1 条 PENDING 被标记 ABORTED，无部分批次可见；
  * float64 朴素求和绝对误差 27，定点 Kahan 绝对误差 0；
  * 重放 40 条裁决逐位一致，统计重建与持久化状态逐位一致。

## 10. 已知边界

* 定点小数位固定为 9；若未来比例需要更高精度，调整 `fixedpoint.DECIMALS` 并
  重新生成样本即可，所有误差上界随 ulp 自动更新。
* 当前镜像去重锚点保留在内存状态并随计数器持久化；极端情况下同一客户端跨多个
  策略版本的窗口不延续（每个策略版本独立累计），这是有意的版本隔离语义。

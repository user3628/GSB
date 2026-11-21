# 技术说明与验证记录

## 1. 偏置矩阵定义

对三种 RoPE 方案，先在对数域构造逆频率 `inv_freq`，对 Q/K 施加同一旋转后计算
`softmax 前注意力 logits = RQ @ (RK)^T / sqrt(D)`，再在批量与头维度平均，得到
`[L, L]` 位置相关偏置矩阵。ALiBi 直接构造 `-slope_h * |i-j|` 的距离偏置并按头平均。
注意力权重本身从不被修改。

- `rope`：`theta_i = base^(-2i/D)`
- `linear_rope`：`theta_i = base^(-2i/D) / scale`（比值走对数域）
- `ntk_rope`：有效基频 `base * scale^(D/(D-2))`（幂与乘积走对数域）
- `alibi`：显式斜率或 `2^(-8/H)` 幂律缺省斜率

非整除头维度按配置补齐到偶数维并在检查点登记 `padding`（raw/padded/rule/width）。

## 2. 外推分档与单调性

按 `distance(i,j)/train_length` 落入 `[ratio_low, ratio_high)` 档位。每档输出：
均值、方差、峰值、相邻位置间距差分绝对值均值 `diff_abs_mean`、距离带均值的单调
违反次数 `monotonic_violations`（距离增大而均值抬高即违反）、变化率 `change_rate`
（相对首个非空档锚点档）。空档保留完整记录。ALiBi 在演示数据上单调违反恒为 0，
RoPE 系因随机 Q/K 内容在远档出现单调性破坏，正是工作台要捕获的外推异常形态。

## 3. 定点与误差传播

浮点偏置按 `scale = 1.5 * peak / 2^31` 量化为 int32，`scale<=0` 或出现 NaN/Inf 时
抛 `E_NUMERIC`（携带 scheme / bin_id / 上下文）。对数域乘除对零、负值、非有限输入
逐一守卫，杜绝 `math.log` 域错误与浮点漂移。

## 4. 四阶段检查点与恢复

阶段链 `config -> bias -> binning -> tolerance`，每阶段记录
`{stage, sequence, input_fingerprint, config_version, artifact_summary, fingerprint}`，
原子写盘。停止请求只在当前不可拆分单元（一个 shard×scheme 或一个 scheme 分档）
完成后生效；恢复时指纹命中的阶段直接复用，最终报告与从零运行逐字节一致，序号
1..N 连续。

## 5. 版本失效与并发

每阶段输入指纹 = 上游指纹 + 本阶段输入（分片哈希 / 频率 / 缩放 / 档位 / 容差）。
配置变更后识别最早失配阶段，删除该阶段及其全部下游检查点并重建，上游继续复用。
多进程发布经 `flock` 串行化，重复序号、落后序号、同序号异指纹、跨阶段错位均返回
`E_STALE_CHECKPOINT`；缺少前置阶段时返回 `E_INVALID_STAGE`。

## 6. 实际验证结果（本机 Python 3.11.18 / numpy 2.0.2 / PyYAML 6.0.2）

- `python -m unittest tests.test_workbench`：14/14 通过。
- 重复运行 `data/reports/diagnostic_report.json` 与全部 `*.npy` 量化产物 SHA256
  完全一致（BYTE_IDENTICAL_RERUN）。
- 演示数据：3 分片 × (B=2,H=4,L=32,D=16)，四方案 × 三档（train/near/far）全部非空。
- 注入矩阵：seq_too_long→E_SEQ_TOO_LONG，invalid_scale→E_INVALID_PARAM，
  zero_bins→E_INVALID_CONFIG，nan_shard / zero_base_freq / zero_quant_scale→
  E_NUMERIC（含方案与分片上下文）。
- 并发发布实测：两个 seq=1 之一被拒、seq=0 落后被拒、seq=2 正常成为最终记录。
- HTTP：8765 被占用时预检拒绝并返回 E_PORT_UNAVAILABLE；8792 上 health/run/stop/
  resume/inject/PUT config/schemes/invalidations 全部验证通过，恢复后报告哈希不变。
- 旧清单 v1→v2→v1→v2 回导，配置规范 JSON 与偏置 / 分档摘要逐字节一致。

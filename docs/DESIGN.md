# 技术说明与验证记录

## 1. 设计目标与约束

工作台围绕两个核心难点构建：**交互延迟可控** 与 **计算结果跨运行/跨平台一致**。
约束为不使用真实模型、预训练声纹、GPU、容器或外部服务，因此全部算法均为确定性的
经典信号处理 + 种子化数值过程。

## 2. 数据流与阶段划分

```
WAV + manifest.json
   │  导入校验（样本总数、时长上界、分段合法性、顺序稠密）
   ▼
timelines/<shard>.json（参考时间轴，编辑版本号）
   │  separation 阶段
   ▼  STFT → 迭代幅值掩码 → iSTFT（int16 定点）+ 帧能量 + 通道嵌入
artifacts/<shard>/separation/
   │  recognition 阶段（依赖 separation 产物 + 当前时间轴 + 识别配置）
   ▼  质心 → 一对一通道指派 → 最近质心归类 → 低置信/重叠判定
artifacts/<shard>/recognition/
   ▼
snapshots/（每阶段原子快照）+ exports/export.json
```

每个分片两个可重算阶段：`separation`、`recognition`。`import` 只做校验与时间轴
引导，不产生可重算产物。

## 3. 确定性是如何保证的

1. **数据生成**：每分片一个派生种子（`base_seed + order*101`），RNG 仅用于
   确定性的音高轮廓、调制与有色噪声；布局由固定规则给出，抖动也来自该 RNG。
2. **STFT**：固定帧长/帧移、汉宁窗、对称零填充；NumPy `np.fft.rfft` 对相同输入
   与尺寸返回相同结果，重建误差 ~1e-15（机器精度）。
3. **掩码初始化**：由分片内容指纹经 SHA-256 派生种子，生成可复现的频率基与时序
   调制；迭代次数与 floor 均来自配置，无随机收敛分支。
4. **嵌入**：mel 滤波器组由解析公式构造；哈希投影用种子矩阵 + QR 正交化；最终
   L2 归一化后做 Q14 定点量化（int16 小端字节），消除浮点打印差异。
5. **序列化**：所有 JSON 经 `sort_keys + 紧凑分隔符` 规范化；波形/嵌入为小端
   二进制定点；指纹统一取规范化字节的 SHA-256 截断。因此同一输入与配置在不同
   运行中产生**逐采样点一致波形**与**逐字节一致嵌入/摘要**。

## 4. 识别、低置信与重叠判定

- 质心 = 参考段内通道嵌入的均值，叠加由 `base_seed` 与内容种子决定的 1e-3 微扰
  后归一化，保证质心是「参考分段 + 本地种子」的纯函数。
- 无模型掩码分离存在通道置换歧义，故先做**确定性全局一对一指派**（枚举所有排列、
  取总余弦相似度最大、按通道下标确定性破并），再做最近质心判定。
- 最佳与次佳相似度差 `gap < recognition_margin` 时：`confidence=low`、
  `assigned_speaker=null`，通道仍完整保留在 `diagnostics` 中，不静默归类。
- 重叠段：先取被≥2 个参考说话人覆盖的帧作为候选（静音帧天然排除），再用每通道
  自适应分位能量阈值确认，合并≤6 帧的缝隙并过滤<3 帧碎片，输出毫秒起止与参与者。

## 5. 时间轴与编辑模型

分段以毫秒整数存储 `(seg_id,start,end,speaker,confidence,overlap)`。所有编辑
返回新时间轴，版本号 `+1` 并记录 `EditRecord(operation, origin, affected_ms,
detail)`。校验禁止负区间、`end<=start`、超出分片时长。支持：切分、相邻同说话人
合并、改名、说话人合并、段上插/删、跨分片身份统一。草稿（draft）与已发布版本
分文件存储，丢弃草稿不影响已发布时间轴的可读性。

## 6. 延迟预算与缓存一致性

- 查询类型：`segments` / `similarity` / `overlaps`。预算默认取
  `config.latency_budget_ms`，可按请求覆盖。
- 超出预算返回 `E_LATENCY_BUDGET`（HTTP 503），带 `query_id`、`elapsed_ms`、
  `budget_ms`，不阻塞等待。
- 缓存键 = `SHA256(shard_id, kind, 内容指纹, 配置版本, 编辑版本)`。编辑发布或
  配置变更立即清空进程缓存，因此旧编辑版本的结果不可能被再次返回。

## 7. 增量重算与全量一致性

阶段版本指纹的输入：

- separation：`内容指纹 + 配置指纹 + 说话人数 + mask_iterations + mask_floor`
- recognition：`内容指纹 + 配置指纹 + 说话人数 + separation 阶段版本 +
  recognition_margin + overlap_quantile + 当前时间轴分段摘要`

运行时先比对已落盘 `result.json` 的 `stage_version`：相同则复用，不同则从该阶段
起重算，上游未变产物原样保留。`verify_consistency()` 对每个分片独立从零重算
分离与识别，比对波形 digest、识别统计、重叠段与诊断 digest；任何差异抛
`E_INCREMENTAL_MISMATCH` 并附 `{shard_id, area}` 差异摘要。

## 8. 快照、停止恢复与整体替换识别

- 快照写临时文件后 `os.replace` 原子落盘，内含阶段名、分片指纹、配置版本、编辑
  版本、产物摘要 digest 与快照自 digest，任意中断不会留下半写记录。
- 停止通过标志文件实现；流水线在每个不可拆分单元（单阶段/单分片）边界检查，
  收到请求后完成当前单元抛 `E_STOPPED`。`recover()` 先校验全部快照完整性，再
  仅补缺/补过期阶段，恢复结果与完整运行一致（测试中以 byte-identical 校验）。
- 替换识别：快照与所认证产物的阶段版本不一致时，版本变小判 `rollback`、变大判
  `tamper`；自 digest 或摘要不匹配判 `tamper`；版本一致且完整则视为正常推进，
  允许覆盖。仅正常推进被接受，其余返回 `E_STALE_SNAPSHOT`。

## 9. 并发发布保护

`meta/publish.lock` 使用 `fcntl`（POSIX）/`msvcrt`（Windows）跨进程排他锁。
发布在锁内比较 `expected_version` 与当前版本，仅 `current+1` 的连续推进提交；
落后/重复/不连续返回 `E_STALE_VERSION`。`tests/test_concurrency.py` 用 spawn
两个真实子进程同时发布，验证恰有一个成功、另一个收到 E_STALE_VERSION，且已发布
时间轴未被污染。

## 10. 实际验证结果（本机，Python 3.14 / NumPy 2.5.5 / SciPy 1.18.1）

- 单元与并发测试：`python -m unittest discover -s tests` → **14 passed**，
  开启 `ResourceWarning` 报错模式无句柄泄漏。
- 端到端演示 `tools/run_demo.py`：3 个分片（2/3/2 说话人，含静音与重叠），
  连续两次运行的 `export_digest` 与各阶段版本指纹完全相同，示例固定摘要：
  - `export_digest = 026a1a53fdb258c608d76be005bd9dabe5d9b34de6d73cebdd75589d601033d0`
  - recognition 阶段版本：`fp-f9372abf21594406`、`fp-fe95c5184f7a14f1`、
    `fp-ce9aaffe80ca55fd`
  - 重叠段数量：shard-000=1、shard-001=4、shard-002=2
  - 低置信通道：shard-001=1、shard-002=1（均保留在诊断、未强制归类）
- 错误码注入全部命中：`E_INVALID_CONFIG`（margin=-1/1.5、iterations=0）、
  `E_INVALID_STAGE`（未分离直接识别）、`E_LATENCY_BUDGET`（budget=0）、
  `E_STALE_VERSION`（重复 expected_version）、`E_STALE_SNAPSHOT`（回退+篡改）、
  `E_STOPPED`（运行前置停止标志后恢复）。
- HTTP 隔离自检脚本验证 `/health`、`/import`、`/run-all`、`/publish`、
  `/config`、`/query`、`/consistency` 的正常与错误返回码均符合约定。

> 说明：上述固定摘要依赖 `requirements.txt` 锁定的 NumPy 主版本与默认配置；
> 更换 NumPy 大版本理论上可能改变 FFT 末位浮点，重新运行 `run_demo.py` 会在
> 该环境内重新得到稳定但可能不同的摘要。

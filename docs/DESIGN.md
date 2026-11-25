# 分层量化标定与误差传播审计工作台 — 技术说明

## 1. 范围与约束

- 纯 Go 实现（Go 1.22+，实际验证环境为 Go 1.27），SQLite 使用纯 Go 驱动
  `modernc.org/sqlite`，`CGO_ENABLED=0` 可静态构建，无 CGO 依赖。
- 不使用 Docker/GPU/在线数据集；权重、PNG 分片、量化策略与高精度参考输出全部由
  内置固定种子生成器（`internal/generator`）在本地确定性产出。
- 演示网络为 4 层全连接 MLP（64→24→16→12→4），覆盖 8/4 位两档位宽、对称/非对称
  两种模式、逐层/逐通道两种粒度，并在 `fc2` 注入一个离群输出通道以触发边界钳制。

## 2. 包结构

| 包 | 职责 |
| --- | --- |
| `internal/model` | 领域类型、稳定业务错误码、规范化 JSON 与内容指纹 |
| `internal/calib` | 阶段 1：确定性分层抽样、PNG 尺寸校验、灰度归一化展平 |
| `internal/quant` | 定点原语、逐层/逐通道标定、整数前向引擎与钳制统计 |
| `internal/infer` | float64 参考前向、量化推理、显式抽样位置真值比对 |
| `internal/audit` | 双路误差归因、可复核上界推导、有效位数 ENOB |
| `internal/store` | SQLite 事务快照、版本表、进度表、flock 文件锁与条件发布 |
| `internal/engine` | 四阶段编排、版本指纹、替换/回退识别、失效传播与恢复 |
| `internal/generator` | 固定种子本地样本生成（权重/PNG/策略/参考输出） |
| `internal/server` | 仅监听本地回环地址的 HTTP JSON 接口 |
| `cmd/qaudit` | CLI 与 HTTP 入口 |

## 3. 四阶段流水线

### 阶段 1：校准样本选取与特征规范化

- 按标签分层，使用 `math/rand/v2` 的 PCG（种子来自策略声明）在每个层内洗牌；
  配额用最大余数法分配，保证选取总数精确等于 `round(ratio*N)` 且分层确定。
- PNG 解码后校验实际宽高必须同时等于分片声明与策略声明，否则 `E_CALIB_SAMPLE`。
- 灰度固定规则 `gray = 0.299R + 0.587G + 0.114B`，归一化 `x = (gray/255 - 0.5)/0.5`，
  输出范围 `[-1,1]`，按行优先展平。
- 校准清单（索引/ID/标签）走规范化 JSON 取 SHA-256；特征矩阵走固定小端字节布局
  （`FeatureBlob`）取 SHA-256。相同输入与策略产生逐字节一致的两份摘要。
- `ratio <= 0` 或 `round(ratio*N) > N` 返回带 `ratio/requested/available` 上下文的
  `E_CALIB_SAMPLE`。

### 阶段 2：逐层定点标定

- 对称模式：有符号域 `[-(2^(b-1)-1), 2^(b-1)-1]`，零点固定 0，
  `scale = max(|min|,|max|)/qmax`。
- 非对称模式：无符号域 `[0, 2^b-1]`，`scale=(max-min)/(qmax-qmin)`，
  `zero_point=round(-min/scale)-qmin` 并钳制到域内。
- 取值范围覆盖校准极值；量化时对越界整数做边界钳制并累计钳制次数。
- 权重支持逐层（单组参数）或逐通道（每行一组参数）；激活按校准 mini-batch 做
  逐张量标定，层输出按该层校准输出独立标定。
- 常量/全零对称张量使用单位尺度，避免静默产生零尺度；全零通道、非有限权重/激活、
  零尺度、不支持位宽（仅允许 4/8）统一返回带 `layer/channel/reason` 的
  `E_QUANT_PARAM`，不会静默产出全零权重。

### 阶段 3：量化推理与真值比对

- 整数域完成乘加：权重与输入先量化，偏置按累加域量化并折叠零点项；累加位宽
  `2*b + ceil(log2(inDim)) + 1`，溢出做累加钳制并计数。
- 反量化显式补偿输入/权重零点，再按输出激活尺度重定点，ReLU 在实值域应用后再做
  输出量化，钳制计数分别记录。
- 在策略显式声明的抽样位置上，与高精度 float64 参考逐层输出和端到端 logits 比对，
  给出每层最大绝对偏差、相对偏差与端到端相对误差。
- 容差按最大绝对偏差判定（可在策略中配置）；超差返回带 `layer/position/max_dev/
  tolerance` 的 `E_NUMERIC_MISMATCH`，但审计阶段仍继续产出归因与上界供复核。

### 阶段 4：误差传播归因与上界审计

- 唯一真值是本地高精度参考输出；参考缺失或量化权重全零返回 `E_AUDIT_INPUT`。
- 归因方法 A（一阶敏感度）：权重算子范数 × 量化步长，归一化为分摊份额并排序。
- 归因方法 B（逐层扰动重放）：仅把目标层替换为 float64 执行，其余层保持量化，
  测量端到端 logits 相对误差并排序。
- 两种排序不一致的层标记 `needs_review` 并同时保留两种排名依据。
- 误差上界由显式尺度逐步推导。对 `y = Wx + b`，无穷范数下
  `e_out <= ||W||_1*(e_in + s_x/2) + (s_w/2)*||x||_1 + s_y/2`，
  其中 `e_in` 为上游传播界，`s_x/s_w/s_y` 分别为输入、权重、输出量化步长；
  ReLU 是 1-Lipschitz，不增加上界。测试断言每层上界均覆盖实测最大偏差。
- 有效位数 `ENOB = 0.5*log2(signal_power/noise_power)`，以声明位宽封顶、零为底。

## 4. 快照持久化与崩溃恢复

- SQLite 表：`snapshots(version, seq, layer_name, input_fingerprint,
  policy_fingerprint, artifact_digest, payload, UNIQUE(version,layer_name))`、
  `versions(version,fingerprint,kind)`、`pipeline_state(version,fingerprint,stage)`。
- 每层标定在独立事务中提交，`seq` 由 `MAX(seq)+1` 单调生成；读端永远看不到半提交。
- 恢复只依据已提交事实：重算校准集与版本指纹并与 `pipeline_state` 比对，逐层出库
  校验 `artifact_digest`，缺失层按原 `seq` 续发，然后确定性重建推理与审计。
- 验证：发布第 2 层后模拟强制终止，恢复后报告摘要与一次连续执行逐字节一致。

## 5. 版本指纹、替换与回退

- 四类阶段摘要（权重、校准清单+特征、策略、抽样位置）合成单一版本指纹。
- 指纹等于最新版本：正常推进并复用已提交层；指纹在历史版本出现但落后于最新：
  判定回退，拒绝并返回 `E_STALE_VERSION`；全新指纹：整体替换，分配 `latest+1`，
  旧版本全部层标定/审计失效，诊断中逐层列明失效原因，未受影响的上游标定可在同版本
  复用时通过摘要校验继续复用。

## 6. 并发发布保护

- 每个数据库伴随真实 `flock(2)` 文件锁；发布先取文件锁，再做 SQL 条件检查。
- 仅允许版本连续推进；落后版本、重复层、跨层错位序号一律 `E_STALE_VERSION`。
- 16 个 goroutine 竞争同一序号槽位的测试中恰有 1 个成功，其余 15 个稳定失败。

## 7. 接口

- CLI：`generate/run/recover/status/audit/versions/export/inject/serve/stop`，
  `run -crash-after-layer N` 用于强制终止演练。
- HTTP（仅 `127.0.0.1`）：`/api/import`、`/api/policy`、`/api/run`、`/api/recover`、
  `/api/status`、`/api/fingerprints`、`/api/audit`、`/api/versions`、`/api/export`、
  `/api/inject`、`/api/stop`、`/healthz`。
- 注入入口支持 `bad_bitwidth/zero_channel/nan_weight/calib_ratio`，在各自阶段返回
  稳定业务码并保留诊断上下文；注入运行使用一次性隔离快照库，不污染基线版本。

## 8. 确定性保证

- 所有随机数来自固定种子 PCG；所有指纹走排序键的规范化 JSON 或固定字节布局。
- 两次独立生成+运行的版本指纹、校准清单、逐层产物摘要、导出报告摘要完全一致，
  由 `scripts/verify.sh` 自动断言。

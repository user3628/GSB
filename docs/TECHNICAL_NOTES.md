# TLS 握手与证书链诊断工作台 — 技术说明

## 1. 范围与运行方式

工作台完全离线运行：证书与私钥由 `cryptography` 在本地生成，握手参数为固定
JSON，持久化使用本地 SQLite 文件，HTTP 服务仅绑定 `127.0.0.1`。不依赖外部
CA、真实站点或任何网络目标，也不使用容器。

- 初始化：`python -m tls_workbench.cli init`（Windows：`.\setup.ps1`）
- 演示：`python demo.py all`（Windows：`.\run_demo.ps1`）
- 测试：`python -m unittest discover -s tests -v`（Windows：`.\run_tests.ps1`）
- 服务：`python run_server.py --host 127.0.0.1 --port 8080`

## 2. 目录结构

- `tls_workbench/fixedpoint.py`：定点整数契约（比例因子 1e9，半数偶舍入）。
- `tls_workbench/kahan.py`：定点/浮点 Kahan 求和、朴素求和误差上界审计。
- `tls_workbench/timeutil.py`：时钟偏移注入、亚秒截断、毫秒整数余量与零边界。
- `tls_workbench/certificates.py`：确定性密钥缓存、证书签发、DER 解析。
- `tls_workbench/chain.py`：路径构建、环检测、签名与有效期整链验证。
- `tls_workbench/scoring.py`：定点强度评分、逐步舍入追踪、容差比较。
- `tls_workbench/budget.py`：预算累计、超限裁决、受保护降级策略。
- `tls_workbench/states.py`：五阶段状态机与合法迁移表。
- `tls_workbench/persistence.py`：SQLite 表结构、意图日志、崩溃恢复。
- `tls_workbench/engine.py`：单写者推进、阶段校验、诊断组装。
- `tls_workbench/webapp.py`：本地 Flask API。
- `samples/handshake_samples.json`：固定握手参数样本。
- `certs/der/<chain>/*.der` 与 `chain_metadata.json`：确定性证书链。

## 3. 数值精度与误差传播契约

### 3.1 定点表示

所有业务数值（评分、余量因子、预算、占比）都以 Python 任意精度整数保存，
比例因子 `SCALE = 1_000_000_000`（9 位小数）。整数加减精确；乘除后需要重新
缩放时统一采用 `ROUND_HALF_EVEN`（半数偶）舍入，作用于精确有理值，避免先转
浮点带来的平台差异。因此同一条链重复评分、跨进程重放均逐位一致。

每个评分步骤在 `steps` 中记录：操作数、精确有理值对应的定点理想值、实际取值、
舍入模式与该步舍入误差；`cumulative_rounding_error` 汇总累计舍入误差。

### 3.2 强度评分

单证书：`score = base_strength × algorithm_weight × margin_factor`。

- `base_strength`：RSA 为 `模长位数 / 256`；EC 按曲线安全等级（P-256=8、
  P-384=12）。
- `algorithm_weight`：SHA256-RSA=1.00、SHA384-RSA=1.10、ECDSA-SHA256=1.05、
  ECDSA-SHA384=1.15。
- `margin_factor = clamp(0.5 + 剩余毫秒/86_400_000/3650, 0.5, 1.2)`，负余量
  （已过期）落到 0.5 下限而非产生负因子。

链评分按角色权重 `leaf=0.5 / intermediate=0.3 / root=0.2` 加权求和，权重之和
在定点下恰为 1.0。链间比较使用相对容差 `1e-6`：容差内判平局，并按链 ID 的
稳定字典序选出强者，平局结论不会因浮点抖动翻转。

### 3.3 预算累计与误差上界

预算规范值在定点域用 Kahan（补偿）求和得到；由于底层是整数加法，补偿项恒为
零并在结果中显式报告。同时保留浮点朴素求和、浮点 Kahan、逆序与交错顺序
四条计算路径用于误差说明：

- 先验误差上界：`(n-1) × eps × Σ|term|`，`eps = 2^-53`。
- 观测误差：朴素/Kahan 浮点结果相对定点规范值的绝对差。
- 审计同时报告 `bound_respected`，验证观测误差不超出理论上界。

分项占比用定点除法计算，占比之和与总量在重复运行中逐位一致，运算顺序变化
不会翻转“是否超预算”的结论，因为裁决只基于定点规范值。

## 4. 时间与有效期规则

DER 的 `UTCTime/GeneralizedTime` 只编码整秒（实测 `YYMMDDHHMMSSZ`），证书
边界天然丢失亚秒信息，样本因此把证书有效期对齐到秒，而把毫秒精度放在验证
时刻上。

- 偏移注入（先加后截）：
  `effective = floor((nominal_wall_ms + clock_offset_ms) / g) × g`。
- 默认截断粒度 `g=1ms`；样本可显式改成 1000ms 演示亚秒截断对结论的影响。
- 有效期为闭区间 `[not_before_ms, not_after_ms]`：
  - 上边界余量为 0（恰在 `not_after`）判定 `VALID`；
  - 下边界余量为 0（恰在 `not_before`）判定 `VALID`；
  - 负余量分别给出 `E_CERT_NOT_YET_VALID` / `E_CERT_EXPIRED` 与具体毫秒数。

## 5. 状态机、失败处理与降级

合法迁移仅允许顺序前进：
`INIT → CLIENT_HELLO → SERVER_HELLO → CERTIFICATE → CERT_VERIFY → FINISHED`。
非法迁移抛出 `E_INVALID_TRANSITION`，且在抛错前不执行任何写库操作，已持久化
状态保持不变。

阶段失败会记录阶段、业务码、消息与细节。`continue_on_failure=true` 时继续
推进以完成诊断，会话最终标记为 `COMPLETED_WITH_FAILURES`；否则标记 `FAILED`
并停在失败边界。

降级只允许跳过 `STRENGTH_SCORE_DETAIL`、`CHAIN_MARGIN_NARRATIVE`、
`SUMMATION_ORDER_AUDIT` 等非关键项；`CERT_SIGNATURE_VERIFICATION`、
`CERT_VALIDITY_VERIFICATION`、`PATH_CONSTRUCTION` 受保护，任何跳过请求返回
`E_DOWNGRADE_FORBIDDEN`，降级清单逐项返回可核对。

预算超限在 `FINISHED` 阶段裁决：先提交完整阶段边界与诊断，再抛出
`E_BUDGET_EXCEEDED`，细节包含触发阶段、实测值、限额与全部违规项。

## 6. SQLite 持久化与强制终止恢复

表：`sessions`、`stage_states`、`diagnostics`、`intent_log`。写路径遵循
“意图先落盘”：

1. 写入 `PENDING` 意图；
2. 在独立事务中提交阶段边界（`stage_states` + `sessions.version+1`）；
3. 把意图标记为 `COMPLETED`。

重启时 `recover_pending()` 扫描所有 `PENDING` 意图，只依据已提交事实：

- `ADVANCE_STAGE` 意图的目标阶段不在 `stage_states` 中，说明进程在边界提交
  前被杀：会话标记 `ABORTED`，当前阶段回滚到最后一个完整边界；
- 目标阶段已存在则补记意图完成；
- 未完成的建会话/诊断意图直接中止。

恢复报告只陈述已持久化内容，不会在强制终止后虚构一份完整 Finished 报告。
恢复本身幂等，再次扫描返回空动作列表。

## 7. 并发一致性

- 每个会话一把锁，握手推进为单写者串行；同一会话的并发请求阻塞复用，首个
  请求执行一次，其余复用同一份结果，重复推进不会产生第二条阶段记录。
- 重放请求若携带与建会话时不同的参数，返回 `E_CONFLICTING_SESSION`。
- 读路径打开 `query_only` 连接，只读取已提交版本；写者每次推进一个完整阶段
  边界后才提交，读者不会看到半个阶段。

## 8. HTTP API（仅 127.0.0.1）

- `GET  /api/health`
- `POST /api/sessions`
- `POST /api/sessions/<id>/advance`
- `GET  /api/sessions/<id>`
- `GET  /api/sessions/<id>/diagnostic`
- `GET  /api/sessions`
- `POST /api/recover`
- `GET  /api/chains/<chain_id>`
- `GET  /api/numeric/audit`

## 9. 需求到验证的映射

| 业务要求 | 实现位置 | 测试 |
| --- | --- | --- |
| 1. DER 解析、缺根/坏 DER 的 E_CHAIN_INVALID 与快照保护 | `certificates.py`, `engine.SampleStore` | `test_certificates_chain.py` |
| 2. 路径构建、环检测、毫秒余量与零边界 | `chain.py`, `timeutil.py` | `test_timeutil.py`, `test_certificates_chain.py` |
| 3. 定点评分、舍入追踪、容差与字典序平局 | `fixedpoint.py`, `scoring.py` | `test_fixedpoint.py`, `test_certificates_chain.py` |
| 4. Kahan 累计、朴素求和误差上界、顺序不翻转 | `kahan.py`, `budget.py` | `test_fixedpoint.py`, `test_engine_persistence.py` |
| 5. 五阶段状态机、非法迁移不落库、失败原因 | `states.py`, `engine.py` | `test_engine_persistence.py` |
| 6. 预算超限裁决与受保护降级 | `budget.py`, `engine.py` | `test_engine_persistence.py`, `test_webapp.py` |
| 7. 意图日志、强制终止恢复、基于事实的报告 | `persistence.py` | `test_engine_persistence.py` |
| 8. 单写者多读者、幂等推进、冲突检测、HTTP 与演示 | `engine.py`, `webapp.py`, `demo.py` | `test_engine_persistence.py`, `test_webapp.py` |

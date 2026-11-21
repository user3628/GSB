# TLS 握手与证书链诊断工作台（本地回环 / 离线）

面向网络运维与安全工程师的离线工作台，用于评估证书链验证中的数值精度与
误差传播。所有证书、私钥、握手参数、数据库均在本地确定性生成，不访问外部
CA 或网络目标，不使用容器。

## 环境要求

- Python 3.11+（开发与验证使用 3.14）
- 依赖（见 `requirements.txt`，可改用本地 wheel 离线安装）：
  - `Flask==3.1.3`
  - `cryptography==45.0.4`

### Windows 初始化

```powershell
.\setup.ps1            # 创建虚拟环境、安装依赖、生成样本与 SQLite
.\run_demo.ps1         # 五个离线演示场景
.\run_tests.ps1        # 单元测试
.\run_server.ps1       # 启动 http://127.0.0.1:8080
```

离线 wheel 安装：把 wheel 放到 `wheels/`，然后
`pip install --no-index --find-links .\wheels -r requirements.txt`。

### Linux / macOS

```bash
python -m pip install -r requirements.txt
python -m tls_workbench.cli init
python demo.py all
python -m unittest discover -s tests -v
python run_server.py --host 127.0.0.1 --port 8080
```

## 确定性样本

- 证书链（`certs/der/<chain>/`，含 DER 与 `chain_metadata.json`）：
  - `rsa2048_chain`：RSA-2048/SHA-256，叶子恰在验证时刻到期（零余量边界）。
  - `rsa3072_chain`：RSA-3072/SHA-384，更强的对照链。
  - `ec256_chain`：EC P-256/ECDSA-SHA-256。
  - `expired_chain`：验证时刻叶子已过期。
- 握手参数（`samples/handshake_samples.json`）：协议版本、密码套件、SNI、
  会话票据长度、时钟偏移毫秒、截断粒度、分阶段与总预算上限。
- 私钥在首次生成后以 PKCS#8 DER 缓存于 `certs/keys/`（被 `.gitignore`
  忽略），再次生成使用同一密钥材料，输出完全可复现。

## 演示场景

`python demo.py all` 依次执行：

1. 正常五阶段握手，输出零余量判定、定点评分、预算与占比。
2. 预算超限，输出 `E_BUDGET_EXCEEDED`、触发阶段与朴素/Kahan 误差审计。
3. 时钟偏移导致叶子过期，失败后继续完成诊断。
4. 强制终止恢复：PENDING 意图扫描、回滚到最后完整阶段边界。
5. 数值精度说明：重复评分逐位一致、链强弱比较、求和误差上界。

## 关键设计

- 定点契约：比例因子 1e9 的任意精度整数，统一半数偶舍入，逐步记录舍入误差。
- 时间规则：偏移先加后 floor 截断；有效期为闭区间，零余量边界确定有效。
- 状态机：`CLIENT_HELLO → SERVER_HELLO → CERTIFICATE → CERT_VERIFY → FINISHED`，
  非法迁移返回 `E_INVALID_TRANSITION` 且不落库。
- 持久化：意图先写 `intent_log` 再提交阶段事务；崩溃后只按已提交事实恢复。
- 并发：会话级单写者锁 + 幂等推进，读者只读已提交快照。

详见 `docs/TECHNICAL_NOTES.md`。

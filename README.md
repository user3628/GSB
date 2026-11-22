# 本地微服务注册与健康探测中间件

这是一个面向本地微服务测试环境的注册中心实现，重点解决注册快照与探测结果的格式互操作、版本兼容、租约并发、探测一致性以及强制终止后的 SQLite 事实恢复。实现不依赖 Docker、外部注册中心或外部网络。

## 交付内容

- 紧凑二进制格式：魔数 `SRMW`、版本、大小端标记、长度前缀、逐记录 CRC32、可选字段位图。
- 分片 JSONL：每行包含服务名、实例 ID、端点、权重、探测方式和探测参数，`sequence` 保证跨分片顺序。
- 显式兼容矩阵：v1 降级接受，v2 接受，v3+ 与未知必填字段整体拒绝。
- 权重 `1..1000` 千分比与毫秒内部时间；浮点比例/微秒输入必须声明舍入或截断规则。
- 租约版本、并发续约单写复用、HTTP/TCP/租约三类真实探测。
- 全局并发 + 有界队列 + 轮次预算；实例状态与探测历史同事务提交。
- 导入覆盖/跳过/拒绝裁决、批次原子回滚、批次 ID 幂等。
- 意图日志与崩溃恢复：未提交探测回到待探测、暂存导入标记中止。
- 本地测试服务、确定性样本、23 项自动化测试与完整端到端脚本。

## Windows 快速开始

在 PowerShell 中执行：

```powershell
.\scripts\setup.ps1
.\scripts\run_tests.ps1
.\scripts\run_e2e.ps1
```

`setup.ps1` 优先使用 `py -3.11`，不存在时通过 winget 安装 Python 3.11，并在 `.venv` 中安装固定版本依赖。

## Linux / WSL 快速开始

```bash
./scripts/setup.sh
.venv/bin/pytest -q
.venv/bin/python scripts/e2e.py
```

## 本地运行

```bash
.venv/bin/python -m svc_registry.test_service --http-port 28765 --tcp-port 28766
.venv/bin/python -m svc_registry.cli serve --host 127.0.0.1 --port 18080 --db data/registry.db
```

注册中心 API 文档：`http://127.0.0.1:18080/docs`。样本位于 `samples/legacy_v1.jsonl`、`samples/modern_v2.bin` 与 `samples/endpoints.json`。

## 主要端点

| 方法 | 路径 | 用途 |
|---|---|---|
| GET | `/health` | 注册中心健康与启动恢复摘要 |
| GET | `/api/v1/compatibility` | 显式兼容矩阵 |
| POST | `/api/v1/negotiate` | 导入前格式/版本协商与预览 |
| POST | `/api/v1/convert` | JSONL/二进制互转 |
| POST | `/api/v1/import` | 原子批次导入 |
| GET | `/api/v1/export` | 导出注册或探测结果快照 |
| POST | `/api/v1/register` | 注册/更新实例 |
| POST | `/api/v1/instances/{service}/{id}/renew` | 按租约版本续约 |
| POST | `/api/v1/probes/run` | 执行一轮并行探测 |
| GET | `/api/v1/probes/history` | 查询已提交探测历史 |
| GET | `/api/v1/intents` | 查询意图日志 |
| GET | `/api/v1/recovery/latest` | 最近一次启动恢复报告 |

所有业务错误统一返回：

```json
{"ok":false,"error":{"code":"E_FORMAT_CORRUPT","message":"record 2: record CRC32 mismatch","details":{"record_index":1,"record_number":2}}}
```

## 文档

- `docs/FORMAT.md`：二进制/JSONL 布局、位图、版本矩阵、归一化规则。
- `docs/DESIGN.md`：事务、调度、冲突与恢复设计。
- `docs/VALIDATION.md`：实际测试与端到端验证结果。
- `artifacts/e2e/e2e_report.json`：最近一次端到端原始报告。

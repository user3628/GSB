# 资源编排依赖图诊断工作台

本地、确定性、仅回环的资源编排依赖图诊断工作台：整份快照与增量片段提交、
拓扑分层 / 环检测 / 下游影响面 / 编排批次 / 跨资源权重一致度、
单写者多读者并发、提交幂等、级联原子重建与强制终止恢复。

## 环境准备（Windows）

需要 Python 3.10+，可执行 `python` 与 `pip`：

```bat
python --version
pip install -r requirements.txt
```

依赖（开发环境实际安装版本，见 `requirements.txt`）：
fastapi 0.115.6、uvicorn 0.34.0、numpy 2.2.1、pytest 8.3.4、httpx 0.28.1。

## 运行

```bat
REM 启动服务，仅监听 127.0.0.1:8080，数据库默认 data\graph_bench.db
scripts\run_server.bat

REM 另开终端：完整自动化测试（25 个用例）
scripts\run_tests.bat

REM 端到端 HTTP 演示（自动拉起/关闭本地服务，打印 13 个场景的核对结果）
python tools\run_demo.py
```

POSIX 环境使用 `scripts/run_server.sh` / `scripts/run_tests.sh`。
可用环境变量：`GRAPH_BENCH_DB`（数据库路径）、`GRAPH_BENCH_HOST`、`GRAPH_BENCH_PORT`、
`GRAPH_BENCH_CRASH_POINT`（崩溃注入，仅测试用）。

## 快速上手

```bat
curl -X POST http://127.0.0.1:8080/snapshots -H "Content-Type: application/json" ^
  -d "{\"request_id\":\"s1\",\"payload\":<samples/snapshot_v1.json 内容>}"
curl http://127.0.0.1:8080/views/layers
curl "http://127.0.0.1:8080/resources/gateway/impact?budget_ms=200"
```

## 目录结构

```
graph_bench/   领域模型、图算法、SQLite 存储、服务层、FastAPI 接口
samples/       确定性 JSON 样本（快照 / 增量 / 环 / 错误摘要）
tests/         pytest 用例，含真实子进程崩溃恢复测试
scripts/       Windows 与 POSIX 启动/测试脚本
tools/         摘要回填工具与端到端 HTTP 演示
docs/design.md 完整技术设计（事务边界、schema、恢复协议、业务码契约）
data/ logs/    运行期生成（已在 .gitignore 忽略）
```

## 验收要点对照

- 规格与快照校验：`E_SPEC_INVALID` / `E_SNAPSHOT_INVALID`，失败不改变已提交状态；
- 增量并发：基础版本不匹配或同基础版本竞争失败均为 `E_STALE_BASE`；
- 环检测：`E_DEPENDENCY_CYCLE` 附有序环路径，同时返回未污染资源分层与受阻清单；
- 级联原子重建：资源、边、派生结构同事务提交，读者只见完整版本；
- 预算查询：超时返回部分结果与未完成标识，绝不跨版本混合；
- 幂等：同 `request_id` 同内容只执行一次，内容冲突返回 `E_CONFLICTING_REQUEST`；
- 中断恢复：PENDING 意图重启后标记 ABORTED，当前版本回滚指向最后完整版本，
  恢复报告仅记录已落盘事实。

实际验证结果见 `artifacts/VERIFICATION.md`。

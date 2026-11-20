# 验证记录（实际运行结果）

- 运行环境：Python 3.14.4（满足 3.11+ 要求），SQLite 标准库，Linux/WSL 本地；
  同样提供 `run_demo.bat` 供 Windows 直接运行，全程无 Docker、无外部网络。
- 依赖版本（本机实际安装的稳定版）：
  fastapi 0.165.0、uvicorn 0.54.0、pytest 9.2.0、httpx 0.28.1、
  starlette 1.10.0、pydantic 2.14.4。
- 固定样本：`samples/manifest.json`，由种子 20261120 的确定性生成器产出。
  - `demo/alpha.obj`：4 片，896 字节，期望摘要
    `31db61a0e66c8fec7fad81f79ce06204e40db81cf961bc7a4e2024c17e5614a3`
  - `demo/bravo.obj`：3 片，384 字节，期望摘要
    `28f5a14dead3b0cf63fc5b341cb6e64ee25fbb97b22e5027038a75e5f19fa9b5`

## 1. 单元/集成测试：28 passed

命令：`python -m pytest` → `28 passed, 1 warning in 1.57s`
（唯一 warning 为 starlette TestClient 的弃用提示，与业务逻辑无关。）

| 测试模块 | 覆盖的业务要求 | 用例数 |
| --- | --- | --- |
| `test_01_sessions_and_validation.py` | 要求 1：冲突策略、非法序号/大小不改动会话、末分片豁免、统一信封 | 6 |
| `test_02_idempotency_and_ordering.py` | 要求 2：幂等、E_PART_MISMATCH 保留首次、客户端摘要校验、乱序合并 | 4 |
| `test_03_merge_validation.py` | 要求 3：E_PART_MISSING/E_SIZE_MISMATCH、无对象部分写入、取消语义 | 5 |
| `test_04_atomic_commit_and_versions.py` | 要求 4：重复合并拒绝、版本单调递增、旧版本不可读为当前 | 2 |
| `test_05_merge_strategies.py` | 要求 5：fast 证据、full 逐片耗时、两路径摘要一致、坏字节检测 | 4 |
| `test_06_concurrency.py` | 要求 6：并发同序号只存一条、并发合并版本 1..N、读端无中间态 | 3 |
| `test_07_recovery.py` | 要求 7：崩溃注入→恢复→重合并摘要一致、空跑、异常意图中止、跨进程重启自动恢复 | 4 |

## 2. 端到端演示（真实 uvicorn 子进程 + httpx）

命令：`python scripts/demo_e2e.py`，退出码 0，五个规定场景全部通过：

1. **乱序提交后合并**：到达顺序 `[3,1,4,2]`，清单存储顺序 `[1,2,3,4]`；
   fast 路径摘要 `31db61a0...`（version=1），同键并存会话 full 路径摘要相同
   （version=2），full 输出每片耗时与总耗时，fast 输出 `parts` 表持久化证据。
2. **重复分片**：相同内容重传返回 `idempotent` 且分片数不增加；不同内容返回
   `E_PART_MISMATCH`，首次提交的 SHA-256 保留。
3. **缺口合并失败**：仅提交 1、3 片时合并返回 `E_PART_MISSING`
   （`missing_part_numbers=[2]`），对象查询返回 `E_OBJECT_NOT_FOUND`，会话仍为
   `open`，不存在部分写入的对象元数据。
4. **取消会话**：取消后状态为 `cancelled`，继续提交分片返回
   `E_SESSION_CANCELLED`；取消接口本身幂等。
5. **强制终止恢复**：合并请求带 `simulate_crash_after_intent=true` 后返回
   `E_SIMULATED_CRASH`(503)，持久化事实为会话 `merging`、对象不可见；
   `POST /api/v1/recovery/run` 报告 `scanned=1 reopened=1 aborted=0`，
   事实条目明确写明“pending 意图已持久化、元数据事务未提交”；恢复后会话回到
   `open` 且 4 个分片全部保留，full 路径重新合并摘要仍为 `31db61a0...`，
   与中断前一致。跨进程重启场景由 `test_recovery_runs_automatically_on_process_restart`
   覆盖：新应用实例 lifespan 启动即自动完成同样的恢复。

## 3. 统一业务码响应抽样

- 缺口合并：`HTTP 409 {"ok":false,"error":{"code":"E_PART_MISSING",...}}`
- 重复内容冲突：`HTTP 409 ... "code":"E_PART_MISMATCH"`
- 非法序号：`HTTP 400 ... "code":"E_PART_INVALID"`，会话计数器前后不变
- 对象不存在：`HTTP 404 ... "code":"E_OBJECT_NOT_FOUND"`
- 崩溃注入：`HTTP 503 ... "code":"E_SIMULATED_CRASH"`
- 请求体不合法：`HTTP 422 ... "code":"E_REQUEST_INVALID"`

## 4. 复现步骤

```bash
cd shard_store
python -m venv .venv && source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt
python scripts/make_samples.py
python -m scripts.init_db --reset
python -m pytest
python scripts/demo_e2e.py
# 或一键：./run_demo.sh （Windows: run_demo.bat）
```

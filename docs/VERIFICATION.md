# 实际验证记录

## 环境

- Python `3.11.18`
- Flask `3.1.3`
- pytest `8.4.2`
- SQLite `3.53.1`
- Linux/WSL 本地文件系统，SQLite 文件数据库；未使用 Docker、容器、外部账号或网络目标。

Python 3.11 虚拟环境位于 `.venv`。Windows 使用 `py -3.11` 创建等价环境。

## 固定样本重建

```bash
python scripts/build_fixtures.py
```

连续两次重建的 SHA-256 完全一致：

```text
41ccb3502b2e30658fbd4f9a97498662977458ce547c525f6c158de704c743d8  data/slowquery_sample.sqlite3
c70f3688763616930e4529b76e766deae768372349c8cda8bd93a3d8321ad6c8  data/snapshot_v1.json
3ece8c016e127b77ace30a462fb7f779bc619ae01b013d1949ef722990b0cc40  data/snapshot_v2.json
d2c39a6ff9ae704787daf24046d948de3463edf29d3dd155ef893c87bc6c3068  data/samples.json
```

## 自动化测试

执行：

```bash
.venv/bin/python -m pytest -q
```

结果：

```text
38 passed in 4.56s
```

覆盖内容：

- 样本负值、浮点、布尔、64 位溢出、非法 SQL/时间与整批回滚。
- 快照摘要、summary 与版本单调性。
- 合法/非法状态迁移、终态隔离、建议原子性、无验证记录不可 `VERIFIED`。
- 四类慢因与四类建议、快照版本引用、无快照禁止建议。
- 验证成功、结果不一致、执行异常、真实连接关闭与临时库删除。
- 同会话 8 路重复诊断只执行一次并复用；验证暂停期间 8 个并发读者不观察中间状态。
- 子进程 `os._exit(86)` 中断分析和验证后的恢复。
- HTTP 统一错误体、快照退休和重复验证复用。

## 端到端演示

执行：

```bash
.venv/bin/python scripts/demo.py --output artifacts/demo_output.txt
```

最终输出：

```text
DEMO PASS
```

关键观测：

| 项目 | 结果 |
| --- | --- |
| 缺索引改写 | `VERIFIED`，13 -> 13 行，摘要相等，代价 250 -> 5 |
| 选择率改写 | `VERIFIED`，0 -> 0 行，摘要相等 |
| 锁等待/超量返回 | `VERIFIED`，247 -> 247 行 |
| OR 拆分 | `VERIFIED`，434 -> 434 行，摘要相等 |
| 返回行过多 | `VERIFIED`，251 -> 251 行，LIMIT 500 未截断 |
| 全表扫描加 LIMIT | `FAILED / E_VERIFY_MISMATCH`，1000 -> 500 行 |
| 资源释放 | 源连接、复制目标连接、验证连接均关闭；临时库删除 |
| 快照退休 | 新会话请求 v1 返回 HTTP 409 `E_SNAPSHOT_RETIRED` |
| 旧会话绑定 | 活动 v2 后原会话仍引用 v1 建议并保持可验证 |
| 分析强杀 | 子进程退出 86，重启后 `ANALYZING -> CREATED`，无建议 |
| 验证强杀 | 子进程退出 86，重启后 `ANALYZING -> FAILED`，无验证记录 |
| 恢复不变量 | 无 SUGGESTED 缺建议、无 VERIFIED 缺记录、无遗留 PENDING |

完整输出见 `artifacts/demo_output.txt`。

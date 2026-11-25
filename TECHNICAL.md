# 列式块副本一致性巡检组件 — 技术说明

## 组件结构

| 包 | 职责 |
| --- | --- |
| `internal/cerr` | 业务错误码（E_MANIFEST_INVALID / E_PAGE_MISMATCH / E_NO_QUORUM / E_CONFLICTING_REQUEST / E_RECOVERY_INCOMPLETE 等）与统一错误信封 |
| `internal/manifest` | 块清单模型、32 字节定长摘要、Merkle 式分块摘要树（叶=页摘要，中间节点=子摘要拼接取 SHA-256，奇节点原样上提） |
| `internal/sample` | 固定种子确定性样本生成器（清单、页数据、多副本报告），同一 Spec 产出逐位一致 |
| `internal/verify` | 叶级/中间层核验、多副本分叉定位与严格多数共识、增量核验 |
| `internal/trust` | 定点（Scale=1e6）可信度与损坏范围计算，逐步记录舍入方式与累计舍入误差 |
| `internal/store` | SQLite（modernc.org/sqlite，纯 Go 无 CGO）意图持久化、检查点提交、崩溃恢复 |
| `internal/service` | 单写者编排、请求幂等、意图先行的事务提交 |
| `internal/api` | 本地 HTTP JSON API，统一错误响应 |
| `cmd/blockinspectd` | API 服务进程（启动时自动执行恢复扫描） |
| `cmd/verifyrun` | 可复现验证工具（固定种子，全量/增量/分叉/强制终止恢复各一次） |

## 关键设计

- **清单校验**：页数必须为正整数；所有摘要必须为 32 字节定长；根摘要必须与叶摘要集合重建出的树根一致（根与最底层叶数量的对应关系）。校验失败返回 `E_MANIFEST_INVALID`，已加载的旧清单保持可用（原子替换）。
- **摘要核验**：页数据缺失或长度与清单不符立即返回 `E_PAGE_MISMATCH`（附期望/实际长度），不会跳过错误页继续向上汇总。分叉输出首个不一致页号、首次分叉层级、分叉处左右子摘要及两侧摘要所属副本。
- **共识判定**：按根摘要分组计票，严格多数（>N/2）胜出；无多数时返回 `E_NO_QUORUM` 并附各候选摘要得票，不默认任何副本胜出。共识页摘要集合取自字典序首个法定副本，保证确定性。
- **增量核验**：仅对声明变化的页重算叶摘要，沿受影响路径重算中间节点（`AffectedNodes`），未变化子树复用登记值；输出重算/复用节点数与定点节省比例。结果为输入的纯函数，同块同检查点重复核验逐位一致。
- **可信度**：`trust = (Scale - 0.7*divPages/pageCount - 0.3*divLevel/maxLevel) * quorum/total`，全部使用 int64 定点（Scale=1_000_000），每步记录 `ROUND_HALF_UP` 与精确舍入残差（有理数），输出累计舍入误差（微单位）。比较使用相对容差 0.1%，并列时按副本标识字典序稳定破平。损坏范围由分叉页分布推出起始/结束页与受影响列。
- **并发与幂等**：每块一把写锁（单写者串行）；只读查询只读 SQLite 已提交版本，单事务提交保证不会看到“页已核验而根未落定”的中间态。同一 `request_id` 相同负载复用结果（在途则等待），负载不同返回 `E_CONFLICTING_REQUEST`。
- **意图持久化与恢复**：先写 `intents(pending)` 再单事务提交（检查点推进 + 页/节点摘要 + 共识结论 + 请求结果 + 意图置 done）。强制终止后重启扫描 pending 意图并标记 aborted；由于单事务提交，不存在半提交状态，摘要树与共识结论自然停留在最后一个完整检查点。恢复报告只含已落盘事实。未提交请求查询返回 `E_RECOVERY_INCOMPLETE`。

## HTTP API

- `POST /v1/sample/generate` 生成固定种子样本并加载清单
- `POST /v1/manifest/load` `{path}` 加载清单；`GET /v1/manifest/{block}` 查询
- `POST /v1/reports` 提交副本摘要报告
- `POST /v1/verify` 触发核验 `{request_id, block_id, mode: full|incremental, checkpoint_id, changed_pages}`
- `GET /v1/verify/{request_id}` 查询分叉定位与可信度明细
- `GET /v1/blocks/{block}/incremental` 增量化统计
- `GET /v1/recovery` 恢复报告

失败响应统一为 `{"error":{"code","message","context"}}`。

## 运行

```sh
go build ./...
go test ./...
scripts/verify.sh        # Windows: scripts/verify.ps1
```

## 实际验证结果（固定种子 seed=20261125，96 页，3 副本）

`cmd/verifyrun` 共 28 项检查全部通过：

- 全量核验：共识根 `53f55f19…e1ace5`，首个分叉页 7，分叉层 0，两侧子摘要及所属副本（replica-a/b vs replica-c）均输出
- 可信度：replica-a/b = 0.666667，replica-c = 0.652083，累计舍入误差 333333.333333 微单位，9 步计算逐步记录
- 损坏范围：起始页 7、结束页 41、受影响列 24 个
- 增量核验：变化 3 页，重算 15 节点、复用 177 节点，节省比例 0.921875；同检查点重复核验逐位一致
- 错误码：E_MANIFEST_INVALID / E_PAGE_MISMATCH(期望 578 实际 574) / E_NO_QUORUM(得票 2/1/1/1) / E_CONFLICTING_REQUEST / E_RECOVERY_INCOMPLETE 均按约定返回
- 强制终止恢复：意图写入后 `os.Exit(2)` 模拟强杀，重启后恢复报告列出已中止意图，最后完整检查点保持可用

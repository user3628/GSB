# 增量体素重建服务 — 技术说明

## 1. 总览

本组件把多视角灰度分片与 JSON 场景清单重建为稀疏体素场，固定四阶段流水线：

1. `projection` 视角投影采样
2. `local_volume` 视角局部体素贡献体
3. `global_fusion` 全局融合与稀疏化
4. `validation` 真值数值校验

全部代码位于 `internal/pipeline`，HTTP 入口在 `cmd/voxelrecon`。无第三方数据/模型
依赖；唯一外部模块是 `github.com/mattn/go-sqlite3`（CGO）与
`golang.org/x/sys`（跨平台文件锁），并已 `go mod vendor` 以支持离线构建。

## 2. 确定性与逐字节一致

- 固定种子生成器 `generator.go` 使用内置 LCG（不依赖 `math/rand`），同种子在任意
  Go 版本/平台产出相同 PNG 像素与 JSON 清单。
- 所有产物经 `digest.go` 的规范 JSON 编码落盘：map 键排序、float64 统一
  `strconv.FormatFloat('g',-1,64)`，拒绝 NaN/Inf 进入指纹。
- 几何量在投影阶段统一保留 9 位小数（`round6`），体素按整数 key 排序，融合按
  分片序 → 视角序 → 体素 key 序遍历，保证与求和顺序无关的可复算结果。
- 增量结果通过 `VerifyFieldEquivalence` 与纯内存 `ComputeFieldFromScratch`
  做整段规范字节比对；不一致返回 `E_INCREMENTAL_MISMATCH` 并附首个差异体素坐标。

## 3. 四阶段要点

- 投影：像素反投影为世界射线（Xc=R·Xw+T 的逆变换），在表面深度的截断带内按
  `ray_step` 采样。旋转非正交、焦距为零返回带 `shard_id/view_id/field` 的
  `E_POSE_INVALID`；越界像素返回 `E_SAMPLE_OUT_OF_RANGE`；NaN 像素返回
  `E_NAN_INPUT`，均不静默跳过。
- 局部体素：量化到整数体素，显式保留 `color_sum/weight_sum/hits`，同视角多次命中
  按投影到达顺序合并。
- 全局融合：Chebyshev 融合半径核给出显式权重，加权平均与归一化完全可由
  `color_sum/weight_sum` 复算；低于 `sparsity_prune` 命中数的体素被剪枝并记录原因。
- 校验：以清单真值为唯一真值，计算最近体素中心逐点偏差与重投影像素误差，超差返回
  带 `view_id/max_deviation` 的 `E_NUMERIC_MISMATCH`。

## 4. 自适应路径取舍（planner.go）

输入：分片数、视角数、估算体素数、相机姿态变化范围、全局预算。固定阈值：

- 小场景（体素 ≤ 20000 且分片 ≤ 4）：`SPARSE_INCREMENTAL` 不可选并记录淘汰原因。
- 大场景（体素 ≥ 200000 或分片 ≥ 16）：`FULL_RECOMPUTE` 不可选并记录淘汰原因。
- 其余按确定性成本模型给出 0–100 适应度分数，选最高分；平分按策略名字典序。
  返回 `PlanRecord`，含候选分数、资格表、淘汰原因与可读选择理由。

## 5. 缓存与延迟预算（cache.go）

缓存键 = 查询类型 + 场景内容指纹 + 配置版本 + 相机指纹。命中返回与重算完全相同的
规范字节。计算在 goroutine 中执行并由 `context.WithTimeout` 约束；超时立即返回
`E_LATENCY_BUDGET`，含 `query_id` 与 `elapsed_ms`，不阻塞等待。

## 6. 检查点、文件锁与恢复（store.go / lock*.go / atomicio.go）

- 产物经“同目录临时文件 + fsync + 原子 rename”落盘，崩溃只留下完整旧文件或新文件。
- 检查点写入 SQLite，按分片维护单调连续序号；条件更新拒绝跳号、重复、跨分片错位，
  统一返回 `E_STALE_CHECKPOINT`。
- 同主键整体替换被分类为：序号变小=回退（`E_CHECKPOINT_ROLLBACK`），同序号哈希
  变化=篡改（`E_CHECKPOINT_TAMPERED`），序号连续且内容更新=正常推进（接受）。
- 跨进程互斥使用真实 OS 文件锁：Unix `flock(2)`，Windows `LockFileEx`。
- 优雅停止在当前不可拆分分片单元结束后退出；恢复仅依赖磁盘检查点，不依赖内存状态。

## 7. 版本变化与增量失效（incremental.go / fingerprint.go）

每视角保存图像指纹与相机指纹，每阶段保存输入摘要。变化映射到最早失效阶段：

- 像素 / 相机内外参 / 采样密度 → `projection`
- 体素分辨率 / 截断距离 → `local_volume`
- 融合半径 / 稀疏阈值 → `global_fusion`

失效阶段及其全部下游（`DownstreamStages`）重建；未受影响视角的局部贡献体在通过
工件哈希校验后继续复用。

## 8. 业务错误码

E_POSE_INVALID、E_NAN_INPUT、E_EMPTY_VIEWS、E_BAD_DENSITY、E_SAMPLE_OUT_OF_RANGE、
E_LATENCY_BUDGET、E_STALE_CHECKPOINT、E_CHECKPOINT_TAMPERED、E_CHECKPOINT_ROLLBACK、
E_INCREMENTAL_MISMATCH、E_NUMERIC_MISMATCH、E_NOT_FOUND、E_INVALID_REQUEST、
E_LOCKED、E_INTERNAL。所有错误为同一 `BizError` 结构，HTTP JSON 体与 stderr
输出字段一致。

## 9. HTTP 接口

| 方法 | 路径 | 用途 |
| --- | --- | --- |
| POST | `/api/dataset/generate` | 固定种子生成 PNG 分片与清单 |
| GET  | `/api/dataset/manifest` | 读取清单 |
| PUT  | `/api/shards/{shard}/{view}` | 导入/替换 PNG 分片 |
| GET/PUT | `/api/config` | 读取/修改融合参数（产生新配置版本） |
| POST | `/api/reconstruct` | 触发重建，支持 force_full、verify_equality |
| POST | `/api/stop` | 请求优雅停止 |
| GET  | `/api/jobs/{id}` | 查询作业状态 |
| GET  | `/api/query/summary`、`/api/query/prune`、`/api/query/errors` | 延迟预算内查询 |
| GET  | `/api/checkpoints` | 检查点列表 |

## 10. 构建与验证

```bash
go test ./...                  # 单元+集成测试
go test -race ./...            # 竞态检测
GOOS=windows go build ./...    # Windows 交叉编译
./scripts/recovery_demo.sh     # 独立进程强杀-重启恢复演示
```

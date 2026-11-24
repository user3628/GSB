# MotionLab 技术设计

## 1. 确定性边界

系统把“逻辑结果”定义为一组可按分支号与步号排序的事实：

- 初始状态与初始帧摘要；
- 每步完整 `MotionState` 的规范 JSON 及 SHA-256；
- 每步预测 RGB 像素的 SHA-256 与真值 RGB SHA-256；
- 每步 MAE、取值范围标记、裁决；
- 分支最终状态、最终帧、误差曲线、配额与裁决；
- 运行配置、策略版本与策略指纹。

规范报告指纹不包含墙钟时间、恢复次数、物理事务交错产生的主键/全局提交序号。这些属于审计元数据，不参与逻辑结果。分支步骤本身按 `(branch_id, step_no)` 排序，因此并发提交顺序不影响指纹。

固定性来自以下实现选择：

- 自实现 SplitMix64，而不是依赖标准库伪随机实现；
- 浮点状态以字段固定顺序 JSON 序列化，并在恢复时按 IEEE-754 往返；
- 渲染只使用共享初始纹理与分支状态，不读取墙钟、环境随机量或 GPU；
- 所有误差与汇总统一从 SQLite 已提交步骤重算，避免“连续执行内存累计”和“恢复执行 DB 读取”之间出现路径差异。

## 2. 运动方程

记第 `t` 步状态为 `S_t=(x,y,vx,vy,sx,sy,r,e)`。先应用分支参数：

```text
x'  = x + vx * velocity_scale
y'  = y + vy * velocity_scale
sx' = sx + 0.0042 + |vx| * 0.00022 + deform_drift
sy' = sy + 0.0028 + |vy| * 0.00030 - deform_drift * 0.35
r'  = r + sin(t * 0.41) * 0.004 + |deform_drift| * 0.03
```

若 `t == direction_change_step` 且突变尚未应用：

```text
vx' = vx * turn_x
vy' = vy * turn_y
r'  = r' + 0.14
e'  = true
```

最后：

```text
vx'' = vx' * velocity_decay * decay_scale
vy'' = vy' * velocity_decay * decay_scale
```

真值与分支 0 的 `velocity_scale=1, turn_x=-0.78, turn_y=0.62, decay_scale=1, deform_drift=0`，因此两者状态与帧逐字节一致。其他分支是确定性生成的运动假设；部分分支在突变后快速偏离并被剪枝。

## 3. 渲染

初始帧由固定种子生成：低对比网格背景、带条纹的暖色方块、青紫色渐变色块。不可变 `SharedFrame` 为每个分支提供：

- 共享初始帧；
- 无物体背景；
- 方块矩形纹理；
- 色块矩形纹理。

每步渲染：

1. 从背景 RGB 复制输出画布；
2. 方块中心按状态位移，在目标包围盒内做逆映射与双线性纹理采样；
3. 色块按 `scale_x/scale_y`、旋转和耦合位移做逆仿射映射；
4. 读取源纹理四个邻域像素，按 `(1-tx)(1-ty)` 等权重插值；
5. 以浮点颜色混合背景，统一经 `Quantize8` 量化为 `[0,255]`；
6. 对最终 RGB 字节计算 SHA-256。

边界为零不透明：源坐标超出纹理时跳过该目标像素。量化使用 `round(v * 255)` 并饱和截断，保证平台一致。

## 4. 调度计划

对每个分支：

```text
target = min(requested_steps, branch_quota)
cost   = target * 1 vms + 2 vms
```

每波接纳数：

```text
byConcurrency = global_concurrency
byBudget      = floor(round_budget_ms / cost)
perWave       = min(byConcurrency, byBudget)
```

若 `perWave == 0`，该分支以 `not_started` 持久化，并写入精确的 `E_TIME_BUDGET` 原因。否则按分支号分配波次：

```text
wave = 1 + admitted_index / perWave
```

执行器一次只启动一个波次的 goroutine；同波次分支并行推进，下一步要等该波结束。这样实际并发 goroutine 数严格不超过全局上限，计划本身又是确定性的。

`stage_mode=true` 时，执行器完成一个波次后主动把 run 置为 `paused`；下一次恢复继续下一个波次。它与优雅停止共用已提交事实恢复路径，但暂停点固定在波次边界。

**为何使用虚拟毫秒**：若按墙钟决定接纳，机器负载会使同一配置在不同运行中得到不同分支集合，破坏剪枝等价与恢复一致性。虚拟毫秒把“时间预算”转成可重放的确定性工作预算。实际墙钟吞吐仅用于观测，不参与裁决。

## 5. 每步事务

`Store.CommitStep` 的顺序：

1. 获取 scope 对应的操作系统文件锁；
2. `BEGIN` SQLite 事务；
3. 读取 run 当前 `checkpoint_seq` 与 `policy_hash`；
4. 校验期望序号；期望为 0 时自动取 `current + 1`；
5. 条件更新 `runs.checkpoint_seq = current + 1 WHERE checkpoint_seq = current`；
6. 插入有效步骤记录，包含预测 PNG；
7. 更新分支进度与最终裁决；
8. 追加剪枝/完成事件；
9. 插入 `(scope, seq)` 检查点记录；
10. `COMMIT`，释放文件锁。

`steps` 的部分唯一索引：

```sql
CREATE UNIQUE INDEX ux_steps_active
ON steps(run_id, branch_id, step_no)
WHERE valid = 1;
```

策略替换将旧记录 `valid=0`，从而允许同一逻辑步在新 epoch 重建，同时保留审计历史。

## 6. 恢复

启动运行或恢复时：

1. 读取 run 的配置与策略；
2. 读取全部分支行；
3. 只把 `pending/running/stopped` 视为可继续；
4. 每分支从已持久化的 `state_json`、`error_curve_json`、`next_step` 继续；
5. 从步骤表重算最终误差汇总，而不是信任内存；
6. 全部终止后写入最终报告与指纹。

若进程在步骤事务提交前死亡，该步不存在；若在提交后、下一渲染前死亡，该步完整存在。不存在“半提交”分支状态。

## 7. 文件锁与条件更新

- Unix：`syscall.Flock(fd, LOCK_EX|LOCK_NB)`；
- Windows：`windows.LockFileEx(LOCKFILE_EXCLUSIVE_LOCK|LOCKFILE_FAIL_IMMEDIATELY)`；
- 获取失败在 context/deadline 内重试；
- 进程死亡时操作系统自动释放锁，不需要清理遗留锁文件。

即使错误地有多个工作进程同时尝试发布：

- 文件锁把检查与写入串行化；
- SQLite 条件更新再次校验 `checkpoint_seq`；
- `steps` 唯一索引第三次防止同分支同步重放。

三层保护分别覆盖跨进程、事务条件和逻辑记录唯一性。

## 8. 策略分类

`ClassifyPolicy(current, next)`：

```text
same hash                         -> advance (identical)
next.version < current.version    -> rollback error
version+1 && parent match && same ops -> advance
otherwise                         -> replacement
```

`replacement` 在 run 事务内：

- `policy_epoch += 1`；
- 更新策略快照与运行配置；
- 旧 steps 全部 `valid=0`；
- 删除分支行并插入新计划；
- `checkpoint_seq = 0`；
- 清空最终报告；
- 写 `policy_events`。

新检查点 scope 为 `run:<id>:e<epoch>`，不会与旧 epoch 的序号冲突。

## 9. 错误模型

所有业务错误实现同一结构化 JSON：

```json
{
  "code": "E_TIMELINE_OVERFLOW",
  "message": "...",
  "sequence_id": "harbor_slide",
  "branch_id": 2,
  "details": {"overflow_steps": 3}
}
```

`branch_id` 使用指针，确保合法的分支 `0` 不会被省略。HTTP 状态映射：验证 400、溢出/共享帧 422、检查点/策略冲突 409、缺失 404。

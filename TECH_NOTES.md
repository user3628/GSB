# 技术说明与验证记录

## 1. 确定性设计

- **哈希**：`prefalign/core/hashing.py` 实现 FNV-1a 64 位、SimHash 与带种子哈希，
  纯 Python 实现，不受 `PYTHONHASHSEED` 影响，跨平台逐字节一致。
- **规范化 JSON**：排序键、`separators=(",",":")`、`ensure_ascii=False`、禁止 NaN，
  所有产物与检查点通过同一 canonical 序列化落盘。
- **随机**：演示生成器固定种子 `20240917`；分折用 `seeded_hash64(group_id, fold_seed)
  % n_folds`；层内抽样用由层名与采样种子派生的 `random.Random`，无全局随机状态。
- **浮点**：质量分按配置有效位数取整；最大余数法对近似整数配额加 1e-9 容差，
  避免 `149.99999999999997` 一类浮点漂移破坏确定性配额。

## 2. 阶段指纹与失效链

阶段 `s` 的指纹 = `hash64(canonical({input, config_s, upstream}))`，其中
`config_s` 只包含该阶段实际依赖的配置子树（见 `config.STAGE_CONFIG_KEYS`）。
因此：

| 变更 | 最早失效阶段 | 复用 |
| --- | --- | --- |
| 样本编辑 / source 分片替换（输入摘要变化） | normalize | 无 |
| `quality.weights` / `source_priors` | quality | normalize |
| `folds.*` / `features.*` | features | normalize, quality |
| `sampling.target_weights` / `sampling_seed` | sampling | 前 3 阶段 |

失效时 `checkpoint.truncate_from` 删除该阶段及下游检查点与产物，并在
`workspace/state/invalidation_log.json` 追加原因、阶段链与新指纹。

## 3. 一致性保证

- 全量重建在 `workspace/verify_full/` 复制一份干净工作区（保留输入注册表与编辑
  日志、排除检查点/产物/锁），从零跑四阶段，再逐阶段比较 `artifact_digest`。
- 增量入口（编辑、替换、配置变更）后调用 `Pipeline.verify_incremental`，
  任一阶段摘要不同即抛 `E_INCREMENTAL_MISMATCH`，附 `first_diff_stage` 与差异位置。
- 48 个 pytest 用例与 `scripts/run_demo.py` 在当前环境均验证 `matched`。

## 4. 检查点与并发

- 原子写：同目录临时文件 + `fsync` + `os.replace`，崩溃只可能出现完整新文件或旧文件。
- `latest_consistent` 只接受序号 1..N 连续且 schema 完整的前缀，损坏 JSON 被忽略。
- 发布在 `workspace/locks/publish.lock` 上 `flock(LOCK_EX)`，校验期望序号、前置
  阶段存在性，并拒绝任何重复/错位发布（`E_STALE_CHECKPOINT` / `E_INVALID_STAGE`）。
- 优雅停止：标记文件在每阶段执行前检查，停止时不启动任何新阶段单元；恢复从最近
  一致检查点继续。测试 `test_stop_after_first_stage_resumes_remaining` 覆盖“完成
  第一阶段后停止、再恢复剩余三阶段、序号 1..4 连续”。

## 5. Windows 风格路径

配置 `storage.*` 与输入注册表全部存反斜杠逻辑路径；`paths.resolve` 在 POSIX 上
剥离可选盘符并相对项目根拼接，在原生 Windows 上直接使用绝对路径。测试见
`tests/test_paths.py`。

## 6. 本次实际验证结果（环境：Ubuntu 26.04 / CPython 3.11.18 venv）

- 依赖：numpy 2.4.6、flask 3.1.3、pytest 9.2.0、pyyaml 6.0.3
- 数据：4 分片 60 条（52 有效 + 8 异常），28 个 prompt 组，3 折均非空
- `scripts/run_demo.py`：全量构建 → 编辑失效全链 → 停止/恢复 → 并发发布被拒
  （`E_STALE_CHECKPOINT`）→ 增量/全量四阶段 `matched` → 混合集导出成功
- HTTP 实测（127.0.0.1）：health/import/run/status/edit/stop/resume/verify/export
  全部返回预期，非法 stage 返回 HTTP 400
- `pytest`：48 passed

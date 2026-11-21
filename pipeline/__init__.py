"""本地确定性训练/推理一体化数据处理管线。

四个阶段：
  stage1 特征化 -> stage2 模型拟合 -> stage3 推理判定 -> stage4 结果汇总

所有产物均以规范化 JSON 落盘并附带内容摘要，支持增量失效、检查点恢复与
全量一致性校验。纯本地运行，不依赖 GPU 或外部服务。
"""

__version__ = "1.0.0"

STAGES = ("stage1_features", "stage2_model", "stage3_inference", "stage4_summary")

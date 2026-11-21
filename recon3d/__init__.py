"""recon3d - 本地确定性多视角图像三维重建管线。

四个阶段：特征提取 -> 多视角三角化 -> 几何一致性裁剪/降采样 -> 结果封装。
所有产物均为内容寻址、规范化序列化，保证跨运行与增量/全量重建逐字节一致。
"""

__version__ = "1.0.0"

STAGE_FEATURES = "features"
STAGE_TRIANGULATION = "triangulation"
STAGE_GEOMETRY = "geometry"
STAGE_PACKAGING = "packaging"

PIPELINE_STAGES = (
    STAGE_FEATURES,
    STAGE_TRIANGULATION,
    STAGE_GEOMETRY,
    STAGE_PACKAGING,
)

GLOBAL_SCOPE = "global"

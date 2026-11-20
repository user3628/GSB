"""本地多组件配置分发与状态协调管线。"""

from .coordinator import Coordinator
from .errors import DispatchError

__all__ = ["Coordinator", "DispatchError"]

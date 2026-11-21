"""GraySwitch：本地灰度路由与流量镜像中间件。

该包的所有比例类计算都在显式定点（:mod:`grayswitch.app.fixedpoint`）上完成，
禁止在未声明的情况下混用二进制浮点与定点。
"""

__all__ = ["__version__"]
__version__ = "1.0.0"

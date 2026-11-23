"""业务路由汇总：所有模块共用一套资源路由，由领域服务完成分派。"""
from __future__ import annotations

from app.routers import resource
from app.routers import triple_source

# 固定路径的 triple-source 接口必须排在通用 /api/{module} 动态路由之前，
# 否则会被资源路由抢先匹配。
ROUTERS = [triple_source, resource]

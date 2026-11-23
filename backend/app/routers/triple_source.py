"""版本化三元组来源索引 v2 的迁移、切换与溯源查询接口。

固定路径 /api/triple-source/*，必须在通用资源路由 /api/{module} 之前注册，
避免被动态模块路径抢先匹配。
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter
from pydantic import BaseModel, Field

from app.services.extract_lineage import ExtractLineageService
from app.services.triple_source import (
    MigrationInterrupt,
    triple_source_service,
)

router = APIRouter(prefix="/api/triple-source", tags=["三元组来源索引v2"])


class MigrationPayload(BaseModel):
    max_edges: int | None = Field(default=None, ge=1)
    fail_after: int | None = Field(default=None, ge=1)


class SwitchPayload(BaseModel):
    version: str


@router.post("/migration")
def run_migration(payload: MigrationPayload) -> dict[str, Any]:
    """运行（或中断恢复）v1.0 -> v2 来源索引迁移，分批/中断均返回当前进度。"""
    try:
        return triple_source_service.run_migration(
            max_edges=payload.max_edges, fail_after=payload.fail_after
        )
    except MigrationInterrupt as exc:
        # 中断不是失败：检查点与 staging 已落盘，再次调用即可恢复
        return {
            "status": "interrupted",
            "resumable": True,
            "message": str(exc),
            "checkpoint": triple_source_service._read_migration_checkpoint(),
        }


@router.post("/switch")
def switch_index(payload: SwitchPayload) -> dict[str, Any]:
    """切换激活的索引版本（v1.0 / v2 共存）。"""
    return triple_source_service.switch_active(payload.version.strip())


@router.post("/rollback")
def rollback_index() -> dict[str, Any]:
    """回滚到旧图谱 v1.0，封存的 v2 索引保留落盘但不再权威。"""
    return triple_source_service.rollback()


@router.get("/deployment")
def deployment() -> dict[str, Any]:
    """查询部署状态：激活索引、封存状态、迁移与切换轨迹。"""
    return triple_source_service.deployment()


@router.get("/history")
def history(
    triple_id: str | None = None,
    head: str | None = None,
    status: str | None = None,
    graph_file: str | None = None,
) -> dict[str, Any]:
    """历史数据查询：旧图谱边始终可读，v2 证据按封存索引标注，staging 不暴露。"""
    return triple_source_service.history(
        triple_id=triple_id, head=head, status=status, graph_file=graph_file
    )


@router.get("/records")
def records(status: str | None = None) -> dict[str, Any]:
    """来源记录列表，可按 complete_source/rewritten_source/pending_review 过滤。"""
    return triple_source_service.records(status=status)


@router.get("/lineage/{triple_id}")
def lineage(triple_id: str, graph_file: str | None = None) -> dict[str, Any]:
    """抽取血缘反查：边 -> 关系集合 -> 关系证据 -> 原分块的可核对溯源明细。"""
    return ExtractLineageService().trace_triple_source(triple_id, graph_file=graph_file)

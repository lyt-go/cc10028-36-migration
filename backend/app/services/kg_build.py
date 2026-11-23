"""知识图谱服务：实体关系三元组的图谱入图、版本管理与来源分块追溯。

从关系抽取产物读取三元组并构建知识图谱，将三元组持久化为图谱快照文件。
"""
from __future__ import annotations

import time
from typing import Any

from app.services.base import BaseModuleService
from app.store import store


class KgBuildService(BaseModuleService):
    MODULE = "kg_build"
    ENTITY = "图谱三元组"
    OPERATION_TERM = "建图"
    LIST_FIELDS = ["三元组编号", "头实体", "谓词关系", "尾实体", "置信度", "来源分块", "图谱版本", "入图时间"]
    REQUIRED_FIELDS = ["三元组编号", "头实体", "谓词关系"]
    STATUS_ORDER = ["草稿", "待建图", "已建图", "已废弃"]
    ACTION_RULES = {"启动建图": "待建图", "确认建图": "已建图", "标记废弃": "已废弃"}
    NEGATIVE_ACTIONS = ["标记废弃"]

    GRAPH_VERSION = "v1.0"

    def build_from_relations(self, relation_set_id: str) -> dict[str, Any]:
        """从关系抽取产物构建知识图谱三元组，将三元组写入图谱快照文件。"""
        relation_file = self._data_dir("relations") / f"{relation_set_id}_relations.jsonl"
        relations = self._read_jsonl(relation_file)
# 数据操作：入湖三元组数据构建知识图谱节点与边数据
        if not relations:
            return {"error": f"关系抽取产物 {relation_set_id} 不存在或为空，请先执行关系抽取"}

        triples = []
        for rel in relations:
            if rel.get("confidence", 0) < 0.6:
                continue
            triple_id = f"TRIPLE-{rel['relation_id']}"
            triples.append({
                "triple_id": triple_id,
                "head": rel["head_entity"],
                "relation": rel["relation_type"],
                "tail": rel["tail_entity"],
                "confidence": rel["confidence"],
                "source": relation_set_id,
                "graph_version": self.GRAPH_VERSION,
                "ingested_at": time.time(),
            })

        # 去重：相同头-关系-尾的保留置信度最高的
        triples = self._deduplicate_triples(triples)

        output_path = self._data_dir("graph") / f"{relation_set_id}_graph.jsonl"
        written = self._write_jsonl(output_path, triples)

        # 建图期关系文件校验和：作为旧图边历史来源的证据基线，
        # 迁移时用于识别同名关系文件后来的更正，避免把改写后内容补成确定来源。
        relation_file_checksum = self._compute_checksum(
            relation_file.read_text(encoding="utf-8")
        ) if relation_file.exists() else None

        # 生成图谱边和节点的统计
        nodes = set()
        edges = []
        for t in triples:
            nodes.add(t["head"])
# 数据产物：图谱三元组数据集写入graph目录持久化文件
            nodes.add(t["tail"])
            edges.append(f"{t['head']}→[ {t['relation']} ]→{t['tail']}")

        self._write_checkpoint(f"graph_{relation_set_id}", {
            "relation_set_id": relation_set_id,
            "version": self.GRAPH_VERSION,
            "nodes": len(nodes),
            "edges": len(edges),
            "min_confidence": min((t["confidence"] for t in triples), default=0),
            "avg_confidence": sum(t["confidence"] for t in triples) / max(len(triples), 1),
            "relation_file_checksum": relation_file_checksum,
        })

        # v2 来源索引激活时，把当前边实时登记进已封存索引（内部自带激活态判断，
        # v1 激活/未封存时返回 None，绝不把新边混进旧图）。
        live_indexed = 0
        try:
            from app.services.triple_source import triple_source_service

            for triple in triples:
                if triple_source_service.index_triple_live(triple, relation_set_id) is not None:
                    live_indexed += 1
        except Exception:
            live_indexed = 0

        return {
            "relation_set_id": relation_set_id,
            "triples": len(triples),
            "nodes": len(nodes),
            "edges": len(edges),
            "records_written": written,
# 数据导出：三元组导出文件写入导出目录供下游下载
            "output": str(output_path),
            "live_indexed_v2": live_indexed,
        }

    def _deduplicate_triples(self, triples: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """按头实体、关系、尾实体去重，保留置信度最高的三元组。"""
        seen = {}
        for t in triples:
            key = (t["head"], t["relation"], t["tail"])
            if key not in seen or t["confidence"] > seen[key]["confidence"]:
                seen[key] = t
        return sorted(seen.values(), key=lambda x: x["triple_id"])

    def query_graph(self, entity: str) -> dict[str, Any]:
        """从图谱中查询与指定实体相关的三元组。"""
        graph_files = list(self._data_dir("graph").glob("*.jsonl"))
        results = []
        for gf in graph_files:
            triples = self._read_jsonl(gf)
            for t in triples:
                if entity in (t.get("head", ""), t.get("tail", "")):
                    results.append(t)
        return {"entity": entity, "related_triples": len(results), "triples": results}
"""抽取血缘服务：源文件到解析、分块、抽取产物与下游消费方的全链路血缘记录。

记录文档从接入到抽取全过程的血缘链路，提供数据产品的追溯能力。
"""
from __future__ import annotations

import time
from typing import Any

from app.services.base import BaseModuleService
from app.store import store


class ExtractLineageService(BaseModuleService):
    MODULE = "extract_lineage"
    ENTITY = "血缘链路"
    OPERATION_TERM = "采集"
    LIST_FIELDS = ["链路编号", "源文件", "解析节点", "分块节点", "抽取产物", "下游消费方", "血缘版本", "采集时间"]
    REQUIRED_FIELDS = ["链路编号", "源文件", "解析节点"]
    STATUS_ORDER = ["草稿", "待校验", "已生效", "已停用"]
    ACTION_RULES = {"提交校验": "待校验", "确认生效": "已生效", "标记停用": "已停用"}
    NEGATIVE_ACTIONS = ["标记停用"]

    def record_from_parse(self, parse_task_key: str) -> dict[str, Any]:
        """从解析任务记录全链路血缘，采集源文件→解析→分块→抽取→下游的全部节点。"""
        # 查找batch记录获取源文件
        batch_files = sorted(self._data_dir("batches").glob("*.jsonl"))
        source_file = parse_task_key  # 默认来源文件即批次键

# 数据操作：采集血缘链路数据记录源文件到下游的全链路节点
        # 检查各级产物的存在性
        parsed_exists = (self._data_dir("parsed") / f"{source_file}_parsed.jsonl").exists() if source_file else False
        chunks_exists = (self._data_dir("chunks") / f"{source_file}_parsed_chunks.jsonl").exists() if source_file else False
        entities_exists = (self._data_dir("entities") / f"{source_file}_parsed_entities.jsonl").exists() if source_file else False
        relations_exists = (self._data_dir("relations") / f"{source_file}_parsed_relations.jsonl").exists() if source_file else False
        graph_exists = (self._data_dir("graph") / f"{source_file}_parsed_graph.jsonl").exists() if source_file else False
        vectors_exists = (self._data_dir("vectors") / f"{source_file}_parsed_vectors.jsonl").exists() if source_file else False

        # 构建血缘节点列表
        nodes = []
        if source_file:
            nodes.append(f"源文件: {source_file}")
        if parsed_exists:
            nodes.append(f"解析节点: {source_file}_parsed")
        if chunks_exists:
            nodes.append(f"分块节点: {source_file}_chunks")
        if entities_exists:
            nodes.append(f"实体抽取: {source_file}_entities")
        if relations_exists:
            nodes.append(f"关系抽取: {source_file}_relations")
        if graph_exists:
            nodes.append(f"图谱节点: {source_file}_graph")
        if vectors_exists:
            nodes.append(f"向量索引: {source_file}_vectors")

# 数据产物：血缘链路数据写入lineage目录持久化文件
        # 确定下游消费方
        downstream = []
        if graph_exists:
            downstream.append("知识图谱系统")
        if vectors_exists:
            downstream.append("向量检索引擎")
        if entities_exists and relations_exists:
            downstream.append("数据可视化平台")
        if not downstream:
            downstream.append("待确认")

        lineage = {
            "链路编号": f"LINEAGE-{parse_task_key}",
            "源文件": source_file or parse_task_key,
            "解析节点": f"format_parse::{parse_task_key}" if parsed_exists else "未解析",
            "分块节点": f"chunk_split::{source_file}" if chunks_exists else "未切分",
            "抽取产物": f"entity+relation::{source_file}" if (entities_exists or relations_exists) else "未抽取",
            "下游消费方": ", ".join(downstream),
            "血缘版本": "v1.0",
            "采集时间": time.strftime("%Y-%m-%d %H:%M:%S"),
        }

        output_path = self._data_dir("lineage") / f"{parse_task_key}_lineage.jsonl"
        self._write_jsonl(output_path, [lineage])

        self._write_checkpoint(f"lineage_{parse_task_key}", {
            "parse_task_key": parse_task_key,
            "nodes_count": len(nodes),
            "downstream_systems": downstream,
            "coverage": {
                "parsed": parsed_exists,
                "chunks": chunks_exists,
                "entities": entities_exists,
                "relations": relations_exists,
                "graph": graph_exists,
                "vectors": vectors_exists,
            },
        })

        return {
            "parse_task_key": parse_task_key,
            "nodes": len(nodes),
            "downstream": downstream,
            "output": str(output_path),
        }

    def trace_upstream(self, artifact_path: str) -> dict[str, Any]:
        """反向追溯：给定一个抽取产物文件路径，查找其所有上游来源。"""
        lineage_files = sorted(self._data_dir("lineage").glob("*.jsonl"))
        upstream = []
        for lf in lineage_files:
            records = self._read_jsonl(lf)
            for rec in records:
                # 检查是否与artifact有关联
                if any(
                    artifact_path in str(rec.get(k, ""))
                    for k in ["源文件", "解析节点", "分块节点", "抽取产物"]
                ):
                    upstream.append(rec)

        return {
            "artifact": artifact_path,
            "upstream_count": len(upstream),
            "upstream_lineages": upstream,
        }

    def trace_triple_source(self, triple_id: str, graph_file: str | None = None) -> dict[str, Any]:
        """抽取血缘反查：通过版本化三元组来源索引读取边的完整溯源明细。

        输出可核对的三层链路：图谱边 -> 关系集合（含建图期/当前校验和）->
        关系证据行 -> 原分块。证据不完整时返回可解释的待核结论，不猜测来源。
        """
        from app.services.triple_source import triple_source_service

        detail = triple_source_service.lineage(triple_id, graph_file=graph_file)
        chain = []
        if detail.get("found_in_index"):
            edge = detail["edge"]
            rel_set = detail["relation_set"]
            rel_ev = detail["relation_evidence"]
            chunks = detail["source_chunks"]
            chain = [
                {"level": "图谱边", "ref": triple_id,
                 "detail": f"{edge['head']} ->[{edge['relation']}]-> {edge['tail']}",
                 "artifact": edge["graph_file"]},
                {"level": "关系集合", "ref": rel_set["relation_set_id"],
                 "detail": (f"关系文件 {rel_set['relation_file']}；"
                            f"建图期校验和={rel_set['checksum_baseline']}，"
                            f"当前校验和={rel_set['checksum_current']}"),
                 "artifact": rel_set["relation_file"]},
                {"level": "关系证据", "ref": rel_ev["relation_id"] or "未核对到关系行",
                 "detail": rel_ev["evidence_sentence"] or "无证据句子（待核）",
                 "artifact": rel_set["relation_file"]},
                {"level": "原分块", "ref": ", ".join(chunks["chunk_ids"]) or "未定位到分块",
                 "detail": f"分块文件 {chunks['chunk_file'] or '不存在'}，共 {len(chunks['chunks'])} 个分块",
                 "artifact": chunks["chunk_file"]},
            ]
        detail["lineage_chain"] = chain
        detail["lineage_service"] = "extract_lineage"
        return detail

    def validate_lineage_integrity(self, parse_task_key: str) -> dict[str, Any]:
        """校验血缘链路的完整性，检查各级节点是否都存在有效产物文件。"""
        parsed_file = self._data_dir("parsed") / f"{parse_task_key}_parsed.jsonl"
        chunks_file = self._data_dir("chunks") / f"{parse_task_key}_parsed_chunks.jsonl"
        entities_file = self._data_dir("entities") / f"{parse_task_key}_parsed_entities.jsonl"
        relations_file = self._data_dir("relations") / f"{parse_task_key}_parsed_relations.jsonl"

        checks = {
            "parsed": parsed_file.exists(),
            "chunks": chunks_file.exists(),
            "entities": entities_file.exists(),
            "relations": relations_file.exists(),
        }

        all_ok = all(checks.values())
        return {
            "parse_task_key": parse_task_key,
            "checks": checks,
            "integrity": "完整" if all_ok else "存在断链",
            "missing_nodes": [k for k, v in checks.items() if not v],
        }
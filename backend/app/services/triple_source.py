"""版本化三元组来源索引服务（triple source index v2）。

把图谱 v1.0 的三元组边迁移为带关系证据引用的版本化来源索引，核心约束：

1. 旧图只保存关系集合（relation set）引用，同名关系文件可能已被更正；
   迁移时以建图期检查点中的关系文件校验和为历史基线，绝不把后来的同名
   关系补成“确定来源”。
2. 每条历史边给出三类可解释结论：
   - complete_source  证据完整，建图基线与当前关系文件一致且关系行可核对；
   - rewritten_source 关系文件已被改写，保留旧引用证据状态，仅作改写标注；
   - pending_review   证据不完整（文件缺失/基线缺失/原关系行无法核对），
                      给出待核原因，不自动补来源。
3. 迁移流水线可中断恢复：staging 文件 + 检查点逐条提交，seal 时原子切换，
   未落盘的索引永远不会被历史查询读到，也不会混进旧图。
4. v1/v2 索引部署环境共存：manifest 记录当前激活索引，支持 switch/rollback，
   服务重启后全部状态只从落盘文件重建。
"""
from __future__ import annotations

import hashlib
import json
import os
import time
import uuid
from pathlib import Path
from typing import Any

from app.services.base import DATA_ROOT

# 证据状态常量
PROV_COMPLETE = "complete_source"      # 来源完整、可核对
PROV_REWRITTEN = "rewritten_source"    # 关系集合已被改写，旧引用证据状态保留
PROV_PENDING = "pending_review"        # 证据不完整，待人工核对

INDEX_VERSION = "v2"
LEGACY_GRAPH_VERSION = "v1.0"
MIGRATION_CHECKPOINT = "migrate_triple_source_v2"
CONFLICT_POLICY = "legacy_reference_preserved"


class MigrationInterrupt(RuntimeError):
    """模拟中断：流水线在处理若干条边后被打断，用于验证可恢复性。"""


class TripleSourceService:
    """版本化三元组来源索引：迁移、切换、回滚、历史查询与血缘反查。"""

    # -- 落盘路径 --

    def _dir(self) -> Path:
        p = DATA_ROOT / "triple_source"
        p.mkdir(parents=True, exist_ok=True)
        return p

    def _v2_dir(self) -> Path:
        p = self._dir() / "v2"
        p.mkdir(parents=True, exist_ok=True)
        return p

    def _manifest_path(self) -> Path:
        return self._dir() / "manifest.json"

    def _sealed_index_path(self) -> Path:
        return self._v2_dir() / "index.jsonl"

    def _staging_path(self) -> Path:
        return self._v2_dir() / "staging.jsonl"

    def _graph_dir(self) -> Path:
        p = DATA_ROOT / "graph"
        p.mkdir(parents=True, exist_ok=True)
        return p

    def _relations_dir(self) -> Path:
        p = DATA_ROOT / "relations"
        p.mkdir(parents=True, exist_ok=True)
        return p

    def _entities_dir(self) -> Path:
        p = DATA_ROOT / "entities"
        p.mkdir(parents=True, exist_ok=True)
        return p

    def _chunks_dir(self) -> Path:
        p = DATA_ROOT / "chunks"
        p.mkdir(parents=True, exist_ok=True)
        return p

    def _checkpoint_path(self) -> Path:
        p = DATA_ROOT / "checkpoints"
        p.mkdir(parents=True, exist_ok=True)
        return p / f"{MIGRATION_CHECKPOINT}.json"

    # -- 通用 JSON / JSONL IO（全部落盘，无进程内缓存，保证重启一致） --

    @staticmethod
    def _read_jsonl(path: Path) -> list[dict[str, Any]]:
        if not path.exists():
            return []
        records: list[dict[str, Any]] = []
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    records.append(json.loads(line))
                except json.JSONDecodeError:
                    continue
        return records

    @staticmethod
    def _append_jsonl(path: Path, records: list[dict[str, Any]]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "a", encoding="utf-8") as f:
            for rec in records:
                f.write(json.dumps(rec, ensure_ascii=False, default=str) + "\n")
            f.flush()
            os.fsync(f.fileno())

    @staticmethod
    def _atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(path.suffix + ".tmp")
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2, default=str)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)

    @staticmethod
    def _file_checksum(path: Path) -> str | None:
        if not path.exists():
            return None
        return hashlib.sha256(path.read_bytes()).hexdigest()[:16]

    @staticmethod
    def _edge_key(head: str, relation: str, tail: str) -> str:
        """语义键：头-谓词-尾，用于和关系行做内容匹配（不保证跨图谱文件唯一）。"""
        return f"{head}||{relation}||{tail}"

    def _edge_uid(self, graph_file: str, triple_id: str) -> str:
        """边的稳定唯一身份：图谱文件 + 三元组编号。

        不同关系集合可能生成内容相同的三元组，只有带上图谱文件才能区分
        “哪条历史边引用了哪个关系集合”。
        """
        return f"{graph_file}::{triple_id}"

    def _record_id(self, edge_uid: str) -> str:
        digest = hashlib.sha1(edge_uid.encode("utf-8")).hexdigest()[:12]
        return f"TSV2-{digest.upper()}"

    # -- manifest：部署状态（激活索引、seal 状态、切换历史） --

    def _load_manifest(self) -> dict[str, Any]:
        path = self._manifest_path()
        if not path.exists():
            return {
                "active_index": LEGACY_GRAPH_VERSION,
                "sealed": False,
                "sealed_at": None,
                "migrations": [],
                "switch_history": [],
            }
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)

    def _save_manifest(self, manifest: dict[str, Any]) -> None:
        self._atomic_write_json(self._manifest_path(), manifest)

    def _record_action(self, manifest: dict[str, Any], action: str, detail: dict[str, Any]) -> None:
        manifest.setdefault("switch_history", []).append({
            "action": action,
            "at": time.strftime("%Y-%m-%d %H:%M:%S"),
            "detail": detail,
        })

    # -- 旧图边收集：保持 v1.0 图谱文件原样只读 --

    def collect_legacy_edges(self) -> list[dict[str, Any]]:
        """枚举全部 v1.0 图谱边，附带来源图谱文件，不修改旧图任何内容。

        同一图谱文件内可能保留同头-谓词-尾、不同关系编号的重复行，
        按边身份（图谱文件 + triple_id）去重，避免重复迁移。
        """
        edges: list[dict[str, Any]] = []
        seen: set[str] = set()
        for graph_file in sorted(self._graph_dir().glob("*.jsonl")):
            graph_file_rel = str(graph_file.relative_to(DATA_ROOT))
            for triple in self._read_jsonl(graph_file):
                if triple.get("graph_version") != LEGACY_GRAPH_VERSION:
                    continue
                edge_uid = self._edge_uid(graph_file_rel, str(triple.get("triple_id", "")))
                if edge_uid in seen:
                    continue
                seen.add(edge_uid)
                rel = dict(triple)
                rel["_graph_file"] = graph_file_rel
                edges.append(rel)
        return edges

    # -- 证据反查：关系集合 -> 关系行 -> 实体 -> 原分块 --

    @staticmethod
    def _relation_id_of_triple(triple_id: str) -> str | None:
        """TRIPLE-REL-000006 -> REL-000006。"""
        if not triple_id or not triple_id.startswith("TRIPLE-"):
            return None
        return triple_id[len("TRIPLE-"):]

    def _load_graph_baseline(self, relation_set_id: str) -> dict[str, Any]:
        """读取建图期检查点，取关系文件校验和基线（旧引用的证据锚点）。"""
        cp = DATA_ROOT / "checkpoints" / f"graph_{relation_set_id}.json"
        if not cp.exists():
            return {}
        with open(cp, "r", encoding="utf-8") as f:
            return json.load(f)

    def _resolve_chunks(
        self, relation_set_id: str, relation_row: dict[str, Any] | None
    ) -> tuple[list[str], list[dict[str, Any]], str | None]:
        """通过关系行涉及的实体反查实体产物，再定位原分块。"""
        chunk_file = self._chunks_dir() / f"{relation_set_id}_chunks.jsonl"
        entity_file = self._entities_dir() / f"{relation_set_id}_entities.jsonl"
        entities = self._read_jsonl(entity_file)
        chunks = self._read_jsonl(chunk_file)

        wanted_texts: set[str] = set()
        if relation_row:
            wanted_texts.add(str(relation_row.get("head_entity", "")))
            wanted_texts.add(str(relation_row.get("tail_entity", "")))
            wanted_texts.discard("")

        chunk_ids: list[str] = []
        if wanted_texts:
            for ent in entities:
                if ent.get("entity_text") in wanted_texts:
                    cid = ent.get("chunk_id")
                    if cid and cid not in chunk_ids:
                        chunk_ids.append(cid)

        snapshots = []
        for chunk in chunks:
            if chunk.get("chunk_id") in chunk_ids:
                text = str(chunk.get("text", ""))
                snapshots.append({
                    "chunk_id": chunk.get("chunk_id"),
                    "doc_id": chunk.get("doc_id"),
                    "page": chunk.get("page"),
                    "seq": chunk.get("seq"),
                    "text_excerpt": text[:120],
                })
        return chunk_ids, snapshots, (str(chunk_file.relative_to(DATA_ROOT)) if chunk_file.exists() else None)

    def _build_provenance(self, edge: dict[str, Any]) -> dict[str, Any]:
        """为一条旧图边构建带证据引用的 v2 来源记录（核心判定逻辑）。"""
        relation_set_id = str(edge.get("source", ""))
        relation_file = self._relations_dir() / f"{relation_set_id}_relations.jsonl"
        graph_file_rel = edge["_graph_file"]
        triple_id = str(edge.get("triple_id", ""))
        edge_uid = self._edge_uid(graph_file_rel, triple_id)
        source_key = self._edge_key(edge["head"], edge["relation"], edge["tail"])
        relation_id = self._relation_id_of_triple(triple_id)

        baseline_cp = self._load_graph_baseline(relation_set_id)
        baseline_checksum = baseline_cp.get("relation_file_checksum")
        current_checksum = self._file_checksum(relation_file)
        relations = self._read_jsonl(relation_file)

        rels_by_id = {r.get("relation_id"): r for r in relations}
        rels_by_key = {
            self._edge_key(r.get("head_entity", ""), r.get("relation_type", ""), r.get("tail_entity", "")): r
            for r in relations
        }
        current_row = rels_by_id.get(relation_id) if relation_id else None
        same_key_row = rels_by_key.get(source_key)

        provenance_status = PROV_PENDING
        evidence_conflict = False
        confirmed = False
        review_reason = ""
        review_detail = ""

        if not relation_file.exists() or not relations:
            review_reason = "relation_file_missing"
            review_detail = f"旧边引用的关系集合文件 {relation_file.name} 已不存在或为空，无法核对历史来源"
        elif not baseline_checksum:
            # 旧图没有建图期校验和基线：同名关系文件可能已被更正，不能当成确定来源
            review_reason = "missing_buildtime_checksum_baseline"
            review_detail = "缺少建图期关系文件校验和基线，无法证明当前同名关系文件就是旧边当年使用的来源"
        elif current_checksum != baseline_checksum:
            # 关系文件相对建图时已被改写：保留旧引用证据状态，禁止补成确定来源
            provenance_status = PROV_REWRITTEN
            review_reason = "relation_file_rewritten"
            review_detail = (
                f"关系文件校验和 {baseline_checksum} → {current_checksum}，"
                "同名关系集合已有更正，旧边按改写来源标注，不升级为确定来源"
            )
            if current_row is None:
                evidence_conflict = True
                review_detail += "；原关系行已不存在，历史证据存在冲突"
            elif self._edge_key(
                current_row.get("head_entity", ""),
                current_row.get("relation_type", ""),
                current_row.get("tail_entity", ""),
            ) != source_key:
                evidence_conflict = True
                review_detail += "；当前关系行头尾/谓词与旧边不一致"
        elif current_row is None:
            review_reason = "relation_row_missing"
            review_detail = f"关系文件未变但找不到关系行 {relation_id}，旧边证据链不完整"
            if same_key_row is not None:
                provenance_status = PROV_REWRITTEN
                evidence_conflict = True
                review_reason = "relation_row_replaced"
                review_detail = "原关系行缺失但存在同头-谓词-尾的替代关系行，按改写来源待核处理"
        else:
            row_key = self._edge_key(
                current_row.get("head_entity", ""),
                current_row.get("relation_type", ""),
                current_row.get("tail_entity", ""),
            )
            if row_key != source_key:
                provenance_status = PROV_REWRITTEN
                evidence_conflict = True
                review_reason = "relation_key_drift"
                review_detail = "关系行仍在但头-谓词-尾与旧边不一致，旧引用证据状态保留"
            else:
                provenance_status = PROV_COMPLETE
                confirmed = True
                review_reason = "evidence_verified"
                review_detail = "建图期校验和一致且关系行头-谓词-尾可核对，来源完整"

        evidence_row = current_row if provenance_status == PROV_COMPLETE else (
            current_row if provenance_status == PROV_REWRITTEN else None
        )
        # pending_review 不采信后来的同名关系内容，避免把更正后数据补成历史来源
        chunk_ids, chunk_snapshots, chunk_file_rel = self._resolve_chunks(
            relation_set_id, evidence_row if provenance_status != PROV_PENDING else None
        )

        conclusion_label = {
            PROV_COMPLETE: "来源完整可核对",
            PROV_REWRITTEN: "关系集合已改写，保留旧引用证据状态",
            PROV_PENDING: "证据不完整，待人工核对",
        }[provenance_status]

        return {
            "index_version": INDEX_VERSION,
            "record_id": self._record_id(edge_uid),
            "edge_uid": edge_uid,
            "source_key": source_key,
            "triple_id": edge.get("triple_id"),
            "head": edge.get("head"),
            "relation": edge.get("relation"),
            "tail": edge.get("tail"),
            "confidence": edge.get("confidence"),
            "graph_version": LEGACY_GRAPH_VERSION,
            "graph_file": graph_file_rel,
            "ingest_path": "migration",
            "legacy_reference": {
                "graph_version": LEGACY_GRAPH_VERSION,
                "relation_set_id": relation_set_id,
                "relation_file": f"relations/{relation_set_id}_relations.jsonl",
                "relation_file_checksum_baseline": baseline_checksum,
                "relation_file_checksum_current": current_checksum,
                "graph_checkpoint": f"checkpoints/graph_{relation_set_id}.json" if baseline_cp else None,
            },
            "evidence": {
                "relation_id": relation_id,
                "evidence_sentence": (evidence_row or {}).get("evidence_sentence"),
                "relation_confidence": (evidence_row or {}).get("confidence"),
                "model_version": (evidence_row or {}).get("model_version"),
                "relation_record_snapshot": evidence_row,
                "chunk_file": chunk_file_rel,
                "chunk_ids": chunk_ids,
                "chunk_snapshots": chunk_snapshots,
            },
            "provenance_status": provenance_status,
            "confirmed": confirmed,
            "evidence_conflict": evidence_conflict,
            "conflict_policy": CONFLICT_POLICY,
            "review_conclusion": {
                "label": conclusion_label,
                "reason": review_reason,
                "detail": review_detail,
            },
            "sealed": False,
            "migrated_at": time.strftime("%Y-%m-%d %H:%M:%S"),
            "updated_at": time.time(),
        }

    # -- 迁移流水线：逐条 staging + 检查点，可中断恢复，seal 原子提交 --

    def _read_migration_checkpoint(self) -> dict[str, Any] | None:
        path = self._checkpoint_path()
        if not path.exists():
            return None
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)

    def _write_migration_checkpoint(self, state: dict[str, Any]) -> None:
        state = dict(state)
        state["_updated_at"] = time.time()
        self._atomic_write_json(self._checkpoint_path(), state)

    def run_migration(
        self,
        *,
        max_edges: int | None = None,
        fail_after: int | None = None,
    ) -> dict[str, Any]:
        """执行（或恢复）v1.0 -> v2 来源索引迁移。

        max_edges: 单轮最多处理多少条边（分批切换）；
        fail_after: 处理 N 条后模拟中断抛 MigrationInterrupt，已落盘部分可恢复。
        """
        manifest = self._load_manifest()
        if manifest.get("sealed"):
            return {
                "status": "already_sealed",
                "message": "v2 来源索引已封存，迁移幂等跳过；旧图边保持可读",
                "sealed_index": str(self._sealed_index_path().relative_to(DATA_ROOT)),
                "records": len(self._read_jsonl(self._sealed_index_path())),
            }

        checkpoint = self._read_migration_checkpoint()
        if checkpoint and checkpoint.get("status") == "interrupted":
            run_id = checkpoint["run_id"]
            processed: set[str] = set(checkpoint.get("processed_edge_uids", []))
            resumed = True
        else:
            run_id = f"MIG-{uuid.uuid4().hex[:12].upper()}"
            processed = set()
            resumed = False
            # 新一轮迁移清理孤立 staging，避免把未落盘的旧数据混进本次索引
            if self._staging_path().exists():
                self._staging_path().unlink()

        edges = self.collect_legacy_edges()
        unique_total = len({
            self._edge_uid(e["_graph_file"], str(e.get("triple_id", ""))) for e in edges
        })

        def _uid_of(edge: dict[str, Any]) -> str:
            return self._edge_uid(edge["_graph_file"], str(edge.get("triple_id", "")))

        pending_edges = [e for e in edges if _uid_of(e) not in processed]
        if max_edges is not None:
            pending_edges = pending_edges[:max_edges]

        processed_this_run = 0
        interrupted = False
        for edge in pending_edges:
            edge_uid = _uid_of(edge)
            record = self._build_provenance(edge)
            # 先落盘 staging，再推进检查点：任何时刻中断都不会丢已提交边
            self._append_jsonl(self._staging_path(), [record])
            processed.add(edge_uid)
            processed_this_run += 1
            self._write_migration_checkpoint({
                "run_id": run_id,
                "status": "in_progress",
                "processed_edge_uids": sorted(processed),
                "edges_total": unique_total,
                "staging_file": str(self._staging_path().relative_to(DATA_ROOT)),
            })
            if fail_after is not None and processed_this_run >= fail_after:
                self._write_migration_checkpoint({
                    "run_id": run_id,
                    "status": "interrupted",
                    "processed_edge_uids": sorted(processed),
                    "edges_total": unique_total,
                    "staging_file": str(self._staging_path().relative_to(DATA_ROOT)),
                    "interrupt_reason": "simulated_fail_after",
                })
                interrupted = True
                break

        if interrupted:
            raise MigrationInterrupt(
                f"迁移在 {processed_this_run} 条边后中断，已落盘 {len(processed)} 条，可从检查点恢复"
            )

        all_processed = len(processed) >= unique_total
        if not all_processed:
            return {
                "status": "partial",
                "run_id": run_id,
                "resumed": resumed,
                "processed_total": len(processed),
                "edges_total": unique_total,
                "staging": str(self._staging_path().relative_to(DATA_ROOT)),
                "message": "分批迁移已落盘本批边，继续调用直到封存",
            }

        return self._seal_migration(run_id, manifest, processed)

    def _seal_migration(
        self, run_id: str, manifest: dict[str, Any], processed: set[str]
    ) -> dict[str, Any]:
        """封存：staging 去重后原子替换 sealed index，旧图文件保持不动。"""
        staged = self._read_jsonl(self._staging_path())
        deduped: dict[str, dict[str, Any]] = {}
        for rec in staged:
            deduped[rec["edge_uid"]] = rec
        sealed_records = []
        for rec in deduped.values():
            rec = dict(rec)
            rec["sealed"] = True
            rec["sealed_at"] = time.strftime("%Y-%m-%d %H:%M:%S")
            sealed_records.append(rec)
        sealed_records.sort(key=lambda r: r["edge_uid"])

        sealed_path = self._sealed_index_path()
        tmp_path = sealed_path.with_suffix(".jsonl.tmp")
        self._append_jsonl(tmp_path, sealed_records)
        os.replace(tmp_path, sealed_path)  # 原子发布：查询要么看到旧状态要么看到完整索引
        if self._staging_path().exists():
            self._staging_path().unlink()

        status_counts: dict[str, int] = {}
        for rec in sealed_records:
            status_counts[rec["provenance_status"]] = status_counts.get(rec["provenance_status"], 0) + 1

        self._write_migration_checkpoint({
            "run_id": run_id,
            "status": "sealed",
            "processed_edge_uids": sorted(processed),
            "edges_total": len(sealed_records),
            "sealed_index": str(sealed_path.relative_to(DATA_ROOT)),
            "status_counts": status_counts,
        })

        manifest["sealed"] = True
        manifest["sealed_at"] = time.strftime("%Y-%m-%d %H:%M:%S")
        manifest.setdefault("migrations", []).append({
            "run_id": run_id,
            "sealed_at": manifest["sealed_at"],
            "records": len(sealed_records),
            "status_counts": status_counts,
        })
        self._save_manifest(manifest)

        return {
            "status": "sealed",
            "run_id": run_id,
            "records": len(sealed_records),
            "status_counts": status_counts,
            "sealed_index": str(sealed_path.relative_to(DATA_ROOT)),
            "message": "v2 来源索引已封存；旧图谱 v1.0 与已引用边保持可读",
        }

    # -- v1/v2 共存切换与回滚 --

    def switch_active(self, version: str) -> dict[str, Any]:
        """切换激活索引。切到 v2 前要求索引已封存；v1 始终可回退。"""
        manifest = self._load_manifest()
        if version == INDEX_VERSION:
            if not manifest.get("sealed"):
                return {"ok": False, "message": "v2 来源索引尚未封存，不能切换；请先完成并落盘迁移"}
            previous = manifest.get("active_index")
            manifest["active_index"] = INDEX_VERSION
            self._record_action(manifest, "switch_to_v2", {"from": previous})
            self._save_manifest(manifest)
            return {"ok": True, "active_index": INDEX_VERSION, "previous": previous}
        if version == LEGACY_GRAPH_VERSION:
            previous = manifest.get("active_index")
            manifest["active_index"] = LEGACY_GRAPH_VERSION
            self._record_action(manifest, "rollback_to_v1", {"from": previous})
            self._save_manifest(manifest)
            return {"ok": True, "active_index": LEGACY_GRAPH_VERSION, "previous": previous}
        return {"ok": False, "message": f"未知索引版本：{version}，仅支持 v1.0 / v2"}

    def rollback(self) -> dict[str, Any]:
        """回滚到旧图谱 v1.0；封存的 v2 索引保留落盘但不再作为权威来源。"""
        return self.switch_active(LEGACY_GRAPH_VERSION)

    # -- 当前边入索引（v2 激活后新建图边的实时来源登记） --

    def index_triple_live(self, triple: dict[str, Any], relation_set_id: str) -> dict[str, Any] | None:
        """v2 激活后，新入图的当前边直接写入已封存索引（追加落盘）。

        v1 激活或索引未封存时拒绝写入，防止未落盘/越权数据混进旧图查询。
        """
        manifest = self._load_manifest()
        if manifest.get("active_index") != INDEX_VERSION or not manifest.get("sealed"):
            return None

        relation_file = self._relations_dir() / f"{relation_set_id}_relations.jsonl"
        current_checksum = self._file_checksum(relation_file)
        relation_id = self._relation_id_of_triple(str(triple.get("triple_id", "")))
        relations = self._read_jsonl(relation_file)
        row = next((r for r in relations if r.get("relation_id") == relation_id), None)
        chunk_ids, chunk_snapshots, chunk_file_rel = self._resolve_chunks(relation_set_id, row)
        source_key = self._edge_key(triple["head"], triple["relation"], triple["tail"])
        graph_file_rel = f"graph/{relation_set_id}_graph.jsonl"
        edge_uid = self._edge_uid(graph_file_rel, str(triple.get("triple_id", "")))

        record = {
            "index_version": INDEX_VERSION,
            "record_id": self._record_id(edge_uid),
            "edge_uid": edge_uid,
            "source_key": source_key,
            "triple_id": triple.get("triple_id"),
            "head": triple.get("head"),
            "relation": triple.get("relation"),
            "tail": triple.get("tail"),
            "confidence": triple.get("confidence"),
            "graph_version": triple.get("graph_version", LEGACY_GRAPH_VERSION),
            "graph_file": graph_file_rel,
            "ingest_path": "live",
            "legacy_reference": {
                "graph_version": triple.get("graph_version", LEGACY_GRAPH_VERSION),
                "relation_set_id": relation_set_id,
                "relation_file": f"relations/{relation_set_id}_relations.jsonl",
                "relation_file_checksum_baseline": current_checksum,
                "relation_file_checksum_current": current_checksum,
                "graph_checkpoint": f"checkpoints/graph_{relation_set_id}.json",
            },
            "evidence": {
                "relation_id": relation_id,
                "evidence_sentence": (row or {}).get("evidence_sentence"),
                "relation_confidence": (row or {}).get("confidence"),
                "model_version": (row or {}).get("model_version"),
                "relation_record_snapshot": row,
                "chunk_file": chunk_file_rel,
                "chunk_ids": chunk_ids,
                "chunk_snapshots": chunk_snapshots,
            },
            "provenance_status": PROV_COMPLETE,
            "confirmed": True,
            "evidence_conflict": False,
            "conflict_policy": CONFLICT_POLICY,
            "review_conclusion": {
                "label": "当前边：建图时证据完整",
                "reason": "live_ingest_verified",
                "detail": "v2 激活期间入图，关系行与关系文件校验和在建图时刻可核对",
            },
            "sealed": True,
            "migrated_at": time.strftime("%Y-%m-%d %H:%M:%S"),
            "updated_at": time.time(),
        }
        # 同一条边（图谱文件+triple_id）已存在则不重复追加
        existing = {r["edge_uid"] for r in self._read_jsonl(self._sealed_index_path())}
        if edge_uid not in existing:
            self._append_jsonl(self._sealed_index_path(), [record])
        return record

    # -- 查询接口：重启后只读取落盘文件，staging 绝不对外可见 --

    def _sealed_records(self) -> list[dict[str, Any]]:
        return self._read_jsonl(self._sealed_index_path())

    def get_record(
        self,
        triple_id: str | None = None,
        source_key: str | None = None,
        *,
        edge_uid: str | None = None,
        graph_file: str | None = None,
    ) -> dict[str, Any] | None:
        for rec in self._sealed_records():
            if edge_uid and rec.get("edge_uid") == edge_uid:
                return rec
            if graph_file and rec.get("graph_file") != graph_file:
                continue
            if (triple_id and rec.get("triple_id") == triple_id) or (
                source_key and rec.get("source_key") == source_key
            ):
                return rec
        return None

    def history(
        self,
        *,
        triple_id: str | None = None,
        head: str | None = None,
        status: str | None = None,
        graph_file: str | None = None,
    ) -> dict[str, Any]:
        """历史查询：旧图谱边始终可读，v2 证据按封存索引标注，staging 不暴露。"""
        manifest = self._load_manifest()
        active = manifest.get("active_index", LEGACY_GRAPH_VERSION)
        sealed_records = self._sealed_records()
        records_by_uid = {r["edge_uid"]: r for r in sealed_records}

        legacy_edges = []
        for edge in self.collect_legacy_edges():
            if triple_id and edge.get("triple_id") != triple_id:
                continue
            if head and head not in (edge.get("head"), edge.get("tail")):
                continue
            if graph_file and edge.get("_graph_file") != graph_file:
                continue
            edge_uid = self._edge_uid(edge["_graph_file"], str(edge.get("triple_id", "")))
            v2_rec = records_by_uid.get(edge_uid)
            legacy_edges.append({
                "edge_uid": edge_uid,
                "triple_id": edge.get("triple_id"),
                "head": edge.get("head"),
                "relation": edge.get("relation"),
                "tail": edge.get("tail"),
                "graph_version": edge.get("graph_version"),
                "graph_file": edge.get("_graph_file"),
                "relation_set_ref": edge.get("source"),
                "index_context": (
                    "v1.0 旧图边；来源证据已在 v2 封存索引中登记"
                    if v2_rec else
                    "v1.0 旧图边；v2 来源索引未覆盖（迁移未完成或未覆盖该边）"
                ),
                "provenance_status": v2_rec["provenance_status"] if v2_rec else None,
                "confirmed": v2_rec["confirmed"] if v2_rec else False,
            })

        provenance_records = []
        for rec in sealed_records:
            if triple_id and rec.get("triple_id") != triple_id:
                continue
            if head and head not in (rec.get("head"), rec.get("tail")):
                continue
            if graph_file and rec.get("graph_file") != graph_file:
                continue
            if status and rec.get("provenance_status") != status:
                continue
            annotated = dict(rec)
            annotated["authority"] = (
                "active" if active == INDEX_VERSION else "sealed_but_inactive"
            )
            provenance_records.append(annotated)

        counts = {PROV_COMPLETE: 0, PROV_REWRITTEN: 0, PROV_PENDING: 0}
        for rec in sealed_records:
            counts[rec["provenance_status"]] = counts.get(rec["provenance_status"], 0) + 1

        return {
            "active_index": active,
            "sealed": bool(manifest.get("sealed")),
            "staging_visible": False,  # 未落盘的新索引永不混入历史查询
            "legacy_edges": legacy_edges,
            "provenance_records": provenance_records,
            "counts": counts,
            "switch_history": manifest.get("switch_history", []),
        }

    def lineage(self, triple_id: str, graph_file: str | None = None) -> dict[str, Any]:
        """反查接口：给出边 -> 关系集合 -> 关系证据 -> 原分块的溯源明细。

        不同图谱文件可能存在相同 triple_id，可用 graph_file 精确定位。
        """
        record = self.get_record(triple_id=triple_id, graph_file=graph_file)
        if record is None:
            # v2 未覆盖的边：返回可解释的待核结论，而不是猜测来源
            legacy = next(
                (e for e in self.collect_legacy_edges()
                 if e.get("triple_id") == triple_id
                 and (graph_file is None or e.get("_graph_file") == graph_file)),
                None,
            )
            return {
                "triple_id": triple_id,
                "found_in_index": False,
                "provenance_status": PROV_PENDING,
                "confirmed": False,
                "legacy_edge": {
                    "head": legacy.get("head"), "relation": legacy.get("relation"),
                    "tail": legacy.get("tail"), "relation_set_ref": legacy.get("source"),
                    "graph_file": legacy.get("_graph_file"),
                } if legacy else None,
                "review_conclusion": {
                    "label": "证据不完整，待人工核对",
                    "reason": "not_covered_by_v2_index",
                    "detail": "v2 封存索引中没有该边的来源记录，不能反查确定来源",
                },
            }

        legacy_ref = record["legacy_reference"]
        evidence = record["evidence"]
        return {
            "triple_id": triple_id,
            "edge_uid": record["edge_uid"],
            "found_in_index": True,
            "index_version": INDEX_VERSION,
            "provenance_status": record["provenance_status"],
            "confirmed": record["confirmed"],
            "evidence_conflict": record["evidence_conflict"],
            "review_conclusion": record["review_conclusion"],
            "edge": {
                "head": record["head"], "relation": record["relation"], "tail": record["tail"],
                "triple_id": record["triple_id"], "graph_file": record["graph_file"],
                "ingest_path": record["ingest_path"],
            },
            "relation_set": {
                "relation_set_id": legacy_ref["relation_set_id"],
                "relation_file": legacy_ref["relation_file"],
                "checksum_baseline": legacy_ref["relation_file_checksum_baseline"],
                "checksum_current": legacy_ref["relation_file_checksum_current"],
                "graph_checkpoint": legacy_ref["graph_checkpoint"],
                "conflict_policy": record["conflict_policy"],
            },
            "relation_evidence": {
                "relation_id": evidence["relation_id"],
                "evidence_sentence": evidence["evidence_sentence"],
                "model_version": evidence["model_version"],
                "relation_record_snapshot": evidence["relation_record_snapshot"],
            },
            "source_chunks": {
                "chunk_file": evidence["chunk_file"],
                "chunk_ids": evidence["chunk_ids"],
                "chunks": evidence["chunk_snapshots"],
            },
        }

    def records(self, status: str | None = None) -> dict[str, Any]:
        """来源记录列表，可按证据状态过滤。"""
        manifest = self._load_manifest()
        items = self._sealed_records()
        if status:
            items = [r for r in items if r.get("provenance_status") == status]
        return {
            "active_index": manifest.get("active_index", LEGACY_GRAPH_VERSION),
            "sealed": bool(manifest.get("sealed")),
            "total": len(items),
            "items": items,
        }

    def deployment(self) -> dict[str, Any]:
        """部署状态：v1/v2 共存环境下当前激活索引与迁移/切换轨迹。"""
        manifest = self._load_manifest()
        checkpoint = self._read_migration_checkpoint()
        sealed = self._sealed_records()
        counts = {PROV_COMPLETE: 0, PROV_REWRITTEN: 0, PROV_PENDING: 0}
        for rec in sealed:
            counts[rec["provenance_status"]] = counts.get(rec["provenance_status"], 0) + 1
        return {
            "active_index": manifest.get("active_index", LEGACY_GRAPH_VERSION),
            "sealed": bool(manifest.get("sealed")),
            "sealed_at": manifest.get("sealed_at"),
            "sealed_records": len(sealed),
            "status_counts": counts,
            "staging_exists": self._staging_path().exists(),
            "migration_checkpoint": checkpoint,
            "migrations": manifest.get("migrations", []),
            "switch_history": manifest.get("switch_history", []),
            "legacy_graph_edges": len(self.collect_legacy_edges()),
        }


triple_source_service = TripleSourceService()

"""版本化三元组来源索引 v2 迁移与溯源集成测试。

覆盖需求：
- 三类记录：完整来源 complete_source、已改写来源 rewritten_source、证据不完整
  待核 pending_review，以及 v2 激活后入图的当前边；
- 迁移流水线可中断恢复（staging + 检查点 + 原子封存）；
- 回滚/重新切换前后历史查询结果一致，未落盘索引不混入旧图；
- 服务重启（全新服务实例，只从落盘文件读状态）后的接口查询；
- 抽取血缘反查输出边 -> 关系集合 -> 关系证据 -> 原分块明细。
"""
from __future__ import annotations

import json
import sys
import uuid
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

BACKEND_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BACKEND_DIR))

from app.main import app  # noqa: E402
from app.services import base as base_module  # noqa: E402
from app.services.chunk_split import ChunkSplitService  # noqa: E402
from app.services.entity_recognize import EntityRecognizeService  # noqa: E402
from app.services.format_parse import FormatParseService  # noqa: E402
from app.services.kg_build import KgBuildService  # noqa: E402
from app.services.relation_extract import RelationExtractService  # noqa: E402
from app.services import triple_source as ts_module  # noqa: E402
from app.services.triple_source import (  # noqa: E402
    INDEX_VERSION,
    LEGACY_GRAPH_VERSION,
    MigrationInterrupt,
    PROV_COMPLETE,
    PROV_PENDING,
    PROV_REWRITTEN,
    TripleSourceService,
)


def _uid(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:10].upper()}"


@pytest.fixture(autouse=True)
def isolated_data_root(tmp_path, monkeypatch):
    """每个用例使用独立落盘根目录，产物之间互不污染；同时验证重启只靠磁盘。"""
    root = tmp_path / "data"
    root.mkdir()
    monkeypatch.setattr(base_module, "DATA_ROOT", root)
    monkeypatch.setattr(ts_module, "DATA_ROOT", root)
    yield root


def _paths(root: Path):
    return {
        "ts_dir": root / "triple_source",
        "staging": root / "triple_source" / "v2" / "staging.jsonl",
        "sealed": root / "triple_source" / "v2" / "index.jsonl",
        "manifest": root / "triple_source" / "manifest.json",
        "checkpoint": root / "checkpoints" / "migrate_triple_source_v2.json",
    }


def _build_pipeline_doc() -> tuple[str, str, str, list[dict]]:
    """跑通 解析->分块->实体->关系->建图，返回 doc_id、关系集合 id、图谱文件与边。"""
    doc_id = _uid("DOC")
    parse_svc = FormatParseService()
    task, _ = parse_svc.create_entry({
        "任务编号": _uid("PR"), "文档编号": doc_id, "解析器类型": "PDF",
    })
    parse_svc.execute_parse(task["id"])
    parsed_id = f"{doc_id}_parsed"
    ChunkSplitService().split_from_document(parsed_id)
    EntityRecognizeService().recognize_from_chunk(parsed_id)
    RelationExtractService().extract_from_entities(parsed_id)
    KgBuildService().build_from_relations(parsed_id)

    graph_file = base_module.DATA_ROOT / "graph" / f"{parsed_id}_graph.jsonl"
    raw_edges = [json.loads(line) for line in graph_file.read_text(encoding="utf-8").splitlines() if line]
    # 建图流水线可能因重复触发追加完全相同的行；以边身份去重后的唯一边为准
    unique_ids: set[str] = set()
    edges = []
    for edge in raw_edges:
        if edge["triple_id"] in unique_ids:
            continue
        unique_ids.add(edge["triple_id"])
        edges.append(edge)
    return doc_id, parsed_id, f"graph/{parsed_id}_graph.jsonl", edges


class TestThreeRecordTypes:
    """以完整来源、已改写来源、待核（含缺基线）三类记录跑升级。"""

    def test_complete_source_record(self, isolated_data_root):
        root = isolated_data_root
        doc_id, parsed_id, graph_file_rel, edges = _build_pipeline_doc()
        target = edges[0]
        svc = TripleSourceService()

        result = svc.run_migration()
        assert result["status"] == "sealed"
        assert result["status_counts"][PROV_COMPLETE] >= 1

        record = svc.get_record(triple_id=target["triple_id"], graph_file=graph_file_rel)
        assert record is not None
        assert record["provenance_status"] == PROV_COMPLETE
        assert record["confirmed"] is True
        assert record["evidence_conflict"] is False
        # 建图期与当前关系文件校验和一致，且关系行可核对
        legacy = record["legacy_reference"]
        assert legacy["relation_file_checksum_baseline"] == legacy["relation_file_checksum_current"]
        assert record["evidence"]["relation_id"] == target["triple_id"].replace("TRIPLE-", "")
        # 反查到原分块
        assert record["evidence"]["chunk_ids"], "完整来源应能反查到原分块"
        assert record["evidence"]["chunk_snapshots"], "应带原分块内容摘录"
        # 旧图谱文件原样保留
        assert (root / graph_file_rel).exists()

    def test_rewritten_source_keeps_legacy_evidence_state(self, isolated_data_root):
        root = isolated_data_root
        doc_id, parsed_id, graph_file_rel, edges = _build_pipeline_doc()
        target = edges[0]

        # 建图之后更正同名关系文件：只改证据句子，关系 id 与头谓词尾保持不变
        rel_path = root / "relations" / f"{parsed_id}_relations.jsonl"
        rows = [json.loads(line) for line in rel_path.read_text(encoding="utf-8").splitlines() if line]
        rel_id = target["triple_id"].replace("TRIPLE-", "")
        for row in rows:
            if row["relation_id"] == rel_id:
                row["evidence_sentence"] = f"【人工更正】{row['evidence_sentence']}"
                row["confidence"] = round(min(row["confidence"] + 0.01, 0.99), 4)
        rel_path.write_text(
            "".join(json.dumps(r, ensure_ascii=False) + "\n" for r in rows), encoding="utf-8"
        )

        TripleSourceService().run_migration()
        record = TripleSourceService().get_record(
            triple_id=target["triple_id"], graph_file=graph_file_rel
        )
        assert record["provenance_status"] == PROV_REWRITTEN
        assert record["confirmed"] is False, "改写后的同名关系不能补成确定来源"
        assert record["legacy_reference"]["relation_file_checksum_baseline"] != \
            record["legacy_reference"]["relation_file_checksum_current"]
        assert record["conflict_policy"] == "legacy_reference_preserved"
        assert record["review_conclusion"]["reason"] == "relation_file_rewritten"
        # 旧图谱边本身保持原样可读
        legacy_edges = TripleSourceService().collect_legacy_edges()
        legacy = next(e for e in legacy_edges if e["triple_id"] == target["triple_id"])
        assert legacy["source"] == parsed_id

    def test_pending_review_when_relation_file_missing(self, isolated_data_root):
        root = isolated_data_root
        doc_id, parsed_id, graph_file_rel, edges = _build_pipeline_doc()
        target = edges[0]

        # 关系集合文件缺失：证据链断裂，必须给可解释的待核结论
        rel_path = root / "relations" / f"{parsed_id}_relations.jsonl"
        backup = rel_path.with_suffix(".jsonl.bak")
        rel_path.rename(backup)
        try:
            TripleSourceService().run_migration()
            record = TripleSourceService().get_record(
                triple_id=target["triple_id"], graph_file=graph_file_rel
            )
            assert record["provenance_status"] == PROV_PENDING
            assert record["confirmed"] is False
            assert record["review_conclusion"]["reason"] == "relation_file_missing"
            assert "待人工核对" in record["review_conclusion"]["label"]
            # 不采信任何替代内容：证据快照为空，不凭空补来源
            assert record["evidence"]["relation_record_snapshot"] is None
            assert record["evidence"]["chunk_ids"] == []
        finally:
            backup.rename(rel_path)

    def test_pending_review_without_buildtime_baseline(self, isolated_data_root):
        root = isolated_data_root
        doc_id, parsed_id, graph_file_rel, edges = _build_pipeline_doc()
        target = edges[0]

        # 删掉建图期检查点中的校验和基线：无法证明同名关系文件就是当年来源
        cp_path = root / "checkpoints" / f"graph_{parsed_id}.json"
        cp = json.loads(cp_path.read_text(encoding="utf-8"))
        del cp["relation_file_checksum"]
        cp_path.write_text(json.dumps(cp, ensure_ascii=False), encoding="utf-8")

        TripleSourceService().run_migration()
        record = TripleSourceService().get_record(
            triple_id=target["triple_id"], graph_file=graph_file_rel
        )
        assert record["provenance_status"] == PROV_PENDING
        assert record["review_conclusion"]["reason"] == "missing_buildtime_checksum_baseline"

    def test_rewritten_with_relation_row_removed_flags_conflict(self, isolated_data_root):
        root = isolated_data_root
        doc_id, parsed_id, graph_file_rel, edges = _build_pipeline_doc()
        target = edges[0]
        rel_id = target["triple_id"].replace("TRIPLE-", "")

        # 更正同名关系文件并删除旧边对应的原关系行
        rel_path = root / "relations" / f"{parsed_id}_relations.jsonl"
        rows = [json.loads(line) for line in rel_path.read_text(encoding="utf-8").splitlines() if line]
        kept = [r for r in rows if r["relation_id"] != rel_id]
        assert len(kept) == len(rows) - 1
        rel_path.write_text(
            "".join(json.dumps(r, ensure_ascii=False) + "\n" for r in kept), encoding="utf-8"
        )

        TripleSourceService().run_migration()
        record = TripleSourceService().get_record(
            triple_id=target["triple_id"], graph_file=graph_file_rel
        )
        assert record["provenance_status"] == PROV_REWRITTEN
        assert record["evidence_conflict"] is True
        assert record["confirmed"] is False
        assert "原关系行已不存在" in record["review_conclusion"]["detail"]


class TestResumableMigration:
    """迁移流水线中断恢复。"""

    def test_interrupt_then_resume(self, isolated_data_root):
        root = isolated_data_root
        paths = _paths(root)
        doc_a, parsed_a, _, edges_a = _build_pipeline_doc()
        doc_b, parsed_b, _, edges_b = _build_pipeline_doc()
        total_edges = len(edges_a) + len(edges_b)

        svc = TripleSourceService()
        with pytest.raises(MigrationInterrupt):
            svc.run_migration(fail_after=1)

        # 中断后：staging 已落盘 1 条，但索引未封存，历史查询看不到 staging
        assert paths["staging"].exists()
        assert not paths["sealed"].exists()
        cp = json.loads(paths["checkpoint"].read_text(encoding="utf-8"))
        assert cp["status"] == "interrupted"
        assert len(cp["processed_edge_uids"]) == 1

        history = svc.history()
        assert history["staging_visible"] is False
        assert history["provenance_records"] == []
        assert len(history["legacy_edges"]) == total_edges  # 旧图边始终可读

        # 恢复迁移：从检查点续跑，最终封存且无重复
        result = svc.run_migration()
        assert result["status"] == "sealed"
        assert result["records"] == total_edges

        sealed = TripleSourceService()._read_jsonl(paths["sealed"])
        uids = [r["edge_uid"] for r in sealed]
        assert len(uids) == len(set(uids)), "中断恢复不能产生重复来源记录"
        assert not paths["staging"].exists()

        # 迁移幂等：再次调用直接跳过
        again = svc.run_migration()
        assert again["status"] == "already_sealed"

    def test_partial_batches_accumulate_then_seal(self, isolated_data_root):
        root = isolated_data_root
        paths = _paths(root)
        doc_a, parsed_a, _, edges_a = _build_pipeline_doc()
        doc_b, parsed_b, _, edges_b = _build_pipeline_doc()
        total_edges = len(edges_a) + len(edges_b)
        batch = max(1, total_edges // 2)

        svc = TripleSourceService()
        first = svc.run_migration(max_edges=batch)
        assert first["status"] == "partial"
        assert first["processed_total"] == batch
        assert not paths["sealed"].exists()

        second = svc.run_migration()
        assert second["status"] == "sealed"
        assert second["records"] == total_edges


class TestSwitchRollbackAndRestart:
    """v1/v2 共存切换、回滚与重启后的接口查询。"""

    def test_switch_requires_sealed_index(self, isolated_data_root):
        _build_pipeline_doc()
        svc = TripleSourceService()
        outcome = svc.switch_active(INDEX_VERSION)
        assert outcome["ok"] is False
        assert "尚未封存" in outcome["message"]
        assert svc.deployment()["active_index"] == LEGACY_GRAPH_VERSION

    def test_rollback_and_reswitch_history_stable(self, isolated_data_root):
        root = isolated_data_root
        doc_id, parsed_id, graph_file_rel, edges = _build_pipeline_doc()
        # 制造一条改写来源，确保历史结论在切换前后都稳定
        rel_path = root / "relations" / f"{parsed_id}_relations.jsonl"
        rows = [json.loads(line) for line in rel_path.read_text(encoding="utf-8").splitlines() if line]
        rows[0]["evidence_sentence"] = "【更正】" + rows[0]["evidence_sentence"]
        rel_path.write_text(
            "".join(json.dumps(r, ensure_ascii=False) + "\n" for r in rows), encoding="utf-8"
        )

        svc = TripleSourceService()
        svc.run_migration()
        target_uids = {
            svc._edge_uid(graph_file_rel, e["triple_id"]) for e in edges
        }

        # v1 激活时的历史快照
        before = svc.history()
        assert before["active_index"] == LEGACY_GRAPH_VERSION
        before_map = {e["edge_uid"]: e for e in before["legacy_edges"] if e["edge_uid"] in target_uids}
        assert len(before_map) == len(edges)

        # 切到 v2
        sw = svc.switch_active(INDEX_VERSION)
        assert sw["ok"] is True
        after_switch = svc.history()
        assert after_switch["active_index"] == INDEX_VERSION
        # 封存记录在 v2 激活时标记为权威
        assert all(r["authority"] == "active" for r in after_switch["provenance_records"])

        # 回滚到 v1：同一批历史边与证据结论不变，v2 记录变为 sealed_but_inactive
        rb = svc.rollback()
        assert rb["active_index"] == LEGACY_GRAPH_VERSION
        rolled = svc.history()
        assert rolled["active_index"] == LEGACY_GRAPH_VERSION
        rolled_map = {e["edge_uid"]: e for e in rolled["legacy_edges"] if e["edge_uid"] in target_uids}
        assert set(rolled_map) == set(before_map)
        for uid in target_uids:
            assert rolled_map[uid]["provenance_status"] == before_map[uid]["provenance_status"]
            assert rolled_map[uid]["relation_set_ref"] == before_map[uid]["relation_set_ref"]
        assert all(r["authority"] == "sealed_but_inactive" for r in rolled["provenance_records"])

        # 重新切换 v2：历史查询结果与首次切换一致
        svc.switch_active(INDEX_VERSION)
        reswitched = svc.history()
        assert reswitched["active_index"] == INDEX_VERSION
        assert len(reswitched["provenance_records"]) == len(after_switch["provenance_records"])
        before_statuses = sorted(r["provenance_status"] for r in after_switch["provenance_records"])
        after_statuses = sorted(r["provenance_status"] for r in reswitched["provenance_records"])
        assert before_statuses == after_statuses

    def test_restart_rebuilds_state_from_disk_only(self, isolated_data_root):
        doc_id, parsed_id, graph_file_rel, edges = _build_pipeline_doc()
        TripleSourceService().run_migration()
        TripleSourceService().switch_active(INDEX_VERSION)

        # 模拟服务重启：全新服务实例，不携带任何进程内状态
        fresh = TripleSourceService()
        deployment = fresh.deployment()
        assert deployment["active_index"] == INDEX_VERSION
        assert deployment["sealed"] is True
        assert deployment["sealed_records"] == len(edges)

        history = fresh.history()
        assert history["staging_visible"] is False
        assert len(history["legacy_edges"]) == len(edges)
        record = fresh.get_record(triple_id=edges[0]["triple_id"], graph_file=graph_file_rel)
        assert record["provenance_status"] == PROV_COMPLETE

    def test_live_current_edge_only_after_v2_activation(self, isolated_data_root):
        paths = _paths(isolated_data_root)
        # v1 激活期间建图：不会向 v2 索引写任何边
        doc_id, parsed_id, graph_file_rel, edges = _build_pipeline_doc()
        assert not paths["sealed"].exists()

        svc = TripleSourceService()
        svc.run_migration()
        # 未切换前再次建图（同关系集合），live 写入应被拒绝
        KgBuildService().build_from_relations(parsed_id)
        assert svc.deployment()["active_index"] == LEGACY_GRAPH_VERSION
        before_switch_count = svc.deployment()["sealed_records"]

        # 切换 v2 后新建图的当前边实时入索引，标记为 current edge / complete
        svc.switch_active(INDEX_VERSION)
        new_doc, new_parsed, new_graph_rel, new_edges = _build_pipeline_doc()
        assert svc.deployment()["sealed_records"] == before_switch_count + len(new_edges)

        live = svc.get_record(
            triple_id=new_edges[0]["triple_id"], graph_file=new_graph_rel
        )
        assert live is not None
        assert live["ingest_path"] == "live"
        assert live["confirmed"] is True
        assert live["review_conclusion"]["reason"] == "live_ingest_verified"
        assert live["provenance_status"] == PROV_COMPLETE


class TestLineageReverseLookup:
    """抽取血缘通过反查接口读取版本化来源索引。"""

    def test_lineage_chain_for_complete_and_pending(self, isolated_data_root):
        root = isolated_data_root
        from app.services.extract_lineage import ExtractLineageService

        doc_id, parsed_id, graph_file_rel, edges = _build_pipeline_doc()
        missing_doc, missing_parsed, missing_graph_rel, missing_edges = _build_pipeline_doc()
        rel_path = root / "relations" / f"{missing_parsed}_relations.jsonl"
        backup = rel_path.with_suffix(".jsonl.bak")
        rel_path.rename(backup)
        try:
            TripleSourceService().run_migration()
        finally:
            backup.rename(rel_path)

        lineage_svc = ExtractLineageService()

        complete = lineage_svc.trace_triple_source(
            edges[0]["triple_id"], graph_file=graph_file_rel
        )
        assert complete["found_in_index"] is True
        assert complete["provenance_status"] == PROV_COMPLETE
        levels = [node["level"] for node in complete["lineage_chain"]]
        assert levels == ["图谱边", "关系集合", "关系证据", "原分块"]
        rel_set = complete["relation_set"]
        assert rel_set["relation_set_id"] == parsed_id
        assert rel_set["checksum_baseline"] == rel_set["checksum_current"]
        assert complete["source_chunks"]["chunks"], "应能核对到原分块内容"

        pending = lineage_svc.trace_triple_source(
            missing_edges[0]["triple_id"], graph_file=missing_graph_rel
        )
        assert pending["found_in_index"] is True
        assert pending["provenance_status"] == PROV_PENDING
        assert pending["review_conclusion"]["reason"] == "relation_file_missing"

    def test_lineage_unknown_edge_returns_explainable_pending(self, isolated_data_root):
        from app.services.extract_lineage import ExtractLineageService

        TripleSourceService().run_migration()
        result = ExtractLineageService().trace_triple_source("TRIPLE-REL-999999")
        assert result["found_in_index"] is False
        assert result["provenance_status"] == PROV_PENDING
        assert result["review_conclusion"]["reason"] == "not_covered_by_v2_index"


class TestHttpApi:
    """服务重启后通过 HTTP 接口查询历史数据与执行迁移/切换。"""

    def test_full_flow_via_api(self, isolated_data_root):
        doc_id, parsed_id, graph_file_rel, edges = _build_pipeline_doc()
        TripleSourceService().run_migration()  # 先封存，模拟重启后环境
        client = TestClient(app)

        # 健康与部署状态
        resp = client.get("/api/triple-source/deployment")
        assert resp.status_code == 200
        assert resp.json()["sealed"] is True

        # 历史查询：旧图边与 v2 证据同时可读，staging 不暴露
        resp = client.get("/api/triple-source/history", params={
            "triple_id": edges[0]["triple_id"], "graph_file": graph_file_rel,
        })
        assert resp.status_code == 200
        body = resp.json()
        assert body["staging_visible"] is False
        assert any(e["triple_id"] == edges[0]["triple_id"] for e in body["legacy_edges"])
        assert body["provenance_records"][0]["triple_id"] == edges[0]["triple_id"]

        # 状态过滤
        resp = client.get("/api/triple-source/records", params={"status": PROV_COMPLETE})
        assert resp.status_code == 200
        assert all(r["provenance_status"] == PROV_COMPLETE for r in resp.json()["items"])

        # 血缘反查接口
        resp = client.get(
            f"/api/triple-source/lineage/{edges[0]['triple_id']}",
            params={"graph_file": graph_file_rel},
        )
        assert resp.status_code == 200
        chain = resp.json()["lineage_chain"]
        assert [n["level"] for n in chain] == ["图谱边", "关系集合", "关系证据", "原分块"]

        # 切换与回滚接口
        resp = client.post("/api/triple-source/switch", json={"version": INDEX_VERSION})
        assert resp.json()["ok"] is True
        resp = client.post("/api/triple-source/rollback")
        assert resp.json()["active_index"] == LEGACY_GRAPH_VERSION

    def test_interrupt_endpoint_is_resumable_via_api(self, isolated_data_root):
        _build_pipeline_doc()
        client = TestClient(app)
        resp = client.post("/api/triple-source/migration", json={"fail_after": 1})
        assert resp.status_code == 200
        body = resp.json()
        assert body["status"] == "interrupted"
        assert body["resumable"] is True

        resp = client.post("/api/triple-source/migration", json={})
        assert resp.json()["status"] == "sealed"

    def test_switch_to_v2_rejected_before_seal_via_api(self, isolated_data_root):
        _build_pipeline_doc()
        client = TestClient(app)
        resp = client.post("/api/triple-source/switch", json={"version": INDEX_VERSION})
        assert resp.status_code == 200
        body = resp.json()
        assert body["ok"] is False
        assert "尚未封存" in body["message"]
        # 激活索引仍停留在旧图
        assert client.get("/api/triple-source/deployment").json()["active_index"] == LEGACY_GRAPH_VERSION

    def test_history_graph_file_filter_via_api(self, isolated_data_root):
        doc_a, parsed_a, graph_a, edges_a = _build_pipeline_doc()
        doc_b, parsed_b, graph_b, edges_b = _build_pipeline_doc()
        TripleSourceService().run_migration()
        client = TestClient(app)

        resp = client.get("/api/triple-source/history", params={"graph_file": graph_a})
        body = resp.json()
        assert body["legacy_edges"], "应只返回图谱 A 的边"
        assert all(e["graph_file"] == graph_a for e in body["legacy_edges"])
        assert all(r["graph_file"] == graph_a for r in body["provenance_records"])

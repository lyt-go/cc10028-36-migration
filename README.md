# 文档解析与知识抽取平台

面向多格式文档批量接入、版式解析、分块切分、实体关系抽取、向量化嵌入与抽取质量校验入库的非结构化知识抽取平台。

这是一个前后端分离的管理平台：前端 Vue 3 + Vite + TypeScript，后端 FastAPI（Python）。
两边各自独立启动，前端 dev server 已关掉自动打开页面，启动后按终端打印的地址手工打开。

## 目录结构

```text
.
├── frontend/                 Vue 3 + Vite + TypeScript 前端
│   ├── src/views/WorkbenchView.vue|tsx  全部模块共享工作台
│   ├── src/data/modules.ts              18个业务模块配置
│   ├── src/api/              统一请求封装
│   └── vite.config.ts        dev server 配置（open: false）
├── backend/                  FastAPI（Python） 后端
│   ├── app/routers/resource.py  通用查询/导出/登记/动作接口
│   ├── app/services/<key>.py    每个模块独立领域逻辑
│   ├── app/services/base.py     共享服务骨架
│   └── app/store.py          内存数据仓库与示例数据
├── .gitignore
└── docker-compose.yml
```

## 启动

### 后端

```bash
cd backend
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
./run.sh
```

健康检查：`curl http://127.0.0.1:8000/api/health`

### 前端

```bash
cd frontend
npm ci
npm run dev
```

前端默认监听 `http://127.0.0.1:5173/`，dev server 不会自动打开浏览器，
需要自己访问。`/api` 由 vite 代理到后端 `http://127.0.0.1:8000`。

## 业务模块

| 模块 | 目录 | 业务对象 | 主要字段 |
| --- | --- | --- | --- |
| 文件接入 | `file_intake` | 接入批次 | 批次编号、来源系统、文件清单 |
| 格式解析 | `format_parse` | 解析任务 | 任务编号、文档编号、解析器类型 |
| 版式还原 | `layout_restore` | 版式结构 | 文档编号、段落结构、标题层级 |
| 表格抽取 | `table_extract` | 表格区域 | 表格编号、文档编号、页码位置 |
| OCR识别 | `ocr_engine` | 识别任务 | 任务编号、图片区域、识别语言 |
| 分块切分 | `chunk_split` | 文本分块 | 分块编号、文档编号、分块长度 |
| 实体识别 | `entity_recognize` | 命名实体 | 实体编号、分块编号、实体类型 |
| 关系抽取 | `relation_extract` | 实体关系 | 关系编号、头实体、尾实体 |
| 术语归一 | `term_normalize` | 术语映射 | 映射编号、源术语、标准术语 |
| 敏感词过滤 | `sensitive_filter` | 过滤规则 | 规则编号、匹配模式、命中词条 |
| 向量嵌入 | `vector_embed` | 向量化任务 | 任务编号、分块编号、嵌入模型 |
| 知识图谱 | `kg_build` | 图谱三元组 | 三元组编号、头实体、谓词关系 |
| 抽取质量 | `extract_quality` | 质检报告 | 报告编号、批次编号、抽样比例 |
| 人工复核 | `annot_review` | 复核任务 | 复核编号、分块编号、复核对象 |
| 文档版本 | `doc_version` | 版本快照 | 快照编号、文档编号、版本号 |
| 抽取血缘 | `extract_lineage` | 血缘链路 | 链路编号、源文件、解析节点 |
| 结果交付 | `result_deliver` | 交付任务 | 交付编号、快照编号、交付格式 |
| 失败重采 | `parse_retry` | 重采任务 | 重采编号、失败环节、原始批次 |

## 约定

- 全部模块共享`frontend/src/views/WorkbenchView.vue|tsx`和一套资源路由。
- 每个模块仍保留独立`backend/app/services/<模块>.py`；纯前端保留
  `frontend/src/domain/<模块>.ts`，用于承载真实业务差异并维持36题位容量。
- 列表接口统一返回 `{ items, total, page, size }`，动作接口统一返回 `{ ok, message }`。
- 状态流转只允许在 `app/services` 里改，路由层不做业务判断。

## 版本化三元组来源索引 v2

把图谱 v1.0 的三元组边迁移为带关系证据引用的来源索引，解决“旧图只存关系集合
引用、同名关系文件可能已被更正”的历史溯源问题。实现见
`backend/app/services/triple_source.py`，接口前缀 `/api/triple-source`。

### 三类证据状态

| 状态 | 含义 |
| --- | --- |
| `complete_source` | 建图期关系文件校验和与当前一致、关系行头-谓词-尾可核对，来源完整 |
| `rewritten_source` | 同名关系集合已有更正（校验和变化/行漂移/行被替换），保留旧引用证据状态，不补成确定来源 |
| `pending_review` | 关系文件缺失、缺建图期校验和基线或关系行无法核对，给出可解释的待核结论 |

### 关键约束

- 边身份为 `图谱文件 + triple_id`（`edge_uid`），不同关系集合的同内容三元组不会互相覆盖。
- 建图期把关系文件校验和写入 `checkpoints/graph_<关系集合>.json` 作为证据基线，
  迁移只比对基线，绝不把后来的同名关系补成确定来源。
- 迁移流水线逐条写 `staging.jsonl` 并推进检查点，可中断恢复；封存时原子替换
  `v2/index.jsonl`。未落盘的 staging 对历史查询不可见，也不会混进旧图。
- v1/v2 通过 `manifest.json` 共存切换，支持 `switch`/`rollback`；服务重启后
  全部状态只从落盘文件重建。
- v2 激活后新入图的“当前边”经 `KgBuildService` 实时登记（`ingest_path=live`），
  v1 激活或未封存时拒绝写入。

### 接口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/api/triple-source/migration` | 运行/恢复迁移，支持 `max_edges` 分批、`fail_after` 模拟中断 |
| POST | `/api/triple-source/switch` | 切换激活索引（v1.0 / v2，未封存拒绝切 v2） |
| POST | `/api/triple-source/rollback` | 回滚到旧图谱 v1.0 |
| GET | `/api/triple-source/deployment` | 部署状态、迁移与切换轨迹 |
| GET | `/api/triple-source/history` | 历史查询：旧图边 + v2 证据，支持 `triple_id/head/status/graph_file` |
| GET | `/api/triple-source/records` | 来源记录列表，可按证据状态过滤 |
| GET | `/api/triple-source/lineage/{triple_id}` | 血缘反查：边→关系集合→关系证据→原分块 |

测试见 `backend/tests/test_triple_source_migration.py`（三类记录、中断恢复、
回滚/重切换对比、重启隔离、HTTP 接口与血缘反查）。

领域：数据工程
子方向：数据采集、爬虫与RPA

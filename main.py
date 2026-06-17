"""
AstrBot Obsidian 知识库同步插件 v2

通过 Fast Note Sync Service 获取 Obsidian 笔记，自动同步到 AstrBot 知识库。
支持增量同步、并发获取、智能跳过、重试机制、手动同步进度反馈等功能。
"""

import asyncio
import hashlib
import json
import re
import time
from fnmatch import fnmatch
from pathlib import Path
from typing import Optional

import httpx

from astrbot.api import AstrBotConfig, logger
from astrbot.api.event import AstrMessageEvent, filter
from astrbot.api.star import Context, Star
from astrbot.core.utils.astrbot_path import get_astrbot_data_path
from astrbot.core.knowledge_base.chunking.markdown import MarkdownChunker


class ObsidianKBSyncPlugin(Star):
    """Obsidian 知识库同步插件（Fast Note Sync 客户端）"""

    def __init__(self, context: Context, config: AstrBotConfig):
        super().__init__(context)
        self.config = config
        self._sync_task: Optional[asyncio.Task] = None
        self._sync_state: dict = {}
        self._last_sync_time: float = 0
        self._sync_count: int = 0
        self._is_syncing: bool = False
        self._kb_helper = None  # 缓存 KB helper

        # Fast Note Sync 配置
        self.fns_url: str = config.get("fns_url", "").rstrip("/")
        self.fns_token: str = config.get("fns_token", "")
        self.fns_vault: str = config.get("fns_vault", "")

        # 知识库配置
        self.kb_id: str = config.get("kb_id", "")
        self.kb_name: str = config.get("kb_name", "Obsidian Vault")

        # 同步配置
        self.auto_sync: bool = config.get("auto_sync", True)
        self.sync_interval: int = config.get("sync_interval", 300)
        self.exclude_patterns: list = config.get(
            "exclude_patterns", [".obsidian", ".trash", "*.tmp", ".git"]
        )
        self.restore_deleted: bool = config.get("restore_deleted", True)

        # 新增：性能配置
        self.max_file_size: int = config.get("max_file_size", 100)  # KB，超过此大小跳过
        self.concurrent_fetches: int = config.get("concurrent_fetches", 5)  # 并发获取笔记数
        self.retry_count: int = config.get("retry_count", 3)  # 重试次数
        self.verify_interval: int = config.get("verify_interval", 10)  # 每 N 次同步全量校验
        self.chunk_size: int = config.get("chunk_size", 512)  # 分块大小
        self.chunk_overlap: int = config.get("chunk_overlap", 50)  # 分块重叠

        # Markdown 分块器（与 AstrBot 知识库使用相同逻辑）
        self._chunker = MarkdownChunker(
            chunk_size=self.chunk_size,
            chunk_overlap=self.chunk_overlap,
        )

        # 数据目录
        self._data_dir = Path(get_astrbot_data_path()) / "plugin_data" / "astrbot_plugin_obsidian_kb_sync"
        self._data_dir.mkdir(parents=True, exist_ok=True)
        self._state_file = self._data_dir / "sync_state.json"

        self._load_state()
        logger.info(
            f"Obsidian KB Sync v2 初始化完成 | FNS: {self.fns_url} | "
            f"Vault: {self.fns_vault} | 并发: {self.concurrent_fetches} | "
            f"最大文件: {self.max_file_size}KB"
        )

    # ── 状态管理 ──────────────────────────────────────────────

    def _load_state(self):
        try:
            if self._state_file.exists():
                with open(self._state_file, "r", encoding="utf-8") as f:
                    self._sync_state = json.load(f)
                logger.info(f"已加载 {len(self._sync_state)} 条同步记录")
        except Exception as e:
            logger.error(f"加载同步状态失败: {e}")
            self._sync_state = {}

    async def _save_state(self):
        """异步保存状态，避免阻塞事件循环"""
        try:
            content = json.dumps(self._sync_state, ensure_ascii=False, indent=2)
            # 使用 run_in_executor 避免阻塞
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(None, self._write_state_sync, content)
        except Exception as e:
            logger.error(f"保存同步状态失败: {e}")

    def _write_state_sync(self, content: str):
        with open(self._state_file, "w", encoding="utf-8") as f:
            f.write(content)

    # ── 重试机制 ──────────────────────────────────────────────

    async def _retry_request(self, client: httpx.AsyncClient, method: str, url: str,
                             max_retries: int = None, **kwargs) -> Optional[httpx.Response]:
        """带重试的 HTTP 请求"""
        retries = max_retries or self.retry_count
        for attempt in range(retries):
            try:
                resp = await getattr(client, method)(url, **kwargs)
                if resp.status_code == 401:
                    logger.error("FNS Token 无效或已过期")
                    return None
                if resp.status_code == 200:
                    return resp
                if attempt < retries - 1:
                    wait = 2 ** attempt
                    logger.warning(f"请求 {url} 返回 {resp.status_code}，{wait}s 后重试 ({attempt+1}/{retries})")
                    await asyncio.sleep(wait)
                    continue
                logger.warning(f"请求 {url} 最终返回 {resp.status_code}")
                return resp
            except (httpx.ConnectError, httpx.ReadTimeout, httpx.WriteTimeout) as e:
                if attempt < retries - 1:
                    wait = 2 ** attempt
                    logger.warning(f"请求 {url} 失败: {e}，{wait}s 后重试 ({attempt+1}/{retries})")
                    await asyncio.sleep(wait)
                else:
                    logger.error(f"请求 {url} 最终失败: {e}")
                    return None
            except Exception as e:
                logger.error(f"请求 {url} 异常: {type(e).__name__}: {e}")
                return None
        return None

    # ── Fast Note Sync 客户端 ─────────────────────────────────

    def _fns_headers(self) -> dict:
        return {"token": self.fns_token}

    async def _fns_list_notes(self, client: httpx.AsyncClient) -> list[dict]:
        """列出 vault 中所有笔记（不含内容）"""
        notes = []
        page = 1
        while True:
            resp = await self._retry_request(
                client, "get",
                f"{self.fns_url}/api/notes",
                params={"vault": self.fns_vault, "page": page, "pageSize": 100},
                headers=self._fns_headers(),
                timeout=30.0,
            )
            if resp is None:
                break
            data = resp.json()
            items = data.get("data", {}).get("list", [])
            total = data.get("data", {}).get("pager", {}).get("totalRows", 0)
            if not items:
                break
            notes.extend(items)
            if len(notes) >= total:
                break
            page += 1
        return notes

    async def _fns_get_note(self, client: httpx.AsyncClient, path: str) -> Optional[str]:
        """获取单条笔记内容"""
        resp = await self._retry_request(
            client, "get",
            f"{self.fns_url}/api/note",
            params={"vault": self.fns_vault, "path": path},
            headers=self._fns_headers(),
            timeout=30.0,
        )
        if resp is not None:
            return resp.json().get("data", {}).get("content", "")
        return None

    async def _fns_get_notes_concurrent(self, client: httpx.AsyncClient,
                                        paths: list[str]) -> dict[str, Optional[str]]:
        """并发获取多条笔记内容"""
        sem = asyncio.Semaphore(self.concurrent_fetches)
        results = {}

        async def fetch_one(path: str):
            async with sem:
                content = await self._fns_get_note(client, path)
                results[path] = content

        tasks = [fetch_one(p) for p in paths]
        await asyncio.gather(*tasks, return_exceptions=True)
        return results

    # ── 内容清洗 ──────────────────────────────────────────────

    def _strip_frontmatter(self, content: str) -> str:
        return re.sub(r"^---\s*\n.*?\n---\s*\n", "", content, flags=re.DOTALL).strip()

    def _clean_obsidian_syntax(self, content: str) -> str:
        content = re.sub(r"!\[\[([^\]]+)\]\]", r"[\1]", content)
        content = re.sub(r"\[\[([^\]|]+)\|([^\]]+)\]\]", r"\2", content)
        content = re.sub(r"\[\[([^\]]+)\]\]", r"\1", content)
        content = re.sub(r"^>\s*\[![^\]]*\]\s*", "", content, flags=re.MULTILINE)
        content = re.sub(r"==([^=]+)==", r"\1", content)
        content = re.sub(r"%%.*?%%", "", content, flags=re.DOTALL)
        return content

    def _should_exclude(self, note_path: str) -> bool:
        for pattern in self.exclude_patterns:
            if fnmatch(note_path, pattern) or fnmatch(note_path.split("/")[-1], pattern):
                return True
            for part in note_path.split("/"):
                if fnmatch(part, pattern):
                    return True
        return False

    # ── AstrBot 知识库内部 API ────────────────────────────────

    async def _get_kb_helper(self, force_refresh: bool = False):
        """获取 KBHelper 实例（带缓存）"""
        if self._kb_helper and not force_refresh:
            return self._kb_helper

        kb_mgr = self.context.kb_manager
        if self.kb_id:
            helper = await kb_mgr.get_kb(self.kb_id)
            if helper:
                self._kb_helper = helper
                return helper
        # 按名称查找
        helper = await kb_mgr.get_kb_by_name(self.kb_name)
        if helper:
            self.kb_id = helper.kb.kb_id
            self._kb_helper = helper
            return helper
        return None

    async def _ensure_knowledge_base(self):
        """确保知识库存在，返回 KBHelper"""
        helper = await self._get_kb_helper()
        if helper:
            return helper

        # 无 kb_id 时尝试创建
        kb_mgr = self.context.kb_manager
        try:
            provider_mgr = self.context.provider_manager
            embedding_id = ""
            for p in provider_mgr.provider_insts:
                if "embedding" in p.meta().type.lower():
                    embedding_id = p.id()
                    break
            if not embedding_id:
                logger.error("未找到 Embedding 模型提供商，请先在 AstrBot 中配置")
                return None

            helper = await kb_mgr.create_kb(
                kb_name=self.kb_name,
                description="Obsidian 笔记库自动同步",
                embedding_provider_id=embedding_id,
            )
            self.kb_id = helper.kb.kb_id
            self._kb_helper = helper
            logger.info(f"已创建知识库: {self.kb_name} (ID: {self.kb_id})")
            return helper
        except Exception as e:
            logger.error(f"创建知识库失败: {e}")
            return None

    async def _upload_document(self, note_path: str, content: str) -> Optional[str]:
        """上传文档到知识库，返回 doc_id"""
        helper = await self._get_kb_helper()
        if not helper:
            return None
        try:
            file_name = note_path.split("/")[-1]
            if not file_name.endswith(".md"):
                file_name += ".md"
            doc = await helper.upload_document(
                file_name=file_name,
                file_content=content.encode("utf-8"),
                file_type="md",
            )
            # 即时验证：确认文档确实存在于知识库
            try:
                verified = await helper.get_document(doc.doc_id)
                if verified:
                    return doc.doc_id
                else:
                    logger.error(f"上传成功但验证失败（文档不存在）: {note_path}")
                    return None
            except Exception as ve:
                logger.warning(f"上传验证异常（仍记录）: {note_path}: {ve}")
                return doc.doc_id
        except Exception as e:
            logger.error(f"上传文档失败 {note_path}: {e}")
            return None

    async def _delete_document(self, doc_id: str) -> bool:
        helper = await self._get_kb_helper()
        if not helper:
            return False
        try:
            await helper.delete_document(doc_id)
            return True
        except Exception as e:
            logger.warning(f"删除文档失败 {doc_id}: {e}")
            return False

    # ── 增量同步 ─────────────────────────────────────────────

    async def _upload_incremental(self, note_path: str, content: str,
                                  old_doc_id: str) -> tuple[Optional[str], list[str]]:
        """增量上传：只重新 embedding 变化的 chunk，保留未变 chunk。
        返回 (doc_id, new_chunk_hashes)"""
        helper = await self._get_kb_helper()
        if not helper:
            return None, []

        try:
            # 1. 用与 AstrBot 相同的 MarkdownChunker 分块
            new_chunks = await self._chunker.chunk(content)
            if not new_chunks:
                return None, []

            # 2. 计算每块内容 hash
            new_chunk_hashes = [hashlib.md5(c.encode("utf-8")).hexdigest() for c in new_chunks]

            # 3. 获取旧文档的所有 chunk
            old_chunks_data = await helper.get_chunks_by_doc_id(old_doc_id, limit=9999)

            # 旧 chunk 的 hash → chunk_id 映射
            old_hash_to_id = {}
            for chunk in old_chunks_data:
                chunk_hash = hashlib.md5(chunk["content"].encode("utf-8")).hexdigest()
                old_hash_to_id[chunk_hash] = chunk["chunk_id"]

            # 4. 对比差异
            # 找出需要重新 embedding 的 chunk（新增或内容变化）
            changed_indices = []
            for i, h in enumerate(new_chunk_hashes):
                if h not in old_hash_to_id:
                    changed_indices.append(i)

            # 找出需要删除的旧 chunk（不再存在的内容）
            new_hash_set = set(new_chunk_hashes)
            chunks_to_delete = [
                cid for h, cid in old_hash_to_id.items()
                if h not in new_hash_set
            ]

            unchanged = len(new_chunks) - len(changed_indices)
            logger.info(
                f"增量分块: {len(new_chunks)} 块, "
                f"{unchanged} 未变, {len(changed_indices)} 需重嵌, "
                f"{len(chunks_to_delete)} 待删除"
            )

            # 无需变更时直接返回
            if not changed_indices and not chunks_to_delete:
                return old_doc_id, new_chunk_hashes

            vec_db = helper.vec_db

            # 5. 删除不再存在的旧 chunk
            for chunk_id in chunks_to_delete:
                try:
                    await vec_db.delete(chunk_id)
                except Exception:
                    pass

            # 6. 重新 embedding 变化的 chunk 并插入 vec_db
            for i in changed_indices:
                await vec_db.insert(
                    content=new_chunks[i],
                    metadata={
                        "kb_id": self.kb_id,
                        "kb_doc_id": old_doc_id,
                        "chunk_index": i,
                    },
                )

            # 7. 更新文档元数据（chunk_count）
            try:
                doc = await helper.get_document(old_doc_id)
                if doc:
                    doc.chunk_count = len(new_chunks)
                    async with helper.kb_db.get_db() as session:
                        async with session.begin():
                            session.add(doc)
                            await session.commit()
            except Exception as e:
                logger.warning(f"更新文档元数据失败: {e}")

            # 8. 刷新知识库统计
            await helper.kb_db.update_kb_stats(kb_id=self.kb_id, vec_db=vec_db)
            await helper.refresh_kb()

            return old_doc_id, new_chunk_hashes

        except Exception as e:
            logger.error(f"增量上传失败 {note_path}: {e}", exc_info=True)
            return None, []

    async def _batch_check_docs_exist(self, doc_ids: list[str]) -> dict[str, bool]:
        """批量检查文档是否存在（并发）"""
        if not doc_ids:
            return {}
        helper = await self._get_kb_helper()
        if not helper:
            return {did: False for did in doc_ids}

        results = {}
        sem = asyncio.Semaphore(10)

        async def check_one(doc_id: str):
            async with sem:
                try:
                    doc = await helper.get_document(doc_id)
                    results[doc_id] = doc is not None
                except Exception:
                    results[doc_id] = False

        tasks = [check_one(did) for did in doc_ids]
        await asyncio.gather(*tasks, return_exceptions=True)
        return results

    # ── 核心同步逻辑 ──────────────────────────────────────────

    async def _verify_all_doc_ids(self) -> int:
        """定期全量校验：检查 sync_state 中所有 doc_id 是否仍存在于知识库。
        返回被清除的条目数（这些条目将在下次同步时重新上传）。"""
        helper = await self._get_kb_helper()
        if not helper:
            return 0

        tracked = {
            p: v["doc_id"]
            for p, v in self._sync_state.items()
            if v.get("doc_id")
        }
        if not tracked:
            return 0

        logger.info(f"全量校验: 检查 {len(tracked)} 个 doc_id...")
        existence = await self._batch_check_docs_exist(list(tracked.values()))

        removed = 0
        for path, doc_id in tracked.items():
            if not existence.get(doc_id, True):
                logger.warning(f"校验发现文档已丢失，将重新上传: {path}")
                del self._sync_state[path]
                removed += 1

        if removed:
            logger.warning(f"全量校验完成: {removed} 个文档已从知识库丢失，将在下次同步时重新上传")
        else:
            logger.info(f"全量校验完成: 全部 {len(tracked)} 个文档正常")
        return removed

    async def _do_sync(self, progress_callback=None) -> dict:
        if not self.fns_url or not self.fns_vault:
            return {"error": "未配置 Fast Note Sync 地址或 Vault 名称"}
        if not self.fns_token:
            return {"error": "未配置 FNS Token"}
        if self._is_syncing:
            return {"error": "正在同步中，请稍候"}

        self._is_syncing = True
        result = {
            "total": 0, "new": 0, "updated": 0, "deleted": 0,
            "unchanged": 0, "skipped_size": 0, "errors": 0,
            "verify_removed": 0,
            "start_time": time.time(),
        }

        try:
            kb_helper = await self._ensure_knowledge_base()
            if not kb_helper:
                return {"error": "无法创建或找到 AstrBot 知识库"}

            async with httpx.AsyncClient() as client:
                # 1. 获取笔记列表
                notes = await self._fns_list_notes(client)
                result["total"] = len(notes)
                logger.info(f"FNS 返回 {len(notes)} 条笔记")

                if not notes:
                    await self._save_state()
                    result["duration"] = time.time() - result["start_time"]
                    return result

                # 2. 过滤排除项
                current_notes = {}
                for note in notes:
                    path = note.get("path", "")
                    if path and not self._should_exclude(path):
                        current_notes[path] = note

                tracked_paths = set(self._sync_state.keys())
                current_paths = set(current_notes.keys())

                # 3. 批量删除已移除的笔记
                deleted_paths = tracked_paths - current_paths
                if deleted_paths:
                    doc_ids_to_delete = [
                        (p, self._sync_state[p].get("doc_id"))
                        for p in deleted_paths if self._sync_state[p].get("doc_id")
                    ]
                    for path, doc_id in doc_ids_to_delete:
                        if await self._delete_document(doc_id):
                            result["deleted"] += 1
                        del self._sync_state[path]
                    for p in deleted_paths - {dp for dp, _ in doc_ids_to_delete}:
                        del self._sync_state[p]

                # 4. 分类处理：需要更新 vs 可跳过
                need_content_paths = []  # 需要获取内容的路径
                unchanged_paths = []     # 未变更的路径

                for path, note_info in current_notes.items():
                    remote_hash = note_info.get("contentHash", "")
                    tracked = self._sync_state.get(path)

                    # 快速跳过：远端 hash 未变
                    if tracked and remote_hash and tracked.get("remote_hash") == remote_hash:
                        unchanged_paths.append(path)
                        continue

                    # 无 remote_hash 时，检查本地 content_hash
                    if tracked and not remote_hash and tracked.get("hash"):
                        need_content_paths.append(path)
                        continue

                    # 新笔记或有变更
                    need_content_paths.append(path)

                # 5. 批量检查 restore_deleted（仅对 unchanged 的笔记）
                if self.restore_deleted and unchanged_paths:
                    tracked_doc_ids = {
                        p: self._sync_state[p]["doc_id"]
                        for p in unchanged_paths
                        if self._sync_state[p].get("doc_id")
                    }
                    if tracked_doc_ids:
                        existence = await self._batch_check_docs_exist(list(tracked_doc_ids.values()))
                        for path in unchanged_paths:
                            doc_id = tracked_doc_ids.get(path)
                            if doc_id and not existence.get(doc_id, True):
                                # 文档已从知识库删除，需要重新上传
                                logger.info(f"检测到文档已从知识库删除，将重新上传: {path}")
                                del self._sync_state[path]
                                need_content_paths.append(path)
                            else:
                                result["unchanged"] += 1
                    else:
                        result["unchanged"] += len(unchanged_paths)
                else:
                    result["unchanged"] += len(unchanged_paths)

                # 6. 并发获取需要处理的笔记内容
                if need_content_paths:
                    logger.info(f"需处理 {len(need_content_paths)} 条笔记（并发获取中...）")
                    contents = await self._fns_get_notes_concurrent(client, need_content_paths)
                else:
                    contents = {}

                # 7. 处理每条笔记
                for path in need_content_paths:
                    content = contents.get(path)
                    if content is None:
                        result["errors"] += 1
                        continue

                    content = self._strip_frontmatter(content)
                    content = self._clean_obsidian_syntax(content)

                    # 文件大小检查
                    content_size_kb = len(content.encode("utf-8")) / 1024
                    if self.max_file_size > 0 and content_size_kb > self.max_file_size:
                        result["skipped_size"] += 1
                        logger.debug(f"跳过（{content_size_kb:.0f}KB > {self.max_file_size}KB）: {path}")
                        continue

                    if not content.strip():
                        result["unchanged"] += 1
                        continue

                    # 内容 hash 比对
                    content_hash = hashlib.md5(content.encode("utf-8")).hexdigest()
                    tracked = self._sync_state.get(path)
                    if tracked and tracked.get("hash") == content_hash:
                        result["unchanged"] += 1
                        # 更新 remote_hash
                        remote_hash = current_notes[path].get("contentHash", "")
                        if remote_hash:
                            self._sync_state[path]["remote_hash"] = remote_hash
                        continue

                    # 上传
                    doc_id = None
                    chunk_hashes = []

                    # 已有文档且有 chunk_hashes → 尝试增量上传
                    if tracked and tracked.get("doc_id") and tracked.get("chunk_hashes"):
                        doc_id, chunk_hashes = await self._upload_incremental(
                            path, content, tracked["doc_id"]
                        )

                    # 新文档 或 增量失败 → 全量上传
                    if not doc_id:
                        doc_id = await self._upload_document(path, content)
                        if doc_id:
                            # 全量上传成功后计算 chunk_hashes 供下次增量使用
                            try:
                                chunks = await self._chunker.chunk(content)
                                chunk_hashes = [hashlib.md5(c.encode("utf-8")).hexdigest() for c in chunks]
                            except Exception:
                                chunk_hashes = []

                    if doc_id:
                        # 全量上传时删除旧文档（增量上传已内部处理）
                        if tracked and tracked.get("doc_id") and tracked["doc_id"] != doc_id:
                            await self._delete_document(tracked["doc_id"])

                        remote_hash = current_notes[path].get("contentHash", "")
                        self._sync_state[path] = {
                            "hash": content_hash,
                            "remote_hash": remote_hash,
                            "doc_id": doc_id,
                            "chunk_hashes": chunk_hashes,
                            "sync_time": time.time(),
                        }
                        if tracked:
                            result["updated"] += 1
                        else:
                            result["new"] += 1
                        logger.info(f"已同步: {path} ({'更新' if tracked else '新增'})")
                    else:
                        result["errors"] += 1

                    # 进度回调
                    if progress_callback:
                        processed = result["new"] + result["updated"] + result["errors"] + result["unchanged"] + result["skipped_size"]
                        await progress_callback(processed, len(current_notes), path)

                    # 节奏控制：每处理 5 条让出事件循环
                    if (result["new"] + result["updated"]) % 5 == 0:
                        await asyncio.sleep(0.1)

            # 异步保存状态
            await self._save_state()
            result["duration"] = time.time() - result["start_time"]
            self._last_sync_time = time.time()
            self._sync_count += 1

            # 定期全量校验：检查所有已记录的 doc_id 是否仍存在于知识库
            if self.verify_interval > 0 and self._sync_count % self.verify_interval == 0:
                verify_removed = await self._verify_all_doc_ids()
                if verify_removed:
                    result["verify_removed"] = verify_removed
                    await self._save_state()

            summary = (
                f"同步完成: 新增 {result['new']}, 更新 {result['updated']}, "
                f"删除 {result['deleted']}, 未变 {result['unchanged']}"
            )
            if result["skipped_size"]:
                summary += f", 跳过(大文件) {result['skipped_size']}"
            if result.get("verify_removed"):
                summary += f", 校验恢复 {result['verify_removed']}"
            summary += f", 错误 {result['errors']}, 耗时 {result['duration']:.1f}s"
            logger.info(summary)

        except Exception as e:
            logger.error(f"同步异常: {e}", exc_info=True)
            result["error"] = str(e)
        finally:
            self._is_syncing = False

        return result

    # ── 后台任务 ──────────────────────────────────────────────

    async def _background_sync_loop(self):
        logger.info(f"后台同步已启动，间隔 {self.sync_interval}s")
        while True:
            try:
                await asyncio.sleep(self.sync_interval)
                if self.auto_sync and self.fns_url and self.fns_vault:
                    logger.info("开始自动同步...")
                    await self._do_sync()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"后台同步出错: {e}")
                await asyncio.sleep(60)

    @filter.on_astrbot_loaded()
    async def on_loaded(self):
        logger.info("Obsidian KB Sync v2 插件已加载")
        if self.auto_sync and self.fns_url and self.fns_vault and self.fns_token:
            self._sync_task = asyncio.create_task(self._background_sync_loop())

    # ── 指令 ──────────────────────────────────────────────────

    @filter.command("obsidian_sync")
    async def manual_sync(self, event: AstrMessageEvent):
        '''手动触发 Obsidian 同步'''
        if not self.fns_url or not self.fns_token:
            yield event.plain_result("❌ 请先配置 FNS 服务地址和 Token")
            return

        if self._is_syncing:
            yield event.plain_result("⏳ 已有同步任务在执行中，请稍候...")
            return

        yield event.plain_result("🔄 正在从 Fast Note Sync 同步笔记...")

        # 带进度反馈的回调
        last_report = [0]

        async def progress_cb(processed, total, current_path):
            now = time.time()
            if now - last_report[0] >= 30 and total > 0:  # 每 30s 报告一次
                last_report[0] = now
                pct = processed / total * 100
                logger.info(f"同步进度: {processed}/{total} ({pct:.0f}%) - {current_path}")

        result = await self._do_sync(progress_callback=progress_cb)
        if "error" in result:
            yield event.plain_result(f"❌ 同步失败: {result['error']}")
        else:
            msg = (
                f"✅ 同步完成！\n"
                f"📊 总笔记: {result['total']}\n"
                f"  新增: {result['new']} | 更新: {result['updated']} | 删除: {result['deleted']}\n"
                f"  未变: {result['unchanged']}"
            )
            if result.get("skipped_size"):
                msg += f" | 跳过(大文件): {result['skipped_size']}"
            msg += f" | 错误: {result['errors']}\n⏱️ 耗时: {result.get('duration', 0):.1f}s"
            yield event.plain_result(msg)

    @filter.command("obsidian_status")
    async def show_status(self, event: AstrMessageEvent):
        '''查看 Obsidian 同步状态'''
        tracked = len(self._sync_state)
        last_sync = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(self._last_sync_time)) if self._last_sync_time else "从未同步"
        yield event.plain_result(
            f"📁 Obsidian 知识库同步 v2\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"🔗 FNS: {self.fns_url or '未配置'}\n"
            f"📂 Vault: {self.fns_vault or '未配置'}\n"
            f"📚 知识库: {self.kb_name} ({self.kb_id or '未设置'})\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"📋 已同步: {tracked} 条\n"
            f"🔄 自动同步: {'开启' if self.auto_sync else '关闭'}\n"
            f"⏰ 间隔: {self.sync_interval}s\n"
            f"🚀 并发: {self.concurrent_fetches}\n"
            f"📦 最大文件: {self.max_file_size}KB\n"
            f"🔍 校验间隔: 每 {self.verify_interval} 次同步\n"
            f"🕐 上次: {last_sync}\n"
            f"📊 次数: {self._sync_count}"
        )

    @filter.command("obsidian_reset")
    async def reset_sync(self, event: AstrMessageEvent):
        '''重置同步状态'''
        self._sync_state = {}
        await self._save_state()
        self._sync_count = 0
        self._last_sync_time = 0
        self._kb_helper = None
        yield event.plain_result("✅ 已重置！下次同步将全量重新上传。")

    async def terminate(self):
        if self._sync_task and not self._sync_task.done():
            self._sync_task.cancel()
            try:
                await self._sync_task
            except asyncio.CancelledError:
                pass
        await self._save_state()
        logger.info("Obsidian KB Sync v2 插件已停止")

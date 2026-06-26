"""
AstrBot 插件：Obsidian KB Staging Layer

在 FNS（Fast Note Sync）和 AstrBot 知识库之间添加一个暂存层（Staging）。
提供 Web Dashboard 用于浏览、编辑暂存文档，支持选择性推送到知识库。

功能：
- 从 FNS 拉取所有笔记到本地暂存目录（保留原始内容）
- Web Dashboard 浏览文件夹/文档、编辑内容、设置元数据
- 选择性同步：标记 sync_to_kb 的文档推送到 AstrBot 知识库
- 增量同步：pre_chunk 模式下只重新 embedding 变化的 chunk
"""

import asyncio
import hashlib
import json
import os
import re
import time
from pathlib import Path
from typing import Optional

import httpx
from quart import Quart, jsonify, request

from astrbot.api import AstrBotConfig, logger
from astrbot.api.event import AstrMessageEvent, filter
from astrbot.api.star import Context, Star
from astrbot.core.knowledge_base.chunking.markdown import MarkdownChunker
from astrbot.core.utils.astrbot_path import get_astrbot_data_path


# ═══════════════════════════════════════════════════════════════════
#  FNS HTTP 客户端（带重试）
# ═══════════════════════════════════════════════════════════════════


class FNSClient:
    """Fast Note Sync 异步 HTTP 客户端，共享 httpx.AsyncClient 实例。"""

    def __init__(self, base_url: str, token: str, vault: str,
                 retry_count: int = 3, concurrency: int = 5):
        self.base_url = base_url.rstrip("/")
        self.token = token
        self.vault = vault
        self.retry_count = retry_count
        self.concurrency = concurrency
        self._client: Optional[httpx.AsyncClient] = None

    async def _get_client(self) -> httpx.AsyncClient:
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(
                limits=httpx.Limits(max_connections=20, max_keepalive_connections=10),
                timeout=httpx.Timeout(30.0, connect=10.0),
            )
        return self._client

    async def close(self):
        if self._client and not self._client.is_closed:
            await self._client.aclose()
            self._client = None

    def _headers(self) -> dict:
        return {"token": self.token}

    async def _retry_request(self, method: str, url: str,
                              max_retries: Optional[int] = None, **kwargs) -> Optional[httpx.Response]:
        """带指数退避重试的 HTTP 请求。401 不重试。"""
        retries = max_retries or self.retry_count
        client = await self._get_client()
        for attempt in range(retries):
            try:
                resp = await getattr(client, method)(url, **kwargs)
                if resp.status_code == 401:
                    logger.error("FNS Token 无效或已过期 (401)")
                    return None
                if resp.status_code == 200:
                    return resp
                if attempt < retries - 1:
                    wait = 2 ** attempt
                    logger.warning(
                        f"FNS 请求 {url} 返回 {resp.status_code}，"
                        f"{wait}s 后重试 ({attempt + 1}/{retries})"
                    )
                    await asyncio.sleep(wait)
                    continue
                logger.warning(f"FNS 请求 {url} 最终返回 {resp.status_code}")
                return resp
            except (httpx.ConnectError, httpx.ReadTimeout, httpx.WriteTimeout) as e:
                if attempt < retries - 1:
                    wait = 2 ** attempt
                    logger.warning(
                        f"FNS 请求 {url} 失败: {e}，{wait}s 后重试 ({attempt + 1}/{retries})"
                    )
                    await asyncio.sleep(wait)
                else:
                    logger.error(f"FNS 请求 {url} 最终失败: {e}")
                    return None
            except Exception as e:
                logger.error(f"FNS 请求 {url} 异常: {type(e).__name__}: {e}")
                return None
        return None

    async def list_notes(self) -> list[dict]:
        """列出 vault 中所有笔记（分页，不含内容）。"""
        notes: list[dict] = []
        page = 1
        while True:
            resp = await self._retry_request(
                "get",
                f"{self.base_url}/api/notes",
                params={"vault": self.vault, "page": page, "pageSize": 100},
                headers=self._headers(),
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

    async def get_note(self, path: str) -> Optional[str]:
        """获取单条笔记内容。"""
        resp = await self._retry_request(
            "get",
            f"{self.base_url}/api/note",
            params={"vault": self.vault, "path": path},
            headers=self._headers(),
        )
        if resp is not None:
            return resp.json().get("data", {}).get("content", "")
        return None

    async def get_notes_concurrent(self, paths: list[str]) -> dict[str, Optional[str]]:
        """并发获取多条笔记内容。"""
        sem = asyncio.Semaphore(self.concurrency)
        results: dict[str, Optional[str]] = {}

        async def fetch_one(p: str):
            async with sem:
                content = await self.get_note(p)
                results[p] = content

        tasks = [fetch_one(p) for p in paths]
        await asyncio.gather(*tasks, return_exceptions=True)
        return results

    async def upload_note(self, path: str, content: str) -> bool:
        """上传/更新单条笔记到 FNS。"""
        resp = await self._retry_request(
            "post",
            f"{self.base_url}/api/note",
            json={"vault": self.vault, "path": path, "content": content},
            headers={
                "token": self.token,
                "X-Client": "WebGui",
                "X-Client-Name": "AstrBot-Staging",
                "X-Client-Version": "1.0.0",
                "Domain": self.base_url,
            },
        )
        if resp is not None and resp.status_code == 200:
            data = resp.json()
            return data.get("code") == 0 or data.get("success", False) or data.get("status", False)
        return False

    async def upload_notes_concurrent(self, items: dict[str, str]) -> dict[str, bool]:
        """并发上传多条笔记到 FNS。items: {path: content}"""
        sem = asyncio.Semaphore(self.concurrency)
        results: dict[str, bool] = {}

        async def upload_one(path: str, content: str):
            async with sem:
                results[path] = await self.upload_note(path, content)

        tasks = [upload_one(p, c) for p, c in items.items()]
        await asyncio.gather(*tasks, return_exceptions=True)
        return results


# ═══════════════════════════════════════════════════════════════════
#  内容清洗（Obsidian → 纯 Markdown）
# ═══════════════════════════════════════════════════════════════════


def clean_obsidian_content(content: str) -> str:
    """将 Obsidian 扩展 Markdown 转为纯 Markdown。

    清洗步骤：
    1. 去除 YAML frontmatter
    2. 转换 wikilinks → 普通链接/文本
    3. 去除 callouts 标记
    4. 去除高亮 ==text==
    5. 去除注释 %%text%%
    """
    # 1. 去除 frontmatter
    content = re.sub(r"^---\s*\n.*?\n---\s*\n", "", content, flags=re.DOTALL).strip()

    # 2. 转换 wikilinks
    # ![[image]] → [image]
    content = re.sub(r"!\[\[([^\]]+)\]\]", r"[\1]", content)
    # [[target|display]] → display
    content = re.sub(r"\[\[([^\]|]+)\|([^\]]+)\]\]", r"\2", content)
    # [[target]] → target
    content = re.sub(r"\[\[([^\]]+)\]\]", r"\1", content)

    # 3. 去除 callouts 标记（> [!type] → 去掉标记行首）
    content = re.sub(r"^>\s*\[[^\]]*\]\s*", "", content, flags=re.MULTILINE)

    # 4. 去除高亮
    content = re.sub(r"==([^=]+)==", r"\1", content)

    # 5. 去除注释
    content = re.sub(r"%%.*?%%", "", content, flags=re.DOTALL)

    return content


# ═══════════════════════════════════════════════════════════════════
#  StagingManager：本地暂存目录管理
# ═══════════════════════════════════════════════════════════════════


def _default_metadata() -> dict:
    """每篇文档的默认元数据模板。"""
    return {
        "fns_hash": "",          # FNS 返回的 contentHash（可能为空）
        "content_hash": "",      # 文件内容 MD5
        "fns_content_hash": "",  # 上次从 FNS 同步时的内容 MD5（用于冲突检测）
        "size_kb": 0.0,          # 文件大小 KB
        "sync_to_kb": False,     # 是否标记推送到知识库
        "pre_chunk": True,       # 是否使用增量分块同步
        "chunk_hashes": [],      # 上次同步的 chunk hash 列表
        "kb_doc_id": "",         # 知识库中的 doc_id
        "last_sync": 0.0,        # 上次推送到 KB 的时间戳
        "staged_at": 0.0,        # 暂存时间戳
    }


class StagingManager:
    """管理本地暂存目录，镜像 Obsidian vault 结构。"""

    def __init__(self, data_dir: Path, max_file_size_kb: int = 100):
        self.staging_dir = data_dir / "staging"
        self.metadata_file = data_dir / "staging_metadata.json"
        self.max_file_size_kb = max_file_size_kb
        self.staging_dir.mkdir(parents=True, exist_ok=True)

        self._metadata: dict[str, dict] = {}
        self._sync_history_file = data_dir / "sync_history.json"
        self._load_metadata_sync()

    # ── 元数据读写（同步加载，异步保存） ─────────────────────────

    def _load_metadata_sync(self):
        """同步加载元数据文件。"""
        try:
            if self.metadata_file.exists():
                with open(self.metadata_file, "r", encoding="utf-8") as f:
                    self._metadata = json.load(f)
                logger.info(f"Staging: 已加载 {len(self._metadata)} 条元数据")
        except Exception as e:
            logger.error(f"Staging: 加载元数据失败: {e}")
            self._metadata = {}

    async def save_metadata(self):
        """异步保存元数据（使用 run_in_executor 避免阻塞）。"""
        try:
            content = json.dumps(self._metadata, ensure_ascii=False, indent=2)
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(None, self._write_metadata_sync, content)
        except Exception as e:
            logger.error(f"Staging: 保存元数据失败: {e}")

    def _write_metadata_sync(self, content: str):
        with open(self.metadata_file, "w", encoding="utf-8") as f:
            f.write(content)

    # ── 文档操作 ────────────────────────────────────────────────

    def _doc_path(self, path: str) -> Path:
        """获取暂存文件的完整路径。"""
        return self.staging_dir / path

    def get_document(self, path: str) -> Optional[str]:
        """读取暂存文档内容。"""
        fp = self._doc_path(path)
        if not fp.exists():
            return None
        try:
            return fp.read_text(encoding="utf-8")
        except Exception as e:
            logger.error(f"Staging: 读取文档失败 {path}: {e}")
            return None

    def save_document(self, path: str, content: str) -> bool:
        """保存编辑后的文档内容（更新 content_hash 和 size_kb）。"""
        fp = self._doc_path(path)
        try:
            fp.parent.mkdir(parents=True, exist_ok=True)
            fp.write_text(content, encoding="utf-8")
            # 更新元数据
            meta = self._metadata.get(path, _default_metadata())
            meta["content_hash"] = hashlib.md5(content.encode("utf-8")).hexdigest()
            meta["size_kb"] = len(content.encode("utf-8")) / 1024
            self._metadata[path] = meta
            return True
        except Exception as e:
            logger.error(f"Staging: 保存文档失败 {path}: {e}")
            return False

    def delete_document(self, path: str) -> bool:
        """删除暂存文档及其元数据。"""
        fp = self._doc_path(path)
        try:
            if fp.exists():
                fp.unlink()
            self._metadata.pop(path, None)
            return True
        except Exception as e:
            logger.error(f"Staging: 删除文档失败 {path}: {e}")
            return False

    def get_metadata(self, path: str) -> Optional[dict]:
        """获取文档元数据。"""
        return self._metadata.get(path)

    def update_metadata(self, path: str, updates: dict) -> bool:
        """更新文档元数据字段。"""
        if path not in self._metadata:
            return False
        self._metadata[path].update(updates)
        return True

    def rebuild_metadata(self, clear_kb: bool = True, clear_fns: bool = False) -> int:
        """重建暂存元数据，返回受影响的文档数。

        clear_kb=True: 清除 kb_doc_id / chunk_hashes / last_sync，
                       下次 KB 同步将全量重新上传。
        clear_fns=True: 额外清除 fns_hash / content_hash / fns_content_hash，
                        下次 FNS 同步将重新拉取所有内容。
        """
        count = 0
        for path, meta in self._metadata.items():
            changed = False
            if clear_kb:
                for key in ("kb_doc_id", "chunk_hashes", "last_sync"):
                    if meta.get(key):
                        meta[key] = [] if key == "chunk_hashes" else ("" if key == "kb_doc_id" else 0.0)
                        changed = True
            if clear_fns:
                for key in ("fns_hash", "content_hash", "fns_content_hash"):
                    if meta.get(key):
                        meta[key] = ""
                        changed = True
            if changed:
                count += 1
        return count

    def list_documents(self, folder: Optional[str] = None) -> list[dict]:
        """列出暂存文档及其元数据。可选按文件夹过滤。"""
        results = []
        for path, meta in self._metadata.items():
            if folder and not path.startswith(folder):
                continue
            results.append({"path": path, **meta})
        results.sort(key=lambda x: x["path"])
        return results

    def get_folder_tree(self) -> dict:
        """构建文件夹树结构（用于侧边栏导航）。"""
        tree: dict = {}
        for path in self._metadata:
            parts = path.split("/")
            node = tree
            # 遍历目录部分
            for part in parts[:-1]:
                if part not in node:
                    node[part] = {"__files__": []}
                node = node[part]
            # 添加文件
            if "__files__" not in node:
                node["__files__"] = []
            node["__files__"].append({
                "name": parts[-1],
                "path": path,
                "size_kb": self._metadata[path].get("size_kb", 0),
                "sync_to_kb": self._metadata[path].get("sync_to_kb", False),
            })
        return tree

    def get_stats(self) -> dict:
        """获取暂存统计信息。"""
        total = len(self._metadata)
        selected = sum(1 for m in self._metadata.values() if m.get("sync_to_kb"))
        synced = sum(1 for m in self._metadata.values() if m.get("kb_doc_id"))
        pre_chunked = sum(1 for m in self._metadata.values() if m.get("pre_chunk"))
        total_size = sum(m.get("size_kb", 0) for m in self._metadata.values())
        return {
            "total_documents": total,
            "selected_for_sync": selected,
            "synced_to_kb": synced,
            "pre_chunked": pre_chunked,
            "total_size_kb": round(total_size, 2),
        }

    # ── 同步历史 ────────────────────────────────────────────────

    async def _load_sync_history(self) -> list[dict]:
        try:
            if self._sync_history_file.exists():
                import json as _json
                content = self._sync_history_file.read_text(encoding="utf-8")
                return _json.loads(content)
        except Exception:
            pass
        return []

    async def _save_sync_record(self, record: dict):
        try:
            import json as _json
            history = await self._load_sync_history()
            history.insert(0, record)
            history = history[:50]  # 保留最近 50 条
            loop = asyncio.get_event_loop()
            content = _json.dumps(history, ensure_ascii=False, indent=2)
            await loop.run_in_executor(
                None, self._sync_history_file.write_text, content, "utf-8"
            )
        except Exception as e:
            logger.error(f"Staging: 保存同步历史失败: {e}")

    # ── FNS 同步 ────────────────────────────────────────────────

    async def sync_from_fns(self, fns_client: FNSClient,
                            exclude_patterns: Optional[list[str]] = None,
                            progress_callback=None) -> dict:
        """从 FNS 拉取所有笔记到暂存目录。

        返回 {total, new, updated, unchanged, skipped, errors, duration}。
        progress_callback: async callable(phase, current, total, filename, **kw)
        """
        from fnmatch import fnmatch

        _pc = progress_callback
        exclude_patterns = exclude_patterns or []
        result = {
            "total": 0, "new": 0, "updated": 0,
            "unchanged": 0, "skipped": 0, "errors": 0,
            "start_time": time.time(),
        }

        # 1. 列出所有笔记
        if _pc:
            await _pc("listing", 0, 0, "")
        notes = await fns_client.list_notes()
        result["total"] = len(notes)
        logger.info(f"Staging: FNS 返回 {len(notes)} 条笔记")

        if not notes:
            result["duration"] = time.time() - result["start_time"]
            await self._save_sync_record({"type": "fns", "status": "ok", **result})
            if _pc:
                await _pc("done", 0, 0, "")
            return result

        # 2. 过滤排除项
        def should_exclude(note_path: str) -> bool:
            for pattern in exclude_patterns:
                if fnmatch(note_path, pattern) or fnmatch(note_path.split("/")[-1], pattern):
                    return True
                for part in note_path.split("/"):
                    if fnmatch(part, pattern):
                        return True
            return False

        filtered_notes = [n for n in notes if n.get("path") and not should_exclude(n["path"])]

        # 3. 分类：需要获取内容 vs 可跳过
        need_content_paths: list[str] = []
        for note in filtered_notes:
            path = note["path"]
            remote_hash = note.get("contentHash", "")
            existing = self._metadata.get(path)

            # 快速跳过：fns_hash 未变
            if existing and remote_hash and existing.get("fns_hash") == remote_hash:
                result["unchanged"] += 1
                continue

            # 无 remote_hash 时，检查本地 content_hash（需要获取内容来比对）
            need_content_paths.append(path)

        # 4. 并发获取需要处理的笔记内容
        if need_content_paths:
            logger.info(f"Staging: 需获取 {len(need_content_paths)} 条笔记内容")
            if _pc:
                await _pc("fetching", 0, len(need_content_paths), f"{len(need_content_paths)} 篇文档")
            contents = await fns_client.get_notes_concurrent(need_content_paths)
        else:
            contents = {}

        # 5. 处理每条笔记
        notes_by_path = {n["path"]: n for n in filtered_notes}
        processed = 0
        total_to_process = len(need_content_paths)

        for path in need_content_paths:
            processed += 1
            if _pc:
                await _pc("processing", processed, total_to_process, path.split("/")[-1])
            content = contents.get(path)
            if content is None:
                result["errors"] += 1
                continue

            # 文件大小检查（基于原始内容字节数）
            content_bytes = content.encode("utf-8")
            content_size_kb = len(content_bytes) / 1024
            if self.max_file_size_kb > 0 and content_size_kb > self.max_file_size_kb:
                result["skipped"] += 1
                logger.debug(f"Staging: 跳过 {path}（{content_size_kb:.0f}KB > {self.max_file_size_kb}KB）")
                continue

            if not content.strip():
                result["unchanged"] += 1
                continue

            # 内容 hash 比对
            content_hash = hashlib.md5(content_bytes).hexdigest()
            existing = self._metadata.get(path)
            if existing and existing.get("content_hash") == content_hash:
                result["unchanged"] += 1
                # 更新 fns_hash
                remote_hash = notes_by_path[path].get("contentHash", "")
                if remote_hash:
                    self._metadata[path]["fns_hash"] = remote_hash
                continue

            # 写入暂存文件（保留原始内容）
            fp = self._doc_path(path)
            fp.parent.mkdir(parents=True, exist_ok=True)
            fp.write_text(content, encoding="utf-8")

            # 更新元数据
            remote_hash = notes_by_path[path].get("contentHash", "")
            # FNS contentHash 可能为空 → 用内容 MD5 作为 fallback
            fns_hash = remote_hash if remote_hash else content_hash

            old_meta = self._metadata.get(path, _default_metadata())
            old_meta.update({
                "fns_hash": fns_hash,
                "content_hash": content_hash,
                "fns_content_hash": content_hash,  # 记录 FNS 同步时的内容 hash
                "size_kb": content_size_kb,
                "staged_at": time.time(),
            })
            self._metadata[path] = old_meta

            if existing:
                result["updated"] += 1
            else:
                result["new"] += 1

        # 6. 清理已删除的笔记（暂存中有但 FNS 中已不存在）
        fns_paths = {n["path"] for n in filtered_notes}
        stale_paths = [p for p in list(self._metadata.keys()) if p not in fns_paths]
        for p in stale_paths:
            fp = self._doc_path(p)
            if fp.exists():
                fp.unlink()
            del self._metadata[p]

        # 7. 保存元数据
        await self.save_metadata()

        result["duration"] = time.time() - result["start_time"]
        result["deleted"] = len(stale_paths)
        logger.info(
            f"Staging: 同步完成 | 新增 {result['new']}, 更新 {result['updated']}, "
            f"未变 {result['unchanged']}, 跳过 {result['skipped']}, "
            f"错误 {result['errors']}, 删除 {len(stale_paths)}, "
            f"耗时 {result['duration']:.1f}s"
        )
        await self._save_sync_record({"type": "fns", "status": "ok", **result})
        if _pc:
            await _pc("done", processed if need_content_paths else 0,
                      total_to_process if need_content_paths else 0, "")
        return result

    async def sync_to_fns(self, fns_client: FNSClient,
                          paths: Optional[list[str]] = None,
                          progress_callback=None) -> dict:
        """将暂存区文档推送到 FNS。

        paths: 指定要推送的路径列表。None = 推送所有有变更的文档。
        返回 {total, uploaded, unchanged, errors, duration}。
        """
        _pc = progress_callback
        result = {
            "total": 0, "uploaded": 0, "unchanged": 0, "errors": 0,
            "start_time": time.time(),
        }

        # 确定要推送的文档
        if paths is not None:
            target_paths = paths
        else:
            # 推送所有暂存文档
            target_paths = list(self._metadata.keys())

        result["total"] = len(target_paths)
        if not target_paths:
            result["duration"] = time.time() - result["start_time"]
            await self._save_sync_record({"type": "to_fns", "status": "ok", **result})
            if _pc:
                await _pc("done", 0, 0, "")
            return result

        # 构建 {path: content} 字典
        to_upload: dict[str, str] = {}
        for path in target_paths:
            content = self.get_document(path)
            if content is None:
                result["errors"] += 1
                continue
            to_upload[path] = content

        if not to_upload:
            result["duration"] = time.time() - result["start_time"]
            return result

        # 并发上传
        if _pc:
            await _pc("uploading", 0, len(to_upload), f"{len(to_upload)} 篇文档")
        upload_results = await fns_client.upload_notes_concurrent(to_upload)

        uploaded_count = 0
        for path, ok in upload_results.items():
            uploaded_count += 1
            if ok:
                result["uploaded"] += 1
            else:
                result["errors"] += 1
            if _pc:
                await _pc("uploading", uploaded_count, len(to_upload), path.split("/")[-1])

        result["duration"] = time.time() - result["start_time"]
        logger.info(
            f"Staging: 推送到 FNS 完成 | 总计 {result['total']}, "
            f"成功 {result['uploaded']}, 错误 {result['errors']}, "
            f"耗时 {result['duration']:.1f}s"
        )
        await self._save_sync_record({"type": "to_fns", "status": "ok", **result})
        if _pc:
            await _pc("done", uploaded_count, len(to_upload), "")
        return result


    # ── 冲突检测 ────────────────────────────────────────────────

    async def detect_conflicts(self, fns_client: FNSClient,
                                exclude_patterns: Optional[list[str]] = None) -> list[dict]:
        """检测 FNS 和暂存区之间的冲突。

        返回冲突列表，每项包含:
        - path: 文档路径
        - type: 冲突类型
          - "both_modified": FNS 和暂存区都已修改
          - "remote_modified": 仅 FNS 已修改
          - "remote_deleted": FNS 已删除但暂存区仍有
        - fns_hash: FNS 当前 hash
        - local_hash: 暂存区当前 content_hash
        - base_hash: 上次同步时的 fns_content_hash
        """
        from fnmatch import fnmatch

        exclude_patterns = exclude_patterns or []
        conflicts: list[dict] = []

        def should_exclude(note_path: str) -> bool:
            for pattern in exclude_patterns:
                if fnmatch(note_path, pattern) or fnmatch(note_path.split("/")[-1], pattern):
                    return True
                for part in note_path.split("/"):
                    if fnmatch(part, pattern):
                        return True
            return False

        # 1. 获取 FNS 当前笔记列表（含 contentHash）
        notes = await fns_client.list_notes()
        fns_map: dict[str, dict] = {}
        for n in notes:
            p = n.get("path", "")
            if p and not should_exclude(p):
                fns_map[p] = n

        # 2. 检测冲突
        for path, meta in self._metadata.items():
            if should_exclude(path):
                continue

            stored_fns_hash = meta.get("fns_hash", "")
            local_hash = meta.get("content_hash", "")
            base_hash = meta.get("fns_content_hash", "")  # 上次 FNS 同步时的 hash

            if path not in fns_map:
                # FNS 已删除
                if local_hash and stored_fns_hash:
                    conflicts.append({
                        "path": path,
                        "type": "remote_deleted",
                        "fns_hash": "",
                        "local_hash": local_hash,
                        "base_hash": base_hash,
                        "size_kb": meta.get("size_kb", 0),
                    })
                continue

            fns_note = fns_map[path]
            current_fns_hash = fns_note.get("contentHash", "")

            # 无 hash 或 hash 未变 → 无冲突
            if not current_fns_hash or current_fns_hash == stored_fns_hash:
                continue

            # FNS 已变化（current_fns_hash != stored_fns_hash）
            # 判断暂存区是否也有本地修改
            local_changed = bool(base_hash and local_hash != base_hash)

            if local_changed:
                conflict_type = "both_modified"
            else:
                conflict_type = "remote_modified"

            conflicts.append({
                "path": path,
                "type": conflict_type,
                "fns_hash": current_fns_hash,
                "local_hash": local_hash,
                "base_hash": base_hash,
                "size_kb": meta.get("size_kb", 0),
            })

        # 按类型排序：both_modified 优先
        type_order = {"both_modified": 0, "remote_modified": 1, "remote_deleted": 2}
        conflicts.sort(key=lambda c: (type_order.get(c["type"], 9), c["path"]))

        logger.info(f"Staging: 冲突检测完成，发现 {len(conflicts)} 个冲突")
        return conflicts

    async def resolve_conflict(self, path: str, resolution: str,
                                fns_client: Optional[FNSClient] = None) -> dict:
        """解决单个冲突。

        resolution:
        - "keep_fns": 用 FNS 版本覆盖暂存区
        - "keep_local": 保留暂存区版本（更新 fns_hash 和 fns_content_hash）
        - "skip": 不做任何操作
        """
        meta = self._metadata.get(path)
        if not meta:
            return {"error": f"文档 {path} 不在暂存区"}

        if resolution == "skip":
            return {"success": True, "action": "skipped"}

        if resolution == "keep_fns":
            # 从 FNS 重新拉取内容覆盖暂存区
            if not fns_client:
                return {"error": "需要 FNS 客户端"}
            content = await fns_client.get_note(path)
            if content is None:
                return {"error": f"无法从 FNS 获取 {path}"}
            content_bytes = content.encode("utf-8")
            content_hash = hashlib.md5(content_bytes).hexdigest()
            fp = self._doc_path(path)
            fp.parent.mkdir(parents=True, exist_ok=True)
            fp.write_text(content, encoding="utf-8")
            meta.update({
                "fns_hash": meta.get("fns_hash", ""),
                "content_hash": content_hash,
                "fns_content_hash": content_hash,
                "size_kb": len(content_bytes) / 1024,
                "staged_at": time.time(),
            })
            await self.save_metadata()
            logger.info(f"Staging: 冲突解决 keep_fns → {path}")
            return {"success": True, "action": "kept_fns"}

        if resolution == "keep_local":
            # 保留暂存区版本，更新 base hash 为当前 local hash
            local_hash = meta.get("content_hash", "")
            meta["fns_content_hash"] = local_hash
            await self.save_metadata()
            logger.info(f"Staging: 冲突解决 keep_local → {path}")
            return {"success": True, "action": "kept_local"}

        return {"error": f"未知解决方式: {resolution}"}

    async def resolve_conflicts_batch(self, conflicts: list[dict],
                                       resolution: str,
                                       fns_client: Optional[FNSClient] = None) -> dict:
        """批量解决冲突。"""
        results = {"resolved": 0, "errors": 0, "details": []}
        for c in conflicts:
            r = await self.resolve_conflict(c["path"], resolution, fns_client)
            if r.get("success"):
                results["resolved"] += 1
            else:
                results["errors"] += 1
            results["details"].append({"path": c["path"], **r})
        return results


# ═══════════════════════════════════════════════════════════════════
#  SyncEngine：Staging → AstrBot 知识库同步
# ═══════════════════════════════════════════════════════════════════


class SyncEngine:
    """将暂存中标记的文档同步到 AstrBot 知识库。

    - pre_chunk=True: 增量同步（分块 + hash 比对 + 只重新 embedding 变化的 chunk）
    - pre_chunk=False: 全量上传
    """

    def __init__(self, staging: StagingManager, context: Context,
                 kb_id: str, kb_name: str,
                 chunk_size: int = 512, chunk_overlap: int = 50):
        self.staging = staging
        self.context = context
        self.kb_id = kb_id
        self.kb_name = kb_name
        self._kb_helper = None
        self._chunker = MarkdownChunker(
            chunk_size=chunk_size,
            chunk_overlap=chunk_overlap,
        )
        self._is_syncing = False
        self._progress_queues: dict[str, asyncio.Queue] = {}

    # ── KB Helper ───────────────────────────────────────────────

    async def _get_kb_helper(self, force_refresh: bool = False):
        if self._kb_helper and not force_refresh:
            return self._kb_helper
        kb_mgr = self.context.kb_manager
        if self.kb_id:
            helper = await kb_mgr.get_kb(self.kb_id)
            if helper:
                self._kb_helper = helper
                return helper
        helper = await kb_mgr.get_kb_by_name(self.kb_name)
        if helper:
            self.kb_id = helper.kb.kb_id
            self._kb_helper = helper
            return helper
        return None

    async def _ensure_knowledge_base(self):
        helper = await self._get_kb_helper()
        if helper:
            return helper
        kb_mgr = self.context.kb_manager
        try:
            provider_mgr = self.context.provider_manager
            embedding_id = ""
            for p in provider_mgr.provider_insts:
                if "embedding" in p.meta().type.lower():
                    embedding_id = p.id()
                    break
            if not embedding_id:
                logger.error("Staging: 未找到 Embedding 模型提供商")
                return None
            helper = await kb_mgr.create_kb(
                kb_name=self.kb_name,
                description="Obsidian 笔记库暂存同步",
                embedding_provider_id=embedding_id,
            )
            self.kb_id = helper.kb.kb_id
            self._kb_helper = helper
            logger.info(f"Staging: 已创建知识库 {self.kb_name} (ID: {self.kb_id})")
            return helper
        except Exception as e:
            logger.error(f"Staging: 创建知识库失败: {e}")
            return None

    # ── 文档级操作 ──────────────────────────────────────────────

    async def _upload_document(self, file_name: str, content: str) -> Optional[str]:
        """全量上传文档到知识库，返回 doc_id。"""
        helper = await self._get_kb_helper()
        if not helper:
            return None
        try:
            # file_name 必须以 .md 结尾
            if not file_name.endswith(".md"):
                file_name += ".md"
            # file_content 必须是 bytes
            doc = await helper.upload_document(
                file_name=file_name,
                file_content=content.encode("utf-8"),
                file_type="md",
            )
            return doc.doc_id
        except Exception as e:
            logger.error(f"Staging: 上传文档失败 {file_name}: {e}")
            return None

    async def _delete_document(self, doc_id: str) -> bool:
        helper = await self._get_kb_helper()
        if not helper:
            return False
        try:
            await helper.delete_document(doc_id)
            return True
        except Exception as e:
            logger.warning(f"Staging: 删除文档失败 {doc_id}: {e}")
            return False

    async def _upload_incremental(self, path: str, cleaned_content: str,
                                   old_doc_id: str) -> tuple[Optional[str], list[str]]:
        """增量上传：只重新 embedding 变化的 chunk。返回 (doc_id, new_chunk_hashes)。"""
        helper = await self._get_kb_helper()
        if not helper:
            return None, []

        try:
            new_chunks = await self._chunker.chunk(cleaned_content)
            if not new_chunks:
                return None, []

            new_chunk_hashes = [hashlib.md5(c.encode("utf-8")).hexdigest() for c in new_chunks]

            old_chunks_data = await helper.get_chunks_by_doc_id(old_doc_id, limit=9999)
            old_hash_to_id = {}
            for chunk in old_chunks_data:
                chunk_hash = hashlib.md5(chunk["content"].encode("utf-8")).hexdigest()
                old_hash_to_id[chunk_hash] = chunk["chunk_id"]

            # 找出变化的 chunk
            changed_indices = [i for i, h in enumerate(new_chunk_hashes) if h not in old_hash_to_id]
            new_hash_set = set(new_chunk_hashes)
            chunks_to_delete = [cid for h, cid in old_hash_to_id.items() if h not in new_hash_set]

            unchanged = len(new_chunks) - len(changed_indices)
            logger.info(
                f"Staging: 增量分块 {path} | {len(new_chunks)} 块, "
                f"{unchanged} 未变, {len(changed_indices)} 需重嵌, {len(chunks_to_delete)} 待删除"
            )

            if not changed_indices and not chunks_to_delete:
                return old_doc_id, new_chunk_hashes

            vec_db = helper.vec_db

            for chunk_id in chunks_to_delete:
                try:
                    await vec_db.delete(chunk_id)
                except Exception:
                    pass

            for i in changed_indices:
                try:
                    await vec_db.insert(
                        content=new_chunks[i],
                        metadata={
                            "kb_id": self.kb_id,
                            "kb_doc_id": old_doc_id,
                            "chunk_index": i,
                        },
                    )
                except Exception as e:
                    logger.warning(f"Staging: chunk {i} embedding 失败 ({path}): {e}")

            # 更新文档元数据
            try:
                doc = await helper.get_document(old_doc_id)
                if doc:
                    doc.chunk_count = len(new_chunks)
                    async with helper.kb_db.get_db() as session:
                        async with session.begin():
                            session.add(doc)
                            await session.commit()
            except Exception as e:
                logger.warning(f"Staging: 更新文档 chunk_count 失败: {e}")

            await helper.kb_db.update_kb_stats(kb_id=self.kb_id, vec_db=vec_db)
            await helper.refresh_kb()

            return old_doc_id, new_chunk_hashes

        except Exception as e:
            logger.error(f"Staging: 增量上传失败 {path}: {e}", exc_info=True)
            return None, []

    # ── 主同步流程 ──────────────────────────────────────────────

    async def _emit_progress(self, sync_type: str, phase: str, current: int,
                              total: int, filename: str, **kwargs):
        """发送进度事件到 SSE 队列。"""
        q = self._progress_queues.get(sync_type)
        if q:
            try:
                q.put_nowait({"phase": phase, "current": current, "total": total,
                              "filename": filename, **kwargs})
            except Exception:
                pass

    async def sync_to_kb(self, progress_callback=None) -> dict:
        """将暂存中 sync_to_kb=True 的文档推送到 AstrBot 知识库。"""
        if self._is_syncing:
            return {"error": "正在同步中，请稍候"}

        self._is_syncing = True
        _pc = progress_callback
        result = {
            "total": 0, "new": 0, "updated": 0,
            "unchanged": 0, "errors": 0,
            "start_time": time.time(),
        }

        try:
            if _pc:
                await _pc("preparing", 0, 0, "")
            kb_helper = await self._ensure_knowledge_base()
            if not kb_helper:
                return {"error": "无法创建或找到 AstrBot 知识库"}

            selected_docs = self.staging.list_documents()
            # 只处理 sync_to_kb=True 的文档
            selected_docs = [d for d in selected_docs if d.get("sync_to_kb")]
            result["total"] = len(selected_docs)

            if not selected_docs:
                result["duration"] = time.time() - result["start_time"]
                await self.staging._save_sync_record({"type": "kb", "status": "ok", **result})
                if _pc:
                    await _pc("done", 0, 0, "")
                return result

            for idx, doc_info in enumerate(selected_docs, 1):
                path = doc_info["path"]
                meta = doc_info
                fname = path.split("/")[-1]

                if _pc:
                    await _pc("syncing", idx, len(selected_docs), fname)

                # 读取暂存的原始内容
                raw_content = self.staging.get_document(path)
                if raw_content is None:
                    result["errors"] += 1
                    continue

                # 清洗内容（同步到 KB 时才清洗）
                cleaned = clean_obsidian_content(raw_content)
                if not cleaned.strip():
                    result["unchanged"] += 1
                    continue

                cleaned_hash = hashlib.md5(cleaned.encode("utf-8")).hexdigest()

                # 检查是否需要更新（对比清洗后的内容 hash）
                # 使用 content_hash 作为原始 hash 的参考，但真正的判断应基于清洗后的 hash
                # 如果已有 kb_doc_id 且内容未变 → 跳过
                old_kb_doc_id = meta.get("kb_doc_id", "")
                old_chunk_hashes = meta.get("chunk_hashes", [])

                if old_kb_doc_id and old_chunk_hashes:
                    # 尝试增量同步
                    doc_id, chunk_hashes = await self._upload_incremental(
                        path, cleaned, old_kb_doc_id
                    )
                    if doc_id:
                        self.staging.update_metadata(path, {
                            "chunk_hashes": chunk_hashes,
                            "kb_doc_id": doc_id,
                            "last_sync": time.time(),
                        })
                        if doc_id == old_kb_doc_id:
                            result["unchanged"] += 1
                        else:
                            result["updated"] += 1
                        continue

                if old_kb_doc_id and not old_chunk_hashes:
                    # 有 doc_id 但无 chunk_hashes（pre_chunk=False 的全量上传）
                    # 先删除旧文档再重新上传
                    await self._delete_document(old_kb_doc_id)

                # 全量上传
                file_name = fname
                if not file_name.endswith(".md"):
                    file_name += ".md"

                doc_id = await self._upload_document(file_name, cleaned)
                if doc_id:
                    # 计算 chunk_hashes 供下次增量使用
                    chunk_hashes = []
                    try:
                        chunks = await self._chunker.chunk(cleaned)
                        chunk_hashes = [hashlib.md5(c.encode("utf-8")).hexdigest() for c in chunks]
                    except Exception:
                        pass

                    self.staging.update_metadata(path, {
                        "chunk_hashes": chunk_hashes,
                        "kb_doc_id": doc_id,
                        "last_sync": time.time(),
                    })

                    if old_kb_doc_id:
                        result["updated"] += 1
                    else:
                        result["new"] += 1
                else:
                    result["errors"] += 1

                # 节奏控制
                if (result["new"] + result["updated"]) % 5 == 0:
                    await asyncio.sleep(0.1)

            await self.staging.save_metadata()
            result["duration"] = time.time() - result["start_time"]

            logger.info(
                f"Staging: KB 同步完成 | 新增 {result['new']}, 更新 {result['updated']}, "
                f"未变 {result['unchanged']}, 错误 {result['errors']}, "
                f"耗时 {result['duration']:.1f}s"
            )

        except Exception as e:
            logger.error(f"Staging: KB 同步异常: {e}", exc_info=True)
            result["error"] = str(e)
        finally:
            self._is_syncing = False

        await self.staging._save_sync_record({"type": "kb", "status": "error" if result.get("error") else "ok", **result})
        if _pc:
            await _pc("done", result.get("total", 0), result.get("total", 0), "")
        return result


# ═══════════════════════════════════════════════════════════════════
#  Dashboard：Quart Web 服务器
# ═══════════════════════════════════════════════════════════════════


# HTML 从 pages/staging-dashboard/index.html 文件读取（与 AstrBot 插件页面共享同一份）
_DASHBOARD_HTML_PATH = Path(__file__).parent / "pages" / "staging-dashboard" / "index.html"


def _load_dashboard_html() -> str:
    """读取 Dashboard HTML，失败时返回错误提示页。"""
    try:
        return _DASHBOARD_HTML_PATH.read_text(encoding="utf-8")
    except Exception as e:
        logger.error(f"Dashboard HTML 加载失败: {e}")
        return f"<h1>Dashboard HTML 加载失败</h1><p>{e}</p><p>路径: {_DASHBOARD_HTML_PATH}</p>"


class Dashboard:
    """Quart Web 仪表盘，提供 REST API + HTML 页面。"""

    def __init__(self, staging: StagingManager, sync_engine: SyncEngine,
                 fns_client_factory, exclude_patterns: list[str],
                 port: int = 6190, plugin=None):
        self.staging = staging
        self.sync_engine = sync_engine
        self.fns_client_factory = fns_client_factory
        self.exclude_patterns = exclude_patterns
        self.port = port
        self._plugin = plugin
        self.app = Quart(__name__)
        self._server_task: Optional[asyncio.Task] = None
        self._setup_routes()

    def _setup_routes(self):
        app = self.app

        @app.after_request
        async def add_cors_headers(response):
            response.headers["Access-Control-Allow-Origin"] = "*"
            response.headers["Access-Control-Allow-Methods"] = "GET, POST, OPTIONS"
            response.headers["Access-Control-Allow-Headers"] = "Content-Type"
            return response

        @app.route("/")
        async def index():
            html = _load_dashboard_html()
            return html, 200, {"Content-Type": "text/html; charset=utf-8"}

        @app.route("/api/folders")
        async def api_folders():
            return jsonify(self.staging.get_folder_tree())

        @app.route("/api/documents")
        async def api_documents():
            folder = request.args.get("folder", "")
            docs = self.staging.list_documents(folder if folder else None)
            return jsonify(docs)

        @app.route("/api/document")
        async def api_get_document():
            path = request.args.get("path", "")
            if not path:
                return jsonify({"error": "path required"}), 400
            content = self.staging.get_document(path)
            if content is None:
                return jsonify({"error": "not found"}), 404
            return jsonify({"path": path, "content": content})

        @app.route("/api/document", methods=["POST"])
        async def api_save_document():
            data = await request.get_json()
            path = data.get("path", "")
            content = data.get("content", "")
            if not path:
                return jsonify({"error": "path required"}), 400
            ok = self.staging.save_document(path, content)
            if ok:
                await self.staging.save_metadata()
            return jsonify({"success": ok})

        @app.route("/api/document/create", methods=["POST"])
        async def api_create_document():
            data = await request.get_json()
            folder = data.get("folder", "")
            name = data.get("name", "")
            if not name:
                return jsonify({"error": "name required"}), 400
            if not name.endswith(".md"):
                name += ".md"
            path = f"{folder}/{name}" if folder else name
            # Check if already exists
            if self.staging.get_document(path) is not None:
                return jsonify({"error": "文件已存在"}), 409
            ok = self.staging.save_document(path, f"# {name.replace('.md', '')}\n\n")
            if ok:
                await self.staging.save_metadata()
            return jsonify({"success": ok, "path": path})

        @app.route("/api/document/delete", methods=["POST"])
        async def api_delete_document():
            data = await request.get_json()
            path = data.get("path", "")
            if not path:
                return jsonify({"error": "path required"}), 400
            ok = self.staging.delete_document(path)
            if ok:
                await self.staging.save_metadata()
            return jsonify({"success": ok})

        @app.route("/api/document/import", methods=["POST"])
        async def api_import_document():
            files = await request.files
            if "file" not in files:
                return jsonify({"error": "no file"}), 400
            f = files["file"]
            folder = (await request.form).get("folder", "")
            name = f.filename
            if not name:
                return jsonify({"error": "no filename"}), 400
            if not name.endswith(".md"):
                name += ".md"
            path = f"{folder}/{name}" if folder else name
            content = f.read().decode("utf-8", errors="replace")
            ok = self.staging.save_document(path, content)
            if ok:
                await self.staging.save_metadata()
            return jsonify({"success": ok, "path": path})

        @app.route("/api/document/export")
        async def api_export_document():
            path = request.args.get("path", "")
            if not path:
                return jsonify({"error": "path required"}), 400
            content = self.staging.get_document(path)
            if content is None:
                return jsonify({"error": "not found"}), 404
            filename = path.split("/")[-1]
            return content, 200, {
                "Content-Type": "text/markdown; charset=utf-8",
                "Content-Disposition": f'attachment; filename="{filename}"'
            }

        @app.route("/api/metadata", methods=["POST"])
        async def api_update_metadata():
            data = await request.get_json()
            path = data.get("path", "")
            if not path:
                return jsonify({"error": "path required"}), 400
            updates = {k: v for k, v in data.items() if k != "path"}
            ok = self.staging.update_metadata(path, updates)
            if ok:
                await self.staging.save_metadata()
            return jsonify({"success": ok})

        @app.route("/api/metadata/batch", methods=["POST"])
        async def api_batch_metadata():
            data = await request.get_json()
            paths = data.get("paths", [])
            updates = data.get("updates", {})
            count = 0
            for p in paths:
                if self.staging.update_metadata(p, updates):
                    count += 1
            if count:
                await self.staging.save_metadata()
            return jsonify({"updated": count})

        @app.route("/api/sync/fns", methods=["POST"])
        async def api_sync_fns():
            async def _progress(phase, current, total, filename, **kw):
                await self.sync_engine._emit_progress("fns", phase, current, total, filename, **kw)
            fns = self.fns_client_factory()
            try:
                result = await self.staging.sync_from_fns(fns, self.exclude_patterns, _progress)
            finally:
                await fns.close()
            return jsonify(result)

        @app.route("/api/sync/kb", methods=["POST"])
        async def api_sync_kb():
            async def _progress(phase, current, total, filename, **kw):
                await self.sync_engine._emit_progress("kb", phase, current, total, filename, **kw)
            result = await self.sync_engine.sync_to_kb(_progress)
            return jsonify(result)

        @app.route("/api/staging/rebuild", methods=["POST"])
        async def api_rebuild_staging():
            data = await request.get_json() or {}
            clear_kb = data.get("clear_kb", True)
            clear_fns = data.get("clear_fns", False)
            count = self.staging.rebuild_metadata(clear_kb=clear_kb, clear_fns=clear_fns)
            await self.staging.save_metadata()
            mode = []
            if clear_kb:
                mode.append("KB")
            if clear_fns:
                mode.append("FNS")
            logger.info(f"Staging: 重建元数据完成 | 模式: {'+'.join(mode)} | 影响 {count} 条文档")
            return jsonify({"success": True, "affected": count, "mode": mode})

        @app.route("/api/sync/to-fns", methods=["POST"])
        async def api_sync_to_fns():
            data = await request.get_json() or {}
            paths = data.get("paths")  # None = all, or list of paths
            async def _progress(phase, current, total, filename, **kw):
                await self.sync_engine._emit_progress("to_fns", phase, current, total, filename, **kw)
            fns = self.fns_client_factory()
            try:
                result = await self.staging.sync_to_fns(fns, paths, _progress)
            finally:
                await fns.close()
            return jsonify(result)

        @app.route("/api/sync/progress")
        async def api_sync_progress():
            from quart import Response
            sync_type = request.args.get("type", "fns")
            q = asyncio.Queue()
            self.sync_engine._progress_queues[sync_type] = q
            async def generate():
                try:
                    while True:
                        try:
                            data = await asyncio.wait_for(q.get(), timeout=60)
                            yield f"data: {json.dumps(data, ensure_ascii=False)}\n\n"
                            if data.get("phase") == "done":
                                break
                        except asyncio.TimeoutError:
                            yield ": keepalive\n\n"
                finally:
                    self.sync_engine._progress_queues.pop(sync_type, None)
            return Response(generate(), mimetype="text/event-stream",
                          headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})

        @app.route("/api/sync/history")
        async def api_sync_history():
            history = await self.staging._load_sync_history()
            return jsonify(history)

        @app.route("/api/conflicts")
        async def api_conflicts():
            fns = self.fns_client_factory()
            try:
                conflicts = await self.staging.detect_conflicts(fns, self.exclude_patterns)
            finally:
                await fns.close()
            return jsonify({"conflicts": conflicts, "total": len(conflicts)})

        @app.route("/api/conflicts/resolve", methods=["POST"])
        async def api_resolve_conflict():
            data = await request.get_json()
            path = data.get("path", "")
            resolution = data.get("resolution", "skip")
            if not path:
                return jsonify({"error": "path required"}), 400
            fns = self.fns_client_factory()
            try:
                result = await self.staging.resolve_conflict(path, resolution, fns)
            finally:
                await fns.close()
            return jsonify(result)

        @app.route("/api/conflicts/resolve-batch", methods=["POST"])
        async def api_resolve_conflicts_batch():
            data = await request.get_json()
            paths = data.get("paths", [])
            resolution = data.get("resolution", "skip")
            if not paths:
                return jsonify({"error": "paths required"}), 400
            conflicts = [{"path": p} for p in paths]
            fns = self.fns_client_factory()
            try:
                result = await self.staging.resolve_conflicts_batch(conflicts, resolution, fns)
            finally:
                await fns.close()
            return jsonify(result)

        @app.route("/api/status")
        async def api_status():
            return jsonify(self.staging.get_stats())

        @app.route("/api/config")
        async def api_config():
            return await self._plugin.webui_api_config()

        @app.route("/api/config/save", methods=["POST"])
        async def api_config_save():
            return await self._plugin.webui_api_config_save()

    async def start(self):
        """在后台 asyncio task 中启动 Quart 服务器。"""
        self._server_task = asyncio.create_task(
            self.app.run_task(host="0.0.0.0", port=self.port)
        )
        # 等待服务器启动
        await asyncio.sleep(0.5)
        logger.info(f"Staging Dashboard 已启动: http://0.0.0.0:{self.port}")

    async def stop(self):
        """停止 Quart 服务器。"""
        if self._server_task and not self._server_task.done():
            self._server_task.cancel()
            try:
                await self._server_task
            except asyncio.CancelledError:
                pass
            logger.info("Staging Dashboard 已停止")


# ═══════════════════════════════════════════════════════════════════
#  Plugin 主类
# ═══════════════════════════════════════════════════════════════════


class ObsidianKBStagingPlugin(Star):
    """Obsidian KB Staging Layer 插件。"""

    def __init__(self, context: Context, config: AstrBotConfig):
        super().__init__(context)
        self.config = config

        # FNS 配置
        self.fns_url: str = config.get("fns_url", "").rstrip("/")
        self.fns_token: str = config.get("fns_token", "")
        self.fns_vault: str = config.get("fns_vault", "")

        # 知识库配置
        self.kb_id: str = config.get("kb_id", "")
        self.kb_name: str = config.get("kb_name", "Obsidian Vault")

        # Dashboard 配置
        self.dashboard_port: int = config.get("dashboard_port", 6190)

        # 同步配置
        self.max_file_size: int = config.get("max_file_size", 100)
        self.chunk_size: int = config.get("chunk_size", 512)
        self.chunk_overlap: int = config.get("chunk_overlap", 50)
        self.concurrent_fetches: int = config.get("concurrent_fetches", 5)
        self.retry_count: int = config.get("retry_count", 3)
        self.exclude_patterns: list = config.get(
            "exclude_patterns", [".obsidian", ".trash", "*.tmp", ".git"]
        )

        # 数据目录
        self._data_dir = Path(get_astrbot_data_path()) / "plugin_data" / "astrbot_plugin_obsidian_kb_sync"
        self._data_dir.mkdir(parents=True, exist_ok=True)

        # 核心组件
        self.staging = StagingManager(
            data_dir=self._data_dir,
            max_file_size_kb=self.max_file_size,
        )
        self.sync_engine = SyncEngine(
            staging=self.staging,
            context=context,
            kb_id=self.kb_id,
            kb_name=self.kb_name,
            chunk_size=self.chunk_size,
            chunk_overlap=self.chunk_overlap,
        )
        self.dashboard = Dashboard(
            staging=self.staging,
            sync_engine=self.sync_engine,
            fns_client_factory=self._make_fns_client,
            exclude_patterns=self.exclude_patterns,
            port=self.dashboard_port,
            plugin=self,
        )

        # 状态
        self._is_syncing = False
        self._last_sync_time: float = 0
        self._last_fns_sync: float = 0
        self._last_kb_sync: float = 0

        # 注册 AstrBot WebUI API 路由（通过反向代理访问时使用）
        self._register_web_api(context)

        # 启动 Dashboard（兼容热重载：on_loaded 仅首次触发，热重载不会重新调用）
        self._startup_task = asyncio.create_task(self._async_startup())

        logger.info(
            f"Staging 插件初始化完成 | FNS: {self.fns_url} | "
            f"Vault: {self.fns_vault} | Dashboard: :{self.dashboard_port}"
        )

    async def _async_startup(self):
        """异步启动：启动 Dashboard 并执行初始同步。"""
        try:
            await self.dashboard.start()

            # 初始 FNS 同步
            if self.fns_url and self.fns_token and self.fns_vault:
                logger.info("Staging: 执行初始 FNS 同步...")
                fns = self._make_fns_client()
                try:
                    result = await self.staging.sync_from_fns(fns, self.exclude_patterns)
                    self._last_fns_sync = time.time()
                    logger.info(
                        f"Staging: 初始同步完成 | 新增 {result.get('new', 0)}, "
                        f"更新 {result.get('updated', 0)}, 未变 {result.get('unchanged', 0)}"
                    )
                except Exception as e:
                    logger.error(f"Staging: 初始同步失败: {e}")
                finally:
                    await fns.close()
            else:
                logger.warning("Staging: FNS 未配置，跳过初始同步")
        except Exception as e:
            logger.error(f"Staging: 异步启动失败: {e}")

    def _register_web_api(self, context: Context):
        """注册 API 路由到 AstrBot WebUI，供反向代理访问。"""
        if not hasattr(context, "register_web_api"):
            logger.info("AstrBot 版本不支持 register_web_api，跳过 WebUI 路由注册")
            return

        plugin_prefix = "/astrbot_plugin_obsidian_kb_sync"
        routes = [
            (f"{plugin_prefix}/api/folders",       "webui_api_folders",      ["GET"]),
            (f"{plugin_prefix}/api/documents",     "webui_api_documents",    ["GET"]),
            (f"{plugin_prefix}/api/document",      "webui_api_document",     ["GET", "POST"]),
            (f"{plugin_prefix}/api/metadata",      "webui_api_metadata",     ["POST"]),
            (f"{plugin_prefix}/api/metadata/batch","webui_api_batch_meta",   ["POST"]),
            (f"{plugin_prefix}/api/sync/fns",      "webui_api_sync_fns",     ["POST"]),
            (f"{plugin_prefix}/api/sync/kb",       "webui_api_sync_kb",      ["POST"]),
            (f"{plugin_prefix}/api/sync/history",  "webui_api_sync_history", ["GET"]),
            (f"{plugin_prefix}/api/sync/progress", "webui_api_sync_progress",["GET"]),
            (f"{plugin_prefix}/api/conflicts",     "webui_api_conflicts",    ["GET"]),
            (f"{plugin_prefix}/api/conflicts/resolve",      "webui_api_resolve_conflict",       ["POST"]),
            (f"{plugin_prefix}/api/conflicts/resolve-batch","webui_api_resolve_conflicts_batch",["POST"]),
            (f"{plugin_prefix}/api/staging/rebuild",  "webui_api_rebuild_staging", ["POST"]),
            (f"{plugin_prefix}/api/status",        "webui_api_status",       ["GET"]),
            (f"{plugin_prefix}/api/config",        "webui_api_config",       ["GET"]),
            (f"{plugin_prefix}/api/config/save",   "webui_api_config_save",  ["POST"]),
        ]
        for path, handler_name, methods in routes:
            handler = getattr(self, handler_name)
            context.register_web_api(path, handler, methods, f"Staging {handler_name}")

        logger.info(f"Staging: 已注册 {len(routes)} 条 WebUI API 路由")

    # ── WebUI API Handlers ──────────────────────────────────────

    async def webui_api_folders(self, **kwargs):
        from quart import jsonify
        return jsonify(self.staging.get_folder_tree())

    async def webui_api_documents(self, **kwargs):
        from quart import jsonify, request
        folder = request.args.get("folder", "")
        docs = self.staging.list_documents(folder if folder else None)
        return jsonify(docs)

    async def webui_api_document(self, **kwargs):
        from quart import jsonify, request
        if request.method == "GET":
            path = request.args.get("path", "")
            if not path:
                return jsonify({"error": "path required"}), 400
            content = self.staging.get_document(path)
            if content is None:
                return jsonify({"error": "not found"}), 404
            return jsonify({"path": path, "content": content})
        else:
            data = await request.get_json()
            path = data.get("path", "")
            content = data.get("content", "")
            if not path:
                return jsonify({"error": "path required"}), 400
            ok = self.staging.save_document(path, content)
            if ok:
                await self.staging.save_metadata()
            return jsonify({"success": ok})

    async def webui_api_metadata(self, **kwargs):
        from quart import jsonify, request
        data = await request.get_json()
        path = data.get("path", "")
        if not path:
            return jsonify({"error": "path required"}), 400
        updates = {k: v for k, v in data.items() if k != "path"}
        ok = self.staging.update_metadata(path, updates)
        if ok:
            await self.staging.save_metadata()
        return jsonify({"success": ok})

    async def webui_api_batch_meta(self, **kwargs):
        from quart import jsonify, request
        data = await request.get_json()
        paths = data.get("paths", [])
        updates = data.get("updates", {})
        count = 0
        for p in paths:
            if self.staging.update_metadata(p, updates):
                count += 1
        if count:
            await self.staging.save_metadata()
        return jsonify({"updated": count})

    async def webui_api_sync_fns(self, **kwargs):
        from quart import jsonify
        if not self.fns_url or not self.fns_token:
            return jsonify({"error": "FNS 未配置"}), 400
        async def _progress(phase, current, total, filename, **kw):
            await self.sync_engine._emit_progress("fns", phase, current, total, filename, **kw)
        fns = self._make_fns_client()
        try:
            result = await self.staging.sync_from_fns(fns, self.exclude_patterns, _progress)
        finally:
            await fns.close()
        if "error" not in result:
            self._last_fns_sync = time.time()
        return jsonify(result)

    async def webui_api_rebuild_staging(self, **kwargs):
        """重建暂存元数据：清除 KB 关联字段，下次同步全量重传。"""
        from quart import jsonify, request
        data = await request.get_json() or {}
        clear_kb = data.get("clear_kb", True)
        clear_fns = data.get("clear_fns", False)
        count = self.staging.rebuild_metadata(clear_kb=clear_kb, clear_fns=clear_fns)
        await self.staging.save_metadata()
        mode = []
        if clear_kb:
            mode.append("KB")
        if clear_fns:
            mode.append("FNS")
        logger.info(f"Staging: 重建元数据完成 | 模式: {'+'.join(mode)} | 影响 {count} 条文档")
        return jsonify({"success": True, "affected": count, "mode": mode})

    async def webui_api_sync_kb(self, **kwargs):
        from quart import jsonify
        async def _progress(phase, current, total, filename, **kw):
            await self.sync_engine._emit_progress("kb", phase, current, total, filename, **kw)
        result = await self.sync_engine.sync_to_kb(_progress)
        if "error" not in result:
            self._last_kb_sync = time.time()
        return jsonify(result)

    async def webui_api_sync_history(self, **kwargs):
        from quart import jsonify
        history = await self.staging._load_sync_history()
        return jsonify(history)

    async def webui_api_sync_progress(self, **kwargs):
        from quart import Response, request
        import json as _json
        sync_type = request.args.get("type", "fns") if hasattr(request, 'args') else "fns"
        q = asyncio.Queue()
        self.sync_engine._progress_queues[sync_type] = q
        async def generate():
            try:
                while True:
                    try:
                        data = await asyncio.wait_for(q.get(), timeout=60)
                        yield f"data: {_json.dumps(data, ensure_ascii=False)}\n\n"
                        if data.get("phase") == "done":
                            break
                    except asyncio.TimeoutError:
                        yield ": keepalive\n\n"
            finally:
                self.sync_engine._progress_queues.pop(sync_type, None)
        return Response(generate(), mimetype="text/event-stream",
                      headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})

    async def webui_api_status(self, **kwargs):
        from quart import jsonify
        stats = self.staging.get_stats()
        stats["last_fns_sync"] = self._last_fns_sync
        stats["last_kb_sync"] = self._last_kb_sync
        return jsonify(stats)

    async def webui_api_conflicts(self, **kwargs):
        from quart import jsonify
        fns = self._make_fns_client()
        try:
            conflicts = await self.staging.detect_conflicts(fns, self.exclude_patterns)
        finally:
            await fns.close()
        return jsonify({"conflicts": conflicts, "total": len(conflicts)})

    async def webui_api_resolve_conflict(self, **kwargs):
        from quart import jsonify, request
        data = await request.get_json()
        path = data.get("path", "")
        resolution = data.get("resolution", "skip")
        if not path:
            return jsonify({"error": "path required"}), 400
        fns = self._make_fns_client()
        try:
            result = await self.staging.resolve_conflict(path, resolution, fns)
        finally:
            await fns.close()
        return jsonify(result)

    async def webui_api_resolve_conflicts_batch(self, **kwargs):
        from quart import jsonify, request
        data = await request.get_json()
        paths = data.get("paths", [])
        resolution = data.get("resolution", "skip")
        if not paths:
            return jsonify({"error": "paths required"}), 400
        conflicts = [{"path": p} for p in paths]
        fns = self._make_fns_client()
        try:
            result = await self.staging.resolve_conflicts_batch(conflicts, resolution, fns)
        finally:
            await fns.close()
        return jsonify(result)

    # ── Config Schema（用于前端展示） ─────────────────────
    _CONFIG_SCHEMA = {
        "fns_url": {"desc": "FNS 服务地址", "type": "string", "hint": "例如 http://192.168.1.10:9002", "secret": False},
        "fns_token": {"desc": "FNS Token", "type": "string", "hint": "登录 Token（JWT）", "secret": True},
        "fns_vault": {"desc": "Vault 名称", "type": "string", "hint": "FNS 中的 Vault 名称", "secret": False},
        "kb_id": {"desc": "知识库 ID", "type": "string", "hint": "留空则自动创建", "secret": False},
        "kb_name": {"desc": "知识库名称", "type": "string", "hint": "自动创建时使用", "secret": False},
        "auto_sync": {"desc": "自动同步", "type": "bool", "hint": "开启定时自动同步", "secret": False},
        "sync_interval": {"desc": "同步间隔（秒）", "type": "int", "hint": "自动同步间隔", "secret": False},
        "exclude_patterns": {"desc": "排除模式", "type": "list", "hint": "glob 模式，逗号分隔", "secret": False},
        "restore_deleted": {"desc": "自动恢复删除", "type": "bool", "hint": "检测知识库中被删除的文档并重新上传", "secret": False},
        "max_file_size": {"desc": "最大文件（KB）", "type": "int", "hint": "0=不限制", "secret": False},
        "concurrent_fetches": {"desc": "并发获取数", "type": "int", "hint": "建议 3-10", "secret": False},
        "retry_count": {"desc": "重试次数", "type": "int", "hint": "API 失败重试", "secret": False},
        "verify_interval": {"desc": "校验间隔（次）", "type": "int", "hint": "0=关闭", "secret": False},
        "chunk_size": {"desc": "分块大小", "type": "int", "hint": "字符数，需与知识库一致", "secret": False},
        "chunk_overlap": {"desc": "分块重叠", "type": "int", "hint": "字符数，需与知识库一致", "secret": False},
        "dashboard_port": {"desc": "Dashboard 端口", "type": "int", "hint": "默认 6190", "secret": False},
    }

    async def webui_api_config(self, **kwargs):
        from quart import jsonify
        schema = self._CONFIG_SCHEMA
        result = {}
        for key, meta in schema.items():
            val = self.config.get(key, "")
            result[key] = {
                "value": val,
                "desc": meta["desc"],
                "type": meta["type"],
                "hint": meta["hint"],
                "secret": meta["secret"],
            }
        return jsonify(result)

    async def webui_api_config_save(self, **kwargs):
        from quart import jsonify, request
        data = await request.get_json()
        if not data:
            return jsonify({"error": "无效的请求数据"}), 400
        schema = self._CONFIG_SCHEMA
        saved = []
        for key, value in data.items():
            if key not in schema:
                continue
            meta = schema[key]
            # 类型转换
            if meta["type"] == "int":
                try:
                    value = int(value)
                except (ValueError, TypeError):
                    return jsonify({"error": f"{meta['desc']} 必须是整数"}), 400
            elif meta["type"] == "bool":
                value = bool(value)
            elif meta["type"] == "list":
                if isinstance(value, str):
                    value = [v.strip() for v in value.split(",") if v.strip()]
            self.config[key] = value
            saved.append(key)
        if saved:
            self.config.save_config()
            logger.info(f"配置已保存: {saved}")
        return jsonify({"saved": saved})

    def _make_fns_client(self) -> FNSClient:
        """创建 FNSClient 实例。"""
        return FNSClient(
            base_url=self.fns_url,
            token=self.fns_token,
            vault=self.fns_vault,
            retry_count=self.retry_count,
            concurrency=self.concurrent_fetches,
        )

    @filter.on_astrbot_loaded()
    async def on_loaded(self):
        """AstrBot 加载完成回调（Dashboard 已在 __init__ 中启动，这里仅做确认）。"""
        if self.dashboard._server_task and not self.dashboard._server_task.done():
            logger.info("Staging: Dashboard 已在启动流程中运行")
        else:
            # 兜底：如果 __init__ 中的 create_task 失败了，在这里重试
            logger.info("Staging: Dashboard 未运行，尝试启动...")
            await self.dashboard.start()

    # ── 指令 ──────────────────────────────────────────────────

    @filter.command("staging_sync")
    async def cmd_staging_sync(self, event: AstrMessageEvent):
        '''手动触发 FNS→Staging 同步'''
        if not self.fns_url or not self.fns_token:
            yield event.plain_result("❌ 请先配置 FNS 服务地址和 Token")
            return

        if self._is_syncing:
            yield event.plain_result("⏳ 已有同步任务在执行中，请稍候...")
            return

        self._is_syncing = True
        yield event.plain_result("🔄 正在从 FNS 同步到 Staging...")

        fns = self._make_fns_client()
        try:
            result = await self.staging.sync_from_fns(fns, self.exclude_patterns)
            self._last_sync_time = time.time()
            self._last_fns_sync = time.time()

            if "error" in result:
                yield event.plain_result(f"❌ 同步失败: {result['error']}")
            else:
                msg = (
                    f"✅ FNS→Staging 同步完成！\n"
                    f"📊 总笔记: {result['total']}\n"
                    f"  新增: {result['new']} | 更新: {result['updated']}\n"
                    f"  未变: {result['unchanged']} | 跳过: {result['skipped']}\n"
                    f"  错误: {result['errors']}\n"
                    f"⏱️ 耗时: {result.get('duration', 0):.1f}s"
                )
                yield event.plain_result(msg)
        except Exception as e:
            yield event.plain_result(f"❌ 同步异常: {e}")
        finally:
            await fns.close()
            self._is_syncing = False

    @filter.command("staging_push")
    async def cmd_staging_push(self, event: AstrMessageEvent):
        '''手动触发 Staging→KB 同步'''
        yield event.plain_result("📤 正在从 Staging 推送到知识库...")

        result = await self.sync_engine.sync_to_kb()
        self._last_kb_sync = time.time()
        if "error" in result:
            yield event.plain_result(f"❌ 推送失败: {result['error']}")
        else:
            msg = (
                f"✅ Staging→KB 推送完成！\n"
                f"📊 已选文档: {result['total']}\n"
                f"  新增: {result['new']} | 更新: {result['updated']}\n"
                f"  未变: {result['unchanged']} | 错误: {result['errors']}\n"
                f"⏱️ 耗时: {result.get('duration', 0):.1f}s"
            )
            yield event.plain_result(msg)

    @filter.command("staging_status")
    async def cmd_staging_status(self, event: AstrMessageEvent):
        '''查看 Staging 状态'''
        stats = self.staging.get_stats()
        last_sync = (
            time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(self._last_sync_time))
            if self._last_sync_time else "从未同步"
        )
        yield event.plain_result(
            f"📚 Obsidian KB Staging\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"🔗 FNS: {self.fns_url or '未配置'}\n"
            f"📂 Vault: {self.fns_vault or '未配置'}\n"
            f"📚 知识库: {self.kb_name} ({self.kb_id or '未设置'})\n"
            f"🌐 Dashboard: http://0.0.0.0:{self.dashboard_port}\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"📋 暂存文档: {stats['total_documents']}\n"
            f"✅ 已选同步: {stats['selected_for_sync']}\n"
            f"📤 已同步: {stats['synced_to_kb']}\n"
            f"🧩 增量分块: {stats['pre_chunked']}\n"
            f"💾 总大小: {stats['total_size_kb']}KB\n"
            f"🕐 上次同步: {last_sync}"
        )

    async def terminate(self):
        """插件停止时清理资源。"""
        if hasattr(self, '_startup_task') and self._startup_task and not self._startup_task.done():
            self._startup_task.cancel()
        await self.dashboard.stop()
        await self.staging.save_metadata()
        logger.info("Staging 插件已停止")

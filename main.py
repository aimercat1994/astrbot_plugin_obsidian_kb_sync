"""
AstrBot Obsidian 知识库同步插件

通过 Fast Note Sync Service 获取 Obsidian 笔记，自动同步到 AstrBot 知识库。
支持增量同步、手动同步、内容清洗等功能。
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

        # 数据目录
        self._data_dir = Path(get_astrbot_data_path()) / "plugin_data" / "astrbot_plugin_obsidian_kb_sync"
        self._data_dir.mkdir(parents=True, exist_ok=True)
        self._state_file = self._data_dir / "sync_state.json"

        self._load_state()
        logger.info(f"Obsidian KB Sync 初始化完成 | FNS: {self.fns_url} | Vault: {self.fns_vault}")

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

    def _save_state(self):
        try:
            with open(self._state_file, "w", encoding="utf-8") as f:
                json.dump(self._sync_state, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.error(f"保存同步状态失败: {e}")

    # ── Fast Note Sync 客户端 ─────────────────────────────────

    def _fns_headers(self) -> dict:
        return {"token": self.fns_token}

    async def _fns_list_notes(self, client: httpx.AsyncClient) -> list[dict]:
        """列出 vault 中所有笔记（不含内容）"""
        notes = []
        page = 1
        while True:
            try:
                resp = await client.get(
                    f"{self.fns_url}/api/notes",
                    params={"vault": self.fns_vault, "page": page, "pageSize": 100},
                    headers=self._fns_headers(),
                    timeout=30.0,
                )
                if resp.status_code == 401:
                    logger.error("FNS Token 无效或已过期")
                    return notes
                data = resp.json()
                items = data.get("data", {}).get("list", [])
                if not items:
                    break
                notes.extend(items)
                total = data.get("data", {}).get("pager", {}).get("totalRows", 0)
                if len(notes) >= total:
                    break
                page += 1
            except Exception as e:
                logger.error(f"列出笔记失败: {e}")
                break
        return notes

    async def _fns_get_note(self, client: httpx.AsyncClient, path: str) -> Optional[str]:
        """获取单条笔记内容"""
        try:
            resp = await client.get(
                f"{self.fns_url}/api/note",
                params={"vault": self.fns_vault, "path": path},
                headers=self._fns_headers(),
                timeout=30.0,
            )
            if resp.status_code == 200:
                return resp.json().get("data", {}).get("content", "")
            elif resp.status_code == 401:
                logger.error("FNS Token 无效或已过期")
                return None
        except Exception as e:
            logger.error(f"获取笔记失败 {path}: {e}")
        return None

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

    async def _get_kb_helper(self):
        """获取 KBHelper 实例"""
        kb_mgr = self.context.kb_manager
        if self.kb_id:
            helper = await kb_mgr.get_kb(self.kb_id)
            if helper:
                return helper
        # 按名称查找
        helper = await kb_mgr.get_kb_by_name(self.kb_name)
        if helper:
            self.kb_id = helper.kb.kb_id
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
            # 需要 embedding provider，尝试获取第一个可用的
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
            doc = await helper.upload_document(
                file_name=file_name,
                file_content=content.encode("utf-8"),
                file_type="md",
            )
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

    async def _doc_exists_in_kb(self, doc_id: str) -> bool:
        """检查文档是否仍存在于知识库中"""
        helper = await self._get_kb_helper()
        if not helper:
            return False
        try:
            doc = await helper.get_document(doc_id)
            return doc is not None
        except Exception:
            return False

    # ── 核心同步逻辑 ──────────────────────────────────────────

    async def _do_sync(self) -> dict:
        if not self.fns_url or not self.fns_vault:
            return {"error": "未配置 Fast Note Sync 地址或 Vault 名称"}
        if not self.fns_token:
            return {"error": "未配置 FNS Token"}
        if self._is_syncing:
            return {"error": "正在同步中，请稍候"}

        self._is_syncing = True
        result = {"total": 0, "new": 0, "updated": 0, "deleted": 0, "unchanged": 0, "errors": 0, "start_time": time.time()}

        try:
            # 确保知识库存在
            kb_helper = await self._ensure_knowledge_base()
            if not kb_helper:
                return {"error": "无法创建或找到 AstrBot 知识库"}

            async with httpx.AsyncClient() as client:
                # 获取笔记列表
                notes = await self._fns_list_notes(client)
                result["total"] = len(notes)
                logger.info(f"FNS 返回 {len(notes)} 条笔记")

                current_notes = {}
                for note in notes:
                    path = note.get("path", "")
                    if path and not self._should_exclude(path):
                        current_notes[path] = note

                tracked_paths = set(self._sync_state.keys())

                # 删除已移除的笔记
                for path in tracked_paths - set(current_notes.keys()):
                    doc_id = self._sync_state[path].get("doc_id")
                    if doc_id:
                        if await self._delete_document(doc_id):
                            result["deleted"] += 1
                    del self._sync_state[path]

                # 处理新增/更新
                for path, note_info in current_notes.items():
                    remote_hash = note_info.get("contentHash", "")
                    tracked = self._sync_state.get(path)

                    # 快速跳过：远端 hash 未变
                    if tracked and remote_hash and tracked.get("remote_hash") == remote_hash:
                        # 检查知识库中文档是否被手动删除
                        if self.restore_deleted and tracked.get("doc_id"):
                            if not await self._doc_exists_in_kb(tracked["doc_id"]):
                                logger.info(f"检测到文档已从知识库删除，将重新上传: {path}")
                                del self._sync_state[path]
                                # 不跳过，继续走后续上传逻辑
                            else:
                                result["unchanged"] += 1
                                continue
                        else:
                            result["unchanged"] += 1
                            continue

                    # 获取笔记内容
                    content = await self._fns_get_note(client, path)
                    if content is None:
                        result["errors"] += 1
                        continue

                    content = self._strip_frontmatter(content)
                    content = self._clean_obsidian_syntax(content)

                    if not content.strip():
                        result["unchanged"] += 1
                        continue

                    content_hash = hashlib.md5(content.encode("utf-8")).hexdigest()
                    if tracked and tracked.get("hash") == content_hash:
                        # 检查知识库中文档是否被手动删除
                        if self.restore_deleted and tracked.get("doc_id"):
                            if not await self._doc_exists_in_kb(tracked["doc_id"]):
                                logger.info(f"检测到文档已从知识库删除，将重新上传: {path}")
                                del self._sync_state[path]
                                # 不跳过，继续走上传逻辑
                            else:
                                result["unchanged"] += 1
                                if remote_hash:
                                    self._sync_state[path]["remote_hash"] = remote_hash
                                continue
                        else:
                            result["unchanged"] += 1
                            if remote_hash:
                                self._sync_state[path]["remote_hash"] = remote_hash
                            continue

                    # 上传
                    doc_id = await self._upload_document(path, content)
                    if doc_id:
                        if tracked and tracked.get("doc_id"):
                            await self._delete_document(tracked["doc_id"])
                        self._sync_state[path] = {
                            "hash": content_hash,
                            "remote_hash": remote_hash,
                            "doc_id": doc_id,
                            "sync_time": time.time(),
                        }
                        if tracked:
                            result["updated"] += 1
                        else:
                            result["new"] += 1
                        logger.info(f"已同步: {path} ({'更新' if tracked else '新增'})")
                    else:
                        result["errors"] += 1

                    # 节奏控制
                    if (result["new"] + result["updated"]) % 10 == 0:
                        await asyncio.sleep(1)

            self._save_state()
            result["duration"] = time.time() - result["start_time"]
            self._last_sync_time = time.time()
            self._sync_count += 1
            logger.info(
                f"同步完成: 新增 {result['new']}, 更新 {result['updated']}, "
                f"删除 {result['deleted']}, 未变 {result['unchanged']}, "
                f"错误 {result['errors']}, 耗时 {result['duration']:.1f}s"
            )
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
        logger.info("Obsidian KB Sync 插件已加载")
        if self.auto_sync and self.fns_url and self.fns_vault and self.fns_token:
            self._sync_task = asyncio.create_task(self._background_sync_loop())

    # ── 指令 ──────────────────────────────────────────────────

    @filter.command("obsidian_sync")
    async def manual_sync(self, event: AstrMessageEvent):
        '''手动触发 Obsidian 同步'''
        if not self.fns_url or not self.fns_token:
            yield event.plain_result("❌ 请先配置 FNS 服务地址和 Token")
            return
        yield event.plain_result("🔄 正在从 Fast Note Sync 同步笔记...")
        result = await self._do_sync()
        if "error" in result:
            yield event.plain_result(f"❌ 同步失败: {result['error']}")
        else:
            yield event.plain_result(
                f"✅ 同步完成！\n"
                f"📊 总笔记: {result['total']}\n"
                f"  新增: {result['new']} | 更新: {result['updated']} | 删除: {result['deleted']}\n"
                f"  未变: {result['unchanged']} | 错误: {result['errors']}\n"
                f"⏱️ 耗时: {result.get('duration', 0):.1f}s"
            )

    @filter.command("obsidian_status")
    async def show_status(self, event: AstrMessageEvent):
        '''查看 Obsidian 同步状态'''
        tracked = len(self._sync_state)
        last_sync = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(self._last_sync_time)) if self._last_sync_time else "从未同步"
        yield event.plain_result(
            f"📁 Obsidian 知识库同步\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"🔗 FNS: {self.fns_url or '未配置'}\n"
            f"📂 Vault: {self.fns_vault or '未配置'}\n"
            f"📚 知识库: {self.kb_name} ({self.kb_id or '未设置'})\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"📋 已同步: {tracked} 条\n"
            f"🔄 自动同步: {'开启' if self.auto_sync else '关闭'}\n"
            f"⏰ 间隔: {self.sync_interval}s\n"
            f"🕐 上次: {last_sync}\n"
            f"📊 次数: {self._sync_count}"
        )

    @filter.command("obsidian_reset")
    async def reset_sync(self, event: AstrMessageEvent):
        '''重置同步状态'''
        self._sync_state = {}
        self._save_state()
        self._sync_count = 0
        self._last_sync_time = 0
        yield event.plain_result("✅ 已重置！下次同步将全量重新上传。")

    async def terminate(self):
        if self._sync_task and not self._sync_task.done():
            self._sync_task.cancel()
            try:
                await self._sync_task
            except asyncio.CancelledError:
                pass
        self._save_state()
        logger.info("Obsidian KB Sync 插件已停止")

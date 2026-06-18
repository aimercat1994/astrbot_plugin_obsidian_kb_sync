# Changelog

## [3.2.0] - 2026-06-18

### 🎨 UI 重构
- **DaisyUI v5 + Tailwind CSS v4 CDN** 替代 Pico CSS，组件更丰富、暗色主题更完善
- 侧边栏合并文件夹树+文档列表为**统一树形视图**（类 VS Code/Obsidian 文件管理器）
- 所有文件夹支持**展开/收起**操作，默认收起状态
- 修复 `__files__` 数据结构适配问题（对象数组 vs 字符串数组）
- 顶栏渐变装饰条、状态胶囊徽章、toast 弹性动画

### 🐛 Bug Fixes
- 修复文件夹点击不展开文档列表的问题
- 修复叶子文件夹无法收起的问题
- 修复 CSS transition + max-height 在部分浏览器不可靠的问题（改用 display:none）

## [2.0.0] - 2026-06-10

### 🚀 Performance
- Incremental sync: ~30s → **0.4s** (~75x faster) via remote hash skip
- Concurrent note content fetching (default 5 workers, configurable)
- KB helper instance caching — no redundant lookups per upload/delete
- Async state file writes — no longer blocks the event loop

### 🛡️ Reliability
- HTTP requests now retry with exponential backoff (default 3 attempts)
- `restore_deleted` uses concurrent batch doc existence checks instead of O(n) sequential calls
- Precise exception handling: ConnectError / Timeout / HTTP error codes distinguished

### 📏 Resource Protection
- New `max_file_size` config: skip oversized notes to save embedding resources
- Smoother yield control: `await asyncio.sleep(0.1)` every 5 notes instead of `sleep(1)` every 10

### ⚙️ New Config Options
- `max_file_size` (int, default 100): Max note size in KB, 0 = unlimited
- `concurrent_fetches` (int, default 5): Parallel note fetches
- `retry_count` (int, default 3): API retry count

## [1.0.0] - 2026-06-10

### Added
- Initial release
- Incremental sync with content hash comparison
- Obsidian syntax cleanup (wikilinks, highlights, comments, callouts, embeds)
- Exclude patterns (glob)
- Auto/manual sync with configurable interval
- `obsidian_sync` / `obsidian_status` / `obsidian_reset` commands
- Auto-recreate knowledge base if missing
- Restore deleted documents from knowledge base

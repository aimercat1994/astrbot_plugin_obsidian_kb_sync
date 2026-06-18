# Changelog

## [3.3.0] - 2026-06-18

### ✨ 文档预览增强
- **Markdown 渲染升级**：引入 marked.js 替换自定义正则解析，支持 GFM 语法（表格、任务列表、删除线、高亮等）
- **代码语法高亮**：引入 highlight.js，支持 200+ 编程语言自动检测和高亮，明/暗主题自动切换
- **目录导航面板（TOC）**：
  - 自动提取文档标题生成目录（标题 ≥2 个时显示）
  - 点击目录项平滑滚动到对应位置
  - 滚动时自动高亮当前章节（Scroll Spy）
  - 编辑模式自动隐藏目录

### 🎨 样式增强
- 标题底部渐变装饰线
- 代码块圆角 + 等宽字体 + 语法高亮
- 引用块左侧渐变色条
- 表格行悬停高亮
- 任务列表 checkbox 样式
- 链接下划线 + hover 效果
- 图片圆角阴影
- 删除线、高亮标记样式

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

## [3.4.0] - 2026-06-18

### 📄 文档管理
- **新建文档**：顶栏「📄 新建」按钮，弹窗选择文件夹 + 输入文件名
- **删除文档**：标题栏「🗑️ 删除」按钮，带确认弹窗
- **导入文档**：顶栏「📂 导入」按钮，支持 .md/.txt/.markdown 文件
- **导出文档**：标题栏「📥 导出」按钮，自动下载为 .md 文件

### 🔧 后端 API
- `POST /api/document/create` - 新建文档（自动补 .md 扩展名）
- `POST /api/document/delete` - 删除文档
- `POST /api/document/import` - 导入文档（multipart/form-data）
- `GET /api/document/export` - 导出文档（下载 .md 文件）

## [3.5.0] - 2026-06-18

### 📤 同步到 FNS
- **新增「📤 同步到 FNS」按钮**：将暂存区文档推送到 FNS/Obsidian
- 支持全量推送（所有暂存文档）
- 并发上传（默认 5 路），396 篇文档 2.2 秒完成
- Toast 提示同步结果

### 🔧 后端
- FNSClient 新增 `upload_note` / `upload_notes_concurrent` 方法
- StagingManager 新增 `sync_to_fns` 方法
- 新增 `POST /api/sync/to-fns` API 端点

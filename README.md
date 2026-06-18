# Obsidian 知识库同步插件

通过 [Fast Note Sync Service](https://github.com/your-repo/fast-note-sync) 将 Obsidian 笔记同步到 AstrBot 知识库。支持暂存层管理、Web Dashboard、选择性同步、增量同步。

## ✨ 功能特性

### v3.x — 暂存层 + Dashboard

- **暂存层**：FNS 笔记先同步到本地暂存区（镜像 Obsidian vault 目录结构），再从暂存区推送到知识库
- **Web Dashboard**（端口 6190）：浏览/编辑暂存文档、勾选同步和预切块、一键推送
- **选择性同步**：只同步勾选的文档到知识库，未勾选的不消耗 embedding 资源
- **预切块增量同步**：标记为预切块的文档，更新时只重新 embedding 变化的 chunk，token 消耗降低 90%+

### v2.x — 核心同步

- **增量同步**：基于内容 hash 智能跳过未变更笔记
- **并发获取**：多路并发从 FNS 拉取笔记内容（默认 5 路）
- **自动重试**：网络抖动自动指数退避重试（默认 3 次）
- **文件大小限制**：跳过超大文件避免浪费 embedding 资源
- **内容清洗**：自动去除 Obsidian 特有语法（`[[wikilinks]]`、`==高亮==`、`%%注释%%` 等）
- **删除恢复**：检测知识库中文档被手动删除后自动重新上传
- **双层验证**：即时验证 + 定期全量校验，确保知识库与同步状态一致
- **排除规则**：支持 glob 模式排除文件/文件夹

## 📦 安装

1. 在 AstrBot Dashboard → 插件管理 → 安装插件
2. 输入本仓库地址或插件包上传
3. 安装依赖（插件会自动安装 `httpx`、`quart`）

## ⚙️ 配置

在 AstrBot Dashboard → 插件管理 → 本插件 → 配置 中设置：

| 配置项 | 类型 | 默认值 | 说明 |
|--------|------|--------|------|
| `fns_url` | string | `""` | Fast Note Sync 服务地址，如 `http://192.168.1.10:9002` |
| `fns_token` | string | `""` | FNS 登录 Token（JWT） |
| `fns_vault` | string | `""` | FNS 中的 Vault 名称 |
| `kb_id` | string | `""` | 目标 AstrBot 知识库 ID，留空自动创建 |
| `kb_name` | string | `"Obsidian Vault"` | 自动创建知识库时的名称 |
| `dashboard_port` | int | `6190` | Dashboard Web UI 端口 |
| `auto_sync` | bool | `true` | 是否开启自动同步 |
| `sync_interval` | int | `300` | 自动同步间隔（秒） |
| `exclude_patterns` | list | `[".obsidian", ".trash", "*.tmp", ".git"]` | 排除模式（glob） |
| `restore_deleted` | bool | `true` | 知识库中文档被删除后是否自动恢复 |
| `max_file_size` | int | `100` | 最大文件大小（KB），超过跳过，`0` 不限制 |
| `concurrent_fetches` | int | `5` | 并发获取笔记数 |
| `retry_count` | int | `3` | API 请求失败重试次数 |
| `verify_interval` | int | `10` | 全量校验间隔（每 N 次同步），`0` 关闭 |
| `chunk_size` | int | `512` | 增量同步时的文本分块大小（字符） |
| `chunk_overlap` | int | `50` | 增量同步时的分块重叠大小（字符） |

## 🖥️ Dashboard

启动 AstrBot 后访问 `http://<your-ip>:6190` 打开 Dashboard（也可通过 AstrBot 插件页面直接访问）。

**界面框架**：DaisyUI v5 + Tailwind CSS v4 CDN，支持亮色/暗色主题自动切换。

| 功能 | 说明 |
|------|------|
| 资源管理器 | 左侧统一树形视图，文件夹和文档混排展示，点击文件夹展开/收起，支持多级嵌套 |
| 文档查看/编辑 | 右侧主区域，点击文档加载 Markdown 内容，支持在线编辑（Ctrl+S 保存） |
| 搜索过滤 | 侧边栏顶部搜索框，实时过滤文件名 |
| 同步按钮 | 「从 FNS 同步」拉取最新 / 「同步到知识库」推送选中文档 |
| 状态指示 | 顶栏显示 FNS/知识库连接状态，底部状态栏显示文档统计 |

## 🎮 指令

| 指令 | 说明 |
|------|------|
| `staging_sync` | 从 FNS 同步到暂存区 |
| `staging_push` | 将选中文档推送到知识库 |
| `staging_status` | 查看暂存区状态 |

## 📋 前置要求

1. **AstrBot** v4.25+ 且已配置至少一个 Embedding 模型提供商
2. **Fast Note Sync Service** 已部署并运行，Obsidian 插件端已同步笔记

## 🔧 工作原理

### v3 架构（两步同步）

```
Obsidian ──(同步)──► FNS Server ──(HTTP API)──► 暂存区 ──(选择性)──► AstrBot 知识库
                                              (本地 .md)    Dashboard 控制
```

1. **FNS → 暂存区**：从 FNS 拉取笔记，写入本地暂存目录（镜像 Obsidian 目录结构）
2. **用户操作**：在 Dashboard 浏览/编辑文档，勾选要同步的文档和预切块选项
3. **暂存区 → 知识库**：
   - `pre_chunk=false`：全量上传，AstrBot 自动切块 embedding
   - `pre_chunk=true`：增量同步，自己切块 → hash 对比 → 只重新 embedding 变化的 chunk

### 增量同步原理

```
旧文档（200 chunks）──► 获取旧 chunk 内容 ──► 计算每块 hash
新内容 ──► 自己切块（MarkdownChunker）──► 计算每块 hash
对比：3 块变化，197 块未变
→ 只对 3 块重新 embedding（vec_db.insert）
→ 197 块保留原有 embedding，零开销
```

## 📝 更新日志

### v3.2.0 (2026-06-18)

**🎨 UI 重构**
- DaisyUI v5 + Tailwind CSS v4 CDN 替代 Pico CSS，组件更丰富、暗色主题更完善
- 侧边栏合并文件夹树+文档列表为统一树形视图（类 VS Code/Obsidian 文件管理器）
- 所有文件夹支持展开/收起操作，默认收起状态
- 顶栏渐变装饰条、状态胶囊徽章、toast 弹性动画

**🐛 Bug Fixes**
- 修复 `__files__` 数据结构适配问题（对象数组 vs 字符串数组）
- 修复文件夹点击不展开文档列表的问题
- 修复 CSS transition + max-height 在部分浏览器不可靠的问题

### v3.0.0 (2026-06-17)

**🆕 暂存层**
- FNS 笔记先同步到本地暂存区（镜像 Obsidian vault 目录结构）
- 暂存区作为 FNS 客户端，支持一键同步

**🖥️ Web Dashboard**
- 文件夹树 + 文档列表 + Markdown 查看器/编辑器
- 勾选控制 sync_to_kb（同步到知识库）和 pre_chunk（预切块增量同步）
- 批量操作：全选同步、全选预切块、取消全选
- 搜索/筛选功能
- 状态栏：总文档数、已选数、已同步数

**🧩 增量同步**
- 预切块文档使用 MarkdownChunker 自己切块
- 每块算 MD5 hash，对比新旧 chunk
- 只对变化的 chunk 重新 embedding，未变 chunk 零开销
- 300KB 文档改 1 段落：embedding 从几百块降到几块，token 消耗降低 90%+

**🔍 双层验证**
- 即时验证：上传后立即确认 doc_id 存在
- 定期全量校验：每 N 次同步检查所有 doc_id 是否仍存在

**🆕 新增配置**
- `dashboard_port`：Dashboard 端口，默认 6190
- `verify_interval`：全量校验间隔，默认 10
- `chunk_size`：分块大小，默认 512
- `chunk_overlap`：分块重叠，默认 50

### v2.0.0 (2026-06-10)

**🚀 性能优化**
- 增量同步从 ~30s 降至 **0.4s**（~75x 加速）
- 笔记内容并发获取（默认 5 路），替代逐条串行
- KB helper 实例缓存，避免重复查找知识库
- `_save_state` 改为异步写入，不阻塞事件循环

**🛡️ 可靠性提升**
- HTTP 请求增加指数退避重试（默认 3 次）
- `restore_deleted` 改为并发批量检查文档存在性
- 更精确的异常分类处理（ConnectError / Timeout / HTTP 错误码）

**📏 资源保护**
- 新增 `max_file_size` 配置：跳过超大文件避免浪费 embedding 资源
- 节奏控制优化：每 5 条让出事件循环，替代粗暴的 sleep(1)

**🆕 新增配置**
- `max_file_size`：文件大小上限（KB），默认 100
- `concurrent_fetches`：并发获取数，默认 5
- `retry_count`：重试次数，默认 3

### v1.0.0 (2026-06-10)

- 初始版本
- 基本同步功能：增量同步、内容清洗、排除规则
- 手动/自动同步、状态查看、重置指令

## 📄 License

MIT

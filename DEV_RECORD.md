# AstrBot Obsidian 知识库同步插件 — 开发记录

> 本文档记录插件开发过程中的关键技术决策、踩坑、架构选型，供后续开发参考。

## 📐 项目架构

```
astrbot_plugin_obsidian_kb_sync/
├── main.py              # 插件入口、AstrBot 生命周期、指令、API 路由
├── core/
│   ├── __init__.py
│   ├── staging.py       # StagingManager — 暂存区 CRUD、同步逻辑
│   ├── fns_client.py    # FNSClient — FNS HTTP API 封装
│   └── chunker.py       # MarkdownChunker — 文本分块 + MD5 hash
├── pages/
│   └── dashboard.html   # 完整 Dashboard 单文件（HTML + CSS + JS）
├── metadata.yaml        # 插件元数据（版本、依赖、配置项）
├── requirements.txt     # Python 依赖
├── CHANGELOG.md         # 更新日志
├── DEV_RECORD.md        # 本文件
└── README.md            # 用户文档
```

### 关键设计决策

1. **单文件 Dashboard**：所有前端代码（HTML + CSS + JS）写在一个 `pages/dashboard.html` 里，Quart 用 `send_from_directory` 提供，无需构建工具
2. **Quart 独立服务**：Dashboard 用独立 Quart app 运行（端口 6190），不跟 AstrBot 主服务耦合
3. **双入口 API**：Dashboard API 同时注册在 AstrBot 内部路由（:6185 插件页面访问）和独立 Quart 服务（:6190 直接访问）
4. **暂存区即文件系统**：暂存区就是本地文件目录（镜像 Obsidian vault 结构），直接用 `pathlib` 操作

## 🛠️ 技术栈

### 前端
| 库 | 版本 | 用途 | 引入方式 |
|---|---|---|---|
| DaisyUI | v5 | UI 组件（btn/card/table/menu/badge/toast） | CDN |
| Tailwind CSS | v4 | 原子化 CSS | CDN（浏览器端编译） |
| marked.js | - | Markdown → HTML 渲染 | CDN |
| highlight.js | - | 代码语法高亮（200+ 语言） | CDN |

### 后端
| 库 | 用途 |
|---|---|
| Quart | 异步 Web 框架（Dashboard + API） |
| httpx | 异步 HTTP 客户端（FNS API 调用） |
| AstrBot SDK | 知识库操作、配置管理、指令注册 |

## 🐛 踩坑记录

### 1. `__files__` 数据结构不一致
**问题**：文件夹的 `__files__` 存的是对象数组 `[{name, path, size_kb, ...}]`，不是路径字符串数组。代码拿整个对象当 key 去查 `docMap`，自然找不到 → 所有文件都不显示。

**修复**：`_getFilesInNode` 里从对象取 `.path` 再查 docMap。

**教训**：API 返回的嵌套数据结构一定要先 `console.log` 看清楚再写前端逻辑。

### 2. Tailwind CSS v4 CDN + `@keyframes` 不兼容
**问题**：在 `<style>` 标签里写的 `@keyframes toast-in` 动画，被 Tailwind CSS v4 CDN 浏览器端编译时干扰，动画结束后 opacity 回到 0 → toast 创建了但是透明的。

**修复**：改用 Tailwind 的 `transition-opacity duration-300` + JS 直接操作 `style.opacity`。

**教训**：Tailwind CDN 是浏览器端实时编译的，会处理页面上所有 `<style>` 标签，不要在混用自定义 `@keyframes` 和 Tailwind。

### 3. AstrBot 桥接的暗色主题
**问题**：AstrBot 插件页面通过 iframe 嵌入 Dashboard，会注入 `data-theme="dark"` 属性到 `<html>`。

**修复**：用 MutationObserver 监听 `data-theme` 变化，动态切换 DaisyUI 主题 + highlight.js 样式表。

**教训**：AstrBot 的插件桥接机制会修改 iframe DOM，前端必须被动响应。

### 4. inline onclick 在浏览器自动化工具中不触发
**问题**：`browser_click` 工具点击带有 `onclick="toggleFolder('AI')"` 的元素时，onclick 不触发。

**根因**：浏览器自动化工具的 click 事件可能被 CSS 伪元素（`::before`）拦截，或者 inline onclick 的事件绑定在自动化环境下不生效。

**修复**：改用事件委托 + `data-folder` 属性，同时保留 inline onclick 作为真实浏览器的备用。

### 5. FNS API 请求头要求
**问题**：FNS 的 WebGui 认证 API 必须带 `X-Client: WebGui` 和 `Accept: application/json` 头，否则返回 401。

**修复**：`upload_note` 方法加上这两个头。

### 6. `escapeHtml` 重命名为 `esc`
**问题**：重构工具函数时把 `escapeHtml` 改名为 `esc`，但 toast 里还在引用旧名 → JS 报错，toast 根本创建不了。

**教训**：全局重命名时用 IDE 的 rename 功能，不要手动替换。

### 7. CSS transition + `max-height` 不可靠
**问题**：文件夹收起用 `max-height: 0` + `overflow: hidden` + `transition`，在部分浏览器下收起后内容仍然可见。

**修复**：改用 `display: none`（无动画但可靠）。

## 📦 CDN 依赖清单

```html
<!-- DaisyUI v5 + Tailwind CSS v4 -->
<link href="https://cdn.jsdelivr.net/npm/daisyui@5/css/daisyui.css" rel="stylesheet" />
<script src="https://cdn.tailwindcss.com/v4"></script>

<!-- Markdown + 代码高亮 -->
<script src="https://cdn.jsdelivr.net/npm/marked/marked.min.js"></script>
<link id="hljs-theme" href="https://cdn.jsdelivr.net/gh/highlightjs/cdn-release@11/build/styles/github.min.css" rel="stylesheet" />
<script src="https://cdn.jsdelivr.net/gh/highlightjs/cdn-release@11/build/highlight.min.js"></script>
```

## 🔄 版本发布流程

```bash
# 1. 更新版本号
vim metadata.yaml  # version: x.y.z

# 2. 更新 CHANGELOG.md
vim CHANGELOG.md

# 3. 更新 README.md（如需）
vim README.md

# 4. 提交推送
cd ~/data/plugins/astrbot_plugin_obsidian_kb_sync
git add -A && git commit -m "vx.y.z: 描述" && git push

# 5. 创建 GitHub Release
gh release create "vx.y.z" --repo aimercat1994/astrbot_plugin_obsidian_kb_sync \
  --title "vx.y.z" --notes "更新内容"

# 6. 提交插件市场
# 在 https://github.com/AstrBotDevs/AstrBot/issues/new?template=plugin_submission.yml 提交

# 7. 重启 AstrBot 生效
sudo systemctl restart astrbot
```

## 🧪 测试清单

- [ ] `📥 从 FNS 同步` — 从 FNS 拉取文档到暂存区
- [ ] `📤 同步到知识库` — 将选中文档推送到 AstrBot 知识库
- [ ] `📤 同步到 FNS` — 将暂存区修改写回 FNS
- [ ] 文件夹展开/收起
- [ ] 文档点击预览
- [ ] 文档编辑 + Ctrl+S 保存
- [ ] 文档新建/删除/导入/导出
- [ ] 批量操作（全选同步/切块/取消全选）
- [ ] 同步/切块 toggle 开关
- [ ] 设置弹窗打开/保存
- [ ] 搜索过滤
- [ ] 暗色主题切换
- [ ] 目录导航（TOC）显示/跳转
- [ ] 代码语法高亮

## 🎯 后续开发方向

### 高优先级
- **同步历史日志**：记录每次同步的文件数、耗时、失败原因
- **冲突检测**：检测 FNS 和知识库之间的版本差异
- **同步进度条**：大批量同步时显示实时进度

### 中优先级
- **Webhook 实时同步**：FNS 文件变化时自动触发同步
- **快捷键**：Ctrl+F 搜索、方向键导航文档列表
- **文件夹统计**：显示每个文件夹的文档数/总大小

### 低优先级
- **向量搜索**：在 Dashboard 中直接语义搜索知识库
- **多知识库支持**：一个插件管理多个知识库
- **知识图谱**：可视化文档之间的关联关系

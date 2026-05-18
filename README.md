# astrbot_plugin_obsidian_kb_sync

AstrBot 插件：通过 [Fast Note Sync Service](https://github.com/haierkeys/fast-note-sync-service) 将 Obsidian 笔记同步到 AstrBot 知识库。

## 架构

```
┌─────────────┐    同步客户端    ┌─────────────┐    AstrBot 插件    ┌─────────────┐
│  Obsidian   │ ──────────────→ │  FNS 服务   │ ────────────────→ │  AstrBot    │
│  (本地PC)   │                 │  (NAS/服务器)│                   │  知识库      │
└─────────────┘                 └─────────────┘                   └─────────────┘
```

Obsidian 通过 FNS 客户端实时同步笔记到 FNS 服务，本插件定时从 FNS 拉取变更并更新 AstrBot 知识库。

## 功能

- **增量同步** — 通过内容 hash 检测变更，只上传新增/修改的笔记
- **自动删除** — Obsidian 中删除笔记后，知识库中对应文档也会删除
- **自动恢复** — 知识库中文档被手动删除后，下次同步自动重新上传（可关闭）
- **内容清洗** — 自动剥离 YAML frontmatter、`![[嵌入]]`、`[[Wiki链接]]`、`> [!callout]`、`==高亮==`、`%%注释%%`
- **后台定时** — 支持自动定时同步，间隔可配置
- **手动触发** — 通过指令随时手动同步

## 安装

1. 将本仓库克隆或下载到 AstrBot 的 `data/plugins/` 目录
2. 在 AstrBot WebUI 安装依赖（`httpx`）
3. 确保 AstrBot 已配置 Embedding 模型（知识库需要）

```bash
cd /path/to/astrbot/data/plugins
git clone https://github.com/aimercat1994/astrbot_plugin_obsidian_kb_sync.git
```

## 配置

在 AstrBot WebUI 的插件配置页面填写：

| 配置项 | 类型 | 默认值 | 说明 |
|--------|------|--------|------|
| `fns_url` | string | `""` | Fast Note Sync 服务地址，如 `http://192.168.1.10:9002` |
| `fns_token` | string | `""` | FNS 登录 Token（JWT） |
| `fns_vault` | string | `""` | FNS 中的 Vault 名称 |
| `kb_id` | string | `""` | AstrBot 知识库 ID，留空则自动创建 |
| `kb_name` | string | `"Obsidian Vault"` | 自动创建知识库时使用的名称 |
| `auto_sync` | bool | `true` | 开启自动同步 |
| `sync_interval` | int | `300` | 自动同步间隔（秒） |
| `exclude_patterns` | list | `[".obsidian", ".trash", "*.tmp", ".git"]` | 排除的文件/文件夹模式 |
| `restore_deleted` | bool | `true` | 知识库中文档被删除后自动恢复 |

### 获取 FNS Token

```bash
curl -s -X POST "http://YOUR_FNS_HOST:PORT/api/user/login" \
  -H "Content-Type: application/json" \
  -d '{"credentials":"admin","password":"yourpass"}' | jq -r '.data.token'
```

## 指令

| 指令 | 说明 |
|------|------|
| `/obsidian_sync` | 手动触发同步 |
| `/obsidian_status` | 查看同步状态 |
| `/obsidian_reset` | 重置同步状态，下次全量重新上传 |

## 同步逻辑

1. 登录 FNS，获取 Vault 中所有笔记列表
2. 对比本地缓存的 hash，跳过未变化的笔记
3. 对于已跟踪但知识库中被删除的文档，自动重新上传（`restore_deleted`）
4. 对于变更的笔记：拉取内容 → 剥离 frontmatter → 清洗 Obsidian 语法 → 上传到知识库
5. 对于 Obsidian 中已删除的笔记：从知识库中删除对应文档

## 依赖

- AstrBot >= 4.5.0（需要知识库功能）
- httpx >= 0.25.0
- Fast Note Sync Service（[GitHub](https://github.com/haierkeys/fast-note-sync-service)）

## License

AGPL-v3

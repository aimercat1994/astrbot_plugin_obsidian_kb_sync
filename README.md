# Obsidian 知识库同步插件

通过 [Fast Note Sync Service](https://github.com/your-repo/fast-note-sync) 将 Obsidian 笔记自动同步到 AstrBot 知识库。

## ✨ 功能特性

- **增量同步**：基于内容 hash 智能跳过未变更笔记，增量同步仅需秒级完成
- **并发获取**：多路并发从 FNS 拉取笔记内容，默认 5 路并发
- **自动重试**：网络抖动自动指数退避重试（默认 3 次）
- **文件大小限制**：跳过超大文件避免浪费 embedding 资源
- **内容清洗**：自动去除 Obsidian 特有语法（`[[wikilinks]]`、`==高亮==`、`%%注释%%` 等）
- **删除恢复**：检测知识库中文档被手动删除后自动重新上传
- **排除规则**：支持 glob 模式排除文件/文件夹
- **手动/自动同步**：支持定时自动同步 + 指令手动触发

## 📦 安装

1. 在 AstrBot Dashboard → 插件管理 → 安装插件
2. 输入本仓库地址或插件包上传
3. 安装依赖（插件会自动安装 `httpx`）

## ⚙️ 配置

在 AstrBot Dashboard → 插件管理 → 本插件 → 配置 中设置：

| 配置项 | 类型 | 默认值 | 说明 |
|--------|------|--------|------|
| `fns_url` | string | `""` | Fast Note Sync 服务地址，如 `http://192.168.1.10:9002` |
| `fns_token` | string | `""` | FNS 登录 Token（JWT） |
| `fns_vault` | string | `""` | FNS 中的 Vault 名称 |
| `kb_id` | string | `""` | 目标 AstrBot 知识库 ID，留空自动创建 |
| `kb_name` | string | `"Obsidian Vault"` | 自动创建知识库时的名称 |
| `auto_sync` | bool | `true` | 是否开启自动同步 |
| `sync_interval` | int | `300` | 自动同步间隔（秒） |
| `exclude_patterns` | list | `[".obsidian", ".trash", "*.tmp", ".git"]` | 排除模式（glob） |
| `restore_deleted` | bool | `true` | 知识库中文档被删除后是否自动恢复 |
| `max_file_size` | int | `100` | 最大文件大小（KB），超过跳过，`0` 不限制 |
| `concurrent_fetches` | int | `5` | 并发获取笔记数 |
| `retry_count` | int | `3` | API 请求失败重试次数 |

## 🎮 指令

| 指令 | 说明 |
|------|------|
| `obsidian_sync` | 手动触发同步（带进度日志） |
| `obsidian_status` | 查看同步状态、已同步数量、上次同步时间 |
| `obsidian_reset` | 重置同步状态，下次同步全量重新上传 |

## 📋 前置要求

1. **AstrBot** v4.25+ 且已配置至少一个 Embedding 模型提供商
2. **Fast Note Sync Service** 已部署并运行，Obsidian 插件端已同步笔记

## 🔧 工作原理

```
Obsidian ──(同步)──► FNS Server ──(HTTP API)──► 本插件 ──(embedding)──► AstrBot 知识库
```

1. 从 FNS 获取笔记列表（含 `contentHash`）
2. 与本地同步状态比对，分类为：新增 / 更新 / 删除 / 未变更 / 跳过
3. 并发获取需更新笔记的内容
4. 清洗 Obsidian 语法后上传到 AstrBot 知识库
5. 保存同步状态，下次增量同步

## 📝 更新日志

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
